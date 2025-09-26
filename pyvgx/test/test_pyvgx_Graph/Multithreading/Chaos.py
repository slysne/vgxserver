###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Chaos.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

from pytest.pytest import RunTests, Expect, TestFailed, PerformCleanup
from pytest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time



RUNNING = True

NMASK = 0xFFFFFF    # 24 bits   =   0-16 million (node names)
IMASK = 0x3         # 2 bits    =   0-3 M_INT arcs per terminal
FMASK = 0x3         # 2 bits    =   0-3 M_FLT arcs per terminal
TMASK = 0x7F        # 7 bits    =   0-127 terminals


N_FEEDERS = [ 1, 2, 3 ]
N_SEARCHERS = [ 1, 2, 3, 4, 5, 6, 7, 8 ]
N_DELETERS = [ 1 ]
N_MONITORS = [ 1 ]


N_GRAPHS = [ 1, 2 ]
graph_name_fmt = "chaos_%d"




###############################################################################
# feed
#
###############################################################################
def feed( graph ):
    """
    Create random network:


    (a) =[rel_r,M_INT,val]=> (b) =[...]=> (...)
        =[rel_r,M_FLT,val]=> (c)

    """


    global RUNNING
    # run iterations of terminal-to-head connections
    n = 0
    while RUNNING:
        n += 1
        ok = False
        did_notify = False
        while RUNNING and not ok:
            A = None
            B = None
            try:
                if graph.IsGraphReadonly():
                    time.sleep( 1.0 )
                    continue

                # Random anchor node
                nodeA = str(Framehash.rand63() & NMASK)
                nodeB = ""
                A = Vertex( graph, nodeA, None, "w", 1000 )
                # Set properties
                A[ 'rand31' ] = Framehash.rand31()
                A[ 'name' ] = nodeA
                A[ 'seq' ] = n

                # Set of random terminals
                nterms = Framehash.rand63() & TMASK
                _r_nodeB = Framehash.rand63()
                for i in range( nterms ):
                    nodeB = str( _r_nodeB & NMASK )
                    _r_nodeB += 1
                    try:
                        B = Vertex( graph, nodeB, None, "w", 50 )
                    except:
                        graph.RelaxVertex( A )
                        try:
                            B = Vertex( graph, nodeB, None, "w", 1000 )
                        except:
                            B = None
                        try:
                            graph.EscalateVertex( A, 50 )
                        except:
                            A = None

                    if A and B:
                        # put a random number of (rel_r,M_INT) arcs from A to B
                        for r in range( Framehash.rand63() & IMASK ):
                            graph.Connect( A, ("rel_%d"%r, M_INT, i*r), B )
                        # put a random number of (rel_r,M_FLT) arcs from A to B
                        for r in range( Framehash.rand63() & FMASK ):
                            graph.Connect( A, ("rel_%d"%r, M_FLT, i*r), B )
                        graph.CloseVertex( B )
                        B = None
                ok = True
            except Exception as ex:
                if did_notify is False:
                    if not graph.IsGraphReadonly():
                        LogError( "Writer could not acquire tail/head: (%s)-[...]->(%s)   [%s] \n" % (nodeA, nodeB, ex) )
                    did_notify = True
                time.sleep(0.01)
            finally:
                if A is not None:
                    graph.CloseVertex(A)
                if B is not None:
                    graph.CloseVertex(B)




###############################################################################
# delete
#
###############################################################################
def delete( graph ):
    """
    """
    global RUNNING
    # run iterations of terminal-to-head connections
    n = 0
    while RUNNING:
        if graph.IsGraphReadonly():
            time.sleep( 1.0 )
            continue

        n += 1
        nodeA = str(Framehash.rand63() & NMASK)
        did_notify = False
        V = None
        while RUNNING and V is None:
            try: 
                V = graph.OpenVertex( nodeA, mode="a", timeout=1000 )
                if V.degree > 75:
                    graph.DeleteVertex( nodeA )
            except KeyError:
                time.sleep(0.001)
                break
            except Exception as ex:
                if did_notify is False:
                    if not graph.IsGraphReadonly():
                        LogError( "Deleter could not delete %s [%s] \n" % (nodeA, ex) )
                    did_notify = True
                time.sleep(0.01)
            finally:
                if V is not None:
                    graph.CloseVertex( V )





###############################################################################
# search
#
###############################################################################
def search( graph ):
    """
    """
    global RUNNING
    n = 0
    while RUNNING:
        n += 1
        V = None
        try:

            # Pick a random node
            node = str( Framehash.rand63() & NMASK )
            if node not in graph:
                continue

            AggrQ = graph.NewAggregatorQuery(
                    id          =   node,
                    arc         =   ("*", D_OUT, M_INT)
                )


            for R_x in [R_STR, R_DICT]:
                for F_x in [F_ADDR, F_VAL, F_AARC|F_PROP|F_RANK]:
                    for S_x in [S_NONE, S_ID, S_DEG, S_RANDOM]:
                        common = { 'result':R_x, 'fields':F_x, 'sortby':S_x }

                        NQ1 = graph.NewNeighborhoodQuery(
                                id          =   node,
                                arc         =   D_OUT,
                                collect     =   C_COLLECT,
                                **common
                             )


                        NQ2 = graph.NewNeighborhoodQuery(
                                id          = node,
                                arc         =   D_OUT,
                                collect     =   C_NONE,
                                neighbor    =   {
                                    'collect'   :   C_NONE,
                                    'arc'       :   ("rel_0", D_IN),
                                    'neighbor'  :   {
                                        'collect'   :   C_NONE,
                                        'arc'       :   ("rel_0", D_OUT, M_INT),
                                        'neighbor'  :   {
                                            'collect'   :   C_COLLECT,
                                            'arc'       :   ("*", D_IN, M_INT, V_GT, 50)
                                        }
                                     }
                                },
                                **common
                            )

                        NQ3 = graph.NewNeighborhoodQuery(
                                id          =   node,
                                arc         =   D_OUT,
                                filter      =   "filter1",
                                collect     =   C_COLLECT,
                                neighbor    =   {
                                    'traverse'  : {
                                        'arc'       :   D_OUT,
                                        'filter'    :   "filter2",
                                        'collect'   :   C_COLLECT
                                    }
                                },
                                **common
                            )


                        for H_x in [0, 1, 10, 100]:

                            n_1 = NQ1.Execute( hits=H_x, timeout=5000 )

                            aggr = AggrQ.Execute( timeout=5000 )

                            n_2 = NQ2.Execute( hits=H_x, timeout=5000 )
                            
                            n_3 = NQ3.Execute( hits=H_x, timeout=5000 )

                            terminals = graph.Terminals( node )

                            initials = graph.Initials( node )
                        # H_x
                    # S_x
                # F_x
            # R_x
        # try


        except KeyError:
            pass
        except Exception as ex:
            LogError( "Reader could not search: [%s]" % ex )
            time.sleep(0.1)
        finally:
            if V is not None:
                graph.CloseVertex( V )
                V = None



###############################################################################
# monitoring
#
###############################################################################
def monitoring( graph ):
    """
    """
    global RUNNING
    t0 = time.time()
    ts = t0 + 1
    while RUNNING:
        t1 = time.time()
        if t1 > ts:
            LogInfo( graph )
            ts = t1 + 5
        else:
            time.sleep(0.5)



###############################################################################
# freeze
#
###############################################################################
def freeze( graph ):
    """
    """
    global RUNNING
    t0 = time.time()
    flip_interval = 10
    t_flip = t0 + flip_interval
    S_RO = 0
    S_WR = 1
    state = S_WR
    try:
        while RUNNING:
            t1 = time.time()
            if t1 > t_flip:
                if state == S_RO:
                    graph.ClearGraphReadonly()
                    if graph.IsGraphReadonly():
                        LogInfo( "Switching '%s' to WRITABLE did not succeed, still readonly" % (graph.path) )
                    else:
                        LogInfo( "Switched '%s' to WRITABLE for %d seconds" % (graph.path, flip_interval) )
                        state = S_WR
                        flip_interval = 60

                elif state == S_WR:
                    try:
                        graph.SetGraphReadonly( 60000 )
                        state = S_RO
                        flip_interval = 20
                        LogInfo( "Switched '%s' to READONLY for %d seconds" % (graph.path, flip_interval) )
                    except:
                        LogError( "Freezer could not set graph readonly" )
                t_flip = time.time() + flip_interval
            else:
                time.sleep( 1.0 )
    finally:
        while graph.IsGraphReadonly():
            graph.ClearGraphReadonly()







###############################################################################
# TEST_Chaos
#
###############################################################################
def TEST_Chaos():
    """
    Test multiple threads working in parallel to create, delete and search.
    t_nominal=660
    test_level=3701
    """
    global RUNNING

    THREADS = []
    GRAPHS = {}

    def initseed( seed ):
        Framehash.lfsr63( seed )


    for gn in N_GRAPHS:

        name = graph_name_fmt % gn
        G = {
            'instance'      : None,
            'workers'       : {
                'feeders'       : [],
                'searchers'     : [],
                'deleters'      : [],
                'monitors'      : [],
                'freezers'      : [],
            }
        }

        workers = G['workers']

        # feeders
        for fn in N_FEEDERS:
            feeder = Worker( "g%d_feeder_%d" % (gn, fn) )
            workers['feeders'].append( feeder )
        # searchers
        for sn in N_SEARCHERS:
            searcher = Worker( "g%d_searcher_%d" % (gn, sn) )
            workers['searchers'].append( searcher )
        # deleters
        for dn in N_DELETERS:
            deleter = Worker( "g%d_deleter_%d" % (gn, dn) )
            workers['deleters'].append( deleter )
        # monitors
        for mn in N_MONITORS:
            monitor = Worker( "g%d_monitor_%d" % (gn, mn) )
            workers['monitors'].append( monitor )
        # freezers
        for fn in [1]:
            freezer = Worker( "g%d_freezer_%d" % (gn, fn) )
            workers['freezers'].append( freezer )


        # initialize random seed for all threads
        for worker_list in list(workers.values()):
            for w in worker_list:
                w.perform( initseed, random.randint( 1, 2**63 ) )

        # create graph instance
        g = G['instance'] = Graph( name )
        GRAPHS[ name ] = g
        g.Truncate()


        g.Define( "filter1 := (next['rand31'] > vertex['rand31']) || (next.deg > vertex.deg) || next['seq'] % 17 == 0" )
        g.Define( "filter2 := next.arc.value > 49 || next.arc.value < 25 || next.arc.value == 36" )

        # Start all workers
        LogInfo( "Starting feeders, searchers and monitors for %s" % g )
        for w in workers['feeders']:
            THREADS.append( w )
            w.perform( feed, g )
        for w in workers['searchers']:
            THREADS.append( w )
            w.perform( search, g )
        for w in workers['monitors']:
            THREADS.append( w )
            w.perform( monitoring, g )
        for w in workers['deleters']:
            THREADS.append( w )
            w.perform( delete, g )
        for w in workers['freezers']:
            THREADS.append( w )
            w.perform( freeze, g )



    # Run for configured duration
    t_end = time.time() + 600 # 10 minutes
    try:
        while time.time() < t_end:
            time.sleep( 1 )
    except KeyboardInterrupt:
        pass
    finally:
        # Tell all threads to stop
        LogInfo( "Asking all workers to terminate" )
        RUNNING = False
        for w in THREADS:
            w.terminate()
        while len( [ 1 for w in THREADS if not w.is_dead() ] ):
            time.sleep(5.0)
        LogInfo( "All worker threads have terminated" )


    for name, g in list(GRAPHS.items()):
        now = int( time.time() )
        gts = int( g.ts )
        LogInfo( "%s: time=%d g.ts=%d (delta=%d)" % (g, now, gts, now-gts) )
        

        try:
            g.EventDisable()
        except:
            LogWarning( "Failed to disable event processor" )
        vlist = g.Vertices( timeout=30000 )
        LogInfo( "Setting immediate expiration for %d vertices in graph: %s" % (len(vlist), g) )
        for name in vlist:
            try:
                V = g.OpenVertex( name, 'a', timeout=1000 )
                V.ClearExpiration()
                V.SetExpiration( 1, True )
                g.CloseVertex( V )
            except KeyError:
                pass
            except AccessError:
                print("Timeout when setting immediate expiration for for '%s'" % name)
        # go!
        g.EventEnable()
        now = int( time.time() )

        # Waiting max 120 seconds for graph to clear out
        while time.time() - now < 120 and g.order > 0:
            LogInfo( g.GetOpenVertices() )
            LogInfo( g.EventBacklog() )
            LogInfo( str( g ) )
            time.sleep( 2 )

        # Verify empty graphs
        LogInfo( g.EventBacklog() )
        LogInfo( str( g ) )
        Expect( g.order == 0 and g.size == 0,      "Graph should be empty, got %s" % g )
        g.DebugCheckAllocators()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
    for gn in N_GRAPHS:
        name = graph_name_fmt % gn
        g = Graph( name )
        g.Close()
        del g
    PerformCleanup()
