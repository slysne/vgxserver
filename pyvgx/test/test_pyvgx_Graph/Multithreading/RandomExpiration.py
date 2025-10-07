###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    RandomExpiration.py
# Author:  Stian Lysne slysne.dev@gmail.com
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgxtest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time



RUNNING = True

INTERVALS = [ 101, 137, 251, 293, 347 ]

graph_name_fmt = "multithreading_%d"

N_GRAPHS = [ 1, 2, 3, 4 ]
N_FEEDERS = [ 1, 2, 3, 4 ]
N_SEARCHERS = [ 1, 2, 3, 4, 5 ]
N_DELETERS = [ 1, 2 ]
N_MONITORS = [ 1 ]
N_SAVERS = [ 1 ]


###############################################################################
# TEST_RandomExpiration
#
###############################################################################
def TEST_RandomExpiration():
    """
    Test multiple threads working in parallel to create, delete and expire
    vertices and arcs in a chaos-like scenario.
    t_nominal=720
    test_level=3701
    """
    global RUNNING

    GRAPHS = {}

    for gn in N_GRAPHS:
        name = graph_name_fmt % gn
        G = {
            'instance'      : None,
            'save_interval' : INTERVALS[ gn ],
            'workers'       : {
                'feeders'       : [],
                'searchers'     : [],
                'deleters'      : [],
                'monitors'      : [],
                'savers'        : []
            }
        }
        GRAPHS[ name ] = G

        workers = G['workers']

        # feeders
        for fn in N_FEEDERS:
            feeder = Worker( "g%d-feeder%d" % (gn, fn) )
            workers['feeders'].append( feeder )
        # searchers
        for sn in N_SEARCHERS:
            searcher = Worker( "g%d-searcher%d" % (gn, sn) )
            workers['searchers'].append( searcher )
        # deleters
        for dn in N_DELETERS:
            deleter = Worker( "g%d-deleter%d" % (gn, dn) )
            workers['deleters'].append( deleter )
        # monitors
        for mn in N_MONITORS:
            monitor = Worker( "g%d-monitor%d" % (gn, mn) )
            workers['monitors'].append( monitor )
        # savers  
        for sn in N_SAVERS:
            saver = Worker( "g%d-saver%d" % (gn, sn) )
            workers['savers'].append( saver )


    def initseed( seed ):
        Framehash.lfsr63( seed )

    def setlabel( label ):
        threadlabel( label )
        assert threadlabel( None ) == label

    threadlabel( "Python Main" )

    for name, G in list(GRAPHS.items()):
        workers = G['workers']
        for worker_list in list(workers.values()):
            for w in worker_list:
                w.perform( initseed, random.randint( 1, 2**63 ) )
                w.perform( setlabel, w.name )
        g = G['instance'] = Graph( name )
        g.Truncate()


    NMASK = 0x7FFFFF  # 23 bits = 0-8 million (node names)
    IMASK = 0x1F      # 5 bits  = 0-31 int arcs
    XMASK = 0x7       # 3 bits  = 0-7 tmx arcs
    FMASK = 0x1FF     # 9 bits  = 0-512 seconds into the future


    def prepare_vertices( graph ):
        for n in range( NMASK + 1 ):
            graph.CreateVertex( str(n) )


    def feed( graph ):
        global RUNNING
        try:
            # run iterations of terminal-to-head connections
            n = 0
            while RUNNING:
                n += 1
                ok = False
                did_notify = False
                now = int( graph.ts )
                while RUNNING and not ok:
                    A = None
                    B = None
                    try:
                        # Random anchor node with random expiration
                        nodeA = str(Framehash.rand63() & NMASK)
                        nodeB = ""
                        A = Vertex( graph, nodeA, None, "w", 1000 )
                        A.ClearExpiration()
                        A.SetExpiration( now + (Framehash.rand63() & FMASK) )
                        # Set of random terminals with random expiration
                        _r_nodeB = Framehash.rand63()
                        for i in range( 100 ):
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
                                    graph.CloseVertex( A )
                                    A = None
                                    if B:
                                        graph.CloseVertex( B )
                                        B = None
                                    break


                            if A and B:
                                B.ClearExpiration()
                                B.SetExpiration( now + (Framehash.rand63() & FMASK) )
                                # put a random number of (rel_r,M_INT) arcs from A to B
                                for r in range( Framehash.rand63() & IMASK ):
                                    graph.Connect( A, ("rel_%d"%r, M_INT, n*r), B )
                                # put a random number of (rel_r,M_TMX) arcs from A to B
                                for r in range( Framehash.rand63() & XMASK ):
                                    tmx = now + (Framehash.rand63() & FMASK)
                                    graph.Connect( A, ("rel_%d"%r, M_TMX, tmx), B )
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

        finally:
            graph.CloseAll()



    def delete( graph ):
        global RUNNING
        try:
            # run iterations of terminal-to-head connections
            n = 0
            while RUNNING:
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
        finally:
            graph.CloseAll()





    def search( graph ):
        global RUNNING
        try:
            # Get all expiring neighbors for all the neighbors with M_INT arc for all of our anchor's neighbors
            Q3 = graph.NewNeighborhoodQuery(
              "",
              arc = ( "*", D_ANY ),
              collect = C_NONE,
              neighbor = {
                'arc'      : ( "*", D_ANY, M_INT ),
                'collect'  : C_NONE,
                'neighbor' : {
                  'arc'      : ( "*", D_ANY, M_TMX ),
                  'collect'  : C_COLLECT
                }
              },
              sortby    = S_VAL,
              result    = R_STR,
              fields    = F_AARC|F_DEG
            )

            # Get all expiring neighbors for all the neighbors with M_INT arc for all of our anchor's neighbors
            Q4 = graph.NewNeighborhoodQuery(
              "",
              arc = ( "*", D_ANY ),
              collect = C_COLLECT,
              neighbor = {
                'arc'      : ( "*", D_ANY ),
                'collect'  : C_COLLECT,
                'neighbor' : {
                  'arc'      : ( "*", D_ANY ),
                  'collect'  : C_COLLECT #,
                }
              },
              sortby    = S_RANDOM,
              result    = R_STR,
              fields    = F_ALL
            )

            n = 0
            while RUNNING:
                n += 1
                V = None
                try:
                    # Pick a random node
                    node = str( Framehash.rand63() & NMASK )

                    now = int( graph.ts )

                    # Get expiring outarcs that expire within the next 240 seconds
                    q1 = graph.Neighborhood(
                      node,
                      arc       = ( "*", D_OUT, M_TMX, V_LT, now+240 ),
                      sortby    = S_VAL,
                      result    = R_STR,
                      fields    = F_AARC|F_DEG,
                      timeout   = 1000
                    )

                    # Pick another random node
                    node = str( Framehash.rand63() & NMASK )

                    # Get all the initials with M_INT arcs for all of our anchor's terminals that it is connected to for at least another 60 seconds
                    q2 = graph.Neighborhood(
                      node,
                      arc      = ( "*", D_OUT, M_TMX, V_GT, now+60 ),
                      collect  = C_NONE,
                      neighbor = {
                        'arc'     : ( "*", D_IN, M_INT ),
                        'collect' : C_COLLECT
                      },
                      sortby    = S_VAL,
                      result    = R_STR,
                      fields    = F_AARC|F_DEG,
                      timeout   = 4000
                    )

                    # Pick another random node
                    while V is None:
                        node = str( Framehash.rand63() & NMASK )
                        V = Vertex( graph, node, None, "r", 50 )

                    # Get all expiring neighbors for all the neighbors with M_INT arc for all of our anchor's neighbors
                    Q3.id = V
                    Q3.Execute( timeout=16000 )
                    graph.CloseVertex( V )
                    V = None

                    # Pick another random node
                    # Get all expiring neighbors for all the neighbors with M_INT arc for all of our anchor's neighbors
                    Q4.id = str( Framehash.rand63() & NMASK )
                    Q4.Execute( hits=25, timeout=16000 )


                except KeyError:
                    pass
                except Exception as ex:
                    LogError( "Reader could not search: [%s]" % ex )
                    time.sleep(0.1)
                finally:
                    if V is not None:
                        graph.CloseVertex( V )
                        V = None
        finally:
            graph.CloseAll()



    def monitoring( graph ):
        global RUNNING
        try:
            t0 = time.time()
            ts = t0 + 1
            while RUNNING:
                t1 = time.time()
                if t1 > ts:
                    LogInfo( graph.EventBacklog() )
                    LogInfo( graph )
                    ts = t1 + 15
                else:
                    time.sleep(0.5)
        finally:
            graph.CloseAll()



    def save( graph, interval ):
        global RUNNING
        try:
            t0 = time.time()
            ts = t0 + 15
            while RUNNING:
                t1 = time.time()
                if t1 > ts:
                    try:
                        graph.Save( 10000 )
                    except Exception as ex:
                        LogError( str(ex) )
                    ts = time.time() + interval
                else:
                    time.sleep( 0.5 )
        finally:
            graph.CloseAll()


    t_end = time.time() + 600 # 10 minutes
    #t_end = time.time() + 3600


    THREADS = []

    # Prepare vertices in all graphs
    for name, G in list(GRAPHS.items()):
        g = G['instance']
        LogInfo( "Preparing vertices for %s" % g )
        prepare_vertices( g )

    # Start feeders, searchers and monitors
    for G in list(GRAPHS.values()):
        g = G['instance']
        LogInfo( "Starting feeders, searchers and monitors for %s" % g )
        workers = G['workers']
        for w in workers['feeders']:
            THREADS.append( w )
            w.perform( setlabel, w.name )
            w.perform( feed, g )
        for w in workers['searchers']:
            THREADS.append( w )
            w.perform( setlabel, w.name )
            w.perform( search, g )
        for w in workers['monitors']:
            THREADS.append( w )
            w.perform( setlabel, w.name )
            w.perform( monitoring, g )

    # Start deleters and savers
    for G in list(GRAPHS.values()):
        g = G['instance']
        LogInfo( "Starting deleters and savers for %s" % g )
        workers = G['workers']
        for w in workers['deleters']:
            THREADS.append( w )
            w.perform( setlabel, w.name )
            w.perform( delete, g )
        for w in workers['savers']:
            THREADS.append( w )
            w.perform( setlabel, w.name )
            w.perform( save, g, G['save_interval'] )


    # Run for configured duration
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
            for G in list(GRAPHS.values()):
                g = G['instance']
                print()
                print("---------------------------------------------------")
                print("Open Vertices for %s" % g)
                g.ShowOpenVertices()
                print("---------------------------------------------------")
                print()
        LogInfo( "All worker threads have terminated" )


    for G in list(GRAPHS.values()):
        g = G['instance']
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
        # Go!
        g.EventEnable()

    now = int( time.time() )

    def total_order( graphs ):
        return sum( [ G['instance'].order for G in graphs ] )

    count = total_order( list(GRAPHS.values()) )
    no_progress = 0
    while count > 0 and no_progress < 6:
        new_count = total_order( list(GRAPHS.values()) )
        if new_count < count:
            count = new_count
            no_progress = 0
            for G in list(GRAPHS.values()):
                g = G['instance']
                LogInfo( g.GetOpenVertices() )
                LogInfo( g.EventBacklog() )
                LogInfo( str( g ) )
        # no progress
        else:
            LogWarning( "no progress: %d" % new_count )
            no_progress += 1
        time.sleep( 5 )

    # Verify empty graphs
    for G in list(GRAPHS.values()):
        g = G['instance']
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
