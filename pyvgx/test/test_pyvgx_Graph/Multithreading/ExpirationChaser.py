###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    ExpirationChaser.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgxtest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time
import traceback


RUNNING = True
DURATION = 600 # 10 minutes

N_NODES = 8000000

graph_name = "expiration_chase"

MAX_IN = 0

TIMEOUT = 5000



###############################################################################
# nodename
#
###############################################################################
def nodename( n ):
    """
    """
    extra = "x" * (1 + n % 50)
    return "%d_%s" % (n, extra)




###############################################################################
# TEST_ExpirationChaser
#
###############################################################################
def TEST_ExpirationChaser():
    """
    t_nominal=660
    test_level=3701
    """
    global RUNNING
    global MAX_IN

    def connect( graph ):
        global RUNNING
        global MAX_IN

        a = 1
        b = 2
        n = 1

        INITS = list(range(0, a))

        while RUNNING:
            try:
                TERMS = list(range(a, b))

                for i in INITS:
                    init = nodename(i % N_NODES)
                    graph.CloseVertex( graph.NewVertex( init, timeout=TIMEOUT ) )
                    prev = graph.Neighborhood( init, ("*",D_IN), result=R_LIST, fields=F_VAL|F_ID, timeout=TIMEOUT )
                    np = len( prev )
                    if np > MAX_IN:
                        MAX_IN = np
                    for t in TERMS:
                        term = nodename(t % N_NODES)
                        graph.CloseVertex( graph.NewVertex( term, timeout=TIMEOUT ) )
                        rel = "rel_%d" % (n%3)
                        A,B = graph.OpenVertices( [init, term], timeout=TIMEOUT )
                        try:
                            graph.Connect( A, (rel,M_INT,n), B )
                        finally:
                            graph.CloseVertices( [A,B] )

                INITS = TERMS
                a = b
                n += 1
                b += n

            except Exception as ex:
                E = str(ex).split()
                if len(E) >= 5 and E[3] == '@':
                    print("Locked ID: %s" % graph.VertexIdByAddress( int(E[4],16) ))
                print(traceback.format_exc())
            


    def expire( graph ):
        global RUNNING
        n = 0
        while RUNNING:
            n += 1
            try:
                node = nodename(random.randint(0, N_NODES-1))
                N = graph.NewVertex( node, timeout=TIMEOUT )
                N['n'] = n
                #N.SetExpiration( n % 500, True )
                graph.CloseVertex( N )

                # Fake expiration of random
                node = nodename(random.randint(0, N_NODES-1))
                if node in graph:
                    graph.DeleteVertex( node, timeout=TIMEOUT )

            except Exception as ex:
                print(traceback.format_exc())



    graph = Graph( graph_name )
    graph.Truncate()


    LogInfo( "Initializing with %d vertices" % N_NODES )
    for n in range( N_NODES ):
        graph.CreateVertex( nodename(n), type="node" )



    LogInfo( "Running expiration chase for %d seconds" % DURATION )
    LogInfo( "%s" % graph )
    LogInfo( "%s" % graph.EventBacklog() )
    Connecter = Worker( "Connecter" )
    Connecter.perform( connect, graph )
    Expirator = Worker( "Expirator" )
    Expirator.perform( expire, graph )

    t0 = t1 = time.time()
    t_end = t0 + DURATION
    try:
        t2 = t1
        while t1 - t0 < DURATION:
            t1 = time.time()
            time.sleep( 0.1 )
            if t1 - t2 > 5:
                LogInfo( "%s (max_in=%d)" % (graph, MAX_IN) )
                LogInfo( "%s" % graph.EventBacklog() )
                t2 += 5
    except KeyboardInterrupt:
        LogInfo( "User cancelled" )

    LogInfo( "Stopping expiration chase" )
    LogInfo( "%s (max_in=%d)" % (graph, MAX_IN) )
    LogInfo( "%s" % graph.EventBacklog() )
    RUNNING = False
    Connecter.terminate()
    Expirator.terminate()
    t1 = time.time()
    while not Connecter.is_dead() or not Expirator.is_dead():
        time.sleep( 0.1 )
        if time.time() - t1 > 10:
            break

    if Connecter.is_dead():
        LogInfo( "Connecter stopped" )
    else:
        LogWarning( "Failed to stop Connecter" )
    if Expirator.is_dead():
        LogInfo( "Expirator stopped" )
    else:
        LogWarning( "Failed to stop Expirator" )
    
    graph.EventDisable()
    graph.Truncate()
    graph.EventEnable()

    LogInfo( "%s (max_in=%d)" % (graph, MAX_IN) )
    LogInfo( "%s" % graph.EventBacklog() )

    graph.Erase()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
    PerformCleanup()
