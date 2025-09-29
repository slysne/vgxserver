###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Load.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from pyvgxtest.threads import Worker
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import socket
import random
import time

graph = None




###############################################################################
# get_socket
#
###############################################################################
def get_socket( host, port ):
    """
    """
    s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    s.connect( (host, port) )
    s.setblocking( False )
    return s




###############################################################################
# client_loop
#
###############################################################################
def client_loop( self, plugin, getquery, nsock=1 ):
    """
    """

    host, port = Support.get_server_host_port()

    SOCKET = []
    
    REQUEST_FMT = b"GET /vgx/plugin/%s?%%s HTTP/1.1\r\n\r\n" % plugin.encode()

    LogInfo( "Client loop {} running".format(self) )

    while self.is_running():
        try:
            # Make sure we have enough connections
            while len(SOCKET) < nsock:
                SOCKET.append( [get_socket( host, port ), 0] )
            # Send requests to all connections
            for i in range(len(SOCKET)):
                S = SOCKET[i]
                try:
                    query = getquery()
                    request = REQUEST_FMT % query
                    S[0].sendall( request )
                    S[1] = 0
                    self.QueryCounter += 1
                except Exception as ex:
                    LogError( "{}: Error from sendall(): {}".format(self, ex) )
                    S[0].close()
                    SOCKET[i] = [None, 0]
            # Remove closed sockets
            if None in SOCKET:
                SOCKET = [[s,nbytes] for s,nbytes in SOCKET if s is not None]
            # Try to receive from all sockets
            count_blocking = 0
            while len([1 for s,nbytes in SOCKET if nbytes == 0]) > 0:
                for i in range(len(SOCKET)):
                    S = SOCKET[i]
                    try:
                        S[1] += len( S[0].recv( 8192 ) )
                    except BlockingIOError as recv_ex:
                        count_blocking += 1
                if count_blocking > 1000:
                    if not self.is_running():
                        LogWarning( "{}: No final recv data from socket(s) after shutdown".format(self) )
                        break


        except Exception as ex:
            LogError( "{}: Error: {}".format(self, ex) )
            for s, nbytes in SOCKET:
                s.close()
            SOCKET = []

    for s, nbytes in SOCKET:
        s.close()

    LogInfo( "Client loop {} exit".format(self) )




###############################################################################
# add_worker
#
###############################################################################
def add_worker( L, plugin_name, getquery_func, nsock=5 ):
    """
    """
    wn = len(L) + 1
    w = Worker( "Client {}".format(wn) )
    w.QueryCounter = 0
    w.perform( client_loop, w, plugin_name, getquery_func, nsock )
    L.append( w )
    return w




###############################################################################
# run_load
#
###############################################################################
def run_load( plugin_name, getquery_func, duration=30, initial_nworkers=1, inc_nworkers_interval=0, nsock_per_worker=1 ):
    """
    """
    
    LogInfo( "Running load test: plugin_name={} getquery_func={} duration={} initial_nworkers={} inc_nworkers_interval={} nsock_per_worker={}".format( 
                                 plugin_name,   getquery_func.__name__,   duration,   initial_nworkers,   inc_nworkers_interval,   nsock_per_worker ) )

    WORKERS = []

    # Create initial worker pool
    wn = 0
    for n in range( initial_nworkers ):
        add_worker( WORKERS, plugin_name, getquery_func, nsock_per_worker )

    # Run for duration
    t0 = t1 = time.time()
    t_end = t0 + duration
    prev_qps = 0
    prev_count = 0
    delta_t = 0
    if inc_nworkers_interval > 0:
        t_next_inc = t0 + inc_nworkers_interval
    else:
        t_next_inc = t0 + 1e9

    while t1 < t_end:
        # Run for a while
        time.sleep( 1 )
        # Current time
        now = time.time()
        delta_t = now - t1
        t1 = now
        # Sum total requests
        count = sum( [w.QueryCounter for w in WORKERS] )
        qps = (count - prev_count) / delta_t
        prev_count = count
        try:
            server_qps = system.RequestRate()
        except:
            server_qps = -1
        LogInfo( "Total requests: %d  Client rate: %.1f  Server rate: %.1f" % (count, qps, server_qps) )
        prev_qps = qps
        # Add worker
        if now > t_next_inc:
            add_worker( WORKERS, plugin_name, getquery_func, nsock_per_worker )
            t_next_inc += inc_nworkers_interval


    # Terminate workers
    for w in WORKERS:
        w.terminate()

    # Wait for all workers to terminate
    deadline = time.time() + 60
    while len([1 for w in WORKERS if not w.is_dead()]) > 0 and time.time() < deadline:
        time.sleep( 0.5 )

    still_running = len([1 for w in WORKERS if not w.is_dead()])
    if still_running > 0:
        LogWarning( "{} worker threads still running!".format( still_running ) )

    t = time.time() - t0

    LogInfo( "Load test completed in %.2f seconds" % (t) )





###############################################################################
# TEST_performance__simple
#
###############################################################################
def TEST_performance__simple():
    """
    Basic performance test with a simple plugin
    test_level=4101
    t_nominal=75
    """

    def simple_plugin( request, x:float, y:float ):
        """
        Return x * y
        """
        return x * y

    # Function that returns query parameters for plugin
    def getquery():
        r = random.random()
        return b"x=%f&y=%f" % (r,r)

    # Add simple plugin
    system.AddPlugin( plugin=simple_plugin, name="simple" )

    # One worker, single socket per worker
    run_load( "simple", getquery, duration=15, initial_nworkers=1, nsock_per_worker=1 )
    # Two workers, two sockets per worker
    run_load( "simple", getquery, duration=15, initial_nworkers=2, nsock_per_worker=2 )
    # Two workers, 10 sockets per worker
    run_load( "simple", getquery, duration=15, initial_nworkers=2, nsock_per_worker=10 )
    # Two workers, 10 sockets per worker, increase number of workers gradually
    run_load( "simple", getquery, duration=30, initial_nworkers=2, inc_nworkers_interval=5, nsock_per_worker=10 )


    # Remove the plugin
    system.RemovePlugin( "simple" )   

    


###############################################################################
# TEST_performance__graph
#
###############################################################################
def TEST_performance__graph():
    """
    Performance test with graph fill and query
    test_level=4101
    t_nominal=100
    """
    R = range( 1000 )

    def get_connect_params():
        a,b,r = random.sample( R, 3 )
        return b"init=V%d&term=V%d&rel=to&val=%d" % (a, b, r)


    def connect_plugin( request, graph, init:str, term:str, rel:str, val:int ):
        """
        Create two vertices and connect them
        """
        ret = 0
        try:
            graph.CreateVertex( init )
            graph.CreateVertex( term )
            A,B = graph.OpenVertices( [init, term], timeout=5000 )
            ret = graph.Connect( A, (rel,M_INT,val), B )
        finally:
            graph.CloseAll()
        return ret



    def get_neighbor_params():
        a = random.sample( R, 1 )[0]
        return b"anchor=V%d&cpukill=50000" % (a)


    def neighbor_plugin( request, graph, anchor:str, cpukill:int ):
        """
        Neighborhood query
        """
        try:
            A = graph.OpenVertex( anchor, mode="r", timeout=2000 )
            ret = graph.Neighborhood( id=A, arc=D_OUT, sortby=S_VAL, fields=F_VAL, hits=100, filter="cpukill(%d)" % cpukill, timeout=2000 )
        except Exception as ex:
            ret = []
            print(ex)
        finally:
            graph.CloseAll()
        return ret



    # Add two plugins
    system.AddPlugin( plugin=connect_plugin, name="connect", graph=graph )
    system.AddPlugin( plugin=neighbor_plugin, name="neighbor", graph=graph )


    # Prime the graph with data
    for n in R:
        graph.CreateVertex( "V%d" % n )
    for n in range( 2000000 ):
        a,b,r = random.sample( R, 3 )
        A, B = graph.OpenVertices( [ "V%d"%a, "V%d"%b ] )
        graph.Connect( A, ("to",M_INT,r), B )
        graph.CloseVertices( [A,B] )
    graph.CloseAll()


    LogInfo( "Initial graph: {}".format( graph ) )

    # Fill graph with more data
    run_load( "connect", get_connect_params, duration=20, initial_nworkers=4, nsock_per_worker=2 )

    LogInfo( "Updated graph: {}".format( graph ) )

    # Run queries
    run_load( "neighbor", get_neighbor_params, duration=60, initial_nworkers=1, inc_nworkers_interval=5, nsock_per_worker=2 )

    # Remove two plugins
    system.RemovePlugin( "connect" )   
    system.RemovePlugin( "neighbor" )   

    graph.Truncate()

 



###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
