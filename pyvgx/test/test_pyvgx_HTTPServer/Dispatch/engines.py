from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import pprint
import urllib.request
import http.client
import socket
import re
import os
import time
import json
import random
import multiprocessing
from threading import Thread



def redirect_instance_output( instance_id ):
    os.makedirs( instance_id, exist_ok=True )
    logpath = "{}/out.txt".format( instance_id )
    SetOutputStream( logpath )



def get_authtoken( host, port ):
    """
    Return a new authtoken
    """
    bytes, headers = Support.send_request( "vgx/builtin/ADMIN_GetAuthToken", headers={'X-Vgx-Builtin-Min-Executor': 3}, admin=True, json=True, address=(host,port) )
    Support.assert_headers( headers, bytes, "application/json" )
    R = Support.response_from_json( bytes )
    return R['authtoken']



def EngineShutdown( host, port ):
    """
    Send shutdown request
    """
    token1 = get_authtoken( host, port )
    token2 = get_authtoken( host, port )
    Support.send_request( "vgx/builtin/ADMIN_Shutdown?authtoken={}&authshutdown={}".format( token2, token1 ), json=True, admin=True, address=(host,port) )



def WaitUntilEngineReady( host, port, timeout=30.0 ):
    """
    Wait for server engine to start
    """
    # Wait for engine ready
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            R = urllib.request.Request( "http://{}:{}/vgx/hc".format(host, port) )
            U = urllib.request.urlopen( R )
            if U.status == 200:
                return True
        except:
            time.sleep(1)
    return False



N_INIT = 25
N_TERM = 50000



def PopulateServerData( graph ):
    """
    Build server graph with random data
    """
    for i in range( N_INIT ):
        try:
            A = graph.NewVertex( "init_{}".format( i ) )
            for t in range( N_TERM ):
                B = graph.NewVertex( "term_{}".format( t ), type="term" )
                graph.Connect( A, ("to",M_FLT,random.random()), B )
        finally:
            graph.CloseAll()
            A = None
            B = None



def ServerFeedPlugin( request:PluginRequest, graph, id:int, content:bytes ) -> PluginResponse:
    """
    Backend server engine feed plugin
    """
    response = PluginResponse( sortby=S_ODEG )
    try:
        # [ i, [t1,t2,...] ]
        data = json.loads( content )
        init, terms = data
        Expect( id == init, "init==id, got {} != {}".format(init, id) )
        node = "init_{}".format( init )

        try:
            A = graph.NewVertex( node, timeout=1000 )
            for t in terms:
                try:
                    arc = ("to",M_FLT,random.random())
                    B = graph.NewVertex( "term_{}".format( t ), type="term" )
                    graph.Connect( A, arc, B )
                    B.Close()
                except:
                    pass # ignore for simplicity, we're just testing the partial feeding not the algo
            odeg = graph.Degree( node, arc=D_OUT )
            A.Close()
            response.Append( odeg, init )
        except:
            pass # ignore for simplicity, we're just testing the partial feeding not the algo

    except Exception as err:
        print( err )
        response.message = repr(err)
    return response



def DispatchFeedPre( request:PluginRequest, id:int ) -> PluginRequest:
    """
    Dispatcher feed pre-processor
    """
    request.partial = id
    return request



def DispatchFeedPost( response:PluginResponse ) -> PluginResponse:
    """
    Dispatcher feed post-processor
    """
    return response



def ServerSearchPlugin( request:PluginRequest, graph, init:int, hits:int, synload:int=1000 ) -> PluginResponse:
    """
    Backend server engine search plugin
    """
    response = PluginResponse( maxhits=hits, sortby=S_VAL|S_DESC )
    try:
        id = "init_{}".format( init )
        if id in graph:
            R = graph.Neighborhood( id, hits=response.maxhits, sortby=response.sortby, fields=F_AARC, result=R_DICT|R_COUNTS, neighbor={'filter':'cpukill({})'.format(synload)} )
            response.hitcount = R['counts']['arcs']
            for r in R['neighborhood']:
                v = r['arc']['value']
                response.Append( v, r )
    except Exception as err:
        print( err )
        response.message = repr(err)
    return response



def DispatchSearchPre( request:PluginRequest ) -> PluginRequest:
    """
    Dispatcher search pre-processor
    """
    return request



def DispatchSearchPost( response:PluginResponse ) -> PluginResponse:
    """
    Dispatcher search post-processor
    """
    return response



def ServerEntrypoint( port, prefill ):
    """
    Backend server engine process entrypoint
    """
    # Engine instance name
    instance_id = "engine_{}".format(port)
    redirect_instance_output( instance_id )

    # Initialize
    system.Initialize( instance_id )
    g = Graph("g1")

    if prefill:
        PopulateServerData( g )

    # Add engine plugins
    system.AddPlugin( plugin=ServerFeedPlugin, name="feed", graph=g )
    system.AddPlugin( plugin=ServerSearchPlugin, name="search", graph=g )

    # Start server
    system.StartHTTP( port )

    # Run until shutdown
    system.RunServer( "pid={} {}".format(os.getpid(), instance_id) )



def DispatcherEntrypoint( port, cf ):
    """
    Backend dispatcher engine process entrypoint
    """
    # Engine instance name
    instance_id = "dispatch_{}".format(port)
    redirect_instance_output( instance_id )

    # Initialize
    system.Initialize( instance_id )

    # Add disptcher pre/post processors
    system.AddPlugin( pre=DispatchFeedPre, name="feed" )
    system.AddPlugin( pre=DispatchSearchPre, name="search" )
    system.AddPlugin( pre=DispatchSearchPre, post=DispatchSearchPost, name="search" )

    # Start dispatcher
    system.StartHTTP( port, dispatcher=cf )

    # Run until shutdown
    system.RunServer( "pid={} {}".format(os.getpid(), instance_id) )



def GetDispatcherMatrixDimensions( host, port ):
    bytes, headers = Support.send_request( "vgx/status", json=True, address=(host,port) )
    R = json.loads( bytes )
    d = R['response']['dispatcher']
    w = d['matrix-width']
    h = d['matrix-height']
    return w, h



def GetMatrixConfig( width, height, host, ports ):
    R = []
    for h in range(height-1):
       R.append( {'channels':8, 'priority':1} )
    R.append( {'channels':8, 'priority':1, 'primary':1} )

    P = []
    i = 0
    for w in range(width):
        partial = []
        for h in range(height):
            partial.append( {'host':host, 'port':ports[i]} )
            i += 1
        P.append( partial )
    disp_cf = {
        'replicas': R,
        'partitions': P
    }
    return disp_cf



def GetNewConnection( host, port ):
    conn = http.client.HTTPConnection( host, port, timeout=5.0 )
    return conn



def __execute_feed( D, host, port, init_0, init_1, total_terminals ):

    conn = GetNewConnection( host, port)
    try:
        headers = {'accept': 'application/json'}

        N_initials = init_1 - init_0

        TERMS = range( 0, total_terminals-1 )
        sample_size = 200

        degrees = {}

        max_degree = total_terminals * N_initials
        lim_degree = int( 0.75 * max_degree )
        
        # Feed until reasonably full
        while sum( degrees.values() ) < lim_degree:
            # Generate random data to be submitted to one of the partitions
            init = random.randint( init_0, init_1-1 )
            terms = random.sample( TERMS, sample_size )
            content = [ init, terms ]
            body = json.dumps( content )

            # Feed the data
            url = "/vgx/plugin/feed?id={}".format( init )
            conn.request( "POST", url, body=body, headers=headers )
            data = conn.getresponse()
            Expect( data.status == 200, "status 200, got {} {}".format(data.status, data) )

            # Check response
            data = data.read()
            R = json.loads( data )
            response = R['response']
            entries = response['entries']
            if len(entries):
                try:
                    odeg, i = entries[0]
                except:
                    Expect( False, "tuple (odeg, i), got {}".format(entries) )
                Expect( i == init )

                # Keep track of the outdegree of each initial
                degrees[i] = odeg

        D['degrees'] = degrees


    except Exception as err:
        D['error'] = err

    finally:
        conn.close()



def FeedData( host, ports, nthreads=1, delay=5.0 ):
    print( f"FeedData( host={host}, ports={ports}, nthreads={nthreads}, delay={delay} )" )

    if delay > 0.0:
        print( "Starting in {} seconds...".format(delay) )
        time.sleep(5)
        print( "Go!" )

    tdata = [{'worker':n} for n in range(nthreads)]

    total_initials = 1000
    total_terminals = 5000

    T = []
    n_per_thread = total_initials // nthreads
    init_0 = 0
    init_1 = 0
    p = 0
    for D in tdata:
        init_1 += n_per_thread
        port = ports[ p % len(ports) ]
        p += 1
        t = Thread( target=__execute_feed, args=(D,host,port,init_0,init_1,total_terminals) )
        T.append( t )
        init_0 = init_1

    for t in T:
        t.start()

    while [1 for t in T if t.is_alive()]:
        time.sleep(1)

    for t in T:
        t.join()

    for D in tdata:
        Expect( "error" not in D,       "Worker error: {}".format( D ) )

    # Run searches to verify
    headers = {'accept': 'application/json'}
    port = ports[0]
    conn = GetNewConnection( host, port)
    total_hitcount = 0
    for i in range( 0, init_1 ):
        query = "init={}&hits=1".format( i )
        url = "/vgx/plugin/search?{}".format( query )
        conn.request( "GET", url, headers=headers )
        # Run request
        data = conn.getresponse()
        Expect( data.status == 200 )
        data = data.read()
        # Extract response data
        R = json.loads( data )
        response = R['response']
        hitcount = response['hitcount']
        total_hitcount += hitcount

    total_degrees = 0
    for D in tdata:
        total_degrees += sum( D['degrees'].values() )

    Expect( total_hitcount == total_degrees, "Total degree {}, got hitcount {}".format( total_degrees, total_hitcount ) )

    



def RunQueries( host, port, total_count=-1, nthreads=1, quiet=False, synload=1000, delay=5.0 ):
    if total_count < 0:
        width, height = GetDispatcherMatrixDimensions( host, port )
        total_count = width * N_TERM

    print( f"RunQueries( host={host}, port={port}, total_count={total_count}, nthreads={nthreads}, quiet={quiet}, synload={synload}, delay={delay} )" )

    if delay > 0.0:
        print( "Starting in {} seconds...".format(delay) )
        time.sleep(5)
        print( "Go!" )

    def execute( D, host, port, total_count, quiet, synload ):
        conn = GetNewConnection( host, port )
        try:
            headers = {'accept': 'application/json'}
            # Send requests to local dispatcher acting as proxy for backend server engine
            for hits in [0, 1, 2, 5, N_TERM//100, N_TERM//20]:
                if not quiet:
                    LogInfo( "Running queries, hits={}".format(hits) )
                for i in range( N_INIT ):
                    if not quiet:
                        print( ".", end="", flush=True )
                    # Make request
                    query = "init={}&hits={}&synload={}".format( i, hits, synload )
                    url = "/vgx/plugin/search?{}".format( query )
                    try:
                        deadline = time.time() + 10
                        while time.time() < deadline:
                            conn.request( "GET", url, headers=headers )
                            # Run request
                            data = conn.getresponse()
                            if data.status == 200:
                                break
                            elif data.status == 429:
                                continue
                            else:
                                print( data.status, data.read() )
                                break
                        Expect( data.status == 200, "200, got {}".format( data.status ) )
                        data = data.read()
                        # Extract response data
                        R = json.loads( data )
                        response = R['response']
                        hitcount = response['hitcount']
                        entries = response['entries']
                        # Verify response
                        if total_count >= 0:
                            Expect( hitcount == total_count, "total count {}, got {}".format( total_count, hitcount ) )
                        if len(entries) != hits:
                            pprint.pprint( R )
                            Expect( False, "expected {} hits, got {}".format( hits, len(entries) ) )
                        # Verify sort order
                        prev_score = 1.0
                        for score, entry in entries:
                            Expect( score <= prev_score,    "descending order, got {} then {}".format( prev_score, score ) )
                            prev_score = score
                    except (http.client.NotConnected, http.client.ImproperConnectionState, socket.timeout) as cex:
                        conn.close()
                        conn = GetNewConnection( host, port )
                    except Exception as err:
                        Expect( False, "failed to send query: {}".format(err) )
                if not quiet:
                    print()
        except Exception as err:
            D['error'] = err
        finally:
            conn.close()


    tdata = [{'worker':n} for n in range(nthreads)]


    T = [Thread( target=execute, args=(D,host,port,total_count,quiet,synload) ) for D in tdata]
    for t in T:
        t.start()

    while [1 for t in T if t.is_alive()]:
        time.sleep(1)

    for t in T:
        t.join()

    for D in tdata:
        Expect( "error" not in D,       "Worker error: {}".format( D ) )
            



def StartServerEngines( host, ports, prefill=True ):
    """
    Start backend server engines
    Returns: [ (server, host, port), (server, host, port), ... ]
             where server is a multiprocessing.Process instance
    """
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass

    SERVERS = []
    try:
        # Start backend server engines in new processes
        nobanner = os.getenv( "PYVGX_NOBANNER" )
        os.environ["PYVGX_NOBANNER"] = "1"
        for port in ports:
            server = multiprocessing.Process( target=ServerEntrypoint, args=(port,prefill) )
            SERVERS.append( (server, host, port) )
            server.start()
        if nobanner is None:
            del os.environ["PYVGX_NOBANNER"]
        
        # Wait for engines ready
        for server, host, port in SERVERS:
            ready = WaitUntilEngineReady( host, port, timeout=120 )
            Expect( ready,      "Backend server engine failed to start ({}:{})".format(host, port) )

        return SERVERS
    except:
        for server, host, port in SERVERS:
            if server.exitcode is None:
                server.kill()
        raise



def StartDispatcherEngines( host, ports, cf ):
    """
    Start dispatcher engines
    Returns: [ (dispatcher, host, port), (dispatcher, host, port), ... ]
             where dispatcher is a multiprocessing.Process instance
    """
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass

    DISPATCHERS = []
    try:
        # Start backend server engines in new processes
        nobanner = os.getenv( "PYVGX_NOBANNER" )
        os.environ["PYVGX_NOBANNER"] = "1"
        for port in ports:
            dispatcher = multiprocessing.Process( target=DispatcherEntrypoint, args=(port,cf) )
            DISPATCHERS.append( (dispatcher, host, port) )
            dispatcher.start()
        if nobanner is None:
            del os.environ["PYVGX_NOBANNER"]
        
        # Wait for engines ready
        for dispatcher, host, port in DISPATCHERS:
            ready = WaitUntilEngineReady( host, port )
            Expect( ready,      "Backend dispatcher engine failed to start ({}:{})".format(host, port) )

        return DISPATCHERS
    except:
        for dispatcher, host, port in DISPATCHERS:
            if dispatcher.exitcode is None:
                dispatcher.kill()
        raise



def StopEngines( ENGINES ):
    """
    Stop backend server engines previously started with StartXXXEngines()
    ENGINES must be: [ (engine, host, port), (engine, host, port), ... ]
                       where engine is a multiprocessing.Process instance
    """

    try:
        # Shut down backend server engines
        T = [Thread( target=EngineShutdown, args=(host,port) ) for engine, host, port in ENGINES]
        for t in T:
            t.start()

        while [1 for t in T if t.is_alive()]:
            time.sleep(1)

        for t in T:
            t.join()

        # Wait for engines to exit
        for engine, host, port in ENGINES:
            engine.join( timeout=30 )
            Expect( engine.exitcode is not None,    "Backend engine did not stop ({}:{})".format(host, port) )
    except:
        for engine, host, port in ENGINES:
            if engine.exitcode is None:
                engine.kill()
        raise

