###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    putload.py
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

import socket
from threads import *
import time
import random
import io
import sys
import os
from threading import Lock
os.environ['PYVGX_NOBANNER'] = "1"
from pyvgx import StringQueue


WORKING = True



###############################################################################
# getbig
#
###############################################################################
def getbig( N ):
    """
    """
    return bytes( (random.randint(0,255) for n in range(N)) )


X = Lock()
nerr = 0


###############################################################################
# Error
#
###############################################################################
def Error( lines ):
    """
    """
    global nerr
    X.acquire()
    try:
        nerr += 1
        data = "\n".join( [line.decode() for line in lines] )
        print("---- {} -----------------".format(nerr))
        print(data)
        print()
    finally:
        X.release()



N=0


tn = 1


###############################################################################
# connect
#
###############################################################################
def connect( host, port ):
    """
    """
    s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    s.connect( (host, port) )
    s.setblocking( False )
    return s


HOST = "10.10.1.115"
PORT1 = 9990
PORT2 = 9990



###############################################################################
# new_state_data
#
###############################################################################
def new_state_data( sock ):
    """
    """
    return [
        sock,           # Socket instance
        StringQueue(),  # Response data queue
        0,              # Parser state 0: INITIAL, 1: HEADERS, 2: CONTENT, 3: COMPLETE
        0               # Initialized to Content-Length header's value, updated to remaining
    ]




###############################################################################
# reset_state_data
#
###############################################################################
def reset_state_data( sdata ):
    """
    """
    s, Q, state, remain = sdata
    s.close()
    port = random.sample([PORT1,PORT2],1)[0]
    s = connect( HOST, port )
    sdata[0] = s        # new open socket
    sdata[1].clear()    # empty queue
    sdata[2] = 0        # INITIAL
    sdata[3] = 0        # remain




###############################################################################
# parse_error
#
###############################################################################
def parse_error( sdata ):
    """
    """
    Q, state, remain = sdata
    Q.unread()
    Error( Q.getvalue() )
    raise Exception()




###############################################################################
# parse_state_complete
#
###############################################################################
def parse_state_complete( sdata ):
    """
    """
    return sdata[2] == 3 # COMPLETE




###############################################################################
# parse_state_reset
#
###############################################################################
def parse_state_reset( sdata ):
    """
    """
    sdata[2] = 0 # INITIAL




###############################################################################
# parse
#
###############################################################################
def parse( sdata ):
    """
    """

    s, Q, state, remain = sdata
    #print(s)

    # EXPECT INITIAL
    if state == 0:
        #print( "INITIAL" )
        # incomplete
        if b"\r\n" not in Q:
            return False
        # not 200
        if not Q.readline().startswith(b'HTTP/1.1 200'):
            parse_error( sdata )
        # Advance state
        state = sdata[2] = 1 # HEADERS
    
    # EXPECT HEADERS
    if state == 1:
        while True:
            #print( "HEADERS" )
            # incomplete
            if b"\r\n" not in Q:
                return False
            header = Q.readline()
            # END of headers
            if header == b'\r\n':
                # Advance state
                state = sdata[2] = 2 # CONTENT
                break
            # Content-Length
            if header.startswith(b'Content-Length:'):
                # Set expected content length
                remain = sdata[3] = int(header[15:])
                #print( "Expecting {} bytes".format(remain) )
    
    # EXPECT CONTENT
    if state == 2 and remain > 0:
        #print( "CONTENT" )
        data = Q.read( remain )
        actual = len(data)
        remain -= actual
        sdata[3] = remain
        # incomplete
        if remain > 0:
            return False

    # Done!
    # Reset state
    sdata[2] = 3 # COMPLETE
    #print( "COMPLETE" )
    return True














###############################################################################
# work
#
###############################################################################
def work( qdata=None ):
    """
    """
    global N
    global tn
    X.acquire()
    random.seed(tn)
    tn += 1
    X.release()
    s1 = None
    s2 = None
    s3 = None
    s4 = None
    s5 = None
    s6 = None
    try:
        s1 = connect( HOST, PORT1 )
        s2 = connect( HOST, PORT2 )
        s3 = connect( HOST, PORT1 )
        s4 = connect( HOST, PORT2 )
        s5 = connect( HOST, PORT1 )
        s6 = connect( HOST, PORT2 )
    except Exception as ex:
        print( ex )
        return

  

    ready = True
    
    sz_JUNK = 10000
    #sz_JUNK = (5<<20)
    JUNK = ("".join([ chr(random.randint(32,127)) for x in range(sz_JUNK)])).encode()
    #REQUEST = b"POST /vgx/plugin/plugin HTTP/1.1\r\nContent-Length: %d\r\n\r\n%s" % (sz_JUNK, JUNK)
    #REQUEST = b"GET /plugin?neighbors&1234 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/neighborhood?id=ROOT&hits=200&sortby=S_RANDOM&result=1342177280&fields=F_AARC&timeout=5000 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/count HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/builtin/neighbor?graph=g1&id=%d&hits=5&sortby=(int)512&timeout=(int)2000 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/neighbor?id=%d HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/ANNSearch?query=__test__%d&hits=100&explode=idlist&mincos=0.5&maxdist=5&maxtime=0.01&cache=0 HTTP/1.1\r\n\r\n"
    REQUEST = b'GET /vgx/plugin/search?name=%d&hits=100&select=* HTTP/1.1\r\nAccept: application/json\r\n\r\n'
    #REQUEST = b"GET /vgx/plugin/count?x=1 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/zzz?x=1000 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/neighborhood?id=ROOT&hits=1000&sortby=S_RANDOM&result=1342177280&fields=F_AARC&filter=cpukill(10)&timeout=5000&neighbor={%20%22traverse%22:{%20%22filter%22:%22cpukill(10000)%22%20}%20} HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/func?id=1000&timeout=(int)1000&hits=(int)25&fields=(int)3845&sortby=(int)16 HTTP/1.1\r\n\r\n"
    #REQUEST = b"GET /vgx/plugin/func HTTP/1.1\r\n\r\n"

    #REQUEST0 = b"GET /vgx/randint HTTP/1.1\r\n\r\n"
    REQUEST0 = b"GET /vgx/plugin/Ping?spin=300000000&sleep=0.0 HTTP/1.1\r\n\r\n"
    REQUEST0_n = b"GET /vgx/randint HTTP/1.1\r\n\r\n"
    REQUEST1_5 = b"GET /vgx/randint HTTP/1.1\r\nContent-Length: 1\r\n\r\nx"*5

    i = 0
    #S = [s1, s2, s3, s4, s5, s6]
    S = [
            new_state_data(s1),
            new_state_data(s2),
            new_state_data(s3),
            new_state_data(s4),
            new_state_data(s5),
            new_state_data(s6)
        ]
    if qdata is not None:
        #rlist = random.sample( qdata, len(qdata) )
        QDATA = iter( qdata )
    else:
        QDATA = None

    while WORKING:
        i += 1
        try:
            try:
                rcnt = 0
                for sdata in S:
                    s = sdata[0]
                    try:
                        #R = REQUEST % bytes([random.randint(97,122) for n in range(5)])
                        #s.sendall( R )
                        #time.sleep(0.1)
                        if QDATA:
                            line = next(QDATA)
                            R = b"GET %s HTTP/1.1\r\nAccept: application/json\r\n\r\n" % next(QDATA)
                        else:
                            #R = b"GET /vgx/plugin/quick HTTP/1.1\r\n\r\n"
                            R = REQUEST % random.randint(1,10000)
                            #R = b"GET /vgx/hc HTTP/1.1\r\n\r\n"
                        s.sendall( R )
                        rcnt += 1
                    except Exception as ex:
                        pass
                    except StopIteration:
                        return
                X.acquire()
                N += rcnt
                X.release()
            except Exception as ex:
                print( "Error from sendall(): {}".format(ex) )
                #print(ex)
                pass
            #time.sleep(5)
            #for r,_ in S:
            #    try:
            #        data = r.recv( 8192 )
            #    except:
            #        pass
            #time.sleep(0.01)

            t0 = time.perf_counter()
            while True:
                done = True
                for sdata in S:
                    if parse_state_complete( sdata ):
                        continue
                    s,Q,_,_ = sdata
                    try:
                        try:
                            data = s.recv( 8192 )
                            #print(data)
                            Q.write( data )
                        except BlockingIOError:
                            done = False
                            continue

                        try:
                            if parse( sdata ) is False:
                                done = False
                        except:
                            reset_state_data( sdata )
                            time.sleep(0.1)

                    except Exception as ex:
                        t1 = time.perf_counter()
                        if t1 - t0 < 5:
                            done = False
                            time.sleep(0.1)
                        else:
                            done = True
                        print(ex)
                        reset_state_data( sdata )
                        #sys.exit(0)
                if done:
                    break
            for sdata in S:
                parse_state_reset( sdata )


        except BlockingIOError as bex:
            print(bex)
            pass
        except Exception as ex:
            print( "Error during recv() {}".format(ex) )





###############################################################################
# main
#
###############################################################################
def main():
    """
    """
    qdata = None
    try:
        init_n = int( sys.argv[1] )
        if len(sys.argv) > 2:
            fname = sys.argv[2]
            f = open( fname, "rb" )
            qdata = []
            for line in f:
                qdata.append( line.strip() )
            f.close()
    except Exception as err:
        init_n = 1

    n_w = 50

    W = []
    for n in range( n_w ):
        W.append( Worker(n) )
      
    for n in range( n_w ):
        W[n].perform( work, qdata )
        if n >= init_n-1:
            input( "%d running. Ramp up?" % (n+1) )
        time.sleep(0.1)

    print( "Full steam! %s" % len(W) )

    #t0 = time.time()
    #while time.time() - t0 < 60:
    #time.sleep(0.5)

    #WORKING = False
    #print( "Stopping load" )

    X.acquire()
    lastN = N
    X.release()
    while True:
        time.sleep(1.0)
        X.acquire()
        xN = N
        X.release()
        print( xN, xN - lastN )
        lastN = xN

    WORKING = False

    time.sleep(1)


if __name__ == "__main__":
    main()
