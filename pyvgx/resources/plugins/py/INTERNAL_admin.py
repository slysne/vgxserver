###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    INTERNAL_admin.py
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

import pyvgx
import ast
import re
import json
import time
import random
import inspect
import urllib.request
import threading
import http.client
import socket


__ADMIN_DISABLE_AUTH_PROP = "ADMIN_DisableAuth"
__SYSTEM_DESCRIPTOR_PROP = "SYSTEM_Descriptor"
__SYSTEM_DESCRIPTOR_IDENT_PROP = "SYSTEM_DescriptorIdent"

__SHARED_EVAL_MEMORY = {}
__ADMIN_AUTH_TOKENS = {}
__ADMIN_IN_PROGRESS_TOKEN = None
__ADMIN_IN_PROGRESS_RECURSION = 0
__PREVIOUS_AUTH_TOKENS = []
__HTTP_CONNECTIONS = {}

__ADMIN_LOCK = threading.RLock()



def sysplugin__OnServerStartup():
    # Set default descriptor if not set
    D = pyvgx.system.GetProperty( __SYSTEM_DESCRIPTOR_PROP, None )
    ident = pyvgx.system.GetProperty( __SYSTEM_DESCRIPTOR_IDENT_PROP, None )
    # Handle legacy descriptor, transform in-place
    sysplugin__TransformLegacyDescriptor( D )
    default_name = "System Default ({})".format(pyvgx.system.Root())
    default_ident = "G1"
    if D is None or D.get("name") == default_name:
        hostname = socket.gethostname()
        try:
            ip = socket.gethostbyname( hostname )
        except Exception as ex:
            pyvgx.LogError( "Failed to resolve IP for {} ({})".format( hostname, ex ) )
            ip = socket.gethostbyname( "127.0.0.1" )
            pyvgx.LogWarning( "Using fallback IP: {}".format( ip ) )
        ports = pyvgx.system.ServerPorts()
        bound = pyvgx.op.Bound()
        D = {
            "name": default_name,
            "graphs": [],
            "instances": {
                default_ident: {
                    "graph":        None,
                    "group":        1.1,
                    "type":         "generic",
                    "host":         ip,
                    "hport":        ports.get("base",0),
                    "tport":        bound[0],
                    "durable":      bound[1],
                    "description":  "Generic VGX instance"
                }
            },
            "topology": {
                "transaction": {
                    default_ident: {}
                },
                "dispatch": {
                    default_ident: {}
                }
            }
        }
        pyvgx.system.SetProperty( __SYSTEM_DESCRIPTOR_PROP, D )
        pyvgx.system.SetProperty( __SYSTEM_DESCRIPTOR_IDENT_PROP, default_ident )



def sysplugin__GetGraphMemory( graph, size, shared=False ):
    if shared:
        key = '%s:%d' % (graph.name, size)
        M = __SHARED_EVAL_MEMORY.get( key )
        if M is None:
            M = graph.Memory( size )
            __SHARED_EVAL_MEMORY[ key ] = M
    else:
        M = graph.Memory( size )
    return M



def sysplugin__GenerateNextAuthToken( client_uri ):
    """
    Generate a new admin authtoken for this client
    """
    VALID_SECONDS = 300
    host = client_uri.split(':')[1]
    previous = __ADMIN_AUTH_TOKENS.get( host )
    if previous is None:
        X = "".join([chr(x) for x in random.sample(  range(33,128), 64 )])
        Y = pyvgx.sha256( X )
        for n in range( 10 ):
            time.sleep( random.random() / 4 )
            t1 = time.time()
            random.seed( t1 )
            X = "".join([chr(x) for x in random.sample(  range(33,128), 64 )])
            Y = pyvgx.sha256( "%s %s %s" % (X, Y, host) )
        seed = Y
    else:
        seed = previous[0]
    t0 = time.time()
    random.seed( t0 )
    tx = t0 + VALID_SECONDS
    token = pyvgx.sha256( '%s_%.20f_%.20f_%s' % (seed, t0, random.random(), host) )
    __PREVIOUS_AUTH_TOKENS.append( seed )
    while len(__PREVIOUS_AUTH_TOKENS) > 5:
        __PREVIOUS_AUTH_TOKENS.pop(0)
    __ADMIN_AUTH_TOKENS[ host ] = [token, tx]
    for key in list(__ADMIN_AUTH_TOKENS.keys()):
        if t0 > __ADMIN_AUTH_TOKENS[ key ][1]:
            del __ADMIN_AUTH_TOKENS[ key ]
    return (token, t0, tx)



def sysplugin__GetPreviousAuthToken():
    """
    Return previous authtoken
    """
    if len(__PREVIOUS_AUTH_TOKENS) > 0:
        return __PREVIOUS_AUTH_TOKENS[-1]
    else:
        return None
 


def sysplugin__ValidateAndConsumeAuthToken( client_uri, token ):
    host = client_uri.split(':')[1]
    current = __ADMIN_AUTH_TOKENS.get( host )
    if current is None:
        raise PermissionError( 'Not authorized' )
    auth = current[0]
    tx = current[1]
    now = time.time()
    if token == auth:
        if now < tx:
            current[1] = 0.0
        else:
            raise PermissionError( 'Expired authtoken' )
    else:
        raise PermissionError( 'Invalid authtoken' )



def sysplugin__LogAdminOperation( headers, opdata="unknown" ):
    K = dict([(k.lower(),k) for k in headers.keys()])
    host = headers.get( K.get("host") )
    if host is None:
        host = headers.get( K.get("x-vgx-builtin-client"), "unknown" )
    useragent = headers.get( K.get("user-agent"), "unknown" )
    pyvgx.LogInfo( 'ADMIN: op:"{}" origin:"{}" user-agent:"{}"'.format( opdata.replace('"', r'\"'), host, useragent ) )



def sysplugin__AuthorizeAdminOperation( headers, authtoken ):
    if pyvgx.system.GetProperty( __ADMIN_DISABLE_AUTH_PROP, None ) != 1:
        client_uri = headers.get( 'X-Vgx-Builtin-Client' )
        sysplugin__ValidateAndConsumeAuthToken( client_uri, authtoken )
        try:
            caller = inspect.stack()[1].function
            caller = re.sub( "^sysplugin__", "", caller )
        except:
            caller = "?"
        sysplugin__LogAdminOperation( headers, caller )



def sysplugin__BeginAdmin( authtoken ):
    global __ADMIN_IN_PROGRESS_TOKEN
    global __ADMIN_IN_PROGRESS_RECURSION
    if not __ADMIN_LOCK.acquire( timeout=1.0 ):
        raise Exception( "Internal admin error" )
    try:
        if __ADMIN_IN_PROGRESS_TOKEN is None:
            __ADMIN_IN_PROGRESS_RECURSION = 0
            __ADMIN_IN_PROGRESS_TOKEN = authtoken
        elif __ADMIN_IN_PROGRESS_TOKEN != authtoken:
            raise Exception( "Another admin operation is currently in progress" )
        __ADMIN_IN_PROGRESS_RECURSION += 1
    finally:
        __ADMIN_LOCK.release()



def sysplugin__EndAdmin( authtoken ):
    global __ADMIN_IN_PROGRESS_TOKEN
    global __ADMIN_IN_PROGRESS_RECURSION
    if not __ADMIN_LOCK.acquire( timeout=10.0 ):
        raise Exception( "Internal admin error (admin deadlock!)" )
    try:
        if __ADMIN_IN_PROGRESS_TOKEN == authtoken:
            __ADMIN_IN_PROGRESS_RECURSION -= 1
        if __ADMIN_IN_PROGRESS_RECURSION < 1:
            __ADMIN_IN_PROGRESS_TOKEN = None
    finally:
        __ADMIN_LOCK.release()



def sysplugin__CloseHTTPConnection( host, port ):
    key = "{}:{}:{}".format( host, port, threading.get_ident() )
    _, conn = __HTTP_CONNECTIONS.pop( key, (None,None) )
    if conn is not None:
        conn.close()



def sysplugin__CleanupHTTPConnections():
    # Too many connections
    CLEANUP_THRESHOLD = 64
    if len(__HTTP_CONNECTIONS) > CLEANUP_THRESHOLD:
        CLEANUP_COUNT = 16 # Clean up 1/4 of old connections
        for key, entry in sorted( __HTTP_CONNECTIONS.items(), key=lambda x:x[1][0] )[:CLEANUP_COUNT]:
            _, conn = __HTTP_CONNECTIONS.pop( key, (None,None) )
            if conn is not None:
                conn.close()



def sysplugin__NewHTTPConnection( host, port, timeout=4.0 ):
    try:
        sysplugin__CleanupHTTPConnections()
    except Exception as err:
        pyvgx.LogError( "ADMIN HTTPConnection cleanup failed: {}".format(err) )
    key = "{}:{}:{}".format( host, port, threading.get_ident() )
    _, conn = __HTTP_CONNECTIONS.get( key, (None,None) )
    if conn is None:
        conn = http.client.HTTPConnection( host, port, timeout=timeout )
        __HTTP_CONNECTIONS[key] = time.time(), conn
    return conn



def sysplugin__SendAdminRequest( host, port, path, headers={}, timeout=4.0 ):
    attempts = 3
    while attempts > 0:
        try:
            conn = sysplugin__NewHTTPConnection( host, port, timeout=timeout )
            conn.request( "GET", path, headers=headers )
            response = conn.getresponse()
            data = response.read()
            headers = {}
            for k,v in response.getheaders():
                headers[ k.lower() ] = v
            if headers.get("content-type","").startswith("application/json;"):
                data = json.loads( data )
                data = data.get("response",data)
            if response.status != 200:
                conn.close()
            return (data, headers)
        except (http.client.NotConnected, http.client.ImproperConnectionState) as cex:
            conn.close()
            conn.connect()
            attempts -= 1
        except:
            sysplugin__CloseHTTPConnection( host, port )
            raise
        time.sleep(0.25)



def sysplugin__SetSystemDescriptor( descriptor, ident=None ):
    """
    Update system descriptor and optionally set current instance identifier
    """
    if ident is not None:
        pass
    # Validate
    D = sysplugin__ValidateSystemDescriptor( descriptor )
    # Make sure dict is instantiable
    descriptor = pyvgx.Descriptor( D )
    # Verify current instance valid
    if ident is not None:
        instance = descriptor.Get( ident )
        pyvgx.system.SetProperty( __SYSTEM_DESCRIPTOR_IDENT_PROP, ident )
    pyvgx.system.SetProperty( __SYSTEM_DESCRIPTOR_PROP, D )



def sysplugin__GetSystemDescriptor():
    """
    Return system descriptor
    """
    descriptor = pyvgx.system.GetProperty( __SYSTEM_DESCRIPTOR_PROP, None )
    if descriptor is None:
        raise Exception( "Missing system descriptor '{}'".format( __SYSTEM_DESCRIPTOR_PROP ) )
    # Handle legacy descriptor, transform in-place
    sysplugin__TransformLegacyDescriptor( descriptor )
    return descriptor



def sysplugin__GetSystemIdent():
    """
    Return system descriptor current instance ident
    """
    ident = pyvgx.system.GetProperty( __SYSTEM_DESCRIPTOR_IDENT_PROP, None )
    return ident
