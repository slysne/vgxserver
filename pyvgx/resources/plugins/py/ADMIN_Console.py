###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Console.py
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

import pyvgx
import re


sysplugin__RemotePyvgxMethods   = dict([('pyvgx.{}'.format(x),  getattr(pyvgx,        x)) for x in dir(pyvgx)        if not x.startswith('__') and callable(getattr(pyvgx,        x ))])
sysplugin__RemoteGraphMethods   = dict([('Graph.{}'.format(x),  getattr(pyvgx.Graph,  x)) for x in dir(pyvgx.Graph)  if not x.startswith('__') and callable(getattr(pyvgx.Graph,  x ))])
sysplugin__RemoteVertexMethods  = dict([('Vertex.{}'.format(x), getattr(pyvgx.Vertex, x)) for x in dir(pyvgx.Vertex) if not x.startswith('__') and callable(getattr(pyvgx.Vertex, x ))])
sysplugin__RemoteOpMethods      = dict([('op.{}'.format(x),     getattr(pyvgx.op,     x)) for x in dir(pyvgx.op)     if not x.startswith('__') and callable(getattr(pyvgx.op,     x ))])
sysplugin__RemoteSystemMethods  = dict([('system.{}'.format(x), getattr(pyvgx.system, x)) for x in dir(pyvgx.system) if not x.startswith('__') and callable(getattr(pyvgx.system, x ))])

sysplugin__RemoteVertexWritableMethods = set([
    'Vertex.SetRank',
    'Vertex.SetProperty',
    'Vertex.IncProperty',
    'Vertex.RemoveProperty',
    'Vertex.SetProperties',
    'Vertex.RemoveProperties',
    'Vertex.SetVector',
    'Vertex.RemoveVector',
    'Vertex.SetExpiration',
    'Vertex.ClearExpiration',
    'Vertex.SetType'
])

sysplugin__RemoteForbiddenMethods = {
    'pyvgx': ( sysplugin__RemotePyvgxMethods, [
        'threadid',         # no remote value
        'threadlabel',      # no remote value
        'selftest',         # breaks system
        'selftest_all',     # breaks system
        'enable_selftest',  # breaks system
        'SetOutputStream',  # may corrupt logs
        'AutoArcTimestamps' # may have unintended consequences
    ]),
    'Graph': ( sysplugin__RemoteGraphMethods, [
        'Memory',                   # no remote value
        'NewArcsQuery',             # no remote value
        'NewVerticesQuery',         # no remote value
        'NewNeighborhoodQuery',     # no remote value
        'NewAdjacencyQuery',        # no remote value
        'NewAggregatorQuery',       # no remote value
        'Search',                   # stdout
        'Erase',                    # breaks system
        'ShowVertex',               # stdout
        'ShowOpenVertices',         # stdout
        'DebugPrintAllocators',     # stdout
        'DebugDumpGraph',           # stdout
        'Lock'                      # may have unintended consequences
    ]),
    'Vertex': ( sysplugin__RemoteVertexMethods, [
        'Writable',     # no remote value
        'Readable',     # no remote value
        'Readonly',     # no remote value
        'Close',        # no remote value
        'Escalate',     # no remote value
        'Relax',        # no remote value
        'Commit',       # no remote value
        'DebugVector',  # stdout
        'Debug'         # stdout
    ]),
    'op': ( sysplugin__RemoteOpMethods, [
        'URI',              # no remote value
        'SetDefaultURIs',   # no remote value
        'GetDefaultURIs'    # no remote value
    ]),
    'system': ( sysplugin__RemoteSystemMethods, [
        'IsInitialized',    # no remote value
        'Initialize',       # no remote value
        'Unload',           # breaks system
        'DeleteGraph',      # breaks system
        'StartHTTP',        # may break system
        'StopHTTP',         # breaks system
        'AddPlugin',        # security risk
        'RemovePlugin',     # breaks system
        'RunServer'         # may break system
    ])
}


for api, methods in sysplugin__RemoteForbiddenMethods.items():
    method_dict, method_names = methods
    for name in method_names:
        method_dict.pop( "%s.%s" % (api, name) )


sysplugin__RemoteStaticMethods = {}
sysplugin__RemoteStaticMethods.update( sysplugin__RemotePyvgxMethods )
sysplugin__RemoteStaticMethods.update( sysplugin__RemoteOpMethods )
sysplugin__RemoteStaticMethods.update( sysplugin__RemoteSystemMethods )

sysplugin__RemoteAllMethods = {}
sysplugin__RemoteAllMethods.update( sysplugin__RemoteStaticMethods )
sysplugin__RemoteAllMethods.update( sysplugin__RemoteGraphMethods )
sysplugin__RemoteAllMethods.update( sysplugin__RemoteVertexMethods )





###############################################################################
# sysplugin__ParseRemoteCommand
#
###############################################################################
def sysplugin__ParseRemoteCommand( command, recursion=0 ):
    """
    """
    m = re.match( r'^(\S+)\s*:\s*(.*)$', command.replace( '\n', ' ' ) )
    if m:
        return m.group(1), sysplugin__VALIDATOR.SafeEval( m.group(2) )
    elif recursion == 0:
        if "=" in command:
            tmp = re.sub( r'(\w+)\s*=', lambda m:'"%s": ' % m.group(1), command )
            conv = re.sub( r"^(\S+)\s*\((.*)(\)\s*)$", r"\1: {\2}", tmp )
        else:
            conv = re.sub( r'^(\S+)\s*\((.*)(\)\s*)$', r'\1: [\2]', command )
        return sysplugin__ParseRemoteCommand( conv, 1 )
    else:
        raise Exception( "Invalid command" )




###############################################################################
# sysplugin__RemoteCallVertexMethod
#
###############################################################################
def sysplugin__RemoteCallVertexMethod( g, cmd, params ):
    """
    """
    V = None 
    try:
        vcall = sysplugin__RemoteVertexMethods.get( cmd )
        if cmd in sysplugin__RemoteVertexWritableMethods:
            mode = 'w'
        else:
            mode = 'r'
        if type(params) is dict:
            V = g.OpenVertex( id=params.pop( 'id', None ), mode=mode, timeout=params.pop( 'timeout', 0 ) )
            return vcall( V, **params )
        else:
            id = params.pop(0)
            V = g.OpenVertex( id=id, mode=mode, timeout=100 )
            return vcall( V, *params )
    finally:
        if V is not None:
            g.CloseVertex( V )




###############################################################################
# sysplugin__ADMIN_Console
#
###############################################################################
def sysplugin__ADMIN_Console( request:pyvgx.PluginRequest, headers:dict, authtoken:str, graph:str, content:str ):
    """
    ADMIN: Console access to pyvgx API
    """
    # Special case
    if re.search( r"^\s*vgxadmin", content ):
        try:
            sysplugin__LogAdminOperation( headers, opdata=content.strip() )
            args = content.split()[1:]
            result = pyvgx.VGXAdmin.Run( arguments=args, program=None, print_to_stdout=False )
        except Exception as err:
            result = str(err)
        return { 'action': 'vgxadmin', 'result': result }

    # Normal case
    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    g, close = sysplugin__GetGraphObject( graph )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.LogWarning( "ADMIN_Console: graph=%s action=%s" % (graph, content) )

        lines = [line.strip().removesuffix(";") for line in re.split( r';\s*\n', content ) if line.strip()]
        actions = []
        result = []

        for line in lines:
            cmd, params = sysplugin__ParseRemoteCommand( line )
            ret = None
            while True:
                # Graph
                gcall = sysplugin__RemoteGraphMethods.get( cmd )
                if callable( gcall ):
                    if type(params) is dict:
                        ret = gcall( g, **params )
                    else:
                        ret = gcall( g, *params )
                    break
                # Vertex
                if cmd in sysplugin__RemoteVertexMethods:
                    ret = sysplugin__RemoteCallVertexMethod( g, cmd, params )
                    break
                # Static
                scall = sysplugin__RemoteStaticMethods.get( cmd )
                if callable( scall ):
                    if type(params) is dict:
                        ret = scall( **params )
                    else:
                        ret = scall( *params )
                    break
                # dir
                if cmd == "dir":
                    ret = {
                        "Graph":   sorted( sysplugin__RemoteGraphMethods.keys() ),
                        "Vertex":  sorted( sysplugin__RemoteVertexMethods.keys() ),
                        "op":      sorted( sysplugin__RemoteOpMethods.keys() ),
                        "system":  sorted( sysplugin__RemoteSystemMethods.keys() ),
                        "pyvgx":   sorted( sysplugin__RemotePyvgxMethods.keys() )
                    }
                    break
                # python
                if cmd == "python":
                    ret = params
                    break

                raise Exception( "Unknown command: %s" % cmd ) 
            actions.append( cmd )
            result.append( ret )

        return { 'action': actions, 'result': result }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Console )
