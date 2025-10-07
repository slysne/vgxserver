###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_matrix.py
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

def sysplugin__matrix__engine_objects( request:pyvgx.PluginRequest ) -> pyvgx.PluginResponse:
    """
    Engine object count
    """
    OBJ = {}
    for name in pyvgx.system.Registry():
        try:
            g = pyvgx.Graph(name)
        except:
            try:
                g = pyvgx.system.GetGraph(name)
            except:
                continue
        for key, val in g.objcnt.items():
            if key not in OBJ:
                OBJ[key] = 0
            OBJ[key] += val
    response = pyvgx.PluginResponse()
    response.Append( OBJ )
    return response



def sysplugin__matrix__pre_objects( request:pyvgx.PluginRequest ) -> pyvgx.PluginRequest:
    """
    Dispatcher object count pre processor
    """
    request.primary = 1
    return request




###############################################################################
# sysplugin__matrix__post_objects
#
###############################################################################
def sysplugin__matrix__post_objects( response:pyvgx.PluginResponse ):
    """
    Dispatcher object count post processor
    """
    AGGR = {}
    for pos, OBJ in response:
        for key, val in OBJ.items():
            if key not in AGGR:
                AGGR[key] = 0
            AGGR[key] += val
    if response.toplevel:
        aggr_response = {
            'level' : response.level,
            'objects' : AGGR
        }
    else:
        aggr_response = pyvgx.PluginResponse()
        aggr_response.Append( AGGR )
    return aggr_response




def sysplugin__matrix__engine_identify( request:pyvgx.PluginRequest ) -> pyvgx.PluginResponse:
    """
    Engine identify
    """
    response = pyvgx.PluginResponse()
    host = pyvgx.system.ServerHost()
    port = pyvgx.system.ServerPorts().get('base')
    identity = {
        'ident'     : str(request.ident),
        'address'   : "{}:{}".format( host.get('ip'), port ),
        'hostname'  : host.get('host'),
        'registry'  : pyvgx.system.Registry()
    }
    response.Append( request.partition, identity )
    return response



def sysplugin__matrix__pre_identify( request:pyvgx.PluginRequest ) -> pyvgx.PluginRequest:
    """
    Dispatcher identify pre processor
    """
    request.primary = 1
    return request




###############################################################################
# sysplugin__matrix__post_identify
#
###############################################################################
def sysplugin__matrix__post_identify( response:pyvgx.PluginResponse ):
    """
    Dispatcher identify post processor
    """
    host = pyvgx.system.ServerHost()
    port = pyvgx.system.ServerPorts().get('base')
    identity = {
        'ident'     : str(response.ident),
        'address'   : "{}:{}".format( host.get('ip'), port ),
        'hostname'  : host.get('host'),
        'matrix'    : response.entries
    }
    if response.toplevel:
        return identity
    else:
        dispatcher = pyvgx.PluginResponse()
        dispatcher.Append( response.partition, identity )
        return dispatcher







###############################################################################
# __matrix__AddDispatcherPlugins
#
###############################################################################
def __matrix__AddDispatcherPlugins():
    """
    """
    pyvgx.system.AddPlugin( name = "sysplugin__MatrixObjects", pre=sysplugin__matrix__pre_objects,  post=sysplugin__matrix__post_objects )
    pyvgx.system.AddPlugin( name = "sysplugin__Identify",      pre=sysplugin__matrix__pre_identify, post=sysplugin__matrix__post_identify )




###############################################################################
# __matrix__AddEnginePlugins
#
###############################################################################
def __matrix__AddEnginePlugins():
    """
    """
    pyvgx.system.AddPlugin( name = "sysplugin__MatrixObjects", plugin=sysplugin__matrix__engine_objects )
    pyvgx.system.AddPlugin( name = "sysplugin__Identify",      plugin=sysplugin__matrix__engine_identify )




###############################################################################
# __matrix__RemovePlugins
#
###############################################################################
def __matrix__RemovePlugins():
    """
    """
    plugins = [
        "sysplugin__MatrixObjects",
        "sysplugin__Identify"
    ]
    for plugin in plugins:
        try:
            pyvgx.system.RemovePlugin( plugin )
        except:
            pass



pyvgx.__matrix__AddDispatcherPlugins = __matrix__AddDispatcherPlugins
pyvgx.__matrix__AddEnginePlugins = __matrix__AddEnginePlugins
pyvgx.__matrix__RemovePlugins = __matrix__RemovePlugins
