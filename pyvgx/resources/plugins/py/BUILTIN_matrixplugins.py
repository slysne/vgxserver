###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_matrixplugins.py
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




###############################################################################
# __id_plug
#
###############################################################################
def __id_plug( path, plug, typ, level ):
    """
    """
    id = "{} [{:04x}]".format( path, 0xff - level )
    P = {}
    P["matrix"] = {
        "location": typ,
        "level": level
    }
    for k,v in plug.items():
        P[k] = v
    return id, P




###############################################################################
# sysplugin__matrixplugins_pre
#
###############################################################################
def sysplugin__matrixplugins_pre( request:pyvgx.PluginRequest ):
    """
    Deep plugin listing
    """
    #
    # Ensure main server port
    #
    if request.port != request.baseport:
        raise Exception( "Server base port {} required for this request, port {} was used".format( request.baseport, request.port ) )
    #
    # We are a dispatcher
    #
    if pyvgx.system.DispatcherConfig() is not None:
        return request
    #
    # We are an engine
    #
    response = pyvgx.PluginResponse( sortby=pyvgx.S_ID )
    if request.partition != 0:
        return response # empty if not first part
    #
    # Engine's first partition
    #
    level = 0
    engine_plugs = [(p['path'],p) for p in pyvgx.system.GetPlugins()]
    for path, plug in sorted( engine_plugs ):
        id, plug = __id_plug( path, plug, "engine", 0 )
        response.Append( id, plug )
    return response




###############################################################################
# sysplugin__matrixplugins_post
#
###############################################################################
def sysplugin__matrixplugins_post( response:pyvgx.PluginResponse ):
    """
    Deep plugin listing
    """
    #
    # Engine (should never be called this way)
    #
    if pyvgx.system.DispatcherConfig() is None:
        return response # ?
    #
    # We only consider the 0th dispatcher partition
    #
    if response.partition != 0:
        return response
    #
    # Dispatcher part 0
    #
    level = response.level
    dplugs = {}
    dispatch_plugs = [(p['path'],p) for p in pyvgx.system.GetPlugins()]
    for path, plug in dispatch_plugs:
        dtype = "top-dispatch" if response.toplevel else "dispatch"
        id, plug = __id_plug( path, plug, dtype, level )
        dplugs[id] = plug
    #
    # Backfill with matrix plugins 
    #
    for id, plug in response.entries:
        dplugs[id] = plug
    #
    # Created new combined response
    #
    matrix_plugs = dplugs.items()
    if response.toplevel:
        top_response = []
        for id, plug in sorted( matrix_plugs ):
            top_response.append( plug )
        return top_response
    else:
        dispatch_response = pyvgx.PluginResponse( sortby=pyvgx.S_ID )
        for id, plug in sorted( matrix_plugs ):
            dispatch_response.Append( id, plug )
        return dispatch_response



pyvgx.system.AddPlugin( name="sysplugin__matrixplugins", pre=sysplugin__matrixplugins_pre, post=sysplugin__matrixplugins_post )
