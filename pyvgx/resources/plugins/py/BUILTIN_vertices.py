###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_vertices.py
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
import re

sysplugin__vertices_EXCLUDE = set(["headers", "content", "graph", "hits", "query", "sharedmem"])

def sysplugin__vertices( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str, hits:int=25, query:str="", sharedmem:int=0 ):
    """
    Global vertices query
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        Q = {
          'timeout': 5000,
          'hits'   : hits,
          'fields' : pyvgx.F_NAMES
        }

        for k,v in request.params.items():
            if k not in sysplugin__vertices_EXCLUDE:
                Q[k] = v

        sysplugin__UpdateQueryDict( Q, query, content )
        if 'memory' in Q:
            Q['memory'] = sysplugin__GetGraphMemory( g, Q['memory'], sharedmem )

        M = Q.get( 'memory' )
        R = g.Vertices( **Q )
        if M is not None:
            if type(R) is not dict:
                R = {'vertices': R}
            R['memory'] = M.AsList()
        return R
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__vertices )
