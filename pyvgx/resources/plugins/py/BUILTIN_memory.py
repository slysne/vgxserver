###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_memory.py
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
# sysplugin__memory
#
###############################################################################
def sysplugin__memory( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str ):
    """
    Memory usage for graph
    """

    if graph == "*":
        response = {}
        for name in pyvgx.system.Registry():
            response[name] = sysplugin__memory( request, headers, content, name )
        return response
    else:
        g, close = sysplugin__GetGraphObject( graph )

    try:
        return g.GetMemoryUsage()
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__memory )
