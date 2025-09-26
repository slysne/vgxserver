###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_createvertex.py
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
import json



###############################################################################
# sysplugin__createvertex
#
###############################################################################
def sysplugin__createvertex( request:pyvgx.PluginRequest, headers:dict, graph:str, id:str, type:str="", lifespan:int=-1, properties:json={} ):
    """
    Create/update vertex
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        r = g.CreateVertex( id=id, type=type, lifespan=lifespan, properties=properties )
        action = "created" if r > 0 else "updated"
        return { 'id':id, 'action':action }
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__createvertex )
