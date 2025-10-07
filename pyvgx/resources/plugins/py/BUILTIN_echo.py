###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_echo.py
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


###############################################################################
# sysplugin__echo
#
###############################################################################
def sysplugin__echo( request:pyvgx.PluginRequest, headers:dict, method:str, content:bytes ):
    """
    Server echo
    """
    http = {}
    echo = { 'HTTP': http }
    http['method'] = method
    http['request'] = request.params
    H = {}
    for k,v in sorted( headers.items() ):
        try:
            H[k] = v.decode()
        except:
            H[k] = repr(v)
    http['headers'] = H
    try:
        http['content'] = content.decode()
    except:
        http['content'] = repr( content )
    return echo

pyvgx.system.AddPlugin( plugin=sysplugin__echo )
