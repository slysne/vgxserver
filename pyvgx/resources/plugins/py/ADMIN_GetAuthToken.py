###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_GetAuthToken.py
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

def sysplugin__ADMIN_GetAuthToken( request:pyvgx.PluginRequest, headers:dict ):
    """
    ADMIN: Get a new token to allow one admin operation
    """
    
    client_uri = headers.get( 'X-Vgx-Builtin-Client' )
    token, t0, tx = sysplugin__GenerateNextAuthToken( client_uri )
    return {
        "authtoken" : token,
        "t0"        : t0,
        "tx"        : tx
    }

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_GetAuthToken )
