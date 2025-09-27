###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Subscribe.py
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


###############################################################################
# sysplugin__ADMIN_Subscribe
#
###############################################################################
def sysplugin__ADMIN_Subscribe( request:pyvgx.PluginRequest, headers:dict, authtoken:str, uri:str, hardsync:int=0, timeout:int=30000):
    """
    ADMIN: Subscribe to provider
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        match = re.match( r"vgx://([^:]+):(\d+)", uri )
        if match is None:
            raise Exception( "Invalid subscriber uri: %s" % uri )
        host = match.group(1)
        port = int( match.group(2) )

        pyvgx.op.Subscribe( address=(host, port), hardsync=hardsync, timeout=timeout )
        return { 'action': 'subscribed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Subscribe )
