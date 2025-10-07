###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Throttle.py
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
# sysplugin__ADMIN_Throttle
#
###############################################################################
def sysplugin__ADMIN_Throttle( request:pyvgx.PluginRequest, headers:dict, authtoken:str, rate:float=-1.0, unit:str="bytes" ):
    """
    ADMIN: Throttle TX input

    rate : Limit TX input processing to units per second
    unit : bytes, opcodes, operations, transactions
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        if rate < 0.0:
            current_limits = pyvgx.op.Throttle()
        else:
            current_limits = pyvgx.op.Throttle( rate=rate, unit=unit )
        return { 'action': 'throttle', 'limits':current_limits }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Throttle )
