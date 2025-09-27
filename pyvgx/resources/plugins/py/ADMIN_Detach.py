###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Detach.py
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
# sysplugin__ADMIN_Detach
#
###############################################################################
def sysplugin__ADMIN_Detach( request:pyvgx.PluginRequest, headers:dict, authtoken:str, uri:str ):
    """
    ADMIN: Detach a subscriber
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    TxInputSuspended = False
    sysplugin__BeginAdmin( authtoken )
    try:
        TxInputSuspended = pyvgx.op.SuspendTxInput();

        detached = pyvgx.op.Detach( uri, force=True )
        if detached < 1:
            raise Exception( "Cannot detach subscriber (input server running?)" )

        return { 'subscriber':uri, 'action':'detached' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )
        if TxInputSuspended:
            pyvgx.op.ResumeTxInput()


pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Detach )
