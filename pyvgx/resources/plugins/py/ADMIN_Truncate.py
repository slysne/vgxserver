###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Truncate.py
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

def sysplugin__ADMIN_Truncate( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Delete all graph data
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        tx_suspended = pyvgx.op.SuspendTxInput()
        pyvgx.op.Detach( uri=None, force=True )
        pyvgx.system.ClearReadonly()
        pyvgx.system.SetReadonly()
        pyvgx.system.ClearReadonly()
        if tx_suspended:
            pyvgx.op.ResumeTxInput()
        G = []
        for name, o_s in pyvgx.system.Registry().items():
            try:
                g = pyvgx.Graph( name )
            except:
                g = pyvgx.system.GetGraph( name )
            G.append( g )
        for g in G:
            g.Truncate()

        return { 'action': 'truncate' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Truncate )
