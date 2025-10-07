###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_ResumeTxInput.py
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
# sysplugin__ADMIN_ResumeTxInput
#
###############################################################################
def sysplugin__ADMIN_ResumeTxInput( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Resume Transaction Input
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        nRO = pyvgx.system.CountReadonly()
        if nRO != 0:
            raise Exception( "Cannot enable transaction input with {} readonly graph(s)".format( nRO ) )
        pyvgx.op.ResumeTxInput()
        return { 'action': 'tx_input_resumed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ResumeTxInput )
