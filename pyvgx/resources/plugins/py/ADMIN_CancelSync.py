###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_CancelSync.py
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
# sysplugin__ADMIN_CancelSync
#
###############################################################################
def sysplugin__ADMIN_CancelSync( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Cancel Synchronize subscribers
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    # NOTE: This operation is allowed even when another admin operation is running (e.g. Sync)
    try:

        sync_stopped = pyvgx.system.CancelSync()
        if sync_stopped > 0:
            status = "sync stopped"
        elif sync_stopped == 0:
            status = "sync not running"
        else:
            status = "failed to stop"

        return { 'action': 'cancel_sync', 'status': status }
        
    except Exception as err:
        return err
    finally:
        pass

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_CancelSync )
