###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Persist.py
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
import threading
import time


###############################################################################
# sysplugin__ADMIN_Persist
#
###############################################################################
def sysplugin__ADMIN_Persist( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Create local snapshot
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    progress = []

    def perform():
        len(progress)
        try:
            pyvgx.system.Persist( timeout=5000, force=True )
            progress.append( (True, None) )
        except Exception as perr:
            progress.append( (False, perr) )
        

    sysplugin__BeginAdmin( authtoken )
    try:
        Saver = threading.Thread( target=perform )
        Saver.start()
        t0 = time.time()
        while len( progress ) == 0 and time.time() - t0 < 6.0:
            time.sleep( 1 )
        if len( progress ) > 0:
            if progress[0][0] == False:
                status = str( progress[0][1] )
            else:
                status = "complete"
        else:
            status = "in progress"

        return { 'action': 'persist', 'status': status }
        
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )


pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Persist )
