###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Sync.py
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
import threading
import time
import json


###############################################################################
# sysplugin__ADMIN_Sync
#
###############################################################################
def sysplugin__ADMIN_Sync( request:pyvgx.PluginRequest, headers:dict, authtoken:str, mode:str="repair" ):
    """
    ADMIN: Synchronize subscribers
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    progress = []

    def perform( mode, SubscribersGoal, SubscribersSync, resumeTxIn ):
        len(progress)
        try:
            if mode == "repair":
                __internal__ADMIN_UpdateSubscribersExecute( SubscribersGoal, SubscribersSync, authtoken )
            elif mode == "hard":
                pyvgx.system.Sync( hard=True )
            elif mode == "soft":
                pyvgx.system.Sync()
            else:
                raise ValueError( "mode must be 'repair', 'hard', or 'soft' (got '{}')".format( mode ) )
            # Done
            progress.append( (True, None) )
        except Exception as perr:
            progress.append( (False, perr) )
        finally:
             # Perform
            if resumeTxIn:
                try:
                    pyvgx.op.ResumeTxInput()
                except:
                    pass
       
    
    sysplugin__BeginAdmin( authtoken )
    try:
        nRO = pyvgx.system.CountReadonly()
        if nRO != 0:
            raise Exception( "Cannot sync while {} graph(s) are readonly".format( nRO ) )

        TxInputSuspended = pyvgx.op.SuspendTxInput()


        if mode == "repair":
            server_ports = pyvgx.system.ServerPorts()
            admin_port = server_ports.get( "admin" )
            response, headers = sysplugin__SendAdminRequest( host="127.0.0.1", port=admin_port, path="/vgx/peerstat" )
            dest = []
            for uri, details in response["subscribers"]:
                host = details.get( "host" )
                adminport = details.get( "adminport" )
                dest.append( "{}:{}".format( host, adminport ) )
            goal = {
                'control': { 'repair': True },
                'destinations': dest
            }
            goalJSON = json.dumps( goal )
            SubscribersGoal, SubscribersSync = __internal__ADMIN_UpdateSubscribersGetGoalAndSyncLists( goalJSON )
        else:
            SubscribersGoal, SubscribersSync = (None, None)


        Syncer = threading.Thread( target=perform, args=(mode, SubscribersGoal, SubscribersSync, TxInputSuspended)  )
        Syncer.start()

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

        return { 'action': 'sync', 'mode': mode, 'status': status }
        
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Sync )
