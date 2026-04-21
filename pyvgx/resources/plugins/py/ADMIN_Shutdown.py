###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ADMIN_Shutdown.py
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
import sys
import os
import signal


__ADMIN_Shutdown__Undertaker = None


###############################################################################
# __ADMIN_Shutdown__fullreset
#
###############################################################################
def __ADMIN_Shutdown__fullreset():

    try:
        # Detach
        tx_suspended = pyvgx.op.SuspendTxInput()
        pyvgx.op.Detach( uri=None, force=True )
        pyvgx.system.ClearReadonly()
        pyvgx.system.SetReadonly()
        pyvgx.system.ClearReadonly()
        if tx_suspended:
            pyvgx.op.ResumeTxInput()
        # Get all graphs
        G = []
        for name, o_s in pyvgx.system.Registry().items():
            try:
                g = pyvgx.Graph( name )
            except Exception as ex:
                g = pyvgx.system.GetGraph( name )
            G.append( g )
        # Truncate all graphs
        for g in G:
            try:
                g.Truncate()
            except Exception as ex:
                pyvgx.LogError( f"Truncation error ({g}): {ex}" )
        # Persist all truncated graphs
        for g in G:
            try:
                g.Save( force=True )
            except Exception as ex:
                pyvgx.LogError( f"Persist error ({g}): {ex}" )
        # Erase all graph instances
        for g in G:
            try:
                g.Erase()
            except Exception as ex:
                pyvgx.LogError( f"Erase graph error ({g}): {ex}" )
        # Close all graph instances
        for g in G:
            try:
                g.Close()
            except Exception as ex:
                pyvgx.LogError( f"Close graph error ({g}): {ex}" )
        del g
        # Remove everything from registry
        for name, o_s in pyvgx.system.Registry().items():
            try:
                pyvgx.system.DeleteGraph( name )
            except Exception as ex:
                pyvgx.LogError( f"Registry removal error ({g}): {ex}" )
        # Persist empty system
        pyvgx.system.Persist( force=True )
    except Exception as ex:
        pyvgx.LogError( f"Full reset error: {ex}" )



###############################################################################
# __ADMIN_Shutdown__shutdown
#
###############################################################################
def __ADMIN_Shutdown__shutdown( persist=False, restartable=False, fullreset=False ):
    """
    """
    pyvgx.LogInfo( "Final shutdown initiated" )
    time.sleep(2)
    # Stop HTTP
    pyvgx.LogInfo( "Stopping HTTP Server" )
    try:
        pyvgx.system.StopHTTP()
    except:
        pass
    # Full reset
    if fullreset:
        pyvgx.LogInfo( "Full reset before shutdown" )
        __ADMIN_Shutdown__fullreset()
    # Persist
    elif persist:
        pyvgx.LogInfo( "Shutdown persist" )
        try:
            pyvgx.system.Persist( force=True, timeout=30000 )
            time.sleep(2)
        except Exception as err:
            pyvgx.LogError( "Persist error {}".format(err) )
    # Stop in a restartable manner
    if restartable:
        # Ready to stop and maybe restart if application supports it
        for n in range(3):
            pyvgx.LogInfo( "Exit RunServer() in {}...".format(3-n) )
            time.sleep(1)
        pyvgx.LogInfo( "Exit RunServer()" )
        pyvgx.system.ExitRunServer()
    # Terminate process
    else:
        # Unload
        pyvgx.LogInfo( "Unloading" )
        try:
            pyvgx.system.Unload()
        except:
            pass
        # Ready to die
        for n in range(3):
            pyvgx.LogInfo( "SIGTERM in {}...".format(3-n) )
            time.sleep(1)
        pyvgx.LogInfo( "SIGTERM" )
        os.kill( os.getpid(), signal.SIGTERM )




###############################################################################
# sysplugin__ADMIN_Shutdown
#
###############################################################################
def sysplugin__ADMIN_Shutdown( request:pyvgx.PluginRequest, headers:dict, authtoken:str, authshutdown:str, persist:int=0, restartable:int=0, authfullreset:str="" ):
    """
    ADMIN: Shutdown
    """
    global __ADMIN_Shutdown__Undertaker

    if authshutdown != sysplugin__GetPreviousAuthToken():
        raise Exception( "Invalid authshutdown token" )

    if len(authfullreset) > 0 and authfullreset != authshutdown:
        raise Exception( "Invalid authfullreset token" )

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    progress = []

    def prepare():
        pyvgx.LogInfo( "Shutdown initiated" )
        err = []
        # Service Out
        try:
            pyvgx.system.ServiceInHTTP( service_in=False )
        except Exception as ex:
            err.append( ex )

        # Detach all subscribers
        try:
            pyvgx.op.Detach( uri=None, force=True )
        except Exception as ex:
            err.append( ex )

        # Unbind
        try:
            pyvgx.op.Unbind()
        except Exception as ex:
            err.append( ex )

        # Close all graphs
        #try:
        #    for name, o_s in pyvgx.system.Registry().items():
        #        try:
        #            g = pyvgx.Graph( name )
        #        except:
        #            g = pyvgx.system.GetGraph( name )
        #        g.Close()
        #        del g
        #except Exception as ex:
        #    err.append( ex )

        if err:
            progress.append( (False, err) )
        else:
            progress.append( (True, None) )


    sysplugin__BeginAdmin( authtoken )
    try:
        # Prepare shutdown
        pyvgx.LogInfo( "Received remote shutdown command" )
        Preparer = threading.Thread( target=prepare )
        Preparer.start()
        t0 = time.time()
        while len( progress ) == 0 and time.time() - t0 < 60.0:
            time.sleep( 1 )
        if len( progress ) > 0:
            ok, err = progress[0]
            if ok:
                status = "going offline"
            else:
                status = str(err)
        else:
            status = "in progress"

        # Stage final shutdown
        pyvgx.LogInfo( "Staging final shutdown phase" )
        do_persist = True if persist > 0 else False
        allow_restartable = True if restartable > 0 else False
        perform_fullreset = True if authfullreset == authshutdown else False
        __ADMIN_Shutdown__Undertaker = threading.Thread( target=__ADMIN_Shutdown__shutdown, args=(do_persist, allow_restartable, perform_fullreset) )
        __ADMIN_Shutdown__Undertaker.start()

        return { 'action': 'shutdown', 'status': status }
        
    except Exception as err:
        pyvgx.LogInfo( "Shutdown error {}".format( err ) )
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Shutdown )
