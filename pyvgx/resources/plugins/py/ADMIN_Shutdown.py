import pyvgx
import threading
import time
import sys
import os
import signal


__ADMIN_Shutdown__Undertaker = None

def __ADMIN_Shutdown__shutdown( persist=False ):
    pyvgx.LogInfo( "Final shutdown initiated" )
    time.sleep(2)
    if persist:
        # Persist
        pyvgx.LogInfo( "Shutdown persist" )
        try:
            pyvgx.system.Persist( force=True, timeout=30000 )
            time.sleep(2)
        except Exception as err:
            pyvgx.LogError( "Persist error {}".format(err) )
    # Stop HTTP
    pyvgx.LogInfo( "Stopping HTTP Server" )
    try:
        pyvgx.system.StopHTTP()
    except:
        pass
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



def sysplugin__ADMIN_Shutdown( request:pyvgx.PluginRequest, headers:dict, authtoken:str, authshutdown:str, persist:int=0 ):
    """
    ADMIN: Shutdown
    """
    global __ADMIN_Shutdown__Undertaker

    if authshutdown != sysplugin__GetPreviousAuthToken():
        raise Exception( "Invalid token" )

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
        __ADMIN_Shutdown__Undertaker = threading.Thread( target=__ADMIN_Shutdown__shutdown, args=(do_persist,) )
        __ADMIN_Shutdown__Undertaker.start()

        return { 'action': 'shutdown', 'status': status }
        
    except Exception as err:
        pyvgx.LogInfo( "Shutdown error {}".format( err ) )
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Shutdown )

