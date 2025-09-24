import pyvgx
import threading
import time

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

