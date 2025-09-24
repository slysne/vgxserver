import pyvgx
import threading
import time

def sysplugin__ADMIN_RestartHTTP( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Restart HTTP Server
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    progress = []

    def perform( ident, current ):
        len( progress )
        was_serving = 0
        try:

            # System Descriptor
            descriptor = pyvgx.Descriptor( sysplugin__GetSystemDescriptor() )

            # Make sure descriptor is working
            instance = descriptor.Get( ident )
            instance.HC()

            # Get current S-IN state
            was_serving = instance.Status()['request']['serving']

            # S-OUT and wait until all activity is idle
            pyvgx.system.ServiceInHTTP( 0 )
            deadline = time.time() + 30
            while time.time() < deadline:
                status = instance.Status()
                active = status['request']['working'] + status['request']['waiting']
                if active > 0:
                    time.sleep(1)
                else:
                    break
            if time.time() > deadline:
                raise Exception( "Timeout due to ongoing activity" )

            # Signal that we're ready, wait for parent thread to have a chance to respond before we shut down
            progress.append( (True, None) )
            time.sleep( 2.0 )

            # Stop Server
            pyvgx.system.StopHTTP()

            time.sleep(1)

            # Start Server
            port = current['port']
            ip = current['ip']
            prefix = current['prefix']
            for cfdispatcher in [instance.cfdispatcher, current['dispatcher']]:
                try:
                    # Start server
                    pyvgx.system.StartHTTP( port, ip=ip, prefix=prefix, servicein=0, dispatcher=cfdispatcher )

                    # Restore connection and verify
                    for n in range(5):
                        try:
                            instance.HC()
                            instance.Status()
                            break
                        except:
                            time.sleep(1)
                    # OK
                    break
                    
                except Exception as ex:
                    time.sleep(2)

        except Exception as perr:
            progress.append( (False, perr) )
        finally:
            # Restore original S-IN state
            if was_serving:
                try:
                    pyvgx.system.ServiceInHTTP(1)
                except:
                    pass
       
    
    sysplugin__BeginAdmin( authtoken )
    try:

        ident = sysplugin__GetSystemIdent()
        if ident is None:
            raise TypeError( "System identifier missing, operation cannot be completed" )

        # Get current config if we need to roll back
        current = {
            'port':  pyvgx.system.ServerPorts()['base'],
            'ip': pyvgx.system.ServerAdminIP(),
            'prefix': pyvgx.system.ServerPrefix(),
            'dispatcher': pyvgx.system.DispatcherConfig()
        }

        Restarter = threading.Thread( target=perform, args=(ident, current)  )
        Restarter.start()

        status = 'unknown'
        deadline = time.time() + 30
        while len( progress ) == 0 and time.time() < deadline:
            time.sleep(0.1)
        if len( progress ) > 0:
            if progress[0][0] is True:
                status = 'in progress'
            else:
                status = progress[0][1]
        else:
            status = "timeout"

        return { 'action': 'restart_http', 'status': status }
        
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_RestartHTTP )

