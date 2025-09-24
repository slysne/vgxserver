import pyvgx
import re

def sysplugin__ADMIN_Subscribe( request:pyvgx.PluginRequest, headers:dict, authtoken:str, uri:str, hardsync:int=0, timeout:int=30000):
    """
    ADMIN: Subscribe to provider
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        match = re.match( r"vgx://([^:]+):(\d+)", uri )
        if match is None:
            raise Exception( "Invalid subscriber uri: %s" % uri )
        host = match.group(1)
        port = int( match.group(2) )

        pyvgx.op.Subscribe( address=(host, port), hardsync=hardsync, timeout=timeout )
        return { 'action': 'subscribed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Subscribe )

