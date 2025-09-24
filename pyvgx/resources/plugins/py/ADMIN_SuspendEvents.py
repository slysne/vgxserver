import pyvgx

def sysplugin__ADMIN_SuspendEvents( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Suspend TTL
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.SuspendEvents()
        return { 'action': 'events_suspended' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_SuspendEvents )

