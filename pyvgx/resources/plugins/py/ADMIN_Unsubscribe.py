import pyvgx

def sysplugin__ADMIN_Unsubscribe( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Unsubscribe from provider
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.Unsubscribe()
        return { 'action': 'unsubscribed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Unsubscribe )

