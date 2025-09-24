import pyvgx

def sysplugin__ADMIN_ServiceOut( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: HTTP Service Out
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.ServiceInHTTP( False )
        return { 'action': 'service_out' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ServiceOut )

