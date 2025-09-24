import pyvgx

def sysplugin__ADMIN_ServiceIn( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: HTTP Service In
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.ServiceInHTTP()
        return { 'action': 'service_in' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ServiceIn )

