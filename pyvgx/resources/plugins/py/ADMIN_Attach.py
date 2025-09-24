import pyvgx

def sysplugin__ADMIN_Attach( request:pyvgx.PluginRequest, headers:dict, authtoken:str, uri:str ):
    """
    ADMIN: Attach a subscriber
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.Attach( uri )
        return { 'subscriber':uri, 'action':'attached' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Attach )

