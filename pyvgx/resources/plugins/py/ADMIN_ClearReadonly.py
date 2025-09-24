import pyvgx

def sysplugin__ADMIN_ClearReadonly( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Clear system readonly
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.ClearReadonly()
        return { 'action': 'system mutable' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )


pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ClearReadonly )

