import pyvgx

def sysplugin__ADMIN_SetReadonly( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Set system readonly
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.SuspendTxInput()
        pyvgx.system.SetReadonly()
        return { 'action': 'system readonly' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_SetReadonly )

