import pyvgx

def sysplugin__ADMIN_SuspendTxInput( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Suspend Transaction Input
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.SuspendTxInput()
        return { 'action': 'tx_input_suspended' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_SuspendTxInput )

