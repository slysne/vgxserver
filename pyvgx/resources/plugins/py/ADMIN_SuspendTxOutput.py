import pyvgx

def sysplugin__ADMIN_SuspendTxOutput( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Suspend Transaction Output
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.SuspendTxOutput()
        return { 'action': 'tx_output_suspended' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_SuspendTxOutput )

