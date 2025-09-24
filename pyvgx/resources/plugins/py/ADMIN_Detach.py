import pyvgx

def sysplugin__ADMIN_Detach( request:pyvgx.PluginRequest, headers:dict, authtoken:str, uri:str ):
    """
    ADMIN: Detach a subscriber
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    TxInputSuspended = False
    sysplugin__BeginAdmin( authtoken )
    try:
        TxInputSuspended = pyvgx.op.SuspendTxInput();

        detached = pyvgx.op.Detach( uri, force=True )
        if detached < 1:
            raise Exception( "Cannot detach subscriber (input server running?)" )

        return { 'subscriber':uri, 'action':'detached' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )
        if TxInputSuspended:
            pyvgx.op.ResumeTxInput()


pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Detach )

