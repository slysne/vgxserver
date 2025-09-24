import pyvgx

def sysplugin__ADMIN_ResumeTxInput( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Resume Transaction Input
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        nRO = pyvgx.system.CountReadonly()
        if nRO != 0:
            raise Exception( "Cannot enable transaction input with {} readonly graph(s)".format( nRO ) )
        pyvgx.op.ResumeTxInput()
        return { 'action': 'tx_input_resumed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ResumeTxInput )

