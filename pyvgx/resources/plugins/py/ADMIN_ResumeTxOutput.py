import pyvgx

def sysplugin__ADMIN_ResumeTxOutput( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Resume Transaction Output
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.ResumeTxOutput()
        return { 'action': 'tx_output_resumed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ResumeTxOutput )

