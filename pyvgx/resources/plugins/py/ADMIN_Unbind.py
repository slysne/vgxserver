import pyvgx

def sysplugin__ADMIN_Unbind( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: System unbind
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.Unbind()
        return { 'action': 'unbind' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Unbind )

