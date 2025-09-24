import pyvgx

def sysplugin__ADMIN_SystemDescriptor( request:pyvgx.PluginRequest, headers:dict, authtoken:str, content:str ):
    """
    ADMIN: Update system descriptor
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        descriptor = json.loads( content )
        sysplugin__SetSystemDescriptor( descriptor )
        return { 'action': 'system descriptor updated' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_SystemDescriptor )

