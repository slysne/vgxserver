import pyvgx

def sysplugin__ADMIN_ResumeEvents( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Resume TTL
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.ResumeEvents()
        return { 'action': 'events_resumed' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ResumeEvents )

