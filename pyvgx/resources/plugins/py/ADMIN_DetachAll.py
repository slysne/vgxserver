import pyvgx

def sysplugin__ADMIN_DetachAll( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Detach all subscribers
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.op.Detach( uri=None, force=True )
        return { 'subscriber':'all', 'action':'detached' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_DetachAll )

