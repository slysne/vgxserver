import pyvgx

def sysplugin__ADMIN_ResetMetrics( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Reset HTTP Server Metrics
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        pyvgx.system.ResetMetrics()
        return { 'action': 'metrics_reset' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ResetMetrics )

