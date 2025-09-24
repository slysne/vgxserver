import pyvgx
import json

def sysplugin__ADMIN_ReloadPlugins( request:pyvgx.PluginRequest, headers:dict, authtoken:str, content:json ):
    """
    ADMIN: Reload Plugins
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        if content is not None:
            if type( content ) is not list:
                raise TypeError( "content must be list of dicts" )
            for plugin_def in content:
                if type( plugin_def ) is not dict:
                    raise TypeError( "content must be list of dicts" )
            if len( content ) == 0:
                content = None
        added = pyvgx.VGXInstance.LoadPlugins( plugins=content )
        return { 'action': 'plugins_reloaded', 'result': added }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_ReloadPlugins )

