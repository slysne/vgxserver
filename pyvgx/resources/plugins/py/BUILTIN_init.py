import pyvgx

def sysplugin__init( request:pyvgx.PluginRequest ):
    """
    Server init
    """
    sysplugin__OnServerStartup()
    return { "init": "ok" }

pyvgx.system.AddPlugin( plugin=sysplugin__init )

