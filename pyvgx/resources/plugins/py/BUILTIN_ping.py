import pyvgx

def sysplugin__ping( request:pyvgx.PluginRequest ):
    """
    Server ping
    """
    return True

pyvgx.system.AddPlugin( plugin=sysplugin__ping )

