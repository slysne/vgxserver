import pyvgx

def sysplugin__properties( request:pyvgx.PluginRequest ):
    """
    System properties
    """
    return pyvgx.system.GetProperties()

pyvgx.system.AddPlugin( plugin=sysplugin__properties )

