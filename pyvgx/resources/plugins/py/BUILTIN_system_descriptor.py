import pyvgx

def sysplugin__system_descriptor( request:pyvgx.PluginRequest ):
    """
    System descriptor
    """
    return sysplugin__GetSystemDescriptor()

pyvgx.system.AddPlugin( plugin=sysplugin__system_descriptor )

