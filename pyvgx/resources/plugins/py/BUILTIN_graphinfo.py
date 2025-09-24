import pyvgx

def sysplugin__graphinfo( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str, simple:int=0 ):
    """
    Graph status information
    """
    g, close = sysplugin__GetGraphObject( graph )

    try:
        return g.Status( None, simple )
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__graphinfo )

