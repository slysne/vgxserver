import pyvgx

def sysplugin__memory( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str ):
    """
    Memory usage for graph
    """

    if graph == "*":
        response = {}
        for name in pyvgx.system.Registry():
            response[name] = sysplugin__memory( request, headers, content, name )
        return response
    else:
        g, close = sysplugin__GetGraphObject( graph )

    try:
        return g.GetMemoryUsage()
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__memory )

