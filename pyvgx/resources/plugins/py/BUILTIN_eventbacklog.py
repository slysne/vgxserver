import pyvgx

def sysplugin__eventbacklog( request:pyvgx.PluginRequest, graph:str ):
    """
    Event processor backlog
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        return g.EventBacklog()
    except:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__eventbacklog )

