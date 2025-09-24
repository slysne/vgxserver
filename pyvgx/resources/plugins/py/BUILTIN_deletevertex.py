import pyvgx
import re


def sysplugin__deletevertex( request:pyvgx.PluginRequest, headers:dict, graph:str, id:str ):
    """
    Delete vertex
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        r = g.DeleteVertex( id )
        action = "deleted" if r > 0 else None
        return { 'id':id, 'action':action }
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__deletevertex )

