import pyvgx
import re
import json


def sysplugin__createvertex( request:pyvgx.PluginRequest, headers:dict, graph:str, id:str, type:str="", lifespan:int=-1, properties:json={} ):
    """
    Create/update vertex
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        r = g.CreateVertex( id=id, type=type, lifespan=lifespan, properties=properties )
        action = "created" if r > 0 else "updated"
        return { 'id':id, 'action':action }
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__createvertex )

