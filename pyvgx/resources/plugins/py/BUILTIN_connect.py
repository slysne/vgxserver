import pyvgx
import re


def sysplugin__connect( request:pyvgx.PluginRequest, headers:dict, graph:str, initial:str, terminal:str, relationship:str="__related__", modifier:str="M_FLT", value:float=0.0, lifespan:int=-1, timeout:int=5000 ):
    """
    Connect two vertices
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        mod = getattr(pyvgx, modifier)
        r = g.Connect( initial=initial, arc=(relationship,mod,value), terminal=terminal, lifespan=lifespan, timeout=timeout )
        action = "connected" if r > 0 else "updated"
        return { 'initial':initial, 'terminal':terminal, 'action':action }
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__connect )

