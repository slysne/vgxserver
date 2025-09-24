import pyvgx
import re


def sysplugin__disconnect( request:pyvgx.PluginRequest, headers:dict, graph:str, id:str, neighbor:str="*", relationship:str="*", direction:str="D_OUT", modifier:str="M_ANY", condition:str="V_ANY", value:float=0.0, timeout:int=5000 ):
    """
    Disconnect initial from terminal(s)
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        dir = getattr(pyvgx,direction)
        mod = getattr(pyvgx,modifier)
        if mod == pyvgx.M_ANY:
            arc = (relationship, dir)
        else:
            vcond = getattr(pyvgx,condition)
            val = getattr(pyvgx,value)
            arc = (relationship, dir, mod, vcond, val)
        r = g.Disconnect( id=id, arc=arc, neighbor=neighbor, timeout=timeout )
        action = "disconnected" if r > 0 else None
        return { 'id':id, 'action':action, 'arcs':r }
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__disconnect )

