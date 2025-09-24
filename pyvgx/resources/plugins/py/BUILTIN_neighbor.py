import pyvgx
import re

sysplugin__neighbor_EXCLUDE = set(["headers", "content", "graph", "id", "hits", "query", "sharedmem"])

def sysplugin__neighbor( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str, id:str="", hits:int=25, query:str="", sharedmem:int=0 ):
    """
    Neighborhood query
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        Q = {
          'timeout': 5000,
          'hits'   : hits,
          'fields' : pyvgx.F_AARC
        }

        if id:
            Q['id'] = id

        for k,v in request.params.items():
            if k not in sysplugin__neighbor_EXCLUDE:
                Q[k] = v

        sysplugin__UpdateQueryDict( Q, query, content )
        if 'memory' in Q:
            Q['memory'] = sysplugin__GetGraphMemory( g, Q['memory'], sharedmem )

        M = Q.get( 'memory' )
        R = g.Neighborhood( **Q )
        if M is not None:
            if type(R) is not dict:
                R = {'neighborhood':R}
            R['memory'] = M.AsList()
        return R
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__neighbor )

