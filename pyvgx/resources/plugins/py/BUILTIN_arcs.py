import pyvgx
import re

sysplugin__arcs_EXCLUDE = set(["headers", "content", "graph", "hits", "query", "arc", "sharedmem"])

def sysplugin__arcs( request:pyvgx.PluginRequest, headers:dict, content:str, graph:str, hits:int=25, query:str="", arc:str="", sharedmem:int=0 ):
    """
    Global arcs query
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        Q = {
          'timeout'   : 5000,
          'hits'   : hits,
          'fields' : pyvgx.F_AARC
        }

        for k,v in request.params.items():
            if k not in sysplugin__arcs_EXCLUDE:
                Q[k] = v

        sysplugin__UpdateQueryDict( Q, query, content )
        if arc:
            if 'condition' in Q and 'arc' not in Q['condition']:
                Q['condition']['arc'] = sysplugin__VALIDATOR.SafeEval( arc )
            elif 'condition' not in Q:
                Q['condition'] = { 'arc': sysplugin__VALIDATOR.SafeEval( arc ) }
        if 'memory' in Q:
            Q['memory'] = sysplugin__GetGraphMemory( g, Q['memory'], sharedmem )

        M = Q.get( 'memory' )
        R = g.Arcs( **Q )
        if M is not None:
            if type(R) is not dict:
                R = {'arcs': R}
            R['memory'] = M.AsList()
        return R
    finally:
        if close:
            g.Close()

pyvgx.system.AddPlugin( plugin=sysplugin__arcs )

