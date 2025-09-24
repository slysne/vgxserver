import pyvgx

def sysplugin__evaluate( request:pyvgx.PluginRequest, content:str, graph:str, expression:str="", tail:str="", arc:str="", head:str="", memory:int=128, memresult:int=0, sharedmem:int=0 ):
    """
    Expression evaluator
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        M = sysplugin__GetGraphMemory( g, memory, sharedmem )
        params = { 'memory': M }

        if content:
            params['expression'] = content
        elif expression:
            params['expression'] = expression

        if tail:
            params['tail'] = tail
        if arc:
            params['arc'] = sysplugin__VALIDATOR.SafeEval( arc )
        if head:
            params['head'] = head

        if 'tail' in params:
            params['tail'] = g.OpenVertex( params['tail'], mode='r', timeout=5000 )
        if 'head' in params:
            params['head'] = g.OpenVertex( params['head'], mode='r', timeout=5000 )

        R = g.Evaluate( **params )
        if memresult:
            L = M.AsList()
        else:
            L = []
        response = { 'result': repr(R), 'memory': L }
        return response
    finally:
        try:
            if 'tail' in params:
                g.CloseVertex( params['tail'] )
            if 'head' in params:
                g.CloseVertex( params['head'] )
        finally:
            if close:
                g.Close()


pyvgx.system.AddPlugin( plugin=sysplugin__evaluate )

