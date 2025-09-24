import pyvgx

def sysplugin__vertex( request:pyvgx.PluginRequest, graph:str, id:str, maxarcs:int=3 ):
    """
    Get vertex information
    """
    g, close = sysplugin__GetGraphObject( graph )
    V = None
    try:
        V = g.OpenVertex( id, mode='r', timeout=5000 )
        ident = 'Vertex( %s )' % id
        data = {}
        result = { ident: data }
        for arcdir, key in [(pyvgx.D_OUT,'outarcs'), (pyvgx.D_IN,'inarcs')]:
            top = g.Neighborhood( V, arc=arcdir, sortby=pyvgx.S_VAL, fields=pyvgx.F_AARC, hits=maxarcs, result=pyvgx.R_STR|pyvgx.R_COUNTS, timeout=5000 )
            c = top.get('counts',{}).get('arcs',0)
            N = top.get( 'neighborhood', [] )
            h = len( N )
            if h < c:
                N.append( '(%d more...)' % (c-h) )
            data[ key ] = N
        V_dict = V.AsDict()
        V_dict.pop( 'id', None )
        data[ 'properties' ] = V_dict.pop( 'properties', {} )
        data[ 'attributes' ] = V_dict
        return result
    finally:
        try:
            if V is not None:
                V.Close()
        finally:
            if close:
                g.Close()


pyvgx.system.AddPlugin( plugin=sysplugin__vertex )

