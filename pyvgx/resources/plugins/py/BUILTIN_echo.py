import pyvgx

def sysplugin__echo( request:pyvgx.PluginRequest, headers:dict, method:str, content:bytes ):
    """
    Server echo
    """
    http = {}
    echo = { 'HTTP': http }
    http['method'] = method
    http['request'] = request.params
    H = {}
    for k,v in sorted( headers.items() ):
        try:
            H[k] = v.decode()
        except:
            H[k] = repr(v)
    http['headers'] = H
    try:
        http['content'] = content.decode()
    except:
        http['content'] = repr( content )
    return echo

pyvgx.system.AddPlugin( plugin=sysplugin__echo )

