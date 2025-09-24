import pyvgx

def sysplugin__ADMIN_Bind( request:pyvgx.PluginRequest, headers:dict, authtoken:str, port:int, durable:int=0 ):
    """
    ADMIN: System bind
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        nRO = pyvgx.system.CountReadonly()
        if nRO != 0:
            raise Exception( "Cannot bind with {} readonly graph(s)".format( nRO ) )
        pyvgx.op.Bind( port=port, durable=durable )
        return { 'action': 'bind', 'port':port, 'durable':durable }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Bind )

