import pyvgx

def sysplugin__ADMIN_Throttle( request:pyvgx.PluginRequest, headers:dict, authtoken:str, rate:float=-1.0, unit:str="bytes" ):
    """
    ADMIN: Throttle TX input

    rate : Limit TX input processing to units per second
    unit : bytes, opcodes, operations, transactions
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        if rate < 0.0:
            current_limits = pyvgx.op.Throttle()
        else:
            current_limits = pyvgx.op.Throttle( rate=rate, unit=unit )
        return { 'action': 'throttle', 'limits':current_limits }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Throttle )

