import pyvgx

def sysplugin__ADMIN_GetAuthToken( request:pyvgx.PluginRequest, headers:dict ):
    """
    ADMIN: Get a new token to allow one admin operation
    """
    
    client_uri = headers.get( 'X-Vgx-Builtin-Client' )
    token, t0, tx = sysplugin__GenerateNextAuthToken( client_uri )
    return {
        "authtoken" : token,
        "t0"        : t0,
        "tx"        : tx
    }

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_GetAuthToken )

