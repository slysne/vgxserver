import pyvgx

def sysplugin__ADMIN_Truncate( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Delete all graph data
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        tx_suspended = pyvgx.op.SuspendTxInput()
        pyvgx.op.Detach( uri=None, force=True )
        pyvgx.system.ClearReadonly()
        pyvgx.system.SetReadonly()
        pyvgx.system.ClearReadonly()
        if tx_suspended:
            pyvgx.op.ResumeTxInput()
        G = []
        for name, o_s in pyvgx.system.Registry().items():
            try:
                g = pyvgx.Graph( name )
            except:
                g = pyvgx.system.GetGraph( name )
            G.append( g )
        for g in G:
            g.Truncate()

        return { 'action': 'truncate' }
    except Exception as err:
        return err
    finally:
        sysplugin__EndAdmin( authtoken )

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_Truncate )

