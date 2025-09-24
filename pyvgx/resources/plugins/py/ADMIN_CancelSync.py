import pyvgx

def sysplugin__ADMIN_CancelSync( request:pyvgx.PluginRequest, headers:dict, authtoken:str ):
    """
    ADMIN: Cancel Synchronize subscribers
    """

    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    # NOTE: This operation is allowed even when another admin operation is running (e.g. Sync)
    try:

        sync_stopped = pyvgx.system.CancelSync()
        if sync_stopped > 0:
            status = "sync stopped"
        elif sync_stopped == 0:
            status = "sync not running"
        else:
            status = "failed to stop"

        return { 'action': 'cancel_sync', 'status': status }
        
    except Exception as err:
        return err
    finally:
        pass

pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_CancelSync )

