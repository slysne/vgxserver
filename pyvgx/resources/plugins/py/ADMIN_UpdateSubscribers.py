import pyvgx
import threading
import time
import json
import re
import time


def __internal__ADMIN_UpdateSubscribersExecute( SubscribersGoal, SubscribersSync, authtoken ):

    def PrepareSync():
        """
        Enter into local state where sync is possible
        Return True if local TX input becomes suspended here
        """
        # Local graph(s) must be writable for sync to work
        nRO = pyvgx.system.CountReadonly()
        if nRO != 0:
            raise Exception( "Cannot update subscribers while {} graph(s) are readonly".format( nRO ) )



    def PerformSync( SubscribersSync ):
        """
        Detach all current subscribers, attach to sync destinations, perform sync, then detach all
        """
        # Detach any current subscribers
        pyvgx.op.Detach()

        # Attach new subscribers only
        pyvgx.op.Attach( SubscribersSync )

        # Hard sync of new subscribers
        # (this will take a while, consider making the whole procedure async)
        pyvgx.system.Sync( hard=True )

        # Detach all new subscribers
        pyvgx.op.Detach()



    def AttachGoal( SubscribersGoal ):
        """
        Attach to all subscribers
        """
        # Attach end-goal subscribers (if different from existing subscribers)
        if set( pyvgx.op.Attached() ) != set( SubscribersGoal ):
            # Detach any current subscribers
            pyvgx.op.Detach()

            # Attach the end goal set of subscribers
            pyvgx.op.Attach( SubscribersGoal )



    # Initial state
    TxSuspendedHere = False
    SubscribersOriginal = pyvgx.op.Attached()
    
    actions = {}

    sysplugin__BeginAdmin( authtoken )

    try:
        # Choke off any streaming input
        TxSuspendedHere = True if pyvgx.op.SuspendTxInput() == 1 else False

        # Perform local actions to prepare for sync
        if len( SubscribersSync ) > 0:
            PrepareSync()

            # Perform sync to new destinations (this will take time)
            PerformSync( SubscribersSync )

        # Attach to all subscribers as specified
        AttachGoal( SubscribersGoal )

        actions['action'] = "attached"
        actions['added'] = sorted( SubscribersSync )
        actions['removed'] = sorted( list( set(SubscribersOriginal) - set(SubscribersGoal) ) )
        actions['result'] = sorted( SubscribersGoal )
        
    except Exception as err:
        # Restore original set of subscribers if anything went wrong
        if set( pyvgx.op.Attached() ) != set( SubscribersOriginal ):
            pyvgx.op.Detach()
            pyvgx.op.Attach( SubscribersOriginal )
        return err
    finally:
        try:
            if TxSuspendedHere:
                pyvgx.op.ResumeTxInput()
        finally:
            sysplugin__EndAdmin( authtoken )

    return actions



def __internal__ADMIN_UpdateSubscribersGetGoalAndSyncLists( goal, content="" ):
    """
    Return two lists: The final goal, and the intermediary sync destinations
    """
    def GetDestinations( GoalJSON ):
        """
        Parse json and return list of destinations
        """
        subscribers = {}
        goal = json.loads( GoalJSON )
        if type( goal ) is dict:
            control = goal.get("control")
            if control is not None and type( control ) is not dict:
                raise TypeError( 'Subscriber goal "control" must be dict, got {}'.format( type( control ) ) )
            destinations = goal.get("destinations")
            if destinations is None:
                raise ValueError( 'Subscriber goal missing "destinations"' )
        else:
            control = {}
            destinations = goal

        if type(destinations) is not list:
            raise TypeError( "Subscriber destinations must be list, got {}".format( type(destinations) ) )

        # Make sure each host is reachable and able to be a subscriber
        for dest in destinations:
            m = re.match( r"([^:]+):(\d+)", dest )
            if m is None:
                raise ValueError( "Invalid subscriber address: {}".format( dest ) )
            host = m.group(1)
            admin_port = int(m.group(2))
            key = "{}:{}".format( host, admin_port )
            if key in subscribers:
                raise ValueError( "Duplicate destination specified: {}".format( key ) )
            subscribers[ key ] = ( host, admin_port )
        # List of destinations
        return (sorted( subscribers.values() ), control)



    def GetHostIP( host, admin_port ):
        """
        Contact host and get its self-reported IP address
        """
        try:
            ping, headers = sysplugin__SendAdminRequest( host, admin_port, path="/vgx/ping" )
            return ping["host"]["ip"]
        except Exception as err:
            raise Exception( "{}:{}/vgx/ping failed: {}".format( host, admin_port, err ) )



    def GetSubscriberState( host, admin_port ):
        """
        Get TX input service state using host's self-reported IP
        """
        try:
            peerstat, headers = sysplugin__SendAdminRequest( host, admin_port, path="/vgx/peerstat" )
            tx_port = int( peerstat.get("port",0) )
            tx_provider = peerstat.get( "provider", None )
            return (tx_port, tx_provider)
        except Exception as err:
            raise Exception( "{}:{}/vgx/peerstat failed: {}".format( host, admin_port, err ) )



    def DestinationRequiresSync( host, admin_port ):
        """
        Return True if destination has different data than local system
        """
        response, headers = sysplugin__SendAdminRequest( host, admin_port, path="/vgx/graphsum" )
        try:
            graphsum = response["graphsum"]
            remote_digest = graphsum.get("digest", None)
            local_digest = pyvgx.system.Fingerprint()
            if remote_digest != local_digest:
                return True
            else:
                return False
        except:
            # Safeguard, any error assumes no sync required
            return False



    def IsHostServiceIn( host, admin_port ):
        """
        Return True if host is in service-in state
        """
        status, headers = sysplugin__SendAdminRequest( host, admin_port, path="/vgx/status" )
        try:
            return False if status["request"]["serving"] == 0 else True
        except:
            # Safeguard, any error assumes service-in !
            return True



    def IsHostGraphEmpty( host, admin_port ):
        """
        Return True if host has no data
        """
        response, headers = sysplugin__SendAdminRequest( host, admin_port, path="/vgx/graphsum" )
        try:
            graphsum = response["graphsum"]
            order = graphsum["order"]
            size = graphsum["size"]
            properties = graphsum["properties"]
            vectors = graphsum["vectors"]
            total = order + size + properties + vectors
            return True if total == 0 else False
        except:
            # Safeguard, any error assumes data exists
            return False



    def DestinationAllowsSync( host, admin_port ):
        """
        Return True if this host can safely be synced.
        """
        # 1. Service-out means we are allowed to sync
        if IsHostServiceIn( host, admin_port ) is not True:
            return True

        # 2. No graph data means we are allowed to sync
        if IsHostGraphEmpty( host, admin_port ) is True:
            return True

        return False



    def SubscriberState( host, admin_port, repair=True, nosync=False ):
        """
        Return a prospective subscriber's self reported IP address, tx input port, and sync-flag.
        The sync flag is True if nosync parameter is False, AND:
            * the destination currently has no connected provider and its fingerprint differs from the local host
              - or -
            * the repair flag is True and the destination's fingerprint differs from the local host
        """
        # Get host's self-reported IP address
        host_ip = GetHostIP( host, admin_port )
        # Get host's subscriber state
        tx_port, tx_provider = GetSubscriberState( host_ip, admin_port )
        # TX service running ?
        if tx_port <= 0:
            raise Exception( "Destination has no TX input service: {}:{}".format( host, admin_port ) )
        # Override sync to False
        if nosync is True:
            sync = False
        # Determine sync based on flags
        else:
            # Explicit repair requested, check if sync is needed
            if repair:
                sync = DestinationRequiresSync( host_ip, admin_port )
            # Candidate for sync if destination does not already have a provider
            else:
                # The host is a candidate for sync
                sync = True if tx_provider is None else False
                # Do we really need to sync?
                if sync is True:
                    sync = DestinationRequiresSync( host_ip, admin_port )
            # Verify that sync is allowed
            if sync is True:
                if DestinationAllowsSync( host_ip, admin_port ) is not True:
                    raise Exception( "Cannot sync to destination {}:{}. Service-out and retry.".format( host, admin_port ) )
        
        # Subscriber OK
        return (host_ip, tx_port, sync)



    def GetVerifiedGoal( goal ):
        """
        Return list of (host, admin_port, tx_port, sync) for all destinations.
        """
        subscribers = {}
        # Get subscriber destinations from goal json
        destinations, control = GetDestinations( goal )
        # Validate destinations
        for host, admin_port in destinations:
            # control: repair
            repair = True if control.get("repair") is True else False
            # control: nosync
            nosync = True if control.get("nosync") is True else False
            # Get subscriber state
            host_ip, tx_port, sync = SubscriberState( host, admin_port, repair, nosync )
            ip_key = "{}:{}".format( host_ip, tx_port )
            if ip_key in subscribers:
                raise ValueError( "Duplicate destination IP detected: {} at {}:{}".format( ip_key, host, admin_port ) )
            subscribers[ ip_key ] = (host, admin_port, tx_port, sync)
        # Return list of unique, verified subscribers
        return sorted( subscribers.values() )


    # Override goal from (POSTed) content if given
    if content:
        goal = content

    # Verify the end goal
    VerifiedGoalQuad = GetVerifiedGoal( goal )
    SubscribersGoal = ["vgx://{}:{}".format(host, tx_port) for host, admin_port, tx_port, sync in VerifiedGoalQuad]

    # Get list of new subscribers that will be sync'ed
    SubscribersSync = ["vgx://{}:{}".format(host, tx_port) for host, admin_port, tx_port, sync in VerifiedGoalQuad if sync]

    return (SubscribersGoal, SubscribersSync)



def sysplugin__ADMIN_UpdateSubscribers( request:pyvgx.PluginRequest, headers:dict, authtoken:str, content:str, goal:str="[]" ):
    """
    ADMIN: Update Subscribers
    """
    sysplugin__AuthorizeAdminOperation( headers, authtoken )

    sysplugin__BeginAdmin( authtoken )
    try:
        SubscribersGoal, SubscribersSync = __internal__ADMIN_UpdateSubscribersGetGoalAndSyncLists( goal, content )
        return __internal__ADMIN_UpdateSubscribersExecute( SubscribersGoal, SubscribersSync, authtoken )
    finally:
        sysplugin__EndAdmin( authtoken )



pyvgx.system.AddPlugin( plugin=sysplugin__ADMIN_UpdateSubscribers )

