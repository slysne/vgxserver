/*######################################################################
 *#
 *# channel.c
 *#
 *#
 *######################################################################
 */


#include "_vxserver_dispatcher.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_channel__dump( const vgx_VGXServerDispatcherChannel_t *channel, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherChannel_t, channel ) {
    CXLIB_OSTREAM( "id");
    CXLIB_OSTREAM( "    channel   = %d", channel->id.channel );
    CXLIB_OSTREAM( "    replica   = %d", channel->id.replica );
    CXLIB_OSTREAM( "    partition = %d", channel->id.partition );
    CXLIB_OSTREAM( "state         = %08X", channel->state );
    CXLIB_OSTREAM( "socket" );
    CXLIB_OSTREAM( "    s         = %lld", (long long)channel->socket.s );
    CXLIB_OSTREAM( "request" );
    CXLIB_OSTREAM( "    read      = %llp", channel->request.read );
    CXLIB_OSTREAM( "    end       = %llp", channel->request.end );
    CXLIB_OSTREAM( "    counter   = %lld", channel->request.counter );
    CXLIB_OSTREAM( "chain" );
    CXLIB_OSTREAM( "    prev      = %llp", channel->chain.prev );
    CXLIB_OSTREAM( "    next      = %llp", channel->chain.next );
    CXLIB_OSTREAM( "response" );
    vgx_server_response__dump( channel->response, NULL );
    CXLIB_OSTREAM( "flag" );
    CXLIB_OSTREAM( "    partial   = %d", (int)channel->flag.partial );
    CXLIB_OSTREAM( "    busy      = %d", (int)channel->flag.busy );
    CXLIB_OSTREAM( "    connected = %d", (int)channel->flag.connected_MCS );
    CXLIB_OSTREAM( "    yielded   = %d", (int)channel->flag.yielded );
    CXLIB_OSTREAM( "remain_ident  = %llu", channel->remain_ident );
    CXLIB_OSTREAM( "ident_request = %s", channel->ident_request );
    CXLIB_OSTREAM( "t0_ns         = %lld", channel->t0_ns );
    CXLIB_OSTREAM( "parent.dynamic" );
    CXLIB_OSTREAM( "    client    = %llp", channel->parent.dynamic.client );
    CXLIB_OSTREAM( "parent.permanent" );
    CXLIB_OSTREAM( "    replica   = %llp", channel->parent.permanent.replica );
    CXLIB_OSTREAM( "    partition = %llp", channel->parent.permanent.partition );
  } END_CXLIB_OBJ_DUMP;
  if( fatal_message ) {
    FATAL( 0xEEE, "%s", fatal_message );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_channel__init( vgx_VGXServer_t *server, int8_t channel_id, vgx_VGXServerDispatcherChannel_t *channel, vgx_VGXServerDispatcherReplica_t *replica, vgx_VGXServerDispatcherPartition_t *partition ) {
  int ret = 0;

  XTRY {
    if( channel_id < 0 || channel_id >= REPLICA_MAX_CHANNELS ) {
      THROW_ERROR( CXLIB_ERR_CONFIG, 0x000 );
    }

    // -------------------------------------------------------------------
    // Channel ID
    // [Q1.1.1.1]
    channel->id.channel = channel_id;
    // [Q1.1.1.2]
    channel->id.replica = replica->id.replica;
    // [Q1.1.1.3]
    channel->id.partition = partition->id.partition;
    // [Q1.1.1.4]
    channel->id.__rsv_1_1_1_4 = 0;

    // [Q1.1.2]
    // State
    channel->state = VGXSERVER_CHANNEL_STATE__RESET;

    // [Q1.2]
    // Socket (initialized in the invalid state)
    memset( &channel->socket, 0, sizeof( CXSOCKET ) );
    cxsocket_invalidate( &channel->socket );

    // [Q1.3]
    channel->request.read = NULL;

    // [Q1.4]
    channel->request.end = NULL;

    // [Q1.5]
    channel->request.counter = -1;

    // [Q1.6.1]
    channel->request.cost = -1;

    // [Q1.6.2]
    channel->request.__rsv_1_6_2 = 0;

    // [Q1.7]
    channel->chain.prev = NULL;

    // [Q1.8]
    channel->chain.next = NULL;
    // -------------------------------------------------------------------


    // -------------------------------------------------------------------
    // [Q2.1]
    channel->response = NULL;

    // [Q2.2]
    channel->__rsv_2_2 = 0;

    // [Q2.3.1.1]
    channel->flag.partial = partition->flag.partial;

    // [Q2.3.1.2]
    channel->flag.busy = false;

    // [Q2.3.1.3]
    channel->flag.connected_MCS = false;

    // [Q2.3.1.4]
    channel->flag.yielded = 0;

    // [Q2.3.2.1]
    channel->remain_ident = 0;

    // [Q2.3.2.2]
    channel->sz_ident = 0;

    // [Q2.4]
    if( (channel->ident_request = calloc( 64, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    // XVGXIDENT width height depth partition replica channel primary
    snprintf( channel->ident_request, 63, "XVGXIDENT %d %d %d %d %d %d %d" CRLF, (int)server->matrix.partition.width, (int)partition->replica.height, (int)replica->depth, (int)channel->id.partition, (int)channel->id.replica, (int)channel->id.channel, (int)replica->primary );
    SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED
    channel->sz_ident = (short)strnlen( channel->ident_request, 63 );

    // [Q2.5]
    channel->t0_ns = 0;
    
    // [Q2.6]
    channel->parent.dynamic.client = NULL;

    // [Q2.7]
    channel->parent.permanent.replica = replica;

    // [Q2.8]
    channel->parent.permanent.partition = partition;

    // -------------------------------------------------------------------

  }
  XCATCH( errcode ) {
    vgx_server_dispatcher_channel__clear( channel );
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_channel__clear( vgx_VGXServerDispatcherChannel_t *channel ) {
  if( channel ) {
    // [Q2.8]
    free( channel->ident_request );

    // Zero everything
    memset( channel, 0, sizeof( vgx_VGXServerDispatcherChannel_t ) );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_channel__invalid_push( const vgx_VGXServerDispatcherChannel_t *channel ) {
  VGX_SERVER_DISPATCHER_CRITICAL( 0x001, "Attempted to push channel on full stack" );
  vgx_server_dispatcher_channel__dump( channel, NULL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_channel__return( vgx_VGXServerDispatcherChannel_t *channel ) {
  // Yank the channel out of the client's list of dispatcher channels
  if( vgx_server_dispatcher_client__channel_yank( channel->parent.dynamic.client, channel ) ) {

    // Return the channel back to the replica's resource pool for use by other clients
    vgx_server_dispatcher_replica__push_channel( channel->parent.permanent.replica, channel );
  }
}

