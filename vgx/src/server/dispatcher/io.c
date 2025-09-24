/*######################################################################
 *#
 *# io.c
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
static int __send_ident( vgx_VGXServerDispatcherChannel_t *channel ) {

  const char *segment = channel->ident_request + ((int64_t)channel->sz_ident - channel->remain_ident);

  // Send remaining ident data to socket
  short n_sent = (short)cxsend( &channel->socket, segment, channel->remain_ident, 0 );

  // Socket not writable or error
  if( n_sent <= 0 ) {
    // Error not caused by socket temporarily unwritable
    if( n_sent < 0 && !iURI.Sock.Busy( errno ) ) {
      return -1;
    }
    return 0;
  }

  // Adjust remaining data
  channel->remain_ident -= n_sent;

  // Data remains
  if( channel->remain_ident > 0 ) {
    return 0;
  }

  CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__SEND );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_io__send( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel ) {

  // Manage channel state
  if( channel->state < VGXSERVER_CHANNEL_STATE__SEND ) {
    switch( channel->state ) {
    // Detect aborted channel
    case VGXSERVER_CHANNEL_STATE__RESET:
      goto aborted;
    // Transition to send state on first call
    case VGXSERVER_CHANNEL_STATE__ASSIGNED:
      if( channel->remain_ident == 0 ) {
        CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__SEND );
        break;
      }
      /* FALLTHRU */
    case VGXSERVER_CHANNEL_STATE__IDENT:
      // Channel must identify itself to the backend
      if( __send_ident( channel ) < 0 ) {
        goto error;
      }
      // Incomplete
      if( channel->state < VGXSERVER_CHANNEL_STATE__SEND ) {
        return 0;
      }
      break;
    // Unexpected state
    default:
      goto error;
    }
  }

  const char *segment = channel->request.read;
  const char *end = channel->request.end;
  size_t n_remain = end - segment;
  size_t sz_segment = minimum_value( SEND_CHUNK_SZ, n_remain );

  // Send segment to socket
  int64_t n_sent = cxsend( &channel->socket, segment, sz_segment, 0 );

  // Socket not writable or error
  if( n_sent <= 0 ) {
    // Error not caused by socket temporarily unwritable
    if( n_sent < 0 && !iURI.Sock.Busy( errno ) ) {
      goto error;
    }
    return 0;
  }

  // At least one byte sent to socket
  channel->request.read += n_sent;

  // Data still remains to be sent, no action
  if( channel->request.read < channel->request.end ) {
    return 0;
  }

  // All data sent, we now transition channel to inbound
  CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__RECV_INITIAL );

  // Increment request counter
  channel->request.counter++;

  return 0;

error:
  return vgx_server_dispatcher_io__handle_exception( server, channel, HTTP_STATUS__INTERNAL_SOCKET_ERROR, NULL );

aborted:
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __get_recv_limit( const vgx_VGXServerDispatcherChannel_t *channel ) {
  if( channel->state == VGXSERVER_CHANNEL_STATE__RECV_CONTENT ) {
    int64_t unfilled = __vgxserver_response_unfilled_content_buffer( channel->response );
    if( unfilled < RECV_CHUNK_SZ ) {
      return unfilled;
    }
  }
  return RECV_CHUNK_SZ;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_io__recv( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel ) {
  // Detect invalid channel state
  if( channel->state < VGXSERVER_CHANNEL_STATE__RECV_INITIAL ) {
    switch( channel->state ) {
    // Aborted channel
    case VGXSERVER_CHANNEL_STATE__RESET:
      goto aborted;
    // Unexpected state
    default:
      goto error;
    }
  }

  char *segment;
  int64_t sz_segment;
  int64_t n_recv;
  vgx_StreamBuffer_t *rbuf = channel->response->buffers.content;
  int64_t recv_limit = __get_recv_limit( channel );

  // Receive data from socket into buffer.
  // Each pass around the loop fills a linear region of the buffer with data read from socket.
  // We may perform one or more passes around the loop.
recv_segment:

  // Get a linear region of response buffer into which we are able to receive bytes from socket
  if( (segment = iStreamBuffer.WritableSegmentEx( rbuf, recv_limit, &sz_segment )) == NULL ) {
    goto payload_too_large;
  }

  // Receive bytes from socket into request buffer and advance the request buffer write pointer
  if( (n_recv = cxrecv( &channel->socket, segment, sz_segment, 0 )) > 0 ) {

    // Move buffer's write pointer forward
    if( iStreamBuffer.AdvanceWrite( rbuf, n_recv ) != n_recv ) {
      goto buffer_error;
    }

    // We filled the entire segment and we still have room in our recv chunk byte budget.
    recv_limit -= n_recv;
    if( recv_limit > 0 && n_recv == sz_segment ) {
      // We will make another pass around the loop
      goto recv_segment;
    }
  }
  // Socket is no longer readable  because there is no data to read at the moment
  else if( n_recv < 0 && iURI.Sock.Busy( errno ) ) {
    goto handle_channel_response;
  }
  else {
    goto socket_exception;
  }


handle_channel_response:
  return vgx_server_dispatcher_response__handle( server, channel );

payload_too_large:
  VGX_SERVER_DISPATCHER_CRITICAL( 0x000, "Dispatcher (ch=%d.%d.%d) response buffer capacity limit reached: %lld", channel->id.partition, channel->id.replica, channel->id.channel, iStreamBuffer.Capacity( rbuf ) );
  // TODO: Propagate error details to front
  goto error;

buffer_error:
  VGX_SERVER_DISPATCHER_CRITICAL( 0x000, "internal buffer error" );
  // TODO: Propagate error details to front
  goto error;

socket_exception:
  if( n_recv < 0 ) {
    // Handle general exception
    goto error;
  }

  // Handle socket closed

error:
  return vgx_server_dispatcher_io__handle_exception( server, channel, HTTP_STATUS__INTERNAL_SOCKET_ERROR, NULL );

aborted:
  return -1;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle__no_replica( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, bool defunct ) {
  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;

  // Can't re-dispatch, no replicas
  // Remove Client from dispatcher I/O chain
  vgx_server_dispatcher_matrix__client_yank_matrix( matrix, client );

  // Abort all channels.
  vgx_server_dispatcher_matrix__abort_channels( matrix, client );

  // Client request failed and we do not allow incomplete results
  if( defunct && !matrix->flags.allow_incomplete ) {
    // Return client tp front poll chain
    vgx_server_client__append_front( server, client );

    // Produce error response
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "Partition(s) down", false );
  }

  // Client goes to backlog
  if( vgx_server_dispatcher_dispatch__append_backlog( server, client ) < 0 ) {
    // Backlog full!
    // 
    // Return client tp front poll chain
    vgx_server_client__append_front( server, client );

    // Produce error response
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__TooManyRequests, "Client backlog full after re-dispatch attempt", true );
  }

  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle__try_next_channel( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherPartition_t *partition ) {

  // Get another channel
  bool defunct = false;
  vgx_VGXServerDispatcherChannel_t *channel = vgx_server_dispatcher_matrix__get_partition_channel( &server->matrix, partition, -1, &defunct );
  if( channel == NULL ) {
    // Abort current dispatch and move client to backlog if we have backlog capacity, otherwise fail the request
    return __handle__no_replica( server, client, defunct );
  }

  // Assign front and dispatcher buffer references and
  // append re-dispatched channel to client's set of dispatcher channels
  vgx_server_dispatcher_client__channel_append( client, channel );

  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __handle__socket_error( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel, const char *message ) {
  // Grab partition and replica before we close channel
  vgx_VGXServerDispatcherPartition_t *partition = channel->parent.permanent.partition;
  vgx_VGXServerDispatcherReplica_t *replica = channel->parent.permanent.replica;

  // Remove channel from parent client, close socket
  VGX_SERVER_DISPATCHER_VERBOSE( 0x000, "Exception: channel %d.%d.%d (%s)", channel->id.channel, channel->id.replica, channel->id.partition, message );
  vgx_server_dispatcher_matrix__channel_close( &server->matrix, channel );

  // -------------------
  // Set replica defunct
  // -------------------
  vgx_server_dispatcher_matrix__set_replica_defunct( &server->matrix, replica );

  return __handle__try_next_channel( server, client, partition );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle__TooManyRequests( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel ) {
  // Grab partition and replica before we close channel
  vgx_VGXServerDispatcherPartition_t *partition = channel->parent.permanent.partition;
  vgx_VGXServerDispatcherReplica_t *replica = channel->parent.permanent.replica;

  // Return the channel for the busy backend
  vgx_server_dispatcher_channel__return( channel );

  // ---------------
  // Deboost replica
  // ---------------
  vgx_server_dispatcher_matrix__deboost_replica( &server->matrix, replica );

  return __handle__try_next_channel( server, client, partition );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle__ServiceUnavailable( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel ) {
  // Grab partition and replica before we close channel
  vgx_VGXServerDispatcherPartition_t *partition = channel->parent.permanent.partition;
  vgx_VGXServerDispatcherReplica_t *replica = channel->parent.permanent.replica;

  // Remove channel from parent client, close socket
  VGX_SERVER_DISPATCHER_VERBOSE( 0x000, "Service Unavailable: channel %d.%d.%d", channel->id.channel, channel->id.replica, channel->id.partition );
  vgx_server_dispatcher_matrix__channel_close( &server->matrix, channel );

  // -------------------
  // Set replica defunct
  // -------------------
  vgx_server_dispatcher_matrix__set_replica_defunct( &server->matrix, replica );

  return __handle__try_next_channel( server, client, partition );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle__Unrecoverable( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, HTTPStatus code, const char *message ) {
  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;

  // Remove Client from dispatcher I/O chain
  vgx_server_dispatcher_matrix__client_yank_matrix( matrix, client );

  // Abort all channels.
  vgx_server_dispatcher_matrix__abort_channels( matrix, client );

  // Return client tp front poll chain
  vgx_server_client__append_front( server, client );

  // Produce error response
  return vgx_server_response__produce_error( server, client, code, message, true );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_io__handle_exception( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel, HTTPStatus code, const char *message ) {
  vgx_VGXServerClient_t *client = channel->parent.dynamic.client;

  if( channel->state != VGXSERVER_CHANNEL_STATE__RESET && client != NULL ) {
    switch( code ) {
    case HTTP_STATUS__INTERNAL_SOCKET_ERROR:
      __handle__socket_error( server, client, channel, message );
      break;
    case HTTP_STATUS__TooManyRequests:
      __handle__TooManyRequests( server, client, channel );
      break;
    case HTTP_STATUS__ServiceUnavailable:
      __handle__ServiceUnavailable( server, client, channel );
      break;
    default:
      __handle__Unrecoverable( server, client, code, message );
    }
  }

  // Dispatch next in line client if backlog exists
  vgx_server_dispatcher_dispatch__apply_backlog( server );

  return -1;
}


