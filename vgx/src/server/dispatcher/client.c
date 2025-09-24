/*######################################################################
 *#
 *# client.c
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
DLL_HIDDEN void vgx_server_dispatcher_client__channel_append( vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel ) {
  // Client will be channel's parent until all I/O for this request is complete
  channel->parent.dynamic.client = client;

  // Channel references client's dispatcher CONTENT buffer's internal start pointer and end pointer
  vgx_StreamBuffer_t *stream = client->dispatcher.streams->prequest->buffers.content;
  iStreamBuffer.ReadableSegment( stream, LLONG_MAX, &channel->request.read, &channel->request.end );

  // Channel keeps record of the client's start-of-request timestamp
  // (This will never be set to zero, it keeps track of how long a channel has been idle.)
  channel->t0_ns = client->io_t0_ns;
  
  // Mark channel as busy
  channel->flag.busy = true;

  // Channel references client's dispatcher response instance corresponding to channel's partition
  if( CHANNEL_IS_PARTIAL( channel ) || CLIENT_HAS_ANY_PROCESSOR( client ) ) {
    channel->response = vgx_server_dispatcher_streams__get_reset_response( client->dispatcher.streams, channel->id.partition );
  }
  // Channel references client's own response buffer since the response is relayed exactly as-is to the front client
  else {
    int64_t exec_ns = client->response.exec_ns; // we save this...
    channel->response = &client->response;
    vgx_server_response__reset( channel->response ); // ...because we reset here...
    channel->response->exec_ns = exec_ns; // ...so we can continue to accumulate elapse time 
  }

  // Backend self-reported backlog unknown at this time
  channel->response->x_vgx_backlog = -1;

  // Append channel to client's list of channels
  VGX_LLIST_APPEND( client->dispatcher.channels, channel );
  
  // Channel is assigned to client and ready to transmit request
  CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__ASSIGNED );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherChannel_t * vgx_server_dispatcher_client__channel_yank( vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel ) {
  if( CHANNEL_IN_CHAIN( channel ) ) {
    VGX_LLIST_YANK( client->dispatcher.channels, channel );

    // TODO:
    //
    //  If channel is inflight (has performed any dispatcher IO for this request) 
    //  we need to flush the channel by closing it such that a future client request
    //  will not produce or receive a corrupted data stream!

    CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__READY );
    // Channel has served its purpose, now forget all BORROWED references
    // to structures it has been using while performing dispatcher I/O operations.
    // These resources are still allocated and managed by the dispatcher matrix.
    channel->request.read = NULL;
    channel->request.end = NULL;
    channel->request.cost = -1;
    channel->flag.busy = false;
    channel->response = NULL;
    channel->parent.dynamic.client = NULL;
    return channel;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool vgx_server_dispatcher_client__no_channels( const vgx_VGXServerClient_t *client ) {
  return VGX_LLIST_EMPTY( client->dispatcher.channels );
}

