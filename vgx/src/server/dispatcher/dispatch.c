/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    dispatch.c
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

#include "_vxserver_dispatcher.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __dispatch( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, bool *defunct, bool *alldown ) {
  // Allocate dispatcher channels for client
  if( vgx_server_dispatcher_matrix__assign_client_channels( matrix, client, defunct, alldown ) < 0 ) {
    // Temporary overload
    return -1;
  }

  // Append client to matrix's iolist
  vgx_server_dispatcher_matrix__client_append_matrix( matrix, client );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_dispatch__forward( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  int ret = 0;
  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;

  // Sanity check
  // NOTE: Matrix uses only CONTENT buffer in the request, which should be prefilled with the entire request at this point
  if( client->dispatcher.streams == NULL || iStreamBuffer.Size( client->dispatcher.streams->prequest->buffers.content ) == 0 ) {
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Empty dispatcher request", true );
  }

  // We must yank client out of poll chain before dispatching to another component
  // (It will not be in the chain if we come from the executor, but it will be in chain
  // if we come straight from front request handler in passthru mode)
  vgx_server_client__yank_front( server, client );
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__AWAIT_DISPATCH );

  bool defunct = false;
  bool alldown = false;

  XTRY {

    // Allocate channels and response buffers, and enter client into dispatcher I/O.
    // If all channels are busy, put client on backlog queue.
    if( __dispatch( matrix, client, &defunct, &alldown ) < 0 ) {
      if( defunct ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      if( vgx_server_dispatcher_dispatch__append_backlog( server, client ) ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

  }
  XCATCH( errcode ) {


    // Dispatch failed, we return client to poll chain to proceed with error response
    vgx_server_client__append_front( server, client );

    // Produce error
    if( alldown ) {
      ret = vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "All partitions down", true );
    }
    else if( defunct ) {
      ret = vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "Partition(s) down", false );
    }
    else {
      ret = vgx_server_response__produce_error( server, client, HTTP_STATUS__TooManyRequests, "Client backlog full", true );
    }
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
DLL_HIDDEN void vgx_server_dispatcher_dispatch__complete( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  // Ensure idle buffers are kept at a reasonable max size
  vgx_server_dispatcher_streams__trim_set( client->dispatcher.streams );

  // Discard set of dispatcher streams
  vgx_server_dispatcher_matrix__delete_stream_set( &server->matrix, &client->dispatcher.streams );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_dispatch__append_backlog( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  int ret = -1;
  // Don't allow backlog to include all clients.
  int64_t limit = (int64_t)server->pool.clients.capacity - server->pool.executors->sz;
  if( ATOMIC_READ_i32( &server->matrix.backlog_sz_atomic ) < limit  ) {
    CQwordQueue_t *B = server->matrix.backlog;
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__AWAIT_DISPATCH );
    uintptr_t client_addr = (uintptr_t)client;
    if( CALLABLE( B )->AppendNolock( B, (QWORD*)&client_addr ) == 1 ) {
      ATOMIC_INCREMENT_i32( &server->matrix.backlog_sz_atomic );
      ret = 0;
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_dispatch__apply_backlog( vgx_VGXServer_t *server ) {
  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;
  // Empty backlog
  if( ATOMIC_READ_i32( &matrix->backlog_sz_atomic ) == 0 ) {
    return 0;
  }
  int ret = 0;
  CQwordQueue_t *B = matrix->backlog;
  uintptr_t client_addr = 0;
  bool defunct = false;
  bool alldown = false;
  // Get first client in queue (but don't consume from queue yet)
  CALLABLE( B )->GetNolock( B, 0, (QWORD*)&client_addr );
  // Try to dispatch backlogged client
  vgx_VGXServerClient_t *client = (vgx_VGXServerClient_t*)client_addr;
  ret = __dispatch( &server->matrix, client, &defunct, &alldown );
  if( ret < 0 && defunct ) {
    // Client back to the front
    vgx_server_client__append_front( server, client );
    if( alldown ) {
      vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "All partitions down", true );
    }
    else {
      vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "Partition(s) down", false );
    }
  }
  // Backlog applied, consume the client from queue
  // Or unrecoverable, remove from backlog
  if( ret == 0 || defunct ) {
    CALLABLE( B )->NextNolock( B, (QWORD*)&client_addr );
    ATOMIC_DECREMENT_i32( &matrix->backlog_sz_atomic );
    ATOMIC_INCREMENT_i32( &matrix->backlog_count_atomic );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_dispatch__backlog_size( vgx_VGXServer_t *server ) {
  if( server->matrix.flags.enabled && server->matrix.backlog ) {
    return ATOMIC_READ_i32( &server->matrix.backlog_sz_atomic );
  }
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_dispatch__backlog_count( vgx_VGXServer_t *server ) {
  return ATOMIC_READ_i32( &server->matrix.backlog_count_atomic );
}
