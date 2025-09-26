/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_client.c
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

#include "_vgx.h"
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static void __client__clear_buffers( vgx_VGXServerBufferPair_t *buffers );

static void __client__unregister( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
static void __client__dump_all( vgx_VGXServer_t *server, const char *message );
static vgx_VGXServerClient_t * __client__register( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_URI_t *ClientURI );



SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN void vgx_server_client__trap_illegal_access( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  FATAL( 0xFFF, "Illegal access to dispatched client" );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN void vgx_server_client__assert_client_reset( vgx_VGXServerClient_t *client ) {
  if( client->request.state != VGXSERVER_CLIENT_STATE__RESET
      ||
      client->URI != NULL
      ||
      !VGX_LLIST_IS_OBJECT_ISOLATED( client )
    )
  {
    vgx_server_client__dump( client, "Expected client in reset state" );
  }
}



/*******************************************************************//**
 *
 * Append the client to the tail of the servers's linked list of
 * monitored clients.
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_client__append_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  VGX_LLIST_APPEND( server->pool.clients.iolist, client );
  CLIENT_STATE__ADD_IOCHAIN( client );

  if( client == client->chain.next ) {
    FATAL( 0xEEE, "Client chain self reference!" );
  }

}



/*******************************************************************//**
 *
 * Remove client from server's linked list of actively monitored clients.
 * If client is not in the list, no action.
 * 
 * Returns the client. 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerClient_t * vgx_server_client__yank_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  if( CLIENT_STATE__IN_IOCHAIN( client ) ) {
    VGX_LLIST_YANK( server->pool.clients.iolist, client );
    CLIENT_STATE__REMOVE_IOCHAIN( client );

    if( client == client->chain.next ) {
      FATAL( 0xEEE, "Client chain self reference!" );
    }

    return client;
  }
  else {
    return NULL;
  };
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_client__accept_into_iochain( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_URI_t *ClientURI ) {
  // Client goes to ready state
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__READY );
  // Insert client into iochain linked list
  vgx_server_client__append_front( server, client );
  // Set the URI
  client->URI = ClientURI;
  // Increment counters
  server->counters.perf->connected_clients++;
  server->counters.perf->total_clients++;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __client__clear_buffers( vgx_VGXServerBufferPair_t *buffers ) {
  iStreamBuffer.Clear( buffers->stream );
  iStreamBuffer.Clear( buffers->content );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __client__unregister( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  if( client && client->request.state != VGXSERVER_CLIENT_STATE__RESET ) {
    // Reset partial_ident
    client->partial_ident._bits = 0;

    // Release dispatcher resources
    if( client->dispatcher.streams ) {
      vgx_server_dispatcher_dispatch__complete( server, client );
    }

    // Close connection and delete
    if( client->URI ) {
      iURI.Delete( &client->URI );
    }
    
    // Remove client from linked list
    if( vgx_server_client__yank_front( server, client ) ) {

      if( server->counters.perf->connected_clients > 0 ) {
        server->counters.perf->connected_clients--;
      }
      else {
        FATAL( 0x000, "Negative client count! %lld", server->counters.perf->connected_clients );
      }
    }
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__RESET );

    iStreamBuffer.Trim( client->request.buffers.stream, CLIENT_REQUEST_STREAM_BUFFER_MAX_IDLE_CAPACITY );
    iStreamBuffer.Trim( client->request.buffers.content, CLIENT_REQUEST_CONTENT_BUFFER_MAX_IDLE_CAPACITY );
    iStreamBuffer.Trim( client->response.buffers.stream, CLIENT_RESPONSE_STREAM_BUFFER_MAX_IDLE_CAPACITY );
    iStreamBuffer.Trim( client->response.buffers.content, CLIENT_RESPONSE_CONTENT_BUFFER_MAX_IDLE_CAPACITY );
    ASSERT_CLIENT_RESET( client );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_client__dump( vgx_VGXServerClient_t *client, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerClient_t, client ) {

    CXLIB_OSTREAM( "id: %d", client->id );
    CXLIB_OSTREAM( "flags" );
    CXLIB_OSTREAM( "  .direct   : %d", (int)client->flags.direct );
    CXLIB_OSTREAM( "  .plugin   : %d", (int)client->flags.plugin );
    CXLIB_OSTREAM( "  .primary  : %d", (int)client->flags.primary );
    CXLIB_OSTREAM( "  .yielded  : %d", (int)client->flags.yielded );
    CXLIB_OSTREAM( "URI: %s", client->URI ? iURI.URI( client->URI ) : "null" );
    CXLIB_OSTREAM( "chain" );
    CXLIB_OSTREAM( "  .prev: %d @ %llp", client->chain.prev ? client->chain.prev->id : -1, client->chain.prev );
    CXLIB_OSTREAM( "  .next: %d @ %llp", client->chain.next ? client->chain.next->id : -1, client->chain.next );
    CXLIB_OSTREAM( "io_t0_ns: %lld", client->io_t0_ns );
    CXLIB_OSTREAM( "dispatcher" );
    vgx_VGXServerDispatcherStreamSet_t *set = client->dispatcher.streams;
    if( set ) {
      CXLIB_OSTREAM( "  .streams.responses.list: @llp (len=%d)", set->responses.list, set->responses.len );
    }
    else {
      CXLIB_OSTREAM( "  .streams.responses.list: null" );
    }
    CXLIB_OSTREAM( "  .channels" );
    vgx_VGXServerDispatcherChannel_t *head = client->dispatcher.channels.head;
    vgx_VGXServerDispatcherChannel_t *tail = client->dispatcher.channels.tail;
    if( head ) {
      CXLIB_OSTREAM( "    .head  : @llp (%d,%d,%d)", head, head->id.partition, head->id.replica, head->id.channel );
    }
    else {
      CXLIB_OSTREAM( "    .head  : null" );
    }
    if( tail ) {
      CXLIB_OSTREAM( "    .tail  : @llp (%d,%d,%d)", tail, tail->id.partition, tail->id.replica, tail->id.channel );
    }
    else {
      CXLIB_OSTREAM( "    .tail  : null" );
    }

    // request
    CXLIB_OSTREAM( "request" );
    vgx_server_request__dump( &client->request, NULL );

    // response
    CXLIB_OSTREAM( "response" );
    vgx_server_response__dump( &client->response, NULL );

    // partial_ident
    CXLIB_OSTREAM( "partial_ident" );
    CXLIB_OSTREAM( "  .defined     : %d", (int)client->partial_ident.defined );
    CXLIB_OSTREAM( "  .matrix" );
    CXLIB_OSTREAM( "    .width     : %d", (int)client->partial_ident.matrix.width );
    CXLIB_OSTREAM( "    .heigh     : %d", (int)client->partial_ident.matrix.height );
    CXLIB_OSTREAM( "    .depth     : %d", (int)client->partial_ident.matrix.depth );
    CXLIB_OSTREAM( "  .position" );
    CXLIB_OSTREAM( "    .partition : %d", (int)client->partial_ident.position.partition );
    CXLIB_OSTREAM( "    .replica   : %d", (int)client->partial_ident.position.replica );
    CXLIB_OSTREAM( "    .channel   : %d", (int)client->partial_ident.position.channel );

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
static void __client__dump_all( vgx_VGXServer_t *server, const char *message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServer_t, server ) {
    CXLIB_OSTREAM( "______________________________________________________________________" );
    CXLIB_OSTREAM( "STATE AT %s", message );
    CXLIB_OSTREAM( "______________________________________________________________________" );
    vgx_VGXServerClient_t *client = server->pool.clients.clients;
    vgx_VGXServerClient_t *end = client + server->pool.clients.capacity;
    int nactive = 0;
    CXLIB_OSTREAM( "CLIENT POOL:" );
    while( client < end ) {
      if( client->chain.prev ) {
        PRINT_OSTREAM( "    [%03d] <-- ", client->chain.prev->id );
      }
      else {
        PRINT_OSTREAM( "    [   ] <-- " );
      }

      if( client->URI == NULL ) {
        PRINT_OSTREAM( "[%03d <>]", client->id  );
      }
      else {
        CXSOCKET *sock = iURI.Sock.Input.Get( client->URI );
        int64_t fd = sock ? (int64_t)sock->s : LLONG_MIN;
        const char *uri = iURI.URI( client->URI );
        PRINT_OSTREAM( "[%03d <fd=%lld %s>]", client->id, fd, uri );
        ++nactive;
      }

      if( client->chain.next ) {
        PRINT_OSTREAM( " --> [%03d]", client->chain.next->id );
      }
      else {
        PRINT_OSTREAM( " --> [   ]" );
      }

      CXLIB_OSTREAM( "" );

      ++client;
    }
    CXLIB_OSTREAM( "" );

    int sz_chain = 0;
    CXLIB_OSTREAM( "CLIENT CHAIN:" );
    client = server->pool.clients.iolist.head;
    while( client ) {
      ++sz_chain;
      if( client == server->pool.clients.iolist.head ) {
        PRINT_OSTREAM( "CHAIN:    [%03d]", client->id );
        if( client->chain.prev != NULL ) {
          CXLIB_OSTREAM( "\n!!! CHAIN ERROR !!! Head [%03d] has previous [%03d]", client->id, client->chain.prev->id );
        }
      }
      else {
        PRINT_OSTREAM( "[%03d]", client->id );
        if( client->chain.prev == NULL ) {
          CXLIB_OSTREAM( "\n!!! CHAIN ERROR !!! Node [%03d] has no previous", client->id );
        }
      }

      if( client == server->pool.clients.iolist.tail ) {
        CXLIB_OSTREAM( ".", client->id );
        if( client->chain.next != NULL ) {
          CXLIB_OSTREAM( "\n!!! CHAIN ERROR !!! Tail [%03d] has next [%03d]", client->id, client->chain.next->id );
        }
      }

      if( client->chain.next != NULL ) {
        PRINT_OSTREAM( " --> " );
      }
      

      client = client->chain.next;
    }

    if( nactive > 0 ) {
      if( server->pool.clients.iolist.head == NULL ) {
        CXLIB_OSTREAM( "\n!!! CHAIN ERROR !!! Empty chain when %d active clients exist", nactive );
      }
    }

    if( sz_chain != nactive ) {
      CXLIB_OSTREAM( "\n!!! CHAIN ERROR !!! Expected chain of %d active clients, chain length was %d]", nactive, sz_chain );
    }

  } END_CXLIB_OBJ_DUMP;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_client__close( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  TRAP_ILLEGAL_CLIENT_ACCESS( server, client );

  __client__unregister( server, client );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_client__close_all( vgx_VGXServer_t *server ) {
  vgx_VGXServerClient_t *io_client = server->pool.clients.iolist.head;
  while( io_client ) {
    __client__unregister( server, io_client );
    io_client = io_client->chain.next;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_client__create_pool( vgx_VGXServer_t *server, int capacity ) {
  int ret = 0;
  XTRY {

    vgx_VGXServerClientPool_t *pool = &server->pool.clients;

    // Number of clients
    pool->capacity = capacity;
    
    // Number of clients voluntarily yielded when enough inbound data exists to continue processing
    pool->n_yielded = 0;

    pool->__rsv2 = 0;

    // Create client pool with the same number of slots as the file descriptor poll slots
    // NOTE: Slots 0 and 1 are reserved and will never be used.
    PALIGNED_ZARRAY_THROWS( pool->clients, vgx_VGXServerClient_t, pool->capacity, 0x001 );

    // No clients are active
    pool->iolist.head = NULL;
    pool->iolist.tail = NULL;

    // Usable clients start at slot 2. (Slots 0 and 1 are reserved.)
    vgx_VGXServerClient_t *client = pool->clients;
    vgx_VGXServerClient_t *end = pool->clients + pool->capacity;
    int id = 0;
    char label[512] = {0};
    while( client < end ) {
      if( id >= MAX_NCLIENT_SLOTS ) {
        FATAL( 0xEEE, "CODE ERROR. Max client ID limited to %d for technical reasons.", MAX_NCLIENT_SLOTS-1 );
      }

      // [Q1.1.1]
      client->id = id++;

      // [Q1.1.2.1]
      client->flags.direct = 0;
      // [Q1.1.2.2]
      client->flags.plugin = 0;
      // [Q1.1.2.3]
      client->flags.primary = 0;
      // [Q1.1.2.4]
      client->flags.yielded = 0;

      // [Q1.2]
      client->URI = NULL;

      // [Q1.3]
      client->chain.prev = NULL;

      // [Q1.4]
      client->chain.next = NULL;

      // [Q1.5]
      client->io_t0_ns = 0;

      // [Q1.6]
      client->dispatcher.streams = NULL;

      // [Q1.7]
      client->dispatcher.channels.head = NULL;

      // [Q1.8]
      client->dispatcher.channels.tail = NULL;

      // [Q2]
      // Initialize request struct
#ifdef _DEBUG
      snprintf( label, 511, "client.%d.request", client->id );
#endif
      if( vgx_server_request__init( &client->request, label ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }

      // [Q3]
      // Initialize response struct
#ifdef _DEBUG
      snprintf( label, 511, "client.%d.response", client->id );
#endif
      if( vgx_server_response__init( &client->response, label ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
      }

      // [Q4.1]
      // x-vgx-partial-ident
      client->partial_ident._bits = 0;

      // [Q4.2-3]
      memset( &client->dispatch_metas, 0, sizeof( client->dispatch_metas ) );

      // [Q4.4]
      client->tbase_ns = 0;
      
      // [Q4.5]
      client->p_x_vgx_backlog_word = NULL;
      
      // [Q4.6]
      client->env.main_server = DISPATCHER_MATRIX_ENABLED( server );
      client->env.__rsv_4_6_2 = 0;
      client->env.__rsv_4_6_3 = 0;
      client->env.__rsv_4_6_4 = 0;
      client->env.port_base = server->config.cf_server->front->port.base;
      client->env.port_offset = server->config.cf_server->front->port.offset;
      
      // [Q4.7]
      client->io_t1_ns = 0;

      // [Q4.8]
      client->__rsv_4_8 = 0;

      ++client;
    }

  }
  XCATCH( errcode ) {
    vgx_server_client__destroy_pool( server );
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
DLL_HIDDEN void vgx_server_client__destroy_pool( vgx_VGXServer_t *server ) {
  vgx_VGXServerClientPool_t *pool = &server->pool.clients;
  if( pool->clients ) {
    // Clients start at slot 2. (Slots 0 and 1 are reserved.)
    int init = 2;
    vgx_VGXServerClient_t *client = &pool->clients[ init ];
    vgx_VGXServerClient_t *end = pool->clients + pool->capacity;

    while( client < end ) {
      // URI
      iURI.Delete( &client->URI );
      
      // Request struct
      vgx_server_request__destroy( &client->request );

      // Response struct
      vgx_server_response__destroy( &client->response );

      ++client;
    }

    ALIGNED_FREE( pool->clients );

    pool->clients = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerClient_t * __client__register( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_URI_t *ClientURI ) {
  ASSERT_CLIENT_RESET( client );
  // Enter idle client into iochain
  vgx_server_client__accept_into_iochain( server, client, ClientURI );
  // Clear buffers
  __client__clear_buffers( &client->request.buffers );
  __client__clear_buffers( &client->response.buffers );
  // Prepare client for request
  vgx_server_request__client_request_ready( server, client );
  // Clear response
  vgx_server_response__client_response_reset( client );
  // Reset x-vgx-partial-ident
  client->partial_ident._bits = 0;
  // Reset metas
  memset( &client->dispatch_metas, 0, sizeof(client->dispatch_metas) );

  return client;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerClient_t * vgx_server_client__register( vgx_VGXServer_t *server, vgx_URI_t *ClientURI ) {
  vgx_VGXServerClientPool_t *pool = &server->pool.clients;

  // Scan pool to find available slot.
  // Usable clients start at slot 2. (Slots 0 and 1 are reserved.)
  vgx_VGXServerClient_t *start = &pool->clients[ CLIENT_FD_START ];
  vgx_VGXServerClient_t *end = pool->clients + pool->capacity;
  vgx_VGXServerClient_t *client = start;
  while( client < end ) {
    if( client->URI ) {
      ++client;
      continue;
    }
    return __client__register( server, client, ClientURI );
  }

  // All clients busy, now check if some appear stuck and clean up
  int64_t cutoff_age_ns = 5LL * 1000000000LL; // If 5 seconds since last request, force close the connection
  int64_t now_ns = __GET_CURRENT_NANOSECOND_TICK();
  client = start;
  while( client < end ) {
    if( client->request.state == VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE ) {
      int64_t deadline_ns = client->io_t0_ns + cutoff_age_ns;
      if( now_ns > deadline_ns ) {
        __client__unregister( server, client );
        return __client__register( server, client, ClientURI );
      }
    }
    ++client;
  }

  // No available client
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_client__count_TCS( vgx_VGXServer_t *server ) {
  int n = 0;
  vgx_VGXServerClient_t *io_client = server->pool.clients.iolist.head;
  while( io_client ) {
    ++n;
    io_client = io_client->chain.next;
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_client__count_active_TCS( vgx_VGXServer_t *server ) {
  int n = 0;
  vgx_VGXServerClient_t *io_client = server->pool.clients.iolist.head;
  while( io_client ) {
    if( __vgxserver_client_state__busy( &io_client->request ) ) {
      ++n;
    }
    io_client = io_client->chain.next;
  }
  return n;
}
