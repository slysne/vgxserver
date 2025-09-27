/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    matrix.c
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



#define CHANNEL_CONNECT_TIMEOUT 100



static void __matrix__assert_bufferset( const vgx_VGXServerDispatcherMatrix_t *matrix );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__dump( const vgx_VGXServerDispatcherMatrix_t *matrix, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherMatrix_t, matrix ) {
    CXLIB_OSTREAM( "flags");
    CXLIB_OSTREAM( "  enabled          = %d", (int)matrix->flags.enabled );
    CXLIB_OSTREAM( "  allow_incomplete = %d", (int)matrix->flags.allow_incomplete );
    CXLIB_OSTREAM( "  rsv1113          = %d", (int)matrix->flags.__rsv_1_1_1_3 );
    CXLIB_OSTREAM( "  rsv1114          = %d", (int)matrix->flags.__rsv_1_1_1_4 );
    CXLIB_OSTREAM( "n_ch_yielded       = %d", matrix->n_ch_yielded );
    CXLIB_OSTREAM( "partition");
    CXLIB_OSTREAM( "  width              = %d", matrix->partition.width );
    CXLIB_OSTREAM( "  nopen_channels_MCS = %d", (int)matrix->partition.nopen_channels_MCS );
    CXLIB_OSTREAM( "  nmax_channels_MCS  = %d", (int)matrix->partition.nmax_channels_MCS );
    CXLIB_OSTREAM( "  list");
    for( int i=0; i<matrix->partition.width; i++ ) {
      vgx_server_dispatcher_partition__dump( &matrix->partition.list[i], NULL );
    }
    CXLIB_OSTREAM( "active");
    CXLIB_OSTREAM( "  head");
    vgx_server_client__dump( matrix->active.head, NULL );
    CXLIB_OSTREAM( "  tail");
    vgx_server_client__dump( matrix->active.tail, NULL );
    CXLIB_OSTREAM( "stream_set_pool");
    CXLIB_OSTREAM( "  sets");
    vgx_server_dispatcher_streams__dump_sets( matrix->stream_set_pool.sets, NULL );
    if( matrix->stream_set_pool.stack && matrix->stream_set_pool.idle ) {
      CXLIB_OSTREAM( "  *stack           = @ %llp", *matrix->stream_set_pool.stack );
      CXLIB_OSTREAM( "  *idle            = @ %llp (used=%llu)", *matrix->stream_set_pool.idle, (matrix->stream_set_pool.idle - matrix->stream_set_pool.stack) );
    }
    CXLIB_OSTREAM( "lock               = @ %llp", &matrix->lock );
    CXLIB_OSTREAM( "asynctask          = @ %llp", matrix->asynctask );
    CXLIB_OSTREAM( "backlog            = @ %llp", matrix->backlog );
    CXLIB_OSTREAM( "backlog_sz_atomic  = %d", (int)ATOMIC_READ_i32( &((vgx_VGXServerDispatcherMatrix_t*)matrix)->backlog_sz_atomic ) );
    CXLIB_OSTREAM( "backlog_count_atomic= %d", (int)ATOMIC_READ_i32( &((vgx_VGXServerDispatcherMatrix_t*)matrix)->backlog_count_atomic ) );
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
DLL_HIDDEN int vgx_server_dispatcher_matrix__init( vgx_VGXServer_t *server, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;
 
  XTRY {
    vgx_VGXServerDispatcherConfig_t *cf = server->config.cf_server->dispatcher;

    if( cf && cf->shape.width > 0 ) {
      if( iVGXServer.Config.Dispatcher.Verify( cf, CSTR__error ) == false ) {
        THROW_SILENT( CXLIB_ERR_CONFIG, 0x002 );
      }

      // [Q1.1.1.1]
      // Dispatcher enabled
      matrix->flags.enabled = true;

      // [Q1.1.1.2]
      matrix->flags.allow_incomplete = cf->allow_incomplete;

      // [Q1.1.1.3]
      matrix->flags.__rsv_1_1_1_3 = false;

      // [Q1.1.1.4]
      matrix->flags.__rsv_1_1_1_4 = false;

      // [Q1.1.2]
      // Number of channels voluntarily yielded when enough inbound data exists to continue processing
      matrix->n_ch_yielded = 0;

      // [Q1.2.1]
      // Number of partitions
      matrix->partition.width = cf->shape.width;

      // [Q1.2.2.1]
      // Number of active channels across all partition replicas
      matrix->partition.nopen_channels_MCS = 0;

      // [Q1.2.2.1]
      // Maximum channels across all partition replicas
      matrix->partition.nmax_channels_MCS = 0;

      // [Q1.3]
      // Allocate partitions
      if( (matrix->partition.list = calloc( matrix->partition.width + 1LL, sizeof( vgx_VGXServerDispatcherPartition_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }

      // Initialize all partitions from config
      BYTE partition_id = 0;
      vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list;
      vgx_VGXServerDispatcherPartition_t *end = partition + matrix->partition.width;
      vgx_VGXServerDispatcherConfigPartition_t *cf_partition = cf->partitions;
      while( partition < end ) {
        if( vgx_server_dispatcher_partition__init( server, partition_id++, partition++, cf_partition++ ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }
      }

      // [Q1.4]
      matrix->active.head = NULL;

      // [Q1.5]
      matrix->active.tail = NULL;

      // [Q1.6]
      // Collection of stream set instances
      matrix->stream_set_pool.sets = vgx_server_dispatcher_streams__new_sets( server, cf );

      // [Q1.7]
      // Array of pointers to stream sets, plus terminating NULL pointer
      if( (matrix->stream_set_pool.stack = calloc( matrix->stream_set_pool.sets->sz + 1LL, sizeof( vgx_VGXServerDispatcherStreamSet_t* ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
      }

      // Hook up stack entries to stream set instances
      for( int i=0; i<matrix->stream_set_pool.sets->sz; i++ ) {
        matrix->stream_set_pool.stack[i] = &matrix->stream_set_pool.sets->list[i];
      }

      // [Q1.8]
      // Points to top of stream set stack
      matrix->stream_set_pool.idle = matrix->stream_set_pool.stack;

      // [Q2.7]
      if( (matrix->backlog = CQwordQueueNew( 64 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }

      // [Q2.1/2/3/4/5]
      INIT_SPINNING_CRITICAL_SECTION( &matrix->lock.lock, 4000 );

      // [Q2.8.1]
      matrix->backlog_sz_atomic = 0;

      // [Q2.8.2]
      matrix->backlog_count_atomic = 0;

      // [Q2.6]
      if( (matrix->asynctask = vgx_server_dispatcher_asynctask__new( server )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
      }

    }
    else {
      // [Q1.1.1.1]
      // Dispatcher disabled
      matrix->flags.enabled = false;
    }

  }
  XCATCH( errcode ) {
    ret = -1;
    vgx_server_dispatcher_matrix__clear( server );
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
DLL_HIDDEN void vgx_server_dispatcher_matrix__clear( vgx_VGXServer_t *server ) {

  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;

  if( matrix->flags.enabled ) {

    // Destroy the connecter thread
    if( matrix->asynctask ) {
      vgx_server_dispatcher_asynctask__destroy( server, &matrix->asynctask );
    }

    matrix->flags.enabled = false;

    // Destroy the client overload queue
    if( matrix->backlog ) {
      COMLIB_OBJECT_DESTROY( matrix->backlog );
      matrix->backlog = NULL;
      DEL_CRITICAL_SECTION( &matrix->lock.lock );
    }

    // --------------------------------
    // Delete partition list 
    if( matrix->partition.list ) {

      // Clean up each partition in list
      vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list; 
      vgx_VGXServerDispatcherPartition_t *end = partition + matrix->partition.width;
      while( partition < end ) {
        // Delete replica list for partition
        vgx_server_dispatcher_partition__clear( partition++ );
      }
      free( matrix->partition.list );
      matrix->partition.list = NULL;
    }
    matrix->partition.width = 0;
    // --------------------------------


    // --------------------------------
    // Check for exceptional state
    if( matrix->active.head || matrix->active.tail ) {
      // BAD (active clients at destruction time)
    }
    // --------------------------------


    // --------------------------------
    // Clean up stream set stack
    if( matrix->stream_set_pool.stack ) {
      if( matrix->stream_set_pool.idle != matrix->stream_set_pool.stack ) {
        // BAD (active stream sets at destruction time)
      }
      free( matrix->stream_set_pool.stack );
      matrix->stream_set_pool.stack = NULL;
    }

    matrix->stream_set_pool.idle = NULL;
    // --------------------------------


    // --------------------------------
    // Delete stream sets instance
    vgx_server_dispatcher_streams__delete_sets( &matrix->stream_set_pool.sets );
    // --------------------------------

  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_matrix__width( const vgx_VGXServer_t *server ) {
  return server->matrix.partition.width;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void  vgx_server_dispatcher_matrix__abort_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client ) {
  vgx_VGXServerDispatcherChannel_t *channel;
  while( (channel = client->dispatcher.channels.tail) != NULL ) {
    // Partial I/O performed on aborted channel. We must close it to abandon I/O in delegate service
    if( CHANNEL_INFLIGHT( channel ) ) {
      vgx_server_dispatcher_matrix__channel_close( matrix, channel );
    }
    // Clean abort
    else {
      vgx_server_dispatcher_channel__return( channel );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __ignore_incomplete( vgx_VGXServerDispatcherPartition_t *partition, vgx_VGXServerClient_t *client ) {
  x_vgx_partial__header *dh = &client->dispatcher.streams->responses.headers[ partition->id.partition ];
  vgx_server_dispatcher_partial__reset_header( dh );
  dh->status = X_VGX_PARTIAL_STATUS__EMPTY;
  client->dispatch_metas.level.incomplete_parts++;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_optimal_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, int replica_affinity, bool *defunct ) {
  vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list;
  vgx_VGXServerDispatcherPartition_t *end = partition + matrix->partition.width;
  vgx_VGXServerDispatcherChannel_t *channel;

  // Get all dispatcher channels needed for request
  while( partition < end ) {
    // Get the optimal channel for this partition
    if( (channel = vgx_server_dispatcher_matrix__get_partition_channel( matrix, partition, replica_affinity, defunct )) == NULL ) {
      // Ignore defunct partials if so configured
      if( defunct && matrix->flags.allow_incomplete ) {
        __ignore_incomplete( partition, client );
        goto next;
      }
      // all channels are busy (or down)
      return -1;
    }

    // Assign front and dispatcher buffer references and
    // append channel to client's set of dispatcher channels
    vgx_server_dispatcher_client__channel_append( client, channel );
    
    // Next
  next:
    ++partition;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_primary_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, bool *defunct ) {
  vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list;
  vgx_VGXServerDispatcherPartition_t *end = partition + matrix->partition.width;
  vgx_VGXServerDispatcherChannel_t *channel;

  // Get all dispatcher channels needed for request
  while( partition < end ) {
    // Get the a channel to the primary replica for this partition
    if( (channel = vgx_server_dispatcher_matrix__get_primary_channel( matrix, partition++, defunct )) == NULL ) {
      return -1; // all channels are busy (or down)
    }

    // Assign front and dispatcher buffer references and
    // append channel to client's set of dispatcher channels
    vgx_server_dispatcher_client__channel_append( client, channel );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_target_optimal_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, int replica_affinity, bool *defunct ) {
  vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list;
  vgx_VGXServerDispatcherChannel_t *channel;

  // modulo width in case target is a hash
  client->dispatcher.streams->prequest->target_partial %= (int8_t)matrix->partition.width;
  partition += client->dispatcher.streams->prequest->target_partial;

  // Get the optimal channel for this partition
  if( (channel = vgx_server_dispatcher_matrix__get_partition_channel( matrix, partition, replica_affinity, defunct )) == NULL ) {
    // all channels are busy (or down)
    return -1;
  }

  // Assign front and dispatcher buffer references and
  // append channel to client's set of dispatcher channels
  vgx_server_dispatcher_client__channel_append( client, channel );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_target_primary_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, bool *defunct ) {
  vgx_VGXServerDispatcherPartition_t *partition = matrix->partition.list;
  vgx_VGXServerDispatcherChannel_t *channel;

  // modulo width in case target is a hash
  client->dispatcher.streams->prequest->target_partial %= (int8_t)matrix->partition.width;
  partition += client->dispatcher.streams->prequest->target_partial;
  // Get a channel for the primary partition
  if( (channel = vgx_server_dispatcher_matrix__get_primary_channel( matrix, partition, defunct )) == NULL ) {
    return -1;
  }

  // Assign front and dispatcher buffer references and
  // append channel to client's set of dispatcher channels
  vgx_server_dispatcher_client__channel_append( client, channel );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_matrix__assign_client_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, bool *defunct, bool *alldown ) {
  vgx_VGXServerRequest_t *request = client->dispatcher.streams->prequest;

  // Reset incomplete partition counter (for use in allow-incomplete mode)
  client->dispatch_metas.level.incomplete_parts = 0;

  // Dispatch to all partitions
  if( request->target_partial < 0 ) {
    // Find optimal replicas
    if( CLIENT_USE_ANY_REPLICA( client ) ) {
      if( __get_optimal_channels( matrix, client, request->replica_affinity, defunct ) < 0 ) {
        goto dispatcher_overload; // all channels are busy for this partition
      }
    }
    // Primary only
    else {
      if( __get_primary_channels( matrix, client, defunct ) < 0 ) {
        goto dispatcher_overload; // all channels are busy for this partition
      }
    }
  }
  // Dispatch to single target partition
  else {
    // Find optimal replica within target partition
    if( CLIENT_USE_ANY_REPLICA( client ) ) {
      if( __get_target_optimal_channel( matrix, client, request->replica_affinity, defunct ) < 0 ) {
        goto dispatcher_overload;
      }
    }
    // Primary target only
    else {
      if( __get_target_primary_channel( matrix, client, defunct ) < 0 ) {
        goto dispatcher_overload;
      }
    }
  }

  // Fail request if no partitions are available, even if we allow incomplete results
  if( vgx_server_dispatcher_client__no_channels( client ) ) {
    goto all_partitions_down;
  }

  return 0;


all_partitions_down:
  *alldown = true;
  return -1;

dispatcher_overload:

  // Return all allocated channels
  vgx_server_dispatcher_matrix__abort_channels( matrix, client );
  
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__channel_close( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherChannel_t *channel ) {

  vgx_server_dispatcher_channel__return( channel );

  SYNCHRONIZE_ON( matrix->lock ) {
    CXSOCKET *psock = &channel->socket;
    cxclose( &psock );
    channel->flag.connected_MCS = false;
    channel->request.counter = -1;
  } RELEASE;

  CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__RESET );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __channel_connect( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherChannel_t *channel ) {

  int ret = 0;

  SYNCHRONIZE_ON( matrix->lock ) {
    // Make sure any existing socket is closed
    CXSOCKET *psock = &channel->socket;
    cxclose( &psock );
    channel->flag.connected_MCS = false;

    // Get replica 
    struct addrinfo *address = channel->parent.permanent.replica->addrinfo_MCS;

    // Create a new socket
    if( address == NULL || cxsocket( &channel->socket, AF_INET, SOCK_STREAM, IPPROTO_TCP, true, 0, false ) == NULL ) {
      ret = -1;
    }
    // Connect in non-blocking mode
    else {
      // Non-blocking socket
      cxsocket_set_nonblocking( &channel->socket );

      // Expect this to return -1.
      // We can't wait for the connection to be established since we have other sockets to process.
      // The main I/O poll loop needs to check the state of connection.
      struct addrinfo *a = cxlowaddr( address );
      short revents = 0;
      if( cxconnect( &channel->socket, a->ai_addr, a->ai_addrlen, 0, &revents ) < 0 ) {
        // ignore, we can't connect and that's that
      }

      // Mark as connected
      channel->flag.connected_MCS = true;
    }
  } RELEASE;

  if( ret == 0 ) {
    // Initialize request counter and bytes to zero
    channel->request.counter = 0;
    // Trigger identifier request to be sent upon successful connection
    channel->remain_ident = channel->sz_ident;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerDispatcherChannel_t * __matrix__get_ready_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica, int cost ) {

  vgx_VGXServerDispatcherChannel_t *channel = vgx_server_dispatcher_replica__pop_channel( replica );

  // Channel is in ready state, check that socket is actually connected
  if( channel->state == VGXSERVER_CHANNEL_STATE__READY ) {
    // Close this channel if socket is not connected due to server-side error
    if( cxsocket_is_connected( &channel->socket ) == false ) {
      vgx_server_dispatcher_matrix__channel_close( matrix, channel );
    }
  }

  // Transition channel to ready state if needed
  if( channel->state == VGXSERVER_CHANNEL_STATE__RESET ) {

    // Perform non-blocking socket connect
    if( __channel_connect( matrix, channel ) < 0 ) {
      // Connect failed likely due to address error

      // Force backlog to zero
      if( channel->response ) {
        channel->response->x_vgx_backlog = 0;
      }

      // Return channel to the replica
      vgx_server_dispatcher_replica__push_channel( replica, channel );

      // Flag replica as defunct
      vgx_server_dispatcher_matrix__set_replica_defunct( matrix, replica );

      return NULL;
    }

    // Channel may not yet be connected. We monitor connection state in I/O loop.
    CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__READY );
  }

  channel->request.cost = cost;

  return channel;
}



/*******************************************************************//**
 * Select a channel from the most available replica in this partition.
 * Availability is determined from a combination of the replica's
 * static priority and the number of currently active channels for the
 * replica.
 *
 * Return:  channel from selected replica
 *          or NULL if no channels available
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherChannel_t * vgx_server_dispatcher_matrix__get_partition_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherPartition_t *partition, int replica_affinity, bool *defunct ) {
  vgx_VGXServerDispatcherChannel_t *channel;
    
  do {
    int cost = INT_MAX;
    vgx_VGXServerDispatcherReplica_t *replica = NULL;

    // Replica affinity specified
    if( replica_affinity >= 0 ) {
      replica = &partition->replica.list[ replica_affinity % partition->replica.height ];
      // Ok, use this replica
      if( (cost = vgx_server_dispatcher_replica__cost( replica )) < REPLICA_MAX_COST ) {
        goto accept;
      }
      // Specified replica not available
      replica = NULL;
    }

    // Select the replica with lowest cost
    vgx_VGXServerDispatcherReplica_t *cursor = partition->replica.list;
    vgx_VGXServerDispatcherReplica_t *end = cursor + partition->replica.height;
    int min_cost = REPLICA_MAX_COST; // the cost to beat to get selected

    while( cursor < end ) {
      // Is this replica less expensive?
      if( (cost = vgx_server_dispatcher_replica__cost( cursor )) < min_cost ) {
        replica = cursor;
        min_cost = cost;
      }
      ++cursor;
    }

    // No replica with low enough cost was found
    if( replica == NULL || REPLICA_EXHAUSTED( replica ) ) {
      // All replicas defunct
      if( cost >= REPLICA_DEFUNCT_COST_FLOOR ) {
        *defunct = true;
      }
      return NULL;
    }

accept:
    // Get channel and attempt nonblocking connect as needed
    channel = __matrix__get_ready_channel( matrix, replica, cost );

  } while( channel == NULL ); // connect error, try to get another channel from replica

  return channel;
}



/*******************************************************************//**
 * Select a channel from the primary replica in this partition.
 *
 * Return:  channel from primary replica
 *          or NULL if no channels available
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherChannel_t * vgx_server_dispatcher_matrix__get_primary_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherPartition_t *partition, bool *defunct ) {
  vgx_VGXServerDispatcherChannel_t *channel;
    
  do {
    // Select the primary replica
    vgx_VGXServerDispatcherReplica_t *cursor = partition->replica.list;
    vgx_VGXServerDispatcherReplica_t *end = cursor + partition->replica.height;
    vgx_VGXServerDispatcherReplica_t *primary = NULL;
    int cost = INT_MAX;
    // Find the primary
    while( cursor < end ) {
      // Primary AND it is not defunct
      if( cursor->primary && (cost = vgx_server_dispatcher_replica__cost( cursor )) < REPLICA_MAX_COST ) {
        primary = cursor;
        break;
      }
      ++cursor;
    }

    // No primary found or no channels
    if( primary == NULL || REPLICA_EXHAUSTED( primary ) ) {
      // Primary defunct
      if( cost > REPLICA_DEFUNCT_COST_FLOOR ) {
        *defunct = true;
      }
      return NULL;
    }

    // Get channel and attempt nonblocking connect as needed
    channel = __matrix__get_ready_channel( matrix, primary, cost );

  } while( channel == NULL ); // connect error, try to get another channel from replica

  return channel;
}



/*******************************************************************//**
 *
 * Append client to end of matrix's chain of active clients
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__client_append_matrix( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client ) {
  VGX_LLIST_APPEND( matrix->active, client );
  CLIENT_STATE__ADD_IOCHAIN( client );
}



/*******************************************************************//**
 *
 * Remove client from matrix's chain of active clients if client is
 * present in the chain, then return the client. If client is not
 * present in the chain no action is performed and NULL is returned.
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerClient_t * vgx_server_dispatcher_matrix__client_yank_matrix( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client ) {
  if( CLIENT_STATE__IN_IOCHAIN( client ) ) {
    VGX_LLIST_YANK( matrix->active, client );
    CLIENT_STATE__REMOVE_IOCHAIN( client );
    return client;
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
__inline static void __matrix__assert_stream_set( const vgx_VGXServerDispatcherMatrix_t *matrix ) {
  if( *matrix->stream_set_pool.idle == NULL ) {
    vgx_server_dispatcher_matrix__dump( matrix, "No more stream sets" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __matrix__assert_stream_set_stack( const vgx_VGXServerDispatcherMatrix_t *matrix ) {
  // Safeguard
  if( matrix->stream_set_pool.idle <= matrix->stream_set_pool.stack ) {
    VGX_SERVER_DISPATCHER_FATAL( 0xEEE, "Attempted to push stream set on full stack" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherStreamSet_t * vgx_server_dispatcher_matrix__new_stream_set( vgx_VGXServerDispatcherMatrix_t *matrix ) {
  // Ensure availability
  __matrix__assert_stream_set( matrix );

  // (Allocation implemented by pre-allocated stack)
  // Pop stream set, stack pointer down
  vgx_VGXServerDispatcherStreamSet_t *set = *matrix->stream_set_pool.idle++;

  // By default reference the set's own request object
  set->prequest = &set->_request;

  // Return a clean set
  


  return set;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__delete_stream_set( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherStreamSet_t **stream_set ) {
  if( stream_set && *stream_set ) {
    __matrix__assert_stream_set_stack( matrix );

    // (Allocation implemented by pre-allocated stack)
    // Push stream set, stack pointer up
    *(--matrix->stream_set_pool.idle) = *stream_set;
    *stream_set = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__set_replica_defunct( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica ) {

  SYNCHRONIZE_ON( matrix->lock ) {
    if( !REPLICA_IS_DEFUNCT_MCS( replica ) ) {
      // Set priority deboost to prevent replica from being selected
      REPLICA_SET_DEFUNCT_MCS( replica );

      // Delete address 
      iURI.DeleteAddrInfo( &replica->addrinfo_MCS );
    }

  } RELEASE;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_matrix__deboost_replica( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica ) {
  SYNCHRONIZE_ON( matrix->lock ) {
    if( !REPLICA_IS_TMP_DEBOOST_MCS( replica ) ) {
      // Set temporary priority deboost to lower the chance of a replica being selected
      REPLICA_SET_TMP_DEBOOST_MCS( replica );
    }
  } RELEASE;
}
