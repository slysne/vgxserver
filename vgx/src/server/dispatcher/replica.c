/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    replica.c
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
DLL_HIDDEN void vgx_server_dispatcher_replica__dump( const vgx_VGXServerDispatcherReplica_t *replica, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherReplica_t, replica ) {
    CXLIB_OSTREAM( "id" );
    CXLIB_OSTREAM( "  replica      = %d", replica->id.replica );
    CXLIB_OSTREAM( "  partition    = %d", replica->id.partition );
    CXLIB_OSTREAM( "resource" );
    CXLIB_OSTREAM( "  cost         = %d", (int)replica->resource.cost );
    CXLIB_OSTREAM( "  priority.mix = %d", (int)replica->resource.priority.mix );
    CXLIB_OSTREAM( "    base       = %d", (int)replica->resource.priority.base );
    CXLIB_OSTREAM( "    deboost    = %d", (int)replica->resource.priority.deboost );
    CXLIB_OSTREAM( "flags_MCS" );
    CXLIB_OSTREAM( "  defunct_raised  = %d", (int)replica->flags_MCS.defunct_raised );
    CXLIB_OSTREAM( "  defunct_caught  = %d", (int)replica->flags_MCS.defunct_caught );
    CXLIB_OSTREAM( "  initial_attempt = %d", (int)replica->flags_MCS.initial_attempt );
    CXLIB_OSTREAM( "  __rsv_1_2_2_1_4 = %d", (int)replica->flags_MCS.__rsv_1_2_2_1_4 );
    CXLIB_OSTREAM( "  __rsv_1_2_2_1_5 = %d", (int)replica->flags_MCS.__rsv_1_2_2_1_5 );
    CXLIB_OSTREAM( "  tmp_deboost     = %d", (int)replica->flags_MCS.tmp_deboost );
    CXLIB_OSTREAM( "  __rsv_1_2_2_1_7 = %d", (int)replica->flags_MCS.__rsv_1_2_2_1_7 );
    CXLIB_OSTREAM( "  __rsv_1_2_2_1_8 = %d", (int)replica->flags_MCS.__rsv_1_2_2_1_8 );
    CXLIB_OSTREAM( "depth             = %d", (int)replica->depth );
    CXLIB_OSTREAM( "primary           = %d", (int)replica->primary );
    CXLIB_OSTREAM( "__rsv_1_2_2_4     = %d", (int)replica->__rsv_1_2_2_4 );
    CXLIB_OSTREAM( "remote            = %s", iURI.URI( replica->remote ) );
    CXLIB_OSTREAM( "addrinfo          = @ %llp", replica->addrinfo_MCS );
    CXLIB_OSTREAM( "__rsv_1_5         = %llu", replica->__rsv_1_5 );
    CXLIB_OSTREAM( "channel_pool" );
    CXLIB_OSTREAM( "  data         = @ %llp", replica->channel_pool.data );
    if( replica->channel_pool.stack && replica->channel_pool.idle ) {
      CXLIB_OSTREAM( "  stack        = @ %llp", *replica->channel_pool.stack );
      CXLIB_OSTREAM( "  idle         = @ %llp (used=%llu)", *replica->channel_pool.idle, (replica->channel_pool.idle - replica->channel_pool.stack) );
    }
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
DLL_HIDDEN int vgx_server_dispatcher_replica__init( vgx_VGXServer_t *server, BYTE replica_id, vgx_VGXServerDispatcherReplica_t *replica, const vgx_VGXServerDispatcherConfigReplica_t *cf_replica, vgx_VGXServerDispatcherPartition_t *partition ) {
  int ret = 0;
  XTRY {
    if( replica_id >= SERVER_MATRIX_HEIGHT_MAX ) {
      THROW_ERROR( CXLIB_ERR_CONFIG, 0x000 );
    }

    // Replica ID
    // [Q1.1.1]
    replica->id.replica = replica_id;
    // [Q1.1.2]
    replica->id.partition = partition->id.partition;

    int8_t cost_base = cf_replica->settings.priority;

    // [Q1.2.1.1]
    // Dynamically updated cost. Incremented in steps of the priority base.
    replica->resource.cost = cost_base;

    // [Q1.2.1.2]
    // Priority (lower value means replica is prioritized)
    replica->resource.priority.base = cost_base;
    // Initialized to defunct. Will be resolved asynchronously by asynctask thread
    REPLICA_SET_DEFUNCT_MCS( replica );

    // [Q1.2.2.1]
    // Starts out defunct to trigger initial connect
    replica->flags_MCS.defunct_raised = true;
    replica->flags_MCS.defunct_caught = false;
    replica->flags_MCS.initial_attempt = true;
    replica->flags_MCS.__rsv_1_2_2_1_4 = false;
    replica->flags_MCS.__rsv_1_2_2_1_5 = false;
    replica->flags_MCS.tmp_deboost = false;
    replica->flags_MCS.__rsv_1_2_2_1_7 = false;
    replica->flags_MCS.__rsv_1_2_2_1_8 = false;

    // [Q1.2.2.2]
    // The number of channels per replica
    replica->depth = cf_replica->settings.channels;

    // [Q1.2.2.3]
    // True if this is a primary (writable) replica
    replica->primary = cf_replica->settings.flag.writable;

    // [Q1.2.2.4]
    replica->__rsv_1_2_2_4 = 0;

    // [Q1.3]
    // New URI copy from config
    if( (replica->remote = iURI.Clone( cf_replica->uri )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // [Q1.4]
    // Address will be resolved asynchronously in asynctask thread
    replica->addrinfo_MCS = NULL;

    // [Q1.5]
    replica->__rsv_1_5 = 0;

    // [Q1.6]
    // Channel pool array
    if( (replica->channel_pool.data = calloc( replica->depth, sizeof( vgx_VGXServerDispatcherChannel_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // [Q1.7]
    // Channel stack (NULL terminated)
    if( (replica->channel_pool.stack = calloc( replica->depth + 1LL, sizeof( vgx_VGXServerDispatcherChannel_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }

    // Hook up stack entries to channel objects
    for( int c=0; c<replica->depth; c++ ) {
      replica->channel_pool.stack[c] = &replica->channel_pool.data[c];
    }

    // [Q1.8]
    // Intialize idle to top of stack
    replica->channel_pool.idle = replica->channel_pool.stack;

    // Initialize channel objects
    int8_t channel_id = 0;
    vgx_VGXServerDispatcherChannel_t *channel = replica->channel_pool.data;
    vgx_VGXServerDispatcherChannel_t *end = channel + replica->depth;
    while( channel < end ) {
      if( vgx_server_dispatcher_channel__init( server, channel_id++, channel++, replica, partition ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
    }

  }
  XCATCH( errcode ) {
    vgx_server_dispatcher_replica__clear( replica );
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
DLL_HIDDEN void vgx_server_dispatcher_replica__clear( vgx_VGXServerDispatcherReplica_t *replica ) {
  if( replica ) {
    // [Q1.8]
    replica->channel_pool.idle = NULL;

    // [Q1.7]
    free( replica->channel_pool.stack );
    replica->channel_pool.stack = NULL;

    // [Q1.6]
    // Clear channel objects
    if( replica->channel_pool.data ) {
      vgx_VGXServerDispatcherChannel_t *channel = replica->channel_pool.data;
      vgx_VGXServerDispatcherChannel_t *end = channel + replica->depth;
      while( channel < end ) {
        vgx_server_dispatcher_channel__clear( channel++ );
      }
      free( replica->channel_pool.data );
      replica->channel_pool.data = NULL;
    }

    // [Q1.5]
    replica->__rsv_1_5 = 0;

    // [Q1.4]
    iURI.DeleteAddrInfo( &replica->addrinfo_MCS );

    // [Q1.3]
    iURI.Delete( &replica->remote );

    // [Q1.2.2.4]
    replica->__rsv_1_2_2_4 = 0;

    // [Q1.2.2.3]
    replica->primary = 0;

    // [Q1.2.2.2]
    replica->depth = 0;

    // [Q1.2.2.1]
    replica->flags_MCS._bits = 0;

    // [Q1.2.1.2]
    *(char*)&replica->resource.priority = -1;

    // [Q1.2.1.1]
    replica->resource.cost = -1;

    // [Q1.1.2]
    replica->id.partition = SERVER_MATRIX_WIDTH_NONE;

    // [Q1.1.1]
    replica->id.replica = SERVER_MATRIX_HEIGHT_NONE;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_replica__push_channel( vgx_VGXServerDispatcherReplica_t *replica, vgx_VGXServerDispatcherChannel_t *channel ) {
  // Safeguard
  if( replica->channel_pool.idle <= replica->channel_pool.stack ) {
    vgx_server_dispatcher_channel__invalid_push( channel );
  }

  // Push channel, stack pointer up
  *(--replica->channel_pool.idle) = channel;

  // Some channels still busy, reduce replica cost
  if( replica->channel_pool.idle > replica->channel_pool.stack ) {
    // No backlog information, reduce cost by one base unit
    if( channel->response == NULL || channel->response->x_vgx_backlog < 0 ) {
      replica->resource.cost -= (short)replica->resource.priority.base;
    }
    // Backend self-reported backlog determines current replica cost
    else {
      int true_cost = channel->response->x_vgx_backlog * (int)replica->resource.priority.base;
      replica->resource.cost = true_cost > SHRT_MAX ? SHRT_MAX : (short)true_cost;
    }
  }
  // All channels idle, reset cost to base (regardless of true backend cost)
  else {
    replica->resource.cost = (short)replica->resource.priority.base;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherChannel_t * vgx_server_dispatcher_replica__pop_channel( vgx_VGXServerDispatcherReplica_t *replica ) {

  // One less available channel, increase replica cost
  // Assume no wrap to negative possible sice we will not be able to
  // select this channel in the first place if cost is above REPLICA_MAX_COST (16256)
  replica->resource.cost += (short)replica->resource.priority.base;

  // Pop channel, stack pointer down
  vgx_VGXServerDispatcherChannel_t *channel = *replica->channel_pool.idle++;

  return channel;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_replica__cost( const vgx_VGXServerDispatcherReplica_t *replica ) {
  // Max cost = 32640
  // When deboost is set priority.mix will automatically go beyond max cost
  // Normally priority.mix equals priority base, which is then added to the dynamic
  // cost which increases as channels become busy and decreases as channels become idle.
  int cost = (int)replica->resource.priority.mix + (int)replica->resource.cost;
  
  // min(resource.priority.mix) = 1
  // max(resource.priority.mix) = (0x40 << 8) + 127 = 16511 when defunct
  
  // min(resource.cost)         = min(resource.priority.base) = 1
  // max(resource.cost)         = max(resource.priority.base) * max(depth) (channels per replica) = 127 * 127 = 16129

  // min(cost)                  = min(resource.priority.mix) + min(resource.cost) = 1 + 1 = 2
  // max(cost)                  = max(resource.priority.mix) + max(resource.cost) = 16511 + 16129 = 32640

  // Automatically go to "infinite" cost if no available channels: cost + 16256
  return cost + REPLICA_EXHAUSTED( replica ) * REPLICA_MAX_COST;

  // min(return)                = min(cost) = 2
  // max(return)                = max(cost) + REPLICA_MAX_COST = 32640 + 16256 = 48896
}
