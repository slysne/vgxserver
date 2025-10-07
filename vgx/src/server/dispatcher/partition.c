/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    partition.c
 * Author:  Stian Lysne slysne.dev@gmail.com
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
DLL_HIDDEN void vgx_server_dispatcher_partition__dump( const vgx_VGXServerDispatcherPartition_t *partition, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherPartition_t, partition ) {
    CXLIB_OSTREAM( "id" );
    CXLIB_OSTREAM( "  partition    = %d", partition->id.partition );
    CXLIB_OSTREAM( "flag" );
    CXLIB_OSTREAM( "  partial      = %d", (int)partition->flag.partial );
    CXLIB_OSTREAM( "  rsv1222      = %d", (int)partition->flag.__rsv_1_2_2_2 );
    CXLIB_OSTREAM( "  rsv1223      = %d", (int)partition->flag.__rsv_1_2_2_3 );
    CXLIB_OSTREAM( "  rsv1224      = %d", (int)partition->flag.__rsv_1_2_2_4 );
    CXLIB_OSTREAM( "replica" );
    CXLIB_OSTREAM( "  height       = %d", (int)partition->replica.height );
    CXLIB_OSTREAM( "  __rsv_1_2_2  = %d", (int)partition->replica.__rsv_1_2_2 );
    for( int k=0; k<partition->replica.height; k++ ) {
      vgx_server_dispatcher_replica__dump( &partition->replica.list[k], NULL );
    }
    CXLIB_OSTREAM( "__rsv_1_4      = %llu", partition->__rsv_1_4 );

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
DLL_HIDDEN int vgx_server_dispatcher_partition__init( vgx_VGXServer_t *server, BYTE partition_id, vgx_VGXServerDispatcherPartition_t *partition, const vgx_VGXServerDispatcherConfigPartition_t *cf_partition ) {
  int ret = 0;
  XTRY {
    if( partition_id >= SERVER_MATRIX_WIDTH_MAX ) {
      THROW_ERROR( CXLIB_ERR_CONFIG, 0x000 );
    }

    // [Q1.1.1]
    // Partition ID
    partition->id.partition = partition_id;

    // [Q1.1.2.1]
    partition->flag.partial = cf_partition->partial;

    partition->flag.__rsv_1_2_2_2 = false;
    partition->flag.__rsv_1_2_2_3 = false;
    partition->flag.__rsv_1_2_2_4 = false;

    // [Q1.2.1]
    // Number of replicas in partition
    partition->replica.height = cf_partition->height;

    // [Q1.2.2]
    partition->replica.__rsv_1_2_2 = 0;

    // [Q1.3]
    // Allocate list of replicas
    if( (partition->replica.list = calloc( cf_partition->height, sizeof( vgx_VGXServerDispatcherReplica_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Initialize all replicas in partition
    BYTE replica_id = 0;
    vgx_VGXServerDispatcherReplica_t *replica = partition->replica.list;
    vgx_VGXServerDispatcherReplica_t *end = replica + cf_partition->height;
    vgx_VGXServerDispatcherConfigReplica_t *cf_replica = cf_partition->replicas;
    while( replica < end ) {
      if( vgx_server_dispatcher_replica__init( server, replica_id++, replica++, cf_replica++, partition ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

    // [Q1.4]
    partition->__rsv_1_4 = 0;

  }
  XCATCH( errcode ) {
    vgx_server_dispatcher_partition__clear( partition );
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
DLL_HIDDEN void vgx_server_dispatcher_partition__clear( vgx_VGXServerDispatcherPartition_t *partition ) {
  // [Q1.4]
  partition->__rsv_1_4 = 0;

  // [Q1.3]
  // Delete replica list
  if( partition->replica.list ) {
    // Clear each replica in partition
    vgx_VGXServerDispatcherReplica_t *replica = partition->replica.list;
    vgx_VGXServerDispatcherReplica_t *end = replica + partition->replica.height;
    while( replica < end ) {
      vgx_server_dispatcher_replica__clear( replica++ );
    }
    free( partition->replica.list );
    partition->replica.list = NULL;
  }

  // [Q1.2.2]
  partition->replica.__rsv_1_2_2 = 0;

  // [Q1.2.1]
  partition->replica.height = 0;

  // [Q1.1.2]
  partition->flag._bits = 0;

  // [Q.1.1]
  partition->id.partition = SERVER_MATRIX_WIDTH_NONE;
}
