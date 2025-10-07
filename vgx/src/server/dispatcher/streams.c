/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    streams.c
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
static void __streams__dump_set( const vgx_VGXServerDispatcherStreamSet_t *stream_set, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherStreamSet_t, stream_set ) {
    CXLIB_OSTREAM( "======== request ========" );
    vgx_server_request__dump( stream_set->prequest, NULL );
    CXLIB_OSTREAM( "responses.len     : %d", stream_set->responses.len );
    CXLIB_OSTREAM( "responses.headers : @ %llp", stream_set->responses.headers );
    for( int i=0; i<stream_set->responses.len; i++ ) {
      CXLIB_OSTREAM( "======== header %d ========", i );
      vgx_server_dispatcher_partial__dump_header( &stream_set->responses.headers[i], NULL );
    }
    CXLIB_OSTREAM( "responses.list    : @ %llp", stream_set->responses.list );
    for( int i=0; i<stream_set->responses.len; i++ ) {
      CXLIB_OSTREAM( "======== response %d ========", i );
      vgx_server_response__dump( stream_set->responses.list[i], NULL );
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
DLL_HIDDEN void vgx_server_dispatcher_streams__dump_sets( const vgx_VGXServerDispatcherStreamSets_t *sets, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherStreamSets_t, sets ) {
    CXLIB_OSTREAM( "sz        = %d", sets->sz );
    CXLIB_OSTREAM( "__rsv     = %d", sets->__rsv );
    CXLIB_OSTREAM( "list      = @ %llp", sets->list );
    for( int n=0; n<sets->sz; n++ ) {
      __streams__dump_set( &sets->list[n], NULL );
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
DLL_HIDDEN vgx_VGXServerDispatcherStreamSets_t * vgx_server_dispatcher_streams__new_sets( vgx_VGXServer_t *server, vgx_VGXServerDispatcherConfig_t *cf ) {
  vgx_VGXServerDispatcherStreamSets_t *sets = NULL;


  XTRY {
    // Allocate sets container
    if( (sets = calloc( 1, sizeof( vgx_VGXServerDispatcherStreamSets_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // The worst-case number of sets needed is the total number of clients.
    // Accounts for clients:
    //   * busy with dispatched channels
    //   * in executor threads
    //   * in completion queue
    sets->sz = server->pool.clients.capacity;

    // Allocate array of sets (plus 1 null-set)
    if( (sets->list = calloc( sets->sz + 1LL, sizeof( vgx_VGXServerDispatcherStreamSet_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Populate sets
    char label[512] = {0};
    for( int i=0; i<sets->sz; i++ ) {
      vgx_VGXServerDispatcherStreamSet_t *set = &sets->list[i];
      // Initialize set's request struct
#ifdef _DEBUG
      snprintf( label, 511, "dispatcher.stream.set.%d.request", i );
#endif
      vgx_server_request__init( &set->_request, label );
      set->prequest = NULL;
      // A response set's length is equal to the number of partitions
      set->responses.len = cf->shape.width;
      // Allocate space for header data for each response instance
      if( (set->responses.headers = calloc( set->responses.len, sizeof(x_vgx_partial__header) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      // Allocate set list (plus NULL terminator)
      if( (set->responses.list = calloc( set->responses.len + 1LL, sizeof( vgx_VGXServerResponse_t* ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
      // Create response instances in set
      for( int k=0; k<set->responses.len; k++ ) {
#ifdef _DEBUG
        snprintf( label, 511, "dispatcher.stream.set.%d.response.%d", i, k );
#endif
        if( (set->responses.list[k] = vgx_server_response__new( label )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
        }
      }
      // Terminate
      set->responses.list[ set->responses.len ] = NULL;
    }
    // Terminate
    vgx_server_request__init( &sets->list[ sets->sz ]._request, "dispatcher.stream.set.terminator.request" );
    sets->list[ sets->sz ].prequest = NULL;
    sets->list[ sets->sz ].responses.len = 0;
    sets->list[ sets->sz ].responses.list = NULL;
  }
  XCATCH( errcode ) {
    vgx_server_dispatcher_streams__delete_sets( &sets );
  }
  XFINALLY {
    
  }

  return sets;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_streams__delete_sets( vgx_VGXServerDispatcherStreamSets_t **sets ) {
  if( sets && *sets ) {
    if( (*sets)->list ) {
      vgx_VGXServerDispatcherStreamSet_t *set = (*sets)->list;
      // Delete all sets until terminator
      while( set->responses.list ) {

        // Destroy the request struct
        vgx_server_request__destroy( &set->_request );

        SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY
        free( set->responses.headers );

        // Delete all response instances in set
        for( int k=0; k<set->responses.len; k++ ) {
          vgx_server_response__delete( &set->responses.list[k] );
        }

        // Delete set's array of pointers to instances
        SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY
        free( set->responses.list );

        // Next set
        ++set;
      }

      // Delete array of sets
      free( (*sets)->list );
    }

    // Delete sets container
    free( *sets );
    *sets = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_streams__trim_set( vgx_VGXServerDispatcherStreamSet_t *stream_set ) {
  if( stream_set ) {
    iStreamBuffer.Trim( stream_set->_request.buffers.stream, CHANNEL_REQUEST_STREAM_BUFFER_MAX_IDLE_CAPACITY );
    iStreamBuffer.Trim( stream_set->_request.buffers.content, CHANNEL_REQUEST_CONTENT_BUFFER_MAX_IDLE_CAPACITY );
    vgx_VGXServerResponse_t **response = stream_set->responses.list;
    vgx_VGXServerResponse_t **end = response + stream_set->responses.len;
    while( response < end ) {
      iStreamBuffer.Trim( (*response)->buffers.stream, CHANNEL_RESPONSE_STREAM_BUFFER_MAX_IDLE_CAPACITY );
      iStreamBuffer.Trim( (*response)->buffers.content, CHANNEL_RESPONSE_CONTENT_BUFFER_MAX_IDLE_CAPACITY );
      ++response;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_streams__set_request( vgx_VGXServerDispatcherStreamSet_t *set, vgx_VGXServerRequest_t *request ) {
  // Override the set request object pointer
  set->prequest = request;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerRequest_t * vgx_server_dispatcher_streams__get_request( vgx_VGXServerDispatcherStreamSet_t *set ) {
  // Get the request (DO NOT RESET)
  return set->prequest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN x_vgx_partial__header * vgx_server_dispatcher_streams__get_header( vgx_VGXServerDispatcherStreamSet_t *set, int i ) {
  // Get the i'th header in set
  return &set->responses.headers[i];
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerResponse_t * vgx_server_dispatcher_streams__get_reset_response( vgx_VGXServerDispatcherStreamSet_t *set, int i ) {
  // Get the i'th response in set
  vgx_VGXServerResponse_t *response = set->responses.list[i];
  // Make sure buffer is reset
  vgx_server_response__reset( response );
  // Clear the partial header
  vgx_server_dispatcher_partial__reset_header( &set->responses.headers[i] );

  return response;
}
