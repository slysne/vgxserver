/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_tracker.c
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

#include "_vgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static __THREAD int64_t THREAD_ID = -1;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct __s_tracker_context {
  QWORD graph_instance_id;
  framehash_dynamic_t *dynamic;
  framehash_cell_t **directory;
  framehash_cell_t *tracker;
  int64_t count;
} __tracker_context;




/*******************************************************************//**
 *
 ***********************************************************************
 */
static __THREAD __tracker_context gt_WL_context = {
  .graph_instance_id  = 0,
  .dynamic            = NULL,
  .directory          = NULL,
  .tracker            = NULL,
  .count              = 0
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
static __THREAD __tracker_context gt_RO_context = {
  .graph_instance_id  = 0,
  .dynamic            = NULL,
  .directory          = NULL,
  .tracker            = NULL,
  .count              = 0
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __count_locks_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell );
static framehash_cell_t * __load_tracker_CS( __tracker_context *context );
static int __set_tracker_context_WL_CS( vgx_Graph_t *self, __tracker_context *context_WL );
static int __set_tracker_context_RO_CS( vgx_Graph_t *self, __tracker_context *context_RO );
static int __track_vertex_LCK_for_thread_CS( vgx_Vertex_t *vertex_LCK, __tracker_context *context, bool inc );
static int64_t __yield_inarcs_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell );
static int64_t __reclaim_inarcs_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell );
static VertexAndInt64List_t * __get_vertex_acquisition_map_CS( __tracker_context *context, int64_t thread_id_filter );
static int64_t __count_vertex_acquisition_map_CS( __tracker_context *context, int64_t thread_id_filter );
static int64_t __close_vertices_CS( vgx_Graph_t *self, __tracker_context *context, int64_t thread_id_filter );
static int64_t __commit_vertices_CS( vgx_Graph_t *self, __tracker_context *context, int64_t thread_id_filter );




/*******************************************************************//**
 * CELL PROCESSOR
 * Count acquisitions
 ***********************************************************************
 */
static int64_t __count_locks_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  *(int64_t*)processor->processor.output += APTR_AS_INTEGER( cell );
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * __load_tracker_CS( __tracker_context *context ) {
  int64_t tracker_address = 0;

  if( THREAD_ID < 0 ) {
    THREAD_ID = GET_CURRENT_THREAD_ID();
  }

  // Retrieve existing tracker from graph's tracker directory
  if( iFramehash.simple.GetInt( *context->directory, context->dynamic, THREAD_ID, &tracker_address ) == 1 ) {
    context->tracker = (framehash_cell_t*)tracker_address;
  }
  // Create new tracker and enter into graph's tracker directory
  else if( (context->tracker = iFramehash.simple.New( context->dynamic )) != NULL ) {
    // Enter new tracker into graph's directory
    if( iFramehash.simple.SetInt( context->directory, context->dynamic, THREAD_ID, (int64_t)context->tracker ) != 1 ) {
      // Failed to enter tracker into directory, destroy the tracker
      iFramehash.simple.Destroy( &context->tracker, context->dynamic );
    }
  }
  // Failed to create new tracker
  else {
    context->tracker = NULL;
  }

  // Set the acquisition counter from tracker
  if( context->tracker ) {
    context->count = 0;
    iFramehash.simple.Process( context->tracker, __count_locks_CS, NULL, &context->count );
  }

  return context->tracker;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __set_tracker_context_WL_CS( vgx_Graph_t *self, __tracker_context *context_WL ) {
  if( context_WL->graph_instance_id != self->instance_id ) {
    context_WL->graph_instance_id = self->instance_id;
    context_WL->dynamic = &self->vtxmap_fhdyn;
    context_WL->directory = &self->vtxmap_WL;
    if( __load_tracker_CS( context_WL ) == NULL ) {
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __set_tracker_context_RO_CS( vgx_Graph_t *self, __tracker_context *context_RO ) {
  if( context_RO->graph_instance_id != self->instance_id ) {
    context_RO->graph_instance_id = self->instance_id;
    context_RO->dynamic = &self->vtxmap_fhdyn;
    context_RO->directory = &self->vtxmap_RO;
    if( __load_tracker_CS( context_RO ) == NULL ) {
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __track_vertex_LCK_for_thread_CS( vgx_Vertex_t *vertex_LCK, __tracker_context *context, bool inc ) {
  static const int DELTA = 1;
  static const bool AUTODELETE = true;

  // Save a local reference in case map changes address
  framehash_cell_t *pre_tracker = context->tracker;

  // Add/inc count for vertex lock
  if( inc ) {
    if( iFramehash.simple.IncInt( &context->tracker, context->dynamic, (QWORD)vertex_LCK, DELTA, NULL ) < 0 ) {
      FATAL( 0xD81, "Failed to track vertex acquisition: vertex_LCK=%llp thread=%lu", vertex_LCK, THREAD_ID );
    }
    context->count++;
  }
  // Decrement count for vertex lock
  else {
    int64_t value = 0;
    int ret;
    if( (ret = iFramehash.simple.DecInt( &context->tracker, context->dynamic, (QWORD)vertex_LCK, DELTA, &value, AUTODELETE, NULL )) != 0 ) {
      if( ret < 0 ) {
        FATAL( 0xD82, "Failed to untrack vertex acquisition: vertex_LCK=%llp thread=%lu", vertex_LCK, THREAD_ID );
      }
      // A previously untracked vertex is now being released in tracked mode.
      else if( value < 0 ) {
        int lck = vertex_LCK->descriptor.state.lock.lck;
        int rwl = vertex_LCK->descriptor.state.lock.rwl;
        unsigned writer = __vertex_get_writer_threadid( vertex_LCK );
        int64_t refcnt =  rwl ? Vertex_REFCNT_CS_RO( vertex_LCK ) : Vertex_REFCNT_WL( vertex_LCK );
        CRITICAL( 0xD82, "Tracked release (thread=%lu) of previously untracked vertex:\"%s\", lck=%d, rwl=%s, writer=%u, refcnt=%lld", THREAD_ID, CALLABLE(vertex_LCK)->IDString(vertex_LCK), lck, lck ? (rwl?"RO":"WL") : "-", writer, refcnt );
      }
    }

    context->count--;

  }

  // Update map address if its address changed
  if( context->tracker != pre_tracker ) {
    int64_t tracker_address = (intptr_t)context->tracker;
    if( iFramehash.simple.SetInt( context->directory, context->dynamic, THREAD_ID, tracker_address ) < 0 ) {
      FATAL( 0xD83, "Failed to track vertex acquisition: vertex_LCK=%llp thread=%lu", vertex_LCK, THREAD_ID );
    }
  }

  return 0;
}



/*******************************************************************//**
 * Returns: total writelocks for this thread (including recursive)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__register_vertex_WL_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  __assert_state_lock( self );

  // Ensure thread uses correct context
  if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
    return -1;
  }

  if( __track_vertex_LCK_for_thread_CS( vertex_WL, &gt_WL_context, true ) < 0 ) {
    return -1;
  }
  else {
    return gt_WL_context.count;
  }
}



/*******************************************************************//**
 * Returns: total writelocks for this thread (including recursive)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__register_vertex_WL_for_thread_OPEN( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  int64_t count;
  GRAPH_LOCK( self ) {
    count = _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_WL );
  } GRAPH_RELEASE;
  return count;
}



/*******************************************************************//**
 * Returns: total readlocks for this thread (including recursive)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__register_vertex_RO_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO ) {
  __assert_state_lock( self );

  // Ensure thread uses correct context
  if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
    return -1;
  }

  if( __track_vertex_LCK_for_thread_CS( vertex_RO, &gt_RO_context, true ) < 0 ) {
    return -1;
  }
  else {
    return gt_RO_context.count;
  }
}



/*******************************************************************//**
 * Returns: total writelocks for this thread (including recursive)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__unregister_vertex_WL_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  __assert_state_lock( self );

  // Ensure thread uses correct context
  if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
    return -1;
  }

  if( __track_vertex_LCK_for_thread_CS( vertex_WL, &gt_WL_context, false ) < 0 ) {
    return -1;
  }
  else {
    return gt_WL_context.count;
  }
}



/*******************************************************************//**
 * Returns: total readlocks for this thread (including recursive)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__unregister_vertex_RO_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO ) {
  __assert_state_lock( self );

  // Ensure thread uses correct context
  if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
    return -1;
  }

  if( __track_vertex_LCK_for_thread_CS( vertex_RO, &gt_RO_context, false ) < 0 ) {
    return -1;
  }
  else {
    return gt_RO_context.count;
  }
}



/*******************************************************************//**
 * CELL PROCESSOR
 * Yield all inarcs
 ***********************************************************************
 */
static int64_t __yield_inarcs_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  Key64Value56_t item = {
    .key = cell->annotation,          // vertex address
    .value56 = APTR_AS_INTEGER( cell )  // recursion count
  };
  // ASSUMPTION: This processor is run on a map of write locked vertices. We don't check whether WL
  // is held by current thread.

  // Current vertex processed
  vgx_Vertex_t *vertex_WL = (vgx_Vertex_t*)item.key;

  // This vertex (if given) will NOT have its inarcs yielded.
  vgx_Vertex_t *exempt = (vgx_Vertex_t*)processor->processor.input;
  if( vertex_WL == exempt ) {
    return 0;
  }

  // Yield inarcs
  __vertex_set_yield_inarcs( vertex_WL );
  return 1;
}



/*******************************************************************//**
 * CELL PROCESSOR
 * Re-acquire all inarcs
 ***********************************************************************
 */
static int64_t __reclaim_inarcs_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  vgx_Graph_t *graph = (vgx_Graph_t*)processor->processor.input;
  Key64Value56_t item = {
    .key = cell->annotation,          // vertex address
    .value56 = APTR_AS_INTEGER( cell )  // recursion count
  };
  // ASSUMPTION: This processor is run on a map of write locked vertices whose
  // inarcs yield flag has been cleared but may have inarcs still borrowed.

  vgx_ExecutionTimingBudget_t *timing_budget = (vgx_ExecutionTimingBudget_t*)processor->processor.output;

  // Current vertex processed
  vgx_Vertex_t *vertex_yiWL = (vgx_Vertex_t*)item.key;

  // Wait until the inarcs are not borrowed by anyone else, restoring WL of vertex
  return _vxgraph_state__reclaim_inarcs_CS_WL( graph, vertex_yiWL, timing_budget );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__has_readonly_locks_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
    return -1;
  }
  else {
    return gt_RO_context.count > 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__has_readonly_locks_OPEN( vgx_Graph_t *self ) {
  int has_RO;
  GRAPH_LOCK( self ) {
    has_RO = _vxgraph_tracker__has_readonly_locks_CS( self );
  } GRAPH_RELEASE;
  return has_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__num_readonly_locks_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
    return -1;
  }
  else {
    return gt_RO_context.count;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__num_readonly_locks_OPEN( vgx_Graph_t *self ) {
  int64_t num_RO;
  GRAPH_LOCK( self ) {
    num_RO = _vxgraph_tracker__num_readonly_locks_CS( self );
  } GRAPH_RELEASE;
  return num_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__has_writable_locks_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
    return -1;
  }
  else {
    return gt_WL_context.count > 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__has_writable_locks_OPEN( vgx_Graph_t *self ) {
  int has_WL;
  GRAPH_LOCK( self ) {
    has_WL = _vxgraph_tracker__has_writable_locks_CS( self );
  } GRAPH_RELEASE;
  return has_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__num_writable_locks_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
    return -1;
  }
  else {
    return gt_WL_context.count;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__num_writable_locks_OPEN( vgx_Graph_t *self ) {
  int64_t num_WL;
  GRAPH_LOCK( self ) {
    num_WL = _vxgraph_tracker__num_writable_locks_CS( self );
  } GRAPH_RELEASE;
  return num_WL;
}



/*******************************************************************//**
 * Return list of pointer to vertices held readonly by current thread.
 * List is terminated by a NULL-pointer.
 *
 * NOTE: Caller owns returned list!
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t ** _vxgraph_tracker__get_readonly_vertices_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  vgx_Vertex_t **vertex_list = NULL;

  if( _vxgraph_tracker__has_readonly_locks_CS( self ) > 0 ) {
    framehash_dynamic_t *dyn = &self->vtxmap_fhdyn;
    // Get list of vertex -> recursion
    VertexAndInt64List_t *vertex_recursion_list = iFramehash.simple.IntItems( gt_RO_context.tracker, dyn, NULL );
    if( vertex_recursion_list ) {
      int64_t n_vertices = CALLABLE( vertex_recursion_list )->Length( vertex_recursion_list );
      // Create output list and populate with vertices extracted from out thread-local map
      if( (vertex_list = calloc( n_vertices + 1, sizeof( vgx_Vertex_t* ) )) != NULL ) {
        for( int64_t vx=0; vx<n_vertices; vx++ ) {
          VertexAndInt64_t entry; 
          CALLABLE( vertex_recursion_list )->Get( vertex_recursion_list, vx, &entry.m128 );
          vertex_list[ vx ] = entry.vertex;
        }
      }
      COMLIB_OBJECT_DESTROY( vertex_recursion_list );
    }
  }

  return vertex_list;
}



/*******************************************************************//**
 * Return list of pointer to vertices held readonly by current thread.
 * List is terminated by a NULL-pointer.
 *
 * NOTE: Caller owns returned list!
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t ** _vxgraph_tracker__get_readonly_vertices_OPEN( vgx_Graph_t *self ) {
  vgx_Vertex_t **vertex_list;
  GRAPH_LOCK( self ) {
    vertex_list = _vxgraph_tracker__get_readonly_vertices_CS( self );
  } GRAPH_RELEASE;
  return vertex_list;
}



/*******************************************************************//**
 * Return list of pointer to vertices held writelocked by current thread.
 * List is terminated by a NULL-pointer.
 *
 * NOTE: Caller owns returned list!
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t ** _vxgraph_tracker__get_writable_vertices_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  vgx_Vertex_t **vertex_list = NULL;

  if( _vxgraph_tracker__has_writable_locks_CS( self ) > 0 ) {
    framehash_dynamic_t *dyn = &self->vtxmap_fhdyn;
    // Get list of vertex -> recursion
    VertexAndInt64List_t *vertex_recursion_list = iFramehash.simple.IntItems( gt_WL_context.tracker, dyn, NULL );
    if( vertex_recursion_list ) {
      int64_t n_vertices = CALLABLE( vertex_recursion_list )->Length( vertex_recursion_list );
      // Create output list and populate with vertices extracted from out thread-local map
      if( (vertex_list = calloc( n_vertices + 1, sizeof( vgx_Vertex_t* ) )) != NULL ) {
        for( int64_t vx=0; vx<n_vertices; vx++ ) {
          VertexAndInt64_t entry; 
          CALLABLE( vertex_recursion_list )->Get( vertex_recursion_list, vx, &entry.m128 );
          vertex_list[ vx ] = entry.vertex;
        }
      }
      COMLIB_OBJECT_DESTROY( vertex_recursion_list );
    }
  }

  return vertex_list;
}



/*******************************************************************//**
 * Return list of pointer to vertices held writelocked by current thread.
 * List is terminated by a NULL-pointer.
 *
 * NOTE: Caller owns returned list!
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t ** _vxgraph_tracker__get_writable_vertices_OPEN( vgx_Graph_t *self ) {
  vgx_Vertex_t **vertex_list;
  GRAPH_LOCK( self ) {
    vertex_list = _vxgraph_tracker__get_writable_vertices_CS( self );
  } GRAPH_RELEASE;
  return vertex_list;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_tracker__readonly_vertices_as_cstring_CS( vgx_Graph_t *self, int64_t *n ) {
  __assert_state_lock( self );
  CString_t *CSTR__readonly_vertices = NULL;
  
  // Check if current thread has readonly vertices
  if( _vxgraph_tracker__has_readonly_locks_CS( self ) > 0 ) {
    // Prepare to construct an elaborate error message
    CStringQueue_t *messageQ = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
    if( messageQ ) {
      // Get list of vertices held readonly by current thread
      vgx_Vertex_t **readonly = _vxgraph_tracker__get_readonly_vertices_CS( self );
      if( readonly ) {
        // Iterate over all vertices in the list and append the name of each vertex to the error message
        vgx_Vertex_t **cursor = readonly;
        vgx_Vertex_t *vertex = NULL;
        while( ( vertex = *cursor++ ) != NULL ) {
          // Put comma separator
          if( CALLABLE( messageQ )->Length( messageQ ) > 0 ) {
            CALLABLE( messageQ )->WriteNolock( messageQ, ", ", 2 );
          }
          // Format the vertex name before adding to message
          CString_t *CSTR__name = CStringNewFormat( "(%s)", CALLABLE( vertex )->IDString( vertex ) );
          if( CSTR__name ) {
            // Add formatted vertex name to message
            CALLABLE( messageQ )->WriteNolock( messageQ, CStringValue( CSTR__name ), CStringLength( CSTR__name ) );
            ++(*n);
            CStringDelete( CSTR__name );
          }
        }
        // Get the message a NUL-terminated string
        CALLABLE( messageQ )->NulTermNolock( messageQ );
        char *vertex_names = NULL;
        CALLABLE( messageQ )->GetValueNolock( messageQ, (void**)&vertex_names );
        // Create the message with the vertex name details we produced above
        if( vertex_names ) {
          CSTR__readonly_vertices = CStringNew( vertex_names );
          ALIGNED_FREE( vertex_names );
        }
        free( readonly );
      }
      COMLIB_OBJECT_DESTROY( messageQ );
    }
  }
  return CSTR__readonly_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_tracker__readonly_vertices_as_cstring_OPEN( vgx_Graph_t *self, int64_t *n ) {
  CString_t *CSTR__readonly_vertices;
  GRAPH_LOCK( self ) {
    CSTR__readonly_vertices = _vxgraph_tracker__readonly_vertices_as_cstring_CS( self, n );
  } GRAPH_RELEASE;
  return CSTR__readonly_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_tracker__writable_vertices_as_cstring_CS( vgx_Graph_t *self, int64_t *n ) {
  __assert_state_lock( self );
  CString_t *CSTR__writable_vertices = NULL;
  
  // Check if current thread has writable vertices
  if( _vxgraph_tracker__has_writable_locks_CS( self ) > 0 ) {
    // Prepare to construct an elaborate error message
    CStringQueue_t *messageQ = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
    if( messageQ ) {
      // Get list of vertices held writable by current thread
      vgx_Vertex_t **writable = _vxgraph_tracker__get_writable_vertices_CS( self );
      if( writable ) {
        // Iterate over all vertices in the list and append the name of each vertex to the error message
        vgx_Vertex_t **cursor = writable;
        vgx_Vertex_t *vertex = NULL;
        while( ( vertex = *cursor++ ) != NULL ) {
          // Put comma separator
          if( CALLABLE( messageQ )->Length( messageQ ) > 0 ) {
            CALLABLE( messageQ )->WriteNolock( messageQ, ", ", 2 );
          }
          // Format the vertex name before adding to message
          CString_t *CSTR__name = CStringNewFormat( "(%s)", CALLABLE( vertex )->IDString( vertex ) );
          if( CSTR__name ) {
            // Add formatted vertex name to message
            CALLABLE( messageQ )->WriteNolock( messageQ, CStringValue( CSTR__name ), CStringLength( CSTR__name ) );
            ++(*n);
            CStringDelete( CSTR__name );
          }
        }
        // Get the message a NUL-terminated string
        CALLABLE( messageQ )->NulTermNolock( messageQ );
        char *vertex_names = NULL;
        CALLABLE( messageQ )->GetValueNolock( messageQ, (void**)&vertex_names );
        // Create the message with the vertex name details we produced above
        if( vertex_names ) {
          CSTR__writable_vertices = CStringNew( vertex_names );
          ALIGNED_FREE( vertex_names );
        }
        free( writable );
      }
      COMLIB_OBJECT_DESTROY( messageQ );
    }
  }
  return CSTR__writable_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_tracker__writable_vertices_as_cstring_OPEN( vgx_Graph_t *self, int64_t *n ) {
  CString_t *CSTR__writable_vertices;
  GRAPH_LOCK( self ) {
    CSTR__writable_vertices = _vxgraph_tracker__writable_vertices_as_cstring_CS( self, n );
  } GRAPH_RELEASE;
  return CSTR__writable_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__enter_safe_multilock_CS( vgx_Graph_t *self, vgx_Vertex_t *exempt_if_WL, vgx_AccessReason_t *reason ) {
  __assert_state_lock( self );
  int ret = 0;
  vgx_AccessReason_t access_reason = VGX_ACCESS_REASON_NONE;

  XTRY {
    // Multilock not allowed if we hold any readonly locks already (or context error)
    if( _vxgraph_tracker__has_readonly_locks_CS( self ) != 0 ) {
      access_reason = VGX_ACCESS_REASON_RO_DISALLOWED;
      THROW_SILENT( CXLIB_ERR_API, 0xDA1 );
    }

    // Yield inarcs for all writelocked vertices held by current thread
    if( _vxgraph_tracker__has_writable_locks_CS( self ) > 0 ) {
      iFramehash.simple.Process( gt_WL_context.tracker, __yield_inarcs_WL_CS, exempt_if_WL, NULL );
      // Broadcast an availability signal
      SIGNAL_VERTEX_AVAILABLE( self );
    }
  }
  XCATCH( errcode ) {
    if( reason ) {
      *reason = access_reason;
    }
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
DLL_HIDDEN int _vxgraph_tracker__leave_safe_multilock_CS( vgx_Graph_t *self, vgx_AccessReason_t *reason ) {
  __assert_state_lock( self );
  int ret = 0;
  vgx_AccessReason_t access_reason = VGX_ACCESS_REASON_NONE;

  XTRY {

    if( _vxgraph_tracker__has_writable_locks_CS( self ) ) {

      // Reclaim all yielded inarcs for vertices write locked by current thread
      // Practically block, but not forever to avoid permanent deadlock in case of unexpected problems
      int attempts = 30;
      do {
        vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, 1000 );
        if( iFramehash.simple.Process( gt_WL_context.tracker, __reclaim_inarcs_WL_CS, self, &timing_budget ) >= 0 ) {
          break; // ok
        }
      } while( --attempts > 0 );
       
      // We can't reclaim all inarcs. That's bad.
      if( attempts == 0 ) {
        access_reason = VGX_ACCESS_REASON_ERROR;
        THROW_CRITICAL_MESSAGE( CXLIB_ERR_GENERAL, 0xDB1, "Partial inarc reclaim after leaving safe multilock" );
      }
    }

  }
  XCATCH( errcode ) {
    if( reason ) {
      *reason = access_reason;
    }
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
static VertexAndInt64List_t * __get_vertex_acquisition_map_CS( __tracker_context *context, int64_t thread_id_filter ) {
  VertexAndInt64List_t *vertex_thread_list = COMLIB_OBJECT_NEW_DEFAULT( Cm128iList_t );
  if( vertex_thread_list ) {
    // Get list of thread->vertex_map
    Key64Value56List_t *trackers = iFramehash.simple.IntItems( *context->directory, context->dynamic, NULL );
    if( trackers ) {
      int64_t n_threads = CALLABLE( trackers )->Length( trackers );
      for( int64_t tx=0; tx<n_threads; tx++ ) {
        Key64Value56_t item = {0};
        CALLABLE( trackers )->Get( trackers, tx, &item.m128 );
        int64_t threadid = item.key;
        // Optionally filter by threadid
        if( thread_id_filter == 0 || thread_id_filter == threadid ) {
          framehash_cell_t *tracker = (framehash_cell_t*)item.value56;
          if( tracker ) {
            // Get list of vertex->recursion
            VertexAndInt64List_t *vertex_list = iFramehash.simple.IntItems( tracker, context->dynamic, NULL );
            if( vertex_list ) {
              int64_t n_vertices = CALLABLE( vertex_list )->Length( vertex_list );
              for( int64_t vx=0; vx<n_vertices; vx++ ) {
                VertexAndInt64_t vertex_entry = {0};
                CALLABLE( vertex_list )->Get( vertex_list, vx, &vertex_entry.m128 );
                VertexAndInt64_t output_entry = {
                  .vertex   = vertex_entry.vertex,
                  .value    = threadid
                };
                CALLABLE( vertex_thread_list )->Append( vertex_thread_list, &output_entry.m128 );
              }
              COMLIB_OBJECT_DESTROY( vertex_list );
            }
          }
        }
      }
      COMLIB_OBJECT_DESTROY( trackers );
    }
  }
  return vertex_thread_list;
}



/*******************************************************************//**
 * Return all open vertices, optionally filtered by thread if threadid
 * is a positive integer. Vertices RO/WL are listed separately into the
 * respective output lists. If a list pointer is NULL it will not be
 * populated.
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__get_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter, VertexAndInt64List_t **readonly, VertexAndInt64List_t **writable ) {
  __assert_state_lock( self );
  int ret = 0;

  XTRY {
    if( readonly ) {
      if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xDC1 );
      }
      if( (*readonly = __get_vertex_acquisition_map_CS( &gt_RO_context, thread_id_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xDC2 );
      }
    }

    if( writable ) {
      if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xDC3 );
      }
      if( (*writable = __get_vertex_acquisition_map_CS( &gt_WL_context, thread_id_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xDC4 );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
    if( readonly && *readonly ) {
      COMLIB_OBJECT_DESTROY( *readonly );
      *readonly = NULL;
    }
    if( writable && *writable ) {
      COMLIB_OBJECT_DESTROY( *writable );
      *writable = NULL;
    }
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 * Return all open vertices, optionally filtered by thread if threadid
 * is a positive integer. Vertices RO/WL are listed separately into the
 * respective output lists. If a list pointer is NULL it will not be
 * populated.
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__get_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter, VertexAndInt64List_t **readonly, VertexAndInt64List_t **writable ) {
  int ret = 0;

  GRAPH_LOCK( self ) {
    ret = _vxgraph_tracker__get_open_vertices_CS( self, thread_id_filter, readonly, writable );
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __count_vertex_acquisition_map_CS( __tracker_context *context, int64_t thread_id_filter ) {
  int64_t count = 0;
  // Get list of thread->vertex_map
  Key64Value56List_t *trackers = iFramehash.simple.IntItems( *context->directory, context->dynamic, NULL );
  if( trackers ) {
    int64_t n_threads = CALLABLE( trackers )->Length( trackers );
    for( int64_t tx=0; tx<n_threads; tx++ ) {
      Key64Value56_t item = {0};
      CALLABLE( trackers )->Get( trackers, tx, &item.m128 );
      int64_t threadid = item.key;
      // Optionally filter by threadid
      if( thread_id_filter == 0 || thread_id_filter == threadid ) {
        framehash_cell_t *tracker = (framehash_cell_t*)item.value56;
        if( tracker ) {
          count += iFramehash.simple.Length( tracker );
        }
      }
    }
    COMLIB_OBJECT_DESTROY( trackers );
  }
  return count;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__num_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter, int64_t *readonly, int64_t *writable ) {
  __assert_state_lock( self );
  int ret = 0;

  XTRY {
    if( readonly ) {
      if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }
      if( (*readonly = __count_vertex_acquisition_map_CS( &gt_RO_context, thread_id_filter )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

    if( writable ) {
      if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
      if( (*writable = __count_vertex_acquisition_map_CS( &gt_WL_context, thread_id_filter )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tracker__num_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter, int64_t *readonly, int64_t *writable ) {
  int ret = 0;

  GRAPH_LOCK( self ) {
    ret = _vxgraph_tracker__num_open_vertices_CS( self, thread_id_filter, readonly, writable );
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __close_vertices_CS( vgx_Graph_t *self, __tracker_context *context, int64_t thread_id_filter ) {
  //
  // NOTE: thread_id_filter must equal current thread ID.
  // TODO: Remove this parameter and get current thread ID automatically.
  //
  //
  int64_t n_unlocks = 0;
  VertexAndInt64List_t *vertices = NULL;
  vgx_VertexList_t *closable = NULL;
  XTRY {
    int64_t n = 0;
    int64_t n_unlocks_pre;
    do { 
      n_unlocks_pre = n_unlocks;

      // Get vertices from map
      if( (vertices = __get_vertex_acquisition_map_CS( context, thread_id_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xDD1 );
      }
      
      // Any open vertices?
      if( (n = CALLABLE( vertices )->Length( vertices )) > 0 ) {
        // Allocate closable vertex list
        if( (closable = iVertex.List.New( n )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0xDD2 );
        }

        // Populate closable vertex list with vertices owned by current thread
        int64_t n_owned = 0;
        for( int64_t i=0; i<n; i++ ) {
          VertexAndInt64_t entry = {0};
          CALLABLE( vertices )->Get( vertices, i, &entry.m128 );
          if( entry.vertex != NULL && entry.value == thread_id_filter ) {
            iVertex.List.Set( closable, n_owned++, entry.vertex );
          }
        }
        n = iVertex.List.Truncate( closable, n_owned );

        // Capture
        iOperation.Unlock_CS.ReleaseLCK( self, closable );

        // Unlock one level of recursion for each owned vertex
        for( int64_t i=0; i<n; i++ ) {
          vgx_Vertex_t *vertex = iVertex.List.Get( closable, i );
          if( _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, VGX_VERTEX_RECORD_ALL ) == true ) {
            ++n_unlocks;
          }
          else {
            if( __vertex_is_writable( vertex ) ) {
              uint32_t tid = __vertex_get_writer_threadid( vertex );
              CRITICAL( 0xDD3, "Failed to unlock writable vertex '%s' owned by thread %u", vertex->identifier.idprefix.data, tid );
            }
            else {
              CRITICAL( 0xDD3, "Failed to unlock readonly vertex '%s'", vertex->identifier.idprefix.data);
            }
            // Force untrack (this could lead to problems)
            __track_vertex_LCK_for_thread_CS( vertex, context, false );
          }
        }

        // Clean up
        iVertex.List.Delete( &closable );
      }

      // Clean up
      COMLIB_OBJECT_DESTROY( vertices );
      vertices = NULL;

      // Repeat unlock until all recursive locks released
      // Proceed only when open vertices still exist and progress is being made
    } while( n > 0 && n_unlocks > n_unlocks_pre );
  }
  XCATCH( errcode ) {
    n_unlocks = -1;
  }
  XFINALLY {
    if( closable ) {
      iVertex.List.Delete( &closable );
    }
    if( vertices ) {
      COMLIB_OBJECT_DESTROY( vertices );
    }
  }
  return n_unlocks;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_readonly_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter ) {
  __assert_state_lock( self );
  int64_t n_ro = 0;

  XTRY {
    // Close all readonly
    if( __set_tracker_context_RO_CS( self, &gt_RO_context ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDE1 );
    }
    if( (n_ro = __close_vertices_CS( self, &gt_RO_context, thread_id_filter )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDE2 );
    }
  }
  XCATCH( errcode ) {
    n_ro = -1;
  }
  XFINALLY {
  }

  return n_ro;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_readonly_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxgraph_tracker__close_readonly_vertices_CS( self, thread_id_filter );
  } GRAPH_RELEASE;

  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_writable_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter ) {
  __assert_state_lock( self );
  int64_t n_wl = 0;

  XTRY {
    // Close all writable
    if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDE3 );
    }
    if( (n_wl = __close_vertices_CS( self, &gt_WL_context, thread_id_filter )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDE4 );
    }
  }
  XCATCH( errcode ) {
    n_wl = -1;
  }
  XFINALLY {
  }

  return n_wl;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_writable_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxgraph_tracker__close_writable_vertices_CS( self, thread_id_filter );
  } GRAPH_RELEASE;

  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter ) {
  __assert_state_lock( self );
  int64_t n = 0;

  XTRY {
    int64_t n_ro, n_wl;

    if( (n_ro = _vxgraph_tracker__close_readonly_vertices_CS( self, thread_id_filter )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( (n_wl = _vxgraph_tracker__close_writable_vertices_CS( self, thread_id_filter )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Total vertices closed
    n = n_ro + n_wl;
  }
  XCATCH( errcode ) {
    n = -1;
  }
  XFINALLY {
  }

  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__close_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxgraph_tracker__close_open_vertices_CS( self, thread_id_filter );
  } GRAPH_RELEASE;

  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __commit_vertices_CS( vgx_Graph_t *self, __tracker_context *context, int64_t thread_id_filter ) {
  int64_t n = 0;
  VertexAndInt64List_t *vertices = NULL;
  XTRY {

    // Get vertices from map
    if( (vertices = __get_vertex_acquisition_map_CS( context, thread_id_filter )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDF1 );
    }
    
    // Any open vertices?
    int64_t n_open;
    if( (n_open = CALLABLE( vertices )->Length( vertices )) > 0 ) {
      for( int64_t i=0; i<n_open; i++ ) {
        VertexAndInt64_t entry = {0};
        CALLABLE( vertices )->Get( vertices, i, &entry.m128 );
        vgx_Vertex_t *V = entry.vertex;
        if( V != NULL ) {
          if( thread_id_filter == 0
              ||
              (entry.value == thread_id_filter && __vertex_is_locked_writable_by_thread( V, (uint32_t)thread_id_filter ))
              )
          {
            ++n;
            _vxdurable_commit__commit_vertex_CS_WL( self, V, true );
          }
        }
      }
    }

    // Clean up
    COMLIB_OBJECT_DESTROY( vertices );
    vertices = NULL;

  }
  XCATCH( errcode ) {
    n = -1;
  }
  XFINALLY {
    if( vertices ) {
      COMLIB_OBJECT_DESTROY( vertices );
    }
  }
  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__commit_writable_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter ) {
  __assert_state_lock( self );
  int64_t n = 0;

  XTRY {
    if( __set_tracker_context_WL_CS( self, &gt_WL_context ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDF8 );
    }
    if( (n = __commit_vertices_CS( self, &gt_WL_context, thread_id_filter )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xDF9 );
    }
  }
  XCATCH( errcode ) {
    n = -1;
  }
  XFINALLY {
  }

  return n;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_tracker__commit_writable_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxgraph_tracker__commit_writable_vertices_CS( self, thread_id_filter );
  } GRAPH_RELEASE;

  return n;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_tracker.h"

test_descriptor_t _vgx_vxgraph_tracker_tests[] = {
  { "VGX Graph Tracker Tests", __utest_vxgraph_tracker },
  {NULL}
};
#endif
