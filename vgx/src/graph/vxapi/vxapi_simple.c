/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxapi_simple.c
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
 *
 ***********************************************************************
 */


static int64_t __vertex_degree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_arc_direction direction, int timeout_ms, vgx_AccessReason_t *reason );
static int __vertex_aggregator( vgx_Graph_t *self, const vgx_Vertex_t *vertex_RO, vgx_aggregator_search_context_t *search );
static int64_t __global_query( vgx_Graph_t *self, vgx_GlobalQuery_t *query );


static int Graph_create_vertex_simple( vgx_Graph_t *self, const char *name, const char *type );
static int Graph_create_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_open_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_VertexAccessMode_t mode, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_new_vertex( vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_create_vertex_lifespan( vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static bool Graph_close_vertex( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );
static int Graph_delete_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static bool Graph_has_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name );
static int Graph_connect( vgx_Graph_t *self, const vgx_Relation_t *relation, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );

static int64_t Graph_disconnect( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query );

static int64_t Graph_degree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_arc_direction direction, int timeout_ms, vgx_AccessReason_t *reason );
static int64_t Graph_vertex_degree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );
static int64_t Graph_vertex_indegree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );
static int64_t Graph_vertex_outdegree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );

static vgx_vertex_type_t Graph_vertex_set_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_type, int timeout_ms, vgx_AccessReason_t *reason );
static const CString_t * Graph_vertex_get_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason );

static vgx_Relation_t * Graph_has_adjacency( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query );
static vgx_Vertex_t * Graph_open_neighbor( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query, bool readonly );
static vgx_ArcHead_t Graph_arc_value( vgx_Graph_t *self, const vgx_Relation_t *relation, int timeout_ms, vgx_AccessReason_t *reason );
static int64_t Graph_neighborhood( vgx_Graph_t *self, vgx_NeighborhoodQuery_t *query );
static int64_t Graph_aggregate( vgx_Graph_t *self, vgx_AggregatorQuery_t *query );
static int64_t Graph_arcs( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
static int64_t Graph_vertices( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
static int64_t Graph_order_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_type );

static int Graph_relationships( vgx_Graph_t *self, CtptrList_t **CSTR__strings );
static int Graph_vertex_types( vgx_Graph_t *self, CtptrList_t **CSTR__strings );
static int Graph_property_keys( vgx_Graph_t *self, CtptrList_t **CSTR__strings );
static int Graph_property_string_values( vgx_Graph_t *self, CtptrList_t **CSTR__string_qwo_list );

static int64_t Graph_truncate( vgx_Graph_t *self, CString_t **CSTR__error );
static int64_t Graph_truncate_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_type, CString_t **CSTR__error );

static vgx_Evaluator_t * Graph_define_evaluator( vgx_Graph_t *self, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error );
static vgx_Evaluator_t * Graph_get_evaluator( vgx_Graph_t *self, const char *name );
static vgx_Evaluator_t ** Graph_get_evaluators( vgx_Graph_t *self, int64_t *sz );




static vgx_IGraphSimple_t SimpleMethods = {
  .CreateVertexSimple   = Graph_create_vertex_simple,
  .CreateVertex         = Graph_create_vertex,
  .OpenVertex           = Graph_open_vertex,
  .NewVertex            = Graph_new_vertex,
  .CreateVertexLifespan = Graph_create_vertex_lifespan,
  .CloseVertex          = Graph_close_vertex,
  .DeleteVertex         = Graph_delete_vertex,
  .HasVertex            = Graph_has_vertex,
  .Connect              = Graph_connect,
  .Disconnect           = Graph_disconnect,
  .Degree               = Graph_degree,
  .VertexDegree         = Graph_vertex_degree,      // DEPRECATED
  .VertexInDegree       = Graph_vertex_indegree,    // DEPRECATED
  .VertexOutDegree      = Graph_vertex_outdegree,   // DEPRECATED
  .VertexSetType        = Graph_vertex_set_type,
  .VertexGetType        = Graph_vertex_get_type,
  .HasAdjacency         = Graph_has_adjacency,
  .OpenNeighbor         = Graph_open_neighbor,
  .ArcValue             = Graph_arc_value,
  .Neighborhood         = Graph_neighborhood,
  .Aggregate            = Graph_aggregate,
  .Arcs                 = Graph_arcs,
  .Vertices             = Graph_vertices,
  .OrderType            = Graph_order_type,
  .Relationships        = Graph_relationships,
  .VertexTypes          = Graph_vertex_types,
  .PropertyKeys         = Graph_property_keys,
  .PropertyStringValues = Graph_property_string_values,
  .Truncate             = Graph_truncate,
  .TruncateType         = Graph_truncate_type,
  .DefineEvaluator      = Graph_define_evaluator,
  .GetEvaluator         = Graph_get_evaluator,
  .GetEvaluators        = Graph_get_evaluators
};



DLL_HIDDEN vgx_IGraphSimple_t * _vxapi_simple = &SimpleMethods;







/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __capture_query_search_time( vgx_BaseQuery_t *query, int64_t t0_ns ) {
  int64_t t1_ns = __GET_CURRENT_NANOSECOND_TICK();
  query->exe_time.t_search = (t1_ns - t0_ns) / 1e9;
  return t1_ns;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __capture_query_result_time( vgx_BaseQuery_t *query, int64_t t0_ns ) {
  int64_t t1_ns = __GET_CURRENT_NANOSECOND_TICK();
  query->exe_time.t_result = (t1_ns - t0_ns) / 1e9;
  return t1_ns;
}



#define BEGIN_QUERY( Query, PtrExeTimeNanoSec )             \
  do {                                                      \
    vgx_BaseQuery_t *__query = (vgx_BaseQuery_t*)(Query);   \
    bool __local_pri_req = iSystem.BeginQuery( true );      \
    int64_t __t0_ns = __GET_CURRENT_NANOSECOND_TICK();      \
    int64_t __t1_ns = __t0_ns;                              \
    int64_t __t2_ns = __t0_ns;                              \
    int64_t *__pt_exe_ns = PtrExeTimeNanoSec;               \
    do


#define END_QUERY_SEARCH_PHASE  (__t2_ns = __t1_ns = __capture_query_search_time( __query, __t0_ns ))

#define END_QUERY_RESULT_PHASE  (__t2_ns = __capture_query_result_time( __query, __t1_ns ))

#define INVALIDATE_QUERY_TIMING (__t2_ns = __t1_ns = __t0_ns - 1)

#define END_QUERY                                           \
    WHILE_ZERO;                                             \
    *__pt_exe_ns = __t2_ns - __t0_ns;                       \
    iSystem.EndQuery( __local_pri_req, __t2_ns - __t0_ns ); \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 * 1  : hit
 * 0  : miss
 * -1 : error
 ***********************************************************************
 */
static vgx_ArcFilter_match __vertex_has_adjacency( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_adjacency_search_context_t *search, vgx_Arc_t *first_match ) {

  const objectid_t *vertex_obid = CSTR__vertex_name ? CStringObid( CSTR__vertex_name ) : NULL;
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;

  vgx_arc_direction direction = search->probe->conditional.arcdir;

  vgx_Vertex_t * vertex_RO = NULL;
  XTRY {
    _vgx_start_graph_execution_timing_budget( self, search->timing_budget );
    GRAPH_LOCK( self ) {
      vertex_RO = _vxgraph_state__acquire_readonly_vertex_CS( self, vertex_obid, search->timing_budget );
    } GRAPH_RELEASE;

    if( vertex_RO == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0xA01 );
    }

    // Bi-directional ANY
    if( direction == VGX_ARCDIR_ANY ) {
      // Override probe direction and check inarcs and outarcs individually
      search->probe->conditional.arcdir = VGX_ARCDIR_IN;
      vgx_ArcFilter_match match_in = iGraphInspect.HasAdjacency( vertex_RO, search, first_match );
      search->probe->conditional.arcdir = VGX_ARCDIR_OUT;
      vgx_ArcFilter_match match_out = iGraphInspect.HasAdjacency( vertex_RO, search, first_match );

      // Any direction
      if( direction == VGX_ARCDIR_ANY ) {
        if( match_in == VGX_ARC_FILTER_MATCH_HIT || match_out == VGX_ARC_FILTER_MATCH_HIT ) {
          match = VGX_ARC_FILTER_MATCH_HIT;
        }
      }
      // Both directions
      else {
        if( match_in == VGX_ARC_FILTER_MATCH_HIT && match_out == VGX_ARC_FILTER_MATCH_HIT ) {
          match = VGX_ARC_FILTER_MATCH_HIT;
        }
      }

      // Restore original probe direction
      search->probe->conditional.arcdir = direction;

      // Check for errors
      if( __is_arcfilter_error( match_in ) || __is_arcfilter_error( match_out ) ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA02 );
      }
    }
    // IN, OUT or BOTH
    else {
      // match, no match, or error
      match = iGraphInspect.HasAdjacency( vertex_RO, search, first_match ); // caller should check for error

      // Check for errors
      if( __is_arcfilter_error( match ) ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA03 );
      }
    }

  }
  XCATCH( errcode ) {
    if( search->CSTR__error == NULL ) {
      __set_error_string_from_reason( &search->CSTR__error, CSTR__vertex_name, search->timing_budget->reason );
    }
    match = __arcfilter_error();
  }
  XFINALLY {
    if( vertex_RO ) {
      _vxgraph_state__release_vertex_OPEN_LCK( self, &vertex_RO );
    }
  }
  
  return match;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __vertex_aggregator( vgx_Graph_t *self, const vgx_Vertex_t *vertex_RO, vgx_aggregator_search_context_t *search ) {
  int ret = 0;

  XTRY {
    vgx_arc_direction arcdir = search->probe->traversing.arcdir;

    if( arcdir == VGX_ARCDIR_IN || arcdir == VGX_ARCDIR_OUT ) {
      if( iGraphTraverse.AggregateNeighborhood( vertex_RO, search ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA11 );
      }
    }
    else if( arcdir == VGX_ARCDIR_BOTH ) {
      __set_error_string( &search->CSTR__error, "Bi-directional arc aggregation is not possible" );
      THROW_SILENT( CXLIB_ERR_API, 0xA12 );
    }
    else {
      __set_error_string( &search->CSTR__error, "Invalid arc direction" );
      THROW_SILENT( CXLIB_ERR_API, 0xA13 );
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
 *
 ***********************************************************************
 */
static int64_t __global_query( vgx_Graph_t *self, vgx_GlobalQuery_t *query ) {

  int64_t n_hits = 0;

  // Are we creating a new result list or are we appending to a list owned by caller?
  bool external_owner = query->collector != NULL ? true : false;

  vgx_global_search_context_t *search = NULL;

  int ro_frozen = 0;

  int64_t qt_ns = 0;
  BEGIN_QUERY( query, &qt_ns ) {

    XTRY {
      int ret;

      // ------------
      // Search phase
      // ------------
      bool readonly_graph = false;

      // Freeze readonly if needed
      if( (ro_frozen = CALLABLE( self )->advanced->FreezeGraphReadonly_OPEN( self, &query->CSTR__error )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA80 );
      }
      else if( ro_frozen > 0 ) {
        readonly_graph = true;
      }

      // Start timing budget
      _vgx_start_graph_execution_timing_budget( self, &query->timing_budget );

      // Validate global hit counts
      if( iGraphTraverse.ValidateGlobalCollectableCounts( self, query ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0xA81 );
      }

      // Create internal search context from external query
      if( (search = iGraphProbe.NewGlobalSearch( self, readonly_graph, query )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA82 );
      }
      if( external_owner ) {
        search->result = query->collector;
      }

      // Execute global search
      ret = iGraphTraverse.TraverseGlobalItems( self, search, ro_frozen );

      END_QUERY_SEARCH_PHASE;

      // ------------
      // Result phase
      // ------------

      // Error
      if( ret < 0 ) {
        if( search && query ) {
          CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA83 );
      }

      // Consume collector into new list
      if( iGraphCollector.ConvertToBaseListCollector( search->collector.base ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA84 );
      }

      search->n_items = search->collector.base->n_collectable;

      // Discard items at end of list (if we didn't collect as many as requested) and at beginning of list (if offset > 0)
      if( iGraphCollector.TrimBaseListCollector( search->collector.base, search->n_items, search->offset, search->hits ) == NULL ) {
        iGraphCollector.DeleteCollector( &search->collector.base );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA85 );
      }
      
      // Transfer the search results to the output: n_hits now counts the entire result, including any previous items from a previous query if they exist
      n_hits = iGraphCollector.TransferBaseList( search->ranking_context, &search->collector.base, &search->result );

      // Set brand new result list as query result since caller did not supply a list
      if( external_owner == false ) {
        query->collector = search->result;
      }

      // Forget the search result - query has the official reference
      search->result = NULL;

      // Patch in the evaluator memory as needed
      if( query->selector ) {
        if( query->evaluator_memory == NULL ) {
          if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0xA86 );
          }
        }
        CALLABLE( query->selector )->OwnMemory( query->selector, query->evaluator_memory );
      }

      // Render search result into a new, self contained object
      //
      //
      vgx_ResponseFields_t response_fields = { 
        .fastmask       = query->fieldmask,
        .selecteval     = query->selector,
        .include_mod_tm = true
      };

      query->n_items = search->n_items;
      if( iGraphResponse.BuildSearchResult( self, &response_fields, NULL, (vgx_BaseQuery_t*)query ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA87 );
      }

      END_QUERY_RESULT_PHASE;

    }
    XCATCH( errcode ) {
      iGraphResponse.DeleteSearchResult( &query->search_result );
      INVALIDATE_QUERY_TIMING;
      n_hits = -1;
    }
    XFINALLY {
      // Don't report counts if they're not accurate
      if( !(search && search->counts_are_deep) ) {
        query->n_items = -1;
      }
      iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
      // Unfreeze readonly
      if( ro_frozen > 0 ) {
        CALLABLE( self )->advanced->UnfreezeGraphReadonly_OPEN( self );
      }
    }
  } END_QUERY;

  GRAPH_LOCK( self ) {
    query->parent_opid = iOperation.GetId_LCK( &self->operation );
    CALLABLE( self )->CountQueryNolock( self, qt_ns );
  } GRAPH_RELEASE;

  return n_hits;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_create_vertex_simple( vgx_Graph_t *self, const char *name, const char *type ) {
  int ret = 0;
  CString_t *CSTR__vertex_name = NULL;
  CString_t *CSTR__vertex_type = NULL;

  XTRY {
    if( (CSTR__vertex_name = NewEphemeralCString( self, name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( type ) {
      if( (CSTR__vertex_type = NewEphemeralCString( self, type )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }
    }

    ret = Graph_create_vertex( self, CSTR__vertex_name, CSTR__vertex_type, NULL, NULL );

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__vertex_name );
    iString.Discard( &CSTR__vertex_type );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_create_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int ret;
  const objectid_t *vertex_obid = CStringObid( CSTR__vertex_name );
  GRAPH_LOCK( self ) {
    ret = _vxgraph_state__create_vertex_CS( self, CSTR__vertex_name, vertex_obid, CSTR__vertex_typename, NULL, 0, reason, CSTR__error );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 * Open the named vertex with the requested access rights. 
 * 
 * NONE mode
 * ---------
 *    The vertex pointer is returned without performing any acquisition
 *    actions. The vertex refcount is incremented so the caller becomes
 *    another owner of the vertex and is responsible for decrementing
 *    the refcount later. The caller should not rely on the integrity of
 *    the vertex object since no lock has been acquired.
 *
 * WRITABLE mode
 * -------------
 *    If the vertex does not already exist it is first created with default
 *    attributes. Once WRITABLE access to the vertex has been acquired by
 *    the current thread a pointer to the vertex object is returned. 
 *    The returned vertex will be in the OPEN, WRITABLE, INCOMPLETE state.
 *
 *    Caller owns an exclusive lock on the returned vertex. No other thread
 *    can open the vertex (in any mode) while the current thread has WRITABLE
 *    access. Caller is responsible for surrendering WRITABLE access when
 *    such access is no longer needed, and is required for any other thread
 *    to be able to open the vertex. Caller can surrender WRITABLE access
 *    either by closing the vertex or by de-escalating the access mode from
 *    WRITABLE to READONLY.
 *
 * READONLY mode
 * -------------
 *    If the vertex exists, an attempt to become a READONLY subscriber of the
 *    vertex is made and when successful a pointer to the vertex object is 
 *    returned. The returned vertex will be in the OPEN, READONLY, COMPLETE state.
 * 
 *    Caller owns a shared READONLY lock on the returned vertex. No other
 *    thread can open the vertex in WRITABLE mode while at least one thread
 *    holds the READONLY lock. There is a limited number of READONLY locks
 *    available. If the maximum number of READONLY subscribers has been reached
 *    another thread attempting to obtain the READONLY lock will be blocked
 *    until one of the READONLY subscribers closes the vertex.
 * 
 * 
 * READONLY subscription limit remarks
 * -----------------------------------
 *    If another thread attempts to open the vertex in WRITABLE mode while at
 *    least one thread holds a READONLY subscription, the multi-subscriber
 *    READONLY policy is temporarily suspended. This means no further READONLY
 *    subscriptions can be obtained, even if the current number of READONLY
 *    subscribers is below the normal subscription limit. All existing READONLY
 *    subscriptions will still be valid. All READONLY subscribers must release
 *    the vertex before the pending WRITABLE request can be fulfilled. Normal
 *    multi-subscriber READONLY policy will be restored once the WRITABLE 
 *    request has been cleared, which happens when no thread owns a WRITABLE
 *    lock or when a timeout for a blocked WRITABLE request occurs.
 *
 * Blocking or timeout remarks
 * ---------------------------
 *    When the vertex cannot immediately be opened in the requested access mode,
 *    this function may block until an optional timeout occurs. The timeout is
 *    given in milliseconds and the parameter has the following meaning:
 *      timeout_ms = 0      Return immediately, even if vertex cannot be opened
 *      timeout_ms > 0      Block for no longer than this amount of time
 *      timeout_ms < 0      Block indefinitely until vertex can be opened
 *                           
 *
 * Remarks on returned vertex
 * --------------------------
 *    When non-NULL is returned, the vertex is guaranteed to be available for
 *    use by the caller in the requested mode, indefinitely. The caller is
 *    responsible for using the vertex in the appropriate mode only. If a 
 *    thread has obtained a READONLY (or NONE mode) vertex it is illegal
 *    for that thread to call non-readonly methods on the vertex. If the caller
 *    inadvertently performs a vertex operation not in accordance with the
 *    acquisition mode the behavior is undefined. The vertex API shall not be
 *    relied upon to enforce legal method calls. Note that the NONE mode
 *    is similar to READONLY mode, except that the integrity of the vertex
 *    is not guaranteed, i.e. attributes could change at any time. For this
 *    reason the NONE mode should only be used for internal access when
 *    the caller knows exactly what is going on in other threads, or when
 *    accessing vertex attributes that are primitives.
 *
 *
 *
 * Return value:
 *    Pointer to vertex object accessible in the requested mode,
 *    or NULL on failure to open the vertex within the specified timeout.
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_open_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_VertexAccessMode_t mode, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex = NULL;
  const objectid_t *vertex_obid = CStringObid( CSTR__vertex_name );
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  GRAPH_LOCK( self ) {
    switch( mode ) {
    case VGX_VERTEX_ACCESS_WRITABLE:
      vertex = _vxgraph_state__acquire_writable_vertex_CS( self, CSTR__vertex_name, vertex_obid, VERTEX_STATE_CONTEXT_MAN_REAL, &timing_budget, CSTR__error );
      __set_access_reason( reason, timing_budget.reason );
      break;
    case VGX_VERTEX_ACCESS_WRITABLE_NOCREATE:
      vertex = _vxgraph_state__acquire_writable_vertex_CS( self, NULL, vertex_obid, VERTEX_STATE_CONTEXT_MAN_NULL, &timing_budget, CSTR__error );
      __set_access_reason( reason, timing_budget.reason );
      break;
    case VGX_VERTEX_ACCESS_READONLY:
      vertex = _vxgraph_state__acquire_readonly_vertex_CS( self, vertex_obid, &timing_budget );
      __set_access_reason( reason, timing_budget.reason );
      break;
    default:
      __set_access_reason( reason, VGX_ACCESS_REASON_INVALID );
    }
  } GRAPH_RELEASE;
  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_new_vertex( vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex = NULL;
  vgx_IGraphAdvanced_t *igraphadv = CALLABLE( self )->advanced;
  CString_t *CSTR__vertex_name = NULL;
  CString_t *CSTR__vertex_type = NULL;

  if( (CSTR__vertex_name = NewEphemeralCString( self, name )) == NULL ) {
    return NULL;
  }

  if( type && (CSTR__vertex_type = NewEphemeralCString( self, type )) == NULL ) {
    iString.Discard( &CSTR__vertex_name );
    return NULL;
  }

  const objectid_t *obid = CStringObid( CSTR__vertex_name );
  GRAPH_LOCK( self ) {
    if( (vertex = igraphadv->NewVertex_CS( self, obid, CSTR__vertex_name, CSTR__vertex_type, timeout_ms, reason, CSTR__error )) != NULL ) {
      // Set Lifespan
      if( lifespan > -1 ) {
        uint32_t tmx = _vgx_graph_seconds( self );
        if( lifespan == 0 ) {
          tmx -= 1; // trick vertex into thinking it's already expired
        }
        else {
          tmx += lifespan;
        }
        if( CALLABLE( vertex )->SetExpirationTime( vertex, tmx ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          __format_error_string( CSTR__error, "failed to set expiration time: %u", tmx );
          igraphadv->ReleaseVertex_CS( self, &vertex );
        }
      }
    }
  } GRAPH_RELEASE;

  iString.Discard( &CSTR__vertex_name );
  iString.Discard( &CSTR__vertex_type );

  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_create_vertex_lifespan( vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *V = Graph_new_vertex( self, name, type, lifespan, timeout_ms, reason, CSTR__error );
  if( V == NULL ) {
    return -1;
  }
  Graph_close_vertex( self, &V );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_close_vertex( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK ) {
  return _vxgraph_state__release_vertex_OPEN_LCK( self, vertex_LCK );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_delete_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_execution_timing_budget( 0, timeout_ms );
  return _vxdurable_commit__delete_vertex_OPEN( self, CSTR__vertex_name, NULL, &timing_budget, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_has_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name ) {
  return _vxgraph_vxtable__exists_OPEN( self, CSTR__vertex_name, NULL, VERTEX_TYPE_ENUMERATION_WILDCARD );
}



/*******************************************************************//**
 *
 *
 * Returns: >= 1: Created connection
 *             0: Connection already existed
 *            -1: Error
 *
 ***********************************************************************
 */
static int Graph_connect( vgx_Graph_t *self, const vgx_Relation_t *relation, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {

  // Ensure call can proceed
  if( _vxgraph_tracker__has_readonly_locks_OPEN( self ) ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_RO_DISALLOWED );
    return -1;
  }

  int ret = -1;

  const objectid_t *initial_obid = CStringObid( relation->initial.CSTR__name );
  const objectid_t *terminal_obid = CStringObid( relation->terminal.CSTR__name );

  vgx_Arc_t arc_WL = {0};
  bool rel_enc = false;

  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_execution_timing_budget( 0, timeout_ms );

  // Special case: loop A->A (vertex connects to itself)
  if( idmatch( initial_obid, terminal_obid ) ) {
    const CString_t *CSTR__vertex_idstr = relation->initial.CSTR__name;
    const objectid_t *vertex_obid = initial_obid;
    vgx_Vertex_t *vertex_WL = _vxgraph_state__acquire_writable_vertex_OPEN( self, CSTR__vertex_idstr, vertex_obid, VERTEX_STATE_CONTEXT_MAN_REAL, &timing_budget, CSTR__error );
    __set_access_reason( reason, timing_budget.reason );
    if( vertex_WL ) {
      if( iRelation.SetStoredArc_OPEN( self, &arc_WL, vertex_WL, vertex_WL, relation ) ) {
        // Create arc
        if( __relationship_enumeration_valid( arc_WL.head.predicator.rel.enc ) ) {
          rel_enc = true;
          ret = _vxgraph_state__create_arc_WL( self, &arc_WL, lifespan, arc_condition_set, reason, CSTR__error );
        }
        else if( arc_WL.head.predicator.rel.enc == VGX_PREDICATOR_REL_INVALID ) {
          __format_error_string( CSTR__error, "invalid relationship: '%s'", CStringValue(relation->relationship.CSTR__name) );
        }

        // Release
        if( _vxgraph_state__release_vertex_OPEN_LCK( self, &vertex_WL ) == false ) {
          // Operation failed and we are possibly in an inconsistent state
          // TODO: investigate inconsistency scenarios and fix.
          ret = -1;
        }
      }
    }
  }
  // Normal case: A->B
  else {
    vgx_Vertex_t *initial_WL;
    vgx_Vertex_t *terminal_WL;

    //
    // TODO: Why are we acquiring terminal WL and not iWL? Perhaps we should make a new function to acquire initial WL and terminal iWL ?
    //
    int n_acquired =_vxgraph_state__acquire_writable_initial_and_terminal_OPEN( self, &initial_WL, relation->initial.CSTR__name, initial_obid, &terminal_WL, relation->terminal.CSTR__name, terminal_obid, VERTEX_STATE_CONTEXT_MAN_VIRTUAL, &timing_budget, CSTR__error );
    __set_access_reason( reason, timing_budget.reason );
    if( n_acquired == 2 && initial_WL && terminal_WL ) {
      // Both vertices are now WL
      if( iRelation.SetStoredArc_OPEN( self, &arc_WL, initial_WL, terminal_WL, relation ) ) {
        // Create arc
        if( __relationship_enumeration_valid( arc_WL.head.predicator.rel.enc ) ) {
          rel_enc = true;
          ret = _vxgraph_state__create_arc_WL( self, &arc_WL, lifespan, arc_condition_set, reason, CSTR__error );

          /*
          AVOID THIS ISSUE:
          PROVIDER                                          SUBSCRIBER
          --------------                                    -------------------
          new B                         "create B"          new B
          new A (WL)                    "create A"          new A
          arc A->B        (no transaction yet)              
          del B           (B expires)   "delete B"          del B
          commit A                      "connect A to B"    "B no longer exists"
          */
          if( ret >= 0 && iSystem.IsAttached() ) {
            GRAPH_LOCK( self ) {
              _vxdurable_commit__commit_vertex_CS_WL( self, initial_WL, false );
            } GRAPH_RELEASE;

            // Implicitly created terminal. Capture virtualization operation.
            if( __vertex_is_manifestation_virtual( terminal_WL ) ) {
              iOperation.Vertex_WL.Convert( terminal_WL, VERTEX_STATE_CONTEXT_MAN_VIRTUAL );
            }
          }
        }
        else if( arc_WL.head.predicator.rel.enc == VGX_PREDICATOR_REL_INVALID ) {
          __format_error_string( CSTR__error, "invalid relationship: '%s'", CStringValue(relation->relationship.CSTR__name) );
        }
        else if( arc_WL.head.predicator.rel.enc == VGX_PREDICATOR_REL_NO_MAPPING ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ENUM_NOTYPESPACE );
        }

        // Release
        if( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( self, &initial_WL, &terminal_WL ) == false ) {
          // Operation failed and we are possibly in an inconsistent state
          // TODO: investigate inconsistency scenarios and fix.
          CRITICAL( 0xA21, "Failed to release writable initial and terminal atomically" );
          ret = -1;
        }
      }
    }
    else if( !__has_access_reason(reason) ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
    }
  }

  if( !rel_enc && !__has_access_reason( reason ) ) {
    if( arc_WL.head.predicator.rel.enc == VGX_PREDICATOR_REL_NO_MAPPING ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_ENUM_NOTYPESPACE );
    }
    else {
      REASON( 0xA22, "No relationship encoding?" );
      __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
    }
  } 

  return ret;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int __is_valid_disconnect_search( vgx_adjacency_search_context_t *search, vgx_AccessReason_t *reason ) {
  // -----------------------------
  // TODO:
  // Support head vertex conditions as well. Involves passing a filter down to the arcvector
  // module and re-doing the disconnect callbacks, wildcard removal, and all the nasty stuff
  // that we don't want to break at the moment.
  //
  vgx_vertex_probe_t *VP = search->probe->conditional.vertex_probe;
  if( VP && !_vgx_vertex_condition_full_wildcard( VP->spec, VP->manifestation ) ) {
    // DISALLOW OTHER VERTEX CONDITIONS UNTIL WE FIX THE ABOVE TODO
    // The vertex condition can only be an exact vertex ID. Any other conditions are invalid for now.
    if( VP->spec != (_VERTEX_PROBE_ID_ENA | _VERTEX_PROBE_ID_EQU) ) {
      __set_error_string( &search->CSTR__error, "Vertex conditions other than exact ID match not allowed" );
      *reason = VGX_ACCESS_REASON_NONE;
      return 0; // search not valid
    }
  }
  //
  //
  // ----------------------------
  return 1; // search ok
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static vgx_predicator_t __predicator_from_adjacency_search_context( const vgx_adjacency_search_context_t *search ) {
  if( search && search->probe && search->probe->conditional.arcfilter ) {
    return ((vgx_GenericArcFilter_context_t*)search->probe->conditional.arcfilter)->pred_condition1;
  }
  else {
    return VGX_PREDICATOR_ANY;
  }
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int64_t __disconnect_directional( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {

  int64_t n_disconnected = -1;

  const CString_t *CSTR__anchor_id = query->CSTR__anchor_id;
  vgx_adjacency_search_context_t *search = NULL;

  if( query->arc_condition_set->arcdir == VGX_ARCDIR_ANY || query->arc_condition_set->arcdir == VGX_ARCDIR_BOTH ) {
    return -1;
  }

  vgx_ExecutionTimingBudget_t *timing_budget = &query->timing_budget;


  XTRY {

    // Compute internal ids (NULL results in id=0)
    objectid_t anchor_obid = *CStringObid( CSTR__anchor_id );
    objectid_t head_obid = {0};
    if( query->vertex_condition && query->vertex_condition->CSTR__idlist ) {
      vgx_StringList_t *list = query->vertex_condition->CSTR__idlist;
      if( _vgx_vertex_condition_has_id_prefix( query->vertex_condition->spec ) || iString.List.Size( list ) != 1 ) {
        __set_error_string( CSTR__error, "Vertex conditions other than exact ID match not allowed" );
        THROW_SILENT( CXLIB_ERR_API, 0xA31 );
      }
      head_obid = *CStringObid( iString.List.GetItem( list, 0 ) );
    }

    // Special case: loop A->A (vertex connects to itself)
    if( idmatch( &anchor_obid, &head_obid ) ) {
      // Acquire anchor
      vgx_Vertex_t *anchor_WL = _vxgraph_state__acquire_writable_vertex_OPEN( self, CSTR__anchor_id, &anchor_obid, VERTEX_STATE_CONTEXT_MAN_NULL, timing_budget, CSTR__error );
      __set_access_reason( reason, timing_budget->reason );
      if( anchor_WL ) {
        // Create and validate adjacency probe
        if( (search = iGraphProbe.NewAdjacencySearch( self, false, anchor_WL, query )) != NULL && __is_valid_disconnect_search( search, reason ) ) {
          //
          // Set the self-disconnect condition
          vgx_Arc_t loop_WL = {
            .tail = anchor_WL,
            .head = {
              .vertex     = anchor_WL,
              .predicator = __predicator_from_adjacency_search_context( search )
            }
          };
          // Disconnect
          if( (n_disconnected = _vxgraph_state__remove_arc_WL( self, &loop_WL )) < 0 ) {
            __set_error_string( &search->CSTR__error, "Arc removal failed for self-loop" );
          }
        }

        // Release anchor
        if( _vxgraph_state__release_vertex_OPEN_LCK( self, &anchor_WL ) == false ) {
          // Operation failed and we are possibly in an inconsistent state
          // TODO: investigate inconsistency scenarios and fix.
          __set_error_string( &query->CSTR__error, "Failed to release anchor after self-loop arc removal (commit error?)" );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA32 );
        }

        // Check for errors
        if( n_disconnected < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0xA33 );
        }
      }
      else {
        __set_error_string_from_reason( &query->CSTR__error, CSTR__anchor_id, *reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA34 );
      }
    }
    // remove A->B
    else if( !idnone( &head_obid ) ) {
      vgx_Vertex_t *anchor_WL;
      vgx_Vertex_t *head_WL;
      const CString_t *CSTR__id = iString.List.GetItem( query->vertex_condition->CSTR__idlist, 0 );
      //
      // TODO: Why are we acquiring terminal WL and not iWL? Perhaps we should make a new function to acquire initial WL and terminal iWL ?
      //
      int acode = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( self, &anchor_WL, CSTR__anchor_id, &anchor_obid, &head_WL, CSTR__id, &head_obid, VERTEX_STATE_CONTEXT_MAN_NULL, timing_budget, CSTR__error );
      __set_access_reason( reason, timing_budget->reason );
      if( acode == 2 ) {
        if( anchor_WL && head_WL ) {
          // Create and validate adjacency probe around anchor
          if( (search = iGraphProbe.NewAdjacencySearch( self, false, anchor_WL, query )) != NULL && __is_valid_disconnect_search( search, reason ) ) {
            // Set the disconnect condition
            vgx_Arc_t arc_WL = {
              .tail = anchor_WL,
              .head = {
                .vertex     = head_WL,
                .predicator = __predicator_from_adjacency_search_context( search )
              }
            };
            // Disconnect
            //
            // NOTE: We are not yielding inarcs of thread's other writable vertices
            //       before disconnected. Since hold both ends of arc writelocked there
            //       should not be any contention during delete and it is guaranteed
            //       to complete.
            if( (n_disconnected = _vxgraph_state__remove_arc_WL( self, &arc_WL )) < 0 ) {
              __set_error_string( &search->CSTR__error, "Arc removal failed" );
            }
          }

          // Release anchor
          if( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( self, &anchor_WL, &head_WL ) == false ) {
            // Operation failed and we are possibly in an inconsistent state
            // TODO: investigate inconsistency scenarios and fix.
            __set_error_string( &query->CSTR__error, "Failed to release anchor after arc removal (commit error?)" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0xA35 );
          }

          // Check for errors
          if( n_disconnected < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0xA36 );
          }
        }
        else {
          FATAL( 0xA37, "Acquired pair NULL pointer(s)" ); // TODO: fix all these fatal things
        }
      }
      else {
        if( CSTR__error == NULL || *CSTR__error == NULL ) {
          const CString_t *CSTR__locked = CStringCheck( timing_budget->resource );
          __set_error_string_from_reason( &query->CSTR__error, CSTR__locked, *reason );
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA38 );
      }
    }
    // Remove many outarcs or inarcs
    else {
      GRAPH_LOCK( self ) {
        BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
          bool wait = true;
          BEGIN_EXECUTION_BLOCKED_WHILE( wait ) {
            n_disconnected = 0; // not an error while we try to acquire
            vgx_Vertex_t *anchor = _vxgraph_vxtable__query_CS( self, NULL, &anchor_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
            if( anchor ) {
              // OUT
              vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_graph_execution_timing_budget( self, 10 );
              if( query->arc_condition_set->arcdir == VGX_ARCDIR_OUT ) {
                vgx_Vertex_t *anchor_WL = _vxgraph_state__lock_vertex_writable_CS( self, anchor, &short_timeout, VGX_VERTEX_RECORD_OPERATION );
                __set_access_reason( reason, short_timeout.reason );
                if( anchor_WL ) {
                  //
                  // Yield inarcs of all writelocked vertices owned by current thread
                  // except for the anchor whose outarcs we are about to delete.
                  //
                  ENTER_SAFE_MULTILOCK_CS( self, anchor_WL, reason ) {
                    GRAPH_SUSPEND_LOCK( self ) {
                      // Create and validate adjacency probe around anchor
                      n_disconnected = -1; // error if we can't complete the search and disconnect
                      if( (search = iGraphProbe.NewAdjacencySearch( self, false, anchor_WL, query )) != NULL && __is_valid_disconnect_search( search, reason ) ) {
                        // Disconnect
                        //
                        // TODO:  fix_arcdelnoblock  This needs re-work. May return -1 below if removal timed out, which means
                        // removal was partial or none at all. Check timing budget/reason to determine cause
                        // and next action.
                        //
                        if( (n_disconnected = _vxgraph_state__remove_outarcs_OPEN_WL( self, anchor_WL, timing_budget, __predicator_from_adjacency_search_context( search ) )) < 0 ) {
                          __format_error_string( &search->CSTR__error, "Outarcs removal is incomplete (%lld remaining), try again.", iarcvector.Degree( &anchor_WL->outarcs ) );
                          __set_access_reason( reason, timing_budget->reason );
                        }
                      }
                      // Release anchor
                      wait = false;
                    } GRAPH_RESUME_LOCK;
                  } LEAVE_SAFE_MULTILOCK_CS( n_disconnected = -1 );
                  if( _vxgraph_state__unlock_vertex_CS_LCK( self, &anchor_WL, VGX_VERTEX_RECORD_OPERATION ) == false ) {
                    // we have a problem. can't commit the vertex. die instead of deadlock.
                    FATAL( 0xA39, "Failed to commit vertex after removing outarcs." ); // TODO: fix all these fatal things
                  }
                }
                else {
                  SET_EXECUTION_RESOURCE_BLOCKED( CSTR__anchor_id ); // For error reporting
                }
              }
              // IN
              else {
                vgx_Vertex_t *anchor_iWL = _vxgraph_state__lock_terminal_inarcs_writable_CS( self, anchor, &short_timeout );
                __set_access_reason( reason, short_timeout.reason );
                if( anchor_iWL ) {
                  //
                  // Yield inarcs of all writelocked vertices owned by current thread
                  // except for the anchor whose inarcs we are about to delete.
                  //
                  ENTER_SAFE_MULTILOCK_CS( self, anchor_iWL, reason ) {
                    GRAPH_SUSPEND_LOCK( self ) {
                      // Create and validate adjacency probe around anchor
                      n_disconnected = -1; // error if we can't complete the search and disconnect
                      if( (search = iGraphProbe.NewAdjacencySearch( self, false, anchor_iWL, query )) != NULL && __is_valid_disconnect_search( search, reason ) ) {
                        // Disconnect
                        //
                        // TODO:  fix_arcdelnoblock  This needs re-work. May return -1 below if removal timed out, which means
                        // removal was partial or none at all. Check timing budget/reason to determine cause
                        // and next action.
                        //
                        if( (n_disconnected = _vxgraph_state__remove_inarcs_OPEN_iWL( self, anchor_iWL, timing_budget, __predicator_from_adjacency_search_context( search ) )) < 0 ) {
                          __format_error_string( &search->CSTR__error, "Inarcs removal is incomplete (%lld remaining), try again.", iarcvector.Degree( &anchor_iWL->inarcs ) );
                          __set_access_reason( reason, timing_budget->reason );
                        }
                      }
                      // Release anchor
                      wait = false;
                    } GRAPH_RESUME_LOCK;
                  } LEAVE_SAFE_MULTILOCK_CS( n_disconnected = -1 );
                  if( _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( self, &anchor_iWL ) == false ) {
                    // we have a problem. can't commit the vertex. die instead of deadlock.
                    FATAL( 0xA3A, "Failed to commit vertex after removing outarcs." ); // TODO: fix all these fatal things
                  }
                }
                else {
                  SET_EXECUTION_RESOURCE_BLOCKED( CSTR__anchor_id );
                }
              }
            }
            else {
              *reason = VGX_ACCESS_REASON_NOEXIST;
              __set_error_string_from_reason( &query->CSTR__error, CSTR__anchor_id, *reason );
              n_disconnected = -1; // flag the error
            }
            // Check if error occurred
            if( n_disconnected < 0 ) {
              wait = false;
              FORCE_GRAPH_RELEASE;
              THROW_SILENT( CXLIB_ERR_GENERAL, 0xA3B );
            }
          } END_EXECUTION_BLOCKED_WHILE;
        } END_EXECUTE_WITH_TIMING_BUDGET_CS;
      } GRAPH_RELEASE;

      // Check errors
      if( *reason != VGX_ACCESS_REASON_OBJECT_ACQUIRED ) {
        __set_error_string_from_reason( &query->CSTR__error, CSTR__anchor_id, *reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA3C );
      }

    }
  }
  XCATCH( errcode ) {
    if( search && query ) {
      CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
    }
    n_disconnected = -1;
  }
  XFINALLY {
    iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
  }


  return n_disconnected;
}



/*******************************************************************//**
 *
 * Returns: > 0 : Number of disconnected arcs
 *          = 0 : No arcs disconnected (vertex may not exist, check query.error)
 *          < 0 : Disconnection error (see query.error)
 * 
 ***********************************************************************
 */
static int64_t Graph_disconnect( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query ) {

  // Ensure call can proceed
  if( _vxgraph_tracker__has_readonly_locks_OPEN( self ) ) {
    __set_access_reason( &query->access_reason, VGX_ACCESS_REASON_RO_DISALLOWED );
    return -1;
  }

  int64_t n_disconnected = -1; // error by default

  XTRY {
    // No arc condition in query, create one here because we need to set direction
    if( query->arc_condition_set == NULL ) {
      if( (query->arc_condition_set = iArcConditionSet.NewEmpty( self, true, VGX_ARCDIR_ANY )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA41 );
      }
    }

    // Handle bidirectional disconnect
    // Call ourselves twice, first disconnect IN then disconnect OUT
    // NOTE: We currently treat AND and BOTH the same for disconnect: Remove inarcs and outarcs.
    if( query->arc_condition_set->arcdir == VGX_ARCDIR_ANY || query->arc_condition_set->arcdir == VGX_ARCDIR_BOTH ) {
      // First disconnect inarcs
      query->arc_condition_set->arcdir = VGX_ARCDIR_IN;
      if( (n_disconnected = __disconnect_directional( self, query, &query->access_reason, &query->CSTR__error )) >= 0 ) {
        query->access_reason = VGX_ACCESS_REASON_NONE; // reset for next query
        if( !query->CSTR__error ) {
          int64_t n_out;
          // Then disconnect outarcs
          query->arc_condition_set->arcdir = VGX_ARCDIR_OUT;
          n_out = __disconnect_directional( self, query, &query->access_reason, &query->CSTR__error );
          if( n_out >= 0 ) {
            n_disconnected += n_out; // SUCCESS
          }
          // If vertex was virtual it will have been deleted by the first disconnect, it's ok
          if( n_out >=0 || query->access_reason == VGX_ACCESS_REASON_NOEXIST ) {
            if( query->CSTR__error ) { // Clear any transient errors we don't need to report up (like vertex no longer existing)
              iString.Discard( &query->CSTR__error );
            }
          }
          else {
            // TODO: deal with partial disconnect (roll back?)
            __set_error_string( &query->CSTR__error, "Partial bi-directional disconnect (outarcs failed)" );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0xA42 );
          }
        }
      }
      else {
        __set_error_string( &query->CSTR__error, "Bi-directional disconnect failed" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA43 );
      }
    }
    // OUT or IN
    else {
      if( (n_disconnected = __disconnect_directional( self, query, &query->access_reason, &query->CSTR__error )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA44 );
      }
    }
  }
  XCATCH( errcode ) {

    n_disconnected = -1;
  }
  XFINALLY {
  }

  return n_disconnected;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_degree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_arc_direction direction, int timeout_ms, vgx_AccessReason_t *reason ) {
  int64_t degree = -1;
  const objectid_t *vertex_obid = CStringObid( CSTR__vertex_name );

  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  vgx_Vertex_t *vertex_RO;
  GRAPH_LOCK( self ) {
    vertex_RO = _vxgraph_state__acquire_readonly_vertex_CS( self, vertex_obid, &timing_budget );
  } GRAPH_RELEASE;
  __set_access_reason( reason, timing_budget.reason );

  if( vertex_RO ) {
    switch( direction ) {
    case VGX_ARCDIR_IN:
      degree = CALLABLE(vertex_RO)->InDegree(vertex_RO);
      break;
    case VGX_ARCDIR_OUT:
      degree = CALLABLE(vertex_RO)->OutDegree(vertex_RO);
      break;
    case VGX_ARCDIR_ANY:
      /* FALLTHRU */
    case VGX_ARCDIR_BOTH:
      degree = CALLABLE(vertex_RO)->Degree(vertex_RO);
      break;
    default:
      if( reason ) {
        *reason = VGX_ACCESS_REASON_INVALID;
      }
    }
    _vxgraph_state__release_vertex_OPEN_LCK( self, &vertex_RO );
  }
  return degree;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_vertex_type_t Graph_vertex_set_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_type, int timeout_ms, vgx_AccessReason_t *reason ) {
  // Default type
  vgx_vertex_type_t vxtype = VERTEX_TYPE_ENUMERATION_NONE;
  // Compute obid before locking graph
  const objectid_t *pobid = CStringObid( CSTR__vertex_name );
  GRAPH_LOCK( self ) {
    // Retrieve vertex by name
    vgx_Vertex_t *vertex = _vxgraph_vxtable__query_CS( self, NULL, pobid, VERTEX_TYPE_ENUMERATION_WILDCARD );
    if( vertex ) {
      // Map type string to code
      CString_t *CSTR__mapped_instance = NULL;
      vxtype = iEnumerator_CS.VertexType.Encode( self, CSTR__vertex_type, &CSTR__mapped_instance, true );
      if( vxtype < __VERTEX_TYPE_ENUMERATION_START_EXC_RANGE ) {
        // Lock vertex writable
        vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
        vgx_Vertex_t *vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_OPERATION );
        __set_access_reason( reason, timing_budget.reason );
        if( vertex_WL ) {
          // Type is different - update
          if( __vertex_get_type( vertex_WL ) != vxtype ) {
            _vxgraph_vxtable__reindex_vertex_type_CS_WL( self, vertex_WL, vxtype );
          }
          // Unlock and commit vertex
          if( _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex_WL, VGX_VERTEX_RECORD_OPERATION ) == false ) {
            vxtype = VERTEX_TYPE_ENUMERATION_ERROR;
          }
        }
      }
      else {
        __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
      }
    }
    else {
      __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
    }
  } GRAPH_RELEASE;
  return vxtype;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * Graph_vertex_get_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason ) {
  const CString_t *CSTR__type = NULL;
  // Compute obid before locking graph
  const objectid_t *pobid = CStringObid( CSTR__vertex_name );
  GRAPH_LOCK( self ) {
    // Retrieve vertex by name
    vgx_Vertex_t *vertex = _vxgraph_vxtable__query_CS( self, NULL, pobid, VERTEX_TYPE_ENUMERATION_WILDCARD );
    if( vertex ) {
      // Lock the vertex readonly
      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
      vgx_Vertex_t *vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_NONE );
      __set_access_reason( reason, timing_budget.reason );
      if( vertex_RO ) {
        // Map code to type string
        CSTR__type = _vxenum_vtx__decode_CS( self, vertex_RO->descriptor.type.enumeration );
        _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex_RO, VGX_VERTEX_RECORD_OPERATION );
      }
    }
    else {
      __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
    }
  } GRAPH_RELEASE;
  return CSTR__type;
}



/*******************************************************************//**
 *
 *
 * NOTE: caller will own memory of returned relation
 ***********************************************************************
 */
static vgx_Relation_t * Graph_has_adjacency( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query ) {

  vgx_Relation_t *relation = NULL;

  vgx_Vertex_t *vertex_RO = NULL;

  vgx_adjacency_search_context_t *search = NULL;

  int64_t qt_ns = 0;
  BEGIN_QUERY( query, &qt_ns ) {

    XTRY {

      bool readonly_graph = false;
      const objectid_t *vertex_obid = query->CSTR__anchor_id ? CStringObid( query->CSTR__anchor_id ) : NULL;

      // Acquire the vertex
      vertex_RO = _vxgraph_state__acquire_readonly_vertex_OPEN( self, vertex_obid, &query->timing_budget );
      __set_access_reason( &query->access_reason, query->timing_budget.reason );
      if( vertex_RO == NULL ) {
        __set_error_string_from_reason( &query->CSTR__error, query->CSTR__anchor_id, query->access_reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA51 );
      }

      // Set up internal adjacency probe from external query 
      if( (search = iGraphProbe.NewAdjacencySearch( self, readonly_graph, vertex_RO, query )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA52 );
      }

      // Execute search
      vgx_Arc_t first_match = {
        .head = {
          .vertex     = NULL,
          .predicator = VGX_PREDICATOR_NONE
        },
        .tail = NULL
      };

      if( (relation = iRelation.New( self, NULL, NULL, NULL, VGX_PREDICATOR_MOD_NONE, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xA53 );
      }

      vgx_ArcFilter_match match = __vertex_has_adjacency( self, query->CSTR__anchor_id, search, &first_match );
      // Found match, build result
      if( match == VGX_ARC_FILTER_MATCH_HIT ) {
        vgx_arc_direction direction = query->arc_condition_set ? query->arc_condition_set->arcdir : VGX_ARCDIR_ANY;
        const char *initial = CStringValue( query->CSTR__anchor_id );
        const char *terminal = NULL;
        if( query->vertex_condition && query->vertex_condition->CSTR__idlist ) {
          if( iString.List.Size( query->vertex_condition->CSTR__idlist ) != 1 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0xA54 );
          }
          terminal = CStringValue( iString.List.GetItem( query->vertex_condition->CSTR__idlist, 0 ) );
        }

        vgx_predicator_t match_pred = first_match.head.predicator;
        if( direction == VGX_ARCDIR_OUT ) {
          iRelation.SetInitial( relation, initial );
          iRelation.SetTerminal( relation, terminal );
        }
        else {
          iRelation.SetInitial( relation, terminal );
          iRelation.SetTerminal( relation, initial );
        }
        const CString_t * CSTR__relationship = iEnumerator_OPEN.Relationship.Decode( self, match_pred.rel.enc );
        iRelation.SetRelationship( relation, CStringValue( CSTR__relationship ) );
        iRelation.SetModifierAndValue( relation, match_pred.mod.bits, &match_pred.val.bits );
      }
      // Error
      else if( __is_arcfilter_error( match ) ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA55 );
      }

      END_QUERY_SEARCH_PHASE;
    }
    XCATCH( errcode ) {
      if( query ) {
        if( _vgx_is_execution_halted( &query->timing_budget ) ) {
          query->access_reason = query->timing_budget.reason;
        }
        if( search ) {
          CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
        }
      }
      INVALIDATE_QUERY_TIMING;
      iRelation.Delete( &relation );
    }
    XFINALLY {
      // Release the vertex
      if( vertex_RO ) {
        _vxgraph_state__release_vertex_OPEN_LCK( self, (vgx_Vertex_t**)&vertex_RO );
      }
      iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
    }
  } END_QUERY;

  GRAPH_LOCK( self ) {
    query->parent_opid = iOperation.GetId_LCK( &self->operation );
    CALLABLE( self )->CountQueryNolock( self, qt_ns );
  } GRAPH_RELEASE;

  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_open_neighbor( vgx_Graph_t *self, vgx_AdjacencyQuery_t *query, bool readonly ) {

  vgx_Vertex_t *neighbor = NULL;
  vgx_Vertex_t *neighbor_LCK = NULL;
  vgx_Vertex_t *vertex_RO = NULL;

  vgx_adjacency_search_context_t *search = NULL;

  int64_t qt_ns = 0;
  BEGIN_QUERY( query, &qt_ns ) {

    XTRY {

      bool readonly_graph = false;
      const objectid_t *vertex_obid = query->CSTR__anchor_id ? CStringObid( query->CSTR__anchor_id ) : NULL;

      // Acquire the vertex
      vertex_RO = _vxgraph_state__acquire_readonly_vertex_OPEN( self, vertex_obid, &query->timing_budget );
      __set_access_reason( &query->access_reason, query->timing_budget.reason );
      if( vertex_RO == NULL ) {
        __set_error_string_from_reason( &query->CSTR__error, query->CSTR__anchor_id, query->access_reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Set up internal adjacency probe from external query 
      if( (search = iGraphProbe.NewAdjacencySearch( self, readonly_graph, vertex_RO, query )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Execute search
      vgx_Arc_t first_match = {
        .head = {
          .vertex     = NULL,
          .predicator = VGX_PREDICATOR_NONE
        },
        .tail = NULL
      };

      vgx_ArcFilter_match match = __vertex_has_adjacency( self, query->CSTR__anchor_id, search, &first_match );
      // Found match, build result
      if( match == VGX_ARC_FILTER_MATCH_HIT ) {
        neighbor = first_match.head.vertex;
      }
      else if( match == VGX_ARC_FILTER_MATCH_MISS ) {
        __set_access_reason( &query->access_reason, VGX_ACCESS_REASON_NOEXIST_MSG );
        CString_t *CSTR__error = CStringNew( "No matching neighbor" );
        CALLABLE( query )->SetErrorString( query, &CSTR__error );
      }
      // Error
      else if( __is_arcfilter_error( match ) ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }

      END_QUERY_SEARCH_PHASE;
    }
    XCATCH( errcode ) {
      if( query ) {
        if( _vgx_is_execution_halted( &query->timing_budget ) ) {
          query->access_reason = query->timing_budget.reason;
        }
        if( search ) {
          CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
        }
      }
      INVALIDATE_QUERY_TIMING;
    }
    XFINALLY {
      // Release the vertex
      if( vertex_RO ) {
        _vxgraph_state__release_vertex_OPEN_LCK( self, (vgx_Vertex_t**)&vertex_RO );
      }
      iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
    }
  } END_QUERY;

  GRAPH_LOCK( self ) {
    query->parent_opid = iOperation.GetId_LCK( &self->operation );
    CALLABLE( self )->CountQueryNolock( self, qt_ns );
    if( neighbor ) {
      if( readonly ) {
        neighbor_LCK = _vxgraph_state__lock_vertex_readonly_CS( self, neighbor, &query->timing_budget, VGX_VERTEX_RECORD_ALL );
      }
      else {
        if( (neighbor_LCK = _vxgraph_state__lock_vertex_writable_CS( self, neighbor, &query->timing_budget, VGX_VERTEX_RECORD_ALL )) != NULL ) {
          // Make sure its REAL. Outsiders (who acquire the vertex) only work with REAL vertices.
          _vxgraph_state__convert_WL( self, neighbor_LCK, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL );
        }
      }
      query->access_reason = query->timing_budget.reason;
    }
  } GRAPH_RELEASE;

  return neighbor_LCK;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcHead_t Graph_arc_value( vgx_Graph_t *self, const vgx_Relation_t *relation, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_ArcHead_t archead = {
    .vertex     = NULL,
    .predicator = VGX_PREDICATOR_NONE
  };
  // Compute obids from ID strings before we acquire state lock (expensive)
  const objectid_t *initial_obid = CStringObid( relation->initial.CSTR__name );
  const objectid_t *terminal_obid = CStringObid( relation->terminal.CSTR__name );
  
  GRAPH_LOCK( self ) {
    // Encode relationship if this relationship exists in the graph
    vgx_predicator_rel_enum rel_enum;
    if( relation->relationship.CSTR__name ) {
      rel_enum = (vgx_predicator_rel_enum)iEnumerator_CS.Relationship.GetEnum( self, relation->relationship.CSTR__name );
      if( !__relationship_enumeration_valid( rel_enum ) || rel_enum == VGX_PREDICATOR_REL_NONEXIST ) {
        // TODO: Distinguish between various errors and report back details to caller via code or error string
        rel_enum = VGX_PREDICATOR_REL_ERROR;
      }
    }
    else {
      rel_enum = VGX_PREDICATOR_REL_WILDCARD;
    }

    if( rel_enum != VGX_PREDICATOR_REL_ERROR ) {
      // Acquire the initial vertex
      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
      vgx_Vertex_t *initial_RO = _vxgraph_state__acquire_readonly_vertex_CS( self, initial_obid, &timing_budget );
      __set_access_reason( reason, timing_budget.reason );
      if( initial_RO ) {
        // Get address of terminal vertex
        vgx_Vertex_t *terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
        if( terminal ) {
          vgx_ArcHead_t probe = { .vertex = terminal, .predicator = {0} };
          probe.predicator.rel.dir = VGX_ARCDIR_OUT;
          probe.predicator.rel.enc = rel_enum;
          probe.predicator.mod = _vgx_predicator_mod_from_enum( relation->relationship.mod_enum );
          vgx_predicator_t found;
          // Stay in CS if lookup will be quick
          if( _vgx_predicator_specific( probe.predicator ) || iarcvector.Degree( &initial_RO->outarcs ) < 200 ) {
            found = iarcvector.GetArcValue( &self->arcvector_fhdyn, &initial_RO->outarcs, &probe );
          }
          // Release CS during lookup
          else {
            GRAPH_SUSPEND_LOCK( self ) {
              found = iarcvector.GetArcValue( &self->arcvector_fhdyn, &initial_RO->outarcs, &probe );
            } GRAPH_RESUME_LOCK;
          }
          if( __predicator_has_relationship( found ) ) {
            archead.vertex = terminal;
            archead.predicator = found;
            archead.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_DISTANCE;
            archead.predicator.eph.value = 1;
          }
        }
        _vxgraph_state__release_vertex_CS_LCK( self, &initial_RO );
      }
    }
  } GRAPH_RELEASE;

  return archead;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_neighborhood( vgx_Graph_t *self, vgx_NeighborhoodQuery_t *query ) {

  int64_t n_hits = 0;

  // Are we creating a new result list or are we appending to a list owned by caller?
  bool external_owner = query->collector != NULL ? true : false;

  vgx_Vertex_t *vertex_RO = NULL;
  int ro_frozen = 0;
  vgx_neighborhood_search_context_t *search = NULL;

  int64_t qt_ns = 0;
  BEGIN_QUERY( query, &qt_ns ) {

    XTRY {

      // Hit counter
      int64_t *hit_counter = NULL;

      // ------------
      // Search phase
      // ------------

      const objectid_t *vertex_obid = query->CSTR__anchor_id ? CStringObid( query->CSTR__anchor_id ) : NULL;

      bool readonly_graph = false;

      // Freeze readonly if needed
      if( (ro_frozen = CALLABLE( self )->advanced->FreezeGraphReadonly_OPEN( self, &query->CSTR__error )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA61 );
      }
      else if( ro_frozen > 0 ) {
        readonly_graph = true;
      }

      // Acquire the vertex
      if( (vertex_RO = _vxgraph_state__acquire_readonly_vertex_OPEN( self, vertex_obid, &query->timing_budget )) == NULL ) {
        __set_access_reason( &query->access_reason, query->timing_budget.reason );
        __set_error_string_from_reason( &query->CSTR__error, query->CSTR__anchor_id, query->access_reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA62 );
      }

      // Validate neighborhood hit counts
      if( iGraphTraverse.ValidateNeighborhoodCollectableCounts( self, readonly_graph, query, vertex_RO ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0xA63 );
      }

      // Set up internal search context from external query specification
      if( (search = iGraphProbe.NewNeighborhoodSearch( self, readonly_graph, vertex_RO, query )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA64 );
      }

      // Pick the appropriate traversal routine (collect arcs or vertices)
      int (*traverse_neighborhood)( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search );
      if( _vgx_collector_mode_type( query->collector_mode ) == VGX_COLLECTOR_MODE_COLLECT_VERTICES ) {
        traverse_neighborhood = iGraphTraverse.TraverseNeighborVertices;
        hit_counter = &search->n_vertices;
      }
      else if( _vgx_collector_mode_type( query->collector_mode ) == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
        traverse_neighborhood = iGraphTraverse.TraverseNeighborArcs;
        hit_counter = &search->n_arcs;
      }
      else {
        THROW_ERROR( CXLIB_ERR_API, 0xA65 );
      }
      
      // Who owns the final result list?
      if( external_owner ) {
        search->result = query->collector; // append to existing list rather than creating new one to return
      }
      
      // Bi-directional: Arcs can be either in or out
      if( query->arc_condition_set == NULL || query->arc_condition_set->arcdir == VGX_ARCDIR_ANY ) {
        // 1: search the outarcs
        search->probe->traversing.arcdir = VGX_ARCDIR_OUT;
        if( traverse_neighborhood( vertex_RO, search ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0xA66 );
        }

        // 2: search the inarcs
        search->probe->traversing.arcdir = VGX_ARCDIR_IN;
        if( traverse_neighborhood( vertex_RO, search ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0xA67 );
        }
      }
      // Search IN, OUT or BOTH
      else {
        if( traverse_neighborhood( vertex_RO, search ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0xA68 );
        }
      }

      END_QUERY_SEARCH_PHASE;

      // ------------
      // Result phase
      // ------------

      // Set the number of search hits from the selected counter
      n_hits = *hit_counter;

      bool is_aggregation = _vgx_collector_is_aggregation( search->collector );

      // Convert collector to a base list which will contain the final output in the correct order
      // NEW: baselist->n_collectable is now n_hits.
      if( iGraphCollector.ConvertToBaseListCollector( search->collector ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA69 );
      }

      // Refresh counts in case the consume collector modified them (if we're using aggregation)
      if( is_aggregation ) {
        n_hits = search->collector->n_collectable;
        search->n_arcs = n_hits;
        search->n_neighbors = n_hits;
      }

      // Discard items at end of list (if we didn't collect as many as requested) and at beginning of list (if offset > 0)
      if( iGraphCollector.TrimBaseListCollector( search->collector, n_hits, search->offset, search->hits ) == NULL ) {
        iGraphCollector.DeleteCollector( &search->collector );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA6A );
      }
      
      // Transfer the search results to the output: n_hits now counts the entire result, including any previous items from a previous query if they exist
      n_hits = iGraphCollector.TransferBaseList( search->ranking_context, &search->collector, &search->result );

      // Set brand new result collector as query result since caller did not supply a collector
      if( external_owner == false ) {
        query->collector = search->result;
      }
      
      // Forget the search result - query has the official reference
      search->result = NULL;

      // Patch in the evaluator memory as needed
      if( query->selector ) {
        if( query->evaluator_memory == NULL ) {
          if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0xA6B );
          }
        }
        CALLABLE( query->selector )->OwnMemory( query->selector, query->evaluator_memory );
      }

      // Render search result into a new, self contained object
      //
      //
      vgx_ResponseFields_t response_fields = { 
        .fastmask       = query->fieldmask,
        .selecteval     = query->selector,
        .include_mod_tm = true
      };

      query->n_arcs = search->n_arcs;
      query->n_neighbors = search->n_neighbors;
      if( iGraphResponse.BuildSearchResult( self, &response_fields, NULL, (vgx_BaseQuery_t*)query ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA6C );
      }

      // ------------
      // End
      // ------------
      END_QUERY_RESULT_PHASE;

    }
    XCATCH( errcode ) {
      if( query ) {
        if( _vgx_is_execution_halted( &query->timing_budget ) ) {
          query->access_reason = query->timing_budget.reason;
        }
        if( search ) {
          CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
        }
      }
      iGraphResponse.DeleteSearchResult( &query->search_result );
      n_hits = -1;
      INVALIDATE_QUERY_TIMING;
    }
    XFINALLY {
      // Don't report counts if they're not accurate
      if( !(search && search->counts_are_deep) ) {
        query->n_arcs = -1;
        query->n_neighbors = -1;
      }
      // Delete the search object
      iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
      
    }

  } END_QUERY;

  // Release the vertex and unfreeze
  if( vertex_RO || ro_frozen > 0 ) {
    GRAPH_LOCK( self ) {
      query->parent_opid = iOperation.GetId_LCK( &self->operation );
      if( vertex_RO ) {
        _vxgraph_state__release_vertex_CS_LCK( self, (vgx_Vertex_t**)&vertex_RO );
      }
      if( ro_frozen > 0 ) {
        CALLABLE( self )->advanced->UnfreezeGraphReadonly_CS( self );
      }
      CALLABLE( self )->CountQueryNolock( self, qt_ns );
    } GRAPH_RELEASE;
  }

  return n_hits;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_aggregate( vgx_Graph_t *self, vgx_AggregatorQuery_t *query ) {
  // TODO: Return SearchResult from this method instead, to encapsulate the entire
  // query form start to finish with readonly protection and all that.
  //
  // Then wrap this call in a readonly freeze block.
  //


  int64_t n_aggregated = 0;
  
  vgx_aggregator_search_context_t *search = NULL;
  vgx_Vertex_t *vertex_RO = NULL;
  int ro_frozen = 0;

  int64_t qt_ns = 0;
  BEGIN_QUERY( query, &qt_ns ) {

    XTRY {

      bool readonly_graph = false;

      const objectid_t *vertex_obid = query->CSTR__anchor_id ? CStringObid( query->CSTR__anchor_id ) : NULL;

      // Freeze readonly if needed
      if( (ro_frozen = CALLABLE( self )->advanced->FreezeGraphReadonly_OPEN( self, &query->CSTR__error )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA70 );
      }
      else if( ro_frozen > 0 ) {
        readonly_graph = true;
      }

      // Acquire the vertex
      if( (vertex_RO = _vxgraph_state__acquire_readonly_vertex_OPEN( self, vertex_obid, &query->timing_budget )) == NULL ) {
        __set_access_reason( &query->access_reason, query->timing_budget.reason );
        __set_error_string_from_reason( &query->CSTR__error, query->CSTR__anchor_id, query->access_reason );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA71 );
      }

      // Set up internal search context from external query specification
      if( (search = iGraphProbe.NewAggregatorSearch( self, readonly_graph, vertex_RO, query )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0xA72 );
      }
      
      // Any-directional neighborhood aggregation
      if( query->arc_condition_set == NULL || query->arc_condition_set->arcdir == VGX_ARCDIR_ANY ) {
        search->probe->traversing.arcdir = VGX_ARCDIR_OUT;
        if( __vertex_aggregator( self, vertex_RO, search ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA73 );
        }
        search->probe->traversing.arcdir = VGX_ARCDIR_IN;
        if( __vertex_aggregator( self, vertex_RO, search ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA74 );
        }
      }
      // Uni-directional neighborhood aggregation
      else {
        search->probe->traversing.arcdir = query->arc_condition_set->arcdir;
        // Execute search
        if( __vertex_aggregator( self, vertex_RO, search ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0xA75 );
        }
      }

      END_QUERY_SEARCH_PHASE;

      n_aggregated = search->n_arcs;

      // NOTE: query->fields already points to the same data as search->fields
      // We have used this pointer to perform aggregation. Set search->fields
      // to NULL now to forget this pointer, to avoid resetting all data when
      // deleting the search object below.
      search->fields = NULL;

    }
    XCATCH( errcode ) {
      if( query && search ) {
        CALLABLE( query )->SetErrorString( query, &search->CSTR__error );
      }
      INVALIDATE_QUERY_TIMING;
      n_aggregated = -1;
    }
    XFINALLY {
      // Set the counts
      if( search && search->counts_are_deep ) {
        query->n_arcs = search->n_arcs;
        query->n_neighbors = search->n_neighbors;
      }
      else { // don't report counts if they're not accurate
        query->n_arcs = -1;
        query->n_neighbors = -1;
      }
      // Delete the search object
      iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );

    }
  } END_QUERY;

  // Release the vertex and unfreeze
  if( vertex_RO || ro_frozen > 0 ) {
    GRAPH_LOCK( self ) {
      if( vertex_RO ) {
        _vxgraph_state__release_vertex_CS_LCK( self, (vgx_Vertex_t**)&vertex_RO );
      }
      if( ro_frozen > 0 ) {
        CALLABLE( self )->advanced->UnfreezeGraphReadonly_CS( self );
      }
      CALLABLE( self )->CountQueryNolock( self, qt_ns );
    } GRAPH_RELEASE;
  }


  return n_aggregated;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_arcs( vgx_Graph_t *self, vgx_GlobalQuery_t *query ) {
  return __global_query( self, query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_vertices( vgx_Graph_t *self, vgx_GlobalQuery_t *query ) {
  return __global_query( self, query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_order_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_type ) {
  vgx_vertex_type_t vxtype;
  // NULL means the untyped vertex
  if( CSTR__vertex_type == NULL ) {
    vxtype = VERTEX_TYPE_ENUMERATION_VERTEX;
  }
  // Wildcard means remove all vertices
  else if( CStringEqualsChars( CSTR__vertex_type, "*" ) ) {
    vxtype = VERTEX_TYPE_ENUMERATION_WILDCARD;
  }
  // Encode type string
  else {
    vxtype = (vgx_vertex_type_t)iEnumerator_OPEN.VertexType.GetEnum( self, CSTR__vertex_type );
  }

  if( vxtype == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
    return 0;
  }
  else if( __vertex_type_enumeration_valid( vxtype ) ) {
    return _vxgraph_vxtable__len_OPEN( self, vxtype );
  }
  else {
    return -1;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_relationships( vgx_Graph_t *self, CtptrList_t **CSTR__strings ) {
  // Create new return list unless one is supplied by caller.
  if( *CSTR__strings == NULL ) {
    // NOTE: Caller will own the new list!
    *CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t );
  }

  if( *CSTR__strings != NULL ) {
    return iEnumerator_OPEN.Relationship.GetAll( self, *CSTR__strings );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_vertex_types( vgx_Graph_t *self, CtptrList_t **CSTR__strings ) {
  // Create new return list unless one is supplied by caller.
  if( *CSTR__strings == NULL ) {
    // NOTE: Caller will own the new list!
    *CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t );
  }

  if( *CSTR__strings != NULL ) {
    return iEnumerator_OPEN.VertexType.GetAll( self, *CSTR__strings );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_property_keys( vgx_Graph_t *self, CtptrList_t **CSTR__strings ) {
  // Create new return list unless one is supplied by caller.
  if( *CSTR__strings == NULL ) {
    // NOTE: Caller will own the new list!
    *CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t );
  }

  if( *CSTR__strings != NULL ) {
    return iEnumerator_OPEN.Property.Key.GetAll( self, *CSTR__strings );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_property_string_values( vgx_Graph_t *self, CtptrList_t **CSTR__string_qwo_list ) {
  // Create new return list unless one is supplied by caller.
  if( *CSTR__string_qwo_list == NULL ) {
    // NOTE: Caller will own the new list!
    *CSTR__string_qwo_list = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t );
  }

  if( *CSTR__string_qwo_list != NULL ) {
    return iEnumerator_OPEN.Property.Value.GetAll( self, *CSTR__string_qwo_list );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_truncate( vgx_Graph_t *self, CString_t **CSTR__error ) {
  return _vxgraph_vxtable__truncate_OPEN( self, VERTEX_TYPE_ENUMERATION_WILDCARD, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_truncate_type( vgx_Graph_t *self, const CString_t *CSTR__vertex_type, CString_t **CSTR__error ) {
  vgx_vertex_type_t vxtype;
  // NULL means the untyped vertex
  if( CSTR__vertex_type == NULL ) {
    vxtype = VERTEX_TYPE_ENUMERATION_VERTEX;
  }
  // Wildcard means remove all vertices
  else if( CStringEqualsChars( CSTR__vertex_type, "*" ) ) {
    vxtype = VERTEX_TYPE_ENUMERATION_WILDCARD;
  }
  // Encode type string
  else {
    vxtype = (vgx_vertex_type_t)iEnumerator_OPEN.VertexType.GetEnum( self, CSTR__vertex_type );
  }

  if( vxtype == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
    return 0;
  }
  else if( __vertex_type_enumeration_valid( vxtype ) ) {
    return _vxgraph_vxtable__truncate_OPEN( self, vxtype, CSTR__error );
  }
  else {
    if( CSTR__error ) {
      *CSTR__error = CStringNewFormat( "invalid vertex type: '%s'", CStringValue( CSTR__vertex_type ) );
    }
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Evaluator_t * Graph_define_evaluator( vgx_Graph_t *self, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error ) {
  vgx_Evaluator_constructor_args_t args = {
    .parent       = self,
    .expression   = expression,
    .vector       = vector,
    .CSTR__error  = CSTR__error
  };
  // Create new evaluator object from expression
  vgx_Evaluator_t *eobj = COMLIB_OBJECT_NEW( vgx_Evaluator_t, NULL, &args ); // refc=2 (one for new object, one for map ownership)
  // Verify correct creation and insertion into evaluator map
  if( eobj ) {
    if( eobj->rpn_program.CSTR__assigned == NULL ) {
      __set_error_string( CSTR__error, "assignment required: \"<name> := <expression>\"" );
    }
    else {
      // Make sure evaluator can be retrieved
      vgx_Evaluator_t *E;
      const char *name = CStringValue( eobj->rpn_program.CSTR__assigned );
      if( (E = Graph_get_evaluator( self, name )) != NULL ) { // refc=3 OK
        CALLABLE( E )->Discard( E );
      }
      // Correct instance
      if( E == eobj ) {
        return E; 
      }
    }
    // Error
    COMLIB_OBJECT_DESTROY( eobj );
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Evaluator_t * Graph_get_evaluator( vgx_Graph_t *self, const char *name ) {
  vgx_Evaluator_t *evaluator = NULL;
  objectid_t obid;
  obid.H = obid.L = CharsHash64( name );
  GRAPH_LOCK( self ) {
    if( CALLABLE( self->evaluators )->GetObj128Nolock( self->evaluators, &obid, (comlib_object_t**)&evaluator ) == CELL_VALUE_TYPE_OBJECT128 && evaluator != NULL ) {
      CALLABLE( evaluator )->Own( evaluator );
    }
    else {
      evaluator = NULL;
    }
  } GRAPH_RELEASE;
  return evaluator;
}



/*******************************************************************//**
 * Return NULL-terminated list of evaluator object pointers.
 * NOTE: CALLER OWNS RETURNED LIST!
 ***********************************************************************
 */
static vgx_Evaluator_t ** Graph_get_evaluators( vgx_Graph_t *self, int64_t *sz ) {
  vgx_Evaluator_t **evaluators = NULL;
  GRAPH_LOCK( self ) {
    framehash_t *E = self->evaluators;
    CtptrList_t *list = E->_dynamic.ref_list;
    f_CtptrList_get get_value = CALLABLE( list )->Get;
    *sz = CALLABLE( E )->GetValues( E );
    if( (evaluators = calloc( *sz+1, sizeof( vgx_Evaluator_t* ) )) != NULL ) {
      vgx_Evaluator_t **cursor = evaluators;
      for( int64_t i=0; i<*sz; i++ ) {
        tptr_t value;
        if( get_value( list, i, &value ) == 1 ) {
          if( TPTR_IS_POINTER( &value ) ) {
            if( (*cursor = (vgx_Evaluator_t*)TPTR_GET_POINTER( &value )) != NULL ) {
              vgx_Evaluator_t *e = *cursor++;
              CALLABLE( e )->Own( e );
            }
          }
        }
      }
    }
    CALLABLE( list )->Clear( list );
  } GRAPH_RELEASE;
  return evaluators;
}



/*******************************************************************//**
 * DEPRECATED - use Degree()
 ***********************************************************************
 */
static int64_t Graph_vertex_degree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason ) {
  return Graph_degree( self, CSTR__vertex_name, VGX_ARCDIR_ANY, -1, reason );
}



/*******************************************************************//**
 * DEPRECATED - use Degree()
 ***********************************************************************
 */
static int64_t Graph_vertex_indegree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason ) {
  return Graph_degree( self, CSTR__vertex_name, VGX_ARCDIR_IN, -1, reason );
}



/*******************************************************************//**
 * DEPRECATED - use Degree()
 ***********************************************************************
 */
static int64_t Graph_vertex_outdegree( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason ) {
  return Graph_degree( self, CSTR__vertex_name, VGX_ARCDIR_OUT, -1, reason );
}








#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxapi_simple.h"

test_descriptor_t _vgx_vxapi_simple_tests[] = {
  { "VGX Graph Simple API Tests", __utest_vxapi_simple },
  {NULL}
};
#endif
