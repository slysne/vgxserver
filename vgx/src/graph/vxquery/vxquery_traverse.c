/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_traverse.c
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

#include "_vxtraverse.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static void __set_collectable_counts( const int64_t hits, const int offset, vgx_collect_counts_t *counts );

static vgx_collect_counts_t _vxquery_traverse__get_neighborhood_collectable_counts( vgx_Graph_t *self, bool readonly_graph, const vgx_Vertex_t *vertex_RO, vgx_arc_direction arcdir, int offset, int64_t hits );
static vgx_collect_counts_t _vxquery_traverse__get_global_collectable_counts( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
static int64_t _vxquery_traverse__validate_neighborhood_collectable_counts( vgx_Graph_t *self, bool readonly_graph, vgx_NeighborhoodQuery_t *query, const vgx_Vertex_t *vertex_RO );
static int64_t _vxquery_traverse__validate_global_collectable_counts( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
static int _vxquery_traverse__traverse_neighbor_arcs_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search );
static int _vxquery_traverse__traverse_neighbor_vertices_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search );
static int _vxquery_traverse__traverse_global_items_OPEN( vgx_Graph_t *self, vgx_global_search_context_t *search, bool readonly_graph );
static int _vxquery_traverse__aggregate_neighborhood_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_aggregator_search_context_t *search );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IGraphTraverse_t iGraphTraverse = {
  .GetNeighborhoodCollectableCounts       = _vxquery_traverse__get_neighborhood_collectable_counts,
  .GetGlobalCollectableCounts             = _vxquery_traverse__get_global_collectable_counts,
  .ValidateNeighborhoodCollectableCounts  = _vxquery_traverse__validate_neighborhood_collectable_counts,
  .ValidateGlobalCollectableCounts        = _vxquery_traverse__validate_global_collectable_counts,
  .TraverseNeighborArcs                   = _vxquery_traverse__traverse_neighbor_arcs_OPEN_RO,
  .TraverseNeighborVertices               = _vxquery_traverse__traverse_neighbor_vertices_OPEN_RO,
  .TraverseGlobalItems                    = _vxquery_traverse__traverse_global_items_OPEN,
  .AggregateNeighborhood                  = _vxquery_traverse__aggregate_neighborhood_OPEN_RO,
};




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_collectable_counts( const int64_t hits, const int offset, vgx_collect_counts_t *counts ) {
  // offset never less than 0
  counts->offset = offset < 0 ? 0 : offset;
  // hits defaults to entire neighborhood (if negative requested)
  counts->hits = hits < 0 ? counts->data_size : hits;
  // collect count equals hits plus offset
  counts->n_collect = counts->hits + counts->offset;                   
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_collect_counts_t _vxquery_traverse__get_neighborhood_collectable_counts( vgx_Graph_t *self, bool readonly_graph, const vgx_Vertex_t *vertex_RO, vgx_arc_direction arcdir, int offset, int64_t hits ) {

  // the return value
  vgx_collect_counts_t counts = {0};

  // Get the total number of arcs in the immediate vertex neighborhood
  // NOTE: This does not extend beyond the immediate neighborhood so when collecting
  // beyond immediate neighborhood we can't get the upper counts automatically.
  if( (counts.data_size = __get_total_neighborhood_size( vertex_RO, arcdir )) < 0 ) {
    counts.n_collect = -1;
  }
  else {
    // Compute the counts
    __set_collectable_counts( hits, offset, &counts );
  }

  return counts;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_collect_counts_t _vxquery_traverse__get_global_collectable_counts( vgx_Graph_t *self, vgx_GlobalQuery_t *query ) {

  // The return value
  vgx_collect_counts_t counts = {0};
  vgx_vertex_type_t vxtype = VERTEX_TYPE_ENUMERATION_WILDCARD;

  switch( query->collector_mode ) {
  case VGX_COLLECTOR_MODE_COLLECT_ARCS:
    counts.data_size = GraphSize( self );
    if( query->vertex_condition ) {
      if( iVertexCondition.HasArcTraversal( query->vertex_condition ) ) {
        if( query->vertex_condition->advanced.recursive.traversing.arc_condition_set->arcdir == VGX_ARCDIR_ANY ) {
          counts.data_size *= 2;
        }
      }
    }
    break;
  case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
    if( query->vertex_condition ) {
      if( _vgx_vertex_condition_has_obid_match( query->vertex_condition->spec ) ) {
        if( query->hits != 0 ) {
          query->hits = 1;
        }
        break;
      }
      else if( (vxtype = iVertexCondition.GetTypeEnumeration( query->vertex_condition, self )) == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
        // Early termination
        return counts;
      }
      else if( !__vertex_type_enumeration_valid( vxtype ) ) {
        if( vxtype == VERTEX_TYPE_ENUMERATION_INVALID && query->vertex_condition->CSTR__vertex_type != NULL ) {
          if( !query->CSTR__error ) {
            query->CSTR__error = CStringNewFormat( "Invalid vertex type: '%s'", CStringValue( query->vertex_condition->CSTR__vertex_type ) );
          }
        }
        // Error
        counts.n_collect = -1;
      }
    }

    if( vxtype != VERTEX_TYPE_ENUMERATION_NO_MAPPING ) {
      // Get the total number of vertices in the graph
      counts.data_size = _vxgraph_vxtable__len_OPEN( self, vxtype );
    }
    break;

  default:
    break;
  }

  // Compute the counts
  __set_collectable_counts( query->hits, query->offset, &counts );

  return counts;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxquery_traverse__validate_neighborhood_collectable_counts( vgx_Graph_t *self, bool readonly_graph, vgx_NeighborhoodQuery_t *query, const vgx_Vertex_t *vertex_RO ) {
  int64_t n_collect = 0;
  vgx_NeighborhoodQuery_t *pre_query = NULL;
  int64_t system_limit = Comlib_Cm256iList_t_ElementCapacity();
  CString_t *CSTR__error = NULL;

  XTRY {
    //
    vgx_collect_counts_t counts = { .n_collect = query->hits + query->offset };
    // We ask for all vertices or a very large number of vertices: get actual count to see if we can fulfil request
    if( query->hits < 0 || query->hits > system_limit ) {
      // First get the total worst-case (no filters) neighborhood count to see if a pre query is needed
      vgx_arc_direction arcdir = query->arc_condition_set ? query->arc_condition_set->arcdir : VGX_ARCDIR_ANY;
      counts = iGraphTraverse.GetNeighborhoodCollectableCounts( self, readonly_graph, vertex_RO, arcdir, query->offset, query->hits );
      // If too many immediate neighbors, run a pre-query with the same filters as the query (but hits=0)
      // to get the actual neighborhood count we should expect to collect. We normally don't do this, so we avoid two queries, but now we have to.
      // Also run the pre query if the query is recursive into extended neighborhood
      if( counts.n_collect > system_limit || _vgx_vertex_condition_is_recursive( query->vertex_condition ) ) {
        if( (pre_query = iGraphQuery.CloneNeighborhoodQuery( query, &CSTR__error )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x388 );
        }
        pre_query->hits = 0;
        pre_query->offset = 0;
        pre_query->ranking_condition->sortspec = VGX_SORTBY_NATIVE | VGX_SORT_DIRECTION_ASCENDING; // need sorting to get deep counts
        if( CALLABLE(self)->simple->Neighborhood( self, pre_query ) < 0 ) {
          if( _vgx_is_execution_halted( &pre_query->timing_budget ) ) {
            __format_error_string( &query->CSTR__error, "Timeout during neighborhood validation @ %p", pre_query->timing_budget.resource );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x389 );
          }
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x38A );
        }
        // The actual number of neighbors to collect (may still be too high)
        counts.n_collect = pre_query->n_arcs;
        query->hits = counts.n_collect - query->offset;
      }
    }
    // Check that query hit count (whether specified directly or computed with a pre-query) is within maximum system limit
    if( counts.n_collect > system_limit ) {
      if( !query->CSTR__error ) {
        query->CSTR__error = CStringNewFormat( "Maximum hit count exceeded: %lld > %lld", counts.n_collect, system_limit );
      }
      THROW_SILENT( CXLIB_ERR_API, 0x38B );
    }
    n_collect = counts.n_collect;
  }
  XCATCH( errcode ) {
    if( pre_query ) {
      query->access_reason = pre_query->access_reason;
      if( pre_query->CSTR__error ) {
        __set_error_string( &query->CSTR__error, CStringValue( pre_query->CSTR__error ) );
      }
    }
    if( CSTR__error ) {
      __set_error_string( &query->CSTR__error, CStringValue( CSTR__error ) );
    }
    else {
      __set_error_string( &query->CSTR__error, "Unknown error during neighborhood validation" );
    }
    n_collect = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
    iGraphQuery.DeleteNeighborhoodQuery( &pre_query );
  }

  return n_collect;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxquery_traverse__validate_global_collectable_counts( vgx_Graph_t *self, vgx_GlobalQuery_t *query ) {

  int64_t n_collect = 0;
  vgx_GlobalQuery_t *pre_query = NULL;
  int64_t system_limit = Comlib_Cm256iList_t_ElementCapacity();

  CString_t *CSTR__error = NULL;

  XTRY {
    //
    vgx_collect_counts_t counts = { .n_collect = query->hits + query->offset };
    // We ask for all items or a very large number of items: get actual count to see if we can fulfil request
    if( query->hits < 0 || query->hits > system_limit ) {
      // First get the total item count in graph (no filters other than vxtype and collector mode) to see if a pre query is needed
      counts = iGraphTraverse.GetGlobalCollectableCounts( self, query );
      // Early termination
      if( counts.data_size == 0 ) {
        counts.n_collect = 0;
      }
      // If too many vertices in the graph, run a pre-query with the same filters (but hits=0)
      // to get the actual counts. We normally don't do this, so we avoid two queries, but now we have to.
      else if( counts.n_collect > system_limit ) {
        if( (pre_query = iGraphQuery.CloneGlobalQuery( query, &CSTR__error )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x383 );
        }
        pre_query->hits = 0;
        pre_query->offset = 0;
        pre_query->ranking_condition->sortspec = VGX_SORTBY_NATIVE | VGX_SORT_DIRECTION_ASCENDING; // need sorting to get deep counts
        if( CALLABLE(self)->simple->Vertices( self, pre_query ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x384 );
        }
        // The actual number of vertices to collect (may still be too high)
        counts.n_collect = pre_query->n_items;
        query->hits = counts.n_collect - query->offset;
      }
      // Error
      else if( counts.n_collect < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x385 );
      }
    }
    // Check that query hit count (whether specified directly or computed with a pre-query) is within maximum system limit
    if( counts.n_collect > system_limit ) {
      if( !query->CSTR__error ) {
        query->CSTR__error = CStringNewFormat( "Maximum hit count exceeded: %lld > %lld", counts.n_collect, system_limit );
      }
      THROW_SILENT( CXLIB_ERR_API, 0x386 );
    }
    n_collect = counts.n_collect;
  }
  XCATCH( errcode ) {
    if( pre_query && pre_query->CSTR__error ) {
      __set_error_string( &query->CSTR__error, CStringValue( pre_query->CSTR__error ) );
    }
    else if( CSTR__error ) {
      __set_error_string( &query->CSTR__error, CStringValue( CSTR__error ) );
    }
    else {
      __set_error_string( &query->CSTR__error, "Unknown error during global counts validation" );
    }
    n_collect = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
    iGraphQuery.DeleteGlobalQuery( &pre_query );
  }

  return n_collect;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vgxquery_traverse__recursive_next_beam_width( vgx_recursion_config_t *recursion, int64_t current_beam_width ) {
  // next = width * curve + offset
  int64_t next = (int64_t)round( current_beam_width * recursion->beam.curve + recursion->beam.offset );
  // clamp 
  return clamp_value( next, recursion->beam.max_width, recursion->beam.min_width );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vgxquery_traverse__recursive_beamify_frontier( vgx_BaseCollector_context_t *collector, vgx_recursion_config_t *recursion, vgx_CollectorItem_t *difficulty ) {

  Cm256iHeap_t *B = collector->beam_heap;
  vgx_FrontierQueue_t *Q = collector->frontier;
  vgx_CollectorItem_t frontier = {0};
  vgx_CollectorItem_t discarded = {0};
  int64_t effective_width;

  // Adjust beam width
  collector->beam_width = _vgxquery_traverse__recursive_next_beam_width( recursion, collector->beam_width );
  // Mutilate the heap size attribute (internal buffer remains sufficiently allocated due to max_beam_width guarantees when created)
  // Beam heap is never larger than the current frontier queue, but usually smaller controlled by current beam_width
  effective_width = minimum_value( collector->beam_width, ComlibSequenceLength(Q) );
  // Initialize beam heap
  CALLABLE(B)->Clear(B);
  CALLABLE(B)->Initialize(B, &collector->empty.item, effective_width);
  // Consume entire frontier queue and push to beam heap
  while( CALLABLE(Q)->Next(Q, &frontier.item) ) {
    // Prune as we go, ignoring frontier items known to be inferior at this point
    if( B->_cmp( &difficulty->item, &frontier.item ) > 0 ) { // heap-compare, for min-heaps "root > candidate" means the root is small and will be yanked
      // Push and replace, item transferred from frontier to beam retains same vertex reference
      if( CALLABLE(B)->HeapPushTopK(B, &frontier.item, &discarded.item ) != NULL ) {
        // Item discarded from beam must be closed if it is a non-dummy item
        if( discarded.headref && discarded.headref->refcnt > 0 ) {
          _vxquery_collector__del_vertex_reference_OPEN( collector, discarded.headref ); // <- former beam item closed
        }
        continue;
      }
    }
    // Nothing pushed, which means this frontier item is not part of the beam
    _vxquery_collector__del_vertex_reference_OPEN( collector, frontier.headref ); // <- inferior frontier item closed
  }

  // Frontier queue now empty and beam contains frontier's top items. Sort before we push
  qsort( B->_buffer, B->_size, sizeof( vgx_CollectorItem_t ), (int (*)( const void *, const void *))B->_cmp );

  // Push beam back on frontier queue to continue processing
  vgx_CollectorItem_t *buffer = (vgx_CollectorItem_t*)B->_buffer;
  for( int i=0; i<effective_width; ++i ) {
    // Next beam heap entry
    vgx_CollectorItem_t *pitem = &buffer[i];
    if( pitem->headref->refcnt < 0 ) { // <- Safety: ignore dummy item, but should never exist due to heap size set above
      continue;
    }
    // Push to frontier queue (transferring ownership back)
    CALLABLE(Q)->Append(Q, &pitem->item); // <- should always succeed
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcFilter_match _vxquery_traverse__recursive_traverse_neighbor_outarcs_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search ) {
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_ERROR;
  vgx_BaseCollector_context_t *collector = search->collector;
  vgx_recursion_config_t *recursion = &search->recursion;

  vgx_FrontierQueue_t *Q = collector->frontier;
  Cm256iHeap_t *main_heap = collector->container.sequence.heap;
  if( Q == NULL || main_heap == NULL ) {
    return VGX_ARC_FILTER_MATCH_ERROR;
  }
  Cm256iHeap_t *beam_heap = collector->beam_heap;
  
  // Early termination triggers
  int64_t frontier_limit = recursion->limit.frontier;
  int64_t expansion_limit = recursion->limit.expansion;
  int64_t depth_limit = recursion->limit.depth;
  int64_t exec_nanosec_limit = recursion->limit.exec_ms * 1000000LL;

  // Beam parameters
  int64_t beam_width = recursion->beam.width;
  int64_t beam_offset = recursion->beam.offset;
  int64_t beam_min = recursion->beam.min_width;
  int64_t beam_max = recursion->beam.max_width;
  double beam_curve = recursion->beam.curve;

  XTRY {

    // Handle edge case
    if( expansion_limit < 1 || depth_limit < 1 ) {
      match = VGX_ARC_FILTER_MATCH_MISS;
      XBREAK;
    }
    if( recursion->visit.reset_state ) {
      vgx_Evaluator_t *E = search->probe->traversing.arcfilter->traversing_evaluator;
      if( E ) {
        iEvaluator.ClearDWordSet( E->context.memory );
      }
    }
    
    // Initialize beam width in the collector
    collector->beam_width = recursion->beam.width;

    int64_t t0_ns = __GET_CURRENT_NANOSECOND_TICK();
    int64_t t_end_ns = exec_nanosec_limit > 0 ? t0_ns + exec_nanosec_limit : -1;

    // Initial neighborhood traversal
    int64_t expansions = 1;   // initial query implied
    int64_t depth = 1;        // initial neighborhood implied
    if( __is_arcfilter_error( (match = iarcvector.GetArcs( &vertex_RO->outarcs, search->probe )) ) ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Limits reached after initial traversal
    if( ComlibSequenceLength(Q) > frontier_limit || expansions >= expansion_limit || depth >= depth_limit ) {
      XBREAK;
    }

    // Initialize difficulty to none
    vgx_CollectorItem_t difficulty;
    CALLABLE(main_heap)->HeapTop(main_heap, &difficulty.item);

    // Number of nodes in initial neighborhood to expand
    int64_t level_size = ComlibSequenceLength(Q);
    vgx_CollectorItem_t frontier = {0};

    while( level_size > 0 ) {
      // Max depth reached
      if( ++depth > depth_limit ) {
        goto terminate_outer;
      }
      // Frontier size at the start of this loop is exactly the number of nodes at the current depth
      for( int64_t i=0; i<level_size; ++i ) {
        
        // TODO:
        // If beam enabled, consume the beam heap items instead of the frontier queue (which is empty)
        // 

        // Next anchor in the frontier
        if( CALLABLE(Q)->Next(Q, &frontier.item) < 1 || frontier.headref->vertex == NULL ) {
          THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x009 );
        }

        // Require items from the queue to outrank the lowest scoring item on the heap
        if( main_heap->_cmp( &difficulty.item, &frontier.item ) > 0 ) { // heap-compare, for min-heaps "root > candidate" means the root is small and will be yanked
          // Perform expansion around next anchor (frontier limit is enforced by the internal collector)
          vgx_Vertex_t *next = frontier.headref->vertex;
          if( __is_arcfilter_error( iarcvector.GetArcs( &next->outarcs, search->probe ) ) ) {
            match = VGX_ARC_FILTER_MATCH_ERROR;
            goto terminate_inner;
          }

          // Early termination if queue limit reached
          if( ++expansions >= expansion_limit ) {
            goto terminate_inner;
          }

          // We are execution time limited, check
          if( t_end_ns > 0 && __GET_CURRENT_NANOSECOND_TICK() > t_end_ns ) {
            goto terminate_inner;
          }

          // Update min score to increase difficulty after heap refinement
          CALLABLE(main_heap)->HeapTop(main_heap, &difficulty.item);
        }

        // Used item from frontier must be closed here
        _vxquery_collector__del_vertex_reference_OPEN( collector, frontier.headref );
      }

      // If beam enabled, focus beam to current top beam-width items in the frontier
      if( beam_heap ) {
        _vgxquery_traverse__recursive_beamify_frontier( collector, recursion, &difficulty );
      }

      // Next level is the queue size after expanding current level
      level_size = ComlibSequenceLength(Q);
    }

    // The initial match only since this determines the overall whether anything was found at all
    XBREAK;

  terminate_inner:
    _vxquery_collector__del_vertex_reference_OPEN( collector, frontier.headref );

  terminate_outer:
    GRAPH_LOCK( collector->graph ) {
      while( CALLABLE(Q)->Next(Q, &frontier.item) > 0 ) {
        _vxquery_collector__del_vertex_reference_ACQUIRE_CS( collector, frontier.headref, &collector->graph );
      }
    } GRAPH_RELEASE;
  }
  XCATCH( errcode ) {
    match = VGX_ARC_FILTER_MATCH_ERROR;
  }
  XFINALLY {
  }

  return match;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_traverse__traverse_neighbor_arcs_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search ) {
  __assert_vertex_lock( vertex_RO );

  int ret = 0;

  Cm256iList_t *neighborhood = NULL;

  XTRY {
    vgx_ExecutionTimingBudget_t *tb = search->timing_budget;
    bool blocking = _vgx_is_execution_blocking( tb );
    vgx_arc_direction arcdir = search->probe->traversing.arcdir;
    const vgx_ArcVector_cell_t *V1 = NULL;
    const vgx_ArcVector_cell_t *V2 = NULL;
    vgx_ArcFilter_match match;

    if( arcdir == VGX_ARCDIR_BOTH ) {
      V1 = &vertex_RO->inarcs;
      V2 = &vertex_RO->outarcs;
      match = iarcvector.GetArcsBidirectional( V1, V2, search->probe );
    }
    else {
      if( search->recursion.mode == VGX_RECURSION_MODE_NONE ) {
        V1 = arcdir == VGX_ARCDIR_IN ? &vertex_RO->inarcs : &vertex_RO->outarcs;
        match = iarcvector.GetArcs( V1, search->probe );
      }
      else if( search->recursion.mode == VGX_RECURSION_MODE_BFS_PROGRESSIVE ) {
        match = _vxquery_traverse__recursive_traverse_neighbor_outarcs_OPEN_RO( vertex_RO, search );
      }
      else {
        // ???
        match = VGX_ARC_FILTER_MATCH_ERROR;
      }
    }

    if( __is_arcfilter_error( match ) ) {
      if( _vgx_is_execution_halted( tb ) ) {
        if( tb->reason == VGX_ACCESS_REASON_EXECUTION_TIMEOUT ) {
          __format_error_string( &search->CSTR__error, "Execution timeout after %d ms", (tb->tt_ms - tb->t0_ms) );
        }
        else {
          const char *timeout = blocking ? "Timeout: " : "";
          const char *neighbor = arcdir == VGX_ARCDIR_OUT ? "terminal" : arcdir == VGX_ARCDIR_IN ? "initial" : "neighbor";
          const void *obj = tb->resource;
          const vgx_Vertex_t *locked = vgx_CheckVertex( obj );
          const char *idprefix = locked ? locked->identifier.idprefix.data : "?";
          __format_error_string( &search->CSTR__error, "%s%s <%s@%p> is locked", timeout, neighbor, idprefix, obj );
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Update search context with collector's counts
    vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)search->probe->common_collector;
    search->n_neighbors = collector->n_neighbors;
    search->n_arcs = collector->n_collectable;
    search->counts_are_deep = collector->counts_are_deep;
    
  }
  XCATCH( errcode ) {
    ret = -1;
    __set_error_string( &search->CSTR__error, "Neighborhood collector error" );
  }
  XFINALLY {
    if( neighborhood ) {
      COMLIB_OBJECT_DESTROY( neighborhood );
    }
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_traverse__traverse_neighbor_vertices_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search ) {
  int ret = 0;

  Cm256iList_t *unique = NULL;

  XTRY {
    vgx_ExecutionTimingBudget_t *tb = search->timing_budget;
    bool blocking = _vgx_is_execution_blocking( tb );

    vgx_ArcFilter_match match;
    vgx_arc_direction arcdir = search->probe->traversing.arcdir;
    const vgx_ArcVector_cell_t *V1 = NULL;
    const vgx_ArcVector_cell_t *V2 = NULL;

    if( arcdir == VGX_ARCDIR_BOTH ) {
      V1 = &vertex_RO->inarcs;
      V2 = &vertex_RO->outarcs;
      match = iarcvector.GetVerticesBidirectional( V1, V2, search->probe );
    }
    else {
      V1 = arcdir == VGX_ARCDIR_IN ? &vertex_RO->inarcs : &vertex_RO->outarcs;
      match = iarcvector.GetVertices( V1, search->probe );
    }

    if( __is_arcfilter_error( match ) ) {
      if( _vgx_is_execution_halted( tb ) ) {
        const char *timeout = blocking ? "Timeout: " : "";
        const char *neighbor = arcdir == VGX_ARCDIR_OUT ? "terminal" : arcdir == VGX_ARCDIR_IN ? "initial" : "neighbor";
        const void *obj = tb->resource;
        const vgx_Vertex_t *locked = vgx_CheckVertex( obj );
        const char *idprefix = locked ? locked->identifier.idprefix.data : "";
        __format_error_string( &search->CSTR__error, "%s%s <%s@%p> is locked", timeout, neighbor, idprefix, obj );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)search->probe->common_collector;
    search->n_neighbors = collector->n_vertices;
    search->n_vertices = collector->n_collectable;
    search->counts_are_deep = collector->counts_are_deep;

  }
  XCATCH( errcode ) {
    ret = -1;
    __set_error_string( &search->CSTR__error, "Unique neighborhood collector error" );
  }
  XFINALLY {
    if( unique ) {
      COMLIB_OBJECT_DESTROY( unique );
    }
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_traverse__traverse_global_items_OPEN( vgx_Graph_t *self, vgx_global_search_context_t *search, bool readonly_graph ) {
  int ret = 0;

  vgx_VertexFilter_context_t *local_vertexfilter_context = NULL;
  Cm256iList_t *vertices = NULL;

  XTRY {
    // ------------------------------------
    // 1: Set up default filter if no probe
    // ------------------------------------
    vgx_VertexFilter_context_t *filter = search->probe ? search->probe->vertexfilter_context : (local_vertexfilter_context = iVertexFilter.New( NULL, search->timing_budget ));
    if( filter == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x3B1 );
    }

    // ---------------------------
    // 2. Execute global collector
    // ---------------------------
    if( (search->n_items = _vxgraph_vxtable__collect_items_OPEN( self, search, readonly_graph, filter )) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x3B7 );
    }

    if( search->collector.mode == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
      search->counts_are_deep = search->collector.arc->counts_are_deep;
    }
    else {
      search->counts_are_deep = search->collector.vertex->counts_are_deep;
    }

  }
  XCATCH( errcode ) {
    ret = -1;
    switch( search->timing_budget->reason ) {
    case VGX_ACCESS_REASON_LOCKED:
    case VGX_ACCESS_REASON_TIMEOUT:
      __format_error_string( &search->CSTR__error, "Timeout during global traversal (0x%x)", search->timing_budget->reason );
      break;
    case VGX_ACCESS_REASON_EXECUTION_TIMEOUT:
      __format_error_string( &search->CSTR__error, "Global search execution timeout after %d ms", (search->timing_budget->tt_ms - search->timing_budget->t0_ms) );
      break;
    case VGX_ACCESS_REASON_READONLY_GRAPH:
    case VGX_ACCESS_REASON_READONLY_PENDING:
      __format_error_string( &search->CSTR__error, "Global traversal readonly conflict (0x%x)", search->timing_budget->reason );
      break;
    case VGX_ACCESS_REASON_SEMAPHORE:
      __set_error_string( &search->CSTR__error, "Global traversal semaphore error" );
      break;
    case VGX_ACCESS_REASON_ERROR:
      __set_error_string( &search->CSTR__error, "Global traversal filter error" );
      break;
    default:
      __format_error_string( &search->CSTR__error, "Global collector error (0x%x)", search->timing_budget->reason );
    }
  }
  XFINALLY {
    if( local_vertexfilter_context ) {
      iVertexFilter.Delete( &local_vertexfilter_context );
    }
    if( vertices ) {
      COMLIB_OBJECT_DESTROY( vertices );
    }
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_traverse__aggregate_neighborhood_OPEN_RO( const vgx_Vertex_t *vertex_RO, vgx_aggregator_search_context_t *search ) {

  int ret = 0;

  XTRY {

    vgx_arc_direction direction = search->probe->traversing.arcdir;
    const vgx_ArcVector_cell_t *arcvector = direction == VGX_ARCDIR_OUT ? &vertex_RO->outarcs : &vertex_RO->inarcs;

    // ----------------------
    // Execute aggregation
    // ----------------------
    if( __is_arcfilter_error( iarcvector.GetArcs( arcvector, search->probe ) ) ) {
      if( _vgx_is_execution_halted( search->timing_budget ) ) {
        const void *obj = search->timing_budget->resource;
        __format_error_string( &search->CSTR__error, "Timeout during aggregation @ %p", obj );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)search->probe->common_collector;
    search->n_neighbors = collector->n_neighbors;
    search->n_arcs = collector->n_collectable;
    search->counts_are_deep = collector->counts_are_deep;

  }
  XCATCH( errcode ) {
    ret = -1;
    __set_error_string( &search->CSTR__error, "Aggregator collector error" );
  }
  XFINALLY {
  }

  return ret;

}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_traverse.h"

test_descriptor_t _vgx_vxquery_traverse_tests[] = {
  { "VGX Graph Traverse Tests", __utest_vxquery_traverse },
  {NULL}
};
#endif
