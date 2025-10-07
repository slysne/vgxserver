/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_api.c
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

#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


/* The API layer */
#define        __api_arcvector_init __arcvector_cell_set_no_arc
#define        __api_arcvector_is_init __arcvector_cell_has_no_arc
#define        __api_arcvector_cell_type __arcvector_cell_type
static int64_t __api_arcvector_degree( const vgx_ArcVector_cell_t *V );
static int     __api_arcvector_add_arc( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, f_Vertex_connect_event connect_event );
static int64_t __api_arcvector_remove_arc( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect );
static int64_t __api_arcvector_expire_arcs( framehash_dynamic_t *dynamic, vgx_Vertex_t *vertex_WL, uint32_t now_ts, uint32_t *next_ts );
#define        __api_arcvector_get_arc_cell _vxarcvector_fhash__get_arc_cell
static vgx_predicator_t __api_arcvector_get_arc_value( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, vgx_ArcHead_t *arc_head );
static vgx_ArcFilter_match __api_arcvector_get_arcs( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
static vgx_ArcFilter_match __api_arcvector_get_arcs_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
static vgx_ArcFilter_match __api_arcvector_get_vertices( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
static vgx_ArcFilter_match __api_arcvector_get_vertices_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
static vgx_ArcFilter_match __api_arcvector_has_arc( const vgx_ArcVector_cell_t *V, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_Arc_t *first_match );
static vgx_ArcFilter_match __api_arcvector_has_arc_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
static int64_t __api_arcvector_serialize( const vgx_ArcVector_cell_t *V, CQwordQueue_t *output );
static int64_t __api_arcvector_deserialize( vgx_Vertex_t *tail, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, vgx_ArcVector_cell_t *V, CQwordQueue_t *input );
static void    __api_arcvector_print_debug_dump( const vgx_ArcVector_cell_t *V, const char *message );


DLL_EXPORT vgx_IArcVector_t iarcvector = {
  .SetNoArc                 = __api_arcvector_init,
  .HasNoArc                 = __api_arcvector_is_init,
  .CellType                 = __api_arcvector_cell_type,
  .Degree                   = __api_arcvector_degree,
  .Add                      = __api_arcvector_add_arc,
  .Remove                   = __api_arcvector_remove_arc,
  .Expire                   = __api_arcvector_expire_arcs,
  .GetArcCell               = __api_arcvector_get_arc_cell,
  .GetArcValue              = __api_arcvector_get_arc_value,
  .GetArcs                  = __api_arcvector_get_arcs,
  .GetArcsBidirectional     = __api_arcvector_get_arcs_bidirectional,
  .GetVertices              = __api_arcvector_get_vertices,
  .GetVerticesBidirectional = __api_arcvector_get_vertices_bidirectional,
  .HasArc                   = __api_arcvector_has_arc,
  .HasArcBidirectional      = __api_arcvector_has_arc_bidirectional,
  .Serialize                = __api_arcvector_serialize,
  .Deserialize              = __api_arcvector_deserialize,
  .PrintDebugDump           = __api_arcvector_print_debug_dump
};



/*******************************************************************//**
 ********** 
 ********    API LAYER
 ********** 
 ***********************************************************************
 */


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __api_arcvector_degree( const vgx_ArcVector_cell_t *V ) {
  switch( TPTR_AS_TAG( &V->VxD ) ) {
  case VGX_ARCVECTOR_VxD_VERTEX:
    switch( TPTR_AS_TAG( &V->FxP ) ) {
    case VGX_ARCVECTOR_FxP_PREDICATOR:
      return 1;
    case VGX_ARCVECTOR_FxP_FRAME:
      return _vxarcvector_cellproc__count_nactive( V );
    default:
      return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
    }
  case VGX_ARCVECTOR_VxD_EMPTY:
    return 0;
  case VGX_ARCVECTOR_VxD_DEGREE:
    return __arcvector_get_degree( V );
  default:
    return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __api_arcvector_add_arc( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, f_Vertex_connect_event connect_event ) {
  int n_inserted = -1;

  //
  if( arc->head.predicator.rel.enc != VGX_PREDICATOR_REL_NONE ) {
    if( arc->tail && arc->head.vertex ) {

      bool outarc;

      // Select the vertex arcvector (OUT or IN) based on arc's direction
      vgx_ArcVector_cell_t *V;
      switch( __predicator_direction( arc->head.predicator ) ) {
      case VGX_ARCDIR_OUT:
        V = &arc->tail->outarcs;
        outarc = true;
        break;
      case VGX_ARCDIR_IN:
        V = &arc->tail->inarcs;
        outarc = false;
        break;
      default:
        return __ARCVECTOR_ERROR( arc, NULL, NULL, NULL );
      }
      
#ifndef NDEBUG
      vgx_Vertex_t *tail = arc->tail;
      int64_t refcnt_pre = Vertex_REFCNT_WL( tail );
#endif

      switch( __arcvector_cell_type( V ) ) {
      // Insert as simple arc
      case VGX_ARCVECTOR_NO_ARCS:
        // Should be exactly one, otherwise failure. (we had no previous arc, so we're never overwriting)
        if( connect_event( dynamic, arc, 1 ) == 1 ) {
          // Convert cell to simple arc
          __arcvector_cell_set_simple_arc( V, arc );
          // set OUT
          if( outarc ) {
            __vertex_set_has_outarcs( arc->tail );
          }
          // set IN
          else {
            __vertex_set_has_inarcs( arc->tail );
          }
          n_inserted = 1;
        }
        break;

      // Convert to array of arcs, then insert
      case VGX_ARCVECTOR_SIMPLE_ARC:
        // Existing arc
        if( __arcvector_vertex_match( V, arc->head.vertex ) && __arcvector_predicator_match( V, arc->head.predicator ) ) {
          if( connect_event( dynamic, arc, 0 ) == 0 ) {
            vgx_predicator_t pred = {.data = __arcvector_as_predicator_bits( V )};
            vgx_Arc_t updated = {
              .tail = arc->tail,
              .head = {
                .vertex     = arc->head.vertex,
                .predicator = _vgx_update_predicator_value_if_accumulator( pred, arc->head.predicator ),
              }
            };
            __arcvector_cell_set_simple_arc( V, &updated );
            n_inserted = 0; // updated
          }
          break;
        }
        // New arc - convert then fall thru to next case
        else if( _vxarcvector_fhash__convert_simple_arc_to_array_of_arcs( dynamic, V ) < 0 ) {
          break; // error
        }
        /* FALLTHRU */

      // Insert into array of arcs
      case VGX_ARCVECTOR_ARRAY_OF_ARCS:
        n_inserted = _vxarcvector_dispatch__array_add( dynamic, V, arc, connect_event );
        break;

      // Invalid for add
      case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
        FATAL( 0xFFF, "%s: Invalid arcvector context: indegree counter only", __FUNCTION__ );
        /* FALLTHRU */

      // ERROR
      default:
        break;
      }

      // Increment graph size
      if( n_inserted == 1 ) {
        vgx_Graph_t *graph = arc->tail->graph;
        if( outarc ) {
          IncGraphSize( graph );
#ifdef NDEBUG
        }
#else
          int64_t refcnt_post = Vertex_REFCNT_WL( tail );
          int64_t expected_refdelta = n_inserted;
          if( tail == arc->head.vertex ) {
            expected_refdelta++;
          }
          if( refcnt_post - refcnt_pre != expected_refdelta ) {
            PRINT_VERTEX( tail );
            iarcvector.PrintDebugDump( &tail->outarcs, "Outarcs" );
            iarcvector.PrintDebugDump( &tail->inarcs, "Inarcs" );
            FATAL( 0xFFF, "%s: Bad refcount after insert: refcnt pre/post:%lld/%lld, n_inserted:%lld", __FUNCTION__, refcnt_pre, refcnt_post, n_inserted );
          }
        }
        // Reverse arc
        else {
          IncGraphRevSize( graph );
        }
#endif
      }
    }
  }
  else {
    n_inserted = 0;
  } 

  return n_inserted;
}



#ifdef VGX_CONSISTENCY_CHECK

/**************************************************************************//**
 * __assert_inarcs_stable
 *
 ******************************************************************************
 */
static void __assert_inarcs_stable( vgx_Vertex_t *vertex ) {
  // WL must either have no yielded inarcs or they must be claimed busy by owner thread
  if( __vertex_is_locked_writable_by_current_thread( vertex ) ) {
    if( __vertex_is_inarcs_yielded( vertex ) ) {
      if( !__vertex_is_borrowed_inarcs_busy( vertex ) ) {
        PRINT_VERTEX( vertex );
        FATAL( 0xF0A, "Invalid vertex lock state: WL but inarcs yielded and not busy" );
      }
    }
  }
  // iWL
  else {
    if( !__vertex_is_inarcs_yielded( vertex ) || !__vertex_is_borrowed_inarcs_busy( vertex ) ) {
      PRINT_VERTEX( vertex );
      FATAL( 0xF0B, "Invalid vertex lock state: iWL but inarcs not yielded nor not busy" );
    }
  }
}
#else
#define __assert_inarcs_stable( vertex ) ((void)0)
#endif



/*******************************************************************//**
 * 
 * Returns: 
 *           >= 0 : Number of arcs removed. 
 *           < 0  : Timing budget should be inspected for timeout/reason
 * 
 ***********************************************************************
 */
static int64_t __api_arcvector_remove_arc( framehash_dynamic_t *dynamic, vgx_Arc_t *probe, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event ) {
  int64_t n_removed = 0;

  // Select the vertex arcvector (OUT or IN) based on probe's direction
  vgx_ArcVector_cell_t *V;
  vgx_Vertex_t *vertex = probe->tail;
  vgx_Graph_t *graph = vertex->graph;
  int del = 0;

  bool outarc;

  // Remove outarc(s)
  if( __predicator_direction( probe->head.predicator ) == VGX_ARCDIR_OUT ) {
    V = &vertex->outarcs;
    outarc = true;
  }
  // Remove inarc(s)
  else {
    V = &vertex->inarcs;
    outarc = false;
  }

  __assert_inarcs_stable( vertex );

  switch( __arcvector_cell_type( V ) ) {

  // No arcs exist, so no arcs removed
  case VGX_ARCVECTOR_NO_ARCS:
    break;

  // Simple arc exists, remove it if match
  case VGX_ARCVECTOR_SIMPLE_ARC:
    del = _vxarcvector_delete__delete_simple_arc( dynamic, V, probe, timing_budget, disconnect_event, &n_removed );
    break;
  
  // Remove matching arc from array of arcs
  case VGX_ARCVECTOR_ARRAY_OF_ARCS:
    // Disconnect
    del = _vxarcvector_dispatch__array_remove( dynamic, V, probe, timing_budget, disconnect_event, &n_removed );
    break;

  // Indegree Counter - no action
  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    break;

  default:
    break;

  }

  // Clear the vertex arc bit if no arcs left
  if( __arcvector_cell_has_no_arc( V ) ) {
    // clear OUT
    if( outarc ) {
      __vertex_clear_has_outarcs( vertex );
    }
    // clear IN
    else {
      __vertex_clear_has_inarcs( vertex );
    }
  }

  // Decrement graph size
  if( n_removed > 0 ) {
    if( outarc ) {
      if( SubGraphSize( graph, n_removed ) < 0 ) {
        FATAL( 0xFFF, "Negative graph size after arc removal" );
      }
#ifdef NDEBUG
    }
#else
    }
    else {
      if( SubGraphRevSize( graph, n_removed ) < 0 ) {
        FATAL( 0xFFF, "graph size seems to have gone negative!!" );
      }
    }

#endif
  }

  // Change reason to arc error in this case
  if( del < 0 && __is_access_reason_transient( timing_budget->reason ) ) {
    timing_budget->reason = VGX_ACCESS_REASON_VERTEX_ARC_ERROR;
    return -1;
  }

  return n_removed;
}



/*******************************************************************//**
 * 
 * We will have to scan the arcvector to find all relationships that include a M_TMX arc with a value less than the supplied now_ts timestamp.
 * Those relationships will be deleted. At the same time we will inspect all M_TMX timestamps that may exist for any non-deleted relationships
 * and keep a record of the SMALLEST of those timestamps. When the operation completes we place that timestamp into the output next_ts. This
 * becomes the timestamp for the next expiration event. If no remaining arcs have M_TMX timestamp this value will not be set.
 * 
 * Return: Number of expired arcs (NOTE: one or more arcs per relationship, we return the individual arc count)
 * Output: *next_ts is set to the timestamp of the next relationship that expires, if any.
 ***********************************************************************
 */
static int64_t __api_arcvector_expire_arcs( framehash_dynamic_t *dynamic, vgx_Vertex_t *vertex_WL, uint32_t now_ts, uint32_t *next_ts ) {

  int64_t n_expired = 0;
  vgx_ArcVector_cell_t *V = &vertex_WL->outarcs;
  vgx_predicator_t pred;
  uint32_t arc_min_tmx = TIME_EXPIRES_NEVER;

  switch( __arcvector_cell_type( V ) ) {

  // No arcs exist, so no arcs removed
  case VGX_ARCVECTOR_NO_ARCS:
    break;

  // Simple arc, remove if expired
  case VGX_ARCVECTOR_SIMPLE_ARC:
    // Does our simple arc have a M_TMX modifier and is it expired?
    if( _vgx_predicator_is_expired( (pred = __arcvector_as_predicator( V )), now_ts ) ) {
      // Disconnect reverse
      vgx_Arc_t del_arc = {
        .tail = vertex_WL,
        .head = {
          .vertex     = __arcvector_get_vertex( V ),
          .predicator = pred
        }
      };
      // Try disconnect reverse first (NON-BLOCKING!)
      vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
      if( (n_expired = _vxgraph_arc__disconnect_WL_reverse_GENERAL( dynamic, &del_arc, 1, &zero_timeout )) == 1 ) {
        // Ok, remove this arc
        __arcvector_cell_set_no_arc( V );
      }
      // Could not acquire head vertex inarcs immediately, so we defer expiration until a future time
      else if( n_expired == 0 ) {
        arc_min_tmx = pred.val.uinteger; // reschedule for immediate retry
      }
      else {
        CRITICAL( 0x211, "Disconnect error during arc expiration: expected exactly 1 arc, n_removed=%lld", n_expired );
        // disconnect error: should be exactly 1 (the one we matched above)
        n_expired = __ARCVECTOR_ERROR( &del_arc, vertex_WL, V, NULL );
      }
    }
    break;
  
  // Remove matching arc from array of arcs
  case VGX_ARCVECTOR_ARRAY_OF_ARCS:
    _vxarcvector_expire__expire_arcs( dynamic, V, vertex_WL, now_ts, &arc_min_tmx, &n_expired );
    break;

  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    FATAL( 0xFFF, "TODO: IMPLEMENT" );
    break;

  default:
    break;

  }

  // Clear the vertex outarcs bit if no arcs left
  if( __arcvector_cell_has_no_arc( V ) ) {
    __vertex_clear_has_outarcs( vertex_WL );
  }

  // Decrement graph size
  if( n_expired > 0 ) {
    vgx_Graph_t *graph = vertex_WL->graph;
    if( SubGraphSize( graph, n_expired ) < 0 ) {
      FATAL( 0xFFF, "Negative graph size after arc expiration" );
    }
  }

  // Record the earliest arc expiration timestamp remaining and return this value if not never
  if( (vertex_WL->TMX.arc_ts = arc_min_tmx) < TIME_EXPIRES_NEVER ) {
    *next_ts = arc_min_tmx;
  }

  return n_expired;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_predicator_t __api_arcvector_get_arc_value( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, vgx_ArcHead_t *arc_head ) {
  vgx_ArcHead_t match = {
    .vertex     = NULL,
    .predicator = VGX_PREDICATOR_NONE
  };

  switch( __arcvector_cell_type( V ) ) {

  // WHITE: Empty
  case VGX_ARCVECTOR_NO_ARCS:
    break;

  // BLUE: Simple arc
  case VGX_ARCVECTOR_SIMPLE_ARC:
    if( __arcvector_get_vertex( V ) == arc_head->vertex ) {
      vgx_predicator_t pred = __arcvector_as_predicator( V );
      if( predmatchfunc.Generic( arc_head->predicator, pred ) ) {
        match.predicator.data = pred.data;
        match.vertex = arc_head->vertex;
      }
    }
    break;

  // GREEN: Array of arcs
  case VGX_ARCVECTOR_ARRAY_OF_ARCS:
    _vxarcvector_dispatch__get_arc( dynamic, V, arc_head, &match );
    break;

  // GRAY: Indegree Counter
  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    break;

  default:
    break;
  }

  return match.predicator;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __set_rank_evaluator_context( vgx_neighborhood_probe_t *probe ) {
  vgx_BaseCollector_context_t *collector = probe->common_collector;
  vgx_Evaluator_t *R;
  if( collector && collector->ranker && (R = collector->ranker->evaluator) != NULL ) {
    vgx_virtual_ArcFilter_context_t *traverse_filter = probe->traversing.arcfilter;
    const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
    CALLABLE( R )->SetContext( R, previous->current_tail, previous->current_head, NULL, 0.0 );
  }
}



/*******************************************************************//**
 * 
 * 
 * Get all arcs as aptr_t (predicator, vertex), including flattened multiple arcs
 * Returns:
 *        1 : filter(s) matched, number of arcs collected incremented in the collector instance
 *        0 : filter(s) did not match, nothing collected
 *       -1 : error
 ***********************************************************************
 */
static vgx_ArcFilter_match __api_arcvector_get_arcs( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe ) {
  _vgx_ArcVector_cell_type ctype = __arcvector_cell_type( V );

  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;
  
  // WHITE: Empty
  // GRAY: Indegree Counter
  if( ctype == VGX_ARCVECTOR_NO_ARCS || ctype == VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
    return __arcfilter_MISS( recursive );
  }

  // Context: Reference tail and arc leading us here if any, otherwise "tail"="this" and current head is NULL
  vgx_virtual_ArcFilter_context_t *traverse_filter = recursive->arcfilter;
  if( (traverse_filter->type & __VGX_ARC_FILTER_TYPE_MASK_REQUIRE_TAIL) && traverse_filter->current_tail == NULL ) {
    return __arcfilter_error();
  }

  // Set RANK evaluator context
  __set_rank_evaluator_context( neighborhood_probe );

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;

  // BLUE: Simple arc
  if( ctype == VGX_ARCVECTOR_SIMPLE_ARC ) {
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    vgx_ArcHead_t archead = __arcvector_init_archead_from_cell( V );
    __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_SIMPLE_ARC, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, traverse_filter->timing_budget, &filter_match ) {
      const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
      _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance ); // set distance from anchor
      vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
      __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
        __pre_arc_visit( traverse_filter, &LARC, &filter_match ) {
          __begin_arcvector_filter_accept_arc( traverse_filter, &LARC, &filter_match ) {
            if( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
              vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)neighborhood_probe->common_collector;
              vgx_virtual_ArcFilter_context_t *collect_filter = neighborhood_probe->collect_filter_context;
              vgx_ArcFilter_match collect_match = VGX_ARC_FILTER_MATCH_MISS;
              __begin_arcvector_filter_collect_arc( collect_filter, &LARC, &collect_match ) {
                if( __arcvector_collect_arc( collector, &LARC, 0.0, NULL ) < 0 ) {
                  collect_match = __arcfilter_error();
                }
              } __end_arcvector_filter_collect_arc;
              if( __is_arcfilter_error( collect_match ) ) {
                filter_match = __arcfilter_error();
              }
              collector->n_neighbors++;
            }
          } __end_arcvector_filter_accept_arc;
        } __post_arc_visit( true );

      } __end_arc_evaluator_context;
    } __end_lockable_arc_context;
    return __arcfilter_THRU( recursive, filter_match );
  }
  // GREEN: Array of arcs
  else {
    return _vxarcvector_traverse__traverse_arcarray( V, neighborhood_probe );
  }
}



/*******************************************************************//**
 * 
 * 
 * Get all arcs as aptr_t (predicator, vertex), including flattened multiple arcs,
 * with a bidirectional filter. Both inarcs and outarcs arcvectors are
 * required for this filter test.
 ***********************************************************************
 */
static vgx_ArcFilter_match __api_arcvector_get_arcs_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe ) {

  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  
  int64_t indegree = __api_arcvector_degree( V_IN );
  int64_t outdegree = __api_arcvector_degree( V_OUT );
  vgx_virtual_ArcFilter_context_t *arcfilter_V2 = NULL;
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;

  // Both arcvectors are populated
  if( indegree && outdegree ) {

    // Select the shortest arcvector for linear traversal
    const vgx_ArcVector_cell_t *V1 = indegree > outdegree ? V_IN : V_OUT;
    const vgx_ArcVector_cell_t *V2 = indegree > outdegree ? V_OUT: V_IN;

    // Context: Reference tail and arc leading us here if any, otherwise "tail"="this" and current head is NULL
    vgx_virtual_ArcFilter_context_t *traverse_filter = recursive->arcfilter;
    if( (traverse_filter->type & __VGX_ARC_FILTER_TYPE_MASK_REQUIRE_TAIL) && traverse_filter->current_tail == NULL ) {
      return __arcfilter_error();
    }
  
    // Set RANK evaluator context
    __set_rank_evaluator_context( neighborhood_probe );

    // Readonly ?
    bool readonly = neighborhood_probe->readonly_graph;

    switch( __arcvector_cell_type( V1 ) ) {

    // WHITE: First arcvector Empty
    case VGX_ARCVECTOR_NO_ARCS:
      break;

    // BLUE: First arcvector Simple arc
    case VGX_ARCVECTOR_SIMPLE_ARC:
      {
        vgx_ArcHead_t archead = __arcvector_init_archead_from_cell( V1 );
        __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_SIMPLE_ARC, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, traverse_filter->timing_budget, &filter_match ) {
          const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
          _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance ); // set distance from anchor
          vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
          __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
            __pre_arc_visit( traverse_filter, &LARC, &filter_match ) {
              // Filter condition1 : arc must match the (only) entry in the first arcvector
              __begin_arcvector_filter_accept_arc( traverse_filter, &LARC, &filter_match ) {
                // Filter condition 2: and also exist in the second arcvector
                // Create a separate arc filter context for the second arcvector test so we can specify the exact head vertex
                if( (arcfilter_V2 = iArcFilter.Clone( traverse_filter )) == NULL ) {
                  filter_match = __arcfilter_error();
                }
                else {
                  vgx_GenericArcFilter_context_t *generic_filter_V2 = (vgx_GenericArcFilter_context_t*)arcfilter_V2;
                  // HEAD: the opposing V2 arc has to terminate at the same vertex as the V1 arc
                  generic_filter_V2->terminal.current = LARC.head.vertex;
                  generic_filter_V2->terminal.logic = VGX_LOGICAL_AND;
                  // PREDICATOR CONDITION: the opposing V2 relationship and modifier should match the V1 arc
                  generic_filter_V2->pred_condition1 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition1, LARC.head.predicator );
                  generic_filter_V2->pred_condition2 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition2, LARC.head.predicator );
                  __arcvector_probe_push_conditional( neighborhood_probe, arcfilter_V2, recursive->vertex_probe, recursive->arcdir, recursive->evaluator ) {
                    vgx_Arc_t hasarc2 = {0};
                    if( (filter_match = __api_arcvector_has_arc( V2, recursive, neighborhood_probe, &hasarc2 )) == VGX_ARC_FILTER_MATCH_HIT ) {
                      _vgx_arc_set_distance( &hasarc2, neighborhood_probe->distance ); // set distance from anchor
                      switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
                      case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
                        break;
                      case VGX_COLLECTOR_MODE_COLLECT_ARCS:
                        {
                          // TODO: Support different collect_arc and sortby_arc
                          vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)neighborhood_probe->common_collector;
                          // !!! WARNING !!!
                          // The collect filter does not support the bi-directional condition.
                          // Exact predicator data match: Collapse fwd and rec arcs to a single bi-directional arc 
                          if( _vgx_predicator_data_match( hasarc2.head.predicator, LARC.head.predicator ) ) {
                            LARC.head.predicator.rel.dir = VGX_ARCDIR_BOTH;
                            if( __arcvector_collect_arc( collector, &LARC, 0.0, NULL ) < 0 ) {
                              filter_match = __arcfilter_error();
                            }
                            else {
                              collector->n_neighbors++;
                            }
                          }
                          // Fw/rev arcs have different data, collect individually
                          else {
                            if( __arcvector_collect_arc( collector, &LARC, 0.0, NULL ) < 0 ) {
                              filter_match = __arcfilter_error();
                            }
                            else {
                              collector->n_neighbors++;
                              if( collector->n_remain > 0 ) {
                                LARC.head.predicator = hasarc2.head.predicator;
                                if( __arcvector_collect_arc( collector, &LARC, 0.0, NULL ) < 0 ) {
                                  filter_match = __arcfilter_error();
                                }
                              }
                            }
                          }
                        }
                        break;
                      default:
                        break;
                      }
                    }
                  } __arcvector_probe_pop_conditional;
                }
              } __end_arcvector_filter_accept_arc;
            } __post_arc_visit( true );
          } __end_arc_evaluator_context;
        } __end_lockable_arc_context;
      }
      break;

    // GREEN: First arcvector Array of arcs
    case VGX_ARCVECTOR_ARRAY_OF_ARCS:
      filter_match = _vxarcvector_traverse__traverse_arcarray_bidirectional( V1, V2, neighborhood_probe );
      break;

    // GRAY: First arcvector Indegree Counter
    case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
      break;

    default:
      filter_match = __arcfilter_error();
      break;
    }
    // Clean up
    iArcFilter.Delete( &arcfilter_V2 );

    return __arcfilter_THRU( recursive, filter_match );
  }
  else {
    return __arcfilter_MISS( recursive );
  }


}



/*******************************************************************//**
 * 
 * 
 * Get all vertices at end of arcs, so a multiple arc results in a single vertex. 
 ***********************************************************************
 */
static vgx_ArcFilter_match __api_arcvector_get_vertices( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe ) {
  _vgx_ArcVector_cell_type ctype = __arcvector_cell_type( V );
  
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;

  // WHITE: Empty
  // GRAY: Indegree Counter
  if( ctype == VGX_ARCVECTOR_NO_ARCS || ctype == VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
    return __arcfilter_MISS( recursive );
  }

  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;

  // Context: Reference tail and arc leading us here if any, otherwise "tail"="this" and current head is NULL
  vgx_virtual_ArcFilter_context_t *traverse_filter = recursive->arcfilter;

  // Set RANK evaluator context
  __set_rank_evaluator_context( neighborhood_probe );

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;
  
  // BLUE: Simple arc
  if( ctype == VGX_ARCVECTOR_SIMPLE_ARC ) {
    vgx_ArcHead_t archead = __arcvector_init_archead_from_cell( V );
    __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_SIMPLE_ARC, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, traverse_filter->timing_budget, &filter_match ) {
      // NOTE: this function returns a vgx_Arc_t *, NULL on miss, non-NULL on hit
      const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
      _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance ); // set distance from anchor
      vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
      __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
        __begin_arcvector_filter_accept_arc( traverse_filter, &LARC, &filter_match ) {
          switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
          case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
            break;
          case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
            {
              vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)neighborhood_probe->common_collector;
              if( __arcvector_collect_vertex( collector, &LARC, NULL ) < 0 ) {
                filter_match = __arcfilter_error();
              }
            }
            break;
          default:
            break;
          }
        } __end_arcvector_filter_accept_arc;
      } __end_arc_evaluator_context;
    } __end_lockable_arc_context;
    return __arcfilter_THRU( recursive, filter_match );
  }
  // GREEN: Array of arcs
  else if( ctype == VGX_ARCVECTOR_ARRAY_OF_ARCS ) {
    __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, VGX_PREDICATOR_NONE, neighborhood_probe->current_tail_RO, traverse_filter->timing_budget, &filter_match ) {
      const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
      vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
      __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
        filter_match = _vxarcvector_cellproc__collect_vertices( V, neighborhood_probe );
      } __end_arc_evaluator_context;
    } __end_lockable_arc_context;
    return __arcfilter_THRU( recursive, filter_match );
  }
  // ???
  else {
    return __arcfilter_error();
  }
}



/*******************************************************************//**
 * 
 * 
 * Get all vertices at end of arcs, so a multiple arc results in a single vertex, 
 * with a bidirectional filter applied. Both inarcs and outarcs arcvectors are
 * required for this filter test.
 ***********************************************************************
 */

static vgx_ArcFilter_match __api_arcvector_get_vertices_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe ) {

  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  
  int64_t indegree = __api_arcvector_degree( V_IN );
  int64_t outdegree = __api_arcvector_degree( V_OUT );
  vgx_virtual_ArcFilter_context_t *arcfilter_V2 = NULL;
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;

  // Both arcvectors are populated
  if( indegree && outdegree ) {
    // Context: Reference tail and arc leading us here if any, otherwise "tail"="this" and current head is NULL
    vgx_virtual_ArcFilter_context_t *traverse_filter = recursive->arcfilter;

    // Set RANK evaluator context
    __set_rank_evaluator_context( neighborhood_probe );

    // Select the shortest arcvector for linear traversal
    const vgx_ArcVector_cell_t *V1 = indegree > outdegree ? V_IN : V_OUT;
    const vgx_ArcVector_cell_t *V2 = indegree > outdegree ? V_OUT: V_IN;

    // Readonly ?
    bool readonly = neighborhood_probe->readonly_graph;

    switch( __arcvector_cell_type( V1 ) ) {

    // WHITE: First arcvector Empty
    case VGX_ARCVECTOR_NO_ARCS:
      break;

    // BLUE: First arcvector Simple arc
    case VGX_ARCVECTOR_SIMPLE_ARC:
      {
        vgx_ArcHead_t archead = __arcvector_init_archead_from_cell( V1 );
        __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_SIMPLE_ARC, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, traverse_filter->timing_budget, &filter_match ) {
          const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
          // Filter condition1 : arc must match the (only) entry in the first arcvector
          _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance ); // set distance from anchor
          vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
          __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
            __begin_arcvector_filter_accept_arc( traverse_filter, &LARC, &filter_match ) {
              // Filter condition 2: and also exist in the second arcvector
              // Create a separate arc filter context for the second arcvector test so we can specify the exact head vertex
              if( (arcfilter_V2 = iArcFilter.Clone( recursive->arcfilter )) == NULL ) {
                filter_match = __arcfilter_error();
              }
              else {
                vgx_GenericArcFilter_context_t *generic_filter_V2 = (vgx_GenericArcFilter_context_t*)arcfilter_V2;
                // HEAD: the opposing V2 arc has to terminate at the same vertex as the V1 arc
                generic_filter_V2->terminal.current = LARC.head.vertex;
                generic_filter_V2->terminal.logic = VGX_LOGICAL_AND;
                // PREDICATOR CONDITION: the opposing V2 relationship and modifier should match the V1 arc
                generic_filter_V2->pred_condition1 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition1, LARC.head.predicator );
                generic_filter_V2->pred_condition2 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition2, LARC.head.predicator );
                __arcvector_probe_push_conditional( neighborhood_probe, arcfilter_V2, recursive->vertex_probe, recursive->arcdir, recursive->evaluator ) {
                  vgx_Arc_t hasarc2 = {0};
                  if( (filter_match = __api_arcvector_has_arc( V2, recursive, neighborhood_probe, &hasarc2 )) == VGX_ARC_FILTER_MATCH_HIT ) {
                    switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
                    case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
                      break;
                    case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
                      {
                        vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)neighborhood_probe->common_collector;
                        if( __arcvector_collect_vertex( collector, &LARC, NULL ) < 0 ) {
                          filter_match = __arcfilter_error();
                        }
                      }
                      break;
                    default:
                      break;
                    }
                  }
                } __arcvector_probe_pop_conditional;
              }
            } __end_arcvector_filter_accept_arc;
          } __end_arc_evaluator_context;
        } __end_lockable_arc_context;
      }
      break;

    // GREEN: First arcvector Array of arcs
    case VGX_ARCVECTOR_ARRAY_OF_ARCS:
      __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, VGX_PREDICATOR_NONE, neighborhood_probe->current_tail_RO, traverse_filter->timing_budget, &filter_match ) {
        const vgx_virtual_ArcFilter_context_t *previous = traverse_filter->previous_context ? traverse_filter->previous_context : traverse_filter;
        vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
        __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
          filter_match = _vxarcvector_cellproc__collect_vertices_bidirectional( V1, V2, neighborhood_probe );
        } __end_arc_evaluator_context;
      } __end_lockable_arc_context;
      break;

    // GRAY: Indegree Counter
    case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
      break;

    default:
      filter_match = __arcfilter_error();
      break;
    }

    // Clean up
    iArcFilter.Delete( &arcfilter_V2 );
  }

  return __arcfilter_THRU( recursive, filter_match );

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_ArcFilter_match __simple_arc_match( vgx_ArcHead_t *archead, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_ArcFilter_match *filter_match, vgx_Arc_t *first_match ) {
  vgx_virtual_ArcFilter_context_t *arcfilter = recursive->arcfilter;
  __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_SIMPLE_ARC, neighborhood_probe->readonly_graph, neighborhood_probe->current_tail_RO, archead->predicator, archead->vertex, arcfilter->timing_budget, filter_match ) {
    // NOTE: this function returns a vgx_Arc_t *, NULL on miss, non-NULL on hit
    _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance ); // set distance from anchor
    const vgx_virtual_ArcFilter_context_t *previous = arcfilter->previous_context ? arcfilter->previous_context : arcfilter;
    vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
    __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, filter_match ) {
      __pre_arc_visit( arcfilter, &LARC, filter_match ) {
        __begin_arcvector_filter_accept_arc( arcfilter, &LARC, filter_match ) {
          VGX_COPY_ARC( first_match, (vgx_Arc_t*)&LARC );
        } __end_arcvector_filter_accept_arc;
      } __post_arc_visit( *filter_match == VGX_ARC_FILTER_MATCH_MISS ); // Only allow synthetic arc (i.e. process again with synthetic predicator) if arc filter was miss on normal predicator
    } __end_arc_evaluator_context;
  } __end_lockable_arc_context;

  *filter_match = __arcfilter_THRU( recursive, *filter_match );
  return *filter_match;
}



/*******************************************************************//**
 * 
 * 
 * -1  : error
 *  0  : miss
 *  1  : hit
 ***********************************************************************
 */
static vgx_ArcFilter_match __api_arcvector_has_arc( const vgx_ArcVector_cell_t *V, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_Arc_t *first_match ) {
  _vgx_ArcVector_cell_type ctype = __arcvector_cell_type( V );
  
  // WHITE: No arcs
  // GRAY: Indegree Counter
  if( ctype == VGX_ARCVECTOR_NO_ARCS || ctype == VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
    return __arcfilter_MISS( recursive );
  }
  // GREEN: Array of arcs
  else if( ctype == VGX_ARCVECTOR_ARRAY_OF_ARCS ) {
    return _vxarcvector_exists__has_arc( V, recursive, neighborhood_probe, first_match ); // first_match gets populated if hit
  }

  // BLUE: Simple arc
  else if( ctype == VGX_ARCVECTOR_SIMPLE_ARC ) {
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    vgx_virtual_ArcFilter_context_t *arcfilter_context = recursive->arcfilter;
    vgx_GenericArcFilter_context_t *generic_arcfilter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
    vgx_ArcHead_t archead = __arcvector_init_archead_from_cell( V );

    // We are looking for the existence of an arc to a specific terminal
    if( arcfilter_context->type != VGX_ARC_FILTER_TYPE_PASS && generic_arcfilter->terminal.logic != VGX_LOGICAL_NO_LOGIC ) {
      vgx_ArcFilterTerminal_t *terminal = &generic_arcfilter->terminal;

      // Single specific vertex probe against simple arc
      if( terminal->current ) {
        // Terminal match, proceed with filter
        if( archead.vertex == terminal->current ) {
          return __simple_arc_match( &archead, recursive, neighborhood_probe, &filter_match, first_match );
        }
        // Terminal miss
        else {
          return __arcfilter_MISS( recursive );
        }
      }
      // Multiple specific vertex probes against simple arc
      else if( terminal->list ) {
        const vgx_Vertex_t **list = terminal->list;
        const vgx_Vertex_t **cursor = list;
        const vgx_Vertex_t *probe_vertex;
        while( cursor && (probe_vertex = *cursor++) != NULL ) {
          // Reset match for each iteration
          filter_match = VGX_ARC_FILTER_MATCH_MISS;
          // Terminal match, proceed with filter
          if( archead.vertex == probe_vertex ) {
            __simple_arc_match( &archead, recursive, neighborhood_probe, &filter_match, first_match );
          }
          switch( terminal->logic ) {
          case VGX_LOGICAL_AND:
            // Terminate on first non-hit
            if( filter_match != VGX_ARC_FILTER_MATCH_HIT ) {
              return __arcfilter_THRU( recursive, filter_match ); // miss or error
            }
            break; // hit, continue
          case VGX_LOGICAL_OR:
            // Terminate on first non-miss
            if( filter_match != VGX_ARC_FILTER_MATCH_MISS ) {
              return __arcfilter_THRU( recursive, filter_match ); // hit or error
            }
            break; // miss, continue
          // TODO: Support XOR logic ?
          default:
            return __arcfilter_error();
          }
        }
        return __arcfilter_THRU( recursive, filter_match );
      }
      else {
        return __arcfilter_error();
      }
    }
    // Generic case
    else {
      return __simple_arc_match( &archead, recursive, neighborhood_probe, &filter_match, first_match );
    }
  }
  // Bad arcvector
  else {
    return __arcfilter_error();
  }

}



/*******************************************************************//**
 *  This is a lazy version of the bidirectional arc test. A more efficient
 *  implementation should be made. TODO: Implement more efficient version.
 * 
 * -1  : error
 *  0  : miss
 *  1  : hit
 ***********************************************************************
 */
static vgx_ArcFilter_match __api_arcvector_has_arc_bidirectional( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe ) {
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;

  vgx_collector_mode_t orig_mode = neighborhood_probe->collector_mode;
  vgx_virtual_ArcFilter_context_t *orig_collect_filter = neighborhood_probe->collect_filter_context;
  vgx_BaseCollector_context_t *orig_collector = neighborhood_probe->common_collector;

  neighborhood_probe->collector_mode = VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST;
  neighborhood_probe->collect_filter_context = NULL;
  neighborhood_probe->common_collector = NULL;

  // Context: Reference tail and arc leading us here if any, otherwise "tail"="this" and current head is NULL

  match = __api_arcvector_get_arcs_bidirectional( V_IN, V_OUT, neighborhood_probe );

  neighborhood_probe->collector_mode = orig_mode;
  neighborhood_probe->collect_filter_context = orig_collect_filter;
  neighborhood_probe->common_collector = orig_collector;

  return match;
}



/*******************************************************************//**
 * 
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __api_arcvector_serialize( const vgx_ArcVector_cell_t *V, CQwordQueue_t *output ) {

  return _vxarcvector_serialization__serialize( V, output );

}



/*******************************************************************//**
 * 
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __api_arcvector_deserialize( vgx_Vertex_t *tail, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, vgx_ArcVector_cell_t *V, CQwordQueue_t *input ) {

  return _vxarcvector_serialization__deserialize( tail, V, dynamic, vertex_allocator, input ); 

}



/*******************************************************************//**
 * 
 * 
 * 
 * 
 ***********************************************************************
 */
static void __api_arcvector_print_debug_dump( const vgx_ArcVector_cell_t *V, const char *message ) {

  CQwordQueue_t *dump = NULL;

  XTRY {
    if( (dump = COMLIB_OBJECT_NEW_DEFAULT( CQwordQueue_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x241 );
    }

    if( iarcvector.Serialize( V, dump ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x242 );
    }

    CQwordQueue_vtable_t *idump = CALLABLE( dump );
    QWORD qword; 
    int64_t n = idump->Length( dump );
    printf( "\n----------------------------------------------------------\n" );
    if( message ) {
      printf( "%s\n", message );
    }
    printf( "deg=%lld\n", iarcvector.Degree( V ) );
    for( int64_t i=0; i<n; i++ ) {
      idump->GetNolock( dump, i, &qword );
      printf( "%016llX ", qword );
    }
    printf( "\n----------------------------------------------------------\n" );
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( dump ) {
      COMLIB_OBJECT_DESTROY( dump );
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static char * __api_arcvector_dump_raw_vertex( vgx_Vertex_t *vertex, char *buffer ) {
  char *wp = buffer;
  if( vertex ) {
    cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex );
    QWORD *vtxqw = (QWORD*)linehead;
    int nqwords = (int)qwsizeof( vgx_AllocatedVertex_t );
    vgx_Operation_t *op = &vertex->operation;
    vgx_VertexDescriptor_t *desc = &vertex->descriptor;
    wp += sprintf( wp, "refc=%d bidx=%u offset=%u op=%lld(%d) wt=%u sus=%u lck=%u rwl=%u yib=%u iny=%u data=[", linehead->data.refc, linehead->data.bidx, linehead->data.offset, iOperation.GetId_LCK( op ), iOperation.IsDirty( op ), (uint32_t)desc->writer.threadid, desc->state.context.sus, desc->state.lock.lck, desc->state.lock.rwl, desc->state.lock.yib, desc->state.lock.iny );
    for( int i=0; i<nqwords; i++ ) {
      wp += sprintf( wp, "%016llx ", *vtxqw++ );
    }
    *(wp-1) = ']';
  }
  else {
    wp += sprintf( wp, "null" );
  }
  *wp = '\0';
  return buffer;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int __arcvector_error( const char *funcname, int line, const vgx_Arc_t *arc, const vgx_Vertex_t *vertex, const vgx_ArcVector_cell_t *cell, vgx_ExecutionTimingBudget_t *timing_budget ) {
  uint32_t tid = GET_CURRENT_THREAD_ID();
  CRITICAL( 0xFFF, "===== Arcvector error in %s line %d (thread=%u) =====", funcname, line, tid );

  // ARC
  if( arc ) {

    const char *tail_dots = (arc->tail && arc->tail->identifier.CSTR__idstr) ? "..." : "";
    const char *head_dots = (arc->head.vertex && arc->head.vertex->identifier.CSTR__idstr) ? "..." : "";
    const char *tail_prefix = arc->tail ? CALLABLE( arc->tail )->IDPrefix( arc->tail ) : "?";
    const char *head_prefix = arc->head.vertex ? CALLABLE( arc->head.vertex )->IDPrefix( arc->head.vertex ) : "?";
    vgx_predicator_t pred = arc->head.predicator;
    int rel_enc = pred.rel.enc;
    int rel_dir = pred.rel.dir;
    int mod_bits = pred.mod.bits;
    int f = pred.mod.stored.f;
    int ival = pred.val.integer;
    float fval = pred.val.real;
    CRITICAL( 0xFFF, "  ARC      : (%s%s) -[ %016llX rel=%05d dir=%d mod=0x%02X f=%d val=<%d/%#g> ]-> (%s%s)", tail_prefix, tail_dots, pred.data, rel_enc, rel_dir, mod_bits, f, ival, fval, head_prefix, head_dots );
    char buffer[ 1024 ] = {0};
    CRITICAL( 0xFFF, "  TAIL     : %s", __api_arcvector_dump_raw_vertex( arc->tail, buffer ) );
    CRITICAL( 0xFFF, "  HEAD     : %s", __api_arcvector_dump_raw_vertex( arc->head.vertex, buffer ) );
    if( arc->tail ) {
      vgx_ArcVector_cell_t *inarcs = &arc->tail->inarcs;
      vgx_ArcVector_cell_t *outarcs = &arc->tail->outarcs;
      iarcvector.PrintDebugDump( inarcs, "TAIL INARCS DUMP" );
      iarcvector.PrintDebugDump( outarcs, "TAIL OUTARCS DUMP" );
    }
    if( arc->head.vertex ) {
      vgx_ArcVector_cell_t *inarcs = &arc->head.vertex->inarcs;
      vgx_ArcVector_cell_t *outarcs = &arc->head.vertex->inarcs;
      iarcvector.PrintDebugDump( inarcs, "HEAD INARCS DUMP" );
      iarcvector.PrintDebugDump( outarcs, "HEAD OUTARCS DUMP" );
    }
  }
  else {
    CRITICAL( 0xFFF, "  ARC      : n/a" );
  }


  // VERTEX
  if( vertex ) { 
    CRITICAL( 0xFFF, "  VERTEX   : %s", CALLABLE( vertex )->IDPrefix( vertex ) );
    PRINT_VERTEX( vertex );
  }
  else {
    CRITICAL( 0xFFF, "  VERTEX   : n/a" );
  }
  // ARCVECTOR CELL
  if( cell ) {
    CRITICAL( 0xFFF, "  VxD      : %016llX tag=%u <V=%p/D=%lld>",      cell->VxD.qword, TPTR_AS_TAG( &cell->VxD ), __arcvector_get_vertex( cell ),  __arcvector_get_degree( cell ) );
    CRITICAL( 0xFFF, "  FxP      : %016llX tag=%u <F=%p/P=0x%016llx>", cell->FxP.qword, TPTR_AS_TAG( &cell->FxP ), __arcvector_as_frametop( cell ), __arcvector_as_predicator_bits( cell ) );
  }
  else {
    CRITICAL( 0xFFF, "  VxD      : n/a" );
    CRITICAL( 0xFFF, "  FxP      : n/a" );
  }
  // TIMING BUDGET
  if( timing_budget ) {
    vgx_ExecutionTimingBudget_t *t = timing_budget;
    CRITICAL( 0xFFF, "  TIMEOUT  : t=%d t0=%lld tt=%lld tr=%lld", t->timeout_ms, t->t0_ms, t->tt_ms, t->t_remain_ms );
    CRITICAL( 0xFFF, "  TIMEOUT  : reason=%08x", t->reason );
    CRITICAL( 0xFFF, "  TIMEOUT  : resource=%p", t->resource );
    CRITICAL( 0xFFF, "  TIMEOUT  : blocking=%d halted=%d infinite=%d blocked=%d", (int)t->flags.is_blocking, (int)t->flags.is_halted, (int)t->flags.is_infinite, (int)t->flags.resource_blocked );
  }
  else {
    CRITICAL( 0xFFF, "  TIMEOUT  : n/a" );
  }

  CRITICAL( 0xFFF, "================================================================" );

  cxlib_print_backtrace( 0 );

  return -1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_api.h"
#include "tests/__utest_vxarcvector_api__basic_add_remove.h"
#include "tests/__utest_vxarcvector_api__advanced_add_remove.h"
#include "tests/__utest_vxarcvector_api__basic_get.h"
#include "tests/__utest_vxarcvector_api__complete_add_remove.h"
#include "tests/__utest_vxarcvector_api__chaotic_add_remove.h" 


test_descriptor_t _vgx_vxarcvector_api_tests[] = {
  { "VGX Arcvector API Test",   __utest_vxarcvector_api },
  { "Basic Add/Remove",         __utest_vxarcvector_api__basic_add_remove },
  { "Basic Get",                __utest_vxarcvector_api__basic_get },
  { "Advanced Add/Remove",      __utest_vxarcvector_api__advanced_add_remove },
  { "Complete Add/Remove",      __utest_vxarcvector_api__complete_add_remove },
  { "Chaotic Add/Remove",       __utest_vxarcvector_api__chaotic_add_remove },
  {NULL}
};
#endif
