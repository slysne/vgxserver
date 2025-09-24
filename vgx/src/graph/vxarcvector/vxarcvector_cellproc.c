/*######################################################################
 *#
 *# vxarcvector_cellproc.c
 *#
 *#
 *######################################################################
 */


#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static int64_t __set_as_aptr(               framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __collect_as_vertex(         framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );



/*******************************************************************//**
 * Used for bi-directional arc filters 
 * 
 ***********************************************************************
 */
typedef struct __s_cell_filter_t {
  const vgx_ArcVector_cell_t      *cell;
  vgx_virtual_ArcFilter_context_t *filter;
  vgx_ArcFilter_match             match;
} __cell_filter_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __set_as_aptr( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  framehash_cell_t *output = (framehash_cell_t*)processor->processor.output;
  APTR_COPY( output, fh_cell );
  FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_cellproc__count_nactive( const vgx_ArcVector_cell_t *V ) {
  framehash_cell_t eph_top;
  __arcvector_set_ephemeral_top( V, &eph_top );
  return iFramehash.processing.StandardSubtreeProcessors->count_nactive( &eph_top, NULL );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_cellproc__collect_first_cell( const vgx_ArcVector_cell_t *V, framehash_cell_t *first_cell ) {
  framehash_cell_t eph_top;
  __arcvector_set_ephemeral_top( V, &eph_top );
  framehash_processing_context_t set_as_aptr = FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( &eph_top, NULL, __set_as_aptr, 1 );
  FRAMEHASH_PROCESSOR_SET_IO( &set_as_aptr, NULL, first_cell );
  if( iFramehash.processing.ProcessNolock( &set_as_aptr ) == 1 ) {
    return 1;
  }
  else {
    APTR_INIT( first_cell );
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_match_predicator {
  vgx_predicator_t probe;
  vgx_predicator_t first;
  int (*match)( const vgx_predicator_t A, const vgx_predicator_t B );
} __match_predicator;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __first_predicator( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  __match_predicator *matcher = (__match_predicator*)processor->processor.input;
  vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
  if( matcher->match( matcher->probe, stored ) ) {
    matcher->first = stored;
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_cellproc__first_predicator( const vgx_ArcVector_cell_t *MAV, vgx_predicator_t probe, vgx_predicator_t *first_predicator ) {
    
  __match_predicator matcher = {
    .probe = probe,
    .first = VGX_PREDICATOR_NONE,
    .match = __select_predicator_matcher( probe )
  };

  framehash_cell_t eph_top;
  __arcvector_set_ephemeral_top( MAV, &eph_top );

  framehash_processing_context_t match_first_predicator = FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( &eph_top, NULL, __first_predicator, 1 );
  FRAMEHASH_PROCESSOR_SET_IO( &match_first_predicator, &matcher, NULL );

  if( iFramehash.processing.ProcessNolock( &match_first_predicator ) == 1 ) {
    *first_predicator = matcher.first;
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void _vxarcvector_cellproc__update_MAV( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, framehash_cell_t *predicator_map_pre, framehash_processing_context_t *arc_array_processor, framehash_cell_t *arc_array_fh_cell ) {
  // A new structure may have been formed
  framehash_cell_t *predicator_map_post = __arcvector_as_frametop( MAV );
  int64_t n_MAV_items = iFramehash.access.SubtreeLength( predicator_map_post, dynamic, 2 );
  // Still multiple arc
  if( n_MAV_items > 1 ) {
    // ... and there is a new structure
    if( predicator_map_post != predicator_map_pre ) {
      // Patch in the new reference directly
      APTR_SET_PTR56(arc_array_fh_cell, predicator_map_post);
    }
  }
  // Only a single arc left, this arcvector entry must be converted to a simple arc
  else if( n_MAV_items == 1 ) {
    // Retrieve the only active item from MAV
    framehash_cell_t singleton;
    _vxarcvector_cellproc__collect_first_cell( MAV, &singleton );
    // Copy the predicator part from the (only) item in predicator map directly into the FxP field of the entry in our array of arcs and mark as simple arc
    TPTR_SET_UNSIGNED_AND_TAG( &((vgx_ArcVector_cell_t*)arc_array_fh_cell)->FxP, APTR_AS_UNSIGNED( &singleton ), VGX_ARCVECTOR_FxP_PREDICATOR );
    // Discard the predicator map with the single item we just copied
    _vxarcvector_fhash__discard( dynamic, MAV );
  }
  // Zero entries in MAV, we discard the MAV and update array of arcs framehash by deleting item
  else {
    // Discard the empty predicator map
    _vxarcvector_fhash__discard( dynamic, MAV );
    // Force erase the framehash cell
    FRAMEHASH_PROCESSOR_DELETE_CELL( arc_array_processor, arc_array_fh_cell );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __collect_as_vertex( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t traversed = 0;
  vgx_neighborhood_probe_t *neighborhood_probe = (vgx_neighborhood_probe_t*)processor->processor.input;
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;
  vgx_virtual_ArcFilter_context_t *arcfilter = recursive->arcfilter;
  framehash_cell_t eph_top;
  vgx_ArcVector_cell_t arc_cell;
  vgx_ArcHead_t archead = VGX_ARCHEAD_INIT_PREDICATOR_BITS( VGX_PREDICATOR_NONE_BITS, (vgx_Vertex_t*)fh_cell->annotation );
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;

  __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, arcfilter->timing_budget, &filter_match ) {
    // GREEN    Multiple Arc: FRAME
    if( __arcvector_fhash_is_multiple_arc( fh_cell ) ) {
      // Multiple Arc: FRAME
      __arcvector_cell_set_multiple_arc( &arc_cell, LARC.head.vertex, (framehash_cell_t*)APTR_GET_PTR56(fh_cell) );
      __arcvector_set_ephemeral_top( &arc_cell, &eph_top );
      vgx_Arc_t first_match; // dummy, we don't care about the contents
      filter_match = _vxarcvector_exists__multi_predicator_has_arc( &eph_top, &LARC, neighborhood_probe->distance, arcfilter, &first_match );
    }
    // Simple Arc: PREDICATOR
    else {
      // Simple Arc: PREDICATOR
      LARC.head.predicator.data = APTR_AS_UNSIGNED(fh_cell);
      _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance );

      __pre_arc_visit( arcfilter, &LARC, &filter_match ) {
        __begin_arcvector_filter_accept_arc( arcfilter, &LARC, &filter_match ) {
        } __end_arcvector_filter_accept_arc;
      } __post_arc_visit( filter_match == VGX_ARC_FILTER_MATCH_MISS ); // Only allow synthetic arc (i.e. process again with synthetic predicator) if arc filter was miss on normal predicator
    }

    if( filter_match == VGX_ARC_FILTER_MATCH_HIT ) {
      ++traversed;
      // Set overall match (i.e. "1 or more") to HIT
      *(vgx_ArcFilter_match*)processor->processor.output = VGX_ARC_FILTER_MATCH_HIT;
      switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
      case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
        if( _vgx_collector_mode_is_deep_collect( neighborhood_probe->collector_mode ) == false )  {
          FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
        }
        break;
      case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
        {
          vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)neighborhood_probe->common_collector;
          if( __arcvector_collect_vertex( collector, &LARC, processor ) < 0 || LARC.acquired.head_lock < 0 ) {
            filter_match = __arcfilter_error();
          }
        }
        break;
      default:
        break;
      }
    }

  } __end_lockable_arc_context;

  if( __is_arcfilter_error( filter_match ) ) {
    traversed = -1;
  }

  return traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_cellproc__collect_vertices( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe ) {
  vgx_ArcFilter_match at_least_one_match = VGX_ARC_FILTER_MATCH_MISS;
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;

  framehash_cell_t eph_top;
  __arcvector_set_ephemeral_top( V, &eph_top );

  framehash_processing_context_t collect_as_vertex = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_top, NULL, __collect_as_vertex );
  FRAMEHASH_PROCESSOR_SET_IO( &collect_as_vertex, neighborhood_probe, &at_least_one_match );

  if( iFramehash.processing.ProcessNolock( &collect_as_vertex ) < 0 ) {
    return __arcfilter_error();
  }

  return __arcfilter_THRU( recursive, at_least_one_match );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __collect_as_vertex_bidirectional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t traversed = 0;
  __cell_filter_t *cell_filter_V2 = (__cell_filter_t*)processor->processor.input;
  const vgx_ArcVector_cell_t *V2 = cell_filter_V2->cell;
  vgx_virtual_ArcFilter_context_t *arcfilter_V2 = cell_filter_V2->filter;
  vgx_neighborhood_probe_t *neighborhood_probe = (vgx_neighborhood_probe_t*)processor->processor.output;
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;
  vgx_virtual_ArcFilter_context_t *arcfilter = recursive->arcfilter;
  framehash_cell_t eph_top_V1;
  vgx_ArcVector_cell_t arc_cell;
  vgx_ArcHead_t archead = VGX_ARCHEAD_INIT_PREDICATOR_BITS( VGX_PREDICATOR_NONE_BITS, (vgx_Vertex_t*)fh_cell->annotation );
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;

  __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, archead.predicator, archead.vertex, arcfilter->timing_budget, &filter_match ) {

    // GREEN    Multiple Arc: FRAME
    if( __arcvector_fhash_is_multiple_arc( fh_cell ) ) {
      // Multiple Arc: FRAME
      __arcvector_cell_set_multiple_arc( &arc_cell, LARC.head.vertex, (framehash_cell_t*)APTR_GET_PTR56(fh_cell) );
      __arcvector_set_ephemeral_top( &arc_cell, &eph_top_V1 );
      vgx_Arc_t first_match; // dummy, we don't care about the contents
      vgx_GenericArcFilter_context_t *generic_filter_V2 = (vgx_GenericArcFilter_context_t*)arcfilter_V2;
      // Update HEAD each time: the opposing V2 arc has to terminate at the same vertex as the V1 arc
      generic_filter_V2->terminal.current = LARC.head.vertex;
      generic_filter_V2->terminal.logic = VGX_LOGICAL_AND;
      // Update PREDICATOR CONDITION each time: the opposing V2 relationship and modifier should match the V1 arc
      generic_filter_V2->pred_condition1 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition1, LARC.head.predicator );
      generic_filter_V2->pred_condition2 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition2, LARC.head.predicator );
      filter_match = _vxarcvector_exists__multi_predicator_has_arc( &eph_top_V1, &LARC, neighborhood_probe->distance, arcfilter, &first_match );
    }
    // Simple Arc: PREDICATOR
    else {
      LARC.head.predicator.data = APTR_AS_UNSIGNED(fh_cell);
      _vgx_arc_set_distance( (vgx_Arc_t*)&LARC, neighborhood_probe->distance );
      // Require bi-directional
      __begin_arcvector_filter_accept_arc( arcfilter, &LARC, &filter_match ) {
        vgx_GenericArcFilter_context_t *generic_filter_V2 = (vgx_GenericArcFilter_context_t*)arcfilter_V2;
        // Update HEAD each time: the opposing V2 arc has to terminate at the same vertex as the V1 arc
        generic_filter_V2->terminal.current = LARC.head.vertex;
        generic_filter_V2->terminal.logic = VGX_LOGICAL_AND;
        // Update PREDICATOR CONDITION each time: the opposing V2 relationship and modifier should match the V1 arc
        generic_filter_V2->pred_condition1 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition1, LARC.head.predicator );
        generic_filter_V2->pred_condition2 = _vgx_predicator_merge_inherit_key( generic_filter_V2->pred_condition2, LARC.head.predicator );
        __arcvector_probe_push_conditional( neighborhood_probe, arcfilter_V2, recursive->vertex_probe, recursive->arcdir, recursive->evaluator ) {
          vgx_Arc_t a2 = {0};
          filter_match = iarcvector.HasArc( V2, recursive, neighborhood_probe, &a2 );
        } __arcvector_probe_pop_conditional;
      } __end_arcvector_filter_accept_arc;
    }

    if( filter_match == VGX_ARC_FILTER_MATCH_HIT ) {
      ++traversed;
      // Set overall match (i.e. "1 or more") to HIT
      cell_filter_V2->match = VGX_ARC_FILTER_MATCH_HIT;
      switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
      case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
        if( _vgx_collector_mode_is_deep_collect( neighborhood_probe->collector_mode ) == false ) {
          FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
        }
        break;
      case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
        {
          vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)neighborhood_probe->common_collector;
          if( __arcvector_collect_vertex( collector, &LARC, processor ) < 0 || LARC.acquired.head_lock < 0 ) {
            filter_match = __arcfilter_error();
          }
        }
        break;
      default:
        break;
      }
    }

  } __end_lockable_arc_context;

  if( __is_arcfilter_error( filter_match ) ) {
    traversed = -1;
  }

  return traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_cellproc__collect_vertices_bidirectional( const vgx_ArcVector_cell_t *V1, const vgx_ArcVector_cell_t *V2, vgx_neighborhood_probe_t *neighborhood_probe ) {

  framehash_cell_t eph_top_V1;
  __arcvector_set_ephemeral_top( V1, &eph_top_V1 );
  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;

  __cell_filter_t cell_filter = {
    .cell   = V2,
    .filter = iArcFilter.Clone( recursive->arcfilter ),
    .match  = VGX_ARC_FILTER_MATCH_MISS
  };
  
  if( cell_filter.filter == NULL ) {
    return -1;
  }

  framehash_processing_context_t collect_as_vertex_bidirectional = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_top_V1, NULL, __collect_as_vertex_bidirectional );
  FRAMEHASH_PROCESSOR_SET_IO( &collect_as_vertex_bidirectional, &cell_filter, neighborhood_probe );

  int64_t n_proc = iFramehash.processing.ProcessNolock( &collect_as_vertex_bidirectional );
  iArcFilter.Delete( &cell_filter.filter );
  if( n_proc < 0 || __is_arcfilter_error( cell_filter.match ) ) {
    return __arcfilter_error();
  }

  return __arcfilter_THRU( recursive, cell_filter.match );
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __operation_sync_multiple_arc_CS_NT( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  vgx_Arc_t *arc = processor->processor.input;
  arc->head.predicator.data = APTR_AS_UNSIGNED( fh_cell );
  return iOperation.Arc_WL.Connect( arc );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __operation_sync_arcs_CS_NT( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t n_traversed = 0;
  framehash_cell_t eph_top;
  vgx_ArcVector_cell_t arc_cell;

  vgx_Arc_t arc = {
    .tail = processor->processor.input,
    .head = VGX_ARCHEAD_INIT_PREDICATOR_BITS( VGX_PREDICATOR_NONE_BITS, (vgx_Vertex_t*)fh_cell->annotation )
  };

  // GREEN    Multiple Arc: FRAME
  if( __arcvector_fhash_is_multiple_arc( fh_cell ) ) {
    // Multiple Arc: FRAME
    __arcvector_cell_set_multiple_arc( &arc_cell, arc.head.vertex, (framehash_cell_t*)APTR_GET_PTR56(fh_cell) );
    __arcvector_set_ephemeral_top( &arc_cell, &eph_top );
    framehash_processing_context_t multiple = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_top, NULL, __operation_sync_multiple_arc_CS_NT );
    FRAMEHASH_PROCESSOR_SET_IO( &multiple, &arc, NULL );
    n_traversed = iFramehash.processing.ProcessNolock( &multiple );
  }
  // Simple Arc: PREDICATOR
  else {
    // Simple Arc: PREDICATOR
    arc.head.predicator.data = APTR_AS_UNSIGNED(fh_cell);
    n_traversed = iOperation.Arc_WL.Connect( &arc );
  }

  return n_traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_cellproc__operation_sync_outarcs_CS_NT( vgx_Vertex_t *tail ) {
  int64_t n_arcs = 0;

  vgx_ArcVector_cell_t *V = &tail->outarcs;
  _vgx_ArcVector_cell_type ctype = __arcvector_cell_type( V );

  // Not WHITE: (Not Empty)
  if( ctype != VGX_ARCVECTOR_NO_ARCS ) {
    // Should be impossible: outarcs arcvector can never have this type
    if( ctype == VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
      return -1;
    }
    vgx_Graph_t *graph = tail->graph;
    vgx_Vertex_t *tail_WL = __vertex_lock_writable_CS( tail );
    Vertex_INCREF_WL( tail_WL );
    if( iOperation.Open_CS( graph, &tail_WL->operation, COMLIB_OBJECT( tail_WL ), true ) < 0 ) {
      // ERROR: can't open operation
      n_arcs = -1;
    }
    else {
      // BLUE: Simple arc
      if( ctype == VGX_ARCVECTOR_SIMPLE_ARC ) {
        vgx_Arc_t arc = {
          .tail = tail_WL,
          .head = __arcvector_init_archead_from_cell( V )
        };
        n_arcs = iOperation.Arc_WL.Connect( &arc );
      }
      // GREEN: Array of arcs
      else {
        framehash_cell_t eph_top;
        __arcvector_set_ephemeral_top( V, &eph_top );

        framehash_processing_context_t sync_arcs = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_top, NULL, __operation_sync_arcs_CS_NT );
        FRAMEHASH_PROCESSOR_SET_IO( &sync_arcs, tail_WL, NULL );

        n_arcs = iFramehash.processing.ProcessNolock( &sync_arcs );
      }

      iOperation.Close_CS( graph, &tail_WL->operation, true );
    }
    __vertex_unlock_writable_CS( tail_WL );
    Vertex_DECREF_WL( tail_WL );
  }

  return n_arcs;
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_cellproc.h"


test_descriptor_t _vgx_vxarcvector_cellproc_tests[] = {
  { "Basic Add/Remove",     __utest_vxarcvector_cellproc },
  {NULL}
};
#endif

