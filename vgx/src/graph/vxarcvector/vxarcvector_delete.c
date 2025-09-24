/*######################################################################
 *#
 *# vxarcvector_delete.c
 *#
 *#
 *######################################################################
 */


#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_filtered_predicator_delete {
  const vgx_predicator_t probe;
  __f_match_pred match_pred;
  int64_t n_deleted;
} __filtered_predicator_delete;




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __count_predicator( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  __filtered_predicator_delete *deleter = (__filtered_predicator_delete*)processor->processor.input;
  vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
  if( deleter->match_pred( deleter->probe, stored ) ) {
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __delete_predicator( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  // TODO 1: MAKE THIS MORE EFFICIENT BY NOT DOING INDIVIDUAL DISCONNECT EVENTS

  __filtered_predicator_delete *deleter = (__filtered_predicator_delete*)processor->processor.input;
  vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
  if( deleter->match_pred( deleter->probe, stored ) ) {
    // Force erase the framehash cell (NO STRUCTURE COMPACTIFICATION WILL OCCUR!)
    FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
    deleter->n_deleted++;
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 * Returns:
 *          1: if predicator map was modified (deleted from and/or compactified)
 *          0: if no changes were made (no deletions, no compactification)
 *         -1: error (or timeout)
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_delete__delete_predicators( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted ) {
  int modified = 0;
  framehash_cell_t *frame = __arcvector_get_frametop( MAV );

  int64_t n_arcs = 0;

  __filtered_predicator_delete deleter = {
    .probe          = arc->head.predicator,
    .match_pred     = __select_predicator_matcher( arc->head.predicator ),
    .n_deleted      = 0
  };

  // Count the number of matching arcs so we know how to call disconnect event
  framehash_processing_context_t count_predicators = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __count_predicator );
  FRAMEHASH_PROCESSOR_SET_IO( &count_predicators, &deleter, NULL );
  if( (n_arcs = iFramehash.processing.ProcessNolock( &count_predicators )) > 0 ) {
    int64_t n_reverse = 0;
    int64_t n_forward = 0;

    vgx_Vertex_t *current_head = arc->head.vertex;
    vgx_Graph_t *graph = arc->tail->graph;
    GRAPH_LOCK( graph ) {
      // Lock the head
      vgx_Vertex_t *head_WL = NULL;
      bool external_lock = __vertex_is_locked_writable_by_current_thread( current_head );
      int inarcs_reclaimed = 0;
      if( external_lock ) {
        // Reclaim inarcs if yielded
        if( !__vertex_is_inarcs_yielded( current_head ) || (inarcs_reclaimed = _vxgraph_state__reclaim_inarcs_CS_WL( graph, current_head, timing_budget )) == 1 ) {
          head_WL = current_head;
        }
      }
      else {
        head_WL = _vxgraph_state__lock_vertex_writable_CS( graph, current_head, timing_budget, VGX_VERTEX_RECORD_OPERATION );
      }

      // We have headlock
      if( head_WL ) {
        // Disconnect other end
        n_reverse = disconnect_event( dynamic, arc, n_arcs, timing_budget );
        // Release
        if( external_lock == false ) {
          _vxgraph_state__unlock_vertex_CS_LCK( graph, &head_WL, VGX_VERTEX_RECORD_OPERATION );
        }
        // Or re-yield inarcs if originally yielded
        else if( inarcs_reclaimed ) {
          _vxgraph_state__yield_inarcs_CS_WL( graph, head_WL );
        }
      }
    } GRAPH_RELEASE;

    // Success disconnecting other end
    if( n_reverse == n_arcs ) {
      // Delete arcs from arcvector
      framehash_processing_context_t delete_predicators = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __delete_predicator );
      FRAMEHASH_PROCESSOR_SET_IO( &delete_predicators, &deleter, NULL );
      FRAMEHASH_PROCESSOR_MAY_MODIFY( &delete_predicators );
      n_forward = iFramehash.processing.ProcessNolock( &delete_predicators );
    }
    // Timeout
    else if( n_reverse == 0 && _vgx_is_execution_halted( timing_budget ) ) {
      return -1;
    }
    else {
      CRITICAL( 0x651, "Disconnect error: expected exactly %lld arcs, disconnected=%lld", n_arcs, n_reverse );
      return __ARCVECTOR_ERROR( arc, NULL, MAV, timing_budget );
    }

    // Check fwd/rev symmetry
    if( n_forward != n_reverse ) {
      CRITICAL( 0x652, "Asymmetric arc removal" );
      return __ARCVECTOR_ERROR( arc, NULL, MAV, timing_budget );
    }

    // Deletion(s) occurred
    if( deleter.n_deleted > 0 ) {
      // Adjust predicator map structure after direct cell deletion and flag modification if compactified
      if( _vxarcvector_fhash__compactify( dynamic, MAV ) < 0 ) {
        CRITICAL( 0x653, "Failed to compactify predicator map" );
        return __ARCVECTOR_ERROR( arc, NULL, MAV, timing_budget );
      }

      // Assign the number of deleted items to the output variable and flag modification
      modified = 1;
      *n_deleted = deleter.n_deleted;
    }
  }
  else {
    *n_deleted = 0;
  }

  // This will be 1 if compactification and/or deletion occurred
  return modified;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_filtered_arc_delete {
  const vgx_predicator_t probe;
  __f_match_pred match_pred;
  vgx_Arc_t *arc;
  vgx_arc_direction arcdir;
  framehash_dynamic_t *dynamic;
  f_Vertex_disconnect_event disconnect;
  vgx_ArcVector_cell_t *V;
  int64_t n_deleted;
  int64_t n_pending;
  int64_t n_errors;
  vgx_ExecutionTimingBudget_t zero_budget;
} __filtered_arc_delete;



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __delete_arc_CS( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  // TODO 1: MAKE THIS MORE EFFICIENT BY NOT DOING INDIVIDUAL DISCONNECT EVENTS
  int64_t n_affected = 0;
  __filtered_arc_delete *deleter = (__filtered_arc_delete*)processor->processor.input;
  vgx_Graph_t *graph = deleter->arc->tail->graph;

  vgx_Vertex_t *prev_vertex = deleter->arc->head.vertex;

  // Set current head vertex
  vgx_Vertex_t *current_head = deleter->arc->head.vertex = (vgx_Vertex_t*)APTR_AS_ANNOTATION( fh_cell );

  // GREEN    Multiple Arc: FRAME
  if( __arcvector_fhash_is_multiple_arc( fh_cell ) ) {
    //          We need to forward the deletion to the multiple predicators deleter
    framehash_cell_t *predicator_map_pre = (framehash_cell_t*)APTR_GET_PTR56(fh_cell);

    // Load info into temporary arcvector cell for MAV processing, then run MAV deletion
    vgx_ArcVector_cell_t entry;
    __arcvector_cell_set_multiple_arc( &entry, current_head, predicator_map_pre );
    int64_t n_multiple_deleted = 0;
    int modified = 0;
    GRAPH_SUSPEND_LOCK( graph ) {
      modified = _vxarcvector_delete__delete_predicators( deleter->dynamic, &entry, deleter->arc, &deleter->zero_budget, deleter->disconnect, &n_multiple_deleted );
    } GRAPH_RESUME_LOCK;

#ifdef VGX_CONSISTENCY_CHECK
    _vxarcvector_fhash__trap_bad_frame_reference( &entry );
#endif

    // If predicator map was modified (deleted and/or compacted) we have to do more processing
    if( modified > 0 ) {
      __arcvector_dec_degree( deleter->V, n_multiple_deleted );
      deleter->n_deleted += n_multiple_deleted;
      _vxarcvector_cellproc__update_MAV( deleter->dynamic, &entry, predicator_map_pre, processor, fh_cell );
      n_affected = 1;
    }
    // Error
    else if( modified < 0 ) {
      // Head timeout - will try again
      if( _vgx_is_execution_halted( &deleter->zero_budget ) ) {
        deleter->n_pending++;
      }
      // Other error, will not try again
      else {
        REASON( 0x671, "Disconnect error: arc head '%s' unexpected reason %08x", CALLABLE( current_head )->IDString( current_head ), deleter->zero_budget.reason );
        deleter->n_errors++;
      }
      // Reset the budget for next iteration
      _vgx_reset_execution_timing_budget( &deleter->zero_budget );
    }

  }
  // Simple Arc: PREDICATOR
  else {
    vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
    if( deleter->match_pred( deleter->probe, stored ) ) {
      // Update arc with an exact rel_mod key force direct removal on the other side of the disconnect event
      vgx_predicator_t orig_pred = deleter->arc->head.predicator;
      deleter->arc->head.predicator.rkey = stored.rkey;

      // Lock the head
      vgx_Vertex_t *head_WL = NULL;
      bool external_lock = __vertex_is_locked_writable_by_current_thread( current_head );
      int inarcs_reclaimed = 0;
      if( external_lock ) {
        // Reclaim inarcs if yielded
        if( !__vertex_is_inarcs_yielded( current_head ) || (inarcs_reclaimed = _vxgraph_state__reclaim_inarcs_CS_WL( graph, current_head, &deleter->zero_budget )) == 1 ) {
          head_WL = current_head;
        }
      }
      else {
        head_WL = _vxgraph_state__lock_vertex_writable_CS( graph, current_head, &deleter->zero_budget, VGX_VERTEX_RECORD_OPERATION );
      }

      // We have headlock
      if( head_WL ) {
        // First disconnect other end
        int64_t n;
        if( (n = deleter->disconnect( deleter->dynamic, deleter->arc, 1, &deleter->zero_budget )) == 1 ) {
          // Then force erase the framehash cell
          FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
          n_affected = 1;
          __arcvector_dec_degree( deleter->V, 1 );
          deleter->n_deleted++;
        }
        // Deletion failed
        else {
          CRITICAL( 0x672, "Disconnect error: expected exactly 1 arc, disconnected=%lld", n );
          n_affected = __ARCVECTOR_ERROR( deleter->arc, NULL, NULL, NULL );
          deleter->n_errors++;
        }
        // Release
        if( external_lock == false ) {
          _vxgraph_state__unlock_vertex_CS_LCK( graph, &head_WL, VGX_VERTEX_RECORD_OPERATION );
        }
        // Re-yield inarcs if they were originally yielded
        else if( inarcs_reclaimed ) {
          _vxgraph_state__yield_inarcs_CS_WL( graph, head_WL );
        }
      }
      // Failed to lock head
      else {
        // Head timeout - will try again
        if( _vgx_is_execution_halted( &deleter->zero_budget ) ) {
          deleter->n_pending++;
        }
        // Other error, will not try again
        else {
          SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
          REASON( 0x673, "Disconnect error: arc head '%s' unexpected reason %08x", CALLABLE( current_head )->IDString( current_head ), deleter->zero_budget.reason );
          deleter->n_errors++;
        }
        // Reset the budget for next iteration
        _vgx_reset_execution_timing_budget( &deleter->zero_budget );
      }
      deleter->arc->head.predicator = orig_pred;
    }
  }

  deleter->arc->head.vertex = prev_vertex;

  return n_affected;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_delete__delete_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted ) {
  int modified = 0;
  vgx_Vertex_t *H = __arcvector_get_vertex( V );
  vgx_predicator_t P;
  // Either wildcard remove or the specified probe matches existing simple arc
  if( arc->head.vertex == NULL || arc->head.vertex == H ) {
    P.data = __arcvector_as_predicator_bits( V );
    if( predmatchfunc.Generic( arc->head.predicator, P ) ) {
      // Arc match - simple arc, extract head
      vgx_Arc_t del_arc = {
        .tail = arc->tail,
        .head = {
          .vertex     = H,
          .predicator = arc->head.predicator
        }
      };
      // Disconnect other end first
      int64_t n_rev;
      if( (n_rev = disconnect_event( dynamic, &del_arc, 1, timing_budget )) == 1 ) {
        (*n_deleted)++;
        // Convert cell to empty
        __arcvector_cell_set_no_arc( V );
        modified = 1;
      }
      // Disconnect timeout
      else if( n_rev == 0 && _vgx_is_execution_halted( timing_budget ) ) {
        modified = -1;
      }
      else {
        CRITICAL( 0x201, "Disconnect error: expected exactly 1 arc, n_removed=%lld", n_rev );
        modified = __ARCVECTOR_ERROR( arc, NULL, V, timing_budget ); // disconnect error: should be exactly 1 (the one we matched above)
      }
    }
  }

  return modified;
}



/*******************************************************************//**
 *
 * Returns:
 *          1: if array of arcs was modified (deleted from and/or compactified)
 *          0: if no changes were made (no deletions, no compactification)
 *         -1: error
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_delete__delete_arcs( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted ) {
  int modified = 0;

  __filtered_arc_delete deleter = {
    .probe        = arc->head.predicator,
    .match_pred   = __select_predicator_matcher( arc->head.predicator ),
    .arc          = arc,
    .arcdir       = __predicator_direction( arc->head.predicator ),
    .dynamic      = dynamic,
    .disconnect   = disconnect_event,
    .V            = V,
    .n_deleted    = 0,    // number of arcs deleted, including individual arc in multiple arcs
    .n_pending    = 0,    // number of arcs that should be deleted but cannot because of head lock
    .n_errors     = 0,
    .zero_budget  = _vgx_get_zero_execution_timing_budget()
  };


  vgx_Graph_t *graph = arc->tail->graph;

  if( timing_budget->t0_ms == 0 ) {
    _vgx_start_graph_execution_timing_budget( graph, timing_budget );
  }

  do {
    // Previous attempt made, take a short nap and check for timeout
    if( deleter.n_pending > 0 || deleter.n_errors > 0 ) {
      GRAPH_LOCK( graph ) {
        BEGIN_DISALLOW_READONLY_CS( &graph->readonly ) {
          // Yield inarcs and short sleep ONLY IF WE ARE DELETING OUTARCS
          if( deleter.arcdir == VGX_ARCDIR_OUT ) {
            _vxgraph_state__yield_vertex_inarcs_and_wait_CS_WL( graph, arc->tail, 1 ); // <-----!!!!!!!!!!!
          }
          // (Yielding inarcs in the middle of removing our own inarcs is not possible)
          else {
            GRAPH_SUSPEND_LOCK( graph ) {
              sleep_milliseconds( 1 );
            } GRAPH_RESUME_LOCK;
          }
        } END_DISALLOW_READONLY_CS;
        // Halt if budget exceeded
        if( _vgx_graph_milliseconds( graph ) > timing_budget->tt_ms ) {
          timing_budget->reason = VGX_ACCESS_REASON_TIMEOUT;
          _vgx_set_execution_resource_blocked( timing_budget, NULL );
          _vgx_update_graph_execution_timing_budget( graph, timing_budget, true );
        }
      } GRAPH_RELEASE;

      // Timeout
      if( _vgx_is_execution_halted( timing_budget ) ) {
        return -1;
      }

      // Reset counters and timing budget
      deleter.n_deleted = 0;
      deleter.n_pending = 0;
      deleter.n_errors = 0;
      _vgx_reset_execution_timing_budget( &deleter.zero_budget );
    }

    // Configure processor for deletion
    framehash_cell_t *frame = __arcvector_get_frametop( V );
    framehash_processing_context_t delete_arcs_CS = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __delete_arc_CS );
    FRAMEHASH_PROCESSOR_SET_IO( &delete_arcs_CS, &deleter, NULL );
    FRAMEHASH_PROCESSOR_MAY_MODIFY( &delete_arcs_CS );

    // Perform direct removal of all matching arcs (and inner multiple arcs)
    GRAPH_LOCK( graph ) {
      if( iFramehash.processing.ProcessNolock( &delete_arcs_CS ) < 0 ) {
        modified = -1;
      }
    } GRAPH_RELEASE;
    if( modified < 0 ) {
      return -1;
    }

    // One ore more deletions occurred
    if( deleter.n_deleted > 0 ) {
      *n_deleted += deleter.n_deleted;

      // Adjust array of arcs structure after direct cell deletion and flag modification if compaction occurred
      if( _vxarcvector_fhash__compactify( dynamic, V ) < 0 ) {
        CRITICAL( 0x661, "Failed to compactify array of arcs" );
      }

      modified = 1;

      int64_t n_remain = __arcvector_get_degree( V );

      // Convert array of arcs to simple arc if only one arc remaining
      if( n_remain == 1 ) {
        if( _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( dynamic, V ) < 0 ) {
          CRITICAL( 0x662, "Conversion from array of arcs to simple arc failed" );
          return __ARCVECTOR_ERROR( NULL, NULL, V, timing_budget );
        }
      }
      // Discard the whole thing and mark as empty if no arcs remaining
      else if( n_remain == 0 ) {
        _vxarcvector_fhash__discard( dynamic, V );
        __arcvector_cell_set_no_arc( V );
      }
    }

  } while( deleter.n_pending > 0 && __arcvector_cell_type( V ) == VGX_ARCVECTOR_ARRAY_OF_ARCS );

  // Common edge case: All but one arc deleted
  if( __arcvector_cell_type( V ) == VGX_ARCVECTOR_SIMPLE_ARC && deleter.n_pending > 0 ) {
    int del;
    if( (del = _vxarcvector_delete__delete_simple_arc( dynamic, V, arc, timing_budget, disconnect_event, n_deleted )) < 0 ) {
      modified = -1;
    }
    else if( del > 0 ) {
      modified = 1;
    }
  }

  return modified;
}





/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_delete.h"


test_descriptor_t _vgx_vxarcvector_delete_tests[] = {
  { "Arcvector Delete",     __utest_vxarcvector_delete },
  {NULL}
};
#endif
