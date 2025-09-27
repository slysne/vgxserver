/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_dispatch.c
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

#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int     __array_delete_simple_arc(                 framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *SAC, vgx_Arc_t *probe_arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed );
static int     __array_try_compactify(                    framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V );
static int     __predicator_map_update_try_compactify(    framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, framehash_cell_t *original_map );
static int     __array_remove_multiple_arc(               framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed );
static int     __array_delete_specific_from_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *del_arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed );
static int     __array_predicator_remove(                 framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *entry, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed );




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_dispatch__set_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *arc_cell, vgx_Arc_t *set_arc ) {
  // Add A-(rel)->B to the V structure, given the predetermined state of the arc_cell in V (WHITE, BLUE or GREEN)
  int n_inserted = -1;
  framehash_cell_t *previous_ft, *current_ft;
  vgx_predicator_t arcpred;

  // WHITE, BLUE or GREEN
  _vgx_ArcVector_cell_type arc_type = __arcvector_cell_type( arc_cell );

  switch( arc_type ) {
  // WHITE: No arcs ->B exist, add a Simple Arc A-(rel)->B
  case VGX_ARCVECTOR_NO_ARCS:
    // Becomes BLUE
    if( (n_inserted = _vxarcvector_fhash__set_simple_arc( dynamic, V, set_arc )) < 0 ) {
      return __ARCVECTOR_ERROR( set_arc, NULL, V, NULL );
    }
    break;

  // BLUE: Simple Arc A->(x)->B exists, we update or need to convert existing Simple Arc to new Multiple Arc A=(x,...)=>B
  case VGX_ARCVECTOR_SIMPLE_ARC:
    arcpred.data = __arcvector_as_predicator_bits( arc_cell );
    if( set_arc->head.predicator.rkey == arcpred.rkey ) {
      // Stays BLUE
      vgx_Arc_t updated = {
        .tail = set_arc->tail,
        .head = {
          .vertex = set_arc->head.vertex,
          .predicator = _vgx_update_predicator_value_if_accumulator( arcpred, set_arc->head.predicator )
        }
      };
      if( (n_inserted = _vxarcvector_fhash__set_simple_arc( dynamic, V, &updated )) != 0 ) {
        return __ARCVECTOR_ERROR( &updated, NULL, V, NULL );
      }
      break;
    }
    else {
      // Becomes GREEN
      if( _vxarcvector_fhash__convert_simple_arc_to_multiple_arc( dynamic, arc_cell ) < 0 ) {
        return -1; // conversion failed
      }
      else {
        framehash_cell_t *predicator_frametop = __arcvector_get_frametop( arc_cell );
        if( _vxarcvector_fhash__set_predicator_map( dynamic, V, set_arc->head.vertex, predicator_frametop ) != 0 ) { // <= should not insert, just overwrite, so == 0
          // TODO: rollback of conversion?
          return __ARCVECTOR_ERROR( NULL, set_arc->head.vertex, V, NULL );
        }
      }
      /* FALLTHRU */
    }
  
  // GREEN: Multiple Arc A=(x,...)=>B exists, add the new arc so we have A=(x,rel,...)=>B
  case VGX_ARCVECTOR_MULTIPLE_ARC:
    // Stays GREEN
    previous_ft = __arcvector_get_frametop( arc_cell );
    if( (n_inserted = _vxarcvector_fhash__append_to_multiple_arc( dynamic, arc_cell, set_arc->head.predicator )) < 0 ) {
      return __ARCVECTOR_ERROR( set_arc, NULL, arc_cell, NULL );
    }
    else {
      current_ft = __arcvector_get_frametop( arc_cell );
      if( previous_ft != current_ft ) {
        // The arc's underlying framehash changed - we have to update the array of arcs
        if( _vxarcvector_fhash__set_predicator_map( dynamic, V, set_arc->head.vertex, current_ft ) != 0 ) { // <= should not insert, just overwrite, so == 0
          // TODO: rollback of conversion?
          return __ARCVECTOR_ERROR( NULL, set_arc->head.vertex, V, NULL );
        }
      }
    }
    break;

  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    FATAL( 0xFFF, "%s: Invalid arcvector context: indegree counter only", __FUNCTION__ );
    /* FALLTHRU */

  // INVALID arc_cell
  default:
    return __ARCVECTOR_ERROR( set_arc, NULL, arc_cell, NULL );
  }

  // Increment degree if insertion resulted in item being added
  TPTR_AS_INTEGER( &V->VxD ) += n_inserted;

  return n_inserted;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcHead_t * _vxarcvector_dispatch__get_arc( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, vgx_ArcHead_t *probe, vgx_ArcHead_t *ret ) {

  vgx_ArcHead_t *arc = NULL;

  vgx_ArcVector_cell_t entry;
  _vgx_ArcVector_cell_type tp = __arcvector_cell_type( _vxarcvector_fhash__get_arc_cell( dynamic, V, probe->vertex, &entry ) );

  // Not WHITE and not GRAY and vertex matches probe
  if( tp != VGX_ARCVECTOR_NO_ARCS && tp != VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY && __arcvector_get_vertex( &entry ) == probe->vertex ) {
    vgx_predicator_t pred;
    switch( tp ) {
    // BLUE: Simple Arc
    case VGX_ARCVECTOR_SIMPLE_ARC:
      pred = __arcvector_as_predicator( &entry );
      break;
    // GREEN: Multiple Arc
    case VGX_ARCVECTOR_MULTIPLE_ARC:
      pred = _vxarcvector_fhash__get_predicator( dynamic, &entry, probe->predicator );
      break;
    // ???
    default:
      pred = VGX_PREDICATOR_NONE;
      break;
    }

    if( predmatchfunc.Generic( probe->predicator, pred ) ) {
      ret->predicator.data = pred.data;
      ret->vertex = probe->vertex;
      arc = ret;
    }
  }


  return arc;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __array_delete_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *SAC, vgx_Arc_t *probe_arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed ) {

  int del = 0;

  // Conditional delete
  if( !_vgx_predicator_full_wildcard( probe_arc->head.predicator ) ) {
    // Cancel deletion if stored arc we're trying to delete does not match our condition
    if( !predmatchfunc.Generic( probe_arc->head.predicator, __arcvector_as_predicator( SAC ) ) ) {
      return 0; // arc's predicator did not match probe arc, zero arcs removed
    }
  }
  
  // Proceed with removal. Remove Simple Arc (keyed by Vertex).

  // Disconnect reverse then erase arc
  int64_t n_rev;
  if( (n_rev = disconnect_event( dynamic, probe_arc, 1, timing_budget )) == 1 ) {
    // Remove arc from arcvector
    if( (del = _vxarcvector_fhash__del_simple_arc( dynamic, V, probe_arc->head.vertex )) == 1 ) {
      (*n_removed)++;
      // If now there is only one arc left in the array of arcs we try to convert the array of arcs (framehash)
      // so the root arcvector will be a Simple Arc with no framehash underneath. 
      if( __arcvector_dec_degree( V, 1 ) == 1 ) {
        if( _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( dynamic, V ) < 0 ) {
          CRITICAL( 0x231, "Conversion from array of arcs to simple arc failed!" );
          return __ARCVECTOR_ERROR( probe_arc, NULL, V, timing_budget );
        }
      }
    }
    // This is bad.
    else {
      CRITICAL( 0x232, "Framehash simple arc deletion failed!" );
      return __ARCVECTOR_ERROR( probe_arc, NULL, V, timing_budget );
    }
  }
  // Timeout
  else if( n_rev == 0 && _vgx_is_execution_halted( timing_budget ) ) {
    return -1;
  }

  return del;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __array_try_compactify( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V ) {
  switch( iFramehash.access.SubtreeLength( __arcvector_as_frametop( V ), dynamic, 2 ) ) {
  // No entries left in V: Set arcvector to empty
  case 0:
    // Delete the (empty) array of arcs and set the V reference to No Acs
    if( _vxarcvector_fhash__discard( dynamic, V ) < 0 ) {
      CRITICAL( 0x243, "Framehash failed to discard topframe!" );
      return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
    }
    __arcvector_cell_set_no_arc( V );
    return 1;

  // Only 1 entry left in V. The single entry may be a Simple Arc or a Multiple Arc. Attempt conversion of V to Simple Arc.
  case 1:
    // NOTE: Conversion will only be performed if V's single entry is a Simple Arc, not if it's a Multiple Arc.
    {
      int compacted = _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( dynamic, V );
      if( compacted < 0 ) {
        CRITICAL( 0x242, "Conversion from array of arcs to simple arc failed!" );
      }
      return compacted;
    }
  // 
  default:
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __predicator_map_update_try_compactify( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, framehash_cell_t *original_map ) {
  int compacted_updated = 0;
  framehash_cell_t *current_map = __arcvector_as_frametop( MAV );
  int64_t n_active_MAV = iFramehash.access.SubtreeLength( current_map, dynamic, 2 );

  // Multiple items still left in the MAV, don't compactify just update reference in V
  if( n_active_MAV > 1 ) {
    // The predicator framehash structure has a different entry point, update the frametop reference in V
    if( current_map != original_map ) {
      // MAV references a different frame, update V
      vgx_Vertex_t *vertex = __arcvector_get_vertex( MAV );
      if( _vxarcvector_fhash__set_predicator_map( dynamic, V, vertex, current_map ) != 0 ) { // <= should UPDATE existing entry
        CRITICAL( 0x254, "Update of MAV entry in array of arcs failed" );
        compacted_updated = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
      }
    }
  }
  // Only 1 item left in MAV, convert by extracting it and enter as Simple Arc into V.
  else if( n_active_MAV == 1 ) {
    if( (compacted_updated = _vxarcvector_fhash__convert_multiple_arc_to_simple_arc( dynamic, V, MAV )) < 0 ) {
      // TODO: conversion failed when it shouldn't have - we will have deleted above but not converted.
      CRITICAL( 0x252, "Conversion from multiple arc to simple arc failed" );
    }
    // After MAV conversion, check if V should also be down-converted due to holding just one single arc
    else if( iFramehash.access.SubtreeLength( __arcvector_as_frametop( V ), dynamic, 2 ) == 1 ) {
      if( (compacted_updated = _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( dynamic, V )) < 0 ) {
        CRITICAL( 0x253, "Conversion from array of arcs to simple arc failed" );
      }
    }
  }
  // Zero items left in MAV,
  else {
    // Throw away the (empty) predicator map
    _vxarcvector_fhash__del_predicator_map( dynamic, V, MAV );
    // Try to compactify array of arcs
    if( (compacted_updated = __array_try_compactify( dynamic, V )) < 0 ) {
      CRITICAL( 0x257, "Framehash arcvector corrupted" );
    }
  }

  return compacted_updated;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __array_remove_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed ) {
  int del = 0;

  // We will remove all arcs in the multiple
  int64_t n_fwd = _vxarcvector_cellproc__count_nactive( MAV );

  if( n_fwd > 0 ) {
    // Disconnect other end first then proceed locally
    int64_t n_rev;
    if( (n_rev = disconnect_event( dynamic, arc, n_fwd, timing_budget )) > 0 ) {
      // Remove the multiple arc map from arc vector
      if( _vxarcvector_fhash__del_predicator_map( dynamic, V, MAV ) < 0 ) {
        CRITICAL( 0x241, "Framehash predicator map deletion failed!" );
        return __ARCVECTOR_ERROR( arc, NULL, MAV, timing_budget );
      }

      // Set number of removed arcs to the forward arcs (hopefully match reverse)
      *n_removed = n_fwd;
      del = 1;

      if( n_rev != n_fwd ) {
        CRITICAL( 0x242, "Framehash predicator map asymmetric arc removal! (out:%lld in:%lld)", n_fwd, n_rev );
      }

      // After complete removal of the MAV decrement the total degree and if new degree
      // after removal is 0 or 1 perform down-conversion of the array of arcs.
      if( __arcvector_dec_degree( V, n_fwd ) < 2 ) {
        if( __array_try_compactify( dynamic, V ) < 0 ) {
          del = __ARCVECTOR_ERROR( arc, NULL, V, timing_budget );
        }
      }

    }
    // Timeout
    else if( n_rev == 0 && _vgx_is_execution_halted( timing_budget ) ) {
      return -1;
    }
    // Error
    else {
      CRITICAL( 0x243, "Arc predicator map disconnect failed" );
      return __ARCVECTOR_ERROR( arc, NULL, MAV, timing_budget );
    }
  }

  return del;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __array_delete_specific_from_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *del_arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed ) {
  int del = 0;

  // At most one single arc removed from the multiple arc
  framehash_cell_t *original_map = __arcvector_as_frametop( MAV );

  // Try to remove specific predicator (REL and MOD specified, value may or may not be wildcard) 
  if( _vxarcvector_fhash__multiple_arc_has_key( dynamic, MAV, del_arc->head.predicator ) ) {
    // Disconnect opposite end first
    int64_t n_rev;
    if( (n_rev = disconnect_event( dynamic, del_arc, 1, timing_budget )) == 1 ) {
      if( (del = _vxarcvector_fhash__remove_key_from_multiple_arc( dynamic, MAV, del_arc->head.predicator )) != 1 ) {
        CRITICAL( 0x251, "Framehash multiple arc removal failed!" );
        return __ARCVECTOR_ERROR( del_arc, NULL, MAV, timing_budget );
      }
      (*n_removed)++;
      // Decrement the array of arcs degree field
      __arcvector_dec_degree( V, 1 );
      // Manage compaction of the MAV
      __predicator_map_update_try_compactify( dynamic, V, MAV, original_map );
    }
    // Timeout
    else if( n_rev == 0 && _vgx_is_execution_halted( timing_budget ) ) {
      return -1;
    }
    // Error
    else {
      CRITICAL( 0x252, "Disconnect error" );
      return __ARCVECTOR_ERROR( del_arc, NULL, MAV, timing_budget );
    }
  }

  return del;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_dispatch__array_add( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, f_Vertex_connect_event connect_event ) {
  // We're going to add arc A-(rel)->B to the array of arcs represented by a Framehash map structure.
  // V        : reference to the Framehash structure (this is really a member of vertex A, extracted out higher up and passed in here)
  // arc      : A-(rel)->B

  // Retrieve a cell from the map, keyed by arc's head vertex B. Three possible results:
  // WHITE  : no hit, i.e. no entry exists for B
  // BLUE   : Simple Arc, i.e. A-(x)->B already exists
  // GREEN  : Multiple Arc, i.e. A=(x,y,...)=>B already exists 
  vgx_ArcVector_cell_t arc_cell;
  _vxarcvector_fhash__get_arc_cell( dynamic, V, arc->head.vertex, &arc_cell );
#ifdef VGX_CONSISTENCY_CHECK
  if( __arcvector_has_framepointer( &arc_cell ) ) {
    _vxarcvector_fhash__trap_bad_frame_reference( &arc_cell );
  }
#endif

  // Now add (or overwrite) A-(rel)->B to the V structure
  int n_added = _vxarcvector_dispatch__set_arc( dynamic, V, &arc_cell, arc );  // 0 or 1 added

  // The only valid result is overwrite existing (n=0) or add one arc (n=1)
  if( (n_added == 1 || n_added == 0) && connect_event( dynamic, arc, n_added ) == n_added ) {
    return n_added;
  }
  else {
    return __ARCVECTOR_ERROR( arc, NULL, V, NULL );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __array_predicator_remove( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *entry, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed ) {

  // GREEN: entry is multiple arc
  if( __arcvector_cell_type( entry ) == VGX_ARCVECTOR_MULTIPLE_ARC ) {
    // MAV is completely removed
    if( _vgx_predicator_full_wildcard( arc->head.predicator ) ) {
      return __array_remove_multiple_arc( dynamic, V, entry, arc, timing_budget, disconnect_event, n_removed );
    }
    // Zero or one predicator removed from MAV
    else if( _vgx_predicator_specific( arc->head.predicator ) ) {
      return __array_delete_specific_from_multiple_arc( dynamic, V, entry, arc, timing_budget, disconnect_event, n_removed );
    }
    // Filtered deletion in MAV
    else {
      framehash_cell_t *original_map = __arcvector_as_frametop( entry );
      // Delete from predicator map and proceed with more processing if deletions or structure modification occurred
      int del = _vxarcvector_delete__delete_predicators( dynamic, entry, arc, timing_budget, disconnect_event, n_removed );
      if( del > 0 ) {
        // Decrement array of arc's degree field
        __arcvector_dec_degree( V, *n_removed ); // 1 or more removed
        // Try conversion
        __predicator_map_update_try_compactify( dynamic, V, entry, original_map );
      }
      return del;
    }
  }
  // BLUE: entry is simple arc
  else {
    return __array_delete_simple_arc( dynamic, V, entry, arc, timing_budget, disconnect_event, n_removed );
    // V may now be down-conveted from RED Vector to BLUE Simple
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_dispatch__array_remove( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_removed ) {
  // We're going to remove arc A-[pred]->B (where pred and/or B may be or contain wildcards) from the array of arcs represented by a Framehash map structure.
  // V        : reference to the Framehash structure (this is really a member of vertex A, extracted out higher up and passed in here)
  // arc      : A-[rel,mod,val]->B where rel, mod, val, B may be wildcard(s)
  
  int del = 0;

  // HEAD SPECIFIED ( ->B )
  if( arc->head.vertex != NULL ) {
    // Retrieve the cell representing B from the map, keyed by arc's head vertex B. Three possible results:
    // WHITE  : no hit, i.e. no entry exists for B (so nothing will be removed)
    // BLUE   : Simple Arc, i.e. A-(x)->B exists (so remove it if arc's predicator matches (x))
    // GREEN  : Multiple Arc, i.e. A=(x,y,...)=>B exists (so remove multiple arc if wildcard predicator, or specific predicator if given)
    vgx_ArcVector_cell_t entry;
    if( __arcvector_cell_type( _vxarcvector_fhash__get_arc_cell( dynamic, V, arc->head.vertex, &entry ) ) != VGX_ARCVECTOR_NO_ARCS ) {
      del = __array_predicator_remove( dynamic, V, &entry, arc, timing_budget, disconnect_event, n_removed );
    }
  }

  // HEAD WILDCARD ( -> * )
  else {
    del = _vxarcvector_delete__delete_arcs( dynamic, V, arc, timing_budget, disconnect_event, n_removed );
  }

  return del;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_dispatch.h"


test_descriptor_t _vgx_vxarcvector_dispatch_tests[] = {
  { "VGX Arcvector Dispatch Tests",     __utest_vxarcvector_dispatch },
  {NULL}
};
#endif
