/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_expire.c
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



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_arc_expiration {
  uint32_t now_ts;
  uint32_t next_ts;
  vgx_Arc_t arc;
  framehash_dynamic_t *dynamic;
  vgx_ArcVector_cell_t *V;
  int64_t n_deleted;
} __arc_expiration;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static uint16_t __relset_add( uint16_t relset[], uint16_t relenc ) {
  int key = relenc;
  do {
    int idx = key & 0x1f;       // select one of 32 slots in relset
    if( relset[ idx ] == 0 ) {  // slot available if 0
      relset[ idx ] = relenc;   // put relenc into the available slot
      return relenc;            // return the relenc to indicate success (never 0, by definition of relenc enum space)
    }
  } while( (key >>= 1) != 0 );  // selected slot not available, continue probing with other key bits until all 0
  return 0;                     // return 0 when no available slots
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static bool __relset_has( const uint16_t relset[], uint16_t relenc ) {
  int key = relenc;
  do {
    int idx = key & 0x1f;             // select one of 32 slots in relset
    if( relset[ idx ] == relenc ) {   // slot contains the relenc we're looking for
      return true;                    // return true to indicate match
    }
    else if( relset[ idx ] == 0 ) {   // slot is empty
      return false;                   // return false to indicate relenc does not exist in set
    }
  } while( (key >>= 1) != 0 );        // selected slot not matching, continue probing with other key bits until all 0
  return false;                       // return false when probing failed to find match
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __expire_relationship_WL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  uint16_t relenc = *(uint16_t*)processor->processor.input;
  vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
  // This arc is an expired relationship
  if( relenc == stored.rel.enc ) {
    // Force erase the framehash cell (NO STRUCTURE COMPACTIFICATION WILL OCCUR!)
    FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
    int64_t *n_deleted = (int64_t*)processor->processor.output;
    ++(*n_deleted);
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
 *         -1: error
 ***********************************************************************
 */
static int __expire_relationships_WL_to_iWL( uint16_t relset[], framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *arc_WL_to_iWL, int64_t *n_deleted ) {
  int modified = 0;
  framehash_cell_t *frame = __arcvector_get_frametop( MAV );

  int64_t n_del_all_rels = 0;

  if( !__vertex_is_locked_writable_by_current_thread( arc_WL_to_iWL->tail )
      ||
      !__vertex_is_locked_writable_by_current_thread( arc_WL_to_iWL->head.vertex ) ) {
    __ARCVECTOR_ERROR( arc_WL_to_iWL, NULL, NULL, NULL );
    vgx_Vertex_t *tail = arc_WL_to_iWL->tail;
    vgx_Vertex_t *head = arc_WL_to_iWL->head.vertex;
    CRITICAL( 0x999, "TAIL: %s", CALLABLE( tail )->IDPrefix( tail ) );
    PRINT_VERTEX( tail );
    CRITICAL( 0x999, "HEAD: %s", CALLABLE( head )->IDPrefix( head ) );
    PRINT_VERTEX( head );
    FATAL( 0x999, "Illegal lock state for arc!" );
  }


  // Arc removal has to be performed one relationship after another because of how
  // disconnect events work (and can't have OR-logic to cover multiple relationships)
  uint16_t relenc;
  for( int idx=0; idx<32; idx++ ) {
    if( (relenc = relset[idx]) == 0 ) {
      continue;
    }
    int64_t n_del_single_rel = 0;

    // --------------------------------------------------------------------------------------
    // -- This section must succeed as a unit, otherwise we have asymmetric removal and 
    // -- system will be irreparably broken.
    // --------------------------------------------------------------------------------------
    // Remove the forward arcs for this relationship
    framehash_processing_context_t expire_relationship = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __expire_relationship_WL );
    FRAMEHASH_PROCESSOR_MAY_MODIFY( &expire_relationship );
    FRAMEHASH_PROCESSOR_SET_IO( &expire_relationship, &relenc, &n_del_single_rel );
    int64_t n_forward = iFramehash.processing.ProcessNolock( &expire_relationship );

    // Remove the reverse arcs for this relationship
    arc_WL_to_iWL->head.predicator.rel.enc = relenc;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    __check_vertex_consistency_WL( arc_WL_to_iWL->tail );
    __check_vertex_consistency_WL( arc_WL_to_iWL->head.vertex );
    int64_t n_reverse = _vxgraph_arc__disconnect_WL_reverse_iWL( dynamic, arc_WL_to_iWL, n_del_single_rel, &zero_timeout );
    __arcvector_dec_degree( V, n_reverse );
    // --------------------------------------------------------------------------------------

    // Count
    n_del_all_rels += n_del_single_rel;
    
    // Check fwd/rev symmetry
    if( n_forward != n_reverse ) {
      CRITICAL( 0x671, "Asymmetric arc removal" );
      modified = __ARCVECTOR_ERROR( arc_WL_to_iWL, NULL, MAV, &zero_timeout ); // System is now broken
      break;
    }
  }

  // Did we delete anything?
  if( n_del_all_rels > 0 ) {
    // Adjust predicator map structure after direct cell deletion and flag modification if compactified
    if( _vxarcvector_fhash__compactify( dynamic, MAV ) < 0 ) {
      CRITICAL( 0x672, "Failed to compactify predicator map" );
    }

    // Finally increment the overall deletion count by the number of deleted arcs for this relationship and flag modification
    *n_deleted += n_del_all_rels;
    if( !modified ) { // protect error flag if set earlier
      modified = 1;
    }
  }

  // This will be 1 if compactification and/or deletion occurred
  return modified;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __find_expired_relationship_WL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  __arc_expiration *expirator = (__arc_expiration*)processor->processor.input;
  vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };

  // Is this a M_TMX arc?
  if( _vgx_predicator_is_expiration( stored ) ) {
    uint32_t tmx = stored.val.uinteger;
    // Has it expired?
    if( tmx <= expirator->now_ts ) {
      uint16_t *relset = (uint16_t*)processor->processor.output;
      // Add this relationship to the set and return
      if( __relset_add( relset, stored.rel.enc ) ) {
        return 1;
      }
      // Set full, we'll defer expiration until a future event
    }
    // Track the earliest next expiration timestamp so we can schedule another event
    if( tmx < expirator->next_ts ) {
      expirator->next_ts = tmx;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __expire_arc_WL_to_ANY( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  int64_t n_affected = 0;
  __arc_expiration *expirator = (__arc_expiration*)processor->processor.input;

  // Set current head vertex
  vgx_Vertex_t *head_vertex_ANY = (vgx_Vertex_t*)APTR_AS_ANNOTATION( fh_cell );

  // GREEN    Multiple Arc: FRAME
  if( __arcvector_fhash_is_multiple_arc( fh_cell ) ) {
    //          We need to forward the deletion to the multiple predicators deleter
    framehash_cell_t *predicator_map_pre = (framehash_cell_t*)APTR_GET_PTR56(fh_cell);

    // ---------------------------------
    // 1. Scan for expired relationships
    // ---------------------------------
    cacheline_t relset = {0};
    framehash_processing_context_t find_expired_relationships = FRAMEHASH_PROCESSOR_NEW_CONTEXT( predicator_map_pre, expirator->dynamic, __find_expired_relationship_WL );
    FRAMEHASH_PROCESSOR_SET_IO( &find_expired_relationships, expirator, relset.words );
    int64_t n_expired_rel = iFramehash.processing.ProcessNolock( &find_expired_relationships );

    // -------------------------------
    // 2. Delete expired relationships
    // -------------------------------
    if( n_expired_rel > 0 ) {
      vgx_Graph_t *self = expirator->arc.tail->graph;
      vgx_Vertex_t *head_iWL = NULL;
      vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( self );
      GRAPH_LOCK( self ) {
        // Let's get the head inarcs now so we know the expiration routine will not be blocked.
        // Ok to not yield tail's inarcs because we're doing a non-blocking acquisition.
        head_iWL = _vxgraph_state__lock_terminal_inarcs_writable_CS( self, head_vertex_ANY, &zero_timeout );
      } GRAPH_RELEASE;
      // Got the lock, proceed
      if( head_iWL ) {
        int64_t n_deleted = 0;
        // Load info into temporary arcvector cell for MAV processing, then run MAV expiration
        vgx_ArcVector_cell_t entry;
        __arcvector_cell_set_multiple_arc( &entry, head_iWL, predicator_map_pre );
        vgx_Arc_t del_multiarc_WL_to_iWL = {
          .tail     = expirator->arc.tail,
          .head     = {
            .vertex     = head_iWL,
            .predicator = VGX_PREDICATOR_ANY_OUT  // will get rel.enc set inside expiration function
          }
        };
        if( __expire_relationships_WL_to_iWL( relset.words, expirator->dynamic, expirator->V, &entry, &del_multiarc_WL_to_iWL, &n_deleted ) > 0 ) {
          expirator->n_deleted += n_deleted;
          _vxarcvector_cellproc__update_MAV( expirator->dynamic, &entry, predicator_map_pre, processor, fh_cell );
          n_affected = 1;
        }
        GRAPH_LOCK( self ) {
          _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( self, &head_iWL );
        } GRAPH_RELEASE;
      }
      // Head is busy, re-schedule
      else {
        // Ok to schedule next for immediate execution because we already verified some arcs have already expired
        expirator->next_ts = expirator->now_ts;
      }
    }
  }
  // Simple Arc: PREDICATOR
  else {
    vgx_predicator_t stored = { .data = APTR_AS_UNSIGNED( fh_cell ) };
    // Does our simple arc have a M_TMX modifier?
    if( _vgx_predicator_is_expiration( stored ) ) {
      int64_t n_deleted = 0;
      uint32_t tmx = stored.val.uinteger;
      // Has it expired?
      if( tmx <= expirator->now_ts ) {
        vgx_Graph_t *self = expirator->arc.tail->graph;
        vgx_Vertex_t *head_iWL = NULL;
        vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( self );
        GRAPH_LOCK( self ) {
          // Let's get the head inarcs now so we know the expiration routine will not be blocked.
          // Ok to not yield tail's inarcs because we're doing a non-blocking acquisition.
          head_iWL = _vxgraph_state__lock_terminal_inarcs_writable_CS( self, head_vertex_ANY, &zero_timeout );
        } GRAPH_RELEASE;
        // Got the inarcs
        if( head_iWL ) {
          // Update arc with an exact rel_mod key force direct removal on the other side of the disconnect event
          expirator->arc.head.predicator = stored;
          expirator->arc.head.vertex = head_iWL;
          // Reverse disconnect (NON-BLOCKING!)
          if( (n_deleted = _vxgraph_arc__disconnect_WL_reverse_iWL( expirator->dynamic, &expirator->arc, 1, false )) == 1 ) {
            // Reverse delete successful.
            // Now force erase the framehash cell to complete the deletion.
            FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
            __arcvector_dec_degree( expirator->V, 1 );
            expirator->n_deleted++;
            n_affected = 1;
          }
          GRAPH_LOCK( self ) {
            _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( self, &head_iWL );
          } GRAPH_RELEASE;
        }
      }
      // Either not yet expired or we tried to delete but failed to acquire head's inarcs (non-blocking)
      if( n_deleted == 0 ) {
        // Track the earliest next expiration timestamp so we can schedule another event
        if( tmx < expirator->next_ts ) {
          expirator->next_ts = tmx;
        }
      }
    }
  }

  return n_affected;
}



/*******************************************************************//**
 *
 * Returns:
 *          1: if array of arcs was modified (deleted from and/or compactified)
 *          0: if no changes were made (no deletions, no compactification)
 *         -1: error
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_expire__expire_arcs( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *vertex, uint32_t now_ts, uint32_t *next_ts, int64_t *n_deleted ) {
  int modified = 0;

  framehash_cell_t *frame = __arcvector_get_frametop( V );
  __arc_expiration expirator = {
    .now_ts       = now_ts,
    .next_ts      = TIME_EXPIRES_NEVER,
    .arc          = {
      .tail         = vertex,
      .head         = {
        .predicator = VGX_PREDICATOR_NONE,  // will be updated by the cell
        .vertex     = NULL                  // processor as needed
      }
    },
    .dynamic      = dynamic,
    .V            = V,
    .n_deleted    = 0,     // number of arcs deleted, including individual arc in multiple arcs
  };
  
  framehash_processing_context_t expire_arcs = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __expire_arc_WL_to_ANY );
  FRAMEHASH_PROCESSOR_SET_IO( &expire_arcs, &expirator, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &expire_arcs );

  // Perform direct removal of all expired arcs (and inner multiple arcs)
  iFramehash.processing.ProcessNolock( &expire_arcs );
  *n_deleted = expirator.n_deleted;

  // Adjust array of arcs structure after direct cell deletion and flag modification if compaction occurred
  if( (modified = _vxarcvector_fhash__compactify( dynamic, V )) < 0 ) {
    CRITICAL( 0x681, "Failed to compactify array of arcs" );
  }
  // Flag modification if any deletions occurred
  else if( *n_deleted > 0 ) {
    modified = 1;

    int64_t n_remain = __arcvector_get_degree( V );

    // Convert array of arcs to simple arc if only one arc remaining
    if( n_remain == 1 ) {
      if( _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( dynamic, V ) < 0 ) {
        CRITICAL( 0x682, "Conversion from array of arcs to simple arc failed" );
        modified = __ARCVECTOR_ERROR( NULL, vertex, V, NULL );
      }
    }
    // Discard the whole thing and mark as empty if no arcs remaining
    else if( n_remain == 0 ) {
      _vxarcvector_fhash__discard( dynamic, V );
      __arcvector_cell_set_no_arc( V );
    }
  }

  // Return the earliest timestamp for any remaining M_TMX arcs
  if( expirator.next_ts < TIME_EXPIRES_NEVER ) {
    *next_ts = expirator.next_ts;
  }

  return modified;

}





/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_expire.h"


test_descriptor_t _vgx_vxarcvector_expire_tests[] = {
  { "Arcvector Expiration",     __utest_vxarcvector_expire },
  {NULL}
};
#endif
