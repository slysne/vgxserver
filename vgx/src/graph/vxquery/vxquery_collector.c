/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_collector.c
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

#include "_vxcollector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static void __delete_search_ranker_context( vgx_Ranker_t **ranker );
static vgx_Ranker_t * __new_search_ranker_context( const vgx_ranking_context_t *ranking_context );

static vgx_VertexRef_t * __new_vertex_reference_map( int64_t collector_size, int64_t *mapsz );
static int64_t __destroy_vertex_reference_map( vgx_Graph_t *graph, vgx_VertexRef_t **refmap, int64_t mapsz );

static vgx_ArcCollector_context_t * __new_sorted_list_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query );
static vgx_ArcCollector_context_t * __new_unsorted_list_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query );
static vgx_ArcCollector_context_t * __new_aggregation_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query );
static vgx_ArcCollector_context_t * __new_null_arc_collector( vgx_Graph_t *graph, vgx_BaseQuery_t *query );
static vgx_VertexCollector_context_t * __new_sorted_list_vertex_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query );
static vgx_VertexCollector_context_t * __new_unsorted_list_vertex_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query );
static vgx_VertexCollector_context_t * __new_null_vertex_collector( vgx_Graph_t *graph, vgx_BaseQuery_t *query );

static void _vxquery_collector__delete_collector( vgx_BaseCollector_context_t **collector );
static vgx_ArcCollector_context_t * _vxquery_collector__new_arc_collector( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts );
static vgx_VertexCollector_context_t * _vxquery_collector__new_vertex_collector( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts );
static vgx_BaseCollector_context_t * _vxquery_collector__convert_to_base_list_collector( vgx_BaseCollector_context_t *collector );
static vgx_BaseCollector_context_t * _vxquery_collector__trim_base_list_collector( vgx_BaseCollector_context_t *collector, int64_t n_collected, int offset, int64_t hits );
static int64_t _vxquery_collector__transfer_base_list( vgx_ranking_context_t *ranking_context, vgx_BaseCollector_context_t **src, vgx_BaseCollector_context_t **dest );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IGraphCollector_t iGraphCollector = {
  .DeleteCollector                    = _vxquery_collector__delete_collector,
  .NewArcCollector                    = _vxquery_collector__new_arc_collector,
  .NewVertexCollector                 = _vxquery_collector__new_vertex_collector,
  .ConvertToBaseListCollector         = _vxquery_collector__convert_to_base_list_collector,
  .TrimBaseListCollector              = _vxquery_collector__trim_base_list_collector,
  .TransferBaseList                   = _vxquery_collector__transfer_base_list,
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_search_ranker_context( vgx_Ranker_t **ranker ) {
  if( *ranker ) {
    free( *ranker );
    *ranker = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Ranker_t * __new_search_ranker_context( const vgx_ranking_context_t *ranking_context ) {
  vgx_Ranker_t *ranker = calloc( 1, sizeof( vgx_Ranker_t ) );
  if( ranker ) {
    ranker->arc_score = __get_arc_rank_scorer_function( ranking_context );
    ranker->vertex_score = __get_vertex_rank_scorer_function( ranking_context );
    if( ranking_context ) {
      ranker->simcontext = ranking_context->simcontext;             // BORROW the simcontext from ranking_context
      ranker->probe = ranking_context->vector;                      // BORROW the vector from ranking_context
      ranker->locked_head_access = false;                           // Mayve set later: Graph is readonly for ranker lifetime
      ranker->graph = ranking_context->graph;                       //
      ranker->evaluator = ranking_context->evaluator;               // BORROW the evalcontext from ranking_context
      ranker->timing_budget = ranking_context->timing_budget;       //

      // Head vertex must be locked whenever dereferenced, unless graph is
      // readonly and then we acquire another graph readonly lock while lasts
      // until the ranker is destroyed.
      if( ranking_context->readonly_graph == false ) {
        if( (ranking_context->sortspec & _VGX_SORTBY_DEREF_MASK) || (ranker->evaluator && CALLABLE( ranker->evaluator )->HeadDeref( ranker->evaluator )) ) {
          ranker->locked_head_access = true;
        }
      }
    }
  }
  return ranker;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexRef_t * __new_vertex_reference_map( int64_t collector_size, int64_t *mapsz ) {
  vgx_VertexRef_t *refmap;
  // Create the vertex reference map.
  // Size is a power of 2 to simplify calculation of hash keys.
  // Size must be able to accommodate all unique tails and heads (worst case) plus two
  // additional tail + head in case of heaps with a discarded collector item that
  // needs processing. However, we go higher than this too to avoid fill rate close to 100%
  // worst case. We add 50% to minimum size and then use the nearest higher power of 2 for size.
  int64_t min_sz = 2 * collector_size + 2;
  int64_t sz = 1LL << imag2( (int64_t)(1.5 * min_sz) );
  if( (refmap = calloc( sz, sizeof( vgx_VertexRef_t ) )) == NULL ) {
    return NULL;
  }
  *mapsz = sz;
  return refmap;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __destroy_vertex_reference_map( vgx_Graph_t *graph, vgx_VertexRef_t **refmap, int64_t mapsz ) {
  int64_t n_released = 0;
  if( refmap && *refmap ) {
    vgx_VertexRef_t *cursor = *refmap;
    vgx_VertexRef_t *end = *refmap + mapsz;
    // Make sure any still-acquired vertices are released now
    while( cursor < end ) {
      if( cursor->slot.locked > 0 ) {
        goto continue_locked;
      }
      ++cursor;
    }
    goto completed;

continue_locked:
    // Only here if any locked slots were found in the map.
    // Now we have to lock the graph and release locks.
    GRAPH_LOCK( graph ) {
      while( cursor < end ) {
        if( cursor->slot.locked > 0 ) {
          _vxgraph_state__unlock_vertex_CS_LCK( graph, &cursor->vertex, VGX_VERTEX_RECORD_OPERATION );
          ++n_released;
        }
        ++cursor;
      }
    } GRAPH_RELEASE;

completed:
    free( *refmap );
    *refmap = NULL;
  }
  return n_released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VertexRef_t * _vxquery_collector__add_vertex_reference( vgx_BaseCollector_context_t *collector, vgx_Vertex_t *vertex, vgx_VertexRefLock_t *ext_lock ) {
#define JUMP_STATE( Label ) goto Label
  int64_t sz = collector->sz_refmap;   // assumption: power of 2
  uint64_t mask = sz - 1;              //   x % sz == x & mask
  vgx_VertexRef_t *base = collector->refmap;
  uint64_t offset = __vertex_get_index( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( vertex ) );
  vgx_VertexRef_t *slot = base + (offset & mask);
  vgx_VertexRef_t *orig;
  vgx_VertexRef_t *first_available;
  vgx_VertexRef_t *last_slot;
  uint64_t hash;

  switch( slot->slot.state ) {
  // Virgin slot: Occupy, steal lock state and set refcnt to 1
  case VGX_VERTEXREF_STATE_VIRGIN:
  OCCUPY_SLOT:
    slot->vertex = vertex;
    slot->refcnt = 1;
    slot->slot.state = VGX_VERTEXREF_STATE_OCCUPIED;
    if( *ext_lock > 0 ) {
      slot->slot.locked = 1;  // Steal one lock
      --(*ext_lock);          // from the outside
    }
    return slot;
  // Previously occupied but now available slot: Remember slot as first available
  case VGX_VERTEXREF_STATE_AVAILABLE:
    // Re-occupy this slot since no other vertex claimed it while available
    if( (first_available = slot)->vertex == vertex ) {
      JUMP_STATE( OCCUPY_SLOT );
    }
    else {
      hash = ihash64( offset ); // scramble
      orig = slot;
      last_slot = base + mask;
      JUMP_STATE( NEXT_SLOT );
    }
  // Occupied slot: If vertex match, increment refcnt
  case VGX_VERTEXREF_STATE_OCCUPIED:
    // Vertex match: increment refcnt
    if( slot->vertex == vertex ) {
    INCREF_SLOT:
      slot->refcnt++;
      // Previously not locked but now lock is required, steal one lock from the outside
      if( *ext_lock > 0 && slot->slot.locked == 0 ) {
        slot->slot.locked = 1;  // Steal one lock
        --(*ext_lock);          // from the outside
      }
      return slot;
    }
    else {
      first_available = NULL;
      hash = ihash64( offset ); // scramble
      orig = slot;
      last_slot = base + mask;
      JUMP_STATE( NEXT_SLOT );
    }
  // Error
  default:
    return NULL;
  }

  // ----------
  // Probe loop
  // ----------

PROBE:
  switch( slot->slot.state ) {
  // Virgin slot after probe: Occupy this slot unless an available slot
  // was encountered earlier which is then occupied instead.
  // Steal the lock state (may be 0 or 1), initialize refcount to 1 and
  // return slot.
  // (It is guaranteed the vertex does not exist further down the probing
  // since there is no way for the vertex to have gotten past this slot
  // before and slots can never return to virgin state after having been
  // occupied by another vertex that was later deleted.)
  case VGX_VERTEXREF_STATE_VIRGIN:
    // Rewind to previously encountered available slot if one exists.
    if( first_available ) {
      slot = first_available;
    }
    // Now occupy the slot and return it
    JUMP_STATE( OCCUPY_SLOT );
  // Available slot after probe: We may find the vertex at this slot,
  // which means it previously occupied the slot but was then deleted.
  // This means the vertex does not exist beyond this slot and we can
  // re-occupy the slot. If the vertex does not match it means our
  // vertex may exist further down the probing chain and so we have to
  // continue probing. If this is the first available slot encountered
  // record the slot as first available.
  case VGX_VERTEXREF_STATE_AVAILABLE:
    // Re-occupy this slot since no other vertex claimed it while available.
    if( slot->vertex == vertex ) {
      JUMP_STATE( OCCUPY_SLOT );
    }
    // This is the first available slot, remember it.
    else if( first_available == NULL ) {
      first_available = slot;
    }
    // Continue probing
    JUMP_STATE( NEXT_SLOT );
  // Slot occupied after probe. We may find the vertex at this slot,
  // which means probing should be terminated and we incref.
  // Otherwise continue probing.
  case VGX_VERTEXREF_STATE_OCCUPIED:
    // Vertex match after probe: Increment refcnt and return slot.
    if( slot->vertex == vertex ) {
      JUMP_STATE( INCREF_SLOT );
    }
    // Continue probing.
    else {
      JUMP_STATE( NEXT_SLOT );
    }
  // ERROR
  default:
    return NULL;
  }

  // Collision after probe: Continue probing
NEXT_SLOT:
  // Probe using shifted bits from original offset as long as 1s exist in hash
  if( hash != 0 ) {
    // Compute next slot based on different hash and accept next slot
    // if different from current slot, otherwise keep refreshing hash.
    hash >>= 1; // could go to zero
    // Use hash plus (unique) offset plus 1 to compute next slot.
    // We add 1 to offset to avoid re-probing original slot.
    vgx_VertexRef_t *next_slot = base + ( (hash + offset + 1) & mask );
    // Not acceptable, try again.
    if( next_slot == slot ) {
      JUMP_STATE( NEXT_SLOT );
    }
    // Computed new slot, now probe it.
    else {
      slot = next_slot;
      JUMP_STATE( PROBE );
    }
  }
  // Hash bits have been exhausted, we are now probing linearly starting 
  // from the point where we gave up hashing, wrapping at most once.
  else if( slot != orig ) {
    if( ++slot > last_slot ) {
      slot = base; // wrap
    }
    JUMP_STATE( PROBE );
  }
  // We have probed beyond the array without finding ourselves,
  // any virgin slots, or any available slots that used to belong to us.
  // If an available slot was found along the way, we now occupy it
  else if( (slot = first_available) != NULL ) {
    JUMP_STATE( OCCUPY_SLOT );
  }
  // No available slots, vertex cannot be added to map.
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * CALLER IS RESPONSIBLE FOR RELEASING ANY CS LOCK ACQUIRED BY THIS FUNCTION
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxquery_collector__del_vertex_reference_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_VertexRef_t *vertexref, vgx_Graph_t **locked_graph ) {
  // Negative refcount: no action
  if( vertexref == NULL || vertexref->refcnt < 0 ) {
    return -1;
  }

  // Decrement refcount, then release vertex lock and free slot when refcount goes to zero
  if( --(vertexref->refcnt) == 0 ) {
    // First release vertex if locked
    if( vertexref->slot.locked > 0 ) {
      vgx_Graph_t *graph = collector->graph;
      vgx_Vertex_t *vertex = vertexref->vertex;
      // Not already locked, do it now
      if( *locked_graph == NULL ) {
        *locked_graph = GRAPH_ENTER_CRITICAL_SECTION( graph );
      }
      _vxgraph_state__unlock_vertex_CS_LCK( graph, &vertex, VGX_VERTEX_RECORD_OPERATION );
      vertexref->slot.locked = 0;
    }
    vertexref->slot.state = VGX_VERTEXREF_STATE_AVAILABLE;
    // NOTE: We do not reset the vertex to NULL, because this slot may become
    //       re-occupied by the same vertex later.
  }

  return vertexref->refcnt;
}



/*******************************************************************//**
 *
 * CALLER IS RESPONSIBLE FOR RELEASING ANY CS LOCK ACQUIRED BY THIS FUNCTION
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxquery_collector__safe_tail_access_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_Graph_t **locked_graph ) {
  static const int TWO_LOCKS = 2;
  // Acquire the tail readonly if needed
  if( collector->locked_tail_access && larc->acquired.tail_lock < TWO_LOCKS ) { // <- TWO!
    vgx_Graph_t *graph = collector->graph;
    // Not already locked, do it now
    if( *locked_graph == NULL ) {
      *locked_graph = GRAPH_ENTER_CRITICAL_SECTION( graph ); 
    }
    if( _vxgraph_state__lock_vertex_readonly_CS( collector->graph, larc->tail, collector->timing_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {
      larc->acquired.tail_lock++;
    }
    else {
      collector->timing_budget->resource = larc->tail;
      return NULL; // timeout
    }
  }
  return larc->tail;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxquery_collector__safe_head_access_OPEN( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  static const int ONE_LOCK = 1;
  // Acquire the head readonly if needed
  if( collector->locked_head_access && larc->acquired.head_lock < ONE_LOCK ) {
    if( _vxgraph_state__lock_vertex_readonly_OPEN( collector->graph, larc->head.vertex, collector->timing_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {
      larc->acquired.head_lock = 1;
    }
    else {
      return NULL; // timeout
    }
  }
  return larc->head.vertex;
}



/*******************************************************************//**
 *
 * CALLER IS RESPONSIBLE FOR RELEASING ANY CS LOCK ACQUIRED BY THIS FUNCTION
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxquery_collector__safe_head_access_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_Graph_t **locked_graph ) {
  // Acquire the head readonly if needed
  if( collector->locked_head_access && larc->acquired.head_lock < 1 ) {
    vgx_Graph_t *graph = collector->graph;
    // Not already locked, do it now
    if( *locked_graph == NULL ) {
      *locked_graph = GRAPH_ENTER_CRITICAL_SECTION( graph );
    }
    if( _vxgraph_state__lock_vertex_readonly_CS( collector->graph, larc->head.vertex, collector->timing_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {
      larc->acquired.head_lock = 1;
    }
    else {
      collector->timing_budget->resource = larc->head.vertex;
      return NULL; // timeout
    }
  }
  return larc->head.vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __locked_arc_access( bool readonly_graph, vgx_BaseQuery_t *query, bool *locked_tail, bool *locked_head ) {
  //
  // TODO: REPLACE WITH A TRUSTED PARAMETER TO THIS FUNCTION 
  //       WHICH INDICATES GUARANTEED READONLY FOR THE
  //       LIFETIME OF THE COLLECTOR
  //
  //
  int locked = 0;
  vgx_ResponseAttrFastMask fieldmask = vgx_query_response_attr_fastmask( query );
  // Vertices will be dereferenced
  if( fieldmask & VGX_RESPONSE_ATTRS_DEREF ) {
    // TODO:  Don't lock unnecessarily if property selector includes no-deref items only!
    // Graph is not readonly - we have to lock head
    if( readonly_graph == false ) {
      // Tails must be locked
      if( fieldmask & VGX_RESPONSE_ATTRS_TAIL_DEREF ) {
        *locked_tail = true;
      }
      // Heads must be locked if fields other than properties or property fields require head deref
      if( vgx_query_response_head_deref( query ) ) {
        *locked_head = true;
      }
      locked = 1;
    }
  }
  return locked;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_collector_stage( vgx_CollectorStage_t **stage ) {
  if( stage && *stage ) {
    ALIGNED_FREE( *stage );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_CollectorStage_t * __new_collector_stage( void ) {
  vgx_CollectorStage_t *stage = NULL;
  if( CALIGNED_MALLOC( stage, vgx_CollectorStage_t ) != NULL ) {
    memset( stage->slot, 0, sizeof( vgx_CollectorStage_t ) );
  }
  return stage;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * __new_sorted_list_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query ) {
  // The heap will be initialized values of lowest possible rank (last in sorted result)

  // Create the collector context
  vgx_ArcCollector_context_t *top_k_collector = NULL;
  Cm256iHeap_t *heap = NULL;
  vgx_VertexRef_t *refmap = NULL;
  vgx_Ranker_t *ranker = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    vgx_CollectorItem_t empty = {0};

    if( (top_k_collector = calloc( 1, sizeof(vgx_ArcCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x321 );
    }

    // We will collect using a heap
    Cm256iHeap_constructor_args_t heap_args = {
      .element_capacity = size,
      .comparator = (f_Cm256iHeap_comparator_t)__get_arc_comparator( ranking_context, &empty )
    };

    // Create the new heap
    if( (heap = COMLIB_OBJECT_NEW( Cm256iHeap_t, NULL, &heap_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x322 );
    }

    // Create vertex reference map
    if( (refmap = __new_vertex_reference_map( size, &top_k_collector->sz_refmap )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x323 );
    }

    // Create the ranker
    if( (ranker = __new_search_ranker_context( ranking_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x324 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x325 );
    }

    bool locked_tail = false;
    bool locked_head = false;
    __locked_arc_access( ranking_context->readonly_graph, query, &locked_tail, &locked_head );


    // Initialize the collector context
    top_k_collector->type                     = VGX_COLLECTOR_TYPE_SORTED_ARC_LIST;
    top_k_collector->graph                    = graph;
    top_k_collector->ranker                   = ranker;
    top_k_collector->container.sequence.heap  = heap;
    top_k_collector->refmap                   = refmap;
    top_k_collector->stage                    = stage;
    top_k_collector->postheap                 = NULL,
    top_k_collector->size                     = size;
    top_k_collector->n_remain                 = LLONG_MAX;
    top_k_collector->n_collectable            = 0;
    top_k_collector->locked_tail_access       = locked_tail;
    top_k_collector->locked_head_access       = locked_head;
    top_k_collector->fieldmask                = vgx_query_response_attr_fastmask( query );
    top_k_collector->timing_budget            = vgx_query_timing_budget( query );
    __get_arc_collector_functions( ranking_context, &top_k_collector->stage_arc, &top_k_collector->collect_arc );
    top_k_collector->post_collect             = _iCollectArc.no_collect;
    top_k_collector->post_stage               = _iStageArc.no_stage;
    top_k_collector->postfilter               = NULL;
    top_k_collector->n_arcs                   = 0;
    top_k_collector->n_neighbors              = 0;
    top_k_collector->counts_are_deep          = true;
    
    // Initialize the heap with "lowest" values
    CALLABLE(heap)->Initialize( heap, &empty.item, size );
  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&top_k_collector );
  }
  XFINALLY {
  }

  // The collector context is now ready
  return top_k_collector;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * __new_unsorted_list_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query ) {

  // Create the collector context
  vgx_ArcCollector_context_t *collector = NULL;
  Cm256iList_t *list = NULL;
  vgx_VertexRef_t *refmap = NULL;
  vgx_Ranker_t *ranker = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    if( (collector = calloc( 1, sizeof(vgx_ArcCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x331 );
    }

    // We will collect using a list (no sort order defined)
    Cm256iList_constructor_args_t list_args = {
      .element_capacity = size,
      .comparator = NULL
    };

    // Create the new list
    if( (list = COMLIB_OBJECT_NEW( Cm256iList_t, NULL, &list_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x332 );
    }

    // Create refmap
    if( (refmap = __new_vertex_reference_map( size, &collector->sz_refmap )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x333 );
    }

    // Create the ranker
    if( (ranker = __new_search_ranker_context( ranking_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x334 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x335 );
    }

    bool locked_tail = false;
    bool locked_head = false;
    __locked_arc_access( ranking_context->readonly_graph, query, &locked_tail, &locked_head );


    // Initialize the collector context
    collector->type                     = VGX_COLLECTOR_TYPE_UNSORTED_ARC_LIST;
    collector->graph                    = graph;
    collector->ranker                   = ranker;
    collector->container.sequence.list  = list;
    collector->refmap                   = refmap;
    collector->stage                    = stage;
    collector->postheap                 = NULL,
    collector->size                     = size;
    collector->n_remain                 = size;
    collector->n_collectable            = 0;
    collector->locked_tail_access       = locked_tail;
    collector->locked_head_access       = locked_head;
    collector->fieldmask                = vgx_query_response_attr_fastmask( query );
    collector->timing_budget            = vgx_query_timing_budget( query );
    __get_arc_collector_functions( ranking_context, &collector->stage_arc, &collector->collect_arc );
    collector->post_collect             = _iCollectArc.no_collect;
    collector->post_stage               = _iStageArc.no_stage;
    collector->postfilter               = NULL;
    collector->n_arcs                   = 0;
    collector->n_neighbors              = 0;
    collector->counts_are_deep          = false;

  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&collector );
  }
  XFINALLY {}

  // The collector context is now ready
  return collector;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * __new_aggregation_arc_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query ) {
  // We will collect into a framehash map, which naturally deduplicates and aggregates the collected arc values.
  // When consumed by new_list_consume_collector, values are extracted and sorted at that point.

  vgx_ArcCollector_context_t *map_collector = NULL;
  framehash_t *fhmap = NULL;
  Cm256iHeap_t *postheap = NULL;
  vgx_VertexRef_t *refmap = NULL;
  vgx_Ranker_t *ranker = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    if( (map_collector = calloc( 1, sizeof( vgx_ArcCollector_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x341 );
    }

    // Create the aggregation map
    framehash_constructor_args_t args = FRAMEHASH_DEFAULT_ARGS;
    args.param.order = 3;
    if( (fhmap = COMLIB_OBJECT_NEW( framehash_t, NULL, &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x342 );
    }

    // Create vertex reference map
    if( (refmap = __new_vertex_reference_map( size, &map_collector->sz_refmap )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x343 );
    }
    
    // Create the ranker
    if( (ranker = __new_search_ranker_context( ranking_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x344 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x345 );
    }

    vgx_CollectorType_t type;
    if( _vgx_sortby( ranking_context->sortspec ) ) {
      type = VGX_COLLECTOR_TYPE_SORTED_ARC_AGGREGATION;
      vgx_CollectorItem_t empty = {0};
      // We will post-process using a heap, in order to extract the top hits from the aggregation map
      Cm256iHeap_constructor_args_t heap_args = {
        .element_capacity = size,
        .comparator = (f_Cm256iHeap_comparator_t)__get_arc_comparator( ranking_context, &empty )
      };

      // Create the new heap for post-collection sorting
      if( (postheap = COMLIB_OBJECT_NEW( Cm256iHeap_t, NULL, &heap_args )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x346 );
      } 

      // Initialize the heap with "lowest" values
      CALLABLE( postheap )->Initialize( postheap, &empty.item, size );

    }
    else {
      type = VGX_COLLECTOR_TYPE_UNSORTED_ARC_AGGREGATION;
    }

    bool locked_tail = false;
    bool locked_head = false;
    __locked_arc_access( ranking_context->readonly_graph, query, &locked_tail, &locked_head );

    // Initialize the collector context
    map_collector->type                         = type;
    map_collector->graph                        = graph;
    map_collector->ranker                       = ranker;
    map_collector->container.mapping.aggregator = fhmap;
    map_collector->refmap                       = refmap;
    map_collector->stage                        = stage;
    map_collector->postheap                     = postheap;
    map_collector->size                         = size;
    map_collector->n_remain                     = LLONG_MAX;
    map_collector->n_collectable                = 0;
    map_collector->locked_tail_access           = locked_tail;
    map_collector->locked_head_access           = locked_head;
    map_collector->fieldmask                    = vgx_query_response_attr_fastmask( query );
    map_collector->timing_budget                = vgx_query_timing_budget( query );
    __get_arc_collector_functions( ranking_context, &map_collector->stage_arc, &map_collector->collect_arc );
    __get_arc_collector_functions_by_sortspec( ranking_context->sortspec, ranking_context->modifier, &map_collector->post_stage, &map_collector->post_collect );
    map_collector->postfilter                   = ranking_context->postfilter_context;
    map_collector->n_arcs                       = 0;
    map_collector->n_neighbors                  = 0;
    map_collector->counts_are_deep              = true;

    //

  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&map_collector );
  }
  XFINALLY {
  }

  return map_collector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * __new_null_arc_collector( vgx_Graph_t *graph, vgx_BaseQuery_t *query ) {

  // Create the collector context
  vgx_ArcCollector_context_t *collector = NULL;
  vgx_CollectorStage_t *stage = NULL;
  
  XTRY {
    if( (collector = calloc( 1, sizeof(vgx_ArcCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x351 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x352 );
    }

    // Initialize the collector context
    collector->type               = VGX_COLLECTOR_TYPE_NONE;
    collector->graph              = graph;
    collector->ranker             = NULL;
    collector->container.object   = NULL;
    collector->refmap             = NULL;
    collector->sz_refmap          = 0;
    collector->stage              = stage;
    collector->postheap           = NULL;
    collector->size               = 0;
    collector->n_remain           = LLONG_MAX;
    collector->n_collectable      = 0;
    collector->locked_tail_access = false;
    collector->locked_head_access = false;
    collector->fieldmask          = VGX_RESPONSE_ATTRS_NONE;
    collector->timing_budget      = vgx_query_timing_budget( query );
    __get_arc_collector_functions( NULL, &collector->stage_arc, &collector->collect_arc );
    collector->post_collect       = _iCollectArc.no_collect;
    collector->post_stage         = _iStageArc.no_stage;
    collector->postfilter         = NULL;
    collector->n_arcs             = 0;
    collector->n_neighbors        = 0;
    collector->counts_are_deep    = false;

  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&collector );
  }
  XFINALLY {}

  // The collector context is now ready
  return collector;


}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCollector_context_t * __new_sorted_list_vertex_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query ) {
  // The heap will be initialized values of lowest possible rank (last in sorted result)

  // Create the collector context
  vgx_VertexCollector_context_t *top_k_collector = NULL;
  Cm256iHeap_t *heap = NULL;
  vgx_VertexRef_t *refmap = NULL;
  vgx_Ranker_t *ranker = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    // Make sure size is within bounds
    int64_t max_capacity = Comlib_Cm256iHeap_t_ElementCapacity();
    if( size > max_capacity ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x361 );
    }

    vgx_CollectorItem_t empty = {0};

    if( (top_k_collector = calloc( 1, sizeof(vgx_VertexCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x362 );
    }

    // We will collect using a heap
    Cm256iHeap_constructor_args_t heap_args = {
      .element_capacity = size,
      .comparator = (f_Cm256iHeap_comparator_t)__get_vertex_comparator( ranking_context, &empty )
    };

    // Create the new heap
    if( (heap = COMLIB_OBJECT_NEW( Cm256iHeap_t, NULL, &heap_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x363 );
    }

    // Create the vertex reference map
    if( (refmap = __new_vertex_reference_map( size, &top_k_collector->sz_refmap )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x364 );
    }

    // Create the ranker
    if( (ranker = __new_search_ranker_context( ranking_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x365 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x366 );
    }

    bool locked_tail = false;
    bool locked_head = false;
    __locked_arc_access( ranking_context->readonly_graph, query, &locked_tail, &locked_head );

    // Initialize the collector context
    top_k_collector->type                     = VGX_COLLECTOR_TYPE_SORTED_VERTEX_LIST;
    top_k_collector->graph                    = graph;
    top_k_collector->ranker                   = ranker;
    top_k_collector->container.sequence.heap  = heap;
    top_k_collector->refmap                   = refmap;
    top_k_collector->stage                    = stage;
    top_k_collector->postheap                 = NULL;
    top_k_collector->size                     = size;
    top_k_collector->n_remain                 = LLONG_MAX;
    top_k_collector->n_collectable            = 0;
    top_k_collector->locked_tail_access       = locked_tail;
    top_k_collector->locked_head_access       = locked_head;
    top_k_collector->fieldmask                = vgx_query_response_attr_fastmask( query );
    top_k_collector->timing_budget            = vgx_query_timing_budget( query );
    __get_vertex_collector_functions( ranking_context, &top_k_collector->stage_vertex, &top_k_collector->collect_vertex );
    top_k_collector->n_vertices               = 0;
    top_k_collector->counts_are_deep          = true;
    
    // Initialize the heap with "lowest" values
    CALLABLE(heap)->Initialize( heap, &empty.item, size );
  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&top_k_collector );
  }
  XFINALLY {}

  // The collector context is now ready
  return top_k_collector;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCollector_context_t * __new_unsorted_list_vertex_collector( vgx_Graph_t *graph, const int64_t size, const vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query ) {

  // Create the collector context
  vgx_VertexCollector_context_t *collector = NULL;
  Cm256iList_t *list = NULL;
  vgx_VertexRef_t *refmap = NULL;
  vgx_Ranker_t *ranker = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    // Make sure size is within bounds
    int64_t max_capacity = Comlib_Cm256iList_t_ElementCapacity();
    if( size > max_capacity ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x371 );
    }

    // Allocate the context
    if( (collector = calloc( 1, sizeof(vgx_VertexCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x372 );
    }

    // We will collect using a list (no sort order defined)
    Cm256iList_constructor_args_t list_args = {
      .element_capacity = size,
      .comparator = NULL
    };

    // Create the new list
    if( (list = COMLIB_OBJECT_NEW( Cm256iList_t, NULL, &list_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x373 );
    }

    // Create the vertex reference map
    if( (refmap = __new_vertex_reference_map( size, &collector->sz_refmap )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x374 );
    }

    // Create the ranker
    if( (ranker = __new_search_ranker_context( ranking_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x375 );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x376 );
    }

    bool locked_tail = false;
    bool locked_head = false;
    __locked_arc_access( ranking_context->readonly_graph, query, &locked_tail, &locked_head );

    // Initialize the collector context
    collector->type                     = VGX_COLLECTOR_TYPE_UNSORTED_VERTEX_LIST;
    collector->graph                    = graph;
    collector->ranker                   = ranker;
    collector->container.sequence.list  = list;
    collector->refmap                   = refmap;
    collector->stage                    = stage;
    collector->postheap                 = NULL;
    collector->size                     = size;
    collector->n_remain                 = size;
    collector->n_collectable            = 0;
    collector->locked_tail_access       = locked_tail;
    collector->locked_head_access       = locked_head;
    collector->fieldmask                = vgx_query_response_attr_fastmask( query );
    collector->timing_budget            = vgx_query_timing_budget( query );
    __get_vertex_collector_functions( ranking_context, &collector->stage_vertex, &collector->collect_vertex );
    collector->n_vertices               = 0;
    collector->counts_are_deep          = false;

  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&collector );
  }
  XFINALLY {}

  // The collector context is now ready
  return collector;

}



static vgx_VertexCollector_context_t * __new_null_vertex_collector( vgx_Graph_t *graph, vgx_BaseQuery_t *query ) {
  // Create the collector context
  vgx_VertexCollector_context_t *collector = NULL;
  vgx_CollectorStage_t *stage = NULL;

  XTRY {
    if( (collector = calloc( 1, sizeof(vgx_VertexCollector_context_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x37A );
    }

    // Create the stage
    if( (stage = __new_collector_stage()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x37B );
    }

    // Initialize the collector context
    collector->type               = VGX_COLLECTOR_TYPE_NONE;
    collector->graph              = graph;
    collector->ranker             = NULL;
    collector->container.object   = NULL;
    collector->refmap             = NULL;
    collector->sz_refmap        = 0;
    collector->stage              = stage;
    collector->postheap           = NULL;
    collector->size               = 0;
    collector->n_remain           = LLONG_MAX;
    collector->n_collectable      = 0;
    collector->locked_tail_access = false;
    collector->locked_head_access = false;
    collector->fieldmask          = VGX_RESPONSE_ATTRS_NONE;
    collector->timing_budget      = vgx_query_timing_budget( query );
    __get_vertex_collector_functions( NULL, &collector->stage_vertex, &collector->collect_vertex );
    collector->n_vertices         = 0;
    collector->counts_are_deep    = true;

  }
  XCATCH( errcode ) {
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&collector );
  }
  XFINALLY {}

  // The collector context is now ready
  return collector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_collect_counts( const int64_t hits, const int offset, vgx_collect_counts_t *counts ) {
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
static void _vxquery_collector__delete_collector( vgx_BaseCollector_context_t **collector ) {
  if( *collector ) {

    if( (*collector)->ranker ) {
      // Delete the ranker
      __delete_search_ranker_context( &(*collector)->ranker );
    }

    // Delete the container
    if( (*collector)->container.object ) {
      COMLIB_OBJECT_DESTROY( (*collector)->container.object );
    }

    // Delete the stage
    __delete_collector_stage( &(*collector)->stage );

    // Delete any vertex reference map
    if( (*collector)->refmap ) {
      __destroy_vertex_reference_map( (*collector)->graph, &(*collector)->refmap, (*collector)->sz_refmap );
    }

    // Delete any postheap
    if( (*collector)->postheap ) {
      COMLIB_OBJECT_DESTROY( (*collector)->postheap );
    }

    // Delete the collector
    free( *collector );
    *collector = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __collect_aggregator_cell_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  // Extract the key from cell
  vgx_ArcAggregationKey_t key = { .bits = APTR_AS_ANNOTATION( fh_cell ) };

  // The collector
  vgx_ArcCollector_context_t *collector = ((vgx_ArcCollector_context_t*)processor->processor.input);

  vgx_predicator_t predicator = VGX_PREDICATOR_NONE;

  predicator.rkey = key.pred_key;

  // Special treatment
  if( collector->collect_arc == _iCollectArc.into_average_value_map ) {
    vgx_ArcAggregationValue_t value = { .ibits = APTR_AS_INTEGER( fh_cell ) };
    predicator.val.real = value.real;
    predicator.mod.bits = VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR;
  }
  else {
    if( _vgx_predicator_value_is_float( predicator ) ) {
      predicator.val.real = (float)APTR_GET_REAL( fh_cell );
    }
    else {
      int64_t value56 = APTR_AS_INTEGER( fh_cell );
      if( value56 > INT_MAX ) {
        predicator.val.integer = INT_MAX;
      }
      else if( value56 < INT_MIN ) {
        predicator.val.integer = INT_MIN;
      }
      else {
        predicator.val.integer = (int32_t)value56;
      }
    }
  }

  // Build vertex reference from address in cell key
  // TODO: [dev_AMBD-2470] IS IT POSSIBLE THAT VERTEX MAY BE LOCKED AND WE MUST TRANSFER THE LOCK BIT TO THE GENERATED REFERENCE ????
  vgx_VertexRef_t *ref = (vgx_VertexRef_t*)__TPTR_UNPACK( key.vertexref_qwo );

  if( ref == NULL ) {
    return -1;
  }

  // Apply postfilter if we have one
  vgx_virtual_ArcFilter_context_t *postfilter = collector->postfilter;
  if( postfilter ) {
    vgx_LockableArc_t larc = VGX_LOCKABLE_ARC_INIT( ref->vertex, ref->slot.locked, predicator, ref->vertex, ref->slot.locked );
    vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
    // No match - remove reference
    if( !postfilter->filter( postfilter, &larc, &match ) ) {
      vgx_Graph_t *locked_graph = NULL;
      _vxquery_collector__del_vertex_reference_ACQUIRE_CS( (vgx_BaseCollector_context_t*)collector, ref, &locked_graph );
      GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );
      return 0;
    }
    // Error
    else if( __is_arcfilter_error( match ) ) {
      return -1;
    }
  }

  // Incref to account for duplication since we're assigning reference to both tail and head in the collector item
  ++ref->refcnt;

  // Final collected item in output
  vgx_CollectorItem_t collected = {
    .tailref    = ref,
    .headref    = ref,
    .predicator = predicator,
    .sort       = {0}
  };

  // Append item to output list
  Cm256iList_t *list = (Cm256iList_t*)processor->processor.output;
  collector->n_collectable++;
  return CALLABLE(list)->Append( list, &collected.item );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __collect_aggregator_cell_into_heap( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {

  // Extract the key from cell
  vgx_ArcAggregationKey_t key = { .bits = APTR_AS_ANNOTATION( fh_cell ) };

  // The collector
  vgx_ArcCollector_context_t *collector = ((vgx_ArcCollector_context_t*)processor->processor.input);
  
  vgx_predicator_t predicator = VGX_PREDICATOR_NONE;

  predicator.rkey = key.pred_key;

  // Special treatment
  if( collector->collect_arc == _iCollectArc.into_average_value_map ) {
    vgx_ArcAggregationValue_t value = { .ibits = APTR_AS_INTEGER( fh_cell ) };
    predicator.val.real = value.real;
    predicator.mod.bits = VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR;
  }
  else {
    if( _vgx_predicator_value_is_float( predicator ) ) {
      predicator.val.real = (float)APTR_GET_REAL( fh_cell );
    }
    else {
      int64_t value56 = APTR_AS_INTEGER( fh_cell );
      if( value56 > INT_MAX ) {
        predicator.val.integer = INT_MAX;
      }
      else if( value56 < INT_MIN ) {
        predicator.val.integer = INT_MIN;
      }
      else {
        predicator.val.integer = (int32_t)value56;
      }
    }
  }

  // Build vertex reference from address in cell key
  // TODO: [dev_AMBD-2470] IS IT POSSIBLE THAT VERTEX MAY BE LOCKED AND WE MUST TRANSFER THE LOCK BIT TO THE GENERATED REFERENCE ????
  vgx_VertexRef_t *ref = (vgx_VertexRef_t*)__TPTR_UNPACK( key.vertexref_qwo );

  if( ref == NULL ) {
    return -1;
  }

  // Head and tail share the same reference in this case
  vgx_LockableArc_t larc = VGX_LOCKABLE_ARC_INIT( ref->vertex, ref->slot.locked, predicator, ref->vertex, ref->slot.locked );

  // Run the post-collect collector function
  // NOTE: The collector must be configured with the collector container set to the heap we're post-collecting into from the mapping
  vgx_virtual_ArcFilter_context_t *postfilter = collector->postfilter;
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
  if( postfilter == NULL || postfilter->filter( postfilter, &larc, &match ) ) {
    ++ref->refcnt; // We are collecting, incref since the ref is now used by both tail and head
    collector->n_collectable++;
    return collector->post_collect( collector, &larc );
  }
  // No match - remove reference
  else if( !__is_arcfilter_error( match ) ) {
    vgx_Graph_t *locked_graph = NULL;
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( (vgx_BaseCollector_context_t*)collector, ref, &locked_graph );
    GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );
    return 0;
  }
  // Error
  else {
    return -1;
  }

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __populate_list_from_aggregator_mapping( vgx_ArcCollector_context_t *collector, Cm256iList_t *list ) {

  // This is our map to extract items from
  framehash_t *mapping = collector->container.mapping.aggregator;

  // If sorted output is required, we must use a heap when extracting items from the aggregator mapping
  f_framehash_cell_processor_t cellproc;
  if( collector->type == VGX_COLLECTOR_TYPE_SORTED_ARC_AGGREGATION ) {
    cellproc = __collect_aggregator_cell_into_heap;
    collector->container.sequence.heap = collector->postheap; // WARNING! Override the collector container during post collection! Must restore below.
  }
  else {
    cellproc = __collect_aggregator_cell_into_list;
  }

  // Configure post collection processor
  framehash_processing_context_t collect_aggregator_cell = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &mapping->_topframe, &mapping->_dynamic, cellproc );
  FRAMEHASH_PROCESSOR_SET_IO( &collect_aggregator_cell, collector, list );

  // Reset collectable counter for the next processor - this will become the aggregated count
  collector->n_collectable = 0;

  // Run processor
  int64_t n_proc = iFramehash.processing.ProcessNolock( &collect_aggregator_cell );

  // Restore the container sequence in case it was overridden above. (Need this for proper destruction.)
  collector->container.mapping.aggregator = mapping;

  if( n_proc >= 0 && list ) {
    // Heap was used (pre-sized to correct count), we must transfer the heap's internal buffer (top-k items) to the list object and re-sort
    if( collector->type == VGX_COLLECTOR_TYPE_SORTED_ARC_AGGREGATION ) {
      CALLABLE( list )->TransplantFrom( list, (Cm256iList_t*)collector->postheap );
      CALLABLE( list )->Sort( list );
    }
    // List was used, we truncate according to requested count
    else {
      CALLABLE( list )->Truncate( list, collector->size );
    }
  }

  // This is the final number of items collected
  return n_proc;
}



/*******************************************************************//**
 * Convert the collector's data to a list according the collector mode.
 * Return the collector object, or NULL on error.
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * _vxquery_collector__convert_to_base_list_collector( vgx_BaseCollector_context_t *collector ) {

  Cm256iList_constructor_args_t list_args = {0};
  bool is_sorted = false; 
  bool is_sequence = false;
  bool is_mapping = false;
  switch( collector->type ) {

  // Unsorted lists
  case VGX_COLLECTOR_TYPE_UNSORTED_ARC_LIST:
    /* FALLTHRU */
  case VGX_COLLECTOR_TYPE_UNSORTED_VERTEX_LIST:
    list_args.comparator = NULL;
    is_sorted = false;
    is_sequence = true;
    break;

  // Sorted lists
  case VGX_COLLECTOR_TYPE_SORTED_ARC_LIST:
    /* FALLTHRU */
  case VGX_COLLECTOR_TYPE_SORTED_VERTEX_LIST:
    // Sort the final output list the same way as the heap used for collection
    list_args.comparator = collector->container.sequence.heap->_cmp;
    is_sorted = true;
    is_sequence = true;
    break;

  // Unsorted aggregator
  case VGX_COLLECTOR_TYPE_UNSORTED_ARC_AGGREGATION:
    list_args.comparator = NULL;
    is_sorted = false;
    is_mapping = true;
    break;

  // Sorted aggregator
  case VGX_COLLECTOR_TYPE_SORTED_ARC_AGGREGATION:
    // Sort the final output list the same way as the postheap which will be used when extracting from aggregation map
    list_args.comparator = collector->postheap->_cmp;
    is_sorted = true;
    is_mapping = true;
    break;

  // ?
  default:
    break;
  }

  Cm256iList_t *output;

  // Make the output list
  if( (output = COMLIB_OBJECT_NEW( Cm256iList_t, NULL, &list_args )) == NULL ) {
    return NULL; // error
  }

  // Collector container is a sequence
  // Transfer collector's data elements into final output list using a transplant method
  if( is_sequence ) {
    CALLABLE( output )->TransplantFrom( output, collector->container.sequence.list );
    if( is_sorted ) {
      // Sequence is a heap, which isn't really sorted when viewed as list, so we have to sort it now.
      // Presumably a quick operation since we're dealing with the top-k items only.
      CALLABLE( output )->Sort( output ); // Final sort of top-k items
    }
  }
  // Collector container is a mapping
  // We have to run a post-collection extraction from mapping into output list
  else if( is_mapping ) {
    int64_t n_items = __populate_list_from_aggregator_mapping( (vgx_ArcCollector_context_t*)collector, output );
    if( n_items < 0 ) {
      COMLIB_OBJECT_DESTROY( output );
      return NULL; // error
    }
    // N_HITS is now = collector->n_collectable
    // since we're doing the secondary collect with post filter
  }

  // Replace the collector container with the new one
  if( collector->container.object ) {
    COMLIB_OBJECT_DESTROY( collector->container.object );
  }
  collector->container.sequence.list = output;

  return collector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __reset_collector_item( vgx_BaseCollector_context_t *collector, vgx_CollectorItem_t *item, vgx_Graph_t **locked_graph ) {
  _vxquery_collector__del_vertex_reference_ACQUIRE_CS( collector, item->tailref, locked_graph );
  _vxquery_collector__del_vertex_reference_ACQUIRE_CS( collector, item->headref, locked_graph );
  item->tailref = NULL;
  item->headref = NULL;
  item->predicator = VGX_PREDICATOR_NONE;
  item->sort.qword = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __remove_collector_items( vgx_BaseCollector_context_t *collector, int64_t offset, int64_t count ) {
  int64_t sz = 0;
  if( collector && collector->graph ) {
    vgx_Graph_t *locked_graph = NULL;
    Cm256iList_t *list = collector->container.sequence.list;
    if( list ) {
      Cm256iList_vtable_t *iList = CALLABLE( list );
      sz = iList->Length( list );
      vgx_CollectorItem_t item;
      int64_t n = 0;
      if( offset < 0 ) {
        offset = 0;
      }
      if( count < 0 ) {
        count = sz;
      }
      for( int64_t i=offset; i<sz && n<count; i++,n++ ) {
        iList->Get( list, i , &item.item );
        __reset_collector_item( collector, &item, &locked_graph );
      }
    }
    for( int i=0; i<VGX_COLLECTOR_STAGE_SIZE; i++ ) {
      __reset_collector_item( collector, &collector->stage->slot[i], &locked_graph );
    }
  
    GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * _vxquery_collector__trim_base_list_collector( vgx_BaseCollector_context_t *collector, int64_t n_collected, int offset, int64_t hits ) {
  Cm256iList_t *list = collector->container.sequence.list;
  Cm256iList_vtable_t *iList = CALLABLE( list );
  int64_t n_needed = hits + offset;

  // We didn't collect enough elements to reach the max hits count, so there may be null elements in the list that must be truncated.
  if( n_collected < n_needed && iList->Length( list ) > n_collected ) {
    // Release any items at end (should not release anything since they're null/dummies)
    __remove_collector_items( collector, n_collected, -1 );
    // Truncate end
    if( iList->Truncate( list, n_collected ) < 0 ) {
      return NULL;
    }
  }

  // Skip ahead in list according to offset
  if( offset > 0 ) {
    // Release any items at beginning
    __remove_collector_items( collector, 0, offset );
    // Discard beginning of list
    iList->Discard( list, offset );
  }

  // Truncate again if list is larger than required by hits+offset.
  // This can happen with aggregation where deephits are large but the actual result list should be smaller
  if( iList->Length( list ) > n_needed ) {
    // Release all un-needed items at end
    __remove_collector_items( collector, n_needed, -1 );
    // Truncate end
    if( iList->Truncate( list, n_needed ) < 0 ) {
      return NULL;
    }
  }

  return collector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxquery_collector__transfer_base_list( vgx_ranking_context_t *ranking_context, vgx_BaseCollector_context_t **src, vgx_BaseCollector_context_t **dest ) {
  Cm256iList_vtable_t *iList = (Cm256iList_vtable_t*)COMLIB_CLASS_VTABLE( Cm256iList_t );
  // Caller's return collector (dest) doesn't exist, use the local result (src) directly as the return collector.
  // This simply moves src to dest.
  if( *dest == NULL ) {
    *dest = *src;
    *src = NULL; // Consume the source
  }
  // Append our local result (src) to the caller's existing collector (dest)
  else {
    // Caller's existing list is extended with everything from local result (src)
    Cm256iList_t *src_list = (*src)->container.sequence.list;
    Cm256iList_t *dest_list = (*dest)->container.sequence.list;

    iList->Absorb( dest_list, src_list, -1 );
    (*dest)->n_collectable += (*src)->n_collectable;
    if( ranking_context && ranking_context->sortspec != VGX_SORTBY_NONE ) {
      // NOTE: This is inefficient, let's improve by
      // adding a merge method to the list interface
      iList->Sort( dest_list );
    }
  }

  return iList->Length( (*dest)->container.sequence.list );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * _vxquery_collector__new_arc_collector( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts ) {
  vgx_ArcCollector_context_t *collector = NULL;
  
  if( ranking_context ) {

    // Simplest case, collect individual arcs (including duplicates) into unsorted list
    if( ranking_context->sortspec == VGX_SORTBY_NONE ) {
      collector = __new_unsorted_list_arc_collector( graph, counts->n_collect, ranking_context, query );
    }
    //
    else {
      vgx_sortspec_t aggr = _vgx_aggregate( ranking_context->sortspec );
      // We will collect arcs into aggregating map, collapsing equivalent arcs to same vertex into one arc with an aggregated value
      // Sorting may occur as a post-collection step, according to the sortspec
      if( aggr ) {
        collector = __new_aggregation_arc_collector( graph, counts->n_collect, ranking_context, query );
      }
      // We will collect the top hits (individual arcs) according to some sorting function
      else {
        collector = __new_sorted_list_arc_collector( graph, counts->n_collect, ranking_context, query );
      }
    }
  }
  else {
    collector = __new_null_arc_collector( graph, query );
  }

  return collector;
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCollector_context_t * _vxquery_collector__new_vertex_collector( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts ) {
  vgx_VertexCollector_context_t *collector = NULL ;

  if( ranking_context ) {

    // Collector is SORTED
    if( ranking_context->sortspec != VGX_SORTBY_NONE ) {
      // We will collect the top hits according to some sorting function
      collector = __new_sorted_list_vertex_collector( graph, counts->n_collect, ranking_context, query );
    }
    // Collector is UNSORTED
    else {
      collector = __new_unsorted_list_vertex_collector( graph, counts->n_collect, ranking_context, query );
    }
  }
  else {
    collector = __new_null_vertex_collector( graph, query );
  }

  return collector;
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_collector.h"

test_descriptor_t _vgx_vxquery_collector_tests[] = {
  { "VGX Graph Collector Tests", __utest_vxquery_collector },
  {NULL}
};
#endif
