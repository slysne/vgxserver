/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    leaf.c
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

#include "_framehash.h"
#include "_fcell.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



typedef enum __e_resize_direction_t {
  RESIZE_EXPAND,
  RESIZE_REDUCE
} __resize_direction_t;


#define __USE_HALFSLOT( Order_p )           ( (Order_p) <= _FRAMEHASH_MAX_P_SMALL_FRAME )


static int __rehash_cells( framehash_context_t * const dest_context, const framehash_cell_t * const source_cells, const int ncells );
static framehash_retcode_t __leaf_resize( framehash_context_t * const context, const __resize_direction_t direction );
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
static void __hashbits_for_frame_order( const int p, _hashbits_t * const h16, int * const speculation_threshold );
#else
static void __hashbits_for_frame_order( const int p, _hashbits_t * const h16 );
#endif


static bool __probe_cell_find_available( const framehash_context_t * const context, const framehash_cell_t ** const target, const framehash_cell_t *probe_cursor, const framehash_cell_t * const pslot, const int ncells );
static bool __probe_cell_match_key( const framehash_context_t * const context, const framehash_cell_t ** const target, const framehash_cell_t *probe_cursor, const framehash_cell_t * const pslot, const int ncells );


static framehash_retcode_t __leaf_zoneprobe( const framehash_context_t * const context, const framehash_cell_t ** const target, const bool find_available );




/*******************************************************************//**
 * __rehash_cells
 *
 ***********************************************************************
 */
__inline static int __rehash_cells( framehash_context_t * const dest_context, const framehash_cell_t * const source_cells, const int ncells ) {
  int retcode = 0;

  const framehash_cell_t *cell = source_cells;

  for( int j=0; j < ncells; j++, cell++ ) {
    if( _CELL_IS_EMPTY( cell ) ) {    // EMPTY: Skip this cell
      continue;
    }
    else if( _CELL_IS_END( cell ) ) { // END: Skip rest of this slot
      break;
    }
    else {
      // insert the object or value into new frame, which may or may not resize or re-structure itself (edge case: TODO: A GOOD TEST CASE for this is crucial)
      _LOAD_CONTEXT_FROM_CELL( dest_context, cell );

      framehash_retcode_t rehashing = _framehash_radix__set( dest_context );
      // If nothing new got inserted (i.e. item was overwritten) we have a serious problem: DUPLICATE in original frame
      if( rehashing.delta == 0 ) {
        retcode = -1;
        break;
      }
      // Other insertion error?
      else if( rehashing.error ) {
        // Flag that max capacity was reached
        retcode = -1;  // system is full, new frame could not resize or re-structure itself
        break;
      }
    }
  }

  return retcode;
}



/*******************************************************************//**
 * __leaf_resize
 *
 ***********************************************************************
 */
static framehash_retcode_t __leaf_resize( framehash_context_t * const context, const __resize_direction_t direction ) {
  framehash_retcode_t resizing = {0};
  framehash_cell_t *leaf = context->frame;
  framehash_metas_t *leaf_metas = CELL_GET_FRAME_METAS( leaf );
  framehash_slot_t *leaf_slots = CELL_GET_FRAME_SLOTS( leaf );
  int p = leaf_metas->order;
  int domain = leaf_metas->domain;
  int new_p = 0;
  framehash_cell_t new_leaf;
  framehash_slot_t *new_slots = NULL;

  framehash_context_t rehash_context = _CONTEXT_INIT_REHASH( context, &new_leaf );
  
  /* ______ */
  /* Step 0: Sanity check. */
  if( leaf_metas->ftype != FRAME_TYPE_LEAF ) {
    resizing.error = true;
    resizing.unresizable = true;
    return resizing;
  }

  /* ______ */
  /* Step 1: Determine resize direction and create a new suitable target frame for the rehash */
  if( direction == RESIZE_EXPAND && p < _FRAMEHASH_P_MAX ) {
    // increment new order, cap at max
    if( p > _FRAMEHASH_P_MAX - _FRAMEHASH_P_INCREMENT ) {
      new_p = _FRAMEHASH_P_MAX;
    }
    else if( __USE_HALFSLOT(p) || context->control.growth.minimal || domain >= _FRAMEHASH_OPTIMIZE_MEM_FROM_DOMAIN ) {
      new_p = p + 1; // gentle increase from the minimim to the next order
    }
    else {
      new_p = p + _FRAMEHASH_P_INCREMENT;
    }
  }
  else if( direction == RESIZE_REDUCE && p > _FRAMEHASH_P_MIN ) {
    int load = __get_loadfactor( leaf );
    // Strong reduction (load is much lower than max LF) 
    if( load * 4 < _FRAMEHASH_MAX_LOADFACTOR && p > _FRAMEHASH_P_MIN+1 ) {
      new_p = p - 2;
    }
    // Normal reduction (cut in half if LF after cut will be less than max LF)
    else if( load * 2 < _FRAMEHASH_MAX_LOADFACTOR ) {
      new_p = p - 1;
    }
    else {
      resizing.unresizable = true;
      return resizing;
    }

  }
  else {
    resizing.unresizable = true;
    return resizing;
  }

  XTRY {
    size_t nslots = _FRAME_NSLOTS( p );
    framehash_slot_t *slot;
    framehash_cell_t *cell_array;
    framehash_halfslot_t *halfslot = _framehash_frameallocator__header_half_slot( leaf );
#if _FRAMEHASH_USE_MEMORY_STREAMING
    framehash_cell_t mcell[FRAMEHASH_CELLS_PER_SLOT];
#endif

    /* ______ */
    /* Step 2: Allocate a new frame */
    // TODO: metas.fileno is currently fixed at 0 since we're only implementing memory-based frames at this point.
    if( (new_slots = _framehash_memory__new_frame( &rehash_context, new_p, leaf_metas->domain, FRAME_TYPE_LEAF )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x401 );
    }

    /* ______ */
    /* Step 3: Rehash */
    // cells in header's halfslot
    if( __rehash_cells( &rehash_context, halfslot->cells, FRAMEHASH_CELLS_PER_HALFSLOT ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_CAPACITY, 0x402 );
    }
    // all slots in frame
    slot = leaf_slots;
    for( unsigned fx=0; fx<nslots; fx++, slot++ ) {
#if _FRAMEHASH_USE_MEMORY_STREAMING
      // stream load all cells in slot into streaming buffer NTA
      for( unsigned j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
        mcell[j].m128i = _mm_stream_load_si128( (__m128i*)(slot) + j );
      }
      // TODO: explain why lfence is needed here?
      // From the Intel intrinsics manual:
      //      Perform a serializing operation on all load-from-memory instructions that were issued prior to this instruction.
      //      Guarantees that every load instruction that precedes, in program order, is globally visible before any load
      //      instruction which follows the fence in program order.
      _mm_lfence();
      cell_array = mcell;
#else
      cell_array = (framehash_cell_t*)slot;
#endif
      if( __rehash_cells( &rehash_context, cell_array, FRAMEHASH_CELLS_PER_SLOT ) < 0 ) {
        // Flag that max capacity was reached
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0x403 ); // system is full, new frame could not resize or re-structure itself
      }
    }


    // A few words at this point:
    // The new frame new_frame may or may not be the same as before the copy. If new_frame needed another rehash in the midst of this rehash it will have been modified.
    // Reductions may result in the new frame being of same size as the original, which is an edge case, but is handled transparently, albeit with wasted work being done.

    /* ______ */
    /* Step 4: Swap in new frame and discard old frame */
    // Discard old frame slots
    _framehash_memory__discard_frame( context );
    _CELL_STEAL( context->frame, rehash_context.frame );

    // Flag the modification
    resizing.completed = true;
    resizing.modified = true;
    resizing.depth = CELL_GET_FRAME_METAS( context->frame )->domain;

#ifdef FRAMEHASH_INSTRUMENTATION
    if( direction == RESIZE_EXPAND ) {
      context->instrument->resize.nup++;
    }
    else if( direction == RESIZE_REDUCE ) {
      context->instrument->resize.ndown++;
    }
#endif

#ifdef FRAMEHASH_CONSISTENCY_CHECK
    if( !iFramehash.memory.IsFrameValid( context->frame ) ) {
      FATAL( 0xFC1, "Bad frame after resize" );
    }
#endif


  }
  XCATCH( errcode ) {
    resizing.error = true;
    if( new_slots ) {
      _framehash_memory__discard_frame( &rehash_context );
    }
  }
  XFINALLY {

  }


  return resizing;
}



/*******************************************************************//**
 * __hashbits_for_frame_order
 *
 ***********************************************************************
 */

#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
#define __SPECULATION_THRESHOLD( Order_p ) *speculation_threshold = ((((2<<(Order_p-1))-1) * FRAMEHASH_CELLS_PER_SLOT)>>1) /* half capacity of order p, e.g. p=6 => threshold=252/2=126 */
__inline static void __hashbits_for_frame_order( const int p, _hashbits_t * const h16, int * const speculation_threshold )
#else
__inline static void __hashbits_for_frame_order( const int p, _hashbits_t * const h16 )
#endif
{

  // discard unused hash bits
  switch( p ) {
  case /* 6 */ _FRAMEHASH_P_MAX:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX );
#endif
    return;
  case /* 5 */ _FRAMEHASH_P_MAX-1:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX-1 );
#endif
    *h16 >>= 1*(_FRAMEHASH_P_MAX-1) + 0;  // 5
    return;
  case /* 4 */ _FRAMEHASH_P_MAX-2:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX-2 );
#endif
    *h16 >>= 2*(_FRAMEHASH_P_MAX-2) + 1;  // 9
    return;
  case /* 3 */ _FRAMEHASH_P_MAX-3:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX-3 );
#endif
    *h16 >>= 3*(_FRAMEHASH_P_MAX-3) + 3;  // 12
    return;
  case /* 2 */ _FRAMEHASH_P_MAX-4:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX-4 );
#endif
    *h16 >>= 4*(_FRAMEHASH_P_MAX-4) + 6;  // 14
    return;
  case /* 1 */ _FRAMEHASH_P_MAX-5:
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
    __SPECULATION_THRESHOLD( _FRAMEHASH_P_MAX-5 );
#endif
    *h16 >>= 5*(_FRAMEHASH_P_MAX-5) + 10; // 15
    return;
  case /* 0 */ _FRAMEHASH_P_MAX-6:
    *h16 = 0;
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * __probe_cell_find_available
 *
 ***********************************************************************
 */
__inline static bool __probe_cell_find_available( const framehash_context_t * const context, const framehash_cell_t ** const target, const framehash_cell_t *probe_cursor, const framehash_cell_t * const pslot, const int ncells ) {
  bool match;
#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.nleafzones++;
#endif
  // probe each cell in loaded slot
  for( int j=0; j<ncells; j++, probe_cursor++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nleafcells++;
#endif
    if( _ITEM_IS_VALID( probe_cursor ) ) {
      // verify match
      _CELL_ITEM_MATCH( context, probe_cursor, &match );
      if( match ) {
        // verified!
        *target = pslot + j;
        return true; // probe hit
      }
    }
    else if( _CELL_IS_END( probe_cursor ) ) {
      // We hit the end marker
      if( *target == NULL ) {
        // update target with END unless we found an earlier target
        *target = pslot + j;
      } /* else: we found EMPTY earlier and we'll use that since it's higher up */
      return true; // probe hit
    }
    else if( _CELL_IS_EMPTY( probe_cursor ) ) {
      // We hit an empty/available cell
      if( *target == NULL ) {
        // update target with EMPTY unless we already found an earlier target
        *target = pslot + j;
        // but we're never completed by finding EMPTY, since the item may exist farther down
      }
    }
  }
  return false; // probe miss
}



/*******************************************************************//**
 * __probe_cell_match_key
 *
 ***********************************************************************
 */
__inline static bool __probe_cell_match_key( const framehash_context_t * const context, const framehash_cell_t ** const target, const framehash_cell_t *probe_cursor, const framehash_cell_t * const pslot, const int ncells ) {
  bool match;
#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.nleafzones++;
#endif
  // probe each cell in loaded slot
  for( int j=0; j<ncells; j++, probe_cursor++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nleafcells++;
#endif
    if( _ITEM_IS_VALID( probe_cursor ) ) {
      // verify match
      _CELL_ITEM_MATCH( context, probe_cursor, &match );
      if( match ) {
        // verified!
        *target = pslot + j;
        return true;
      }
    }
    else if( _CELL_IS_END( probe_cursor ) ) {
      // We hit the end marker
      return false; // probe miss
    }
  }
  return false; // probe miss
}



/*******************************************************************//**
 * __leaf_halfslot_probe
 *
 ***********************************************************************
 */
__inline static bool __leaf_halfslot_probe( const framehash_context_t * const context, const framehash_cell_t ** const target, const bool find_available ) {
  framehash_cell_t *phslot = _framehash_frameallocator__header_half_slot( (framehash_cell_t*)context->frame )->cells;
  framehash_cell_t *probe_cursor = phslot;
  if( find_available ) {
    return __probe_cell_find_available( context, target, probe_cursor, phslot, FRAMEHASH_CELLS_PER_HALFSLOT );
  }
  else {
    return __probe_cell_match_key( context, target, probe_cursor, phslot, FRAMEHASH_CELLS_PER_HALFSLOT );
  }
}



static const framehash_retcode_t PROBE_MISS = {0};

static const framehash_retcode_t PROBE_HIT = {
  .completed    = 1,
  .cacheable    = 0,
  .modified     = 0,
  .empty        = 0,
  .unresizable  = 0,
  .error        = 0,
  .delta        = 0,
  .depth        = 0
};



/*******************************************************************//**
 * __probe_stop
 *
 ***********************************************************************
 */
__inline static framehash_retcode_t __probe_stop( const framehash_cell_t ** const target ) {
  // Probing stopped, may have a target cell
  // Probing determined a target EMPTY cell with no MATCH or END after, and since we're leaf there's nothing more farther down
  if( *target != NULL ) {
    return PROBE_HIT;
  }
  else {
    return PROBE_MISS;
  }
}



/*******************************************************************//**
 * __leaf_zoneprobe
 *
 ***********************************************************************
 */
static framehash_retcode_t __leaf_zoneprobe( const framehash_context_t * const context, const framehash_cell_t ** const target, const bool find_available ) {
  framehash_cell_t *frame = context->frame;
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  int p = frame_metas->order;
  framehash_cell_t *pslot;
  framehash_cell_t *probe_cursor;

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.nCL++; // first zone to be probed
#endif

  // For a minimal frame with header only, search the halfslot
  if( p == 0 ) {
    if( __leaf_halfslot_probe( context, target, find_available ) ) {
      return PROBE_HIT;
    }
    else {
      return __probe_stop( target );
    }
  }
  // For normal frames go down the zones from largest to smallest, and then consult halfslot (if used for the frame)
  else {
    // Probing starts in the largest zone k which is order p minus 1
    int k = p - 1;
    _hashbits_t indexmask = _ZONE_INDEXMASK( k );
    
    // Extract the hash region relevant to the current domain
    _hashbits_t h16 = _framehash_hashing__get_hashbits( frame_metas->domain, context->key.shortid );
    
    // Adjust the hashbits according to frame order
#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
#define __NEXT_ZONE_FRAME_LOAD( ThisLoad, ThisZone_k ) ((ThisLoad) - _FRAME_ZONESLOTS( ThisZone_k ) * FRAMEHASH_CELLS_PER_SLOT)
#define __NEXT_ZONE_SPECULATION_THRESHOLD( ThisThreshold ) (ThisThreshold >> 1)
    int load = frame_metas->nactive;
    int speculation_threshold;
    __hashbits_for_frame_order( p, &h16, &speculation_threshold );
#else
    __hashbits_for_frame_order( p, &h16 );
#endif

    // Compute the index of the first slot to probe
    int fx = _framehash_radix__get_frameindex( p, k, (h16 & indexmask) );
    int next_fx = fx;


#if _FRAMEHASH_USE_MEMORY_STREAMING
    // TODO: Implement the streaming correctly.
    // If we use the vector intrinsics to load from memory into ymm/zmm we should also make
    // a probing function that uses the vector instructions operating directly on those wide
    // registers.
    // What is implemented here currently isn't very good.
    framehash_cell_t mcell[FRAMEHASH_CELLS_PER_SLOT];
#endif


    // Probe each zone until loop terminates itself from the inside
    framehash_slot_t *self_slots = CELL_GET_FRAME_SLOTS( frame );
    for(;;) {
      // Probe this slot
      pslot = (framehash_cell_t*)(self_slots + fx);

#if     _FRAMEHASH_USE_MEMORY_STREAMING
      if( frame_metas->domain >= _FRAMEHASH_MEMORY_STREAMING_MIN_DOMAIN ) {
        // fill a streaming load buffer with the cacheline at frame index fx
        // TODO: use _mm256_stream_load_si256 when AVX2 is available
        // TODO: use _mm512_stream_load_si512 when AVX-512F is available
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          mcell[j].m128i = _mm_stream_load_si128( (__m128i*)pslot+j );
        }
        probe_cursor = mcell; // NOTE: probe_cursor points into the local mcell[] buffer
      }
      else {
        probe_cursor = pslot;
      }
#else
      probe_cursor = pslot;
#endif
#if   _FRAMEHASH_ENABLE_ZONE_PREFETCH
      // If another zone exists after the current zone then prefetch the next slot in that zone if there is
      // a certain probability we will need it. Speculation heuristics are based on the idea that a fuller
      // frame makes it more likely to need more probes.
      if( k > 0 && load > speculation_threshold ) {
        // Adjust the mask, hash bits and select the next slot
        indexmask >>= 1;
        h16 >>= k;
        next_fx = _framehash_radix__get_frameindex( p, k-1, (h16 & indexmask) );

        // TODO: What happens if the actual memory access for this slot happens too soon after the
        //       prefetch? If the prefetch has completed it will load data from L2 to L1 and we have
        //       benefited from the prefetch. If the prefetch has not completed, is there any benefit
        //       or did we just waste memory cycles? I.e., does the prefetch become an "early start"
        //       of a memory access that will then continue when the actual access occurs, or will the
        //       actual access invalidate the incomplete prefetch and start the cycle all over again?

        // Issue prefetch of potentially next needed slot to L2. Won't pollute L1 if slot not needed.
        
        __prefetch_L2( (void*)(self_slots + next_fx) );
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.nCL++; // memory traffic initiated
#endif
      }
      // adjust estimated load "ahead" and threshold for remaining zones
      load = __NEXT_ZONE_FRAME_LOAD( load, k );
      speculation_threshold = __NEXT_ZONE_SPECULATION_THRESHOLD( speculation_threshold );
#endif

#if     _FRAMEHASH_USE_MEMORY_STREAMING
      // TODO: explain why lfence is needed here?
      _mm_lfence();
#endif

      // Perform probing in the selected slot in current zone
      // This will terminate probe loop if we find the cell we are looking for.
      if( find_available ) {
        // EXIT CONDITION #1: Found available cell
        if( __probe_cell_find_available( context, target, probe_cursor, pslot, FRAMEHASH_CELLS_PER_SLOT ) ) {
          return PROBE_HIT;
        }
      }
      else {
        // EXIT CONDITION #2: Found cell with key match
        if( __probe_cell_match_key( context, target, probe_cursor, pslot, FRAMEHASH_CELLS_PER_SLOT ) ) {
          return PROBE_HIT;
        }
      }

      // We just has a miss in the smallest zone, probing is almost over
      if( k == 0 ) {
        // Do a last-chance check in the header's halfslot if halfslots are active for this frame
        if( __USE_HALFSLOT(p) ) {
          // EXIT CONDITION #3: Found the cell we are looking for in the halfslot
          if( __leaf_halfslot_probe( context, target, find_available ) ) {
            return PROBE_HIT;
          }
        }
        // EXIT CONDITION #4: Probing MISS!
        return __probe_stop( target );
      }

      // Continue probing ...

      // We have not yet computed the next slot from the hash bits, do it now
      if( fx == next_fx ) {
        indexmask >>= 1;
        h16 >>= k--;
        next_fx = _framehash_radix__get_frameindex( p, k, (h16 & indexmask) );
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.nCL++; // memory traffic will occur shortly in the next zone to be probed
#endif
      }
      // We computed the next slot as part of prefetch, only change the current zone
      else {
        --k;
      }

      // This is the next slot index
      fx = next_fx;

    }
  }
}



/*******************************************************************//**
 * __expand
 *
 ***********************************************************************
 */
static framehash_retcode_t __expand( framehash_context_t * const context ) {
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );
  framehash_retcode_t expansion = {0};
  XTRY {
    switch( self_metas->ftype ) {
    case FRAME_TYPE_LEAF:
      // try to expand and rehash the leaf frame
      if( (expansion = __leaf_resize( context, RESIZE_EXPAND )).completed ) {
        break; // expansion ok
      }
      // error
      else if( expansion.error ) {
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0x423 );   // out of mem during rehash
      }
      // Unresizable leaf, convert to internal
      /* FALLTHRU */
    case FRAME_TYPE_INTERNAL:
      // add another chainslot (ASSUMPTION: it was already determined that the current shortid didn't hash to an existing chain, so we're guaranteed to create a new chain)
      if( (expansion = _framehash_radix__create_internal_chain( context )).error ) {
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0x424 );
      }
      break;
    default:
      THROW_ERROR( CXLIB_ERR_UNDEFINED, 0x425 );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
  }
  return expansion;
}



/*******************************************************************//**
 * _framehash_leaf__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_leaf__set( framehash_context_t * const context ) {
  framehash_retcode_t insertion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  XTRY {
    framehash_cell_t *target = NULL;
    framehash_retcode_t location = __leaf_zoneprobe( context, (const framehash_cell_t**)&target, true );

    // Did we find a suitable target cell?
    if( location.completed ) {
      // Available cell
      if( _ITEM_IS_INVALID(target) ) {
        // we will insert a new item so track it
        self_metas->nactive++;
        insertion.delta = true;
      }
      // Occupied cell
      else {
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.hit = true;
        context->instrument->probe.hit_ftype = FRAME_TYPE_LEAF;
#endif
        // Cell already contains OBJECT128 and the new item is NOT an OBJECT128
        if( _CELL_IS_DESTRUCTIBLE( target ) && context->vtype != CELL_VALUE_TYPE_OBJECT128 ) {
          // We will replace the previous OBJECT128 item with another data type.
          // Destroy the previous object before we insert the new item.
          COMLIB_OBJECT_DESTROY( _CELL_GET_OBJECT128( target ) );
        }
      }
      //
      __set_cell( context, target );
      //
      insertion.completed = true;
      //
      int perform_expansion = 0;
      // Rehash if loadfactor exceeded 
      if( context->control.loadfactor.high > 0 ) {
        perform_expansion = context->control.loadfactor.high < 100 && __get_loadfactor( self ) > context->control.loadfactor.high;
      }
#if _FRAMEHASH_MAX_LOADFACTOR < 100
#define __NACTIVE_CHECK_LOAD_THRESHOLD( Order_p ) ( (FRAMEHASH_CELLS_PER_SLOT/2) << (Order_p) ) //  ~ 50% of frame capacity
      else if( self_metas->nactive > __NACTIVE_CHECK_LOAD_THRESHOLD( self_metas->order )
          &&
          self_metas->domain < _FRAMEHASH_OPTIMIZE_MEM_FROM_DOMAIN
          &&
          !__USE_HALFSLOT( self_metas->order ))
      {
        perform_expansion = __get_loadfactor( self ) > _FRAMEHASH_MAX_LOADFACTOR;
      }
#endif
      
      if( perform_expansion ) {
        // Expand due to loadfactor
        framehash_retcode_t expansion = __expand( context );
        // Failed to meet loadfactor requirement, structure is full
        if( expansion.error ) {
          insertion.unresizable = true;
          THROW_CRITICAL( CXLIB_ERR_CAPACITY, 0x426 );
        }
        // Carry the modification flag
        insertion.modified = expansion.modified;
      }

    }
    // Could not find a cell to store the value. Frame full, expand.
    else {
      // Expand due to failure to find a location for new item
      framehash_retcode_t expansion = __expand( context );
      // Retry insertion
      if( expansion.completed ) {
        insertion = _framehash_radix__set( context );
      }
      // Expansion failed, structure is full.
      else {
        insertion.unresizable = true;
        THROW_CRITICAL( CXLIB_ERR_CAPACITY, 0x427 );
      }
      // Carry the modification flag
      insertion.modified = expansion.modified;
    }
  }
  XCATCH( errcode ) {
    insertion.error = true;
  }
  XFINALLY {
    insertion.depth = self_metas->domain;
  }
  return insertion;
}



/*******************************************************************//**
 * _framehash_leaf__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_leaf__get( framehash_context_t * const context ) {
  framehash_retcode_t retrieval = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  framehash_cell_t *hit = NULL;
  framehash_retcode_t location = __leaf_zoneprobe( context, (const framehash_cell_t**)&hit, false );

  // did we find leaf cell to retrieve data from?
  if( location.completed ) {
    _LOAD_CONTEXT_VALUE( context, hit );
    retrieval.completed = true;
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.hit = true;
    context->instrument->probe.hit_ftype = FRAME_TYPE_LEAF;
#endif
  }
  else {
    _LOAD_NULL_CONTEXT_VALUE( context );
  }
  retrieval.depth = self_metas->domain;
#if _FRAMEHASH_ENABLE_READ_CACHE
  if( context->control.cache.enable_read ) {
    retrieval.cacheable = true;  // we allow both hits AND misses to be cacheable!
  }
#endif
  return retrieval;
}



/*******************************************************************//**
 * _framehash_leaf__delete_item
 *
 ***********************************************************************
 */
DLL_HIDDEN uint8_t _framehash_leaf__delete_item( framehash_cell_t *frame, framehash_cell_t *target ) {
  if( _CELL_IS_DESTRUCTIBLE( target ) ) {
    COMLIB_OBJECT_DESTROY( _CELL_GET_OBJECT128( target ) );
  }
  _DELETE_ITEM( target );

  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  frame_metas->nactive--;
  return frame_metas->nactive;
}



/*******************************************************************//**
 * _framehash_leaf__del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_leaf__del( framehash_context_t * const context ) {
  framehash_retcode_t deletion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif
  
  framehash_cell_t *target = NULL;

  XTRY {
    framehash_retcode_t location = __leaf_zoneprobe( context, (const framehash_cell_t**)&target, false );
    // did we find leaf cell to delete?
    if( location.completed ) {
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.hit = true;
      context->instrument->probe.hit_ftype = FRAME_TYPE_LEAF;
#endif

      deletion.delta = true;
      deletion.completed = true;
      deletion.depth = self_metas->domain;

      _LOAD_CONTEXT_VALUE_TYPE( context, target );

      _framehash_leaf__delete_item( self, target );

      int perform_rehash = 0;

      if( context->control.loadfactor.low > 0 ) {
        perform_rehash = context->control.loadfactor.low > 1 && __get_loadfactor(self) < context->control.loadfactor.low;
      }
      else {
        perform_rehash = __get_loadfactor(self) < _FRAMEHASH_MIN_LOADFACTOR;
      }

      if( perform_rehash ) {
        framehash_retcode_t reduction = {0};
        switch( self_metas->ftype ) {
        case FRAME_TYPE_LEAF:
          // try to reduce and rehash the leaf frame
          reduction = __leaf_resize( context, RESIZE_REDUCE );
          if( reduction.error ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x431 );
          }
          if( reduction.unresizable ) {
            deletion.unresizable = true; // hint that we've reached the smallest size, so parent may consider re-structuring
          }
          break;
        case FRAME_TYPE_INTERNAL:
          deletion.unresizable = true; // we can't resize an internal frame, but hint that parent may consider re-structuring
          break;
        default:
          THROW_ERROR( CXLIB_ERR_BUG, 0x432 ); 
        }

        if( reduction.completed ) {
          deletion.modified = true;
        }
      }
    }

    if( self_metas->nactive == 0 ) {
      if( self_metas->ftype == FRAME_TYPE_LEAF ) {
        deletion.empty = true;
      }
      else if( self_metas->ftype == FRAME_TYPE_INTERNAL && self_metas->chainbits == 0 ) {
        deletion.empty = true;
      }
    }
  }
  XCATCH( errcode ) {
    deletion.error = true;
  }
  XFINALLY {
  }
  return deletion;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_leaf.h"

DLL_HIDDEN test_descriptor_t _framehash_leaf_tests[] = {
  { "leaf",   __utest_framehash_leaf },
  {NULL}
};
#endif
