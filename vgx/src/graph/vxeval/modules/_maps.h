/*
###################################################
#
# File:   _maps.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_MAPS_H
#define _VGX_VXEVAL_MODULES_MAPS_H

#include "_memory.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_maps_isetadd( vgx_Evaluator_t *self );
static void __eval_maps_vsetadd( vgx_Evaluator_t *self );
static void __eval_maps_isetdel( vgx_Evaluator_t *self );
static void __eval_maps_vsetdel( vgx_Evaluator_t *self );
static void __eval_maps_isethas( vgx_Evaluator_t *self );
static void __eval_maps_vsethas( vgx_Evaluator_t *self );
static void __eval_maps_xsetclr( vgx_Evaluator_t *self );
static void __eval_maps_xsetlen( vgx_Evaluator_t *self );
static void __eval_maps_xsetini( vgx_Evaluator_t *self );



#define __maps__EMPTY 0xFFFFFFFFUL


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __maps__dwset_add( vgx_ExpressEvalMemory_t *mem, uint32_t key, DWORD item ) {
  uint32_t index = key & mem->dwset.mask;

  // Use index to locate slot
  DWORD *cursor = mem->dwset.slots[ index ].entries;
  DWORD *end = cursor + VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE;

  // Put value into available entry
#ifdef __AVX2__
  __m128i E = _mm_set1_epi32( (int)__maps__EMPTY );
  __m128i M = _mm_set1_epi32( (int)item );

  __m128i data, cmp;

  while( cursor < end ) {
    data = _mm_load_si128( (__m128i*)cursor );
    // Already in set
    cmp = _mm_cmpeq_epi32( M, data );
    if( !_mm_testz_si128( cmp, cmp ) ) {
      return 0;
    }
    // No empty slots in this region, continue
    cmp = _mm_cmpeq_epi32( E, data );
    if( _mm_testz_si128( cmp, cmp ) ) {
      cursor += 4;
      continue;
    }
    // At least one empty slot, find it and insert item there
    while( *cursor != __maps__EMPTY ) {
      ++cursor;
    }
    *cursor = item;
    return 1;
  }

#else

  while( cursor < end ) {
    // Fill available entry
    if( *cursor == __maps__EMPTY ) {
      *cursor = item;
      return 1;
    }
    // Already in set
    if( *cursor == item ) {
      return 0;
    }
    ++cursor;
  }

#endif
  // Out of room
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __maps__dwset_del( vgx_ExpressEvalMemory_t *mem, uint32_t key, DWORD item ) {
  uint32_t index = key & mem->dwset.mask;

  // Use index to locate slot
  DWORD *cursor = mem->dwset.slots[ index ].entries;

  // Find entry and remove
  DWORD *end = cursor + VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE;
  while( cursor < end ) {
    // End
    if( *cursor == __maps__EMPTY ) {
      return 0;
    }
    // Remove from set
    if( *cursor == item ) {
      // Move subsequent entries back and free one entry
      DWORD *next = cursor + 1;
      while( next < end && *next != __maps__EMPTY ) {
        *cursor++ = *next++;
      }
      *cursor = __maps__EMPTY;
      return 1;
    }
    ++cursor;
  }

  // Not found
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __maps__dwset_has( vgx_ExpressEvalMemory_t *mem, uint32_t key, DWORD item ) {
  uint32_t index = key & mem->dwset.mask;

  // Use index to locate slot
  DWORD *cursor = mem->dwset.slots[ index ].entries;


  // Check if entry exists
#ifdef __AVX2__

  __m256i M = _mm256_set1_epi32( (int)item );
  __m256i cmp;

  cmp = _mm256_cmpeq_epi32( M, _mm256_load_si256( (__m256i*)cursor ) );
  if( !_mm256_testz_si256( cmp, cmp ) ) {
    return 1;
  }

  cursor += 8;
  
  cmp = _mm256_cmpeq_epi32( M, _mm256_load_si256( (__m256i*)cursor ) );
  if( !_mm256_testz_si256( cmp, cmp ) ) {
    return 1;
  }

#else
  
  DWORD *end = cursor + VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE;

  while( cursor < end ) {
    // Found
    if( *cursor == item ) {
      return 1;
    }
    // End
    if( *cursor == __maps__EMPTY ) {
      return 0;
    }
    ++cursor;
  }
#endif

  // Not found
  return 0;
  
}



typedef enum __e_maps__keymode {
  MAPS_KEYMODE__INTEGER,
  MAPS_KEYMODE__VERTEX
} __maps__keymode;


#define __maps__SZ_MIN_MASK   12
#define __maps__MIN_MASK      ((1 << __maps__SZ_MIN_MASK) - 1)
#define __maps__EXPAND_SHIFT  2
#define __maps__EXPAND_MASK   ((1 << __maps__EXPAND_SHIFT) - 1)


/*******************************************************************//**
 * Expand (or initialize) dwset
 * Returns 1: Expanded set
 *         0: Initialized empty set
 *        -1: OOM or rehash error
 ***********************************************************************
 */
static int __maps__dwset_expand( vgx_ExpressEvalMemory_t *mem, __maps__keymode mode ) {

  vgx_ExpressEvalDWordSetSlot_t *old_slots = mem->dwset.slots;
  uint32_t old_mask = mem->dwset.mask;
  mem->dwset.mask = old_mask != 0 ? ((old_mask << __maps__EXPAND_SHIFT) | __maps__EXPAND_MASK) : __maps__MIN_MASK;

  // Allocate next set
  uint32_t nslots = (mem->dwset.mask+1);
  PALIGNED_ARRAY( mem->dwset.slots, vgx_ExpressEvalDWordSetSlot_t, nslots );
  if( mem->dwset.slots == NULL ) {
    goto error;
  }

#if __AVX2__
  __m256i E = _mm256_set1_epi32( (int)__maps__EMPTY );
  __m256i *p256 = (__m256i*)mem->dwset.slots->entries;
  for( uint32_t i=0; i<nslots; ++i ) {
    _mm256_stream_si256( p256++, E );
    _mm256_stream_si256( p256++, E );
  }
#else
  cacheline_t E = {
    .dwords = { __maps__EMPTY, __maps__EMPTY, __maps__EMPTY, __maps__EMPTY,
                __maps__EMPTY, __maps__EMPTY, __maps__EMPTY, __maps__EMPTY,
                __maps__EMPTY, __maps__EMPTY, __maps__EMPTY, __maps__EMPTY,
                __maps__EMPTY, __maps__EMPTY, __maps__EMPTY, __maps__EMPTY }
  };
  cacheline_t *p = &mem->dwset.slots->data;
  for( uint32_t i=0; i<nslots; ++i ) {
    *p++ = E;
  }
#endif

  // Previous set exists, rehash into larger set
  if( old_slots ) {
    cacheline_t *pline = (cacheline_t*)old_slots;
    cacheline_t *pend = pline + old_mask + 1;
    for( int n=0; n<8; ++n ) {
      __prefetch_nta( pline++ );
    }

    // All slots in old set
    for( uint32_t i=0; i <= old_mask; ++i ) {
      DWORD *cursor = old_slots[i].entries;
      DWORD *end = cursor + VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE;
      switch( mode ) {
      case MAPS_KEYMODE__INTEGER:
        while( cursor < end && *cursor != __maps__EMPTY ) {
          uint32_t key = (uint32_t)ihash64( *cursor );
          if( __maps__dwset_add( mem, key, *cursor ) != 1 ) {
            goto error;
          }
          ++cursor;
        }
        break;
      case MAPS_KEYMODE__VERTEX:
        while( cursor < end && *cursor != __maps__EMPTY ) {
          uint32_t key = ((*cursor) << __maps__SZ_MIN_MASK) | i;
          if( __maps__dwset_add( mem, key, *cursor ) != 1 ) {
            goto error;
          }
          ++cursor;
        }
        if( pline < pend ) {
          __prefetch_nta( pline++ );
        }
        break;
      }
    }

    // Discard old set
    ALIGNED_FREE( old_slots );

    return 1;
  }

  return 0;

error:
  if( mem->dwset.slots ) {
    ALIGNED_FREE( mem->dwset.slots );
  }
  mem->dwset.slots = old_slots;
  mem->dwset.mask = old_mask;
  return -1;

}



/*******************************************************************//**
 * __maps__xsetadd()
 *
 ***********************************************************************
 */
static void __maps__xsetadd( vgx_Evaluator_t *self, uint32_t key, DWORD item, __maps__keymode mode ) {

  if( item == __maps__EMPTY ) {
    STACK_RETURN_INTEGER( self, -1 );
  }

  vgx_ExpressEvalMemory_t *mem = self->context.memory;

  int n = 0;

  // Set is allocated and we add object to set
  if( mem->dwset.slots && (n = __maps__dwset_add( mem, key, item )) >= 0 ) {
    goto added;
  }

  // Either no set or set full, initialize or expand
  if( __maps__dwset_expand( mem, mode ) < 0 ) {
    STACK_RETURN_INTEGER( self, -1 );
  }

  n = __maps__dwset_add( mem, key, item );
added:
  mem->dwset.sz += n;
  STACK_RETURN_INTEGER( self, n );

}



/*******************************************************************//**
 * isetadd( object ) -> 1 (added), 0 (not added), -1 (error)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_isetadd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  uint32_t key = (uint32_t)ihash64( x->bits );
  __maps__xsetadd( self, key, (DWORD)x->bits, MAPS_KEYMODE__INTEGER );
}



/*******************************************************************//**
 * vsetadd( vertex ) -> 1 (added), 0 (not added), -1 (error)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_vsetadd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  if( x->type != STACK_ITEM_TYPE_VERTEX ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  uint64_t index = __vertex_get_index( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( x->vertex ) );
  uint32_t key = (uint32_t)index;
  DWORD item = (DWORD)(index >> __maps__SZ_MIN_MASK);
  __maps__xsetadd( self, key, item, MAPS_KEYMODE__VERTEX );
}



/*******************************************************************//**
 * __maps__xsetdel()
 *
 ***********************************************************************
 */
static void __maps__xsetdel( vgx_Evaluator_t *self, uint32_t key, DWORD item ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;

  // Set is allocated and we delete object from set
  if( mem->dwset.slots && __maps__dwset_del( mem, key, item ) > 0 ) {
    mem->dwset.sz--;
    STACK_RETURN_INTEGER( self, 1 );
  }

  // Either no set or we didn't delete anything
  STACK_RETURN_INTEGER( self, 0 );
}



/*******************************************************************//**
 * isetdel( object ) -> 1 (deleted) or 0 (not deleted)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_isetdel( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  uint32_t key = (uint32_t)ihash64( x->bits );
  __maps__xsetdel( self, key, (uint32_t)x->bits );
}



/*******************************************************************//**
 * vsetdel( vertex ) -> 1 (deleted) or 0 (not deleted)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_vsetdel( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  if( x->type != STACK_ITEM_TYPE_VERTEX ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  uint64_t index = __vertex_get_index( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( x->vertex ) );
  uint32_t key = (uint32_t)index;
  DWORD item = (DWORD)(index >> __maps__SZ_MIN_MASK);
  __maps__xsetdel( self, key, item );
}



/*******************************************************************//**
 * __maps__xsethas()
 *
 ***********************************************************************
 */
static void __maps__xsethas( vgx_Evaluator_t *self, uint32_t key, DWORD item ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;

  // Set is allocated, look for object
  if( mem->dwset.slots && __maps__dwset_has( mem, key, item ) > 0 ) {
    STACK_RETURN_INTEGER( self, 1 );
  }

  // Not in set
  STACK_RETURN_INTEGER( self, 0 );
}



/*******************************************************************//**
 * isethas( object ) -> 1 (exists) or 0 (does not exist)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_isethas( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  uint32_t key = (uint32_t)ihash64( x->bits );
  __maps__xsethas( self, key, (DWORD)x->bits );
}



/*******************************************************************//**
 * vsethas( vertex ) -> 1 (exists) or 0 (does not exist)
 *
 *
 ***********************************************************************
 */
static void __eval_maps_vsethas( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  if( x->type != STACK_ITEM_TYPE_VERTEX ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  uint64_t index = __vertex_get_index( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( x->vertex ) );
  uint32_t key = (uint32_t)index;
  DWORD item = (DWORD)(index >> __maps__SZ_MIN_MASK);
  __maps__xsethas( self, key, item );
}



/*******************************************************************//**
 * xsetclr() -> n_cleared
 *
 *
 ***********************************************************************
 */
static void __eval_maps_xsetclr( vgx_Evaluator_t *self ) {
  int64_t n = iEvaluator.ClearDWordSet( self->context.memory );
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * xsetlen() -> size
 *
 *
 ***********************************************************************
 */
static void __eval_maps_xsetlen( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  STACK_RETURN_INTEGER( self, mem->dwset.sz );
}



/*******************************************************************//**
 * xsetini( size ) -> size, or -1 on error
 *
 *
 ***********************************************************************
 */
static void __eval_maps_xsetini( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *x = POP_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  iEvaluator.ClearDWordSet( self->context.memory );

  if( x->type != STACK_ITEM_TYPE_INTEGER ) {
    STACK_RETURN_INTEGER( self, -1 );
  }

  int mag = imag2( 2 * x->integer / VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE );
  mem->dwset.mask = (1 << mag) - 1;
  if( __maps__dwset_expand( mem, MAPS_KEYMODE__INTEGER ) < 0 ) {
    STACK_RETURN_INTEGER( self, -1 );
  }

  STACK_RETURN_INTEGER( self, x->integer );

}





#endif
