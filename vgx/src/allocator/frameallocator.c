/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    frameallocator.c
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

#include "_cxmalloc.h"
#include "_framehash.h"
#include "frameallocator.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );

/*******************************************************************//**
 * FrameAllocator
 ***********************************************************************
 */
#ifndef MINIMAL_FOOTPRINT
#define MINIMAL_FOOTPRINT 0
#endif

#if MINIMAL_FOOTPRINT
#define __BLOCK_BYTES (1ULL<<21)  // number of framehash slots (incl. alloc overhead) per data block in allocators (2MB = 32768 slots = 1 PD entry = 512 full frames)
#elif defined FRAMEHASH_FRAME_BLOCK_SIZE_MB
#define __BLOCK_BYTES ((size_t)FRAMEHASH_FRAME_BLOCK_SIZE_MB << 20)
#else
#define __BLOCK_BYTES (1ULL<<26)  // number of framehash slots (incl. alloc overhead) per data block in allocators (64MB = 1048576 slots = 32 PD entries = 16384 full frames)
#endif

#define __MAX_FRAMEHASH_SLOTS ( (1<<(_FRAMEHASH_P_MAX)) - 1 )



CXMALLOC_TEMPLATE( _framehash_frameallocator__,   /* ATYPE:           generated type is FrameAllocator                    */
                   framehash_slot_t,              /* UTYPE:           sizeof(framehash_slot_t) = 64, i.e. a cache line    */
                   slots,                         /* Array:           array will be known as "slots" in method signatures */
                   __BLOCK_BYTES,                 /* BlockBytes:                                                          */
                   __MAX_FRAMEHASH_SLOTS,         /* MaxArraySize:    aidx=6 => size=63 with S=0                          */
                   sizeof(framehash_halfslot_t),  /* HeaderSize:      32 bytes, two header cells                          */
                   0,                             /* Growth:          S=0 => 0, 1, 3, 7, 15, 31, 63                       */
                   0,                             /* AllowOversized:  disallow oversized                                  */
                   _FRAMEHASH_P_MAX+1             /* MaxAllocators:   7 => aidx 0 - 6                                     */
                 )


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __FRAMEHASH_HEADER_METAS( lineptr )     ((framehash_metas_t*)(CXMALLOC_META_FROM_ARRAY( _framehash_frameallocator__, 1, lineptr )))
#define __FRAMEHASH_HEADER_SLOTS( lineptr )     ((framehash_slot_t**)(CXMALLOC_META_FROM_ARRAY( _framehash_frameallocator__, 2, lineptr )))
#define __FRAMEHASH_HEADER_TOPCELL( lineptr )   ((framehash_cell_t*)CXMALLOC_TOPAPTR_FROM_ARRAY( _framehash_frameallocator__, lineptr ))


 
 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_frameallocator__initialize( framehash_cell_t *frameref ) {
  _framehash_frameallocator__update_top_cell( frameref );
  framehash_halfslot_t *halfslot = _framehash_frameallocator__header_half_slot( frameref );
  framehash_cell_t *cell = halfslot->cells;
  for( int i=0; i<FRAMEHASH_CELLS_PER_HALFSLOT; i++, cell++ ) {
    APTR_AS_ANNOTATION( cell ) = FRAMEHASH_KEY_NONE;
    APTR_AS_QWORD( cell ) = FRAMEHASH_REF_NULL;
  }
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_metas_t _framehash_frameallocator__get_metas( const framehash_cell_t *frameref ) {
  return *__FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) );
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _framehash_frameallocator__top_cell_from_slot_array( const framehash_slot_t *slots ) {
  return CXMALLOC_TOPAPTR_FROM_ARRAY( _framehash_frameallocator__, slots );
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _framehash_frameallocator__update_top_cell( framehash_cell_t *frameref ) {
  framehash_cell_t *frametop = __FRAMEHASH_HEADER_TOPCELL( CELL_GET_FRAME_SLOTS(frameref) );
  APTR_COPY( frametop, frameref );
  return frametop;
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _UNROLLED_framehash_frameallocator__update_top_cell( framehash_cell_t *frameref ) {
  // THIS function is here to document the insanity that goes on with framehash when we update the top cell from the supplied
  // reference. All this does is to enter into the very first 32 bytes of the frame the framehash meta and the additional qword representing the
  // address of itself.

  /* the actual frametop is a pointer to the frame's first byte cast */
  framehash_cell_t *frametop = (framehash_cell_t*) (
    /* get the address of annotated pointer */
    &
    /* frame meta as annotated pointer */
    (
      (cxmalloc_metaflex_t*) (
        /* start of the frame slots */
        (char*) (
          /* convert the packed pointer stored in the frameref to a proper memory address */
          ( (int64_t)((&frameref->tptr)->ncptr.qwo) << (3 + __TPTR_SIGN_EXTEND_SHIFT) ) >> __TPTR_SIGN_EXTEND_SHIFT
        ) 
        -
        /* rewind 64 bytes to the frame meta */
        g__framehash_frameallocator___family->header_bytes
      )
    )->aptr
  );

  frametop->annotation = frameref->annotation;
  (&frametop->tptr)->qword = (&frameref->tptr)->qword;

  return frametop;
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN uint16_t _framehash_frameallocator__get_chainbits( const framehash_cell_t *frameref ) {
  return __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->chainbits;
}


 /*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN uint16_t _framehash_frameallocator__set_chainbits( framehash_cell_t *frameref, const uint16_t chainbits ) {
  __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->chainbits = chainbits;
  return chainbits;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN uint16_t _framehash_frameallocator__get_fileno( const framehash_cell_t *frameref ) {
  return __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->fileno;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN uint16_t _framehash_frameallocator__set_fileno( framehash_cell_t *frameref, const uint16_t fileno ) {
  __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->fileno = fileno;
  return fileno;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_ftype_t _framehash_frameallocator__get_ftype( const framehash_cell_t *frameref ) {
  return __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->ftype;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_ftype_t _framehash_frameallocator__set_ftype( framehash_cell_t *frameref, const framehash_ftype_t ftype ) {
  __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->ftype = (uint8_t)ftype;
  return (uint8_t)ftype;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_frameallocator__get_domain( const framehash_cell_t *frameref ) {
  return __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->domain;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_frameallocator__set_domain( framehash_cell_t *frameref, const int domain ) {
  __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->domain = (uint8_t)domain;
  return (uint8_t)domain;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_frameallocator__get_order( const framehash_cell_t *frameref ) {
  return __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->order;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_frameallocator__set_order( framehash_cell_t *frameref, const int order ) {
  __FRAMEHASH_HEADER_METAS( CELL_GET_FRAME_SLOTS(frameref) )->order = (uint8_t)order;
  return (uint8_t)order;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_halfslot_t * _framehash_frameallocator__header_half_slot( framehash_cell_t *frameref ) {
  return CXMALLOC_OBJECT_FROM_ARRAY( framehash_halfslot_t, _framehash_frameallocator__, CELL_GET_FRAME_SLOTS(frameref) );
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN bool _framehash_frameallocator__is_frame_valid( const framehash_cell_t * const frameref ) {
  framehash_metas_t *refmetas = CELL_GET_FRAME_METAS( frameref );
  framehash_slot_t *refslots = CELL_GET_FRAME_SLOTS( frameref );

  // Slots should be allocated
  if( refslots == NULL ) {
    return false;
  }

  // No further check for caches (malloc used)
  if( refmetas->ftype == FRAME_TYPE_CACHE ) {
    return true;
  }

  // Allocator refcount for frame allocator managed frames should be positive
  void *obj = CXMALLOC_OBJECT_FROM_ARRAY( void, _framehash_frameallocator__, refslots );
  if( _cxmalloc_object_refcnt_nolock( obj ) <= 0 ) {
    return false;
  }

  // Static parts of the metas should be the same in the reference and the actual frame's meta
  framehash_metas_t *metas = __FRAMEHASH_HEADER_METAS( refslots );
  if( metas->domain     != refmetas->domain ||
      metas->order      != refmetas->order ||
      metas->ftype      != refmetas->ftype ||
      metas->chainbits  != refmetas->chainbits )
  {
    return false;
  }

  // LEAF frame order and domain are within bounds
  if( metas->ftype == FRAME_TYPE_LEAF ) {
    if( metas->order <= _FRAMEHASH_P_MAX && metas->domain <= FRAME_DOMAIN_LAST_FRAME ) {
      return true;
    }
    else {
      return false;
    }
  }

  // INTERNAL frames are always the maximum order, domain is within bounds and chain(s) exist
  if( metas->ftype == FRAME_TYPE_INTERNAL ) {
    if( metas->order == _FRAMEHASH_P_MAX && metas->domain <= FRAME_DOMAIN_LAST_FRAME && metas->chainbits > 0 ) {
      return true;
    }
    else {
      return false;
    }
  }

  // BASEMENT
  if( metas->ftype == FRAME_TYPE_BASEMENT ) {
    // TODO: Implement check
    return true;
  }

  // INALID frame type
  return false;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_frameallocator.h"


DLL_HIDDEN test_descriptor_t _framehash_frameallocator_tests[] = {
  { "frameallocator",   __utest_framehash_frameallocator },
  {NULL}
};
#endif
