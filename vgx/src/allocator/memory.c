/*######################################################################
 *#
 *# memory.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 * _framehash_memory__new_frame_allocator
 *
 ***********************************************************************
 */
DLL_HIDDEN cxmalloc_family_t * _framehash_memory__new_frame_allocator( const char *description, size_t block_size ) {
  cxmalloc_family_t *falloc = NULL;
  char strbuf[OBJECTID_LONGSTRING_MAX+1]; 

  cxmalloc_descriptor_t falloc_desc = {
    .meta = {
      .initval          = {0},                            /*                                                        */
      .serialized_sz    = 0                               /*                                                        */
    },
    .obj = {
      .sz               = sizeof( framehash_halfslot_t ), /* 2 header cells in the "object" portion - slight abuse  */
      .serialized_sz    = 0
    },
    .unit = {
      .sz               = sizeof( framehash_slot_t ),     /* slot with 4 cells                                      */
      .serialized_sz    = 0
    },
    .serialize_line     = NULL, // we manage serialization ourselves, so we don't register
    .deserialize_line   = NULL, // any serialization callbacks with cxmalloc 
    .parameter = {
      .block_sz         = block_size,                     /*                                                        */
      .line_limit       = (1<<_FRAMEHASH_P_MAX)-1,        /* aidx=6 => size=63 with S=0                             */
      .subdue           = 0,                              /* S=0 =>   0:0,  1:1,  2:3,  3:7,  4:15,  5:31,  6:63    */
      .allow_oversized  = 0,                              /* disallow oversized                                     */
      .max_allocators   = _FRAMEHASH_P_MAX+1              /* 7 => aidx 0 - 6                                        */
    },
    .persist = {
      .CSTR__path            = NULL,                      /* the allocator is non-persistent                        */
    },
    .auxiliary = {
      NULL                                                /* no associated auxiliary objects                        */
    }
  };

  cxmalloc_family_constructor_args_t falloc_args = {
    .family_descriptor  = &falloc_desc //
  };

  snprintf( strbuf, OBJECTID_LONGSTRING_MAX, "Framehash Frame Allocator (%s)", description );
  falloc = COMLIB_OBJECT_NEW( cxmalloc_family_t, strbuf, &falloc_args );

#ifdef DEBUG_ALLOCATORS
  COMLIB_OBJECT_PRINT( falloc );
#endif

  return falloc;

}



/*******************************************************************//**
 * _framehash_memory__new_basement_allocator
 *
 ***********************************************************************
 */
DLL_HIDDEN cxmalloc_family_t * _framehash_memory__new_basement_allocator( const char *description, size_t block_size ) {
  cxmalloc_family_t *balloc = NULL;
  char strbuf[OBJECTID_LONGSTRING_MAX+1]; 
  
  // Let's make our own basement allocator
  cxmalloc_descriptor_t balloc_desc = {
    .meta = {
      .initval          = {0},                        /*                            */
      .serialized_sz    = 0                           /*                            */
    },
    .obj = {
      .sz               = 0,                          /* no object data             */
      .serialized_sz    = 0                           /*                            */
    },
    .unit = {
      .sz               = sizeof( framehash_cell_t ), /* 1 cell                     */
      .serialized_sz    = 0                           /*                            */
    },
    .serialize_line     = NULL, // we manage serialization ourselves, so we don't register
    .deserialize_line   = NULL, // any serialization callbacks with cxmalloc 
    .parameter = {
      .block_sz         = block_size,                 /*                            */
      .line_limit       = _FRAMEHASH_BASEMENT_SIZE,   /* aidx=2 => size=6 with S=0  */
      .subdue           = 0,                          /* S=0 =>   0:0,  1:2,  2:6   */
      .allow_oversized  = 0,                          /* disallow oversized         */
      .max_allocators   = 3                           /* 3 => aidx 0 - 2            */
    },
    .persist = {
      .CSTR__path            = NULL,                  /* the allocator is non-persistent  */
    },
    .auxiliary = {
      NULL                                            /* no associated auxiliary objects */
    }
  };


  cxmalloc_family_constructor_args_t falloc_args = {
    .family_descriptor  = &balloc_desc //
  };

  snprintf( strbuf, OBJECTID_LONGSTRING_MAX, "Framehash Basement Allocator (%s)", description );
  balloc = COMLIB_OBJECT_NEW( cxmalloc_family_t, strbuf, &falloc_args );

#ifdef DEBUG_ALLOCATORS
  COMLIB_OBJECT_PRINT( balloc );
#endif

  return balloc;

}



/*******************************************************************//**
 * _framehash_memory__new_frame
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_slot_t * _framehash_memory__new_frame( framehash_context_t *context, const int order_p, const int domain, const framehash_ftype_t ftype ) {
  cxmalloc_family_t *allocator;
  framehash_slot_t *new_slots = NULL;
  framehash_cell_t *target = context->frame;
  framehash_metas_t *target_metas = CELL_GET_FRAME_METAS( target );
  framehash_slot_t *slot;
  framehash_cell_t *cell;
  uint32_t nslots;

  target_metas->QWORD = 0;
  target_metas->order = (uint8_t)order_p;
  target_metas->domain = (uint8_t)domain;
  target_metas->ftype = (uint8_t)ftype;

  int max_cache_domain;

  if( context->dynamic ) {
    allocator = context->dynamic->falloc;
    max_cache_domain = context->dynamic->cache_depth;
  }
  else {
    allocator = iFramehash.memory.DefaultFrameAllocator();
    max_cache_domain = FRAMEHASH_DEFAULT_MAX_CACHE_DOMAIN;
  }

  // TODO: we only support memory-based frames at the moment

  switch( ftype ) {
  case FRAME_TYPE_CACHE:
    if( order_p <= _FRAMEHASH_MAX_CACHE_FRAME_ORDER ) {
      // number of cache slots
      nslots = _CACHE_FRAME_NSLOTS( order_p );
      // cache frames are regular malloc memory - alignment is the size itself to prevent straddling higher levels in the page table radix tree.
      ALIGNED_ARRAY( new_slots, framehash_slot_t, nslots, nslots * sizeof(framehash_slot_t) );
      if( new_slots ) {
        // Initialize reference and metas
        _SET_CELL_REFERENCE( target, new_slots );
        target_metas->nchains = 0; // no chains hooked up yet
        target_metas->flags.cancache = 1;  // of course
        target_metas->flags.cachetyp = target_metas->domain == 0 ? FCACHE_TYPE_STATIC : FCACHE_TYPE_CMORPH; // domain=0 is STATIC!
        target_metas->flags.readonly = 0;
        // Initialize cache slots
        slot = new_slots;
        for( uint32_t fx=0; fx<nslots; fx++, slot++ ) {
          cell = slot->cells;
          for( int j=0; j<_FRAMEHASH_CACHE_CELLS_PER_SLOT; j++, cell++ ) {
            // all but the last cell are initialized to EMPTY state
            _INITIALIZE_DATA_CELL( cell, CELL_TYPE_EMPTY );
          }
          // the last cell is a chain cell initialized to NULL
          _INITIALIZE_REFERENCE_CELL( cell, 0xF, 0xF, FRAME_TYPE_NONE );
        }
      }
      else { /* memerror */ }
    }
    else { /* order out of range */ }
    break;
  case FRAME_TYPE_LEAF:
    /* FALLTHRU */
  case FRAME_TYPE_INTERNAL:
    if( order_p <= _FRAMEHASH_P_MAX ) {
      nslots = _FRAME_NSLOTS( order_p );
      if( (new_slots = CALLABLE(allocator)->New(allocator, nslots)) != NULL ) {
        // Initialize reference and metas
        _SET_CELL_REFERENCE( target, new_slots );
        target_metas->flags.cancache = target_metas->domain <= max_cache_domain ? 1 : 0; // caching allowed if not exceeding max domain
        target_metas->flags.cachetyp = target_metas->flags.cancache ? FCACHE_TYPE_CMORPH : FCACHE_TYPE_NONE; // either morph cache or no cache
        target_metas->flags.readonly = 0;
        _framehash_frameallocator__initialize( target );
        // Initialize header's halfslot cells
        framehash_halfslot_t *halfslot = _framehash_frameallocator__header_half_slot( target );
        cell = halfslot->cells;
        for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++, cell++ ) {
          _INITIALIZE_DATA_CELL( cell, CELL_TYPE_END );
        }
        // Initialize frame slots
        slot = new_slots;
        for( uint32_t fx=0; fx<nslots; fx++, slot++ ) {
          cell = slot->cells;
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
            _INITIALIZE_DATA_CELL( cell, CELL_TYPE_END );
          }
        }
      }
      else { /* memerror */ }
    }
    else { /* order out of range */ }
    break;
  default:
    /* error */
    break;
  }

  return new_slots;
}



/*******************************************************************//**
 * _framehash_memory__discard_frame
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_memory__discard_frame( framehash_context_t * const context ) {
  cxmalloc_family_t *allocator;
  framehash_cell_t *frame = context->frame;
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  int p = frame_metas->order;
  int64_t n_active = frame_metas->nactive;
  framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( frame );
  int nslots;

  // Detect invalid call
  if( frame == NULL || _CELL_REFERENCE_IS_NULL( frame ) ) {
    return -1;
  }

  if( context->dynamic ) {
    allocator = context->dynamic->falloc;
  }
  else {
    allocator = iFramehash.memory.DefaultFrameAllocator();
  }

  switch( frame_metas->ftype ) {
  case FRAME_TYPE_CACHE:
    // number of cache slots
    nslots = _CACHE_FRAME_NSLOTS( p );
    for( int fx=0; fx<nslots; fx++ ) {
      // last cell in cache slot is chain cell
      context->frame = &frame_slots[fx].cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
      n_active += _framehash_memory__discard_frame( context );
    }
    context->frame = frame;
    frame_metas->nchains = 0;
    // now that all children have been taken care of, free ourselves


    // TODO: sometimes self_slots == g_dummy_chain, causing crash.  (Maybe this is fixed? 20151028)
    // This happens if we fail somewhere in the middle of deserialization.
    // 

    ALIGNED_FREE( frame_slots );
    
    break;
  case FRAME_TYPE_LEAF:
    // no children possible in leaf, just discard ourselves
    CALLABLE( allocator )->Discard( allocator, frame_slots );
    break;
  case FRAME_TYPE_INTERNAL:
    // number of chainslots
    nslots = _FRAME_NCHAINSLOTS( p );
    // first slot in chainzone
    framehash_slot_t *chainslot = frame_slots + _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), 0 );
    for( int q=0; q<nslots; q++ ) {
      // scan all chainslots
      if( _framehash_radix__is_chain( frame_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
        // slot is a chain, now discard all children
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          context->frame = &chainslot[q].cells[j];
          n_active += _framehash_memory__discard_frame( context );
        }
        context->frame = frame;
      }
    }
    frame_metas->chainbits = 0;
    _framehash_frameallocator__set_chainbits( frame, 0 );
    // now that all children have been taken care of, discard ourselves
    CALLABLE( allocator )->Discard( allocator, frame_slots );
    break;
  case FRAME_TYPE_BASEMENT:
    // forward to basement handler
    n_active += _framehash_memory__discard_basement( context );
    break;
  default:
    return n_active;
  }
  CELL_GET_FRAME_METAS( frame )->QWORD = 0;
  DELETE_CELL_REFERENCE( frame );

  return n_active;
}



/*******************************************************************//**
 * _framehash_memory__new_basement
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _framehash_memory__new_basement( framehash_context_t *context, const int domain ) {
  cxmalloc_family_t *allocator;
  framehash_cell_t *target = context->frame;
  framehash_metas_t *target_metas = CELL_GET_FRAME_METAS(target);
  framehash_cell_t *target_cells = NULL;
  framehash_cell_t *cell;

  if( context->dynamic ) {
    allocator = context->dynamic->balloc;
  }
  else {
    allocator = iFramehash.memory.DefaultBasementAllocator();
  }

  target_metas->QWORD = 0;
  target_metas->domain = (uint8_t)domain;
  target_metas->ftype = (uint8_t)FRAME_TYPE_BASEMENT;
  target_metas->flags.cancache = 0;
  target_metas->flags.cachetyp = FCACHE_TYPE_NONE;
  // TODO: we only support memory-based basements at the moment

  if( (target_cells = CALLABLE( allocator )->New( allocator, _FRAMEHASH_BASEMENT_SIZE )) != NULL ) {
    _SET_CELL_REFERENCE( target, target_cells );
    _framehash_basementallocator__initialize( target );
    cell = target_cells;
    for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++, cell++ ) {
      _INITIALIZE_DATA_CELL( cell, CELL_TYPE_END );
    }
  }

  return target_cells;
}



/*******************************************************************//**
 * _framehash_memory__discard_basement
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_memory__discard_basement( framehash_context_t *context ) {
  cxmalloc_family_t *allocator;
  framehash_cell_t *basement = context->frame;
  framehash_cell_t *chain;
  int64_t n_active = 0;
  
  if( basement == NULL || _CELL_REFERENCE_IS_NULL( basement ) ) {
    return 0;
  }

  n_active = CELL_GET_FRAME_METAS( basement )->nactive;

  if( context->dynamic ) {
    allocator = context->dynamic->balloc;
  }
  else {
    allocator = iFramehash.memory.DefaultBasementAllocator();
  }

  chain = _framehash_basementallocator__get_chain_cell( basement );

  if( _CELL_REFERENCE_EXISTS( chain ) ) {
    context->frame = chain;
    n_active += _framehash_memory__discard_basement( context );
    context->frame = basement;
  }

  CALLABLE( allocator )->Discard( allocator, CELL_GET_FRAME_CELLS(basement) );

  CELL_GET_FRAME_METAS( basement )->QWORD = 0;
  DELETE_CELL_REFERENCE( basement );

  return n_active;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_memory.h"


DLL_HIDDEN test_descriptor_t _framehash_memory_tests[] = {
  { "Cache Frame Allocation",       __utest_framehash_memory__cache },
  { "Leaf Frame Allocation",        __utest_framehash_memory__leaf },
  { "Internal Frame Allocation",    __utest_framehash_memory__internal },
  { "Basement Allocation",          __utest_framehash_memory__basement },
  {NULL}
};
#endif



