/*######################################################################
 *#
 *# processor.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


typedef int64_t (*f_process_level)( framehash_processing_context_t * const processor, int64_t *nproc );

static int64_t    __cache_process( framehash_processing_context_t * const processor, int64_t *nproc );
static int64_t     __leaf_process( framehash_processing_context_t * const processor, int64_t *nproc );
static int64_t __internal_process( framehash_processing_context_t * const processor, int64_t *nproc );
static int64_t __basement_process( framehash_processing_context_t * const processor, int64_t *nproc );
static int64_t     __none_process( framehash_processing_context_t * const processor, int64_t *nproc );


static const f_process_level PROCESS[8] = {
  __cache_process,        // FRAME_TYPE_CACHE = 0
  __leaf_process,         // FRAME_TYPE_LEAF = 1
  __internal_process,     // FRAME_TYPE_INTERNAL = 2
  __basement_process,     // FRAME_TYPE_BASEMENT = 3
  NULL,                   // (undefined)
  NULL,                   // (undefined)
  NULL,                   // (undefined)
  __none_process,         // FRAME_TYPE_NONE = 7
};


static int64_t __cache_process_partial( framehash_processing_context_t * const processor, int64_t *nproc, uint64_t selector );


#define __PUSH_FRAME( ContextPtr, Frame )                               \
  do {                                                                  \
    /* Processing context */                                            \
    framehash_processing_context_t * const __processor__ = ContextPtr;  \
    /* Save current frame */                                            \
    framehash_cell_t * const __frame__ = __processor__->instance.frame; \
    /* Push next frame */                                               \
    __processor__->instance.frame = Frame;                              \
    do


#define __POP_FRAME                                   \
    WHILE_ZERO;                                       \
    /* Restore previous frame */                      \
    __processor__->instance.frame = __frame__;        \
  } WHILE_ZERO



#define __PREFETCH_AHEAD_CL_A             4
#define __PREFETCH_AHEAD_CL_B             8
#define __ADDRESS_CL_MASK                 0xFFFFFFFFFFFFFFC0ULL
#define __ADDRESS_CL_ALIGNED_MASK         0x000000000000003FULL
#define __ADDRESS_256_ALIGNED_MASK        0x000000000000001FULL
#define __ADDRESS_IS_CL_ALIGNED( Address )  (((uintptr_t)(Address) & __ADDRESS_CL_ALIGNED_MASK) == 0)
#define __ADDRESS_IS_256_ALIGNED( Address ) (((uintptr_t)(Address) & __ADDRESS_256_ALIGNED_MASK) == 0)



/*******************************************************************//**
 * __follow_chain
 * Returns    : 0 on success and processing should continue.
 *            : non-zero when processing should stop, either due to error
 *              or because processing has reached max limit or processing
 *              completed.
 ***********************************************************************
 */
__inline static int __follow_chain( framehash_processing_context_t * const processor, framehash_cell_t * const chain, int64_t *nproc ) {
  int64_t pr = 0;
  if( !_CELL_REFERENCE_IS_NULL( chain ) ) {
    // Keep the chain cells close the processor
    __prefetch_L1( chain );
    framehash_ftype_t ftype = CELL_GET_FRAME_TYPE( chain );
    __PUSH_FRAME( processor, chain ) {
      switch( ftype ) {
      case FRAME_TYPE_CACHE:
        pr = __cache_process( processor, nproc );
        break;
      case FRAME_TYPE_LEAF:
        pr = __leaf_process( processor, nproc );
        break;
      case FRAME_TYPE_INTERNAL:
        pr = __internal_process( processor, nproc );
        break;
      case FRAME_TYPE_BASEMENT:
        pr = __basement_process( processor, nproc );
        break;
      case FRAME_TYPE_NONE:
        pr = 0;
        break;
      default:
        pr = -1;
      }
    } __POP_FRAME;
  }

  return pr < 0 || pr >= processor->processor.limit || processor->flags.completed;
}



/*******************************************************************//**
 * __process_cell_region
 * Returns    : 0 on success and processing should continue.
 *            : negative on failure, *nproc will be set to -1 and
 *              processor->failed will be set to true.
 *            : positive on completion, *nproc will be set to the
 *              number of processed cells and processor->completion
 *              will be set to true.
 ***********************************************************************
 */
__inline static int __process_cell_region( framehash_processing_context_t * const processor, framehash_cell_t **start, const framehash_cell_t * const end, int64_t *nproc ) {
  int64_t prstate = 0;
#define __process_cell( cell )                                            \
  if( _ITEM_IS_VALID( (cell) ) ) {                                        \
    if( (prstate = proc( processor, (cell) )) >= 0 ) {                    \
      if( (*nproc += prstate) >= limit || processor->flags.completed ) {  \
        goto completed;                                                   \
      }                                                                   \
    }                                                                     \
    else {                                                                \
      goto error;                                                         \
    }                                                                     \
  }

  const f_framehash_cell_processor_t proc = processor->processor.function;
  const int64_t limit = processor->processor.limit;

  if( !__ADDRESS_IS_256_ALIGNED( *start ) ) {
    FATAL( 0, "framehash cell alignment error" );
  }

  framehash_cell_t *cell = *start;

  // --------------------
  // We will mark the processed cache lines as non-temporal
  // to minimize cache pollution
  // --------------------

  // First prefetch at current cache line
  cacheline_t *pfaddr = ((cacheline_t*)((uintptr_t)cell & __ADDRESS_CL_MASK));
  // Stop prefetching when this CL is reached
  cacheline_t *pfend = ((cacheline_t*)((uintptr_t)end & __ADDRESS_CL_MASK));
  // Number of CLs to be ahead in the prefetch sequence 
  int pf_ahead = 3;
  // Issue initial prefetches
  while( pfaddr < pfend && pf_ahead-- > 0 ) {
    __prefetch_L2( pfaddr++ );
  }

  // Process header cells
  if( !__ADDRESS_IS_CL_ALIGNED( cell ) ) {
    __process_cell( cell )
    ++cell;
    __process_cell( cell )
    ++cell;
  }

  // Process slots
  while( cell < end ) {
    if( pfaddr < pfend ) {
      __prefetch_nta( pfaddr++ );
    }
    __process_cell( cell )
    ++cell;
    __process_cell( cell )
    ++cell;
    __process_cell( cell )
    ++cell;
    __process_cell( cell )
    ++cell;
  }

  // Done with this region
  *start = (framehash_cell_t*)end;
  return 0;

// Processing completed
completed:
  return 1;

// Processing error
error:
  *nproc = -1;
  processor->flags.failed = true;
  return -1;
}




/*******************************************************************//**
 * __cache_process
 *
 ***********************************************************************
 */
static int64_t __cache_process( framehash_processing_context_t * const processor, int64_t *nproc ) {
  framehash_cell_t * const cache = processor->instance.frame;
  framehash_slot_t *slot = CELL_GET_FRAME_SLOTS(cache);
  framehash_cell_t *chain, *cursor;
  const framehash_cell_t * end;
  int nslots = _CACHE_FRAME_NSLOTS( CELL_GET_FRAME_METAS(cache)->order );

  XDO {
    // Horizontal -->
    for( int fx=0; fx<nslots; fx++, slot++ ) {
      cursor = slot->cells;
      end = cursor + _FRAMEHASH_CACHE_CELLS_PER_SLOT;

      // ALLOW CACHED ITEMS
      // Process cache cells just like normal cells
      if( processor->flags.allow_cached ) {
        // Vertical | $ |  <- process this
        //          | $ |  <- and this
        //          | $ |  <- and this
        //          |*->|
        if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
          XBREAK;
        }
      }
      // DISALLOW CACHED ITEMS
      // Do not process caches. Flush and invalidate all cached items before we follow
      // chains down the tree to process the items.
      else {
        framehash_context_t context = CONTEXT_INIT_TOP_FRAME( cache, processor->instance.dynamic );
        framehash_retcode_t flushing = _framehash_cache__flush_slot( &context, slot, true );
        if( flushing.error == true || flushing.completed == false ) {
          *nproc = -1;
          processor->flags.failed = true;
          XBREAK;
        }
      }
      // Chaincell |*->|  (may have a NULL reference if there is no chain below)
      chain = &slot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
      if( __follow_chain( processor, chain, nproc ) != 0 ) {
        XBREAK;
      }
    }
  }
  XFINALLY {
  }

  return *nproc;
}



/*******************************************************************//**
 * __cache_process_partial
 *
 ***********************************************************************
 */
static int64_t __cache_process_partial( framehash_processing_context_t * const processor, int64_t *nproc, uint64_t selector ) {
  framehash_cell_t * const cache = processor->instance.frame;
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( cache );
  int fx = _FRAMEHASH_TOPINDEX( metas->order, selector );
  framehash_slot_t *slot = CELL_GET_FRAME_SLOTS( cache ) + fx;
  
  XDO {
    framehash_cell_t *cursor = slot->cells;
    framehash_cell_t *end = cursor + _FRAMEHASH_CACHE_CELLS_PER_SLOT;

    // ALLOW CACHED ITEMS
    // Process cache cells just like normal cells
    if( processor->flags.allow_cached ) {
      // Vertical | $ |  <- process this
      //          | $ |  <- and this
      //          | $ |  <- and this
      //          |*->|
      if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
        XBREAK;
      }
    }
    // DISALLOW CACHED ITEMS
    // Do not process caches. Flush and invalidate all cached items before we follow
    // chains down the tree to process the items.
    else {
      framehash_context_t context = CONTEXT_INIT_TOP_FRAME( cache, processor->instance.dynamic );
      framehash_retcode_t flushing = _framehash_cache__flush_slot( &context, slot, true );
      if( flushing.error == true || flushing.completed == false ) {
        *nproc = -1;
        processor->flags.failed = true;
        XBREAK;
      }
    }
    // Chaincell |*->|  (may have a NULL reference if there is no chain below)
    framehash_cell_t *chain = &slot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
    if( __follow_chain( processor, chain, nproc ) != 0 ) {
      XBREAK;
    }
  }
  XFINALLY {
  }

  return *nproc;
}



/*******************************************************************//**
 * __leaf_process
 *
 ***********************************************************************
 */
#define __USE_HALFSLOT( Order_p )           ( (Order_p) <= _FRAMEHASH_MAX_P_SMALL_FRAME )
static int64_t __leaf_process( framehash_processing_context_t * const processor, int64_t *nproc ) {
  framehash_cell_t * const leaf = processor->instance.frame;
  XDO {
    // Process the leaf frame as one contiguous region, starting at the header cells
    int p = CELL_GET_FRAME_METAS( leaf )->order;
    int n = _FRAME_NSLOTS( p ) * FRAMEHASH_CELLS_PER_SLOT;
    framehash_cell_t *cursor;
    // Include header's halfslot in processing
    if( __USE_HALFSLOT( p ) ) {
      n += FRAMEHASH_CELLS_PER_HALFSLOT;
      cursor = _framehash_frameallocator__header_half_slot(leaf)->cells;
    }
    // Don't include halfslot
    else {
      cursor = CELL_GET_FRAME_SLOTS( leaf )->cells;
    }
    const framehash_cell_t * const end = cursor + n;
    if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
      XBREAK;
    }
  }
  XFINALLY {
  }

  return *nproc;
}



/*******************************************************************//**
 * __internal_process
 *
 ***********************************************************************
 */
static int64_t __internal_process( framehash_processing_context_t * const processor, int64_t *nproc ) {

  framehash_cell_t * const intern = processor->instance.frame;

  framehash_cell_t * const start = CELL_GET_FRAME_SLOTS(intern)->cells;
  const framehash_cell_t *end;
  framehash_cell_t *cursor;
  framehash_metas_t *internal_metas = CELL_GET_FRAME_METAS(intern);
  int p = internal_metas->order;
  int nchainslots = _FRAME_NCHAINSLOTS( p );


  XDO {
    // NOTE: internal frames do not use header cells! No need to process these.
    cursor = start;
    // All zones
    for( int k=p-1; k >= 0; k-- ) {
      // Chain zone
      if( k == _FRAME_CHAINZONE(p) ) {
        for( int q=0; q<nchainslots; q++ ) {
          end = cursor + FRAMEHASH_CELLS_PER_SLOT;
          // Chain slot
          if( _framehash_radix__is_chain( internal_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
            do {
              if( __follow_chain( processor, cursor, nproc ) != 0 ) {
                XBREAK;
              }
            } while( ++cursor < end );
          }
          // Leaf slot
          else {
            if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
              XBREAK;
            }
          }
        }
      }
      // Leaf zone
      else {
        end = cursor + _FRAME_ZONESLOTS(k) * (int)FRAMEHASH_CELLS_PER_SLOT;
        if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
          XBREAK;
        }
      }
    }
  }
  XFINALLY {
  }

  return *nproc;
}



/*******************************************************************//**
 * __basement_process
 *
 ***********************************************************************
 */
static int64_t __basement_process( framehash_processing_context_t * const processor, int64_t *nproc ) {
  framehash_cell_t * const basement = processor->instance.frame;
  framehash_cell_t *cursor = CELL_GET_FRAME_CELLS(basement);
  const framehash_cell_t * const end = cursor + _FRAMEHASH_BASEMENT_SIZE;
  
  XDO {
    // process this basement's cells
    if( __process_cell_region( processor, &cursor, end, nproc ) != 0 ) {
      XBREAK;
    }

    // process next basement's cells
    if( CELL_GET_FRAME_METAS(basement)->hasnext ) {
      framehash_cell_t *chain = _framehash_basementallocator__get_chain_cell( basement );
      if( __follow_chain( processor, chain, nproc ) ) {
        XBREAK;
      }
    }
  }
  XFINALLY {
  }

  return *nproc;
}



/*******************************************************************//**
 * __none_process
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __none_process( framehash_processing_context_t * const processor, int64_t *nproc ) {
  return *nproc;
}



/*******************************************************************//**
 * count_nactive_limit
 *
 ***********************************************************************
 */
// CELL PROCESSOR
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_count_nactive( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  return 1;
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_count_nactive_limit( framehash_cell_t *frame, framehash_dynamic_t *dynamic, int64_t limit ) {
  if( limit < 0 ) {
    limit = LLONG_MAX;
  }

  framehash_processing_context_t count_active_limit = FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( frame, dynamic, __f_process_count_nactive, limit );
  return _framehash_processor__process( &count_active_limit );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__count_nactive_limit( framehash_t * const self, int64_t limit ) {
  return _framehash_processor__subtree_count_nactive_limit( &self->_topframe, &self->_dynamic, limit );
}



/*******************************************************************//**
 * count_nactive
 *
 ***********************************************************************
 */

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_count_nactive( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  return _framehash_processor__subtree_count_nactive_limit( frame, dynamic, LLONG_MAX );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__count_nactive( framehash_t * const self ) {
  return _framehash_processor__subtree_count_nactive( &self->_topframe, &self->_dynamic );
}



/*******************************************************************//**
 * destroy_objects
 *
 *
 * WARNING: The object destructor is called and the cell item is 
 * replaced with a MEMBERSHIP item whose shortid is the same as
 * the object's and whose IdHigh equals the deleted object's address.
 * This keeps the framehash correctly populated and the item counts consistent.
 ***********************************************************************
 */
// CELL PROCESSOR
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_destroy_objects( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_DESTRUCTIBLE( cell ) ) {
    comlib_object_t *obj = _CELL_GET_OBJECT128( cell );
    COMLIB_OBJECT_DESTROY( obj );
    APTR_SET_IDHIGH_AND_TAG( cell, (QWORD)obj, CELL_TYPE_RAW56 );
    return 1;
  }
  return 0;
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_destroy_objects( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t destroy_objects = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_destroy_objects );
  destroy_objects.flags.readonly = false;
  return _framehash_processor__process( &destroy_objects );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__destroy_objects( framehash_t * const self ) {
  return _framehash_processor__subtree_destroy_objects( &self->_topframe, &self->_dynamic );
}



/*******************************************************************//**
 * collect_cells
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_cell( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  _DEFINE_FUNCTION_POINTER_FROM_DATA( f_framehash_cell_collector_t, collect, processor->processor.output );
  return collect( cell );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_cells( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector ) {
  framehash_processing_context_t collect_cells = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_cell );
  SUPPRESS_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER
  FRAMEHASH_PROCESSOR_SET_IO( &collect_cells, NULL, (void*)collector );
  return _framehash_processor__process( &collect_cells );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_cells( framehash_t * const self, f_framehash_cell_collector_t collector ) {
  return _framehash_processor__subtree_collect_cells( &self->_topframe, &self->_dynamic, collector ); 
}



/*******************************************************************//**
 * collect_cells_filter
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_cell_filter( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  _DEFINE_FUNCTION_POINTER_FROM_DATA( f_framehash_cell_filter_t, filter, processor->processor.input );
  if( filter(cell) ) {
    _DEFINE_FUNCTION_POINTER_FROM_DATA( f_framehash_cell_collector_t, collect, processor->processor.output );
    return collect(cell);
  }
  else {
    return 0;
  }
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_cells_filter( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector, f_framehash_cell_filter_t *filter ) {
  framehash_processing_context_t collect_cells_filter = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_cell_filter );
  SUPPRESS_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER
  FRAMEHASH_PROCESSOR_SET_IO( &collect_cells_filter, filter, (void*)collector ); 
  return _framehash_processor__process( &collect_cells_filter );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_cells_filter( framehash_t * const self, f_framehash_cell_collector_t collector, f_framehash_cell_filter_t *filter ) {
  return _framehash_processor__subtree_collect_cells_filter( &self->_topframe, &self->_dynamic, collector, filter );
}



/*******************************************************************//**
 * collect_cells_into_list
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_cell_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CaptrList_vtable->Append( processor->instance.dynamic->cell_list, cell );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_cells_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_cells_into_list = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_cell_into_list );
  return _framehash_processor__process( &collect_cells_into_list );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_cells_into_list( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_cells_into_list( &self->_topframe, &self->_dynamic ); 
}



/*******************************************************************//**
 * collect_cells_into_heap
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_cell_into_heap( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CaptrHeap_vtable->HeapPush( processor->instance.dynamic->cell_heap, cell );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_cells_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_cells_into_heap = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_cell_into_heap );
  return _framehash_processor__process( &collect_cells_into_heap );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_cells_into_heap( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_cells_into_heap( &self->_topframe, &self->_dynamic ); 
}



/*******************************************************************//**
 * collect_refs_into_list
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_ref_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CtptrList_vtable->Append( processor->instance.dynamic->ref_list, &cell->tptr );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_refs_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_refs_into_list = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_ref_into_list );
  return _framehash_processor__process( &collect_refs_into_list );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_refs_into_list( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_refs_into_list( &self->_topframe, &self->_dynamic ); 
}



/*******************************************************************//**
 * collect_refs_into_heap
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_ref_into_heap( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CtptrHeap_vtable->HeapPush( processor->instance.dynamic->ref_heap, &cell->tptr );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_refs_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_refs_into_heap = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_ref_into_heap );
  return _framehash_processor__process( &collect_refs_into_heap );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_refs_into_heap( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_refs_into_heap( &self->_topframe, &self->_dynamic ); 
}




/*******************************************************************//**
 * collect_annotations_into_list
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_annotation_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CQwordList_vtable->Append( processor->instance.dynamic->annotation_list, &cell->annotation );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_annotations_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_annotations_into_list = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_annotation_into_list );
  return _framehash_processor__process( &collect_annotations_into_list );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_annotations_into_list( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_annotations_into_list( &self->_topframe, &self->_dynamic ); 
}




/*******************************************************************//**
 * collect_annotations_into_heap
 *
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __f_process_collect_annotation_into_heap( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  return g_CQwordHeap_vtable->HeapPush( processor->instance.dynamic->annotation_heap, &cell->annotation );
}

// SUBTREE INTERFACE
DLL_HIDDEN int64_t _framehash_processor__subtree_collect_annotations_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic ) {
  framehash_processing_context_t collect_annotations_into_heap = FRAMEHASH_PROCESSOR_NEW_CONTEXT( frame, dynamic, __f_process_collect_annotation_into_heap );
  return _framehash_processor__process( &collect_annotations_into_heap );
}

// OUTER INTERFACE
DLL_HIDDEN int64_t _framehash_processor__collect_annotations_into_heap( framehash_t * const self ) {
  return _framehash_processor__subtree_collect_annotations_into_heap( &self->_topframe, &self->_dynamic ); 
}



/*******************************************************************//**
 *
 * Cell processor interface
 *
 ***********************************************************************
 */
DLL_HIDDEN IStandardCellProcessors_t _framehash_processor__iStandardCellProcessors = {
  .count_nactive                = __f_process_count_nactive,
  .destroy_objects              = __f_process_destroy_objects,
  .collect_cell                 = __f_process_collect_cell,
  .collect_cell_filter          = __f_process_collect_cell_filter,
  .collect_cell_into_list       = __f_process_collect_cell_into_list,
  .collect_cell_into_heap       = __f_process_collect_cell_into_heap,
  .collect_ref_into_list        = __f_process_collect_ref_into_list,
  .collect_ref_into_heap        = __f_process_collect_ref_into_heap,
  .collect_annotation_into_list = __f_process_collect_annotation_into_list,
  .collect_annotation_into_heap = __f_process_collect_annotation_into_heap
};



/*******************************************************************//**
 *
 * Subtree processor interface
 *
 ***********************************************************************
 */
DLL_HIDDEN IStandardSubtreeProcessors_t _framehash_processor__iStandardSubtreeProcessors = {
  .count_nactive                  = _framehash_processor__subtree_count_nactive,
  .count_nactive_limit            = _framehash_processor__subtree_count_nactive_limit,
  .destroy_objects                = _framehash_processor__subtree_destroy_objects,
  .collect_cells                  = _framehash_processor__subtree_collect_cells,
  .collect_cells_filter           = _framehash_processor__subtree_collect_cells_filter,
  .collect_cells_into_list        = _framehash_processor__subtree_collect_cells_into_list,
  .collect_cells_into_heap        = _framehash_processor__subtree_collect_cells_into_heap,
  .collect_refs_into_list         = _framehash_processor__subtree_collect_refs_into_list,
  .collect_refs_into_heap         = _framehash_processor__subtree_collect_refs_into_heap,
  .collect_annotations_into_list  = _framehash_processor__subtree_collect_annotations_into_list,
  .collect_annotations_into_heap  = _framehash_processor__subtree_collect_annotations_into_heap
};



/*******************************************************************//**
 * _framehash_processor__process_nolock_nocache
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_processor__process_nolock_nocache( framehash_processing_context_t * const processor ) {
  int64_t nproc = 0;
  const f_process_level process = PROCESS[ CELL_GET_FRAME_TYPE( processor->instance.frame ) ];
  process( processor, &nproc );
  return nproc;
}



/*******************************************************************//**
 * _framehash_processor__process_cache_partial_nolock_nocache
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_processor__process_cache_partial_nolock_nocache( framehash_processing_context_t * const processor, uint64_t selector ) {
  int64_t nproc = 0;
  if( CELL_GET_FRAME_TYPE( processor->instance.frame ) == FRAME_TYPE_CACHE ) {
    __cache_process_partial( processor, &nproc, selector );
  }
  else {
    nproc = -1;
  }
  return nproc;
}



/*******************************************************************//**
 * _framehash_processor__process
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_processor__process_nolock( framehash_processing_context_t * const processor ) {
  return _framehash_processor__process_nolock_nocache( processor );
}



/*******************************************************************//**
 * _framehash_processor__process
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_processor__process( framehash_processing_context_t * const processor ) {
  int64_t nproc = 0;
  CS_LOCK *lock = processor->instance.dynamic ? processor->instance.dynamic->pflock : NULL;
  SYNCHRONIZE_ON_PTR( lock ) {
    nproc = _framehash_processor__process_nolock( processor );
  } RELEASE;
  
  return nproc;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_processor.h"

DLL_HIDDEN test_descriptor_t _framehash_processor_tests[] = {
  { "processor",   __utest_framehash_processor },
  {NULL}
};
#endif

