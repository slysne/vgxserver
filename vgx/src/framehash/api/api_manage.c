/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_manage.c
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



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 * _framehash_api_manage__flush
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__flush( framehash_t * const self, bool invalidate ) {
  framehash_retcode_t flushing = {0};
  framehash_context_t flush_context = {
    .frame   = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };
  BEGIN_FRAMEHASH_WRITE( self, &flush_context ) {
    __SYNCHRONIZE_ALL_SUBTREES( self ) {
      if( !self->_flag.clean ) {
        flushing = _framehash_cache__flush( &flush_context, invalidate );
        if( flushing.completed ) {
          self->_flag.clean = 1;
        }
      }
    } __RELEASE_ALL_SUBTREES;
  } END_FRAMEHASH_WRITE( &flushing );
  return __RETCODE_BY_OPERATION( flushing );
}



/*******************************************************************//**
 * _framehash_api_manage__discard
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__discard( framehash_t * const self ) {
  int64_t n_discarded = 0;
  framehash_retcode_t discard = {0};
  framehash_context_t discard_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };
  BEGIN_FRAMEHASH_WRITE( self, &discard_context ) {
    __SYNCHRONIZE_ALL_SUBTREES( self ) {
      framehash_retcode_t flushing = _framehash_cache__flush( &discard_context, false );
      if( flushing.error == false && flushing.completed ) {
        // First discard all OBJECT128 instances (run their destructors)
        _framehash_processor__destroy_objects( self );

        // Brute-force discard the entire structure
        if( _CELL_REFERENCE_EXISTS( discard_context.frame ) ) {
          n_discarded = _framehash_memory__discard_frame( &discard_context );
          self->_nobj = 0;
          self->_opcnt++;   // TOOD: Does this make sense?
        }

        // Re-create structure, starting with an empty new top frame
        if( _framehash_memory__new_frame( &discard_context, self->_order, 0, FRAME_TYPE_CACHE ) == NULL ) {
          discard.error = true;
        }
        else {
          discard.completed = true;
        }
      }
      else {
        discard.error = true;
      }
    } __RELEASE_ALL_SUBTREES;
  } END_FRAMEHASH_WRITE( &discard );

  return __RETCODE_BY_OPERATION( discard );
}



static framehash_retcode_t __compactify_subtree( framehash_context_t *context );



/*******************************************************************//**
 * __compactify_subtree
 *
 ***********************************************************************
 */
static framehash_retcode_t __compactify_cache( framehash_context_t *context ) {
  framehash_retcode_t compaction = {0};
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( context->frame );
  framehash_slot_t *slots = CELL_GET_FRAME_SLOTS( context->frame );

  int nslots = _CACHE_FRAME_NSLOTS( metas->order );
  // Compactify all subtrees
  framehash_slot_t *chainslot = slots;
  for( int fx=0; fx<nslots; fx++, chainslot++ ) {
    framehash_cell_t *chaincell = &chainslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
    if( _CELL_REFERENCE_EXISTS( chaincell ) ) {
      _PUSH_CONTEXT_FRAME( context, chaincell ) {
        framehash_retcode_t subtree_compaction = __compactify_subtree( context );
        if( subtree_compaction.completed && subtree_compaction.modified ) {
          compaction.completed = 1;
          compaction.modified = 1;
        }
        else if( subtree_compaction.error ) {
          compaction.error = 1;
        }
      } _POP_CONTEXT_FRAME;
    }
  }

  // TODO: We are currently not converting caches back to smaller frames.
  // So if a very large structure with deep caches is emptied and then
  // compactified we will retain all the caches in place. Implement
  // more logic to collapse the caches so we can eventually reduce the
  // structure to its minimal form.

  return compaction;
}



/*******************************************************************//**
 * __compactify_internal
 *
 ***********************************************************************
 */
static framehash_retcode_t __compactify_internal( framehash_context_t *context ) {
  static const int max_leaf_capacity = _FRAME_NSLOTS( _FRAMEHASH_P_MAX ) * FRAMEHASH_CELLS_PER_SLOT;

  framehash_retcode_t compaction = {0};
  framehash_cell_t *frame = context->frame;
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( frame );
  framehash_slot_t *slots = CELL_GET_FRAME_SLOTS( frame );

  int p = metas->order;
  int k = _FRAME_CHAINZONE( p ); 

  int mask = metas->chainbits;
  
  // This will sum up the number of items in this internal frame plus
  // all the items in subtrees. This will be used later to determine
  // whether the internal frame can be converted to leaf.
  int64_t n_active_estimate = metas->nactive;
  const int UNKNOWN = INT_MAX;

  // Process all active chainslots
  int q = 0;
  while( mask != 0 ) {
    // Active chain
    if( mask & 1 ) {
      int fx = _framehash_radix__get_frameindex( p, k, q );
      framehash_slot_t *chainslot = slots + fx;
      // Process chain cells in chainslot
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
        framehash_cell_t *chaincell = chainslot->cells + j;
        // Forward compaction to the next domain
        _PUSH_CONTEXT_FRAME( context, chaincell ) {
          // Any completion flag below propagates up. We don't propagate
          // the modified flag because it is local to this chaincell only.
          if( __compactify_subtree( context ).completed ) {
            compaction.completed = true;
          }
          framehash_metas_t *sub_metas = CELL_GET_FRAME_METAS( context->frame );
          if( sub_metas->ftype == FRAME_TYPE_LEAF || sub_metas->ftype == FRAME_TYPE_BASEMENT ) {
            n_active_estimate += sub_metas->nactive;
          }
          else {
            n_active_estimate += UNKNOWN;
          }
        } _POP_CONTEXT_FRAME;
      }
    }
    // Next
    ++q;
    mask >>= 1;
  }

  // Are we able to convert this internal frame to a leaf?
  if( n_active_estimate < max_leaf_capacity ) {
    // Now adjust for loadfactor and check again
    int leaf_capacity = max_leaf_capacity;
    int loadfactor;
    if( context->control.loadfactor.high > 0 ) {
      loadfactor = context->control.loadfactor.high;
    }
    else {
      loadfactor = _FRAMEHASH_MAX_LOADFACTOR;
    }

    // Divide by 128 (not 100), this gives us some safety margin so we can
    // be pretty sure we won't waste our time trying to convert internal into leaf
    leaf_capacity = (leaf_capacity * loadfactor) >> 7;

    // Proceed.
    if( n_active_estimate < leaf_capacity ) {
      compaction = _framehash_radix__try_compaction( context );
    }
  }

  return compaction;
}



/*******************************************************************//**
 * __compactify_subtree
 *
 ***********************************************************************
 */
static framehash_retcode_t __compactify_subtree( framehash_context_t *context ) {
  framehash_retcode_t no_action = {0};
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( context->frame );
  switch( frame_metas->ftype ) {
  case FRAME_TYPE_CACHE:
    return __compactify_cache( context );
  case FRAME_TYPE_LEAF:
    return _framehash_radix__try_compaction( context );
  case FRAME_TYPE_INTERNAL:
    return __compactify_internal( context );
  default:
    return no_action;
  }
}



/*******************************************************************//**
 * _framehash_api_manage__compactify
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__compactify( framehash_t * const self ) {
  framehash_retcode_t compaction = {0};
  framehash_context_t compaction_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };
  BEGIN_FRAMEHASH_WRITE( self, &compaction_context ) {
    __SYNCHRONIZE_ALL_SUBTREES( self ) {
      compaction = __compactify_subtree( &compaction_context );
    } __RELEASE_ALL_SUBTREES;
  } END_FRAMEHASH_WRITE( &compaction );
  return __RETCODE_BY_OPERATION( compaction );
}



/*******************************************************************//**
 * _framehash_api_manage__compactify_partial
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__compactify_partial( framehash_t * const self, uint64_t selector ) {
  framehash_retcode_t compaction = {0};
  framehash_context_t compaction_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( compaction_context.frame );
  int fx = _FRAMEHASH_TOPINDEX( metas->order, selector );
  framehash_slot_t *cacheslot = CELL_GET_FRAME_SLOTS( compaction_context.frame ) + fx;
  framehash_cell_t *chaincell = &cacheslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
  if( chaincell ) {
    BEGIN_FRAMEHASH_WRITE( self, &compaction_context ) {
      __SYNCHRONIZE_SUBTREE( self, fx ) {
        _PUSH_CONTEXT_FRAME( &compaction_context, chaincell ) {
          compaction = __compactify_subtree( &compaction_context );
        } _POP_CONTEXT_FRAME;
      } __RELEASE_SUBTREE;
    } END_FRAMEHASH_WRITE( &compaction );
  }
  return __RETCODE_BY_OPERATION( compaction );
}



/*******************************************************************//**
 * _framehash_api_manage__set_readonly
 *
 * Returns: >= 1 : readonly recursion count
 *            -1 : error
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__set_readonly( framehash_t *self ) {
  int do_flush = 0;
  int readonly = 0;

  // 1. Enter readonly mode to ensure no further writes will be allowed
  __SYNCHRONIZE_ALL_SUBTREES( self ) {
    readonly = ++(self->_flag.readonly);
    do_flush = !self->_flag.clean; // Perform cache flush if writes have been performed since last readonly mode
  } __RELEASE_ALL_SUBTREES;

  // 2. Flush caches if needed
  if( do_flush ) {
    framehash_retcode_t flushing = {0};
    framehash_context_t flush_context = {
      .frame = &self->_topframe,
      .dynamic = &self->_dynamic,
      .control = FRAMEHASH_CONTROL_FLAGS_INIT
    };
    __SYNCHRONIZE_ALL_SUBTREES( self ) {
      if( !self->_flag.clean ) {
        flushing = _framehash_cache__flush( &flush_context, false );
        if( flushing.completed ) {
          self->_flag.clean = 1;
        }
        else {
          readonly = -1;
        }
      }
    } __RELEASE_ALL_SUBTREES;
  }

  // 3. Make allocators readonly
  XTRY {
    // First check if we succeeded above
    if( readonly < 0 ) { // NOTE: will also be negative if we entered readonly recursively too many times!
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xA91 );
    }

    // Make allocators readonly
    if( CALLABLE( self->_dynamic.falloc )->SetReadonly( self->_dynamic.falloc ) < 0 ||
        CALLABLE( self->_dynamic.balloc )->SetReadonly( self->_dynamic.balloc ) < 0
    )
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xA92 );
    }
  }
  XCATCH( errcode ) {
    self->_flag.readonly--;
    readonly = -1;
    // Exit readonly mode if we failed
    _framehash_api_manage__clear_readonly( self );
  }
  XFINALLY {
  }

  return readonly;
}



/*******************************************************************//**
 * _framehash_api_manage__is_readonly
 *
 * Returns:   0 : writable
 *            1 : readonly
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__is_readonly( framehash_t *self ) {
  int ret = 0;
  __SYNCHRONIZE_ALL_SUBTREES( self ) {
    ret = self->_flag.readonly > 0;
  } __RELEASE_ALL_SUBTREES;
  return ret;
}



/*******************************************************************//**
 * _framehash_api_manage__clear_readonly
 *
 * Returns:    0 : Writable after clear
 *          >= 1 : Still readonly with this recursion count
 *            -1 : Error
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__clear_readonly( framehash_t *self ) {
  int readonly = 0;

  // 1. Make allocators writable
  CALLABLE( self->_dynamic.falloc )->ClearReadonly( self->_dynamic.falloc );
  CALLABLE( self->_dynamic.balloc )->ClearReadonly( self->_dynamic.balloc );

  // 2. Exit readonly mode
  __SYNCHRONIZE_ALL_SUBTREES( self ) {
    if( self->_flag.readonly > 0 ) {
      readonly = --(self->_flag.readonly);
    }
    else {
      readonly = -1;
    }
  } __RELEASE_ALL_SUBTREES;

  return readonly;
}



/*******************************************************************//**
 * _framehash_api_manage__enable_read_caches
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__enable_read_caches( framehash_t *self ) {
  int ret = 0;
  __SYNCHRONIZE_ALL_SUBTREES( self ) {
    self->_flag.cache.r_ena = 1;
  } __RELEASE_ALL_SUBTREES;
  return ret;
}



/*******************************************************************//**
 * _framehash_api_manage__disable_read_caches
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_manage__disable_read_caches( framehash_t *self ) {
  int ret = 0;
  __SYNCHRONIZE_ALL_SUBTREES( self ) {
    framehash_retcode_t flushing = {0};
    framehash_context_t flush_context = {
      .frame = &self->_topframe,
      .dynamic = &self->_dynamic,
      .control = FRAMEHASH_CONTROL_FLAGS_INIT
    };
    // Flush caches if dirty
    flushing = _framehash_cache__flush( &flush_context, true );
    if( flushing.completed ) {
      self->_flag.clean = 1;
    }
    else {
      ret = -1;
    }
    // If caches now clean, disable them
    if( self->_flag.clean ) {
      self->_flag.cache.r_ena = 0;
    }
  } __RELEASE_ALL_SUBTREES;
  return ret;
}



/*******************************************************************//**
 * _framehash_api_manage__enable_write_caches
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _framehash_api_manage__enable_write_caches( framehash_t *self ) {
  return 0; // TODO
}



/*******************************************************************//**
 * _framehash_api_manage__disable_write_caches
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _framehash_api_manage__disable_write_caches( framehash_t *self ) {
  return 0; // TODO
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_manage.h"

DLL_HIDDEN test_descriptor_t _framehash_api_manage_tests[] = {
  { "api_manage",   __utest_framehash_api_manage },
  {NULL}
};
#endif
