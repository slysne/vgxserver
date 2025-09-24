/*######################################################################
 *#
 *# radix.c
 *#
 *#
 *######################################################################
 */



#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __ZONE_MAX_Q( Zone_k )              ( (1 << (Zone_k)) - 1 )
#define __CELL_INDEX( FrameIdx, J )         ( (FrameIdx) * FRAMEHASH_CELLS_PER_SLOT + (J) )
#define __CHAINZONE_FULL( ChainBits )       ( (ChainBits) == 0xFFFF )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_metas_t * __set_chain( framehash_metas_t *frame_metas, const _chainindex_t cidx );
static framehash_metas_t * __clear_chain( framehash_metas_t * const frame_metas, const _chainindex_t cidx );
static framehash_retcode_t __try_destroy_chain( framehash_context_t * const context, _framehash_celltype_t termtype );
static framehash_retcode_t __try_destroy_internal_chain( framehash_context_t * const context );



/*******************************************************************//**
 * __set_chain
 *
 ***********************************************************************
 */
__inline static framehash_metas_t * __set_chain( framehash_metas_t *frame_metas, const _chainindex_t cidx ) {
  frame_metas->chainbits |= (1 << _CHAININDEX_SLOT_Q(cidx));
  return frame_metas;
}


/*******************************************************************//**
 * __clear_chain
 *
 ***********************************************************************
 */
__inline static framehash_metas_t * __clear_chain( framehash_metas_t * const frame_metas, const _chainindex_t cidx ) {
  frame_metas->chainbits &= ~(1 << _CHAININDEX_SLOT_Q(cidx));
  return frame_metas;
}



/*******************************************************************//**
 * __try_destroy_chain
 *
 ***********************************************************************
 */
static framehash_retcode_t __try_destroy_chain( framehash_context_t * const context, _framehash_celltype_t termtype ) {
  framehash_metas_t *chain_metas = CELL_GET_FRAME_METAS( context->frame );
  framehash_retcode_t destruction = {0};
  framehash_retcode_t flushing;

  switch( chain_metas->ftype ) {
  case FRAME_TYPE_CACHE:
    flushing = _framehash_cache__flush( context, false ); // ensure clean cache so nchains check below can be trusted
    destruction.error = flushing.error;
    if( flushing.completed && _CACHE_HAS_NO_CHAINS(chain_metas) ) {
      // cache is clean and it has no chains => it's completely empty and can be destroyed
      _framehash_memory__discard_frame( context ); 
      destruction.completed = true;
    }
    break;
  case FRAME_TYPE_INTERNAL:
    if( chain_metas->nactive == 0 && chain_metas->chainbits == 0 ) {
      // internal frame is empty when it has not active items and no chains
      _framehash_memory__discard_frame( context );
      destruction.completed = true;
    }
    break;
  case FRAME_TYPE_LEAF:
    if( chain_metas->nactive == 0 ) {
      // leaf frame is empty when it has no active items
      _framehash_memory__discard_frame( context );
      destruction.completed = true;
    }
    break;
  case FRAME_TYPE_BASEMENT:
    if( chain_metas->nactive == 0 ) {
      // basement frame is empty when it has no active items
      _framehash_memory__discard_basement( context );
      destruction.completed = true;
    }
    break;
  }

  if( destruction.completed ) {
    if( termtype == CELL_TYPE_EMPTY ) {
      _INITIALIZE_DATA_CELL( context->frame, termtype );
    }
    else if( termtype == CELL_TYPE_END ) {
      _INITIALIZE_REFERENCE_CELL( context->frame, 0xF, 0xF, FRAME_TYPE_NONE );
    }
    else {
      FATAL( 0xFFF, "Illegal terminator type for chain destruction" );
    }
  }

  return destruction;
}



/*******************************************************************//**
 * __try_destroy_internal_chain
 *
 ***********************************************************************
 */
static framehash_retcode_t __try_destroy_internal_chain( framehash_context_t * const context ) {
  framehash_retcode_t destruction = {0};

  framehash_cell_t *frame = context->frame;
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  _chainindex_t chainindex = _framehash_radix__get_chainindex( _framehash_hashing__get_hashbits( frame_metas->domain, context->key.shortid ) );

  // Make sure the context is referencing a chain
  if( _framehash_radix__is_chain( frame_metas, chainindex ) ) {
    framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( frame );
    int p = frame_metas->order;
    int fx = _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), _CHAININDEX_SLOT_Q( chainindex ) );
    framehash_slot_t *chainslot = frame_slots + fx;
    bool all_chains_empty = true; // prove it false below

    // Make sure all chains are empty - otherwise we can't destroy
    for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
      framehash_cell_t *chain = chainslot->cells + j;
      framehash_metas_t *chain_metas = CELL_GET_FRAME_METAS( chain );
      if( chain_metas->nactive != 0 || chain_metas->ftype != FRAME_TYPE_LEAF ) {
        // can't remove chainslot, the structures below are not empty
        all_chains_empty = false;
        destruction.completed = false;
        break;
      }
    }

    if( all_chains_empty ) {
      // All chains are empty, proceed
      bool incomplete = false; // in case conversion fails, flag it
      framehash_cell_t rollback_pool[FRAMEHASH_CELLS_PER_SLOT];
      
      // Initialize rollback_pool chains to null pointers
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
        DELETE_CELL_REFERENCE( &rollback_pool[j] ); // make sure it starts out empty
      }

      // Perform destruction
      XTRY {
        framehash_context_t rollback_context = _CONTEXT_INIT_ROLLBACK( context );

        // Create rollback_pool slot with minimal, empty leaves
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          rollback_context.frame = &rollback_pool[j];
          if( _framehash_memory__new_frame( &rollback_context, _FRAMEHASH_P_MIN, frame_metas->domain+1, FRAME_TYPE_LEAF ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x301 );
          }
        }

        // Run the destruction
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          _PUSH_CONTEXT_FRAME( context, &chainslot->cells[j] ) {
            destruction = __try_destroy_chain( context, CELL_TYPE_EMPTY ); // EMPTY, since we're converting back to leaf slot
            if( !destruction.completed ) {
              incomplete = true; // need rollback later down
            }
          } _POP_CONTEXT_FRAME;
        }

        // Did we succeed?
        if( incomplete ) {
          // No: Conversion was partial - use small rollback frames for the chains we actually destroyed above
          framehash_cell_t *next = chainslot->cells;
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, next++ ) {
            if( _CELL_IS_EMPTY( next ) ) {
              _CELL_STEAL( next, &rollback_pool[j] ); // the deleted chain gets a small leaf from the rollback pool
            }
          }
          destruction.completed = false;
        }
        else {
          // Yes: All chains destroyed, update frame header to clear the chain bit for this frame slot
          destruction.modified = true;
          __clear_chain( frame_metas, chainindex );
          _framehash_frameallocator__set_chainbits( frame, frame_metas->chainbits );
          // After last chain is removed, we become a LEAF again
          if( frame_metas->chainbits == 0 ) {
            frame_metas->ftype = FRAME_TYPE_LEAF;
            _framehash_frameallocator__set_ftype( frame, frame_metas->ftype );
          }
          // Heuristic: if number of chainslots is small, try to recreate the structure
          else if( POPCNT16( frame_metas->chainbits ) < _FRAME_NCHAINSLOTS(p)/2 ) {
            // less than 1/2 slots in chainzone are chainslots, let's try to compact ourselves and all substructures into a new leaf
            framehash_retcode_t compaction = _framehash_radix__try_compaction( context );
            if( compaction.completed ) { }
            // TODO: deal with the result

          }
          destruction.completed = true;
        }

      }
      XCATCH( errcode ) {
        destruction.error = true;
      }
      XFINALLY {
        // Destroy any unused rollback chains - normally all of them are unused and need
        // to be discarded when the chain slot was successfully destroyed
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          if( _CELL_REFERENCE_EXISTS( &rollback_pool[j] ) ) {
            _PUSH_CONTEXT_FRAME( context, &rollback_pool[j] ) {
              _framehash_memory__discard_frame( context );
            } _POP_CONTEXT_FRAME;
          }
        }
      }
    }
  }

  return destruction;
}



#ifdef DEBUG_COMPACTION
/*******************************************************************//**
 * __debug_compaction
 *
 ***********************************************************************
 */
static void __debug_compaction( const char *msg, const framehash_cell_t *frameref, int64_t deep_count ) {
  char binbuf[17];
  const framehash_metas_t *metas = CELL_GET_FRAME_METAS( frameref );
  const framehash_slot_t *slots = CELL_GET_FRAME_SLOTS( frameref );
  const framehash_cell_t *frame = iFramehash.TopCell( slots );
  
  switch( metas->ftype ) {
  case FRAME_TYPE_CACHE:
    pmesg( 4, "((%s:%lld)) CACHE @ %llp: domain=%d, nchains=%d\n", msg, deep_count, frame, metas->domain, metas->nchains );
    break;
  case FRAME_TYPE_LEAF:
    pmesg( 4, "((%s:%lld)) LEAF @ %llp: domain=%d, order=%d, nactive=%d\n", msg, deep_count, frame, metas->domain, metas->order, metas->nactive );
    break;
  case FRAME_TYPE_INTERNAL:
    pmesg( 4, "((%s:%lld)) INTERNAL @ %llp: domain=%d, order=%d, nactive=%d, chainbits=[%s]\n", msg, deep_count, frame, metas->domain, metas->order, metas->nactive, uint16_to_bin( binbuf, metas->chainbits ) );
    break;
  case FRAME_TYPE_BASEMENT:
    pmesg( 4, "((%s:%lld)) BASEMENT @ %llp: domain=%d, order=%d, nactive=%d, hasnext=%d\n", msg, deep_count, frame, metas->domain, metas->order, metas->nactive, metas->hasnext );
    break;
  default:
    pmesg( 4, "((%s:%lld)) UNSUPPORTED @ %llp: unsupported ftype=%d!\n", msg, deep_count, frame, metas->ftype );
    break;
  }
}
#endif



/*******************************************************************//**
 * __f_transfer_cell
 *
 ***********************************************************************
 */
static int64_t __f_transfer_cell( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  // Update context with this cell's data to be transferred via 
  framehash_context_t *context = (framehash_context_t*)processor->processor.input;
  _LOAD_CONTEXT_FROM_CELL( context, cell );

  // Insert data into other structure
  if( _framehash_radix__set( context ).completed ) {
    return 1;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 * _framehash_radix__try_compaction
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__try_compaction( framehash_context_t * const context ) {
  framehash_retcode_t compaction = {0};
  framehash_cell_t *sparse_frame = context->frame;
  framehash_metas_t *sparse_metas = CELL_GET_FRAME_METAS( sparse_frame );
  framehash_cell_t compact_frame;
  framehash_context_t compaction_context = _CONTEXT_INIT_REHASH( context, &compact_frame );

  // Something reasonable as a default
  int p_compact = _FRAMEHASH_P_MAX / 2;

#ifdef DEBUG_COMPACTION
  int64_t pre_count = _framehash_processor__subtree_count_nactive( sparse_frame, context->dynamic );
  __debug_compaction( "Will compact", sparse_frame, pre_count );
#endif
  
  // If our starting point is a cache frame, make sure everything is clean before we collect and compactify cells
  if( sparse_metas->ftype == FRAME_TYPE_CACHE ) {
    if( sparse_metas->domain == 0 ) {
      FATAL( 0x311, "NEVER compactify the top-level cache!" );
    }
    if( _framehash_cache__flush( context, false ).error ) {
      compaction.error = true;
      return compaction;
    }
  }
  // Find a more accurate estimate for the new leaf order
  else if( sparse_metas->ftype == FRAME_TYPE_LEAF ) {
    if( sparse_metas->nactive > FRAMEHASH_CELLS_PER_HALFSLOT ) {
      // Pretend the source has 10% (1024*1.1=1127) more data than it has to end up with a reasonable guess for new frame's order (i.e. not 100% fill)
      int n_slots = 1 + ( ((1127 * (int)(sparse_metas->nactive)) >> 10) - 1) / FRAMEHASH_CELLS_PER_SLOT;
      p_compact = imag2( n_slots + 1 );
      // Cancel compaction - estimated order is not lower than current order
      if( p_compact >= sparse_metas->order ) {
        return compaction;
      }
    }
    else {
      p_compact = 0;
    }
  }
  // Can't apply this function beyond the last frame domain
  else if( sparse_metas->domain > FRAME_DOMAIN_LAST_FRAME ) {
    return compaction;
  }

  // Compacted structure starts with this new frame
  if( _framehash_memory__new_frame( &compaction_context, p_compact, sparse_metas->domain, FRAME_TYPE_LEAF ) != NULL ) {
    framehash_processing_context_t processor = FRAMEHASH_PROCESSOR_NEW_CONTEXT( sparse_frame, context->dynamic, __f_transfer_cell );
    FRAMEHASH_PROCESSOR_SET_IO( &processor, &compaction_context, NULL );
    // Run compaction process
    int64_t n_items = _framehash_processor__process( &processor );

    if( FRAMEHASH_PROCESSOR_IS_FAILED( &processor ) ) {
      compaction.error = true;
    }
    else {
      // Swap in new compacted structure and discard old structure
      _framehash_memory__discard_frame( context );                                // Discard old structure
      _CELL_STEAL( context->frame, compaction_context.frame );  // Patch the new structure into our context
      // Flag the modification
      compaction.modified = true;
      compaction.completed = true;
      if( n_items == 0 ) {
        compaction.empty = true;
      }
    }
#ifdef DEBUG_COMPACTION
    int64_t post_count = _framehash_processor__subtree_count_nactive( context->frame, context->dynamic );
    __debug_compaction( "Compacted into new", context->frame, post_count );
    if( pre_count != post_count ) {
      pmesg( 4, "COMPACTION ERROR! pre_count(%lld) != post_count(%lld)\n", pre_count, post_count );
      FATAL( 0x312, "Framehash corrupted after compaction!" );
    }
#endif
  }
  else {
    compaction.error = true;
  }

#ifdef FRAMEHASH_CONSISTENCY_CHECK
  if( !iFramehash.memory.IsFrameValid( context->frame ) ) {
    FATAL( 0xFC1, "Invalid frame after compaction" );
  }
#endif

  return compaction;
}



/*******************************************************************//**
 * _framehash_radix__try_destroy_cache_chain
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__try_destroy_cache_chain( framehash_context_t * const context ) {
  framehash_retcode_t destruction = {0};
  framehash_slot_t *cacheslot = _framehash_radix__get_cacheslot( context );
  framehash_cell_t *next = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];

  // Make sure we have a chain and that the chain isn't pointing to another cache
  if( _CELL_REFERENCE_EXISTS( next ) && CELL_GET_FRAME_METAS(next)->ftype != FRAME_TYPE_CACHE ) {
    // The cache is pointing to a non-cache sub structure.
    
    // If we have dirty items in this cacheslot we can't destroy the chain
    bool cancel = false;
    for( int j=0; j<_FRAMEHASH_CACHE_CHAIN_CELL_J; j++ ) {
      if( _ITEM_IS_DIRTY( &cacheslot->cells[j] ) ) {
        cancel = true;
        break;
      }
    }

    // Proceed?
    if( !cancel ) {
      _PUSH_CONTEXT_FRAME( context, next ) {
        destruction = __try_destroy_chain( context, CELL_TYPE_END ); // END because there's nothing below us now
      } _POP_CONTEXT_FRAME;
      if( destruction.completed ) {
        _CACHE_DECREMENT_NCHAINS( CELL_GET_FRAME_METAS( context->frame ) );
      }
    }
  }

  return destruction;
}



/*******************************************************************//**
 * _framehash_radix__get_surrogate_id
 * Return an objectID based on 64 bits, where the high part's LSBs are set to
 * a suitable range of the 64-bit ID's MSBs, and the low part is set to the 64-bit ID.
 *
 ***********************************************************************
 */
DLL_HIDDEN objectid_t _framehash_radix__get_surrogate_id( const shortid_t shortid ) {
  objectid_t surrogate;
  uint64_t h = shortid;
  // parts of murmur:
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  surrogate.H = h;
  surrogate.L = shortid;
  return surrogate;
}



/*******************************************************************//**
 * _framehash_radix__get_cacheslot
 * Return a pointer to the cache slot selected by the shortid or the obid
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_slot_t * _framehash_radix__get_cacheslot( framehash_context_t * const context ) {
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( context->frame );
  int p = self_metas->order;
  framehash_slot_t *slot = CELL_GET_FRAME_SLOTS( context->frame ); // we'll add offset below

  if( self_metas->domain == FRAME_DOMAIN_TOP ) {
    if( context->obid == NULL ) {
      objectid_t surrogate = _framehash_radix__get_surrogate_id( context->key.shortid );
      slot += _FRAMEHASH_TOPINDEX( p, surrogate.H );
    }
    else {
      slot += _FRAMEHASH_TOPINDEX( p, context->obid->H );
    }
  }
  else {
    _hashbits_t h16 = _framehash_hashing__get_hashbits( self_metas->domain, context->key.shortid );
    _chainindex_t cache_slot_q = _framehash_radix__get_chainindex( h16 );  // <= note how all bits in chainindex is interpreted as a slot index here!
    slot += cache_slot_q;
  }

  return slot;
}



/*******************************************************************//**
 * _framehash_radix__internal_set
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__internal_set( framehash_context_t * const context ) {
  framehash_retcode_t insertion = {0};
  framehash_retcode_t chaining = {0};
  framehash_cell_t *chaincell = NULL;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( context->frame );

  // Determine if we need to follow a chain or create a chain (due to a balancing request) then follow it
  if( (chaincell = __get_internal_chaincell( context )) != NULL || self_metas->flags.dobalance ) {
    // Mo chain but create it due to an earlier balancing request
    if( chaincell == NULL ) {
      // Create chain
      chaining = _framehash_radix__create_internal_chain( context );
      // Chain creation completed and we are still an internal frame
      if( chaining.modified && self_metas->ftype == FRAME_TYPE_INTERNAL ) {
        // This is the chaincell to use for the internal frame insertion below
        chaincell = __get_internal_chaincell( context );
      }
    }
    // We have a chaincell to use for internal insertion
    if( chaincell ) {
      // forward the request to the next domain
      _PUSH_CONTEXT_FRAME( context, chaincell ) {
        insertion = _framehash_radix__set( context );
      } _POP_CONTEXT_FRAME;
      // insertion occurred at greater depth than next domain, we should convert
      // entire chainzone to chains so that we can become a cache
      if( insertion.depth > self_metas->domain + 1 ) {
        self_metas->flags.dobalance = 1;
      }
      // Carry the modification flag in case chain was created here due to a balancing request
      insertion.modified = chaining.modified;
    }
    // We do not have a chaincell for internal insertion
    else {
      // Chaining resulted in completion of internal chainzone followed by conversion to cache
      if( chaining.completed && chaining.modified && self_metas->ftype != FRAME_TYPE_INTERNAL ) {
        insertion = _framehash_radix__set( context );
        insertion.modified = 1;
      }
      // Error
      else {
        insertion.code = chaining.code;
      }
    }
  }
  else {
    // forward the request to the leaf handler
    insertion = _framehash_leaf__set( context );
  }
  return insertion;
}



/*******************************************************************//**
 * _framehash_radix__internal_get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__internal_get( framehash_context_t * const context ) {
  framehash_retcode_t retrieval = {0};
  framehash_cell_t *chaincell = NULL;
  // Determine if we need to follow a chain
  if( (chaincell = __get_internal_chaincell( context )) != NULL ) {
    // forward the request to the next domain
    _PUSH_CONTEXT_FRAME( context, chaincell ) {
      retrieval = _framehash_radix__get( context );
    } _POP_CONTEXT_FRAME;
  }
  else { 
    // forward the request to the leaf handler
    retrieval = _framehash_leaf__get( context );
  }
  return retrieval;
}



/*******************************************************************//**
 * _framehash_radix__internal_del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__internal_del( framehash_context_t * const context ) {
  framehash_retcode_t deletion = {0};
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( context->frame );
  framehash_cell_t *chaincell = NULL;
  // Determine if we need to follow a chain
  if( (chaincell = __get_internal_chaincell( context )) != NULL ) {
    // forward the request to the next domain
    _PUSH_CONTEXT_FRAME( context, chaincell ) {
      deletion = _framehash_radix__del( context );
    } _POP_CONTEXT_FRAME;
    if( deletion.completed ) {
      if( deletion.unresizable || deletion.empty ) {
        // subframe has reached a minimum size, let's see if we can de-construct the chains
        framehash_retcode_t destruction = __try_destroy_internal_chain( context );
        if( destruction.completed && self_metas->chainbits == 0 && self_metas->nactive == 0 ) {
          deletion.empty = true; // we're empty because we succeeded destroying the chain, and it was the last chain, and we have no other leaf items either
        }
        else {
          deletion.empty = false; // not empty
        }
        // Signal upward that our structure was modified
        deletion.modified = destruction.modified;
      }
      // Another heuristic to compactify when things get really sparse
      else if( POPCNT16( self_metas->chainbits ) <= 2 ) {
        // Only 1 internal frame slot has chains to sub domains, let's see if the current
        // chaincell points to a subframe that is also very sparse and if so try to compactify.
        int64_t n_active_here = 0;
        framehash_metas_t *sub_metas = CELL_GET_FRAME_METAS( chaincell );
        bool proceed_compaction = true;
        switch( sub_metas->ftype ) {
        // Stop if leaf isn't very small
        case FRAME_TYPE_LEAF:
          if( sub_metas->nactive >= 16 ) {
            proceed_compaction = false;
          }
          break;
        // Stop if basement is full
        case FRAME_TYPE_BASEMENT:
          if( sub_metas->nactive >= 6 ) {
            proceed_compaction = false;
          }
          break;
        // Stop if deeper domains
        default:
          proceed_compaction = false;
        }
        // Continue checking if we should try compaction
        if( proceed_compaction ) {
          // Go ahead with the compaction attempt if few items in this entire subtree
          if( (n_active_here = _framehash_processor__subtree_count_nactive_limit( context->frame, context->dynamic, 64 )) < 64 ) {
#ifndef NDEBUG
            DEBUG( 0, "PRE COMPACTION structure: domain=%u ftype=%u nactive=%u, c=0x%04X (total active=%lld)", self_metas->domain, self_metas->ftype, self_metas->nactive, self_metas->chainbits, n_active_here );
#endif
            framehash_retcode_t compaction = _framehash_radix__try_compaction( context );
            if( compaction.modified ) {
              deletion.modified = true;
#ifndef NDEBUG
              n_active_here = _framehash_processor__subtree_count_nactive( context->frame, context->dynamic );
              framehash_metas_t *new_metas = CELL_GET_FRAME_METAS( context->frame );
              DEBUG( 0, "POST COMPACTION new structure: domain=%u ftype=%u nactive=%u, c=0x%04X (total active=%lld)", new_metas->domain, new_metas->ftype, new_metas->nactive, new_metas->chainbits, n_active_here );
            }
            else {
              DEBUG( 0, "COMPACTION DID NOT OCCUR" );
#endif
            }
          }
        }
      }
    }
    else {
      deletion.empty = false; // let's be explicit, we're not empty
    }
  }
  else {
    // Forward the request to the leaf handler
    deletion = _framehash_leaf__del( context );

    // Internal frame has no leaf items, but chains exist. Compactify into new structure.
    if( deletion.unresizable && self_metas->nactive == 0 && self_metas->chainbits != 0 ) {
      framehash_retcode_t compaction = _framehash_radix__try_compaction( context );
      if( compaction.error ) {
        deletion.error = true;
      }
      else if( (deletion.modified = compaction.modified) == true ) {
        deletion.unresizable = false;
      }
    }
  }
  return deletion;
}



/*******************************************************************//**
 * _framehash_radix__create_internal_chain
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_radix__create_internal_chain( framehash_context_t *context ) {
  framehash_retcode_t creation = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS(self);
  framehash_slot_t *self_slots = CELL_GET_FRAME_SLOTS(self);
  int p = self_metas->order;
  int chainzone_k = _FRAME_CHAINZONE( p ); // our chain zone is always the second largest zone
  _chainindex_t chainindex = _framehash_radix__get_chainindex( _framehash_hashing__get_hashbits( self_metas->domain, context->key.shortid ) );
  uint8_t migrant_tags[ _FRAMEHASH_OFFSET_MASK * FRAMEHASH_CELLS_PER_SLOT ] = {0xFF}; // use this for rollback
  framehash_slot_t *slot, *chainslot;
  framehash_cell_t *cell, *chaincell;
  framehash_cell_t new_chaincells[ FRAMEHASH_CELLS_PER_SLOT ] = {0};
  int q_first, q_last;
  uint16_t nslots;
#if _FRAMEHASH_USE_MEMORY_STREAMING
  framehash_cell_t mcell[FRAMEHASH_CELLS_PER_SLOT];
#endif

  framehash_context_t migrant_context = _CONTEXT_INIT_MIGRANT( context );
  
  XTRY {
    if( _framehash_radix__is_chain( self_metas, chainindex ) ) {
      /* Don't create a chain if we already have a chain */
    }
    else {
      _chainindex_t migrant_chainindex;

      /* ______ */
      /* Step 1: Determine if subframes or basements are needed, then create them */
      if( self_metas->domain < FRAME_DOMAIN_LAST_FRAME ) {
        // Subframes:
        int order = _FRAMEHASH_P_MIN;
        int domain = self_metas->domain+1;
        framehash_ftype_t ftype = FRAME_TYPE_LEAF;
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          _INITIALIZE_REFERENCE_CELL( &new_chaincells[j], order, domain, ftype );
        }
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          migrant_context.frame = &new_chaincells[j];
          if( _framehash_memory__new_frame( &migrant_context, order, domain, ftype ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x411 );
          }
        }
      }
      else {
        // Basements:
        int order_na = 0xF; // N/A
        int domain = self_metas->domain+1;
        framehash_ftype_t ftype = FRAME_TYPE_BASEMENT;
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          _INITIALIZE_REFERENCE_CELL( &new_chaincells[j], order_na, domain, ftype );
        }
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
          migrant_context.frame = &new_chaincells[j];
          if( _framehash_memory__new_basement( &migrant_context, domain ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x412 );
          }
        }
      }

      /* ______ */
      /* Step2: Get the chainslot in our current internal frame */
      chainslot = self_slots + _framehash_radix__get_frameindex( p, chainzone_k, _CHAININDEX_SLOT_Q(chainindex) );

      /* ______ */
      /* Step 3: Scan internal frame and move cells to the new subframes if the chain region of their hash bits match the new chain slot */
      for( int k=p-1; k>=0; k-- ) {
        // scan zone k
        int fx = _framehash_radix__get_frameindex( p, k, 0 );
        if( k == chainzone_k ) {
          // In the chainzone, only scan the chainslot! This is why we make sure to overlap the hashbits' chain slot with 2 nd largest zone (k=p-1)
          slot = chainslot;
          q_first = q_last = _CHAININDEX_SLOT_Q(chainindex);
        }
        else {
          slot = self_slots + fx;
          q_first = 0;
          q_last = __ZONE_MAX_Q(k);
        }
        for( int q=q_first; q<=q_last; q++, slot++ ) {
          // scan slot q in zone k
#if _FRAMEHASH_USE_MEMORY_STREAMING
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
            mcell[j].m128i = _mm_stream_load_si128( ((__m128i*)slot) + j );
          }
          // TODO: explain why lfence is needed here?
          _mm_lfence();
          cell = mcell;
#else
          cell = slot->cells;
#endif
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
            // scan cell j in slot q in zone k
            if( _ITEM_IS_VALID( cell ) ) {
              _LOAD_CONTEXT_KEY( &migrant_context, cell );
              migrant_chainindex = _framehash_radix__get_chainindex( _framehash_hashing__get_hashbits( self_metas->domain, migrant_context.key.shortid ) );
              if( _CHAININDEX_SLOT_Q(migrant_chainindex) == _CHAININDEX_SLOT_Q(chainindex) ) {
                // This cell hashes to the new chain, we must move it to the subframe
                migrant_context.frame = &new_chaincells[ _CHAININDEX_CELL_J(migrant_chainindex) ];
                _LOAD_CONTEXT_VALUE( &migrant_context, cell );
                framehash_retcode_t migration = _framehash_radix__set( &migrant_context );
                // Insertion error ?
                if( migration.error ) {
                  // TODO!: handle various error types and se if we can do rollback! difficult. For now, let's undefine it.
                  THROW_ERROR( CXLIB_ERR_UNDEFINED, 0x413 ); // FATAL.  But it's an extreme edge case and will probably never happen. Famous last words.
                }
                // Keep a map of the moved cell tags so we can roll back if needed
                migrant_tags[ __CELL_INDEX(fx,j) ] = (uint8_t)_CELL_TYPE( cell );
                // Delete from this frame, completing the move
                _DELETE_ITEM( cell );
#if _FRAMEHASH_USE_MEMORY_STREAMING
                _mm_stream_si128( ((__m128i*)slot) + j, cell->m128i );
                _mm_sfence();
#endif
                self_metas->nactive--; // <- only updated in the parent cell, not this frame's header!
              }
            }
            else if( _CELL_IS_END( cell ) ) {   //  END: Skip rest of slot
              break;
            }
            else if( _CELL_IS_EMPTY( cell ) ) {  //  EMPTY: Skip this cell
              continue;
            }
          }
        } 
      }

      /* ______ */
      /* Step 4: Hook the subframes into the chainslot */
      cell = chainslot->cells;
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
        _CELL_COPY( cell, &new_chaincells[j] );
      }

      /* ______ */
      /* Step 5: Mark as chain */
      __set_chain( self_metas, chainindex );
      _framehash_frameallocator__set_chainbits( self, self_metas->chainbits );

      /* ______ */
      /* Step 6: Ensure frame type INTERNAL and indicate modification of frame */
      self_metas->ftype = FRAME_TYPE_INTERNAL;
      _framehash_frameallocator__set_ftype( self, self_metas->ftype ); // (this had been forgotten until 20151202!)
      creation.modified = true;
      creation.completed = true;
      creation.depth = self_metas->domain;

#ifdef FRAMEHASH_CONSISTENCY_CHECK
      if( !iFramehash.memory.IsFrameValid( context->frame ) ) {
        FATAL( 0xFC1, "Bad frame after resize" );
      }
#endif
    }

    /* ______ */
    /* Step 7: Convert to CACHE frame when all slots in chainzone are chainslots */
#if _FRAMEHASH_ENABLE_CACHE_FRAMES
    if( __CHAINZONE_FULL( self_metas->chainbits ) && self_metas->flags.cancache ) {
      if( self_metas->flags.cachetyp == FCACHE_TYPE_CMORPH ) {
        framehash_cell_t cache;
        framehash_metas_t *cache_metas;
        framehash_slot_t *cache_slots;

        framehash_context_t cache_context = _CONTEXT_INIT_CACHE_CONVERT( context, &cache );

        if( _framehash_memory__new_frame( &cache_context, p, self_metas->domain, FRAME_TYPE_CACHE ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x414 );
        }
        // source cell starts at beginning of chainzone in our existing internal frame
        chaincell = (self_slots + _framehash_radix__get_frameindex( p, chainzone_k, 0 ))->cells;
        // destination slot starts at beginning of our new cache frame 
        cache_metas = CELL_GET_FRAME_METAS( cache_context.frame );
        cache_slots = CELL_GET_FRAME_SLOTS( cache_context.frame );
        slot = cache_slots;
        nslots = _CACHE_FRAME_NSLOTS( cache_metas->order );
        // copy all chaincells from the internal frame to the last cell in the new cache frame
        for( int fx=0; fx<nslots; fx++, slot++, chaincell++ ) {
          _CELL_STEAL( &slot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ], chaincell );
        }
        self_metas->chainbits = 0; // <- prevent disaster when we discard below!
        // delete the old internal frame - this will not propagate down subtrees since we reset the chainbits and 
        _framehash_memory__discard_frame( context );
        // now it's safe to set our cache's nchains, which is equal to nslots - i.e. all are chains by definition since we're converting from internal to cache frame
        _CACHE_SET_NCHAINS( cache_metas, nslots );
        // update the parent's cell to refer to our new cache frame instead
        _CELL_STEAL( self, cache_context.frame );
      }
      else if( self_metas->flags.cachetyp == FCACHE_TYPE_CZONEP ) {
        /* TODO: Convert to perform caching in the largest zone */
      }
    }
#endif
  }
  XCATCH( errcode ) {
    /* -- Roll back -- */
    // restore cell tags for already moved cells
    nslots = _FRAME_NSLOTS( p );
    uint8_t tag;
    for( int fx=0; fx<nslots; fx++ ) {
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
        if( (tag=migrant_tags[ __CELL_INDEX(fx,j) ]) != 0xFF ) {
          cell = self_slots->cells + j;
          _SET_ITEM_TAG( cell, tag );
          self_metas->nactive++; // <- only updated in the parent cell, not this frame's header!
        }
      }
    }
    // delete any allocated subframes
    for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
      migrant_context.frame = &new_chaincells[j];
      _framehash_memory__discard_frame( &migrant_context );
    }

    creation.error = true;
  }
  XFINALLY {}

  return creation;
}



/*******************************************************************//**
 * _framehash_radix__compactify_subtree
 *
 * Returns:   1: Subtree is different (compaction performed)
 *            0: Subtree is the same (no compaction)
 *           -1: Error
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_radix__compactify_subtree( framehash_context_t * const context ) {
  int compacted = 0;
  // Frameref is the cell containing metas and pointer to the subframe slots. When we compactify the subtree
  // we update this cell afterwards with the correct metas and pointer to a new subtree if different from the one we started with

  int perform_compaction = 1;
  
  // Simple check to see if we should proceed with compaction
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( context->frame );
  if( metas->ftype == FRAME_TYPE_LEAF && metas->order > 0 ) {
    if( context->control.loadfactor.low > 0 ) {
      perform_compaction = context->control.loadfactor.low > 1 && __get_loadfactor( context->frame ) < context->control.loadfactor.low;
    }
    else {
      perform_compaction = __get_loadfactor( context->frame ) < _FRAMEHASH_MIN_LOADFACTOR;
    }
  }

  if( perform_compaction ) {
    framehash_retcode_t compaction = _framehash_radix__try_compaction( context );
    // Compaction 
    if( compaction.completed && compaction.modified ) {
      compacted = 1;
    }
    // Error
    else if( compaction.error ) {
      compacted = -1;
    }
    // Nothing happened (should never be the case)
    else {
      compacted = 0;
    }
  }

  return compacted;
}



/*******************************************************************//**
 * _framehash_radix__subtree_length
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_radix__subtree_length( framehash_cell_t *self, framehash_dynamic_t *dynamic, int64_t limit ) {
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );
  if( self_metas->ftype == FRAME_TYPE_LEAF ) {
    return self_metas->nactive;
  }
  else {
    return _framehash_processor__subtree_count_nactive_limit( self, dynamic, limit );
  }
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_radix.h"

DLL_HIDDEN test_descriptor_t _framehash_radix_tests[] = {
  { "radix",   __utest_framehash_radix },
  {NULL}
};
#endif

