/*######################################################################
 *#
 *# cache.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


static framehash_retcode_t __cache_evict_slot( framehash_context_t * const context, framehash_slot_t * const cacheslot, framehash_metas_t * const cache_metas, bool invalidate );
static framehash_retcode_t __cache_evict_cell( framehash_context_t * const context, framehash_cell_t *victim, framehash_cell_t *drain );
static framehash_cell_t * __cache_shift_cells( framehash_cell_t *top );
static framehash_cell_t * __cache_fill_positive( const framehash_context_t * const context, framehash_slot_t * const cacheslot );
static framehash_cell_t * __cache_fill_negative( const framehash_context_t * const context, framehash_slot_t * const cacheslot );
static framehash_cell_t * __cache_get_chain( framehash_context_t * const context, framehash_slot_t * const cacheslot, framehash_metas_t * const cache_metas );


/*******************************************************************//**
 * __cache_evict_slot
 *
 ***********************************************************************
 */
static framehash_retcode_t __cache_evict_slot( framehash_context_t * const context, framehash_slot_t * const cacheslot, framehash_metas_t * const cache_metas, bool invalidate ) {
  framehash_retcode_t eviction = {0};
  bool incomplete = false;
  bool error = false;

  framehash_cell_t *cursor = cacheslot->cells;
  framehash_cell_t *chaincell = cursor + _FRAMEHASH_CACHE_CHAIN_CELL_J;
  framehash_cell_t *drain = NULL;

  // perform eviction and/or invalidation in all cache cells
  do {
    // only evict cell if it's dirty
    if( _ITEM_IS_DIRTY( cursor ) ) {
      // get the chain only once, and make sure we have a chain
      if( drain == NULL && (drain = __cache_get_chain( context, cacheslot, cache_metas )) == NULL ) {
        incomplete = error = true;
      }
      // evict victim down the drain
      eviction = __cache_evict_cell( context, cursor, drain );
      if( eviction.completed == false ) {
        incomplete = true;
      }
      if( eviction.error ) {
        error = true;
      }
    }
    // Erase cell if requested
    if( invalidate ) {
      _DELETE_ITEM( cursor );
    }
  } while( ++cursor < chaincell );

  // eviction is complete if none of the individual evictions were incomplete
  eviction.completed = incomplete ? false : true;
  eviction.error = error ? true : false;

  return eviction;  // will be unresizable if the last eviction above was unresizable!
}



/*******************************************************************//**
 * __cache_evict_cell
 *
 ***********************************************************************
 */
static framehash_retcode_t __cache_evict_cell( framehash_context_t * const context, framehash_cell_t *victim, framehash_cell_t *drain ) {
  framehash_retcode_t eviction = {0};
  objectid_t obid;

  if( victim && drain ) {

    // previously cached item to be evicted has been modified and must be written back
    framehash_context_t writeback_context = _CONTEXT_INIT_WRITEBACK( context, drain );
    _LOAD_CONTEXT_FROM_CELL( &writeback_context, victim );

    if( writeback_context.vtype == CELL_VALUE_TYPE_ERROR ) {
      eviction.error = true;
      return eviction;
    }

    // populate obid of writeback context for an object that requires 128-bit id
    if( writeback_context.ktype == CELL_KEY_TYPE_HASH128 ) {
      if( writeback_context.vtype == CELL_VALUE_TYPE_OBJECT128 ) {
        comlib_object_t *evict_obj = writeback_context.value.pobject;
        // We are evicting the same object that triggered eviction in the first place - i.e. delete
        if( evict_obj == context->value.pobject ) {
          writeback_context.obid = context->obid;
        }
        // Fall back to more expensive extraction of obid from the evicted cell's referenced object
        if( writeback_context.obid == NULL ) {
          writeback_context.obid = idcpy( &obid, COMLIB_OBJECT_GETID( evict_obj ) );
        }
      }
      else if( writeback_context.vtype == CELL_VALUE_TYPE_MEMBER ) {
        // build obid from shortid and the cell value
        writeback_context.obid = idset( &obid, writeback_context.value.idH56, writeback_context.key.shortid );
      }
    }

    // valid item = update
    if( _ITEM_IS_VALID(victim) ) {
      eviction = _framehash_radix__set( &writeback_context );
      if( eviction.completed ) { // <- subframe set must complete for eviction to be successful
        _DELETE_ITEM( victim );  // no longer cached
      }
    }
    // invalid item = delete
    else {
      eviction = _framehash_radix__del( &writeback_context ); 
      if( eviction.error == false ) {
        // eviction of deletion is successful when subframe delete does not throw an error (since eviction will not complete if the item doesn't exist below)
        // it is possible to have the non-existence of an item cached, so the above deletion will not complete but also not throw an error
        _DELETE_ITEM( victim );     // no longer cached
        eviction.completed = true;  // declare eviction complete
      }
      else {
        eviction.completed = false; // for debug
      }
    }
  }
  else {
    eviction.error = true;
  }

  return eviction;
}



/*******************************************************************//**
 * __cache_shift_cells
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __cache_shift_cells( framehash_cell_t *top ) {
  // shift two cells down, overwrite last entry, leave space for new entry
  *(top+2) = *(top+1);
  *(top+1) = *(top);
  return top;
}



/*******************************************************************//**
 * __cache_fill_positive
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __cache_fill_positive( const framehash_context_t * const context, framehash_slot_t * const cacheslot ) {
  framehash_cell_t *cell = __cache_shift_cells( cacheslot->cells );

  // write the new item to position j=0
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    return NULL;  // illegal
  case CELL_VALUE_TYPE_MEMBER:
    _STORE_MEMBERSHIP( context, cell );
    return cell;
  case CELL_VALUE_TYPE_BOOLEAN:
    _STORE_BOOLEAN( context, cell );
    return cell;
  case CELL_VALUE_TYPE_UNSIGNED:
    _STORE_UNSIGNED( context, cell );
    return cell;
  case CELL_VALUE_TYPE_INTEGER:
    _STORE_INTEGER( context, cell );
    return cell;
  case CELL_VALUE_TYPE_REAL:
    _STORE_REAL( context, cell );
    return cell;
  case CELL_VALUE_TYPE_POINTER:
    _STORE_POINTER( context, cell );
    return cell;
  case CELL_VALUE_TYPE_OBJECT64:
    _STORE_OBJECT64_POINTER( context, cell );
    return cell;
  case CELL_VALUE_TYPE_OBJECT128: /* COMLIB_VS_POINTER */
    _STORE_OBJECT128_POINTER( context, cell );
    return cell;
  default:
    return NULL;  // illegal
  }
}


/*******************************************************************//**
 * __cache_fill_negative
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __cache_fill_negative( const framehash_context_t * const context, framehash_slot_t * const cacheslot ) {
  framehash_cell_t *cell = __cache_shift_cells( cacheslot->cells );
  _MARK_CACHED_NONEXIST( context, cell );
  return cell;
}



/*******************************************************************//**
 * __cache_get_chain
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __cache_get_chain( framehash_context_t * const context, framehash_slot_t * const cacheslot, framehash_metas_t * const cache_metas ) {
  framehash_cell_t *chaincell = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
  if( _CELL_REFERENCE_IS_NULL( chaincell ) ) {
    framehash_context_t chain_context = CONTEXT_INIT_NEW_FRAME( context, chaincell );
    if( _framehash_memory__new_frame( &chain_context, _FRAMEHASH_P_MIN, cache_metas->domain+1, FRAME_TYPE_LEAF ) == NULL ) {
      return NULL;
    }
    _CACHE_INCREMENT_NCHAINS( cache_metas );
  }
  return chaincell;
}



/*******************************************************************//**
 * __prefetch_chain
 *
 ***********************************************************************
 */
static void __prefetch_chain( framehash_context_t * const context, framehash_cell_t *chaincell ) {
  _PUSH_CONTEXT_FRAME( context, chaincell ) {
    framehash_slot_t *prefetch_slot = NULL;
    framehash_metas_t *prefetch_metas = CELL_GET_FRAME_METAS( context->frame );
    switch( prefetch_metas->ftype ) {
    case FRAME_TYPE_CACHE:
      prefetch_slot = _framehash_radix__get_cacheslot( context );
      break;
    case FRAME_TYPE_LEAF:
      {
        int p = prefetch_metas->order;
        framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( context->frame );
        if( p == 0 ) {
          prefetch_slot = frame_slots - 1;
        }
        else {
          _hashbits_t h16 = _framehash_hashing__get_hashbits( prefetch_metas->domain, context->key.shortid );
          prefetch_slot = frame_slots + _framehash_radix__get_frameindex( p, p-1, (h16 & _ZONE_INDEXMASK( p-1 )) );
        }
      }
      break;
    case FRAME_TYPE_INTERNAL:
      {
        framehash_slot_t *internal_chainslot = __get_internal_chainslot( context );
        if( internal_chainslot ) {
          prefetch_slot = internal_chainslot;
        }
        else { 
          framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( context->frame );
          _hashbits_t h16 = _framehash_hashing__get_hashbits( prefetch_metas->domain, context->key.shortid );
          prefetch_slot = frame_slots + _framehash_radix__get_frameindex( 6, 5, (h16 & _ZONE_INDEXMASK( 5 )) );
        }
      }
      break;
    case FRAME_TYPE_BASEMENT:
      break;
      //TODO
    }

    if( prefetch_slot ) {
      __prefetch_L2( (void*)prefetch_slot );
    }
  } _POP_CONTEXT_FRAME;
}



/*******************************************************************//**
 * __set_cache_cell
 *
 ***********************************************************************
 */
__inline static void __set_cache_cell( framehash_context_t * const context, framehash_cell_t *cache_cell, framehash_slot_t *cache_slot ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    // NOOP for now, in future consider this to mean delete
    return;
  case CELL_VALUE_TYPE_MEMBER:
    _STORE_MEMBERSHIP( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_BOOLEAN:
    _STORE_BOOLEAN( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_UNSIGNED:
    _STORE_UNSIGNED( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_INTEGER:
    _STORE_INTEGER( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_REAL:
    _STORE_REAL( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_POINTER:
    _STORE_POINTER( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_OBJECT64:
    // replacement does NOT delete old object!
    _STORE_OBJECT64_POINTER( context, cache_cell );
    return;
  case CELL_VALUE_TYPE_OBJECT128: /* COMLIB_VS_POINTER */
    // different object replaces old valid object - must evict old before we replace old with new in order to propagate delete all the way to leaf or basement
    if( _ITEM_IS_VALID(cache_cell) && _CELL_REFERENCE_IS_DIFFERENT(cache_cell, context->value.pobject) ) {
      // We are here if a new object with the SAME obid as an old object is inserted.
      // We will NOT do any of this if we're just re-entering an object into the cache because its pointer will still be the same.
      _MARK_ITEM_INVALID( cache_cell ); // old object marked invalid = same as DELETE this object
      _MARK_ITEM_DIRTY( cache_cell );   // old object marked dirty, i.e. modified
      framehash_metas_t *cache_metas = CELL_GET_FRAME_METAS( context->frame );
      // This eviction will DELETE the old object.
      framehash_retcode_t eviction = __cache_evict_cell( context, cache_cell, __cache_get_chain( context, cache_slot, cache_metas ) );
      if( eviction.error ) {
        CRITICAL( 0x203, "cache eviction error" );
      }
    }
    _STORE_OBJECT128_POINTER( context, cache_cell );
    break;
  default:
    CRITICAL( 0x204, "bad cell value type in cache set" );
  }
}



/*******************************************************************//**
 * _framehash_cache__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_cache__set( framehash_context_t * const context ) {
  framehash_retcode_t insertion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  XTRY {

    framehash_slot_t *cacheslot = _framehash_radix__get_cacheslot( context );
    framehash_cell_t *chaincell = NULL;
#if _FRAMEHASH_ENABLE_CACHE_FRAMES
    bool match;

    /* __________________ */
    /* Cache Write Policy */
    /* If a previous version of the same item (valid or invalid) exists in a cell, overwrite the cell then mark the cell as dirty. */
    /* If no previous version exists, follow chain and continue in next domain */
    framehash_cell_t *cell = cacheslot->cells;
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nCL++;
#endif
    for( int j=0; j<_FRAMEHASH_CACHE_CELLS_PER_SLOT; j++, cell++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.ncachecells++;
#endif
      _CELL_ITEM_MATCH( context, cell, &match );
      if( match ) {
        if( _ITEM_IS_INVALID(cell) ) {
          // item count will change when this invalid item becomes valid by setting to new value below
          insertion.delta = true;
        }
        //
        __set_cache_cell( context, cell, cacheslot );
        //
        _MARK_ITEM_DIRTY( cell );  // future eviction will trigger insertion in subdomain due to valid flag
        insertion.completed = true;
        insertion.depth = self_metas->domain;
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.hit = true;
        context->instrument->probe.hit_ftype = FRAME_TYPE_CACHE;
#endif
        break;
      }
    }
#endif

    // No previous version, continue in next domain, create new subframe if it has not been created yet.
    if( insertion.completed == false ) {
      if( chaincell == NULL && (chaincell = __cache_get_chain( context, cacheslot, self_metas )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x205 );
      }
      _PUSH_CONTEXT_FRAME(  context, chaincell ) {
        insertion = _framehash_radix__set( context );
      } _POP_CONTEXT_FRAME;
    }
  }
  XCATCH( errcode ) {
    insertion.error = true;
  }
  XFINALLY {}
  return insertion;
}



/*******************************************************************//**
 * _framehash_cache__del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_cache__del( framehash_context_t * const context ) {
  framehash_retcode_t deletion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  XTRY {
    framehash_slot_t *cacheslot = _framehash_radix__get_cacheslot( context );
    int item_was_cached = false;
#if _FRAMEHASH_ENABLE_CACHE_FRAMES
    bool match;

    /* ___________________ */
    /* Cache Delete Policy */
    /* If the item exists in a cell and is valid, mark the cell invalid and dirty. */
    /* If the item exists in a cell and is already invalid, do nothing. */
    /* If the item does not exist, follow chain and continue in next domain. */
    framehash_cell_t *cell = cacheslot->cells;
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nCL++;
#endif
    for( int j=0; j<_FRAMEHASH_CACHE_CELLS_PER_SLOT; j++, cell++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.ncachecells++;
#endif
      _CELL_ITEM_MATCH( context, cell, &match );
      if( match ) {
        if( _ITEM_IS_VALID( cell ) ) {
          _LOAD_CONTEXT_VALUE( context, cell ); // load vtype and value into context for return to caller
          _MARK_ITEM_INVALID( cell ); // delete 
          _MARK_ITEM_DIRTY( cell );   // and dirty so eviction will propagate down
          if( _CELL_IS_REFERENCE( cell ) ) {
            // Evict the reference immediately since we cannot access the referenced object in the future
            framehash_retcode_t eviction = __cache_evict_cell( context, cell, __cache_get_chain( context, cacheslot, self_metas ) );
            if( eviction.error ) {
              // TODO: rollback? things may get inconsistent now.
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x211 );
            }
            else if( eviction.delta == false ) {
              // Item did not exist in subframe so there may be remaining actions required to complete the deletion
              if( COMLIB_OBJECT_DESTRUCTIBLE( context->value.pobject ) ) {
                // The evicted reference was a destructible object and subframe could not complete destruction - do it now.
                COMLIB_OBJECT_DESTROY( context->value.pobject );
              }
            }
            _MARK_CACHED_NONEXIST( context, cell );
          }
          deletion.delta = true; // <= indicate that this cache has handled the delete completely
          deletion.completed = true;
        }
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.hit = true;
        context->instrument->probe.hit_ftype = FRAME_TYPE_CACHE;
#endif
        deletion.depth = self_metas->domain;
        item_was_cached = true;
        break;
      }
    }
#endif

    // No cached version, continue in next domain if it exists
    if( item_was_cached == false ) {
      framehash_cell_t *next = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
      if( _CELL_REFERENCE_EXISTS(next) ) { // <== NOTE: since we're deleting, no need to create a subtree if we don't have one
        // there's a subtree, delegate deletion to it
        _PUSH_CONTEXT_FRAME( context, next ) {
          deletion = _framehash_radix__del( context );
        } _POP_CONTEXT_FRAME;
        if( deletion.error == false && (deletion.unresizable || deletion.empty) ) {
          // we got a hint that things are getting empty-ish down there
          // simple handler for now: if subtree is completely empty, restructure
          framehash_retcode_t destruction = _framehash_radix__try_destroy_cache_chain( context ); // <- will only succeed if cacheslot is clean!

          // If we're *NOT* a top level cache, attempt to re-structure. This may include total deletion of ourselves if everything is empty
          // or some other heuristic to determine if we should re-structure into a different frame type.
          // NOTE: Top level cache frames are statically defined and will never be modified or deleted.
          if( self_metas->flags.cachetyp == FCACHE_TYPE_CMORPH) {

            if( destruction.error == false && destruction.completed && _CACHE_HAS_NO_CHAINS(self_metas) ) {
              // we destroyed a chain, and it was our last chain, so now we have no real storage underneath ourselves
              // make sure we are clean before we self-destruct!
              framehash_retcode_t flushing = _framehash_cache__flush( context, false ); // <= may result in the creation of new chains if cached items were updated (e.g. now dirty) since chain removed
              if( flushing.completed && _CACHE_HAS_NO_CHAINS(self_metas) ) {
                // we just flushed and we still have no chains, so all our cache slots are clean and we have no chains to subtrees
                _framehash_memory__discard_frame( context );
                deletion.unresizable = false; // clear the unresizable event since we just resized!
              }
              else {
                deletion.empty = false; // not empty after all, since the flushing resulted in brand new chains for dirty items
              }
            }
            
            // more advanced handler: number of chains is small, try to recreate a more compact structure
            else {
              uint16_t nchains = _CACHE_GET_NCHAINS(self_metas);
              uint16_t nslots_half = _CACHE_FRAME_NSLOTS(self_metas->order)/2;
              if( nchains < nslots_half ) {
                // Less than 1/2 of cache slots have chains to sub domains, let's try to compactify
                // 
                framehash_retcode_t compaction = _framehash_radix__try_compaction( context );
                if( compaction.completed ) {
                  deletion.unresizable = false; // clear the unresizable event since we just resized!
                }
                if( compaction.modified ) {
                  deletion.modified = true; // need to flag this up, right??
                }
              }
              else {
                deletion.empty = false; // not empty, either the cacheslot had dirty items, or there are still other slots with chains
              }
            }
          }
        }
      }
    }
  }
  XCATCH( errcode ) {
    deletion.error = true;
  }
  XFINALLY {}
  return deletion;
}



/*******************************************************************//**
 * _framehash_cache__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_cache__get( framehash_context_t * const context ) {
  framehash_retcode_t retrieval = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif
  
  framehash_slot_t *cacheslot = _framehash_radix__get_cacheslot( context );

  XTRY {
    framehash_cell_t *cell;
    framehash_cell_t *chaincell = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
#if _FRAMEHASH_ENABLE_CACHE_CHAIN_PREFETCH
    // Get the chain prefetch (to L2) in early if recent hitrate is poor
    if( self_metas->hitrate.val < 4 ) { // < 4 is roughly < 25%, hitrate range is 4 bits, i.e. 0-15 linear.
      __prefetch_chain( context, chaincell );
    }
#endif
#if _FRAMEHASH_ENABLE_CACHE_FRAMES
    bool match;
#if _FRAMEHASH_USE_MEMORY_STREAMING
    framehash_cell_t mcell[FRAMEHASH_CELLS_PER_SLOT];
    if( self_metas->domain >= _FRAMEHASH_MEMORY_STREAMING_MIN_DOMAIN ) {
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
        mcell[j].m128i = _mm_stream_load_si128( (__m128i*)cacheslot + j );
      }
      cell = mcell;
      _mm_lfence();
    }
    else {
      cell = cacheslot->cells;
    }
#else
    cell = cacheslot->cells;
#endif
    /* _________________ */
    /* Cache Read Policy */
    /* TBD! */
    /*      */
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nCL++;
#endif
    for( int j=0; j<_FRAMEHASH_CACHE_CELLS_PER_SLOT; j++, cell++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.ncachecells++;
#endif
      _CELL_ITEM_MATCH( context, cell, &match );
      if( match ) {
        if( _ITEM_IS_VALID( cell ) ) {
          _LOAD_CONTEXT_VALUE( context, cell );
          retrieval.completed = true;
        }
        else {
          _LOAD_NULL_CONTEXT_VALUE( context );
        }
#if _FRAMEHASH_ENABLE_READ_CACHE
        if( context->control.cache.enable_read ) {
          retrieval.cacheable = true;  // a cache hit is cacheable in parent
        }
#endif
#ifdef FRAMEHASH_INSTRUMENTATION
        context->instrument->probe.hit = true;
        context->instrument->probe.hit_ftype = FRAME_TYPE_CACHE;
#endif
        retrieval.depth = self_metas->domain;

        // CACHE HIT
        self_metas->hitrate._acc += (uint8_t)(~self_metas->hitrate._acc) >> 4;
        XBREAK;  // <= cache entry found, if invalid we conclude "not found", and also allow this condition to be cacheable
      }
    }
#endif

    // CACHE MISS
    self_metas->hitrate._acc -= self_metas->hitrate._acc >> 4;

    // No cached version, continue in next domain if it exists
#if _FRAMEHASH_USE_MEMORY_STREAMING && _FRAMEHASH_ENABLE_CACHE_FRAMES
    if( self_metas->domain >= _FRAMEHASH_MEMORY_STREAMING_MIN_DOMAIN ) {
      chaincell = &mcell[_FRAMEHASH_CACHE_CHAIN_CELL_J];
    }
    else {
      chaincell = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
    }
#else

    chaincell = &cacheslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
#endif
    if( _CELL_REFERENCE_EXISTS( chaincell ) ) {
      _PUSH_CONTEXT_FRAME( context, chaincell ) {
        retrieval = _framehash_radix__get( context );
      } _POP_CONTEXT_FRAME;

#if _FRAMEHASH_ENABLE_READ_CACHE
      if( retrieval.cacheable && context->control.cache.enable_read ) { // <= fill cache only if the fetched item is cacheable
        // let's cache the item we retrieved, which means we must evict another (if the other is dirty, otherwise just overwrite it)
        framehash_cell_t *victim = &cacheslot->cells[ _FRAMEHASH_CACHE_EVICTION_J ];
        framehash_retcode_t eviction = {0};
        if( _ITEM_IS_DIRTY( victim ) ) {
          // evict dirty victim
          eviction = __cache_evict_cell( context, victim, __cache_get_chain( context, cacheslot, self_metas ) );
        }
        else {
          // consider clean victim as eviction completed
          eviction.completed = true; 
        }

        if( eviction.completed ) {
          // enter most recent search result into cache (only if eviction succeeded)
          if( retrieval.completed && context->vtype != CELL_VALUE_TYPE_NULL ) {
            cell = __cache_fill_positive( context, cacheslot );
          }
          else {
            cell = __cache_fill_negative( context, cacheslot );
          }
          if( cell == NULL ) {
            THROW_FATAL_MESSAGE( CXLIB_ERR_BUG, 0x221, "invalid cache fill" ); // severe punishment
          }
          if( eviction.unresizable ) {
            // TODO!!!  what if the eviction above caused a deletion, which fully emptied the subtree, which means it reported unresizable?
            //  Unless we handle it here, the empty subtree won't be deleted since the unresizable "event" was ignored.
          }
        }
        else if( eviction.error ) {
          //TODO: if eviction.error, check if chaincell is NULL and try again?
        }
#if _FRAMEHASH_CONSERVATIVE_READ_CACHE
        retrieval.cacheable = false; // a cache fill after miss is NOT cacheable in parent. (if accessed again in the near future, it will be a hit and then be cacheable in parent.)
#endif
        
      }
#endif
    }
  }
  XCATCH( errcode ) {
    retrieval.error = true;
  }
  XFINALLY {
  }
  return retrieval;
}



/*******************************************************************//**
 * _framehash_cache__flush_slot
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_cache__flush_slot( framehash_context_t * const context, framehash_slot_t * const chainslot, bool invalidate ) {
  framehash_retcode_t slot_flushing = {0};
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( context->frame );
  bool incomplete = false;

  // evict any dirty items in this slot
  framehash_cell_t *const chaincell = &chainslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
  
  framehash_retcode_t eviction = __cache_evict_slot( context, chainslot, frame_metas, invalidate ); // try first...
  if( eviction.error && _CELL_REFERENCE_IS_NULL(chaincell) ) {
    // aha. there were dirty items but somehow we had no subtree to flush to.
    framehash_context_t chain_context = CONTEXT_INIT_NEW_FRAME( context, chaincell );
    if( _framehash_memory__new_frame( &chain_context, _FRAMEHASH_P_MIN, frame_metas->domain+1, FRAME_TYPE_LEAF ) == NULL ) {
      /* sorry - there's nothing we can do now, continue with best effort */
      slot_flushing.error = true;
      slot_flushing.completed = false;
      return slot_flushing;   // :-(
    }
    _CACHE_INCREMENT_NCHAINS( frame_metas );

    // try one more time
    eviction = __cache_evict_slot( context, chainslot, frame_metas, invalidate );
    if( eviction.error ) {
      slot_flushing.error = true;
      slot_flushing.completed = false;
      return slot_flushing;   // it's not our day
    }
  }
  //
  if( eviction.completed == false ) {
    incomplete = true;
  }

  slot_flushing.completed = incomplete ? false : true;

  return slot_flushing;
}



/*******************************************************************//**
 * _framehash_cache__flush
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_cache__flush( framehash_context_t * const context, bool invalidate ) {
  framehash_retcode_t flushing = {0};
  framehash_cell_t *frame = context->frame;
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  int p = frame_metas->order;
  framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( frame );
  framehash_slot_t *chainslot;
  framehash_cell_t *chaincell;
  int nslots;
  bool incomplete = false;
  bool error = false;

  if( frame_slots ) {

    switch( frame_metas->ftype ) {
    case FRAME_TYPE_CACHE:
      // We are a cache. Our responsibility is to evict all dirty items and delegate the
      // flushing process to the next domain.
      nslots = _CACHE_FRAME_NSLOTS( p );
      chainslot = frame_slots;
      for( int fx=0; fx<nslots; fx++, chainslot++ ) {
        // evict any dirty items in this slot
        framehash_retcode_t slot_flush = _framehash_cache__flush_slot( context, chainslot, invalidate );
        if( slot_flush.error == true ) {
          error = true;
        }
        if( slot_flush.completed == false ) {
          incomplete = true;
        }
        if( error ) {
          continue;
        }

        // descend to next domain if it exists (could be any frame type) and continue flushing down there
        chaincell = &chainslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
        if( _CELL_REFERENCE_EXISTS(chaincell) ) {
          _PUSH_CONTEXT_FRAME( context, chaincell ) {
            flushing = _framehash_cache__flush( context, invalidate );
          } _POP_CONTEXT_FRAME;
          if( flushing.completed == false ) {
            incomplete = true;
          }
          if( flushing.error ) {
            error = true;
          }
        }

      }
      break;
    case FRAME_TYPE_INTERNAL:
      // We are an internal frame. Our responsibility is to delegate the flushing process
      // to the next domain.
      nslots = _FRAME_NCHAINSLOTS( p );
      chainslot = frame_slots + _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE( p ), 0 ); // first slot in chainzone
      for( int q=0; q<nslots; q++, chainslot++ ) {
        // scan all chainslots in the chainzone
        if( _framehash_radix__is_chain( frame_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
          // slot is a chain, now flush all children in the slot
          chaincell = chainslot->cells;
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, chaincell++ ) {
            _PUSH_CONTEXT_FRAME( context, chaincell ) {
              flushing = _framehash_cache__flush( context, invalidate );
            } _POP_CONTEXT_FRAME;
            if( flushing.completed == false ) {
              incomplete = true;
            }
            if( flushing.error ) {
              error = true;
            }
          }
        }
      }
      break;
    }
    // eviction is complete if none of the individual evictions were incomplete
    flushing.completed = incomplete ? false : true;
    flushing.error = error ? true : false;
  }
  else {
    flushing.error = true;
    flushing.completed = false;
  }

  return flushing;
}



/*******************************************************************//**
 * _framehash_cache__hitrate
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_cache__hitrate( framehash_context_t * const context, framehash_cache_hitrate_t * const hitrate ) {
  framehash_cell_t *frame = context->frame;
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS( frame );
  int p = frame_metas->order;
  int H = frame_metas->domain;
  framehash_slot_t *frame_slots = CELL_GET_FRAME_SLOTS( frame );

  if( frame_slots ) {
    switch( frame_metas->ftype ) {
    case FRAME_TYPE_CACHE:
      {
        // Add our own hitrate stats
        hitrate->accval[ H ] += frame_metas->hitrate.val;
        hitrate->count[ H ] += 1;

        // Descend
        int nslots = _CACHE_FRAME_NSLOTS( p );
        for( int fx=0; fx<nslots; fx++ ) {
          framehash_cell_t *chaincell = &frame_slots[fx].cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
          if( _CELL_REFERENCE_EXISTS(chaincell) ) {
            _PUSH_CONTEXT_FRAME( context, chaincell ) {
              _framehash_cache__hitrate( context, hitrate );
            } _POP_CONTEXT_FRAME;
          }
        }
      }
      break;
    case FRAME_TYPE_INTERNAL:
      {
        // Descend
        int nslots = _FRAME_NCHAINSLOTS( p );
        framehash_slot_t *chainzone = frame_slots + _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE( p ), 0 ); // first slot in chainzone
        for( int q=0; q<nslots; q++ ) {
          if( _framehash_radix__is_chain( frame_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
            framehash_cell_t *chaincell = chainzone[q].cells;
            for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, chaincell++ ) {
              _PUSH_CONTEXT_FRAME( context, chaincell ) {
                _framehash_cache__hitrate( context, hitrate );
              } _POP_CONTEXT_FRAME;
            }
          }
        }
      }
      break;
    }
  }

  return 0;
}


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_cache.h"

DLL_HIDDEN test_descriptor_t _framehash_cache_tests[] = {
  { "cache",   __utest_framehash_cache },
  {NULL}
};
#endif
