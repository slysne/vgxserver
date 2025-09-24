/*######################################################################
 *#
 *# basement.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"
#include "_fcell.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


static framehash_retcode_t __basement_probe( const framehash_context_t * const context, const framehash_cell_t ** const target, const bool find_available );
static void __basement_cleanup( framehash_context_t * const context );


/*******************************************************************//**
 * __basement_probe
 *
 ***********************************************************************
 */
static framehash_retcode_t __basement_probe( const framehash_context_t * const context, const framehash_cell_t ** const target, const bool find_available ) {
  framehash_retcode_t location = {0};
  framehash_cell_t *basement_addr = CELL_GET_FRAME_CELLS( context->frame );
  framehash_cell_t *cell;
  bool match;

#if _FRAMEHASH_USE_MEMORY_STREAMING
  framehash_cell_t mcell[_FRAMEHASH_BASEMENT_SIZE];

  for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++ ) {
    mcell[j].m128i = _mm_stream_load_si128( (__m128i*)basement_addr + j );
  }
  cell = mcell; // cell points into the local mcell[] buffer
  _mm_lfence();
#else
  cell = basement_addr;
#endif

#ifdef FRAMEHASH_INSTRUMENTATION
  const int clj = 2; // j-in-slot where next cache line will be loaded
  context->instrument->probe.nCL++;
#endif
  // 1st cacheline in basement
  for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++, cell++ ) {
#ifdef FRAMEHASH_INSTRUMENTATION
    context->instrument->probe.nbasementcells++;
    if( j == clj ) {
      context->instrument->probe.nCL++;
    }
#endif
    if( _ITEM_IS_VALID( cell ) ) {
      _CELL_ITEM_MATCH( context, cell, &match );
      if( match ) {
        *target = basement_addr + j;
        location.completed = true;
        break;
      }
    }
    else if( _CELL_IS_END( cell ) ) {
      // We hit the end marker
      if( find_available ) {
        if( *target == NULL ) {
          // update target with END unless we found an earlier target
          *target = basement_addr + j;
        } /* else: we found EMPTY earlier and we'll use that since it's higher up */
        location.completed = true;
      }
      break;
    }
    else if( _CELL_IS_EMPTY( cell ) ) {
      // We hit an empty/available cell
      if( find_available && *target == NULL ) {
        // update target with EMPTY unless we already found an earlier target
        *target = basement_addr + j;
        // but we're never completed by finding EMPTY, since the item may exist farther down
      }
    }
  }

#if _FRAMEHASH_ENABLE_ZONE_PREFETCH
  // Probing determined a target
  if( *target != NULL ) {
    // EXPERIMENT  : I don't know if this prefetch will change the hint and quickly move data from the streaming load buffers to L1 with NTA hint, or re-load from MEMORY
    __prefetch_L1( (void*)*target ); // we're going to need it - TODO: but we don't know for how long - this might be good or bad
    // EXPERIMENT
  }
#endif

  return location;
}



/*******************************************************************//**
 * __basement_cleanup
 *
 ***********************************************************************
 */
static void __basement_cleanup( framehash_context_t * const context ) {
  framehash_cell_t *basement = context->frame;
  framehash_metas_t *basement_metas = CELL_GET_FRAME_METAS( basement );
  framehash_cell_t tmp;
  uint8_t domain = basement_metas->domain;

  if( basement_metas->nactive == 0 ) {
    if( basement_metas->hasnext == true ) {
      _PUSH_CONTEXT_FRAME( context, _framehash_basementallocator__get_chain_cell( basement ) ) {
        if( _CELL_REFERENCE_EXISTS( context->frame ) ) {
          // we are empty but there is stuff in the next basement
          _CELL_STEAL( &tmp, context->frame );     // save the next basement since we're about to destroy ourselves
          _framehash_memory__discard_basement( context );              // destroy ourselves, but will not find the next basement
          _CELL_STEAL( basement, &tmp );              // resurrect ourselves as the next basement!
          basement_metas = CELL_GET_FRAME_METAS( basement ); // the next basement's domain is now lifted up to the destroyed basement
          basement_metas->domain = domain;            //    creating a discontinuity in the numbering below, but we think it's ok.
          __basement_cleanup( context );             // continue cleanup below
        }
        else {
          /* CORRUPTION - TODO: what? */
        }
      } _POP_CONTEXT_FRAME;
    }
  }
}



/*******************************************************************//**
 * _framehash_basement__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_basement__set( framehash_context_t * const context ) {
  framehash_retcode_t insertion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS(self);

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  XTRY {
    framehash_cell_t *target = NULL;
    framehash_retcode_t location = __basement_probe( context, (const framehash_cell_t**)&target, true );

    // Did we find a suitable target cell?
    if( location.completed ) {
      if( _ITEM_IS_INVALID(target) ) {
        // unused cell, we are inserting a new item
        self_metas->nactive++;
        insertion.delta = true;
      }
#ifdef FRAMEHASH_INSTRUMENTATION
      else {
        context->instrument->probe.hit = true;
        context->instrument->probe.hit_ftype = FRAME_TYPE_BASEMENT;
      }
#endif
      //
      __set_cell( context, target );
      //
      insertion.completed = true;
      insertion.depth = self_metas->domain;
    }
    else {
      // We didn't find an available cell in this basement, keep digging
      _PUSH_CONTEXT_FRAME( context, _framehash_basementallocator__get_chain_cell( self ) ) {
        if( self_metas->hasnext == false ) {
          // no sub-basement exists, let's create another
          if( _CELL_REFERENCE_EXISTS( context->frame ) ) {
            THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x503 );
          }
          framehash_cell_t sub_basement;
          framehash_context_t sub_context = CONTEXT_INIT_NEW_FRAME( context, &sub_basement );
          if( self_metas->domain < FRAME_DOMAIN_LAST_BASEMENT ) {
            if( _framehash_memory__new_basement( &sub_context, self_metas->domain+1 ) == NULL ) {
              THROW_ERROR( CXLIB_ERR_MEMORY, 0x504 );
            }
          }
          else {
            insertion.depth = self_metas->domain;
            insertion.unresizable = true;
            THROW_ERROR( CXLIB_ERR_CAPACITY, 0x505 );
          }

          _CELL_STEAL( context->frame, sub_context.frame );
          self_metas->hasnext = true;
        }
        // Continue insertion attempt in the next basement
        insertion = _framehash_basement__set( context );
      } _POP_CONTEXT_FRAME;
    }
  }
  XCATCH( errcode ) {
    insertion.error = true;
  }
  XFINALLY {
  }
  return insertion;
}



/*******************************************************************//**
 * _framehash_basement__del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_basement__del( framehash_context_t * const context ) {
  framehash_retcode_t deletion = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif
  
  XTRY {
    framehash_cell_t *target;
    framehash_retcode_t location = __basement_probe( context, (const framehash_cell_t**)&target, false );
    // Did we find the item?
    if( location.completed ) {
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.hit = true;
      context->instrument->probe.hit_ftype = FRAME_TYPE_BASEMENT;
#endif

      deletion.delta = true;
      deletion.completed = true;
      deletion.depth = self_metas->domain;

      _LOAD_CONTEXT_VALUE_TYPE( context, target );

      // Small enough to restructure?
      if( _framehash_leaf__delete_item( self, target ) == 0 ) {
        __basement_cleanup( context );
        if( self_metas->nactive == 0 ) {  // still empty after cleanup/consolidation of sub-basement(s) ?
          deletion.empty = true;
          deletion.unresizable = true;  // communicate up that we're empty down here but can't resize ourselves.
        }
      }
    }
    else {
      // We didn't find our item in this basement, try next basement if we have one
      if( self_metas->hasnext == true ) {
        _PUSH_CONTEXT_FRAME( context, _framehash_basementallocator__get_chain_cell( self ) ) {
          if( _CELL_REFERENCE_IS_NULL( context->frame ) ) {
            THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x511 );
          }
          deletion = _framehash_basement__del( context );
        } _POP_CONTEXT_FRAME;
      }
      else {
        deletion.depth = self_metas->domain; // this is how far we got (without deleting anything)
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



/*******************************************************************//**
 * _framehash_basement__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_retcode_t _framehash_basement__get( framehash_context_t * const context ) {
  framehash_retcode_t retrieval = {0};
  framehash_cell_t *self = context->frame;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( self );

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument->probe.depth = self_metas->domain;
#endif

  XTRY {
    framehash_cell_t *hit = NULL;
    framehash_retcode_t location = __basement_probe( context, (const framehash_cell_t**)&hit, false );
    // did we find a match?
    if( location.completed ) {
      _LOAD_CONTEXT_VALUE( context, hit );
      retrieval.completed = true;
#ifdef FRAMEHASH_INSTRUMENTATION
      context->instrument->probe.hit = true;
      context->instrument->probe.hit_ftype = FRAME_TYPE_BASEMENT;
#endif
      retrieval.depth = self_metas->domain;
    }
    else {
      // We didn't find our item in this basement, try next basement?
      if( self_metas->hasnext == true ) {
        _PUSH_CONTEXT_FRAME( context, _framehash_basementallocator__get_chain_cell( context->frame ) ) {
          if( _CELL_REFERENCE_IS_NULL( context->frame ) ) {
            THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x521 );
          }
          retrieval = _framehash_basement__get( context );
        } _POP_CONTEXT_FRAME;
      }
      else {
        /* no match */
        _LOAD_NULL_CONTEXT_VALUE( context );
        retrieval.depth = self_metas->domain; // This is how far we got (without finding anything)
      }
    }
  }
  XCATCH( errcode ) {
    retrieval.error = true;
  }
  XFINALLY {
#if _FRAMEHASH_ENABLE_READ_CACHE
    if( context->control.cache.enable_read ) {
      retrieval.cacheable = true;  // we allow both hits AND misses to be cacheable!
    }
#endif
  }
   
  return retrieval;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_basement.h"


DLL_HIDDEN test_descriptor_t _framehash_basement_tests[] = {
  { "basement",   __utest_framehash_basement },
  {NULL}
};
#endif

