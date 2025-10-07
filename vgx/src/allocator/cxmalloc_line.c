/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_line.c
 * Author:  Stian Lysne slysne.dev@gmail.com
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );



static  void * __cxmalloc_line__new_OPEN(               cxmalloc_family_t *family, uint32_t sz );
static  void * __cxmalloc_line__new_oversized_OPEN(     cxmalloc_family_t *family, uint32_t sz );
static  void * __cxmalloc_line__renew_OPEN(             cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
static int64_t __cxmalloc_line__discard_OPEN(           cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
static int64_t __cxmalloc_line__discard_NOLOCK(         cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
static     int __cxmalloc_line__validate_inactive_BCS(  const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead );
static int64_t __cxmalloc_line__validate_refcount_BCS(  const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead );





DLL_HIDDEN _icxmalloc_line_t _icxmalloc_line = {
  .New_OPEN             = __cxmalloc_line__new_OPEN,
  .NewOversized_OPEN    = __cxmalloc_line__new_oversized_OPEN,
  .Renew_OPEN           = __cxmalloc_line__renew_OPEN,
  .Discard_OPEN         = __cxmalloc_line__discard_OPEN,
  .Discard_NOLOCK       = __cxmalloc_line__discard_NOLOCK,
  .ValidateInactive_BCS = __cxmalloc_line__validate_inactive_BCS,
  .ValidateRefcount_BCS = __cxmalloc_line__validate_refcount_BCS
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void * __cxmalloc_line__new_oversized_OPEN( cxmalloc_family_t *family, uint32_t sz ) {
  void *arraydata = NULL;

  // 1. Get the "virtual" aidx and its size
  uint32_t alength;
  uint16_t aidx = _icxmalloc_shape.GetAIDX_FRO( family, sz, &alength );
  
  // 2. Calculate the number of one-off malloc bytes for this virtual aidx
  int64_t bytes = family->header_bytes + (int64_t)alength * family->descriptor->unit.sz; // <- by design, always a multiple of cacheline size

  // 3: Allocate in terms of linechunks (page aligned)
  cxmalloc_linechunk_t *linechunk;
  if( PALIGNED_ARRAY( linechunk, cxmalloc_linechunk_t, bytes/sizeof(cxmalloc_linechunk_t) ) != NULL ) {
    cxmalloc_linehead_t *linehead = (cxmalloc_linehead_t*) linechunk;

    // 4: Prepare line for use by caller
    linehead->metaflex.M1 = family->descriptor->meta.initval.M1;
    linehead->metaflex.M2 = family->descriptor->meta.initval.M2;
    linehead->data.aidx = aidx;  // virtual aidx
    linehead->data.bidx = 0xFFFF; // N/A
    linehead->data.flags.ovsz = 1;     // yes, we're oversized
    linehead->data.flags.invl = 0;     // we're valid
    linehead->data.flags._chk = 0;     //
    linehead->data.flags._act = 1;     // active
    linehead->data.flags._mod = 1;     //
    linehead->data.flags._rsv = 0;     // reserved
    linehead->data.offset = 0;
    linehead->data.size = alength; // length is the size of the virtual aidx
    linehead->data.refc = 1;     // caller will be only owner after this

    
    // 5: Advance return pointer to start of data segment
    arraydata = _cxmalloc_array_from_linehead( family, linehead );
  }

  return arraydata;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void * __cxmalloc_line__new_OPEN( cxmalloc_family_t *family, uint32_t sz ) {
  void *arraydata = NULL;

  // 1: Select allocator
  uint16_t aidx = _icxmalloc_shape.GetAIDX_FRO( family, sz, NULL );

  // 2: Grab the allocator
  cxmalloc_allocator_t *allocator = family->allocators[ aidx ];

  // 3: Create new if not already initialized
  if( allocator == NULL ) { // <= quick unlocked check
    IF_WRITABLE_FAMILY_THEN_SYNCHRONIZE( family ) {
      if( (allocator = family_CS_W->allocators[ aidx ]) == NULL ) { // <= locked check - still NULL?
        family_CS_W->allocators[ aidx ] = allocator = _icxmalloc_allocator.CreateAllocator_FCS( family_CS_W, aidx );
      }
    } END_SYNCHRONIZED_WRITABLE_FAMILY;
    if( allocator == NULL ) {
      return NULL;
    }
  }

  // 4: Allocate a new line
  cxmalloc_linehead_t *linehead = NULL;
  IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( allocator ) {
    linehead = _icxmalloc_allocator.New_ACS( allocator_CS_W );
  } END_SYNCHRONIZED_WRITABLE_ALLOCATOR;
  
  if( linehead ) {
    // 5: Incref (caller is single owner after this)
    linehead->data.refc = 1;
    // 6: Set return pointer to start of data segment
    arraydata = _cxmalloc_array_from_linehead( family, linehead );
  }

  return arraydata;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void * __cxmalloc_line__renew_OPEN( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead ) {

  void *renewed_line = NULL;

  /*
  TODO:
  Implement collaborative defragmentation.
  By calling renew() a client make a collaborative effort helping the allocator
  to consolidate active allocations into a smaller set of blocks. When the line
  upon which renew() is called belongs to a block other than the current block
  whose fillrate is below a threshold, the line is re-allocated in the current block
  and the data copied from the old to the new. The old line is discarded and the
  new block is returned. This can only be carried out for single ownership, however.
  If the line's refcount is >1 no action is possible.
  */

  cxmalloc_allocator_t *allocator = family->allocators[ linehead->data.aidx ];
  // our block is low-utilization, let's try to compactify
  IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( allocator ) {
    cxmalloc_block_t *block_CS = allocator_CS_W->blocks[ linehead->data.bidx ];
    if( block_CS->available < block_CS->defrag_threshold ) {
      /* proceed only if exactly one owner and the block is not the current active block */
      if( linehead->data.refc == 1 && block_CS != *allocator_CS_W->head ) {
        cxmalloc_linehead_t *new_linehead = _icxmalloc_allocator.New_ACS( allocator_CS_W );
        if( new_linehead != NULL ) {
          void *new_line, *old_line;
          size_t n_cachelines = (size_t)allocator_CS_W->shape.linemem.awidth * (size_t)allocator_CS_W->shape.linemem.unit_sz / sizeof(cacheline_t);

          /* One new owner */
          new_linehead->data.refc = 1;

          /* Copy existing data to the new line */
          // 1. metaflex 
          new_linehead->metaflex.M1.bits = linehead->metaflex.M1.bits;
          new_linehead->metaflex.M2.bits = linehead->metaflex.M2.bits;
          // 2. extra header
          new_line = _cxmalloc_array_from_linehead( family, new_linehead );
          old_line = _cxmalloc_array_from_linehead( family, linehead );
          memcpy( _cxmalloc_object_from_array( family, new_line ), _cxmalloc_object_from_array( family, old_line ), family->object_bytes );
          // 3. main array
          #if defined CXPLAT_ARCH_X64
          stream_memset( new_line, old_line, n_cachelines );
          #else
          memcpy( new_line, old_line, sizeof(cacheline_t) * n_cachelines );
          #endif

          /* Free the old line */
          linehead->data.refc--;

          _icxmalloc_allocator.Delete_ACS( allocator_CS_W, linehead );
          _icxmalloc_chain.ManageChain_ACS( allocator_CS_W, block_CS );
          // TODO: anything else to do cleanup????

          /* Patch in the new line */
          renewed_line = new_line;
        }
        else { /* silently fail for now - no harm just no defrag */ }
      }
    }
  } END_SYNCHRONIZED_WRITABLE_ALLOCATOR;
  
  return renewed_line;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_line__discard_OPEN( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead ) {
  int64_t refcnt = -1; // error if we don't succeed below

  if( linehead->data.flags.ovsz ) {
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      refcnt = --(linehead->data.refc);
      if( refcnt == 0 ) {
        ALIGNED_FREE( linehead );
      }
    } RELEASE_CXMALLOC_FAMILY;
#ifdef CXMALLOC_CONSISTENCY_CHECK
    if( refcnt < 0 ) {
      CXMALLOC_FATAL( 0x901, "Refcount wrapped to negative" );
    }
#endif
  }
  else {
    if( family->flag.lazy_discards ) {
      /* obliteration pending - don't waste time */
      linehead->data.flags.invl = 1; // ok to do without lock
      refcnt = 0; // special case
    }
    else {
      SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
        // We are allowed to DECREF EVEN IF READONLY, so long as the refcount does not go to zero!
        if( (refcnt = --(linehead->data.refc)) == 0 ) {
          // Refcnt = 0, which requires the lock and writable access (since we will modify the allocator structure by checking line back in)
          // Grab the allocator
          cxmalloc_allocator_t *allocator = family_CS->allocators[ linehead->data.aidx ];
          // Do the following only if allocator is WRITABLE.
          // This block is SKIPPED if allocator is READONLY.
          refcnt = -1; // we need to succeed below to flip this back to the 0 return value
          IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( allocator ) {
            // put the line back into line register
            cxmalloc_block_t *block_CS = _icxmalloc_allocator.Delete_ACS( allocator_CS_W, linehead );
            // deletion complete
            refcnt = 0;
#ifdef CXMALLOC_CONSISTENCY_CHECK
            int64_t n_available_registry = _icxmalloc_block.ComputeAvailable_ARO( block_CS );
            int64_t available = block_CS->available;
            int64_t active = block_CS->capacity - available;
            int64_t n_active_linedata = active; // _icxmalloc_block.CountActiveLines_ARO( block_CS );
            if( n_available_registry != available || n_active_linedata != active ) {
              CXMALLOC_CRITICAL( 0x902, "Block corruption detected after line deletion in %s: block.available=%lld, count(registry)=%lld, block.active=%lld, count(linedata)=%lld", __FUNCTION__, available, n_available_registry, active, n_active_linedata );
              CXMALLOC_INFO( 0x902, "Forcing correction of counters to match registry" );
              block_CS->available = n_available_registry;
            }
#endif
            // manage the re-use and hole chain, and do cleanup as needed
            _icxmalloc_chain.ManageChain_ACS( allocator_CS_W, block_CS );
          } END_SYNCHRONIZED_WRITABLE_ALLOCATOR;
#ifdef CXMALLOC_CONSISTENCY_CHECK
          if( refcnt == -1 ) {
            CXMALLOC_FATAL( 0x903, "Discarded line leaked due to readonly allocator in %s", __FUNCTION__ );
          }
#endif
        }
        else if( refcnt < 0 ) {
    #ifndef NDEBUG
          cxlib_print_backtrace( 0 );
    #endif
          CXMALLOC_WARNING( 0x904, "%s attempted discard when refcnt=%lu", __FUNCTION__, refcnt+1 );
        }
      } RELEASE_CXMALLOC_FAMILY;

      //
      //  REFACTOR STOP.  Not done!
      //

      int32_t remaining_blocks = 1;
      if( remaining_blocks == 0 ) { //TODO: what's remaining blocks for? DO something about it.
        IF_WRITABLE_FAMILY_THEN_SYNCHRONIZE( family ) {
          cxmalloc_allocator_t *allocator = family_CS_W->allocators[ linehead->data.aidx ];
          _icxmalloc_allocator.DestroyAllocator_FCS( family_CS_W, &allocator );
        } END_SYNCHRONIZED_WRITABLE_FAMILY;
      }

      //
      // ...........................
      //
    }
  }
  return refcnt;
}



/*******************************************************************//**
 * 
 * NOTE: Call this method only if there is no chance another thread
 * will operate against this family instance.
 ***********************************************************************
 */
static int64_t __cxmalloc_line__discard_NOLOCK( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead ) {
  int64_t refcnt = -1; // error if we don't succeed below

  if( linehead->data.flags.ovsz ) {
      refcnt = --(linehead->data.refc);
      if( refcnt == 0 ) {
        ALIGNED_FREE( linehead );
      }
#ifdef CXMALLOC_CONSISTENCY_CHECK
    if( refcnt < 0 ) {
      CXMALLOC_FATAL( 0x911, "Refcount wrapped to negative" );
    }
#endif
  }
  else {
    if( family->flag.lazy_discards ) {
      /* obliteration pending - don't waste time */
      linehead->data.flags.invl = 1; // ok to do without lock
      refcnt = 0; // special case
    }
    else {
      // We are allowed to DECREF EVEN IF READONLY, so long as the refcount does not go to zero!
      //
      // TODO: Make sure this decref is really atomic. (It isn't now.)
      //
      if( (refcnt = --(linehead->data.refc)) == 0 ) { // atomic decrement (not really)
#ifdef CXMALLOC_CONSISTENCY_CHECK
        if( CALLABLE( family )->IsReadonly( family ) ) {
          CXMALLOC_FATAL( 0x912, "Attempted object decref to zero with readonly allocator" );
        }
#endif
        // Refcnt went to zero, which requires the lock and writable access (since we will modify the allocator structure by checking line back in)
        // Grab the allocator.
        // NOTE: WE MUST STILL SYNCHRONIZE ON THE ALLOCATOR even if this called within
        // a nolock context. It is only the family which is nolock, if we have to touch the
        // allocator (because refcnt will go to zero) we have to lock the allocator.
        cxmalloc_allocator_t *allocator = family->allocators[ linehead->data.aidx ];
        // Do the following only if allocator is WRITABLE.
        // This block is SKIPPED if allocator is READONLY.
        refcnt = -1; // assume failure to deallocate until proven successful below
        IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( allocator ) {
          // put the line back into line register
          cxmalloc_block_t *block_CS = _icxmalloc_allocator.Delete_ACS( allocator_CS_W, linehead );
          // deletion complete
          refcnt = 0;
#ifdef CXMALLOC_CONSISTENCY_CHECK
          int64_t n_available_registry = _icxmalloc_block.ComputeAvailable_ARO( block_CS );
          int64_t available = block_CS->available;
          int64_t active = block_CS->capacity - available;
          int64_t n_active_linedata = active; // _icxmalloc_block.CountActiveLines_ARO( block_CS );
          if( n_available_registry != available || n_active_linedata != active ) {
            CXMALLOC_CRITICAL( 0x913, "Block corruption detected after line deletion in %s: block.available=%lld, count(registry)=%lld, block.active=%lld, count(linedata)=%lld", __FUNCTION__, available, n_available_registry, active, n_active_linedata );
            CXMALLOC_INFO( 0x914, "Forcing correction of counters to match registry" );
            block_CS->available = n_available_registry;
          }
#endif
          // manage the re-use and hole chain, and do cleanup as needed
         _icxmalloc_chain.ManageChain_ACS( allocator_CS_W, block_CS );
        } END_SYNCHRONIZED_WRITABLE_ALLOCATOR;

#ifdef CXMALLOC_CONSISTENCY_CHECK
        if( refcnt == -1 ) {
          CXMALLOC_FATAL( 0x915, "Discarded line leaked due to readonly allocator in %s", __FUNCTION__ );
        }
#endif
      }
      else if( refcnt < 0 ) {
  #ifndef NDEBUG
        cxlib_print_backtrace( 0 );
  #endif
        CXMALLOC_WARNING( 0x916, "%s attempted discard when refcnt=%lld", __FUNCTION__, refcnt+1 );
      }

      //
      //  REFACTOR STOP.  Not done!
      //

      int32_t remaining_blocks = 1;
      if( remaining_blocks == 0 ) { //TODO: what's remaining blocks for? DO something about it.
        cxmalloc_allocator_t *allocator = family->allocators[ linehead->data.aidx ];
        _icxmalloc_allocator.DestroyAllocator_FCS( family, &allocator );
      }

      //
      // ...........................
      //
    }
  }
  return refcnt;
}



/*******************************************************************//**
 * 
 * NOTE: Call this method only if there is no chance another thread
 * will operate against this family instance.
 ***********************************************************************
 */
static int __cxmalloc_line__validate_inactive_BCS( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead ) {
  int ret = 0;
  cxmalloc_linechunk_t *linedata = block_CS->linedata;
  cxmalloc_datashape_t *shape = &block_CS->parent->shape;
  uint32_t chunks_per_line = shape->linemem.chunks;
  size_t bytes_per_line = chunks_per_line * sizeof( cxmalloc_linechunk_t );
  size_t lines_per_block = shape->blockmem.quant;
  size_t chunks_per_block = chunks_per_line * lines_per_block;
  cxmalloc_linechunk_t *end_data = linedata + chunks_per_block;

  XTRY {
    // Verify line exists
    if( linehead == NULL ) {
      CXMALLOC_CRITICAL( 0x921, "Inactive line is NULL" );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x922 );
    }
    // Verify line belongs in this block
    if( (uintptr_t)linehead < (uintptr_t)linedata || (uintptr_t)linehead >= (uintptr_t)end_data ) {
      CXMALLOC_CRITICAL( 0x923, "Inactive line @0x%016llx does not belong in this block [0x%016llx - 0x%016llx]", linehead, linedata, end_data );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x924 );
    }
    // Verify line is aligned correctly in linedata block
    if( ((uintptr_t)linehead - (uintptr_t)linedata) % bytes_per_line != 0 ) {
      CXMALLOC_CRITICAL( 0x925, "Inactive line @0x%016llx has bad alignment", linehead );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x926 );
    }
    //  Verify refcnt of inactive line is zero
    if( linehead->data.refc != 0 ) {
      CXMALLOC_CRITICAL( 0x927, "Inactive line @0x%016llx (bidx=%u, offset=%lu) has nonzero refcnt: %ld", block_CS->bidx, linehead->data.offset, linehead->data.refc );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x928 );
    }
    // Verify not active
    if( linehead->data.flags._act ) {
      CXMALLOC_CRITICAL( 0x929, "Inactive line @0x%016llx (bidx=%u, offset=%lu) marked as active", block_CS->bidx, linehead->data.offset );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x92A );
    }
    // Mark as checked
    linehead->data.flags._chk = 1;
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }
  return ret;
}




/*******************************************************************//**
 * 
 * NOTE: Call this method only if there is no chance another thread
 * will operate against this family instance.
 ***********************************************************************
 */
static int64_t __cxmalloc_line__validate_refcount_BCS( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead ) {
  int64_t refcnt = 0;
  XTRY {
    refcnt = linehead->data.refc;
    // Verify refcount is not negative
    if( refcnt < 0 ) {
      CXMALLOC_CRITICAL( 0x931, "line @0x%016llx (bidx=%u) has negative refcount: %ld", linehead, block_CS->bidx, linehead->data.refc );
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x932 );
    }
    // Add refcnt to total and count the active line
    else if( refcnt > 0 ) {
      if( linehead->data.flags._act == 0 ) {
        CXMALLOC_CRITICAL( 0x933, "line @0x%016llx (bidx=%u) has positive refcnt %ld but marked as inactive", linehead, block_CS->bidx, linehead->data.refc );
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x934 );
      }
      if( linehead->data.flags._chk == 1 ) {
        CXMALLOC_CRITICAL( 0x935, "line @0x%016llx (bidx=%u) is managed by registry (unallocated) but has positive refcnt %ld", linehead, block_CS->bidx, linehead->data.refc );
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x936 );
      }
    }
    // Count the inactive line
    else {
      if( linehead->data.flags._act == 1 ) {
        CXMALLOC_CRITICAL( 0x937, "line @0x%016llx (bidx=%u) has zero refcnt but marked as active", linehead, block_CS->bidx );
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x938 );
      }
      if( linehead->data.flags._chk == 0 ) {
        CXMALLOC_CRITICAL( 0x939, "line @0x%016llx (bidx=%u) is detached from registry (allocated) but has zero refcnt", linehead, block_CS->bidx );
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x93A );
      }
    }
  }
  XCATCH( errcode ) {
    refcnt = -1;
  }
  XFINALLY {
  }
  return refcnt;
}
