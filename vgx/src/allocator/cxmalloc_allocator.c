/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_allocator.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );



static cxmalloc_allocator_t * __cxmalloc_allocator__create_allocator_FCS( cxmalloc_family_t *family_CS, const uint16_t aidx );
static                   void __cxmalloc_allocator__destroy_allocator_FCS( cxmalloc_family_t *family_CS, cxmalloc_allocator_t **ppallocator );
static  cxmalloc_linehead_t * __cxmalloc_allocator__new_ACS( cxmalloc_allocator_t *allocator_CS );
static     cxmalloc_block_t * __cxmalloc_allocator__delete_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t *linehead );
static                int64_t __cxmalloc_allocator__restore_objects_FCS_ACS( cxmalloc_allocator_t *allocator_CS );
static                int64_t __cxmalloc_allocator__validate_refcounts_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t **bad_linehead );
static      comlib_object_t * __cxmalloc_allocator__get_object_at_address_ACS( cxmalloc_allocator_t *allocator_CS, QWORD address );
static      comlib_object_t * __cxmalloc_allocator__find_object_by_obid_ACS( cxmalloc_allocator_t *allocator_CS, objectid_t obid );
static      comlib_object_t * __cxmalloc_allocator__get_object_ACS( cxmalloc_allocator_t *allocator_CS, int64_t n );
static                int64_t __cxmalloc_allocator__sweep_FCS_ACS( cxmalloc_allocator_t *allocator, f_get_object_identifier get_object_identifier );




DLL_HIDDEN _icxmalloc_allocator_t _icxmalloc_allocator = {
  .CreateAllocator_FCS    = __cxmalloc_allocator__create_allocator_FCS,
  .DestroyAllocator_FCS   = __cxmalloc_allocator__destroy_allocator_FCS,
  .New_ACS                = __cxmalloc_allocator__new_ACS,
  .Delete_ACS             = __cxmalloc_allocator__delete_ACS,
  .RestoreObjects_FCS_ACS = __cxmalloc_allocator__restore_objects_FCS_ACS,
  .ValidateRefcounts_ACS  = __cxmalloc_allocator__validate_refcounts_ACS,
  .GetObjectAtAddress_ACS = __cxmalloc_allocator__get_object_at_address_ACS,
  .FindObjectByObid_ACS   = __cxmalloc_allocator__find_object_by_obid_ACS,
  .GetObject_ACS          = __cxmalloc_allocator__get_object_ACS,
  .Sweep_FCS_ACS          = __cxmalloc_allocator__sweep_FCS_ACS
};




/*******************************************************************//**
 * 
 * NOTE: caller owns returned memory! 
 ***********************************************************************
 */
static CString_t * __get_allocator_path( const CString_t *CSTR__family_path ) {
  CString_t *CSTR__allocator_path = NULL;

  XTRY {
    const char *fampath = CStringValue( CSTR__family_path );
    if( (CSTR__allocator_path = CStringNewFormat( "%s", fampath )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x501 );
    }

  }
  XCATCH( errcode ) {
    if( CSTR__allocator_path ) {
      CStringDelete( CSTR__allocator_path );
      CSTR__allocator_path = NULL;
    }
  }
  XFINALLY {
  }

  return CSTR__allocator_path;
}



/*******************************************************************//**
 * Create a new allocator 
 * family         : the allocator family
 * aidx           : allocator's index/ID
 * 
 * Returns  : pointer to fully initialized allocator, or NULL on failure
 ***********************************************************************
 */
static cxmalloc_allocator_t * __cxmalloc_allocator__create_allocator_FCS( cxmalloc_family_t *family_CS, const uint16_t aidx ) {
  cxmalloc_allocator_t *allocator = NULL;
  const CString_t *CSTR__allocdir = NULL;

  XTRY {

    // ------------------------------------------------------------
    // 1. Get the allocator path string if we're in persistent mode
    // ------------------------------------------------------------
    if( family_CS->descriptor->persist.CSTR__path ) {
      if( (CSTR__allocdir = __get_allocator_path( family_CS->descriptor->persist.CSTR__path )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x511 );
      }
    }

    // ------------------------------------
    // 2. Allocate allocator and initialize
    // ------------------------------------

    // Allocate the allocator container 
    TALIGNED_MALLOC_THROWS( allocator, cxmalloc_allocator_t, 0x512 );
    memset( allocator, 0, sizeof( cxmalloc_allocator_t ) );
    
    // [1] alock
    INIT_SPINNING_CRITICAL_SECTION( &allocator->alock.lock, 4000 );

    // [2] blocks
    PALIGNED_INITIALIZED_ARRAY_THROWS( allocator->blocks, cxmalloc_block_t*, CXMALLOC_BLOCK_REGISTER_SIZE, NULL, 0x513 );

    // [3] head
    allocator->head = allocator->blocks;
    
    // [4] last_reuse
    allocator->last_reuse = NULL;
    
    // [5] last_hole
    allocator->last_hole = NULL;
    
    // [6] space
    allocator->space = allocator->blocks;
    
    // [7] end
    allocator->end = allocator->blocks + CXMALLOC_BLOCK_REGISTER_SIZE;
    
    // [8] family
    allocator->family = family_CS;
    
    // [9] aidx
    allocator->aidx = aidx;

    // [10] readonly
    allocator->readonly_atomic = 0;

    // [12] CSTR__allocdir
    allocator->CSTR__allocdir = CSTR__allocdir; // will be non-NULL if we're in persistent mode

    // [13] n_active
    allocator->n_active = 0;
    
    // [14] shape
    if( _icxmalloc_shape.ComputeShape_FRO( family_CS, aidx, &allocator->shape ) == NULL ) {
      THROW_CRITICAL( CXLIB_ERR_CONFIG, 0x514 );
    }

    // -----------
    // 3.
    // -----------
    int64_t n = 0;
    SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
      n = _icxmalloc_serialization.RestoreAllocator_ACS( allocator_CS );
    } RELEASE_CXMALLOC_ALLOCATOR;
    if( n < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x515 );
    }

#ifndef NDEBUG 
    DEBUG( 0x516, "New allocator in family \"%s (%lu)\": aidx=%u, width=%lu, quant=%lu", _cxmalloc_id_string(family_CS), family_CS->descriptor->unit.sz, aidx, allocator->shape.linemem.awidth, allocator->shape.blockmem.quant );
#endif

  }
  XCATCH( errcode ) {
    __cxmalloc_allocator__destroy_allocator_FCS( family_CS, &allocator );

    if( cxlib_exc_subtype( errcode ) == CXLIB_ERR_MEMORY ) {
      CXMALLOC_CRITICAL( errcode, "OUT OF MEMORY! Allocator %u in family \"%s\" could not be created.", aidx, _cxmalloc_id_string(family_CS) );
    }
  }
  XFINALLY {
    if( allocator ) {
      // [11] ready
      allocator->ready = 1;
    }
  }

  return allocator;
}



/*******************************************************************//**
 * Delete allocator 
 * family           : the allocator family
 * allocator        : the allocator
 * 
 ***********************************************************************
 */
static void __cxmalloc_allocator__destroy_allocator_FCS( cxmalloc_family_t *family_CS, cxmalloc_allocator_t **ppallocator ) {

  if( ppallocator && *ppallocator ) {

    cxmalloc_allocator_t *allocator = *ppallocator;
    SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {

      // Remove allocator from family
      family_CS->allocators[ allocator_CS->aidx ] = NULL;

      // [11] ready
      allocator_CS->ready = 0;

      // [13] shape
      memset( &allocator_CS->shape, 0, sizeof( cxmalloc_datashape_t) );

      // [12] CSTR__allocdir
      if( allocator_CS->CSTR__allocdir ) {
        CStringDelete( allocator_CS->CSTR__allocdir );
        allocator_CS->CSTR__allocdir = NULL;
      }
      
      // [10] readonly
      allocator_CS->readonly_atomic = 0;

      // [9] aidx
      allocator_CS->aidx = 0;

      // [7] end
      allocator_CS->end = NULL;
      
      // [6] space
      allocator_CS->space = NULL;
      
      // [5] last_hole
      allocator_CS->last_hole = NULL;

      // [4] last_reuse
      allocator_CS->last_reuse = NULL;
      
      // [3] head
      allocator_CS->head = NULL;
      
      // [2] blocks
      if( allocator_CS->blocks ) {
        cxmalloc_block_t **cur_CS = allocator_CS->blocks;
        cxmalloc_block_t **end_CS = allocator_CS->blocks + CXMALLOC_BLOCK_REGISTER_SIZE;
        cxmalloc_block_t *block_CS;
        while( cur_CS < end_CS ) {
          if( (block_CS = *cur_CS++) != NULL ) {
            _icxmalloc_block.Delete_ACS( allocator_CS, block_CS );
          }
        }
        ALIGNED_FREE( allocator_CS->blocks );
        allocator_CS->blocks = NULL;
      }
      
      // [8] family
      allocator_CS->family = NULL;
    } RELEASE_CXMALLOC_ALLOCATOR;
    
    // [1] alock
    DEL_CRITICAL_SECTION( &allocator->alock.lock );

    // Deallocate allocator container
    ALIGNED_FREE( *ppallocator );
    *ppallocator = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_linehead_t * __allocator_corruption_new_is_active( cxmalloc_block_t *block, cxmalloc_linehead_t *linehead ) {
  uint32_t tid = GET_CURRENT_THREAD_ID();
  CXMALLOC_CRITICAL( 0xFFF, "---------------------------------------------------------------" );
  CXMALLOC_CRITICAL( 0xFFF, "Allocator Corruption: Line is already active %p (thread=%u)", linehead, tid );
  if( linehead ) {
    CXMALLOC_CRITICAL( 0xFFF, "    block.available  : %lld", block->available );
    CXMALLOC_CRITICAL( 0xFFF, "    block.capacity   : %lld", block->capacity );
    CXMALLOC_CRITICAL( 0xFFF, "    block.prev.bidx  : %d", block->prev_block ? (int)block->prev_block->bidx : -1 );
    CXMALLOC_CRITICAL( 0xFFF, "    block.next.bidx  : %d", block->next_block ? (int)block->next_block->bidx : -1 );
    CXMALLOC_CRITICAL( 0xFFF, "    linehead.aidx    : %u", linehead->data.aidx );
    CXMALLOC_CRITICAL( 0xFFF, "    linehead.bidx    : %u", linehead->data.bidx );
    CXMALLOC_CRITICAL( 0xFFF, "    linehead.offset  : %u", linehead->data.offset );
    CXMALLOC_CRITICAL( 0xFFF, "    linehead.refc    : %d", linehead->data.refc );
    CXMALLOC_CRITICAL( 0xFFF, "    linehead.flags   : %02x", linehead->data.flags.bits );
    cxmalloc_datashape_t *shape = &block->parent->shape;
    size_t nqwords = shape->linemem.chunks * qwsizeof( cxmalloc_linechunk_t );
    char *buffer = calloc( nqwords, 18 );
    if( buffer ) {
      char *wp = buffer;
      QWORD *qw = (QWORD*)linehead;
      for( size_t i=0; i<nqwords; i++ ) {
        wp += sprintf( wp, "%016llx ", *qw++ );
      }
      *(wp-1) = '\0';
      CXMALLOC_CRITICAL( 0xFFF, "    line.data        : [%s]", buffer );
      free( buffer );
    }
  }
  CXMALLOC_CRITICAL( 0xFFF, "---------------------------------------------------------------" );

  cxlib_print_backtrace( 0 );

  return NULL;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_linehead_t * __cxmalloc_allocator__new_ACS( cxmalloc_allocator_t *allocator_CS ) {
  cxmalloc_linehead_t *linehead_CS;
  cxmalloc_block_t *block_CS;
  
  // 1: Extend capacity if needed
  if( (block_CS = *allocator_CS->head) == NULL || block_CS->reg.get == NULL ) {
    if( (block_CS = _icxmalloc_chain.AdvanceBlock_ACS( allocator_CS )) == NULL ) {
      return NULL; // error
    }
  }
  
  // TODO: prefetch line-1 ?
  // 2: Grab next available line and mark as checked out
  linehead_CS = *block_CS->reg.get;
  if( linehead_CS->data.flags._act == 1 ) {
    return __allocator_corruption_new_is_active( block_CS, linehead_CS );
  }

  // 3: Establish start point for freeing if not set
  if( block_CS->reg.put == NULL ) {
    block_CS->reg.put = block_CS->reg.get;
  }
  
  // 4: Advance get pointer and wrap
  if( ++(block_CS->reg.get) == block_CS->reg.bottom ) {
    block_CS->reg.get = block_CS->reg.top;
  }
  
  // 5: Detect if get pointer caught up with put pointer (all lines checked out) and mark as empty
  if( block_CS->reg.get == block_CS->reg.put ) {
    block_CS->reg.get = NULL; // empty
  }
  
  // 6: Decrement available
  block_CS->available--;
  
  // 7: Update flags
  linehead_CS->data.flags._act = 1;
  linehead_CS->data.flags._mod = 1;

  // 8: Set active bit
  _cxmalloc_bitvector_set( &block_CS->active, linehead_CS );

  // 9: Increment allocator active counter
  allocator_CS->n_active++;

  return linehead_CS;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_allocator__delete_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t *linehead ) {
  // Grab the block
  cxmalloc_block_t *block_CS = allocator_CS->blocks[ linehead->data.bidx ];

  // Decrement allocator active counter
  allocator_CS->n_active--;

  // Increment block available
  block_CS->available++;

  // Update flags
  linehead->data.flags._act = 0;
  linehead->data.flags._mod = 1;

  // Clear active bit
  _cxmalloc_bitvector_clear( &block_CS->active, linehead );

#ifdef CXMALLOC_INVALIDATE_ON_DELETE
  // Invalidate data
  {
    cxmalloc_metaflex_t *metaflex = (cxmalloc_metaflex_t*)linehead;
    void *line = _cxmalloc_array_from_linehead( allocator_CS->family, linehead );
    void *obj = _cxmalloc_object_from_array( allocator_CS->family, line );
    GET_CURRENT_THREAD_LABEL( (char*)linehead );
    metaflex->M2._i32_1 = GET_CURRENT_THREAD_ID();
    memset( obj, 0x00, allocator_CS->family->object_bytes );
    memset( line, 0xcc, (size_t)allocator_CS->shape.linemem.awidth * allocator_CS->shape.linemem.unit_sz );
  }
#endif

  // [optimization] If linehead to be deleted was most recently allocated linehead,
  // then just back the get pointer back one slot.
  if( block_CS->reg.get > block_CS->reg.top && *(block_CS->reg.get-1) == linehead ) {
    --(block_CS->reg.get);
  }
  else {
    // Establish start point for getting if not set
    if( block_CS->reg.get == NULL ) {
      block_CS->reg.get = block_CS->reg.put;
    }

    // Put freed line back in register
    *block_CS->reg.put = linehead;

    // Advance put slot and wrap if needed
    if( ++(block_CS->reg.put) == block_CS->reg.bottom ) {
      block_CS->reg.put = block_CS->reg.top;
    }
  }

  // The putter caught up with the getter, all lines have been freed
  if( block_CS->reg.put == block_CS->reg.get ) {
     block_CS->reg.put = NULL;
  }
  

#ifdef CXMALLOC_CONSISTENCY_CHECK
  if( linehead->data.refc == 0 ) {
    if( _icxmalloc_line.ValidateInactive_BCS( block_CS, linehead ) < 0 ) {
    }
  }
  if( _icxmalloc_line.ValidateRefcount_BCS( block_CS, linehead ) < 0 ) {
  }
  linehead->data.flags._chk = 0;
#endif
  return block_CS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_allocator__restore_objects_FCS_ACS( cxmalloc_allocator_t *allocator_CS ) {
  int64_t nqwords = 0;
  int64_t n;

  XTRY {
    if( allocator_CS->CSTR__allocdir ) {
      const char *allocdir = CStringValue( allocator_CS->CSTR__allocdir );
      CXMALLOC_VERBOSE( 0x521, "Restoring: %s", allocdir );

      cxmalloc_block_t **cursor_CS = allocator_CS->blocks;
      while( cursor_CS < allocator_CS->space ) {
        cxmalloc_block_t *block_CS = *cursor_CS++;
        if( (n = _icxmalloc_block.RestoreObjects_FCS_ACS( block_CS )) < 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x522, "Failed to restore allocator: '%s'", allocdir );
        }
        nqwords += n;
        int64_t n_active = block_CS->capacity - block_CS->available;
        allocator_CS->n_active += n_active;
      }

      if( nqwords > 0 ) {
        CXMALLOC_INFO( 0x522, "Restored: %s (%lld bytes)", allocdir, nqwords * sizeof(QWORD) );
      }
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
  }

  return nqwords;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_allocator__validate_refcounts_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t **bad_linehead ) {
  int64_t total_refcnt = 0;
  cxmalloc_block_t **cursor = NULL;
  cxmalloc_block_t **space = allocator_CS->space;
  cxmalloc_block_t **end = allocator_CS->end;

  // Validate all blocks
  cursor = allocator_CS->blocks;
  while( cursor < space ) {
    cxmalloc_block_t *block = *cursor++;
    int64_t block_refcnt = _icxmalloc_block.ValidateRefcounts_ACS( block, bad_linehead );
    if( block_refcnt < 0 ) {
      CXMALLOC_CRITICAL( 0x531, "Bad block in allocator aidx=%u", allocator_CS->aidx );
      return -1;
    }
    total_refcnt += block_refcnt;
  }

  // Ensure space is empty
  cursor = space;
  while( cursor < end ) {
    if( *cursor++ != NULL ) {
      CXMALLOC_CRITICAL( 0x532, "Lost block exists in space of allocator (aidx=%u)", allocator_CS->aidx );
      return -1;
    }
  }

  // OK!
  return total_refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_allocator__get_object_at_address_ACS( cxmalloc_allocator_t *allocator_CS, QWORD address ) {
  comlib_object_t *obj = NULL;
  cxmalloc_block_t **cursor_CS = NULL;
  cxmalloc_block_t **space = allocator_CS->space;
  cursor_CS = allocator_CS->blocks;
  while( cursor_CS < space ) {
    cxmalloc_block_t *block_CS = *cursor_CS++;
    if( (obj = _icxmalloc_block.GetObjectAtAddress_ACS( block_CS, address )) != NULL ) {
      break;
    }
  }
  return obj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_allocator__find_object_by_obid_ACS( cxmalloc_allocator_t *allocator_CS, objectid_t obid ) {
  comlib_object_t *obj = NULL;
  cxmalloc_block_t **cursor_CS = NULL;
  cxmalloc_block_t **space = allocator_CS->space;
  cursor_CS = allocator_CS->blocks;
  while( cursor_CS < space ) {
    cxmalloc_block_t *block_CS = *cursor_CS++;
    if( (obj = _icxmalloc_block.FindObjectByObid_ACS( block_CS, obid )) != NULL ) {
      break;
    }
  }
  return obj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_allocator__get_object_ACS( cxmalloc_allocator_t *allocator_CS, int64_t n ) {
  comlib_object_t *obj = NULL;
  cxmalloc_block_t **cursor_CS = NULL;
  cxmalloc_block_t **space = allocator_CS->space;
  cursor_CS = allocator_CS->blocks;

  // Seek to the block containing the selected object number
  int64_t n_scan = 0;
  while( obj == NULL && cursor_CS < space ) {
    cxmalloc_block_t *block_CS = *cursor_CS++;
    int64_t n_last = n_scan;
    int64_t n_active = block_CS->capacity - block_CS->available;
    n_scan += n_active;
    // This block contains the selected object number
    if( n < n_scan ) {
      int64_t i = n - n_last;
      obj = _icxmalloc_block.GetObject_ACS( block_CS, i );
    }
  }
  return obj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_allocator__sweep_FCS_ACS( cxmalloc_allocator_t *allocator_CS, f_get_object_identifier get_object_identifier ) {
  int64_t n_fix = 0;
  int64_t n = 0;
  cxmalloc_block_t **cursor_CS = allocator_CS->blocks;
  cxmalloc_block_t **space_CS = allocator_CS->space;
  while( cursor_CS < space_CS ) {
    if( (n = _icxmalloc_block.Sweep_FCS_ACS( *cursor_CS++, get_object_identifier )) < 0 ) {
      n_fix = -1;
      break;
    }
    n_fix += n;
  }
  return n_fix;
}
