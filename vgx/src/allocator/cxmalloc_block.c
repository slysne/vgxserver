/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_block.c
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

static                    int __cxmalloc_block__get_filepaths_OPEN( const CString_t *CSTR__allocator_path, const uint16_t aidx, const cxmalloc_bidx_t bidx, CString_t **CSTR__path, CString_t **CSTR__base_path, CString_t **CSTR__ext_path );
static     cxmalloc_block_t * __cxmalloc_block__new_ACS( cxmalloc_allocator_t *allocator_CS, const cxmalloc_bidx_t bidx, bool allocate_data );
static                   void __cxmalloc_block__delete_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t* block_CS );
static                    int __cxmalloc_block__create_block_data_ACS( cxmalloc_block_t *block_CS );
static cxmalloc_linechunk_t * __cxmalloc_block__new_initialized_data_lines_ACS( const cxmalloc_allocator_t *allocator_CS, cxmalloc_bidx_t bidx );
static cxmalloc_bitvector_t   __cxmalloc_block__new_bitvector_ACS( const cxmalloc_allocator_t *allocator_CS );
static                    int __cxmalloc_block__initialize_line_register_ACS( const cxmalloc_datashape_t *shape, cxmalloc_block_t *block_CS );
static                   void __cxmalloc_block__destroy_block_data_ACS( cxmalloc_block_t *block_CS );
static                 size_t __cxmalloc_block__bytes_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__compute_available_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__compute_active_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__count_active_lines_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__count_refcnt_lines_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__modified_ARO( const cxmalloc_block_t *block_RO );
static                 bool   __cxmalloc_block__needs_persist_ARO( const cxmalloc_block_t *block_RO );
static                int64_t __cxmalloc_block__restore_objects_FCS_ACS( cxmalloc_block_t *block_CS );
static                int64_t __cxmalloc_block__validate_refcounts_ACS( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t **bad_linehead );
static      comlib_object_t * __cxmalloc_block__get_object_at_address_ACS( const cxmalloc_block_t *block_CS, QWORD address );
static      comlib_object_t * __cxmalloc_block__find_object_by_obid_ACS( const cxmalloc_block_t *block_CS, objectid_t obid );
static      comlib_object_t * __cxmalloc_block__get_object_ACS( const cxmalloc_block_t *block_CS, int64_t n );
static                int64_t __cxmalloc_block__sweep_FCS_ACS( cxmalloc_block_t *block_CS, f_get_object_identifier get_object_identifier );
static cxmalloc_linehead_t ** __cxmalloc_block__find_lost_lines_ACS( cxmalloc_block_t *block_CS, int force );
static       CStringQueue_t * __cxmalloc_block__repr_ARO( const cxmalloc_block_t *block_RO, CStringQueue_t *output );



DLL_HIDDEN _icxmalloc_block_t _icxmalloc_block = {
  .GetFilepaths_OPEN      = __cxmalloc_block__get_filepaths_OPEN,
  .New_ACS                = __cxmalloc_block__new_ACS,
  .Delete_ACS             = __cxmalloc_block__delete_ACS,
  .CreateData_ACS         = __cxmalloc_block__create_block_data_ACS,
  .DestroyData_ACS        = __cxmalloc_block__destroy_block_data_ACS,
  .ComputeAvailable_ARO   = __cxmalloc_block__compute_available_ARO,
  .ComputeActive_ARO      = __cxmalloc_block__compute_active_ARO,
  .CountActiveLines_ARO   = __cxmalloc_block__count_active_lines_ARO,
  .CountRefcntLines_ARO   = __cxmalloc_block__count_refcnt_lines_ARO,
  .Modified_ARO           = __cxmalloc_block__modified_ARO,
  .NeedsPersist_ARO       = __cxmalloc_block__needs_persist_ARO,
  .RestoreObjects_FCS_ACS = __cxmalloc_block__restore_objects_FCS_ACS,
  .ValidateRefcounts_ACS  = __cxmalloc_block__validate_refcounts_ACS,
  .GetObjectAtAddress_ACS = __cxmalloc_block__get_object_at_address_ACS,
  .FindObjectByObid_ACS   = __cxmalloc_block__find_object_by_obid_ACS,
  .GetObject_ACS          = __cxmalloc_block__get_object_ACS,
  .Sweep_FCS_ACS          = __cxmalloc_block__sweep_FCS_ACS,
  .FindLostLines_ACS      = __cxmalloc_block__find_lost_lines_ACS,
  .Repr_ARO               = __cxmalloc_block__repr_ARO
};



/*******************************************************************//**
 * 
 * NOTE: caller owns returned memory! 
 ***********************************************************************
 */
static int __cxmalloc_block__get_filepaths_OPEN( const CString_t *CSTR__allocator_path, const uint16_t aidx, const cxmalloc_bidx_t bidx, CString_t **CSTR__path, CString_t **CSTR__base_path, CString_t **CSTR__ext_path ) {
  const char *prefix = "bx";
  const char *base_suffix = "bas";
  const char *ext_suffix = "ext";
  int ret = 0;

  XTRY {
    const char *allocpath = CStringValue( CSTR__allocator_path );
    
    if( (*CSTR__path = CStringNewFormat( "%s", allocpath )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x401 );
    }

    const char *path = CStringValue( *CSTR__path );

    if( (*CSTR__base_path = CStringNewFormat( "%s/%s_%04x%04x.%s", path, prefix, aidx, bidx, base_suffix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x402 );
    }
    if( (*CSTR__ext_path = CStringNewFormat( "%s/%s_%04x%04x.%s", path, prefix, aidx, bidx, ext_suffix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x403 );
    }
  }
  XCATCH( errcode ) {
    if( *CSTR__path ) {
      CStringDelete( *CSTR__path );
      *CSTR__path = NULL;
    }
    if( *CSTR__base_path ) {
      CStringDelete( *CSTR__base_path );
      *CSTR__base_path = NULL;
    }
    if( *CSTR__ext_path ) {
      CStringDelete( *CSTR__ext_path );
      *CSTR__ext_path = NULL;
    }
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 * Create a new allocator block.
 *
 * allocator  : parent allocator that will own the new block
 * bidx       : the new block's unique ID within allocator
 *
 * Block data segment shape is described by allocator for which block
 * is created.
 *
 * Upon successful creation block is fully initialized and ready to use.
 *
 * Returns    : pointer to block, or NULL on failure.
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_block__new_ACS( cxmalloc_allocator_t *allocator_CS, const cxmalloc_bidx_t bidx, bool allocate_data ) {
  cxmalloc_block_t *block_CS = NULL;

  CString_t *CSTR__path = NULL;
  CString_t *CSTR__base_path = NULL;
  CString_t *CSTR__ext_path = NULL;

  XTRY {
    
    // -----------------------------------------------------------
    // 1. Compute the block file paths if we're in persisting mode
    // -----------------------------------------------------------
    if( allocator_CS->CSTR__allocdir ) {
      if( __cxmalloc_block__get_filepaths_OPEN( allocator_CS->CSTR__allocdir, allocator_CS->aidx, bidx, &CSTR__path, &CSTR__base_path, &CSTR__ext_path ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x411 );
      }
    }

    // -------------------------
    // 2. Create the block shell
    // -------------------------
    CALIGNED_MALLOC_THROWS( block_CS, cxmalloc_block_t, 0x412 );
    memset( block_CS, 0, sizeof( cxmalloc_block_t ) );

    // [1.1] reg.top
    block_CS->reg.top = NULL;

    // [1.2] reg.bottom
    block_CS->reg.bottom = NULL;

    // [1.3] reg.get
    block_CS->reg.get = NULL;

    // [1.4] reg.put
    block_CS->reg.put = NULL;

    // [2] linedata
    block_CS->linedata = NULL;

    // [3] parent
    block_CS->parent = allocator_CS;

    // [4] next_block
    block_CS->next_block = NULL;

    // [5] prev_block
    block_CS->prev_block = NULL;

    // [6] capacity
    block_CS->capacity = 0; // No capacity until linedata is allocated

    // [7] available
    block_CS->available = 0;

    // [8] min_until
    time( &block_CS->min_until );
    block_CS->min_until += CXMALLOC_MIN_BLOCK_LIFETIME_SECONDS;
    
    // [9] reuse_threshold
    block_CS->reuse_threshold = 0;
    
    // [10] defrag_threshold
    block_CS->defrag_threshold = block_CS->parent->shape.blockmem.quant / 4; // allow collaborative defrag when less than 1/4 of block is used.

    // [11] bidx
    block_CS->bidx = bidx;

    // [12] base_fileno
    block_CS->base_fileno = 0;

    // [13] ext_fileno
    block_CS->ext_fileno = 0;

    // [14] blockdir
    block_CS->CSTR__blockdir = CSTR__path; // will be non-NULL if we're in persistent mode

    // [15] base_path
    block_CS->CSTR__base_path = CSTR__base_path; // will be non-NULL if we're in persistent mode

    // [16] ext_path
    block_CS->CSTR__ext_path = CSTR__ext_path; // will be non-NULL if we're in persistent mode

    // [17] base_bulkin
    block_CS->base_bulkin = NULL;

    // [18] base_bulkout
    block_CS->base_bulkout = NULL;

    // [19] ext_bulkin
    block_CS->ext_bulkin = NULL;

    // [20] ext_bulkout
    block_CS->ext_bulkout = NULL;

    // ------------------------
    // 3. Create the block data
    // ------------------------
    if( allocate_data ) {
      if( __cxmalloc_block__create_block_data_ACS( block_CS ) < 0 ) {
        THROW_CRITICAL( CXLIB_ERR_MEMORY, 0x413 );
      }
    }

    // NOTE:
    // If there is a data file with previously persisted block data on disk
    // it will not be loaded here.
    // It is necessary to explicitly call __cxmalloc_block__restore_objects() for each
    // block to deserialize from data on disk.
    // This is because of dependencies in objects that exist in different blocks, allocators or families.
    // All allocators will have to be completely constructed (in an initialized state) before any
    // deserialization can occur in any allocator's block.

#ifndef NDEBUG 
    DEBUG( 0x414, "New block %u in allocator %ld in family: %s",  bidx, allocator_CS->aidx, _cxmalloc_id_string(allocator_CS->family) );
#endif

  }
  XCATCH( errcode ) {
    __cxmalloc_block__delete_ACS( allocator_CS, block_CS );
    if( allocator_CS ) {
      CXMALLOC_CRITICAL( errcode, "Out of memory in allocator \"%s\", aidx=%u, bidx=%u)", _cxmalloc_id_string(allocator_CS->family), allocator_CS->aidx, block_CS->bidx );
    }
    block_CS = NULL;
  }
  XFINALLY {}

  return block_CS;
}



/*******************************************************************//**
 * Delete a block
 * 
 ***********************************************************************
 */
static void __cxmalloc_block__delete_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t* block_CS ) {
  cxmalloc_block_t *next_CS;
  if( block_CS ) {
    // Delete the data lines and line register
    __cxmalloc_block__destroy_block_data_ACS( block_CS );

    // Yank block out of any chain it may be in
    next_CS = _icxmalloc_chain.RemoveBlock_ACS( allocator_CS, block_CS ); // next may be NULL
    
    // We move the head only if the head is being deleted
    if( allocator_CS->head && block_CS == *allocator_CS->head ) {
      *allocator_CS->head = next_CS;
    }
    
    // Make the block register forget this block
    allocator_CS->blocks[ block_CS->bidx ] = NULL; 

#ifndef NDEBUG 
    DEBUG( 0x421, "Delete block %u in allocator %u in family: %s",  block_CS->bidx, allocator_CS->aidx, _cxmalloc_id_string(allocator_CS->family) );
#endif


    // [20] ext_bulkout
    if( block_CS->ext_bulkout ) {
      COMLIB_OBJECT_DESTROY( block_CS->ext_bulkout );
    }
    
    // [19] ext_bulkin
    if( block_CS->ext_bulkin ) {
      COMLIB_OBJECT_DESTROY( block_CS->ext_bulkin );
    }

    // [18] base_bulkout
    if( block_CS->base_bulkout ) {
      COMLIB_OBJECT_DESTROY( block_CS->base_bulkout );
    }

    // [17] base_bulkin
    if( block_CS->base_bulkin ) {
      COMLIB_OBJECT_DESTROY( block_CS->base_bulkin );
    }

    // [16] $ext_path
    if( block_CS->CSTR__ext_path ) {
      CStringDelete( block_CS->CSTR__ext_path );
    }
    
    // [15] $base_path
    if( block_CS->CSTR__base_path ) {
      CStringDelete( block_CS->CSTR__base_path );
    }

    // [14] $blockdir
    if( block_CS->CSTR__blockdir ) {
      CStringDelete( block_CS->CSTR__blockdir );
    }

    // [13] ext_fileno
    if( block_CS->ext_fileno > 0 ) {
      CX_CLOSE( block_CS->ext_fileno );
    }

    // [12] base_fileno
    if( block_CS->base_fileno > 0 ) {
      CX_CLOSE( block_CS->base_fileno );
    }

    // free block
    ALIGNED_FREE( block_CS );
  }
}



/*******************************************************************//**
 * Create main data allocation within a block
 * 
 * block    : pointer (not NULL) to block for which data will be created 
 *
 * Return   : 0 on success, -1 on failure
 ***********************************************************************
 */
static int __cxmalloc_block__create_block_data_ACS( cxmalloc_block_t *block_CS ) {
  int retcode = 0;
  XTRY {
    cxmalloc_allocator_t *allocator_CS = block_CS->parent;
    cxmalloc_bidx_t bidx = block_CS->bidx;

    // ----------------------------
    // 1. Destroy any previous data
    // ----------------------------
    
    __cxmalloc_block__destroy_block_data_ACS( block_CS );

    // -----------------------------------------
    // 2. Allocate and initialize the data lines
    // -----------------------------------------

    // Create new data lines
    if( (block_CS->linedata = __cxmalloc_block__new_initialized_data_lines_ACS( allocator_CS, bidx )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x421 );
    }

    // Create bitvector for active lines
    block_CS->active = __cxmalloc_block__new_bitvector_ACS( allocator_CS );
    if( block_CS->active.data == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x422 );
    }
    
    // Create initialized line register
    if( __cxmalloc_block__initialize_line_register_ACS( &allocator_CS->shape, block_CS ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x423 );
    }

    // [6] set capacity
    block_CS->capacity = allocator_CS->shape.blockmem.quant;
    
    // [7] set available
    block_CS->available = block_CS->capacity;

  }
  XCATCH( errcode ) {
    __cxmalloc_block__destroy_block_data_ACS( block_CS );
    retcode = -1;
  }
  XFINALLY {
    if( block_CS->base_bulkin ) {
      COMLIB_OBJECT_DESTROY( block_CS->base_bulkin );
      block_CS->base_bulkin = NULL;
    }
  }

  return retcode;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_linechunk_t * __cxmalloc_block__new_initialized_data_lines_ACS( const cxmalloc_allocator_t *allocator_CS, cxmalloc_bidx_t bidx ) {

  // Set up initialization values
  cxmalloc_linehead_t init_linehead = {
    .metaflex = allocator_CS->family->descriptor->meta.initval,
    .data = {
      .aidx     = allocator_CS->aidx,
      .bidx     = bidx,
      //.offset   = 0, // to be filled in
      .flags    = {0},
      .size     = allocator_CS->shape.linemem.awidth,
      .refc     = 0
    }
  };

  init_linehead.data.offset = 0; // to be filled in

  cacheline_t first_CL = {
    .m256i = {
      init_linehead._chunk,
      {0}
    }
  };
  cacheline_t empty_CL = {0};
  cxmalloc_linehead_t *linehead = (cxmalloc_linehead_t*)&first_CL.m256i[0];


  // Allocate data lines, page aligned
  cxmalloc_linechunk_t *data = NULL;
  size_t n_chunks_alloc = allocator_CS->shape.blockmem.chunks;
#ifndef NDEBUG
  // Allocate 64 additional bytes at end of block data for guard/debug check
  n_chunks_alloc += 2;
#endif
  if( PALIGNED_ARRAY( data, cxmalloc_linechunk_t, n_chunks_alloc ) != NULL ) {
    // Compute sizes
    size_t n_lines = allocator_CS->shape.blockmem.quant;
    size_t n_CL_per_line = allocator_CS->shape.linemem.chunks / (sizeof(cacheline_t) / sizeof(cxmalloc_linechunk_t));
    size_t n_empty_CL_per_line = n_CL_per_line - 1;
    cacheline_t *data_CL = (cacheline_t*)data;

    #if defined CXPLAT_ARCH_X64
    // Initialize data lines in a cache-friendly way
    for( size_t n=0; n < n_lines; n++ ) {
      // Write first cacheline (includes the initialized header)
      stream_memset( data_CL, &first_CL, 1 );
      data_CL++;
      // Write rest of empty cachelines
      stream_memset( data_CL, &empty_CL, n_empty_CL_per_line );
      data_CL += n_empty_CL_per_line;
      // Next
      linehead->data.offset++;
    }
    #else
    // Initialize data lines
    for( size_t n=0; n < n_lines; n++ ) {
      // Write first cacheline (includes the initialized header)
      memcpy( data_CL, &first_CL, sizeof(first_CL) );
      data_CL++;
      // Zero the rest
      memset( data_CL, 0, sizeof(cacheline_t) * n_empty_CL_per_line );
      data_CL += n_empty_CL_per_line;
      // Next
      linehead->data.offset++;
    }
    #endif
#ifndef NDEBUG
    cxmalloc_linechunk_t debug_chunk1;
    memset( &debug_chunk1, 0xFF, sizeof( cxmalloc_linechunk_t ) );
    cxmalloc_linechunk_t debug_chunk2;
    strncpy( (char*)&debug_chunk2, "END OF BLOCK", 31 );
    cxmalloc_linechunk_t *eob = (cxmalloc_linechunk_t*)data_CL;
    memcpy( eob++, &debug_chunk1, sizeof( cxmalloc_linechunk_t ) );
    memcpy( eob,   &debug_chunk2, sizeof( cxmalloc_linechunk_t ) );
#endif
  }

  return data;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_bitvector_t __cxmalloc_block__new_bitvector_ACS( const cxmalloc_allocator_t *allocator_CS ) {
  // Number of lines in block
  size_t quant = allocator_CS->shape.blockmem.quant;

  cxmalloc_bitvector_t B;

  // Number of qwords needed to have a bit for each line
  B.nq = ceilmultpow2( quant, 64 ) / 64;
  
  // Data
  if( PALIGNED_ARRAY( B.data, QWORD, B.nq ) != NULL ) {
    memset( B.data, 0, B.nq * sizeof( QWORD ) );
  }

  return B;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __cxmalloc_block__initialize_line_register_ACS( const cxmalloc_datashape_t *shape, cxmalloc_block_t *block_CS ) {
  int retcode = 0;
  cxmalloc_linehead_t **line_register = NULL;

  XTRY {
    // Allocate the line register array
    TALIGNED_ARRAY_THROWS( line_register, cxmalloc_linehead_t*, shape->blockmem.quant, 0x441 );

    // [1] top      (the register)
    block_CS->reg.top = line_register;

    // [2] bottom   (one beyond last register entry)
    block_CS->reg.bottom = line_register + shape->blockmem.quant;

    // [3] get      (start at the top)
    block_CS->reg.get = block_CS->reg.top;

    // [4] put      (no lines in use)
    block_CS->reg.put = NULL;

    // Hook it up
    cxmalloc_linehead_t **cursor_CS = block_CS->reg.top;
    cxmalloc_linehead_t *line_CS = (cxmalloc_linehead_t*)block_CS->linedata;
    cxmalloc_linehead_t **end_CS = block_CS->reg.bottom;
    uint32_t stride = shape->linemem.chunks;
    while( cursor_CS < end_CS ) {
      // hook up registry entry to line in data segment
      *cursor_CS++ = line_CS;
      // advance
      line_CS += stride;
    }
  }
  XCATCH( errcode ) {
    if( line_register ) {
      ALIGNED_FREE( line_register );
      block_CS->reg.top = NULL;
      block_CS->reg.bottom = NULL;
      block_CS->reg.get = NULL;
      block_CS->reg.put = NULL;
    }
    retcode = -1;
  }
  XFINALLY {
  }

  return retcode;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__restore_objects_FCS_ACS( cxmalloc_block_t *block_CS ) {
  int64_t nqwords = 0;
  int64_t n;

  XTRY {
    if( block_CS->CSTR__blockdir != NULL ) {

#ifdef HASVERBOSE
      const char *blockdir = CStringValue( block_CS->CSTR__blockdir );
      CXMALLOC_VERBOSE( 0x431, "Restoring block %u objects: %s", block_CS->bidx, blockdir );
#endif

      // Attempt restore from disk
      if( (n = _icxmalloc_serialization.RestoreBlock_ACS( block_CS )) < 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x432, "Failed to restore block: %u", block_CS->bidx );
      }
      nqwords += n;
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
 * Destroy main data allocation within a block
 * 
 * 
 ***********************************************************************
 */
static void __cxmalloc_block__destroy_block_data_ACS( cxmalloc_block_t *block_CS ) {
  if( block_CS ) {
    // Delete the main data block
    if( block_CS->linedata ) {
      // [2]
      ALIGNED_FREE( block_CS->linedata );
      block_CS->linedata = NULL;
    }
    // Delete the active bitvector
    if( block_CS->active.data ) {
      ALIGNED_FREE( block_CS->active.data );
      block_CS->active.data = NULL;
    }
    // Delete the line register
    if( block_CS->reg.top ) {
      // [1.1]
      ALIGNED_FREE( block_CS->reg.top );
      block_CS->reg.top = NULL;
      // [1.2]
      block_CS->reg.bottom = NULL;
      // [1.3]
      block_CS->reg.get = NULL;
      // [1.4]
      block_CS->reg.put = NULL;
    }
    // Update capacity
    // [6]
    block_CS->capacity = 0;
    // [7]
    block_CS->available = 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static size_t __cxmalloc_block__bytes_ARO( const cxmalloc_block_t *block_RO ) {
  size_t bytes = 0;
  // the block container
  bytes += sizeof(cxmalloc_block_t);
  // [1] lineregister
  if( block_RO->reg.top ) {
    bytes += sizeof(cxmalloc_linehead_t*) * (block_RO->reg.bottom - block_RO->reg.top);
  }
  // [2] linedata
  if( block_RO->linedata ) {
    bytes += (size_t)block_RO->parent->shape.linemem.chunks * (size_t)block_RO->parent->shape.blockmem.quant * sizeof(cxmalloc_linechunk_t);
  }
  return bytes;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__compute_available_ARO( const cxmalloc_block_t *block_RO ) {
  // no space to put, so it must have all lines available
  // this is true even if linedata is not yet allocated (or we're a hole)
  if( block_RO->reg.put == NULL ) {
    return block_RO->parent->shape.blockmem.quant;
  }
  // no lines to get, so they must all be checked out
  if( block_RO->reg.get == NULL ) {
    return 0;
  }
  // checked out lines are those between the put and get pointers
  if( block_RO->reg.get > block_RO->reg.put ) {
    return block_RO->parent->shape.blockmem.quant - (block_RO->reg.get - block_RO->reg.put);
  }
  // available lines are those between the get and put pointers
  if( block_RO->reg.get < block_RO->reg.put ) {
    return block_RO->reg.put - block_RO->reg.get;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__compute_active_ARO( const cxmalloc_block_t *block_RO ) {
  if( block_RO->reg.top ) {
    return (size_t)(block_RO->reg.bottom - block_RO->reg.top) - __cxmalloc_block__compute_available_ARO( block_RO );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__count_active_lines_ARO( const cxmalloc_block_t *block_RO ) {
  size_t count = 0;
  size_t stride = block_RO->parent->shape.linemem.chunks;
  cxmalloc_linehead_t *cursor = (cxmalloc_linehead_t*)block_RO->linedata;
  cxmalloc_linehead_t *end = cursor + stride * block_RO->parent->shape.blockmem.quant;
  while( cursor < end ) {
    if( cursor->data.flags._act ) {
      ++count;
    }
    cursor += stride;
  }
  return count;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__count_refcnt_lines_ARO( const cxmalloc_block_t *block_RO ) {
  size_t count = 0;
  size_t stride = block_RO->parent->shape.linemem.chunks;
  cxmalloc_linehead_t *cursor = (cxmalloc_linehead_t*)block_RO->linedata;
  cxmalloc_linehead_t *end = cursor + stride * block_RO->parent->shape.blockmem.quant;
  while( cursor < end ) {
    if( cursor->data.refc > 0 ) {
      ++count;
    }
    cursor += stride;
  }
  return count;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__modified_ARO( const cxmalloc_block_t *block_RO ) {
  size_t n_mod = 0;
  cxmalloc_linechunk_t *linedata = block_RO->linedata;
  if( linedata != NULL ) {
    cxmalloc_datashape_t *shape = &block_RO->parent->shape;
    uint32_t chunks_per_line = shape->linemem.chunks;
    size_t lines_per_block = shape->blockmem.quant;
    size_t chunks_per_block = chunks_per_line * lines_per_block;
    cxmalloc_linechunk_t *end_data = linedata + chunks_per_block;
    cxmalloc_linechunk_t *chunk_cursor = linedata;
    cxmalloc_linehead_t current = {0};
    // TODO: Find a way to detect block modification without having to scan everything
    while( chunk_cursor < end_data ) {
      #if defined CXPLAT_ARCH_X64
      // Use a vetor register so we can avoid cache pollution via stream load
      current.data.m128i = _mm_stream_load_si128( (__m128i*)chunk_cursor + 1 );
      n_mod += current.data.flags._mod;
      #elif defined CXPLAT_ARCH_ARM64
      // Neon has no stream load
      n_mod += ((cxmalloc_linehead_t*)chunk_cursor)->data.flags._mod;
      #else
      #error "Unsupported architecture"
      #endif
      chunk_cursor += chunks_per_line;
    }
  }
  return n_mod;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static bool __cxmalloc_block__needs_persist_ARO( const cxmalloc_block_t *block_RO ) {
#ifdef CXMALLOC_DISABLE_INCREMENTAL_PERSISTENCE
  return true;
#else
  cxmalloc_linechunk_t *linedata = block_RO->linedata;
  if( linedata != NULL ) {
    cxmalloc_datashape_t *shape = &block_RO->parent->shape;
    uint32_t chunks_per_line = shape->linemem.chunks;
    size_t lines_per_block = shape->blockmem.quant;
    size_t chunks_per_block = chunks_per_line * lines_per_block;
    cxmalloc_linechunk_t *end_data = linedata + chunks_per_block;
    cxmalloc_linechunk_t *chunk_cursor = linedata;
    cxmalloc_linehead_t current = {0};
    // TODO: Find a way to detect block modification without having to scan everything
    while( chunk_cursor < end_data ) {
      #if defined CXPLAT_ARCH_X64
      // Use a vetor register so we can avoid cache pollution via stream load
      current.data.m128i = _mm_stream_load_si128( (__m128i*)chunk_cursor + 1 );
      if( current.data.flags._mod ) {
        return true;
      }
      #elif defined CXPLAT_ARCH_ARM64
      // Neon has no stream load
      if( ((cxmalloc_linehead_t*)chunk_cursor)->data.flags._mod ) {
        return true;
      }
      #else
      #error "Unsupported architecture"
      #endif
      chunk_cursor += chunks_per_line;
    }
  }
  return false;
#endif
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__validate_refcounts_ACS( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t **bad_linehead ) {
  int64_t total_refcnt = 0;

  cxmalloc_linechunk_t *linedata = block_CS->linedata;
  if( linedata == NULL ) {
    // Hole block
    return 0;
  }

  cxmalloc_datashape_t *shape = &block_CS->parent->shape;
  uint32_t chunks_per_line = shape->linemem.chunks;
  size_t lines_per_block = shape->blockmem.quant;
  size_t chunks_per_block = chunks_per_line * lines_per_block;
  cxmalloc_linechunk_t *end_data = linedata + chunks_per_block;

  cxmalloc_linehead_t *linehead = NULL;
  cxmalloc_linehead_t **cursor = NULL;
  cxmalloc_linehead_t **top = block_CS->reg.top;
  cxmalloc_linehead_t **end = block_CS->reg.bottom;
  cxmalloc_linehead_t **put = block_CS->reg.put;
  cxmalloc_linehead_t **get = block_CS->reg.get;

  size_t inactive_count = 0;
  size_t active_count = 0;

  XTRY {

    // Validate that inactive lines have refcnt == 0
    if( get ) {
      cursor = get;
      while( cursor != put ) {
        linehead = *cursor++;

        // Check line
        if( _icxmalloc_line.ValidateInactive_BCS( block_CS, linehead ) < 0 ) {
          *bad_linehead = linehead;
          THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x441 );
        }

        // Count
        inactive_count++;
        // Wrap
        if( cursor == end ) {
          cursor = top;
        }
        // All have been scanned (and put=NULL, i.e. ALL lines are inactive)
        if( cursor == get ) {
          break;
        }
      }
    }

    // Count active lines in registry
    if( put ) {
      cursor = put;
      while( cursor != get ) {
        // Count
        active_count++;
        // Next
        ++cursor;
        // Wrap
        if( cursor == end ) {
          cursor = top;
        }
        // All have been scanned (and get=NULL, i.e. ALL lines are active)
        if( cursor == put ) {
          break;
        }
      }
    }

    // Verify inactive and active counts add up to block capacity
    if( active_count + inactive_count != lines_per_block ) {
      CXMALLOC_CRITICAL( 0x442, "Block registry (bidx=%u) corrupted, incorrect counts: active:%llu + inactive:%llu != capacity:%llu", block_CS->bidx, active_count, inactive_count, lines_per_block );
      *bad_linehead = NULL;
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x443 );
    }

    // Scan data block and verify
    size_t zero_refcnt_lines = 0;
    size_t positive_refcnt_lines = 0;
    cxmalloc_linechunk_t *chunk_cursor = linedata;
    int64_t refcnt;
    while( chunk_cursor < end_data ) {
      linehead = (cxmalloc_linehead_t*)chunk_cursor;

      if( (refcnt = _icxmalloc_line.ValidateRefcount_BCS( block_CS, linehead )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x444 );
      }
      else if( refcnt == 0 ) {
        zero_refcnt_lines++;
      }
      else {
        positive_refcnt_lines++;
        total_refcnt += refcnt;
      }

      chunk_cursor += chunks_per_line;
    }

    // Re-check active counts
    if( active_count != positive_refcnt_lines || inactive_count != zero_refcnt_lines ) {
      CXMALLOC_CRITICAL( 0x445, "Block data (bidx=%u) corrupted, incorrect counts: active:%llu/%llu inactive:%llu/%llu", block_CS->bidx, active_count, positive_refcnt_lines, inactive_count, zero_refcnt_lines );
      *bad_linehead = NULL;
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x446 );
    }

    // Validate refcount total is at least number of active lines
    if( (size_t)total_refcnt < active_count ) {
      CXMALLOC_CRITICAL( 0x447, "Total refcount %lld < active count %llu", total_refcnt, active_count );
      *bad_linehead = NULL;
      THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x448 );
    }
  }
  XCATCH( errcode ) {
    total_refcnt = -1;
  }
  XFINALLY {
    // Clear the check flags
    cxmalloc_linechunk_t *chunk_cursor = linedata;
    while( chunk_cursor < end_data ) {
      linehead = (cxmalloc_linehead_t*)chunk_cursor;
      linehead->data.flags._chk = 0;
      chunk_cursor += chunks_per_line;
    }
  }

  // OK!
  return total_refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __print_object_location( const cxmalloc_block_t *block, comlib_object_t *obj ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );
  int64_t reg_pos = -1;
  char buf[33];
  printf( "object '%s' at %p in allocator %s (aidx=%u bidx=%u offset=%u refc=%d _act=%d ", idtostr( buf, obj->vtable->vm_getid( obj ) ), obj, block->parent->family->obid.longstring.string, linehead->data.aidx, linehead->data.bidx, linehead->data.offset, linehead->data.refc, linehead->data.flags._act );
  cxmalloc_linehead_t **reg_cursor = block->reg.top;
  cxmalloc_linehead_t *reg_entry;
  while( reg_cursor < block->reg.bottom ) {
    reg_entry = *reg_cursor++;
    if( reg_entry == linehead ) {
      break;
    }
  }
  if( reg_pos < 0 ) {
    printf( "reg_pos=<not_in_registry>)\n" );
  }
  else {
    printf( "reg_pos=%lld)\n", reg_pos );
  }
  if( (linehead->data.refc > 0 || linehead->data.flags._act) && reg_pos >= 0 ) {
    printf( "ERROR! Active line in allocator register (line free but still in use)\n" );
  }
  if( (linehead->data.refc == 0 || linehead->data.flags._act == 0) && reg_pos < 0 ) {
    printf( "ERROR! Inactive line not in allocator register (line leaked)\n" );
  }
  if( (linehead->data.refc > 0 && linehead->data.flags._act == 0) || (linehead->data.refc == 0 && linehead->data.flags._act) ) {
    printf( "ERROR! Line active flag and refcnt disagree\n" );
  }

  return 0;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_block__get_object_at_address_ACS( const cxmalloc_block_t *block_CS, QWORD address ) {
  void *obj = NULL;
  cxmalloc_linechunk_t *linedata_CS = block_CS->linedata;
  if( linedata_CS != NULL && address >= (QWORD)linedata_CS ) {
    cxmalloc_datashape_t *shape = &block_CS->parent->shape;
    uint32_t chunks_per_line = shape->linemem.chunks;
    size_t lines_per_block = shape->blockmem.quant;
    size_t chunks_per_block = chunks_per_line * lines_per_block;
    cxmalloc_linechunk_t *end_data = linedata_CS + chunks_per_block;
    QWORD max_addr = (QWORD)end_data - sizeof( comlib_object_t );
    if( address <= max_addr ) {
      if( address % sizeof( comlib_object_t ) == 0 ) {
        comlib_object_t *o = COMLIB_OBJECT( address );
        if( o->vtable ) {
          comlib_object_typeslot_t *ts = COMLIB_GetClassTypeSlot( o->typeinfo.tp_class );
          if( ts && ts->vtable == o->vtable ) {
            // Reasonably sure this is an object (but not 100%)
            obj = o;
          }
        }
      }
    }
  }
  return (comlib_object_t*)obj;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_block__find_object_by_obid_ACS( const cxmalloc_block_t *block_CS, objectid_t obid ) {
  comlib_object_t *obj = NULL;
  cxmalloc_linechunk_t *linedata_CS = block_CS->linedata;
  if( linedata_CS != NULL ) {
    cxmalloc_linechunk_t *cursor_CS = linedata_CS;
    // find end of data
    cxmalloc_datashape_t *shape = &block_CS->parent->shape;
    uint32_t chunks_per_line = shape->linemem.chunks;
    size_t lines_per_block = shape->blockmem.quant;
    size_t chunks_per_block = chunks_per_line * lines_per_block;
    cxmalloc_linechunk_t *end_data = linedata_CS + chunks_per_block;
    // scan all data
    while( cursor_CS < end_data ) {
      cxmalloc_linehead_t *linehead_CS = (cxmalloc_linehead_t*)cursor_CS;
      comlib_object_t *candidate = __cxmalloc_block__get_object_at_address_ACS( block_CS, (uintptr_t)_cxmalloc_object_from_linehead( linehead_CS ) );
      if( candidate ) {
        objectid_t *candidate_obid = candidate->vtable->vm_getid( candidate );
        // Match found
        if( candidate_obid && idmatch( candidate_obid, &obid ) ) {
          obj = candidate;
          __print_object_location( block_CS, obj );
          break;
        }
      }
      cursor_CS += chunks_per_line;
    }
  }
  return obj;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_block__get_object_ACS( const cxmalloc_block_t *block_CS, int64_t n ) {
  cxmalloc_linechunk_t *linedata_CS = block_CS->linedata;
  if( linedata_CS != NULL ) {
    // scan active objects until we find selected object number n
    int64_t a = 0; // running count of active lines
    int64_t c = 0; // running count of active lines per bitvector qword
    int64_t i = 0; // physical line number
    QWORD *bsegm = block_CS->active.data;
    QWORD *end = bsegm + block_CS->active.nq;
    // scan bitvector segments
    while( bsegm < end ) {
      // active count in segment
      size_t csegm = __popcnt64( *bsegm );
      // active count covered by bitvector so far
      c += csegm;
      // selected object n is found in current bitvector segment
      if( n < c ) {
        QWORD mask = 1ULL;
        while( mask ) {
          // active position in bitvector segment
          if( (*bsegm & mask) ) {
            // selected object found when running active count equals the selected object number (0-based)
            if( a == n ) {
              cxmalloc_linehead_t *linehead_CS = (cxmalloc_linehead_t*)linedata_CS + (i * block_CS->parent->shape.linemem.chunks );
              // sanity check
              if( linehead_CS->data.flags._act ) {
                // found
                return _cxmalloc_object_from_linehead( linehead_CS );
              }
              else {
                // will never be found
                CXMALLOC_FATAL( 0xFFF, "Bitvector error" );
              }
            }
            // increment running active count
            ++a;
          }
          // next bit in segment
          mask <<= 1;
          // next physical line
          ++i;
        }
      }
      // add active count covered by bitvector segment to the running active count
      else {
        a += csegm;
        i += 64;
      }
      // next bitvector segment
      ++bsegm;
    }
  }
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_block__sweep_FCS_ACS( cxmalloc_block_t *block_CS, f_get_object_identifier get_object_identifier ) {
  int64_t n_fix = 0;
  cxmalloc_linechunk_t *linedata = block_CS->linedata;
  cxmalloc_family_t *family_CS = block_CS->parent->family;
  const char *name = family_CS->obid.longstring.string;
  if( linedata != NULL ) {
    cxmalloc_linechunk_t *chunk_cursor = linedata;
    // find end of data
    cxmalloc_datashape_t *shape = &block_CS->parent->shape;
    uint32_t chunks_per_line = shape->linemem.chunks;
    size_t lines_per_block = shape->blockmem.quant;
    size_t chunks_per_block = chunks_per_line * lines_per_block;
    cxmalloc_linechunk_t *end_data = linedata + chunks_per_block;
    cxmalloc_linehead_t *linehead = NULL;

    // fix any incorrect active flags
    while( chunk_cursor < end_data ) {
      linehead = (cxmalloc_linehead_t*)chunk_cursor;
      if( linehead->data.refc == 0 && linehead->data.flags._act ) {
        CXMALLOC_WARNING( 0x451, "Allocator repair (%s): unreferenced active line at %p (aidx=%u bidx=%u offset=%u refc=%d)", name, linehead, linehead->data.aidx, linehead->data.bidx, linehead->data.offset, linehead->data.refc );
        linehead->data.flags._act = 0;
        linehead->data.flags._mod = 1;
        _cxmalloc_bitvector_clear( &block_CS->active, linehead );
      }
      if( linehead->data.refc > 0 && linehead->data.flags._act == 0 ) {
        CXMALLOC_WARNING( 0x452, "Allocator repair (%s): referenced inactive line at %p (aidx=%u bidx=%u offset=%u refc=%d)", name, linehead, linehead->data.aidx, linehead->data.bidx, linehead->data.offset, linehead->data.refc );
        linehead->data.flags._act = 1;
        linehead->data.flags._mod = 1;
        _cxmalloc_bitvector_set( &block_CS->active, linehead );
      }
      chunk_cursor += chunks_per_line;
    }

    // find and fix lines that are in disagreement with line register
    cxmalloc_linehead_t **bad_lines = _icxmalloc_block.FindLostLines_ACS( block_CS, true );
    char obidbuf[33] = {0};
    char identbuf[512] = {0};
    char *noid = "<not_object>";
    const char *identifier = NULL;
    objectid_t *obid = NULL;
    if( bad_lines ) {
      cxmalloc_linehead_t **line_cursor = bad_lines;
      int64_t n_bad = 0;
      while( (linehead = *line_cursor++) != NULL ) {
        ++n_bad;
      }
      if( n_bad > 0 ) {
        const char *many = n_bad > 1 ? "s" : "";
        CXMALLOC_WARNING( 0x453, "Allocator error (%s): found %lld bad line%s (aidx=%u bidx=%u)", name, n_bad, many, block_CS->parent->aidx, block_CS->bidx );
      }
      line_cursor = bad_lines;
      while( (linehead = *line_cursor++) != NULL ) {
        identifier = noid;
        comlib_object_t *obj = __cxmalloc_block__get_object_at_address_ACS( block_CS, (uintptr_t)_cxmalloc_object_from_linehead( linehead ) );
        if( obj ) {
          if( get_object_identifier ) {
            const char *object_identifier = get_object_identifier( obj );
            identifier = strncpy( identbuf, object_identifier, 511 );
          }
          else if( (obid = CALLABLE( obj )->vm_getid( obj )) != NULL ) {
            identifier = idtostr( obidbuf, obid );
          }
        }

        // Lost line: restore a positive refcount and discard to repair line register
        if( linehead->data.refc == 0 ) {
          linehead->data.refc = 1;
          _icxmalloc_line.Discard_NOLOCK( block_CS->parent->family, linehead );
          CXMALLOC_WARNING( 0x454, "Allocator repair (%s): reclaimed lost line at %p (aidx=%u bidx=%u offset=%u refc=%d) objectid=%s", name, linehead, linehead->data.aidx, linehead->data.bidx, linehead->data.offset, linehead->data.refc, identifier );
          ++n_fix;
        }
        // Ownership conflict
        else {
          CXMALLOC_CRITICAL( 0x455, "Allocator unrecoverable corruption (%s): ownership conflict for line at %p (aidx=%u bidx=%u offset=%u refc=%d) objectid=%s", name, linehead, linehead->data.aidx, linehead->data.bidx, linehead->data.offset, linehead->data.refc, identifier );
          n_fix = -1;
          break;
        }
      }
      free( bad_lines );
    }
  }

  return n_fix;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_linehead_t ** __cxmalloc_block__find_lost_lines_ACS( cxmalloc_block_t *block_CS, int force ) {
  cxmalloc_linehead_t **list = NULL;
  cxmalloc_linehead_t *linehead = NULL;
  cxmalloc_datashape_t *shape = &block_CS->parent->shape;

  if( block_CS->linedata == NULL ) {
    return NULL;
  }

  // If block corrupted, allocate and return a list of lines with problems
  // WARNING: Caller owns returned memory
  if( force || __cxmalloc_block__validate_refcounts_ACS( block_CS, &linehead ) < 0 ) {
    if( (list = calloc( block_CS->capacity+1, sizeof( cxmalloc_linehead_t* ) )) != NULL ) {
      // Now scan the block's line register and set checkmark to 1 for the lines owned by allocator
      cxmalloc_linehead_t **reg_cursor = block_CS->reg.top;
      cxmalloc_linehead_t **wp = list;
      // Mark all unallocated lines with a checkmark
      while( reg_cursor < block_CS->reg.bottom ) {
        // Allocator owns line (i.e. unallocated)
        if( (linehead = *reg_cursor++) != NULL && !linehead->data.flags._act ) {
          // Mark line as checked
          linehead->data.flags._chk = 1;
          // Block thinks it owns the line, but it's actually in use!
          // This is incorrect, add line to output.
          if( linehead->data.refc != 0 ) {
            *wp++ = linehead;
          }
        }
      }
      // Now look at lines without checkmarks, i.e. not known by line register
      size_t stride = shape->linemem.chunks;
      cxmalloc_linechunk_t *data_cursor = block_CS->linedata;
      cxmalloc_linechunk_t *data_end = block_CS->linedata + stride * shape->blockmem.quant;
      while( data_cursor < data_end ) {
        // Get the linehead and advance cursor
        linehead = (cxmalloc_linehead_t*)data_cursor;
        data_cursor += stride;
        // Unmarked means allocated
        if( linehead->data.flags._chk == 0 ) {
          // Leaked line. Refcnt zero means nobody has ownership
          if( linehead->data.refc == 0 ) {
            *wp++ = linehead;
          }
        }
      }
      // Reset all checkmarks
      data_cursor = block_CS->linedata;
      while( data_cursor < data_end ) {
        // Get the linehead and advance cursor
        linehead = (cxmalloc_linehead_t*)data_cursor;
        data_cursor += stride;
        // Reset checkmark
        linehead->data.flags._chk = 0;
      }
    }
  }

  return list;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static CStringQueue_t * __cxmalloc_block__repr_ARO( const cxmalloc_block_t *block_RO, CStringQueue_t *output ) {
  typedef int64_t (*f_format)( CStringQueue_t *q, const char *fmt, ... );
  f_format Format = CALLABLE(output)->Format;
# define PUT( FormatString, ... ) Format( output, FormatString, ##__VA_ARGS__ )
# define LINE( FormatString, ... ) Format( output, FormatString"\n", ##__VA_ARGS__ )

  if( block_RO ) {
    cxmalloc_datashape_t *shape = &block_RO->parent->shape;

    LINE( "Block @ %llp", block_RO );
    LINE( "  .bidx           :  %lld", block_RO->bidx );
    LINE( "  .family         :  %s", block_RO->parent->family->obid.longstring.string );
    LINE( "  .path           :  %s", CStringValue( block_RO->CSTR__base_path ) );
    LINE( "  .capacity       :  %lld", block_RO->capacity );
    LINE( "  .available      :  %lld", block_RO->available );
    LINE( "  .register       :  [top:%llp  get:%llp  put:%llp  bottom:%llp]", block_RO->reg.top, block_RO->reg.get, block_RO->reg.put, block_RO->reg.bottom );
    if( block_RO->linedata ) {
      if( block_RO->reg.get < block_RO->reg.put ) {
    LINE( "  .register.ofs   :  [top:0  get:%lld  put:%lld  bottom:%lld]", block_RO->reg.get ? block_RO->reg.get - block_RO->reg.top : -1, block_RO->reg.put ? block_RO->reg.put - block_RO->reg.top : -1, block_RO->reg.bottom - block_RO->reg.top );
      }
      else {
    LINE( "  .register.ofs   :  [top:0  put:%lld  get:%lld  bottom:%lld]", block_RO->reg.put ? block_RO->reg.put - block_RO->reg.top : -1, block_RO->reg.get ? block_RO->reg.get - block_RO->reg.top : -1, block_RO->reg.bottom - block_RO->reg.top );
      }
    }
    LINE( "  .linedata       :  %llp", block_RO->linedata );
    LINE( "  .sz_line        :  %llu", shape->linemem.chunks * sizeof(cxmalloc_linechunk_t) );
    LINE( "  .sz_linedata    :  %llu", shape->blockmem.chunks * sizeof(cxmalloc_linechunk_t) );
    LINE( "  .previous       :  %llp", block_RO->prev_block );
    LINE( "  .next           :  %llp", block_RO->next_block );
  }
  else {
    LINE( "<NULL>" );
  }

#undef PUT
#undef LINE

  return output;
}
