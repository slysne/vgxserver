/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_shape.c
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

//static               uint16_t __cxmalloc_shape__get_aidx_FRO(         cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength, uint16_t *aidx );
static               uint16_t __cxmalloc_shape__get_aidx_FRO(         cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength );
static               uint32_t __cxmalloc_shape__get_length_FRO(       const cxmalloc_family_t *family_RO, const uint16_t aidx );
static cxmalloc_datashape_t * __cxmalloc_shape__compute_shape_FRO(    cxmalloc_family_t *family_RO, const uint16_t aidx, cxmalloc_datashape_t *shape );
static                 size_t __cxmalloc_shape__block_bytes_ARO(      cxmalloc_block_t *block_RO );
static                 size_t __cxmalloc_shape__allocator_bytes_ACS(  cxmalloc_allocator_t *allocator_CS );
static                int64_t __cxmalloc_shape__family_bytes_FCS(     cxmalloc_family_t *family_CS );
static          histogram_t * __cxmalloc_shape__histogram_FCS(        cxmalloc_family_t *family_CS );




DLL_HIDDEN _icxmalloc_shape_t _icxmalloc_shape = {
  .GetAIDX_FRO        = __cxmalloc_shape__get_aidx_FRO,
  .GetLength_FRO      = __cxmalloc_shape__get_length_FRO,
  .ComputeShape_FRO   = __cxmalloc_shape__compute_shape_FRO,
  .BlockBytes_ARO     = __cxmalloc_shape__block_bytes_ARO,
  .AllocatorBytes_ACS = __cxmalloc_shape__allocator_bytes_ACS,
  .FamilyBytes_FCS    = __cxmalloc_shape__family_bytes_FCS,
  .Histogram_FCS      = __cxmalloc_shape__histogram_FCS
};



/*******************************************************************//**
 * Compute the required line size and corresponding allocator index
 * for the requested size;
 * 
 * family   : allocator family
 * sz       : requested size
 * alength  : pointer to integer where required size will be written
 * 
 * Return   : allocator index
 * 
 ***********************************************************************
 */
//static uint16_t __cxmalloc_shape__get_aidx_FRO( cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength, uint16_t *aidx ) {
static uint16_t __cxmalloc_shape__get_aidx_FRO( cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength ) {
  /* 
  Let's explain this.
  We use allocators of different sizes. We need to pick which allocator to use based on the requested size.
  Allocator size does not grow in even steps because at higher sizes, the cost of growing gets higher and
  higher since we need to copy data from the old to the new. Size does not grow exponentially either, because
  that has an overly heavy impact on memory consumption. As a compromise we grow in even steps for a while,
  then increase the step size for further growth. The points where a new step size is introduced are powers
  of two. A subdue control parameter is used to determine the number of size increments inbetween each power
  of two.
  Q     : The growth step size
  R     : The power-of-two exponent range for requested size, e.g. sz=1234 has R=10 because 2**10 = 1024.
  S     : The subdue damper exponent determining number of steps within each range R, e.g. S=3 yields 2**3=8
          steps within each range R, so from 1024 to 2048 there are 8 steps of Q=128.
  Rmin  : The smallest range for which Q is computed using S. Sizes below this range are all quantized to
          the minimum Q which is 64.

  We have to compute both the quantized size and find its allocator index.

  For the computation we measure everything in bytes. The minimum Q=64 because 64 is the size of a cache line.
  Growth is always in steps of 64 for smaller sizes. For larger sizes we compute Q as a power of two which is
  "S smaller" than the size range. If we use S=3 and our requested size is 1234 such that R=10, we calculate
  Q = 2**(R-S) = 2**7 = 128.( If S=2 then Q = 2**8 = 256, etc.) The smallest allocator for sz=1234 is the one
  within range R=log2(1234)=10 with the minimum number of Q-sized (128) increments from the base 1024. We reach
  a sufficient size by adding two steps of size Q to 2**R, i.e. alength = 2**10 + 2*(2**7) = 1280. Once we know
  R and Q it's easy to find alength by rounding up to the nearest multiple of Q: alength = (sz + Q - 1) & ~(Q -1).

  We find the allocator index by calculating the total number of growth steps from the smallest allocator to the
  chosen allocator. This is done by multiplying the number of Q-sized steps within each range R by the number of
  ranges below our chosen range, and then adding the number of Q-sized steps within our range to get to the 
  chosen allocator.
  */
  size_t Q;
  size_t bytes;
  unsigned int S;
  unsigned int Rmin;
  unsigned int R;
  uint16_t A;

  bytes = ((size_t)sz) << family_RO->unit_sz_order; // bytes = sz * unit_sz
  S = family_RO->subdue; // small S -> fewer reallocs but higher mem waste. large S -> lower memwaste but more reallocs
  Rmin = 6 + S; // log2(64) = 6

  // But wait.. we only do the crazy algo for full line sizes. The halfline shared with header is special.
  if( bytes > sizeof(cacheline_t) - family_RO->header_bytes ) {
    // let's add in the header bytes to get nicely aligned (i.e. non-odd = even) number of cachelines
    bytes += family_RO->header_bytes;
    if( bytes > (1ULL << Rmin) ) {
      // Find the power-of-two range R
      R = ilog2(bytes-1); // fast, we use bsr
      // The step size is a power of two S orders of magnitude smaller than our range R
      Q = 1ULL << (R - S);
      // aidx = the sum of:
      //  steps in ranges below chosen range R: (R-Rmin+1) * 2**S           e.g. (10-9+1) * 8 = 16
      //  steps within chosen range R:          (sz-1 - 2**R) / Q + 1 - 1   e.g. (1234-1 - 1024) / 128 = 1
      A = (uint16_t)(((R - Rmin + 1ULL) << S) + ((bytes - 1 - (1ULL << R)) >> (R - S)));
    }
    else {
      // The minimum step size is a single cache line
      Q = sizeof(cxmalloc_linequant_t); // 64
      // aidx = number of Q-sized steps we are from 0, i.e. aidx=(sz-1)/Q
      A = (uint16_t)((bytes-1) >> 6);
    }
    // Find the quantized allocator size in bytes by rounding up to next multiple of Q
    // Convert from bytes back to allocation units and add half a line
    // alength = "next multiple of Q" / unit_sz - "units used by header data"
    if( alength ) {
      *alength = (uint32_t)(((bytes + Q - 1) & ~(Q - 1)) >> family_RO->unit_sz_order) - (family_RO->header_bytes >> family_RO->unit_sz_order);
    }
  }
  else {
    // request can be fulfilled using a single cacheline shared with header
    A = 0;
    if( alength ) {
      *alength = (uint32_t)(sizeof(cacheline_t) - family_RO->header_bytes) >> family_RO->unit_sz_order; // number of units shared with header data
    }
  }
  /*
  if( aidx ) {
    *aidx = A;
  }
  */

  return A;
}



/*******************************************************************//**
 * Compute the allocator size based on its index.
 * 
 ***********************************************************************
 */
__inline static uint32_t __cxmalloc_shape__get_length_FRO( const cxmalloc_family_t *family_RO, const uint16_t aidx ) {
  size_t bytes;
  size_t A = aidx + 1; // +1 needed because of the halfline header, i.e. aidx=0 -> A=1 -> bytes A << 6 = 64 -> return (64-32) >> order 
  int S = family_RO->subdue;
  size_t R;

  if( A > (1ULL << S)  ) {
    R = (A >> S) + (6 + S - 1);
    bytes = (1ULL << R) + ((A & ((1 << S) - 1)) << (R - S));
  }
  else {
    bytes = A << 6;
  }
  return (uint32_t)((bytes - family_RO->header_bytes) >> family_RO->unit_sz_order);
}



/*******************************************************************//**
 * Compute datashape parameters for allocator number aidx in family.
 * Parameters are set in shape.
 * 
 * family : allocator family
 * aidx   : allocator index within family
 * shape  : pointer to datashape where parameters will be set
 * 
 * Return: pointer to shape, or NULL on failure
 ***********************************************************************
 */
static cxmalloc_datashape_t * __cxmalloc_shape__compute_shape_FRO( cxmalloc_family_t *family_RO, const uint16_t aidx, cxmalloc_datashape_t *shape ) {

  XTRY {
    /* pre-check */
    SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT
    if( sizeof(cxmalloc_linehead_t) != sizeof(cxmalloc_linechunk_t) ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x801 );
    }
    if( __cxmalloc_shape__get_aidx_FRO( family_RO, __cxmalloc_shape__get_length_FRO( family_RO, aidx ), NULL ) != aidx ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x802 );
    }
    /* linemem */
    // round to next lower multiple of page size
    size_t block_bytes = (family_RO->descriptor->parameter.block_sz / ARCH_PAGE_SIZE) * ARCH_PAGE_SIZE; 
    // [1] width depends on this allocator's index within family
    shape->linemem.awidth = __cxmalloc_shape__get_length_FRO( family_RO, aidx );
    // [2] unit size is the same for all allocators in family 
    shape->linemem.unit_sz = family_RO->descriptor->unit.sz;
    // total utilized bytes per line includes header bytes and all units
    size_t line_bytes = family_RO->header_bytes + (size_t)shape->linemem.awidth * (size_t)shape->linemem.unit_sz; 
    // [3] number of __m256 sized chunks includes header and line
    shape->linemem.chunks = (uint32_t)((line_bytes - 1) / sizeof(cxmalloc_linechunk_t) + 1);
    // [4] pad end of line to account for non-utilized data (should always be 0, but account for the possibility of non-zero)
    shape->linemem.pad = (uint32_t)((sizeof(cxmalloc_linequant_t) - line_bytes % sizeof(cxmalloc_linequant_t)) % sizeof(cxmalloc_linequant_t));   // pad > 0 if required, but ideally pad=0
    // [5] number of QWORDs in the array portion of the line
    shape->linemem.qwords = (shape->linemem.awidth * shape->linemem.unit_sz + shape->linemem.pad) / sizeof( QWORD );
    
    /* blockmem */
    // [6] number of __m256 sized chunks in block, including non-utilized space at end of block
    shape->blockmem.chunks = block_bytes / sizeof(cxmalloc_linechunk_t);
    // [7] number of lines that fit in this block
    shape->blockmem.quant = (uint32_t)(block_bytes / (line_bytes + shape->linemem.pad));
    // total bytes for all lines
    size_t total_bytes = (family_RO->header_bytes + sizeof(QWORD) * shape->linemem.qwords ) * (size_t)shape->blockmem.quant;
    // [8] pad end of last page in block to align block to end of page
    shape->blockmem.pad = ((ARCH_PAGE_SIZE - total_bytes % ARCH_PAGE_SIZE) % ARCH_PAGE_SIZE);

    /* serialized */
    // [9]
    shape->serialized.meta_sz = family_RO->descriptor->meta.serialized_sz;
    // [10]
    shape->serialized.obj_sz = family_RO->descriptor->obj.serialized_sz;
    // [11]
    shape->serialized.unit_sz = family_RO->descriptor->unit.serialized_sz;

    /* verify */
    // we need at least one line
    if( shape->blockmem.quant < 1 ) {
      THROW_ERROR( CXLIB_ERR_CAPACITY, 0x803 );
    }
    // check for correct padding
    if( (family_RO->header_bytes + ((size_t)shape->linemem.awidth * (size_t)shape->linemem.unit_sz + shape->linemem.pad)) * shape->blockmem.quant + shape->blockmem.pad > block_bytes ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x804 );
    }
    // check correct linechunk count
    if( (size_t)shape->linemem.chunks * (size_t)shape->blockmem.quant * sizeof(cxmalloc_linechunk_t) + shape->blockmem.pad > block_bytes ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x805 );
    }
    // check correct chunk count
    if( shape->blockmem.chunks * sizeof(cxmalloc_linechunk_t) > block_bytes ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x806 );
    }
  }
  XCATCH( errcode ) {
    shape = NULL;
  }
  XFINALLY {}
  return shape;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static size_t __cxmalloc_shape__block_bytes_ARO( cxmalloc_block_t *block_RO ) {
  size_t bytes = 0;
  // the block container
  bytes += sizeof(cxmalloc_block_t);
  // the line register
  if( block_RO->reg.top ) {
    bytes += sizeof(cxmalloc_linehead_t*) * (block_RO->reg.bottom - block_RO->reg.top);
  }
  // the block data
  if( block_RO->linedata ) {
    bytes += (size_t)block_RO->parent->shape.linemem.chunks * (size_t)block_RO->parent->shape.blockmem.quant * sizeof(cxmalloc_linechunk_t);
  }
  return bytes;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static size_t __cxmalloc_shape__allocator_bytes_ACS( cxmalloc_allocator_t *allocator_CS ) {
  cxmalloc_block_t *block_RO;
  cxmalloc_block_t **cursor_RO;
  size_t bytes = 0;
  // the allocator container
  bytes += sizeof(cxmalloc_allocator_t);
  // the block register
  if( allocator_CS->blocks ) {
    bytes += sizeof(cxmalloc_block_t*) * (allocator_CS->end - allocator_CS->blocks);
  }
  // the blocks
  cursor_RO = allocator_CS->blocks;
  IGNORE_WARNING_DEREFERENCING_NULL_POINTER
  while( cursor_RO < allocator_CS->space ) {
    block_RO = *cursor_RO++;
    bytes += __cxmalloc_shape__block_bytes_ARO( block_RO );
  }
  RESUME_WARNINGS
  return bytes;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_shape__family_bytes_FCS( cxmalloc_family_t *family_CS ) {
  int64_t bytes = 0;
  // the family container
  bytes += sizeof(cxmalloc_family_t);

  // the descriptor
  if( family_CS->descriptor ) {
    bytes += sizeof(cxmalloc_descriptor_t);
    bytes += strnlen( _cxmalloc_id_string(family_CS), OBJECTID_LONGSTRING_MAX ) + 1;
  }
  // the allocator register and allocators
  if( family_CS->allocators ) {
    bytes += sizeof(cxmalloc_allocator_t*) * family_CS->size;
    for( uint16_t aidx=0; aidx<family_CS->size; aidx++ ) {
      if( family_CS->allocators[aidx] ) {
        cxmalloc_allocator_t *allocator = family_CS->allocators[aidx];
        SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
          bytes += __cxmalloc_shape__allocator_bytes_ACS( allocator_CS );
        } RELEASE_CXMALLOC_ALLOCATOR;
      }
    }
  }
  return bytes;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static histogram_t * __cxmalloc_shape__histogram_FCS( cxmalloc_family_t *family_CS ) {
  histogram_t *histogram = NULL;

  if( (histogram = (histogram_t*) malloc( sizeof(histogram_t) * family_CS->descriptor->parameter.max_allocators )) == NULL ) {
    return NULL;
  }

  for( uint16_t aidx=0; aidx<family_CS->descriptor->parameter.max_allocators; aidx++ ) {
    histogram[aidx].freq = 0;
    if( aidx < family_CS->size ) {
      histogram[ aidx ].size = _icxmalloc_shape.GetLength_FRO( family_CS, aidx );
      cxmalloc_allocator_t *allocator = family_CS->allocators[ aidx ];
      if( allocator ) {
        SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
          cxmalloc_block_t **cursor_CS = allocator_CS->blocks;
          while( cursor_CS < allocator_CS->space ) {
            cxmalloc_block_t *block_CS = *cursor_CS++;
            histogram[aidx].freq += allocator_CS->shape.blockmem.quant - _icxmalloc_block.ComputeAvailable_ARO( block_CS );
          }
        } RELEASE_CXMALLOC_ALLOCATOR;
      }
    }
    else {
      histogram[aidx].size = 0;
    }
  }

  SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE
  return histogram; // <- Caller owns this memory!
}
