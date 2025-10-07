/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxsim_lsh.c
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

#include "_vgx.h"
#include "_vxsim.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_LSH );

/* number of bits set to one in a byte */
/* (256 bytes table = 4 CLs) */
static const unsigned char g_popcnt8[] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */



/* common comlib_object_vtable_t interface */
static vgx_Fingerprinter_t * Fingerprinter_constructor( const void *identifier, vgx_Fingerprinter_constructor_args_t *args );
static void Fingerprinter_destructor( vgx_Fingerprinter_t *self );
/* Fingerprinter interface */
// TODO

static int Fingerprinter_hamming_distance_lookup( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );
static int Fingerprinter_hamming_distance_naive( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );
static int Fingerprinter_hamming_distance_parallel( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );
static int Fingerprinter_hamming_distance_intrinsic( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );

static FP_t Fingerprinter_compute( const vgx_Fingerprinter_t *self, vgx_Vector_t *vector, int64_t seed, FP_t *rlcm );
static FP_t Fingerprinter_compute_bytearray( const vgx_Fingerprinter_t *self, const BYTE* bytes, int sz, int64_t seed, FP_t *rlcm );
static char * Fingerprinter_projections( const vgx_Fingerprinter_t *self, char buffer321[], const vgx_Vector_t *vector, FP_t lsh, FP_t lcm, WORD seed, int ksize, bool reduce, bool expand );

static int Fingerprinter_segm_valid( const vgx_Fingerprinter_t *self, int nsegm, int nsign );
static int Fingerprinter_pno_valid( const vgx_Fingerprinter_t *self, int pno, int nsegm, int nsign );
static int Fingerprinter_pure_regions( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );



static vgx_Fingerprinter_vtable_t Fingerprinter_Methods = {
  /* common comlib_object_vtable_t interface */
  .vm_cmpid         = NULL,
  .vm_getid         = NULL,
  .vm_serialize     = NULL,
  .vm_deserialize   = NULL,
  .vm_construct     = (f_object_constructor_t)Fingerprinter_constructor,
  .vm_destroy       = (f_object_destructor_t)Fingerprinter_destructor,
  .vm_represent     = NULL,
  .vm_allocator     = NULL,
  /* Fingerprinter interface */
  .Distance         = Fingerprinter_hamming_distance_intrinsic,
  .Compute          = Fingerprinter_compute,
  .ComputeBytearray = Fingerprinter_compute_bytearray,
  .Projections      = Fingerprinter_projections,
  .SegmValid        = Fingerprinter_segm_valid,
  .PnoValid         = Fingerprinter_pno_valid,
  .PureRegions      = Fingerprinter_pure_regions
  // TODO
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Fingerprinter_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( vgx_Fingerprinter_t, CXLIB_OBTYPE_SIGNATURE, &Fingerprinter_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Fingerprinter_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Fingerprinter_t );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Fingerprinter_t * Fingerprinter_constructor( const void *identifier, vgx_Fingerprinter_constructor_args_t *args ) {
  vgx_Fingerprinter_t *self = NULL;

  XTRY {
    CALIGNED_MALLOC_THROWS( self, vgx_Fingerprinter_t, 0x541 );
    memset( self, 0, sizeof(vgx_Fingerprinter_t) );
    if( COMLIB_OBJECT_INIT( vgx_Fingerprinter_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x542 );
    }

    if( args ) {
      self->use_segm = (unsigned char)(args->nsegm > 1);
      self->nsegm = (uint8_t)args->nsegm;
      self->nsign = (uint8_t)args->nsign;
      if( self->use_segm ) {
        int b = 8*sizeof(FP_t);
        int n = self->nsegm = (uint8_t)args->nsegm;
        if( args->nsegm > MAX_FP_PARTS || args->nsegm < MIN_FP_PARTS ) {
          THROW_ERROR( CXLIB_ERR_CONFIG, 0x543 );
        }
        while( n > 0 ) {
          self->bits[n-1] = (unsigned char)(b/n);
          n--;
          b -= self->bits[n];
          self->shifts[n] = (unsigned char)b;
          self->masks[n] = (( 1ULL << self->bits[n] ) - 1);
        }
      }

      self->dimencode = args->dimension_encoder;
      self->parent = args->parent;

#ifndef NDEBUG
      pmesg(4, "fingerprint generator (nsegm=%d)\n", self->nsegm);
      for(int i=0; i<self->nsegm; i++) {
        pmesg(4, "MASK[%d]  %llx\n", i, self->masks[i] );
        pmesg(4, "SHIFT[%d] %d\n", i, self->shifts[i]);
        pmesg(4, "SMASK[%d] %016llX\n", i, self->masks[i] << self->shifts[i] );
        pmesg(4, "BITS[%d]  %d\n", i, self->bits[i]);
        pmesg(4, "--------\n");
      }
#endif
    }
  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
    }
    self = NULL;
  }
  XFINALLY {}

  return self;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void Fingerprinter_destructor( vgx_Fingerprinter_t *self ) {
  if( self ) {
    ALIGNED_FREE( self );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_segm_valid( const vgx_Fingerprinter_t *self, int nsegm, int nsign ) {
  if( nsegm < MIN_FP_PARTS ) {
    return 0;
  }
  if( nsign >= nsegm ) {
    return 0;
  }
  if( nsegm > MAX_FP_PARTS ) {
    return 0;
  }
  return 1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_pno_valid( const vgx_Fingerprinter_t *self, int pno, int nsegm, int nsign ) {
  if( pno < 0 ) {
    return 0;
  }
  if( pno >= (int)comb(nsegm, nsign) ) {
    return 0;
  }
  return 1;
}



/*******************************************************************//**
 * Return the number of significant regions with identical bits in the
 * two fingerprints, according to global segmentation configuration.
 *
 ***********************************************************************
 */
static int Fingerprinter_pure_regions( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 ) {
  int cnt = 0;
  int n = self->nsegm;
  FP_t mask;

  while( n-- > 0 ) {
    mask = self->masks[n] << self->shifts[n];
    if( (fp1 & mask) == (fp2 & mask) ) {
      cnt++;
    }
  }

  return cnt;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static FP_t __fp_int_eucl_segm( const vgx_Fingerprinter_t *self, const vgx_Vector_t *vector, int64_t seed ) {
  FP_t FP = 0;
  int vlen = vector->metas.vlen;
  int32_t fp_array[ 64 ] = {0}; // 64 slots, one per bit in FP_t
  int32_t *end_array = fp_array + 64;
  int32_t *fp_slot;
  const char *elem = (char*)CALLABLE( vector )->Elements( vector );
  /* fp vars */
  unsigned char touched[MAX_FP_PARTS] = {0}; // big enough for max-case segments
  // hash dim index, parts and add/subtract index's value from corresponding fp_array positions
  uint64_t r = ihash64( seed );
  for( int64_t vx=0, pn=0; vx < vlen; vx++, pn++ ) {
    if( pn >= self->nsegm ) {
      pn = 0; // cycle part following index and wrap
    }
    int fx = self->shifts[ pn ];
    int32_t patt = (int32_t)ihash64( vx * r++ ); // bit pattern source is hash of index
    patt &= (int32_t)self->masks[pn];       // then finalize pattern using lower mask bits (16, 13, 12, 11, 10, 9, or 8 bits)
    if( patt == 0 ) {
      patt = 1;                              // rare case, ensure patt is nonzero if dim is nonzero
    }
    int nbits = self->bits[pn];
    int32_t x = *elem++;
    for( int j=0; j<nbits; j++ ) {
      int f = (((patt & 1) << 1) - 1); // -1 or 1 without using if
      patt >>= 1;
      fp_array[fx++] += x * f; // x times -1 or x times 1
    }
    touched[pn] = 1; // at least one term hashed to this segment
  }
  // set bits in FP according to fp_array +/-.
  fp_slot = end_array;
  while( --fp_slot >= fp_array ) {
    FP = (FP<<1) | (int)(*fp_slot > 0);
  }
  // untouched segments get bits from other segments (cycling)
  for( int pj=-1,pn=0; pn<self->nsegm; pn++ ) {
    if( !touched[pn] ) {
      for( int j=0; j<self->nsegm; j++ ) {
        if( touched[ pj = (pj+1) % self->nsegm ] ) {
          FP |= (((FP >> self->shifts[pj]) & self->masks[pj]) & self->masks[pn]) << self->shifts[pn];
          break;
        }
      }
    }
  }
  return FP;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static FP_t __fp_int_feat_segm( const vgx_Fingerprinter_t *self, const vgx_Vector_t *vector ) {
  FP_t FP = 0;
  int vlen = vector->metas.vlen;
  int16_t fp_array[ 64 ] = {0}; // 64 slots, one per bit in FP_t
  int16_t *end_array = fp_array + 64LL;
  int16_t *fp_slot;
  uint32_t dim;
  const vector_feature_t *elem = (vector_feature_t*)CALLABLE( vector )->Elements( vector );

  /* fp vars */
  unsigned char touched[MAX_FP_PARTS] = {0}; // big enough for max-case segments
  // hash terms, parts and add/subtract term's weight from corresponding fp_array positions
  for( int vx=0; vx < vlen && (dim=elem->dim) != 0; vx++ ) { // terminate on first 0-dim even if vlen is larger
    int pn = (dim >> (FEATURE_VECTOR_DIM_BITS-8)) % self->nsegm;  // determine part# using
    int fx = self->shifts[ pn ];                          // upper 8 bits of dim
    uint32_t patt = dim & (uint32_t)self->masks[pn];           // determine bit pattern using lower mask bits (16, 13, 12, 11, 10, 9, or 8 bits)
    if(!patt && dim) patt = 1;                        // rare case, ensure patt is nonzero if dim is nonzero
    int nbits = self->bits[pn];
    for(int j=0; j<nbits; j++) {
      fp_array[fx++] += ( (patt & 1) ? (int16_t)_vxsim_magnitude_map[elem->mag] : -(int16_t)_vxsim_magnitude_map[elem->mag] ); // binary 1 means +, 0 means -
      patt >>= 1;
    }
    touched[pn] = 1; // at least one term hashed to this segment
    elem++;
  }
  // set bits in FP according to fp_array +/-.
  fp_slot = end_array;
  while( --fp_slot >= fp_array ) {
    FP = (FP<<1) | (int)(*fp_slot > 0);
  }
  // untouched segments get bits from other segments (cycling)
  for( int pj=-1,pn=0; pn<self->nsegm; pn++ ) {
    if( !touched[pn] ) {
      for( int j=0; j<self->nsegm; j++ ) {
        if( touched[ pj = (pj+1) % self->nsegm ] ) {
          FP |= (((FP >> self->shifts[pj]) & self->masks[pj]) & self->masks[pn]) << self->shifts[pn];
          break;
        }
      }
    }
  }

  return FP;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static FP_t __fp_bytes_eucl( const BYTE *bytes, int sz, int64_t seed, FP_t *rlcm ) {
  FP_t lsh = 0;
  int A[ 64 ] = {0}; // 64 slots
  int *E = A + 64;
  const char *m = (const char*)bytes;
  const char *z = m + sz;
  uint64_t seeda = ihash64( seed+1 );
  uint64_t seedb = ihash64( seed+2 );
  uint64_t seedc = ihash64( seed+3 );
  uint64_t seedd = ihash64( seed+4 );
  uint64_t h64a = ihash64( seeda );
  uint64_t h64b = ihash64( seedb );
  uint64_t h64c = ihash64( seedc );
  uint64_t h64d = ihash64( seedd );
  int x;
  int w;
  int *p;

  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;

  // N dimensions
  while( m < z ) {
    // #1
    x = *m++;
    a = h64a;
    b = h64b;
    c = h64c;
    d = h64d;
    // bit 0 - 63
    p = A;
    while( p < E ) {
      
      w = (int)((a&0xF) << 1) - 15; 
      a >>= 4;
      *p++ += w * x;

      w = (int)((b&0xF) << 1) - 15; 
      b >>= 4;
      *p++ += w * x;

      w = (int)((c&0xF) << 1) - 15; 
      c >>= 4;
      *p++ += w * x;

      w = (int)((d&0xF) << 1) - 15; 
      d >>= 4;
      *p++ += w * x;

    }

    h64a = ihash64( h64b + seeda );
    h64b = ihash64( h64c + seedb );
    h64c = ihash64( h64d + seedc );
    h64d = ihash64( h64a + seedd );
  }

  int o = 1;
  p = A;
  while( p < E ) {
    int i = *p++;
    int y = i >> 31; // 0xffffffff if neg
    int u = (i ^ y) - y; // abs
    o |= u; // order of magnitude bits
  }
  int t = ilog2( o ) - 4;

  // Set bits in lsh according to A +/-.
  FP_t lcm = 0; // low confidence mask
  p = A;
  while( p < E ) {
    int i = *p++;
    int y = i >> 31; // 0xffffffff if neg
    int u = (i ^ y) - y; // abs
    lcm = (lcm << 1) | !(u >> t);
    lsh = (lsh << 1) | (uint64_t)(y&1);
  }
  if( rlcm ) {
    *rlcm = lcm;
  }
  return lsh;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static FP_t __fp_int_eucl( const vgx_Vector_t *vector, int64_t seed, FP_t *rlcm ) {
  const BYTE *bytes = (BYTE*)CALLABLE( vector )->Elements( vector );
  return __fp_bytes_eucl( bytes, vector->metas.vlen, seed, rlcm );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static FP_t __fp_int_feat( const vgx_Vector_t *vector, int64_t seed ) {
  FP_t FP = 0;
  int vlen = vector->metas.vlen;
  int16_t fp_array[ 64 ] = {0}; // 64 slots, one per bit in FP_t
  int16_t *end_array = fp_array + 64LL;
  int16_t *fp_slot;
  uint32_t dim;
  const vector_feature_t *elem = (vector_feature_t*)CALLABLE( vector )->Elements( vector );
  int64_t h64;
  int64_t imag;
  uint64_t r = ihash64( seed );
  for( int vx=0; vx < vlen && (dim=elem->dim) != 0; vx++, elem++ ) { // terminate on first 0-dim even if vlen is larger
    imag = _vxsim_magnitude_map[elem->mag];
    h64 = (int64_t)ihash64( r + dim );
    fp_slot = fp_array;
    while( fp_slot < end_array ) {
      *fp_slot++ += (int16_t)((((h64 & 1) << 1) - 1) * imag); // imag times -1 or x times 1 (without using conditional)
      h64 >>= 1;
    }
  }
  // set bits in FP according to fp_array +/-.
  fp_slot = fp_array;
  while( fp_slot < end_array ) {
    FP = (FP << 1) | ((uint16_t)(*fp_slot++) >> 15);
  }
  return FP;
}





/*******************************************************************//**
 * Compute segmented LSH of vector using current global configuration.
 * Any segments (partitions) untouched after all vector elements have been
 * hashed/applied are filled in using data from other segments.
 *
 * CURRENT WEAKNESS: if the 64-bit fingerprint array is all negative the FP=0.
 *
 *
 * vector   : vector object
 * seed     : seed for randomized projection
 * rlcm     : return low confidence mask (if not NULL)
 *
 * Returns  : 64-bit fingerprint
 ***********************************************************************
 */
static FP_t Fingerprinter_compute( const vgx_Fingerprinter_t *self, vgx_Vector_t *vector, int64_t seed, FP_t *rlcm ) {

  // null vector
  if( vector->metas.flags.nul ) {
    return 0; // by definition
  }

  FP_t FP = 0;

  // internal vector
  if( (vector->metas.type & __VECTOR__MASK_INTERNAL) ) {
    // EUCLIDEAN VECTOR
    if( vector->metas.flags.ecl ) {
      // Non-segmented LSH
      if( !self->use_segm ) {
        FP = __fp_int_eucl( vector, seed, rlcm );
      }
      // Segmented LSH
      else {
        FP = __fp_int_eucl_segm( self, vector, seed );
      }
    }
    // FEATURE VECTOR
    else {
      // Non-segmented LSH
      if( !self->use_segm ) {
        FP = __fp_int_feat( vector, seed );
      }
      // Segmented LSH
      else {
        FP = __fp_int_feat_segm( self, vector );
      }
    }

    // Ensure never zero
    FP |= -(FP == 0);
    
  }
  // external vector
  else if( (vector->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    vgx_VectorContext_t *vector_context = CALLABLE(vector)->Context(vector);
    vgx_Similarity_t *simobj = vector_context->simobj;
    // We need to create a temporary internal vector to compute fingerprint from
    vgx_Vector_t *internal_vector = CALLABLE(simobj)->InternalizeVector( simobj, vector, true, NULL );
    if( internal_vector == NULL ) {
      return 0; // error
    }
    if( (vector->metas.type & __VECTOR__MASK_INTERNAL) ) {
      FP = Fingerprinter_compute( self, internal_vector, seed, rlcm );
    }
    CALLABLE( internal_vector )->Decref( internal_vector );
  }

  return FP;
}



/*******************************************************************//**
 *  
 *  Return fingerprint of internal vector represented by pure bytes
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static FP_t Fingerprinter_compute_bytearray( const vgx_Fingerprinter_t *self, const BYTE *bytes, int sz, int64_t seed, FP_t *rlcm ) {
  return __fp_bytes_eucl( bytes, sz, seed, rlcm );
}



/*******************************************************************//**
 *  Fast lookup table solution.
 *  Return hamming distance between fp1 and fp2.
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_hamming_distance_lookup( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 ) {
  int dist = 0;
  FP_t diff = fp1 ^ fp2;

  for( int i=0; i<8; i++ ) {
    dist += g_popcnt8[ 0xFF & diff ];
    diff >>= 8;
  }

  return dist;
}


/*******************************************************************//**
 * a more elegant solution, but much slower unfortunately
 *  always slower than lookup regardless of hamming distance
 *    6-7 times slower for "typical" hamming distance=23
 *    19 times slower for distance 64
 *    about the same for distance 0 (1% slower)
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_hamming_distance_naive( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 ) {
  int dist = 0;
  FP_t bits;
  bits = fp1 ^ fp2;   // will have 1's where different
  while( bits ) {
    ++dist;
    bits &= bits - 1; // clear the lowest 1 repeatedly until the result is 0
  }
  return dist;
}


/*******************************************************************//**
 *
 * parallel
 *
 ***********************************************************************
 */
#define __01010101__ 0x5555555555555555ULL
#define __00110011__ 0x3333333333333333ULL
#define __00001111__ 0x0F0F0F0F0F0F0F0FULL
#define __00000001__ 0x0101010101010101ULL


/**************************************************************************//**
 * Fingerprinter_hamming_distance_parallel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_hamming_distance_parallel( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 ) {
  FP_t bits = fp1 ^ fp2;
  bits = bits - ((bits >> 1) & __01010101__);
  bits = (bits & __00110011__) + ((bits >> 2) & __00110011__);
  return (((bits + (bits >> 4)) & __00001111__) * __00000001__) >> 56;
}


/*******************************************************************//**
 *
 * intrinsic
 *
 ***********************************************************************
 */
#if defined CXPLAT_WINDOWS_X64
unsigned __int64 __popcnt64( unsigned __int64 value );
#  pragma intrinsic ( __popcnt64 )
#elif defined(__GNUC__) || defined(__clang__)
/* GCC/Clang (Linux, macOS) */
#  define __popcnt64  __builtin_popcountll
#else
#error "__popcnt64 not defined for this platform"
#endif


/**************************************************************************//**
 * Fingerprinter_hamming_distance_intrinsic
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Fingerprinter_hamming_distance_intrinsic( const vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 ) {
  return (int)__popcnt64( fp1 ^  fp2 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static FP_t __lsh_rotr( FP_t lsh, int d ) {
  return (lsh >> d) | (lsh << (64-d));
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static uint32_t __lsh_rotr_mask32( FP_t lsh, int d ) {
  return (uint32_t)__lsh_rotr( lsh, d );
}



/*******************************************************************//**
 *
 *  Populate buffer321 with string "_pppp|sss\n_pppp|sss\n..._pppp|sss\n"
 *  where each <pppp> is a projection key computed from the LSH and <sss>
 *  is the LSH seed.
 * 
 *  There are 16 projections 0 - 15 in a projection set.
 *  <pppp> for projection key i is computed as (i << 8) | ki
 *
 *  where keys k0 - k15 are extracted from LSH as follows:
 *
 *  LSH = 1010101010101010101010101010101010101010101010101010101010101010
 *        [__k14_][__k12_][__k10_][__k8__][__k6__][__k4__][__k2__][__k0__]
 *        k15][__k13_][__k11_][__k9__][__k7__][__k5__][__k3__][__k1__][___
 *
 *  Thus a projection set contains 16 * 256 = 4096 unique keys.
 * 
 *  The first subset of projections in a projection set contains the 256 keys
 *  extracted from LSH region k0, the second subset contains 256 keys from
 *  LSH region k1, and so on.
 *
 *  The projection set tag <sss> associates a projection set with the LSH seed.
 *
 ************************************************************************
 */

/*******************************************************************//**
 *
 *  Populate buffer321 with string "_pppp|sss\n_pppp|sss\n..._pppp|sss\n"
 *  where each <pppp> is a projection key computed from the LSH and <sss>
 *  is the LSH seed.
 * 
 *  There are 12 projections 0 - 11 in a projection set.
 *  <pppp> for projection key i is computed as (i << 10) | ki
 *
 *  where keys k0 - k11 are extracted from LSH as follows:
 *
 *  LSH = 1010101010101010101010101010101010101010101010101010101010101010
 *        ....[__k10___][___k8___][___k6___][___k4___][___k2___][___k0___]
 *        __k11___][___k9___][___k7___][___k5___][___k3___][___k1___]....[
 *
 * 
 *  Thus a projection set contains 12 * 1024 = 12288 unique keys.
 * 
 *  The first subset of projections in a projection set contains the 1024 keys
 *  extracted from LSH region k0, the second subset contains 1024 keys from
 *  LSH region k1, and so on.
 *
 *  The projection set tag <sss> associates a projection set with the LSH seed.
 *
 ************************************************************************
 */

/*******************************************************************//**
 *
 *  Populate buffer321 with string "_pppp|sss\n_pppp|sss\n..._pppp|sss\n"
 *  where each <pppp> is a projection key computed from the LSH and <sss>
 *  is the LSH seed.
 * 
 *  There are 15 projections 0 - 14 in a projection set.
 *  <pppp> for projection key i is computed as (i << 12) | ki
 *
 *  where keys k0 - k14 are extracted from LSH as follows:
 *
 *  LSH = 1010101010101010101010101010101010101010101010101010101010101010
 *        ....[___k12____][____k9____][____k6____][____k3____][____k0____]
 *        [___k13____][___k10____][____k7____][____k4____][____k1____]....
 *        __k14__][___k11____][____k8____][____k5____][____k2____]....[___
 * 
 *  Thus a projection set contains 15 * 4096 = 61440 unique keys.
 * 
 *  The first subset of projections in a projection set contains the 4096 keys
 *  extracted from LSH region k0, the second subset contains 4096 keys from
 *  LSH region k1, and so on.
 *
 *  The projection set tag <sss> associates a projection set with the LSH seed.
 *
 ************************************************************************
 */

__inline static char * __lsh_PX_keys( int ksize, FP_t lsh, FP_t lcm, char buffer321[], WORD seed, bool expand ) {

#define SUBSET( BITS, LSH, LCM ) \
  do { \
    unsigned mask = (unsigned)(LCM & ((1u<<BITS)-1u)); \
    if( mask == 0 || POPCNT32( mask ) == 1 ) { \
      unsigned kx = (unsigned)(LSH & ((1<<BITS)-1)); \
      LSH >>= BITS; \
      unsigned s = (subset<<BITS) + kx; \
      *p++ = '_'; \
      p = write_HEX_word( p, (WORD)s ); \
      memcpy( p, sseed, 5 ); \
      p += 5; \
      if( expand && mask ) { \
        *p++ = '_'; \
        p = write_HEX_word( p, (WORD)(s ^ mask) ); \
        memcpy( p, sseed, 5 ); \
        p += 5; \
      } \
    } \
    LCM >>= BITS; \
    ++subset; \
  } WHILE_ZERO

  char sseed[] = "....\n";
  write_HEX_word( sseed, seed );
  sseed[0] = '|';

  char *p = buffer321;
  unsigned subset = 0;

  FP_t lshA = lsh;
  FP_t lcmA = lcm;
  FP_t lshB;
  FP_t lcmB;
  FP_t lshC;
  FP_t lcmC;

  switch( ksize ) {
  case 8:
    lshB = __lsh_rotr( lshA, 4 );
    lcmB = __lsh_rotr( lcmA, 4 );

    while( subset < 16 ) {
      SUBSET( 8, lshA, lcmA );
      SUBSET( 8, lshB, lcmB );
    }
    break;
  
  case 10:
    lshB = __lsh_rotr( lshA, 5 );
    lcmB = __lsh_rotr( lcmA, 5 );

    while( subset < 12 ) {
      SUBSET( 10, lshA, lcmA );
      SUBSET( 10, lshB, lcmB );
    }
    break;

  case 12:
    lshB = __lsh_rotr( lshA, 4 );
    lcmB = __lsh_rotr( lcmA, 4 );
    lshC = __lsh_rotr( lshA, 8 );
    lcmC = __lsh_rotr( lcmA, 8 );

    while( subset < 15 ) {
      SUBSET( 12, lshA, lcmA );
      SUBSET( 12, lshB, lcmB );
      SUBSET( 12, lshC, lcmC );
    }
    break;

  default:
    return NULL;
  }

  *p++ = '\0';

  return buffer321;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static char * Fingerprinter_projections( const vgx_Fingerprinter_t *self, char buffer321[], const vgx_Vector_t *vector, FP_t lsh, FP_t lcm, WORD seed, int ksize, bool reduce, bool expand ) {
  if( lsh == 0 ) {
    lsh = __fp_int_eucl( vector, seed, &lcm );
  }

  // Force low confidence mask to all zero if we're
  // not going to discard low confidence keys (reduce)
  // AND
  // not going to expand by creating two projections for keys with one low confidence bit (expand)
  if( !reduce && !expand ) {
    lcm = 0; // Keep low confidence keys
  }

  return __lsh_PX_keys( ksize, lsh, lcm, buffer321, seed, expand );
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxsim_lsh.h"
  
test_descriptor_t _vgx_vxsim_lsh_tests[] = {
  { "VGX Graph LSH Tests", __utest_vxsim_lsh },
  {NULL}
};
#endif
