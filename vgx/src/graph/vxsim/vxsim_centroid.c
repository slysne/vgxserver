/*######################################################################
 *#
 *# vxsim_centroid.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxsim.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_VECTOR );


// vector magnitude summation cell
typedef struct __s_sum_vecelem_t {
  uint32_t dimension;
  int32_t magnitude;
} __sum_vecelem_t;



static int __count_vectors( const vgx_Vector_t *vectors[] );
static __sum_vecelem_t * __rehash( __sum_vecelem_t **htable, int *sz_htable );
static __sum_vecelem_t * __aggregate_linear( __sum_vecelem_t **output, int *sz_output, int *cnt_output, const vgx_Vector_t *vectors[] );
static unsigned * __get_moccur_map( __sum_vecelem_t *atable, int sz_atable, size_t nvectors );
static unsigned * __get_offset_map( const unsigned moccur_map[], uint16_t *centroid_length, int *min_mag );
static vector_feature_t * __get_map_centroid_elements( const __sum_vecelem_t *atable, int sz_atable, uint16_t length, unsigned offset_map[], int32_t mag_cutoff );



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __count_vectors( const vgx_Vector_t *vectors[] ) {
  int nvectors = 0;
  const vgx_Vector_t **cursor = vectors;
  const vgx_Vector_t *vector;
  while( *cursor != NULL ) {
    vector = *cursor++;
    if( (vector->metas.type & __VECTOR__MASK_EXTERNAL) ) {
      return -1; // only internal vectors supported
    }
    nvectors++;
  }
  return nvectors;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static __sum_vecelem_t * __rehash( __sum_vecelem_t **htable, int *sz_htable ) {
  if( *htable  ) {
    int prev_sz = *sz_htable;
    int next_sz = prev_sz * 2;
    uint32_t next_hmask = next_sz - 1;
    __sum_vecelem_t *prev_htable = *htable;
    __sum_vecelem_t *prev_end = prev_htable + prev_sz;
    __sum_vecelem_t *next_htable, *next_end;
    
    // Allocate larger hash table and set everything to 0
    if( PALIGNED_ARRAY( next_htable, __sum_vecelem_t, next_sz ) == NULL ) return NULL;
    memset( next_htable, 0, next_sz * sizeof(__sum_vecelem_t) );
    next_end = next_htable + next_sz;
   
    // Rehash from previous table into next (larger) table
    for( __sum_vecelem_t *prev = prev_htable, *next; prev < prev_end; prev++ ) {
      if( prev->dimension != 0 ) {
        // Find slot in next table
        next = next_htable + (prev->dimension & next_hmask);
        // Probe as needed to find available slot
        while( next->dimension != 0 ) {
          if( ++next == next_end ) next = next_htable;  // linear probe (wrap at end)
        }
        // Insert data into next table at selected slot
        next->dimension = prev->dimension;
        next->magnitude = prev->magnitude;
      }
    }
    
    // Delete old table and return new table
    ALIGNED_FREE( prev_htable );
    *htable = next_htable;
    *sz_htable = next_sz;
    return next_htable;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static __sum_vecelem_t * __aggregate_linear( __sum_vecelem_t **output, int *sz_output, int *cnt_output, const vgx_Vector_t *vectors[] ) {

  __sum_vecelem_t *htable = NULL;         //
  int sz_htable = *sz_output;             // initial 
  int count = 0;

  XTRY {
    __sum_vecelem_t *target = NULL;       //
    __sum_vecelem_t *htable_end = NULL;   //
    uint32_t htable_mask ;                //
    int resize_threshold;                 //

    // Allocate initial hash table
    if( sz_htable < 32 ) {
      sz_htable = 32; // minimum size
    }
    PALIGNED_ARRAY_THROWS( htable, __sum_vecelem_t, sz_htable, 0x301 );
    memset( htable, 0, sz_htable * sizeof(__sum_vecelem_t) );
    htable_end = htable + sz_htable;
    htable_mask = sz_htable - 1;
    resize_threshold = (int)(sz_htable * 0.8);

    //  Aggregate dimension weights using hash table
    for( const vgx_Vector_t **cursor = vectors; *cursor != NULL; cursor++ ) {
      const vgx_Vector_t *vector = *cursor;
      vector_feature_t *elem = (vector_feature_t*)CALLABLE( vector )->Elements( vector );
      const vector_feature_t *vector_end = elem + vector->metas.vlen;
      while( elem < vector_end ) {
        __sum_vecelem_t *hashpoint = target = htable + (elem->dim & htable_mask); // dimension hashes here
        __sum_vecelem_t *breakpoint = htable_end;
        // probe to hash slot if needed
        while( target->dimension != 0 && target->dimension != elem->dim ) {
          if( ++target == breakpoint ) {
            if( target == htable_end ) {
              target = htable; // wrap
              breakpoint = hashpoint;
            }
            else {
              break;
            }
          }
        }
        // occupy and count dimensions
        if( target->dimension == 0 ) {
          target->dimension = elem->dim;  // occupy this hash slot
          count++;                        // inc total dim count
        }
        // aggregate weights after linearizing
        // NOTE: The aggregated magnitude needs to be mapped back to 6-bit float later
        target->magnitude += (int)_vxsim_magnitude_map[ elem->mag ];   // add to dimension's aggregated weight
        // next element
        elem++;
        // check size and rehash if needed
        if( count == resize_threshold ) {
          if( __rehash( &htable, &sz_htable ) == NULL ) { // update table and size in-place
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x302 );
          }
          else {
            // update table vars
            resize_threshold = (int)(sz_htable * 0.8);
            htable_mask = sz_htable - 1;
            htable_end = htable + sz_htable;
          }
        }
      }
    }
  }
  XCATCH( errcode ) {
    if( htable ) {
      ALIGNED_FREE( htable );
      htable = NULL;
    }
    sz_htable = 0;
  }
  XFINALLY {
    *output = htable;
    *sz_output = sz_htable;
    *cnt_output = count;
  }

  return htable;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static unsigned * __get_moccur_map( __sum_vecelem_t *atable, int sz_atable, size_t nvectors ) {
  // Compute average magnitude for all dimensions and populate weight occurrence map

  unsigned *moccur_map = NULL;
#define __RECIPROC_SHIFT 32
  // a/b = a*(1/b) = a * ((2**n)/b) / (2**n) = a * reciproc / (2**n).  The point: Avoid DIV instruction inside the loop
  uint64_t reciproc = (1ULL<<__RECIPROC_SHIFT)/(nvectors > 0 ? nvectors : 1); // use a/b = ( a * ((1<<32)/b) ) >> 32
  __sum_vecelem_t *atable_end;

  // Allocate linearized occurrence map and initialize to 0
  if( CALIGNED_ARRAY( moccur_map, unsigned, _vxsim_magnitude_max+1LL ) == NULL ) {
    return NULL;
  }
  memset( moccur_map, 0, sizeof(unsigned) * (_vxsim_magnitude_max+1LL) );
  atable_end = atable + sz_atable;

  // Scan table and populate magnitude occurrence map
  for( __sum_vecelem_t *slot = atable; slot < atable_end; slot++ ) {
    if( slot->dimension != 0 ) {
      // compute average magnitude for this dimension as mag = aggregated/nvectors
      slot->magnitude = (slot->magnitude * reciproc) >> __RECIPROC_SHIFT; // avoid DIV
      slot->magnitude &= _vxsim_magnitude_max; // protect against overflow (caused by duplicate dimensions in input vector(s))
      moccur_map[ slot->magnitude ]++;        // inc freq for this magnitude
    }
  }
  return moccur_map;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static unsigned * __get_offset_map( const unsigned moccur_map[], uint16_t *centroid_length, int *min_mag ) {
  // Produce offset map from magnitude occurrence map
  unsigned *offset_map = NULL;
  if( CALIGNED_ARRAY( offset_map, unsigned, _vxsim_magnitude_max + 1LL ) == NULL ) {
    return NULL;
  }

  unsigned offset = 0;
  unsigned len = *centroid_length;

  for( unsigned mag=_vxsim_magnitude_max; mag > 0; mag-- ) { // mag > 0 because mag == 0 is a null dimension
    offset_map[ mag ] = offset;
    offset += moccur_map[ mag ];  // next offset
    if( offset >= len ) {
      *min_mag = mag;  // dimensions of this magnutude were the last to fit in the output centroid (and not necessarily all of the dims of this mag)
      break;
    }
  }

  if( offset < len ) {
    *centroid_length = (uint16_t)offset;
  }

  return offset_map;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vector_feature_t * __get_map_centroid_elements( const __sum_vecelem_t *atable, int sz_atable, uint16_t length, unsigned offset_map[], int32_t mag_cutoff ) {
  // Populate centroid elements from hash 
  vector_feature_t *elements = NULL;
  vector_feature_t *elem;
  unsigned offset;

  if( length > 0 ) {
    const __sum_vecelem_t *atable_end = atable + sz_atable;
    if( CALIGNED_ARRAY( elements, vector_feature_t, length ) == NULL ) {
      return NULL;
    }
    for( const __sum_vecelem_t *slot = atable; slot < atable_end; slot++ ) {
      if( slot->magnitude >= mag_cutoff ) {
        offset = offset_map[ slot->magnitude ]++;
        if( offset < length ) {
          elem = elements + offset;
          elem->dim = slot->dimension;
          elem->mag = _vxsim_weight_as_magnitude( (float)slot->magnitude / _vxsim_magnitude_unity );
        }
      }
    }
  }
  return elements;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Vector_constructor_args_t * __euclidean_centroid( vgx_Vector_constructor_args_t *cargs, const vgx_Vector_t *vectors[], int N ) {

  // TODO
  //  We need to scale up internal vectors by scalar.factor
  // 
  //  THIS COMPUTATION IS CURRENTLY INCORRECT WITHOUT THE SCALING FACTOR!
  // 
  // 
  //


  // 0. Ensure clean slate
  if( cargs->elements ) {
    ALIGNED_FREE( (void*)cargs->elements );
  }
  cargs->elements = NULL;
  cargs->vlen = 0;

  // Use longest vector length (should all be the same)
  const vgx_Vector_t *V;
  for( int n=0; n<N; n++ ) {
    V = vectors[n];
    if( V->metas.vlen > cargs->vlen ) {
      cargs->vlen = V->metas.vlen;
    }
  }

  if( cargs->vlen > 0 ) {

    // Centroid data
    BYTE *data;
    if( CALIGNED_ARRAY( data, BYTE, cargs->vlen ) == NULL ) {
      return NULL;
    }

    // Aggregator array
    double *aggr;
    if( CALIGNED_ARRAY( aggr, double, cargs->vlen ) == NULL ) {
      ALIGNED_FREE( data );
      return NULL;
    }
    memset( aggr, 0, cargs->vlen * sizeof( double ) );

    // Perform aggregation
    int vlen = cargs->vlen;
    for( int n=0; n<N; n++ ) {
      V = vectors[n];
      const char *elem = CALLABLE(V)->Elements(V);
      const char *e_end = elem + V->metas.vlen;
      double factor = V->metas.scalar.factor; 
      double *a = aggr;
      while( elem < e_end ) {
        *a++ += __decode_char_to_double( *elem++, factor );
      }
    }

    // Scaling factor
    double factor = __euclidean_scaling_factor_dbl( aggr, vlen );
    cargs->scale = (float)factor;
    double inv_scale = factor > 0.0 ? 1.0/factor : 0.0;

    // Convert to internal
    double *src = aggr;
    double *end = src + vlen;
    BYTE *dest = data;
    while( src < end ) {
      *dest++ = __encode_double_to_char( *src++, inv_scale );
    }
    
    ALIGNED_FREE( aggr );

    cargs->elements = data;
  }

  return cargs;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Vector_constructor_args_t * __map_centroid( vgx_Vector_constructor_args_t *cargs, uint16_t max_vlen, const vgx_Vector_t *vectors[], int N ) {

  __sum_vecelem_t *atable = NULL;         // aggregator table
  int sz_atable = 0;                      // size of aggregator table
  unsigned *moccur_map = NULL;            // map magnitude to number of dimension with that weight
  unsigned *offset_map = NULL;            // map magnitude to offset in output centroid array where next dimension with this weight will be placed

  XTRY {
    int unique = 0;                       // total number of unique dimensions
    int min_mag = 1;                  // smallest magnitude to make it into centroid

    // 0. Ensure clean slate
    if( cargs->elements ) {
      ALIGNED_FREE( (void*)cargs->elements );
    }
    cargs->elements = NULL;
    cargs->vlen = 0;

    // 2. Aggregate magnitudes into new aggregator table
    sz_atable = pow2log2( N ); // some formula to guess initial size of aggregator table
    if( __aggregate_linear( &atable, &sz_atable, &unique, vectors ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x311 ); 
    }

    // 3. Compute expected length of centroid
    cargs->vlen = unique < max_vlen ? (uint16_t)unique : max_vlen;

    // 4. Generate magnitude occurrence map
    if( (moccur_map = __get_moccur_map( atable, sz_atable, N )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x312 );
    }

    // 5. Generate magnitude offset map into output centroid array (for direct insertion of centroid dimensions sorted by magnitude, descending)
    offset_map = __get_offset_map( moccur_map, &cargs->vlen, &min_mag ); // (centroid length may be shorter than expected above)
    
    // 6. Generate sorted list of output elements
    cargs->elements = __get_map_centroid_elements( atable, sz_atable, cargs->vlen, offset_map, min_mag ); // Generate sorted list of output elements

  }
  XCATCH( errcode ) {
    if( cargs && cargs->elements ) {
      ALIGNED_FREE( (void*)cargs->elements );
    }
    cargs = NULL;
  }
  XFINALLY {
    // Remove all working maps and tables
    if( atable ) {
      ALIGNED_FREE( atable );
    }
    if( moccur_map ) {
      ALIGNED_FREE( moccur_map );
    }
    if( offset_map ) {
      ALIGNED_FREE( offset_map );
    }
  }

  return cargs;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vector_constructor_args_t * centroid_constructor_args( vgx_Vector_constructor_args_t *cargs, uint16_t max_vlen, const vgx_Vector_t *vectors[] ) {
  int N = __count_vectors( vectors );
 
  if( N < 0 ) {
    return NULL;
  }

  if( igraphfactory.EuclideanVectors() ) {
    return __euclidean_centroid( cargs, vectors, N );
  }
  else {
    return __map_centroid( cargs, max_vlen, vectors, N );
  }
}







#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxsim_centroid.h"
  
test_descriptor_t _vgx_vxsim_centroid_tests[] = {
  { "VGX Graph Centroid Tests", __utest_vxsim_centroid },
  {NULL}
};
#endif














