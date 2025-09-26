/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxsim.c
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

#include "_vgx.h"
#include "_vxsim.h"
#include "_vgx_serialization.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );





static vector_feature_t __encode_and_own_vecelem_CS( vgx_Similarity_t *self, const ext_vector_feature_t *feature );
static ext_vector_feature_t * __decode_vecelem_CS( vgx_Similarity_t *self, ext_vector_feature_t *dest, vector_feature_t elem );


static int64_t __deserialize_similarity( vgx_Similarity_t * self );
static int64_t __serialize_similarity( vgx_Similarity_t * self );


static int g_initialized = 0;


/* common comlib_object_vtable_t interface */
static int64_t Similarity_serialize( vgx_Similarity_t *self, CStringQueue_t *out_queue );
static vgx_Similarity_t * Similarity_deserialize( CStringQueue_t *in_queue );
static vgx_Similarity_t * Similarity_constructor( const void *identifier, vgx_Similarity_constructor_args_t *args );
static void Similarity_destructor( vgx_Similarity_t *self );
/* Similarity interface */

static vgx_Similarity_t * Similarity_clone( vgx_Similarity_t *self );

static vgx_Vector_t * Similarity_new_internal_vector( vgx_Similarity_t *self, const void *elements, float scale, uint16_t sz, bool ephemeral );
static vgx_Vector_t * Similarity_new_external_vector( vgx_Similarity_t *self, const void *elements, uint16_t sz, bool ephemeral );
static vgx_Vector_t * Similarity_new_internal_vector_from_external_elements( vgx_Similarity_t *self, const void *external_elements, uint16_t sz, bool ephemeral, CString_t **CSTR__error );
static vgx_Vector_t * Similarity_new_empty_internal_vector( vgx_Similarity_t *self, uint16_t vlen, bool ephemeral );
static vgx_Vector_t * Similarity_new_empty_external_vector( vgx_Similarity_t *self, uint16_t vlen, bool ephemeral );
static vgx_Vector_t * Similarity_internalize_vector( vgx_Similarity_t *self, vgx_Vector_t *source, bool ephemeral, CString_t **CSTR__error );
static vgx_Vector_t * Similarity_externalize_vector( vgx_Similarity_t *self, vgx_Vector_t *source, bool ephemeral );
static vgx_Vector_t * Similarity_translate_vector( vgx_Similarity_t *self, vgx_Vector_t *src, bool ephemeral, CString_t **CSTR__error );
static vgx_Vector_t * Similarity_new_centroid( vgx_Similarity_t *self, const vgx_Vector_t *vectors[], bool ephemeral );
static int Similarity_hamming_distance( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static float Similarity_euclidean_distance( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static float Similarity_cosine( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static float Similarity_jaccard( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static int8_t Similarity_intersect( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static float Similarity_similarity( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static bool Similarity_valid( vgx_Similarity_t *self );
static void Similarity_clear( vgx_Similarity_t *self );
static vgx_Similarity_value_t * Similarity_value( vgx_Similarity_t *self );
static int Similarity_match( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
static vgx_Vector_t * Similarity_vector_arithmetic( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B, bool subtract, CString_t **CSTR__error );
static vgx_Vector_t * Similarity_vector_scalar_multiply( vgx_Similarity_t *self, const vgx_Vector_t *A, double factor, CString_t **CSTR__error );
static double Similarity_vector_dot_product( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B, CString_t **CSTR__error );
static int Similarity_cmp_vectors( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B );
static int Similarity_set_readonly( vgx_Similarity_t *self );
static int Similarity_is_readonly( vgx_Similarity_t *self );
static int Similarity_clear_readonly( vgx_Similarity_t *self );
static void Similarity_print_vector_allocator( vgx_Similarity_t *self, vgx_Vector_t *vector );
static void Similarity_print_allocators( vgx_Similarity_t *self );
static int Similarity_check_allocators( vgx_Similarity_t *self );
static int64_t Similarity_verify_allocators( vgx_Similarity_t *self );
static int64_t Similarity_bulk_serialize( vgx_Similarity_t *self, bool force );



static vgx_Similarity_vtable_t Similarity_Methods = {
  /* common comlib_object_vtable_t interface */
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = (f_object_serializer_t)Similarity_serialize,
  .vm_deserialize = (f_object_deserializer_t)Similarity_deserialize,
  .vm_construct   = (f_object_constructor_t)Similarity_constructor,
  .vm_destroy     = (f_object_destructor_t)Similarity_destructor,
  .vm_represent   = NULL,
  .vm_allocator   = NULL,
  /* Similarity interface */
  .Clone                          = Similarity_clone,
  .NewInternalVector              = Similarity_new_internal_vector,
  .NewExternalVector              = Similarity_new_external_vector,
  .NewInternalVectorFromExternal  = Similarity_new_internal_vector_from_external_elements,
  .NewEmptyInternalVector         = Similarity_new_empty_internal_vector,
  .NewEmptyExternalVector         = Similarity_new_empty_external_vector,
  .InternalizeVector              = Similarity_internalize_vector,
  .ExternalizeVector              = Similarity_externalize_vector,
  .TranslateVector                = Similarity_translate_vector,
  .NewCentroid                    = Similarity_new_centroid,
  .HammingDistance                = Similarity_hamming_distance,
  .EuclideanDistance              = Similarity_euclidean_distance,
  .Cosine                         = Similarity_cosine,
  .Jaccard                        = Similarity_jaccard,
  .Intersect                      = Similarity_intersect,
  .Similarity                     = Similarity_similarity,
  .Valid                          = Similarity_valid,
  .Clear                          = Similarity_clear,
  .Value                          = Similarity_value,
  .Match                          = Similarity_match,
  .VectorArithmetic               = Similarity_vector_arithmetic,
  .VectorScalarMultiply           = Similarity_vector_scalar_multiply,
  .VectorDotProduct               = Similarity_vector_dot_product,
  .CompareVectors                 = Similarity_cmp_vectors,
  .SetReadonly                    = Similarity_set_readonly,
  .IsReadonly                     = Similarity_is_readonly,
  .ClearReadonly                  = Similarity_clear_readonly,
  .PrintVectorAllocator           = Similarity_print_vector_allocator,
  .PrintAllocators                = Similarity_print_allocators,
  .CheckAllocators                = Similarity_check_allocators,
  .VerifyAllocators               = Similarity_verify_allocators,
  .BulkSerialize                  = Similarity_bulk_serialize
};

static float __distance_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __distance_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __cosine_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __cosine_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __jaccard_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __jaccard_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static int8_t __intersect_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );
static int8_t __intersect_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B );


static float __distance_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __distance_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __cosine_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __cosine_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __jaccard_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static float __jaccard_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static int8_t __intersect_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B );
static int8_t __intersect_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vector_feature_t __encode_and_own_vecelem_CS( vgx_Similarity_t *self, const ext_vector_feature_t *feature ) {
  vector_feature_t elem = {
    .dim = _vxenum_dim__encode_chars_CS( self, feature->term, NULL, true ),
    .mag = _vxsim_weight_as_magnitude( feature->weight )
  };
  return elem;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static ext_vector_feature_t * __decode_vecelem_CS( vgx_Similarity_t *self, ext_vector_feature_t *dest, vector_feature_t elem ) {
  const char *dim = CStringValue( _vxenum_dim__decode_CS( self, elem.dim ) );
  strncpy( dest->term, dim, MAX_FEATURE_VECTOR_TERM_LEN );
  dest->term[ MAX_FEATURE_VECTOR_TERM_LEN ] = 0; 
  dest->weight = _vxsim_magnitude_as_weight( elem.mag );
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __distance_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  const BYTE *a_bytes = ivectorobject.GetElements( (vgx_Vector_t*)A );
  const BYTE *b_bytes = ivectorobject.GetElements( (vgx_Vector_t*)B );
  int len = minimum_value( A->metas.vlen, B->metas.vlen );
  float distance = (float)vxeval_bytearray_distance( a_bytes, b_bytes, A->metas.scalar.factor, B->metas.scalar.factor, len );
  return distance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __distance_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  const float *fa = ivectorobject.GetElements( (vgx_Vector_t*)A );
  const float *fb = ivectorobject.GetElements( (vgx_Vector_t*)B );
  int len = minimum_value( A->metas.vlen, B->metas.vlen );
  const float *ea = fa + len;
  double ssqdiff = 0.0;
  double diff;
  while( fa < ea ) {
    diff = *fa++ - *fb++;
    ssqdiff += diff * diff;
  }
  return (float)sqrt( ssqdiff );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __cosine_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  const BYTE *a_bytes = ivectorobject.GetElements( (vgx_Vector_t*)A );
  const BYTE *b_bytes = ivectorobject.GetElements( (vgx_Vector_t*)B );
  int len = minimum_value( A->metas.vlen, B->metas.vlen );
  double ba_dp = vxeval_bytearray_dot_product( a_bytes, b_bytes, len );
  double a_ba_rsqrt_ssq = vxeval_bytearray_rsqrt_ssq( a_bytes, len );
  double b_ba_rsqrt_ssq = vxeval_bytearray_rsqrt_ssq( b_bytes, len );
  double cosine = ba_dp * a_ba_rsqrt_ssq * b_ba_rsqrt_ssq;
  if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
    cosine = (double)((cosine > 0.0) - (cosine < 0.0));
  }
  return (float)cosine;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __cosine_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  const float *fa = ivectorobject.GetElements( (vgx_Vector_t*)A );
  const float *fb = ivectorobject.GetElements( (vgx_Vector_t*)B );
  int len = minimum_value( A->metas.vlen, B->metas.vlen );
  const float *ea = fa + len;
  double dp = 0.0;
  double da, db;
  while( fa < ea ) {
    da = *fa++;
    db = *fb++;
    dp += da * db;
  }
  double mAmB = A->metas.scalar.norm * B->metas.scalar.norm;
  double cosine = mAmB > 0.0 ? dp / mAmB : 0.0;
  return (float)cosine;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static float __jaccard_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return 0.0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static float __jaccard_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return 0.0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int8_t __intersect_internal_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int8_t __intersect_external_euclidean( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static float __distance_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return INFINITY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static float __distance_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return INFINITY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __cosine_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  vector_feature_t *a_elem = (vector_feature_t*)CALLABLE(A)->Elements(A);
  vector_feature_t *b_elem = (vector_feature_t*)CALLABLE(B)->Elements(B);
  vector_feature_t *a_end = a_elem + A->metas.vlen;
  vector_feature_t *b_end = b_elem + B->metas.vlen;
  vector_feature_t *a, *b;
  int idotprod = 0;
  float dotprod;
  float magprod;

  // compute dot product (A dot B)
  register uint32_t dimA, dimB;
  a = a_elem;
  b = b_elem;
  while( a < a_end && (dimA = a->dim) != 0 ) {  // note: terminate loop on first 0-dimension
    int a_mag = _vxsim_magnitude_map[ a->mag ];
    while( b < b_end && (dimB = b->dim) != 0 ) {
      if( dimA == dimB ) {  // <= hotspot!
        idotprod += a_mag * _vxsim_magnitude_map[ b->mag ];
        break; // no need to scan rest of B, dims are unique per vector
      }
      b++;
    }
    b = b_elem;
    a++;
  }
  
  // compute cosine: (A dot B) / |A||B|
  dotprod = _vxsim_square_magnitude_from_isqmag( idotprod );
  magprod = _vxsim_multiply_quantized( A->metas.scalar.norm, B->metas.scalar.norm );
  if( magprod > 0 ) {
    float cos = dotprod >= magprod ? 1.0f : _vxsim_divide_quantized( dotprod, magprod ) ; // cap it at 1.0 (in case of anomalies)
    return cos;
  }
  else {
    return 0.0f;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __cosine_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  ext_vector_feature_t *a_elem = (ext_vector_feature_t*)CALLABLE(A)->Elements(A);
  ext_vector_feature_t *b_elem = (ext_vector_feature_t*)CALLABLE(B)->Elements(B);
  ext_vector_feature_t *a_end = a_elem + A->metas.vlen;
  ext_vector_feature_t *b_end = b_elem + B->metas.vlen;
  ext_vector_feature_t *a, *b;
  float dotprod = 0;
  float magprod;

  // compute dot product (A dot B)
  const char *dimA, *dimB;
  a = a_elem;
  b = b_elem;
  while( a < a_end && *(dimA = a->term) != '\0' ) {  // note: terminate loop on first empty string
    while( b < b_end && *(dimB = b->term) != '\0' ) {
      if( strncmp( dimA, dimB, MAX_FEATURE_VECTOR_TERM_LEN ) == 0 ) {  // <= hotspot!
        dotprod += a->weight * b->weight;
        break; // no need to scan rest of B, dims are unique per vector
      }
      b++;
    }
    b = b_elem;
    a++;
  }

  // compute cosine: (A dot B) / |A||B|
  magprod = _vxsim_multiply_quantized( A->metas.scalar.norm, B->metas.scalar.norm );
  if( magprod > 0 ) {
    float cosine = dotprod >= magprod ? 1.0f : _vxsim_divide_quantized( dotprod, magprod ); // cap it at 1.0 (in case of anomalies)
    return cosine;
  }
  else {
    return 0.0f;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __jaccard_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  vector_feature_t *a_elem = (vector_feature_t*)CALLABLE(A)->Elements(A);
  vector_feature_t *b_elem = (vector_feature_t*)CALLABLE(B)->Elements(B);
  vector_feature_t *a_end = a_elem + A->metas.vlen;
  vector_feature_t *b_end = b_elem + B->metas.vlen;
  vector_feature_t *a, *b;
  int isum_union = 0;
  int isum_isect = 0;
  int imagA, imagB;
  uint32_t dimA, dimB;

  // Sum of intersection frequencies (min freq)
  // and sum of A.union(B) frequencies (max freq)
  a = a_elem;
  while( a < a_end && a->data != 0 ) {  // note: terminate loop on first 0-dimension
    dimA = a->dim;
    dimB = 0;
    b = b_elem;
    while( b < b_end && (dimB = b->dim) != 0 && dimB != dimA ) {
      b++; // scan until dim match
    }
    imagA = _vxsim_magnitude_map[ a->mag ];
    if( dimA == dimB ) { // dim match
      imagB = _vxsim_magnitude_map[ b->mag ];
      isum_isect += imagA < imagB ? imagA : imagB; // min
      isum_union += imagA < imagB ? imagB : imagA; // max
    }
    else { // no dim match
      isum_union += imagA;
    }
    a++;
  }

  if( isum_isect > 0 ) {
    // Sum of B.difference(A) frequencies
    b = b_elem;
    while( b < b_end && b->data != 0 ) {
      dimB = b->dim;
      dimA = 0;
      a = a_elem;
      while( a < a_end && (dimA = a->dim) != 0 && dimA != dimB ) {
        a++; // scan until dim match
      }
      if( dimA != dimB ) { // no dim match
        isum_union += _vxsim_magnitude_map[ b->mag ];
      }
      b++;
    }
    if( isum_union > 0 ) {
      float mag_isect = _vxsim_magnitude_from_imag( isum_isect );
      float mag_union = _vxsim_magnitude_from_imag( isum_union );
      float jaccard = isum_union <= isum_isect ? 1.0f : _vxsim_divide_quantized( mag_isect, mag_union ); // cap it at 1.0 (in case of anomalies)
      return jaccard;
    }
    else {
      return 0.0f;
    }
  }
  else {
    return 0.0f;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __jaccard_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  ext_vector_feature_t *a_elem = (ext_vector_feature_t*)CALLABLE(A)->Elements(A);
  ext_vector_feature_t *b_elem = (ext_vector_feature_t*)CALLABLE(B)->Elements(B);
  ext_vector_feature_t *a_end = a_elem + A->metas.vlen;
  ext_vector_feature_t *b_end = b_elem + B->metas.vlen;
  ext_vector_feature_t *a, *b;
  float sum_union = 0.0f;
  float sum_isect = 0.0f;
  const char *dimA, *dimB;
  int diff = 0;

  // Sum of intersection frequencies (min freq)
  // and sum of A.union(B) frequencies (max freq)
  a = a_elem;
  while( a < a_end && *(dimA = a->term) != '\0' ) {  // note: terminate loop on first 0-dimension
    b = b_elem;
    while( b < b_end && *(dimB = b->term) != '\0' && (diff=strncmp(dimB, dimA, MAX_FEATURE_VECTOR_TERM_LEN)) != 0 ) {
      b++; // scan until dim match
    }
    if( !diff ) { // dim match
      sum_isect += a->weight < b->weight ? a->weight : b->weight; // min
      sum_union += a->weight < b->weight ? b->weight : a->weight; // max
    }
    else { // no dim match
      sum_union += a->weight;
    }
    a++;
  }

  if( sum_isect > 0 ) {
    // Sum of B.difference(A) frequencies
    b = b_elem;
    while( b < b_end && *(dimB = b->term) != '\0' ) {
      a = a_elem;
      while( a < a_end && *(dimA = a->term) != '\0' && (diff=strncmp(dimB, dimA, MAX_FEATURE_VECTOR_TERM_LEN)) != 0 ) {
        a++; // scan until dim match
      }
      if( diff ) { // no dim match
        sum_union += b->weight;
      }
      b++;
    }
    if( sum_union > 0 ) {
      float jaccard = sum_union <= sum_isect ? 1.0f : _vxsim_divide_quantized( sum_isect, sum_union ); // cap it at 1.0 (in case of anomalies)
      return jaccard;
    }
    else {
      return 0.0f;
    }
  }
  else {
    return 0.0f;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int8_t __intersect_internal_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  vector_feature_t *a_elem = (vector_feature_t*)CALLABLE(A)->Elements(A);
  vector_feature_t *b_elem = (vector_feature_t*)CALLABLE(B)->Elements(B);
  vector_feature_t *a_end = a_elem + A->metas.vlen;
  vector_feature_t *b_end = b_elem + B->metas.vlen;
  vector_feature_t *a, *b;
  int8_t ndim = 0;

  a = a_elem;
  while( a < a_end && a->data != 0 ) {
    b = b_elem;
    while( b < b_end && b->data != 0 ) {
      if( a->dim == b->dim ) {
        ndim++;
        break;
      }
      b++;
    }
    a++;
  }
  return ndim;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int8_t __intersect_external_map( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  ext_vector_feature_t *a_elem = (ext_vector_feature_t*)CALLABLE(A)->Elements(A);
  ext_vector_feature_t *b_elem = (ext_vector_feature_t*)CALLABLE(B)->Elements(B);
  ext_vector_feature_t *a_end = a_elem + A->metas.vlen;
  ext_vector_feature_t *b_end = b_elem + B->metas.vlen;
  ext_vector_feature_t *a, *b;
  int8_t ndim = 0;

  a = a_elem;
  while( a < a_end && *a->term != '\0' ) {
    b = b_elem;
    while( b < b_end && *b->term != '\0' ) {
      if( strncmp( a->term, b->term, MAX_FEATURE_VECTOR_TERM_LEN ) == 0 ) {
        ndim++;
        break;
      }
      b++;
    }
    a++;
  }
  return ndim;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Similarity_RegisterClass( void ) {


  ASSERT_TYPE_SIZE( vgx_Similarity_t,                     3 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_Similarity_config_t,              5 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_Similarity_fingerprint_config_t,  1 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_Similarity_vector_config_t,       3 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_Similarity_threshold_config_t,    1 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_Similarity_value_t,               2 * sizeof( QWORD ) );

  COMLIB_REGISTER_CLASS( vgx_Similarity_t, CXLIB_OBTYPE_MANAGER, &Similarity_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Similarity_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Similarity_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int vgx_SIM_INIT( void ) { 
  if( !g_initialized && ++g_initialized == 1 ) {
    SET_EXCEPTION_CONTEXT
    vgx_Similarity_RegisterClass();
    vgx_Fingerprinter_RegisterClass();
    vgx_Vector_RegisterClass();
    return 1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void vgx_SIM_DESTROY( void ) { 
  if( g_initialized == 1 ) {
    vgx_Vector_UnregisterClass();
    vgx_Fingerprinter_UnregisterClass();
    vgx_Similarity_UnregisterClass();
    g_initialized = 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxsim__compare_vectors( const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  return Similarity_cmp_vectors( NULL, A, B );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t Similarity_serialize( vgx_Similarity_t *self, CStringQueue_t *out_queue ) {
  return 0; // not supported. TODO: support.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Similarity_t * Similarity_deserialize( CStringQueue_t *in_queue ) {
  return NULL; // not supported. TODO: support.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Similarity_t * Similarity_constructor( const void *identifier, vgx_Similarity_constructor_args_t *args ) {
  vgx_Similarity_t *self;
  const vgx_Similarity_config_t *config = NULL;
  vgx_Graph_t *graph = args->parent;

  CString_t *CSTR__sim_dat = NULL;
  CQwordQueue_t *__INPUT = NULL;
  int64_t n = 0;
  const char *persistent_path = NULL;

  bool euclidean = igraphfactory.EuclideanVectors();
  bool feature = igraphfactory.FeatureVectors();

  // Default: Euclidean
  if( !euclidean && !feature ) {
    euclidean = true;
  }

  XTRY {

    if( identifier ) {
      WARN( 0xC01, "identifier not supported" );
    }

    // [1] [2] Create and initialize object (zero data, set vtable and typeinfo)
    CALIGNED_MALLOC_THROWS( self, vgx_Similarity_t, 0xC02 );
    memset( self, 0, sizeof(vgx_Similarity_t) );
    if( COMLIB_OBJECT_INIT( vgx_Similarity_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC03 );
    }

    // [3] Parent graph
    self->parent = graph;

    // [5] CString_t constructor
    self->cstring_construct = (f_CString_constructor_t)COMLIB_CLASS_CONSTRUCTOR( CString_t );

    if( self->parent ) {
      persistent_path = CALLABLE(self->parent)->FullPath(self->parent);
    }

    // EUCLIDEAN VECTORS
    if( euclidean ) {
      if( (self->fingerprinter = COMLIB_OBJECT_NEW( vgx_Fingerprinter_t, NULL, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC04 );
      }
      self->dimension_allocator_context = NULL;

      // [9] Internal euclidean vector allocator
      if( (self->int_vector_allocator = ivectoralloc.NewInternalEuclidean( self, "Internal Persistent Euclidean Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC08 );
      }

      // [10] External euclidean vector allocator
      if( (self->ext_vector_allocator = ivectoralloc.NewExternalEuclidean( self, "External Persistent Euclidean Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0A );
      } 

      // [15] Internal euclidean vector ephemeral allocator (to serve internal vector allocation during readonly mode)
      if( (self->int_vector_ephemeral_allocator = ivectoralloc.NewInternalEuclideanEphemeral( self, "Internal Ephemeral Euclidean Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0B );
      }

      // [16] External euclidean vector ephemeral allocator (to serve external vector allocation during readonly mode)
      if( (self->ext_vector_ephemeral_allocator = ivectoralloc.NewExternalEuclideanEphemeral( self, "External Ephemeral Euclidean Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0C );
      } 

    }
    // FEATURE VECTORS
    else if( feature ) {
      // [4] Fingerprinter
      vgx_Fingerprinter_constructor_args_t fpargs = {
        .nsegm = self->params.fingerprint.nsegm,
        .nsign = self->params.fingerprint.nsign,
        .dimension_encoder = __encode_and_own_vecelem_CS,
        .parent = self
      };
      if( (self->fingerprinter = COMLIB_OBJECT_NEW( vgx_Fingerprinter_t, NULL, &fpargs )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC04 );
      }

      // [6] dimension allocator context
      if( (self->dimension_allocator_context = icstringalloc.NewContext( self->parent, NULL, NULL, persistent_path, "vector/dimension/data" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC05 );
      }
      if( persistent_path ) {
        if( icstringalloc.RestoreObjects( self->dimension_allocator_context ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xC06 );
        }
      }

      // [7 + 8] Vector dimension enumerator (dim_encoder + dim_decoder)
      if( _vxenum_dim__create_enumerator( self ) < 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xC07, "Failed to create graph vector dimension enumerator" );
      }

      // [9] Internal feature vector allocator
      if( (self->int_vector_allocator = ivectoralloc.NewInternalMap( self, "Internal Persistent Feature Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC08 );
      }

      // [10] External feature vector allocator
      if( (self->ext_vector_allocator = ivectoralloc.NewExternalMap( self, "External Persistent Feature Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0A );
      } 

      // [15] Internal feature vector ephemeral allocator (to serve internal vector allocation during readonly mode)
      if( (self->int_vector_ephemeral_allocator = ivectoralloc.NewInternalMapEphemeral( self, "Internal Ephemeral Feature Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0B );
      }

      // [16] External feature vector ephemeral allocator (to serve external vector allocation during readonly mode)
      if( (self->ext_vector_ephemeral_allocator = ivectoralloc.NewExternalMapEphemeral( self, "External Ephemeral Feature Vectors" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0C );
      } 

    }
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0D );
    }

    if( persistent_path ) {
      if( CALLABLE( self->int_vector_allocator )->RestoreObjects( self->int_vector_allocator ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0E );
      }
    }

    // [11] nullvector
    self->nullvector = NULL;

    // [13] readonly
    self->readonly = 0;

    // [14] Sim value cache
    memset( &self->value.bits, 0, sizeof( vgx_Similarity_value_t ) );

    // [17] Sim config
    memset( &self->params.qwords, 0, sizeof( vgx_Similarity_config_t ) );

    // RESTORE
    if( persistent_path ) {
      if( (n = __deserialize_similarity( self )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC0F );
      }
    }

    // Nothing was restored from disk - use defaults
    if( n == 0 ) {
      // [11] Null Vector
      if( (self->nullvector = ivectorobject.New( self, VECTOR_TYPE_NULL, 0, false )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC10 );
      }

      // [17] Copy parameters
      if( args->params ) {
        config = args->params;
      }
      else if( euclidean ) {
        config = &DEFAULT_EUCLIDEAN_SIMCONFIG;
      }
      else if( feature ) {
        config = &DEFAULT_FEATURE_SIMCONFIG;
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC11 );
      }
      memcpy( &self->params, config, sizeof(vgx_Similarity_config_t) );
    }
  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
    if( CSTR__sim_dat ) {
      CStringDelete( CSTR__sim_dat );
    }
    if( __INPUT ) {
      COMLIB_OBJECT_DESTROY( __INPUT );
    }
  }

  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Similarity_destructor( vgx_Similarity_t *self ) {
  if( self ) {

    // First delete nullvector if we're the only owner
    if( CALLABLE(self->nullvector)->Refcnt(self->nullvector) == 1 ) {
      ivectorobject.Delete( self->nullvector );
    }

    // Verify no allocated vector objects
    cxmalloc_family_t *A[] = {
      self->ext_vector_ephemeral_allocator,
      self->int_vector_ephemeral_allocator,
      self->ext_vector_allocator,
      self->int_vector_allocator,
      NULL
    };
    int64_t n = 0;
    for( cxmalloc_family_t **a = A; *a != NULL; ++a ) {
      n += CALLABLE(*a)->Active(*a);
    }
    if( n > 0 ) {
      WARN( 0x000, "Cannot delete similarity context with %lld allocated vector objects", n );
      return;
    }

    // [17] Sim config
    memset( &self->params.qwords, 0, sizeof( vgx_Similarity_config_t ) );

    // [16] External vector ephemeral allocator
    ivectoralloc.Delete( &self->ext_vector_ephemeral_allocator );

    // [15] Internal vector ephemeral allocator
    ivectoralloc.Delete( &self->int_vector_ephemeral_allocator );

    // [14] Sim value cache
    memset( &self->value.bits, 0, sizeof( vgx_Similarity_value_t ) );

    // [13] readonly
    self->readonly = 0;

    // [11] Null Vector
    self->nullvector = NULL;

    // [10] External vector allocator
    ivectoralloc.Delete( &self->ext_vector_allocator );

    // [9] Internal vector allocator
    ivectoralloc.Delete( &self->int_vector_allocator );
    // [8 + 7] Vector dimension enumerator
    _vxenum_dim__destroy_enumerator( self );

    // [6] dimension allocator context
    icstringalloc.DeleteContext( &self->dimension_allocator_context );

    // [5] CString_t constructor
    self->cstring_construct = NULL;

    // [4]
    if( self->fingerprinter ) {
      COMLIB_OBJECT_DESTROY( self->fingerprinter );
      self->fingerprinter = NULL;
    }

    // [3] Parent graph
    self->parent = NULL;

    // [2]
    // [1]
    ALIGNED_FREE( self );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Similarity_t * Similarity_clone( vgx_Similarity_t *self ) {
  vgx_Similarity_t *clone = COMLIB_OBJECT_CLONE( vgx_Similarity_t, self );
  return clone;
}



/*******************************************************************//**
 *
 * Create new internal vector from data elements.
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_internal_vector( vgx_Similarity_t *self, const void *elements, float scale, uint16_t sz, bool ephemeral ) {
  vector_type_t vtype;
  if( igraphfactory.EuclideanVectors() ) {
    vtype = VECTOR_TYPE_INTERNAL_EUCLIDEAN;
  }
  else if( igraphfactory.FeatureVectors() ) {
    vtype = VECTOR_TYPE_INTERNAL_FEATURE;
  }
  else {
    return NULL;
  }

  vgx_Vector_constructor_args_t vargs = {
    .vlen           = sz,
    .ephemeral      = ephemeral,
    .type           = vtype,
    .elements       = elements,
    .scale          = scale,
    .simcontext     = self
  };

  return COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs );
}



/*******************************************************************//**
 *
 * Create new external vector from data elements.
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_external_vector( vgx_Similarity_t *self, const void *elements, uint16_t sz, bool ephemeral ) {
  vector_type_t vtype;
  if( igraphfactory.EuclideanVectors() ) {
    vtype = VECTOR_TYPE_EXTERNAL_EUCLIDEAN;
  }
  else if( igraphfactory.FeatureVectors() ) {
    vtype = VECTOR_TYPE_EXTERNAL_FEATURE;
  }
  else {
    return NULL;
  }

  vgx_Vector_constructor_args_t vargs = {
    .vlen           = sz,
    .ephemeral      = ephemeral,
    .type           = vtype,
    .elements       = elements,
    .scale          = 1.0f,
    .simcontext     = self
  };

  return COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs );
}



/*******************************************************************//**
 *
 * Create new internal vector from external data elements.
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_internal_vector_from_external_elements( vgx_Similarity_t *self, const void *external_elements, uint16_t sz, bool ephemeral, CString_t **CSTR__error ) {
  vgx_Vector_t *vector = NULL;
  uint16_t vlen = sz;
  vector_type_t vtype;
  bool euclidean = igraphfactory.EuclideanVectors();
  if( euclidean ) {
#if defined CXPLAT_ARCH_X64
#ifdef __cxlib_AVX512_MINIMUM__
    vlen = ceilmultpow2( sz, sizeof( __m512i ) );
#else
    vlen = ceilmultpow2( sz, sizeof( __m256i ) );
#endif
#elif defined CXPLAT_ARCH_ARM64
    vlen = ceilmultpow2( sz, sizeof( uint64x2_t ) );
#else
#error "Unsupported architecture"
#endif
    vtype = VECTOR_TYPE_INTERNAL_EUCLIDEAN;
  }
  else if( igraphfactory.FeatureVectors() ) {
    vtype = VECTOR_TYPE_INTERNAL_FEATURE;
  }
  else {
    __set_error_string( CSTR__error, "unknown vector mode" );
    return NULL;
  }


  vgx_Vector_constructor_args_t vargs = {
    .vlen           = vlen,
    .ephemeral      = ephemeral,
    .type           = vtype,
    .elements       = NULL,
    .scale          = 1.0f, // will compute below
    .simcontext     = self
  };

  // First create new internal vector without elements set
  if( (vector = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs )) != NULL ) {
    if( euclidean ) {
      char *elements = CALLABLE(vector)->Elements(vector);
      char *end = elements + vargs.vlen;
      char *cursor = elements;
      const float *xelem = (float*)external_elements;
      const float *xend = xelem + sz; // end of supplied elements
      double factor = __euclidean_scaling_factor( xelem, sz );
      vector->metas.scalar.factor = (float)factor;
      double inv_scale = factor > 0.0 ? 1.0/factor : 0.0;

      // Populate internal vector from external elements
      while( xelem < xend ) {
        *cursor++ = __encode_float_to_char( *xelem++, inv_scale );
      }
      // Zero-fill trailing slots not supplied
      while( cursor < end ) {
        *cursor++ = 0;
      }

      vector->metas.flags.pop = 1;

      // Compute fingerprint
      vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, vector, 0, NULL );
    }
    else {

      vector_feature_t *elements = (vector_feature_t*)CALLABLE(vector)->Elements(vector);
      vector_feature_t *end = elements + vargs.vlen;
      vector_feature_t *cursor = elements;
      unsigned sum_sq_imag = 0;

      // ... then populate internal vector from external elements
      const ext_vector_feature_t *xelem = external_elements;
      bool encoded_ok = false;
      if( self->parent ) {
        vgx_Graph_t *graph = self->parent;
        GRAPH_LOCK( graph ) {
          if( _vxenum_dim__remain_CS( self ) >= vector->metas.vlen ) {
            BEGIN_GRAPH_COMMIT_GROUP_CS( graph ) {
              while( cursor < end ) {
                *cursor++ = __encode_and_own_vecelem_CS( self, xelem++ );
              }
            } END_GRAPH_COMMIT_GROUP_CS;
            encoded_ok = true;
          }
        } GRAPH_RELEASE;
      }
      else {
        if( _vxenum_dim__remain_CS( self ) >= vector->metas.vlen ) {
          while( cursor < end ) {
            *cursor++ = __encode_and_own_vecelem_CS( self, xelem++ );
          }
          encoded_ok = true;
        }
      }

      // Dimension encoding completed
      if( encoded_ok ) {
        vector->metas.flags.pop = 1;

        // Compute magnitude
        cursor = elements;
        while( cursor < end ) {
          sum_sq_imag += _vxsim_square_magnitude_map[ (cursor++)->mag ];
        }
        vector->metas.scalar.norm = (float)sqrt( _vxsim_square_magnitude_from_isqmag( sum_sq_imag ) );

        // Compute fingerprint
        vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, vector, 0, NULL );
      }
      // Cannot encode dimensions, abort
      else {
        COMLIB_OBJECT_DESTROY( vector );
        vector = NULL;
        if( _vxenum_dim__remain_OPEN( self ) < vargs.vlen ) {
          if( CSTR__error ) {
            *CSTR__error = CStringNew( "Dimension enumeration space exhausted" );
          }
        }
      }
    }
  }
  else if( CSTR__error ) {
    *CSTR__error = CStringNew( "Vector constructor failed" );
  }

  return vector;
}



/*******************************************************************//**
 *
 * Create new, empty internal vector of specified length
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_empty_internal_vector( vgx_Similarity_t *self, uint16_t vlen, bool ephemeral ) {
  vector_type_t vtype;
  bool euclidean = igraphfactory.EuclideanVectors();
  if( euclidean ) {
    vtype = VECTOR_TYPE_INTERNAL_EUCLIDEAN;
  }
  else if( igraphfactory.FeatureVectors() ) {
    vtype = VECTOR_TYPE_INTERNAL_FEATURE;
  }
  else {
    return NULL;
  }

  vgx_Vector_constructor_args_t vargs = {
    .vlen           = vlen,
    .ephemeral      = ephemeral,
    .type           = vtype,
    .elements       = NULL,
    .scale          = 1.0f,
    .simcontext     = self
  };

  return COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs );
}



/*******************************************************************//**
 *
 * Create new, empty external vector of specified length
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_empty_external_vector( vgx_Similarity_t *self, uint16_t vlen, bool ephemeral ) {
  vector_type_t vtype;
  bool euclidean = igraphfactory.EuclideanVectors();
  if( euclidean ) {
    vtype = VECTOR_TYPE_EXTERNAL_EUCLIDEAN;
  }
  else if( igraphfactory.FeatureVectors() ) {
    vtype = VECTOR_TYPE_EXTERNAL_FEATURE;
  }
  else {
    return NULL;
  }

  vgx_Vector_constructor_args_t vargs = {
    .vlen           = vlen,
    .ephemeral      = ephemeral,
    .type           = vtype,
    .elements       = NULL,
    .scale          = 1.0f,
    .simcontext     = self
  };

  return COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_internalize_vector( vgx_Similarity_t *self, vgx_Vector_t *source, bool ephemeral, CString_t **CSTR__error ) {
  if( CALLABLE(source)->IsExternal(source) ) {
    vector_type_t vtype;
    bool euclidean = igraphfactory.EuclideanVectors();
    if( euclidean ) {
      vtype = VECTOR_TYPE_INTERNAL_EUCLIDEAN;
    }
    else if( igraphfactory.FeatureVectors() ) {
      vtype = VECTOR_TYPE_INTERNAL_FEATURE;
    }
    else {
      __set_error_string( CSTR__error, "unknown vector mode" );
      return NULL;
    }
    
    vgx_Vector_t *internal_vector = NULL;

    // create and return a new internal version of the external vector
    vgx_Vector_constructor_args_t vargs = {
      .vlen           = source->metas.vlen,
      .ephemeral      = ephemeral,
      .type           = vtype,
      .elements       = NULL,
      .scale          = 1.0f, // Will compute below
      .simcontext     = self
    };

    if( (internal_vector = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs )) != NULL ) {
      if( euclidean ) {
        char *elements = CALLABLE( internal_vector )->Elements( internal_vector );
        char *cursor = elements;
        float *xelem = CALLABLE( source )->Elements( source );
        float *ext_end = xelem + source->metas.vlen;

        double factor = __euclidean_scaling_factor( xelem, source->metas.vlen );
        internal_vector->metas.scalar.factor = (float)factor;
        double inv_scale = factor > 0.0 ? 1.0/factor : 0.0;

        while( xelem < ext_end ) {
          *cursor++ = __encode_float_to_char( *xelem++, inv_scale );
        }

        internal_vector->metas.flags.pop = 1;

        if( source->fp != 0 ) {
          internal_vector->fp = source->fp;
        }
        else {
          internal_vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, internal_vector, 0, NULL );
        }
      }
      else {
        vector_feature_t *elements = (vector_feature_t*)CALLABLE( internal_vector )->Elements( internal_vector );
        vector_feature_t *cursor = elements;
        ext_vector_feature_t *ext_elem = (ext_vector_feature_t*)CALLABLE( source )->Elements( source );
        ext_vector_feature_t *ext_end = ext_elem + source->metas.vlen;
        bool encoded_ok = false;
        if( self->parent ) {
          GRAPH_LOCK( self->parent ) {
            if( _vxenum_dim__remain_CS( self ) >= source->metas.vlen ) {
              while( ext_elem < ext_end ) {
                *cursor++ = __encode_and_own_vecelem_CS( self, ext_elem++ );
              }
              encoded_ok = true;
            }
          } GRAPH_RELEASE;
        }
        else {
          if( _vxenum_dim__remain_CS( self ) >= source->metas.vlen ) {
            while( ext_elem < ext_end ) {
              *cursor++ = __encode_and_own_vecelem_CS( self, ext_elem++ );
            }
            encoded_ok = true;
          }
        }

        if( encoded_ok ) {
          internal_vector->metas.flags.pop = 1;
          internal_vector->metas.scalar.norm = source->metas.scalar.norm;
          if( source->fp != 0 ) {
            internal_vector->fp = source->fp;
          }
          else {
            internal_vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, internal_vector, 0, NULL );
          }
        }
        else {
          COMLIB_OBJECT_DESTROY( internal_vector );
          internal_vector = NULL;
          if( _vxenum_dim__remain_OPEN( self ) < vargs.vlen ) {
            if( CSTR__error ) {
              *CSTR__error = CStringNew( "Dimension enumeration space exhausted" );
            }
          }
        }
      }
    }
    else if( CSTR__error ) {
      *CSTR__error = CStringNew( "Vector constructor failed" );
    }

    return internal_vector;
  }
  else {
    // source already internal vector, return a new reference
    CALLABLE( source )->Incref( source );
    return source;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_externalize_vector( vgx_Similarity_t *self, vgx_Vector_t *source, bool ephemeral ) {
  if( !CALLABLE(source)->IsExternal(source) ) {
    vector_type_t vtype;
    bool euclidean = igraphfactory.EuclideanVectors();
    if( euclidean ) {
      vtype = VECTOR_TYPE_EXTERNAL_EUCLIDEAN;
    }
    else if( igraphfactory.FeatureVectors() ) {
      vtype = VECTOR_TYPE_EXTERNAL_FEATURE;
    }
    else {
      return NULL;
    }

    // create and return a new external version of the internal vector
    vgx_Vector_constructor_args_t vargs = {
      .vlen           = source->metas.vlen,
      .ephemeral      = ephemeral,
      .type           = vtype,
      .elements       = NULL,
      .scale          = 1.0f,
      .simcontext     = self
    };

    vgx_Vector_t *external_vector = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs );
    if( external_vector == NULL ) {
      return NULL; // error
    }

    if( euclidean ) {
      float *ext_elem = CALLABLE( external_vector )->Elements( external_vector );
      const BYTE *elem = CALLABLE( source )->Elements( source );
      const char *src = (char*)elem; 
      const char *end = src + source->metas.vlen;
      double factor = source->metas.scalar.factor; 
      while( src < end ) {
        *ext_elem++ = __decode_char_to_float( *src++, factor );
      }
      external_vector->metas.flags.pop = 1;
      external_vector->metas.scalar.norm = (float)(sqrt( vxeval_bytearray_sum_squares( elem, source->metas.vlen ) ) * factor);

      if( source->fp != 0 ) {
        external_vector->fp = source->fp;
      }
      else {
        external_vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, source, 0, NULL );
      }
    }
    else {
      ext_vector_feature_t *ext_elem = (ext_vector_feature_t*)CALLABLE( external_vector )->Elements( external_vector );
      const vector_feature_t *elem = (vector_feature_t*)CALLABLE( source )->Elements( source );
      const vector_feature_t *end = elem + source->metas.vlen;
      if( self->parent ) {
        GRAPH_LOCK( self->parent ) {
          while( elem < end ) {
            __decode_vecelem_CS( self, ext_elem++, *elem++ );
          }
        } GRAPH_RELEASE;
      }
      else {
        while( elem < end ) {
          __decode_vecelem_CS( self, ext_elem++, *elem++ );
        }
      }
      external_vector->metas.flags.pop = 1;
      external_vector->metas.scalar.norm = source->metas.scalar.norm;
      if( source->fp != 0 ) {
        external_vector->fp = source->fp;
      }
      else {
        external_vector->fp = CALLABLE(self->fingerprinter)->Compute( self->fingerprinter, source, 0, NULL );
      }
    }
    return external_vector;
  }
  else {
    // source already external vector, return a new reference
    CALLABLE( source )->Incref( source );
    return source;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_translate_vector( vgx_Similarity_t *self, vgx_Vector_t *src, bool ephemeral, CString_t **CSTR__error ) {
  if( (src->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    return Similarity_internalize_vector( self, src, ephemeral, CSTR__error );
  }
  else {
    return Similarity_externalize_vector( self, src, ephemeral );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_Vector_t * __object_vector( const vgx_Comparable_t pobj ) {
  if( vgx_Vector_t_CheckExact( pobj ) ) {
    return (vgx_Vector_t*)pobj;
  }
  else if( vgx_Vertex_t_CheckExact( pobj ) ) {
    vgx_Vertex_t *vtx = (vgx_Vertex_t*)pobj;
    return vtx->vector;
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
static int Similarity_hamming_distance( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );
  if( v1 && v2 ) {
    return CALLABLE(self->fingerprinter)->Distance( self->fingerprinter, v1->fp, v2->fp );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * Compute Euclidean distance
 *
 *
 ***********************************************************************
 */
static float Similarity_euclidean_distance( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );

  if( v1 == NULL || v2 == NULL ) {
    return INFINITY;
  }

  // TODO?  Allow mixed vector types?  ext/int

  float distance;

  if( v1->metas.flags.compat.bits == v2->metas.flags.compat.bits ) {
    if( igraphfactory.EuclideanVectors() ) {
      if( v1->metas.flags.ext ) {
        distance = __distance_external_euclidean( v1, v2 );
      }
      else {
        distance = __distance_internal_euclidean( v1, v2 );
      }
    }
    else {
      if( v1->metas.flags.ext ) {
        distance = INFINITY; // ???
      }
      else {
        distance = INFINITY; // ???
      }
    }
  }
  else {
    if( v1->metas.flags.nul ) {
      distance = CALLABLE( v2 )->Magnitude( v2 );
    }
    else if( v2->metas.flags.nul ) {
      distance = CALLABLE( v1 )->Magnitude( v1 );
    }
    else {
      return -1.0f; // incompatible vectors
    }
  }
  // ??? TODO: Add euclidean member to self->value ?
  self->value.valid = 1;
  return distance;
}



/*******************************************************************//**
 *
 * Compute cosine similarity as (A dot B) / (|A|*|B|)
 *
 * NOTE2: Vectors MUST have unique dimensions.
 *        Cosine will be undefined if duplicate dimensions exist!
 *        (TODO: Dimension aliasing could produce duplicate dimensions. Fix this using the new dimension enumerator map.)
 *
 ***********************************************************************
 */
static float Similarity_cosine( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );
  if( v1 == NULL || v2 == NULL ) {
    return -1.0f;
  }

  // TODO?  Allow mixed vector types?  ext/int

  if( v1->metas.flags.compat.bits == v2->metas.flags.compat.bits ) {
    if( igraphfactory.EuclideanVectors() ) {
      if( v1->metas.flags.ext ) {
        self->value.cosine = __cosine_external_euclidean( v1, v2 );
      }
      else {
        self->value.cosine = __cosine_internal_euclidean( v1, v2 );
      }
    }
    else {
      if( v1->metas.flags.ext ) {
        self->value.cosine = __cosine_external_map( v1, v2 );
      }
      else {
        self->value.cosine = __cosine_internal_map( v1, v2 );
      }
    }
  }
  else if( v1->metas.flags.compat_nul.bits == v2->metas.flags.compat_nul.bits ) { // <- one or both null vector since compat bits were different
    return 0.0f;
  }
  else {
    self->value.cosine = -1.0f; // incompatible vectors
  }
  self->value.valid = 1;
  return self->value.cosine;
}



/*******************************************************************//**
 *
 * Compute Jaccard index.
 *
 *
 * NOTE1: Vectors MUST have unique dimensions.
 *        Jaccard will be undefined if duplicate dimensions exist!
 *        (TODO: Dimension aliasing could produce duplicate dimensions. Fix this using the new dimension enumerators. )
 *
 ***********************************************************************
 */
static float Similarity_jaccard( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );
  if( v1 == NULL || v2 == NULL ) {
    return -1.0f;
  }

  if( v1->metas.flags.compat.bits == v2->metas.flags.compat.bits ) {
    if( igraphfactory.EuclideanVectors() ) {
      if( v1->metas.flags.ext ) {
        self->value.jaccard = __jaccard_external_euclidean( v1, v2 );
      }
      else {
        self->value.jaccard = __jaccard_internal_euclidean( v1, v2 );
      }
    }
    else {
      if( v1->metas.flags.ext ) {
        self->value.jaccard = __jaccard_external_map( v1, v2 );
      }
      else {
        self->value.jaccard = __jaccard_internal_map( v1, v2 );
      }
    }
  }
  else {
    self->value.jaccard = -1.0f; // incompatible vectors
  }
  self->value.valid = 1;
  return self->value.jaccard;
}



/*******************************************************************//**
 * Count number of common dimensions in vectors A and B.
 *
 * NOTE1: Vectors MUST have unique dimensions.
 *        Intersection will be undefined (incorrect) if duplicate dimensions exist!
 *        (TODO: Dimension aliasing could produce duplicate dimensions. Fix this using the new dimension enumerators. )* 
 ***********************************************************************
 */
static int8_t Similarity_intersect( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );

  if( v1 == NULL || v2 == NULL ) {
    return -1;
  }

  if( v1->metas.flags.compat.bits == v2->metas.flags.compat.bits ) {
    if( igraphfactory.EuclideanVectors() ) {
      if( v1->metas.flags.ext ) {
        self->value.intersect = __intersect_external_euclidean( v1, v2 );
      }
      else {
        self->value.intersect = __intersect_internal_euclidean( v1, v2 );
      }
    }
    else {
      if( v1->metas.flags.ext ) {
        self->value.intersect = __intersect_external_map( v1, v2 );
      }
      else {
        self->value.intersect = __intersect_internal_map( v1, v2 );
      }
    }
  }
  else {
    self->value.intersect = -1; // incompatible vectors
  }
  self->value.valid = 1;
  return self->value.intersect;
}



/******************************************************//**
 * Compute composite similarity measure as defined by configuration
 *    MIN_ISECT : if > 1 and len(A.intersection(B)) < MIN_ISECT then similarity is 0
 *    MIN_COS   : if COS_EXP>0 and cos(A,B)<MIN_COS then similarity is 0
 *    MIN_JAC   : if JAC_EXP>0 and jac(A,B)<MIN_JAC then similarity is 0
 *    COS_EXP   : ignored if JAC_EXP is 0 and then similarity = cos(A,B)
 *    JAC_EXP   : ignored if COS_EXP is 0 and then similarity = jac(A,B)
 *
 *    If similarity is not 0 or otherwise computed by above special cases
 *    similarity = cos(A,B)**COS_EXP * jac(A,B)**JAC_EXP
 *
 * NOTE1: Feature vectors MUST have unique dimensions.
 *        Cosine will be undefined if duplicate dimensions exist!
 *        (TODO: Dimension aliasing could produce duplicate dimensions. Fix this.)
 *
 ***********************************************************************
 */
static float Similarity_similarity( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {

  if( igraphfactory.EuclideanVectors() ) {
    return Similarity_cosine( self, A, B );
  }


  vgx_Similarity_vector_config_t *cf = &self->params.vector;
  vgx_Similarity_value_t *val = &self->value;

  // Reset
  #if defined CXPLAT_ARCH_X64
  static const __m128i zero = {0};
  _mm_store_si128( &val->bits, zero );
  #else
  memset( &val->bits, 0, sizeof(__m128i));
  #endif

  // Intersect
  if( cf->min_intersect > 1 ) {
    if( Similarity_intersect( self, A, B ) < cf->min_intersect ) {
      return val->similarity = 0.0f;
    }
  }

  // Jaccard
  if( cf->jaccard_exponent > 0.0f ) {
    if( Similarity_jaccard( self, A, B ) < cf->min_jaccard ) {
      return val->similarity = 0.0f;
    }
    if( !(cf->cosine_exponent > 0.0f) ) {
      return val->similarity = val->jaccard;
    }
  }

  // Cosine
  if( cf->cosine_exponent > 0.0f ) {
    if( Similarity_cosine( self, A, B ) < cf->min_cosine ) {
      return val->similarity = 0.0f;
    }
    if( !(cf->jaccard_exponent > 0.0f) ) {
      return val->similarity = val->cosine;
    }
  }

  self->value.valid = 1;
  return val->similarity = _vxsim_multiply_quantized( (float)pow( val->jaccard, cf->jaccard_exponent ), (float)pow( val->cosine, cf->cosine_exponent ) );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool Similarity_valid( vgx_Similarity_t *self ) {
  return self->value.valid ? true : false;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void Similarity_clear( vgx_Similarity_t *self ) {
  self->value.valid = 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Similarity_value_t * Similarity_value( vgx_Similarity_t *self ) {
  return &self->value;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int Similarity_match( vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B ) {
  vgx_Vector_t *v1 = __object_vector( A );
  vgx_Vector_t *v2 = __object_vector( B );
  if( v1 && v2 ) {
    if( igraphfactory.EuclideanVectors() ) {
      if( !(Similarity_cosine( self, A, B ) < self->params.threshold.similarity ) ) {
        return 1;
      }
    }
    else {
      // First: low-cost fingerprint distance match
      if( CALLABLE(self->fingerprinter)->Distance( self->fingerprinter, v1->fp, v2->fp ) <= self->params.threshold.hamming ) {
        // Second: costlier vector similarity
        if( !(Similarity_similarity( self, A, B ) < self->params.threshold.similarity) ) {
          return 1;
        }
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Vector_t * Similarity_vector_arithmetic( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B, bool subtract, CString_t **CSTR__error ) {
  vgx_Vector_t *vector = NULL;

  if( A == NULL || B == NULL || !igraphfactory.EuclideanVectors()) {
    __set_error_string( CSTR__error, "must be Euclidean vectors" ); 
    return NULL;
  }

  if( A->metas.flags.compat.bits != B->metas.flags.compat.bits ) {
    if( A->metas.flags.compat_nul.bits == B->metas.flags.compat_nul.bits ) { // <- one or both null vector since compat bits were different
      if( A->metas.flags.nul ) {
        return CALLABLE(B)->OwnOrClone(B, true);
      }
      return CALLABLE(A)->OwnOrClone(A, true);
    }
    __set_error_string( CSTR__error, "incompatible vectors" ); 
    return NULL;
  }

  if( A->metas.vlen != B->metas.vlen ) {
    __set_error_string( CSTR__error, "vectors must have same length" ); 
    return NULL;
  }

  if( A->metas.flags.ext ) {
  }
  else {
    const char *a = CALLABLE(A)->Elements(A);
    const char *b = CALLABLE(B)->Elements(B);

    float fA = A->metas.scalar.factor;
    float fB = B->metas.scalar.factor;

    float *aggr = malloc( sizeof(float) * A->metas.vlen );
    if( aggr ) {
      const char *pa = a;
      const char *pb = b;
      float *pf = aggr;
      float *end = aggr + A->metas.vlen;
      if( subtract ) {
        while( pf < end ) {
          *pf++ = *pa++ * fA - *pb++ * fB;
        }
      }
      else {
        while( pf < end ) {
          *pf++ = *pa++ * fA + *pb++ * fB;
        }
      }

      vector = CALLABLE(self)->NewInternalVectorFromExternal( self, aggr, A->metas.vlen, true, NULL );

      free( aggr );
    }

  }

  return vector;

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Vector_t * Similarity_vector_scalar_multiply( vgx_Similarity_t *self, const vgx_Vector_t *A, double factor, CString_t **CSTR__error ) {
  vgx_Vector_t *vector = NULL;

  if( A == NULL || !igraphfactory.EuclideanVectors()) {
    __set_error_string( CSTR__error, "must be Euclidean vector" ); 
    return NULL;
  }

  if( A->metas.flags.ext ) {
  }
  else {
    const char *a = CALLABLE(A)->Elements(A);

    double fA_x_f = A->metas.scalar.factor * factor;

    float *aggr = malloc( sizeof(float) * A->metas.vlen );
    if( aggr ) {
      const char *pa = a;
      float *pf = aggr;
      float *end = aggr + A->metas.vlen;
      while( pf < end ) {
        *pf++ = (float)(*pa++ * fA_x_f);
      }

      vector = CALLABLE(self)->NewInternalVectorFromExternal( self, aggr, A->metas.vlen, true, NULL );

      free( aggr );
    }

  }

  return vector;

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double Similarity_vector_dot_product( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B, CString_t **CSTR__error ) {
  if( !igraphfactory.EuclideanVectors() ) {
    __set_error_string( CSTR__error, "must be Euclidean vectors" ); 
    return NAN;
  }

  if( A->metas.flags.compat.bits != B->metas.flags.compat.bits ) {
    if( A->metas.flags.compat_nul.bits == B->metas.flags.compat_nul.bits ) { // <- one or both null vector since compat bits were different
      return 0.0;
    }
    __set_error_string( CSTR__error, "incompatible vectors" ); 
    return NAN;
  }

  double dp = 0.0;
  const void *a = ivectorobject.GetElements( (vgx_Vector_t*)A );
  const void *b = ivectorobject.GetElements( (vgx_Vector_t*)B );
  int len = minimum_value( A->metas.vlen, B->metas.vlen );
  if( A->metas.flags.ext ) {
    const float *fa = (float*)a;
    const float *fb = (float*)b;
    const float *ea = fa + len;
    double da, db;
    while( fa < ea ) {
      da = *fa++;
      db = *fb++;
      dp += da * db;
    }
  }
  else {
    dp = vxeval_bytearray_dot_product( (BYTE*)a, (BYTE*)b, len ) * CALLABLE(A)->Scaler(A) * CALLABLE(B)->Scaler(B);
  }
  return dp;
}



/*******************************************************************//**
 * Compare the two vectors and return 0 if equal, or non-zero otherwise.
 * NULL vectors allowed.
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Similarity_cmp_vectors( vgx_Similarity_t *self, const vgx_Vector_t *A, const vgx_Vector_t *B ) {
  // Handle null vectors first
  if( A == NULL ) {
    if( B != NULL ) {
      return -1;  // null A is "less than" non-null B
    }
    else {
      return 0;   // both A and B are null
    }
  }
  else if( B == NULL ) {
    return 1; // non-null A is "greater than" null B
  }

  // Compatible?
  if( A->metas.flags.compat.bits != B->metas.flags.compat.bits ) {
    if( A->metas.flags.ext ) {
      return 1;
    }
    else {
      return -1;
    }
  }
  
  // Different size?
  if( A->metas.vlen < B->metas.vlen ) {
    return -1;
  }
  if( A->metas.vlen > B->metas.vlen ) {
    return 1;
  }

  // Same data? (Length is the same)
  if( A->metas.flags.ecl ) {
    if( A->metas.flags.ext ) {
      const float *a = CALLABLE(A)->Elements(A);
      const float *b = CALLABLE(B)->Elements(B);
      const float *a_end = a + A->metas.vlen;
      while( a < a_end ) {
        if( *a < *b ) {
          return -1;
        }
        if( *a > *b ) {
          return 1;
        }
        ++a;
        ++b;
      }
    }
    else {
      const BYTE *a = CALLABLE(A)->Elements(A);
      const BYTE *b = CALLABLE(B)->Elements(B);
      const BYTE *a_end = a + A->metas.vlen;
      while( a < a_end ) {
        if( *a < *b ) {
          return -1;
        }
        if( *a > *b ) {
          return 1;
        }
        ++a;
        ++b;
      }
    }
  }
  else {
    if( A->metas.flags.ext ) {
      // external
      ext_vector_feature_t *a = (ext_vector_feature_t*)CALLABLE(A)->Elements(A);
      ext_vector_feature_t *b = (ext_vector_feature_t*)CALLABLE(B)->Elements(B);
      ext_vector_feature_t *a_end = a + A->metas.vlen;
      while( a < a_end ) {
        int cmp = strncmp( a->term, b->term, MAX_FEATURE_VECTOR_TERM_LEN );
        if( cmp < 0 ) {
          return -1;
        }
        if( cmp > 0 ) {
          return 1;
        }
        if( a->weight < b->weight ) {
          return -1;
        }
        if( a->weight > b->weight ) {
          return 1;
        }
        if( *a->term == '\0' ) {
          break; // b->term is also empty string
        }
        a++; b++;
      }
    }
    else {
      // internal
      vector_feature_t *a = (vector_feature_t*)CALLABLE(A)->Elements(A);
      vector_feature_t *b = (vector_feature_t*)CALLABLE(B)->Elements(B);
      vector_feature_t *a_end = a + A->metas.vlen;
      while( a < a_end ) {
        if( a->data < b->data ) {
          return -1;
        }
        if( a->data > b->data ) {
          return 1;
        }
        if( a->data == 0 ) {
          break; // b->data is also 0
        }
        a++; b++;
      }
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Similarity_new_centroid( vgx_Similarity_t *self, const vgx_Vector_t *vectors[], bool ephemeral ) {
  vgx_Vector_t *centroid = NULL;          // the centroid 
  vgx_Vector_constructor_args_t cargs = {0};

  XTRY {
    cargs.ephemeral = ephemeral;
    cargs.simcontext = self;
    if( igraphfactory.EuclideanVectors() ) {
      cargs.type = VECTOR_TYPE_CENTROID_EUCLIDEAN;
    }
    else if( igraphfactory.FeatureVectors() ) {
      cargs.type = VECTOR_TYPE_CENTROID_FEATURE;
    }
    else {
      return NULL;
    }
    if( centroid_constructor_args( &cargs, self->params.vector.max_size, vectors ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC11 );
    }
    if( (centroid = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &cargs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC12 );
    }
  }
  XCATCH( errcode ) {
    if( centroid ) {
      COMLIB_OBJECT_DESTROY( centroid );
      centroid = NULL;
    }
  }
  XFINALLY {
    if( cargs.elements ) {
      ALIGNED_FREE( (void*)cargs.elements );
    }
  }

  return centroid;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static CString_t * __get_new_filepath( vgx_Similarity_t *self ) {
  const char subdir[] = VGX_PATHDEF_INSTANCE_SIMILARITY;
  vgx_Graph_t *graph = self->parent;
  const char *graph_path = CALLABLE( graph )->FullPath( graph );
  CString_t *CSTR__sim_path = NULL;
  CString_t *CSTR__sim_dat = NULL;
  
  XTRY {
    // Define the similarity directory path 
    if( (CSTR__sim_path = CStringNewFormat( "%s/%s", graph_path, subdir )) != NULL ) {
      const char *sim_path = CStringValue( CSTR__sim_path );
      // Create dir if it doesn't exist
      if( !dir_exists( sim_path ) ) {
        if( create_dirs( sim_path ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0xC21 );
        }
      }
      // Define the full similarity file path
      CSTR__sim_dat = CStringNewFormat( "%s/" VGX_PATHDEF_INSTANCE_SIMILARITY_FMT VGX_PATHDEF_EXT_DATA, sim_path, CStringValue(graph->CSTR__name) );
    }
  }
  XCATCH( errcode ) {
    if( CSTR__sim_dat ) {
      CStringDelete( CSTR__sim_dat );
    }
    CSTR__sim_dat = NULL;
  }
  XFINALLY {
    if( CSTR__sim_path ) {
      CStringDelete( CSTR__sim_path );
    }
  }

  return CSTR__sim_dat;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __serialize_similarity( vgx_Similarity_t * self ) {
  int64_t __NQWORDS = 0;
  CString_t *CSTR__sim_dat = NULL;
  CQwordQueue_t *__OUTPUT = NULL;

  XTRY {
    if( (CSTR__sim_dat = __get_new_filepath( self )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC31 );
    }

    if( (__OUTPUT = CQwordQueueNewOutput( 1024, CStringValue( CSTR__sim_dat ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC32 );
    }

    // Start file
    EVAL_OR_THROW( iSerialization.WriteBeginFile( CSTR__sim_dat, __OUTPUT ), 0xC33 );

    // Similarity Config
    EVAL_OR_THROW( iSerialization.WriteBeginSectionFormat( __OUTPUT, "Similarity Config" ), 0xC34 );
    WRITE_OR_THROW( self->params.qwords, qwsizeof(vgx_Similarity_config_t), 0xC35 );

    // Nullvector Handle
    EVAL_OR_THROW( iSerialization.WriteBeginSectionFormat( __OUTPUT, "Nullvector Handle" ), 0xC36 );
    cxmalloc_handle_t nullvector_handle = _vxoballoc_vector_as_handle( self->nullvector );
    WRITE_OR_THROW( &nullvector_handle.qword, 1, 0xC37 );

    // End file
    EVAL_OR_THROW( iSerialization.WriteEndFile( __OUTPUT ), 0xC38 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    if( CSTR__sim_dat ) {
      CStringDelete( CSTR__sim_dat );
    }
    if( __OUTPUT ) {
      COMLIB_OBJECT_DESTROY( __OUTPUT );
    }
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __deserialize_similarity( vgx_Similarity_t * self ) {
  int64_t __NQWORDS = 0;
  CString_t *CSTR__sim_dat = NULL;
  CQwordQueue_t *__INPUT = NULL;

  XTRY {

    if( (CSTR__sim_dat = __get_new_filepath( self )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC41 );
    }

    if( file_exists( CStringValue( CSTR__sim_dat ) ) ) {
      if( (__INPUT = CQwordQueueNewInput( 1024, CStringValue( CSTR__sim_dat ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC42 );
      }

      // Begin File
      EVAL_OR_THROW( iSerialization.ExpectBeginFile( CSTR__sim_dat, __INPUT ), 0xC43 );
      
      // Similarity Config
      EVAL_OR_THROW( iSerialization.ExpectBeginSectionFormat( __INPUT, "Similarity Config" ), 0xC44 );
      QWORD *pqword = self->params.qwords;
      READ_OR_THROW( pqword, qwsizeof( vgx_Similarity_config_t ), 0xC45 );

      // Nullvector Handle
      EVAL_OR_THROW( iSerialization.ExpectBeginSectionFormat( __INPUT, "Nullvector Handle" ), 0xC46 );
      cxmalloc_handle_t nullvector_handle;
      pqword = &nullvector_handle.qword;
      READ_OR_THROW( pqword, 1, 0xC47 );
      if( (self->nullvector = CALLABLE( self->int_vector_allocator )->HandleAsObjectNolock( self->int_vector_allocator, nullvector_handle )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC48 );
      }
      self->nullvector->metas.flags.ecl = igraphfactory.EuclideanVectors(); // <- ensure compat when loading older data
      if( _cxmalloc_is_object_active( self->nullvector ) ) {
        CALLABLE( self->nullvector )->Incref( self->nullvector );
      }
      else {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0xC49 );
      }

      // End File
      EVAL_OR_THROW( iSerialization.ExpectEndFile( __INPUT ), 0xC4A );
    }
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    if( CSTR__sim_dat ) {
      CStringDelete( CSTR__sim_dat );
    }
    if( __INPUT ) {
      COMLIB_OBJECT_DESTROY( __INPUT );
    }

  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 * Returns: >= 1 : readonly recursion count
 *            -1 : error
 *
 ***********************************************************************
 */
static int Similarity_set_readonly( vgx_Similarity_t *self ) {
  int readonly = 0;

  XTRY {
    framehash_vtable_t *iFH = (framehash_vtable_t*)COMLIB_CLASS_VTABLE( framehash_t );

    // Set the dimension allocator readonly
    if( self->dimension_allocator_context ) {
      if( icstringalloc.SetReadonly( self->dimension_allocator_context ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC51 );
      }
    }

    // Set the dimension enumeration tables readonly
    if( self->dim_encoder ) {
      if( iFH->SetReadonly( self->dim_encoder ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC52 );
      }
    }

    if( self->dim_decoder ) {
      if( iFH->SetReadonly( self->dim_decoder ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC53 );
      }
    }

    // Set the vector allocators readonly
    if( CALLABLE( self->int_vector_allocator )->SetReadonly( self->int_vector_allocator ) < 0 ||
        CALLABLE( self->ext_vector_allocator )->SetReadonly( self->ext_vector_allocator ) < 0
    )
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC54 );
    }

    readonly = ++(self->readonly);
  }
  XCATCH( errcode ) {
    Similarity_clear_readonly( self );
    readonly = -1;
  }
  XFINALLY {
  }

  return readonly;
}



/*******************************************************************//**
 *
 * Returns:   0 : writable
 *            1 : readonly
 *
 ***********************************************************************
 */
static int Similarity_is_readonly( vgx_Similarity_t *self ) {
  return self->readonly > 0;
}



/*******************************************************************//**
 *
 * Returns:    0 : Writable after clear
 *          >= 1 : Still readonly with this recursion count
 *            -1 : Error
 *
 ***********************************************************************
 */
static int Similarity_clear_readonly( vgx_Similarity_t *self ) {
  int readonly = 0;
  framehash_vtable_t *iFH = (framehash_vtable_t*)COMLIB_CLASS_VTABLE( framehash_t );
  CALLABLE( self->int_vector_allocator )->ClearReadonly( self->int_vector_allocator );
  CALLABLE( self->ext_vector_allocator )->ClearReadonly( self->ext_vector_allocator );

  if( self->dim_decoder ) {
    iFH->ClearReadonly( self->dim_decoder );
  }
  
  if( self->dim_encoder ) {
    iFH->ClearReadonly( self->dim_encoder );
  }

  if( self->dimension_allocator_context ) {
    icstringalloc.ClearReadonly( self->dimension_allocator_context );
  }

  if( self->readonly > 0 ) {
    readonly = --(self->readonly);
  }
  else {
    readonly = -1;
  }

  return readonly;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void Similarity_print_vector_allocator( vgx_Similarity_t *self, vgx_Vector_t *vector ) {

  bool ext = CALLABLE( vector )->IsExternal( vector );
  bool eph = CALLABLE( vector )->IsEphemeral( vector );
  bool ecl = CALLABLE( vector )->IsEuclidean( vector );


  cxmalloc_family_t *vector_allocator_family = ivectoralloc.Get( vector );
  int64_t vector_refcnt = CALLABLE( vector )->Refcnt( vector );
  cxmalloc_linehead_t *vector_linehead = _cxmalloc_linehead_from_object( vector );
  uint16_t aidx = vector_linehead->data.aidx;
  uint16_t bidx = vector_linehead->data.bidx;
  uint32_t offset = vector_linehead->data.offset;

  printf( "VECTOR ALLOCATOR FAMILY\n" );
  PRINT( vector_allocator_family );
  printf( "\n" );
  
  printf( "VECTOR\n" );
  PRINT( vector );
  printf( "\n" );

  printf( "DETAILS\n" );
  printf( "aidx=%u bidx=%u offset=%u\n", aidx, bidx, offset );
  printf( "refcnt=%lld\n", vector_refcnt );
  int length = CALLABLE( vector )->Length( vector );
  float magnitude = CALLABLE( vector )->Magnitude( vector );
  printf( "length=%d\n", length );
  printf( "magnitude=%#g\n", magnitude );


  if( ecl ) {
    if( ext ) {
      printf( "%s EXTERNAL VECTOR\n", eph ? "EPHEMERAL" : "PERSISTENT" );
      float *elems = (float*)CALLABLE( vector )->Elements( vector );
      float *cursor = elems;
      float *end = elems + length;
      while( cursor < end ) {
        printf( "%#g\n", *cursor++ );
      }
    }
    else {
      printf( "%s INTERNAL VECTOR\n", eph ? "EPHEMERAL" : "PERSISTENT" );
      BYTE *elems = (BYTE*)CALLABLE( vector )->Elements( vector );
      BYTE *cursor = elems;
      BYTE *end = elems + length;
      while( cursor < end ) {
        printf( "%u\n", *cursor++ );
      }
    }
  }
  else {
    if( ext ) {
      printf( "%s EXTERNAL VECTOR\n", eph ? "EPHEMERAL" : "PERSISTENT" );
      ext_vector_feature_t *elems = (ext_vector_feature_t*)CALLABLE( vector )->Elements( vector );
      ext_vector_feature_t *cursor = elems;
      ext_vector_feature_t *end = elems + length;
      while( cursor < end ) {
        const char *term = cursor->term;
        float weight = cursor->weight;
        if( iEnumerator_OPEN.Dimension.Exists( self, term ) ) {
          feature_vector_dimension_t dim = iEnumerator_OPEN.Dimension.EncodeChars( self, term, NULL, false );
          const CString_t *CSTR__dimension = iEnumerator_OPEN.Dimension.Decode( self, dim );
          cxmalloc_linehead_t *dim_linehead = _cxmalloc_linehead_from_object( CSTR__dimension );
          printf( "(%s=%d, %#g=%d) refcnt=%d\n", term, dim, weight, _vxsim_weight_as_magnitude( weight ), dim_linehead->data.refc );
        }
        else {
          feature_vector_dimension_t dim = iEnumerator_OPEN.Dimension.EncodeChars( self, term, NULL, false );
          printf( "(%s=%d, %#g=%d) ***UNMAPPED***\n", term, dim, weight, _vxsim_weight_as_magnitude( weight ) );
        }
        ++cursor;
      }
    }
    else {
      printf( "%s INTERNAL VECTOR\n", eph ? "EPHEMERAL" : "PERSISTENT" );
      vector_feature_t *elems = (vector_feature_t*)CALLABLE( vector )->Elements( vector );
      vector_feature_t *cursor = elems;
      vector_feature_t *end = elems + length;
      while( cursor < end ) {
        feature_vector_dimension_t dim = cursor->dim;
        int mag = cursor->mag;
        if( iEnumerator_OPEN.Dimension.ExistsEnum( self, dim ) ) {
          const CString_t *CSTR__dim = iEnumerator_OPEN.Dimension.Decode( self, dim );
          cxmalloc_linehead_t *dim_linehead = _cxmalloc_linehead_from_object( CSTR__dim );
          printf( "(%d=%s, %d=%#g) refcnt=%d\n", dim, CStringValue( CSTR__dim ), mag, _vxsim_magnitude_as_weight( mag ), dim_linehead->data.refc );
        }
        else {
          printf( "(%d=???, %d=%#g) ***UNMAPPED***\n", dim, mag, _vxsim_magnitude_as_weight( mag ) );
        }
        ++cursor;
      }
    }
  }
  printf( "\n" );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void Similarity_print_allocators( vgx_Similarity_t *self ) {

  // Vector dimension allocator
  if( self->dimension_allocator_context ) {
    cxmalloc_family_t *dimension_allocator = (cxmalloc_family_t*)self->dimension_allocator_context->allocator;
    PRINT( dimension_allocator );
  }

  // Dimension encoder map
  if( self->dim_encoder ) {
    CALLABLE( self->dim_encoder )->PrintAllocators( self->dim_encoder );
  }

  // Dimension decoder map
  if( self->dim_decoder ) {
    CALLABLE( self->dim_decoder )->PrintAllocators( self->dim_decoder );
  }

  // Internal vector allocator
  PRINT( self->int_vector_allocator );

  // External vector allocator
  PRINT( self->ext_vector_allocator );

  // Internal vector ephemeral allocator
  PRINT( self->int_vector_ephemeral_allocator );

  // External vector ephemeral allocator
  PRINT( self->ext_vector_ephemeral_allocator );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int Similarity_check_allocators( vgx_Similarity_t *self ) {
  int err = 0;

  // Vector dimension allocator
  if( self->dimension_allocator_context ) {
    cxmalloc_family_t *dimension_allocator = (cxmalloc_family_t*)self->dimension_allocator_context->allocator;
    if( dimension_allocator ) {
      err += CALLABLE( dimension_allocator )->Check( dimension_allocator );
    }
  }

  // Dimension encoder map
  if( self->dim_encoder ) {
    err += CALLABLE( self->dim_encoder )->CheckAllocators( self->dim_encoder );
  }

  // Dimension decoder map
  if( self->dim_decoder ) {
    err += CALLABLE( self->dim_decoder )->CheckAllocators( self->dim_decoder );
  }

  // Internal vector allocator
  if( self->int_vector_allocator ) {
    err += CALLABLE( self->int_vector_allocator )->Check( self->int_vector_allocator );
  }

  // External vector allocator
  if( self->ext_vector_allocator ) {
    err += CALLABLE( self->ext_vector_allocator )->Check( self->ext_vector_allocator );
  }

  // Internal vector ephemeral allocator
  if( self->int_vector_ephemeral_allocator ) {
    err += CALLABLE( self->int_vector_ephemeral_allocator )->Check( self->int_vector_ephemeral_allocator );
  }

  // External vector ephemeral allocator
  if( self->ext_vector_ephemeral_allocator ) {
    err += CALLABLE( self->ext_vector_ephemeral_allocator )->Check( self->ext_vector_ephemeral_allocator );
  }

  return err;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int64_t Similarity_verify_allocators( vgx_Similarity_t *self ) {
  int64_t n_fix = 0;
  int64_t n;

  // Vector dimension allocator
  if( self->dimension_allocator_context ) {
    cxmalloc_family_t *dimension_allocator = (cxmalloc_family_t*)self->dimension_allocator_context->allocator;
    if( dimension_allocator ) {
      if( (n = ivectoralloc.Verify( dimension_allocator )) < 0 ) {
        return -1;
      }
      n_fix += n;
    }
  }

  // Internal vector allocator
  if( self->int_vector_allocator ) {
    if( (n = ivectoralloc.Verify( self->int_vector_allocator )) < 0 ) {
      return -1;
    }
    n_fix += n;
  }

  // External vector allocator
  if( self->ext_vector_allocator ) {
    if( (n = ivectoralloc.Verify( self->ext_vector_allocator )) < 0 ) {
      return -1;
    }
    n_fix += n;
  }

  // Internal vector ephemeral allocator
  if( self->int_vector_ephemeral_allocator ) {
    if( (n = ivectoralloc.Verify( self->int_vector_ephemeral_allocator )) < 0 ) {
      return -1;
    }
    n_fix += n;
  }

  // External vector ephemeral allocator
  if( self->ext_vector_ephemeral_allocator ) {
    if( (n = ivectoralloc.Verify( self->ext_vector_ephemeral_allocator )) < 0 ) {
      return -1;
    }
    n_fix += n;
  }

  return n_fix;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __serialize_enumerators( vgx_Similarity_t *self, bool force ) {
  int64_t __NQWORDS = 0;
  if( self->dim_decoder ) {
    PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( NULL ) {
      XTRY {
        EVAL_OR_THROW( CALLABLE( self->dim_decoder )->BulkSerialize( self->dim_decoder, force ), 0xC61 );
      }
      XCATCH( errcode ) {
        __NQWORDS = -1;
      }
      XFINALLY {
      }
    } POP_STRING_ALLOCATOR_CONTEXT;
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t Similarity_bulk_serialize( vgx_Similarity_t *self, bool force ) {
  int64_t __NQWORDS = 0;
  int64_t value;

#ifdef HASVERBOSE
  const char *parent_path = CALLABLE( self->parent )->FullPath( self->parent );
#endif
  XTRY {
    // Enter READONLY mode
    Similarity_set_readonly( self );
    
    // [6] dimension allocator context allocator
    if( self->dimension_allocator_context ) {
      cxmalloc_family_t *dimension_allocator = (cxmalloc_family_t*)self->dimension_allocator_context->allocator;
      value = CALLABLE( dimension_allocator )->Bytes( dimension_allocator );
      VERBOSE( 0xC71, "Graph(%s) Serializing: vector dimension data (%lld bytes)", parent_path, value );
      EVAL_OR_THROW( CALLABLE( dimension_allocator )->BulkSerialize( dimension_allocator, force ), 0xC72 );
    }

    // [7] dim_encoder
    if( self->dim_encoder ) {
      value = CALLABLE( self->dim_encoder )->Items( self->dim_encoder );
      VERBOSE( 0xC73, "Graph(%s) Serializing: vector dimension encoder (%lld mappings)", parent_path, value );
      EVAL_OR_THROW( CALLABLE( self->dim_encoder )->BulkSerialize( self->dim_encoder, force ), 0xC74 );
    }
    
    // [8] dim_decoder
    if( self->dim_decoder ) {
      value = CALLABLE( self->dim_decoder )->Items( self->dim_decoder );
      VERBOSE( 0xC75, "Graph(%s) Serializing: vector dimension decoder (%lld mappings)", parent_path, value );
    }

    int64_t enum_qwords = __serialize_enumerators( self, force );
    if( enum_qwords < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC76 );
    }
    __NQWORDS += enum_qwords;

    // [9] int_vector_allocator
    value = CALLABLE( self->int_vector_allocator )->Bytes( self->int_vector_allocator );
    VERBOSE( 0xC77, "Graph(%s) Serializing: internal vectors (%lld bytes)", parent_path, value );
    EVAL_OR_THROW( CALLABLE( self->int_vector_allocator )->BulkSerialize( self->int_vector_allocator, force ), 0xC78 );

    // [10] ext_vector_allocator
    value = CALLABLE( self->ext_vector_allocator )->Bytes( self->ext_vector_allocator );
    VERBOSE( 0xC79, "Graph(%s) Serializing: external vectors (%lld bytes)", parent_path, value );
    EVAL_OR_THROW( CALLABLE( self->ext_vector_allocator )->BulkSerialize( self->ext_vector_allocator, force ), 0xC7A );
    
    // Similarity data
    VERBOSE( 0xC7B, "Graph(%s) Serializing: similarity configuration", parent_path );
    EVAL_OR_THROW( __serialize_similarity( self ), 0xC7C );

  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    // Enter WRITABLE mode
    Similarity_clear_readonly( self );
  }


  return __NQWORDS;
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxsim.h"
  
test_descriptor_t _vgx_vxsim_tests[] = {
  { "VGX Graph Similarity Tests", __utest_vxsim },
  {NULL}
};
#endif
