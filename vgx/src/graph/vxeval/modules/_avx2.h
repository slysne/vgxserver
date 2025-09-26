/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _avx2.h
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

#ifndef _VGX_VXEVAL_MODULES_AVX2_H
#define _VGX_VXEVAL_MODULES_AVX2_H

#include "_memory.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_avx2_ecld_pi8( vgx_Evaluator_t *self );
static void __eval_avx2_ssq_pi8( vgx_Evaluator_t *self );
static void __eval_avx2_rsqrtssq_pi8( vgx_Evaluator_t *self );
static void __eval_avx2_dp_pi8( vgx_Evaluator_t *self );
static void __eval_avx2_cos_pi8( vgx_Evaluator_t *self );



// Convert lower 8x bytes in a to 8x floats
#define _mm256_low8_cvtepi8_ps( a ) _mm256_cvtepi32_ps( _mm256_cvtepi8_epi32( _mm256_castsi256_si128( a ) ) )

// Extract the upper 128 bits of a 256 bit register
#define _mm256_extract_upper128(a) _mm256_castsi128_si256( _mm256_extracti128_si256( a, 1 ) );


 
/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __sum_sqdiff_avx2( const BYTE *a_cur, const BYTE *b_cur, const __m256 *ascale, const __m256 *bscale, __m256 *ssqdiff ) {
  __m256i a32 = _mm256_load_si256( (__m256i*)a_cur );
  __m256i b32 = _mm256_load_si256( (__m256i*)b_cur );
  __m256i aU, bU;
  __m256 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [33333333 22222222 11111111 00000000]
  //                             ^^^^^^^^
  // 8x bytes -> 8x int -> 8x float
  ps_a0 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b0 = _mm256_low8_cvtepi8_ps( b32 );

  // [33333333 22222222 11111111 00000000]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a2 = _mm256_low8_cvtepi8_ps( aU );
  ps_b2 = _mm256_low8_cvtepi8_ps( bU );

  // [-------- 33333333 -------- 11111111]
  a32 = _mm256_srli_si256( a32, 8 );
  b32 = _mm256_srli_si256( b32, 8 );

  // [-------- 33333333 -------- 11111111]
  //                             ^^^^^^^^
  ps_a1 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b1 = _mm256_low8_cvtepi8_ps( b32 );

  // [-------- 33333333 -------- 11111111]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a3 = _mm256_low8_cvtepi8_ps( aU );
  ps_b3 = _mm256_low8_cvtepi8_ps( bU );

  // Apply scaling factor 
  ps_a0 = _mm256_mul_ps( ps_a0, *ascale );
  ps_a1 = _mm256_mul_ps( ps_a1, *ascale );
  ps_a2 = _mm256_mul_ps( ps_a2, *ascale );
  ps_a3 = _mm256_mul_ps( ps_a3, *ascale );
  ps_b0 = _mm256_mul_ps( ps_b0, *bscale );
  ps_b1 = _mm256_mul_ps( ps_b1, *bscale );
  ps_b2 = _mm256_mul_ps( ps_b2, *bscale );
  ps_b3 = _mm256_mul_ps( ps_b3, *bscale );

  // Subtract
  ps_a0 = _mm256_sub_ps( ps_a0, ps_b0 );
  ps_a1 = _mm256_sub_ps( ps_a1, ps_b1 );
  ps_a2 = _mm256_sub_ps( ps_a2, ps_b2 );
  ps_a3 = _mm256_sub_ps( ps_a3, ps_b3 );

  // Square and add to running sum
  *ssqdiff = _mm256_fmadd_ps( ps_a0, ps_a0, *ssqdiff );
  *ssqdiff = _mm256_fmadd_ps( ps_a1, ps_a1, *ssqdiff );
  *ssqdiff = _mm256_fmadd_ps( ps_a2, ps_a2, *ssqdiff );
  *ssqdiff = _mm256_fmadd_ps( ps_a3, ps_a3, *ssqdiff );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __ssq_avx2( const BYTE *a_cur, __m256 *ssq ) {
  __m256i a32 = _mm256_load_si256( (__m256i*)a_cur );
  __m256i aU;
  __m256 ps_a0, ps_a1, ps_a2, ps_a3;

  // [33333333 22222222 11111111 00000000]
  //                             ^^^^^^^^
  // 8x bytes -> 8x int -> 8x float
  ps_a0 = _mm256_low8_cvtepi8_ps( a32 );

  // [33333333 22222222 11111111 00000000]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  ps_a2 = _mm256_low8_cvtepi8_ps( aU );

  // [-------- 33333333 -------- 11111111]
  a32 = _mm256_srli_si256( a32, 8 );

  // [-------- 33333333 -------- 11111111]
  //                             ^^^^^^^^
  ps_a1 = _mm256_low8_cvtepi8_ps( a32 );

  // [-------- 33333333 -------- 11111111]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  ps_a3 = _mm256_low8_cvtepi8_ps( aU );

  // Square and add to running sum
  *ssq = _mm256_fmadd_ps( ps_a0, ps_a0, *ssq );
  *ssq = _mm256_fmadd_ps( ps_a1, ps_a1, *ssq );
  *ssq = _mm256_fmadd_ps( ps_a2, ps_a2, *ssq );
  *ssq = _mm256_fmadd_ps( ps_a3, ps_a3, *ssq );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __dp_avx2( const BYTE *a, const BYTE *b, __m256 *sum ) {
  // Should we test for zero to optimize?

  __m256i a32 = _mm256_load_si256( (__m256i*)a );
  __m256i b32 = _mm256_load_si256( (__m256i*)b );
  __m256i aU, bU;
  __m256 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [33333333 22222222 11111111 00000000]
  //                             ^^^^^^^^
  // 8x bytes -> 8x int -> 8x float
  ps_a0 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b0 = _mm256_low8_cvtepi8_ps( b32 );

  // [33333333 22222222 11111111 00000000]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a2 = _mm256_low8_cvtepi8_ps( aU );
  ps_b2 = _mm256_low8_cvtepi8_ps( bU );

  // [-------- 33333333 -------- 11111111]
  a32 = _mm256_srli_si256( a32, 8 );
  b32 = _mm256_srli_si256( b32, 8 );

  // [-------- 33333333 -------- 11111111]
  //                             ^^^^^^^^
  ps_a1 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b1 = _mm256_low8_cvtepi8_ps( b32 );

  // [-------- 33333333 -------- 11111111]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a3 = _mm256_low8_cvtepi8_ps( aU );
  ps_b3 = _mm256_low8_cvtepi8_ps( bU );


  // multiply 8x floats and add to running sums
  *sum = _mm256_fmadd_ps( ps_a0, ps_b0, *sum );
  *sum = _mm256_fmadd_ps( ps_a1, ps_b1, *sum );
  *sum = _mm256_fmadd_ps( ps_a2, ps_b2, *sum );
  *sum = _mm256_fmadd_ps( ps_a3, ps_b3, *sum );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __dp_ssq_avx2( const BYTE *a, const BYTE *b, __m256 *sum, __m256 *ssq_a, __m256 *ssq_b ) {
  // Should we test for zero to optimize?

  __m256i a32 = _mm256_load_si256( (__m256i*)a );
  __m256i b32 = _mm256_load_si256( (__m256i*)b );
  __m256i aU, bU;
  __m256 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [33333333 22222222 11111111 00000000]
  //                             ^^^^^^^^
  // 8x bytes -> 8x int -> 8x float
  ps_a0 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b0 = _mm256_low8_cvtepi8_ps( b32 );

  // [33333333 22222222 11111111 00000000]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a2 = _mm256_low8_cvtepi8_ps( aU );
  ps_b2 = _mm256_low8_cvtepi8_ps( bU );

  // [-------- 33333333 -------- 11111111]
  a32 = _mm256_srli_si256( a32, 8 );
  b32 = _mm256_srli_si256( b32, 8 );

  // [-------- 33333333 -------- 11111111]
  //                             ^^^^^^^^
  ps_a1 = _mm256_low8_cvtepi8_ps( a32 );
  ps_b1 = _mm256_low8_cvtepi8_ps( b32 );

  // [-------- 33333333 -------- 11111111]
  //           ^^^^^^^^
  aU = _mm256_extract_upper128( a32 );
  bU = _mm256_extract_upper128( b32 );
  ps_a3 = _mm256_low8_cvtepi8_ps( aU );
  ps_b3 = _mm256_low8_cvtepi8_ps( bU );

  // multiply 8x floats and add to running sums
  *ssq_a = _mm256_fmadd_ps( ps_a0, ps_a0, *ssq_a );
  *ssq_b = _mm256_fmadd_ps( ps_b0, ps_b0, *ssq_b );
  *sum   = _mm256_fmadd_ps( ps_a0, ps_b0, *sum );

  *ssq_a = _mm256_fmadd_ps( ps_a1, ps_a1, *ssq_a );
  *ssq_b = _mm256_fmadd_ps( ps_b1, ps_b1, *ssq_b );
  *sum   = _mm256_fmadd_ps( ps_a1, ps_b1, *sum );
  
  *ssq_a = _mm256_fmadd_ps( ps_a2, ps_a2, *ssq_a );
  *ssq_b = _mm256_fmadd_ps( ps_b2, ps_b2, *ssq_b );
  *sum   = _mm256_fmadd_ps( ps_a2, ps_b2, *sum );
  
  *ssq_a = _mm256_fmadd_ps( ps_a3, ps_a3, *ssq_a );
  *ssq_b = _mm256_fmadd_ps( ps_b3, ps_b3, *ssq_b );
  *sum   = _mm256_fmadd_ps( ps_a3, ps_b3, *sum );
}



/*******************************************************************//**
 *
 * Horizontal add 8 floats into rightmost 32-bit float
 *
 ***********************************************************************
 */
__inline static __m256 * __hadd_ps_avx2( __m256 *ps ) {
  // ps = {abcdefgh}
  //
  // LAT  CPI
  //  3    1    {a b c d e f g h} -> {e f g h a b c d}
  //
  __m256 ymm1 = _mm256_permute2f128_ps( *ps, *ps, 1 );

  // 
  //  4   0.5   {abcdefgh} + {efghabcd} -> {a+e b+f c+g d+h e+a f+b g+c h+d}
  //                                        __dont care____ _____r1-r4_____
  __m256 ymm2 = _mm256_add_ps( *ps, ymm1 );
  
  //             ____128____ ____128____ 
  //  1    1    {----------- r1 r2 r3 r4} -> {xx xx xx xx r3 r4 r1 r2} 
  //                                                      ___________
  ymm1 = _mm256_permute_ps( ymm2, _MM_SHUFFLE(1,0,3,2) );


  //  4   0.5   { ... r1 r2 r3 r4 } + {... r3 r4 r1 r2 } -> { ... r1+r3 r2+r4 r3+r1 r4+r2 }
  //                                                          ___dont care___ ___s1-s2___
  ymm1 = _mm256_add_ps( ymm2, ymm1 );

  //  1    1    { ... r1+r3 r2+r4 r3+r1 r4+r2 } -> { ... 0 r1+r3 r2+r4 r3+r1 }
  //                                                     ___ignore____ _s1__
  ymm2 = _mm256_castsi256_ps( _mm256_srli_si256( _mm256_castps_si256( ymm1 ), 4 ) );


  //  4   0.5   { ... r3+r1 r4+r2 } + { ... r3+r1 } -> { ... r4+r2+r3+r1 }
  //                                                         ____sum____
  *ps = _mm256_add_ps( ymm1, ymm2 );
  
  return ps;
}



/*******************************************************************//**
 *
 * Extract the lower 32 bits as float
 *
 ***********************************************************************
 */
__inline static float __extract_float_avx2( __m256 *ps ) {
  int x = _mm256_cvtsi256_si32( _mm256_castps_si256( *ps ) );
  return *(float*)&x;
}



/*******************************************************************//**
 *
 * Return square root of horizontal sum of 8 floats in ps
 *
 ***********************************************************************
 */
__inline static float __sqrt_hadd_ps_avx2( __m256 *ps ) {
  // 12    3    { ... sum } -> { ... sqrt(sum) }
  __m128 root = _mm_sqrt_ss( _mm256_castps256_ps128( *__hadd_ps_avx2( ps ) ) );

  //  2    1    Extract lower 32-bit value
  union { int32_t i32; float f; } S;
  S.i32 = _mm_cvtsi128_si32( _mm_castps_si128( root ) );

  return S.f;
}



/*******************************************************************//**
 *
 * Return reciprocal square root of horizontal sum of 8 floats in ps
 *
 ***********************************************************************
 */
__inline static float __rsqrt_hadd_ps_avx2( __m256 *ps ) {
  //  4    1    { ... sum } -> { ... 1/sqrt(sum) }
  __m256 rsqrt = _mm256_rsqrt_ps( *__hadd_ps_avx2( ps ) );

  //  2    1    Extract lower 32-bit value
  union { int32_t i32; float f; } S;
  S.i32 = _mm256_cvtsi256_si32( _mm256_castps_si256( rsqrt ) );

  return S.f;
}



/*******************************************************************//**
 * ecld( A, B ) -> euclidean distance
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static double __avx2_ecld_pi8( const BYTE *A, const BYTE *B, float fA, float fB, int len ) {
#ifdef __AVX2__
  // Eight bins of running (float) sums of squares of differences
  __m256 ssqdiff = _mm256_setzero_ps();
  // Process chunks of 32 bytes, any trailing non-multiple of 32 is ignored!!
  int N = len >> 5;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  __m256 ascale = _mm256_broadcast_ss( &fA );
  __m256 bscale = _mm256_broadcast_ss( &fB );
  for( int i=0; i<N; i++, a_cur += sizeof(__m256i), b_cur += sizeof(__m256i) ) {
    // Aggregate sum bins from partial squares of differences
    __sum_sqdiff_avx2( a_cur, b_cur, &ascale, &bscale, &ssqdiff );
  }

  return __sqrt_hadd_ps_avx2( &ssqdiff );
#else
  return __scalar_ecld_pi8( A, B, fA, fB, len );
#endif
}



/*******************************************************************//**
 * ssq( A ) -> sum
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static double __avx2_ssq_pi8( const BYTE *A, int len ) {
#ifdef __AVX2__
  // Eight bins of running (float) sums of squares
  __m256 ssq = _mm256_setzero_ps();
  // Process chunks of 32 bytes, any trailing non-multiple of 32 is ignored!!
  int N = len >> 5;
  const BYTE *a_cur = A;
  for( int i=0; i<N; i++, a_cur += sizeof(__m256i) ) {
    // Aggregate sum bins from partial squares
    __ssq_avx2( a_cur, &ssq );
  }

  return __extract_float_avx2( __hadd_ps_avx2( &ssq ) );
#else
  return __scalar_ssq_pi8( A, len );
#endif
}



/*******************************************************************//**
 * rsqrtssq( A ) -> reciprocal square root of sum of squares
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static double __avx2_rsqrtssq_pi8( const BYTE *A, int len ) {
#ifdef __AVX2__
  // Eight bins of running (float) sums of squares
  __m256 ssq = _mm256_setzero_ps();
  // Process chunks of 32 bytes, any trailing non-multiple of 32 is ignored!!
  int N = len >> 5;
  const BYTE *a_cur = A;
  for( int i=0; i<N; i++, a_cur += sizeof(__m256i) ) {
    // Aggregate sum bins from partial squares
    __ssq_avx2( a_cur, &ssq );
  }

  return __rsqrt_hadd_ps_avx2( &ssq );
#else
  return __scalar_rsqrtssq_pi8( A, len );
#endif
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static double __avx2_dp_pi8( const BYTE *A, const BYTE *B, int len ) {
#ifdef __AVX2__
  // Eight bins of running (float) sums of products
  __m256 sum = _mm256_setzero_ps();
  // Process chunks of 32 bytes, any trailing non-multiple of 32 is ignored!!
  int N = len >> 5;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  for( int i=0; i<N; i++, a_cur += sizeof(__m256i), b_cur += sizeof(__m256i) ) {
    __dp_avx2( a_cur, b_cur, &sum );
  }

  return __extract_float_avx2( __hadd_ps_avx2( &sum ) );
#else
  return __scalar_dp_pi8( A, B, len );
#endif
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static double __avx2_cos_pi8( const BYTE *A, const BYTE *B, int len ) {
#ifdef __AVX2__
  __m256 sum = _mm256_setzero_ps();
  __m256 ssq_a = _mm256_setzero_ps();
  __m256 ssq_b = _mm256_setzero_ps();
  // Process chunks of 32 bytes, any trailing non-multiple of 32 is ignored!!
  int N = len >> 5;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  for( int i=0; i<N; i++, a_cur += sizeof(__m256i), b_cur += sizeof(__m256i) ) {
    __dp_ssq_avx2( a_cur, b_cur, &sum, &ssq_a, &ssq_b );
  }

  // Dot product
  double dp = __extract_float_avx2( __hadd_ps_avx2( &sum ) );
  // Reciprocal norms
  double rnorm_a = __rsqrt_hadd_ps_avx2( &ssq_a );
  double rnorm_b = __rsqrt_hadd_ps_avx2( &ssq_b );
  // Cosine
  double cosine = dp * rnorm_a * rnorm_b;
  if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
    cosine = (double)((cosine > 0.0) - (cosine < 0.0));
  }
  return cosine;
#else
  return __scalar_cos_pi8( A, B, len );
#endif
}



/*******************************************************************//**
 * EuclideanDistance( A, B )
 *
 * Byte array length / Vector length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static void __eval_avx2_ecld_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx2_ecld_pi8( a_data, b_data, a_scale, b_scale, len );
  }
  else {
    SET_REAL_VALUE( self, INFINITY );
  }
#else
  __eval_scalar_ecld_pi8( self );
#endif
}



/*******************************************************************//**
 * SumOfSquares( A )
 *
 * Byte array length / Vector length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static void __eval_avx2_ssq_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __avx2_ssq_pi8( data, len ) * scale * scale;
  }
#else
  __eval_scalar_ssq_pi8( self );
#endif
}



/*******************************************************************//**
 * ReciprocalSquareRootSumOfSquares( A )
 *
 * Byte array length / Vector length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static void __eval_avx2_rsqrtssq_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __avx2_rsqrtssq_pi8( data, len ) / scale;
  }
#else
  __eval_scalar_rsqrtssq_pi8( self );
#endif
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static void __eval_avx2_dp_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx2_dp_pi8( a_data, b_data, len ) * a_scale * b_scale;
  }
#else
  __eval_scalar_dp_pi8( self );
#endif
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 32.
 *
 *
 ***********************************************************************
 */
static void __eval_avx2_cos_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx2_cos_pi8( a_data, b_data, len );
  }
#else
  __eval_scalar_cos_pi8( self );
#endif
}




#endif
