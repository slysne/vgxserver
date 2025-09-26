/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _avx512.h
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

#ifndef _VGX_VXEVAL_MODULES_AVX512_H
#define _VGX_VXEVAL_MODULES_AVX512_H

#include "_memory.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_avx512_ecld_pi8( vgx_Evaluator_t *self );
static void __eval_avx512_ssq_pi8( vgx_Evaluator_t *self );
static void __eval_avx512_rsqrtssq_pi8( vgx_Evaluator_t *self );
static void __eval_avx512_dp_pi8( vgx_Evaluator_t *self );
static void __eval_avx512_cos_pi8( vgx_Evaluator_t *self );



// Convert lower 16x bytes in a to 16x floats
#define _mm512_low16_cvtepi8_ps( a ) _mm512_cvtepi32_ps( _mm512_cvtepi8_epi32( _mm512_castsi512_si128( a ) ) )

// Extract the upper 256 bits of a 512 bit register
#define _mm512_extract_upper256(a) _mm512_castsi256_si512( _mm512_extracti32x8_epi32( a, 1 ) );


 
/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __sum_sqdiff_avx512( const BYTE *a_cur, const BYTE *b_cur, const __m512 *ascale, const __m512 *bscale, __m512 *ssqdiff ) {
  __m512i a64 = _mm512_load_si512( (__m512i*)a_cur );
  __m512i b64 = _mm512_load_si512( (__m512i*)b_cur );
  __m512i aU, bU;
  __m512 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                                                     ^^^^^^^^^^^^^^^^
  // 16x bytes -> 16x int -> 16x float
  ps_a0 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b0 = _mm512_low16_cvtepi8_ps( b64 );

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a2 = _mm512_low16_cvtepi8_ps( aU );
  ps_b2 = _mm512_low16_cvtepi8_ps( bU );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  a64 = _mm512_shuffle_i32x4( a64, a64, _MM_SHUFFLE(2,3,0,1) );
  b64 = _mm512_shuffle_i32x4( b64, b64, _MM_SHUFFLE(2,3,0,1) );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                                                     ^^^^^^^^^^^^^^^^
  ps_a1 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b1 = _mm512_low16_cvtepi8_ps( b64 );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a3 = _mm512_low16_cvtepi8_ps( aU );
  ps_b3 = _mm512_low16_cvtepi8_ps( bU );

  // Apply scaling factor 
  ps_a0 = _mm512_mul_ps( ps_a0, *ascale );
  ps_a1 = _mm512_mul_ps( ps_a1, *ascale );
  ps_a2 = _mm512_mul_ps( ps_a2, *ascale );
  ps_a3 = _mm512_mul_ps( ps_a3, *ascale );
  ps_b0 = _mm512_mul_ps( ps_b0, *bscale );
  ps_b1 = _mm512_mul_ps( ps_b1, *bscale );
  ps_b2 = _mm512_mul_ps( ps_b2, *bscale );
  ps_b3 = _mm512_mul_ps( ps_b3, *bscale );

  // Subtract
  ps_a0 = _mm512_sub_ps( ps_a0, ps_b0 );
  ps_a1 = _mm512_sub_ps( ps_a1, ps_b1 );
  ps_a2 = _mm512_sub_ps( ps_a2, ps_b2 );
  ps_a3 = _mm512_sub_ps( ps_a3, ps_b3 );

  // Square and add to running sum
  *ssqdiff = _mm512_fmadd_ps( ps_a0, ps_a0, *ssqdiff );
  *ssqdiff = _mm512_fmadd_ps( ps_a1, ps_a1, *ssqdiff );
  *ssqdiff = _mm512_fmadd_ps( ps_a2, ps_a2, *ssqdiff );
  *ssqdiff = _mm512_fmadd_ps( ps_a3, ps_a3, *ssqdiff );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __ssq_avx512( const BYTE *a_cur, __m512 *ssq ) {
  __m512i a64 = _mm512_load_si512( (__m512i*)a_cur );
  __m512i aU;
  __m512 ps_a0, ps_a1, ps_a2, ps_a3;

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                                                     ^^^^^^^^^^^^^^^^
  // 16x bytes -> 16x int -> 16x float
  ps_a0 = _mm512_low16_cvtepi8_ps( a64 );

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  ps_a2 = _mm512_low16_cvtepi8_ps( aU );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  a64 = _mm512_shuffle_i32x4( a64, a64, _MM_SHUFFLE(2,3,0,1) );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                                                     ^^^^^^^^^^^^^^^^
  ps_a1 = _mm512_low16_cvtepi8_ps( a64 );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  ps_a3 = _mm512_low16_cvtepi8_ps( aU );

  // Square and add to running sum
  *ssq = _mm512_fmadd_ps( ps_a0, ps_a0, *ssq );
  *ssq = _mm512_fmadd_ps( ps_a1, ps_a1, *ssq );
  *ssq = _mm512_fmadd_ps( ps_a2, ps_a2, *ssq );
  *ssq = _mm512_fmadd_ps( ps_a3, ps_a3, *ssq );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __dp_avx512( const BYTE *a, const BYTE *b, __m512 *sum ) {
  // Should we test for zero to optimize?

  __m512i a64 = _mm512_load_si512( (__m512i*)a );
  __m512i b64 = _mm512_load_si512( (__m512i*)b );
  __m512i aU, bU;
  __m512 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                                                     ^^^^^^^^^^^^^^^^
  // 16x bytes -> 16x int -> 16x float
  ps_a0 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b0 = _mm512_low16_cvtepi8_ps( b64 );

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a2 = _mm512_low16_cvtepi8_ps( aU );
  ps_b2 = _mm512_low16_cvtepi8_ps( bU );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  a64 = _mm512_shuffle_i32x4( a64, a64, _MM_SHUFFLE(2,3,0,1) );
  b64 = _mm512_shuffle_i32x4( b64, b64, _MM_SHUFFLE(2,3,0,1) );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                                                     ^^^^^^^^^^^^^^^^
  ps_a1 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b1 = _mm512_low16_cvtepi8_ps( b64 );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a3 = _mm512_low16_cvtepi8_ps( aU );
  ps_b3 = _mm512_low16_cvtepi8_ps( bU );

  // multiply 16x floats and add to running sums
  *sum = _mm512_fmadd_ps( ps_a0, ps_b0, *sum );
  *sum = _mm512_fmadd_ps( ps_a1, ps_b1, *sum );
  *sum = _mm512_fmadd_ps( ps_a2, ps_b2, *sum );
  *sum = _mm512_fmadd_ps( ps_a3, ps_b3, *sum );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __dp_ssq_avx512( const BYTE *a, const BYTE *b, __m512 *sum, __m512 *ssq_a, __m512 *ssq_b ) {
  // Should we test for zero to optimize?

  __m512i a64 = _mm512_load_si512( (__m512i*)a );
  __m512i b64 = _mm512_load_si512( (__m512i*)b );
  __m512i aU, bU;
  __m512 ps_a0, ps_a1, ps_a2, ps_a3, ps_b0, ps_b1, ps_b2, ps_b3;

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                                                     ^^^^^^^^^^^^^^^^
  // 16x bytes -> 16x int -> 16x float
  ps_a0 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b0 = _mm512_low16_cvtepi8_ps( b64 );

  // [3333333333333333 2222222222222222 1111111111111111 0000000000000000]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a2 = _mm512_low16_cvtepi8_ps( aU );
  ps_b2 = _mm512_low16_cvtepi8_ps( bU );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  a64 = _mm512_shuffle_i32x4( a64, a64, _MM_SHUFFLE(2,3,0,1) );
  b64 = _mm512_shuffle_i32x4( b64, b64, _MM_SHUFFLE(2,3,0,1) );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                                                     ^^^^^^^^^^^^^^^^
  ps_a1 = _mm512_low16_cvtepi8_ps( a64 );
  ps_b1 = _mm512_low16_cvtepi8_ps( b64 );

  // [2222222222222222 3333333333333333 0000000000000000 1111111111111111]
  //                   ^^^^^^^^^^^^^^^^
  aU = _mm512_extract_upper256( a64 );
  bU = _mm512_extract_upper256( b64 );
  ps_a3 = _mm512_low16_cvtepi8_ps( aU );
  ps_b3 = _mm512_low16_cvtepi8_ps( bU );

  // multiply 16x floats and add to running sums
  *ssq_a = _mm512_fmadd_ps( ps_a0, ps_a0, *ssq_a );
  *ssq_b = _mm512_fmadd_ps( ps_b0, ps_b0, *ssq_b );
  *sum   = _mm512_fmadd_ps( ps_a0, ps_b0, *sum );

  *ssq_a = _mm512_fmadd_ps( ps_a1, ps_a1, *ssq_a );
  *ssq_b = _mm512_fmadd_ps( ps_b1, ps_b1, *ssq_b );
  *sum   = _mm512_fmadd_ps( ps_a1, ps_b1, *sum );
  
  *ssq_a = _mm512_fmadd_ps( ps_a2, ps_a2, *ssq_a );
  *ssq_b = _mm512_fmadd_ps( ps_b2, ps_b2, *ssq_b );
  *sum   = _mm512_fmadd_ps( ps_a2, ps_b2, *sum );
  
  *ssq_a = _mm512_fmadd_ps( ps_a3, ps_a3, *ssq_a );
  *ssq_b = _mm512_fmadd_ps( ps_b3, ps_b3, *ssq_b );
  *sum   = _mm512_fmadd_ps( ps_a3, ps_b3, *sum );
}



/*******************************************************************//**
 *
 * Horizontal add 16 floats into rightmost 32-bit float
 *
 ***********************************************************************
 */
__inline static __m512 * __hadd_ps_avx512( __m512 *ps ) {
  // ps = {abcdefghijklmnop}
  //
  // LAT  CPI    ___A___ ___B___ ___C___ ___D___      ___C___ ___D___ ___A___ ___B___
  //  3    1    {a b c d e f g h i j k l m n o p} -> {i j k l m n o p a b c d e f g h}
  //            
  __m512 zmm1 = _mm512_shuffle_f32x4( *ps, *ps, _MM_SHUFFLE(1,0,3,2) );

  //  4   0.5   {abcdefghijklmnop} + {ijklmnopabcdefgh} -> {a+i b+j c+k d+l e+m f+n g+o h+p i+a j+b k+c l+d m+e n+f o+g p+h}
  //                                                        _________ dont care ___________ ___________ q1 - q8 ___________
  __m512 zmm2 = _mm512_add_ps( *ps, zmm1 );

  //             ____128____ ____128____ ____128____ ____128____
  //  3    1    {----------------------- q1 q2 q3 q4 q5 q6 q7 q8} -> { xx xx xx xx xx xx xx xx q5 q6 q7 q8 q1 q2 q3 q4 }
  //                                                                                           _______________________
  zmm1 = _mm512_shuffle_f32x4( zmm2, zmm2, _MM_SHUFFLE(0,0,0,1) );

  //  4   0.5   { ... q1 q2 q3 q4 q5 q6 q7 q8 } + { ... q5 q6 q7 q8 q1 q2 q3 q4 } -> { ... q1+q5 q2+q6 q3+q7 q4+q8 q5+q1 q6+q2 q7+q3 q8+q4 }
  //                                                                                   __________ dont care ______ ______ r1 - r4 ________
  zmm1 = _mm512_add_ps( zmm2, zmm1 );  

  //             ____128____ ____128____ ____128____ ____128____
  //  1    1    {---------------------------------- r1 r2 r3 r4} -> { xx xx xx xx xx xx xx xx xx xx xx xx r3 r4 r1 r2}
  //                                                                                                      ___________
  zmm2 = _mm512_shuffle_ps( zmm1, zmm1, _MM_SHUFFLE(1,0,3,2) );

  //  4   0.5   { ... r1 r2 r3 r4 } + { ... r3 r4 r1 r2 } -> { ... r1+r3 r2+r4 r3+r1 r4+r2 }
  //                                                           ___dont care___ ___s1-s2___
  zmm1 = _mm512_add_ps( zmm1, zmm2 );

  //  1    1    { ... r1+r3 r2+r4 r3+r1 r4+r2 } -> { ... 0 r1+r3 r2+r4 r3+r1 }
  //                                                     ___ignore____ _s1__
  zmm2 = _mm512_castsi512_ps( _mm512_bsrli_epi128( _mm512_castps_si512( zmm1 ), 4 ) );

  //  4   0.5   { ... r3+r1 r4+r2 } + { ... r3+r1 } -> { ... r4+r2+r3+r1 }
  //                                                         ____sum____
  *ps = _mm512_add_ps( zmm1, zmm2 );

  return ps;
}



/*******************************************************************//**
 *
 * Extract the lower 32 bits as float
 *
 ***********************************************************************
 */
__inline static float __extract_float_avx512( __m512 *ps ) {
  int x = _mm512_cvtsi512_si32( _mm512_castps_si512( *ps ) );
  return *(float*)&x;
}



/*******************************************************************//**
 *
 * Return square root of horizontal sum of 16 floats in ps
 *
 ***********************************************************************
 */
__inline static float __sqrt_hadd_ps_avx512( __m512 *ps ) {
  // 12    3    { ... sum } -> { ... sqrt(sum) }
  __m128 root = _mm_sqrt_ss( _mm512_castps512_ps128( *__hadd_ps_avx512( ps ) ) );

  //  2    1    Extract lower 32-bit value
  union { int32_t i32; float f; } S;
  S.i32 = _mm_cvtsi128_si32( _mm_castps_si128( root ) );

  return S.f;
}



/*******************************************************************//**
 *
 * Return reciprocal square root of horizontal sum of 16 floats in ps
 *
 ***********************************************************************
 */
__inline static float __rsqrt_hadd_ps_avx512( __m512 *ps ) {
  //  4    1    { ... sum } -> { ... 1/sqrt(sum) }
  __m256 rsqrt = _mm256_rsqrt_ps( _mm512_castps512_ps256( *__hadd_ps_avx512( ps ) ) );

  //  2    1    Extract lower 32-bit value
  union { int32_t i32; float f; } S;
  S.i32 = _mm256_cvtsi256_si32( _mm256_castps_si256( rsqrt ) );

  return S.f;
}



/*******************************************************************//**
 * ecld( A, B ) -> euclidean distance
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static double __avx512_ecld_pi8( const BYTE *A, const BYTE *B, float fA, float fB, int len ) {
#ifdef __cxlib_AVX512_MINIMUM__
  // Sixteen bins of running (float) sums of squares of differences
  __m512 ssqdiff = _mm512_setzero_ps();
  // Process chunks of 64 bytes, any trailing non-multiple of 64 is ignored!!
  int N = len >> 6;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  __m512 ascale = _mm512_broadcastss_ps( _mm_load_ps( &fA ) );
  __m512 bscale = _mm512_broadcastss_ps( _mm_load_ps( &fB ) );
  for( int i=0; i<N; i++, a_cur += sizeof(__m512i), b_cur += sizeof(__m512i) ) {
    // Aggregate sum bins from partial squares of differences
    __sum_sqdiff_avx512( a_cur, b_cur, &ascale, &bscale, &ssqdiff );
  }

  return __sqrt_hadd_ps_avx512( &ssqdiff );
#else
  return __avx2_ecld_pi8( A, B, fA, fB, len );
#endif
}



/*******************************************************************//**
 * ssq( A ) -> sum
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static double __avx512_ssq_pi8( const BYTE *A, int len ) {
#ifdef __cxlib_AVX512_MINIMUM__
  // Sixteen bins of running (float) sums of squares
  __m512 ssq = _mm512_setzero_ps();
  // Process chunks of 64 bytes, any trailing non-multiple of 64 is ignored!!
  int N = len >> 6;
  const BYTE *a_cur = A;
  for( int i=0; i<N; i++, a_cur += sizeof(__m512i) ) {
    // Aggregate sum bins from partial squares
    __ssq_avx512( a_cur, &ssq );
  }

  return __extract_float_avx512( __hadd_ps_avx512( &ssq ) );
#else
  return __avx2_ssq_pi8( A, len );
#endif
}



/*******************************************************************//**
 * rsqrtssq( A ) -> reciprocal square root of sum of squares
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static double __avx512_rsqrtssq_pi8( const BYTE *A, int len ) {
#ifdef __cxlib_AVX512_MINIMUM__
  // Sixteen bins of running (float) sums of squares
  __m512 ssq = _mm512_setzero_ps();
  // Process chunks of 64 bytes, any trailing non-multiple of 64 is ignored!!
  int N = len >> 6;
  const BYTE *a_cur = A;
  for( int i=0; i<N; i++, a_cur += sizeof(__m512i) ) {
    // Aggregate sum bins from partial squares
    __ssq_avx512( a_cur, &ssq );
  }

  return __rsqrt_hadd_ps_avx512( &ssq );
#else
  return __avx2_rsqrtssq_pi8( A, len );
#endif
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static double __avx512_dp_pi8( const BYTE *A, const BYTE *B, int len ) {
#ifdef __cxlib_AVX512_MINIMUM__
  // Sixteen bins of running (float) sums of products
  __m512 sum = _mm512_setzero_ps();
  // Process chunks of 64 bytes, any trailing non-multiple of 64 is ignored!!
  int N = len >> 6;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  for( int i=0; i<N; i++, a_cur += sizeof(__m512i), b_cur += sizeof(__m512i) ) {
    __dp_avx512( a_cur, b_cur, &sum );
  }

  return __extract_float_avx512( __hadd_ps_avx512( &sum ) );
#else
  return __avx2_dp_pi8( A, B, len );
#endif
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static double __avx512_cos_pi8( const BYTE *A, const BYTE *B, int len ) {
#ifdef __cxlib_AVX512_MINIMUM__
  __m512 sum = _mm512_setzero_ps();
  __m512 ssq_a = _mm512_setzero_ps();
  __m512 ssq_b = _mm512_setzero_ps();
  // Process chunks of 64 bytes, any trailing non-multiple of 64 is ignored!!
  int N = len >> 6;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  for( int i=0; i<N; i++, a_cur += sizeof(__m512i), b_cur += sizeof(__m512i) ) {
    __dp_ssq_avx512( a_cur, b_cur, &sum, &ssq_a, &ssq_b );
  }

  // Dot product
  double dp = __extract_float_avx512( __hadd_ps_avx512( &sum ) );
  // Reciprocal norms
  double rnorm_a = __rsqrt_hadd_ps_avx512( &ssq_a );
  double rnorm_b = __rsqrt_hadd_ps_avx512( &ssq_b );
  // Cosine
  double cosine = dp * rnorm_a * rnorm_b;
  if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
    cosine = (double)((cosine > 0.0) - (cosine < 0.0));
  }
  return cosine;
#else
  return __avx2_cos_pi8( A, B, len );
#endif
}



/*******************************************************************//**
 * EuclideanDistance( A, B )
 *
 * Byte array length / Vector length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static void __eval_avx512_ecld_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx512_ecld_pi8( a_data, b_data, a_scale, b_scale, len );
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
 * Byte array length / Vector length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static void __eval_avx512_ssq_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __avx512_ssq_pi8( data, len ) * scale * scale;
  }
#else
  __eval_scalar_ssq_pi8( self );
#endif
}



/*******************************************************************//**
 * ReciprocalSquareRootSumOfSquares( A )
 *
 * Byte array length / Vector length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static void __eval_avx512_rsqrtssq_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __avx512_rsqrtssq_pi8( data, len ) / scale;
  }
#else
  __eval_scalar_rsqrtssq_pi8( self );
#endif
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static void __eval_avx512_dp_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx512_dp_pi8( a_data, b_data, len ) * a_scale * b_scale;
  }
#else
  __eval_scalar_dp_pi8( self );
#endif
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 64.
 *
 *
 ***********************************************************************
 */
static void __eval_avx512_cos_pi8( vgx_Evaluator_t *self ) {
#ifdef CXPLAT_ARCH_HASFMA
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __avx512_cos_pi8( a_data, b_data, len );
  }
#else
  __eval_scalar_cos_pi8( self );
#endif
}




#endif
