/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _neon.h
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

#ifndef _VGX_VXEVAL_MODULES_NEON_H
#define _VGX_VXEVAL_MODULES_NEON_H

#include "_memory.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_neon_ecld_pi8( vgx_Evaluator_t *self );
static void __eval_neon_ssq_pi8( vgx_Evaluator_t *self );
static void __eval_neon_rsqrtssq_pi8( vgx_Evaluator_t *self );
static void __eval_neon_dp_pi8( vgx_Evaluator_t *self );
static void __eval_neon_cos_pi8( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static float __horizontal_sum( float32x4_t f32x4 ) {
  // Horizontal sum of final float32x4_t accumulator
  float32x2_t sum2 = vadd_f32( vget_low_f32(f32x4), vget_high_f32(f32x4) );
  float result = vget_lane_f32(sum2, 0) + vget_lane_f32(sum2, 1);
  return result;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static float __sqrt_horizontal_sum( float32x4_t f32x4 ) {
  float32x2_t vhsum = vdup_n_f32( __horizontal_sum( f32x4 ) );
  float32x2_t vsqrt = vsqrt_f32( vhsum );
  return vget_lane_f32(vsqrt, 0); 
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static float __rsqrt_horizontal_sum( float32x4_t f32x4 ) {
  // Use one round of Newton-Raphson refinement since the baseline neon precision is poor
  // y1 = y0 * (3 - x * y0^2) / 2
  float32x2_t x = vdup_n_f32( __horizontal_sum( f32x4 ) );
  float32x2_t y0 = vrsqrte_f32( x ); // Initial approx

  // One Newton-Raphson refinement:
  float32x2_t y0_sq = vmul_f32(y0, y0);     // y0^2
  float32x2_t x_y0sq = vmul_f32(x, y0_sq);  // x * y0^2
  float32x2_t half = vdup_n_f32(0.5f);
  float32x2_t three = vdup_n_f32(3.0f); 
  float32x2_t nr = vmul_f32(half, vmul_f32(y0, vsub_f32(three, x_y0sq)));  // y1
  // Extract lane 0
  return vget_lane_f32(nr, 0);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __sum_sqdiff_neon( const BYTE *A, const BYTE *B, float32x4_t fAv, float32x4_t fBv, float32x4_t *ssd ) {

  // Load 16 bytes from each input
  int8x16_t a8 = vld1q_s8((const int8_t*)A);
  int8x16_t b8 = vld1q_s8((const int8_t*)B);
  
  // Split both A and B into two 8-byte halves and widen to int16
  int16x8_t a16L = vmovl_s8(vget_low_s8(a8));   // A: Lower 8 bytes as 8 x int16
  int16x8_t a16H = vmovl_s8(vget_high_s8(a8));  // A: Upper 8 bytes as 8 x int16
  int16x8_t b16L = vmovl_s8(vget_low_s8(b8));   // B: Lower 8 bytes as 8 x int16
  int16x8_t b16H = vmovl_s8(vget_high_s8(b8));  // B: Upper 8 bytes as 8 x int16

  // Widen to int32 and convert to float
  float32x4_t af0 = vcvtq_f32_s32(vmovl_s16(vget_low_s16(a16L)));   //  A  0 - 3
  float32x4_t af1 = vcvtq_f32_s32(vmovl_s16(vget_high_s16(a16L)));  //  A  4 - 7
  float32x4_t af2 = vcvtq_f32_s32(vmovl_s16(vget_low_s16(a16H)));   //  A  8 - 11
  float32x4_t af3 = vcvtq_f32_s32(vmovl_s16(vget_high_s16(a16H)));  //  A 12 - 15
  float32x4_t bf0 = vcvtq_f32_s32(vmovl_s16(vget_low_s16(b16L)));   //  B  0 - 3
  float32x4_t bf1 = vcvtq_f32_s32(vmovl_s16(vget_high_s16(b16L)));  //  B  4 - 7
  float32x4_t bf2 = vcvtq_f32_s32(vmovl_s16(vget_low_s16(b16H)));   //  B  8 - 11
  float32x4_t bf3 = vcvtq_f32_s32(vmovl_s16(vget_high_s16(b16H)));  //  B 12 - 15

  // Scaling
  af0 = vmulq_f32( af0, fAv ); //  A  0 - 3
  af1 = vmulq_f32( af1, fAv ); //  A  4 - 7
  af2 = vmulq_f32( af2, fAv ); //  A  8 - 11
  af3 = vmulq_f32( af3, fAv ); //  A 12 - 15
  bf0 = vmulq_f32( bf0, fBv ); //  B  0 - 3
  bf1 = vmulq_f32( bf1, fBv ); //  B  4 - 7
  bf2 = vmulq_f32( bf2, fBv ); //  B  8 - 11
  bf3 = vmulq_f32( bf3, fBv ); //  B 12 - 15

  // Compute difference
  float32x4_t diff0 = vsubq_f32(af0, bf0);  //  0 - 3
  float32x4_t diff1 = vsubq_f32(af1, bf1);  //  4 - 7
  float32x4_t diff2 = vsubq_f32(af2, bf2);  //  8 - 11
  float32x4_t diff3 = vsubq_f32(af3, bf3);  // 12 - 15

  // Fused multiply-add: acc += diff * diff
  *ssd = vfmaq_f32(*ssd, diff0, diff0); //  0 - 3
  *ssd = vfmaq_f32(*ssd, diff1, diff1); //  4 - 7
  *ssd = vfmaq_f32(*ssd, diff2, diff2); //  8 - 11
  *ssd = vfmaq_f32(*ssd, diff3, diff3); // 12 - 15
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __ssq_neon( const int8_t *data, float32x4_t *ssq ) {

  // Load 16 signed int8 into NEON vector
  int8x16_t v_int8 = vld1q_s8(data);

  // Split into two 8-byte halves and widen to int16
  int16x8_t v_low = vmovl_s8(vget_low_s8(v_int8));   // Lower 8 bytes as 8 x int16
  int16x8_t v_high = vmovl_s8(vget_high_s8(v_int8)); // Upper 8 bytes as 8 x int16

  // Square all 16 int16 and widen to 16 int32, 4 at a time
  int32x4_t v_low_sq_0 = vmull_s16(vget_low_s16(v_low), vget_low_s16(v_low)); // square lower 4 x int16 of the first 8 int16
  int32x4_t v_low_sq_1 = vmull_s16(vget_high_s16(v_low), vget_high_s16(v_low)); // square upper 4 x int16 of the first 8 int16
  int32x4_t v_high_sq_0 = vmull_s16(vget_low_s16(v_high), vget_low_s16(v_high)); // square lower 4 x int16 of the second 8 int16
  int32x4_t v_high_sq_1 = vmull_s16(vget_high_s16(v_high), vget_high_s16(v_high)); // square upper 4 x int16 of the second int16

  // Convert int32 to float32
  float32x4_t f0 = vcvtq_f32_s32(v_low_sq_0);   //  0 - 3
  float32x4_t f1 = vcvtq_f32_s32(v_low_sq_1);   //  4 - 7
  float32x4_t f2 = vcvtq_f32_s32(v_high_sq_0);  //  8 - 11
  float32x4_t f3 = vcvtq_f32_s32(v_high_sq_1);  // 12 - 15

  // Maintain 4 lanes of square sums
  *ssq = vaddq_f32( *ssq, f0 );
  *ssq = vaddq_f32( *ssq, f1 );
  *ssq = vaddq_f32( *ssq, f2 );
  *ssq = vaddq_f32( *ssq, f3 );
}



/*******************************************************************//**
 * ecld( A, B ) -> euclidean distance
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static double __neon_ecld_pi8( const BYTE *A, const BYTE *B, float fA, float fB, int len ) {
  // Four bins of running (float) sums of squares of differences
  float32x4_t acc = vdupq_n_f32(0.0f); // running sum

  // Process chunks of 16 bytes, any trailing non-multiple of 16 is ignored!!
  int N = len >> 4;
  const BYTE *a_cur = A;
  const BYTE *b_cur = B;
  float32x4_t fAv = vdupq_n_f32(fA);
  float32x4_t fBv = vdupq_n_f32(fB);
  for( int i=0; i<N; i++, a_cur += sizeof(float32x4_t), b_cur += sizeof(float32x4_t) ) {
    // Aggregate sum bins from partial squares of differences
    __sum_sqdiff_neon( a_cur, b_cur, fAv, fBv, &acc );
  }

  // Square root of horizontal sum of final float32x4_t accumulator
  return __sqrt_horizontal_sum( acc );
}



/*******************************************************************//**
 * ssq( A ) -> sum
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static double __neon_ssq_pi8( const BYTE *A, int len ) {
  // Four bins of running (float) square sums
  float32x4_t acc = vdupq_n_f32(0.0f); // running sum

  // Process chunks of 16 bytes, any trailing non-multiple of 16 is ignored!!
  int N = len >> 4;
  const int8_t *a_cur = (const int8_t*)A;
  for( int i=0; i<N; i++, a_cur += sizeof(float32x4_t) ) {
    // Aggregate sum bins from partial square sums
    __ssq_neon( a_cur, &acc );
  }

  // Horizontal sum of final float32x4_t accumulator
  return __horizontal_sum( acc );
}



/*******************************************************************//**
 * rsqrtssq( A ) -> reciprocal square root of sum of squares
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static double __neon_rsqrtssq_pi8( const BYTE *A, int len ) {
  // Four bins of running (float) square sums
  float32x4_t acc = vdupq_n_f32(0.0f); // running sum

  // Process chunks of 16 bytes, any trailing non-multiple of 16 is ignored!!
  int N = len >> 4;
  const int8_t *a_cur = (const int8_t*)A;
  for( int i=0; i<N; i++, a_cur += sizeof(float32x4_t) ) {
    // Aggregate sum bins from partial square sums
    __ssq_neon( a_cur, &acc );
  }

  // Horizontal sum of final float32x4_t accumulator
  return __rsqrt_horizontal_sum( acc );
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static double __neon_dp_pi8( const BYTE *A, const BYTE *B, int len ) {
  // Four bins of running (float) sums of products
  int32x4_t acc = vdupq_n_s32(0); // running sum

  // Process chunks of 16 bytes, any trailing non-multiple of 16 is ignored!!
  int N = len >> 4;
  const int8_t *a_cur = (const int8_t*)A;
  const int8_t *b_cur = (const int8_t*)B;
  for( int i=0; i<N; i++, a_cur += sizeof(int8x16_t), b_cur += sizeof(int8x16_t) ) {
    acc = vdotq_s32( acc, vld1q_s8( a_cur ), vld1q_s8( b_cur ) );
  }

  // Convert to float
  float32x4_t float_acc = vcvtq_f32_s32(acc);

  // Horizontal sum of final float32x4_t accumulator
  return __horizontal_sum( float_acc );
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static double __neon_cos_pi8( const BYTE *A, const BYTE *B, int len ) {

  int32x4_t dp_acc = vdupq_n_s32(0); // running dot product
  float32x4_t ssq_a = vdupq_n_f32(0.0f); // running square sum of A
  float32x4_t ssq_b = vdupq_n_f32(0.0f); // running square sum of B

  // Process chunks of 16 bytes, any trailing non-multiple of 16 is ignored!!
  int N = len >> 4;
  const int8_t *a_cur = (const int8_t*)A;
  const int8_t *b_cur = (const int8_t*)B;
  for( int i=0; i<N; i++, a_cur += sizeof(int8x16_t), b_cur += sizeof(int8x16_t) ) {
    // Accumulate dot product
    dp_acc = vdotq_s32( dp_acc, vld1q_s8( a_cur ), vld1q_s8( b_cur ) );
    // Accumulate sum of squares for A and B
    __ssq_neon( a_cur, &ssq_a );
    __ssq_neon( b_cur, &ssq_b );
  }

  // Convert dot product accumulator to float
  float32x4_t float_dpacc = vcvtq_f32_s32(dp_acc);

  // Horizontal sum of final float32x4_t accumulator
  double dp = __horizontal_sum( float_dpacc );

  // Reciprocal norms
  double rnorm_a = __rsqrt_horizontal_sum( ssq_a );
  double rnorm_b = __rsqrt_horizontal_sum( ssq_b );
  
  // Cosine
  double cosine = dp * rnorm_a * rnorm_b;
  if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
    cosine = (double)((cosine > 0.0) - (cosine < 0.0));
  }
  return cosine;
}



/*******************************************************************//**
 * EuclideanDistance( A, B )
 *
 * Byte array length / Vector length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static void __eval_neon_ecld_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __neon_ecld_pi8( a_data, b_data, a_scale, b_scale, len );
  }
  else {
    SET_REAL_VALUE( self, INFINITY );
  }
}



/*******************************************************************//**
 * SumOfSquares( A )
 *
 * Byte array length / Vector length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static void __eval_neon_ssq_pi8( vgx_Evaluator_t *self ) {
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __neon_ssq_pi8( data, len ) * scale * scale;
  }
}



/*******************************************************************//**
 * ReciprocalSquareRootSumOfSquares( A )
 *
 * Byte array length / Vector length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static void __eval_neon_rsqrtssq_pi8( vgx_Evaluator_t *self ) {
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __neon_rsqrtssq_pi8( data, len ) / scale;
  }
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static void __eval_neon_dp_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __neon_dp_pi8( a_data, b_data, len ) * a_scale * b_scale;
  }
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
static void __eval_neon_cos_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __neon_cos_pi8( a_data, b_data, len );
  }
}




#endif
