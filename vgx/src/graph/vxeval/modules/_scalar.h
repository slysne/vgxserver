/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _scalar.h
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

#ifndef _VGX_VXEVAL_MODULES_SCALAR_H
#define _VGX_VXEVAL_MODULES_SCALAR_H

#include "_memory.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_scalar_ecld_pi8( vgx_Evaluator_t *self );
static void __eval_scalar_ssq_pi8( vgx_Evaluator_t *self );
static void __eval_scalar_dp_pi8( vgx_Evaluator_t *self );
static void __eval_scalar_cos_pi8( vgx_Evaluator_t *self );



/*******************************************************************//**
 * ecld( A, B ) -> euclidean distance
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 *
 ***********************************************************************
 */
static double __scalar_ecld_pi8( const BYTE *A, const BYTE *B, float fA, float fB, int len ) {
  const int8_t *pa = (const int8_t*)A;
  const int8_t *paend = pa + len;
  const int8_t *pb = (const int8_t*)B;

  double a, b, d;
  double sum = 0.0;

  while( pa < paend ) {
    a = (double)*pa++ * fA;
    b = (double)*pb++ * fB;
    d = a - b;
    sum += d * d;
  }

  double ecld = sqrt( sum );
  return ecld;
}



/*******************************************************************//**
 * ssq( A ) -> sum
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 *
 *
 ***********************************************************************
 */
static double __scalar_ssq_pi8( const BYTE *A, int len ) {
  const int8_t *pa = (const int8_t*)A;
  const int8_t *paend = pa + len;

  double a;
  double ssq = 0.0;

  while( pa < paend ) {
    a = (double)*pa++;
    ssq += a * a;
  }

  return ssq;
}



/*******************************************************************//**
 * rsqrtssq( A ) -> reciprocal square root of sum of squares
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 *
 *
 ***********************************************************************
 */
static double __scalar_rsqrtssq_pi8( const BYTE *A, int len ) {
  double ssq = __scalar_ssq_pi8( A, len );
  if( ssq > 0.0 ) {
    return 1.0 / sqrt(ssq);
  }
  else {
    return 0.0;
  }
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 *
 ***********************************************************************
 */
static double __scalar_dp_pi8( const BYTE *A, const BYTE *B, int len ) {
  const int8_t *pa = (const int8_t*)A;
  const int8_t *paend = pa + len;
  const int8_t *pb = (const int8_t*)B;

  double a, b;
  double dp = 0.0;

  while( pa < paend ) {
    a = (double)*pa++;
    b = (double)*pb++;
    dp += a * b;
  }

  return dp;
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 *
 ***********************************************************************
 */
static double __scalar_cos_pi8( const BYTE *A, const BYTE *B, int len ) {
  double dp = __scalar_dp_pi8( A, B, len );
  double ssqA = __scalar_ssq_pi8( A, len );
  double ssqB = __scalar_ssq_pi8( B, len );
  double m = sqrt( ssqA * ssqB );
  double cosine = m > 0.0 ? dp / m : 0.0;
  if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
    cosine = (double)((cosine > 0.0) - (cosine < 0.0));
  }
  return cosine;
}



/*******************************************************************//**
 * EuclideanDistance( A, B )
 *
 *
 ***********************************************************************
 */
static void __eval_scalar_ecld_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __scalar_ecld_pi8( a_data, b_data, a_scale, b_scale, len );
  }
  else {
    SET_REAL_VALUE( self, INFINITY );
  }
}



/*******************************************************************//**
 * SumOfSquares( A )
 *
 *
 ***********************************************************************
 */
static void __eval_scalar_ssq_pi8( vgx_Evaluator_t *self ) {
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __scalar_ssq_pi8( data, len ) * scale * scale;
  }
}



/*******************************************************************//**
 * ReciprocalSquareRootSumOfSquares( A )
 *
 *
 ***********************************************************************
 */
static void __eval_scalar_rsqrtssq_pi8( vgx_Evaluator_t *self ) {
  const BYTE *data;
  float scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_one( self, &data, &scale, &len );
  if( px ) {
    px->real = __scalar_rsqrtssq_pi8( data, len ) / scale;
  }
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 *
 ***********************************************************************
 */
static void __eval_scalar_dp_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __scalar_dp_pi8( a_data, b_data, len ) * a_scale * b_scale;
  }
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 *
 ***********************************************************************
 */
static void __eval_scalar_cos_pi8( vgx_Evaluator_t *self ) {
  const BYTE *a_data, *b_data;
  float a_scale, b_scale;
  int len;
  vgx_EvalStackItem_t *px = __eval_prepare_two( self, &a_data, &b_data, &a_scale, &b_scale, &len );
  if( px ) {
    px->real = __scalar_cos_pi8( a_data, b_data, len );
  }
}




#endif
