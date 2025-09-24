/*
###################################################
#
# File:   _pi8.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_PI8_H
#define _VGX_VXEVAL_MODULES_PI8_H

#include "_memory.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t * __eval_prepare_one( vgx_Evaluator_t *self, const BYTE **data, float *scale, int *len );
static vgx_EvalStackItem_t * __eval_prepare_two( vgx_Evaluator_t *self, const BYTE **a_data, const BYTE **b_data, float *a_scale, float *b_scale, int *len );

static double __ecld_pi8_inf( const BYTE *A, const BYTE *B, float fA, float fB, int len );
static double __ssq_pi8_zero( const BYTE *A, int len );
static double __dp_pi8_zero( const BYTE *A, const BYTE *B, int len );
static double __cos_pi8_zero( const BYTE *A, const BYTE *B, int len );

static void __eval_ecld_pi8( vgx_Evaluator_t *self );
static void __eval_ssq_pi8( vgx_Evaluator_t *self );
static void __eval_dp_pi8( vgx_Evaluator_t *self );
static void __eval_cos_pi8( vgx_Evaluator_t *self );
static void __eval_ham_pi8( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t * __eval_prepare_one( vgx_Evaluator_t *self, const BYTE **data, float *scale, int *len ) {
  vgx_EvalStackItem_t A = POP_ITEM( self );
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_REAL;
  px->real = 0.0;

  int32_t sz;

  switch( A.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    *data = (BYTE*)CStringValue( A.CSTR__str );
    sz = CStringLength( A.CSTR__str );
    *scale = 1.0f;
    break;
  case STACK_ITEM_TYPE_VECTOR:
    *data = (BYTE*)CALLABLE( A.vector )->Elements( A.vector );
    sz = A.vector->metas.vlen;
    *scale = CALLABLE( A.vector )->Scaler( A.vector );
    break;
  default:
    return NULL;
  }

  *len = sz;
  return px;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t * __eval_prepare_two( vgx_Evaluator_t *self, const BYTE **a_data, const BYTE **b_data, float *a_scale, float *b_scale, int *len ) {
  vgx_EvalStackItem_t B = POP_ITEM( self );
  vgx_EvalStackItem_t A = POP_ITEM( self );
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_REAL;
  px->real = 0.0;

  int32_t sz_a, sz_b;

  switch( A.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    *a_data = (BYTE*)CStringValue( A.CSTR__str );
    sz_a = CStringLength( A.CSTR__str );
    *a_scale = 1.0f;
    break;
  case STACK_ITEM_TYPE_VECTOR:
    *a_data = (BYTE*)CALLABLE( A.vector )->Elements( A.vector );
    sz_a = A.vector->metas.vlen;
    *a_scale = CALLABLE( A.vector )->Scaler( A.vector );
    break;
  default:
    return NULL;
  }

  switch( B.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    *b_data = (BYTE*)CStringValue( B.CSTR__str );
    sz_b = CStringLength( B.CSTR__str );
    *b_scale = 1.0f;
    break;
  case STACK_ITEM_TYPE_VECTOR:
    *b_data = (BYTE*)CALLABLE( B.vector )->Elements( B.vector );
    sz_b = B.vector->metas.vlen;
    *b_scale = CALLABLE( B.vector )->Scaler( B.vector );
    break;
  default:
    return NULL;
  }

  *len = minimum_value( sz_a, sz_b );
  return px;
}



/*******************************************************************//**
 * distance( A, B ) -> euclidean distance
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16.
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double __ecld_pi8_inf( const BYTE *A, const BYTE *B, float fA, float fB, int len ) {
  return INFINITY;
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
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double __ssq_pi8_zero( const BYTE *A, int len ) {
  return 0.0;
}



/*******************************************************************//**
 * rsqrtssq( A ) -> 1/sqrt(ssq)
 *
 * A is a packed byte array (i.e. a string interpreted as bytes)
 * Length must be a multiple of 16.
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double __rsqrtssq_pi8_inf( const BYTE *A, int len ) {
  return INFINITY;
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
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double __dp_pi8_zero( const BYTE *A, const BYTE *B, int len ) {
  return 0.0;
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
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static double __cos_pi8_zero( const BYTE *A, const BYTE *B, int len ) {
  return 0.0;
}



/*******************************************************************//**
 * EuclideanDistance( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16, 32 or 64
 * depending on the CPU max AVX version.
 * 
 * A and B byte arrays may be supplied as Vector instances, which are
 * automatically interpreted as byte arrays with scaling factors encoded
 * in the vector instances.
 * 
 * AVX    : 16
 * AVX2   : 32
 * AVX512 : 64
 *
 ***********************************************************************
 */
static void __eval_ecld_pi8( vgx_Evaluator_t *self ) {
  f_ecld_pi8( self );
}



/*******************************************************************//**
 * SumOfSquares( A )
 *
 * A is packed bytes array (i.e. strings interpreted as bytes)
 * Length must be a multiple of 16, 32 or 64 depending on the CPU max AVX version.
 * 
 * A byte array may be supplied as Vector instance, which is
 * automatically interpreted as byte array with scaling factor encoded
 * in the vector instance.
 * 
 * AVX    : 16
 * AVX2   : 32
 * AVX512 : 64
 *
 ***********************************************************************
 */
static void __eval_ssq_pi8( vgx_Evaluator_t *self ) {
  f_ssq_pi8( self );
}



/*******************************************************************//**
 * A dot B -> dot_product
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16, 32 or 64
 * depending on the CPU max AVX version.
 * 
 * AVX    : 16
 * AVX2   : 32
 * AVX512 : 64
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __eval_dp_pi8( vgx_Evaluator_t *self ) {
  f_dp_pi8( self );
}



/*******************************************************************//**
 * Cosine( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length. Length must be a multiple of 16, 32 or 64
 * depending on the CPU max AVX version.
 * 
 * AVX    : 16
 * AVX2   : 32
 * AVX512 : 64
 *
 ***********************************************************************
 */
static void __eval_cos_pi8( vgx_Evaluator_t *self ) {
  f_cos_pi8( self );
}



/*******************************************************************//**
 * Hamdist( A, B )
 *
 * Both A and B are packed bytes arrays (i.e. strings interpreted as bytes)
 * and must have equal length.
 *
 ***********************************************************************
 */
static void __eval_ham_pi8( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t B = POP_ITEM( self );
  vgx_EvalStackItem_t A = POP_ITEM( self );
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_INTEGER;
  px->integer = 64;
  vgx_Similarity_t *sim = self->graph->similarity;

  FP_t fpA = 0;
  FP_t fpB = 0;

  switch( A.type ) {
  case STACK_ITEM_TYPE_VECTOR:
    fpA = CALLABLE( A.vector )->Fingerprint( A.vector );
    break;
  case STACK_ITEM_TYPE_CSTRING:
    fpA = CALLABLE( sim->fingerprinter )->ComputeBytearray( sim->fingerprinter, (BYTE*)CStringValue( A.CSTR__str ), CStringLength( A.CSTR__str ), 0, NULL );
    break;
  default:
    return;
  }

  switch( B.type ) {
  case STACK_ITEM_TYPE_VECTOR:
    fpB = CALLABLE( B.vector )->Fingerprint( B.vector );
    break;
  case STACK_ITEM_TYPE_CSTRING:
    fpB = CALLABLE( sim->fingerprinter )->ComputeBytearray( sim->fingerprinter, (BYTE*)CStringValue( B.CSTR__str ), CStringLength( B.CSTR__str ), 0, NULL );
    break;
  default:
    return;
  }

  px->integer = __popcnt64( fpA ^  fpB );
}






#endif
