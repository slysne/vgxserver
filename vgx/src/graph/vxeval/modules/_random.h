/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _random.h
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

#ifndef _VGX_VXEVAL_MODULES_RANDOM_H
#define _VGX_VXEVAL_MODULES_RANDOM_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_nullary_random_real( vgx_Evaluator_t *self );
static void __eval_nullary_random_bits( vgx_Evaluator_t *self );
static void __eval_binary_random_int( vgx_Evaluator_t *self );
static void __eval_unary_hash( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_nullary_random_real( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  double r = (double)rand63() / (double)LLONG_MAX;
  SET_REAL_PITEM_VALUE( item, r );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_nullary_random_bits( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_BITVECTOR_PITEM_VALUE( item, rand64() );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_random_int( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  int64_t L, H;
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    L = px->integer;
    H = y.integer;
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    L = px->integer;
    H = (int64_t)y.real;
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    L = (int64_t)px->real;
    H = y.integer;
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    L = (int64_t)px->real;
    H = (int64_t)y.real;
    break;
  default:
    L = 0;
    H = 1;
  }

  int64_t r =  L + (H > L ? rand63() % (H - L) : 0);
  SET_INTEGER_PITEM_VALUE( px, r );
}



/*******************************************************************//**
 * hash( x )
 ***********************************************************************
 */
static void __eval_unary_hash( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int64_t h;
  switch( px->type ) {
  // ihash64() of integer
  case STACK_ITEM_TYPE_INTEGER:
    px->integer = ihash64( px->integer );
    return;
  // ihash64() of bits if real or bitvector
  case STACK_ITEM_TYPE_REAL:
  case STACK_ITEM_TYPE_BITVECTOR:
    SET_INTEGER_PITEM_VALUE( px, ihash64( px->bits ) );
    return;
  // simple hash
  case STACK_ITEM_TYPE_KEYVAL:
    SET_INTEGER_PITEM_VALUE( px, vgx_cstring_array_map_hashkey( &px->bits ) );
    return;
  // ihash64() of address
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, ihash64( (uint64_t)px->vertex ) );
    return;
  // hash64() of string
  case STACK_ITEM_TYPE_CSTRING:
    SET_INTEGER_PITEM_VALUE( px, CStringHash64( px->CSTR__str ) );
    return;
  // Interpret hash(vector) as its fingerprint
  case STACK_ITEM_TYPE_VECTOR:
    h = CALLABLE( px->vector )->Fingerprint( px->vector );
    SET_INTEGER_PITEM_VALUE( px, h );
    return;
  // Interpret hash(vertex.id) as the low part of its internalid
  case STACK_ITEM_TYPE_VERTEXID:
    {
      vgx_Vertex_t *vertex = (vgx_Vertex_t*)((char*)px->vertexid - offsetof( vgx_Vertex_t, identifier ));
      SET_INTEGER_PITEM_VALUE( px, __vertex_internalid( vertex )->L );
    }
    return;
  //
  default:
    SET_INTEGER_PITEM_VALUE( px, 0 );
  }

}






#endif
