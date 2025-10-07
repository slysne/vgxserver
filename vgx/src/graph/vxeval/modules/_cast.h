/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _cast.h
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

#ifndef _VGX_VXEVAL_MODULES_CAST_H
#define _VGX_VXEVAL_MODULES_CAST_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_cast_int( vgx_Evaluator_t *self );
static void __eval_unary_cast_intr( vgx_Evaluator_t *self );
static void __eval_unary_cast_asint( vgx_Evaluator_t *self );
static void __eval_unary_cast_asbits( vgx_Evaluator_t *self );
static void __eval_unary_cast_real( vgx_Evaluator_t *self );
static void __eval_unary_cast_asreal( vgx_Evaluator_t *self );
static void __eval_unary_cast_bitvector( vgx_Evaluator_t *self );
static void __eval_variadic_bytes( vgx_Evaluator_t *self );
static void __eval_binary_cast_keyval( vgx_Evaluator_t *self );






/*******************************************************************//**
 * int( x )
 ***********************************************************************
 */
static void __eval_unary_cast_int( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  // Standard cast to int
  case STACK_ITEM_TYPE_REAL:
    if( isinf( px->real ) ) {
      SET_INTEGER_PITEM_VALUE( px, px->real > 0 ? LLONG_MAX : LLONG_MIN );
    }
    else {
      SET_INTEGER_PITEM_VALUE( px, (int64_t)px->real );
    }
    return;
  // Nan and None are cast to 0
  case STACK_ITEM_TYPE_NAN:
  case STACK_ITEM_TYPE_NONE:
    SET_INTEGER_PITEM_VALUE( px, 0 );
    return;
  // Interpret int(keyval) as its key
  case STACK_ITEM_TYPE_KEYVAL:
    SET_INTEGER_PITEM_VALUE( px, vgx_cstring_array_map_key( &px->bits ) );
    return;
  // Interpret int(vertex) as its address
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->vertex );
    return;
  // Interpret int(string) as its address
  case STACK_ITEM_TYPE_CSTRING:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->CSTR__str );
    return;
  // Interpret int(vector) as its address
  case STACK_ITEM_TYPE_VECTOR:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->vector );
    return;
  // Interpret int(vertex.id) as its address
  case STACK_ITEM_TYPE_VERTEXID:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->vertexid );
    return;
  default:
    px->type = STACK_ITEM_TYPE_INTEGER;
  }
}



/*******************************************************************//**
 * intr( x )
 ***********************************************************************
 */
static void __eval_unary_cast_intr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_REAL ) {
    px->real = round( px->real );
  }
  __eval_unary_cast_int( self );
}



/*******************************************************************//**
 * asint( x )
 ***********************************************************************
 */
static void __eval_unary_cast_asint( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  // Reinterpret double bits as integer
  case STACK_ITEM_TYPE_REAL:
    SET_INTEGER_PITEM_VALUE( px, *((int64_t*)&px->real) );
    return;
  // Nan and None are cast to zero
  case STACK_ITEM_TYPE_NAN:
  case STACK_ITEM_TYPE_NONE:
    SET_INTEGER_PITEM_VALUE( px, 0 );
    return;
  // Interpret asint(vertex) as its address
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->vertex );
    return;
  // Interpret asint(string) as its value converted to integer, or 0 if not a number
  case STACK_ITEM_TYPE_CSTRING:
  {
    int64_t val = CALLABLE( px->CSTR__str )->AsInteger( px->CSTR__str );
    SET_INTEGER_PITEM_VALUE( px, val );
    return;
  }
  // Interpret asint(vector) as its length
  case STACK_ITEM_TYPE_VECTOR:
  {
    const vgx_Vector_t *vec = px->vector;
    int len = CALLABLE( vec )->Length( vec );
    SET_INTEGER_PITEM_VALUE( px, len );
    return;
  }
  // Interpret asint(vertex.id) as its address
  case STACK_ITEM_TYPE_VERTEXID:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)px->vertexid );
    return;
  default:
    px->type = STACK_ITEM_TYPE_INTEGER;
  }
}



/*******************************************************************//**
 * asbits( x )
 ***********************************************************************
 */
static void __eval_unary_cast_asbits( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, px->bits );
}



/*******************************************************************//**
 * real( x )
 ***********************************************************************
 */
static void __eval_unary_cast_real( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  // Standard cast to double
  case STACK_ITEM_TYPE_INTEGER:
    SET_REAL_PITEM_VALUE( px, (double)px->integer );
    return;
  // Nan is float nan
  case STACK_ITEM_TYPE_NAN:
    SET_REAL_PITEM_VALUE( px, (double)NAN );
    return;
  // None is zero
  case STACK_ITEM_TYPE_NONE:
    SET_REAL_PITEM_VALUE( px, 0.0 );
    return;
  // Interpret real(vertex) as its address
  case STACK_ITEM_TYPE_VERTEX:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->vertex );
    return;
  // Interpret real(string) as its address
  case STACK_ITEM_TYPE_CSTRING:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->CSTR__str );
    return;
  // Interpret real(keyval) as its value
  case STACK_ITEM_TYPE_KEYVAL:
    SET_REAL_PITEM_VALUE( px, vgx_cstring_array_map_val( &px->bits ) );
    return;
  // Interpret real(vector) as its address
  case STACK_ITEM_TYPE_VECTOR:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->vector );
    return;
  // Interpret real(vertex.id) as its address
  case STACK_ITEM_TYPE_VERTEXID:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->vertexid );
    return;
  default:
    px->type = STACK_ITEM_TYPE_REAL;
  }
}



/*******************************************************************//**
 * asreal( x )
 ***********************************************************************
 */
static void __eval_unary_cast_asreal( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  // Reinterpret integer bits as double
  case STACK_ITEM_TYPE_INTEGER:
    SET_REAL_PITEM_VALUE( px, *((double*)&px->integer) );
    return;
  // Infinity "truncated" to large number
  case STACK_ITEM_TYPE_REAL:
    if( isinf( px->real ) ) {
      SET_REAL_PITEM_VALUE( px, px->real > 0 ? DBL_MAX : -DBL_MAX );
    }
    return;
  // Nan is float nan
  case STACK_ITEM_TYPE_NAN:
    SET_REAL_PITEM_VALUE( px, (double)NAN );
    return;
  // None is zero
  case STACK_ITEM_TYPE_NONE:
    SET_REAL_PITEM_VALUE( px, 0.0 );
    return;
  // Interpret asreal(vertex) as its address
  case STACK_ITEM_TYPE_VERTEX:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->vertex );
    return;
  // Interpret asreal(string) as its value converted to float, or 0.0 if not a number
  case STACK_ITEM_TYPE_CSTRING:
  {
    double val = CALLABLE( px->CSTR__str )->AsReal( px->CSTR__str );
    SET_REAL_PITEM_VALUE( px, val );
    return;
  }
  // Interpret float(vector) as its magnitude
  case STACK_ITEM_TYPE_VECTOR:
  {
    const vgx_Vector_t *vec = px->vector;
    double mag = CALLABLE( vec )->Magnitude( vec );
    SET_REAL_PITEM_VALUE( px, mag );
    return;
  }
  // Interpret float(vertex.id) as its address
  case STACK_ITEM_TYPE_VERTEXID:
    SET_REAL_PITEM_VALUE( px, (double)(uintptr_t)px->vertexid );
    return;
  default:
    px->type = STACK_ITEM_TYPE_REAL;
  }
}



/*******************************************************************//**
 * bitvector( x ) -> bv
 ***********************************************************************
 */
static void __eval_unary_cast_bitvector( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  px->type = STACK_ITEM_TYPE_BITVECTOR;
}



/*******************************************************************//**
 * bytes( ... ) -> bytes string
 ***********************************************************************
 */
static void __eval_variadic_bytes( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  CString_constructor_args_t args = {
    .string      = NULL,
    .len         = (int32_t)nargs,
    .ucsz        = 0,
    .format      = NULL,
    .format_args = NULL,
    .alloc       = self->graph->ephemeral_string_allocator_context
  };
  CString_t *CSTR__bytes = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  if( CSTR__bytes == NULL ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_NONE( self ); 
  }

  BYTE *data = (BYTE*)CALLABLE( CSTR__bytes )->ModifiableQwords( CSTR__bytes );
  // Write string backwards
  BYTE *wp = data + args.len;
  *wp-- = '\0';
  vgx_EvalStackItem_t *px;
  while( nargs-- > 0 ) {
    px = POP_PITEM( self );
    // Assume integer in range 0-255
    *wp-- = px->integer & 0xFF;
  }
  // Force output string to pure bytes
  CStringAttributes( CSTR__bytes ) = CSTRING_ATTR_BYTES;
  px = NEXT_PITEM( self );

  vgx_EvalStackItem_t scoped = {
    .type = STACK_ITEM_TYPE_CSTRING,
    .CSTR__str = CSTR__bytes
  };

  if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
    STACK_RETURN_NONE( self ); 
  }

  *px = scoped;

}



/*******************************************************************//**
 * keyval( k, v ) -> keyval
 ***********************************************************************
 */
static void __eval_binary_cast_keyval( vgx_Evaluator_t *self ) {
  __eval_unary_cast_real( self );
  vgx_EvalStackItem_t val = POP_ITEM( self );
  float f32 = (float)val.real;
  __eval_unary_cast_int( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  px->keyval.key = (int)px->integer;
  px->keyval.value = f32;
  px->type = STACK_ITEM_TYPE_KEYVAL;
}







#endif
