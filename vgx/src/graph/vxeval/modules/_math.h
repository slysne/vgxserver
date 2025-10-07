/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _math.h
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

#ifndef _VGX_VXEVAL_MODULES_MATH_H
#define _VGX_VXEVAL_MODULES_MATH_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_neg( vgx_Evaluator_t *self );
static void __eval_unary_isnan( vgx_Evaluator_t *self );
static void __eval_unary_isinf( vgx_Evaluator_t *self );
static void __eval_unary_isint( vgx_Evaluator_t *self );
static void __eval_unary_isreal( vgx_Evaluator_t *self );
static void __eval_unary_isbitvector( vgx_Evaluator_t *self );
static void __eval_unary_iskeyval( vgx_Evaluator_t *self );
static void __eval_unary_isstr( vgx_Evaluator_t *self );
static void __eval_unary_isvector( vgx_Evaluator_t *self );
static void __eval_unary_isbytearray( vgx_Evaluator_t *self );
static void __eval_unary_isbytes( vgx_Evaluator_t *self );
static void __eval_unary_isutf8( vgx_Evaluator_t *self );
static void __eval_unary_isarray( vgx_Evaluator_t *self );
static void __eval_unary_ismap( vgx_Evaluator_t *self );
static void __eval_variadic_anynan( vgx_Evaluator_t *self );
static void __eval_variadic_allnan( vgx_Evaluator_t *self );

static void __eval_unary_inv( vgx_Evaluator_t *self );
static void __eval_unary_log2( vgx_Evaluator_t *self );
static void __eval_unary_log( vgx_Evaluator_t *self );
static void __eval_unary_log10( vgx_Evaluator_t *self );
static void __eval_unary_rad( vgx_Evaluator_t *self );
static void __eval_unary_deg( vgx_Evaluator_t *self );
static void __eval_unary_sin( vgx_Evaluator_t *self );
static void __eval_unary_cos( vgx_Evaluator_t *self );
static void __eval_unary_tan( vgx_Evaluator_t *self );
static void __eval_unary_asin( vgx_Evaluator_t *self );
static void __eval_unary_acos( vgx_Evaluator_t *self );
static void __eval_unary_atan( vgx_Evaluator_t *self );
static void __eval_unary_sinh( vgx_Evaluator_t *self );
static void __eval_unary_cosh( vgx_Evaluator_t *self );
static void __eval_unary_tanh( vgx_Evaluator_t *self );
static void __eval_unary_asinh( vgx_Evaluator_t *self );
static void __eval_unary_acosh( vgx_Evaluator_t *self );
static void __eval_unary_atanh( vgx_Evaluator_t *self );
static void __eval_unary_sinc( vgx_Evaluator_t *self );
static void __eval_unary_exp( vgx_Evaluator_t *self );
static void __eval_unary_abs( vgx_Evaluator_t *self );
static void __eval_unary_sqrt( vgx_Evaluator_t *self );
static void __eval_unary_ceil( vgx_Evaluator_t *self );
static void __eval_unary_floor( vgx_Evaluator_t *self );
static void __eval_unary_round( vgx_Evaluator_t *self );
static void __eval_unary_sign( vgx_Evaluator_t *self );
static void __eval_unary_fac( vgx_Evaluator_t *self );
static void __eval_unary_popcnt( vgx_Evaluator_t *self );
static void __eval_binary_comb( vgx_Evaluator_t *self );
static void __eval_binary_add( vgx_Evaluator_t *self );
static void __eval_binary_sub( vgx_Evaluator_t *self );
static void __eval_binary_mul( vgx_Evaluator_t *self );
static void __eval_binary_div( vgx_Evaluator_t *self );
static void __eval_binary_mod( vgx_Evaluator_t *self );
static void __eval_binary_pow( vgx_Evaluator_t *self );
static void __eval_binary_atan2( vgx_Evaluator_t *self );
static void __eval_binary_max( vgx_Evaluator_t *self );
static void __eval_binary_min( vgx_Evaluator_t *self );
static void __eval_binary_prox( vgx_Evaluator_t *self );
static void __eval_variadic_sum( vgx_Evaluator_t *self );
static void __eval_variadic_sumsqr( vgx_Evaluator_t *self );
static void __eval_variadic_stdev( vgx_Evaluator_t *self );
static void __eval_variadic_invsum( vgx_Evaluator_t *self );
static void __eval_variadic_product( vgx_Evaluator_t *self );
static void __eval_variadic_mean( vgx_Evaluator_t *self );
static void __eval_variadic_harmmean( vgx_Evaluator_t *self );
static void __eval_variadic_geomean( vgx_Evaluator_t *self );





/*******************************************************************//**
 * neg( x ) -> -x
 ***********************************************************************
 */
static void __eval_unary_neg( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
  case STACK_ITEM_TYPE_BITVECTOR:
    px->integer = -px->integer;
    return;
  case STACK_ITEM_TYPE_REAL:
  case STACK_ITEM_TYPE_NAN:
    px->real = -px->real;
    return;
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, -(int64_t)px->vertex );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    {
      vgx_Similarity_t *sim = self->graph->similarity;
      vgx_EvalStackItem_t scoped = {
        .type = STACK_ITEM_TYPE_VECTOR,
        .vector = CALLABLE( sim )->VectorScalarMultiply( sim, px->vector, -1.0, NULL )
      };
      if( scoped.ptr == NULL ) {
        SET_NAN( px );
        return;
      }

      if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
        SET_NAN( px );
        return;
      }

      *px = scoped;
    }
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * isnan( x )
 ***********************************************************************
 */
static void __eval_unary_isnan( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // x is numeric, isnan(x) -> false
  if( __is_stack_item_type_integer_compatible( px->type ) || (px->type == STACK_ITEM_TYPE_REAL && !isnan(px->real)) ) {
    SET_INTEGER_PITEM_VALUE( px, 0 );
    return;
  }
  // x is not numeric, isnan(x) -> true
  SET_INTEGER_PITEM_VALUE( px, 1 );
}



/*******************************************************************//**
 * isinf( x )
 ***********************************************************************
 */
static void __eval_unary_isinf( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // x is inf or -inf
  if( px->type == STACK_ITEM_TYPE_REAL && isinf( px->real ) ) {
    SET_INTEGER_PITEM_VALUE( px, 1 );
  }
  // x is not a real inf or -inf
  else {
    SET_INTEGER_PITEM_VALUE( px, 0 );
  }
}



/*******************************************************************//**
 * isint( x )
 ***********************************************************************
 */
static void __eval_unary_isint( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int i = px->type == STACK_ITEM_TYPE_INTEGER;
  SET_INTEGER_PITEM_VALUE( px, i );
}



/*******************************************************************//**
 * isreal( x )
 ***********************************************************************
 */
static void __eval_unary_isreal( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int r = px->type == STACK_ITEM_TYPE_REAL && !isnan( px->real );
  SET_INTEGER_PITEM_VALUE( px, r );
}



/*******************************************************************//**
 * isbitvector( x )
 ***********************************************************************
 */
static void __eval_unary_isbitvector( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int r = px->type == STACK_ITEM_TYPE_BITVECTOR;
  SET_INTEGER_PITEM_VALUE( px, r );
}



/*******************************************************************//**
 * iskeyval( x )
 ***********************************************************************
 */
static void __eval_unary_iskeyval( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int r = px->type == STACK_ITEM_TYPE_KEYVAL;
  SET_INTEGER_PITEM_VALUE( px, r );
}



/*******************************************************************//**
 * isstr( x )
 ***********************************************************************
 */
static void __eval_unary_isstr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int s = px->type == STACK_ITEM_TYPE_CSTRING;
  SET_INTEGER_PITEM_VALUE( px, s );
}



/*******************************************************************//**
 * isvector( x )
 ***********************************************************************
 */
static void __eval_unary_isvector( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int s = px->type == STACK_ITEM_TYPE_VECTOR;
  SET_INTEGER_PITEM_VALUE( px, s );
}



/*******************************************************************//**
 * isbytearray( x )
 ***********************************************************************
 */
static void __eval_unary_isbytearray( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_CSTRING && px->CSTR__str != NULL ) {
    if( CStringAttributes( px->CSTR__str ) & CSTRING_ATTR_BYTEARRAY ) {
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * isbytes( x )
 ***********************************************************************
 */
static void __eval_unary_isbytes( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_CSTRING && px->CSTR__str != NULL ) {
    if( CStringAttributes( px->CSTR__str ) & CSTRING_ATTR_BYTES ) {
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * isutf8( x )
 ***********************************************************************
 */
static void __eval_unary_isutf8( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_CSTRING && px->CSTR__str != NULL ) {
    int64_t errpos;
    if( (CStringAttributes( px->CSTR__str ) & CSTRING_ATTR_BYTES) && COMLIB_check_utf8( (const BYTE*)CStringValue( px->CSTR__str ), &errpos ) ) {
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * isarray( x )
 ***********************************************************************
 */
static void __eval_unary_isarray( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_CSTRING && px->CSTR__str != NULL ) {
    switch( CStringAttributes( px->CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK ) {
    case CSTRING_ATTR_ARRAY_INT:
    case CSTRING_ATTR_ARRAY_FLOAT:
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * ismap( x )
 ***********************************************************************
 */
static void __eval_unary_ismap( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_CSTRING && px->CSTR__str != NULL ) {
    switch( CStringAttributes( px->CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK ) {
    case CSTRING_ATTR_ARRAY_MAP:
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * anynan( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_anynan( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int any = 0; // speculate no nans, now disprove
  while( nargs-- > 0 ) {
    vgx_EvalStackItem_t *px = POP_PITEM( self );
    if( !__is_stack_item_type_integer_compatible( px->type ) && px->type != STACK_ITEM_TYPE_REAL ) {
      any = 1; // found something that is not integer, bitvector, or real
      break;
    }
  }
  DISCARD_ITEMS( self, nargs );
  STACK_RETURN_INTEGER( self, any );
}



/*******************************************************************//**
 * allnan( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_allnan( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int all = 1; // speculate all nans, now disprove
  while( nargs-- > 0 ) {
    vgx_EvalStackItem_t *px = POP_PITEM( self );
    if( __is_stack_item_type_integer_compatible( px->type ) || px->type == STACK_ITEM_TYPE_REAL ) {
      all = 0; // found integer, bitvector, or real
      break;
    }
  }
  DISCARD_ITEMS( self, nargs );
  STACK_RETURN_INTEGER( self, all );
}



/*******************************************************************//**
 * inv( x ) -> 1/x
 ***********************************************************************
 */
static void __eval_unary_inv( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    SET_REAL_PITEM_VALUE( px, px->bits ? 1.0 / px->integer : FLT_MAX );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = px->bits ? 1.0 / px->real : FLT_MAX;
    return;
  case STACK_ITEM_TYPE_NAN:
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * log2( x )
 ***********************************************************************
 */
static void __eval_unary_log2( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
  case STACK_ITEM_TYPE_VERTEX:
    SET_REAL_PITEM_VALUE( px, px->integer > 0 ? log2( (double)px->integer ) : -1074 );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = px->real > 0 ? log2( px->real ) : -1074;
    return;
  case STACK_ITEM_TYPE_NAN:
    return;
  default:
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 * log( x )
 ***********************************************************************
 */
static void __eval_unary_log( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
  case STACK_ITEM_TYPE_VERTEX:
    SET_REAL_PITEM_VALUE( px, px->integer > 0 ? log( (double)px->integer ) : -745.0 );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = px->real > 0 ? log( px->real ) : -745.0;
    return;
  case STACK_ITEM_TYPE_NAN:
    return;
  default:
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 * log10( x )
 ***********************************************************************
 */
static void __eval_unary_log10( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
  case STACK_ITEM_TYPE_VERTEX:
    SET_REAL_PITEM_VALUE( px, px->integer > 0 ? log10( (double)px->integer ) : -324 );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = px->real > 0 ? log10( px->real ) : -324;
    return;
  case STACK_ITEM_TYPE_NAN:
    return;
  default:
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 * rad( x )
 ***********************************************************************
 */
static void __eval_unary_rad( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  double deg;
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    deg = RADIANS( px->integer );
    break;
  case STACK_ITEM_TYPE_REAL:
    deg = RADIANS( px->real );
    break;
  default:
    deg = 0.0;
  }
  SET_REAL_PITEM_VALUE( px, deg );
}



/*******************************************************************//**
 * deg( x )
 ***********************************************************************
 */
static void __eval_unary_deg( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  double rad;
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    rad = DEGREES( px->integer );
    break;
  case STACK_ITEM_TYPE_REAL:
    rad = DEGREES( px->real );
    break;
  default:
    rad = 0.0;
  }
  SET_REAL_PITEM_VALUE( px, rad );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __EVAL_UNARY_FUNC( FuncName )                             \
static void __eval_unary_##FuncName( vgx_Evaluator_t *self ) {    \
  vgx_EvalStackItem_t *px = GET_PITEM( self );                        \
  switch( px->type ) {                                            \
  case STACK_ITEM_TYPE_INTEGER:                                   \
    SET_REAL_PITEM_VALUE( px, FuncName( (double)px->integer ) );  \
    return;                                                       \
  case STACK_ITEM_TYPE_REAL:                                      \
    px->real = FuncName( px->real );                              \
    return;                                                       \
  default:                                                        \
    return;                                                       \
  }                                                               \
}                                                                 \


__EVAL_UNARY_FUNC( sin )
__EVAL_UNARY_FUNC( sinh )
__EVAL_UNARY_FUNC( asin )
__EVAL_UNARY_FUNC( asinh )
__EVAL_UNARY_FUNC( cos )
__EVAL_UNARY_FUNC( cosh )
__EVAL_UNARY_FUNC( acos )
__EVAL_UNARY_FUNC( acosh )
__EVAL_UNARY_FUNC( tan )
__EVAL_UNARY_FUNC( tanh )
__EVAL_UNARY_FUNC( atan )
__EVAL_UNARY_FUNC( atanh )
__EVAL_UNARY_FUNC( exp )



/*******************************************************************//**
 * sinc( x )
 ***********************************************************************
 */
static void __eval_unary_sinc( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  double x = M_PI;
  double y;
  if( px->type == STACK_ITEM_TYPE_REAL ) {
    x *= px->real;
  }
  else {
    x *= (double)px->integer;
  }
  if( x != 0.0 ) {
    y = sin( x ) / x;
  }
  else {
    y = 1.0;
  }
  SET_REAL_PITEM_VALUE( px, y );
}



/*******************************************************************//**
 * abs( x )
 ***********************************************************************
 */
static void __eval_unary_abs( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    SET_INTEGER_PITEM_VALUE( px, llabs( px->integer ) );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = fabs( px->real );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    SET_REAL_PITEM_VALUE( px, CALLABLE( px->vector )->Magnitude( px->vector ) );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * sqrt( x )
 ***********************************************************************
 */
static void __eval_unary_sqrt( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    SET_REAL_PITEM_VALUE( px, px->integer > 0 ? sqrt( (double)px->integer ) : 0.0 );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = px->real > 0 ? sqrt( px->real ) : 0.0;
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * ceil( x )
 ***********************************************************************
 */
static void __eval_unary_ceil( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_REAL ) {
    px->real = ceil( px->real );
  }
}



/*******************************************************************//**
 * floor( x )
 ***********************************************************************
 */
static void __eval_unary_floor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_REAL ) {
    px->real = floor( px->real );
  }
}



/*******************************************************************//**
 * round( x )
 ***********************************************************************
 */
static void __eval_unary_round( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_REAL ) {
    px->real = round( px->real );
  }
}



/*******************************************************************//**
 * sign( x )
 ***********************************************************************
 */
static void __eval_unary_sign( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    px->integer = (px->integer > 0LL) - (px->integer < 0LL);
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = (px->real > 0.0) - (px->real < 0.0);
    return;
  case STACK_ITEM_TYPE_NAN:
    return;
  default:
    SET_INTEGER_PITEM_VALUE( px, 1 );
  }
}



/*******************************************************************//**
 * fac( x ) -> x!
 ***********************************************************************
 */
static void __eval_unary_fac( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    SET_REAL_PITEM_VALUE( px, fac( (int)px->integer ) );
    return;
  case STACK_ITEM_TYPE_REAL:
    px->real = fac( (int)px->real );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * popcnt( x ) -> n
 ***********************************************************************
 */
static void __eval_unary_popcnt( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, __popcnt64( px->bits ) );
}



/*******************************************************************//**
 * comb( x, y )
 ***********************************************************************
 */
static void __eval_binary_comb( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );      // m
  vgx_EvalStackItem_t *px = GET_PITEM( self );   // n
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    SET_REAL_PITEM_VALUE( px, comb( (int)px->integer, (int)y.integer ) );
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, comb( (int)px->integer, (int)y.real ) );
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    SET_REAL_PITEM_VALUE( px, comb( (int)px->real, (int)y.integer ) );
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    SET_REAL_PITEM_VALUE( px, comb( (int)px->real, (int)y.real ) );
    return;
  default:
    SET_INTEGER_PITEM_VALUE( px, 1 );
    return;
  }
}



/*******************************************************************//**
 * x + y
 ***********************************************************************
 */
static void __eval_binary_add( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    ADD_INTEGER_PITEM_VALUE( px, y.integer );
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, (double)px->integer + y.real );
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    ADD_REAL_PITEM_VALUE( px, y.integer );
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    ADD_REAL_PITEM_VALUE( px, y.real );
    return;
  case STACK_PAIR_TYPE_XSTR_YSTR:
    goto CONCAT;
  case STACK_PAIR_TYPE_XVEC_YVEC:
    goto VECTORADD;
  default:
    // assign y to x if x is null
    if( px->type == STACK_ITEM_TYPE_NONE ) {
      *px = y;
    }
    // concat x+y if x is string and y is castable to string
    else if( px->type == STACK_ITEM_TYPE_CSTRING && __cast_str( self, &y ) != NULL ) {
      goto CONCAT;
    }
    // otherwise treat x + y as raw bits and set result to bitvector if y is not null
    else if( y.type != STACK_ITEM_TYPE_NONE ) {
      px->bits += y.bits;
      px->type = STACK_ITEM_TYPE_BITVECTOR;
    }
    return;
  }

  vgx_EvalStackItem_t scoped;
  vgx_Similarity_t *sim;
CONCAT:
  scoped.type = STACK_ITEM_TYPE_CSTRING;
  scoped.CSTR__str = CStringConcatAlloc( px->CSTR__str, y.CSTR__str, NULL, self->graph->ephemeral_string_allocator_context );
  goto ADD_LOCAL_SCOPE;

VECTORADD:
  sim = self->graph->similarity;
  scoped.type = STACK_ITEM_TYPE_VECTOR;
  scoped.vector = CALLABLE( sim )->VectorArithmetic( sim, px->vector, y.vector, false, NULL );
  goto ADD_LOCAL_SCOPE;

ADD_LOCAL_SCOPE:
  if( scoped.ptr == NULL ) {
    return; // ignore
  }

  if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
    return; // ignore
  }

  *px = scoped;

}



/*******************************************************************//**
 * x - y
 ***********************************************************************
 */
static void __eval_binary_sub( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    SUB_INTEGER_PITEM_VALUE( px, y.integer );
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, (double)px->integer - y.real );
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    SUB_REAL_PITEM_VALUE( px, y.integer );
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    SUB_REAL_PITEM_VALUE( px, y.real );
    return;
  case STACK_PAIR_TYPE_XVEC_YVEC:
    goto VECTORSUB;
  default:
    // assign minus y to x if x is null
    if( px->type == STACK_ITEM_TYPE_NONE ) {
      *px = y;
      __eval_unary_neg( self );
    }
    // otherwise treat x - y as raw bits and set result to bitvector if y is not null
    else if( y.type != STACK_ITEM_TYPE_NONE ) {
      px->bits -= y.bits;
      px->type = STACK_ITEM_TYPE_BITVECTOR;
    }
    return;
  }

  vgx_Similarity_t *sim;
  vgx_EvalStackItem_t scoped;

VECTORSUB:
  sim = self->graph->similarity;
  scoped.type = STACK_ITEM_TYPE_VECTOR;
  scoped.vector = CALLABLE( sim )->VectorArithmetic( sim, px->vector, y.vector, true, NULL );

  if( scoped.ptr == NULL ) {
    return; // ignore
  }

  if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
    return; // ignore
  }

  *px = scoped;

}



/*******************************************************************//**
 * x * y
 ***********************************************************************
 */
static void __eval_binary_mul( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  // INT * INT
  case STACK_PAIR_TYPE_XINT_YINT:
    px->integer *= y.integer;
    return;
  // REA * REA
  case STACK_PAIR_TYPE_XREA_YREA:
    MUL_REAL_PITEM_VALUE( px, y.real );
    return;
  // REA * INT
  case STACK_PAIR_TYPE_XREA_YINT:
    MUL_REAL_PITEM_VALUE( px, y.integer );
    return;
  // INT * REA
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, (double)px->integer * y.real );
    return;
  // VEC * VEC = dotproduct
  case STACK_PAIR_TYPE_XVEC_YVEC:
    goto DOTPROD;
  // a * VEC = scalar product
  case STACK_PAIR_TYPE_XREA_YVEC:
  case STACK_PAIR_TYPE_XINT_YVEC:
    goto SCALARMULTIPLY;
  // VEC * a = scalar product
  case STACK_PAIR_TYPE_XVEC_YREA:
  case STACK_PAIR_TYPE_XVEC_YINT:
    goto SCALARMULTIPLY;
  default:
    break;
  }

  // -> BTV
  // INT * VTX
  // INT * BTV
  // VTX * INT
  // VTX * VTX
  // VTX * BTV
  // BTV * INT
  // BTV * VTX
  // BTV * BTV
  if( __is_stack_item_type_bit_comparable( px->type ) && __is_stack_item_type_bit_comparable( y.type ) ) {
    px->bits *= y.bits;
    px->type = STACK_ITEM_TYPE_BITVECTOR;
    return;
  }

  if( y.type == STACK_ITEM_TYPE_NONE ) {
    if( __is_stack_item_type_integer_compatible( px->type ) || px->type == STACK_ITEM_TYPE_REAL ) {
      px->bits = 0;
      return;
    }
    SET_NONE( px );
    return;
  }

  if( px->type == STACK_ITEM_TYPE_NONE ) {
    if( __is_stack_item_type_integer_compatible( y.type ) || y.type == STACK_ITEM_TYPE_REAL ) {
      px->type = y.type;
      px->bits = 0;
    }
    return;
  }

  // Otherwise result is nan
  SET_NAN( px );
  return;

  vgx_Similarity_t *sim;

DOTPROD:
  {
    sim = self->graph->similarity;
    double dp = CALLABLE( sim )->VectorDotProduct( sim, px->vector, y.vector, NULL );
    SET_REAL_PITEM_VALUE( px, dp );
    return;
  }

  vgx_EvalStackItem_t scoped;
SCALARMULTIPLY:
  {
    sim = self->graph->similarity;
    const vgx_Vector_t *V;
    double a;
    if( px->type == STACK_ITEM_TYPE_VECTOR ) {
      V = px->vector;
      a = y.type == STACK_ITEM_TYPE_REAL ? y.real : (double)y.integer;
    }
    else if( y.type == STACK_ITEM_TYPE_VECTOR ) {
      V = y.vector;
      a = px->type == STACK_ITEM_TYPE_REAL ? px->real : (double)px->integer;
    }
    else {
      return;
    }

    scoped.type = STACK_ITEM_TYPE_VECTOR;
    scoped.vector = CALLABLE( sim )->VectorScalarMultiply( sim, V, a, NULL );

    if( scoped.ptr == NULL ) {
      return; // ignore
    }

    if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
      return; // ignore
    }

    *px = scoped;

  }
}



/*******************************************************************//**
 * x / y
 ***********************************************************************
 */
static void __eval_binary_div( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  double divisor;
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    divisor = y.bits == 0 ? FLT_MIN : y.integer;
    SET_REAL_PITEM_VALUE( px, px->integer / divisor );
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    divisor = y.bits == 0 ? FLT_MIN : y.real;
    SET_REAL_PITEM_VALUE( px, px->integer / divisor );
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    divisor = y.bits == 0 ? FLT_MIN : y.integer;
    SET_REAL_PITEM_VALUE( px, px->real / divisor );
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    divisor = y.bits == 0 ? FLT_MIN : y.real;
    SET_REAL_PITEM_VALUE( px, px->real / divisor );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * x % y
 ***********************************************************************
 */
static void __eval_binary_mod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    px->integer = y.bits ? px->integer % y.integer : 0; // NOTE: define zero remainder for mod zero
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, y.bits ? fmod( (double)px->integer, y.real) : FLT_MIN ); // NOTE: define very small remainder for mod zero
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    px->real = y.bits ? fmod( px->real, (double)y.integer) : FLT_MIN; // NOTE: define very small remainder for mod zero
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    px->real = y.bits ? fmod( px->real, y.real) : FLT_MIN; // NOTE: define very small remainder for mod zero
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * x ** y
 ***********************************************************************
 */
static void __eval_binary_pow( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  // x ** 0 := 1
  if( y.bits == 0 ) {
    if( px->type == STACK_ITEM_TYPE_REAL ) {
      SET_REAL_PITEM_VALUE( px, 1.0 );
    }
    else {
      SET_INTEGER_PITEM_VALUE( px, 1 );
    }
    return;
  }

  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    SET_INTEGER_PITEM_VALUE( px, (int64_t)pow( (double)px->integer, (double)y.integer ) );
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, pow( px->integer > 0 ? (double)px->integer : 0.0, y.real ) );
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    px->real = pow( px->real, (double)y.integer );
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    px->real = pow( px->real > 0 ? px->real : 0.0, y.real );
    break;
  default:
    break;
  }

  if( isinf( px->real ) ) {
    px->real = DBL_MAX;
  }
}



/*******************************************************************//**
 * atan2( x, y )
 ***********************************************************************
 */
static void __eval_binary_atan2( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    SET_REAL_PITEM_VALUE( px, atan2( (double)px->integer, (double)y.integer ) );
    return;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( px, atan2( (double)px->integer, y.real ) );
    return;
  case STACK_PAIR_TYPE_XREA_YINT:
    px->real = atan2( px->real, (double)y.integer );
    return;
  case STACK_PAIR_TYPE_XREA_YREA:
    px->real = atan2( px->real, y.real );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 * max( x, y )
 ***********************************************************************
 */
static void __eval_binary_max( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    if( y.integer > px->integer ) {
      px->integer = y.integer;
    }
    return;
  }
  // All other tests
  else {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XINT_YREA:
      if( y.real > px->integer ) {
        SET_REAL_PITEM_VALUE( px, y.real );
      }
      return;
    case STACK_PAIR_TYPE_XREA_YINT:
      if( y.integer > px->real ) {
        SET_INTEGER_PITEM_VALUE( px, y.integer );
      }
      return;
    case STACK_PAIR_TYPE_XREA_YREA:
      if( y.real > px->real ) {
        px->real = y.real;
      }
      return;
    // both strings
    case STACK_PAIR_TYPE_XSTR_YSTR:
      if( CALLABLE( y.CSTR__str )->Compare( y.CSTR__str, px->CSTR__str ) > 0 ) {
        px->CSTR__str = y.CSTR__str;
      }
      return;
    // qword test
    default:
      // y > x if x is null or qword is greater
      if( px->type == STACK_ITEM_TYPE_NONE || y.bits > px->bits ) {
        *px = y;
      }
    }
  }
}



/*******************************************************************//**
 * min( x, y )
 ***********************************************************************
 */
static void __eval_binary_min( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    if( y.integer < px->integer ) {
      px->integer = y.integer;
    }
    return;
  }
  // All others
  else {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XINT_YREA:
      if( y.real < px->integer ) {
        SET_REAL_PITEM_VALUE( px, y.real );
      }
      return;
    case STACK_PAIR_TYPE_XREA_YINT:
      if( y.integer < px->real ) {
        SET_INTEGER_PITEM_VALUE( px, y.integer );
      }
      return;
    case STACK_PAIR_TYPE_XREA_YREA:
      if( y.real < px->real ) {
        px->real = y.real;
      }
      return;
    // two strings
    case STACK_PAIR_TYPE_XSTR_YSTR:
      if( CALLABLE( y.CSTR__str )->Compare( y.CSTR__str, px->CSTR__str ) < 0 ) {
        px->CSTR__str = y.CSTR__str;
      }
      return;
    // qword tests
    default:
      // y < x if x is null or qword is less
      if( px->type == STACK_ITEM_TYPE_NONE || y.bits < px->bits ) {
        *px = y;
      }
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_EvalStackItem_t * __two_doubles( vgx_Evaluator_t *self, double *target, double *actual ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );

  // Signed ints
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    *target = (double)y.integer;
    *actual = (double)px->integer;
    return px;
  }
  // All others
  else {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XINT_YREA:
      *target = y.real;
      *actual = (double)px->integer;
      return px;
    case STACK_PAIR_TYPE_XREA_YINT:
      *target = (double)y.integer;
      *actual = px->real;
      return px;
    case STACK_PAIR_TYPE_XREA_YREA:
      *target = y.real;
      *actual = px->real;
      return px;
    // qwords
    default:
      *target = (double)y.bits;
      *actual = (double)px->bits;
      return px;
    }
  }
}



/*******************************************************************//**
 * prox( x, y ) 
 ***********************************************************************
 */
static void __eval_binary_prox( vgx_Evaluator_t *self ) {
  double target;
  double actual;
  vgx_EvalStackItem_t *px = __two_doubles( self, &target, &actual );
  double diff = fabs( target - actual );
  double score = 256.0 / (256.0 + diff);
  SET_REAL_PITEM_VALUE( px, score );
}



/*******************************************************************//**
 * approx( x, y, error ) 
 ***********************************************************************
 */
static void __eval_binary_approx( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t err = POP_ITEM( self );
  double maxerr = err.type == STACK_ITEM_TYPE_REAL ? err.real : (double)err.integer;
  double target;
  double actual;
  vgx_EvalStackItem_t *px = __two_doubles( self, &target, &actual );
  double e;
  if( target == 0.0 ) {
    e = fabs( actual );
  }
  else {
    e = fabs( (actual - target) / target );
  }
  int match = e < fabs( maxerr );
  SET_INTEGER_PITEM_VALUE( px, match );
}



#define __termf_val( ValPtr )  (*(ValPtr))
#define __termf_sq( ValPtr )   (*(ValPtr) * *(ValPtr))
#define __termf_inv( ValPtr )  ( *(ValPtr) ? 1.0 / (double)(*(ValPtr)) : FLT_MIN )


/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __xxxarray_sum( SumType, TermType, Name, TermFunc ) \
__inline static SumType Name( const CString_t *A, int64_t *rsz ) {  \
  SumType s = 0;          \
  TermType *p = (TermType*)CStringValue( A ); \
  int64_t sz = CStringLength( A ) / sizeof( TermType ); \
  TermType *e = p + sz;   \
  while( p < e ) {        \
    s += TermFunc( p );   \
    ++p;                  \
  }                       \
  *rsz += sz;             \
  return s;               \
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __xxxarray_prod( ProdType, TermType, Name, TermFunc ) \
__inline static ProdType Name( const CString_t *A, int64_t *rsz ) {  \
  ProdType prod = 1;        \
  TermType *p = (TermType*)CStringValue( A ); \
  int64_t sz = CStringLength( A ) / sizeof( TermType ); \
  TermType *e = p + sz;     \
  while( p < e ) {          \
    prod *= TermFunc( p );  \
    ++p;                    \
  }                         \
  *rsz += sz;               \
  return prod;              \
}




__xxxarray_sum( int64_t, BYTE, __bytearray_sum, __termf_val );
__xxxarray_sum( int64_t, int64_t, __intarray_sum, __termf_val );
__xxxarray_sum( double, double, __dblarray_sum, __termf_val );

__xxxarray_sum( int64_t, BYTE, __bytearray_sumsq, __termf_sq );
__xxxarray_sum( int64_t, int64_t, __intarray_sumsq, __termf_sq );
__xxxarray_sum( double, double, __dblarray_sumsq, __termf_sq );

__xxxarray_sum( double, BYTE, __bytearray_invsum, __termf_inv );
__xxxarray_sum( double, int64_t, __intarray_invsum, __termf_inv );
__xxxarray_sum( double, double, __dblarray_invsum, __termf_inv );

__xxxarray_prod( int64_t, BYTE, __bytearray_prod, __termf_val );
__xxxarray_prod( int64_t, int64_t, __intarray_prod, __termf_val );
__xxxarray_prod( double, double, __dblarray_prod, __termf_val );



/*******************************************************************//**
 * sum( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_sum( vgx_Evaluator_t *self ) {
  // Store nargs in tmp register for potential future use
  self->tmp.i64 = self->op->arg.integer;
  int64_t nargs = self->tmp.i64;
  double rsum = 0.0;
  int64_t isum = 0;
  // Add arguments
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    switch( x.type ) {
    case STACK_ITEM_TYPE_INTEGER:
      isum += x.integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      rsum += x.real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( x.CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        isum += __intarray_sum( x.CSTR__str, &self->tmp.i64 );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        rsum += __dblarray_sum( x.CSTR__str, &self->tmp.i64 );
        break;
      default:
        isum += __bytearray_sum( x.CSTR__str, &self->tmp.i64 );
        break;
      }
      self->tmp.i64--; // account for the +1 of the string instance itself
      continue;
    default:
      continue;
    }
  }
  // Combine real and integer sums into one final sum
  if( rsum != 0.0 ) {
    rsum += isum;
    STACK_RETURN_REAL( self, rsum );
  }
  else {
    STACK_RETURN_INTEGER( self, isum );
  }
}



/*******************************************************************//**
 * sumsqr( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_sumsqr( vgx_Evaluator_t *self ) {
  // Store nargs in tmp register for potential future use
  self->tmp.i64 = self->op->arg.integer;
  int64_t nargs = self->tmp.i64;
  double rsum = 0.0;
  int64_t isum = 0;
  // Add arguments
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    switch( x.type ) {
    case STACK_ITEM_TYPE_INTEGER:
      isum += x.integer * x.integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      rsum += x.real * x.real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( x.CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        isum += __intarray_sumsq( x.CSTR__str, &self->tmp.i64 );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        rsum += __dblarray_sumsq( x.CSTR__str, &self->tmp.i64 );
        break;
      default:
        isum += __bytearray_sumsq( x.CSTR__str, &self->tmp.i64 );
        break;
      }
      self->tmp.i64--; // account for the +1 of the string instance itself
      continue;
    default:
      continue;
    }
  }
  // Combine real and integer sums into one final sum
  if( rsum != 0.0 ) {
    rsum += isum;
    STACK_RETURN_REAL( self, rsum );
  }
  else {
    STACK_RETURN_INTEGER( self, isum );
  }
}



/*******************************************************************//**
 * stdev( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_stdev( vgx_Evaluator_t *self ) {
  // Store nargs in tmp register for potential future use
  self->tmp.i64 = self->op->arg.integer;
  int64_t nargs = self->tmp.i64;
  // Compute sum of squares
  double ssq = 0.0;
  for( int64_t i=0; i<nargs; ++i ) {
    vgx_EvalStackItem_t *px = IDX_PITEM( self, i );
    switch( px->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      ssq += (double)px->integer * (double)px->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      ssq += px->real * px->real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( px->CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        ssq += (double)__intarray_sumsq( px->CSTR__str, &self->tmp.i64 );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        ssq += __dblarray_sumsq( px->CSTR__str, &self->tmp.i64 );
        break;
      default:
        ssq += (double)__bytearray_sumsq( px->CSTR__str, &self->tmp.i64 );
        break;
      }
      self->tmp.i64--; // account for the +1 of the string instance itself
      continue;
    default:
      continue;
    }
  }
  int64_t n = self->tmp.i64;
  if( n < 2 ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_REAL( self, 0.0 );
  }

  // Compute sum
  int64_t _ign;
  double sum = 0.0;
  for( int64_t i=0; i<nargs; ++i ) {
    vgx_EvalStackItem_t *px = IDX_PITEM( self, i );
    switch( px->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      sum += (double)px->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      sum += px->real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( px->CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        sum += (double)__intarray_sum( px->CSTR__str, &_ign );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        sum += __dblarray_sum( px->CSTR__str, &_ign );
        break;
      default:
        sum += (double)__bytearray_sum( px->CSTR__str, &_ign );
        break;
      }
      continue;
    default:
      continue;
    }
  }

  // Compute stdev
  double stdev = sqrt( (ssq - (sum*sum / n)) / (n - 1) );
  DISCARD_ITEMS( self, nargs );
  STACK_RETURN_REAL( self, stdev );
}



/*******************************************************************//**
 * sum( [1/a [,1/b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_invsum( vgx_Evaluator_t *self ) {
  // Store nargs in tmp register for potential future use
  self->tmp.i64 = self->op->arg.integer;
  int64_t nargs = self->tmp.i64;
  double rsum = 0.0;
  // Add arguments
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    if( x.bits == 0 || x.type == STACK_ITEM_TYPE_NAN ) {
      x.real = FLT_MIN;
      x.type = STACK_ITEM_TYPE_REAL;
    }
    switch( x.type ) {
    case STACK_ITEM_TYPE_INTEGER:
      rsum += 1.0 / x.integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      rsum += 1.0 / x.real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( x.CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        rsum += __intarray_invsum( x.CSTR__str, &self->tmp.i64 );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        rsum += __dblarray_invsum( x.CSTR__str, &self->tmp.i64 );
        break;
      default:
        rsum += __bytearray_invsum( x.CSTR__str, &self->tmp.i64 );
        break;
      }
      self->tmp.i64--; // account for the +1 of the string instance itself
      continue;
    default:
      continue;
    }
  }
  STACK_RETURN_REAL( self, rsum );
}



/*******************************************************************//**
 * prod( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_product( vgx_Evaluator_t *self ) {
  // Store nargs in tmp register for potential future use
  self->tmp.i64 = self->op->arg.integer;
  int64_t nargs = self->tmp.i64;
  if( nargs == 0 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  double rprod = 1.0;
  int64_t iprod = 1;
  // Multiply arguments
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    switch( x.type ) {
    case STACK_ITEM_TYPE_INTEGER:
      iprod *= x.integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      rprod *= x.real;
      continue;
    case STACK_ITEM_TYPE_CSTRING:
      switch( CStringAttributes( x.CSTR__str ) ) {
      case CSTRING_ATTR_ARRAY_INT:
        iprod *= __intarray_prod( x.CSTR__str, &self->tmp.i64 );
        break;
      case CSTRING_ATTR_ARRAY_FLOAT:
        rprod *= __dblarray_prod( x.CSTR__str, &self->tmp.i64 );
        break;
      default:
        iprod *= __bytearray_prod( x.CSTR__str, &self->tmp.i64 );
        break;
      }
      self->tmp.i64--; // account for the +1 of the string instance itself
      continue;
    default:
      continue;
    }
  }
  // Combine real and integer products into one final product
  if( rprod != 1.0 ) {
    rprod *= iprod;
    STACK_RETURN_REAL( self, rprod );
  }
  else {
    STACK_RETURN_INTEGER( self, iprod );
  }
}



/*******************************************************************//**
 * mean( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_mean( vgx_Evaluator_t *self ) {
  double x = 0.0;
  __eval_variadic_sum( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );
  int64_t n = self->tmp.i64;
  if( n > 0 ) {
    switch( px->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      x = px->integer / (double)n;
      break;
    case STACK_ITEM_TYPE_REAL:
      x = px->real / (double)n;
      break;
    default:
      break;
    }
  }
  STACK_RETURN_REAL( self, x );
}



/*******************************************************************//**
 * harmmean( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_harmmean( vgx_Evaluator_t *self ) {
  double x = 0.0;
  __eval_variadic_invsum( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );
  int64_t n = self->tmp.i64;
  if( n > 0 ) {
    if( px->bits ) {
      x = n / px->real;
    }
    else {
      x = n / FLT_MIN;
    }
  }
  STACK_RETURN_REAL( self, x );
}



/*******************************************************************//**
 * geomean( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_geomean( vgx_Evaluator_t *self ) {
  double x = 0.0;
  __eval_variadic_product( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );
  int64_t n = self->tmp.i64;
  if( n > 0 ) {
    if( px->type == STACK_ITEM_TYPE_INTEGER ) {
      x = pow( (double)px->integer, 1.0/n );
    }
    else {
      x = pow( px->real, 1.0/n );
    }
  }
  STACK_RETURN_REAL( self, x );
}






#endif
