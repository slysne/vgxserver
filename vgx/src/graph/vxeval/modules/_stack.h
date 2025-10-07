/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _stack.h
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

#ifndef _VGX_VXEVAL_MODULES_STACK_H
#define _VGX_VXEVAL_MODULES_STACK_H


#include "_conditional.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __debug_eval( vgx_Evaluator_t *self );
static void __stack_noop( vgx_Evaluator_t *self );

static void __eval_return( vgx_Evaluator_t *self );
static void __eval_returnif( vgx_Evaluator_t *self );
static void __eval_require( vgx_Evaluator_t *self );

static void __eval_halt( vgx_Evaluator_t *self );
static void __eval_haltif( vgx_Evaluator_t *self );
static void __eval_halted( vgx_Evaluator_t *self );
static void __eval_continue( vgx_Evaluator_t *self );
static void __eval_continueif( vgx_Evaluator_t *self );



static void __eval_variadic_first( vgx_Evaluator_t *self );
static void __eval_variadic_firstval( vgx_Evaluator_t *self );
static void __eval_variadic_lastval( vgx_Evaluator_t *self );
static void __eval_variadic_set( vgx_Evaluator_t *self );
static void __eval_binary_range( vgx_Evaluator_t *self );
static void __eval_variadic_do( vgx_Evaluator_t *self );
static void __eval_variadic_void( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __debug_eval( vgx_Evaluator_t *self ) {
  int64_t n = self->sp - self->rpn_program.stack.data;
  vgx_EvalStackItem_t *item = GET_PITEM( self );
  char buf[33];
  const char *str;
  const vgx_VertexIdentifier_t *pid;
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    INFO( 0x101, "STACK[%lld]: (int) %lld", n, item->integer );
    return;
  case STACK_ITEM_TYPE_REAL:
    INFO( 0x102, "STACK[%lld]: (flt) %#g", n, item->real );
    return;
  case STACK_ITEM_TYPE_NAN:
    INFO( 0x103, "STACK[%lld]: (nan)", n );
    return;
  case STACK_ITEM_TYPE_NONE:
    INFO( 0x103, "STACK[%lld]: (null)", n );
    return;
  case STACK_ITEM_TYPE_VERTEX:
    INFO( 0x104, "STACK[%lld]: (oid) %s", n, idtostr( buf, __vertex_internalid( item->vertex ) ) );
    return;
  case STACK_ITEM_TYPE_CSTRING:
    switch( CStringAttributes( item->CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK ) {
    case CSTRING_ATTR_ARRAY_INT:
      INFO( 0x105, "STACK[%lld]: (intarray) %llu (@%llp)", n, item->CSTR__str ? VGX_CSTRING_ARRAY_LENGTH( item->CSTR__str ) : 0, item->CSTR__str );
      return;
    case CSTRING_ATTR_ARRAY_FLOAT:
      INFO( 0x105, "STACK[%lld]: (floatarray) %llu (@%llp)", n, item->CSTR__str ? VGX_CSTRING_ARRAY_LENGTH( item->CSTR__str ) : 0, item->CSTR__str );
      return;
    case CSTRING_ATTR_ARRAY_MAP:
      INFO( 0x105, "STACK[%lld]: (map) %d (@%llp)", n, item->CSTR__str ? vgx_cstring_array_map_len( (QWORD*)CStringValue( item->CSTR__str ) ) : 0, item->CSTR__str );
      return;
    default:
      INFO( 0x105, "STACK[%lld]: (str) %s (@%llp)", n, item->CSTR__str ? CStringValue( item->CSTR__str ) : "?", item->CSTR__str );
      return;
    }
  case STACK_ITEM_TYPE_VECTOR:
    INFO( 0x106, "STACK[%lld]: (vec) @ %llp", n, item->vector );
    return;
  case STACK_ITEM_TYPE_BITVECTOR:
    INFO( 0x101, "STACK[%lld]: (btv) 0x%016llx", n, item->bits );
    return;
  case STACK_ITEM_TYPE_KEYVAL:
    INFO( 0x101, "STACK[%lld]: (keyval) (%d,%g)", n, vgx_cstring_array_map_key( &item->bits ), vgx_cstring_array_map_val( &item->bits ) );
    return;
  case STACK_ITEM_TYPE_VERTEXID:
    pid = item->vertexid;
    str = pid ? (pid->CSTR__idstr ? CStringValue( pid->CSTR__idstr ) : pid->idprefix.data) : "?";
    INFO( 0x107, "STACK[%lld]: (vtx) %s", n, str );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __stack_noop( vgx_Evaluator_t *self ) {
  return;
}



/*******************************************************************//**
 * return( value )
 ***********************************************************************
 */
static void __eval_return( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalProgram_t *p = &self->rpn_program;
  vgx_ExpressEvalOperation_t *end = p->operations + p->length + p->n_passthru;
  self->op = end-1;
}



/*******************************************************************//**
 * returnif( cond, value )
 ***********************************************************************
 */
static void __eval_returnif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *value = POP_PITEM( self );
  vgx_EvalStackItem_t *cx = GET_PITEM( self );
  if( __condition( cx ) ) {
    *cx = *value;
    __eval_return( self );
  }
}



/*******************************************************************//**
 * require( cond )
 ***********************************************************************
 */
static void __eval_require( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cx = GET_PITEM( self );
  if( !__condition( cx ) ) {
    SET_INTEGER_PITEM_VALUE( cx, 0 );
    __eval_return( self );
  }
}



/*******************************************************************//**
 * halt()
 ***********************************************************************
 */
static void __eval_halt( vgx_Evaluator_t *self ) {
  if( self->context.timing_budget ) {
    _vgx_set_execution_explicit_halt( self->context.timing_budget );
    STACK_RETURN_INTEGER( self, 1 );
  }
  STACK_RETURN_INTEGER( self, 0 );
}



/*******************************************************************//**
 * haltif( cond )
 ***********************************************************************
 */
static void __eval_haltif( vgx_Evaluator_t *self ) {
  if( __condition( GET_PITEM(self) ) && self->context.timing_budget ) {
    _vgx_set_execution_explicit_halt( self->context.timing_budget );
  }
}



/*******************************************************************//**
 * halted()
 ***********************************************************************
 */
static void __eval_halted( vgx_Evaluator_t *self ) {
  int halted = self->context.timing_budget ? _vgx_is_execution_explicitly_halted( self->context.timing_budget ) : 0;
  STACK_RETURN_INTEGER( self, halted );
}



/*******************************************************************//**
 * continue()
 ***********************************************************************
 */
static void __eval_continue( vgx_Evaluator_t *self ) {
  if( self->context.timing_budget ) {
    _vgx_clear_execution_explicit_halt( self->context.timing_budget );
  }
  STACK_RETURN_INTEGER( self, 1 );
}



/*******************************************************************//**
 * continueif( cond )
 ***********************************************************************
 */
static void __eval_continueif( vgx_Evaluator_t *self ) {
  if( __condition( GET_PITEM(self) ) && self->context.timing_budget ) {
    _vgx_clear_execution_explicit_halt( self->context.timing_budget );
  }
}



/*******************************************************************//**
 * first( [a [, b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_first( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  // Discard all but first item
  while( nargs-- > 1 ) {
    POP_PITEM( self );
  }
}



/*******************************************************************//**
 * firstval( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_firstval( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  // Find first value that is not NONE
  vgx_EvalStackItem_t item;
  SET_NONE( &item );
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    if( x.type != STACK_ITEM_TYPE_NONE && x.type != STACK_ITEM_TYPE_NAN ) { 
      item = x;
    }
  }

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  *px = item;
}



/*******************************************************************//**
 * lastval( [a [,b [, ...]]] )
 ***********************************************************************
 */
static void __eval_variadic_lastval( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  // Find last value that is not NONE
  vgx_EvalStackItem_t item;
  SET_NONE( &item );
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    if( x.type != STACK_ITEM_TYPE_NONE && x.type != STACK_ITEM_TYPE_NAN ) { 
      item = x;
      while( nargs-- >= 1 ) {
        POP_PITEM( self ); // discard all other arguments once item has been set
      }
      break;
    }
  }

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  *px = item;
}



/*******************************************************************//**
 * { x0, x1, ..., xn }
 ***********************************************************************
 */
static void __eval_variadic_set( vgx_Evaluator_t *self ) {
  // Push the number of set items on stack
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->type = STACK_ITEM_TYPE_SET;
  item->integer = self->op->arg.integer; // number of args
}



/*******************************************************************//**
 * in range( a, b )
 ***********************************************************************
 */
static void __eval_binary_range( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->type = STACK_ITEM_TYPE_RANGE;
  item->integer = 2;
}



/*******************************************************************//**
 * do( ... )
 ***********************************************************************
 */
static void __eval_variadic_do( vgx_Evaluator_t *self ) {
  DISCARD_ITEMS( self, self->op->arg.integer );
  // Push 1
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, 1 );
}



/*******************************************************************//**
 * void( ... )
 ***********************************************************************
 */
static void __eval_variadic_void( vgx_Evaluator_t *self ) {
  DISCARD_ITEMS( self, self->op->arg.integer );
  // Push null
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_NONE( px );
}



#endif
