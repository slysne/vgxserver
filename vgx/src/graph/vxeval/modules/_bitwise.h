/*
###################################################
#
# File:   _bitwise.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_BITWISE_H
#define _VGX_VXEVAL_MODULES_BITWISE_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_bitwise_not( vgx_Evaluator_t *self );
static void __eval_bitwise_shl( vgx_Evaluator_t *self );
static void __eval_bitwise_shr( vgx_Evaluator_t *self );
static void __eval_bitwise_or( vgx_Evaluator_t *self );
static void __eval_bitwise_and( vgx_Evaluator_t *self );
static void __eval_bitwise_xor( vgx_Evaluator_t *self );




/*******************************************************************//**
 * ~x
 ***********************************************************************
 */
static void __eval_unary_bitwise_not( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_BITVECTOR ) {
    px->bits = ~px->bits;
  }
  else {
    if( px->type != STACK_ITEM_TYPE_INTEGER ) {
      __eval_unary_cast_int( self );
    }
    int64_t x = px->integer;
    px->integer = ~x;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_bitwise_shl( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( y.type == STACK_ITEM_TYPE_INTEGER ) {
    if( px->type == STACK_ITEM_TYPE_BITVECTOR ) {
      px->bits <<= y.integer;
    }
    else {
      if( px->type != STACK_ITEM_TYPE_INTEGER ) {
        __eval_unary_cast_int( self );
      }
      px->integer <<= y.integer;
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_bitwise_shr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( y.type == STACK_ITEM_TYPE_INTEGER ) {
    if( px->type == STACK_ITEM_TYPE_BITVECTOR ) {
      px->bits >>= y.integer;
    }
    else {
      if( px->type != STACK_ITEM_TYPE_INTEGER ) {
        __eval_unary_cast_int( self );
      }
      px->integer >>= y.integer;
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_bitwise_or( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  if( __is_stack_pair_type_integer_compatible( pair_type ) ) {
    px->bits |= y.bits;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_bitwise_and( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  if( __is_stack_pair_type_integer_compatible( pair_type ) ) {
    px->bits &= y.bits;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_bitwise_xor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, &y );
  if( __is_stack_pair_type_integer_compatible( pair_type ) ) {
    px->bits ^= y.bits;
  }
}





#endif
