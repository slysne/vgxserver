/*
###################################################
#
# File:   _logical.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_LOGICAL_H
#define _VGX_VXEVAL_MODULES_LOGICAL_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_not( vgx_Evaluator_t *self );
static void __eval_logical_or( vgx_Evaluator_t *self );
static void __eval_logical_and( vgx_Evaluator_t *self );



/*******************************************************************//**
 * !x
 ***********************************************************************
 */
static void __eval_unary_not( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, px->bits == 0 );
}



/*******************************************************************//**
 * ||
 ***********************************************************************
 */
static void __eval_logical_or( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->bits == 0 && y.bits ) {
    px->type = y.type;
    px->bits = y.bits;
  }
}



/*******************************************************************//**
 * &&
 ***********************************************************************
 */
static void __eval_logical_and( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->bits && y.bits ) {
    px->type = y.type;
    px->bits = y.bits;
  }
  else {
    px->type = STACK_ITEM_TYPE_INTEGER;
    px->bits = 0;
  }
}




#endif
