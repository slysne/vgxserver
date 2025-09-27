/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _logical.h
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
