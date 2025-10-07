/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _constant.h
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

#ifndef _VGX_VXEVAL_MODULES_CONSTANT_H
#define _VGX_VXEVAL_MODULES_CONSTANT_H


#define __M_SQRT3    1.73205080756887729353   // sqrt(3)
#define __M_SQRT5    2.23606797749978969641   // sqrt(5)
#define __M_PHI      1.61803398874989484820   // (1+sqrt(5)) / 2
#define __M_ZETA3    1.20205690315959428540   // zeta(3)
#define __M_GOOGOL   1e100
#define __REG1       EXPRESS_EVAL_MEM_REGISTER_R1
#define __REG2       EXPRESS_EVAL_MEM_REGISTER_R2
#define __REG3       EXPRESS_EVAL_MEM_REGISTER_R3
#define __REG4       EXPRESS_EVAL_MEM_REGISTER_R4
#define __CSTAGE1    0
#define __CSTAGE2    1
#define __CSTAGE3    2
#define __CSTAGE4    3


/******************************************************************************
 * 
 * 
 ******************************************************************************
 */

// Push Constant
static void __stack_push_constant_none( vgx_Evaluator_t *self );
static void __stack_push_constant_nan( vgx_Evaluator_t *self );
static void __stack_push_constant_inf( vgx_Evaluator_t *self );
static void __stack_push_constant_wild( vgx_Evaluator_t *self );
static void __stack_push_constant_string( vgx_Evaluator_t *self );
static void __stack_push_constant_integer( vgx_Evaluator_t *self );
static void __stack_push_constant_bitvector( vgx_Evaluator_t *self );
static void __stack_push_constant_real( vgx_Evaluator_t *self );
static void __stack_push_constant_true( vgx_Evaluator_t *self );
static void __stack_push_constant_false( vgx_Evaluator_t *self );
static void __stack_push_constant_inception( vgx_Evaluator_t *self );
static void __stack_push_constant_epoch( vgx_Evaluator_t *self );
static void __stack_push_constant_age( vgx_Evaluator_t *self );
static void __stack_push_constant_order( vgx_Evaluator_t *self );
static void __stack_push_constant_size( vgx_Evaluator_t *self );
static void __stack_push_constant_opcnt( vgx_Evaluator_t *self );
static void __stack_push_constant_vector( vgx_Evaluator_t *self );
static void __stack_push_constant_T_NEVER( vgx_Evaluator_t *self );
static void __stack_push_constant_T_MIN( vgx_Evaluator_t *self );
static void __stack_push_constant_T_MAX( vgx_Evaluator_t *self );
static void __stack_push_constant_D_ANY( vgx_Evaluator_t *self );
static void __stack_push_constant_D_IN( vgx_Evaluator_t *self );
static void __stack_push_constant_D_OUT( vgx_Evaluator_t *self );
static void __stack_push_constant_D_BOTH( vgx_Evaluator_t *self );
static void __stack_push_constant_M_ANY( vgx_Evaluator_t *self );
static void __stack_push_constant_M_STAT( vgx_Evaluator_t *self );
static void __stack_push_constant_M_SIM( vgx_Evaluator_t *self );
static void __stack_push_constant_M_DIST( vgx_Evaluator_t *self );
static void __stack_push_constant_M_LSH( vgx_Evaluator_t *self );
static void __stack_push_constant_M_INT( vgx_Evaluator_t *self );
static void __stack_push_constant_M_UINT( vgx_Evaluator_t *self );
static void __stack_push_constant_M_FLT( vgx_Evaluator_t *self );
static void __stack_push_constant_M_CNT( vgx_Evaluator_t *self );
static void __stack_push_constant_M_ACC( vgx_Evaluator_t *self );
static void __stack_push_constant_M_TMC( vgx_Evaluator_t *self );
static void __stack_push_constant_M_TMM( vgx_Evaluator_t *self );
static void __stack_push_constant_M_TMX( vgx_Evaluator_t *self );
static void __stack_push_constant_pi( vgx_Evaluator_t *self );
static void __stack_push_constant_e( vgx_Evaluator_t *self );
static void __stack_push_constant_root2( vgx_Evaluator_t *self );
static void __stack_push_constant_root3( vgx_Evaluator_t *self );
static void __stack_push_constant_root5( vgx_Evaluator_t *self );
static void __stack_push_constant_phi( vgx_Evaluator_t *self );
static void __stack_push_constant_zeta3( vgx_Evaluator_t *self );
static void __stack_push_constant_googol( vgx_Evaluator_t *self );
static void __stack_push_constant_R1( vgx_Evaluator_t *self );
static void __stack_push_constant_R2( vgx_Evaluator_t *self );
static void __stack_push_constant_R3( vgx_Evaluator_t *self );
static void __stack_push_constant_R4( vgx_Evaluator_t *self );
static void __stack_push_constant_C1( vgx_Evaluator_t *self );
static void __stack_push_constant_C2( vgx_Evaluator_t *self );
static void __stack_push_constant_C3( vgx_Evaluator_t *self );
static void __stack_push_constant_C4( vgx_Evaluator_t *self );
static void __stack_push_constant_SYNARC( vgx_Evaluator_t *self );
static void __stack_push_sys_tick( vgx_Evaluator_t *self );
static void __stack_push_sys_uptime( vgx_Evaluator_t *self );
static void __stack_push_stackval( vgx_Evaluator_t *self );


/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __push_integer( vgx_Evaluator_t *self, int64_t x ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( item, x );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __push_real( vgx_Evaluator_t *self, double x ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_REAL_PITEM_VALUE( item, x );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_none( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_NONE( item );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_nan( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_NAN( item );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_inf( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_REAL_PITEM_VALUE( item, INFINITY );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_wild( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = 0;
  item->type = STACK_ITEM_TYPE_WILD;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_string( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->CSTR__str = self->op->arg.CSTR__str;
  item->type = STACK_ITEM_TYPE_CSTRING;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_integer( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( item, self->op->arg.integer );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_bitvector( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_BITVECTOR_PITEM_VALUE( item, self->op->arg.bits );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_constant_real( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_REAL_PITEM_VALUE( item, self->op->arg.real );
}
  


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_true( vgx_Evaluator_t *self ) {
  __push_integer( self, true );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_false( vgx_Evaluator_t *self ) {
  __push_integer( self, false );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_inception( vgx_Evaluator_t *self ) {
  self->op->arg.real = self->current.t0;
  __stack_push_constant_real( self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_epoch( vgx_Evaluator_t *self ) {
  self->op->arg.real = self->current.tnow;
  __stack_push_constant_real( self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_age( vgx_Evaluator_t *self ) {
  self->op->arg.real = (self->current.tnow - self->current.t0);
  __stack_push_constant_real( self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_order( vgx_Evaluator_t *self ) {
  __push_integer( self, self->current.order );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_size( vgx_Evaluator_t *self ) {
  __push_integer( self, self->current.size );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_opcnt( vgx_Evaluator_t *self ) {
  __push_integer( self, self->current.op);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_vector( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  if( self->current.vector ) {
    item->vector = self->current.vector;
  }
  else {
    item->vector = self->graph->similarity->nullvector;
  }
  item->type = STACK_ITEM_TYPE_VECTOR;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_T_NEVER( vgx_Evaluator_t *self ) {
  __push_integer( self, TIME_EXPIRES_NEVER );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_T_MIN( vgx_Evaluator_t *self ) {
  __push_integer( self, TIMESTAMP_MIN );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_T_MAX( vgx_Evaluator_t *self ) {
  __push_integer( self, TIMESTAMP_MAX );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_D_ANY( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_ARCDIR_ANY );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_D_IN( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_ARCDIR_IN );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_D_OUT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_ARCDIR_OUT );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_D_BOTH( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_ARCDIR_BOTH );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_ANY( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_WILDCARD );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_STAT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_STATIC );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_SIM( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_SIMILARITY );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_DIST( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_DISTANCE );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_LSH( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_LSH );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_INT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_INTEGER );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_UINT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_UNSIGNED );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_FLT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_FLOAT );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_CNT( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_COUNTER );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_ACC( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_ACCUMULATOR );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_TMC( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_TIME_CREATED );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_TMM( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_TIME_MODIFIED );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_M_TMX( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_MOD_TIME_EXPIRES );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_pi( vgx_Evaluator_t *self ) {
  __push_real( self, M_PI );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_e( vgx_Evaluator_t *self ) {
  __push_real( self, M_E );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_root2( vgx_Evaluator_t *self ) {
  __push_real( self, M_SQRT2 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_root3( vgx_Evaluator_t *self ) {
  __push_real( self, __M_SQRT3 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_root5( vgx_Evaluator_t *self ) {
  __push_real( self, __M_SQRT5 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_phi( vgx_Evaluator_t *self ) {
  __push_real( self, __M_PHI );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_zeta3( vgx_Evaluator_t *self ) {
  __push_real( self, __M_ZETA3 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_googol( vgx_Evaluator_t *self ) {
  __push_real( self, __M_GOOGOL );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_R1( vgx_Evaluator_t *self ) {
  __push_integer( self, __REG1 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_R2( vgx_Evaluator_t *self ) {
  __push_integer( self, __REG2 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_R3( vgx_Evaluator_t *self ) {
  __push_integer( self, __REG3 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_R4( vgx_Evaluator_t *self ) {
  __push_integer( self, __REG4 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_C1( vgx_Evaluator_t *self ) {
  __push_integer( self, __CSTAGE1 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_C2( vgx_Evaluator_t *self ) {
  __push_integer( self, __CSTAGE2 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_C3( vgx_Evaluator_t *self ) {
  __push_integer( self, __CSTAGE3 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_C4( vgx_Evaluator_t *self ) {
  __push_integer( self, __CSTAGE4 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_constant_SYNARC( vgx_Evaluator_t *self ) {
  __push_integer( self, VGX_PREDICATOR_REL_SYNTHETIC );
}




/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_sys_tick( vgx_Evaluator_t *self ) {
  int64_t tick = __GET_CURRENT_NANOSECOND_TICK();
  __push_integer( self, tick );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_sys_uptime( vgx_Evaluator_t *self ) {
  double uptime = __GET_CURRENT_NANOSECOND_TICK() / 1000000000.0;
  __push_real( self, uptime );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_push_stackval( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  *item = self->rpn_program.stack.data[ self->op->arg.integer ];
}





#endif
