/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vector.h
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

#ifndef _VGX_VXEVAL_MODULES_VECTOR_H
#define _VGX_VXEVAL_MODULES_VECTOR_H

#include "_maps.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_hamdist( vgx_Evaluator_t *self );
static void __eval_binary_euclidean( vgx_Evaluator_t *self );
static void __eval_binary_sim( vgx_Evaluator_t *self );
static void __eval_binary_cosine( vgx_Evaluator_t *self );
static void __eval_binary_jaccard( vgx_Evaluator_t *self );
static void __eval_ternary_anncollect( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_hamdist( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( PAIR_TYPE( px, &y ) == STACK_PAIR_TYPE_XVEC_YVEC && px->vector && y.vector ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    int64_t hamdist = CALLABLE( sim )->HammingDistance( sim, px->vector, y.vector );
    SET_INTEGER_PITEM_VALUE( px, hamdist ); 
  }
  else {
    SET_INTEGER_PITEM_VALUE( px, 64 );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_euclidean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( PAIR_TYPE( px, &y ) == STACK_PAIR_TYPE_XVEC_YVEC && px->vector && y.vector ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    double distance = CALLABLE( sim )->EuclideanDistance( sim, px->vector, y.vector );
    SET_REAL_PITEM_VALUE( px, distance ); 
  }
  else {
    SET_REAL_PITEM_VALUE( px, 1.0/FLT_MIN );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_sim( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( PAIR_TYPE( px, &y ) == STACK_PAIR_TYPE_XVEC_YVEC && px->vector && y.vector ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    double value = CALLABLE( sim )->Similarity( sim, px->vector, y.vector );
    SET_REAL_PITEM_VALUE( px, value ); 
  }
  else {
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_cosine( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( PAIR_TYPE( px, &y ) == STACK_PAIR_TYPE_XVEC_YVEC && px->vector && y.vector ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    double cosine = CALLABLE( sim )->Cosine( sim, px->vector, y.vector );
    SET_REAL_PITEM_VALUE( px, cosine ); 
  }
  else {
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_jaccard( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( PAIR_TYPE( px, &y ) == STACK_PAIR_TYPE_XVEC_YVEC && px->vector &&y.vector ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    double jaccard = CALLABLE( sim )->Jaccard( sim, px->vector, y.vector );
    SET_REAL_PITEM_VALUE( px, jaccard ); 
  }
  else {
    SET_REAL_PITEM_VALUE( px, 0.0 );
  }
}



/*******************************************************************//**
 * anncollect( vector_slot, minscore_slot, score_slot )
 ***********************************************************************
 */
static void __eval_ternary_anncollect( vgx_Evaluator_t *self ) {
  /*
  """ anncollect :=
        score = 1+cos_pi8(r1, next.vector);
        require( score > r2 );
        require( vset.add(next) > 0 );
        store(R3, score);
        collect();
  """
  */

  
  // Arguments are in memory locations
  // [ . . . v t s ]
  //             ^----- score mem location
  //         SP^
  vgx_EvalStackItem_t *mscore = POP_PITEM( self );
  int64_t idx_score = mscore->integer;
  // [ . . . v t s ]
  //           ^------- threshold mem location
  //       SP^
  vgx_EvalStackItem_t *mthres = POP_PITEM( self );
  int64_t idx_thres = mthres->integer;
  // [ . . . v t s]
  //         ^--------- probe mem location
  //     SP^
  vgx_EvalStackItem_t *mvector = POP_PITEM( self );
  int64_t idx_vector = mvector->integer;

  // Must have next vertex
  if( self->context.HEAD == NULL || self->context.HEAD->vector == NULL ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
 
  // Require next unvisited
  // [ . . . x t s]
  //         ^--------- 1 if already visited, else 0
  //       SP^
  __maps_vsethas( self, self->context.HEAD );
  //     SP^
  vgx_EvalStackItem_t *pvisited = POP_PITEM( self );
  if( pvisited->integer != 0 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }

  // Get argument objects from memory locations
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *data = mem->data;
  vgx_EvalStackItem_t *pscore = &data[ idx_score & mask ]; // s
  vgx_EvalStackItem_t *pthres = &data[ idx_thres & mask ]; // t
  double min_score = pthres->type == STACK_ITEM_TYPE_REAL ? pthres->real : pthres->type == STACK_ITEM_TYPE_INTEGER ? pthres->integer : 0.0;
  vgx_EvalStackItem_t *pvector = &data[ idx_vector & mask ]; // v

  // Vectors for cosine eval will be pushed on stack
  // [ . . . A t s]
  //         ^--------- probe vector object
  //       SP^
  vgx_EvalStackItem_t *pa = NEXT_PITEM( self );
  *pa = *pvector;

  vgx_EvalStackItem_t *pb = NEXT_PITEM( self );
  // [ . . . A B s]
  //           ^------ target vector object
  //         SP^
  pb->vector = self->context.HEAD->vector;
  pb->type = STACK_ITEM_TYPE_VECTOR; 
  
  // Compute cosine(A,B)
  // [ . . . c B s]
  //         ^-------- cosine score value (-1.0 - 1.0)
  //       SP^
  f_cos_pi8( self );

  // Require sufficient cosine score
  // [ . . . c B s]
  //         ^-------- cosine score value (-1.0 - 1.0)
  //     SP^
  vgx_EvalStackItem_t *psim = POP_PITEM( self );
  double sim = psim->real + 1; // (0.0 - 2.0)
  if( sim <= min_score ) {
    STACK_RETURN_INTEGER( self, 0 ); // not collected
  }

  // Mark as visited
  // [ . . . m B s]
  //         ^-------- 1 (next marked as visited)
  //       SP^
  __maps_vsetadd( self, self->context.HEAD );

  // Write sim score to memory location
  SET_REAL_PITEM_VALUE( pscore, sim );

  // Collect
  // [ . . . m B s]
  //         ^-------- Already 1 from above (assume collect successful below)
  //       SP^
  __collect( self, NULL );
}



#endif
