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
static void __eval_unary_anncollect( vgx_Evaluator_t *self );



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
    double distance = CALLABLE( sim )->EuclideanDistance( sim, px->vector, y.vector, -1.0f );
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
    double value = CALLABLE( sim )->Similarity( sim, px->vector, y.vector, -1.0f );
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
    double cosine = CALLABLE( sim )->Cosine( sim, px->vector, y.vector, -1.0f );
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
    double jaccard = CALLABLE( sim )->Jaccard( sim, px->vector, y.vector, -1.0f );
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
  __collect( self, pscore );
}




/*
def ham(sim, sigma=1.5):
  p = acos(sim)/pi
  s = sqrt( 64.0*p*(1.0-p) )
  h = 64.0*p+sigma*s
  return int(round(h))

ham(1) -> 0
ham(0.99) -> 5
ham(0.98) -> 7
ham(0.97) -> 8
...
ham(0.9) -> 13
ham(0.8) -> 18
ham(0.7) -> 21
ham(0.6) -> 24
ham(0.5) -> 27
ham(0.4) -> 29
ham(0.3) -> 32
ham(0.2) -> 34
ham(0.1) -> 36
ham(0.0) -> 38
ham(-0.1) -> 40
ham(-0.2) -> 42
ham(-0.3) -> 44
ham(-0.4) -> 46
ham(-0.5) -> 48
ham(-0.6) -> 51
ham(-0.7) -> 53
ham(-0.8) -> 56
ham(-0.8) -> 56
ham(-0.9) -> 59
ham(-1.0) -> 64
*/

/*******************************************************************//**
 * Assume cosine similarity sim in range [0.0, 1.0], then:
 * cos_to_hamdist_1_5_sigma[ int(sim * 127) ] -> hamdist
 *
 * 
 ***********************************************************************
 */
static BYTE cos_to_hamdist_1_5_sigma[] = {
  38, 38, 38, 37, 37, 37, 37, 37, 37, 36, 36, 36, 36, 36, 36, 35,
  35, 35, 35, 35, 35, 34, 34, 34, 34, 34, 34, 33, 33, 33, 33, 33,
  33, 32, 32, 32, 32, 32, 32, 31, 31, 31, 31, 31, 31, 30, 30, 30,
  30, 30, 29, 29, 29, 29, 29, 29, 28, 28, 28, 28, 28, 27, 27, 27,
  27, 27, 26, 26, 26, 26, 26, 25, 25, 25, 25, 25, 24, 24, 24, 24,
  23, 23, 23, 23, 23, 22, 22, 22, 22, 21, 21, 21, 21, 20, 20, 20,
  20, 19, 19, 19, 18, 18, 18, 17, 17, 17, 16, 16, 16, 15, 15, 15,
  14, 14, 13, 13, 13, 12, 12, 11, 10, 10,  9,  8,  7,  6,  5,  0
};


/*******************************************************************//**
 * anncollect( hamfilter_above_score )
 ***********************************************************************
 */
static void __eval_unary_anncollect( vgx_Evaluator_t *self ) {

  // Arguments are in memory locations
  // [ . . . H ]
  //         ^----- hamfilter_above_sim
  //     SP^
  vgx_EvalStackItem_t *p_minscore_ham = POP_PITEM( self );
  double hamfilter_above_score = p_minscore_ham->real;

  vgx_ExpressEvalContext_t *ctx = &self->context;
  const vgx_Vertex_t *head = ctx->HEAD;


  // Must have vector
  vgx_Vector_t *target = head->vector;

  // Verify vectors exist
  vgx_ExpressEvalMemory_t *mem = ctx->memory;
  if( target == NULL || mem->probe == NULL ) {
    //SET_INTEGER_PITEM_VALUE( pexitat, 2 );
    STACK_RETURN_REAL( self, 0.0 );
  }

  // Checkpoint 1
  mem->counter.c1++;

  // Extract probe vector bytes
  BYTE *A = (BYTE*)CALLABLE( mem->probe )->Elements( mem->probe );
  int32_t lenA = mem->probe->metas.vlen;

  // Extract target vector bytes
  BYTE *B = (BYTE*)CALLABLE( target )->Elements( target );
  int32_t lenB = target->metas.vlen;

  // Safeguard
  int32_t len = minimum_value( lenA, lenB );

  // Hamming distance filter enabled when > 1.0
  if( hamfilter_above_score > 1.0 && mem->threshold > hamfilter_above_score && mem->threshold <= 2.0 ) {
    FP_t lshA = mem->probe->fp;
    FP_t lshB = target->fp;
    double min_cos = mem->threshold - 1.0;
    int idx = (int)(min_cos * 127) & 0x7F;
    int max_ham = cos_to_hamdist_1_5_sigma[ idx ];
    // LSH Hamming distance filter progressively stricter with higher thresholds
    if( hamdist64( lshA, lshB ) > max_ham ) {
      STACK_RETURN_REAL( self, 0.0 );
    }
  }
  
  // Checkpoint 2
  mem->counter.c2++;

  // -------------------
  // COMPUTE COSINE(A,B)
  // -------------------
  // Faster when both vectors are cosine_mode
  double cosine;
  if( mem->probe->metas.flags.cos && target->metas.flags.cos ) {
    double invnormprod = mem->probe->metas.scalar.invnorm * target->metas.scalar.invnorm;
    double min_cosine = mem->threshold - 1.0;
    cosine = vxeval_bytearray_dp_cosine_with_threshold( A, B, len, invnormprod, min_cosine );
    /*
    double dp = vxeval_bytearray_dot_product(A, B, len);
    cosine = dp * invnormprod;
    if( fabs( cosine ) > 1.0 || isnan( cosine ) ) {
      cosine = (double)((cosine > 0.0) - (cosine < 0.0));
    }
    */
  }
  else {
    cosine = vxeval_bytearray_cosine(A, B, len);
  }

  double score = cosine + 1.0; // [0.0 - 2.0]

  // Require sufficient cosine score
  if( score <= mem->threshold ) {
    STACK_RETURN_REAL( self, 0.0 ); // not collected
  }
   
  // Checkpoint 3
  mem->counter.c3++;

  // Collect
  // [ . . . _]
  //     SP^
  vgx_EvalStackItem_t score_arc = {
    .type = STACK_ITEM_TYPE_REAL,
    .real = score,
  };
  __collect( self, &score_arc );
  
  // Refresh running threshold
  if( ctx->collector->type == VGX_COLLECTOR_TYPE_SORTED_ARC_LIST ) {
    Cm256iHeap_t *heap = ctx->collector->container.sequence.heap;
    vgx_CollectorItem_t difficulty;
    CALLABLE(heap)->HeapTop(heap, &difficulty.item);
    // Update running difficulty (0.0 = 2.0)
    mem->threshold = difficulty.sort.flt64.value;
    // Update running cosine difficulty (-1.0 - 1.0)
    if( self->context.collector ) {
      self->context.collector->current_cos_difficulty = cosine;
    }

  }

  STACK_RETURN_REAL( self, score );
  
}



#endif
