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

__inline static void __stall_detect( vgx_BaseCollector_context_t *collector, vgx_ExpressEvalMemory_t *mem, float score ) {
  // Detect result heap stall
  if( score > mem->stall_check.last_top_k_th_margin ) {
    mem->stall_check.unimproved_count = 0;
    // pretent recorded top-k is a little bit worse to prevent stall when running scores are still pretty good
    mem->stall_check.last_top_k_th_margin = score - collector->dynamic_taper_gamma;
    mem->stall_check.heap_stalled = false;
  }
  else {
    int W = collector->shadow_trail.length;
    // heap stalled
    if( ++mem->stall_check.unimproved_count > W ) {
      mem->stall_check.heap_stalled = true;
    }
  }
}


__inline static void __dynamic_taper( vgx_BaseCollector_context_t *collector, vgx_ExpressEvalMemory_t *mem, float cosine ) {

#define VISIT_WINDOW_CHECKPOINT 128               //
#define VISIT_WINDOW_UNIMPROVED_MAX 112           // 87.5% of checkpoint window
#define VISIT_WINDOW_UNIMPROVED_MIN 96            // 75%% of checkpoint window
#define DYNAMIC_TAPER_MAX_LOOSEN_FACTOR 1.0625    // 1 + 1/16
#define DYNAMIC_TAPER_MIN_LOOSEN_FACTOR 1.03125   // 1 + 1/32
#define DYNAMIC_TAPER_MIN_TIGHTEN_FACTOR 0.96875  // 1 - 1/32
#define DYNAMIC_TAPER_MAX_TIGHTEN_FACTOR 0.9375   // 1 - 1/16
#define DYNAMIC_TAPER_UPPER_BOUND 2.875           // 3 - 1/8
#define DYNAMIC_TAPER_LOWER_BOUND 0.3125          // 1/4 + 1/16
#define HIGH_COSINE_GAIN 0.018f                   //
#define LOW_COSINE_GAIN 0.009f                    //

  // Maintain running top cosine for beam taper
  if( cosine > mem->dynamic_taper.top_1_best ) {
    mem->dynamic_taper.top_1_best = cosine;
    mem->dynamic_taper.window_top_1_unimproved = 0;
  }
  else {
    mem->dynamic_taper.window_top_1_unimproved++;
  }

  // Evaluate our progress
  if( ++mem->dynamic_taper.window_counter >= VISIT_WINDOW_CHECKPOINT ) {
    double factor;
    // -- LOOSEN --
    // We're decidedly not improving the running top cosine, loosen taper
    if( mem->dynamic_taper.window_top_1_unimproved > VISIT_WINDOW_UNIMPROVED_MAX ) {
      factor = DYNAMIC_TAPER_MAX_LOOSEN_FACTOR;
    }
    // We're mostly not improving the top cosine, loosen taper a bit
    else if( mem->dynamic_taper.window_top_1_unimproved > VISIT_WINDOW_UNIMPROVED_MIN ) {
      factor = DYNAMIC_TAPER_MIN_LOOSEN_FACTOR;
    }
    // -- TIGHTEN --
    // We are improving at a decent rate, tighten taper a bit
    else if( mem->dynamic_taper.top_1_best > mem->dynamic_taper.previous_window_best + LOW_COSINE_GAIN ) {
      factor =  DYNAMIC_TAPER_MIN_TIGHTEN_FACTOR;
    }
    // We are improving at a very good rate, tighten taper
    else if( mem->dynamic_taper.top_1_best > mem->dynamic_taper.previous_window_best + HIGH_COSINE_GAIN ) {
      factor = DYNAMIC_TAPER_MAX_TIGHTEN_FACTOR;
    }
    // -- STEADY --
    else {
      factor = 1.0;
    }

    // New taper
    double taper = factor * collector->dynamic_taper;
    collector->dynamic_taper = clamp_value( taper, DYNAMIC_TAPER_LOWER_BOUND, DYNAMIC_TAPER_UPPER_BOUND );

    // Update cosine at checkpoint
    mem->dynamic_taper.previous_window_best = mem->dynamic_taper.top_1_best;
    
    // Reset window
    mem->dynamic_taper.window_counter = 0;
    mem->dynamic_taper.window_top_1_unimproved = 0;
  }
}



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
 * anncollect( )
 ***********************************************************************
 */
static float __fast_anncollect( vgx_Evaluator_t *self, const vgx_Vector_t *probe, const vgx_Vector_t *target ) {
  if( probe == NULL || target == NULL ) {
    return 0.0f;
  }

  vgx_ExpressEvalMemory_t *mem = self->context.memory;

  // Eval counter
  mem->counter.eval++;

  // Extract probe vector bytes
  BYTE *A = (BYTE*)CALLABLE( probe )->Elements( probe );
  int32_t lenA = probe->metas.vlen;

  // Extract target vector bytes
  int32_t lenB = target->metas.vlen;
  BYTE *B = (BYTE*)CALLABLE( target )->Elements( target );

  // Safeguard
  int32_t len = minimum_value( lenA, lenB );

  /*
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
  */  

  // -------------------
  // COMPUTE COSINE(A,B)
  // -------------------
  // Faster when both vectors are cosine_mode
  float cosine;
  if( mem->probe->metas.flags.cos && target->metas.flags.cos ) {
    double invnormprod = mem->probe->metas.scalar.invnorm * target->metas.scalar.invnorm;
    cosine = (float)vxeval_bytearray_dp_cosine( A, B, len, invnormprod );
    //double min_cosine = mem->threshold - 1.0;
    //cosine = vxeval_bytearray_dp_cosine_with_threshold( A, B, len, invnormprod, min_cosine );
  }
  else {
    cosine = (float)vxeval_bytearray_cosine(A, B, len);
  }

  vgx_BaseCollector_context_t *base = self->context.collector;

  float top_k_th = _vxquery_collector__worst_heap_flt64_score( base->container.sequence.heap );
  float beam_j_th = base->beam_heap != NULL ? _vxquery_collector__worst_heap_flt64_score( base->beam_heap ) : 0.0f;

  // Adaptive search enabled
  if( base->adaptive_recursion ) {
    //__stall_detect( base, mem, top_k_th );
    __dynamic_taper( base, mem, cosine );
  }

  float score = cosine + 1.0f; // [0.0 - 2.0]
  float threshold = _vxquery_collector__get_current_threshold( base );

  // Ignore everything below the running threshold
  if( score < threshold ) {
    // Inject running threshold to keep delay line ticking
    _vxquery_collector__push_shadow_trail( &base->shadow_trail, threshold );
    return 0.0f;
  }

  // Item is not collectable to result or beam, update threshold queue with inferior score
  if( score <= top_k_th && score <= beam_j_th ) {
    // Score is good enough to help refine the baseline threshold
    mem->counter.contrib++;
    //_vxquery_collector__push_shadow_trail( &base->shadow_trail, score  );
    _vxquery_collector__push_shadow_trail( &base->shadow_trail, score );
    return 0.0f;
  }
      
  // Score is good enough to help refine the baseline threshold
  mem->counter.contrib++;

  // Frontier contribution 
  if( score > beam_j_th ) {
    mem->counter.frontier++;
  }

  // Result contribution
  if( score > top_k_th ) {
    mem->counter.accept++;
  }

  // Collect
  // [ . . . _]
  //     SP^
  vgx_EvalStackItem_t score_arc = {
    .type = STACK_ITEM_TYPE_REAL,
    .real = score,
  };
  __collect( self, &score_arc );
  
  //mem->threshold = base->shadow_trail.threshold;
  
  /*
  // Refresh running threshold
  if( self->context.collector->type == VGX_COLLECTOR_TYPE_SORTED_ARC_LIST ) {
    // Update running difficulty (0.0 = 2.0)
    vgx_CollectorItem_t difficulty;
    mem->threshold = _vxquery_collector__get_current_threshold( self->context.collector, &difficulty );
    // // Update running cosine difficulty (-1.0 - 1.0)
    // self->context.collector->current_cos_difficulty = mem->threshold - 1.0;
  }
  */

  return score;
  
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_anncollect( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  double score = 0.0;
  if( px->type == STACK_ITEM_TYPE_VECTOR ) {
    const vgx_Vector_t *probe = px->vector;
    const vgx_Vector_t *target = self->context.HEAD->vector;
    score = __fast_anncollect( self, probe, target );
  }
  SET_REAL_PITEM_VALUE( px, score );
}



#endif
