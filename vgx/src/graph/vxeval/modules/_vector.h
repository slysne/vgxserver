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


__inline static void __dynamic_taper( vgx_BaseCollector_context_t *collector, vgx_ExpressEvalMemory_t *mem, float score ) {

#define VISIT_WINDOW_CHECKPOINT 100               //
#define VISIT_WINDOW_UNIMPROVED_MAX 88            // 88% of checkpoint window
#define VISIT_WINDOW_UNIMPROVED_MIN 64            // 64% of checkpoint window
#define DYNAMIC_TAPER_MAX_LOOSEN_FACTOR 1.05      //
#define DYNAMIC_TAPER_MIN_LOOSEN_FACTOR 1.02      //
#define DYNAMIC_TAPER_MIN_TIGHTEN_FACTOR 0.98     //
#define DYNAMIC_TAPER_MAX_TIGHTEN_FACTOR 0.95     //
#define DYNAMIC_TAPER_UPPER_BOUND 3.0             //
#define DYNAMIC_TAPER_LOWER_BOUND (1.0/3)         //
#define HIGH_SCORE_GAIN 0.040f                    //
#define LOW_SCORE_GAIN 0.020f                     //

  // Maintain running top score for beam taper 
  if( score > mem->dynamic_taper.top_1_best ) {
    mem->dynamic_taper.top_1_best = score;
    mem->dynamic_taper.window_top_1_unimproved = 0;
  }
  else if( score > mem->dynamic_taper.previous_1_window_best ) {
    mem->dynamic_taper.window_top_1_unimproved /= 2;
  }
  else {
    mem->dynamic_taper.window_top_1_unimproved++;
  }
  
  // Keep counting
  if( ++mem->dynamic_taper.window_counter < VISIT_WINDOW_CHECKPOINT ) {
    return;
  }

  // Evaluate our progress
  double factor;
  // -- LOOSEN --
  // We're decidedly not improving the running top score, loosen taper
  if( mem->dynamic_taper.window_top_1_unimproved > VISIT_WINDOW_UNIMPROVED_MAX ) {
      factor = DYNAMIC_TAPER_MAX_LOOSEN_FACTOR;
  }
  // We're mostly not improving the top score, loosen taper a bit
  else if( mem->dynamic_taper.window_top_1_unimproved > VISIT_WINDOW_UNIMPROVED_MIN ) {
    factor = DYNAMIC_TAPER_MIN_LOOSEN_FACTOR;
  }
  // -- TIGHTEN --
  // We are improving at a decent rate, tighten taper a bit
  else if( mem->dynamic_taper.top_1_best > mem->dynamic_taper.previous_1_window_best + LOW_SCORE_GAIN ) {
    factor = DYNAMIC_TAPER_MIN_TIGHTEN_FACTOR;
    factor = clamp_value( factor, DYNAMIC_TAPER_MIN_TIGHTEN_FACTOR, 1.0f );
  }
  // We are improving at a very good rate, tighten taper
  else if( mem->dynamic_taper.top_1_best > mem->dynamic_taper.previous_1_window_best + HIGH_SCORE_GAIN ) {
    factor = DYNAMIC_TAPER_MAX_TIGHTEN_FACTOR;
    factor = clamp_value( factor, DYNAMIC_TAPER_MAX_TIGHTEN_FACTOR, 1.0f );
  }
  
  // -- STEADY --
  else {
    factor = 1.0;
  }

  // delta: reactivity control -> >0.0 expand, <0.0 limit, -1.0 turn controller off
  factor += collector->delta * (factor - 1.0);

  // New taper
  double taper = factor * collector->dynamic_taper;
  collector->dynamic_taper = clamp_value( taper, DYNAMIC_TAPER_LOWER_BOUND, DYNAMIC_TAPER_UPPER_BOUND );

  // Update score at checkpoint
  mem->dynamic_taper.previous_1_window_best = mem->dynamic_taper.top_1_best;
  
  // Reset window
  mem->dynamic_taper.window_counter = 0;
  mem->dynamic_taper.window_top_1_unimproved = 0;

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
static int __fast_anncollect( vgx_Evaluator_t *self, const vgx_Vector_t *probe, const vgx_Vertex_t *vertex, const vgx_Vector_t *target, float *rscore ) {

  vgx_ExpressEvalMemory_t *mem = self->context.memory;

  // Eval counter
  mem->counter.eval++;

  vgx_BaseCollector_context_t *base = self->context.collector;
  vgx_Evaluator_t *RF = base->recursion_filter;

  float score;

  if( probe && target ) {
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
      FP_t lshA = probe->fp;
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
    double cosine;
    if( probe->metas.flags.cos && target->metas.flags.cos ) {
      double invnormprod = probe->metas.scalar.invnorm * target->metas.scalar.invnorm;
      cosine = vxeval_bytearray_dp_cosine( A, B, len, invnormprod );
    }
    else {
      cosine = vxeval_bytearray_cosine(A, B, len);
    }
    score = (float)cosine + 1.0f; // range is [0.0 - 2.0], so 1.0 represents "zero" middle ground
  }
  else if( RF ) {
    // Execute custom recursion filter
    vgx_EvalStackItem_t *result = CALLABLE( RF )->EvalVertex( RF, vertex );
    if( result == NULL || !iEvaluator.IsPositive( result ) ) {
      self->context.larc->flag.recursion_skip_heap_collect = true;
    }
    double retval = (float)iEvaluator.GetReal( result );
    RF->context.rankscore = score = clamp_value( (float)retval, 0.0f, 2.0f );
    RF = NULL; // forget the filter so we don't execute it again below
  }
  else {
    score = 1.0f;
  }

  // Adaptive search enabled
  if( base->adaptive_recursion ) {
    __dynamic_taper( base, mem, score );
  }

  float threshold = _vxquery_collector__get_current_threshold( base ) + base->epsilon;
  float injection;

  // Ignore everything below the running threshold
  if( score < threshold ) {
    // Inject running threshold to keep delay line ticking
    _vxquery_collector__push_shadow_trail( &base->shadow_trail, threshold );
    *rscore = score;
    return 0;
  }
  
  // Score is good enough to help refine the baseline threshold
  mem->counter.contrib++;
  
  // Extract worst score on the heaps
  float top_k_th = fmaxf( _vxquery_collector__worst_heap_recursion_score( base->container.sequence.heap ), threshold );
  float beam_j_th = base->beam_heap != NULL ? fmaxf( _vxquery_collector__worst_heap_recursion_score( base->beam_heap ), threshold ) : threshold;
  float collectable_threshold = fminf( top_k_th, beam_j_th ); // <- worst of either beam or heap
  
  int collected = 0;

  // Score good enough for at least one of the heaps
  if( score > collectable_threshold ) {
    // Frontier contribution 
    if( score > beam_j_th ) {
      mem->counter.frontier++;
    }

    // Result contribution
    if( score > top_k_th ) {
      mem->counter.accept++;
      if( RF ) {
        RF->context.rankscore = score;
        // Execute custom recursion filter
        vgx_EvalStackItem_t *result = CALLABLE( RF )->EvalVertex( RF, vertex );
        if( result == NULL || !iEvaluator.IsPositive( result ) ) {
          self->context.larc->flag.recursion_skip_heap_collect = true;
        }
        // Special case: recursion filter returned a float, interpret as score override
        if( result->type == STACK_ITEM_TYPE_REAL ) {
          RF->context.rankscore = score = clamp_value( (float)result->real, 0.0f, 2.0f );
        }
      }
    }

    // Collect
    // [ . . . _]
    //     SP^
    vgx_EvalStackItem_t score_arc = {
      .type = STACK_ITEM_TYPE_REAL,
      .real = score,
    };
    collected = __collect( self, &score_arc );
    
    // Clear any temporary flags
    self->context.larc->flag.bits = 0;

    // Refresh
    top_k_th = fmaxf( _vxquery_collector__worst_heap_recursion_score( base->container.sequence.heap ), threshold );
    beam_j_th = base->beam_heap != NULL ? fmaxf( _vxquery_collector__worst_heap_recursion_score( base->beam_heap ), threshold ) : threshold;
    collectable_threshold = fminf( top_k_th, beam_j_th );

    // Update current beam's best score
    mem->dynamic_taper.beam_1_best = fmaxf( mem->dynamic_taper.beam_1_best, score );
  }
  
  // Inject value into the delay line derived from current score and the current state of search progress
  float top_1 = fmaxf( mem->dynamic_taper.top_1_best, threshold );
  float beam_1 = fmaxf( mem->dynamic_taper.beam_1_best, threshold );
  //float short_threshold = _vxquery_collector__get_current_short_threshold( base ); // + base->epsilon;
  float heap_signal = (top_k_th + beam_j_th) / 2; 
  
  // good beam quality -> 0.0 (ignore negative)
  // bad beam quality -> 1.0
  float beam_deficit = (top_k_th - beam_1) / fmaxf( (top_1 - beam_1), 1e-6f );
  // good beam -> more contribution from heap worst values
  // bad beam ->  less contribution from heap worst values
  float beta = clamp_value( beam_deficit, 0.6f, 0.9f );
  injection = beta * score + (1.0f - beta) * heap_signal;

  _vxquery_collector__push_shadow_trail( &base->shadow_trail, injection );
  
  *rscore = score;
  return collected;
  
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_unary_anncollect( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  float score = 0.0f;
  if( px->type == STACK_ITEM_TYPE_VECTOR ) {
    const vgx_Vector_t *probe = px->vector;
    const vgx_Vertex_t *vertex = self->context.HEAD;
    const vgx_Vector_t *target = vertex->vector;
    __fast_anncollect( self, probe, vertex, target, &score );
  }
  SET_REAL_PITEM_VALUE( px, score );
}



#endif
