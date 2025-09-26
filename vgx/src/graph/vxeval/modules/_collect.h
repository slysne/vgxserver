/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _collect.h
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

#ifndef _VGX_VXEVAL_MODULES_COLLECT_H
#define _VGX_VXEVAL_MODULES_COLLECT_H

#include "_vxarcvector.h"
#include "_conditional.h"
#include "_stack.h"
#include "_heap.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */

// Collect
static void __eval_stage_var0_2( vgx_Evaluator_t *self );
static void __eval_stageif_var1_3( vgx_Evaluator_t *self );
static void __eval_unstage_var0_1( vgx_Evaluator_t *self );
static void __eval_unstageif_var1_2( vgx_Evaluator_t *self );
static void __eval_commit_var0_1( vgx_Evaluator_t *self );
static void __eval_commitif_var1_2( vgx_Evaluator_t *self );
static void __eval_collect_var0_1( vgx_Evaluator_t *self );
static void __eval_collectif( vgx_Evaluator_t *self );

static void __stack_push_collectable_real( vgx_Evaluator_t *self );
static void __stack_push_collectable_int( vgx_Evaluator_t *self );

static void __eval_mcull( vgx_Evaluator_t *self );
static void __eval_mcullif( vgx_Evaluator_t *self );





/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_predicator_t * __override_predicator( vgx_EvalStackItem_t *arcvalue, vgx_predicator_t *pred ) {
  // No override
  if( arcvalue == NULL || arcvalue->type == STACK_ITEM_TYPE_NONE ) {
    return NULL;
  }

  switch( arcvalue->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    pred->mod.bits = VGX_PREDICATOR_MOD_INTEGER;
    pred->val.integer = (int32_t)arcvalue->integer;
    break;
  case STACK_ITEM_TYPE_REAL:
    pred->mod.bits = VGX_PREDICATOR_MOD_FLOAT;
    pred->val.real = (float)arcvalue->real;
    break;
  case STACK_ITEM_TYPE_BITVECTOR:
    pred->mod.bits = VGX_PREDICATOR_MOD_UNSIGNED;
    pred->val.uinteger = (uint32_t)arcvalue->bits;
    break;
  default:
    pred->mod.bits = VGX_PREDICATOR_MOD_NONE;
    pred->val.bits = 0;
  }
  return pred;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __stage( vgx_Evaluator_t *self, int cx, vgx_EvalStackItem_t *arcvalue ) {
  int staged = 0;
  vgx_LockableArc_t *larc = self->context.larc;
  if( larc == NULL ) {
    return -1;
  }

  switch( self->context.collector_type & __VGX_COLLECTOR_ITEM_MASK ) {
  case __VGX_COLLECTOR_ARC:
    {
      vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)self->context.collector;
      vgx_predicator_t predicator = larc->head.predicator;
      // Override arc value if provided
      vgx_predicator_t *predicator_override = __override_predicator( arcvalue, &predicator );
      // larc is the original, additional override predicator pointer if we're overriding
      staged = collector->stage_arc( collector, larc, cx, predicator_override );
    }
    break;

  case __VGX_COLLECTOR_VERTEX:
    {
      vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)self->context.collector;
      staged = collector->stage_vertex( collector, larc, cx );
    }
    break;
  }
  return staged;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __unstage( vgx_Evaluator_t *self, int cx ) {
  int unstaged = 0;
  if( (self->context.collector_type & __VGX_COLLECTOR_ITEM_MASK) != 0 ) {
    unstaged = _vxarcvector_comparator__unstage( self->context.collector, cx );
  }
  return unstaged;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __commit( vgx_Evaluator_t *self, int cx ) {
  int committed = 0;
  if( (self->context.collector_type & __VGX_COLLECTOR_ITEM_MASK) != 0 ) {
    committed = _vxarcvector_comparator__commit( self->context.collector, cx );
  }
  return committed;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __collect( vgx_Evaluator_t *self, vgx_EvalStackItem_t *arcvalue ) {
  if( arcvalue && arcvalue->type == STACK_ITEM_TYPE_NONE ) {
    return 0;
  }

  int collected = 0;
  vgx_LockableArc_t *larc = self->context.larc;
  if( larc == NULL ) {
    return -1;
  }

  switch( self->context.collector_type & __VGX_COLLECTOR_ITEM_MASK ) {
  case __VGX_COLLECTOR_ARC:
    {
      vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)self->context.collector;
      // Save original larc predicator
      vgx_predicator_t orig_predicator = larc->head.predicator;
      // Override larc's arc value (in place) if provided
      __override_predicator( arcvalue, &larc->head.predicator );
      collected = __arcvector_collect_arc( collector, larc, 0.0, NULL );
      // Restore original larc predicator
      larc->head.predicator = orig_predicator;
    }
    break;

  case __VGX_COLLECTOR_VERTEX:
    {
      vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)self->context.collector;
      collected = __arcvector_collect_vertex( collector, larc, NULL );
    }
    break;
  }
  return collected;
}



/*******************************************************************//**
 * stage( [arcvalue [, cx] ] ) -> 1, 0, -1
 ***********************************************************************
 */
static void __eval_stage_var0_2( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int cx = 0; // default
  vgx_EvalStackItem_t arcvalue, *parcvalue = NULL;
  int staged;
  switch( nargs ) {
  case 2:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 1:
    *(parcvalue = &arcvalue) = POP_ITEM( self );
    /* FALLTHRU */
  case 0:
    staged = __stage( self, cx, parcvalue );
    break;
  default:
    /* nargs > 2 is automatic error and discard all items */
    DISCARD_ITEMS( self, nargs );
    staged = -1;
  }
  PUSH_INTEGER_VALUE( self, staged );
}



/*******************************************************************//**
 * stageif( condition, [arcvalue [, cx] ) -> condition
 ***********************************************************************
 */
static void __eval_stageif_var1_3( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int cx = 0; // default
  vgx_EvalStackItem_t arcvalue, *parcvalue = NULL;
  vgx_EvalStackItem_t *cond;
  switch( nargs ) {
  case 3:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 2:
    *(parcvalue = &arcvalue) = POP_ITEM( self );
    /* FALLTHRU */
  case 1:
    cond = GET_PITEM( self );
    if( __condition( cond ) ) {
      if( __stage( self, cx, parcvalue ) < 0 ) {
        SET_INTEGER_PITEM_VALUE( cond, 0 ); // did not stage! (error)
      }
    }
    return;
  default:
    DISCARD_ITEMS( self, nargs );
    PUSH_INTEGER_VALUE( self, 0 ); // i.e. cond = false
  }
}



/*******************************************************************//**
 * unstage( [cx] ) -> -1, 0, 1
 ***********************************************************************
 */
static void __eval_unstage_var0_1( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int cx = 0; // default
  int unstaged;
  switch( nargs ) {
  case 1:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 0:
    unstaged = __unstage( self, cx );
    break;
  default:
    DISCARD_ITEMS( self, nargs );
    unstaged = -1;
  }
  PUSH_INTEGER_VALUE( self, unstaged );
}



/*******************************************************************//**
 * unstageif( condition, [cx] ) -> condition
 ***********************************************************************
 */
static void __eval_unstageif_var1_2( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_EvalStackItem_t *cond;
  int cx = 0; // default
  switch( nargs ) {
  case 2:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 1:
    cond = GET_PITEM( self );
    if( __condition( cond ) ) {
      if( __unstage( self, cx ) < 0 ) {
        SET_INTEGER_PITEM_VALUE( cond, 0 ); // unstage error! -> false
      }
    }
    return;
  default:
    DISCARD_ITEMS( self, nargs );
    PUSH_INTEGER_VALUE( self, 0 ); // i.e. cond = false
  }
}


/*******************************************************************//**
 * commit( [cx] ) -> 1, 0, -1
 ***********************************************************************
 */
static void __eval_commit_var0_1( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int cx = 0; // default
  int committed;
  switch( nargs ) {
  case 1:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 0:
    committed = __commit( self, cx );
    break;
  default:
    DISCARD_ITEMS( self, nargs );
    committed = -1;
  }
  PUSH_INTEGER_VALUE( self, committed );
}



/*******************************************************************//**
 * commitif( condition, [cx] ) -> condition
 ***********************************************************************
 */
static void __eval_commitif_var1_2( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_EvalStackItem_t *cond;
  int cx = 0; // default
  switch( nargs ) {
  case 2:
    cx = (int)POP_ITEM( self ).integer; // assume C1-C4 (integers)
    /* FALLTHRU */
  case 1:
    cond = GET_PITEM( self );
    if( __condition( cond ) ) {
      if( __commit( self, cx ) < 0 ) {
        SET_INTEGER_PITEM_VALUE( cond, 0 ); // commit error! -> false
      }
    }
    return;
  default:
    DISCARD_ITEMS( self, nargs );
    PUSH_INTEGER_VALUE( self, 0 ); // i.e. cond = false
  }
}



/*******************************************************************//**
 * collect( [arcvalue] ) -> 1, 0, -1
 ***********************************************************************
 */
static void __eval_collect_var0_1( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_EvalStackItem_t arcvalue, *parcvalue = NULL;
  int collected;
  switch( nargs ) {
  case 1:
    *(parcvalue = &arcvalue) = POP_ITEM( self );
    /* FALLTHRU */
  case 0:
    collected = __collect( self, parcvalue );
    break;
  default:
    DISCARD_ITEMS( self, nargs );
    collected = -1;
  }
  PUSH_INTEGER_VALUE( self, collected );
}



/*******************************************************************//**
 * collectif( condition, [arcvalue] ) -> condition
 ***********************************************************************
 */
static void __eval_collectif_var1_2( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_EvalStackItem_t arcvalue, *parcvalue = NULL;
  vgx_EvalStackItem_t *cond;
  switch( nargs ) {
  case 2:
    *(parcvalue = &arcvalue) = POP_ITEM( self );
    /* FALLTHRU */
  case 1:
    cond = GET_PITEM( self );
    if( __condition( cond ) ) {
      if( __collect( self, parcvalue ) < 0 ) {
        SET_INTEGER_PITEM_VALUE( cond, 0 ); // collect error! -> false
      }
    }
    return;
  default:
    DISCARD_ITEMS( self, nargs );
    PUSH_INTEGER_VALUE( self, 0 ); // i.e. cond = false
  }
}



/*******************************************************************//**
 * collectable_sort_value()
 ***********************************************************************
 */
static QWORD __collectable_sort_value( vgx_Evaluator_t *self ) {
  vgx_CollectorItem_t top = {0};
  if( self->context.collector && _vgx_collector_is_sorted( self->context.collector ) ) {
    Cm256iHeap_t *heap = self->context.collector->container.sequence.heap;
    CALLABLE(heap)->HeapTop(heap, &top.item);
  }
  return top.sort.qword;
}



/*******************************************************************//**
 * collectable.real -> min_score
 ***********************************************************************
 */
static void __stack_push_collectable_real( vgx_Evaluator_t *self ) {
  vgx_VertexSortValue_t sort = {
    .qword = __collectable_sort_value( self )
  };
  STACK_RETURN_REAL( self, sort.flt64.value );
}



/*******************************************************************//**
 * collectable.int -> min_score
 ***********************************************************************
 */
static void __stack_push_collectable_int( vgx_Evaluator_t *self ) {
  vgx_VertexSortValue_t sort = {
    .qword = __collectable_sort_value( self )
  };
  STACK_RETURN_INTEGER( self, sort.int64.value );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cullheap_try_replace( vgx_ArcHeadHeapItem_t *pA, int64_t k, vgx_ArcHeadHeapItem_t *pitem ) {
#define downcmp( a, b ) ((a)->score < (b)->score)

  if( !downcmp( pA, pitem ) ) {
    return 0;
  }

  // Replace root with replacement element in preparation for down-heap operation
  *pA = *pitem;

  // Perform down-heap
  vgx_ArcHeadHeapItem_t *pparent = pA;
  int64_t i = 1;
  while( i < k ) {
    int64_t left = i;
    int64_t right = i+1;
    vgx_ArcHeadHeapItem_t *phigh = pA + right;

    if( downcmp( pA + left, pparent ) ) {
      if( right < k && downcmp( pA + right, pA + left ) ) {
        ++i;
      }
      else {
        --phigh;
      }
    }
    else if( right < k && downcmp( pA + right, pparent ) ) {
      ++i;
    }
    else {
      return 1;
    }

    vgx_ArcHeadHeapItem_t tmp = *phigh;
    *phigh = *pparent;
    *pparent = tmp;
    pparent = phigh;

    i = 2*i + 1;

  }

  return 1;
}



/*******************************************************************//**
 * mcull( score, k ) -> 1, 0, -1
 ***********************************************************************
 */
static void __eval_mcull( vgx_Evaluator_t *self ) {
  POP_PITEM( self ); // discard, k == self->rpn_program.cull
  vgx_EvalStackItem_t *pscore = POP_PITEM( self );

  vgx_ArcHeadHeapItem_t archead = {
    .score = pscore->type == STACK_ITEM_TYPE_REAL ? pscore->real : (double)pscore->integer,
    .vertex = self->context.VERTEX,
    .predicator = self->context.arrive
  };

  int64_t r = __cullheap_try_replace( self->context.cullheap, self->rpn_program.cull, &archead );
  STACK_RETURN_INTEGER( self, r ); 
}



/*******************************************************************//**
 * mcullif( condition, score, k ) -> condition
 ***********************************************************************
 */
static void __eval_mcullif( vgx_Evaluator_t *self ) {
  POP_PITEM( self ); // discard, k == self->rpn_program.cull
  vgx_EvalStackItem_t *pscore = POP_PITEM( self );

  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ArcHeadHeapItem_t archead = {
      .score = pscore->type == STACK_ITEM_TYPE_REAL ? pscore->real : (double)pscore->integer,
      .vertex = self->context.VERTEX,
      .predicator = self->context.arrive
    };

    __cullheap_try_replace( self->context.cullheap, self->rpn_program.cull, &archead );
  }
}





#endif
