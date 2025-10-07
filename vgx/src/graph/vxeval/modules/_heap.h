/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _heap.h
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

#ifndef _VGX_VXEVAL_MODULES_HEAP_H
#define _VGX_VXEVAL_MODULES_HEAP_H

#include "_math.h"
#include "_conditional.h"
#include "_memory.h"



static void __eval_heap_init( vgx_Evaluator_t *self );

static void __eval_heap_pushmin( vgx_Evaluator_t *self );
static void __eval_heap_pushmax( vgx_Evaluator_t *self );

static void __eval_heap_writemin( vgx_Evaluator_t *self );
static void __eval_heap_writemax( vgx_Evaluator_t *self );

static void __eval_heap_heapifymin( vgx_Evaluator_t *self );
static void __eval_heap_heapifymax( vgx_Evaluator_t *self );

static void __eval_heap_siftmin( vgx_Evaluator_t *self );
static void __eval_heap_siftmax( vgx_Evaluator_t *self );

static void __eval_heap_msort( vgx_Evaluator_t *self );
static void __eval_heap_msortrev( vgx_Evaluator_t *self );



typedef int (*__f_heapcmp)( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb );




/**************************************************************************//**
 * __minheap_lt
 *
 ******************************************************************************
 */
__inline static int __minheap_lt( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  // NONE interpreted as min value
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return pb->type != STACK_ITEM_TYPE_NONE; // a is -inf and thus less than b unless b is also -inf
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return 0; // a is not -inf and thus never less than -inf (b)
  }
  return __lt( NULL, pa, pb );
}




/**************************************************************************//**
 * __minheap_gt
 *
 ******************************************************************************
 */
__inline static int __minheap_gt( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  // NONE interpreted as min value
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return 0; // a is -inf and thus never greater than b
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return 1; // a is not -inf and thus always greater than b (-inf)
  }
  return __gt( NULL, pa, pb );
}




/**************************************************************************//**
 * __maxheap_lt
 *
 ******************************************************************************
 */
__inline static int __maxheap_lt( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  // NONE interpreted as max value
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return 0; // a is inf and can never be less than b
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return 1; // a not inf and is alwaus less than inf (b)
  }
  return __lt( NULL, pa, pb );
}




/**************************************************************************//**
 * __maxheap_gt
 *
 ******************************************************************************
 */
__inline static int __maxheap_gt( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  // NONE interpreted as max value
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return pb->type != STACK_ITEM_TYPE_NONE; // a is inf and always greater than b unless b is also inf
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return 0; // a not inf and thus never greater than inf (b)
  }
  return __gt( NULL, pa, pb );
}




/**************************************************************************//**
 * __heap_swap
 *
 ******************************************************************************
 */
__inline static vgx_EvalStackItem_t * __heap_swap( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  vgx_EvalStackItem_t tmp = *pa;
  *pa = *pb;
  *pb = tmp;
  return pa;
}



/*******************************************************************//**
 * mheapinit( h1, k ) -> k
 ***********************************************************************
 */
static void __eval_heap_init( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *ph1 = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;

  int64_t h1 = ph1->integer;
  int64_t k = pk->integer;
  if( h1 + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    vgx_EvalStackItem_t *px = pmem + h1;
    vgx_EvalStackItem_t *pend = px + k;
    while( px < pend ) {
      SET_NONE( px );
      ++px;
    }
    STACK_RETURN_INTEGER( self, k );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __upheap( vgx_EvalStackItem_t *pA, int64_t k, __f_heapcmp upcmp ) {
  int64_t i = k;
  while( i > 0 ) {
    vgx_EvalStackItem_t *pitem = pA + i;
    i = (i-1) >> 1;
    vgx_EvalStackItem_t *pparent = pA + i;
    if( upcmp( pparent, pitem ) ) {
      __heap_swap( pparent, pitem );
    }
    else {
      return;
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __downheap( vgx_EvalStackItem_t *pA, int64_t i, int64_t k, __f_heapcmp downcmp ) {
  vgx_EvalStackItem_t *pparent = pA + i;
  i = 2*i + 1;
  while( i < k ) {
    int64_t left = i;
    int64_t right = i+1;
    vgx_EvalStackItem_t *phigh = pA + right;

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
      return;
    }

    pparent = __heap_swap( phigh, pparent );

    i = 2*i + 1;

  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __heap_try_replace( vgx_EvalStackItem_t *pA, int64_t szA, vgx_EvalStackItem_t *pitem, __f_heapcmp downcmp ) {

  if( downcmp( pA, pitem ) ) {

    // Replace root with replacement element in preparation for down-heap operation
    *pA = *pitem;

    // Perform down-heap
    __downheap( pA, 0, szA, downcmp );

    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_heappush( vgx_Evaluator_t *self, __f_heapcmp downcmp ) {
  vgx_EvalStackItem_t *pvalue = POP_PITEM( self );
  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *ph1 = POP_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;
  int64_t h1 = ph1->integer;
  int64_t k = pk->integer;
  if( h1 + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 ); // TBD: what to return ?
  }
  else {
    int64_t r = __heap_try_replace( pmem + h1, k, pvalue, downcmp );
    STACK_RETURN_INTEGER( self, r ); 
  }
} 



/*******************************************************************//**
 * mheappushmin( h1, k, value ) -> 1 or 0
 ***********************************************************************
 */
static void __eval_heap_pushmin( vgx_Evaluator_t *self ) {
  __stack_heappush( self, __minheap_lt );
} 



/*******************************************************************//**
 * mheappushmax( h1, k, value ) -> 1 or 0
 ***********************************************************************
 */
static void __eval_heap_pushmax( vgx_Evaluator_t *self ) {
  __stack_heappush( self, __maxheap_gt );
} 



/*******************************************************************//**
 * *write( h1, k, ... ) -> n_written
 ***********************************************************************
 */
static void __stack_heapwrite( vgx_Evaluator_t *self, __f_heapcmp downcmp ) {

  int64_t nargs = self->op->arg.integer;
  if( nargs < 3 ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {

    int64_t n_items = nargs - 2;
    vgx_EvalStackItem_t *cursor = GET_PITEM( self ) - (nargs-1);

    vgx_EvalStackItem_t *ph1 = cursor++;
    vgx_EvalStackItem_t *pk = cursor++;

    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    int64_t msz = 1LL << mem->order;
    vgx_EvalStackItem_t *pmem = mem->data;

    int64_t h1 = ph1->integer;
    int64_t k = pk->integer;
    if( h1 + k > msz || k < 1 ) {
      STACK_RETURN_INTEGER( self, 0 ); // TBD: what to return ?
    }
    else {
      int64_t n_written = 0;
      while( n_items-- > 0 ) {
        n_written += __heap_try_replace( pmem + h1, k, cursor, downcmp );
        ++cursor;
      }

      STACK_RETURN_INTEGER( self, n_written );
    }
  }
} 



/*******************************************************************//**
 * mheapwritemin( h1, k, ... ) -> n_written
 ***********************************************************************
 */
static void __eval_heap_writemin( vgx_Evaluator_t *self ) {
  __stack_heapwrite( self, __minheap_lt );
} 



/*******************************************************************//**
 * mheapwritemax( h1, k, ... ) -> n_written
 ***********************************************************************
 */
static void __eval_heap_writemax( vgx_Evaluator_t *self ) {
  __stack_heapwrite( self, __maxheap_gt );
} 


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __heapify( vgx_EvalStackItem_t *pA, int64_t szA, __f_heapcmp downcmp ) {
  int64_t i = (szA / 2) - 1;
  while( i >= 0 ) {
    __downheap( pA, i--, szA, downcmp );
  }
} 



/*******************************************************************//**
 * heapify( h1, k ) -> k
 ***********************************************************************
 */
static void __stack_heapify( vgx_Evaluator_t *self, __f_heapcmp downcmp ) {
  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *ph1 = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;

  int64_t h1 = ph1->integer;
  int64_t k = pk->integer;
  if( h1 + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 ); // TBD: what to return ?
  }
  else {
    __heapify( pmem + h1, k, downcmp );
    STACK_RETURN_INTEGER( self, k );
  }
} 



/*******************************************************************//**
 * mheapifymin( h1, k ) -> k
 ***********************************************************************
 */
static void __eval_heap_heapifymin( vgx_Evaluator_t *self ) {
  __stack_heapify( self, __minheap_lt );
} 



/*******************************************************************//**
 * mheapifymax( h1, k ) -> k
 ***********************************************************************
 */
static void __eval_heap_heapifymax( vgx_Evaluator_t *self ) {
  __stack_heapify( self, __maxheap_gt );
} 



/*******************************************************************//**
 * ( h1, k, src_1, src_n ) -> n
 ***********************************************************************
 */
static void __stack_sift( vgx_Evaluator_t *self, __f_heapcmp downcmp ) {

  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );

  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *ph1 = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;

  int64_t h1 = ph1->integer;
  int64_t k = pk->integer;
  if( h1 + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    vgx_EvalStackItem_t *pA = pmem + h1;

    // Heapify
    __heapify( pA, k, downcmp );

    while( cursor < end ) {
      __heap_try_replace( pA, k, cursor, downcmp );
      ++cursor;
    }

    STACK_RETURN_INTEGER( self, n );
  }
}



/*******************************************************************//**
 * mheapsiftmin( h1, k, src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_heap_siftmin( vgx_Evaluator_t *self ) {
  __stack_sift( self, __minheap_lt );
}



/*******************************************************************//**
 * siftmax( h1, k, src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_heap_siftmax( vgx_Evaluator_t *self ) {
  __stack_sift( self, __maxheap_gt );
} 




#endif
