/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    __utest_comlibsequence_heap.h
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

#ifndef __UTEST_COMLIBSEQUENCE_HEAP_H
#define __UTEST_COMLIBSEQUENCE_HEAP_H

#include "__comlibtest_macro.h"

#define HEAP_TEST_NAME( type ) __utest_heap_##type
#define X_HEAP_TEST_NAME( type ) HEAP_TEST_NAME( type )



/**************************************************************************//**
 * cmp_reverse
 *
 ******************************************************************************
 */
__inline static int cmp_reverse( const _CSEQ_ELEMENT_TYPE *a, const _CSEQ_ELEMENT_TYPE *b ) {
  return __compare_elements_default( b, a );
}


BEGIN_UNIT_TEST( X_HEAP_TEST_NAME( _CSEQ_TYPENAME ) ) {
  
  _CSEQ_TYPENAME *min_heap = NULL;
  _CSEQ_VTABLETYPE *imin_heap = NULL;
  _CSEQ_TYPENAME *max_heap = NULL;
  _CSEQ_VTABLETYPE *imax_heap = NULL;

  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create min heap" ) {
    _CSEQ_CONSTRUCTOR_ARGSTYPE min_heap_args = { .element_capacity=1, .comparator=cmp_reverse };
    min_heap = COMLIB_OBJECT_NEW( _CSEQ_TYPENAME, NULL, &min_heap_args );
    TEST_ASSERTION( min_heap, "min_heap created" );
    imin_heap = CALLABLE(min_heap);
    TEST_ASSERTION( imin_heap, "imin_heap interface exists" );
    TEST_ASSERTION( imin_heap->Capacity( min_heap ) == (1 << _CSEQ_MIN_CAPACITY_EXP), "min_heap has minimum capacity" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create max heap" ) {
    _CSEQ_CONSTRUCTOR_ARGSTYPE max_heap_args = { .element_capacity=1, .comparator=NULL };
    max_heap = COMLIB_OBJECT_NEW( _CSEQ_TYPENAME, NULL, &max_heap_args );
    TEST_ASSERTION( max_heap, "max_heap created" );
    imax_heap = CALLABLE(max_heap);
    TEST_ASSERTION( imax_heap, "imax_heap interface exists" );
    TEST_ASSERTION( imax_heap->Capacity( max_heap ) == (1 << _CSEQ_MIN_CAPACITY_EXP), "max_heap has minimum capacity" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Push and pop min-heap" ) {
    int N = 1000000;
    TEST_ASSERTION( imin_heap->Length( min_heap ) == 0,                   "heap starts empty" );

    _CSEQ_ELEMENT_TYPE rnd;
    _CSEQ_ELEMENT_TYPE elem, prev;

    // Push random elements
    for( int i=0; i<N; i++ ) {
      __random_element( &rnd );
      TEST_ASSERTION( imin_heap->HeapPush( min_heap, &rnd ) == 1,   "one random element pushed" );
    }
    TEST_ASSERTION( imin_heap->Length( min_heap ) == N,                   "all elements pushed" );

    // Pop elements, should be in ascending order
    TEST_ASSERTION( imin_heap->HeapPop( min_heap, &prev ) == 1,     "first min element popped" );
    for( int i=1; i<N; i++ ) {
      TEST_ASSERTION( imin_heap->HeapPop( min_heap, &elem ) == 1,   "min element popped" );
      TEST_ASSERTION( __compare_elements_default( &elem, &prev ) >= 0,    "top heap element should be larger than or equal to previous" );
      __copy_element( &prev, &elem );
    }
    TEST_ASSERTION( imin_heap->Length( min_heap ) == 0,                   "heap empty" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Push and pop max-heap" ) {
    int N = 1000000;
    TEST_ASSERTION( imax_heap->Length( max_heap ) == 0,                   "heap starts empty" );

    _CSEQ_ELEMENT_TYPE rnd;
    _CSEQ_ELEMENT_TYPE elem, prev;

    // Push random elements
    for( int i=0; i<N; i++ ) {
      __random_element( &rnd );
      TEST_ASSERTION( imax_heap->HeapPush( max_heap, &rnd ) == 1,   "one random element pushed" );
    }
    TEST_ASSERTION( imax_heap->Length( max_heap ) == N,                   "all elements pushed" );

    // Pop elements, should be in descending order
    TEST_ASSERTION( imax_heap->HeapPop( max_heap, &prev ) == 1,     "first max element popped" );
    for( int i=1; i<N; i++ ) {
      TEST_ASSERTION( imax_heap->HeapPop( max_heap, &elem ) == 1,   "max element popped" );
      TEST_ASSERTION( __compare_elements_default( &elem, &prev ) <= 0,    "top heap element should be smaller than or equal previous" );
      __copy_element( &prev, &elem );
    }
    TEST_ASSERTION( imax_heap->Length( max_heap ) == 0,                   "heap empty" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Top-k using min-heap" ) {
    int N = 2000000;
    int k = 200;
    TEST_ASSERTION( imin_heap->Length( min_heap ) == 0,                   "heap starts empty" );
    
    _CSEQ_ELEMENT_TYPE vmin;

    // populate random test input data
    _CSEQ_ELEMENT_TYPE *source_data;
    CALIGNED_ARRAY( source_data, _CSEQ_ELEMENT_TYPE, N );
    TEST_ASSERTION( source_data != NULL, "test data created" );
    _CSEQ_ELEMENT_TYPE *p = source_data;
    for( int i=0; i<N; i++ ) {
      __random_element( p++ );
    }

    // traverse input data and maintain top-k in min-heap
    p = source_data;
    for( int i=0; i<N; i++ ) {
      if( imin_heap->Length( min_heap ) < k ) {
        TEST_ASSERTION( imin_heap->HeapPush( min_heap, p ) == 1,  "pushed element to heap" );
        TEST_ASSERTION( imin_heap->HeapTop( min_heap, &vmin ) == 1, "extracted top of heap" );
      }
      else if( __compare_elements_default( p, &vmin ) > 0 ) {
        TEST_ASSERTION( imin_heap->HeapReplace( min_heap, NULL, p ) == 0, "replaced larger value into heap" );
        TEST_ASSERTION( imin_heap->HeapTop( min_heap, &vmin ) == 1, "extracted top of heap" );
      }
      p++;
    }

    // sort test data (ascending, largest at the end)
    qsort( source_data, N, sizeof(_CSEQ_ELEMENT_TYPE), (f_compare_t)__compare_elements_default );

    // validate output against top k in sorted test data
    _CSEQ_ELEMENT_TYPE heap_elem;
    _CSEQ_ELEMENT_TYPE source_elem;
    for( int i=0; i<k; i++ ) {
      source_elem = source_data[N-k+i];
      TEST_ASSERTION( imin_heap->HeapPop( min_heap, &heap_elem ) == 1,      "popped top element from min-heap (smallest)" );
      TEST_ASSERTION( __eq_element( &source_elem, &heap_elem ),             "heap element in correct order" );
    }

    ALIGNED_FREE( source_data );
    source_data = NULL;

  } END_TEST_SCENARIO




  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy heaps" ) {
    COMLIB_OBJECT_DESTROY( min_heap );
    COMLIB_OBJECT_DESTROY( max_heap );
  } END_TEST_SCENARIO

} END_UNIT_TEST

#endif
