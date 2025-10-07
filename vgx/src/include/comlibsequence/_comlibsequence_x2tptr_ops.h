/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    _comlibsequence_x2tptr_ops.h
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

#ifndef _COMLIBSEQUENCE_X2TPTR_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_X2TPTR_OPS_H_INCLUDED



/**************************************************************************//**
 * __copy_element
 *
 ******************************************************************************
 */
__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  TPTR_COPY( &dest->t_1, &src->t_1 );
  TPTR_COPY( &dest->t_2, &src->t_2 );
}


/**************************************************************************//**
 * __set_element_int
 *
 ******************************************************************************
 */
__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, uintptr_t p ) {
  TPTR_AS_QWORD( &e->t_1 ) = p;
  TPTR_AS_QWORD( &e->t_2 ) = p;
}



/**************************************************************************//**
 * __get_element_int
 *
 ******************************************************************************
 */
__inline static uintptr_t __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return (uintptr_t)TPTR_AS_QWORD( &e->t_1 );
}



/**************************************************************************//**
 * __print_element
 *
 ******************************************************************************
 */
__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "x2tptr_t @ %p\n", e );
  printf( "  ->t_1   : %016llX\n", TPTR_AS_QWORD( &e->t_1 ) );
  printf( "  ->t_2   : %016llX\n", TPTR_AS_QWORD( &e->t_2 ) );
}



/**************************************************************************//**
 * __random_element
 *
 ******************************************************************************
 */
__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  TPTR_AS_QWORD( &dest->t_1 ) = rand64(); // <- scary :-)  just for testing of course
  TPTR_AS_QWORD( &dest->t_2 ) = rand64();
}



/**************************************************************************//**
 * __eq_element
 *
 ******************************************************************************
 */
__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return TPTR_MATCH( &e1->t_1, &e2->t_1 ) && TPTR_MATCH( &e1->t_2, &e2->t_2 );
}



/**************************************************************************//**
 * __set_zero_element
 *
 ******************************************************************************
 */
__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  TPTR_INIT( &e->t_1 );
  TPTR_INIT( &e->t_2 );
}



/**************************************************************************//**
 * __is_zero_element
 *
 ******************************************************************************
 */
__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  return TPTR_IS_NULL( &e->t_1 ) && TPTR_IS_NULL( &e->t_2 );
}



/**************************************************************************//**
 * __swap_elements
 *
 ******************************************************************************
 */
__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  TPTR_SWAP( &e1->t_1, &e2->t_1 );
  TPTR_SWAP( &e1->t_2, &e2->t_2 );
  return e1;
}



/**************************************************************************//**
 * __compare_elements_default
 *
 ******************************************************************************
 */
__inline static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  // This probably doesn't make sense to generalize since aptr use cases are probably uniqe somehow, but
  // we have to define the default somehow: First annotations, then pointers
  QWORD q1_1 = TPTR_AS_QWORD( &e1->t_1 );
  QWORD q2_1 = TPTR_AS_QWORD( &e2->t_1 );
  int c = (q1_1 > q2_1) - (q1_1 < q2_1);
  if( c != 0 ) { 
    // tptr.t_1 differ, return 1 or -1
    return c; 
  }
  else {
    // annotations match, differentiate on address: 1, 0 or -1
    QWORD q1_2 = TPTR_AS_QWORD( &e1->t_2 );
    QWORD q2_2 = TPTR_AS_QWORD( &e2->t_2 );
    return ( q1_2 > q2_2 ) - ( q1_2 < q2_2 ); 
  }
}




#endif
