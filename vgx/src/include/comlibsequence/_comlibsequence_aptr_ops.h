/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    _comlibsequence_aptr_ops.h
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

#ifndef _COMLIBSEQUENCE_APTR_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_APTR_OPS_H_INCLUDED



/**************************************************************************//**
 * __copy_element
 *
 ******************************************************************************
 */
__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  APTR_COPY( dest, src );
}


/**************************************************************************//**
 * __set_element_int
 *
 ******************************************************************************
 */
__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, QWORD v ) {
  APTR_AS_ANNOTATION( e ) = v;
  APTR_AS_QWORD( e ) = v;
}



/**************************************************************************//**
 * __get_element_int
 *
 ******************************************************************************
 */
__inline static QWORD __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return APTR_AS_ANNOTATION( e );
}



/**************************************************************************//**
 * __print_element
 *
 ******************************************************************************
 */
__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "aptr_t @ %p\n", e );
  printf( "  ->ANNOTATION: %016llX\n", APTR_AS_ANNOTATION( e ) );
  printf( "  ->QWORD     : %016llX\n", APTR_AS_QWORD( e ) );
}



/**************************************************************************//**
 * __random_element
 *
 ******************************************************************************
 */
__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  APTR_AS_ANNOTATION( dest ) = rand64();
  APTR_AS_QWORD( dest ) = rand64(); // <- scary :-)  just for testing of course
}



/**************************************************************************//**
 * __eq_element
 *
 ******************************************************************************
 */
__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return APTR_MATCH( e1, e2 );
}



/**************************************************************************//**
 * __set_zero_element
 *
 ******************************************************************************
 */
__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  APTR_INIT( e );
}



/**************************************************************************//**
 * __is_zero_element
 *
 ******************************************************************************
 */
__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  return APTR_IS_NULL( e ) && APTR_AS_ANNOTATION(e) == 0;
}



/**************************************************************************//**
 * __swap_elements
 *
 ******************************************************************************
 */
__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  APTR_SWAP( e1, e2 );
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
  QWORD a1 = APTR_AS_ANNOTATION( e1 );
  QWORD a2 = APTR_AS_ANNOTATION( e2 );
  int c = (a1 > a2) - (a1 < a2);
  if( c != 0 ) { 
    // annotations differ, return 1 or -1
    return c; 
  }
  else {
    // annotations match, differentiate on address: 1, 0 or -1
    QWORD q1 = APTR_AS_QWORD( e1 );
    QWORD q2 = APTR_AS_QWORD( e2 );
    return ( q1 > q2 ) - ( q1 < q2 ); 
  }
}


#endif
