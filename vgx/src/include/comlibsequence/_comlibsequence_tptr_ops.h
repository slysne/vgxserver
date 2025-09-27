/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    _comlibsequence_tptr_ops.h
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

#ifndef _COMLIBSEQUENCE_TPTR_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_TPTR_OPS_H_INCLUDED



/**************************************************************************//**
 * __copy_element
 *
 ******************************************************************************
 */
__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  TPTR_COPY( dest, src );
}


/**************************************************************************//**
 * __set_element_int
 *
 ******************************************************************************
 */
__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, uintptr_t p ) {
  TPTR_AS_QWORD( e ) = p;
}



/**************************************************************************//**
 * __get_element_int
 *
 ******************************************************************************
 */
__inline static uintptr_t __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return (uintptr_t)TPTR_AS_QWORD( e );
}



/**************************************************************************//**
 * __print_element
 *
 ******************************************************************************
 */
__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "tptr_t @ %p\n", e );
  printf( "  QWORD : %016llX\n", TPTR_AS_QWORD( e ) );
}



/**************************************************************************//**
 * __random_element
 *
 ******************************************************************************
 */
__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  TPTR_AS_QWORD( dest ) = rand64(); // <- scary :-)  just for testing of course
}



/**************************************************************************//**
 * __eq_element
 *
 ******************************************************************************
 */
__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return TPTR_MATCH( e1, e2 );
}



/**************************************************************************//**
 * __set_zero_element
 *
 ******************************************************************************
 */
__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  TPTR_INIT( e );
}



/**************************************************************************//**
 * __is_zero_element
 *
 ******************************************************************************
 */
__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  return TPTR_IS_NULL( e );
}



/**************************************************************************//**
 * __swap_elements
 *
 ******************************************************************************
 */
__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  TPTR_SWAP( e1, e2 );
  return e1;
}



/**************************************************************************//**
 * __compare_elements_default
 *
 ******************************************************************************
 */
__inline static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD q1 = TPTR_AS_QWORD( e1 );
  QWORD q2 = TPTR_AS_QWORD( e2 );
  return ( q1 > q2 ) - ( q1 < q2 ); 
}




#endif
