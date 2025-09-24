/*
 * _comlibsequence_tptr_ops.h
 *
 *
*/
#ifndef _COMLIBSEQUENCE_TPTR_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_TPTR_OPS_H_INCLUDED


__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  TPTR_COPY( dest, src );
}

__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, uintptr_t p ) {
  TPTR_AS_QWORD( e ) = p;
}


__inline static uintptr_t __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return (uintptr_t)TPTR_AS_QWORD( e );
}


__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "tptr_t @ %p\n", e );
  printf( "  QWORD : %016llX\n", TPTR_AS_QWORD( e ) );
}


__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  TPTR_AS_QWORD( dest ) = rand64(); // <- scary :-)  just for testing of course
}


__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return TPTR_MATCH( e1, e2 );
}


__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  TPTR_INIT( e );
}


__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  return TPTR_IS_NULL( e );
}


__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  TPTR_SWAP( e1, e2 );
  return e1;
}


__inline static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD q1 = TPTR_AS_QWORD( e1 );
  QWORD q2 = TPTR_AS_QWORD( e2 );
  return ( q1 > q2 ) - ( q1 < q2 ); 
}




#endif






