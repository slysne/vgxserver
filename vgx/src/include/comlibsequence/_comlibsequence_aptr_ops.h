/*
 * _comlibsequence_aptr_ops.h
 *
 *
*/
#ifndef _COMLIBSEQUENCE_APTR_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_APTR_OPS_H_INCLUDED


__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  APTR_COPY( dest, src );
}

__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, QWORD v ) {
  APTR_AS_ANNOTATION( e ) = v;
  APTR_AS_QWORD( e ) = v;
}


__inline static QWORD __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return APTR_AS_ANNOTATION( e );
}


__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "aptr_t @ %p\n", e );
  printf( "  ->ANNOTATION: %016llX\n", APTR_AS_ANNOTATION( e ) );
  printf( "  ->QWORD     : %016llX\n", APTR_AS_QWORD( e ) );
}


__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  APTR_AS_ANNOTATION( dest ) = rand64();
  APTR_AS_QWORD( dest ) = rand64(); // <- scary :-)  just for testing of course
}


__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return APTR_MATCH( e1, e2 );
}


__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  APTR_INIT( e );
}


__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  return APTR_IS_NULL( e ) && APTR_AS_ANNOTATION(e) == 0;
}


__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  APTR_SWAP( e1, e2 );
  return e1;
}


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






