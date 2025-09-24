/*
 * _comlibsequence_m128i_ops.h
 *
 *
*/
#ifndef _COMLIBSEQUENCE_M128I_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_M128I_OPS_H_INCLUDED



__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  QWORD *d = (QWORD*)dest;
  QWORD *s = (QWORD*)src;
  d[0] = s[0];
  d[1] = s[1];
}

__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, QWORD v ) {
  QWORD *q = (QWORD*)e;
  q[0] = q[1] = v;
}


__inline static QWORD __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return *((QWORD*)e);
}


__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "m128i @ %p\n", e );
  printf( "  0: %016llX\n", ((QWORD*)e)[0] );
  printf( "  1: %016llX\n", ((QWORD*)e)[1] );
}


__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  QWORD *q = (QWORD*)dest;
  q[0] = rand64();
  q[1] = rand64();
}


__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD *a = (QWORD*)e1;
  QWORD *b = (QWORD*)e2;
  return a[0] == b[0] && a[1] == b[1];
}


__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  QWORD *q = (QWORD*)e;
  q[0] = q[1] = 0;
}


__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  QWORD *q = (QWORD*)e;
  return q[0] == 0 && q[1] == 0;
}


__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  __m128i tmp = *e1;
  *e1 = *e2;
  *e2 = tmp;
  return e1;
}


__inline static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD *a = (QWORD*)e1;
  QWORD *b = (QWORD*)e2;
  int c;
  if( (c = (a[1] > b[1]) - (a[1] < b[1])) != 0 ) { return c; }
  else                                           { return (a[0] > b[0]) - (a[0] < b[0]); }
}





#endif






