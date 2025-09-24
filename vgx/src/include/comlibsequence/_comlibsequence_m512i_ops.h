/*
 * _comlibsequence_m512i_ops.h
 *
 *
*/
#ifndef _COMLIBSEQUENCE_M512I_OPS_H_INCLUDED
#define _COMLIBSEQUENCE_M512I_OPS_H_INCLUDED



__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  QWORD *d = (QWORD*)dest;
  QWORD *s = (QWORD*)src;
  d[0] = s[0];
  d[1] = s[1];
  d[2] = s[2];
  d[3] = s[3];
  d[4] = s[4];
  d[5] = s[5];
  d[6] = s[6];
  d[7] = s[7];
}

__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, QWORD v ) {
  QWORD *q = (QWORD*)e;
  q[0] = q[1] = q[2] = q[3] = q[4] = q[5] = q[6] = q[7] = v;
}

__inline static QWORD __get_element_int( _CSEQ_ELEMENT_TYPE *e ) {
  return *((QWORD*)e);
}


__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  printf( "m512i @ %p\n", e );
  printf( "  0: %016llX\n", ((QWORD*)e)[0] );
  printf( "  1: %016llX\n", ((QWORD*)e)[1] );
  printf( "  2: %016llX\n", ((QWORD*)e)[2] );
  printf( "  3: %016llX\n", ((QWORD*)e)[3] );
  printf( "  4: %016llX\n", ((QWORD*)e)[4] );
  printf( "  5: %016llX\n", ((QWORD*)e)[5] );
  printf( "  6: %016llX\n", ((QWORD*)e)[6] );
  printf( "  7: %016llX\n", ((QWORD*)e)[7] );
}


__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  QWORD *q = (QWORD*)dest;
  q[0] = rand64();
  q[1] = rand64();
  q[2] = rand64();
  q[3] = rand64();
  q[4] = rand64();
  q[5] = rand64();
  q[6] = rand64();
  q[7] = rand64();
}

__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD *a = (QWORD*)e1;
  QWORD *b = (QWORD*)e2;
  return a[0] == b[0] && a[1] == b[1] && a[2] == b[2] && a[3] == b[3] && a[4] == b[4] && a[5] == b[5] && a[6] == b[6] && a[7] == b[7];
}



__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) {
  QWORD *q = (QWORD*)e;
  q[0] = q[1] = q[2] = q[3] = q[4] = q[5] = q[6] = q[7] = 0;
}



__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) {
  QWORD *q = (QWORD*)e;
  return q[0] == 0 && q[1] == 0 && q[2] == 0 && q[3] == 0 && q[4] == 0 && q[5] == 0 && q[6] == 0 && q[7] == 0;
}


__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  __m256i tmp0 = *(e1->m256i);
  __m256i tmp1 = *(e1->m256i+1);
  *(e1->m256i) = *(e2->m256i);
  *(e1->m256i+1) = *(e2->m256i+1);
  *(e2->m256i) = tmp0;
  *(e2->m256i+1) = tmp1;
  return e1;
}


__inline static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  QWORD *a = (QWORD*)e1;
  QWORD *b = (QWORD*)e2;
  int c;
  if(      (c = (a[7] > b[7]) - (a[7] < b[7])) != 0 ) { return c; }
  else if( (c = (a[6] > b[6]) - (a[6] < b[6])) != 0 ) { return c; }
  else if( (c = (a[5] > b[5]) - (a[5] < b[5])) != 0 ) { return c; }
  else if( (c = (a[4] > b[4]) - (a[4] < b[4])) != 0 ) { return c; }
  else if( (c = (a[3] > b[3]) - (a[3] < b[3])) != 0 ) { return c; }
  else if( (c = (a[2] > b[2]) - (a[2] < b[2])) != 0 ) { return c; }
  else if( (c = (a[1] > b[1]) - (a[1] < b[1])) != 0 ) { return c; }
  else                                                { return (a[0] > b[0]) - (a[0] < b[0]); }
}



#endif






