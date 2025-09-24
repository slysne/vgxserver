/*
 * cm512iqueue.c
 *
 *
*/

/*******************************************************************//**
 * Cm512iQueue_t
 * 
 ***********************************************************************
 */
#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cm512iQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cm512iQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cm512iQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cm512iQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cm512iQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cm512iQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cm512iQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 6    // 2**6 = 64 __m512i = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     0    // 2**0 = 1 __m512i = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     26   // 2**26 __m512i = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_m512i_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

static int s = sizeof(Cm512iQueue_t);

test_descriptor_t _comlib_cm512iqueue_tests[] = {
  { "Cm512iQueue Basic Tests",     __utest_basic_Cm512iQueue_t },
  { "Cm512iQueue_t Heap Tests",    __utest_heap_Cm512iQueue_t },
  {NULL}
};


