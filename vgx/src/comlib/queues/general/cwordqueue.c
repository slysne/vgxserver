/*
 * cwordqueue.c
 *
 *
*/

/*******************************************************************//**
 * CWordQueue_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          1
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_CWordQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_CWordQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_CWordQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_CWordQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_CWordQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_CWordQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_CWordQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 11   // 2**11 = 2048 WORDS = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     5    // 2**5 = 32 WORDS = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     31   // 2**31 WORDS = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(CWordQueue_t);

#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

test_descriptor_t _comlib_cwordqueue_tests[] = {
  { "CWordQueue_t Basic Tests",     __utest_basic_CWordQueue_t },
  { "CWordQueue_t Heap Tests",      __utest_heap_CWordQueue_t },
  {NULL}
};

