/*
 * cbytequeue.c
 *
 *
*/

/*******************************************************************//**
 * CByteQueue_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          1
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_CByteQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_CByteQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_CByteQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_CByteQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_CByteQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_CByteQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_CByteQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 12  // page size
#define _CSEQ_MIN_CAPACITY_EXP     6   // cache line size
#define _CSEQ_MAX_CAPACITY_EXP     32  // 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_code.h"


#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

static int s = sizeof(CByteQueue_t);

test_descriptor_t _comlib_cbytequeue_tests[] = {
  { "CByteQueue_t Basic Tests",     __utest_basic_CByteQueue_t },
  { "CByteQueue_t Heap Tests",      __utest_heap_CByteQueue_t },
  {NULL}
};


