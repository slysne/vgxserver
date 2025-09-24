/*
 * cdwordqueue.c
 *
 *
*/

/*******************************************************************//**
 * CDwordQueue_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          1
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_CDwordQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_CDwordQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_CDwordQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_CDwordQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_CDwordQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_CDwordQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_CDwordQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 10   // 2**10 = 1024 DWORDS = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     4    // 2**4 = 16 DWORDS = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     30   // 2**30 DWORDS = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_code.h"


#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

static int s = sizeof(CDwordQueue_t);

test_descriptor_t _comlib_cdwordqueue_tests[] = {
  { "CDwordQueue_t Basic Tests",     __utest_basic_CDwordQueue_t },
  { "CDwordQueue_t Heap Tests",      __utest_heap_CDwordQueue_t },
  {NULL}
};

