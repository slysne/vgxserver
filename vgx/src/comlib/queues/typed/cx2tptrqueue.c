/*
 * cx2tptrqueue.c
 *
 *
*/

/*******************************************************************//**
 * Cx2tptrQueue_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cx2tptrQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cx2tptrQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cx2tptrQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cx2tptrQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cx2tptrQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cx2tptrQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cx2tptrQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 8    // 2**8 = 256 x2tptr_t = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     2    // 2**2 = 4 x2tptr_t = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     28   // 2**28 x2tptr_t = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_x2tptr_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(Cx2tptrQueue_t);

#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

test_descriptor_t _comlib_cx2tptrqueue_tests[] = {
  { "Cx2tptrQueue Basic Tests",     __utest_basic_Cx2tptrQueue_t },
  { "Cx2tptrQueue_t Heap Tests",    __utest_heap_Cx2tptrQueue_t },
  {NULL}
};

