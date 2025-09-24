/*
 * cm512iheap.c
 *
 *
*/

/*******************************************************************//**
 * Cm512iHeap_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_HEAP

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cm512iHeap_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cm512iHeap_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cm512iHeap_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cm512iHeap_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cm512iHeap_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cm512iHeap_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cm512iHeap_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 6    // 2**6 = 64 __m512i = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     0    // 2**0 = 0 __m512i = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     26   // 2**26 __m512i = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_m512i_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(Cm512iHeap_t);

#include "comlibsequence/__utest_comlibsequence_heap.h"
