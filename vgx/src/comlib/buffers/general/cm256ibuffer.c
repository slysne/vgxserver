/*
 * cm256ibuffer.c
 *
 *
*/

/*******************************************************************//**
 * Cm256iBuffer_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_BUFFER

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cm256iBuffer_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cm256iBuffer_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cm256iBuffer_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cm256iBuffer_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cm256iBuffer_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cm256iBuffer_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cm256iBuffer_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 7    // 2**7 = 128 __m256i = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     1    // 2**1 = 2 __m256i = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     27   // 2**27 __m256i = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_m256i_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(Cm256iBuffer_t);

#include "comlibsequence/__utest_comlibsequence_basic.h"

test_descriptor_t _comlib_cm256ibuffer_tests[] = {
  { "Cm256iBuffer_t Basic Tests",     __utest_basic_Cm256iBuffer_t },
  {NULL}
};


