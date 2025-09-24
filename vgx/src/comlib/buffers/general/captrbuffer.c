/*
 * captrbuffer.c
 *
 *
*/

/*******************************************************************//**
 * CaptrBuffer_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_GENERIC_BUFFER

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_CaptrBuffer_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_CaptrBuffer_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_CaptrBuffer_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_CaptrBuffer_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_CaptrBuffer_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_CaptrBuffer_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_CaptrBuffer_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 8    // 2**8 = 256 aptr_t = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     2    // 2**2 = 4 aptr_t = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     28   // 2**28 aptr_t = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_aptr_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(CaptrBuffer_t);

#include "comlibsequence/__utest_comlibsequence_basic.h"

test_descriptor_t _comlib_captrbuffer_tests[] = {
  { "CaptrBuffer_t Basic Tests",     __utest_basic_CaptrBuffer_t },
  {NULL}
};


