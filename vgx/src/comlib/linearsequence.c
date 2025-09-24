/*
 * linearsequence.c
 *
 *
*/

/*******************************************************************//**
 * ComlibLinearSequence_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_BASE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          1
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_ComlibLinearSequence_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_ComlibLinearSequence_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_ComlibLinearSequence_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_ComlibLinearSequence_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_ComlibLinearSequence_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_ComlibLinearSequence_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_ComlibLinearSequence_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 9    // 2**9 = 512 QWORDS = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     3    // 2**3 = 8 QWORDS = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     29   // 2**29 QWORDS = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_code.h"

