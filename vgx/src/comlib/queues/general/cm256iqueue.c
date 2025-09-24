/*
 * cm256iqueue.c
 *
 *
*/

/*******************************************************************//**
 * Cm256iQueue_t
 * 
 ***********************************************************************
 */
#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cm256iQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cm256iQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cm256iQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cm256iQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cm256iQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cm256iQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cm256iQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 7    // 2**7 = 128 __m256i = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     1    // 2**1 = 2 __m256i = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     27   // 2**27 __m256i = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_m256i_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(Cm256iQueue_t);

/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_EXPORT Cm256iQueue_t * Cm256iQueueNew( int64_t initial_capacity ) {
  Cm256iQueue_constructor_args_t args = {
    .comparator       = NULL,
    .element_capacity = initial_capacity
  };
  Cm256iQueue_t *Q = COMLIB_OBJECT_NEW( Cm256iQueue_t, NULL, &args );
  return Q;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_EXPORT Cm256iQueue_t * Cm256iQueueNewOutput( int64_t initial_capacity, const char *path ) {
  Cm256iQueue_t *Q = Cm256iQueueNew( initial_capacity );
  if( Q ) {
    if( CALLABLE(Q)->AttachOutputStream( Q, path ) < 0 ) {
      COMLIB_OBJECT_DESTROY( Q );
      Q = NULL;
    }
  }
  return Q;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_EXPORT Cm256iQueue_t * Cm256iQueueNewInput( int64_t initial_capacity, const char *path ) {
  Cm256iQueue_t *Q = Cm256iQueueNew( initial_capacity );
  if( Q ) {
    if( CALLABLE(Q)->AttachInputStream( Q, path ) < 0 ) {
      COMLIB_OBJECT_DESTROY( Q );
      Q = NULL;
    }
  }
  return Q;
}


#include "comlibsequence/__utest_comlibsequence_basic.h"
#include "comlibsequence/__utest_comlibsequence_heap.h"

test_descriptor_t _comlib_cm256iqueue_tests[] = {
  { "Cm256iQueue Basic Tests",     __utest_basic_Cm256iQueue_t },
  { "Cm256iQueue_t Heap Tests",    __utest_heap_Cm256iQueue_t },
  {NULL}
};


