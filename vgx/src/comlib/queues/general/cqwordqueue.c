/*
 * cqwordqueue.c
 *
 *
*/

/*******************************************************************//**
 * CQwordQueue_t
 * 
 ***********************************************************************
 */




#define _CSEQ_IMPLEMENTATION_GENERIC_QUEUE

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          1
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_CQwordQueue_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_CQwordQueue_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_CQwordQueue_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_CQwordQueue_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_CQwordQueue_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_CQwordQueue_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_CQwordQueue_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 9    // 2**9 = 512 QWORDS = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     3    // 2**3 = 8 QWORDS = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     29   // 2**29 QWORDS = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_code.h"


static int s = sizeof(CQwordQueue_t);

/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_EXPORT CQwordQueue_t * CQwordQueueNew( int64_t initial_capacity ) {
  CQwordQueue_constructor_args_t args = {
    .comparator       = NULL,
    .element_capacity = initial_capacity
  };
  CQwordQueue_t *Q = COMLIB_OBJECT_NEW( CQwordQueue_t, NULL, &args );
  return Q;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_EXPORT CQwordQueue_t * CQwordQueueNewOutput( int64_t initial_capacity, const char *path ) {
  CQwordQueue_t *Q = CQwordQueueNew( initial_capacity );
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
DLL_EXPORT CQwordQueue_t * CQwordQueueNewInput( int64_t initial_capacity, const char *path ) {
  CQwordQueue_t *Q = CQwordQueueNew( initial_capacity );
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

test_descriptor_t _comlib_cqwordqueue_tests[] = {
  { "CQwordQueue_t Basic Tests",     __utest_basic_CQwordQueue_t },
  { "CQwordQueue_t Heap Tests",      __utest_heap_CQwordQueue_t },
  {NULL}
};

