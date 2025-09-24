/*######################################################################
 *#
 *# __utest_comlibsequence_basic.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_COMLIBSEQUENCE_BASIC_H
#define __UTEST_COMLIBSEQUENCE_BASIC_H

#include "__comlibtest_macro.h"


#define BASIC_TEST_NAME( type ) __utest_basic_##type
#define X_BASIC_TEST_NAME( type ) BASIC_TEST_NAME( type )


BEGIN_UNIT_TEST( X_BASIC_TEST_NAME( _CSEQ_TYPENAME ) ) {
  
  _CSEQ_TYPENAME *seq = NULL;
  _CSEQ_VTABLETYPE *iSeq = NULL;

  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and destroy sequence" ) {
    _CSEQ_CONSTRUCTOR_ARGSTYPE args = { 0 };
    args.element_capacity = 1;
    seq = COMLIB_OBJECT_NEW( _CSEQ_TYPENAME, NULL, &args );
    TEST_ASSERTION( seq, "sequence created" );
    COMLIB_OBJECT_DESTROY( seq );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create sequence" ) {
    _CSEQ_CONSTRUCTOR_ARGSTYPE args = { 0 };
    args.element_capacity = 1;
    seq = COMLIB_OBJECT_NEW( _CSEQ_TYPENAME, NULL, &args );
    TEST_ASSERTION( seq, "sequence created" );
    iSeq = CALLABLE(seq);
    TEST_ASSERTION( iSeq, "sequence interface exists" );
    TEST_ASSERTION( iSeq->Capacity( seq ) == (1 << _CSEQ_MIN_CAPACITY_EXP), "sequence has minimum capacity" );
  } END_TEST_SCENARIO


#if defined _CSEQ_CIRCULAR_BUFFER
  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Circular basic" ) {
    int64_t capacity = iSeq->Capacity( seq );
    _CSEQ_ELEMENT_TYPE w, r, e;
    _CSEQ_ELEMENT_TYPE *p;
    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "circular sequence empty before start" );

    __set_element_int( &w, 1 );

    for( int64_t i=0; i<capacity; i++ ) {
      TEST_ASSERTION( iSeq->Write( seq, &w, 1 ) == 1,               "write 1 element" );
      __set_element_int( &w, __get_element_int( &w ) + 1 );
    }
    TEST_ASSERTION( iSeq->Length( seq ) == iSeq->Capacity( seq ),       "circular sequence is full" );
    p = &r;
    __set_element_int( &e, 1 );
    TEST_ASSERTION( iSeq->Read( seq, (void**)&p, 1 ) == 1,          "read 1 element" );
    if( !__eq_element( &e, &r ) ) {
      __print_element( &e );
      __print_element( &r );
    }
    TEST_ASSERTION( __eq_element( &e, &r ),                         "correct element read" );
    __set_element_int( &e, __get_element_int( &e ) + 1 );

    int64_t pre_capacity = iSeq->Capacity( seq );
    TEST_ASSERTION( iSeq->Length( seq ) == pre_capacity - 1,        "circular sequence has one slot free" );
    TEST_ASSERTION( iSeq->Write( seq, &w, 1 ) == 1,                 "write 1 element" );

#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
    int64_t post_capacity = iSeq->Capacity( seq );
    TEST_ASSERTION( post_capacity > pre_capacity,                   "capacity expansion due to unread buffer" );
#else
    TEST_ASSERTION( iSeq->Length( seq ) == iSeq->Capacity( seq ),   "circular sequence is full" );
#endif

    for( int64_t i=0; i<capacity; i++ ) {
      TEST_ASSERTION( iSeq->Read( seq, (void**)&p, 1 ) == 1,        "read 1 element" );
      TEST_ASSERTION( __eq_element( &e, &r ),                       "correct element read" );
      __set_element_int( &e, __get_element_int( &e ) + 1 );
    }
    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "circular sequence is empty" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Write/Read multiple" ) {
    _CSEQ_ELEMENT_TYPE *data;
    CALIGNED_ARRAY( data, _CSEQ_ELEMENT_TYPE, 4 );
    TEST_ASSERTION( data, "test data allocated" );
    for( uint8_t i=0; i<4; i++ ) {
      __set_element_int( &data[i], i );
    }

    _CSEQ_ELEMENT_TYPE *buf = NULL;
    _CSEQ_ELEMENT_TYPE *buf2;

    TEST_ASSERTION( iSeq->Write( seq, data, 4 ) == 4,               "write 4 elements" );
    TEST_ASSERTION( iSeq->Length( seq ) == 4,                       "sequence has 4 elements" );
    TEST_ASSERTION( iSeq->Read( seq, (void**)&buf, 2 ) == 2 && buf != NULL, "read 2 elements into auto allocated destination" );
    TEST_ASSERTION( __eq_element( &data[0], &buf[0] ),          "element match" );
    TEST_ASSERTION( __eq_element( &data[1], &buf[1] ),          "element match" );

    ALIGNED_FREE( buf );

    CALIGNED_ARRAY( buf, _CSEQ_ELEMENT_TYPE, 2 );
    TEST_ASSERTION( (buf2 = buf) != NULL,                       "destination allocated" );
    TEST_ASSERTION( iSeq->Read( seq, (void**)&buf, 2 ) == 2 && buf == buf2, "read 2 elements into supplied destination" );
    TEST_ASSERTION( __eq_element( &data[2], &buf[0] ),          "element match" );
    TEST_ASSERTION( __eq_element( &data[3], &buf[1] ),          "element match" );
    ALIGNED_FREE( buf );
    buf2 = buf = NULL;

    ALIGNED_FREE( data );
  } END_TEST_SCENARIO

#endif


#if defined _CSEQ_LINEAR_BUFFER
  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Linear basic" ) {
    int64_t capacity = iSeq->Capacity( seq );
    _CSEQ_ELEMENT_TYPE w;
    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "linear sequence empty before start" );

    __set_element_int( &w, 1 );

    for( int64_t i=0; i<capacity; i++ ) {
      TEST_ASSERTION( iSeq->Append( seq, &w ) == 1,                 "write 1 element" );
      __set_element_int( &w, __get_element_int( &w ) + 1 );
    }
    TEST_ASSERTION( iSeq->Length( seq ) == iSeq->Capacity( seq ),   "linear sequence is full" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Extend/Discard multiple" ) {
    _CSEQ_ELEMENT_TYPE *data;
    CALIGNED_ARRAY( data, _CSEQ_ELEMENT_TYPE, 4 );
    TEST_ASSERTION( data, "test data allocated" );
    for( uint8_t i=0; i<4; i++ ) {
      __set_element_int( &data[i], i );
    }

    _CSEQ_ELEMENT_TYPE *buf = NULL;

    TEST_ASSERTION( iSeq->Extend( seq, data, 4 ) == 4,              "write 4 elements" );
    TEST_ASSERTION( iSeq->Length( seq ) == 4,                       "sequence has 4 elements" );

    // Check elements 0 and 1
    TEST_ASSERTION( iSeq->Get( seq, 0, buf ) == 1 && __eq_element( &data[0], buf ),   "element 0 should match data[0]" );
    TEST_ASSERTION( iSeq->Get( seq, 1, buf ) == 1 && __eq_element( &data[1], buf ),   "element 1 should match data[1]" );
    // Discard elements 0 and 1
    iSeq->Discard( seq, 2 );

    // Check remaining elements
    TEST_ASSERTION( iSeq->Get( seq, 0, buf ) == 1 && __eq_element( &data[2], buf ),   "element 0 should match data[2]" );
    TEST_ASSERTION( iSeq->Get( seq, 1, buf ) == 1 && __eq_element( &data[3], buf ),   "element 1 should match data[3]" );
  } END_TEST_SCENARIO

#endif


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Expansion" ) {
    int64_t capacity = iSeq->Capacity( seq );
    int test_sz = 1000000;
    //_CSEQ_ELEMENT_TYPE *data = malloc( test_sz * sizeof( _CSEQ_ELEMENT_TYPE ) );
    _CSEQ_ELEMENT_TYPE *data = NULL;
    TALIGNED_ARRAY( data, _CSEQ_ELEMENT_TYPE, test_sz );
    TEST_ASSERTION( data, "malloc()" );
    _CSEQ_ELEMENT_TYPE c;
    _CSEQ_ELEMENT_TYPE *r;

    // populate test data with random elements
    for( int i=0; i<test_sz; i++ ) {
      __set_element_int( &data[i], (char)rand32() ); // smallest type so we don't get warnings
    }

    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "sequence is empty" );

    // Write 1 element at a time
    for( int i=0; i<test_sz; i++ ) {
#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Write( seq, &data[i], 1 ) == 1,         "write 1 element, expand as needed" );
#elif defined _CSEQ_LINEAR_BUFFER
      TEST_ASSERTION( iSeq->Append( seq, &data[i]) == 1,            "append 1 element, expand as needed" );
#endif
    }

    TEST_ASSERTION( iSeq->Length( seq ) == test_sz,                 "all data in sequence" );
    TEST_ASSERTION( iSeq->Capacity( seq ) >= test_sz,               "capacity increased" );

    r = &c;
    for( int i=0; i<test_sz; i++ ) {
#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Read( seq, (void**)&r, 1 ) == 1,        "read 1 element" );
#elif defined _CSEQ_LINEAR_BUFFER
      TEST_ASSERTION( iSeq->Get( seq, 0, r ) == 1,                  "get element at index 0" );
      iSeq->Discard( seq, 1 );
#endif
      TEST_ASSERTION( __eq_element( r, &data[i] ),                  "got the expected element" );
    }
    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "sequence empty" );

    //free( data );
    ALIGNED_FREE( data );
    data = NULL;

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy sequence" ) {
    COMLIB_OBJECT_DESTROY( seq );
    seq = NULL;
    iSeq = NULL;
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Expansion2" ) {
    _CSEQ_CONSTRUCTOR_ARGSTYPE args = { 0 };
    args.element_capacity = 1;
    seq = COMLIB_OBJECT_NEW( _CSEQ_TYPENAME, NULL, &args );
    iSeq = CALLABLE( seq );
    int test_sz = 10000;
    //_CSEQ_ELEMENT_TYPE *data = malloc( test_sz * sizeof( _CSEQ_ELEMENT_TYPE ) );
    _CSEQ_ELEMENT_TYPE *data = NULL;
    TALIGNED_ARRAY( data, _CSEQ_ELEMENT_TYPE, test_sz );
    TEST_ASSERTION( data, "malloc()" );
    _CSEQ_ELEMENT_TYPE c;
    _CSEQ_ELEMENT_TYPE *r = NULL;

    // populate test data with random elements
    for( int i=0; i<test_sz; i++ ) {
      __set_element_int( &data[i], (char)rand32() ); // smallest type so we don't get warnings
    }

    // Make sure sequence is empty
    TEST_ASSERTION( iSeq->Length( seq ) == 0,                       "sequence is empty" );

    // Try to read empty sequence
#if defined _CSEQ_CIRCULAR_BUFFER
    TEST_ASSERTION( iSeq->Read( seq, (void**)&r, -1 ) == 0,         "nothing read from empty sequence" );
    TEST_ASSERTION( r == NULL,                                      "no return buffer allocated" );
#elif defined _CSEQ_LINEAR_BUFFER
    TEST_ASSERTION( iSeq->Get( seq, 0, r ) < 0,                     "nothing at index 0 in empty sequence" );
#endif

    // Write and read larger and larger portions of test data
    for( int i=0; i<test_sz; i++ ) {

#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Write( seq, data, i ) == i,             "write many elements" );
#elif defined _CSEQ_LINEAR_BUFFER
      TEST_ASSERTION( iSeq->Extend( seq, data, i ) == i,            "write many elements" );
#endif
      TEST_ASSERTION( iSeq->Length( seq ) == i,                     "size should equal written elements" );
      TEST_ASSERTION( iSeq->Capacity( seq ) >= i,                   "capacity should be at least the size" );

#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Read( seq, (void**)&r, -1 ) == i,       "read back all written elements" );
#elif defined _CSEQ_LINEAR_BUFFER
      r = iSeq->Cursor( seq, 0 );
      iSeq->Discard( seq, i );
#endif
      TEST_ASSERTION( iSeq->Length( seq ) == 0,                     "size should be zero" );
      TEST_ASSERTION( memcmp( data, r, i*sizeof( _CSEQ_ELEMENT_TYPE ) ) == 0, "read data matches written data" );

#if defined _CSEQ_CIRCULAR_BUFFER
      ALIGNED_FREE( r );
      r = NULL;
#endif
    }

    // Write many and read one by one larger and larger portions of test data
    r = &c;
    for( int i=0; i<test_sz; i++ ) {

#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Write( seq, data, i ) == i,             "write many elements" );
#elif defined _CSEQ_LINEAR_BUFFER
      TEST_ASSERTION( iSeq->Extend( seq, data, i ) == i,            "write many elements" );
#endif
      TEST_ASSERTION( iSeq->Length( seq ) == i,                     "size should equal written elements" );
      TEST_ASSERTION( iSeq->Capacity( seq ) >= i,                   "capacity should be at least the size" );

      for( int n=0; n<i; n++ ) {
#if defined _CSEQ_CIRCULAR_BUFFER
        TEST_ASSERTION( iSeq->Read( seq, (void**)&r, 1 ) == 1,      "read back one element" );
#elif defined _CSEQ_LINEAR_BUFFER
        TEST_ASSERTION( iSeq->Get( seq, 0, &c ) == 1,                 "get element at index 0" );
        iSeq->Discard( seq, 1 );
#endif
        TEST_ASSERTION( __eq_element( &c, &data[n] ),               "got the expected element" );
      }

#if defined _CSEQ_CIRCULAR_BUFFER
      TEST_ASSERTION( iSeq->Read( seq, (void**)&r, 1 ) == 0,        "nothing more to read" );
      TEST_ASSERTION( iSeq->Read( seq, (void**)&r, -1 ) == 0,       "nothing more to read" );
#elif defined _CSEQ_LINEAR_BUFFER
      TEST_ASSERTION( iSeq->Get( seq, 0, &c ) < 0,                  "no more elements" );
#endif

      TEST_ASSERTION( iSeq->Length( seq ) == 0,                     "size should be zero" );
    }

    //free( data );
    ALIGNED_FREE(data);
    data = NULL;

    COMLIB_OBJECT_DESTROY( seq );

  } END_TEST_SCENARIO



  //
  // TODO: A LOT MORE TEST CASES NEEDED
  //
  //



} END_UNIT_TEST


#endif
