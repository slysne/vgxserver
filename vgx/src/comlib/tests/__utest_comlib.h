/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    __utest_comlib.h
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

#ifndef __UTEST_COMLIB_H
#define __UTEST_COMLIB_H


#include "comlibsequence/__comlibtest_macro.h"


/*******************************************************************//**
 *
 ***********************************************************************
 */
BEGIN_UNIT_TEST( __utest_comlib_basic ) {
  
  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Factorial fac()" ) {
    TEST_ASSERTION( fac(0) == 1,          "0! == 1" );
    TEST_ASSERTION( fac(1) == 1,          "1! == 1" );
    TEST_ASSERTION( fac(2) == 1*2,        "2! == 2" );
    TEST_ASSERTION( fac(3) == 1*2*3,      "3! == 6" );
    TEST_ASSERTION( fac(4) == 1*2*3*4,    "4! == 24" );
    TEST_ASSERTION( fac(10) == 3628800,   "10! == 3628800" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Combinations comb()" ) {
    TEST_ASSERTION( comb(0,0) == 1,                 "Comb(0,0) == 1" );
    TEST_ASSERTION( comb(10,10) == 1,               "Comb(10,10) == 1" );
    TEST_ASSERTION( comb(8,3) == 56,                "Comb(8,3) == 56" );
    TEST_ASSERTION( comb(8,2) == 28,                "Comb(8,2) == 28" );
    TEST_ASSERTION( comb(6,3) == 20,                "Comb(6,3) == 20" );
    TEST_ASSERTION( comb(50,17) == comb(50,50-17),  "Comb(50,17) == Comb(50,50-17) == 9847379391150" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Integer to binary itob()" ) {
    TEST_ASSERTION( !strcmp( itob(0xffffffffffffffffull), "1111111111111111111111111111111111111111111111111111111111111111" ), "2**64-1 => all 1s" );
    TEST_ASSERTION( !strcmp( itob(0),                     "0000000000000000000000000000000000000000000000000000000000000000" ), "0 => all 0s" );
    TEST_ASSERTION( !strcmp( itob(0x80369f05cc305499ull), "1000000000110110100111110000010111001100001100000101010010011001" ), "something" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "String to int64 strtoint64" ) {
    int64_t value = 0;
    int64_t n;
    char buf[32];
    for( n=-500000; n<500000; n++ ) {
      sprintf( buf, "%lld", n );
      TEST_ASSERTION( strtoint64( buf, &value ) == 0,             "strtoint64(\"%s\") == 0", buf );
      TEST_ASSERTION( value == n,                                 "strtoint64(\"%s\") -> %lld, got %lld", buf, n, value );
    }

    const int64_t limits[] = {
      LLONG_MIN,
      LLONG_MIN+1,
      LLONG_MIN+2,
      LLONG_MAX,
      LLONG_MAX-1,
      LLONG_MAX-2,
      0
    };
    const int64_t *pn = limits;
    while( (n=*pn++) != 0 ) {
      sprintf( buf, "%lld", n );
      TEST_ASSERTION( strtoint64( buf, &value ) == 0,             "strtoint64(\"%s\") == 0", buf );
      TEST_ASSERTION( value == n,                                 "strtoint64(\"%s\") -> %lld, got %lld", buf, n, value );
    }

    const char *invalid[] = {
      "", " ", "-", "- ", " -", "1 ", " 1", "1 1", "1-1", "a", "1a", ":", "1:",
      "1234567-", "- 1234567", "1234567abc", "abc1234567", "123abc456",
      NULL
    };
    const char **p = invalid;
    const char *str;
    while( (str=*p++) != NULL ) {
      TEST_ASSERTION( strtoint64( str, &value ) < 0,              "strtoint64(\"%s\") is invalid", str );
    }
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "round()" ) {
    TEST_ASSERTION( (int)round(1.49999) == 1,       "round(1.49999) == 1" );
    TEST_ASSERTION( (int)round(0.5) == 1,           "round(0.5) == 1" );
    TEST_ASSERTION( (int)round(100.5) == 101,       "round(100.5) == 101" );
    TEST_ASSERTION( (int)round(100.4999) == 100,    "round(100.4999) == 100" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand64() uniformity" ) {
    double sum = 0.0;
    uint64_t u64_max = ULLONG_MAX;
    int64_t count = 1LL << 29;
    for( int64_t i=0; i<count; i++ ) {
      sum += rand64();
    }
    double avg = sum/count;
    double ratio = avg / u64_max; // ~0.5
    TEST_ASSERTION( (int64_t)round(100 * ratio) == 50 , "avg rand64() should be 63 bits got %g", ratio );
  } END_TEST_SCENARIO
    


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand63() uniformity" ) {
    double sum = 0.0;
    uint64_t u63_max = (1ULL<<63)-1;
    int64_t count = 1LL << 29;
    for( int64_t i=0; i<count; i++ ) {
      sum += rand63();
    }
    double avg = sum/count;
    double ratio = avg / u63_max; // ~0.5
    TEST_ASSERTION( (int64_t)round(100 * ratio) == 50 , "avg rand63() should be 62 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand32() uniformity" ) {
    int64_t sum = 0;
    uint32_t u32_max = CXLIB_ULONG_MAX;
    int64_t count = 1LL << 29;
    for( int64_t i=0; i<count; i++ ) {
      sum += rand32();
    }
    double avg = (double)sum/count;
    double ratio = avg / u32_max; // ~0.5
    TEST_ASSERTION( (int32_t)round(100 * ratio) == 50 , "avg rand32() should be 31 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand31() uniformity" ) {
    int64_t sum = 0;
    uint32_t u31_max = (1UL<<31)-1;
    int64_t count = 1LL << 29;
    for( int64_t i=0; i<count; i++ ) {
      sum += rand31();
    }
    double avg = (double)sum/count;
    double ratio = avg / u31_max; // ~0.5
    TEST_ASSERTION( (int32_t)round(100 * ratio) == 50 , "avg rand31() should be 30 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand26() uniformity" ) {
    int64_t sum = 0;
    uint32_t u26_max = (1L<<26)-1;
    int count = (int)u26_max;
    for( int i=0; i<count; i++ ) {
      sum += rand26();
    }
    double avg = (double)sum/count;
    double ratio = avg / u26_max; // ~0.5
    TEST_ASSERTION( (int32_t)round(100 * ratio) == 50 , "avg rand26() should be 25 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand18() uniformity" ) {
    int64_t sum = 0;
    uint32_t u18_max = (1L<<18)-1;
    int count = (int)u18_max;
    for( int i=0; i<count; i++ ) {
      sum += rand18();
    }
    double avg = (double)sum/count;
    double ratio = avg / u18_max; // ~0.5
    TEST_ASSERTION( (int32_t)round(100 * ratio) == 50 , "avg rand18() should be 17 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand16() uniformity" ) {
    int64_t sum = 0;
    uint16_t u16_max = (1<<16)-1;
    int count = (int)u16_max;
    for( int i=0; i<count; i++ ) {
      sum += rand16();
    }
    double avg = (double)sum/count;
    double ratio = avg / u16_max; // ~0.5
    TEST_ASSERTION( (int)round(100 * ratio) == 50 , "avg rand16() should be 15 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sanity check rand8() uniformity" ) {
    int64_t sum = 0;
    uint8_t u8_max = (1<<8)-1;
    int count = 100000;
    srand(123);
    for( int i=0; i<count; i++ ) {
      sum += rand8(); // not LFSR-based
    }
    double avg = (double)sum/count;
    double ratio = avg / u8_max; // ~0.5
    TEST_ASSERTION( (int)round(100 * ratio) == 50 , "avg rand8() should be 7 bits got %g", ratio );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sleep" ) {
    time_t ts0, ts1;
    time( &ts0 );
    for( int ms=1; ms<=1024; ms*=2 ) {
      sleep_milliseconds( ms );
    }
    time( &ts1 );
    TEST_ASSERTION( ts1-ts0 >= 2, "sleeps should add up to at least 2s, got %llus", ts1-ts0 );
  } END_TEST_SCENARIO

} END_UNIT_TEST



/*******************************************************************//**
 *
 ***********************************************************************
 */
BEGIN_UNIT_TEST( __utest_comlib_aptr ) {
  
  uint64_t value;
  uint64_t *ptr = &value;
  aptr_t aptr1 = {0}, aptr2 = {0};
  QWORD qw;

  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Assertion Test" ) {
    SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT
    TEST_ASSERTION( 1, "1 is true" );
    TEST_ASSERTION( true, "true is true" );
    TEST_ASSERTION( !false, "false is false" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "NULL Test" ) {
    *ptr = 0x123456789abcdef0;
    APTR_INIT( &aptr1 );
    TEST_ASSERTION( APTR_IS_NULL(&aptr1),                           "Initialized APTR is NULL" );
    TEST_ASSERTION( aptr1.tptr.qword == 0 && aptr1.annotation == 0, "APTR is initialized" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Assignment Test" ) {
    *ptr = 0x123456789abcdef0;
    APTR_SET_POINTER( &aptr1, ptr );
    TEST_ASSERTION( !APTR_IS_NULL(&aptr1),                                                "Assigned APTR is not NULL" );
    TEST_ASSERTION( (uint64_t*)APTR_GET_POINTER( &aptr1) == ptr && aptr1.annotation == 0, "APTR is assigned" );
    TEST_ASSERTION( APTR_IS_POINTER(&aptr1),                                              "APTR holds a proper pointer" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Tag Tests 1" ) {
    APTR_MAKE_VALID( &aptr1 );
    TEST_ASSERTION( APTR_IS_VALID(&aptr1) && APTR_AS_TAG(&aptr1) == TPTR_TAG_SINGLETON_POINTER ,    "Valid flag set" );
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == aptr1.tptr.tag.value,                                    "Consistent APTR tag macro" );
    TEST_ASSERTION( !APTR_IS_INVALID(&aptr1),                                                       "Terminal is not invalid" );
    TEST_ASSERTION( *((uint64_t*)APTR_GET_POINTER( &aptr1 )) == *ptr,                               "APTR pointer unaffected by tag" );
    APTR_MAKE_DIRTY( &aptr1 );
    TEST_ASSERTION( APTR_IS_DIRTY(&aptr1) && APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_POINTER,   "Terminal and Dirty flags are set" );
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == aptr1.tptr.tag.value,                                    "Consistent APTR tag macro" );
    TEST_ASSERTION( !APTR_IS_CLEAN(&aptr1),                                                         "Dirty is not clean" );
    TEST_ASSERTION( *((uint64_t*)APTR_GET_POINTER( &aptr1 )) == *ptr,                               "APTR pointer unaffected by tag" );
    APTR_MAKE_NONPTR(&aptr1);
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1) && APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_DATA,     "Terminal, Dirty and Nonptr flags are set" );
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == aptr1.tptr.tag.value,                                    "Consistent APTR tag macro" );
    TEST_ASSERTION( !APTR_IS_POINTER(&aptr1),                                                       "Nonptr is not pointer" );
    qw = APTR_AS_QWORD( &aptr1 );
    TEST_ASSERTION( (qw >> 3) << 3 == (uintptr_t)ptr,                                               "APTR data unaffected by tag, got 0x%0llx", APTR_AS_QWORD(&aptr1) );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Tag Tests 2" ) {
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_ARRAY_POINTER;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_ARRAY_POINTER,                                "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_POINTER(&aptr1) && APTR_IS_CLEAN(&aptr1) && APTR_IS_INVALID(&aptr1),  "APTR tag 0 0 0 => POINTER CLEAN INVALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_SINGLETON_POINTER;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_SINGLETON_POINTER,                            "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_POINTER(&aptr1) && APTR_IS_CLEAN(&aptr1) && APTR_IS_VALID(&aptr1),    "APTR tag 0 0 1 => POINTER CLEAN VALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_DIRTY_INVALID_POINTER;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_INVALID_POINTER,                        "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_POINTER(&aptr1) && APTR_IS_DIRTY(&aptr1) && APTR_IS_INVALID(&aptr1),  "APTR tag 0 1 0 => POINTER DIRTY INVALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_DIRTY_VALID_POINTER;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_POINTER,                          "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_POINTER(&aptr1) && APTR_IS_DIRTY(&aptr1) && APTR_IS_VALID(&aptr1),    "APTR tag 0 1 1 => POINTER DIRTY VALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_EMPTY;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_EMPTY,                                        "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1) && APTR_IS_CLEAN(&aptr1) && APTR_IS_INVALID(&aptr1),   "APTR tag 1 0 0 => NONPTR CLEAN INVALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_NON_POINTER_DATA;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_NON_POINTER_DATA,                             "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1) && APTR_IS_CLEAN(&aptr1) && APTR_IS_VALID(&aptr1),     "APTR tag 1 0 1 => NONPTR CLEAN VALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_DIRTY_INVALID_DATA;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_INVALID_DATA,                           "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1) && APTR_IS_DIRTY(&aptr1) && APTR_IS_INVALID(&aptr1),   "APTR tag 1 1 0 => NONPTR DIRTY INVALID" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_DIRTY_VALID_DATA;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_DATA,                             "APTR tag equal to set tag" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1) && APTR_IS_DIRTY(&aptr1) && APTR_IS_VALID(&aptr1),     "APTR tag 1 1 1 => NONPTR DIRTY VALID" );
    qw = APTR_AS_QWORD( &aptr1 );
    TEST_ASSERTION( (qw >> 3) << 3 == (uintptr_t)ptr,                                             "APTR data unaffected by tag operations" );
    APTR_MAKE_INVALID(&aptr1);
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_INVALID_DATA,                           "APTR tag back to nonterm" );
    APTR_MAKE_CLEAN(&aptr1);
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_EMPTY,                                        "APTR tag back to clean" );
    APTR_MAKE_POINTER(&aptr1);
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_ARRAY_POINTER,                                "APTR tag back to pointer" );
    TEST_ASSERTION( *((uint64_t*)APTR_GET_POINTER( &aptr1 )) == *ptr,                             "APTR pointer data unaffected by tag operations" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Field Independence" ) {
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_DIRTY_VALID_DATA;
    APTR_AS_ANNOTATION(&aptr1) = 1;
    qw = APTR_AS_QWORD( &aptr1 );
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_DATA && 
                    APTR_AS_ANNOTATION(&aptr1) == 1 && 
                    (qw >> 3) << 3 == (uintptr_t)ptr,                       "APTR fields are independent" );
    APTR_AS_ANNOTATION(&aptr1) = 0xFFFFFFFFFFFFFFFFull;
    qw = APTR_AS_QWORD( &aptr1 );
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_DIRTY_VALID_DATA &&
                    APTR_AS_ANNOTATION(&aptr1) == 0xFFFFFFFFFFFFFFFFull &&
                    (qw >> 3) << 3 == (uintptr_t)ptr,                       "APTR fields are independent" );
    APTR_AS_TAG( &aptr1 ) = TPTR_TAG_ARRAY_POINTER;
    TEST_ASSERTION( APTR_AS_TAG(&aptr1) == TPTR_TAG_ARRAY_POINTER &&
                    APTR_AS_ANNOTATION(&aptr1) == 0xFFFFFFFFFFFFFFFFull &&
                    *((uint64_t*)APTR_GET_POINTER( &aptr1 )) == *ptr,       "APTR fields are independent" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Integer Tests" ) {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostics push
#pragma GCC diagnostic ignored "-Woverflow"
#endif
    // 1 => 1
    APTR_INIT(&aptr1);
    APTR_SET_INTEGER(&aptr1, 1);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == 1,                           "APTR Integer value equal to value set" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // 2**55 - 1 => 2**55 - 1
    APTR_INIT(&aptr1);
    APTR_SET_INTEGER(&aptr1, (1LL<<55)-1);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == (1LL<<55)-1,                 "APTR Integer has 55 bits (plus positive sign bit)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // 2**55 => (-)2**55
    APTR_INIT(&aptr1);
    int64_t overflow = 1LL<<55;
    APTR_SET_INTEGER(&aptr1, overflow);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == -(1LL<<55),                  "APTR Integer becomes negative on overflow" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // -1 => -1
    APTR_INIT(&aptr1);
    APTR_SET_INTEGER(&aptr1, -1);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == -1,                          "APTR Integer can hold negative values" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // (-)2**55 => (-)2**55
    APTR_INIT(&aptr1);
    APTR_SET_INTEGER(&aptr1, -(1LL<<55));
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == -(1LL<<55),                  "APTR Integer has 55 bits (plus negative sign bit)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // (-)2**55 - 1 => 0
    APTR_INIT(&aptr1);
    int64_t underflow = -(1LL<<55) - 1;
    APTR_SET_INTEGER(&aptr1, underflow);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == (1LL<<55)-1,                 "APTR Integer wraps to max positive on underflow, got 0x%016llx", APTR_AS_INTEGER( &aptr1 ) );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );
    // (-)2**55 - 2 => 1
    APTR_INIT(&aptr1);
    int64_t underflow2 = -(1LL<<55) - 2;
    APTR_SET_INTEGER(&aptr1, underflow2);
    TEST_ASSERTION( APTR_AS_INTEGER( &aptr1 ) == (1LL<<55)-2,                 "APTR Integer wraps to positive on underflow, got 0x%016llx", APTR_AS_INTEGER( &aptr1 ) );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_INT56,              "APTR DTYPE is INT56" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Unsigned Tests" ) {
    // 1 => 1
    APTR_INIT(&aptr1);
    APTR_SET_UNSIGNED(&aptr1, 1);
    TEST_ASSERTION( APTR_AS_UNSIGNED( &aptr1 ) == 1,                          "APTR Unsigned value equal to value set" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_UINT56,             "APTR DTYPE is UINT56" );
    // 2**56 - 1 => 2**56 - 1
    APTR_INIT(&aptr1);
    APTR_SET_UNSIGNED(&aptr1, (1ULL<<56)-1);
    TEST_ASSERTION( APTR_AS_UNSIGNED( &aptr1 ) == (1ULL<<56)-1,               "APTR Unsigned can hold 56 bits" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_UINT56,             "APTR DTYPE is UINT56" );
    // 2**56 => 0
    APTR_INIT(&aptr1);
    uint64_t overflow = 1ULL<<56;
    APTR_SET_UNSIGNED(&aptr1, overflow);
    TEST_ASSERTION( APTR_AS_UNSIGNED( &aptr1 ) == 0,                          "APTR Unsigned can hold no more than 56 bits" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_UINT56,             "APTR DTYPE is UINT56" );
    // -1 => 2**56 - 1
    APTR_INIT(&aptr1);
    uint64_t underflow = (uint64_t)-1;
    SUPPRESS_WARNING_SIGNED_UNSIGNED_MISMATCH
    APTR_SET_UNSIGNED(&aptr1, underflow);
    TEST_ASSERTION( APTR_AS_UNSIGNED( &aptr1 ) == (1ULL<<56)-1,               "APTR Unsigned wraps when negative value set" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_UINT56,             "APTR DTYPE is UINT56" );
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostics pop
#endif
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Real Tests" ) {
    double f56_epsilon = 1.0 / (1LL<<44); // 44-bit coefficient, 11-bit exponent, 1-bit sign = 56 bits
#define __f56_eq( A, B ) (fabs( 1 - ( (A)/(B) ) ) < f56_epsilon)
    double avogadro = 6.02214085774e23;
    double planck = 6.62607004081e-34;
    // 1.0 => 1.0
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, 1.0);
    TEST_ASSERTION( APTR_GET_REAL( &aptr1 ) == 1.0,                           "APTR Real value equal to value set" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // 3.1415926 => ~3.1415926
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, M_PI);
    TEST_ASSERTION( fabs(APTR_GET_REAL( &aptr1 ) - M_PI) < f56_epsilon,       "APTR Real has 56-bit precision" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // -3.1415926 => ~-3.1415926
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, -M_PI);
    TEST_ASSERTION( fabs(APTR_GET_REAL( &aptr1 ) + M_PI) < f56_epsilon,       "APTR Real has 56-bit precision" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // large positive number
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, avogadro);
    TEST_ASSERTION( __f56_eq( APTR_GET_REAL( &aptr1), avogadro ),             "APTR Real has 56-bit precision (large positive number)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // small positive number
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, planck);
    TEST_ASSERTION( __f56_eq( APTR_GET_REAL( &aptr1 ), planck ),              "APTR Real has 56-bit precision (small positive number)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // large negative number
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, -avogadro);
    TEST_ASSERTION( __f56_eq( APTR_GET_REAL( &aptr1 ), -avogadro),            "APTR Real has 56-bit precision (large negative number)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
    // small negative number
    APTR_INIT(&aptr1);
    APTR_SET_REAL(&aptr1, -planck);
    TEST_ASSERTION( __f56_eq( APTR_GET_REAL( &aptr1 ), -planck),              "APTR Real has 56-bit precision (small negative number)" );
    TEST_ASSERTION( APTR_IS_NONPTR(&aptr1),                                   "APTR is a nonptr" );
    TEST_ASSERTION( APTR_AS_DTYPE(&aptr1) == TAGGED_DTYPE_REAL56,             "APTR DTYPE is REAL56" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "APTR Copy" ) {
    APTR_INIT(&aptr1);
    APTR_INIT(&aptr2);
    APTR_SET_POINTER( &aptr1, ptr );
    APTR_AS_ANNOTATION( &aptr1 ) = 1234;
    APTR_MAKE_DIRTY( &aptr1 );
    APTR_COPY( &aptr2, &aptr1 );
    TEST_ASSERTION( (uint64_t*)APTR_GET_POINTER( &aptr1 ) == (uint64_t*)APTR_GET_POINTER( &aptr2 ), "copied pointer should be the same" );
    TEST_ASSERTION( APTR_AS_ANNOTATION( &aptr1 ) == APTR_AS_ANNOTATION( &aptr2 ),                   "copied annotation should be the same" );
  } END_TEST_SCENARIO

} END_UNIT_TEST



#endif
