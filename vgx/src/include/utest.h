/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    utest.h
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

#ifndef COMLIB_UTEST_H
#define COMLIB_UTEST_H

#include "debug.h"
#include "messages.h"

struct s_UnitTest;
struct s_UnitTestSet;
struct s_UnitTestSuite;

typedef struct s_test_failure_info_t {
  char reason[CXLIB_MSG_SIZE+1];
  char file_name[MAX_PATH+1];
  int line_number;
} test_failure_info_t;




#define BEGIN_UNIT_TEST( UnitTestName )               \
static int UnitTestName( UnitTest *__self, test_failure_info_t *__failure_info ) {  \
  IGNORE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED        \
  const char *TestName = #UnitTestName;               \
  int retcode = 0;                                    \
  UnitTest *__self__ = __self;                        \
  char __file__[MAX_PATH] = {'\0'};                   \
  int __line__ = 0;                                   \
  XTRY


#define END_UNIT_TEST \
  XCATCH( errcode ) {  \
    retcode = -1;     \
  }                   \
  XFINALLY {           \
  }                   \
  RESUME_WARNINGS     \
  return retcode;     \
}


#define TEST_ASSERTION( SuccessCondition, FailureMessage, ... )                           \
do {                                                                                      \
  SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT                                     \
  if( !(SuccessCondition) ) {                                                             \
    UnitTestFailed( __failure_info, __FILE__, __LINE__, FailureMessage, ##__VA_ARGS__ );  \
    THROW_ERROR( CXLIB_ERR_ASSERTION, 0 );                                                \
  }                                                                                       \
} WHILE_ZERO


#define NEXT_TEST_SCENARIO( Enabled, Description )        \
do {                                                      \
  bool __enabled__ = Enabled;                             \
  const char *__scenario__ = Description;                 \
  if( __enabled__ ) {                                     \
    UnitTestMessage( "Run scenario : %s", __scenario__ ); \
  } else {                                                \
    UnitTestMessage( "DISABLED     : %s", __scenario__ ); \
    __self__->_skipped_scenarios++;                       \
  }                                                       \
  if( __enabled__ ) {                                     \
    uint64_t t0 = __GET_CURRENT_MICROSECOND_TICK();       \
    uint64_t t1;                                          \
    do

#define END_TEST_SCENARIO                                 \
    WHILE_ZERO;                                           \
    t1 = __GET_CURRENT_MICROSECOND_TICK();                \
    UnitTestMessage( "*** PASS *** : %s (%.3f ms)",       \
              __scenario__, (double)(t1-t0)/1000.0f );    \
    __self__->_completed_scenarios++;                     \
  }                                                       \
} WHILE_ZERO;


typedef int (*test_procedure_t)( struct s_UnitTest *self, test_failure_info_t *__failure_info );


typedef struct s_test_descriptor_t {
  char *name;
  test_procedure_t procedure;
} test_descriptor_t;


typedef struct s_test_descriptor_set_t {
  const char *name;
  test_descriptor_t *test_descriptors;
} test_descriptor_set_t;



typedef struct s_UnitTest_vtable {
  int (*Run)( struct s_UnitTest *test );
  test_failure_info_t * (*Info)( struct s_UnitTest *test );
  uint64_t (*Duration)( struct s_UnitTest *test );
  int64_t (*Scenarios)( struct s_UnitTest *test );
  int64_t (*Skipped)( struct s_UnitTest *test );
  int64_t (*Completed)( struct s_UnitTest *test );
} UnitTest_vtable;


typedef struct s_UnitTestSet_vtable {
  void (*AddTest)( struct s_UnitTestSet *test_set, struct s_UnitTest *test );
  int (*Run)( struct s_UnitTestSet *test_set );
  uint64_t (*Duration)( struct s_UnitTestSet *test_set );
  int64_t (*Scenarios)( struct s_UnitTestSet *test_set );
  int64_t (*Skipped)( struct s_UnitTestSet *test_set );
  int64_t (*Completed)( struct s_UnitTestSet *test_set );
} UnitTestSet_vtable;


typedef struct s_UnitTestSuite_vtable {
  int (*ValidateTestNames)( struct s_UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *names[], const char **error );
  void (*AddTestSet)( struct s_UnitTestSuite *test_suite, struct s_UnitTestSet *test_set );
  int (*ExtendTestDescriptorSets)( struct s_UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *selected_tests[], const char **error );
  int (*Run)( struct s_UnitTestSuite *test_suite );
  uint64_t (*Duration)( struct s_UnitTestSuite *test_set_suite );
  int64_t (*Scenarios)( struct s_UnitTestSuite *test_suite );
  int64_t (*Skipped)( struct s_UnitTestSuite *test_suite );
  int64_t (*Completed)( struct s_UnitTestSuite *test_suite );
} UnitTestSuite_vtable;


typedef struct s_UnitTest {

  UnitTest_vtable *vtable;

  int _number;
  char *_name;

  int _completed_scenarios;
  int _skipped_scenarios;

  uint64_t t0;
  uint64_t t1;

  test_procedure_t _procedure;
  struct s_UnitTest *_prev_test;
  struct s_UnitTest *_next_test;

  test_failure_info_t *_info;


} UnitTest;


typedef struct s_UnitTestSet {

  UnitTestSet_vtable *vtable;

  int _number;
  char *_name;

  uint64_t t0;
  uint64_t t1;

  struct s_UnitTest *_head;
  struct s_UnitTest *_tail;
  struct s_UnitTestSet *_prev_set;
  struct s_UnitTestSet *_next_set;


} UnitTestSet;


typedef struct s_UnitTestSuite {

  UnitTestSuite_vtable *vtable;

  int _number;
  char *_name;

  uint64_t t0;
  uint64_t t1;

  struct s_UnitTestSet *_head;
  struct s_UnitTestSet *_tail;
  struct s_UnitTestSuite *_prev_suite;
  struct s_UnitTestSuite *_next_suite;



} UnitTestSuite;





DLL_COMLIB_PUBLIC extern int SetCurrentTestDirectory( const char *dirname );
DLL_COMLIB_PUBLIC extern const char * GetCurrentTestDirectory( void );

DLL_COMLIB_PUBLIC extern void UnitTestMessage( const char *format, ... );

DLL_COMLIB_PUBLIC extern test_failure_info_t * UnitTestFailed( test_failure_info_t *info, const char *file_name, int line_number, const char *format, ... );

DLL_COMLIB_PUBLIC extern UnitTest * NewUnitTest( const char *name, test_procedure_t procedure );
DLL_COMLIB_PUBLIC extern UnitTestSet * NewUnitTestSet( const char *name, test_descriptor_t *test_list );
DLL_COMLIB_PUBLIC extern UnitTestSuite * NewUnitTestSuite( const char *name );



DLL_COMLIB_PUBLIC extern void DeleteUnitTest( UnitTest **test );
DLL_COMLIB_PUBLIC extern void DeleteUnitTestSet( UnitTestSet **test_set );
DLL_COMLIB_PUBLIC extern void DeleteUnitTestSuite( UnitTestSuite **test_suite );

#endif
