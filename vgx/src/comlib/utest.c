/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    utest.c
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

#include "_comlib.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_UTEST );


#define TEST_FAILED( Test, Reason )   THROW_ERROR_MESSAGE( COMLIB_MSG_MOD_UTEST, 0, "Test %d:'%s' FAILED. Reason: %s", Test->_number, Test->_name, Reason )

// Current Test Directory
const char *g_current_test_dir = NULL;

// Current Test
const char *g_current_test_name = NULL;
const char *g_current_test_set_name = NULL;
int g_current_test_number = -1;
int g_current_test_set_number = -1;

// UnitTest
static void __UnitTest_delete( UnitTest **test );
static UnitTest * __UnitTest_new( const char *name, test_procedure_t procedure );
static int __UnitTest_run( UnitTest *test );
static test_failure_info_t * __UnitTest_info( UnitTest *test );
static uint64_t __UnitTest_duration( UnitTest *test );
static int64_t __UnitTest_scenarios( UnitTest *test );
static int64_t __UnitTest_skipped_scenarios( UnitTest *test );
static int64_t __UnitTest_completed_scenarios( UnitTest *test );

// UnitTestSet
static void __UnitTestSet_delete( UnitTestSet **test_set );
static UnitTestSet * __UnitTestSet_new( const char *name, test_descriptor_t *test_list );
static int __UnitTestSet_run( UnitTestSet *test_set );
static void __UnitTestSet_add_test( UnitTestSet *test_set, UnitTest *test );
static uint64_t __UnitTestSet_duration( UnitTestSet *test_set );
static int64_t __UnitTestSet_scenarios( UnitTestSet *test_set );
static int64_t __UnitTestSet_skipped_scenarios( UnitTestSet *test_set );
static int64_t __UnitTestSet_completed_scenarios( UnitTestSet *test_set );

// UnitTestSuite
static void __UnitTestSuite_delete( UnitTestSuite **test_suite );
static UnitTestSuite * __UnitTestSuite_new( const char *name );
static int __UnitTestSuite_validate_test_names( UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *names[], const char **error );
static int __UnitTestSuite_run( UnitTestSuite *test_suite );
static void __UnitTestSuite_add_test_set( UnitTestSuite *test_suite, UnitTestSet *test_set );
static int __UnitTestSuite_extend_test_descriptor_sets( UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *selected_tests[], const char **error );
static uint64_t __UnitTestSuite_duration( UnitTestSuite *test_suite );
static int64_t __UnitTestSuite_scenarios( UnitTestSuite *test_suite );
static int64_t __UnitTestSuite_skipped_scenarios( UnitTestSuite *test_suite );
static int64_t __UnitTestSuite_completed_scenarios( UnitTestSuite *test_suite );


static void __print( const char *format, ... );


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTest_vtable UnitTest_Methods = {
  .Run        = __UnitTest_run,
  .Info       = __UnitTest_info,
  .Duration   = __UnitTest_duration,
  .Scenarios  = __UnitTest_scenarios,
  .Skipped    = __UnitTest_skipped_scenarios,
  .Completed  = __UnitTest_completed_scenarios
};


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTestSet_vtable UnitTestSet_Methods = {
  .AddTest    = __UnitTestSet_add_test,
  .Run        = __UnitTestSet_run,
  .Duration   = __UnitTestSet_duration,
  .Scenarios  = __UnitTestSet_scenarios,
  .Skipped    = __UnitTestSet_skipped_scenarios,
  .Completed  = __UnitTestSet_completed_scenarios
};


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTestSuite_vtable UnitTestSuite_Methods = {
  .ValidateTestNames        = __UnitTestSuite_validate_test_names,
  .AddTestSet               = __UnitTestSuite_add_test_set,
  .ExtendTestDescriptorSets = __UnitTestSuite_extend_test_descriptor_sets,
  .Run                      = __UnitTestSuite_run,
  .Duration                 = __UnitTestSuite_duration,
  .Scenarios                = __UnitTestSuite_scenarios,
  .Skipped                  = __UnitTestSuite_skipped_scenarios,
  .Completed                = __UnitTestSuite_completed_scenarios
};


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __UnitTest_delete( UnitTest **test ) {
  if( test != NULL && *test != NULL ) {
    if( (*test)->_name ) {
      free( (*test)->_name );
    }
    if( (*test)->_info ) {
      free( (*test)->_info );
    }
    if( (*test)->_prev_test ) {
      (*test)->_prev_test->_next_test = (*test)->_next_test;
    }
    if( (*test)->_next_test ) {
      (*test)->_next_test->_prev_test = (*test)->_prev_test;
    }
    free( *test );
    *test = NULL;
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTest * __UnitTest_new( const char *name, test_procedure_t procedure ) {
  UnitTest *test = NULL;

  XTRY {
    size_t sz_name = strnlen( name, CXLIB_MSG_SIZE+1 );
    if( (test = (UnitTest*)calloc( 1, sizeof(UnitTest) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x101 );
    }
    if( (test->_name = (char*)calloc( sz_name+1, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x102 );
    }
    test->_number = -1;
    strncpy( test->_name, name, sz_name+1 );
    test->_procedure = procedure;
    test->_completed_scenarios = 0;
    test->_skipped_scenarios = 0;
    test->_prev_test = NULL;
    test->_next_test = NULL;

    test->_info = NULL;

    test->vtable = &UnitTest_Methods;

  }
  XCATCH( errcode ) {
    __UnitTest_delete( &test );
  }
  XFINALLY {
  }

  return test;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __UnitTest_run( UnitTest *test ) {
  int testcode;
  test->t0 = __GET_CURRENT_MICROSECOND_TICK();
  test->_info = (test_failure_info_t*)calloc( 1, sizeof(test_failure_info_t) );
  if( test->_info ) {
    testcode = test->_procedure( test, test->_info );
    if( testcode == 0 ) {
      free( test->_info );
      test->_info = NULL;
    }
  }
  else {
    testcode = -1;
  }
  test->t1 = __GET_CURRENT_MICROSECOND_TICK();
  return testcode;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static test_failure_info_t * __UnitTest_info( UnitTest *test ) {
  return test->_info;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static uint64_t __UnitTest_duration( UnitTest *test ) {
  return test->t1 - test->t0;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTest_scenarios( UnitTest *test ) {
  return test->_completed_scenarios + test->_skipped_scenarios;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTest_skipped_scenarios( UnitTest *test ) {
  return test->_skipped_scenarios;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTest_completed_scenarios( UnitTest *test ) {
  return test->_completed_scenarios;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __UnitTestSet_delete( UnitTestSet **test_set ) {
  if( test_set != NULL && *test_set != NULL ) {
    UnitTest *test;
    while( (test = (*test_set)->_head) != NULL ) {
      (*test_set)->_head = test->_next_test;
      __UnitTest_delete( &test );
    }
    if( (*test_set)->_name ) {
      free( (*test_set)->_name );
      (*test_set)->_name = NULL;
    }
    if( (*test_set)->_prev_set ) {
      (*test_set)->_prev_set->_next_set = (*test_set)->_next_set;
    }
    if( (*test_set)->_next_set ) {
      (*test_set)->_next_set->_prev_set = (*test_set)->_prev_set;
    }
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTestSet * __UnitTestSet_new( const char *name, test_descriptor_t *test_list ) {
  UnitTestSet *test_set = NULL;

  XTRY {
    size_t sz_name = strnlen(name, CXLIB_MSG_SIZE+1);
    // Allocate and initialize test set
    if( (test_set = (UnitTestSet*)calloc( 1, sizeof(UnitTestSet) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x121 );
    }
    if( (test_set->_name = (char*)calloc( sz_name+1, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x122 );
    }
    test_set->_number = -1;
    strncpy( test_set->_name, name, sz_name+1 );
    test_set->_head = NULL;
    test_set->_tail = NULL;
    test_set->_prev_set = NULL;
    test_set->_next_set = NULL;

    // methods
    test_set->vtable = &UnitTestSet_Methods;

    // add tests
    if( test_list ) {
      test_descriptor_t *descriptor = test_list;
      UnitTest *test;
      while( descriptor->name ) {
        if( (test = NewUnitTest( descriptor->name, descriptor->procedure )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x123 );
        }
        test_set->vtable->AddTest( test_set, test );
        descriptor++;
      }

    }

  }
  XCATCH( errcode ) {
    __UnitTestSet_delete( &test_set );
  }
  XFINALLY {
  }

  return test_set;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __UnitTestSet_run( UnitTestSet *test_set ) {
  int retcode = -1;

  test_set->t0 = __GET_CURRENT_MICROSECOND_TICK();

  XTRY {
    UnitTest *test = test_set->_head;
    int result;
    test_failure_info_t *info;

    __print( "*****************************************************************************" );
    __print( "********** TEST SET %04d: %-40s **********", test_set->_number, test_set->_name );
    __print( "*****************************************************************************" );
    __print( "" );

    while( test ) {
      __print( "********** TEST: %04d-%04d [%s %s] **********", test_set->_number, test->_number, test_set->_name, test->_name );
      
      g_current_test_set_number = test_set->_number;
      g_current_test_number = test->_number;
      g_current_test_set_name = test_set->_name;
      g_current_test_name = test->_name;

      result = test->vtable->Run( test );
      if( result != 0 ) {
        info = test->vtable->Info( test );
        __print( "*************************************************************************************************" );
        __print( "**********                                TEST FAILED !                                **********" );
        __print( "*********  SET   %04d :   %-60s  *********", test_set->_number, test_set->_name );
        __print( "********   TEST  %04d :   %-60s   ********", test->_number, test->_name );
        __print( "********   FILE       :   %-60s   ********", info ? info->file_name : "???" );
        __print( "********   LINE       :   %-60d   ********", info ? info->line_number : -1 );
        __print( "*********  EXPECTED   :   %-60s  *********", info ? info->reason : "???" );
        __print( "**********                                                                             **********" );
        __print( "*************************************************************************************************" );
        __print( "" );
        THROW_ERROR_MESSAGE( CXLIB_MSG_MOD_UTEST, 0x131, "Test termination" );
      }
      __print( "[OK %04d-%04d] (%.3f ms)", test_set->_number, test->_number, (double)test->vtable->Duration(test)/1000.0f );
      test = test->_next_test;
    }
    retcode = 0; // success
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    test_set->t1 = __GET_CURRENT_MICROSECOND_TICK();
  }

  return retcode;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __UnitTestSet_add_test( UnitTestSet *test_set, UnitTest *test ) {
  if( test_set && test ) {
    if( test_set->_head == NULL ) {
      test->_number = 1;
      test_set->_head = test;
      test_set->_tail = test;
    }
    else {
      test->_number = test_set->_tail->_number + 1;
      test->_next_test = test_set->_tail->_next_test;
      test->_prev_test = test_set->_tail;
      test_set->_tail->_next_test = test;
      test_set->_tail = test;
    }
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static uint64_t __UnitTestSet_duration( UnitTestSet *test_set ) {
  return test_set->t1 - test_set->t0;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTestSet_scenarios( UnitTestSet *test_set ) {
  int64_t n = 0;
  UnitTest *test = test_set->_head;
  while( test ) {
    n += test->vtable->Scenarios(test);
    test = test->_next_test;
  }
  return n; 
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTestSet_skipped_scenarios( UnitTestSet *test_set ) {
  int64_t n = 0;
  UnitTest *test = test_set->_head;
  while( test ) {
    n += test->vtable->Skipped(test);
    test = test->_next_test;
  }
  return n; 
}


/*******************************************************************//**
*
***********************************************************************
*/
static int64_t __UnitTestSet_completed_scenarios( UnitTestSet *test_set ) {
  int64_t n = 0;
  UnitTest *test = test_set->_head;
  while( test ) {
    n += test->vtable->Completed(test);
    test = test->_next_test;
  }
  return n; 
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __UnitTestSuite_delete( UnitTestSuite **test_suite ) {
  if( test_suite != NULL && *test_suite != NULL ) {
    UnitTestSet *test_set;
    while( (test_set = (*test_suite)->_head) != NULL ) {
      (*test_suite)->_head = test_set->_next_set;
      __UnitTestSet_delete( &test_set );
    }
    if( (*test_suite)->_name ) {
      free( (*test_suite)->_name );
      (*test_suite)->_name = NULL;
    }
    if( (*test_suite)->_prev_suite ) {
      (*test_suite)->_prev_suite->_next_suite = (*test_suite)->_next_suite;
    }
    if( (*test_suite)->_next_suite ) {
      (*test_suite)->_next_suite->_prev_suite = (*test_suite)->_prev_suite;
    }
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static UnitTestSuite * __UnitTestSuite_new( const char *name ) {
  UnitTestSuite *test_suite = NULL;

  XTRY {
    size_t sz_name = strnlen(name, CXLIB_MSG_SIZE+1);
    // Allocate and initialize test suite
    if( (test_suite = (UnitTestSuite*)calloc( 1, sizeof(UnitTestSuite) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x141 );
    }
    if( (test_suite->_name = (char*)calloc( sz_name+1, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x142 );
    }
    test_suite->_number = -1;
    strncpy( test_suite->_name, name, sz_name+1 );
    test_suite->_head = NULL;
    test_suite->_tail = NULL;
    test_suite->_prev_suite = NULL;
    test_suite->_next_suite = NULL;

    // methods
    test_suite->vtable = &UnitTestSuite_Methods;

  }
  XCATCH( errcode ) {
    __UnitTestSuite_delete( &test_suite );
  }
  XFINALLY {
  }

  return test_suite;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __UnitTestSuite_run( UnitTestSuite *test_suite ) {
  int retcode = -1;
  
  test_suite->t0 = __GET_CURRENT_MICROSECOND_TICK();

  XTRY {
    UnitTestSet *test_set = test_suite->_head;
    int result;

    __print( "*****************************************************************************" );
    __print( "***********                                                       ***********" );
    __print( "*********  STARTING TEST SUITE %04d: %-40s  *********", test_suite->_number, test_suite->_name );
    __print( "***********                                                       ***********" );
    __print( "*****************************************************************************" );
    __print( "" );

    if( GetCurrentTestDirectory() == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_MSG_MOD_UTEST, 0x151, "Cannot run test suite without a base directory set" );
    }
    if( !dir_exists( GetCurrentTestDirectory() ) ) {
      THROW_ERROR_MESSAGE( CXLIB_MSG_MOD_UTEST, 0x152, "Test suite directory '%s' does not exist", GetCurrentTestDirectory() );
    }

    while( test_set ) {
      result = test_set->vtable->Run( test_set );
      if( result != 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_MSG_MOD_UTEST, 0x153, "Failed in test set %s", test_set->_name );
      }
      test_set = test_set->_next_set;
      __print( "" );
    }

    UnitTest *test;
    int set_count;
    int total_count = 0;
    test_suite->t1 = __GET_CURRENT_MICROSECOND_TICK();
    __print( "*************************************************************************************************" );
    __print( "**********                                                                             **********" );
    __print( "*********                              ALL TESTS PASSED !                               *********" );
    __print( "********                                                                                 ********" );
    __print( "********      %-72s   ********", test_suite->_name );
    __print( "********      ======================================================================     ********" );
    __print( "********      SET   #    TESTS  SCENARIOS  DURATION  NAME                                ********" );
    __print( "********      ======================================================================     ********" );
    test_set = test_suite->_head;
    while( test_set ) {
      test = test_set->_head;
      set_count = 0;
      while( test ) {
        ++set_count;
        ++total_count;
        test = test->_next_test;
      }
      __print( "********  %3s SET %3d     %4d  %3d / %-3d  %6.1f s  %-34s  ********",
                          test_set->vtable->Skipped( test_set ) > 0 ? "!!!" : "",
                                  test_set->_number,
                                          set_count,
                                               test_set->vtable->Completed(test_set),
                                                     test_set->vtable->Scenarios(test_set),
                                                            (double)test_set->vtable->Duration(test_set)/1000000.0f,
                                                                     test_set->_name );
      test_set = test_set->_next_set;
    }
    __print( "********      ======================================================================     ********" );
    __print( "********      TOTAL %3s   %4d  %3d / %-3d  %6.1f s  %-34s  ********",
                                  test_suite->vtable->Skipped( test_suite ) > 0 ? "!!!" : "",
                                        total_count,
                                             test_suite->vtable->Completed(test_suite),
                                                   test_suite->vtable->Scenarios(test_suite),
                                                          (double)test_suite->vtable->Duration(test_suite)/1000000.0f,
                                                                   "" );
    __print( "********                                                                                 ********" );
    __print( "**********                                                                             **********" );
    __print( "*************************************************************************************************" );
    __print( "" );

    retcode = 0; // success
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
  }

  return retcode;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __UnitTestSuite_validate_test_names( UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *names[], const char **error ) {
  int ret = 0;
  if( names != NULL ) {
    const char **runonly_name = names;
    while( *runonly_name ) {
      int found = 0;
      test_descriptor_set_t *valid_descriptor = utest_sets;
      while( valid_descriptor->name ) {
        if( CharsEqualsConst( *runonly_name, valid_descriptor->name ) ) {
          found = 1;
          break;
        }
        valid_descriptor++;
      }
      if( !found ) {
        if( error ) {
          *error = *runonly_name;
        }
        ret = -1;
        break;
      }
      runonly_name++;
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __UnitTestSuite_add_test_set( UnitTestSuite *test_suite, UnitTestSet *test_set ) {
  if( test_suite && test_set ) {
    if( test_suite->_head == NULL ) {
      test_set->_number = 1;
      test_suite->_head = test_set;
      test_suite->_tail = test_set;
    }
    else {
      test_set->_number = test_suite->_tail->_number + 1;
      test_set->_next_set = test_suite->_tail->_next_set;
      test_set->_prev_set = test_suite->_tail;

      test_suite->_tail->_next_set = test_set;
      test_suite->_tail = test_set;
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __UnitTestSuite_extend_test_descriptor_sets( UnitTestSuite *test_suite, test_descriptor_set_t utest_sets[], const char *selected_tests[], const char **error ) {
  int ret = 0;
  test_descriptor_set_t *cursor = utest_sets;
  while( cursor->name ) {
    if( selected_tests == NULL || string_list_contains( cursor->name, selected_tests ) ) {
      UnitTestSet *utest_set = NewUnitTestSet( cursor->name, cursor->test_descriptors );
      if( !utest_set ) {
        if( error ) {
          *error = cursor->name;
        }
        ret = -1;
        break;
      }
      CALLABLE(test_suite)->AddTestSet( test_suite, utest_set );
    }
    cursor++;
  }
  return ret;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static uint64_t __UnitTestSuite_duration( UnitTestSuite *test_suite ) {
  return test_suite->t1 - test_suite->t0;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTestSuite_scenarios( UnitTestSuite *test_suite ) {
  int64_t n = 0;
  UnitTestSet *test_set = test_suite->_head;
  while( test_set ) {
    n += test_set->vtable->Scenarios( test_set );
    test_set = test_set->_next_set;
  }
  return n;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTestSuite_skipped_scenarios( UnitTestSuite *test_suite ) {
  int64_t n = 0;
  UnitTestSet *test_set = test_suite->_head;
  while( test_set ) {
    n += test_set->vtable->Skipped( test_set );
    test_set = test_set->_next_set;
  }
  return n;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __UnitTestSuite_completed_scenarios( UnitTestSuite *test_suite ) {
  int64_t n = 0;
  UnitTestSet *test_set = test_suite->_head;
  while( test_set ) {
    n += test_set->vtable->Completed( test_set );
    test_set = test_set->_next_set;
  }
  return n;
}



/**************************************************************************//**
 * __print
 *
 ******************************************************************************
 */
static void __print( const char *format, ... ) {
  va_list args;
  va_start( args, format );
  vfprintf( stderr, format, args );
  fprintf( stderr, "\n" );
  va_end( args );
  fflush( stderr );
}




/**************************************************************************//**
 * IncScenarioCount
 *
 ******************************************************************************
 */
DLL_EXPORT void IncScenarioCount(void) {

}




/**************************************************************************//**
 * SetCurrentTestDirectory
 *
 ******************************************************************************
 */
DLL_EXPORT int SetCurrentTestDirectory( const char *dirname ) {
  int ret = 0;
  g_current_test_dir = dirname;
  if( !dir_exists( g_current_test_dir ) ) {
    ret = create_dirs( g_current_test_dir );
  }
  return ret;
}



DLL_EXPORT const char * GetCurrentTestDirectory( void ) {
  return g_current_test_dir;
}




/**************************************************************************//**
 * UnitTestMessage
 *
 ******************************************************************************
 */
DLL_EXPORT void UnitTestMessage( const char *format, ... ) {
  va_list args;
  char tbuf[32] = {0};
  time_t ltime;
  struct tm *now;

  time( &ltime );
  if( (now = localtime( &ltime )) != NULL ) {
    strftime( tbuf, 31, "%Y-%m-%d %H:%M:%S", now ); 
  }

  va_start( args, format );
  fprintf( stderr, "TEST: %04d-%04d [%s %s] [%s]    ", g_current_test_set_number, g_current_test_number, g_current_test_set_name, g_current_test_name, tbuf );
  vfprintf( stderr, format, args );
  fprintf( stderr, "\n" );
  va_end( args );
  fflush( stderr );
}





/**************************************************************************//**
 * NewUnitTestSuite
 *
 ******************************************************************************
 */
DLL_EXPORT UnitTestSuite * NewUnitTestSuite( const char *name ) {
  return __UnitTestSuite_new( name );
}



/**************************************************************************//**
 * NewUnitTestSet
 *
 ******************************************************************************
 */
DLL_EXPORT UnitTestSet * NewUnitTestSet( const char *name, test_descriptor_t *test_list ) {
  return __UnitTestSet_new( name, test_list );
}



/**************************************************************************//**
 * NewUnitTest
 *
 ******************************************************************************
 */
DLL_EXPORT UnitTest * NewUnitTest( const char *name, test_procedure_t procedure ) {
  return __UnitTest_new( name, procedure );
}



/**************************************************************************//**
 * DeleteUnitTest
 *
 ******************************************************************************
 */
DLL_EXPORT void DeleteUnitTest( UnitTest **test ) {
  __UnitTest_delete( test );
}



/**************************************************************************//**
 * DeleteUnitTestSet
 *
 ******************************************************************************
 */
DLL_EXPORT void DeleteUnitTestSet( UnitTestSet **test_set ) {
  __UnitTestSet_delete( test_set );
}



/**************************************************************************//**
 * DeleteUnitTestSuite
 *
 ******************************************************************************
 */
DLL_EXPORT void DeleteUnitTestSuite( UnitTestSuite **test_suite ) {
  __UnitTestSuite_delete( test_suite );
}




/**************************************************************************//**
 * UnitTestFailed
 *
 ******************************************************************************
 */
DLL_EXPORT test_failure_info_t * UnitTestFailed( test_failure_info_t *info, const char *file_name, int line_number, const char *format, ... ) {
  va_list args;
  va_start( args, format );
  vsnprintf( info->reason, CXLIB_MSG_SIZE, format, args );
  va_end( args );
  strncpy( info->file_name, file_name, strnlen(file_name, MAX_PATH) );
  info->line_number = line_number;
  return info;
}
