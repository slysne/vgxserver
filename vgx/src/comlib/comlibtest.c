/*######################################################################
 *#
 *# comlibtest.c
 *#
 *#
 *######################################################################
 */


#include "_comlib.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );


static int show_sizeof(void) {
  /*
   * INFO
   *
   *
   */
  pmesg( 4, "sizeof(char)          : %d\n", sizeof(char) );
  pmesg( 4, "sizeof(short)         : %d\n", sizeof(short) );
  pmesg( 4, "sizeof(int)           : %d\n", sizeof(int) );
  pmesg( 4, "sizeof(long)          : %d\n", sizeof(long) );
  pmesg( 4, "sizeof(long long)     : %d\n", sizeof(long long) );
  pmesg( 4, "sizeof(size_t)        : %d\n", sizeof(size_t) );
  pmesg( 4, "sizeof(float)         : %d\n", sizeof(float) );
  pmesg( 4, "sizeof(double)        : %d\n", sizeof(double) );
  pmesg( 4, "sizeof(long double)   : %d\n", sizeof(long double) );
  pmesg( 4, "sizeof(objectid_t)    : %d\n", sizeof(objectid_t) );
  pmesg( 4, "\n" );

  return 0;
}



#ifdef INCLUDE_UNIT_TESTS
test_descriptor_set_t comlib_utest_sets[] = {
  { "COMLIB",                     _comlib_tests },

  { "cxcstring.c",                _comlib_cxcstring_tests },

  { "CStringQueue_t",             _comlib_cstringqueue_tests },

  { "CByteQueue_t",               _comlib_cbytequeue_tests },
  { "CWordQueue_t",               _comlib_cwordqueue_tests },
  { "CDwordQueue_t",              _comlib_cdwordqueue_tests },
  { "CQwordQueue_t",              _comlib_cqwordqueue_tests },
  { "Cm128iQueue_t",              _comlib_cm128iqueue_tests },
  { "Cm256iQueue_t",              _comlib_cm256iqueue_tests },
  { "Cm512iQueue_t",              _comlib_cm512iqueue_tests },
  { "CtptrQueue_t",               _comlib_ctptrqueue_tests },
  { "Cx2tptrQueue_t",             _comlib_cx2tptrqueue_tests },
  { "CaptrQueue_t",               _comlib_captrqueue_tests },

  { "CByteHeap_t",                _comlib_cbyteheap_tests },
  { "CWordHeap_t",                _comlib_cwordheap_tests },
  { "CDwordHeap_t",               _comlib_cdwordheap_tests },
  { "CQwordHeap_t",               _comlib_cqwordheap_tests },
  { "CtptrHeap_t",                _comlib_ctptrheap_tests },
  { "CaptrHeap_t",                _comlib_captrheap_tests },

  {NULL}
};
#endif





DLL_EXPORT char ** comlib_get_unit_test_names( void ) {
#ifdef INCLUDE_UNIT_TESTS
#define __TEST_COUNT( Set ) ((sizeof( Set ) / sizeof( test_descriptor_set_t ))-1)
  char **names = NULL;
  const int sz = __TEST_COUNT( comlib_utest_sets );
  if( (names = calloc( sz+1, sizeof( char* ) )) != NULL ) {
    test_descriptor_set_t *src = comlib_utest_sets;
    const char **dest = (const char**)names;
    while( src->name ) {
      *dest++ = src++->name;
    }
  }
  return names;
#else
  return calloc( 1, sizeof( char*) );
#endif
}



SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_EXPORT int comlib_unit_tests( const char *runonly[], const char *testdir ) {
#ifdef INCLUDE_UNIT_TESTS
  int retcode = -1;

  // TODO: implement selective tests if runonly != NULL

  comlib_INIT();

  if( SetCurrentTestDirectory( testdir ) < 0 ) {
    return -1;
  }



  UnitTestSuite *comlib_utest_suite = NULL;

  XTRY {
    int result;
    const char *error = NULL;

    // Create the test suite
    if( (comlib_utest_suite = NewUnitTestSuite( "COMLIB Test Suite" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xC01 );
    }

    // Make sure all selected tests are defined
    if( CALLABLE( comlib_utest_suite )->ValidateTestNames( comlib_utest_suite, comlib_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xC02, "Undefined test: %s", error );
    }

    // Add all (selected) test sets to test suite
    if( CALLABLE( comlib_utest_suite )->ExtendTestDescriptorSets( comlib_utest_suite, comlib_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xC03, "Failed to add test: %s", error );
    }

    // Run test suite
    result = CALLABLE(comlib_utest_suite)->Run( comlib_utest_suite ) ;

    // Check result
    if( result != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xC04, "Test suite failed" );
    }

    retcode = 0;
  }
  XCATCH( errcode ) {
    
  }
  XFINALLY {
    DeleteUnitTestSuite( &comlib_utest_suite );
  }

  return retcode;
#else
  return 0;
#endif
}







