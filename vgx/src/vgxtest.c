/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgxtest.c
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

#include "_vgx.h"
#include "_vxtest.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );




/**************************************************************************//**
 * show_sizeof
 *
 ******************************************************************************
 */
static int show_sizeof(void) {
  /*
   * INFO
   */

  pmesg( 4, "sizeof(vgx_Vertex_vtable_t)            : %llu\n", sizeof(vgx_Vertex_vtable_t) );
  pmesg( 4, "sizeof(vgx_Vertex_t)                   : %llu\n", sizeof(vgx_Vertex_t) );
  pmesg( 4, "sizeof(vgx_VertexData_t)               : %llu\n", sizeof(vgx_VertexData_t) );
  pmesg( 4, "sizeof(vgx_Vertex_constructor_args_t)  : %llu\n", sizeof(vgx_Vertex_constructor_args_t) );

  pmesg( 4, "sizeof(vgx_IOperation_t)               : %llu\n", sizeof(vgx_IOperation_t) );

  pmesg( 4, "sizeof(vgx_Graph_vtable_t)             : %llu\n", sizeof(vgx_Graph_vtable_t) );
  pmesg( 4, "sizeof(vgx_Graph_t)                    : %llu\n", sizeof(vgx_Graph_t) );
  pmesg( 4, "sizeof(vgx_Graph_constructor_args_t)   : %llu\n", sizeof(vgx_Graph_constructor_args_t) );

  pmesg( 4, "sizeof(vgx_IArcVector_t)               : %llu\n", sizeof(vgx_IArcVector_t) );

  pmesg( 4, "\n" );

  //TODO: add more things, enums etc.

  return 0;
}




/**************************************************************************//**
 * vgx_get_unit_test_definitions
 *
 ******************************************************************************
 */
DLL_EXPORT test_descriptor_set_t * vgx_get_unit_test_definitions( void ) {
#ifdef INCLUDE_UNIT_TESTS
  const int sz = __TOTAL_TEST_COUNT;
  test_descriptor_set_t *vgx_utest_sets = calloc( sz+1, sizeof( test_descriptor_set_t ) );
  if( vgx_utest_sets != NULL ) {
    // Populate from descriptors
    test_descriptor_set_t *cursor = vgx_utest_sets;
    const testgroup_t *group = test_groups;
    while( group->name ) {
      const test_descriptor_set_t *entry = group->tests;
      while( entry->name ) {
        cursor->name = entry->name;
        cursor->test_descriptors = entry->test_descriptors;
        ++cursor;
        ++entry;
      }
      ++group;
    }
  }
  return vgx_utest_sets;
#else
  return calloc( 1, sizeof( test_descriptor_set_t ) );
#endif
}




/**************************************************************************//**
 * vgx_get_unit_test_names
 *
 ******************************************************************************
 */
DLL_EXPORT char ** vgx_get_unit_test_names( void ) {
#ifdef INCLUDE_UNIT_TESTS
  char **names = NULL;
  test_descriptor_set_t *vgx_utest_sets = vgx_get_unit_test_definitions();
  if( vgx_utest_sets ) {
    const int sz = __TOTAL_TEST_COUNT;
    if( (names = calloc( sz+1, sizeof( char* ) )) != NULL ) {
      test_descriptor_set_t *src = vgx_utest_sets;
      const char **dest = (const char**)names;
      while( src->name ) {
        *dest++ = src++->name;
      }
    }
    free( vgx_utest_sets );
  }
  return names;
#else
  return calloc( 1, sizeof( char*) );
#endif
}



SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * vgx_unit_tests
 *
 ******************************************************************************
 */
DLL_EXPORT int vgx_unit_tests( const char *runonly[], const char *testdir ) {
#ifdef INCLUDE_UNIT_TESTS
  int retcode = -1;

  vgx_context_t *init_here = NULL;
  if( !igraphfactory.IsInitialized() ) {
    init_here = vgx_INIT( NULL );
  }

  if( SetCurrentTestDirectory( testdir ) < 0 ) {
    return -1;
  }

  // Populate from descriptors
  test_descriptor_set_t *vgx_utest_sets = vgx_get_unit_test_definitions();
  if( vgx_utest_sets == NULL ) {
    return -1;
  }

  UnitTestSuite *vgx_utest_suite = NULL;

  XTRY {
    int result;
    const char *error = NULL;

    // Create the test suite
    if( (vgx_utest_suite = NewUnitTestSuite( "VGX Test Suite" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xE01 );
    }

    // Make sure all selected tests are defined
    if( CALLABLE( vgx_utest_suite )->ValidateTestNames( vgx_utest_suite, vgx_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xE02, "Undefined test: %s", error );
    }

    // Add all (selected) test sets to test suite
    if( CALLABLE( vgx_utest_suite )->ExtendTestDescriptorSets( vgx_utest_suite, vgx_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xE03, "Failed to add test: %s", error );
    }

    // Run test suite
    result = vgx_utest_suite->vtable->Run( vgx_utest_suite ) ;

    // Check result
    if( result != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xE04, "Test suite failed" );
    }

    retcode = 0;
  }
  XCATCH( errcode ) {
    
  }
  XFINALLY {
    DeleteUnitTestSuite( &vgx_utest_suite );
    if( vgx_utest_sets ) {
      free( vgx_utest_sets );
    }
  }

  vgx_DESTROY( &init_here );

  return retcode;
#else
  WARN( 0x001, "Unit tests disabled for this build" );
  return 0;
#endif
}
