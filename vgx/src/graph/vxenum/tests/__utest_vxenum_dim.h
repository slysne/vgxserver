/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxenum_dim.h
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

#ifndef __UTEST_VXENUM_DIM_H
#define __UTEST_VXENUM_DIM_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxenum_dim ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = NULL;
  vgx_Similarity_t *simobj = NULL;

  /*******************************************************************//**
   * CREATE TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    simobj = graph->similarity;
    TEST_ASSERTION( simobj != NULL, "graph should have similarity object" );
    // so we can have fresh ones in each scenario:
    _vxenum_dim__destroy_enumerator( simobj );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM A SIMPLE DIMENSION MAPPING
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform a simple dimension mapping" ) {
    const CString_t *CSTR__dimname;
    feature_vector_dimension_t dim;
    int lower = FEATURE_VECTOR_DIMENSION_MIN;
    int upper = FEATURE_VECTOR_DIMENSION_MAX;

    TEST_ASSERTION( _vxenum_dim__create_enumerator( simobj ) == 0 , "" );

    CString_t *CSTR__DIM = iString.New( NULL, "dimension" );

    dim = _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__DIM), NULL, true );
    TEST_ASSERTION( dim != FEATURE_VECTOR_DIMENSION_NONE,                    "Dimension should be encoded" );
    TEST_ASSERTION( dim >= lower && dim <= upper,                    "Dimension code within user range" );

    CSTR__dimname = _vxenum_dim__decode_CS( simobj, dim );
    TEST_ASSERTION( CSTR__dimname != NULL,                                 "Dimension code should be decoded" );
    TEST_ASSERTION( CStringEquals(CSTR__DIM, CSTR__dimname),                     "Decoded dimension should match original" );

    _vxenum_dim__destroy_enumerator( simobj );
    iString.Discard( &CSTR__DIM );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM MANY DIMENSION MAPPINGS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform many dimension mappings" ) {
    const char *dimension_strings[] = { "red", "blue", "green", "yellow", "large", "small", "medium", "high", "low", "wide", "narrow",
                                        "fun", "boring", "c++", "java", "python", "coffee", "chocolate", "strawberry", "fields", "forever" 
                                      };

    CString_t **CSTR__dimnames = NULL;

    feature_vector_dimension_t *dimmap;

    const CString_t *CSTR__dimname;
    feature_vector_dimension_t dim;
    int lower = FEATURE_VECTOR_DIMENSION_MIN;
    int upper = FEATURE_VECTOR_DIMENSION_MAX;

    CString_t **CSTR__cursor;
    const CString_t *CSTR__entry;
    
    int size = 0;
    int mx;

    const size_t count = qwsizeof( dimension_strings );

    CSTR__dimnames = (CString_t**)calloc( count+1, sizeof(CString_t*) ); // plus NUL term

    TEST_ASSERTION( CSTR__dimnames != NULL,  "dimnames array created" );

    for( size_t i = 0; i < count; i++ ) {
      CSTR__dimnames[i] = iString.New( NULL, dimension_strings[i] );
      TEST_ASSERTION( CSTR__dimnames[i] != NULL, "dimname entry created" );
    }
    CSTR__dimnames[count] = NULL; // term

    TEST_ASSERTION( _vxenum_dim__create_enumerator( simobj ) == 0 , "" );

    // Initialize verification map
    dimmap = (feature_vector_dimension_t*)calloc( count, sizeof(feature_vector_dimension_t) );
    TEST_ASSERTION( dimmap != NULL,                                      "Verification map created" );

    // Insert and check
    CSTR__cursor = CSTR__dimnames;
    mx = 0;
    while( (CSTR__dimname = *CSTR__cursor++) != NULL ) {
      dim = _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__dimname), NULL, true );
      TEST_ASSERTION( dim != FEATURE_VECTOR_DIMENSION_NONE,               "Dimension '%s' should be encoded", CALL(CSTR__dimname, Value) );
      TEST_ASSERTION( dim >= lower && dim <= upper,               "Dimension code within user range, got %u", dim );

      dimmap[mx++] = dim;

      CSTR__entry = _vxenum_dim__decode_CS( simobj, dim );
      TEST_ASSERTION( CSTR__entry != NULL,                              "Dimension code '%u' should be decoded, got NULL", dim );
      TEST_ASSERTION( CStringEquals(CSTR__entry,CSTR__dimname),               "Decoded dimension '%u' should match original '%s', got '%s'",
                                                                                      dim, CALL(CSTR__dimname,Value), CALL(CSTR__entry,Value) );

      TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__dimname), NULL, true ) == dim, "Existing mapping should return same value" );
    }

    // Check decoding again
    CSTR__cursor = CSTR__dimnames;
    mx = 0; 
    while( (CSTR__dimname = *CSTR__cursor++) != NULL ) {
      dim = dimmap[mx++];
      CSTR__entry = _vxenum_dim__decode_CS( simobj, dim );
      TEST_ASSERTION( CSTR__entry != NULL,                              "Dimension code '%u' should be decoded, got NULL", dim );
      TEST_ASSERTION( CStringEquals(CSTR__entry,CSTR__dimname),               "Decoded dimension '%u' should match original '%s', got '%s'",
                                                                                      dim, CALL(CSTR__dimname,Value), CALL(CSTR__entry,Value) );
    }

    // Check encoding again
    CSTR__cursor = CSTR__dimnames;
    mx = 0; 
    while( (CSTR__dimname = *CSTR__cursor++) != NULL ) {
      dim = _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__dimname), NULL, true );
      TEST_ASSERTION( dim == dimmap[mx++],                            "Existing mapping should return same value" );
    }

    // Destroy dimnames array
    for( size_t i=0; i<count; i++ ) {
      iString.Discard( &CSTR__dimnames[i] );
    }
    free_const( CSTR__dimnames );

    // Destroy verification map
    free( dimmap );

    // Destroy enumerator
    _vxenum_dim__destroy_enumerator( simobj );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DIMENSION MAP CACHE CORRECTNESS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Dimension map cache correctness" ) {
    CString_t *CSTR__FIRST = iString.New( NULL, "something" );
    CString_t *CSTR__SECOND = iString.New( NULL, "something else" );
    const CString_t *CSTR__dimname1, *CSTR__dimname2;
    feature_vector_dimension_t dim1, dim2;

    int lower = FEATURE_VECTOR_DIMENSION_MIN;
    int upper = FEATURE_VECTOR_DIMENSION_MAX;

    TEST_ASSERTION( _vxenum_dim__create_enumerator( simobj ) == 0 , "" );

    // Encoder
    dim1 = _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__FIRST), NULL, true );
    TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__FIRST), NULL, true ) == dim1,   "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__FIRST), NULL, true ) == dim1,   "Cache hit gives same encoding" );
    dim2 = _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__SECOND), NULL, true );
    TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__SECOND), NULL, true ) == dim2,  "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__FIRST), NULL, true ) == dim1,   "Cache miss, still correct encoding" );
    TEST_ASSERTION( _vxenum_dim__encode_chars_CS( simobj, CStringValue(CSTR__SECOND), NULL, true ) == dim2,  "Cache miss, still correct encoding" );

    // Decoder
    CSTR__dimname1 = _vxenum_dim__decode_CS( simobj, dim1 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname1,CSTR__FIRST),             "Decoded dimension should match original" );
    CSTR__dimname1 = _vxenum_dim__decode_CS( simobj, dim1 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname1,CSTR__FIRST),             "Cache hit gives same decoding" );
    CSTR__dimname1 = _vxenum_dim__decode_CS( simobj, dim1 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname1,CSTR__FIRST),             "Cache hit gives same decoding" );
    CSTR__dimname2 = _vxenum_dim__decode_CS( simobj, dim2 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname2,CSTR__SECOND),            "Decoded dimension should match original" );
    CSTR__dimname2 = _vxenum_dim__decode_CS( simobj, dim2 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname2,CSTR__SECOND),            "Cache hit gives same decoding" );
    CSTR__dimname1 = _vxenum_dim__decode_CS( simobj, dim1 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname1,CSTR__FIRST),             "Cache miss, still correct decoding" );
    CSTR__dimname2 = _vxenum_dim__decode_CS( simobj, dim2 );
    TEST_ASSERTION( CStringEquals(CSTR__dimname2,CSTR__SECOND),            "Cache miss, still correct decoding" );

    iString.Discard( &CSTR__FIRST );
    iString.Discard( &CSTR__SECOND );
    
    _vxenum_dim__destroy_enumerator( simobj );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM DIMENSION MAPPINGS TO EXHAUSTION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform dimension mappings to exhaustion" ) {
    feature_vector_dimension_t dim;
    int lower = FEATURE_VECTOR_DIMENSION_MIN;
    int upper = FEATURE_VECTOR_DIMENSION_MAX;
    char dimname[32] = {'\0'};

    TEST_ASSERTION( _vxenum_dim__create_enumerator( simobj ) == 0 , "" );

    // Use up the full range
    for( int i = 0; i < _MAX_DIMENSION_ENUMERATOR_ENTRIES; i++ ) {
      const CString_t *CSTR__entry;

      //do {
      int len = rand64() % 21 + 10;  // 10 - 30
      int cx = 0;
      // Generate a random word
      while( cx < len ) {
        dimname[cx++] = (char)(rand64() % 26 + 'a');
      }
      dimname[cx] = '\0';
      dim = _vxenum_dim__encode_chars_CS( simobj, dimname, NULL, true );
      if( len > MAX_FEATURE_VECTOR_TERM_LEN ) {
        dimname[ MAX_FEATURE_VECTOR_TERM_LEN ] = '\0';
        feature_vector_dimension_t dim_trunc = _vxenum_dim__encode_chars_CS( simobj, dimname, NULL, false );
        TEST_ASSERTION( dim == dim_trunc,                       "Oversized vector dimension should be truncated" );
      }

      TEST_ASSERTION( dim != FEATURE_VECTOR_DIMENSION_NONE,             "Should be able to create mapping, full at %d", i );
      TEST_ASSERTION( dim >= lower && dim <= upper,             "Dimension code within user range, got %u", dim );
      CSTR__entry = _vxenum_dim__decode_CS( simobj, dim );
      TEST_ASSERTION( CStringEqualsChars(CSTR__entry,dimname),  "Decoded dimension '%u' should match original '%s', got '%s'",
                                                                                      dim, dimname, CALL(CSTR__entry,Value) );
    }

    // Try one more (it should fail)
    const char *one_too_many = "ONE_TOO_MANY";
    dim = _vxenum_dim__encode_chars_CS( simobj, one_too_many, NULL, true );
    TEST_ASSERTION( dim == FEATURE_VECTOR_DIMENSION_NOENUM,                 "Should be unable to map beyond capacity" );

    _vxenum_dim__destroy_enumerator( simobj );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    // Re-create enumerator for graph destructor to work properly
    TEST_ASSERTION( _vxenum_dim__create_enumerator( simobj ) == 0 , "" );
    // Destroy graph
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    uint32_t owner;
    while( igraphfactory.CloseGraph( &graph, &owner ) > 0 );
    int ret = igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 0, false, NULL );
    TEST_ASSERTION( ret == 1, "graph should be destroyed" );
  } END_TEST_SCENARIO


  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST




#endif
