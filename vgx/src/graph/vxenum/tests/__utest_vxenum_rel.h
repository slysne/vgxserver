/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxenum_rel.h
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

#ifndef __UTEST_VXENUM_REL_H
#define __UTEST_VXENUM_REL_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxenum_rel ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();
  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = NULL;


  /*******************************************************************//**
   * CREATE TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    // so we can have fresh ones in each scenario:
    _vxenum_rel__destroy_enumerator( graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * PERFORM A SIMPLE RELATIONSHIP MAPPING
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform a simple relationship mapping" ) {
    const CString_t *CSTR__relname = NULL;
    vgx_predicator_rel_enum rel;
    int lower = __VGX_PREDICATOR_REL_START_USER_RANGE;
    int upper = __VGX_PREDICATOR_REL_END_USER_RANGE;

    TEST_ASSERTION( _vxenum_rel__create_enumerator( graph ) == 0, "" );

    CString_t *CSTR__HAS = COMLIB_OBJECT_NEW( CString_t, "has", NULL );
    CString_t *CSTR__mapped;
    rel = _vxenum_rel__encode_CS( graph, CSTR__HAS, &CSTR__mapped, true );
    TEST_ASSERTION( rel != VGX_PREDICATOR_REL_ERROR,                    "Relationship should be encoded" );
    TEST_ASSERTION( rel != VGX_PREDICATOR_REL_NO_MAPPING,               "Relationship should be encoded" );
    TEST_ASSERTION( rel >= lower && rel <= upper,                       "Relationship code within user range" );

    CSTR__relname = _vxenum_rel__decode_CS( graph, rel );
    TEST_ASSERTION( CSTR__relname != NULL,                                    "Relationship code should be decoded" );
    TEST_ASSERTION( CALLABLE(CSTR__relname)->Equals(CSTR__relname, CSTR__HAS),            "Decoded relationship should match original" );

    _vxenum_rel__destroy_enumerator( graph );

    iString.Discard( &CSTR__HAS );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * PERFORM MANY RELATIONSHIP MAPPINGS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform many relationship mappings" ) {
    const char *relationship_strings[] = { "likes", "knows", "viewed", "purchased", "searched", "owns", "has",
                                           "clicked", "scrolled", "is", "put_in_cart", "deleted", "sold" };

    CString_t **CSTR__relationships;

    vgx_predicator_rel_enum *relmap;

    CString_t *CSTR__relationship;
    CString_t *CSTR__mapped;
    vgx_predicator_rel_enum rel;
    int lower = __VGX_PREDICATOR_REL_START_USER_RANGE;
    int upper = __VGX_PREDICATOR_REL_END_USER_RANGE;

    CString_t **CSTR__cursor;
    const CString_t *CSTR__entry;

    int size = 0;
    int mx;

    const size_t count = qwsizeof( relationship_strings );

    CSTR__relationships = (CString_t**)calloc( count+1, sizeof(CString_t*) ); // plus NULL term

    TEST_ASSERTION( CSTR__relationships != NULL,  "relationships array created" );

    for( size_t i=0; i<count; i++ ) {
      CSTR__relationships[i] = (CString_t*)iString.New( NULL, relationship_strings[i] );
      TEST_ASSERTION( CSTR__relationships[i] != NULL, "relationship entry created" );
    }
    CSTR__relationships[count] = NULL; // term

    TEST_ASSERTION( _vxenum_rel__create_enumerator( graph ) == 0, "" );

    // Initialize verification map
    relmap = (vgx_predicator_rel_enum*)calloc( count, sizeof(vgx_predicator_rel_enum) );
    TEST_ASSERTION( relmap != NULL,                                       "Verification map created" );

    // Insert and check
    CSTR__cursor = CSTR__relationships;
    mx = 0;
    while( (CSTR__relationship = *CSTR__cursor++) != NULL ) {
      rel = _vxenum_rel__encode_CS( graph, CSTR__relationship, &CSTR__mapped, true );
      TEST_ASSERTION( rel != VGX_PREDICATOR_REL_ERROR,                    "Relationship '%s' should be encoded", CALL( CSTR__relationship, Value ) );
      TEST_ASSERTION( rel != VGX_PREDICATOR_REL_NO_MAPPING,               "Relationship '%s' should be encoded", CALL( CSTR__relationship, Value ) );
      TEST_ASSERTION( rel >= lower && rel <= upper,                       "Relationship code within user range, got %u", rel );

      relmap[mx++] = rel;

      CSTR__entry = _vxenum_rel__decode_CS( graph, rel );
      TEST_ASSERTION( CSTR__entry != NULL,                                      "Relationship code '%u' should be decoded, got NULL", rel );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__relationship),        "Decoded relationship '%u' should match original '%s', got '%s'",
                                                                                                rel, CALL( CSTR__relationship, Value ), CALL( CSTR__entry, Value ) );

      TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__relationship, &CSTR__mapped, true ) == rel, "Existing mapping should return same value" );
    }

    // Check decoding again
    CSTR__cursor = CSTR__relationships;
    mx = 0;
    while( (CSTR__relationship = *CSTR__cursor++) != NULL ) {
      rel = relmap[mx++];
      CSTR__entry = _vxenum_rel__decode_CS( graph, rel );
      TEST_ASSERTION( CSTR__entry != NULL,                                      "Relationship code '%u' should be decoded, got NULL", rel );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__relationship),        "Decoded relationship '%u' should match original '%s', got '%s'",
                                                                                                rel, CALL( CSTR__relationship, Value ), CALL( CSTR__entry, Value ) );
    }

    // Check encoding again
    CSTR__cursor = CSTR__relationships;
    mx = 0;
    while( (CSTR__relationship = *CSTR__cursor++) != NULL ) {
      rel = _vxenum_rel__encode_CS( graph, CSTR__relationship, &CSTR__mapped, true );
      TEST_ASSERTION( rel == relmap[mx++],                                "Existing mapping should return same value" );

    }

    // Destroy relationship array
    for( size_t i=0; i<count; i++ ) {
      iString.Discard( &CSTR__relationships[i] );
    }
    free_const( CSTR__relationships );

    // Destroy verification map
    free( relmap );

    // Destroy enumerator
    _vxenum_rel__destroy_enumerator( graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * RELATIONSHIP MAP CACHE CORRECTNESS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Relationship map cache correctness" ) {
    CString_t *CSTR__FIRST = (CString_t*)iString.New( NULL, "something" );
    CString_t *CSTR__SECOND = (CString_t*)iString.New( NULL, "something_else" );
    const CString_t *CSTR__relationship1, *CSTR__relationship2;
    CString_t *CSTR__mapped; 
    vgx_predicator_rel_enum rel1, rel2;

    int lower = __VGX_PREDICATOR_REL_START_USER_RANGE;
    int upper = __VGX_PREDICATOR_REL_END_USER_RANGE;

    TEST_ASSERTION( _vxenum_rel__create_enumerator( graph ) == 0 , "" );

    // Encoder
    rel1 = _vxenum_rel__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true );
    TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == rel1,  "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == rel1,  "Cache hit gives same encoding" );
    rel2 = _vxenum_rel__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true );
    TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true ) == rel2, "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == rel1,  "Cache miss, still correct encoding" );
    TEST_ASSERTION( _vxenum_rel__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true ) == rel2, "Cache miss, still correct encoding" );

    // Decoder
    CSTR__relationship1 = _vxenum_rel__decode_CS( graph, rel1 );
    TEST_ASSERTION( CALLABLE(CSTR__FIRST)->Equals(CSTR__FIRST, CSTR__relationship1),          "Decoded vertex type should match original" );
    CSTR__relationship1 = _vxenum_rel__decode_CS( graph, rel1 );
    TEST_ASSERTION( CALLABLE(CSTR__FIRST)->Equals(CSTR__FIRST, CSTR__relationship1),          "Cache hit gives same decoding" );
    CSTR__relationship1 = _vxenum_rel__decode_CS( graph, rel1 );
    TEST_ASSERTION( CALLABLE(CSTR__FIRST)->Equals(CSTR__FIRST, CSTR__relationship1),          "Cache hit gives same decoding" );
    CSTR__relationship2 = _vxenum_rel__decode_CS( graph, rel2 );
    TEST_ASSERTION( CALLABLE(CSTR__SECOND)->Equals(CSTR__SECOND, CSTR__relationship2),        "Decoded vertex type should match original" );
    CSTR__relationship2 = _vxenum_rel__decode_CS( graph, rel2 );
    TEST_ASSERTION( CALLABLE(CSTR__SECOND)->Equals(CSTR__SECOND, CSTR__relationship2),        "Cache hit gives same decoding" );
    CSTR__relationship1 = _vxenum_rel__decode_CS( graph, rel1 );
    TEST_ASSERTION( CALLABLE(CSTR__FIRST)->Equals(CSTR__FIRST, CSTR__relationship1),          "Cache miss, still correct decoding" );
    CSTR__relationship2 = _vxenum_rel__decode_CS( graph, rel2 );
    TEST_ASSERTION( CALLABLE(CSTR__SECOND)->Equals(CSTR__SECOND, CSTR__relationship2),        "Cache miss, still correct decoding" );

    iString.Discard( &CSTR__FIRST );
    iString.Discard( &CSTR__SECOND );

    _vxenum_rel__destroy_enumerator( graph );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * PERFORM RELATIONSHIP MAPPINGS TO EXHAUSTION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform relationship mappings to exhaustion" ) {
    vgx_predicator_rel_enum rel;
    int lower = __VGX_PREDICATOR_REL_START_USER_RANGE;
    int upper = __VGX_PREDICATOR_REL_END_USER_RANGE;
    char word[32] = {'\0'};

    TEST_ASSERTION( _vxenum_rel__create_enumerator( graph ) == 0, "" );

    CString_t *CSTR__mapped;

    // Use up the full range
    for( int i = lower; i <= upper; i++ ) {
      const CString_t *CSTR__entry;
      CString_t *CSTR__relationship;
      // Generate a random word
      {
        int len = rand64() % 21 + 10;  // 10 - 30
        int cx = 0;
        while( cx < len ) {
          word[cx++] = (char)(rand64() % 26 + 'a');
        }
        word[cx] = '\0';
        CSTR__relationship = (CString_t*)iString.New( NULL, word );
      }

      rel = _vxenum_rel__encode_CS( graph, CSTR__relationship, &CSTR__mapped, true );
      TEST_ASSERTION( rel != VGX_PREDICATOR_REL_NO_MAPPING,             "Should be able to create mapping, full at %d", i );
      TEST_ASSERTION( rel >= lower && rel <= upper,                     "Relationship code within user range, got %u", rel );

      CSTR__entry = _vxenum_rel__decode_CS( graph, rel );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__relationship),      "Decoded relationship '%u' should match original '%s', got '%s'",
                                                                                              rel, CALL( CSTR__relationship, Value ), CALL( CSTR__entry, Value ) );
      iString.Discard( &CSTR__relationship );
    }

    // Try one more (it should fail)
    CString_t *CSTR__one_too_many = (CString_t*)iString.New( NULL, "ONE_TOO_MANY" );
    rel = _vxenum_rel__encode_CS( graph, CSTR__one_too_many, &CSTR__mapped, true );
    TEST_ASSERTION( rel == VGX_PREDICATOR_REL_NO_MAPPING,               "Should be unable to map beyond capacity" );
    iString.Discard( &CSTR__one_too_many );

    _vxenum_rel__destroy_enumerator( graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    // Re-create enumerator for graph destructor to work properly
    TEST_ASSERTION( _vxenum_rel__create_enumerator( graph ) == 0 , "" );
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
