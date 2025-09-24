/*######################################################################
 *#
 *# __utest_vxenum_vtx.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXENUM_VTX_H
#define __UTEST_VXENUM_VTX_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxenum_vtx ) {
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
    _vxenum_vtx__destroy_enumerator( graph );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM A SIMPLE VERTEX TYPE MAPPING
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform a simple vertex type mapping" ) {
    const CString_t *CSTR__tname;
    vgx_vertex_type_t vxtype;
    int lower = __VERTEX_TYPE_ENUMERATION_START_USER_RANGE;
    int upper = __VERTEX_TYPE_ENUMERATION_END_USER_RANGE;

    TEST_ASSERTION( _vxenum_vtx__create_enumerator( graph ) == 0 , "" );

    CString_t *CSTR__OBJ = iString.New( NULL, "object" );
    CString_t *CSTR__mapped;
    vxtype = _vxenum_vtx__encode_CS( graph, CSTR__OBJ, &CSTR__mapped, true );
    TEST_ASSERTION( vxtype != VERTEX_TYPE_ENUMERATION_ERROR,            "Vertex type should be encoded" );
    TEST_ASSERTION( vxtype != VERTEX_TYPE_ENUMERATION_NO_MAPPING,       "Vertex type should be encoded" );
    TEST_ASSERTION( vxtype >= lower && vxtype <= upper,                 "Vertex type code within user range" );

    CSTR__tname = _vxenum_vtx__decode_CS( graph, vxtype );
    TEST_ASSERTION( CSTR__tname != NULL,                                      "Vertex type code should be decoded" );
    TEST_ASSERTION( CALLABLE(CSTR__tname)->Equals(CSTR__tname,CSTR__OBJ),                 "Decoded vertex type should match original" );

    _vxenum_vtx__destroy_enumerator( graph );

    iString.Discard( &CSTR__OBJ );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM MANY VERTEX TYPE MAPPINGS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform many vertex type mappings" ) {
    const char *typename_strings[] = { "object", "user", "query", "item", "thing", "product", "person",
                                       "review", "server", "node", "manufacturer", "store", "site" };

    CString_t **CSTR__typenames = NULL;

    vgx_vertex_type_t *typemap;

    const CString_t *CSTR__tname;
    CString_t *CSTR__mapped;
    vgx_vertex_type_t vxtype;
    int lower = __VERTEX_TYPE_ENUMERATION_START_USER_RANGE;
    int upper = __VERTEX_TYPE_ENUMERATION_END_USER_RANGE;

    CString_t **CSTR__cursor;
    const CString_t *CSTR__entry;
    
    int size = 0;
    int mx;

    const size_t count = qwsizeof( typename_strings );

    CSTR__typenames = (CString_t**)calloc( count+1, sizeof(CString_t*) ); // plus NUL term

    TEST_ASSERTION( CSTR__typenames != NULL,  "typenames array created" );

    for( size_t i = 0; i < count; i++ ) {
      CSTR__typenames[i] = iString.New( NULL, typename_strings[i] );
      TEST_ASSERTION( CSTR__typenames[i] != NULL, "typename entry created" );
    }
    CSTR__typenames[count] = NULL; // term

    TEST_ASSERTION( _vxenum_vtx__create_enumerator( graph ) == 0 , "" );

    // Initialize verification map
    typemap = (vgx_vertex_type_t*)calloc( count, sizeof(vgx_vertex_type_t) );
    TEST_ASSERTION( typemap != NULL,                                      "Verification map created" );

    // Insert and check
    CSTR__cursor = CSTR__typenames;
    mx = 0;
    while( (CSTR__tname = *CSTR__cursor++) != NULL ) {
      vxtype = _vxenum_vtx__encode_CS( graph, CSTR__tname, &CSTR__mapped, true );
      TEST_ASSERTION( vxtype != VERTEX_TYPE_ENUMERATION_ERROR,            "Vertex type '%s' should be encoded", CALL(CSTR__tname, Value) );
      TEST_ASSERTION( vxtype != VERTEX_TYPE_ENUMERATION_NO_MAPPING,       "Vertex type '%s' should be encoded", CALL(CSTR__tname, Value) );
      TEST_ASSERTION( vxtype >= lower && vxtype <= upper,                 "Vertex type code within user range, got %u", vxtype );

      typemap[mx++] = vxtype;

      CSTR__entry = _vxenum_vtx__decode_CS( graph, vxtype );
      TEST_ASSERTION( CSTR__entry != NULL,                                      "Vertex type code '%u' should be decoded, got NULL", vxtype );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__tname),               "Decoded vertex type '%u' should match original '%s', got '%s'",
                                                                                                vxtype, CALL(CSTR__tname,Value), CALL(CSTR__entry,Value) );

      TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__tname, &CSTR__mapped, true ) == vxtype, "Existing mapping should return same value" );
    }

    // Check decoding again
    CSTR__cursor = CSTR__typenames;
    mx = 0; 
    while( (CSTR__tname = *CSTR__cursor++) != NULL ) {
      vxtype = typemap[mx++];
      CSTR__entry = _vxenum_vtx__decode_CS( graph, vxtype );
      TEST_ASSERTION( CSTR__entry != NULL,                                      "Vertex type code '%u' should be decoded, got NULL", vxtype );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__tname),               "Decoded vertex type '%u' should match original '%s', got '%s'",
                                                                                                vxtype, CALL(CSTR__tname,Value), CALL(CSTR__entry,Value) );
    }

    // Check encoding again
    CSTR__cursor = CSTR__typenames;
    mx = 0; 
    while( (CSTR__tname = *CSTR__cursor++) != NULL ) {
      vxtype = _vxenum_vtx__encode_CS( graph, CSTR__tname, &CSTR__mapped, true );
      TEST_ASSERTION( vxtype == typemap[mx++],                            "Existing mapping should return same value" );
    }

    // Destroy typenames array
    for( size_t i=0; i<count; i++ ) {
      iString.Discard( &CSTR__typenames[i] );
    }
    free_const( CSTR__typenames );

    // Destroy verification map
    free( typemap );

    // Destroy enumerator
    _vxenum_vtx__destroy_enumerator( graph );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * VERTEX TYPE MAP CACHE CORRECTNESS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Vertex type map cache correctness" ) {
    CString_t *CSTR__FIRST = iString.New( NULL, "something" );
    CString_t *CSTR__SECOND = iString.New( NULL, "something_else" );
    const CString_t *CSTR__tname1, *CSTR__tname2;
    CString_t *CSTR__mapped;
    vgx_vertex_type_t vxtype1, vxtype2;

    int lower = __VERTEX_TYPE_ENUMERATION_START_USER_RANGE;
    int upper = __VERTEX_TYPE_ENUMERATION_END_USER_RANGE;

    TEST_ASSERTION( _vxenum_vtx__create_enumerator( graph ) == 0 , "" );

    // Encoder
    vxtype1 = _vxenum_vtx__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true );
    TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == vxtype1,  "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == vxtype1,  "Cache hit gives same encoding" );
    vxtype2 = _vxenum_vtx__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true );
    TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true ) == vxtype2, "Cache hit gives same encoding" );
    TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__FIRST, &CSTR__mapped, true ) == vxtype1,  "Cache miss, still correct encoding" );
    TEST_ASSERTION( _vxenum_vtx__encode_CS( graph, CSTR__SECOND, &CSTR__mapped, true ) == vxtype2, "Cache miss, still correct encoding" );

    // Decoder
    CSTR__tname1 = _vxenum_vtx__decode_CS( graph, vxtype1 );
    TEST_ASSERTION( CALLABLE(CSTR__tname1)->Equals(CSTR__tname1,CSTR__FIRST),                "Decoded vertex type should match original" );
    CSTR__tname1 = _vxenum_vtx__decode_CS( graph, vxtype1 );
    TEST_ASSERTION( CALLABLE(CSTR__tname1)->Equals(CSTR__tname1,CSTR__FIRST),                "Cache hit gives same decoding" );
    CSTR__tname1 = _vxenum_vtx__decode_CS( graph, vxtype1 );
    TEST_ASSERTION( CALLABLE(CSTR__tname1)->Equals(CSTR__tname1,CSTR__FIRST),                "Cache hit gives same decoding" );
    CSTR__tname2 = _vxenum_vtx__decode_CS( graph, vxtype2 );
    TEST_ASSERTION( CALLABLE(CSTR__tname2)->Equals(CSTR__tname2,CSTR__SECOND),               "Decoded vertex type should match original" );
    CSTR__tname2 = _vxenum_vtx__decode_CS( graph, vxtype2 );
    TEST_ASSERTION( CALLABLE(CSTR__tname2)->Equals(CSTR__tname2,CSTR__SECOND),               "Cache hit gives same decoding" );
    CSTR__tname1 = _vxenum_vtx__decode_CS( graph, vxtype1 );
    TEST_ASSERTION( CALLABLE(CSTR__tname1)->Equals(CSTR__tname1,CSTR__FIRST),                "Cache miss, still correct decoding" );
    CSTR__tname2 = _vxenum_vtx__decode_CS( graph, vxtype2 );
    TEST_ASSERTION( CALLABLE(CSTR__tname2)->Equals(CSTR__tname2,CSTR__SECOND),               "Cache miss, still correct decoding" );

    iString.Discard( &CSTR__FIRST );
    iString.Discard( &CSTR__SECOND );
    
    _vxenum_vtx__destroy_enumerator( graph );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * PERFORM VERTEX TYPE MAPPINGS TO EXHAUSTION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Perform vertex type mappings to exhaustion" ) {
    vgx_vertex_type_t vxtype;
    int lower = __VERTEX_TYPE_ENUMERATION_START_USER_RANGE;
    int upper = __VERTEX_TYPE_ENUMERATION_END_USER_RANGE;
    char word[32] = {'\0'};

    TEST_ASSERTION( _vxenum_vtx__create_enumerator( graph ) == 0 , "" );
    CString_t *CSTR__mapped;

    // Use up the full range
    for( int i = lower; i <= upper; i++ ) {
      const CString_t *CSTR__entry;
      CString_t *CSTR__tname;

      // Generate a random word
      {
        int len = rand64() % 21 + 10;  // 10 - 30
        int cx = 0;
        while( cx < len ) {
          word[cx++] = (char)(rand64() % 26 + 'a');
        }
        word[cx] = '\0';
        CSTR__tname = iString.New( NULL, word );
      }

      vxtype = _vxenum_vtx__encode_CS( graph, CSTR__tname, &CSTR__mapped, true );
      TEST_ASSERTION( vxtype != VERTEX_TYPE_ENUMERATION_NO_MAPPING,         "Should be able to create mapping, full at %d", i );
      TEST_ASSERTION( vxtype >= lower && vxtype <= upper,                   "Vertex type code within user range, got %u", vxtype );

      CSTR__entry = _vxenum_vtx__decode_CS( graph, vxtype );
      TEST_ASSERTION( CALLABLE(CSTR__entry)->Equals(CSTR__entry,CSTR__tname),                 "Decoded vertex type '%u' should match original '%s', got '%s'",
                                                                                                  vxtype, CALL(CSTR__tname,Value), CALL(CSTR__entry,Value) );
      iString.Discard( &CSTR__tname );
    }

    // Try one more (it should fail)
    CString_t *CSTR__one_too_many = iString.New( NULL, "ONE_TOO_MANY" );
    vxtype = _vxenum_vtx__encode_CS( graph, CSTR__one_too_many, &CSTR__mapped, true );
    TEST_ASSERTION( vxtype == VERTEX_TYPE_ENUMERATION_NO_MAPPING,           "Should be unable to map beyond capacity" );
    iString.Discard( &CSTR__one_too_many );

    _vxenum_vtx__destroy_enumerator( graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    // Re-create enumerator for graph destructor to work properly
    TEST_ASSERTION( _vxenum_vtx__create_enumerator( graph ) == 0 , "" );
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
