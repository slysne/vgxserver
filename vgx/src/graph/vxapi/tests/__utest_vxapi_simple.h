/*######################################################################
 *#
 *# __utest_vxapi_simple.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXAPI_SIMPLE_H
#define __UTEST_VXAPI_SIMPLE_H

#include "__vxtest_macro.h"

BEGIN_UNIT_TEST( __utest_vxapi_simple ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  vgx_Graph_t *graph = NULL;

  DWORD threadid = GET_CURRENT_THREAD_ID();

  vgx_Graph_vtable_t *igraph = (vgx_Graph_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Graph_t );
  vgx_Vertex_vtable_t *iV = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );


  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL,              "Graph constructed, graph=%llp", graph );
    TEST_ASSERTION( GraphOrder( graph ) == 0,   "Graph has no vertices" );
    TEST_ASSERTION( GraphSize( graph ) == 0,    "Graph has no edges" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * BASIC VERTEX LIFECYCLE
   ***********************************************************************
   */
   NEXT_TEST_SCENARIO( true, "Basic Vertex Lifecycle" ) {

    const char *name_A = "A";
    const char *name_B = "B";
    const CString_t *CSTR___A = NewEphemeralCString( graph, name_A );
    const CString_t *CSTR___B = NewEphemeralCString( graph, name_B );
    const CString_t *CSTR___THING = NewEphemeralCString( graph, "THING" );
    const CString_t *CSTR___DIFFERENT_THING = NewEphemeralCString( graph, "DIFFERENT_THING" );

    vgx_Vertex_t *A = NULL;
    vgx_Vertex_t *B = NULL;
    vgx_Vertex_t *X;
    objectid_t obid1, obid2;
    vgx_vertex_type_t vxtype;
    const CString_t *CSTR__tpname;

    // Create Vertex A
    TEST_ASSERTION( igraph->simple->CreateVertexSimple( graph, name_A, NULL ) == 1,            "Vertex A created" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                                                 "Graph has 1 vertex" );

    // Open Vertex A and inspect
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 100, NULL, NULL );
    TEST_ASSERTION( A != NULL,                                                                 "Opened A" );
    TEST_ASSERTION( Vertex_REFCNT_WL( A ) == VXTABLE_VERTEX_REFCOUNT + 1,                      "A has 2 owners (us and index)" );
    TEST_ASSERTION( strncmp( iV->IDString( A ), CStringValue(CSTR___A), 2 ) == 0,              "Vertex A ID OK" );
    obid1 = iV->InternalID( A );
    obid2 = *CStringObid( CSTR___A );
    TEST_ASSERTION( idmatch( &obid1, &obid2 ),                                                 "Vertex A OBID OK" );
    TEST_ASSERTION( iV->Degree( A ) == 0,                                                      "Vertex A degree = 0" );

    // Close vertex A
    X = A;
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );
    TEST_ASSERTION( A == NULL,                                                                 "Vertex pointer set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL( X ) == VXTABLE_VERTEX_REFCOUNT,                          "A has 1 owner (index)" );

    // Set vertex type
    vxtype = igraph->simple->VertexSetType( graph, CSTR___A, CSTR___THING, 100, NULL );
    TEST_ASSERTION( vxtype >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
                    &&
                    vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE,                        "Vertex type mapped to user range" );

    // Get vertex type
    CSTR__tpname = igraph->simple->VertexGetType( graph, CSTR___A, 100, NULL );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___THING ),                               "Reverse mapping matches original" );

    // Open vertex, set same vertex type again, then close
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 100, NULL, NULL );
    vxtype = igraph->simple->VertexSetType( graph, CSTR___A, CSTR___THING, 100, NULL );
    TEST_ASSERTION( vxtype >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
                    &&
                    vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE,                        "Vertex type mapped to user range" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );

    // Open vertex A readonly and check type
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_READONLY, 100, NULL, NULL );
    CSTR__tpname = CALLABLE( A )->TypeName( A );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___THING ),                               "Reverse mapping matches original" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );

    // Again, open vertex, set same vertex type again, then close
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 100, NULL, NULL );
    vxtype = igraph->simple->VertexSetType( graph, CSTR___A, CSTR___THING, 100, NULL );
    TEST_ASSERTION( vxtype >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
                    &&
                    vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE,                        "Vertex type mapped to user range" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );

    // Open vertex A readonly and check type
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_READONLY, 100, NULL, NULL );
    CSTR__tpname = CALLABLE( A )->TypeName( A );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___THING ),                               "Reverse mapping matches original" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );

    // Set different vertex types
    TEST_ASSERTION( igraph->simple->CreateVertexSimple( graph, name_B, NULL ) == 1,            "Vertex B created" );
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 100, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 100, NULL, NULL );
    vxtype = igraph->simple->VertexSetType( graph, CSTR___A, CSTR___THING, 100, NULL );
    TEST_ASSERTION( vxtype >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
                    &&
                    vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE,                        "Vertex type mapped to user range" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed." );
    vxtype = igraph->simple->VertexSetType( graph, CSTR___B, CSTR___DIFFERENT_THING, 100, NULL );
    TEST_ASSERTION( vxtype >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
                    &&
                    vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE,                        "Vertex type mapped to user range" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ),                                  "Vertex B closed." );

    // Get vertex type for B
    CSTR__tpname = igraph->simple->VertexGetType( graph, CSTR___B, 100, NULL );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___DIFFERENT_THING ),                     "Reverse mapping matches original" );

    // Get vertex type for A
    CSTR__tpname = igraph->simple->VertexGetType( graph, CSTR___A, 100, NULL );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___THING ),                               "Reverse mapping matches original" );

    // Open vertices A and B
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_READONLY, 100, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_READONLY, 100, NULL, NULL );

    //
    CSTR__tpname = CALLABLE( A )->TypeName( A );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___THING ),                               "Reverse mapping matches original" );

    //
    CSTR__tpname = CALLABLE( B )->TypeName( B );
    TEST_ASSERTION( CSTR__tpname != NULL,                                                      "Reverse mapping exists" );
    TEST_ASSERTION( CStringEquals( CSTR__tpname, CSTR___DIFFERENT_THING ),                     "Reverse mapping matches original" );

    // Close vertex B
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ),                                  "Vertex B closed." );

    // Can't delete open vertex
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR___A, 100, NULL, NULL ) == -1,    "Can't delete open vertex" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                                                 "Graph still has 2 vertices" );
    TEST_ASSERTION( Vertex_REFCNT_WL( A ) == VXTABLE_VERTEX_REFCOUNT + 1,                      "A has 2 owners (us and index)" );

    // Close vertex A
    X = A;
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ),                                  "Vertex A closed" );
    TEST_ASSERTION( Vertex_REFCNT_WL( X ) == VXTABLE_VERTEX_REFCOUNT,                          "A has 1 owner (index)" );

    // Delete vertex A
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR___A, 100, NULL, NULL ) == 1,     "Vertex A deleted" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                                                 "Graph has 1 vertex left (B)" );
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR___A, 100, NULL, NULL ) == 0,     "Vertex A does not exist" );

    // Delete vertex B
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR___B, 100, NULL, NULL ) == 1,     "Should delete vertex B" );
    TEST_ASSERTION( GraphOrder(graph) == 0,                                                 "Graph is empty" );



   } END_TEST_SCENARIO 






  /*******************************************************************//**
   * MEMORY TEST - REAL VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Memory Test - REAL Vertices" ) {
    const CString_t *CSTR__idA = NULL;
    const CString_t *CSTR__id = NULL;
    int N = 1 << 26; // ~64M vertices
    vgx_Vertex_t *V = NULL;
    for( int n_vertices=1; n_vertices<=N; n_vertices *= 2 ) {
      // Create vertices
      for( int a=0; a<n_vertices; a++ ) {
        CSTR__idA = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "A_%d", a );
        TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR__idA, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL,  "Vertex %d created", a );
        CSTR__id = NewEphemeralCString( graph, CALLABLE(V)->IDString(V) );
        TEST_ASSERTION( CStringEquals( CSTR__idA, CSTR__id ),                             "Vertex %s, got %s", CStringValue(CSTR__idA), CStringValue(CSTR__id) );
        TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true,                 "Vertex %d closed", a );
        CStringDelete( CSTR__idA );
        CStringDelete( CSTR__id );
      }
      // Re-open vertices readonly, then close, then delete
      for( int a=0; a<n_vertices; a++ ) {
        CSTR__idA = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "A_%d", a );
        TEST_ASSERTION( ( V = igraph->simple->OpenVertex( graph, CSTR__idA, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "Vertex %d opened", a );
        CSTR__id = NewEphemeralCString( graph, CALLABLE(V)->IDString(V) );
        TEST_ASSERTION( CStringEquals( CSTR__idA, CSTR__id ),                                 "Vertex %s, got %s", CStringValue(CSTR__idA), CStringValue(CSTR__id) );
        TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true,                     "Vertex %d closed", a );
        TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__idA, 0, NULL, NULL ) == 1, "Vertex %d deleted", a );
        CStringDelete( CSTR__idA );
        CStringDelete( CSTR__id );
      }
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY THE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    uint32_t owner;
    TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,          "Graph closed" );
    TEST_ASSERTION( graph == NULL,                                            "Graph pointer set to NULL" );
    TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 10, false, NULL ) == 1, "Graph deleted" );
  } END_TEST_SCENARIO

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST


#endif
