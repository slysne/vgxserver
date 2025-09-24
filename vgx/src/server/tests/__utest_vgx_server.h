/*######################################################################
 *#
 *# __utest_vgx_server.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VGX_SERVER_H
#define __UTEST_VGX_SERVER_H

#include "__vxtest_macro.h"

BEGIN_UNIT_TEST( __utest_vgx_server ) {

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
    TEST_ASSERTION( GraphOrder(graph) == 0,  "Graph has no vertices" );
    TEST_ASSERTION( GraphSize(graph) == 0,   "Graph has no edges" );
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
