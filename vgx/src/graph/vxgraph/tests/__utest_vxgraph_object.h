/*######################################################################
 *#
 *# __utest_vxgraph_object.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXGRAPH_OBJECT_H
#define __UTEST_VXGRAPH_OBJECT_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxgraph_object ) {

  if( igraphfactory.IsInitialized() ) {
    WARN( 0x001, "Cannot run unit test, graph factory already initialized." );
    XBREAK;
  }

  const char *pathbase = TestName;
  const vgx_Graph_vtable_t *igraph = (vgx_Graph_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Graph_t );

  const char *basedir = GetCurrentTestDirectory();
  vgx_context_t VGX_CONTEXT = {0};
  strncpy( VGX_CONTEXT.sysroot, basedir, 254 );


  /*******************************************************************//**
   * CREATE, TEST, THEN DESTROY THE GRAPH FACTORY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create, test, then destroy Graph Factory" ) {
    const char *name = "TestGraph1";
    const CString_t *CSTR__graph_name = CStringNew( name );
    const CString_t *CSTR__graph_path = CStringNewFormat( "%s/%s", pathbase, name );
    vgx_Graph_t *gx;
    vgx_Graph_t *graph = NULL;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    // Initialize graph factory
    TEST_ASSERTION( igraphfactory.Initialize( &VGX_CONTEXT, __VECTOR__MASK_FEATURE, false, false, NULL ) == 1, "Graph factory initialized" );
    // Create a new graph
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 );
    TEST_ASSERTION( graph != NULL,                                              "Graph constructed" );
    TEST_ASSERTION( CALLABLE(graph) == igraph,                                  "Graph vtable exists" );
    TEST_ASSERTION( graph->recursion_count == 1,                                "Graph opened once" );
    TEST_ASSERTION( GraphOrder(graph) == 0,                                  "Graph has no vertices" );
    TEST_ASSERTION( GraphSize(graph) == 0,                                   "Graph has no edges" );
    // Re-open the already open graph
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 );
    TEST_ASSERTION( graph != NULL,                                              "Graph recursively re-opened" );
    TEST_ASSERTION( graph->recursion_count == 2,                                "Graph opened twice" );
    // Close the twice-opened graph
    gx = graph;
    uint32_t owner;
    TEST_ASSERTION( igraphfactory.CloseGraph( &gx, &owner ) == 1,               "Graph closed once, still open" );
    TEST_ASSERTION( gx == NULL,                                                 "Graph pointer set to NULL" );
    TEST_ASSERTION( graph->recursion_count == 1,                                "Graph opened once" );
    // Try (and fail) to delete still open graph
    CString_t *CSTR__reason = NULL;
    TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 100, false, &CSTR__reason ) < 0,   "Graph open, not deleted" );
    TEST_ASSERTION( CSTR__reason != NULL,                                       "Error reason should be set" );
    CStringDelete( CSTR__reason );
    // Simulate different thread is owner of graph
    graph->owner_threadid = threadid + 1;
    TEST_ASSERTION( igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 ) == NULL, "Graph open by other thread, can't open" );
    TEST_ASSERTION( igraphfactory.CloseGraph( &gx, &owner ) < 0,                "Graph open by other thread, can't close" );
    graph->owner_threadid = threadid;
    // Close graph
    gx = graph;
    TEST_ASSERTION( igraphfactory.CloseGraph( &gx, &owner ) == 0,               "Graph closed" );
    TEST_ASSERTION( gx == NULL,                                                 "Graph pointer set to NULL" );
    TEST_ASSERTION( graph->recursion_count == 0,                                "Graph not open" );
    // Delete closed graph
    TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 100, false, NULL ) == 1,  "Graph deleted" );
    TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 100, false, NULL ) == 0,  "Nothing to delete" );
    // Destroy graph factory
    igraphfactory.Shutdown();
    // Clean up strings
    CStringDelete( CSTR__graph_name );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CREATE MANY GRAPHS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create many graphs, remove some, re-open and check" ) {
    unsigned N = 50;
    const CString_t *CSTR__graph_path = NULL;
    const CString_t *CSTR__graph_name = NULL;
    const CString_t *CSTR__initial = NULL;
    const CString_t *CSTR__terminal = NULL;
    const CString_t *CSTR__relationship = NULL;
    vgx_predicator_modifier_enum modifier;
    vgx_Graph_t *graph = NULL;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    uint32_t owner;

    // Initialize graph factory
    TEST_ASSERTION( igraphfactory.Initialize( &VGX_CONTEXT, __VECTOR__MASK_FEATURE, false, false, NULL ) == 1,       "Graph factory initialized" );

    // Create many graphs
    for( unsigned i=0; i<N; i++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%03d", i );
      CSTR__graph_path = CStringNewFormat( "%s/%03d", pathbase, i );
      graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 );
      TEST_ASSERTION( graph != NULL,                                                  "Graph constructed" );
      TEST_ASSERTION( CALLABLE(graph) == igraph,                                      "Graph vtable exists" );
      CStringDelete( CSTR__graph_name );
      CStringDelete( CSTR__graph_path );
      TEST_ASSERTION( graph->recursion_count == 1,                                    "Graph opened once" );
      TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,                "Graph closed" );
    }

    // Create something in each graph
    for( unsigned g=0; g<N; g++ ) {
      unsigned w = 0;
      unsigned n_arcs = 0;
      // Open graph
      CSTR__graph_name = CStringNewFormat( "Graph_%03d", g );
      CSTR__graph_path = CStringNewFormat( "%s/%03d", pathbase, g );
      graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 );
      TEST_ASSERTION( graph != NULL,                                                  "Graph opened" );
      TEST_ASSERTION( CALLABLE(graph) == igraph,                                      "Graph vtable exists" );
      vgx_Relation_t *relation = iRelation.New( graph, NULL, NULL, NULL, VGX_PREDICATOR_MOD_NONE, NULL );
      TEST_ASSERTION( relation != NULL,                                               "Relation created" );
      CStringDelete( CSTR__graph_name );
      CStringDelete( CSTR__graph_path );
      // Create vertices
      for( unsigned v=0; v<N; v++ ) {
        CSTR__initial = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "Vertex_%03d_%03d", g, v );
        igraph->simple->CreateVertexSimple( graph, CStringValue( CSTR__initial ), NULL );
        CStringDelete( CSTR__initial );
      }
      TEST_ASSERTION( GraphOrder(graph) == N,                                      "%d vertices", N );
      TEST_ASSERTION( GraphSize(graph) == 0,                                       "No edges" );
      // Create arcs
      while( w < N ) {
        for( unsigned v=w; v<N; v++ ) {
          CSTR__initial = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "Vertex_%03d_%03d", g, w );
          CSTR__terminal = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "Vertex_%03d_%03d", g, v );
          CSTR__relationship = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "relationship_%03d", (v*g) % 10 ); // 10 different kinds
          modifier = VGX_PREDICATOR_MOD_STATIC;
          iRelation.Set( relation, CStringValue(CSTR__initial), CStringValue(CSTR__terminal), CStringValue(CSTR__relationship), &modifier, NULL ); // steals the CStrings!
          TEST_ASSERTION( igraph->simple->Connect( graph, relation, -1, NULL, 100, NULL, NULL ) == 1,      "One arc created" );
          ++n_arcs;
          iRelation.Unset( relation );
          CStringDelete( CSTR__relationship );
          CStringDelete( CSTR__terminal );
          CStringDelete( CSTR__initial );
        }
        ++w;
      }
      TEST_ASSERTION( GraphOrder(graph) == N,                                      "%d vertices", N );
      TEST_ASSERTION( GraphSize(graph) == n_arcs,                                  "%d arcs", n_arcs );
      // Close graph
      TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,                "Graph closed" );

      iRelation.Delete( &relation );
    }

    // Delete half of the graphs
    for( unsigned g=0; g<N/2; g++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%03d", g );
      CSTR__graph_path = CStringNewFormat( "%s/%03d", pathbase, g );
      TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 100, false, NULL ) == 1,      "Graph %d deleted", g );
      CStringDelete( CSTR__graph_name );
      CStringDelete( CSTR__graph_path );
    }

    // Open all graphs and check
    for( unsigned g=0; g<N; g++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%03d", g );
      CSTR__graph_path = CStringNewFormat( "%s/%03d", pathbase, g );
      graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 100 );
      TEST_ASSERTION( graph != NULL,                                                  "Graph opened" );
      TEST_ASSERTION( CALLABLE(graph) == igraph,                                      "Graph vtable exists" );
      CStringDelete( CSTR__graph_name );
      CStringDelete( CSTR__graph_path );
      if( g < N/2 ) {
        TEST_ASSERTION( GraphOrder(graph) == 0,                                    "No vertices" );
        TEST_ASSERTION( GraphSize(graph) == 0,                                     "No arcs" );
      }
      else {
        TEST_ASSERTION( GraphOrder(graph) == N,                                    "%d vertices", N );
        TEST_ASSERTION( GraphSize(graph) > 0,                                      "arcs exist" );
      }
      TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,                "Graph close" );
    }

    // Delete all graphs
    for( unsigned i=0; i<N; i++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%03d", i );
      CSTR__graph_path = CStringNewFormat( "%s/%03d", pathbase, i );
      TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 100, false, NULL ) == 1,      "Graph %d deleted", i );
      CStringDelete( CSTR__graph_name );
      CStringDelete( CSTR__graph_path );
    }

    igraphfactory.Shutdown();

  } END_TEST_SCENARIO




} END_UNIT_TEST




#endif
