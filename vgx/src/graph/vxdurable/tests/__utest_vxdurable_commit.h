/*######################################################################
 *#
 *# __utest_vxdurable_commit.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXDURABLE_COMMIT_H
#define __UTEST_VXDURABLE_COMMIT_H

#include "__vxtest_macro.h"




BEGIN_UNIT_TEST( __utest_vxdurable_commit ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  vgx_Graph_t *graph = NULL;


  /*******************************************************************//**
   * CREATE, DESTROY, CREATE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    uint32_t owner;
    igraphfactory.CloseGraph( &graph, &owner );
    TEST_ASSERTION( graph == NULL, "graph deleted" );
    graph = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Vertex Construction/Destruction" ) {
    vgx_Graph_vtable_t *igraph = CALLABLE(graph);
    CString_t *CSTR__A = NewEphemeralCString( graph, "A" );
    vgx_Vertex_t *A = NULL;
    const objectid_t *pobidA, *pobid = NULL;
    
    pobidA = CStringObid( CSTR__A );

    // Create Vertex and verify it exists in the graph index
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL,  "Vertex A is created" );
    pobid = COMLIB_OBJECT_GETID( A );
    TEST_ASSERTION( pobid != NULL,                                              "Vertex has objectid" );
    TEST_ASSERTION( idcmp( pobid, pobidA ) == 0,                                "Vertex objectid should be digest of ID string" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true,           "Vertex A is released" );
    TEST_ASSERTION( A == NULL,                                                  "Vertex A has been set to NULL" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL,  "Get Vertex indexed by ID string" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true,           "Vertex A is released" );
    TEST_ASSERTION( A == NULL,                                                  "Vertex A has been set to NULL" );
    
    // Destroy Vertex by ID string and verify it does not exist in the graph index
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__A, 0, NULL, NULL ) == 1,         "Vertex A deleted by ID string" );
    TEST_ASSERTION( igraph->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL ) == NULL,     "Vertex indexed by ID string should not exist" );

    CStringDelete( CSTR__A );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Vertices and Arcs" ) {
    vgx_Graph_vtable_t *igraph = CALLABLE(graph);
    int64_t degree;
    vgx_Vertex_t *A, *B, *C;
    const CString_t * CSTR__A = NewEphemeralCString( graph, "A" );
    const CString_t * CSTR__B = NewEphemeralCString( graph, "B" );
    const CString_t * CSTR__C = NewEphemeralCString( graph, "C" );
    const CString_t * CSTR__rel_test1 = NewEphemeralCString( graph, "rel_test1" );
    const CString_t * CSTR__rel_test2 = NewEphemeralCString( graph, "rel_test2" );
    vgx_Relation_t *relation = NULL;
    vgx_AdjacencyQuery_t *disconnect_query = NULL;
    vgx_ArcConditionSet_t *arc_condition_set = NULL;
    vgx_ArcCondition_t *arc_condition = NULL;
    vgx_VertexCondition_t *vertex_condition = NULL;
    int ret;

    // Add A-(test1)->B vvvvv
    relation = iRelation.New( graph, CStringValue( CSTR__A ), CStringValue( CSTR__B ), CStringValue( CSTR__rel_test1 ), VGX_PREDICATOR_MOD_STATIC, NULL );
    TEST_ASSERTION( relation != NULL,     "Relation created" );
    TEST_ASSERTION( igraph->simple->Connect( graph, relation, -1, NULL, 0, NULL, NULL ) == 1,         "A-(test1)->B" );
    // Graph: vertices=2, arcs=1
    TEST_ASSERTION( GraphOrder( graph ) == 2 && GraphSize( graph ) == 1,  "vertices=2, arcs=1" );
    // A is REAL, B is VIRTUAL
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, relation->initial.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "A is opened" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, relation->terminal.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "B is opened" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),                "Vertex A is REAL" );
    TEST_ASSERTION( __vertex_is_source( A ),                            "Vertex A is SOURCE" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( B ),             "Vertex B is VIRTUAL" );
    TEST_ASSERTION( __vertex_is_sink( B ),                              "Vertex B is SINK" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true,   "Vertex A released" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true,   "Vertex B released" );
    TEST_ASSERTION( A == NULL,                                          "Vertex A set to NULL" );
    TEST_ASSERTION( B == NULL,                                          "Vertex B set to NULL" );
    // 0-A-1-B-0
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, relation->initial.CSTR__name, NULL )) == 0,   "A has 0 inarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, relation->initial.CSTR__name, NULL )) == 1,  "A has 1 outarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, relation->initial.CSTR__name, NULL )) == 1,     "A has degree=1" );
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, relation->terminal.CSTR__name, NULL )) == 1,   "B has 1 inarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, relation->terminal.CSTR__name, NULL )) == 0,  "B has 0 outarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, relation->terminal.CSTR__name, NULL )) == 1,     "B has degree=1" );
    iRelation.Delete( &relation );

    // Add B-(test2)->C   vvvvvvv
    relation = iRelation.New( graph, CStringValue( CSTR__B ), CStringValue( CSTR__C ), CStringValue( CSTR__rel_test2 ), VGX_PREDICATOR_MOD_STATIC, NULL );
    TEST_ASSERTION( relation != NULL,     "Relation created" );
    TEST_ASSERTION( igraph->simple->Connect( graph, relation, -1, NULL, 0, NULL, NULL ) == 1,         "B-(test2)->C" );
    // Graph: vertices=3, arcs=2
    TEST_ASSERTION( GraphOrder( graph ) == 3 && GraphSize( graph ) == 2,  "vertices=3, arcs=2" );
    // B is REAL, C is VIRTUAL
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, relation->initial.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "B is opened" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, relation->terminal.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "B is opened" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),                "Vertex B is REAL" );
    TEST_ASSERTION( __vertex_is_internal( B ),                          "Vertex B is INTERNAL" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( C ),             "Vertex C is VIRTUAL" );
    TEST_ASSERTION( __vertex_is_sink( C ),                              "Vertex C is SINK" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true,   "Vertex B released" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true,   "Vertex C released" );
    TEST_ASSERTION( B == NULL,                                          "Vertex B set to NULL" );
    TEST_ASSERTION( C == NULL,                                          "Vertex C set to NULL" );
    // 1-B-1-C-0
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, relation->initial.CSTR__name, NULL )) == 1,   "B has 1 inarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, relation->initial.CSTR__name, NULL )) == 1,  "B has 1 outarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, relation->initial.CSTR__name, NULL )) == 2,     "B has degree=2" );
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, relation->terminal.CSTR__name, NULL )) == 1,   "C has 1 inarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, relation->terminal.CSTR__name, NULL )) == 0,  "C has 0 outarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, relation->terminal.CSTR__name, NULL )) == 1,     "C has degree=1" );
    iRelation.Delete( &relation );

    //TODO: add test that gets B's outbound (C)

    // Remove B-(test2)->C
    disconnect_query = iGraphQuery.NewAdjacencyQuery( graph, CStringValue( CSTR__B ), NULL );
    TEST_ASSERTION( disconnect_query != NULL,     "Neighborhood query created" );
    arc_condition_set = iArcConditionSet.NewEmpty( graph, true, VGX_ARCDIR_OUT );
    arc_condition = iArcCondition.New( graph, true, CStringValue( CSTR__rel_test2 ), VGX_PREDICATOR_MOD_STATIC, VGX_VALUE_ANY, NULL, NULL );
    iArcConditionSet.Add( arc_condition_set, &arc_condition );
    TEST_ASSERTION( arc_condition_set,             "Arc condition created" );
    vertex_condition = iVertexCondition.New( true );
    TEST_ASSERTION( vertex_condition,             "Vertex condition created" );
    ret = iVertexCondition.RequireIdentifier( vertex_condition, VGX_VALUE_EQU, CStringValue( CSTR__C ) );
    TEST_ASSERTION( ret == 0,                     "Vertex condition requires identifier" );
    CALLABLE( disconnect_query )->AddArcConditionSet( disconnect_query, &arc_condition_set );
    CALLABLE( disconnect_query )->AddVertexCondition( disconnect_query, &vertex_condition );
    TEST_ASSERTION( igraph->simple->Disconnect( graph, disconnect_query ) == 1,      "B -x-> C " );
    iGraphQuery.DeleteAdjacencyQuery( &disconnect_query );

    // Graph: vertices=2, arcs=1
    TEST_ASSERTION( GraphOrder( graph ) == 2 && GraphSize( graph ) == 1,  "vertices=2, arcs=1" );
    // B is REAL
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL,  "" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),                "Vertex B is REAL" );
    TEST_ASSERTION( __vertex_is_sink( B ),                              "Vertex B is SINK" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true,   "Vertex B released" );
    TEST_ASSERTION( B == NULL,                                          "Vertex B set to NULL" );
    // 1-B
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, CSTR__B, NULL )) == 1,   "B has 1 inarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, CSTR__B, NULL )) == 0,  "B has 0 outarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, CSTR__B, NULL )) == 1,     "B has degree=2" );
    // C does not exist
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR__C, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) == NULL, "C does not exist" );

    // Delete B
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__B, 0, NULL, NULL ) == 1,     "Convert B to VIRTUAL" );
    // Graph: vertices=2, arcs=1
    TEST_ASSERTION( GraphOrder( graph ) == 2 && GraphSize( graph ) == 1,        "vertices=2, arcs=1 " );
    // B is VIRTUAL
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "B is opened" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( B ),             "Vertex B is VIRTUAL" );
    TEST_ASSERTION( __vertex_is_sink( B ),                              "Vertex B is SINK" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true,   "Vertex B released" );
    TEST_ASSERTION( B == NULL,                                          "Vertex B set to NULL" );
    // 0-A-1-B-0
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, CSTR__A, NULL )) == 0,   "A has 0 inarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, CSTR__A, NULL )) == 1,  "A has 1 outarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, CSTR__A, NULL )) == 1,     "A has degree=1" );
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, CSTR__B, NULL )) == 1,   "B has 1 inarc" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, CSTR__B, NULL )) == 0,  "B has 0 outarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, CSTR__B, NULL )) == 1,     "B has degree=1" );


    // Remove A-(test1)->B
    disconnect_query = iGraphQuery.NewAdjacencyQuery( graph, CStringValue( CSTR__A ), NULL );
    TEST_ASSERTION( disconnect_query != NULL,     "Neighborhood query created" );
    arc_condition_set = iArcConditionSet.NewEmpty( graph, true, VGX_ARCDIR_OUT );
    arc_condition = iArcCondition.New( graph, true, CStringValue( CSTR__rel_test1 ), VGX_PREDICATOR_MOD_STATIC, VGX_VALUE_ANY, NULL, NULL );
    iArcConditionSet.Add( arc_condition_set, &arc_condition );
    TEST_ASSERTION( arc_condition_set,            "Arc condition created" );
    vertex_condition = iVertexCondition.New( true );
    TEST_ASSERTION( vertex_condition,             "Vertex condition created" );
    ret = iVertexCondition.RequireIdentifier( vertex_condition, VGX_VALUE_EQU, CStringValue( CSTR__B ) );
    TEST_ASSERTION( ret == 0,                     "Vertex condition requires identifier" );
    CALLABLE( disconnect_query )->AddArcConditionSet( disconnect_query, &arc_condition_set );
    CALLABLE( disconnect_query )->AddVertexCondition( disconnect_query, &vertex_condition );
    TEST_ASSERTION( igraph->simple->Disconnect( graph, disconnect_query ) == 1,      "A -x-> B" );
    iGraphQuery.DeleteAdjacencyQuery( &disconnect_query );

    // Graph: vertices=1, arcs=0
    TEST_ASSERTION( GraphOrder( graph ) == 1 && GraphSize( graph ) == 0,  "vertices=1, arcs=0" );
    // A is REAL
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) != NULL, "A is opened" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),                "Vertex A is REAL" );
    TEST_ASSERTION( __vertex_is_isolated( A ),                          "Vertex A is ISOLATED" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true,   "Vertex A released" );
    TEST_ASSERTION( A == NULL,                                          "Vertex A set to NULL" );
    // 0-A-0
    TEST_ASSERTION( (degree=igraph->simple->VertexInDegree( graph, CSTR__A, NULL )) == 0,   "A has 0 inarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexOutDegree( graph, CSTR__A, NULL )) == 0,  "A has 0 outarcs" );
    TEST_ASSERTION( (degree=igraph->simple->VertexDegree( graph, CSTR__A, NULL )) == 0,     "A has degree=0" );

    // B does not exist
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) == NULL, "B does not exist" );

    // Delete A
    TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__A, 0, NULL, NULL ) == 1, "Delete A" );
    // Graph: vertices=0, arcs=0
    TEST_ASSERTION( GraphOrder( graph ) == 0 && GraphSize( graph ) == 0, "vertices=0, arcs=0" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_READONLY, 0, NULL, NULL )) == NULL, "A does not exist" );



    CStringDelete( CSTR__A );
    CStringDelete( CSTR__B );
    CStringDelete( CSTR__C );
    CStringDelete( CSTR__rel_test1 );
    CStringDelete( CSTR__rel_test2 );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY THE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    uint32_t owner;
    igraphfactory.CloseGraph( &graph, &owner );
    TEST_ASSERTION( graph == NULL,    "graph deleted" );
  } END_TEST_SCENARIO


  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST






#endif
