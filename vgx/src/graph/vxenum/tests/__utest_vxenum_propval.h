/*######################################################################
 *#
 *# __utest_vxenum_propval.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXENUM_PROPVAL_H
#define __UTEST_VXENUM_PROPVAL_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxenum_propval ) {

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
    _vxenum_prop__destroy_enumerator( graph );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    // Re-create enumerator for graph destructor to work properly
    TEST_ASSERTION( _vxenum_prop__create_enumerator( graph ) == 0 , "" );
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
