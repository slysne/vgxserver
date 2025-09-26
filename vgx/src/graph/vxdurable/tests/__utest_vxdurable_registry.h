/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxdurable_registry.h
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

#ifndef __UTEST_VXDURABLE_REGISTRY_H
#define __UTEST_VXDURABLE_REGISTRY_H

#include "__vxtest_macro.h"

static vgx_Graph_t * __retrieve_graph_by_path_and_name_OPEN( const CString_t *CSTR__path, const CString_t *CSTR__name) {
  vgx_Graph_t *graph;
  REGISTRY_LOCK {
    graph = __retrieve_graph_by_path_and_name_RCS( CSTR__path, CSTR__name );
  } REGISTRY_RELEASE;
  return graph;
}
#define retrieve_graph_by_path_and_name( Path, Name ) __retrieve_graph_by_path_and_name_OPEN( Path, Name )


static int __del_graph_OPEN( vgx_Graph_t **graph, bool erase ) {
  int ret;
  REGISTRY_LOCK {
    ret = __del_graph_RCS( graph, erase );
  } REGISTRY_RELEASE;
  return ret;
}
#define del_graph( GraphPtrPtr, BoolErase ) __del_graph_OPEN( GraphPtrPtr, BoolErase )



BEGIN_UNIT_TEST( __utest_vxdurable_registry ) {

  if( igraphfactory.IsInitialized() ) {
    WARN( 0x001, "Cannot run unit test, graph factory already initialized." );
    XBREAK;
  }


  const CString_t *CSTR__graph_path = CStringNew( TestName );
  int sz_reg_pre = 0;

  const char *basedir = GetCurrentTestDirectory();

  vgx_context_t VGX_CONTEXT = {0};
  strncpy( VGX_CONTEXT.sysroot, basedir, 254 );

  /*******************************************************************//**
   * CREATE AND DESTROY REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and destroy graph registry" ) {
    TEST_ASSERTION( igraphfactory.IsInitialized() == false,                     "Graph registry not initialized" );
    TEST_ASSERTION( g_graph_registry == NULL,                                   "Graph registry does not exist" );
    TEST_ASSERTION( CSTR__g_system_root == NULL,                                "Graph system root path not set" );
    TEST_ASSERTION( igraphfactory.Initialize( &VGX_CONTEXT, __VECTOR__MASK_FEATURE, false, false, NULL ) == 1, "Graph registry initialized" );
    TEST_ASSERTION( igraphfactory.IsInitialized() == true,                      "Graph registry is initialized" );
    TEST_ASSERTION( g_graph_registry != NULL,                                   "Graph registry exists" );
    TEST_ASSERTION( CSTR__g_system_root != NULL,                                "Graph system root path set" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__g_system_root, basedir ),         "Graph system root path correct" );
    TEST_ASSERTION( igraphfactory.SystemRoot() == CSTR__g_system_root,          "Graph system root path returned" );
    igraphfactory.Shutdown();
    TEST_ASSERTION( igraphfactory.IsInitialized() == false,                     "Graph registry not initialized" );
    TEST_ASSERTION( g_graph_registry == NULL,                                   "Graph registry does not exist" );
    TEST_ASSERTION( CSTR__g_system_root == NULL,                                "Graph system root path not set" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create new graph registry" ) {
    TEST_ASSERTION( g_graph_registry == NULL,                                   "Graph registry does not exist" );
    TEST_ASSERTION( igraphfactory.Initialize( &VGX_CONTEXT, __VECTOR__MASK_FEATURE, false, false, NULL ) == 1, "Graph registry initialized" );
    sz_reg_pre = igraphfactory.Size();
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ADD NEW GRAPH TO REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Add new graph to registry" ) {
    vgx_Graph_t *graph1 = NULL;
    vgx_Graph_t *graph2 = NULL;
    const CString_t *CSTR__graph_name = CStringNew( "TestGraph" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                         "Graph registry initial size" );
    graph1 = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    TEST_ASSERTION( graph1 != NULL,                                             "Graph created" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + 1,                     "Graph registry has 1 additional graph" );
    graph2 = retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR__graph_name );
    TEST_ASSERTION( graph2 == graph1,                                           "Graph retrieved" );
    TEST_ASSERTION( del_graph( &graph1, false ) == 1,                           "Deleted 1 graph" );
    TEST_ASSERTION( graph1 == NULL,                                             "Graph deleted" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                         "Graph registry initial size" );
    graph2 = retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR__graph_name );
    TEST_ASSERTION( graph2 == NULL,                                             "Graph does not exist in registry" );
    TEST_ASSERTION( del_graph( &graph2, false ) == 0,                           "Graph does not exist in registry" );
    CStringDelete( CSTR__graph_name );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * VERIFY NEW GRAPH LOGIC
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Verify new graph logic" ) {
    vgx_Graph_t *graph1 = NULL;
    vgx_Graph_t *graph2 = NULL;
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                         "Graph registry initial size" );
    const CString_t *CSTR__graph_name = CStringNew( "RegistryTestGraph" );
    graph1 = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    graph2 = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    TEST_ASSERTION( graph1 == graph2,                                           "Existing graph retrieved (not creating new)" );
    TEST_ASSERTION( del_graph( &graph1, false ) == 1,                           "Deleted 1 graph" );
    TEST_ASSERTION( graph1 == NULL,                                             "Graph deleted" );
    CStringDelete( CSTR__graph_name );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ADD MULTIPLE GRAPHS TO REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Add multiple graphs to registry" ) {
    vgx_Graph_t *graphs[] = {NULL, NULL, NULL};
    vgx_Graph_t *graph = NULL;
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                     "Graph registry initial size" );
    const CString_t *CSTR___G1 = CStringNew( "G1" );
    const CString_t *CSTR___G2 = CStringNew( "G2" );
    const CString_t *CSTR___G3 = CStringNew( "G3" );
    graphs[0] = igraphfactory.NewGraph( CSTR__graph_path, CSTR___G1, true, NULL );
    graphs[1] = igraphfactory.NewGraph( CSTR__graph_path, CSTR___G2, true, NULL );
    graphs[2] = igraphfactory.NewGraph( CSTR__graph_path, CSTR___G3, true, NULL );

    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + 3,                 "Graph registry has 3 additional graphs" );
    TEST_ASSERTION( retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR___G1 ) == graphs[0], "Retrieved graph" );
    TEST_ASSERTION( retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR___G2 ) == graphs[1], "Retrieved graph" );
    TEST_ASSERTION( retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR___G3 ) == graphs[2], "Retrieved graph" );
    TEST_ASSERTION( del_graph( &graphs[0], false ) == 1,                    "Deleted 1 graph" );
    TEST_ASSERTION( graphs[0] == NULL,                                      "Graph deleted" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + 2,                 "Graph registry has 2 graphs" );
    TEST_ASSERTION( del_graph( &graphs[1], false ) == 1,                    "Deleted 1 graph" );
    TEST_ASSERTION( graphs[1] == NULL,                                      "Graph deleted" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + 1,                 "Graph registry has 1 graph" );
    TEST_ASSERTION( del_graph( &graphs[2], false ) == 1,                    "Deleted 1 graph" );
    TEST_ASSERTION( graphs[2] == NULL,                                      "Graph deleted" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                     "Graph registry empty" );
    CStringDelete( CSTR___G1 );
    CStringDelete( CSTR___G2 );
    CStringDelete( CSTR___G3 );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ADD A LARGE NUMBER OF GRAPHS TO REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Add a large number of graphs to registry" ) {
    int N = 100;
    const CString_t *CSTR__graph_name = NULL;
    vgx_Graph_t **graphs = calloc( N, sizeof(vgx_Graph_t*) );
    vgx_Graph_t *graph = NULL;
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                     "Graph registry initial size" );

    COMLIB_MuteOutputStream();
    for( int i=0; i<N; i++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%d", i );
      graphs[i] = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
      CStringDelete( CSTR__graph_name );
    }
    COMLIB_UnmuteOutputStream();
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + N,                 "Graph registry has %d graphs", N );

    COMLIB_MuteOutputStream();
    for( int i=0; i<N; i++ ) {
      CSTR__graph_name = CStringNewFormat( "Graph_%d", i );
      TEST_ASSERTION( retrieve_graph_by_path_and_name( CSTR__graph_path, CSTR__graph_name ) == graphs[i],   "Retrieved graph" );
      CStringDelete( CSTR__graph_name );
    }
    COMLIB_UnmuteOutputStream();

    COMLIB_MuteOutputStream();
    for( int i=0; i<N; i++ ) {
      TEST_ASSERTION( del_graph( &graphs[i], false ) == 1,                  "Deleted 1 graph" );
      TEST_ASSERTION( graphs[i] == NULL,                                    "Graph deleted" );
    }
    COMLIB_UnmuteOutputStream();
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                     "Graph registry is empty" );
    free( graphs );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ADD AND REMOVE GRAPH MANY TIMES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Add and remove graph many times" ) {
    int N = 200;
    vgx_Graph_t *graph = NULL;
    const CString_t *CSTR__graph_name = CStringNew( "name" );
    TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                     "Graph registry empty" );
    COMLIB_MuteOutputStream();
    for( int i=0; i<N; i++ ) {
      graph = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
      TEST_ASSERTION( graph != NULL,                                        "Graph created" );
      TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre + 1,               "Graph registry has 1 graph" );
      TEST_ASSERTION( del_graph( &graph, false ) == 1,                      "Deleted 1 graph" );
      TEST_ASSERTION( graph == NULL,                                        "Graph deleted" );
      TEST_ASSERTION( igraphfactory.Size() == sz_reg_pre,                   "Graph registry empty" );
    }
    COMLIB_UnmuteOutputStream();
    CStringDelete( CSTR__graph_name );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy graph registry" ) {
    TEST_ASSERTION( g_graph_registry != NULL,                                   "Graph registry exists" );
    igraphfactory.Shutdown();
    TEST_ASSERTION( g_graph_registry == NULL,                                   "Graph registry does not exist" );
  } END_TEST_SCENARIO

  CStringDelete( CSTR__graph_path );

} END_UNIT_TEST




#endif
