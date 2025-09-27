/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxdurable_operation_buffers.h
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

#ifndef __UTEST_VXDURABLE_OPERATION_BUFFERS_H
#define __UTEST_VXDURABLE_OPERATION_BUFFERS_H

#include "__vxtest_macro.h"




BEGIN_UNIT_TEST( __utest_vxdurable_operation_buffers ) {

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
