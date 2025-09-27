/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxgraph_mapping.h
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

#ifndef __UTEST_VXGRAPH_MAPPING_H
#define __UTEST_VXGRAPH_MAPPING_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxgraph_mapping ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();
  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );

  /*******************************************************************//**
   * CREATE AND DESTROY MAPPING
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and destroy mapping" ) {
    TEST_ASSERTION( graph != NULL,                                                  "Created test graph" );
    CString_t *CSTR__mapping = CStringNew( "test_mapping" );
    vgx_mapping_spec_t spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;
    framehash_t *map = _vxgraph_mapping__new_map( graph->CSTR__fullpath, CSTR__mapping, 100, -1, spec, CLASS_NONE );
    TEST_ASSERTION( map != NULL,                                                    "Created map" );
    framehash_vtable_t *imap = CALLABLE(map);
    TEST_ASSERTION( imap->Items(map) == 0,                                          "Map is empty" );
    _vxgraph_mapping__delete_map( &map );
    CStringDelete( CSTR__mapping );
  } END_TEST_SCENARIO

  CALLABLE( graph )->advanced->CloseOpenVertices( graph );
  CALLABLE( graph )->simple->Truncate( graph, NULL );
  uint32_t owner;
  igraphfactory.CloseGraph( &graph, &owner );

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST




#endif
