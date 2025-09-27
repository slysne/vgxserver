/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxarcvector_api__complete_add_remove.h
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

#ifndef __UTEST_VXARCVECTOR_API__COMPLETE_ADD_REMOVE_H
#define __UTEST_VXARCVECTOR_API__COMPLETE_ADD_REMOVE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxarcvector_api__complete_add_remove ) {
  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  const CString_t *CSTR___V = NULL;
  const CString_t *CSTR___A = NULL;
  const CString_t *CSTR___B = NULL;
  const CString_t *CSTR___C = NULL;

  vgx_Graph_t *graph = NULL;
  vgx_Graph_vtable_t *igraph = NULL;
  framehash_dynamic_t *dyn = NULL;
  vgx_Vertex_t *V = NULL, *A = NULL, *B = NULL, *C = NULL;
  vgx_ArcVector_cell_t *Vin = NULL, *Vout = NULL;
  vgx_ArcVector_cell_t *Ain = NULL, *Aout = NULL;
  vgx_ArcVector_cell_t *Bin = NULL, *Bout = NULL;
  vgx_ArcVector_cell_t *Cin = NULL, *Cout = NULL;

  f_Vertex_connect_event connect_event = _vxgraph_arc__connect_WL_reverse_WL;
  f_Vertex_disconnect_event disconnect_event = _vxgraph_arc__disconnect_WL_reverse_WL;

  vgx_predicator_mod_t M_STAT = { .bits = VGX_PREDICATOR_MOD_STATIC };
  vgx_predicator_val_t sval = { .integer = 0 };

  /*******************************************************************//**
   * CREATE A TEST GRAPH AND TEST VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Test Graph and Test Vertices" ) {
    vgx_Graph_constructor_args_t graph_args = {
      .CSTR__graph_path     = CSTR__graph_path,
      .CSTR__graph_name     = CSTR__graph_name,
      .vertex_block_order   = 16,
      .graph_t0             = __SECONDS_SINCE_1970(),
      .start_opcount        = 1000,
      .simconfig            = NULL,
      .with_event_processor = true,
      .idle_event_processor = false,
      .force_readonly       = false,
      .force_writable       = true,
      .local_only           = true
    };
    objectid_t obid = *CStringObid( graph_args.CSTR__graph_name );
    graph = COMLIB_OBJECT_NEW( vgx_Graph_t, &obid, &graph_args );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    igraph = CALLABLE(graph);
    dyn = &graph->arcvector_fhdyn;

    CSTR___V = NewEphemeralCString( graph, "V" );
    CSTR___A = NewEphemeralCString( graph, "A" );
    CSTR___B = NewEphemeralCString( graph, "B" );
    CSTR___C = NewEphemeralCString( graph, "C" );

    V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( V && A && B && C, "vertices constructed" );
    Vin = _vxvertex__get_vertex_inarcs( V );
    Vout = _vxvertex__get_vertex_outarcs( V );
    Ain = _vxvertex__get_vertex_inarcs( A );
    Aout = _vxvertex__get_vertex_outarcs( A );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Bout = _vxvertex__get_vertex_outarcs( B );
    Cin = _vxvertex__get_vertex_inarcs( C );
    Cout = _vxvertex__get_vertex_outarcs( C );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * MULTIPLE ARCS BETWEEN MANY ARCS ARE ADDED UP TO MAXIMUM CAPACITY OF
   * PREDICATORS, THEN REMOVED
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove Multiple Arcs between many vertices" ) {
    vgx_Arc_t arc;
    int64_t n_arcs = 0;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    // Add V->A, V->B, V->C, A->B, B->C
    vgx_predicator_rel_enum test1 = _VGX_PREDICATOR_REL_TEST1;
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, A, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test1)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, B, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test1)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, C, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test1)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test1)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, C, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test1)->C" );
    n_arcs += 5;

    EXPECT_ARRAY_OF_ARCS(         "V->[A,B,C]",               Vout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->A",       dyn,  Vout, A, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->B",       dyn,  Vout, B, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->C",       dyn,  Vout, C, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "A-(test1)->B",             Aout, B, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "B-(test1)->C",             Bout, C, test1,  M_STAT,  sval        );
    EXPECT_NO_ARCS(               "C -> None",                Cout                  );
    EXPECT_NO_ARCS(               "rev V -> None",            Vin                   );
    EXPECT_SIMPLE_ARC(            "rev A-(test1)->V",         Ain,  V, test1,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[V,A]",             Bin,                2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->V",    dyn, Bin,  V, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->A",    dyn, Bin,  A, test1,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[V,B]",             Cin,                2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test1)->V",    dyn, Cin,  V, test1,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test1)->B",    dyn, Cin,  B, test1,  M_STAT,  sval        );

    // Make fully connected A->C, A->V, B->V, B->A, C->V, C->A, C->B
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test1)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, V, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test1)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, V, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test1)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, A, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test1)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, V, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test1)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test1)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, test1, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test1)->B" );
    n_arcs += 7;
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B,C]",               Vout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->A",       dyn,  Vout, A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->B",       dyn,  Vout, B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test1)->C",       dyn,  Vout, C, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,C,V]",               Aout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test1)->B",       dyn,  Aout, B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test1)->C",       dyn,  Aout, C, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test1)->V",       dyn,  Aout, V, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "B->[C,V,A]",               Bout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test1)->C",       dyn,  Bout, C, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test1)->V",       dyn,  Bout, V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test1)->A",       dyn,  Bout, A, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,A,B]",               Cout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->V",       dyn,  Cout, V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->A",       dyn,  Cout, A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->B",       dyn,  Cout, B, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,B,C]",           Vin,                3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->A",    dyn, Vin,  A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->B",    dyn, Vin,  B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->C",    dyn, Vin,  C, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[B,C,V]",           Ain,                3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test1)->B",    dyn, Ain,  B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test1)->C",    dyn, Ain,  C, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test1)->V",    dyn, Ain,  V, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[C,V,A]",           Bin,                3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->C",    dyn, Bin,  C, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->V",    dyn, Bin,  V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->A",    dyn, Bin,  A, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[V,A,B]",           Cin,                3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test1)->V",    dyn, Cin,  V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test1)->A",    dyn, Cin,  A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test1)->B",    dyn, Cin,  B, test1,  M_STAT,  sval     );

    // Add second predicator to some arcs in the already fully connected graph
    // Add V->A, V->B, V->C, A->B, A->C, B->C
    vgx_predicator_rel_enum test2 = _VGX_PREDICATOR_REL_TEST2;
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, A, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test2)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, B, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test2)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, C, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(test2)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test2)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test2)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, C, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test2)->C" );
    n_arcs += 6;
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,=>2B,=>2C]",              Vout,               6 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>A",         dyn,  Vout, A,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,            2 );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,=>2C,V]",                 Aout,               5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test1)->V",               dyn,  Aout, V, test1,  M_STAT,  sval     );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>B",         dyn,  Aout, B,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,            2 );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2C,V,A]",                    Bout,               4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>C",         dyn,  Bout, C,            2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test1)->V",               dyn,  Bout, V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test1)->A",               dyn,  Bout, A, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,A,B]",                       Cout,               3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->V",               dyn,  Cout, V, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->A",               dyn,  Cout, A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test1)->B",               dyn,  Cout, B, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,B,C]",                   Vin,                3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->A",           dyn,  Vin,  A, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->B",           dyn,  Vin,  B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test1)->C",           dyn,  Vin,  C, test1,  M_STAT,  sval     );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[B,C,=>2V]",                Ain,                4 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test1)->B",           dyn,  Ain,  B, test1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test1)->C",           dyn,  Ain,  C, test1,  M_STAT,  sval     );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>V",     dyn,  Ain,  V,            2 );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[C,=>2V,=>2A]",             Bin,                5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test1)->C",           dyn,  Bin,  C, test1,  M_STAT,  sval     );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>A",     dyn,  Bin,  A,            2 );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A,=>2B]",          Cin,                6 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,            2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>B",     dyn,  Cin,  B,            2 );

    // Add second predicator to the rest of the fully connected graph
    // Add A->V, B->V, B->A, C->V, C->A, C->B
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, V, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(test2)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, V, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test2)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, A, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(test2)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, V, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test2)->V" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test2)->A" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, test2, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(test2)->B" );
    n_arcs += 6;

    int64_t degree = 6;
    int64_t sz_multi = 2;
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,=>2B,=>2C]",              Vout,               degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>A",         dyn,  Vout, A,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,=>2C,=>2V]",              Aout,               degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>V",         dyn,  Aout, V,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>B",         dyn,  Aout, B,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2C,=>2V,=>2A]",              Bout,               degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>C",         dyn,  Bout, C,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>V",         dyn,  Bout, V,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>A",         dyn,  Bout, A,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>2V,=>2A,=>2B]",              Cout,               degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>V",         dyn,  Cout, V,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>A",         dyn,  Cout, A,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>B",         dyn,  Cout, B,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2A,=>2B,=>2C]",          Vin,                degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>A",     dyn,  Vin,  A,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>B",     dyn,  Vin,  B,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>C",     dyn,  Vin,  C,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2B,=>2C,=>2V]",          Ain,                degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>B",     dyn,  Ain,  B,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>C",     dyn,  Ain,  C,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>V",     dyn,  Ain,  V,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2C,=>2V,=>2A]",          Bin,                degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>C",     dyn,  Bin,  C,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>A",     dyn,  Bin,  A,          sz_multi );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A,=>2B]",          Cin,                degree );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,          sz_multi );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>B",     dyn,  Cin,  B,          sz_multi );

    // Add multiarc predicators to full capacity

    for( int rel = __VGX_PREDICATOR_REL_START_SYS_RANGE; rel <= __VGX_PREDICATOR_REL_END_USER_RANGE; rel++ ) {
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, A, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(0x%0X)->A", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, B, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(0x%0X)->B", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, C, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(0x%0X)->C", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(0x%0X)->B", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(0x%0X)->C", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, V, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(0x%0X)->V", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, C, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(0x%0X)->C", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, V, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(0x%0X)->V", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, A, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(0x%0X)->A", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, V, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(0x%0X)->V", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(0x%0X)->A", rel );
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(0x%0X)->B", rel );
      n_arcs += 12;
      degree += 3;
      sz_multi += 1;
      EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,=>2B,=>2C]",              Vout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,=>2C,=>2V]",              Aout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "B->[=>2C,=>2V,=>2A]",              Bout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "C->[=>2V,=>2A,=>2B]",              Cout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2A,=>2B,=>2C]",          Vin,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2B,=>2C,=>2V]",          Ain,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2C,=>2V,=>2A]",          Bin,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A,=>2B]",          Cin,                degree );
      if( rel < __VGX_PREDICATOR_REL_START_USER_RANGE ||
          rel % 1187 == 0 ||
          rel > __VGX_PREDICATOR_REL_END_USER_RANGE - 10 ) 
      { // <= do sample checks for deep counts (otherwise the test takes too long)
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>A",         dyn,  Vout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>V",         dyn,  Aout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>B",         dyn,  Aout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>C",         dyn,  Bout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>V",         dyn,  Bout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>A",         dyn,  Bout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>V",         dyn,  Cout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>A",         dyn,  Cout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>B",         dyn,  Cout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>A",     dyn,  Vin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>B",     dyn,  Vin,  B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>C",     dyn,  Vin,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>B",     dyn,  Ain,  B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>C",     dyn,  Ain,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>V",     dyn,  Ain,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>C",     dyn,  Bin,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>A",     dyn,  Bin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>B",     dyn,  Cin,  B,          sz_multi );
      }
    }

    // Check total connectivity
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Try to re-add an existing arc
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, A, test1, VGX_ARCDIR_OUT ), connect_event ) == 0, "Nothing is added, V-(test1)->A already exists" );
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Remove all but two
    for( int rel = __VGX_PREDICATOR_REL_START_SYS_RANGE; rel <= __VGX_PREDICATOR_REL_END_USER_RANGE; rel++ ) {
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, A, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(0x%0X)->A", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(0x%0X)->B", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, C, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(0x%0X)->C", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(0x%0X)->B", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, C, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(0x%0X)->C", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, V, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(0x%0X)->V", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, C, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(0x%0X)->C", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, V, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(0x%0X)->V", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, A, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(0x%0X)->A", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, V, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(0x%0X)->V", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, A, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(0x%0X)->A", rel );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(0x%0X)->B", rel );
      n_arcs -= 12;
      degree -= 3;
      sz_multi -= 1;
      EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,=>2B,=>2C]",              Vout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,=>2C,=>2V]",              Aout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "B->[=>2C,=>2V,=>2A]",              Bout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "C->[=>2V,=>2A,=>2B]",              Cout,               degree );
      EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2A,=>2B,=>2C]",          Vin,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2B,=>2C,=>2V]",          Ain,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2C,=>2V,=>2A]",          Bin,                degree );
      EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A,=>2B]",          Cin,                degree );

      if( rel < __VGX_PREDICATOR_REL_START_USER_RANGE ||
          rel % 1187 == 0 ||
          rel > __VGX_PREDICATOR_REL_END_USER_RANGE - 10 ) 
      { // <= do sample checks for deep counts (otherwise the test takes too long)
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>A",         dyn,  Vout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>V",         dyn,  Aout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>B",         dyn,  Aout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>C",         dyn,  Bout, C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>V",         dyn,  Bout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>A",         dyn,  Bout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>V",         dyn,  Cout, V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>A",         dyn,  Cout, A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>B",         dyn,  Cout, B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>A",     dyn,  Vin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>B",     dyn,  Vin,  B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>C",     dyn,  Vin,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>B",     dyn,  Ain,  B,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>C",     dyn,  Ain,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>V",     dyn,  Ain,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>C",     dyn,  Bin,  C,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>A",     dyn,  Bin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,          sz_multi );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>B",     dyn,  Cin,  B,          sz_multi );
      }
    }

    // Check total connectivity
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Remove some predicators to convert some arcs back to simple
    // A-(test1)->B, B-(test1)->C, C-(test1)->V, V-(test1)->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, B, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test1)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, C, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(test1)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, V, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test1)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, A, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test1)->A" );
    n_arcs -= 4;
    EXPECT_ARRAY_OF_ARCS(         "V->[A,=>2B,=>2C]",                 Vout,             5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test2)->A",               dyn,  Vout, A, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,          2 );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,=>2C,=>2V]",                 Aout,             5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>V",         dyn,  Aout, V,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test2)->B",               dyn,  Aout, B, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,          2 );
    EXPECT_ARRAY_OF_ARCS(         "B->[C,=>2V,=>2A]",                 Bout,             5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(test2)->C",               dyn,  Bout, C, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>V",         dyn,  Bout, V,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>A",         dyn,  Bout, A,          2 );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,=>2A,=>2B]",                 Cout,             5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test2)->V",               dyn,  Cout, V, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>A",         dyn,  Cout, A,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>B",         dyn,  Cout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2A,=>2B,C]",             Vin,              5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>A",     dyn,  Vin,  A,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>B",     dyn,  Vin,  B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test2)->C",           dyn,  Vin,  C, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2B,=>2C,V]",             Ain,              5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>B",     dyn,  Ain,  B,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>C",     dyn,  Ain,  C,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test2)->V",           dyn,  Ain,  V, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2C,=>2V,A]",             Bin,              5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>C",     dyn,  Bin,  C,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test2)->A",           dyn,  Bin,  A, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A,B]",             Cin,              5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test2)->B",           dyn,  Cin,  B, test2,  M_STAT,  sval      );

    // Check total connectivity
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Remove more predicators to convert more arcs back to simple and some to no arcs
    // A-(test2)->B, B-(test2)->C, B-(all)->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, B, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test2)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, C, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(test2)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, A, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 2, "Remove B-(test1,test2)->A" );
    n_arcs -= 4;
    EXPECT_ARRAY_OF_ARCS(         "V->[A,=>2B,=>2C]",                 Vout,             5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test2)->A",               dyn,  Vout, A, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>B",         dyn,  Vout, B,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(test1,test2)=>C",         dyn,  Vout, C,          2 );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2C,=>2V]",                   Aout,             4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>V",         dyn,  Aout, V,          2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No A -> B",                  dyn,  Aout, B             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(test1,test2)=>C",         dyn,  Aout, C,          2 );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2V]",                        Bout,             2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No B -> C",                  dyn,  Bout, C             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(test1,test2)=>V",         dyn,  Bout, V,          2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No B -> A",                  dyn,  Bout, A             );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,=>2A,=>2B]",                 Cout,             5 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test2)->V",               dyn,  Cout, V, test2,  M_STAT,  sval      );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>A",         dyn,  Cout, A,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(test1,test2)=>B",         dyn,  Cout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2A,=>2B,C]",             Vin,              5 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>A",     dyn,  Vin,  A,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(test1,test2)=>B",     dyn,  Vin,  B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test2)->C",           dyn,  Vin,  C, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2C,V]",                  Ain,              3 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev A -> B",              dyn,  Ain,  B             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(test1,test2)=>C",     dyn,  Ain,  C,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test2)->V",           dyn,  Ain,  V, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2C,=>2V]",               Bin,              4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>C",     dyn,  Bin,  C,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(test1,test2)=>V",     dyn,  Bin,  V,          2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev B -> A",              dyn,  Bin,  A             );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2V,=>2A]",               Cin,              4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>V",     dyn,  Cin,  V,          2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(test1,test2)=>A",     dyn,  Cin,  A,          2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev C -> B",              dyn,  Cin,  B             );

    // Check total connectivity
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Remove rest of TEST1 predicators (execute remove for all and expect non-existent to not remove anything)
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, B, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent A-(test1)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, C, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test1)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, V, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test1)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, C, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent B-(test1)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, V, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(test1)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, A, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent B-(test1)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, V, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent C-(test1)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, A, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test1)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, B, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test1)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, A, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent V-(test1)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, B, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test1)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, C, test1, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test1)->C" );
    n_arcs -= 7;
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B,C]",                       Vout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test2)->A",               dyn,  Vout, A, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test2)->B",               dyn,  Vout, B, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(test2)->C",               dyn,  Vout, C, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "A->[C,V]",                         Aout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test2)->V",               dyn,  Aout, V, test2,  M_STAT,  sval      );
    EXPECT_NO_ARC_IN_ARRAY(       "No A -> B",                  dyn,  Aout, B             );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(test2)->C",               dyn,  Aout, C, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC(            "B->V",                             Bout, V, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,A,B]",                       Cout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test2)->V",               dyn,  Cout, V, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test2)->A",               dyn,  Cout, A, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(test2)->B",               dyn,  Cout, B, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,B,C]",                   Vin,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test2)->A",           dyn,  Vin,  A, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test2)->B",           dyn,  Vin,  B, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(test2)->C",           dyn,  Vin,  C, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[C,V]",                     Ain,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev A -> B",              dyn,  Ain,  B             );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test2)->C",           dyn,  Ain,  C, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(test2)->V",           dyn,  Ain,  V, test2,  M_STAT,  sval      );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[C,V]",                     Bin,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test2)->C",           dyn,  Bin,  C, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(test2)->V",           dyn,  Bin,  V, test2,  M_STAT,  sval      );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev B -> A",              dyn,  Bin,  A             );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[V,A]",                     Cin,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test2)->V",           dyn,  Cin,  V, test2,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(test2)->A",           dyn,  Cin,  A, test2,  M_STAT,  sval      );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev C -> B",              dyn,  Cin,  B             );

    // Check total connectivity
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    // Remove rest of TEST2 predicators (execute remove for all and expect non-existent to not remove anything)
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, B, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent A-(test2)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, C, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test2)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, A, V, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove A-(test2)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, C, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent B-(test2)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, V, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove B-(test2)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, B, A, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 0, "No removal of non-existent B-(test2)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, V, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test2)->V" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, A, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test2)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, C, B, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove C-(test2)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, A, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test2)->A" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, B, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test2)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY( &arc, V, C, test2, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_event ) == 1, "Remove V-(test2)->C" );
    n_arcs -= 9;

    // Check total connectivity
    TEST_ASSERTION( n_arcs == 0, "No arcs" );
    do {
      int64_t n_in = 0;
      int64_t n_out = 0;
      n_out += iarcvector.Degree( Vout );
      n_out += iarcvector.Degree( Aout );
      n_out += iarcvector.Degree( Bout );
      n_out += iarcvector.Degree( Cout );
      n_in += iarcvector.Degree( Vin );
      n_in += iarcvector.Degree( Ain );
      n_in += iarcvector.Degree( Bin );
      n_in += iarcvector.Degree( Cin );
      TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld out=%lld, arcs=%lld", n_in, n_out, n_arcs );
      TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() reported" );
    } WHILE_ZERO;

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY ALL THE TEST VERTICES AND THE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Vertices and Test Graph" ) {
    CALLABLE(graph)->simple->DeleteVertex( graph, CSTR___V, 0, NULL, NULL );
    CALLABLE(graph)->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    CALLABLE(graph)->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );
    CALLABLE(graph)->simple->DeleteVertex( graph, CSTR___C, 0, NULL, NULL );
    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );

    COMLIB_OBJECT_DESTROY(graph);
  } END_TEST_SCENARIO

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );


} END_UNIT_TEST


#endif
