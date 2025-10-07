/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxarcvector_api__basic_add_remove.h
 * Author:  Stian Lysne slysne.dev@gmail.com
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

#ifndef __UTEST_VXARCVECTOR_API__BASIC_ADD_REMOVE_H
#define __UTEST_VXARCVECTOR_API__BASIC_ADD_REMOVE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxarcvector_api__basic_add_remove ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  vgx_Graph_t *graph = NULL;
  vgx_Graph_vtable_t *igraph = NULL;
  framehash_dynamic_t *dyn = NULL;
  vgx_Vertex_t *V = NULL;
  vgx_ArcVector_cell_t *Vin = NULL;
  vgx_ArcVector_cell_t *Vout = NULL;
  vgx_Vertex_t *A = NULL;
  vgx_ArcVector_cell_t *Ain = NULL;
  vgx_ArcVector_cell_t *Aout = NULL;
  vgx_Vertex_t *B = NULL;
  vgx_ArcVector_cell_t *Bin = NULL;
  vgx_ArcVector_cell_t *Bout = NULL;
  vgx_Vertex_t *C = NULL;
  vgx_ArcVector_cell_t *Cin = NULL;
  vgx_ArcVector_cell_t *Cout = NULL;
  vgx_Vertex_t *D = NULL;
  vgx_ArcVector_cell_t *Din = NULL;
  vgx_ArcVector_cell_t *Dout = NULL;
  vgx_Vertex_t *E = NULL;
  vgx_ArcVector_cell_t *Ein = NULL;
  vgx_ArcVector_cell_t *Eout = NULL;
  

  f_Vertex_connect_event connect_event = _vxgraph_arc__connect_WL_reverse_WL;
  f_Vertex_disconnect_event disconnect_REV = _vxgraph_arc__disconnect_WL_reverse_WL;
  f_Vertex_disconnect_event disconnect_FWD = _vxgraph_arc__disconnect_WL_forward_WL;

  vgx_predicator_mod_t M_STAT = { .bits = VGX_PREDICATOR_MOD_STATIC };
  vgx_predicator_val_t sval = { .integer = 0 };


  /*******************************************************************//**
   * CREATE A TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Test Graph" ) {
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

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CREATE AND DESTROY A TEST VERTEX
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create TEST Vertex" ) {
    const CString_t *CSTR___TEST = NewEphemeralCString( graph, "TEST" );
    // Create TEST vertex
    vgx_Vertex_t *TEST = igraph->simple->OpenVertex( graph, CSTR___TEST, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( TEST != NULL, "TEST vertex acquired" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &TEST ) == true, "" );
    TEST_ASSERTION( TEST == NULL, "" );
    // Delete TEST vertex
    int n_deleted = igraph->simple->DeleteVertex( graph, CSTR___TEST, 0, NULL, NULL );
    TEST_ASSERTION( n_deleted == 1, "TEST vertex deleted" );
    CStringDelete( CSTR___TEST );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CREATE TEST VERTEX V
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Vertex V" ) {
    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( V != NULL, "vertex constructed" );
    Vin = _vxvertex__get_vertex_inarcs( V );
    Vout = _vxvertex__get_vertex_outarcs( V );
    TEST_ASSERTION( __arcvector_cell_type( Vin ) == VGX_ARCVECTOR_NO_ARCS, "vertex has no inarcs" );
    TEST_ASSERTION( __arcvector_cell_type( Vout ) == VGX_ARCVECTOR_NO_ARCS, "vertex has no outarcs" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( V == NULL, "" );
    CStringDelete( CSTR___V );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CREATE TEST VERTICES A, B, C, D
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Vertices A, B, C, D" ) {
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    const CString_t *CSTR___D = NewEphemeralCString( graph, "D" );
    const CString_t *CSTR___E = NewEphemeralCString( graph, "E" );

    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Ain = _vxvertex__get_vertex_inarcs( A );
    Aout = _vxvertex__get_vertex_outarcs( A );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Bout = _vxvertex__get_vertex_outarcs( B );
    C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Cin = _vxvertex__get_vertex_inarcs( C );
    Cout = _vxvertex__get_vertex_outarcs( C );
    D = igraph->simple->OpenVertex( graph, CSTR___D, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Din = _vxvertex__get_vertex_inarcs( D );
    Dout = _vxvertex__get_vertex_outarcs( D );
    E = igraph->simple->OpenVertex( graph, CSTR___E, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Ein = _vxvertex__get_vertex_inarcs( E );
    Eout = _vxvertex__get_vertex_outarcs( E );
    TEST_ASSERTION( A && B && C && D && E, "vertices are constructed" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &D ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &E ) == true, "" );

    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );
    CStringDelete( CSTR___D );
    CStringDelete( CSTR___E );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ONE SINGLE SIMPLE ARC IS ADDED THEN REMOVED
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove one Simple Arc" ) {
    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    
    // add arc V-(111)->A
    SET_STATIC_ARC( &arc, V, A, 111, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "V-(111)->A is added" );
    // make sure simple V->A and simple reverse V<-A
    EXPECT_SIMPLE_ARC(            "V-(111)->A",               Vout, A,  111,  M_STAT,   sval  );
    EXPECT_SIMPLE_ARC(            "rev A-(111)->V",           Ain,  V,  111,  M_STAT,   sval  );

    // again add arc V-(111)->A
    SET_STATIC_ARC( &arc, V, A, 111, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 0, "Nothing is added, V-(111)->A already exist" );
    // make sure simple V->A and simple reverse V<-A
    EXPECT_SIMPLE_ARC(            "V-(111)->A",               Vout, A,  111,  M_STAT,   sval  );
    EXPECT_SIMPLE_ARC(            "rev A-(111)->V",           Ain,  V,  111,  M_STAT,   sval  );

    // remove arc
    SET_ARC_REL_QUERY( &arc, V, A, 111, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "V-(111)->A is removed" );
    // make sure we are back to empty
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    // remove arc again
    SET_ARC_REL_QUERY( &arc, V, A, 111, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "Nothing is removed, V-(111)-> no longer exists" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * SEVERAL SIMPLE ARCS ARE ADDED THEN REMOVED
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove several Simple Arcs" ) {
    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

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

    // add first arc V-(1001)->A
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, A, 1001, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_SIMPLE_ARC(            "V-(1001)->A",              Vout, A,  1001,  M_STAT,    sval );
    EXPECT_SIMPLE_ARC(            "rev A-(1001)->V",          Ain,  V,  1001,  M_STAT,    sval );
    
    // add second arc V-(1002)->B
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, B, 1002, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B]",                 Vout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(1002)->B",        dyn,  Vout, B,  1002,  M_STAT,    sval );
    EXPECT_SIMPLE_ARC(            "rev B-(1002)->V",          Bin,  V,  1002,  M_STAT,    sval );

    // add third arc V-(1003)->C
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, C, 1003, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B,C]",               Vout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(1003)->C",        dyn,  Vout, C,  1003,  M_STAT,    sval );
    EXPECT_SIMPLE_ARC(            "rev C-(1003)->V",          Cin,  V,  1003,  M_STAT,    sval );

    // add fourth arc A-(1004)->B
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 1004, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_SIMPLE_ARC(            "A-(1004)->B",              Aout, B,  1004,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[V,A]",             Bin,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(1004)->A",    dyn,  Bin,  A,  1004,  M_STAT,    sval );

    // add fifth arc A-(1005)->V
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, V, 1005, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,V]",                 Aout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(1005)->V",        dyn,  Aout, V,  1005,  M_STAT,    sval );
    EXPECT_SIMPLE_ARC(            "rev V-(1005)->A",          Vin,  A,  1005,  M_STAT,    sval );

    // add sixth arc B-(1006)->V
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, V, 1006, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_SIMPLE_ARC(            "B-(1006)->V",              Bout, V,  1006,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,B]",             Vin,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(1006)->B",    dyn,  Vin,  B,  1006,  M_STAT,    sval );

    // add seventh arc B-(1007)->A
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, A, 1007, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "B->[V,A]",                 Bout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(1007)->A",        dyn,  Bout, A,  1007,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[V,B]",             Ain,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(1007)->B",    dyn,  Ain,  B,  1007,  M_STAT,    sval );

    // add eighth arc A-(1008)->C
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 1008, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,V,C]",               Aout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(1008)->C",        dyn,  Aout, C,  1008,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[V,A]",             Cin,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(1008)->A",    dyn,  Cin,  A,  1008,  M_STAT,    sval );

    // add ninth arc B-(1009)->C
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, C, 1009, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "B->[V,A,C]",               Bout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "B-(1009)->C",        dyn,  Bout, C,  1009,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[V,A,B]",           Cin,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev C-(1009)->B",    dyn,  Cin,  B,  1009,  M_STAT,    sval );

    // add tenth arc C-(1010)->V
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, V, 1010, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_SIMPLE_ARC(            "C-(1010)->V",              Cout, V,  1010,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,B,C]",           Vin,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(1010)->C",    dyn,  Vin,  C,  1010,  M_STAT,    sval );

    // add eleventh arc C-(1011)->A
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 1011, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,A]",                 Cout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(1011)->A",        dyn,  Cout, A,  1011,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[V,B,C]",           Ain,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev A-(1011)->C",    dyn,  Ain,  C,  1011,  M_STAT,    sval );

    // add twelfth arc C-(1012)->B
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 1012, VGX_ARCDIR_OUT ), connect_event );
    EXPECT_ARRAY_OF_ARCS(         "C->[V,A,B]",               Cout,             3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(1012)->B",        dyn,  Cout, B,  1012,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[V,A,C]",           Bin,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(1012)->C",    dyn,  Bin,  C,  1012,  M_STAT,    sval );

    // remove first arc V-(1001)->A
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, A, 1001, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_ARRAY_OF_ARCS(         "V->[B,C]",                 Vout,             2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No V -> A",          dyn,  Vout, A             );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[B,C]",             Ain,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev A -> V",      dyn,  Ain,  V             );
    
    // remove second arc V-(1002)->B
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, B, 1002, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_SIMPLE_ARC(            "V->C",                     Vout, C,  1003,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[A,C]",             Bin,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev B -> V",      dyn,  Bin,  V             );

    // remove third arc V-(1003)->C
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, C, 1003, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_NO_ARCS(               "V -> None",                Vout                );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[A,B]",             Cin,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev C -> V",      dyn,  Cin,  V             );

    // remove fourth arc A-(1004)->B
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 1004, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_ARRAY_OF_ARCS(         "A->[V,C]",                 Aout,             2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No A -> B",          dyn,  Aout, B             );
    EXPECT_SIMPLE_ARC(            "rev B->C",                 Bin,  C,    1012,  M_STAT,    sval );

    // remove fifth arc A-(1005)->V
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, V, 1005, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_SIMPLE_ARC(            "A->C",                     Aout, C,    1008,  M_STAT,    sval );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[B,C]",             Vin,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev V -> A",      dyn,  Vin,  A             );

    // remove sixth arc B-(1006)->V
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, V, 1006, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_ARRAY_OF_ARCS(         "B->[A,C]",                 Bout,             2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No B -> V",          dyn,  Bout, V             );
    EXPECT_SIMPLE_ARC(            "rev V->C",                 Vin,  C,    1010,  M_STAT,    sval );

    // remove seventh arc B-(1007)->A
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, A, 1007, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_SIMPLE_ARC(            "B->C",                     Bout, C,    1009,  M_STAT,    sval );
    EXPECT_SIMPLE_ARC(            "rev A->C",                 Ain,  C,    1011,  M_STAT,    sval );

    // remove eighth arc A-(1008)->C
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, C, 1008, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_NO_ARCS(               "A -> None",                Aout                );
    EXPECT_SIMPLE_ARC(            "rev C->B",                 Cin,  B,    1009,  M_STAT,    sval );

    // remove ninth arc B-(1009)->C
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, C, 1009, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_NO_ARCS(               "B -> None",                Bout                );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                 );

    // remove tenth arc C-(1010)->V
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, V, 1010, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_ARRAY_OF_ARCS(         "C->[A,B]",                 Cout,             2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No C -> V",          dyn,  Cout, V             );
    EXPECT_NO_ARCS(               "rev V -> None",            Vin                 );

    // remove eleventh arc C-(1011)->A
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, A, 1011, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_SIMPLE_ARC(            "C->B",                     Cout, B,    1012,  M_STAT,    sval );
    EXPECT_NO_ARCS(               "rev A -> None",            Ain                 );

    // remove twelfth arc C-(1012)->B
    __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, B, 1012, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV );
    EXPECT_NO_ARCS(               "C -> None",                Cout                );
    EXPECT_NO_ARCS(               "rev B -> None",            Bin                 );

    // make sure all empty
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    // release vertices
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CONVERSION BETWEEN ARC REPRESENTATIONS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Conversion between arc representations" ) {
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };
    vgx_ArcVector_cell_t arc_cell, *cell=&arc_cell;

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    // Exists:  none
    // Add:     A-(111)->B
    // Result:  A-(111)->B
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 111, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(111)->B" );
    EXPECT_SIMPLE_ARC(            "A-(111)->B",               Aout, B,  111,  M_STAT,  sval       );
    EXPECT_SIMPLE_ARC(            "rev B-(111)->A",           Bin,  A,  111,  M_STAT,  sval       );

    // Exists:  A-(111)->B
    // Add:     A-(111)->B
    // Result:  A-(111)->B (no change)
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 111, VGX_ARCDIR_OUT ), connect_event ) == 0, "A-(111)->B already exists" );
    EXPECT_SIMPLE_ARC(            "A-(111)->B",               Aout, B,  111,  M_STAT,  sval       );
    EXPECT_SIMPLE_ARC(            "rev B-(111)->A",           Bin,  A,  111,  M_STAT,  sval       );

    // Exists:  A-(111)->B
    // Add:     A-(222)->B
    // Result:  A=(111,222)=>B
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 222, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(222)->B" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B]",                Aout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(111,222)=>B",     dyn,  Aout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(111,222)=>A", dyn,  Bin,  A,          2 );

    // Exists:  A=(111,222)=>B
    // Add:     A-(222)->B
    // Result:  A=(111,222)=>B  (no change)
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 222, VGX_ARCDIR_OUT ), connect_event ) == 0, "A-(222)->B already exists" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B]",                Aout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(111,222)=>B",     dyn,  Aout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(111,222)=>A", dyn,  Bin,  A,          2 );

    // Exists:  A=(111,222)=>B
    // Remove:  A-(111)->B
    // Result:  A-(222)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->B" );
    EXPECT_SIMPLE_ARC(            "A-(222)->B",               Aout, B,  222,  M_STAT,  sval       );
    EXPECT_SIMPLE_ARC(            "rev B-(222)->A",           Bin,  A,  222,  M_STAT,  sval       );

    // Exists:  A-(222)->B
    // Remove:  A-(111)->B
    // Result:  A-(222)->B (no action)
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(111)->B does not exist" );
    EXPECT_SIMPLE_ARC(            "A-(222)->B",               Aout, B,  222,  M_STAT,  sval       );
    EXPECT_SIMPLE_ARC(            "rev B-(222)->A",           Bin,  A,  222,  M_STAT,  sval       );

    // Exists:  A-(222)->B
    // Remove:  A-(222)->B
    // Result:  none
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(222)->B" );
    EXPECT_ARC( "A->None",                                  VGX_ARCVECTOR_NO_ARCS,        Aout,   0 );
    EXPECT_ARC( "rev B->None",                              VGX_ARCVECTOR_NO_ARCS,        Bin,    0 );

    // Exists:  none
    // Remove:  A-(222)->B
    // Result:  none
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(222)->B does not exist" );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );

    // Exists:  none
    // Add:     A-(333)->B
    // Result:  A-(333)->B
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 333, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(333)->B" );
    EXPECT_SIMPLE_ARC(            "A-(333)->B",               Aout, B,  333,  M_STAT,  sval       );
    EXPECT_SIMPLE_ARC(            "rev B-(333)->A",           Bin,  A,  333,  M_STAT,  sval       );

    // Exists:  A-(333)->B
    // Add:     A-(444)->C
    // Result:  A->[ -(333)->B, -(444)->C ]
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 444, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(444)->C" );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,C]",                 Aout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(333)->B",         dyn,  Aout, B, 333,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(444)->C",         dyn,  Aout, C, 444,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(333)->A",           Bin,  A, 333,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev C-(444)->A",           Cin,  A, 444,  M_STAT,  sval        );

    // Exists:  A->[ -(333)->B, -(444)->C ]
    // Add:     A-(555)->B
    // Result:  A->[ =(333,555)=>B, -(444)->C ]
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 555, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(555)->B" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,C]",              Aout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(333,555)=>B",     dyn,  Aout, B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(444)->C",         dyn,  Aout, C, 444,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(333,555)=>A", dyn,  Bin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev C-(444)->A",           Cin,  A,  444,  M_STAT,  sval        );

    // Exists:  A->[ =(333,555)=>B, -(444)->C ]
    // Add:     A-(444)->C
    // Result:  A->[ =(333,555)=>B, -(444)->C ]  (no change)
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 444, VGX_ARCDIR_OUT ), connect_event ) == 0, "A-(444)->C already exists" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,C]",              Aout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(333,555)=>B",     dyn,  Aout, B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(444)->C",         dyn,  Aout, C, 444,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(333,555)=>A", dyn,  Bin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev C-(444)->A",           Cin,  A, 444,  M_STAT,  sval        );

    // Exists:  A->[ =(333,555)=>B, -(444)->C ]
    // Add:     A-(555)->B
    // Result:  A->[ =(333,555)=>B, -(444)->C ]  (no change)
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 555, VGX_ARCDIR_OUT ), connect_event ) == 0, "A-(555)->B already exists" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,C]",              Aout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(333,555)=>B",     dyn,  Aout, B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(444)->C",         dyn,  Aout, C, 444,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(333,555)=>A", dyn,  Bin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev C-(444)->A",           Cin,  A, 444,  M_STAT,  sval        );

    // Exists:  A->[ =(333,555)=>B, -(444)->C ]
    // Remove:  A-(444)->C
    // Result:  A->[ =(333,555)=>B ]
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, C, 444, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(444)->C" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,C]",              Aout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(333,555)=>B",     dyn,  Aout, B,          2 );
    EXPECT_NO_ARC_IN_ARRAY(       "None to C",          dyn,  Aout, C             );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(333,555)=>A", dyn,  Bin,  A,          2 );
    EXPECT_NO_ARCS(               "None from C",              Cin                 );

    // Exists:  A->[ =(333,555)=>B ]
    // Add:     A-(666)->C
    // Result:  A->[ =(333,555)=>B, -(666)->C ]
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 666, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(666)->C" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>2B,C]",              Aout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(333,555)=>B",     dyn,  Aout, B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(666)->C",         dyn,  Aout, C, 666,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2A]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(333,555)=>A", dyn,  Bin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev C-(666)->A",           Cin,  A, 666,  M_STAT,  sval        );

    // Exists:  A->[ =(333,555)=>B, -(666)->C ]
    // Remove:  A-(333)->B
    // Result:  A->[ -(555)->B, -(666)->C ]
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(333)->B" );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,C]",                 Aout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(555)->B",         dyn,  Aout, B, 555,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(666)->C",         dyn,  Aout, C, 666,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(555)->A",           Bin,  A, 555,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev C-(666)->A",           Cin,  A, 666,  M_STAT,  sval        );

    // Exists:  A->[ -(555)->B, -(666)->C ]
    // Remove:  A-(333)->B
    // Result:  A->[ -(555)->B, -(666)->C ]  (no change)
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(333)->B does not exist" );
    EXPECT_ARRAY_OF_ARCS(         "A->[B,C]",                 Aout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(555)->B",         dyn,  Aout, B, 555,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(666)->C",         dyn,  Aout, C, 666,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(555)->A",           Bin,  A, 555,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev C-(666)->A",           Cin,  A, 666,  M_STAT,  sval        );

    // Exists:  A->[ -(555)->B, -(666)->C ]
    // Remove:  A-(555)->B
    // Result:  A-(666)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, 555, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(555)->B" );
    EXPECT_SIMPLE_ARC(            "A-(666)->C",               Aout, C, 666,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev C-(666)->A",           Cin,  A, 666,  M_STAT,  sval        );

    // Exists:  A-(666)->C
    // Remove:  A-(666)->C
    // Result:  none
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, C, 666, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(666)->C" );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CONVERSION BETWEEN LARGER ARC REPRESENTATIONS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Conversion between larger arc representations" ) {
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };
    vgx_ArcVector_cell_t arc_cell, *cell=&arc_cell;

    // make sure it's empty before we start
    EXPECT_NO_ARCS(                 "A -> None",                Aout  );
    EXPECT_NO_ARCS(                 "None <- A",                Ain   );
    EXPECT_NO_ARCS(                 "B -> None",                Bout  );
    EXPECT_NO_ARCS(                 "None <- B",                Bin   );
    EXPECT_NO_ARCS(                 "C -> None",                Cout  );
    EXPECT_NO_ARCS(                 "None <- C",                Cin   );

    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    int max_rel = 1000;
    vgx_predicator_rel_enum base_rel = __VGX_PREDICATOR_REL_START_USER_RANGE - 1;
    vgx_predicator_rel_enum rel;

    // A -> None
    EXPECT_NO_ARCS(                 "A -> None",                Aout                    );

    // A-(1)->B
    rel = base_rel + 1;
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(%d)->B", rel );
    EXPECT_SIMPLE_ARC(              "A-(rel)->B",               Aout,   B,  rel,  M_STAT,  sval         );

    // A=(2...m)=>B
    for( int m = 2; m <= max_rel; m++ ) {
      rel = base_rel + m;
      TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, rel, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(%d)->B", rel );
      EXPECT_ARRAY_OF_ARCS(         "A-[=>mB]",                 Aout,                 m );
      EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(1...m)=>B",       dyn,  Aout,   B,            m );
    }

    // Remove A=(m...3)=>B
    for( int m = max_rel; m >= 3; m-- ) {
      rel = base_rel + m;
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(%d)->B", rel );
      EXPECT_ARRAY_OF_ARCS(         "A-[=>mB]",                 Aout,                 m-1 );
      EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(1...m)=>B",       dyn,  Aout,   B,            m-1 );
    }

    // Remove A=(2)=>B
    rel = base_rel + 2;
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(%d)->B", rel );
    EXPECT_SIMPLE_ARC(              "A-(rel)->B",               Aout,   B,  base_rel + 1,  M_STAT,  sval  );

    // Remove A-(1)->B
    rel = base_rel + 1;
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B, rel, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(%d)->B", rel );
    EXPECT_NO_ARCS(                 "A -> None",                Aout                      );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * SELF LOOP
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and try to remove arcs ensuring predicator match" ) {
    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );

    // Acquire vertex
    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );

    // Add simple arc loop
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, V, 1001, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(1001)->V" );
    EXPECT_SIMPLE_ARC(            "V-(1001)->V",               Vout, V, 1001,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev V-(1001)->V",           Vin,  V, 1001,  M_STAT,  sval        );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 1, "Outdegree == 1" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 1, "Indegree == 1" );

    // Try to remove non-existant arc
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, V, 1003, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "V-(1003)->V does not exist" );
    EXPECT_SIMPLE_ARC(            "V-(1001)->V",               Vout, V, 1001,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev V-(1001)->V",           Vin,  V, 1001,  M_STAT,  sval        );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 1, "Outdegree == 1" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 1, "Indegree == 1" );

    // Make multiple arc loop
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, V, 1002, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(1002)->V" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2V]",                 Vout,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001,1002)=>V",    dyn,  Vout, V,           2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2V]",             Vin,               2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1001,1002)=>V",dyn,  Vin,  V,           2 );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 2, "Outdegree == 2" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 2, "Indegree == 2" );

    // Try to remove non-existant arc
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, V, 1003, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "V-(1003)->V does not exist" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2V]",                 Vout,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001,1002)=>V",    dyn,  Vout, V,           2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2V]",             Vin,               2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1001,1002)=>V",dyn,  Vin,  V,           2 );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 2, "Outdegree == 2" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 2, "Indegree == 2" );

    // Add 3rd arc to loop
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, V, 1003, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(1003)->V" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>3V]",                 Vout,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001-1003)=>V",    dyn,  Vout, V,           3 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>3V]",             Vin,               3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1001-1003)=>V",dyn,  Vin,  V,           3 );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 3, "Outdegree == 3" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 3, "Indegree == 3" );

    // Remove 3rd arc
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, V, 1003, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove V-(1003)->V" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2V]",                 Vout,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001,1002)=>V",    dyn,  Vout, V,           2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2V]",             Vin,               2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1001,1002)=>V",dyn,  Vin,  V,           2 );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 2, "Outdegree == 2" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 2, "Indegree == 2" );

    // Remove 1st arc
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, V, 1001, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove V-(1001)->V" );
    EXPECT_SIMPLE_ARC(            "V-(1002)->V",               Vout, V, 1002,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev V-(1002)->V",           Vin,  V, 1002,  M_STAT,  sval        );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 1, "Outdegree == 1" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 1, "Indegree == 1" );

    // Add back 1st arc
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, V, V, 1001, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add V-(1001)->V" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2V]",                 Vout,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1002,1001)=>V",    dyn,  Vout, V,           2 );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2V]",             Vin,               2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1002,1001)=>V",dyn,  Vin,  V,           2 );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 2, "Outdegree == 2" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 2, "Indegree == 2" );

    // Remove all arcs
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, V, NULL, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 2, "Remove V-(*)->V" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 0, "Outdegree == 0" );
    TEST_ASSERTION( iarcvector.Degree( Vin ) == 0, "Indegree == 0" );

    // Release vertex
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );

    CStringDelete( CSTR___V );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * REMOVAL OF ARCS HAS TO MATCH THE PREDICATOR
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and try to remove arcs ensuring predicator match" ) {

    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };
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

    // add first arc V-(101)->A
    SET_STATIC_ARC( &arc, V, A, 101, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(101)->A" );
    EXPECT_SIMPLE_ARC(            "V-(101)->A",               Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );

    // try to remove arc V-(999)->A, which does not exist
    SET_ARC_REL_QUERY( &arc, V, A, 999, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(999)->A" );
    EXPECT_SIMPLE_ARC(            "V-(101)->A",               Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );

    // add second arc V-(501)->B
    SET_STATIC_ARC( &arc, V, B, 501, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(501)->B" );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B]",                 Vout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(101)->A",         dyn,  Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // try to remove arc V-(999)->B, which does not exist
    SET_ARC_REL_QUERY( &arc, V, B, 999, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(999)->B" );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B]",                 Vout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(101)->A",         dyn,  Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // try to remove arc V-(101)->B, which does not exist
    SET_ARC_REL_QUERY( &arc, V, B, 101, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(101)->B" );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B]",                 Vout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(101)->A",         dyn,  Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // try to remove arc V-(501)->A, which does not exist
    SET_ARC_REL_QUERY( &arc, V, A, 501, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(501)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[A,B]",                 Vout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(101)->A",         dyn,  Vout, A, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev A-(101)->V",           Ain,  V, 101,  M_STAT,  sval        );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // add third arc V=(101,102)=>A (now a multiple arc)
    SET_STATIC_ARC( &arc, V, A, 102, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(102)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,B]",              Vout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(101,102)=>A",     dyn,  Vout, A,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(101,102)=>V", dyn,  Ain,  V,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // try to remove arc V-(999)->A, which does not exist
    SET_ARC_REL_QUERY( &arc, V, A, 999, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(999)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,B]",              Vout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(101,102)=>A",     dyn,  Vout, A,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(101,102)=>V", dyn,  Ain,  V,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // add fourth arc from V to A making multiarc V=(101,102,103)=>A
    SET_STATIC_ARC( &arc, V, A, 103, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(103)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>3A,B]",              Vout,             4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(101,102,103)=>A", dyn,  Vout, A,          3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>3V]",            Ain,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(101-103)=>V", dyn,  Ain,  V,          3 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // try to remove arc V-(104)->A, which does not exist
    SET_ARC_REL_QUERY( &arc, V, A, 104, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 0, "No removal of non-existant V-(104)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>3A,B]",              Vout,             4 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(101,102,103)=>A", dyn,  Vout, A,          3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>3V]",            Ain,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(101-103)=>V", dyn,  Ain,  V,          3 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // remove first arc V-(101)->A
    SET_ARC_REL_QUERY( &arc, V, A, 101, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(101)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A,B]",              Vout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(102,103)=>A",     dyn,  Vout, A,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(501)->B",         dyn,  Vout, B, 501,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(102,103)=>V", dyn,  Ain,  V,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // remove entire multiple arc V=>A
    SET_ARC_REL_QUERY( &arc, V, A, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "Remove V=(102,103)=>A" );
    EXPECT_SIMPLE_ARC(            "V-(501)->B",               Vout, B, 501,  M_STAT,  sval        );
    EXPECT_NO_ARCS(               "No rev A -> V",            Ain                 );
    EXPECT_SIMPLE_ARC(            "rev B-(501)->V",           Bin,  V, 501,  M_STAT,  sval        );

    // add another arc from V to B making multiarc V=(501,502)=>B
    SET_STATIC_ARC( &arc, V, B, 502, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(502)->B" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2B]",                Vout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(501,502)=>B",     dyn,  Vout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2V]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(501,502)=>V", dyn,  Bin,  V,          2 );

    // add another arc V-(701)->C and
    SET_STATIC_ARC( &arc, V, C, 701, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(701)->C" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2B,C]",              Vout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(501,502)=>B",     dyn,  Vout, B,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(701)->C",         dyn,  Vout, C, 701,  M_STAT,  sval        );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2V]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(501,502)=>V", dyn,  Bin,  V,          2 );
    EXPECT_SIMPLE_ARC(            "rev C-(701)->V",           Cin,  V, 701,  M_STAT,  sval        );

    // remove arc V-(701)->C
    SET_ARC_REL_QUERY( &arc, V, C, 701, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(701)->C" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2B]",                Vout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(501,502)=>B",     dyn,  Vout, B,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2V]",            Bin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(501,502)=>V", dyn,  Bin,  V,          2 );

    // remove entire multiple arc V=>B - we should now be back to empty
    SET_ARC_REL_QUERY( &arc, V, B, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "Remove V=(501,502)=>B" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * MULTIPLE ARCS FROM V TO A ARE ADDED THEN REMOVED
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove Multiple arcs between two vertices" ) {

    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    // add first arc from V to A
    SET_STATIC_ARC( &arc, V, A, 1001, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(1001)->A" );
    EXPECT_SIMPLE_ARC(            "V-(1001)->A",              Vout,  A, 1001,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC(            "rev A-(1001)->V",          Ain,   V, 1001,  M_STAT,  sval      );

    // add second arc from V to A and expect the primary structure to change to array of arcs (multiple predicator arc will be chained off this)
    SET_STATIC_ARC( &arc, V, A, 1002, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(1002)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A]",                Vout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001,1002)=>A",   dyn,  Vout, A,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(1001,1002)=>V",dyn, Ain,  V,          2 );

    // add third arc from V to A and expect the primary structure to remain array of arcs and multiple predicator arc chained off of the array
    SET_STATIC_ARC( &arc, V, A, 1003, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(1003)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>3A]",                Vout,             3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001-1003)=>A",   dyn,  Vout, A,          3 );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>3V]",            Ain,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(1001-1003)=>V",dyn, Ain,  V,          3 );

    // remove first arc from V to A and expect the primary structure to remain array of arcs and multiple predicator arc chained off of the array
    SET_ARC_REL_QUERY( &arc, V, A, 1001, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(1001)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A]",                Vout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1002,1003)=>A",   dyn,  Vout, A,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(1002,1003)=>V",dyn, Ain,  V,          2 );

    // remove second arc from V to A and expect the primary structure to change to simple arc
    SET_ARC_REL_QUERY( &arc, V, A, 1002, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(1002)->A" );
    EXPECT_SIMPLE_ARC(            "V-(1003)->A",              Vout,  A, 1003,  M_STAT,  sval      );
    EXPECT_SIMPLE_ARC(            "rev A-(1003)->V",          Ain,   V, 1003,  M_STAT,  sval      );

    // remove third arc from V to A and expect empty after this
    SET_ARC_REL_QUERY( &arc, V, A, 1003, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(1003)->A" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * MULTIPLE ARCS FROM V TO A ARE ADDED UP TO MAXIMUM CAPACITY OF
   * PREDICATORS, THEN REMOVED
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove Multiple Arc" ) {

    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    // Add first arc and make sure we are simple arc
    SET_STATIC_ARC( &arc, V, A, _VGX_PREDICATOR_REL_TEST1, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(TEST1)->A" );
    EXPECT_SIMPLE_ARC(            "V-(TEST1)->A",             Vout,  A, _VGX_PREDICATOR_REL_TEST1,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC(            "rev A-(TEST1)->V",         Ain,   V, _VGX_PREDICATOR_REL_TEST1,  M_STAT,  sval     );

    // Add second arc and make sure we are multiple arc
    SET_STATIC_ARC( &arc, V, A, _VGX_PREDICATOR_REL_TEST2, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(TEST2)->A" );
    EXPECT_ARRAY_OF_ARCS(         "V->[=>2A]",                Vout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(TEST1,TEST2)=>A", dyn,  Vout, A,          2 );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2V]",            Ain,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(TEST1,TEST2)=>V",dyn,Ain, V,          2 );

    // Add multiarc predicators to full capacity
    int expect_degree = 2;
    for( int rel = __VGX_PREDICATOR_REL_START_SYS_RANGE; rel <= __VGX_PREDICATOR_REL_END_USER_RANGE; rel++ ) {
      SET_STATIC_ARC( &arc, V, A, rel, VGX_ARCDIR_OUT );
      TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Add V-(0x%0X)->A", rel );
      expect_degree++;
      EXPECT_ARRAY_OF_ARCS(         "V->[=>nA]",                Vout,             expect_degree );
      EXPECT_ARRAY_OF_ARCS(         "rev A->[=>nV]",            Ain,              expect_degree );
      if( rel < __VGX_PREDICATOR_REL_START_USER_RANGE ||
          rel % 1187 == 0 ||
          rel > __VGX_PREDICATOR_REL_END_USER_RANGE - 10 ) 
      { // <= do sample checks for deep counts (otherwise the test takes too long)
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(...)=>A",         dyn,  Vout, A,          expect_degree );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>V",     dyn,  Ain,  V,          expect_degree );
      }
    }

    // Remove all but two
    for( int rel = __VGX_PREDICATOR_REL_START_SYS_RANGE; rel <= __VGX_PREDICATOR_REL_END_USER_RANGE; rel++ ) {
      SET_ARC_REL_QUERY( &arc, V, A, rel, VGX_ARCDIR_OUT );
      TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(0x%0X)->A", rel );
      expect_degree--;
      EXPECT_ARRAY_OF_ARCS(         "V->[=>nA]",                Vout,             expect_degree );
      EXPECT_ARRAY_OF_ARCS(         "rev A->[=>nV]",            Ain,              expect_degree );
      if( rel < __VGX_PREDICATOR_REL_START_USER_RANGE ||
          rel % 1187 == 0 ||
          rel > __VGX_PREDICATOR_REL_END_USER_RANGE - 10 ) 
      { // <= do sample checks for deep counts (otherwise the test takes too long)
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(...)=>A",         dyn,  Vout, A,          expect_degree );
        EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>V",     dyn,  Ain,  V,          expect_degree );
      }
    }

    // remove second to last arc and make sure we are converted back to simple arc
    SET_ARC_REL_QUERY( &arc, V, A, _VGX_PREDICATOR_REL_TEST1, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(TEST1)->A" );
    EXPECT_SIMPLE_ARC(            "V-(TEST2)->A",             Vout,  A, _VGX_PREDICATOR_REL_TEST2,  M_STAT,  sval     );
    EXPECT_SIMPLE_ARC(            "rev A-(TEST2)->V",         Ain,   V, _VGX_PREDICATOR_REL_TEST2,  M_STAT,  sval     );

    // Remove last arc and make sure we are back to empty
    SET_ARC_REL_QUERY( &arc, V, A, _VGX_PREDICATOR_REL_TEST2, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove V-(TEST2)->A" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * MASS REMOVAL
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and remove Multiple Arc" ) {

    const CString_t *CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t *CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t *CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t *CSTR___C = NewEphemeralCString( graph, "C" );
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Arc_t arc = {
      .tail = NULL,
      .head = {
        .vertex = NULL,
        .predicator = {
          .val = 1,
          .mod = VGX_PREDICATOR_MOD_STATIC,
          .rel = VGX_PREDICATOR_REL_NONE
        }
      }
    };

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    // Add large multiarc V=>A
    int start = __VGX_PREDICATOR_REL_START_USER_RANGE;
    int end = start + 500;
    for( int rel = start; rel < end; rel++ ) {
      SET_STATIC_ARC( &arc, V, A, rel, VGX_ARCDIR_OUT );
      iarcvector.Add( dyn, &arc, connect_event );
    }

    // Add small multiarc V=>B
    SET_STATIC_ARC( &arc, V, B, 1001, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );
    SET_STATIC_ARC( &arc, V, B, 1002, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );

    // Add simple arc V->C
    SET_STATIC_ARC( &arc, V, C, 1003, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );

    // Add simple arc A->V
    SET_STATIC_ARC( &arc, A, V, 1004, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );

    // Add multiple arc B=>V
    SET_STATIC_ARC( &arc, B, V, 1005, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );
    SET_STATIC_ARC( &arc, B, V, 1006, VGX_ARCDIR_OUT );
    iarcvector.Add( dyn, &arc, connect_event );

    // Add multiple arc C=>A
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 2001, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 2002, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 2003, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 2004, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, A, 2005, VGX_ARCDIR_OUT ), connect_event );

    // Add multiple arc C=>B
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2003, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2004, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2005, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2006, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2007, VGX_ARCDIR_OUT ), connect_event );
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, B, 2008, VGX_ARCDIR_OUT ), connect_event );

    // Add simple arc C->V
    iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, V, 2001, VGX_ARCDIR_OUT ), connect_event );


    EXPECT_ARRAY_OF_ARCS(         "V->[=>500A,=>2B,C]",       Vout,             503 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(...)=>A",         dyn,  Vout, A,          500 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "V=(1001,1002)=>B",   dyn,  Vout, B,          2   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "V-(1003)->C",        dyn,  Vout, C, 1003,  M_STAT,  sval         );
    EXPECT_SIMPLE_ARC(            "A-(1004)->V",              Aout, V, 1004,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2V]",                Bout,             2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(1005,1006)=>V",   dyn,  Bout, V,          2   );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>5A,=>6B,V]",         Cout,             12  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2001)->V",        dyn,  Cout, V, 2001,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>500V,=>5C]",     Ain,              505 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>V",     dyn,  Ain,  V,          500 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          5   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>2V,=>6C]",       Bin,              8   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(1001,1002)=>V",dyn, Bin,  V,          2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_SIMPLE_ARC(            "rev C-(1003)->V",          Cin,  V, 1003,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,=>2B,C]",        Vin,              4   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(1004)->A",     dyn, Vin,  A, 1004,  M_STAT,  sval         );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1005,1006)=>B",dyn, Vin,  B,          2   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(2001)->C",     dyn, Vin,  C, 2001,  M_STAT,  sval         );

    // Remove all arcs from V to other vertices
    SET_ARC_REL_QUERY( &arc, V, NULL, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 503, "All arcs from V to other vertices are removed" );
    EXPECT_NO_ARCS(               "V -> None",                Vout                  );
    EXPECT_SIMPLE_ARC(            "A-(1004)->V",              Aout, V, 1004,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2V]",                Bout,             2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(1005,1006)=>V",   dyn,  Bout, V,          2   );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>5A,=>6B,V]",         Cout,             12  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2001)->V",        dyn,  Cout, V, 2001,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>5C]",      Ain,                    5   );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev A -> V",      dyn,  Ain,  V               );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          5   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>6C]",       Bin,                    6   );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev B -> V",      dyn,  Bin,  V               );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                   );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[A,=>2B,C]",        Vin,              4   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(1004)->A",    dyn,  Vin,  A, 1004,  M_STAT,  sval         );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1005,1006)=>B",dyn, Vin,  B,          2   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(2001)->C",    dyn,  Vin,  C, 2001,  M_STAT,  sval         );

    // Remove all arcs from A to other vertices
    SET_ARC_REL_QUERY( &arc, A, NULL, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "All arcs from A to other vertices are removed" );
    EXPECT_NO_ARCS(               "V -> None",                Vout                  );
    EXPECT_NO_ARCS(               "A -> None",                Aout                  );
    EXPECT_ARRAY_OF_ARCS(         "B->[=>2V]",                Bout,             2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "B=(1005,1006)=>V",   dyn,  Bout, V,          2   );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>5A,=>6B,V]",         Cout,             12  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2001)->V",        dyn,  Cout, V, 2001,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>5C]",      Ain,                    5   );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev A -> V",      dyn,  Ain,  V               );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          5   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>6C]",       Bin,                    6   );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev B -> V",      dyn,  Bin,  V               );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                   );
    EXPECT_ARRAY_OF_ARCS(         "rev V->[=>2B,C]",          Vin,              3   );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev V -> A",      dyn,  Vin,  A               );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev V=(1005,1006)=>B",dyn, Vin,  B,          2   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev V-(2001)->C",    dyn,  Vin,  C, 2001,  M_STAT,  sval         );

    // Remove all arcs from B to other vertices
    SET_ARC_REL_QUERY( &arc, B, NULL, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "All arcs from B to other vertices are removed" );
    EXPECT_NO_ARCS(               "V -> None",                Vout                  );
    EXPECT_NO_ARCS(               "A -> None",                Aout                  );
    EXPECT_NO_ARCS(               "B -> None",                Bout                  );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>5A,=>6B,V]",         Cout,             12  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2001)->V",        dyn,  Cout, V, 2001,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>5C]",      Ain,                    5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          5   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>6C]",       Bin,                    6   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                   );
    EXPECT_SIMPLE_ARC(            "rev V-(2001)->C",          Vin,  C, 2001,  M_STAT,  sval         );

    // Remove all arcs from C to other vertices with the "2002" relationship
    SET_ARC_REL_QUERY( &arc, C, NULL, 2002, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 1, "Remove C-(2002)->A only" );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>4A,=>6B,V]",         Cout,             11  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          4   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2001)->V",        dyn,  Cout, V, 2001,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>4C]",      Ain,                    4   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          4   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>6C]",       Bin,                    6   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_SIMPLE_ARC(            "rev V-(2001)->C",          Vin,  C, 2001,  M_STAT,  sval         );

    // Remove all arcs from C to other vertices with the "2001" relationship
    SET_ARC_REL_QUERY( &arc, C, NULL, 2001, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "Remove C-(2001)->A and C-(2001)->V" );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>3A,=>6B]",           Cout,             9   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>A",         dyn,  Cout, A,          3   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          6   );
    EXPECT_NO_ARC_IN_ARRAY(       "No C -> V",          dyn,  Cout, V               );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>3C]",      Ain,                    3   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(...)=>C",     dyn,  Ain,  C,          3   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>6C]",       Bin,                    6   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          6   );
    EXPECT_NO_ARCS(               "rev V -> None",            Vin                   );

    // Remove all arcs from C to other vertices with the "2005" relationship
    SET_ARC_REL_QUERY( &arc, C, NULL, 2005, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "Remove C-(2005)->A and C-(2005)->B" );
    EXPECT_ARRAY_OF_ARCS(         "C->[=>2A,=>5B]",           Cout,             7   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(2003,2004)=>A",   dyn,  Cout, A,          2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          5   );
    EXPECT_ARRAY_OF_ARCS(         "rev A->[=>2C]",      Ain,                    2   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev A=(2003,2004)=>C",dyn, Ain,  C,          2   );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>5C]",       Bin,                    5   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          5   );

    // Remove all arcs from C to other vertices with the "2003" relationship
    SET_ARC_REL_QUERY( &arc, C, NULL, 2003, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 2, "Remove C-(2003)->A and C-(2003)->B" );
    EXPECT_ARRAY_OF_ARCS(         "C->[A,=>4B]",              Cout,             5   );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(2004)->A",        dyn,  Cout, A, 2004,  M_STAT,  sval         );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "C=(...)=>B",         dyn,  Cout, B,          4   );
    EXPECT_SIMPLE_ARC(            "rev A->C",                 Ain,  C, 2004,  M_STAT,  sval         );
    EXPECT_ARRAY_OF_ARCS(         "rev B->=>4C]",       Bin,                    4   );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(...)=>C",     dyn,  Bin,  C,          4   );

    // Remove all arcs from C to other vertices
    SET_ARC_REL_QUERY( &arc, C, NULL, VGX_PREDICATOR_REL_NONE, VGX_ARCDIR_OUT );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_REV ) == 5, "Remove C-(*)->*" );
    EXPECT_NO_ARCS(               "V -> None",                Vout  );
    EXPECT_NO_ARCS(               "None <- V",                Vin   );
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY ALL THE TEST VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Vertices" ) {

    const CString_t * CSTR___V = NewEphemeralCString( graph, "V" );
    const CString_t * CSTR___A = NewEphemeralCString( graph, "A" );
    const CString_t * CSTR___B = NewEphemeralCString( graph, "B" );
    const CString_t * CSTR___C = NewEphemeralCString( graph, "C" );
    const CString_t * CSTR___D = NewEphemeralCString( graph, "D" );
    const CString_t * CSTR___E = NewEphemeralCString( graph, "E" );

    igraph->simple->DeleteVertex( graph, CSTR___V, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___C, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___D, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___E, 0, NULL, NULL );

    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );
    CStringDelete( CSTR___D );
    CStringDelete( CSTR___E );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY THE TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    COMLIB_OBJECT_DESTROY(graph);
  } END_TEST_SCENARIO

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_name );
  CStringDelete( CSTR__graph_path );

} END_UNIT_TEST


#endif
