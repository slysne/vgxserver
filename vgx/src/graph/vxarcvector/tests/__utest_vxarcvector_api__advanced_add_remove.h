/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxarcvector_api__advanced_add_remove.h
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

#ifndef __UTEST_VXARCVECTOR_API__ADVANCED_ADD_REMOVE_H
#define __UTEST_VXARCVECTOR_API__ADVANCED_ADD_REMOVE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxarcvector_api__advanced_add_remove ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  const CString_t *CSTR___A = NULL;
  const CString_t *CSTR___B = NULL;
  const CString_t *CSTR___C = NULL;
  const CString_t *CSTR___D = NULL;
  const CString_t *CSTR___E = NULL;
  
  vgx_Graph_t *graph = NULL;
  vgx_Graph_vtable_t *igraph = NULL;
  framehash_dynamic_t *dyn = NULL;

  f_Vertex_connect_event connect_event = _vxgraph_arc__connect_WL_reverse_WL;
  f_Vertex_disconnect_event disconnect_REV = _vxgraph_arc__disconnect_WL_reverse_WL;
  f_Vertex_disconnect_event disconnect_FWD = _vxgraph_arc__disconnect_WL_forward_WL;

  vgx_predicator_mod_t M_INT = { .bits = VGX_PREDICATOR_MOD_INTEGER };
  vgx_predicator_mod_t M_FLT = { .bits = VGX_PREDICATOR_MOD_FLOAT };
  vgx_predicator_mod_t M_STAT = { .bits = VGX_PREDICATOR_MOD_STATIC };
  vgx_predicator_val_t ival = {0};
  vgx_predicator_val_t rval = {0};



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
    CSTR___A = NewEphemeralCString( graph, "A" );
    CSTR___B = NewEphemeralCString( graph, "B" );
    CSTR___C = NewEphemeralCString( graph, "C" );
    CSTR___D = NewEphemeralCString( graph, "D" );
    CSTR___E = NewEphemeralCString( graph, "E" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ADVANCED WILDCARD REMOVE SCENARIOS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Advanced wildcard remove scenarios" ) {
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

    vgx_Vertex_t *A, *B, *C, *D, *E = NULL;
    vgx_ArcVector_cell_t *Ain, *Bin, *Cin, *Din, *Ein;
    vgx_ArcVector_cell_t *Aout, *Bout, *Cout, *Dout, *Eout;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    D = igraph->simple->OpenVertex( graph, CSTR___D, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    E = igraph->simple->OpenVertex( graph, CSTR___E, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );

    TEST_ASSERTION( A && B && C && D && E, "Created vertices" );

    Ain = _vxvertex__get_vertex_inarcs( A );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Cin = _vxvertex__get_vertex_inarcs( C );
    Din = _vxvertex__get_vertex_inarcs( D );
    Ein = _vxvertex__get_vertex_inarcs( E );

    Aout = _vxvertex__get_vertex_outarcs( A );
    Bout = _vxvertex__get_vertex_outarcs( B );
    Cout = _vxvertex__get_vertex_outarcs( C );
    Dout = _vxvertex__get_vertex_outarcs( D );
    Eout = _vxvertex__get_vertex_outarcs( E );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );
    EXPECT_NO_ARCS(               "D -> None",                Dout  );
    EXPECT_NO_ARCS(               "None <- D",                Din   );
    EXPECT_NO_ARCS(               "E -> None",                Eout  );
    EXPECT_NO_ARCS(               "None <- E",                Ein   );
    

    /*

    GRAPH:

             A-(111)->B-(222)->C-(333)->D=(555,666)=>E
                                \-(444)------------->E


    NON-MATCHING:

    *-(111)->A
    *-( * )->A
             A-(222)->B
             A-(111)->C
             A-( * )->C
             A-(222)->*
             *-(222)->B 
                      B-(111)->A
                      *-(111)->C
                               C-(222)->D
                               C-(333)->E
                               C-( * )->A
                               C-(222)->*
                                        D-(333)->E
                                        D-(555)->A
                                        D-( * )->A
                                        D-(333)->*
                                        *-(333)->E
                                        
    MATCHING:
             A-(111)->B
             A-( * )->B 
             A-(111)->*
             A-( * )->*
                      *-(222)->C
                      *-( * )->C
                               C-(333)->D
                               C-( * )->D
                               C-(333)->*
                               C-( * )->*
                               *-(333)->D
                               *-( * )->D
                                        D-(555)->E
                                        D-( * )->E
                                        D-(555)->*
                                        D-( * )->*
                                        *-(555)->E
                               *-(444)---------->E
                               C-( * )---------->E
                                        *-( * )->E

    */

    // Build the graph
    //    A-(111)->B-(222)->C-(333)->D=(555,666)=>E
    //                       \-(444)------------->E
    //
    //
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 111, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, B, C, 222, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add B-(222)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, D, 333, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(333)->D" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, C, E, 444, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add C-(444)->E" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, D, E, 555, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add D-(555)->E" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, D, E, 666, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add D-(666)->E" );
    ival.integer = 0;
    EXPECT_SIMPLE_ARC(            "A-(111)->B",               Aout, B,  111, M_STAT,    ival  );
    EXPECT_SIMPLE_ARC(            "B-(222)->C",               Bout, C,  222, M_STAT,    ival  );
    EXPECT_ARRAY_OF_ARCS(         "C->[D,E]",                 Cout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(333)->D",         dyn,  Cout, D,  333, M_STAT,    ival  );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(444)->E",         dyn,  Cout, E,  444, M_STAT,    ival  );
    EXPECT_ARRAY_OF_ARCS(         "D->[=>2E]",                Dout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "D=(555,666)=>E",     dyn,  Dout, E,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(111)->A",           Bin,  A,  111, M_STAT,    ival  );
    EXPECT_SIMPLE_ARC(            "rev C-(222)->B",           Cin,  B,  222, M_STAT,    ival  );
    EXPECT_SIMPLE_ARC(            "rev D-(333)->C",           Din,  C,  333, M_STAT,    ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,  444, M_STAT,    ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );

    vgx_predicator_rel_enum WILD = VGX_PREDICATOR_REL_WILDCARD;

    // -----------------------------------------------------------
    // Try (and fail) to remove all non-existant relation patterns
    // against this graph:
    //
    //    A-(111)->B-(222)->C-(333)->D=(555,666)=>E
    //                       \-(444)------------->E
    //
    //  *-(111)->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, NULL,  111,  VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(111)->A does not exist" );
    //  *-( * )->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, NULL,  WILD, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-( * )->A does not exist" );
    //  A-(222)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B,     222,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(222)->B does not exist" );
    //  A-(111)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, C,     111,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(111)->C does not exist" );
    //  A-( * )->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, C,     WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-( * )->C does not exist" );
    //  A-(222)->*
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, NULL,  222,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(222)->* does not exist" );
    //  *-(222)->B 
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, NULL,  222,  VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(222)->B does not exist" );
    //  B-(111)->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, A,     111,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "B-(111)->A does not exist" );
    //  *-(111)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, NULL,  111,  VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(111)->A does not exist" );
    //  C-(222)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, D,     222,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "C-(222)->D does not exist" );
    //  C-(333)->E
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, E,     333,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "C-(333)->E does not exist" );
    //  C-( * )->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, A,     WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "C-( * )->A does not exist" );
    //  C-(222)->*
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, NULL,  222,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "C-(222)->* does not exist" );
    //  D-(333)->E
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, E,     333,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "D-(333)->E does not exist" );
    //  D-(555)->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, A,     555,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "D-(555)->A does not exist" );
    //  D-( * )->A
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, A,     WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "D-( * )->A does not exist" );
    //  D-(333)->*
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, NULL,  333,  VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "D-(333)->* does not exist" );
    //  *-(333)->E
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, E, NULL,  333,  VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(333)->E does not exist" );
    // Graph unchanged
    EXPECT_SIMPLE_ARC(            "A-(111)->B",               Aout, B,  111,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC(            "B-(222)->C",               Bout, C,  222,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "C->[D,E]",                 Cout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(333)->D",         dyn,  Cout, D,  333,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(444)->E",         dyn,  Cout, E,  444,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "D->[=>2E]",                Dout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "D=(555,666)=>E",     dyn,  Dout, E,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(111)->A",           Bin,  A,  111,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC(            "rev C-(222)->B",           Cin,  B,  222,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC(            "rev D-(333)->C",           Din,  C,  333,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,  444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );



    // -----------------------------------------------------------
    // Verify removal of all matching patterns against this graph:
    //
    //    A-(111)->B-(222)->C-(333)->D=(555,666)=>E
    //                       \-(444)------------->E
    //
    //
    //    A-(111)->B    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,    111,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->B" );
    EXPECT_NO_ARCS(               "A -> None",                Aout                );
    EXPECT_NO_ARCS(               "rev B -> None",            Bin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,    111,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add A-(111)->B" );

    //    A-( * )->B    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,    WILD, VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove A-( * )->B" );
    EXPECT_NO_ARCS(               "A -> None",                Aout                );
    EXPECT_NO_ARCS(               "rev B -> None",            Bin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,    111,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add A-(111)->B" );

    //    A-(111)->*    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL, 111,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->*" );
    EXPECT_NO_ARCS(               "A -> None",                Aout                );
    EXPECT_NO_ARCS(               "rev B -> None",            Bin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,    111,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add A-(111)->B" );

    //    A-( * )->*    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL, WILD, VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove A-( * )->*" );
    EXPECT_NO_ARCS(               "A -> None",                Aout                );
    EXPECT_NO_ARCS(               "rev B -> None",            Bin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,    111,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add A-(111)->B" );

    //    *-(222)->C    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL, 222,  VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(222)->C" );
    EXPECT_NO_ARCS(               "B -> None",                Bout                );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, B, C,    222,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add B-(222)->C" );

    //    *-( * )->C    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL, WILD, VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-( * )->C" );
    EXPECT_NO_ARCS(               "B -> None",                Bout                );
    EXPECT_NO_ARCS(               "rev C -> None",            Cin                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, B, C,    222,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add B-(222)->C" );

    //    C-(333)->D    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, D,    333,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-(333)->D" );
    EXPECT_SIMPLE_ARC(            "C-(444)->E",               Cout, E,    444,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );

    //    C-( * )->D    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, D,    333,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-(333)->D" );
    EXPECT_SIMPLE_ARC(            "C-(444)->E",               Cout, E,    444,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );

    //    C-(333)->*    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL, 333,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-(333)->*" );
    EXPECT_SIMPLE_ARC(            "C-(444)->E",               Cout, E,    444,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );

    //    C-( * )->*    2
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL, WILD, VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 2, "Remove C-( * )->*" );
    EXPECT_NO_ARCS(               "C -> None",                Cout                );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[=>2D]",            Ein,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev E -> C",      dyn,  Ein,  C             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, E,    444,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(444)->E" );

    //    *-(333)->D    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL, 333,  VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(333)->D" );
    EXPECT_SIMPLE_ARC(            "C-(444)->E",               Cout, E,    444,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );
    
    //    *-( * )->D    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL, WILD, VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-( * )->D" );
    EXPECT_SIMPLE_ARC(            "C-(444)->E",               Cout, E,    444,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "rev D -> None",            Din                 );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, D,    333,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(333)->D" );

    //    D-(555)->E    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, E,    555,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove D-(555)->E" );
    EXPECT_SIMPLE_ARC(            "D-(666)->E",               Dout, E,    666,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,D]",             Ein,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(666)->D",     dyn,  Ein,  D,    666,  M_STAT,   ival  );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );

    //    D-( * )->E    2
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, E,    WILD, VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 2, "Remove D-( * )->E" );
    EXPECT_NO_ARCS(               "D -> None",                Dout                );
    EXPECT_SIMPLE_ARC(            "rev E-(444)->C",           Ein,  C,    444,  M_STAT,   ival  );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    666,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(666)->E" );

    //    D-(555)->*    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, E,    555,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove D-(555)->E" );
    EXPECT_SIMPLE_ARC(            "D-(666)->E",               Dout, E,    666,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,D]",             Ein,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(666)->D",     dyn,  Ein,  D,    666,  M_STAT,   ival  );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );

    //    D-( * )->*    2
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL, WILD, VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 2, "Remove D-( * )->*" );
    EXPECT_NO_ARCS(               "D -> None",                Dout                );
    EXPECT_SIMPLE_ARC(            "rev E-(444)->C",           Ein,  C,    444,  M_STAT,   ival  );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    666,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(666)->E" );

    //    *-(555)->E    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, E, NULL, 555,  VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(555)->E" );
    EXPECT_SIMPLE_ARC(            "D-(666)->E",               Dout, E,    666,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,D]",             Ein,              2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,    444,  M_STAT,   ival  );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(666)->D",     dyn,  Ein,  D,    666,  M_STAT,   ival  );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );

    //    *-(444)->E    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, E, NULL, 444,  VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(444)->E" );
    EXPECT_SIMPLE_ARC(            "C-(333)->D",               Cout, D,    333,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[=>2D]",            Ein,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev E -> C",      dyn,  Ein,  C             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, E,    444,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(444)->E" );

    //    C-( * )->E    1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, E,   WILD,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-( * )->E" );
    EXPECT_SIMPLE_ARC(            "C-(333)->D",               Cout, D,    333,  M_STAT,   ival  );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[=>2D]",            Ein,              2 );
    EXPECT_NO_ARC_IN_ARRAY(       "No rev E -> C",      dyn,  Ein,  C             );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, E,    444,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(444)->E" );

    //    *-( * )->E    3
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, E, NULL, WILD, VGX_ARCDIR_IN   ), &zero_timeout, disconnect_FWD ) == 3, "Remove *-( * )->E" );
    EXPECT_SIMPLE_ARC(            "C-(333)->D",               Cout, D,    333,  M_STAT,   ival  );
    EXPECT_NO_ARCS(               "D -> None",                Dout                );
    EXPECT_NO_ARCS(               "rev E -> None",            Ein                 );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, C, E,    444,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add C-(444)->E" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    555,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(555)->E" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, D, E,    666,  VGX_ARCDIR_OUT  ), connect_event  ) == 1, "Add D-(666)->E" );

    // Original graph still in place
    EXPECT_SIMPLE_ARC(            "A-(111)->B",               Aout, B,  111,  M_STAT,   ival    );
    EXPECT_SIMPLE_ARC(            "B-(222)->C",               Bout, C,  222,  M_STAT,   ival    );
    EXPECT_ARRAY_OF_ARCS(         "C->[D,E]",                 Cout,             2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(333)->D",         dyn,  Cout, D,  333,  M_STAT,   ival    );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "C-(444)->E",         dyn,  Cout, E,  444,  M_STAT,   ival    );
    EXPECT_ARRAY_OF_ARCS(         "D->[=>2E]",                Dout,             2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "D=(555,666)=>E",     dyn,  Dout, E,          2 );
    EXPECT_SIMPLE_ARC(            "rev B-(111)->A",           Bin,  A,  111,  M_STAT,   ival    );
    EXPECT_SIMPLE_ARC(            "rev C-(222)->B",           Cin,  B,  222,  M_STAT,   ival    );
    EXPECT_SIMPLE_ARC(            "rev D-(333)->C",           Din,  C,  333,  M_STAT,   ival    );
    EXPECT_ARRAY_OF_ARCS(         "rev E->[C,=>2D]",          Ein,              3 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev E-(444)->C",     dyn,  Ein,  C,  444,  M_STAT,   ival    );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev E=(555,666)=>D", dyn,  Ein,  D,          2 );

    // Deconstruct
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, A, B,   111,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->B" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, B, C,   222,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove B-(222)->C" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, D,   333,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-(333)->D" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, C, E,   444,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove C-(444)->E" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, E,   555,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove D-(555)->E" );
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY( &arc, D, E,   666,  VGX_ARCDIR_OUT  ), &zero_timeout, disconnect_REV ) == 1, "Remove D-(666)->E" );

    // Now empty
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );
    EXPECT_NO_ARCS(               "D -> None",                Dout  );
    EXPECT_NO_ARCS(               "None <- D",                Din   );
    EXPECT_NO_ARCS(               "E -> None",                Eout  );
    EXPECT_NO_ARCS(               "None <- E",                Ein   );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &D ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &E ) == true, "" );

    igraph->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___C, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___D, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___E, 0, NULL, NULL );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * MORE ADVANCED WILDCARD REMOVE SCENARIOS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "More advanced wildcard remove scenarios" ) {
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

    vgx_Vertex_t *A, *B, *C, *D = NULL;
    vgx_ArcVector_cell_t *Ain, *Bin, *Cin, *Din;
    vgx_ArcVector_cell_t *Aout, *Bout, *Cout, *Dout;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    D = igraph->simple->OpenVertex( graph, CSTR___D, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );

    TEST_ASSERTION( A && B && C && D, "Created vertices" );

    Ain = _vxvertex__get_vertex_inarcs( A );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Cin = _vxvertex__get_vertex_inarcs( C );
    Din = _vxvertex__get_vertex_inarcs( D );

    Aout = _vxvertex__get_vertex_outarcs( A );
    Bout = _vxvertex__get_vertex_outarcs( B );
    Cout = _vxvertex__get_vertex_outarcs( C );
    Dout = _vxvertex__get_vertex_outarcs( D );

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );
    EXPECT_NO_ARCS(               "D -> None",                Dout  );
    EXPECT_NO_ARCS(               "None <- D",                Din   );

    /*
    GRAPH:

         A=(111,222,333)=>B
          \=(222,333)=>C
           \-(333)->D
            
         
         A-(444)->B   0
         A-(444)->D   0
         A-(444)->*   0

         A-(111)->B   1
         A-(111)->C   0
         A-(111)->D   0

         A-(222)->B   1
         A

         A-( * )->B   3
         A-( * )->C   2
         A-( * )->D   1

         A-(*)->*     6 

    */

    // Build the graph
    //     A=(111,222,333)=>B
    //      \=(222,333)=>C
    //       \-(333)->D
    //
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 111, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 222, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, B, 333, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(333)->B" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 222, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(222)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, C, 333, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(333)->C" );
    TEST_ASSERTION( iarcvector.Add( dyn, SET_STATIC_ARC( &arc, A, D, 333, VGX_ARCDIR_OUT ), connect_event ) == 1, "Add A-(333)->D" );
    EXPECT_ARRAY_OF_ARCS(         "A->[=>3B,=>2C,D]",         Aout,             6 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(111,222,333)=>B", dyn,  Aout, B,          3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(222,333)=>C",     dyn,  Aout, C,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(333)->D",         dyn,  Aout, D,  333,  M_STAT,   ival    );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>3A]",            Bin,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(111,222,333)=>A",dyn,Bin, A,          3 );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2A]",            Cin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(222,333)=>A", dyn,  Cin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev D-(333)->A",           Din,  A,  333,  M_STAT,   ival    );

    vgx_predicator_rel_enum WILD = VGX_PREDICATOR_REL_WILDCARD;

    // -----------------------------------------------------------

    //  A-(111)->B  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,     111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(111)->B" );

    //  A-(111)->C  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, C,     111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(111)->C does not exist" );

    //  A-(111)->D  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, D,     111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(111)->D does not exist" );

    //  A-(222)->B  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,     222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->B" );

    //  A-(222)->C  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, C,     222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(222)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->C" );

    //  A-(222)->D  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, D,     222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(222)->D does not exist" );

    //  A-(333)->B  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,     333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(333)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->B" );

    //  A-(333)->C  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, C,     333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(333)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->C" );

    //  A-(333)->D  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, D,     333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(333)->D" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->D" );

    //  A-(444)->B  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,     444, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(444)->B does not exist" );

    //  A-(444)->C  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, C,     444, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(444)->C does not exist" );

    //  A-(444)->D  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, D,     444, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(444)->D does not exist" );

    //  A-( * )->B  3
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, B,    WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 3, "Remove A-( * )->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->B" );

    //  A-( * )->C  2
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, C,    WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 2, "Remove A-( * )->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->C" );

    //  A-( * )->D  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, D,    WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-( * )->D" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->D" );

    //  A-(111)->*  1
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL,  111, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 1, "Remove A-(111)->*" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(111)->B" );

    //  A-(222)->*  2
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL,  222, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 2, "Remove A-(222)->*" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(222)->C" );

    //  A-(333)->*  3
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL,  333, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 3, "Remove A-(333)->*" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add    A-(333)->D" );

    //  A-(444)->*  0
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL,  444, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 0, "A-(444)->* does not exist" );

    //  A-( * )->*  6
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL, WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 6, "Remove A-( * )->*" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->D" );

    // *-(111)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, B, NULL,  111, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(111)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(111)->B" );

    // *-(111)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL,  111, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(111)->C does not exist" );

    // *-(111)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL,  111, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(111)->D does not exist" );

    // *-(222)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, B, NULL,  222, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->B" );

    // *-(222)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL,  222, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(222)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->C" );

    // *-(222)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL,  222, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(222)->D does not exist" );

    // *-(333)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, B, NULL,  333, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(333)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->B" );

    // *-(333)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL,  333, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(333)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->C" );

    // *-(333)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL,  333, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-(333)->D" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->D" );

    // *-(444)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, B, NULL,  444, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(444)->B does not exist" );

    // *-(444)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL,  444, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(444)->C does not exist" );

    // *-(444)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL,  444, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 0, "*-(444)->D does not exist" );

    // *-(*)->B
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, B, NULL, WILD, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 3, "Remove *-( * )->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     111, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(111)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->B" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, B,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->B" );

    // *-(*)->C
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, C, NULL, WILD, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 2, "Remove *-( * )->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     222, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(222)->C" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, C,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->C" );

    // *-(*)->D
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, D, NULL, WILD, VGX_ARCDIR_IN  ), &zero_timeout, disconnect_FWD ) == 1, "Remove *-( * )->D" );
    TEST_ASSERTION( iarcvector.Add(    dyn, SET_STATIC_ARC(     &arc, A, D,     333, VGX_ARCDIR_OUT ), connect_event  ) == 1, "Add A-(333)->D" );

    // Verify original graph
    EXPECT_ARRAY_OF_ARCS(         "A->[=>3B,=>2C,D]",         Aout,             6 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(111,222,333)=>B", dyn,  Aout, B,          3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(222,333)=>C",     dyn,  Aout, C,          2 );
    EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(333)->D",         dyn,  Aout, D,  333,  M_STAT,   ival    );
    EXPECT_ARRAY_OF_ARCS(         "rev B->[=>3A]",            Bin,              3 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(111,222,333)=>A",dyn,Bin, A,          3 );
    EXPECT_ARRAY_OF_ARCS(         "rev C->[=>2A]",            Cin,              2 );
    EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev C=(222,333)=>A", dyn,  Cin,  A,          2 );
    EXPECT_SIMPLE_ARC(            "rev D-(333)->A",           Din,  A,  333,  M_STAT,   ival    );

    // Remove graph
    TEST_ASSERTION( __api_arcvector_remove_arc( dyn, SET_ARC_REL_QUERY(  &arc, A, NULL, WILD, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV ) == 6, "Remove A-( * )->*" );

    // Now empty
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    EXPECT_NO_ARCS(               "C -> None",                Cout  );
    EXPECT_NO_ARCS(               "None <- C",                Cin   );
    EXPECT_NO_ARCS(               "D -> None",                Dout  );
    EXPECT_NO_ARCS(               "None <- D",                Din   );


    // Destroy vertices
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &D ) == true, "" );

    igraph->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___C, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___D, 0, NULL, NULL );

  } END_TEST_SCENARIO




  /*******************************************************************//**
   * EXHAUSTIVE DIAMOND TEST
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Exhaustive diamond test" ) {
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

    vgx_ArcVector_cell_t *Ain, *Bin, *Vin;
    vgx_ArcVector_cell_t *Aout, *Bout, *Vout;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Vertex_t *A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    vgx_Vertex_t *B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );

    TEST_ASSERTION( A && B, "Created vertices" );

    Ain = _vxvertex__get_vertex_inarcs( A );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Aout = _vxvertex__get_vertex_outarcs( A );
    Bout = _vxvertex__get_vertex_outarcs( B );

    int max_via = 472; // guarantee testing of INTERNAL frame (not just LEAF) in V
    int max_rel = 709; // guarantee testing of INTERNAL frame (not just LEAF) in MAV
    vgx_predicator_rel_enum base_rel = __VGX_PREDICATOR_REL_START_USER_RANGE - 1; // m will be 1-based, see later
    vgx_predicator_rel_enum rel;



    int64_t n_added;
    int64_t n_removed;
    int64_t n_arcs = 0;

    // make it 1-based for easier test doc
    vgx_Vertex_t **V = (vgx_Vertex_t**)malloc( sizeof(vgx_Vertex_t*) * (max_via+1) );
    const CString_t *CSTR__name;
    for( int i = 1; i <= max_via; i++ ) {
      CSTR__name = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      TEST_ASSERTION( (V[i] = igraph->simple->OpenVertex( graph, CSTR__name, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Via Vertex %s created", CStringValue(CSTR__name) );
      CStringDelete( CSTR__name );
    }

    // make sure it's empty before we start
    EXPECT_NO_ARCS(               "A -> None",                Aout  );
    EXPECT_NO_ARCS(               "None <- A",                Ain   );
    EXPECT_NO_ARCS(               "B -> None",                Bout  );
    EXPECT_NO_ARCS(               "None <- B",                Bin   );
    for( int i = 1; i <= max_via; i++ ) {
      EXPECT_NO_ARCS(   "Vin",    _vxvertex__get_vertex_inarcs(  V[i] ) );
      EXPECT_NO_ARCS(   "Vout",   _vxvertex__get_vertex_outarcs( V[i] ) );
    }

    //
    // Increase the number of Via vertices gradually from 1 to max
    //
    // Build graph with variable number of via vertices between A and B,
    // and with variable arc-size between vertices
    //

    //
    //
    // V[1]...V[n_via]  (n = power of two to finish test in reasonable amount of time)
    for( int n_via = 1; n_via <= max_via; n_via = 1 + (int)(n_via * 1.5) ) {
      // rel_1...rel_m_rel  (m = power of two to finish test in reasonable amount of time)
      for( int m_rel = 1; m_rel <= max_rel; m_rel = 1 + (int)(m_rel * 1.5) ) {
        printf( "GRAPH: n_via=%d/%d  m_rel=%d/%d ...", n_via, max_via, m_rel, max_rel );
        fflush( stdout );
        // CREATE GRAPH:
        //   /==(1...m)==>V[1]==(1...m)==\    .
        //  A===(1...m)==>V[2]==(1...m)===>B  .
        //   \==(1...m)==>V[n]==(1...m)==/    .
        for( int n = 1; n <= n_via; n++ ) {
          // Produce A=(1...m)=>V[n]=(1...m)=>B
          for( int m = 1; m <= m_rel; m++ ) {
            // Add A-(rel,M_INT,n*m)->V[n]-(rel,M_FLT,n*m)->B
            rel = base_rel + m;
            ival.integer = n+m;
            TEST_ASSERTION( (n_added = iarcvector.Add( dyn, SET_ARC( &arc, A, V[n], rel, M_INT, ival, VGX_ARCDIR_OUT ), connect_event )) == 1, "Should add A-(0x%04x,0x%02x,%d)->V[%d]", rel, M_INT.bits, ival.integer, n );
            n_arcs += n_added;
            rval.real = (float)n*m;
            TEST_ASSERTION( (n_added = iarcvector.Add( dyn, SET_ARC( &arc, V[n], B, rel, M_FLT, rval, VGX_ARCDIR_OUT ), connect_event )) == 1, "Should add V[%d]-(0x%04x,0x%02x,%g)->B", n, rel, M_FLT.bits, rval.real );
            n_arcs += n_added;
          }
        }
        // VERIFY GRAPH
        if( n_via == 1 ) {
          Vin = _vxvertex__get_vertex_inarcs( V[1] );
          Vout = _vxvertex__get_vertex_outarcs( V[1] );
          if( m_rel == 1 ) {
            // 
            //  A --> V1 --> B
            //
            rel = base_rel + 1;
            ival.integer = n_via + m_rel;
            rval.real = (float)n_via * m_rel;
            EXPECT_SIMPLE_ARC(              "A-(r)->V1",                Aout,   V[1],   rel, M_INT, ival );
            EXPECT_SIMPLE_ARC(              "V1-(r)->B",                Vout,   B,      rel, M_FLT, rval );
            EXPECT_SIMPLE_ARC(              "rev V1-(r)-A",             Vin,    A,      rel, M_INT, ival );
            EXPECT_SIMPLE_ARC(              "rev B-(r)-V1",             Bin,    V[1],   rel, M_FLT, rval );
          }
          else {
            //
            //  A ==> V1 ==> B
            //
            EXPECT_ARRAY_OF_ARCS(           "A->[=>mV1]",               Aout,                   m_rel );
            EXPECT_MULTIPLE_ARC_IN_ARRAY(   "A-(1...m)->V1",      dyn,  Aout,   V[1],           m_rel );
            EXPECT_ARRAY_OF_ARCS(           "V1->[=>mB]",               Vout,                   m_rel );
            EXPECT_MULTIPLE_ARC_IN_ARRAY(   "V1-(1...m)->B",      dyn,  Vout,   B,              m_rel );
            EXPECT_ARRAY_OF_ARCS(           "rev V1->[=>mA]",           Vin,                    m_rel );
            EXPECT_MULTIPLE_ARC_IN_ARRAY(   "rev V1-(1...m)->A",  dyn,  Vin,    A,              m_rel );
            EXPECT_ARRAY_OF_ARCS(           "rev B->[=>mV1]",           Bin,                    m_rel );
            EXPECT_MULTIPLE_ARC_IN_ARRAY(   "rev B-(1...m)->V1",  dyn,  Bin,    V[1],           m_rel );
          }
        }
        else {
          if( m_rel == 1 ) {
            //    /--> V1 --\      .
            //  A ---> V2 ---> B   .
            //    \--> Vn --/      .
            EXPECT_ARRAY_OF_ARCS(           "A->[V1,V2,...,Vn]",        Aout,                   n_via );
            EXPECT_ARRAY_OF_ARCS(           "rev B->[V1,V2,...,Vn]",    Bin,                    n_via );
            rel = base_rel + m_rel;
            for( int i = 1; i <= n_via; i++ ) {
              Vin = _vxvertex__get_vertex_inarcs( V[i] );
              Vout = _vxvertex__get_vertex_outarcs( V[i] );
              ival.integer = i + m_rel;
              rval.real = (float)i * m_rel;
              EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(r)->Vi",          dyn,  Aout,   V[i], rel, M_INT, ival  );
              EXPECT_SIMPLE_ARC(            "Vi-(r)->B",                Vout,   B,    rel, M_FLT, rval  );
              EXPECT_SIMPLE_ARC(            "rev Vi-(r)->A",            Vin,    A,    rel, M_INT, ival  );
              EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(r)->Vi",      dyn,  Bin,    V[i], rel, M_FLT, rval  );
            }

          }
          else {
            //    /==> V1 ==\      .
            //  A ===> V2 ===> B   .
            //    \==> Vn ==/      .
            EXPECT_ARRAY_OF_ARCS(           "A->[=>mV1,...,=>mVn]",     Aout,                   n_via * m_rel );
            EXPECT_ARRAY_OF_ARCS(           "rev B->[=>mV1,...,=>mVn]", Bin,                    n_via * m_rel );
            for( int i = 1; i <= n_via; i++ ) {
              Vin = _vxvertex__get_vertex_inarcs( V[i] );
              Vout = _vxvertex__get_vertex_outarcs( V[i] );
              EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(1...m)=>Vi",      dyn,  Aout,   V[i],           m_rel );
              EXPECT_ARRAY_OF_ARCS(         "Vi->[=>mB]",               Vout,                   m_rel );
              EXPECT_MULTIPLE_ARC_IN_ARRAY( "Vi=(1...m)=>B",      dyn,  Vout,   B,              m_rel );
              EXPECT_ARRAY_OF_ARCS(         "rev Vi->[=>mA]",           Vin,                    m_rel );
              EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev Vi=>(1...m)->A", dyn,  Vin,    A,              m_rel );
              EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(1...m)=>Vi",  dyn,  Bin,    V[i],           m_rel );
            }
          }
        }

        printf( "CREATED, now removing ..." );
        fflush( stdout );

        // BREAK DOWN GRAPH GRADUALLY AND SPOT CHECK GRAPH SHAPE
        //
        //
        vgx_predicator_mod_t M_ANY_INT = { .bits = VGX_PREDICATOR_MOD_INTEGER };
        vgx_predicator_mod_t M_ANY_FLT = { .bits = VGX_PREDICATOR_MOD_FLOAT };
        vgx_predicator_val_t dcval = { .bits = 0 };

        for( int m = 1; m <= m_rel; m++ ) {
          // Remove A-(rel,M_INT,*)->*
          rel = base_rel + m;
          TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_QUERY( &arc, A, NULL, rel, M_ANY_INT, dcval, VGX_ARCDIR_OUT ), &zero_timeout, disconnect_REV )) == n_via, "Remove %d arcs A-(%d)->*", n_via, rel );
          n_arcs -= n_removed;
          // Remove *-(rel)->B
          TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_QUERY( &arc, B, NULL, rel, M_ANY_FLT, dcval, VGX_ARCDIR_IN ), &zero_timeout, disconnect_FWD )) == n_via, "Remove %d arcs *-(%d)->B", n_via, rel );
          n_arcs -= n_removed;

          // VERIFY GRAPH
          if( n_via == 1 ) {
            Vin = _vxvertex__get_vertex_inarcs( V[1] );
            Vout = _vxvertex__get_vertex_outarcs( V[1] );
            if( m == m_rel ) {
              //
              //  A     V1    B
              //
              EXPECT_NO_ARCS(                 "A -> None",                Aout  );
              EXPECT_NO_ARCS(                 "V1 -> None",               Vout  );
              EXPECT_NO_ARCS(                 "rev V1 -> A",              Vin   );
              EXPECT_NO_ARCS(                 "rev B - > None",           Bin   );
            }
            else if( m == m_rel - 1 ) {
              // 
              //  A --> V1 --> B
              //
              rel = base_rel + m_rel;
              ival.integer = n_via + m_rel;
              rval.real = (float)n_via * m_rel;
              EXPECT_SIMPLE_ARC(              "A-(r)->V1",                Aout,   V[1], rel, M_INT, ival  );
              EXPECT_SIMPLE_ARC(              "V1-(r)->B",                Vout,   B,    rel, M_FLT, rval  );
              EXPECT_SIMPLE_ARC(              "rev V1-(r)-A",             Vin,    A,    rel, M_INT, ival  );
              EXPECT_SIMPLE_ARC(              "rev B-(r)-V1",             Bin,    V[1], rel, M_FLT, rval  );
            }
            else {
              //
              //  A ==> V1 ==> B
              //
              EXPECT_ARRAY_OF_ARCS(           "A->[=>mV1]",               Aout,                   m_rel - m );
              EXPECT_MULTIPLE_ARC_IN_ARRAY(   "A-(1...m)->V1",      dyn,  Aout,   V[1],           m_rel - m );
              EXPECT_ARRAY_OF_ARCS(           "V1->[=>mB]",               Vout,                   m_rel - m );
              EXPECT_MULTIPLE_ARC_IN_ARRAY(   "V1-(1...m)->B",      dyn,  Vout,   B,              m_rel - m );
              EXPECT_ARRAY_OF_ARCS(           "rev V1->[=>mA]",           Vin,                    m_rel - m );
              EXPECT_MULTIPLE_ARC_IN_ARRAY(   "rev V1-(1...m)->A",  dyn,  Vin,    A,              m_rel - m );
              EXPECT_ARRAY_OF_ARCS(           "rev B->[=>mV1]",           Bin,                    m_rel - m );
              EXPECT_MULTIPLE_ARC_IN_ARRAY(   "rev B-(1...m)->V1",  dyn,  Bin,    V[1],           m_rel - m );
            }
          }
          else {
            if( m == m_rel ) {
              //         V1          .
              //  A      V2          .
              //         Vn          .
              for( int i = 1; i <= n_via; i++ ) {
                Vin = _vxvertex__get_vertex_inarcs( V[i] );
                Vout = _vxvertex__get_vertex_outarcs( V[i] );
                EXPECT_NO_ARCS(               "A -> None",                Aout  );
                EXPECT_NO_ARCS(               "Vi -> None",               Vout  );
                EXPECT_NO_ARCS(               "rev Vi -> A",              Vin   );
                EXPECT_NO_ARCS(               "rev B - > None",           Bin   );
              }
            }
            else if( m == m_rel - 1 ) {
              //    /--> V1 --\      .
              //  A ---> V2 ---> B   .
              //    \--> Vn --/      .
              EXPECT_ARRAY_OF_ARCS(           "A->[V1,V2,...,Vn]",        Aout,                   n_via );
              EXPECT_ARRAY_OF_ARCS(           "rev B->[V1,V2,...,Vn]",    Bin,                    n_via );
              for( int i = 1; i <= n_via; i++ ) {
                Vin = _vxvertex__get_vertex_inarcs( V[i] );
                Vout = _vxvertex__get_vertex_outarcs( V[i] );
                rel = base_rel + m_rel;
                ival.integer = i + m_rel;
                rval.real = (float)i * m_rel;
                EXPECT_SIMPLE_ARC_IN_ARRAY(   "A-(r)->Vi",          dyn,  Aout,   V[i], rel, M_INT, ival  );
                EXPECT_SIMPLE_ARC(            "Vi-(r)->B",                Vout,   B,    rel, M_FLT, rval  );
                EXPECT_SIMPLE_ARC(            "rev Vi-(r)->A",            Vin,    A,    rel, M_INT, ival  );
                EXPECT_SIMPLE_ARC_IN_ARRAY(   "rev B-(r)->Vi",      dyn,  Bin,    V[i], rel, M_FLT, rval  );
              }

            }
            else if( m > m_rel - 10 || m % 17 == 0 ) {  // <= spot check multiple arcs (but do all last 10 towards end of deconstruction)
              //    /==> V1 ==\      .
              //  A ===> V2 ===> B   .
              //    \==> Vn ==/      .
              EXPECT_ARRAY_OF_ARCS(           "A->[=>mV1,...,=>mVn]",     Aout,                   n_via * (m_rel - m) );
              EXPECT_ARRAY_OF_ARCS(           "rev B->[=>mV1,...,=>mVn]", Bin,                    n_via * (m_rel - m) );
              for( int i = 1; i <= n_via; i++ ) {
                Vin = _vxvertex__get_vertex_inarcs( V[i] );
                Vout = _vxvertex__get_vertex_outarcs( V[i] );
                EXPECT_MULTIPLE_ARC_IN_ARRAY( "A=(1...m)=>Vi",      dyn,  Aout,   V[i],           m_rel - m );
                EXPECT_ARRAY_OF_ARCS(         "Vi->[=>mB]",               Vout,                   m_rel - m );
                EXPECT_MULTIPLE_ARC_IN_ARRAY( "Vi=(1...m)=>B",      dyn,  Vout,   B,              m_rel - m );
                EXPECT_ARRAY_OF_ARCS(         "rev Vi->[=>mA]",           Vin,                    m_rel - m );
                EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev Vi=>(1...m)->A", dyn,  Vin,    A,              m_rel - m );
                EXPECT_MULTIPLE_ARC_IN_ARRAY( "rev B=(1...m)=>Vi",  dyn,  Bin,    V[i],           m_rel - m );
              }
            }
          }
        }

        printf( "REMOVED\n" );

        TEST_ASSERTION( n_arcs == 0, "All arcs removed" );
        EXPECT_NO_ARCS(               "A -> None",                Aout  );
        EXPECT_NO_ARCS(               "None <- A",                Ain   );
        EXPECT_NO_ARCS(               "B -> None",                Bout  );
        EXPECT_NO_ARCS(               "None <- B",                Bin   );
        for( int i = 1; i <= max_via; i++ ) {
          EXPECT_NO_ARCS(   "Vin",    _vxvertex__get_vertex_inarcs(  V[i] ) );
          EXPECT_NO_ARCS(   "Vout",   _vxvertex__get_vertex_outarcs( V[i] ) );
        }

      }
    }


    // Destroy vertices
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );

    igraph->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );

    for( int i = 1; i <= max_via; i++ ) {
      TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V[i] ) == true, "" );
      CSTR__name = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      igraph->simple->DeleteVertex( graph, CSTR__name, 0, NULL, NULL );
      CStringDelete( CSTR__name );
    }

    free( V );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY THE TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Graph" ) {
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );
    CStringDelete( CSTR___D );
    CStringDelete( CSTR___E );
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    COMLIB_OBJECT_DESTROY(graph);
  } END_TEST_SCENARIO

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_name );
  CStringDelete( CSTR__graph_path );

} END_UNIT_TEST


#endif
