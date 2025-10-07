/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxarcvector_api__basic_get.h
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

#ifndef __UTEST_VXARCVECTOR_API__BASIC_GET_H
#define __UTEST_VXARCVECTOR_API__BASIC_GET_H

#include "__vxtest_macro.h"

BEGIN_UNIT_TEST( __utest_vxarcvector_api__basic_get ) {

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
  vgx_Vertex_t *V = NULL;
  vgx_Vertex_t *A = NULL;
  vgx_Vertex_t *B = NULL;
  vgx_Vertex_t *C = NULL;
  vgx_ArcVector_cell_t *Vin = NULL;
  vgx_ArcVector_cell_t *Vout = NULL;
  vgx_ArcVector_cell_t *Ain = NULL;
  vgx_ArcVector_cell_t *Aout = NULL;
  vgx_ArcVector_cell_t *Bin = NULL;
  vgx_ArcVector_cell_t *Bout = NULL;
  vgx_ArcVector_cell_t *Cin = NULL;
  vgx_ArcVector_cell_t *Cout = NULL;
  

  f_Vertex_connect_event connect_event = _vxgraph_arc__connect_WL_reverse_WL;

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

    CSTR___V = NewEphemeralCString( graph, "V" );
    CSTR___A = NewEphemeralCString( graph, "A" );
    CSTR___B = NewEphemeralCString( graph, "B" );
    CSTR___C = NewEphemeralCString( graph, "C" );

  } END_TEST_SCENARIO

  /*******************************************************************//**
   * CREATE AN INITIAL (SOURCE) TEST VERTEX V
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Initial Vertex" ) {
    V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( V != NULL, "initial vertex constructed" );
    Vin = _vxvertex__get_vertex_inarcs( V );
    Vout = _vxvertex__get_vertex_outarcs( V );
    TEST_ASSERTION( __arcvector_cell_type( Vin ) == VGX_ARCVECTOR_NO_ARCS, "initial vertex has no inarcs" );
    TEST_ASSERTION( __arcvector_cell_type( Vout ) == VGX_ARCVECTOR_NO_ARCS, "initial vertex has no outarcs" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
  } END_TEST_SCENARIO

  /*******************************************************************//**
   * CREATE TERMINAL (DESTINATION) VERTICES A, B, AND C
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Terminal Vertices" ) {
    A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    Ain = _vxvertex__get_vertex_inarcs( A );
    Aout = _vxvertex__get_vertex_outarcs( A );
    Bin = _vxvertex__get_vertex_inarcs( B );
    Bout = _vxvertex__get_vertex_outarcs( B );
    Cin = _vxvertex__get_vertex_inarcs( C );
    Cout = _vxvertex__get_vertex_outarcs( C );
    TEST_ASSERTION( A && B && C, "terminal vertices are constructed" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );
  } END_TEST_SCENARIO

  /*******************************************************************//**
   * CONNECT VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Connect Vertices" ) {
    vgx_Arc_t arc;

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );
    TEST_ASSERTION( (A = igraph->simple->OpenVertex( graph, CSTR___A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired A" );
    TEST_ASSERTION( (B = igraph->simple->OpenVertex( graph, CSTR___B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired B" );
    TEST_ASSERTION( (C = igraph->simple->OpenVertex( graph, CSTR___C, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired C" );

    //
    //  /-(101)----------->\
    // V=(101,102)=>A-(103)->C
    //  \-(105)->B-(104)-->/
    //
    // V=(101,102)=>A
    SET_STATIC_ARC( &arc, V, A, 101, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Added V-(101)->A" );
    SET_STATIC_ARC( &arc, V, A, 102, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Added V-(102)->A" );
    // A-(103)->C
    SET_STATIC_ARC( &arc, A, C, 103, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Added A-(103)->C" );
    // B-(104)->C
    SET_STATIC_ARC( &arc, B, C, 104, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Added B-(104)->C" );
    // V-(105)->B
    SET_STATIC_ARC( &arc, V, B, 105, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "ADDED V-(105)->B" );
    // V-(101)->C
    SET_STATIC_ARC( &arc, V, C, 101, VGX_ARCDIR_OUT );
    TEST_ASSERTION( iarcvector.Add( dyn, &arc, connect_event ) == 1, "Added V-(101)->C" );

    TEST_ASSERTION( iarcvector.Degree( Vin )  == 0, "V has 0 inarcs"  );
    TEST_ASSERTION( iarcvector.Degree( Vout ) == 4, "V has 4 outarcs" );
    TEST_ASSERTION( iarcvector.Degree( Ain )  == 2, "A has 2 inarcs"  );
    TEST_ASSERTION( iarcvector.Degree( Aout ) == 1, "A has 1 outarc"  );   
    TEST_ASSERTION( iarcvector.Degree( Bin )  == 1, "B has 1 inarc"   );
    TEST_ASSERTION( iarcvector.Degree( Bout ) == 1, "B has 1 outarc"  );
    TEST_ASSERTION( iarcvector.Degree( Cin )  == 3, "C has 3 inarcs"  );
    TEST_ASSERTION( iarcvector.Degree( Cout ) == 0, "C has 0 outarcs" );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &A ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &B ) == true, "" );
    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &C ) == true, "" );

  } END_TEST_SCENARIO

  /*******************************************************************//**
   * QUERY VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Query Vertices" ) {

    vgx_NeighborhoodQuery_t *query = iGraphQuery.NewNeighborhoodQuery( graph, CStringValue( CSTR___V ), NULL, VGX_COLLECTOR_MODE_COLLECT_VERTICES, NULL );
    CALLABLE( query )->SetTimeout( query, 0, false );

    vgx_ranking_context_t ranking_context = {0};
    ranking_context.sortspec = VGX_SORTBY_NONE;
    ranking_context.graph = graph;
    ranking_context.readonly_graph = false;
    ranking_context.timing_budget = vgx_query_timing_budget( (vgx_BaseQuery_t*)query );
    
    vgx_collect_counts_t counts = {0};
    counts.data_size = 1000; // big enough
    counts.hits = counts.data_size;
    counts.n_collect = counts.hits;

    vgx_VertexCollector_context_t *collector = iGraphCollector.NewVertexCollector( graph, &ranking_context, (vgx_BaseQuery_t*)query, &counts );

    TEST_ASSERTION( collector != NULL,    "Collector created" );
    Cm256iList_t *L = collector->container.sequence.list;


    vgx_GenericArcFilter_context_t pass_filter = {
      .type               = VGX_ARC_FILTER_TYPE_PASS,
      .positive_match     = true,
      .filter             = arcfilterfunc.Pass,
      .timing_budget      = vgx_query_timing_budget( (vgx_BaseQuery_t*)query ),
      .previous_context   = NULL,
      .current_tail       = NULL,
      .current_head       = NULL,
      .superfilter        = NULL,
      .terminal           = {
          .current          = NULL,
          .list             = NULL,
          .logic            = VGX_LOGICAL_NO_LOGIC
      },
      .arcfilter_callback = NULL,
      .logic              = VGX_LOGICAL_NO_LOGIC,
      .pred_condition1    = VGX_PREDICATOR_ANY,
      .pred_condition2    = VGX_PREDICATOR_ANY,
      .vertex_probe       = NULL,
      .culleval           = NULL,
      .locked_cull        = 0
    };

    vgx_virtual_ArcFilter_context_t *no_filter = (vgx_virtual_ArcFilter_context_t*)&pass_filter;

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );

    vgx_neighborhood_probe_t probe = {
      .graph                  = graph,
      .conditional = {
        .arcfilter              = no_filter,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        }
      },
      .traversing = {
        .arcfilter              = no_filter,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        }
      },
      .distance               = 1,
      .readonly_graph         = false,
      .collector_mode         = VGX_COLLECTOR_MODE_COLLECT_VERTICES,
      .current_tail_RO        = V,
      .common_collector       = (vgx_BaseCollector_context_t*)collector,
      .pre_evaluator          = NULL,
      .post_evaluator         = NULL,
      .collect_filter_context = no_filter
    };

    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "List is empty before query" );
    
    // We have this:
    //
    //  /-(101)----------->\
    // V=(101,102)=>A-(103)->C
    //  \-(105)->B-(104)-->/
    //

    iarcvector.GetVertices( Vin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "0 vertices connect to V" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Vout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 3, "V connects to 3 vertices (A, B, C)" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Ain, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "1 vertex (V) connects to A" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Aout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "A connects to 1 vertex (C)" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Bin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "1 vertex (V) connects to B" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Bout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "B connects to 1 vertex (C)" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Cin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 3, "3 vertices (V,A,B) connect to C" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetVertices( Cout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "C connects to 0 vertices" );
    CALLABLE(L)->Clear(L);

    iGraphCollector.DeleteCollector( &probe.common_collector );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );

    iGraphQuery.DeleteNeighborhoodQuery( &query );

  } END_TEST_SCENARIO

  /*******************************************************************//**
   * QUERY ARCS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Query Arcs" ) {

    vgx_NeighborhoodQuery_t *query = iGraphQuery.NewDefaultNeighborhoodQuery( graph, CStringValue( CSTR___V ), NULL );
    CALLABLE( query )->SetTimeout( query, 0, false );

    vgx_ranking_context_t ranking_context = {0};
    ranking_context.sortspec = VGX_SORTBY_NONE;
    ranking_context.graph = graph;
    ranking_context.readonly_graph = false;
    ranking_context.timing_budget = vgx_query_timing_budget( (vgx_BaseQuery_t*)query );
    
    vgx_collect_counts_t counts = {0};
    counts.data_size = 1000; // big enough
    counts.hits = counts.data_size;
    counts.n_collect = counts.hits;

    vgx_ArcCollector_context_t *collector = iGraphCollector.NewArcCollector( graph, &ranking_context, (vgx_BaseQuery_t*)query, &counts );

    TEST_ASSERTION( collector != NULL,    "Collector created" );
    Cm256iList_t *L = collector->container.sequence.list;

    vgx_GenericArcFilter_context_t pass_filter = {
      .type               = VGX_ARC_FILTER_TYPE_PASS,
      .positive_match     = true,
      .filter             = arcfilterfunc.Pass,
      .timing_budget      = vgx_query_timing_budget( (vgx_BaseQuery_t*)query ),
      .previous_context   = NULL,
      .current_tail       = NULL,
      .current_head       = NULL,
      .superfilter        = NULL,
      .terminal           = {
          .current          = NULL,
          .list             = NULL,
          .logic            = VGX_LOGICAL_NO_LOGIC
      },
      .arcfilter_callback = NULL,
      .logic              = VGX_LOGICAL_NO_LOGIC,
      .pred_condition1    = VGX_PREDICATOR_ANY,
      .pred_condition2    = VGX_PREDICATOR_ANY,
      .vertex_probe       = NULL,
      .culleval           = NULL,
      .locked_cull        = 0
    };

    vgx_virtual_ArcFilter_context_t *no_filter = (vgx_virtual_ArcFilter_context_t*)&pass_filter;

    TEST_ASSERTION( (V = igraph->simple->OpenVertex( graph, CSTR___V, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL )) != NULL, "Acquired V" );

    vgx_neighborhood_probe_t probe = {
      .graph                  = graph,
      .conditional = {
        .arcfilter              = no_filter,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        }
      },
      .traversing = {
        .arcfilter              = no_filter,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        }
      },
      .distance               = 1,
      .readonly_graph         = false,
      .collector_mode         = VGX_COLLECTOR_MODE_COLLECT_ARCS,
      .current_tail_RO        = V,
      .common_collector       = (vgx_BaseCollector_context_t*)collector,
      .pre_evaluator          = NULL,
      .post_evaluator         = NULL,
      .collect_filter_context = no_filter
    };

    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "Arc List is empty before query" );
    
    // We have this:
    //
    //  /-(101)----------->\
    // V=(101,102)=>A-(103)->C
    //  \-(105)->B-(104)-->/
    //

    iarcvector.GetArcs( Vin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "0 arcs inbound on V" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Vout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 4, "V has 4 arcs outbound ( -(101)->C, -(101,102)=>A, -(105)->B )" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Ain, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 2, "2 arcs ( V=(101,102)=> ) inbound on A" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Aout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "A has 1 arc outbound ( -(103)->C )" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Bin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "1 arc ( V-(105)-> ) inbound on B" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Bout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 1, "B has 1 arc outbound ( -(104)->C )" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Cin, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 3, "3 arcs ( V-(101)->, A-(103)->, B-(104)-> ) inbound on C" );
    CALLABLE(L)->Clear(L);

    iarcvector.GetArcs( Cout, &probe );
    TEST_ASSERTION( CALLABLE(L)->Length(L) == 0, "C has 0 arcs outbound" );
    CALLABLE(L)->Clear(L);

    iGraphCollector.DeleteCollector( &probe.common_collector );

    TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V ) == true, "" );

    iGraphQuery.DeleteNeighborhoodQuery( &query );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY ALL THE TEST VERTICES
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Vertices" ) {
    igraph->simple->DeleteVertex( graph, CSTR___V, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___A, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___B, 0, NULL, NULL );
    igraph->simple->DeleteVertex( graph, CSTR___C, 0, NULL, NULL );
  } END_TEST_SCENARIO

  /*******************************************************************//**
   * DESTROY THE TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Graph" ) {
    CStringDelete( CSTR___V );
    CStringDelete( CSTR___A );
    CStringDelete( CSTR___B );
    CStringDelete( CSTR___C );
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    COMLIB_OBJECT_DESTROY(graph);
  } END_TEST_SCENARIO


  CStringDelete( CSTR__graph_name );
  CStringDelete( CSTR__graph_path );

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

} END_UNIT_TEST


#endif
