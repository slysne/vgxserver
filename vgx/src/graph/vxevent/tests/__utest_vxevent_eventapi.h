/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxevent_eventapi.h
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

#ifndef __UTEST_VXEVENT_EVENTAPI_H
#define __UTEST_VXEVENT_EVENTAPI_H

#include "__vxtest_macro.h"

BEGIN_UNIT_TEST( __utest_vxevent_eventapi ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();
  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = NULL;

  vgx_Graph_vtable_t *igraph = (vgx_Graph_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Graph_t );
  vgx_IGraphSimple_t *isimple = igraph->simple;
  vgx_Vertex_vtable_t *iV = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );


  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL,              "Graph constructed, graph=%llp", graph );
    TEST_ASSERTION( GraphOrder(graph) == 0,  "Graph has no vertices" );
    TEST_ASSERTION( GraphSize(graph) == 0,   "Graph has no edges" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * CREATE A VERTEX WITH EXPIRATION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Vertex with Expiration" ) {
    CString_t *CSTR__A = NewEphemeralCString( graph, "A" );
    CString_t *CSTR__B = NewEphemeralCString( graph, "B" );
    vgx_Vertex_t *A = NULL;
    vgx_Vertex_t *B = NULL;

    // Create vertex A
    A = isimple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( A != NULL,                                          "Vertex A should be created" );
    isimple->CloseVertex( graph, &A );

    // Create vertex B
    B = isimple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    TEST_ASSERTION( B != NULL,                                          "Vertex B should be created" );
    isimple->CloseVertex( graph, &B );

    // Make sure both exist
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__A ) == true,       "Vertex A should exist" );
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__B ) == true,       "Vertex B should exist" );

    // Set A to expire 10 seconds from now
    A = isimple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
    uint32_t ttl = 5;
    uint32_t now_ts = __SECONDS_SINCE_1970();
    uint32_t future_ts = now_ts + ttl;
    iV->SetExpirationTime( A, future_ts );
    isimple->CloseVertex( graph, &A );

    // Make sure both still exists
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__A ) == true,       "Vertex A should exist" );
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__B ) == true,       "Vertex B should exist" );

    // Sleep a short time - should not expire yet
    printf( "Sleep a short time...\n" );
    sleep_milliseconds( 2000 );

    // Make sure both still exists
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__A ) == true,       "Vertex A should exist" );
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__B ) == true,       "Vertex B should exist" );

    // Sleep until expiration of A should have occurred (give it ample time)
    printf( "Sleep until expiration...\n" );
    sleep_milliseconds( 1000 * (ttl+3) );

    // Make sure A is deleted but B exists
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__A ) == false,      "Vertex A should no longer exist" );
    TEST_ASSERTION( isimple->HasVertex( graph, CSTR__B ) == true,       "Vertex B should exist" );

    CStringDelete( CSTR__A );
    CStringDelete( CSTR__B );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE MANY VERTICES WITH SHORT TERM EXPIRATION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Many Vertices with Short Term Expiration" ) {
    const int N = 10000;
    const int max_ttl = 10;  // seconds
    const int exp_per_sec = N / max_ttl; // expirations per second
    const int max_delay = 5; // TTL should not take more than this many seconds after the scheduled tmx to perform deletion
    
    // Get initial graph order
    int64_t pre_order = GraphOrder( graph );

    uint32_t now_ts = __SECONDS_SINCE_1970();
    uint32_t max_tmx = now_ts + max_ttl;

    // Create vertices and set increasing expiration times
    for( int n=0; n<N; n++ ) {
      CString_t *CSTR__name = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", n );
      vgx_Vertex_t *vertex = isimple->OpenVertex( graph, CSTR__name, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
      uint32_t tmx = now_ts + n/exp_per_sec;
      iV->SetExpirationTime( vertex, tmx );
      isimple->CloseVertex( graph, &vertex );
      CStringDelete( CSTR__name );
    }

    // Wait for all vertices to expire
    int64_t order;
    while( (order = GraphOrder( graph )) > pre_order && (now_ts = __SECONDS_SINCE_1970()) <= max_tmx + max_delay ) {
      vgx_EventBacklogInfo_t backlog = iGraphEvent.BacklogInfo( graph );
      printf( "order=%lld  t_remain=%u  (%lld->%lld->[%lld/%lld]->%lld)\n", order, max_tmx-now_ts, backlog.n_api, backlog.n_input, backlog.n_long, backlog.n_short, backlog.n_exec );
      sleep_milliseconds( 1000 );
    }

    // Make sure all vertices are deleted at this time
    vgx_GlobalQuery_t *query = iGraphQuery.NewDefaultGlobalQuery( graph, NULL );
    isimple->Vertices( graph, query );
    iGraphResponse.FormatResultsToStream( graph, (vgx_BaseQuery_t*)query, stdout ); 
    iGraphQuery.DeleteGlobalQuery( &query );
    TEST_ASSERTION( (order = GraphOrder( graph )) == pre_order,    "All vertices should have expired"  );

    // Make sure we did not delete all vertices too early
    sleep_milliseconds( 1000 );
    TEST_ASSERTION( (now_ts = __SECONDS_SINCE_1970()) >= max_tmx,     "Vertices should not be deleted prematurely (now_ts=%u < max_tmx=%u)", now_ts, max_tmx );


  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE A LARGE NUMBER OF VERTICES WITH LONG AND SHORT TERM EXPIRATION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create A Large Number of Vertices with Long and Short Expiration" ) {
    const int N = 1000000;
    const int max_ttl = 200;  // seconds
    const int exp_per_sec = N / max_ttl; // expirations per second
    const int max_delay = 5; // TTL should not take more than this many seconds after the scheduled tmx to perform deletion
    
    // Get initial graph order
    int64_t pre_order = GraphOrder( graph );
    
    // Save original params
    vgx_EventParamInfo_t orig_param = *g_pevent_param;

    // Set event processor parameters to run actions more often than default to get things moving here
    GRAPH_LOCK( graph ) {
      g_pevent_param->LongTerm.migration_cycle_tms          = 2*60*1000;  // 2 minutes
      g_pevent_param->ShortTerm.insertion_threshold_tms     = 1*60*1000;  // 1 minute
      g_pevent_param->MediumTerm.migration_cycle_tms        =   40*1000;  // 40 seconds
    } GRAPH_RELEASE;

    // Create vertices
    INFO( 0, "Creating %ld vertices", N );
    for( int n=0; n<N; n++ ) {
      CString_t *CSTR__name = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", n );
      isimple->CreateVertexSimple( graph, CStringValue( CSTR__name ), NULL );
      CStringDelete( CSTR__name );
    }

    // Set increasing expiration time for all vertices
    uint32_t now_ts = __SECONDS_SINCE_1970();
    uint32_t offset_s = 60;
    uint32_t max_tmx = now_ts + offset_s + max_ttl;
    INFO( 0, "Setting expiration time for %ld vertices", N );
    for( int n=0; n<N; n++ ) {
      CString_t *CSTR__name = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", n );
      vgx_Vertex_t *vertex = isimple->OpenVertex( graph, CSTR__name, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
      uint32_t tmx = now_ts + offset_s + n/exp_per_sec;
      iV->SetExpirationTime( vertex, tmx );
      isimple->CloseVertex( graph, &vertex );
      CStringDelete( CSTR__name );
    }

    // Wait for all vertices to expire
    INFO( 0, "Waiting for %ld vertices to expire", N );
    int64_t order;
    while( (order = GraphOrder( graph )) > pre_order && (now_ts = __SECONDS_SINCE_1970()) <= max_tmx + max_delay ) {
      vgx_EventBacklogInfo_t backlog = iGraphEvent.BacklogInfo( graph );
      printf( "order=%lld  t_remain=%u  (%lld->%lld->[%lld/%lld]->%lld)\n", order, max_tmx-now_ts, backlog.n_api, backlog.n_input, backlog.n_long, backlog.n_short, backlog.n_exec );
      sleep_milliseconds( 1000 );
    }

    // Make sure all vertices are deleted at this time
    vgx_GlobalQuery_t *query = iGraphQuery.NewDefaultGlobalQuery( graph, NULL );
    isimple->Vertices( graph, query );
    iGraphResponse.FormatResultsToStream( graph, (vgx_BaseQuery_t*)query, stdout ); 
    iGraphQuery.DeleteGlobalQuery( &query );
    TEST_ASSERTION( (order = GraphOrder( graph )) == pre_order,    "All vertices should have expired"  );

    // Make sure we did not delete all vertices too early
    sleep_milliseconds( 1000 );
    TEST_ASSERTION( (now_ts = __SECONDS_SINCE_1970()) >= max_tmx,     "Vertices should not be deleted prematurely (now_ts=%u < max_tmx=%u)", now_ts, max_tmx );

    // Restore original scheduling parameters
    GRAPH_LOCK( graph ) {
      *g_pevent_param = orig_param;
    } GRAPH_RELEASE;

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY THE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    uint32_t owner;
    TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,          "Graph closed" );
    TEST_ASSERTION( graph == NULL,                                            "Graph pointer set to NULL" );
    TEST_ASSERTION( igraphfactory.DeleteGraph( CSTR__graph_path, CSTR__graph_name, 10, false, NULL ) == 1, "Graph deleted" );
  } END_TEST_SCENARIO

  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST




#endif
