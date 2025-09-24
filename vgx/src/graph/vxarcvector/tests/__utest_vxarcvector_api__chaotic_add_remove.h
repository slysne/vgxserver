/*######################################################################
 *#
 *# __utest_vxarcvector_api__chaotic_add_remove.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXARCVECTOR_API__CHAOTIC_ADD_REMOVE_H
#define __UTEST_VXARCVECTOR_API__CHAOTIC_ADD_REMOVE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxarcvector_api__chaotic_add_remove ) {

  const CString_t *CSTR__graph_path_small = CStringNewFormat( "%s_small", TestName );
  const CString_t *CSTR__graph_path_large = CStringNewFormat( "%s_large", TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path_small && CSTR__graph_path_large && CSTR__graph_name, "graph_path and graph_name created" );

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( GetCurrentTestDirectory(), false );

  vgx_Graph_t *graph = NULL;
  vgx_Graph_vtable_t *igraph = NULL;
  framehash_dynamic_t *dyn = NULL;

  const CString_t *CSTR__idstr = NULL;


  /*******************************************************************//**
   * CREATE A TEST GRAPH FOR SMALL CHAOS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Test Graph For Small Chaos" ) {
    vgx_Graph_constructor_args_t graph_args = {
      .CSTR__graph_path     = CSTR__graph_path_small,
      .CSTR__graph_name     = CSTR__graph_name,
      .vertex_block_order   = 10,
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
   * ALLOCATOR VERIFICATION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Allocator Verification Before Small Chaos" ) {
    int64_t n_vertices = 1;
    do {
      // Create and destroy an increasingly larger set of vertices
      for( int i=0; i<n_vertices; i++ ) {
        CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
        TEST_ASSERTION( igraph->simple->CreateVertexSimple( graph, CStringValue( CSTR__idstr ), NULL ) == true, "Vertex '%s' should be created", CStringValue(CSTR__idstr) );
        CStringDelete( CSTR__idstr );
      }
      // Destroy vertices
      for( int i=0; i<n_vertices; i++ ) {
        CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
        TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__idstr, 0, NULL, NULL ) == 1, "Vertex '%s' should be destroyed", CStringValue(CSTR__idstr) );
        CStringDelete( CSTR__idstr );
      }
      n_vertices *= 2;
    } while( n_vertices <= (1<<20) );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CHAOS - SMALL, DENSE GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Chaos - small, dense graph until 1/8 complete" ) {
    int64_t n_vertices = 32;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Vertex_t **V;
    vgx_ArcVector_cell_t **Vin, **Vout;
    vgx_Arc_t arc;
    vgx_predicator_rel_enum rel;

    int64_t a, b;
    int64_t n_arcs = 0;
    int64_t max_arcs = 16383 * n_vertices * (n_vertices-1) / 2;
    int64_t completeness_arcs = max_arcs / 8;

    int64_t n = 0;

    // Create vertex array and arcvector arrays
    V = (vgx_Vertex_t**)malloc( sizeof(vgx_Vertex_t*) * n_vertices );
    Vin = (vgx_ArcVector_cell_t**)malloc( sizeof(vgx_ArcVector_cell_t*) * n_vertices );
    Vout = (vgx_ArcVector_cell_t**)malloc( sizeof(vgx_ArcVector_cell_t*) * n_vertices );

    TEST_ASSERTION( V && Vin && Vout, "malloc() worked" );

    // Create vertices
    for( int i=0; i<n_vertices; i++ ) {
      CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      V[i] = igraph->simple->OpenVertex( graph, CSTR__idstr, VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
      TEST_ASSERTION( V[i], "Vertex %s constructed", CStringValue(CSTR__idstr) );
      Vin[i] = _vxvertex__get_vertex_inarcs( V[i] );
      Vout[i] = _vxvertex__get_vertex_outarcs( V[i] );
      CStringDelete( CSTR__idstr );
    }

    do {
      // iterations
      n += 1;

      // random add
      a = rand64() % n_vertices;
      b = rand64() % n_vertices;
      while( (rel = rand16() & 0x3FFF) == 0 );

      SET_STATIC_ARC( &arc, V[a], V[b], rel == 0 ? 1 : rel, VGX_ARCDIR_OUT );
      n_arcs += iarcvector.Add( dyn, &arc, _vxgraph_arc__connect_WL_reverse_WL );

      // random remove sometimes
      if( (rand64() & 0xff) == 0xff ) {
        a = rand64() % n_vertices;
        rel = rand16() & 0x3FFF;

        // sometimes rel to any node (sometimes 0=* too)
        if( (rand64() & 0xf) == 0xf ) {
          SET_ARC_REL_QUERY( &arc, V[a], NULL, rel, VGX_ARCDIR_OUT );
        }
        // often rel to specific node
        else {
          b = rand64() % n_vertices;
          SET_ARC_REL_QUERY( &arc, V[a], V[b], rel, VGX_ARCDIR_OUT );
        }

        n_arcs -= __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, _vxgraph_arc__disconnect_WL_reverse_WL );
      }

      TEST_ASSERTION( n_arcs >= 0, "Number of arcs never less than zero" );
      TEST_ASSERTION( n_arcs <= max_arcs, "Number of arcs never greater than complete graph with full capacity predicator multiarcs" );

      if( n % 100000 == 0 ) {
        int64_t n_in = 0;
        int64_t n_out = 0;
        for( int i=0; i<n_vertices; i++ ) {
          n_out += iarcvector.Degree( Vout[i] );
          n_in += iarcvector.Degree( Vin[i] );
        }
        TEST_ASSERTION( n_out == n_in, "Number of inarcs equals number of outarcs" );
        TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() and Remove() reported" );
        printf( "ARCS = %lld\n", n_arcs );
      }

    } while( n_arcs < completeness_arcs );

    // Relase all vertices
    for( int i=0; i<n_vertices; i++ ) {
      TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V[i] ) == true, "Vertex number %d should be released", i );
    }

    // Destroy all vertices
    for( int i=0; i<n_vertices; i++ ) {
      CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__idstr, 0, NULL, NULL ) >= 0, "Vertex '%s' should be destroyed or virtualized", CStringValue(CSTR__idstr) );
      CStringDelete( CSTR__idstr );
    }

    // Graph should be empty
    TEST_ASSERTION( GraphOrder( graph ) == 0 && GraphSize( graph ) == 0, "No vertices or edges in graph" );

    // Destroy vertex array and arcvector arrays
    free( V );
    free( Vin );
    free( Vout );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Graph" ) {
    COMLIB_OBJECT_DESTROY( graph );
    graph = NULL;
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE A TEST GRAPH FOR LARGE CHAOS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Test Graph For Large Chaos" ) {
    vgx_Graph_constructor_args_t graph_args = {
      .CSTR__graph_path     = CSTR__graph_path_large,
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
   * ALLOCATOR VERIFICATION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Allocator Verification Before Large Chaos" ) {
    int64_t n_vertices = 1;
    do {
      // Create and destroy an increasingly larger set of vertices
      for( int i=0; i<n_vertices; i++ ) {
        CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
        TEST_ASSERTION( igraph->simple->CreateVertexSimple( graph, CStringValue( CSTR__idstr ), NULL ) == true, "Vertex '%s' should be created", CStringValue(CSTR__idstr) );
        CStringDelete( CSTR__idstr );
      }
      // Destroy vertices
      for( int i=0; i<n_vertices; i++ ) {
        CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
        TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__idstr, 0, NULL, NULL ) == 1, "Vertex '%s' should be destroyed", CStringValue(CSTR__idstr) );
        CStringDelete( CSTR__idstr );
      }
      n_vertices *= 2;
    } while( n_vertices <= (1<<18) );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CHAOS - LARGE, SPARSE GRAPH, FIXED VERTEX COUNT
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Chaos for 15 minutes - large, sparse graph" ) {
    int64_t n_vertices = (1<<16);
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();

    vgx_Vertex_t **V;
    vgx_ArcVector_cell_t **Vin, **Vout;
    vgx_ArcVector_cell_t *arcs;

    vgx_Arc_t arc;
    vgx_predicator_rel_enum rel;

    int64_t a, b;
    int64_t n_arcs = 0;
    int64_t max_arcs = 16383 * n_vertices * (n_vertices-1) / 2;

    int64_t degree;
    int64_t delta;
    int64_t n_added;
    int64_t n_removed;

    int64_t arc_limit = 300000000;
    unsigned multiadd_rate = 1;      // 1-99 percentage of iterations where a multiple arc is added
    unsigned mass_removal_rate = 1;  // 1-99 percentage of iterations where a mass removal is performed

    int64_t n = 0;
    int test_duration = 900;
    time_t next_rate_update_t;  // timestamp when new randomized multiadd and mass removal rates are computed
    time_t ts0, ts1;

    // Create vertex array and arcvector arrays
    V = (vgx_Vertex_t**)malloc( sizeof(vgx_Vertex_t*) * n_vertices );
    Vin = (vgx_ArcVector_cell_t**)malloc( sizeof(vgx_ArcVector_cell_t*) * n_vertices );
    Vout = (vgx_ArcVector_cell_t**)malloc( sizeof(vgx_ArcVector_cell_t*) * n_vertices );
    // Create vertices
    for( int i=0; i<n_vertices; i++ ) {
      CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      V[i] = igraph->simple->OpenVertex( graph, CSTR__idstr , VGX_VERTEX_ACCESS_WRITABLE, 0, NULL, NULL );
      TEST_ASSERTION( V[i], "Vertex %s constructed", CStringValue(CSTR__idstr) );
      Vout[i] = _vxvertex__get_vertex_outarcs( V[i] );
      Vin[i] = _vxvertex__get_vertex_inarcs( V[i] );
      CStringDelete( CSTR__idstr );
    }

    time( &ts0 );

    next_rate_update_t = ts0;
    
    do {
      // iterations
      n += 1;
      if( n % 100000 == 0 ) {
        printf( "\n\niterations: %lld\n\n", n );
      }
      // random add
      a = rand64() % n_vertices;
      b = rand64() % n_vertices;
      do {
        degree = iarcvector.Degree( Vout[a] );
        n_added = 0;
        if( rand64() % 100 < multiadd_rate ) {
          // X% of the time add a randomly sized wide multiple arc
          int wide = rand32() % 10000;
          for( int i=0; i<wide; i++ ) {
            rel = rand16() % VGX_PREDICATOR_REL_MAX + 1; // lfsr, all will be unique (0 not allowed!)
            SET_STATIC_ARC( &arc, V[a], V[b], rel, VGX_ARCDIR_OUT );
            n_added += iarcvector.Add( dyn, &arc, _vxgraph_arc__connect_WL_reverse_WL );
          }
        }
        else {
          // (100-X)% of the time add a simple arc (may overwrite, or may result in creation of multiple arc)
          rel = rand16() % VGX_PREDICATOR_REL_MAX + 1; // disallow 0
          SET_STATIC_ARC( &arc, V[a], V[b], rel, VGX_ARCDIR_OUT );
          n_added += iarcvector.Add( dyn, &arc, _vxgraph_arc__connect_WL_reverse_WL );
        }
        delta = iarcvector.Degree( Vout[a] ) - degree;
        TEST_ASSERTION( delta == n_added, "added %lld arcs, degree delta %lld reported", n_added, delta );
        n_arcs += n_added;
      } WHILE_ZERO;

      // random remove
      a = rand64() % n_vertices;
      b = rand64() % n_vertices;
      do {
        n_removed = 0;
        if( rand64() % 100 < mass_removal_rate ) {
          // X% of the time do a mass removal
          f_Vertex_disconnect_event disconnect_event = NULL;
          int r = ((unsigned)rand32()) % 5;
          switch( r ) {
          case 0:
            // 20% remove all outarcs A-(*)->*
            SET_ARC_WILD_QUERY( &arc, V[a], NULL, VGX_ARCDIR_OUT );
            arcs = &arc.tail->outarcs;
            disconnect_event = _vxgraph_arc__disconnect_WL_reverse_WL;
            break;
          case 1:
            // 20% remove all inarcs *-(*)->A
            SET_ARC_WILD_QUERY( &arc, V[a], NULL, VGX_ARCDIR_IN ); // reverse!
            arcs = &arc.tail->inarcs;
            disconnect_event = _vxgraph_arc__disconnect_WL_forward_WL;
            break;
          case 2:
            // 20% remove all outarcs A-(*)->B
            SET_ARC_WILD_QUERY( &arc, V[a], V[b], VGX_ARCDIR_OUT );
            arcs = &arc.tail->outarcs;
            disconnect_event = _vxgraph_arc__disconnect_WL_reverse_WL;
            break;
          case 3:
            // 20% remove all outarcs A-(rel)->*
            rel = rand16() % VGX_PREDICATOR_REL_MAX + 1; // LFSR, all will be unique 1-65535
            SET_ARC_STATIC_QUERY( &arc, V[a], NULL, rel, VGX_ARCDIR_OUT );
            arcs = &arc.tail->outarcs;
            disconnect_event = _vxgraph_arc__disconnect_WL_reverse_WL;
            break;
          case 4:
            // 20% remove all inarcs *-(rel)->A
            rel = rand16() % VGX_PREDICATOR_REL_MAX + 1; // LFSR, all will be unique 1-65535
            SET_ARC_STATIC_QUERY( &arc, V[a], NULL, rel, VGX_ARCDIR_IN );
            arcs = &arc.tail->inarcs;
            disconnect_event = _vxgraph_arc__disconnect_WL_forward_WL;
            break;
          default:
            TEST_ASSERTION( false, "Invalid case" );
          }
          degree = iarcvector.Degree( arcs );
          n_removed = __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, disconnect_event );
          delta = degree - iarcvector.Degree( arcs );
        }
        else {
          // (100-X)% of the time remove a specific arc A-(rel)->B
          rel = rand16() % VGX_PREDICATOR_REL_MAX + 1; // LFSR, all will be unique 1-65535
          SET_ARC_STATIC_QUERY( &arc, V[a], V[b], rel, VGX_ARCDIR_OUT );
          arcs = &arc.tail->outarcs;
          degree = iarcvector.Degree( arcs );
          n_removed = __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, _vxgraph_arc__disconnect_WL_reverse_WL );
          delta = degree - iarcvector.Degree( arcs );
        }
        delta = degree - iarcvector.Degree( arcs );
        TEST_ASSERTION( delta == n_removed, "all %lld arcs removed, degree delta %lld reported", n_removed, delta );
        n_arcs -= n_removed;
      } WHILE_ZERO;

      TEST_ASSERTION( n_arcs >= 0, "Number of arcs never less than zero" );
      TEST_ASSERTION( n_arcs <= max_arcs, "Number of arcs never greater than complete graph with full capacity predicator multiarcs" );

      if( n % 50000 == 0 ) { // spot check every so often
        int64_t n_in = 0;
        int64_t n_out = 0;
        for( int i=0; i<n_vertices; i++ ) {
          n_out += iarcvector.Degree( Vout[i] );
          n_in += iarcvector.Degree( Vin[i] );
        }
        TEST_ASSERTION( n_out == n_in, "Total indegree = outdegree, got in=%lld, out=%lld, arcs=%lld", n_in, n_out, n_arcs );
        TEST_ASSERTION( n_out == n_arcs, "Number of arcs is consistent with what Add() and Remove() reported, got in=%lld, out=%lld, arcs=%lld", n_in, n_out, n_arcs );
        printf( "ARCS = %lld\n", n_arcs );

        if( n_arcs > arc_limit ) {
          double lim = ((double)rand32() / CXLIB_ULONG_MAX) / 4; // 0% - 25% of original
          int64_t threshold = (int64_t)(arc_limit * lim);
          printf( "MAX ARCS reached, reducing to %.1f%% of original (target=%lld)\n", lim*100, threshold );
          int m = 10;
          while( n_arcs > threshold ) {
            if( m > 0 ) {
              --m;
              for( int i=0; i<n_vertices; i++ ) {
                rel = rand32() % VGX_PREDICATOR_REL_MAX + 1;
                TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY(&arc, V[i], NULL, rel, VGX_ARCDIR_OUT), &zero_timeout, _vxgraph_arc__disconnect_WL_reverse_WL )) >= 0, "Removal ok");
                n_arcs -= n_removed;
                rel = rand32() % VGX_PREDICATOR_REL_MAX + 1;
                TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_STATIC_QUERY(&arc, V[i], NULL, rel, VGX_ARCDIR_IN), &zero_timeout, _vxgraph_arc__disconnect_WL_forward_WL )) >= 0, "Removal ok" );
                n_arcs -= n_removed;
              }
            }
            else {
              int i = rand32() % (int)n_vertices;
              TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_WILD_QUERY(&arc, V[i], NULL, VGX_ARCDIR_OUT), &zero_timeout, _vxgraph_arc__disconnect_WL_reverse_WL )) >= 0, "Removal ok" );
              n_arcs -= n_removed;
              TEST_ASSERTION( (n_removed = __api_arcvector_remove_arc( dyn, SET_ARC_WILD_QUERY(&arc, V[i], NULL, VGX_ARCDIR_IN), &zero_timeout, _vxgraph_arc__disconnect_WL_forward_WL )) >= 0, "Removal ok" );
              n_arcs -= n_removed;
            }
            if( rand32() % 100 < 2 ) {
              printf( "   (ARCS = %lld)\n", n_arcs );
            }
          }
        }
      }

      time( &ts1 );

      if( ts1 > next_rate_update_t ) {
        // update the randomized multiadd and mass removal rates
        multiadd_rate = rand32() % 10 + 1;      // 1-10 % of the time
        mass_removal_rate = rand32() % 19 + 2;  // 2-20 % of the time
        printf( " new multiadd rate:     %d %%\n", multiadd_rate );
        printf( " new mass removal rate: %d %%\n", mass_removal_rate );
        next_rate_update_t += 30; //
      }

    } while( ts1 - ts0 < test_duration );

    // Remove all arcs
    for( int i=0; i<n_vertices; i++ ) {
      degree = iarcvector.Degree( Vout[i] );
      SET_ARC_WILD_QUERY( &arc, V[i], NULL, VGX_ARCDIR_OUT );
      n_removed = __api_arcvector_remove_arc( dyn, &arc, &zero_timeout, _vxgraph_arc__disconnect_WL_reverse_WL );
      delta = degree - iarcvector.Degree( &arc.tail->outarcs );
      TEST_ASSERTION( delta == n_removed, "all %lld arcs removed, degree delta %lld reported", n_removed, delta );
    }

    // Check zero degree
    for( int i=0; i<n_vertices; i++ ) {
      TEST_ASSERTION( iarcvector.Degree( Vout[i] ) == 0, "No outarcs" );
      TEST_ASSERTION( iarcvector.Degree( Vin[i] ) == 0, "No inarcs" );
    }

    // Release all vertices
    for( int i=0; i<n_vertices; i++ ) {
      TEST_ASSERTION( igraph->simple->CloseVertex( graph, &V[i] ) == true, "Vertex number %d should be released", i );
    }

    // Destroy vertices
    for( int i=0; i<n_vertices; i++ ) {
      CSTR__idstr = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "V_%d", i );
      TEST_ASSERTION( igraph->simple->DeleteVertex( graph, CSTR__idstr, 0, NULL, NULL ) >= 0, "Vertex '%s' should be destroyed or virtualized", CStringValue(CSTR__idstr) );
      CStringDelete( CSTR__idstr );
    }

    // Graph should be empty
    TEST_ASSERTION( GraphOrder( graph ) == 0 && GraphSize( graph ) == 0, "No vertices or edges in graph" );

    // Destroy vertex array and arcvector arrays
    free( V );
    free( Vin );
    free( Vout );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESTROY TEST GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Test Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    COMLIB_OBJECT_DESTROY( graph );
  } END_TEST_SCENARIO


  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path_small );
  CStringDelete( CSTR__graph_path_large );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST


#endif
