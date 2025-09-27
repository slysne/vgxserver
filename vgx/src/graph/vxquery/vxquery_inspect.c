/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_inspect.c
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

#include "_vxarcvector.h"
#include "_vxcollector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static vgx_ArcFilter_match _vxquery_inspect__has_adjacency_OPEN_RO( vgx_Vertex_t *vertex_RO, vgx_adjacency_search_context_t *search, vgx_Arc_t *first_match );


DLL_HIDDEN IGraphInspect_t iGraphInspect = {
  .HasAdjacency = _vxquery_inspect__has_adjacency_OPEN_RO
};



/*******************************************************************//**
 *
 *
 * 1  : hit
 * 0  : miss
 * -1 : error
 ***********************************************************************
 */
static vgx_ArcFilter_match _vxquery_inspect__has_adjacency_OPEN_RO( vgx_Vertex_t *vertex_RO, vgx_adjacency_search_context_t *search, vgx_Arc_t *first_match ) {
  vgx_ArcFilter_match match;

  __assert_vertex_lock( vertex_RO );
  vgx_recursive_probe_t *RP = &search->probe->conditional;

  // Prefer conditional probe direction (same as traversing unless explicitly overridden)
  switch( RP->arcdir ) {
  case VGX_ARCDIR_IN:
    match = iarcvector.HasArc( &vertex_RO->inarcs, RP, search->probe, first_match ); // HIT(1), MISS(0), or ERROR(-1)
    break;
  case VGX_ARCDIR_OUT:
    match = iarcvector.HasArc( &vertex_RO->outarcs, RP, search->probe, first_match ); // HIT(1), MISS(0), or ERROR(-1)
    break;
  case VGX_ARCDIR_BOTH:
    // TODO: Why not use iarcvector.HasArcBidirectional ??
    match = iarcvector.GetArcsBidirectional( &vertex_RO->inarcs, &vertex_RO->outarcs, search->probe );
    break;
  default:
    match = __arcfilter_error();
  }

  if( __is_arcfilter_error( match ) ) {
    // Adjacency search timeout?
    if( _vgx_is_execution_halted( search->timing_budget ) ) {
      const void *obj = search->timing_budget->resource;
      __format_error_string( &search->CSTR__error, "Timeout during adjacency check @ %p", obj ); 
    }
  }

  return __arcfilter_THRU( RP, match );
}







#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_inspect.h"

test_descriptor_t _vgx_vxquery_inspect_tests[] = {
  { "VGX Graph Inspect Tests", __utest_vxquery_inspect },
  {NULL}
};
#endif
