/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_aggregator.c
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

#include "_vxcollector.h"
#include "_vxarcvector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static void _vxquery_aggregator__delete_aggregator( vgx_BaseCollector_context_t **collector );
static vgx_ArcCollector_context_t * _vxquery_aggregator__new_neighborhood_aggregator( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget );
static vgx_VertexCollector_context_t * _vxquery_aggregator__new_vertex_aggregator( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget );


DLL_HIDDEN IGraphAggregator_t iGraphAggregator = {
  .DeleteAggregator           = _vxquery_aggregator__delete_aggregator,
  .NewNeighborhoodAggregator  = _vxquery_aggregator__new_neighborhood_aggregator,
  .NewVertexAggregator        = _vxquery_aggregator__new_vertex_aggregator,
};




/**************************************************************************//**
 * __get_arc_degree
 *
 ******************************************************************************
 */
__inline static int64_t __get_arc_degree( const vgx_ArcVector_cell_t *arcs ) {
  _vgx_ArcVector_VxD_tag tag = TPTR_AS_TAG( &arcs->VxD );
  return tag == VGX_ARCVECTOR_VxD_DEGREE ? __arcvector_get_degree( arcs ) : tag == VGX_ARCVECTOR_VxD_EMPTY ? 0 : 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __aggregate_predicator( const vgx_LockableArc_t *larc, vgx_aggregator_fields_t *fields ) {
  if( fields->predval ) {
    if( larc->head.predicator.mod.stored.f ) {
      fields->predval->real += larc->head.predicator.val.real;
    }
    else {
      fields->predval->integer += larc->head.predicator.val.integer;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __aggregate_vertex_fields( const vgx_Vertex_t *vertex, vgx_aggregator_fields_t *fields ) {
  if( fields->degree ) {
    *(fields->degree) += __get_arc_degree( &vertex->inarcs ) + __get_arc_degree( &vertex->outarcs );
  }
  if( fields->indegree ) {
    *(fields->indegree) += __get_arc_degree( &vertex->inarcs );
  }
  if( fields->outdegree ) {
    *(fields->outdegree) += __get_arc_degree( &vertex->outarcs );
  }
}



/*******************************************************************//**
 * This is the collector->add_arc function
 * NOTE: Instead of a comlib sequence as the first argument we pass a
 *       reference to the aggregator fields instead. Not a clean solution
 *       but it works.
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __aggregate_neighborhood( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  // NOTE: the __ignore arc is needed for signature compatibility with the neighborhood collector which uses both collect_arc and sortby_arc.
  vgx_aggregator_fields_t *fields = collector->container.fields;
  __aggregate_predicator( larc, fields );
  // Aggregate vertex fields unless already done (i.e. during multiple arc aggregation)
  if( fields->_this_vertex != larc->head.vertex ) {
    vgx_Vertex_t *head = _vxquery_collector__safe_head_access_OPEN( (vgx_BaseCollector_context_t*)collector, larc );
    if( head == NULL ) {
      return -1; // timeout
    }
    __aggregate_vertex_fields( head, fields );
    fields->_this_vertex = larc->head.vertex;
  }
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __aggregate_neighborhood_quick( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __aggregate_global( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_aggregator_fields_t *fields = collector->container.fields;
  vgx_Vertex_t *head = _vxquery_collector__safe_head_access_OPEN( (vgx_BaseCollector_context_t*)collector, larc );
  if( head == NULL ) {
    return -1; // timeout
  }
  __aggregate_vertex_fields( larc->head.vertex, fields );
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __aggregate_global_quick( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_aggregator__delete_aggregator( vgx_BaseCollector_context_t **collector ) {
  free( *collector );
  *collector = NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_field_access( const vgx_aggregator_fields_t *fields, vgx_ResponseAttrFastMask *fieldmask, bool *head_access ) {
  *fieldmask = VGX_RESPONSE_ATTRS_NONE;
  *head_access = false;
  if( fields ) {
    if( fields->predval ) {
      *fieldmask |= VGX_RESPONSE_ATTR_VALUE;
    }
    if( fields->degree ) {
      *fieldmask |= VGX_RESPONSE_ATTR_DEGREE;
    }
    if( fields->indegree ) {
      *fieldmask |= VGX_RESPONSE_ATTR_INDEGREE;
    }
    if( fields->outdegree ) {
      *fieldmask |= VGX_RESPONSE_ATTR_OUTDEGREE;
    }
    if( *fieldmask & VGX_RESPONSE_ATTRS_DEGREES ) {
      *head_access = true;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCollector_context_t * _vxquery_aggregator__new_neighborhood_aggregator( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget ) {

  // Create the collector context
  vgx_ArcCollector_context_t *collector = calloc( 1, sizeof(vgx_ArcCollector_context_t) );
  if( collector == NULL ) {
    return NULL;
  }

  vgx_ResponseAttrFastMask fieldmask;
  bool head_access;
  __set_field_access( fields, &fieldmask, &head_access );


  // We will collect using an aggregator function
  collector->type               = VGX_COLLECTOR_TYPE_NEIGHBORHOOD_AGGREGATION;
  collector->graph              = graph;
  collector->ranker             = NULL;
  collector->container.fields   = fields; // maybe NULL
  collector->refmap             = NULL;
  collector->sz_refmap          = 0;
  collector->postheap           = NULL;
  collector->size               = 1;
  collector->n_remain           = LLONG_MAX;
  collector->n_collectable      = 0;
  collector->locked_tail_access = false;
  collector->locked_head_access = head_access;
  collector->fieldmask          = fieldmask;
  collector->timing_budget      = timing_budget;
  collector->collect_arc        = fields ? __aggregate_neighborhood : __aggregate_neighborhood_quick;
  collector->stage_arc          = _iStageArc.no_stage;
  collector->post_collect       = _iCollectArc.no_collect;
  collector->post_stage         = _iStageArc.no_stage;
  collector->postfilter         = NULL;
  collector->n_arcs             = 0;
  collector->n_neighbors        = 0;
  collector->counts_are_deep    = true;

  // The collector context is now ready
  return collector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCollector_context_t * _vxquery_aggregator__new_vertex_aggregator( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget ) {

  // Create the collector context
  vgx_VertexCollector_context_t *collector = calloc( 1, sizeof(vgx_VertexCollector_context_t) );
  if( collector == NULL ) {
    return NULL;
  }

  vgx_ResponseAttrFastMask fieldmask;
  bool head_access;
  __set_field_access( fields, &fieldmask, &head_access );


  // We will collect using an aggregator function
  collector->type               = VGX_COLLECTOR_TYPE_GLOBAL_AGGREGATION;
  collector->graph              = graph; 
  collector->ranker             = NULL;
  collector->container.fields   = fields; // maybe NULL
  collector->refmap             = NULL;
  collector->sz_refmap          = 0;
  collector->postheap           = NULL;
  collector->size               = 1;
  collector->n_remain           = LLONG_MAX;
  collector->n_collectable      = 0;
  collector->locked_tail_access = false;
  collector->locked_head_access = head_access;
  collector->fieldmask          = fieldmask;
  collector->timing_budget      = timing_budget;
  collector->collect_vertex     = fields ? __aggregate_global: __aggregate_global_quick;
  collector->stage_vertex       = _iStageVertex.no_stage;
  collector->n_vertices         = 0;
  collector->counts_are_deep    = true;

  // The collector context is now ready
  return collector;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_aggregator.h"

test_descriptor_t _vgx_vxquery_aggregator_tests[] = {
  { "VGX Graph Aggregator Tests", __utest_vxquery_aggregator },
  {NULL}
};
#endif
