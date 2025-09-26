/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_query.c
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

#include "_vgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );





// Vertex Condition
static void __initialize_vertex_condition( vgx_VertexCondition_t *vertex_condition );
static void __clear_vertex_condition( vgx_VertexCondition_t *vertex_condition );

// Ranking Condition
static void __clear_ranking_condition( vgx_RankingCondition_t *ranking_condition );

// Aggregator fields
static void __reset_aggregator_fields( vgx_aggregator_fields_t *fields );
static void __delete_aggregator_fields( vgx_aggregator_fields_t **fields );
static vgx_aggregator_fields_t * __new_aggregator_fields( void );
static void __copy_aggregator_fields( vgx_aggregator_fields_t *dest, const vgx_aggregator_fields_t *src );

// ???
static void __initialize_base_query( vgx_BaseQuery_t *query );
static int __copy_base_query( vgx_BaseQuery_t *dest, const vgx_BaseQuery_t *src );
static void __clear_base_query( vgx_BaseQuery_t *query );
static void __empty_base_query( vgx_BaseQuery_t *query );

static int __configure_adjacency_query( vgx_AdjacencyQuery_t *query, const char *vertex_id, CString_t **CSTR__error );
static int __copy_adjacency_query( vgx_AdjacencyQuery_t *dest, const vgx_AdjacencyQuery_t *src );
static void __clear_adjacency_query( vgx_AdjacencyQuery_t *query );


/*******************************************************************//**
 * IGraphQuery_t
 ***********************************************************************
 */
//
static void _vxquery_query__empty_adjacency_query( vgx_AdjacencyQuery_t *query );
static void _vxquery_query__empty_neighborhood_query( vgx_NeighborhoodQuery_t *query );
static void _vxquery_query__empty_global_query( vgx_GlobalQuery_t *query );
static void _vxquery_query__empty_aggregator_query( vgx_AggregatorQuery_t *query );
static void _vxquery_query__empty_query( vgx_BaseQuery_t *query );

//
static void _vxquery_query__reset_adjacency_query( vgx_AdjacencyQuery_t *query );
static void _vxquery_query__reset_neighborhood_query( vgx_NeighborhoodQuery_t *query );
static void _vxquery_query__reset_global_query( vgx_GlobalQuery_t *query );
static void _vxquery_query__reset_aggregator_query( vgx_AggregatorQuery_t *query );
static void _vxquery_query__reset_query( vgx_BaseQuery_t *query );

//
static void _vxquery_query__delete_adjacency_query( vgx_AdjacencyQuery_t **query );
static void _vxquery_query__delete_neighborhood_query( vgx_NeighborhoodQuery_t **query );
static void _vxquery_query__delete_global_query( vgx_GlobalQuery_t **query );
static void _vxquery_query__delete_aggregator_query( vgx_AggregatorQuery_t **query );
static void _vxquery_query__delete_query( vgx_BaseQuery_t **query );

//
static vgx_AdjacencyQuery_t *    _vxquery_query__new_adjacency_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
static vgx_NeighborhoodQuery_t * _vxquery_query__new_neighborhood_query( vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode, CString_t **CSTR__error );
static vgx_GlobalQuery_t *       _vxquery_query__new_global_query( vgx_Graph_t *graph, vgx_collector_mode_t collector_mode, CString_t **CSTR__error );
static vgx_AggregatorQuery_t *   _vxquery_query__new_aggregator_query( vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, CString_t **CSTR__error );

//
static vgx_AdjacencyQuery_t *    _vxquery_query__new_default_adjacency_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
static vgx_NeighborhoodQuery_t * _vxquery_query__new_default_neighborhood_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
static vgx_GlobalQuery_t *       _vxquery_query__new_default_global_query( vgx_Graph_t *graph, CString_t **CSTR__error );
static vgx_AggregatorQuery_t *   _vxquery_query__new_default_aggregator_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );

//
static vgx_AdjacencyQuery_t *    _vxquery_query__clone_adjacency_query( const vgx_AdjacencyQuery_t *other, CString_t **CSTR__error );
static vgx_NeighborhoodQuery_t * _vxquery_query__clone_neighborhood_query( const vgx_NeighborhoodQuery_t *other, CString_t **CSTR__error );
static vgx_GlobalQuery_t *       _vxquery_query__clone_global_query( const vgx_GlobalQuery_t *other, CString_t **CSTR__error );
static vgx_AggregatorQuery_t *   _vxquery_query__clone_aggregator_query( const vgx_AggregatorQuery_t *other, CString_t **CSTR__error );


DLL_EXPORT vgx_IGraphQuery_t iGraphQuery = {

  .EmptyAdjacencyQuery          = _vxquery_query__empty_adjacency_query,
  .EmptyNeighborhoodQuery       = _vxquery_query__empty_neighborhood_query,
  .EmptyGlobalQuery             = _vxquery_query__empty_global_query,
  .EmptyAggregatorQuery         = _vxquery_query__empty_aggregator_query,
  .EmptyQuery                   = _vxquery_query__empty_query,

  .ResetAdjacencyQuery          = _vxquery_query__reset_adjacency_query,
  .ResetNeighborhoodQuery       = _vxquery_query__reset_neighborhood_query,
  .ResetGlobalQuery             = _vxquery_query__reset_global_query,
  .ResetAggregatorQuery         = _vxquery_query__reset_aggregator_query,
  .ResetQuery                   = _vxquery_query__reset_query,

  .DeleteAdjacencyQuery         = _vxquery_query__delete_adjacency_query,
  .DeleteNeighborhoodQuery      = _vxquery_query__delete_neighborhood_query,
  .DeleteGlobalQuery            = _vxquery_query__delete_global_query,
  .DeleteAggregatorQuery        = _vxquery_query__delete_aggregator_query,
  .DeleteQuery                  = _vxquery_query__delete_query,

  .NewAdjacencyQuery            = _vxquery_query__new_adjacency_query,
  .NewNeighborhoodQuery         = _vxquery_query__new_neighborhood_query,
  .NewGlobalQuery               = _vxquery_query__new_global_query,
  .NewAggregatorQuery           = _vxquery_query__new_aggregator_query,

  .NewDefaultAdjacencyQuery     = _vxquery_query__new_default_adjacency_query,
  .NewDefaultNeighborhoodQuery  = _vxquery_query__new_default_neighborhood_query,
  .NewDefaultGlobalQuery        = _vxquery_query__new_default_global_query,
  .NewDefaultAggregatorQuery    = _vxquery_query__new_default_aggregator_query,

  .CloneAdjacencyQuery          = _vxquery_query__clone_adjacency_query,
  .CloneNeighborhoodQuery       = _vxquery_query__clone_neighborhood_query,
  .CloneGlobalQuery             = _vxquery_query__clone_global_query,
  .CloneAggregatorQuery         = _vxquery_query__clone_aggregator_query,
};



/*******************************************************************//**
 * IVertexCondition_t
 ***********************************************************************
 */
static vgx_VertexCondition_t * _vxquery_query__new_default_vertex_condition( bool positive_match );
static vgx_VertexCondition_t * _vxquery_query__clone_vertex_condition( const vgx_VertexCondition_t *other );
static void _vxquery_query__delete_vertex_condition( vgx_VertexCondition_t **vertex_condition );
static void _vxquery_query__reset_vertex_condition( vgx_VertexCondition_t *vertex_condition );
static void _vxquery_query__set_vertex_condition_require_manifestation( vgx_VertexCondition_t *vertex_condition, vgx_VertexStateContext_man_t manifestation );
static int _vxquery_query__set_vertex_condition_require_type( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const vgx_value_comparison vcomp, const char *type ); 
static vgx_VertexTypeEnumeration_t _vxquery_query__get_vertex_condition_type_enumeration( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph );

// local filter expressions
static int _vxquery_query__set_vertex_condition_require_local_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *local_filter_expression );
static int _vxquery_query__set_vertex_condition_require_post_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *post_filter_expression );

// degree
static int _vxquery_query__set_vertex_condition_require_degree( vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const vgx_arc_direction direction, const int64_t degree );
static void __delete_degree_condition( vgx_DegreeCondition_t **degree_condition );
static int _vxquery_query__set_vertex_condition_require_conditional_degree( vgx_VertexCondition_t *vertex_condition, vgx_DegreeCondition_t **degree_condition );

// similarity
static int _vxquery_query__set_vertex_condition_require_similarity( vgx_VertexCondition_t *vertex_condition, vgx_SimilarityCondition_t **similarity_condition );

// identifier
static int _vxquery_query__set_vertex_condition_require_identifier( vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const char *identifier );
static int _vxquery_query__set_vertex_condition_require_identifier_list( vgx_VertexCondition_t *vertex_condition, vgx_StringList_t **idlist );

// properties
static int _vxquery_query__init_vertex_condition_property_set( vgx_VertexCondition_t *vertex_condition, bool positive );
static int _vxquery_query__set_vertex_condition_require_property( vgx_VertexCondition_t *vertex_condition, vgx_VertexProperty_t **vertex_property );

// timestamp
static vgx_TimestampCondition_t * __new_timestamp_condition( bool positive );
static void __delete_timestamp_condition( vgx_TimestampCondition_t **timestamp_condition );
static int _vxquery_query__init_vertex_condition_timestamps( vgx_VertexCondition_t *vertex_condition, bool positive );
static int _vxquery_query__set_vertex_condition_require_TMC( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmc_condition ); 
static int _vxquery_query__set_vertex_condition_require_TMM( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmm_condition ); 
static int _vxquery_query__set_vertex_condition_require_TMX( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmx_condition ); 

// recursive condition
static void _vxquery_query__set_vertex_condition_require_recursive_condition( vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition );
static void _vxquery_query__set_vertex_condition_require_arc_condition( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set );
static int  _vxquery_query__set_vertex_condition_require_condition_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression );
static void  _vxquery_query__set_vertex_condition_assert_condition( vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match );

// recursive traversal
static void _vxquery_query__set_vertex_condition_require_recursive_traversal( vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition );
static void _vxquery_query__set_vertex_condition_require_arc_traversal( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set );
static int  _vxquery_query__set_vertex_condition_require_traversal_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression );
static void _vxquery_query__set_vertex_condition_neighbor_collector_mode( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode );
static void  _vxquery_query__set_vertex_condition_assert_traversal( vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match );

// has...
static int _vxquery_query__has_vertex_condition_manifestation( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_type( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_local_filter( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_post_filter( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_degree( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_conditional_degree( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_similarity( const vgx_VertexCondition_t *vertex_condition );
static vgx_Vector_t * _vxquery_query__own_vertex_condition_similarity_vector( vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_identifier( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_property( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_TMC( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_TMM( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_TMX( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_recursive_condition( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_arc_condition( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_condition_filter( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_assert_condition( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_recursive_traversal( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_arc_traversal( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_traversal_filter( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_assert_traversal( const vgx_VertexCondition_t *vertex_condition );
static int _vxquery_query__has_vertex_condition_collector( const vgx_VertexCondition_t *vertex_condition );





DLL_EXPORT vgx_IVertexCondition_t iVertexCondition = {
  // Lifecycle
  .New                      = _vxquery_query__new_default_vertex_condition,
  .Clone                    = _vxquery_query__clone_vertex_condition,
  .Delete                   = _vxquery_query__delete_vertex_condition,
  .Reset                    = _vxquery_query__reset_vertex_condition,

  // Conditions
  .RequireManifestation       = _vxquery_query__set_vertex_condition_require_manifestation,
  .RequireType                = _vxquery_query__set_vertex_condition_require_type,
  .GetTypeEnumeration         = _vxquery_query__get_vertex_condition_type_enumeration,
  .RequireLocalFilter         = _vxquery_query__set_vertex_condition_require_local_filter,
  .RequirePostFilter          = _vxquery_query__set_vertex_condition_require_post_filter,
  .RequireDegree              = _vxquery_query__set_vertex_condition_require_degree,
  .RequireConditionalDegree   = _vxquery_query__set_vertex_condition_require_conditional_degree,
  .RequireSimilarity          = _vxquery_query__set_vertex_condition_require_similarity,
  .RequireIdentifier          = _vxquery_query__set_vertex_condition_require_identifier,
  .RequireIdentifierList      = _vxquery_query__set_vertex_condition_require_identifier_list,
  .InitPropertyConditionSet   = _vxquery_query__init_vertex_condition_property_set,
  .RequireProperty            = _vxquery_query__set_vertex_condition_require_property,
  .InitTimestampConditions    = _vxquery_query__init_vertex_condition_timestamps,
  .RequireCreationTime        = _vxquery_query__set_vertex_condition_require_TMC,
  .RequireModificationTime    = _vxquery_query__set_vertex_condition_require_TMM,
  .RequireExpirationTime      = _vxquery_query__set_vertex_condition_require_TMX,

  .RequireRecursiveCondition  = _vxquery_query__set_vertex_condition_require_recursive_condition,
  .RequireArcCondition        = _vxquery_query__set_vertex_condition_require_arc_condition,
  .RequireConditionFilter     = _vxquery_query__set_vertex_condition_require_condition_filter,
  .SetAssertCondition         = _vxquery_query__set_vertex_condition_assert_condition,
  .RequireRecursiveTraversal  = _vxquery_query__set_vertex_condition_require_recursive_traversal,
  .RequireArcTraversal        = _vxquery_query__set_vertex_condition_require_arc_traversal,
  .RequireTraversalFilter     = _vxquery_query__set_vertex_condition_require_traversal_filter,
  .SetAssertTraversal         = _vxquery_query__set_vertex_condition_assert_traversal,

  .CollectNeighbors           = _vxquery_query__set_vertex_condition_neighbor_collector_mode,

  .HasManifestation           = _vxquery_query__has_vertex_condition_manifestation,
  .HasType                    = _vxquery_query__has_vertex_condition_type,
  .HasLocalFilter             = _vxquery_query__has_vertex_condition_local_filter,
  .HasPostFilter              = _vxquery_query__has_vertex_condition_post_filter,
  .HasDegree                  = _vxquery_query__has_vertex_condition_degree,
  .HasConditionalDegree       = _vxquery_query__has_vertex_condition_conditional_degree,
  .HasSimilarity              = _vxquery_query__has_vertex_condition_similarity,
  .OwnSimilarityVector        = _vxquery_query__own_vertex_condition_similarity_vector,
  .HasIdentifier              = _vxquery_query__has_vertex_condition_identifier,
  .HasProperty                = _vxquery_query__has_vertex_condition_property,
  .HasCreationTime            = _vxquery_query__has_vertex_condition_TMC,
  .HasModificationTime        = _vxquery_query__has_vertex_condition_TMM,
  .HasExpirationTime          = _vxquery_query__has_vertex_condition_TMX,
  .HasRecursiveCondition      = _vxquery_query__has_vertex_condition_recursive_condition,
  .HasArcCondition            = _vxquery_query__has_vertex_condition_arc_condition,
  .HasConditionFilter         = _vxquery_query__has_vertex_condition_condition_filter,
  .HasAssertCondition         = _vxquery_query__has_vertex_condition_assert_condition,
  .HasRecursiveTraversal      = _vxquery_query__has_vertex_condition_recursive_traversal,
  .HasArcTraversal            = _vxquery_query__has_vertex_condition_arc_traversal,
  .HasTraversalFilter         = _vxquery_query__has_vertex_condition_traversal_filter,
  .HasAssertTraversal         = _vxquery_query__has_vertex_condition_assert_traversal,
  .HasCollector               = _vxquery_query__has_vertex_condition_collector
  
};



/*******************************************************************//**
 * IRankingCondition_t
 ***********************************************************************
 */
static vgx_RankingCondition_t * _vxquery_query__new_ranking_condition( vgx_Graph_t *graph, const char *expression, vgx_sortspec_t sortspec, const vgx_predicator_modifier_enum modifier, vgx_Vector_t *sort_vector, vgx_ArcConditionSet_t **aggregate_condition_set, int64_t aggregate_deephits, CString_t **CSTR__error );
static vgx_RankingCondition_t * _vxquery_query__new_default_ranking_condition( void );
static vgx_RankingCondition_t * _vxquery_query__clone_ranking_condition( const vgx_RankingCondition_t *other );
static void _vxquery_query__delete_ranking_condition( vgx_RankingCondition_t **ranking_condition );

DLL_EXPORT vgx_IRankingCondition_t iRankingCondition = {
  // Lifecycle
  .New        = _vxquery_query__new_ranking_condition,
  .NewDefault = _vxquery_query__new_default_ranking_condition,
  .Clone      = _vxquery_query__clone_ranking_condition,
  .Delete     = _vxquery_query__delete_ranking_condition,
};


/*******************************************************************//**
 * IArcCondition_t
 ***********************************************************************
 */

static vgx_ArcCondition_t * _vxquery_query__set_arc_condition( vgx_Graph_t *graph, vgx_ArcCondition_t *dest, bool positive, const char *relationship, const vgx_predicator_modifier_enum mod_enum, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
static vgx_ArcCondition_t * _vxquery_query__new_arc_condition( vgx_Graph_t *graph, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
static vgx_ArcCondition_t * _vxquery_query__clone_arc_condition( const vgx_ArcCondition_t *other );
static vgx_ArcCondition_t * _vxquery_query__copy_arc_condition( vgx_ArcCondition_t *dest, const vgx_ArcCondition_t *other );
static void _vxquery_query__clear_arc_condition( vgx_ArcCondition_t *arc_condition );
static void _vxquery_query__delete_arc_condition( vgx_ArcCondition_t **arc_condition );
static int _vxquery_query__is_arc_condition_wildcard( vgx_ArcCondition_t *arc_condition );
static vgx_predicator_modifier_enum _vxquery_query__arc_condition_modifier( const vgx_ArcCondition_t *arc_condition );

DLL_EXPORT vgx_IArcCondition_t iArcCondition = {
  // Lifecycle
  .Set      = _vxquery_query__set_arc_condition,
  .Clear    = _vxquery_query__clear_arc_condition,
  .New      = _vxquery_query__new_arc_condition,
  .Clone    = _vxquery_query__clone_arc_condition,
  .Copy     = _vxquery_query__copy_arc_condition,
  .Delete   = _vxquery_query__delete_arc_condition,
  .IsWild   = _vxquery_query__is_arc_condition_wildcard,
  .Modifier = _vxquery_query__arc_condition_modifier
};


/*******************************************************************//**
 * IArcConditionSet_t
 ***********************************************************************
 */
static vgx_ArcConditionSet_t * _vxquery_query__new_empty_arc_condition_set( vgx_Graph_t *graph, bool accept, const vgx_arc_direction direction );
static vgx_ArcConditionSet_t * _vxquery_query__new_simple_arc_condition_set( vgx_Graph_t *graph, const vgx_arc_direction direction, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
static int _vxquery_query__add_arc_condition( vgx_ArcConditionSet_t *arc_condition_set, vgx_ArcCondition_t **arc_condition );
static vgx_ArcConditionSet_t * _vxquery_query__clone_arc_condition_set( const vgx_ArcConditionSet_t *other_set );
static void _vxquery_query__clear_arc_condition_set( vgx_ArcConditionSet_t *arc_condition_set );
static void _vxquery_query__delete_arc_condition_set( vgx_ArcConditionSet_t **arc_condition_set );
static vgx_predicator_modifier_enum _vxquery_query__arc_condition_set_modifier( const vgx_ArcConditionSet_t *arc_condition_set );

DLL_EXPORT vgx_IArcConditionSet_t iArcConditionSet = {
  // Lifecycle
  .NewEmpty   = _vxquery_query__new_empty_arc_condition_set,
  .NewSimple  = _vxquery_query__new_simple_arc_condition_set,
  .Add        = _vxquery_query__add_arc_condition,
  .Clone      = _vxquery_query__clone_arc_condition_set,
  .Clear      = _vxquery_query__clear_arc_condition_set,
  .Delete     = _vxquery_query__delete_arc_condition_set,
  .Modifier   = _vxquery_query__arc_condition_set_modifier
};




/*******************************************************************//**
 * SetDebug
 *
 ***********************************************************************
 */
__inline static int __BaseQuery__SetDebug( vgx_BaseQuery_t *self, vgx_query_debug debug ) {
  self->debug = debug;
  return (int)self->debug;
}
#define __Define__SetDebug( Class )                                 \
static int SetDebug_##Class( Class *self, vgx_query_debug debug ) { \
  return __BaseQuery__SetDebug( (vgx_BaseQuery_t*)self, debug );    \
}
#define __FunctionName__SetDebug( Class ) SetDebug_##Class




/*******************************************************************//**
 * SetTimeout
 *
 ***********************************************************************
 */
__inline static int __BaseQuery__SetTimeout( vgx_BaseQuery_t *self, int timeout_ms, bool limexec ) {
  self->timing_budget = _vgx_get_execution_timing_budget( 0, timeout_ms );
  if( limexec ) {
    _vgx_set_execution_limited( &self->timing_budget );
  }
  return (int)self->timing_budget.t_remain_ms;
}
#define __Define__SetTimeout( Class )                                             \
static int SetTimeout_##Class( Class *self, int timeout_ms, bool limexec ) {      \
  return __BaseQuery__SetTimeout( (vgx_BaseQuery_t*)self, timeout_ms, limexec );  \
}
#define __FunctionName__SetTimeout( Class ) SetTimeout_##Class



/*******************************************************************//**
 * SetMemory
 *
 ***********************************************************************
 */
__inline static vgx_ExpressEvalMemory_t * __BaseQuery__SetMemory( vgx_BaseQuery_t *self, vgx_ExpressEvalMemory_t *memory ) {
  if( self->evaluator_memory ) {
    iEvaluator.DiscardMemory( &self->evaluator_memory );
  }
  if( (self->evaluator_memory = memory) != NULL ) {
    iEvaluator.OwnMemory( memory );
  }
  return memory;
}
#define __Define__SetMemory( Class )                                                              \
static vgx_ExpressEvalMemory_t * SetMemory_##Class( Class *self, vgx_ExpressEvalMemory_t *memory ) {  \
  return __BaseQuery__SetMemory( (vgx_BaseQuery_t*)self, memory );                                \
}
#define __FunctionName__SetMemory( Class ) SetMemory_##Class



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const char * __add_filter( CString_t **CSTR__target, const char *filter_expression ) {
  CString_t *CSTR__filter = iString.New( NULL, filter_expression );
  if( CSTR__filter ) {
    // Remove any previous filter
    iString.Discard( CSTR__target );
    // Steal the expression string
    *CSTR__target = CSTR__filter;
    return filter_expression;
  }
  else {
    return NULL;
  }
}


/*******************************************************************//**
 * AddPreFilter
 *
 ***********************************************************************
 */
#define __Define__AddPreFilter( Class )                                                   \
static const char * AddPreFilter_##Class( Class *self, const char *filter_expression ) {  \
  return __add_filter( &((vgx_BaseQuery_t*)self)->CSTR__pre_filter, filter_expression );  \
}
#define __FunctionName__AddPreFilter( Class ) AddPreFilter_##Class


/*******************************************************************//**
 * AddFilter
 *
 ***********************************************************************
 */
#define __Define__AddFilter( Class )                                                        \
static const char * AddFilter_##Class( Class *self, const char *filter_expression ) {       \
  return __add_filter( &((vgx_BaseQuery_t*)self)->CSTR__vertex_filter, filter_expression );   \
}
#define __FunctionName__AddFilter( Class ) AddFilter_##Class


/*******************************************************************//**
 * AddPostFilter
 *
 ***********************************************************************
 */
#define __Define__AddPostFilter( Class )                                                   \
static const char * AddPostFilter_##Class( Class *self, const char *filter_expression ) {  \
  return __add_filter( &((vgx_BaseQuery_t*)self)->CSTR__post_filter, filter_expression );  \
}
#define __FunctionName__AddPostFilter( Class ) AddPostFilter_##Class



/*******************************************************************//**
 * AddVertexCondition
 *
 ***********************************************************************
 */
__inline static vgx_VertexCondition_t * __BaseQuery__AddVertexCondition( vgx_BaseQuery_t *self, vgx_VertexCondition_t **vertex_condition ) {
  // Remove any previous condition
  __clear_vertex_condition( self->vertex_condition );
  // Steal the vertex condition
  self->vertex_condition = *vertex_condition;
  *vertex_condition = NULL;
  return self->vertex_condition;
}
#define __Define__AddVertexCondition( Class )                                                                         \
static vgx_VertexCondition_t * AddVertexCondition_##Class( Class *self, vgx_VertexCondition_t **vertex_condition ) {  \
  return __BaseQuery__AddVertexCondition( (vgx_BaseQuery_t*)self, vertex_condition );                                 \
}
#define __FunctionName__AddVertexCondition( Class ) AddVertexCondition_##Class



/*******************************************************************//**
 * AddRankingCondition
 *
 ***********************************************************************
 */
__inline static vgx_RankingCondition_t * __BaseQuery__AddRankingCondition( vgx_BaseQuery_t *self, vgx_RankingCondition_t **ranking_condition ) {
  // Remove any previous condition
  iRankingCondition.Delete( &self->ranking_condition );
  // Set the ranking condition
  self->ranking_condition = *ranking_condition;
  *ranking_condition = NULL;
  return self->ranking_condition;
}
#define __Define__AddRankingCondition( Class )                                                                            \
static vgx_RankingCondition_t * AddRankingCondition_##Class( Class *self, vgx_RankingCondition_t **ranking_condition ) {  \
  return __BaseQuery__AddRankingCondition( (vgx_BaseQuery_t*)self, ranking_condition );                                   \
}
#define __FunctionName__AddRankingCondition( Class ) AddRankingCondition_##Class



/*******************************************************************//**
 * SetErrorString
 *
 ***********************************************************************
 */
__inline static const CString_t * __BaseQuery__SetErrorString( vgx_BaseQuery_t *self, CString_t **CSTR__error ) {
  // Steal the error string if one exists
  if( CSTR__error && *CSTR__error ) {
    // Remove any previous error string
    iString.Discard( &self->CSTR__error );
    // Steal the pointer
    self->CSTR__error = *CSTR__error;
    *CSTR__error = NULL;
  }
  // Return the current and possibly updated error string of the query
  return self->CSTR__error;
}
#define __Define__SetErrorString( Class )                                                   \
static const CString_t * SetErrorString_##Class( Class *self, CString_t **CSTR__error ) {   \
  return __BaseQuery__SetErrorString( (vgx_BaseQuery_t*)self, CSTR__error );                \
}
#define __FunctionName__SetErrorString( Class ) SetErrorString_##Class



/*******************************************************************//**
 * YankSearchResult
 *
 ***********************************************************************
 */
__inline static vgx_SearchResult_t * __BaseQuery__YankSearchResult( vgx_BaseQuery_t *self ) {
  vgx_SearchResult_t *SR = self->search_result;
  if( SR ) {
    SR->exe_time = self->exe_time;
    self->search_result = NULL;
  }
  return SR;
}
#define __Define__YankSearchResult( Class )                           \
static vgx_SearchResult_t * YankSearchResult_##Class( Class *self ) { \
  return __BaseQuery__YankSearchResult( (vgx_BaseQuery_t*)self );     \
}
#define __FunctionName__YankSearchResult( Class ) YankSearchResult_##Class



/*******************************************************************//**
 * GetExecutionTime
 *
 ***********************************************************************
 */
__inline static vgx_ExecutionTime_t __BaseQuery__GetExecutionTime( vgx_BaseQuery_t *self ) {
  return self->exe_time;
}
#define __Define__GetExecutionTime( Class )                           \
static vgx_ExecutionTime_t GetExecutionTime_##Class( Class *self ) {  \
  return __BaseQuery__GetExecutionTime( (vgx_BaseQuery_t*)self );     \
}
#define __FunctionName__GetExecutionTime( Class ) GetExecutionTime_##Class



/*******************************************************************//**
 * SetAnchor
 *
 ***********************************************************************
 */
__inline static int __AdjacencyQuery__SetAnchor( vgx_AdjacencyQuery_t *self, const char *anchor_id, CString_t **CSTR__error ) {
  int ret = 0;
  XTRY {
    // Discard any previous anchor
    iString.Discard( &self->CSTR__anchor_id );

    if( anchor_id ) {
      // Disallow wildcard in anchor query but allow literal * (escaped)
      //
      vgx_value_comparison equals = VGX_VALUE_EQU;
      // Not a valid anchor ID (most likely containing an actual non-prefix wildcard which isn't supported at all, but just use the error from the call)
      if( (self->CSTR__anchor_id = iString.Parse.AllowPrefixWildcard( self->graph, anchor_id, &equals, CSTR__error )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x001 );
      }
      // Not a valid anchor ID, it contains an actual wildcard
      if( equals != VGX_VALUE_EQU ) {
        __format_error_string( CSTR__error, "Wildcard not allowed in anchor: %s", anchor_id );
        THROW_SILENT( CXLIB_ERR_API, 0x002 );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }
  return ret;
}
#define __Define__SetAnchor( Class )                                                            \
static int SetAnchor_##Class( Class *self, const char *anchor_id, CString_t **CSTR__error ) {   \
  return __AdjacencyQuery__SetAnchor( (vgx_AdjacencyQuery_t*)self, anchor_id, CSTR__error  );   \
}
#define __FunctionName__SetAnchor( Class ) SetAnchor_##Class



/*******************************************************************//**
 * AddArcConditionSet
 *
 ***********************************************************************
 */
__inline static vgx_ArcConditionSet_t * __AdjacencyQuery__AddArcConditionSet( vgx_AdjacencyQuery_t *self, vgx_ArcConditionSet_t **arc_condition_set ) {
  // Remove any previous condition set
  iArcConditionSet.Delete( &self->arc_condition_set );
  // Set the arc condition set
  self->arc_condition_set = *arc_condition_set;
  *arc_condition_set = NULL;
  return self->arc_condition_set;
}
#define __Define__AddArcConditionSet( Class )                                                                           \
static vgx_ArcConditionSet_t * AddArcConditionSet_##Class( Class *self, vgx_ArcConditionSet_t **arc_condition_set ) {   \
  return __AdjacencyQuery__AddArcConditionSet( (vgx_AdjacencyQuery_t*)self, arc_condition_set );                        \
}
#define __FunctionName__AddArcConditionSet( Class ) AddArcConditionSet_##Class



/*******************************************************************//**
 * SetResponseFormat
 *
 ***********************************************************************
 */
__inline static int __ResponseQuery__SetResponseFormat( vgx_BaseQuery_t *self, vgx_ResponseAttrFastMask format ) {

  if( format & VGX_RESPONSE_SHOW_WITH_TIMING ) {
    self->exe_time.pt_search = &self->exe_time.t_search;
    self->exe_time.pt_result = &self->exe_time.t_result;
  }

  if( self->type == VGX_QUERY_TYPE_NEIGHBORHOOD ) {
    vgx_NeighborhoodQuery_t *query = (vgx_NeighborhoodQuery_t*)self;
    query->fieldmask = format;
    if( query->selector ) {
      query->fieldmask |= VGX_RESPONSE_ATTR_PROPERTY;
    }
    return 0;
  }
  else if( self->type == VGX_QUERY_TYPE_GLOBAL ) {
    vgx_GlobalQuery_t *query = (vgx_GlobalQuery_t*)self;
    query->fieldmask = format;
    if( query->selector ) {
      query->fieldmask |= VGX_RESPONSE_ATTR_PROPERTY;
    }
    return 0;
  }
  else {
    return -1;
  }
}

#define __Define__SetResponseFormat( Class )                                             \
static int SetResponseFormat_##Class( Class *self, vgx_ResponseAttrFastMask format ) {   \
  return __ResponseQuery__SetResponseFormat( (vgx_BaseQuery_t*)self, format );           \
}
#define __FunctionName__SetResponseFormat( Class ) SetResponseFormat_##Class



/*******************************************************************//**
 * SelectStatement
 *
 ***********************************************************************
 */
__inline static int __ResponseQuery__SelectStatement( vgx_BaseQuery_t *self, vgx_Graph_t *graph, const char *select_statement, CString_t **CSTR__error ) {
  
  vgx_Evaluator_t *selector = NULL;
  if( !CharsEqualsConst( select_statement, "*" ) ) {
    vgx_Vector_t *vector = NULL;
    if( self->ranking_condition ) {
      vector = self->ranking_condition->vector;
    }

    if( (selector = iGraphResponse.ParseSelectProperties( graph, select_statement, vector, CSTR__error )) == NULL ) {
      return -1;
    }
  }

  if( self->type == VGX_QUERY_TYPE_NEIGHBORHOOD ) {
    vgx_NeighborhoodQuery_t *query = (vgx_NeighborhoodQuery_t*)self;
    query->fieldmask |= VGX_RESPONSE_ATTR_PROPERTY;
    query->selector = selector;
    return 0;
  }
  else if( self->type == VGX_QUERY_TYPE_GLOBAL ) {
    vgx_GlobalQuery_t *query = (vgx_GlobalQuery_t*)self;
    query->fieldmask |= VGX_RESPONSE_ATTR_PROPERTY;
    query->selector = selector;
    return 0;
  }
  else {
    return -1;
  }
}
#define __Define__SelectStatement( Class )                                                                                      \
static int SelectStatement_##Class( Class *self, vgx_Graph_t *graph, const char *select_statement, CString_t **CSTR__error ) {  \
  return __ResponseQuery__SelectStatement( (vgx_BaseQuery_t*)self, graph, select_statement, CSTR__error );                      \
}
#define __FunctionName__SelectStatement( Class ) SelectStatement_##Class








/*******************************************************************//**
 * AggregateDegree
 ***********************************************************************
 */
__inline static int64_t __AggregatorQuery__AggregateDegree( vgx_AggregatorQuery_t *self, vgx_arc_direction arcdir ) {
  switch( arcdir ) {
  case VGX_ARCDIR_IN:
    self->fields->indegree = &self->fields->data.indegree;
    return self->fields->data.indegree;
  case VGX_ARCDIR_OUT:
    self->fields->outdegree = &self->fields->data.outdegree;
    return self->fields->data.outdegree;
  case VGX_ARCDIR_ANY:
    /* FALLTHRU */
  case VGX_ARCDIR_BOTH:
    self->fields->degree = &self->fields->data.degree;
    return self->fields->data.degree;
  default:
    return -1;
  }
}
#define __Define__AggregateDegree( Class )                                              \
static int64_t AggregateDegree_##Class( Class *self, vgx_arc_direction arcdir ) {    \
  return __AggregatorQuery__AggregateDegree( (vgx_AggregatorQuery_t*)self, arcdir );    \
}
#define __FunctionName__AggregateDegree( Class ) AggregateDegree_##Class



/*******************************************************************//**
 * AggregatePredicatorValue
 ***********************************************************************
 */
__inline static vgx_aggregator_predicator_value_t __AggregatorQuery__AggregatePredicatorValue( vgx_AggregatorQuery_t *self ) {
  self->fields->predval = &self->fields->data.predval;
  return self->fields->data.predval;
}
#define __Define__AggregatePredicatorValue( Class )                                         \
static vgx_aggregator_predicator_value_t AggregatePredicatorValue_##Class( Class *self ) {  \
  return __AggregatorQuery__AggregatePredicatorValue( (vgx_AggregatorQuery_t*)self );       \
}
#define __FunctionName__AggregatePredicatorValue( Class ) AggregatePredicatorValue_##Class



/*******************************************************************//**
 * BaseQuery
 ***********************************************************************
 */
#define __Define__BaseQuery_Methods( Class )  \
  __Define__SetDebug( Class )                 \
  __Define__SetTimeout( Class )               \
  __Define__SetMemory( Class )                \
  __Define__AddPreFilter( Class )             \
  __Define__AddFilter( Class )                \
  __Define__AddPostFilter( Class )            \
  __Define__AddVertexCondition( Class )       \
  __Define__AddRankingCondition( Class )      \
  __Define__SetErrorString( Class )           \
  __Define__YankSearchResult( Class )         \
  __Define__GetExecutionTime( Class )

#define __BaseQuery_vtable_entries( Class )                               \
  .SetDebug               = __FunctionName__SetDebug( Class ),            \
  .SetTimeout             = __FunctionName__SetTimeout( Class ),          \
  .SetMemory              = __FunctionName__SetMemory( Class ),           \
  .AddPreFilter           = __FunctionName__AddPreFilter( Class ),        \
  .AddFilter              = __FunctionName__AddFilter( Class ),           \
  .AddPostFilter          = __FunctionName__AddPostFilter( Class ),       \
  .AddVertexCondition     = __FunctionName__AddVertexCondition( Class ),  \
  .AddRankingCondition    = __FunctionName__AddRankingCondition( Class ), \
  .SetErrorString         = __FunctionName__SetErrorString( Class ),      \
  .YankSearchResult       = __FunctionName__YankSearchResult( Class ),    \
  .GetExecutionTime       = __FunctionName__GetExecutionTime( Class )


/*******************************************************************//**
 * AdjacencyQuery
 ***********************************************************************
 */
#define __Define__AdjacencyQuery_Methods( Class )   \
  __Define__BaseQuery_Methods( Class )              \
  __Define__SetAnchor( Class )                      \
  __Define__AddArcConditionSet( Class )

#define __AdjacencyQuery_vtable_entries( Class )                    \
  __BaseQuery_vtable_entries( Class ),                              \
  .SetAnchor          = __FunctionName__SetAnchor( Class ),         \
  .AddArcConditionSet = __FunctionName__AddArcConditionSet( Class )


/*******************************************************************//**
 * NeighborhoodQuery
 ***********************************************************************
 */
#define __Define__NeighborhoodQuery_Methods( Class )  \
  __Define__AdjacencyQuery_Methods( Class )           \
  __Define__SetResponseFormat( Class )                \
  __Define__SelectStatement( Class )

#define __NeighborhoodQuery_vtable_entries( Class )                 \
  __AdjacencyQuery_vtable_entries( Class ),                         \
  .SetResponseFormat = __FunctionName__SetResponseFormat( Class ),  \
  .SelectStatement = __FunctionName__SelectStatement( Class )


/*******************************************************************//**
 * GlobalQuery
 ***********************************************************************
 */
#define __Define__GlobalQuery_Methods( Class )  \
  __Define__BaseQuery_Methods( Class )          \
  __Define__SetResponseFormat( Class )          \
  __Define__SelectStatement( Class )

#define __GlobalQuery_vtable_entries( Class )                       \
  __BaseQuery_vtable_entries( Class ),                              \
  .SetResponseFormat = __FunctionName__SetResponseFormat( Class ),  \
  .SelectStatement = __FunctionName__SelectStatement( Class )


/*******************************************************************//**
 * AggregatorQuery
 ***********************************************************************
 */
#define __Define__AggregatorQuery_Methods( Class )  \
  __Define__AdjacencyQuery_Methods( Class )         \
  __Define__AggregateDegree( Class )                \
  __Define__AggregatePredicatorValue( Class )

#define __AggregatorQuery_vtable_entries( Class )                                       \
  __AdjacencyQuery_vtable_entries( Class ),                                             \
  .AggregateDegree = __FunctionName__AggregateDegree( Class ),                          \
  .AggregatePredicatorValue = __FunctionName__AggregatePredicatorValue( Class ),        \





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AdjacencyQuery_t * AdjacencyQuery_constructor( const void *identifier, vgx_AdjacencyQuery_constructor_args_t *args );
static vgx_NeighborhoodQuery_t * NeighborhoodQuery_constructor( const void *identifier, vgx_NeighborhoodQuery_constructor_args_t *args );
static vgx_GlobalQuery_t * GlobalQuery_constructor( const void *identifier, vgx_GlobalQuery_constructor_args_t *args );
static vgx_AggregatorQuery_t * AggregatorQuery_constructor( const void *identifier, vgx_AggregatorQuery_constructor_args_t *args );

static void AdjacencyQuery_destructor( vgx_AdjacencyQuery_t *self );
static void NeighborhoodQuery_destructor( vgx_NeighborhoodQuery_t *self );
static void GlobalQuery_destructor( vgx_GlobalQuery_t *self );
static void AggregatorQuery_destructor( vgx_AggregatorQuery_t *self );

static CStringQueue_t * Query_represent( const vgx_BaseQuery_t *base, CStringQueue_t *output );


__Define__AdjacencyQuery_Methods( vgx_AdjacencyQuery_t )
__Define__NeighborhoodQuery_Methods( vgx_NeighborhoodQuery_t )
__Define__GlobalQuery_Methods( vgx_GlobalQuery_t )
__Define__AggregatorQuery_Methods( vgx_AggregatorQuery_t )

static vgx_AdjacencyQuery_vtable_t AdjacencyQuery_Methods = {
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = NULL,
  .vm_deserialize = NULL,
  .vm_construct   = (f_object_constructor_t)AdjacencyQuery_constructor,
  .vm_destroy     = (f_object_destructor_t)AdjacencyQuery_destructor,
  .vm_represent   = (f_object_representer_t)Query_represent,
  .vm_allocator   = NULL,
  // ------------
  __AdjacencyQuery_vtable_entries( vgx_AdjacencyQuery_t )
};

static vgx_NeighborhoodQuery_vtable_t NeighborhoodQuery_Methods = {
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = NULL,
  .vm_deserialize = NULL,
  .vm_construct   = (f_object_constructor_t)NeighborhoodQuery_constructor,
  .vm_destroy     = (f_object_destructor_t)NeighborhoodQuery_destructor,
  .vm_represent   = (f_object_representer_t)Query_represent,
  .vm_allocator   = NULL,
  // ------------
  __NeighborhoodQuery_vtable_entries( vgx_NeighborhoodQuery_t )
};


static vgx_GlobalQuery_vtable_t GlobalQuery_Methods = {
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = NULL,
  .vm_deserialize = NULL,
  .vm_construct   = (f_object_constructor_t)GlobalQuery_constructor,
  .vm_destroy     = (f_object_destructor_t)GlobalQuery_destructor,
  .vm_represent   = (f_object_representer_t)Query_represent,
  .vm_allocator   = NULL,
  // ------------
  __GlobalQuery_vtable_entries( vgx_GlobalQuery_t )
};


static vgx_AggregatorQuery_vtable_t AggregatorQuery_Methods = {
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = NULL,
  .vm_deserialize = NULL,
  .vm_construct   = (f_object_constructor_t)AggregatorQuery_constructor,
  .vm_destroy     = (f_object_destructor_t)AggregatorQuery_destructor,
  .vm_represent   = (f_object_representer_t)Query_represent,
  .vm_allocator   = NULL,
  // ------------
  __AggregatorQuery_vtable_entries( vgx_AggregatorQuery_t )
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_AdjacencyQuery_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( vgx_AdjacencyQuery_t, CXLIB_OBTYPE_QUERY, &AdjacencyQuery_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}

void vgx_NeighborhoodQuery_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( vgx_NeighborhoodQuery_t, CXLIB_OBTYPE_QUERY, &NeighborhoodQuery_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}

void vgx_GlobalQuery_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( vgx_GlobalQuery_t, CXLIB_OBTYPE_QUERY, &GlobalQuery_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}


void vgx_AggregatorQuery_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( vgx_AggregatorQuery_t, CXLIB_OBTYPE_QUERY, &AggregatorQuery_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
}

void vgx_AdjacencyQuery_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_AdjacencyQuery_t );
}

void vgx_NeighborhoodQuery_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_NeighborhoodQuery_t );
}

void vgx_GlobalQuery_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_GlobalQuery_t );
}

void vgx_AggregatorQuery_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_AggregatorQuery_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void AdjacencyQuery_destructor( vgx_AdjacencyQuery_t *self ) {

  if( self->debug & VGX_QUERY_DEBUG_QUERY_POST ) {
    PRINT( self );
  }

  // Clear and initialize the adjacency query members
  __clear_adjacency_query( self );

  // Free adjacency query
  free( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_AdjacencyQuery_t * AdjacencyQuery_constructor( const void *identifier, vgx_AdjacencyQuery_constructor_args_t *args ) {
  vgx_AdjacencyQuery_t *self = NULL;
  
  XTRY {

    // 1. Allocate a new adjacency query object
    if( (self = (vgx_AdjacencyQuery_t*)calloc( 1, sizeof( vgx_AdjacencyQuery_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x701 );
    }

    // 2. Initialize the query object (hook up vtable and set typeinfo)
    if( COMLIB_OBJECT_INIT( vgx_AdjacencyQuery_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x702 );
    }

    // 3. Set the type
    self->type = VGX_QUERY_TYPE_ADJACENCY;

    // 4. Set parent graph
    self->graph = args->graph;
    
    // 5. Configure the adjacency query
    if( __configure_adjacency_query( self, args->anchor_id, args->CSTR__error ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x703 );
    }

    self->evaluator_memory = NULL;

  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void NeighborhoodQuery_destructor( vgx_NeighborhoodQuery_t *self ) {

  if( self->debug & VGX_QUERY_DEBUG_QUERY_POST ) {
    PRINT( self );
  }

  // Clear and initialize the neighborhood query members
  iGraphQuery.ResetNeighborhoodQuery( self );

  // Discard the anchor ID
  // NOTE: The reset above clears the query to an initial state and deletes
  // everything EXCEPT the anchor ID. We need to delete the anchor ID here
  iString.Discard( (CString_t**)&self->CSTR__anchor_id );

  // Free neighborhood query
  free( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_NeighborhoodQuery_t * NeighborhoodQuery_constructor( const void *identifier, vgx_NeighborhoodQuery_constructor_args_t *args ) {
  vgx_NeighborhoodQuery_t *self = NULL;
  
  XTRY {

    // 1. Allocate a new neighborhood query object
    if( (self = (vgx_NeighborhoodQuery_t*)calloc( 1, sizeof( vgx_NeighborhoodQuery_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x711 );
    }

    // 2. Initialize the query object (hook up vtable and set typeinfo)
    if( COMLIB_OBJECT_INIT( vgx_NeighborhoodQuery_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x712 );
    }

    // 3. Set the type
    self->type = VGX_QUERY_TYPE_NEIGHBORHOOD;
    
    // 4. Set parent graph
    self->graph = args->graph;

    // 5. Configure the adjacency portion of the neighborhood query
    if( __configure_adjacency_query( (vgx_AdjacencyQuery_t*)self, args->anchor_id, args->CSTR__error ) != 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x713 );
    }

    self->evaluator_memory = NULL;

    // 6. Initialize the result portion of the neighborhood query
    self->fieldmask = VGX_RESPONSE_ATTRS_NONE;
    self->selector = NULL;
    self->offset = 0;    // no offset
    self->hits   = -1;   // unlimited
    self->collector = NULL; //

    // 7. Set the collector mode
    self->collector_mode = args->collector_mode;

    // 8. Set (steal) the collect condition
    if( args->collect_arc_condition_set && *args->collect_arc_condition_set ) {
      self->collect_arc_condition_set = *args->collect_arc_condition_set;
      *args->collect_arc_condition_set = NULL;
    }

  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void GlobalQuery_destructor( vgx_GlobalQuery_t *self ) {

  if( self->debug & VGX_QUERY_DEBUG_QUERY_POST ) {
    PRINT( self );
  }

  // Clear and initialize the global query members
  iGraphQuery.ResetGlobalQuery( self );

  // Free global query
  free( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_GlobalQuery_t * GlobalQuery_constructor( const void *identifier, vgx_GlobalQuery_constructor_args_t *args ) {
  vgx_GlobalQuery_t *self = NULL;
  
  XTRY {
    // 1. Allocate a new global query object
    if( (self = (vgx_GlobalQuery_t*)calloc( 1, sizeof( vgx_GlobalQuery_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x721 );
    }

    // 2. Initialize the query object (hook up vtable and set typeinfo)
    if( COMLIB_OBJECT_INIT( vgx_GlobalQuery_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x722 );
    }

    // 3. Initialize base portion of the global query
    __initialize_base_query( (vgx_BaseQuery_t*)self );

    // 4. Set the type
    self->type = VGX_QUERY_TYPE_GLOBAL;

    // 5. Set the graph
    self->graph = args->graph;

    self->collector_mode = args->collector_mode;
    self->evaluator_memory = NULL;

    // 6. Set specific vertex if applicable
    const char *vertex_id = args->vertex_id ? args->vertex_id : "*";
    if( (self->CSTR__vertex_id = NewEphemeralCString( self->graph, vertex_id )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x723 );
    }

    // 7. Initialize the global query result count
    self->n_items = 0;

    // 8. Initialize the result portion of the global query
    self->fieldmask = VGX_RESPONSE_ATTRS_NONE;
    self->selector  = NULL;
    self->offset    = 0;    // no offset
    self->hits      = -1;   // unlimited
    self->collector = NULL; //
  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void AggregatorQuery_destructor( vgx_AggregatorQuery_t *self ) {

  if( self->debug & VGX_QUERY_DEBUG_QUERY_POST ) {
    PRINT( self );
  }

  // 4. Delete the aggregation fields
  __delete_aggregator_fields( &self->fields );

  // 3. Clear and initialize the aggregator query members
  iGraphQuery.ResetAggregatorQuery( self );

  // 2. Discard the anchor ID
  // NOTE: The reset above clears the query to an initial state and deletes
  // everything EXCEPT the anchor ID. We need to delete the anchor ID here
  iString.Discard( (CString_t**)&self->CSTR__anchor_id );

  // 1. Free aggregator query and set to NULL
  free( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_AggregatorQuery_t * AggregatorQuery_constructor( const void *identifier, vgx_AggregatorQuery_constructor_args_t *args ) {
  vgx_AggregatorQuery_t *self = NULL;
  
  XTRY {
    // 1. Allocate a new aggregator query object
    if( (self = (vgx_AggregatorQuery_t*)calloc( 1, sizeof( vgx_AggregatorQuery_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x731 );
    }

    // 2. Initialize the query object (hook up vtable and set typeinfo)
    if( COMLIB_OBJECT_INIT( vgx_AggregatorQuery_t, self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x732 );
    }

    // 3. Set the type
    self->type = VGX_QUERY_TYPE_AGGREGATOR;

    // 4. Set parent graph
    self->graph = args->graph;

    // 5. Configure the adjacency portion of the aggregator query
    if( __configure_adjacency_query( (vgx_AdjacencyQuery_t*)self, args->anchor_id, args->CSTR__error ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x733 );
    }

    self->evaluator_memory = NULL;

    // 6. Set (steal) the collect condition
    if( args->collect_arc_condition_set && *args->collect_arc_condition_set ) {
      self->collect_arc_condition_set = *args->collect_arc_condition_set;
      *args->collect_arc_condition_set = NULL;
    }

    // 7. Initialize aggregation fields to default
    if( (self->fields = __new_aggregator_fields()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x734 );
    }

  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Query_represent( const vgx_BaseQuery_t *base, CStringQueue_t *output ) {
  return _vxquery_dump__query( base, output );
}



/*******************************************************************//**
 *
 * VERTEX CONDITION
 *
 ***********************************************************************
 */


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __initialize_vertex_condition( vgx_VertexCondition_t *vertex_condition ) {
  // Initialize to default
  *vertex_condition = DEFAULT_VERTEX_CONDITION;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __clone_recursive_condition_into( vgx_RecursiveCondition_t *dest, const vgx_RecursiveCondition_t *src ) {
  // evaluator
  if( src->evaluator ) {
    if( (dest->evaluator = CALLABLE( src->evaluator )->Clone( src->evaluator, NULL )) == NULL ) {
      return -1;
    }
  }

  // vertex_condition
  if( src->vertex_condition ) {
    if( (dest->vertex_condition = iVertexCondition.Clone( src->vertex_condition )) == NULL ) {
      return -1;
    }
  }
  
  // arc_condition_set
  if( src->arc_condition_set ) {
    if( (dest->arc_condition_set = iArcConditionSet.Clone( src->arc_condition_set )) == NULL ) {
      return -1;
    }
  }

  // override
  dest->override = src->override;

  // ok
  return 0;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCondition_t * _vxquery_query__clone_vertex_condition( const vgx_VertexCondition_t *other ) {
  vgx_VertexCondition_t * self = NULL;
  XTRY {
    if( (self = iVertexCondition.New( other->positive )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x751 );
    }

    // spec
    self->spec = other->spec;
    // manifestation
    self->manifestation = other->manifestation;
    // CSTR__vertex_type
    if( other->CSTR__vertex_type ) {
      if( (self->CSTR__vertex_type = CStringClone( other->CSTR__vertex_type )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x752 );
      }
    }
    // degree
    self->degree = other->degree;
    // indegree
    self->indegree = other->indegree;
    // outdegree
    self->outdegree = other->outdegree;
    
    // CSTR__idlist
    if( other->CSTR__idlist ) {
      if( (self->CSTR__idlist = iString.List.Clone( other->CSTR__idlist )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x753 );
      }
    }

    // -----------------
    // ADVANCED
    // -----------------

    // local evaluators
    if( other->advanced.local_evaluator.filter ) {
      if( (self->advanced.local_evaluator.filter = CALLABLE( other->advanced.local_evaluator.filter )->Clone( other->advanced.local_evaluator.filter, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x755 );
      }
    }
    if( other->advanced.local_evaluator.post ) {
      if( (self->advanced.local_evaluator.post = CALLABLE( other->advanced.local_evaluator.post )->Clone( other->advanced.local_evaluator.post, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x756 );
      }
    }

    // degree
    if( other->advanced.degree_condition ) {
      if( (self->advanced.degree_condition = calloc( 1, sizeof( vgx_DegreeCondition_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x757 );
      }
      if( (self->advanced.degree_condition->arc_condition_set = iArcConditionSet.Clone( other->advanced.degree_condition->arc_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x758 );
      }
      iVertexProperty.CloneValueConditionInto( &self->advanced.degree_condition->value_condition, &other->advanced.degree_condition->value_condition );
    }

    // timestamp
    if( other->advanced.timestamp_condition ) {
      if( (self->advanced.timestamp_condition = __new_timestamp_condition( other->advanced.timestamp_condition->positive )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x759 );
      }
      iVertexProperty.CloneValueConditionInto( &self->advanced.timestamp_condition->tmc_valcond, &other->advanced.timestamp_condition->tmc_valcond );
      iVertexProperty.CloneValueConditionInto( &self->advanced.timestamp_condition->tmm_valcond, &other->advanced.timestamp_condition->tmm_valcond );
      iVertexProperty.CloneValueConditionInto( &self->advanced.timestamp_condition->tmx_valcond, &other->advanced.timestamp_condition->tmx_valcond );
    }

    // similarity
    if( other->advanced.similarity_condition ) {
      if( (self->advanced.similarity_condition = calloc( 1, sizeof( vgx_SimilarityCondition_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x75A );
      }
      self->advanced.similarity_condition->positive = other->advanced.similarity_condition->positive;
      iVertexProperty.CloneValueConditionInto( &self->advanced.similarity_condition->simval_condition, &other->advanced.similarity_condition->simval_condition );
      iVertexProperty.CloneValueConditionInto( &self->advanced.similarity_condition->hamval_condition, &other->advanced.similarity_condition->hamval_condition );
      self->advanced.similarity_condition->probevector = other->advanced.similarity_condition->probevector;
      CALLABLE( self->advanced.similarity_condition->probevector )->Incref( self->advanced.similarity_condition->probevector );
    }

    // properties
    if( other->advanced.property_condition_set ) {
      if( (self->advanced.property_condition_set = iVertexProperty.CloneSet( other->advanced.property_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x75B );
      }
    }

    const vgx_RecursiveCondition_t *src_conditional = &other->advanced.recursive.conditional;
    const vgx_RecursiveCondition_t *src_traversing = &other->advanced.recursive.traversing;
    vgx_RecursiveCondition_t *dest_conditional = &self->advanced.recursive.conditional;
    vgx_RecursiveCondition_t *dest_traversing = &self->advanced.recursive.traversing;

    // Evaluator
    //
    // CONDITIONAL evaluator
    if( src_conditional->evaluator ) {
      if( (dest_conditional->evaluator = CALLABLE( src_conditional->evaluator )->Clone( src_conditional->evaluator, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x75C );
      }
    }

    // TRAVERSING evaluator
    if( src_traversing->evaluator ) {
      if( src_traversing->evaluator == src_conditional->evaluator ) {
        dest_traversing->evaluator = dest_conditional->evaluator; // SHARED (with additional ownership)
        CALLABLE( dest_traversing->evaluator )->Own( dest_traversing->evaluator );
      }
      else if( (dest_traversing->evaluator = CALLABLE( src_traversing->evaluator )->Clone( src_traversing->evaluator, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x75D );
      }
    }
    
    // Vertex Condition
    //
    // CONDITIONAL vertex_condition
    if( src_conditional->vertex_condition ) {
      if( (dest_conditional->vertex_condition = iVertexCondition.Clone( src_conditional->vertex_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x75E );
      }
    }
     
    // TRAVERSING
    if( src_traversing->vertex_condition ) {
      if( src_traversing->vertex_condition == src_conditional->vertex_condition ) {
        dest_traversing->vertex_condition = dest_conditional->vertex_condition; // SHARED
      }
      else if( (dest_traversing->vertex_condition = iVertexCondition.Clone( src_traversing->vertex_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x75F );
      }
    }
    
    // Arc Condition Set
    //
    // CONDITIONAL arc_condition_set
    if( src_conditional->arc_condition_set ) {
      if( (dest_conditional->arc_condition_set = iArcConditionSet.Clone( src_conditional->arc_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x760 );
      }
    }

    // TRAVERSING arc_condition_set
    if( src_traversing->arc_condition_set ) {
      if( (dest_traversing->arc_condition_set = iArcConditionSet.Clone( src_traversing->arc_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x761 );
      }
    }

    // Match Override
    // CONDITIONAL
    dest_conditional->override = src_conditional->override;
    // TRAVERSING
    dest_traversing->override = src_traversing->override;

    // recursive.collect_condition_set
    if( other->advanced.recursive.collect_condition_set ) {
      if( (self->advanced.recursive.collect_condition_set = iArcConditionSet.Clone( other->advanced.recursive.collect_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x762 );
      }
    }

    // recursive.collector_mode
    self->advanced.recursive.collector_mode = other->advanced.recursive.collector_mode;
    
    // CSTR__error
    if( other->CSTR__error ) {
      if( (self->CSTR__error = CStringClone( other->CSTR__error )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x763 );
      }
    }
  }
  XCATCH( errcode ) {
    if( self ) {
      __clear_vertex_condition( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_similarity_condition( vgx_SimilarityCondition_t **similarity_condition ) {
  if( similarity_condition && *similarity_condition ) {
    vgx_Vector_t *vector;
    if( (vector = (*similarity_condition)->probevector) != NULL ) {
      CALLABLE( vector )->Decref( vector );
    }
    iVertexProperty.ClearValueCondition( &(*similarity_condition)->simval_condition );
    iVertexProperty.ClearValueCondition( &(*similarity_condition)->hamval_condition );
    free( *similarity_condition );
    *similarity_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_vertex_condition( vgx_VertexCondition_t *vertex_condition ) {
  if( vertex_condition ) {

    // Discard the reference to any error string we may own
    iString.Discard( &vertex_condition->CSTR__error );

    vgx_RecursiveCondition_t *conditional = &vertex_condition->advanced.recursive.conditional;
    vgx_RecursiveCondition_t *traversing = &vertex_condition->advanced.recursive.traversing;

    int shared_arc_condition = conditional->arc_condition_set == traversing->arc_condition_set;
    int shared_vertex_condition = conditional->vertex_condition == traversing->vertex_condition;

    // Discard the reference to any evaluator we may own
    iEvaluator.DiscardEvaluator( &conditional->evaluator );
    iEvaluator.DiscardEvaluator( &traversing->evaluator );

    // Discard any neighbor arc condition
    iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&conditional->arc_condition_set );
    if( shared_arc_condition ) {
      traversing->arc_condition_set = NULL;
    }
    else {
      iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&traversing->arc_condition_set );
    }

    // Discard any neighbor vertex condition (recursively)
    iVertexCondition.Delete( (vgx_VertexCondition_t**)&conditional->vertex_condition );
    if( shared_vertex_condition ) {
      traversing->vertex_condition = NULL;
    }
    else {
      iVertexCondition.Delete( (vgx_VertexCondition_t**)&traversing->vertex_condition );
    }

    // Discard any neighbor collect condition
    if( vertex_condition->advanced.recursive.collect_condition_set ) {
      iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&vertex_condition->advanced.recursive.collect_condition_set );
    }

    // Discard the references to any local evaluators we may own
    iEvaluator.DiscardEvaluator( &vertex_condition->advanced.local_evaluator.filter );
    iEvaluator.DiscardEvaluator( &vertex_condition->advanced.local_evaluator.post );

    // Discard any property conditions
    iVertexProperty.DeleteSet( &vertex_condition->advanced.property_condition_set );

    // Discard any similarity condition
    __delete_similarity_condition( &vertex_condition->advanced.similarity_condition );

    // Discard any timestamp conditions
    __delete_timestamp_condition( &vertex_condition->advanced.timestamp_condition);

    // Discard any conditional degree
    __delete_degree_condition( &vertex_condition->advanced.degree_condition );

    // Discard the reference to any ID condition we may own
    iString.List.Discard( &vertex_condition->CSTR__idlist );

    // Discard the reference to any vertex type condition string we may own
    iString.Discard( (CString_t**)&vertex_condition->CSTR__vertex_type );

    // Reset to initial state
    __initialize_vertex_condition( vertex_condition );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_vertex_condition( vgx_VertexCondition_t **vertex_condition ) {
  if( vertex_condition && *vertex_condition ) {
    // Reset all members
    __clear_vertex_condition( *vertex_condition );

    // Free and set to NULL
    free( *vertex_condition );
    *vertex_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__reset_vertex_condition( vgx_VertexCondition_t *vertex_condition ) {
  __clear_vertex_condition( vertex_condition );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexCondition_t * _vxquery_query__new_default_vertex_condition( bool positive_match ) {
  vgx_VertexCondition_t *vertex_condition = NULL;
  if( (vertex_condition = (vgx_VertexCondition_t*)calloc( 1, sizeof( vgx_VertexCondition_t ) )) != NULL ) {
    __initialize_vertex_condition( vertex_condition );
    vertex_condition->positive = positive_match;
  }
  return vertex_condition;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_manifestation( vgx_VertexCondition_t *vertex_condition, vgx_VertexStateContext_man_t manifestation ) {
  vertex_condition->manifestation = manifestation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_virtual( vgx_VertexCondition_t *vertex_condition ) {
  vertex_condition->manifestation = VERTEX_STATE_CONTEXT_MAN_VIRTUAL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_type( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const vgx_value_comparison vcomp, const char *type ) {
  // Remove any previous condition
  _vgx_vertex_condition_clear_vertextype( &vertex_condition->spec );
  iString.Discard( (CString_t**)&vertex_condition->CSTR__vertex_type );

  // == or !=
  if( vcomp == VGX_VALUE_EQU || vcomp == VGX_VALUE_NEQ ) {
    // Either default (untyped) vertex or not wildcard "*"
    if( type == NULL || !CharsEqualsConst( type, "*" ) ) {
      _vgx_vertex_condition_add_vertextype( &vertex_condition->spec, vcomp );
      if( type ) {
        if( (vertex_condition->CSTR__vertex_type = NewEphemeralCString( graph, type )) == NULL ) {
          return -1; // memory error
        }
      }
    }
  }
  // Wildcard
  else if( vcomp == VGX_VALUE_ANY ) {
    ; // ok
  }
  // Any other is illegal
  else {
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexTypeEnumeration_t _vxquery_query__get_vertex_condition_type_enumeration( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph ) {
  if( vertex_condition && _vgx_vertex_condition_has_vertextype( vertex_condition->spec ) ) {
    return (vgx_VertexTypeEnumeration_t)iEnumerator_OPEN.VertexType.GetEnum( graph, vertex_condition->CSTR__vertex_type );
  }
  return VERTEX_TYPE_ENUMERATION_WILDCARD;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_degree( vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const vgx_arc_direction direction, const int64_t degree ) {
  if( vcomp & VGX_VALUE_NONE ) {
    return -1; // invalid value comparison
  }
  switch( direction ) {
  case VGX_ARCDIR_IN:
    _vgx_vertex_condition_clear_indegree( &vertex_condition->spec );
    _vgx_vertex_condition_add_indegree( &vertex_condition->spec, vcomp );
    vertex_condition->indegree = degree;
    break;
  case VGX_ARCDIR_OUT:
    _vgx_vertex_condition_clear_outdegree( &vertex_condition->spec );
    _vgx_vertex_condition_add_outdegree( &vertex_condition->spec, vcomp );
    vertex_condition->outdegree = degree;
    break;
  case VGX_ARCDIR_ANY:
    /* FALLTHRU */
  case VGX_ARCDIR_BOTH:
    _vgx_vertex_condition_clear_degree( &vertex_condition->spec );
    _vgx_vertex_condition_add_degree( &vertex_condition->spec, vcomp );
    vertex_condition->degree = degree;
    break;
  default:
    break; // no action
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_vertex_condition_require_local_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, bool post, const char *filter_expression, vgx_Evaluator_t **evaluator ) {
  if( filter_expression ) {
    vgx_Vector_t *vector = vertex_condition->advanced.similarity_condition ? vertex_condition->advanced.similarity_condition->probevector : NULL;
    vgx_Evaluator_t *ev = iEvaluator.NewEvaluator( graph, filter_expression, vector, &vertex_condition->CSTR__error );
    if( ev == NULL ) {
      return -1;
    }
    if( CALLABLE( ev )->Traversals( ev ) > 0 ) {
      iEvaluator.DiscardEvaluator( &ev );
      __set_error_string( &vertex_condition->CSTR__error, "traversal not allowed in vertex condition local filter" );
      return -1;
    }
    if( post && CALLABLE( ev )->HasCull( ev ) ) {
      iEvaluator.DiscardEvaluator( &ev );
      __set_error_string( &vertex_condition->CSTR__error, "mcull() not allowed in vertex condition post filter" );
      return -1;
    }
    iEvaluator.DiscardEvaluator( evaluator );
    *evaluator = ev;
    // Mark as advanced query
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_local_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *local_filter_expression ) {
  return __set_vertex_condition_require_local_filter( vertex_condition, graph, false, local_filter_expression, &vertex_condition->advanced.local_evaluator.filter );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_post_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *post_filter_expression ) {
  return __set_vertex_condition_require_local_filter( vertex_condition, graph, true, post_filter_expression, &vertex_condition->advanced.local_evaluator.post );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_vertex_condition_require_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression, vgx_Evaluator_t **dest ) {
  if( filter_expression ) {
    vgx_Vector_t *vector = vertex_condition->advanced.similarity_condition ? vertex_condition->advanced.similarity_condition->probevector : NULL;
    vgx_Evaluator_t *ev = iEvaluator.NewEvaluator( graph, filter_expression, vector, &vertex_condition->CSTR__error );
    if( ev == NULL ) {
      return -1;
    }
    // Discard any previous evaluator
    iEvaluator.DiscardEvaluator( dest );
    // Set the conditional evaluator
    *dest = ev;
    // Mark as advanced query
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_condition_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression ) {
  return __set_vertex_condition_require_filter( vertex_condition, graph, filter_expression, &vertex_condition->advanced.recursive.conditional.evaluator );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_assert_condition( vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match ) {
  vertex_condition->advanced.recursive.conditional.override.enable = true;
  vertex_condition->advanced.recursive.conditional.override.match = assert_match;
  _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_traversal_filter( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression ) {
  return __set_vertex_condition_require_filter( vertex_condition, graph, filter_expression, &vertex_condition->advanced.recursive.traversing.evaluator );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_assert_traversal( vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match ) {
  vertex_condition->advanced.recursive.traversing.override.enable = true;
  vertex_condition->advanced.recursive.traversing.override.match = assert_match;
  _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_degree_condition( vgx_DegreeCondition_t **degree_condition ) {
  if( degree_condition && *degree_condition ) {
    iArcConditionSet.Delete( &(*degree_condition)->arc_condition_set );
    free( *degree_condition );
    *degree_condition = NULL;
  }
}



/*******************************************************************//**
 *
 * NOTE: Steals the degree condition
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_conditional_degree( vgx_VertexCondition_t *vertex_condition, vgx_DegreeCondition_t **degree_condition ) {
  // Remove any previous condition
  __delete_degree_condition( &vertex_condition->advanced.degree_condition );

  // Set the new condition (STEAL)
  if( degree_condition && *degree_condition ) {
    vertex_condition->advanced.degree_condition = *degree_condition;
    *degree_condition = NULL;
  }
  
  // Mark as advanced query
  _vgx_vertex_condition_add_advanced( &vertex_condition->spec );

  return 0;
}



/*******************************************************************//**
 *
 * NOTE: Steals the similarity condition
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_similarity( vgx_VertexCondition_t *vertex_condition, vgx_SimilarityCondition_t **similarity_condition ) {

  // Discard any previous similarity condition
  __delete_similarity_condition( &vertex_condition->advanced.similarity_condition );

  // Set the new condition (STEAL)
  if( similarity_condition && *similarity_condition ) {
    vertex_condition->advanced.similarity_condition = *similarity_condition;
    *similarity_condition = NULL;
  }

  // Mark as advanced query
  _vgx_vertex_condition_add_advanced( &vertex_condition->spec );

  return 0;
}



/*******************************************************************//**
 *
 * Require a SINGLE identifier condition
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_identifier( vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const char *identifier ) {

  // Clear any previous error
  if( vertex_condition->CSTR__error != NULL ) {
    CStringDelete( vertex_condition->CSTR__error );
    vertex_condition->CSTR__error = NULL;
  }

  // Ensure valid comparison
  if( !_vgx_is_valid_value_comparison( vcomp ) ) {
    vertex_condition->CSTR__error = CStringNewFormat( "Invalid value comparison: %x", (int)vcomp );
    return -1; // invalid value comparison
  }

  // Reset and remove any previous identifier
  _vgx_vertex_condition_clear_id( &vertex_condition->spec );
  iString.List.Discard( &vertex_condition->CSTR__idlist );

  // identifier with == or != condition
  if( identifier != NULL && (vcomp == VGX_VALUE_EQU || vcomp == VGX_VALUE_NEQ) ) {
    // Not == "*"
    if( !CharsEqualsConst( identifier, "*" ) ) {
      // Create a singleton id condition
      if( (vertex_condition->CSTR__idlist = iString.List.New( NULL, 1 )) == NULL ) {
        __set_error_string( &vertex_condition->CSTR__error, "Memory error" );
        return -1;
      }

      // Create the id probe string
      vgx_value_comparison id_vcomp = vcomp;
      CString_t *CSTR__probe = iString.Parse.AllowPrefixWildcard( NULL, identifier, &id_vcomp, &vertex_condition->CSTR__error );
      if( CSTR__probe == NULL ) {
        __set_error_string( &vertex_condition->CSTR__error, "Failed to parse identifier probe" );
        return -1;
      }

      // Populate the internal obid
      CStringObid( CSTR__probe );

      // Add probe to id condition
      iString.List.SetItemSteal( vertex_condition->CSTR__idlist, 0, &CSTR__probe );

      // Set the condition comparison code
      _vgx_vertex_condition_add_id( &vertex_condition->spec, id_vcomp );
    }
  }
  // Full wildcard *
  else if( vcomp == VGX_VALUE_ANY ) {
    ; // ok
  }

  return 0;
}



/*******************************************************************//**
 *
 * Require MULTIPLE identifier condition (OR LOGIC)
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_identifier_list( vgx_VertexCondition_t *vertex_condition, vgx_StringList_t **idlist ) {
  // Clear any previous error
  if( vertex_condition->CSTR__error != NULL ) {
    CStringDelete( vertex_condition->CSTR__error );
    vertex_condition->CSTR__error = NULL;
  }
 
  // Reset and remove any previous identifier
  _vgx_vertex_condition_clear_id( &vertex_condition->spec );
  iString.List.Discard( &vertex_condition->CSTR__idlist );

  if( idlist && *idlist ) {

    vertex_condition->CSTR__idlist = *idlist;
    *idlist = NULL; // stolen

    // Set the condition comparison code
    _vgx_vertex_condition_add_idlist( &vertex_condition->spec );
    
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__init_vertex_condition_property_set( vgx_VertexCondition_t *vertex_condition, bool positive ) {
  // Create new property condition set container if we don't have one
  if( vertex_condition->advanced.property_condition_set == NULL ) {
    if( (vertex_condition->advanced.property_condition_set = iVertexProperty.NewSet( positive )) == NULL ) {
      return -1; // error
    }
    else {
      return 1; // set
    }
  }
  else {
    return 0; // no change
  }
}



/*******************************************************************//**
 *
 * STEALS the property
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_property( vgx_VertexCondition_t *vertex_condition, vgx_VertexProperty_t **vertex_property ) {
  int ret = 0;

  // Clear any previous error
  if( vertex_condition->CSTR__error != NULL ) {
    CStringDelete( vertex_condition->CSTR__error );
    vertex_condition->CSTR__error = NULL;
  }

  XTRY {
    
    // Create new property condition set container if we don't have one
    if( _vxquery_query__init_vertex_condition_property_set( vertex_condition, true ) < 0 ) { // positive matching by default if property condition set was not created earlier
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x771 );
    }

    vgx_PropertyConditionSet_t *condition_set = vertex_condition->advanced.property_condition_set;

    // Add the condition to the set
    if( vertex_property && *vertex_property ) {
      if( condition_set->Append( condition_set->__data, (const vgx_VertexProperty_t**)vertex_property ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x772 );
      }
      *vertex_property = NULL; // owned by the list now
    }

    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );

  }
  XCATCH( errcode ) {
    ret = -1;
    iVertexProperty.DeleteSet( &vertex_condition->advanced.property_condition_set );
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TimestampCondition_t * __new_timestamp_condition( bool positive ) {
  vgx_TimestampCondition_t *condition = NULL;
  if( (condition = calloc( 1, sizeof( vgx_TimestampCondition_t ) )) != NULL ) {
    condition->positive = positive;
    condition->tmc_valcond.vcomp = VGX_VALUE_ANY;
    condition->tmm_valcond.vcomp = VGX_VALUE_ANY;
    condition->tmx_valcond.vcomp = VGX_VALUE_ANY;
  }
  return condition;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_timestamp_condition( vgx_TimestampCondition_t **timestamp_condition ) {
  if( timestamp_condition && *timestamp_condition ) {
    free( *timestamp_condition );
    *timestamp_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__init_vertex_condition_timestamps( vgx_VertexCondition_t *vertex_condition, bool positive ) {
  // Not allowed to re-initialize
  if( vertex_condition->advanced.timestamp_condition != NULL ) {
    return -1;
  }
  // Create new timestamp condition with the given match sign
  if( (vertex_condition->advanced.timestamp_condition = __new_timestamp_condition( positive )) == NULL ) {
    return -1;
  }
  // ok
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_TMC( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmc_condition ) {
  // TODO: DYNAMIC

  if( _vgx_is_dynamic_value_comparison( tmc_condition.vcomp ) ) {
    return -1; // TODO: Allow DYNAMIC?
  }

  // Auto-initialize (positive sign) if needed
  if( vertex_condition->advanced.timestamp_condition == NULL ) {
    _vxquery_query__init_vertex_condition_timestamps( vertex_condition, true );
  }

  // Set the TMC value condition and update the spec
  if( vertex_condition->advanced.timestamp_condition != NULL ) {
    vertex_condition->advanced.timestamp_condition->tmc_valcond = tmc_condition;
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_TMM( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmm_condition ) {
  // TODO: DYNAMIC

  if( _vgx_is_dynamic_value_comparison( tmm_condition.vcomp ) ) {
    return -1; // TODO: Allow DYNAMIC?
  }

  // Auto-initialize (positive sign) if needed
  if( vertex_condition->advanced.timestamp_condition == NULL ) {
    _vxquery_query__init_vertex_condition_timestamps( vertex_condition, true );
  }

  // Set the TMM value condition and update the spec
  if( vertex_condition->advanced.timestamp_condition != NULL ) {
    vertex_condition->advanced.timestamp_condition->tmm_valcond = tmm_condition;
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__set_vertex_condition_require_TMX( vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmx_condition ) {
  // TODO: DYNAMIC

  if( _vgx_is_dynamic_value_comparison( tmx_condition.vcomp ) ) {
    return -1; // TODO: Allow DYNAMIC?
  }

  // Auto-initialize (positive sign) if needed
  if( vertex_condition->advanced.timestamp_condition == NULL ) {
    _vxquery_query__init_vertex_condition_timestamps( vertex_condition, true );
  }

  // Set the TMX value condition and update the spec
  if( vertex_condition->advanced.timestamp_condition != NULL ) {
    vertex_condition->advanced.timestamp_condition->tmx_valcond = tmx_condition;
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * STEALS the vertex condition if not NULL or pointer to NULL pointer
 * 
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_recursive_condition( vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition ) {
  // Reset and delete any previous vertex condition
  if( vertex_condition->advanced.recursive.conditional.vertex_condition ) {
    iVertexCondition.Delete( (vgx_VertexCondition_t**)&vertex_condition->advanced.recursive.conditional.vertex_condition );
  }

  // Set the new vertex condition (NULL ok for no condition)
  vgx_VertexCondition_t *condition = NULL;
  if( neighbor_condition && (condition = *neighbor_condition) != NULL ) {
    *neighbor_condition = NULL; // Stolen
  }
  if( (vertex_condition->advanced.recursive.conditional.vertex_condition = condition) != NULL ) {
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
}



/*******************************************************************//**
 *
 * STEALS the arc condition set
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_arc_condition( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set ) {
  // Reset and delete any previous arc condition set
  if( vertex_condition->advanced.recursive.conditional.arc_condition_set ) {
    iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&vertex_condition->advanced.recursive.conditional.arc_condition_set );
  }

  // Set the arc condition set (NULL ok for no conditions)
  vgx_ArcConditionSet_t *condition_set = NULL;
  if( arc_condition_set && (condition_set = *arc_condition_set) != NULL ) {
    *arc_condition_set = NULL; // Stolen
  }
  if( (vertex_condition->advanced.recursive.conditional.arc_condition_set = condition_set) != NULL ) {
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
}



/*******************************************************************//**
 *
 * STEALS the vertex condition if not NULL or pointer to NULL pointer
 * 
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_recursive_traversal( vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition ) {
  // Reset and delete any previous vertex condition
  if( vertex_condition->advanced.recursive.traversing.vertex_condition ) {
    iVertexCondition.Delete( (vgx_VertexCondition_t**)&vertex_condition->advanced.recursive.traversing.vertex_condition );
  }

  // Set the new vertex condition (NULL ok for no condition)
  vgx_VertexCondition_t *condition = NULL;
  if( neighbor_condition && (condition = *neighbor_condition) != NULL ) {
    *neighbor_condition = NULL; // Stolen
  }
  if( (vertex_condition->advanced.recursive.traversing.vertex_condition = condition) != NULL ) {
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
}



/*******************************************************************//**
 *
 * STEALS the arc condition set
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_require_arc_traversal( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set ) {
  // Reset and delete any previous arc condition set
  if( vertex_condition->advanced.recursive.traversing.arc_condition_set ) {
    iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&vertex_condition->advanced.recursive.traversing.arc_condition_set );
  }

  // Set the arc condition set (NULL ok for no conditions)
  vgx_ArcConditionSet_t *condition_set = NULL;
  if( arc_condition_set && (condition_set = *arc_condition_set) != NULL ) {
    *arc_condition_set = NULL; // Stolen
  }
  if( (vertex_condition->advanced.recursive.traversing.arc_condition_set = condition_set) != NULL ) {
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
}



/*******************************************************************//**
 *
 * STEALS the arc condition set
 ***********************************************************************
 */
static void _vxquery_query__set_vertex_condition_neighbor_collector_mode( vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode ) {

  // Set the collect condition set (NULL ok for no conditions)
  vgx_ArcConditionSet_t *condition_set = NULL;
  if( collect_arc_condition_set && (condition_set = *collect_arc_condition_set) != NULL ) {
    *collect_arc_condition_set = NULL; // Stolen
  }

  // Assign collector mode
  vertex_condition->advanced.recursive.collector_mode = collector_mode;
  
  // Assign arc condition set if specified
  if( condition_set != NULL ) {
    // Remove any previous arc condition set
    iArcConditionSet.Delete( (vgx_ArcConditionSet_t**)&vertex_condition->advanced.recursive.collect_condition_set );
    // Assign new arc condition set
    vertex_condition->advanced.recursive.collect_condition_set = condition_set;
    // Inverted wildcard (i.e. collect nothing)
    if( (condition_set->set == NULL || iArcCondition.IsWild( *condition_set->set ))
        &&
        condition_set->accept == false ) 
    {
      vertex_condition->advanced.recursive.collector_mode = VGX_COLLECTOR_MODE_NONE_CONTINUE; // turn off collection
    }

  }
  
  if( _vgx_collector_mode_collect( collector_mode ) ) {
    _vgx_vertex_condition_add_advanced( &vertex_condition->spec );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_manifestation( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->manifestation != VERTEX_STATE_CONTEXT_MAN_ANY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_type( const vgx_VertexCondition_t *vertex_condition ) {
  return _vgx_vertex_condition_has_vertextype( vertex_condition->spec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_local_filter( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.local_evaluator.filter != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_post_filter( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.local_evaluator.post != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_degree( const vgx_VertexCondition_t *vertex_condition ) {
  return _vgx_vertex_condition_has_degree( vertex_condition->spec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_conditional_degree( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.degree_condition != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_similarity( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.similarity_condition != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * _vxquery_query__own_vertex_condition_similarity_vector( vgx_VertexCondition_t *vertex_condition ) {
  vgx_Vector_t *vector = NULL;
  if( vertex_condition->advanced.similarity_condition != NULL ) {
     if( (vector = vertex_condition->advanced.similarity_condition->probevector) != NULL ) {
       CALLABLE( vector )->Incref( vector );
     }
  }
  return vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_identifier( const vgx_VertexCondition_t *vertex_condition ) {
  return _vgx_vertex_condition_has_id( vertex_condition->spec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_property( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.property_condition_set != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_TMC( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.timestamp_condition != NULL && vertex_condition->advanced.timestamp_condition->tmc_valcond.vcomp != VGX_VALUE_ANY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_TMM( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.timestamp_condition != NULL && vertex_condition->advanced.timestamp_condition->tmm_valcond.vcomp != VGX_VALUE_ANY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_TMX( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.timestamp_condition != NULL && vertex_condition->advanced.timestamp_condition->tmx_valcond.vcomp != VGX_VALUE_ANY;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_recursive_condition( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.conditional.vertex_condition != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_arc_condition( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.conditional.arc_condition_set != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_condition_filter( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.conditional.evaluator != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_assert_condition( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.conditional.override.enable == true;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_recursive_traversal( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.traversing.vertex_condition != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_arc_traversal( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.traversing.arc_condition_set != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_traversal_filter( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.traversing.evaluator != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_assert_traversal( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.traversing.override.enable == true;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__has_vertex_condition_collector( const vgx_VertexCondition_t *vertex_condition ) {
  return vertex_condition->advanced.recursive.collector_mode != VGX_COLLECTOR_MODE_NONE_CONTINUE ||
         vertex_condition->advanced.recursive.collect_condition_set != NULL;
}




/*******************************************************************//**
 *
 * RANKING CONDITION
 *
 ***********************************************************************
 */


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_RankingCondition_t * _vxquery_query__clone_ranking_condition( const vgx_RankingCondition_t *other ) {
  vgx_RankingCondition_t *self = (vgx_RankingCondition_t*)calloc( 1, sizeof( vgx_RankingCondition_t ) );
  if( self ) {
    XTRY {
      // Copy sortspec
      self->sortspec = other->sortspec;

      // Copy modifier
      self->modifier = other->modifier;
      
      // Own another reference to vector if any
      if( (self->vector = other->vector) != NULL ) {
        CALLABLE( self->vector )->Incref( self->vector );
      }
      
      // Ranking formula
      if( other->CSTR__expression ) {
        self->CSTR__expression = CStringClone( other->CSTR__expression );
      }

      // Clone the aggregate condition set if any
      if( other->aggregate_condition_set ) {
        if( (self->aggregate_condition_set = iArcConditionSet.Clone( other->aggregate_condition_set )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x782 );
        }
        self->aggregate_deephits = other->aggregate_deephits;
      }
    }
    XCATCH( errcode ) {
      iRankingCondition.Delete( &self );
    }
    XFINALLY {
    }
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_ranking_condition( vgx_RankingCondition_t *ranking_condition ) {
  if( ranking_condition ) {
    // Discard our reference to the ranking vector, if we have one
    if( ranking_condition->vector ) {
      CALLABLE( ranking_condition->vector )->Decref( ranking_condition->vector );
      ranking_condition->vector = NULL;
    }
    
    // Discard ranking formula
    iString.Discard( &ranking_condition->CSTR__expression );

    // Discard any error we may have
    iString.Discard( &ranking_condition->CSTR__error );

    // Delete any aggregate condition we may have
    iArcConditionSet.Delete( &ranking_condition->aggregate_condition_set );

    // Reset to initial state
    *ranking_condition = DEFAULT_RANKING_CONDITION;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_ranking_condition( vgx_RankingCondition_t **ranking_condition ) {
  if( ranking_condition && *ranking_condition ) {
    // Remove all members
    __clear_ranking_condition( *ranking_condition );

    // Free and set to NULL
    free( *ranking_condition );
    *ranking_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_RankingCondition_t * _vxquery_query__new_ranking_condition( vgx_Graph_t *graph, const char *expression, vgx_sortspec_t sortspec, const vgx_predicator_modifier_enum modifier, vgx_Vector_t *sort_vector, vgx_ArcConditionSet_t **aggregate_condition_set, int64_t aggregate_deephits, CString_t **CSTR__error ) {
  vgx_RankingCondition_t *ranking_condition = NULL;

  XTRY {
    // Allocate new ranking condition
    if( (ranking_condition = (vgx_RankingCondition_t*)calloc( 1, sizeof( vgx_RankingCondition_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x791 );
    }

    // Set sortspec with default direction if not specified
    ranking_condition->sortspec = _vgx_set_sort_direction( &sortspec );

    // Ensure sortby code is a valid code
    if( !_vgx_sortspec_valid( ranking_condition->sortspec ) ) {
      __set_error_string( CSTR__error, "invalid sortby parameter" );
      THROW_SILENT( CXLIB_ERR_API, 0x792 );
    }

    // Own any vector supplied (even if it is not used for sorting)
    if( sort_vector ) {
      ranking_condition->vector = sort_vector;
      CALLABLE( sort_vector )->Incref( sort_vector );
    }

    // 
    switch( _vgx_sortby( ranking_condition->sortspec ) ) {
    case VGX_SORTBY_PREDICATOR:
      ranking_condition->modifier.bits = modifier;
      break;
    case VGX_SORTBY_RANKING:
      if( expression == NULL ) {
        __set_error_string( CSTR__error, "rank sorting requires ranking formula" );
        THROW_SILENT( CXLIB_ERR_API, 0x796 );
      }
      if( (ranking_condition->CSTR__expression = NewEphemeralCString( graph, expression )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x797 );
      }
      break;
    default:
      break;
    }

    // Create our own instance of the aggregate condition
    if( aggregate_condition_set && *aggregate_condition_set ) {
      // Require the arc direction of the aggregate condition to be ANY
      if( (*aggregate_condition_set)->arcdir != VGX_ARCDIR_ANY ) {
        __set_error_string( CSTR__error, "arc direction filter not supported for aggregation" );
        THROW_SILENT( CXLIB_ERR_API, 0x798 );
      }
      // Require the aggregate arc condition(s) to have an AGGREGATOR modifier type
      if( (*aggregate_condition_set)->set ) {
        vgx_ArcCondition_t **cursor = (*aggregate_condition_set)->set;
        vgx_ArcCondition_t *cond;
        while( (cond = *cursor++) != NULL ) {
          switch( cond->modifier.bits ) {
          case VGX_PREDICATOR_MOD_INT_AGGREGATOR:
            /* FALLTHRU */
          case VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR:
            break; // ok
          default:
            __set_error_string( CSTR__error, "aggregation requires aggregation modifier filter" );
            THROW_SILENT( CXLIB_ERR_API, 0x799 );
          }
        }
      }
      // OK, now steal the aggregation set
      ranking_condition->aggregate_condition_set = *aggregate_condition_set;
      *aggregate_condition_set = NULL;

      // Set the deep hits for aggregation (used for multi-neighborhood searches)
      ranking_condition->aggregate_deephits = aggregate_deephits;
    }

  }
  XCATCH( errcode ) {
    iRankingCondition.Delete( &ranking_condition );
  }
  XFINALLY {
  }

  return ranking_condition;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_RankingCondition_t * _vxquery_query__new_default_ranking_condition( void ) {
  vgx_RankingCondition_t *ranking_condition = NULL;

  XTRY {
    // Allocate new ranking condition
    if( (ranking_condition = (vgx_RankingCondition_t*)calloc( 1, sizeof( vgx_RankingCondition_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x7A1 );
    }

    // No sorting
    ranking_condition->sortspec = VGX_SORTBY_NONE;

  }
  XCATCH( errcode ) {
    iRankingCondition.Delete( &ranking_condition );
  }
  XFINALLY {
  }

  return ranking_condition;
}



/*******************************************************************//**
 *
 * ARC CONDITION
 *
 ***********************************************************************
 */



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCondition_t * _vxquery_query__clone_arc_condition( const vgx_ArcCondition_t *other ) {
  const char *relationship = other->CSTR__relationship ? CStringValue(other->CSTR__relationship) : NULL;
  return iArcCondition.New( other->graph, other->positive, relationship, other->modifier.bits, other->vcomp, &other->value1.bits, &other->value2.bits );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCondition_t * _vxquery_query__copy_arc_condition( vgx_ArcCondition_t *dest, const vgx_ArcCondition_t *other ) {
  dest->positive = other->positive;
  dest->graph = other->graph;
  // Clean up previous if any
  if( dest->CSTR__relationship ) {
    CStringDelete( dest->CSTR__relationship );
  }
  // Clone other if any
  if( other->CSTR__relationship ) {
    if( (dest->CSTR__relationship = CStringClone( other->CSTR__relationship )) == NULL ) {
      return NULL;
    }
  }
  //else {
  //  dest->CSTR__relationship;
  //}
  dest->modifier = other->modifier;
  dest->vcomp = other->vcomp;
  dest->value1.bits = other->value1.bits;
  dest->value2.bits = other->value2.bits;
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__clear_arc_condition( vgx_ArcCondition_t *arc_condition ) {
  if( arc_condition ) {

    // Discard the reference to any relationship string we may own
    iString.Discard( (CString_t**)&arc_condition->CSTR__relationship );

    // Reset to initial state
    *arc_condition = DEFAULT_ARC_CONDITION;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_arc_condition( vgx_ArcCondition_t **arc_condition ) {
  if( arc_condition && *arc_condition ) {
    // Remove all members
    _vxquery_query__clear_arc_condition( *arc_condition );

    // Free and set to NULL
    free( *arc_condition );
    *arc_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCondition_t * _vxquery_query__set_arc_condition( vgx_Graph_t *graph, vgx_ArcCondition_t *dest, bool positive, const char *relationship, const vgx_predicator_modifier_enum mod_enum, const vgx_value_comparison vcomp, const void *value1, const void *value2 ) {
  vgx_ArcCondition_t *ret_cond = dest; // assume success
  XTRY {
    // Set to positive or negative match (true=positive match, false=negative match)
    dest->positive = positive;

    // Set parent graph
    dest->graph = graph;

    // Create the relationship string if supplied and not a wildcard
    if( relationship && !CharsEqualsConst( relationship, "*" ) ) {
      if( (dest->CSTR__relationship = NewEphemeralCString( graph, relationship )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7B2 );
      }
    }

    // Set the modifier
    dest->modifier = _vgx_predicator_mod_from_enum( mod_enum );

    // Set the arc condition
    dest->vcomp = vcomp;

    // Parse the first value (may be NULL)
    if( iRelation.ParseModifierValue( mod_enum, value1, &dest->value1 ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x7B3 );
    }

    // Parse the second value if range comparison or LSH modifier
    if( _vgx_is_value_range_comparison( vcomp ) || mod_enum == VGX_PREDICATOR_MOD_LSH ) {
      if( iRelation.ParseModifierValue( mod_enum, value2, &dest->value2 ) != 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7B4 );
      }
    }
  }
  XCATCH( errcode ) {
    ret_cond = NULL;
  }
  XFINALLY {
  }
  return ret_cond;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcCondition_t * _vxquery_query__new_arc_condition( vgx_Graph_t *graph, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 ) {

  vgx_ArcCondition_t *arc_condition = NULL;

  XTRY {
    // Allocate arc condition
    if( (arc_condition = (vgx_ArcCondition_t*)calloc( 1, sizeof( vgx_ArcCondition_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x7B7 );
    }

    if( iArcCondition.Set( graph, arc_condition, positive, relationship, modifier, vcomp, value1, value2 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x7B8 );
    }
  }
  XCATCH( errcode ) {
    iArcCondition.Delete( &arc_condition );
  }
  XFINALLY {
  }

  return arc_condition;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__is_arc_condition_wildcard( vgx_ArcCondition_t *arc_condition ) {
  return arc_condition->CSTR__relationship == NULL 
      && arc_condition->modifier.bits == VGX_PREDICATOR_MOD_WILDCARD
      && arc_condition->vcomp == VGX_VALUE_ANY
      && arc_condition->positive;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_predicator_modifier_enum _vxquery_query__arc_condition_modifier( const vgx_ArcCondition_t *arc_condition ) {
  return arc_condition->modifier.bits;
}



/*******************************************************************//**
 *
 * ARC CONDITION SET
 *
 ***********************************************************************
 */


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcConditionSet_t * _vxquery_query__clone_arc_condition_set( const vgx_ArcConditionSet_t *other_set ) {
  vgx_ArcConditionSet_t *clone = NULL;

  XTRY {
    // Allocate a new instance
    if( (clone = (vgx_ArcConditionSet_t*)calloc( 1, sizeof( vgx_ArcConditionSet_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x7D4 );
    }

    // Copy simple attributes
    clone->accept = other_set->accept;
    clone->logic = other_set->logic;
    clone->arcdir = other_set->arcdir;
    clone->graph = other_set->graph;
    
    // Non-empty conditions
    if( other_set->set ) {
      // Simple condition
      if( other_set->set == other_set->simple ) {
        clone->set = clone->simple;
        *clone->simple = &clone->elem;
        if( _vxquery_query__copy_arc_condition( &clone->elem, &other_set->elem ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x7D5 );
        }
      }
      // Multiple conditions
      else {
        // Count conditions in other set
        vgx_ArcCondition_t **src = other_set->set;
        int sz = 0;
        while( *src++ != NULL ) {
          ++sz;
        }
        
        // Allocate condition pointers in new set
        if( (clone->set = (vgx_ArcCondition_t**)calloc( sz+1, sizeof( vgx_ArcCondition_t* ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x7D6 );
        }
        vgx_ArcCondition_t **dst = clone->set;

        // Clone all conditions from other set into new set
        src = other_set->set;
        while( *src != NULL ) {
          if( (*dst++ = _vxquery_query__clone_arc_condition( *src++ )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x7D7 );
          }
        }
      }
    }
  }
  XCATCH( errcode ) {
    iArcConditionSet.Delete( &clone );
  }
  XFINALLY {
  }

  return clone;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__clear_arc_condition_set( vgx_ArcConditionSet_t *arc_condition_set ) {
  if( arc_condition_set ) {
    vgx_ArcConditionSet_t *ACS = arc_condition_set;

    // Discard the reference to any error string we may own
    iString.Discard( &ACS->CSTR__error );

    // Active arc condition(s) must be cleared
    if( ACS->set ) {
      if( ACS->set == ACS->simple ) {
        if( *ACS->simple == &ACS->elem ) {
          _vxquery_query__clear_arc_condition( &ACS->elem );
          *ACS->simple = NULL;
        }
        // Non-standard config!
        else {
          _vxquery_query__delete_arc_condition( ACS->simple );
        }
      }
      // Multi-set
      else {
        vgx_ArcCondition_t **cursor = ACS->set;
        vgx_ArcCondition_t *cond;
        while( (cond = *cursor++) != NULL ) {
          _vxquery_query__delete_arc_condition( &cond );
        }
        free( ACS->set );
      }
      ACS->set = NULL;
    }

    // Reset to initial state
    *arc_condition_set = DEFAULT_ARC_CONDITION_SET;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_arc_condition_set( vgx_ArcConditionSet_t **arc_condition_set ) {
  if( arc_condition_set && *arc_condition_set ) {
    // Remove all members
    _vxquery_query__clear_arc_condition_set( *arc_condition_set );

    // Free and set to NULL
    free( *arc_condition_set );
    *arc_condition_set = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_predicator_modifier_enum _vxquery_query__arc_condition_set_modifier( const vgx_ArcConditionSet_t *arc_condition_set ) {
  if( arc_condition_set && arc_condition_set->set && *arc_condition_set->set ) {
    return _vxquery_query__arc_condition_modifier( *arc_condition_set->set );
  }
  else {
    return VGX_PREDICATOR_MOD_NONE;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcConditionSet_t * _vxquery_query__new_empty_arc_condition_set( vgx_Graph_t *graph, bool accept, const vgx_arc_direction direction ) {
  vgx_ArcConditionSet_t *arc_condition_set = (vgx_ArcConditionSet_t*)calloc( 1, sizeof( vgx_ArcConditionSet_t ) );
  if( arc_condition_set ) {
    *arc_condition_set = DEFAULT_ARC_CONDITION_SET;
    arc_condition_set->accept = accept;
    arc_condition_set->arcdir = direction;
    arc_condition_set->graph = graph;
  }
  return arc_condition_set;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ArcConditionSet_t * _vxquery_query__new_simple_arc_condition_set( vgx_Graph_t *graph, const vgx_arc_direction direction, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 ) {
  vgx_ArcConditionSet_t *arc_condition_set = NULL;

  XTRY {
    // Allocate
    if( (arc_condition_set = (vgx_ArcConditionSet_t*)calloc( 1, sizeof( vgx_ArcConditionSet_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x7D4 );
    }

    // A new simple set is always in accept mode. (The supplied 'positive' argument is for the contained arc condition.)
    arc_condition_set->accept = true;
    arc_condition_set->logic = VGX_LOGICAL_NO_LOGIC;

    // Set the direction
    arc_condition_set->arcdir = direction;

    // Set parent graph
    arc_condition_set->graph = graph;

    // The simple array will be used, so point the set to our own simple array
    // and point the 0th element of the simple array to our own single element.
    arc_condition_set->set = arc_condition_set->simple;
    arc_condition_set->simple[0] = &arc_condition_set->elem;

    // Configure the single arc condition
    if( iArcCondition.Set( graph, &arc_condition_set->elem, positive, relationship, modifier, vcomp, value1, value2 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x7D5 );
    }
  }
  XCATCH( errcode ) {
    iArcConditionSet.Delete( &arc_condition_set );
  }
  XFINALLY {
  }
  return arc_condition_set;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_query__add_arc_condition( vgx_ArcConditionSet_t *arc_condition_set, vgx_ArcCondition_t **arc_condition ) {
  int retcode = 0;
  if( arc_condition == NULL || *arc_condition == NULL ) {
    return -1;
  }

  vgx_ArcConditionSet_t *ACS = arc_condition_set;

  XTRY {
    // Set is empty, add simple arc condition
    if( ACS->set == NULL ) {
      // Hook up internally for the simple condition
      ACS->set = ACS->simple;
      *ACS->simple = &ACS->elem;
      // Copy the condition into our single element
      if( _vxquery_query__copy_arc_condition( &ACS->elem, *arc_condition ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7E1 );
      }
      // Condition is stolen so free it
      iArcCondition.Delete( arc_condition );
    }
    // Set is not empty, append the arc condition to existing arc condition(s)
    else {
      // Current condition is a simple condition, convert to list and append
      if( ACS->set == ACS->simple ) {
        // New list includes 0-term
        if( (ACS->set = (vgx_ArcCondition_t**)calloc( 3, sizeof( vgx_ArcCondition_t* ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x7E2 );
        }
        // Clone the simple element to a heap allocated condition
        if( (ACS->set[0] = _vxquery_query__clone_arc_condition( &ACS->elem )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x7E3 );
        }
        // Clear the old element we just cloned
        _vxquery_query__clear_arc_condition( &ACS->elem );
        // No longer simple
        *ACS->simple = NULL;
        // Append (STEAL) the new condition to the end
        ACS->set[1] = *arc_condition;
      }
      // Just append to existing condition list
      else {
        // Determine current number of entries in list
        vgx_ArcCondition_t **cursor = ACS->set;
        int sz = 0;
        while( *cursor++ != NULL ) {
          ++sz;
        }
        // Expand list capacity by one
        if( (cursor = (vgx_ArcCondition_t**)realloc( ACS->set, sizeof( vgx_ArcCondition_t* ) * (sz+2LL) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x7E2 );
        }
        ACS->set = cursor;
        // Insert (STEAL) new arc condition at the end, and terminate (since realloc doesn't zero the mem like calloc does)
        cursor += sz;
        *cursor++ = *arc_condition;
        *cursor = NULL; // terminate list
      }
    }
  }
  XCATCH( errcode ) {
    retcode = -1;
  }
  XFINALLY {
    // Steal the condition (even if we fail)
    *arc_condition = NULL;
  }

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __reset_aggregator_fields( vgx_aggregator_fields_t *fields ) {
  // Reset the data
  fields->data.predval.bits = 0;
  fields->data.degree       = 0;
  fields->data.indegree     = 0;
  fields->data.outdegree    = 0;
  // Reset vertex
  fields->_this_vertex = NULL;
  // Deactivate aggregation on all fields
  fields->predval   = NULL;
  fields->degree    = NULL;
  fields->indegree  = NULL;
  fields->outdegree = NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_aggregator_fields( vgx_aggregator_fields_t **fields ) {
  if( *fields ) {
    free( *fields );
    *fields = NULL;
  };
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_aggregator_fields_t * __new_aggregator_fields( void ) {
  // Allocate (and zero everything)
  vgx_aggregator_fields_t *fields = (vgx_aggregator_fields_t *)calloc( 1, sizeof( vgx_aggregator_fields_t ) );
  return fields;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __copy_aggregator_fields( vgx_aggregator_fields_t *dest, const vgx_aggregator_fields_t *src ) {
  // For enabled fields in src, enable those fields in dest as well and reset values to 0
  // (Field VALUES in src are NOT copied!)
  dest->predval   = src->predval    ? &dest->data.predval   : NULL;
  dest->degree    = src->degree     ? &dest->data.degree    : NULL;
  dest->indegree  = src->indegree   ? &dest->data.indegree  : NULL;
  dest->outdegree = src->outdegree  ? &dest->data.outdegree : NULL;

  dest->_this_vertex = NULL;
  
  dest->data.predval.bits = 0;
  dest->data.degree       = 0;
  dest->data.indegree     = 0;
  dest->data.outdegree    = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __initialize_base_query( vgx_BaseQuery_t *query ) {
  static const vgx_ExecutionTime_t zero_exe_time = {0};

  // Default timeout: non-blocking, immediate return
  query->timing_budget = _vgx_get_execution_timing_budget( 0, 0 );

  // Reset execution time
  query->exe_time = zero_exe_time;

  // Reset the error string
  query->CSTR__error = NULL;

  // Reset the filter strings
  query->CSTR__pre_filter = NULL;
  query->CSTR__vertex_filter = NULL;
  query->CSTR__post_filter = NULL;

  // Set vertex condition to default
  query->vertex_condition = NULL;

  // Set ranking condition to default
  query->ranking_condition = NULL;

  // Set evaluator memory to default
  query->evaluator_memory = NULL;

  // Reset search result
  query->search_result = NULL;

  // Reset opid
  query->parent_opid = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __copy_base_query( vgx_BaseQuery_t *dest, const vgx_BaseQuery_t *src ) {
  int retcode = 0;
  XTRY {
    // Type
    dest->type = src->type;

    // Timeout (NOTE: we don't reset remaining time!)
    dest->timing_budget = src->timing_budget;

    // Execution time (NOTE: we don't reset counters!)
    dest->exe_time = src->exe_time;

    // Discard dest error string if any
    iString.Discard( &dest->CSTR__error );

    // Clone src error into dest
    if( src->CSTR__error ) {
      // clone error if any
      if( (dest->CSTR__error = CStringClone( src->CSTR__error )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7F1 );
      }
    }

    // Clone the filters
    // Discard dest filter string if any
    iString.Discard( &dest->CSTR__pre_filter );
    iString.Discard( &dest->CSTR__vertex_filter );
    iString.Discard( &dest->CSTR__post_filter );

    // Clone src filters into dest
    if( src->CSTR__pre_filter ) {
      if( (dest->CSTR__pre_filter = CStringClone( src->CSTR__pre_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7F2 );
      }
    }
    if( src->CSTR__vertex_filter ) {
      if( (dest->CSTR__vertex_filter = CStringClone( src->CSTR__vertex_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7F3 );
      }
    }
    if( src->CSTR__post_filter ) {
      if( (dest->CSTR__post_filter = CStringClone( src->CSTR__post_filter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7F4 );
      }
    }

    // Vertex condition
    if( dest->vertex_condition ) {
      // delete previous if any
      __clear_vertex_condition( dest->vertex_condition );
      dest->vertex_condition = NULL;
    }
    if( src->vertex_condition ) {
      // clone vertex condition if any
      if( (dest->vertex_condition = iVertexCondition.Clone( src->vertex_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7F5 );
      }
    }

    // Ranking condition
    if( dest->ranking_condition ) {
      // delete previous if any
      __clear_ranking_condition( dest->ranking_condition );
      dest->ranking_condition = NULL;
    }
    if( src->ranking_condition ) {
      // clone ranking condition if any
      if( (dest->ranking_condition = iRankingCondition.Clone( src->ranking_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7F6 );
      }
    }

    // Evaluator memory
    if( src->evaluator_memory ) {
      if( (dest->evaluator_memory = iEvaluator.CloneMemory( src->evaluator_memory )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7F7 );
      }
    }
    else {
      dest->evaluator_memory = NULL;
    }


  }
  XCATCH( errcode ) {
    __clear_base_query( dest );
    retcode = -1;
  }
  XFINALLY {
  }
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __empty_base_query( vgx_BaseQuery_t *query ) {
  static const vgx_ExecutionTime_t zero_exe_time = {0};

  // Discard the reference to any error string we may own
  iString.Discard( &query->CSTR__error );

  // Reset timing budget
  _vgx_reset_execution_timing_budget( &query->timing_budget );

  // Reset time counters
  query->exe_time = zero_exe_time;

  // Reset opid
  query->parent_opid = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_base_query( vgx_BaseQuery_t *query ) {
  static const vgx_ExecutionTime_t zero_exe_time = {0};

  // Clear the memory
  iEvaluator.DiscardMemory( &query->evaluator_memory );

  // Clear and initialize the ranking condition
  if( query->ranking_condition ) {
    __clear_ranking_condition( query->ranking_condition );
    free( query->ranking_condition );
    query->ranking_condition = NULL;
  }

  // Clear filter expressions
  iString.Discard( &query->CSTR__pre_filter );
  iString.Discard( &query->CSTR__vertex_filter );
  iString.Discard( &query->CSTR__post_filter );

  // Clear and delete the vertex condition
  if( query->vertex_condition ) {
    __clear_vertex_condition( query->vertex_condition );
    free( query->vertex_condition );
    query->vertex_condition = NULL;
  }

  // Discard the reference to any error string we may own
  iString.Discard( &query->CSTR__error );

  // Reset timeout to default
  query->timing_budget = _vgx_get_execution_timing_budget( 0, 0 );

  // Reset time counters
  query->exe_time = zero_exe_time;

  // Reset opid
  query->parent_opid = 0;

  // KEEP THE TYPE query->type
  // KEEP THE GRAPH query->graph
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __configure_adjacency_query( vgx_AdjacencyQuery_t *query, const char *vertex_id, CString_t **CSTR__error ) {
  int retcode = 0;

  //CString_t *CSTR__validated_anchor_id = NULL;
  XTRY {
    // 1. Initialize base portion of adjacency query
    __initialize_base_query( (vgx_BaseQuery_t*)query );

    if( CALLABLE( query )->SetAnchor( query, vertex_id, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x801 );
    }

    // 3. Initialize the access reason to NONE
    query->access_reason = VGX_ACCESS_REASON_NONE;

    // 5. Initialize arc condition
    query->arc_condition_set = NULL;

    // 6. Reset the counts
    query->n_arcs = 0;
    query->n_neighbors = 0;

    // 7. Reset the safe multilock flag
    query->is_safe_multilock = false;
  }
  XCATCH( errcode ) {
    __clear_adjacency_query( query );
    retcode = -1;
  }
  XFINALLY {
  }

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __copy_adjacency_query( vgx_AdjacencyQuery_t *dest, const vgx_AdjacencyQuery_t *src ) {
  int retcode = 0;
  XTRY {
    // 1. Copy base query
    if( __copy_base_query( (vgx_BaseQuery_t*)dest, (const vgx_BaseQuery_t*)src ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x811 );
    }

    // 2. Copy start vertex (first delete previous if any)
    iString.Discard( (CString_t**)&dest->CSTR__anchor_id );
    if( (dest->CSTR__anchor_id = CStringClone( src->CSTR__anchor_id )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x812 );
    }

    // 3. NA

    // 4. Clone arc condition (if any) after discarding previous (if any)
    iArcConditionSet.Delete( &dest->arc_condition_set );
    if( src->arc_condition_set ) {
      if( (dest->arc_condition_set = iArcConditionSet.Clone( src->arc_condition_set )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x813 );
      }
    }
      
    // 5. RESET counts and flags (DO NOT COPY RESULT INFORMATION)
    dest->n_arcs = 0;
    dest->n_neighbors = 0;
    dest->is_safe_multilock = false;

  }
  XCATCH( errcode ) {
    __clear_adjacency_query( dest );
    retcode = -1;
  }
  XFINALLY {
  }
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_adjacency_query( vgx_AdjacencyQuery_t *query ) {
  // 4. Reset the counts and flags
  query->is_safe_multilock = false;
  query->n_neighbors = 0;
  query->n_arcs = 0;

  // 3. Clear then delete the arc condition set
  if( query->arc_condition_set ) {
    _vxquery_query__clear_arc_condition_set( query->arc_condition_set );
    free( query->arc_condition_set );
    query->arc_condition_set = NULL;
  }

  // 2. Discard the anchor id string
  iString.Discard( (CString_t**)&query->CSTR__anchor_id );

  // 1. Clear and initialize the base query
  __clear_base_query( (vgx_BaseQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__empty_adjacency_query( vgx_AdjacencyQuery_t *query ) {
  // Reset counts and flags
  query->is_safe_multilock = false;
  query->n_neighbors = 0;
  query->n_arcs = 0;

  // Reset access reason code
  query->access_reason = VGX_ACCESS_REASON_NONE;

  // Clean up any previous result
  __empty_base_query( (vgx_BaseQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__reset_adjacency_query( vgx_AdjacencyQuery_t *query ) {
  // Reset counts and flags
  query->is_safe_multilock = false;
  query->n_neighbors = 0;
  query->n_arcs = 0;

  // Reset access reason code
  query->access_reason = VGX_ACCESS_REASON_NONE;

  // Clear then delete the arc condition set
  if( query->arc_condition_set ) {
    _vxquery_query__clear_arc_condition_set( query->arc_condition_set );
    free( query->arc_condition_set );
    query->arc_condition_set = NULL;
  }

  // NOTE: We do not discard the anchor ID string. The string set as part of
  // construction remains in the original state.
  // This is because we are RESETTING and need to be able to use the query again. We have no other way of
  // setting the anchor ID than via the constructor, and we can't call the constructor again.

  // 1. Clear and initialize the base query
  __clear_base_query( (vgx_BaseQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_adjacency_query( vgx_AdjacencyQuery_t **query ) {
  if( query && *query ) {
    COMLIB_OBJECT_DESTROY( *query );
    *query = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AdjacencyQuery_t * _vxquery_query__new_adjacency_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error ) {
  vgx_AdjacencyQuery_constructor_args_t args = {
    .graph = graph,
    .anchor_id = vertex_id,
    .CSTR__error = CSTR__error 
  };
  return COMLIB_OBJECT_NEW( vgx_AdjacencyQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AdjacencyQuery_t * _vxquery_query__new_default_adjacency_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error ) {
  vgx_AdjacencyQuery_constructor_args_t args = {
    .graph = graph,
    .anchor_id = vertex_id,
    .CSTR__error = CSTR__error 
  };

  // TODO: Add the default behavior.

  return COMLIB_OBJECT_NEW( vgx_AdjacencyQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static void _vxquery_query__empty_neighborhood_query( vgx_NeighborhoodQuery_t *query ) {
  if( query->collector ) {
    // TODO: Consider emptying the collector's inner structures rather than deleting them.
    iGraphCollector.DeleteCollector( &query->collector );
  }

  iGraphResponse.DeleteSearchResult( &query->search_result );

  _vxquery_query__empty_adjacency_query( (vgx_AdjacencyQuery_t*)query );
}



/*******************************************************************//**
 * Restore state of query object to what it was right after construction
 * 
 ***********************************************************************
 */
static void _vxquery_query__reset_neighborhood_query( vgx_NeighborhoodQuery_t *query ) {
  // NOTE:
  // We do not reset collect_immediate and collector_mode here. Those were set as part
  // of construction and need to remain in the original state.

  if( query->collector ) {
    // TODO: Consider emptying the collector's inner structures rather than deleting them.
    iGraphCollector.DeleteCollector( &query->collector );
  }

  iGraphResponse.DeleteSearchResult( &query->search_result );

  // Reset parameters
  query->hits = -1;
  query->offset = 0;

  // Discard selector
  if( query->selector ) {
    iEvaluator.DiscardEvaluator( &query->selector );
  }

  // Delete the collect condition
  iArcConditionSet.Delete( &query->collect_arc_condition_set );

  // Clear and initialize the adjacency portion of the neighborhood query
  iGraphQuery.ResetAdjacencyQuery( (vgx_AdjacencyQuery_t*)query );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_neighborhood_query( vgx_NeighborhoodQuery_t **query ) {
  if( query && *query ) {
    COMLIB_OBJECT_DESTROY( *query );
    *query = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_NeighborhoodQuery_t * _vxquery_query__new_neighborhood_query( vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode, CString_t **CSTR__error ) {
  vgx_NeighborhoodQuery_constructor_args_t args = {
    .graph                      = graph,
    .anchor_id                  = vertex_id,
    .CSTR__error                = CSTR__error,
    .collect_arc_condition_set  = collect_arc_condition_set,
    .collector_mode             = collector_mode
  };
  return COMLIB_OBJECT_NEW( vgx_NeighborhoodQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_NeighborhoodQuery_t * _vxquery_query__new_default_neighborhood_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error ) {
  vgx_NeighborhoodQuery_constructor_args_t args = {
    .graph                      = graph,
    .anchor_id                  = vertex_id,
    .CSTR__error                = CSTR__error,
    .collect_arc_condition_set  = NULL,
    .collector_mode             = VGX_COLLECTOR_MODE_COLLECT_ARCS
  };

  // TODO: Add the default behavior

  return COMLIB_OBJECT_NEW( vgx_NeighborhoodQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__empty_global_query( vgx_GlobalQuery_t *query ) {
  // Delete collector
  if( query->collector ) {
    // TODO: Consider emptying the collector's inner structures rather than deleting them.
    iGraphCollector.DeleteCollector( &query->collector );
  }

  // Delete search result
  iGraphResponse.DeleteSearchResult( &query->search_result );

  // Reset count
  query->n_items = 0;

  // Empty base
  __empty_base_query( (vgx_BaseQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__reset_global_query( vgx_GlobalQuery_t *query ) {
  // Delete collector
  if( query->collector ) {
    // TODO: Consider emptying the collector's inner structures rather than deleting them.
    iGraphCollector.DeleteCollector( &query->collector );
  }

  // Delete search result
  iGraphResponse.DeleteSearchResult( &query->search_result );

  // Reset count
  query->n_items = 0;

  // Reset parameters
  query->hits = -1;
  query->offset = 0;

  // Discard selector
  if( query->selector ) {
    iEvaluator.DiscardEvaluator( &query->selector );
  }

  // Discard specific vertex id
  iString.Discard( (CString_t**)&query->CSTR__vertex_id );

  // Clear and initialize the base query
  __clear_base_query( (vgx_BaseQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_global_query( vgx_GlobalQuery_t **query ) {
  if( query && *query ) {
    COMLIB_OBJECT_DESTROY( *query );
    *query = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_GlobalQuery_t * _vxquery_query__new_global_query( vgx_Graph_t *graph, vgx_collector_mode_t collector_mode, CString_t **CSTR__error ) {
  vgx_GlobalQuery_constructor_args_t args = {
    .graph = graph,
    .vertex_id = NULL,
    .CSTR__error = CSTR__error,
    .collector_mode = collector_mode
  };
  return COMLIB_OBJECT_NEW( vgx_GlobalQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 * Collect vertices by default
 ***********************************************************************
 */
static vgx_GlobalQuery_t * _vxquery_query__new_default_global_query( vgx_Graph_t *graph, CString_t **CSTR__error ) {
  vgx_GlobalQuery_t *query;
  vgx_GlobalQuery_constructor_args_t args = {
    .graph = graph,
    .vertex_id = NULL,
    .CSTR__error = CSTR__error,
    .collector_mode = VGX_COLLECTOR_MODE_COLLECT_VERTICES
  };

  if( (query = COMLIB_OBJECT_NEW( vgx_GlobalQuery_t, NULL, &args )) != NULL ) {
    query->ranking_condition = iRankingCondition.NewDefault();
  }

  return query;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__empty_aggregator_query( vgx_AggregatorQuery_t *query ) {
  // Reset the aggregation fields
  if( query->fields ) {
    __reset_aggregator_fields( query->fields );
  }

  _vxquery_query__empty_adjacency_query( (vgx_AdjacencyQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__reset_aggregator_query( vgx_AggregatorQuery_t *query ) {
  // Reset the aggregation fields
  if( query->fields ) {
    __reset_aggregator_fields( query->fields );
  }

  // Delete the collect condition
  iArcConditionSet.Delete( &query->collect_arc_condition_set );

  // Clear and initialize the adjacency portion of the aggregator query
  iGraphQuery.ResetAdjacencyQuery( (vgx_AdjacencyQuery_t*)query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_aggregator_query( vgx_AggregatorQuery_t **query ) {
  if( query && *query ) {
    COMLIB_OBJECT_DESTROY( *query );
    *query = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AggregatorQuery_t * _vxquery_query__new_aggregator_query( vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, CString_t **CSTR__error ) {
  vgx_AggregatorQuery_constructor_args_t args = {
    .graph                      = graph,
    .anchor_id                  = vertex_id,
    .CSTR__error                = CSTR__error,
    .collect_arc_condition_set  = collect_arc_condition_set
  };
  return COMLIB_OBJECT_NEW( vgx_AggregatorQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AggregatorQuery_t * _vxquery_query__new_default_aggregator_query( vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error ) {
  vgx_AggregatorQuery_constructor_args_t args = { 
    .graph                      = graph,
    .anchor_id                  = vertex_id,
    .CSTR__error                = CSTR__error,
    .collect_arc_condition_set  = NULL
  };

  // TODO: Add the default behavior

  return COMLIB_OBJECT_NEW( vgx_AggregatorQuery_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AdjacencyQuery_t * _vxquery_query__clone_adjacency_query( const vgx_AdjacencyQuery_t *other, CString_t **CSTR__error ) {
  vgx_AdjacencyQuery_t *self = iGraphQuery.NewAdjacencyQuery( other->graph, CStringValue(other->CSTR__anchor_id), CSTR__error );
  if( self ) {
    if( __copy_adjacency_query( self, other ) < 0 ) {
      iGraphQuery.DeleteAdjacencyQuery( &self );
    }
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_NeighborhoodQuery_t * _vxquery_query__clone_neighborhood_query( const vgx_NeighborhoodQuery_t *other, CString_t **CSTR__error ) {
  vgx_ArcConditionSet_t *collect_condition = NULL;
  if( other->collect_arc_condition_set ) {
    collect_condition = iArcConditionSet.Clone( other->collect_arc_condition_set );
  }

  // Create query clone without anchor. It will be set when we copy adjacency query.
  vgx_NeighborhoodQuery_t *self = iGraphQuery.NewNeighborhoodQuery( other->graph, NULL, &collect_condition, other->collector_mode, CSTR__error );
  if( self ) {

    // 1. copy adjacency part
    if( __copy_adjacency_query( (vgx_AdjacencyQuery_t*)self, (const vgx_AdjacencyQuery_t*)other ) < 0 ) {
      iGraphQuery.DeleteAdjacencyQuery( (vgx_AdjacencyQuery_t**)&self );
      return NULL;
    }

    // 2. result parameters of neighborhood query
    self->fieldmask = other->fieldmask;
    self->offset = other->offset;
    self->hits = other->hits;

    // 3. destroy previous result if any
    // a) Collector
    if( self->collector ) {
      // NOTE: SPECIAL CASE, DO NOT COPY THE RESULT OF A NEIGHBORHOOD QUERY!!
      iGraphCollector.DeleteCollector( &self->collector );
    }
    // b) Search Result
    iGraphResponse.DeleteSearchResult( &self->search_result );

    // 4. collector mode
    self->collector_mode = other->collector_mode;
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_GlobalQuery_t * _vxquery_query__clone_global_query( const vgx_GlobalQuery_t *other, CString_t **CSTR__error ) {
  vgx_GlobalQuery_t *self = iGraphQuery.NewGlobalQuery( other->graph, other->collector_mode, CSTR__error );
  if( self ) {
    // 1. Copy base query
    if( __copy_base_query( (vgx_BaseQuery_t*)self, (const vgx_BaseQuery_t*)other ) < 0 ) {
      iGraphQuery.DeleteGlobalQuery( &self );
      return NULL;
    }

    // 2. RESET result information
    self->n_items = 0;

    // 3. Copy result parameters
    self->fieldmask = other->fieldmask;
    self->offset = other->offset;
    self->hits = other->hits;

    // 4. destroy previous result if any
    // a) Collector
    if( self->collector ) {
      // NOTE: SPECIAL CASE, DO NOT COPY THE RESULT OF A GLOBAL QUERY!!
      iGraphCollector.DeleteCollector( &self->collector );
    }
    // b) Search Result
    iGraphResponse.DeleteSearchResult( &self->search_result );
  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AggregatorQuery_t * _vxquery_query__clone_aggregator_query( const vgx_AggregatorQuery_t *other, CString_t **CSTR__error ) {
  const char *anchor_id = NULL;
  if( other->CSTR__anchor_id ) {
    anchor_id = CStringValue( other->CSTR__anchor_id );
  }

  vgx_ArcConditionSet_t *collect_condition = NULL;
  if( other->collect_arc_condition_set ) {
    collect_condition = iArcConditionSet.Clone( other->collect_arc_condition_set );
  }

  vgx_AggregatorQuery_t *self = iGraphQuery.NewAggregatorQuery( other->graph, anchor_id, &collect_condition, CSTR__error );
  if( self ) {
    
    // TODO: The distinction between neighborhood and global aggregator is not clean.
    //       Make this better.

    // Neighborhood mode
    if( anchor_id ) {
      // 1. copy adjacency part from other
      if( __copy_adjacency_query( (vgx_AdjacencyQuery_t*)self, (const vgx_AdjacencyQuery_t*)other ) < 0 ) {
        iGraphQuery.DeleteAggregatorQuery( &self );
        return NULL;
      }
    }
    // Global mode
    else {
      // 1. copy base part from other
      if( __copy_base_query( (vgx_BaseQuery_t*)self, (const vgx_BaseQuery_t*)other ) < 0 ) {
        iGraphQuery.DeleteAggregatorQuery( &self );
        return NULL;
      }
    }

    // 2. enable the same aggregator fields as in other (but RESET the counts)
    __copy_aggregator_fields( self->fields, other->fields );

  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__empty_query( vgx_BaseQuery_t *query ) {
  switch( query->type ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    _vxquery_query__empty_adjacency_query( (vgx_AdjacencyQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    _vxquery_query__empty_neighborhood_query( (vgx_NeighborhoodQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_AGGREGATOR:
    _vxquery_query__empty_aggregator_query( (vgx_AggregatorQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    _vxquery_query__empty_global_query( (vgx_GlobalQuery_t*)query );
    break;
  default:
    break;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__reset_query( vgx_BaseQuery_t *query ) {
  switch( query->type ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    _vxquery_query__reset_adjacency_query( (vgx_AdjacencyQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    _vxquery_query__reset_neighborhood_query( (vgx_NeighborhoodQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_AGGREGATOR:
    _vxquery_query__reset_aggregator_query( (vgx_AggregatorQuery_t*)query );
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    _vxquery_query__reset_global_query( (vgx_GlobalQuery_t*)query );
    break;
  default:
    break;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_query__delete_query( vgx_BaseQuery_t **query ) {
  if( query && *query ) {
    COMLIB_OBJECT_DESTROY( *query );
    *query = NULL;
  }
}









#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_query.h"

test_descriptor_t _vgx_vxquery_query_tests[] = {
  { "VGX Graph Query Tests", __utest_vxquery_query },
  {NULL}
};
#endif
