/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxquery_query.h
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

#ifndef __UTEST_VXQUERY_QUERY_H
#define __UTEST_VXQUERY_QUERY_H

#include "__vxtest_macro.h"


static int __match_different_dword_pointers( const DWORD *p1, const DWORD *p2 );
static int __match_value( const vgx_value_t *v1, const vgx_value_t *v2 );
static int __match_value_condition( const vgx_value_condition_t *vc1, const vgx_value_condition_t *vc2 );
static int __match_different_cstring_instances( const CString_t *CSTR__A, const CString_t *CSTR__B );
static int __match_cstring_and_chars( const CString_t *CSTR__A, const char *B );

static int __match_vectors( const vgx_Vector_t *v1, const vgx_Vector_t *v2 );
static int __match_degree_condition( const vgx_DegreeCondition_t *dc1, const vgx_DegreeCondition_t *dc2 );
static int __match_similarity_condition( const vgx_SimilarityCondition_t *sc1, const vgx_SimilarityCondition_t *sc2 );
static int __match_timestamp_condition( const vgx_TimestampCondition_t *tc1, const vgx_TimestampCondition_t *tc2 );

static int __match_properties( const vgx_VertexProperty_t *p1, const vgx_VertexProperty_t *p2 );
static int __match_property_conditions( const vgx_PropertyConditionSet_t *api, CQwordList_t *c1, CQwordList_t *c2 );
static int __match_property_condition_set( const vgx_PropertyConditionSet_t *pc1, const vgx_PropertyConditionSet_t *pc2 );

static int __match_arc_condition( const vgx_ArcCondition_t *a1, const vgx_ArcCondition_t *a2 );
static int __match_arc_conditions( vgx_ArcCondition_t **list1, vgx_ArcCondition_t **list2 );
static int __match_arc_condition_set( const vgx_ArcConditionSet_t *ac1, const vgx_ArcConditionSet_t *ac2 );
static int __match_vertex_condition( const vgx_VertexCondition_t *vc1, const vgx_VertexCondition_t *vc2 );




/**************************************************************************//**
 * __match_different_dword_pointers
 *
 ******************************************************************************
 */
static int __match_different_dword_pointers( const DWORD *p1, const DWORD *p2 ) {
  int equal = 
        ( p1 == NULL && p2 == NULL )
    ||  
        (
              ( p1 != NULL && p2 != NULL )
          &&  ( p1 != p2 )
          &&  ( *p1 == *p2 )
        );
  return equal;
}




/**************************************************************************//**
 * __match_value
 *
 ******************************************************************************
 */
static int __match_value( const vgx_value_t *v1, const vgx_value_t *v2 ) {
  int equal = v1->type == v2->type
           && v1->data.bits == v2->data.bits;
  return equal;
}




/**************************************************************************//**
 * __match_value_condition
 *
 ******************************************************************************
 */
static int __match_value_condition( const vgx_value_condition_t *vc1, const vgx_value_condition_t *vc2 ) {
  int equal = __match_value( &vc1->value1, &vc2->value1 )
           && __match_value( &vc1->value2, &vc2->value2 )
           && vc1->vcomp == vc2->vcomp;
  return equal;
}




/**************************************************************************//**
 * __match_different_cstring_instances
 *
 ******************************************************************************
 */
static int __match_different_cstring_instances( const CString_t *CSTR__A, const CString_t *CSTR__B ) {
  // Match if both are NULL, or both are not NULL and not the same instance but equal strings.
  int equal = 
        ( CSTR__A == NULL && CSTR__B == NULL )  // both are NULL
    ||  (
              ( CSTR__A != NULL && CSTR__B != NULL )  // both are not NULL
          &&  ( CSTR__A != CSTR__B )                  // and not the same instance
          &&  CStringEquals( CSTR__A, CSTR__B )       // and equal strings
        );

  return equal;
}




/**************************************************************************//**
 * __match_different_string_list_instances
 *
 ******************************************************************************
 */
static int __match_different_string_list_instances( const vgx_StringList_t *CSTR__listA, const vgx_StringList_t *CSTR__listB ) {
  // Match if both are NULL, or both are not NULL and contain not the same instances but equal strings.
  if( CSTR__listA == NULL && CSTR__listB == NULL ) {
    return 1; // both are NULL
  }
  else if( CSTR__listA != NULL && CSTR__listB != NULL ) {
    if( iString.List.Size( CSTR__listA ) == iString.List.Size( CSTR__listB ) ) {
      for( int64_t i=0; i<iString.List.Size( CSTR__listA ); i++ ) {
        const CString_t *CSTR__A = iString.List.GetItem( (vgx_StringList_t*)CSTR__listA, i );
        const CString_t *CSTR__B = iString.List.GetItem( (vgx_StringList_t*)CSTR__listB, i );
        if( CSTR__A == CSTR__B ) {
          return 0; // must not be same instance
        }
        if( !CStringEquals( CSTR__A, CSTR__B ) ) {
          return 0; // must be equal
        }
      }
      return 1; // equal
    }
  }
  return 0; // not equal
}




/**************************************************************************//**
 * __match_cstring_and_chars
 *
 ******************************************************************************
 */
static int __match_cstring_and_chars( const CString_t *CSTR__A, const char *B ) {
  // Match if both are NULL, or both are not NULL and contain the same string value.
  int equal = 
        ( CSTR__A == NULL && B == NULL )        // both are NULL
    ||  (
              ( CSTR__A != NULL && B != NULL )  // both are not NULL
          &&  CStringEqualsChars( CSTR__A, B )  // and equal strings
        );

  return equal;
}




/**************************************************************************//**
 * __match_vectors
 *
 ******************************************************************************
 */
static int __match_vectors( const vgx_Vector_t *v1, const vgx_Vector_t *v2 ) {
  // Match if both are NULL, or both are not NULL and not the same instance but equal vectors.
  int equal =
        ( v1 == NULL && v2 == NULL )
    ||  (
              ( v1 != NULL && v2 != NULL )
          &&  ( v1 != v2 )
          &&  _vxsim__compare_vectors( v1, v2 ) == 0
        );
  return equal;
}




/**************************************************************************//**
 * __match_degree_condition
 *
 ******************************************************************************
 */
static int __match_degree_condition( const vgx_DegreeCondition_t *dc1, const vgx_DegreeCondition_t *dc2 ) {
  // Match if both are NLLL, of both are not NULL and not the same instance but equal
  int equal = 
        ( dc1 == NULL && dc2 == NULL )
    ||  (
              dc1 != NULL && dc2 != NULL
           && dc1 != dc2
           && __match_arc_condition_set( dc1->arc_condition_set, dc2->arc_condition_set )
           && __match_value_condition( &dc1->value_condition, &dc2->value_condition )
        );
  return equal;
}



/**************************************************************************//**
 * __match_similarity_condition
 *
 ******************************************************************************
 */
static int __match_similarity_condition( const vgx_SimilarityCondition_t *sc1, const vgx_SimilarityCondition_t *sc2 ) {
  // Match if both are NULL, of both are not NULL and not the same instance but equal
  int equal = 
        ( sc1 == NULL && sc2 == NULL )
    ||  (
              sc1 != NULL && sc2 != NULL
           && sc1 != sc2
           && sc1->positive == sc2->positive
           && __match_vectors( sc1->probevector, sc2->probevector )
           && __match_value_condition( &sc1->simval_condition, &sc2->simval_condition )
           && __match_value_condition( &sc1->hamval_condition, &sc2->hamval_condition )
        );
  return equal;
}



/**************************************************************************//**
 * __match_timestamp_condition
 *
 ******************************************************************************
 */
static int __match_timestamp_condition( const vgx_TimestampCondition_t *tc1, const vgx_TimestampCondition_t *tc2 ) {
  // Match if both are NULL, of both are not NULL and not the same instance but equal
  int equal = 
        ( tc1 == NULL && tc2 == NULL )
    ||  (
              tc1 != NULL && tc2 != NULL
           && tc1 != tc2
           && tc1->positive == tc2->positive
           && __match_value_condition( &tc1->tmc_valcond, &tc2->tmc_valcond )
           && __match_value_condition( &tc1->tmm_valcond, &tc2->tmm_valcond )
           && __match_value_condition( &tc1->tmx_valcond, &tc2->tmx_valcond )
      );
  return equal;
}



/**************************************************************************//**
 * __match_properties
 *
 ******************************************************************************
 */
static int __match_properties( const vgx_VertexProperty_t *p1, const vgx_VertexProperty_t *p2 ) {
  if( p1->key == NULL || p2->key == NULL ) {
    // not equal by definition if key is NULL
    return 0;
  }
  if( __match_different_cstring_instances( p1->key, p2->key ) ) {
    if( p1->keyhash == p2->keyhash ) {
      if( __match_value( &p1->val, &p2->val ) ) {
        if( __match_value_condition( &p1->condition, &p2->condition ) ) {
          // equal
          return 1;
        }
      }
    }
  }
  // not equal
  return 0;
}



/**************************************************************************//**
 * __match_property_conditions
 *
 ******************************************************************************
 */
static int __match_property_conditions( const vgx_PropertyConditionSet_t *api, CQwordList_t *c1, CQwordList_t *c2 ) {
  if( c1 == NULL && c2 == NULL ) {
    return 1;
  }
  else if( c1 != NULL && c2 != NULL ) {
    int64_t sz;
    if( (sz = api->Length( c1 )) == api->Length( c2 ) ) {
      for( int64_t n=0; n<sz; n++ ) {
        vgx_VertexProperty_t *p1 = NULL;
        vgx_VertexProperty_t *p2 = NULL;
        api->Get( c1, n, &p1 );
        api->Get( c2, n, &p2 );
        if( !__match_properties( p1, p2 ) ) {
          return 0; // not equal
        }
      }
      // equal
      return 1;
    }
  }
  // Not equal
  return 0;
}



/**************************************************************************//**
 * __match_property_condition_set
 *
 ******************************************************************************
 */
static int __match_property_condition_set( const vgx_PropertyConditionSet_t *pc1, const vgx_PropertyConditionSet_t *pc2 ) {
  // Match if both are NULL, of both are not NULL and not the same instance but equal
  int equal = 
        ( pc1 == NULL && pc2 == NULL )
    ||
        (
              pc1 != NULL && pc2 != NULL
           && pc1 != pc2
           && pc1->Length == pc2->Length
           && pc1->Get == pc2->Get
           && pc1->Append == pc2->Append
           && pc1->positive == pc2->positive
           && __match_property_conditions( pc1, (CQwordList_t*)pc1->__data, (CQwordList_t*)pc2->__data )
        );
  return equal;
}



/**************************************************************************//**
 * __match_arc_condition
 *
 ******************************************************************************
 */
static int __match_arc_condition( const vgx_ArcCondition_t *a1, const vgx_ArcCondition_t *a2 ) {
  int equal = a1->positive == a2->positive
           && __match_different_cstring_instances( a1->CSTR__relationship, a2->CSTR__relationship )
           && a1->modifier.bits == a2->modifier.bits
           && a1->vcomp == a2->vcomp
           && a1->value1.bits == a2->value1.bits
           && a1->value2.bits == a2->value2.bits;
  return equal;
}




/**************************************************************************//**
 * __match_arc_conditions
 *
 ******************************************************************************
 */
static int __match_arc_conditions( vgx_ArcCondition_t **list1, vgx_ArcCondition_t **list2 ) {
  vgx_ArcCondition_t **cursor1 = list1;
  vgx_ArcCondition_t **cursor2 = list2;

  vgx_ArcCondition_t *ac1 = NULL;
  vgx_ArcCondition_t *ac2 = NULL;

  while( (ac1 = *cursor1++) != NULL && (ac2 = *cursor2++) != NULL ) {
    if( !__match_arc_condition( ac1, ac2 ) ) {
      return 0; // not equal
    }
  }

  if( ac1 == NULL && ac2 == NULL ) {
    return 1; // reach end of both lists, i.e. equal
  }
  else {
    return 0; // different list lengths, i.e. not equal
  }
}




/**************************************************************************//**
 * __match_arc_condition_set
 *
 ******************************************************************************
 */
static int __match_arc_condition_set( const vgx_ArcConditionSet_t *ac1, const vgx_ArcConditionSet_t *ac2 ) {
  // Match if both are NULL, of both are not NULL and not the same instance but equal
  int equal = 
        ( ac1 == NULL && ac2 == NULL )
    ||
        (
             ac1 != NULL && ac2 != NULL
          && ac1 != ac2
          && ac1->accept == ac2->accept
          && ac1->logic == ac2->logic
          && ac1->arcdir == ac2->arcdir
          && __match_arc_conditions( ac1->set, ac2->set )
          && __match_different_cstring_instances( ac1->CSTR__error, ac2->CSTR__error )
        );
  return equal;
}




/**************************************************************************//**
 * __match_vertex_condition
 *
 ******************************************************************************
 */
static int __match_vertex_condition( const vgx_VertexCondition_t *vc1, const vgx_VertexCondition_t *vc2 ) {
  // Match if both are NULL, of both are not NULL and not the same instance but equal
  int equal = 
        ( vc1 == NULL && vc2 == NULL )
    ||
        (
              vc1 != NULL && vc2 != NULL
          &&  vc1 != vc2
          &&  vc1->positive == vc2->positive
          &&  vc1->spec == vc2->spec
          &&  vc1->manifestation == vc2->manifestation
          &&  __match_different_cstring_instances( vc1->CSTR__vertex_type, vc2->CSTR__vertex_type )
          &&  vc1->degree == vc2->degree
          &&  vc1->indegree == vc2->indegree
          &&  vc1->outdegree == vc2->outdegree
          &&  __match_different_string_list_instances( vc1->CSTR__idlist, vc2->CSTR__idlist )
          &&  __match_degree_condition( vc1->advanced.degree_condition, vc2->advanced.degree_condition )
          &&  __match_similarity_condition( vc1->advanced.similarity_condition, vc2->advanced.similarity_condition )
          &&  __match_timestamp_condition( vc1->advanced.timestamp_condition, vc2->advanced.timestamp_condition )
          &&  __match_property_condition_set( vc1->advanced.property_condition_set, vc2->advanced.property_condition_set )
          &&  __match_vertex_condition( vc1->advanced.recursive.conditional.vertex_condition, vc2->advanced.recursive.conditional.vertex_condition )
          &&  __match_arc_condition_set( vc1->advanced.recursive.conditional.arc_condition_set, vc2->advanced.recursive.conditional.arc_condition_set )
          &&  vc1->advanced.recursive.conditional.override.enable == vc2->advanced.recursive.conditional.override.enable
          &&  vc1->advanced.recursive.conditional.override.match == vc2->advanced.recursive.conditional.override.match
          &&  __match_vertex_condition( vc1->advanced.recursive.traversing.vertex_condition, vc2->advanced.recursive.traversing.vertex_condition )
          &&  __match_arc_condition_set( vc1->advanced.recursive.traversing.arc_condition_set, vc2->advanced.recursive.traversing.arc_condition_set )
          &&  vc1->advanced.recursive.traversing.override.enable == vc2->advanced.recursive.traversing.override.enable
          &&  vc1->advanced.recursive.traversing.override.match == vc2->advanced.recursive.traversing.override.match
          &&  vc1->advanced.recursive.collector_mode == vc2->advanced.recursive.collector_mode
          &&  __match_different_cstring_instances( vc1->CSTR__error, vc2->CSTR__error ) 
        );
  return equal;
}



BEGIN_UNIT_TEST( __utest_vxquery_query ) {
  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();
  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = NULL;


  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ADJACENCY QUERY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Adjacency Query" ) {
    CString_t *CSTR__error = NULL;
    const char *anchor_id = "this_is_the_anchor_vertex";

    // Create adjacency query
    vgx_AdjacencyQuery_t *query = iGraphQuery.NewAdjacencyQuery( graph, anchor_id, &CSTR__error );
    TEST_ASSERTION( query != NULL, "adjacency query should be created" );
    
    // Verify data
    TEST_ASSERTION( query->type == VGX_QUERY_TYPE_ADJACENCY,  "adjacency query" );
    TEST_ASSERTION( query->timing_budget.timeout_ms == 0,     "default timeout = 0" );
    TEST_ASSERTION( query->CSTR__error == NULL,               "no error" );
    TEST_ASSERTION( query->vertex_condition == NULL,          "no vertex condition" );
    TEST_ASSERTION( query->ranking_condition == NULL,         "no ranking condition" );
    TEST_ASSERTION( query->evaluator_memory == NULL,          "no evaluator memory" );
    TEST_ASSERTION( CStringEqualsChars( query->CSTR__anchor_id, anchor_id ), "should be supplied anchor id" );
    TEST_ASSERTION( query->access_reason == VGX_ACCESS_REASON_NONE, "no access reason" );
    TEST_ASSERTION( query->arc_condition_set == NULL,         "no arc conditions" );
    TEST_ASSERTION( query->n_arcs == 0,                       "arc count 0" );
    TEST_ASSERTION( query->n_neighbors == 0,                  "neighbors count 0" );
    TEST_ASSERTION( query->is_safe_multilock == false,        "safe multilock flag is false" );

    // Destroy
    iGraphQuery.DeleteAdjacencyQuery( &query );
    TEST_ASSERTION( query == NULL,                            "adjacency query should be destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * NEIGHBORHOOD QUERY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Neighborhood Query" ) {
    CString_t *CSTR__error = NULL;
    const char *anchor_id = "this_is_the_anchor_vertex";

    // Create neighborhood query
    vgx_NeighborhoodQuery_t *query = iGraphQuery.NewNeighborhoodQuery( graph, anchor_id, NULL, VGX_COLLECTOR_MODE_COLLECT_ARCS, &CSTR__error );
    TEST_ASSERTION( query != NULL, "neighborhood query should be created" );
    
    // Verify data
    // BASE
    TEST_ASSERTION( query->type == VGX_QUERY_TYPE_NEIGHBORHOOD,   "neighborhood query" );
    TEST_ASSERTION( query->timing_budget.timeout_ms == 0,         "default timeout = 0" );
    TEST_ASSERTION( query->CSTR__error == NULL,                   "no error" );
    TEST_ASSERTION( query->vertex_condition == NULL,              "no vertex condition" );
    TEST_ASSERTION( query->ranking_condition == NULL,             "no ranking condition" );
    TEST_ASSERTION( query->search_result == NULL,                 "no search result" );
    // ADJACENCY
    TEST_ASSERTION( CStringEqualsChars( query->CSTR__anchor_id, anchor_id ), "should be supplied anchor id" );
    TEST_ASSERTION( query->access_reason == VGX_ACCESS_REASON_NONE, "no access reason" );
    TEST_ASSERTION( query->arc_condition_set == NULL,             "no arc conditions" );
    TEST_ASSERTION( query->n_arcs == 0,                           "arc count 0" );
    TEST_ASSERTION( query->n_neighbors == 0,                      "neighbors count 0" );
    TEST_ASSERTION( query->is_safe_multilock == false,            "safe multilock flag is false" );
    // RESULT
    TEST_ASSERTION( query->offset == 0,                           "offset = 0" );
    TEST_ASSERTION( query->hits == -1,                            "hits = -1 (all)" );
    TEST_ASSERTION( query->collector == NULL,                     "collector not set" );
    // NEIGHBORHOOD
    TEST_ASSERTION( query->collector_mode == VGX_COLLECTOR_MODE_COLLECT_ARCS, "collector mode = collect arcs" );

    // Destroy
    iGraphQuery.DeleteNeighborhoodQuery( &query );
    TEST_ASSERTION( query == NULL,                                "neighborhood query should be destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * VERTEX QUERY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Vertex Query" ) {
    CString_t *CSTR__error = NULL;

    // Create vertex query
    vgx_GlobalQuery_t *query = iGraphQuery.NewGlobalQuery( graph, VGX_COLLECTOR_MODE_COLLECT_VERTICES, &CSTR__error );
    TEST_ASSERTION( query != NULL, "vertex query should be created" );
    
    // Verify data
    // BASE
    TEST_ASSERTION( query->type == VGX_QUERY_TYPE_GLOBAL,         "global query" );
    TEST_ASSERTION( query->timing_budget.timeout_ms == 0,         "default timeout = 0" );
    TEST_ASSERTION( query->CSTR__error == NULL,                   "no error" );
    TEST_ASSERTION( query->vertex_condition == NULL,              "no vertex condition" );
    TEST_ASSERTION( query->ranking_condition == NULL,             "no ranking condition" );
    TEST_ASSERTION( query->search_result == NULL,                 "no search result" );
    // GLOBAL
    TEST_ASSERTION( query->n_items == 0,                          "vertex count 0" );
    TEST_ASSERTION( query->collector_mode == VGX_COLLECTOR_MODE_COLLECT_VERTICES, "collect vertices" );
    // RESULT
    TEST_ASSERTION( query->offset == 0,                           "offset = 0" );
    TEST_ASSERTION( query->hits == -1,                            "hits = -1 (all)" );
    TEST_ASSERTION( query->collector == NULL,                     "collector not set" );

    // Destroy
    iGraphQuery.DeleteGlobalQuery( &query );
    TEST_ASSERTION( query == NULL,                                "global query should be destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * AGGREGATOR QUERY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Aggregator Query" ) {
    CString_t *CSTR__error = NULL;
    const char *anchor_id = "this_is_the_anchor_vertex";

    // Create vertex query
    vgx_AggregatorQuery_t *query = iGraphQuery.NewAggregatorQuery( graph, anchor_id, NULL, &CSTR__error );
    TEST_ASSERTION( query != NULL, "aggregator query should be created" );
    
    // Verify data
    // BASE
    TEST_ASSERTION( query->type == VGX_QUERY_TYPE_AGGREGATOR,     "aggregator query" );
    TEST_ASSERTION( query->timing_budget.timeout_ms == 0,         "default timeout = 0" );
    TEST_ASSERTION( query->CSTR__error == NULL,                   "no error" );
    TEST_ASSERTION( query->vertex_condition == NULL,              "no vertex condition" );
    TEST_ASSERTION( query->ranking_condition == NULL,             "no ranking condition" );
    TEST_ASSERTION( query->evaluator_memory == NULL,              "no evaluator memory" );
    // ADJACENCY
    TEST_ASSERTION( CStringEqualsChars( query->CSTR__anchor_id, anchor_id ), "should be supplied anchor id" );
    TEST_ASSERTION( query->access_reason == VGX_ACCESS_REASON_NONE, "no access reason" );
    TEST_ASSERTION( query->arc_condition_set == NULL,             "no arc conditions" );
    TEST_ASSERTION( query->n_arcs == 0,                           "arc count 0" );
    TEST_ASSERTION( query->n_neighbors == 0,                      "neighbors count 0" );
    TEST_ASSERTION( query->is_safe_multilock == false,            "safe multilock flag is false" );
    // AGGREGATOR
    TEST_ASSERTION( query->fields != NULL,                        "aggregation fields should be allocated" );
    vgx_aggregator_fields_t *f = query->fields;
    TEST_ASSERTION( f->data.predval.bits == 0,                    "" );
    TEST_ASSERTION( f->data.degree == 0,                          "" );
    TEST_ASSERTION( f->data.indegree == 0,                        "" );
    TEST_ASSERTION( f->data.outdegree == 0,                       "" );
    TEST_ASSERTION( f->_this_vertex == NULL,                      "" );
    TEST_ASSERTION( f->predval == NULL,                           "" );
    TEST_ASSERTION( f->degree == NULL,                            "" );
    TEST_ASSERTION( f->indegree == NULL,                          "" );
    TEST_ASSERTION( f->outdegree == NULL,                         "" );

    // Destroy
    iGraphQuery.DeleteAggregatorQuery( &query );
    TEST_ASSERTION( query == NULL,                                "aggregator query should be destroyed" );

  } END_TEST_SCENARIO




  /*******************************************************************//**
   * VERTEX CONDITION (default)
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Vertex Condition (default)" ) {
    CString_t *CSTR__error = NULL;
    bool positive = true;
    int twice = 2;
    while( twice-- ) {
      vgx_VertexCondition_t match = DEFAULT_VERTEX_CONDITION;

      // Create vertex condition
      vgx_VertexCondition_t *condition = iVertexCondition.New( positive );
      TEST_ASSERTION( condition != NULL,                            "vertex condition created" );

      // Verify reset
      match.positive = positive;
      TEST_ASSERTION( memcmp( condition, &match, sizeof( vgx_VertexCondition_t ) ) == 0, "default condition" );

      TEST_ASSERTION( condition->spec == VERTEX_PROBE_WILDCARD,     "wildcard" );
      TEST_ASSERTION( condition->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY,   "any manifestation" );
      TEST_ASSERTION( condition->CSTR__vertex_type == NULL,         "any vertex type" );
      TEST_ASSERTION( condition->degree == -1,                      "any degree" );
      TEST_ASSERTION( condition->indegree == -1,                    "any indegree" );
      TEST_ASSERTION( condition->outdegree == -1,                   "any outdegree" );
      TEST_ASSERTION( condition->CSTR__idlist == NULL,              "any id" );
      TEST_ASSERTION( condition->advanced.degree_condition == NULL,         "no advanced degree condition" );
      TEST_ASSERTION( condition->advanced.timestamp_condition == NULL,      "no timestamp condition" );
      TEST_ASSERTION( condition->advanced.similarity_condition == NULL,     "no similarity condition" );
      TEST_ASSERTION( condition->advanced.property_condition_set == NULL,       "no property condition" );
      TEST_ASSERTION( condition->advanced.recursive.conditional.vertex_condition == NULL,     "no conditional recursive neighborhood condition" );
      TEST_ASSERTION( condition->advanced.recursive.conditional.arc_condition_set == NULL,    "no conditional arc condition" );
      TEST_ASSERTION( condition->advanced.recursive.conditional.evaluator == NULL,            "no conditional evaluator" );
      TEST_ASSERTION( condition->advanced.recursive.conditional.override.enable == false,     "no conditional override" );
      TEST_ASSERTION( condition->advanced.recursive.traversing.vertex_condition == NULL,      "no traversing recursive neighborhood condition" );
      TEST_ASSERTION( condition->advanced.recursive.traversing.arc_condition_set == NULL,     "no traversing arc condition" );
      TEST_ASSERTION( condition->advanced.recursive.traversing.evaluator == NULL,             "no traversing evaluator" );
      TEST_ASSERTION( condition->advanced.recursive.traversing.override.enable == false,      "no traversing override" );
      TEST_ASSERTION( condition->advanced.recursive.collector_mode == VGX_COLLECTOR_MODE_NONE_CONTINUE,   "no collection" );
      TEST_ASSERTION( condition->CSTR__error == NULL,               "no error" );

      TEST_ASSERTION( condition->positive == positive,              "positive match" );

      // Destroy vertex condition 
      iVertexCondition.Delete( &condition );
      TEST_ASSERTION( condition == NULL,                            "vertex condition destroyed" );

      // flip to negative
      if( positive ) {
        positive = false;
      }
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * BASIC VERTEX CONDITION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Vertex Condition" ) {
    CString_t *CSTR__error = NULL;
    const char *type = "type1";
    const char *name = "some_name";
    bool positive = true;
    int twice = 2;
    while( twice-- ) {
      vgx_VertexCondition_t match = DEFAULT_VERTEX_CONDITION;

      // Create vertex condition
      vgx_VertexCondition_t *condition = iVertexCondition.New( positive );
      TEST_ASSERTION( condition != NULL,                            "vertex condition created" );

      // Verify reset
      match.positive = positive;
      TEST_ASSERTION( memcmp( condition, &match, sizeof( vgx_VertexCondition_t ) ) == 0, "default condition" );

      // Manifestation
      iVertexCondition.RequireManifestation( condition, VERTEX_STATE_CONTEXT_MAN_REAL );
      match.manifestation = VERTEX_STATE_CONTEXT_MAN_REAL;
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "manifestation" );

      // Vertex Type
      TEST_ASSERTION( iVertexCondition.RequireType( condition, graph, VGX_VALUE_EQU, type ) == 0,   "require vertex type" );
      _vgx_vertex_condition_add_vertextype( &match.spec, VGX_VALUE_EQU );
      match.CSTR__vertex_type = NewEphemeralCString( graph, type );
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "vertex type" );

      // Simple Degree
      TEST_ASSERTION( iVertexCondition.RequireDegree( condition, VGX_VALUE_GT, VGX_ARCDIR_BOTH, 5 ) == 0,  "require degree" );
      _vgx_vertex_condition_add_degree( &match.spec, VGX_VALUE_GT );
      match.degree = 5;
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "degree" );

      // Simple Indegree
      TEST_ASSERTION( iVertexCondition.RequireDegree( condition, VGX_VALUE_LTE, VGX_ARCDIR_IN, 3 ) == 0,  "require indegree" );
      _vgx_vertex_condition_add_indegree( &match.spec, VGX_VALUE_LTE );
      match.indegree = 3;
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "indegree" );

      // Simple Outdegree
      TEST_ASSERTION( iVertexCondition.RequireDegree( condition, VGX_VALUE_GTE, VGX_ARCDIR_OUT, 4 ) == 0,  "require outdegree" );
      _vgx_vertex_condition_add_outdegree( &match.spec, VGX_VALUE_GTE );
      match.outdegree = 4;
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "outdegree" );

      // Identifier
      TEST_ASSERTION( iVertexCondition.RequireIdentifier( condition, VGX_VALUE_EQU, name ) == 0,   "require identifier" );
      match.CSTR__idlist = iString.List.New( NULL, 1 );
      CString_t *CSTR__id = iString.New( NULL, name );
      iString.List.SetItemSteal( match.CSTR__idlist, 0, &CSTR__id );
      _vgx_vertex_condition_add_id( &match.spec, VGX_VALUE_EQU );
      TEST_ASSERTION( __match_vertex_condition( condition, &match ),    "identifier" );

      // Destroy vertex conditions
      iVertexCondition.Reset( &match );
      iVertexCondition.Delete( &condition );
      TEST_ASSERTION( condition == NULL,                            "vertex condition destroyed" );

      // flip to negative
      if( positive ) {
        positive = false;
      }
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * BASIC ARC CONDITION
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Arc Condition" ) {

    vgx_arc_direction arcdirs[] = {
      VGX_ARCDIR_ANY,
      VGX_ARCDIR_IN,
      VGX_ARCDIR_OUT,
      VGX_ARCDIR_BOTH,
      VGX_ARCDIR_BOTH+1
    };
    bool True = true;
    bool False = false;
    bool *signs[] = {
      &True,
      &False,
      NULL
    };
    const char *END_REL = "END";
    const char *relationships[] = {
      NULL,
      "",
      "relationship1",
      "a_different_rel",
      "something_else",
      END_REL
    };
    vgx_predicator_modifier_enum modifiers[] = { 
      VGX_PREDICATOR_MOD_WILDCARD,
      VGX_PREDICATOR_MOD_STATIC,
      VGX_PREDICATOR_MOD_SIMILARITY,
      VGX_PREDICATOR_MOD_DISTANCE,
      VGX_PREDICATOR_MOD_LSH,
      VGX_PREDICATOR_MOD_INTEGER,
      VGX_PREDICATOR_MOD_UNSIGNED,
      VGX_PREDICATOR_MOD_FLOAT,
      VGX_PREDICATOR_MOD_COUNTER,
      VGX_PREDICATOR_MOD_ACCUMULATOR,
      VGX_PREDICATOR_MOD_INT_AGGREGATOR,
      VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR,
      VGX_PREDICATOR_MOD_TIME_CREATED,
      VGX_PREDICATOR_MOD_TIME_MODIFIED,
      VGX_PREDICATOR_MOD_TIME_EXPIRES,
      _VGX_PREDICATOR_MOD_ALL_MASK
    };
    vgx_value_comparison comparisons[] = {
      VGX_VALUE_ANY,
      VGX_VALUE_EQU,
      VGX_VALUE_NEQ,
      VGX_VALUE_GT,
      VGX_VALUE_LT,
      VGX_VALUE_GTE,
      VGX_VALUE_LTE,
      VGX_VALUE_RANGE,
      VGX_VALUE_NRANGE,
      VGX_VALUE_DYN_RANGE,
      VGX_VALUE_DYN_RANGE_R,
      VGX_VALUE_DYN_EQU,
      VGX_VALUE_DYN_NEQ,
      VGX_VALUE_DYN_GT,
      VGX_VALUE_DYN_LT,
      VGX_VALUE_DYN_GTE,
      VGX_VALUE_DYN_LTE,
      VGX_VALUE_NONE
    };

    float Float1 = 0.123f;
    float Float2 = 0.456f;
    int32_t Integer1 = 123456;
    int32_t Integer2 = 456789;
    DWORD Zero = 0;
    DWORD Unity = 1;

    vgx_arc_direction *arcdir = arcdirs;
    bool **sign = signs;
    const char **rel = relationships;
    vgx_predicator_modifier_enum *mod = modifiers;
    vgx_value_comparison *vcomp = comparisons;

    bool done = false;
    int64_t n_iter = 0;
    while( !done ) {
      ++n_iter;
      /*
      printf( "n_iter = %lld\n", n_iter );
      if( n_iter == 1061 ) {
        printf( "wait a sec\n" );
      }
      */

      // Set the value(s) according to current modifier and value comparison
      DWORD *pval1 = &Zero;
      DWORD *pval2 = &Zero;
      switch( *mod ) {
      case VGX_PREDICATOR_MOD_WILDCARD:
        break;
      case VGX_PREDICATOR_MOD_STATIC:
        pval1 = &Unity;
        if( _vgx_is_value_range_comparison( *vcomp ) ) {
          pval2 = &Unity;
        }
        break;
      default:
        if( *mod & _VGX_PREDICATOR_MOD_FLT_MASK ) {
          pval1 = (DWORD*)&Float1;
          if( _vgx_is_value_range_comparison( *vcomp ) ) {
            pval2 = (DWORD*)&Float2;
          }
        }
        else {
          pval1 = (DWORD*)&Integer1;
          if( _vgx_is_value_range_comparison( *vcomp ) ) {
            pval2 = (DWORD*)&Integer2;
          }
        }
      }

      // Create a new arc condition
      vgx_ArcCondition_t *cond = iArcCondition.New( graph, **sign, *rel, *mod, *vcomp, pval1, pval2 );
      TEST_ASSERTION( cond != NULL,             "arc condition created" );

      // Test the arc condition
      TEST_ASSERTION( __match_cstring_and_chars( cond->CSTR__relationship, *rel ),    "relationship match" );
      TEST_ASSERTION( cond->modifier.bits == *mod,                                    "modifier match" );
      TEST_ASSERTION( cond->positive == **sign,                                       "sign match" );
      TEST_ASSERTION( cond->vcomp == *vcomp,                                          "value comparison match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value1.bits, pval1 ),  "value1 match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value2.bits, pval2 ),  "value2 match" );

      // Clone the arc condition
      vgx_ArcCondition_t *cloned_cond = iArcCondition.Clone( cond );
      TEST_ASSERTION( cloned_cond != NULL,      "arc condition clone created" );

      // Test the cloned arc condition
      TEST_ASSERTION( __match_different_cstring_instances( cond->CSTR__relationship, cloned_cond->CSTR__relationship ),   "cloned relationship match" );
      TEST_ASSERTION( cond->modifier.bits == cloned_cond->modifier.bits,              "cloned modifier match" );
      TEST_ASSERTION( cond->positive == cloned_cond->positive,                        "cloned sign match" );
      TEST_ASSERTION( cond->vcomp == cloned_cond->vcomp,                              "cloned value comparison match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value1.bits, &cloned_cond->value1.bits ), "cloned value1 match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value2.bits, &cloned_cond->value2.bits ), "cloned value2 match" );

      // Create a new arc condition set
      vgx_ArcConditionSet_t *cond_set = iArcConditionSet.NewEmpty( graph, true, *arcdir );
      TEST_ASSERTION( cond_set != NULL,                                           "empty arc condition set created" );
      TEST_ASSERTION( cond_set->arcdir == *arcdir,                                "arcdir match" );
      TEST_ASSERTION( cond_set->accept == true,                                   "accept mode is true" );
      TEST_ASSERTION( cond_set->logic == DEFAULT_ARC_CONDITION_SET.logic,         "condition logic is default" );
      TEST_ASSERTION( cond_set->set == NULL,                                      "set has no conditions" );
      TEST_ASSERTION( cond_set->simple[0] == NULL,                                "no simple condition" );
      TEST_ASSERTION( cond_set->simple[1] == NULL,                                "simple is terminated" );
      TEST_ASSERTION( cond_set->CSTR__error == NULL,                              "no error" );

      // Add the arc condition to the arc condition set
      TEST_ASSERTION( iArcConditionSet.Add( cond_set, &cond ) == 0,               "arc condition added to condition set" );
      TEST_ASSERTION( cond == NULL,                                               "arc condition owned by condition set" );

      // Test the arc condition set after arc condition was added
      TEST_ASSERTION( cond_set->arcdir == *arcdir,                                "arcdir unchanged" );
      TEST_ASSERTION( cond_set->accept == DEFAULT_ARC_CONDITION_SET.accept,       "accept mode unchanged" );
      TEST_ASSERTION( cond_set->logic == DEFAULT_ARC_CONDITION_SET.logic,         "condition logic unchanged" );
      TEST_ASSERTION( cond_set->set == cond_set->simple,                          "simple set" );
      TEST_ASSERTION( cond_set->simple[0] == &cond_set->elem,                     "local element should be used" );
      TEST_ASSERTION( cond_set->simple[1] == NULL,                                "simple is terminated" );
      TEST_ASSERTION( cond_set->CSTR__error == NULL,                              "no error" );
      cond = *cond_set->set;
      TEST_ASSERTION( __match_cstring_and_chars( cond->CSTR__relationship, *rel ),    "relationship match" );
      TEST_ASSERTION( cond->modifier.bits == *mod,                                    "modifier match" );
      TEST_ASSERTION( cond->positive == **sign,                                       "sign match" );
      TEST_ASSERTION( cond->vcomp == *vcomp,                                          "value comparison match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value1.bits, pval1 ),  "value1 match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value2.bits, pval2 ),  "value2 match" );

      // Clone the populated arc condition set
      vgx_ArcConditionSet_t *cloned_cond_set = iArcConditionSet.Clone( cond_set );
      TEST_ASSERTION( cloned_cond_set != NULL,  "" );

      // Delete the arc condition set
      iArcConditionSet.Delete( &cond_set );
      TEST_ASSERTION( cond_set == NULL,  "condition set should be deleted" );

      // Test the cloned arc condition set against the cloned arc condition
      TEST_ASSERTION( cloned_cond_set->accept == DEFAULT_ARC_CONDITION_SET.accept,    "cloned accept mode match" );
      TEST_ASSERTION( cloned_cond_set->logic == DEFAULT_ARC_CONDITION_SET.logic,      "cloned condition logic match" );
      TEST_ASSERTION( cloned_cond_set->arcdir == *arcdir,                             "cloned arcdir match" );
      TEST_ASSERTION( cloned_cond_set->set[0] != NULL,                                "cloned first condition should be set" );
      TEST_ASSERTION( cloned_cond_set->set[1] == NULL,                                "cloned only one condition should exist" );
      cond = *cloned_cond_set->set;
      TEST_ASSERTION( __match_cstring_and_chars( cond->CSTR__relationship, *rel ),    "cloned relationship match" );
      TEST_ASSERTION( cond->modifier.bits == *mod,                                    "cloned modifier match" );
      TEST_ASSERTION( cond->positive == **sign,                                       "cloned sign match" );
      TEST_ASSERTION( cond->vcomp == *vcomp,                                          "cloned value comparison match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value1.bits, pval1 ),  "cloned value1 match" );
      TEST_ASSERTION( __match_different_dword_pointers( &cond->value2.bits, pval2 ),  "cloned value2 match" );

      iArcConditionSet.Delete( &cloned_cond_set );
      TEST_ASSERTION( cloned_cond_set == NULL,  "cloned condition set should be deleted" );

      TEST_ASSERTION( cloned_cond != NULL,  "cloned condition should still exist" );
      iArcCondition.Delete( &cloned_cond );
      TEST_ASSERTION( cloned_cond == NULL,  "cloned condition should be deleted" );


      // Try all combinations of arcdir, sign, relationship, modifier and value comparison
      if( *(++arcdir) > VGX_ARCDIR_BOTH ) {
        arcdir = arcdirs;
        if( *(++sign) == NULL ) {
          sign = signs;
          if( *(++rel) == END_REL ) {
            rel = relationships;
            if( *(++mod) == _VGX_PREDICATOR_MOD_ALL_MASK ) {
              mod = modifiers;
              if( *(++vcomp) == VGX_VALUE_NONE ) {
                // We have now tried everything
                done = true;
              }
            }
          }
        }
      }
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * BASIC CONDITIONAL DEGREE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Conditional Degree" ) {

    // Make the arc condition
    int32_t ten = 10;
    vgx_ArcCondition_t *arc_condition = iArcCondition.New( graph, true, "somerel", VGX_PREDICATOR_MOD_INTEGER, VGX_VALUE_GT, &ten, NULL );
    TEST_ASSERTION( arc_condition != NULL,          "create arc condition" );

    // Allocate the degree condition
    vgx_DegreeCondition_t *degree_condition = calloc( 1, sizeof( vgx_DegreeCondition_t ) );
    TEST_ASSERTION( degree_condition != NULL,       "allocate degree condition" );

    // Make the degree condition's arc condition set
    TEST_ASSERTION( (degree_condition->arc_condition_set = iArcConditionSet.NewEmpty( graph, true, VGX_ARCDIR_OUT )) != NULL,    "create arc condition set" );
    
    // Assign the arc condition to the degree condition's arc condition set
    TEST_ASSERTION( iArcConditionSet.Add( degree_condition->arc_condition_set, &arc_condition ) == 0,     "add arc condition to arc condition set" );

    // Delete the degree condition
    iArcConditionSet.Delete( &degree_condition->arc_condition_set );
    TEST_ASSERTION( degree_condition->arc_condition_set == NULL,      "arc condition set deleted" );
    free( degree_condition );

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
