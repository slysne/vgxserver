/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_filter.c
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

#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 * Arc Filter functions 
 * 
 ***********************************************************************
 */
static int __pass_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __stop_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __relationship_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __modifier_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __specific_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __relationship_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __relationship_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __modifier_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __modifier_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __specific_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __specific_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __evaluator_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_pred_loceval_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_loceval_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_pred_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __direct_recursion_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_pred_loceval_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_loceval_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );
static int __generic_pred_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match );



/*******************************************************************//**
 * Vertex Filter functions 
 * 
 ***********************************************************************
 */
static int __pass_vertexfilter( vgx_VertexFilter_context_t *vertexfilter_context, vgx_Vertex_t *vertex, vgx_ArcFilter_match *match );
static int __evaluator_vertexfilter( vgx_VertexFilter_context_t *vertexfilter_context, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );
static int __generic_vertexfilter( vgx_VertexFilter_context_t *filter, vgx_Vertex_t *vertex, vgx_ArcFilter_match *match );



/*******************************************************************//**
 * Predicator Match functions 
 * 
 ***********************************************************************
 */
static int __arcvector_predicator_match_relationship( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_modifier( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_key( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_any( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_value( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_specific( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_relationship_value( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_relationship_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_modifier_value( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_modifier_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_specific_value( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_specific_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target );
static int __arcvector_predicator_match_generic( const vgx_predicator_t probe, const vgx_predicator_t target );



/*******************************************************************//**
 * Vertex Match functions 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_condition_match_type( const vgx_vertex_probe_spec spec, const vgx_vertex_type_t type_probe, const vgx_Vertex_t *vertex_RO );
static int __arcvector_vertex_condition_match_degree( const vgx_vertex_probe_t *vertex_probe, const vgx_Vertex_t *vertex_RO );
static int __arcvector_vertex_condition_match_timestamps( const vgx_timestamp_probe_t *timestamp_probe, const vgx_Vertex_t *vertex_RO );
static int __arcvector_vertex_condition_match_similarity( const vgx_similarity_probe_t *similarity_probe, const vgx_Vector_t *vertex_vector );
static int __arcvector_vertex_condition_match_single_identifier( vgx_vertex_probe_spec spec, const CString_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid );
static int __arcvector_vertex_condition_match_identifier_list( vgx_vertex_probe_spec spec, const vgx_StringList_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid );
static int __arcvector_vertex_condition_match_eval_recursion( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );
static int __arcvector_vertex_condition_match_details( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );

static int __arcvector_vertex_condition_match_recursive( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );
static int __arcvector_vertex_condition_match_generic( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );



/*******************************************************************//**
 * Arc Filter Interface
 * 
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_arc_filter( vgx_Graph_t *self, bool readonly_graph, const vgx_ArcConditionSet_t *arc_condition_set, const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *traversing_evaluator, vgx_ExecutionTimingBudget_t *timing_budget );
static vgx_virtual_ArcFilter_context_t * __clone_arc_filter( const vgx_virtual_ArcFilter_context_t *other );
static void __delete_arc_filter( vgx_virtual_ArcFilter_context_t **filter );
static vgx_boolean_logic __logic_from_predicators( const vgx_predicator_t predicator1, vgx_predicator_t const predicator2 );
static int __configure_predicators_from_arc_condition_set( vgx_Graph_t *self, const vgx_ArcConditionSet_t *arc_condition_set, vgx_predicator_t *predicator1, vgx_predicator_t *predicator2 );



/*******************************************************************//**
 * Vertex Filter Interface
 * 
 ***********************************************************************
 */
//static f_vgx_VertexFilter __get_vertex_filter_function( vgx_VertexFilter_context_t *vertexfilter_context  );
static vgx_VertexFilter_context_t * __new_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget );
static vgx_VertexFilter_context_t * __clone_vertex_filter( const vgx_VertexFilter_context_t *other );
static void __delete_vertex_filter( vgx_VertexFilter_context_t **filter );



/*******************************************************************//**
 * Private
 * 
 ***********************************************************************
 */

static vgx_virtual_ArcFilter_context_t * __new_wildcard_arc_filter( bool pass );
static vgx_virtual_ArcFilter_context_t * __new_relationship_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_modifier_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_value_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_specific_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_relationship_value_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_modifier_value_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_specific_value_arc_filter( const vgx_predicator_t predicator );
static vgx_virtual_ArcFilter_context_t * __new_evaluator_arc_filter( bool readonly_graph, bool positive, vgx_Evaluator_t *traversing_evaluator );
static vgx_virtual_ArcFilter_context_t * __new_generic_arc_filter( vgx_Graph_t *self, bool readonly_graph, const vgx_predicator_t predicator1, const vgx_predicator_t predicator2, const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *traversing_evaluator, vgx_ExecutionTimingBudget_t *timing_budget );

static vgx_VertexFilter_context_t * __new_evaluator_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget );
static vgx_VertexFilter_context_t * __new_generic_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_ArcFilterFunction_t arcfilterfunc = {
  .Pass                           = __pass_arcfilter,
  .Stop                           = __stop_arcfilter,
  .RelationshipFilter             = __relationship_arcfilter,
  .ModifierFilter                 = __modifier_arcfilter,
  .ValueFilter                    = __value_arcfilter,
  .HamDistFilter                  = __hamming_distance_arcfilter,
  .SpecificFilter                 = __specific_arcfilter,
  .RelationshipValueFilter        = __relationship_value_arcfilter,
  .RelationshipHamDistFilter      = __relationship_hamming_distance_arcfilter,
  .ModifierValueFilter            = __modifier_value_arcfilter,
  .ModifierHamDistFilter          = __modifier_hamming_distance_arcfilter,
  .SpecificValueFilter            = __specific_value_arcfilter,
  .SpecificHamDistFilter          = __specific_hamming_distance_arcfilter,
  .EvaluatorFilter                = __evaluator_arcfilter,
  .GenericArcFilter               = __generic_arcfilter,
  .GenPredLocEvalVertexArcFilter  = __generic_pred_loceval_vertex_arcfilter,
  .GenLocEvalVertexArcFilter      = __generic_loceval_vertex_arcfilter,
  .GenPredVertexArcFilter         = __generic_pred_vertex_arcfilter,
  .GenVertexArcFilter             = __generic_vertex_arcfilter,
  .DirectRecursionArcFilter       = __direct_recursion_arcfilter,
  .GenPredLocEvalArcFilter        = __generic_pred_loceval_arcfilter,
  .GenLocEvalArcFilter            = __generic_loceval_arcfilter,
  .GenPredArcFilter               = __generic_pred_arcfilter
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_PredicatorMatchFunction_t predmatchfunc = {
  .Relationship = __arcvector_predicator_match_relationship,
  .Modifier     = __arcvector_predicator_match_modifier,
  .Key          = __arcvector_predicator_match_key,
  .Any          = __arcvector_predicator_match_any,
  .Generic      = __arcvector_predicator_match_generic,
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_IArcFilter_t iArcFilter = {
  .New                                      = __new_arc_filter,
  .Clone                                    = __clone_arc_filter,
  .Delete                                   = __delete_arc_filter,
  .LogicFromPredicators                     = __logic_from_predicators,
  .ConfigurePredicatorsFromArcConditionSet  = __configure_predicators_from_arc_condition_set
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_VertexFilterFunction_t vertexfilterfunc = {
  .Pass                   = __pass_vertexfilter,
  .GenericVertexFilter    = __generic_vertexfilter,
  .EvaluatorVertexFilter  = __evaluator_vertexfilter
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_VertexMatchFunction_t vtxmatchfunc = {
  .Identifier     = __arcvector_vertex_condition_match_single_identifier,
  .IdentifierList = __arcvector_vertex_condition_match_identifier_list,
  .Type           = __arcvector_vertex_condition_match_type,
  .Degree         = __arcvector_vertex_condition_match_degree,
  .Timestamp      = __arcvector_vertex_condition_match_timestamps,
  .Similarity     = __arcvector_vertex_condition_match_similarity
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_EXPORT vgx_IVertexFilter_t iVertexFilter = {
  .New            = __new_vertex_filter,
  .Clone          = __clone_vertex_filter,
  .Delete         = __delete_vertex_filter
};



/*******************************************************************//**
 * Predicator match macros
 * 
 ***********************************************************************
 */
// Non-zero when Probe REL != Target REL
#define __REL_DIFF( DiffBits )      ((DiffBits) & __VGX_PREDICATOR_REL_MASK)
// Non-zero when Probe REL is not *
#define __REL_CONDITION( Probe )    ((Probe).data & __VGX_PREDICATOR_REL_MASK)
// Non-zero when Probe MOD != Target MOD
#define __MOD_DIFF( DiffBits )     ((DiffBits) & __VGX_PREDICATOR_SMT_MASK)
// Non-zero when Probe MOD is not *
#define __MOD_CONDITION( Probe )   ((Probe).data & __VGX_PREDICATOR_SMT_MASK )
// Non-zero when Probe KEY != Target KEY
#define __KEY_DIFF( DiffBits )      ((DiffBits) & __VGX_PREDICATOR_KEY_MASK)
// Non-zero when Probe KEY is not *
#define __KEY_CONDITION( Probe )    ((Probe).data & __VGX_PREDICATOR_KEY_MASK)
// Non-zero when Probe VAL is not *
#define __VAL_CONDITION( Probe )    ((Probe).data & __VGX_PREDICATOR_EQU_MASK )
// True if value condition is >=
#define __VAL_COND_GTE( Condition ) (Condition == __VGX_PREDICATOR_GTE_MASK)
// True if value condition is <=
#define __VAL_COND_LTE( Condition ) (Condition == __VGX_PREDICATOR_LTE_MASK)
// True if value condition is ==
#define __VAL_COND_EQU( Condition ) (Condition == __VGX_PREDICATOR_EQU_MASK)
// Non-zero if target value is float type, otherwise int type
#define __TARGET_IS_FLOAT( Target ) ((Target).data & __VGX_PREDICATOR_FLT_MASK)
// True if float x < y
#define __FLOAT_X_LT_Y( X, Y )      ((X).val.real < (Y).val.real)
// True if float x > y
#define __FLOAT_X_GT_Y( X, Y )      ((X).val.real > (Y).val.real)
// True if float x != y (using epsilon)
#define __FLOAT_X_NEQ_Y( X, Y )     (fabs( (double)(X).val.real - (Y).val.real ) > (1.0/(1LL<<18)))
// True if int x < y
#define __INT_X_LT_Y( X, Y )        ((X).val.integer < (Y).val.integer)
// True if int x > y
#define __INT_X_GT_Y( X, Y )        ((X).val.integer > (Y).val.integer)
// True if inx x != y
#define __INT_X_NEQ_Y( X, Y )       ((X).val.integer != (Y).val.integer)
//
#define __MISS 0
#define __HIT 1



/*******************************************************************//**
 * Vertex match macros
 * 
 ***********************************************************************
 */
// Convenience test within switch statement
#define __REQUIRE( Condition, Miss ) if( Condition ) { break; } else { return (Miss); }
#define __MISS__(Miss)     return (Miss);
// A < B
#define __LT__(A,B,Miss)   __REQUIRE( (A) <  (B), Miss )
// A <= B
#define __LTE__(A,B,Miss)  __REQUIRE( (A) <= (B), Miss )
// A > B
#define __GT__(A,B,Miss)   __REQUIRE( (A) >  (B), Miss )
// A >= B
#define __GTE__(A,B,Miss)  __REQUIRE( (A) >= (B), Miss )
// A == B
#define __EQU__(A,B,Miss)  __REQUIRE( (A) == (B), Miss )
// A != B
#define __NEQ__(A,B,Miss)  __REQUIRE( (A) != (B), Miss )
// hamdist(A, B) < X
#define __HAM__(A,B,X,Miss)  __REQUIRE( POPCNT64( (A) ^ (B) ) <= (X), Miss )
// X in [L, H]   (endpoints included)
#define __RANGE__(X,L,H,Miss)  __REQUIRE( !((L) > (X) || (H) < (X)), Miss )
// X not in [L, H]   (endpoints included)
#define __NRANGE__(X,L,H,Miss)  __REQUIRE( ((L) > (X) || (H) < (X)), Miss )
// Convenience switch statement for integer value comparison
#define __GENERIC_INT_VCOMP__( ComparisonCode, TargetValue, ProbeValue1, ProbeValue2, MissSign )  \
  switch( ComparisonCode ) {                                                                  \
  case VGX_VALUE_LTE:         __LTE__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_GT:          __GT__(     TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_GTE:         __GTE__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_LT:          __LT__(     TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_EQU:         __EQU__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_NEQ:         __NEQ__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_RANGE:       __RANGE__(  TargetValue, ProbeValue1, ProbeValue2, MissSign )   \
  case VGX_VALUE_NRANGE:      __NRANGE__( TargetValue, ProbeValue1, ProbeValue2, MissSign )   \
  default:                    __MISS__( MissSign )                                            \
  }
// Convenience switch statement for float value comparison
#define __GENERIC_FLOAT_VCOMP__( ComparisonCode, TargetValue, ProbeValue1, ProbeValue2, MissSign )  \
  switch( ComparisonCode ) {                                                                  \
  case VGX_VALUE_LTE:         __LTE__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_GT:          __GT__(     TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_GTE:         __GTE__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_LT:          __LT__(     TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_EQU:         __EQU__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_NEQ:         __NEQ__(    TargetValue, ProbeValue1, MissSign )                \
  case VGX_VALUE_RANGE:       __RANGE__(  TargetValue, ProbeValue1, ProbeValue2, MissSign )   \
  case VGX_VALUE_NRANGE:      __NRANGE__( TargetValue, ProbeValue1, ProbeValue2, MissSign )   \
  default:                    __MISS__( MissSign )                                            \
  }
#define __NO_VALUE__ 0



/*******************************************************************//**
 * Match predicator relationship only
 * NOTE: Probe must not have relationship wildcard
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_relationship( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !__REL_DIFF( probe.data ^ target.data );
}



/*******************************************************************//**
 * Match predicator modifier only
 * NOTE: Probe must not have modifier wildcard
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_modifier( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !__MOD_DIFF( probe.data ^ target.data );
}



/*******************************************************************//**
 * Match predicator key (relationship and modifier)
 * NOTE: Probe key must not be 0
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_key( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !__KEY_DIFF( probe.data ^ target.data );
}



/*******************************************************************//**
 * Always 1
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __arcvector_predicator_match_any( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return 1;
}



/*******************************************************************//**
 * Match predicator value only
 * NOTE: Probe must not have value wildcard
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_value( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  if( __TARGET_IS_FLOAT( target ) ) {
    switch( __VAL_CONDITION( probe ) ) {
    case __VGX_PREDICATOR_GTE_MASK:
      return !(__FLOAT_X_LT_Y( target, probe ) ^ probe.mod.probe.NEG);
    case __VGX_PREDICATOR_LTE_MASK:
      return !(__FLOAT_X_GT_Y( target, probe ) ^ probe.mod.probe.NEG);
    case __VGX_PREDICATOR_EQU_MASK:
      return !(__FLOAT_X_NEQ_Y( target, probe ) ^ probe.mod.probe.NEG);
    }
  }
  else {
    switch( __VAL_CONDITION( probe ) ) {
    case __VGX_PREDICATOR_GTE_MASK:
      return !(__INT_X_LT_Y( target, probe ) ^ probe.mod.probe.NEG);
    case __VGX_PREDICATOR_LTE_MASK:
      return !(__INT_X_GT_Y( target, probe ) ^ probe.mod.probe.NEG);
    case __VGX_PREDICATOR_EQU_MASK:
      return !(__INT_X_NEQ_Y( target, probe ) ^ probe.mod.probe.NEG);
    }
  }
  return !probe.mod.probe.NEG;
}



/*******************************************************************//**
 * Predicator value bit pattern hamming distance match
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  unsigned ham = POPCNT32( target.val.bits ^ probe.val.bits );
  unsigned pval = _vgx_predicator_eph_get_value( probe );
  switch( __VAL_CONDITION( probe ) ) {
  case __VGX_PREDICATOR_GTE_MASK:
    return !( (ham < pval) ^ probe.mod.probe.NEG );
  case __VGX_PREDICATOR_LTE_MASK:
    return !( (ham > pval) ^ probe.mod.probe.NEG );
  case __VGX_PREDICATOR_EQU_MASK:
    return !( (ham != pval) ^ probe.mod.probe.NEG );
  }
  return !probe.mod.probe.NEG;
}



/*******************************************************************//**
 * Match predicator relationship and modifier only
 * NOTE: Probe must not have relationship or modifier wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_specific( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  uint64_t diff = probe.data ^ target.data;
  return !( __REL_DIFF( diff ) || __MOD_DIFF( diff ) );
}



/*******************************************************************//**
 * Match predicator relationship and value
 * NOTE: Probe must not have relationship or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_relationship_value( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !( __REL_DIFF( probe.data ^ target.data ) || !__arcvector_predicator_match_value( probe, target ) );
}



/*******************************************************************//**
 * Match predicator relationship and value hamming distance
 * NOTE: Probe must not have relationship or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_relationship_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !( __REL_DIFF( probe.data ^ target.data ) || !__arcvector_predicator_match_value_hamming_distance( probe, target ) );
}



/*******************************************************************//**
 * Match predicator modifier and value
 * NOTE: Probe must not have modifier or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_modifier_value( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !( __MOD_DIFF( probe.data ^ target.data ) || !__arcvector_predicator_match_value( probe, target ) );
}



/*******************************************************************//**
 * Match predicator modifier and value hamming distance
 * NOTE: Probe must not have modifier or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_modifier_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  return !( __MOD_DIFF( probe.data ^ target.data ) || !__arcvector_predicator_match_value_hamming_distance( probe, target ) );
}



/*******************************************************************//**
 * Match predicator relationship, modifier and value
 * NOTE: Probe must not have relationship, modifier or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_specific_value( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  uint64_t diff = probe.data ^ target.data;
  return !( __REL_DIFF( diff ) || __MOD_DIFF( diff ) || !__arcvector_predicator_match_value( probe, target ) );
}



/*******************************************************************//**
 * Match predicator relationship, modifier and value hamming distance
 * NOTE: Probe must not have relationship, modifier or value wildcards
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_specific_value_hamming_distance( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  uint64_t diff = probe.data ^ target.data;
  return !( __REL_DIFF( diff ) || __MOD_DIFF( diff ) || !__arcvector_predicator_match_value_hamming_distance( probe, target ) );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match_generic( const vgx_predicator_t probe, const vgx_predicator_t target ) {
  if( _vgx_predicator_full_wildcard( probe ) || _vgx_predicator_is_synthetic( target ) ) {
    return __HIT;
  }
  else {
    // Set differing bits to one
    register uint64_t diff = probe.data ^ target.data;

    // Probe REL != Target REL and Probe REL not *
    if( __REL_DIFF(diff) && __REL_CONDITION(probe) ) {
      return __MISS;
    }

    // Probe MOD not *
    if( __MOD_CONDITION(probe) ) {

      // Probe MOD != Target MOD
      if( __MOD_DIFF(diff) ) {
        return __MISS;
      }

      // Probe VAL not *
      if( __VAL_CONDITION(probe) ) {
        if( _vgx_predicator_eph_is_lsh( probe ) ) {
          return __arcvector_predicator_match_value_hamming_distance( probe, target );
        }
        else {
          return __arcvector_predicator_match_value( probe, target );
        }
      }
    }
    // Not filtered out, i.e. hit
    return __HIT;
  }
}



/*******************************************************************//**
 *
 * NOTE: delta_pred_condition will always contain a delta.real or delta.integer
 *       when entering and will be updated by adding this delta.<val>
 *       to the value in the previous_predicator according to the modifier
 *       type of the previous_predicator, then the result is assigned back to the 
 *       delta_pred_condition.val.<vtype> according to the modifier type.
 ***********************************************************************
 */
__inline static int __update_predicator_value_from_delta( vgx_predicator_t *delta_pred_condition, const vgx_predicator_t previous_predicator ) {
  if( ((delta_pred_condition->data ^ previous_predicator.data) & __VGX_PREDICATOR_FLT_MASK) != 0 ) {
    return -1; // Can't update incompatible values
  }

  // Float predicator
  if( delta_pred_condition->data & __VGX_PREDICATOR_FLT_MASK ) {
    delta_pred_condition->val.real = previous_predicator.val.real + delta_pred_condition->delta.real;
  }
  // Integer predicator
  else {
    int32_t delta = delta_pred_condition->delta.integer; // ALWAYS SIGNED INTEGER

    if( delta_pred_condition->mod.stored.type == VGX_PREDICATOR_MOD_INTEGER ) {
      delta_pred_condition->val.integer = delta + (previous_predicator.mod.stored.type == VGX_PREDICATOR_MOD_INTEGER ? previous_predicator.val.integer : previous_predicator.val.uinteger);
    }
    else {
      delta_pred_condition->val.uinteger = delta + (previous_predicator.mod.stored.type == VGX_PREDICATOR_MOD_INTEGER ? previous_predicator.val.integer : previous_predicator.val.uinteger);
    }
  }
  
  return 0;
}



/*******************************************************************//**
 *
 * NOTE: ratio_pred_condition will always contain a ratio.real when entering
 *       and will be updated by multiplying this real value with the value
 *       in previous_predicator according to the modifier type of the
 *       previous_predicator, then the result is assigned back to the 
 *       ratio_pred_condition.val.<vtype> according to the modifier type.
 ***********************************************************************
 */
__inline static int __update_predicator_value_from_ratio( vgx_predicator_t *ratio_pred_condition, const vgx_predicator_t previous_predicator ) {
  if( ((ratio_pred_condition->data ^ previous_predicator.data) & __VGX_PREDICATOR_FLT_MASK) != 0 ) {
    return -1; // Can't update incompatible values
  }

  // Float predicator
  if( ratio_pred_condition->data & __VGX_PREDICATOR_FLT_MASK ) {
    ratio_pred_condition->val.real = ratio_pred_condition->ratio.real * previous_predicator.val.real;
  }
  // Integer predicator
  else {
    double fval = round( ratio_pred_condition->ratio.real * (previous_predicator.mod.stored.type == VGX_PREDICATOR_MOD_INTEGER ? (double)previous_predicator.val.integer : (double)previous_predicator.val.uinteger) );
    if( ratio_pred_condition->mod.stored.type == VGX_PREDICATOR_MOD_INTEGER ) {
      ratio_pred_condition->val.integer = fval > (double)CXLIB_LONG_MAX ? CXLIB_LONG_MAX : fval > (double)CXLIB_LONG_MIN ? (int32_t)fval : CXLIB_LONG_MIN;
    }
    else {
      ratio_pred_condition->val.uinteger = fval > (double)CXLIB_ULONG_MAX ? CXLIB_ULONG_MAX : fval > 0 ? (uint32_t)fval : 0;
    }
  }
  
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __dynamic_predicator_match( vgx_predicator_t pred_condition, vgx_predicator_t this_predicator, vgx_predicator_t previous_predicator ) {
#define __MODIFIER_TYPE_MISMATCH( Predicator1, Predicator2 ) (((Predicator1).data ^ (Predicator2).data) & __VGX_PREDICATOR_SMT_MASK)

  // Predicator is full wildcard: automatic hit
  if( _vgx_predicator_full_wildcard( pred_condition ) ) {
    return 1;
  }

  // Predicator is dynamic with a +/- delta: update the condition using previous predicator's value
  if( _vgx_predicator_eph_is_dyndelta( pred_condition ) ) {
    if( __MODIFIER_TYPE_MISMATCH( pred_condition, this_predicator ) || __update_predicator_value_from_delta( &pred_condition, previous_predicator ) < 0 ) {
      return 0;
    }
  }
  // Predicator is dynamic with a ratio multiplier: update the condition using previous predicator's value
  else if( _vgx_predicator_eph_is_dynratio( pred_condition ) ) {
    if( __MODIFIER_TYPE_MISMATCH( pred_condition, this_predicator ) || __update_predicator_value_from_ratio( &pred_condition, previous_predicator ) < 0 ) {
      return 0;
    }
  }

  // Test predicator
  if( !predmatchfunc.Generic( pred_condition, this_predicator ) ) {
    return 0;
  }

  // Pass
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __dynamic_arc_filter_callback( vgx_GenericArcFilter_context_t *context, const vgx_LockableArc_t *larc ) {

#define __MODIFIER_TYPE_MISMATCH( Predicator1, Predicator2 ) (((Predicator1).data ^ (Predicator2).data) & __VGX_PREDICATOR_SMT_MASK)
  
  if( context->previous_context == NULL ) {
    return 0;  // no previous neighborhood, automatic miss
  }

  vgx_predicator_t this_predicator = larc->head.predicator;
  vgx_predicator_t previous_predicator = context->previous_context->current_head->predicator;

  // == PREDICATOR 1 ==
  if( !__dynamic_predicator_match( context->pred_condition1, this_predicator, previous_predicator ) ) {
    return 0;
  }

  // == PREDICATOR 2 ==
  if( !__dynamic_predicator_match( context->pred_condition2, this_predicator, previous_predicator ) ) {
    return 0;
  }

  // Hit
  return 1;
}



/*******************************************************************//**
 * Match vertex type only
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_type( const vgx_vertex_probe_spec spec, const vgx_vertex_type_t type_probe, const vgx_Vertex_t *vertex_RO ) {
  switch( (spec & _VERTEX_PROBE_TYPE_MASK) ^ _VERTEX_PROBE_TYPE_ENA ) {
  case _VERTEX_PROBE_TYPE_EQU: __EQU__( type_probe, vertex_RO->descriptor.type.enumeration, 0 )
  case _VERTEX_PROBE_TYPE_NEQ: __NEQ__( type_probe, vertex_RO->descriptor.type.enumeration, 0 )
  default:                     return 0; // INVALID CASE, SHOULD NEVER HAPPEN
  }

  // match
  return 1;
}



/*******************************************************************//**
 * Match vertex degree only
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_degree( const vgx_vertex_probe_t *vertex_probe, const vgx_Vertex_t *vertex_RO ) {
  vgx_vertex_probe_spec spec = vertex_probe->spec;
  vgx_value_comparison vcomp;
  _vgx_ArcVector_VxD_tag itag = TPTR_AS_TAG(&vertex_RO->inarcs.VxD);
  _vgx_ArcVector_VxD_tag otag = TPTR_AS_TAG(&vertex_RO->outarcs.VxD);
  int64_t indegree  = itag == VGX_ARCVECTOR_VxD_DEGREE ? __arcvector_get_degree( &vertex_RO->inarcs )  : itag == VGX_ARCVECTOR_VxD_EMPTY ? 0 : 1; 
  int64_t outdegree = otag == VGX_ARCVECTOR_VxD_DEGREE ? __arcvector_get_degree( &vertex_RO->outarcs ) : otag == VGX_ARCVECTOR_VxD_EMPTY ? 0 : 1; 

  static const int hit = 1;

  // total degree
  if( spec & _VERTEX_PROBE_DEGREE_ENA ) {
    int64_t degree = indegree + outdegree;
    vcomp = ( ( spec & _VERTEX_PROBE_DEGREE_MASK ) ^ _VERTEX_PROBE_DEGREE_ENA ) >> _VERTEX_PROBE_DEGREE_OFFSET;
    __GENERIC_INT_VCOMP__( vcomp, degree, vertex_probe->degree, __NO_VALUE__, !hit )
  }

  // indegree
  if( spec & _VERTEX_PROBE_INDEGREE_ENA ) {
    vcomp = ( ( spec & _VERTEX_PROBE_INDEGREE_MASK ) ^ _VERTEX_PROBE_INDEGREE_ENA ) >> _VERTEX_PROBE_INDEGREE_OFFSET;
    __GENERIC_INT_VCOMP__( vcomp, indegree, vertex_probe->indegree, __NO_VALUE__, !hit )
  }

  // outdegree
  if( spec & _VERTEX_PROBE_OUTDEGREE_ENA ) {
    vcomp = ( ( spec & _VERTEX_PROBE_OUTDEGREE_MASK ) ^ _VERTEX_PROBE_OUTDEGREE_ENA ) >> _VERTEX_PROBE_OUTDEGREE_OFFSET;
    __GENERIC_INT_VCOMP__( vcomp, outdegree, vertex_probe->outdegree, __NO_VALUE__, !hit )
  }

  // match
  return hit;
}



/*******************************************************************//**
 * Match vertex timestamps only
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_timestamps( const vgx_timestamp_probe_t *timestamp_probe, const vgx_Vertex_t *vertex_RO ) {
  static const size_t offsets[][2] = {
    { offsetof( vgx_timestamp_probe_t, tmc_valcond ), offsetof( vgx_Vertex_t, TMC ) },
    { offsetof( vgx_timestamp_probe_t, tmm_valcond ), offsetof( vgx_Vertex_t, TMM ) },
    { offsetof( vgx_timestamp_probe_t, tmx_valcond ), offsetof( vgx_Vertex_t, TMX.vertex_ts ) }
  };

  int hit = timestamp_probe->positive ? 1 : 0;

  for( int tx=0; tx<3; tx++ ) {
    vgx_value_condition_t *pcond = (vgx_value_condition_t*)((char*)timestamp_probe + offsets[tx][0]);
    if( pcond->vcomp != VGX_VALUE_ANY ) {
      uint32_t value = *(uint32_t*)((char*)vertex_RO + offsets[tx][1]);
      __GENERIC_INT_VCOMP__( pcond->vcomp, value, pcond->value1.data.simple.integer, pcond->value2.data.simple.integer, !hit )
    }
  }

  return hit;
}



/*******************************************************************//**
 * Match vertex similarity only
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_similarity( const vgx_similarity_probe_t *similarity_probe, const vgx_Vector_t *vertex_vector ) {

  int hit = similarity_probe->positive ? 1 : 0;

  // HAMMING DISANCE (fast)
  if( similarity_probe->hamdist.vcomp != VGX_VALUE_ANY ) {
    // Vertex has NULL vector, automatic miss
    if( vertex_vector == NULL ) {
      return !hit; // miss
    }
    const vgx_value_condition_t *pham = &similarity_probe->hamdist;
    // Compute hamming distance
    int64_t hamdist = hamdist64( similarity_probe->fingerprint, vertex_vector->fp );
    // Match hamming distance
    __GENERIC_INT_VCOMP__( pham->vcomp, hamdist, pham->value1.data.simple.integer, pham->value2.data.simple.integer, !hit );
  }

  // COSINE/JACCARD (slow)
  if( similarity_probe->simscore.vcomp != VGX_VALUE_ANY ) {
    // Vertex has NULL vector, automatic miss
    if( vertex_vector == NULL ) {
      return !hit; // miss
    }
    const vgx_value_condition_t *pscore = &similarity_probe->simscore;
    // Compute similarity score
    vgx_Similarity_t *S = similarity_probe->simcontext;
    double sim = CALLABLE(S)->Similarity( S, similarity_probe->probevector, vertex_vector );
    // Match similarity score
    __GENERIC_FLOAT_VCOMP__( pscore->vcomp, sim, pscore->value1.data.simple.real, pscore->value2.data.simple.real, !hit )
  }

  // match
  return hit;
}



/*******************************************************************//**
 * Match single vertex ID
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_single_identifier( vgx_vertex_probe_spec spec, const CString_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid ) {
  int is_match;
  const char *probe_str = CStringValue( CSTR__probe );

  // Prefix match

  // TODO: COMPRESSED CHECK FOR PREFIX!

  if( (spec & _VERTEX_PROBE_ID_EQU) == _VERTEX_PROBE_ID_LTE ) {
    const size_t sz_probe_str = CStringLength( CSTR__probe );
    const size_t max_prefix = sizeof( vgx_VertexIdentifierPrefix_t ) - 1;
    // Unusual prefix probe: it's very large and will rarely happen
    if( sz_probe_str > max_prefix ) {
      if( vertex_identifier->CSTR__idstr == NULL ) {
        // (edge case)
        // The probe length is greater than the largest embedded prefix that a vertex can store and the
        // vertex doesn't have the extra id string reference, which means there can't be a match.
        is_match = 0;
      }
      else {
        is_match = CStringStartsWith( vertex_identifier->CSTR__idstr, probe_str );
      }
    }
    // Normal prefix probe: probe can be compared directly with the vertex embedded prefix
    else {
      is_match = CharsStartsWithConst( vertex_identifier->idprefix.data, probe_str );
    }
  }
  // Exact match
  else {
    // Quick comparison of obid.
    // WARNING: This assumes that the vertex OBID was derived from its identifier string when it was 
    //          constructed, and not set separately. I.e. if a custom obid was supplied to the vertex
    //          constructor in addition to an identifier string (whose obid is the hash128 of the string
    //          value) then this match will generally always be false.
    // TODO:    Clean this up, either disallow custom obid for vertex construction, or have some flag
    //          to indicate and do something to handle.
    if( vertex_internalid ) {
      is_match = idmatch( vertex_internalid, CStringObid( CSTR__probe ) );
    }
    else if( vertex_identifier->CSTR__idstr ) {
      is_match = CStringEquals( vertex_identifier->CSTR__idstr, CSTR__probe );
    }
    else {
      is_match = CharsEqualsConst( vertex_identifier->idprefix.data, probe_str );
    }
  }

  switch( (spec & _VERTEX_PROBE_ID_MASK) ^ _VERTEX_PROBE_ID_ENA ) {
  case _VERTEX_PROBE_ID_LTE: __REQUIRE( is_match, 0 )  // prefix match
  case _VERTEX_PROBE_ID_GT:  __REQUIRE( !is_match, 0 ) // not prefix match
  case _VERTEX_PROBE_ID_EQU: __REQUIRE( is_match, 0 )  // exact match
  case _VERTEX_PROBE_ID_NEQ: __REQUIRE( !is_match, 0 ) // not exact match
  default:                   return 0;              // INVALID CASE, SHOULD NEVER HAPPEN
  }

  // match
  return 1;
}



/*******************************************************************//**
 * Match vertex ID list (OR LOGIC)
 ***********************************************************************
 */
static __inline int __arcvector_vertex_condition_match_identifier_list( vgx_vertex_probe_spec spec, const vgx_StringList_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid ) {

  int64_t sz = iString.List.Size( CSTR__probe );
  for( int64_t i=0; i<sz; i++ ) {
    if( __arcvector_vertex_condition_match_single_identifier( spec, iString.List.GetItem( (vgx_StringList_t*)CSTR__probe, i ), vertex_identifier, vertex_internalid ) ) {
      return 1;
    }
  }

  return 0; // miss
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
//static int __arcvector_vertex_condition_match_recursive( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_LockableArc_t *larc, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
static int __arcvector_vertex_condition_match_recursive( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
  // LOCAL FILTER
  vgx_Evaluator_t *filter = vertex_probe->advanced.local_evaluator.filter;
  if( filter ) {
    if( arcfilter_context ) {
      // Execute filter evaluation unless already executed as part of culling earlier
      if( ((vgx_GenericArcFilter_context_t*)arcfilter_context)->culleval != filter ) { // <== culleval is the same as filter if culling took place!
        vgx_Vector_t *vector = __simprobe_vector( vertex_probe );
        CALLABLE( filter )->SetContext( filter, arcfilter_context->current_tail, arcfilter_context->current_head, vector, 0.0 );
        vgx_EvalStackItem_t *result = CALLABLE( filter )->EvalVertex( filter, vertex_RO );
        if( result == NULL || !iEvaluator.IsPositive( result ) ) {
          *match = VGX_ARC_FILTER_MATCH_MISS;
          return 0;
        }
      }
    }
    else {
      vgx_EvalStackItem_t *result = CALLABLE( filter )->EvalVertex( filter, vertex_RO );
      if( result == NULL || !iEvaluator.IsPositive( result ) ) {
        *match = VGX_ARC_FILTER_MATCH_MISS;
        return 0;
      }
    }
  }

  // MAIN + POST
  return __arcvector_vertex_condition_match_eval_recursion( arcfilter_context, vertex_probe, vertex_RO, match );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_condition_match_eval_recursion( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
  int ret = 1;

  // NEXT NEIGHBORHOOD PROBE
  if( vertex_probe->advanced.next.neighborhood_probe != NULL ) {
    // Fetch the probe for the next neighborhood and update it with our current visited vertex as the anchor for the recursive probe.
    vgx_neighborhood_probe_t *next = vertex_probe->advanced.next.neighborhood_probe;
    vgx_virtual_ArcFilter_context_t *cond_ctx = next->conditional.arcfilter;
    vgx_virtual_ArcFilter_context_t *trav_ctx = next->traversing.arcfilter;
    vgx_virtual_ArcFilter_context_t *coll_ctx = next->collect_filter_context;
    next->current_tail_RO = vertex_RO;
    // Keep backwards reference to the filter context leading to the next neighborhood
    cond_ctx->previous_context = trav_ctx->previous_context = coll_ctx->previous_context = arcfilter_context;
    // Constant during next neighborhood traversal
    cond_ctx->current_tail = trav_ctx->current_tail = coll_ctx->current_tail = vertex_RO;
    // Variable during next neighborhood traversal (used for multiple arcvector traversal, very detailed stuff..)
    cond_ctx->current_head = trav_ctx->current_head = coll_ctx->current_head = NULL;

    const vgx_ArcHead_t *current_head = arcfilter_context ? arcfilter_context->current_head : NULL;
    vgx_Evaluator_t *cond_ev = next->conditional.evaluator;
    vgx_Evaluator_t *trav_ev = next->traversing.evaluator;

    if( cond_ev ) {
      vgx_Vector_t *vector = __simprobe_vector( next->conditional.vertex_probe );
      CALLABLE( cond_ev )->SetContext( cond_ev, vertex_RO, current_head, vector, 0.0 );
    }
    if( trav_ev && trav_ev != cond_ev ) {
      vgx_Vector_t *vector = __simprobe_vector( next->traversing.vertex_probe );
      CALLABLE( trav_ev )->SetContext( trav_ev, vertex_RO, current_head, vector, 0.0 );
    }

    // ---------------------------
    // Next neighborhood recursion
    if( !__arcvector_vertex_next( next, match ) ) {
      ret = 0;
      goto postfilter;
    }
    // ---------------------------
  }

  // match
  *match = VGX_ARC_FILTER_MATCH_HIT;

postfilter: 
  if( vertex_probe->advanced.local_evaluator.post ) {
    vgx_Evaluator_t *post = vertex_probe->advanced.local_evaluator.post;
    if( arcfilter_context ) {
      vgx_Vector_t *vector = __simprobe_vector( vertex_probe );
      CALLABLE( post )->SetContext( post, arcfilter_context->current_tail, arcfilter_context->current_head, vector, 0.0 );
    }
    vgx_EvalStackItem_t *result = CALLABLE( post )->EvalVertex( post, vertex_RO );
    if( result == NULL || !iEvaluator.IsPositive( result ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
      ret = 0;
    }
  }

  return ret;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_condition_match_details( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {

  // DEGREE PROBE
  vgx_degree_probe_t *dg_probe;
  vgx_neighborhood_probe_t *next;
  if( (dg_probe = vertex_probe->advanced.degree_probe) != NULL ) {
    // Init to miss
    *match = VGX_ARC_FILTER_MATCH_MISS;
    next = dg_probe->neighborhood_probe;
    next->current_tail_RO = vertex_RO;
    vgx_recursive_probe_t *recursive = &next->traversing;
    vgx_virtual_ArcFilter_context_t *filter = recursive->arcfilter;
    filter->previous_context = arcfilter_context;  // Keep backwards reference to the filter context leading to the next neighborhood
    filter->current_tail = vertex_RO;              // Constant during arcvector traversal
    filter->current_head = NULL;                   // Set as needed during traversal
    dg_probe->collector->n_collectable = 0; // Reset count before running traversal
    switch( recursive->arcdir ) {
    case VGX_ARCDIR_ANY:
      iarcvector.GetArcs( &vertex_RO->inarcs, next );
      // TODO AMBD-1938: Yield vertex_RO's inarcs for the duration of outarc scan (if the recursive query cycles back to vertex_RO it will be RO-acquired again, remember then to reclaim inarcs!)
      iarcvector.GetArcs( &vertex_RO->outarcs, next );
      break;
    case VGX_ARCDIR_IN:
      iarcvector.GetArcs( &vertex_RO->inarcs, next );
      break;
    case VGX_ARCDIR_OUT:
      // TODO AMBD-1938: Yield vertex_RO's inarcs for the duration of outarc scan (if the recursive query cycles back to vertex_RO it will be RO-acquired again, remember then to reclaim inarcs!)
      iarcvector.GetArcs( &vertex_RO->outarcs, next );
      break;
    default:
      break;
    }
    // Check timeout
    if( _vgx_is_execution_halted( vertex_probe->timing_budget ) ) {
      *match = __arcfilter_error();
      return 0;
    }
    // Check count
    int64_t count = dg_probe->collector->n_collectable;
    const vgx_value_condition_t *vcond = &dg_probe->value_condition;
    __GENERIC_INT_VCOMP__( vcond->vcomp, count, vcond->value1.data.simple.integer, vcond->value2.data.simple.integer, 0 ); // <- returns on miss!
    dg_probe->current_value = count;
  }

  // TIMESTAMP PROBE
  const vgx_timestamp_probe_t *tm_probe;
  if( (tm_probe = vertex_probe->advanced.timestamp_probe) != NULL ) {
    if( !__arcvector_vertex_condition_match_timestamps( tm_probe, vertex_RO ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
      return 0;
    }
  }

  // SIMILARITY PROBE
  const vgx_similarity_probe_t *sim_probe;
  if( (sim_probe = vertex_probe->advanced.similarity_probe) != NULL ) {
    if( !__arcvector_vertex_condition_match_similarity( sim_probe, vertex_RO->vector ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
      return 0;
    }
  }

  // PROPERTY PROBE
  const vgx_property_probe_t *prop_probe;
  if( (prop_probe = vertex_probe->advanced.property_probe) != NULL ) {
    if( !__arcvector_vertex_condition_match_property_conditions( prop_probe, vertex_RO ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
      return 0;
    }
  }

  // match
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_condition_match_generic( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
  __assert_vertex_lock( vertex_RO );

  // Vertex type
  vgx_vertex_probe_spec spec = vertex_probe->spec;
  if( (spec & _VERTEX_PROBE_TYPE_ENA) && !__arcvector_vertex_condition_match_type( vertex_probe->spec, vertex_probe->vertex_type, vertex_RO ) ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // Vertex degree
  if( (spec & _VERTEX_PROBE_ANY_DEGREE_ENA) && !__arcvector_vertex_condition_match_degree( vertex_probe, vertex_RO ) ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // Vertex ID
  if( (spec & _VERTEX_PROBE_ID_ENA) && !__arcvector_vertex_condition_match_identifier_list( vertex_probe->spec, vertex_probe->CSTR__idlist, &vertex_RO->identifier, __vertex_internalid( vertex_RO ) ) ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // Advanced conditions
  if( spec & _VERTEX_PROBE_ADVANCED_ENA ) {
    // If details becomes non-zero we will check each probe
    uint64_t details =  (uintptr_t)vertex_probe->advanced.degree_probe      |
                        (uintptr_t)vertex_probe->advanced.timestamp_probe   |
                        (uintptr_t)vertex_probe->advanced.similarity_probe  |
                        (uintptr_t)vertex_probe->advanced.property_probe;

    uint64_t recursive = (uintptr_t)vertex_probe->advanced.local_evaluator.filter |
                         (uintptr_t)vertex_probe->advanced.local_evaluator.post   |
                         (uintptr_t)vertex_probe->advanced.next.neighborhood_probe;

    if( details && !__arcvector_vertex_condition_match_details( arcfilter_context, vertex_probe, vertex_RO, match ) ) {
      return 0;
    }

    if( recursive && !__arcvector_vertex_condition_match_recursive( arcfilter_context, vertex_probe, vertex_RO, match ) ) {
      return 0;
    }
  }

  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1;

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __pass_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __stop_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  *match = VGX_ARC_FILTER_MATCH_MISS;
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __relationship_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *rel_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_relationship( rel_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( rel_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __modifier_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *mod_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_modifier( mod_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( mod_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *val_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_value( val_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( val_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *hamdist_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  return *match = __arcvector_predicator_match_value_hamming_distance( hamdist_arc_filter->pred_condition1, larc->head.predicator ) ^ !hamdist_arc_filter->positive_match;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __specific_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *relmod_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_specific( relmod_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( relmod_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __relationship_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *relval_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_relationship_value( relval_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( relval_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __relationship_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *rel_hamdist_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  return *match = __arcvector_predicator_match_relationship_value_hamming_distance( rel_hamdist_arc_filter->pred_condition1, larc->head.predicator ) ^ !rel_hamdist_arc_filter->positive_match;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __modifier_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *modval_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_modifier_value( modval_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( modval_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __modifier_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *mod_hamdist_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  return *match = __arcvector_predicator_match_modifier_value_hamming_distance( mod_hamdist_arc_filter->pred_condition1, larc->head.predicator ) ^ !mod_hamdist_arc_filter->positive_match;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __specific_value_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *relmodval_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  bool m = __arcvector_predicator_match_specific_value( relmodval_arc_filter->pred_condition1, larc->head.predicator ) != 0;
  if( relmodval_arc_filter->positive_match == m ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __specific_hamming_distance_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *relmod_hamdist_arc_filter = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  return *match = __arcvector_predicator_match_specific_value_hamming_distance( relmod_hamdist_arc_filter->pred_condition1, larc->head.predicator ) ^ !relmod_hamdist_arc_filter->positive_match;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __evaluator_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_Evaluator_t *evaluator;
  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;
 
  if( (evaluator = GAF->traversing_evaluator) != NULL ) {
    //
    // SECURE THE ARC HEAD AS NEEDED
    //
    if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
      *match = __arcfilter_error();
      return 0; // vertex not readable ==> miss
    }

    // Head safe when here - either locked or no locking required
    bool miss = false;
    vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, larc ); // WARNING: arc MUST have both tail and head vertices assigned, otherwise crash!
    if( !iEvaluator.IsPositive( result ) ) {
      miss = true;
    }

    // Terminate according to pos/neg match logic
    if( GAF->positive_match == miss ) {
      if( !__is_arcfilter_error( *match ) ) {
        *match = VGX_ARC_FILTER_MATCH_MISS;
      }
      return 0;
    }
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static int __terminal_mismatch( const vgx_GenericArcFilter_context_t *filter, const vgx_Vertex_t *vertex, vgx_ArcFilter_match *match ) {
  if( filter->terminal.current && (filter->terminal.current == vertex) != filter->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_culleval( vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {
  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;
  if( GAF->locked_cull ) {
    if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
      *match = __arcfilter_error();
      return 0; // vertex not readable ==> miss
    }
  }
  vgx_Evaluator_t *CE = GAF->culleval;
  vgx_Vector_t *vector = __simprobe_vector( vertex_probe );
  CALLABLE( CE )->SetContext( CE, GAF->current_tail, &larc->head, vector, 0.0 );
  vgx_EvalStackItem_t *result = CALLABLE( CE )->EvalVertex( CE, larc->head.vertex );
  if( result == NULL || !iEvaluator.IsPositive( result ) ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }
  
  // Special case: Simple arc processing will not run 2nd pass, continue the complete filter logic
  if( larc->ctype != VGX_ARCVECTOR_SIMPLE_ARC ) {
    // We terminate here to let the culling process run to completion, then 2nd pass will continue with recursion
    return 1;
  }

  return -1;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK REQUIRED HEAD VERTEX
  // Do we require specific head and is this a match?
  if( __terminal_mismatch( GAF, larc->head.vertex, match ) ) {
    return 0;
  }

  bool miss = false;

  // 2. CHECK THE ARC
  // We have a custom arcfilter check - call the function to determine pass/fail
  if( GAF->arcfilter_callback ) {
    if( !GAF->arcfilter_callback( GAF, larc ) ) {
      miss = true;
    }
  }
  // We check the predicators here
  else {
    switch( GAF->logic ) {
    case VGX_LOGICAL_NO_LOGIC:
      // hit = A
      miss = !__arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator );
      break;
    case VGX_LOGICAL_AND:
      // hit = A*B
      if( !__arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator )
          ||
          !__arcvector_predicator_match_generic( GAF->pred_condition2, larc->head.predicator ) )
      {
        miss = true; // miss = /hit = /A + /B
      }
      break;
    case VGX_LOGICAL_OR:
      // hit = A+B
      if( !__arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator )
          &&
          !__arcvector_predicator_match_generic( GAF->pred_condition2, larc->head.predicator ) )
      {
        miss = true; // miss = /hit = /A * /B
      }
      break;
    case VGX_LOGICAL_XOR:
      // hit = A^B
      if( __arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator )
          ==
          __arcvector_predicator_match_generic( GAF->pred_condition2, larc->head.predicator ) )
      {
        miss = true; // miss = /hit = /(A^B) = A==B
      }
      break;
    // ???
    default:
      *match = __arcfilter_error();
      return 0;
    }
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;;
  vgx_Evaluator_t *ev = GAF->traversing_evaluator;

  // Evaluator is local
  if( ev && CALLABLE( ev )->HeadDeref( ev ) == 0 ) {
    if( iEvaluator.IsPositive( CALLABLE( ev )->EvalArc( ev, larc ) ) != GAF->positive_match ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
      return 0;
    }
    ev = NULL; // no longer needed
  }

  // 3. RUN ADVANCED FILTERS
  if( ev || VP ) {
    // TODO AMBD-1938: If vertex_RO's inarcs are yielded, reclaim them here
    // At this point we may need to lock the head vertex
    const vgx_ArcHead_t *next = &larc->head;
    vgx_Vertex_t *next_RO = next->vertex;

    //
    // Special case: Exact ID match only and no filter evaluator
    //
    if( ev == NULL && VP->spec == _VERTEX_PROBE_ID_EXACT && VP->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY ) {
      *match = VGX_ARC_FILTER_MATCH_HIT;
      return 1;
    }

    // Cull
    if( GAF->culleval ) {
      int ce = __generic_culleval( arcfilter_context, VP, larc, match );
      if( ce >= 0 ) {
        return ce;
      }
    }

    //
    // SECURE THE ARC HEAD AS NEEDED
    //
    if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
      *match = __arcfilter_error();
      return 0; // vertex not readable ==> miss
    }

    // Head safe when here - either locked or no locking required
    do {
      // Traversing filter evaluation
      if( ev && !iEvaluator.IsPositive( CALLABLE( ev )->EvalArc( ev, larc ) ) ) { // WARNING: arc MUST have both tail and head vertices assigned, otherwise crash!
        miss = true;
        break;
      }

      // Vertex conditions for head vertex
      if( VP ) {
        // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
        if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
          miss = true;
          break;
        }
        // Vertex probe has other/more constraints than exact ID (which already passed)
        else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
          // Update filter to use next head for deep traversal
          const vgx_ArcHead_t *prev = GAF->current_head;
          GAF->current_head = next;
          // Execute deep traversal
          //bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, larc, VP, next_RO, match );
          bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
          // Miss according to pos/neg match logic
          if( VP->vertexfilter_context->positive_match == !deep_match ) {
            miss = true;
          }
          // Restore previous head after deep traversal
          GAF->current_head = prev;
        }
      }
    } WHILE_ZERO;

    // Terminate according to pos/neg match logic
    if( GAF->positive_match == miss ) {
      if( !__is_arcfilter_error( *match ) ) {
        *match = VGX_ARC_FILTER_MATCH_MISS;
      }
      return 0;
    }

  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass


}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_pred_loceval_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK REQUIRED HEAD VERTEX
  // Do we require specific head and is this a match?
  if( __terminal_mismatch( GAF, larc->head.vertex, match ) ) {
    return 0;
  }
  
  // 2. CHECK THE ARC
  // We check single predicator here, terminating according to pos/neg match logic
  if( __arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // 3. EVALUATOR IS LOCAL
  vgx_Evaluator_t *evaluator = GAF->traversing_evaluator;
  if( iEvaluator.IsPositive( CALLABLE( evaluator )->EvalArc( evaluator, larc ) ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;

  // 4. RUN ADVANCED FILTERS
  // TODO AMBD-1938: If vertex_RO's inarcs are yielded, reclaim them here

  //
  // Special case: Exact ID match only and no filter evaluator. No more constraints, i.e. HIT since we checked
  //               the ID match already as the first constraint for this function.
  //
  if( VP->spec == _VERTEX_PROBE_ID_EXACT && VP->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }

  // Cull
  if( GAF->culleval ) {
    int ce = __generic_culleval( arcfilter_context, VP, larc, match );
    if( ce >= 0 ) {
      return ce;
    }
  }

  // At this point we may need to lock the head vertex
  const vgx_ArcHead_t *next = &larc->head;
  vgx_Vertex_t *next_RO = next->vertex;

  //
  // SECURE THE ARC HEAD AS NEEDED
  //
  if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
    *match = __arcfilter_error();
    return 0; // vertex not readable ==> miss
  }

  // Head safe when here - either locked or no locking required
  bool miss = false;
  // Vertex conditions for head vertex
  // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
  if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
    miss = true;
  }
  // Vertex probe has other/more constraints than exact ID (which already passed)
  else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
    // Update filter to use next head for deep traversal
    const vgx_ArcHead_t *prev = GAF->current_head;
    GAF->current_head = next;
    // Execute deep traversal
    bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
    // Miss according to pos/neg match logic
    if( VP->vertexfilter_context->positive_match == !deep_match ) {
      miss = true;
    }
    // Restore previous head after deep traversal
    GAF->current_head = prev;
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    if( !__is_arcfilter_error( *match ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
    }
    return 0;
  }


  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass


}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_loceval_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK REQUIRED HEAD VERTEX
  // Do we require specific head and is this a match?
  if( __terminal_mismatch( GAF, larc->head.vertex, match ) ) {
    return 0;
  }
  
  // 2. EVALUATOR IS LOCAL
  vgx_Evaluator_t *evaluator = GAF->traversing_evaluator;
  if( iEvaluator.IsPositive( CALLABLE( evaluator )->EvalArc( evaluator, larc ) ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;;

  // 3. RUN ADVANCED FILTERS
  // TODO AMBD-1938: If vertex_RO's inarcs are yielded, reclaim them here

  //
  // Special case: Exact ID match only and no filter evaluator. No more constraints, i.e. HIT since we checked
  //               the ID match already as the first constraint for this function.
  //
  if( VP->spec == _VERTEX_PROBE_ID_EXACT && VP->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }

  // Cull
  if( GAF->culleval ) {
    int ce = __generic_culleval( arcfilter_context, VP, larc, match );
    if( ce >= 0 ) {
      return ce;
    }
  }

  // At this point we may need to lock the head vertex
  const vgx_ArcHead_t *next = &larc->head;
  vgx_Vertex_t *next_RO = next->vertex;

  //
  // SECURE THE ARC HEAD AS NEEDED
  //
  if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
    *match = __arcfilter_error();
    return 0; // vertex not readable ==> miss
  }

  // Head safe when here - either locked or no locking required
  bool miss = false;
  // Vertex conditions for head vertex
  // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
  if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
    miss = true;
  }
  // Vertex probe has other/more constraints than exact ID (which already passed)
  else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
    // Update filter to use next head for deep traversal
    const vgx_ArcHead_t *prev = GAF->current_head;
    GAF->current_head = next;
    // Execute deep traversal
    bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
    // Miss according to pos/neg match logic
    if( VP->vertexfilter_context->positive_match == !deep_match ) {
      miss = true;
    }
    // Restore previous head after deep traversal
    GAF->current_head = prev;
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    if( !__is_arcfilter_error( *match ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
    }
    return 0;
  }


  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass


}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_pred_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK REQUIRED HEAD VERTEX
  // Do we require specific head and is this a match?
  if( __terminal_mismatch( GAF, larc->head.vertex, match ) ) {
    return 0;
  }
  
  // 2. CHECK THE ARC
  // We check single predicator here, terminating according to pos/neg match logic
  if( __arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;;

  // 3. RUN ADVANCED FILTERS

  //
  // Special case: Exact ID match only and no filter evaluator. No more constraints, i.e. HIT since we checked
  //               the ID match already as the first constraint for this function.
  //
  if( VP->spec == _VERTEX_PROBE_ID_EXACT && VP->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }

  // Cull
  if( GAF->culleval ) {
    int ce = __generic_culleval( arcfilter_context, VP, larc, match );
    if( ce >= 0 ) {
      return ce;
    }
  }

  // At this point we may need to lock the head vertex
  const vgx_ArcHead_t *next = &larc->head;
  vgx_Vertex_t *next_RO = next->vertex;

  //
  // SECURE THE ARC HEAD AS NEEDED
  //
  if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
    *match = __arcfilter_error();
    return 0; // vertex not readable ==> miss
  }

  // Head safe when here - either locked or no locking required
  bool miss = false;
  // Vertex conditions for head vertex
  // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
  if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
    miss = true;
  }
  // Vertex probe has other/more constraints than exact ID (which already passed)
  else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
    // Update filter to use next head for deep traversal
    const vgx_ArcHead_t *prev = GAF->current_head;
    GAF->current_head = next;
    // Execute deep traversal
    bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
    // Miss according to pos/neg match logic
    if( VP->vertexfilter_context->positive_match == !deep_match ) {
      miss = true;
    }
    // Restore previous head after deep traversal
    GAF->current_head = prev;
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    if( !__is_arcfilter_error( *match ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
    }
    return 0;
  }


  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass


}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_vertex_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK REQUIRED HEAD VERTEX
  // Do we require specific head and is this a match?
  if( __terminal_mismatch( GAF, larc->head.vertex, match ) ) {
    return 0;
  }

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;;

  // 2. RUN ADVANCED FILTERS

  //
  // Special case: Exact ID match only and no filter evaluator. No more constraints, i.e. HIT since we checked
  //               the ID match already as the first constraint for this function.
  //
  if( VP->spec == _VERTEX_PROBE_ID_EXACT && VP->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }

  // Cull
  if( GAF->culleval ) {
    int ce = __generic_culleval( arcfilter_context, VP, larc, match );
    if( ce >= 0 ) {
      return ce;
    }
  }

  // At this point we may need to lock the head vertex
  const vgx_ArcHead_t *next = &larc->head;
  vgx_Vertex_t *next_RO = next->vertex;

  //
  // SECURE THE ARC HEAD AS NEEDED
  //
  if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
    *match = __arcfilter_error();
    return 0; // vertex not readable ==> miss
  }

  // Head safe when here - either locked or no locking required
  bool miss = false;
  // Vertex conditions for head vertex
  // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
  if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
    miss = true;
  }
  // Vertex probe has other/more constraints than exact ID (which already passed)
  else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
    // Update filter to use next head for deep traversal
    const vgx_ArcHead_t *prev = GAF->current_head;
    GAF->current_head = next;
    // Execute deep traversal
    bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
    // Miss according to pos/neg match logic
    if( VP->vertexfilter_context->positive_match == !deep_match ) {
      miss = true;
    }
    // Restore previous head after deep traversal
    GAF->current_head = prev;
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    if( !__is_arcfilter_error( *match ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
    }
    return 0;
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass


}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __direct_recursion_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  const vgx_vertex_probe_t *VP = GAF->vertex_probe;;

  // At this point we may need to lock the head vertex
  const vgx_ArcHead_t *next = &larc->head;
  vgx_Vertex_t *next_RO = next->vertex;

  //
  // SECURE THE ARC HEAD AS NEEDED
  //
  if( __filter_acquire_lockable_arc_head( arcfilter_context, larc ) < 0 ) {
    *match = __arcfilter_error();
    return 0; // vertex not readable ==> miss
  }

  // Head safe when here - either locked or no locking required
  bool miss = false;
  // Vertex conditions for head vertex
  // If matching specific manifestation (REAL or VIRTUAL), is head vertex of correct type?
  if( (VP->manifestation & next_RO->descriptor.state.context.man) == 0 ) {
    miss = true;
  }
  // Vertex probe has other/more constraints than exact ID (which already passed)
  else if( VP->spec != _VERTEX_PROBE_ID_EXACT ) {
    // Update filter to use next head for deep traversal
    const vgx_ArcHead_t *prev = GAF->current_head;
    GAF->current_head = next;
    // Execute deep traversal
    bool deep_match = __arcvector_vertex_condition_match_generic( (vgx_virtual_ArcFilter_context_t*)GAF, VP, next_RO, match );
    // Miss according to pos/neg match logic
    if( VP->vertexfilter_context->positive_match == !deep_match ) {
      miss = true;
    }
    // Restore previous head after deep traversal
    GAF->current_head = prev;
  }

  // Terminate according to pos/neg match logic
  if( GAF->positive_match == miss ) {
    if( !__is_arcfilter_error( *match ) ) {
      *match = VGX_ARC_FILTER_MATCH_MISS;
    }
    return 0;
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_pred_loceval_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK THE ARC
  // We check single predicator here, terminating according to pos/neg match logic
  if( __arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // 2. EVALUATOR IS LOCAL
  vgx_Evaluator_t *evaluator = GAF->traversing_evaluator;
  if( iEvaluator.IsPositive( CALLABLE( evaluator )->EvalArc( evaluator, larc ) ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_loceval_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // EVALUATOR IS LOCAL
  vgx_Evaluator_t *evaluator = GAF->traversing_evaluator;
  if( iEvaluator.IsPositive( CALLABLE( evaluator )->EvalArc( evaluator, larc ) ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int __generic_pred_arcfilter( vgx_virtual_ArcFilter_context_t *arcfilter_context, vgx_LockableArc_t *larc, vgx_ArcFilter_match *match ) {

  vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;

  // 1. CHECK THE ARC
  // We check single predicator here, terminating according to pos/neg match logic
  if( __arcvector_predicator_match_generic( GAF->pred_condition1, larc->head.predicator ) != GAF->positive_match ) {
    *match = VGX_ARC_FILTER_MATCH_MISS;
    return 0;
  }

  // PASS!
  *match = VGX_ARC_FILTER_MATCH_HIT;
  return 1; // pass
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __pass_vertexfilter( vgx_VertexFilter_context_t *vertexfilter_context, vgx_Vertex_t *vertex, vgx_ArcFilter_match *match ) {
  return true;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __evaluator_vertexfilter( vgx_VertexFilter_context_t *vertexfilter_context, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
  vgx_vertex_probe_t *VP = ((vgx_GenericVertexFilter_context_t*)vertexfilter_context)->vertex_probe;

  // Match the vertex condition
  int m = __arcvector_vertex_condition_match_recursive( NULL, VP, vertex_RO, match );
  // Return "hit" according to pos/neg matching logic
  if( !__is_arcfilter_error( *match ) ) {
    if( vertexfilter_context->positive_match == m ) {
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __generic_vertexfilter( vgx_VertexFilter_context_t *vertexfilter_context, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match ) {
  vgx_vertex_probe_t *VP = ((vgx_GenericVertexFilter_context_t*)vertexfilter_context)->vertex_probe;

  // Match the vertex condition
  int m = (VP->manifestation & vertex_RO->descriptor.state.context.man )
           &&
          __arcvector_vertex_condition_match_generic( NULL, VP, vertex_RO, match );
  // Return "hit" according to pos/neg matching logic
  if( !__is_arcfilter_error( *match ) ) {
    if( vertexfilter_context->positive_match == m ) {
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __configure_predicators_from_arc_condition_set( vgx_Graph_t *self, const vgx_ArcConditionSet_t *arc_condition_set, vgx_predicator_t *predicator1, vgx_predicator_t *predicator2 ) {

  // Initialize to wildcard
  predicator1->data = VGX_PREDICATOR_NONE_BITS;
  predicator2->data = VGX_PREDICATOR_NONE_BITS;

  if( arc_condition_set ) {
  
    predicator1->rel.dir = arc_condition_set->arcdir;
    predicator2->rel.dir = arc_condition_set->arcdir;

    // TODO!
    // The internal probe only has room for a single arc condition at this time. Although we provide the arc condition set
    // on the higher levels we can only use the first entry in the set. This will be expanded in the future to allow
    // multiple arc condition within the inner probe.

    if( arc_condition_set->set ) {
      vgx_ArcCondition_t **cursor = arc_condition_set->set;
      vgx_ArcCondition_t *condition;
      while( (condition = *cursor++) != NULL ) {
        vgx_value_comparison vcomp = condition->vcomp;
        vgx_predicator_rel_t rel = {
          .dir = arc_condition_set->arcdir,
          .enc = VGX_PREDICATOR_REL_WILDCARD  // Default relationship=*
        };
        vgx_predicator_mod_t mod = condition->modifier;
        
        // RELATIONSHIP
        // Encode the relationship type if relationship condition is specified
        if( condition->CSTR__relationship && self ) {
          const char *rel_str = CStringValue( condition->CSTR__relationship );
          // Enumeration supplied directly
          if( *rel_str == '#' ) {
            int64_t rn;
            if( strtoint64( rel_str+1, &rn ) < 0 || rn < 0 || rn > VGX_PREDICATOR_REL_MAX || !__relationship_enumeration_valid( rel.enc = (vgx_predicator_rel_enum)rn ) ) {
              __format_error_string( &((vgx_ArcConditionSet_t*)arc_condition_set)->CSTR__error, "invalid relationship: '%s'", rel_str );
              return -1;
            }
          }
          // Translate from string to enumeration
          else if( !__relationship_enumeration_valid( (rel.enc = (uint16_t)iEnumerator_OPEN.Relationship.GetEnum( self, condition->CSTR__relationship )) ) ) {
            // Error
            if( rel.enc == VGX_PREDICATOR_REL_INVALID ) {
              __format_error_string( &((vgx_ArcConditionSet_t*)arc_condition_set)->CSTR__error, "invalid relationship: '%s'", rel_str );
            }
            else if( rel.enc == VGX_PREDICATOR_REL_NO_MAPPING ) {
              __set_error_string( &((vgx_ArcConditionSet_t*)arc_condition_set)->CSTR__error, "relationship typespace exhausted" );
            }
            else {
              __set_error_string( &((vgx_ArcConditionSet_t*)arc_condition_set)->CSTR__error, "relationship encoding error" );
            }
            return -1;
          }
        }
        
        // Basic value comparison is always single value condition (value2 is not used)
        if( _vgx_is_basic_value_comparison( vcomp ) ) {
          predicator1->val = condition->value1;
          predicator1->rel = rel;
          predicator1->mod = mod;
          _vgx_set_predicator_mod_condition_from_value_comparison( &predicator1->mod, vcomp );
          if( mod.probe.type == VGX_PREDICATOR_MOD_LSH ) {
            _vgx_predicator_eph_set_lsh_and_distance( predicator1, condition->value2.integer );
          }
          else { 
            predicator1->eph = VGX_PREDICATOR_EPH_NONE;
          }
        }
        // Extended value comparison may use both values and/or depend dynamically on previous neighborhood arc values
        else if( _vgx_is_extended_value_comparison( vcomp ) ) {
          // value RANGE (basic or dynamic)
          if( _vgx_is_value_range_comparison( vcomp ) ) {
            // Predicator 1
            predicator1->rel = rel;
            predicator1->mod = mod;
            // [ x, ...         endpoint included
            switch( vcomp ) {
            // DYN DELTA
            case VGX_VALUE_DYN_RANGE:
              _vgx_predicator_eph_set_dyndelta( predicator1, VGX_VALUE_DYN_GTE, condition->value1 );
              break;
            // DYN RATIO
            case VGX_VALUE_DYN_RANGE_R:
              _vgx_predicator_eph_set_dynratio( predicator1, VGX_VALUE_DYN_GTE, condition->value1 );
              break;
            // RANGE or NRANGE
            default:
              {
                // vcomp depends on whether RANGE or NRANGE
                vgx_value_comparison lower_vcomp = vcomp & VGX_VALUE_NEG ? VGX_VALUE_LT : VGX_VALUE_GTE;
                _vgx_set_predicator_mod_condition_from_value_comparison( &predicator1->mod, lower_vcomp );
                predicator1->val = condition->value1;
                predicator1->eph = VGX_PREDICATOR_EPH_NONE;
              }
            }

            // Predicator 2
            predicator2->rel = rel;
            predicator2->mod = mod;
            //      ..., y ]    endpoint included
            switch( vcomp ) {
            // DYN DELTA
            case VGX_VALUE_DYN_RANGE:
              _vgx_predicator_eph_set_dyndelta( predicator2, VGX_VALUE_DYN_LTE, condition->value2 );
              break;
            // DYN RATIO
            case VGX_VALUE_DYN_RANGE_R:
              _vgx_predicator_eph_set_dynratio( predicator2, VGX_VALUE_DYN_LTE, condition->value2 );
              break;
            // RANGE or NRANGE
            default:
              {
                vgx_value_comparison upper_vcomp = vcomp & VGX_VALUE_NEG ? VGX_VALUE_GT : VGX_VALUE_LTE;
                _vgx_set_predicator_mod_condition_from_value_comparison( &predicator2->mod, upper_vcomp );
                predicator2->val = condition->value2;
                predicator2->eph = VGX_PREDICATOR_EPH_NONE;
              }
            }
          }
          // single-ended DYNAMIC
          else if( _vgx_is_dynamic_value_comparison( vcomp ) ) {
            predicator1->rel = rel;
            predicator1->mod = mod;
            _vgx_predicator_eph_set_dyndelta( predicator1, vcomp, condition->value1 );
          }

        }

        // Arc condition is negative
        if( condition->positive == false ) {
          _vgx_predicator_eph_invert( predicator1 ); // Flag the inversion in predicator1
        }

        // !!!
        // FOR NOW: We have a limitation that only a single condition is allowed
        // See TODO above.
        // !!!
        if( *cursor != NULL ) {
          WARN( 0x600, "Multiple arc conditions not yet supported!" );
          break;
        }
      }
    }
  }
  // No condition, set direction to any
  else {
    predicator1->rel.dir = VGX_ARCDIR_ANY;
    predicator2->rel.dir = VGX_ARCDIR_ANY;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_arc_filter( vgx_Graph_t *self, bool readonly_graph, const vgx_ArcConditionSet_t *arc_condition_set, const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *traversing_evaluator, vgx_ExecutionTimingBudget_t *timing_budget ) {
  
  vgx_virtual_ArcFilter_context_t *filter_context = NULL;

  vgx_predicator_t pred1 = VGX_PREDICATOR_NONE;
  vgx_predicator_t pred2 = VGX_PREDICATOR_NONE;

  if( __configure_predicators_from_arc_condition_set( self, arc_condition_set, &pred1, &pred2 ) < 0 ) {
    return NULL;
  }

  // No vertex probe or advanced filter, we may be able to pick a faster predicator matching function
  if( vertex_probe == NULL
      || 
      ( _vgx_vertex_condition_full_wildcard( vertex_probe->spec, vertex_probe->manifestation ) 
        &&
        vertex_probe->vertexfilter_context
        &&
        vertex_probe->vertexfilter_context->positive_match
      ) 
    ) 
  {
    // Optimize only for single predicator (except D_BOTH) and non-dynamic value tests.
    // Value ranges and dynamic value tests fall back to generic match.
    if( _vgx_predicator_full_wildcard( pred2 ) ) {
      if( traversing_evaluator == NULL && !_vgx_predicator_eph_is_dynamic( pred1 ) && !_vgx_predicator_is_arcdir_both( pred1 ) ) {
        // RELATIONSHIP
        if( _vgx_predicator_has_rel( pred1 ) ) {
          // MODIFER
          if( _vgx_predicator_has_mod( pred1 ) ) {
            // VALUE
            if( _vgx_predicator_has_val( pred1 ) ) {
              // REL,MOD,VAL
              filter_context = __new_specific_value_arc_filter( pred1 );
            }
            // VAL=*
            else {
              // REL,MOD,*
              filter_context = __new_specific_arc_filter( pred1 );
            }
          }
          // MOD=*
          else {
            // VALUE
            if( _vgx_predicator_has_val( pred1 ) ) {
              // REL,*,VAL
              filter_context = __new_relationship_value_arc_filter( pred1 );
            }
            // VAL=*
            else {
              // REL,*,*
              filter_context = __new_relationship_arc_filter( pred1 );
            }
          }
        }
        // REL=*
        else {
          // MODIFER
          if( _vgx_predicator_has_mod( pred1 ) ) {
            // VALUE
            if( _vgx_predicator_has_val( pred1 ) ) {
              // *,MOD,VAL
              filter_context = __new_modifier_value_arc_filter( pred1 );
            }
            // VAL=*
            else {
              // *,MOD,*
              filter_context = __new_modifier_arc_filter( pred1 );
            }
          }
          // MOD=*
          else {
            // VALUE
            if( _vgx_predicator_has_val( pred1 ) ) {
              // *,*,VAL
              filter_context = __new_value_arc_filter( pred1 );
            }
            // VAL=*
            else {
              // *,*,*
              filter_context = __new_wildcard_arc_filter( _vgx_predicator_eph_is_positive( pred1 ) );
            }
          }
        }
      }
      // At this point: No vertex probe, no predicator filter, only an evaluator instance
      else if( _vgx_predicator_full_wildcard( pred1 ) && traversing_evaluator != NULL ) {
        bool positive = _vgx_predicator_eph_is_positive( pred1 );
        filter_context = __new_evaluator_arc_filter( readonly_graph, positive, traversing_evaluator );
      }
    }
  }

  // Fallback to a generic filter if no optimized filter was selected above
  if( filter_context == NULL ) {
    filter_context = __new_generic_arc_filter( self, readonly_graph, pred1, pred2, vertex_probe, traversing_evaluator, timing_budget );
  }
  // Assign timing budget to simple filter
  else {
    filter_context->timing_budget = timing_budget;
  }

  if( filter_context ) {
    // Set the final positive match.
    // True if both are True
    // True if both are False (double negation)
    // False if one is True and one is False
    bool accept = arc_condition_set ? arc_condition_set->accept : true;
    if( (filter_context->positive_match = !(filter_context->positive_match ^ accept)) == false ) {
      // Overall sign is negative (reject mode).
      // If our filter type was set to all PASS earlier we need to flip it
      // to reject mode.
      if( filter_context->type == VGX_ARC_FILTER_TYPE_PASS ) {
        filter_context->type = VGX_ARC_FILTER_TYPE_STOP;
        filter_context->positive_match = true;
        filter_context->filter = arcfilterfunc.Stop;
      }
    }

    if( filter_context->filter == NULL ) {
      FATAL( 0x001, "NULL!" );
    }
  }

  return filter_context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexFilter_context_t * __new_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget ) {

  vgx_VertexFilter_context_t *vertexfilter_context = NULL;

  // Full WILDCARD: the vertex condition is unspecified: NO FILTER
  if( vertex_probe == NULL || _vgx_vertex_condition_full_wildcard( vertex_probe->spec, vertex_probe->manifestation ) ) {
    if( (vertexfilter_context = (vgx_VertexFilter_context_t*)calloc( 1, sizeof( vgx_VertexFilter_context_t ) )) != NULL ) {
      vertexfilter_context->type = VGX_VERTEX_FILTER_TYPE_PASS;
      vertexfilter_context->positive_match = true;
      vertexfilter_context->local_evaluator.pre = NULL;
      vertexfilter_context->local_evaluator.main = NULL;
      vertexfilter_context->local_evaluator.post = NULL;
      vertexfilter_context->current_thread = 0;
      vertexfilter_context->filter = vertexfilterfunc.Pass;
      vertexfilter_context->timing_budget = timing_budget;
    }
  }
  else {
    // Only advanced filter without degree, property, timestamp or similarity.
    // I.e. possibly an evaluator and/or recursive traversal
    if( ((vertex_probe->spec & _VERTEX_PROBE_ANY_ENA) == _VERTEX_PROBE_ADVANCED_ENA)
        &&
        vertex_probe->advanced.degree_probe == NULL
        &&
        vertex_probe->advanced.property_probe == NULL
        &&
        vertex_probe->advanced.timestamp_probe == NULL
        &&
        vertex_probe->advanced.similarity_probe == NULL
        &&
        vertex_probe->manifestation == VERTEX_STATE_CONTEXT_MAN_ANY
      )
    {
      vertexfilter_context = __new_evaluator_vertex_filter( vertex_probe, timing_budget );
    }
    // Generic filter required
    else {
      vertexfilter_context = __new_generic_vertex_filter( vertex_probe, timing_budget );
    }
  }

  return vertexfilter_context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_wildcard_arc_filter( bool pass ) {
  vgx_GenericArcFilter_context_t *filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( filter != NULL ) {
    if( pass ) {
      filter->type = VGX_ARC_FILTER_TYPE_PASS;
      filter->positive_match = true;
      filter->filter = arcfilterfunc.Pass;
    }
    else {
      filter->type = VGX_ARC_FILTER_TYPE_STOP;
      filter->positive_match = true;
      filter->filter = arcfilterfunc.Stop;
    }
    filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)filter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_relationship_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *rel_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( rel_filter ) {
    // Base
    rel_filter->type = VGX_ARC_FILTER_TYPE_RELATIONSHIP;
    rel_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    rel_filter->filter = arcfilterfunc.RelationshipFilter;
    // Generic
    rel_filter->pred_condition1 = predicator;
    rel_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)rel_filter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_modifier_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *mod_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( mod_filter ) {
    // Base
    mod_filter->type = VGX_ARC_FILTER_TYPE_MODIFIER;
    mod_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    mod_filter->filter = arcfilterfunc.ModifierFilter;
    // Generic
    mod_filter->pred_condition1 = predicator;
    mod_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)mod_filter;
}



/*******************************************************************//**
 *
 * NOTE: No value ranges
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_value_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *val_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( val_filter ) {
    // Base
    val_filter->type = VGX_ARC_FILTER_TYPE_VALUE;
    val_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    if( _vgx_predicator_eph_is_lsh( predicator ) ) {
      val_filter->filter = arcfilterfunc.HamDistFilter;
    }
    else {
      val_filter->filter = arcfilterfunc.ValueFilter;
    }
    // Generic
    val_filter->pred_condition1 = predicator;
    val_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)val_filter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_specific_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *relmod_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( relmod_filter ) {
    // Base
    relmod_filter->type = VGX_ARC_FILTER_TYPE_SPECIFIC;
    relmod_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    relmod_filter->filter = arcfilterfunc.SpecificFilter;
    // Generic
    relmod_filter->pred_condition1 = predicator;
    relmod_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)relmod_filter;
}



/*******************************************************************//**
 *
 * NOTE: No value ranges
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_relationship_value_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *relval_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( relval_filter ) {
    // Base
    relval_filter->type = VGX_ARC_FILTER_TYPE_RELATIONSHIP_VALUE;
    relval_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    if( _vgx_predicator_eph_is_lsh( predicator ) ) {
      relval_filter->filter = arcfilterfunc.RelationshipHamDistFilter;
    }
    else {
      relval_filter->filter = arcfilterfunc.RelationshipValueFilter;
    }
    // Generic
    relval_filter->pred_condition1 = predicator;
    relval_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)relval_filter;
}



/*******************************************************************//**
 *
 * NOTE: No value ranges
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_modifier_value_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *modval_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( modval_filter ) {
    // Base
    modval_filter->type = VGX_ARC_FILTER_TYPE_MODIFIER_VALUE;
    modval_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    if( _vgx_predicator_eph_is_lsh( predicator ) ) {
      modval_filter->filter = arcfilterfunc.ModifierHamDistFilter;
    }
    else {
      modval_filter->filter = arcfilterfunc.ModifierValueFilter;
    }
    // Generic
    modval_filter->pred_condition1 = predicator;
    modval_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)modval_filter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_specific_value_arc_filter( const vgx_predicator_t predicator ) {
  vgx_GenericArcFilter_context_t *relmodval_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( relmodval_filter ) {
    // Base
    relmodval_filter->type = VGX_ARC_FILTER_TYPE_SPECIFIC_VALUE;
    relmodval_filter->positive_match = _vgx_predicator_eph_is_positive( predicator ); // extract filter's accept/reject sign from predicator's eph.neg flag
    if( _vgx_predicator_eph_is_lsh( predicator ) ) {
      relmodval_filter->filter = arcfilterfunc.SpecificHamDistFilter;
    }
    else {
      relmodval_filter->filter = arcfilterfunc.SpecificValueFilter;
    }
    // Generic
    relmodval_filter->pred_condition1 = predicator;
    relmodval_filter->logic = VGX_LOGICAL_NO_LOGIC;
  }
  return (vgx_virtual_ArcFilter_context_t*)relmodval_filter;
}



/*******************************************************************//**
 *
 * Hackish and temporary way to infer the boolean logic between two
 * predicators in the same arc condition needed to perform necessary matching.
 ***********************************************************************
 */
static vgx_boolean_logic __logic_from_predicators( const vgx_predicator_t predicator1, vgx_predicator_t const predicator2 ) {
  vgx_boolean_logic logic;

  // No second predicator, single predicator only
  if( predicator2.data == VGX_PREDICATOR_NONE_BITS ) {
    logic = VGX_LOGICAL_NO_LOGIC;
  }
  // We have two predicators, how to combined them?
  else {
    // Inverted range, infer OR-logic from this condition
    if( _vgx_predicator_cmp_is_LT( predicator1 ) && _vgx_predicator_cmp_is_GT( predicator2 ) ) {
      logic = VGX_LOGICAL_OR;
    }
    // All other situations require both predicators to be true, i.e. AND
    else {
      logic = VGX_LOGICAL_AND;
    }
  }

  return logic;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vertex_probe_head_access( const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *filter_evaluator ) {
  // Head vertex must be locked during filter evaluation if the vertex will be dereferenced

  // Vertex probe exists
  if( vertex_probe && (vertex_probe->spec & _VERTEX_PROBE_ANY_ENA) != 0 && vertex_probe->spec != _VERTEX_PROBE_ID_EXACT ) {
    return true;
  }
  // Filter evaluator with head dereference exists
  else if( filter_evaluator && CALLABLE( filter_evaluator )->HeadDeref( filter_evaluator ) > 0 ) {
    return true;
  }
  // No lock required
  else {
    return false;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_arc_filter( vgx_virtual_ArcFilter_context_t **arcfilter ) {
  if( arcfilter && *arcfilter ) {
    if( _vgx_is_arcfilter_type_generic( (*arcfilter)->type ) ) {
      vgx_GenericArcFilter_context_t *generic_arcfilter = (vgx_GenericArcFilter_context_t*)(*arcfilter);
      free( (vgx_Vertex_t**)generic_arcfilter->terminal.list );
    }

    free( *arcfilter );
    *arcfilter = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_evaluator_arc_filter( bool readonly_graph, bool positive, vgx_Evaluator_t *traversing_evaluator ) {
  vgx_virtual_ArcFilter_context_t *arcfilter = NULL;
  vgx_GenericArcFilter_context_t *evaluator_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( evaluator_filter ) {
    arcfilter = (vgx_virtual_ArcFilter_context_t*)evaluator_filter;
    // Base
    arcfilter->type = VGX_ARC_FILTER_TYPE_EVALUATOR;
    arcfilter->positive_match = positive;
    arcfilter->arcfilter_locked_head_access = false; // Maybe set later
    arcfilter->eval_synarc = false; // Maybe set later
    arcfilter->traversing_evaluator = traversing_evaluator; // BORROW!
    arcfilter->filter = arcfilterfunc.EvaluatorFilter;

    // Synthetic arc eval?
    if( CALLABLE( traversing_evaluator )->SynArcOps( traversing_evaluator ) > 0 ) {
      arcfilter->eval_synarc = true;
    }

    // Head vertex must be locked whenever dereferenced, unless graph is
    // readonly and then we acquire another graph readonly lock while lasts
    // until the filter is destroyed.
    if( readonly_graph == false ) {
      if( __vertex_probe_head_access( NULL, traversing_evaluator ) ) {
        arcfilter->arcfilter_locked_head_access = true;
      }
    }
  }
  return arcfilter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __new_generic_arc_filter( vgx_Graph_t *self, bool readonly_graph, const vgx_predicator_t predicator1, const vgx_predicator_t predicator2, const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *traversing_evaluator, vgx_ExecutionTimingBudget_t *timing_budget ) {
  vgx_virtual_ArcFilter_context_t *arcfilter = NULL;
  vgx_GenericArcFilter_context_t *generic_filter = (vgx_GenericArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) );
  if( generic_filter ) {

    // Single-ended, non-dynamic predicator
    bool single_ended_static_pred = (_vgx_predicator_full_wildcard( predicator2 ) && !_vgx_predicator_eph_is_dynamic( predicator1 ) && !_vgx_predicator_is_arcdir_both( predicator1 ));
    // Dynamic
    bool dynamic_pred = (_vgx_predicator_eph_is_dynamic( predicator1 ) || _vgx_predicator_eph_is_dynamic( predicator2 ));


    arcfilter = (vgx_virtual_ArcFilter_context_t*)generic_filter;
    // Base
    arcfilter->type = VGX_ARC_FILTER_TYPE_GEN_NONE; 
    arcfilter->positive_match = _vgx_predicator_eph_is_positive( predicator1 ); // NOTE: predicator1's eph.neg dictates the filter's sign. Predicator2's eph.neg IGNORED.
    arcfilter->arcfilter_locked_head_access = false; // Maybe set later
    arcfilter->eval_synarc = false; // Maybe set later
    arcfilter->filter = NULL;
    arcfilter->timing_budget = timing_budget;


    generic_filter->pred_condition1 = predicator1;

    // Single static predicator
    if( single_ended_static_pred ) {
      generic_filter->logic = VGX_LOGICAL_NO_LOGIC;
      generic_filter->pred_condition2 = VGX_PREDICATOR_NONE;
      if( !_vgx_predicator_full_wildcard( predicator1 ) ) {
        arcfilter->type |= __VGX_ARC_FILTER_TYPE_MASK_GEN_PRED;
      }
    }
    else {
      generic_filter->logic = __logic_from_predicators( predicator1, predicator2 );
      generic_filter->pred_condition2 = predicator2;
      arcfilter->type = VGX_ARC_FILTER_TYPE_GENERIC; 
    }

    // Dynamic predicator
    if( dynamic_pred ) {
      generic_filter->arcfilter_callback = __dynamic_arc_filter_callback;
      arcfilter->type = VGX_ARC_FILTER_TYPE_GENERIC; 
    }

    // Evaluator
    if( traversing_evaluator ) {
      arcfilter->traversing_evaluator = traversing_evaluator; // BORROW!
      // Head deref?
      if( CALLABLE( traversing_evaluator )->HeadDeref( traversing_evaluator ) == 0 ) {
        arcfilter->type |= __VGX_ARC_FILTER_TYPE_MASK_GEN_LOCEVAL;
      }
      else {
        arcfilter->type |= __VGX_ARC_FILTER_TYPE_MASK_GEN_TRAVEVAL;
      }
      // Synthetic arc eval?
      if( CALLABLE( traversing_evaluator )->SynArcOps( traversing_evaluator ) > 0 ) {
        arcfilter->eval_synarc = true;
      }
    }

    // Vertex probe exists
    if( vertex_probe ) {
      // Exact ID match required
      //
      // TODO: SUPPORT MULTIPLE TERMINALS WITH VARIOUS LOGIC
      //
      //
      if( _vgx_vertex_condition_has_obid_match( vertex_probe->spec )
          &&
          vertex_probe->vertexfilter_context->positive_match )
      {
        const vgx_StringList_t *CSTR__idlist = vertex_probe->CSTR__idlist;
        int64_t sz = iString.List.Size( CSTR__idlist );
        // Multiple vertex id conditions
        if( sz > 1 ) {
          // Set filter
          generic_filter->terminal.current = NULL;
          if( (generic_filter->terminal.list = calloc( sz+1, sizeof( vgx_Vertex_t* ) )) == NULL ) {
            // Memory error
            __delete_arc_filter( &arcfilter );
            return NULL;
          }
          generic_filter->terminal.logic = VGX_LOGICAL_OR; // TODO!!! How do we specify other logic ??
          // Populate filter with vertex addresses (OR LOGIC)
          const vgx_Vertex_t **dest = generic_filter->terminal.list;
          GRAPH_LOCK( self ) {
            for( int64_t i=0; i<sz; i++ ) {
              const CString_t *CSTR__id = iString.List.GetItem( (vgx_StringList_t*)CSTR__idlist, i );
              vgx_Vertex_t *vertex = _vxgraph_vxtable__query_CS( self, CSTR__id, NULL, VERTEX_TYPE_ENUMERATION_WILDCARD ); // TODO: switch to type specific table for speed
              if( vertex ) {
                *dest++ = vertex;
              }
            }
          } GRAPH_RELEASE;
          // At least one non-null vertex required
          if( generic_filter->terminal.list[0] == NULL ) {
            free( (vgx_Vertex_t**)generic_filter->terminal.list );
            generic_filter->terminal.logic = VGX_LOGICAL_NO_LOGIC;
            generic_filter->terminal.list = NULL;
            generic_filter->type = VGX_ARC_FILTER_TYPE_STOP;
            generic_filter->positive_match = true;
            generic_filter->filter = arcfilterfunc.Stop;
          }
        }
        // String represents vertex identifier
        else if( sz == 1 ) {
          const CString_t *CSTR__id = iString.List.GetItem( (vgx_StringList_t*)CSTR__idlist, 0 );
          vgx_Vertex_t *vertex = _vxgraph_vxtable__query_OPEN( self, CSTR__id, NULL, VERTEX_TYPE_ENUMERATION_WILDCARD ); // TODO: switch to type specific table for speed
          if( vertex ) {
            // NOTE: NOT ACQUIRED! Whoever executes the probe is responsible for these things
            generic_filter->terminal.current = vertex;
            generic_filter->terminal.list = NULL;
            generic_filter->terminal.logic = VGX_LOGICAL_AND;
          }
          // Required head does not exist
          else {
            generic_filter->type = VGX_ARC_FILTER_TYPE_STOP;
            generic_filter->positive_match = true;
            generic_filter->filter = arcfilterfunc.Stop;
          }
        }
      }

      generic_filter->vertex_probe = vertex_probe;

      vgx_Evaluator_t *E = vertex_probe->advanced.local_evaluator.filter;
      if( E && CALLABLE(E)->HasCull(E) ) {
        generic_filter->culleval = E; // borrow the reference for easier access later
        if( CALLABLE(E)->ThisNextAccess(E) > 0 ) {
          generic_filter->locked_cull = true;
        }
      }

      arcfilter->type |= __VGX_ARC_FILTER_TYPE_MASK_GEN_VERTEX;
    }

    // Head vertex must be locked whenever dereferenced, unless graph is
    // readonly and then we acquire another graph readonly lock while lasts
    // until the filter is destroyed.
    if( readonly_graph == false ) {
      if( __vertex_probe_head_access( vertex_probe, traversing_evaluator ) ) {
        arcfilter->arcfilter_locked_head_access = true;
      }
    }


    // Select filter function
    switch( arcfilter->type ) {
    case VGX_ARC_FILTER_TYPE_GENERIC:
    case VGX_ARC_FILTER_TYPE_GEN_PRED_TRAVEVAL_VERTEX:
    case VGX_ARC_FILTER_TYPE_GEN_TRAVEVAL_VERTEX:
    case VGX_ARC_FILTER_TYPE_GEN_PRED_TRAVEVAL:
    case VGX_ARC_FILTER_TYPE_GEN_TRAVEVAL:
      arcfilter->type = VGX_ARC_FILTER_TYPE_GENERIC;
      arcfilter->filter = arcfilterfunc.GenericArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_PRED_LOCEVAL_VERTEX:
      arcfilter->filter = arcfilterfunc.GenPredLocEvalVertexArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_LOCEVAL_VERTEX:
      arcfilter->filter = arcfilterfunc.GenLocEvalVertexArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_PRED_VERTEX:
      arcfilter->filter = arcfilterfunc.GenPredVertexArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_VERTEX:
      arcfilter->filter = arcfilterfunc.GenVertexArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_PRED_LOCEVAL:
      arcfilter->filter = arcfilterfunc.GenPredLocEvalArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_LOCEVAL:
      arcfilter->filter = arcfilterfunc.GenLocEvalArcFilter;
      break;
    case VGX_ARC_FILTER_TYPE_GEN_PRED:
      arcfilter->filter = arcfilterfunc.GenPredArcFilter; // ? should never happen
      break;
    case VGX_ARC_FILTER_TYPE_GEN_NONE:
      arcfilter->filter = arcfilterfunc.Pass;
      break;
    default:
      if( arcfilter->filter == NULL ) {
        arcfilter->type = VGX_ARC_FILTER_TYPE_GENERIC;
        arcfilter->filter = arcfilterfunc.GenericArcFilter;
      }
    }

  }
  return arcfilter;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_virtual_ArcFilter_context_t * __clone_arc_filter( const vgx_virtual_ArcFilter_context_t *other ) {
  vgx_virtual_ArcFilter_context_t *context = NULL;

  if( ( context = (vgx_virtual_ArcFilter_context_t*)calloc( 1, sizeof( vgx_GenericArcFilter_context_t ) ) ) != NULL ) {
    memcpy( context, other, sizeof( vgx_GenericArcFilter_context_t ) );
  }

  return context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexFilter_context_t * __clone_vertex_filter( const vgx_VertexFilter_context_t *other ) {
  vgx_VertexFilter_context_t *context = NULL;
  vgx_VertexFilter_type t = other->type;
  switch( t ) {
  case VGX_VERTEX_FILTER_TYPE_PASS:
    context = (vgx_VertexFilter_context_t*)calloc( 1, sizeof( vgx_VertexFilter_context_t ) );
    t = VGX_VERTEX_FILTER_TYPE_PASS;
    break;
  case VGX_VERTEX_FILTER_TYPE_EVALUATOR:
  case VGX_VERTEX_FILTER_TYPE_GENERIC:
    context = (vgx_VertexFilter_context_t*)calloc( 1, sizeof( vgx_GenericVertexFilter_context_t ) );
    break;
  default:
    return NULL;
  }

  if( context ) {
    context->type = t;
    // Clone the basic attributes
    context->positive_match = other->positive_match;
    context->local_evaluator.pre = other->local_evaluator.pre;
    context->local_evaluator.main = other->local_evaluator.main;
    context->local_evaluator.post = other->local_evaluator.post;
    context->current_thread = 0;
    context->filter = other->filter;
    // Clone generic filter attributes
    if( context->type != VGX_VERTEX_FILTER_TYPE_PASS ) {
      vgx_GenericVertexFilter_context_t *this_generic = (vgx_GenericVertexFilter_context_t*)context;
      vgx_GenericVertexFilter_context_t *other_generic = (vgx_GenericVertexFilter_context_t*)other;
      this_generic->vertex_probe = other_generic->vertex_probe;
    }
  }

  return context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_filter( vgx_VertexFilter_context_t **filter ) {
  if( *filter ) {
    free( *filter );
    *filter = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexFilter_context_t * __new_evaluator_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget ) {
  vgx_GenericVertexFilter_context_t *evaluator_filter = (vgx_GenericVertexFilter_context_t*)calloc( 1, sizeof( vgx_GenericVertexFilter_context_t ) );
  if( evaluator_filter ) {
    // Base
    evaluator_filter->type = VGX_VERTEX_FILTER_TYPE_EVALUATOR;
    evaluator_filter->positive_match = true;         // Maybe configure later to false
    if( vertex_probe ) {
      evaluator_filter->local_evaluator.pre = vertex_probe->advanced.local_evaluator.filter;
      evaluator_filter->local_evaluator.post = vertex_probe->advanced.local_evaluator.post;
    }
    evaluator_filter->current_thread = 0;
    evaluator_filter->filter = NULL;                 // Select below
    evaluator_filter->timing_budget = timing_budget;
    // Simple
    evaluator_filter->vertex_probe = vertex_probe;
    evaluator_filter->filter = vertexfilterfunc.EvaluatorVertexFilter;
  }
  return (vgx_VertexFilter_context_t*)evaluator_filter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexFilter_context_t * __new_generic_vertex_filter( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget ) {
  vgx_GenericVertexFilter_context_t *generic_filter = (vgx_GenericVertexFilter_context_t*)calloc( 1, sizeof( vgx_GenericVertexFilter_context_t ) );
  if( generic_filter ) {
    // Base
    generic_filter->type = VGX_VERTEX_FILTER_TYPE_GENERIC;
    generic_filter->positive_match = true;         // Maybe configure later to false
    if( vertex_probe ) {
      generic_filter->local_evaluator.pre = vertex_probe->advanced.local_evaluator.filter;
      generic_filter->local_evaluator.post = vertex_probe->advanced.local_evaluator.post;
    }
    generic_filter->current_thread = 0;
    generic_filter->filter = NULL;                 // Select below
    generic_filter->timing_budget = timing_budget;
    // Simple
    generic_filter->vertex_probe = vertex_probe;
    generic_filter->filter = vertexfilterfunc.GenericVertexFilter;
  }
  return (vgx_VertexFilter_context_t*)generic_filter;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_filter.h"


test_descriptor_t _vgx_vxarcvector_filter_tests[] = {
  { "VGX Arcvector Filter Test",   __utest_vxarcvector_filter },
  {NULL}
};
#endif
