/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_rank.c
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

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

/*******************************************************************//**
 * Arc compute rank
 ***********************************************************************
 */
static vgx_VertexSortValue_t __arc_compute_rank_none(          vgx_Ranker_t *ranker, vgx_LockableArc_t *arc );
static vgx_VertexSortValue_t __arc_compute_rank_as_simscore(   vgx_Ranker_t *ranker, vgx_LockableArc_t *arc );
static vgx_VertexSortValue_t __arc_compute_rank_as_hamdist(    vgx_Ranker_t *ranker, vgx_LockableArc_t *arc );
static vgx_VertexSortValue_t __arc_compute_rank_as_composite(  vgx_Ranker_t *ranker, vgx_LockableArc_t *arc );

DLL_HIDDEN vgx_ArcRankScorer_t _iComputeArcRankScore = {
  .by_nothing         = __arc_compute_rank_none,
  .by_simscore        = __arc_compute_rank_as_simscore,
  .by_hamdist         = __arc_compute_rank_as_hamdist,
  .by_composite       = __arc_compute_rank_as_composite
};



/*******************************************************************//**
 * Vertex compute rank
 ***********************************************************************
 */
static vgx_VertexSortValue_t __vertex_compute_rank_none(          vgx_Ranker_t *ranker, vgx_Vertex_t *vertex );
static vgx_VertexSortValue_t __vertex_compute_rank_as_simscore(   vgx_Ranker_t *ranker, vgx_Vertex_t *vertex );
static vgx_VertexSortValue_t __vertex_compute_rank_as_hamdist(    vgx_Ranker_t *ranker, vgx_Vertex_t *vertex );
static vgx_VertexSortValue_t __vertex_compute_rank_as_composite(  vgx_Ranker_t *ranker, vgx_Vertex_t *vertex );

DLL_HIDDEN vgx_VertexRankScorer_t _iComputeVertexRankScore = {
  .by_nothing         = __vertex_compute_rank_none,
  .by_simscore        = __vertex_compute_rank_as_simscore,
  .by_hamdist         = __vertex_compute_rank_as_hamdist,
  .by_composite       = __vertex_compute_rank_as_composite
};



/*******************************************************************//**
 * __arc_compute_rank_none
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static vgx_VertexSortValue_t __arc_compute_rank_none( vgx_Ranker_t *ranker, vgx_LockableArc_t *arc ) {
  vgx_VertexSortValue_t zero = {0};
  return zero;
}



/*******************************************************************//**
 * __arc_compute_rank_as_simscore
 ***********************************************************************
 */
__inline static vgx_VertexSortValue_t __arc_compute_rank_as_simscore( vgx_Ranker_t *ranker, vgx_LockableArc_t *arc ) {
  vgx_Similarity_t *sim = ranker->simcontext;
  vgx_Similarity_vtable_t *isim = CALLABLE( sim );
  vgx_VertexSortValue_t sort;
  if( isim->Valid( sim ) ) {
    vgx_Similarity_value_t *value = isim->Value( sim );
    sort.flt64.value = value->similarity;
  }
  else if( ranker->probe && arc->head.vertex->vector ) {
    sort.flt64.value = isim->Similarity( sim, ranker->probe, arc->head.vertex->vector );
  }
  else {
    sort.flt64.value = 0.0;
  }
  isim->Clear( sim );
  return sort;
}



/*******************************************************************//**
 * __arc_compute_rank_as_hamdist
 ***********************************************************************
 */
__inline static vgx_VertexSortValue_t __arc_compute_rank_as_hamdist( vgx_Ranker_t *ranker, vgx_LockableArc_t *arc ) {
  vgx_VertexSortValue_t sort;
  if( ranker->probe && arc->head.vertex->vector ) {
    sort.int64.value =  hamdist64( ranker->probe->fp, arc->head.vertex->vector->fp );
  }
  else {
    sort.int64.value = 64; // far away
  }
  return sort;
}



/*******************************************************************//**
 * __arc_compute_rank_as_composite
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static vgx_VertexSortValue_t __arc_compute_rank_as_composite( vgx_Ranker_t *ranker, vgx_LockableArc_t *arc ) {

  // Evaluate ranking formula
  vgx_Evaluator_t *evaluator = ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, arc );

  // Final rank score
  vgx_VertexSortValue_t sort = {
    .flt64.value = iEvaluator.GetReal( result )
  };

  return sort;
}



/*******************************************************************//**
 * __vertex_compute_rank_none
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static vgx_VertexSortValue_t __vertex_compute_rank_none( vgx_Ranker_t *ranker, vgx_Vertex_t *vertex ) {
  vgx_VertexSortValue_t zero = {0};
  return zero;
}



/*******************************************************************//**
 * __vertex_compute_rank_as_simscore
 ***********************************************************************
 */
__inline static vgx_VertexSortValue_t __vertex_compute_rank_as_simscore( vgx_Ranker_t *ranker, vgx_Vertex_t *vertex ) {
  vgx_Similarity_t *sim = ranker->simcontext;
  vgx_Similarity_vtable_t *isim = CALLABLE( sim );
  vgx_VertexSortValue_t sort;
  if( isim->Valid( sim ) ) {
    vgx_Similarity_value_t *value = isim->Value( sim );
    sort.flt64.value = value->similarity;
  }
  else if( ranker->probe && vertex->vector ) {
    sort.flt64.value = isim->Similarity( sim, ranker->probe, vertex->vector );
  }
  else {
    sort.flt64.value = 0.0;
  }
  isim->Clear( sim );
  return sort;
}



/*******************************************************************//**
 * __vertex_compute_rank_as_hamdist
 ***********************************************************************
 */
__inline static vgx_VertexSortValue_t __vertex_compute_rank_as_hamdist( vgx_Ranker_t *ranker, vgx_Vertex_t *vertex ) {
  vgx_VertexSortValue_t sort;
  if( ranker->probe && vertex->vector ) {
    sort.int64.value =  hamdist64( ranker->probe->fp, vertex->vector->fp );
  }
  else {
    sort.int64.value = 64; // far away
  }
  return sort;
}



/*******************************************************************//**
 * __vertex_compute_rank_as_composite
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static vgx_VertexSortValue_t __vertex_compute_rank_as_composite( vgx_Ranker_t *ranker, vgx_Vertex_t *vertex ) {

  // Evaluate ranking formula
  vgx_Evaluator_t *evaluator = ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalVertex( evaluator, vertex );

  // Final rank score
  vgx_VertexSortValue_t sort = {
    .flt64.value = iEvaluator.GetReal( result )
  };

  return sort;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_rank.h"


test_descriptor_t _vgx_vxquery_rank_tests[] = {
  { "VGX Query Rank Tests",     __utest_vxquery_rank },
  {NULL}
};
#endif
