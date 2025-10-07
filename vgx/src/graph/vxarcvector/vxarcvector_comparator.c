/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_comparator.c
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
 * Base comparators
 ***********************************************************************
 */
static int __cmp_char(              const char a,                     const char b          );
static int __cmp_uchar(             const unsigned char a,            const unsigned char b );
static int __cmp_int16(             const int16_t a,                  const int16_t b       );
static int __cmp_uint16(            const uint16_t a,                 const uint16_t b      );
static int __cmp_int32(             const int32_t a,                  const int32_t b       );
static int __cmp_int64(             const int64_t a,                  const int64_t b       );
static int __cmp_uint32(            const uint32_t a,                 const uint32_t b      );
static int __cmp_uint64(            const uint64_t a,                 const uint64_t b      );
static int __cmp_float(             const float a,                    const float b         );
static int __cmp_double(            const double a,                   const double b        );
static int __cmp_address(           const void *a,                    const void *b         );
static int __cmp_internalid(        const vgx_CollectorItem_t *a,     const vgx_CollectorItem_t *b );
static int __cmp_tail_internalid(   const vgx_CollectorItem_t *a,     const vgx_CollectorItem_t *b );
static int __cmp_vertex_identifier(     const vgx_Vertex_t *a,            const vgx_Vertex_t *b );
static int __cmp_identifier_fast(       const vgx_CollectorItem_t *a,     const vgx_CollectorItem_t *b );
static int __cmp_tail_identifier_fast(  const vgx_CollectorItem_t *a,     const vgx_CollectorItem_t *b );




/*******************************************************************//**
 * Arc comparators
 ***********************************************************************
 */
static int __cmp_archead_always_1(          const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
// max
static int __cmp_archead_internalid_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_arctail_internalid_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_identifier_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_arctail_identifier_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_int32_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_uint32_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_int64_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_uint64_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_float_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_double_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
// min
static int __cmp_archead_internalid_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_arctail_internalid_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_identifier_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_arctail_identifier_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_int32_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_uint32_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_int64_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_uint64_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_float_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_archead_double_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );


DLL_HIDDEN vgx_ArcComparator_t _iArcMaxComparator = { 
  .cmp_archead_always_1     = __cmp_archead_always_1,
  .cmp_archead_internalid   = __cmp_archead_internalid_max,
  .cmp_arctail_internalid   = __cmp_arctail_internalid_max,
  .cmp_archead_identifier   = __cmp_archead_identifier_max,
  .cmp_arctail_identifier   = __cmp_arctail_identifier_max,
  .cmp_archead_int32_rank   = __cmp_archead_int32_rank_max,
  .cmp_archead_uint32_rank  = __cmp_archead_uint32_rank_max,
  .cmp_archead_int64_rank   = __cmp_archead_int64_rank_max,
  .cmp_archead_uint64_rank  = __cmp_archead_uint64_rank_max,
  .cmp_archead_float_rank   = __cmp_archead_float_rank_max,
  .cmp_archead_double_rank  = __cmp_archead_double_rank_max
};


DLL_HIDDEN vgx_ArcComparator_t _iArcMinComparator = { 
  .cmp_archead_always_1     = __cmp_archead_always_1,
  .cmp_archead_internalid   = __cmp_archead_internalid_min,
  .cmp_arctail_internalid   = __cmp_arctail_internalid_min,
  .cmp_archead_identifier   = __cmp_archead_identifier_min,
  .cmp_arctail_identifier   = __cmp_arctail_identifier_min,
  .cmp_archead_int32_rank   = __cmp_archead_int32_rank_min,
  .cmp_archead_uint32_rank  = __cmp_archead_uint32_rank_min,
  .cmp_archead_int64_rank   = __cmp_archead_int64_rank_min,
  .cmp_archead_uint64_rank  = __cmp_archead_uint64_rank_min,
  .cmp_archead_float_rank   = __cmp_archead_float_rank_min,
  .cmp_archead_double_rank  = __cmp_archead_double_rank_min
};



/*******************************************************************//**
 * Vertex comparators
 ***********************************************************************
 */
static int __cmp_vertex_always_1(          const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
// max
static int __cmp_vertex_internalid_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_identifier_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_int32_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_uint32_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_int64_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_uint64_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_float_rank_max(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_double_rank_max(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
// min
static int __cmp_vertex_internalid_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_identifier_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_int32_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_uint32_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_int64_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_uint64_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_float_rank_min(    const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );
static int __cmp_vertex_double_rank_min(   const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );


DLL_HIDDEN vgx_VertexComparator_t _iVertexMaxComparator = {
  .cmp_vertex_always_1     = __cmp_vertex_always_1,
  .cmp_vertex_internalid   = __cmp_vertex_internalid_max,
  .cmp_vertex_identifier   = __cmp_vertex_identifier_max,
  .cmp_vertex_int32_rank   = __cmp_vertex_int32_rank_max,
  .cmp_vertex_uint32_rank  = __cmp_vertex_uint32_rank_max,
  .cmp_vertex_int64_rank   = __cmp_vertex_int64_rank_max,
  .cmp_vertex_uint64_rank  = __cmp_vertex_uint64_rank_max,
  .cmp_vertex_float_rank   = __cmp_vertex_float_rank_max,
  .cmp_vertex_double_rank  = __cmp_vertex_double_rank_max
};


DLL_HIDDEN vgx_VertexComparator_t _iVertexMinComparator = { 
  .cmp_vertex_always_1     = __cmp_vertex_always_1,
  .cmp_vertex_internalid   = __cmp_vertex_internalid_min,
  .cmp_vertex_identifier   = __cmp_vertex_identifier_min,
  .cmp_vertex_int32_rank   = __cmp_vertex_int32_rank_min,
  .cmp_vertex_uint32_rank  = __cmp_vertex_uint32_rank_min,
  .cmp_vertex_int64_rank   = __cmp_vertex_int64_rank_min,
  .cmp_vertex_uint64_rank  = __cmp_vertex_uint64_rank_min,
  .cmp_vertex_float_rank   = __cmp_vertex_float_rank_min,
  .cmp_vertex_double_rank  = __cmp_vertex_double_rank_min,
};



/*******************************************************************//**
 * Rank score from sort value
 ***********************************************************************
 */
static double __rank_score_from_none(        const vgx_CollectorItem_t *x );
static double __rank_score_from_predicator(  const vgx_CollectorItem_t *x );
static double __rank_score_from_int32(       const vgx_CollectorItem_t *x );
static double __rank_score_from_int64(       const vgx_CollectorItem_t *x );
static double __rank_score_from_uint32(      const vgx_CollectorItem_t *x );
static double __rank_score_from_uint64(      const vgx_CollectorItem_t *x );
static double __rank_score_from_float(       const vgx_CollectorItem_t *x );
static double __rank_score_from_double(      const vgx_CollectorItem_t *x );
static double __rank_score_from_qword(       const vgx_CollectorItem_t *x );


DLL_HIDDEN vgx_RankScoreFromItem_t _iRankScoreFromItem = {
  .from_none        = __rank_score_from_none,
  .from_predicator  = __rank_score_from_predicator,
  .from_int32       = __rank_score_from_int32,
  .from_int64       = __rank_score_from_int64,
  .from_uint32      = __rank_score_from_uint32,
  .from_uint64      = __rank_score_from_uint64,
  .from_float       = __rank_score_from_float,
  .from_double      = __rank_score_from_double,
  .from_qword       = __rank_score_from_qword
};



/*******************************************************************//**
 * Arc collect to sort on
 ***********************************************************************
 */
static int __arc_no_collect(                                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_unsorted_list(                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_first_value_map(                vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_max_value_map(                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_min_value_map(                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_average_value_map(              vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_counting_map(                   vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_aggregating_sum_map(            vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_aggregating_sum_of_squares_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_into_aggregating_product_map(        vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );

static int __arc_collect_to_sort_by_integer_predicator(       vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_unsigned_predicator(      vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_real_predicator(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_any_predicator(           vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );

static int __arc_collect_to_sort_by_memaddress(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_internalid(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_identifier(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_tail_internalid(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_tail_identifier(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_degree(                   vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_indegree(                 vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_outdegree(                vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_simscore(                 vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_hamdist(                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_rankscore(                vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_vertex_tmc(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_vertex_tmm(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_vertex_tmx(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_native_order(             vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __arc_collect_to_sort_by_random_order(             vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );

DLL_HIDDEN vgx_ArcCollector_t _iCollectArc = {
  .no_collect                     = __arc_no_collect,
  .into_unsorted_list             = __arc_collect_into_unsorted_list,
  .into_first_value_map           = __arc_collect_into_first_value_map,
  .into_max_value_map             = __arc_collect_into_max_value_map,
  .into_min_value_map             = __arc_collect_into_min_value_map,
  .into_average_value_map         = __arc_collect_into_average_value_map,
  .into_counting_map              = __arc_collect_into_counting_map,
  .into_aggregating_sum_map       = __arc_collect_into_aggregating_sum_map,
  .into_aggregating_sqsum_map     = __arc_collect_into_aggregating_sum_of_squares_map,
  .into_aggregating_product_map   = __arc_collect_into_aggregating_product_map,
  .to_sort_by_integer_predicator  = __arc_collect_to_sort_by_integer_predicator,
  .to_sort_by_unsigned_predicator = __arc_collect_to_sort_by_unsigned_predicator,
  .to_sort_by_real_predicator     = __arc_collect_to_sort_by_real_predicator,
  .to_sort_by_any_predicator      = __arc_collect_to_sort_by_any_predicator,
  .to_sort_by_memaddress          = __arc_collect_to_sort_by_memaddress,
  .to_sort_by_internalid          = __arc_collect_to_sort_by_internalid,
  .to_sort_by_identifier          = __arc_collect_to_sort_by_identifier,
  .to_sort_by_tail_internalid     = __arc_collect_to_sort_by_tail_internalid,
  .to_sort_by_tail_identifier     = __arc_collect_to_sort_by_tail_identifier,
  .to_sort_by_degree              = __arc_collect_to_sort_by_degree,
  .to_sort_by_indegree            = __arc_collect_to_sort_by_indegree,
  .to_sort_by_outdegree           = __arc_collect_to_sort_by_outdegree,
  .to_sort_by_simscore            = __arc_collect_to_sort_by_simscore,
  .to_sort_by_hamdist             = __arc_collect_to_sort_by_hamdist,
  .to_sort_by_rankscore           = __arc_collect_to_sort_by_rankscore,
  .to_sort_by_tmc                 = __arc_collect_to_sort_by_vertex_tmc,
  .to_sort_by_tmm                 = __arc_collect_to_sort_by_vertex_tmm,
  .to_sort_by_tmx                 = __arc_collect_to_sort_by_vertex_tmx,
  .to_sort_by_native_order        = __arc_collect_to_sort_by_native_order,
  .to_sort_by_random_order        = __arc_collect_to_sort_by_random_order
};



/*******************************************************************//**
 * Arc stage to sort on
 ***********************************************************************
 */

static int __arc_no_stage(                                 vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_unsorted(                           vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_integer_predicator(       vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_unsigned_predicator(      vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_real_predicator(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_any_predicator(           vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );

static int __arc_stage_to_sort_by_memaddress(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_internalid(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_identifier(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_tail_internalid(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_tail_identifier(          vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_degree(                   vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_indegree(                 vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_outdegree(                vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_simscore(                 vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_hamdist(                  vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_rankscore(                vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_vertex_tmc(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_vertex_tmm(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_vertex_tmx(               vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_native_order(             vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );
static int __arc_stage_to_sort_by_random_order(             vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );




DLL_HIDDEN vgx_ArcStager_t _iStageArc = {
  .no_stage                       = __arc_no_stage,
  .unsorted                       = __arc_stage_unsorted,
  .to_sort_by_integer_predicator  = __arc_stage_to_sort_by_integer_predicator,
  .to_sort_by_unsigned_predicator = __arc_stage_to_sort_by_unsigned_predicator,
  .to_sort_by_real_predicator     = __arc_stage_to_sort_by_real_predicator,
  .to_sort_by_any_predicator      = __arc_stage_to_sort_by_any_predicator,
  .to_sort_by_memaddress          = __arc_stage_to_sort_by_memaddress,
  .to_sort_by_internalid          = __arc_stage_to_sort_by_internalid,
  .to_sort_by_identifier          = __arc_stage_to_sort_by_identifier,
  .to_sort_by_tail_internalid     = __arc_stage_to_sort_by_tail_internalid,
  .to_sort_by_tail_identifier     = __arc_stage_to_sort_by_tail_identifier,
  .to_sort_by_degree              = __arc_stage_to_sort_by_degree,
  .to_sort_by_indegree            = __arc_stage_to_sort_by_indegree,
  .to_sort_by_outdegree           = __arc_stage_to_sort_by_outdegree,
  .to_sort_by_simscore            = __arc_stage_to_sort_by_simscore,
  .to_sort_by_hamdist             = __arc_stage_to_sort_by_hamdist,
  .to_sort_by_rankscore           = __arc_stage_to_sort_by_rankscore,
  .to_sort_by_tmc                 = __arc_stage_to_sort_by_vertex_tmc,
  .to_sort_by_tmm                 = __arc_stage_to_sort_by_vertex_tmm,
  .to_sort_by_tmx                 = __arc_stage_to_sort_by_vertex_tmx,
  .to_sort_by_native_order        = __arc_stage_to_sort_by_native_order,
  .to_sort_by_random_order        = __arc_stage_to_sort_by_random_order
};




/*******************************************************************//**
 * Vertex collect to sort on
 ***********************************************************************
 */
static int __vertex_no_collect(                       vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_nosort(                   vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_memaddress(    vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_internalid(    vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_identifier(    vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_degree(        vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_indegree(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_outdegree(     vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_simscore(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_hamdist(       vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_rankscore(     vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_tmc(           vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_tmm(           vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_tmx(           vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_native_order(  vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );
static int __vertex_collect_to_sort_by_random_order(  vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );


DLL_HIDDEN vgx_VertexCollector_t _iCollectVertex = {
  .no_collect               = __vertex_no_collect,
  .into_unsorted_list       = __vertex_collect_nosort,
  .to_sort_by_memaddress    = __vertex_collect_to_sort_by_memaddress,
  .to_sort_by_internalid    = __vertex_collect_to_sort_by_internalid,
  .to_sort_by_identifier    = __vertex_collect_to_sort_by_identifier,
  .to_sort_by_degree        = __vertex_collect_to_sort_by_degree,
  .to_sort_by_indegree      = __vertex_collect_to_sort_by_indegree,
  .to_sort_by_outdegree     = __vertex_collect_to_sort_by_outdegree,
  .to_sort_by_simscore      = __vertex_collect_to_sort_by_simscore,
  .to_sort_by_hamdist       = __vertex_collect_to_sort_by_hamdist,
  .to_sort_by_rankscore     = __vertex_collect_to_sort_by_rankscore,
  .to_sort_by_tmc           = __vertex_collect_to_sort_by_tmc,
  .to_sort_by_tmm           = __vertex_collect_to_sort_by_tmm,
  .to_sort_by_tmx           = __vertex_collect_to_sort_by_tmx,
  .to_sort_by_native_order  = __vertex_collect_to_sort_by_native_order,
  .to_sort_by_random_order  = __vertex_collect_to_sort_by_random_order
};



/*******************************************************************//**
 * Vertex stage to sort on
 ***********************************************************************
 */

static int __vertex_no_stage(                         vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_unsorted(                   vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_memaddress(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_internalid(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_identifier(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_degree(          vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_indegree(        vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_outdegree(       vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_simscore(        vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_hamdist(         vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_rankscore(       vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_vertex_tmc(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_vertex_tmm(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_vertex_tmx(      vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_native_order(    vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );
static int __vertex_stage_to_sort_by_random_order(    vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );




DLL_HIDDEN vgx_VertexStager_t _iStageVertex = {
  .no_stage                       = __vertex_no_stage,
  .unsorted                       = __vertex_stage_unsorted,
  .to_sort_by_memaddress          = __vertex_stage_to_sort_by_memaddress,
  .to_sort_by_internalid          = __vertex_stage_to_sort_by_internalid,
  .to_sort_by_identifier          = __vertex_stage_to_sort_by_identifier,
  .to_sort_by_degree              = __vertex_stage_to_sort_by_degree,
  .to_sort_by_indegree            = __vertex_stage_to_sort_by_indegree,
  .to_sort_by_outdegree           = __vertex_stage_to_sort_by_outdegree,
  .to_sort_by_simscore            = __vertex_stage_to_sort_by_simscore,
  .to_sort_by_hamdist             = __vertex_stage_to_sort_by_hamdist,
  .to_sort_by_rankscore           = __vertex_stage_to_sort_by_rankscore,
  .to_sort_by_tmc                 = __vertex_stage_to_sort_by_vertex_tmc,
  .to_sort_by_tmm                 = __vertex_stage_to_sort_by_vertex_tmm,
  .to_sort_by_tmx                 = __vertex_stage_to_sort_by_vertex_tmx,
  .to_sort_by_native_order        = __vertex_stage_to_sort_by_native_order,
  .to_sort_by_random_order        = __vertex_stage_to_sort_by_random_order
};




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __update_refmap_head_tail( vgx_BaseCollector_context_t *base, vgx_CollectorItem_t *inserted, vgx_CollectorItem_t *discarded, vgx_LockableArc_t *larc, vgx_predicator_t *pred_ovr ) {
  vgx_Graph_t *locked_graph = NULL;
  int updated = -1;

  // Insert references
  if( inserted ) {
    do {
      inserted->tailref = NULL;
      inserted->headref = NULL;
      if( _vxquery_collector__safe_tail_access_ACQUIRE_CS( base, larc, &locked_graph ) ) {
        if( (inserted->tailref = _vxquery_collector__add_vertex_reference( base, larc->tail, &larc->acquired.tail_lock )) != NULL ) {
          if( _vxquery_collector__safe_head_access_ACQUIRE_CS( base, larc, &locked_graph ) ) {
            if( (inserted->headref = _vxquery_collector__add_vertex_reference( base, larc->head.vertex, &larc->acquired.head_lock )) != NULL ) {
              inserted->predicator = pred_ovr ? *pred_ovr : larc->head.predicator;
              // SUCCESS
              updated = 1;
              break;
            }
          }
        }
      }
      // ERROR
      _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, inserted->tailref, &locked_graph );
      _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, inserted->headref, &locked_graph );
    } WHILE_ZERO;
  }

  // Discard references
  if( discarded ) {
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, discarded->tailref, &locked_graph );
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, discarded->headref, &locked_graph );
  }

  GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );

  return updated;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __update_refmap_vertex( vgx_BaseCollector_context_t *base, vgx_CollectorItem_t *inserted, vgx_CollectorItem_t *discarded, vgx_LockableArc_t *larc ) {
  vgx_Graph_t *locked_graph = NULL;
  int updated = -1;

  // Insert references
  if( inserted ) {
    inserted->tailref = NULL;
    inserted->headref = NULL;
    if( _vxquery_collector__safe_head_access_ACQUIRE_CS( base, larc, &locked_graph ) ) {
      if( (inserted->tailref = inserted->headref = _vxquery_collector__add_vertex_reference( base, larc->head.vertex, &larc->acquired.head_lock )) != NULL ) {
        inserted->headref->refcnt++; // own it twice since both tail and head point to the same vertex
        // SUCCESS
        updated = 1;
      }
    }
  }

  // Discard references
  if( discarded ) {
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, discarded->tailref, &locked_graph );
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( base, discarded->headref, &locked_graph );
  }

  GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );

  return updated;
}



#define __arc_sortby_none             (SORT.uint64.value = (uintptr_t)LARC->head.vertex);
#define __arc_sortby_memaddress       (SORT.uint64.value = (uintptr_t)LARC->head.vertex);
#define __arc_sortby_internalid       (SORT.internalid_H = __vertex_internalid( LARC->head.vertex )->H);
#define __arc_sortby_idprefix         (SORT.qword        = *LARC->head.vertex->identifier.idprefix.qwords);
#define __arc_sortby_tail_internalid  (SORT.internalid_H = __vertex_internalid( LARC->tail )->H);
#define __arc_sortby_tail_idprefix    (SORT.qword        = *LARC->tail->identifier.idprefix.qwords);
#define __arc_sortby_degree           (SORT.int64.value  = iarcvector.Degree( &LARC->head.vertex->inarcs ) + iarcvector.Degree( &LARC->head.vertex->outarcs ));
#define __arc_sortby_indegree         (SORT.int64.value  = iarcvector.Degree( &LARC->head.vertex->inarcs ));
#define __arc_sortby_outdegree        (SORT.int64.value  = iarcvector.Degree( &LARC->head.vertex->outarcs ));
#define __arc_sortby_rankscore        (SORT              = COLLECTOR->ranker->arc_score( COLLECTOR->ranker, LARC ));
#define __arc_sortby_tmc              (SORT.uint32.value = LARC->head.vertex->TMC);
#define __arc_sortby_tmm              (SORT.uint32.value = LARC->head.vertex->TMM);
#define __arc_sortby_tmx              (SORT.uint32.value = LARC->head.vertex->TMX.vertex_ts);


#define __arc_sortby_native_order   \
do {                            \
  SORT.int64.value = 0;         \
} WHILE_ZERO;


#define __arc_sortby_random_order       \
do {                                    \
  SORT.int64.value = (int64_t)rand63(); \
} WHILE_ZERO;


/*******************************************************************//**
 * __arc_no_collect
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __arc_no_collect( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return 0;
}



/*******************************************************************//**
 * __arc_no_stage
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __arc_no_stage( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int stage_index, vgx_predicator_t *predicator_override ) {
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static double __predicator_value_as_double( vgx_predicator_t predicator ) {
  // Convert any predicator value to double, regardless of modifier
  switch( _vgx_predicator_value_range( NULL, NULL, predicator.mod.bits ) ) {
  case VGX_PREDICATOR_VAL_TYPE_INTEGER:
    return (double)predicator.val.integer;
  case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
    return (double)predicator.val.uinteger;
  case VGX_PREDICATOR_VAL_TYPE_REAL:
    return (double)predicator.val.real;
  default:
    return 0.0;
  }
}



/*******************************************************************//**
 * __get_aggregation_key
 ***********************************************************************
 */
  // What we have as basis for a unique arc is the HEAD and the PREDICATOR.
  // The arc direction doesn't matter. What led us to the vertex depends on
  // whether we're traversing the inarc or the outarcs. Either way the "HEAD"
  // will be the vertex that the arcvector points to. So if we're collecting
  // inarcs it is technically confusing to call it the head.vertex, but it's ok
  // as long as we know this.
  //
  // The vertex has a 64-bit address.
  // The predicator "key" is a 19-bit value comprising the encoded relationship (14) and stored modifier (5).
  // How do we cram the pointer and the predicator key into a unique 64-bit framehash key??
  // The ridiculous answer is that on our current platform we only deal with 48-bit addresses,
  // and of those 48 bits only 45 bits are significant since the lower 3 bits are always 0 for quadword aligned access...
  // So by pure luck our pointers are 45 bits of significant data. Add 19 bits of predicator key data and we have a 64-bit unique framehash key.
  // Almost too good to be true but I'll take it.
  //
  // NOTE: This requires __ARCH_MEMORY_48 to be defined. In some future 52-bit address architecture we're going to have to re-design this.
  //
  // NOTE2: The aggregation aspect is tied to the PATH to the collected vertices. If many paths lead to a vertex, all paths are
  //        accounted for by aggregating the values of those paths (the predicator value of the final arc leading to the vertex.)
  //        No sorting is performed here, only insertion into a map, where the predicator values of arcs leading to duplicate vertices 
  //        are summed. Any sorting is deferred to a post-collection step. Since the collected vertices are preserved as part of the
  //        map's key the post-collection sorting is free to sort on any vertex attribute. Or sorting may be performed on the
  //        aggregated predicator values of the vertices.
__inline static vgx_ArcAggregationKey_t __get_aggregation_key( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_ArcAggregationKey_t key = { .bits = 0 };

  // Convert modifier to indicate aggregation of either float or int type
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    pred.mod.bits = VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR;
  }
  else {
    pred.mod.bits = VGX_PREDICATOR_MOD_INT_AGGREGATOR;
  }

  if( _vxquery_collector__safe_head_access_OPEN( collector, larc ) ) {
    vgx_VertexRef_t *ref = _vxquery_collector__add_vertex_reference( collector, larc->head.vertex, &larc->acquired.head_lock );
    if( ref ) {
      // Construct the map key
      key.vertexref_qwo = __TPTR_PACK( ref );
      key.pred_key = pred.rkey;
    }
  }

  return key;
}



/*******************************************************************//**
 * __get_int_aggregation_key
 ***********************************************************************
 */
__inline static vgx_ArcAggregationKey_t __get_int_aggregation_key( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_ArcAggregationKey_t key = { .bits = 0 };

  // Convert modifier to indicate aggregation of either float or int type
  vgx_predicator_t pred = larc->head.predicator;
  pred.mod.bits = VGX_PREDICATOR_MOD_INT_AGGREGATOR;

  if( _vxquery_collector__safe_head_access_OPEN( collector, larc ) ) {
    vgx_VertexRef_t *ref = _vxquery_collector__add_vertex_reference( collector, larc->head.vertex, &larc->acquired.head_lock );
    if( ref ) {
      // Construct the map key
      key.vertexref_qwo = __TPTR_PACK( ref );
      key.pred_key = pred.rkey;
    }
  }

  return key;
}



/*******************************************************************//**
 * __arc_collect_into_first_value_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_first_value_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;
  framehash_vtable_t *ifh = CALLABLE(aggregator);

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Have we already collected the first value?
  if( ifh->HasKey64( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits ) == CELL_VALUE_TYPE_NULL ) {
    // Set once
    vgx_predicator_t pred = larc->head.predicator;
    if( _vgx_predicator_value_is_float( pred ) ) {
      CALLABLE( aggregator )->SetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.real );
    }
    else {
      CALLABLE( aggregator )->SetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.integer );
    }
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_max_value_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_max_value_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;
  framehash_vtable_t *ifh = CALLABLE(aggregator);

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Update value if larger than previous
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    double prev = DBL_MIN;
    ifh->GetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &prev );
    if( pred.val.real > prev ) {
      CALLABLE( aggregator )->SetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.real );
    }
  }
  else {
    int64_t prev = LLONG_MIN;
    ifh->GetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &prev );
    if( pred.val.integer > prev ) {
      CALLABLE( aggregator )->SetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.integer );
    }
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_min_value_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_min_value_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;
  framehash_vtable_t *ifh = CALLABLE(aggregator);

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Update value if smaller than previous
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    double prev = DBL_MAX;
    ifh->GetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &prev );
    if( pred.val.real < prev ) {
      CALLABLE( aggregator )->SetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.real );
    }
  }
  else {
    int64_t prev = LLONG_MAX;
    ifh->GetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &prev );
    if( pred.val.integer < prev ) {
      CALLABLE( aggregator )->SetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.integer );
    }
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_average_value_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_average_value_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;
  framehash_vtable_t *ifh = CALLABLE(aggregator);

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  vgx_predicator_t pred = larc->head.predicator;
  float value;
  if( _vgx_predicator_value_is_float( pred ) ) {
    value = pred.val.real;
  }
  else {
    value = (float)pred.val.integer;
  }

  vgx_ArcAggregationValue_t avg = { .bits = 0 };
  ifh->GetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &avg.ibits );
  uint32_t n = ++avg.count;
  avg.real = avg.real + (value - avg.real) / n;
  ifh->SetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, avg.ibits );

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_counting_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_counting_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_int_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Count
  int64_t count = 0;
  CALLABLE( aggregator )->IncInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, 1, &count );

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_aggregating_sum_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_aggregating_sum_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1;
  }

  // Add this predicator value to the running sum
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    double aggr;
    CALLABLE( aggregator )->IncReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.real, &aggr );
  }
  else {
    int64_t aggr;
    CALLABLE( aggregator )->IncInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, pred.val.integer, &aggr );
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_aggregating_sum_of_squares_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_aggregating_sum_of_squares_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Add this predicator value to the running sum of squares
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    double aggr;
    double val = pred.val.real;
    val *= val;
    CALLABLE( aggregator )->IncReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, val, &aggr );
  }
  else {
    int64_t aggr;
    int64_t val = pred.val.integer;
    val *= val;
    CALLABLE( aggregator )->IncInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, val, &aggr );
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_aggregating_product_map
 ***********************************************************************
 */
__inline static int __arc_collect_into_aggregating_product_map( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {

  // Aggregator map
  framehash_t *aggregator = collector->container.mapping.aggregator;
  framehash_vtable_t *ifh = CALLABLE(aggregator);

  // Get the aggregation key
  vgx_ArcAggregationKey_t key = __get_aggregation_key( (vgx_BaseCollector_context_t*)collector, larc );
  if( key.bits == 0 ) {
    return -1; // timeout
  }

  // Add this predicator value to the running product
  vgx_predicator_t pred = larc->head.predicator;
  if( _vgx_predicator_value_is_float( pred ) ) {
    double aggr = 1.0;
    ifh->GetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &aggr );
    aggr *= pred.val.real;
    CALLABLE( aggregator )->SetReal56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, aggr );
  }
  else {
    int64_t aggr = 1;
    ifh->GetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, &aggr );
    aggr *= pred.val.integer;
    if( aggr > INT_MAX ) {
      aggr = INT_MAX;
    }
    else if( aggr < INT_MIN ) {
      aggr = INT_MIN;
    }
    CALLABLE( aggregator )->SetInt56( aggregator, CELL_KEY_TYPE_PLAIN64, key.bits, aggr );
  }

  return 1;
}



/*******************************************************************//**
 * __push_arc
 ***********************************************************************
 */
__inline static int __push_arc( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_VertexSortValue_t sort ) {
  Cm256iHeap_t *heap = collector->container.sequence.heap;
  
  vgx_VertexRef_t sort_tailref = { .vertex = larc->tail };
  vgx_VertexRef_t sort_headref = { .vertex = larc->head.vertex };
  vgx_CollectorItem_t collected = {
    .tailref    = &sort_tailref, // overwrite with managed reference from refmap if collected
    .headref    = &sort_headref, // overwrite with managed reference from refmap if collected
    .predicator = larc->head.predicator,
    .sort       = sort
  };
  vgx_CollectorItem_t discarded;
  vgx_CollectorItem_t *push_location;

  // Collect item into heap
  if( (push_location = (vgx_CollectorItem_t*)CALLABLE(heap)->HeapPushTopK( heap, &collected.item, &discarded.item )) == NULL ) {
    // Item not sorted high enough, nothing collected
    return 0;
  }

  // New item pushed, lower sorting item discarded
  return __update_refmap_head_tail( (vgx_BaseCollector_context_t*)collector, push_location, &discarded, larc, NULL );
}



/*******************************************************************//**
 * __append_arc
 ***********************************************************************
 */
__inline static int __append_arc( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  Cm256iList_t *list = collector->container.sequence.list;

  vgx_VertexRef_t tailref = { .vertex = larc->tail };
  vgx_VertexRef_t headref = { .vertex = larc->head.vertex };
  vgx_CollectorItem_t collected = {
    .tailref    = &tailref, // overwrite with managed reference from refmap if collected
    .headref    = &headref, // overwrite with managed reference from refmap if collected
    .predicator = larc->head.predicator,
    .sort       = {0}
  };

  // Manage vertex references / arc locks
  if( __update_refmap_head_tail( (vgx_BaseCollector_context_t*)collector, &collected, NULL, larc, NULL ) < 1 ) {
    return -1;
  }

  // Append item to unsorted list
  if( CALLABLE(list)->Append( list, &collected.item ) < 1 ) {
    __update_refmap_head_tail( (vgx_BaseCollector_context_t*)collector, NULL, &collected, NULL, NULL );
    return 0;
  }

  return 1;
}



/*******************************************************************//**
 * __stage_arc
 ***********************************************************************
 */
__inline static int __stage_arc( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override, vgx_VertexSortValue_t sort ) {

  // Staging slot
  vgx_CollectorItem_t *stage = &collector->stage->slot[ index & (VGX_COLLECTOR_STAGE_SIZE-1) ];
  vgx_BaseCollector_context_t *base = (vgx_BaseCollector_context_t*)collector;

  // Discard uncommitted item if any
  vgx_CollectorItem_t discarded = *stage;
  if( discarded.headref ) { // <- if head is not NULL then item exists
    __update_refmap_head_tail( base, NULL, &discarded, NULL, NULL );
  }

  // Stage item into slot
  vgx_VertexRef_t sort_tailref = { .vertex = larc->tail };
  vgx_VertexRef_t sort_headref = { .vertex = larc->head.vertex };
  stage->tailref = &sort_tailref;   // overwrite with managed reference from refmap if collected
  stage->headref = &sort_headref;   // overwrite with managed reference from refmap if collected
  stage->predicator = larc->head.predicator;
  stage->sort = sort;

  // Manage vertex references / arc locks
  if( __update_refmap_head_tail( base, stage, NULL, larc, predicator_override ) < 1 ) {
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 * __arc_collect_into_unsorted_list
 ***********************************************************************
 */
__inline static int __arc_collect_into_unsorted_list( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return __append_arc( collector, larc );
}



/*******************************************************************//**
 * __arc_stage_unsorted
 ***********************************************************************
 */
__inline static int __arc_stage_unsorted( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  vgx_VertexSortValue_t nosort = {0};
  return __stage_arc( collector, larc, index, predicator_override, nosort );
}



/*******************************************************************//**
 * __arc_collect_to_sort_by_integer_predicator
 ***********************************************************************
 */
__inline static int __arc_collect_to_sort_by_integer_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_VertexSortValue_t sort;
  sort.int64.value = (int64_t)larc->head.predicator.val.integer;
  return __push_arc( collector, larc, sort );
}



/*******************************************************************//**
 * __arc_stage_to_sort_by_integer_predicator
 ***********************************************************************
 */
__inline static int __arc_stage_to_sort_by_integer_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  vgx_VertexSortValue_t sort = {
    .int64.value = predicator_override ? predicator_override->val.integer : larc->head.predicator.val.integer
  };
  return __stage_arc( collector, larc, index, predicator_override, sort );
}



/*******************************************************************//**
 * __arc_collect_to_sort_by_unsigned_predicator
 ***********************************************************************
 */
__inline static int __arc_collect_to_sort_by_unsigned_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_VertexSortValue_t sort;
  sort.int64.value = (int64_t)larc->head.predicator.val.uinteger;
  return __push_arc( collector, larc, sort );
}



/*******************************************************************//**
 * __arc_stage_to_sort_by_unsigned_predicator
 ***********************************************************************
 */
__inline static int __arc_stage_to_sort_by_unsigned_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  vgx_VertexSortValue_t sort = {
    .int64.value = (int64_t)predicator_override ? predicator_override->val.uinteger : larc->head.predicator.val.uinteger
  };
  return __stage_arc( collector, larc, index, predicator_override, sort );
}



/*******************************************************************//**
 * __arc_collect_to_sort_by_real_predicator
 ***********************************************************************
 */
__inline static int __arc_collect_to_sort_by_real_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_VertexSortValue_t sort;
  sort.flt64.value = larc->head.predicator.val.real;
  return __push_arc( collector, larc, sort );
}



/*******************************************************************//**
 * __arc_stage_to_sort_by_real_predicator
 ***********************************************************************
 */
__inline static int __arc_stage_to_sort_by_real_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  vgx_VertexSortValue_t sort = {
    .flt64.value = predicator_override ? predicator_override->val.real : larc->head.predicator.val.real
  };
  return __stage_arc( collector, larc, index, predicator_override, sort );
}



/*******************************************************************//**
 * __arc_collect_to_sort_by_any_predicator
 * Special treatment of predicator value
 ***********************************************************************
 */
__inline static int __arc_collect_to_sort_by_any_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_VertexSortValue_t sort;
  // Convert any predicator value to real, regardless of modifier
  sort.flt64.value = __predicator_value_as_double( larc->head.predicator );
  return __push_arc( collector, larc, sort );
}



/*******************************************************************//**
 * __arc_stage_to_sort_by_any_predicator
 * Special treatment of predicator value
 ***********************************************************************
 */
__inline static int __arc_stage_to_sort_by_any_predicator( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  // Convert any predicator value to real, regardless of modifier
  vgx_VertexSortValue_t sort = {
    .flt64.value = __predicator_value_as_double( predicator_override ? *predicator_override : larc->head.predicator )
  };
  return __stage_arc( collector, larc, index, predicator_override, sort );
}



/*******************************************************************//**
 * __arc_collect_to_sort_by_rankscore
 * Special treatment of rankscore
 ***********************************************************************
 */
__inline static int __arc_collect_to_sort_by_rankscore( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_Evaluator_t *evaluator = collector->ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, larc );
  vgx_VertexSortValue_t sort = { .flt64.value = iEvaluator.GetReal( result ) };
  if( sort.flt64.value < 0 ) {
    collector->n_collectable--; // revert
    return 0;
  }
  return __push_arc( collector, larc, sort );
}



/*******************************************************************//**
 * __arc_stage_to_sort_by_rankscore
 * Special treatment of rankscore
 ***********************************************************************
 */
__inline static int __arc_stage_to_sort_by_rankscore( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override ) {
  vgx_Ranker_t *ranker = collector->ranker;
  if( __ranker_acquire_lockable_arc_head( ranker, larc ) < 0 ) {
    return -1;
  }
  vgx_Evaluator_t *evaluator = ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, larc );
  vgx_VertexSortValue_t sort = { .flt64.value = iEvaluator.GetReal( result ) };
  if( sort.flt64.value < 0 ) {
    return 0;
  }
  return __stage_arc( collector, larc, index, predicator_override, sort );
}



#define __arc_collect_TO_SORT_BY( Field ) \
__inline static int __arc_collect_to_sort_by_##Field( vgx_ArcCollector_context_t *COLLECTOR, vgx_LockableArc_t *larc ) { \
  vgx_VertexSortValue_t SORT;                                     \
  vgx_LockableArc_t *LARC = larc;
  

#define __end_arc_collect                                     \
  return __push_arc( COLLECTOR, LARC, SORT );                     \
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER                  \
  SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED                  \
}


__arc_collect_TO_SORT_BY( memaddress )      { __arc_sortby_memaddress       } __end_arc_collect
__arc_collect_TO_SORT_BY( internalid )      { __arc_sortby_internalid       } __end_arc_collect
__arc_collect_TO_SORT_BY( identifier )      { __arc_sortby_idprefix         } __end_arc_collect
__arc_collect_TO_SORT_BY( tail_internalid ) { __arc_sortby_tail_internalid  } __end_arc_collect
__arc_collect_TO_SORT_BY( tail_identifier ) { __arc_sortby_tail_idprefix    } __end_arc_collect
__arc_collect_TO_SORT_BY( degree )          { __arc_sortby_degree           } __end_arc_collect
__arc_collect_TO_SORT_BY( indegree )        { __arc_sortby_indegree         } __end_arc_collect
__arc_collect_TO_SORT_BY( outdegree )       { __arc_sortby_outdegree        } __end_arc_collect
__arc_collect_TO_SORT_BY( simscore )        { __arc_sortby_rankscore        } __end_arc_collect
__arc_collect_TO_SORT_BY( hamdist )         { __arc_sortby_rankscore        } __end_arc_collect
__arc_collect_TO_SORT_BY( vertex_tmc )      { __arc_sortby_tmc              } __end_arc_collect
__arc_collect_TO_SORT_BY( vertex_tmm )      { __arc_sortby_tmm              } __end_arc_collect
__arc_collect_TO_SORT_BY( vertex_tmx )      { __arc_sortby_tmx              } __end_arc_collect
__arc_collect_TO_SORT_BY( native_order )    { __arc_sortby_native_order     } __end_arc_collect
__arc_collect_TO_SORT_BY( random_order )    { __arc_sortby_random_order     } __end_arc_collect





#define __arc_stage_TO_SORT_BY( Field )                             \
__inline static int __arc_stage_to_sort_by_##Field( vgx_ArcCollector_context_t *COLLECTOR, vgx_LockableArc_t *larc, int INDEX, vgx_predicator_t *PREDICATOR_OVERRIDE ) { \
  vgx_VertexSortValue_t SORT;                                       \
  vgx_LockableArc_t *LARC = larc;                                   \
  vgx_Ranker_t *ranker = COLLECTOR->ranker;                         \
  if( ranker ) {                                                    \
    if( __ranker_acquire_lockable_arc_head( ranker, larc ) < 0 ) {  \
      return -1;                                                    \
    }                                                               \
  }

  

#define __end_arc_stage                                                     \
  return __stage_arc( COLLECTOR, LARC, INDEX, PREDICATOR_OVERRIDE, SORT );  \
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER                            \
  SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED                            \
}


__arc_stage_TO_SORT_BY( memaddress )      { __arc_sortby_memaddress       } __end_arc_stage
__arc_stage_TO_SORT_BY( internalid )      { __arc_sortby_internalid       } __end_arc_stage
__arc_stage_TO_SORT_BY( identifier )      { __arc_sortby_idprefix         } __end_arc_stage
__arc_stage_TO_SORT_BY( tail_internalid ) { __arc_sortby_tail_internalid  } __end_arc_stage
__arc_stage_TO_SORT_BY( tail_identifier ) { __arc_sortby_tail_idprefix    } __end_arc_stage
__arc_stage_TO_SORT_BY( degree )          { __arc_sortby_degree           } __end_arc_stage
__arc_stage_TO_SORT_BY( indegree )        { __arc_sortby_indegree         } __end_arc_stage
__arc_stage_TO_SORT_BY( outdegree )       { __arc_sortby_outdegree        } __end_arc_stage
__arc_stage_TO_SORT_BY( simscore )        { if(ranker) {__arc_sortby_rankscore} else {SORT.qword = 0;} } __end_arc_stage
__arc_stage_TO_SORT_BY( hamdist )         { if(ranker) {__arc_sortby_rankscore} else {SORT.qword = 0;} } __end_arc_stage
__arc_stage_TO_SORT_BY( vertex_tmc )      { __arc_sortby_tmc              } __end_arc_stage
__arc_stage_TO_SORT_BY( vertex_tmm )      { __arc_sortby_tmm              } __end_arc_stage
__arc_stage_TO_SORT_BY( vertex_tmx )      { __arc_sortby_tmx              } __end_arc_stage
__arc_stage_TO_SORT_BY( native_order )    { __arc_sortby_native_order     } __end_arc_stage
__arc_stage_TO_SORT_BY( random_order )    { __arc_sortby_random_order     } __end_arc_stage



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_comparator__unstage( vgx_BaseCollector_context_t *collector, int index ) {
  int unstaged = 0;

  // Staging slot
  vgx_CollectorItem_t *stage = &collector->stage->slot[ index & (VGX_COLLECTOR_STAGE_SIZE-1) ];

  if( stage->headref ) {
    // Discard staged item
    __update_refmap_head_tail( collector, NULL, stage, NULL, NULL );
    unstaged = 1;

    // Clear staging slot (item either captured into heap/list or discarded if operation failed)
    stage->headref = NULL;
    stage->tailref = NULL;
  }

  return unstaged;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_comparator__commit( vgx_BaseCollector_context_t *collector, int index ) {
  int committed = 0;

  // Staging slot
  vgx_CollectorItem_t *stage = &collector->stage->slot[ index & (VGX_COLLECTOR_STAGE_SIZE-1) ];

  // Nothing staged ?
  if( stage->headref == NULL ) {
    return 0;
  }

  // Proceed with commit
  if( collector->n_remain > 0 ) {
    if( _vgx_collector_is_sorted( collector ) ) {
      vgx_CollectorItem_t discarded;
      Cm256iHeap_t *heap = collector->container.sequence.heap;
      // Collect item into heap
      if( CALLABLE(heap)->HeapPushTopK( heap, &stage->item, &discarded.item ) != NULL ) {
        // Staged item already in refmap pushed, remove discarded item from refmap
        __update_refmap_head_tail( collector, NULL, &discarded, NULL, NULL );
        committed = 1;
      }
      else {
        // Item not sorted high enough, nothing collected, discard staged item
        __update_refmap_head_tail( collector, NULL, stage, NULL, NULL );
      }
    }
    else {
      Cm256iList_t *list = collector->container.sequence.list;
      // Append staged item (already in refmap) to unsorted list
      if( CALLABLE(list)->Append( list, &stage->item ) > 0 ) {
        committed = 1;
      }
      else {
        // Not appended, discard staged item
        __update_refmap_head_tail( collector, NULL, stage, NULL, NULL );
      }
    }

    // Clear staging slot (item either captured into heap/list or discarded if operation failed)
    stage->headref = NULL;
    stage->tailref = NULL;

    collector->n_collectable++;
    collector->n_remain -= committed;

    switch( collector->type & __VGX_COLLECTOR_ITEM_MASK ) {
    case __VGX_COLLECTOR_ARC:
      ((vgx_ArcCollector_context_t*)collector)->n_arcs += committed;
      break;
    case __VGX_COLLECTOR_VERTEX:
      ((vgx_VertexCollector_context_t*)collector)->n_vertices += committed;
      break;
    }
  }

  return committed;
}






#define __vertex_sortby_memaddress       (SORT.uint64.value = (uintptr_t)VERTEX);
#define __vertex_sortby_internalid       (SORT.internalid_H = __vertex_internalid( VERTEX )->H);
#define __vertex_sortby_idprefix         (SORT.qword        = *VERTEX->identifier.idprefix.qwords);
#define __vertex_sortby_tail_internalid  (SORT.internalid_H = __vertex_internalid( VERTEX )->H);
#define __vertex_sortby_tail_idprefix    (SORT.qword        = *VERTEX->identifier.idprefix.qwords);
#define __vertex_sortby_degree           (SORT.int64.value  = iarcvector.Degree( &VERTEX->inarcs ) + iarcvector.Degree( &VERTEX->outarcs ));
#define __vertex_sortby_indegree         (SORT.int64.value  = iarcvector.Degree( &VERTEX->inarcs ));
#define __vertex_sortby_outdegree        (SORT.int64.value  = iarcvector.Degree( &VERTEX->outarcs ));
#define __vertex_sortby_rankscore        (SORT              = COLLECTOR->ranker->vertex_score( COLLECTOR->ranker, VERTEX ));
#define __vertex_sortby_tmc              (SORT.uint32.value = VERTEX->TMC);
#define __vertex_sortby_tmm              (SORT.uint32.value = VERTEX->TMM);
#define __vertex_sortby_tmx              (SORT.uint32.value = VERTEX->TMX.vertex_ts);


#define __vertex_sortby_native_order   \
do {                            \
  SORT.int64.value = 0;         \
} WHILE_ZERO;


#define __vertex_sortby_random_order    \
do {                                    \
  SORT.int64.value = (int64_t)rand63(); \
} WHILE_ZERO;


/*******************************************************************//**
 * __vertex_no_collect
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __vertex_no_collect( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return 0;
}



/*******************************************************************//**
 * __vertex_no_stage
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __vertex_no_stage( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int stage_index ) {
  return 0;
}



/*******************************************************************//**
 * __push_vertex
 ***********************************************************************
 */
__inline static int __push_vertex( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_VertexSortValue_t sort ) {
  Cm256iHeap_t *heap = collector->container.sequence.heap;
  
  vgx_VertexRef_t sort_tailref = { .vertex = larc->head.vertex };
  vgx_VertexRef_t sort_headref = { .vertex = larc->head.vertex };
  vgx_CollectorItem_t collected = {
    .tailref    = &sort_tailref, // overwrite with managed reference from refmap if collected
    .headref    = &sort_headref, // overwrite with managed reference from refmap if collected
    .predicator = VGX_PREDICATOR_NONE,
    .sort       = sort
  };
  vgx_CollectorItem_t discarded;
  vgx_CollectorItem_t *push_location;

  // Collect item into heap
  if( (push_location = (vgx_CollectorItem_t*)CALLABLE(heap)->HeapPushTopK( heap, &collected.item, &discarded.item )) == NULL ) {
    // Item not sorted high enough, nothing collected
    return 0;
  }

  // New item pushed, lower sorting item discarded
  return __update_refmap_vertex( (vgx_BaseCollector_context_t*)collector, push_location, &discarded, larc );
}



/*******************************************************************//**
 * __append_vertex
 ***********************************************************************
 */
__inline static int __append_vertex( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  Cm256iList_t *list = collector->container.sequence.list;
  vgx_BaseCollector_context_t *base = (vgx_BaseCollector_context_t*)collector;

  vgx_VertexRef_t tailref = { .vertex = larc->tail };
  vgx_VertexRef_t headref = { .vertex = larc->head.vertex };
  vgx_CollectorItem_t collected = {
    .tailref    = &tailref, // overwrite with managed reference from refmap if collected
    .headref    = &headref, // overwrite with managed reference from refmap if collected
    .predicator = VGX_PREDICATOR_NONE,
    .sort       = {0}
  };

  // Manage refmap
  if( __update_refmap_vertex( base, &collected, NULL, larc ) < 1 ) {
    return -1;
  }

  // Append item to unsorted list
  if( CALLABLE(list)->Append( list, &collected.item ) < 1 ) {
    __update_refmap_vertex( base, NULL, &collected, NULL );
    return 0;
  }

  return 1;
}



/*******************************************************************//**
 * __stage_vertex
 ***********************************************************************
 */
__inline static int __stage_vertex( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_VertexSortValue_t sort ) {

  // Staging slot
  vgx_CollectorItem_t *stage = &collector->stage->slot[ index & (VGX_COLLECTOR_STAGE_SIZE-1) ];
  vgx_BaseCollector_context_t *base = (vgx_BaseCollector_context_t*)collector;

  // Discard uncommitted item if any
  vgx_CollectorItem_t discarded = *stage;
  if( discarded.headref ) { // <- if head is not NULL then item exists
    __update_refmap_vertex( base, NULL, &discarded, NULL );
  }

  // Stage item into slot
  vgx_VertexRef_t sort_tailref = { .vertex = larc->tail };
  vgx_VertexRef_t sort_headref = { .vertex = larc->head.vertex };
  stage->tailref = &sort_tailref;   // overwrite with managed reference from refmap if collected
  stage->headref = &sort_headref;   // overwrite with managed reference from refmap if collected
  stage->predicator = VGX_PREDICATOR_NONE;
  stage->sort = sort;

  // Manage refmap
  if( __update_refmap_vertex( base, stage, NULL, larc ) < 1 ) {
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 * __vertex_collect_nosort
 ***********************************************************************
 */
__inline static int __vertex_collect_nosort( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  return __append_vertex( collector, larc );
}



/*******************************************************************//**
 * __vertex_stage_unsorted
 ***********************************************************************
 */
__inline static int __vertex_stage_unsorted( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index ) {
  vgx_VertexSortValue_t nosort = {0};
  return __stage_vertex( collector, larc, index, nosort );
}



/*******************************************************************//**
 * __vertex_collect_to_sort_by_rankscore
 ***********************************************************************
 */
__inline static int __vertex_collect_to_sort_by_rankscore( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  vgx_Evaluator_t *evaluator = collector->ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalVertex( evaluator, larc->head.vertex );
  vgx_VertexSortValue_t sort = { .flt64.value = iEvaluator.GetReal( result ) };
  if( sort.flt64.value < 0 ) {
    collector->n_collectable--; // revert
    return 0;
  }
  return __push_vertex( collector, larc, sort );
}



/*******************************************************************//**
 * __vertex_stage_to_sort_by_rankscore
 ***********************************************************************
 */
__inline static int __vertex_stage_to_sort_by_rankscore( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index ) {
  vgx_Ranker_t *ranker = collector->ranker;
  if( __ranker_acquire_lockable_arc_head( ranker, larc ) < 0 ) {
    return -1;
  }
  vgx_Evaluator_t *evaluator = ranker->evaluator;
  vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalVertex( evaluator, larc->head.vertex );
  vgx_VertexSortValue_t sort = { .flt64.value = iEvaluator.GetReal( result ) };
  if( sort.flt64.value < 0 ) {
    return 0;
  }
  return __stage_vertex( collector, larc, index, sort );
}




#define __vertex_collect_TO_SORT_BY( Field ) \
__inline static int __vertex_collect_to_sort_by_##Field( vgx_VertexCollector_context_t *COLLECTOR, vgx_LockableArc_t *larc ) {   \
  vgx_VertexSortValue_t SORT;                       \
  vgx_LockableArc_t *LARC = larc;                   \
  vgx_Vertex_t *VERTEX = larc->head.vertex;

#define __end_vertex_collect                        \
  return __push_vertex( COLLECTOR, LARC, SORT );    \
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER    \
  SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED    \
}


__vertex_collect_TO_SORT_BY( memaddress )   { __vertex_sortby_memaddress    } __end_vertex_collect
__vertex_collect_TO_SORT_BY( internalid )   { __vertex_sortby_internalid    } __end_vertex_collect
__vertex_collect_TO_SORT_BY( identifier )   { __vertex_sortby_idprefix      } __end_vertex_collect
__vertex_collect_TO_SORT_BY( degree )       { __vertex_sortby_degree        } __end_vertex_collect
__vertex_collect_TO_SORT_BY( indegree )     { __vertex_sortby_indegree      } __end_vertex_collect
__vertex_collect_TO_SORT_BY( outdegree )    { __vertex_sortby_outdegree     } __end_vertex_collect
__vertex_collect_TO_SORT_BY( simscore )     { __vertex_sortby_rankscore     } __end_vertex_collect
__vertex_collect_TO_SORT_BY( hamdist )      { __vertex_sortby_rankscore     } __end_vertex_collect
__vertex_collect_TO_SORT_BY( tmc )          { __vertex_sortby_tmc           } __end_vertex_collect
__vertex_collect_TO_SORT_BY( tmm )          { __vertex_sortby_tmm           } __end_vertex_collect
__vertex_collect_TO_SORT_BY( tmx )          { __vertex_sortby_tmx           } __end_vertex_collect
__vertex_collect_TO_SORT_BY( native_order ) { __vertex_sortby_native_order  } __end_vertex_collect
__vertex_collect_TO_SORT_BY( random_order ) { __vertex_sortby_random_order  } __end_vertex_collect



#define __vertex_stage_TO_SORT_BY( Field )                          \
__inline static int __vertex_stage_to_sort_by_##Field( vgx_VertexCollector_context_t *COLLECTOR, vgx_LockableArc_t *larc, int INDEX ) { \
  vgx_VertexSortValue_t SORT;                                       \
  vgx_LockableArc_t *LARC = larc;                                   \
  vgx_Vertex_t *VERTEX = larc->head.vertex;                         \
  vgx_Ranker_t *ranker = COLLECTOR->ranker;                         \
  if( ranker ) {                                                    \
    if( __ranker_acquire_lockable_arc_head( ranker, larc ) < 0 ) {  \
      return -1;                                                    \
    }                                                               \
  }

  

#define __end_vertex_stage                                \
  return __stage_vertex( COLLECTOR, LARC, INDEX, SORT );  \
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER          \
  SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED          \
}


__vertex_stage_TO_SORT_BY( memaddress )      { __vertex_sortby_memaddress       } __end_vertex_stage
__vertex_stage_TO_SORT_BY( internalid )      { __vertex_sortby_internalid       } __end_vertex_stage
__vertex_stage_TO_SORT_BY( identifier )      { __vertex_sortby_idprefix         } __end_vertex_stage
__vertex_stage_TO_SORT_BY( tail_internalid ) { __vertex_sortby_tail_internalid  } __end_vertex_stage
__vertex_stage_TO_SORT_BY( tail_identifier ) { __vertex_sortby_tail_idprefix    } __end_vertex_stage
__vertex_stage_TO_SORT_BY( degree )          { __vertex_sortby_degree           } __end_vertex_stage
__vertex_stage_TO_SORT_BY( indegree )        { __vertex_sortby_indegree         } __end_vertex_stage
__vertex_stage_TO_SORT_BY( outdegree )       { __vertex_sortby_outdegree        } __end_vertex_stage
__vertex_stage_TO_SORT_BY( simscore )        { if( ranker ) {__vertex_sortby_rankscore} else {SORT.qword = 0;} } __end_vertex_stage
__vertex_stage_TO_SORT_BY( hamdist )         { if( ranker ) {__vertex_sortby_rankscore} else {SORT.qword = 0;} } __end_vertex_stage
__vertex_stage_TO_SORT_BY( vertex_tmc )      { __vertex_sortby_tmc              } __end_vertex_stage
__vertex_stage_TO_SORT_BY( vertex_tmm )      { __vertex_sortby_tmm              } __end_vertex_stage
__vertex_stage_TO_SORT_BY( vertex_tmx )      { __vertex_sortby_tmx              } __end_vertex_stage
__vertex_stage_TO_SORT_BY( native_order )    { __vertex_sortby_native_order     } __end_vertex_stage
__vertex_stage_TO_SORT_BY( random_order )    { __vertex_sortby_random_order     } __end_vertex_stage




/*******************************************************************//**
 ***********************************************************************
 ***  COMPARATOR FUNCTIONS
 ***  (direct data-type)
 ***********************************************************************
 ***********************************************************************
 */

/*******************************************************************//**
 * __cmp_a_gt_b( a, b )
 * Evaluates to:
 *                1 : if a > b
 *                0 : if a == b
 *               -1 : if a < b
 ***********************************************************************
 */
#define __cmp_a_gt_b( a, b ) (int)( (a > b) - (a < b) )



/**************************************************************************//**
 * __cmp_always_1
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __cmp_always_1( const vgx_CollectorItem_t *a, const vgx_CollectorItem_t *b ) {
  return 1;
}


/*******************************************************************//**
 * __cmp_char
 ***********************************************************************
 */
__inline static int __cmp_char( const char a, const char b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_uchar
 ***********************************************************************
 */
__inline static int __cmp_uchar( const unsigned char a, const unsigned char b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_int16
 ***********************************************************************
 */
__inline static int __cmp_int16( const int16_t a, const int16_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_uint16
 ***********************************************************************
 */
__inline static int __cmp_uint16( const uint16_t a, const uint16_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_int32
 ***********************************************************************
 */
__inline static int __cmp_int32( const int32_t a, const int32_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_int64
 ***********************************************************************
 */
__inline static int __cmp_int64( const int64_t a, const int64_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_uint32
 ***********************************************************************
 */
__inline static int __cmp_uint32( const uint32_t a, const uint32_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_uint64
 ***********************************************************************
 */
__inline static int __cmp_uint64( const uint64_t a, const uint64_t b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_float
 ***********************************************************************
 */
__inline static int __cmp_float( const float a, const float b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_double
 ***********************************************************************
 */
__inline static int __cmp_double( const double a, const double b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_address
 ***********************************************************************
 */
__inline static int __cmp_address( const void *a, const void *b ) {
  return __cmp_a_gt_b( a, b );
}

/*******************************************************************//**
 * __cmp_internalid
 ***********************************************************************
 */
__inline static int __cmp_internalid( const vgx_CollectorItem_t *a, const vgx_CollectorItem_t *b ) {
  int cmp = __cmp_a_gt_b( a->sort.internalid_H, b->sort.internalid_H );
  if( cmp != 0 ) {
    return cmp;
  }
  else {
    // Tie breaker when upper 64-bits are equal. This is very rare. (Expensive since vertex pointers have to be dereferenced)
    return idcmp( __vertex_internalid( a->headref->vertex ), __vertex_internalid( b->headref->vertex ) );
  }
}

/*******************************************************************//**
 * __cmp_tail_internalid
 ***********************************************************************
 */
__inline static int __cmp_tail_internalid( const vgx_CollectorItem_t *a, const vgx_CollectorItem_t *b ) {
  int cmp = __cmp_a_gt_b( a->sort.internalid_H, b->sort.internalid_H );
  if( cmp != 0 ) {
    return cmp;
  }
  else {
    // Tie breaker when upper 64-bits are equal. This is very rare. (Expensive since vertex pointers have to be dereferenced)
    return idcmp( __vertex_internalid( a->tailref->vertex ), __vertex_internalid( b->tailref->vertex ) );
  }
}

/*******************************************************************//**
 * __cmp_vertex_identifier
 ***********************************************************************
 */
__inline static int __cmp_vertex_identifier( const vgx_Vertex_t *a, const vgx_Vertex_t *b ) {
  // Dereference both vertices and get their identifier fields
  const vgx_VertexIdentifier_t *id_a = &a->identifier;
  const vgx_VertexIdentifier_t *id_b = &b->identifier;

  // Compare the embedded character string first, this will be conclusive in all but the most extreme cases. 
  int prefix_cmp = strncmp( id_a->idprefix.data, id_b->idprefix.data, sizeof( vgx_VertexIdentifierPrefix_t ) );
  if( prefix_cmp != 0 ) {
    return prefix_cmp;
  }
  // First 56 characters (embedded prefixes) are the same, tie-breaker needed, we must dereference a second time to the CString instances.
  else {
    // The same string instance is being compared
    if( id_a->CSTR__idstr == id_b->CSTR__idstr ) {
      return 0; // a === b
    }
    // Both identifiers have pointers to CStrings (if here, all but the most extreme edge cases will not meet this condition)
    else if( id_a->CSTR__idstr != NULL && id_b->CSTR__idstr != NULL ) {
      return CStringCompare( id_a->CSTR__idstr, id_b->CSTR__idstr );
    }
    // Only A has a CString. This means both prefixes match but B's ID was exactly the length of the prefix and does not have a CString.
    // By definition A must then sort AFTER B.
    else if( id_a->CSTR__idstr ) {
      return 1; // a > b
    }
    // Only B has a CString. This means both prefixes match but A's ID was exactly the length of the prefix and does not have a CString.
    // By definition A must then sort BEFORE B.
    else if( id_b->CSTR__idstr ) {
      return -1; // a < b
    }
    // Neither A nor B have CStrings. This means both IDs are exactly large enough to fit in the prefix and they are equal
    else {
      return 0; // a == b
    }
  }
}



/*******************************************************************//**
 * __cmp_identifier_fast
 ***********************************************************************
 */
__inline static int __cmp_identifier_fast( const vgx_CollectorItem_t *a, const vgx_CollectorItem_t *b ) {
  // quick prefix comparison (because mem is local)
  if( a->sort.qword != b->sort.qword ) {
    return strncmp( a->sort.prefix_string, b->sort.prefix_string, 8 );
  }
  // slow full string comparison (because we must de-reference two vertex pointers)
  else {
    return __cmp_vertex_identifier( a->headref->vertex, b->headref->vertex );
  }
}



/*******************************************************************//**
 * __cmp_tail_identifier_fast
 ***********************************************************************
 */
__inline static int __cmp_tail_identifier_fast( const vgx_CollectorItem_t *a, const vgx_CollectorItem_t *b ) {
  // quick prefix comparison (because mem is local)
  if( a->sort.qword != b->sort.qword ) {
    return strncmp( a->sort.prefix_string, b->sort.prefix_string, 8 );
  }
  // slow full string comparison (because we must de-reference two vertex pointers)
  else {
    return __cmp_vertex_identifier( a->tailref->vertex, b->tailref->vertex );
  }
}



/*******************************************************************//**
 * Collector Item sort fields
 ***********************************************************************
 */
#define __arc_head( item )          (item->headref->vertex)
#define __vertex( item )            (item->headref->vertex)
#define __rank_int32( item )        (item->sort.int32.value)
#define __rank_uint32( item )       (item->sort.uint32.value)
#define __rank_int64( item )        (item->sort.int64.value)
#define __rank_uint64( item )       (item->sort.uint64.value)
#define __rank_flt32( item )        (item->sort.flt32.value)
#define __rank_flt64( item )        (item->sort.flt64.value)


/*******************************************************************//**
 * Arc comparators
 ***********************************************************************
 */
__inline static int __cmp_archead_always_1(        const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_always_1( a1, a2 ); }

__inline static int __cmp_archead_memaddress_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_address( __arc_head(a1), __arc_head(a2) ); }
__inline static int __cmp_archead_memaddress_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_address( __arc_head(a2), __arc_head(a1) ); }

__inline static int __cmp_archead_internalid_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_internalid( a1, a2 ); }
__inline static int __cmp_archead_internalid_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_internalid( a2, a1 ); }

__inline static int __cmp_arctail_internalid_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_tail_internalid( a1, a2 ); }
__inline static int __cmp_arctail_internalid_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_tail_internalid( a2, a1 ); }

__inline static int __cmp_archead_identifier_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_identifier_fast( a1, a2 ); }
__inline static int __cmp_archead_identifier_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_identifier_fast( a2, a1 ); }

__inline static int __cmp_arctail_identifier_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_tail_identifier_fast( a1, a2 ); }
__inline static int __cmp_arctail_identifier_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_tail_identifier_fast( a2, a1 ); }

__inline static int __cmp_archead_int32_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_int32( __rank_int32(a1), __rank_int32(a2) ); }
__inline static int __cmp_archead_int32_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_int32( __rank_int32(a2), __rank_int32(a1) ); }

__inline static int __cmp_archead_uint32_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_uint32( __rank_uint32(a1), __rank_uint32(a2) ); }
__inline static int __cmp_archead_uint32_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_uint32( __rank_uint32(a2), __rank_uint32(a1) ); }

__inline static int __cmp_archead_int64_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_int64( __rank_int64(a1), __rank_int64(a2) ); }
__inline static int __cmp_archead_int64_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_int64( __rank_int64(a2), __rank_int64(a1) ); }

__inline static int __cmp_archead_uint64_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_uint64( __rank_uint64(a1), __rank_uint64(a2) ); }
__inline static int __cmp_archead_uint64_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_uint64( __rank_uint64(a2), __rank_uint64(a1) ); }

__inline static int __cmp_archead_float_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_float( __rank_flt32(a1), __rank_flt32(a2) ); }
__inline static int __cmp_archead_float_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_float( __rank_flt32(a2), __rank_flt32(a1) ); }

__inline static int __cmp_archead_double_rank_max(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_double( __rank_flt64(a1), __rank_flt64(a2) ); }
__inline static int __cmp_archead_double_rank_min(  const vgx_CollectorItem_t *a1, const vgx_CollectorItem_t *a2 ) { return __cmp_double( __rank_flt64(a2), __rank_flt64(a1) ); }



/*******************************************************************//**
 * Vertex comparators
 ***********************************************************************
 */
__inline static int __cmp_vertex_always_1(        const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_always_1( v1, v2 ); }

__inline static int __cmp_vertex_memaddress_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_address( __vertex(v1), __vertex(v2) ); }
__inline static int __cmp_vertex_memaddress_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_address( __vertex(v2), __vertex(v1) ); }

__inline static int __cmp_vertex_internalid_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_internalid( v1, v2 ); }
__inline static int __cmp_vertex_internalid_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_internalid( v2, v1 ); }

__inline static int __cmp_vertex_identifier_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_identifier_fast( v1, v2 ); }
__inline static int __cmp_vertex_identifier_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_identifier_fast( v2, v1 ); }

__inline static int __cmp_vertex_int32_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_int32( __rank_int32(v1), __rank_int32(v2) ); }
__inline static int __cmp_vertex_int32_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_int32( __rank_int32(v2), __rank_int32(v1) ); }

__inline static int __cmp_vertex_uint32_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_uint32( __rank_uint32(v1), __rank_uint32(v2) ); }
__inline static int __cmp_vertex_uint32_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_uint32( __rank_uint32(v2), __rank_uint32(v1) ); }

__inline static int __cmp_vertex_int64_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_int64( __rank_int64(v1), __rank_int64(v2) ); }
__inline static int __cmp_vertex_int64_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_int64( __rank_int64(v2), __rank_int64(v1) ); }

__inline static int __cmp_vertex_uint64_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_uint64( __rank_uint64(v1), __rank_uint64(v2) ); }
__inline static int __cmp_vertex_uint64_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_uint64( __rank_uint64(v2), __rank_uint64(v1) ); }

__inline static int __cmp_vertex_float_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_float( __rank_flt32(v1), __rank_flt32(v2) ); }
__inline static int __cmp_vertex_float_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_float( __rank_flt32(v2), __rank_flt32(v1) ); }

__inline static int __cmp_vertex_double_rank_max(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_double( __rank_flt64(v1), __rank_flt64(v2) ); }
__inline static int __cmp_vertex_double_rank_min(  const vgx_CollectorItem_t *v1, const vgx_CollectorItem_t *v2 ) { return __cmp_double( __rank_flt64(v2), __rank_flt64(v1) ); }



/*******************************************************************//**
 * Rank score from sort value
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static double __rank_score_from_none(        const vgx_CollectorItem_t *x ) { return 0.0; }
__inline static double __rank_score_from_predicator(  const vgx_CollectorItem_t *x ) { return _vgx_predicator_get_value_as_float( x->predicator ); }
__inline static double __rank_score_from_int32(       const vgx_CollectorItem_t *x ) { return (double)x->sort.int32.value; }
__inline static double __rank_score_from_int64(       const vgx_CollectorItem_t *x ) { return (double)x->sort.int64.value; }
__inline static double __rank_score_from_uint32(      const vgx_CollectorItem_t *x ) { return (double)x->sort.uint32.value; }
__inline static double __rank_score_from_uint64(      const vgx_CollectorItem_t *x ) { return (double)x->sort.uint64.value; }
__inline static double __rank_score_from_float(       const vgx_CollectorItem_t *x ) { return x->sort.flt32.value; }
__inline static double __rank_score_from_double(      const vgx_CollectorItem_t *x ) { return x->sort.flt64.value; }
__inline static double __rank_score_from_qword(       const vgx_CollectorItem_t *x ) { return (double)x->sort.qword; }




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_comparator.h"


test_descriptor_t _vgx_vxarcvector_comparator_tests[] = {
  { "VGX Arcvector Comparator Tests",     __utest_vxarcvector_comparator },
  {NULL}
};
#endif
