/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxcollector.h
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

#ifndef _VGX_VXCOLLECTOR_H
#define _VGX_VXCOLLECTOR_H

#include "_vgx.h"





static void __initialize_cmp_vertices( void );
static f_vgx_VertexComparator __get_vertex_comparator( const vgx_ranking_context_t *ranking_context, vgx_CollectorItem_t *lowest );
static void __get_vertex_collector_functions( const vgx_ranking_context_t *ranking_context, f_vgx_StageVertex *stagef, f_vgx_CollectVertex *collectf );
static f_vgx_ArcComparator __get_arc_comparator( const vgx_ranking_context_t *ranking_context, vgx_CollectorItem_t *lowest );
static void __get_arc_collector_functions_by_sortspec( const vgx_sortspec_t sortspec, const vgx_predicator_mod_t modifier, f_vgx_StageArc *stagef, f_vgx_CollectArc *collectf );
static void __get_arc_collector_functions( const vgx_ranking_context_t *ranking_context, f_vgx_StageArc *stagef, f_vgx_CollectArc *collectf );
static f_vgx_ComputeArcDynamicRank __get_arc_rank_scorer_function( const vgx_ranking_context_t *ranking_context );
static f_vgx_ComputeVertexDynamicRank __get_vertex_rank_scorer_function( const vgx_ranking_context_t *ranking_context );





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool cmp_vertices_initialized = false;
static vgx_AllocatedVertex_t _aV_high;
static vgx_AllocatedVertex_t _aV_low;
static const size_t sz_id_prefix = sizeof( vgx_VertexIdentifierPrefix_t ) - 1;
static vgx_Vertex_t *V_high = NULL;
static vgx_Vertex_t *V_low = NULL;
static vgx_VertexRef_t Vref_high;
static vgx_VertexRef_t Vref_low;


/*******************************************************************//**
 *
 * CURRENT LIMITATION 1: ASCII SORT ONLY
 * CURRENT LIMITATION 2: SORTING ON FIRST 1023 (ASCII) CHARACTERS ONLY
 ***********************************************************************
 */
static void __initialize_cmp_vertices( void ) {
  char highest_string[1024];
  for( int i=0; i<1023; i++ ) {
    highest_string[i] = (char)255;
  }
  highest_string[1023] = '\0';

  // high
  _aV_high.vtable = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );
  _aV_high.typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Vertex_t );
  _aV_high.descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_INVALID;
  _aV_high.descriptor.property.degree.in = 1;
  _aV_high.descriptor.property.degree.out = 1;
  _aV_high.descriptor.property.vector.ctr = 0;
  _aV_high.descriptor.property.vector.vec = 0;
  _aV_high.descriptor.property.scope.def = 0;
  _aV_high.descriptor.property.index.type = 0;
  _aV_high.descriptor.property.index.main = 0;
  _aV_high.descriptor.property.event.sch = 0;
  iOperation.InitId( &_aV_high.operation, (1ULL << 56) - 1 );
  _aV_high.graph = NULL;
  _aV_high.vector = NULL;
  _aV_high.rank = vgx_Rank_INIT_SET( FLT_MAX, FLT_MAX );
  _aV_high.inarcs.VxD.tag.value = VGX_ARCVECTOR_VxD_DEGREE;
  _aV_high.inarcs.VxD.data.ival56 = (LLONG_MAX >> 8);
  _aV_high.outarcs.VxD.tag.value = VGX_ARCVECTOR_VxD_DEGREE;
  _aV_high.outarcs.VxD.data.ival56 = (LLONG_MAX >> 8);
  memset( _aV_high.identifier.idprefix.data, 255, sz_id_prefix );
  _aV_high.identifier.idprefix.data[ sz_id_prefix ] = '\0';
  _aV_high.identifier.CSTR__idstr = CStringNew( highest_string ); // (malloc'd memory will be leaked since we never un-initialize the cmp vertices)
  _aV_high.TMX.vertex_ts = CXLIB_ULONG_MAX;
  _aV_high.TMX.arc_ts = CXLIB_ULONG_MAX;
  _aV_high.TMC = CXLIB_ULONG_MAX;
  _aV_high.TMM = CXLIB_ULONG_MAX;
  V_high = (vgx_Vertex_t*)&_aV_high.object;
  _cxmalloc_linehead_from_object( V_high )->data.q3 = ULLONG_MAX;
  _cxmalloc_linehead_from_object( V_high )->data.refc = INT_MAX;
  _cxmalloc_linehead_from_object( V_high )->data.size = UINT_MAX;
  idset( COMLIB_OBJECT_GETID( V_high ), ULLONG_MAX, ULLONG_MAX );
  Vref_high.vertex = V_high;
  Vref_high.refcnt = -1;
  Vref_high.slot.locked = 0;
  Vref_high.slot.state = VGX_VERTEXREF_STATE_INVALID;


  // low
  _aV_low.vtable = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );
  _aV_low.typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Vertex_t );
  _aV_low.descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_INVALID;
  _aV_low.descriptor.property.degree.in = 1;   // need this to pick up the "-1" value
  _aV_low.descriptor.property.degree.out = 1;  // ..../
  _aV_low.descriptor.property.vector.ctr = 0;
  _aV_low.descriptor.property.vector.vec = 0;  // TODO: how can we represent a vector that is always "less" than when doing similarity against another (even nullvec) ?
  _aV_low.descriptor.property.scope.def = 0;
  _aV_low.descriptor.property.index.type = 0;
  _aV_low.descriptor.property.index.main = 0;
  _aV_low.descriptor.property.event.sch = 0;
  iOperation.InitId( &_aV_low.operation, 0 );
  _aV_low.graph = NULL;
  _aV_low.vector = NULL;
  _aV_low.rank = vgx_Rank_INIT_SET( 0.0, 0.0 );
  _aV_low.inarcs.VxD.tag.value = VGX_ARCVECTOR_VxD_DEGREE;   // Artificial values
  _aV_low.inarcs.VxD.data.ival56 = -1;                       // that cannot occur
  _aV_low.outarcs.VxD.tag.value = VGX_ARCVECTOR_VxD_DEGREE;  // in any actual
  _aV_low.outarcs.VxD.data.ival56 = -1;                      // vertex
  memset( _aV_low.identifier.idprefix.data, '\0', sz_id_prefix + 1);
  _aV_low.identifier.CSTR__idstr = NULL;
  _aV_low.TMX.vertex_ts = 0;
  _aV_low.TMX.arc_ts = 0;
  _aV_low.TMC = 0;
  _aV_low.TMM = 0;
  V_low = (vgx_Vertex_t*)&_aV_low.object;
  _cxmalloc_linehead_from_object( V_low )->data.q3 = 0;
  _cxmalloc_linehead_from_object( V_low )->data.refc = 0;
  _cxmalloc_linehead_from_object( V_low )->data.size = 0;
  idset( COMLIB_OBJECT_GETID( V_low ), 0, 0 );
  Vref_low.vertex = V_low;
  Vref_low.refcnt = -1;
  Vref_low.slot.locked = 0;
  Vref_low.slot.state = VGX_VERTEXREF_STATE_INVALID;

  cmp_vertices_initialized = true;
}



/*******************************************************************//**
 * For the given neighborhood search return a suitable comparator function
 * to support sorting according to the sortspec.
 *
 *
 ***********************************************************************
 */
static f_vgx_VertexComparator __get_vertex_comparator( const vgx_ranking_context_t *ranking_context, vgx_CollectorItem_t *lowest ) {
  vgx_VertexComparator_t icomparator;
  vgx_Vertex_t *low_vertex;
  vgx_VertexRef_t *low_vertex_ref;
  
  if( ranking_context ) {
    // Simplify the logic below using a dummy destination for lowest if caller didn't supply one
    vgx_CollectorItem_t local_lowest;
    if( lowest == NULL ) {
      lowest = &local_lowest;
    }

    vgx_Vertex_t *V = NULL;

    if( !cmp_vertices_initialized ) {
      __initialize_cmp_vertices();
    }

    bool sort_ascending = _vgx_sort_direction( ranking_context->sortspec ) == VGX_SORT_DIRECTION_ASCENDING;

    switch( _vgx_sort_direction( ranking_context->sortspec ) ) {
    case VGX_SORT_DIRECTION_ASCENDING:
      icomparator = _iVertexMaxComparator;
      low_vertex = V_high;
      low_vertex_ref = &Vref_high;
      break;
    case VGX_SORT_DIRECTION_DESCENDING:
      icomparator = _iVertexMinComparator;
      low_vertex= V_low;
      low_vertex_ref = &Vref_low;
      break;

    default:
      goto nosort;
    }

    V = low_vertex;

    lowest->tailref = low_vertex_ref;
    lowest->headref = low_vertex_ref;
    
    switch( _vgx_sortby( ranking_context->sortspec ) ) {
    case VGX_SORTBY_NONE:
      goto nosort;

    case VGX_SORTBY_MEMADDRESS:
      lowest->sort.uint64.value = sort_ascending ? ULLONG_MAX : 0;
      return icomparator.cmp_vertex_uint64_rank;

    case VGX_SORTBY_INTERNALID:
      /* FALLTHRU */
    case VGX_SORTBY_ANCHOR_OBID:
      lowest->sort.internalid_H = ((objectid_t*)COMLIB_OBJECT_GETID( V ))->H;
      return icomparator.cmp_vertex_internalid;

    case VGX_SORTBY_IDSTRING:
      /* FALLTHRU */
    case VGX_SORTBY_ANCHOR_ID:
      lowest->sort.qword = V ? CALLABLE(V)->Identifier(V)->idprefix.qwords[0] : 0;
      return icomparator.cmp_vertex_identifier;

    case VGX_SORTBY_DEGREE:
      lowest->sort.int64.value = CALLABLE(V)->Degree(V);
      return icomparator.cmp_vertex_int64_rank;

    case VGX_SORTBY_INDEGREE:
      lowest->sort.int64.value = CALLABLE(V)->InDegree(V);
      return icomparator.cmp_vertex_int64_rank;

    case VGX_SORTBY_OUTDEGREE:
      lowest->sort.int64.value = CALLABLE(V)->OutDegree(V);
      return icomparator.cmp_vertex_int64_rank;

    case VGX_SORTBY_SIMSCORE:
      lowest->sort.flt64.value = sort_ascending ? DBL_MAX : -DBL_MAX;
      return icomparator.cmp_vertex_double_rank;

    case VGX_SORTBY_HAMDIST:
      lowest->sort.int64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_vertex_int64_rank;

    case VGX_SORTBY_RANKING:
      lowest->sort.flt64.value = sort_ascending ? INFINITY : -INFINITY;
      return icomparator.cmp_vertex_double_rank;

    case VGX_SORTBY_TMC:
      lowest->sort.uint32.value = CALLABLE(V)->CreationTime(V);
      return icomparator.cmp_vertex_uint32_rank;

    case VGX_SORTBY_TMM:
      lowest->sort.uint32.value = CALLABLE(V)->ModificationTime(V);
      return icomparator.cmp_vertex_uint32_rank;

    case VGX_SORTBY_TMX:
      lowest->sort.uint32.value = CALLABLE(V)->GetExpirationTime(V);
      return icomparator.cmp_vertex_uint32_rank;

    case VGX_SORTBY_NATIVE:
      lowest->sort.int64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_vertex_int64_rank;

    case VGX_SORTBY_RANDOM:
      lowest->sort.uint64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_vertex_int64_rank;

    /* MEMORY ADDRESS BY DEFAULT */
    default:
      lowest->sort.uint64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_vertex_int64_rank;
    }
  }
nosort:
  lowest->tailref = NULL;
  lowest->headref = NULL;
  lowest->predicator = VGX_PREDICATOR_NONE;
  lowest->sort.qword = 0;
  return NULL;
}



/*******************************************************************//**
 * For the given neighborhood search return a suitable comparator function
 * to support sorting according to the sortspec.
 *
 *
 ***********************************************************************
 */
static f_vgx_ArcComparator __get_arc_comparator( const vgx_ranking_context_t *ranking_context, vgx_CollectorItem_t *lowest ) {
  vgx_ArcComparator_t icomparator;
  vgx_Vertex_t *low_vertex;
  vgx_VertexRef_t *low_vertex_ref;
  
  if( ranking_context ) {

    DWORD *pmin = NULL, *pmax = NULL;
    
    // Simplify the logic below using a dummy destination for lowest if caller didn't supply one
    vgx_CollectorItem_t local_lowest;
    if( lowest == NULL ) {
      lowest = &local_lowest;
    }

    DWORD *plow_predicator = &lowest->predicator.val.bits;
    vgx_Vertex_t *V = NULL;

    if( !cmp_vertices_initialized ) {
      __initialize_cmp_vertices();
    }

    bool sort_ascending = (_vgx_sort_direction( ranking_context->sortspec ) == VGX_SORT_DIRECTION_ASCENDING);

    switch( _vgx_sort_direction( ranking_context->sortspec ) ) {
    case VGX_SORT_DIRECTION_ASCENDING:
      icomparator = _iArcMaxComparator;
      low_vertex = V_high;
      low_vertex_ref = &Vref_high;
      pmax = plow_predicator;
      break;
    case VGX_SORT_DIRECTION_DESCENDING:
      icomparator = _iArcMinComparator;
      low_vertex = V_low;
      low_vertex_ref = &Vref_low;
      pmin = plow_predicator;
      break;

    default:
      goto nosort;
    }

    V = low_vertex;

    lowest->tailref = low_vertex_ref;
    lowest->headref = low_vertex_ref;

    vgx_predicator_val_type pval_type = _vgx_predicator_value_range( pmin, pmax, ranking_context->modifier.bits );
    
    switch( _vgx_sortby( ranking_context->sortspec ) ) {

    case VGX_SORTBY_NONE:
      goto nosort;

    case VGX_SORTBY_PREDICATOR:
      switch( pval_type ) {
      case VGX_PREDICATOR_VAL_TYPE_UNITY:
        /* FALLTHRU */
      case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
        /* FALLTHRU */
      case VGX_PREDICATOR_VAL_TYPE_INTEGER:
        if( pmin ) {
          lowest->sort.int64.value = LLONG_MIN;
        }
        if( pmax ) {
          lowest->sort.int64.value = LLONG_MAX;
        }
        return icomparator.cmp_archead_int64_rank;
      case VGX_PREDICATOR_VAL_TYPE_NONE:
        /* FALLTHRU */
      case VGX_PREDICATOR_VAL_TYPE_REAL:
        if( pmin ) {
          lowest->sort.flt64.value = -DBL_MAX;
        }
        if( pmax ) {
          lowest->sort.flt64.value = DBL_MAX;
        }
        return icomparator.cmp_archead_double_rank;
      default:
        goto nosort;
      }

    case VGX_SORTBY_MEMADDRESS:
      lowest->sort.uint64.value = sort_ascending ? ULLONG_MAX : 0;
      return icomparator.cmp_archead_uint64_rank;

    case VGX_SORTBY_ANCHOR_OBID:
      lowest->sort.internalid_H = ((objectid_t*)COMLIB_OBJECT_GETID( V ))->H;
      return icomparator.cmp_arctail_internalid;

    case VGX_SORTBY_INTERNALID:
      lowest->sort.internalid_H = ((objectid_t*)COMLIB_OBJECT_GETID( V ))->H;
      return icomparator.cmp_archead_internalid;

    case VGX_SORTBY_ANCHOR_ID:
      lowest->sort.qword = V ? CALLABLE(V)->Identifier(V)->idprefix.qwords[0] : 0;
      return icomparator.cmp_arctail_identifier;

    case VGX_SORTBY_IDSTRING:
      lowest->sort.qword = V ? CALLABLE(V)->Identifier(V)->idprefix.qwords[0] : 0;
      return icomparator.cmp_archead_identifier;

    case VGX_SORTBY_DEGREE:
      lowest->sort.int64.value = CALLABLE(V)->Degree(V);
      return icomparator.cmp_archead_int64_rank;

    case VGX_SORTBY_INDEGREE:
      lowest->sort.int64.value = CALLABLE(V)->InDegree(V);
      return icomparator.cmp_archead_int64_rank;

    case VGX_SORTBY_OUTDEGREE:
      lowest->sort.int64.value = CALLABLE(V)->OutDegree(V);
      return icomparator.cmp_archead_int64_rank;

    case VGX_SORTBY_SIMSCORE:
      lowest->sort.flt64.value = sort_ascending ? DBL_MAX : -DBL_MAX;
      return icomparator.cmp_archead_double_rank;

    case VGX_SORTBY_HAMDIST:
      lowest->sort.int64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_archead_int64_rank;

    case VGX_SORTBY_RANKING:
      lowest->sort.flt64.value = sort_ascending ? INFINITY : -INFINITY;
      return icomparator.cmp_archead_double_rank;

    case VGX_SORTBY_TMC:
      lowest->sort.uint32.value = CALLABLE(V)->CreationTime(V);
      return icomparator.cmp_archead_uint32_rank;

    case VGX_SORTBY_TMM:
      lowest->sort.uint32.value = CALLABLE(V)->ModificationTime(V);
      return icomparator.cmp_archead_uint32_rank;

    case VGX_SORTBY_TMX:
      lowest->sort.uint32.value = CALLABLE(V)->GetExpirationTime(V);
      return icomparator.cmp_archead_uint32_rank;

    case VGX_SORTBY_NATIVE:
      lowest->sort.int64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_archead_int64_rank;
    
    case VGX_SORTBY_RANDOM:
      lowest->sort.int64.value = sort_ascending ? LLONG_MAX : LLONG_MIN;
      return icomparator.cmp_archead_int64_rank;

    default:
      goto nosort;
    }
  }
nosort:
  lowest->tailref = NULL;
  lowest->headref = NULL;
  lowest->predicator = VGX_PREDICATOR_NONE;
  lowest->sort.qword = 0;
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __get_vertex_collector_functions( const vgx_ranking_context_t *ranking_context, f_vgx_StageVertex *stagef, f_vgx_CollectVertex *collectf ) {
  if( ranking_context ) {
    switch( _vgx_sortby( ranking_context->sortspec ) ) {
    case VGX_SORTBY_NONE:
      *collectf = _iCollectVertex.into_unsorted_list;
      *stagef = _iStageVertex.unsorted;
      return;
    case VGX_SORTBY_MEMADDRESS:
      *collectf = _iCollectVertex.to_sort_by_memaddress;
      *stagef = _iStageVertex.to_sort_by_memaddress;
      return;
    case VGX_SORTBY_ANCHOR_OBID:
      /* FALLTHRU */
    case VGX_SORTBY_INTERNALID:
      *collectf = _iCollectVertex.to_sort_by_internalid;
      *stagef = _iStageVertex.to_sort_by_internalid;
      return;
    case VGX_SORTBY_ANCHOR_ID:
      /* FALLTHRU */
    case VGX_SORTBY_IDSTRING:
      *collectf = _iCollectVertex.to_sort_by_identifier;
      *stagef = _iStageVertex.to_sort_by_identifier;
      return;
    case VGX_SORTBY_DEGREE:
      *collectf = _iCollectVertex.to_sort_by_degree;
      *stagef = _iStageVertex.to_sort_by_degree;
      return;
    case VGX_SORTBY_INDEGREE:
      *collectf = _iCollectVertex.to_sort_by_indegree;
      *stagef = _iStageVertex.to_sort_by_indegree;
      return;
    case VGX_SORTBY_OUTDEGREE:
      *collectf = _iCollectVertex.to_sort_by_outdegree;
      *stagef = _iStageVertex.to_sort_by_outdegree;
      return;
    case VGX_SORTBY_SIMSCORE:
      *collectf = _iCollectVertex.to_sort_by_simscore;
      *stagef = _iStageVertex.to_sort_by_simscore;
      return;
    case VGX_SORTBY_HAMDIST:
      *collectf = _iCollectVertex.to_sort_by_hamdist;
      *stagef = _iStageVertex.to_sort_by_hamdist;
      return;
    case VGX_SORTBY_RANKING:
      *collectf = _iCollectVertex.to_sort_by_rankscore;
      *stagef = _iStageVertex.to_sort_by_rankscore;
      return;
    case VGX_SORTBY_TMC:
      *collectf = _iCollectVertex.to_sort_by_tmc;
      *stagef = _iStageVertex.to_sort_by_tmc;
      return;
    case VGX_SORTBY_TMM:
      *collectf = _iCollectVertex.to_sort_by_tmm;
      *stagef = _iStageVertex.to_sort_by_tmm;
      return;
    case VGX_SORTBY_TMX:
      *collectf = _iCollectVertex.to_sort_by_tmx;
      *stagef = _iStageVertex.to_sort_by_tmx;
      return;
    case VGX_SORTBY_NATIVE:
      *collectf = _iCollectVertex.to_sort_by_native_order;
      *stagef = _iStageVertex.to_sort_by_native_order;
      return;
    case VGX_SORTBY_RANDOM:
      *collectf = _iCollectVertex.to_sort_by_random_order;
      *stagef = _iStageVertex.to_sort_by_random_order;
      return;
    default:
      *collectf = _iCollectVertex.to_sort_by_memaddress; // default sort on address
      *stagef = _iStageVertex.to_sort_by_memaddress;
      return;
    }
  }
  else {
    *collectf = _iCollectVertex.no_collect;
    *stagef = _iStageVertex.no_stage;
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __get_arc_collector_functions_by_sortspec( const vgx_sortspec_t sortspec, const vgx_predicator_mod_t modifier, f_vgx_StageArc *stagef, f_vgx_CollectArc *collectf ) {
  switch( _vgx_sortby( sortspec ) ) {
  case VGX_SORTBY_NONE:
    *stagef   =   _iStageArc.unsorted;
    *collectf = _iCollectArc.into_unsorted_list;
    return;
  case VGX_SORTBY_PREDICATOR:
    // Specific modifier
    switch( _vgx_predicator_value_range( NULL, NULL, modifier.bits ) ) {
    case VGX_PREDICATOR_VAL_TYPE_INTEGER:
      *stagef   =   _iStageArc.to_sort_by_integer_predicator;
      *collectf = _iCollectArc.to_sort_by_integer_predicator;
      return;
    case VGX_PREDICATOR_VAL_TYPE_UNITY:
      /* FALLTHRU */
    case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
      *stagef   =   _iStageArc.to_sort_by_unsigned_predicator;
      *collectf = _iCollectArc.to_sort_by_unsigned_predicator;
      return;
    case VGX_PREDICATOR_VAL_TYPE_REAL:
      *stagef = _iStageArc.to_sort_by_real_predicator;
      *collectf = _iCollectArc.to_sort_by_real_predicator;
      return;
    default:
      // Any modifier: slower sort, convert any found value to real
      *stagef   =   _iStageArc.to_sort_by_any_predicator;
      *collectf = _iCollectArc.to_sort_by_any_predicator;
      return;
    }
  case VGX_SORTBY_MEMADDRESS:
    *stagef   =   _iStageArc.to_sort_by_memaddress;
    *collectf = _iCollectArc.to_sort_by_memaddress;
    return;
  case VGX_SORTBY_INTERNALID:
    *stagef   =   _iStageArc.to_sort_by_internalid;
    *collectf = _iCollectArc.to_sort_by_internalid;
    return;
  case VGX_SORTBY_IDSTRING:
    *stagef   =   _iStageArc.to_sort_by_identifier;
    *collectf = _iCollectArc.to_sort_by_identifier;
    return;
  case VGX_SORTBY_ANCHOR_OBID:
    *stagef   =   _iStageArc.to_sort_by_tail_internalid;
    *collectf = _iCollectArc.to_sort_by_tail_internalid;
    return;
  case VGX_SORTBY_ANCHOR_ID:
    *stagef   =   _iStageArc.to_sort_by_tail_identifier;
    *collectf = _iCollectArc.to_sort_by_tail_identifier;
    return;
  case VGX_SORTBY_DEGREE:
    *stagef   =   _iStageArc.to_sort_by_degree;
    *collectf = _iCollectArc.to_sort_by_degree;
    return;
  case VGX_SORTBY_INDEGREE:
    *stagef   =   _iStageArc.to_sort_by_indegree;
    *collectf = _iCollectArc.to_sort_by_indegree;
    return;
  case VGX_SORTBY_OUTDEGREE:
    *stagef   =   _iStageArc.to_sort_by_outdegree;
    *collectf = _iCollectArc.to_sort_by_outdegree;
    return;
  case VGX_SORTBY_SIMSCORE:
    *stagef   =   _iStageArc.to_sort_by_simscore;
    *collectf = _iCollectArc.to_sort_by_simscore;
    return;
  case VGX_SORTBY_HAMDIST:
    *stagef   =   _iStageArc.to_sort_by_hamdist;
    *collectf = _iCollectArc.to_sort_by_hamdist;
    return;
  case VGX_SORTBY_RANKING:
    *stagef   =   _iStageArc.to_sort_by_rankscore;
    *collectf = _iCollectArc.to_sort_by_rankscore;
    return;
  case VGX_SORTBY_TMC:
    *stagef   =   _iStageArc.to_sort_by_tmc;
    *collectf = _iCollectArc.to_sort_by_tmc;
    return;
  case VGX_SORTBY_TMM:
    *stagef   =   _iStageArc.to_sort_by_tmm;
    *collectf = _iCollectArc.to_sort_by_tmm;
    return;
  case VGX_SORTBY_TMX:
    *stagef   =   _iStageArc.to_sort_by_tmx;
    *collectf = _iCollectArc.to_sort_by_tmx;
    return;
  case VGX_SORTBY_NATIVE:
    *stagef   =   _iStageArc.to_sort_by_native_order;
    *collectf = _iCollectArc.to_sort_by_native_order;
    return;
  case VGX_SORTBY_RANDOM:
    *stagef   =   _iStageArc.to_sort_by_random_order;
    *collectf = _iCollectArc.to_sort_by_random_order;
    return;
  // default sort on address
  default:
    *stagef   =   _iStageArc.to_sort_by_memaddress;
    *collectf = _iCollectArc.to_sort_by_memaddress;
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __get_arc_collector_functions( const vgx_ranking_context_t *ranking_context, f_vgx_StageArc *stagef, f_vgx_CollectArc *collectf ) {
  if( ranking_context ) {
    switch( _vgx_aggregate( ranking_context->sortspec ) ) {
    case VGX_AGGREGATE_ARC_FIRST_VALUE:
      *collectf = _iCollectArc.into_first_value_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_MAX_VALUE:
      *collectf = _iCollectArc.into_max_value_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_MIN_VALUE:
      *collectf = _iCollectArc.into_min_value_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_AVERAGE_VALUE:
      *collectf = _iCollectArc.into_average_value_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_COUNT:
      *collectf = _iCollectArc.into_counting_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_ADD_VALUES:
      *collectf = _iCollectArc.into_aggregating_sum_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_ADD_SQ_VALUES:
      *collectf = _iCollectArc.into_aggregating_sqsum_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    case VGX_AGGREGATE_ARC_MULTIPLY_VALUES:
      *collectf = _iCollectArc.into_aggregating_product_map;
      *stagef = _iStageArc.no_stage; // TODO!
      return;
    default:
      __get_arc_collector_functions_by_sortspec( ranking_context->sortspec, ranking_context->modifier, stagef, collectf );
      return;
    }
  }
  else {
    *collectf = _iCollectArc.no_collect;
    *stagef = _iStageArc.no_stage;
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_vgx_ComputeArcDynamicRank __get_arc_rank_scorer_function( const vgx_ranking_context_t *ranking_context ) {
  if( ranking_context && ranking_context->sortspec != VGX_SORTBY_NONE ) {
    switch( _vgx_sortby( ranking_context->sortspec ) ) {
    case VGX_SORTBY_SIMSCORE:
      return _iComputeArcRankScore.by_simscore;
    case VGX_SORTBY_HAMDIST:
      return _iComputeArcRankScore.by_hamdist;
    case VGX_SORTBY_RANKING:
      return _iComputeArcRankScore.by_composite;
    default:
      break;
    }
  }
  return _iComputeArcRankScore.by_nothing;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_vgx_ComputeVertexDynamicRank __get_vertex_rank_scorer_function( const vgx_ranking_context_t *ranking_context ) {
  if( ranking_context && ranking_context->sortspec != VGX_SORTBY_NONE ) {
    switch( _vgx_sortby( ranking_context->sortspec ) ) {
    case VGX_SORTBY_SIMSCORE:
      return _iComputeVertexRankScore.by_simscore;
    case VGX_SORTBY_HAMDIST:
      return _iComputeVertexRankScore.by_hamdist;
    case VGX_SORTBY_RANKING:
      return _iComputeVertexRankScore.by_composite;
    default:
      break;
    }
  }
  return _iComputeVertexRankScore.by_nothing;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_vgx_RankScoreFromItem __get_rank_score_from_item_function( const vgx_sortspec_t sortspec ) {
  switch( _vgx_sortby( sortspec ) ) {
  case VGX_SORTBY_NONE:
    return _iRankScoreFromItem.from_none;
  case VGX_SORTBY_PREDICATOR:
    return _iRankScoreFromItem.from_predicator;
  case VGX_SORTBY_TMC:
  case VGX_SORTBY_TMM:
  case VGX_SORTBY_TMX:
    return _iRankScoreFromItem.from_uint32;
  case VGX_SORTBY_DEGREE:
  case VGX_SORTBY_INDEGREE:
  case VGX_SORTBY_OUTDEGREE:
  case VGX_SORTBY_HAMDIST:
  case VGX_SORTBY_NATIVE:
    return _iRankScoreFromItem.from_int64;
  case VGX_SORTBY_MEMADDRESS:
  case VGX_SORTBY_RANDOM:
    return _iRankScoreFromItem.from_uint64;
  case VGX_SORTBY_SIMSCORE:
  case VGX_SORTBY_RANKING:
    return _iRankScoreFromItem.from_double;
  case VGX_SORTBY_INTERNALID:
  case VGX_SORTBY_IDSTRING:
  case VGX_SORTBY_ANCHOR_OBID:
  case VGX_SORTBY_ANCHOR_ID:
  default:
    return _iRankScoreFromItem.from_qword;
  }
}



#endif
