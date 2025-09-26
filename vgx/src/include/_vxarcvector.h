/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxarcvector.h
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

#ifndef _VGX_VXARCVECTOR_H
#define _VGX_VXARCVECTOR_H


#include "_vgx.h"


// _vxarcvector_cellproc
DLL_HIDDEN extern int64_t _vxarcvector_cellproc__count_nactive(                           const vgx_ArcVector_cell_t *V );
DLL_HIDDEN extern int     _vxarcvector_cellproc__collect_first_cell(                      const vgx_ArcVector_cell_t *V, framehash_cell_t *first_cell );
DLL_HIDDEN extern int     _vxarcvector_cellproc__first_predicator(                        const vgx_ArcVector_cell_t *MAV, vgx_predicator_t probe, vgx_predicator_t *first_predicator );
DLL_HIDDEN extern void    _vxarcvector_cellproc__update_MAV( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, framehash_cell_t *predicator_map_pre, framehash_processing_context_t *arc_array_processor, framehash_cell_t *arc_array_fh_cell );

DLL_HIDDEN extern int     _vxarcvector_expire__expire_arcs(                             framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *vertex, uint32_t now_ts, uint32_t *next_ts, int64_t *n_deleted );
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_cellproc__collect_vertices(              const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_cellproc__collect_vertices_bidirectional( const vgx_ArcVector_cell_t *V1, const vgx_ArcVector_cell_t *V2, vgx_neighborhood_probe_t *neighborhood_probe );


DLL_HIDDEN extern int64_t _vxarcvector_cellproc__operation_sync_outarcs_CS_NT( vgx_Vertex_t *tail );



// _vxarcvector_traverse
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_traverse__traverse_arcarray(               const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_traverse__traverse_arcarray_bidirectional( const vgx_ArcVector_cell_t *V1, const vgx_ArcVector_cell_t *V2, vgx_neighborhood_probe_t *neighborhood_probe );

// _vxarcvector_exists
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_exists__multi_predicator_has_arc( framehash_cell_t *fh_top, vgx_LockableArc_t *larc, int distance, vgx_virtual_ArcFilter_context_t *filter, vgx_Arc_t *first_match );
DLL_HIDDEN extern vgx_ArcFilter_match _vxarcvector_exists__has_arc(                  const vgx_ArcVector_cell_t *V, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_Arc_t *first_match );

// _vxarcvector_delete
DLL_HIDDEN extern int     _vxarcvector_delete__delete_predicators(                   framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted );

DLL_HIDDEN extern int     _vxarcvector_delete__delete_simple_arc(                       framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted );
DLL_HIDDEN extern int     _vxarcvector_delete__delete_arcs(                             framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event, int64_t *n_deleted );




// _vxarcvector_fhash
DLL_HIDDEN extern void    _vxarcvector_fhash__trap_bad_frame_reference(            const vgx_ArcVector_cell_t *V );
DLL_HIDDEN extern int     _vxarcvector_fhash__convert_simple_arc_to_array_of_arcs( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V );
DLL_HIDDEN extern int     _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V );
DLL_HIDDEN extern int     _vxarcvector_fhash__convert_simple_arc_to_multiple_arc(  framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *simple_arc_cell );
DLL_HIDDEN extern int     _vxarcvector_fhash__convert_multiple_arc_to_simple_arc(  framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV );
DLL_HIDDEN extern vgx_ArcVector_cell_t * _vxarcvector_fhash__get_arc_cell(         framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, const vgx_Vertex_t *KEY_vertex, vgx_ArcVector_cell_t *ret_arc_cell );
DLL_HIDDEN extern vgx_predicator_t _vxarcvector_fhash__get_predicator(             framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *MAV, vgx_predicator_t KEY_predicator );
DLL_HIDDEN extern int     _vxarcvector_fhash__set_simple_arc(                      framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc );
DLL_HIDDEN extern int     _vxarcvector_fhash__set_predicator_map(                  framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *KEY_vertex, framehash_cell_t *VAL_framehash );
DLL_HIDDEN extern int     _vxarcvector_fhash__append_to_multiple_arc(              framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t predicator );
DLL_HIDDEN extern int     _vxarcvector_fhash__del_simple_arc(                      framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *KEY_vertex );
DLL_HIDDEN extern int64_t _vxarcvector_fhash__del_predicator_map(                  framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV );
DLL_HIDDEN extern int     _vxarcvector_fhash__multiple_arc_has_key(                framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t key_predicator );
DLL_HIDDEN extern int     _vxarcvector_fhash__remove_key_from_multiple_arc(        framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t key_predicator );
DLL_HIDDEN extern int     _vxarcvector_fhash__compactify(                          framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V );
DLL_HIDDEN extern int64_t _vxarcvector_fhash__discard(                             framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V );

// _vxarcvector_dispatch
DLL_HIDDEN extern int         _vxarcvector_dispatch__set_arc(           framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *arc_cell, vgx_Arc_t *set_arc );
DLL_HIDDEN extern vgx_ArcHead_t * _vxarcvector_dispatch__get_arc(       framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, vgx_ArcHead_t *probe, vgx_ArcHead_t *ret );
DLL_HIDDEN extern int         _vxarcvector_dispatch__array_add(         framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, f_Vertex_connect_event connect_event );
DLL_HIDDEN extern int         _vxarcvector_dispatch__array_remove(      framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect, int64_t *n_removed );

// _vxarcvector_serialization
DLL_HIDDEN extern int64_t     _vxarcvector_serialization__serialize(    const vgx_ArcVector_cell_t *V, CQwordQueue_t *output );
DLL_HIDDEN extern int64_t     _vxarcvector_serialization__deserialize(  vgx_Vertex_t *tail, vgx_ArcVector_cell_t *V, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *input );










/*******************************************************************//**
 * 
 * 
 * 
 * 
 * ==== CELL ENCODING ====
 * vgx_ArcVector_cell_t
 * [ tptr_t VxD ][ tptr_t FxP ]
 *
 * The tptr tags determine cell type
 * 
 * WHITE:   [ VxD EMPTY  ][ FxP EMPTY ]     =     [ 100 ][ 100 ]  NO ARCS
 * GRAY:    [ VxD DEGREE ][ FxP EMPTY ]     =     [ 111 ][ 100 ]  INDEGREE COUNTER ONLY
 * BLUE:    [ VxD VERTEX ][ FxP PRED  ]     =     [ 101 ][ 101 ]  SIMPLE ARC
 * RED:     [ VxD DEGREE ][ FxP FRAME ]     =     [ 111 ][ 000 ]  ARRAY OF ARCS
 * GREEN:   [ VxD VERTEX ][ FxP FRAME ]     =     [ 101 ][ 000 ]  MULTIPLE ARC
 * 
 * 
 ***********************************************************************
 */
__inline static _vgx_ArcVector_cell_type __arcvector_cell_type( const vgx_ArcVector_cell_t * const arc_cell ) {
  int VxD_tag = TPTR_AS_TAG( &arc_cell->VxD );  // First tptr in arc_cell
  int FxP_tag = TPTR_AS_TAG( &arc_cell->FxP );  // Second tptr in arc_cell


  switch( VxD_tag ) {
  case VGX_ARCVECTOR_VxD_EMPTY:
    // 100 100  No Arcs                 WHITE
    if( FxP_tag == VGX_ARCVECTOR_FxP_EMPTY ) {
      return VGX_ARCVECTOR_NO_ARCS; // WHITE = NO ARCS
    }
    // 100 ---
    else {
      goto error;
    }
  case VGX_ARCVECTOR_VxD_VERTEX:
    // 101 101  Simple Arc              BLUE
    if( FxP_tag == VGX_ARCVECTOR_FxP_PREDICATOR ) {
      return VGX_ARCVECTOR_SIMPLE_ARC;  // BLUE = SIMPLE ARC
    }
    // 101 000  Multiple Arc            GREEN
    else if( FxP_tag == VGX_ARCVECTOR_FxP_FRAME ) {
      return VGX_ARCVECTOR_MULTIPLE_ARC;  // GREEN = MULTIPLE ARC
    }
    // 101 ---
    else {
      goto error;
    }
  case VGX_ARCVECTOR_VxD_DEGREE:
    // 111 000  Array of Arcs           RED
    if( FxP_tag == VGX_ARCVECTOR_FxP_FRAME ) {
      return VGX_ARCVECTOR_ARRAY_OF_ARCS; // RED = ARRAY OF ARCS
    }
    // 111 100  Indegree Counter Only   GRAY
    else if( FxP_tag == VGX_ARCVECTOR_FxP_EMPTY ) {
      return VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY;
    }
    // 111 ---
    else {
      goto error;
    }
  default:
    goto error;
  }
error:
  // Invalid combination of tags found
  return VGX_ARCVECTOR_INVALID;
}




__inline static bool __arcvector_has_framepointer( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_TAG( &arc_cell->FxP ) == VGX_ARCVECTOR_FxP_FRAME;
}

__inline static void __arcvector_set_empty( vgx_ArcVector_cell_t *arc_cell ) {
  TPTR_AS_TAG( &arc_cell->VxD ) = VGX_ARCVECTOR_VxD_EMPTY;
}

__inline static bool __arcvector_is_empty( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_TAG( &arc_cell->VxD ) == VGX_ARCVECTOR_VxD_EMPTY;
}

__inline static void __arcvector_set_has_degree( vgx_ArcVector_cell_t *arc_cell ) {
  TPTR_AS_TAG( &arc_cell->VxD ) = VGX_ARCVECTOR_VxD_DEGREE;
}

__inline static bool __arcvector_has_degree( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_TAG( &arc_cell->VxD ) == VGX_ARCVECTOR_VxD_DEGREE;
}

__inline static vgx_Vertex_t * __arcvector_get_vertex( const vgx_ArcVector_cell_t *arc_cell ) {
  return (vgx_Vertex_t*)TPTR_GET_OBJ56( &arc_cell->VxD );
}

__inline static framehash_cell_t * __arcvector_as_frametop( const vgx_ArcVector_cell_t *arc_cell ) {
  return (framehash_cell_t*)TPTR_GET_POINTER( &arc_cell->FxP );
}

__inline static framehash_cell_t * __arcvector_get_frametop( const vgx_ArcVector_cell_t *arc_cell ) {
  return __arcvector_has_framepointer( arc_cell ) ? __arcvector_as_frametop( arc_cell ) : NULL;
}

__inline static void __arcvector_set_ephemeral_top( const vgx_ArcVector_cell_t *arc_cell, framehash_cell_t *eph_top ) {
  framehash_cell_t *frametop = __arcvector_get_frametop( arc_cell );
  if( frametop ) {
    APTR_COPY( eph_top, frametop );
  }
  else {
    APTR_INIT( eph_top );
  }
}

__inline static framehash_cell_t __arcvector_avcell_get_ephemeral_top( const vgx_ArcVector_cell_t *av_cell ) {
  framehash_cell_t eph_top;
  APTR_COPY( &eph_top, __arcvector_as_frametop( av_cell ) );
  return eph_top;
}

__inline static framehash_cell_t __arcvector_fhcell_get_ephemeral_top( const framehash_cell_t *fh_cell ) {
  framehash_cell_t eph_top;
  APTR_COPY( &eph_top, (framehash_cell_t*)APTR_GET_PTR56( fh_cell ) );
  return eph_top;
}

__inline static void __arcvector_set_degree( vgx_ArcVector_cell_t *arc_cell, int64_t degree ) {
  if( degree == 0 ) {
    __arcvector_set_empty( arc_cell );
  }
  else {
    __arcvector_set_has_degree( arc_cell );
    TPTR_AS_INTEGER( &arc_cell->VxD ) = degree;
  }
}

__inline static int64_t __arcvector_get_degree( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_INTEGER( &arc_cell->VxD );
}

__inline static int64_t __arcvector_inc_degree( vgx_ArcVector_cell_t *arc_cell, int64_t amount ) {
  TPTR_AS_INTEGER( &arc_cell->VxD ) += amount;
  return __arcvector_get_degree( arc_cell );
}

__inline static int64_t __arcvector_dec_degree( vgx_ArcVector_cell_t *arc_cell, int64_t amount ) {
  TPTR_AS_INTEGER( &arc_cell->VxD ) -= amount;
  return __arcvector_get_degree( arc_cell );
}

__inline static uint64_t __arcvector_as_predicator_bits( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_UNSIGNED( &arc_cell->FxP );
}

__inline static vgx_predicator_t __arcvector_as_predicator( const vgx_ArcVector_cell_t *arc_cell ) {
  vgx_predicator_t pred = {
    .data = TPTR_AS_UNSIGNED( &arc_cell->FxP )
  };
  return pred;
}


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_set_no_arc( vgx_ArcVector_cell_t *arc_cell ) {
  TPTR_SET_POINTER_AND_TAG( &arc_cell->VxD, NULL, VGX_ARCVECTOR_VxD_EMPTY );
  TPTR_SET_POINTER_AND_TAG( &arc_cell->FxP, NULL, VGX_ARCVECTOR_FxP_EMPTY );
  return arc_cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __arcvector_cell_has_no_arc( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_TAG( &arc_cell->VxD ) == VGX_ARCVECTOR_VxD_EMPTY && TPTR_AS_TAG( &arc_cell->FxP ) == VGX_ARCVECTOR_FxP_EMPTY;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_set_indegree_counter_only( vgx_ArcVector_cell_t *arc_cell, int64_t degree ) {
  TPTR_SET_INTEGER_AND_TAG( &arc_cell->VxD, degree, VGX_ARCVECTOR_VxD_DEGREE );
  TPTR_SET_POINTER_AND_TAG( &arc_cell->FxP, NULL, VGX_ARCVECTOR_FxP_EMPTY );
  return arc_cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __arcvector_cell_is_indegree_counter_only( const vgx_ArcVector_cell_t *arc_cell ) {
  return TPTR_AS_TAG( &arc_cell->VxD ) == VGX_ARCVECTOR_VxD_DEGREE && TPTR_AS_TAG( &arc_cell->FxP ) == VGX_ARCVECTOR_FxP_EMPTY;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_copy( vgx_ArcVector_cell_t *dest_arc, const vgx_ArcVector_cell_t *src_arc ) {
  TPTR_COPY( &dest_arc->VxD, &src_arc->VxD );
  TPTR_COPY( &dest_arc->FxP, &src_arc->FxP );
  return dest_arc;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_set_simple_arc( vgx_ArcVector_cell_t *arc_cell, const vgx_Arc_t *arc ) {
  // VxD: VERTEX OBJ56
  TPTR_SET_OBJ56_AND_TAG( &arc_cell->VxD, arc->head.vertex, VGX_ARCVECTOR_VxD_VERTEX );
  // FxP: PREDICATOR UINT56
  TPTR_SET_UNSIGNED_AND_TAG( &arc_cell->FxP, arc->head.predicator.data, VGX_ARCVECTOR_FxP_PREDICATOR );
  return arc_cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_set_array_of_arcs( vgx_ArcVector_cell_t *cell, int64_t degree, const framehash_cell_t *topcell ) {
  // VxD: DEGREE
  TPTR_SET_INTEGER_AND_TAG( &cell->VxD, degree, VGX_ARCVECTOR_VxD_DEGREE );
  // FxP: FRAME
  TPTR_SET_POINTER_AND_TAG( &cell->FxP, topcell, VGX_ARCVECTOR_FxP_FRAME );  // frametop = topcell of the top leaf frame
  return cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcVector_cell_t * __arcvector_cell_set_multiple_arc( vgx_ArcVector_cell_t *cell, const vgx_Vertex_t *vertex, const framehash_cell_t *topcell ) {
  // VxD: VERTEX OBJ56
  TPTR_SET_OBJ56_AND_TAG( &cell->VxD, vertex, VGX_ARCVECTOR_VxD_VERTEX );
  // FxP: FRAME
  TPTR_SET_POINTER_AND_TAG( &cell->FxP, topcell, VGX_ARCVECTOR_FxP_FRAME );
  return cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_vertex_match( const vgx_ArcVector_cell_t *arc_cell, const vgx_Vertex_t *vertex ) {
  return vertex == TPTR_GET_OBJ56( &arc_cell->VxD );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_predicator_match( const vgx_ArcVector_cell_t *arc_cell, const vgx_predicator_t predicator ) {
  vgx_predicator_t arcpred = { .data = __arcvector_as_predicator_bits( arc_cell ) };
  return arcpred.rkey == predicator.rkey;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __arcvector_cell_is_multiple_arc( const vgx_ArcVector_cell_t *arc_cell ) {
  int VxD_tag = TPTR_AS_TAG( &arc_cell->VxD );  // First tptr in arc_cell
  int FxP_tag = TPTR_AS_TAG( &arc_cell->FxP );  // Second tptr in arc_cell
  if( VxD_tag == VGX_ARCVECTOR_VxD_VERTEX && FxP_tag == VGX_ARCVECTOR_FxP_FRAME ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_fhash_is_multiple_arc( const framehash_cell_t * const fh_cell ) {
  return APTR_AS_DTYPE( fh_cell ) == TAGGED_DTYPE_PTR56;
}



/*******************************************************************//**
 * VIRTUAL
 * INPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_VIRTUAL_INPUT_CONTEXT               \
  vgx_ExecutionTimingBudgetFlags_t *flags;                \
  vgx_virtual_ArcFilter_context_t *traverse_filter;       \
  vgx_LockableArc_t *larc;                                \
  int distance;                                           \
  framehash_processing_context_t arcarray_proc;           \
  framehash_processing_context_t multipred_proc_traverse; \
  framehash_processing_context_t multipred_proc_collect;

typedef struct __s_arcvector_virtual_input_context_t { VGX_ARCVECTOR_VIRTUAL_INPUT_CONTEXT } __arcvector_virtual_input_context_t;



/*******************************************************************//**
 * VITUAL
 * OUTPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_VIRTUAL_OUTPUT_CONTEXT      \
  vgx_collector_mode_t mode;                      \
  vgx_ArcFilter_match arc_match;                  \
  vgx_ArcFilter_match neighborhood_match;

typedef struct __s_virtual_arcvector_output_context_t { VGX_ARCVECTOR_VIRTUAL_OUTPUT_CONTEXT } __virtual_arcvector_output_context_t;



/*******************************************************************//**
 * EXISTENCE
 * INPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_EXISTENCE_INPUT_CONTEXT VGX_ARCVECTOR_VIRTUAL_INPUT_CONTEXT

typedef struct __s_arcvector_existence_input_context_t { VGX_ARCVECTOR_EXISTENCE_INPUT_CONTEXT } __arcvector_existence_input_context_t;



/*******************************************************************//**
 * EXISTENCE
 * OUTPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_EXISTENCE_OUTPUT_CONTEXT    \
  VGX_ARCVECTOR_VIRTUAL_OUTPUT_CONTEXT            \
  vgx_Arc_t *match_arc;

typedef struct __s_arcvector_existence_output_context_t { VGX_ARCVECTOR_EXISTENCE_OUTPUT_CONTEXT } __arcvector_existence_output_context_t;



/*******************************************************************//**
 * TRAVERSAL
 * INPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_TRAVERSAL_INPUT_CONTEXT   \
  VGX_ARCVECTOR_VIRTUAL_INPUT_CONTEXT           \
  vgx_Vertex_t *current_tail_RO;

typedef struct __s_arcvector_traversal_input_context_t { VGX_ARCVECTOR_TRAVERSAL_INPUT_CONTEXT } __arcvector_traversal_input_context_t;



/*******************************************************************//**
 * TRAVERSAL
 * OUTPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_TRAVERSAL_OUTPUT_CONTEXT      \
  VGX_ARCVECTOR_VIRTUAL_OUTPUT_CONTEXT              \
  vgx_virtual_ArcFilter_context_t *collect_filter;  \
  vgx_ArcCollector_context_t *collector;            \
  int64_t n_vertices;

typedef struct __s_arcvector_traversal_output_context_t { VGX_ARCVECTOR_TRAVERSAL_OUTPUT_CONTEXT } __arcvector_traversal_output_context_t;



/*******************************************************************//**
 * BIDIRECTIONAL TRAVERSAL
 * INPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_BIDIRECTIONAL_TRAVERSAL_INPUT_CONTEXT \
  VGX_ARCVECTOR_TRAVERSAL_INPUT_CONTEXT                     \
  vgx_neighborhood_probe_t *neighborhood_probe;             \
  struct {                                                  \
    const vgx_ArcVector_cell_t      *arcvector;             \
    vgx_virtual_ArcFilter_context_t *traverse_filter;       \
  } reverse;

typedef struct __s_arcvector_bidirectional_traversal_input_context_t { VGX_ARCVECTOR_BIDIRECTIONAL_TRAVERSAL_INPUT_CONTEXT } __arcvector_bidirectional_traversal_input_context_t;



/*******************************************************************//**
 * BIDIRECTIONAL TRAVERSAL
 * OUTPUT
 ***********************************************************************
 */
#define VGX_ARCVECTOR_BIDIRECTIONAL_TRAVERSAL_OUTPUT_CONTEXT    \
  VGX_ARCVECTOR_TRAVERSAL_OUTPUT_CONTEXT                        \
  struct {                                                      \
    vgx_ArcFilter_match match;                                  \
  } reverse;

typedef struct __s_arcvector_bidirectional_traversal_output_context_t { VGX_ARCVECTOR_BIDIRECTIONAL_TRAVERSAL_OUTPUT_CONTEXT } __arcvector_bidirectional_traversal_output_context_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_Vertex_t * __arcvector_set_archead_vertex( __arcvector_virtual_input_context_t *context, const framehash_cell_t *fh_cell ) {
  context->larc->head.vertex = (vgx_Vertex_t*)APTR_AS_ANNOTATION( fh_cell );
  context->larc->acquired.head_lock = 0; // reset with new head
  context->traverse_filter->current_head = &context->larc->head;
  return context->larc->head.vertex;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_predicator_t __arcvector_set_archead_predicator( __arcvector_virtual_input_context_t *context, const framehash_cell_t *fh_cell ) {
  context->larc->head.predicator.data = APTR_AS_UNSIGNED( fh_cell );
  return context->larc->head.predicator;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __arcvector_clear_archead( __arcvector_virtual_input_context_t *context ) {
  context->traverse_filter->current_head = NULL;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __arcvector_context_prepare_multipred( __arcvector_virtual_input_context_t *context, const framehash_cell_t *fh_cell ) {
  context->larc->head.predicator = VGX_PREDICATOR_NONE;
  if( context->multipred_proc_traverse.instance.frame ) {
    *context->multipred_proc_traverse.instance.frame = __arcvector_fhcell_get_ephemeral_top( fh_cell );
  }
  if( context->multipred_proc_collect.instance.frame ) {
    *context->multipred_proc_collect.instance.frame = __arcvector_fhcell_get_ephemeral_top( fh_cell );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int (*__f_match_pred)( const vgx_predicator_t A, const vgx_predicator_t B );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static __f_match_pred __select_predicator_matcher( vgx_predicator_t probe ) {
  // *
  if( _vgx_predicator_full_wildcard( probe ) ) {
    return predmatchfunc.Any;
  }
  // rel_mod_?
  else if( _vgx_predicator_specific( probe ) ) {
    // rel_mod_val
    if( _vgx_predicator_has_val( probe ) ) {
      return predmatchfunc.Generic;
    }
    // rel_mod_*
    else {
      return predmatchfunc.Key;
    }
  }
  // rel_*_*
  else if( _vgx_predicator_has_rel( probe ) ) {
    return predmatchfunc.Relationship;
  }
  // *_mod_?
  else {
    // *_mod_val
    if( _vgx_predicator_has_val( probe ) ) {
      return predmatchfunc.Generic;
    }
    // *_mod_*
    else {
      return predmatchfunc.Modifier;
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __arcvector_init_archead_from_cell( ArcVectorCell ) \
  VGX_ARCHEAD_INIT_PREDICATOR_BITS( __arcvector_as_predicator_bits( ArcVectorCell ), __arcvector_get_vertex( ArcVectorCell ) )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __arcvector_init_arc_from_cell( ArcVectorCell, TailPtr ) \
  VGX_ARC_INIT_PREDICATOR_BITS( TailPtr, __arcvector_as_predicator_bits( ArcVectorCell ), __arcvector_get_vertex( ArcVectorCell ) )




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_LockableArc_t * __init_lockable_arc( vgx_LockableArc_t *larc, _vgx_ArcVector_cell_type ctype, bool readonly_graph, vgx_Vertex_t *tail_RO, vgx_predicator_t predicator, vgx_Vertex_t *head, vgx_ExecutionTimingBudget_t *timing_budget, vgx_ArcFilter_match *match ) {
  larc->tail = tail_RO ? tail_RO : head;
  larc->head.predicator = predicator;
  larc->head.vertex = head;
  larc->ctype = ctype;
  larc->acquired.head_lock = 0;
  // No locks necessary when graph is readonly for the lifetime of the larc
  if( readonly_graph ) {
    larc->acquired.tail_lock = 0;
  }
  // Lock tail when graph is not readonly
  else {
    vgx_Graph_t *graph = larc->tail->graph;
    GRAPH_LOCK_SPIN( graph, 2 ) {
      static const int8_t TWO_LOCKS = 2;
      // !!! NOTE !!!
      // Own TWO locks here. One is for the traverser, the other for collector (if needed).
      if( _vxgraph_state__lock_vertex_readonly_multi_CS( graph, larc->tail, timing_budget, TWO_LOCKS ) != NULL ) {
        larc->acquired.tail_lock = TWO_LOCKS; // <- TWO!
      }
      else {
        larc = NULL;
        if( match ) {
          *match = __arcfilter_error(); // can't acquire tail readonly
        }
      }
    } GRAPH_RELEASE;
  }
  return larc;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __timing_budget_exhausted( vgx_ExecutionTimingBudget_t *timing_budget, vgx_ArcFilter_match *match ) {
  if( match ) {
    *match = __arcfilter_error();
  }
  _vgx_set_execution_halted( timing_budget, VGX_ACCESS_REASON_EXECUTION_TIMEOUT );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __begin_lockable_arc_context( LockableArcName, ArcVectorCellType, GraphIsReadonly, Tail, Predicator, Head, TimingBudget, MatchPtr ) \
  do {                                                      \
    vgx_ExecutionTimingBudget_t *__tb__ = TimingBudget;     \
    vgx_ArcFilter_match *__match__ = MatchPtr;              \
    if( _vgx_is_execution_limit_exceeded( __tb__ ) ) {      \
      __timing_budget_exhausted( __tb__, __match__ );       \
      break;                                                \
    }                                                       \
    vgx_LockableArc_t LockableArcName;                      \
    vgx_LockableArc_t *__larc__ = __init_lockable_arc( &LockableArcName, ArcVectorCellType, GraphIsReadonly, Tail, Predicator, Head, __tb__, __match__ );  \
    if( __larc__ == NULL ) {                                \
      break;                                                \
    }                                                       \
    else


#define __end_lockable_arc_context                          \
    __release_lockable_arc( __larc__, __tb__ );             \
  } WHILE_ZERO




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_Vertex_t * __lock_probed_vertex_readonly( vgx_Graph_t *graph, vgx_Vertex_t *vertex_OPEN, vgx_ExecutionTimingBudget_t *timing_budget ) {
  timing_budget->resource = vertex_OPEN;
  return _vxgraph_state__lock_vertex_readonly_OPEN( graph, vertex_OPEN, timing_budget, VGX_VERTEX_RECORD_NONE );
}



/*******************************************************************//**
 * 
 * Returns:  1 : arc is locked
 *           0 : no arc lock required
 *          -1 : failed to lock arc
 ***********************************************************************
 */
__inline static int __filter_acquire_lockable_arc_head( vgx_virtual_ArcFilter_context_t *arcfilter, vgx_LockableArc_t *larc ) {
  // Lock head vertex only if needed
  if( arcfilter->arcfilter_locked_head_access && larc->acquired.head_lock == 0 ) {
    vgx_Graph_t *graph = arcfilter->current_tail->graph;
    if( __lock_probed_vertex_readonly( graph, larc->head.vertex, arcfilter->timing_budget ) == NULL ) {
      larc->acquired.head_lock = -1; // head vertex not readable
      return -1;
    }
    // Arc head is now safe
    else {
      larc->acquired.head_lock = 1;
      return 1;
    }
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * Returns:  1 : arc is locked
 *           0 : no arc lock required
 *          -1 : failed to lock arc
 ***********************************************************************
 */
__inline static int __ranker_acquire_lockable_arc_head( vgx_Ranker_t *ranker, vgx_LockableArc_t *larc ) {
  // Lock head vertex only if needed
  if( ranker->locked_head_access && larc->acquired.head_lock == 0 ) {
    if( __lock_probed_vertex_readonly( ranker->graph, larc->head.vertex, ranker->timing_budget ) == NULL ) {
      larc->acquired.head_lock = -1; // head vertex not readable
      return -1;
    }
    // Arc head is now safe
    else {
      larc->acquired.head_lock = 1;
      return 1;
    }
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void __release_lockable_archead( vgx_LockableArc_t *larc ) {
  // If lockable arc still owns head vertex, release it now
  if( larc->acquired.head_lock > 0 ) {
    vgx_Graph_t *graph = larc->head.vertex->graph;
    vgx_Vertex_t *head_LCK = larc->head.vertex;
    _vxgraph_state__unlock_vertex_OPEN_LCK( graph, &head_LCK, VGX_VERTEX_RECORD_OPERATION );
    larc->acquired.head_lock = 0;
  } 
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void __release_lockable_arc( vgx_LockableArc_t *larc, vgx_ExecutionTimingBudget_t *timing_budget ) {

  bool limexec = _vgx_is_execution_limited( timing_budget );

  // If lockable arc still owns vertices, release them now
  if( larc->acquired.head_lock > 0 || larc->acquired.tail_lock > 0 || limexec ) {
    vgx_Graph_t *graph = larc->head.vertex->graph;
    GRAPH_LOCK( graph ) {
      while( larc->acquired.head_lock > 0 ) {
        vgx_Vertex_t *head_LCK = larc->head.vertex;
        _vxgraph_state__unlock_vertex_CS_LCK( graph, &head_LCK, VGX_VERTEX_RECORD_OPERATION );
        larc->acquired.head_lock--;
      }
      while( larc->acquired.tail_lock > 0 ) {
        vgx_Vertex_t *tail_LCK = larc->tail;
        _vxgraph_state__unlock_vertex_CS_LCK( graph, &tail_LCK, VGX_VERTEX_RECORD_OPERATION );
        larc->acquired.tail_lock--;
      }
    } GRAPH_RELEASE;
    // Execution time is limited, update timing budget
    if( limexec ) {
      _vgx_update_graph_execution_timing_budget( graph, timing_budget, false );
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __arcvector_traversal_result( __arcvector_virtual_input_context_t *context, int64_t value ) {
  return context->larc->acquired.head_lock < 0 ? -1 : value;
}


#define __begin_safe_traversal_context( VirtualInputContext, ArcArrayCell  )  \
  do {                                                                        \
    __arcvector_virtual_input_context_t *__context__ = VirtualInputContext;   \
    framehash_cell_t * const __cell__ = ArcArrayCell;                         \
    __arcvector_set_archead_vertex( __context__, __cell__ );                  \
    vgx_LockableArc_t *__larc__ = __context__->larc;


#define __end_safe_traversal_context          \
    __release_lockable_archead( __larc__ );   \
    __arcvector_clear_archead( __context__ ); \
  } WHILE_ZERO



#define begin_direct_recursion_context( VirtualInputContext, ArcHeadHeapItemCursor )  \
  if( (ArcHeadHeapItemCursor)->vertex == NULL ) { \
    continue; \
  }           \
  if( (VirtualInputContext)->flags->is_halted ) { \
    break;    \
  }           \
  do {        \
    __arcvector_virtual_input_context_t *__context__ = VirtualInputContext;     \
    __context__->larc->head.vertex = (vgx_Vertex_t*)(ArcHeadHeapItemCursor)->vertex; \
    __context__->larc->head.predicator = (ArcHeadHeapItemCursor)->predicator;   \
    __context__->larc->acquired.head_lock = 0;                                  \
    __context__->traverse_filter->current_head = &__context__->larc->head;      \
    _vgx_arc_set_distance( (vgx_Arc_t*)__context__->larc, __context__->distance );



#define end_direct_recursion_context  \
    __release_lockable_archead( __context__->larc ); \
    __arcvector_clear_archead( __context__ ); \
  } WHILE_ZERO




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_predicator_t * __update_predicator_synthetic( vgx_predicator_t *p ) {
  p->rel.enc = VGX_PREDICATOR_REL_SYNTHETIC;
  p->mod.bits = VGX_PREDICATOR_MOD_NONE;
  p->val.bits = 0;
  return p;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_Evaluator_t * __synthetic_evaluator( vgx_virtual_ArcFilter_context_t *arcfilter ) {
  // Arcfilter indicates it has a traversing evaluator containing synthetic arc operation(s)
  if( arcfilter->eval_synarc ) {
    vgx_Evaluator_t *E = arcfilter->traversing_evaluator;
    CALLABLE( E )->ClearWReg( E );
    return E;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __eval_clear_wreg( vgx_virtual_ArcFilter_context_t *arcfilter ) {
  // Arcfilter indicates it has a traversing evaluator containing synthetic arc operation(s)
  if( arcfilter->eval_synarc ) {
    vgx_Evaluator_t *E = arcfilter->traversing_evaluator;
    return CALLABLE( E )->ClearWReg( E );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_predicator_t * __set_predicator_synthetic( vgx_predicator_t *p ) {
  p->rel.enc = VGX_PREDICATOR_REL_SYNTHETIC;
  p->mod.bits = VGX_PREDICATOR_MOD_NONE;
  p->val.bits = 0;
  return p;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static framehash_cell_t * __get_synthetic_arc_cell( framehash_cell_t *synthetic_cell, vgx_predicator_t pred, framehash_cell_t * const fh_cell ) {
  APTR_COPY( synthetic_cell, fh_cell );
  APTR_SET_UNSIGNED( synthetic_cell, __set_predicator_synthetic( &pred )->data );
  return synthetic_cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __process_synthetic_arc( vgx_predicator_t predicator, int64_t *counter, f_framehash_cell_processor_t cell_processor, framehash_processing_context_t * const context, framehash_cell_t * const fh_cell ) {
  if( *counter >= 0 ) {
    framehash_cell_t synthetic;
    int64_t s = cell_processor( context, __get_synthetic_arc_cell( &synthetic, predicator, fh_cell ) );
    if( s < 0 ) {
      *counter = s;
    }
    else {
      *counter += s;
    }
  }
  return *counter;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __begin_traverse                            \
  do {                                              \
    vgx_Evaluator_t *__syneval__ = __synthetic_evaluator( __context__->traverse_filter );
   


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __traverse_final_synthetic( AllowSynthetic, SyntheticCellProcessor, CounterPtr, ProcessingContext )                     \
    if( __syneval__ && CALLABLE( __syneval__ )->GetWRegNCall( __syneval__ ) > 0 ) {                                             \
      if( (AllowSynthetic) ) {                                                                                                  \
        __process_synthetic_arc( __larc__->head.predicator, CounterPtr, SyntheticCellProcessor, ProcessingContext, __cell__ );  \
      }                                                                                                                         \
    }                                                                                                                           \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __traverse_multiple                                         \
  if( __arcvector_fhash_is_multiple_arc( __cell__ ) ) {             \
    __arcvector_context_prepare_multipred( __context__, __cell__ );
    /* { multiple arc actions } */



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __traverse_simple                 \
  } else /* { simple arc actions } */     \



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __pre_arc_visit( ArcFilter, LockableArc, FilterMatch )                \
  do {                                                                        \
    vgx_virtual_ArcFilter_context_t *__arcfilter__ = ArcFilter;               \
    vgx_LockableArc_t *__larc__ = LockableArc;                                \
    vgx_ArcFilter_match *__match__ = FilterMatch;                             \
    vgx_Evaluator_t *__syneval__ = __synthetic_evaluator( __arcfilter__ );    \
    int __again__ = __syneval__ ? 1 : 0;                                      \
    do {                                                                      \
      do /* { 
        visit 
      } */



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __post_arc_visit( AllowSynthetic )                                              \
      WHILE_ZERO;                                                                       \
      if( __again__ > 0 && CALLABLE( __syneval__ )->GetWRegNCall( __syneval__ ) > 0 ) { \
        if( (AllowSynthetic) && !__is_arcfilter_error( *__match__ ) ) {                 \
          __set_predicator_synthetic( &__larc__->head.predicator );                     \
        }                                                                               \
        else {                                                                          \
          __again__ = 0;                                                                \
        }                                                                               \
      }                                                                                 \
    } while( __again__-- > 0 );                                                         \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_Vector_t * __simprobe_vector( const vgx_vertex_probe_t *vertex_probe ) {
  if( vertex_probe == NULL || vertex_probe->advanced.similarity_probe == NULL ) {
    return NULL;
  }
  return vertex_probe->advanced.similarity_probe->probevector;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __evaluate_arc( vgx_Evaluator_t *E, vgx_Vector_t *vector, vgx_LockableArc_t *larc, const vgx_virtual_ArcFilter_context_t *previous ) {
  if( E == NULL ) {
    return 1;
  }
  CALLABLE( E )->SetContext( E, previous->current_tail, previous->current_head, vector, 0.0 );
  return iEvaluator.IsPositive( CALLABLE( E )->EvalArc( E, larc ) );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __begin_arc_evaluator_context( PreviousFilterContextPtr, PreEvaluatorPtr, MainEvaluatorPtr, PostEvaluatorPtr, VectorPtr, LockableArcPtr, MatchPtr )  \
  do {                                                                                  \
    const vgx_virtual_ArcFilter_context_t *__previous__ = PreviousFilterContextPtr;     \
    vgx_Evaluator_t *__pre__ = PreEvaluatorPtr;                                         \
    vgx_Evaluator_t *__main__ = MainEvaluatorPtr;                                       \
    vgx_Evaluator_t *__post__ = PostEvaluatorPtr;                                       \
    vgx_Vector_t *__vector__ = VectorPtr;                                               \
    vgx_LockableArc_t *__larc__ = LockableArcPtr;                                       \
    vgx_ArcFilter_match *__match__ = MatchPtr;                                          \
    /* PRE-FILTER */                                                                    \
    if( __evaluate_arc( __pre__, __vector__, __larc__, __previous__ ) ) {               \
      /* CONTINUE (only if PRE matched) */                                              \
      if( __main__ ) {                                                                  \
        CALLABLE( __main__ )->SetContext( __main__, __previous__->current_tail, __previous__->current_head, __vector__, 0.0 ); \
      }                                                                                 \
      do



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __end_arc_evaluator_context             \
      WHILE_ZERO;                               \
      /* POST-FILTER (only if PRE matched) */   \
      if( !__evaluate_arc( __post__, __vector__, __larc__, __previous__ ) ) { \
        /* POST MISS */                         \
        *__match__ = VGX_ARC_FILTER_MATCH_MISS; \
      }                                         \
    }                                           \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __begin_arcvector_filter_accept_arc( VirtualFilterContext, LockableArcPtr, MatchPtr ) \
  if( (VirtualFilterContext)->filter( VirtualFilterContext, LockableArcPtr, MatchPtr ) )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __end_arcvector_filter_accept_arc



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __arcvector_probe_push_conditional( NeighborhoodProbe, ArcFilter, VertexProbe, ArcDir, Evaluator )  \
  do {                                                                                  \
    vgx_neighborhood_probe_t *__PROBE = NeighborhoodProbe;                              \
    vgx_recursive_probe_t __RECURSIVE = __PROBE->conditional;                           \
    __PROBE->conditional.arcfilter = ArcFilter;                                         \
    __PROBE->conditional.vertex_probe = VertexProbe;                                    \
    __PROBE->conditional.arcdir = ArcDir;                                               \
    __PROBE->conditional.evaluator = Evaluator;                                         \
    do



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __arcvector_probe_pop_conditional       \
    WHILE_ZERO;                                 \
    __PROBE->conditional = __RECURSIVE;         \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_filter_collect_arc_impl( vgx_virtual_ArcFilter_context_t *context, vgx_LockableArc_t *arc, vgx_ArcFilter_match *match ) {
  if( context->type == VGX_ARC_FILTER_TYPE_PASS ) {
    *match = VGX_ARC_FILTER_MATCH_HIT;
    return 1;
  }
  else {
    uint16_t dir_condition = ((vgx_GenericArcFilter_context_t*)context)->pred_condition1.rel.dir;
    return (dir_condition == VGX_ARCDIR_ANY || dir_condition == arc->head.predicator.rel.dir) && context->filter( context, arc, match );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __begin_arcvector_filter_collect_arc( VirtualFilterContext, LockableArcPtr, MatchPtr ) \
  if( __arcvector_filter_collect_arc_impl( VirtualFilterContext, LockableArcPtr, MatchPtr ) )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define __end_arcvector_filter_collect_arc



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_collect_arc( vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, double rankscore, framehash_processing_context_t *processor ) {
  int collected = 0;
  if( collector->n_remain > 0 ) {

    // Ensure safe vertex access
    if( collector->ranker ) { 
      if( __ranker_acquire_lockable_arc_head( collector->ranker, larc ) < 0 ) {
        return -1;
      }
      if( collector->ranker->evaluator ) {
        collector->ranker->evaluator->context.rankscore = rankscore;
      }
    }

    // Perform collection
    collector->n_collectable++;
    if( (collected = collector->collect_arc( collector, larc )) < 0 ) {
      return -1;
    }
    collector->n_arcs += collected;
    collector->n_remain -= collected;
  }
  else if( processor ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
  }

  return collected;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_collect_vertex( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, framehash_processing_context_t *processor ) {
  int collected = 0;
  if( collector->n_remain > 0 ) {

    // Ensure safe vertex access
    if( collector->ranker ) { 
      if( __ranker_acquire_lockable_arc_head( collector->ranker, larc ) < 0 ) {
        return -1;
      }
    }

    // Perform collection
    collector->n_collectable++;
    if( (collected = collector->collect_vertex( collector, larc )) < 0 ) {
      return -1;
    }
    collector->n_remain -= collected;

  }
  else if( processor ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
  }

  return collected;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_Vector_t * __push_anchor_vector_to_vertex_probe( vgx_vertex_probe_t *vertex_probe, vgx_Vector_t *anchor_vector, int *terminate ) {
  vgx_similarity_probe_t *simprobe;
  if( vertex_probe && (simprobe=vertex_probe->advanced.similarity_probe) != NULL ) {
    // SPECIAL CASE: if vector is NULL-vector, use the current anchor's vector instead
    if( CALLABLE( simprobe->probevector )->IsNull( simprobe->probevector ) ) {
      // Push the anchor's vector to the probe
      if( anchor_vector != NULL ) {
        // save so we can restore at the end, so future use of probe can detect the null
        vgx_Vector_t *previous_vector = simprobe->probevector;
        // neighborhood search will now use anchor's vector for similarity scoring (BORROW REFERENCE)
        simprobe->probevector = anchor_vector;
        return previous_vector;
      }
      // Flag early termination: anchor's vector is NULL = automatic miss
      else {
        *terminate = 1;
      }
    }
  }
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __restore_vertex_probe_from_vector( vgx_vertex_probe_t *vertex_probe, vgx_Vector_t *vector ) {
  if( vector && vertex_probe && vertex_probe->advanced.similarity_probe ) {
    // Restore. (The ejected reference was borrowed so we can just assign vector back to probe)
    vertex_probe->advanced.similarity_probe->probevector = vector;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __push_anchor_vector_to_simprobes( vgx_neighborhood_probe_t *neighborhood_probe, int *terminate, vgx_Vector_t **conditional_vector, vgx_Vector_t **traversing_vector ) {
  vgx_Vector_t *anchor_vector = neighborhood_probe->current_tail_RO->vector;
  *conditional_vector = __push_anchor_vector_to_vertex_probe( neighborhood_probe->conditional.vertex_probe, anchor_vector, terminate );
  *traversing_vector = __push_anchor_vector_to_vertex_probe( neighborhood_probe->traversing.vertex_probe, anchor_vector, terminate );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __restore_simprobes_from_vectors( vgx_neighborhood_probe_t *neighborhood_probe, vgx_Vector_t *conditional_vector, vgx_Vector_t *traversing_vector ) {
  __restore_vertex_probe_from_vector( neighborhood_probe->conditional.vertex_probe, conditional_vector );
  __restore_vertex_probe_from_vector( neighborhood_probe->traversing.vertex_probe, traversing_vector );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __divergent_recursion( const vgx_neighborhood_probe_t *next ) {
  return next->conditional.arcfilter != next->traversing.arcfilter;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __arcvector_has_arc( vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *probe, vgx_ArcFilter_match *match ) {

  vgx_Arc_t first_match = VGX_ARC_INIT_PREDICATOR_BITS( NULL, VGX_PREDICATOR_NONE_BITS, NULL );
  const vgx_Vertex_t *vertex_RO = probe->current_tail_RO;
  bool ovr_ena = recursive->override.enable;

  switch( recursive->arcdir ) {
  case VGX_ARCDIR_ANY:
    // Match if either direction matches
    recursive->override.enable = false; // disable override for first check in order to proceed with second if miss
    *match = iarcvector.HasArc( &vertex_RO->inarcs, recursive, probe, &first_match );
    recursive->override.enable = ovr_ena; // restore
    // Inarcs miss?
    if( *match != VGX_ARC_FILTER_MATCH_HIT ) {
      if( __is_arcfilter_error( *match ) ) {
        return 0;
      }
      else if( (*match = iarcvector.HasArc( &vertex_RO->outarcs, recursive, probe, &first_match )) != VGX_ARC_FILTER_MATCH_HIT ) {
        return 0;
      }
    }
    break;
  case VGX_ARCDIR_IN:
    // Match if hit in inarcs
    if( (*match = iarcvector.HasArc( &vertex_RO->inarcs, recursive, probe, &first_match )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_OUT:
    // Match if hit in outarcs
    if( (*match = iarcvector.HasArc( &vertex_RO->outarcs, recursive, probe, &first_match )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_BOTH:
    // Match if hit in both inarcs and outarcs
    if( (*match = iarcvector.HasArcBidirectional( &vertex_RO->inarcs, &vertex_RO->outarcs, probe )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  default:
    *match = __arcfilter_error();
    return 0;
  }

  // Hit
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_collect_next_neighborhood_arcs( vgx_neighborhood_probe_t *next, vgx_ArcFilter_match *match ) {

  // Recursive adjacency condition must be met before traversal is attempted, if the adjacency condition differs from traversal condition
  if( __divergent_recursion( next ) ) {
    if( !__arcvector_has_arc( &next->conditional, next, match ) ) {
      return 0;
    }
  }

  const vgx_Vertex_t *vertex_RO = next->current_tail_RO;

  // Traverse
  switch( next->traversing.arcdir ) {
  case VGX_ARCDIR_ANY:
    // Collected anything we can find in both directions, must be at least one item overall to be considered a match
    {
      vgx_ArcFilter_match m_in = iarcvector.GetArcs( &vertex_RO->inarcs, next );
      vgx_ArcFilter_match m_out = iarcvector.GetArcs( &vertex_RO->outarcs, next );
      if( __is_arcfilter_error( m_in ) || __is_arcfilter_error( m_out ) ) {
        *match = __arcfilter_error();
        return 0;
      }
      else if( m_in == VGX_ARC_FILTER_MATCH_HIT || m_out == VGX_ARC_FILTER_MATCH_HIT ) {
        *match = VGX_ARC_FILTER_MATCH_HIT;
      }
      else {
        *match = VGX_ARC_FILTER_MATCH_MISS;
        return 0;
      }
    }
    break;
  case VGX_ARCDIR_IN: 
    // Collect inarcs
    if( (*match = iarcvector.GetArcs( &vertex_RO->inarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_OUT:
    // Collect outarcs
    if( (*match = iarcvector.GetArcs( &vertex_RO->outarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_BOTH:
    // Collect both inarcs and outarcs only if there is at least one match in each of the directions.
    if( (*match = iarcvector.GetArcsBidirectional( &vertex_RO->inarcs, &vertex_RO->outarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  default:
    *match = __arcfilter_error();
    return 0;
  }

  // Hit
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_collect_next_neighbors( vgx_neighborhood_probe_t *next, vgx_ArcFilter_match *match ) {

  if( __divergent_recursion( next ) ) {
    if( !__arcvector_has_arc( &next->conditional, next, match ) ) {
      return 0;
    }
  }

  const vgx_Vertex_t *vertex_RO = next->current_tail_RO;

  // Traverse
  switch( next->traversing.arcdir ) {
  case VGX_ARCDIR_ANY:
    // Collected anything we can find in both directions, must be at least one item overall to be considered a match
    {
      vgx_ArcFilter_match m_in = iarcvector.GetVertices( &vertex_RO->inarcs, next );
      vgx_ArcFilter_match m_out = iarcvector.GetVertices( &vertex_RO->outarcs, next );
      if( __is_arcfilter_error( m_in ) || __is_arcfilter_error( m_out ) ) {
        *match = __arcfilter_error();
        return 0;
      }
      else if( m_in == VGX_ARC_FILTER_MATCH_HIT || m_out == VGX_ARC_FILTER_MATCH_HIT ) {
        *match = VGX_ARC_FILTER_MATCH_HIT;
      }
      else {
        *match = VGX_ARC_FILTER_MATCH_MISS;
        return 0;
      }
    }
    break;
  case VGX_ARCDIR_IN:
    // Collect inarcs
    if( (*match = iarcvector.GetVertices( &vertex_RO->inarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_OUT:
    // Collect outarcs
    if( (*match = iarcvector.GetVertices( &vertex_RO->outarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  case VGX_ARCDIR_BOTH:
    // Collect both inarcs and outarcs only if there is at least one match in each of the directions.
    if( (*match = iarcvector.GetVerticesBidirectional( &vertex_RO->inarcs, &vertex_RO->outarcs, next )) != VGX_ARC_FILTER_MATCH_HIT ) {
      return 0;
    }
    break;
  default:
    *match = __arcfilter_error();
    return 0;
  }

  // Hit
  return 1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_next( vgx_neighborhood_probe_t *next, vgx_ArcFilter_match *match ) {
  int miss = 0;

  // Maybe replace probe vector with anchor's vector.
  vgx_Vector_t *conditional_vector = NULL;
  vgx_Vector_t *traversing_vector = NULL;
  __push_anchor_vector_to_simprobes( next, &miss, &conditional_vector, &traversing_vector );

  if( !miss ) {
    // Perform collection (or just filtration) based on the collector mode of the next neighborhood probe
    switch( _vgx_collector_mode_type( next->collector_mode ) ) {
    case VGX_COLLECTOR_MODE_NONE_CONTINUE:
      /* FALLTHRU */
    case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
      if( (miss = !__arcvector_has_arc( &next->conditional, next, match )) == 0 ) {
        if( __divergent_recursion( next ) ) {
          miss = !__arcvector_has_arc( &next->traversing, next, match );
        }
      }
      break;
    case VGX_COLLECTOR_MODE_COLLECT_ARCS:
      miss = !__arcvector_collect_next_neighborhood_arcs( next, match );
      break;
    case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
      miss = !__arcvector_collect_next_neighbors( next, match );
      break;
    default:
      break;
    }
  }

  // Maybe restore the probe's vector to its original if replaced earlier
  __restore_simprobes_from_vectors( next, conditional_vector, traversing_vector );

  return !miss;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __arcvector_vertex_condition_match_property_conditions( const vgx_property_probe_t *probe, const vgx_Vertex_t *vertex_RO ) {
  vgx_Vertex_vtable_t *iV = CALLABLE( vertex_RO );
  int64_t n = probe->len;
  vgx_VertexProperty_t *cursor = probe->condition_list;

  int hit = probe->positive_match == true ? 1 : 0;

  for( int64_t px=0; px<n; px++ ) {
    if( iV->HasProperty( vertex_RO, cursor++ ) == false ) {
      return !hit; // miss
    }
  }
  return hit;
}



DLL_HIDDEN extern int __arcvector_error( const char *funcname, int line, const vgx_Arc_t *arc, const vgx_Vertex_t *vertex, const vgx_ArcVector_cell_t *cell, vgx_ExecutionTimingBudget_t *timing_budget );

#define __ARCVECTOR_ERROR( Arc, Vertex, Cell, TimingBudget )  __arcvector_error( __FUNCTION__, __LINE__, Arc, Vertex, Cell, TimingBudget )


#endif
