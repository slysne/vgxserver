/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxoballoc_vertex.h
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

#ifndef _VGX_VXOBALLOC_VERTEX_H
#define _VGX_VXOBALLOC_VERTEX_H



#include "_cxmalloc.h"
#include "_vxprivate.h"
#include "vxgraph.h"



/*******************************************************************//**
 *
 * IVertexAllocator_t
 *
 ***********************************************************************
 */
typedef struct s_IVertexAllocator_t {
  int (*BlockOrder)( cxmalloc_family_t * vertex_allocator );
  cxmalloc_family_t * (*New)( vgx_Graph_t *graph, int vertex_block_order, framehash_t **pindex, framehash_t ***ptypeindices );
  void (*Delete)( vgx_Graph_t *graph );
  void (*SetCurrent)( cxmalloc_family_t *vertex_allocator );
  cxmalloc_family_t * (*GetCurrent)( void );
  int64_t (*Verify)( vgx_Graph_t *graph );
} IVertexAllocator_t;

DLL_HIDDEN extern IVertexAllocator_t ivertexalloc;


/*******************************************************************//**
 *
 * IVertexObject_t
 *
 ***********************************************************************
 */
typedef struct s_IVertexObject_t {
  vgx_Vertex_t * (*New)( vgx_Graph_t *graph, objectid_t *obid );
  void (*Delete)( vgx_Graph_t *graph, vgx_Vertex_t *vertex, void (*deconstruct)( vgx_Vertex_t *vertex ) );
  cxmalloc_handle_t (*AsHandle)( const vgx_Vertex_t *vertex );
  vgx_Vertex_t * (*FromHandleNolock)( const cxmalloc_handle_t handle );
  vgx_AllocatedVertex_t * (*AsAllocatedVertex)( const vgx_Vertex_t *vertex );
} IVertexObject_t;

DLL_HIDDEN extern IVertexObject_t ivertexobject;



__inline static cxmalloc_handle_t _vxoballoc_vertex_as_handle( const vgx_Vertex_t *vertex ) {
  cxmalloc_handle_t vertex_handle = _cxmalloc_object_as_handle( vertex );
  vertex_handle.objclass = COMLIB_CLASS_CODE( vgx_Vertex_t );
  return vertex_handle;
}



__inline static int32_t _vxoballoc_vertex_enum32( const vgx_Vertex_t *vertex ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex );
  uint64_t u40 = linehead->data.handle & 0xFFFFFFFFFFULL;
  if( u40 < INT_MAX ) {
    return (int32_t)u40 + 1;
  }
  else {
    return -1;
  }
}



__inline static int64_t _vxoballoc_vertex_incref_WL( vgx_Vertex_t *vertex_WL ) {
  return _cxmalloc_object_incref_nolock( vertex_WL );
}



__inline static int64_t _vxoballoc_vertex_incref_delta_WL( vgx_Vertex_t *vertex_WL, unsigned delta ) {
  return _cxmalloc_object_incref_delta_nolock( vertex_WL, delta );
}



__inline static int64_t _vxoballoc_vertex_refcnt_WL( vgx_Vertex_t *vertex_WL ) {
  return _cxmalloc_object_refcnt_nolock( vertex_WL );
}



static int64_t __vxoballoc_vertex_decref_to_zero_WL( vgx_Vertex_t *vertex_WL ) {
  vgx_Graph_t *graph = vertex_WL->graph;
  int64_t refc;
  GRAPH_LOCK( graph ) {
    // Break down vertex unless it is already defunct
    if( !__vertex_is_defunct( vertex_WL ) ) {
      // WARNING: This will temporarily suspend CS on the inside!
      CALLABLE( vertex_WL )->Initialize_CS( vertex_WL );
    }
    // Release
    if( __vertex_get_writer_recursion( vertex_WL ) > 0 ) {
      while( __vertex_unlock_writable_CS( vertex_WL ) > 0 );
    }
    // Deallocate vertex object
    cxmalloc_family_t *VA = vertex_WL->graph->vertex_allocator;
    refc = CALLABLE( VA )->DiscardObjectNolock( VA, vertex_WL );
  } GRAPH_RELEASE;
  return refc;
}



__inline static int64_t _vxoballoc_vertex_decref_WL( vgx_Vertex_t *vertex_WL ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex_WL );
  // Quick decref since refcnt is not going to zero
  if( linehead->data.refc > 1 ) {
    return --(linehead->data.refc);
  }
  // Refcount is going to zero, do the heavy work
  else if( linehead->data.refc == 1 ) {
    return __vxoballoc_vertex_decref_to_zero_WL( vertex_WL );
  }
  // Safeguard, don't touch refcount if <= 0 (this is technically an error)
  else {
    return linehead->data.refc;
  }
}



__inline static int64_t _vxoballoc_vertex_incref_CS_RO( vgx_Vertex_t *vertex_CS_RO ) {
  return _cxmalloc_object_incref_nolock( vertex_CS_RO );
}



__inline static int64_t _vxoballoc_vertex_incref_delta_CS_RO( vgx_Vertex_t *vertex_CS_RO, unsigned delta ) {
  return _cxmalloc_object_incref_delta_nolock( vertex_CS_RO, delta );
}



DLL_VISIBLE extern void __trap_invalid_vertex_CS_RO( const struct s_vgx_Vertex_t *vertex_RO );



__inline static int64_t _vxoballoc_vertex_decref_CS_RO( vgx_Vertex_t *vertex_CS_RO ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex_CS_RO );
  // Quick decref since refcnt is not going to zero
  if( linehead->data.refc > 1 ) {
    return --(linehead->data.refc);
  }
  // This should never happen for a RO vertex
  else {
    __trap_invalid_vertex_CS_RO( vertex_CS_RO );
    return linehead->data.refc;
  }
}



__inline static int64_t _vxoballoc_vertex_refcnt_CS_RO( const vgx_Vertex_t *vertex_CS_RO ) {
  return _cxmalloc_object_refcnt_nolock( vertex_CS_RO );
}



#ifdef VGX_CONSISTENCY_CHECK
DLL_VISIBLE extern int64_t __check_vertex_consistency_WL( vgx_Vertex_t *vertex_WL );
#else
#define __check_vertex_consistency_WL( vertex_WL ) ((void)0)
#endif

#ifdef VGX_CONSISTENCY_CHECK
DLL_VISIBLE extern const vgx_Vertex_t * __assert_vertex_lock( const vgx_Vertex_t *vertex_LCK );
#else
#define __assert_vertex_lock( vertex_LCK ) ((void)0)
#endif


#define BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( Graph )      \
  do {                                                        \
    vgx_Graph_t *__graph__ = Graph;                           \
    while( _vgx_graph_is_vertex_constructor_blocked_CS( __graph__ ) ) { \
      WAIT_FOR_VERTEX_AVAILABLE( __graph__, 1 );              \
    }                                                         \
    _vgx_graph_block_vertex_constructor_CS( __graph__ );      \
    do


#define END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS                 \
    WHILE_ZERO;                                               \
    _vgx_graph_allow_vertex_constructor_CS( __graph__ );      \
    SIGNAL_VERTEX_AVAILABLE( __graph__ );                     \
  } WHILE_ZERO


#endif
