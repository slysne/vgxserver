/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxoballoc_vector.h
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

#ifndef _VGX_VXOBALLOC_VECTOR_H
#define _VGX_VXOBALLOC_VECTOR_H




#include "_cxmalloc.h"
#include "_vxprivate.h"
#include "vxgraph.h"



/*******************************************************************//**
 *
 * IVectorAllocator_t
 *
 ***********************************************************************
 */
typedef struct s_IVectorAllocator_t {
  cxmalloc_family_t * (*Get)( const vgx_Vector_t *self );
  cxmalloc_family_t * (*NewInternalMap)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewInternalMapEphemeral)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewExternalMap)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewExternalMapEphemeral)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewInternalEuclidean)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewInternalEuclideanEphemeral)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewExternalEuclidean)( vgx_Similarity_t *simobj, const char *name );
  cxmalloc_family_t * (*NewExternalEuclideanEphemeral)( vgx_Similarity_t *simobj, const char *name );
  uint16_t (*CountInternalElements)( const vector_feature_t *elements );
  uint16_t (*CountExternalElements)( const ext_vector_feature_t *elements );
  void (*Delete)( cxmalloc_family_t **allocator );
  int64_t (*Verify)( cxmalloc_family_t *allocator );
} IVectorAllocator_t;


DLL_HIDDEN extern IVectorAllocator_t ivectoralloc;



/*******************************************************************//**
 *
 * IVectorObject_t
 *
 ***********************************************************************
 */
typedef struct s_IVectorObject_t {
  vgx_Vector_t * (*New)( vgx_Similarity_t *context, vector_type_t type, uint16_t length, bool ephemeral );
  vgx_Vector_t * (*Null)( vgx_Similarity_t *context );
  int (*Delete)( vgx_Vector_t *vector );
  void (*IncrefDimensionsNolock)( vgx_Vector_t *vector );
  void (*IncrefDimensions)( vgx_Vector_t *vector );
  int64_t (*IncrefNolock)( vgx_Vector_t *vector );
  int64_t (*Incref)( vgx_Vector_t *vector );
  void (*DecrefDimensionsNolock)( vgx_Vector_t *vector );
  void (*DecrefDimensions)( vgx_Vector_t *vector );
  int64_t (*DecrefNolock)( vgx_Vector_t *vector );
  int64_t (*Decref)( vgx_Vector_t *vector );
  int64_t (*Refcnt)( const vgx_Vector_t *vector );
  cxmalloc_handle_t (*AsHandle)( const vgx_Vector_t *self );
  vgx_Vector_t * (*FromHandleNolock)( const cxmalloc_handle_t handle, cxmalloc_family_t *allocator );
  CString_t * (*Serialize)( const vgx_Vector_t *vector );
  vgx_Vector_t * (*Deserialize)( vgx_Similarity_t *sim, const CString_t *CSTR__data, CString_t **CSTR__error, uint32_t *dimerr );
  vgx_Similarity_t * (*SetSimobj)( vgx_Vector_t *self, vgx_Similarity_t *simobj );
  vgx_Similarity_t * (*GetSimobj)( const vgx_Vector_t *self );
  void * (*SetElements)( vgx_Vector_t *self, void *elements );
  void * (*GetElements)( vgx_Vector_t *self );
  vgx_VectorContext_t * (*SetContext)( const vgx_Vector_t *self, vgx_Similarity_t *simobj, void *elements );
  vgx_VectorContext_t * (*GetContext)( const vgx_Vector_t *self );
} IVectorObject_t;



DLL_HIDDEN extern IVectorObject_t ivectorobject;



/**************************************************************************//**
 * _vxoballoc_vector_as_handle
 *
 ******************************************************************************
 */
__inline static cxmalloc_handle_t _vxoballoc_vector_as_handle( const vgx_Vector_t *vector ) {
  cxmalloc_handle_t vector_handle = _cxmalloc_object_as_handle( vector );
  vector_handle.objclass = COMLIB_CLASS_CODE( vgx_Vector_t );
  return vector_handle;
}






#endif
