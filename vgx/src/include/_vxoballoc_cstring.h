/*
###################################################
#
# File:   _vxoballoc_cstring.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXOBALLOC_CSTRING_H
#define _VGX_VXOBALLOC_CSTRING_H



#include "_cxmalloc.h"
#include "_vxprivate.h"
#include "vxgraph.h"


// 128kiB minus one cacheline for the object metas, minus one final qword of zeros
#define _VXOBALLOC_CSTRING_MAX_LENGTH (int32_t)((1<<17)-sizeof(cacheline_t)-sizeof(QWORD))






/*******************************************************************//**
 *
 * ICStringAllocator_t
 *
 ***********************************************************************
 */
typedef struct s_ICStringAllocator_t {
  object_allocator_context_t * (*NewContext)( vgx_Graph_t *graph, framehash_t **pkeyindex, framehash_t **pvalindex, const char *basepath, const char *name );
  void (*DeleteContext)( object_allocator_context_t **context );
  int64_t (*RestoreObjects)( object_allocator_context_t *context );
  int (*SetReadonly)( object_allocator_context_t *context );
  int (*IsReadonly)( object_allocator_context_t *context );
  int (*ClearReadonly)( object_allocator_context_t *context );
  int64_t (*Verify)( object_allocator_context_t *context );
} ICStringAllocator_t;


DLL_HIDDEN extern ICStringAllocator_t icstringalloc;



/*******************************************************************//**
 *
 * ICStringObject_t
 *
 ***********************************************************************
 */
typedef struct s_ICStringObject_t {
  CString_t * (*New)( object_allocator_context_t *context, const char *str );
  CString_t * (*Clone)( object_allocator_context_t *context, const CString_t *CSTR__other );
  void (*Delete)( CString_t *CSTR__str );
  int64_t (*IncrefNolock)( CString_t *CSTR__str );
  int64_t (*Incref)( CString_t *CSTR__str );
  int64_t (*DecrefNolock)( CString_t *CSTR__str );
  int64_t (*Decref)( CString_t *CSTR__str );
  int64_t (*RefcntNolock)( const CString_t *CSTR__str );
  int64_t (*Refcnt)( const CString_t *CSTR__str );
  cxmalloc_handle_t (*AsHandle)( const CString_t *CSTR__str );
  CString_t * (*FromHandleNolock)( const cxmalloc_handle_t handle, object_allocator_context_t *allocator_context );
  int64_t (*SerializedSize)( const CString_t *CSTR__string );
  int64_t (*Serialize)( char **output, const CString_t *CSTR__string );
  CString_t * (*Deserialize)( const char *input, object_allocator_context_t *allocator_context );
} ICStringObject_t;


DLL_HIDDEN extern ICStringObject_t icstringobject;


__inline static cxmalloc_handle_t _vxoballoc_cstring_as_handle( const CString_t *CSTR__str ) {
  cxmalloc_handle_t cstring_handle = _cxmalloc_object_as_handle( CSTR__str );
  cstring_handle.objclass = COMLIB_CLASS_CODE( CString_t );
  return cstring_handle;
}



/* CString USER FLAG 0: Indexed in property key index */

#define _VXOBALLOC_CSTRING_KEY_MAP_ORDER 2
static const vgx_mapping_spec_t _VXOBALLOC_CSTRING_KEY_MAP_SPEC = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;

__inline static void _vxoballoc_cstring_set_key_indexable( CString_t *CSTR__key ) {
  VGX_CSTRING_FLAG__KEY_INDEX_BIT( CSTR__key ) = 1;
}

__inline static void _vxoballoc_cstring_clear_key_indexable( CString_t *CSTR__key ) {
  VGX_CSTRING_FLAG__KEY_INDEX_BIT( CSTR__key ) = 0;
}

__inline static int _vxoballoc_cstring_is_key_indexable( const CString_t *CSTR__key ) {
  return VGX_CSTRING_FLAG__KEY_INDEX_BIT( CSTR__key );
}


/* CString USER FLAG 1: Indexed in property value index */

#define _VXOBALLOC_CSTRING_VALUE_MAP_ORDER 6
static const vgx_mapping_spec_t _VXOBALLOC_CSTRING_VALUE_MAP_SPEC = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_128BIT | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;

__inline static void _vxoballoc_cstring_set_value_indexable( CString_t *CSTR__val ) {
  VGX_CSTRING_FLAG__VAL_INDEX_BIT( CSTR__val ) = 1;
}

__inline static void _vxoballoc_cstring_clear_value_indexable( CString_t *CSTR__val ) {
  VGX_CSTRING_FLAG__VAL_INDEX_BIT( CSTR__val ) = 0;
}

__inline static int _vxoballoc_cstring_is_value_indexable( const CString_t *CSTR__val ) {
  return VGX_CSTRING_FLAG__VAL_INDEX_BIT( CSTR__val );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static CString_t * NewEphemeralCString( vgx_Graph_t *graph, const char *str ) {
  CString_constructor_args_t args = {
    .string = NULL,
    .len    = 0,
    .ucsz   = 0,
    .format = NULL,
    .format_args = NULL,
    .alloc = graph->ephemeral_string_allocator_context
  };
  CString_t *CSTR__str = COMLIB_OBJECT_NEW( CString_t, str, &args );
  return CSTR__str;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static CString_t * NewEphemeralCStringLen( vgx_Graph_t *graph, const char *str, int len, int ucsz ) {
  CString_constructor_args_t args = {
    .string = str,
    .len    = len,
    .ucsz   = ucsz,
    .format = NULL,
    .format_args = NULL,
    .alloc  = graph->ephemeral_string_allocator_context
  };
  CString_t *CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  return CSTR__str;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static CString_t * CloneCStringEphemeral( vgx_Graph_t *graph, const CString_t *CSTR__orig ) {
  CString_constructor_args_t args = {
    .string = CStringValue( CSTR__orig ),
    .len    = CStringLength( CSTR__orig ),
    .ucsz   = CStringCodepoints( CSTR__orig ),
    .format = NULL,
    .format_args = NULL,
    .alloc  = graph->ephemeral_string_allocator_context
  };
  CString_t *CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  if( CSTR__str ) {
    CSTR__str->meta.flags.state.user = CSTR__orig->meta.flags.state.user;
  }
  return CSTR__str;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static CString_t * OwnOrCloneCString( const CString_t *CSTR__src, object_allocator_context_t *required_allocator_context ) {
  // Allocator context is supplied
  if( required_allocator_context != NULL ) {
    // Own another reference when string was allocated using the specified allocator
    if( CSTR__src->allocator_context == required_allocator_context ) {
      cxmalloc_family_t *family = (cxmalloc_family_t*)required_allocator_context->allocator;
      CString_t *CSTR__owned = (CString_t*)CSTR__src;
      CALLABLE( family )->OwnObject( family, CSTR__owned );
      return CSTR__owned;
    }
    // Clone the relationship string using the property allocator context
    else {
      return CStringCloneAlloc( CSTR__src, required_allocator_context );
    }
  }
  // Error
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static CString_t * OwnOrCloneEphemeralCString( vgx_Graph_t *graph, const CString_t *CSTR__src ) {
  // Own another reference when string was allocated using the specified allocator
  if( CSTR__src->allocator_context == graph->ephemeral_string_allocator_context ) {
    cxmalloc_family_t *family = (cxmalloc_family_t*)graph->ephemeral_string_allocator_context->allocator;
    CString_t *CSTR__owned = (CString_t*)CSTR__src;
    CALLABLE( family )->OwnObject( family, CSTR__owned );
    return CSTR__owned;
  }
  // Clone the relationship string using the property allocator context
  else {
    return CStringCloneAlloc( CSTR__src, graph->ephemeral_string_allocator_context );
  }
}






#endif
