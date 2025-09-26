/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxoballoc_cstring.c
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
#include "_vxoballoc_cstring.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int __vxoballoc_cstring__cxmalloc_serialize_cstring( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_cstring__cxmalloc_deserialize_cstring( cxmalloc_line_deserialization_context_t *context );






#define GRAPH_AUX_IDX           0
#define CONTEXT_AUX_IDX         1
#define KEY_INDEX_AUX_IDX       2
#define VAL_INDEX_AUX_IDX       3

/*

+----------------+----------------+--------+--------+----+----+--------+--------+--------+--------+--------+--------
|      obid      |  (allocator)   | vtable | typinf | sz |flgs| context|           character data  ....
|                |   allocator    |        |        |    |    |        |                                            
+----------------+----------------+--------+--------+----+----+--------+--------+--------+--------+--------+--------
|                                 \ ___________ CString_t ____________ /
|                                                                      |
\_____________  __CStringAllocated_t _________________________________ /

*/

/*******************************************************************//**
 * CString_t Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t CStringAllocatorDescriptor( vgx_Graph_t *graph, const CString_t *CSTR__persist_path ) {
  int cstring_max_length = _VXOBALLOC_CSTRING_MAX_LENGTH + 1;  // add one byte for nul term
  int MAX = 39;

#if defined VGX_GRAPH_CSTRING_BLOCK_SIZE_MB
  const size_t block_size = ((size_t)VGX_GRAPH_CSTRING_BLOCK_SIZE_MB << 20);
#elif defined VGX_GRAPH_CSTRING_BLOCK_SIZE_KB
  const size_t block_size = ((size_t)VGX_GRAPH_CSTRING_BLOCK_SIZE_KB << 10);
#else
  const size_t block_size = (64ULL << 20); // 64 MB default
#endif
  
  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},
      .serialized_sz    = sizeof( objectid_t )
    },
    .obj = {
      .sz               = sizeof( CString_t ),      /* space for the class header and extras  */
      .serialized_sz    = 2 * sizeof( DWORD )       /* size, flags,    */
    },
    .unit = {
      .sz               = sizeof( char ),           /* string data = bytes                    */
      .serialized_sz    = sizeof( char )
    },
    .serialize_line     = __vxoballoc_cstring__cxmalloc_serialize_cstring,
    .deserialize_line   = __vxoballoc_cstring__cxmalloc_deserialize_cstring,
    .parameter = {
      .block_sz         = block_size,               /* block size in bytes                    */
      .line_limit       = cstring_max_length,       /* aidx=35 => size=65472 with S=2         */
      .subdue           = 2,                        /* S=2 =>    0:0,      1:64,     2:128,    3:192,    4:256,    5:320,    6:384,    7:448,   */
                                                    /*           8:576,    9:704,   10:832,   11:960,   12:1216,  13:1472,  14:1728,  15:1984,  */
                                                    /*          16:2496,  17:3008,  18:3520,  19:4032,  20:5056,  21:6080,  22:7104,  23:8128,  */
                                                    /*          24:10176, 25:12224, 26:14272, 27:16320, 28:20416, 29:24512, 30:28608, 31:32704, */
                                                    /*          32:40896, 33:49088, 34:57280, 35:65472  36:81856, 37:98240, 38:114624,39:131008 */
      .allow_oversized  = 0,                        /* disallow oversized                     */
      .max_allocators   = MAX+1                     /* aidx 0 - 39                            */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path,       /*                                        */
    },
    .auxiliary = {
      graph,                    /* 0  (vgx_Graph_t*)                  */
      NULL,                     /* 1: (object_allocator_context_t*)   */
      NULL,                     /* 2: (framehash_t*)                  */
      NULL,                     /* 3: (framehash_t*)                  */
      NULL                      /* -- END __                          */
    }
  };

  return descriptor;
}



/*******************************************************************//**
*
*
***********************************************************************
*/
static object_allocator_context_t * __vxoballoc_cstring__new_allocator_context( vgx_Graph_t *graph, framehash_t **pkeyindex, framehash_t **pvalindex, const char *basepath, const char *name );
static void __vxoballoc_cstring__delete_allocator_context( object_allocator_context_t **context );
static int64_t __vxoballoc_cstring__restore_objects( object_allocator_context_t *context );
static int __vxoballoc_cstring__set_readonly( object_allocator_context_t *context );
static int __vxoballoc_cstring__is_readonly( object_allocator_context_t *context );
static int __vxoballoc_cstring__clear_readonly( object_allocator_context_t *context );
static int64_t __vxoballoc_cstring__verify( object_allocator_context_t *context );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN ICStringAllocator_t icstringalloc = {
  .NewContext     = __vxoballoc_cstring__new_allocator_context,
  .DeleteContext  = __vxoballoc_cstring__delete_allocator_context,
  .RestoreObjects = __vxoballoc_cstring__restore_objects,
  .SetReadonly    = __vxoballoc_cstring__set_readonly, 
  .IsReadonly     = __vxoballoc_cstring__is_readonly, 
  .ClearReadonly  = __vxoballoc_cstring__clear_readonly,
  .Verify         = __vxoballoc_cstring__verify
};



/*******************************************************************//**
*
*
***********************************************************************
*/
static CString_t * __vxoballoc_cstring__new_cstring( object_allocator_context_t *context, const char *str );
static CString_t * __vxoballoc_cstring__clone_cstring( object_allocator_context_t *context, const CString_t *CSTR__other );
static void __vxoballoc_cstring__delete_cstring( CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__incref_cstring_nolock( CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__incref_cstring( CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__decref_cstring_nolock( CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__decref_cstring( CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__refcnt_cstring_nolock( const CString_t *CSTR__str );
static int64_t __vxoballoc_cstring__refcnt_cstring( const CString_t *CSTR__str );
static cxmalloc_handle_t __vxoballoc_cstring__cstring_as_handle( const CString_t *CSTR__str );
static CString_t * __vxoballoc_cstring__cstring_from_handle_nolock( const cxmalloc_handle_t handle, object_allocator_context_t *allocator_context );
static int64_t __vxoballoc_cstring__serialized_size( const CString_t *CSTR__string );
static int64_t __vxoballoc_cstring__serialize( char **output, const CString_t *CSTR__string );
static CString_t * __vxoballoc_cstring__deserialize( const char *input, object_allocator_context_t *allocator_context );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN ICStringObject_t icstringobject = {
  .New              = __vxoballoc_cstring__new_cstring,
  .Clone            = __vxoballoc_cstring__clone_cstring,
  .Delete           = __vxoballoc_cstring__delete_cstring,
  .IncrefNolock     = __vxoballoc_cstring__incref_cstring_nolock,
  .Incref           = __vxoballoc_cstring__incref_cstring,
  .DecrefNolock     = __vxoballoc_cstring__decref_cstring_nolock,
  .Decref           = __vxoballoc_cstring__decref_cstring,
  .RefcntNolock     = __vxoballoc_cstring__refcnt_cstring_nolock,
  .Refcnt           = __vxoballoc_cstring__refcnt_cstring,
  .AsHandle         = __vxoballoc_cstring__cstring_as_handle,
  .FromHandleNolock = __vxoballoc_cstring__cstring_from_handle_nolock,
  .SerializedSize   = __vxoballoc_cstring__serialized_size,
  .Serialize        = __vxoballoc_cstring__serialize,
  .Deserialize      = __vxoballoc_cstring__deserialize
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __cstring_object_allocate( cxmalloc_family_t *family, size_t length ) {
  CString_t *CSTR__obj = NULL;
  void *data = CALLABLE( family )->New( family, (uint32_t)length );
  if( data ) {
    CSTR__obj = CALLABLE( family )->ObjectFromArray( family, data );
  }

  // WARNING: This allocated object has NOT been initialized. It is just a piece of memory
  // large enough to hold a CString object. The caller of this function is responsible for
  // finishing the construction of the object.
  return CSTR__obj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cstring_object_deallocate( cxmalloc_family_t *family, CString_t *CSTR__str ) {
  return CALLABLE( family )->DiscardObject( family, CSTR__str );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint16_t __cstring_size_bounds( cxmalloc_family_t *family, uint32_t sz, uint32_t *low, uint32_t *high ) {
  return CALLABLE( family )->SizeBounds( family, sz, low, high );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __new_index_name( vgx_Graph_t *graph, const char *prefix ) {
  CString_t *CSTR__prefix = NewEphemeralCString( graph, prefix );
  if( CSTR__prefix ) {
    CString_t *CSTR__index_name = iString.Utility.NewGraphMapName( graph, CSTR__prefix );
    CStringDelete( CSTR__prefix );
    return CSTR__index_name;
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
static object_allocator_context_t * __vxoballoc_cstring__new_allocator_context( vgx_Graph_t *graph, framehash_t **pkeyindex, framehash_t **pvalindex, const char *basepath, const char *name ) {
  object_allocator_context_t *context = NULL;
  cxmalloc_family_t *alloc = NULL;
  CString_t *CSTR__home = NULL;
  CString_t *CSTR__allocator_name = NULL;

  XTRY {
    if( basepath ) {
      const char *graphname = CStringValue( CALLABLE(graph)->Name(graph) );
      if( (CSTR__home = CStringNewFormat( "%s/%s[%s]", basepath, name, graphname )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x101 );
      }
    }

    // Get the descriptor without the context and index (will set later)
    cxmalloc_descriptor_t descriptor = CStringAllocatorDescriptor( graph, CSTR__home );

    cxmalloc_family_constructor_args_t args = {
      .family_descriptor  = &descriptor    //
    };

    if( (CSTR__allocator_name = CStringNewFormat( "CStringPropertyAllocator( %s )", name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x103 );
    }

    if( (alloc = COMLIB_OBJECT_NEW( cxmalloc_family_t, CStringValue(CSTR__allocator_name), &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x104 );
    }

    if( (context = (object_allocator_context_t*)calloc( 1, sizeof( object_allocator_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x105 );
    }

    // Set the allocator context in the allocator descriptor's auxiliary object references
    alloc->descriptor->auxiliary.obj[ CONTEXT_AUX_IDX ] = context;
    context->allocator = (comlib_object_t*)alloc;
    context->max_elems = alloc->max_length - 1;
    context->allocfunc = (f_object_allocate_t)__cstring_object_allocate;
    context->deallocfunc = (f_object_deallocate_t)__cstring_object_deallocate;
    context->bounds = (f_allocator_size_bounds_t)__cstring_size_bounds;

    // A non-persisted string index will be created and assigned to the index pointer.
    // This index will be rebuilt from allocator data as objects are restored later.
    if( graph ) {
      // Create key index
      if( pkeyindex ) {
        CString_t *CSTR__key_index = __new_index_name( graph, VGX_PATHDEF_CODEC_KEY_MAP_PREFIX );
        if( CSTR__key_index ) {
          *pkeyindex = iMapping.NewMap( NULL, CSTR__key_index, MAPPING_SIZE_UNLIMITED, _VXOBALLOC_CSTRING_KEY_MAP_ORDER, _VXOBALLOC_CSTRING_KEY_MAP_SPEC, CLASS_NONE ); 
          CStringDelete( CSTR__key_index );
        }
        if( (alloc->descriptor->auxiliary.obj[ KEY_INDEX_AUX_IDX ] = *pkeyindex) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x106 );
        }
      }
      // Create value index
      if( pvalindex ) {
        CString_t *CSTR__value_index = __new_index_name( graph, VGX_PATHDEF_CODEC_VALUE_MAP_PREFIX );
        if( CSTR__value_index ) {
          *pvalindex = iMapping.NewMap( NULL, CSTR__value_index, MAPPING_SIZE_UNLIMITED, _VXOBALLOC_CSTRING_VALUE_MAP_ORDER, _VXOBALLOC_CSTRING_VALUE_MAP_SPEC, CLASS_CString_t ); 
          CStringDelete( CSTR__value_index );
        }
        if( (alloc->descriptor->auxiliary.obj[ VAL_INDEX_AUX_IDX ] = *pvalindex) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x107 );
        }
      }
    }
    alloc = NULL;

#ifndef NDEBUG
    if( alloc ) {
      COMLIB_OBJECT_PRINT( alloc );
    }
#endif

  }
  XCATCH( errcode ) {
    if( alloc ) {
      //TODO: any clean up?
      COMLIB_OBJECT_DESTROY( alloc );
      alloc = NULL;
    }
    if( context ) {
      if( context->allocator ) {
        COMLIB_OBJECT_DESTROY( context->allocator );
      }
      free( context );
      context = NULL;
    }
  }
  XFINALLY {
    if( CSTR__home ) {
      CStringDelete( CSTR__home );
      CSTR__home = NULL;
    }
    if( CSTR__allocator_name ) {
      CStringDelete( CSTR__allocator_name );
      CSTR__allocator_name = NULL;
    }
  }

  return context;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_cstring__delete_allocator_context( object_allocator_context_t **context ) {
  if( context && *context ) {
    if( (*context)->allocator ) {
      COMLIB_OBJECT_DESTROY( (*context)->allocator );
    }
    free( *context );
    *context = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__restore_objects( object_allocator_context_t *context ) {
  int64_t n = 0;
  cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
  if( family ) {
    n = CALLABLE( family )->RestoreObjects( family );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_cstring__set_readonly( object_allocator_context_t *context ) {
  cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
  return CALLABLE( family )->SetReadonly( family );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_cstring__is_readonly( object_allocator_context_t *context ) {
  cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
  return CALLABLE( family )->IsReadonly( family );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_cstring__clear_readonly( object_allocator_context_t *context ) {
  cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
  return CALLABLE( family )->ClearReadonly( family );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__verify( object_allocator_context_t *context ) {
  cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
  CString_vtable_t *iCString = (CString_vtable_t*)COMLIB_CLASS_VTABLE( CString_t );
  return CALLABLE( family )->Sweep( family, (f_get_object_identifier)iCString->Value );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __vxoballoc_cstring__new_cstring( object_allocator_context_t *context, const char *str ) {
  CString_t *CSTR__str;
  CString_constructor_args_t args = {
    .string       = str,
    .len          = -1,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = context
  };

  CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args );

  return CSTR__str;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __vxoballoc_cstring__clone_cstring( object_allocator_context_t *context, const CString_t *CSTR__other ) {
  return CStringCloneAlloc( CSTR__other, context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_cstring__delete_cstring( CString_t *CSTR__str ) {
  if( CSTR__str ) {
    COMLIB_OBJECT_DESTROY( CSTR__str );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __vxoballoc_cstring__incref_cstring_nolock( CString_t *CSTR__str ) {
  if( CSTR__str->allocator_context ) {
    return _cxmalloc_object_incref_nolock( CSTR__str );
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
static int64_t __vxoballoc_cstring__incref_cstring( CString_t *CSTR__str ) {
  int64_t refcnt = 0;
  object_allocator_context_t *context = CSTR__str->allocator_context;
  if( context ) {
    cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
    refcnt = CALLABLE( family )->OwnObject( family, CSTR__str );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__decref_cstring_nolock( CString_t *CSTR__str ) {
  int64_t refcnt = 0;
  object_allocator_context_t *context = CSTR__str->allocator_context;
  if( context ) {
    cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
    refcnt = CALLABLE( family )->DiscardObjectNolock( family, CSTR__str );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__decref_cstring( CString_t *CSTR__str ) {
  int64_t refcnt = 0;
  object_allocator_context_t *context = CSTR__str->allocator_context;
  if( context ) {
    cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
    refcnt = CALLABLE( family )->DiscardObject( family, CSTR__str );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __vxoballoc_cstring__refcnt_cstring_nolock( const CString_t *CSTR__str ) {
  int64_t refcnt = _cxmalloc_object_refcnt_nolock( CSTR__str );
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__refcnt_cstring( const CString_t *CSTR__str ) {
  int64_t refcnt = 0;
  object_allocator_context_t *context = CSTR__str->allocator_context;
  if( context ) {
    cxmalloc_family_t *family = (cxmalloc_family_t*)context->allocator;
    refcnt = CALLABLE( family )->RefCountObject( family, CSTR__str );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_handle_t __vxoballoc_cstring__cstring_as_handle( const CString_t *CSTR__str ) {
  return _vxoballoc_cstring_as_handle( CSTR__str );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __trap_invalid_handle_class( const cxmalloc_handle_t handle ) {
  FATAL( 0xFFF, "Invalid CString_t class code in handle 0x016X. Got class 0x%02X, expected 0x%02X.", handle.qword, handle.objclass, COMLIB_CLASS_CODE( CString_t ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __vxoballoc_cstring__cstring_from_handle_nolock( const cxmalloc_handle_t handle, object_allocator_context_t *allocator_context ) {
  cxmalloc_family_t *allocator = (cxmalloc_family_t*)allocator_context->allocator;
  if( handle.objclass != COMLIB_CLASS_CODE( CString_t ) ) {
    __trap_invalid_handle_class( handle );
  }
  // NOTE: the string object may not be active yet if its allocator has not yet been restored. The string address is correct.
  CString_t *CSTR__str = CALLABLE( allocator )->HandleAsObjectNolock( allocator, handle );
  if( CSTR__str ) {
    CALLABLE( allocator )->OwnObjectNolock( allocator, CSTR__str );
    return CSTR__str;
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
static int64_t __vxoballoc_cstring__serialized_size( const CString_t *CSTR__string ) {
  // Meta plus ndata (2) plus data itself (NDATA) plus padding (1) in 16 char chunks
  QWORD NDATA = CStringQwordLength( CSTR__string );
  return (2 + NDATA + 1) * 16;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_cstring__serialize( char **output, const CString_t *CSTR__string ) {
  // Format:
  //
  // [     META     ][    NDATA     ][   data       ][   data       ]                [   data       ]
  // [ (size/flags) ][  n data qwds ][    0         ][    1         ]                [    n         ]
  // FFFFFFFFSSSSSSSSNNNNNNNNNNNNNNNNdddddddddddddddddddddddddddddddd................dddddddddddddddd
  //

  // Flags and size
  QWORD META = CStringMetaAsQword( CSTR__string );
  // Number of qwords
  QWORD NDATA = CStringQwordLength( CSTR__string );
  // Pointer to string data as qwords
  const QWORD *PDATA = CStringValueAsQwords( CSTR__string );
  // End of data
  const QWORD *END = PDATA + NDATA;

  // Allocate here if no buffer supplied (LEAK WARNING! Caller now owns buffer)
  if( *output == NULL ) {
    int64_t sz = __vxoballoc_cstring__serialized_size( CSTR__string );
    if( (*output = malloc( sz )) == NULL ) {
      return -1;
    }
    (*output)[sz-1] = 0;
  }

  // Write meta and ndata
  char *p = *output;
  p = write_HEX_qword( p, META );
  p = write_HEX_qword( p, NDATA );
  
  // Write data qwords
  while( PDATA < END ) {
    p = write_HEX_qword( p, *PDATA++ );
  }

  return p - *output;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __vxoballoc_cstring__deserialize( const char *input, object_allocator_context_t *allocator_context ) {
  // Format:
  //
  // [     META     ][    NDATA     ][   data       ][   data       ]                [   data       ]
  // [ (size/flags) ][  n data qwds ][    0         ][    1         ]                [    n         ]
  // FFFFFFFFSSSSSSSSNNNNNNNNNNNNNNNNdddddddddddddddddddddddddddddddd................dddddddddddddddd
  //

  const char *p = input;

  // [META]
  CString_meta_t META;
  if( (p = hex_to_QWORD( p, &META._bits )) == NULL ) {
    return NULL;
  }

  // [NDATA]
  QWORD NDATA = 0;
  if( (p = hex_to_QWORD( p, &NDATA )) == NULL ) {
    return NULL;
  }

  // Construct a new, empty string based on META and NDATA
  CString_constructor_args_t args = {
    .string       = NULL,
    .len          = META.size,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = allocator_context
  };

  CString_t *CSTR__string = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  if( CSTR__string == NULL ) {
    return NULL;
  }

  // Populate new string data 1-n
  QWORD *PDATA = CALLABLE( CSTR__string )->ModifiableQwords( CSTR__string );
  QWORD *END = PDATA + NDATA;
  while( PDATA < END && p ) {
    p = hex_to_QWORD( p, PDATA++ );
  }
  
  // Check that all data was successfully converted
  if( p == NULL ) {
    COMLIB_OBJECT_DESTROY( CSTR__string );
    return NULL;
  }

  // Finalize by setting correct metas
  CSTR__string->meta.flags = META.flags;

  return CSTR__string;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_cstring__cxmalloc_serialize_cstring( cxmalloc_line_serialization_context_t *context ) {
  
  cxmalloc_linehead_t *linehead = context->linehead;
  CString_t *CSTR__self = (CString_t*)_cxmalloc_object_from_linehead( linehead );

  // --------------
  // cxmalloc metas
  // [2 QW]
  // --------------
  // [1 + 2]
  idcpy( (objectid_t*)context->tapout.line_meta, CALLABLE( CSTR__self )->Obid( CSTR__self ) );

  // --------------------------------------------
  // cxmalloc obj
  // [1 QW] (i.e. the DWORD size and DWORD flags)
  // --------------------------------------------
  CString_meta_t META = CSTR__self->meta;
  QWORD obj_members = META._bits; // this is the size and flags union data
  QWORD *cursor = context->tapout.line_obj;
  *cursor++ = obj_members;

  // -----------------------------------------------------
  // cxmalloc array
  // [variable] (i.e. the character array as whole qwords)
  // -----------------------------------------------------
  const QWORD *src = CStringValueAsQwords( CSTR__self );
  const QWORD * const end = src + CStringQwordLength( CSTR__self );
  QWORD *dest = context->tapout.line_array;
  while( src < end ) {
    *dest++ = *src++;
  }

#ifdef VGX_CONSISTENCY_CHECK
#ifdef CXMALLOC_CONSISTENCY_CHECK
  CString_flags_t *flags = &CSTR__self->meta.flags;
  char user_str[9];
  uint8_to_bin( user_str, flags->state.user.data8 );

  int init = flags->state.priv.init;
  int shrt = flags->state.priv.shrt;
  int ucsz = flags->state.priv.ucsz;
  int slen = CStringLength( CSTR__self );
  const char *data = CStringValue( CSTR__self );
  int64_t errpos = 0;
  int is_utf8 = COMLIB_check_utf8( (BYTE*)data, &errpos );

  fprintf( context->objdump, "<CString_t init=%d shrt=%d ucsz=%d user=%s len=%d utf8=%d> ", init, shrt, ucsz, user_str, slen, is_utf8 );
  fwrite( data, 1, slen, context->objdump );

#endif
#endif

  // success
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_cstring__cxmalloc_deserialize_cstring( cxmalloc_line_deserialization_context_t *context ) {

  cxmalloc_linehead_t *linehead = context->linehead;
  object_allocator_context_t *alloc_context = context->auxiliary[ CONTEXT_AUX_IDX ];
  framehash_t *key_index = (framehash_t*)context->auxiliary[ KEY_INDEX_AUX_IDX ];
  framehash_t *val_index = (framehash_t*)context->auxiliary[ VAL_INDEX_AUX_IDX ];

  // The string's memory is already allocated internally in cxmalloc, find the object address and cast it to a CString_t pointer.
  CString_t *CSTR__self = (CString_t*)_cxmalloc_object_from_linehead( linehead );

  QWORD *cursor;

  // --------------
  // cxmalloc metas
  // [2 QW]
  // --------------
  objectid_t *pobid = (objectid_t*)context->tapin.line_meta;

  // --------------------------------------------
  // cxmalloc obj
  // [1 QW] (i.e. the DWORD size and DWORD flags)
  // --------------------------------------------

  COMLIB_OBJECT_INIT( CString_t, CSTR__self, pobid );
  cursor = context->tapin.line_obj;

  // Set the size and flags
  CSTR__self->meta._bits = *cursor++;

  // ALLOCATOR CONTEXT
  CSTR__self->allocator_context = alloc_context;

  // -----------------------------------------------------
  // cxmalloc array
  // [variable] (i.e. the character array as whole qwords)
  // -----------------------------------------------------
  const QWORD *src = context->tapin.line_array;
  const QWORD * const end = src + CStringQwordLength( CSTR__self );
  QWORD *dest = (QWORD*)__CSTRING_VALUE_FROM_OBJECT( CSTR__self );
  while( src < end ) {
    *dest++ = *src++;
  }

  // Key index
  if( _vxoballoc_cstring_is_key_indexable( CSTR__self ) && key_index ) {
    shortid_t keyhash = CStringHash64( CSTR__self );
    if( CALLABLE( key_index )->SetObj( key_index, CELL_KEY_TYPE_HASH64, &keyhash, COMLIB_OBJECT( CSTR__self ) ) == CELL_VALUE_TYPE_OBJECT64 ) {
      linehead->data.refc++;
    }
    else {
      return -1;
    }
  }
  
  // Value index
  if( _vxoballoc_cstring_is_value_indexable( CSTR__self ) && val_index ) {
    if( CALLABLE( val_index )->SetObj128Nolock( val_index, pobid, COMLIB_OBJECT( CSTR__self ) ) == CELL_VALUE_TYPE_OBJECT128 ) {
      linehead->data.refc++;
    }
    else {
      return -1;
    }
  }

  // success
  return 0;

  // At this point the CString object exists in the allocator with refcnt 0.
  // Restoration code elsewhere is responsible for setting the refcnt to an appropriate value.
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxoballoc_cstring.h"

test_descriptor_t _vgx_vxoballoc_cstring_tests[] = {
  { "VGX CString Object Allocation Tests", __utest_vxoballoc_cstring },

  {NULL}
};
#endif
