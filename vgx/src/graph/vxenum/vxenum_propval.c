/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum_propval.c
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

#include "_vxenum.h"
#include "_vxoballoc_cstring.h"
#include "_vxoperation.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




static CString_t * __add_value_CS( vgx_Graph_t *self, const objectid_t *value_obid, const CString_t *CSTR__value );




/**************************************************************************//**
 * _vxenum_propval__obid
 *
 ******************************************************************************
 */
DLL_HIDDEN objectid_t _vxenum_propval__obid( const CString_t *CSTR__value ) {  
  const objectid_t *obid = CStringObid( CSTR__value );
  return *obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __add_value_CS( vgx_Graph_t *self, const objectid_t *value_obid, const CString_t *CSTR__value ) {
  // Can only modify map if graph is not readonly!
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return NULL;
  }

  // Mapped instance must use property allocator
  CString_t *CSTR__instance = OwnOrCloneCString( CSTR__value, self->property_allocator_context );
  if( CSTR__instance != NULL ) {

    // NOTE: The underlying allocator is used for both indexed and non-indexed strings. We need to tag
    // the indexed strings in order to known which ones to add to index at restore time.
    _vxoballoc_cstring_set_value_indexable( CSTR__instance );

    // Add shared value instance to map - the map will own one instance independently
    framehash_t *valmap = self->vxprop_valmap;
    if( CALLABLE( valmap )->SetObj128Nolock( valmap, value_obid, COMLIB_OBJECT( CSTR__instance ) ) == CELL_VALUE_TYPE_OBJECT128 ) {
      // Capture and commit
      iOperation.Enumerator_CS.AddStringValue( self, value_obid, CSTR__instance );
      COMMIT_GRAPH_OPERATION_CS( self );
    }
    else {
      CStringDelete( CSTR__instance );
      CSTR__instance = NULL;
    }
  }

  return CSTR__instance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__store_value_chars_CS( vgx_Graph_t *self, const char *value, int32_t len ) {
  CString_t *CSTR__instance = NULL;

  // New instance
  CString_constructor_args_t args = {
    .string       = value,
    .len          = len,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = self->property_allocator_context
  };

  CString_t *CSTR__tmp;
  if( (CSTR__tmp = COMLIB_OBJECT_NEW( CString_t, NULL, &args )) != NULL ) {
    // Add to map or retrieve existing
    CSTR__instance = _vxenum_propval__store_value_CS( self, CSTR__tmp );
    CStringDelete( CSTR__tmp );
  }

  return CSTR__instance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__store_value_chars_OPEN( vgx_Graph_t *self, const char *value, int32_t len ) {
  CString_t *CSTR__instance = NULL;
  GRAPH_LOCK( self ) {
    CSTR__instance = _vxenum_propval__store_value_chars_CS( self, value, len );
  } GRAPH_RELEASE;
  return CSTR__instance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__store_value_CS( vgx_Graph_t *self, const CString_t *CSTR__value ) {
  objectid_t propid = _vxenum_propval__obid( CSTR__value );

  CString_t *CSTR__instance = NULL;

  // This is the graph's map instance for property values
  framehash_t *valmap = self->vxprop_valmap;
  framehash_valuetype_t vtype;

  // Try to retrieve existing value instance 
  if( (vtype = CALLABLE( valmap )->GetObj128Nolock( valmap, &propid, (comlib_object_t**)&CSTR__instance )) == CELL_VALUE_TYPE_NULL ) {
    // No previous value, add to map
    CSTR__instance = __add_value_CS( self, &propid, CSTR__value );
  }

  return CSTR__instance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__store_value_OPEN( vgx_Graph_t *self, const CString_t *CSTR__value ) {
  CString_t *CSTR__mapped_value;
  GRAPH_LOCK( self ) {
    CSTR__mapped_value = _vxenum_propval__store_value_CS( self, CSTR__value );
  } GRAPH_RELEASE;
  return CSTR__mapped_value;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__get_value_CS( vgx_Graph_t *self, const objectid_t * value_obid ) {
  CString_t *CSTR__instance = NULL;
  framehash_t *valmap = self->vxprop_valmap;
  CALLABLE( valmap )->GetObj128Nolock( valmap, value_obid, (comlib_object_t**)&CSTR__instance );
  return CSTR__instance;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propval__get_value_OPEN( vgx_Graph_t *self, const objectid_t * value_obid ) {
  CString_t *CSTR__value;
  GRAPH_LOCK( self ) {
    CSTR__value = _vxenum_propval__get_value_CS( self, value_obid );
  } GRAPH_RELEASE;
  return CSTR__value;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__discard_value_CS( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance ) {
  // Once the refcount of the mapped string instance is about to go to zero, we must delete the mapping.
  // When the refcount is 2 it means the only owners are the vertex which is in the process of discarding
  // the property value and the value map. When the last vertex discards the property value it should
  // also be removed from the value map.
  int64_t refcnt = icstringobject.RefcntNolock( CSTR__shared_value_instance );
  if( refcnt <= 2 ) { // <= 2 owners means it will go to zero

    // Not allowed to remove mapping in readonly mode
    if( _vgx_is_readonly_CS( &self->readonly ) ) {
      // This should normally only occur if someone on the outside is performing WRITABLE operations,
      // which can't happen if graph is readonly.
      return -1; // "should never happen"
    }

    // Remove from map (framehash will decref OBJECT128 instances automatically!)
    objectid_t propid = _vxenum_propval__obid( CSTR__shared_value_instance );

    if( CALLABLE( self->vxprop_valmap )->DelObj128Nolock( self->vxprop_valmap, &propid ) == CELL_VALUE_TYPE_OBJECT128 ) {
      // Map no longer owns string
      if( --refcnt < 1 ) {
        // Zero owners (string has been destroyed)
        CSTR__shared_value_instance = NULL;
      }
      // Mark string value as unindexed if it still exists
      else {
        _vxoballoc_cstring_clear_value_indexable( CSTR__shared_value_instance );
      }
      // Capture but don't commit. (Lazy remove takes effect on next graph commit.)
      if( iOperation.Emitter_CS.IsReady( self ) ) {
        iOperation.Enumerator_CS.RemoveStringValue( self, &propid );
      }
    }
  }

  if( CSTR__shared_value_instance ) {
    // Give up caller's ownership
    refcnt = icstringobject.DecrefNolock( CSTR__shared_value_instance );
  }

  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__discard_value_OPEN( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance ) {
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propval__discard_value_CS( self, CSTR__shared_value_instance );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int64_t _vxenum_propval__own_value_CS( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance ) {
  return icstringobject.IncrefNolock( CSTR__shared_value_instance );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__own_value_OPEN( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance ) {
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propval__own_value_CS( self, CSTR__shared_value_instance );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_propval__values_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  framehash_t *decoder = self->vxprop_valmap;

  // TODO: Possible to allow this operation when graph is readonly?
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &decoder->_dynamic, CSTR__output ) {
    if( CALLABLE( decoder )->GetValues( decoder ) < 0 ) {
      ret = -1;
    }
  } FRAMEHASH_DYNAMIC_POP_REF_LIST;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_propval__values_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_propval__values_CS( self, CSTR__output );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__count_CS( vgx_Graph_t *self ) {
  framehash_t *decoder = self->vxprop_valmap;
  return CALLABLE( decoder )->Items( decoder );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__count_OPEN( vgx_Graph_t *self ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxenum_propval__count_CS( self );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__operation_sync_CS( vgx_Graph_t *self ) {
  int64_t n_val = 0;
  CtptrList_t *CSTR__strings;
  XTRY {
    if( (CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    if( _vxenum_propval__values_CS( self, CSTR__strings ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    n_val = CALLABLE( CSTR__strings )->Length( CSTR__strings );
    
    // Counters needed by emitter checkpoint
    int64_t cnt = 0;
    int64_t delta_counter = 0;
    cxmalloc_object_processing_context_t sync_context = {0};
    sync_context.output = &cnt; // ++ each iteration
    sync_context.input = &delta_counter; // +obj_delta once in a while

    int64_t i = 0;
    int64_t szout = 0;
    int64_t pending = 0;
    while( i < n_val ) {
      tptr_t data;
      if( CALLABLE( CSTR__strings )->Get( CSTR__strings, i, &data ) == 1 ) {
        const CString_t *CSTR__value = (const CString_t*)TPTR_GET_POINTER( &data );
        if( CSTR__value ) {
          objectid_t value_obid = _vxenum_propval__obid( CSTR__value );
          if( iOperation.Enumerator_CS.AddStringValue( self, &value_obid, CSTR__value ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
          }

          // (Over)estimate resulting bytes in the emitter output
          // ____sea_10E0051C_VARSTR_hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
          szout += 64 + icstringobject.SerializedSize( CSTR__value );
          if( szout > __TX_COMMIT_SIZE_LIMIT ) {
            COMMIT_GRAPH_OPERATION_CS( self );
          }
          if( (pending = _vxdurable_operation__emitter_checkpoint_CS( self, &sync_context, 1 )) > 0 ) {
            szout = pending;
          }
        }
      }
      ++i;
    }
    COMMIT_GRAPH_OPERATION_CS( self );
  }
  XCATCH( errcode ) {
    n_val = -1;
  }
  XFINALLY {
    if( CSTR__strings ) {
      COMLIB_OBJECT_DESTROY( CSTR__strings );
    }
  }
  return n_val;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__operation_sync_OPEN( vgx_Graph_t *self ) {
  int64_t n_val = 0;
  GRAPH_LOCK( self ) {
    n_val = _vxenum_propval__operation_sync_CS( self );
  } GRAPH_RELEASE;
  return n_val;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __rebuild_add_cstring_value( cxmalloc_object_processing_context_t *rebuild, CString_t *CSTR__val ) {
  if( CSTR__val && _vxoballoc_cstring_is_value_indexable( CSTR__val ) ) {
    framehash_t *index = (framehash_t*)rebuild->input;
    objectid_t propid = _vxenum_propval__obid( CSTR__val );
    if( CALLABLE( index )->SetObj128Nolock( index, &propid, COMLIB_OBJECT( CSTR__val ) ) == CELL_VALUE_TYPE_OBJECT128 ) {
      _cxmalloc_linehead_from_object( CSTR__val )->data.refc++;
      return 1;
    }
    else {
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propval__rebuild_cstring_value_index( vgx_Graph_t *self ) {
  int64_t n_rebuild = 0;
  // Trigger re-creation of index from cstring allocator if empty (allocator will be empty for a brand new graph so nothing will happen)
  framehash_t *index = self->vxprop_valmap;
  if( CALLABLE( index )->Items( index ) == 0 ) {
    cxmalloc_object_processing_context_t index_rebuild_context = {0};
    index_rebuild_context.object_class = COMLIB_CLASS( CString_t );
    index_rebuild_context.process_object = (f_cxmalloc_object_processor)__rebuild_add_cstring_value;
    index_rebuild_context.input = index;
    cxmalloc_family_t *alloc = (cxmalloc_family_t*)self->property_allocator_context->allocator;
    CALLABLE( alloc )->ProcessObjects( alloc, &index_rebuild_context );
    int64_t n_indexed = CALLABLE( index )->Items( index );
    int64_t n_active = index_rebuild_context.n_objects_active;
    if( n_active == n_indexed ) {
      if( n_indexed > 0 ) {
#ifdef CSTRING_INDEX_PERSIST
        // Persist the restored index to disk
        if( CALLABLE( index )->BulkSerialize( index, true ) < 0 ) {
          n_rebuild = -1;
        }
        else {
          n_rebuild = n_indexed;
        }
#else
        n_rebuild = n_indexed;
#endif
      }
    }
    else {
      WARN( 0xD02, "Property value index rebuild object count mismatch: found %lld active, indexed %lld", n_active, n_indexed );
    }
  }
  return n_rebuild;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum_propval.h"
  
test_descriptor_t _vgx_vxenum_propval_tests[] = {
  { "VGX Graph Property String Value Enumeration Tests", __utest_vxenum_propval },
  {NULL}
};
#endif
