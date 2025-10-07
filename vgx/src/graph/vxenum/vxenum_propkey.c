/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum_propkey.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static const char *__PROPERTY_KEY_ENUMERATION_E32_STRING   = "__vertexenum__";
static const char *__PROPERTY_KEY_ENUMERATION_NONE_STRING  = "__nullkey__";
static const char *__PROPERTY_KEY_ENUMERATION_ERROR_STRING = "__errkey__";
static const char *__PROPERTY_VAL_ENUMERATION_NONE_STRING  = "__nullval__";
static const char *__PROPERTY_VAL_ENUMERATION_ERROR_STRING = "__errval__";

// These will be created when the first enumerator instance is created and never destroyed
static int g_init = 0;
DLL_HIDDEN const CString_t *CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING   = NULL;
DLL_HIDDEN const CString_t *CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING  = NULL;
DLL_HIDDEN const CString_t *CSTR__PROPERTY_KEY_ENUMERATION_ERROR_STRING = NULL;
DLL_HIDDEN const CString_t *CSTR__PROPERTY_VAL_ENUMERATION_NONE_STRING  = NULL;
DLL_HIDDEN const CString_t *CSTR__PROPERTY_VAL_ENUMERATION_ERROR_STRING = NULL;



static CString_t * __add_key_CS( vgx_Graph_t *self, QWORD keyhash, const CString_t *CSTR__key );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propkey__new_key( vgx_Graph_t *self, const char *key ) {
  CString_t *CSTR__key = NULL;
  if( _vxenum__is_valid_storable_key( key ) ) {
    CSTR__key = icstringobject.New( self ? self->property_allocator_context : NULL, key );
  }
  return CSTR__key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propkey__new_select_key( vgx_Graph_t *self, const char *key, shortid_t *keyhash ) {
  CString_t *CSTR__key = NULL;
  if( _vxenum__is_valid_select_key( key ) ) {
    shortid_t h64 = CharsHash64( key );
    if( keyhash ) {
      *keyhash = h64;
    }
    if( self ) {
      CSTR__key = (CString_t*)_vxenum_propkey__decode_key_OPEN( self, h64 );
      if( CSTR__key != CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING ) {
        icstringobject.IncrefNolock( CSTR__key );
      }
      else {
        CSTR__key = icstringobject.New( self->ephemeral_string_allocator_context, key );
      }
    }
    else {
      CSTR__key = icstringobject.New( NULL, key );
    }
  }
  return CSTR__key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __add_key_CS( vgx_Graph_t *self, QWORD keyhash, const CString_t *CSTR__key ) {

  // Cannot add key to readonly graph
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return NULL;
  }

  // Mapped instance must use property allocator
  CString_t *CSTR__instance = OwnOrCloneCString( CSTR__key, self->property_allocator_context );
  if( CSTR__instance != NULL ) {
    // NOTE: The underlying allocator is used for both indexed and non-indexed strings. We need to tag
    // the indexed strings in order to known which ones to add to index at restore time.
    _vxoballoc_cstring_set_key_indexable( CSTR__instance );

    // Add shared key instance to map - the map will own one instance independently
    framehash_t *keymap = self->vxprop_keymap;
    if( CALLABLE( keymap )->SetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, COMLIB_OBJECT( CSTR__instance ) ) == CELL_VALUE_TYPE_OBJECT64 ) {
      // Capture and commit
      iOperation.Enumerator_CS.AddPropertyKey( self, keyhash, CSTR__instance );
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
DLL_HIDDEN shortid_t _vxenum_propkey__set_key_CS( vgx_Graph_t *self, CString_t *CSTR__key ) {
  if( CSTR__key->allocator_context != self->property_allocator_context ) {
    return 0;
  }

  QWORD keyhash = CStringHash64( CSTR__key );

  // Existing?
  framehash_t *keymap = self->vxprop_keymap;
  const CString_t *CSTR__existing = NULL;
  if( CALLABLE( keymap )->GetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, (comlib_object_t**)&CSTR__existing ) == CELL_VALUE_TYPE_OBJECT64 ) {
    if( !CALLABLE( CSTR__existing )->Equals( CSTR__existing, CSTR__key ) ) {
      return 0; // error
    }
  }
  // Add key instance (map will obtain independent ownership)
  else if( __add_key_CS( self, keyhash, CSTR__key ) == NULL ) {
    return 0; // error
  }

  return keyhash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN shortid_t _vxenum_propkey__set_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key ) {
  QWORD keyhash;
  GRAPH_LOCK( self ) {
    keyhash = _vxenum_propkey__set_key_CS( self, CSTR__key );
  } GRAPH_RELEASE;
  return keyhash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN shortid_t _vxenum_propkey__set_key_chars_CS( vgx_Graph_t *self, const char *key ) {

  // Storable key ?
  if( !_vxenum__is_valid_storable_key( key ) ) {
    return 0;
  }

  // New instance
  CString_t *CSTR__key = CStringNewAlloc( key, self->property_allocator_context );
  if( CSTR__key == NULL ) {
    return 0;
  }

  shortid_t keyhash = _vxenum_propkey__set_key_CS( self, CSTR__key );

  CStringDelete( CSTR__key );

  return keyhash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN shortid_t _vxenum_propkey__set_key_chars_OPEN( vgx_Graph_t *self, const char *key ) {
  QWORD keyhash;
  GRAPH_LOCK( self ) {
    keyhash = _vxenum_propkey__set_key_chars_CS( self, key );
  } GRAPH_RELEASE;
  return keyhash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propkey__get_key_CS( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return NULL;
  }
  CString_t *CSTR__key = NULL;
  framehash_t *keymap = self->vxprop_keymap;
  CALLABLE( keymap )->GetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, (comlib_object_t**)&CSTR__key );
  return CSTR__key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum_propkey__get_key_OPEN( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return NULL;
  }
  CString_t *CSTR__key;
  GRAPH_LOCK( self ) {
    CSTR__key = _vxenum_propkey__get_key_CS( self, keyhash );
  } GRAPH_RELEASE;
  return CSTR__key;
}



/*******************************************************************//**
 *
 * Return:  >0 : 64-bit hash code for the key string
 *           0 : error
 *
 ***********************************************************************
 */
DLL_HIDDEN shortid_t _vxenum_propkey__encode_key_CS( vgx_Graph_t *self, const CString_t *CSTR__key, CString_t **CSTR__shared_instance, bool create_nonexist ) {

  // Compute the key hash
  shortid_t keyhash = _vxenum_propkey__get_enum_CS( self, CSTR__key );

  // If we are not adding a mapping, simply return the keyhash and don't look up the mapped instance.
  if( !create_nonexist ) {
    return keyhash;
  }

  CString_t *CSTR__instance = NULL;

  // This is the graph's map instance for property keys
  framehash_t *keymap = self->vxprop_keymap;
  framehash_valuetype_t vtype;

  // Try to retrieve existing key instance 
  if( (vtype = CALLABLE( keymap )->GetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, (comlib_object_t**)&CSTR__instance )) == CELL_VALUE_TYPE_NULL ) {
    // No mapping exists for this key, create mapping.
    if( (CSTR__instance = __add_key_CS( self, keyhash, CSTR__key )) == NULL ) {
      return 0; // error
    }
  }
  else if( vtype == CELL_VALUE_TYPE_ERROR ) {
    return 0; // ???
  }

  // Return shared instance
  if( CSTR__shared_instance ) {
    *CSTR__shared_instance = CSTR__instance;
  }

  return keyhash;
}



/*******************************************************************//**
 *
 * Return:  64-bit hash code for the key string
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN uint64_t _vxenum_propkey__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__key ) {
  return CStringHash64( CSTR__key );
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN shortid_t _vxenum_propkey__encode_key_OPEN( vgx_Graph_t *self, const CString_t *CSTR__key, CString_t **CSTR__shared_instance, bool create_nonexist ) {
  shortid_t keyhash;
  GRAPH_LOCK( self ) {
    keyhash = _vxenum_propkey__encode_key_CS( self, CSTR__key, CSTR__shared_instance, create_nonexist );
  } GRAPH_RELEASE;
  return keyhash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN uint64_t _vxenum_propkey__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__key ) {
  return _vxenum_propkey__get_enum_CS( self, CSTR__key );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__discard_key_CS( vgx_Graph_t *self, CString_t *CSTR__key ) {
  // Once the refcount of the mapped string instance is about to go to zero, we must delete the mapping.
  // When the refcount is 2 it means the only owners are the vertex which is in the process of discarding
  // the property key and the reverse key map. When the last vertex discards the property key it should
  // also be removed from the reverse map.
  int64_t refcnt = icstringobject.RefcntNolock( CSTR__key );
  if( refcnt <= 2 ) { // <= 2 owners means it will go to zero

    // Not allowed to modify the map when graph is readonly!
    if( _vgx_is_readonly_CS( &self->readonly ) ) {
      return -1;
    }

    // Remove from map (framehash DOES NOT decref OBJECT64 instances automatically!)
    shortid_t namehash = CStringHash64( CSTR__key );
    if( CALLABLE( self->vxprop_keymap )->DelObj( self->vxprop_keymap, CELL_KEY_TYPE_HASH64, &namehash ) == CELL_VALUE_TYPE_OBJECT64 ) {
      // Keymap gives up ownership
      if( (refcnt = icstringobject.DecrefNolock( CSTR__key )) < 1 ) {
        // String has been destroyed
        CSTR__key = NULL;
      }
      // Mark string key as unindexed
      else {
        _vxoballoc_cstring_clear_key_indexable( CSTR__key );
      }

      // Capture but don't commit. (Lazy remove takes effect on next graph commit.)
      if( iOperation.Emitter_CS.IsReady( self ) ) {
        iOperation.Enumerator_CS.RemovePropertyKey( self, namehash );
      }
    }
  }

  if( CSTR__key ) {
    // Give up caller's ownership
    refcnt = icstringobject.DecrefNolock( CSTR__key );
  }

  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__discard_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key ) {
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propkey__discard_key_CS( self, CSTR__key );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__discard_key_by_hash_CS( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return 0;
  }
  int64_t refcnt = 0;
  CString_t *CSTR__key = (CString_t*)_vxenum_propkey__decode_key_CS( self, keyhash );
  if( CSTR__key ) {
    refcnt = _vxenum_propkey__discard_key_CS( self, CSTR__key );
  }
  else {
    refcnt = -1; // error: mapping does not exist
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__discard_key_by_hash_OPEN( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return 0;
  }
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propkey__discard_key_by_hash_CS( self, keyhash );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int64_t _vxenum_propkey__own_key_CS( vgx_Graph_t *self, CString_t *CSTR__key ) {
  return icstringobject.IncrefNolock( CSTR__key );
}



/*******************************************************************//**
 *
 *
 ************************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__own_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key ) {
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propkey__own_key_CS( self, CSTR__key );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ************************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__own_key_by_hash_CS( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return 0;
  }
  // Perform lookup
  framehash_t *keymap = self->vxprop_keymap;
  const CString_t *CSTR__key = NULL;
  if( CALLABLE( keymap )->GetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, (comlib_object_t**)&CSTR__key ) == CELL_VALUE_TYPE_OBJECT64 ) {
    return icstringobject.IncrefNolock( (CString_t*)CSTR__key );
  }
  else {
    return 0; // key doesn't exist, so no refs!
  }
}



/*******************************************************************//**
 *
 *
 ************************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__own_key_by_hash_OPEN( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return 0;
  }
  int64_t refcnt;
  GRAPH_LOCK( self ) {
    refcnt = _vxenum_propkey__own_key_by_hash_CS( self, keyhash );
  } GRAPH_RELEASE;
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ************************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_propkey__decode_key_CS( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING;
  }
  // Perform lookup
  framehash_t *keymap = self->vxprop_keymap;
  const CString_t *CSTR__key = NULL;
  if( CALLABLE( keymap )->GetObj( keymap, CELL_KEY_TYPE_HASH64, &keyhash, (comlib_object_t**)&CSTR__key ) != CELL_VALUE_TYPE_OBJECT64 ) {
    CSTR__key = CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING;
  }
  return CSTR__key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_propkey__decode_key_OPEN( vgx_Graph_t *self, shortid_t keyhash ) {
  if( IsPropertyKeyHashVertexEnum( keyhash ) ) {
    return CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING;
  }
  const CString_t *CSTR__key;
  GRAPH_LOCK( self ) {
    CSTR__key = _vxenum_propkey__decode_key_CS( self, keyhash );
  } GRAPH_RELEASE;
  return CSTR__key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_propkey__keys_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  framehash_t *keymap = self->vxprop_keymap;

  // TODO: Possible to allow this operation when graph is readonly?
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }
  
  FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &keymap->_dynamic, CSTR__output ) {
    if( CALLABLE( keymap )->GetValues( keymap ) < 0 ) {
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
DLL_HIDDEN int _vxenum_propkey__keys_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_propkey__keys_CS( self, CSTR__output );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__count_CS( vgx_Graph_t *self ) {
  framehash_t *keymap = self->vxprop_keymap;
  return CALLABLE( keymap )->Items( keymap );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__count_OPEN( vgx_Graph_t *self ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxenum_propkey__count_CS( self );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__operation_sync_CS( vgx_Graph_t *self ) {
  int64_t n_key = 0;
  CtptrList_t *CSTR__strings;
  XTRY {
    if( (CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    if( _vxenum_propkey__keys_CS( self, CSTR__strings ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    n_key = CALLABLE( CSTR__strings )->Length( CSTR__strings );
    int64_t i = 0;
    while( i < n_key ) {
      tptr_t data;
      if( CALLABLE( CSTR__strings )->Get( CSTR__strings, i, &data ) == 1 ) {
        const CString_t *CSTR__key = (const CString_t*)TPTR_GET_PTR56( &data );
        if( CSTR__key ) {
          shortid_t keyhash = _vxenum_propkey__encode_key_CS( self, CSTR__key, NULL, false );
          iOperation.Enumerator_CS.AddPropertyKey( self, keyhash, CSTR__key );
        }
      }
      ++i;
    }
    COMMIT_GRAPH_OPERATION_CS( self );
  }
  XCATCH( errcode ) {
    n_key = -1;
  }
  XFINALLY {
    if( CSTR__strings ) {
      COMLIB_OBJECT_DESTROY( CSTR__strings );
    }
  }
  return n_key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_propkey__operation_sync_OPEN( vgx_Graph_t *self ) {
  int64_t n_key = 0;
  GRAPH_LOCK( self ) {
    n_key = _vxenum_propkey__operation_sync_CS( self );
  } GRAPH_RELEASE;
  return n_key;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __rebuild_add_cstring_key( cxmalloc_object_processing_context_t *rebuild, CString_t *CSTR__key ) {
  if( CSTR__key && _vxoballoc_cstring_is_key_indexable( CSTR__key ) ) {
    framehash_t *index = (framehash_t*)rebuild->input;
    shortid_t keyhash = CStringHash64( CSTR__key );
    if( CALLABLE( index )->SetObj( index, CELL_KEY_TYPE_HASH64, &keyhash, COMLIB_OBJECT( CSTR__key ) ) == CELL_VALUE_TYPE_OBJECT64 ) {
      _cxmalloc_linehead_from_object( CSTR__key )->data.refc++;
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
DLL_HIDDEN int64_t _vxenum_propkey__rebuild_cstring_key_index( vgx_Graph_t *self ) {
  int64_t n_rebuild = 0;
  // Trigger re-creation of index from cstring allocator if empty (allocator will be empty for a brand new graph so nothing will happen)
  framehash_t *index = self->vxprop_keymap;
  if( CALLABLE( index )->Items( index ) == 0 ) {
    cxmalloc_object_processing_context_t index_rebuild_context = {0};
    index_rebuild_context.object_class = COMLIB_CLASS( CString_t );
    index_rebuild_context.process_object = (f_cxmalloc_object_processor)__rebuild_add_cstring_key;
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
      WARN( 0xD01, "Property key index rebuild object count mismatch: found %lld active, indexed %lld", n_active, n_indexed );
    }
  }
  return n_rebuild;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_prop__create_enumerator( vgx_Graph_t *self ) {
  int ret = 0;
  
  CString_t *CSTR__keymap_prefix = CStringNew( VGX_PATHDEF_CODEC_KEY_MAP_PREFIX );
  CString_t *CSTR__valmap_prefix = CStringNew( VGX_PATHDEF_CODEC_VALUE_MAP_PREFIX );
  CString_t *CSTR__keymap_name = NULL;
  CString_t *CSTR__valmap_name = NULL;
  CString_t *CSTR__mapping_fullpath = NULL;

  XTRY {
    if( CSTR__keymap_prefix == NULL || CSTR__valmap_prefix == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x441 );
    }

    if( (CSTR__keymap_name = iString.Utility.NewGraphMapName( self, CSTR__keymap_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x442 );
    }

    if( (CSTR__valmap_name = iString.Utility.NewGraphMapName( self, CSTR__valmap_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x443 );
    }

    if( (CSTR__mapping_fullpath = iString.NewFormat( NULL, "%s/" VGX_PATHDEF_INSTANCE_PROPERTY, CALLABLE(self)->FullPath(self) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x444 );
    }


    // The maps are using the property allocator context for CStrings and custom CString serialization to reference handles in the property allocator
    PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( self->property_allocator_context ) {
      // Create the key map <hash> -> <CString_t*>
      if( self->vxprop_keymap == NULL ) {
        if( (self->vxprop_keymap = iMapping.NewMap( CSTR__mapping_fullpath, CSTR__keymap_name, MAPPING_SIZE_UNLIMITED, _VXOBALLOC_CSTRING_KEY_MAP_ORDER, _VXOBALLOC_CSTRING_KEY_MAP_SPEC, CLASS_NONE )) != NULL ) {
          if( CALLABLE( self->vxprop_keymap )->Items( self->vxprop_keymap ) == 0 ) {
            if( _vxenum_propkey__rebuild_cstring_key_index( self ) < 0 ) {
              COMLIB_OBJECT_DESTROY( self->vxprop_keymap );
              self->vxprop_keymap = NULL;
            }
          }
        }
      }
      // Create the value map <obid> -> <CString_t*>
      if( self->vxprop_valmap == NULL ) {
        if( (self->vxprop_valmap = iMapping.NewMap( CSTR__mapping_fullpath, CSTR__valmap_name, MAPPING_SIZE_UNLIMITED, _VXOBALLOC_CSTRING_VALUE_MAP_ORDER, _VXOBALLOC_CSTRING_VALUE_MAP_SPEC, CLASS_CString_t )) != NULL ) {
          if( CALLABLE( self->vxprop_valmap )->Items( self->vxprop_valmap ) == 0 ) {
            if( _vxenum_propval__rebuild_cstring_value_index( self ) < 0 ) {
              COMLIB_OBJECT_DESTROY( self->vxprop_valmap );
              self->vxprop_valmap = NULL;
            }
          }
        }
      }
    } POP_STRING_ALLOCATOR_CONTEXT;

    // Check
    if( self->vxprop_keymap == NULL || self->vxprop_valmap == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x446 );
    }

  }
  XCATCH( errcode ) {
    _vxenum_prop__destroy_enumerator( self );
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__mapping_fullpath );
    iString.Discard( &CSTR__keymap_name );
    iString.Discard( &CSTR__valmap_name );
    CStringDelete( CSTR__keymap_prefix );
    CStringDelete( CSTR__valmap_prefix );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxenum_prop__destroy_enumerator( vgx_Graph_t *self ) {
  iMapping.DeleteMap( &self->vxprop_keymap );  
  iMapping.DeleteMap( &self->vxprop_valmap );  
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_prop__initialize( void ) {
  // Initialize constants
  if( !g_init ) {
    CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING   = iString.New( NULL, __PROPERTY_KEY_ENUMERATION_E32_STRING );
    CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING  = iString.New( NULL, __PROPERTY_KEY_ENUMERATION_NONE_STRING );
    CSTR__PROPERTY_KEY_ENUMERATION_ERROR_STRING = iString.New( NULL, __PROPERTY_KEY_ENUMERATION_ERROR_STRING );
    CSTR__PROPERTY_VAL_ENUMERATION_NONE_STRING  = iString.New( NULL, __PROPERTY_VAL_ENUMERATION_NONE_STRING );
    CSTR__PROPERTY_VAL_ENUMERATION_ERROR_STRING = iString.New( NULL, __PROPERTY_VAL_ENUMERATION_ERROR_STRING );

    if( 
        !CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING   ||
        !CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING  ||
        !CSTR__PROPERTY_KEY_ENUMERATION_ERROR_STRING ||
        !CSTR__PROPERTY_VAL_ENUMERATION_NONE_STRING  ||
        !CSTR__PROPERTY_VAL_ENUMERATION_ERROR_STRING 
      )
    {
      return -1;
    }
    g_init = 1;
    return 1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_prop__destroy( void ) {
  if( g_init ) {
    iString.Discard( (CString_t**)&CSTR__PROPERTY_KEY_ENUMERATION_E32_STRING );
    iString.Discard( (CString_t**)&CSTR__PROPERTY_KEY_ENUMERATION_NONE_STRING );
    iString.Discard( (CString_t**)&CSTR__PROPERTY_KEY_ENUMERATION_ERROR_STRING );
    iString.Discard( (CString_t**)&CSTR__PROPERTY_VAL_ENUMERATION_NONE_STRING );
    iString.Discard( (CString_t**)&CSTR__PROPERTY_VAL_ENUMERATION_ERROR_STRING );
    return 1;
  }
  return 0;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum_propkey.h"
  
test_descriptor_t _vgx_vxenum_propkey_tests[] = {
  { "VGX Graph Property Key Enumeration Tests", __utest_vxenum_propkey },
  {NULL}
};
#endif
