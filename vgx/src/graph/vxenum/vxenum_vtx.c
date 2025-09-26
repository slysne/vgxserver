/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum_vtx.c
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

#include "_vxenum.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static const char *__VERTEX_TYPE_ENUMERATION_NONE_STRING        = "__null__";
static const char *__VERTEX_TYPE_ENUMERATION_VERTEX_STRING      = "__vertex__";
static const char *__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING      = "__INTERNAL__";
static const char *__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING     = "__UNKNOWN__";
static const char *__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING  = "lock_";

// These will be created when the first enumerator instance is created and never destroyed
static int g_init = 0;
DLL_HIDDEN const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING       = NULL;
DLL_HIDDEN const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING     = NULL;
DLL_HIDDEN const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING     = NULL;
DLL_HIDDEN const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING    = NULL;
DLL_HIDDEN const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING = NULL;


static QWORD g_vertex_typehash = 0;
static QWORD g_lock_typehash = 0;


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_vertex_type_encoder_cache_entry {
  vgx_Graph_t       *graph;   // context
  QWORD             typehash; // KEY
  vgx_vertex_type_t typecode; // VALUE
} __vertex_type_encoder_cache_entry;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_vertex_type_decoder_cache_entry {
  vgx_Graph_t       *graph;       // context
  vgx_vertex_type_t typecode;     // KEY
  const CString_t   *CSTR__tname; // VALUE
} __vertex_type_decoder_cache_entry;



static __vertex_type_encoder_cache_entry g_vtx_enc_cache_CS = {0};
static __vertex_type_decoder_cache_entry g_vtx_dec_cache_CS = {0};

static vgx_vertex_type_t __invalidate_encoder_cache_entry( __vertex_type_encoder_cache_entry *entry, vgx_vertex_type_t code );
static vgx_vertex_type_t __invalidate_decoder_cache_entry( __vertex_type_decoder_cache_entry *entry, vgx_vertex_type_t code );

static vgx_vertex_type_t __verify_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
static vgx_vertex_type_t __add_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode, CString_t **CSTR__mapped_instance );
static vgx_vertex_type_t __set_vertex_type_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode, CString_t **CSTR__mapped_instance );
static vgx_vertex_type_t __encode_user_vertex_type_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance );
static int               __remove_vertex_type_mapping_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype );



static vgx_vertex_type_t __invalidate_encoder_cache_entry( __vertex_type_encoder_cache_entry *entry, vgx_vertex_type_t code ) {
  entry->graph = NULL;
  entry->typehash = 0;
  entry->typecode = code;
  return code;
}


static vgx_vertex_type_t __invalidate_decoder_cache_entry( __vertex_type_decoder_cache_entry *entry, vgx_vertex_type_t code ) {
  entry->graph = NULL;
  entry->typecode = code;
  entry->CSTR__tname = NULL;
  return code;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_vertex_type_t __verify_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode ) {
  framehash_t *decoder = self->vxtype_decoder;
  framehash_t *encoder = self->vxtype_encoder;

  // Check if mapping already exists
  QWORD objkey = typecode;
  const CString_t *CSTR__existing_entry = NULL;
  int64_t mapped_typecode = 0;

  // Does mapping typehash->typecode exist? 
  if( CALLABLE( encoder )->GetInt56( encoder, CELL_KEY_TYPE_HASH64, typehash, &mapped_typecode ) == CELL_VALUE_TYPE_INTEGER ) {
    // Same typecode ?
    if( mapped_typecode == typecode ) {
      // Retrive mapped string
      if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__existing_entry ) == CELL_VALUE_TYPE_NULL ) {
        return VERTEX_TYPE_ENUMERATION_ERROR;
      }
      // Verify string match
      else if( !CALLABLE( CSTR__vertex_type_name )->Equals( CSTR__vertex_type_name, CSTR__existing_entry ) ) {
        return VERTEX_TYPE_ENUMERATION_COLLISION;
      }
      // Existing mapping verified
      else {
        return typecode;
      }
    }
    // Typecode mismatch
    else {
      return VERTEX_TYPE_ENUMERATION_COLLISION;
    }
  }
  // No mapping
  else if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__existing_entry ) == CELL_VALUE_TYPE_NULL ) {
    return VERTEX_TYPE_ENUMERATION_NO_MAPPING;
  }
  // typehash miss but string decode hit, mismatch implied
  else {
    return VERTEX_TYPE_ENUMERATION_COLLISION;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_vertex_type_t __add_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode, CString_t **CSTR__mapped_instance ) {
  framehash_t *decoder = self->vxtype_decoder;
  framehash_t *encoder = self->vxtype_encoder;

  QWORD objkey = typecode;
  CString_t *CSTR__instance = NULL;

  XTRY {

    // Mapped instance must use property allocator
    if( (CSTR__instance = OwnOrCloneCString( CSTR__vertex_type_name, self->property_allocator_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Insert reverse mapping: typecode -> CString_t*
    if( CALLABLE( decoder )->SetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, COMLIB_OBJECT( CSTR__instance ) ) != CELL_VALUE_TYPE_OBJECT64 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
        
    // Insert forward mapping: typehash -> typecode
    if( CALLABLE( encoder )->SetInt56( encoder, CELL_KEY_TYPE_HASH64, typehash, typecode ) != CELL_VALUE_TYPE_INTEGER ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Capture and commit (user defined types only)
    if( __vertex_type_enumeration_in_user_range( typecode ) ) {
      iOperation.Enumerator_CS.AddVertexType( self, typehash, CSTR__instance, typecode );
      COMMIT_GRAPH_OPERATION_CS( self );
    }

  }
  XCATCH( errcode ) {
    CALLABLE( encoder )->DelKey64( encoder, CELL_KEY_TYPE_HASH64, typehash );
    CALLABLE( decoder )->DelObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey );
    icstringobject.Delete( CSTR__instance );
    CSTR__instance = NULL;
    typecode = VERTEX_TYPE_ENUMERATION_ERROR;
  }
  XFINALLY {
    if( CSTR__mapped_instance ) {
      *CSTR__mapped_instance = CSTR__instance;
    }
  }

  return typecode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_vertex_type_t __set_vertex_type_mapping_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode, CString_t **CSTR__mapped_instance ) {
  // Make sure we have a valid mapping
  if( !__vertex_type_enumeration_in_user_range( typecode ) ) {
    return VERTEX_TYPE_ENUMERATION_INVALID;
  }

  vgx_vertex_type_t ret;
  if( (ret = __verify_mapping_CS( self, typehash, CSTR__vertex_type_name, typecode )) == VERTEX_TYPE_ENUMERATION_NO_MAPPING ) {
    return __add_mapping_CS( self, typehash, CSTR__vertex_type_name, typecode, CSTR__mapped_instance );
  }
  else {
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_vertex_type_t __encode_user_vertex_type_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance ) {

  int range = __VERTEX_TYPE_ENUMERATION_USER_RANGE_SIZE;
  int offset = __VERTEX_TYPE_ENUMERATION_START_USER_RANGE;
  int n = 0;
 
  vgx_vertex_type_t typecode;

  do {

    // Special case: __vertex__
    if( CStringEquals( CSTR__vertex_type_name, CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING ) ) {
      typecode = VERTEX_TYPE_ENUMERATION_VERTEX;
    }
    // Special case: lock_
    else if( CStringEquals( CSTR__vertex_type_name, CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING ) ) {
      typecode = VERTEX_TYPE_ENUMERATION_LOCKOBJECT;
    }
    // Derive a candidate vertex type code from the hash code
    else {
      typecode = (vgx_vertex_type_t)((typehash + n) % range + offset);
    }

    // Lookup
    vgx_vertex_type_t existing_typecode = __verify_mapping_CS( self, typehash, CSTR__vertex_type_name, typecode );
    // No existing mapping - Accept the candidate
    if( existing_typecode == VERTEX_TYPE_ENUMERATION_NO_MAPPING ) {
      // No forward collision, add mapping
      if( CALLABLE( self->vxtype_encoder )->HasKey64( self->vxtype_encoder, CELL_KEY_TYPE_HASH64, typehash ) == CELL_VALUE_TYPE_NULL ) {
        // Add mapping
        if( __add_mapping_CS( self, typehash, CSTR__vertex_type_name, typecode, CSTR__mapped_instance ) == typecode ) {
          // Return encoded vertex type
          return typecode;
        }
        // Failed
        else {
          return VERTEX_TYPE_ENUMERATION_ERROR;
        }
      }
      // Forward collision
      else {
        return VERTEX_TYPE_ENUMERATION_NO_MAPPING;
      }
    }
    // Mapping already exists - just return it
    else if( __vertex_type_enumeration_valid( existing_typecode ) ) { 
      return existing_typecode;
    }
    
    // Collision on typecode candidate, try again as long as we're in user range
  } while( ++n < range );

  // Map full
  return VERTEX_TYPE_ENUMERATION_NO_MAPPING;
}



/*******************************************************************//**
 *
 * Returns: 1 : mapping removed
 *          0 : mapping did not exist, no action
 *         -1 : error
 ***********************************************************************
 */
static int __remove_vertex_type_mapping_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype ) {

  framehash_t *decoder = self->vxtype_decoder;
  framehash_t *encoder = self->vxtype_encoder;

  QWORD objkey = vxtype;
  CString_t *CSTR__vertex_type_name = NULL;
  
  // Invalidate all caches
  _vxenum_vtx__clear_cache();

  int rev_deleted = 0;
  int fwd_deleted = 0;
  // Retrieve the string instance by reverse lookup
  if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__vertex_type_name ) == CELL_VALUE_TYPE_OBJECT64 ) {
    // Compute the vertex type name hash
    QWORD vtxhash = CStringHash64( CSTR__vertex_type_name );
    // Remove the reverse mapping
    if( (rev_deleted = CALLABLE( decoder )->DelObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey ) == CELL_VALUE_TYPE_OBJECT64) == 1 ) {
      // Capture but don't commit. (Lazy remove on next graph commit.)
      if( iOperation.Emitter_CS.IsReady( self ) ) {
        iOperation.Enumerator_CS.RemoveVertexType( self, vtxhash, vxtype );
      }

      // Decoder gives up reference
      icstringobject.DecrefNolock( CSTR__vertex_type_name );

      // Remove the forward mapping
      fwd_deleted = CALLABLE( encoder )->DelKey64( encoder, CELL_KEY_TYPE_HASH64, vtxhash ) == CELL_VALUE_TYPE_INTEGER;

    }
  }
  else {
    return 0; // no action
  }



  // Check status
  if( fwd_deleted && rev_deleted ) {
    return 1; // mapping deleted
  }
  else {
    return -1; // error
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__remove_mapping_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype ) {
  // Not allowed to remove from the map if graph is readonly
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  return __remove_vertex_type_mapping_CS( self, vxtype );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__remove_mapping_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vxtype ) {
  int ret;
  GRAPH_LOCK( self ) {
    ret = _vxenum_vtx__remove_mapping_CS( self, vxtype );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN const CString_t * _vxenum_vtx__reserved_None( vgx_Graph_t *self ) {
  return CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN const CString_t * _vxenum_vtx__reserved_Vertex( vgx_Graph_t *self ) {
  return CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN const CString_t * _vxenum_vtx__reserved_System( vgx_Graph_t *self ) {
  return CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN const CString_t * _vxenum_vtx__reserved_Unknown( vgx_Graph_t *self ) {
  return CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN const CString_t * _vxenum_vtx__reserved_LockObject( vgx_Graph_t *self ) {
  return CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxenum_vtx__clear_cache( void ) {
  __invalidate_encoder_cache_entry( &g_vtx_enc_cache_CS, VERTEX_TYPE_ENUMERATION_NONE );
  __invalidate_decoder_cache_entry( &g_vtx_dec_cache_CS, VERTEX_TYPE_ENUMERATION_NONE );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_vertex_type_t _vxenum_vtx__set_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode ) {
  // Not a valid storable key
  if( !_vxenum__is_valid_storable_key( CStringValue( CSTR__vertex_type_name ) ) ) {
    return VERTEX_TYPE_ENUMERATION_INVALID; // TODO: ALSO ALLOW __vertex__ !!!
  }

  // Readonly graph
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return VERTEX_TYPE_ENUMERATION_LOCKED;
  }

  // Assign
  vgx_vertex_type_t ret;
  CString_t *CSTR__mapped_instance = NULL;
  if( (ret = __set_vertex_type_mapping_CS( self, typehash, CSTR__vertex_type_name, typecode, &CSTR__mapped_instance )) == typecode ) {
    // Encoder cache
    g_vtx_enc_cache_CS.graph = self;
    g_vtx_enc_cache_CS.typehash = typehash;
    g_vtx_enc_cache_CS.typecode = typecode;
    // Decoder cache
    g_vtx_dec_cache_CS.graph = self;
    g_vtx_dec_cache_CS.typecode = typecode;
    g_vtx_dec_cache_CS.CSTR__tname = CSTR__mapped_instance;
    return typecode;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_vertex_type_t _vxenum_vtx__set_OPEN( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode ) {
  vgx_vertex_type_t vtx;
  GRAPH_LOCK( self ) {
    vtx = _vxenum_vtx__set_CS( self, typehash, CSTR__vertex_type_name, typecode );
  } GRAPH_RELEASE;
  return vtx;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_vertex_type_t _vxenum_vtx__encode_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance, bool create_nonexist ) {
  QWORD typehash;
  const char *name;

  // Default type if NULL or ""
  if( CSTR__vertex_type_name == NULL || *(name = CStringValue( CSTR__vertex_type_name )) == '\0' ) {
    CSTR__vertex_type_name = CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
    typehash = g_vertex_typehash;
  }
  // "lock_"
  else if( CStringEquals( CSTR__vertex_type_name, CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING ) ) {
    typehash = g_lock_typehash;
  }
  // More checks before computing typehash from string
  else {
    // Lookup and create new if no encoding exists
    if( create_nonexist ) {
      // Not a valid storable key
      if( !_vxenum__is_valid_storable_key( name ) ) {
        return VERTEX_TYPE_ENUMERATION_INVALID;
      }
    }
    // Lookup only
    else {
      // Wildcard select
      if( *name == '*' && *(++name) == '\0' ) {
        return VERTEX_TYPE_ENUMERATION_WILDCARD;
      }
      // Not a valid select key
      else if( !_vxenum__is_valid_select_key( name ) ) {
        return VERTEX_TYPE_ENUMERATION_INVALID;
      }
    }
    // Compute hash from string
    typehash = CStringHash64( CSTR__vertex_type_name );
  }

  // Cache hit: Return cached version immediately
  if( g_vtx_enc_cache_CS.typehash == typehash && g_vtx_enc_cache_CS.graph == self ) {
    return g_vtx_enc_cache_CS.typecode;
  }

  // Cache miss: Do work
  // Update the cache key
  g_vtx_enc_cache_CS.typehash = typehash;
  g_vtx_enc_cache_CS.graph = self;

  // Look up the encoded vertex type, create if requested.
  int64_t typecode_entry;
  framehash_t *encoder = self->vxtype_encoder;
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, g_vtx_enc_cache_CS.typehash, &typecode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    // Update the cache value
    g_vtx_enc_cache_CS.typecode = (vgx_vertex_type_t)typecode_entry;
    return g_vtx_enc_cache_CS.typecode;
  }
  // Encoding does not exist
  else if( !create_nonexist ) {
    return __invalidate_encoder_cache_entry( &g_vtx_enc_cache_CS, VERTEX_TYPE_ENUMERATION_NONEXIST );
  }
  // No entry for this vertex type - create new entry unless readonly
  else if( _vgx_is_writable_CS( &self->readonly ) ) {
    // ADD ENCODING TO MAP - Update the cache value
    if( __vertex_type_enumeration_valid( (g_vtx_enc_cache_CS.typecode = __encode_user_vertex_type_CS( self, g_vtx_enc_cache_CS.typehash, CSTR__vertex_type_name, CSTR__mapped_instance )) ) ) {
      return g_vtx_enc_cache_CS.typecode;
    }
    // ERROR
    else {
      return __invalidate_encoder_cache_entry( &g_vtx_enc_cache_CS, g_vtx_enc_cache_CS.typecode );
    }
  }
  // Readonly graph (invalidate cache)
  else {
    return __invalidate_encoder_cache_entry( &g_vtx_enc_cache_CS, VERTEX_TYPE_ENUMERATION_LOCKED );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN uint64_t _vxenum_vtx__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name ) {
  QWORD typehash;
  const char *name;

  // Default type if NULL or ""
  if( CSTR__vertex_type_name == NULL || *(name = CStringValue( CSTR__vertex_type_name )) == '\0' ) {
    CSTR__vertex_type_name = CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
    typehash = g_vertex_typehash;
  }
  // "lock_"
  else if( CStringEquals( CSTR__vertex_type_name, CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING ) ) {
    typehash = g_lock_typehash;
  }
  // More checks before computing typehash from string
  else {
    // Wildcard select
    if( *name == '*' && *(++name) == '\0' ) {
      return VERTEX_TYPE_ENUMERATION_WILDCARD;
    }
    // Not a valid select key
    else if( !_vxenum__is_valid_select_key( name ) ) {
      return VERTEX_TYPE_ENUMERATION_INVALID;
    }
    // Compute hash from string
    typehash = CStringHash64( CSTR__vertex_type_name );
  }

  // Cache hit: Return cached version immediately
  if( g_vtx_enc_cache_CS.typehash == typehash && g_vtx_enc_cache_CS.graph == self ) {
    return g_vtx_enc_cache_CS.typecode;
  }

  // Cache miss: Do work
  // Update the cache key
  g_vtx_enc_cache_CS.typehash = typehash;
  g_vtx_enc_cache_CS.graph = self;

  // Look up the encoded vertex type, create if requested.
  int64_t typecode_entry;
  framehash_t *encoder = self->vxtype_encoder;
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, g_vtx_enc_cache_CS.typehash, &typecode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    // Update the cache value
    g_vtx_enc_cache_CS.typecode = (vgx_vertex_type_t)typecode_entry;
    return g_vtx_enc_cache_CS.typecode;
  }

  // Encoding does not exist
  return __invalidate_encoder_cache_entry( &g_vtx_enc_cache_CS, VERTEX_TYPE_ENUMERATION_NONEXIST );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_vertex_type_t _vxenum_vtx__encode_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance, bool create_nonexist ) {
  vgx_vertex_type_t vertex_type;
  GRAPH_LOCK( self ) {
    vertex_type = _vxenum_vtx__encode_CS( self, CSTR__vertex_type_name, CSTR__mapped_instance, create_nonexist );
  } GRAPH_RELEASE;
  return vertex_type;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN uint64_t _vxenum_vtx__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name ) {
  uint64_t e;
  GRAPH_LOCK( self ) {
    e = _vxenum_vtx__get_enum_CS( self, CSTR__vertex_type_name );
  } GRAPH_RELEASE;
  return e;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_vtx__decode_CS( vgx_Graph_t *self, vgx_vertex_type_t vertex_type ) {
  // Cache hit
  if( vertex_type == g_vtx_dec_cache_CS.typecode && self == g_vtx_dec_cache_CS.graph ) {
    return g_vtx_dec_cache_CS.CSTR__tname;
  }

  // Cache miss
  switch( vertex_type ) {
  case VERTEX_TYPE_ENUMERATION_VERTEX:
    return CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
  case VERTEX_TYPE_ENUMERATION_LOCKOBJECT:
    return CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING;
  default:
    if( __vertex_type_enumeration_valid( vertex_type ) ) {
      // Update cache key and context
      g_vtx_dec_cache_CS.typecode = vertex_type;
      g_vtx_dec_cache_CS.graph = self;
      // Perform lookup
      framehash_t *decoder = self->vxtype_decoder;
      QWORD objkey = g_vtx_dec_cache_CS.typecode;
      if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&g_vtx_dec_cache_CS.CSTR__tname ) != CELL_VALUE_TYPE_OBJECT64 ) {
        g_vtx_dec_cache_CS.CSTR__tname = CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING;
      }
      return g_vtx_dec_cache_CS.CSTR__tname;
    }
    else {
      return CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_vtx__decode_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vertex_type ) {
  const CString_t *CSTR__vertex_type_name;
  switch( vertex_type ) {
  case VERTEX_TYPE_ENUMERATION_VERTEX:
    CSTR__vertex_type_name = CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
    break;
  case VERTEX_TYPE_ENUMERATION_LOCKOBJECT:
    CSTR__vertex_type_name = CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING;
    break;
  default:
    GRAPH_LOCK( self ) {
      CSTR__vertex_type_name = _vxenum_vtx__decode_CS( self, vertex_type );
    } GRAPH_RELEASE;
  }
  return CSTR__vertex_type_name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_vtx__has_mapping_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name ) {
  QWORD typehash;
  framehash_t *encoder = self->vxtype_encoder;
  // Default type if NULL or ""
  if( CSTR__vertex_type_name == NULL || *CStringValue( CSTR__vertex_type_name ) == '\0') {
    typehash = g_vertex_typehash;
  }
  // "lock_"
  else if( CStringEquals( CSTR__vertex_type_name, CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING ) ) {
    typehash = g_lock_typehash;
  }
  else {
    typehash = CStringHash64( CSTR__vertex_type_name );
  }
  // Lookup
  if( CALLABLE(encoder)->HasKey64( encoder, CELL_KEY_TYPE_HASH64, typehash ) == CELL_VALUE_TYPE_INTEGER ) {
    return true;
  }
  return false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_vtx__has_mapping_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name ) {
  bool exists;
  GRAPH_LOCK( self ) {
    exists = _vxenum_vtx__has_mapping_CS( self, CSTR__vertex_type_name );
  } GRAPH_RELEASE;
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_vtx__has_mapping_vtx_CS( vgx_Graph_t *self, vgx_vertex_type_t vertex_type ) {
  if( __vertex_type_enumeration_valid( vertex_type ) ) {
    framehash_t *decoder = self->vxtype_decoder;
    QWORD objkey = vertex_type;
    const CString_t *CSTR__tname;
    if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__tname ) == CELL_VALUE_TYPE_OBJECT64 ) {
      g_vtx_dec_cache_CS.graph = self;
      g_vtx_dec_cache_CS.typecode = vertex_type;
      g_vtx_dec_cache_CS.CSTR__tname = CSTR__tname;
      return true;
    }
  }
  return false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_vtx__has_mapping_vtx_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vertex_type ) {
  bool exists;
  GRAPH_LOCK( self ) {
    exists = _vxenum_vtx__has_mapping_vtx_CS( self, vertex_type );
  } GRAPH_RELEASE;
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__entries_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  framehash_t *decoder = self->vxtype_decoder;

  // TODO: Possible to allow this operation when graph is readonly?
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &decoder->_dynamic, CSTR__output ) {
    // Add all registered types
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
DLL_HIDDEN int _vxenum_vtx__entries_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_vtx__entries_CS( self, CSTR__output );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__codes_CS( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  framehash_t *encoder = self->vxtype_encoder;

  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &encoder->_dynamic, output ) {
    if( CALLABLE( encoder )->GetValues( encoder ) < 0 ) {
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
DLL_HIDDEN int _vxenum_vtx__codes_OPEN( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_vtx__codes_CS( self, output );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_vtx__count_CS( vgx_Graph_t *self ) {
  framehash_t *decoder = self->vxtype_decoder;
  return CALLABLE( decoder )->Items( decoder );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_vtx__count_OPEN( vgx_Graph_t *self ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxenum_vtx__count_CS( self );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_vtx__operation_sync_CS( vgx_Graph_t *self ) {
  int64_t n_vtx = 0;
  CtptrList_t *CSTR__strings;
  XTRY {
    if( (CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    if( _vxenum_vtx__entries_CS( self, CSTR__strings ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    n_vtx = CALLABLE( CSTR__strings )->Length( CSTR__strings );
    for( int64_t i=0; i<n_vtx; i++ ) {
      tptr_t data;
      if( CALLABLE( CSTR__strings )->Get( CSTR__strings, i, &data ) == 1 ) {
        const CString_t *CSTR__vtx = (const CString_t*)TPTR_GET_PTR56( &data );
        if( CSTR__vtx ) {
          vgx_vertex_type_t typecode = _vxenum_vtx__encode_CS( self, CSTR__vtx, NULL, false );
          iOperation.Enumerator_CS.AddVertexType( self, g_vtx_enc_cache_CS.typehash, CSTR__vtx, typecode );
        }
      }
    }
    COMMIT_GRAPH_OPERATION_CS( self );
  }
  XCATCH( errcode ) {
    n_vtx = -1;
  }
  XFINALLY {
    if( CSTR__strings ) {
      COMLIB_OBJECT_DESTROY( CSTR__strings );
    }
  }
  return n_vtx;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_vtx__operation_sync_OPEN( vgx_Graph_t *self ) {
  int64_t n_vtx = 0;
  GRAPH_LOCK( self ) {
    n_vtx = _vxenum_vtx__operation_sync_CS( self );
  } GRAPH_RELEASE;
  return n_vtx;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__create_enumerator( vgx_Graph_t *self ) {
  int ret = 0;

  CString_t *CSTR__encoder_prefix = CStringNew( VGX_PATHDEF_CODEC_VXTYPE_ENCODER_PREFIX );
  CString_t *CSTR__decoder_prefix = CStringNew( VGX_PATHDEF_CODEC_VXTYPE_DECODER_PREFIX );
  CString_t *CSTR__encoder_name = NULL;
  CString_t *CSTR__decoder_name = NULL;
  CString_t *CSTR__codec_fullpath = NULL;

  XTRY {
    if( CSTR__encoder_prefix == NULL || CSTR__decoder_prefix == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x481 );
    }

    if( (CSTR__encoder_name = iString.Utility.NewGraphMapName( self, CSTR__encoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x482 );
    }
    
    if( (CSTR__decoder_name = iString.Utility.NewGraphMapName( self, CSTR__decoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x483 );
    }

    if( (CSTR__codec_fullpath = iString.NewFormat( NULL, "%s/" VGX_PATHDEF_INSTANCE_GRAPH "/" VGX_PATHDEF_CODEC, CALLABLE(self)->FullPath(self) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x484 );
    }

    int max_size = (1 << (sizeof( vgx_vertex_type_t ) * 8)) - 1;
    vgx_mapping_spec_t encoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;
    vgx_mapping_spec_t decoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;

    // Make sure we don't accidentaly make new ones if they already exist
    if( self->vxtype_encoder != NULL || self->vxtype_decoder != NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x485 );
    }

    // Create the encoder <hash> -> <int>
    self->vxtype_encoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__encoder_name, max_size, MAPPING_DEFAULT_ORDER, encoder_spec, CLASS_NONE );

    // Create the decoder <int> -> <CString_t*>
    PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( self->property_allocator_context ) {
      self->vxtype_decoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__decoder_name, max_size, MAPPING_DEFAULT_ORDER, decoder_spec, CLASS_NONE );
    } POP_STRING_ALLOCATOR_CONTEXT;

    // Check
    if( self->vxtype_encoder == NULL || self->vxtype_decoder == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x486 );
    }

    // Reset the cache
    _vxenum_vtx__clear_cache();



  }
  XCATCH( errcode ) {
    _vxenum_vtx__destroy_enumerator( self );
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__codec_fullpath );
    iString.Discard( &CSTR__decoder_name );
    iString.Discard( &CSTR__encoder_name );
    CStringDelete( CSTR__decoder_prefix );
    CStringDelete( CSTR__encoder_prefix );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxenum_vtx__destroy_enumerator( vgx_Graph_t *self ) {
  iMapping.DeleteMap( &self->vxtype_encoder );
  iMapping.DeleteMap( &self->vxtype_decoder );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_vtx__initialize( void ) {
  // Initialize constants
  if( !g_init ) {
    CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING       = iString.New( NULL, __VERTEX_TYPE_ENUMERATION_NONE_STRING );
    CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING     = iString.New( NULL, __VERTEX_TYPE_ENUMERATION_VERTEX_STRING );
    CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING     = iString.New( NULL, __VERTEX_TYPE_ENUMERATION_SYSTEM_STRING );
    CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING    = iString.New( NULL, __VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING );
    CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING = iString.New( NULL, __VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING );

    if( !CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING ||
        !CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING ||
        !CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING ||
        !CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING ||
        !CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING
        )
    {
      return -1;
    }

    // Set the typehash for "__vertex__"
    g_vertex_typehash = CStringHash64( CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING );
    // Set the typehash for "lock_"
    g_lock_typehash = CStringHash64( CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING );
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
DLL_HIDDEN int _vxenum_vtx__destroy( void ) {
  if( g_init ) {
    iString.Discard( (CString_t**)&CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING );
    iString.Discard( (CString_t**)&CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING );
    iString.Discard( (CString_t**)&CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING );
    iString.Discard( (CString_t**)&CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING );
    iString.Discard( (CString_t**)&CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING );
    return 1;
  }
  return 0;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum_vtx.h"
  
test_descriptor_t _vgx_vxenum_vtx_tests[] = {
  { "VGX Graph Vertex Type Enumeration Tests", __utest_vxenum_vtx },
  {NULL}
};
#endif
