/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum_rel.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static const char *__VGX_PREDICATOR_REL_NONE_STRING       = "__null__";
static const char *__VGX_PREDICATOR_REL_RELATED_STRING    = "__related__";
static const char *__VGX_PREDICATOR_REL_SIMILAR_STRING    = "__similar__";
static const char *__VGX_PREDICATOR_REL_SYNTHETIC_STRING  = "__synthetic__";
static const char *__VGX_PREDICATOR_REL_SYSTEM_STRING     = "__INTERNAL__";
static const char *__VGX_PREDICATOR_REL_UNKNOWN_STRING    = "__UNKNOWN__";

static QWORD __RELATED_HASH = 0;
static QWORD __SIMILAR_HASH = 0;
static QWORD __SYNTHETIC_HASH = 0;


// These will be created when the first enumerator instance is created and never destroyed
static int g_init = 0;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_NONE_STRING      = NULL;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_RELATED_STRING   = NULL;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING   = NULL;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING = NULL;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING    = NULL;
DLL_HIDDEN const CString_t *CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING   = NULL;


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_reltype_encoder_cache_entry {
  vgx_Graph_t             *graph;   // context
  QWORD                   relhash;  // KEY
  vgx_predicator_rel_enum relcode;  // VALUE
} __reltype_encoder_cache_entry;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_reltype_decoder_cache_entry {
  vgx_Graph_t             *graph;         // context
  vgx_predicator_rel_enum relcode;        // KEY
  const CString_t         *CSTR__reltype; // VALUE
} __reltype_decoder_cache_entry;



static __THREAD __reltype_encoder_cache_entry gt_rel_enc_cache = {0};
static __THREAD __reltype_decoder_cache_entry gt_rel_dec_cache = {0};


static vgx_predicator_rel_enum  __invalidate_encoder_cache_entry( __reltype_encoder_cache_entry *entry, vgx_predicator_rel_enum code );
static vgx_predicator_rel_enum  __invalidate_decoder_cache_entry( __reltype_decoder_cache_entry *entry, vgx_predicator_rel_enum code );

static vgx_predicator_rel_enum  __verify_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
static vgx_predicator_rel_enum  __add_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode, CString_t **CSTR__mapped_instance );
static vgx_predicator_rel_enum  __set_relationship_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode, CString_t **CSTR__mapped_instance );
static vgx_predicator_rel_enum  __encode_user_relationship_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance );
static int                      __remove_relationship_mapping_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel );








/**************************************************************************//**
 * __invalidate_encoder_cache_entry
 *
 ******************************************************************************
 */
static vgx_predicator_rel_enum __invalidate_encoder_cache_entry( __reltype_encoder_cache_entry *entry, vgx_predicator_rel_enum code ) {
  entry->graph = NULL;
  entry->relhash = 0;
  entry->relcode = code;
  return code;
}



/**************************************************************************//**
 * __invalidate_decoder_cache_entry
 *
 ******************************************************************************
 */
static vgx_predicator_rel_enum __invalidate_decoder_cache_entry( __reltype_decoder_cache_entry *entry, vgx_predicator_rel_enum code ) {
  entry->graph = NULL;
  entry->relcode = code;
  entry->CSTR__reltype = NULL;
  return code;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_predicator_rel_enum __verify_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode ) {
  framehash_t *decoder = self->rel_decoder;
  framehash_t *encoder = self->rel_encoder;

  // Check if mapping already exists
  QWORD objkey = relcode;
  const CString_t *CSTR__existing_entry = NULL;
  int64_t mapped_relcode = 0;

  // Does mapping relhash->relcode exist? 
  if( CALLABLE( encoder )->GetInt56( encoder, CELL_KEY_TYPE_HASH64, relhash, &mapped_relcode ) == CELL_VALUE_TYPE_INTEGER ) {
    // Same relcode ?
    if( mapped_relcode == relcode ) {
      // Retrive mapped string
      if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__existing_entry ) == CELL_VALUE_TYPE_NULL ) {
        return VGX_PREDICATOR_REL_ERROR;
      }
      // Verify string match
      else if( !CALLABLE( CSTR__relationship )->Equals( CSTR__relationship, CSTR__existing_entry ) ) {
        return VGX_PREDICATOR_REL_COLLISION;
      }
      // Existing mapping verified
      else {
        return relcode;
      }
    }
    // Relcode mismatch
    else {
      return VGX_PREDICATOR_REL_COLLISION;
    }
  }
  // No mapping
  else if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__existing_entry ) == CELL_VALUE_TYPE_NULL ) {
    return VGX_PREDICATOR_REL_NO_MAPPING;
  }
  // relhash miss but string decode hit, mismatch implied
  else {
    return VGX_PREDICATOR_REL_COLLISION;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_predicator_rel_enum  __add_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode, CString_t **CSTR__mapped_instance ) {
  framehash_t *decoder = self->rel_decoder;
  framehash_t *encoder = self->rel_encoder;

  QWORD objkey = relcode;
  CString_t *CSTR__instance = NULL;

  XTRY {
    // Mapped instance must use property allocator
    if( (CSTR__instance = OwnOrCloneCString( CSTR__relationship, self->property_allocator_context )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Insert reverse mapping: relcode -> CString_t*
    if( CALLABLE( decoder )->SetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, COMLIB_OBJECT( CSTR__instance ) ) != CELL_VALUE_TYPE_OBJECT64 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
        
    // Insert forward mapping: relhash -> relcode
    if( CALLABLE( encoder )->SetInt56( encoder, CELL_KEY_TYPE_HASH64, relhash, relcode ) != CELL_VALUE_TYPE_INTEGER ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Capture and commit (user defined types only)
    if( __relationship_enumeration_in_user_range( relcode ) ) { 
      iOperation.Enumerator_CS.AddRelationship( self, relhash, CSTR__instance, relcode );
      COMMIT_GRAPH_OPERATION_CS( self );
    }

  }
  XCATCH( errcode ) {
    CALLABLE( encoder )->DelKey64( encoder, CELL_KEY_TYPE_HASH64, relhash );
    CALLABLE( decoder )->DelObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey );
    icstringobject.Delete( CSTR__instance );
    CSTR__instance = NULL;
    relcode = VGX_PREDICATOR_REL_ERROR;
  }
  XFINALLY {
    if( CSTR__mapped_instance ) {
      *CSTR__mapped_instance = CSTR__instance;
    }
  }

  return relcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_predicator_rel_enum __set_relationship_mapping_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode, CString_t **CSTR__mapped_instance ) {
  // Make sure we have a valid mapping
  if( !__relationship_enumeration_in_user_range( relcode ) ) { 
    return VGX_PREDICATOR_REL_INVALID;
  }

  vgx_predicator_rel_enum ret;
  if( (ret = __verify_mapping_CS( self, relhash, CSTR__relationship, relcode )) == VGX_PREDICATOR_REL_NO_MAPPING ) {
    return __add_mapping_CS( self, relhash, CSTR__relationship, relcode, CSTR__mapped_instance );
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
static vgx_predicator_rel_enum __encode_user_relationship_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance ) {

  // Pick a relationship code for this new relationship. Use the relhash as basis. Keep trying until we find
  // a relationship code that is not already taken. NOTE: We do this to avoid the need for a global counter
  // that would return the "next" code. In most cases we'll quickly find the available code, except for when
  // the system is very full (relationship count close to the maximum). But even then, this operation is a one-time
  // event that only occurs when a new type of relationship is introduced.
  vgx_predicator_rel_enum range = __VGX_PREDICATOR_REL_USER_RANGE_SIZE;
  vgx_predicator_rel_enum offset = __VGX_PREDICATOR_REL_START_USER_RANGE;
  int n = 0;

  do {
    // Derive a candidate relationship code from the hash code
    vgx_predicator_rel_enum relcode = (vgx_predicator_rel_enum)((relhash + n) % range + offset);
    // Lookup
    vgx_predicator_rel_enum existing_relcode = __verify_mapping_CS( self, relhash, CSTR__relationship, relcode );
    // No existing mapping - Accept the candidate
    if( existing_relcode == VGX_PREDICATOR_REL_NO_MAPPING ) {
      // No forward collision, add mapping
      if( CALLABLE( self->rel_encoder )->HasKey64( self->rel_encoder, CELL_KEY_TYPE_HASH64, relhash ) == CELL_VALUE_TYPE_NULL ) {
        // Add mapping
        if( __add_mapping_CS( self, relhash, CSTR__relationship, relcode, CSTR__mapped_instance ) == relcode ) {
          // Return encoded relationship
          return relcode;
        }
        // Failed
        else {
          return VGX_PREDICATOR_REL_ERROR;
        }
      }
      // Forward collision
      else {
        return VGX_PREDICATOR_REL_NO_MAPPING;
      }
    }
    // Mapping already exists - just return it
    else if( __relationship_enumeration_in_user_range( existing_relcode ) ) { 
      return existing_relcode;
    }
    
    // Collision on relcode candidate, try again as long as we're in user range
  } while( ++n < range );

  // Map full
  return VGX_PREDICATOR_REL_NO_MAPPING;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __remove_relationship_mapping_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel ) {

  framehash_t *decoder = self->rel_decoder;
  framehash_t *encoder = self->rel_encoder;

  QWORD objkey = rel;
  CString_t *CSTR__relationship = NULL;

  // Invalidate all caches
  _vxenum_rel__clear_thread_cache();

  int rev_deleted = 0;
  int fwd_deleted = 0;
  // Retrieve the string instance by reverse lookup
  if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__relationship ) == CELL_VALUE_TYPE_OBJECT64 ) {
    // Compute the relationship hash
    QWORD relhash = CStringHash64( CSTR__relationship );
    // Remove the reverse mapping
    if( (rev_deleted = CALLABLE( decoder )->DelObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey ) == CELL_VALUE_TYPE_OBJECT64) == 1 ) {
      // Capture but don't commit. (Lazy remove takes effect on next graph commit.)
      if( iOperation.Emitter_CS.IsReady( self ) ) {
        iOperation.Enumerator_CS.RemoveRelationship( self, relhash, rel );
      }

      // Decoder gives up ownership
      icstringobject.DecrefNolock( CSTR__relationship ); // <= gone from the map, one less owner

      // Remove the forward mapping
      fwd_deleted = CALLABLE( encoder )->DelKey64( encoder, CELL_KEY_TYPE_HASH64, relhash ) == CELL_VALUE_TYPE_INTEGER;

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
DLL_HIDDEN void _vxenum_rel__clear_thread_cache( void ) {
  __invalidate_encoder_cache_entry( &gt_rel_enc_cache, VGX_PREDICATOR_REL_NONE );
  __invalidate_decoder_cache_entry( &gt_rel_dec_cache, VGX_PREDICATOR_REL_NONE );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_predicator_rel_enum _vxenum_rel__set_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode ) {
  // Not a valid storable key
  if( !_vxenum__is_valid_storable_key( CStringValue( CSTR__relationship ) ) ) {
    return VGX_PREDICATOR_REL_INVALID;
  }

  // Readonly graph
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return VGX_PREDICATOR_REL_LOCKED;
  }

  // Assign
  vgx_predicator_rel_enum ret;
  CString_t *CSTR__mapped_instance = NULL;
  if( (ret = __set_relationship_mapping_CS( self, relhash, CSTR__relationship, relcode, &CSTR__mapped_instance )) == relcode ) {
    // Encoder cache
    gt_rel_enc_cache.graph = self;
    gt_rel_enc_cache.relhash = relhash;
    gt_rel_enc_cache.relcode = relcode;
    // Decoder cache
    gt_rel_dec_cache.graph = self;
    gt_rel_dec_cache.relcode = relcode;
    gt_rel_dec_cache.CSTR__reltype = CSTR__mapped_instance;
    return relcode;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_predicator_rel_enum _vxenum_rel__set_OPEN( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode ) {
  vgx_predicator_rel_enum rel;
  GRAPH_LOCK( self ) {
    rel = _vxenum_rel__set_CS( self, relhash, CSTR__relationship, relcode );
  } GRAPH_RELEASE;
  return rel;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_predicator_rel_enum _vxenum_rel__encode_CS( vgx_Graph_t *self, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance, bool create_nonexist ) {
  QWORD relhash;

  // Sanity check
  if( CSTR__relationship == NULL ) {
    return VGX_PREDICATOR_REL_ERROR;
  }
  // More checks before computing relhash from string
  else {
    // Compute hash from string
    relhash = CStringHash64( CSTR__relationship );

    // Special case: the default relationship
    if( relhash == __RELATED_HASH ) {
      return VGX_PREDICATOR_REL_RELATED;
    }
    else if( relhash == __SYNTHETIC_HASH ) {
      return VGX_PREDICATOR_REL_SYNTHETIC;
    }

    // Lookup and create new if no encoding exists
    if( create_nonexist ) {
      // Not a valid storable key
      if( !_vxenum__is_valid_storable_key( CStringValue( CSTR__relationship ) ) ) {
        return VGX_PREDICATOR_REL_INVALID;
      }
    }
    // Lookup only
    else {
      // Wildcard select
      const char *name = CStringValue( CSTR__relationship );
      if( *name == '*' && *(++name) == '\0' ) {
        return VGX_PREDICATOR_REL_WILDCARD;
      }
      // Not a valid select key
      else if( !_vxenum__is_valid_select_key( CStringValue( CSTR__relationship ) ) ) {
        return VGX_PREDICATOR_REL_INVALID;
      }
    }
  }

  // Cache hit: Return cached version immediately
  if( gt_rel_enc_cache.relhash == relhash && gt_rel_enc_cache.graph == self ) {
    return gt_rel_enc_cache.relcode;
  }

  // Cache miss: Do work
  // Update the cache key
  gt_rel_enc_cache.relhash = relhash;
  gt_rel_enc_cache.graph = self;

  // Look up the encoded relationship, create if requested.
  int64_t relcode_entry;
  framehash_t *encoder = self->rel_encoder;
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, gt_rel_enc_cache.relhash, &relcode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    // Update the cache value
    gt_rel_enc_cache.relcode = (vgx_predicator_rel_enum)relcode_entry;
    return gt_rel_enc_cache.relcode;
  }
  // Encoding does not exist
  else if( !create_nonexist ) {
    return __invalidate_encoder_cache_entry( &gt_rel_enc_cache, VGX_PREDICATOR_REL_NONEXIST );
  }
  // No entry for this relationship - create new entry unless readonly
  else if( _vgx_is_writable_CS( &self->readonly ) ) {
    // ADD ENCODING TO MAP - Update the cache value
    if( __relationship_enumeration_valid( (gt_rel_enc_cache.relcode = __encode_user_relationship_CS( self, gt_rel_enc_cache.relhash, CSTR__relationship, CSTR__mapped_instance )) ) ) {
      return gt_rel_enc_cache.relcode;
    }
    // ERROR
    else {
      return __invalidate_encoder_cache_entry( &gt_rel_enc_cache, gt_rel_enc_cache.relcode );
    }
  }
  // Readonly graph (invalidate cache)
  else {
    return __invalidate_encoder_cache_entry( &gt_rel_enc_cache, VGX_PREDICATOR_REL_LOCKED );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN uint64_t _vxenum_rel__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__relationship ) {

  // Sanity check
  if( CSTR__relationship == NULL ) {
    return VGX_PREDICATOR_REL_ERROR;
  }

  // Compute hash from string
  QWORD relhash = CStringHash64( CSTR__relationship );

  // Special case: the default relationship
  if( relhash == __RELATED_HASH ) {
    return VGX_PREDICATOR_REL_RELATED;
  }
  
  // Special case: the synthetic relationship
  if( relhash == __SYNTHETIC_HASH ) {
    return VGX_PREDICATOR_REL_SYNTHETIC;
  }

  // Wildcard select
  const char *name = CStringValue( CSTR__relationship );
  if( *name == '*' && *(++name) == '\0' ) {
    return VGX_PREDICATOR_REL_WILDCARD;
  }

  // Not a valid select key
  if( !_vxenum__is_valid_select_key( CStringValue( CSTR__relationship ) ) ) {
    return VGX_PREDICATOR_REL_INVALID;
  }

  // Cache hit: Return cached version immediately
  if( gt_rel_enc_cache.relhash == relhash && gt_rel_enc_cache.graph == self ) {
    return gt_rel_enc_cache.relcode;
  }

  // Cache miss: Do work
  // Update the cache key
  gt_rel_enc_cache.relhash = relhash;
  gt_rel_enc_cache.graph = self;

  // Look up the encoded relationship, create if requested.
  int64_t relcode_entry;
  framehash_t *encoder = self->rel_encoder;
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, gt_rel_enc_cache.relhash, &relcode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    // Update the cache value
    gt_rel_enc_cache.relcode = (vgx_predicator_rel_enum)relcode_entry;
    return gt_rel_enc_cache.relcode;
  }

  // Encoding does not exist
  return __invalidate_encoder_cache_entry( &gt_rel_enc_cache, VGX_PREDICATOR_REL_NONEXIST );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_predicator_rel_enum _vxenum_rel__encode_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance, bool create_nonexist ) {
  vgx_predicator_rel_enum rel;
  GRAPH_LOCK( self ) {
    rel = _vxenum_rel__encode_CS( self, CSTR__relationship, CSTR__mapped_instance, create_nonexist );
  } GRAPH_RELEASE;
  return rel;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN uint64_t _vxenum_rel__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationship ) {
  uint64_t e;
  GRAPH_LOCK( self ) {
    e = _vxenum_rel__get_enum_CS( self, CSTR__relationship );
  } GRAPH_RELEASE;
  return e;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_rel__decode_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel ) {
  // Cache hit
  if( rel == gt_rel_dec_cache.relcode && self == gt_rel_dec_cache.graph ) {
    return gt_rel_dec_cache.CSTR__reltype;
  }

  // Cache miss
  switch( rel ) {
  case VGX_PREDICATOR_REL_RELATED:
    return CSTR__VGX_PREDICATOR_REL_RELATED_STRING;
  case VGX_PREDICATOR_REL_SIMILAR:
    return CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING;
  case VGX_PREDICATOR_REL_SYNTHETIC:
    return CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING;
  default:
    if( __relationship_enumeration_valid( rel ) ) {
      // Update cache key and context
      gt_rel_dec_cache.relcode = rel;
      gt_rel_dec_cache.graph = self;
      // Perform lookup
      framehash_t *decoder = self->rel_decoder;
      QWORD objkey = gt_rel_dec_cache.relcode;
      if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&gt_rel_dec_cache.CSTR__reltype ) != CELL_VALUE_TYPE_OBJECT64 ) {
        gt_rel_dec_cache.CSTR__reltype = CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING;
      }
      return gt_rel_dec_cache.CSTR__reltype;
    }
    else {
      return CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_rel__decode_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum rel ) {
  const CString_t *CSTR__relationship;
  GRAPH_LOCK( self ) {
    CSTR__relationship = _vxenum_rel__decode_CS( self, rel );
  } GRAPH_RELEASE;
  return CSTR__relationship;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_rel__has_mapping_CS( vgx_Graph_t *self, const CString_t *CSTR__relationship ) {
  if( CSTR__relationship ) {
    framehash_t *encoder = self->rel_encoder;
    QWORD relhash = CStringHash64( CSTR__relationship );
    if( CALLABLE(encoder)->HasKey64( encoder, CELL_KEY_TYPE_HASH64, relhash ) == CELL_VALUE_TYPE_INTEGER ) {
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
DLL_HIDDEN bool _vxenum_rel__has_mapping_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationship ) {
  int exists;
  GRAPH_LOCK( self ) {
    exists = _vxenum_rel__has_mapping_CS( self, CSTR__relationship );
  } GRAPH_RELEASE;
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_rel__has_mapping_chars_CS( vgx_Graph_t *self, const char *relationship ) {
  if( relationship ) {
    framehash_t *encoder = self->rel_encoder;
    QWORD relhash = CharsHash64( (const unsigned char*)relationship );
    if( CALLABLE(encoder)->HasKey64( encoder, CELL_KEY_TYPE_HASH64, relhash ) == CELL_VALUE_TYPE_INTEGER ) {
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
DLL_HIDDEN bool _vxenum_rel__has_mapping_chars_OPEN( vgx_Graph_t *self, const char *relationship ) {
  int exists;
  GRAPH_LOCK( self ) {
    exists = _vxenum_rel__has_mapping_chars_CS( self, relationship );
  } GRAPH_RELEASE;
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_rel__has_mapping_rel_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel ) {
  switch( rel ) {
  case VGX_PREDICATOR_REL_RELATED:
    /* FALLTHRU */
  case VGX_PREDICATOR_REL_SIMILAR:
    return true;
  default:
    if( __relationship_enumeration_valid( rel ) ) {
      framehash_t *decoder = self->rel_decoder;
      QWORD objkey = rel;
      const CString_t *CSTR__reltype = NULL;
      if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__reltype ) == CELL_VALUE_TYPE_OBJECT64 ) {
        gt_rel_dec_cache.graph = self;
        gt_rel_dec_cache.relcode = rel;
        gt_rel_dec_cache.CSTR__reltype = CSTR__reltype;
        return true;
      }
    }
  }
  return false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_rel__has_mapping_rel_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum rel ) {
  bool exists;
  GRAPH_LOCK( self ) {
    exists = _vxenum_rel__has_mapping_rel_CS( self, rel );
  } GRAPH_RELEASE;
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__entries_CS( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  framehash_t *decoder = self->rel_decoder;

  // TODO: Possible to allow this operation when graph is readonly?
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &decoder->_dynamic, output ) {
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
DLL_HIDDEN int _vxenum_rel__entries_OPEN( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_rel__entries_CS( self, output );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__codes_CS( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  framehash_t *encoder = self->rel_encoder;

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
DLL_HIDDEN int _vxenum_rel__codes_OPEN( vgx_Graph_t *self, CtptrList_t *output ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxenum_rel__codes_CS( self, output );
  } GRAPH_RELEASE;
  return ret;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_rel__count_CS( vgx_Graph_t *self ) {
  framehash_t *decoder = self->rel_decoder;
  return CALLABLE( decoder )->Items( decoder );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_rel__count_OPEN( vgx_Graph_t *self ) {
  int64_t n = 0;
  GRAPH_LOCK( self ) {
    n = _vxenum_rel__count_CS( self );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_rel__operation_sync_CS( vgx_Graph_t *self ) {
  int64_t n_rel = 0;
  CtptrList_t *CSTR__strings;
  XTRY {
    if( (CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    if( _vxenum_rel__entries_CS( self, CSTR__strings ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    n_rel = CALLABLE( CSTR__strings )->Length( CSTR__strings );
    for( int64_t i=0; i<n_rel; i++ ) {
      tptr_t data;
      if( CALLABLE( CSTR__strings )->Get( CSTR__strings, i, &data ) == 1 ) {
        const CString_t *CSTR__rel = (const CString_t*)TPTR_GET_PTR56( &data );
        if( CSTR__rel ) {
          vgx_predicator_rel_enum relcode = _vxenum_rel__encode_CS( self, CSTR__rel, NULL, false );
          iOperation.Enumerator_CS.AddRelationship( self, gt_rel_enc_cache.relhash, CSTR__rel, relcode );
        }
      }
    }
    COMMIT_GRAPH_OPERATION_CS( self );
  }
  XCATCH( errcode ) {
    n_rel = -1;
  }
  XFINALLY {
    if( CSTR__strings ) {
      COMLIB_OBJECT_DESTROY( CSTR__strings );
    }
  }
  return n_rel;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_rel__operation_sync_OPEN( vgx_Graph_t *self ) {
  int64_t n_rel = 0;
  GRAPH_LOCK( self ) {
    n_rel = _vxenum_rel__operation_sync_CS( self );
  } GRAPH_RELEASE;
  return n_rel;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__remove_mapping_CS( vgx_Graph_t *self, vgx_predicator_rel_enum enc ) {
  // Not allowed to remove from the map if graph is readonly
  if( _vgx_is_readonly_CS( &self->readonly ) ) {
    return -1;
  }

  return __remove_relationship_mapping_CS( self, enc );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__remove_mapping_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum enc ) {
  int ret;
  GRAPH_LOCK( self ) {
    ret = _vxenum_rel__remove_mapping_CS( self, enc );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__create_enumerator( vgx_Graph_t *self ) {
  int ret = 0;

  CString_t *CSTR__encoder_prefix = CStringNew( VGX_PATHDEF_CODEC_RELATIONSHIP_ENCODER_PREFIX );
  CString_t *CSTR__decoder_prefix = CStringNew( VGX_PATHDEF_CODEC_RELATIONSHIP_DECODER_PREFIX );
  CString_t *CSTR__encoder_name = NULL;
  CString_t *CSTR__decoder_name = NULL;
  CString_t *CSTR__codec_fullpath = NULL;

  XTRY {
    if( CSTR__encoder_prefix == NULL || CSTR__decoder_prefix == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x461 );
    }

    if( (CSTR__encoder_name = iString.Utility.NewGraphMapName( self, CSTR__encoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x462 );
    }
    
    if( (CSTR__decoder_name = iString.Utility.NewGraphMapName( self, CSTR__decoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x463 );
    }

    if( (CSTR__codec_fullpath = iString.NewFormat( NULL, "%s/" VGX_PATHDEF_INSTANCE_GRAPH "/" VGX_PATHDEF_CODEC, CALLABLE(self)->FullPath(self) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x464 );
    }

    int max_size = VGX_PREDICATOR_REL_MAX;
    vgx_mapping_spec_t encoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;
    vgx_mapping_spec_t decoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;

    // Make sure we don't accidentaly make new ones if they already exist
    if( self->rel_encoder != NULL || self->rel_decoder != NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x465 );
    }

    // Create the encoder <hash> -> <int>
    self->rel_encoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__encoder_name, max_size, MAPPING_DEFAULT_ORDER, encoder_spec, CLASS_NONE );

    // Create the decoder <int> -> <CString_t*>
    PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( self->property_allocator_context ) {
      self->rel_decoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__decoder_name, max_size, MAPPING_DEFAULT_ORDER, decoder_spec, CLASS_NONE );
    } POP_STRING_ALLOCATOR_CONTEXT;

    // Check
    if( self->rel_encoder == NULL || self->rel_decoder == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x466 );
    }

    // Reset the thread-local cache
    _vxenum_rel__clear_thread_cache();



  }
  XCATCH( errcode ) {
    _vxenum_rel__destroy_enumerator( self );
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
DLL_HIDDEN void _vxenum_rel__destroy_enumerator( vgx_Graph_t *self ) {
  iMapping.DeleteMap( &self->rel_encoder );  
  iMapping.DeleteMap( &self->rel_decoder );  
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_rel__initialize( void ) {
  // Initialize constants
  if( !g_init ) {
    CSTR__VGX_PREDICATOR_REL_NONE_STRING      = iString.New( NULL, __VGX_PREDICATOR_REL_NONE_STRING );
    CSTR__VGX_PREDICATOR_REL_RELATED_STRING   = iString.New( NULL, __VGX_PREDICATOR_REL_RELATED_STRING );
    CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING   = iString.New( NULL, __VGX_PREDICATOR_REL_SIMILAR_STRING );
    CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING = iString.New( NULL, __VGX_PREDICATOR_REL_SYNTHETIC_STRING );
    CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING    = iString.New( NULL, __VGX_PREDICATOR_REL_SYSTEM_STRING );
    CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING   = iString.New( NULL, __VGX_PREDICATOR_REL_UNKNOWN_STRING );

    if( !CSTR__VGX_PREDICATOR_REL_NONE_STRING ||
        !CSTR__VGX_PREDICATOR_REL_RELATED_STRING ||
        !CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING ||
        !CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING ||
        !CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING ||
        !CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING
        )
      {
        return -1;
      }
    __RELATED_HASH = CStringHash64( CSTR__VGX_PREDICATOR_REL_RELATED_STRING );
    __SIMILAR_HASH = CStringHash64( CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING );
    __SYNTHETIC_HASH = CStringHash64( CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING );

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
DLL_HIDDEN int _vxenum_rel__destroy( void ) {
  if( g_init ) {
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_NONE_STRING );
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_RELATED_STRING );
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING );
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING );
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING );
    iString.Discard( (CString_t**)&CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING );
    return 1;
  }
  return 0;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum_rel.h"
  
test_descriptor_t _vgx_vxenum_rel_tests[] = {
  { "VGX Graph Relationship Enumeration Tests", __utest_vxenum_rel },
  {NULL}
};
#endif
