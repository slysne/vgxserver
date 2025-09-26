/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum_dim.c
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

static const char *__VECTOR_DIMENSION_ENUMERATION_NONE_STRING   = "__dim#null__";
static const char *__VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING = "__dim#locked__";
static const char *__VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING = "__dim#noenum__";
static const char *__VECTOR_DIMENSION_ENUMERATION_ERROR_STRING  = "__dim#error__";

// These will be created when the first enumerator instance is created and never destroyed
static int g_init = 0;
DLL_HIDDEN const CString_t *CSTR__VECTOR_DIMENSION_ENUMERATION_NONE_STRING  = NULL;
DLL_HIDDEN const CString_t *CSTR__VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING = NULL;
DLL_HIDDEN const CString_t *CSTR__VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING = NULL;
DLL_HIDDEN const CString_t *CSTR__VECTOR_DIMENSION_ENUMERATION_ERROR_STRING = NULL;

static CString_t * __new_dimension_string( vgx_Similarity_t *self, const char *dimstr, int sz );
static feature_vector_dimension_t __index_dimension_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode);
static feature_vector_dimension_t __assign_dimension_code_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension );
static int64_t __unassign_dimension_code_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim );


#ifndef _MAX_DIMENSION_ENUMERATOR_ENTRIES
#define _MAX_DIMENSION_ENUMERATOR_ENTRIES ((FEATURE_VECTOR_DIMENSION_MAX - FEATURE_VECTOR_DIMENSION_MIN)/3)
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __new_dimension_string( vgx_Similarity_t *self, const char *dimstr, int sz ) {
  CString_t *CSTR__dimension = NULL;
  CString_constructor_args_t args = {
    .string       = dimstr,
    .len          = sz,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = self->readonly ? self->parent->ephemeral_string_allocator_context : self->dimension_allocator_context
  };

  // String refcnt = 1
  CSTR__dimension = self->cstring_construct( NULL, &args );

  return CSTR__dimension;
}



/*******************************************************************//**
 *
 * Add CString instance to the dimension index (forward + reverse)
 * and own a new reference to the indexed string.
 ***********************************************************************
 */
static feature_vector_dimension_t __index_dimension_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode) {
  // Make sure we don't have a forward collision
  if( self->dim_encoder == NULL || self->dim_decoder == NULL || CALLABLE( self->dim_encoder )->HasKey64( self->dim_encoder, CELL_KEY_TYPE_HASH64, dimhash ) != CELL_VALUE_TYPE_NULL ) {
    dimcode = FEATURE_VECTOR_DIMENSION_NOENUM;
  }
  else if( self->readonly ) {
    dimcode = FEATURE_VECTOR_DIMENSION_LOCKED;
  }
  else {
    QWORD dimcode_key = dimcode;
    CString_t *CSTR__mapped_instance = NULL;
    XTRY {

      if( (CSTR__mapped_instance = OwnOrCloneCString( CSTR__dimension, self->dimension_allocator_context )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x401 );
      }

      // Insert reverse mapping
      // NOTE: The supplied string instance is stored as-is without cloning: THE MAP STEALS THE CSTRING INSTANCE
      if( CALLABLE( self->dim_decoder )->SetObj( self->dim_decoder, CELL_KEY_TYPE_PLAIN64, &dimcode_key, COMLIB_OBJECT(CSTR__mapped_instance) ) != CELL_VALUE_TYPE_OBJECT64 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x402 );
      }
          
      // Insert forward mapping
      if( CALLABLE( self->dim_encoder )->SetInt56( self->dim_encoder, CELL_KEY_TYPE_HASH64, dimhash, dimcode ) != CELL_VALUE_TYPE_INTEGER ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x403 );
      }

      // Capture and commit
      vgx_Graph_t *graph = self->parent;
      if( graph ) {
        iOperation.Enumerator_CS.AddDimension( self, dimhash, CSTR__mapped_instance, dimcode );
        COMMIT_GRAPH_OPERATION_CS( graph );
      }
    }
    XCATCH( errcode ) {
      CALLABLE( self->dim_encoder )->DelKey64( self->dim_encoder, CELL_KEY_TYPE_HASH64, dimhash );
      CALLABLE( self->dim_decoder )->DelObj( self->dim_decoder, CELL_KEY_TYPE_PLAIN64, &dimcode_key );
      dimcode = FEATURE_VECTOR_DIMENSION_ERROR;
    }
    XFINALLY {}
  }
  return dimcode;
}



/*******************************************************************//**
 *
 * NOTE: Dimension will be "stolen" on success! Caller must NOT deallocate it.
 *       (However, on failure caller still owns it and must deallocate.)
 *       If dimension string is NULL, a dimension code is computed but NOT
 *       stored for future lookup.
 ***********************************************************************
 */
static feature_vector_dimension_t __assign_dimension_code_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__new_dimension ) {
  static const feature_vector_dimension_t range = FEATURE_VECTOR_DIMENSION_MAX - FEATURE_VECTOR_DIMENSION_MIN + 1;
  static const feature_vector_dimension_t offset = FEATURE_VECTOR_DIMENSION_MIN;

  if( self->dim_decoder ) {
    framehash_t *decoder = self->dim_decoder;
    framehash_vtable_t *idecoder = CALLABLE( decoder );

    // Pick a dimension code for this new dimension. Use the dimhash as basis. Keep trying until we find
    // a dimension code that is not already taken. NOTE: We do this to avoid the need for a global counter
    // that would return the "next" code. In most cases we'll quickly find the available code, except for when
    // the system is very full (dimension count close to the maximum). But even then, this operation is a one-time
    // event that only occurs when a new dimension is introduced.

    // Perform mapping if max capacity not yet reached
    if( idecoder->Items( decoder ) < _MAX_DIMENSION_ENUMERATOR_ENTRIES ) {
      int n = 0;
      const CString_t *CSTR__indexed_dimension = NULL;
      // Compute candidate and perform lookup on candidate
      do {
        // Derive a candidate dimension code from the hash code
        QWORD dimcode_candidate = (dimhash + n) % range + offset;
        feature_vector_dimension_t dimcode = (feature_vector_dimension_t)dimcode_candidate;
        // Test the candidate and accept it if no mapping exists for it
        if( idecoder->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &dimcode_candidate, (comlib_object_t**)&CSTR__indexed_dimension ) == CELL_VALUE_TYPE_NULL ) {
          // Accept candidate
          // Index and return the assigned code for this dimension
          if( CSTR__new_dimension != NULL ) {
            return __index_dimension_CS( self, dimhash, CSTR__new_dimension, dimcode );
          }
          // Don't index, just return the assigned dimension code
          else {
            return dimcode;
          }
        }
        // Mapping already exists for the candidate dimcode, could be collision or could be the dimension was already indexed
        else {
          // Let's check if dimension was already indexed
          if( CSTR__new_dimension ) {
            // The dimension was already indexed (this means someone called this function when they shouldn't have)
            if( CALLABLE( CSTR__indexed_dimension )->Equals( CSTR__indexed_dimension, CSTR__new_dimension ) ) {
              return dimcode;
            }
            // Collision. Keep probing
            else {}
          }
          // Unable to determine if dimension was already indexed or if we had a collision.
          // Keep probing.
          // NOTE: This means if someone called this function when the dimension is already indexed (should not do that!)
          // we will be assigning a DIFFERENT code to the same dimension leading false negatives when comparing vectors.
          // We are not indexing this so it's not a destructive result, just least to false negatives for vector queries.
          else {}
        }
      } while( ++n < range );
    }
  }

  // Either we've reached max capacity for encoder or the encoding processed failed to find a suitable slot
  // to store the encoded value. Both cases mean we have exhausted the enumeration space.
  return FEATURE_VECTOR_DIMENSION_NOENUM;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __unassign_dimension_code_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  if( self->dim_decoder ) {
    framehash_t *decoder = self->dim_decoder;
    QWORD objkey = dim;
    CString_t *CSTR__dimension = NULL;
    // Retrieve the string instance by reverse lookup
    if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__dimension ) == CELL_VALUE_TYPE_OBJECT64 ) {
      // Compute the dimension hash
      int sz;
      QWORD dimhash = _vgx_enum__dimhash( CStringValue( CSTR__dimension ), &sz );
      // Remove the reverse mapping
      if( CALLABLE( decoder )->DelObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey ) == CELL_VALUE_TYPE_OBJECT64 == 1 ) {
        // Capture but don't commit. (Lazy remove, takes effect on next graph commit.)
        vgx_Graph_t *graph = self->parent;
        if( graph && iOperation.Emitter_CS.IsReady( graph ) ) {
          iOperation.Enumerator_CS.RemoveDimension( self, dimhash, dim );
          //COMMIT_GRAPH_OPERATION_CS( graph );
        }

        // Decoder gives up reference
        // We have to manually decref since framehash doesn't decref OBJECT64 instances!
        int64_t refcnt = icstringobject.DecrefNolock( CSTR__dimension ); // <= gone from the map, one less owner

        if( self->dim_encoder ) {
          framehash_t *encoder = self->dim_encoder;
          // Remove the forward mapping and return refcnt of the string instance
          if( CALLABLE( encoder )->DelKey64( encoder, CELL_KEY_TYPE_HASH64, dimhash ) == CELL_VALUE_TYPE_INTEGER ) {
            return refcnt;
          }
        }
      }
    }
  }

  return -1; // error

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__remain_CS( vgx_Similarity_t *self ) {
  if( self->dim_decoder ) {
    int64_t n_indexed = CALLABLE( self->dim_decoder )->Items( self->dim_decoder );
    return _MAX_DIMENSION_ENUMERATOR_ENTRIES - n_indexed;
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
DLL_HIDDEN int64_t _vxenum_dim__remain_OPEN( vgx_Similarity_t *self ) {
  int64_t n_remain;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      n_remain = _vxenum_dim__remain_CS( self );
    } GRAPH_RELEASE;
  }
  else {
    n_remain = _vxenum_dim__remain_CS( self );
  }
  return n_remain;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__set_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode ) {

  if( CStringLength( CSTR__dimension ) > MAX_FEATURE_VECTOR_TERM_LEN || self->dim_decoder == NULL ) {
    return FEATURE_VECTOR_DIMENSION_INVALID;
  }

  framehash_t *decoder = self->dim_decoder;

  QWORD objkey = dimcode;

  CString_t *CSTR__indexed_dimension = NULL;

  // Already indexed?
  if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__indexed_dimension ) == CELL_VALUE_TYPE_OBJECT64 ) {
    // OK, no action
    if( CALLABLE( CSTR__dimension )->Equals( CSTR__dimension, CSTR__indexed_dimension ) ) {
      return dimcode;
    }
    // Error (can't re-assign)
    else {
      return FEATURE_VECTOR_DIMENSION_COLLISION;
    }
  }
  // New dimension
  else {
    return __index_dimension_CS( self, dimhash, CSTR__dimension, dimcode );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__set_OPEN( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode ) {
  feature_vector_dimension_t dim;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      dim = _vxenum_dim__set_CS( self, dimhash, CSTR__dimension, dimcode );
    } GRAPH_RELEASE;
  }
  else {
    dim = _vxenum_dim__set_CS( self, dimhash, CSTR__dimension, dimcode );
  }
  return dim;
}



/*******************************************************************//**
 *
 *
 * String ownership notes:
 *    1) The reverse map owns one reference to the new string.
 *    2) Every vector, including the one requesting this dimension for the first time, will become an additional owner.
 *       Refcount will be 2 when this function succeeds in creating a new dimension.
 *    3) Refcount wlll be 3 or higher for each additional vector claiming ownership of this dimension.
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__encode_chars_CS( vgx_Similarity_t *self, const char *dimension, QWORD *ret_dimhash, bool create_nonexist ) {
  if( dimension == NULL || self->dim_encoder == NULL ) {
    return FEATURE_VECTOR_DIMENSION_ERROR;
  }

  CString_t *CSTR__dimension = NULL;

  // Compute the dimension hash
  int sz = 0;
  QWORD dimhash = _vgx_enum__dimhash( dimension, &sz );

  int64_t dimcode_entry;
  feature_vector_dimension_t dimcode = FEATURE_VECTOR_DIMENSION_NONE;
  framehash_t *encoder = self->dim_encoder;

  // Look up the encoded dimension
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, dimhash, &dimcode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    dimcode = (feature_vector_dimension_t)dimcode_entry;
  }

  // Are we creating or taking ownership of the string ?
  if( create_nonexist ) {

    // No entry for this dimension - create new entry
    if( dimcode == FEATURE_VECTOR_DIMENSION_NONE ) {
      // When the enumerator is readonly we will NOT add entries to the index, just assign the dimension code best effort!
      if( !self->readonly ) {
        CSTR__dimension = __new_dimension_string( self, dimension, sz );
      }
     
      // Assign a dimension code and add entry to dimension index
      // String refcnt = 2 (one for the enumerator map and one for the caller)
      if( (dimcode = __assign_dimension_code_CS( self, dimhash, CSTR__dimension )) < FEATURE_VECTOR_DIMENSION_MIN ) {
        // Indexing error
        iString.Discard( &CSTR__dimension );
      }
      else if( CSTR__dimension ) {
        if( icstringobject.RefcntNolock( CSTR__dimension ) != 2 ) {
          FATAL( 0xFFF, "new dimension refcnt must be exactly 2" );
        }
      }
    }
    // Dimension already exists, caller will own another reference
    else {
      _vxenum_dim__own_CS( self, dimcode );
    }
  }
  // Just lookup, no creation or ownership
  else if( dimcode == FEATURE_VECTOR_DIMENSION_NONE ) {
    // Computes hash but does NOT add to map
    dimcode = __assign_dimension_code_CS( self, dimhash, NULL );
  }

  if( ret_dimhash ) {
    *ret_dimhash = dimhash;
  }
  
  return dimcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__encode_chars_OPEN( vgx_Similarity_t *self, const char *dimension, QWORD *ret_dimhash, bool create_nonexist ) {
  feature_vector_dimension_t dim;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      dim = _vxenum_dim__encode_chars_CS( self, dimension, ret_dimhash, create_nonexist );
    } GRAPH_RELEASE;
  }
  else {
    dim = _vxenum_dim__encode_chars_CS( self, dimension, ret_dimhash, create_nonexist );
  }
  return dim;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__get_code_CS( vgx_Similarity_t *self, const char *dimension ) {
  if( dimension == NULL || self->dim_encoder == NULL ) {
    return FEATURE_VECTOR_DIMENSION_ERROR;
  }

  // Compute the dimension hash
  int sz = 0;
  QWORD dimhash = _vgx_enum__dimhash( dimension, &sz );

  int64_t dimcode_entry;
  feature_vector_dimension_t dimcode = FEATURE_VECTOR_DIMENSION_NONE;
  framehash_t *encoder = self->dim_encoder;

  // Look up the encoded dimension
  if( CALLABLE(encoder)->GetInt56( encoder, CELL_KEY_TYPE_HASH64, dimhash, &dimcode_entry ) == CELL_VALUE_TYPE_INTEGER ) {
    dimcode = (feature_vector_dimension_t)dimcode_entry;
  }
  else {
    // Computes hash but does NOT add to map
    dimcode = __assign_dimension_code_CS( self, dimhash, NULL );
  }

  return dimcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN feature_vector_dimension_t _vxenum_dim__get_code_OPEN( vgx_Similarity_t *self, const char *dimension ) {
  feature_vector_dimension_t dimcode;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      dimcode = _vxenum_dim__get_code_CS( self, dimension );
    } GRAPH_RELEASE;
  }
  else {
    dimcode = _vxenum_dim__get_code_CS( self, dimension );
  }
  return dimcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__own_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  int64_t refcnt;
  CString_t *CSTR__dimension = (CString_t*)_vxenum_dim__decode_CS( self, dim );
  if( CSTR__dimension ) {
    // Caller OWNS ANOTHER REFERENCE to this dimension
    refcnt = icstringobject.IncrefNolock( CSTR__dimension );
  }
  else {
    refcnt = -1;
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__own_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  int64_t refcnt;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      refcnt = _vxenum_dim__own_CS( self, dim );
    } GRAPH_RELEASE;
  }
  else {
    refcnt = _vxenum_dim__own_CS( self, dim );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__discard_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  int64_t refcnt = 0;
  CString_t *CSTR__dimension = (CString_t*)_vxenum_dim__decode_CS( self, dim );
  if( CSTR__dimension ) {
    // Once the refcount of the mapped string instance is about to go to zero, we must delete the mapping
    // When the refcount is 2 it means the only owners are the vector which is in the process of discarding
    // the dimension and the reverse dimension map. When the last vector discards the dimension it should
    // also be removed from the reverse map.
    if( (refcnt = icstringobject.RefcntNolock( CSTR__dimension )) <= 2 ) { // <= 2 owners means it will go to zero
      // We cannot be allowed to remove mapping if readonly
      if( self->readonly ) {
        // This should normally only occur if someone on the outside is performing WRITABLE operations,
        // which can't happen if graph is readonly.
        return -1; // "should never happen"
      }

      // Remove the mapping
      if( (refcnt = __unassign_dimension_code_CS( self, dim )) < 0 ) { // refcount will be 1 or 0 after this
        return -1; // failed to remove mapping
      }
      // String has been destroyed
      else if( refcnt == 0 ) {
        CSTR__dimension = NULL;
      }
    }

    // Perform the discard if string still exists
    if( CSTR__dimension ) {
      refcnt = icstringobject.DecrefNolock( CSTR__dimension );
    }
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
DLL_HIDDEN int64_t _vxenum_dim__discard_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  int64_t refcnt;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      refcnt = _vxenum_dim__discard_CS( self, dim );
    } GRAPH_RELEASE;
  }
  else {
    refcnt = _vxenum_dim__discard_CS( self, dim );
  }
  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_dim__decode_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  const CString_t *CSTR__dimension = NULL;
  // Perform lookup only if an actual dimension is provided
  if( dim >= FEATURE_VECTOR_DIMENSION_MIN && self->dim_decoder ) {
    framehash_t *decoder = self->dim_decoder;
    QWORD objkey = dim;
    if( CALLABLE( decoder )->GetObj( decoder, CELL_KEY_TYPE_PLAIN64, &objkey, (comlib_object_t**)&CSTR__dimension ) != CELL_VALUE_TYPE_OBJECT64 ) {
      CSTR__dimension = NULL;
    }
  }
  return CSTR__dimension;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _vxenum_dim__decode_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  const CString_t *CSTR__dimension;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      CSTR__dimension = _vxenum_dim__decode_CS( self, dim );
    } GRAPH_RELEASE;
  }
  else {
    CSTR__dimension = _vxenum_dim__decode_CS( self, dim );
  }
  return CSTR__dimension;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_dim__has_mapping_chars_CS( vgx_Similarity_t *self, const char *dimension ) {
  if( dimension && self->dim_encoder ) {
    framehash_t *encoder = self->dim_encoder;
    int sz = 0;
    QWORD dimhash = _vgx_enum__dimhash( dimension, &sz );
    if( CALLABLE(encoder)->HasKey64( encoder, CELL_KEY_TYPE_HASH64, dimhash ) == CELL_VALUE_TYPE_INTEGER ) {
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
DLL_HIDDEN bool _vxenum_dim__has_mapping_chars_OPEN( vgx_Similarity_t *self, const char *dimension ) {
  bool exists;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      exists = _vxenum_dim__has_mapping_chars_CS( self, dimension );
    } GRAPH_RELEASE;
  }
  else {
    exists = _vxenum_dim__has_mapping_chars_CS( self, dimension );
  }
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxenum_dim__has_mapping_dim_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  if( dim >= FEATURE_VECTOR_DIMENSION_MIN && self->dim_decoder ) {
    framehash_t *decoder = self->dim_decoder;
    QWORD objkey = dim;
    if( CALLABLE( decoder )->Has( decoder, CELL_KEY_TYPE_PLAIN64, &objkey ) == CELL_VALUE_TYPE_OBJECT64 ) {
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
DLL_HIDDEN bool _vxenum_dim__has_mapping_dim_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim ) {
  bool exists;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      exists = _vxenum_dim__has_mapping_dim_CS( self, dim );
    } GRAPH_RELEASE;
  }
  else {
    exists = _vxenum_dim__has_mapping_dim_CS( self, dim );
  }
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_dim__entries_CS( vgx_Similarity_t *self, CtptrList_t *CSTR__output ) {
  if( self->dim_decoder == NULL ) {
    return 0;
  }

  int ret = 0;
  framehash_t *decoder = self->dim_decoder;

  // TODO: Possible to allow this operation when graph is readonly?
  if( _vgx_is_readonly_CS( &self->parent->readonly ) ) {
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
DLL_HIDDEN int _vxenum_dim__entries_OPEN( vgx_Similarity_t *self, CtptrList_t *CSTR__output ) {
  int ret = 0;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      ret = _vxenum_dim__entries_CS( self, CSTR__output );
    } GRAPH_RELEASE;
  }
  else {
    ret = _vxenum_dim__entries_CS( self, CSTR__output );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__count_CS( vgx_Similarity_t *self ) {
  if( self->dim_decoder ) {
    framehash_t *decoder = self->dim_decoder;
    return CALLABLE( decoder )->Items( decoder );
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
DLL_HIDDEN int64_t _vxenum_dim__count_OPEN( vgx_Similarity_t *self ) {
  int64_t n = 0;
  if( self->parent ) {
    GRAPH_LOCK( self->parent ) {
      n = _vxenum_dim__count_CS( self );
    } GRAPH_RELEASE;
  }
  else {
    n = _vxenum_dim__count_CS( self );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__operation_sync_CS( vgx_Similarity_t *self ) {
  int64_t n_dim = 0;
  CtptrList_t *CSTR__strings;
  XTRY {
    if( (CSTR__strings = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    if( _vxenum_dim__entries_CS( self, CSTR__strings ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    n_dim = CALLABLE( CSTR__strings )->Length( CSTR__strings );
    int64_t i = 0;
    while( i < n_dim ) {
      tptr_t data;
      if( CALLABLE( CSTR__strings )->Get( CSTR__strings, i, &data ) == 1 ) {
        const CString_t *CSTR__dim = (const CString_t*)TPTR_GET_PTR56( &data );
        if( CSTR__dim ) {
          const char *dim = CStringValue( CSTR__dim );
          QWORD dimhash = 0;
          feature_vector_dimension_t dimcode = _vxenum_dim__encode_chars_CS( self, dim, &dimhash, false );
          iOperation.Enumerator_CS.AddDimension( self, dimhash, CSTR__dim, dimcode );
        }
      }
      ++i;
    }
    COMMIT_GRAPH_OPERATION_CS( self->parent );
  }
  XCATCH( errcode ) {
    n_dim = -1;
  }
  XFINALLY {
    if( CSTR__strings ) {
      COMLIB_OBJECT_DESTROY( CSTR__strings );
    }
  }
  return n_dim;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum_dim__operation_sync_OPEN( vgx_Similarity_t *self ) {
  int64_t n_dim = 0;
  GRAPH_LOCK( self->parent ) {
    n_dim = _vxenum_dim__operation_sync_CS( self );
  } GRAPH_RELEASE;
  return n_dim;
}





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_dim__create_enumerator( vgx_Similarity_t *self ) {
  int ret = 0;
  
  CString_t *CSTR__encoder_prefix = CStringNew( VGX_PATHDEF_CODEC_DIMENSION_ENCODER_PREFIX );
  CString_t *CSTR__decoder_prefix = CStringNew( VGX_PATHDEF_CODEC_DIMENSION_DECODER_PREFIX );
  CString_t *CSTR__encoder_name = NULL;
  CString_t *CSTR__decoder_name = NULL;
  CString_t *CSTR__codec_fullpath = NULL;

  XTRY {
    if( CSTR__encoder_prefix == NULL || CSTR__decoder_prefix == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x411 );
    }

    if( (CSTR__encoder_name = iString.Utility.NewGraphMapName( self->parent, CSTR__encoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x412 );
    }
    
    if( (CSTR__decoder_name = iString.Utility.NewGraphMapName( self->parent, CSTR__decoder_prefix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x413 );
    }

    if( self->parent ) {
      if( (CSTR__codec_fullpath = iString.NewFormat( NULL, "%s/" VGX_PATHDEF_INSTANCE_VECTOR "/" VGX_PATHDEF_CODEC, CALLABLE(self->parent)->FullPath(self->parent) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x414 );
      }
    }

    int max_size = FEATURE_VECTOR_DIMENSION_MAX;
    vgx_mapping_spec_t encoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;
    vgx_mapping_spec_t decoder_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;

    // Make sure we don't accidentaly make new ones if they already exist
    if( self->dim_encoder != NULL || self->dim_decoder != NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x415 );
    }

    // Create the encoder <hash> -> <int>
    self->dim_encoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__encoder_name, max_size, MAPPING_DEFAULT_ORDER, encoder_spec, CLASS_NONE );

    // Create the decoder <int> -> <CString_t*>
    PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( self->dimension_allocator_context ) {
      self->dim_decoder = iMapping.NewMap( CSTR__codec_fullpath, CSTR__decoder_name, max_size, MAPPING_DEFAULT_ORDER, decoder_spec, CLASS_NONE );
    } POP_STRING_ALLOCATOR_CONTEXT;

    // Check
    if( self->dim_encoder == NULL || self->dim_decoder == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x416 );
    }
  }
  XCATCH( errcode ) {
    _vxenum_dim__destroy_enumerator( self );
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
DLL_HIDDEN void _vxenum_dim__destroy_enumerator( vgx_Similarity_t *self ) {
  iMapping.DeleteMap( &self->dim_encoder );  
  iMapping.DeleteMap( &self->dim_decoder );  
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum_dim__initialize( void ) {
  // Initialize constants
  if( !g_init ) {
    CSTR__VECTOR_DIMENSION_ENUMERATION_NONE_STRING  = iString.New( NULL, __VECTOR_DIMENSION_ENUMERATION_NONE_STRING );
    CSTR__VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING = iString.New( NULL, __VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING );
    CSTR__VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING = iString.New( NULL, __VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING );
    CSTR__VECTOR_DIMENSION_ENUMERATION_ERROR_STRING = iString.New( NULL, __VECTOR_DIMENSION_ENUMERATION_ERROR_STRING );

    if( !CSTR__VECTOR_DIMENSION_ENUMERATION_NONE_STRING ||
        !CSTR__VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING ||
        !CSTR__VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING ||
        !CSTR__VECTOR_DIMENSION_ENUMERATION_ERROR_STRING
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
DLL_HIDDEN int _vxenum_dim__destroy( void ) {
  if( g_init ) {
    iString.Discard( (CString_t**)&CSTR__VECTOR_DIMENSION_ENUMERATION_NONE_STRING );
    iString.Discard( (CString_t**)&CSTR__VECTOR_DIMENSION_ENUMERATION_LOCKED_STRING );
    iString.Discard( (CString_t**)&CSTR__VECTOR_DIMENSION_ENUMERATION_NOENUM_STRING );
    iString.Discard( (CString_t**)&CSTR__VECTOR_DIMENSION_ENUMERATION_ERROR_STRING );
    return 1;
  }
  return 0;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum_dim.h"
  
test_descriptor_t _vgx_vxenum_dim_tests[] = {
  { "VGX Graph Dimension Enumeration Tests", __utest_vxenum_dim },
  {NULL}
};
#endif
