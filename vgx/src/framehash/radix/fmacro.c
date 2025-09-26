/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    fmacro.c
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

//#include "_fmacro.h"
#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*********/
/* TESTS */
/*********/

/*******************************************************************//**
 * _CELL_ITEM_MATCH
 *
 ***********************************************************************
 */
void __CELL_ITEM_MATCH( const framehash_context_t * const context, const framehash_cell_t * const cell, bool * const ismatch ) {
#define __1HIT_return { *ismatch = true; return; }
#define __0MISS_return { *ismatch = false; return; }

  switch( context->ktype ) {
  case CELL_KEY_TYPE_PLAIN64:
    if( _CELL_KEY( cell ) == context->key.plain ) __1HIT_return                // integer key match
    else __0MISS_return                                                        // no match
  case CELL_KEY_TYPE_HASH64:
    if( _CELL_SHORTID( cell ) == context->key.shortid ) __1HIT_return          // 64-bit hash match
    else __0MISS_return                                                        // not match
  case CELL_KEY_TYPE_HASH128:
    if( _CELL_SHORTID( cell ) != context->key.shortid ) __0MISS_return         // idL miss, declare no match
    else {
      if( _CELL_IS_VALUE( cell ) ) {
        if( context->obid == NULL ) __1HIT_return                              // by definition, we allow match on shorter key when storing integer if probe's obid is NULL
        else if( APTR_MATCH_IDHIGH( cell, context->obid->H ) ) __1HIT_return   // special case for negative cache hits (cached nonexist) and set memberships
        else __0MISS_return                                                    // probe's idH does not match the cell value
      }
      else {
        if( context->control.__expect_nonexist ) __0MISS_return                     // collision protection during rehashing (shortid match but different object)
        else if ( context->obid == NULL ) __1HIT_return                             // by definition, we allow match on idL only if probe doesn't specify obid
        else if( _CELL_OBJECT128_ID_MATCH( cell, context->obid ) )  __1HIT_return   // idH verified in referenced object, match
        else __0MISS_return                                                         // object's idH does not match probe's idH, no match
      }
    }
  default:
    __0MISS_return                                                             // unknown key type, no match
  };

#undef __1HIT_return
#undef __0MISS_return
}



/*******************************************************************//**
 * _CELL_REFERENCE_MATCH
 *
 ***********************************************************************
 */
bool __CELL_REFERENCE_MATCH( const framehash_cell_t * const cell, const void * const pointer ) {
  if( APTR_IS_POINTER( cell ) && APTR_MATCH_POINTER( cell, pointer ) )
    return true;
  else
    return false;
}



/*******************************************************************//**
 * _CELL_REFERENCE_IS_DIFFERENT
 *
 ***********************************************************************
 */
bool __CELL_REFERENCE_IS_DIFFERENT( const framehash_cell_t * const cell, const void * const pointer ) {
  if( APTR_IS_POINTER( cell ) && !APTR_MATCH_POINTER( cell, pointer ) )
    return true;
  else
    return false;
}



/*******************************************************************//**
 * _CELL_IS_DESTRUCTIBLE
 *
 ***********************************************************************
 */
bool __CELL_IS_DESTRUCTIBLE( const framehash_cell_t * const cell ) {
  if( _CELL_REFERENCE_EXISTS( cell ) && COMLIB_OBJECT_DESTRUCTIBLE( _CELL_GET_OBJECT128( cell ) ) )
    return true;
  else
    return false;
}



/*******************************************************************//**
 * _CELL_OBJECT128_ID_MATCH
 *
 ***********************************************************************
 */
bool __CELL_OBJECT128_ID_MATCH( const framehash_cell_t * const cell, const objectid_t * const pobid ) {
  comlib_object_t *obj = _CELL_GET_OBJECT128( cell );
  bool match = (obj == NULL || COMLIB_OBJECT_CMPID( obj, pobid ) == 0) ? true : false;
  return match;
}



/*******************************************************************//**
 * _SET_CELL_KEY
 *
 ***********************************************************************
 */
void __SET_CELL_KEY( const framehash_context_t * const context, framehash_cell_t * const cell ) {
  if( context->ktype == CELL_KEY_TYPE_PLAIN64 ) {
    APTR_AS_ANNOTATION( cell ) = context->key.plain;
  }
  else {
    APTR_AS_ANNOTATION( cell ) = context->key.shortid;
  }
  APTR_AS_DKEY( cell ) = context->ktype;
}



/*********/
/* MARKS */
/*********/

/*******************************************************************//**
 * _MARK_CACHED_NONEXIST
 *
 ***********************************************************************
 */
void __MARK_CACHED_NONEXIST( const framehash_context_t * const context, framehash_cell_t * const cell ) {
  __SET_CELL_KEY( context, cell );
  // Mark the data as ID56 internally (the hit that this cell is representing the lack of something)
  if( context->obid != NULL )
    APTR_SET_IDHIGH( cell, context->obid->H );
  else
    APTR_SET_IDHIGH( cell, 0 );
  // Mark the cell as empty (invalid, clean, data)
  APTR_AS_TAG( cell ) = CELL_TYPE_EMPTY;
}



/***************/
/* DATA ACCESS */
/***************/

/*******************************************************************//**
 * _STORE_OBJECT128_POINTER
 *
 ***********************************************************************
 */
void __STORE_OBJECT128_POINTER( const framehash_context_t * const context, framehash_cell_t * const cell ) {
  APTR_AS_ANNOTATION( cell ) = context->key.shortid; // objects can only use hashed key
  APTR_SET_POINTER( cell, context->value.pobject );
  APTR_AS_TAG( cell ) = CELL_TYPE_OBJECT128;
}



/*******************************************************************//**
 * _STORE_MEMBERSHIP
 *
 ***********************************************************************
 */
void __STORE_MEMBERSHIP( const framehash_context_t * const context, framehash_cell_t * const cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_IDHIGH( cell, context->value.idH56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_BOOLEAN
 *
 ***********************************************************************
 */
void __STORE_BOOLEAN( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_BOOLEAN( cell, context->value.raw56 ? true : false );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_UNSIGNED
 *
 ***********************************************************************
 */
void __STORE_UNSIGNED( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_UNSIGNED( cell, context->value.raw56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_INTEGER
 *
 ***********************************************************************
 */
void __STORE_INTEGER( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_INTEGER( cell, context->value.raw56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_REAL
 *
 ***********************************************************************
 */
void __STORE_REAL( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_REAL( cell, context->value.real56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_POINTER
 *
 ***********************************************************************
 */
void __STORE_POINTER( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_PTR56( cell, context->value.ptr56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _STORE_OBJECT64_POINTER
 *
 ***********************************************************************
 */
void __STORE_OBJECT64_POINTER( const framehash_context_t * const context, framehash_cell_t *cell ) {
  __SET_CELL_KEY( context, cell );
  APTR_SET_OBJ56( cell, context->value.obj56 );
  APTR_AS_TAG( cell ) = CELL_TYPE_RAW56;
}



/*******************************************************************//**
 * _INITIALIZE_DATA_CELL
 *
 ***********************************************************************
 */
void __INITIALIZE_DATA_CELL( framehash_cell_t * const cell, const _framehash_celltype_t tag ) {
  APTR_AS_ANNOTATION( cell ) = FRAMEHASH_KEY_NONE;
  APTR_AS_QWORD( cell ) = tag & TPTR_TAG_MASK;
}



/*******************************************************************//**
 * _INITIALIZE_REFERENCE_CELL
 *
 ***********************************************************************
 */
void __INITIALIZE_REFERENCE_CELL( framehash_cell_t * const cell, int order, int domain, framehash_ftype_t ftype ) {
  framehash_metas_t *metas = (framehash_metas_t*)APTR_GET_ANNOTATION_POINTER( cell );
  metas->QWORD = 0;
  metas->order = (uint8_t)order;
  metas->domain = (uint8_t)domain;
  metas->ftype = (uint8_t)ftype;
  APTR_AS_QWORD( cell ) = 0;
}



/*******************************************************************//**
 * _SET_ITEM_TAG
 *
 ***********************************************************************
 */
void __SET_ITEM_TAG( framehash_cell_t * const cell, const _framehash_celltype_t tag ) {
  APTR_AS_TAG( cell ) = tag;
}



/*******************************************************************//**
 * _SET_CELL_FROM_ELEMENTS
 *
 ***********************************************************************
 */
void __SET_CELL_FROM_ELEMENTS( framehash_cell_t * const cell, const QWORD annotation, const QWORD data ) {
  APTR_AS_ANNOTATION( cell ) = annotation;
  APTR_AS_QWORD( cell ) = data;
}



/*******************************************************************//**
 * _DESTROY_PREVIOUS_OBJECT128
 *
 ***********************************************************************
 */
void __DESTROY_PREVIOUS_OBJECT128( const framehash_context_t * const context, framehash_cell_t * const cell ) {
  if( __CELL_IS_DESTRUCTIBLE( cell ) && __CELL_REFERENCE_IS_DIFFERENT( cell, context->value.pobject ) ) {
    COMLIB_OBJECT_DESTROY( _CELL_GET_OBJECT128( cell ) );
  }
}



/*******************************************************************//**
 * _DELETE_ITEM
 *
 ***********************************************************************
 */
DLL_EXPORT void __DELETE_ITEM( framehash_cell_t * const cell ) {
  __INITIALIZE_DATA_CELL( cell, CELL_TYPE_EMPTY );
}



/*******************************************************************//**
 * _CELL_STEAL
 *
 ***********************************************************************
 */
void __CELL_STEAL( framehash_cell_t * const dest, framehash_cell_t * const src ) {
  APTR_COPY( dest, src );
  DELETE_CELL_REFERENCE( src );
}



/*******************************************************************//**
 * _GET_CELL_OBJECT128_ID
 *
 ***********************************************************************
 */
objectid_t * __GET_CELL_OBJECT128_ID( framehash_cell_t * const cell ) {
  comlib_object_t *obj = _CELL_GET_OBJECT128( cell );
  return COMLIB_OBJECT_GETID( obj );
}



/****************/
/* DATA LOADING */
/****************/


/*******************************************************************//**
 * _LOAD_OBJECT128_POINTER_CONTEXT_VALUE
 *
 ***********************************************************************
 */
void __LOAD_OBJECT128_POINTER_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  context->value.pobject = APTR_GET_POINTER( cell );
  context->vtype = CELL_VALUE_TYPE_OBJECT128;
}


/*******************************************************************//**
 * _LOAD_OBJECT128_POINTER_CONTEXT_KEY
 *
 ***********************************************************************
 */
void __LOAD_OBJECT128_POINTER_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  // COMLIB pointers have only 3-bit tag and nothing to indicate DTYPE or DKEY like the RAW56 context
  context->ktype = CELL_KEY_TYPE_HASH128;     // <- implied
  context->key.plain = FRAMEHASH_KEY_NONE;
  context->key.shortid = _CELL_SHORTID( cell );
}


/*******************************************************************//**
 * _LOAD_RAW56_CONTEXT_VALUE
 *
 ***********************************************************************
 */
void __LOAD_RAW56_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  tagged_dtype_t dtype = APTR_AS_DTYPE( cell );
  switch( dtype ) {
  case TAGGED_DTYPE_ID56:
    context->value.idH56 = APTR_GET_IDHIGH( cell );
    context->vtype = CELL_VALUE_TYPE_MEMBER;
    break;
  case TAGGED_DTYPE_BOOL:
    context->value.raw56 = APTR_GET_BOOLEAN( cell );
    context->vtype = CELL_VALUE_TYPE_BOOLEAN;
    break;
  case TAGGED_DTYPE_UINT56:
    context->value.raw56 = APTR_AS_UNSIGNED( cell );
    context->vtype = CELL_VALUE_TYPE_UNSIGNED;
    break;
  case TAGGED_DTYPE_INT56:
    context->value.raw56 = APTR_AS_INTEGER( cell );
    context->vtype = CELL_VALUE_TYPE_INTEGER;
    break;
  case TAGGED_DTYPE_REAL56:
    context->value.real56 = APTR_GET_REAL( cell );
    context->vtype = CELL_VALUE_TYPE_REAL;
    break;
  case TAGGED_DTYPE_PTR56:
    context->value.ptr56 = APTR_GET_PTR56( cell );
    context->vtype = CELL_VALUE_TYPE_POINTER;
    break;
  case TAGGED_DTYPE_OBJ56:
    context->value.obj56 = APTR_GET_OBJ56( cell );
    context->vtype = CELL_VALUE_TYPE_OBJECT64;
    break;
  default:
    /* invalid data */
    context->value.qword = APTR_AS_QWORD( cell );
    context->vtype = CELL_VALUE_TYPE_ERROR;
    break;
  }
}


/*******************************************************************//**
 * _LOAD_RAW56_CONTEXT_KEY
 *
 ***********************************************************************
 */
void __LOAD_RAW56_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  tagged_dkey_t dref = APTR_AS_DKEY( cell );
  switch( dref ) {
  case TAGGED_DKEY_PLAIN64:
    context->key.plain = _CELL_KEY( cell );
    context->key.shortid = context->dynamic->hashf( context->key.plain );
    context->ktype = CELL_KEY_TYPE_PLAIN64;
    break;
  case TAGGED_DKEY_HASH64:
    context->key.plain = FRAMEHASH_KEY_NONE;
    context->key.shortid = _CELL_SHORTID( cell );
    context->ktype = CELL_KEY_TYPE_HASH64;
    break;
  default:
    context->key.plain = FRAMEHASH_KEY_NONE;
    if( context->vtype == CELL_VALUE_TYPE_MEMBER ) {
      context->key.shortid = _CELL_SHORTID( cell );
      context->ktype = CELL_KEY_TYPE_HASH128;
    }
    /* invalid data */
    else {
      context->key.shortid = 0;
      context->ktype = CELL_KEY_TYPE_ERROR;
    }
    break;
  }
}


/*******************************************************************//**
 * _LOAD_NULL_CONTEXT_VALUE
 *
 ***********************************************************************
 */
void __LOAD_NULL_CONTEXT_VALUE( framehash_context_t * const context ) {
  context->value.pobject = NULL;
  context->vtype = CELL_VALUE_TYPE_NULL;
}



/*******************************************************************//**
 * _LOAD_CONTEXT_VALUE
 *
 ***********************************************************************
 */
void __LOAD_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  if( _CELL_IS_REFERENCE( cell ) ) {
    __LOAD_OBJECT128_POINTER_CONTEXT_VALUE( context, cell );
  }
  else {
    __LOAD_RAW56_CONTEXT_VALUE( context, cell );
  }
}



/*******************************************************************//**
 * _LOAD_CONTEXT_KEY
 *
 ***********************************************************************
 */
void __LOAD_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  if( _CELL_IS_REFERENCE( cell ) ) {
    __LOAD_OBJECT128_POINTER_CONTEXT_KEY( context, cell );
  }
  else {
    __LOAD_RAW56_CONTEXT_KEY( context, cell );
  }
}



/*******************************************************************//**
 * _LOAD_CONTEXT_VALUE_TYPE
 *
 ***********************************************************************
 */
void __LOAD_CONTEXT_VALUE_TYPE( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  if( _CELL_IS_REFERENCE( cell ) ) {
    context->vtype = CELL_VALUE_TYPE_OBJECT128;
  }
  else {
    tagged_dtype_t dtype = APTR_AS_DTYPE( cell );
    switch( dtype ) {
    case TAGGED_DTYPE_ID56:
      context->vtype = CELL_VALUE_TYPE_MEMBER;
      break;
    case TAGGED_DTYPE_BOOL:
      context->vtype = CELL_VALUE_TYPE_BOOLEAN;
      break;
    case TAGGED_DTYPE_UINT56:
      context->vtype = CELL_VALUE_TYPE_UNSIGNED;
      break;
    case TAGGED_DTYPE_INT56:
      context->vtype = CELL_VALUE_TYPE_INTEGER;
      break;
    case TAGGED_DTYPE_REAL56:
      context->vtype = CELL_VALUE_TYPE_REAL;
      break;
    case TAGGED_DTYPE_PTR56:
      context->vtype = CELL_VALUE_TYPE_POINTER;
      break;
    case TAGGED_DTYPE_OBJ56:
      context->vtype = CELL_VALUE_TYPE_OBJECT64;
      break;
    default:
      /* invalid data */
      context->vtype = CELL_VALUE_TYPE_ERROR;
      break;
    }
  }
}



/*******************************************************************//**
 * _LOAD_CONTEXT_KEY_TYPE
 *
 ***********************************************************************
 */
void __LOAD_CONTEXT_KEY_TYPE( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  if( _CELL_IS_REFERENCE( cell ) ) {
    context->ktype = CELL_KEY_TYPE_HASH128;
  }
  else {
    context->ktype = _CELL_VALUE_DKEY( cell );
  }
}



/*******************************************************************//**
 * _LOAD_CONTEXT_FROM_CELL
 *
 ***********************************************************************
 */
void __LOAD_CONTEXT_FROM_CELL( framehash_context_t * const context, const framehash_cell_t * const cell ) {
  __LOAD_CONTEXT_VALUE( context, cell );
  __LOAD_CONTEXT_KEY( context, cell );
}



/********************/
/* COUNT MANAGEMENT */
/********************/


/*******************************************************************//**
 * _CACHE_INCREMENT_NCHAINS
 *
 ***********************************************************************
 */
uint16_t __CACHE_INCREMENT_NCHAINS( framehash_metas_t * const cache_metas ) {
  cache_metas->nchains++;
  if( cache_metas->nchains > _CACHE_FRAME_NSLOTS(cache_metas->order) )
    FATAL(0,"nchains too high after increment");
  if( cache_metas->nchains == 0 )
    FATAL(0,"nchains zero after increment");
  return cache_metas->nchains;
}



/*******************************************************************//**
 * _CACHE_DECREMENT_NCHAINS
 *
 ***********************************************************************
 */
uint16_t __CACHE_DECREMENT_NCHAINS( framehash_metas_t * const cache_metas ) {
  cache_metas->nchains--;
  if( cache_metas->nchains > _CACHE_FRAME_NSLOTS(cache_metas->order) )
    FATAL(0,"nchains too high after decrement");
  return cache_metas->nchains;
}



/*******************************************************************//**
 * _CACHE_HAS_NO_CHAINS
 *
 ***********************************************************************
 */
bool __CACHE_HAS_NO_CHAINS( const framehash_metas_t * const cache_metas ) {
  return cache_metas->nchains == 0;
}



/*******************************************************************//**
 * _CACHE_SET_NCHAINS
 *
 ***********************************************************************
 */
uint16_t __CACHE_SET_NCHAINS( framehash_metas_t * const cache_metas, const uint16_t nchains ) {
  cache_metas->nchains = nchains;
  if( cache_metas->nchains > _CACHE_FRAME_NSLOTS(cache_metas->order) )
    FATAL(0,"nchains too high after set");
  return cache_metas->nchains;
}



/*******************************************************************//**
 * _CACHE_GET_NCHAINS
 *
 ***********************************************************************
 */
uint16_t __CACHE_GET_NCHAINS( const framehash_metas_t * const cache_metas ) {
  return cache_metas->nchains;
}



/*******************************************************************//**
 * __CELL_GET_FRAME_METAS
 *
 ***********************************************************************
 */
framehash_metas_t * __CELL_GET_FRAME_METAS( const framehash_cell_t * const cell ) {
  return APTR_GET_ANNOTATION_POINTER( cell );
}



/*******************************************************************//**
 * __CELL_GET_FRAME_TYPE
 *
 ***********************************************************************
 */
uint8_t __CELL_GET_FRAME_TYPE( const framehash_cell_t * const cell ) {
  return __CELL_GET_FRAME_METAS( cell )->ftype;
}



/*******************************************************************//**
 * __CELL_GET_FRAME_SLOTS
 *
 ***********************************************************************
 */
framehash_slot_t * __CELL_GET_FRAME_SLOTS( const framehash_cell_t * const cell ) {
  return (framehash_slot_t*)APTR_GET_POINTER( cell );
}



/*******************************************************************//**
 * __CELL_GET_FRAME_CELLS
 *
 ***********************************************************************
 */
framehash_cell_t * __CELL_GET_FRAME_CELLS( const framehash_cell_t * const cell ) {
  return (framehash_cell_t*)APTR_GET_POINTER( cell );
}



/*******************************************************************//**
 * __DELETE_CELL_REFERENCE
 *
 ***********************************************************************
 */
void __DELETE_CELL_REFERENCE( framehash_cell_t * const cell ) {
  APTR_SET_POINTER( cell, NULL );
}




/*******************************************************************//**
 * FRAMEHASH_CONTROL_FLAGS_INIT
 *
 ***********************************************************************
 */
DLL_EXPORT const framehash_control_flags_t FRAMEHASH_CONTROL_FLAGS_INIT = {
  .__expect_nonexist = 0,
  .cache = {
    .enable_read = 1,
    .enable_write = 1,
    .allow_cmorph = 1,
    .__rsv = 0
  },
  .loadfactor = {
    .high = 0,
    .low = 0
  },
  .growth = {
    .minimal = 0,
    .__rsv = 0
  },
  .__rsv = {0}
};



/*******************************************************************//**
 * FRAMEHASH_CONTROL_FLAGS_REHASH
 *
 ***********************************************************************
 */
DLL_EXPORT const framehash_control_flags_t FRAMEHASH_CONTROL_FLAGS_REHASH = {
  .__expect_nonexist = 1, /* Trigger skipping of cells with same shortid when filling new frame, to avoid overwriting it (since it's too costly to load obid for each rehashed object) */
  .cache = {
    .enable_read = 1,
    .enable_write = 1,
    .allow_cmorph = 1,
    .__rsv = 0
  },
  .loadfactor = {
    .high = 0,
    .low = 0
  },
  .growth = {
    .minimal = 0,
    .__rsv = 0
  },
  .__rsv = {0}
};



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_NEW_CONTEXT
 *
 ***********************************************************************
 */
DLL_EXPORT framehash_processing_context_t FRAMEHASH_PROCESSOR_NEW_CONTEXT( framehash_cell_t *startframe, framehash_dynamic_t *dynamic, f_framehash_cell_processor_t function ) {
  framehash_processing_context_t context = {
    .instance = {
      .frame          = startframe,
      .dynamic        = dynamic
    },
    .processor = {
      .function       = function,
      .limit          = LLONG_MAX,
      .input          = NULL,
      .output         = NULL
    },
    .flags = {
      .allow_cached   = 0,
      .readonly       = 1,
      .failed         = 0,
      .completed      = 0,
      .__rsv          = {0}
    },
    .__internal = {
      .__delta_items  = 0
    }
  };
  // Rely on return value optimization for this function
  return context;
}



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT
 *
 ***********************************************************************
 */
DLL_EXPORT framehash_processing_context_t FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( framehash_cell_t *startframe, framehash_dynamic_t *dynamic, f_framehash_cell_processor_t function, int64_t limit ) {
  framehash_processing_context_t context = {
    .instance = {
      .frame          = startframe,
      .dynamic        = dynamic
    },
    .processor = {
      .function       = function,
      .limit          = limit,
      .input          = NULL,
      .output         = NULL
    },
    .flags = {
      .allow_cached   = 0,
      .readonly       = 1,
      .failed         = 0,
      .completed      = 0,
      .__rsv          = {0}
    },
    .__internal = {
      .__delta_items  = 0
    }
  };
  // Rely on return value optimization for this function
  return context;
}



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_SET_INPUT_OUTPUT
 *
 ***********************************************************************
 */
DLL_EXPORT void FRAMEHASH_PROCESSOR_SET_IO( framehash_processing_context_t *context, void *input, void *output ) {
  context->processor.input = input;
  context->processor.output = output;
}



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_MAY_MODIFY
 *
 ***********************************************************************
 */
DLL_EXPORT void FRAMEHASH_PROCESSOR_MAY_MODIFY( framehash_processing_context_t *context ) {
  context->flags.readonly = false;
}



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_PRESERVE_CACHE
 *
 ***********************************************************************
 */
DLL_EXPORT void FRAMEHASH_PROCESSOR_PRESERVE_CACHE( framehash_processing_context_t *context ) {
  context->flags.allow_cached = true;
}



/*******************************************************************//**
 * FRAMEHASH_PROCESSOR_DELETE_CELL
 *
 ***********************************************************************
 */
DLL_EXPORT void FRAMEHASH_PROCESSOR_DELETE_CELL( framehash_processing_context_t *context, framehash_cell_t *cell ) {
  // TODO: Find a better way to do this
  if( context->flags.readonly ) {
    CRITICAL( 0xFFF, "Readonly framehash processor, item not deleted!" );
    return;
  }

  _framehash_leaf__delete_item( context->instance.frame, cell );
  context->__internal.__delta_items--;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_fmacro.h"

DLL_HIDDEN test_descriptor_t _framehash_fmacro_tests[] = {
  {"Arithmetic Macros",      __utest_framehash_fmacro__arithmetic_macros},
  {"Shortid",                __utest_framehash_fmacro__shortid},
  {"_CELL_ITEM_MATCH",       __utest_framehash_fmacro__cell_item_match},
  {NULL}
};
#endif
