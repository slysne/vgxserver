/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_generic.c
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

#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 * _framehash_api_generic__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_generic__set( framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const void *pkey, const void * const data_bytes ) {

  framehash_retcode_t insertion;
  framehash_context_t insert_context = {
    .value = {0},
    .vtype = vtype,
    .control = {0}
  };
  insert_context.control.cache.enable_write = 1;

  if( vtype == CELL_VALUE_TYPE_NULL ) {
    /* special case: interpret as delete */
    return _framehash_api_generic__del( self, ktype, pkey );
  }

  if( __framehash__set_context_key( self, &insert_context, ktype, pkey ) == ktype ) {

    // Populate the context value
    switch( insert_context.vtype ) {
    case CELL_VALUE_TYPE_MEMBER:
      insert_context.value.idH56 = insert_context.obid ? insert_context.obid->H : 0;
      break;
    case CELL_VALUE_TYPE_BOOLEAN:
      insert_context.value.raw56 = *((bool*)data_bytes);
      break;
    case CELL_VALUE_TYPE_UNSIGNED:
      /* FALLTHRU */
    case CELL_VALUE_TYPE_INTEGER:
      insert_context.value.raw56 = *((QWORD*)data_bytes);
      break;
    case CELL_VALUE_TYPE_REAL:
      insert_context.value.real56 = *((double*)data_bytes);
      break;
    case CELL_VALUE_TYPE_POINTER:
      insert_context.value.ptr56 = (char*)data_bytes;
      break;
    case CELL_VALUE_TYPE_OBJECT64:
      if( insert_context.ktype == CELL_KEY_TYPE_HASH128 ) {
        return CELL_VALUE_TYPE_ERROR; // incompatible key
      }
      insert_context.value.obj56 = (comlib_object_t*)data_bytes;
      break;
    case CELL_VALUE_TYPE_OBJECT128:
      if( insert_context.ktype != CELL_KEY_TYPE_HASH128 ) {
        return CELL_VALUE_TYPE_ERROR; // incompatible key
      }
      insert_context.value.pobject = (comlib_object_t*)data_bytes;
      break;
    default:
      /* special case: error */
      return CELL_VALUE_TYPE_ERROR;
    }

    BEGIN_FRAMEHASH_WRITE( self, &insert_context ) {
      if( insert_context.obid ) {
        __SYNCHRONIZE_RADIX_ON_OBID( self, insert_context.obid ) {
          CHANGELOG_EMIT_OPERATION( self, &insert_context );
          insertion = _framehash_radix__set( &insert_context );
          __MOD_INCREMENT( insertion, self );
        } __RELEASE_RADIX;
      }
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, insert_context.key.shortid ) {
          insertion = _framehash_radix__set( &insert_context );
          __MOD_INCREMENT( insertion, self );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_WRITE( &insertion );

  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
  
  return __ACTIONCODE_BY_OPERATION( insertion, insert_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_generic__del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_generic__del( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey ) {
  framehash_retcode_t deletion;
  framehash_context_t delete_context = {
    .value   = {0},
    .vtype   = CELL_VALUE_TYPE_NULL,
    .control = {0}
  };
  delete_context.control.cache.enable_write = 1;

  if( __framehash__set_context_key( self, &delete_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_WRITE( self, &delete_context ) {
      if( ktype == CELL_KEY_TYPE_HASH128 ) {
        __SYNCHRONIZE_RADIX_ON_OBID( self, delete_context.obid ) {
          CHANGELOG_EMIT_OPERATION( self, &delete_context );
          deletion = _framehash_radix__del( &delete_context );
          __MOD_DECREMENT( deletion, self );
        } __RELEASE_RADIX;
      }
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, delete_context.key.shortid ) {
          deletion = _framehash_radix__del( &delete_context );
          __MOD_DECREMENT( deletion, self );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_WRITE( &deletion );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }

  return __ACTIONCODE_BY_OPERATION( deletion, delete_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_generic__has
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_generic__has( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey ) {
  framehash_retcode_t existence;
  framehash_context_t has_context = {
    .value = {0},
    .vtype = CELL_VALUE_TYPE_NULL,
    .control = {0}
  };
  has_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( __framehash__set_context_key( self, &has_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_READ( self, &has_context ) {
      if( ktype == CELL_KEY_TYPE_HASH128 ) {
        __SYNCHRONIZE_RADIX_ON_OBID( self, has_context.obid ) {
          existence = _framehash_radix__get( &has_context );
        } __RELEASE_RADIX;
      }
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, has_context.key.shortid ) {
          existence = _framehash_radix__get( &has_context );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_READ( &existence );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
  
  return __ACTIONCODE_BY_OPERATION( existence, has_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_generic__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_generic__get( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, void * const return_bytes ) {
  framehash_retcode_t retrieval;
  framehash_context_t query_context = {
    .value   = {0},
    .vtype   = CELL_VALUE_TYPE_NULL,
    .control = {0}
  };
  query_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( __framehash__set_context_key( self, &query_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_READ( self, &query_context ) {
      if( ktype == CELL_KEY_TYPE_HASH128 ) {
        __SYNCHRONIZE_RADIX_ON_OBID( self, query_context.obid ) {
          retrieval = _framehash_radix__get( &query_context );
        } __RELEASE_RADIX;
      }
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_context.key.shortid ) {
          retrieval = _framehash_radix__get( &query_context );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_READ( &retrieval );

    if( retrieval.completed ) {
      switch( query_context.vtype ) {
      case CELL_VALUE_TYPE_NULL:
        *(QWORD*)return_bytes = 0;
        break;
      case CELL_VALUE_TYPE_MEMBER:
        *(QWORD*)return_bytes = 1;
        break;
      case CELL_VALUE_TYPE_BOOLEAN:
        *(QWORD*)return_bytes = query_context.value.raw56 != 0;
        break;
      case CELL_VALUE_TYPE_UNSIGNED:
        *(uint64_t*)return_bytes = (uint64_t)query_context.value.raw56;
        break;
      case CELL_VALUE_TYPE_INTEGER:
        *(int64_t*)return_bytes = (int64_t)query_context.value.raw56;
        break;
      case CELL_VALUE_TYPE_REAL:
        *(double*)return_bytes = (double)query_context.value.real56;
        break;
      case CELL_VALUE_TYPE_POINTER:
        *(char**)return_bytes = query_context.value.ptr56; /* scary... */
        break;
      case CELL_VALUE_TYPE_OBJECT64:
        *(comlib_object_t**)return_bytes = (comlib_object_t*)query_context.value.obj56;
        break;
      case CELL_VALUE_TYPE_OBJECT128:
        *(comlib_object_t**)return_bytes = (comlib_object_t*)query_context.value.pobject;
        break;
      default:
        return CELL_VALUE_TYPE_ERROR;
      }
      return query_context.vtype;
    }
    else if( !retrieval.error ) {
      return CELL_VALUE_TYPE_NULL;
    }
    else {
      return CELL_VALUE_TYPE_ERROR;
    }
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_generic__inc
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_generic__inc( framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const QWORD key, void * const upsert_bytes, void * const return_bytes ) {

  framehash_retcode_t retrieval;
  framehash_context_t query_context = {
    .value   = {0},
    .vtype   = CELL_VALUE_TYPE_NULL,
    .control = {0}
  };
  query_context.control.cache.enable_read = self->_flag.cache.r_ena;
  query_context.control.cache.enable_write = self->_flag.cache.w_ena;

  framehash_retcode_t upsertion;
  framehash_context_t upsert_context;

  switch( vtype ) {
  case CELL_VALUE_TYPE_UNSIGNED:
    /* FALLTHRU */
  case CELL_VALUE_TYPE_INTEGER:
    /* FALLTHRU */
  case CELL_VALUE_TYPE_REAL:
    break;
  default:
    // Non-numeric value type not compatible with inc operation
    return CELL_VALUE_TYPE_ERROR;
  }

  if( ktype == CELL_KEY_TYPE_HASH128 ) {
    return CELL_VALUE_TYPE_ERROR;
  }
    
    
  if( __framehash__set_context_key( self, &query_context, ktype, &key ) == ktype ) {
    BEGIN_FRAMEHASH_READ( self, &query_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_context.key.shortid ) {
        retrieval = _framehash_radix__get( &query_context );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &retrieval );

    if( retrieval.error ) {
      return CELL_VALUE_TYPE_ERROR;
    }
    else {
      // Clone the query context into the upsert context and override value and vtype below
      memcpy( &upsert_context, &query_context, sizeof( framehash_context_t ) );
      
      // What's the previous value type?
      switch( query_context.vtype ) {

      /* No previous value */
      case CELL_VALUE_TYPE_NULL:
        // Type will be initial value
        upsert_context.vtype = vtype;
        switch( vtype ) {
        // Initialize with unsigned
        case CELL_VALUE_TYPE_UNSIGNED:
          upsert_context.value.raw56 = *((uint64_t*)upsert_bytes);
          break;
        // Initialize with integer
        case CELL_VALUE_TYPE_INTEGER:
          upsert_context.value.int56 = *((int64_t*)upsert_bytes);
          break;
        // Initialize with real
        case CELL_VALUE_TYPE_REAL:
          upsert_context.value.real56 = *((double*)upsert_bytes);
          break;
        default:
          return CELL_VALUE_TYPE_ERROR;
        }
        break;

      /* Previous value was unsigned */
      case CELL_VALUE_TYPE_UNSIGNED:
        switch( vtype ) {
        // Add new unsigned to existing unsigned (stays unsigned)
        case CELL_VALUE_TYPE_UNSIGNED:
          upsert_context.vtype = CELL_VALUE_TYPE_UNSIGNED;
          upsert_context.value.raw56 = (uint64_t)(query_context.value.raw56 + *((uint64_t*)upsert_bytes));
          break;
        // Add new integer to existing unsigned (convert to signed)
        case CELL_VALUE_TYPE_INTEGER:
          upsert_context.vtype = CELL_VALUE_TYPE_INTEGER;
          upsert_context.value.int56 = (int64_t)(query_context.value.raw56 + *((int64_t*)upsert_bytes));
          break;
        // Add new real to existing unsigned (convert to double)
        case CELL_VALUE_TYPE_REAL:
          upsert_context.vtype = CELL_VALUE_TYPE_REAL;
          upsert_context.value.real56 = (double)(query_context.value.raw56 + *((double*)upsert_bytes));
          break;
        default:
          return CELL_VALUE_TYPE_ERROR;
        }
        break;

      /* Previous value was integer */
      case CELL_VALUE_TYPE_INTEGER:
        switch( vtype ) {
        // Add new unsigned to existing signed (stays signed)
        case CELL_VALUE_TYPE_UNSIGNED:
          upsert_context.vtype = CELL_VALUE_TYPE_INTEGER;
          upsert_context.value.int56 = (int64_t)(query_context.value.int56 + *((uint64_t*)upsert_bytes));
          break;
        // Add new signed to existing signed (stays signed)
        case CELL_VALUE_TYPE_INTEGER:
          upsert_context.vtype = CELL_VALUE_TYPE_INTEGER;
          upsert_context.value.int56 = (int64_t)(query_context.value.int56 + *((int64_t*)upsert_bytes));
          break;
        // Add new real to existing signed (convert to double)
        case CELL_VALUE_TYPE_REAL:
          upsert_context.vtype = CELL_VALUE_TYPE_REAL;
          upsert_context.value.real56 = (double)(query_context.value.int56 + *((double*)upsert_bytes));
          break;
        default:
          return CELL_VALUE_TYPE_ERROR;
        }
        break;

      /* Previous value was real */
      case CELL_VALUE_TYPE_REAL:
        switch( vtype ) {
        // Add new unsigned to existing real (stays double)
        case CELL_VALUE_TYPE_UNSIGNED:
          upsert_context.vtype = CELL_VALUE_TYPE_REAL;
          upsert_context.value.real56 = (double)(query_context.value.real56 + *((uint64_t*)upsert_bytes));
          break;
        // Add new signed to existing real (stays double)
        case CELL_VALUE_TYPE_INTEGER:
          upsert_context.vtype = CELL_VALUE_TYPE_REAL;
          upsert_context.value.real56 = (double)(query_context.value.real56 + *((int64_t*)upsert_bytes));
          break;
        // Add new real to existing real (stays double)
        case CELL_VALUE_TYPE_REAL:
          upsert_context.vtype = CELL_VALUE_TYPE_REAL;
          upsert_context.value.real56 = (double)(query_context.value.real56 + *((double*)upsert_bytes));
          break;
        default:
          return CELL_VALUE_TYPE_ERROR;
        }
        break;

      default:
        // Found incompatible value for inc operation
        return CELL_VALUE_TYPE_ERROR;
      }

      BEGIN_FRAMEHASH_WRITE( self, &upsert_context ) {
        // Perform upsert operation
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, upsert_context.key.shortid ) {
          upsertion = _framehash_radix__set( &upsert_context );
          __MOD_INCREMENT( upsertion, self );
        } __RELEASE_RADIX;
      } END_FRAMEHASH_WRITE( &upsertion );

      // Populate the return value
      switch( upsert_context.vtype ) {
        case CELL_VALUE_TYPE_UNSIGNED:
          *(uint64_t*)return_bytes = upsert_context.value.raw56;
          return upsert_context.vtype;
        case CELL_VALUE_TYPE_INTEGER:
          *(int64_t*)return_bytes = upsert_context.value.int56;
          return upsert_context.vtype;
        case CELL_VALUE_TYPE_REAL:
          *(double*)return_bytes = upsert_context.value.real56;
          return upsert_context.vtype;
        default:
          return CELL_VALUE_TYPE_ERROR;
      }
    }
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_generic.h"

DLL_HIDDEN test_descriptor_t _framehash_api_generic_tests[] = {
  { "api_generic",   __utest_framehash_api_generic },
  {NULL}
};
#endif
