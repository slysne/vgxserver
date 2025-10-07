/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_int56.c
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
 * _framehash_api_int56__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_int56__set( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t value ) {
  framehash_context_t insert_int_context = {
    .vtype = CELL_VALUE_TYPE_INTEGER,
    .value = {
      .int56 = value
    },
    .control = {0}
  };
  insert_int_context.control.cache.enable_write = 1;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &insert_int_context, ktype, &key ) == ktype ) {
    framehash_retcode_t insertion;
    BEGIN_FRAMEHASH_WRITE( self, &insert_int_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, insert_int_context.key.shortid ) {
        // insert integer value keyed by shortid 
        insertion = _framehash_radix__set( &insert_int_context );
        __MOD_INCREMENT( insertion, self );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_WRITE( &insertion );
    return __ACTIONCODE_BY_OPERATION( insertion, insert_int_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_int56__del_key64
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_int56__del_key64( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key ) {
  framehash_context_t delete_key64_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = {0},
    .control  = {0}
  };
  delete_key64_context.control.cache.enable_write = 1;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &delete_key64_context, ktype, &key ) == ktype ) {
    framehash_retcode_t deletion;
    BEGIN_FRAMEHASH_WRITE( self, &delete_key64_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, delete_key64_context.key.shortid ) {
        // delete by shortid
        deletion = _framehash_radix__del( &delete_key64_context );
        __MOD_DECREMENT( deletion, self );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_WRITE( &deletion );
    return __ACTIONCODE_BY_OPERATION( deletion, delete_key64_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_int56__has_key64
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_int56__has_key64( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key ) {
  framehash_context_t contains_key64_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = {0},
    .control  = {0}
  };
  contains_key64_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &contains_key64_context, ktype, &key ) == ktype ) {
    framehash_retcode_t retrieval;
    BEGIN_FRAMEHASH_READ( self, &contains_key64_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, contains_key64_context.key.shortid ) {
        retrieval = _framehash_radix__get( &contains_key64_context );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &retrieval );
    return __ACTIONCODE_BY_OPERATION( retrieval, contains_key64_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_int56__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_int56__get( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, int64_t * const retval ) {
  framehash_context_t query_int_context = {
    .vtype   = CELL_VALUE_TYPE_NULL,
    .value   = {0},
    .control = {0}
  };
  query_int_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_int_context, ktype, &key ) == ktype ) {
    framehash_retcode_t retrieval;
    BEGIN_FRAMEHASH_READ( self, &query_int_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_int_context.key.shortid ) {
        // get integer value by shortid
        retrieval = _framehash_radix__get( &query_int_context );
        if( retrieval.completed ) {
          *retval = query_int_context.value.int56;
        }
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &retrieval );
    return __ACTIONCODE_BY_OPERATION( retrieval, query_int_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_int56__inc
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_int56__inc( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t inc_value, int64_t * const retval ) {
  framehash_context_t upsert_int_context;
  framehash_context_t query_int_context = {
    .vtype   = CELL_VALUE_TYPE_INTEGER,
    .value   = {0},
    .control = {0}
  };
  query_int_context.control.cache.enable_read = self->_flag.cache.r_ena;
  query_int_context.control.cache.enable_write = self->_flag.cache.w_ena;


  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_int_context, ktype, &key ) == ktype ) {
    framehash_retcode_t upsertion;
    memcpy( &upsert_int_context, &query_int_context, sizeof( framehash_context_t ) );
    upsert_int_context.vtype = CELL_VALUE_TYPE_INTEGER;
    upsert_int_context.value.int56 = inc_value;
    __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_int_context.key.shortid ) {
      framehash_retcode_t retrieval = {0};
      BEGIN_FRAMEHASH_READ( self, &query_int_context ) {
        // get integer value by shortid and add with inc_value
        retrieval = _framehash_radix__get( &query_int_context );
        if( retrieval.completed ) {
          upsert_int_context.value.int56 += query_int_context.value.int56;
        }
        BEGIN_FRAMEHASH_WRITE( self, &upsert_int_context ) {
          // insert updated integer value by shortid 
          upsertion = _framehash_radix__set( &upsert_int_context );
          if( upsertion.completed ) {
            *retval = upsert_int_context.value.int56;
          }
          __MOD_INCREMENT( upsertion, self );
        } END_FRAMEHASH_WRITE( &upsertion );
      } END_FRAMEHASH_READ( &retrieval );
    } __RELEASE_RADIX;

    return __ACTIONCODE_BY_OPERATION( upsertion, upsert_int_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_int56.h"

DLL_HIDDEN test_descriptor_t _framehash_api_int56_tests[] = {
  { "api_int56",   __utest_framehash_api_int56 },
  {NULL}
};
#endif
