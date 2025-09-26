/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_real56.c
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

#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static double __context_value_as_double( framehash_context_t *context ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    return 0.0;
  case CELL_VALUE_TYPE_MEMBER:
    return 1.0;
  case CELL_VALUE_TYPE_BOOLEAN:
    return context->value.raw56 != 0 ? 1.0 : 0.0;
  case CELL_VALUE_TYPE_UNSIGNED:
    return (double)context->value.raw56;
  case CELL_VALUE_TYPE_INTEGER:
    return (double)context->value.int56;
  case CELL_VALUE_TYPE_REAL:
    return context->value.real56;
  default:
    return NAN; // incompatible
  }
}



/*******************************************************************//**
 * _framehash_api_real56__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_real56__set( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double value ) {
  framehash_context_t insert_real_context = {
    .vtype = CELL_VALUE_TYPE_REAL,
    .value = {
      .real56 = value
    },
    .control = {0}
  };
  insert_real_context.control.cache.enable_write = 1;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &insert_real_context, ktype, &key ) == ktype ) {
    framehash_retcode_t insertion;
    BEGIN_FRAMEHASH_WRITE( self, &insert_real_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, insert_real_context.key.shortid ) {
        // insert real value keyed by shortid 
        insertion = _framehash_radix__set( &insert_real_context );
        __MOD_INCREMENT( insertion, self );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_WRITE( &insertion );
    return __ACTIONCODE_BY_OPERATION( insertion, insert_real_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_real56__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_real56__get( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, double * const retval ) {
  framehash_context_t query_real_context = {
    .vtype = CELL_VALUE_TYPE_NULL,
    .value = {0},
    .control = {0}
  };
  query_real_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_real_context, ktype, &key ) == ktype ) {
    framehash_retcode_t retrieval;
    BEGIN_FRAMEHASH_READ( self, &query_real_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_real_context.key.shortid ) {
        // get real value by shortid
        retrieval = _framehash_radix__get( &query_real_context );
        if( retrieval.completed ) {
          // value type is real, use directly
          if( query_real_context.vtype == CELL_VALUE_TYPE_REAL ) {
            *retval = query_real_context.value.real56;
          }
          // value type is not a real value, force conversion
          else {
            *retval = __context_value_as_double( &query_real_context );
          }
        }
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &retrieval );
    return __ACTIONCODE_BY_OPERATION( retrieval, query_real_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_real56__inc
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_real56__inc( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double inc_value, double * const retval ) {
  framehash_context_t upsert_real_context;
  framehash_context_t query_real_context = {
    .vtype = CELL_VALUE_TYPE_REAL,
    .value = {
      .real56 = 0.0
    },
    .control = {0}
  };
  query_real_context.control.cache.enable_read = self->_flag.cache.r_ena;
  query_real_context.control.cache.enable_write = self->_flag.cache.w_ena;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_real_context, ktype, &key ) == ktype ) {
    framehash_retcode_t upsertion;
    memcpy( &upsert_real_context, &query_real_context, sizeof( framehash_context_t ) );
    upsert_real_context.vtype = CELL_VALUE_TYPE_REAL;
    upsert_real_context.value.real56 = inc_value;
    __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_real_context.key.shortid ) {
      framehash_retcode_t retrieval = {0};
      BEGIN_FRAMEHASH_READ( self, &query_real_context ) {
        // get real value by shortid and add with inc_value (if not found, we add delta to the default start value 0.0)
        retrieval = _framehash_radix__get( &query_real_context );
        if( retrieval.completed ) {
          double previous;
          // use real value directly
          if( query_real_context.vtype == CELL_VALUE_TYPE_REAL ) {
            previous = query_real_context.value.real56;
          }
          // convert non-real numeric to real before using
          else {
            previous = __context_value_as_double( &query_real_context );
          }
          // update the value
          upsert_real_context.value.real56 += previous;
        }
        BEGIN_FRAMEHASH_WRITE( self, &upsert_real_context ) {
          // insert updated real value by shortid 
          upsertion = _framehash_radix__set( &upsert_real_context );
          if( upsertion.completed ) {
            *retval = upsert_real_context.value.real56;
          }
          __MOD_INCREMENT( upsertion, self );
        } END_FRAMEHASH_WRITE( &upsertion );
      } END_FRAMEHASH_READ( &retrieval );
    } __RELEASE_RADIX;

    return __ACTIONCODE_BY_OPERATION( upsertion, upsert_real_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_real56.h"

DLL_HIDDEN test_descriptor_t _framehash_api_real56_tests[] = {
  { "api_real56",   __utest_framehash_api_real56 },
  {NULL}
};
#endif
