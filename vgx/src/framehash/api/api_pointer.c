/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_pointer.c
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
 * _framehash_api_pointer__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_pointer__set( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const void * const ptr ) {
  framehash_context_t insert_ptr_context = {
    .vtype  = CELL_VALUE_TYPE_POINTER,
    .value  = { 
      .ptr56=(char*)ptr
    },
    .control = {0}
  };
  insert_ptr_context.control.cache.enable_write = 1;


  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &insert_ptr_context, ktype, &key ) == ktype ) {
    framehash_retcode_t insertion;
    BEGIN_FRAMEHASH_READ( self, &insert_ptr_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, insert_ptr_context.key.shortid ) {
        // insert pointer keyed by shortid 
        insertion = _framehash_radix__set( &insert_ptr_context );
        __MOD_INCREMENT( insertion, self );
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &insertion );
    return __ACTIONCODE_BY_OPERATION( insertion, insert_ptr_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * _framehash_api_pointer__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_pointer__get( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, void ** const retptr ) {
  framehash_context_t query_ptr_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = {0},
    .control  = {0}
  };
  query_ptr_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_ptr_context, ktype, &key ) == ktype ) {
    framehash_retcode_t retrieval;
    BEGIN_FRAMEHASH_READ( self, &query_ptr_context ) {
      __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_ptr_context.key.shortid ) {
        // get pointer by shortid
        retrieval = _framehash_radix__get( &query_ptr_context );
        if( retrieval.completed ) {
          *((char**)retptr) = query_ptr_context.value.ptr56;
        }
      } __RELEASE_RADIX;
    } END_FRAMEHASH_READ( &retrieval );
    return __ACTIONCODE_BY_OPERATION( retrieval, query_ptr_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}




/*******************************************************************//**
 * _framehash_api_pointer__inc
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_pointer__inc( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const uintptr_t inc_bytes, void ** const retptr ) {
  framehash_context_t upsert_ptr_context;
  framehash_context_t query_ptr_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = { 0 },
    .control  = { 0 }
  };
  upsert_ptr_context.control.cache.enable_read = self->_flag.cache.r_ena;
  upsert_ptr_context.control.cache.enable_write = self->_flag.cache.w_ena;
  
  if( ktype != CELL_KEY_TYPE_HASH128 && __framehash__set_context_key( self, &query_ptr_context, ktype, &key ) == ktype ) {
    framehash_retcode_t upsertion = {0};
    memcpy( &upsert_ptr_context, &query_ptr_context, sizeof( framehash_context_t ) );
    upsert_ptr_context.vtype = CELL_VALUE_TYPE_POINTER;
    __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_ptr_context.key.shortid ) {
      framehash_retcode_t retrieval = {0};
      BEGIN_FRAMEHASH_READ( self, &query_ptr_context ) {
        // get pointer by shortid and add with inc_value
        retrieval = _framehash_radix__get( &query_ptr_context );
        if( retrieval.completed ) {
          if( query_ptr_context.vtype == CELL_VALUE_TYPE_POINTER ) {
            upsert_ptr_context.value.ptr56 = (char*)(inc_bytes + (uintptr_t)query_ptr_context.value.ptr56);
            BEGIN_FRAMEHASH_WRITE( self, &upsert_ptr_context ) {
              // insert updated pointer by shortid 
              upsertion = _framehash_radix__set( &upsert_ptr_context );
              if( upsertion.completed ) {
                *((char**)retptr) = upsert_ptr_context.value.ptr56;
              }
              __MOD_INCREMENT( upsertion, self );
            } END_FRAMEHASH_WRITE( &upsertion );
          }
          else {
            // can't inc address of something that's not a pointer
            upsert_ptr_context.vtype = CELL_VALUE_TYPE_ERROR;
          }
        }
        else {
          // can't inc address when no pointer exists
          upsert_ptr_context.vtype = CELL_VALUE_TYPE_NULL;
        }
      } END_FRAMEHASH_READ( &retrieval );
    } __RELEASE_RADIX;

    return __ACTIONCODE_BY_OPERATION( upsertion, upsert_ptr_context.vtype );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_pointer.h"

DLL_HIDDEN test_descriptor_t _framehash_api_pointer_tests[] = {
  { "api_pointer",   __utest_framehash_api_pointer },
  {NULL}
};
#endif
