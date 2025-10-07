/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_info.c
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
 * _framehash_api_info__items
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_info__items( const framehash_t * const self ) {
  return self->_nobj;
}



/*******************************************************************//**
 * _framehash_api_info__masterpath
 *
 ***********************************************************************
 */
DLL_HIDDEN const CString_t * _framehash_api_info__masterpath( const framehash_t * const self ) {
  return self->_CSTR__masterpath;
}



/*******************************************************************//**
 * _framehash_api_info__hitrate
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_cache_hitrate_t _framehash_api_info__hitrate( framehash_t * const self ) {
  framehash_cache_hitrate_t hitrate = {0};
  framehash_context_t hitrate_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic
  };
  BEGIN_FRAMEHASH_READ( self, &hitrate_context ) {
    __SYNCHRONIZE_ALL_SUBTREES( self ) {
        _framehash_cache__hitrate( &hitrate_context, &hitrate );
    } __RELEASE_ALL_SUBTREES;
  } END_FRAMEHASH_READ( &flushing );

  for( int H=0; H<5; H++ ) {
    if( hitrate.count[ H ] > 0 ) {
      hitrate.rate[ H ] = (double)hitrate.accval[ H ] / hitrate.count[ H ];
    }
  }

  return hitrate;
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_info.h"

DLL_HIDDEN test_descriptor_t _framehash_api_info_tests[] = {
  { "api_info",   __utest_framehash_api_info },
  {NULL}
};
#endif
