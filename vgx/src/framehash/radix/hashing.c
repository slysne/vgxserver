/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    hashing.c
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
 * framehash_hashing__short_hashkey
 * Return a 64-bit hash value for a 64-bit integer
 ***********************************************************************
 */
DLL_EXPORT shortid_t framehash_hashing__short_hashkey( uint64_t n ) {
  return ihash64( n );
}



/*******************************************************************//**
 * framehash_hashing__tiny_hashkey
 * Return a 32-bit hash value for a 32-bit integer
 ***********************************************************************
 */
#define __OFFSET_31 1500000001 /*  */
DLL_EXPORT uint32_t framehash_hashing__tiny_hashkey( uint32_t n ) {
  uint32_t m = n + __OFFSET_31;
  return (uint32_t)hash32( (unsigned char*)&m, 4 );
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_hashing.h"

DLL_HIDDEN test_descriptor_t _framehash_hashing_tests[] = {
  { "_framehash_hashing__get_hashbits",    __utest_framehash_hashing__get_hashbits },
  { "framehash_hashing__short_hashkey",    __utest_framehash_hashing__short_hashkey },
  { "framehash_hashing__tiny_hashkey",     __utest_framehash_hashing__tiny_hashkey },
  {NULL}
};
#endif
