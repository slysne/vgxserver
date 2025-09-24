/*######################################################################
 *#
 *# hashing.c
 *#
 *#
 *######################################################################
 */


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

