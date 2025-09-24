/*######################################################################
 *#
 *# api_info.c
 *#
 *#
 *######################################################################
 */

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



