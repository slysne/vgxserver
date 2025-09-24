/*######################################################################
 *#
 *# __utest_vxapi_profile.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXAPI_PROFILE_H
#define __UTEST_VXAPI_PROFILE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxapi_profile ) {

  vgx_Graph_t *graph = NULL;

  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    TEST_ASSERTION( 1 == 1, "" );
  } END_TEST_SCENARIO



} END_UNIT_TEST




#endif
