/*######################################################################
 *#
 *# __utest_vxapi_advanced.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXAPI_ADVANCED_H
#define __UTEST_VXAPI_ADVANCED_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxapi_advanced ) {

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
