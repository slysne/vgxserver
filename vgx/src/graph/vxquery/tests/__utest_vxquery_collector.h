/*######################################################################
 *#
 *# __utest_vxquery_collector.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXQUERY_COLLECTOR_H
#define __UTEST_VXQUERY_COLLECTOR_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxquery_collector ) {

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
