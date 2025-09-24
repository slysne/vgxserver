/*######################################################################
 *#
 *# __utest_vxgraph_tick.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXGRAPH_TICK_H
#define __UTEST_VXGRAPH_TICK_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxgraph_tick ) {

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
