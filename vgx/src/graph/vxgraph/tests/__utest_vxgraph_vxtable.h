/*######################################################################
 *#
 *# __utest_vxgraph_vxtable.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXGRAPH_VXTABLE_H
#define __UTEST_VXGRAPH_VXTABLE_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxgraph_vxtable ) {

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
