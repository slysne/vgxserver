/*######################################################################
 *#
 *# __utest_vxsim.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXSIM_H
#define __UTEST_VXSIM_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxsim ) {

  NEXT_TEST_SCENARIO( true, "Test something" ) {
    TEST_ASSERTION( 1 == 1, "" );
  } END_TEST_SCENARIO

} END_UNIT_TEST


#endif
