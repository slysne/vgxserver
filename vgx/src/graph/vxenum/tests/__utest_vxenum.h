/*######################################################################
 *#
 *# __utest_vxenum.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXENUM_H
#define __UTEST_VXENUM_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxenum ) {

  NEXT_TEST_SCENARIO( true, "Test something" ) {
    TEST_ASSERTION( 1 == 1, "" );
  } END_TEST_SCENARIO


} END_UNIT_TEST


#endif
