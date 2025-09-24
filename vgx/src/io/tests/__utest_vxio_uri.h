/*######################################################################
 *#
 *# __utest_vxio_uri.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXIO_URI_H
#define __UTEST_VXIO_URI_H

#include "__vxtest_macro.h"


BEGIN_UNIT_TEST( __utest_vxio_uri ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );

  const char *basedir = GetCurrentTestDirectory();

  vgx_context_t VGX_CONTEXT = {0};
  strncpy( VGX_CONTEXT.sysroot, basedir, 254 );

  /*******************************************************************//**
   * CREATE AND DESTROY REGISTRY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and destroy graph registry" ) {
    TEST_ASSERTION( true, "" );
  } END_TEST_SCENARIO


  CStringDelete( CSTR__graph_path );

} END_UNIT_TEST




#endif
