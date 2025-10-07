/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxio_uri.h
 * Author:  Stian Lysne slysne.dev@gmail.com
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

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
