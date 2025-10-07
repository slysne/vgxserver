/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    comlib.c
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

#include "_comlib.h"
#include "demo_class.h"
#include "versiongen.h"

#ifndef COMLIB_VERSION
#define COMLIB_VERSION ?.?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "comlib", VERSIONGEN_XSTR( COMLIB_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "comlib", VERSIONGEN_XSTR( COMLIB_VERSION ) );


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );

static int g_comlib_initialized = 0;




/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT const char * comlib_version( bool ext ) {
  if( ext ) {
    return g_version_info_ext;
  }
  else {
    return g_version_info;
  }
}





/*******************************************************************//**
 * Initialize COM library
 ***********************************************************************
 */
DLL_EXPORT int comlib_INIT( void ) {

  COMLIB_InitializeMessages();

  if( !g_comlib_initialized ) {
    CStringQueue_RegisterClass();
    CStringBuffer_RegisterClass();
    COMLIB_InitializeObjectModel();

    CByteQueue_RegisterClass();
    CWordQueue_RegisterClass();
    CDwordQueue_RegisterClass();
    CQwordQueue_RegisterClass();
    Cm128iQueue_RegisterClass();
    Cm256iQueue_RegisterClass();
    Cm512iQueue_RegisterClass();
    CtptrQueue_RegisterClass();
    Cx2tptrQueue_RegisterClass();
    CaptrQueue_RegisterClass();

    CByteBuffer_RegisterClass();
    CWordBuffer_RegisterClass();
    CDwordBuffer_RegisterClass();
    CQwordBuffer_RegisterClass();
    Cm128iBuffer_RegisterClass();
    Cm256iBuffer_RegisterClass();
    Cm512iBuffer_RegisterClass();
    CtptrBuffer_RegisterClass();
    Cx2tptrBuffer_RegisterClass();
    CaptrBuffer_RegisterClass();

    CByteHeap_RegisterClass();
    CWordHeap_RegisterClass();
    CDwordHeap_RegisterClass();
    CQwordHeap_RegisterClass();
    Cm128iHeap_RegisterClass();
    Cm256iHeap_RegisterClass();
    Cm512iHeap_RegisterClass();
    CtptrHeap_RegisterClass();
    Cx2tptrHeap_RegisterClass();
    CaptrHeap_RegisterClass();

    CByteList_RegisterClass();
    CWordList_RegisterClass();
    CDwordList_RegisterClass();
    CQwordList_RegisterClass();
    Cm128iList_RegisterClass();
    Cm256iList_RegisterClass();
    Cm512iList_RegisterClass();
    CtptrList_RegisterClass();
    Cx2tptrList_RegisterClass();
    CaptrList_RegisterClass();

    CTokenizer_RegisterClass();
    CString_RegisterClass();

    DemoClass_RegisterClass();
    DemoClass_DemonstrateUsage();

    // Initial spin to prime spinner with an appropriate spin factor
    COMLIB__Spin( 1 );

    g_comlib_initialized = 1;
    return 1;
  }

  return 0;
}



/*******************************************************************//**
 * Tear down COM library (usually on process exit)
 *
 ***********************************************************************
 */
DLL_EXPORT int comlib_DESTROY( void ) {
  if( g_comlib_initialized ) {
    DemoClass_UnregisterClass();

    CString_UnregisterClass();
    CTokenizer_UnregisterClass();

    CaptrList_UnregisterClass();
    Cx2tptrList_UnregisterClass();
    CtptrList_UnregisterClass();
    Cm512iList_UnregisterClass();
    Cm256iList_UnregisterClass();
    Cm128iList_UnregisterClass();
    CQwordList_UnregisterClass();
    CDwordList_UnregisterClass();
    CWordList_UnregisterClass();
    CByteList_UnregisterClass();

    CaptrHeap_UnregisterClass();
    Cx2tptrHeap_UnregisterClass();
    CtptrHeap_UnregisterClass();
    Cm512iHeap_UnregisterClass();
    Cm256iHeap_UnregisterClass();
    Cm128iHeap_UnregisterClass();
    CQwordHeap_UnregisterClass();
    CDwordHeap_UnregisterClass();
    CWordHeap_UnregisterClass();
    CByteHeap_UnregisterClass();

    CaptrQueue_UnregisterClass();
    Cx2tptrQueue_UnregisterClass();
    CtptrQueue_UnregisterClass();
    Cm512iQueue_UnregisterClass();
    Cm256iQueue_UnregisterClass();
    Cm128iQueue_UnregisterClass();
    CQwordQueue_UnregisterClass();
    CDwordQueue_UnregisterClass();
    CWordQueue_UnregisterClass();
    CByteQueue_UnregisterClass();

    COMLIB_DestroyObjectModel();
    CStringQueue_UnregisterClass();

    g_comlib_initialized = 0;
  }
  return 0;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_comlib.h"

test_descriptor_t _comlib_tests[] = {
  { "COMLIB Basic Tests",               __utest_comlib_basic },
  { "COMLIB Annotated Pointer Tests",   __utest_comlib_aptr },
  {NULL}
};
#endif
