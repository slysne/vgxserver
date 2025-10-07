/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    messages.c
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

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );


static cxlib_exc_context_t g_context = {0};



/*******************************************************************//**
 * Initialize messages
 *
 ***********************************************************************
 */
void COMLIB_InitializeMessages( void ) {
  if( !g_context.initflag ) {
    INIT_SPINNING_CRITICAL_SECTION( &g_context.lock.lock, 4000 );
    g_context.recursion = 0;
    g_context.ostream = stderr;
    g_context.msgtrace = NULL;
    g_context.mute = false;
    g_context.widx = 0;
    g_context.ridx = 0;
    memset( g_context.code, 0, sizeof(int) * CXLIB_MAX_ERRCNT );
    g_context.nWarning = 0;
    g_context.nError = 0;
    g_context.nCritical = 0;
    g_context.initflag = 1;
  }
  cxlib_set_exc_context( &g_context );
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT cxlib_exc_context_t * COMLIB_GetExceptionContext( void ) {
  if( !g_context.initflag ) {
    COMLIB_InitializeMessages();
  }
  return &g_context;
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT FILE * COMLIB_SetOutputStream( FILE *new_ostream ) {
  FILE *old_stream = NULL;
  SYNCHRONIZE_ON( g_context.lock ) {
    if( new_ostream != g_context.ostream ) {
      old_stream = g_context.ostream;           // save
      g_context.ostream = new_ostream;          // set new
      g_context.mute = false;
      comlib_set_pmesg_stream( new_ostream );   //
    }
  } RELEASE;
  return old_stream;                        // return old
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT FILE * COMLIB_GetOutputStream( void ) {
  if( g_context.ostream ) {
    return g_context.ostream;
  }
  else {
    return stderr;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_MuteOutputStream( void ) {
  if( g_context.mute == false ) {
    g_context.mute = true;
    comlib_set_pmesg_stream( comlib_null_sink );
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_UnmuteOutputStream( void ) {
  if( g_context.mute == true ) {
    g_context.mute = false;
    comlib_set_pmesg_stream( g_context.ostream );
  }
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT uint32_t COMLIB_GetMessage( void ) {
  uint32_t code = 0;
  SYNCHRONIZE_ON( g_context.lock ) {
    if( g_context.ridx != g_context.widx ) {
      code = g_context.code[ g_context.ridx ];
      g_context.code[ g_context.ridx++ ] = 0; // clear
      if( g_context.ridx >= COMLIB_MAX_ERRCNT ) {
        g_context.ridx = 0; // circular
      }
    }
  } RELEASE;
  return code;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_Fatal( const char *message ) {
  FATAL( 0x999, message );
}
