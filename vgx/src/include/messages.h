/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    messages.h
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

#ifndef COMLIB_MESSAGES_H
#define COMLIB_MESSAGES_H


#include "moduledefs.h"
#include "cxlib.h"


#define COMLIB_MAX_ERRCNT 100


#ifdef __cplusplus
extern "C" {
#endif

void COMLIB_InitializeMessages( void );

DLL_COMLIB_PUBLIC extern cxlib_exc_context_t * COMLIB_GetExceptionContext( void );
DLL_COMLIB_PUBLIC extern FILE * COMLIB_SetOutputStream( FILE *ostream );
DLL_COMLIB_PUBLIC extern FILE * COMLIB_GetOutputStream( void );
DLL_COMLIB_PUBLIC extern void COMLIB_MuteOutputStream( void );
DLL_COMLIB_PUBLIC extern void COMLIB_UnmuteOutputStream( void );
DLL_COMLIB_PUBLIC extern uint32_t COMLIB_GetMessage( void );
DLL_COMLIB_PUBLIC extern void COMLIB_Fatal( const char *message );

#ifdef __cplusplus
}
#endif

#endif
