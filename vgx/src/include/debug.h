/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    debug.h
 * Author:  Stian Lysne <...>
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

#ifndef COMLIB_DEBUG_H
#define COMLIB_DEBUG_H
#include "cxlib.h"


void comlib_set_pmesg_stream( FILE *stream );

DLL_COMLIB_PUBLIC extern FILE *comlib_null_sink;

#ifdef NDEBUG
/* macro with a variable number of arguments. 
   We use this extension here to preprocess pmesg away. */
#define pmesg(level, format, ...) ((void)0)
#else

DLL_COMLIB_PUBLIC extern void pmesg(int level, const char *format, ...);
/* print a message, if it is considered significant enough.
      Adapted from [K&R2], p. 174 */
#endif

#endif /* DEBUG_H */
