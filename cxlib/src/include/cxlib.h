/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxlib.h
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

#ifndef CXLIB_INCLUDED_H
#define CXLIB_INCLUDED_H



#include "cxid.h"
#include "cxmem.h"
#include "cxfileio.h"
#include "cxsock.h"
#include "cxaptr.h"
#include "lz4.h"

#define COMPILE_TIME_ASSERTION( expr ) {typedef char _[(expr)?1:-1];}

int sleep_nanoseconds( WAITABLE_TIMER Timer, int64_t ns );
void sleep_milliseconds( int32_t millisec );

char * uint64_to_bin( char * buf, uint64_t x );
char * uint32_to_bin( char * buf, uint32_t x );
char * uint16_to_bin( char * buf, uint16_t x );
char * uint8_to_bin( char * buf, uint8_t x );
int64_t qwstring_from_cstring( QWORD **dest, const char *src );
int64_t cstring_from_qwstring( char **dest, const QWORD *src );
bool string_list_contains( const char *probe, const char *list[] );


const char * cxlib_version( bool ext );

#endif
