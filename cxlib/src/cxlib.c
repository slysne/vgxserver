/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxlib.c
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

#include "cxlib.h"
#include "versiongen.h"

#ifndef CXLIB_VERSION
#define CXLIB_VERSION ?.?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "cxlib", VERSIONGEN_XSTR( CXLIB_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "cxlib", VERSIONGEN_XSTR( CXLIB_VERSION ) );

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __compile_time_asserts(void) {
  COMPILE_TIME_ASSERTION( sizeof(int) >= 4 );
  COMPILE_TIME_ASSERTION( sizeof(CS_LOCK) == EXPECTED_SIZEOF_CS_LOCK );  // 
  COMPILE_TIME_ASSERTION( sizeof(CS_COND) == EXPECTED_SIZEOF_CS_COND );  // 
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int sleep_nanoseconds( WAITABLE_TIMER Timer, int64_t ns ) {
#ifdef CXPLAT_WINDOWS_X64
  if( Timer ) {
    LARGE_INTEGER DueTime = {
      .QuadPart = -ns/100
    };
    if( SetWaitableTimer( Timer, &DueTime, 0, NULL, NULL, FALSE ) ) {
      WaitForSingleObject( Timer, INFINITE );
      ResetEvent( Timer );
      return 0;
    }
  }
  return -1;
#else
#define __i10e9 1000000000
  struct timespec requested, remaining;
  requested.tv_sec = ns / __i10e9;
  requested.tv_nsec = ns % __i10e9;
  return nanosleep( &requested, &remaining );
#endif
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void sleep_milliseconds( int32_t millisec ) {
#ifdef CXPLAT_WINDOWS_X64
  Sleep( millisec );
#else
#define __i10e3 1000
#define __i10e6 1000000
  struct timespec requested, remaining;
  requested.tv_sec = millisec / __i10e3;
  requested.tv_nsec = (millisec % __i10e3) * __i10e6;
  nanosleep( &requested, &remaining );
#endif
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * uint64_to_bin( char * buf, uint64_t x ) {
  char *p = buf;
  for( unsigned i=0; i<sizeof(uint64_t)*8; i++ ) {
    if( x & 0x8000000000000000 )
      *p++ = '1';
    else
      *p++ = '0';
    x <<= 1;
  }
  *p = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * uint32_to_bin( char * buf, uint32_t x ) {
  char *p = buf;
  for( unsigned i=0; i<sizeof(uint32_t)*8; i++ ) {
    if( x & 0x80000000 )
      *p++ = '1';
    else
      *p++ = '0';
    x <<= 1;
  }
  *p = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * uint16_to_bin( char * buf, uint16_t x ) {
  char *p = buf;
  for( unsigned i=0; i<sizeof(uint16_t)*8; i++ ) {
    if( x & 0x8000 )
      *p++ = '1';
    else
      *p++ = '0';
    x <<= 1;
  }
  *p = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * uint8_to_bin( char * buf, uint8_t x ) {
  char *p = buf;
  for( unsigned i=0; i<sizeof(uint8_t)*8; i++ ) {
    if( x & 0x80 )
      *p++ = '1';
    else
      *p++ = '0';
    x <<= 1;
  }
  *p = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int64_t qwstring_from_cstring( QWORD **dest, const char *src ) {
  QWORD *Q = NULL;
  size_t sz = strlen( src ) + 1; // include nulterm
  if( (Q = malloc( sizeof(QWORD) * sz )) != NULL ) {
    QWORD *pq = Q; // dest
    const char *pc = src; // src
    while( (*pq++ = *pc++) != 0 ); // copy everything including last nulterm
    *dest = Q;
    return sz;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int64_t cstring_from_qwstring( char **dest, const QWORD *src ) {
  char *c = NULL;
  const QWORD *pq = src;
  size_t sz = 1; // nulterm will be included in size so start at 1
  // count non-zero src elements
  while( *pq++ != 0 ) {
    ++sz;
  }
  if( (c = malloc( sz )) != NULL ) {
    char *pc = c; // dest
    pq = src; // src
    while( (*pc++ = (char)(*pq++ & 0xFF) ) != 0 ); // copy everything including last nulterm
    *dest = c;
    return sz;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
bool string_list_contains( const char *probe, const char *list[] ) {
  const char **entry = list;
  while( *entry != NULL ) {
    if( strncmp( probe, *entry++, 1023 ) == 0 ) {
      return true;
    }
  }
  return false;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
const char * cxlib_version( bool ext ) {
  if( ext ) {
    return g_version_info_ext;
  }
  else {
    return g_version_info;
  }
}
