/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    debug.c
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

#include "_comlib.h"


#ifndef VERBOSITY
#define  VERBOSITY 4
#endif

static FILE *g_debug_stream = NULL;

static FILE g_dev_null;
DLL_EXPORT FILE *comlib_null_sink = &g_dev_null;


/**************************************************************************//**
 * comlib_set_pmesg_stream
 *
 ******************************************************************************
 */
void comlib_set_pmesg_stream( FILE *stream ) {
  g_debug_stream = stream;
}

#ifdef NDEBUG
/* Nothing. pmesg has been "defined away" in debug.h already. */
#else




/**************************************************************************//**
 * pmesg
 *
 ******************************************************************************
 */
DLL_EXPORT void pmesg( int level, const char *format, ... ) {
  va_list args;
  char tbuf[32] = {0};
  char *pmilli = tbuf+20; // (see why below)
  struct tm *now;

  if( g_debug_stream == &g_dev_null ) {
    return;
  }

  if( level < VERBOSITY ) { // level is really "importance".  Lower VERBOSITY means more messages.
    return;
  }
  
  if(!g_debug_stream) {
    g_debug_stream = stderr;
  }


  int64_t milliseconds_since_epoch = __MILLISECONDS_SINCE_1970();
  time_t seconds_since_epoch = milliseconds_since_epoch / 1000;
  if( (now = localtime( &seconds_since_epoch )) != NULL ) {
    strftime( tbuf, 31, "%Y-%m-%d %H:%M:%S.", now ); 
    //                 2016-07-07 12:34:56.---
    //                 0123456789..........^
    //                                     20
    sprintf( pmilli, "%03lld", milliseconds_since_epoch % 1000 ); // since system start (not epoch) but good enough for logging, we just want more resolution between seconds
  }

  va_start( args, format );
  fprintf( g_debug_stream, "[%s]    ", tbuf );
  vfprintf( g_debug_stream, format, args );
  va_end( args );
  fflush( g_debug_stream );
}
#endif /* NDEBUG */
