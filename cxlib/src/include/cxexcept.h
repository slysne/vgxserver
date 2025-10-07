/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxexcept.h
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

#ifndef CXLIB_CXEXCEPT_H
#define CXLIB_CXEXCEPT_H

#include "cxfileio.h"

/* SIMPLE EXCEPTION HANDLING MACROS */
//TODO: allow more granular exceptions
#ifdef INFO
#error "Conflict on macro INFO"
#endif
#ifdef VERBOSE
#error "Conflict on macro VERBOSE"
#endif
#ifdef DEBUG
#error "Conflict on macro DEBUG"
#endif
#ifdef WARN
#error "Conflict on macro WARN"
#endif
#ifdef REASON
#error "Conflict on macro REASON"
#endif
#ifdef CRITICAL
#error "Conflict on macro CRITICAL"
#endif
#ifdef FATAL
#error "Conflict on macro FATAL"
#endif
#ifdef XTHROW
#error "Conflict on macro XTHROW"
#endif
#ifdef THROW_ERROR
#error "Conflict on macro THROW_ERROR"
#endif
#ifdef THROW_CRITICAL
#error "Conflict on macro THROW_CRITICAL"
#endif
#ifdef THROW_FATAL
#error "Conflict on macro THROW_FATAL"
#endif
#ifdef TEST_FAILED
#error "Conflict on macro TEST_FAILED"
#endif
#ifdef XDO
#error "Conflict on macro XDO"
#endif
#ifdef XTRY
#error "Conflict on macro XTRY"
#endif
#ifdef XCATCH
#error "Conflict on macro XCATCH"
#endif
#ifdef XFINALLY
#error "Conflict on macro XFINALLY"
#endif



#include "cxlock.h"
#include "cxmath.h"

#define CXLIB_MSG_SIZE 200



/* MESSAGE MASKS                TTSMMCCC  */
typedef enum e_cxlib_exc_mask_t {
  CXLIB_EXC_TYPE_MASK       = 0x7F000000,   //  0111 1111  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_SUBTYPE_MASK    = 0x00F00000,   //  0000 0000  1111 0000  0000 0000  0000 0000
  CXLIB_EXC_MODULE_MASK     = 0x000FF000,   //  0000 0000  0000 1111  1111 0000  0000 0000
  CXLIB_EXC_CODE_MASK       = 0x00000FFF    //  0000 0000  0000 0000  0000 1111  1111 1111
} cxlib_exc_mask_t;

typedef enum e_cxlib_exc_shift_t {
  CXLIB_EXC_TYPE_SHIFT      = 24,           //
  CXLIB_EXC_SUBTYPE_SHIFT   = 20,           //
  CXLIB_EXC_MODULE_SHIFT    = 12,           //
  CXLIB_EXC_CODE_SHIFT      = 0             //
} cxlib_exc_shift_t;

/* MESSAGE CATEGORIES           XX______  */
typedef enum e_cxlib_exc_category_t {
  CXLIB_EXCEPTION           = 0x0C000000,   // 0000 1100  0000 0000  0000 0000  0000 0000
  CXLIB_INFORM              = 0x03000000,   // 0000 0011  0000 0000  0000 0000  0000 0000
  CXLIB_ERR_FATAL           = 0x7C000000,   // 0111 1100  0000 0000  0000 0000  0000 0000
  CXLIB_ERR_CRITICAL        = 0x6C000000,   // 0110 1100  0000 0000  0000 0000  0000 0000
  CXLIB_ERR_ERROR           = 0x4C000000,   // 0100 1100  0000 0000  0000 0000  0000 0000
  CXLIB_ERR_SILENT          = 0x3C000000,   // 0011 1100  0000 0000  0000 0000  0000 0000
  CXLIB_ERR_UNDEFINED       = 0x2C000000,   // 0010 1100  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_FAILED          = 0x73000000,   // 0111 0011  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_WARNING         = 0x43000000,   // 0100 0011  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_INFO            = 0x33000000,   // 0011 0011  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_VERBOSE         = 0x23000000,   // 0010 0011  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_DEBUG           = 0x13000000,   // 0001 0011  0000 0000  0000 0000  0000 0000
  CXLIB_EXC_CAT_LEVEL_MASK  = 0xF0000000    // 1111 0000  0000 0000  0000 0000  0000 0000
} cxlib_exc_category_t;

/* ERROR SUB CATEGORIES         __X_____  */
typedef enum e_cxlib_exc_subcategory_t {
  CXLIB_ERR_INITIALIZATION  = 0x00100000,   // 0000 0000  0001 0000  0000 0000  0000 0000
  CXLIB_ERR_CONFIG          = 0x00200000,   // 0000 0000  0010 0000  0000 0000  0000 0000
  CXLIB_ERR_FORMAT          = 0x00300000,   // 0000 0000  0011 0000  0000 0000  0000 0000
  CXLIB_ERR_LOOKUP          = 0x00400000,   // 0000 0000  0100 0000  0000 0000  0000 0000
  CXLIB_ERR_BUG             = 0x00500000,   // 0000 0000  0101 0000  0000 0000  0000 0000
  CXLIB_ERR_FILESYSTEM      = 0x00600000,   // 0000 0000  0110 0000  0000 0000  0000 0000
  CXLIB_ERR_CAPACITY        = 0x00700000,   // 0000 0000  0111 0000  0000 0000  0000 0000
  CXLIB_ERR_MEMORY          = 0x00800000,   // 0000 0000  1000 0000  0000 0000  0000 0000
  CXLIB_ERR_CORRUPTION      = 0x00900000,   // 0000 0000  1001 0000  0000 0000  0000 0000
  CXLIB_ERR_SEMAPHORE       = 0x00A00000,   // 0000 0000  1010 0000  0000 0000  0000 0000
  CXLIB_ERR_MUTEX           = 0x00B00000,   // 0000 0000  1011 0000  0000 0000  0000 0000
  CXLIB_ERR_GENERAL         = 0x00C00000,   // 0000 0000  1100 0000  0000 0000  0000 0000
  CXLIB_ERR_API             = 0x00D00000,   // 0000 0000  1101 0000  0000 0000  0000 0000
  CXLIB_ERR_ASSERTION       = 0x00E00000,   // 0000 0000  1110 0000  0000 0000  0000 0000
  CXLIB_ERR_IGNORE          = 0x00F00000    // 0000 0000  1111 0000  0000 0000  0000 0000
} cxlib_exc_subcategory_t;

/* ERROR MODULE                 ___XX___  */
typedef enum e_cxlib_message_module_t {
  CXLIB_MSG_MOD_FILEIO      = 0x00001000,   // 0000 0000  0000 0000  0001 0000  0000 0000
  CXLIB_MSG_MOD_APTR        = 0x00002000,   // 0000 0000  0000 0000  0010 0000  0000 0000
  CXLIB_MSG_MOD_GENERAL     = 0x00003000,   // 0000 0000  0000 0000  0011 0000  0000 0000
  CXLIB_MSG_MOD_UTEST       = 0x0000F000    // 0000 0000  0000 0000  1111 0000  0000 0000
} cxlib_message_module_t;

#define CXLIB_MAX_ERRCNT 100


typedef struct __s_msgtrace_node {
  size_t sz_msg;
  char *msg;
  struct __s_msgtrace_node *prev;
  struct __s_msgtrace_node *next;
} __msgtrace_node;

typedef struct __s_msgtrace {
  int sz;
  __msgtrace_node *top;
  __msgtrace_node *bottom;
  __msgtrace_node *cursor;
} __msgtrace;



#ifdef CXPLAT_WINDOWS_X64
typedef struct __declspec(align(64)) s_cxlib_exc_context_t { 
#else
typedef struct __attribute__ ((aligned(64))) s_cxlib_exc_context_t { 
#endif
  // Q1
  CS_LOCK lock;

  // Q2.1.1
  int recursion;
  // Q2.1.2
  bool mute;
  // Q2.2
  FILE *ostream;
  // Q2.3
  __msgtrace *msgtrace;
  // Q2.4.1
  int initflag;
  // Q2.4.2
  int widx;
  // Q2.5.1
  int ridx;
  // Q2.5.1
  int __rsv;
  // Q2.6
  int64_t nWarning;
  // Q2.7
  int64_t nError;
  // Q2.8
  int64_t nCritical;
  // Q3 - Q8, Q9.1.1-Q9.2.2
  int code[ CXLIB_MAX_ERRCNT ];
  // Q9.3 - Q9.8
  QWORD __rsv9[6];
} cxlib_exc_context_t;



#ifdef __cplusplus
extern "C" {
#endif


int cxlib_trace_reset( void );
void cxlib_trace_disable( void );
int cxlib_trace_add( const char *msg );
void cxlib_trace_delete_msglist( char ***list );
char ** cxlib_trace_get_msglist( void );

int64_t cxlib_thread_init( void );
void cxlib_thread_clear( void );
void cxlib_set_exc_context( cxlib_exc_context_t *context );
int cxlib_exc_type( int msgcode );
int cxlib_exc_subtype( int msgcode );
int cxlib_exc_module( int msgcode );
int cxlib_exc_code( int msgcode );
int cxlib_format_code( int msgcode, char *buf, size_t n );
int cxlib_exc( int code, const char *msg, ... );
void cxlib_exception_counters( int64_t *nWarning, int64_t *nError, int64_t *nCritical );
void cxlib_exception_counters_reset( void );
void cxlib_ostream_lock( void );
void cxlib_ostream_release( void );
int cxlib_ostream( const char *msg, ... );
int cxlib_msg_set( const char *filename, int line, const char *msg, ... );
const char * cxlib_msg_get( const char **filename, int *line );
void cxlib_print_backtrace( int nframes );

typedef struct s_cxlib_symbol_name {
  char value[256];
  int len;
} cxlib_symbol_name;

cxlib_symbol_name cxlib_get_symbol_name( const uintptr_t address );

#ifdef __cplusplus
}
#endif



#define GENERAL_EXCEPTION_MODULE( N )   (CXLIB_MSG_MOD_GENERAL | ((N << CXLIB_EXC_MODULE_SHIFT) & CXLIB_EXC_MODULE_MASK))
#define SET_EXCEPTION_MODULE( mod )     static int g_exception_module = mod

#ifndef NDEBUG
#define DEBUG( MessageCode, MessageString, ... )    cxlib_exc( CXLIB_EXC_DEBUG    | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#else
#define DEBUG( MessageCode, MessageString, ... )    ((void)0)
#endif
#if VERBOSITY <= 2
#define VERBOSE( MessageCode, MessageString, ... )  cxlib_exc( CXLIB_EXC_VERBOSE  | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#define HASVERBOSE
#else
#define VERBOSE( MessageCode, MessageString, ... )  ((void)0)
#endif
#define INFO( MessageCode, MessageString, ... )     cxlib_exc( CXLIB_EXC_INFO     | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#define WARN( MessageCode, MessageString, ...)      cxlib_exc( CXLIB_EXC_WARNING  | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#define REASON( MessageCode, MessageString, ...)    cxlib_exc( CXLIB_ERR_ERROR    | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#define CRITICAL( MessageCode, MessageString, ...)  cxlib_exc( CXLIB_ERR_CRITICAL | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )
#define FATAL( MessageCode, MessageString, ...)     cxlib_exc( CXLIB_ERR_FATAL    | g_exception_module | ((MessageCode)&CXLIB_EXC_CODE_MASK), MessageString, ##__VA_ARGS__ )

#define PRINT_OSTREAM cxlib_ostream

#define CXLIB_OSTREAM( Format, ... ) cxlib_ostream( Format "\n", ##__VA_ARGS__ )




#define CXLIB_OSTREAM_LINE CXLIB_OSTREAM( "______________________________________________________________________" )

#define BEGIN_CXLIB_OSTREAM \
do {                        \
  cxlib_ostream_lock();     \
  do /* {
    code
  } */

#define END_CXLIB_OSTREAM   \
  WHILE_ZERO;               \
  cxlib_ostream_release();  \
} WHILE_ZERO



#define BEGIN_CXLIB_OBJ_DUMP( TYPE, Instance )  \
  BEGIN_CXLIB_OSTREAM {                         \
    CXLIB_OSTREAM_LINE;                         \
    CXLIB_OSTREAM( #TYPE " @ %llp (%llu bytes)", (Instance), sizeof(TYPE) ); \
    CXLIB_OSTREAM_LINE;                         \
    if( !(Instance) ) {                         \
      CXLIB_OSTREAM( "NULL" );                  \
    }                                           \
    else /* {
      code
    } */

#define END_CXLIB_OBJ_DUMP  \
    CXLIB_OSTREAM_LINE;     \
  } END_CXLIB_OSTREAM






/* Emulate simple "exceptions" within functions to make error handling more readable and consistent.
   Exceptions are limited to function scope, i.e. they are not propagated up the call hierarchy so
   functions themselves will not raise exceptions. Also, there can only be one XTRY-XCATCH-XFINALLY
   block per function. 
*/

static int __throw_trap( int errcode ) {
  return errcode;
}


// XTHROW and "exception" in the form of an enumerated error code
#define XTHROW( _err_ )             \
do {                                \
  __cxexcept_throwerr__ = __throw_trap( _err_ );  \
  goto __cxexcept_catch__;          \
} WHILE_ZERO



// XBREAK to the XFINALLY block without throwing exception
#define XBREAK goto __cxexcept_end__


#define __cast_int_CXLIB_ERR( Error, ErrorType, ErrorCode ) ((unsigned)(Error) | (unsigned)(ErrorType) | (unsigned)(ErrorCode))


// Convenience THROW_X of various severity
#define THROW_SILENT( ErrorType, ErrorCode )   XTHROW( __cast_int_CXLIB_ERR(CXLIB_ERR_SILENT, ErrorType, ErrorCode) )
#define THROW_ERROR( ErrorType, ErrorCode )    XTHROW( __cast_int_CXLIB_ERR(CXLIB_ERR_ERROR, ErrorType, ErrorCode) )
#define THROW_CRITICAL( ErrorType, ErrorCode ) XTHROW( __cast_int_CXLIB_ERR(CXLIB_ERR_CRITICAL, ErrorType, ErrorCode) )
#define THROW_FATAL( ErrorType, ErrorCode )    XTHROW( __cast_int_CXLIB_ERR(CXLIB_ERR_FATAL, ErrorType, ErrorCode) )

// Convenience THROW MESSAGE
#define THROW_ERROR_MESSAGE( ErrorType, ErrorCode, ErrorMessage, ... )  \
  do {                                                                  \
    cxlib_msg_set( __FILE__, __LINE__, ErrorMessage, ##__VA_ARGS__ );   \
    THROW_ERROR( ErrorType, ErrorCode );                                \
  } WHILE_ZERO

#define THROW_CRITICAL_MESSAGE( ErrorType, ErrorCode, ErrorMessage, ... ) \
  do {                                                                    \
    cxlib_msg_set( __FILE__, __LINE__, ErrorMessage, ##__VA_ARGS__ );     \
    THROW_CRITICAL( ErrorType, ErrorCode );                               \
  } WHILE_ZERO

#define THROW_FATAL_MESSAGE( ErrorType, ErrorCode, ErrorMessage, ... )  \
  do {                                                                  \
    cxlib_msg_set( __FILE__, __LINE__, ErrorMessage, ##__VA_ARGS__ );   \
    THROW_FATAL( ErrorType, ErrorCode );                                \
  } WHILE_ZERO


#define ASSERTION( Expression, Message, ... )                                     \
if( !(Expression) ) {                                                             \
  do {                                                                            \
    uint32_t code = CXLIB_EXC_FAILED | g_exception_module | CXLIB_ERR_ASSERTION;  \
    cxlib_exc( code, Message, ##__VA_ARGS__ );                                    \
    cxlib_exc( code, "FILE: %s LINE: %d", __FILE__, __LINE__ );                   \
    XTHROW( code );                                                                \
  } WHILE_ZERO;                                                                   \
}



/* XCATCH { ... your error handling code ... } */
static int __catch_logic( int err, int exc_mod, const char *funcname ) {
  int code = CXLIB_EXCEPTION | exc_mod | err;
  if( (err & CXLIB_EXC_TYPE_MASK) != CXLIB_ERR_SILENT ) {
    const char *fpath = NULL;
    char *fname = NULL;
    int line = 0;
    const char *msg;
    if( (msg = cxlib_msg_get( &fpath, &line )) != NULL ) {
      if( fpath ) {
        split_path( fpath, NULL, &fname );
      }
      if( fname && strlen(fname) ) {
        cxlib_exc( code, "Exception %03X in %s:%s():%d [%s]", cxlib_exc_code(code), fname, funcname, line, msg );
      }
      else {
        cxlib_exc( code, "Exception %03X in %s() %s", cxlib_exc_code(code), funcname, msg );
      }
      if( fname ) {
        free( fname );
      }
    }
    else {
      cxlib_exc( code, "Exception %03X in %s() [unknown]", cxlib_exc_code(code), funcname );
    }
  } 
  return code;
}

/*  XDO-XFINALLY or XTRY-XCATCH-XFINALLY, one use per function.
    WARNING! Note the non-standard behavior that XFINALLY is not reached when returning from within a XDO, XTRY or XCATCH.
*/

/* XDO { ... your code ... } */
#define XDO   \
  do {        \
    {         \
      {       \
        do /* { ... } */


/* XTRY { ... your code ... } */
#define XTRY  \
  do {        \
    int __cxexcept_throwerr__ = 0;
    /* { ... } */

/* XCATCH(e) { ... your code ... } */
#define XCATCH(__cxexcept_errcode__) \
__cxexcept_catch__:                 \
    if( __cxexcept_throwerr__ ) {   \
      int __cxexcept_errcode__;     \
      if( (__cxexcept_errcode__ = __catch_logic( __cxexcept_throwerr__, g_exception_module, __FUNCTION__ )) != 0 ) { \
        do /* { ... } */

/* XFINALLY { ... your cleanup code ... } */
#define XFINALLY    \
        WHILE_ZERO; \
      }             \
      goto __cxexcept_end__; \
    }               \
  } WHILE_ZERO;     \
__cxexcept_end__:   \
  /* { ... } */





#endif
