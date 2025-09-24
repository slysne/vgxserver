/*
 * cxexcept.c
 *
 *
*/

#include "cxexcept.h"
#include "cxlock.h"

#if defined CXPLAT_WINDOWS_X64
#elif defined CXPLAT_LINUX_ANY
#include <sys/syscall.h>
#include <dlfcn.h>
#elif defined CXPLAT_MAC_ARM64
#include <pthread.h>
#include <dlfcn.h>
#endif


cxlib_exc_context_t *g_context = NULL;


#define SZ_MSGBUF 1024

static __THREAD char gt_prevmsg[ SZ_MSGBUF ] = {0};
static __THREAD char gt_thismsg[ SZ_MSGBUF ] = {0};
static __THREAD char gt_prevfname[ MAX_PATH+1 ] = {0};
static __THREAD char gt_thisfname[ MAX_PATH+1 ] = {0};
static __THREAD int gt_prevline = 0;
static __THREAD int gt_thisline = 0;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static uint32_t __get_threadid( void ) {
  static __THREAD uint32_t thread_id = 0;
  if( !thread_id ) {
#if defined CXPLAT_WINDOWS_X64
    thread_id = GetCurrentThreadId();
#elif defined CXPLAT_LINUX_ANY
    thread_id = (uint32_t)syscall(SYS_gettid);
#elif defined CXPLAT_MAC_ARM64
    uint64_t tid;
    pthread_threadid_np(NULL, &tid);
    thread_id = (uint32_t)tid;
#else
#error "Unsupported platform"
#endif
  }
  return thread_id;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __delete_msgtrace_node( __msgtrace_node **node ) {
  if( node && *node ) {
    if( (*node)->msg ) {
      free( (*node)->msg );
    }
    if( (*node)->prev ) {
      (*node)->prev->next = (*node)->next;
    }
    if( (*node)->next ) {
      (*node)->next->prev = (*node)->prev;
    }
    free( *node );
    *node = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static __msgtrace_node * __new_msgtrace_node( const char *msg, __msgtrace_node *prev ) {
  __msgtrace_node *node = calloc( 1, sizeof( __msgtrace_node ) );
  if( node ) {
    node->sz_msg = strnlen( msg, SZ_MSGBUF );
    if( (node->msg = calloc( node->sz_msg+1, 1 )) != NULL ) {
      strncpy( node->msg, msg, node->sz_msg );
      // If a previous node is provided, insert the new node after the previous node
      if( prev ) {
        if( (node->next = prev->next) != NULL ) {
          node->next->prev = node;
        }
        prev->next = node;
        node->prev = prev;
      }
    }
    else {
      free( node );
      node = NULL;
    }
  }
  return node;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __push_msgtrace( __msgtrace *trace, const char *msg ) {
  if( msg ) {
    __msgtrace_node *node = __new_msgtrace_node( msg, trace->cursor );
    if( node == NULL ) {
      return -1;
    }
    trace->cursor = node;
    // This is the first node, update top
    if( node->prev == NULL ) {
      trace->top = node;
    }
    // This is the last node, update bottom
    if( node->next == NULL ) {
      trace->bottom = node;
    }
    trace->sz++;
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __delete_msgtrace( __msgtrace **trace ) {
  if( trace && *trace ) {
    __msgtrace_node *next = (*trace)->top;
    __msgtrace_node *node;
    while( (node=next) != NULL ) {
      next = node->next;
      __delete_msgtrace_node( &node );
    }
    free( *trace );
    *trace = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static __msgtrace * __new_msgtrace( void ) {
  __msgtrace *trace = (__msgtrace*)calloc( 1, sizeof( __msgtrace ) );
  return trace;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_trace_reset( void ) {
  int ret = 0;

  CS_LOCK *pcs = &g_context->lock;


  SYNCHRONIZE_ON( g_context->lock ) {
    __delete_msgtrace( &g_context->msgtrace );
    if( (g_context->msgtrace = __new_msgtrace()) == NULL ) {
      ret = -1;
    }
  } RELEASE;


  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_trace_disable( void ) {
  SYNCHRONIZE_ON( g_context->lock ) {
    __delete_msgtrace( &g_context->msgtrace );
  } RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_trace_add( const char *msg ) {
  int ret = -1;
  SYNCHRONIZE_ON( g_context->lock ) {
    if( g_context->msgtrace ) {
      ret = __push_msgtrace( g_context->msgtrace, msg );
    }
  } RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_trace_delete_msglist( char ***list ) {
  if( list && *list ) {
    char **cursor = *list;
    char *item;
    while( (item=*cursor++) != NULL ) {
      free( item );
    }
    free( *list );
    *list = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char ** cxlib_trace_get_msglist( void ) {
  char **list = NULL;
  SYNCHRONIZE_ON( g_context->lock ) {
    __msgtrace *trace = g_context->msgtrace;
    if( trace ) {
      if( (list = calloc( (size_t)trace->sz+1, sizeof( char* ) )) != NULL ) {
        char **cursor = list;
        __msgtrace_node *node = trace->top;
        while( node ) {
          if( (*cursor = calloc( node->sz_msg+1, 1 )) != NULL ) {
            memcpy( *cursor++, node->msg, node->sz_msg );
          }
          node = node->next;
        }
      }
    }
  } RELEASE;
  return list;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int is_message_fatal( int msgcode ) {
  return cxlib_exc_type( msgcode ) == CXLIB_ERR_FATAL;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int64_t cxlib_thread_init( void ) {
  int64_t t = 0;
  while( (t = __GET_CURRENT_NANOSECOND_TICK()) <= 0 );
  return t;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_thread_clear( void ) {
  gt_prevmsg[0] = '\0';
  gt_thismsg[0] = '\0';
  gt_prevfname[0] = '\0';
  gt_thisfname[0] = '\0';
  gt_prevline = 0;
  gt_thisline = 0;
  return;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_set_exc_context( cxlib_exc_context_t *context ) {
  cxlib_exc_context_t *prev = g_context;
  g_context = context;
  if( g_context != prev ) {
    SYNCHRONIZE_ON( context->lock ) {
    } RELEASE;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_exc_type( int msgcode ) {
  return msgcode & CXLIB_EXC_TYPE_MASK;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_exc_subtype( int msgcode ) {
  return msgcode & CXLIB_EXC_SUBTYPE_MASK;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_exc_module( int msgcode ) {
  return msgcode & CXLIB_EXC_MODULE_MASK;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_exc_code( int msgcode ) {
  return msgcode & CXLIB_EXC_CODE_MASK;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_format_code( int msgcode, char *buf, size_t n ) {
  int msg_typ = (CXLIB_EXC_TYPE_MASK    & msgcode) >> CXLIB_EXC_TYPE_SHIFT;
  int msg_sub = (CXLIB_EXC_SUBTYPE_MASK & msgcode) >> CXLIB_EXC_SUBTYPE_SHIFT;
  int msg_mod = (CXLIB_EXC_MODULE_MASK  & msgcode) >> CXLIB_EXC_MODULE_SHIFT;
  int msg_cod = (CXLIB_EXC_CODE_MASK    & msgcode) >> CXLIB_EXC_CODE_SHIFT;
  return snprintf( buf, n, "%02X-%01X-%02X-%03X", msg_typ, msg_sub, msg_mod, msg_cod );
}




static void __fprintf_critical( FILE *ostream, const char *tbuf, int msg_typ, int msg_sub, int msg_mod, int msg_cod, uint32_t tid, const char *msg  ) {
  char above[128];
  char below[128];
  char head[64];
  snprintf( head, 63, "[%s]  %-8s  [%02X-%01X-%02X-%03X-%05X]", tbuf, "CRITICAL", msg_typ, msg_sub, msg_mod, msg_cod, tid );
  size_t sz = strnlen( msg, 127 );
  char *a = above;
  char *b = below;
  const char *end_a = above + sz;
  const char *end_b = below + sz;
  while( a < end_a ) {
    *a++ = '!';
  }
  *a = '\0';
  while( b < end_b ) {
    *b++ = '^';
  }
  *b = '\0';
  fprintf( ostream, "%s: %s\n", head, above );
  fprintf( ostream, "%s: %s\n", head, msg );
  fprintf( ostream, "%s: %s\n", head, below );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_exc( int code, const char *msg, ... ) {
  static __THREAD char t_msgbuf[ SZ_MSGBUF ] = { '\0' };
  static int64_t last_digest = 0;
  static int64_t msg_repeat = 1;
  int msg_typ;
  int msg_sub;
  int msg_mod;
  int msg_cod;
  int msgtype;
  int die=0;
  static char unknown[] = "(UNKNOWN)";
  char tbuf[32] = {0}; // (a little more than we need for the 23-char "2016-07-07 12:34:56.789" )
  char *pmilli = tbuf+20; // (see why below)
  struct tm *now;
  va_list args;
  FILE *ostream;
  
  if( g_context != NULL ) {

    if( g_context->mute ) {
      return 0;
    }

    ostream = g_context->ostream ? g_context->ostream : stderr;

    msgtype = cxlib_exc_type( code );

#ifndef VERBOSITY
#define VERBOSITY 3
#endif
#if VERBOSITY == 1
    const int VLEVEL = CXLIB_EXC_DEBUG;
#elif VERBOSITY == 2
    const int VLEVEL = CXLIB_EXC_VERBOSE;
#elif VERBOSITY == 3
    const int VLEVEL = CXLIB_EXC_INFO;
#else
    const int VLEVEL = CXLIB_EXC_WARNING;
#endif

#ifdef NDEBUG
    if( (msgtype & CXLIB_EXC_CAT_LEVEL_MASK) < (VLEVEL & CXLIB_EXC_CAT_LEVEL_MASK) ) {
      return 0;
    }
#endif

    if(!msg) {
      msg = unknown;
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

    SYNCHRONIZE_ON( g_context->lock ) {
      va_start( args, msg );
      vsnprintf( t_msgbuf, SZ_MSGBUF-1, msg, args );
      va_end( args );
      g_context->code[ g_context->widx++ ] = code;
      if( g_context->widx >= CXLIB_MAX_ERRCNT ) {
        g_context->widx = 0; // circular
      }

      msg_typ = (CXLIB_EXC_TYPE_MASK    & code) >> CXLIB_EXC_TYPE_SHIFT;
      msg_sub = (CXLIB_EXC_SUBTYPE_MASK & code) >> CXLIB_EXC_SUBTYPE_SHIFT;
      msg_mod = (CXLIB_EXC_MODULE_MASK  & code) >> CXLIB_EXC_MODULE_SHIFT;
      msg_cod = (CXLIB_EXC_CODE_MASK    & code) >> CXLIB_EXC_CODE_SHIFT;

#define __FPRINTF( MessageType ) \
  fprintf( ostream, "[%s]  %-8s  [%02X-%01X-%02X-%03X-%05X]: %s\n", tbuf, MessageType, msg_typ, msg_sub, msg_mod, msg_cod, __get_threadid(), t_msgbuf )

#define MAX_MSG_REP 500

#define __LIMITED_FPRINTF( MessageType ) \
do {                                    \
  int64_t d = strhash64( (const unsigned char*)t_msgbuf );    \
  if( d == last_digest ) {              \
    ++msg_repeat;                       \
  }                                     \
  else {                                \
    if( msg_repeat > MAX_MSG_REP ) {    \
      char *tmp = calloc( SZ_MSGBUF, 1 ); \
      if( tmp ) {                       \
        memcpy( tmp, t_msgbuf, SZ_MSGBUF ); \
        snprintf( t_msgbuf, SZ_MSGBUF-1, "... previous message repeated %lld times", msg_repeat ); \
        __FPRINTF( MessageType );       \
        memcpy( t_msgbuf, tmp, SZ_MSGBUF ); \
        free( tmp );                    \
      }                                 \
    }                                   \
    msg_repeat = 1;                     \
    last_digest = d;                    \
  }                                     \
  if( msg_repeat == MAX_MSG_REP ) {     \
    snprintf( t_msgbuf, SZ_MSGBUF-1, "..." ); \
    __FPRINTF( MessageType );           \
  }                                     \
  else if( msg_repeat < MAX_MSG_REP ) { \
    __FPRINTF( MessageType );           \
  }                                     \
} WHILE_ZERO


      switch( msgtype ) {
        // Benign messages
        case CXLIB_EXC_DEBUG:
          __FPRINTF( "DEBUG" );
          break;
        case CXLIB_EXC_VERBOSE:
          __FPRINTF( "VERBOSE" );
          break;
        case CXLIB_EXC_INFO:
          __FPRINTF( "INFO" );
          break;
        case CXLIB_EXC_WARNING:
          g_context->nWarning++;
          __LIMITED_FPRINTF( "WARNING" );
          break;
        // Error messages
        case CXLIB_ERR_FATAL:
          __FPRINTF( "FATAL" );
          // Print goodbye message and dump core. TODO: A little extreme, make it better
          cxlib_print_backtrace( 0 );
#ifdef STOP_ON_FATAL
          fprintf( ostream, "[%s]  HARD STOP.\n", tbuf);
          die = 1;
#else
          fprintf( ostream, "[%s]  (Execution will continue - data corruption possible)\n", tbuf);
#endif
          break;
        case CXLIB_ERR_CRITICAL:
          g_context->nCritical++;
          __fprintf_critical( ostream, tbuf, msg_typ, msg_sub, msg_mod, msg_cod, __get_threadid(), t_msgbuf );
          break;
        case CXLIB_ERR_ERROR:
          g_context->nError++;
          __LIMITED_FPRINTF( "ERROR" );
          break;
        default:
          __FPRINTF( "" );
          break;
      }
    } RELEASE;
    fflush( ostream );
  }
  else {
    fprintf( stderr, "PROGRAM ERROR: call to cxlib_exc() with no exception context.\n" );
    fprintf( stderr, "HARD STOP.\n" );
    fflush( stderr );
    die = 1;
  }

  if( die ) {
    // THE END
#if defined CXPLAT_MAC_ARM64
    __builtin_trap();
#else
    *((int*)NULL)=0; // goodbye
#endif
  }

  return 0;
#undef __FPRINTF
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_exception_counters( int64_t *nWarning, int64_t *nError, int64_t *nCritical ) {
  SYNCHRONIZE_ON( g_context->lock ) {
    if( nWarning ) {
      *nWarning  = g_context->nWarning;
    }
    if( nError ) {
      *nError  = g_context->nError;
    }
    if( nCritical ) {
      *nCritical  = g_context->nCritical;
    }
  } RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_exception_counters_reset( void ) {
  SYNCHRONIZE_ON( g_context->lock ) {
    g_context->nWarning = 0;
    g_context->nError = 0;
    g_context->nCritical = 0;
  } RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_ostream_lock( void ) {
  if( g_context != NULL ) {
    ENTER_CRITICAL_SECTION( &g_context->lock.lock ); 
    g_context->recursion++;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void cxlib_ostream_release( void ) {
  if( g_context != NULL ) {
    g_context->recursion--;
    LEAVE_CRITICAL_SECTION( &g_context->lock.lock ); 
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int XXX_cxlib_ostream( const char *msg, ... ) {
  static char buf[1024] = {'\0'};
  static int sz_indent = 4;
  va_list args;
  FILE *ostream;
  
  if( g_context != NULL && msg ) {

    if( g_context->mute ) {
      return 0;
    }

    ostream = g_context->ostream ? g_context->ostream : stderr;

    SYNCHRONIZE_ON( g_context->lock ) {
      int rem = 1023;
      char *p = buf;
      int indent = g_context->recursion - 1;
      while( indent > 0 && rem > sz_indent ) {
        for( int i=0; i<sz_indent; i++ ) {
          *p++ = ' ';
          --rem;
        }
        --indent;
      }
      va_start( args, msg );
      vsnprintf( p, rem, msg, args );
      va_end( args );
      fprintf( ostream, "%s", buf );
    } RELEASE;

    fflush( ostream );
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_ostream( const char *msg, ... ) {

  static __THREAD char _buf[512] = {'\0'};

  char *buf = _buf; // optimistic
  static int sz_indent = 4;
  va_list args;
  FILE *ostream;
  
  if( g_context != NULL && msg ) {

    if( g_context->mute ) {
      return 0;
    }

    ostream = g_context->ostream ? g_context->ostream : stderr;

    int indent;
    SYNCHRONIZE_ON( g_context->lock ) {
      indent = g_context->recursion - 1;
    } RELEASE;

    int capacity = 511;
    int nw;
    do {
      nw = 0;
      char *p = buf;
      int n = indent;
      while( n > 0 && capacity > sz_indent ) {
        for( int i=0; i<sz_indent; i++ ) {
          *p++ = ' ';
        }
        capacity -= sz_indent;
        nw += sz_indent;
        --n;
      }

      va_start( args, msg );
      int nchars = vsnprintf( p, capacity, msg, args );
      va_end( args );
      
      // success
      if( nchars < capacity ) {
        nw += nchars;
        break;
      }
      
      // did not fit and we're already using a malloc'ed buffer that was supposed to be big enough
      if( buf != _buf ) {
        free( buf );
        return -1;
      }

      // malloc buffer big enough and try again
      int msz = nw + nchars + 1;
      if( (buf = calloc( msz + 1LL, sizeof(char) )) == NULL ) {
        return -1;
      }

      capacity = msz;

    } while( buf );

    SYNCHRONIZE_ON( g_context->lock ) {
      fwrite( buf, 1, nw, ostream );
    } RELEASE;

    if( buf != _buf ) {
      free( buf );
    }

    fflush( ostream );
  }

  return 0;
}




/*******************************************************************//**
 *
 ***********************************************************************
 */
int cxlib_msg_set( const char *filename, int line, const char *msg, ... ) {
  int ret;
  va_list args;
  // message
  va_start( args, msg );
  ret = vsnprintf( gt_thismsg, SZ_MSGBUF-1, msg, args );
  va_end( args );
  cxlib_trace_add( gt_thismsg );
  // filename
  if( filename ) {
    strcpy( gt_thisfname, filename );
  }
  else {
    gt_thisfname[0] = '\0';
  }
  // line
  gt_thisline = line;
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
const char * cxlib_msg_get( const char **filename, int *line ) {
  strcpy( gt_prevmsg, gt_thismsg );
  strcpy( gt_prevfname, gt_thisfname );
  gt_prevline = gt_thisline;
  gt_thismsg[0] = '\0';
  gt_thisfname[0] = '\0';
  gt_thisline = 0;
  if( filename ) {
    *filename = gt_prevfname;
  }
  if( line ) {
    *line = gt_prevline;
  }
  return gt_prevmsg;
}




/*******************************************************************//**
 *
 ***********************************************************************
 */

#if defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
#include <execinfo.h>
#elif defined CXPLAT_WINDOWS_X64
IGNORE_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN
#include <DbgHelp.h>
RESUME_WARNINGS
#endif
#if defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
void cxlib_print_backtrace( int nframes ) {
  void *buffer[ 128 ];
  int n = backtrace( buffer, 128 );
  char **names = backtrace_symbols( buffer, n );
  if( names ) {

    if( nframes <= 0 ) {
      nframes = 100;
    }

    CS_LOCK *lock = g_context ? &g_context->lock : NULL;

    SYNCHRONIZE_ON_PTR( lock ) {
      uint64_t tid = 0;
#if defined CXPLAT_LINUX_ANY
      tid = (uint64_t)syscall(SYS_gettid);
#elif defined CXPLAT_MAC_ARM64
      pthread_threadid_np(NULL, &tid);
#endif
      printf( "-----------------------------------------------------------\n" );
      printf( "*** backtrace for thread %llu ***\n", tid );
      printf( "-----------------------------------------------------------\n" );
      for( int i = 1; i < n && nframes-- > 0; i++ ) {
        printf( "%d: %s\n", n-i-1, names[i] );
      }
      printf( "-----------------------------------------------------------\n" );
    } RELEASE;
    free( names ); 
  }
}

cxlib_symbol_name cxlib_get_symbol_name( const uintptr_t address ) {

  cxlib_symbol_name name = {0};
  Dl_info info = {0};

  if( dladdr( (void*)address, &info ) != 0 && info.dli_sname ) {
    name.len = snprintf( name.value, 254, "%s", info.dli_sname );
  }
  else {
    name.len = snprintf( name.value, 254, "<symbol @ %016llx>", address );
  }
  return name;
}

#else

void cxlib_print_backtrace( int nframes ) {
  unsigned int   i;
  void         * stack[ 100 ];
  unsigned short n;
  SYMBOL_INFO  * symbol;
  HANDLE         process;

  process = GetCurrentProcess();

  SymInitialize( process, NULL, TRUE );
  n = CaptureStackBackTrace( 0, 100, stack, NULL );
  if( (symbol = ( SYMBOL_INFO * )calloc( sizeof( SYMBOL_INFO ) + 256 * sizeof( char ), 1 )) == NULL ) {
    return;
  }
  symbol->MaxNameLen = 255;
  symbol->SizeOfStruct = sizeof( SYMBOL_INFO );

  if( nframes <= 0 ) {
    nframes = 100;
  }

  CS_LOCK *lock = g_context ? &g_context->lock : NULL;

  SYNCHRONIZE_ON_PTR( lock ) {
    uint32_t tid = GetCurrentThreadId();
    printf( "-----------------------------------------------------------\n" );
    printf( "*** backtrace for thread %u ***\n", tid );
    printf( "-----------------------------------------------------------\n" );
    for( i = 1; i < n && nframes-- > 0; i++ ) {
      SymFromAddr( process, ( DWORD64 )( stack[ i ] ), 0, symbol );
      printf( "%i: %s - 0x%0llX\n", n - i - 1, symbol->Name, symbol->Address );
    }
    printf( "-----------------------------------------------------------\n" );
  } RELEASE;

  free( symbol );

}

cxlib_symbol_name cxlib_get_symbol_name( const uintptr_t address ) {
  
  cxlib_symbol_name name = {0};
  
  // Get process handle
  HANDLE process = GetCurrentProcess();
  SymInitialize( process, NULL, TRUE );

  // Allocate and initialize symbol
  SYMBOL_INFO *symbol = (SYMBOL_INFO *)calloc( sizeof( SYMBOL_INFO ) + 256 * sizeof( char ), 1 );
  if( symbol != NULL ) {
    symbol->MaxNameLen   = 255;
    symbol->SizeOfStruct = sizeof( SYMBOL_INFO );

    // Get symbol name from supplied address
    if( SymFromAddr( process, address, 0, symbol ) == TRUE ) {
      // Copy name into return string
      strncpy( name.value, symbol->Name, 255 );
      name.len = (int)symbol->NameLen;
    }
    else {
      name.len = snprintf( name.value, 254, "<unknown symbol @ %016llx>", address );
    }

    // Clean up
    free( symbol );
  }

  return name;
}

#endif

