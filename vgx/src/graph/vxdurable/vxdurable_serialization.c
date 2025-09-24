/*######################################################################
 *#
 *# vxdurable_serialization.c
 *#
 *#
 *######################################################################
 */


#include "_vgx_serialization.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static int64_t _vxdurable_serialization__write_string( const CString_t *CSTR__string, CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_digested_string( const CString_t *CSTR__string, CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_key_and_value( const CString_t *CSTR__key, const CString_t *CSTR__value, CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_begin_file( const CString_t *CSTR__filename, CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_end_file( CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_begin_section( const CString_t *CSTR__section_name, CQwordQueue_t *__OUTPUT );
static int64_t _vxdurable_serialization__write_begin_section_format( CQwordQueue_t *__OUTPUT, const char *format, ... );
static int64_t _vxdurable_serialization__write_end_section( CQwordQueue_t *__OUTPUT );

static int64_t _vxdurable_serialization__read_string( CString_t **CSTR__string, const CString_t *CSTR__expect, CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__read_digested_string( CString_t **CSTR__string, const CString_t *CSTR__expect, CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__read_key_and_value( CString_t **CSTR__key, CString_t **CSTR__value, CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__expect_begin_file( const CString_t *CSTR__filename, CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__expect_end_file( CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__expect_begin_section( const CString_t *CSTR__section_name, CQwordQueue_t *__INPUT );
static int64_t _vxdurable_serialization__expect_begin_section_format( CQwordQueue_t *__INPUT, const char *format, ... );
static int64_t _vxdurable_serialization__expect_end_section( CQwordQueue_t *__INPUT );

static void _vxdurable_serialization__print_graph_counts_ROG( vgx_Graph_t *self, const char *message );
static int64_t _vxdurable_serialization__bulk_serialize( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget, bool force, bool remote, CString_t **CSTR__error );
static graph_state_t * _vxdurable_serialization__load_state( vgx_Graph_t *self );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "VX::SER(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXDURABLE_SERIALIZATION_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SERIALIZATION_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SERIALIZATION_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SERIALIZATION_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SERIALIZATION_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SERIALIZATION_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )



DLL_HIDDEN vgx_ISerialization_t iSerialization = {
  .WriteString              = _vxdurable_serialization__write_string,
  .WriteDigestedString      = _vxdurable_serialization__write_digested_string,
  .WriteKeyValue            = _vxdurable_serialization__write_key_and_value,
  .WriteBeginFile           = _vxdurable_serialization__write_begin_file,
  .WriteEndFile             = _vxdurable_serialization__write_end_file,
  .WriteBeginSection        = _vxdurable_serialization__write_begin_section,
  .WriteBeginSectionFormat  = _vxdurable_serialization__write_begin_section_format,
  .WriteEndSection          = _vxdurable_serialization__write_end_section,

  .ReadString               = _vxdurable_serialization__read_string,
  .ReadDigestedString       = _vxdurable_serialization__read_digested_string,
  .ReadKeyValue             = _vxdurable_serialization__read_key_and_value,
  .ExpectBeginFile          = _vxdurable_serialization__expect_begin_file,
  .ExpectEndFile            = _vxdurable_serialization__expect_end_file,
  .ExpectBeginSection       = _vxdurable_serialization__expect_begin_section,
  .ExpectBeginSectionFormat = _vxdurable_serialization__expect_begin_section_format,
  .ExpectEndSection         = _vxdurable_serialization__expect_end_section,

  .PrintGraphCounts_ROG     = _vxdurable_serialization__print_graph_counts_ROG,
  .BulkSerialize            = _vxdurable_serialization__bulk_serialize,
  .LoadState                = _vxdurable_serialization__load_state
};




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __eval_queue_operation_format( CQwordQueue_t *queue, int64_t (*func)(const CString_t *CSTR__string, CQwordQueue_t *queue), const char *format, va_list *args ) {
  int64_t __NQWORDS = 0;

  CString_t *CSTR__string = NULL;

  XTRY {
    CString_constructor_args_t string_args = {
      .string       = NULL,
      .len          = -1,
      .ucsz         = 0,
      .format       = format,
      .format_args  = args,
      .alloc        = NULL
    };
    CSTR__string = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args );

    if( CSTR__string == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x911 );
    }

    EVAL_OR_THROW( func( CSTR__string, queue ), 0x912 );

  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    if( CSTR__string ) {
      COMLIB_OBJECT_DESTROY( CSTR__string );
    }
  }

  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_string( const CString_t *CSTR__string, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Get the string length and number of qwords in string
    QWORD len = CStringLength( CSTR__string );
    QWORD qw_sz = qwcount( len + 1 );

    // Write string length in bytes
    WRITE_OR_THROW( &len, 1, 0x921 );

    // Write total number of qwords in qword string including terminator
    WRITE_OR_THROW( &qw_sz, 1, 0x922 );

    // Write string data
    int64_t n = qw_sz;
    WRITE_OR_THROW( CStringValueAsQwords(CSTR__string), n, 0x923 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 * NOTE: CALLER OWNS MEMORY!
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__read_string( CString_t **CSTR__string, const CString_t *CSTR__expect, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    QWORD len, *plen = &len;
    QWORD qw_sz, *pqw_sz = &qw_sz;

    // String length in bytes
    READ_OR_THROW( plen, 1, 0x931 );

    // String size in qwords
    READ_OR_THROW( pqw_sz, 1, 0x932 );

    // Construct destination string
    CString_constructor_args_t string_args = {
      .string       = NULL,
      .len          = (int32_t)len,
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = NULL
    };
    if( (*CSTR__string = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x933 );
    }
    
    // Populate new string
    QWORD *data = CALLABLE( *CSTR__string )->ModifiableQwords( *CSTR__string );
    int64_t n = qw_sz;
    READ_OR_THROW( data, n, 0x934 );

    // As expected?
    if( CSTR__expect ) {
      if( CALLABLE( *CSTR__string )->Equals( *CSTR__string, CSTR__expect ) == false ) {
        THROW_SILENT( CXLIB_ERR_CORRUPTION, 0x935 );
      }
    }
  }
  XCATCH( errcode ) {
    if( *CSTR__string ) {
      COMLIB_OBJECT_DESTROY( *CSTR__string );
      *CSTR__string = NULL;
    }
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  // NOTE: CALLER OWNS MEMORY!
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_digested_string( const CString_t *CSTR__string, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // String digest
    const objectid_t *pobid = CStringObid(CSTR__string);
    WRITE_OR_THROW( (QWORD*)pobid, qwsizeof(objectid_t), 0x941 );
    // String
    EVAL_OR_THROW( _vxdurable_serialization__write_string( CSTR__string, __OUTPUT ), 942 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 * NOTE: CALLER OWNS MEMORY!
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__read_digested_string( CString_t **CSTR__string, const CString_t *CSTR__expect, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  int64_t n;
  XTRY {
    objectid_t obid;
    QWORD *qw_obid = (QWORD*)&obid;
    // String digest
    READ_OR_THROW( qw_obid, qwsizeof(obid), 0x951 );
    // String data
    if( (n = _vxdurable_serialization__read_string( CSTR__string, CSTR__expect, __INPUT )) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x952 );
    }
    __NQWORDS += n;
    // Check digest
    const objectid_t *pobid_actual = CStringObid( *CSTR__string );
    if( !idmatch( &obid, pobid_actual ) ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x953 );
    }
  }
  XCATCH( errcode ) {
    if( *CSTR__string ) {
      COMLIB_OBJECT_DESTROY( *CSTR__string );
      *CSTR__string = NULL;
    }
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  // NOTE: CALLER OWNS MEMORY!
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_key_and_value( const CString_t *CSTR__key, const CString_t *CSTR__value, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Key Follows
    WRITE_OR_THROW( g_KEY_FOLLOWS, qwsizeof( g_KEY_FOLLOWS ), 0x961 );
    EVAL_OR_THROW( _vxdurable_serialization__write_string( CSTR__key, __OUTPUT ), 0x962 );

    // Value Follows
    WRITE_OR_THROW( g_VALUE_FOLLOWS, qwsizeof( g_VALUE_FOLLOWS ), 0x963 );
    EVAL_OR_THROW( _vxdurable_serialization__write_string( CSTR__value, __OUTPUT ), 0x964 );

    // End KeyValue
    WRITE_OR_THROW( g_END_KEY_VALUE, qwsizeof( g_END_KEY_VALUE ), 0x965 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__read_key_and_value( CString_t **CSTR__key, CString_t **CSTR__value, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Key
    EXPECT_OR_THROW( g_KEY_FOLLOWS, 0x971 );
    EVAL_OR_THROW( _vxdurable_serialization__read_string( CSTR__key, NULL, __INPUT ), 0x972 );

    // Value
    EXPECT_OR_THROW( g_VALUE_FOLLOWS, 0x973 );
    EVAL_OR_THROW( _vxdurable_serialization__read_string( CSTR__value, NULL, __INPUT ), 0x974 );
    
    // Except End of Key Value
    EXPECT_OR_THROW( g_END_KEY_VALUE, 0x975 );
  }
  XCATCH( errcode ) {
    if( *CSTR__key ) {
      COMLIB_OBJECT_DESTROY( *CSTR__key );
      *CSTR__key = NULL;
    }
    if( *CSTR__value ) {
      COMLIB_OBJECT_DESTROY( *CSTR__value );
      *CSTR__value = NULL;
    }
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_begin_file( const CString_t *CSTR__filename, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Begin file delimiter
    WRITE_OR_THROW( g_BEGIN_FILE_DELIM, qwsizeof( g_BEGIN_FILE_DELIM ), 0x981 );
    // Section: filename
    EVAL_OR_THROW( _vxdurable_serialization__write_begin_section( CSTR__filename, __OUTPUT ), 0x982 );
    EVAL_OR_THROW( _vxdurable_serialization__write_end_section( __OUTPUT ), 0x983 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__expect_begin_file( const CString_t *CSTR__filename, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Expect Begin File Delimiter
    EXPECT_OR_THROW( g_BEGIN_FILE_DELIM, 0x991 );
    // Section: filename
    if( _vxdurable_serialization__expect_begin_section( CSTR__filename, __INPUT ) < 0 ) {
      VXDURABLE_SERIALIZATION_WARNING( NULL, 0x992, "File path changed? (%s)", CStringValue( CSTR__filename ) );
    }
    EVAL_OR_THROW( _vxdurable_serialization__expect_end_section( __INPUT ), 0x993 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_end_file( CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // End file delimiter
    WRITE_OR_THROW( g_END_FILE_DELIM, qwsizeof( g_END_FILE_DELIM ), 0x9A1 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {}
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__expect_end_file( CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Expect End File Delimiter
    EXPECT_OR_THROW( g_END_FILE_DELIM, 0x9B1 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_begin_section( const CString_t *CSTR__section_name, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Begin section delimiter
    WRITE_OR_THROW( g_BEGIN_SECTION_DELIM, qwsizeof( g_BEGIN_SECTION_DELIM ), 0x9C1 );
    // Section name
    EVAL_OR_THROW( _vxdurable_serialization__write_digested_string( CSTR__section_name, __OUTPUT ), 0x9C2 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_begin_section_format( CQwordQueue_t *__OUTPUT, const char *format, ... ) {
  va_list args;
  va_start( args, format );
  int64_t __NQWORDS = __eval_queue_operation_format( __OUTPUT, _vxdurable_serialization__write_begin_section, format, &args );
  va_end( args );
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__expect_begin_section( const CString_t *CSTR__section_name, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  int64_t n;
  CString_t *CSTR__string = NULL;
  XTRY {
    // Expect section delimiter
    EXPECT_OR_THROW( g_BEGIN_SECTION_DELIM, 0x9D1 );
    // Validate section name
    if( (n = _vxdurable_serialization__read_digested_string( &CSTR__string, CSTR__section_name, __INPUT )) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x9D2 );
    }
    __NQWORDS += n;
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    if( CSTR__string ) {
      COMLIB_OBJECT_DESTROY( CSTR__string );
    }
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__expect_begin_section_format( CQwordQueue_t *__INPUT, const char *format, ... ) {
  va_list args;
  va_start( args, format );
  int64_t __NQWORDS = __eval_queue_operation_format( __INPUT, _vxdurable_serialization__expect_begin_section, format, &args );
  va_end( args );
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__write_end_section( CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // END section delimiter
    WRITE_OR_THROW( g_END_SECTION_DELIM, qwsizeof( g_END_SECTION_DELIM ), 0x9E1 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__expect_end_section( CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;
  XTRY {
    // Expect section delimiter
    EXPECT_OR_THROW( g_END_SECTION_DELIM, 0x9F1 );
  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __cstring_steal_to_buffer( char *buf, CString_t **CSTR__str ) {
  if( *CSTR__str ) {
    const char *str = CStringValue( *CSTR__str );
    int32_t sz = CStringLength( *CSTR__str );
    const char *src = str;
    char *dest = buf;
    const char *s_end = src + sz;
    const char *d_end = dest + 1023;
    while( src < s_end && dest < d_end ) {
      *dest++ = *src++;
    }
    *dest = '\0';
    CStringDelete( *CSTR__str );
    *CSTR__str = NULL;
  }
  else {
    buf[0] = '?';
    buf[1] = '\0';
  }
  return buf;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __write_ascii_state( vgx_Graph_t *self, graph_state_t *state, const char *fname ) {
  FILE *afile = CX_FOPEN( fname, "w" );
  if( afile == NULL ) {
    return -1;
  }

  CString_t *CSTR__version_long = igraphinfo.Version(1);

  char tmbuf[128] = {0};
  char *p_graph_t0 = tmbuf;
  char *p_TIC_t0 = tmbuf+32;
  char *p_persist_t0 = tmbuf+64;
  char *p_persist_t1 = tmbuf+96;
  time_t graph_t0 = state->time.graph_t0;
  time_t TIC_t0 = state->time.TIC_t0;
  time_t persist_t0 = state->time.persist_t0;
  time_t persist_t1 = state->time.persist_t1;

  struct tm *_tm;
  if( (_tm = localtime( &graph_t0 )) != NULL ) {
    strftime( p_graph_t0,   32, "%Y-%m-%d %H:%M:%S", _tm );
  }
  if( (_tm = localtime( &TIC_t0 )) != NULL ) {
    strftime( p_TIC_t0,     32, "%Y-%m-%d %H:%M:%S", _tm );
  }
  if( (_tm = localtime( &persist_t0 )) != NULL ) {
    strftime( p_persist_t0, 32, "%Y-%m-%d %H:%M:%S", _tm );
  }
  if( (_tm = localtime( &persist_t1 )) != NULL ) {
    strftime( p_persist_t1, 32, "%Y-%m-%d %H:%M:%S", _tm );
  }

  int64_t graph_up = (state->time.graph_up);
  int64_t graph_age = (int64_t)(persist_t0 - graph_t0);
  int64_t persist_dt = (int64_t)(persist_t1 - persist_t0);
  int64_t persist_t = state->time.persist_t;

  if( graph_up < 1 ) {
    graph_up = 1;
  }

  CString_t *CSTR__fmt = NULL;
  char buffer[1024] = {0};


#define WRITE_LINE( FormatStr, ... )  fprintf( afile, FormatStr"\n", ##__VA_ARGS__ )
  // STATE
  WRITE_LINE( "[[STATE]]" );
  WRITE_LINE( "= Graph State %s", p_persist_t1 );
  WRITE_LINE( "" );

#define ADOC_TABLE_HEAD( Col1, Sz1, Col2, Sz2 )   \
  WRITE_LINE( "[cols=\"%d,%d\"]", Sz1, Sz2 );     \
  WRITE_LINE( "|===" );                           \
  WRITE_LINE( "|%s |%s", Col1, Col2 );            \
  WRITE_LINE( "" )

#define ADOC_TABLE_END  \
  WRITE_LINE( "|===" ); \
  WRITE_LINE( "" )

#define ADOC_ROW_INT( Key, IntVal )   \
  WRITE_LINE( "|%s", (Key) );         \
  WRITE_LINE( "|%lld", (IntVal));     \
  WRITE_LINE( "" )

#define ADOC_ROW_STRING( Key, StrVal )  \
  WRITE_LINE( "|%s", (Key) );           \
  WRITE_LINE( "|%s", (StrVal) );        \
  WRITE_LINE( "" )



  // GRAPH
  WRITE_LINE( "[[GRAPH]]" );
  WRITE_LINE( "== General" );
  WRITE_LINE( "" );

  ADOC_TABLE_HEAD(  "Field", 2,             "Value", 8 );
  ADOC_ROW_STRING(  "VGX Version",          CSTR__version_long ? CStringValue( CSTR__version_long ) : "?" );
  CString_t *CSTR__fqdn = iURI.NewFqdn();
  const char *fqdn = CStringValueDefault( CSTR__fqdn, "?" );
  ADOC_ROW_STRING(  "Host Name",             fqdn );
  iString.Discard( &CSTR__fqdn );
  char *ip = cxgetip( NULL );
  ADOC_ROW_STRING(  "Host IP",               ip ? ip : "0.0.0.0" );
  free( ip );
  ADOC_ROW_STRING(  "Graph Name",           state->graph.name );
  ADOC_ROW_INT(     "Vertices (order)",     state->graph.order );
  ADOC_ROW_INT(     "Arcs (size)",          state->graph.size );
  ADOC_ROW_INT(     "Properties",           state->vertex_property.nprop );
  ADOC_ROW_INT(     "Strings",              state->vertex_property.nstrings );
  ADOC_ROW_INT(     "TMX Events",           state->graph.n_tmx );
  CSTR__fmt = CStringNewFormat( "%016llx%016llx", state->graph.tx_id_out.H, state->graph.tx_id_out.L );
  ADOC_ROW_STRING(  "Recent OUT TX",        __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_ROW_INT(     "Recent OUT S/N",       state->graph.tx_serial_out );
  ADOC_ROW_INT(     "OUT TX Count" ,        state->graph.tx_count_out );
  CSTR__fmt = CStringNewFormat( "%016llx%016llx", state->graph.tx_id_in.H, state->graph.tx_id_in.L );
  ADOC_ROW_STRING(  "Recent IN TX",         __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_ROW_INT(     "Recent IN S/N",        state->graph.tx_serial_in );
  ADOC_ROW_INT(     "IN TX Count",          state->graph.tx_count_in );
  ADOC_ROW_INT(     "Vectors",              state->vector.nvectors );
  ADOC_ROW_INT(     "Dimensions",           state->vector.ndim );
  ADOC_ROW_INT(     "Operation Counter",    state->graph.opcount );
  ADOC_ROW_STRING(  "Graph Access State",   state->graph.readonly ? "READONLY" : "WRITABLE" );
  ADOC_ROW_STRING(  "Graph Interconnect",   state->graph.local_only ? "LOCAL ONLY" : "NORMAL" );
  ADOC_TABLE_END;


  // TIME
  WRITE_LINE( "[[TIME]]" );
  WRITE_LINE( "== Timestamps" );
  WRITE_LINE( "" );

  ADOC_TABLE_HEAD( "Field", 2,                  "Value", 4 );
  ADOC_ROW_STRING( "Graph Inception",           p_graph_t0 );
  CSTR__fmt = CStringNewFormat( "%lldd %lldh %lldm %llds", graph_age/86400, (graph_age%86400)/3600, (graph_age%3600)/60, graph_age%60 );
  ADOC_ROW_STRING( "Graph Age",                 __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_ROW_STRING( "Graph Start",               p_TIC_t0 );
  CSTR__fmt = CStringNewFormat( "%lldd %lldh %lldm %llds", graph_up/86400, (graph_up%86400)/3600, (graph_up%3600)/60, graph_up%60 );
  ADOC_ROW_STRING( "Graph Uptime",              __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_ROW_INT(    "Persist Count",             state->time.persist_n );
  ADOC_ROW_STRING( "Last Persist Start",        p_persist_t0 );
  ADOC_ROW_STRING( "Last Persist Complete",     p_persist_t1 );
  CSTR__fmt = CStringNewFormat( "%lldm %llds", persist_dt/60, persist_dt%60 );
  ADOC_ROW_STRING( "Last Persist Duration",     __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_ROW_INT(    "Ops Since Last Persist",    state->graph.n_ops );
  CSTR__fmt = CStringNewFormat( "%lldm %llds", persist_t/60, persist_t%60 );
  ADOC_ROW_STRING( "Lifetime Persist Duration", __cstring_steal_to_buffer( buffer, &CSTR__fmt ) );
  ADOC_TABLE_END;


  // VERTEX TYPE
  WRITE_LINE( "[[VERTEXTYPE]]" );
  WRITE_LINE( "== Vertex Type Enumeration");
  WRITE_LINE( "" );
  WRITE_LINE( "[cols=\"8,2,3\"]" );
  WRITE_LINE( "|===" );
  WRITE_LINE( "|Name |Enum |Order" );
  WRITE_LINE( "" );
  for( int i=0; i<VERTEX_TYPE_ENUMERATION_MAX_ENTRIES; i++ ) {
    QWORD typehash = state->vertex_type.entry[ i ].typehash;
    if( typehash ) {
      QWORD typeenc = state->vertex_type.entry[ i ].typeenc;
      QWORD order = state->vertex_type.entry[ i ].order;
      const char *prefix = state->vertex_type.entry[ i ].prefix;
      WRITE_LINE( "|`+%s+`", prefix );
      WRITE_LINE( "|`0x%04llX`", typeenc );
      WRITE_LINE( "|%llu", order );
      WRITE_LINE( "" );
    }
  }
  WRITE_LINE( "" );
  WRITE_LINE( "|===" );
  WRITE_LINE( "" );

  // RELATIONSHIP
  WRITE_LINE( "[[RELATIONSHIP]]" );
  WRITE_LINE( "== Relationship Enumeration" );
  WRITE_LINE( "" );
  WRITE_LINE( "[cols=\"8,2,3\"]" );
  WRITE_LINE( "|===" );
  WRITE_LINE( "|Name |Enum |Size" );
  WRITE_LINE( "" );
  for( int i=0; i<__VGX_PREDICATOR_REL_USER_RANGE_SIZE; i++ ) {
    QWORD relhash = state->relationship.entry[ i ].relhash;
    if( relhash ) {
      QWORD relenc = state->relationship.entry[ i ].relenc;
      const char *prefix = state->relationship.entry[ i ].prefix;
      WRITE_LINE( "|`+%s+`", prefix );
      WRITE_LINE( "|`0x%04llX`", relenc  );
      WRITE_LINE( "|n/a" );
      WRITE_LINE( "" );
    }
  }
  WRITE_LINE( "|===" );
  WRITE_LINE( "" );

  // PROPERTIES
  WRITE_LINE( "[[VERTEXPROPERTIES]]" );
  WRITE_LINE( "== Vertex Property Enumeration" );
  WRITE_LINE( "" );
  ADOC_TABLE_HEAD( "Enumeration", 3,        "Count", 2 );
  ADOC_ROW_INT(    "Unique Keys",           state->vertex_property.nkey );
  ADOC_ROW_INT(    "Unique String Values",  state->vertex_property.nstrval );
  ADOC_TABLE_END;

  // VECTOR
  WRITE_LINE( "[[VECTOR]]" );
  WRITE_LINE( "== Vector Enumeration" );
  WRITE_LINE( "" );
  ADOC_TABLE_HEAD( "Enumeration", 3,        "Count", 4 );
  ADOC_ROW_INT(    "Unique Dimensions",     state->vector.ndim );
  ADOC_TABLE_END;

  // EVENT BACKLOG INFO
  vgx_EventBacklogInfo_t backlog = iGraphEvent.BacklogInfo( self );
  WRITE_LINE( "[[EVENTSCHEDULE]]" );
  WRITE_LINE( "== TMX Event Schedule" );
  WRITE_LINE( "" );
  ADOC_TABLE_HEAD( "Schedule", 3,   "Events", 4 );
  ADOC_ROW_INT(    "Long Term",     backlog.n_long );
  ADOC_ROW_INT(    "Medium Term",   backlog.n_med );
  ADOC_ROW_INT(    "Short Term",    backlog.n_short + backlog.n_current );
  ADOC_TABLE_END;

  vgx_MemoryInfo_t meminfo = CALLABLE( self )->advanced->GetMemoryInfo( self );

#define ALLOCATOR_ROW( Key, ObjCount, Info )                                                      \
  WRITE_LINE( "|%s", (Key) );                                                                     \
  WRITE_LINE( "|%lld KB", (int64_t)ceil( (double)(Info).bytes/(1024.0) ) );                       \
  if( (ObjCount) >= 0 ) {                                                                         \
    WRITE_LINE( "|%lld", (ObjCount) );                                                            \
  }                                                                                               \
  else {                                                                                          \
    WRITE_LINE( "|" );                                                                            \
  }                                                                                               \
  if( (ObjCount) > 0 ) {                                                                          \
    WRITE_LINE( "|%.1f", (Info).bytes / (double)(ObjCount) );                                     \
  }                                                                                               \
  else {                                                                                          \
    WRITE_LINE( "|" );                                                                            \
  }                                                                                               \
  WRITE_LINE( "|%.1f %%", 100.0 * (Info).utilization  );                                          \
  WRITE_LINE( "|%.1f %%", (double)(Info).bytes/(meminfo.system.process.use.bytes) * 100.0 );      \
  WRITE_LINE( "|%.1f %%", (double)(Info).bytes/(meminfo.system.global.physical.bytes) * 100.0 );  \
  WRITE_LINE( "" )

  int64_t none = -1;

  WRITE_LINE( "[[ALLOCATORS]]" );
  WRITE_LINE( "== Runtime Memory Usage (%s)", state->graph.name );
  WRITE_LINE( "" );
  WRITE_LINE( "[cols=\"4,3,2,2,2,2,2\"]" );   
  WRITE_LINE( "|===" );
  WRITE_LINE( "|Component |Allocated Memory |Objects |Object Cost |Allocator Utilization |VGX Relative |System Relative" );
  WRITE_LINE( "" );
  int64_t order = GraphOrder( self );
  int64_t size = GraphSize( self );
  ALLOCATOR_ROW(    "Vertex",              order, meminfo.pooled.vertex.object );
  ALLOCATOR_ROW(    "Arc",                 size,  meminfo.pooled.vertex.arcvector );
  ALLOCATOR_ROW(    "Property",            state->vertex_property.nprop, meminfo.pooled.vertex.property );

  ALLOCATOR_ROW(    "String",              state->vertex_property.nstrings, meminfo.pooled.string.data );

  ALLOCATOR_ROW(    "Global Index",        order, meminfo.pooled.index.global );
  ALLOCATOR_ROW(    "Type Index",          order, meminfo.pooled.index.type );

  ALLOCATOR_ROW(    "Vertex Type Codec",   state->vertex_type.ntype, meminfo.pooled.codec.vxtype );
  ALLOCATOR_ROW(    "Relationship Codec",  state->relationship.nrel,  meminfo.pooled.codec.rel );
  ALLOCATOR_ROW(    "Property Codec",      state->vertex_property.nprop, meminfo.pooled.codec.vxprop );
  ALLOCATOR_ROW(    "Dimension Codec",     state->vector.ndim, meminfo.pooled.codec.dim );

  ALLOCATOR_ROW(    "Internal Vector",     state->vector.nvectors, meminfo.pooled.vector.internal );
  ALLOCATOR_ROW(    "External Vector",     state->vector.nvectors, meminfo.pooled.vector.external );
  ALLOCATOR_ROW(    "Vector Dimension",    state->vector.ndim, meminfo.pooled.vector.dimension );
  
  ALLOCATOR_ROW(    "Event Schedule",      state->graph.n_tmx, meminfo.pooled.schedule.total );

  ALLOCATOR_ROW(    "Ephemeral String",    none,        meminfo.pooled.ephemeral.string );
  ALLOCATOR_ROW(    "Ephemeral Vector",    none,        meminfo.pooled.ephemeral.vector );
  ALLOCATOR_ROW(    "Acquisition Map",     none,        meminfo.pooled.ephemeral.vtxmap );

  vgx_AllocatorInfo_t other = {
    .bytes = meminfo.system.process.use.bytes - meminfo.pooled.total.bytes,
  };
  other.utilization = other.bytes / (double)meminfo.system.process.use.bytes;

  ALLOCATOR_ROW(    "Other",               none,        other );

  ALLOCATOR_ROW(    "*TOTAL VGX*",         none,        meminfo.system.process.use );
  ALLOCATOR_ROW(    "*SYSTEM MEMORY*",     none,        meminfo.system.global.physical );

  ADOC_TABLE_END;

#undef WRITE_LINE
#undef ADOC_TABLE_HEAD
#undef ADOC_TABLE_END
#undef ADOC_ROW_INT
#undef ADOC_ROW_STRING
#undef ALLOCATOR_BYTES

  iString.Discard( &CSTR__version_long );

  CX_FCLOSE( afile );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __write_binary_state( graph_state_t *state, const char *fname ) {
  static const int64_t state_size = sizeof( graph_state_t );
  int64_t nqwords = qwsizeof( graph_state_t );
  FILE *bfile = CX_FOPEN( fname, "wb" );
  if( bfile == NULL ) {
    return -1;
  }

  if( CX_FWRITE( state, state_size, 1, bfile ) != 1 ) {
    nqwords = -1;
  }

  CX_FCLOSE( bfile );

  return nqwords;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static graph_state_t * __new_graph_state_CS( vgx_Graph_t *self, uint32_t ts_start, int64_t persist_n, int64_t persist_t, int64_t opcnt_n ) {
  // Initialize state structure
  graph_state_t *state = calloc( 1, sizeof( graph_state_t ) );
  if( state == NULL ) {
    return NULL;
  }

  // Graph name
  const char *graph_name = CStringValue( CALLABLE( self )->Name( self ) );

  // Version
  CString_t *CSTR__version_short = igraphinfo.Version(0);
  const char *version = CSTR__version_short ? CStringValue( CSTR__version_short ) : "?";

  // BEGIN
  memcpy( state->begin._delim, g_BEGIN_FILE_DELIM, sizeof( g_BEGIN_FILE_DELIM ) );

  // GRAPH
  // _start
  memcpy( state->graph._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  // order
  state->graph.order = GraphOrder( self );
  // size
  state->graph.size = GraphSize( self );
  // opcount
  state->graph.opcount = iOperation.GetId_LCK( &self->operation );
  // new ops
  state->graph.n_ops = state->graph.opcount - opcnt_n;
  // name
  strncpy( state->graph.name, graph_name, VGX_MAX_GRAPH_NAME );
  // version
  strncpy( state->graph.version, version, 63 );
  // readonly
  // MUST SET READONLY INFO ON THE OUTSIDE!
  // n_tmx
  state->graph.n_tmx = _vxgraph_vxtable__count_vertex_tmx_ROG( self );

  // Disabled emitter means local only
  state->graph.local_only = self->control.local_only;

  // tx_id_out
  idcpy( &state->graph.tx_id_out, &self->tx_id_out );
  // tx_serial_out
  state->graph.tx_serial_out = self->tx_serial_out;
  // tx_count_out
  state->graph.tx_count_out = self->tx_count_out;

  // tx_id_in
  idcpy( &state->graph.tx_id_in, &self->tx_id_in );
  // tx_serial_in
  state->graph.tx_serial_in = self->tx_serial_in;
  // tx_count_in
  state->graph.tx_count_in = self->tx_count_in;

  // _end
  memcpy( state->graph._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // VERTEX TYPE
  // _start
  memcpy( state->vertex_type._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  int64_t ntype = 0;
  for( vgx_vertex_type_t vxtype=__VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype<=__VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype++ ) {
    if( iEnumerator_CS.VertexType.ExistsEnum( self, vxtype ) ) {
      const CString_t *CSTR__vxtype = iEnumerator_CS.VertexType.Decode( self, vxtype );
      ++ntype;
      // typehash
      state->vertex_type.entry[ vxtype ].typehash = CStringHash64( CSTR__vxtype );
      // typeenc
      state->vertex_type.entry[ vxtype ].typeenc = vxtype;
      // order
      state->vertex_type.entry[ vxtype ].order = _vxgraph_vxtable__len_CS( self, vxtype );
      // prefix
      strncpy( state->vertex_type.entry[ vxtype ].prefix, CStringValue( CSTR__vxtype ), 31 );
    }
  }
  // ntype
  state->vertex_type.ntype = ntype;
  // _end
  memcpy( state->vertex_type._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // RELATIONSHIP
  // _start
  memcpy( state->relationship._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  int64_t nrel = 0;
  for( vgx_predicator_rel_enum rel=__VGX_PREDICATOR_REL_START_USER_RANGE; rel<=__VGX_PREDICATOR_REL_END_USER_RANGE; rel++ ) {
    if( iEnumerator_CS.Relationship.ExistsEnum( self, rel ) ) {
      const CString_t *CSTR__rel = iEnumerator_CS.Relationship.Decode( self, rel );
      ++nrel;
      // relhash
      state->relationship.entry[ rel ].relhash = CStringHash64( CSTR__rel );
      // relenc
      state->relationship.entry[ rel ].relenc = rel;
      // size
      state->relationship.entry[ rel ].size = -1; // TBD (we don't have this information readily available)
      // prefix
      strncpy( state->relationship.entry[ rel ].prefix, CStringValue( CSTR__rel ), 31 );
    }
  }
  // nrel
  state->relationship.nrel = nrel;
  // _end
  memcpy( state->relationship._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // VERTEX PROPERTY
  // _start
  memcpy( state->vertex_property._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  // nkey
  state->vertex_property.nkey = CALLABLE( self->vxprop_keymap )->Items( self->vxprop_keymap );
  // nstrval
  state->vertex_property.nstrval = CALLABLE( self->vxprop_valmap )->Items( self->vxprop_valmap );
  // nprop
  state->vertex_property.nprop = GraphPropCount( self );

  // nstrings
  cxmalloc_family_t *propalloc = (cxmalloc_family_t*)self->property_allocator_context->allocator;
  state->vertex_property.nstrings = CALLABLE( propalloc )->Active( propalloc );

  // _end
  memcpy( state->vertex_property._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // VECTOR
  // _start
  memcpy( state->vector._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  // ndim
  if( self->similarity->dim_decoder ) {
    state->vector.ndim = CALLABLE( self->similarity->dim_decoder )->Items( self->similarity->dim_decoder );
  }
  // nvectors
  state->vector.nvectors = GraphVectorCount( self );
  // _end
  memcpy( state->vector._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // TIME
  // _start
  memcpy( state->vector._start, g_BEGIN_SECTION_DELIM, sizeof( g_BEGIN_SECTION_DELIM ) );
  // graph_t0
  state->time.graph_t0 = _vgx_graph_inception( self );
  // TIC_t0
  state->time.TIC_t0 = _vgx_graph_t0( self );
  // graph_up
  state->time.graph_up = _vgx_graph_seconds( self ) - _vgx_graph_t0( self );
  // persist_t0
  state->time.persist_t0 = ts_start;
  // persist_t1
  state->time.persist_t1 = __SECONDS_SINCE_1970(); // Not quite the end but we're close
  // persist_n
  state->time.persist_n = persist_n;
  // persist_t
  state->time.persist_t = persist_t + (state->time.persist_t1 - state->time.persist_t0);
  // _end
  memcpy( state->vector._end, g_END_SECTION_DELIM, sizeof( g_END_SECTION_DELIM ) );

  // END
  memcpy( state->end._delim, g_END_FILE_DELIM, sizeof( g_END_FILE_DELIM ) );

  iString.Discard( &CSTR__version_short );

  // OK!
  return state;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __serialize_state( vgx_Graph_t *self, uint32_t ts_start, int readonly ) {
  int64_t __NQWORDS = 0;

  graph_state_t *state = NULL;
  CString_t *CSTR__asum = NULL;
  CString_t *CSTR__bsum = NULL;

  XTRY {
    const char *name = CStringValue( CALLABLE( self )->Name( self ) );
    const char *graph_path = CALLABLE( self )->FullPath( self );
    
    // Ascii Path
    if( (CSTR__asum = CStringNewFormat( "%s/" VGX_PATHDEF_GRAPHSTATE_FMT VGX_PATHDEF_EXT_ASCIIDOC, graph_path, name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB01 );
    }
    const char *afname = CStringValue( CSTR__asum );

    // Binary Path
    if( (CSTR__bsum = CStringNewFormat( "%s/" VGX_PATHDEF_GRAPHSTATE_FMT VGX_PATHDEF_EXT_DATA, graph_path, name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB03 );
    }
    const char *bfname = CStringValue( CSTR__bsum );

    int64_t persist_n = 0;
    int64_t persist_t = 0;
    int64_t opcnt_n = 0;
    graph_state_t *prev_state = iSerialization.LoadState( self );
    if( prev_state ) {
      persist_n = prev_state->time.persist_n + 1;
      persist_t = prev_state->time.persist_t;
      opcnt_n = prev_state->graph.opcount;
      free( prev_state );
    }

    GRAPH_LOCK( self ) {
      state = __new_graph_state_CS( self, ts_start, persist_n, persist_t, opcnt_n );
    } GRAPH_RELEASE;

    if( state == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB04 );
    }

    // Capture durability point
    idcpy( &self->durable_tx_id, &self->tx_id_in );
    self->durable_tx_serial = self->tx_serial_in;
    self->persisted_ts = state->time.persist_t1;

    // Additional state information
    state->graph.readonly = readonly;

    // ===========================
    // Write the binary state file
    // ===========================
    if( (__NQWORDS = __write_binary_state( state, bfname )) < 0 ) {
      VXDURABLE_SERIALIZATION_CRITICAL( self, 0xB05, "Failed to persist ascii state: %s", bfname );
    }

    // ==========================
    // Write the ascii state file
    // ==========================
    if( __write_ascii_state( self, state, afname ) < 0 ) {
      VXDURABLE_SERIALIZATION_WARNING( self, 0xB06, "Failed to persist ascii state: %s", afname );
    }

  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__asum );
    iString.Discard( &CSTR__bsum );
    if( state ) {
      free( state );
    }
  }

  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static graph_state_t * _vxdurable_serialization__load_state( vgx_Graph_t *self ) {
  static const int64_t state_size = sizeof( graph_state_t );
  graph_state_t *state = NULL;
  CString_t *CSTR__bsum = NULL;
  FILE *bfile = NULL;

  XTRY {
    const char *name = CStringValue( CALLABLE( self )->Name( self ) );
    const char *graph_path = CALLABLE( self )->FullPath( self );

    // Binary Path
    if( (CSTR__bsum = CStringNewFormat( "%s/" VGX_PATHDEF_GRAPHSTATE_FMT VGX_PATHDEF_EXT_DATA, graph_path, name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
    const char *bfname = CStringValue( CSTR__bsum );

    // Load state from file if it exists
    if( file_exists( bfname ) ) {
      if( (bfile = CX_FOPEN( bfname, "rb" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
      }
      if( (state = calloc( 1, sizeof( graph_state_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      if( CX_FREAD( state, state_size, 1, bfile ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x004 );
      }
    }
    // Create state from current graph instance if no persisted state exists
    else {
      GRAPH_LOCK( self ) {
        state = __new_graph_state_CS( self, _vgx_graph_inception( self ), 0, 0, iOperation.GetId_LCK( &self->operation ) );
      } GRAPH_RELEASE;
      if( state == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
    }

  }
  XCATCH( errcode ) {
    if( state ) {
      free( state );
    }
    state = NULL;
  }
  XFINALLY {
    iString.Discard( &CSTR__bsum );
    if( bfile ) {
      CX_FCLOSE( bfile );
    }
  }

  return state;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __serialize_enumerators( vgx_Graph_t *self, const char *path, bool force, CString_t **CSTR__error ) {
  int64_t __NQWORDS = 0;
  int64_t value;

  PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( NULL ) {
    XTRY {
        // [18] vxtype_encoder
        value = CALLABLE( self->vxtype_encoder )->Items( self->vxtype_encoder );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB11, "Serializing: vertex type encoder (%lld mappings)", value );
        EVAL_OR_THROW( CALLABLE( self->vxtype_encoder )->BulkSerialize( self->vxtype_encoder, force ), 0xB12 );
        
        // [19] vxtype_decoder
        value = CALLABLE( self->vxtype_decoder )->Items( self->vxtype_decoder );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB13, "Serializing: vertex type decoder (%lld mappings)", value );
        EVAL_OR_THROW( CALLABLE( self->vxtype_decoder )->BulkSerialize( self->vxtype_decoder, force ), 0xB14 );

        // [20] rel_encoder
        value = CALLABLE( self->rel_encoder )->Items( self->rel_encoder );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB15, "Serializing: relationship type encoder (%lld mappings)", value );
        EVAL_OR_THROW( CALLABLE( self->rel_encoder )->BulkSerialize( self->rel_encoder, force ), 0xB16 );

        // [21] rel_decoder
        value = CALLABLE( self->rel_decoder )->Items( self->rel_decoder );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB17, "Serializing: relationship type decoder (%lld mappings)", value );
        EVAL_OR_THROW( CALLABLE( self->rel_decoder )->BulkSerialize( self->rel_decoder, force ), 0xB18 );

        // [22] vxprop_keymap
        value = CALLABLE( self->vxprop_keymap )->Items( self->vxprop_keymap );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB19, "Serializing: property key map (%lld unique keys)", value );
        EVAL_OR_THROW( CALLABLE( self->vxprop_keymap )->BulkSerialize( self->vxprop_keymap, force ), 0xB1A );

        // [23] vxprop_valmap
        value = CALLABLE( self->vxprop_valmap )->Items( self->vxprop_valmap );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB1B, "Serializing: property value map (%lld unique values)", value );
        EVAL_OR_THROW( CALLABLE( self->vxprop_valmap )->BulkSerialize( self->vxprop_valmap, force ), 0xB1C );
    }
    XCATCH( errcode ) {
      if( CSTR__error && *CSTR__error == NULL ) {
        *CSTR__error = CStringNewFormat( "Error during serialization of '%s': errcode=0x%x", path, errcode );
      }
      __NQWORDS = -1;
    }
    XFINALLY {
    }
  } POP_STRING_ALLOCATOR_CONTEXT;

  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxdurable_serialization__print_graph_counts_ROG( vgx_Graph_t *self, const char *message ) {

  vgx_graph_base_counts_t counts = {0};
  iOperation.Graph_ROG.GetBaseCounts( self, &counts );
  vgx_MemoryInfo_t meminfo = CALLABLE( self )->advanced->GetMemoryInfo( self );

  int64_t vertex_bytes = meminfo.pooled.vertex.object.bytes
                       + meminfo.pooled.index.global.bytes
                       + meminfo.pooled.index.type.bytes
                       + meminfo.pooled.codec.vxtype.bytes;
    
  int64_t property_bytes = meminfo.pooled.codec.vxprop.bytes
                         + meminfo.pooled.vertex.property.bytes;

  int64_t string_bytes = meminfo.pooled.string.data.bytes;

  int64_t vector_bytes = meminfo.pooled.codec.dim.bytes
                       + meminfo.pooled.vector.dimension.bytes
                       + meminfo.pooled.vector.external.bytes
                       + meminfo.pooled.vector.internal.bytes;

  int64_t arc_bytes = meminfo.pooled.vertex.arcvector.bytes
                    + meminfo.pooled.codec.rel.bytes;
  
  int64_t total_bytes = meminfo.pooled.total.bytes;
  int64_t other_bytes = total_bytes - (vertex_bytes + property_bytes + string_bytes + vector_bytes + arc_bytes);

  int64_t obj_count = counts.order + counts.size + counts.nprop + counts.nstrval + counts.nvec;
  int64_t enum_count = (int64_t)counts.ntype + (int64_t)counts.nrel + counts.nkey + counts.nstrval + (int64_t)counts.ndim;

  double MiB = 1024.0 * 1024.0;

  cxlib_ostream_lock();
  VXDURABLE_SERIALIZATION_INFO( self, 0x001, "[%s]", message );
  VXDURABLE_SERIALIZATION_INFO( self, 0x002, "----------+------------+------------------+--------------+------------+" );
  VXDURABLE_SERIALIZATION_INFO( self, 0x003, "Object    | Count      | Enumeration      | Memory (MiB) | Memory (%%) |" );
  VXDURABLE_SERIALIZATION_INFO( self, 0x004, "----------+------------+------------------+--------------+------------+" );
  VXDURABLE_SERIALIZATION_INFO( self, 0x005, "Vertex    | %10lld | %10d (typ) | %12.1f | %10.1f |", counts.order, counts.ntype, vertex_bytes / MiB, 100.0 * (double)vertex_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x006, "Arc       | %10lld | %10d (typ) | %12.1f | %10.1f |", counts.size, counts.nrel, arc_bytes / MiB, 100.0 * (double)arc_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x007, "Property  | %10lld | %10lld (key) | %12.1f | %10.1f |", counts.nprop, counts.nkey, property_bytes / MiB, 100.0 * (double)property_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x008, "String    | %10lld | %10lld (str) | %12.1f | %10.1f |", counts.nstrval, counts.nstrval, string_bytes / MiB, 100.0 * (double)string_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x009, "Vector    | %10lld | %10d (dim) | %12.1f | %10.1f |", counts.nvec, counts.ndim, vector_bytes / MiB, 100.0 * (double)vector_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x00A, "Other     |            |                  | %12.1f | %10.1f |", other_bytes / MiB, 100.0 * (double)other_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x00B, "----------+------------+------------------+--------------+------------+" ) ;
  VXDURABLE_SERIALIZATION_INFO( self, 0x00C, "Total     | %10lld | %16d | %12.1f | %10.1f |", obj_count, enum_count, total_bytes / MiB, 100.0 * (double)total_bytes / total_bytes );
  VXDURABLE_SERIALIZATION_INFO( self, 0x00D, "----------+------------+------------------+--------------+------------+" );
  cxlib_ostream_release();


}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_serialization__bulk_serialize( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget, bool force, bool remote, CString_t **CSTR__error ) {

  int64_t __NQWORDS = 0;
  int64_t value;
  uint32_t ts_start = __SECONDS_SINCE_1970(); 
  int64_t t0 = __GET_CURRENT_MILLISECOND_TICK(); 

  // Optimistic start
  bool serialization_can_proceed = true;
  bool dirty = true;

  const char *path = CALLABLE( self )->FullPath( self );

  int readonly = 0;
  vgx_graph_base_counts_t counts = {0};

  _vgx_start_graph_execution_timing_budget( self, timing_budget );
  GRAPH_LOCK( self ) {

    // Check if another thread is already running serialization
    if( _vgx_is_serializing_CS( &self->readonly ) ) {
      __set_error_string( CSTR__error, "Serialization already running" );
      // Sorry, another thread already running serialization.
      serialization_can_proceed = false;
    }

    // PROCEED - no other thread serializing
    if( serialization_can_proceed ) {

      // Check if current thread holds any vertex locks
      if( _vxgraph_tracker__has_writable_locks_CS( self ) || _vxgraph_tracker__has_readonly_locks_CS( self ) ) {
        int64_t nw = 0;
        int64_t nr = 0;
        CString_t *CSTR__writable = _vxgraph_tracker__writable_vertices_as_cstring_CS( self, &nw );
        CString_t *CSTR__readonly = _vxgraph_tracker__readonly_vertices_as_cstring_CS( self, &nr );
        if( CSTR__writable || CSTR__readonly ) {
          __format_error_string( CSTR__error, "Cannot serialize when current thread holds %lld vertex locks ( writable:[%s]  readonly:[%s] )",
            nw + nr,
            CSTR__writable ? CStringValue( CSTR__writable ) : "",
            CSTR__readonly ? CStringValue( CSTR__readonly ) : ""
          );
          if( CSTR__writable ) {
            CStringDelete( CSTR__writable );
          }
          if( CSTR__readonly ) {
            CStringDelete( CSTR__readonly );
          }
        }
        else {
          __set_error_string( CSTR__error, "Cannot serialize when current thread holds vertex locks. (Failed to gather vertex information.)" );
        }
        // Sorry, current thread holds vertex locks
        serialization_can_proceed = false;
      }
      // Check if this is SYSTEM graph and if consumer service is running
      else if( iSystem.IsSystemGraph( self ) && iOperation.System_OPEN.ConsumerService.BoundPort( iSystem.GetSystemGraph() ) ) {
        __set_error_string( CSTR__error, "Cannot serialize SYSTEM graph while consumer service is running" );
        serialization_can_proceed = false;
      }

      // PROCEED - no vertex locks held by current thread
      if( serialization_can_proceed ) {
        // Already readonly? Will be recorded in the save state
        readonly = _vgx_is_readonly_CS( &self->readonly );

        // Acquire the graph readonly during serialization
        if( _vxgraph_state__acquire_graph_readonly_CS( self, false, timing_budget ) < 1 ) {
          int rodis = _vgx_get_disallow_readonly_recursion_CS( &self->readonly );
          int64_t wlc = _vgx_graph_get_vertex_WL_count_CS( self );
          __format_error_string( CSTR__error, "Cannot serialize, unable to acquire graph '%s' readonly (%03X). (WL vertices: %lld, RO disallowed: %d)", path, timing_budget->reason, wlc, rodis );
          // Sorry, write locks exist by other threads.
          serialization_can_proceed = false;
        }

        // PROCEED - we have the graph readonly and ready to serialize!
        if( serialization_can_proceed ) {
          // Mark graph as being serialized
          _vgx_set_serializing_CS( &self->readonly );
        }
      }
    }
  } GRAPH_RELEASE;

  // We are ready to serialize!
  if( serialization_can_proceed ) {

    // Serialize
    if( force || dirty ) {
      char message[256] = {0};
      snprintf( message, 255, "Serializing: %s", path );
      iSerialization.PrintGraphCounts_ROG( self, message );
      char previous[MAX_PATH+1];
      XTRY {
        const char *tmpbase = NULL;

        // Move existing graph directory, new persist will re-create all files without the possibility of corrupting previous files
        if( force ) {
          snprintf( previous, MAX_PATH, "%s.%lld", path, self->persisted_ts );
          previous[MAX_PATH] = '\0';
          _vxvertex_property__virtual_properties_close( self );
          tmpbase = previous;
          if( rename( path, previous ) != 0 ) {
            VXDURABLE_SERIALIZATION_REASON( self, 0xB26, "Directory rename failed (%s), will overwrite existing: %s", strerror(errno), path );
            tmpbase = NULL;
          }
          if( _vxvertex_property__virtual_properties_open( self, tmpbase ) < 0 ) {
            VXDURABLE_SERIALIZATION_CRITICAL( self, 0xB27, "Failed to reopen virtual properties file!" );
          }
        }

        // TODO: Create a persistent metas file within the graph structure (not the registry)
        //       that will hold important graph information to be restored, such as opcount and
        //       maybe other things. Right now it is in the REGISTRY and the opcount doesn't get
        //       saved when we save a graph because the registry doesn't get saved here.

        // [11] similarity
        EVAL_OR_THROW( CALLABLE( self->similarity )->BulkSerialize( self->similarity, force ), 0xB28 );
        
        // [17] property_allocator_context allocator
        cxmalloc_family_t *property_allocator = (cxmalloc_family_t*)self->property_allocator_context->allocator;
        value = CALLABLE( property_allocator )->Bytes( property_allocator );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB29, "Serializing: properties (%lld bytes)", value );
        EVAL_OR_THROW( CALLABLE( property_allocator )->BulkSerialize( property_allocator, force ), 0xB2A );

        // [18 - 23] enumerators
        int64_t enum_qwords = __serialize_enumerators( self, path, force, CSTR__error );
        if( enum_qwords < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xB2B );
        }
        __NQWORDS += enum_qwords;

        // [24] vertex_allocator
        value = CALLABLE( self->vertex_allocator )->Bytes( self->vertex_allocator );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB2C, "Serializing: vertices and arcs (%lld bytes)", value );
        EVAL_OR_THROW( CALLABLE( self->vertex_allocator )->BulkSerialize( self->vertex_allocator, force ), 0xB2D );

        // [25] vxtable
        value = CALLABLE( self->vxtable )->Items( self->vxtable );
        VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB2E, "Serializing: global vertex index (%lld vertices)", value );
        EVAL_OR_THROW( CALLABLE( self->vxtable )->BulkSerialize( self->vxtable, force ), 0xB2F );

        // [26] vxtypeidx
        for( vgx_vertex_type_t vxtype = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype++ ) {
          framehash_t *index = self->vxtypeidx[ vxtype ];
          if( index ) {
            int64_t n_items = CALLABLE( index )->Items( index );
            // Empty, delete file from disk
            if( n_items == 0 ) {
              if( CALLABLE( index )->Erase( index ) < 0 ) {
                VXDURABLE_SERIALIZATION_WARNING( self, 0xB30, "Failed to remove type index [%02x] from disk", vxtype );
              }
            }
            // Non-empty, serialize memory to file
            else if( iEnumerator_CS.VertexType.ExistsEnum( self, vxtype ) ) {
#ifdef HASVERBOSE
              const CString_t *CSTR__vxtype = iEnumerator_CS.VertexType.Decode( self, vxtype );
              VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB31, "Serializing: vertex index type '%s' (%lld vertices)", (CSTR__vxtype ? CStringValue( CSTR__vxtype ) : "?"), n_items );
#endif
              EVAL_OR_THROW( CALLABLE( index )->BulkSerialize( index, force ), 0xB32 );
            }
            //
            else {
              VXDURABLE_SERIALIZATION_REASON( self, 0xB33, "Type index [%02x] maps %lld vertices but no type enumeration exists", vxtype, n_items );
            }
          }
        }

        // Graph state
        int64_t summary_qwords = __serialize_state( self, ts_start, readonly );
        if( summary_qwords < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xB34 );
        }
        __NQWORDS += summary_qwords;

#ifdef VGX_CONSISTENCY_CHECK
        if( CALLABLE( self )->advanced->DebugCheckAllocators( self, NULL ) < 0 ) {
          THROW_CRITICAL_MESSAGE( CXLIB_ERR_CORRUPTION, 0xB35, "Allocators corrupted after persist" );
        }
#endif

        // Virtual properties commit point
        if( _vxvertex_property__virtual_properties_commit( self ) < 0 ) {
          THROW_CRITICAL_MESSAGE( CXLIB_ERR_CORRUPTION, 0xB36, "Failed to commit virtual properties" );
        }

        // End    
        int64_t t1 = __GET_CURRENT_MILLISECOND_TICK();
        double tp = (t1 - t0) / 1000.0;
        VXDURABLE_SERIALIZATION_INFO( self, 0xB37, "Serialization complete (%.1f seconds)", tp );

        if( force && strlen( previous ) > 0 && dir_exists( previous ) ) {
          // Move virtual properties
          if( tmpbase ) {
            if( _vxvertex_property__virtual_properties_move( self, tmpbase ) < 0 ) {
              THROW_CRITICAL_MESSAGE( CXLIB_ERR_GENERAL, 0xB38, "Failed to move virtual properties" );
            }
          }

          // Clean up
          delete_dir( previous );
        }

      }
      XCATCH( errcode ) {
        // Restore previous if it exists
        if( strlen( previous ) > 0 && dir_exists( previous ) ) {
          if( delete_dir( path ) != 0 || rename( previous, path ) != 0 ) {
            VXDURABLE_SERIALIZATION_REASON( self, 0xB39, "Failed to restore previous snapshot: %s", previous );
          }
        }
        else {
          VXDURABLE_SERIALIZATION_WARNING( self, 0xB3A, "No previous data can be restored" );
        }

        __format_error_string( CSTR__error, "Error during serialization of '%s': errcode=0x%x", path, errcode );
        __NQWORDS = -1;
      }
      XFINALLY {
      }

    }
    else {
      VXDURABLE_SERIALIZATION_VERBOSE( self, 0xB3B, "Serialization skipped, graph is clean." );
    }

    // Release graph
    GRAPH_LOCK( self ) {

      if( _vgx_is_serializing_CS( &self->readonly ) ) {
        // Release readonly state of graph and clear serializing flag
        _vxgraph_state__release_graph_readonly_CS( self );
        _vgx_clear_serializing_CS( &self->readonly );
      }
     
      SIGNAL_VERTEX_AVAILABLE( self );

      // Capture and transmit save operation to remote attached instances, if requested
      if( remote && _vgx_is_writable_CS( &self->readonly ) ) {
        if( iOperation.Graph_CS.Persist( self, &counts, force ) < 0 ) {
          VXDURABLE_SERIALIZATION_CRITICAL( self, 0xB3C, "Failed to capture persist operation" );
        }
        if( iOperation.Graph_CS.State( self, &counts ) < 0 ) {
          VXDURABLE_SERIALIZATION_CRITICAL( self, 0xB3D, "Failed to capture state operation" );
        }
        iOperation.Graph_CS.SetModified( self );
        if( COMMIT_GRAPH_OPERATION_CS( self ) < 0 ) {
          VXDURABLE_SERIALIZATION_CRITICAL( self, 0xB3E, "Failed to capture persist operation" );
        }
      }

      // Log appropriate message
      if( serialization_can_proceed ) {
        int readers = _vgx_get_readonly_readers_CS( &self->readonly );
        if( readers > 0 ) {
          VXDURABLE_SERIALIZATION_INFO( self, 0xB3F, "Still READONLY after serialization (reader(s): %d)", readers );
        }
      }
    } GRAPH_RELEASE;

    //
    if( !iSystem.IsSystemGraph( self ) ) {
      vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
      if( SYSTEM ) {
        _vxdurable_operation_consumer_service__perform_disk_cleanup_OPEN( SYSTEM );
      }
    }
    
  }
  // Not able to serialize - errors have been set earlier.
  else {
    __NQWORDS = -1;
  }

  return __NQWORDS;

}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_serialization.h"

test_descriptor_t _vgx_vxdurable_serialization_tests[] = {
  { "VGX Graph Serialization Tests", __utest_vxdurable_serialization },
  {NULL}
};
#endif
