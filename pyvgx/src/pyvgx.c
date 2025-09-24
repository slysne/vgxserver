/*######################################################################
 *#
 *# pyvgx.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"
#include "versiongen.h"
#include <signal.h>


#ifndef PYVGX_VERSION
#define PYVGX_VERSION ?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "pyvgx", VERSIONGEN_XSTR( PYVGX_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "pyvgx", VERSIONGEN_XSTR( PYVGX_VERSION ) );
static const char *g_build_info = GENERATE_BUILD_INFO;

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );

DLL_HIDDEN PyObject *g_pyvgx = NULL;

DLL_HIDDEN int64_t _time_init = 0;

DLL_HIDDEN vgx_context_t *_vgx_context = NULL;

static CTokenizer_t *g_tokenizer = NULL;
static CTokenizer_t *g_tokenizer_min = NULL;
static int __create_default_tokenizer( void );
static int __create_default_tokenizer_min( void );
static void __delete_default_tokenizer( void );
static void __delete_default_tokenizer_min( void );

DLL_HIDDEN vgx_StringList_t *_default_URIs = NULL;

static FILE * g_output_stream = NULL;

static LogContext_t *g_accesslog_context = NULL;


DLL_HIDDEN __THREAD PyThreadState * PYVGX_THREAD_STATE = NULL;


DLL_HIDDEN bool _module_owns_registry = false;
DLL_HIDDEN bool _registry_loaded = false;
DLL_HIDDEN __THREAD uint64_t _pyvertex_generation_guard = 0;

DLL_HIDDEN bool _pyvgx_api_enabled = true;


DLL_HIDDEN PyObject *g_py_plugins = NULL;

DLL_HIDDEN PyObject *g_py_cfdispatcher = NULL;

DLL_HIDDEN bool _auto_arc_timestamps = false;

DLL_HIDDEN PyObject * g_py_zero = NULL;
DLL_HIDDEN PyObject * g_py_one = NULL;
DLL_HIDDEN PyObject * g_py_minus_one = NULL;
DLL_HIDDEN PyObject * g_py_char_w = NULL;
DLL_HIDDEN PyObject * g_py_char_a = NULL;
DLL_HIDDEN PyObject * g_py_char_r = NULL;
DLL_HIDDEN PyObject * g_py_noargs = NULL;
DLL_HIDDEN PyObject * g_py_SYS_prop_id = NULL;
DLL_HIDDEN PyObject * g_py_SYS_prop_type = NULL;
DLL_HIDDEN PyObject * g_py_SYS_prop_mode = NULL;

static bool g_ena_selftest = true;


DLL_HIDDEN PyVGX_Similarity *_global_default_similarity = NULL;
DLL_HIDDEN PyVGX_System *_global_system_object = NULL;
DLL_HIDDEN PyVGX_Operation *_global_operation_object = NULL;

DLL_HIDDEN PyObject * PyVGX_VertexError = NULL;
DLL_HIDDEN PyObject * PyVGX_EnumerationError = NULL;
DLL_HIDDEN PyObject * PyVGX_ArcError = NULL;
DLL_HIDDEN PyObject * PyVGX_AccessError = NULL;
DLL_HIDDEN PyObject * PyVGX_QueryError = NULL;
DLL_HIDDEN PyObject * PyVGX_SearchError = NULL;
DLL_HIDDEN PyObject * PyVGX_ResultError = NULL;
DLL_HIDDEN PyObject * PyVGX_RequestError = NULL;
DLL_HIDDEN PyObject * PyVGX_ResponseError = NULL;
DLL_HIDDEN PyObject * PyVGX_PluginError = NULL;
DLL_HIDDEN PyObject * PyVGX_InternalError = NULL;
DLL_HIDDEN PyObject * PyVGX_OperationTimeout = NULL;
DLL_HIDDEN PyObject * PyVGX_DataError = NULL;





/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __initialize( void ) {
  g_output_stream = stderr;

  if( _vgx_context == NULL ) {
    if( (_vgx_context = vgx_INIT( NULL )) == NULL ) {
      return -1;
    }
  }
  
  if( __create_default_tokenizer() < 0 ) {
    return -1;
  }

  if( __create_default_tokenizer_min() < 0 ) {
    return -1;
  }

  if( (_default_URIs = iString.List.New( NULL, 0 )) == NULL ) {
    return -1;
  }

  if( iPyVGXParser.Initialize() < 0 ) {
    return -1;
  }

  _time_init = __SECONDS_SINCE_1970();

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __uninit( void ) {
  // NOTE: This cannot be called on shutdown because we get tstate NULL pointer.
  __pyvgx_plugin__delete();
  _ipyvgx_builder__delete();
  _ipyvgx_codec__delete();
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __destroy( void ) {
  if( g_accesslog_context ) {
    COMLIB__DeleteLogContext( &g_accesslog_context );
  }

  iPyVGXParser.Destroy();
  __delete_default_tokenizer();
  __delete_default_tokenizer_min();
  __delete_default_URIs();

  vgx_DESTROY( &_vgx_context );

  if( getenv( "PYVGX_NOBANNER" ) == NULL ) {
    int64_t t_alive = __SECONDS_SINCE_1970() - _time_init;
    PRINT_OSTREAM( "Exit %s, alive for %lld seconds\n", g_version_info, t_alive );
  }
  
  if( g_output_stream != stderr && g_output_stream != NULL ) {
    CX_FCLOSE( g_output_stream );
    g_output_stream = NULL;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define USE_WINAPI_SIGNAL_HANDLER
#if defined (CXPLAT_WINDOWS_X64) && defined (USE_WINAPI_SIGNAL_HANDLER)
BOOL WINAPI __ignore_ctrl_c_event( DWORD s ) {
  PYVGX_API_WARNING( "pyvgx", 0x000, "Shutdown cannot be interrupted" );
  return s == CTRL_C_EVENT ? TRUE : FALSE;
}
#else
void __ignore_ctrl_c_event( int s ) {
  PYVGX_API_WARNING( "pyvgx", 0x000, "Shutdown cannot be interrupted" );
  PyOS_setsig( SIGINT, __ignore_ctrl_c_event );
}
#endif



/******************************************************************************
 *
 *
 ******************************************************************************
 */
void __shutdown( void ) {

  if( _module_owns_registry ) {
    // Disable SIGINT during shutdown
#if defined (CXPLAT_WINDOWS_X64) && defined (USE_WINAPI_SIGNAL_HANDLER)
    SetConsoleCtrlHandler( __ignore_ctrl_c_event, TRUE );
#else
    PyOS_sighandler_t ohandler = PyOS_setsig( SIGINT, __ignore_ctrl_c_event );
#endif
    PYVGX_API_VERBOSE( "pyvgx", 0x000, "VGX shutting down..." );

    // Shutdown
    _registry_loaded = false;
    igraphfactory.Shutdown();

    PYVGX_API_VERBOSE( "pyvgx", 0x000, "VGX shutdown complete" );
    // Restore SIGINT
#if defined (CXPLAT_WINDOWS_X64) && defined (USE_WINAPI_SIGNAL_HANDLER)
    SetConsoleCtrlHandler( __ignore_ctrl_c_event, FALSE );
#else
    PyOS_setsig( SIGINT, ohandler );
#endif
  }

  __destroy();
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN void __delete_default_URIs( void ) {
  iString.List.Discard( &_default_URIs );
}



/******************************************************************************
 * PyVGX_version
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_version( PyObject *self, PyObject *args ) {
  PyObject *py_version = NULL;
  int verbosity = 0;

  if( !PyArg_ParseTuple( args, "|i", &verbosity) ) {
    return NULL;
  }

  const CString_t *CSTR__version = NULL;
  const CString_t *CSTR__vgx_version = NULL;
  
  BEGIN_PYVGX_THREADS {
    XTRY {

      switch( verbosity ) {
      case 0:
        CSTR__version = CStringNew( g_version_info );
        break;
      case 1:
        CSTR__version = CStringNew( g_version_info_ext );
        break;
      default:
        {
          if( (CSTR__vgx_version = igraphinfo.Version( verbosity )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x131 );
          }
          if( (CSTR__version = CStringNewFormat( "%s\n%s", g_version_info_ext, CStringValue( CSTR__vgx_version ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x132 );
          }
        }
      }

      BEGIN_PYTHON_INTERPRETER {
        py_version = PyUnicode_FromString( CStringValue( CSTR__version ) );
      } END_PYTHON_INTERPRETER;
    }
    XCATCH( errcode ) {
      PyVGXError_SetString( PyExc_Exception, "internal error" );
    }
    XFINALLY {
      if( CSTR__version ) {
        CStringDelete( CSTR__version );
      }
      if( CSTR__vgx_version ) {
        CStringDelete( CSTR__vgx_version );
      }
    }
  } END_PYVGX_THREADS;

  return py_version;
}



/******************************************************************************
 * PyVGX_popcnt
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_popcnt( PyObject *self, PyObject *py_num ) {
  uint64_t num;
  if( PyLong_CheckExact( py_num ) ) {
    num = PyLong_AsUnsignedLongLongMask( py_num );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "A number is required" );
    return NULL;
  }
  int c;
  BEGIN_PYVGX_THREADS {
    c = (int)__popcnt64( num );
  } END_PYVGX_THREADS;
  return PyLong_FromLong( c );
}



/******************************************************************************
 * PyVGX_ihash64
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_ihash64( PyObject *self, PyObject *py_num ) {
  uint64_t num;
  if( PyLong_CheckExact( py_num ) ) {
    num = PyLong_AsUnsignedLongLongMask( py_num );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "A number is required" );
    return NULL;
  }
  uint64_t h64;
  BEGIN_PYVGX_THREADS {
    h64 = ihash64( (uint64_t)num );
  } END_PYVGX_THREADS;
  return PyLong_FromUnsignedLongLong( h64 );
}



/******************************************************************************
 * PyVGX_ihash128
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_ihash128( PyObject *self, PyObject *py_num ) {
  uint64_t num;
  if( PyLong_CheckExact( py_num ) ) {
    num = PyLong_AsUnsignedLongLongMask( py_num );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "A number is required" );
    return NULL;
  }
  objectid_t h128;
  char idbuf[33];
  BEGIN_PYVGX_THREADS {
    h128 = ihash128( (uint64_t)num );
    idtostr( idbuf, &h128 );
  } END_PYVGX_THREADS;
  return PyUnicode_FromStringAndSize( idbuf, 32 );
}



/******************************************************************************
 * PyVGX_strhash64
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_strhash64( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
  if( (str = PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
    return NULL;
  }
  else {
    int64_t h64;

    BEGIN_PYVGX_THREADS {
      h64 = hash64( (const unsigned char*)str, sz );
    } END_PYVGX_THREADS;
    return PyLong_FromLongLong( h64 );
  }
}



/******************************************************************************
 * PyVGX_strhash128
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_strhash128( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
  if( (str = PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
    return NULL;
  }
  else {
    objectid_t obid;
    char buf[33];

    BEGIN_PYVGX_THREADS {
      obid = hash128( (const unsigned char*)str, sz );
      idtostr( buf, &obid );
    } END_PYVGX_THREADS;

    return PyUnicode_FromStringAndSize( buf, 32 );
  }
}



/******************************************************************************
 * PyVGX_md5
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_md5( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
  if( (str = PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
    return NULL;
  }
    
  if( sz > UINT_MAX ) {
    PyVGXError_SetString( PyExc_ValueError, "Input string too large" );
    return NULL;
  }

  char buf[33];
  objectid_t obid;
  BEGIN_PYVGX_THREADS {
    obid = md5_len( str, (unsigned int)sz );
    idtostr( buf, &obid );
  } END_PYVGX_THREADS;

  return PyUnicode_FromStringAndSize( buf, 32 );

}



/******************************************************************************
 * PyVGX_sha256
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_sha256( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
  if( (str = PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
    return NULL;
  }
    
  if( sz > UINT_MAX ) {
    PyVGXError_SetString( PyExc_ValueError, "Input string too large" );
    return NULL;
  }
  
  char buf[65];
  sha256_t hash;
  BEGIN_PYVGX_THREADS {
    hash = sha256_len( str, sz );
    sha256tostr( buf, &hash );
  } END_PYVGX_THREADS;

  return PyUnicode_FromStringAndSize( buf, 64 );
}



/******************************************************************************
 * PyVGX_crc32c
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_crc32c( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str = NULL;
  Py_ssize_t sz = 0;
  Py_ssize_t ucsz;
  if( (str = PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
    return NULL;
  }
  unsigned int crc = crc32c( 0, str, sz );
  return PyLong_FromUnsignedLong( crc );
}



/******************************************************************************
 * PyVGX_rstr
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_rstr( PyObject *self, PyObject *py_n ) {
  Py_ssize_t n;
  if( !PyLong_Check( py_n ) || (n =PyLong_AsLongLong( py_n )) < 1 ) {
    PyErr_SetString( PyExc_TypeError, "a positive integer is required" );
    return NULL;
  }


  PyObject *py_str = PyUnicode_New( n, 127 );
  if( py_str == NULL ) {
    return NULL;
  }

  char *p = PyUnicode_DATA( py_str );
  char *end = p + n;
  BEGIN_PYVGX_THREADS {
    while( p < end ) {
      *p++ = 'a' + (rand63() % 26);
    }
  } END_PYVGX_THREADS;

  return py_str;
}



/******************************************************************************
 * PyVGX_vgxrpndefs
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_vgxrpndefs( PyObject *self ) {
  PyObject *py_defs = NULL;
  vgx_StringList_t *defs = iEvaluator.GetRpnDefinitions();
  if( defs ) {
    if( (py_defs = PyDict_New()) != NULL ) {
      int64_t sz = iString.List.Size( defs );
      for( int64_t i=0; i<sz; i++ ) {
        const char *str = iString.List.GetChars( defs, i );
        const char *p = str;
        while( *p && *p != ':' ) {
          ++p;
        }
        if( *p == ':' ) {
          ++p;
        }
        while( *p && *p == ' ' ) {
          ++p;
        }
        const char *name = p;
        while( *p && *p != '(' ) {
          ++p;
        }
        PyObject *py_name = PyUnicode_FromStringAndSize( name, p-name );
        PyObject *py_str = PyUnicode_FromString( str );
        if( py_name && py_str ) {
          PyDict_SetItem( py_defs, py_name, py_str );
        }
        Py_XDECREF( py_name );
        Py_XDECREF( py_str );
      }
    }
    iString.List.Discard( &defs );
  }

  if( py_defs == NULL && !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
  }

  return py_defs;
}



/******************************************************************************
 * __create_default_tokenizer
 *
 ******************************************************************************
 */
static int __create_default_tokenizer( void ) {
  int ret = 0;
  if( g_tokenizer == NULL ) {
    BEGIN_PYVGX_THREADS {

      CTokenizer_constructor_args_t tokargs = {
        .keep_digits        = 1,
        .normalize_accents  = 0,
        .lowercase          = 0,
        .keepsplit          = 1,
        .strict_utf8        = 0,
        .overrides          = {0}
      };


      if( (g_tokenizer = COMLIB_OBJECT_NEW( CTokenizer_t, "Generic Tokenizer", &tokargs )) == NULL ) {
        ret = -1;
      }
      else {
        ret = 1;
      }
    } END_PYVGX_THREADS;
  }
  return ret;
}



/******************************************************************************
 * __create_default_tokenizer_min
 *
 ******************************************************************************
 */
static int __create_default_tokenizer_min( void ) {
  int ret = 0;

  unicode_codepoint_t ucKEEP[] ={
    '#',  '$', '@', '_',
    0
  };

  unicode_codepoint_t ucSKIP[] ={
    0
  };

//  '!' '"' '#' '$' '%' '&' '\'' '(' ')' '*' '+' ',' '-' '.' '/' 
//  ':' ';' '<' '=' '>' '?'
//  @ A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
//  '[' '\\' ']' '^' '_' '`'
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  '{' '|' ''} '~'

  unicode_codepoint_t ucSPLIT[] ={
    '!',  '"',  
    '%',        '\'', '(',
    ')',  '*',  '+',  ',',
    '-',  '.',  '/',  ':',
    ';',  '<',  '=',  '>',
    '?',  '[',  '\\', ']',
    '^',        '`',  '{',
    '|',  '}',  '~',
    0
  };

  unicode_codepoint_t ucIGNORE[] ={
    0
  };

  unicode_codepoint_t ucCOMBINE[] ={
    '&'
  };

  if( g_tokenizer_min == NULL ) {
    BEGIN_PYVGX_THREADS {

      CTokenizer_constructor_args_t tokargs ={
        .keep_digits        = 1,
        .normalize_accents  = 1,
        .lowercase          = 1,
        .keepsplit          = 0,
        .strict_utf8        = 0,
        .overrides          = {
              .keep               = ucKEEP,
              .skip               = ucSKIP,
              .split              = ucSPLIT,
              .ignore             = ucIGNORE,
              .combine            = ucCOMBINE
        }
      };

      if( (g_tokenizer_min = COMLIB_OBJECT_NEW( CTokenizer_t, "Generic Tokenizer Min", &tokargs )) == NULL ) {
        ret = -1;
      }
      else {
        ret = 1;
      }
    } END_PYVGX_THREADS;
  }
  return ret;
}



/******************************************************************************
 * __delete_default_tokenizer
 *
 ******************************************************************************
 */
static void __delete_default_tokenizer( void ) {
  if( g_tokenizer ) {
    COMLIB_OBJECT_DESTROY( g_tokenizer );
    g_tokenizer = NULL;
  }
}



/******************************************************************************
 * __delete_default_tokenizer_min
 *
 ******************************************************************************
 */
static void __delete_default_tokenizer_min( void ) {
  if( g_tokenizer_min ) {
    COMLIB_OBJECT_DESTROY( g_tokenizer_min );
    g_tokenizer_min = NULL;
  }
}



/******************************************************************************
 * __PyVGX_tokenize
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_tokenize( PyObject *self, PyObject *py_str, CTokenizer_t *tokenizer ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  PyObject *py_tokens = NULL;

  const char *utf8_str;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
 
  if( (utf8_str = PyVGX_PyObject_AsUTF8AndSize( py_str, &sz, &ucsz, NULL )) == NULL ) {
    return NULL;
  }
    
  if( sz > INT_MAX ) {
    PyVGXError_SetString( PyExc_ValueError, "Input string too large" );
    return NULL;
  }
  tokenmap_t *tokenmap = NULL;

  PyObject *py_tok = NULL;

  CTokenizer_vtable_t *iTokenizer = CALLABLE( tokenizer );

  CString_t *CSTR__error = NULL;

  XTRY {
    BEGIN_PYVGX_THREADS {
      tokenmap = iTokenizer->Tokenize( tokenizer, (BYTE*)utf8_str, &CSTR__error );
    } END_PYVGX_THREADS;

    if( tokenmap == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x141 );
    }

    int32_t ntok = tokenmap->ntok;
    if( (py_tokens = PyList_New( ntok )) != NULL ) {
      const BYTE *token = NULL;
      tokinfo_t tokinfo;
      int32_t i=0;
      while( ( token = iTokenizer->GetTokenAndInfo( tokenizer, tokenmap, &tokinfo ) ) != NULL ) {
        if( (py_tok = PyUnicode_FromStringAndSize( (char*)token, tokinfo.len )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x142 );
        }
        PyList_SET_ITEM( py_tokens, i++, py_tok );
      }
    }
  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      if( CSTR__error ) {
        PyErr_Format( PyExc_Exception, "Tokenizer error: %s", CStringValue( CSTR__error ) );
      }
      else {
        PyErr_SetString( PyExc_Exception, "Tokenizer error" );
      }
    }
  }
  XFINALLY {
    if( tokenmap ) {
      iTokenizer->DeleteTokenmap( tokenizer, &tokenmap );
    }
    iString.Discard( &CSTR__error );
  }

  return py_tokens;

}



/******************************************************************************
 * PyVGX_tokenize
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_tokenize( PyObject *self, PyObject *py_str ) {
  return __PyVGX_tokenize( self, py_str, g_tokenizer );
}



/******************************************************************************
 * PyVGX_tokenize_min
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_tokenize_min( PyObject *self, PyObject *py_str ) {
  return __PyVGX_tokenize( self, py_str, g_tokenizer_min );
}



/******************************************************************************
 * PyVGX_encode_utf8
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_encode_utf8( PyObject *self, PyObject *py_unicode ) {
  PyObject *py_encoded = NULL;
  if( PyUnicode_Check( py_unicode ) ) {
    int64_t sz = PyUnicode_GET_LENGTH( py_unicode );
    CDwordList_constructor_args_t args = {
      .element_capacity = sz,
      .comparator = NULL
    };

    CDwordList_t * uni = COMLIB_OBJECT_NEW( CDwordList_t, NULL, &args );
    if( uni ) {
      // HACK!
      DWORD *dest = uni->_wp;
      void *src_raw = PyUnicode_DATA( py_unicode );
      // Unicode codepoints can produce 1-4 utf-8 bytes.
      // The maximum number of utf-8 bytes possible depends on the UCS encoding.
      switch( PyUnicode_KIND( py_unicode ) ) {
      case PyUnicode_1BYTE_KIND:
        {
          Py_UCS1 *src = (Py_UCS1*)src_raw;
          Py_UCS1 *end = src + sz;
          while( src < end ) {
            *dest++ = *src++;
          }
        }
        break;
      case PyUnicode_2BYTE_KIND:
        {
          Py_UCS2 *src = (Py_UCS2*)src_raw;
          Py_UCS2 *end = src + sz;
          while( src < end ) {
            *dest++ = *src++;
          }
        }
        break;
      case PyUnicode_4BYTE_KIND:
        {
          Py_UCS4 *src = (Py_UCS4*)src_raw;
          Py_UCS4 *end = src + sz;
          while( src < end ) {
            *dest++ = *src++;
          }
        }
        break;
      default:
        PyErr_SetString( PyExc_TypeError, "a unicode object is required" );
        COMLIB_OBJECT_DESTROY( uni );
        return NULL;
      }

      uni->_wp = dest;
      uni->_size = sz;

      BYTE *enc;

      int64_t len = 0;
      int64_t errpos = -1;

      BEGIN_PYVGX_THREADS {
        enc = COMLIB_encode_utf8( uni, &len, &errpos );
      } END_PYVGX_THREADS;

      if( enc ) {
        py_encoded = PyBytes_FromStringAndSize( (char*)enc, len );
        ALIGNED_FREE( enc );
      }
      else {
        PyErr_Format( PyExc_UnicodeError, "encoder error at position %lld", errpos );
      }

      COMLIB_OBJECT_DESTROY( uni );
    }
    else {
      PyErr_SetNone( PyExc_MemoryError );
    }
  }
  else {
    PyErr_SetString( PyExc_TypeError, "a unicode object is required" );
  }

  return py_encoded;
}



/******************************************************************************
 * PyVGX_decode_utf8
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_decode_utf8( PyObject *self, PyObject *py_string ) {
  PyObject *py_unicode = NULL;
  if( PyBytes_CheckExact( py_string ) ) {
    const char *string = PyBytes_AS_STRING( py_string );
    int64_t errpos = -1;
    Unicode uni;
    if( (uni = COMLIB_decode_utf8( (BYTE*)string, &errpos )) != NULL ) {
      int64_t len = CALLABLE( uni )->Length( uni );
      DWORD *data = CALLABLE( uni )->Cursor( uni, 0 );
      DWORD *end = data + len;
      DWORD *src = data;
      Py_UCS4 m = 0;
      while( src < end ) {
        m |= *src++;
      }
      if( (py_unicode = PyUnicode_New( len, m )) != NULL ) {
        src = data;
        void *dest_raw = PyUnicode_DATA( py_unicode );
        // Unicode codepoints can produce 1-4 utf-8 bytes.
        // The maximum number of utf-8 bytes possible depends on the UCS encoding.
        switch( PyUnicode_KIND( py_unicode ) ) {
        case PyUnicode_1BYTE_KIND:
          {
            Py_UCS1 *dest = (Py_UCS1*)dest_raw;
            while( src < end ) {
              *dest++ = (Py_UCS1)*src++;
            }
          }
          break;
        case PyUnicode_2BYTE_KIND:
          {
            Py_UCS2 *dest = (Py_UCS2*)dest_raw;
            while( src < end ) {
              *dest++ = (Py_UCS2)*src++;
            }
          }
          break;
        case PyUnicode_4BYTE_KIND:
          {
            Py_UCS4 *dest = (Py_UCS4*)dest_raw;
            while( src < end ) {
              *dest++ = *src++;
            }
          }
          break;
        default:
          PyErr_SetString( PyExc_UnicodeError, "Unicode UCS error" );
          Py_DECREF( py_unicode );
          py_unicode = NULL;
        }
      }
      COMLIB_OBJECT_DESTROY( uni );
    }
    else {
      PyErr_Format( PyExc_UnicodeError, "decoder error at position %lld", errpos );
    }
  }
  else {
    PyErr_SetString( PyExc_TypeError, "a bytes-like object is required" );
  }

  return py_unicode;
}



/******************************************************************************
 * PyVGX_serialize
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_serialize( PyObject *self, PyObject *py_obj ) {
  return iPyVGXCodec.NewSerializedPyBytesFromPyObject( py_obj );
}



/******************************************************************************
 * PyVGX_deserialize
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_deserialize( PyObject *self, PyObject *py_bytes ) {
  return iPyVGXCodec.NewPyObjectFromSerializedPyBytes( py_bytes );
}



/******************************************************************************
 * PyVGX_compress
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_compress( PyObject *self, PyObject *py_obj ) {
  return iPyVGXCodec.NewCompressedPyBytesFromPyObject( py_obj );
}



/******************************************************************************
 * PyVGX_decompress
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_decompress( PyObject *self, PyObject *py_bytes ) {
  return iPyVGXCodec.NewPyObjectFromCompressedPyBytes( py_bytes );
}



/******************************************************************************
 * PyVGX_timestamp
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_timestamp( PyObject *self ) {
  double t_sec;
  BEGIN_PYVGX_THREADS {
    t_sec = __GET_CURRENT_NANOSECOND_TICK() / 1000000000.0;
  } END_PYVGX_THREADS;
  return PyFloat_FromDouble( t_sec );
}



/******************************************************************************
 * PyVGX_threadlabel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_threadlabel( PyObject *self, PyObject *py_label ) {
  PyObject *py_ret = NULL;
  if( PyVGX_PyObject_CheckString( py_label ) ) {
    py_ret = PyUnicode_FromString( SET_CURRENT_THREAD_LABEL( PyVGX_PyObject_AsString( py_label ) ) );
  }
  else if( py_label == Py_None ) {
    char label[16] = {0};
    py_ret = PyUnicode_FromString( GET_CURRENT_THREAD_LABEL( label ) );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "String or None required" );
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_threadinit
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_threadinit( PyObject *self, PyObject *args ) {
  int64_t seed = 0;

  if( !PyArg_ParseTuple( args, "|L", &seed) ) {
    return NULL;
  }

  uint64_t h64 = ihash64( seed + GET_CURRENT_THREAD_ID() );

  __lfsr_init( h64 );
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_threadid
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_threadid( PyObject *self ) {
  return PyLong_FromUnsignedLong( GET_CURRENT_THREAD_ID() );
}



/******************************************************************************
 * PyVGX_selftest
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_selftest( PyObject *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "testroot", "library", "names", "force", NULL }; 

  if( g_ena_selftest == false ) {
    Py_RETURN_NONE;
  }

  static const char *default_testroot = "./pyvgx_selftest";

  const char *testroot = default_testroot;
  int64_t sz_testroot = strlen( testroot );
  const char *library = NULL;
  int64_t sz_library = 0;
  PyObject *py_testnames = NULL;
  int force = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s#s#Oi", kwlist, &testroot, &sz_testroot, &library, &sz_library, &py_testnames, &force ) ) {
    return NULL;
  }

  if( dir_exists( testroot ) ) {
    // Require manual deletion of testdir unless default dir is used (to avoid accidents)
    if( force > 0 || testroot == default_testroot ) {
      if( delete_dir( testroot ) < 0 ) {
        PyErr_Format( PyExc_Exception, "Failed to remove directory '%s' ", testroot );
        return NULL;
      }
    }
    else {
      PyErr_Format( PyExc_Exception, "Remove directory '%s' before running unit tests", testroot );
      return NULL;
    }
  }

  const char **testnames = NULL;

  if( py_testnames != NULL ) {

    if( !PyList_Check( py_testnames ) ) {
      PyVGXError_SetString( PyExc_TypeError, "test names must be a list of strings" );
      return NULL;
    }

    int64_t sz = PyList_Size( py_testnames );
    for( int64_t i=0; i<sz; i++ ) {
      if( !PyUnicode_Check( PyList_GET_ITEM( py_testnames, i ) ) ) {
        PyVGXError_SetString( PyExc_TypeError, "test names must be a list of strings" );
        return NULL;
      }
    }

    testnames = calloc( sz+1, sizeof(char*) );

    for( int64_t i=0; i<sz; i++ ) {
      const char *name = PyUnicode_AsUTF8( PyList_GET_ITEM( py_testnames, i ) );
      testnames[i] = name;
    }
  }

  int ret = 0;
  CString_t *CSTR__err = NULL;

  BEGIN_PYVGX_THREADS {
    if( library == NULL || CharsEqualsConst( library, "vgx" ) ) {
      ret = vgx_unit_tests( testnames, testroot );
    }
    else if( CharsEqualsConst( library, "comlib" ) ) {
      ret = comlib_unit_tests( testnames, testroot );
    }
    else if( CharsEqualsConst( library, "framehash" ) ) {
      ret = iFramehash.test.RunUnitTests( testnames, testroot );
    }
    else {
      ret = -1;
      CSTR__err = CStringNewFormat( "Not a valid library '%s'. Valid libraries: comlib, framehash, vgx", library );
    }

  } END_PYVGX_THREADS;

  if( testnames ) {
    free( (char**)testnames );
  }

  if( CSTR__err ) {
    PyVGXError_SetString( PyExc_Exception, CStringValue( CSTR__err ) );
    iString.Discard( &CSTR__err );
  }

  // Success
  if( ret == 0 ) {
    Py_RETURN_NONE;
  }
  else {
    if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_Exception, "TESTS FAILED!" );
    }
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_selftest_all
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_selftest_all( PyObject *self, PyObject *py_testroot ) {
  if( g_ena_selftest == false ) {
    Py_RETURN_NONE;
  }

  if( !PyUnicode_Check( py_testroot ) ) {
    PyVGXError_SetString( PyExc_ValueError, "A string argument is required" );
    return NULL;
  }

  const char *testroot = PyUnicode_AsUTF8( py_testroot );
  int ret;
  int64_t t0 = __SECONDS_SINCE_1970();
  BEGIN_PYVGX_THREADS {
    if( (ret = comlib_unit_tests( NULL, testroot )) == 0 ) {
      int64_t t1 = __SECONDS_SINCE_1970();
      if( (ret = iFramehash.test.RunUnitTests( NULL, testroot )) == 0 ) {
        int64_t t2 = __SECONDS_SINCE_1970();
        if( (ret = vgx_unit_tests( NULL, testroot )) == 0 ) {
          // ok!
          int64_t t3 = __SECONDS_SINCE_1970();
          printf( "\n" );
          printf( "ALL TEST SUITES PASSED (%lld seconds)\n", t3-t0 );
          printf( "  COMLIB     (%lld seconds)\n", t1-t0 );
          printf( "  FRAMEHASH  (%lld seconds)\n", t2-t1 );
          printf( "  VGX        (%lld seconds)\n", t3-t2 );
          printf( "\n" );
        }
      }
    }
  } END_PYVGX_THREADS;

  if( ret == 0 ) {
    Py_RETURN_NONE;
  }
  else {
    PyVGXError_SetString( PyExc_Exception, "Tests failed!" );
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_enable_selftest
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_enable_selftest( PyObject *self, PyObject *py_flag ) {
  if( !PyLong_Check( py_flag ) ) {
    PyErr_SetString( PyExc_TypeError, "True or False required" );
    return NULL;
  }

  g_ena_selftest = (bool)PyLong_AsLong( py_flag );

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_profile
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_profile( PyObject *self ) {
  PyObject *py_ret = NULL;

  CString_t *CSTR__info = NULL;

  XTRY {
    CString_t *CSTR__tmp;

    #if defined CXPLAT_ARCH_X64
    int n_cores = 0;
    int n_threads = 0;
    iVGXProfile.CPU.GetCoreCount( &n_cores, &n_threads );
    int L2_kb = iVGXProfile.CPU.GetL2Size();
    int L2_ways = iVGXProfile.CPU.GetL2Associativity();

    CSTR__info = iVGXProfile.CPU.GetBrandString();
    if( CSTR__info == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    const char *brand = CStringValue( CSTR__info );
    CSTR__tmp = CStringNewFormat( "%s (%d cores / %d threads) (L2=%dkB %d-way)", brand, n_cores, n_threads, L2_kb, L2_ways );
    CStringDelete( CSTR__info );
    if( (CSTR__info = CSTR__tmp) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }
    #elif CXPLAT_ARCH_ARM64
    int n_P_cores = 0;
    int n_E_cores = 0;
    iVGXProfile.CPU.GetCoreCount( &n_P_cores, &n_E_cores );

    CSTR__info = iVGXProfile.CPU.GetBrandString();
    if( CSTR__info == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    const char *brand = CStringValue( CSTR__info );
    CSTR__tmp = CStringNewFormat( "%s (%d P-cores / %d E-cores)", brand, n_P_cores, n_E_cores );
    CStringDelete( CSTR__info );
    if( (CSTR__info = CSTR__tmp) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    #endif

    if( (py_ret = PyUnicode_FromString( CStringValue( CSTR__info ) )) != NULL ) {


    }

    CSTR__tmp = iVGXProfile.CPU.GetCacheInfo();
    if( CSTR__tmp ) {
      const char *info = CStringValue( CSTR__tmp );
      printf( "\nCACHE:\n%s\n", info );
      CStringDelete( CSTR__tmp );
    }

    CSTR__tmp = iVGXProfile.CPU.GetTLBInfo();
    if( CSTR__tmp ) {
      const char *info = CStringValue( CSTR__tmp );
      printf( "\nTLB:\n%s\n", info );
      CStringDelete( CSTR__tmp );
    }

    pyvgx_SystemProfile( 1LL<<24, 1LL<<21, false );
    
  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_ret );
    py_ret = NULL;
    PyErr_SetString( PyExc_Exception, "internal error" );
  }
  XFINALLY {
    if( CSTR__info ) {
      CStringDelete( CSTR__info );
    }
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_meminfo
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_meminfo( PyObject *self ) {
  PyErr_WarnEx( PyExc_DeprecationWarning, "use pyvgx.system.Meminfo()", 1 );
  static PyObject *py_system_Meminfo = NULL;
  if( py_system_Meminfo == NULL ) {
    py_system_Meminfo = PyObject_GetAttrString( (PyObject*)_global_system_object, "Meminfo" );
  }
  return PyObject_CallObject( py_system_Meminfo, NULL );
}



/******************************************************************************
 * PyVGX_cpuid
 *
 ******************************************************************************
 */
#if defined CXPLAT_ARCH_X64
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_cpuid( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = {"leaf", "subleaf", "obj", NULL}; 
  unsigned int leaf = 0;
  unsigned int subleaf = 0;
  int obj = 0;
  PyObject *py_ret = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "I|Ii", kwlist, &leaf, &subleaf, &obj ) ) {
    return NULL;
  }

  XTRY {
    int EAX = 0;
    int EBX = 0;
    int ECX = 0;
    int EDX = 0;
    
    cxplat_cpuidex( leaf, subleaf, &EAX, &EBX, &ECX, &EDX );

    // Return a simple tuple of numbers
    if( obj == 0 ) {
      if( (py_ret = PyTuple_New( 4 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      PyTuple_SetItem( py_ret, 0, PyLong_FromLong( EAX ) );
      PyTuple_SetItem( py_ret, 1, PyLong_FromLong( EBX ) );
      PyTuple_SetItem( py_ret, 2, PyLong_FromLong( ECX ) );
      PyTuple_SetItem( py_ret, 3, PyLong_FromLong( EDX ) );
    }
    // Return a richer object
    else {
      if( (py_ret = PyDict_New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

      char buf[64];

      iPyVGXBuilder.DictMapStringToInt( py_ret, "EAX", EAX );
      iPyVGXBuilder.DictMapStringToInt( py_ret, "EBX", EBX );
      iPyVGXBuilder.DictMapStringToInt( py_ret, "ECX", ECX );
      iPyVGXBuilder.DictMapStringToInt( py_ret, "EDX", EDX );

      PyObject *py_hex = PyDict_New();
      if( py_hex ) {
        iPyVGXBuilder.DictMapStringToString( py_hex, "EAX", write_term( write_hex_dword( buf, EAX ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_hex, "EBX", write_term( write_hex_dword( buf, EBX ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_hex, "ECX", write_term( write_hex_dword( buf, ECX ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_hex, "EDX", write_term( write_hex_dword( buf, EDX ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToPyObject( py_ret, "hex", &py_hex );
      }

      PyObject *py_bin = PyDict_New();
      if( py_bin ) {
        iPyVGXBuilder.DictMapStringToString( py_bin, "EAX", write_term( write_bin_dword( buf, EAX, " " ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_bin, "EBX", write_term( write_bin_dword( buf, EBX, " " ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_bin, "ECX", write_term( write_bin_dword( buf, ECX, " " ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToString( py_bin, "EDX", write_term( write_bin_dword( buf, EDX, " " ) ) ? buf : "?" );
        iPyVGXBuilder.DictMapStringToPyObject( py_ret, "bin", &py_bin );
      }

    }
    
  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_ret );
    py_ret = NULL;
    PyErr_SetString( PyExc_Exception, "internal error" );
  }
  XFINALLY {
  }

  return py_ret;
}
#endif



/******************************************************************************
 * PyVGX_avxbuild
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_avxbuild( PyObject *self ) {
#ifdef __cxlib_AVX512_MINIMUM__
  int avx = 512;
#elif __AVX2__
  int avx = 2;
#elif __AVX__
  int avx = 1;
#else
  int avx = 0;
#endif
  return PyLong_FromLong( avx );
}



/******************************************************************************
 * PyVGX_initadmin
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_initadmin( PyObject *self ) {
  if( __pyvgx_plugin__init_pyvgx() < 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static const char *__get_log_message( PyObject *py_message ) {
  if( PyVGX_PyObject_CheckString( py_message ) ) {
    return PyVGX_PyObject_AsString( py_message );
  }
  else if( py_message ) {
    return __get_log_message( PyObject_Repr( py_message ) );
  }
  else {
    return NULL;
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_set_output_stream( const char *filepath, CString_t **CSTR__error ) {

  cxlib_exc_context_t *context = COMLIB_GetExceptionContext();
  int ret = 0;
  SYNCHRONIZE_ON( context->lock ) {

    // Close previous file if any
    if( g_output_stream != stderr ) {
      if( g_output_stream != NULL ) {
        CX_FCLOSE( g_output_stream );
      }
      g_output_stream = stderr;
    }

    XTRY {
      if( filepath ) {
        if( COMLIB__OpenLog( filepath, &g_output_stream, NULL, CSTR__error ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }
      }

      COMLIB_SetOutputStream( g_output_stream );
    }
    XCATCH( errcode ) {
      __pyvgx_set_output_stream( NULL, NULL  );
      ret = -1;
    }
    XFINALLY {
    }
  } RELEASE;

  return ret;

}



/******************************************************************************
 * PyVGX_SetOutputStream
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_SetOutputStream( PyObject *self, PyObject *py_filepath ) {

  const char *filepath = NULL;
  if( py_filepath != Py_None ) {
    if( (filepath = PyUnicode_AsUTF8( py_filepath )) == NULL ) {
      return NULL;
    }
  }

  CString_t *CSTR__error = NULL;
  if( __pyvgx_set_output_stream( filepath, &CSTR__error ) < 0 ) {
    PyErr_SetString( PyExc_IOError, CStringValueDefault( CSTR__error, "unknown" ) );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_LogDebug
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_LogDebug( PyObject *self, PyObject *py_message ) {
#ifndef NDEBUG
  DEBUG( 0, "[PyVGX] %s", __get_log_message( py_message ) );
#endif
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_LogWarning
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_LogWarning( PyObject *self, PyObject *py_message ) {
  PYVGX_API_WARNING( "pyvgx", 0, "%s", __get_log_message( py_message ) );
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_LogInfo
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_LogInfo( PyObject *self, PyObject *py_message ) {
  PYVGX_API_INFO( "pyvgx", 0, "%s", __get_log_message( py_message ) );
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_LogError
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_LogError( PyObject *self, PyObject *py_message ) {
  PYVGX_API_REASON( "pyvgx", 0, "%s", __get_log_message( py_message ) );
  Py_RETURN_NONE;
}



/******************************************************************************
 * __update_accesslog_context
 *
 ******************************************************************************
 */
static void __update_accesslog_context( LogContext_t *context ) {
  BEGIN_PYTHON_INTERPRETER {
    g_accesslog_context = context;
  } END_PYTHON_INTERPRETER;
}



/******************************************************************************
 * __pyvgx_open_access_log
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_open_access_log( const char *filepath, CString_t **CSTR__error ) {
  LogContext_t *context = g_accesslog_context;
  if( context == NULL ) {
    if( (context =  COMLIB__NewLogContext()) == NULL ) {
      __set_error_string( CSTR__error, "async output task cannot start" );
      return -1;
    }
    __update_accesslog_context( context );
  }

  // New file
  if( filepath ) {
    if( COMLIB__LogRotate( context, filepath, CSTR__error ) < 0 ) {
      COMLIB__DeleteLogContext( &context );
      __update_accesslog_context( NULL );
      return -1;
    }
  }
  // No file (will block until queue flushed)
  else {
    COMLIB__DeleteLogContext( &context );
    __update_accesslog_context( NULL );
  }
  return 0;
}



/******************************************************************************
 * PyVGX_OpenAccessLog
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_OpenAccessLog( PyObject *self, PyObject *py_filepath ) {

  if( py_filepath != NULL ) {
    if( py_filepath != Py_None && !PyUnicode_Check( py_filepath ) ) {
      PyErr_SetString( PyExc_TypeError, "filepath must be string or None" );
      return NULL;
    }
  }

  const char *filepath = NULL;
  if( py_filepath != NULL && py_filepath != Py_None ) {
    filepath = PyUnicode_AsUTF8( py_filepath );
  }

  CString_t *CSTR__error = NULL;
  if( __pyvgx_open_access_log( filepath, &CSTR__error ) < 0 ) {
     PyErr_Format( PyExc_Exception, "Access log error: %s", CStringValueDefault( CSTR__error, "unknown" ) );
     iString.Discard( &CSTR__error );
     return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_LogTimestamp
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_LogTimestamp( PyObject *self, PyObject *args, PyObject *kwds ) {
  const char *msg = NULL;
  
  PyObject *py_obj = NULL;
  PyObject *py_ts = NULL;
  int clf = 0;

  static char *kwlist[] = {"obj", "ts", "clf", NULL}; 
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|Oi", kwlist, &py_obj, &py_ts, &clf ) ) {
    return NULL;
  }

  // -------------
  // Logged object
  // -------------
  vgx_VGXServerRequest_t *request = NULL;
  vgx_VGXServerResponse_t *response = NULL;
  int64_t n_entries = 0;
  int64_t n_hits = 0;
  if( PyUnicode_Check(py_obj) ) {
    if( (msg = PyUnicode_AsUTF8(py_obj)) == NULL ) {
      return NULL;
    }
  }
  else if( PyVGX_PluginResponse_CheckExact(py_obj) ) {
    PyVGX_PluginResponse *py_response = (PyVGX_PluginResponse*)py_obj;
    SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
    if( py_response->py_entries ) {
      n_entries =  PyList_Size( py_response->py_entries );
    }
    n_hits = py_response->metas.hitcount;
    request = py_response->request;
    response = py_response->response;
  }
  else if( PyVGX_PluginRequest_CheckExact(py_obj) ) {
    PyVGX_PluginRequest *py_request = (PyVGX_PluginRequest*)py_obj;
    SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
    request = py_request->request;
  }
  else {
    PyErr_Format( PyExc_TypeError, "str, PluginResponse or PluginRequest required, not %s", Py_TYPE( py_obj )->tp_name );
    return NULL;
  }
  
  if( !request && !msg ) {
    Py_RETURN_NONE;
  }

  // ---------
  // Timestamp
  // ---------
  int64_t ns_1970;
  if( py_ts == NULL ) {
    ns_1970 = 0;
  }
  // Nanoseconds since 1970
  else if( PyLong_Check( py_ts ) ) {
    ns_1970 = PyLong_AsLongLong( py_ts );
  }
  // Seconds since 1970
  else if( PyFloat_Check( py_ts ) ) {
    double s = PyFloat_AsDouble( py_ts );
    ns_1970 = (int64_t)(s * 1e9);
  }
  else {
    PyErr_SetString( PyExc_TypeError, "timestamp must be int or float" );
    return NULL;
  }

  int ret;

  BEGIN_PYVGX_THREADS {
    CString_t *CSTR__msg = NULL;
    // Use supplied message string
    if( msg ) {
      CSTR__msg = CStringNew( msg );
    }
    // Extract message from request object
    else if( request ) {
      const char *method = __vgx_http_request_method( request->method );
      const char *path = request->path;
      int64_t sn = request->headers->sn;

      // Common Log Format
      // host ident authuser timestamp request-line status bytes
      // 127.0.0.1 - - [01/May/2025:07:20:10 +0000] "GET /index.html HTTP/1.1" 200 9481
      if( clf > 0 ) {
        vgx_VGXServerClient_t *client = request->headers->client;
        const char *ip = client && client->URI ? iURI.HostIP( client->URI ) : "-";
        time_t seconds_since_epoch = ns_1970 > 0 ? ns_1970 / 1000000000LL : __SECONDS_SINCE_1970();
        struct tm *now = localtime( &seconds_since_epoch );
        if( now ) {
          HTTPStatus status = response == NULL ? HTTP_STATUS__OK :
                              response->info.http_errcode != HTTP_STATUS__NONE ? response->info.http_errcode :
                              response->status.code != HTTP_STATUS__NONE ? response->status.code :
                              HTTP_STATUS__OK;
          char tbuf[32];
          tbuf[0] = '\0';
          strftime( tbuf, 31, "%d/%b/%Y:%H:%M:%S %z", now );
          CSTR__msg = CStringNewFormat( "%s - - [%s] \"%s %s HTTP/%d.%d\" %d -", ip, tbuf, method, path, (int)request->version.major, (int)request->version.minor, status );
          ns_1970 = -1;
        }
      }
      // vgxserver log format
      else {
        // We also have a response object
        if( response ) {
          CSTR__msg = CStringNewFormat( "%s %s %016llx %lld %lld %.3f", method, path, sn, n_entries, n_hits, response->exec_ns / 1e9 );
        }
        else {
          CSTR__msg = CStringNewFormat( "%s %s %016llx", method, path, sn );
        }
      }
    }

    // Put message on queue
    if( CSTR__msg ) {
      ret = COMLIB__Log( g_accesslog_context, ns_1970, &CSTR__msg );
    }
    else {
      ret = -1;
    }

    // Safeguard if not stolen
    iString.Discard( &CSTR__msg );

  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyExc_IOError, "log output failed" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_AutoArcTimestamps
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_AutoArcTimestamps( PyObject *self, PyObject *py_enable ) {
  
  if( PyBool_Check(py_enable) || PyLong_Check( py_enable ) ) {
    if( (_auto_arc_timestamps = PyLong_AsLong( py_enable )) == true ) {
      PYVGX_API_WARNING( "pyvgx", 0, "Automatic arc timestamps enabled. Expect additional memory and processing overhead." );
    }
  }
  else {
    PyErr_SetString( PyExc_TypeError, "an integer is required" );
    return NULL;
  }

  Py_RETURN_NONE;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef pyvgx_methods[] = {

  { "version",            (PyCFunction)PyVGX_version,           METH_VARARGS,                   "version( verbosity=0 ) -> str" },

  { "popcnt",             (PyCFunction)PyVGX_popcnt,            METH_O,                         "popcnt( x ) -> int"  },
  { "ihash64",            (PyCFunction)PyVGX_ihash64,           METH_O,                         "ihash64( x ) -> long"  },
  { "ihash128",           (PyCFunction)PyVGX_ihash128,          METH_O,                         "ihash128( x ) -> str"  },
  { "strhash64",          (PyCFunction)PyVGX_strhash64,         METH_O,                         "strhash64( x ) -> long"  },
  { "strhash128",         (PyCFunction)PyVGX_strhash128,        METH_O,                         "strhash128( x ) -> str"  },
  { "md5",                (PyCFunction)PyVGX_md5,               METH_O,                         "md5( x ) -> str"  },
  { "sha256",             (PyCFunction)PyVGX_sha256,            METH_O,                         "sha256( x ) -> str"  },
  { "crc32c",             (PyCFunction)PyVGX_crc32c,            METH_O,                         "crc32c( x ) -> long"  },
  
  { "rstr",               (PyCFunction)PyVGX_rstr,              METH_O,                         "rstr( n ) -> str"  },

  { "vgxrpndefs",         (PyCFunction)PyVGX_vgxrpndefs,        METH_NOARGS,                    "vgxrpndefs() -> list"  },

  { "tokenize",           (PyCFunction)PyVGX_tokenize,          METH_O,                         "tokenize( x ) -> list"  },
  { "tokenize_min",       (PyCFunction)PyVGX_tokenize_min,      METH_O,                         "tokenize_min( x ) -> list"  },

  { "encode_utf8",        (PyCFunction)PyVGX_encode_utf8,       METH_O,                         "encode_utf8( unicode ) -> str"  },
  { "decode_utf8",        (PyCFunction)PyVGX_decode_utf8,       METH_O,                         "decode_utf8( str ) -> unicode"  },

  { "serialize",          (PyCFunction)PyVGX_serialize,         METH_O,                         "serialize( obj ) -> bytes" },
  { "deserialize",        (PyCFunction)PyVGX_deserialize,       METH_O,                         "deserialize( bytes ) -> obj" },

  { "compress",           (PyCFunction)PyVGX_compress,          METH_O,                         "compress( obj ) -> bytes" },
  { "decompress",         (PyCFunction)PyVGX_decompress,        METH_O,                         "decompress( bytes ) -> obj" },

  { "timestamp",          (PyCFunction)PyVGX_timestamp,         METH_NOARGS,                    "timestamp() -> float" },

  { "threadinit",         (PyCFunction)PyVGX_threadinit,        METH_VARARGS,                   "threadinit( [seed] )" },
  { "threadid",           (PyCFunction)PyVGX_threadid,          METH_NOARGS,                    "threadid() -> long" },
  { "threadlabel",        (PyCFunction)PyVGX_threadlabel,       METH_O,                         "threadlabel( label ) -> label" },

  { "selftest",           (PyCFunction)PyVGX_selftest,          METH_VARARGS | METH_KEYWORDS,   "selftest( testroot[, library[, names[, force]]] ) -> None" },
  { "selftest_all",       (PyCFunction)PyVGX_selftest_all,      METH_O,                         "selftest_all( testroot ) -> None" },
  { "enable_selftest",    (PyCFunction)PyVGX_enable_selftest,   METH_O,                         "enable_selftest( flag ) -> None" },
  
  { "profile",            (PyCFunction)PyVGX_profile,           METH_NOARGS,                    "profile()" },
  { "meminfo",            (PyCFunction)PyVGX_meminfo,           METH_NOARGS,                    "meminfo() -> (total, used)" },
#if defined CXPLAT_ARCH_X64
  { "cpuid",              (PyCFunction)PyVGX_cpuid,             METH_VARARGS | METH_KEYWORDS,   "cpuid( leaf[, subleaf[, obj]] ) -> (EAX, EBX, ECX, EDX)" },
#endif
  { "avxbuild",           (PyCFunction)PyVGX_avxbuild,          METH_NOARGS,                    "avxbuild() -> int" },

  { "initadmin",          (PyCFunction)PyVGX_initadmin,         METH_NOARGS,                    "initadmin()" },

  // LOGGING
  {"SetOutputStream",     (PyCFunction)PyVGX_SetOutputStream,   METH_O,                         "SetOutputStream( filepath ) -> None" },
  {"LogDebug",            (PyCFunction)PyVGX_LogDebug,          METH_O,                         "LogDebug( message ) -> None"  },
  {"LogInfo",             (PyCFunction)PyVGX_LogInfo,           METH_O,                         "LogInfo( message ) -> None"  },
  {"LogWarning",          (PyCFunction)PyVGX_LogWarning,        METH_O,                         "LogWarning( message ) -> None"  },
  {"LogError",            (PyCFunction)PyVGX_LogError,          METH_O,                         "LogError( message ) -> None"  },

  {"OpenAccessLog",       (PyCFunction)PyVGX_OpenAccessLog,     METH_O,                         "OpenAccesslog( filepath ) -> None" },
  {"LogTimestamp",        (PyCFunction)PyVGX_LogTimestamp,      METH_VARARGS | METH_KEYWORDS,   "LogTimestamp( message, ts=<now>, clf=False ) -> None"  },

  {"AutoArcTimestamps",   (PyCFunction)PyVGX_AutoArcTimestamps, METH_O,                         "AutoArcTimestamps( enable=False ) -> None" },

  {NULL}  /* Sentinel */
};
RESUME_WARNINGS


/******************************************************************************
 * compat_check
 *
 ******************************************************************************
 */
static int compat_check( void ) {
#if defined CXPLAT_ARCH_X64
    static int build_avx = 
#ifdef __cxlib_AVX512_MINIMUM__
      512;
#elif defined __AVX2__
      2;
#else
#error "This software must be built with AVX2 or AVX512 support"
#endif
  int cpu_avx = iVGXProfile.CPU.GetAVXVersion();

  if( cpu_avx < build_avx ) {
    char *brand = get_new_cpu_brand_string();
    char buf[512];
    snprintf( buf, 511, "Required instruction set extension AVX%d not available on CPU: %s", build_avx, brand ? brand : "unknown" );
    PyErr_SetString( PyExc_Exception, buf );
    free( brand );
    return -1;
  }
#elif defined CXPLAT_ARCH_ARM64


#endif

  return 0;
}

static const char *SIMD_ARCH = 
#if defined CXPLAT_ARCH_X64
  #ifdef __cxlib_AVX512_MINIMUM__
        " (AVX-512)";
  #elif defined __AVX2__
        " (AVX2)";
  #elif defined __AVX__
        " (AVX)";
  #else
        "";
  #endif
#elif defined CXPLAT_ARCH_ARM64
  #if defined __ARM_NEON && defined __ARM_NEON_FP && __ARM_FEATURE_FMA
        " (NEON)";
  #else
        "";
  #endif
#else
        "";
#endif



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static const char * __banner_get_indent( unsigned indent ) {
  static char spaces[] = "                ";
  const char *x = &spaces[sizeof(spaces)-1] - (indent < 16 ? indent : 16);
  return x;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static CString_t * __banner_entry( unsigned indent, const char *key, const char *fmt, ... ) {
  char buf[256] = {0};
  va_list args;
  va_start( args, fmt );
  vsnprintf( buf, 255, fmt, args );
  va_end( args );
  const char *indent_str = __banner_get_indent(indent);
  return CStringNewFormat( "%s%-8s: %s", indent_str, key, buf );
}


#define __print_banner_entry( Indent, Key, Format, ... ) do { \
  CString_t *CSTR__entry = __banner_entry( Indent, Key, Format, ##__VA_ARGS__ );  \
  if( CSTR__entry ) { \
    CXLIB_OSTREAM( "%s", CStringValue( CSTR__entry ) ); \
    iString.Discard( &CSTR__entry ); \
  } \
} WHILE_ZERO




/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static char * __get_banner_bar( unsigned indent ) {
  static char bar[128] = {0};
  CString_t *CSTR__build = __banner_entry( indent, "BUILD", "%s%s", g_build_info, SIMD_ARCH );
  if( CSTR__build ) {
    for( int i=0; i<CStringLength(CSTR__build) && i<127; i++ ) {
      bar[i] = '_';
    }
  }
  iString.Discard( &CSTR__build );
  return bar;
}

#define __print_banner_bar( Indent ) CXLIB_OSTREAM( "%s", __get_banner_bar( Indent ) )



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __banner_logo( unsigned indent ) {
  static const char bdata[] = "000000010000010A00000000000000225F5F202020202020"
                              "2020202020205F5F5F5F202020205F20205F5F5F5F5F5F5F"
                              "20200A20205F5F20205F5F202F202020207C5F20205F5F5C"
                              "5F202F202F20207C202F7C202F5F5F5F202020200A20202F"
                              "202F202F5F2F202F207C202F202F202F5F5F202F202F202F"
                              "20202F2020207C205F202F2020200A202F5F2F202F5F5F5F"
                              "2F202F7C207C2F20202F2F202F5F2F200A202020207C2020"
                              "2020202F5F2F2020202F202C5F5F5C205F5F5C2F5F5F5F7C"
                              "5F7C2F5F2F2F5F5F20200A202020207C5F2F202020202020"
                              "202020202F5F5F5F20202020202020202020202020202020"
                              "202020200A20202020202020202020202020202020202020"
                              "202020202020202020202020202020200000000000000A20";

  const sha256_t h = sha256( bdata );
  sha256_t xh = { 
    .A = 0x9D3C9814CD068ABD,
    .B = 0xDAD6FFCD77C1428E,
    .C = 0xDAC386BD1DC08D70,
    .D = 0x479D923DEB748A48
  };

  CString_t *CSTR__x;
  if( memcmp( &h.id256, &xh.id256, sizeof(sha256_t) ) != 0 || (CSTR__x = iString.Serialization.Loads( bdata )) == NULL ) {
    return;
  }

  __print_banner_bar( indent );
  const char *x =__banner_get_indent( indent );
  PRINT_OSTREAM( "%s", x );
  const char *p = CStringValue( CSTR__x );
  char symbol[2] = {0};
  int last = CStringLength(CSTR__x) - 38;
  for( int i=0; i<last; i++ ) {
    *symbol = p[i];
    PRINT_OSTREAM( "%s", symbol );
    if( *symbol == '\n' && i < last-1) {
      PRINT_OSTREAM( "%s", x );
    }
  }
  CStringDelete( CSTR__x );
  __print_banner_bar( indent );
}
 


/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __banner_version( unsigned indent ) {
  // VERSION
  __print_banner_entry( indent, "VERSION", "%s", g_version_info );

  // BUILD
  __print_banner_entry( indent, "BUILD", "%s%s", g_build_info, SIMD_ARCH );

  // PYTHON
  __print_banner_entry( indent, "PYTHON", "%s", Py_GetVersion() );

  __print_banner_bar( indent );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __banner_cpu( unsigned indent ) {
  // CPU
  CString_t *CSTR__brand = iVGXProfile.CPU.GetBrandString();
  const char *brand = CStringValueDefault( CSTR__brand, "?" );
  #if defined CXPLAT_ARCH_X64
  int n_cores = 0;
  int n_threads = 0;
  iVGXProfile.CPU.GetCoreCount( &n_cores, &n_threads );
  if( n_cores > 1 && n_threads > 1 ) {
    __print_banner_entry( indent, "CPU", "%s (%d cores / %d threads)", brand, n_cores, n_threads );
  }
  #elif defined CXPLAT_ARCH_ARM64
  int n_P_cores = 0;
  int n_E_cores = 0;
  iVGXProfile.CPU.GetCoreCount( &n_P_cores, &n_E_cores );
  if( (n_P_cores + n_E_cores) > 0 ) {
    __print_banner_entry( indent, "CPU", "%s (%d Performance / %d Efficiency)", brand, n_P_cores, n_E_cores );
  }
  #else
  if(0) {}
  #endif
  else {
    __print_banner_entry( indent, "CPU", "%s", brand );
  }
  iString.Discard( &CSTR__brand );

  // CPU-EXT
  int avxcompat = 0;
  CString_t *CSTR__cpuext = iVGXProfile.CPU.GetInstructionSetExtensions( &avxcompat );
  int32_t n_ext = 0;
  const char *key = "CPU-EXT";
  int64_t max_width = strnlen( __get_banner_bar( indent ), 127 ) - strlen(key) - strlen(" : ");
  char linebuf[256] = {0};
  char *end = linebuf + 255;
  int max_ext_per_line = 15;
  if( CSTR__cpuext ) {
    CString_t **CSTR__features = CALLABLE(CSTR__cpuext)->Split(CSTR__cpuext, " ", &n_ext);
    if( CSTR__features ) {
      int xn = 0;
      while( xn < n_ext ) {
        int xnl = 0;
        char *p = linebuf;
        while( xnl < max_ext_per_line && xn < n_ext ) {
          const char *ext_str = CStringValue( CSTR__features[xn] );
          if( (p - linebuf) + CStringLength( CSTR__features[xn] ) >= max_width ) {
            break;
          }
          while( *ext_str != '\0' && p < end ) {
            *p++ = *ext_str++;
          }
          *p++ = ' ';
          ++xnl;
          ++xn;
        }
        *p = '\0';
        bool empty = true;
        for( const char *c=linebuf; c<p; ++c ) {
          if( *c != ' ' ) {
            empty = false;
            break;
          }
        }
        if( !empty ) {
          __print_banner_entry( indent, key, "%s", linebuf );
        }
        key = "";
      }
      CStringDeleteList( &CSTR__features );
    }
    iString.Discard( &CSTR__cpuext );
  }
  // UTEST
#ifdef INCLUDE_UNIT_TESTS
  __print_banner_entry( indent, "UTEST", "Enabled" );
#endif

  // HSCORE
  // Quick profile
  // Get profiler time in microseconds. Lower is better.
  int64_t n_hash = 1LL << 19;
  int64_t tp = pyvgx_SystemProfile( n_hash, 0, true );
  double hscore =  n_hash / (double)tp;
  const double baseline_hscore = 1.064829558379;
  __print_banner_entry( indent, "HSCORE", "%.0f/10", 10*hscore/baseline_hscore );

  __print_banner_bar( indent );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __banner_memory( unsigned indent ) {
  // MEMORY
  int64_t g_phys = 0;
  int64_t proc_phys = 0;
  get_system_physical_memory( &g_phys, NULL, &proc_phys );
  __print_banner_entry( indent, "MEMORY", "%5lld GiB", g_phys >> 30 );

  // L3
  // L2
  // L1I
  // L1D
  CString_t *CSTR__cache = iVGXProfile.CPU.GetCacheInfo(); // maybe NULL
  if( CSTR__cache ) {
    int32_t sz = 0;
    CString_t **CSTR__caches = CALLABLE( CSTR__cache )->Split( CSTR__cache, "\n", &sz );
    if( CSTR__caches && sz > 1 ) {
      const char *cache = CStringValue( CSTR__caches[0] );
      for( int32_t i=sz-1; i>=0; i-- ) {
        cache = CStringValue( CSTR__caches[i] );
        if( cache[0] != '\0' ) {
          CXLIB_OSTREAM( "%s   %s", __banner_get_indent(indent), cache );
        }
      }
      CStringDeleteList( &CSTR__caches );
    }
    else {
      CXLIB_OSTREAM( "%s(NO CACHE INFO)", __banner_get_indent(indent) );
    }
    iString.Discard( &CSTR__cache );
  }

  // STLB
  // DTLB
  // ITLB
  CString_t *CSTR__tlb = iVGXProfile.CPU.GetTLBInfo(); // maybe NULL
  if( CSTR__tlb ) {
    int32_t sz = 0;
    CString_t **CSTR__tlbs = CALLABLE( CSTR__tlb )->Split( CSTR__tlb, "\n", &sz );
    if( CSTR__tlbs && sz > 1 ) {
      const char *tlb = CStringValue( CSTR__tlbs[0] );
      for( int32_t i=sz-1; i>=0; i-- ) {
        tlb = CStringValue( CSTR__tlbs[i] );
        if( tlb[0] != '\0' ) {
          CXLIB_OSTREAM( "%s   %s", __banner_get_indent(indent), tlb );
        }
      }
      CStringDeleteList( &CSTR__tlbs );
    }
    else {
      // CXLIB_OSTREAM( "%s(NO TLB INFO)", __banner_get_indent(indent) );
    }
    iString.Discard( &CSTR__tlb );
  }

  __print_banner_bar( indent );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __banner_host( unsigned indent ) {
  // TIME
  CString_t *CSTR__now = igraphinfo.CTime(-1, false);
  __print_banner_entry( indent, "TIME", "%s", CSTR__now ? CStringValue(CSTR__now) : "?" );
  iString.Discard( &CSTR__now );

  // HOSTUP
  int64_t up_t = __GET_CURRENT_MILLISECOND_TICK() / 1000;
  int64_t up_days =    up_t / 86400;
  int64_t up_hours = ( up_t % 86400 ) / 3600;
  int64_t up_min =   ( up_t % 3600  ) / 60;
  int64_t up_sec =     up_t % 60;
  __print_banner_entry( indent, "HOST-UP", "%lld:%02lld:%02lld:%02lld", up_days, up_hours, up_min, up_sec );

  // HOST
  char *ip = cxgetip( NULL );
  CString_t *CSTR__fqdn = iURI.NewFqdn();
  const char *hostname = CStringValueDefault( CSTR__fqdn, "?" );
  __print_banner_entry( indent, "HOST", "%s (%s)", hostname, ip?ip:"" );
  iString.Discard( &CSTR__fqdn );
  free( ip );

  // CWD
  char cwdbuf[MAX_PATH+1] = {0};
  const char *cwd;
#ifdef CXPLAT_WINDOWS_X64
  cwd = _getcwd( cwdbuf, MAX_PATH );
#else
  cwd = getcwd( cwdbuf, MAX_PATH );
#endif
  __print_banner_entry( indent, "CWD", "%s", cwd );

  // PID
  uint64_t pid = GET_CURRENT_PROCESS_ID();
  __print_banner_entry( indent, "PID", "%u", pid );

  __print_banner_bar( indent );
}



/******************************************************************************
 * banner
 *
 ******************************************************************************
 */
static int banner( void ) {
  unsigned indent = 2;
/*
______________________________________________________________________________
      ____       _    _________  __
     / __ \__  _| |  / / ____/ |/ /
    / /_/ / / / / | / / / __ |   /
   / ____/ /_/ /| |/ / /_/ //   |
  /_/    \__, / |___/\____//_/|_|
        /____/
______________________________________________________________________________
*/
  __banner_logo( indent );
/*
  VERSION : pyvgx v?.?.?
  BUILD   : [date: Feb 10 2024 16:35:32, built by: local, build #: N/A] (AVX2)
______________________________________________________________________________
*/
  __banner_version( indent );
/*
  CPU     : Intel(R) Xeon(R) W-2255 CPU @ 3.70GHz (10 cores / 20 threads)
  CPU-EXT : FMA AVX F16C AVX2 AVX512F AVX512DQ AVX512CD AVX512BW AVX512VL AVX512_VNNI
  UTEST   : Enabled
  HSCORE  : 10/10
______________________________________________________________________________
*/
  __banner_cpu( indent );
/*
  MEMORY  :   127 GiB
     L3   : 19712 kiB 11-way
     L2   :  1024 kiB 16-way (x 10)
     L1I  :    32 kiB 8-way (x 10)
     L1D  :    32 kiB 8-way (x 10)
     STLB :  1536 sets 6-way (x 10)
     DTLB :    64 sets 4-way (x 10)
     ITLB :    64 sets 8-way (x 10)
______________________________________________________________________________
*/
  __banner_memory( indent );
/*
  TIME    : Sat Feb 10 18:15:25 2024
  HOST-UP : 3:06:16:39
  HOST    : Sim10 (10.10.1.115)
  CWD     : H:\VGX\x64\Release
  PID     : 9300
______________________________________________________________________________
*/
  __banner_host( indent );

  return 0;
}



/******************************************************************************
 * init_PyVGX
 *
 ******************************************************************************
 */
int init_PyVGX( PyObject *module ) {
  SET_EXCEPTION_CONTEXT
  if( __initialize() < 0 ) {
    PyErr_SetString( PyExc_ImportError, "Core initialization failed" );
    return -1;
  }

  g_py_zero = PyLong_FromLong( 0 );
  g_py_one = PyLong_FromLong( 1 );
  g_py_minus_one = PyLong_FromLong( -1 );
  g_py_char_w = PyUnicode_FromString( "w" );
  g_py_char_a = PyUnicode_FromString( "a" );
  g_py_char_r = PyUnicode_FromString( "r" );
  g_py_noargs = PyTuple_New(0);

  if( g_py_zero == NULL || g_py_one == NULL || g_py_minus_one == NULL || g_py_char_w == NULL || g_py_char_a == NULL || g_py_char_r == NULL || g_py_noargs == NULL ) {
    return -1;
  }

  g_py_SYS_prop_id = PyBytes_FromString( iSystem.Name.PropertyVertex() );
  g_py_SYS_prop_type = PyBytes_FromString( iSystem.Name.VertexType() );
  g_py_SYS_prop_mode = PyBytes_FromString( "w" );

  if( g_py_SYS_prop_id == NULL || g_py_SYS_prop_type == NULL || g_py_SYS_prop_mode == NULL ) {
    return -1;
  }

  if( __pyvgx_xvgxpartial__init() < 0 ) {
    return -1;
  }

  if( __pyvgx_framehash__init() < 0 ) {
    return -1;
  }
  
  Py_SET_TYPE( p_PyVGX_Framehash__FramehashType, &PyType_Type );
  Py_SET_TYPE( p_PyVGX_StringQueue__StringQueueType, &PyType_Type );
  Py_SET_TYPE( p_PyVGX_Graph__GraphType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Vertex__VertexType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Vector__VectorType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Similarity__SimilarityType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Memory__MemoryType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_PluginRequestType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_PluginResponseType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Query__QueryType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_System__SystemType, &PyType_Type);
  Py_SET_TYPE( p_PyVGX_Operation__OperationType, &PyType_Type);


  if( PyType_Ready(p_PyVGX_Framehash__FramehashType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_StringQueue__StringQueueType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_Graph__GraphType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_Vertex__VertexType) < 0 ) {
    return -1;
  }
  
  if( PyType_Ready(p_PyVGX_Vector__VectorType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_Similarity__SimilarityType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_Memory__MemoryType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_PluginRequestType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_PluginResponseType) < 0 ) {
    return -1;
  }

  if( PyType_Ready(p_PyVGX_Query__QueryType) < 0 ) {
    return -1;
  }

  p_PyVGX_System__SystemType->tp_dict = PyVGX_System_GetNewConstantsDict();

  if( PyType_Ready(p_PyVGX_System__SystemType) < 0 ) {
    return -1;
  }

  p_PyVGX_Operation__OperationType->tp_dict = PyVGX_Operation_GetNewConstantsDict();

  if( PyType_Ready(p_PyVGX_Operation__OperationType) < 0 ) {
    return -1;
  }


  // Framehash
  Py_INCREF( p_PyVGX_Framehash__FramehashType );
  if( PyModule_AddObject(module, "Framehash", (PyObject*)p_PyVGX_Framehash__FramehashType) < 0 ) {
    Py_DECREF( p_PyVGX_Framehash__FramehashType );
    return -1;
  }

  // StringQueue
  Py_INCREF( p_PyVGX_StringQueue__StringQueueType );
  if( PyModule_AddObject(module, "StringQueue", (PyObject*)p_PyVGX_StringQueue__StringQueueType) < 0 ) {
    Py_DECREF( p_PyVGX_StringQueue__StringQueueType );
    return -1;
  }

  // Graph
  Py_INCREF( p_PyVGX_Graph__GraphType );
  if( PyModule_AddObject( module, "Graph", (PyObject*)p_PyVGX_Graph__GraphType ) < 0 ) {
    Py_DECREF( p_PyVGX_Graph__GraphType );
    return -1;
  }

  // Vertex
  Py_INCREF( p_PyVGX_Vertex__VertexType );
  if( PyModule_AddObject( module, "Vertex", (PyObject*)p_PyVGX_Vertex__VertexType ) < 0 ) {
    Py_DECREF( p_PyVGX_Vertex__VertexType );
    return -1;
  }

  // Vector
  Py_INCREF( p_PyVGX_Vector__VectorType );
  if( PyModule_AddObject( module, "Vector", (PyObject*)p_PyVGX_Vector__VectorType ) < 0 ) {
    Py_DECREF( p_PyVGX_Vector__VectorType );
    return -1;
  }

  // Similarity
  Py_INCREF( p_PyVGX_Similarity__SimilarityType );
  if( PyModule_AddObject( module, "Similarity", (PyObject*)p_PyVGX_Similarity__SimilarityType ) < 0 ) {
    Py_DECREF( p_PyVGX_Similarity__SimilarityType );
    return -1;
  }

  if( (_global_default_similarity = (PyVGX_Similarity*)PyObject_CallObject( (PyObject*)p_PyVGX_Similarity__SimilarityType, NULL )) == NULL ) {
    return -1;
  }

  // Evalmem
  Py_INCREF( p_PyVGX_Memory__MemoryType );
  if( PyModule_AddObject( module, "Memory", (PyObject*)p_PyVGX_Memory__MemoryType ) < 0 ) {
    Py_DECREF( p_PyVGX_Memory__MemoryType );
    return -1;
  }

  // Plugin Request
  Py_INCREF( p_PyVGX_PluginRequestType );
  if( PyModule_AddObject( module, "PluginRequest", (PyObject*)p_PyVGX_PluginRequestType ) < 0 ) {
    Py_DECREF( p_PyVGX_PluginRequestType );
    return -1;
  }

  // Plugin Response
  Py_INCREF( p_PyVGX_PluginResponseType );
  if( PyModule_AddObject( module, "PluginResponse", (PyObject*)p_PyVGX_PluginResponseType ) < 0 ) {
    Py_DECREF( p_PyVGX_PluginResponseType );
    return -1;
  }

  // Query
  Py_INCREF( p_PyVGX_Query__QueryType );
  if( PyModule_AddObject( module, "Query", (PyObject*)p_PyVGX_Query__QueryType ) < 0 ) {
    Py_DECREF( p_PyVGX_Query__QueryType );
    return -1;
  }

  // Operation
  Py_INCREF( p_PyVGX_Operation__OperationType );
  if( PyModule_AddObject( module, "Operation", (PyObject*)p_PyVGX_Operation__OperationType ) < 0 ) {
    Py_DECREF( p_PyVGX_Operation__OperationType );
    return -1;
  }



  // Exit handler
  Py_AtExit( __shutdown );

#ifdef CXPLAT_WINDOWS_X64
#ifndef NDEBUG
  _CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
  uint32_t now = __SECONDS_SINCE_1970();
  CString_t *CSTR__filename = CStringNewFormat( "pyvgx_leak_report_%u.txt", now );
  HANDLE hMemLeaks = CreateFile( CStringValue(CSTR__filename), GENERIC_WRITE, FILE_SHARE_WRITE, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL );
  CStringDelete( CSTR__filename );
  _CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE | _CRTDBG_MODE_DEBUG );
  _CrtSetReportFile( _CRT_WARN, hMemLeaks );
#endif
#endif

  if( PyModule_AddObject( module, "Framehash", (PyObject*)p_PyVGX_Framehash__FramehashType ) < 0 ) {
    Py_DECREF( p_PyVGX_Framehash__FramehashType );
    return -1;
  }

  if( PyModule_AddObject( module, "StringQueue", (PyObject*)p_PyVGX_StringQueue__StringQueueType ) < 0 ) {
    Py_DECREF( p_PyVGX_StringQueue__StringQueueType );
    return -1;
  }

  // Global similarity
  if( PyModule_AddObject( module, "DefaultSimilarity", (PyObject*)_global_default_similarity ) < 0 ) {
    Py_DECREF( _global_default_similarity );
    return -1;
  }

  // Singleton system object
  if( (_global_system_object = (PyVGX_System*)PyObject_CallObject( (PyObject*)p_PyVGX_System__SystemType, NULL )) == NULL ) {
    return -1;
  }

  if( PyModule_AddObject( module, "system", (PyObject*)_global_system_object ) < 0 ) {
    Py_DECREF( _global_system_object );
    return -1;
  }

  // Singleton operation object
  if( (_global_operation_object = (PyVGX_Operation*)PyObject_CallObject( (PyObject*)p_PyVGX_Operation__OperationType, NULL )) == NULL ) {
    return -1;
  }

  if( PyModule_AddObject( module, "op", (PyObject*)_global_operation_object ) < 0 ) {
    Py_DECREF( _global_operation_object );
    return -1;
  }
  
  // Builder
  if( _ipyvgx_builder__init() < 0 ) {
    return -1;
  }

  // Codec
  if( _ipyvgx_codec__init() < 0 ) {
    return -1;
  }

  // Timestamp constants
  PyModule_AddObject( module,  "T_NEVER",  PyLong_FromUnsignedLongLong( TIME_EXPIRES_NEVER ) );
  PyModule_AddObject( module,  "T_MIN",    PyLong_FromUnsignedLongLong( TIMESTAMP_MIN ) );
  PyModule_AddObject( module,  "T_MAX",    PyLong_FromUnsignedLongLong( TIMESTAMP_MAX ) );

  // Misc limits
  PyModule_AddObject( module,  "MAX_STRING", PyLong_FromUnsignedLongLong( _VXOBALLOC_CSTRING_MAX_LENGTH ) );

  // Predicator modifiers
  PyModule_AddIntConstant( module,  "M_ANY",      VGX_PREDICATOR_MOD_WILDCARD );
  PyModule_AddIntConstant( module,  "M_STAT",     VGX_PREDICATOR_MOD_STATIC );
  PyModule_AddIntConstant( module,  "M_SIM",      VGX_PREDICATOR_MOD_SIMILARITY );
  PyModule_AddIntConstant( module,  "M_DIST",     VGX_PREDICATOR_MOD_DISTANCE );
  PyModule_AddIntConstant( module,  "M_LSH",      VGX_PREDICATOR_MOD_LSH );
  PyModule_AddIntConstant( module,  "M_INT",      VGX_PREDICATOR_MOD_INTEGER );
  PyModule_AddIntConstant( module,  "M_UINT",     VGX_PREDICATOR_MOD_UNSIGNED );
  PyModule_AddIntConstant( module,  "M_FLT",      VGX_PREDICATOR_MOD_FLOAT );
  PyModule_AddIntConstant( module,  "M_CNT",      VGX_PREDICATOR_MOD_COUNTER );
  PyModule_AddIntConstant( module,  "M_ACC",      VGX_PREDICATOR_MOD_ACCUMULATOR );
  PyModule_AddIntConstant( module,  "M_INTAGGR",  VGX_PREDICATOR_MOD_INT_AGGREGATOR );
  PyModule_AddIntConstant( module,  "M_FLTAGGR",  VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR );
  PyModule_AddIntConstant( module,  "M_TMC",      VGX_PREDICATOR_MOD_TIME_CREATED );
  PyModule_AddIntConstant( module,  "M_TMM",      VGX_PREDICATOR_MOD_TIME_MODIFIED );
  PyModule_AddIntConstant( module,  "M_TMX",      VGX_PREDICATOR_MOD_TIME_EXPIRES );

  // OR-able add-on modifier mask
  PyModule_AddIntConstant( module,  "M_AUTOTM",   _VGX_PREDICATOR_MOD_AUTO_TM );        // Automatic timestamps
  PyModule_AddIntConstant( module,  "M_FWDONLY",  _VGX_PREDICATOR_MOD_FORWARD_ONLY );   // Forward-only arc

  // Value comparison
  // Basic
  PyModule_AddIntConstant( module,  "V_ANY",      VGX_VALUE_ANY );
  PyModule_AddIntConstant( module,  "V_NOT",      VGX_VALUE_NEG );
  PyModule_AddIntConstant( module,  "V_LTE",      VGX_VALUE_LTE );
  PyModule_AddIntConstant( module,  "V_GT",       VGX_VALUE_GT  );
  PyModule_AddIntConstant( module,  "V_GTE",      VGX_VALUE_GTE );
  PyModule_AddIntConstant( module,  "V_LT",       VGX_VALUE_LT  );
  PyModule_AddIntConstant( module,  "V_EQ",       VGX_VALUE_EQU );
  PyModule_AddIntConstant( module,  "V_NEQ",      VGX_VALUE_NEQ );
  // Extended
  PyModule_AddIntConstant( module,  "V_RANGE",    VGX_VALUE_RANGE );
  PyModule_AddIntConstant( module,  "V_NRANGE",   VGX_VALUE_NRANGE );
  PyModule_AddIntConstant( module,  "V_DYN_DELTA",VGX_VALUE_DYN_RANGE );
  PyModule_AddIntConstant( module,  "V_DYN_RATIO",VGX_VALUE_DYN_RANGE_R );
  PyModule_AddIntConstant( module,  "V_DYN_LTE",  VGX_VALUE_DYN_LTE );
  PyModule_AddIntConstant( module,  "V_DYN_GT",   VGX_VALUE_DYN_GT );
  PyModule_AddIntConstant( module,  "V_DYN_GTE",  VGX_VALUE_DYN_GTE );
  PyModule_AddIntConstant( module,  "V_DYN_LT",   VGX_VALUE_DYN_LT );
  PyModule_AddIntConstant( module,  "V_DYN_EQ",   VGX_VALUE_DYN_EQU );
  PyModule_AddIntConstant( module,  "V_DYN_NEQ",  VGX_VALUE_DYN_NEQ );


  // Sortby
  PyModule_AddIntConstant( module,  "S_NONE",         VGX_SORTBY_NONE );
  PyModule_AddIntConstant( module,  "S_ASC",          VGX_SORT_DIRECTION_ASCENDING );
  PyModule_AddIntConstant( module,  "S_DESC",         VGX_SORT_DIRECTION_DESCENDING );
  PyModule_AddIntConstant( module,  "S_VAL",          VGX_SORTBY_PREDICATOR );
  PyModule_AddIntConstant( module,  "S_ADDR",         VGX_SORTBY_MEMADDRESS );
  PyModule_AddIntConstant( module,  "S_OBID",         VGX_SORTBY_INTERNALID );
  PyModule_AddIntConstant( module,  "S_ID",           VGX_SORTBY_IDSTRING );
  PyModule_AddIntConstant( module,  "S_ANCHOR",       VGX_SORTBY_ANCHOR_ID );
  PyModule_AddIntConstant( module,  "S_ANCHOR_OBID",  VGX_SORTBY_ANCHOR_OBID );
  PyModule_AddIntConstant( module,  "S_DEG",          VGX_SORTBY_DEGREE );
  PyModule_AddIntConstant( module,  "S_IDEG",         VGX_SORTBY_INDEGREE );
  PyModule_AddIntConstant( module,  "S_ODEG",         VGX_SORTBY_OUTDEGREE );
  PyModule_AddIntConstant( module,  "S_SIM",          VGX_SORTBY_SIMSCORE );
  PyModule_AddIntConstant( module,  "S_HAM",          VGX_SORTBY_HAMDIST );
  PyModule_AddIntConstant( module,  "S_RANK",         VGX_SORTBY_RANKING );
  PyModule_AddIntConstant( module,  "S_TMC",          VGX_SORTBY_TMC );
  PyModule_AddIntConstant( module,  "S_TMM",          VGX_SORTBY_TMM );
  PyModule_AddIntConstant( module,  "S_TMX",          VGX_SORTBY_TMX );
  PyModule_AddIntConstant( module,  "S_NATIVE",       VGX_SORTBY_NATIVE );
  PyModule_AddIntConstant( module,  "S_RANDOM",       VGX_SORTBY_RANDOM );

  // Collection
  PyModule_AddObject( module,       "C_NONE",         Py_False );
  PyModule_AddObject( module,       "C_COLLECT",      Py_True );
  PyObject *false_wild = PyTuple_New( 2 );
  Py_INCREF( Py_False );
  PyTuple_SET_ITEM( false_wild, 0, Py_False );
  PyTuple_SET_ITEM( false_wild, 1, PyVGX_PyUnicode_FromStringNoErr( "*" ) );
  PyModule_AddObject( module,       "C_SCAN",         false_wild );
  false_wild = NULL;

  // Arc Direction
  PyModule_AddIntConstant( module,  "D_ANY",          VGX_ARCDIR_ANY );
  PyModule_AddIntConstant( module,  "D_IN",           VGX_ARCDIR_IN );
  PyModule_AddIntConstant( module,  "D_OUT",          VGX_ARCDIR_OUT );
  PyModule_AddIntConstant( module,  "D_BOTH",         VGX_ARCDIR_BOTH );

  // Evaluator registers
  PyModule_AddIntConstant( module,  "R1",             EXPRESS_EVAL_MEM_REGISTER_R1 );
  PyModule_AddIntConstant( module,  "R2",             EXPRESS_EVAL_MEM_REGISTER_R2 );
  PyModule_AddIntConstant( module,  "R3",             EXPRESS_EVAL_MEM_REGISTER_R3 );
  PyModule_AddIntConstant( module,  "R4",             EXPRESS_EVAL_MEM_REGISTER_R4 );

  // Fields
  // Names
  PyModule_AddIntConstant( module,  "F_ANCHOR",       VGX_RESPONSE_ATTR_ANCHOR );
  PyModule_AddIntConstant( module,  "F_ANCHOR_OBID",  VGX_RESPONSE_ATTR_ANCHOR_OBID );
  PyModule_AddIntConstant( module,  "F_ID",           VGX_RESPONSE_ATTR_ID );
  PyModule_AddIntConstant( module,  "F_OBID",         VGX_RESPONSE_ATTR_OBID );
  PyModule_AddIntConstant( module,  "F_TYPE",         VGX_RESPONSE_ATTR_TYPENAME );
  PyModule_AddIntConstant( module,  "F_NAMES",        VGX_RESPONSE_ATTRS_NAMES );
  // Degree
  PyModule_AddIntConstant( module,  "F_DEG",      VGX_RESPONSE_ATTR_DEGREE );
  PyModule_AddIntConstant( module,  "F_IDEG",     VGX_RESPONSE_ATTR_INDEGREE );
  PyModule_AddIntConstant( module,  "F_ODEG",     VGX_RESPONSE_ATTR_OUTDEGREE );
  PyModule_AddIntConstant( module,  "F_DEGREES",  VGX_RESPONSE_ATTRS_DEGREES );
  // Predicator
  PyModule_AddIntConstant( module,  "F_ARCDIR",   VGX_RESPONSE_ATTR_ARCDIR );
  PyModule_AddIntConstant( module,  "F_REL",      VGX_RESPONSE_ATTR_RELTYPE );
  PyModule_AddIntConstant( module,  "F_MOD",      VGX_RESPONSE_ATTR_MODIFIER );
  PyModule_AddIntConstant( module,  "F_VAL",      VGX_RESPONSE_ATTR_VALUE );
  PyModule_AddIntConstant( module,  "F_PRED",     VGX_RESPONSE_ATTRS_PREDICATOR );
  PyModule_AddIntConstant( module,  "F_ARC",      VGX_RESPONSE_ATTRS_ARC );
  PyModule_AddIntConstant( module,  "F_AARC",     VGX_RESPONSE_ATTRS_ANCHORED_ARC );
  // Properties
  PyModule_AddIntConstant( module,  "F_VEC",      VGX_RESPONSE_ATTR_VECTOR );
  PyModule_AddIntConstant( module,  "F_PROP",     VGX_RESPONSE_ATTR_PROPERTY );
  PyModule_AddIntConstant( module,  "F_PROPS",    VGX_RESPONSE_ATTRS_PROPERTIES );
  // Relevance
  PyModule_AddIntConstant( module,  "F_RANK",     VGX_RESPONSE_ATTR_RANKSCORE );
  PyModule_AddIntConstant( module,  "F_SIM",      VGX_RESPONSE_ATTR_SIMILARITY );
  PyModule_AddIntConstant( module,  "F_HAM",      VGX_RESPONSE_ATTR_HAMDIST );
  PyModule_AddIntConstant( module,  "F_RLV",      VGX_RESPONSE_ATTRS_RELEVANCE );
  // Timestamps
  PyModule_AddIntConstant( module,  "F_TMC",      VGX_RESPONSE_ATTR_TMC );
  PyModule_AddIntConstant( module,  "F_TMM",      VGX_RESPONSE_ATTR_TMM );
  PyModule_AddIntConstant( module,  "F_TMX",      VGX_RESPONSE_ATTR_TMX );
  PyModule_AddIntConstant( module,  "F_TSTAMP",   VGX_RESPONSE_ATTRS_TIMESTAMP );
  // Internal
  PyModule_AddIntConstant( module,  "F_DESCR",    VGX_RESPONSE_ATTR_DESCRIPTOR );
  PyModule_AddIntConstant( module,  "F_ADDR",     VGX_RESPONSE_ATTR_ADDRESS );
  PyModule_AddIntConstant( module,  "F_HANDLE",   VGX_RESPONSE_ATTR_HANDLE );
  PyModule_AddIntConstant( module,  "F_RAW",      VGX_RESPONSE_ATTR_RAW_VERTEX );
  PyModule_AddIntConstant( module,  "F_DETAILS",  VGX_RESPONSE_ATTRS_DETAILS );
  // Everything
  PyModule_AddIntConstant( module,  "F_ALL",      VGX_RESPONSE_ATTRS_FULL );
  // Nothing
  PyModule_AddIntConstant( module,  "F_NONE",     VGX_RESPONSE_ATTRS_NONE );
  // Default
  PyModule_AddIntConstant( module,  "F_DEFAULT",  VGX_RESPONSE_ATTR_ANCHOR | VGX_RESPONSE_ATTRS_PREDICATOR | VGX_RESPONSE_ATTR_ID );

  // Result
  PyModule_AddIntConstant( module,  "R_STR",      VGX_RESPONSE_SHOW_AS_STRING );
  PyModule_AddIntConstant( module,  "R_LIST",     VGX_RESPONSE_SHOW_AS_LIST );
  PyModule_AddIntConstant( module,  "R_DICT",     VGX_RESPONSE_SHOW_AS_DICT );
  PyModule_AddIntConstant( module,  "R_SIMPLE",   VGX_RESPONSE_SHOW_WITH_NONE );
  PyModule_AddIntConstant( module,  "R_TIMING",   VGX_RESPONSE_SHOW_WITH_TIMING );
  PyModule_AddIntConstant( module,  "R_COUNTS",   VGX_RESPONSE_SHOW_WITH_COUNTS );
  PyModule_AddIntConstant( module,  "R_METAS",    VGX_RESPONSE_SHOW_WITH_METAS );
  PyModule_AddIntConstant( module,  "R_DEFAULT",  VGX_RESPONSE_SHOW_AS_DICT | VGX_RESPONSE_SHOW_WITH_TIMING );

  PyVGX_VertexError       = PyErr_NewExceptionWithDoc( "pyvgx.VertexError",       "Vertex error", PyExc_Exception, NULL );
  PyVGX_EnumerationError  = PyErr_NewExceptionWithDoc( "pyvgx.EnumerationError",  "Enumeration error", PyExc_LookupError, NULL );
  PyVGX_ArcError          = PyErr_NewExceptionWithDoc( "pyvgx.ArcError",          "Invalid arc", PyExc_ValueError, NULL );
  PyVGX_AccessError       = PyErr_NewExceptionWithDoc( "pyvgx.AccessError",       "Object cannot be acquired", PyExc_Exception, NULL );
  PyVGX_QueryError        = PyErr_NewExceptionWithDoc( "pyvgx.QueryError",        "Query error",  PyExc_Exception, NULL );
  PyVGX_SearchError       = PyErr_NewExceptionWithDoc( "pyvgx.SearchError",       "Search error", PyExc_Exception, NULL );
  PyVGX_ResultError       = PyErr_NewExceptionWithDoc( "pyvgx.ResultError",       "Result error", PyExc_Exception, NULL );
  PyVGX_RequestError      = PyErr_NewExceptionWithDoc( "pyvgx.RequestError",      "Request error", PyExc_Exception, NULL );
  PyVGX_ResponseError     = PyErr_NewExceptionWithDoc( "pyvgx.ResponseError",     "Response error", PyExc_Exception, NULL );
  PyVGX_PluginError       = PyErr_NewExceptionWithDoc( "pyvgx.PluginError",       "Plugin error", PyExc_Exception, NULL );
  PyVGX_InternalError     = PyErr_NewExceptionWithDoc( "pyvgx.InternalError",     "Internal error", PyExc_Exception, NULL );
  PyVGX_OperationTimeout  = PyErr_NewExceptionWithDoc( "pyvgx.OperationTimeout",  "Operation timeout", PyExc_Exception, NULL );
  PyVGX_DataError         = PyErr_NewExceptionWithDoc( "pyvgx.DataError",         "Data error", PyExc_Exception, NULL );

  PyModule_AddObject( module, "VertexError",      PyVGX_VertexError );
  PyModule_AddObject( module, "EnumerationError", PyVGX_EnumerationError );
  PyModule_AddObject( module, "ArcError",         PyVGX_ArcError );
  PyModule_AddObject( module, "AccessError",      PyVGX_AccessError );
  PyModule_AddObject( module, "QueryError",       PyVGX_QueryError );
  PyModule_AddObject( module, "SearchError",      PyVGX_SearchError );
  PyModule_AddObject( module, "ResultError",      PyVGX_ResultError );
  PyModule_AddObject( module, "RequestError",     PyVGX_RequestError );
  PyModule_AddObject( module, "ResponseError",    PyVGX_ResponseError );
  PyModule_AddObject( module, "PluginError",      PyVGX_PluginError );
  PyModule_AddObject( module, "InternalError",    PyVGX_InternalError );
  PyModule_AddObject( module, "OperationTimeout", PyVGX_OperationTimeout );
  PyModule_AddObject( module, "DataError",        PyVGX_DataError );

  

  const char *pyvgx_output = getenv( "PYVGX_OUTPUT" );
  if( pyvgx_output ) {
    PyObject *py_pyvgx_output = PyUnicode_FromString( pyvgx_output );
    if( py_pyvgx_output == NULL ) {
      return -1;
    }
    PyObject *py_tmp = PyVGX_SetOutputStream( NULL, py_pyvgx_output );
    Py_DECREF( py_pyvgx_output );
    if( py_tmp == NULL ) {
      return -1;
    }
    Py_DECREF( py_tmp );
  }

  if( getenv( "PYVGX_NOBANNER" ) != NULL ) {
    return 0;
  }

  return banner();

}



/***********************************************************************
 *
 *
 ***********************************************************************
 */
static PyModuleDef pyvgx_module = {
  PyModuleDef_HEAD_INIT,
  .m_name       = "pyvgx",
  .m_doc        = NULL,
  .m_size       = -1,
  .m_methods    = pyvgx_methods,
  .m_slots      = NULL,
  .m_traverse   = NULL,
  .m_clear      = NULL,
  .m_free       = NULL
};



/***********************************************************************
 * PyInit_pyvgx
 * Initialize Python module
 *
 ***********************************************************************
 */
PyMODINIT_FUNC PyInit_pyvgx( void ) {
  if( compat_check() < 0 ) {
    return NULL;
  }

  char description[201];

  snprintf( description, 200, "Python VGX C Extensions - Compiled for Python %d.%d", PY_MAJOR_VERSION, PY_MINOR_VERSION );

  pyvgx_module.m_doc = description;

  if( (g_pyvgx = PyModule_Create( &pyvgx_module )) == NULL ) {
    return NULL;
  }

  if( init_PyVGX( g_pyvgx ) < 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "init error" );
    }
    return NULL;
  }

  return g_pyvgx;
}

