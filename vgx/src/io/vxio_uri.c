/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxio_uri.c
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

#include "_vgx.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

__inline static const char *__uri__( const vgx_URI_t *uri ) {
  return uri ? iURI.URI( uri ) : "";
}

#define __MESSAGE( LEVEL, URI, Code, Format, ... ) LEVEL( Code, "IO::URI(%s): " Format, __uri__( URI ), ##__VA_ARGS__ )

#define IO_VERBOSE( URI, Code, Format, ... )   __MESSAGE( VERBOSE, URI, Code, Format, ##__VA_ARGS__ )
#define IO_INFO( URI, Code, Format, ... )      __MESSAGE( INFO, URI, Code, Format, ##__VA_ARGS__ )
#define IO_WARNING( URI, Code, Format, ... )   __MESSAGE( WARN, URI, Code, Format, ##__VA_ARGS__ )
#define IO_REASON( URI, Code, Format, ... )    __MESSAGE( REASON, URI, Code, Format, ##__VA_ARGS__ )
#define IO_CRITICAL( URI, Code, Format, ... )  __MESSAGE( CRITICAL, URI, Code, Format, ##__VA_ARGS__ )
#define IO_FATAL( URI, Code, Format, ... )     __MESSAGE( FATAL, URI, Code, Format, ##__VA_ARGS__ )


static const char * __win_format_last_error( char *buf, int maxsz, int *err );
static int __parse_URI_port( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_host( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_userinfo( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_authority( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_path( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_query( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );
static int __parse_URI_fragment( vgx_URI_t *URI, int *a, CString_t **CSTR__error );
static int __parse_URI_scheme( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error );

static int __set_input_socket( vgx_URI_t *URI, CXSOCKET sock );
static int __set_output_socket( vgx_URI_t *URI, CXSOCKET sock );

static vgx_URI_t *        _vxio_uri__new( const char *uri, CString_t **CSTR__error );
static vgx_URI_t *        _vxio_uri__new_elements( const char *scheme, const char *userinfo, const char *host, unsigned short port, const char *path, const char *query, const char *fragment, CString_t **CSTR__error );
static vgx_URI_t *        _vxio_uri__clone( const vgx_URI_t *URI );
static void               _vxio_uri__delete( vgx_URI_t **URI );
static const char *       _vxio_uri__get_uri( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_scheme( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_userinfo( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_host( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_port( const vgx_URI_t *URI );
static unsigned short     _vxio_uri__get_port_int( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_path( const vgx_URI_t *URI );
static int64_t            _vxio_uri__get_path_length( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_query( const vgx_URI_t *URI );
static int64_t            _vxio_uri__get_query_length( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_fragment( const vgx_URI_t *URI );
static int64_t            _vxio_uri__get_fragment_length( const vgx_URI_t *URI );
static const char *       _vxio_uri__get_host_ip( vgx_URI_t *URI );
static const char *       _vxio_uri__get_nameinfo( vgx_URI_t *URI );
static bool               _vxio_uri__match( const vgx_URI_t *A, const vgx_URI_t *B );
static struct addrinfo *  _vxio_uri__new_addrinfo( const vgx_URI_t *URI, CString_t **CSTR__error );
static void               _vxio_uri__delete_addrinfo( struct addrinfo **address );
static int                _vxio_uri__bind( vgx_URI_t *URI, CString_t **CSTR__error );
static int                _vxio_uri__listen( vgx_URI_t *URI, int backlog, CString_t **CSTR__error );
static vgx_URI_t *        _vxio_uri__accept( vgx_URI_t *URI, const char *scheme, CString_t **CSTR__error );
static CXSOCKET *         _vxio_uri__connect_inet_stream_tcp( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error, int *rerr );
static int                _vxio_uri__open_file( vgx_URI_t *URI, CString_t **CSTR__error );
static int                _vxio_uri__open_null( vgx_URI_t *URI, CString_t **CSTR__error );
static int                _vxio_uri__is_connected( const vgx_URI_t *URI );
static int                _vxio_uri__connect( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error );
static void               _vxio_uri__close( vgx_URI_t *URI );
static CString_t *        _vxio_uri__new_format( const vgx_URI_t *URI );
static CString_t *        _vxio_uri__new_fqdn( void );
static int                _vxio_uri__address_error( CString_t **CSTR__error, int err );
static int                _vxio_uri__file_get_descriptor( vgx_URI_t *URI );
static bool               _vxio_uri__is_null_output( vgx_URI_t *URI );
static CXSOCKET *         _vxio_uri__sock_output_get( vgx_URI_t *URI );
static CXSOCKET *         _vxio_uri__sock_input_get( vgx_URI_t *URI );
static int                _vxio_uri__sock_error( vgx_URI_t *URI, CString_t **CSTR__error, int err, short revents );
static int                _vxio_uri__sock_busy( int err );

static int                __set_URI_query_parameters( const char *path, vgx_URIQueryParameters_t *params );
static void               __clear_URI_query_parameters( vgx_URIQueryParameters_t *params );


DLL_EXPORT vgx_IURI_t iURI = {
  .New                  = _vxio_uri__new,
  .NewElements          = _vxio_uri__new_elements,
  .Clone                = _vxio_uri__clone,
  .Delete               = _vxio_uri__delete,
  .URI                  = _vxio_uri__get_uri,
  .Scheme               = _vxio_uri__get_scheme,
  .UserInfo             = _vxio_uri__get_userinfo,
  .Host                 = _vxio_uri__get_host,
  .Port                 = _vxio_uri__get_port,
  .PortInt              = _vxio_uri__get_port_int,
  .Path                 = _vxio_uri__get_path,
  .PathLength           = _vxio_uri__get_path_length,
  .Query                = _vxio_uri__get_query,
  .QueryLength          = _vxio_uri__get_query_length,
  .Fragment             = _vxio_uri__get_fragment,
  .FragmentLength       = _vxio_uri__get_fragment_length,
  .HostIP               = _vxio_uri__get_host_ip,
  .NameInfo             = _vxio_uri__get_nameinfo,
  .Match                = _vxio_uri__match,
  .NewAddrInfo          = _vxio_uri__new_addrinfo,
  .DeleteAddrInfo       = _vxio_uri__delete_addrinfo,
  .Bind                 = _vxio_uri__bind,
  .Listen               = _vxio_uri__listen,
  .Accept               = _vxio_uri__accept,
  .ConnectInetStreamTCP = _vxio_uri__connect_inet_stream_tcp,
  .OpenFile             = _vxio_uri__open_file,
  .OpenNull             = _vxio_uri__open_null,
  .IsConnected          = _vxio_uri__is_connected,
  .Connect              = _vxio_uri__connect,
  .Close                = _vxio_uri__close,
  .NewFormat            = _vxio_uri__new_format,
  .NewFqdn              = _vxio_uri__new_fqdn,
  .Address = {
    .Error              = _vxio_uri__address_error
  },
  .File = {
    .GetDescriptor      = _vxio_uri__file_get_descriptor,
    .IsNullOutput       = _vxio_uri__is_null_output
  },
  .Sock = {
    .Output = {
      .Get              = _vxio_uri__sock_output_get
    },
    .Input = {
      .Get              = _vxio_uri__sock_input_get
    },
    .Error              = _vxio_uri__sock_error,
    .Busy               = _vxio_uri__sock_busy
  },
  .Parse = {
    .SetQueryParam      = __set_URI_query_parameters,
    .ClearQueryParam    = __clear_URI_query_parameters
  }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef CXPLAT_WINDOWS_X64
static const char * __win_format_last_error( char *buf, int maxsz, int *err ) {
  int msgId;
  if( err == NULL || *err <= 0 ) {
    msgId = WSAGetLastError();
    if( err ) {
      *err = msgId;
    }
  }
  else {
    msgId = *err;
  }

  wchar_t message[256] = {0};
  LPWSTR p = (LPWSTR)message;
  FormatMessageW( FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_MAX_WIDTH_MASK,
                  NULL,
                  msgId,
                  MAKELANGID( LANG_NEUTRAL, SUBLANG_DEFAULT ),
                  p,
                  sizeof( message ) - 1,
                  NULL );
  if( maxsz > 512 ) {
    maxsz = 512;
  }
  WideCharToMultiByte( CP_UTF8, 0, message, -1, buf, maxsz, NULL, NULL );
  return buf;
}
#endif




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __set_URI_query_parameters( const char *path, vgx_URIQueryParameters_t *params ) {

  // Example:
  //
  //  /vgx/plugin?func&foo=123&bar=something
  //              ^
  // _buffer:    [func_foo_123_bar_something_]
  //              ^   0^  0^  0^  0^        0
  //              |    |   |   |   |
  //  k0----------'    |   |   |   |
  //  v0 NULL          |   |   |   |
  //  k1 --------------'   |   |   |
  //  v1 ------------------'   |   |
  //  k2 ----------------------'   |
  //  v2 --------------------------'

  // Skip until just after the first '?'
  const char *p = path;
  while( *p && *p++ != '?' );

  if( *p ) {
    params->sz = 1; // first parameter follows '?'
    // Count the number of remaining parameters and find total string length
    const char *z = p;
    while( *z ) {
      if( *z++ == '&' ) {
        params->sz++;
      }
    }
    int64_t len = z - p;

    // Unified allocation:    [.........................................]
    //                        [   KV slots   ][     _buffer chars       ]
    //
    if( (params->_data = malloc( sizeof( vgx_KeyVal_t ) * params->sz + len + 1 )) == NULL ) {
      return -1;
    }
    params->keyval = (vgx_KeyVal_t*)params->_data;
    params->_buffer = (char*)params->_data + sizeof( vgx_KeyVal_t ) * params->sz;

    // Parse
    vgx_KeyVal_t *kv = params->keyval;
    void *item = &kv->key;
    char *b = params->_buffer;
    kv->key = b;
    char c;
    while( (c = *p++) != '\0' ) {
      if( c == '=' && item == &kv->key ) {
        *b++ = '\0';
        item = &kv->val;
        kv->val.data.simple.string = b;
        kv->val.type = VGX_VALUE_TYPE_STRING;
      }
      else if( c == '&' ) {
        *b++ = '\0';
        if( item == &kv->key ) {
          kv->val.data.bits = 0;
          kv->val.type = VGX_VALUE_TYPE_NULL;
        }
        item = &(++kv)->key;
        kv->key = b;
      }
      else {
        *b++ = c;
      }
    }
    *b = '\0';
    if( item == &kv->key ) {
      kv->val.data.bits = 0;
      kv->val.type = VGX_VALUE_TYPE_NULL;
    }

    // Decode
    kv = params->keyval;
    vgx_KeyVal_t *end = kv + params->sz;
    vgx_KeyVal_t *cursor = kv;
    while( (cursor = kv++) < end ) {
      if( (cursor->key && decode_percent_plus_inplace( cursor->key ) < 0) ||
          (cursor->val.type == VGX_VALUE_TYPE_STRING && decode_percent_plus_inplace( (char*)cursor->val.data.simple.string ) < 0)
        )
      {
        __clear_URI_query_parameters( params );
        return -1;
      }

      // Value type conversion
      const char *s = cursor->val.data.simple.string;
      if( cursor->val.type == VGX_VALUE_TYPE_STRING && s && *s++ == '(' ) {
        char *e = NULL;
        int64_t q;
        double d;
        switch( *s++ ) {
        case 'q':
          if( *s++ == ')' ) {
            goto convert_int64;
          }
          continue;
        case 'd':
          if( *s++ == ')' ) {
            goto convert_double;
          }
          continue;
        case 'i':
          if( !strncmp( s, "nt)", 3 ) ) {
            s += 3;
            goto convert_int64;
          }
          continue;
        case 'f':
          if( !strncmp( s, "loat)", 5 ) ) {
            s += 5;
            goto convert_double;
          }
          continue;
        default:
          continue;
        }

      convert_int64:
        q = strtoll( s, &e, 0 );
        if( !e || *e == '\0' ) {
          cursor->val.data.simple.integer = q;
          cursor->val.type = VGX_VALUE_TYPE_INTEGER;
        }
        continue;

      convert_double:
        d = strtof( s, &e );
        if( !e || *e == '\0' ) {
          cursor->val.data.simple.real = d;
          cursor->val.type = VGX_VALUE_TYPE_REAL;
        }
        continue;

      }
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __clear_URI_query_parameters( vgx_URIQueryParameters_t *params ) {
  if( params->keyval ) {
    free( params->keyval );
    memset( params, 0, sizeof( vgx_URIQueryParameters_t ) );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_port( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {
  // scheme : [ // [ userinfo @ ] host [ : port ] / ] path [ ? query ] [ # fragment ]
  //                                     b a

  // path follows port
  if( (*b = *a + CStringFind( CSTR__URI, "/", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                       a        b
  }
  // query follows port
  else if( (*b = *a + CStringFind( CSTR__URI, "?", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                       a               b
  }
  // fragment follows port
  else if( (*b = *a + CStringFind( CSTR__URI, "#", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                       a                           b
  }
  // port until end of uri
  else {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                       a                                        b
    *b = -1;
    b = NULL;
  }

  if( (URI->CSTR__port = CStringSlice( CSTR__URI, a, b )) == NULL ) {
    return -1;
  }

  if( b ) {
    *a = *b + 1;
  }

  const char *port = CStringValue( URI->CSTR__port );
  const char *cursor = port;
  int64_t val = 0;
  while( isdigit( *cursor ) ) {
    int d = *cursor++ - 48;
    val = 10 * val + d;
  }

  if( cursor > port ) {
    if( val > 0 && val <= 0xFFFF ) { 
      URI->port = (unsigned short)val;
      return 0; // ok
    }
  }
  
  __format_error_string( CSTR__error, "invalid URI port: %s", port );

  return -1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_host( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {
  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  //                              a

  // port follows host
  if( (*b = *a + CStringFind( CSTR__URI, ":", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a      b
  }
  // path follows host
  else if( (*b = *a + CStringFind( CSTR__URI, "/", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a                 b
  }
  // query follows host
  else if( (*b = *a + CStringFind( CSTR__URI, "?", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a                        b
  }
  // fragment follows host
  else if( (*b = *a + CStringFind( CSTR__URI, "#", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a                                    b
  }
  // host until end of uri
  else {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a                                                 b
    *b = -1;
    b = NULL;
  }

  if( (URI->CSTR__host = CStringSlice( CSTR__URI, a, b )) == NULL ) {
    __format_error_string( CSTR__error, "internal error in %s", __FUNCTION__ );
    return -1;
  }

  if( b ) {
    *a = *b + 1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_userinfo( CString_t *CSTR__URI, vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  //            b    a
  if( (*b = *a + CStringFind( CSTR__URI, "@", *a )) > *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                 a        b
    if( (URI->CSTR__userinfo = CStringSlice( CSTR__URI, a, b )) == NULL ) {
      __format_error_string( CSTR__error, "internal error in %s", __FUNCTION__ );
      return -1;
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                          b   a
    *a = *b + 1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_authority( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {

  // scheme : /// path [ ? query ] [ # fragment ]
  //        b a
  if( (*b = *a + CStringFind( URI->CSTR__uri, "///", *a )) == *a ) {
    *a += 3;
    *b += 2;
  }

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  //        b   a
  else if( (*b = *a + CStringFind( URI->CSTR__uri, "//", *a )) == *a ) {
    *a += 2;
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //            b    a
    if( __parse_URI_userinfo( URI->CSTR__uri, URI, a, b, CSTR__error ) < 0 ) {
      return -1;
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                              a
    if( __parse_URI_host( URI->CSTR__uri, URI, a, b, CSTR__error ) < 0 ) {
      return -1;
    }

    // end of uri
    if( *b < 0 ) {
      return 0;
    }
    // port is next
    else if(  CStringAtIndex( URI->CSTR__uri, ":", *b ) ) {
      // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
      //                                     b a
      if( __parse_URI_port( URI->CSTR__uri, URI, a, b, CSTR__error ) < 0 ) {
        return -1;
      }
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_path( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {
  // scheme : [ // [ userinfo @ ] host [ : port ] ]/path [ ? query ] [ # fragment ]
  //        b                                      ba 

  // Reverse a one character back. We want the '/'
  --(*a);

  // query follows path
  if( (*b = *a + CStringFind( URI->CSTR__uri, "?", *a )) >= *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                a      b
  }
  // fragment follows path
  else if( (*b = *a + CStringFind( URI->CSTR__uri, "#", *a )) >= *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                a                  b
  }
  // path until end of uri
  else {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                a                               b
    *b = -1;
    b = NULL;
  }

  if( URI_SCHEME__file( iURI.Scheme( URI ) ) ) {
#ifdef CXPLAT_WINDOWS_X64
    // skip leading / for all file paths
    ++(*a);
#else
    // skip leading / for relative file paths
    const char *path = CStringValue( URI->CSTR__uri ) + *a;
    if( *path && *(path+1) == '.') {
      ++(*a);
    }
#endif
  }

  if( (URI->CSTR__path = CStringSlice( URI->CSTR__uri, a, b )) == NULL ) {
    __format_error_string( CSTR__error, "internal error in %s", __FUNCTION__ );
    return -1;
  }

  if( b ) {
    *a = *b + 1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_query( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {
  // scheme : [ // [ userinfo @ ] host [ : port ] ]/path [ ? query ] [ # fragment ]
  //                                                       b a

  // fragment follows query
  if( (*b = *a + CStringFind( URI->CSTR__uri, "#", *a )) >= *a ) {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                         a         b
  }
  // query until end of uri
  else {
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                         a                      b
    *b = -1;
    b = NULL;
  }

  if( (URI->CSTR__query = CStringSlice( URI->CSTR__uri, a, b )) == NULL ) {
    __format_error_string( CSTR__error, "internal error in %s", __FUNCTION__ );
    return -1;
  }

  if( b ) {
    *a = *b + 1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_fragment( vgx_URI_t *URI, int *a, CString_t **CSTR__error ) {
  // scheme : [ // [ userinfo @ ] host [ : port ] ]/path [ ? query ] [ # fragment ]
  //                                                                   b a

  if( (URI->CSTR__fragment = CStringSlice( URI->CSTR__uri, a, NULL )) == NULL ) {
    __format_error_string( CSTR__error, "internal error in %s", __FUNCTION__ );
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __parse_URI_scheme( vgx_URI_t *URI, int *a, int *b, CString_t **CSTR__error ) {
  CString_vtable_t *iStr = (CString_vtable_t*)COMLIB_CLASS_VTABLE( CString_t );

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  // a
  if( (*b = iStr->Find( URI->CSTR__uri, ":", *a )) < *a ) {
    __set_error_string( CSTR__error, "invalid URI: scheme required" );
    return -1;
  }

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  // a      b
  if( (URI->CSTR__scheme = iStr->Slice( URI->CSTR__uri, a, b ) ) == NULL ) {
    return -1;
  }

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  //        b
  //            a           -- OR --                a
  *a = *b + 1;

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __set_input_socket( vgx_URI_t *URI, CXSOCKET sock ) {
  // Close any previous
  iURI.Close( URI );

  // vgx or http schemes only
  const char *scheme = iURI.Scheme( URI );
  if( URI_SCHEME__vgx( scheme ) || URI_SCHEME__http( scheme ) ) {
    // set input
    URI->_sock = sock;
    URI->input.psock = &URI->_sock;
    return 0;
  }

  return -1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __set_output_socket( vgx_URI_t *URI, CXSOCKET sock ) {
  // Close any previous
  iURI.Close( URI );

  // vgx scheme only
  const char *scheme = iURI.Scheme( URI );
  if( URI_SCHEME__vgx( scheme ) ) {
    // set output
    URI->_sock = sock;
    URI->output.psock = &URI->_sock;
    return 0;
  }

  return -1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_URI_t * _vxio_uri__new( const char *uri, CString_t **CSTR__error ) {

  vgx_URI_t *URI = NULL;

  //
  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
  //

  int A = 0;
  int B = 0;

  int *a = &A;
  int *b = &B;

  XTRY {
    // URI object
    if( (URI = calloc( 1, sizeof( vgx_URI_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Original URI string
    if( (URI->CSTR__uri = CStringNew( uri )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    // a
    if( __parse_URI_scheme( URI, a, b, CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //        b   a
    if( __parse_URI_authority( URI, a, b, CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_API, 0x004 );
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ]/path [ ? query ] [ # fragment ]
    //                                               ba 
    if( *b > 0 && (CStringAtIndex( URI->CSTR__uri, "/", *b )) ) {
      if( __parse_URI_path( URI, a, b, CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x005 );
      }
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                       b a 
    if( *b > 0 && CStringAtIndex( URI->CSTR__uri, "?", *b ) ) {
      if( __parse_URI_query( URI, a, b, CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x006 );
      }
    }

    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //                                                                   b a 
    if( *b > 0 && CStringAtIndex( URI->CSTR__uri, "#", *b ) ) {
      if( __parse_URI_fragment( URI, a, CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x007 );
      }
    }


    // Validate file URI
    //
    const char *scheme = iURI.Scheme( URI );
    if( URI_SCHEME__file( scheme ) ) {
      const char *host = iURI.Host( URI );
      if( !CharsEqualsConst( host, "localhost" ) && !CharsEqualsConst( host, "" ) ) {
        __format_error_string( CSTR__error, "only localhost supported for files (got host=%s)", host );
        THROW_SILENT( CXLIB_ERR_API, 0x008 );
      }
    }


  }
  XCATCH( errcode ) {
    __format_error_string( CSTR__error, "internal error 0x%0X", errcode );
    _vxio_uri__delete( &URI );
  }
  XFINALLY {
  }

  return URI;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_URI_t * _vxio_uri__new_elements( const char *scheme, const char *userinfo, const char *host, unsigned short port, const char *path, const char *query, const char *fragment, CString_t **CSTR__error ) {

  vgx_URI_t *URI = NULL;

  vgx_StreamBuffer_t *stream = iStreamBuffer.New( 8 );
  if( stream == NULL ) {
    return NULL;
  }

#define WRITE_STRING( Str ) iStreamBuffer.WriteString( stream, Str )
#define WRITE_CONST( Str )  iStreamBuffer.Write( stream, Str, sizeof(Str)-1 )

  XTRY {
    //
    // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]
    //
    if( scheme == NULL ) {
      __set_error_string( CSTR__error, "URI scheme cannot be NULL" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    // scheme
    WRITE_STRING( scheme );
    WRITE_CONST( "://" );

    // userinfo
    if( userinfo ) {
      WRITE_STRING( userinfo );
      WRITE_CONST( "@" );
    }

    // host
    if( host ) {
      WRITE_STRING( host );
    }

    // port
    if( port > 0 ) {
      WRITE_CONST( ":" );
      char digits[8] = {0};
      snprintf( digits, 7, "%u", port );
      WRITE_STRING( digits );
    }

    // path
    if( path ) {
      if( path[0] != '/' ) {
        WRITE_CONST( "/" );
      }
      WRITE_STRING( path );
    }

    // query
    if( query && query[0] ) {
      WRITE_CONST( "?" );
      WRITE_STRING( query );
    }

    // fragment
    if( fragment && fragment[0] ) {
      WRITE_CONST( "#" );
      WRITE_STRING( fragment );
    }

    // TERMINATE
    iStreamBuffer.TermString( stream );

    const char *segment = NULL;
    iStreamBuffer.ReadableSegment( stream, LLONG_MAX, &segment, NULL );

    if( (URI = iURI.New( segment, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x015 );
    }

  }
  XCATCH( errcode ) {
    iURI.Delete( &URI );
  }
  XFINALLY {
    iStreamBuffer.Delete( &stream );
  }

  return URI;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_URI_t * _vxio_uri__clone( const vgx_URI_t *URI ) {
  vgx_URI_t *clone = calloc( 1, sizeof( vgx_URI_t ) );
  if( clone ) {
    XTRY {
      // ip
      if( URI->CSTR__ip && (clone->CSTR__ip = iString.Clone( URI->CSTR__ip )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x000 );
      }

      // nameinfo
      if( URI->CSTR__nameinfo && (clone->CSTR__nameinfo = iString.Clone( URI->CSTR__nameinfo )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x000 );
      }

      // uri
      if( URI->CSTR__uri && (clone->CSTR__uri = iString.Clone( URI->CSTR__uri )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // scheme
      if( URI->CSTR__scheme && (clone->CSTR__scheme = iString.Clone( URI->CSTR__scheme )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

      // userinfo
      if( URI->CSTR__userinfo && (clone->CSTR__userinfo = iString.Clone( URI->CSTR__userinfo )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }

      // host
      if( URI->CSTR__host && (clone->CSTR__host = iString.Clone( URI->CSTR__host )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }

      // port
      if( URI->CSTR__port && (clone->CSTR__port = iString.Clone( URI->CSTR__port )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
      }
      clone->port = URI->port;

      // path
      if( URI->CSTR__path && (clone->CSTR__path = iString.Clone( URI->CSTR__path )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
      }

      // query
      if( URI->CSTR__query && (clone->CSTR__query = iString.Clone( URI->CSTR__query )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x007 );
      }

      // fragment
      if( URI->CSTR__fragment && (clone->CSTR__fragment = iString.Clone( URI->CSTR__fragment )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x008 );
      }
    }
    XCATCH( errcode ) {
      iURI.Delete( &clone );
    }
    XFINALLY {
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void _vxio_uri__delete( vgx_URI_t **URI ) {
  if( URI && *URI) {
    iURI.Close( *URI );

    iString.Discard( &(*URI)->CSTR__ip );
    iString.Discard( &(*URI)->CSTR__nameinfo );
    iString.Discard( &(*URI)->CSTR__uri );
    iString.Discard( &(*URI)->CSTR__scheme );
    iString.Discard( &(*URI)->CSTR__userinfo );
    iString.Discard( &(*URI)->CSTR__host );
    iString.Discard( &(*URI)->CSTR__port );
    iString.Discard( &(*URI)->CSTR__path );
    iString.Discard( &(*URI)->CSTR__query );
    iString.Discard( &(*URI)->CSTR__fragment );

    free( (void*)*URI );
    *URI = NULL;
  }
}


#define __null_uri "(URI=null)"


/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_uri( const vgx_URI_t *URI ) {
#define __default_uri ""
  return URI ? CStringValueDefault( URI->CSTR__uri, __default_uri ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_scheme( const vgx_URI_t *URI ) {
#define __default_scheme ""
  return URI ? CStringValueDefault( URI->CSTR__scheme, __default_scheme ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_userinfo( const vgx_URI_t *URI ) {
#define __default_userinfo ""
  return URI ? CStringValueDefault( URI->CSTR__userinfo, __default_userinfo ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_host( const vgx_URI_t *URI ) {
#define __default_host "localhost"
  return URI ? CStringValueDefault( URI->CSTR__host, __default_host ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_port( const vgx_URI_t *URI ) {
#define __default_port ""
  return URI ? CStringValueDefault( URI->CSTR__port, __default_port ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static unsigned short _vxio_uri__get_port_int( const vgx_URI_t *URI ) {
  return URI ? URI->port : 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_path( const vgx_URI_t *URI ) {
#define __default_path "/"
  return URI ? CStringValueDefault( URI->CSTR__path, __default_path ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t _vxio_uri__get_path_length( const vgx_URI_t *URI ) {
  return URI && URI->CSTR__path ? CStringLength( URI->CSTR__path ) : 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_query( const vgx_URI_t *URI ) {
#define __default_query ""
  return URI ? CStringValueDefault( URI->CSTR__query, __default_query ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t _vxio_uri__get_query_length( const vgx_URI_t *URI ) {
  return URI && URI->CSTR__query ? CStringLength( URI->CSTR__query ) : 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_fragment( const vgx_URI_t *URI ) {
#define __default_fragment ""
  return URI ? CStringValueDefault( URI->CSTR__fragment, __default_fragment ) : __null_uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t _vxio_uri__get_fragment_length( const vgx_URI_t *URI ) {
  return URI && URI->CSTR__fragment ? CStringLength( URI->CSTR__fragment ) : 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_host_ip( vgx_URI_t *URI ) {
#define __default_host_ip "0.0.0.0"
  if( URI->CSTR__ip == NULL ) {
    const char *host = _vxio_uri__get_host( URI );
    if( host ) {
      char *ip = cxgetip( host );
      if( ip ) {
        URI->CSTR__ip = CStringNew( ip );
        free( ip );
      }
    }
  }
  const char *host_ip = URI ? CStringValueDefault( URI->CSTR__ip, __default_host_ip ) : __null_uri;
  return host_ip;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * _vxio_uri__get_nameinfo( vgx_URI_t *URI ) {
#define __default_nameinfo ""
  if( URI->CSTR__nameinfo == NULL ) {
    const char *host = _vxio_uri__get_host( URI );
    if( host ) {
      char *name = cxgetnameinfo( host ); // NULL if host is not IP address or other error
      if( name ) {
        URI->CSTR__nameinfo = CStringNew( name );
        free( name );
      }
    }
  }
  const char *nameinfo = URI ? CStringValueDefault( URI->CSTR__nameinfo, __default_nameinfo ) : __null_uri;
  return nameinfo;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool _vxio_uri__match( const vgx_URI_t *A, const vgx_URI_t *B ) {
  return A && B && CStringEquals( A->CSTR__uri, B->CSTR__uri );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static struct addrinfo * _vxio_uri__new_addrinfo( const vgx_URI_t *URI, CString_t **CSTR__error ) {
  struct addrinfo *address = NULL;
  const char *host = iURI.Host( URI );
  const char *port = iURI.PortInt( URI ) > 0 ? iURI.Port( URI ) : NULL;
  int err;
  if( (address = cxgetaddrinfo( host, port, &err )) == NULL ) {
    iURI.Address.Error( CSTR__error, err );
    iURI.DeleteAddrInfo( &address );
  }
  return address;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void _vxio_uri__delete_addrinfo( struct addrinfo **address ) {
  cxfreeaddrinfo( address );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxio_uri__bind( vgx_URI_t *URI, CString_t **CSTR__error ) {
  if( URI == NULL ) {
    return -1;
  }

  iURI.Close( URI );
  const char *scheme = iURI.Scheme( URI );

  // Check scheme
  if( !(URI_SCHEME__vgx( scheme ) || URI_SCHEME__http( scheme )) ) {
    __format_error_string( CSTR__error, "Scheme '%s' not compatible with socket bind", scheme );
    return -1;
  }

  // Create the socket
  if( (URI->input.psock = cxsocket( &URI->_sock, AF_INET, SOCK_STREAM, IPPROTO_TCP, true, 0, true )) == NULL ) {
    __set_error_string( CSTR__error, "Failed to create socket" );
    return -1;
  }

  int err;
  const char *host = iURI.Host( URI );
  unsigned short port = iURI.PortInt( URI );

  // bind
  if( (err = cxbind( URI->input.psock, AF_INET, host, port )) != 0 ) {
    iURI.Sock.Error( URI, CSTR__error, err, 0 );
    const char *serr = (CSTR__error && *CSTR__error) ? CStringValue( *CSTR__error ) : "?";
    IO_REASON( URI, 0x001, "Bind error: %s", serr );
    iURI.Close( URI );
    return -1;
  }
    
  // ok
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxio_uri__listen( vgx_URI_t *URI, int backlog, CString_t **CSTR__error ) {
  if( URI == NULL || URI->input.psock == NULL ) {
    return -1;
  }

  cxsocket_set_nonblocking( URI->input.psock );

  int err;
  if( (err = cxlisten( URI->input.psock, backlog )) != 0 ) {
    iURI.Sock.Error( URI, CSTR__error, err, 0 );
    iURI.Close( URI );
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_URI_t * _vxio_uri__accept( vgx_URI_t *URI, const char *scheme, CString_t **CSTR__error ) {
  vgx_URI_t *ClientURI = NULL;

  CXSOCKET accepted;

  struct sockaddr_in addr = {0};
  int addrlen = sizeof( struct sockaddr_in );
  if( cxaccept( &accepted, URI->input.psock, (struct sockaddr*)&addr, &addrlen ) < 0 ) {
    iURI.Sock.Error( URI, CSTR__error, -1, 0 );
    return NULL;
  }

  char buffer[64] = {0};
  char *ip = buffer;
  unsigned short port;
  cxsockaddrtohostport( (struct sockaddr*)&addr, &ip, 63, &port );

  if( (ClientURI = iURI.NewElements( scheme, NULL, ip, port, NULL, NULL, NULL, CSTR__error )) == NULL ) {
    return NULL;
  }

  if( __set_input_socket( ClientURI, accepted ) < 0 ) {
    iURI.Delete( &ClientURI );
  }

  // Return URI object for accepted socket
  return ClientURI;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CXSOCKET * _vxio_uri__connect_inet_stream_tcp( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error, int *rerr ) {
  iURI.Close( URI );
  const char *scheme = iURI.Scheme( URI );
  if( URI_SCHEME__vgx( scheme ) || URI_SCHEME__http( scheme ) ) {
    // Get address info from URI
    struct addrinfo *address = iURI.NewAddrInfo( URI, CSTR__error );
    if( address ) {
      int err = 0;

      // Scan to last address unless address is 127.0.0.1
      struct addrinfo *a = cxislocalhost( address->ai_addr ) ? address : cxlowaddr( address );

      do {
        // Create socket
        if( (URI->output.psock = cxsocket( &URI->_sock, AF_INET, SOCK_STREAM, IPPROTO_TCP, true, 0, false )) == NULL ) {
          // Error
          iURI.Sock.Error( URI, CSTR__error, -1, 0 );
          break;
        }

        // Set socket to nonblocking
        if( cxsocket_set_nonblocking( URI->output.psock ) < 0 ) {
          // Error
          iURI.Sock.Error( URI, CSTR__error, -1, 0 );
          break;
        }

        // Discard any previous error string
        if( CSTR__error && *CSTR__error ) {
          iString.Discard( CSTR__error );
        }

        // Use socket to connect to current address name
        // Connect.
        // If timeout is nonzero this will block until connection is made or timeout is reached
        // If timeout is zero this will return immediately and it will be necessary for user 
        // to later verify the socket is connected before sending or receiving data.
        short revents = 0;
        if( (err = cxconnect( URI->output.psock, a->ai_addr, a->ai_addrlen, timeout_ms, &revents )) > 0 ) {
#ifdef HASVERBOSE
          // Success
          int64_t s = (int64_t)URI->output.psock->s;
          IO_VERBOSE( URI, 0x001, "Socket %lld connected", s );
#endif
        }
        // Failed to connect
        else {
          // Timeout
          if( err == 0 ) {
            __format_error_string( CSTR__error, "[URI=%s] Socket connect timeout", iURI.URI( URI ) );
          }
          // Connection error
          else {
            // Get error message string
            *rerr = iURI.Sock.Error( URI, CSTR__error, err, revents );
            // Log error
            if( CSTR__error && *CSTR__error ) {
              IO_REASON( URI, 0x002, "%s", CStringValue( *CSTR__error ) );
            }
          }
          // Close
          iURI.Close( URI );
        }

        //
        iURI.NameInfo( URI );
      } WHILE_ZERO;

      // Clean up address info
      iURI.DeleteAddrInfo( &address );
    }
  }
  else {
    __format_error_string( CSTR__error, "Scheme '%s' not compatible with socket connection", scheme );
  }

  return URI->output.psock;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxio_uri__open_file( vgx_URI_t *URI, CString_t **CSTR__error ) {
  iURI.Close( URI );
  const char *scheme = iURI.Scheme( URI );
  if( URI_SCHEME__file( scheme ) ) {

    const char *path = iURI.Path( URI );
    errno_t err = 0;
    if( path == NULL ) {
      __set_error_string( CSTR__error, "internal error" );
      return -1;
    }
    else if( 
        /* create file */
        (err = OPEN_W_SEQ( &URI->_fd, path )) != 0 
        ||
        /* close */
        CX_CLOSE( URI->_fd) != 0
        ||
        /* open r/w */
        (err = OPEN_RW_SEQ( &URI->_fd, path )) != 0 )
    {
      __format_error_string( CSTR__error, "%d %s", err, strerror( err ) );
      URI->output.pfd = NULL;
      return -1;
    }
    else {
      URI->output.pfd = &URI->_fd;
      IO_VERBOSE( URI, 0x001, "File output" );
    }
    
    // ok
    return 0;
  }
  else {
    __format_error_string( CSTR__error, "Scheme '%s' not compatible with file output", scheme );
    return -1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxio_uri__open_null( vgx_URI_t *URI, CString_t **CSTR__error ) {
  iURI.Close( URI );
  const char *scheme = iURI.Scheme( URI );
  if( URI_SCHEME__null( scheme ) ) {
    URI->_fd = 0;
    URI->output.pfd = &URI->_fd;
    IO_VERBOSE( URI, 0x001, "Null output" );
    // ok
    return 0;
  }
  else {
    __format_error_string( CSTR__error, "Scheme '%s' not compatible with null output", scheme );
    return -1;
  }
}




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxio_uri__is_connected( const vgx_URI_t *URI ) {
  if( URI && (URI->output.psock || URI->input.psock || URI->output.pfd ) ) {
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxio_uri__connect( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error ) {
  int ret = 0;
  if( URI ) {
    const char *scheme = iURI.Scheme( URI );
    // Socket is not connected, try to reconnect (if 'vgx' or 'http' scheme)
    if( URI->output.psock == NULL && (URI_SCHEME__vgx( scheme ) || URI_SCHEME__http( scheme )) ) {
      int err = 0;
      if( iURI.ConnectInetStreamTCP( URI, timeout_ms, CSTR__error, &err ) == NULL ) {
        if( err > 0 ) {
          ret = -err;
        }
        else {
          ret = -1;
        }
      }
    }
    else if( URI->output.pfd == NULL && URI_SCHEME__file( scheme ) ) {
      if( iURI.OpenFile( URI, CSTR__error ) < 0 ) {
        ret = -1;
      }
    }
    else if( URI->output.pfd == NULL && URI_SCHEME__null( scheme ) ) {
      if( iURI.OpenNull( URI, CSTR__error ) < 0 ) {
        ret = -1;
      }
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void _vxio_uri__close( vgx_URI_t *URI ) {
  if( URI ) {
    const char *scheme = iURI.Scheme( URI );
    if( URI_SCHEME__vgx( scheme ) || URI_SCHEME__http( scheme ) ) {
      int64_t s;
      // Output
      if( URI->output.psock ) {
        if( (s = cxclose( &URI->output.psock )) > 0 ) {
          IO_VERBOSE( URI, 0x001, "Socket (out) %lld closed", s );
        }
      }
      // Input
      if( URI->input.psock ) {
        if( (s = cxclose( &URI->input.psock )) > 0 ) {
          IO_VERBOSE( URI, 0x002, "Socket (in) %lld closed", s );
        }
      }
    }
    else if( URI_SCHEME__file( scheme ) ) {
      if( URI->output.pfd ) {
        CX_CLOSE( *URI->output.pfd );
        IO_VERBOSE( URI, 0x003, "File output closed" );
        *URI->output.pfd = 0;
        URI->output.pfd = NULL;
      }
    }
    else if( URI_SCHEME__null( scheme ) ) {
      URI->output.pfd = NULL;
    }
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CString_t * _vxio_uri__new_format( const vgx_URI_t *URI ) {
  CString_t *CSTR__uri = NULL;

  // scheme : [ // [ userinfo @ ] host [ : port ] ] path [ ? query ] [ # fragment ]

  const char *scheme = iURI.Scheme( URI );
  const char *userinfo = iURI.UserInfo( URI );
  const char *host = iURI.Host( URI );
  const char *port = iURI.Port( URI );
  const char *path = iURI.Path( URI );
  const char *query = iURI.Query( URI );
  const char *fragment = iURI.Fragment( URI );

  CByteList_t *S = COMLIB_OBJECT_NEW_DEFAULT( CByteList_t );
  typedef int64_t (*f_write)( CByteList_t *L, const BYTE *data, int64_t n );
#define write( String ) writef( S, (const BYTE*)(String), strlen(String) )
  if( S ) {
    f_write writef = CALLABLE( S )->Extend;
    write( scheme );
    write( ":" );
    if( *host ) {
      write( "//" );
      if( *userinfo ) {
        write( userinfo );
        write( "@" );
      }
      write( host );
      if( *port ) {
        write( ":" );
        write( port );
      }
    }
    if( *path ) {
      write( path );
    }
    if( *query ) {
      write( "?" );
      write( query );
    }
    if( *fragment ) {
      write( "#" );
      write( fragment );
    }
    BYTE term = 0;
    CALLABLE( S )->Append( S, &term );

    const char *cursor = (char*)CALLABLE( S )->Cursor( S, 0 );
    CSTR__uri = CStringNew( cursor );
    COMLIB_OBJECT_DESTROY( S );
  }
#undef write

  return CSTR__uri;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CString_t * _vxio_uri__new_fqdn( void ) {
  CString_t *CSTR__fqdn = NULL;
  char name[1024] = {0};
  if( gethostname( name, 1023 ) == 0 ) {
    int err = 0;
    struct addrinfo *address;
    if( (address = cxgetaddrinfo( name, NULL, &err )) != NULL ) {
      struct addrinfo *a = address;
      do {
        if( a->ai_canonname ) {
          CSTR__fqdn = CStringNew( a->ai_canonname );
          break;
        }
      } while( (a = a->ai_next) != NULL );
      cxfreeaddrinfo( &address );
    }
  }
  return CSTR__fqdn;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxio_uri__address_error( CString_t **CSTR__error, int err ) {
  static const char unknown[] = "Unknown error";
  const char *serr = NULL;
  char utf8msg[512] = {0};
  cxlib_ostream_lock();
#ifdef CXPLAT_WINDOWS_X64
  serr = __win_format_last_error( utf8msg, 512, &err );
#else
  if( err == 0 ) {
    serr = unknown;
  }
  else {
    if( err < 0 ) {
      err = errno;
    }
    serr = gai_strerror( err );
  }
#endif

  if( CSTR__error ) {
    __format_error_string( CSTR__error, "Address error: %d [%s]", err, serr );
  }
  else {
    IO_REASON( NULL, 0x001, "ADDRESS ERROR: %d [%s]", err, serr );
  }
  cxlib_ostream_release();
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxio_uri__file_get_descriptor( vgx_URI_t *URI ) {
  int fd = 0;
  if( URI && URI->output.pfd ) {
    // Return open file
    fd = *URI->output.pfd;
  }
  return fd;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool _vxio_uri__is_null_output( vgx_URI_t *URI ) {
  return URI && URI->output.pfd && *URI->output.pfd == 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CXSOCKET * _vxio_uri__sock_output_get( vgx_URI_t *URI ) {
  CXSOCKET *psock = NULL;
  // Output socket connected
  if( URI && URI->output.psock ) {
    // Return connected output socket
    psock = URI->output.psock;
  }
  return psock;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CXSOCKET * _vxio_uri__sock_input_get( vgx_URI_t *URI ) {
  CXSOCKET *psock = NULL;
  // Input socket connected
  if( URI && URI->input.psock ) {
    // Return connected input socket
    psock = URI->input.psock;
  }
  return psock;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxio_uri__sock_error( vgx_URI_t *URI, CString_t **CSTR__error, int err, short revents ) {
  static const char unknown[] = "Unknown error";
  const char *serr = NULL;
  char utf8msg[512] = {0};
  cxlib_ostream_lock();
#ifdef CXPLAT_WINDOWS_X64
  if( (revents & POLLHUP) != 0 ) {
    err = WSAECONNREFUSED;
  }
  serr = __win_format_last_error( utf8msg, 512, &err );
#else
  if( (revents & POLLHUP) != 0 ) {
    err = ECONNREFUSED;
  }
  if( err == 0 ) {
    serr = unknown;
  }
  else {
    if( err < 0 ) {
      err = errno;
    }
    serr = strerror( err );
  }
#endif

  if( CSTR__error ) {
    __format_error_string( CSTR__error, "[URI=%s] Socket error: %d [%s]", iURI.URI( URI ), err, serr );
  }
  else {
    IO_REASON( URI, 0x001, "SOCKET ERROR: %d [%s]", err, serr );
  }
  cxlib_ostream_release();
  return err;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int _vxio_uri__sock_busy( int err ) {
  int busy = 0;
  if(
#ifdef CXPLAT_WINDOWS_X64
    WSAGetLastError() == WSAEWOULDBLOCK
#else
    err == EWOULDBLOCK || err == EAGAIN
#endif
  ) {
    busy = 1;
  }
  return busy;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxio_uri.h"
  
test_descriptor_t _vgx_vxio_uri_tests[] = {
  { "VGX IO URI Tests", __utest_vxio_uri },
  {NULL}
};
#endif
