/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_parser.c
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
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int                  __match_lower_prefix( const char *str, const char *prefix );
static const char *         __expect_token( const char *tc, const char *cp );
static const char *         __read_string( const char *sc, const char *cp );
static HTTPRequestMethod    __parse_initial__method( const char **p );
static char *               __parse_initial__path( vgx_VGXServerRequest_t *request, const char **p );

static vgx_MediaType        __parse_header__accept( const char *data );
static vgx_MediaType        __parse_header__content_type( const char *data );
static int64_t              __parse_header__content_length( const char *data );
static int8_t               __parse_header__x_vgx_partial_target( const char *data );
static int                  __parse_header__x_vgx_builtin_min_executor( vgx_VGXServer_t *server, const char *data );
static int16_t              __parse_header__x_vgx_backlog( const char *data );
static int                  __parse_header__x_vgx_bypass_sout( const char *data );
static void                 __parse_header__ignore( const char *data );


#define IS_HEADER_END( Line ) ( *(Line) == '\r' && *((Line)+1) == '\n' )


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __match_lower_prefix( const char *str, const char *prefix ) {
  // Lowercase alpha match, digits and most symbols,
  // EXCEPT:  Control chars 0 - 31
  //          @ [ \ ] ^ _ 

  while( *str && (*str | 0x20) == *prefix ) {
    ++str;
    ++prefix;
  }

  // Match if we got to the end of prefix
  return *prefix == '\0';
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __expect_token( const char *tc, const char *cp ) {
  while( *cp && *tc && *cp++ == *tc++ );
  return (*cp > 32 || *tc) ? NULL : cp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __read_string( const char *sc, const char *cp ) {
  while( *cp && *sc && *cp++ == *sc++ );
  return *sc ? NULL : cp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __parse_BYTE( const char **cp, BYTE *dst ) {
  int acc = 0;
  do {
    unsigned d = (unsigned)(**cp - 48);
    if( d > 9 ) {
      return NULL;
    }
    acc *= 10;
    acc += (int)d;
    ++(*cp);
  } while( **cp > 32 );
  if( acc > UCHAR_MAX) {
    return NULL;
  }
  *dst = (BYTE)acc;
  return *cp;
}




#define __skip_spaces( ptr ) while( *(ptr) && *(ptr) <= 32 ) { ++(ptr); }
#define __skip_line( ptr ) while( *(ptr) && *(ptr) != '\n' ) { ++(ptr); }
#define __is_end_or_comment( ptr ) (!*(ptr) || *(ptr) == '#')



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static HTTPRequestMethod __parse_initial__method( const char **p ) {
#define expect_method( m ) if( (*p = __expect_token( m, *p )) == NULL ) { goto bad_method; }

  // HTTP request method
  switch( **p ) {
  // GET
  case 'G':
    expect_method( "GET" )
    return HTTP_GET;
  // HEAD
  case 'H':
    expect_method( "HEAD" )
    return HTTP_HEAD;
  // POST/PUT/PATCH
  case 'P':
    switch( *(*p+1) ) {
    // POST
    case 'O':
      expect_method( "POST" )
      return HTTP_POST;
    // PUT
    case 'U':
      expect_method( "PUT" )
      return HTTP_PUT;
    // PATCH
    case 'A':
      expect_method( "PATCH" )
      return HTTP_PATCH;
    // ???
    default:
      goto bad_method;
    }
  // XVGX****
  case 'X':
    if( !CharsStartsWithConst( *p, "XVGX" ) ) {
      goto bad_method;
    }
    switch( *(*p+4) ) {
    // XVGXIDENT
    case 'I':
      expect_method( "XVGXIDENT" )
      return XVGX_IDENT;
    // ???
    default:
      goto bad_method;
    }
  // OPTIONS
  case 'O':
    expect_method( "OPTIONS" )
    return HTTP_OPTIONS;
  // DELETE
  case 'D':
    expect_method( "DELETE" )
    return HTTP_DELETE;
  // CONNECT
  case 'C':
    expect_method( "CONNECT" )
    return HTTP_CONNECT;
  // TRACE
  case 'T':
    expect_method( "TRACE" )
    return HTTP_TRACE;
  }

bad_method:
  return HTTP_NONE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __parse_initial__path( vgx_VGXServerRequest_t *request, const char **p ) {
  char *pwp = request->path;
  const char *pend = request->path + HTTP_PATH_MAX - 1;
  const char *cp = *p;
  while( *cp > 32 && pwp < pend ) {
    *pwp++ = *cp++;
  }
  *pwp = '\0';
  if( *cp > 32 ) {
    return NULL;
  }
  *p = cp;
  request->sz_path = (int16_t)(pwp - request->path);
  return request->path;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __parse_xvgx_ident( vgx_VGXServerRequest_t *request, const char **cp ) {
  vgx_VGXServerClient_t *client = request->headers->client;
  if( client == NULL ) {
    return -1;
  }
  // ------
  // matrix
  // ------

  // width
  if( __parse_BYTE( cp, &client->partial_ident.matrix.width ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );
  // height
  if( __parse_BYTE( cp, &client->partial_ident.matrix.height ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );
  // depth
  if( __parse_BYTE( cp, &client->partial_ident.matrix.depth ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );

  // --------
  // position
  // --------

  // partition
  if( __parse_BYTE( cp, &client->partial_ident.position.partition ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );
  // replica
  if( __parse_BYTE( cp, &client->partial_ident.position.replica ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );
  // channel
  if( __parse_BYTE( cp, &client->partial_ident.position.channel ) == NULL ) {
    return -1;
  }
  __skip_spaces( *cp );
  // primary
  if( __parse_BYTE( cp, &client->partial_ident.position.primary ) == NULL ) {
    return -1;
  }

  client->partial_ident.defined = 1;

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_parser__parse_request_initial_line( vgx_VGXServerRequest_t *request, const char *line ) {
  const char *cp = line;

  // HTTP method
  if( (request->method = __parse_initial__method( &cp )) == HTTP_NONE ) {
    goto bad_method;
  }

  __skip_spaces( cp );

  // Proper HTTP protocol
  if( request->method <= __HTTP_MAX_MASK ) {
    // HTTP request path
    if( __parse_initial__path( request, &cp ) == NULL ) {
      goto bad_path;
    }
    __skip_spaces( cp );

    // HTTP version
    if( (cp = __read_string( "HTTP/", cp )) == NULL ) {
      goto bad_protocol;
    }
    if( (request->version.major = *cp++ - 48) < 0 ) {
      goto bad_version;
    }
    if( *cp++ != '.' ) {
      goto bad_version;
    }
    if( (request->version.minor = *cp++ - 48) < 0 ) {
      goto bad_version;
    }
  }
  // XVGX extended protocol
  else {
    switch( request->method ) {
    case XVGX_IDENT:
      if( __parse_xvgx_ident( request, &cp ) < 0 ) {
        goto bad_protocol;
      }
      request->version.major = 1;
      request->version.minor = 1;
      break;
    default:
      goto bad_method;
    }
  }

  __skip_line( cp );
  if( *cp != '\n' ) {
    goto bad_line;
  }

  // request line ok
  return 1;

bad_method:
  goto error;
bad_path:
  goto error;
bad_protocol:
  goto error;
bad_version:
  goto error;
bad_line:
  goto error;
error:
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_parser__parse_response_initial_line( const char *line, vgx_VGXServerResponse_t *response ) {
  const char *cp = line;
  // HTTP version
  if( (cp = __read_string( "HTTP/", cp )) == NULL ) {
    goto bad_protocol;
  }
  if( (*cp++ - 48) < 0 ) {
    goto bad_version;
  }
  if( *cp++ != '.' ) {
    goto bad_version;
  }
  if( (*cp++ - 48) < 0 ) {
    goto bad_version;
  }
  __skip_spaces( cp );

  // Code
  int64_t value = 0;
  if( (cp = decimal_to_integer( cp, &value )) == NULL || value < 0 ) {
    goto bad_status;
  }
  response->status.code = (HTTPStatus)value;

  // Skip until end of line
  __skip_line( cp );
  if( *cp != '\n' ) {
    goto bad_line;
  }

  // response line ok
  return 1;

bad_protocol:
bad_version:
bad_status:
bad_line:
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_Accept "accept:"
#define sz_HEADER_Accept (sizeof( HEADER_Accept ) - 1)
#define IS_HEADER_Accept( Line ) __match_lower_prefix( Line, HEADER_Accept )

/**************************************************************************//**
 * __parse_header__accept
 *
 ******************************************************************************
 */
__inline static vgx_MediaType __parse_header__accept( const char *data ) {
  vgx_MediaType accept;
  data += sz_HEADER_Accept;
  __skip_spaces( data );
  // TODO: Content negotiation.
  //       Right now we do a simple check.
  if( CharsStartsWithConst( data, "application/x-vgx-partial" ) ) {
    accept = MEDIA_TYPE__application_x_vgx_partial;
  }
  else if( CharsStartsWithConst( data, "application/json" ) ) {
    accept = MEDIA_TYPE__application_json;
  }
  else if( CharsStartsWithConst( data, "text/plain" ) ) {
    accept = MEDIA_TYPE__text_plain;
  }
  else {
    accept = MEDIA_TYPE__ANY;
  }
  __skip_line( data );
  return accept;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_ContentType "content-type:"
#define sz_HEADER_ContentType (sizeof( HEADER_ContentType ) - 1)
#define IS_HEADER_ContentType( Line ) __match_lower_prefix( Line, HEADER_ContentType )

/**************************************************************************//**
 * __parse_header__content_type
 *
 ******************************************************************************
 */
__inline static vgx_MediaType __parse_header__content_type( const char *data ) {
  vgx_MediaType content_type;
  data += sz_HEADER_ContentType;
  __skip_spaces( data );
  if( CharsStartsWithConst( data, "application/x-vgx-partial" ) ) {
    content_type = MEDIA_TYPE__application_x_vgx_partial;
  }
  else if( CharsStartsWithConst( data, "application/json" ) ) {
    content_type = MEDIA_TYPE__application_json;
  }
  else if( CharsStartsWithConst( data, "text/plain" ) ) {
    content_type = MEDIA_TYPE__text_plain;
  }
  else {
    content_type = MEDIA_TYPE__NONE;
  }
  __skip_line( data );
  return content_type;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_ContentLength "content-length:"
#define sz_HEADER_ContentLength (sizeof(HEADER_ContentLength) - 1)
#define IS_HEADER_ContentLength( Line ) __match_lower_prefix( Line, HEADER_ContentLength )

/**************************************************************************//**
 * __parse_header__content_length
 *
 ******************************************************************************
 */
__inline static int64_t __parse_header__content_length( const char *data ) {
  data += sz_HEADER_ContentLength;
  __skip_spaces( data );
  int64_t n = 0;
  if( (data = decimal_to_integer( data, &n )) == NULL ) {
    return -1;
  }
  __skip_line( data );
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_XVgxPartialTarget "x-vgx-partial-target:"
#define sz_HEADER_XVgxPartialTarget (sizeof(HEADER_XVgxPartialTarget) - 1)
#define IS_HEADER_XVgxPartialTarget( Line ) __match_lower_prefix( Line, HEADER_XVgxPartialTarget )

/**************************************************************************//**
 * __parse_header__x_vgx_partial_target
 *
 ******************************************************************************
 */
__inline static int8_t __parse_header__x_vgx_partial_target( const char *data ) {
  data += sz_HEADER_XVgxPartialTarget;
  __skip_spaces( data );
  int64_t target = 0;
  if( (data = decimal_to_integer( data, &target )) == NULL ) {
    return -1;
  }
  __skip_line( data );
  if( target < 0 ) {
    return -1;
  }
  return (int8_t)target;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_XVgxBuiltinMinExecutor "x-vgx-builtin-min-executor:"
#define sz_HEADER_XVgxBuiltinMinExecutor (sizeof(HEADER_XVgxBuiltinMinExecutor) - 1)
#define IS_HEADER_XVgxBuiltinMinExecutor( Line ) __match_lower_prefix( Line, HEADER_XVgxBuiltinMinExecutor )

/**************************************************************************//**
 * __parse_header__x_vgx_builtin_min_executor
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __parse_header__x_vgx_builtin_min_executor( vgx_VGXServer_t *server, const char *data ) {
  int pool_n;
  data += sz_HEADER_XVgxBuiltinMinExecutor;
  __skip_spaces( data );
  int64_t n = 0;
  if( (data = decimal_to_integer( data, &n )) == NULL ) {
    return -1;
  }
  __skip_line( data );
  if( (pool_n = (int)n) < 0 ) {
    return -1;
  }
  else if( pool_n > DISPATCH_QUEUE_COUNT-1 ) {
    pool_n = 3;
  }
  return pool_n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_XVgxBacklog "x-vgx-backlog:"
#define sz_HEADER_XVgxBacklog (sizeof(HEADER_XVgxBacklog) - 1)
#define IS_HEADER_XVgxBacklog( Line ) __match_lower_prefix( Line, HEADER_XVgxBacklog )

/**************************************************************************//**
 * __parse_header__x_vgx_backlog
 *
 ******************************************************************************
 */
__inline static int16_t __parse_header__x_vgx_backlog( const char *data ) {
  data += sz_HEADER_XVgxBacklog;
  __skip_spaces( data );
  WORD bsz = 0;
  if( (data = hex_to_WORD( data, &bsz )) == NULL ) {
    return -1;
  }
  __skip_line( data );
  return (int16_t)bsz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define HEADER_XVgxBypassSOUT "x-vgx-bypass-sout:"
#define sz_HEADER_XVgxBypassSOUT (sizeof(HEADER_XVgxBypassSOUT) - 1)
#define IS_HEADER_XVgxBypassSOUT( Line ) __match_lower_prefix( Line, HEADER_XVgxBypassSOUT )

/**************************************************************************//**
 * __parse_header__x_vgx_bypass_sout
 *
 ******************************************************************************
 */
__inline static int __parse_header__x_vgx_bypass_sout( const char *data ) {
  data += sz_HEADER_XVgxBypassSOUT;
  __skip_spaces( data );
  int64_t value = 0;
  if( (data = decimal_to_integer( data, &value )) == NULL ) {
    return -1;
  }
  __skip_line( data );
  if( value < 0 ) {
    return -1;
  }

  return value > 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __parse_header__ignore( const char *data ) {
  __skip_line( data );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_HTTPRequestHeaderField vgx_server_parser__parse_request_header_line( vgx_VGXServer_t *server, vgx_VGXServerRequest_t *request, const char *line ) {
  // End of headers
  if( IS_HEADER_END( line ) ) {
    return HTTP_REQUEST_HEADER__END_OF_HEADERS;
  }

  // Select headers to match based on first character (lowercase)
  switch( line[0] | 0x20 ) {
  case 'x':
    // X-Vgx-Partial-Target:
    if( IS_HEADER_XVgxPartialTarget( line ) ) {
      request->target_partial = __parse_header__x_vgx_partial_target( line );
      return HTTP_REQUEST_HEADER_FIELD__XVgxPartialTarget;
    }
    
    // X-Vgx-Builtin-Min-Executor:
    if( IS_HEADER_XVgxBuiltinMinExecutor( line ) ) {
      // We ignore this header on the main server port
      if( server->config.cf_server->front->port.offset == 0 ) {
        goto ignore_header;
      }
      request->min_executor_pool = (int8_t)__parse_header__x_vgx_builtin_min_executor( server, line );
      return HTTP_REQUEST_HEADER_FIELD__XVgxBuiltinExecutor;
    }
    
    // X-Vgx-Bypass-SOUT:
    if( IS_HEADER_XVgxBypassSOUT( line ) ) {
      if( (request->headers->control.bypass_sout = (int8_t)__parse_header__x_vgx_bypass_sout( line )) < 0 ) {
        goto bad_header;
      }
      return HTTP_REQUEST_HEADER_FIELD__XVgxBypassSOUT;
    }
    goto ignore_header;

  case 'c':
    // Content-Type:
    if( IS_HEADER_ContentType( line ) ) {
      request->content_type = __parse_header__content_type( line );
      return HTTP_REQUEST_HEADER_FIELD__ContentType;
    }
    
    // Content-Length:
    if( IS_HEADER_ContentLength( line ) ) {
      if( (request->headers->content_length = __parse_header__content_length( line )) < 0 ) {
        goto bad_header;
      }
      return HTTP_REQUEST_HEADER_FIELD__ContentLength;
    }
    goto ignore_header;

  case 'a':
    // Accept:
    if( IS_HEADER_Accept( line ) ) {
      request->accept_type = __parse_header__accept( line );
      return HTTP_REQUEST_HEADER_FIELD__Accept;
    }
    goto ignore_header;

  }

ignore_header:
  // Other
  __parse_header__ignore( line );
  return HTTP_REQUEST_HEADER_IGNORED;

bad_header:
  return HTTP_REQUEST_HEADER_ERROR;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_HTTPResponseHeaderField vgx_server_parser__parse_response_header_line( const char *line, vgx_VGXServerResponse_t *response ) {
  // End of headers
  if( IS_HEADER_END( line ) ) {
    return HTTP_RESPONSE_HEADER__END_OF_HEADERS;
  }
  
  // Select headers to match based on first character (lowercase)
  switch( line[0] | 0x20 ) {
  case 'c':
    // Content-Type:
    if( IS_HEADER_ContentType( line ) ) {
      response->mediatype = __parse_header__content_type( line );
      return HTTP_RESPONSE_HEADER_FIELD__ContentType;
    }

    // Content-Length:
    if( IS_HEADER_ContentLength( line ) ) {
      response->content_length = __parse_header__content_length( line );
      return HTTP_RESPONSE_HEADER_FIELD__ContentLength;
    }

    goto ignore_header;

  case 'x':
    // X-Vgx-Backlog:
    if( IS_HEADER_XVgxBacklog( line ) ) {
      response->x_vgx_backlog = __parse_header__x_vgx_backlog( line );
      return HTTP_RESPONSE_HEADER_FIELD__XVgxBacklog;
    }

    goto ignore_header;
  }

ignore_header:
  __parse_header__ignore( line );
  return HTTP_RESPONSE_HEADER_IGNORED;
}
