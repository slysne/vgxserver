/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_response.c
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



static int64_t                      __write_response_status( vgx_VGXServerClient_t *client, HTTPStatus code );
static int64_t                      __write_response_port( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
static int64_t                      __write_response_executor( vgx_VGXServerClient_t *client );
static int64_t                      __write_response_matrix( vgx_VGXServerClient_t *client );
static int64_t                      __write_response_duration( vgx_VGXServerClient_t *client );

static int64_t                      __write_common_headers( vgx_VGXServerClient_t *client );
static int64_t                      __write_header__Allowed( vgx_VGXServerClient_t *client );
static int64_t                      __write_header__ContentType( vgx_VGXServerClient_t *client );
static int64_t                      __write_header__ContentLength( vgx_VGXServerClient_t *client, int64_t content_length );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_response__dump( const vgx_VGXServerResponse_t *response, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerResponse_t, response ) {
    CXLIB_OSTREAM( "status" );
    CXLIB_OSTREAM( "    code        = %d", response->status.code );
    CXLIB_OSTREAM( "exec_ns         = %lld", response->exec_ns );
    CXLIB_OSTREAM( "content_length  = %lld", response->content_length );
    CXLIB_OSTREAM( "buffers" );
    CXLIB_OSTREAM( "    stream      = %llp", response->buffers.stream );
    CXLIB_OSTREAM( "    content     = %llp", response->buffers.content );
    CXLIB_OSTREAM( "mediatype       = %04X", response->mediatype );
    CXLIB_OSTREAM( "x_vgx_backlog   = %d", (int)response->x_vgx_backlog );
    CXLIB_OSTREAM( "content_offset  = %lld", response->content_offset );
    CXLIB_OSTREAM( "info" );
    CXLIB_OSTREAM( "    http_errcode= %d", (int)response->info.http_errcode );
    CXLIB_OSTREAM( "    error" );
    CXLIB_OSTREAM( "        svc_exe = %d", (int)response->info.error.svc_exe );
    CXLIB_OSTREAM( "        mem_err = %d", (int)response->info.error.mem_err );
    CXLIB_OSTREAM( "    execution" );
    CXLIB_OSTREAM( "        system    = %d", (int)response->info.execution.system );
    CXLIB_OSTREAM( "        fileio    = %d", (int)response->info.execution.fileio );
    CXLIB_OSTREAM( "        plugin    = %d", (int)response->info.execution.plugin );
    CXLIB_OSTREAM( "        nometrics = %d", (int)response->info.execution.nometrics );
  } END_CXLIB_OBJ_DUMP;
  if( fatal_message ) {
    FATAL( 0xEEE, "%s", fatal_message );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_response_status( vgx_VGXServerClient_t *client, HTTPStatus code ) {
  static __THREAD char STATUS[64] = "HTTP/1.1 ";
  char *p = STATUS + 9;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  // Code, Reason
  client->response.status.code = code;
  client->response.status.reason = __get_http_response_reason( code );
  // Write
  p = write_decimal( p, client->response.status.code );
  p = write_chars( p, " " );
  p = write_chars( p, client->response.status.reason );
  p = write_chars( p, CRLF );
  write_term( p );

  int64_t sz = p - STATUS;
  return iStreamBuffer.Write( outstream, STATUS, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_response_port( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  static __THREAD char PORT[32] = ", \"port\": [";
  if( client->response.mediatype != MEDIA_TYPE__application_json ) {
    return 0;
  }
  char *p = PORT + 11;
  p = write_decimal( p, __server_port( server ) );
  p = write_chars( p, ", " );
  p = write_decimal( p, __server_port_offset( server ) );
  p = write_chars( p, "]" );
  write_term( p );
  int64_t sz = p - PORT;
  return iStreamBuffer.Write( client->response.buffers.content, PORT, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_response_executor( vgx_VGXServerClient_t *client ) {
  static __THREAD char EXECUTOR[32] = ", \"exec_id\": ";
  if( client->response.mediatype != MEDIA_TYPE__application_json ) {
    return 0;
  }
  char *p = EXECUTOR + 13;
  p = write_decimal( p, client->request.executor_id );
  write_term( p );
  int64_t sz = p - EXECUTOR;
  return iStreamBuffer.Write( client->response.buffers.content, EXECUTOR, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_response_matrix( vgx_VGXServerClient_t *client ) {
  static __THREAD char LEVEL[16] = ", \"level\": ";
  static __THREAD char PARTITIONS[32] = ", \"partitions\": [";
  static __THREAD char NOMATRIX[] = ", \"level\": 0, \"partitions\": null";

  char *p;
  int64_t sz;

  if( client->response.mediatype != MEDIA_TYPE__application_json ) {
    return 0;
  }

  if( client->response.info.execution.dispatch ) {
    // level
    p = LEVEL + 11;
    p = write_decimal( p, client->dispatch_metas.level.number );
    write_term( p );
    sz = p - LEVEL;
    iStreamBuffer.Write( client->response.buffers.content, LEVEL, sz );
    // partitions
    p = PARTITIONS + 17;
    p = write_decimal( p, client->dispatch_metas.level.parts );
    *p++ = ',';
    *p++ = ' ';
    p = write_decimal( p, client->dispatch_metas.level.incomplete_parts );
    *p++ = ',';
    *p++ = ' ';
    p = write_decimal( p, client->dispatch_metas.level.deep_parts );
    *p++ = ']';
    write_term( p );
    sz = p - PARTITIONS;
    return iStreamBuffer.Write( client->response.buffers.content, PARTITIONS, sz );
  }
  else {
    return iStreamBuffer.Write( client->response.buffers.content, NOMATRIX, sizeof(NOMATRIX)-1 );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_response_duration( vgx_VGXServerClient_t *client ) {
  static __THREAD char EXEC_MS[64] = ", \"exec_ms\": ";
  if( client->response.mediatype != MEDIA_TYPE__application_json ) {
    return 0;
  }
  char *p = EXEC_MS + 13;
  int64_t t1_ns = __GET_CURRENT_NANOSECOND_TICK();
  int64_t acc_ns = t1_ns - client->request.exec_t0_ns;
  client->response.exec_ns += acc_ns;
  double exec_ms = client->response.exec_ns / 1000000.0;
  p = write_double( p, exec_ms, 3 );
  write_term( p );
  int64_t sz = p - EXEC_MS;
  return iStreamBuffer.Write( client->response.buffers.content, EXEC_MS, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_common_headers( vgx_VGXServerClient_t *client ) {
  // Common Headers
  static const char HEADERS_Common[] = 
    "Server: " VGX_SERVER_HEADER CRLF
    "Connection: keep-Alive" CRLF
    "Access-Control-Allow-Origin: *" CRLF;
  static int64_t sz_HEADERS_Common = sizeof( HEADERS_Common )-1;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  return iStreamBuffer.Write( outstream, HEADERS_Common, sz_HEADERS_Common );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_header_placeholder__XVgxBacklog( vgx_VGXServerClient_t *client ) {
  static char X_VGX_BACKLOG[] = "X-Vgx-Backlog: 0000" CRLF; // <-- will be populated by main server thread!
  static size_t sz_X_VGX_BACKLOG = sizeof( X_VGX_BACKLOG ) - 1;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  return iStreamBuffer.Write( outstream, X_VGX_BACKLOG, sz_X_VGX_BACKLOG ) - 6; // <-- offset of the 0000 placeholder
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_header__Allowed( vgx_VGXServerClient_t *client ) {
  static __THREAD char ALLOWED[128] = "Allowed: OPTIONS";
  char *p = ALLOWED + 16;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  // Allowed:
  int16_t allowed = client->response.allowed_methods_mask;
  HTTPRequestMethod m = __HTTP_MIN_MASK;
  while( m <= __HTTP_MAX_MASK ) {
    if( ((int16_t)m & allowed) != 0 ) {
      const char * method = __vgx_http_request_method( m );
      p = write_chars( p, ", " );
      p = write_chars( p, method );
    }
    m <<= 1;
  }
  p = write_chars( p, CRLF );
  write_term( p );
  int64_t sz = p - ALLOWED;
  return iStreamBuffer.Write( outstream, ALLOWED, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_header__ContentType( vgx_VGXServerClient_t *client ) {
  static __THREAD char CONTENT_TYPE[128] = "Content-Type: ";
  char *p = CONTENT_TYPE + 14;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  // Content-Type:
  const char *content_type = __get_http_mediatype( client->response.mediatype );
  p = write_chars( p, content_type );
  p = write_chars( p, CRLF );
  write_term( p );
  int64_t sz = p - CONTENT_TYPE;
  return iStreamBuffer.Write( outstream, CONTENT_TYPE, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __write_header__ContentLength( vgx_VGXServerClient_t *client, int64_t content_length ) {
  static __THREAD char CONTENT_LENGTH[32] = "Content-Length: ";
  char *p = CONTENT_LENGTH + 16;
  vgx_StreamBuffer_t *outstream = client->response.buffers.stream;
  // Content-Length:
  p = write_decimal( p, content_length );
  p = write_chars( p, CRLF );
  write_term( p );
  int64_t sz = p - CONTENT_LENGTH;
  return iStreamBuffer.Write( outstream, CONTENT_LENGTH, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_response__prepare_body( vgx_VGXServerResponse_t *response ) {
  static const char BODY_OK_JSON_PreWrap[] = "{\"status\": \"OK\", \"response\": ";
  static int sz_BODY_OK_JSON_PreWrap = sizeof( BODY_OK_JSON_PreWrap ) - 1;

  switch( response->mediatype ) {
  case MEDIA_TYPE__application_json:
    if( !response->info.execution.prewrap ) {
      const char *data = BODY_OK_JSON_PreWrap;
      int sz = sz_BODY_OK_JSON_PreWrap;
      if( iStreamBuffer.Write( response->buffers.content, data, sz ) < 0 ) {
        return -1;
      }
      response->info.execution.prewrap = true;
    }
    return 0;
  default:
    return 0;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_response__complete_body( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  static const char BODY_PostWrap[] = "}";
  static int sz_BODY_PostWrap = sizeof( BODY_PostWrap ) - 1;

  if( !client->response.info.execution.nometas ) {
    // Port information
    if( __write_response_port( server, client ) < 0 ) {
      return -1;
    }

    // Executor information
    if( __write_response_executor( client ) < 0 ) {
      return -1; 
    }
  }

  // Level and partition information
  if( __write_response_matrix( client ) < 0 ) {
    return -1; 
  }

  // Request duration information
  if( __write_response_duration( client ) < 0 ) {
    return -1;
  }

  if( client->response.mediatype == MEDIA_TYPE__application_json ) {
    if( iStreamBuffer.Write( client->response.buffers.content, BODY_PostWrap, sz_BODY_PostWrap ) < 0 ) {
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_response__prepare_body_error( vgx_VGXServerResponse_t *response, CString_t *CSTR__error ) {
  static const char BODY_ERROR_PreWrap[] = "{\"status\": \"ERROR\", \"message\": ";
  static int sz_BODY_ERROR_PreWrap = sizeof( BODY_ERROR_PreWrap ) - 1;
  // Discard any previously prepared body
  iStreamBuffer.Clear( response->buffers.content );
  // Write error wrapper with message
  static const char *dflt = "unknown error";
  const char *serr = CSTR__error ? CStringValue( CSTR__error ) : dflt;
  int64_t sz_serr = CSTR__error ? CStringLength( CSTR__error ) : strlen( dflt );
  int64_t msz = sz_serr + 64; // extra for wrapper
  char *errbuf = malloc( msz + 1 );
  if( errbuf == NULL ) {
    return -1;
  }
  int bsz = 0;

  if( response->mediatype == MEDIA_TYPE__application_json ) {
    char first = *serr;
    char last = *(serr+sz_serr-1);
    const char *prequote = "";
    const char *postquote = "";

    if( !(first == '{' && last == '}') &&
        !(first == '[' && last == ']') )
    {
      if( first != '"' ) {
        prequote = "\"";
      }
      if( last != '"' ) {
        postquote = "\"";
      }
    }
    bsz = snprintf( errbuf, msz, "{\"status\": \"ERROR\", \"message\": %s%s%s", prequote, serr, postquote ); // NOTE: <- json not terminated yet
  }
  else if( response->mediatype == MEDIA_TYPE__text_plain ) {
    bsz = snprintf( errbuf, msz, "ERROR: %s", serr );
  }

  int ret = 0;
  if( iStreamBuffer.Write( response->buffers.content, errbuf, bsz ) < 0 ) {
    ret = -1;
  }

  free( errbuf );
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_response__produce( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, HTTPStatus code ) {

  // OK Response
  if( code == HTTP_STATUS__OK ) {
    if( client->response.mediatype == MEDIA_TYPE__application_json ) {
      // Handle empty response payload
      if( iStreamBuffer.Empty( client->response.buffers.content ) ) {
        if( vgx_server_response__prepare_body( &client->response ) < 0 ) {
          return -1;
        }
      }
      // Complete body
      if( vgx_server_response__complete_body( server, client ) < 0 ) {
        return -1;
      }
    }
  }
  // Non 200 OK is recorded in response flags
  else {
    client->response.info.http_errcode = code;
  }


  // ------------------------------------------------------------------
  // STATUS and HEADERS
  // ------------------------------------------------------------------
  // HTTP Response Status 
  // e.g. "HTTP/1.1 200 OK\r\n"
  int64_t sz_status;
  if( (sz_status = __write_response_status( client, code )) < 0 ) {
    return -1;
  }

  int64_t offset_backlog;
  if( (offset_backlog = __write_header_placeholder__XVgxBacklog( client )) < 0 ) {
    return -1;
  }

  // Content-Length
  int64_t content_length = iStreamBuffer.Size( client->response.buffers.content );

  switch( client->request.method ) {
  case HTTP_HEAD:
    // No body for HEAD request
    iStreamBuffer.Clear( client->response.buffers.content );
    break;
  case HTTP_OPTIONS:
    do {
      // HTTP Header Allowed
      // e.g. "Allowed: OPTIONS, GET, POST, HEAD"
      if( __write_header__Allowed( client ) < 0 ) {
        return -1;
      }
    } WHILE_ZERO;
    break;
  default:
    break;
  }

  // HTTP Common Headers
  if( __write_common_headers( client ) < 0 ) {
    return -1;
  }

  if( content_length > 0 ) {
    // HTTP Header Content-Type
    // e.g. "Content-Type: application/json; charset=UTF-8"
    if( __write_header__ContentType( client ) < 0 ) {
      return -1;
    }

    // HTTP Header Content-Length
    // e.g. "Content-Length: 12345"
    if( __write_header__ContentLength( client, content_length ) < 0 ) {
      return -1;
    }
  }

  // Empty Line
  if( iStreamBuffer.Write( client->response.buffers.stream, CRLF, sz_CRLF ) < 0 ) {
    return -1;
  }
  // ------------------------------------------------------------------

  // Record pointer to WORD field which can be overwritten with backlog counter
  char *data;
  iStreamBuffer.ReadableSegment( client->response.buffers.stream, LLONG_MAX, (const char**)&data, NULL );
  client->p_x_vgx_backlog_word = data + (sz_status + offset_backlog);

  return 0; 

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_response__produce_error( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, HTTPStatus code, const char *message, bool errlog ) {

  CLIENT_STATE__SET_ERROR( client );
  vgx_VGXServerResponse_t *response = &client->response;

  // Message overrides anything previously written to response body
  if( message != NULL ) {

    if( errlog ) {
      REASON( 0x000, "%d %s", (int)code, message );
    }

    // Get rid of anything in the response body that possibly caused the error
    iStreamBuffer.Clear( response->buffers.content );

    // Wrap string in quotes if json response
    const char *quote = "\"";
    int sz_quote = response->mediatype == MEDIA_TYPE__application_json ? 1 : 0;

    // Open quote
    iStreamBuffer.Write( response->buffers.content, quote, sz_quote );

    // Render http code as string
    char scode[8] = {0};
    int sz_scode = snprintf( scode, 7, "%03d ", code );

    // Write message to response body
    iStreamBuffer.Write( response->buffers.content, scode, sz_scode );
    int64_t sz = strnlen( message, 4094 );
    iStreamBuffer.Write( response->buffers.content, message, sz );

    // End quote
    iStreamBuffer.Write( response->buffers.content, quote, sz_quote );
  }
  
  vgx_server_response__produce( server, client, code );

  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int vgx_server_response__init( vgx_VGXServerResponse_t *response, const char *label ) {

  XTRY {
    memset( response, 0, sizeof( vgx_VGXServerResponse_t ) );

    // [Q1.1.1]
    response->status.code = HTTP_STATUS__NONE;

    // [Q1.1.2]
    response->status.__rsv = 0;

    // [Q1.2]
    response->status.reason = NULL;

    // [Q1.3]
    response->exec_ns = 0;

    // [Q1.4]
    response->content_length = 0;

    // [Q1.5]
    if( (response->buffers.stream = iStreamBuffer.New( SEND_CHUNK_ORDER )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
#ifdef _DEBUG
    char buffername[512] = {0};
    snprintf( buffername, 511, "%s/stream", label );
    iStreamBuffer.SetName( response->buffers.stream, buffername );
#endif

    // [Q1.6]
    if( (response->buffers.content = iStreamBuffer.New( SEND_CHUNK_ORDER )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
#ifdef _DEBUG
    snprintf( buffername, 511, "%s/content", label );
    iStreamBuffer.SetName( response->buffers.content, buffername );
#endif

    // [Q1.7.1]
    response->mediatype = MEDIA_TYPE__text_plain;

    // [Q1.7.2.1]
    response->x_vgx_backlog = 0;

    // [Q1.7.2.2]
    response->allowed_methods_mask = 0;

    // [Q1.8.1]
    response->content_offset = 0;

    // [Q1.8.2]
    response->info._bits = 0;
  }
  XCATCH( errcode ) {
    vgx_server_response__destroy( response );
  }
  XFINALLY {
  }

  return 0;
}


  
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_response__destroy( vgx_VGXServerResponse_t *response ) {
  iStreamBuffer.Delete( &response->buffers.stream );
  iStreamBuffer.Delete( &response->buffers.content );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerResponse_t * vgx_server_response__new( const char *label ) {
  vgx_VGXServerResponse_t *response = calloc( 1, sizeof(vgx_VGXServerResponse_t) );
  if( response ) {
    if( vgx_server_response__init( response, label ) < 0 ) {
      free( response );
      response = NULL;
    }
  }
  return response;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_response__delete( vgx_VGXServerResponse_t **response ) {
  if( response && *response ) {
    vgx_server_response__destroy( *response );
    free( *response );
    *response = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_response__reset( vgx_VGXServerResponse_t *response ) {
  response->status.code = HTTP_STATUS__NONE;
  response->status.reason = NULL;
  response->exec_ns = 0;
  response->content_length = 0;
  if( response->buffers.stream ) {
    iStreamBuffer.Clear( response->buffers.stream );
  }
  if( response->buffers.content ) {
    iStreamBuffer.Clear( response->buffers.content );
  }
  response->mediatype = MEDIA_TYPE__NONE;
  response->x_vgx_backlog = -1;
  response->content_offset = 0;
  response->info._bits = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_response__client_response_reset( vgx_VGXServerClient_t *client ) {
  if( client ) {
    vgx_server_response__reset( &client->response );
    memset( &client->dispatch_metas, 0, sizeof(client->dispatch_metas) );
  }
}
