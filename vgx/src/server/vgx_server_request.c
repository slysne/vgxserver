/*######################################################################
 *#
 *# vgx_server_request.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static bool                         __allow_service_out_request( const vgx_VGXServer_t *server, const vgx_VGXServerRequest_t *request );
static vgx_HTTPHeaders_t *          __new_headers_object( void );
static void                         __delete_headers_object( vgx_HTTPHeaders_t **headers );
static int                          __add_raw_request_header( vgx_VGXServerRequest_t *request, const char *raw_header, int64_t sz_raw_header );
static void                         __clear_request_headers( vgx_VGXServerRequest_t *request );
static void                         __reset_request( vgx_VGXServerRequest_t *request );
static void                         __client_ready( vgx_VGXServerClient_t *client );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__dump( const vgx_VGXServerRequest_t *request, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerRequest_t, request ) {
    CXLIB_OSTREAM( "state        : %08x", request->state  );
    CXLIB_OSTREAM( "method       : %04x", request->method  );
    CXLIB_OSTREAM( "exec_t0_ns   : %lld", request->exec_t0_ns );
    CXLIB_OSTREAM( "version      : %d.%d", (int)request->version.major, (int)request->version.minor );
    CXLIB_OSTREAM( "executor_id  : %d", (int)request->executor_id );
    char path[64] = {0};
    if( request->path ) {
      strncpy( path, request->path, 63 );
    }
    CXLIB_OSTREAM( "path         : %s", path );
    CXLIB_OSTREAM( "buffers" );
    CXLIB_OSTREAM( "  .stream" );
    iStreamBuffer.Dump( request->buffers.stream );
    CXLIB_OSTREAM( "  .content" );
    iStreamBuffer.Dump( request->buffers.content );

    vgx_HTTPHeaders_t *headers = request->headers;
    CXLIB_OSTREAM( "  .headers @%llp", headers );
    if( headers ) {
      CXLIB_OSTREAM( "    .sz             : %d", headers->sz );
      CXLIB_OSTREAM( "    .content_offset : %d", headers->content_offset );
      CXLIB_OSTREAM( "    .content_length : %lld", headers->content_length );
    }
    CXLIB_OSTREAM( "  .content_type : %04x", request->content_type );
    CXLIB_OSTREAM( "  .accept_type  : %04x", request->accept_type );

  } END_CXLIB_OBJ_DUMP;
  if( fatal_message ) {
    FATAL( 0xEEE, "%s", fatal_message );
  }
}



/*******************************************************************//**
 * @brief 
 * 
 * @param server 
 * @param request 
 * @return true 
 * @return false 
 ***********************************************************************
*/
static bool __allow_service_out_request( const vgx_VGXServer_t *server, const vgx_VGXServerRequest_t *request ) {

  bool HC = CharsStartsWithConst( request->path, "/vgx/hc" );

  // Always allow non-main (admin) port except for HC
  if( server->config.cf_server->front->port.offset > 0 && !HC ) {
    return true;
  }

  // Allow requests where executor id has already been assigned or explicitly bypass s-out
  if( request->executor_id >= 0 || request->headers->control.bypass_sout > 0 ) {
    return true;
  }

  // Reject 
  if( HC ) {
    return false;
  }

  // Reject plugin requests
  if( CharsStartsWithConst( request->path, "/vgx/plugin/" ) ) {
    return false;
  }

  // Reject builtin requests
  if( CharsStartsWithConst( request->path, "/vgx/builtin/" ) ) {
    return false;
  }

  // Allow everything else
  return true;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
*/
static int __set_client_flags( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  // Matrix is enabled
  if( DISPATCHER_MATRIX_ENABLED( server ) ) {
    vgx_server_pathspec_t pathspec;
    vgx_server_resource__init_pathspec( &pathspec, server, &client->request );

    // Not a plugin request
    if( !(pathspec.type & __VGX_SERVER_PLUGIN_TYPE__PLUG_MASK) ) {
      return 0;
    }

    const char *plugin_name = vgx_server_resource__plugin_name( &pathspec );

    // Plugin phase mask
    client->flags.plugin = vgx_server_resource__get_plugin_phases( plugin_name );

    // A plugin request in matrix context for which we have no preprocessor is a direct request
    client->flags.direct = !CLIENT_HAS_REQUEST_PROCESSOR( client );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
*/
static int __write_direct( vgx_VGXServerRequest_t *request, const char *line, int64_t sz_line ) {
  // Use CONTENT buffer to hold entire direct request
  return iStreamBuffer.Write( request->buffers.content, line, sz_line ) == sz_line;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
*/
static int __add_header__x_vgx_partial( vgx_VGXServerRequest_t *request, int (*writef)(vgx_VGXServerRequest_t*, const char*, int64_t) ) {
  static const char HEADER_Accept_x_vgx_partial[] = "Accept: application/x-vgx-partial" CRLF;
  static int64_t sz_HEADER_Accept_x_vgx_partial = sizeof( HEADER_Accept_x_vgx_partial ) - 1;
  return writef( request, HEADER_Accept_x_vgx_partial, sz_HEADER_Accept_x_vgx_partial );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_MediaType __get_response_mediatype( vgx_VGXServerRequest_t *request ) {
  // Set response media type
  switch( request->accept_type ) {
  case MEDIA_TYPE__application_x_vgx_partial:
  case MEDIA_TYPE__application_json:
  case MEDIA_TYPE__text_plain:
    return request->accept_type;
  default:
    return MEDIA_TYPE__application_json;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
*/
DLL_HIDDEN int vgx_server_request__handle( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
TRAP_ILLEGAL_CLIENT_ACCESS( server, client );

  vgx_VGXServerRequest_t *request = &client->request;
  vgx_StreamBuffer_t *instream = client->request.buffers.stream;
  
  // We're a dispatcher with multiple partials or we have a processor of any kind
  bool multipart;

  int sz_line;
  vgx_HTTPRequestHeaderField field;
  int64_t unfilled;
  int64_t transferred;

  switch( CLIENT_STATE_NOERROR( client ) ) {
  case VGXSERVER_CLIENT_STATE__READY:
    // Start the clock
    client->io_t0_ns = __GET_CURRENT_NANOSECOND_TICK();
    client->tbase_ns = server->tbase_ns;
    client->request.exec_t0_ns = client->io_t0_ns;
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__EXPECT_INITIAL );

  // ------------------------------
  // Request line
  // e.g. "GET /some/path HTTP/1.1"
  // ------------------------------
  case VGXSERVER_CLIENT_STATE__EXPECT_INITIAL:
    // Read a complete line into buffer
    if( (sz_line = (int)iStreamBuffer.ReadUntil( instream, HTTP_LINE_MAX, &server->io.buffer, '\n' )) <= 0 ) {
      goto incomplete_line;
    }

    // Parse request line e.g. "GET /some/path HTTP/1.1" or "XVGXIDENT ..."
    if( vgx_server_parser__parse_request_initial_line( request, server->io.buffer ) < 0 ) {
      goto bad_request_line;
    }

    // Check HTTP mversion
    if( client->request.version.major != 1 ) {
      goto unsupported_http_version;
    }

    // We just got ident data, back to ready and yield
    if( request->method == XVGX_IDENT ) {
      CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__READY );
      goto yield;
    }

    // Set client flags
    if( __set_client_flags( server, client ) < 0 ) {
      goto bad_request_line;
    }
    
    multipart = DISPATCHER_MATRIX_MULTI_PARTIAL( &server->matrix ) || CLIENT_HAS_ANY_PROCESSOR( client );

    // Add "Accept: application/x-vgx-partial\r\n" to request headers
    if( multipart ) {
      if( !__add_header__x_vgx_partial( request, __add_raw_request_header ) ) {
        goto memory_error;
      }
    }

    // Will request go directly to matrix?
    if( CLIENT_IS_DIRECT( client ) ) {
      // Initial line into CONTENT
      if( !__write_direct( request, server->io.buffer, sz_line ) ) {
        goto memory_error;
      }
      // Add "Accept: application/x-vgx-partial\r\n" to CONTENT stream
      if( multipart ) {
        if( !__add_header__x_vgx_partial( request, __write_direct ) ) {
          goto memory_error;
        }
      }
    }

    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__EXPECT_HEADERS );

  // ------------------------------
  // Header line(s)
  // e.g. "Content-Length: 1234"
  // ------------------------------
  case VGXSERVER_CLIENT_STATE__EXPECT_HEADERS:
    multipart = DISPATCHER_MATRIX_MULTI_PARTIAL( &server->matrix ) || CLIENT_HAS_ANY_PROCESSOR( client );

    do {
      // Header line or empty line
      if( (sz_line = (int)iStreamBuffer.ReadUntil( instream, HTTP_LINE_MAX, &server->io.buffer, '\n' )) <= 0 ) {
        goto incomplete_line;
      }

      // Parse header line e.g. "Content-Length: 123"
      field = vgx_server_parser__parse_request_header_line( server, request, server->io.buffer );

      if( field == HTTP_REQUEST_HEADER_ERROR ) {
        goto bad_header;
      }

      // Request's Accept type controls the response mediatype
      if( field == HTTP_REQUEST_HEADER_FIELD__Accept ) {
        // Capture the client's requested Accept type so we know how to respond later
        client->response.mediatype = __get_response_mediatype( request );
        // Do not add this header field if we're a multi-partial dispatcher because it
        // was already automatically written above
        if( multipart ) {
          goto direct_header;
        }
      }

      // Add raw header data to request
      if( field != HTTP_REQUEST_HEADER__END_OF_HEADERS ) {
        __add_raw_request_header( request, server->io.buffer, sz_line );
      }

    direct_header:
      // Write header to content buffer when mode is direct to matrix
      if( CLIENT_IS_DIRECT( client ) ) {
        // Include header in direct request unless this is the Accept header and matrix is multi-partial
        if( !(field == HTTP_REQUEST_HEADER_FIELD__Accept && multipart) ) {
          if( !__write_direct( request, server->io.buffer, sz_line ) ) {
            goto memory_error;
          }
        }
      }
    } while( field != HTTP_REQUEST_HEADER__END_OF_HEADERS );

    // Multi-partial request, force request accept type (it's ok, we captured the response mediatype earlier)
    if( multipart ) {
      request->accept_type = MEDIA_TYPE__application_x_vgx_partial;
    }

    // Empty \r\n line reached
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__EXPECT_CONTENT );

    // Ensure service in
    if( !server->control.serving_public && !__allow_service_out_request( server, request ) ) {
      goto service_out;
    }
    else if( !VGXSERVER_GLOBAL_IS_SERVING( server ) ) {
      goto service_out;
    }

    // Record the start of payload in CONTENT buffer
    request->headers->content_offset = (int)iStreamBuffer.Size( request->buffers.content );

    // If no request content go straight to dispatch
    if( (unfilled = __vgxserver_request_unfilled_content_buffer( request )) == 0 ) {
      goto dispatch;
    }

    // No data in stream buffer, we need more socket reads to fill content buffer
    if( iStreamBuffer.Empty( instream ) ) {
      goto incomplete_content;
    }

    // Transfer from stream buffer into content buffer
    if( (transferred = iStreamBuffer.Absorb( request->buffers.content, instream, unfilled )) < 0 ) {
      goto memory_error;
    }

    // Not enough data in stream buffer, we need more socket reads to fill content buffer
    if( transferred < unfilled ) {
      goto incomplete_content;
    }

    // We have all the content already
    goto dispatch;

  // ------------------------------
  // CONTENT
  // ------------------------------
  case VGXSERVER_CLIENT_STATE__EXPECT_CONTENT:
    if( __vgxserver_request_unfilled_content_buffer( request ) > 0 ) {
      goto incomplete_content;
    }
    goto dispatch;

  // ------------------------------
  // State machine error
  // ------------------------------
  default:
    goto state_machine_error;
  }


dispatch:
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__HANDLE_REQUEST );
  CLIENT_BLOCK( server, client );
  return vgx_server_dispatch__dispatch( server, client );

yield:
  CLIENT_YIELD( server, client );
  return 0;

  /**********************************
  * EXCEPTIONS
  ***********************************/

incomplete_line:
  // Protect against overlong URI
  if( iStreamBuffer.Size( instream ) >= HTTP_LINE_MAX ) {
    HTTPStatus too_long = HTTP_STATUS__URITooLong;
    vgx_server_response__produce_error( server, client, too_long, __get_http_response_reason( too_long ), false );
    goto error;
  }

incomplete_content:
  CLIENT_BLOCK( server, client );
  return 0;

service_out:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "Service out", false );
  goto error;

bad_request_line:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__BadRequest, "Invalid HTTP request line", false );
  goto error;

unsupported_http_version:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__HTTPVersionNotSupported, "Unsupported HTTP version ", false );
  goto error;

bad_header:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__BadRequest, "Invalid HTTP header", false );
  goto error;

memory_error:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Out of memory while building request content", true );
  goto error;

state_machine_error:
  vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Internal state error", true );
  goto error;

error:
  CLIENT_BLOCK( server, client );
  return -1;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_headers_object( vgx_HTTPHeaders_t **headers ) {
  if( headers && *headers ) {
    vgx_HTTPHeaders_t *h = *headers;
    if( h->list ) {
      free( h->list );
    }
    if( h->_buffer ) {
      free( h->_buffer );
    }
    DESTROY_HEADERS_CAPSULE( &h->capsule );
    free( h );
    *headers = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_HTTPHeaders_t * __new_headers_object( void ) {
  vgx_HTTPHeaders_t *headers = NULL;
  XTRY {
    // Headers object
    if( (headers = calloc( 1, sizeof( vgx_HTTPHeaders_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Buffer
    int64_t szbuf = (HTTP_LINE_MAX+1) * (HTTP_MAX_HEADERS+1);  // over-allocate by one line and add room for nulterm per line
    if( (headers->_buffer = calloc( 1, szbuf )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    headers->_end = headers->_buffer + szbuf;
    headers->_wp = headers->_buffer;  

    // List of pointers (plus space for terminator)
    if( (headers->list = calloc( HTTP_MAX_HEADERS+1, sizeof( char* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }
    headers->_cursor = headers->list;

    // Number of headers
    headers->sz = 0;

    // Payload offset in content buffer
    headers->content_offset = 0;

    // Content-length header
    headers->content_length = 0;

    // No parent client
    headers->client = NULL;

    // Request signature
    idunset( &headers->signature );

    // Serial number will be assigned per request
    headers->sn = 0;

    // Clear flag
    headers->flag.__bits = 0;

    // Capsule
    headers->capsule.data = NULL;
    headers->capsule.destroyf = NULL;

    headers->control.bypass_sout = 0;
    headers->control.resubmit = false;
    headers->control._rsv_2_7_1_3 = 0;
    headers->control._rsv_2_7_1_4 = 0;

    headers->nresubmit = 0;

    headers->__rsv_2_8 = 0;
  }
  XCATCH( errcode ) {
    __delete_headers_object( &headers );
  }
  XFINALLY {
  }
  return headers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __add_raw_request_header( vgx_VGXServerRequest_t *request, const char *raw_header, int64_t sz_raw_header ) {
  vgx_HTTPHeaders_t *headers = request->headers;
  if( headers->sz < HTTP_MAX_HEADERS && (headers->_wp + sz_raw_header + 1) < headers->_end ) {
    // Add reference to location in buffer where raw header will be stored
    *headers->_cursor++ = headers->_wp;
    headers->sz++;

    // Copy raw header into buffer, advance write pointer 
    memcpy( headers->_wp, raw_header, sz_raw_header );
    headers->_wp += sz_raw_header;

    // Set the *next* list entry to the *next* location in the buffer
    // that we may or may not write to. We do this so a future reader
    // of the list can easily determine the size of a substring
    // by subtracting the substring's point from the *next* substring's 
    // pointer even if the *next* substring is empty.
    *headers->_wp = '\0';
    *headers->_cursor = headers->_wp;

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
static void __clear_request_headers( vgx_VGXServerRequest_t *request ) {
  vgx_HTTPHeaders_t *headers = request->headers;
  headers->_cursor = headers->list;
  *headers->_cursor = NULL;
  headers->_wp = headers->_buffer;
  headers->sz = 0;
  headers->content_offset = 0;
  headers->content_length = 0;
  headers->client = NULL;
  idunset( &headers->signature );
  headers->flag.__bits = 0;
  headers->control.bypass_sout = 0;
  headers->control.resubmit = false;
  headers->nresubmit = 0;
  DESTROY_HEADERS_CAPSULE( &headers->capsule );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __copy_headers( vgx_HTTPHeaders_t *dest, const vgx_HTTPHeaders_t *src ) {
  // Copy the block of raw data
  int64_t sz_buf = *src->_cursor - src->_buffer;
  memcpy( dest->_buffer, src->_buffer, sz_buf );
  dest->_wp = dest->_buffer + sz_buf;
  *dest->_wp = '\0';

  // Populate entry pointers (including the terminator)
  dest->sz = src->sz;
  dest->_cursor = dest->list;
  for( int i = 0; i<src->sz; ++i ) {
    int64_t offset = src->list[i] - src->_buffer;
    *dest->_cursor++ = dest->_buffer + offset;
  }
  // Term
  *dest->_cursor = dest->_wp;

  // Counts
  dest->content_length = src->content_length;
  dest->content_offset = src->content_offset;

  // Parent
  dest->client = src->client;

  // Request signature
  idcpy( &dest->signature, &src->signature );

  // Request serial number
  dest->sn = src->sn;

  // Request user flags
  dest->flag.__bits = src->flag.__bits;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int vgx_server_request__init( vgx_VGXServerRequest_t *request, const char *label ) {
  int ret = 0;

  // [Q1.1.1] 
  request->state = VGXSERVER_CLIENT_STATE__RESET;

  // [Q1.1.2]
  request->method = HTTP_NONE;

  // [Q1.2]
  request->exec_t0_ns = 0;

  // [Q1.3.1]
  request->version.major = 0;
  request->version.minor = 0;

  // [Q1.3.2.
  request->sz_path = 0;

  // [Q1.3.3.1]
  request->executor_id = -1;

  // [Q1.3.3.2]
  request->min_executor_pool = 0;

  // [Q1.3.3.3]
  request->replica_affinity = -1;

  // [Q1.3.3.4]
  request->target_partial = -1; // all


  XTRY {
    request->headers = NULL;

    // [Q1.4]
    // Pre-allocate max path
    if( (request->path = calloc( HTTP_PATH_MAX, sizeof(char) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // [Q1.5]
    if( (request->buffers.stream = iStreamBuffer.New( 10 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }
#ifdef _DEBUG
    char buffername[512] = {0};
    snprintf( buffername, 511, "%s/stream", label );
    iStreamBuffer.SetName( request->buffers.stream, buffername );
#endif

    // [Q1.6]
    if( (request->buffers.content = iStreamBuffer.New( RECV_CHUNK_ORDER )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }
#ifdef _DEBUG
    snprintf( buffername, 511, "%s/content", label );
    iStreamBuffer.SetName( request->buffers.content, buffername );
#endif

    // [Q1.7]
    if( (request->headers = __new_headers_object()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
  }
  XCATCH( errcode ) {
    free( request->path );
    free( request->headers );
  }
  XFINALLY {
  }

  // [Q1.8.1] 
  request->content_type = MEDIA_TYPE__text_plain;

  // [Q1.8.2]
  request->accept_type = MEDIA_TYPE__ANY;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __reset_request( vgx_VGXServerRequest_t *request ) {
  request->state = VGXSERVER_CLIENT_STATE__RESET;
  request->method = HTTP_NONE;
  request->exec_t0_ns = 0;
  request->version.bits = 0;
  request->sz_path = 0;
  request->executor_id = -1;
  request->min_executor_pool = 0;
  request->replica_affinity = -1;
  request->target_partial = -1;
  if( request->path ) {
    request->path[0] = '\0';
  }
  __clear_request_headers( request );
  if( request->buffers.stream ) {
    iStreamBuffer.Clear( request->buffers.stream );
  }
  if( request->buffers.content ) {
    iStreamBuffer.Clear( request->buffers.content );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __client_ready( vgx_VGXServerClient_t *client ) {
  vgx_VGXServerRequest_t *request = &client->request;
  __reset_request( request );
  // Set parent
  request->headers->client = client;
  // Reset flags
  client->flags._bits = 0;
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__READY );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__client_request_ready( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  if( client ) {
    // Reset all fields and buffers, and enter the ready state
    __client_ready( client );

    // Assign next serial number
    client->request.headers->sn = ++server->req_sn;

    // Clear user flag
    client->request.headers->flag.__bits = 0;

    // Prepare a new set of dispatcher streams to use with client for this request (will never fail)
    if( DISPATCHER_MATRIX_ENABLED( server ) ) {
      // Get a new set of dispatcher streams
      client->dispatcher.streams = vgx_server_dispatcher_matrix__new_stream_set( &server->matrix );
      // Reset the dispatcher request
      __reset_request( vgx_server_dispatcher_streams__get_request( client->dispatcher.streams ) );
    }
  }
}



/*******************************************************************//**
 * Copy src request into dest request, EXCEPT buffer contents
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__copy_clean( vgx_VGXServerRequest_t *dest, const vgx_VGXServerRequest_t *src ) {
  dest->state = src->state;
  dest->method = src->method;
  dest->exec_t0_ns = src->exec_t0_ns;
  dest->version = src->version;
  dest->sz_path = src->sz_path;
  dest->executor_id = src->executor_id;
  dest->min_executor_pool = src->min_executor_pool;
  dest->replica_affinity = src->replica_affinity;
  dest->target_partial = src->target_partial;
  memcpy( dest->path, src->path, src->sz_path );
  dest->path[ src->sz_path ] = '\0';
  __copy_headers( dest->headers, src->headers );
  dest->content_type = src->content_type;
  dest->accept_type = src->accept_type;
}



/*******************************************************************//**
 * Copy src request into dest request
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__copy_all( vgx_VGXServerRequest_t *dest, const vgx_VGXServerRequest_t *src ) {
  vgx_server_request__copy_clean( dest, src );
  iStreamBuffer.Clear( dest->buffers.stream );
  iStreamBuffer.Clear( dest->buffers.content );
  iStreamBuffer.Copy( dest->buffers.stream, src->buffers.stream );
  iStreamBuffer.Copy( dest->buffers.content, src->buffers.content );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerRequest_t * vgx_server_request__new( HTTPRequestMethod method, const char *path ) {
  vgx_VGXServerRequest_t *request = calloc( 1, sizeof( vgx_VGXServerRequest_t ) );
  if( request ) {
#ifdef _DEBUG
    const char *label = path;
#else
    const char *label = NULL;
#endif
    if( vgx_server_request__init( request, label ) < 0 ) {
      free( request );
      request = NULL;
    }
    else {
      request->method = method;
      if( path ) {
        const char *src = path;
        char *dest = request->path;
        int n = 0;
        while( *src != '\0' && ++n < HTTP_PATH_MAX ) {
          *dest++ = *src++;
        }
        *dest = '\0';
        request->sz_path = (int16_t)n;
      }
    }
  }
  return request;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__destroy( vgx_VGXServerRequest_t *request ) {
  // path
  if( request->path ) {
    free( request->path );
  }
  // buffer
  iStreamBuffer.Delete( &request->buffers.stream );
  // content
  iStreamBuffer.Delete( &request->buffers.content );
  // headers
  __delete_headers_object( &request->headers );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_request__delete( vgx_VGXServerRequest_t **request ) {
  if( request && *request ) {
    // Destroy
    vgx_server_request__destroy( *request );
    // Free
    free( *request );
    *request = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_request__add_header( vgx_VGXServerRequest_t *request, const char *field, const char *value ) {
  char buffer[1024];
  const char *endf = buffer + 127;
  const char *end = buffer + 1020;
  char *p = buffer;
  const char *rp = field;
  while( *rp && p < endf ) {
    *p++ = *rp++;
  }
  *p++ = ':';
  *p++ = ' ';
  rp = value;
  while( *rp && p < end ) {
    *p++ = *rp++;
  }
  *p++ = '\r';
  *p++ = '\n';
  *p = '\0';
  return __add_raw_request_header( request, buffer, p-buffer );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t vgx_server_request__add_content( vgx_VGXServerRequest_t *request, const char *data, int64_t sz ) {
  return iStreamBuffer.Write( request->buffers.content, data, sz );
}


