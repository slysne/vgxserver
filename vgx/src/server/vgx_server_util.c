/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_util.c
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



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_util__sendall( vgx_URI_t *URI, vgx_VGXServerRequest_t *request, int timeout_ms ) {
  int ret = 0;

  const char *path = iURI.Path( URI );
  const char *query = iURI.Query( URI ); 

  int64_t sz_path_query = iURI.PathLength( URI ) + iURI.QueryLength( URI );
  // Allocate enough for HTTP protocol, some headers plus the path and query, plus big margin
  int64_t sz_linebuf = 1024 + sz_path_query;
  char *_linebuf = calloc( sz_linebuf, sizeof(char) );
  if( _linebuf == NULL ) {
    return -1;
  }

  char *line = _linebuf;

  XTRY {

    CXSOCKET *psock = iURI.Sock.Output.Get( URI );
    if( psock == NULL ) {
      if( (psock = iURI.Sock.Input.Get( URI )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }
    }

    const char *method;
    if( request->method == HTTP_NONE ) {
      method = "GET"; // default
    }
    else {
      method = __vgx_http_request_method( request->method );
    }

    // HTTP/1.1 by default
    if( request->version.bits == 0 ) {
      request->version.major = 1;
      request->version.minor = 1;
    }


    char *p = line;
    int64_t n;
    int64_t remain = sz_linebuf - 1;
    // Initial request line
    if( query && query[0] ) {
      if( (n = snprintf( p, remain, "%s %s?%s HTTP/%d.%d" CRLF, method, path, query, (int)request->version.major, (int)request->version.minor )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }
    else {
      if( (n = snprintf( p, remain, "%s %s HTTP/%d.%d" CRLF, method, path, (int)request->version.major, (int)request->version.minor )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
    }
    p += n;
    remain -= n;
    // Accept:
    if( remain > 0 && request->accept_type != MEDIA_TYPE__ANY ) {
      if( (n = snprintf( p, remain, "Accept: %s" CRLF, __get_http_mediatype( request->accept_type ) )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
      p += n;
      remain -= n;
    }
    // Content-Type:
    if( remain > 0 && request->content_type != MEDIA_TYPE__ANY ) {
      if( (n = snprintf( p, remain, "Content-Type: %s" CRLF, __get_http_mediatype( request->content_type ) )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
      p += n;
      remain -= n;
    }
    // Content-Length:
    int64_t sz_content = request->buffers.content ? iStreamBuffer.Size( request->buffers.content ) : 0;
    if( remain > 0 && sz_content > 0 ) {
      if( (n = snprintf( p, remain, "Content-Length: %lld" CRLF, sz_content )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }
      p += n;
      remain -= n;
    }
    if( request->headers ) {
      request->headers->content_length = sz_content;
      char **H = request->headers->list;
      for( int64_t i=0; i<request->headers->sz; i++ ) {
        const char *h = *H++;
        int64_t sz = *H - h;
        if( remain >= sz ) {
          memcpy( p, h, sz );
          p += sz;
          remain -= sz;
        }
      }
    }
    // Newline
    if( remain > 0 ) {
      if( (n = snprintf( p, remain, CRLF )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
      }
      p += n;
      remain -= n;
    }

    const char *data = line;
    int64_t sz_data = p - line;
    int64_t err;

    // No content
    if( sz_content == 0 ) {
      if( (err = cxsendall( psock, data, sz_data, timeout_ms )) == sz_data ) {
        XBREAK;
      }
      else {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x009, "cxsendall() error: %lld", err );
      }
    }

    // Nonzero content
    // Fill existing line buffer as full as we can and send
    const char *segment;
    int64_t sz_segment = iStreamBuffer.ReadableSegment( request->buffers.content, remain, &segment, NULL );
    iStreamBuffer.AdvanceRead( request->buffers.content, sz_segment );
    memcpy( p, segment, sz_segment );
    sz_data += sz_segment;
    remain -= sz_segment;
    if( (err = cxsendall( psock, data, sz_data, timeout_ms )) != sz_data ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00A, "cxsendall() error: %lld", err );
    }
    
    // Send rest of content in chunks
    while( (sz_content = iStreamBuffer.Size( request->buffers.content )) > 0 ) {
      int64_t maxsend = minimum_value( sz_content, SEND_CHUNK_SZ );
      sz_segment = iStreamBuffer.ReadableSegment( request->buffers.content, maxsend, &segment, NULL );
      iStreamBuffer.AdvanceRead( request->buffers.content, sz_segment );
      if( (err = cxsendall( psock, segment, sz_segment, timeout_ms )) != sz_segment ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00B, "cxsendall() error: %lld", err );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( _linebuf );
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_util__recvall_sock( CXSOCKET *psock, vgx_VGXServerResponse_t *response, int timeout_ms ) {
#ifdef CXPLAT_WINDOWS_X64
#define __nfds( Socket ) 0 /* ignored on windows */
#else
#define __nfds( Socket ) (Socket)->s + 1 /* Set to the highest-numbered file descriptor plus 1 */
#endif

  int ret = 0;

  vgx_StreamBuffer_t *workbuf = NULL;
  char *_linebuf = NULL;

  XTRY {
    if( psock == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x000 );
    }

    vgx_server_response__reset( response );

    int64_t t0_ns = __GET_CURRENT_NANOSECOND_TICK();
    int64_t t1_ns = t0_ns;
    int64_t deadline_ns = timeout_ms < 0 ? LLONG_MAX : t0_ns + (timeout_ms * 1000000LL);

    if( (_linebuf = malloc( HTTP_LINE_MAX )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    char *line = _linebuf;

    if( (workbuf = iStreamBuffer.New( 13 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    vgx_VGXServerClientState state = VGXSERVER_CLIENT_STATE__EXPECT_INITIAL;

    int64_t maxreceive = RECV_CHUNK_SZ;
    char *write_segment;
    const char *read_segment;
    int64_t sz_segment;

    do {
      // Remaining microseconds
      int64_t remain_ns = deadline_ns - t1_ns;
      struct timeval tim = {
        .tv_sec = (int)(remain_ns / 1000000000),
        .tv_usec = (int)((remain_ns % 1000000000) / 1000)
      };

      // Wait until socket is readable
      fd_set Readable;
      FD_ZERO( &Readable );
      IGNORE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT
      FD_SET( psock->s, &Readable );
      RESUME_WARNINGS

      int s = select( __nfds( psock ), &Readable, NULL, NULL, &tim );

      // Socket is readable
      if( s > 0 && FD_ISSET( psock->s, &Readable ) ) {
        // Get a linear segment into which we can receive at least one byte
        while( (sz_segment = iStreamBuffer.WritableSegment( workbuf, maxreceive, &write_segment, NULL )) < 1 ) {
          if( iStreamBuffer.Expand( workbuf ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
          }
        }
        
        // Receive bytes into linear segment
        int64_t n_recv = cxrecv( psock, write_segment, minimum_value( sz_segment, RECV_CHUNK_SZ ), 0 );
        if( n_recv > 0 ) {
          iStreamBuffer.AdvanceWrite( workbuf, n_recv );
          int64_t sz_line;
          while( iStreamBuffer.IsReadable( workbuf ) ) {
            // We are expecting initial response line or headers
            if( state < VGXSERVER_CLIENT_STATE__EXPECT_CONTENT ) {
              // Try to read a full line
              if( (sz_line = iStreamBuffer.ReadUntil( workbuf, HTTP_LINE_MAX, &line, '\n' )) > 0 ) {
                // The line is the initial response line
                if( state == VGXSERVER_CLIENT_STATE__EXPECT_INITIAL ) {
                  if( vgx_server_parser__parse_response_initial_line( line, response ) < 0 ) {
                    THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
                  }
                  state = VGXSERVER_CLIENT_STATE__EXPECT_HEADERS;
                }
                // The line is a header line
                else if( state == VGXSERVER_CLIENT_STATE__EXPECT_HEADERS ) {
                  vgx_HTTPResponseHeaderField field = vgx_server_parser__parse_response_header_line( line, response );
                  // End of headers
                  if( field == HTTP_RESPONSE_HEADER__END_OF_HEADERS ) {
                    state = VGXSERVER_CLIENT_STATE__EXPECT_CONTENT;
                  }
                  // Header
                  else if( field == HTTP_RESPONSE_HEADER_FIELD__ContentLength ) {
                    if( response->buffers.content == NULL ) {
                      int order = imag2( response->content_length + 1 );
                      if( (response->buffers.content = iStreamBuffer.New( order )) == NULL ) {
                        THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
                      }
                    }
                  }
                  else {
                    // handle ?
                  }
                }
                // Invalid state
                else {
                  THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
                }
              }
              // Not a complete line, we need to receive more
              else {
                break;
              }
            }
            // We are expecting body content
            else if( response->content_length > 0 && response->buffers.content != NULL ) {
              int64_t remain = response->content_length - iStreamBuffer.Size( response->buffers.content );
              while( remain > 0 && (sz_segment = iStreamBuffer.ReadableSegment( workbuf, remain, &read_segment, NULL )) > 0 ) {
                if( iStreamBuffer.Write( response->buffers.content, read_segment, sz_segment ) < 0 ) {
                  THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
                }
                iStreamBuffer.AdvanceRead( workbuf, sz_segment );
                remain -= sz_segment;
              }
              // Response complete
              if( remain == 0 ) {
                XBREAK;
              }
              // We need more data
              else {
                maxreceive = remain;
                break;
              }
            }
            else {
              XBREAK;
            }
          }
        }
        // Socket error if failure to read was not caused by "wouldblock"
        else if( !iURI.Sock.Busy( errno ) ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
        }
      }

      // Refresh current timestamp
      t1_ns = __GET_CURRENT_NANOSECOND_TICK();

    } while( t1_ns < deadline_ns );

    THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iStreamBuffer.Delete( &workbuf );
    free( _linebuf );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_util__recvall( vgx_URI_t *URI, vgx_VGXServerResponse_t *response, int timeout_ms ) {
  CXSOCKET *psock = iURI.Sock.Input.Get( URI );
  if( psock == NULL ) {
    if( (psock = iURI.Sock.Output.Get( URI )) == NULL ) {
      return -1;
    }
  }

  return vgx_server_util__recvall_sock( psock, response, timeout_ms );
}
