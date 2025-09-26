/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_xvgxpartial.c
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

#include "pyvgx.h"
#include <marshal.h>


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );



#define STR_MEDIA_TYPE__application_octet_stream      "application/octet-stream"
#define sz_STR_MEDIA_TYPE__application_octet_stream   (sizeof(STR_MEDIA_TYPE__application_octet_stream) - 1)
#define STR_MEDIA_TYPE__application_json              "application/json"
#define sz_STR_MEDIA_TYPE__application_json           (sizeof(STR_MEDIA_TYPE__application_json) - 1)
#define STR_MEDIA_TYPE__text_plain                    "text/plain"
#define sz_STR_MEDIA_TYPE__text_plain                 (sizeof(STR_MEDIA_TYPE__text_plain) - 1)


static PyObject *g_py_key_partials = NULL;
static PyObject *g_py_key_levelparts = NULL;
static PyObject *g_py_key_level = NULL;
static PyObject *g_py_key_hitcount = NULL;
static PyObject *g_py_key_aggregator = NULL;
static PyObject *g_py_key_message = NULL;
static PyObject *g_py_key_entries = NULL;
static PyObject *g_py_content_length = NULL;
static PyObject *g_py_content_type = NULL;
static PyObject *g_py_application_octet_stream = NULL;
static PyObject *g_py_application_json = NULL;
static PyObject *g_py_text_plain = NULL;
static PyObject *g_py_x_vgx_partial_target = NULL;



/******************************************************************************
 * ident
 * Returns: ( defined, width, height, depth, partition, replica, channel, primary )
 *
 ******************************************************************************
 */
static PyObject * __get_client_ident( vgx_VGXServerClient_t *client ) {
  if( client == NULL || client->partial_ident.defined == 0 ) {
    return Py_BuildValue( "iiiiiiii", 0, 1, 1, 1, 0, 0, 0, 1 );
  }

  x_vgx_partial_ident *p = &client->partial_ident;
  return Py_BuildValue( "iBBBBBBB", 1, p->matrix.width, p->matrix.height, p->matrix.depth, p->position.partition, p->position.replica, p->position.channel, p->position.primary );
}



/******************************************************************************
 * partition
 *
 ******************************************************************************
 */
static PyObject * __get_client_partition( vgx_VGXServerClient_t *client ) {
  int partition = 0;
  if( client && client->partial_ident.defined ) {
    partition = client->partial_ident.position.partition;
  }
  return PyLong_FromLong( partition );
}



/******************************************************************************
 * width
 *
 ******************************************************************************
 */
static PyObject * __get_client_width( vgx_VGXServerClient_t *client ) {
  int width = 1;
  if( client && client->partial_ident.defined ) {
    width = client->partial_ident.matrix.width;
  }
  return PyLong_FromLong( width );
}



/******************************************************************************
 * replica
 *
 ******************************************************************************
 */
static PyObject * __get_client_replica( vgx_VGXServerClient_t *client ) {
  int replica = 0;
  if( client && client->partial_ident.defined ) {
    replica = client->partial_ident.position.replica;
  }
  return PyLong_FromLong( replica );
}



/******************************************************************************
 * height
 *
 ******************************************************************************
 */
static PyObject * __get_client_height( vgx_VGXServerClient_t *client ) {
  int height = 1;
  if( client && client->partial_ident.defined ) {
    height = client->partial_ident.matrix.height;
  }
  return PyLong_FromLong( height );
}



/******************************************************************************
 * channel
 *
 ******************************************************************************
 */
static PyObject * __get_client_channel( vgx_VGXServerClient_t *client ) {
  int channel = 0;
  if( client && client->partial_ident.defined ) {
    channel = client->partial_ident.position.channel;
  }
  return PyLong_FromLong( channel );
}



/******************************************************************************
 * depth
 *
 ******************************************************************************
 */
static PyObject * __get_client_depth( vgx_VGXServerClient_t *client ) {
  int depth = 1;
  if( client && client->partial_ident.defined ) {
    depth = client->partial_ident.matrix.depth;
  }
  return PyLong_FromLong( depth );
}



/******************************************************************************
 * toplevel
 *
 ******************************************************************************
 */
static PyObject * __get_client_toplevel( vgx_VGXServerClient_t *client ) {
  if( client && client->response.mediatype != MEDIA_TYPE__application_x_vgx_partial ) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}



/******************************************************************************
 * hasmatrix
 *
 ******************************************************************************
 */
static PyObject * __get_client_hasmatrix( vgx_VGXServerClient_t *client ) {
  if( client && client->env.main_server ) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}



/******************************************************************************
 * port
 *
 ******************************************************************************
 */
static PyObject * __get_client_port( vgx_VGXServerClient_t *client ) {
  if( client == NULL ) {
    return PyLong_FromLong( -1 );
  }
  return PyLong_FromLong( client->env.port_base + client->env.port_offset );
}



/******************************************************************************
 * baseport
 *
 ******************************************************************************
 */
static PyObject * __get_client_baseport( vgx_VGXServerClient_t *client ) {
  if( client == NULL ) {
    return PyLong_FromLong( -1 );
  }
  return PyLong_FromLong( client->env.port_base );
}










/******************************************************************************
 *
 * ===================
 * PyVGX_PluginRequest
 * ===================
 *
 ******************************************************************************
 */



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
__inline static void __delete_signature( PyVGX_PluginRequest *py_plugreq ) {
  idunset( &py_plugreq->request->headers->signature );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __process_uri_parameters( PyVGX_PluginRequest *py_plugreq ) {
  vgx_VGXServerRequest_t *request = py_plugreq->request;
  vgx_URIQueryParameters_t params = {0};
  // Parse request path to extract parameter keyval pairs
  if( iURI.Parse.SetQueryParam( request->path, &params ) < 0 ) {
    PyErr_SetString( PyVGX_RequestError, "bad request" );
    return -1;
  }
  // 
  if( py_plugreq->py_params == NULL ) {
    if( (py_plugreq->py_params = PyDict_New()) == NULL ) {
      return -1;
    }
  }
  //
  __delete_signature( py_plugreq );
  // Build params dict
  vgx_KeyVal_t *cursor = params.keyval;
  vgx_KeyVal_t *kv_end = cursor + params.sz;
  while( cursor < kv_end ) {
    if( __pyvgx_plugin__map_keyval_to_dict( cursor++, py_plugreq->py_params ) != 0 ) {
      return -1;
    }
  }
  return 0;
}



/******************************************************************************
 * path
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_path( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return PyUnicode_FromStringAndSize( py_plugreq->request->path, py_plugreq->request->sz_path );
}



/******************************************************************************
 * method
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_method( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  const char *method = __vgx_http_request_method( py_plugreq->request->method );
  return PyUnicode_FromString( method );
}



/******************************************************************************
 * params
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_params( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  __delete_signature( py_plugreq ); // since params may be modified after this
  Py_INCREF( py_plugreq->py_params );
  return py_plugreq->py_params;
}



/******************************************************************************
 * headers
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_headers( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  if( py_plugreq->py_headers == NULL ) {
    return __vgx_PyBytes_FromHTTPHeaders( py_plugreq->request->headers );
  }
  Py_INCREF( py_plugreq->py_headers );
  return py_plugreq->py_headers;
}



/******************************************************************************
 * t0
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_t0( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  int64_t t0 = -1;
  vgx_VGXServerClient_t *client = py_plugreq->request->headers->client;
  if( client ) {
    t0 = client->tbase_ns + client->io_t0_ns;
  }
  return PyLong_FromLongLong( t0 );
}



/******************************************************************************
 * sn
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_sn( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  vgx_HTTPHeaders_t *h = py_plugreq->request->headers;
  return PyLong_FromLongLong( h->sn );
}



/******************************************************************************
 * signature
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_signature( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  char fmtbuf[33];
  vgx_HTTPHeaders_t *h = py_plugreq->request->headers;
  if( idnone( &h->signature ) ) {
    vgx_StreamBuffer_t *buf = iStreamBuffer.New( 4 );
    if( buf == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      return NULL;
    }

     Py_ssize_t sz_str;
    const char *str;

    // params
    if( PyDict_Size(py_plugreq->py_params) > 0 ) {
      iStreamBuffer.Write( buf, "?", 1 );
      PyObject *py_key, *py_value;
      Py_ssize_t pos = 0;
      while( PyDict_Next( py_plugreq->py_params, &pos, &py_key, &py_value ) ) {
        
        if( (str = PyUnicode_AsUTF8AndSize( py_key, &sz_str )) == NULL ) {
          if( PyBytes_AsStringAndSize( py_key, (char**)&str, &sz_str ) < 0 ) {
            PyErr_Clear();
            continue; // ignore
          }
        }
        iStreamBuffer.Write( buf, str, sz_str );

        if( py_value == Py_None ) {
          continue;
        }

        if( PyLong_Check( py_value ) ) {
          sz_str = snprintf( fmtbuf, 32, "=%lld", PyLong_AsLongLong( py_value ) );
          iStreamBuffer.Write( buf, fmtbuf, sz_str );
        }
        else if( PyFloat_Check( py_value ) ) {
          sz_str = snprintf( fmtbuf, 32, "=%f", PyFloat_AsDouble( py_value ) );
          iStreamBuffer.Write( buf, fmtbuf, sz_str );
        }
        else {
          if( PyUnicode_CheckExact( py_value ) ) {
            if( (str = PyUnicode_AsUTF8AndSize( py_value, &sz_str )) == NULL ) {
              PyErr_Clear();
              continue; // ignore
            }
          }
          else if( PyBytes_CheckExact( py_value ) ) {
            if( PyBytes_AsStringAndSize( py_value, (char**)&str, &sz_str ) < 0 ) {
              PyErr_Clear();
              continue; // ignore
            }
          }
          else {
            continue; // ignore
          }

          iStreamBuffer.Write( buf, "=", 1 );
          iStreamBuffer.Write( buf, str, sz_str );
        }
        iStreamBuffer.Write( buf, "&", 1 );
      }
    }

    // append path up to beginning of parameters
    const char *p = str = py_plugreq->request->path;
    sz_str = 0;
    while( *p && *p++ != '?' ) {
      ++sz_str;
    }
    iStreamBuffer.Write( buf, str, sz_str );

    // digest
    BEGIN_PYVGX_THREADS {
      sz_str = iStreamBuffer.ReadableSegment( buf, LLONG_MAX, &str, NULL );
      h->signature = obid_from_string_len(str, (unsigned)sz_str );
      iStreamBuffer.Delete( &buf );
    } END_PYVGX_THREADS;
  }

  idtostr( fmtbuf, &h->signature );

  return PyUnicode_FromStringAndSize( fmtbuf, 32 );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_signature
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_signature( PyVGX_PluginRequest *py_plugreq, PyObject *py_signature, void *closure ) {

  vgx_HTTPHeaders_t *h = py_plugreq->request->headers;

  if( py_signature == NULL || py_signature == Py_None ) {
    idunset( &h->signature );
    return 0;
  }

  if( !PyUnicode_Check( py_signature ) ) {
    PyErr_Format( PyExc_TypeError, "signature must be str, not %s", Py_TYPE( py_signature )->tp_name );
    return -1;
  }
  const char *data = PyUnicode_AsUTF8( py_signature );
  objectid_t s = strtoid( data );
  if( idnone(&s) ) {
    PyErr_SetString( PyExc_ValueError, "invalid signature" );
    return -1;
  }
  h->signature = s;
  return 0;
}



/******************************************************************************
 * executor
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_executor( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return PyLong_FromLongLong( py_plugreq->request->executor_id );
}



/******************************************************************************
 * flag
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_flag( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  vgx_HTTPHeaders_t *h = py_plugreq->request->headers;
  if( !h->flag.__bits ) {
    Py_RETURN_NONE;
  }
  return PyUnicode_FromStringAndSize( h->flag.value, strnlen( h->flag.value, sizeof(h->flag) ) );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_flag
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_flag( PyVGX_PluginRequest *py_plugreq, PyObject *py_flag, void *closure ) {
  vgx_HTTPHeaders_t *h = py_plugreq->request->headers;

  if( py_flag == NULL || py_flag == Py_None ) {
    h->flag.__bits = 0; 
    return 0;
  }

  if( !PyUnicode_Check( py_flag ) ) {
    PyErr_SetString( PyExc_TypeError, "flag must be string" );
    return -1;
  }

  Py_ssize_t sz = 0;
  const char *flag = PyUnicode_AsUTF8AndSize( py_flag, &sz );

  if( sz > sizeof(h->flag) ) {
    PyErr_SetString( PyExc_ValueError, "too many characters in flag" );
    return -1;
  }

  h->flag.__bits = 0; 
  strncpy( h->flag.value, flag, sizeof(h->flag) );

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
void destroy_request_local_capsule( vgx_HTTPHeadersCapsule_t *capsule ) {
  if( capsule == NULL ) {
    return;
  }

  if( capsule->data ) {
    BEGIN_PYTHON_INTERPRETER {
      PyObject *py_capsule = (PyObject*)capsule->data;
      PyObject *py_obj = (PyObject*)PyCapsule_GetPointer( py_capsule, NULL );
      Py_XDECREF( py_obj );
      Py_DECREF( py_capsule );
    } END_PYTHON_INTERPRETER;
    capsule->data = NULL;
  }

  capsule->destroyf = NULL;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * local_capsule_get( vgx_HTTPHeaders_t *h ) {
  if( !h->capsule.data ) {
    Py_RETURN_NONE;
  }

  PyObject *py_capsule = (PyObject*)h->capsule.data;
  PyObject *py_obj = (PyObject*)PyCapsule_GetPointer( py_capsule, NULL );
  if( py_obj == NULL ) {
    Py_RETURN_NONE;
  }

  Py_INCREF( py_obj );
  return py_obj;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int local_capsule_set( vgx_HTTPHeaders_t *h, PyObject *py_obj ) {

  if( py_obj == NULL || py_obj == Py_None ) {
    DESTROY_HEADERS_CAPSULE( &h->capsule );
    return 0;
  }

  Py_INCREF( py_obj );

  PyObject *py_capsule = PyCapsule_New( py_obj, NULL, NULL );
  if( py_capsule == NULL ) {
    Py_DECREF( py_obj );
    return -1;
  }

  h->capsule.data = py_capsule;
  h->capsule.destroyf = destroy_request_local_capsule;

  return 0;

}



/******************************************************************************
 * local
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_local( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return local_capsule_get( py_plugreq->request->headers );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_local
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_local( PyVGX_PluginRequest *py_plugreq, PyObject *py_obj, void *closure ) {
  return local_capsule_set( py_plugreq->request->headers, py_obj );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __update_headers( PyVGX_PluginRequest *py_plugreq, vgx_MediaType mtype, int64_t sz_bytes, vgx_VGXServerRequest_t *request ) {

  PyObject *py_dict_headers = NULL;
  PyObject *py_str_mtype = NULL;

  switch( mtype ) {
  case MEDIA_TYPE__NONE:
    break;
  case MEDIA_TYPE__application_octet_stream:
    py_str_mtype = g_py_application_octet_stream;
    break;
  case MEDIA_TYPE__application_json:
    py_str_mtype = g_py_application_json;
    break;
  default:
    py_str_mtype = g_py_text_plain;
  }

  PyObject *py_nbytes = NULL;
  PyObject *py_str_target = NULL;

  int ret = 0;

  XTRY {

    // Rebuild headers from raw data if not already dict
    if( py_plugreq->py_headers == NULL || !PyDict_CheckExact( py_plugreq->py_headers ) ) {
      if( (py_dict_headers = PyDict_New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // Populate dict from raw headers data
      __pyvgx_plugin__update_object_from_headers( request->headers, py_dict_headers, __pyvgx_plugin__set_dict_keyval );
    }
    else {
      py_dict_headers = py_plugreq->py_headers; // already dict
    }

    if( sz_bytes > 0 ) {
      // content length
      if( (py_nbytes = PyUnicode_FromFormat( "%lld", sz_bytes )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

      // content-type
      // update headers dict (if this fails content-length and content-type will be out of sync)
      if( PyDict_SetItem( py_dict_headers, g_py_content_length, py_nbytes ) < 0 ||
          PyDict_SetItem( py_dict_headers, g_py_content_type, py_str_mtype ) )
      {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
    }
    else {
      if( PyDict_GetItem( py_dict_headers, g_py_content_length ) ) {
        PyDict_DelItem( py_dict_headers, g_py_content_length );
      }
      if( PyDict_GetItem( py_dict_headers, g_py_content_type ) ) {
        PyDict_DelItem( py_dict_headers, g_py_content_type );
      }
    }

    // x-vgx-partial-target
    if( py_plugreq->request->target_partial >= 0 ) {
      if( (py_str_target = PyUnicode_FromFormat( "%d", (int)py_plugreq->request->target_partial )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
      // update headers dict (if this fails content-length and content-type will be out of sync)
      if( PyDict_SetItem( py_dict_headers, g_py_x_vgx_partial_target, py_str_target ) ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
    }
    else {
      if( PyDict_GetItem( py_dict_headers, g_py_x_vgx_partial_target ) ) {
        PyDict_DelItem( py_dict_headers, g_py_x_vgx_partial_target );
      }
    }

    // We created new headers dict above - replace
    if( py_plugreq->py_headers != py_dict_headers ) {
      Py_XDECREF( py_plugreq->py_headers );
      py_plugreq->py_headers = py_dict_headers;
      py_dict_headers = NULL;
    }

    // Assign header values to request object
    request->headers->content_length = sz_bytes;
    request->content_type = mtype;

  }
  XCATCH( errcode ) {
    if( py_plugreq->py_headers != py_dict_headers ) {
      Py_XDECREF( py_dict_headers );
    }
    ret = -1;
  }
  XFINALLY {
    Py_XDECREF( py_nbytes );
    Py_XDECREF( py_str_target );
  }

  return ret;
}




/******************************************************************************
 * content
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_content( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  if( py_plugreq->request->content_type == MEDIA_TYPE__NONE ) {
    Py_RETURN_NONE;
  }

  // Raw buffer: convert to bytes on first access
  if( py_plugreq->py_content == NULL ) {
    const char *bytes;
    int64_t sz = iStreamBuffer.ReadableSegment( py_plugreq->request->buffers.content, LLONG_MAX, &bytes, NULL );
    if( (py_plugreq->py_content = PyBytes_FromStringAndSize( bytes, sz )) == NULL ) {
      return NULL;
    }
  }

  Py_INCREF( py_plugreq->py_content );
  return py_plugreq->py_content;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_content
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_content( PyVGX_PluginRequest *py_plugreq, PyObject *py_content, void *closure ) {

  int ret = 0;
  vgx_VGXServerRequest_t *request = py_plugreq->request;
  PyObject *py_bytes = NULL;
  int64_t sz_bytes = 0;
  vgx_MediaType mtype;

  XTRY {

    if( py_content ) {
      // Capsule
      // Verify capsule points to the raw request content buffer but leave py_content NULL
      if( PyCapsule_CheckExact( py_content ) ) {
        const char *mem = PyCapsule_GetPointer( py_content, NULL );
        const char *buf;
        sz_bytes = iStreamBuffer.ReadableSegment( py_plugreq->request->buffers.content, LLONG_MAX, &buf, NULL );
        if( mem != buf ) {
          PyErr_SetString( PyExc_Exception, "internal error (invalid content buffer)" );
          THROW_ERROR( CXLIB_ERR_BUG, 0x001 );
        }
        mtype = MEDIA_TYPE__application_octet_stream;
      }
      else {
        // Raw bytes
        if( PyBytes_CheckExact( py_content ) ) {
          Py_INCREF( py_content );
          py_bytes = py_content;
          mtype = MEDIA_TYPE__application_octet_stream;
        }
        // String
        else if( PyUnicode_CheckExact( py_content ) ) {
          if( (py_bytes = PyUnicode_AsUTF8String( py_content )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
          }
          mtype = MEDIA_TYPE__text_plain;
        }
        // Any other object will be converted to json bytes
        else {
          if( (py_bytes = iPyVGXCodec.NewJsonPyBytesFromPyObject( py_content )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
          }
          mtype = MEDIA_TYPE__application_json;
        }

        sz_bytes = PyBytes_Size( py_bytes );
      }
    }
    else {
      mtype = MEDIA_TYPE__NONE;
      sz_bytes = 0;
    }

    if( __update_headers( py_plugreq, mtype, sz_bytes, request ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Assign content to main object
    Py_XDECREF( py_plugreq->py_content );
    py_plugreq->py_content = py_bytes; // may be NULL if PyCapsule is used as indicator to access raw request content buffer

  }
  XCATCH( errcode ) {
    ret = -1;
    Py_XDECREF( py_bytes );
  }
  XFINALLY {
  }

  return ret;
}



/******************************************************************************
 * partial
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_partial( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  if( py_plugreq->request->target_partial >= 0 ) {
    return PyLong_FromLong( py_plugreq->request->target_partial );
  }
  Py_RETURN_NONE;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_partial
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_partial( PyVGX_PluginRequest *py_plugreq, PyObject *py_partial, void *closure ) {
  if( py_partial == NULL || py_partial == Py_None ) {
    py_plugreq->request->target_partial = -1;
  }
  else if( PyLong_CheckExact(py_partial) ) {
    int64_t p = PyLong_AsLongLong( py_partial );
    py_plugreq->request->target_partial = p < 0 ? -1 : (int8_t)(p & 0x7f);
  }
  else {
    PyErr_SetString( PyExc_TypeError, "partial must be int" );
    return -1;
  }

  int ret;
  if( py_plugreq->py_headers && PyDict_CheckExact( py_plugreq->py_headers ) && py_plugreq->request->target_partial >= 0 ) {
    PyObject *py_target = PyUnicode_FromFormat( "%d", (int)py_plugreq->request->target_partial );
    if( py_target == NULL ) {
      return -1;
    }
    ret = PyDict_SetItem( py_plugreq->py_headers, g_py_x_vgx_partial_target, py_target );
    Py_DECREF( py_target );
  }
  else {
    ret = __update_headers( py_plugreq, py_plugreq->request->content_type, py_plugreq->request->headers->content_length, py_plugreq->request );
  }

  return ret;
}



/******************************************************************************
 * affinity
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_affinity( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  if( py_plugreq->request->replica_affinity >= 0 ) {
    return PyLong_FromLong( py_plugreq->request->replica_affinity );
  }
  Py_RETURN_NONE;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_affinity
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_affinity( PyVGX_PluginRequest *py_plugreq, PyObject *py_affinity, void *closure ) {
  if( py_affinity == NULL || py_affinity == Py_None ) {
    py_plugreq->request->replica_affinity = -1;
    return 0;
  }

  if( PyLong_CheckExact(py_affinity) ) {
    py_plugreq->request->replica_affinity = (int8_t)(PyLong_AsLongLong( py_affinity ) & 0x7f);
    return 0;
  }

  PyErr_SetString( PyExc_TypeError, "affinity must be int" );
  return -1;
}



/******************************************************************************
 * primary
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_primary( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  if( py_plugreq->request->headers->client == NULL ) {
    return PyLong_FromLong( -1 );
  }
  return PyLong_FromLong( py_plugreq->request->headers->client->flags.primary );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_set_primary
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_primary( PyVGX_PluginRequest *py_plugreq, PyObject *py_primary, void *closure ) {
  int p;
  if( py_primary == NULL ) {
    PyErr_SetString( PyExc_AttributeError, "cannot delete primary attribute" );
    return -1;
  }

  if( (p = PyLong_AsLong( py_primary )) < 0 ) {
    return -1;
  }
  if( py_plugreq->request->headers->client ) {
    py_plugreq->request->headers->client->flags.primary = p > 0;
  }
  return 0;
}



/******************************************************************************
 * ident
 * Returns: ( defined, width, height, depth, partition, replica, channel, primary )
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_ident( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_ident( py_plugreq->request->headers->client );
}



/******************************************************************************
 * partition
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_partition( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_partition( py_plugreq->request->headers->client );
}



/******************************************************************************
 * width
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_width( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_width( py_plugreq->request->headers->client );
}



/******************************************************************************
 * replica
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_replica( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_replica( py_plugreq->request->headers->client );
}



/******************************************************************************
 * height
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_height( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_height( py_plugreq->request->headers->client );
}



/******************************************************************************
 * channel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_channel( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_channel( py_plugreq->request->headers->client );
}



/******************************************************************************
 * depth
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_depth( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_depth( py_plugreq->request->headers->client );
}



/******************************************************************************
 * toplevel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_toplevel( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_toplevel( py_plugreq->request->headers->client );
}



/******************************************************************************
 * hasmatrix
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_hasmatrix( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_hasmatrix( py_plugreq->request->headers->client );
}



/******************************************************************************
 * port
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_port( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_port( py_plugreq->request->headers->client );
}



/******************************************************************************
 * baseport
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest_get_baseport( PyVGX_PluginRequest *py_plugreq, void *closure ) {
  return __get_client_baseport( py_plugreq->request->headers->client );
}



/******************************************************************************
 * PyVGX_PluginRequest__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_PluginRequest__dealloc( PyVGX_PluginRequest *py_plugreq ) {
  if( py_plugreq->owns_request ) {
    iVGXServer.Request.Delete( &py_plugreq->request );
  }
  Py_XDECREF( py_plugreq->py_params );
  Py_XDECREF( py_plugreq->py_headers );
  Py_XDECREF( py_plugreq->py_content );
  Py_TYPE( py_plugreq )->tp_free( py_plugreq );
}



/******************************************************************************
 * __new_PluginRequest
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __new_PluginRequest( void ) {
  PyVGX_PluginRequest *py_plugreq;
  if( (py_plugreq = (PyVGX_PluginRequest*)p_PyVGX_PluginRequestType->tp_alloc(p_PyVGX_PluginRequestType, 0)) == NULL ) {
    return NULL;
  }
  py_plugreq->request = NULL;
  py_plugreq->owns_request = false;
  py_plugreq->py_params = NULL;
  py_plugreq->py_headers = NULL;
  py_plugreq->py_content = NULL;
  return (PyObject *)py_plugreq;
}



/******************************************************************************
 * __init_PluginRequest
 *
 ******************************************************************************
 */
static int __init_PluginRequest( PyVGX_PluginRequest *py_plugreq, PyObject *py_plugin, PyObject *py_params, PyObject *py_headers, PyObject *py_content, PyObject *py_target, PyObject *py_request_capsule ) {

  int ret = 0;

  XTRY {

    // ---------------
    // request capsule
    // ---------------
    if( py_request_capsule ) {
      if( (py_plugreq->request = (vgx_VGXServerRequest_t*)PyCapsule_GetPointer( py_request_capsule, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_API, 0x001 );
      }
    }

    // -------
    // request
    // -------
    if( py_plugreq->request == NULL ) {
      if( py_plugin == NULL || !PyUnicode_Check( py_plugin ) ) {
        PyErr_SetString( PyVGX_RequestError, "plugin name (str) is required" );
        THROW_ERROR( CXLIB_ERR_API, 0x002 );
      }

      const char *plugin = PyUnicode_AsUTF8( py_plugin );

      char path[SZ_VGX_RESOURCE_PATH_BUFFER+1] = VGX_PLUGIN_PATH_PREFIX;
      strncpy( path + sz_VGX_SERVER_RESOURCE_PLUGIN_PATH_PREFIX, plugin, SZ_VGX_RESOURCE_PATH_BUFFER - sz_VGX_SERVER_RESOURCE_PLUGIN_PATH_PREFIX );
      HTTPRequestMethod method = (py_content && py_content != Py_None) ? HTTP_POST : HTTP_GET;
      if( (py_plugreq->request = iVGXServer.Request.New( method, path )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      py_plugreq->owns_request = true;
      py_plugreq->request->version.major = 1;
      py_plugreq->request->version.minor = 1;
      py_plugreq->request->accept_type = MEDIA_TYPE__application_x_vgx_partial;
    }

    // ------
    // params
    // ------
    if( py_params && py_params != Py_None ) {
      if( !PyDict_CheckExact( py_params ) ) {
        PyErr_SetString( PyVGX_RequestError, "params must be dict" );
        THROW_SILENT( CXLIB_ERR_API, 0x004 );
      }
      Py_INCREF( py_params );
      py_plugreq->py_params = py_params;
    }
    // inherit from core request
    else {
      if( __process_uri_parameters( py_plugreq ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x005 );
      }
    }

    // -------
    // headers
    // -------
    if( py_headers && py_headers != Py_None ) {
      Py_INCREF( py_headers );
      py_plugreq->py_headers = py_headers;
    }
    // inherit from core request
    else {
      if( (py_plugreq->py_headers = __vgx_PyBytes_FromHTTPHeaders( py_plugreq->request->headers )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x006 );
      }
    }

    // -------
    // content
    // -------
    if( py_plugreq->request->method == HTTP_POST && py_content && py_content != Py_None ) {
      if( PyVGX_PluginRequest_set_content( py_plugreq, py_content, NULL ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x007 );
      }
    }
    else {
      py_plugreq->request->content_type = MEDIA_TYPE__NONE;
    }

    // -------
    // partial
    // -------
    if( py_target && py_target != Py_None ) {
      if( PyVGX_PluginRequest_set_partial( py_plugreq, py_target, NULL ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x008 );
      }
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}




/******************************************************************************
 * PyVGX_PluginRequest__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  return __new_PluginRequest();
}



/******************************************************************************
 * PyVGX_PluginRequest__init
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest__init( PyVGX_PluginRequest *py_plugreq, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "plugin", "params", "headers", "content", "partial", "_request", NULL };
  PyObject *py_plugin = NULL;
  PyObject *py_params = NULL;
  PyObject *py_headers = NULL;
  PyObject *py_content = NULL;
  PyObject *py_target = NULL;
  PyObject *py_request_capsule = NULL;
  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|OOOOOO", kwlist, &py_plugin, &py_params, &py_headers, &py_content, &py_target, &py_request_capsule ) ) {
    return -1;
  }

  return __init_PluginRequest( py_plugreq, py_plugin, py_params, py_headers, py_content, py_target, py_request_capsule );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginRequest__vectorcall( PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "plugin",
    "params",
    "headers",
    "content",
    "partial",
    "_request",
    NULL
  };

  typedef union u_request_args {
    PyObject *args[6];
    struct {
      PyObject *py_plugin;
      PyObject *py_params;
      PyObject *py_headers;
      PyObject *py_content;
      PyObject *py_target;
      PyObject *py_request_capsule;
    };
  } request_args;

  request_args vcargs = {0};

  int64_t nargs = PyVectorcall_NARGS( nargsf );

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 6, vcargs.args ) < 0 ) {
    return NULL;
  }

  PyObject *pyobj = __new_PluginRequest();
  if( pyobj ) {
    if( __init_PluginRequest( (PyVGX_PluginRequest*)pyobj, vcargs.py_plugin, vcargs.py_params, vcargs.py_headers, vcargs.py_content, vcargs.py_target, vcargs.py_request_capsule ) < 0 ) {
      PyVGX_PluginRequest__dealloc( (PyVGX_PluginRequest*)pyobj );
      return NULL;
    }
  }
  return pyobj;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN PyVGX_PluginRequest * __pyvgx_PluginRequest_New( vgx_VGXServerRequest_t *request, PyObject *py_params, PyObject *py_headers, PyObject *py_content ) {
  PyVGX_PluginRequest *py_plugreq = NULL;

  PyObject *py_request_capsule = PyCapsule_New( request, NULL, NULL );
  if( py_request_capsule == NULL ) {
    return NULL;
  }

  PyObject *args[] = {
    NULL,
    Py_None,
    py_params  ? py_params  : Py_None,
    py_headers ? py_headers : Py_None,
    py_content ? py_content : Py_None,
    Py_None,
    py_request_capsule
  };

  XTRY {

    if( (py_plugreq = (PyVGX_PluginRequest*)PyObject_Vectorcall( (PyObject*)p_PyVGX_PluginRequestType, args+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }


  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
  }
  XFINALLY {
    Py_XDECREF( py_request_capsule );
  }

  return py_plugreq;
}



/******************************************************************************
 * PyVGX_PluginRequest_Serialize 
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SerializeRequest__doc__,
  "Serialize() -> None\n"
);
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginRequest_Serialize
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_Serialize( PyVGX_PluginRequest *py_plugreq ) {
  PyObject *py_bytes = NULL;
  vgx_StreamBuffer_t *output = NULL;
  XTRY {
    if( (output = iStreamBuffer.New( 8 )) == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( __pyvgx_PluginRequest_Serialize( py_plugreq, output ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    const char *data = NULL;
    int64_t sz_data = iStreamBuffer.ReadableSegment( output, LLONG_MAX, &data, NULL );
    if( sz_data <= 0 ) {
      PyErr_SetNone( PyExc_MemoryError );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    py_bytes = PyBytes_FromStringAndSize( data, sz_data );

  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    iStreamBuffer.Delete( &output );
  }
  return py_bytes;
}



/******************************************************************************
 * PyVGX_PluginRequest_get
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_get( PyVGX_PluginRequest *py_plugreq, PyObject *args ) {
  PyObject *py_key = NULL;
  PyObject *py_default = Py_None;
  if( !PyArg_ParseTuple( args, "O|O", &py_key, &py_default ) ) {
    return NULL;
  }
  PyObject *py_item = PyDict_GetItem( py_plugreq->py_params, py_key );
  if( py_item == NULL ) {
    py_item = py_default;
  }
  __delete_signature( py_plugreq ); // since caller may modify the object
  Py_INCREF( py_item );
  return py_item;
}



/******************************************************************************
 * PyVGX_PluginRequest_items
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_items( PyVGX_PluginRequest *py_plugreq ) {
  __delete_signature( py_plugreq ); // since caller may modify the object
  return PyDict_Items( py_plugreq->py_params );
}



/******************************************************************************
 * PyVGX_PluginRequest_keys
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_keys( PyVGX_PluginRequest *py_plugreq ) {
  return PyDict_Keys( py_plugreq->py_params );
}



/******************************************************************************
 * PyVGX_PluginRequest_values
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_values( PyVGX_PluginRequest *py_plugreq ) {
  __delete_signature( py_plugreq ); // since caller may modify the object
  return PyDict_Values( py_plugreq->py_params );
}



/******************************************************************************
 * PyVGX_PluginRequest_len
 *
 ******************************************************************************
 */
static Py_ssize_t PyVGX_PluginRequest_len( PyVGX_PluginRequest *py_plugreq ) {
  return PyDict_Size( py_plugreq->py_params );
}



/******************************************************************************
 * PyVGX_PluginRequest_get_item
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_get_item( PyVGX_PluginRequest *py_plugreq, PyObject *py_key ) {
  PyObject *py_item = PyDict_GetItem( py_plugreq->py_params, py_key );
  if( py_item ) {
    __delete_signature( py_plugreq ); // since caller may modify the object
    Py_INCREF( py_item );
    return py_item;
  }
  PyErr_SetObject( PyExc_KeyError, py_key );
  return NULL;
}



/******************************************************************************
 * PyVGX_PluginRequest_set_item
 *
 ******************************************************************************
 */
static int PyVGX_PluginRequest_set_item( PyVGX_PluginRequest *py_plugreq, PyObject *py_key, PyObject *py_value ) {
  __delete_signature( py_plugreq );
  return PyDict_SetItem( py_plugreq->py_params, py_key, py_value );
}



/******************************************************************************
 * PyVGX_PluginRequest_repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginRequest_repr( PyVGX_PluginRequest *py_plugreq ) {
  vgx_VGXServerRequest_t *r = py_plugreq->request;
  const char *path = r->path;
  int partial = r->target_partial;
  int64_t sz_content = r->headers->content_length;
  return PyUnicode_FromFormat( "<pyvgx.PluginRequest path=%s partial=%d content-length=%lld>", path, partial, sz_content );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int64_t urlencode( vgx_StreamBuffer_t *output, const char *str, int64_t sz_str ) {
#define buffer_size 1023
  char buffer[buffer_size+1];

  int64_t required = requires_percent_encoding( str, sz_str );
  if( required == 0 ) {
    return iStreamBuffer.Write( output, str, sz_str );
  }

  char *encoding_buffer;
  if( required < buffer_size ) {
    encoding_buffer = buffer;
  }
  else {
    // we need to malloc
    if( (encoding_buffer = malloc( required + 1 )) == NULL ) {
      return -1;
    }
  }

  int64_t sz_enc = encode_percent_plus( str, sz_str, encoding_buffer );
  int64_t ret = iStreamBuffer.Write( output, encoding_buffer, sz_enc );
  if( encoding_buffer != buffer ) {
    free( encoding_buffer );
  }

  return ret;
}



/******************************************************************************
 * __pyvgx_PluginRequest__Serialize
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_PluginRequest_Serialize( PyVGX_PluginRequest *py_plugreq, vgx_StreamBuffer_t *output ) {
#define buffer_size 1023
  char buffer[buffer_size+1];

  Py_ssize_t sz_data;
  const char *data;
  const char *p;


  // method
  data = __vgx_http_request_method( py_plugreq->request->method );
  iStreamBuffer.Write( output, data, strlen(data) );
  iStreamBuffer.Write( output, " ", 1 );


  // path
  // stop render at '?' if encountered
  data = py_plugreq->request->path;
  p = data;
  sz_data = 0;
  while( *p && *p++ != '?' ) {
    ++sz_data;
  }
  iStreamBuffer.Write( output, data, sz_data );


  // params
  if( PyDict_Size(py_plugreq->py_params) > 0 ) {
    iStreamBuffer.Write( output, "?", 1 );
    PyObject *py_key, *py_value;
    const char *key, *value;
    Py_ssize_t sz_key, sz_value;
    Py_ssize_t pos = 0;
    int n = 0;
    while( PyDict_Next( py_plugreq->py_params, &pos, &py_key, &py_value ) ) {
      
      if( (key = PyUnicode_AsUTF8AndSize( py_key, &sz_key )) == NULL ) {
        if( PyBytes_AsStringAndSize( py_key, (char**)&key, &sz_key ) < 0 ) {
          goto error;
        }
      }
      if( n++ > 0 ) {
        iStreamBuffer.Write( output, "&", 1 );
      }

      if( urlencode( output, key, sz_key ) < 0 ) {
        goto error;
      }

      if( py_value == Py_None ) {
        continue;
      }

      if( PyLong_Check( py_value ) ) {
        sz_value = snprintf( buffer, 32, "=(q)%lld", PyLong_AsLongLong( py_value ) );
        iStreamBuffer.Write( output, buffer, sz_value );
      }
      else if( PyFloat_Check( py_value ) ) {
        sz_value = snprintf( buffer, 32, "=(d)%g", PyFloat_AsDouble( py_value ) );
        iStreamBuffer.Write( output, buffer, sz_value );
      }
      else {
        if( PyUnicode_CheckExact( py_value ) ) {
          if( (value = PyUnicode_AsUTF8AndSize( py_value, &sz_value )) == NULL ) {
            goto error;
          }
        }
        else if( PyBytes_CheckExact( py_value ) ) {
          if( PyBytes_AsStringAndSize( py_value, (char**)&value, &sz_value ) < 0 ) {
            goto error;
          }
        }
        else {
          PyErr_Format( PyVGX_RequestError, "unsupported parameter type: %s", Py_TYPE(py_value)->tp_name );
          goto error;
        }

        iStreamBuffer.Write( output, "=", 1 );

        if( urlencode( output, value, sz_value ) < 0 ) {
          PyErr_SetNone( PyExc_MemoryError );
          goto error;
        }

      }
    }
  }


  // end request line
  iStreamBuffer.Write( output, " HTTP/1.1\r\n", 11 );


  // headers
  if( py_plugreq->py_headers ) {
    if( PyDict_CheckExact( py_plugreq->py_headers ) ) {
      if( PyDict_Size(py_plugreq->py_headers) > 0 ) {
        PyObject *py_key, *py_value;
        const char *key, *value;
        Py_ssize_t sz_key, sz_value;
        Py_ssize_t pos = 0;
        while( PyDict_Next( py_plugreq->py_headers, &pos, &py_key, &py_value ) ) {
          
          if( (key = PyUnicode_AsUTF8AndSize( py_key, &sz_key )) == NULL ) {
            PyBytes_AsStringAndSize( py_key, (char**)&key, &sz_key );
          }
          
          if( (value = PyUnicode_AsUTF8AndSize( py_value, &sz_value )) == NULL ) {
            if( PyBytes_AsStringAndSize( py_value, (char**)&value, &sz_value ) < 0 ) {
              PyErr_SetString( PyVGX_RequestError, "header field value must be string" );
              goto error;
            }
          }

          if( !key || !value ) {
            goto error;
          }
          if( sz_value > buffer_size ) {
            PyErr_SetString( PyVGX_RequestError, "header field too large" );
            goto error;
          }

          iStreamBuffer.Write( output, key, sz_key );
          iStreamBuffer.Write( output, ": ", 2 );
          iStreamBuffer.Write( output, value, sz_value );
          iStreamBuffer.Write( output, "\r\n", 2 );
        }
      }
    }
    else if( PyBytes_CheckExact( py_plugreq->py_headers ) ) {
      PyBytes_AsStringAndSize( py_plugreq->py_headers, (char**)&data, &sz_data );
      iStreamBuffer.Write( output, data, sz_data );
      sz_data = iStreamBuffer.ReadableSegment( output, LLONG_MAX, &data, NULL );
      if( sz_data >= 4 ) {
        if( !memcmp( data + (sz_data-4), "\r\n\r\n", 4 ) ) {
          goto content_segment;
        }
      }
    }
  }

  // end headers
  iStreamBuffer.Write( output, "\r\n", 2 );

content_segment:

  if( py_plugreq->request->content_type != MEDIA_TYPE__NONE ) {
    if( py_plugreq->py_content ) {
      if( PyBytes_AsStringAndSize( py_plugreq->py_content, (char**)&data, &sz_data ) < 0 ) {
        goto error;
      }
    }
    else {
      sz_data = iStreamBuffer.ReadableSegment( py_plugreq->request->buffers.content, LLONG_MAX, &data, NULL );
    }
    iStreamBuffer.Write( output, data, sz_data );
  }

  return 0;

error:
  if( !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "unknown error" );
  }
  iStreamBuffer.Clear( output );
  return -1;
}



/******************************************************************************
 * PyVGX_PluginRequest_members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_PluginRequest_members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginRequest_getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_PluginRequest_getset[] = {
  {"path",                    (getter)PyVGX_PluginRequest_get_path,                (setter)0,                                 "path",        NULL },
  {"method",                  (getter)PyVGX_PluginRequest_get_method,              (setter)0,                                 "method",      NULL },
  {"params",                  (getter)PyVGX_PluginRequest_get_params,              (setter)0,                                 "params",      NULL },
  {"headers",                 (getter)PyVGX_PluginRequest_get_headers,             (setter)0,                                 "headers",     NULL },
  {"t0",                      (getter)PyVGX_PluginRequest_get_t0,                  (setter)0,                                 "t0",          NULL },
  {"sn",                      (getter)PyVGX_PluginRequest_get_sn,                  (setter)0,                                 "sn",          NULL },
  {"signature",               (getter)PyVGX_PluginRequest_get_signature,           (setter)PyVGX_PluginRequest_set_signature, "signature",   NULL },
  {"executor",                (getter)PyVGX_PluginRequest_get_executor,            (setter)0,                                 "executor",    NULL },
  {"flag",                    (getter)PyVGX_PluginRequest_get_flag,                (setter)PyVGX_PluginRequest_set_flag,      "flag",        NULL },
  {"local",                   (getter)PyVGX_PluginRequest_get_local,               (setter)PyVGX_PluginRequest_set_local,     "local",       NULL },
  {"content",                 (getter)PyVGX_PluginRequest_get_content,             (setter)PyVGX_PluginRequest_set_content,   "content",     NULL },
  {"partial",                 (getter)PyVGX_PluginRequest_get_partial,             (setter)PyVGX_PluginRequest_set_partial,   "partial",     NULL },
  {"affinity",                (getter)PyVGX_PluginRequest_get_affinity,            (setter)PyVGX_PluginRequest_set_affinity,  "affinity",    NULL },
  {"primary",                 (getter)PyVGX_PluginRequest_get_primary,             (setter)PyVGX_PluginRequest_set_primary,   "primary",     NULL },
  {"ident",                   (getter)PyVGX_PluginRequest_get_ident,               (setter)0,                                 "ident",       NULL },
  {"partition",               (getter)PyVGX_PluginRequest_get_partition,           (setter)0,                                 "partition",   NULL },
  {"width",                   (getter)PyVGX_PluginRequest_get_width,               (setter)0,                                 "width",       NULL },
  {"replica",                 (getter)PyVGX_PluginRequest_get_replica,             (setter)0,                                 "replica",     NULL },
  {"height",                  (getter)PyVGX_PluginRequest_get_height,              (setter)0,                                 "height",      NULL },
  {"channel",                 (getter)PyVGX_PluginRequest_get_channel,             (setter)0,                                 "channel",     NULL },
  {"depth",                   (getter)PyVGX_PluginRequest_get_depth,               (setter)0,                                 "depth",       NULL },
  {"toplevel",                (getter)PyVGX_PluginRequest_get_toplevel,            (setter)0,                                 "toplevel",    NULL },
  {"hasmatrix",               (getter)PyVGX_PluginRequest_get_hasmatrix,           (setter)0,                                 "hasmatrix",   NULL },
  {"port",                    (getter)PyVGX_PluginRequest_get_port,                (setter)0,                                 "port",        NULL },
  {"baseport",                (getter)PyVGX_PluginRequest_get_baseport,            (setter)0,                                 "baseport",    NULL },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginRequest_methods
 *
 ******************************************************************************
 */
static PyMethodDef PyVGX_PluginRequest_methods[] = {
  {"Serialize",        (PyCFunction)PyVGX_PluginRequest_Serialize,          METH_NOARGS,                    SerializeRequest__doc__ },
  {"get",              (PyCFunction)PyVGX_PluginRequest_get,                METH_VARARGS,                   "like dict.get()" },
  {"items",            (PyCFunction)PyVGX_PluginRequest_items,              METH_NOARGS,                    "like dict.items()" },
  {"keys",             (PyCFunction)PyVGX_PluginRequest_keys,               METH_NOARGS,                    "like dict.keys()" },
  {"values",           (PyCFunction)PyVGX_PluginRequest_values,             METH_NOARGS,                    "like dict.values()" },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginRequest_as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_PluginRequest_as_mapping = {
    .mp_length          = (lenfunc)PyVGX_PluginRequest_len,
    .mp_subscript       = (binaryfunc)PyVGX_PluginRequest_get_item,
    .mp_ass_subscript   = (objobjargproc)PyVGX_PluginRequest_set_item
};



/******************************************************************************
 * PyVGX_PluginRequestType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( PluginRequest__doc__,
  "PluginRequest( method=\"GET\" ) -> info string\n"
  "\n"
  "\n"
);
static PyTypeObject PyVGX_PluginRequestType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.PluginRequest",
    .tp_basicsize       = sizeof(PyVGX_PluginRequest),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_PluginRequest__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_PluginRequest_repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = 0,
    .tp_as_mapping      = &PyVGX_PluginRequest_as_mapping,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX PluginRequest objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_PluginRequest_methods,
    .tp_members         = PyVGX_PluginRequest_members,
    .tp_getset          = PyVGX_PluginRequest_getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_PluginRequest__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_PluginRequest__new,
    .tp_free            = (freefunc)0,
    .tp_is_gc           = (inquiry)0,
    .tp_bases           = NULL,
    .tp_mro             = NULL,
    .tp_cache           = NULL,
    .tp_subclasses      = NULL,
    .tp_weaklist        = NULL,
    .tp_del             = (destructor)0,
    .tp_version_tag     = 0,
    .tp_finalize        = (destructor)0,
    .tp_vectorcall      = PyVGX_PluginRequest__vectorcall
};


DLL_HIDDEN PyTypeObject * p_PyVGX_PluginRequestType = &PyVGX_PluginRequestType;





/******************************************************************************
 *
 * ====================
 * PyVGX_PluginResponse
 * ====================
 *
 ******************************************************************************
 */

static int PyVGX_PluginResponse_set_message( PyVGX_PluginResponse *py_plugres, PyObject *py_message, void *closure );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_xvgxpartial__init( void ) {
#define NewUnicodeOrError( Var, Value ) \
  if( Var == NULL ) { \
    if( (Var = PyUnicode_FromString( Value )) == NULL ) { return -1; } \
  }

  NewUnicodeOrError( g_py_key_partials, "partials" )
  NewUnicodeOrError( g_py_key_levelparts, "levelparts" )
  NewUnicodeOrError( g_py_key_level, "level" )
  NewUnicodeOrError( g_py_key_hitcount, "hitcount" )
  NewUnicodeOrError( g_py_key_aggregator, "aggregator" )
  NewUnicodeOrError( g_py_key_message, "message" )
  NewUnicodeOrError( g_py_key_entries, "entries" )
  NewUnicodeOrError( g_py_content_length, "content-length" )
  NewUnicodeOrError( g_py_content_type, "content-type" )
  NewUnicodeOrError( g_py_application_octet_stream, "application/octet-stream" )
  NewUnicodeOrError( g_py_application_json, "application/json" )
  NewUnicodeOrError( g_py_text_plain, "text/plain" )
  NewUnicodeOrError( g_py_x_vgx_partial_target, "x-vgx-partial-target" )
  return 0;
}




/******************************************************************************
 *
 *
 ******************************************************************************
 */
static x_vgx_partial__header __get_header_from_object( const PyVGX_PluginResponse *py_plugres ) {
  const PyVGX_PluginResponse_metas *metas = &py_plugres->metas;
  x_vgx_partial__header header = {0};
  header.status = X_VGX_PARTIAL_STATUS__OK;
  header.maxhits = metas->maxhits;
  header.ktype = metas->ktype;
  header.sortspec = metas->sortspec;
  header.n_entries = PyList_GET_SIZE( py_plugres->py_entries );
  header.hitcount = metas->hitcount;
  header.level.deep_parts = metas->level.deep_parts;
  header.level.parts = metas->level.parts;
  header.level.number = metas->level.number;
  header.aggregator = py_plugres->aggregator;
  return header;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyVGX_PluginResponse * __new_deserialized_pluginresponse( PyObject *py_message, PyObject *py_entries, x_vgx_partial__header *header ) {

  PyVGX_PluginResponse *py_plugres = NULL;

  XTRY {

    PyObject *args[] = { NULL };
    if( (py_plugres = (PyVGX_PluginResponse*)PyObject_Vectorcall( (PyObject*)p_PyVGX_PluginResponseType, args+1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( py_message ) {
      Py_INCREF( py_message );
      py_plugres->py_message = py_message;
    }

    if( py_entries ) {
      Py_XDECREF( py_plugres->py_entries );
      Py_INCREF( py_entries );
      py_plugres->py_entries = py_entries; 
    } 

    py_plugres->metas.level.parts = header->level.parts;
    py_plugres->metas.level.deep_parts = header->level.deep_parts;
    py_plugres->metas.level.number = header->level.number;
    py_plugres->metas.maxhits = header->maxhits;
    py_plugres->metas.hitcount = header->hitcount;
    py_plugres->metas.sortspec = header->sortspec;
    py_plugres->metas.ktype = header->ktype;

    if( py_plugres->metas.ktype == X_VGX_PARTIAL_SORTKEYTYPE__double ) {
      py_plugres->metas.py_keytype = &PyFloat_Type;
    }
    else if( py_plugres->metas.ktype == X_VGX_PARTIAL_SORTKEYTYPE__int64 ) {
      py_plugres->metas.py_keytype = &PyLong_Type;
    }
    else if( py_plugres->metas.ktype == X_VGX_PARTIAL_SORTKEYTYPE__bytes ) {
      py_plugres->metas.py_keytype = &PyBytes_Type;
    }
    else if( py_plugres->metas.ktype == X_VGX_PARTIAL_SORTKEYTYPE__unicode ) {
      py_plugres->metas.py_keytype = &PyUnicode_Type;
    }
    else {
      py_plugres->metas.py_keytype = NULL;
    }


    py_plugres->aggregator = header->aggregator;

  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
  }
  XFINALLY {
  }

  return py_plugres;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __serialize_partial_error( PyObject *py_err, vgx_StreamBuffer_t *output ) {
  int ret = 0;
  x_vgx_partial__header header = {0};
  header.status = X_VGX_PARTIAL_STATUS__ERROR;

  const char *message = NULL;
  Py_ssize_t sz_message = 0;
  PyObject *py_repr = PyObject_Repr( py_err );
  if( py_repr ) {
    message = PyUnicode_AsUTF8AndSize( py_repr, &sz_message );
  }
  if( message == NULL ) {
    message = "unknown internal error";
    sz_message = strlen( message );
  }

  header.segment.message = sizeof( x_vgx_partial__header );
  header.segment.keys = header.segment.message + sizeof(int) + sz_message;
  header.segment.strings = header.segment.keys;
  header.segment.items = header.segment.strings;
  header.segment.end = header.segment.items;

  if( vgx_server_dispatcher_partial__write_output_binary( &header, message, (int)sz_message, NULL, output ) < 0 ) {
    ret = -1;
  }

  Py_XDECREF( py_repr );

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __serialize_partial( PyVGX_PluginResponse *py_plugres, vgx_StreamBuffer_t *output ) {
  int ret = 0;

#define UpdateRunningOffset( OffsetCounter, BinEntry ) (OffsetCounter) += sizeof( (BinEntry).sz.val ) + (BinEntry).sz.val

  x_vgx_partial__entry *entries = NULL;
  x_vgx_partial__header header = __get_header_from_object( py_plugres );
  if( (entries = calloc( header.n_entries, sizeof(x_vgx_partial__entry) )) == NULL ) {
    return -1;
  }
  const x_vgx_partial__entry *end = entries + header.n_entries;
  x_vgx_partial__entry *entry = NULL;
  int64_t running_offset = 0;
  const char *message = "";
  Py_ssize_t sz_message = 0;

  PyObject *py_message_bytes = NULL;

  XTRY {
    // Message
    if( py_plugres->py_message ) {
      if( PyUnicode_CheckExact( py_plugres->py_message ) ) {
        header.message_type = X_VGX_PARTIAL_MESSAGE__UTF8;
        if( (message = PyUnicode_AsUTF8AndSize( py_plugres->py_message, &sz_message )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }
      }
      else {
        header.message_type = X_VGX_PARTIAL_MESSAGE__OBJECT;
        if( (py_message_bytes = PyMarshal_WriteObjectToString( py_plugres->py_message, Py_MARSHAL_VERSION )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_API, 0x002 );
        }
        if( PyBytes_AsStringAndSize( py_message_bytes, (char**)&message, &sz_message ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
      }
    }

    // Header: Set offsets for message, keys and strings
    header.segment.message = sizeof( x_vgx_partial__header );
    header.segment.keys = header.segment.message + sizeof(int) + sz_message;
    header.segment.strings = header.segment.keys + header.n_entries * sizeof( x_vgx_partial__entry_key );
    running_offset = header.segment.strings;

    // Serialize and compute string offsets
    PyObject *py_entries = py_plugres->py_entries;
    for( entry=entries; entry < end; ++entry ) {
      // Get entry
      int64_t idx = entry - entries;
      PyObject *py_entry = PyList_GET_ITEM( py_entries, idx );
      // Ensure entry is tuple (key,item)
      if( !PyTuple_Check( py_entry ) || PyTuple_GET_SIZE( py_entry ) != 2 ) {
        PyErr_Format( PyVGX_ResponseError, "Invalid response entry at index %d, a tuple (sortkey, item) is required", idx );
        THROW_SILENT( CXLIB_ERR_API, 0x004 );
      }
      PyObject *py_sortkey = PyTuple_GET_ITEM( py_entry, 0 );
      PyObject *py_item = PyTuple_GET_ITEM( py_entry, 1 );

      // Extract sortkey
      Py_ssize_t sz;
      switch( header.ktype ) {
      case X_VGX_PARTIAL_SORTKEYTYPE__double:
        if( (entry->key.sortkey.dval = PyFloat_AsDouble( py_sortkey )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }
        break;
      case X_VGX_PARTIAL_SORTKEYTYPE__int64:
        if( (entry->key.sortkey.ival = PyLong_AsLongLong( py_sortkey )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }
        break;
      case X_VGX_PARTIAL_SORTKEYTYPE__bytes:
        PyBytes_AsStringAndSize( py_sortkey, (char**)&entry->sortkey.data, &sz );
        goto record_string_offset;
      case X_VGX_PARTIAL_SORTKEYTYPE__unicode:
        entry->sortkey.data = PyUnicode_AsUTF8AndSize( py_sortkey, &sz );
record_string_offset:
        if( entry->sortkey.data == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
        }
        entry->key.sortkey.offset = running_offset;
        entry->sortkey.sz.val = (int)sz;
        UpdateRunningOffset( running_offset, entry->sortkey );
        break;
      default:
        break;
      }

      // Serialize item
      if( (entry->obj = PyMarshal_WriteObjectToString( py_item, Py_MARSHAL_VERSION )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x008 );
      }
      entry->item.data = PyBytes_AS_STRING( _PyObject_CAST(entry->obj) );
      entry->item.sz.val = (int)PyBytes_GET_SIZE( _PyObject_CAST(entry->obj) );
    }

    int64_t nw;
    BEGIN_PYVGX_THREADS {
      // Header: we now know the end of strings, set the items offset
      header.segment.items = running_offset;

      // Compute item offsets
      for( entry=entries; entry < end; ++entry ) {
        // Populate item offset
        entry->key.item.offset = running_offset;
        UpdateRunningOffset( running_offset, entry->item );
      }

      // Header: we now know the end of data, set the end offset
      header.segment.end = running_offset;

      // OUTPUT
      nw = vgx_server_dispatcher_partial__write_output_binary( &header, message, (int)sz_message, entries, output );
    } END_PYVGX_THREADS;

    if( nw < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
    ret = -1;
  }
  XFINALLY {
    Py_XDECREF( py_message_bytes );
    if( entries ) {
      for( entry=entries; entry < end; ++entry ) {
        Py_XDECREF( _PyObject_CAST(entry->obj) );
      }
      free( entries );
    }
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyVGX_PluginResponse * __deserialize_partial( const char *buffer, int64_t sz_buffer ) {
  static int64_t KSZ = sizeof( x_vgx_partial__entry_key );
  static int64_t HSZ = sizeof( x_vgx_partial__header );

  PyVGX_PluginResponse *py_plugres = NULL;

  typedef struct __s_segment {
    int64_t sz;
    const char *data;
  } __segment;

#define SetEntryOrThrow( List, Index, Sortkey, Item, Errcode ) do {   \
  PyObject *py_entry = PyTuple_New(2);            \
  if( py_entry && (Sortkey) && (Item) ) {         \
    PyTuple_SET_ITEM( py_entry, 0, (Sortkey) );   \
    PyTuple_SET_ITEM( py_entry, 1, (Item) );      \
    PyList_SET_ITEM( (List), (Index), py_entry ); \
  }                                               \
  else {                                          \
    Py_XDECREF( py_entry );                       \
    Py_XDECREF( (Sortkey) );                      \
    Py_XDECREF( (Item) );                         \
    THROW_ERROR( CXLIB_ERR_MEMORY, (Errcode) );   \
  }                                               \
} WHILE_ZERO

  x_vgx_partial__header header;
  if( vgx_server_dispatcher_partial__deserialize_header( buffer, sz_buffer, &header ) < 0 ) {
    PyErr_SetString( PyVGX_ResponseError, "invalid partial data" );
    return NULL;
  }

  // Set up segments
  struct {
    __segment header;
    __segment message;
    __segment keys;
    __segment strings;
    __segment items;
    __segment end;
  } segment = {
      .header  = { .sz = header.segment.message,                          .data = buffer },
      .message = { .sz = header.segment.keys    - header.segment.message, .data = buffer + header.segment.message },
      .keys    = { .sz = header.segment.strings - header.segment.keys,    .data = buffer + header.segment.keys },
      .strings = { .sz = header.segment.items   - header.segment.strings, .data = buffer + header.segment.strings },
      .items   = { .sz = header.segment.end     - header.segment.items,   .data = buffer + header.segment.items },
      .end     = { .sz = 0,                                               .data = buffer + header.segment.end }
  };

  PyObject *py_message = NULL;
  PyObject *py_entries = NULL;

  XTRY {

    // Allocate entries
    if( (py_entries = PyList_New( header.n_entries )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Deserialize entries
    const char *p;
    x_vgx_partial__binentry message;
    x_vgx_partial__binentry sortkey;
    x_vgx_partial__binentry item;

    p = segment.message.data;
    memcpy( message.sz.bytes, p, sizeof( message.sz.bytes ) );
    if( message.sz.val > 0 ) {
      message.data = p + sizeof( message.sz.bytes );
      if( message.data + message.sz.val > segment.keys.data ) {
        THROW_ERROR( CXLIB_ERR_FORMAT, 0x002 );
      }
      switch( header.message_type ) {
      case X_VGX_PARTIAL_MESSAGE__UTF8:
        if( (py_message = PyUnicode_FromStringAndSize( message.data, message.sz.val )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
        break;
      case X_VGX_PARTIAL_MESSAGE__OBJECT:
        if( (py_message = PyMarshal_ReadObjectFromString( message.data, message.sz.val )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }
        break;
      default:
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
    }

    int64_t i = 0;
    const char *k = segment.keys.data;
    const char *end = segment.keys.data + segment.keys.sz;
    for( ; k < end; k += KSZ, ++i ) {
      // Entry key
      x_vgx_partial__entry_key key;
      memcpy( &key.m128i, k, KSZ );

      // Sortkey
      PyObject *py_sortkey = NULL;
      switch( header.ktype ) {
      case X_VGX_PARTIAL_SORTKEYTYPE__double:
        py_sortkey = PyFloat_FromDouble( key.sortkey.dval );
        break;
      case X_VGX_PARTIAL_SORTKEYTYPE__int64:
        py_sortkey = PyLong_FromLongLong( key.sortkey.ival );
        break;
      case X_VGX_PARTIAL_SORTKEYTYPE__bytes:
      case X_VGX_PARTIAL_SORTKEYTYPE__unicode:
        // Length of string data
        p = buffer + key.sortkey.offset;
        memcpy( sortkey.sz.bytes, p, sizeof( sortkey.sz.bytes ) );
        // String data
        sortkey.data = p + sizeof( sortkey.sz.bytes );
        if( sortkey.data + sortkey.sz.val > segment.items.data ) {
          THROW_ERROR( CXLIB_ERR_FORMAT, 0x004 );
        } 
        if( header.ktype == X_VGX_PARTIAL_SORTKEYTYPE__bytes ) {
          py_sortkey = PyBytes_FromStringAndSize( sortkey.data, sortkey.sz.val );
        }
        else {
          py_sortkey = PyUnicode_FromStringAndSize( sortkey.data, sortkey.sz.val );
        }
        break;
      default:
        Py_INCREF( Py_None );
        py_sortkey = Py_None;
      }

      // Length of item data
      p = buffer + key.item.offset;
      memcpy( item.sz.bytes, p, sizeof( item.sz.bytes ) );

      // Item data
      item.data = p + sizeof( item.sz.bytes );
      if( item.data + item.sz.val > segment.end.data ) {
        THROW_ERROR( CXLIB_ERR_FORMAT, 0x005 );
      }
      PyObject *py_item = PyMarshal_ReadObjectFromString( item.data, item.sz.val );

      SetEntryOrThrow( py_entries, i, py_sortkey, py_item, 0x006 );
    }

    if( (py_plugres = __new_deserialized_pluginresponse( py_message, py_entries, &header )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
  }
  XFINALLY {
    Py_XDECREF( py_message );
    Py_XDECREF( py_entries );
  }

  return py_plugres;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_PluginResponse_serialize_x_vgx_partial( PyVGX_PluginResponse *py_plugres, vgx_StreamBuffer_t *output ) {
  int ret = 0;
  if( PyVGX_PluginResponse_CheckExact( py_plugres ) ) {
    return __serialize_partial( py_plugres, output );
  }

  PyObject *py_msg = PyObject_Repr( (PyObject*)py_plugres );
  const char *msg = NULL;
  Py_ssize_t sz_msg = 0;
  if( py_msg ) {
    msg = PyUnicode_AsUTF8AndSize( py_msg, &sz_msg );
  }
  if( msg == NULL ) {
    msg = "unknown internal error";
    sz_msg = strlen( msg );
  }

  if( vgx_server_dispatcher_partial__serialize_partial_error( msg, sz_msg, output ) < 0 ) {
    ret = -1;
  }

  Py_XDECREF( py_msg );

  return ret;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN PyVGX_PluginResponse * __pyvgx_PluginResponse_deserialize_x_vgx_partial( vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response ) {
  const char *binary_data;
  int64_t sz_binary_data = iStreamBuffer.ReadableSegment( response->buffers.content, response->content_length, &binary_data, NULL );
  PyVGX_PluginResponse *py_plugres = __deserialize_partial( binary_data, sz_binary_data );
  if( py_plugres ) {
    iStreamBuffer.AdvanceRead( response->buffers.content, sz_binary_data );
    py_plugres->request = request;
    py_plugres->response = response;
  }
  return py_plugres;
}




/******************************************************************************
 * maxhits
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_maxhits( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLongLong( py_plugres->metas.maxhits );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_maxhits
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_maxhits( PyVGX_PluginResponse *py_plugres, PyObject *py_maxhits, void *closure ) {
  if( py_maxhits == NULL ) {
    py_plugres->metas.maxhits = -1;
  }
  else if( (py_plugres->metas.maxhits = PyLong_AsLong( py_maxhits )) < 0 ) {
    if( PyErr_Occurred() ) {
      return -1;
    }
  }
  return 0;
}



/******************************************************************************
 * sortby
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_sortby( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLongLong( py_plugres->metas.sortspec );
}



/******************************************************************************
 * keytype
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_keytype( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->metas.py_keytype == NULL ) {
    Py_RETURN_NONE;
  }
  return (PyObject*)py_plugres->metas.py_keytype;
}



/******************************************************************************
 * message
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_message( PyVGX_PluginResponse *py_plugres, void *closure ) {
  PyObject *py_ret;
  if( py_plugres->py_message ) {
    py_ret = py_plugres->py_message;
  }
  else {
    py_ret = Py_None;
  }
  Py_INCREF( py_ret );
  return py_ret;
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_message
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_message( PyVGX_PluginResponse *py_plugres, PyObject *py_message, void *closure ) {
  Py_XDECREF( py_plugres->py_message );
  if( (py_plugres->py_message = py_message) != NULL ) {
    Py_INCREF( py_plugres->py_message );
  }
  return 0;
}



/******************************************************************************
 * hitcount
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_hitcount( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLongLong( py_plugres->metas.hitcount );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_hitcount
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_hitcount( PyVGX_PluginResponse *py_plugres, PyObject *py_hitcount, void *closure ) {
  int64_t sz = PyList_Size( py_plugres->py_entries );
  // on del, set hitcount to len(entries)
  if( py_hitcount == NULL ) {
    py_plugres->metas.hitcount = sz;
    return 0;
  }
  int64_t h = PyLong_AsLongLong( py_hitcount );
  // error
  if( h < 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_ValueError, "hitcount cannot be negative" );
    }
    return -1;
  }
  // ok
  py_plugres->metas.hitcount = h;
  return 0;
}



/******************************************************************************
 * partials
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_partials( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLong( (int)py_plugres->metas.level.deep_parts );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_partials
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_partials( PyVGX_PluginResponse *py_plugres, PyObject *py_partials, void *closure ) {
  if( py_partials == NULL ) {
    py_plugres->metas.level.deep_parts = 1;
    return 0;
  }
  if( (py_plugres->metas.level.deep_parts = (int)PyLong_AsLong( py_partials )) > 0 ) {
    return 0;
  }
  if( !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_ValueError, "partials must be a positive integer" );
  }
  return -1;
}



/******************************************************************************
 * requestpath
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_requestpath( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }

  return PyUnicode_FromStringAndSize( py_plugres->request->path, py_plugres->request->sz_path );
}



/******************************************************************************
 * requestmethod
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_requestmethod( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  const char *method = __vgx_http_request_method( py_plugres->request->method );
  return PyUnicode_FromString( method );
}



/******************************************************************************
 * t0
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_t0( PyVGX_PluginResponse *py_plugres, void *closure ) {
  int64_t t0 = -1;
  if( py_plugres->request != NULL ) {
    vgx_VGXServerClient_t *client = py_plugres->request->headers->client;
    if( client != NULL ) {
      t0 = client->tbase_ns + client->io_t0_ns;
    }
  }
  return PyLong_FromLongLong( t0 );
}



/******************************************************************************
 * texec
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_texec( PyVGX_PluginResponse *py_plugres, void *closure ) {
  double exec_sec = py_plugres->response->exec_ns / 1e9;
  return PyFloat_FromDouble( exec_sec );
}



/******************************************************************************
 * sn
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_sn( PyVGX_PluginResponse *py_plugres, void *closure ) {
  int64_t sn = -1;
  if( py_plugres->request != NULL ) {
    sn = py_plugres->request->headers->sn;
  }
  return PyLong_FromLongLong( sn );
}



/******************************************************************************
 * signature
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_signature( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }

  objectid_t *s = &py_plugres->request->headers->signature;
  if( idnone( s ) ) {
    Py_RETURN_NONE;
  }

  char buf[33];
  return PyUnicode_FromStringAndSize( idtostr(buf, s), 32 );
}



/******************************************************************************
 * executor
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_executor( PyVGX_PluginResponse *py_plugres, void *closure ) {
  int exec_id = py_plugres->request != NULL ? py_plugres->request->executor_id : -1;
  return PyLong_FromLongLong( exec_id );
}



/******************************************************************************
 * flag
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_flag( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  vgx_HTTPHeaders_t *h = py_plugres->request->headers;
  if( !h->flag.__bits ) {
    Py_RETURN_NONE;
  }
  size_t sz = strnlen( h->flag.value, sizeof( h->flag.value ) );
  return PyUnicode_FromStringAndSize( h->flag.value, sz );
}



/******************************************************************************
 * local
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject* PyVGX_PluginResponse_get_local(PyVGX_PluginResponse* py_plugres, void* closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return local_capsule_get( py_plugres->request->headers );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_local
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_local( PyVGX_PluginResponse *py_plugres, PyObject *py_obj, void *closure ) {
  if( py_plugres->request == NULL ) {
    PyErr_SetString( PyVGX_ResponseError, "invalid context (no request)" );
    return -1;
  }
  return local_capsule_set( py_plugres->request->headers, py_obj );
}



/******************************************************************************
 * resubmits
 * Returns: number of times request was resubmitted to backend matrix
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_resubmits( PyVGX_PluginResponse *py_plugres, void *closure ) {
  int n = 0;
  if( py_plugres->request ) {
    n = py_plugres->request->headers->nresubmit;
  }
  return PyLong_FromLong( n );
}



/******************************************************************************
 * ident
 * Returns: ( defined, width, height, depth, partition, replica, channel, primary )
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_ident( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_ident( py_plugres->request->headers->client );
}



/******************************************************************************
 * partition
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_partition( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_partition( py_plugres->request->headers->client );
}



/******************************************************************************
 * width
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_width( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_width( py_plugres->request->headers->client );
}



/******************************************************************************
 * replica
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_replica( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_replica( py_plugres->request->headers->client );
}



/******************************************************************************
 * height
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_height( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_height( py_plugres->request->headers->client );
}



/******************************************************************************
 * channel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_channel( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_channel( py_plugres->request->headers->client );
}



/******************************************************************************
 * depth
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_depth( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_depth( py_plugres->request->headers->client );
}



/******************************************************************************
 * toplevel
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_toplevel( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_toplevel( py_plugres->request->headers->client );
}



/******************************************************************************
 * hasmatrix
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_hasmatrix( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_hasmatrix( py_plugres->request->headers->client );
}



/******************************************************************************
 * port
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_port( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_port( py_plugres->request->headers->client );
}



/******************************************************************************
 * baseport
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_baseport( PyVGX_PluginResponse *py_plugres, void *closure ) {
  if( py_plugres->request == NULL ) {
    Py_RETURN_NONE;
  }
  return __get_client_baseport( py_plugres->request->headers->client );
}



/******************************************************************************
 * level
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_level( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLong( (int)py_plugres->metas.level.number );
}



/******************************************************************************
 * levelparts
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_levelparts( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLong( (int)py_plugres->metas.level.parts );
}



#define __aggr_i0 0
#define __aggr_i1 1
#define __aggr_f2 0
#define __aggr_f3 1




/**************************************************************************//**
 * __set_int_aggregator
 *
 ******************************************************************************
 */
static int __set_int_aggregator( PyVGX_PluginResponse *py_plugres, PyObject *py_X, int i ) {
  // int
  if( PyLong_CheckExact( py_X ) ) {
    int ovf;
    py_plugres->aggregator.int_aggr[i] = PyLong_AsLongLongAndOverflow( py_X, &ovf );
    if( ovf != 0 ) {
      return -1;
    }
    return 0;
  }
  // error
  PyErr_Format( PyExc_TypeError, "int required, not %s", Py_TYPE( py_X )->tp_name );
  return -1;
}




/**************************************************************************//**
 * __set_dbl_aggregator
 *
 ******************************************************************************
 */
static int __set_dbl_aggregator( PyVGX_PluginResponse *py_plugres, PyObject *py_X, int i ) {
  // float
  if( PyFloat_CheckExact( py_X ) ) {
    py_plugres->aggregator.dbl_aggr[i] = PyFloat_AS_DOUBLE( py_X );
    return 0;
  }
  // int
  if( PyLong_CheckExact( py_X ) ) {
    int ovf;
    py_plugres->aggregator.dbl_aggr[i] = (double)PyLong_AsLongLongAndOverflow( py_X, &ovf );
    if( ovf != 0 ) {
      return -1;
    }
    return 0;
  }
  // error
  PyErr_Format( PyExc_TypeError, "float or int required, not %s", Py_TYPE( py_X )->tp_name );
  return -1;
}



/******************************************************************************
 * i0
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_i0( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLongLong( py_plugres->aggregator.int_aggr[ __aggr_i0 ] );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_i0
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_i0( PyVGX_PluginResponse *py_plugres, PyObject *py_i0, void *closure ) {
  return __set_int_aggregator( py_plugres, py_i0, __aggr_i0 );
}



/******************************************************************************
 * i1
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_i1( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyLong_FromLongLong( py_plugres->aggregator.int_aggr[ __aggr_i1 ] );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_i1
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_i1( PyVGX_PluginResponse *py_plugres, PyObject *py_i1, void *closure ) {
  return __set_int_aggregator( py_plugres, py_i1, __aggr_i1 );
}



/******************************************************************************
 * f2
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_f2( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyFloat_FromDouble( py_plugres->aggregator.dbl_aggr[ __aggr_f2 ] );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_f2
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_f2( PyVGX_PluginResponse *py_plugres, PyObject *py_f2, void *closure ) {
  return __set_dbl_aggregator( py_plugres, py_f2, __aggr_f2 );
}



/******************************************************************************
 * f3
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_f3( PyVGX_PluginResponse *py_plugres, void *closure ) {
  return PyFloat_FromDouble( py_plugres->aggregator.dbl_aggr[ __aggr_f3 ] );
}
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_set_f3
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse_set_f3( PyVGX_PluginResponse *py_plugres, PyObject *py_f3, void *closure ) {
  return __set_dbl_aggregator( py_plugres, py_f3, __aggr_f3 );
}



/******************************************************************************
 * entries
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse_get_entries( PyVGX_PluginResponse *py_plugres, void *closure ) {
  Py_INCREF( py_plugres->py_entries );
  return py_plugres->py_entries;
}



/******************************************************************************
 * PyVGX_PluginResponse_dealloc
 *
 ******************************************************************************
 */
static void PyVGX_PluginResponse_dealloc( PyVGX_PluginResponse *py_plugres ) {
  Py_XDECREF( py_plugres->py_message );
  Py_XDECREF( py_plugres->py_entries );
  Py_XDECREF( py_plugres->py_prev_key );

  Py_TYPE( py_plugres )->tp_free( py_plugres );
}



/******************************************************************************
 * __new_PluginResponse
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __new_PluginResponse( void ) {
  PyVGX_PluginResponse *py_plugres;
  PyObject *py_entries = PyList_New(0);
  if( py_entries == NULL || (py_plugres = (PyVGX_PluginResponse*)p_PyVGX_PluginResponseType->tp_alloc(p_PyVGX_PluginResponseType, 0)) == NULL ) {
    Py_XDECREF( py_entries );
    return NULL;
  }
  py_plugres->request = NULL;

  py_plugres->metas.level.deep_parts = 1;
  py_plugres->metas.level.parts = 1;
  py_plugres->metas.level.number = 0;
  py_plugres->metas.level.incomplete_parts = 0;

  py_plugres->metas.maxhits = -1;
  py_plugres->metas.hitcount = 0;
  py_plugres->metas.sortspec = VGX_SORTBY_NONE;
  py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__NONE;
  py_plugres->metas.py_keytype = NULL;
  memset( &py_plugres->aggregator, 0, sizeof(x_vgx_partial__aggregator) );
  py_plugres->py_message = NULL;
  py_plugres->py_entries = py_entries;
  Py_INCREF( Py_None );
  py_plugres->py_prev_key = Py_None;
  return (PyObject *)py_plugres;
}



/******************************************************************************
 * __init_PluginResponse
 *
 ******************************************************************************
 */
static int __init_PluginResponse( PyVGX_PluginResponse *py_plugres, PyObject *py_maxhits, PyObject *py_sortby, PyObject *py_keytype ) {

#define keytype_is(tp) (py_keytype == ((PyObject*)&(tp)))

  // maxhits
  if( py_maxhits ) {
    if( !PyLong_Check( py_maxhits ) ) {
      PyErr_SetString( PyExc_TypeError, "maxhits: an integer is required" );
      return -1;
    }
    py_plugres->metas.maxhits = PyLong_AS_LONG( py_maxhits );
  }

  // sortby
  if( py_sortby ) {
    if( !PyLong_Check( py_sortby ) ) {
      PyErr_SetString( PyExc_TypeError, "sortby: an integer is required" );
      return -1;
    }
    py_plugres->metas.sortspec = PyLong_AS_LONG( py_sortby );
  }

  // Set sort direction if not specified
  if( !_vgx_sortspec_valid( _vgx_set_sort_direction( &py_plugres->metas.sortspec ) ) ) {
    PyErr_SetString( PyExc_ValueError, "invalid sortby" );
    return -1;
  }

  // keytype
  if( py_keytype && py_keytype != Py_None ) {
    if( !PyType_CheckExact( py_keytype ) ) {
      PyErr_SetString( PyExc_TypeError, "keytype: a type is required" );
      return -1;
    }
    if( keytype_is(PyLong_Type) ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__int64;
    }
    else if( keytype_is(PyFloat_Type) ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__double;
    }
    else if( keytype_is(PyBytes_Type) ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__bytes;
    }
    else if( keytype_is(PyUnicode_Type) ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__unicode;
    }
    else {
      PyErr_SetString( PyExc_TypeError, "keytype: int, float, bytes or unicode required" );
      return -1;
    }
    // Check consistent keytype vs sortspec
    if( py_plugres->metas.sortspec != VGX_SORTBY_NONE ) {
      if( _vgx_sortspec_numeric( py_plugres->metas.sortspec ) ) {
        switch( py_plugres->metas.ktype ) {
        case X_VGX_PARTIAL_SORTKEYTYPE__bytes:
        case X_VGX_PARTIAL_SORTKEYTYPE__unicode:
          PyErr_SetString( PyExc_TypeError, "incompatible sortby/keytype (numeric/string)" );
          return -1;
        default:
          break;
        }
      }
      else if( _vgx_sortspec_string( py_plugres->metas.sortspec ) ) {
        switch( py_plugres->metas.ktype ) {
        case X_VGX_PARTIAL_SORTKEYTYPE__int64:
        case X_VGX_PARTIAL_SORTKEYTYPE__double:
          PyErr_SetString( PyExc_TypeError, "incompatible sortby/keytype (string/numeric)" );
          return -1;
        default:
          break;
        }
      }
    }
    // Set keytype
    py_plugres->metas.py_keytype = (PyTypeObject*)py_keytype;
  }

  return 0;
}



/******************************************************************************
 * PyVGX_PluginResponse__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  return __new_PluginResponse();
}



/******************************************************************************
 * PyVGX_PluginResponse__init
 *
 ******************************************************************************
 */
static int PyVGX_PluginResponse__init( PyVGX_PluginResponse *py_plugres, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "maxhits", "sortby", "keytype", NULL };

  PyObject *py_maxhits = NULL;
  PyObject *py_sortby = NULL;
  PyObject *py_keytype = NULL;
  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|OOO", kwlist, &py_maxhits, &py_sortby, &py_keytype ) ) {
    return -1;
  }

  return __init_PluginResponse( py_plugres, py_maxhits, py_sortby, py_keytype );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_PluginResponse__vectorcall( PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "maxhits",
    "sortby",
    "keytype",
    NULL
  };

  typedef union u_response_args {
    PyObject *args[3];
    struct {
      PyObject *py_maxhits;
      PyObject *py_sortby;
      PyObject *py_keytype;
    };
  } response_args;

  response_args vcargs = {0};

  int64_t nargs = PyVectorcall_NARGS( nargsf );

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 3, vcargs.args ) < 0 ) {
    return NULL;
  }

  PyObject *pyobj = __new_PluginResponse();
  if( pyobj ) {
    if( __init_PluginResponse( (PyVGX_PluginResponse*)pyobj, vcargs.py_maxhits, vcargs.py_sortby, vcargs.py_keytype ) < 0 ) {
      PyVGX_PluginResponse_dealloc( (PyVGX_PluginResponse*)pyobj );
      return NULL;
    }
  }
  return pyobj;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _get_sortkey( PyVGX_PluginResponse *py_plugres, PyObject *py_sortkey ) {
  if( py_sortkey == NULL ) {
    return PyLong_FromLongLong( 1 + PyList_GET_SIZE( py_plugres->py_entries ) );
  }

  PyTypeObject *py_type = Py_TYPE( py_sortkey );

  // Keytype is known and matches the item's key
  if( py_plugres->metas.py_keytype == py_type ) {
  return_sortkey:
    Py_INCREF( py_sortkey );
    return py_sortkey;
  }

  // First item determines key type if not defined
  if( py_plugres->metas.py_keytype == NULL && PyList_GET_SIZE( py_plugres->py_entries ) == 0 ) {
    py_plugres->metas.py_keytype = py_type;
    if( py_type == &PyFloat_Type ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__double;
      goto numeric_sort;
    }
    else if( py_type == &PyLong_Type ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__int64;
      goto numeric_sort;
    }
    else if( py_type == &PyBytes_Type ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__bytes;
      goto string_sort;
    }
    else if( py_type == &PyUnicode_Type ) {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__unicode;
      goto string_sort;
    }
    else {
      py_plugres->metas.ktype = X_VGX_PARTIAL_SORTKEYTYPE__NONE;
      py_plugres->metas.py_keytype = NULL;
      goto return_sortkey;
    }

  numeric_sort:
    // sortspec predicator value by default
    if( py_plugres->metas.sortspec == VGX_SORTBY_NONE ) {
      py_plugres->metas.sortspec = _VGX_SORTBY_PREDICATOR_ASCENDING;
      goto return_sortkey;
    }
    if( _vgx_sortspec_numeric( py_plugres->metas.sortspec ) ) {
      goto return_sortkey;
    }
    if( _vgx_sortspec_dontcare( py_plugres->metas.sortspec ) ) {
      goto return_sortkey;
    }
    goto incompatible_sortspec;

  string_sort:
    // sortspec idstring by default
    if( py_plugres->metas.sortspec == VGX_SORTBY_NONE ) {
      py_plugres->metas.sortspec = _VGX_SORTBY_IDSTRING_ASCENDING;
      goto return_sortkey;
    }
    if( _vgx_sortspec_string( py_plugres->metas.sortspec ) ) {
      goto return_sortkey;
    }
    if( _vgx_sortspec_dontcare( py_plugres->metas.sortspec ) ) {
      goto return_sortkey;
    }
    goto incompatible_sortspec;
  }

  // Item's key does not match, but we don't care about sorting
  if( py_plugres->metas.py_keytype == NULL ) {
    goto return_sortkey;
  }

  // Try to cast if possible and return new object
  // int -> float
  if( py_plugres->metas.py_keytype == &PyFloat_Type && py_type == &PyLong_Type ) {
    return PyFloat_FromDouble( (double)PyLong_AsLongLong( py_sortkey ) );
  }
  // float -> int  (only if whole number)
  if( py_plugres->metas.py_keytype == &PyLong_Type && py_type == &PyFloat_Type ) {
    double val = PyFloat_AsDouble( py_sortkey );
    if( val == (int64_t)val ) {
      return PyLong_FromLongLong( (int64_t)val );
    }
  }

  // Item's key does not match
  PyErr_Format( PyExc_TypeError, "sortkey %R must be '%s', not '%s'", py_sortkey, py_plugres->metas.py_keytype->tp_name, py_type->tp_name );
  return NULL;

incompatible_sortspec:
  PyErr_Format( PyExc_TypeError, "incompatible sortkey %R (sortby=%s)", py_sortkey, _vgx_sortspec_as_string(py_plugres->metas.sortspec) );
  return NULL;

}



/**************************************************************************//**
 * __not_ascending
 *
 ******************************************************************************
 */
static bool __not_ascending( PyObject *py_this, PyObject *py_prev ) {
  if( PyFloat_CheckExact( py_this ) && PyFloat_CheckExact( py_prev ) ) {
    return PyFloat_AS_DOUBLE( py_this ) < PyFloat_AS_DOUBLE( py_prev );
  }
  if( PyLong_CheckExact( py_this ) && PyLong_CheckExact( py_prev ) ) {
    int64_t a = PyLong_AsLongLong( py_this );
    int64_t b = PyLong_AsLongLong( py_prev );
    return a < b;
  }
  return PyObject_RichCompareBool( py_this, py_prev, Py_LT ) > 0;
}




/**************************************************************************//**
 * __not_descending
 *
 ******************************************************************************
 */
static bool __not_descending( PyObject *py_this, PyObject *py_prev ) {
  if( PyFloat_CheckExact( py_this ) && PyFloat_CheckExact( py_prev ) ) {
    return PyFloat_AS_DOUBLE( py_this ) > PyFloat_AS_DOUBLE( py_prev );
  }
  if( PyLong_CheckExact( py_this ) && PyLong_CheckExact( py_prev ) ) {
    int64_t a = PyLong_AsLongLong( py_this );
    int64_t b = PyLong_AsLongLong( py_prev );
    return a > b;
  }
  return PyObject_RichCompareBool( py_this, py_prev, Py_GT ) > 0;
}



/******************************************************************************
 * PyVGX_PluginResponse__Append
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Append__doc__,
  "Append( [sortkey,] item ) -> list\n"
);

/**************************************************************************//**
 * PyVGX_PluginResponse_Append
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginResponse_Append( PyVGX_PluginResponse *py_plugres, PyObject *args ) {
  if( py_plugres->metas.maxhits >= 0 ) {
    if( PyList_GET_SIZE( py_plugres->py_entries ) >= py_plugres->metas.maxhits ) {
      goto maxhits_error;
    }
  }

  PyObject *py_ret = NULL;
  PyObject *py_sortkey = NULL;
  PyObject *py_item = NULL;

  if( !PyArg_ParseTuple( args, "O|O", &py_sortkey, &py_item ) ) {
    return NULL;
  }

  PyObject *py_entry = PyTuple_New(2);
  if( py_entry == NULL ) {
    return NULL;
  }

  // If only one arg given, it is the item and the sortkey is auto generated
  if( py_item == NULL ) {
    py_item = py_sortkey;
    py_sortkey = NULL;
  }

  // Validate sortkey and get a NEW REFERENCE to it if valid
  if( (py_sortkey = _get_sortkey( py_plugres, py_sortkey )) == NULL ) {
    goto end;
  }

  // Verify sort order
  if( py_plugres->py_prev_key != Py_None ) {
    switch( _vgx_sort_direction( py_plugres->metas.sortspec ) ) {
    case VGX_SORT_DIRECTION_ASCENDING:
      if( __not_ascending( py_sortkey, py_plugres->py_prev_key ) ) {
        goto order_error;
      }
      break;
    case VGX_SORT_DIRECTION_DESCENDING:
      if( __not_descending( py_sortkey, py_plugres->py_prev_key ) ) {
        goto order_error;
      }
      break;
    default:
      break;
    }
  }

  // Hand over sortkey and item to the entry tuple
  PyTuple_SET_ITEM( py_entry, 0, py_sortkey );
  Py_INCREF( py_item );
  PyTuple_SET_ITEM( py_entry, 1, py_item );
  if( PyList_Append( py_plugres->py_entries, py_entry ) < 0 ) {
    goto end;
  }

  // Replace previous key with new key
  Py_DECREF( py_plugres->py_prev_key );
  Py_INCREF( py_sortkey );
  py_plugres->py_prev_key = py_sortkey;

  // Update hitcount attribute if needed
  int64_t sz = PyList_GET_SIZE( py_plugres->py_entries );
  if( py_plugres->metas.hitcount < sz ) {
    py_plugres->metas.hitcount = sz;
  }

  // Return None
  Py_INCREF( Py_None );
  py_ret = Py_None;
  
end:
  Py_DECREF( py_entry );
  return py_ret;

order_error:
  PyErr_Format( PyVGX_ResponseError, "out of order sortkey %R with previous %R (sortby=%s)", py_sortkey, py_plugres->py_prev_key, _vgx_sortspec_as_string(py_plugres->metas.sortspec) );
  Py_DECREF( py_sortkey );
  goto end;

maxhits_error:
  PyErr_Format( PyVGX_ResponseError, "maxhits %d reached", py_plugres->metas.maxhits );
  return NULL;
}



/******************************************************************************
 * PyVGX_PluginResponse_Serialize
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SerializeResponse__doc__,
  "Serialize() -> bytes\n"
);

/**************************************************************************//**
 * PyVGX_PluginResponse_Serialize
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginResponse_Serialize( PyVGX_PluginResponse *py_plugres ) {
  vgx_StreamBuffer_t *buffer = iStreamBuffer.New(8);
  if( buffer == NULL ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }
  if( __pyvgx_PluginResponse_serialize_x_vgx_partial( py_plugres, buffer ) < 0 ) {
    iStreamBuffer.Delete( &buffer );
    return NULL;
  }
  const char *data;
  int64_t sz_data = iStreamBuffer.ReadableSegment( buffer, LLONG_MAX, &data, NULL );
  PyObject *py_serialized = PyBytes_FromStringAndSize( data, sz_data );
  iStreamBuffer.Delete( &buffer );
  return py_serialized;
}



/******************************************************************************
 * PyVGX_PluginResponse__Deserialize
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Deserialize__doc__,
  "Deserialize( data ) -> obj\n"
);
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * PyVGX_PluginResponse_Deserialize
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginResponse_Deserialize( PyVGX_PluginResponse *py_plugres, PyObject *py_data ) {
  char *data;
  Py_ssize_t sz_data;
  if( PyBytes_AsStringAndSize( py_data, &data, &sz_data ) < 0 ) {
    return NULL;
  }
  return (PyObject*)__deserialize_partial( data, sz_data );
}



/******************************************************************************
 * PyVGX_PluginResponse__ToJSON
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ToJSON__doc__,
  "ToJSON() -> str\n"
);

/**************************************************************************//**
 * __pyvgx_PluginResponse_ToJSON
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * __pyvgx_PluginResponse_ToJSON( PyVGX_PluginResponse *py_plugres ) {
  PyObject *py_json = NULL;
  PyObject *py_obj = PyDict_New();
  if( py_obj ) {
    // level
    PyDict_SetItem( py_obj, g_py_key_level, PyLong_FromLong( (int)py_plugres->metas.level.number ) );
    // levelparts
    PyDict_SetItem( py_obj, g_py_key_levelparts, PyLong_FromLong( (int)py_plugres->metas.level.parts ) );
    // partials
    PyDict_SetItem( py_obj, g_py_key_partials, PyLong_FromLong( (int)py_plugres->metas.level.deep_parts ) );
    // hitcount
    PyDict_SetItem( py_obj, g_py_key_hitcount, PyLong_FromLongLong( py_plugres->metas.hitcount ) );
    // aggregator
    PyObject *py_aggr = PyList_New(4);
    if( py_aggr ) {
      x_vgx_partial__aggregator *A = &py_plugres->aggregator;
      PyList_SetItem( py_aggr, 0, PyLong_FromLongLong( A->int_aggr[0] ) );
      PyList_SetItem( py_aggr, 1, PyLong_FromLongLong( A->int_aggr[1] ) );
      PyList_SetItem( py_aggr, 2, PyFloat_FromDouble( A->dbl_aggr[0] ) );
      PyList_SetItem( py_aggr, 3, PyFloat_FromDouble( A->dbl_aggr[1] ) );
      PyDict_SetItem( py_obj, g_py_key_aggregator, py_aggr );
      Py_DECREF( py_aggr );
    }
    // message
    if( py_plugres->py_message ) {
      PyDict_SetItem( py_obj, g_py_key_message, py_plugres->py_message );
    }
    // entries
    PyDict_SetItem( py_obj, g_py_key_entries, py_plugres->py_entries );
    // to json
    py_json = iPyVGXCodec.NewJsonPyStringFromPyObject( py_obj );
    Py_DECREF( py_obj );
  }
  return py_json;
}



/******************************************************************************
 * PyVGX_PluginResponse__Resubmittable
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Resubmittable__doc__,
  "Resubmittable() -> request\n"
);

/**************************************************************************//**
 * __pyvgx_PluginResponse_Resubmittable
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * __pyvgx_PluginResponse_Resubmittable( PyVGX_PluginResponse *py_plugres ) {

  vgx_VGXServerRequest_t *request = py_plugres->request;
  if( request == NULL ) {
    PyErr_SetString( PyVGX_ResponseError, "invalid context (no request)" );
    return NULL;
  }

  if( request->method != HTTP_GET ) {
    PyErr_Format( PyVGX_RequestError, "resubmit not possible for %s (must be GET)", __vgx_http_request_method( request->method ) );
    return NULL;
  }

  // SUPER IMPORTANT: flag no content to avoid content buffer corruption when serializing (due to stream/content swapping)
  request->content_type = MEDIA_TYPE__NONE;
  request->headers->content_length = 0;


  PyVGX_PluginRequest *py_plugreq_resubmit = NULL;

  XTRY {
    //
    if( (py_plugreq_resubmit = (PyVGX_PluginRequest*)__new_PluginRequest()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    //
    py_plugreq_resubmit->request = request;

    // params
    if( __process_uri_parameters( py_plugreq_resubmit ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_API, 0x002 );
    }

    // headers
    if( (py_plugreq_resubmit->py_headers = __vgx_PyBytes_FromHTTPHeaders( request->headers )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_API, 0x003 );
    }

    // Indicate request is being resubmitted
    request->headers->control.resubmit = true;

  }
  XCATCH( errcode ) {
    Py_XDECREF( py_plugreq_resubmit );
    py_plugreq_resubmit = NULL;
  }
  XFINALLY {
  }

  return (PyObject*)py_plugreq_resubmit;

}
 


/******************************************************************************
 * PyVGX_PluginResponse_len
 *
 ******************************************************************************
 */
static Py_ssize_t PyVGX_PluginResponse_len( PyVGX_PluginResponse *py_plugres ) {
  return PyList_Size( py_plugres->py_entries );
}



/******************************************************************************
 * PyVGX_PluginResponse_item
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginResponse_item( PyVGX_PluginResponse *py_plugres, Py_ssize_t idx ) {
  PyObject *py_item = PyList_GetItem( py_plugres->py_entries, idx );
  if( py_item ) {
    Py_INCREF( py_item );
  }
  return py_item;
}



/******************************************************************************
 * PyVGX_PluginResponse_repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_PluginResponse_repr( PyVGX_PluginResponse *py_plugres ) {
  int maxhits = py_plugres->metas.maxhits;
  const char *sortby = _vgx_sortspec_as_string( py_plugres->metas.sortspec );
  const char *sdir = _vgx_sort_direction_as_string( py_plugres->metas.sortspec );
  int64_t hitcount = py_plugres->metas.hitcount;
  int64_t sz = PyList_GET_SIZE( py_plugres->py_entries );
  const char *ktype = py_plugres->metas.py_keytype ? py_plugres->metas.py_keytype->tp_name : "None";
  return PyUnicode_FromFormat( "<pyvgx.PluginResponse maxhits=%d sortby=%s%s%s ktype=%s hitcount=%lld sz=%lld>", maxhits, sortby, *sdir?"|":"", sdir, ktype, hitcount, sz );
}



/******************************************************************************
 * PyVGX_PluginResponse_members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_PluginResponse_members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginResponse_getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_PluginResponse_getset[] = {
  {"maxhits",                 (getter)PyVGX_PluginResponse_get_maxhits,             (setter)PyVGX_PluginResponse_set_maxhits,   "maxhits",        NULL },
  {"sortby",                  (getter)PyVGX_PluginResponse_get_sortby,              (setter)0,                                  "sortby",         NULL },
  {"keytype",                 (getter)PyVGX_PluginResponse_get_keytype,             (setter)0,                                  "keytype",        NULL },
  {"message",                 (getter)PyVGX_PluginResponse_get_message,             (setter)PyVGX_PluginResponse_set_message,   "message",        NULL },
  {"entries",                 (getter)PyVGX_PluginResponse_get_entries,             (setter)0,                                  "entries",        NULL },
  {"hitcount",                (getter)PyVGX_PluginResponse_get_hitcount,            (setter)PyVGX_PluginResponse_set_hitcount,  "hitcount",       NULL },
  {"partials",                (getter)PyVGX_PluginResponse_get_partials,            (setter)PyVGX_PluginResponse_set_partials,  "partials",       NULL },
  {"requestpath",             (getter)PyVGX_PluginResponse_get_requestpath,         (setter)0,                                  "requestpath",    NULL },
  {"requestmethod",           (getter)PyVGX_PluginResponse_get_requestmethod,       (setter)0,                                  "requestmethod",  NULL },
  {"t0",                      (getter)PyVGX_PluginResponse_get_t0,                  (setter)0,                                  "t0",             NULL },
  {"texec",                   (getter)PyVGX_PluginResponse_get_texec,               (setter)0,                                  "texec",          NULL },
  {"sn",                      (getter)PyVGX_PluginResponse_get_sn,                  (setter)0,                                  "sn",             NULL },
  {"signature",               (getter)PyVGX_PluginResponse_get_signature,           (setter)0,                                  "signature",      NULL },
  {"executor",                (getter)PyVGX_PluginResponse_get_executor,            (setter)0,                                  "executor",       NULL },
  {"flag",                    (getter)PyVGX_PluginResponse_get_flag,                (setter)0,                                  "flag",           NULL },
  {"local",                   (getter)PyVGX_PluginResponse_get_local,               (setter)PyVGX_PluginResponse_set_local,     "local",          NULL },
  {"resubmits",               (getter)PyVGX_PluginResponse_get_resubmits,           (setter)0,                                  "resubmits",      NULL },
  {"ident",                   (getter)PyVGX_PluginResponse_get_ident,               (setter)0,                                  "ident",          NULL },
  {"partition",               (getter)PyVGX_PluginResponse_get_partition,           (setter)0,                                  "partition",      NULL },
  {"width",                   (getter)PyVGX_PluginResponse_get_width,               (setter)0,                                  "width",          NULL },
  {"replica",                 (getter)PyVGX_PluginResponse_get_replica,             (setter)0,                                  "replica",        NULL },
  {"height",                  (getter)PyVGX_PluginResponse_get_height,              (setter)0,                                  "height",         NULL },
  {"channel",                 (getter)PyVGX_PluginResponse_get_channel,             (setter)0,                                  "channel",        NULL },
  {"depth",                   (getter)PyVGX_PluginResponse_get_depth,               (setter)0,                                  "depth",          NULL },
  {"toplevel",                (getter)PyVGX_PluginResponse_get_toplevel,            (setter)0,                                  "toplevel",       NULL },
  {"hasmatrix",               (getter)PyVGX_PluginResponse_get_hasmatrix,           (setter)0,                                  "hasmatrix",      NULL },
  {"port",                    (getter)PyVGX_PluginResponse_get_port,                (setter)0,                                  "port",           NULL },
  {"baseport",                (getter)PyVGX_PluginResponse_get_baseport,            (setter)0,                                  "baseport",       NULL },
  {"level",                   (getter)PyVGX_PluginResponse_get_level,               (setter)0,                                  "level",          NULL },
  {"levelparts",              (getter)PyVGX_PluginResponse_get_levelparts,          (setter)0,                                  "levelparts",     NULL },
  {"i0",                      (getter)PyVGX_PluginResponse_get_i0,                  (setter)PyVGX_PluginResponse_set_i0,        "i0",             NULL },
  {"i1",                      (getter)PyVGX_PluginResponse_get_i1,                  (setter)PyVGX_PluginResponse_set_i1,        "i1",             NULL },
  {"f2",                      (getter)PyVGX_PluginResponse_get_f2,                  (setter)PyVGX_PluginResponse_set_f2,        "f2",             NULL },
  {"f3",                      (getter)PyVGX_PluginResponse_get_f3,                  (setter)PyVGX_PluginResponse_set_f3,        "f3",             NULL },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginResponse_methods
 *
 ******************************************************************************
 */
static PyMethodDef PyVGX_PluginResponse_methods[] = {
  {"Append",           (PyCFunction)PyVGX_PluginResponse_Append,             METH_VARARGS,                   Append__doc__ },
  {"Serialize",        (PyCFunction)PyVGX_PluginResponse_Serialize,          METH_NOARGS,                    SerializeResponse__doc__ },
  {"Deserialize",      (PyCFunction)PyVGX_PluginResponse_Deserialize,        METH_STATIC | METH_O,           Deserialize__doc__ },
  {"ToJSON",           (PyCFunction)__pyvgx_PluginResponse_ToJSON,           METH_NOARGS,                    ToJSON__doc__ },
  {"Resubmittable",    (PyCFunction)__pyvgx_PluginResponse_Resubmittable,    METH_NOARGS,                    Resubmittable__doc__ },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_PluginResponse_as_sequence
 *
 ******************************************************************************
 */
static PySequenceMethods PyVGX_PluginResponse_as_sequence = {
    .sq_length          = (lenfunc)PyVGX_PluginResponse_len,
    .sq_concat          = (binaryfunc)0,
    .sq_repeat          = (ssizeargfunc)0,
    .sq_item            = (ssizeargfunc)PyVGX_PluginResponse_item,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,//(ssizeobjargproc)PyVGX_PluginResponse_ass_item,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)0,
    .sq_inplace_concat  = (binaryfunc)0,
    .sq_inplace_repeat  = (ssizeargfunc)0
};



/******************************************************************************
 * PyVGX_PluginResponse_as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_PluginResponse_as_mapping = {
    .mp_length          = (lenfunc)PyVGX_PluginResponse_len,
    .mp_subscript       = 0,//(binaryfunc)PyVGX_PluginResponse_get_item,
    .mp_ass_subscript   = 0//(objobjargproc)PyVGX_PluginResponse_set_item
};



/******************************************************************************
 * PyVGX_PluginResponseType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_PluginResponseType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.PluginResponse",
    .tp_basicsize       = sizeof(PyVGX_PluginResponse),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_PluginResponse_dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_PluginResponse_repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &PyVGX_PluginResponse_as_sequence,
    .tp_as_mapping      = &PyVGX_PluginResponse_as_mapping,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX PluginResponse objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_PluginResponse_methods,
    .tp_members         = PyVGX_PluginResponse_members,
    .tp_getset          = PyVGX_PluginResponse_getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_PluginResponse__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_PluginResponse__new,
    .tp_free            = (freefunc)0,
    .tp_is_gc           = (inquiry)0,
    .tp_bases           = NULL,
    .tp_mro             = NULL,
    .tp_cache           = NULL,
    .tp_subclasses      = NULL,
    .tp_weaklist        = NULL,
    .tp_del             = (destructor)0,
    .tp_version_tag     = 0,
    .tp_finalize        = (destructor)0,
    .tp_vectorcall      = (vectorcallfunc)PyVGX_PluginResponse__vectorcall

};


DLL_HIDDEN PyTypeObject * p_PyVGX_PluginResponseType = &PyVGX_PluginResponseType;
