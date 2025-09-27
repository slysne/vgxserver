/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_plugin.c
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
#include "generated/_pyvgx_plugin_builtins.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );



static bool g_init_pyvgx = false;
static int g_builtin_counter = 0;

static PyObject *mod_pyvgx = NULL;
static PyObject *mod_inspect = NULL;
static PyObject *meth_signature = NULL;
static PyObject *meth_getfullargspec = NULL;
static PyObject *mod_ast = NULL;

static PyObject *g_py_param_request = NULL;
static PyObject *g_py_param_response = NULL;
static PyObject *g_py_param_graph = NULL;
static PyObject *g_py_param_method = NULL;
static PyObject *g_py_param_headers = NULL;
static PyObject *g_py_param_content = NULL;


/******************************************************************************
 * 
 *
 ******************************************************************************
 */
typedef struct __s_plugin {
  PyObject *function;
  PyObject *argspec;
  PyObject *bound_graph;
} __plugin;



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
typedef struct __s_plugin_param {
  PyObject *py_obj;
  bool in_signature;
} __plugin_param;






/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void             __plugin__delete_pyvgx( PyObject **py_pyvgx );
static int              __plugin__init_pyvgx( PyObject **py_pyvgx );
static void             __plugin__delete_inspect( PyObject **py_inspect, PyObject **py_signature, PyObject **py_getfullargspec );
static int              __plugin__init_inspect( PyObject **py_inspect, PyObject **py_signature, PyObject **py_getfullargspec );
static void             __plugin__delete_ast( PyObject **py_ast );
static int              __plugin__init_ast( PyObject **py_ast );
static int              __plugin__execute_python_source( const char *source_lines[], const char *filename );
static int              __plugin__autocast_str_by_annotation( const PyObject *py_type, vgx_KeyVal_t *kv );
static HTTPStatus       __plugin__map_keyval_to_kwargs_and_dict( vgx_KeyVal_t *kv, PyObject *py_arg_type, PyObject *kwargs, PyObject *py_all );
static PyObject *       __plugin__new_request_content( const PyTypeObject *py_content_type, vgx_VGXServerRequest_t *request );
static const char *     __plugin__get_pytype_simple_string( PyObject *py_type );
static bool             __plugin__is_pyobj_json_compatible( PyObject *py_obj );
static int              __get_plugin( const char *plugin_name, __plugin *plugin );
static bool             __plugin_param_in_signature( __plugin *plugin, PyObject *py_param_name, __plugin_param *param );
static PyObject *       __get_arg_type( PyObject *argspec, const char *argname );
static int              __process_uri_parameters( vgx_URIQueryParameters_t *params, __plugin *plugin, PyObject *py_kwargs, PyObject *py_all, vgx_VGXServerResponse_t *response );
static int              __set_list_keyval( PyObject *py_list, int64_t i, const char *key, int64_t sz_key, const char *value, int64_t sz_value );
static int              __set_noop( PyObject *__ign0, int64_t __ign1, const char *__ign2, int64_t __ign3, const char *__ign4, int64_t __ign5 );
static f_keyval_setfunc __get_header_setfunc( PyTypeObject *py_headers_type );
static PyObject *       __capture_headers_into_kwargs( vgx_VGXServerRequest_t *request, PyObject *py_argspec, PyObject *py_kwargs );
static int              __render_result( vgx_VGXServerResponse_t *response, __plugin *plugin, PyObject *py_result );
static int              __plugin__call( const char *plugin_name, bool post, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error );



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __plugin__delete_pyvgx( PyObject **py_pyvgx ) {
  Py_XDECREF( *py_pyvgx );
  *py_pyvgx = NULL;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__init_pyvgx( PyObject **py_pyvgx ) {
  int ret = 0;
  *py_pyvgx = PyImport_ImportModule( "pyvgx" );
  if( *py_pyvgx == NULL ) {
    __plugin__delete_pyvgx( py_pyvgx );
    ret = -1;
  }
  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __plugin__delete_inspect( PyObject **py_inspect, PyObject **py_signature, PyObject **py_getfullargspec ) {
  Py_XDECREF( *py_inspect );
  Py_XDECREF( *py_signature );
  Py_XDECREF( *py_getfullargspec );
  *py_inspect = NULL;
  *py_signature = NULL;
  *py_getfullargspec = NULL;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__init_inspect( PyObject **py_inspect, PyObject **py_signature, PyObject **py_getfullargspec ) {
  int ret = 0;
  *py_inspect = PyImport_ImportModule( "inspect" );
  *py_signature = PyUnicode_FromString( "signature" );
  *py_getfullargspec = PyUnicode_FromString( "getfullargspec" );
  if( *py_inspect == NULL || *py_signature == NULL || *py_getfullargspec == NULL ) {
    __plugin__delete_inspect( py_inspect, py_signature, py_getfullargspec );
    ret = -1;
  }
  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void __plugin__delete_ast( PyObject **py_ast ) {
  Py_XDECREF( *py_ast );
  *py_ast = NULL;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__init_ast( PyObject **py_ast ) {
  int ret = 0;
  *py_ast = PyImport_ImportModule( "ast" );
  if( *py_ast == NULL ) {
    __plugin__delete_ast( py_ast );
    ret = -1;
  }
  return ret;
}


/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__execute_python_source( const char *source_lines[], const char *filename ) {
  int ret = 0;

  char *py_program = NULL;
  PyObject *py_code = NULL;
  PyObject *py_eval = NULL;
  PyObject *py_pyvgx_dict = NULL;

  XTRY {
    // Python source lines
    const char **line = source_lines;

    // Determine size of python source
    int64_t sz_py_program = 0;
    while( *line != NULL ) {
      sz_py_program += strlen( *line ) + 1; // Account for newline which will be added
      ++line;
    }

    // Allocate memory to hold python source
    if( (py_program = malloc( sz_py_program + 1 )) == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Write lines to source string
    line = source_lines;
    char *p = py_program;
    while( *line != NULL ) {
      p = write_chars( p, *line++ );
      *p++ = '\n';
    }
    *p = '\0';

    // Compile Python source
    if( (py_code = Py_CompileStringExFlags( py_program, filename, Py_file_input, NULL, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // pyvgx.__dict__
    if( (py_pyvgx_dict = PyObject_GenericGetDict( g_pyvgx, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Evaluate Python source to capture builtin definitions
    PyObject *py_globals = PyEval_GetGlobals();
    
    if( (py_eval = PyEval_EvalCode( py_code, py_globals, py_globals )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( py_program );
    Py_XDECREF( py_eval );
    Py_XDECREF( py_code );
    Py_XDECREF( py_pyvgx_dict );
  }

  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__autocast_str_by_annotation( const PyObject *py_type, vgx_KeyVal_t *kv ) {
  char *e = NULL;
  if( py_type == (PyObject*)&PyLong_Type ) {
    goto integer;
  }
  else if( py_type == (PyObject*)&PyFloat_Type ) {
    goto real;
  }
  else if( py_type == (PyObject*)&PyBytes_Type ) {
    goto bytes;
  }
  else if( iPyVGXCodec.IsTypeJson( py_type ) ) {
    goto json;
  }
  return 0;

// Interpret string as int64 value
integer:
  {
    int64_t val = strtoll( kv->val.data.simple.string, &e, 0 );
    if( e && *e != '\0' ) {
      PyErr_Format( PyExc_TypeError, "%s=%s (an integer is required)", kv->key, kv->val.data.simple.string );
      return -1;
    }
    kv->val.data.simple.integer = val;
    kv->val.type = VGX_VALUE_TYPE_INTEGER;
    return 0;
  }

// Interpret string as double value
real:
  {
    double val = strtod( kv->val.data.simple.string, &e );
    if( e && *e != '\0' ) {
      PyErr_Format( PyExc_TypeError, "%s=%s (a number is required)", kv->key, kv->val.data.simple.string );
      return -1;
    }
    kv->val.data.simple.real = val;
    kv->val.type = VGX_VALUE_TYPE_REAL;
    return 0;
  }

// Interpret string as bytes
bytes:
  {
    // Abuse this value type to indicate 'bytes'
    kv->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
    return 0;
  }

// Interpret string as JSON
json:
  {
    PyObject *py_json_string = PyUnicode_FromString( kv->val.data.simple.string );
    if( py_json_string ) {
      PyObject *py_object = iPyVGXCodec.NewPyObjectFromJsonPyString( py_json_string );
      Py_DECREF( py_json_string );
      if( py_object ) {
        // LEAK WARNING: Must decref this
        kv->val.data.simple.pointer = py_object;
        kv->val.type = VGX_VALUE_TYPE_POINTER;
      }
      else {
        PyErr_Clear();
        PyErr_Format( PyExc_ValueError, "%s=%s (invalid JSON)", kv->key, kv->val.data.simple.string );
        return -1;
      }
    }
    return 0;
  }


}



#define RETURN_BadRequest( Format, ... ) do { \
  PyErr_Format( PyExc_Exception, Format, ##__VA_ARGS__ ); \
  return HTTP_STATUS__BadRequest; \
} WHILE_ZERO



#define RETURN_InternalServerError( Format, ... ) do { \
  PyErr_Format( PyExc_Exception, Format, ##__VA_ARGS__ ); \
  return HTTP_STATUS__InternalServerError; \
} WHILE_ZERO



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static HTTPStatus __plugin__parameter_mapping_error( const char *key ) {
  if( PyErr_Occurred() ) {
    CString_t *CSTR__error = NULL;
    iPyVGXBuilder.CatchPyExceptionIntoOutput( NULL, NULL, &CSTR__error, NULL );
    if( CSTR__error ) {
      char serr[512] = {0};
      strncpy( serr, CStringValue( CSTR__error ), 511 );
      iString.Discard( &CSTR__error );
      RETURN_BadRequest( "parameter '%s' error: %s", key, serr );
    }
  }
  RETURN_InternalServerError( "internal error while mapping plugin parameter '%s'", key );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static HTTPStatus __plugin__map_keyval_to_kwargs_and_dict( vgx_KeyVal_t *kv, PyObject *py_arg_type, PyObject *kwargs, PyObject *py_all ) {

  if( kv->key == NULL || *kv->key == '\0' ) {
    return HTTP_STATUS__NONE;
  }

  PyObject *py_key = PyUnicode_FromString( kv->key );
  if( py_key == NULL ) {
    RETURN_InternalServerError( "internal parameter key error" );
  }
  PyObject *py_object;

  // Try to convert string to correct type if function has non-string annotation for this parameter
  if( kv->val.type == VGX_VALUE_TYPE_STRING && py_arg_type != (PyObject*)&PyUnicode_Type ) {
    if( __plugin__autocast_str_by_annotation( py_arg_type, kv ) < 0 ) {
      Py_DECREF( py_key );
      return HTTP_STATUS__BadRequest;
    }
  }

  switch( kv->val.type ) {
  case VGX_VALUE_TYPE_NULL:
    Py_INCREF( Py_None );
    py_object = Py_None;
    break;
  // special indicator for 'bytes'
  case VGX_VALUE_TYPE_BORROWED_STRING:
    py_object = PyBytes_FromString( kv->val.data.simple.string );
    break;
  case VGX_VALUE_TYPE_STRING:
    py_object = PyUnicode_FromString( kv->val.data.simple.string );
    break;
  case VGX_VALUE_TYPE_INTEGER:
    py_object = PyLong_FromLongLong( kv->val.data.simple.integer );
    break;
  case VGX_VALUE_TYPE_REAL:
    py_object = PyFloat_FromDouble( kv->val.data.simple.real );
    break;
  // PyObject* created by autocast above
  case VGX_VALUE_TYPE_POINTER:
    py_object = kv->val.data.simple.pointer; // 1 ref already
    break;
  default:
    Py_DECREF( py_key );
    PyErr_Format( PyExc_TypeError, "unsupported value type for parameter '%s'", kv->key );
    return HTTP_STATUS__BadRequest;
  }

  if( py_object == NULL ) {
    Py_DECREF( py_key );
    return __plugin__parameter_mapping_error( kv->key );
  }

  int r = PyDict_SetItem( kwargs, py_key, py_object );
  r += PyDict_SetItem( py_all, py_key, py_object );

  Py_DECREF( py_key );
  Py_DECREF( py_object );

  if( r ) {
    RETURN_InternalServerError( "unknown internal error" );
  }
  
  return HTTP_STATUS__NONE;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
DLL_HIDDEN HTTPStatus __pyvgx_plugin__map_keyval_to_dict( vgx_KeyVal_t *kv, PyObject *py_dict ) {
  if( kv->key == NULL || *kv->key == '\0' ) {
    return HTTP_STATUS__NONE;
  }

  PyObject *py_key = PyUnicode_FromString( kv->key );
  if( py_key == NULL ) {
    RETURN_InternalServerError( "internal parameter key error" );
  }
  PyObject *py_object;

  switch( kv->val.type ) {
  case VGX_VALUE_TYPE_NULL:
    Py_INCREF( Py_None );
    py_object = Py_None;
    break;
  case VGX_VALUE_TYPE_STRING:
    py_object = PyUnicode_FromString( kv->val.data.simple.string );
    break;
  case VGX_VALUE_TYPE_INTEGER:
    py_object = PyLong_FromLongLong( kv->val.data.simple.integer );
    break;
  case VGX_VALUE_TYPE_REAL:
    py_object = PyFloat_FromDouble( kv->val.data.simple.real );
    break;
  default:
    Py_DECREF( py_key );
    PyErr_Format( PyExc_TypeError, "unsupported value type for parameter '%s'", kv->key );
    return HTTP_STATUS__BadRequest;
  }

  if( py_object == NULL ) {
    Py_DECREF( py_key );
    return __plugin__parameter_mapping_error( kv->key );
  }

  int r = PyDict_SetItem( py_dict, py_key, py_object );

  Py_DECREF( py_key );
  Py_DECREF( py_object );

  if( r ) {
    RETURN_InternalServerError( "unknown internal error" );
  }
  
  return HTTP_STATUS__NONE;

}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyObject * __plugin__new_request_content( const PyTypeObject *py_content_type, vgx_VGXServerRequest_t *request ) {
  // Assumption: request_content is contiguous, i.e. contained in a single segment in the buffer
  const char *bytes = "";
  int64_t sz = 0;
  if( request ) {
    sz = iStreamBuffer.ReadableSegment( request->buffers.content, LLONG_MAX, &bytes, NULL );
  }

  // capsule
  if( py_content_type == &PyCapsule_Type ) {
    return PyCapsule_New( (void*)bytes, NULL, NULL );
  }
  // bytes (DEFAULT)
  else if( py_content_type == NULL || py_content_type == (PyTypeObject*)Py_None || py_content_type == &PyBytes_Type ) {
    return PyBytes_FromStringAndSize( bytes, sz );
  }
  // str
  else if( py_content_type == &PyUnicode_Type ) {
    return PyUnicode_FromStringAndSize( bytes, sz );
  }
  // json
  else if( iPyVGXCodec.IsTypeJson( (PyObject*)py_content_type ) ) {
    if( sz > 0 ) {
      return iPyVGXCodec.NewPyObjectFromJsonBytes( bytes, sz );
    }
    Py_INCREF( Py_None );
    return Py_None;
  }
  // Unsupported annotation
  else {
    PyObject *py_repr = PyObject_Repr( (PyObject*)py_content_type );
    const char *stype = py_repr ? PyUnicode_AsUTF8( py_repr ) : "None";
    PyErr_Format( PyExc_TypeError, "Plugin error: invalid annotation '%s' for 'content' parameter", stype );
    Py_XDECREF( py_repr );
    return NULL;
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static const char * __plugin__get_pytype_simple_string( PyObject *py_type ) {
  if( py_type == Py_None ) {
    return "*";
  }
  else if( py_type == (PyObject*)&PyLong_Type ) {
    return "int";
  }
  else if( py_type == (PyObject*)&PyFloat_Type ) {
    return "float";
  }
  else if( py_type == (PyObject*)&PyUnicode_Type ) {
    return "str";
  }
  else if( py_type == (PyObject*)&PyBytes_Type ) {
    return "bytes";
  }
  else if( py_type == (PyObject*)&PyByteArray_Type ) {
    return "bytearray";
  }
  else if( py_type == (PyObject*)&PyTuple_Type ) {
    return "tuple";
  }
  else if( py_type == (PyObject*)&PyList_Type ) {
    return "list";
  }
  else if( py_type == (PyObject*)&PyDict_Type ) {
    return "dict";
  }
  else if( py_type == (PyObject*)&PySet_Type ) {
    return "set";
  }
  else if( iPyVGXCodec.IsTypeJson( py_type ) ) {
    return "json";
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
static bool __plugin__is_pyobj_json_compatible( PyObject *py_obj ) {
  if( py_obj ) {
    PyTypeObject *py_type = (PyTypeObject*)PyObject_Type( py_obj );
    if( py_type == &PyLong_Type ||
        py_type == &PyFloat_Type ||
        py_type == &PyUnicode_Type ||
        iPyVGXCodec.IsTypeJson( (PyObject*)py_type ) )
    {
      return true;
    }
  }
  return false; 
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __get_plugin( const char *plugin_name, __plugin *plugin ) {
  // Retrieve plugin
  PyObject *py_plugin_tuple = PyDict_GetItemString( g_py_plugins, plugin_name ? plugin_name : "<null>" );

  if( py_plugin_tuple == NULL ) {
    PyErr_Format( PyExc_LookupError, "undefined plugin: %s", plugin_name );
    return -1;
  }

  if( PyTuple_Check( py_plugin_tuple ) && PyTuple_Size( py_plugin_tuple ) == 3 ) {
    // 0: plugin function
    plugin->function = PyTuple_GET_ITEM( py_plugin_tuple, 0 );
    Py_INCREF( plugin->function );
    // 1: plugin argspec
    plugin->argspec = PyTuple_GET_ITEM( py_plugin_tuple, 1 ); 
    Py_INCREF( plugin->argspec );
    // 2: plugin graph
    plugin->bound_graph = PyTuple_GET_ITEM( py_plugin_tuple, 2 );
    if( PyVGX_Graph_Check( plugin->bound_graph ) ) {
      Py_INCREF( plugin->bound_graph );
    }
    else {
      plugin->bound_graph = NULL;
    }
  }
  
  return 0;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static bool __plugin_param_in_signature( __plugin *plugin, PyObject *py_param_name, __plugin_param *param ) {
  if( !PyDict_GetItem( plugin->argspec, py_param_name ) ) {
    return false;
  }

  param->in_signature = true;
  return true;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyObject * __get_arg_type( PyObject *argspec, const char *argname ) {
  // type  OR  (type,default)
  PyObject *py_arg_info = PyDict_GetItemString( argspec, argname );
  // type is:
  //    NULL if plugin has no such parameter
  //    Py_None if plugin has unannotated parameter
  //    PyTypeObject instance if plugin parameter has annotation
  if( py_arg_info && PyTuple_Check( py_arg_info ) ) {
    return PyTuple_GetItem( py_arg_info, 0 );
  }
  else {
    return py_arg_info;
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __process_uri_parameters( vgx_URIQueryParameters_t *params, __plugin *plugin, PyObject *py_kwargs, PyObject *py_all, vgx_VGXServerResponse_t *response ) {
  // Parameters that are part of the function's argspec are passed in kwargs.
  // Parameters that are not part of the functions's argspec are passed in the 'request' dict.
  vgx_KeyVal_t *cursor = params->keyval;
  vgx_KeyVal_t *kv_end = cursor + params->sz;
  while( cursor < kv_end ) {
    vgx_KeyVal_t *kv = cursor++;
    PyObject *py_arg_type = __get_arg_type( plugin->argspec, kv->key );
    HTTPStatus ret;
    if( py_arg_type ) {
      ret = __plugin__map_keyval_to_kwargs_and_dict( kv, py_arg_type, py_kwargs, py_all );
    }
    else {
      ret = __pyvgx_plugin__map_keyval_to_dict( kv, py_all );
    }
    if( ret ) {
      response->info.http_errcode = ret;
      return -1;
    }
  }
  return 0;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_noop( PyObject *__ign0, int64_t __ign1, const char *__ign2, int64_t __ign3, const char *__ign4, int64_t __ign5 ) {
  return 0;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int __pyvgx_plugin__set_dict_keyval( PyObject *py_dict, int64_t __ign, const char *key, int64_t sz_key, const char *value, int64_t sz_value ) {

  // key
  PyObject *py_key = PyUnicode_FromStringAndSize( key, sz_key );
  if( py_key == NULL ) {
    return -1;
  }

  // value
  PyObject *py_value = PyUnicode_FromStringAndSize( value, sz_value );
  if( py_value == NULL ) {
    PyErr_Clear();
    if( (py_value = PyBytes_FromStringAndSize( value, sz_value )) == NULL ) {
      Py_DECREF( py_key );
      return -1;
    }
  }

  int ret = PyDict_SetItem( py_dict, py_key, py_value );

  Py_DECREF( py_key );
  Py_DECREF( py_value );

  if( ret < 0 ) {
    PyErr_Clear();
  }

  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __set_list_keyval( PyObject *py_list, int64_t i, const char *key, int64_t sz_key, const char *value, int64_t sz_value ) {

  // key
  PyObject *py_key = PyUnicode_FromStringAndSize( key, sz_key );
  if( py_key == NULL ) {
    return -1;
  }

  // value
  PyObject *py_value = PyUnicode_FromStringAndSize( value, sz_value );
  if( py_value == NULL ) {
    PyErr_Clear();
    if( (py_value = PyBytes_FromStringAndSize( value, sz_value )) == NULL ) {
      Py_DECREF( py_key );
      return -1;
    }
  }

  // tuple
  PyObject *py_entry = PyTuple_New(2);
  if( py_entry ) {
    PyTuple_SET_ITEM( py_entry, 0, py_key );
    PyTuple_SET_ITEM( py_entry, 1, py_value );
    PyList_SET_ITEM( py_list, i, py_entry );
    return 0;
  }

  Py_DECREF( py_key );
  Py_DECREF( py_value );

  PyErr_Clear();
  return -1;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static f_keyval_setfunc __get_header_setfunc( PyTypeObject *py_headers_type ) {

  // bytes (DEFAULT) or str
  if( py_headers_type == NULL || py_headers_type == (PyTypeObject*)Py_None || py_headers_type == &PyBytes_Type || py_headers_type == &PyUnicode_Type ) {
    return __set_noop;
  }
  // dict
  else if( py_headers_type == &PyDict_Type ) {
    return __pyvgx_plugin__set_dict_keyval;
  }
  // list of tuples
  else if( py_headers_type == &PyList_Type ) {
    return __set_list_keyval;
  }
  else {
    PyObject *py_repr = PyObject_Repr( (PyObject*)py_headers_type );
    const char *stype = py_repr ? PyUnicode_AsUTF8( py_repr ) : "None";
    PyErr_Format( PyExc_TypeError, "Plugin error: invalid annotation '%s' for 'headers' parameter", stype );
    Py_XDECREF( py_repr );
    return NULL;
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * __pyvgx_plugin__update_object_from_headers( vgx_HTTPHeaders_t *headers, PyObject *py_object, f_keyval_setfunc setfunc ) {
  // For all headers
  for( int64_t i=0; i<headers->sz; i++ ) {
    char *raw_header = headers->list[ i ];
    const char *colon = strstr( raw_header, ":" );
    if( colon ) {
      int64_t sz_field_name = colon - raw_header;
      const char *field_name = lower_inplace( raw_header, sz_field_name ); // NOTE: raw header data mofified in place (lowercased field name)
      const char *field_value = colon+1;
      // skip spaces after colon
      while( *field_value > 0 && *field_value <= 0x20 ) {
        ++field_value;
      }
      // Count value bytes until end of line
      const char *p = field_value;
      while( *p && *p != '\r' ) {
        ++p;
      }
      int64_t sz_field_value = p - field_value;

      // Best effort
      setfunc( py_object, i, field_name, sz_field_name, field_value, sz_field_value );
    }
  }
  return py_object;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyObject * __capture_headers_into_kwargs( vgx_VGXServerRequest_t *request, PyObject *py_argspec, PyObject *py_kwargs ) {
  PyObject *py_headers = NULL;

  PyTypeObject *py_headers_type = (PyTypeObject*)__get_arg_type( py_argspec, "headers" );

  vgx_HTTPHeaders_t *headers = request->headers;

  f_keyval_setfunc setfunc = __get_header_setfunc( py_headers_type );

  // bytes (DEFAULT) or str
  if( setfunc == __set_noop ) {
    if( py_headers_type == &PyBytes_Type ) {
      py_headers = __vgx_PyBytes_FromHTTPHeaders( headers );
    }
    else {
      py_headers = __vgx_PyUnicode_FromHTTPHeaders( headers );
    }
  }
  // dict
  else if( setfunc == __pyvgx_plugin__set_dict_keyval ) {
    py_headers = PyDict_New();
  }
  // list of tuples
  else if( setfunc == __set_list_keyval ) {
    py_headers = PyList_New( headers->sz + 1LL ); // one extra slot for internal header
  }
  else {
    PyObject *py_repr = PyObject_Repr( (PyObject*)py_headers_type );
    const char *stype = py_repr ? PyUnicode_AsUTF8( py_repr ) : "None";
    PyErr_Format( PyExc_TypeError, "Plugin error: invalid annotation '%s' for 'headers' parameter", stype );
    Py_XDECREF( py_repr );
    return NULL;
  }

  if( py_headers == NULL ) {
    return NULL;
  }

  // not default bytes
  if( setfunc != __set_noop ) {

    __pyvgx_plugin__update_object_from_headers( headers, py_headers, setfunc );

    // Add internal headers
    vgx_VGXServerClient_t *client = headers->client;
    const CString_t *CSTR__uri = client->URI->CSTR__uri;
    setfunc( py_headers, headers->sz, "X-Vgx-Builtin-Client", 20, CStringValue( CSTR__uri ), CStringLength( CSTR__uri ) );
  }

  // Assign to kwargs
  if( PyDict_SetItemString( py_kwargs, "headers", py_headers ) < 0 ) {
    Py_DECREF( py_headers );
    return NULL;
  }

  return py_headers;
}



/******************************************************************************
 * 
 *   Register this callback function with the vgx http server. 
 *   The http server can then call this function with the appropriate
 *   arguments to invoke registered plugin functions by name.
 *
 ******************************************************************************
 */
static int __render_result( vgx_VGXServerResponse_t *response, __plugin *plugin, PyObject *py_result ) {
  PyObject *py_plugin_return_type = NULL;
  // Require return type to be a PluginResponse of required by requesting client
  if( response->mediatype == MEDIA_TYPE__application_x_vgx_partial ) {
    if( !PyVGX_PluginResponse_CheckExact( py_result ) ) {
      PyErr_Format( PyExc_TypeError, "plugin must return PluginResponse, not %s", Py_TYPE(py_result)->tp_name );
      return -1;
    }
    py_plugin_return_type = (PyObject*)p_PyVGX_PluginResponseType;
  }
  // Otherwise use plugin-defined return type annotation (NULL or PyTypeObject)
  else if( (py_plugin_return_type = __get_arg_type( plugin->argspec, "return" )) != NULL ) {
    if( !PyObject_TypeCheck( py_result, (PyTypeObject*)py_plugin_return_type ) ) {
      PyErr_Format( PyExc_TypeError, "plugin return type mismatch, expected %R, got %s", py_plugin_return_type, Py_TYPE(py_result)->tp_name );
      return -1;
    }
  }

  iVGXServer.Response.PrepareBody( response ); 

  return iPyVGXCodec.RenderPyObjectByMediatype( response->mediatype, py_plugin_return_type, py_result, response->buffers.content );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __inherit_attributes( PyVGX_PluginResponse *src, PyObject *dest ) {
  if( !PyVGX_PluginResponse_CheckExact( dest ) ) {
    return 0;
  }

  PyVGX_PluginResponse *dest_pr = (PyVGX_PluginResponse*)dest;
  // level
  dest_pr->metas.level.number = src->metas.level.number;

  return 1;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __serialize_to_request_stream( PyVGX_PluginRequest *py_plugreq, vgx_VGXServerRequest_t *request ) {
  // Serialize into the STREAM (we will later swap STREAM and CONTENT because matrix reads CONTENT only)
  iStreamBuffer.Clear( request->buffers.stream );
  return __pyvgx_PluginRequest_Serialize( py_plugreq, request->buffers.stream );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyObject * __exec_post_plugin( const char *post_name, const char *plugin_name, PyVGX_PluginResponse *py_plugres, CString_t **CSTR__error ) {

  __plugin py_postproc = {0};
  PyObject *py_args = NULL;
  PyObject *py_kwargs = NULL;
  PyObject *py_result = NULL;

  XTRY {
    if( __get_plugin( post_name, &py_postproc ) < 0 ) {
      __format_error_string( CSTR__error, "undefined post-processor: %s (%s)", plugin_name, post_name );
      PyErr_Clear();
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( (py_args = PyTuple_New(0)) == NULL || (py_kwargs = PyDict_New()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Assign response to kwargs
    if( PyDict_SetItem( py_kwargs, g_py_param_response, (PyObject*)py_plugres ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Assign graph to kwargs if post-processor plugin definition includes a bound graph.
    if( py_postproc.bound_graph ) {
      if( PyDict_SetItem( py_kwargs, g_py_param_graph, py_postproc.bound_graph ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
    }

    // ----------------------------------------------
    // Execute post-processor plugin
    // ----------------------------------------------
    if( (py_result = PyObject_Call( py_postproc.function, py_args, py_kwargs )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
    }


    // Enforce rule: Post-processor must return PluginResponse or PluginRequest instance if using internal protocol
    if( py_plugres->response->mediatype == MEDIA_TYPE__application_x_vgx_partial ) {
      if( !PyVGX_PluginResponse_CheckExact( py_result ) ) {
        if( PyVGX_PluginRequest_CheckExact( py_result ) ) {
          if( !((PyVGX_PluginRequest*)py_result)->request->headers->control.resubmit ) {
            __format_error_string( CSTR__error, "post-processor must return a resubmittable PluginRequest" );
            THROW_SILENT( CXLIB_ERR_API, 0x006 );
          }
        }
        else {
          __format_error_string( CSTR__error, "post-processor must return PluginResponse, not %s", Py_TYPE(py_result)->tp_name );
          THROW_SILENT( CXLIB_ERR_API, 0x007 );
        }
      }
    }

    // Post processor plugin returned a different object from the response object passed in
    if( py_result != (PyObject*)py_plugres ) {
      // Inherit certain attributes from the original object
      __inherit_attributes( py_plugres, py_result );
    }
  }
  XCATCH( errcode ) {
    if( PyErr_Occurred() ) {
      iPyVGXBuilder.CatchPyExceptionIntoOutput( NULL, NULL, CSTR__error, NULL );
    }
    py_result = NULL;
  }
  XFINALLY {
    Py_XDECREF( py_args );
    Py_XDECREF( py_kwargs );
  }

  // Maybe original PluginResponse instance or maybe a new Python object
  return py_result;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __postprocess( const char *plugin_name, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error ) {

  int ret = 0;

  PyVGX_PluginResponse *py_plugres = NULL;
  PyObject *py_post_result = NULL;
  vgx_VGXServerClient_t *client;
  const char *post_name = NULL;
  char __post_name_buf[32] = {0};
  if( plugin_name ) {
    snprintf( __post_name_buf, 31, ".%llx", CharsHash64( plugin_name ) );
    post_name = __post_name_buf;
  }

  BEGIN_PYTHON_INTERPRETER {

    XTRY {

      // Convert binary x-vgx-partial data to PluginResponse instance
      if( (py_plugres = __pyvgx_PluginResponse_deserialize_x_vgx_partial( request, response )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Capture current dispatcher level into client metas
      if( (client = request->headers->client) != NULL ) {
        client->dispatch_metas.level.number = py_plugres->metas.level.number;
        client->dispatch_metas.level.parts = py_plugres->metas.level.parts;
        client->dispatch_metas.level.deep_parts = py_plugres->metas.level.deep_parts;
      }

      // Execute post-processor if defined
      if( post_name ) {
        if( (py_post_result = __exec_post_plugin( post_name, plugin_name, py_plugres, CSTR__error )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
        }

        // Post processor generated a new request, we will resubmit
        if( PyVGX_PluginRequest_CheckExact( py_post_result ) ) {
          PyVGX_PluginRequest *py_plugreq_resubmit = (PyVGX_PluginRequest*)py_post_result;

          // Assert inner request object is the same
          if( py_plugreq_resubmit->request != request ) {
            THROW_ERROR( CXLIB_ERR_BUG, 0x003 );
          }

          // Serialize to buffer
          // TODO: Ensure CONTENT buffer is either empty or content type is NONE to avoid duplicating
          // any existing content buffer data into the stream buffer.
          if( __serialize_to_request_stream( py_plugreq_resubmit, request ) < 0 ) {
            THROW_SILENT( CXLIB_ERR_API, 0x004 );
          }

          XBREAK;
        }

      }
      else {
        py_post_result = (PyObject*)py_plugres;
        py_plugres = NULL;
      }

      // Clear any resubmit that may have been set previously
      request->headers->control.resubmit = false;

      // Render response instance to output according to mediatype
      if( iPyVGXCodec.RenderPyObjectByMediatype( response->mediatype, NULL, py_post_result, response->buffers.content ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }

    }
    XCATCH( errcode ) {
      if( PyErr_Occurred() ) {
        iPyVGXBuilder.CatchPyExceptionIntoOutput( NULL, NULL, CSTR__error, NULL );
      }
      ret = -1;
    }
    XFINALLY {
      Py_XDECREF( py_plugres );
      Py_XDECREF( py_post_result );
    }
  } END_PYTHON_INTERPRETER;

  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __plugin__init( void ) {
  uint64_t tid = GET_CURRENT_THREAD_ID();
  __lfsr_init( ihash64( tid ) );
  return 0;
}



/******************************************************************************
 * 
 *   Register this callback function with the vgx http server. 
 *   The http server can then call this function with the appropriate
 *   arguments to invoke registered plugin functions by name.
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __plugin__call( const char *plugin_name, bool post, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error ) {

  static __THREAD bool init = false;

  // Init
  if( !init ) {
    init = true;
    __plugin__init();
  }

#define THROW_InternalServerError( ErrorType, ErrorCode, Response ) do { \
  (Response)->info.http_errcode = HTTP_STATUS__InternalServerError; \
  THROW_ERROR( ErrorType, ErrorCode ); \
} WHILE_ZERO


  int err = 0;
  if( post ) {
    if( (err = __postprocess( plugin_name, request, response, CSTR__error )) == 0 ) {
      response->info.execution.nometas = 1;
      return 0;
    }
  }


  int ret = 0;

  __plugin py_plugin = {0};

  struct {
    PyVGX_PluginRequest *py_plugreq;
    __plugin_param content;
    __plugin_param headers;
    __plugin_param method;
  } plugin_param = {0};

  BEGIN_PYTHON_INTERPRETER {

    PyObject *py_plugin_result = NULL;
    PyObject *py_args = NULL;
    PyObject *py_kwargs = NULL;
    PyObject *py_params = NULL;
    PyObject *py_headers = NULL;
    PyObject *py_method = NULL;
    PyObject *py_content = NULL;

    const char *err_wrap = "system";

    XTRY {
      if( err < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      if( (py_args = PyTuple_New(0)) == NULL ||
          (py_kwargs = PyDict_New()) == NULL ||
          (py_params = PyDict_New()) == NULL )
      {
        THROW_InternalServerError( CXLIB_ERR_MEMORY, 0x002, response );
      }

      // Retrieve plugin
      if( __get_plugin( plugin_name, &py_plugin ) < 0 ) {
        response->info.http_errcode = HTTP_STATUS__NotFound;
        err_wrap = "vgx";
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Build call arguments from URI parameters
      //
      // All http parameters received are entered into py_params.
      // Parameters in the plugin function signature are also mapped to py_kwargs.
      if( params && __process_uri_parameters( params, &py_plugin, py_kwargs, py_params, response ) < 0 ) {
        err_wrap = "vgx";
        THROW_SILENT( CXLIB_ERR_API, 0x004 );
      }

      // param: graph
      // 
      // Assign graph to kwargs if plugin definition includes a bound graph.
      if( py_plugin.bound_graph ) {
        if( PyDict_SetItem( py_kwargs, g_py_param_graph, py_plugin.bound_graph ) < 0 ) {
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x005, response );
        }
      }

      // param: headers
      //
      // Populate kwargs 'headers'->dict of headers if function takes an argument named 'headers'.
      // The only way to receive headers is to define the plugin function to take a parameter named 'headers'.
      if( __plugin_param_in_signature( &py_plugin, g_py_param_headers, &plugin_param.headers ) ) {
        if( (py_headers = __capture_headers_into_kwargs( request, py_plugin.argspec, py_kwargs )) == NULL ) {
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x006, response );
        }
      }

      // param: method
      //
      // Populate kwargs 'method'->HTTPMethod if function takes an argument named 'method'.
      // The only way to receive method is to define the plugin function to take a parameter named 'method'.
      if( __plugin_param_in_signature( &py_plugin, g_py_param_method, &plugin_param.method ) ) {
        if( (py_method = PyUnicode_FromString( __vgx_http_request_method( request->method ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x007 );
        }
        // Assign method to kwargs
        if( PyDict_SetItem( py_kwargs, g_py_param_method, py_method ) < 0 ) {
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x008, response );
        }
      }

      // param: content
      // 
      // Select how to represent content
      PyObject *py_content_type = NULL;
      if( __plugin_param_in_signature( &py_plugin, g_py_param_content, &plugin_param.content ) ) {
        // Create object from request content
        py_content_type = __get_arg_type( py_plugin.argspec, "content" ); // maybe NULL
      }
      else {
        // minimal overhead if content parameter is not in plugin signature, use a capsule
        py_content_type = (PyObject*)&PyCapsule_Type;
      }

      // Create content object
      if( (py_content = __plugin__new_request_content( (PyTypeObject*)py_content_type, request )) == NULL ) {
        if( !response->info.http_errcode ) {
          THROW_InternalServerError( CXLIB_ERR_API, 0x009, response );
        }
        THROW_SILENT( CXLIB_ERR_API, 0x00A );
      }

      // Populate kwargs 'content'->bytes if function takes an argument named 'content'.
      if( plugin_param.content.in_signature ) {
        // Assign content to kwargs
        if( PyDict_SetItem( py_kwargs, g_py_param_content, py_content ) < 0 ) {
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x00B, response );
        }
      }

      // param: request
      if( PyDict_GetItem( py_plugin.argspec, g_py_param_request ) ) {
        // Create new PluginRequest
        if( (plugin_param.py_plugreq = __pyvgx_PluginRequest_New( request, py_params, py_headers, py_content )) == NULL ) {
          THROW_InternalServerError( CXLIB_ERR_MEMORY, 0x00C, response );
        }
        // Assign request to kwargs
        if( PyDict_SetItem( py_kwargs, g_py_param_request, (PyObject*)plugin_param.py_plugreq ) < 0 ) {
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x00D, response );
        }
      }

      // ----------------------------------------------
      // Execute plugin
      // ----------------------------------------------
      if( (py_plugin_result = PyObject_Call( py_plugin.function, py_args, py_kwargs )) == NULL ) {
        err_wrap = "plugin";
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00E );
      }


      // ----------------------------------------------
      // Plugin returned a request
      // ----------------------------------------------
      if( PyVGX_PluginRequest_CheckExact( py_plugin_result ) ) {
        PyVGX_PluginRequest *py_plugreq = (PyVGX_PluginRequest*)py_plugin_result;

        if( request->state != VGXSERVER_CLIENT_STATE__PREPROCESS ) {
          err_wrap = "plugin";
          __set_error_string( CSTR__error, "Preprocessor plugin only allowed in dispatcher context (PluginRequest was returned)" );
          THROW_SILENT( CXLIB_ERR_API, 0x00F );
        }

        
        // Serialize to buffer
        if( __serialize_to_request_stream( py_plugreq, request ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_API, 0x010 );
        }
      }
      // ----------------------------------------------
      // Plugin returned a response
      // ----------------------------------------------
      else {
        if( __render_result( response, &py_plugin, py_plugin_result ) < 0 ) {
          err_wrap = "vgx";
          THROW_InternalServerError( CXLIB_ERR_GENERAL, 0x011, response );
        }
        // Execution complete
        response->info.execution.complete = true;
        response->info.execution.nometas = 1;
      }
    }
    XCATCH( errcode ) {
      bool X_VGX_PARTIAL = response->mediatype == MEDIA_TYPE__application_x_vgx_partial;
      // Python generated error
      if( PyErr_Occurred() ) {
        // Stream x-vgx-partial error directly to output
        if( X_VGX_PARTIAL ) {
          iPyVGXBuilder.CatchPyExceptionIntoOutput( err_wrap, &response->mediatype, NULL, response->buffers.content );
        }
        // Create an error string
        else if( CSTR__error ) {
          iPyVGXBuilder.CatchPyExceptionIntoOutput( err_wrap, &response->mediatype, CSTR__error, NULL );
          iVGXServer.Response.PrepareBodyError( response, *CSTR__error );
        }
      }
      // Non-Python generated error
      else {
        CString_t *CSTR__reason = NULL;
        CString_t *CSTR__local = NULL;
        if( CSTR__error == NULL || *CSTR__error == NULL ) {
          CSTR__reason = CSTR__local = CStringNew( "unknown internal error" );
        }
        else {
          CSTR__reason = *CSTR__error;
        }

        if( CSTR__reason ) {
          PyObject *py_str = PyUnicode_FromString( CStringValue( CSTR__reason ) );
          if( py_str ) {
            // Stream x-vgx-partial error directly to output
            if( X_VGX_PARTIAL ) {
              iPyVGXCodec.RenderPyObjectByMediatype( response->mediatype, NULL, py_str, response->buffers.content );
            }
            // Create an error string
            else {
              const char *serr = NULL;
              int64_t sz_serr = 0;
              PyObject *py_media = iPyVGXCodec.ConvertPyObjectByMediatype( response->mediatype, NULL, py_str, &serr, &sz_serr );
              if( serr ) {
                if( CSTR__local ) {
                  iString.Discard( &CSTR__local );
                  CSTR__reason = CSTR__local = CStringNew( serr );
                }
                else {
                  iString.Discard( CSTR__error );
                  *CSTR__error = CSTR__reason = CStringNew( serr );
                }
              }
              Py_XDECREF( py_str );
              Py_XDECREF( py_media );
              iVGXServer.Response.PrepareBodyError( response, CSTR__reason );
            }
          }
        }

        iString.Discard( &CSTR__local );
      }
      // Indicate error already caught and written to output buffer
      if( !response->info.http_errcode ) {
        response->info.http_errcode = HTTP_STATUS__InternalServerError;
      }
      if( (errcode & CXLIB_EXC_TYPE_MASK) != CXLIB_ERR_SILENT ) {
        if( CSTR__error && *CSTR__error ) {
          PYVGX_API_REASON( "plugin", errcode, "[%d] %s", (int)response->info.http_errcode, CStringValue( *CSTR__error ) );
        }
        else {
          PYVGX_API_REASON( "plugin", errcode, "[%d] Internal error", (int)response->info.http_errcode );
        }
      }
      ret = -1;
    }
    XFINALLY {
      Py_XDECREF( py_plugin_result );
      Py_XDECREF( py_args );
      Py_XDECREF( py_kwargs );
      Py_XDECREF( py_params );
      Py_XDECREF( py_headers );
      Py_XDECREF( py_method );
      Py_XDECREF( py_content );

      Py_XDECREF( plugin_param.py_plugreq );
      Py_XDECREF( plugin_param.content.py_obj );
      Py_XDECREF( plugin_param.headers.py_obj );
      Py_XDECREF( plugin_param.method.py_obj );

      Py_XDECREF( py_plugin.function );
      Py_XDECREF( py_plugin.argspec );
      Py_XDECREF( py_plugin.bound_graph );
    }
  } END_PYTHON_INTERPRETER;

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_plugin__init_pyvgx( void ) {
  if( g_init_pyvgx ) {
    return 0;
  }

  if( mod_pyvgx == NULL ) {
    if( __plugin__init_pyvgx( &mod_pyvgx ) < 0 ) {
      return -1;
    }
  }

  const char *types[] = {
    "# PYVGX",
    "# INTERNAL",
    NULL
  };
  const char **tcur = types;
  const char *type;

  while( (type = *tcur++) != NULL ) {
    const char ***py_builtin = PYVGX_PLUGIN_BUILTINS;
    char filename[] = "builtin_plugin_XX";
    char *pfn = filename + 15;
    while( *py_builtin != NULL ) {
      const char **codelines = *py_builtin++;
      const char *first = codelines[0];
      if( first == NULL || !CharsStartsWithConst( first, type ) ) {
        continue;
      }
      ++g_builtin_counter;
      snprintf( pfn, 3, "%02d", g_builtin_counter );
      if( __plugin__execute_python_source( codelines, filename ) < 0 ) {
        return -1;
      }
    }
  }

  g_init_pyvgx = true;
  return 1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_plugin__init_plugins( void ) {
  if( g_py_plugins == NULL ) {
    if( (g_py_plugins = PyDict_New()) == NULL ) {
      return -1;
    }

    if( mod_pyvgx == NULL ) {
      if( __plugin__init_pyvgx( &mod_pyvgx ) < 0 ) {
        return -1;
      }
    }

    if( mod_inspect == NULL ) {
      if( __plugin__init_inspect( &mod_inspect, &meth_signature, &meth_getfullargspec ) < 0 ) {
        return -1;
      }
    }

    if( mod_ast == NULL ) {
      if( __plugin__init_ast( &mod_ast ) < 0 ) {
        return -1;
      }
    }

    g_py_param_request = PyUnicode_FromString( "request" );
    g_py_param_response = PyUnicode_FromString( "response" );
    g_py_param_graph = PyUnicode_FromString( "graph" );
    g_py_param_method = PyUnicode_FromString( "method" );
    g_py_param_headers = PyUnicode_FromString( "headers" );
    g_py_param_content = PyUnicode_FromString( "content" );
    if( !g_py_param_request || !g_py_param_response || !g_py_param_graph || !g_py_param_method || !g_py_param_headers || !g_py_param_content ) {
      return -1;
    }

    // Add builtin plugins
    // Status( [graph[, simple]] )
    PyObject *py_plugin__Status = PyObject_GetAttrString( (PyObject*)_global_system_object, "Status" );
    PyObject *py_plugin_argspec__Status = PyDict_New();
    PyDict_SetItemString( py_plugin_argspec__Status, "graph", Py_None );
    PyDict_SetItemString( py_plugin_argspec__Status, "simple", (PyObject*)&PyLong_Type );

    PyObject *py_plugin_argspec__empty = PyDict_New();

    // GetPlugins( name )
    PyObject *py_plugin__ArgSpec = PyObject_GetAttrString( (PyObject*)_global_system_object, "GetPlugins" );
    PyObject *py_plugin_argspec__ArgSpec = PyDict_New();
    PyDict_SetItemString( py_plugin_argspec__ArgSpec, "plugin", (PyObject*)&PyUnicode_Type );

    // GetPlugins()
    PyObject *py_plugin__GetPlugins = PyObject_GetAttrString( (PyObject*)_global_system_object, "GetPlugins" );

    // GetBuiltins()
    PyObject *py_plugin__GetBuiltins = PyObject_GetAttrString( (PyObject*)_global_system_object, "GetBuiltins" );

    struct __s_plugin {
      const char *name;
      PyObject *py_plugin;
      PyObject *py_argspec;
    } builtin_plugins[] = {
      { .name = "sysplugin__status",       .py_plugin = py_plugin__Status,      .py_argspec = py_plugin_argspec__Status },
      { .name = "sysplugin__argspec",      .py_plugin = py_plugin__ArgSpec,     .py_argspec = py_plugin_argspec__ArgSpec },
      { .name = "sysplugin__plugins",      .py_plugin = py_plugin__GetPlugins,  .py_argspec = py_plugin_argspec__empty },
      { .name = "sysplugin__builtins",     .py_plugin = py_plugin__GetBuiltins, .py_argspec = py_plugin_argspec__empty },
      { NULL, NULL }
    };

    struct __s_plugin *cursor = builtin_plugins;
    while( cursor->py_plugin ) {
      // 3-tuple
      PyObject *py_plugin_tuple = PyTuple_New( 3 );
      if( py_plugin_tuple ) {
        // 0: function
        Py_INCREF( cursor->py_plugin );
        PyTuple_SetItem( py_plugin_tuple, 0, cursor->py_plugin );
        // 1: argspec
        Py_INCREF( cursor->py_argspec );
        PyTuple_SetItem( py_plugin_tuple, 1, cursor->py_argspec );
        // 2: graph is none
        Py_INCREF( Py_None );
        PyTuple_SetItem( py_plugin_tuple, 2, Py_None );

        // Assign
        PyDict_SetItemString( g_py_plugins, cursor->name, py_plugin_tuple );
        Py_DECREF( py_plugin_tuple );
      }

      // Next builtin plugin
      ++cursor;
    }

    Py_XDECREF( py_plugin__Status );
    Py_XDECREF( py_plugin_argspec__Status );
    Py_XDECREF( py_plugin__ArgSpec );
    Py_XDECREF( py_plugin_argspec__ArgSpec );
    Py_XDECREF( py_plugin__GetPlugins );
    Py_XDECREF( py_plugin__GetBuiltins );
    Py_XDECREF( py_plugin_argspec__empty );

    // Add builtin plugins
    char filename[] = "builtin_plugin_XX";
    char *pfn = filename + 15;

    const char *types[] = {
      "# PYVGX",
      "# INTERNAL",
      "# ADMIN",
      "# BUILTIN",
      NULL
    };
    const char **tcur = types;
    const char *type;

    while( (type = *tcur++) != NULL ) {
      if( g_init_pyvgx ) {
        // Already init, skip already defined types
        if( CharsEqualsConst( type, "# PYVGX" ) || CharsEqualsConst( type, "# INTERNAL" ) ) {
          continue;
        }
      }

      const char ***py_builtin = PYVGX_PLUGIN_BUILTINS;
      while( *py_builtin != NULL ) {
        const char **codelines = *py_builtin++;
        const char *first = codelines[0];
        if( first == NULL || !CharsStartsWithConst( first, type ) ) {
          continue;
        }
        ++g_builtin_counter;
        snprintf( pfn, 3, "%02d", g_builtin_counter );
        if( __plugin__execute_python_source( codelines, filename ) < 0 ) {
          return -1;
        }
      }
    }

    return 1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_plugin__delete( void ) {
  if( g_py_plugins != NULL ) {
    Py_DECREF( g_py_plugins );
    g_py_plugins = NULL;
    __plugin__delete_inspect( &mod_inspect, &meth_signature, &meth_getfullargspec );
    __plugin__delete_ast( &mod_ast );
    return 1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * __pyvgx_plugin__get_argspec( PyObject *py_function ) {
  // Get function's full argspec 
  return PyObject_CallMethodObjArgs( mod_inspect, meth_getfullargspec, py_function, NULL );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN f_vgx_ServicePluginCall __pyvgx_plugin__get_call( void ) {
  return __plugin__call;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_plugin__add( const char *plugin_name, vgx_server_plugin_phase phase, PyObject *py_plugin, PyObject *py_bound_graph ) {

#define THROW_INVALID_ANNOTATION( Code, ParameterName, Detail ) \
  PyErr_SetString( PyExc_TypeError, "invalid annotation for parameter '" ParameterName "': " Detail " required" ); \
  THROW_SILENT( CXLIB_ERR_API, Code )

#define THROW_DEFAULT_NOT_ALLOWED( Code, ParameterName ) \
  PyErr_SetString( PyExc_ValueError, "default value not allowed for parameter '" ParameterName "'" ); \
  THROW_SILENT( CXLIB_ERR_API, Code )



  PyObject *py_name = NULL;
  PyObject *py_spec = NULL;
  PyObject *py_plugin_argspec = NULL;

  XTRY {

    const char *p = plugin_name;
    char c = *p++;
    if( !isalpha( c ) && c != '_' ) {
      PyErr_SetString( PyExc_ValueError, "invalid plugin name (must start with [a-zA-Z_])" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }
    while( (c = *p++) != '\0' ) {
      if( !isalnum( c ) && c != '_' ) {
        PyErr_SetString( PyExc_ValueError, "invalid plugin name (must contain only [a-zA-Z0-9_])" );
        THROW_SILENT( CXLIB_ERR_API, 0x002 );
      }
    }

    if( py_bound_graph ) {
      if( !PyVGX_Graph_Check( py_bound_graph ) ) {
        PyErr_SetString( PyExc_TypeError, "a graph instance is required" );
        THROW_SILENT( CXLIB_ERR_API, 0x003 );
      }
    }

    // Has parameter?
    bool has_request = false;
    bool has_response = false;
    bool has_graph = false;

    if( strlen( plugin_name ) > 255 ) {
      PyErr_SetString( PyExc_ValueError, "plugin name too long" );
      THROW_SILENT( CXLIB_ERR_API, 0x006 );
    }

    // This dict will hold a mapping from all function parameter names to their annotations.
    // If a parameter does not have an annotation it will map to None
    if( (py_plugin_argspec = PyDict_New()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x008 );
    }

    // Get function's full argspec 
    if( (py_spec = __pyvgx_plugin__get_argspec( py_plugin )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
    }

    // Make sure we actually got a spec tuple
    if( !PyTuple_Check( py_spec ) || PyTuple_Size( py_spec ) != 7 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
    }

    // Make sure the arcspec is what we think it is
    PyObject *py_args_list = PyTuple_GetItem( py_spec, 0 );
    PyObject *py_defaults_tuple = PyTuple_GetItem( py_spec, 3 );
    PyObject *py_annotations_dict = PyTuple_GetItem( py_spec, 6 );
    if( !py_args_list || !PyList_Check( py_args_list ) || 
        !py_annotations_dict || !PyDict_Check( py_annotations_dict ) ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
    }
    int64_t n_defaults = 0;
    if( py_defaults_tuple && PyTuple_Check( py_defaults_tuple ) ) {
      n_defaults = PyTuple_Size( py_defaults_tuple );
    }

    // Populate dict which will become associated with the plugin
    int64_t n_args = PyList_Size( py_args_list );
    int64_t default_offset = n_args - n_defaults;
    for( int64_t i=0; i<n_args; i++ ) {
      PyObject *py_arg_name = PyList_GetItem( py_args_list, i );
      PyObject *py_arg_default = NULL;
      // Get default for this arg
      int64_t dx = i - default_offset;
      if( dx >= 0 ) {
        py_arg_default = PyTuple_GetItem( py_defaults_tuple, dx );
      }


      PyTypeObject *py_type = (PyTypeObject*)PyDict_GetItem( py_annotations_dict, py_arg_name );
      PyObject *py_tmp;
      const char *arg_name = PyUnicode_AsUTF8( py_arg_name );
      if( arg_name ) {
        // check default value type
        if( py_arg_default && py_type ) {
          if( !iPyVGXCodec.IsTypeJson( (PyObject*)py_type ) ) {
            if( PyObject_Type( py_arg_default ) != (PyObject*)py_type ) {
              PyErr_SetString( PyExc_TypeError, "default parameter value incompatible with annotation" );
              THROW_SILENT( CXLIB_ERR_API, 0x00D );
            }
          }
        }

        // request
        if( CharsEqualsConst( arg_name, "request" ) ) {
          if( py_type == NULL ) {
            py_type = p_PyVGX_PluginRequestType;
          }
          if( py_type != p_PyVGX_PluginRequestType ) {
            THROW_INVALID_ANNOTATION( 0x00E, "request", "pyvgx.PluginRequest" );
          }
          if( py_arg_default ) {
            THROW_DEFAULT_NOT_ALLOWED( 0x00F, "request" );
          }
          has_request = true;
        }
        // response
        else if( CharsEqualsConst( arg_name, "response" ) ) {
          if( py_type == NULL ) {
            py_type = p_PyVGX_PluginResponseType;
          }
          if( py_type != p_PyVGX_PluginResponseType ) {
            THROW_INVALID_ANNOTATION( 0x010, "response", "pyvgx.PluginResponse" );
          }
          if( py_arg_default ) {
            THROW_DEFAULT_NOT_ALLOWED( 0x011, "response" );
          }
          has_response = true;
        }
        // graph
        else if( CharsEqualsConst( arg_name, "graph" ) && py_bound_graph ) {
          if( py_type == NULL ) {
            py_type = p_PyVGX_Graph__GraphType;
          }
          if( py_type != p_PyVGX_Graph__GraphType ) {
            THROW_INVALID_ANNOTATION( 0x012, "graph", "pyvgx.Graph" );
          }
          if( py_arg_default && py_arg_default != Py_None ) { // special case, allow default=None for graph
            THROW_DEFAULT_NOT_ALLOWED( 0x013, "graph" );
          }
          has_graph = true;
        }
        // content
        else if( CharsEqualsConst( arg_name, "content" ) ) {
          if( (py_tmp = __plugin__new_request_content( py_type, NULL )) == NULL ) {
            THROW_SILENT( CXLIB_ERR_API, 0x012 );
          }
          Py_DECREF( py_tmp );
          if( py_arg_default ) {
            THROW_DEFAULT_NOT_ALLOWED( 0x013, "content" );
          }
        }
        // headers
        else if( CharsEqualsConst( arg_name, "headers" ) ) {
          if( py_type == NULL ) {
            py_type = &PyBytes_Type;
          }
          if( __get_header_setfunc( py_type ) == NULL ) {
            THROW_INVALID_ANNOTATION( 0x014, "headers", "bytes, str, dict or list" );
          }
          if( py_arg_default ) {
            THROW_DEFAULT_NOT_ALLOWED( 0x015, "headers" );
          }
        }
        // method
        else if( CharsEqualsConst( arg_name, "method" ) ) {
          if( py_type == NULL ) {
            py_type = &PyUnicode_Type;
          }
          if( py_type != &PyUnicode_Type ) {
            THROW_INVALID_ANNOTATION( 0x016, "method", "str" );
          }
          if( py_arg_default ) {
            THROW_DEFAULT_NOT_ALLOWED( 0x017, "method" );
          }
        }
      }
      if( py_type == NULL ) {
        py_type = (PyTypeObject*)Py_None;
      }

      if( py_arg_default ) {
        PyObject *py_type_and_default = PyTuple_New(2);
        if( py_type_and_default == NULL ) {
          THROW_SILENT( CXLIB_ERR_API, 0x013 );
        }
        PyTuple_SetItem( py_type_and_default, 0, (PyObject*)py_type );
        Py_INCREF( py_type );
        PyTuple_SetItem( py_type_and_default, 1, py_arg_default );
        Py_INCREF( py_arg_default );
        PyDict_SetItem( py_plugin_argspec, py_arg_name, py_type_and_default );
      }
      else {
        PyDict_SetItem( py_plugin_argspec, py_arg_name, (PyObject*)py_type );
      }
    }
    PyObject *py_return_type = PyDict_GetItemString( py_annotations_dict, "return" );
    if( py_return_type ) {
      PyDict_SetItemString( py_plugin_argspec, "return", py_return_type );
    }

    const char *msg_prefix = "";
    const char *required = "";
    switch( phase ) {
    case VGX_SERVER_PLUGIN_PHASE__PRE:
      if( !has_request || (py_bound_graph && !has_graph) ) {
        msg_prefix = "pre-processor";
        required = py_bound_graph ? "'request' and 'graph'" : "'request'";
      }
      break;
    case VGX_SERVER_PLUGIN_PHASE__EXEC:
      if( !has_request || (py_bound_graph && !has_graph) ) {
        msg_prefix = "plugin";
        required = py_bound_graph ? "'request' and 'graph'" : "'request'";
      }
      break;
    case VGX_SERVER_PLUGIN_PHASE__POST:
      if( !has_response || (py_bound_graph && !has_graph) ) {
        msg_prefix = "post-processor";
        required = py_bound_graph ? "'response' and 'graph'" : "'response'";
      }
      break;
    }

    if( strlen( msg_prefix ) > 0 ) {
      PyErr_Format( PyExc_TypeError, "%s function signature incomplete: %s required", msg_prefix, required );
      THROW_SILENT( CXLIB_ERR_API, 0x014 );
    }


    // 0: function
    // 1: dict of parameter names mapping to annotations (or None)
    // 2: graph instance
    PyObject *py_plugin_tuple = PyTuple_New( 3 );
    if( py_plugin_tuple == NULL ) {
      THROW_SILENT( CXLIB_ERR_API, 0x015 );
    }

    // 0: function
    Py_INCREF( py_plugin );
    PyTuple_SetItem( py_plugin_tuple, 0, py_plugin );

    // 1: dict of parameter names mapping to annotations (or None)
    Py_INCREF( py_plugin_argspec );
    PyTuple_SetItem( py_plugin_tuple, 1, py_plugin_argspec );

    // 2: graph instance 
    if( py_bound_graph == NULL ) {
      py_bound_graph = Py_None;
    }
    Py_INCREF( py_bound_graph );
    PyTuple_SetItem( py_plugin_tuple, 2, py_bound_graph );

    // Register plugin existence with server core
    if( iVGXServer.Resource.Plugin.Register( plugin_name, phase ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x016 );
    }

    // Register plugin in Python framework
    // SPECIAL name for post processors

    char post_name[32] = {0};
    const char *reg_name = plugin_name;
    if( phase == VGX_SERVER_PLUGIN_PHASE__POST ) {
      snprintf( post_name, 31, ".%llx", CharsHash64( plugin_name ) );
      reg_name = post_name;
    }

    if( PyDict_SetItemString( g_py_plugins, reg_name, py_plugin_tuple ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x017 );
    }

    Py_DECREF( py_plugin_tuple );

  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
  }
  XFINALLY {
    Py_XDECREF( py_name );
    Py_XDECREF( py_spec );
    Py_XDECREF( py_plugin_argspec );
  }

  if( PyErr_Occurred() ) {
    return -1;
  }

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_plugin__remove( const char *name ) {

  // Unregister plugin in Python framework
  if( PyDict_DelItemString( g_py_plugins, name ) < 0 ) {
    return -1;
  }
  
  // Unregister plugin existence with server core
  if( iVGXServer.Resource.Plugin.Unregister( name ) < 0 ) {
    PyErr_SetString( PyExc_Exception, "unknown internal error" );
    return -1;
  }

  // Try to remove post processor (if we have one)
  char post_name[32] = {0};
  snprintf( post_name, 31, ".%llx", CharsHash64( name ) );
  if( PyDict_GetItemString( g_py_plugins, post_name ) ) {
    if( __pyvgx_plugin__remove( post_name ) < 0 ) {
      return -1;
    }
  }

  return 0;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * __pyvgx_plugin__get_plugins( bool user, const char *onlyname ) {
  PyObject *py_display_list = NULL;
  PyObject *py_plugins = NULL;

  XTRY {
    if( (py_display_list = PyList_New( 0 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( (py_plugins = PyDict_Items( g_py_plugins )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    char plugin_pathbuf[256] = {"/vgx/plugin/"};
    char builtin_pathbuf[256] = {"/vgx/builtin/"};
    char *plugin_pname = plugin_pathbuf + 12;
    char *builtin_pname = builtin_pathbuf + 13;

    // Number of plugins
    int64_t sz_list = PyList_Size( py_plugins );
    for( int64_t i=0; i<sz_list; i++ ) {
      // Get plugin
      PyObject *py_item = PyList_GetItem( py_plugins, i );
      if( PyTuple_Check( py_item ) && PyTuple_Size( py_item ) == 2 ) {
        // 0: Plugin name
        PyObject *py_plugin_name = PyTuple_GetItem( py_item, 0 );
        const char *name = PyUnicode_AsUTF8( py_plugin_name );
        if( name == NULL || CharsStartsWithConst( name, "." ) ) {
          continue;
        }

        bool is_builtin = CharsStartsWithConst( name, "sysplugin__" );

        if( onlyname ) {
          if( is_builtin ) {
            if( !CharsEqualsConst( name+11, onlyname ) ) {
              continue;
            }
          }
          else {
            if( !CharsEqualsConst( name, onlyname ) ) {
              continue;
            }
          }
        }
        else {
          if( user == is_builtin ) {
            continue;
          }
        }

        // 1: Plugin definition
        PyObject *py_plugin_tuple = PyTuple_GetItem( py_item, 1 );
        if( PyTuple_Size( py_plugin_tuple ) != 3 ) {
          continue;
        }
      
        // 0: Plugin definition: function
        PyObject *py_plugin = PyTuple_GetItem( py_plugin_tuple, 0 );
        // 1: Plugin definition: argspec
        PyObject *py_plugin_argspec = PyTuple_GetItem( py_plugin_tuple, 1 );
        // 2: Plugin definition: graph
        PyObject *py_graph = PyTuple_GetItem( py_plugin_tuple, 2 );
        if( !PyUnicode_Check( py_plugin_name ) || !PyFunction_Check( py_plugin ) || !PyDict_Check( py_plugin_argspec ) || !(PyVGX_Graph_Check( py_graph ) || py_graph == Py_None) ) {
          continue;
        }

        PyObject *py_entry = PyDict_New();
        if( py_entry == NULL ) {
          continue;
        }

        //
        // path:
        //
        if( is_builtin ) {
          strncpy( builtin_pname, name+11, 243 );
          iPyVGXBuilder.DictMapStringToString( py_entry, "path", builtin_pathbuf );
        }
        else {
          strncpy( plugin_pname, name, 243 );
          iPyVGXBuilder.DictMapStringToString( py_entry, "path", plugin_pathbuf );
        }

        //
        // description:
        //
        const char *description = NULL;
        PyObject *py_doc = PyObject_GetAttrString( py_plugin, "__doc__" );
        if( py_doc ) {
          if( PyUnicode_Check( py_doc ) && PyUnicode_GET_LENGTH( py_doc ) > 0 ) {
            if( (description = PyUnicode_AsUTF8( py_doc )) != NULL ) {
              CString_t *CSTR__doc = CStringNew( description );
              if( CSTR__doc ) {
                int32_t n_lines = 0;
                CString_t **CSTR__lines = CALLABLE( CSTR__doc )->Split( CSTR__doc, "\n", &n_lines );
                CStringDelete( CSTR__doc );
                if( CSTR__lines ) {
                  PyObject *py_description = PyList_New( 0 );
                  CString_t **CSTR__cursor = CSTR__lines;
                  // Find the indentation level of the least indented line
                  int64_t indent = -1;
                  while( *CSTR__cursor ) {
                    const char *str = CStringValue( *CSTR__cursor );
                    const char *p = str;
                    while( *p == ' ' || *p == '\t' ) {
                      ++p;
                    }
                    if( *p ) {
                      int64_t this_indent = p - str;
                      if( indent < 0 || this_indent < indent ) {
                        indent = this_indent;
                      }
                    }
                    ++CSTR__cursor;
                  }
                  // Create list of doc lines with 
                  if( indent < 0 ) {
                    indent = 0;
                  }
                  CSTR__cursor = CSTR__lines;
                  while( *CSTR__cursor ) {
                    const char *line = CStringValue( *CSTR__cursor );
                    int64_t len = CStringLength( *CSTR__cursor );
                    if( len > indent ) {
                      line += indent;
                      len -= indent;
                    }
                    PyObject *py_line = PyUnicode_FromStringAndSize( line, len );
                    if( py_line ) {
                      if( py_description ) {
                        PyList_Append( py_description, py_line );
                      }
                      else {
                        Py_DECREF( py_line );
                      }
                    }
                    CStringDelete( *CSTR__cursor );
                    ++CSTR__cursor;
                  }
                  free( CSTR__lines );
                  CSTR__lines = NULL;
                  if( py_description ) {
                    iPyVGXBuilder.DictMapStringToPyObject( py_entry, "description", &py_description );
                  }
                  PyErr_Clear();
                }
              }
            }
          }
        }

        if( description == NULL || strlen(description) == 0 ) {
          iPyVGXBuilder.DictMapStringToString( py_entry, "description", "plugin" );
        }
        if( PyErr_Occurred() ) {
          PyErr_Clear();
        }
        Py_XDECREF( py_doc );

        //
        // parameters:
        //
        PyObject *py_argspec_items = PyDict_Items( py_plugin_argspec );
        if( py_argspec_items ) {
          // number of parameters
          int64_t sz_argspec = PyList_Size( py_argspec_items );
          PyObject *py_argspec = PyDict_New();
          if( py_argspec ) {
            // for all parameters
            for( int64_t a=0; a<sz_argspec; a++ ) {
              // (name, type)  OR  (name, (type,default))
              PyObject *py_argspec_tuple = PyList_GetItem( py_argspec_items, a );
              PyObject *py_parameter_name = PyTuple_GetItem( py_argspec_tuple, 0 );
              PyObject *py_parameter_info = PyTuple_GetItem( py_argspec_tuple, 1 );
              PyObject *py_parameter_type = NULL;
              PyObject *py_parameter_default = NULL;
              if( PyTuple_Check( py_parameter_info ) ) {
                py_parameter_type = PyTuple_GetItem( py_parameter_info, 0 );
                py_parameter_default = PyTuple_GetItem( py_parameter_info, 1 );
              }
              else {
                py_parameter_type = py_parameter_info;
              }

              const char *parameter_name = PyUnicode_AsUTF8( py_parameter_name );
              // get type if possible
              const char *parameter_type = __plugin__get_pytype_simple_string( py_parameter_type );
              PyObject *py_type_repr = NULL;
              if( parameter_type == NULL ) {
                // fallback to <class 'type'>
                if( (py_type_repr = PyObject_Repr( py_parameter_type )) != NULL ) {
                  parameter_type = PyUnicode_AsUTF8( py_type_repr );
                }
              }
              // populate output dict (except 'return' which is assigned to its own key in entry)
              if( parameter_name && parameter_type ) {
                if( CharsEqualsConst( parameter_name, "return" ) ) {
                  iPyVGXBuilder.DictMapStringToString( py_entry, "return_type", parameter_type );
                }
                else {
                  if( __plugin__is_pyobj_json_compatible( py_parameter_default ) ) {
                    PyObject *py_type_and_default = PyTuple_New(2);
                    if( py_type_and_default ) {
                      PyTuple_SetItem( py_type_and_default, 0, PyUnicode_FromString( parameter_type ) );
                      PyTuple_SetItem( py_type_and_default, 1, py_parameter_default );
                      Py_INCREF( py_parameter_default );
                      iPyVGXBuilder.DictMapStringToPyObject( py_argspec, parameter_name, &py_type_and_default );
                    }
                  }
                  else {
                    iPyVGXBuilder.DictMapStringToString( py_argspec, parameter_name, parameter_type );
                  }
                }
              }


              Py_XDECREF( py_type_repr );
            }
            
            iPyVGXBuilder.DictMapStringToPyObject( py_entry, "parameters", &py_argspec );
          }
          Py_DECREF( py_argspec_items );
        }

        //
        // bound_graph:
        //
        PyObject *py_graph_repr = PyObject_Repr( py_graph );
        iPyVGXBuilder.DictMapStringToPyObject( py_entry, "bound_graph", &py_graph_repr );

        // Add entry to plugin list
        PyList_Append( py_display_list, py_entry );
      }
    }
    PyErr_Clear();
  }
  XCATCH( errcode ) {
    if( py_display_list ) {
      Py_DECREF( py_display_list );
      py_display_list = NULL;
    }
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "unknown error" );
    }
  }
  XFINALLY {
    Py_XDECREF( py_plugins );
  }

  return py_display_list;
}
