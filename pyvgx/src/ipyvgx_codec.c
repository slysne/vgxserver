/*######################################################################
 *#
 *# ipyvgx_codec.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"
#include "_vxsim.h"
#include <marshal.h>

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );

// constants
static PyObject *g_py_str_compress = NULL;
static PyObject *g_py_str_decompress = NULL;
static PyObject *g_py_int_1 = NULL;
static PyObject *g_py_int_2 = NULL;
static PyObject *g_py_int_4 = NULL;

static PyObject *mod_pickle = NULL;
static PyObject *f_pickle_dumps = NULL;
static PyObject *f_pickle_loads = NULL;

static PyObject *mod_json = NULL;
static PyObject *f_json_dumps = NULL;
static PyObject *f_json_loads = NULL;



static int __create_constants( void );
static void __delete_constants( void );
static PyObject * __new_function_from_code_object( PyObject *py_code, PyObject *py_defaults, PyObject *py_annotations );

// marshal
static PyObject * __marshal_dumps( PyObject *py_object );
static PyObject * __marshal_loads( PyObject *py_marshalled );
static PyObject * __marshal_load( const char *bytes, int64_t sz_bytes );

// _pickle
static int __import_pickle( void );
static PyObject * __pickle_dumps( PyObject *py_object );
static PyObject * __pickle_loads( PyObject *py_pickled );

// json
static int __import_json( void );
static PyObject * __json_dumps( PyObject *py_object );
static PyObject * __json_dump( PyObject *py_object );
static PyObject * __json_loads( PyObject *py_json );
static PyObject * __json_load( const char *bytes, int64_t sz_bytes );


static CString_t *  __new_cstring( const char *data, int64_t sz, int64_t ucsz, object_allocator_context_t *allocator_context, CString_attr attr, bool allow_oversized );
static CString_t *  __new_cstring_from_pystring( PyObject *py_data, object_allocator_context_t *allocator_context, CString_attr attr, bool allow_oversized );
static PyObject *   __new_pybytearray_from_cstring( const CString_t *CSTR__string );
static PyObject *   __new_pybytes_from_cstring( const CString_t *CSTR__string );
static PyObject *   __new_pyunicode_from_cstring( const CString_t *CSTR__string );
static int          __can_pickle( PyObject *py_object );
static CString_t *  _ipyvgx_codec__new_encoded_object_from_pyobject( PyObject *py_key, PyObject *py_object, object_allocator_context_t *allocator_context, bool allow_oversized );
static PyObject *   _ipyvgx_codec__new_pyobject_from_encoded_object( const CString_t *CSTR__encoded, PyObject **py_retkey );
static bool         _ipyvgx_codec__is_cstring_compressible( const CString_t *CSTR__string );
static bool         _ipyvgx_codec__is_pystring_compressible( PyObject *py_string );
static PyObject *   _ipyvgx_codec__new_compressed_pybytes_from_pyobject( PyObject *py_object );
static PyObject *   _ipyvgx_codec__new_pyobject_from_compressed_pybytes( PyObject *py_bytes );
static bool         _ipyvgx_codec__is_type_json( const PyObject *py_type );
static int          _ipyvgx_codec__render_pyobject_by_mediatype( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, vgx_StreamBuffer_t *output );
static PyObject *   _ipyvgx_codec__convert_pyobject_by_mediatype( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, const char **rstr, int64_t *rsz );



/******************************************************************************
 * Equivalent to:
 *
 * >>> dumps = "dumps"
 * >>> loads = "loads"
 * >>> load = "load"
 * >>> compress = "compress"
 * >>> decompress = "decompress"
 * >>> int_1 = 1
 * >>> int_2 = 2
 *
 ******************************************************************************
 */
static int __create_constants( void ) {
  if( g_py_str_compress == NULL ) {
    g_py_str_compress = PyUnicode_FromString( "compress" );
  }
  if( g_py_str_decompress == NULL ) {
    g_py_str_decompress = PyUnicode_FromString( "decompress" );
  }
  if( g_py_int_1 == NULL ) {
    g_py_int_1 = PyLong_FromLong( 1 );
  }
  if( g_py_int_2 == NULL ) {
    g_py_int_2 = PyLong_FromLong( 2 );
  }
  if( g_py_int_4 == NULL ) {
    g_py_int_4 = PyLong_FromLong( 4 );
  }

  if( !g_py_str_compress || !g_py_str_decompress || !g_py_int_1 || !g_py_int_2 || !g_py_int_4 ) {
    return -1;
  }
  else {
    return 0;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static void __delete_constants( void ) {
  if( g_py_str_compress ) {
    Py_DECREF( g_py_str_compress );
    g_py_str_compress = NULL;
  }

  if( g_py_str_decompress ) {
    Py_DECREF( g_py_str_decompress );
    g_py_str_decompress = NULL;
  }

  if( g_py_int_1 ) {
    Py_DECREF( g_py_int_1 );
    g_py_int_1 = NULL;
  }

  if( g_py_int_2 ) {
    Py_DECREF( g_py_int_2 );
    g_py_int_2 = NULL;
  }

  if( g_py_int_4 ) {
    Py_DECREF( g_py_int_4 );
    g_py_int_4 = NULL;
  }

}



/******************************************************************************
 * Equivalent to:
 *
 * >>> types.FunctionType( <code_obj>, globals() )
 *
 ******************************************************************************
 */
static PyObject * __new_function_from_code_object( PyObject *py_code, PyObject *py_defaults, PyObject *py_annotations ) {
  PyObject *py_function = NULL;
  PyObject *py_globals = PyEval_GetGlobals();
  if( py_globals ) {
    Py_INCREF( py_globals );
  }
  else {
    if( (py_globals = PyDict_New()) == NULL ) {
      return NULL;
    }
  }
  py_function = PyFunction_New( py_code, py_globals );
  if( py_defaults && py_defaults != Py_None ) {
    PyFunction_SetDefaults( py_function, py_defaults );
  }
  if( py_annotations && py_annotations != Py_None ) {
    PyFunction_SetAnnotations( py_function, py_annotations );
  }
  Py_DECREF( py_globals );
  return py_function;
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> marshal.dumps( <some_object> )
 *
 ******************************************************************************
 */
static PyObject * __marshal_dumps( PyObject *py_object ) {
  return PyMarshal_WriteObjectToString( py_object, Py_MARSHAL_VERSION );
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> marshal.loads( <marshalled_string> )
 *
 ******************************************************************************
 */
static PyObject * __marshal_loads( PyObject *py_marshalled ) {
  // Execute: >>> marshal.loads( py_marshalled )
  char *bytes;
  Py_ssize_t sz_bytes;
  if( PyBytes_AsStringAndSize( py_marshalled, &bytes, &sz_bytes ) < 0 ) {
    return NULL;
  }
  return PyMarshal_ReadObjectFromString( bytes, sz_bytes );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __marshal_load( const char *bytes, int64_t sz_bytes ) {
  return PyMarshal_ReadObjectFromString( bytes, sz_bytes );
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> import _pickle
 *
 ******************************************************************************
 */
static int __import_pickle( void ) {
  PyObject *py_test_pickled = NULL;
  PyObject *py_test_string = NULL;
  PyObject *py_restored_string = NULL;

  // Only import once
  if( mod_pickle ) {
    return 0;
  }

  XTRY {
    // Import _pickle 
    if( (mod_pickle = PyImport_ImportModule( "_pickle" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x801 );
    }

    if( (f_pickle_loads = PyObject_GetAttrString( mod_pickle, "loads" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x802 );
    }

    if( (f_pickle_dumps = PyObject_GetAttrString( mod_pickle, "dumps" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x803 );
    }

    // Perform _pickle.dumps( "test pickle dump" )
    if( (py_test_string = PyBytes_FromString("test pickle dump")) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x804 );
    }
    if( (py_test_pickled = __pickle_dumps( py_test_string )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x805 );
    }

    // Perform _pickle.loads( <pickled_data> )
    if( (py_restored_string = __pickle_loads( py_test_pickled )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x806 );
    }
    if( PyObject_RichCompareBool( py_test_string, py_restored_string, Py_EQ ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x807 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( mod_pickle );
    mod_pickle = NULL;
  }
  XFINALLY {
    PyVGX_XDECREF( py_test_pickled );
    PyVGX_XDECREF( py_test_string );
    PyVGX_XDECREF( py_restored_string );
  }

  if( mod_pickle ) {
    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> _pickle.dumps( <some_object> )
 *
 ******************************************************************************
 */
static PyObject * __pickle_dumps( PyObject *py_object ) {
  // Execute: >>> _pickle.dumps( py_object )
  PyObject *args[] = {
    py_object,
    g_py_int_2
  };
  return PyObject_Vectorcall( f_pickle_dumps, args, 2, NULL );
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> _pickle.loads( <pickled_string> )
 *
 ******************************************************************************
 */
static PyObject * __pickle_loads( PyObject *py_pickled ) {
  // Execute: >>> _pickle.loads( py_pickled )
  PyObject *args[] = { py_pickled };
  return PyObject_Vectorcall( f_pickle_loads, args, 1, NULL );
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> import json
 *
 ******************************************************************************
 */
static int __import_json( void ) {
  PyObject *py_test_json_string = NULL;
  PyObject *py_test_decoded = NULL;
  PyObject *py_restored_string = NULL;

  // Only import once
  if( mod_json ) {
    return 0;
  }

  XTRY {
    // Import json
    if( (mod_json = PyImport_ImportModule( "json" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( (f_json_loads = PyObject_GetAttrString( mod_json, "loads" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    if( (f_json_dumps = PyObject_GetAttrString( mod_json, "dumps" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Perform json.loads( str )
    const char *json = "{\"x\": \"hello\", \"y\": [1, 2, 3]}";
    if( (py_test_json_string = PyUnicode_FromString( json )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }
    if( (py_test_decoded = __json_loads( py_test_json_string )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Perform json.dumps( decoded )
    if( (py_restored_string = __json_dumps( py_test_decoded )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
    }
    const char *restored = PyUnicode_AsUTF8( py_restored_string );
    if( restored == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }

    // Check
    if( strcmp( json, restored ) ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( mod_json );
    mod_json = NULL;
  }
  XFINALLY {
    PyVGX_XDECREF( py_test_json_string );
    PyVGX_XDECREF( py_test_decoded );
    PyVGX_XDECREF( py_restored_string );
  }

  if( mod_json ) {
    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> json.dumps( <some_object> )
 *
 ******************************************************************************
 */
static PyObject * __json_dumps( PyObject *py_object ) {
  // Execute: >>> json.dumps( py_object )
  PyObject *args[] = { py_object };
  return PyObject_Vectorcall( f_json_dumps, args, 1, NULL );
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> json.dumps( <some_object> ).encode()
 *
 ******************************************************************************
 */
static PyObject * __json_dump( PyObject *py_object ) {
  // Execute: >>> json.dumps( py_object )
  PyObject *args[] = { py_object };
  PyObject *py_json_str = PyObject_Vectorcall( f_json_dumps, args, 1, NULL );
  if( py_json_str == NULL ) {
    return NULL;
  }
  Py_ssize_t sz_bytes;
  const char *bytes = PyUnicode_AsUTF8AndSize( py_json_str, &sz_bytes );
  PyObject *py_json_bytes = PyBytes_FromStringAndSize( bytes, sz_bytes );
  Py_DECREF( py_json_str );
  return py_json_bytes;
}



/******************************************************************************
 * Equivalent to:
 *
 * >>> json.loads( <json_string> )
 *
 ******************************************************************************
 */
static PyObject * __json_loads( PyObject *py_json ) {
  // Execute: >>>  json.loads( py_json )
  PyObject *args[] = { py_json };
  return PyObject_Vectorcall( f_json_loads, args, 1, NULL );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __json_load( const char *bytes, int64_t sz_bytes ) {
  PyObject *py_obj = NULL;
  // TODO: Implement this so we don't have to go via a python string
  PyObject *py_str = PyUnicode_FromStringAndSize( bytes, sz_bytes );
  if( py_str ) {
    py_obj = __json_loads( py_str );
    Py_DECREF( py_str );
  }
  return py_obj;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static CString_t * __new_cstring( const char *data, int64_t sz, int64_t ucsz, object_allocator_context_t *allocator_context, CString_attr attr, bool allow_oversized ) {
  CString_t *CSTR__string = NULL;

  // Large string where malloc is allowed
  if( sz > _VXOBALLOC_CSTRING_MAX_LENGTH && allow_oversized ) {
    allocator_context = NULL;
  }

  // Compress?
  if( sz >= CSTRING_MIN_COMPRESS_SZ ) {
    attr |= CSTRING_ATTR_COMPRESSED;
  }

  // Construct CString
  if( (CSTR__string = CStringNewSizeAttrAlloc( data, (int)sz, (int)ucsz, allocator_context, attr )) == NULL ) {
    // Failed with allocator context, try without allocator context if allowed
    if( allocator_context && allow_oversized ) {
      CSTR__string = CStringNewSizeAttrAlloc( data, (int)sz, (int)ucsz, allocator_context, attr );
    }
  }

  if( CSTR__string ) {
    return CSTR__string;
  }

  // Too large
  char detail[32] = " ";
  char *p = detail;
  if( (attr & CSTRING_ATTR_COMPRESSED) != 0 ) {
    p = write_chars( p, "compressed " );
  }
  if( (attr & CSTRING_ATTR_BYTES) != 0 ) {
    p = write_chars( p, "bytes " );
  }
  write_term( p );
  BEGIN_PYTHON_INTERPRETER {
    PyErr_Format( PyExc_Exception, "Internal %svalue too large (max length is %u, got %lld bytes)", detail, _VXOBALLOC_CSTRING_MAX_LENGTH, sz );
  } END_PYTHON_INTERPRETER;
  return NULL;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static CString_t * __new_cstring_from_pystring( PyObject *py_data, object_allocator_context_t *allocator_context, CString_attr attr, bool allow_oversized ) {

  const char *data;
  Py_ssize_t sz;
  Py_ssize_t ucsz;

  if( PyVGX_PyObject_CheckString( py_data ) ) {
    if( (data = PyVGX_PyObject_AsStringAndSize( py_data, &sz, &ucsz )) == NULL ) {
      return NULL;
    }
  }
  else if( PyByteArray_CheckExact( py_data ) ) {
    data = PyByteArray_AS_STRING( py_data );
    sz = PyByteArray_GET_SIZE( py_data );
    ucsz = 0;
  }
  else {
    PyErr_SetString( PyExc_TypeError, "string or bytes-like object required" );
    return NULL;
  }

  CString_t *CSTR__str;
  BEGIN_PYVGX_THREADS {
    CSTR__str = __new_cstring( data, sz, ucsz, allocator_context, attr, allow_oversized );
  } END_PYVGX_THREADS;
  return CSTR__str;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * __new_pybytearray_from_cstring( const CString_t *CSTR__string ) {
  PyObject *py_bytearray = NULL;
  if( CSTR__string ) {
    py_bytearray = PyByteArray_FromStringAndSize( CStringValue( CSTR__string ), CStringLength( CSTR__string ) );
  }
  return py_bytearray;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * __new_pybytes_from_cstring( const CString_t *CSTR__string ) {
  if( CSTR__string == NULL ) {
    return NULL;
  }
  return  PyBytes_FromStringAndSize( CStringValue( CSTR__string ), CStringLength( CSTR__string ) );
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * __new_pyunicode_from_cstring( const CString_t *CSTR__string ) {
  PyObject *py_string = NULL;
  if( CSTR__string ) {
    py_string = PyUnicode_FromStringAndSize( CStringValue( CSTR__string ), CStringLength( CSTR__string ) );
  }
  return py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __can_pickle( PyObject *py_object ) {
  struct _typeobject *tp = py_object->ob_type;

  // NOTE: This is not complete, because we could have nested objects.
  // Find a better way to bullet-proof things.

  if( tp == p_PyVGX_Graph__GraphType ||
      tp == p_PyVGX_Vertex__VertexType ||
      tp == p_PyVGX_Vector__VectorType ||
      tp == p_PyVGX_Similarity__SimilarityType ||
      tp == p_PyVGX_Memory__MemoryType
    )
  {
    PyErr_Format( PyExc_ValueError, "Objects of type '%s' cannot be encoded.", py_object->ob_type->tp_name );
    return 0;
  }
  else {
    return 1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __new_keyval( PyObject *py_key, PyObject *py_val, CString_attr *attr ) {
  PyObject *py_keyval = PyTuple_New( 2 );
  if( py_keyval ) {
    PyTuple_SET_ITEM( py_keyval, 0, py_key );
    Py_INCREF( py_key );
    PyTuple_SET_ITEM( py_keyval, 1, py_val );
    Py_INCREF( py_val );
    PyObject *py_serialized = __pickle_dumps( py_keyval );
    Py_DECREF( py_keyval );
    if( py_serialized ) {
      *attr |= CSTRING_ATTR_KEYVAL;
      return py_serialized;
    }
  }
  return NULL;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __encode_bytearray( PyObject *py_key, PyObject *py_bytearray, CString_t **CSTR__encoded_bytearray, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  if( !PyByteArray_CheckExact( py_bytearray ) ) {
    return 0;
  }

  CString_attr attr = CSTRING_ATTR_BYTEARRAY;

  PyObject *py_encoded;

  if( py_key ) {
    py_encoded = __new_keyval( py_key, py_bytearray, &attr );
  }
  else {
    py_encoded = py_bytearray;
    Py_INCREF( py_encoded );
  }

  if( py_encoded ) {
    *CSTR__encoded_bytearray = __new_cstring_from_pystring( py_encoded, allocator_context, attr, allow_oversized );
    Py_DECREF( py_encoded );
    if( *CSTR__encoded_bytearray ) {
      return 1;
    }
  }

  return -1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __encode_string( PyObject *py_key, PyObject *py_string, CString_t **CSTR__encoded_string, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  if( !PyVGX_PyObject_CheckString( py_string ) ) {
    return 0;
  }

  CString_attr attr = CSTRING_ATTR_NONE;

  // Python object is bytes, set flag
  if( PyBytes_CheckExact( py_string ) ) {
    attr |= CSTRING_ATTR_BYTES;
  }

  // Encode as keyval
  PyObject *py_encoded = py_string;
  if( py_key ) {
    if( (py_encoded = __new_keyval( py_key, py_string, &attr )) == NULL ) {
      return -1;
    }
  }
  // No key
  else {
    Py_INCREF( py_encoded );
  }

  // Convert to cstring
  *CSTR__encoded_string = __new_cstring_from_pystring( py_encoded, allocator_context, attr, allow_oversized );
  Py_DECREF( py_encoded );

  if( *CSTR__encoded_string == NULL ) {
    return -1;
  }

  return 1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __encode_callable( PyObject *py_key, PyObject *py_callable, CString_t **CSTR__encoded, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  if( !PyFunction_Check( py_callable ) ) {
    return 0;
  }

  if( PyFunction_GetClosure( py_callable ) != NULL ) {
    PyErr_SetString( PyExc_ValueError, "Functions with closures cannot be encoded" );
    return -1;
  }

  CString_attr attr = CSTRING_ATTR_CALLABLE;

  // Extract the code from the function
  PyObject *py_func_code = PyFunction_GetCode( py_callable ); // borrowed ref.
  // Extract default parameters from the function
  PyObject *py_func_defaults = PyFunction_GetDefaults( py_callable ); // borrowed
  // Extract annotations from the function
  PyObject *py_func_annotations = PyFunction_GetAnnotations( py_callable ); // borrowed

  // Make sure we can proceed with the serialization
  if( !(py_func_code && (py_func_defaults == NULL || __can_pickle( py_func_defaults )) && (py_func_annotations == NULL || __can_pickle( py_func_annotations ))) ) {
    return -1;
  }

  PyObject *py_serialized = NULL;
  // Packed format is always a 3-tuple (code, defaults, annotations)
  PyObject *py_triple = PyTuple_New( 3 );
  if( !py_triple ) {
    return -1;
  }
  // Marshal function code
  PyObject *py_bytes = PyMarshal_WriteObjectToString( py_func_code, Py_MARSHAL_VERSION );
  if( !py_bytes ) {
    Py_DECREF( py_triple );
    return -1;
  }

  // Code in slot 0
  PyTuple_SET_ITEM( py_triple, 0, py_bytes );

  // Defaults (or None) in slot 1
  if( !py_func_defaults ) {
    py_func_defaults = Py_None;
  }
  Py_INCREF( py_func_defaults );
  PyTuple_SET_ITEM( py_triple, 1, py_func_defaults );

  // Annotations (or None) in slot 2
  if( !py_func_annotations ) {
    py_func_annotations = Py_None;
  }
  Py_INCREF( py_func_annotations );
  PyTuple_SET_ITEM( py_triple, 2, py_func_annotations );

  // Keyval packing
  if( py_key ) {
    py_serialized = __new_keyval( py_key, py_triple, &attr );
  }
  else {
    py_serialized = __pickle_dumps( py_triple );
  }
  Py_DECREF( py_triple );

  if( py_serialized == NULL ) {
    return -1;
  }

  // Convert to cstring
  *CSTR__encoded = __new_cstring_from_pystring( py_serialized, allocator_context, attr, allow_oversized );
  Py_DECREF( py_serialized );

  if( *CSTR__encoded ) {
    return 1;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __encode_pickled( PyObject *py_key, PyObject *py_object, CString_t **CSTR__encoded, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  if( !__can_pickle( py_object ) ) {
    return 0;
  }

  CString_attr attr = CSTRING_ATTR_SERIALIZED_TXT;

  PyObject *py_serialized;

  // Keyval packing
  if( py_key ) {
    py_serialized = __new_keyval( py_key, py_object, &attr );
  }
  else {
    py_serialized = __pickle_dumps( py_object );
  }

  if( py_serialized == NULL ) {
    return -1;
  }

  // Convert to cstring
  *CSTR__encoded = __new_cstring_from_pystring( py_serialized, allocator_context, attr, allow_oversized );
  Py_DECREF( py_serialized );

  if( *CSTR__encoded ) {
    return 1;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __set_cstring_qword_int( QWORD *data, int64_t i, PyObject *py_int_value ) {
  int64_t *p = (int64_t*)data + i;
  if( PyLong_CheckExact( py_int_value ) ) {
    if( (*p = PyLong_AsLongLong( py_int_value )) == -1 && PyErr_Occurred() ) {
      return -1;
    }
    return 1;
  }
  else {
    return 0;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __set_cstring_qword_float( QWORD *data, int64_t i, PyObject *py_float_value ) {
  double *p = (double*)data + i;
  if( PyFloat_CheckExact( py_float_value ) ) {
    *p = PyFloat_AS_DOUBLE( py_float_value );
    return 1;
  }
  else {
    return 0;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __encode_num_list( PyObject *py_key, PyObject *py_object, CString_t **CSTR__encoded, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  static const int64_t maxlen = _VXOBALLOC_CSTRING_MAX_LENGTH >> 3;

  int64_t sz = 0;
  if( PyList_CheckExact( py_object ) ) {
    sz = PyList_GET_SIZE( py_object );
  }
  else {
    return 0; // not a list
  }

  if( sz > maxlen ) {
    if( allow_oversized ) {
      allocator_context = NULL;
    }
    else {
      if( PyNumber_Check( PyList_GET_ITEM( py_object, 0 ) ) ) {
        PyErr_SetString( PyExc_ValueError, "too many values" );
        return -1;
      }
      else {
        return 0; // not a numeric list
      }
    }
  }

  // Construct destination string
  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = (int32_t)sz * 8,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = allocator_context
  };
  CString_t *CSTR__list;
  if( (CSTR__list = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
    PyErr_SetString( PyExc_MemoryError, "internal error" );
    return -1;
  }
  QWORD *qdata = CALLABLE( CSTR__list )->ModifiableQwords( CSTR__list );
  if( qdata == NULL ) {
    iString.Discard( &CSTR__list );
    PyErr_SetString( PyExc_AssertionError, "internal error" );
    return -1;
  }

  int is_encoded = 0;
  CString_attr attr = CSTRING_ATTR_ARRAY_INT;

  PyObject *py_item;
  if( sz > 0 ) {
    py_item = PyList_GET_ITEM( py_object, 0 );
    if( PyFloat_CheckExact( py_item ) ) {
      attr = CSTRING_ATTR_ARRAY_FLOAT;
    }
    else if( !PyLong_CheckExact( py_item ) ) {
      iString.Discard( &CSTR__list );
      return 0; // not a list of numbers
    }
  }

  XTRY {
    int r;
    for( int64_t i=0; i<sz; i++ ) {
      py_item = PyList_GET_ITEM( py_object, i );
      if( attr == CSTRING_ATTR_ARRAY_FLOAT ) {
        r = __set_cstring_qword_float( qdata, i, py_item );
      }
      else {
        r = __set_cstring_qword_int( qdata, i, py_item );
      }

      // can't be encoded as internal array, or error
      if( r <= 0 ) {
        // error
        if( r < 0 ) {
          is_encoded = -1;
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
    }

    if( py_key == NULL ) {
      is_encoded = 1;
      CStringAttributes( CSTR__list ) = attr;
      /* // Can't compress internal array because vxeval can't decompress on the fly
      if( CStringLength( CSTR__list ) >= CSTRING_MIN_COMPRESS_SZ ) {
        CString_t *CSTR__compressed = CALLABLE( CSTR__list )->Compress( CSTR__list, CSTRING_COMPRESS_MODE_LZ4 );
        if( CSTR__compressed ) {
          iString.Discard( &CSTR__list );
          CSTR__list = CSTR__compressed;
        }
      }
      */
    }
    else {
      is_encoded = -1;
      PyObject *py_tmp = PyBytes_FromStringAndSize( CStringValue( CSTR__list ), CStringLength( CSTR__list ) );
      iString.Discard( &CSTR__list );
      if( py_tmp == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }
      PyObject *py_bytes = __new_keyval( py_key, py_tmp, &attr );
      Py_DECREF( py_tmp );
      if( py_bytes == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      if( (CSTR__list = __new_cstring_from_pystring( py_bytes, allocator_context, attr, allow_oversized )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
      is_encoded = 1;
    }

    // Return
    *CSTR__encoded = CSTR__list;
  }
  XCATCH( errcode ) {
    iString.Discard( &CSTR__list );
  }
  XFINALLY {
  }

  return is_encoded;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __encode_num_mapping( PyObject *py_key, PyObject *py_object, CString_t **CSTR__encoded, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  static const uint64_t maxlen = _VXOBALLOC_CSTRING_MAX_LENGTH >> 3;

  if( !PyDict_CheckExact( py_object ) ) {
    return 0;
  }

  // Make sure all items are int:num
  Py_ssize_t pos = 0;
  PyObject *py_intkey;
  PyObject *py_numval;
  while( PyDict_Next( py_object, &pos, &py_intkey, &py_numval ) ) {
    if( !PyLong_CheckExact( py_intkey ) || !PyNumber_Check( py_numval ) ) {
      return 0;
    }
  }


  // Number of mapped values
  int64_t sz = PyDict_Size( py_object );

  // The hash table must have size power-of-2 so we can apply a simple
  // bitmask to the hashkey to find the offset.
  //
  // We need a memory array larger than the number of mapped elements
  // in order to avoid too many collisions and linear probes.
  //
  // We need one slot at the start to hold map metas in a QWORD
  //
  // 
  //
  //
  //
  //

  static const double maxfill = 0.66;



  // Minimum number of cells in hash table to not exceed max fillrate
  uint64_t hsize_min = (uint64_t)(sz / maxfill) + 1; // at least one cell

  // Next power-of-two
  uint64_t hsize = ipow2log2( hsize_min );

  // Total size includes header plus one unused cell at the end
  uint64_t ncells = hsize + 2;

  if( ncells > maxlen ) {
    vgx_cstring_array_map_header_t H;
    uint64_t max_oversized = 1ULL << (8*sizeof( H.mask ));
    if( allow_oversized && ncells <= max_oversized ) {
      allocator_context = NULL;
    }
    else {
      PyErr_SetString( PyExc_ValueError, "too many mapped values" );
      return -1;
    }
  }

  // Construct destination string
  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = (int32_t)ncells * 8,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = allocator_context
  };
  CString_t *CSTR__map;
  if( (CSTR__map = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
    PyErr_SetString( PyExc_MemoryError, "internal error" );
    return -1;
  }
  QWORD *qdata = CALLABLE( CSTR__map )->ModifiableQwords( CSTR__map );
  if( qdata == NULL ) {
    iString.Discard( &CSTR__map );
    PyErr_SetString( PyExc_AssertionError, "internal error" );
    return -1;
  }

  // Initialize array header
  vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)qdata;
  header->mask = (uint16_t)hsize - 1;
  header->items = (uint16_t)sz;
  header->_collisions = 0;

  // Initialize array cells to unoccupied
  vgx_cstring_array_map_cell_t *htable = (vgx_cstring_array_map_cell_t*)qdata+1; // start of hash table
  vgx_cstring_array_map_cell_t *cell = htable;
  vgx_cstring_array_map_cell_t *end = htable + hsize;
  while( cell < end ) {
    cell->key = VGX_CSTRING_ARRAY_MAP_KEY_NONE;
    cell->value = 0.0;
    ++cell;
  }

  int is_encoded = 0;
  CString_attr attr = CSTRING_ATTR_ARRAY_MAP;

  XTRY {

    // Iterate over all items in dict and insert into hashtable
    pos = 0;
    while( PyDict_Next( py_object, &pos, &py_intkey, &py_numval ) ) {
      int ovf;
      int key = PyLong_AsLongAndOverflow( py_intkey, &ovf );
      float val = (float)PyFloat_AsDouble( py_numval );
      if( ovf != 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      else if( vgx_cstring_array_map_set( qdata, key, val ) == NULL ) {
        PyErr_SetString( PyExc_Exception, "mapping space exhausted" );
        is_encoded = -1;
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

    if( py_key == NULL ) {
      is_encoded = 1;
      CStringAttributes( CSTR__map ) = attr;
      /* // Can't compress internal map because vxeval can't decompress on the fly
      if( CStringLength( CSTR__map ) >= CSTRING_MIN_COMPRESS_SZ ) {
        CString_t *CSTR__compressed = CALLABLE( CSTR__map )->Compress( CSTR__map, CSTRING_COMPRESS_MODE_LZ4 );
        if( CSTR__compressed ) {
          iString.Discard( &CSTR__map );
          CSTR__map = CSTR__compressed;
        }
      }
      */
    }
    else {
      is_encoded = -1;
      PyObject *py_tmp = PyBytes_FromStringAndSize( CStringValue( CSTR__map ), CStringLength( CSTR__map ) );
      iString.Discard( &CSTR__map );
      if( py_tmp == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
      PyObject *py_bytes = __new_keyval( py_key, py_tmp, &attr );
      Py_DECREF( py_tmp );
      if( py_bytes == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }

      if( (CSTR__map = __new_cstring_from_pystring( py_bytes, allocator_context, attr, allow_oversized )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }
      is_encoded = 1;
    }

    // Return
    *CSTR__encoded = CSTR__map;
  }
  XCATCH( errcode ) {
    iString.Discard( &CSTR__map );
  }
  XFINALLY {
  }

  return is_encoded;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static CString_t * _ipyvgx_codec__new_encoded_object_from_pyobject( PyObject *py_key, PyObject *py_object, object_allocator_context_t *allocator_context, bool allow_oversized ) {
  CString_t *CSTR__encoded = NULL;

  // Bytearray
  if( __encode_bytearray( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Unicode or Bytes
  else if( __encode_string( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Python object is a list of numeric objects
  else if( __encode_num_list( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Python object is a dict mapping integer keys to numeric values
  else if( __encode_num_mapping( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Python object is a function. Marshal it and possibly compress it.
  else if( __encode_callable( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Python object other than function. Pickle it and possibly compress it.
  else if( __encode_pickled( py_key, py_object, &CSTR__encoded, allocator_context, allow_oversized ) ) {
    return CSTR__encoded;
  }
  // Unsupported object
  else {
    return NULL;
  }
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * __decode_callable( PyObject *py_triple ) {

  // Unpickle to get the 3-tuple (code, defaults, annotation)
  if( py_triple == NULL || !PyTuple_Check( py_triple ) || PyTuple_GET_SIZE( py_triple ) != 3 ) {
    return NULL;
  }

  PyObject *py_callable = NULL;

  PyObject *py_code = NULL;
  PyObject *py_defaults = NULL;
  PyObject *py_annotations = NULL;
  // 0: code
  // 1: defaults
  // 2: annotations
  PyObject *py_code_bytes = PyTuple_GET_ITEM( py_triple, 0 );
  if( PyBytes_CheckExact( py_code_bytes ) ) {
    py_code = PyMarshal_ReadObjectFromString( PyBytes_AS_STRING( py_code_bytes ), PyBytes_GET_SIZE( py_code_bytes ) );
  }
  py_defaults = PyTuple_GET_ITEM( py_triple, 1 );
  Py_INCREF( py_defaults );
  py_annotations = PyTuple_GET_ITEM( py_triple, 2 );
  Py_INCREF( py_annotations );

  // Create function instance from code, defaults and annotations
  if( py_code ) {
    py_callable = __new_function_from_code_object( py_code, py_defaults, py_annotations );
    Py_DECREF( py_code );
  }
  Py_XDECREF( py_defaults );
  Py_XDECREF( py_annotations );

  return py_callable;
}





/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_codec__new_pyobject_from_encoded_object( const CString_t *CSTR__encoded, PyObject **py_retkey ) {
  char PALIGNED_ decomp_buffer[ARCH_PAGE_SIZE];

  if( CSTR__encoded == NULL ) {
    return NULL;
  }

  PyObject *py_object = NULL;
    
  const char *data = CStringValue( CSTR__encoded );
  int32_t len = CStringLength( CSTR__encoded );
  int32_t uclen = CStringCodepoints( CSTR__encoded );
  CString_attr attr = CStringAttributes( CSTR__encoded );

  PyObject *py_key = NULL;
  PyObject *py_bytes = NULL;
  PyObject *py_exc = NULL;
  const char *serr = NULL;

  char *decompressed = NULL;
  Py_ssize_t sz = 0;

  bool keyval = false;

  XTRY {

    // If compressed, decompress it first
    if( (attr & CSTRING_ATTR_COMPRESSED) ) {
      int r;
      BEGIN_PYVGX_THREADS {
        int32_t csz = len;
        r = CStringDecompressBytesToBytes( data, csz, decomp_buffer, ARCH_PAGE_SIZE, &decompressed, &len, &uclen, &attr );
      } END_PYVGX_THREADS;
      if( r < 0 ) {
        py_exc = PyExc_MemoryError;
        serr = "decompress error";
        THROW_SILENT( CXLIB_ERR_MEMORY, 0x001 );
      }
      data = decompressed;
    }

    // Unpack keyval
    if( (attr & CSTRING_ATTR_KEYVAL) ) {
      attr ^= CSTRING_ATTR_KEYVAL; // remove keyval bit
      keyval = true;

      // We need a PyBytes object to unpickle
      if( (py_bytes = PyBytes_FromStringAndSize( data, len )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Unpickle to get the tuple (key, val)
      PyObject *py_tuple = __pickle_loads( py_bytes );
      Py_DECREF( py_bytes );
      if( py_tuple == NULL || !PyTuple_Check( py_tuple ) || PyTuple_GET_SIZE( py_tuple ) != 2 ) {
        if( !PyErr_Occurred() ) {
          py_exc = PyExc_Exception;
          serr = "bad internal keyval encoding";
        }
        Py_XDECREF( py_tuple );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Own the key
      py_key = PyTuple_GET_ITEM( py_tuple, 0 );
      Py_INCREF( py_key );

      // Own the value
      py_bytes = PyTuple_GET_ITEM( py_tuple, 1 );
      Py_INCREF( py_bytes );

      // We don't know what's inside py_bytes
      data = NULL;
      len = 0;
      uclen = 0;

      // Discard the tuple
      Py_DECREF( py_tuple );
    }

    switch( attr ) {
    case CSTRING_ATTR_NONE:
      if( data ) {
        py_object = PyUnicode_FromStringAndSize( data, len );
      }
      else if( (py_object = py_bytes) != NULL ) {
        if( PyUnicode_Check( py_object ) ) {
          py_bytes = NULL; // stolen
        }
        else {
          py_object = PyUnicode_FromEncodedObject( py_bytes, "utf-8", NULL ); 
        }
      }
      break;
    case CSTRING_ATTR_SERIALIZED_TXT:
      // We need PyBytes to unpickle
      if( data ) {
        py_bytes = PyBytes_FromStringAndSize( data, len );
      }
      if( py_bytes ) {
        if( keyval ) {
          py_object = py_bytes; // already unpickled, steal
          py_bytes = NULL;
        }
        else {
          py_object = __pickle_loads( py_bytes );
        }
      }
      break;
    case CSTRING_ATTR_BYTES:
      if( data ) {
        py_object = PyBytes_FromStringAndSize( data, len );
      }
      else if( (py_object = py_bytes) != NULL ) {
        if( PyBytes_CheckExact( py_object ) ) {
          py_bytes = NULL; // stolen
        }
        else {
          py_object = PyBytes_FromObject( py_bytes );
        }
      }
      break;
    case CSTRING_ATTR_BYTEARRAY:
      if( data ) {
        py_object = PyByteArray_FromStringAndSize( data, len );
      }
      else if( (py_object = py_bytes) != NULL ) {
        py_bytes = NULL; // stolen
      }
      break;
    case CSTRING_ATTR_CALLABLE:
      if( py_bytes == NULL ) {
        py_bytes = PyBytes_FromStringAndSize( data, len );
      }
      if( py_bytes ) {
        PyObject *py_triple;
        if( keyval ) {
          py_triple = py_bytes; // already unpickled, steal
          py_bytes = NULL;
        }
        else {
          py_triple = __pickle_loads( py_bytes );
        }
        if( py_triple ) {
          py_object = __decode_callable( py_triple );
          Py_DECREF( py_triple );
        }
      }
      break;
    case CSTRING_ATTR_ARRAY_INT:
    case CSTRING_ATTR_ARRAY_FLOAT:
    case CSTRING_ATTR_ARRAY_MAP:
      if( py_bytes ) {
        if( PyBytes_AsStringAndSize( py_bytes, (char**)&data, &sz ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
        }
        len = (int)sz;
      }
      if( data ) {
        if( attr == CSTRING_ATTR_ARRAY_INT ) {
          py_object = iPyVGXBuilder.NumberListFromBytes( data, len, false );
        }
        else if( attr == CSTRING_ATTR_ARRAY_FLOAT ) {
          py_object = iPyVGXBuilder.NumberListFromBytes( data, len, true );
        }
        else {
          py_object = iPyVGXBuilder.NumberMapFromBytes( data );
        }
      }
      break;
    default:
      break;
    }

    if( !py_object ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
    }

    if( py_object && py_retkey ) {
      *py_retkey = py_key; // steal for return
      py_key = NULL;
    }

  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      if( py_exc && serr ) {
        PyErr_SetString( py_exc, serr );
      }
      else {
        PyErr_SetString( PyExc_Exception, "unknown decoder error" );
      }
    }

    Py_XDECREF( py_object );
    py_object = NULL;
  }
  XFINALLY {
    if( decompressed && decompressed != decomp_buffer ) {
      ALIGNED_FREE( decompressed );
    }
    Py_XDECREF( py_key );
    Py_XDECREF( py_bytes );
  }

  return py_object;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
static PyObject * ORIG_ipyvgx_codec__new_pyobject_from_encoded_object( const CString_t *CSTR__encoded, PyObject **py_retkey ) {
  char PALIGNED_ decomp_buffer[ARCH_PAGE_SIZE];

  if( CSTR__encoded == NULL ) {
    return NULL;
  }
    
  CString_attr attr = CStringAttributes( CSTR__encoded );

  PyObject *py_key = NULL;
  PyObject *py_bytes = NULL;

  // If compressed, decompress it first
  bool compressed = false;
  if( attr & CSTRING_ATTR_COMPRESSED ) {
    attr ^= CSTRING_ATTR_COMPRESSED; // remove compressed bit
    compressed = true;

    char *rdata;
    int rsz;
    int rucsz;
    CString_attr rattr;
    int r;
    BEGIN_PYVGX_THREADS {
      r = CALLABLE( CSTR__encoded )->DecompressToBytes( CSTR__encoded, decomp_buffer, ARCH_PAGE_SIZE, &rdata, &rsz, &rucsz, &rattr );
    } END_PYVGX_THREADS;
    if( r < 0 ) {
      PyErr_SetString( PyExc_MemoryError, "decompress error" );
      return NULL;
    }

    py_bytes = PyBytes_FromStringAndSize( rdata, rsz );
    if( rdata != decomp_buffer ) {
      ALIGNED_FREE( rdata );
    }
    if( py_bytes == NULL ) {
      return NULL;
    }
  }

  // Unpack keyval
  bool keyval = false;
  if( attr & CSTRING_ATTR_KEYVAL ) {
    attr ^= CSTRING_ATTR_KEYVAL; // remove keyval bit
    keyval = true;
    // Convert cstring to bytes if we didn't decompress above
    if( py_bytes == NULL ) {
      if( (py_bytes = __new_pybytes_from_cstring( CSTR__encoded )) == NULL ) {
        return NULL;
      }
    }
    // Unpickle to get the tuple (key, val)
    PyObject *py_tuple = __pickle_loads( py_bytes );
    Py_DECREF( py_bytes );
    if( py_tuple == NULL || !PyTuple_Check( py_tuple ) || PyTuple_GET_SIZE( py_tuple ) != 2 ) {
      if( !PyErr_Occurred() ) {
        PyErr_SetString( PyExc_Exception, "bad internal encoding" );
      }
      Py_XDECREF( py_tuple );
      return NULL;
    }

    py_key = PyTuple_GET_ITEM( py_tuple, 0 );
    Py_INCREF( py_key );

    py_bytes = PyTuple_GET_ITEM( py_tuple, 1 );
    Py_INCREF( py_bytes );

    Py_DECREF( py_tuple );
  }

  // Data was compressed and/or a keyval pair, create a new cstring for further processing
  CString_t *CSTR__bytes = NULL;
  if( py_bytes && ( (attr & CSTRING_ATTR_BYTEARRAY) || (attr & __CSTRING_ATTR_ARRAY_MASK) ) ) {
    CSTR__bytes = __new_cstring_from_pystring( py_bytes, NULL, attr, true );
    Py_DECREF( py_bytes );
    py_bytes = NULL;
    if( (CSTR__encoded = CSTR__bytes) == NULL ) {
      Py_XDECREF( py_key );
      return NULL;
    }
  }

  PyObject *py_object = NULL;

  // Bytearray
  if( attr & CSTRING_ATTR_BYTEARRAY ) {
    py_object = __new_pybytearray_from_cstring( CSTR__encoded );
  }
  // Bytes
  else if( attr == CSTRING_ATTR_BYTES ) {
    if( py_bytes ) {
      if( PyBytes_CheckExact( py_bytes ) ) {
        py_object = py_bytes; // already bytes, steal
        py_bytes = NULL;
      }
      else {
        py_object = PyBytes_FromObject( py_bytes );
      }
    }
    else {
      py_object = __new_pybytes_from_cstring( CSTR__encoded );
    }
  }
  // Unicode string
  else if( attr == CSTRING_ATTR_NONE ) {
    if( py_bytes ) {
      if( PyUnicode_Check( py_bytes ) ) {
        py_object = py_bytes; // already unicode, steal
        py_bytes = NULL;
      }
      else {
        py_object = PyUnicode_FromEncodedObject( py_bytes, "utf-8", NULL ); 
      }
    }
    else {
      py_object = __new_pyunicode_from_cstring( CSTR__encoded );
    }
  }
  // Encoded array or map
  else if( attr & __CSTRING_ATTR_ARRAY_MASK ) {
    switch( attr & __CSTRING_ATTR_ARRAY_MASK ) {
    case CSTRING_ATTR_ARRAY_INT:
    case CSTRING_ATTR_ARRAY_FLOAT:
      py_object = iPyVGXBuilder.NumberListFromCString( CSTR__encoded );
      break;
    case CSTRING_ATTR_ARRAY_MAP:
      py_object = iPyVGXBuilder.NumberMapFromCString( CSTR__encoded );
      break;
    }
  }
  // Callable
  else if( attr & CSTRING_ATTR_CALLABLE ) {
    // Convert cstring to bytes if we didn't already do so because of compression and/or keyval packing
    if( py_bytes == NULL ) {
      py_bytes = __new_pybytes_from_cstring( CSTR__encoded );
    }
    if( py_bytes ) {
      PyObject *py_triple;
      if( keyval ) {
        py_triple = py_bytes; // already unpickled, steal
        py_bytes = NULL;
      }
      else {
        py_triple = __pickle_loads( py_bytes );
      }
      // Unpickle to get the 3-tuple (code, defaults, annotation)
      if( py_triple && PyTuple_Check( py_triple ) && PyTuple_GET_SIZE( py_triple ) == 3 ) {
        PyObject *py_code = NULL;
        PyObject *py_defaults = NULL;
        PyObject *py_annotations = NULL;
        // 0: code
        // 1: defaults
        // 2: annotations
        PyObject *py_code_bytes = PyTuple_GET_ITEM( py_triple, 0 );
        if( PyBytes_CheckExact( py_code_bytes ) ) {
          py_code = PyMarshal_ReadObjectFromString( PyBytes_AS_STRING( py_code_bytes ), PyBytes_GET_SIZE( py_code_bytes ) );
        }
        py_defaults = PyTuple_GET_ITEM( py_triple, 1 );
        Py_INCREF( py_defaults );
        py_annotations = PyTuple_GET_ITEM( py_triple, 2 );
        Py_INCREF( py_annotations );
        Py_XDECREF( py_triple );

        // Create function instance from code, defaults and annotations
        if( py_code ) {
          py_object = __new_function_from_code_object( py_code, py_defaults, py_annotations );
          Py_DECREF( py_code );
        }
        Py_XDECREF( py_defaults );
        Py_XDECREF( py_annotations );
      }
    }
  }
  else if( attr & CSTRING_ATTR_SERIALIZED_TXT ) {
    // Convert cstring to bytes if we didn't already do so because of compression and/or keyval packing
    if( py_bytes == NULL ) {
      py_bytes = __new_pybytes_from_cstring( CSTR__encoded );
    }
    if( py_bytes ) {
      if( keyval ) {
        py_object = py_bytes; // already unpickled, steal
        py_bytes = NULL;
      }
      else {
        py_object = __pickle_loads( py_bytes );
      }
    }
  }

  iString.Discard( &CSTR__bytes );

  if( py_object && py_retkey ) {
    *py_retkey = py_key;
    py_key = NULL;
  }
  Py_XDECREF( py_key );
  Py_XDECREF( py_bytes );

  return py_object;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
__inline static bool _ipyvgx_codec__is_cstring_compressible( const CString_t *CSTR__string ) {
  return CALLABLE( CSTR__string )->Length( CSTR__string ) >= CSTRING_MIN_COMPRESS_SZ;
}



/******************************************************************************
 *
 * 
 *
 ******************************************************************************
 */
__inline static bool _ipyvgx_codec__is_pystring_compressible( PyObject *py_string ) {
  // Bytes
  if( PyBytes_CheckExact( py_string ) ) {
    return PyBytes_GET_SIZE( py_string ) >= CSTRING_MIN_COMPRESS_SZ;
  }
  // Unicode
  if( PyUnicode_Check( py_string ) ) {
    Py_ssize_t sz = 0;
    PyUnicode_AsUTF8AndSize( py_string, &sz );
    return sz >= CSTRING_MIN_COMPRESS_SZ;
  }
  // Not a string
  return false;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_codec__new_compressed_pybytes_from_pyobject( PyObject *py_object ) {
  PyObject *s = __marshal_dumps( py_object );
  if( s == NULL ) {
    return NULL;
  }

  if( !PyBytes_Check( s ) ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }
  
  int64_t sz = PyBytes_GET_SIZE( s );
  const char *data = PyBytes_AS_STRING( s );
  
  CString_t *CSTR__compressed;
  BEGIN_PYVGX_THREADS {
    CSTR__compressed = CStringNewCompressedAlloc( data, (int)sz, 0, NULL, CSTRING_ATTR_NONE, CSTRING_COMPRESS_MODE_LZ4 );
  } END_PYVGX_THREADS;

  if( !CSTR__compressed ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }

  PyObject *py_bytes = PyBytes_FromStringAndSize( CStringValue( CSTR__compressed ), CStringLength( CSTR__compressed ) );

  CStringDelete( CSTR__compressed );

  return py_bytes;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_codec__new_pyobject_from_compressed_pybytes( PyObject *py_bytes ) {
  char PALIGNED_ decomp_buffer[ARCH_PAGE_SIZE];

  if( !PyBytes_Check( py_bytes ) ) {
    PyErr_SetString( PyExc_TypeError, "bytes required" );
    return NULL;
  }

  int64_t sz = PyBytes_GET_SIZE( py_bytes );
  const char *data = PyBytes_AS_STRING( py_bytes );

  const char *marshalled;
  char *decompressed = NULL;
  int32_t len = 0;
  int32_t uclen = 0;
  CString_attr attr = CSTRING_ATTR_NONE;
  if( CStringCharsNotCompressed( data, sz ) ) {
    marshalled = data;
    len = (int)sz;
  }
  else {
    int cret;
    BEGIN_PYVGX_THREADS {
      cret = CStringDecompressBytesToBytes( data, (int)sz, decomp_buffer, ARCH_PAGE_SIZE, &decompressed, &len, &uclen, &attr );
    } END_PYVGX_THREADS;
    if( cret < 0 ) {
      PyErr_SetString( PyExc_Exception, "internal decompression error" );
      return NULL;
    }
    marshalled = decompressed;
  }

  PyObject *py_marshalled_bytes = PyBytes_FromStringAndSize( marshalled, len );

  if( decompressed && decompressed != decomp_buffer ) {
    ALIGNED_FREE( decompressed );
  }

  if( py_marshalled_bytes == NULL ) {
    return py_marshalled_bytes;
  }

  PyObject *py_obj = __marshal_loads( py_marshalled_bytes );

  Py_DECREF( py_marshalled_bytes );

  return py_obj;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static bool _ipyvgx_codec__is_type_json( const PyObject *py_type ) {
  return py_type == mod_json;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __render_bytes( const char *data, int64_t sz_data, vgx_StreamBuffer_t *output ) {
  if( iStreamBuffer.Write( output, data, sz_data ) < 0 ) {
    PyErr_SetString( PyExc_MemoryError, "out of memory" );
    return -1;
  }
  return 0;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __render_pyunicode( PyObject *py_obj, vgx_StreamBuffer_t *output ) {
  Py_ssize_t sz_data = 0;
  const char *data = PyUnicode_AsUTF8AndSize( py_obj, &sz_data );
  if( data ) {
    return __render_bytes( data, sz_data, output );
  }
  PyErr_Clear();
  PyObject *py_fallback = PyObject_Repr( py_obj );
  if( py_fallback == NULL ) {
    return -1;
  }
  int ret;
  if( (data = PyUnicode_AsUTF8AndSize( py_fallback, &sz_data )) == NULL ) {
    ret = -1;
  }
  else {
    ret = __render_bytes( data, sz_data, output );
  }
  Py_DECREF( py_fallback );
  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int __render_pyobject_as_json( PyObject *py_obj, vgx_StreamBuffer_t *output ) {
  int ret;
  PyObject *py_json;
  if( PyVGX_PluginResponse_CheckExact( py_obj ) ) {
    py_json = __pyvgx_PluginResponse_ToJSON( (PyVGX_PluginResponse*)py_obj );
  }
  else {
    py_json = __json_dumps( py_obj );
  }
  if( py_json ) {
    ret = __render_pyunicode( py_json, output );
    Py_DECREF( py_json );
    return ret;
  }
  PyErr_Clear();
  PyObject *py_fallback = PyObject_Repr( py_obj );
  if( py_fallback == NULL ) {
    return -1;
  }
  if( (py_json = __json_dumps( py_fallback )) == NULL ) {
    ret = -1;
  }
  else {
    ret = __render_pyunicode( py_json, output );
    Py_DECREF( py_json );
  }
  Py_DECREF( py_fallback );
  return ret;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int _ipyvgx_codec__render_pyobject_by_mediatype( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, vgx_StreamBuffer_t *output ) {
  switch( mtype ) {
  // x-vgx-partial
  case MEDIA_TYPE__application_x_vgx_partial:
    return __pyvgx_PluginResponse_serialize_x_vgx_partial( (PyVGX_PluginResponse*)py_obj, output );
  // json
  case MEDIA_TYPE__application_json:
    // Not already json, encode
    if( !_ipyvgx_codec__is_type_json( py_plugin_return_type ) ) {
      return __render_pyobject_as_json( py_obj, output );
    }
    /* FALLTHRU */
  // string
  default:
    return __render_pyunicode( py_obj, output );
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_codec__convert_pyobject_by_mediatype( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, const char **rstr, int64_t *rsz ) {
  PyObject *py_media = NULL;

  switch( mtype ) {
  case MEDIA_TYPE__application_x_vgx_partial:

      

    break;
  case MEDIA_TYPE__application_json:
    // Not already json, encode
    if( !_ipyvgx_codec__is_type_json( py_plugin_return_type ) ) {
      if( (py_media = __json_dumps( py_obj )) == NULL ) {
        PyErr_Clear();
        PyObject *py_repr = PyObject_Repr( py_obj );
        if( py_repr ) {
          py_media = __json_dumps( py_repr );
          Py_DECREF( py_repr );
        }
      }
    }
    break;
  default:
    break;
  }
  
  // Assign string to media if not already created
  if( py_media == NULL ) {
    // media is a plain string
    if( PyUnicode_Check( py_obj ) ) {
      Py_INCREF( py_obj );
      py_media = py_obj;
    }
    // Fallback on repr
    else {
      py_media = PyObject_Repr( py_obj );
    }
  }

  // Extract bytes into return variable
  if( py_media ) {
    Py_ssize_t size = 0;
    *rstr = PyUnicode_AsUTF8AndSize( py_media, &size );
    *rsz = size;
  }

  // Return new object representing the input object.
  // The returned object must not be decrefed until the
  // raw data returned in rstr is no longer needed.
  return py_media;
}





/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int _ipyvgx_codec__init( void ) {

  if( __create_constants() < 0 ) {
    return -1;
  }

  if( mod_pickle == NULL ) {
    if( __import_pickle() < 0 ) {
      return -1;
    }
  }

  if( mod_json == NULL ) {
    if( __import_json() < 0 ) {
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
DLL_HIDDEN int _ipyvgx_codec__delete( void ) {

  if( mod_pickle ) {
    Py_DECREF( mod_pickle );
    mod_pickle = NULL;
  }

  if( mod_json ) {
    Py_DECREF( mod_json );
    mod_json = NULL;
  }

  __delete_constants();

  return 0;
}





DLL_HIDDEN IPyVGXCodec iPyVGXCodec = {
  .NewEncodedObjectFromPyObject       = _ipyvgx_codec__new_encoded_object_from_pyobject,
  .NewPyObjectFromEncodedObject       = _ipyvgx_codec__new_pyobject_from_encoded_object,
  .IsCStringCompressible              = _ipyvgx_codec__is_cstring_compressible,

  .NewSerializedPyBytesFromPyObject   = __marshal_dumps,
  .NewPyObjectFromSerializedPyBytes   = __marshal_loads,
  .NewPyObjectFromSerializedBytes     = __marshal_load,

  .NewCompressedPyBytesFromPyObject   = _ipyvgx_codec__new_compressed_pybytes_from_pyobject,
  .NewPyObjectFromCompressedPyBytes   = _ipyvgx_codec__new_pyobject_from_compressed_pybytes,

  .NewJsonPyStringFromPyObject        = __json_dumps,
  .NewJsonPyBytesFromPyObject         = __json_dump,
  .NewPyObjectFromJsonPyString        = __json_loads,
  .NewPyObjectFromJsonBytes           = __json_load,
  .IsTypeJson                         = _ipyvgx_codec__is_type_json,

  .RenderPyObjectByMediatype          = _ipyvgx_codec__render_pyobject_by_mediatype,
  .ConvertPyObjectByMediatype         = _ipyvgx_codec__convert_pyobject_by_mediatype
};

