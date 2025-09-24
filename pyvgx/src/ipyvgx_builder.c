/*######################################################################
 *#
 *# ipyvgx_builder.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"
#include "_vxsim.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


static PyObject *mod_traceback = NULL;
static PyObject *meth_format_tb = NULL;


static void           _ipyvgx_builder__delete_traceback( PyObject **py_traceback, PyObject **py_format_tb );
static int            _ipyvgx_builder__init_traceback( PyObject **py_traceback, PyObject **py_format_tb );
static PyObject *     _ipyvgx_builder__build_internal_vector( vgx_Vector_t *vector );
static PyObject *     _ipyvgx_builder__build_internal_vector_array( vgx_Vector_t *vector );
static PyObject *     _ipyvgx_builder__build_external_vector( vgx_Vector_t *vector );
static PyObject *     _ipyvgx_builder__pylist_from_CtptrList_of_CString_pointers( CtptrList_t *CSTR__strings );
static int            _ipyvgx_builder__pydict_map_string_to_long_long( PyObject *py_dict, const char *key, const int64_t value );
static int            _ipyvgx_builder__pydict_map_string_to_unsigned_long_long( PyObject *py_dict, const char *key, const uint64_t value );
static int            _ipyvgx_builder__pydict_map_string_to_int( PyObject *py_dict, const char *key, const int value );
static int            _ipyvgx_builder__pydict_map_string_to_string( PyObject *py_dict, const char *key, const char *value );
static int            _ipyvgx_builder__pydict_map_string_to_float( PyObject *py_dict, const char *key, const double value );
static int            _ipyvgx_builder__pydict_map_string_to_pyobject( PyObject *py_dict, const char *key, PyObject **py_object );
static PyObject *     _ipyvgx_builder__pylist_from_bytes_numarray( const char *data, int sz, bool isfloat );
static PyObject *     _ipyvgx_builder__pylist_from_cstring_numarray( const CString_t *CSTR__str );
static PyObject *     _ipyvgx_builder__pydict_from_bytes_nummap( const char *data );
static PyObject *     _ipyvgx_builder__pydict_from_cstring_nummap( const CString_t *CSTR__str );
static int64_t        _ipyvgx_builder__map_integer_constants( PyObject *py_dict, vgx_KeyVal_char_int64_t *data );
static PyObject *     _ipyvgx_builder__pytuple_from_cstring_map_keyval( const QWORD *item_bits );
static PyObject *     _ipyvgx_builder__vertex_properties_as_pydict( vgx_Vertex_t *vertex_RO );
static void           _ipyvgx_builder__set_error_from_messages( vgx_StringTupleList_t *messages );
static bool           _ipyvgx_builder__py_error_from_reason( const char *object_name, vgx_AccessReason_t reason, CString_t **CSTR__error );
static int            _ipyvgx_builder__catch_pyexception_into_output( const char *wrap, vgx_MediaType *mediatype, CString_t **CSTR__output, vgx_StreamBuffer_t *output );





/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static void _ipyvgx_builder__delete_traceback( PyObject **py_traceback, PyObject **py_format_tb ) {
  Py_XDECREF( *py_traceback );
  Py_XDECREF( *py_format_tb );
  *py_traceback = NULL;
  *py_format_tb = NULL;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__init_traceback( PyObject **py_traceback, PyObject **py_format_tb ) {
  int ret = 0;
  *py_traceback = PyImport_ImportModule( "traceback" );
  *py_format_tb = PyUnicode_FromString( "format_tb" );
  if( *py_traceback == NULL || *py_format_tb == NULL ) {
    _ipyvgx_builder__delete_traceback( py_traceback, py_format_tb );
    ret = -1;
  }
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __build_internal_euclidean_vector( const vgx_Vector_t *vint ) {
  PyObject *py_internal = NULL;
  vgx_Vector_vtable_t *ivector = CALLABLE(vint);
  const char *elem = ivector->Elements( vint );
  int sz = ivector->Length( vint );
  BEGIN_PYTHON_INTERPRETER {
    py_internal = PyByteArray_FromStringAndSize( elem, sz );
  } END_PYTHON_INTERPRETER;
  return py_internal;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __build_internal_feature_vector( const vgx_Vector_t *vint ) {
  PyObject *py_internal = NULL;
  vgx_Vector_vtable_t *ivector = CALLABLE(vint);
  vector_feature_t *elem = ivector->Elements( vint );
  int sz = ivector->Length( vint );
  BEGIN_PYTHON_INTERPRETER {
    if( (py_internal = PyList_New( sz )) != NULL ) { 
      for( int i=0; i<sz; i++, elem++ ) {
        PyObject *py_dim = PyLong_FromLong( elem->dim );
        PyObject *py_mag = PyLong_FromLong( elem->mag );
        PyObject *py_tuple = PyTuple_New(2);
        if( py_dim && py_mag && py_tuple ) {
          PyTuple_SET_ITEM( py_tuple, 0, py_dim );
          PyTuple_SET_ITEM( py_tuple, 1, py_mag );
          PyList_SET_ITEM( py_internal, i, py_tuple );
        }
        else {
          PyVGX_XDECREF( py_dim );
          PyVGX_XDECREF( py_mag );
          PyVGX_XDECREF( py_tuple );
          PyVGX_DECREF( py_internal );
          py_internal = NULL;
          break;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return py_internal;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__build_internal_vector( vgx_Vector_t *vector ) {
  PyObject *py_internal = NULL;

  CString_t *CSTR__error = NULL;

  if( vector == NULL ) {
    py_internal = PyList_New( 0 );
  }
  else {
    BEGIN_PYVGX_THREADS {
      vgx_VectorContext_t *vector_context = CALLABLE( vector )->Context( vector );
      vgx_Similarity_t *sim = vector_context->simobj;
      vgx_Vector_t *vint = CALLABLE( sim )->InternalizeVector( sim, vector, true, &CSTR__error );
      if( vint ) {
        if( igraphfactory.EuclideanVectors() ) {
          py_internal = __build_internal_euclidean_vector( vint );
        }
        else {
          py_internal = __build_internal_feature_vector( vint );
        }
        CALLABLE( vint )->Decref( vint );
      }
    } END_PYVGX_THREADS;
  }

  if( py_internal == NULL ) {
    if( CSTR__error ) {
      PyVGXError_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
      CStringDelete( CSTR__error );
    }
    else if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_Exception, "Internal error" );
    }
  }

  return py_internal;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __build_internal_euclidean_vector_array( const vgx_Vector_t *vint ) {
  vgx_Vector_vtable_t *ivector = CALLABLE(vint);
  const char *elem = ivector->Elements( vint );
  int sz = ivector->Length( vint );
  PyObject *py_internal_array = NULL;
  BEGIN_PYTHON_INTERPRETER {
    if( (py_internal_array = PyList_New( sz )) != NULL ) {
      for( int i=0; i<sz; i++ ) {
        if( PyList_SetItem( py_internal_array, i, PyFloat_FromDouble( (double)*elem++ ) ) < 0 ) {
          Py_DECREF( py_internal_array );
          py_internal_array = NULL;
          break;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return py_internal_array;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__build_internal_vector_array( vgx_Vector_t *vector ) {
  PyObject *py_internal_array = NULL;

  CString_t *CSTR__error = NULL;

  if( vector == NULL ) {
    py_internal_array = PyList_New( 0 );
  }
  else {
    BEGIN_PYVGX_THREADS {
      vgx_VectorContext_t *vector_context = CALLABLE( vector )->Context( vector );
      vgx_Similarity_t *sim = vector_context->simobj;
      vgx_Vector_t *vint = CALLABLE( sim )->InternalizeVector( sim, vector, true, &CSTR__error );
      if( vint ) {
        if( igraphfactory.EuclideanVectors() ) {
          py_internal_array = __build_internal_euclidean_vector_array( vint );
        }
        else {
          PyErr_SetString( PyExc_NotImplementedError, "unsupported for feature vectors" );
        }
        CALLABLE( vint )->Decref( vint );
      }
    } END_PYVGX_THREADS;
  }

  if( py_internal_array == NULL ) {
    if( CSTR__error ) {
      PyVGXError_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
      CStringDelete( CSTR__error );
    }
    else if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_Exception, "Internal error" );
    }
  }

  return py_internal_array;
}




/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __build_external_euclidean_vector( const vgx_Vector_t *vext ) {
  PyObject *py_external = NULL;
  vgx_Vector_vtable_t *ivector = CALLABLE( vext );
  float *elem = ivector->Elements( vext );
  int sz = ivector->Length( vext );
  BEGIN_PYTHON_INTERPRETER {
    if( (py_external = PyList_New( sz )) != NULL ) { 
      for( int i=0; i<sz; i++, elem++ ) {
        PyObject *py_float = PyFloat_FromDouble( *elem );
        if( py_float ) {
          PyList_SET_ITEM( py_external, i, py_float );
        }
        else {
          Py_DECREF( py_external );
          py_external = NULL;
          break;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return py_external;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __build_external_feature_vector( const vgx_Vector_t *vext ) {
  PyObject *py_external = NULL;
  vgx_Vector_vtable_t *ivector = CALLABLE( vext );
  ext_vector_feature_t *elem = ivector->Elements( vext );
  int sz = ivector->Length( vext );
  BEGIN_PYTHON_INTERPRETER {
    if( (py_external = PyList_New( sz )) != NULL ) { 
      for( int i=0; i<sz; i++, elem++ ) {
        PyObject *py_term = PyUnicode_FromString( elem->term );
        PyObject *py_weight = PyFloat_FromDouble( elem->weight );
        PyObject *py_tuple = PyTuple_New(2);
        if( py_term && py_weight && py_tuple ) {
          PyTuple_SET_ITEM( py_tuple, 0, py_term );
          PyTuple_SET_ITEM( py_tuple, 1, py_weight );
          PyList_SET_ITEM( py_external, i, py_tuple );
        }
        else {
          PyVGX_XDECREF( py_term );
          PyVGX_XDECREF( py_weight );
          PyVGX_XDECREF( py_tuple );
          PyVGX_DECREF( py_external );
          py_external = NULL;
          break;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return py_external;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__build_external_vector( vgx_Vector_t *vector ) {
  PyObject *py_external = NULL;

  if( vector == NULL ) {
    py_external = PyList_New( 0 );
  }
  else {
    BEGIN_PYVGX_THREADS {
      vgx_VectorContext_t *vector_context = CALLABLE( vector )->Context( vector );
      vgx_Similarity_t *sim = vector_context->simobj;
      vgx_Vector_t *vext = CALLABLE( sim )->ExternalizeVector( sim, vector, true );
      if( vext ) {
        if( igraphfactory.EuclideanVectors() ) {
          py_external = __build_external_euclidean_vector( vext );
        }
        else {
          py_external = __build_external_feature_vector( vext );
        }
        CALLABLE( vext )->Decref( vext );
      }
    } END_PYVGX_THREADS;
  }

  if( py_external == NULL ) {
    if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_Exception, "Internal error" );
    }
  }

  return py_external;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pylist_from_CtptrList_of_CString_pointers( CtptrList_t *CSTR__strings ) {
  
  PyObject *py_list = NULL;

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      CtptrList_vtable_t *ilist = CALLABLE( CSTR__strings );
      tptr_t data;
      const CString_t *CSTR__string;
      PyObject *py_string;
      int64_t sz = ilist->Length( CSTR__strings );
      if( ( py_list = PyList_New( sz ) ) != NULL ) {
        for( int64_t i=0; i<sz; i++ ) {
          if( ilist->Get( CSTR__strings, i, &data ) == 1 ) {
            CSTR__string = (const CString_t*)TPTR_GET_PTR56( &data );
            if( (py_string = PyUnicode_FromStringAndSize( CStringValue(CSTR__string), CStringLength(CSTR__string) )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x901 );
            }
            PyList_SET_ITEM( py_list, i, py_string );
          }
          else {
            PyVGXError_SetString( PyExc_Exception, "Internal list error" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x902 );
          }
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x903 );
      }
    }
    XCATCH( errcode ) {
      PyVGX_XDECREF( py_list );
      py_list = NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return py_list;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_long_long( PyObject *py_dict, const char *key, const int64_t value ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    err = PyVGX_DictStealItemString( py_dict, key, PyLong_FromLongLong( value ) );
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_unsigned_long_long( PyObject *py_dict, const char *key, const uint64_t value ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    err = PyVGX_DictStealItemString( py_dict, key, PyLong_FromUnsignedLongLong( value ) );
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_int( PyObject *py_dict, const char *key, const int value ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    err = PyVGX_DictStealItemString( py_dict, key, PyLong_FromLong( value ) );
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_string( PyObject *py_dict, const char *key, const char *value ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    err = PyVGX_DictStealItemString( py_dict, key, PyUnicode_FromString( value ) );
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_float( PyObject *py_dict, const char *key, const double value ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    err = PyVGX_DictStealItemString( py_dict, key, PyFloat_FromDouble( value ) );
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__pydict_map_string_to_pyobject( PyObject *py_dict, const char *key, PyObject **py_object ) {
  int err;
  BEGIN_PYTHON_INTERPRETER {
    if( py_object ) {
      err = PyVGX_DictStealItemString( py_dict, key, *py_object );
      *py_object = NULL; // steal even on error
    }
    else {
      err = -1;
    }
  } END_PYTHON_INTERPRETER;
  return err;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pylist_from_bytes_numarray( const char *data, int sz, bool isfloat ) {
  PyObject *py_list = NULL;

  QWORD *qw = (QWORD*)data;
  int64_t nqw = qwcount( sz );
  if( (py_list = PyList_New( nqw )) == NULL ) {
    return NULL;
  }

  PyObject *py_num;

  if( !isfloat ) {
    for( int64_t i=0; i<nqw; i++ ) {
      if( (py_num = PyLong_FromLongLong( *(int64_t*)qw++ )) == NULL ) {
        goto error;
      }
      PyList_SET_ITEM( py_list, i, py_num );
    }
  }
  else {
    for( int64_t i=0; i<nqw; i++ ) {
      if( (py_num = PyFloat_FromDouble( *(double*)qw++ )) == NULL ) {;
        goto error;
      }
      PyList_SET_ITEM( py_list, i, py_num );
    }
  }

  return py_list;

error:
  PyVGX_DECREF( py_list );
  return NULL;
}


/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pylist_from_cstring_numarray( const CString_t *CSTR__str ) {
  CString_attr attr = CStringAttributes( CSTR__str );
  bool isfloat = (attr & CSTRING_ATTR_ARRAY_FLOAT) != 0;
  return _ipyvgx_builder__pylist_from_bytes_numarray( CStringValue( CSTR__str ), CStringLength( CSTR__str ), isfloat );
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pydict_from_bytes_nummap( const char *data ) {
  PyObject *py_dict;

  if( (py_dict = PyDict_New()) == NULL ) {
    return NULL;
  }

  QWORD *qdata = (QWORD*)data;

  vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)qdata;
  vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)qdata + 1;
  vgx_cstring_array_map_cell_t *last = cell + header->mask;
  while( cell <= last ) {
    if( cell->key != VGX_CSTRING_ARRAY_MAP_KEY_NONE ) {
      PyObject *py_key = PyLong_FromLong( cell->key );
      PyObject *py_val = PyFloat_FromDouble( cell->value );
      if( py_key && py_val && PyDict_SetItem( py_dict, py_key, py_val ) == 0 ) {
        PyVGX_DECREF( py_key );
        PyVGX_DECREF( py_val );
      }
      else {
        PyVGX_XDECREF( py_key );
        PyVGX_XDECREF( py_val );
        PyVGX_XDECREF( py_dict );
        return NULL;
      }
    }
    ++cell;
  }

  return py_dict;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pydict_from_cstring_nummap( const CString_t *CSTR__str ) {
  return _ipyvgx_builder__pydict_from_bytes_nummap( CStringValue( CSTR__str ) );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int64_t _ipyvgx_builder__map_integer_constants( PyObject *py_dict, vgx_KeyVal_char_int64_t *data ) {
  vgx_KeyVal_char_int64_t *cursor = data;
  while( cursor->key ) {
    if( iPyVGXBuilder.DictMapStringToLongLong( py_dict, cursor->key, cursor->value ) < 0 ) {
      return -1;
    }
    ++cursor;
  }
  return cursor - data;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__pytuple_from_cstring_map_keyval( const QWORD *item_bits ) {
  PyObject *py_keyval = PyTuple_New( 2 );
  PyObject *py_key = PyLong_FromLong( vgx_cstring_array_map_key( item_bits ) );
  PyObject *py_val = PyFloat_FromDouble( vgx_cstring_array_map_val( item_bits ) );
  if( py_keyval && py_key && py_val ) {
    PyTuple_SET_ITEM( py_keyval, 0, py_key );
    PyTuple_SET_ITEM( py_keyval, 1, py_val );
    return py_keyval;
  }
  else {
    PyVGX_XDECREF( py_keyval );
    PyVGX_XDECREF( py_key );
    PyVGX_XDECREF( py_val );
    return NULL;
  }
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_builder__vertex_properties_as_pydict( vgx_Vertex_t *vertex_RO ) {
  PyObject *py_dict = NULL;
  
  BEGIN_PYVGX_THREADS {
    vgx_SelectProperties_t *selected_properties = NULL;
    vgx_Graph_t *graph = vertex_RO->graph;

    XTRY {

      // Get all properties
      if( (selected_properties = CALLABLE( vertex_RO )->GetProperties( vertex_RO )) == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Error during property lookup" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x911 );
      }
      int64_t n_items = selected_properties->len;
      vgx_VertexProperty_t *cursor = selected_properties->properties;
      CString_t *CSTR__str;

      // Insert all properties into dict
      bool failed = false;
      BEGIN_PYTHON_INTERPRETER {
        if( (py_dict = PyDict_New()) != NULL ) {
          PyObject *py_obj;
          for( int64_t px=0; px<n_items && !failed; px++ ) {
            // Ignore entries without keys
            if( cursor->key == NULL ) {
              ++cursor;
              continue;
            }
            const char *key = CStringValue( cursor->key );
            const char *strval = cursor->val.data.simple.string; // speculate, will overwrite if CString
            switch( cursor->val.type ) {
            case VGX_VALUE_TYPE_BOOLEAN:
              py_obj = Py_True;
              Py_INCREF( py_obj );
              if( iPyVGXBuilder.DictMapStringToPyObject( py_dict, key, &py_obj ) < 0 ) {
                failed = true;
              }
              break;
            case VGX_VALUE_TYPE_INTEGER:
              if( iPyVGXBuilder.DictMapStringToLongLong( py_dict, key, cursor->val.data.simple.integer ) < 0 ) {
                failed = true;
              }
              break;
            case VGX_VALUE_TYPE_REAL:
              if( iPyVGXBuilder.DictMapStringToFloat( py_dict, key, cursor->val.data.simple.real ) < 0 ) {
                failed = true;
              }
              break;

            //
            // STRING VALUE
            //
            case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
              /* FALLTHRU */
            case VGX_VALUE_TYPE_CSTRING:
              CSTR__str = cursor->val.data.simple.CSTR__string;

              if( (py_obj = iPyVGXCodec.NewPyObjectFromEncodedObject( CSTR__str, NULL )) != NULL ) {
                if( iPyVGXBuilder.DictMapStringToPyObject( py_dict, key, &py_obj ) < 0 ) {
                  failed = true;
                }
              }
              else {
                if( PyErr_Occurred() ) {
                  PyErr_Clear();
                }
                const char *b = CStringValue( cursor->val.data.simple.CSTR__string );
                int64_t bsz = CStringLength( cursor->val.data.simple.CSTR__string );
                PyObject *py_bytes_fallback = PyBytes_FromStringAndSize( b, bsz );
                iPyVGXBuilder.DictMapStringToPyObject( py_dict, key, &py_bytes_fallback );
              }
              break;
              /* FALLTHRU */
            case VGX_VALUE_TYPE_STRING:
              /* FALLTHRU */
            case VGX_VALUE_TYPE_BORROWED_STRING:
              if( iPyVGXBuilder.DictMapStringToString( py_dict, key, strval ) < 0 ) {
                failed = true;
              }
              break;
            default:
              break;
            }
            ++cursor;
          }
          PyErr_Clear();
        }
      } END_PYTHON_INTERPRETER;

      if( py_dict == NULL || failed ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x912 );
      }
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_dict );
      } END_PYTHON_INTERPRETER;
      py_dict = NULL;
    }
    XFINALLY {
      iVertexProperty.FreeSelectProperties( graph, &selected_properties );
    }
  } END_PYVGX_THREADS;

  return py_dict;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static void _ipyvgx_builder__set_error_from_messages( vgx_StringTupleList_t *messages ) {
  if( messages ) {
    BEGIN_PYTHON_INTERPRETER {
      PyObject *py_EQ = PyUnicode_FromString( "=" );
      PyObject *py_COMMA = PyUnicode_FromString( ", " );
      int64_t sz = _vgx_string_tuple_list_size( messages );
      PyObject *py_list = PyList_New( sz );
      int64_t n=0;
      if( py_list && py_COMMA && py_EQ ) {
        for( int64_t i=0; i<sz; i++ ) {
          PyObject *py_tuple = PyTuple_New(2);
          if( py_tuple ) {
            vgx_StringTuple_t *tuple = _vgx_string_tuple_list_get_item( messages, i );
            PyTuple_SET_ITEM( py_tuple, 0, PyVGX_PyUnicode_FromStringNoErr( _vgx_string_tuple_key( tuple ) ) );
            PyTuple_SET_ITEM( py_tuple, 1, PyVGX_PyUnicode_FromStringNoErr( _vgx_string_tuple_value( tuple ) ) );
            PyObject *py_item = PyUnicode_Join( py_EQ, py_tuple );
            if( py_item ) {
              PyList_SET_ITEM( py_list, n, py_item );
              ++n;
            }
            Py_DECREF( py_tuple );
          }
        }
        PyObject *py_error = PyUnicode_Join( py_COMMA, py_list );
        if( py_error ) {
          PyErr_SetObject( PyVGX_DataError, py_error );
          PyVGX_DECREF( py_error );
        }
      }
      else {
        PyErr_SetString( PyExc_MemoryError, "out of memory" );
      }

      Py_XDECREF( py_list );
      Py_XDECREF( py_COMMA );
      Py_XDECREF( py_EQ );

    } END_PYTHON_INTERPRETER;
  }
}



/******************************************************************************
 * Set Python error string according to the access reason given.
 * If a Python error has already been set, no action is performed.
 *
 * Returns true or false
 *  true    : Error string set to a conclusive value
 *  false   : Error string set, but caller may want to investigate further and
 *            set a more specific error string.
 ******************************************************************************
 */
static bool _ipyvgx_builder__py_error_from_reason( const char *object_name, vgx_AccessReason_t reason, CString_t **CSTR__error ) {
  bool is_conclusive = false;
  
  char buffer[512] = {0};
  const char *noname = "?";
  const char *show_name;

  if( object_name == NULL ) {
    show_name = noname;
  }
  else {
    BYTE c = '\0';
    const BYTE *cursor = (BYTE*)object_name;
    char *dest = buffer;
    int sz = 0;
    while( sz++ < 508 && (c = *cursor++) != '\0' ) {
      if( c < 0x20 ) {
        c = '.';
      }
      *dest++ = (char)c;
    }
    // Dots
    if( c != '\0' ) {
      *dest++ = '.';
      *dest++ = '.';
      *dest++ = '.';
    }
    // Term
    *dest = '\0';
    show_name = buffer;
  }


  const char noerr[] = "no details";
  const char *errstr = NULL;
  int haserr = 0;
  if( CSTR__error && *CSTR__error ) {
    errstr = CStringValue( *CSTR__error );
    haserr = 1;
  }
  else {
    errstr = noerr;
  }

  BEGIN_PYTHON_INTERPRETER {
    if( !PyErr_Occurred() ) {
      switch( reason ) {
      case VGX_ACCESS_REASON_NONE:
        PyErr_Format( PyVGX_AccessError, "%s", errstr );
        is_conclusive = false;
        break;
      case VGX_ACCESS_REASON_OBJECT_ACQUIRED:
        if( haserr ) {
          PyErr_Format( PyVGX_InternalError, "%s", errstr );
        }
        is_conclusive = false;
        break;
      case VGX_ACCESS_REASON_LOCKED:
        if( errstr == noerr ) {
          PyErr_Format( PyVGX_AccessError, "Object is locked: '%s'", show_name );
        }
        else {
          PyErr_Format( PyVGX_AccessError, "%s", errstr );
        }
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_TIMEOUT:
        if( errstr == noerr ) {
          PyErr_Format( PyVGX_AccessError, "Acquisition timeout: '%s'", show_name );
        }
        else {
          PyErr_Format( PyVGX_AccessError, "%s", errstr );
        }
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_OPFAIL:
        if( errstr == noerr ) {
          PyErr_Format( PyVGX_OperationTimeout, "Operation failed: '%s'", show_name );
        }
        else {
          PyErr_Format( PyVGX_OperationTimeout, "%s", errstr );
        }
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_EXECUTION_TIMEOUT:
        PyErr_Format( PyVGX_SearchError, "%s", errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_VERTEX_ARC_ERROR:
        PyErr_Format( PyVGX_ArcError, "'%s': %s", show_name, errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_READONLY_GRAPH:
        PyVGXError_SetString( PyVGX_AccessError, "Graph is readonly" );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_READONLY_PENDING:
        PyVGXError_SetString( PyVGX_AccessError, "Graph readonly pending" );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_NOEXIST:
        PyErr_Format( PyExc_KeyError, "Object does not exist: '%s'", show_name );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_NOEXIST_MSG:
        PyErr_Format( PyExc_KeyError, "No object : '%s'", errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_OBJECT_CREATED:
        if( haserr ) {
          PyErr_Format( PyVGX_InternalError, "%s", errstr );
        }
        is_conclusive = false;
        break;
      case VGX_ACCESS_REASON_INVALID:
        PyErr_Format( PyVGX_AccessError, "Invalid mode (%s)", errstr );
        is_conclusive = false;
        break;
      case VGX_ACCESS_REASON_NOCREATE:
        PyErr_Format( PyVGX_VertexError, "Cannot create object: '%s' (%s)", show_name, errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_ENUM_NOTYPESPACE:
        PyErr_Format( PyVGX_EnumerationError, "Enumeration typespace exhausted (%s)", errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_TYPEMISMATCH:
        PyErr_Format( PyVGX_VertexError, "Type mismatch for existing object: '%s' (%s)", show_name, errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_SEMAPHORE:
        PyErr_Format( PyVGX_AccessError, "Recursion limit reached for object: '%s'", show_name );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_BAD_CONTEXT:
        PyErr_Format( PyVGX_AccessError, "Bad context (%s)", errstr );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_RO_DISALLOWED:
        PyErr_Format( PyVGX_AccessError, "Read-only vertices held by current thread, operation not allowed" );
        is_conclusive = true;
        break;
      case VGX_ACCESS_REASON_ERROR:
        PyErr_Format( PyVGX_AccessError, "Internal error (%s)", errstr );
        is_conclusive = false;
        break;
      default:
        PyErr_Format( PyExc_Exception, "Error (reason=%lu, object='%s') (%s)", reason, show_name, errstr );
        is_conclusive = false;
      }
    }
    else {
      is_conclusive = true;
    }
  } END_PYTHON_INTERPRETER;

  if( is_conclusive ) {
    if( CSTR__error && *CSTR__error ) {
      CStringDelete( *CSTR__error );
      *CSTR__error = NULL;
    }
  }

  return is_conclusive;
}




/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_builder__catch_pyexception_into_output( const char *wrap, vgx_MediaType *mediatype, CString_t **CSTR__output, vgx_StreamBuffer_t *output ) {
  if( PyErr_Occurred() ) {
    PyObject *py_typ=NULL, *py_val=NULL, *py_tb=NULL;
    PyErr_Fetch( &py_typ, &py_val, &py_tb );

    PyObject *py_ex = PyDict_New();

    if( py_ex ) {
      PyObject *py_tmp;

      if( py_typ ) {
        if( (py_tmp = PyObject_Str( py_typ )) != NULL ) {
          iPyVGXBuilder.DictMapStringToPyObject( py_ex, "exception", &py_tmp );
        }
        Py_DECREF( py_typ );
      }

      if( py_val ) {
        if( PyTuple_Check( py_val ) ) {
          int64_t sz = PyTuple_Size( py_val );
          PyObject *py_list = PyList_New( sz );
          if( py_list ) {
            for( int64_t i=0; i<sz; i++ ) {
              PyObject *py_item = PyObject_Str( PyTuple_GET_ITEM( py_val, i ) );
              if( py_item == NULL ) {
                py_item = Py_None;
                Py_INCREF( py_item );
              }
              PyList_SetItem( py_list, i, py_item );
            }
            Py_DECREF( py_val );
            py_val = py_list;
          }
        }
        else if( !PyUnicode_Check( py_val ) ) {
          PyObject *py_str = PyObject_Str( py_val );
          if( py_str ) {
            Py_DECREF( py_val );
            py_val = py_str;
            // Strip double quotes if string is quoted
            Py_ssize_t sz_str = 0;
            const char *str = PyUnicode_AsUTF8AndSize( py_val, &sz_str );
            if( str && *str == '"' && *(str+sz_str-1) == '"' ) {
              if( (py_str = PyUnicode_FromStringAndSize( str+1, sz_str-2 )) != NULL ) {
                Py_DECREF( py_val );
                py_val = py_str;
              }
            }
          }
        }
        PyErr_Clear(); // just in case
        iPyVGXBuilder.DictMapStringToPyObject( py_ex, "value", &py_val );
      }

      if( py_tb ) {
        if( (py_tmp = PyObject_CallMethodObjArgs( mod_traceback, meth_format_tb, py_tb, NULL )) != NULL ) {
          iPyVGXBuilder.DictMapStringToPyObject( py_ex, "traceback", &py_tmp );
        }
        Py_DECREF( py_tb );
      }

      if( wrap ) {
        PyObject *py_wrap = PyDict_New();
        if( py_wrap != NULL ) {
          if( PyDict_SetItemString( py_wrap, wrap, py_ex ) < 0 ) {
            Py_DECREF( py_wrap );
          }
          else {
            Py_DECREF( py_ex );
            py_ex = py_wrap;
          }
        }
      }

      const char *serr = NULL;
      if( mediatype ) {
        if( CSTR__output ) {
          int64_t sz_serr = 0;
          PyObject *py_media = iPyVGXCodec.ConvertPyObjectByMediatype( *mediatype, NULL, py_ex, &serr, &sz_serr );
          __set_error_string( CSTR__output, serr ? serr : "internal error" );
          Py_XDECREF( py_media );
        }
        if( output ) {
          iPyVGXCodec.RenderPyObjectByMediatype( *mediatype, NULL, py_ex, output );
        }
      }
      else {
        PyObject *py_str = PyObject_Str( py_ex );
        serr = py_str ? PyUnicode_AsUTF8( py_str ) : "internal error";
        if( CSTR__output ) {
          __set_error_string( CSTR__output, serr ? serr : "unknown error" );
        }
        if( output && serr ) {
          iStreamBuffer.Write( output, serr, strlen(serr) );
        }
        Py_XDECREF( py_str );
      }

      Py_DECREF( py_ex );

      // Just in case
      PyErr_Clear();

      return 1;
    }
    else {
      return -1;
    }
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
DLL_HIDDEN int _ipyvgx_builder__init( void ) {

  if( mod_traceback == NULL ) {
    if( _ipyvgx_builder__init_traceback( &mod_traceback, &meth_format_tb ) < 0 ) {
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
DLL_HIDDEN int _ipyvgx_builder__delete( void ) {
  if( mod_traceback ) {
    _ipyvgx_builder__delete_traceback( &mod_traceback, &meth_format_tb );
  }
  return 0;
}







DLL_HIDDEN IPyVGXBuilder iPyVGXBuilder = {
  .InternalVector                   = _ipyvgx_builder__build_internal_vector,
  .InternalVectorArray              = _ipyvgx_builder__build_internal_vector_array,
  .ExternalVector                   = _ipyvgx_builder__build_external_vector,
  .StringListFromTptrList           = _ipyvgx_builder__pylist_from_CtptrList_of_CString_pointers,
  .DictMapStringToLongLong          = _ipyvgx_builder__pydict_map_string_to_long_long,
  .DictMapStringToUnsignedLongLong  = _ipyvgx_builder__pydict_map_string_to_unsigned_long_long,
  .DictMapStringToInt               = _ipyvgx_builder__pydict_map_string_to_int,
  .DictMapStringToString            = _ipyvgx_builder__pydict_map_string_to_string,
  .DictMapStringToFloat             = _ipyvgx_builder__pydict_map_string_to_float,
  .DictMapStringToPyObject          = _ipyvgx_builder__pydict_map_string_to_pyobject,
  .NumberListFromBytes              = _ipyvgx_builder__pylist_from_bytes_numarray,
  .NumberListFromCString            = _ipyvgx_builder__pylist_from_cstring_numarray,
  .NumberMapFromBytes               = _ipyvgx_builder__pydict_from_bytes_nummap,
  .NumberMapFromCString             = _ipyvgx_builder__pydict_from_cstring_nummap,
  .MapIntegerConstants              = _ipyvgx_builder__map_integer_constants,
  .TupleFromCStringMapKeyVal        = _ipyvgx_builder__pytuple_from_cstring_map_keyval,
  .VertexPropertiesAsDict           = _ipyvgx_builder__vertex_properties_as_pydict,
  .SetErrorFromMessages             = _ipyvgx_builder__set_error_from_messages,
  .SetPyErrorFromAccessReason       = _ipyvgx_builder__py_error_from_reason,
  .CatchPyExceptionIntoOutput       = _ipyvgx_builder__catch_pyexception_into_output
};

