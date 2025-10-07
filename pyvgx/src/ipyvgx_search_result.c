/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    ipyvgx_search_result.c
 * Author:  Stian Lysne slysne.dev@gmail.com
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

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );




/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static char * __ipyvgx_format_raw_vertex_qwords( char *buffer, const vgx_Vertex_t *vertex ) {
  char *ptr = buffer;
  QWORD *vtxqw = (QWORD*)_cxmalloc_linehead_from_object( vertex );
  for( int i=0; i<(int)qwsizeof(vgx_AllocatedVertex_t); i++ ) {
    snprintf( ptr, 18, "%016llX ", vtxqw[i] );
    ptr += 17;
  }
  return buffer;
}
#define RAW_VERTEX_BUFFER( Name ) char Name[ 17 * qwsizeof(vgx_AllocatedVertex_t) + 1 ]



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __ipyvgx_pyhexstring_from_cstring( const CString_t *CSTR__str ) {
  int len = CStringLength( CSTR__str );
  const unsigned char *bytes = (unsigned char*)CStringValue( CSTR__str );
  char *buffer = NULL;
  int hexlen = bytestohex( &buffer, bytes, len );
  if( hexlen < 0 ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }
  PyObject *py_hexstr = PyBytes_FromStringAndSize( buffer, hexlen );
  free( buffer );
  return py_hexstr;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int64_t __ipyvgx_search_result__strnadd( char **dest, const char *prefix, const char *body, const char *suffix, int64_t n_available ) {
  char *cursor = *dest;

  // prefix
  if( n_available > 0 && prefix ) {
    int64_t sz_prefix = strnlen( prefix, n_available );
    strncpy( cursor, prefix, sz_prefix );
    n_available -= sz_prefix;
    cursor += sz_prefix;
  }

  // body
  if( n_available > 0 && body ) {
    while( *body != '\0' && n_available-- > 0 ) {
      *cursor++ = *body++;
    }
  }

  // suffix
  if( n_available > 0 && suffix ) {
    int64_t sz_suffix = strnlen( suffix, n_available );
    strncpy( cursor, suffix, sz_suffix );
    n_available -= sz_suffix;
    cursor += sz_suffix;
  }

  // update destination's position for next time
  *dest = cursor;
  return n_available;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_search_result__py_predicator_val_from_archead( const vgx_ArcHead_t *archead ) {
  PyObject *py_ret = NULL;
  BEGIN_PYTHON_INTERPRETER {
    switch( _vgx_predicator_value_range( NULL, NULL, archead->predicator.mod.bits ) ) {
    case VGX_PREDICATOR_VAL_TYPE_INTEGER:
      py_ret = PyLong_FromLong( archead->predicator.val.integer );
      break;
    case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
      py_ret = PyLong_FromUnsignedLong( archead->predicator.val.uinteger );
      break;
    case VGX_PREDICATOR_VAL_TYPE_REAL:
      py_ret = PyFloat_FromDouble( archead->predicator.val.real );
      break;
    default:
      py_ret = Py_None;
      Py_INCREF( Py_None );
    }
  } END_PYTHON_INTERPRETER;
  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef PyObject * (*__f_render_pyobject)( vgx_ResponseFieldValue_t value, PyObject **py_obj );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__from_cstring_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  *py_string = PyUnicode_FromStringAndSize( CStringValue( value.CSTR__str ), CStringLength( value.CSTR__str ) );
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__from_ident_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  if( value.ident != NULL ) {
    CString_t *CSTR__id = value.ident->identifier.CSTR__idstr;
    if( CSTR__id ) {
      *py_string = PyUnicode_FromStringAndSize( CStringValue( CSTR__id ), CStringLength( CSTR__id ) );
    }
    else {
      *py_string = PyUnicode_FromString( value.ident->identifier.idprefix.data );
    }
  }
  else {
    *py_string = PyUnicode_FromStringAndSize( "*", 1 );
  }
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__from_obid_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  char hex[33];
  *py_string = PyUnicode_FromStringAndSize( idtostr( hex, &value.ident->internalid ), 32 );
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_long__from_i64_field( vgx_ResponseFieldValue_t value, PyObject **py_long ) {
  *py_long = PyLong_FromLongLong( value.i64 );
  return *py_long;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_long__from_qword_field( vgx_ResponseFieldValue_t value, PyObject **py_long ) {
  *py_long = PyLong_FromUnsignedLongLong( value.bits );
  return *py_long;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_float__from_real_field( vgx_ResponseFieldValue_t value, PyObject **py_float ) {
  *py_float = PyFloat_FromDouble( value.real );
  return *py_float;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_int__reldir_from_pred_field( vgx_ResponseFieldValue_t value, PyObject **py_int ) {
  *py_int = PyLong_FromLong( value.pred.rel.dir );
  return *py_int;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__reldir_from_pred_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  *py_string = PyUnicode_FromString( __reverse_arcdir_map[ value.pred.rel.dir ] );
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_int__mod_from_pred_field( vgx_ResponseFieldValue_t value, PyObject **py_int ) {
  *py_int = PyLong_FromLong( value.pred.mod.bits );
  return *py_int;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__mod_from_pred_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  *py_string = PyUnicode_FromString( _vgx_modifier_as_string( value.pred.mod ) );
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_object__val_from_pred_field( vgx_ResponseFieldValue_t value, PyObject **py_number ) {
  switch( _vgx_predicator_value_range( NULL, NULL, value.pred.mod.bits ) ) {
  case VGX_PREDICATOR_VAL_TYPE_UNITY:
    *py_number = PyLong_FromLong( 1 );
    break;
  case VGX_PREDICATOR_VAL_TYPE_INTEGER:
    *py_number = PyLong_FromLong( value.pred.val.integer );
    break;
  case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
    *py_number = PyLong_FromUnsignedLong( value.pred.val.uinteger );
    break;
  case VGX_PREDICATOR_VAL_TYPE_REAL:
    *py_number = PyFloat_FromDouble( value.pred.val.real );
    break;
  default:
    *py_number = PyLong_FromLong( -1 );
  }
  return *py_number;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_dict__from_select_properties( vgx_SelectProperties_t *selected, PyObject **py_dict ) {
  
  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      if( (*py_dict = PyDict_New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA01 );
      }
        
      int64_t n_prop = selected->len;
      vgx_VertexProperty_t *cursor = selected->properties;
      CString_t *CSTR__str;
      CString_attr attr;
      PyObject *py_obj;
      for( int64_t px=0; px<n_prop; px++ ) {

        // Ignore this hidden property
        if( cursor->key == NULL ) {
          ++cursor;
          continue;
        }

        const char *name = CStringValue( cursor->key );
        const char *strval = cursor->val.data.simple.string; // speculate, will be overridden if needed
        int err = 0;
        switch( cursor->val.type ) {
        case VGX_VALUE_TYPE_NULL:
          py_obj = Py_None;
          Py_INCREF( py_obj );
          err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, name, &py_obj );
          break;
        case VGX_VALUE_TYPE_BOOLEAN:
          py_obj = Py_True;
          Py_INCREF( py_obj );
          err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, name, &py_obj );
          break;
        case VGX_VALUE_TYPE_INTEGER:
          err = iPyVGXBuilder.DictMapStringToLongLong( *py_dict, name, cursor->val.data.simple.integer );
          break;
        case VGX_VALUE_TYPE_REAL:
          err = iPyVGXBuilder.DictMapStringToFloat( *py_dict, name, cursor->val.data.simple.real );
          break;

        //
        // STRING VALUE
        //
        case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
          /* FALLTHRU */
        case VGX_VALUE_TYPE_CSTRING:
          CSTR__str = cursor->val.data.simple.CSTR__string;
          attr = CStringAttributes( CSTR__str );
          if( attr & __CSTRING_ATTR_ARRAY_MASK ) {
            PyObject *py_arrobj;
            switch( attr & __CSTRING_ATTR_ARRAY_MASK ) {
            case CSTRING_ATTR_ARRAY_INT:
            case CSTRING_ATTR_ARRAY_FLOAT:
              py_arrobj = iPyVGXBuilder.NumberListFromCString( CSTR__str );
              break;
            case CSTRING_ATTR_ARRAY_MAP:
              py_arrobj = iPyVGXBuilder.NumberMapFromCString( CSTR__str );
              break;
            default:
              py_arrobj = NULL;
            }
            err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, name, &py_arrobj );
            break;
          }
          else if( attr & CSTRING_ATTR_BYTEARRAY ) {
            PyObject *py_hexstr = __ipyvgx_pyhexstring_from_cstring( CSTR__str );
            err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, name, &py_hexstr );
            break;
          }
          else {
            strval = CStringValue( CSTR__str );
          }
          /* FALLTHRU */
        case VGX_VALUE_TYPE_STRING:
          /* FALLTHRU */
        case VGX_VALUE_TYPE_BORROWED_STRING:

          // TODO: What about Compression, Functions, Pickled stuff, py bytes, etc?
          //
          //

          err = iPyVGXBuilder.DictMapStringToString( *py_dict, name, strval );
          break;
        default:
          break;
        }
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA02 );
        }
        ++cursor;
      }
    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      PyVGX_XDECREF( *py_dict );
      *py_dict = NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return *py_dict;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_dict__from_properties_field( vgx_ResponseFieldValue_t value, PyObject **py_dict ) {
  
  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      if( (*py_dict = PyDict_New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA01 );
      }
        
      vgx_ExpressEvalStack_t *stack = value.eval_properties;
      vgx_EvalStackItem_t *cursor = stack->data + 1; // Skip first dummy item
      PyObject *py_obj;
      int n_items = 0;
      while( !EvalStackItemIsTerminator( cursor ) ) {
        
        // ignore empty slot
        if( cursor->type != STACK_ITEM_TYPE_CSTRING ) {
          ++cursor;
          ++cursor;
          continue;
        }

        ++n_items;

        //
        // KEY
        //
        const char *key = CStringValue( cursor->CSTR__str );


        // Value is next
        ++cursor;

        //
        // VALUE
        //
        int err = 0;
        switch( cursor->type ) {
        case STACK_ITEM_TYPE_INTEGER:
          err = iPyVGXBuilder.DictMapStringToLongLong( *py_dict, key, cursor->integer );
          break;
        case STACK_ITEM_TYPE_REAL:
          if( cursor->real < INFINITY ) {
            err = iPyVGXBuilder.DictMapStringToFloat( *py_dict, key, cursor->real );
            break;
          }
        case STACK_ITEM_TYPE_NONE:
        case STACK_ITEM_TYPE_NAN:
          py_obj = Py_None;
          Py_INCREF( py_obj );
          err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, key, &py_obj );
          break;
        case STACK_ITEM_TYPE_VERTEXID:
          if( cursor->vertexid->CSTR__idstr == NULL ) {
            err = iPyVGXBuilder.DictMapStringToString( *py_dict, key, cursor->vertexid->idprefix.data );
          }
          else {
            err = iPyVGXBuilder.DictMapStringToString( *py_dict, key, CStringValue( cursor->vertexid->CSTR__idstr ) );
          }
          break;
        case STACK_ITEM_TYPE_CSTRING:
          if( (py_obj = iPyVGXCodec.NewPyObjectFromEncodedObject( cursor->CSTR__str, NULL )) == NULL ) {
            PyErr_Clear();
            py_obj = Py_None;
            Py_INCREF( Py_None );
          }
          err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, key, &py_obj );
          break;
        case STACK_ITEM_TYPE_VECTOR:
          {
            PyObject *py_vector = iPyVGXBuilder.ExternalVector( (vgx_Vector_t*)cursor->vector );
            if( py_vector ) {
              err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, key, &py_vector );
            }
            else {
              err = -1;
            }
          }
          break;
        case STACK_ITEM_TYPE_BITVECTOR:
          err = iPyVGXBuilder.DictMapStringToUnsignedLongLong( *py_dict, key, cursor->bits );
          break;
        case STACK_ITEM_TYPE_KEYVAL:
          {
            PyObject *py_tuple = iPyVGXBuilder.TupleFromCStringMapKeyVal( &cursor->bits );
            err = iPyVGXBuilder.DictMapStringToPyObject( *py_dict, key, &py_tuple );
          }
          break;
        case STACK_ITEM_TYPE_INIT:
        case STACK_ITEM_TYPE_RANGE:
        case STACK_ITEM_TYPE_SET:
        default:
          err = iPyVGXBuilder.DictMapStringToString( *py_dict, key, "?" );
        }

        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA02 );
        }

        // Next key/val pair
        cursor++;
      }
    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      PyVGX_XDECREF( *py_dict );
      *py_dict = NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return *py_dict;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__from_properties_field( vgx_ResponseFieldValue_t value, PyObject **py_list ) {
  
  BEGIN_PYTHON_INTERPRETER {

    PyObject *py_entry = NULL;
    PyObject *py_val = NULL;

    XTRY {
      vgx_ExpressEvalStack_t *stack = value.eval_properties;
      if( stack == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x000 );
      }
      vgx_EvalStackItem_t *cursor = stack->data + 1; // Skip first dummy item

      // Count items
      int n_items = 0;
      while( !EvalStackItemIsTerminator( cursor ) ) {
        // count
        if( cursor->type == STACK_ITEM_TYPE_CSTRING ) {
          ++n_items;
        }
        // next item
        ++cursor;
        ++cursor;
        continue;
      }

      // Verify terminator
      if( !EvalStackItemIsTerminator( cursor ) ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Reset
      cursor = stack->data + 1;

      if( (*py_list = PyList_New( n_items )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      
      int idx = 0;
      for( int n=0; n<n_items; n++ ) {
        // Skip invalid entries
        if( cursor->type != STACK_ITEM_TYPE_CSTRING ) {
          ++cursor;
          ++cursor;
          continue;
        }

        if( (py_entry = PyTuple_New( 2 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
        }

        //
        // KEY
        //
        PyTuple_SET_ITEM( py_entry, 0, PyVGX_PyUnicode_FromStringNoErr( CStringValue( cursor->CSTR__str ) ) );

        // Value is next
        ++cursor;

        //
        // VALUE
        //
        switch( cursor->type ) {
        case STACK_ITEM_TYPE_INTEGER:
          py_val = PyLong_FromLongLong( cursor->integer );
          break;
        case STACK_ITEM_TYPE_REAL:
          if( cursor->real < INFINITY ) {
            py_val = PyFloat_FromDouble( cursor->real );
            break;
          }
        case STACK_ITEM_TYPE_NONE:
        case STACK_ITEM_TYPE_NAN:
          Py_INCREF( Py_None );
          py_val = Py_None;
          break;
        case STACK_ITEM_TYPE_VERTEXID:
          if( cursor->vertexid->CSTR__idstr == NULL ) {
            py_val = PyUnicode_FromString( cursor->vertexid->idprefix.data );
          }
          else {
            py_val = PyUnicode_FromStringAndSize( CStringValue( cursor->vertexid->CSTR__idstr ), CStringLength( cursor->vertexid->CSTR__idstr ) );
          }
          break;
        case STACK_ITEM_TYPE_CSTRING:
          if( (py_val = iPyVGXCodec.NewPyObjectFromEncodedObject( cursor->CSTR__str, NULL )) == NULL ) {
            PyErr_Clear();
            py_val = Py_None;
            Py_INCREF( Py_None );
          }
          break;
        case STACK_ITEM_TYPE_VECTOR:
          py_val = iPyVGXBuilder.ExternalVector( (vgx_Vector_t*)cursor->vector );
          break;
        case STACK_ITEM_TYPE_BITVECTOR:
          py_val = PyLong_FromUnsignedLongLong( cursor->bits );
          break;
        case STACK_ITEM_TYPE_KEYVAL:
          py_val = iPyVGXBuilder.TupleFromCStringMapKeyVal( &cursor->bits );
          break;
        case STACK_ITEM_TYPE_INIT:
        case STACK_ITEM_TYPE_RANGE:
        case STACK_ITEM_TYPE_SET:
        default:
          py_val = Py_None;
          Py_INCREF( Py_None );
        }

        if( py_val == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }

        PyTuple_SET_ITEM( py_entry, 1, py_val );
        PyList_SET_ITEM( *py_list, idx, py_entry );
        py_val = NULL;
        py_entry = NULL;

        // Next key/val pair
        ++idx;
        ++cursor;
      }
    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      PyVGX_XDECREF( py_val );
      PyVGX_XDECREF( py_entry );
      PyVGX_XDECREF( *py_list );
      *py_list = NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return *py_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_object__from_properties_field( vgx_ResponseFieldValue_t value, PyObject **py_obj ) {
  return __py_dict__from_properties_field( value, py_obj );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_vector__from_vector_field( vgx_ResponseFieldValue_t value, PyObject **py_vector ) {
  *py_vector = iPyVGXBuilder.ExternalVector( (vgx_Vector_t*)value.vector );
  return *py_vector;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__from_handle_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  cxmalloc_handle_t handle = {0};
  handle.qword = value.bits;
  unsigned aidx = handle.aidx;
  unsigned bidx = handle.bidx;
  unsigned offset = handle.offset;
  unsigned objclass = handle.objclass;
  CString_t *CSTR__fmt = CStringNewFormat( "%02X:%u:%u:%u", objclass, aidx, bidx, offset );
  if( CSTR__fmt ) {
    *py_string = PyUnicode_FromString( CStringValue( CSTR__fmt ) );
    CStringDelete( CSTR__fmt );
  }
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_string__from_raw_vertex_field( vgx_ResponseFieldValue_t value, PyObject **py_string ) {
  RAW_VERTEX_BUFFER( rawvertex );
  *py_string = PyUnicode_FromString( __ipyvgx_format_raw_vertex_qwords( rawvertex, value.vertex ) );
  return *py_string;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_ResponseFieldMap_t pyobj_fieldmap_definition[] = {
  // Anchor
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ANCHOR,     .render=(f_ResponseValueRender)__py_string__from_ident_field,       .fieldname="anchor" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ANCHOR_OBID,.render=(f_ResponseValueRender)__py_string__from_obid_field,        .fieldname="anchor-internalid" },
  // Predicator
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ARCDIR,     .render=(f_ResponseValueRender)__py_string__reldir_from_pred_field, .fieldname="direction" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RELTYPE,    .render=(f_ResponseValueRender)__py_string__from_cstring_field,     .fieldname="relationship" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_MODIFIER,   .render=(f_ResponseValueRender)__py_string__mod_from_pred_field,    .fieldname="modifier" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_VALUE,      .render=(f_ResponseValueRender)__py_object__val_from_pred_field,    .fieldname="value" },
  // Vertex
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ID,         .render=(f_ResponseValueRender)__py_string__from_ident_field,       .fieldname="id" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_OBID,       .render=(f_ResponseValueRender)__py_string__from_obid_field,        .fieldname="internalid" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TYPENAME,   .render=(f_ResponseValueRender)__py_string__from_cstring_field,     .fieldname="type" },
  // Degree
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_DEGREE,     .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="degree" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_INDEGREE,   .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="indegree" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_OUTDEGREE,  .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="outdegree" },
  // Properties
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_VECTOR,     .render=(f_ResponseValueRender)__py_vector__from_vector_field,      .fieldname="vector" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_PROPERTY,   .render=(f_ResponseValueRender)__py_object__from_properties_field,  .fieldname="properties" }, // NOTE: placeholder!
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__P_RSV,     .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="RESERVED" },
  // Relevance
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RANKSCORE,  .render=(f_ResponseValueRender)__py_float__from_real_field,         .fieldname="rankscore" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_SIMILARITY, .render=(f_ResponseValueRender)__py_float__from_real_field,         .fieldname="similarity" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_HAMDIST,    .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="hamming-distance" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__R_RSV,     .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="RESERVED" },
  // Timestamps
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMC,        .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="created" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMM,        .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="modified" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMX,        .render=(f_ResponseValueRender)__py_long__from_i64_field,           .fieldname="expires" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__T_RSV,     .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="RESERVED" },
  // Details
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_DESCRIPTOR, .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="descriptor" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ADDRESS,    .render=(f_ResponseValueRender)__py_long__from_qword_field,         .fieldname="address" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_HANDLE,     .render=(f_ResponseValueRender)__py_string__from_handle_field,      .fieldname="handle" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RAW_VERTEX, .render=(f_ResponseValueRender)__py_string__from_raw_vertex_field,  .fieldname="raw-vertex" },
  // END
  { .srcpos=-1,  .attr=0,                            .render=NULL,                                                       .fieldname=NULL }
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_ResponseFieldMap_t pystring_single_entry_definition[] = {
  { .srcpos=-1,  .attr=VGX_RESPONSE_SHOW_AS_STRING, .render=(f_ResponseValueRender)__py_string__from_cstring_field,      .fieldname="as-string" },  // PROBE FOR SPECIAL USE OF THIS ENUM
  // END
  { .srcpos=-1,  .attr=0,                           .render=NULL,                                                        .fieldname=NULL }
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__single_string_entries( const vgx_SearchResult_t *search_result ) {
  int64_t length = search_result->list_length;
  const vgx_ResponseFieldData_t *entry = search_result->list;
  PyObject *py_result_list = PyList_New( length );
  if( py_result_list ) {
    for( int64_t n=0; n<length; n++ ) {
      PyObject *py_string = NULL;
      const CString_t *CSTR__string = entry++->value.CSTR__str;
      if( CSTR__string ) {
        if( (py_string = iPyVGXCodec.NewPyObjectFromEncodedObject( CSTR__string, NULL )) == NULL ) {
          PyErr_Clear();
          py_string = Py_None;
          Py_INCREF( Py_None );
        }
      }
      else {
        py_string = Py_None;
        Py_INCREF( Py_None );
      }
      PyList_SET_ITEM( py_result_list, n, py_string );
    }
  }
  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_object__from_result_entry( const vgx_ResponseFieldMap_t *fieldmap, const vgx_ResponseFieldData_t *entry ) {
  // Process first value in entry
  // Move data from core list to output object
  if( fieldmap->srcpos != -1 ) {
    __f_render_pyobject rfunc = (__f_render_pyobject)fieldmap->render;
    vgx_ResponseFieldValue_t val = entry[ fieldmap->srcpos ].value;
    PyObject *py_obj;
    rfunc( val, &py_obj );
    return py_obj;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__single_object_entries( const vgx_SearchResult_t *search_result, const vgx_ResponseFieldMap_t *fieldmap ) {
  int64_t length = search_result->list_length;
  int width = search_result->list_width;
  vgx_ResponseFieldData_t *entry = search_result->list;
  PyObject *py_result_list = PyList_New( length );
  if( py_result_list ) {
    PyObject *py_entry;
    for( int64_t n=0; n<length; n++ ) {
      if( (py_entry = __py_object__from_result_entry( fieldmap, entry )) != NULL ) {
        PyList_SET_ITEM( py_result_list, n, py_entry );
        py_entry = NULL;
      }
      // Next entry
      entry += width;
    }
  }
  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_tuple__from_result_entry( const vgx_ResponseFieldMap_t *fieldmap, int64_t width, const vgx_ResponseFieldData_t *entry ) {
  PyObject *py_tuple = PyTuple_New( width );
  if( py_tuple ) {
    // Process all values in entry
    const vgx_ResponseFieldMap_t *cursor = fieldmap;
    int dstpos = 0;
    // Move data from core list to output tuple in the map-specified order
    while( cursor->srcpos != -1 ) {
      __f_render_pyobject rfunc = (__f_render_pyobject)cursor->render;
      if( rfunc == __py_object__from_properties_field ) { 
        rfunc = __py_list__from_properties_field;
      }
      vgx_ResponseFieldValue_t val = entry[ cursor->srcpos ].value;
      PyObject *py_obj;
      if( rfunc( val, &py_obj ) ) {
        PyTuple_SET_ITEM( py_tuple, dstpos++, py_obj );
      }
      ++cursor;
    }
  }
  return py_tuple;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__tuple_entries( const vgx_SearchResult_t *search_result, const vgx_ResponseFieldMap_t *fieldmap ) {
  int64_t length = search_result->list_length;
  int width = search_result->list_width;
  vgx_ResponseFieldData_t *entry = search_result->list;
  PyObject *py_result_list = PyList_New( length );
  if( py_result_list ) {
    PyObject *py_entry;
    for( int64_t n=0; n<length; n++ ) {
      if( (py_entry = __py_tuple__from_result_entry( fieldmap, width, entry )) != NULL ) {
        PyList_SET_ITEM( py_result_list, n, py_entry );
        py_entry = NULL;
      }
      // Next entry
      entry += width;
    }
  }
  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_dict__from_result_entry( const vgx_ResponseFieldMap_t *fieldmap, const vgx_ResponseFieldData_t *entry ) {
  PyObject *py_entry_dict = PyDict_New();
  if( py_entry_dict ) {
    PyObject *py_arc_dict = NULL;
    // Distance
    int distance = -1;
    // Process all values in entry
    const vgx_ResponseFieldMap_t *cursor = fieldmap;
    // Move data from core list to output dict
    while( cursor->srcpos != -1 ) {
      __f_render_pyobject rfunc = (__f_render_pyobject)cursor->render;
      vgx_ResponseFieldValue_t val = entry[ cursor->srcpos ].value;
      PyObject *py_obj;
      PyObject *py_dict;
      if( cursor->attr & VGX_RESPONSE_ATTRS_PREDICATOR ) {
        if( py_arc_dict == NULL ) {
          if( (py_arc_dict = PyDict_New()) != NULL ) {
            PyDict_SetItemString( py_entry_dict, "arc", py_arc_dict );
          }
        }
        py_dict = py_arc_dict;
        if( distance < 0 ) {
          if( val.pred.eph.type == VGX_PREDICATOR_EPH_TYPE_DISTANCE ) {
            distance = val.pred.eph.value;
            PyVGX_DictStealItemString( py_entry_dict, "distance", PyLong_FromLong( distance ) );
          }
        }
      }
      else {
        py_dict = py_entry_dict;
      }
      if( rfunc( val, &py_obj ) && py_dict ) {
        const char *name = cursor->fieldname;
        PyVGX_DictStealItemString( py_dict, name, py_obj );
      }
      ++cursor;
    }
    PyVGX_XDECREF( py_arc_dict );
  }
  return py_entry_dict;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__dict_entries( const vgx_SearchResult_t *search_result, const vgx_ResponseFieldMap_t *fieldmap ) {
  int64_t length = search_result->list_length;
  int width = search_result->list_width;
  vgx_ResponseFieldData_t *entry = search_result->list;
  PyObject *py_result_list = PyList_New( length );
  if( py_result_list ) {
    PyObject *py_entry;
    for( int64_t n=0; n<length; n++ ) {
      if( (py_entry = __py_dict__from_result_entry( fieldmap, entry )) != NULL ) {
        PyList_SET_ITEM( py_result_list, n, py_entry );
        py_entry = NULL;
      }
      // Next entry
      entry += width;
    }
  }
  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_list__nested_dict_entries( const vgx_SearchResult_t *search_result, const vgx_ResponseFieldMap_t *fieldmap, int64_t nested_hits ) {

  if( nested_hits < 0 ) {
    nested_hits = LLONG_MAX;
  }

  vgx_ResponseAttrFastMask predmask = search_result->list_fields.fastmask & VGX_RESPONSE_ATTRS_MASK_PRED;
  int64_t n_pred_fields = POPCNT32( predmask ); 

  int64_t err = 0;

  PyObject *py_key__arc = PyUnicode_FromString( "arc" );
  PyObject *py_key__next = PyUnicode_FromString( "next" );
  PyObject *py_result_list = PyList_New( 0 );
  if( py_result_list && py_key__arc && py_key__next ) {

    // Level map index
    int level = 0;
    // Level maps
    PyObject *py_level_map[16] = {0};

    PyObject *py_this_level;
    PyObject *py_previous_level;
    PyObject *py_next;
    PyObject *py_inner;
    PyObject *py_rendered_value;

    // Tail
    PyObject *py_A = NULL;
    // Head
    PyObject *py_B = NULL;


    // Implied fields (at least): anchor, predicator, id.
    // 

    int64_t length = search_result->list_length;
    int width = search_result->list_width;
    vgx_ResponseFieldData_t *entry = search_result->list;
    PyObject *py_entry = NULL;
    PyObject *py_arc_tuple = NULL;
    for( int64_t n=0; n<length; n++ ) {
      if( (py_entry = PyDict_New()) == NULL ) {
        --err;
        break;
      }
      if( (py_arc_tuple = PyTuple_New( n_pred_fields )) == NULL ) {
        --err;
        break;
      }
      if( (err = PyDict_SetItem( py_entry, py_key__arc, py_arc_tuple )) < 0 ) {
        break;
      }
      
      Py_DECREF( py_arc_tuple ); // give up ownership, now borrowed from result entry
      int arc_idx = 0;


      // Process all values in entry
      const vgx_ResponseFieldMap_t *cursor = fieldmap;
      // Move data from core list to output dict
      while( cursor->srcpos != -1 ) {
        // Get field value
        vgx_ResponseFieldValue_t val = entry[ cursor->srcpos ].value;

        // Render field value
        __f_render_pyobject rfunc = (__f_render_pyobject)cursor->render;
        if( rfunc( val, &py_rendered_value ) == NULL ) {
          --err;
          break;
        }

        vgx_ResponseAttrFastMask arcfield = cursor->attr & VGX_RESPONSE_ATTRS_MASK_PRED;

        // Field is part of arc 
        if( arcfield ) {
          if( arcfield == VGX_RESPONSE_ATTR_VALUE ) {
            level = val.pred.eph.value;
          }
          // Populate arc tuple with rendered value
          PyTuple_SET_ITEM( py_arc_tuple, arc_idx, py_rendered_value );
          ++arc_idx;
        }
        // Non-arc field
        else {
          if( cursor->attr == VGX_RESPONSE_ATTR_ANCHOR ) {
            // Capture and own tail object
            py_A = py_rendered_value; // own
          }
          else {
            const char *name = cursor->fieldname;
            if( cursor->attr == VGX_RESPONSE_ATTR_ID ) {
              // Capture and own head object, and populate entry dict
              py_B = py_rendered_value; // own
              if( (err = PyDict_SetItemString( py_entry, name, py_rendered_value )) < 0 ) {
                break;
              }
            }
            else {
              // Populate entry dict with rendered value
              if( (err = PyDict_SetItemString( py_entry, name, py_rendered_value )) < 0 ) {
                Py_DECREF( py_rendered_value );
                break;
              }
            }
          }
        }


        // TODO:  Optimize:
        //  When we have level >= 2 AND both py_A and py_B have been found (consider NOT rendering py_A and py_B until source data for both found!)
        //  then we retrieve the level dict for this vertex and make sure we STOP 
        //  rendering if the inner result list length will exceed nested_hits.
        //
        //


        ++cursor;
      }

      if( err < 0 || py_A == NULL || py_B == NULL ) {
        --err;
        break;
      }

      // Use map for current nesting level
      if( (py_this_level = py_level_map[ level ]) == NULL ) {
        py_this_level = PyDict_New();
        if( (py_level_map[ level ] = py_this_level) == NULL ) {
          --err;
          break;
        }
      }

      // Retrieve inner result list for B, or create new
      if( (py_next = PyDict_GetItem( py_this_level, py_B )) == NULL ) {
        if( (py_next = PyList_New( 0 )) == NULL ) {
          --err;
          break;
        }
        // Assign inner list to B at current neighborhood level map
        err = PyDict_SetItem( py_this_level, py_B, py_next );
        Py_DECREF( py_next );
        if( err < 0 ) {
          break;
        }
      }
      Py_DECREF( py_B );
      py_B = NULL;
      // Populate entry dict with inner result list for B
      if( (err = PyDict_SetItem( py_entry, py_key__next, py_next )) < 0 ) {
        break;
      }

      // Immediate neighborhood (or self reference) entries into main list
      if( level < 2 ) {
        // Append entry dict to result list
        // TODO: CONSIDER using the top level hits= param to control this?
        if( (err = PyList_Append( py_result_list, py_entry )) < 0 ) {
          break;
        }
        Py_DECREF( py_entry );
        py_entry = NULL;
      }
      // Extended neighborhood entries
      else {
        // Use map for previous nesting level
        if( (py_previous_level = py_level_map[ level-1 ]) == NULL ) {
          py_previous_level = PyDict_New();
          if( (py_level_map[ level-1 ] = py_previous_level) == NULL ) {
            --err;
            break;
          }
        }
        // Retrive inner result list for A and append entry
        if( (py_inner = PyDict_GetItem( py_previous_level, py_A )) != NULL ) {
          // TODO: MOVE this check before we waste time rendering into py_entry!!
          if( PyList_GET_SIZE( py_inner ) < nested_hits ) {
            if( (err = PyList_Append( py_inner, py_entry )) < 0 ) {
              break;
            }
          }
          Py_DECREF( py_entry );
          py_entry = NULL;
        }
        // Create new inner result list for A and set single entry
        else {
          if( (py_inner = PyList_New( 1 )) == NULL ) {
            --err;
            break;
          }
          // Nest entry into the inner list
          PyList_SET_ITEM( py_inner, 0, py_entry );
          py_entry = NULL; // stolen by inner list
          // Assign inner list to A at previous neighborhood level map
          err = PyDict_SetItem( py_previous_level, py_A, py_inner );
          Py_DECREF( py_inner );
          py_inner = NULL;
          if( err < 0 ) {
            break;
          }
        }
      }

      Py_DECREF( py_A );
      py_A = NULL;

      // Next entry
      entry += width;
    }

    Py_XDECREF( py_A );
    Py_XDECREF( py_B );
    Py_XDECREF( py_entry );

    const int64_t n = sizeof( py_level_map ) / sizeof( PyObject* );
    PyObject **py_map = py_level_map;
    PyObject **py_map_end = py_map + n;
    while( py_map < py_map_end ) {
      Py_XDECREF( *py_map );
      ++py_map;
    }
  }

  Py_XDECREF( py_key__arc );
  Py_XDECREF( py_key__next );

  if( err < 0 ) {
    Py_XDECREF( py_result_list );
    py_result_list = NULL;
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyVGX_ResultError, "Internal error: failed to render result" );
    }
  }

  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_search_result__py_result_list_from_search_result( vgx_SearchResult_t *search_result, bool nested, int64_t nested_hits ) {

  
  PyObject *py_result_list = NULL;
  vgx_ResponseFieldMap_t *fieldmap = NULL;

  double t_pyresult = 0.0;
  double *pt_pyresult = NULL;
  bool with_timing = (search_result->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_TIMING) != 0;
  
  if( with_timing ) {
    pt_pyresult = &t_pyresult;
  }

  __START_TIMED_BLOCK( pt_pyresult, NULL, NULL ) {
    XTRY {
      int64_t length = search_result->list_length;
      int width = search_result->list_width;

      if( length > 0 && search_result->list ) {
        // Create the fieldmap
        if( !vgx_response_show_as_string(search_result->list_fields.fastmask) ) {
          vgx_ResponseFieldData_t *first_entry = search_result->list; // at least one entry so this is ok
          if( (fieldmap = iGraphResponse.NewFieldMap( first_entry, width, pyobj_fieldmap_definition )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0xA11 );
          }
        }

        vgx_ResponseAttrFastMask mask = vgx_response_show_as( search_result->list_fields.fastmask );

        BEGIN_PYTHON_INTERPRETER {
          // Create nested result list
          if( nested && mask == VGX_RESPONSE_SHOW_AS_DICT ) {
            py_result_list = __py_list__nested_dict_entries( search_result, fieldmap, nested_hits );
          }
          // Create flat result list
          else {
            // Process all entries
            switch( mask ) {
            case VGX_RESPONSE_SHOW_AS_STRING:
              py_result_list = __py_list__single_string_entries( search_result );
              break;
            case VGX_RESPONSE_SHOW_AS_LIST:
              py_result_list = __py_list__tuple_entries( search_result, fieldmap );
              break;
            case VGX_RESPONSE_SHOW_AS_DICT:
              py_result_list = __py_list__dict_entries( search_result, fieldmap );
              break;
            default:
              py_result_list = __py_list__single_object_entries( search_result, fieldmap );
            }
          }
        } END_PYTHON_INTERPRETER;

        if( py_result_list == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xA12 );
        }

      }
      // Zero hits
      else {
        BEGIN_PYTHON_INTERPRETER {
          py_result_list = PyList_New(0);
        } END_PYTHON_INTERPRETER;
      }

    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_XDECREF( py_result_list );
        py_result_list = NULL;
        if( !PyErr_Occurred() ) {
          PyVGXError_SetString( PyExc_Exception, "internal error" );
        }
      } END_PYTHON_INTERPRETER;
    }
    XFINALLY {
      if( fieldmap ) {
        free( fieldmap );
      }
    }
  } __END_TIMED_BLOCK;

  search_result->exe_time.t_result += t_pyresult;

  return py_result_list;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static const char * __vertex_obid_string( const vgx_Vertex_t *vertex ) {
  static __THREAD char buffer[33];
  objectid_t obid = CALLABLE(vertex)->InternalID(vertex);
  return idtostr( buffer, &obid );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _ipyvgx_search_result__py_dict_from_vertex_properties( vgx_Vertex_t *vertex ) {
  PyObject *py_dict = NULL;

  BEGIN_PYVGX_THREADS {
    // Generate a property list from the vertex (we will own it here)
    vgx_SelectProperties_t *all_properties = CALLABLE( vertex )->GetProperties( vertex );

    // Populate python dict from properties
    __py_dict__from_select_properties( all_properties, &py_dict );

    // Delete the property list
    iVertexProperty.FreeSelectProperties( vertex->graph, &all_properties );
  } END_PYVGX_THREADS;

  // May or may not be NULL
  return py_dict;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN IPyVGXSearchResult iPyVGXSearchResult = {
  .PyResultList_FromSearchResult      = _ipyvgx_search_result__py_result_list_from_search_result,
  .PyPredicatorValue_FromArcHead      = _ipyvgx_search_result__py_predicator_val_from_archead,
  .PyDict_FromVertexProperties        = _ipyvgx_search_result__py_dict_from_vertex_properties
};
