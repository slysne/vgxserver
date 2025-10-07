/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_evalmem.c
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


#define TEST_PARENT_GRAPH( PyMemObj )  ((PyMemObj)->py_parent != NULL && (PyMemObj)->parent == (PyMemObj)->py_parent->graph)

#define ASSERT_PARENT_GRAPH( PyMemObj )                 \
  if( !TEST_PARENT_GRAPH( PyMemObj ) ) {                \
    PyErr_SetString( PyExc_Exception, "no graph" );     \
    return NULL;                                        \
  }


#define PyVGX_MEMORY_LEN( PyMemObj )  (1LL << (PyMemObj)->evalmem->order)



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * __pylist_singleton( uint64_t value ) {
  PyObject *py_list = PyList_New( 1 );
  if( py_list ) {
    PyList_SET_ITEM( py_list, 0, PyVGX_PyLong_FromUnsignedLongLongNoErr( value ) );
  }
  return py_list;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static CString_t * __new_string_literal( PyVGX_Memory *pymem, PyObject *py_data ) {
  const char *data;
  Py_ssize_t sz;
  Py_ssize_t ucsz;
  CString_attr attr;

  ASSERT_PARENT_GRAPH( pymem )
  
  if( PyByteArray_CheckExact( py_data ) ) {
    data = PyByteArray_AS_STRING( py_data );
    sz = PyByteArray_GET_SIZE( py_data );
    ucsz = 0;
    attr = CSTRING_ATTR_BYTEARRAY;
  }
  else if( PyBytes_CheckExact( py_data ) ) {
    data = PyBytes_AS_STRING( py_data );
    sz = PyBytes_GET_SIZE( py_data );
    ucsz = 0;
    attr = CSTRING_ATTR_BYTES;
  }
  else if( PyUnicode_Check( py_data ) ) {
    if( (data = PyUnicode_AsUTF8AndSize( py_data, &sz )) == NULL ) {
      return NULL;
    }
    ucsz = PyUnicode_GET_LENGTH( py_data );
    attr = CSTRING_ATTR_NONE;
  }
  else {
    PyErr_SetString( PyExc_TypeError, "a string or bytes-like object is required" );
    return NULL;
  }
  
  object_allocator_context_t *alloc = pymem->py_parent->graph->ephemeral_string_allocator_context;
  if( sz > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
    alloc = NULL;
  }

  CString_constructor_args_t cargs = {
    .string       = data,
    .len          = (int)sz,
    .ucsz         = (int)ucsz,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = alloc
  };

  vgx_ExpressEvalString_t *str;
  int err = 0;

  BEGIN_PYVGX_THREADS {
    str = calloc( 1, sizeof( vgx_ExpressEvalString_t ) );
    if( str && sz < INT_MAX && (str->CSTR__literal = COMLIB_OBJECT_NEW( CString_t, NULL, &cargs )) != NULL ) {
      CStringAttributes( str->CSTR__literal ) = attr;
    }
    else {
      err = -1;
    }
  } END_PYVGX_THREADS;

  if( err < 0 ) {
    free( str );
    PyErr_SetString( PyExc_MemoryError, "internal string error" );
    return NULL;
  }

  if( pymem->str_tail ) {
    pymem->str_tail->next = str;
    pymem->str_tail = str;
  }
  else {
    pymem->str_head = pymem->str_tail = str;
  }
  return str->CSTR__literal;

}



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_Vector_t * __own_vector_literal( PyVGX_Memory *pymem, PyObject *py_data ) {

  ASSERT_PARENT_GRAPH( pymem )
  vgx_Graph_t *graph = pymem->parent;
  
  if( !PyVGX_Vector_CheckExact( py_data ) || !igraphfactory.EuclideanVectors() ) {
    PyErr_SetString( PyExc_TypeError, "Euclidean vector object is required" );
    return NULL;
  }

  PyVGX_Vector *py_vector = (PyVGX_Vector*)py_data;

  vgx_Vector_t *vector = py_vector->vint;

  vgx_Similarity_t *sim = CALLABLE( vector )->Context( vector )->simobj;
  if( sim != graph->similarity ) {
    PyErr_SetString( PyExc_Exception, "incompatible vector similarity context" );
    return NULL;
  }

  // Optimization: We are repeatedly setting the same vector, no additional ownership
  if( pymem->vector_tail && pymem->vector_tail->vector == vector ) {
    return vector;
  }

  vgx_ExpressEvalVector_t *vec = calloc( 1, sizeof( vgx_ExpressEvalVector_t ) );
  if( vec == NULL ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }

  CALLABLE( vector )->Incref( vector );
  vec->vector = vector;

  if( pymem->vector_tail ) {
    pymem->vector_tail->next = vec;
    pymem->vector_tail = vec;
  }
  else {
    pymem->vector_head = pymem->vector_tail = vec;
  }

  return vector;

}



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * __pyobject_from_string_literal( PyVGX_Memory *pymem, const CString_t *CSTR__string ) {
  ASSERT_PARENT_GRAPH( pymem )
  if( CStringCheck( CSTR__string ) ) {
    const char *data = CStringValue( CSTR__string );
    int64_t sz = CStringLength( CSTR__string );
    CString_attr attr = CStringAttributes( CSTR__string );
    if( attr & CSTRING_ATTR_BYTEARRAY ) {
      return PyByteArray_FromStringAndSize( data, sz );
    }
    else if( attr & CSTRING_ATTR_BYTES ) {
      return PyBytes_FromStringAndSize( data, sz );
    }
    else {
      return PyUnicode_FromStringAndSize( data, sz );
    }
  }
  else {
    return PyFloat_FromDouble( Py_NAN );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * __pyunicode_from_vertexid( PyVGX_Memory *pymem, const vgx_VertexIdentifier_t *vertexid ) {
  ASSERT_PARENT_GRAPH( pymem )
  if( vertexid ) {
    if( vertexid->CSTR__idstr ) {
      return PyUnicode_FromStringAndSize( CStringValue( vertexid->CSTR__idstr ), CStringLength( vertexid->CSTR__idstr ) );
    }
    else {
      return PyUnicode_FromString( vertexid->idprefix.data );
    }
  }
  else {
    return PyFloat_FromDouble( Py_NAN );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * __pyunicode_from_vertex( PyVGX_Memory *pymem, const vgx_Vertex_t *vertex ) {
  ASSERT_PARENT_GRAPH( pymem )
  if( vertex && !__vertex_is_manifestation_null( vertex ) ) { 
    return __pyunicode_from_vertexid( pymem, CALLABLE( vertex )->Identifier( vertex ) );
  }
  else {
    return PyFloat_FromDouble( Py_NAN );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static void __discard_string_literals( PyVGX_Memory *pymem ) {
  vgx_ExpressEvalString_t *str = pymem->str_head;
  vgx_ExpressEvalString_t *cur;

  if( TEST_PARENT_GRAPH( pymem ) ) {
    while( (cur = str) != NULL ) {
      str = cur->next;
      iString.Discard( &cur->CSTR__literal );
      free( cur );
    }
  }
  else {
    while( (cur = str) != NULL ) {
      str = cur->next;
      free( cur );
    }
  }

  pymem->str_head = pymem->str_tail = NULL;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static void __discard_vector_literals( PyVGX_Memory *pymem ) {
  vgx_ExpressEvalVector_t *vec = pymem->vector_head;
  vgx_ExpressEvalVector_t *cur;

  if( TEST_PARENT_GRAPH( pymem ) ) {
    while( (cur = vec) != NULL ) {
      vec = cur->next;
      CALLABLE( cur->vector )->Decref( cur->vector );
      free( cur );
    }
  }
  else {
    while( (cur = vec) != NULL ) {
      vec = cur->next;
      free( cur );
    }
  }

  pymem->vector_head = pymem->vector_tail = NULL;
}



/******************************************************************************
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Memory__get_order( PyVGX_Memory *pymem, void *closure ) {
  return PyLong_FromLong( pymem->evalmem->order );
}



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Memory__get_REG( PyVGX_Memory *pymem, int idx ) {

  ASSERT_PARENT_GRAPH( pymem )

  vgx_ExpressEvalMemory_t *memory = pymem->evalmem;
  vgx_EvalStackItem_t *item = &memory->data[ idx & memory->mask ];
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    return PyLong_FromLongLong( item->integer );
  case STACK_ITEM_TYPE_BITVECTOR:
    return __pylist_singleton( item->bits );
  case STACK_ITEM_TYPE_KEYVAL:
    return iPyVGXBuilder.TupleFromCStringMapKeyVal( &item->bits );
  case STACK_ITEM_TYPE_REAL:
    return PyFloat_FromDouble( item->real );
  case STACK_ITEM_TYPE_CSTRING:
    return __pyobject_from_string_literal( pymem, item->CSTR__str );
  case STACK_ITEM_TYPE_VERTEXID:
    return __pyunicode_from_vertexid( pymem, item->vertexid );
  case STACK_ITEM_TYPE_VERTEX:
    return __pyunicode_from_vertex( pymem, item->vertex );
  default:
    return PyFloat_FromDouble( Py_NAN );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static int __PyVGX_Memory__set_REG( PyVGX_Memory *pymem, int64_t idx, PyObject *py_obj ) {
  vgx_ExpressEvalMemory_t *memory = pymem->evalmem;
  vgx_EvalStackItem_t *item = &memory->data[ idx & memory->mask ];
  if( py_obj ) {
    if( PyList_CheckExact( py_obj ) && PyList_GET_SIZE( py_obj ) == 1 ) {
      PyObject *py_item = PyList_GET_ITEM( py_obj, 0 );
      if( PyLong_CheckExact( py_item ) ) {
        item->bits = PyLong_AsUnsignedLongLong( py_item );
      }
      else {
        PyErr_SetString( PyExc_TypeError, "an integer is required" );
        return -1;
      }
      item->type = STACK_ITEM_TYPE_BITVECTOR; 
    }
    else {
      if( PyLong_CheckExact( py_obj ) ) {
        if( (item->integer = PyLong_AsLongLong( py_obj )) == -1 && PyErr_Occurred() ) {
          return -1;
        }
        item->type = STACK_ITEM_TYPE_INTEGER;
      }
      else if( PyFloat_CheckExact( py_obj ) ) {
        item->real = PyFloat_AS_DOUBLE( py_obj );
        item->type = STACK_ITEM_TYPE_REAL;
      }
      else if( PyTuple_CheckExact( py_obj ) && PyTuple_GET_SIZE( py_obj ) == 2 ) {
        PyObject *py_key = PyTuple_GET_ITEM( py_obj, 0 );
        PyObject *py_val = PyTuple_GET_ITEM( py_obj, 1 );
        if( PyLong_CheckExact( py_key ) && PyNumber_Check( py_val ) ) {
          item->bits = vgx_cstring_array_map_item_from_key_and_val( PyLong_AsLong( py_key ), (float)PyFloat_AsDouble( py_val ) );
          item->type = STACK_ITEM_TYPE_KEYVAL;
        }
        else {
          PyErr_SetString( PyExc_TypeError, "a tuple (int, num) is required" );
          return -1;
        }
      }
      else if( PyByteArray_CheckExact( py_obj ) || PyVGX_PyObject_CheckString( py_obj ) ) {
        if( (item->CSTR__str = __new_string_literal( pymem, py_obj )) == NULL ) {
          return -1;
        }
        item->type = STACK_ITEM_TYPE_CSTRING;
      }
      else if( PyVGX_Vector_CheckExact( py_obj ) ) {
        if( (item->vector = __own_vector_literal( pymem, py_obj )) == NULL ) {
          return -1;
        }
        item->type = STACK_ITEM_TYPE_VECTOR;
      }
      else {
        PyErr_SetString( PyExc_TypeError, "valid types are: number, string, vector, bytearray, keyval, singleton list [n]" );
        return -1;
      }
    }
  }
  else {
    item->integer = 0;
    item->type = STACK_ITEM_TYPE_INTEGER;
  }

  return 0;
}



/**************************************************************************//**
 * __PyVGX_Memory__get_R1
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Memory__get_R1( PyVGX_Memory *pymem, void *closure ) {
  return __PyVGX_Memory__get_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R1 );
}



/**************************************************************************//**
 * __PyVGX_Memory__set_R1
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Memory__set_R1( PyVGX_Memory *pymem, PyObject *py_obj, void *closure ) {
  return __PyVGX_Memory__set_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R1, py_obj );
}



/**************************************************************************//**
 * __PyVGX_Memory__get_R2
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Memory__get_R2( PyVGX_Memory *pymem, void *closure ) {
  return __PyVGX_Memory__get_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R2 );
}



/**************************************************************************//**
 * __PyVGX_Memory__set_R2
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Memory__set_R2( PyVGX_Memory *pymem, PyObject *py_obj, void *closure ) {
  return __PyVGX_Memory__set_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R2, py_obj );
}



/**************************************************************************//**
 * __PyVGX_Memory__get_R3
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Memory__get_R3( PyVGX_Memory *pymem, void *closure ) {
  return __PyVGX_Memory__get_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R3 );
}



/**************************************************************************//**
 * __PyVGX_Memory__set_R3
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Memory__set_R3( PyVGX_Memory *pymem, PyObject *py_obj, void *closure ) {
  return __PyVGX_Memory__set_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R3, py_obj );
}



/**************************************************************************//**
 * __PyVGX_Memory__get_R4
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Memory__get_R4( PyVGX_Memory *pymem, void *closure ) {
  return __PyVGX_Memory__get_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R4 );
}



/**************************************************************************//**
 * __PyVGX_Memory__set_R4
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Memory__set_R4( PyVGX_Memory *pymem, PyObject *py_obj, void *closure ) {
  return __PyVGX_Memory__set_REG( pymem, EXPRESS_EVAL_MEM_REGISTER_R4, py_obj );
}



/******************************************************************************
 * PyVGX_Memory__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Memory__dealloc( PyVGX_Memory *pymem ) {
  if( pymem->threadid == GET_CURRENT_THREAD_ID() ) {
    vgx_ExpressEvalMemory_t *evalmem = pymem->evalmem;
    if( evalmem ) {
      iEvaluator.DiscardMemory( &evalmem );
    }
    __discard_string_literals( pymem );
    __discard_vector_literals( pymem );
    Py_TYPE( pymem )->tp_free( pymem );
  }
}



/******************************************************************************
 * __new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __new( void ) {
  PyObject *pyobj = p_PyVGX_Memory__MemoryType->tp_alloc( p_PyVGX_Memory__MemoryType, 0 );
  if( pyobj ) {
    PyVGX_Memory *pymem = (PyVGX_Memory*)pyobj;
    pymem->py_parent = NULL;
    pymem->parent = NULL;
    pymem->evalmem = NULL;
    pymem->str_head = NULL;
    pymem->str_tail = NULL;
    pymem->vector_head = NULL;
    pymem->vector_tail = NULL;
    pymem->threadid = 0;
  }
  return pyobj;
}



/******************************************************************************
 * __init
 *
 ******************************************************************************
 */
static int __init( PyVGX_Memory *pymem, PyObject *pygraph, PyObject *py_source ) {
  if( pygraph == NULL ) {
    PyErr_SetString( PyExc_TypeError, "graph object required" );
    return -1;
  }

  if( PyVGX_Graph_Check(pygraph) ) {
    pymem->py_parent = (PyVGX_Graph*)pygraph;
  }
  else if( PyCapsule_CheckExact(pygraph) ) {
    pymem->py_parent = (PyVGX_Graph*)PyCapsule_GetPointer( pygraph, NULL );
    if( pymem->py_parent == NULL || pymem->py_parent->graph == NULL || !COMLIB_OBJECT_ISINSTANCE( pymem->py_parent->graph, vgx_Graph_t ) ) {
      PyVGXError_SetString( PyExc_ValueError, "graph capsule does not contain a graph object" );
      return -1;
    }
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Invalid graph object" );
    return -1;
  }

  if( py_source == NULL ) {
    PyErr_SetString( PyExc_TypeError, "source object required" );
    return -1;
  }

  if( (pymem->evalmem = iPyVGXParser.NewExpressEvalMemory( pymem->py_parent->graph, py_source )) == NULL ) {
    return -1;
  }

  // Only this thread is allowed to operate on this memory
  pymem->threadid = GET_CURRENT_THREAD_ID();

  // Safeguard: py_parent's graph reference should remain consistent
  pymem->parent = pymem->py_parent->graph;

  return 0;
}



/******************************************************************************
 * PyVGX_Memory__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Memory__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  return __new();
}



/******************************************************************************
 * PyVGX_Memory__init
 *
 ******************************************************************************
 */
static int PyVGX_Memory__init( PyVGX_Memory *pymem, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "graph", "source", NULL };
  PyObject *pygraph;
  PyObject *py_source;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "OO", kwlist, &pygraph, &py_source ) ) {
    return -1;
  }

  return __init( pymem, pygraph, py_source );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Memory__vectorcall( PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames ) {
  static const char *kwlist[] = {
    "graph",
    "source",
    NULL
  };

  typedef union u_memory_args {
    PyObject *args[2];
    struct {
      PyObject *pygraph;
      PyObject *py_source;
    };
  } memory_args;

  memory_args vcargs = {0};

  int64_t nargs = PyVectorcall_NARGS( nargsf );

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs.args ) < 0 ) {
    return NULL;
  }

  PyObject *pyobj = __new();
  if( pyobj ) {
    if( __init( (PyVGX_Memory*)pyobj, vcargs.pygraph, vcargs.py_source ) < 0 ) {
      PyVGX_Memory__dealloc( (PyVGX_Memory*)pyobj );
      return NULL;
    }
  }
  return pyobj;
}



/******************************************************************************
 * PyVGX_Memory__AsList
 *
 ******************************************************************************
 */
PyDoc_STRVAR( AsList__doc__,
  "AsList() -> list\n"
);

/**************************************************************************//**
 * PyVGX_Memory__AsList
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory__AsList( PyVGX_Memory *pymem ) {
  if( pymem->threadid != GET_CURRENT_THREAD_ID() ) {
    PyVGXError_SetString( PyVGX_AccessError, "Not owner thread" );
    return NULL;
  }

  size_t sz =  PyVGX_MEMORY_LEN( pymem );
  PyObject *py_list = PyList_New( sz );
  if( py_list == NULL ) {
    return NULL;
  }
  size_t idx = 0;
  vgx_EvalStackItem_t *cursor = pymem->evalmem->data;
  vgx_EvalStackItem_t *last = cursor + pymem->evalmem->mask;
  do {
    PyObject *py_obj = NULL;
    switch( cursor->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      py_obj = PyLong_FromLongLong( cursor->integer );
      break;
    case STACK_ITEM_TYPE_BITVECTOR:
      py_obj = __pylist_singleton( cursor->bits );
      break;
    case STACK_ITEM_TYPE_KEYVAL:
      py_obj = iPyVGXBuilder.TupleFromCStringMapKeyVal( &cursor->bits );
      break;
    case STACK_ITEM_TYPE_REAL:
      if( !isnan( cursor->real ) ) {
        py_obj = PyFloat_FromDouble( cursor->real );
      }
      break;
    case STACK_ITEM_TYPE_CSTRING:
      py_obj = __pyobject_from_string_literal( pymem, cursor->CSTR__str );
      break;
    case STACK_ITEM_TYPE_VERTEXID:
      py_obj = __pyunicode_from_vertexid( pymem, cursor->vertexid );
      break;
    case STACK_ITEM_TYPE_VERTEX:
      py_obj = __pyunicode_from_vertex( pymem, cursor->vertex );
      break;
    default:
      break;
    }

    if( py_obj == NULL ) {
      PyErr_Clear();
      py_obj = Py_None;
      Py_INCREF( py_obj );
    }

    PyList_SET_ITEM( py_list, idx, py_obj );
    ++idx;

  } while( cursor++ < last );

  return py_list;
}



/******************************************************************************
 * PyVGX_Memory__Stack
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Stack__doc__,
  "Stack() -> list\n"
);

/**************************************************************************//**
 * PyVGX_Memory__Stack
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory__Stack( PyVGX_Memory *pymem ) {
  if( pymem->threadid != GET_CURRENT_THREAD_ID() ) {
    PyVGXError_SetString( PyVGX_AccessError, "Not owner thread" );
    return NULL;
  }

  // NOTE: The memory stack grows downward
  vgx_ExpressEvalMemory_t *mem = pymem->evalmem;
  vgx_EvalStackItem_t *bottom = &mem->data[ -5 & mem->mask ];
  vgx_EvalStackItem_t *top = &mem->data[ (mem->sp+1) & mem->mask ];

  size_t sz = top <= bottom ? (bottom - top) + 1 : 0;
  PyObject *py_list = PyList_New( sz );
  if( py_list == NULL ) {
    return NULL;
  }
  size_t idx = 0;
  vgx_EvalStackItem_t *sp = bottom;
  while( sp >= top ) {
    PyObject *py_obj;
    switch( sp->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      py_obj = PyLong_FromLongLong( sp->integer );
      break;
    case STACK_ITEM_TYPE_BITVECTOR:
      py_obj = __pylist_singleton( sp->bits );
      break;
    case STACK_ITEM_TYPE_KEYVAL:
      py_obj = iPyVGXBuilder.TupleFromCStringMapKeyVal( &sp->bits );
      break;
    case STACK_ITEM_TYPE_REAL:
      py_obj = PyFloat_FromDouble( sp->real );
      break;
    case STACK_ITEM_TYPE_CSTRING:
      py_obj = __pyobject_from_string_literal( pymem, sp->CSTR__str );
      break;
    case STACK_ITEM_TYPE_VERTEXID:
      py_obj = __pyunicode_from_vertexid( pymem, sp->vertexid );
      break;
    case STACK_ITEM_TYPE_VERTEX:
      py_obj = __pyunicode_from_vertex( pymem, sp->vertex );
      break;
    default:
      py_obj = PyVGX_PyFloat_FromDoubleNoErr( Py_NAN );
      break;
    }

    if( py_obj == NULL ) {
      PyErr_Clear();
      Py_INCREF( Py_None );
      py_obj = Py_None;
    }

    PyList_SET_ITEM( py_list, idx, py_obj );

    ++idx;
    --sp;
  }

  return py_list;
}



/******************************************************************************
 * PyVGX_Memory__Reset
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Reset__doc__,
  "Reset( [value [,increment] ] ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Memory__Reset
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory__Reset( PyVGX_Memory *pymem, PyObject *args, PyObject *kwds ) {
  // Args
  static char *kwlist[] = {"value", "increment", NULL};
  PyObject *py_value = NULL;
  PyObject *py_increment = NULL;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist, &py_value, &py_increment ) ) {
    return NULL;
  }

  if( pymem->threadid != GET_CURRENT_THREAD_ID() ) {
    PyVGXError_SetString( PyVGX_AccessError, "not owner thread" );
    return NULL;
  }

  vgx_EvalStackItem_t value = {
    .type     = STACK_ITEM_TYPE_INTEGER,
    .integer  = 0
  };

  if( py_value ) {
    if( PyLong_CheckExact( py_value ) ) {
      if( (value.integer = PyLong_AsLongLong( py_value )) == -1 && PyErr_Occurred() ) {
        return NULL;
      }
    }
    else if( PyFloat_CheckExact( py_value ) ) {
      value.type = STACK_ITEM_TYPE_REAL;
      value.real = PyFloat_AS_DOUBLE( py_value );
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "a numeric value is required" );
      return NULL;
    }
  }

  int64_t iinc = 0;
  double dinc = 0.0;

  if( py_increment ) {
    if( PyLong_CheckExact( py_increment ) ) {
      if( (iinc = PyLong_AsLongLong( py_increment )) == -1 && PyErr_Occurred() ) {
        return NULL;
      }
      dinc = (double)iinc;
    }
    else if( PyFloat_CheckExact( py_increment ) ) {
      dinc = PyFloat_AS_DOUBLE( py_increment );
      iinc = (int64_t)dinc;
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "a numeric value is required" );
      return NULL;
    }
  }

  // Clean up string and vector data
  __discard_string_literals( pymem );
  __discard_vector_literals( pymem );


  vgx_ExpressEvalMemory_t *mem = pymem->evalmem;
  vgx_EvalStackItem_t *cursor = mem->data;
  vgx_EvalStackItem_t *last = cursor + mem->mask;


  // Set all values
  BEGIN_PYVGX_THREADS {
    if( py_increment == NULL ) {
      while( cursor <= last ) {
        *cursor++ = value;
      }
    }
    else if( value.type == STACK_ITEM_TYPE_INTEGER ) {
      while( cursor <= last ) {
        *cursor++ = value;
        value.integer += iinc;
      }
    }
    else {
      while( cursor <= last ) {
        *cursor++ = value;
        value.real += dinc;
      }
    }
    iEvaluator.ClearCStrings( mem );
    iEvaluator.ClearVectors( mem );
    iEvaluator.ClearDWordSet( mem );
  } END_PYVGX_THREADS;

  // Reset stack pointer to right below register R4
  mem->sp = -5;


  Py_RETURN_NONE;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __compare_real( const vgx_EvalStackItem_t *a, const vgx_EvalStackItem_t *b ) {
  return (a->real > b->real) - (a->real < b->real);
}

/**************************************************************************//**
 * __compare_real_rev
 *
 ******************************************************************************
 */
__inline static int __compare_real_rev( const vgx_EvalStackItem_t *b, const vgx_EvalStackItem_t *a ) {
  return (a->real > b->real) - (a->real < b->real);
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __compare_integer( const vgx_EvalStackItem_t *a, const vgx_EvalStackItem_t *b ) {
  return (a->integer > b->integer) - (a->integer < b->integer);
}

/**************************************************************************//**
 * __compare_integer_rev
 *
 ******************************************************************************
 */
__inline static int __compare_integer_rev( const vgx_EvalStackItem_t *b, const vgx_EvalStackItem_t *a ) {
  return (a->integer > b->integer) - (a->integer < b->integer);
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __compare_bits( const vgx_EvalStackItem_t *a, const vgx_EvalStackItem_t *b ) {
  return (a->bits > b->bits) - (a->bits < b->bits);
}

/**************************************************************************//**
 * __compare_bits_rev
 *
 ******************************************************************************
 */
__inline static int __compare_bits_rev( const vgx_EvalStackItem_t *b, const vgx_EvalStackItem_t *a ) {
  return (a->bits > b->bits) - (a->bits < b->bits);
}



/******************************************************************************
 *
 ******************************************************************************
 */
typedef int (*fcompare)( const void *, const void *);



/******************************************************************************
 * PyVGX_Memory__Sort
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Sort__doc__,
  "Sort( a, b[, reverse=False ] ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Memory__Sort
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory__Sort( PyVGX_Memory *pymem, PyObject *args, PyObject *kwds ) {
  if( pymem->threadid != GET_CURRENT_THREAD_ID() ) {
    PyVGXError_SetString( PyVGX_AccessError, "not owner thread" );
    return NULL;
  }

  int64_t sz = PyVGX_MEMORY_LEN( pymem );

  int64_t a = 0;
  int64_t b = sz + EXPRESS_EVAL_MEM_REGISTER_R4;
  int reverse = 0;
  // Args
  static char *kwlist[] = {"start", "end", "reverse", NULL};
  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|IIi", kwlist, &a, &b, &reverse ) ) {
    return NULL;
  }

  if( a < 0 || a >= sz || b < a || b > sz ) {
    PyErr_SetString( PyExc_IndexError, "indices out of range" );
    return NULL;
  }

  vgx_EvalStackItem_t *base = pymem->evalmem->data + a;
  size_t n = b-a;

  BEGIN_PYVGX_THREADS {
    // NOTE: Data type determined by first item!
    switch( base->type ) {
    case STACK_ITEM_TYPE_REAL:
      if( reverse ) {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_real_rev );
      }
      else {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_real );
      }
      break;
    case STACK_ITEM_TYPE_INTEGER:
      if( reverse ) {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_integer_rev );
      }
      else {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_integer );
      }
      break;
    default:
      if( reverse ) {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_bits_rev );
      }
      else {
        qsort( base, n, sizeof(vgx_EvalStackItem_t), (fcompare)__compare_bits );
      }
    }
  } END_PYVGX_THREADS;

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Memory__DualInt
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DualInt__doc__,
  "DualInt( a[, b] ) -> int or tuple\n"
);

/**************************************************************************//**
 * PyVGX_Memory__DualInt
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Memory__DualInt( PyObject *self, PyObject *args ) {
  
  PyObject *py_a = NULL;
  PyObject *py_b = NULL;

  if( !PyArg_ParseTuple( args, "O|O", &py_a, &py_b) ) {
    return NULL;
  }

  // Pack two 32-bit integers a and b into 64-bit value
  if( py_b ) {
    uint64_t a;
    uint32_t b;
  TWO_ARGS:
    if( PyLong_Check( py_a ) && PyLong_Check( py_b ) ) {
      a = PyLong_AsUnsignedLongLongMask( py_a );
      b = PyLong_AsUnsignedLongMask( py_b );
      if( a <= UINT32_MAX && b <= UINT32_MAX ) {
        return PyLong_FromUnsignedLongLong( (a<<32) | b );
      }
    }
    PyErr_SetString( PyExc_TypeError, "Two (32-bit) integers required" );
  }
  // Pack 2-tuple of 32-bit integers a and b into 64-bit value
  else if( py_a && PyTuple_Check( py_a ) && PyTuple_GET_SIZE( py_a ) == 2 ) {
    PyObject *py_tuple = py_a;
    py_a = PyTuple_GET_ITEM( py_tuple, 0 );
    py_b = PyTuple_GET_ITEM( py_tuple, 1 );
    goto TWO_ARGS;
  }
  // Unpack 64-bit integer a to tuple of two 32-bit integers
  else {
    PyObject *py_tuple;
    if( (py_tuple = PyTuple_New( 2 )) != NULL ) {
      if( PyLong_Check( py_a ) ) {
        uint64_t a = PyLong_AsUnsignedLongLongMask( py_a );
        int x = (int)(a >> 32);
        int y = (int)(a & 0xFFFFFFFF);
        PyTuple_SET_ITEM( py_tuple, 0, PyVGX_PyLong_FromLongNoErr( x ) );
        PyTuple_SET_ITEM( py_tuple, 1, PyVGX_PyLong_FromLongNoErr( y ) );
        return py_tuple;
      }
      PyErr_SetString( PyExc_TypeError, "Integer required" );
    }
  }
  return NULL;
}



/******************************************************************************
 * PyVGX_Memory_len
 *
 ******************************************************************************
 */
static Py_ssize_t PyVGX_Memory_len( PyVGX_Memory *pymem ) {
  return PyVGX_MEMORY_LEN( pymem );
}



/******************************************************************************
 * PyVGX_Memory_item
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory_item( PyVGX_Memory *pymem, Py_ssize_t idx ) {
  // TODO: Threadsafe
  int64_t sz = PyVGX_MEMORY_LEN( pymem );
  if( idx >= sz || idx < 0 ) {
    PyErr_SetString( PyExc_IndexError, "list index out of range" );
    return NULL;
  }

  vgx_EvalStackItem_t value = pymem->evalmem->data[ idx ];
  switch( value.type ) {
  case STACK_ITEM_TYPE_INTEGER:
    return PyLong_FromLongLong( value.integer );
  case STACK_ITEM_TYPE_BITVECTOR:
    return __pylist_singleton( value.bits );
  case STACK_ITEM_TYPE_KEYVAL:
    return iPyVGXBuilder.TupleFromCStringMapKeyVal( &value.bits );
  case STACK_ITEM_TYPE_REAL:
    return PyFloat_FromDouble( value.real );
  case STACK_ITEM_TYPE_CSTRING:
    return __pyobject_from_string_literal( pymem, value.CSTR__str );
  case STACK_ITEM_TYPE_VERTEXID:
    return __pyunicode_from_vertexid( pymem, value.vertexid );
  case STACK_ITEM_TYPE_VERTEX:
    return __pyunicode_from_vertex( pymem, value.vertex );
  case STACK_ITEM_TYPE_VECTOR:
    ASSERT_PARENT_GRAPH( pymem );
    return PyVGX_Vector__FromVector( (vgx_Vector_t*)value.vector );
  default:
    return PyFloat_FromDouble( Py_NAN );
  }
}



/******************************************************************************
 * PyVGX_Memory_get_item
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory_get_item( PyVGX_Memory *pymem, PyObject *key ) {
  int64_t sz = PyVGX_MEMORY_LEN( pymem );

  if( PyLong_Check( key ) ) {
    int64_t i = PyLong_AsLongLong( key );
    if( i < 0 ) {
      if( i == -1 && PyErr_Occurred() ) {
        return NULL;
      }
      i += sz;
    }
    return PyVGX_Memory_item( pymem, i );
  }
  else if( PySlice_Check( key ) ) {

    Py_ssize_t ilow;
    Py_ssize_t ihigh;
    Py_ssize_t step;

    if( PySlice_Unpack( key, &ilow, &ihigh, &step ) < 0 || step != 1 ) {
      return NULL;
    }


    Py_ssize_t len = PySlice_AdjustIndices( sz, &ilow, &ihigh, step );

    PyListObject *py_list = (PyListObject*)PyList_New( len );
    if( py_list == NULL ) {
      return NULL;
    }

    PyObject **dest = py_list->ob_item;
    vgx_EvalStackItem_t *src = pymem->evalmem->data + ilow;
    vgx_EvalStackItem_t *end = src + len;

    while( src < end ) {

      switch( src->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        *dest = PyLong_FromLongLong( src->integer );
        break;
      case STACK_ITEM_TYPE_BITVECTOR:
        *dest = __pylist_singleton( src->bits );
        break;
      case STACK_ITEM_TYPE_KEYVAL:
        *dest = iPyVGXBuilder.TupleFromCStringMapKeyVal( &src->bits );
        break;
      case STACK_ITEM_TYPE_REAL:
        *dest = PyFloat_FromDouble( src->real );
        break;
      case STACK_ITEM_TYPE_CSTRING:
        *dest = __pyobject_from_string_literal( pymem, src->CSTR__str );
        break;
      case STACK_ITEM_TYPE_VERTEXID:
        *dest = __pyunicode_from_vertexid( pymem, src->vertexid );
        break;
      case STACK_ITEM_TYPE_VERTEX:
        *dest = __pyunicode_from_vertex( pymem, src->vertex );
        break;
      default:
        *dest = PyFloat_FromDouble( Py_NAN );
        break;
      }

      // Failsafe
      if( *dest == NULL ) {
        *dest = Py_None;
        Py_INCREF( *dest );
      }

      ++dest;
      ++src;

    }

    return (PyObject*)py_list;
  }
  else {
    PyErr_SetString( PyExc_TypeError, "memory indices must be integers" );
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_Memory_ass_item
 *
 ******************************************************************************
 */
static int PyVGX_Memory_ass_item( PyVGX_Memory *pymem, Py_ssize_t idx, PyObject *py_value ) {
  int64_t sz = PyVGX_MEMORY_LEN( pymem );
  if( idx >= sz || idx < 0 ) {
    PyErr_SetString( PyExc_IndexError, "list index out of range" );
    return -1;
  }
  return __PyVGX_Memory__set_REG( pymem, idx, py_value );
}



/******************************************************************************
 * PyVGX_Memory_set_item
 *
 ******************************************************************************
 */
static int PyVGX_Memory_set_item( PyVGX_Memory *pymem, PyObject *key, PyObject *py_value ) {
  int64_t sz = PyVGX_MEMORY_LEN( pymem );

  if( PyLong_Check( key ) ) {
    int64_t i = PyLong_AsLongLong( key );
    if( i < 0 ) {
      if( i == -1 && PyErr_Occurred() ) {
        return -1;
      }
      i += sz;
    }
    return PyVGX_Memory_ass_item( pymem, i, py_value );
  }
  else if( PySlice_Check( key ) ) {

    Py_ssize_t ilow;
    Py_ssize_t ihigh;
    Py_ssize_t step;
    if( PySlice_Unpack( key, &ilow, &ihigh, &step ) < 0 || step != 1 ) {
      return -1;
    }

    Py_ssize_t len = PySlice_AdjustIndices( sz, &ilow, &ihigh, step );

    if( py_value ) {
      // Only allow lists to be assigned
      if( PyList_CheckExact( py_value ) ) {
        int64_t len_values = PyList_GET_SIZE( py_value );
        if( len_values > len ) {
          PyErr_SetString( PyExc_ValueError, "too many values" );
          return -1;
        }
        int64_t i = 0;
        int64_t s = ilow;
        while( i < len_values && s < ihigh ) {
          PyObject *py_item = PyList_GET_ITEM( py_value, i );
          if( __PyVGX_Memory__set_REG( pymem, s++, py_item ) < 0 ) {
            return -1;
          }
          ++i;
        }
        return 0; // ok
      }
      // error
      else {
        PyErr_SetString( PyExc_ValueError, "can only assign list" );
        return -1;
      }
    }
    else {
      int64_t s = ilow;
      vgx_ExpressEvalMemory_t *memory = pymem->evalmem;
      while( s < ihigh ) {
        vgx_EvalStackItem_t *item = &memory->data[ s++ & memory->mask ];
        item->type = STACK_ITEM_TYPE_INTEGER;
        item->integer = 0;
      }
      return 0; // ok
    }
  }
  else {
    PyErr_SetString( PyExc_TypeError, "memory indices must be integers" );
    return -1;
  }
}



/******************************************************************************
 * PyVGX_Memory__repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Memory__repr( PyVGX_Memory *pymem ) {
  return PyUnicode_FromFormat( "<pyvgx.Memory size=%lld>", PyVGX_MEMORY_LEN( pymem ) );
}



/******************************************************************************
 * PyVGX_Memory__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Memory__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Memory__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_Memory__getset[] = {
  {"order",               (getter)__PyVGX_Memory__get_order,           (setter)NULL,                    "order", NULL },
  {"R1",                  (getter)__PyVGX_Memory__get_R1,              (setter)__PyVGX_Memory__set_R1,  "R1",    NULL },
  {"R2",                  (getter)__PyVGX_Memory__get_R2,              (setter)__PyVGX_Memory__set_R2,  "R2",    NULL },
  {"R3",                  (getter)__PyVGX_Memory__get_R3,              (setter)__PyVGX_Memory__set_R3,  "R3",    NULL },
  {"R4",                  (getter)__PyVGX_Memory__get_R4,              (setter)__PyVGX_Memory__set_R4,  "R4",    NULL },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Memory__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Memory__methods[] = {

    {"AsList",            (PyCFunction)PyVGX_Memory__AsList,              METH_NOARGS,                    AsList__doc__  },
    {"Stack",             (PyCFunction)PyVGX_Memory__Stack,               METH_NOARGS,                    Stack__doc__  },
    {"Reset",             (PyCFunction)PyVGX_Memory__Reset,               METH_VARARGS | METH_KEYWORDS,   Reset__doc__  },
    {"Sort",              (PyCFunction)PyVGX_Memory__Sort,                METH_VARARGS | METH_KEYWORDS,   Sort__doc__  },

    {"DualInt",           (PyCFunction)PyVGX_Memory__DualInt,             METH_VARARGS,                   DualInt__doc__ },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_Memory_as_sequence
 *
 ******************************************************************************
 */
static PySequenceMethods PyVGX_Memory_as_sequence = {
    .sq_length          = (lenfunc)PyVGX_Memory_len,
    .sq_concat          = (binaryfunc)0,
    .sq_repeat          = (ssizeargfunc)0,
    .sq_item            = (ssizeargfunc)PyVGX_Memory_item,
    .was_sq_slice       = 0,
    .sq_ass_item        = (ssizeobjargproc)PyVGX_Memory_ass_item,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)0,
    .sq_inplace_concat  = (binaryfunc)0,
    .sq_inplace_repeat  = (ssizeargfunc)0,
};



/******************************************************************************
 * PyVGX_Memory_as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_Memory_as_mapping = {
    .mp_length          = (lenfunc)PyVGX_Memory_len,
    .mp_subscript       = (binaryfunc)PyVGX_Memory_get_item,
    .mp_ass_subscript   = (objobjargproc)PyVGX_Memory_set_item
};



/******************************************************************************
 * PyVGX_Memory__MemoryType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Memory__MemoryType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Memory",
    .tp_basicsize       = sizeof(PyVGX_Memory),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Memory__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = (getattrfunc)0,
    .tp_setattr         = (setattrfunc)0,
    .tp_as_async        = NULL,
    .tp_repr            = (reprfunc)PyVGX_Memory__repr,
    .tp_as_number       = NULL,
    .tp_as_sequence     = &PyVGX_Memory_as_sequence,
    .tp_as_mapping      = &PyVGX_Memory_as_mapping,
    .tp_hash            = (hashfunc)0,
    .tp_call            = (ternaryfunc)0,
    .tp_str             = (reprfunc)0,
    .tp_getattro        = (getattrofunc)0,
    .tp_setattro        = (setattrofunc)0,
    .tp_as_buffer       = NULL,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Evalmem objects",
    .tp_traverse        = (traverseproc)0,
    .tp_clear           = (inquiry)0,
    .tp_richcompare     = (richcmpfunc)0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = (getiterfunc)0,
    .tp_iternext        = (iternextfunc)0,
    .tp_methods         = PyVGX_Memory__methods,
    .tp_members         = PyVGX_Memory__members,
    .tp_getset          = PyVGX_Memory__getset,
    .tp_base            = NULL,
    .tp_dict            = NULL,
    .tp_descr_get       = (descrgetfunc)0,
    .tp_descr_set       = (descrsetfunc)0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Memory__init,
    .tp_alloc           = (allocfunc)0,
    .tp_new             = PyVGX_Memory__new,
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
    .tp_vectorcall      = PyVGX_Memory__vectorcall
};


DLL_HIDDEN PyTypeObject * p_PyVGX_Memory__MemoryType = &PyVGX_Memory__MemoryType;
