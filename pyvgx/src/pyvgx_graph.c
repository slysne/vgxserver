/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_graph.c
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


// Stores *addresses* of PyVGX_Graph instances as PyLong objects
static PyObject *g_py_open_graphs = NULL;



static PyObject * __PyVGX_Graph__get_name( PyVGX_Graph *pygraph, void *closure );
static PyObject * __PyVGX_Graph__get_path( PyVGX_Graph *pygraph, void *closure );
static PyObject * __PyVGX_Graph__get_size( PyVGX_Graph *pygraph, void *closure );
static PyObject * __PyVGX_Graph__get_order( PyVGX_Graph *pygraph, void *closure );
static PyObject * __PyVGX_Graph__sim( PyVGX_Graph *pygraph, void *closure );


static PyObject * PyVGX_Graph__Close( PyVGX_Graph *pygraph );

static int sq_contains_PyVGX_Graph( PyVGX_Graph *pygraph, PyObject *py_vertex_name );


/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int PyVGX_Graph_Init( void ) {
  if( g_py_open_graphs == NULL ) {
    if( (g_py_open_graphs = PyDict_New()) == NULL ) {
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
DLL_HIDDEN int PyVGX_Graph_Unload( void ) {
  if( g_py_open_graphs != NULL ) {

    // Allocate array to store graph addresses while we close them
    int64_t sz_list = 0;
    PyObject *key, *val;
    Py_ssize_t pos = 0;
    while( PyDict_Next( g_py_open_graphs, &pos, &key, &val ) ) {
      sz_list += PyList_Size( val );
    }

    intptr_t *address_list = calloc( sz_list, sizeof(intptr_t) );
    if( address_list == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      return -1;
    }

    // Populate address list
    pos = 0;
    int idx = 0;
    while( PyDict_Next( g_py_open_graphs, &pos, &key, &val ) ) {
      int64_t n = PyList_Size( val );
      for( int64_t i=0; i<n; i++ ) {
        PyObject *py_long = PyList_GetItem( val, i );
        intptr_t addr = PyLong_AsLongLong( py_long );
        if( addr > 0 && idx < sz_list ) {
          address_list[idx] = addr;
        }
        ++idx;
      }
    }

    // Close graphs
    for( int i=0; i<sz_list; i++ ) {
      intptr_t addr = address_list[i];
      if( addr ) {
        PyVGX_Graph *pygraph = (PyVGX_Graph*)addr;
        PyObject *py_ret = PyVGX_Graph__Close( pygraph );
        if( py_ret ) {
          Py_DECREF( py_ret );
        }
        else {
          break;
        }
      }
    }
    free( address_list );

    if( PyErr_Occurred() ) {
      return -1;
    }

    // No open graphs
    if( PyDict_Size( g_py_open_graphs ) > 0 ) {
      PyErr_SetString( PyExc_Exception, "open graphs still exist (internal error)" );
      return -1;
    }

    // Delete the open graph register
    Py_DECREF( g_py_open_graphs );
    g_py_open_graphs = NULL;
  }
  return 0;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int __PyVGX_Graph__OGR_add( PyVGX_Graph *pygraph ) {
  if( g_py_open_graphs == NULL ) {
    PyErr_SetString( PyExc_Exception, "not initialized" );
    return -1;
  }

  intptr_t this_addr = (intptr_t)pygraph;
  PyObject *key = pygraph->py_name;

  // Graph instance not indexed, add it
  PyObject *py_list = PyDict_GetItem( g_py_open_graphs, key );
  if( py_list == NULL ) {
    // Create list of instance addresses for the same key
    if( (py_list = PyList_New( 1 )) == NULL ) {
      return -1;
    }
    PyObject *py_this_addr = PyLong_FromLongLong( this_addr );
    PyList_SET_ITEM( py_list, 0, py_this_addr );
    // Put the list into our dict
    if( PyDict_SetItem( g_py_open_graphs, key, py_list ) < 0 ) {
      Py_DECREF( py_list );
      return -1;
    }
    return 0;
  }

  // Existing instance(s) for this key
  int64_t sz = PyList_Size( py_list );
  for( int64_t i=0; i<sz; i++ ) {
    PyObject *py_existing_address = PyList_GET_ITEM( py_list, i );
    // Same instance already indexed, no action
    if( PyLong_AsLongLong( py_existing_address ) == this_addr ) {
      return 0;
    }
  }

  // This instance is not indexed, add it
  PyObject *py_this_addr = PyLong_FromLongLong( this_addr );
  int ret = PyList_Append( py_list, py_this_addr );
  if( ret == 0 ) {
    Py_DECREF( py_this_addr );
  }
  return ret;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static int __PyVGX_Graph__OGR_del( PyVGX_Graph *pygraph ) {
  if( g_py_open_graphs == NULL ) {
    PyErr_SetString( PyExc_Exception, "not initialized" );
    return -1;
  }

  intptr_t this_addr = (intptr_t)pygraph;
  PyObject *key = pygraph->py_name;

  // No graph instance with this name indexed, no action
  PyObject *py_list = PyDict_GetItem( g_py_open_graphs, key );
  if( py_list == NULL ) {
    return 0;
  }

  // Look for this instance in indexed list
  int64_t sz = PyList_Size( py_list );
  for( int64_t i=0; i<sz; i++ ) {
    PyObject *py_existing_address = PyList_GET_ITEM( py_list, i );
    // This instance address found, remove it
    if( PyLong_AsLongLong( py_existing_address ) == this_addr ) {
      if( PySequence_DelItem( py_list, i ) < 0 ) {
        return -1;
      }
      // Was this the last instance?
      if( PyList_Size( py_list ) == 0 ) {
        // Delete the now empty list from the instance dict
        return PyDict_DelItem( g_py_open_graphs, key );
      }
      return 0;
    }
  }

  // Instance was not indexed
  return 0;
}



/******************************************************************************
 * Return a pyvgx.Graph instance.
 * Returns a new reference.
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * PyVGX_Graph__get_open( PyObject *py_name ) {
  if( g_py_open_graphs == NULL ) {
    PyErr_SetString( PyExc_Exception, "not initialized" );
    return NULL;
  }

  // Key is valid
  if( PyUnicode_Check( py_name ) ) {
    // Graph exists
    PyObject *py_list = PyDict_GetItem( g_py_open_graphs, py_name );
    if( py_list ) {
      // Pick the first instance found
      PyObject *py_existing_address = PyList_GET_ITEM( py_list, 0 );
      intptr_t pygraph_addr = PyLong_AsLongLong( py_existing_address );
      PyObject *py_instance = (PyObject*)pygraph_addr;
      Py_INCREF( py_instance );
      return py_instance;
    }
    // No graph with this name
    else {
      PyErr_Format( PyExc_KeyError, "Graph instance not available: %U", py_name );
      return NULL;
    }
  }
  // Invalid key
  else {
    PyErr_SetString( PyExc_ValueError, "Graph name must be a string" );
    return NULL;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * __get_object_repr( comlib_object_t *obj ) {
  PyObject *py_repr = NULL;
  if( obj ) {
    CStringQueue_t *queue = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
    if( queue ) {
      COMLIB_OBJECT_REPR( obj, queue );
      char *value = NULL;
      int64_t sz = CALLABLE( queue )->GetValueNolock( queue, (void**)&value );
      if( value ) {
        py_repr = PyUnicode_FromStringAndSize( value, sz );
        ALIGNED_FREE( value );
      }
      COMLIB_OBJECT_DESTROY( queue );
    }
  }
  if( py_repr == NULL ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "invalid object" );
    }
  }
  return py_repr;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_Vector_t * __own_vertex_vector( vgx_Graph_t *graph, const CString_t *CSTR__vertex_id, int timeout_ms, CString_t **CSTR__error ) {
  vgx_Vector_t *vector = NULL;

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *vertex_RO = NULL;
    XTRY {
      vgx_AccessReason_t reason;
      // Use the anchor's vector by default
      vertex_RO = CALLABLE(graph)->simple->OpenVertex( graph, CSTR__vertex_id, VGX_VERTEX_ACCESS_READONLY, timeout_ms, &reason, CSTR__error );
      if( vertex_RO == NULL ) {
        if( __py_set_vertex_error_check_is_noexist( CStringValue(CSTR__vertex_id), reason ) ) {
          THROW_SILENT( CXLIB_ERR_LOOKUP, 0x211 );
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x212 );
        }
      }
      if( (vector = vertex_RO->vector) != NULL ) {
        CALLABLE(vector)->Incref(vector);
      }
    }
    XCATCH( errcode ) {
      if( vector ) {
        CALLABLE(vector)->Decref(vector);
      }
      vector = NULL;
    }
    XFINALLY {
      if( vertex_RO ) {
        CALLABLE(graph)->simple->CloseVertex( graph, &vertex_RO );
      }
    }
  } END_PYVGX_THREADS;

  return vector;
}



/******************************************************************************
 * PyVGX_Graph__Define
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Define__doc__,
  "Define( '<name> := <expression>' ) -> None\n"
  "\n"
  "Create a new function formula that can be used by queries for\n"
  "filtering and ranking.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Define
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Define( PyVGX_Graph *pygraph, PyObject *py_expression ) {
  PyObject *py_ret = NULL;
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  const char *expression = PyVGX_PyObject_AsString( py_expression );
  if( expression == NULL ) {
    PyVGXError_SetString( PyExc_ValueError, "Expression must be a string" );
    return NULL;
  }

  CString_t *CSTR__error = NULL;

  XTRY {
    int ret = -1;
    BEGIN_PYVGX_THREADS {
      vgx_Evaluator_t *evaluator = CALLABLE( graph )->simple->DefineEvaluator( graph, expression, NULL, &CSTR__error );
      if( evaluator ) {
        ret = 0;
        COMLIB_OBJECT_DESTROY( evaluator );
      }
    } END_PYVGX_THREADS;

    if( ret < 0 ) {
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    py_ret = Py_None;
    Py_INCREF( py_ret );
  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
      PyErr_SetString( PyExc_ValueError, CStringValue( CSTR__error ) );
    }
    else {
      PyErr_SetString( PyExc_ValueError, "internal error" );
    }
  }
  XFINALLY {
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__Evaluate
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Evaluate__doc__,
  "Evaluate( expression, tail, arc=None, head=None, vector=None, memory=None ) -> value\n"
  "\n"
  "Evaluate the expression.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Evaluate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Evaluate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"expression", "tail", "arc", "head", "vector", "memory", NULL};
  const char *expression = NULL;
  Py_ssize_t sz_expression = 0;
  PyObject *py_tail = NULL;
  PyObject *py_arc = NULL;
  PyObject *py_head = NULL;
  PyObject *py_vector = NULL;
  PyObject *py_evalmem = NULL;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|z#OOOOO", kwlist, &expression, &sz_expression, &py_tail, &py_arc, &py_head, &py_vector, &py_evalmem ) ) {
    return NULL;
  }

  if( expression == NULL ) {
    expression = "null";
  }

  pyvgx_VertexIdentifier_t tail = {0};
  pyvgx_VertexIdentifier_t head = {0};
  if( py_tail ) {
    if( iPyVGXParser.GetVertexID( pygraph, py_tail, &tail, NULL, true, "Tail ID" ) < 0 ) {
      return NULL;
    }
  }
  if( py_head ) {
    if( iPyVGXParser.GetVertexID( pygraph, py_head, &head, NULL, true, "Head ID" ) < 0 ) {
      return NULL;
    }
  }

  vgx_IGraphSimple_t *isimple = CALLABLE( graph )->simple;

  vgx_Evaluator_t *evaluator = NULL;
  vgx_StackItemType_t value_type = STACK_ITEM_TYPE_NONE;
  vgx_EvalStackItem_t *result = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;
  const char *err_vertex = NULL;
  bool rel_err = false;

  vgx_Vector_t *vector = NULL;
  if( py_vector ) {
    if( (vector = iPyVGXParser.InternalVectorFromPyObject( graph->similarity, py_vector, NULL, true )) == NULL ) {
      return NULL;
    }
  }

  vgx_GlobalQuery_t *query = iGraphQuery.NewDefaultGlobalQuery( graph, NULL );
  if( query && py_evalmem ) {
    vgx_ExpressEvalMemory_t *evalmem = iPyVGXParser.NewExpressEvalMemory( graph, py_evalmem );
    if( evalmem ) {
      // Evaluator Memory (query owns +1 ref)
      CALLABLE( query )->SetMemory( query, evalmem );
      // Discard, now only the query is owner
      iEvaluator.DiscardMemory( &evalmem );
    }
    else {
      iGraphQuery.DeleteGlobalQuery( &query );
    }
  }

  if( query == NULL ) {
    if( vector ) {
      CALLABLE( vector )->Decref( vector );
    }
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    // Retrieve function
    vgx_Evaluator_t *E = NULL;
    if( (E = isimple->GetEvaluator( graph, expression )) != NULL || (E = iEvaluator.NewEvaluator( graph, expression, vector, &CSTR__error)) != NULL ) {
      if( head.id == NULL && CALLABLE( E )->Traversals( E ) > 0 ) {
        CSTR__error = CStringNew( "formula requires head vertex" );
      }
      else {
        // Clone evaluator
        evaluator = CALLABLE( E )->Clone( E, vector );
        COMLIB_OBJECT_DESTROY( E );
        // Create the test relation
        vgx_LockableArc_t larc_RO = {0};
        vgx_Relation_t *relation = NULL;
        if( (relation = iPyVGXParser.NewRelation( graph, tail.id, py_arc, head.id )) != NULL ) {
          // Set the predicator
          if( relation->relationship.CSTR__name ) {
            larc_RO.head.predicator.rel.enc = (uint16_t)iEnumerator_OPEN.Relationship.GetEnum( graph, relation->relationship.CSTR__name );
            larc_RO.head.predicator.val = relation->relationship.value;
            larc_RO.head.predicator.mod.bits = relation->relationship.mod_enum;
            larc_RO.head.predicator.rel.dir = VGX_ARCDIR_OUT;
          }
          else {
            larc_RO.head.predicator = VGX_PREDICATOR_NONE;
          }

          if( __relationship_enumeration_valid( larc_RO.head.predicator.rel.enc ) ) {
            // Open tail (if given)
            err_vertex = tail.id;
            if( tail.id == NULL || (larc_RO.tail = isimple->OpenVertex( graph, relation->initial.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, &reason, &CSTR__error )) != NULL ) {
              // Open head (if given)
              err_vertex = head.id;
              if( head.id == NULL || (larc_RO.head.vertex = isimple->OpenVertex( graph, relation->terminal.CSTR__name, VGX_VERTEX_ACCESS_READONLY, 0, &reason, &CSTR__error )) != NULL ) {
                larc_RO.acquired.tail_lock = larc_RO.tail ? 1 : 0;
                larc_RO.acquired.head_lock = larc_RO.head.vertex ? 1 : 0;
                err_vertex = NULL;
                if( query->evaluator_memory ) {
                  CALLABLE( evaluator )->OwnMemory( evaluator, query->evaluator_memory );
                }
                // Run evaluator
                if( larc_RO.head.vertex ) {
                  if( larc_RO.tail == NULL ) {
                    larc_RO.tail = CALLABLE( graph )->advanced->AcquireVertexObjectReadonly( graph, larc_RO.head.vertex, 0, &reason );
                  }
                  _vgx_arc_set_distance( (vgx_Arc_t*)&larc_RO, 1 );
                  CALLABLE( evaluator )->SetContext( evaluator, larc_RO.tail, &larc_RO.head, NULL, 0.0 );
                  result = CALLABLE( evaluator )->EvalArc( evaluator, &larc_RO );
                }
                else if( larc_RO.tail ) {
                  result = CALLABLE( evaluator )->EvalVertex( evaluator, larc_RO.tail );
                }
                else {
                  result = CALLABLE( evaluator )->Eval( evaluator );
                }
                // Get value type of result
                value_type = CALLABLE( evaluator )->ValueType( evaluator ); 
                // Close head vertex (if we have one)
                if( larc_RO.head.vertex ) {
                  isimple->CloseVertex( graph, &larc_RO.head.vertex );
                }
                larc_RO.acquired.head_lock = 0;
              }
              // Close tail vertex (if we have one)
              if( larc_RO.tail ) {
                isimple->CloseVertex( graph, &larc_RO.tail );
              }
              larc_RO.acquired.tail_lock = 0;
            }
          }
          else if( CSTR__error == NULL ) {
            CSTR__error = CStringNewFormat( "invalid relationship: '%s'", relation->relationship.CSTR__name ? CStringValue( relation->relationship.CSTR__name ) : "?" );
          }

          // Delete relation
          iRelation.Delete( &relation );
        }
        else {
          rel_err = true;
        }
      }
    }
    else if( CSTR__error == NULL ) {
      CSTR__error = CStringNewFormat( "invalid or undefined function: '%s'", expression );
    }
  } END_PYVGX_THREADS;

  // Relationship error
  if( rel_err ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "unknown relationship error" );
    }
  }
  // Vertex error
  else if( err_vertex ) {
    iPyVGXBuilder.SetPyErrorFromAccessReason( err_vertex, reason, &CSTR__error );
  }
  // Other error
  else if( CSTR__error ) {
    PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
  }
  // Produce output
  else if( evaluator && result ) {
    const CString_t *CSTR__str;
    switch( value_type ) {
    case STACK_ITEM_TYPE_NONE:
      Py_INCREF( Py_None );
      py_ret = Py_None;
      break;
    case STACK_ITEM_TYPE_INTEGER:
      py_ret = PyLong_FromLongLong( iEvaluator.GetInteger( result ) );
      break;
    case STACK_ITEM_TYPE_REAL:
      {
        double d = result->real;
        if( !isnan( d ) ) {
          py_ret = PyFloat_FromDouble( d );
          break;
        }
      }
    case STACK_ITEM_TYPE_NAN:
      py_ret = PyFloat_FromDouble( Py_NAN );
      break;
    case STACK_ITEM_TYPE_VERTEX:
      py_ret = PyUnicode_FromString( CALLABLE( result->vertex )->IDString( result->vertex ) );
      break;
    case STACK_ITEM_TYPE_RANGE:
      py_ret = PyUnicode_FromString( "<range>" );
      break;
    case STACK_ITEM_TYPE_CSTRING:
      py_ret = iPyVGXCodec.NewPyObjectFromEncodedObject( result->CSTR__str, NULL );
      break;
    case STACK_ITEM_TYPE_VECTOR:
      py_ret = PyUnicode_FromString( "<vector>" );
      break;
    case STACK_ITEM_TYPE_BITVECTOR:
      if( (CSTR__str = CStringNewFormat( "<bitvector 0x%016llx>", result->bits )) != NULL ) {
        py_ret = PyUnicode_FromStringAndSize( CStringValue( CSTR__str ), CStringLength( CSTR__str ) );
        CStringDelete( CSTR__str );
      }
      break;
    case STACK_ITEM_TYPE_KEYVAL:
      if( (CSTR__str = CStringNewFormat( "<keyval (%d,%g)>", vgx_cstring_array_map_key( &result->bits ), vgx_cstring_array_map_val( &result->bits ) )) != NULL ) {
        py_ret = PyUnicode_FromStringAndSize( CStringValue( CSTR__str ), CStringLength( CSTR__str ) );
        CStringDelete( CSTR__str );
      }
      break;
    case STACK_ITEM_TYPE_VERTEXID:
      py_ret = PyUnicode_FromString( result->vertexid->CSTR__idstr ? CStringValue( result->vertexid->CSTR__idstr ) : result->vertexid->idprefix.data );
      break;
    case STACK_ITEM_TYPE_SET:
      py_ret = PyUnicode_FromString( "<set>" );
      break;
    default:
      PyErr_SetString( PyExc_Exception, "unknown evaluator error" );
      break;
    }
  }
  else {
    PyErr_SetString( PyExc_Exception, "unknown internal error" );
  }

  if( py_ret == NULL && !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "unknown internal error" );
  }

  if( evaluator ) {
    BEGIN_PYVGX_THREADS {
      COMLIB_OBJECT_DESTROY( evaluator );
    } END_PYVGX_THREADS;
  }

  iGraphQuery.DeleteGlobalQuery( &query );

  if( vector ) {
    CALLABLE( vector )->Decref( vector );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__Memory
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Memory__doc__,
  "Memory( initializer ) -> Memory object\n"
  "\n"
  "Return a new Memory object for use with expression evaluators\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Memory
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Memory( PyVGX_Graph *pygraph, PyObject *py_order ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *args[] = {
    NULL,
    (PyObject*)pygraph,
    py_order
  };

  return PyObject_Vectorcall( (PyObject*)p_PyVGX_Memory__MemoryType, args+1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );
}



/******************************************************************************
 * PyVGX_Graph__GetDefinition
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetDefinition__doc__,
  "GetDefinition( name ) -> string\n"
  "\n"
  "Return the expression identified by name\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetDefinition
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetDefinition( PyVGX_Graph *pygraph, PyObject *py_name ) {
  PyObject *py_ret = NULL;
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  const char *name = PyVGX_PyObject_AsUTF8( py_name, NULL );
  if( name == NULL ) {
    if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_ValueError, "Name must a string or bytes-like object" );
    }
    return NULL;
  }

  vgx_Evaluator_t *e = NULL;
  BEGIN_PYVGX_THREADS {
    e = CALLABLE( graph )->simple->GetEvaluator( graph, name );
  } END_PYVGX_THREADS;

  if( e ) {
    if( e->rpn_program.CSTR__expression ) {
      const char *expression = CStringValue( e->rpn_program.CSTR__expression );
      int64_t len = CStringLength( e->rpn_program.CSTR__expression );
      py_ret = PyUnicode_FromStringAndSize( expression, len );
    }
    BEGIN_PYVGX_THREADS {
      COMLIB_OBJECT_DESTROY( e );
    } END_PYVGX_THREADS;
  }
  else {
    PyErr_SetString( PyExc_KeyError, name );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__GetDefinitions
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetDefinitions__doc__,
  "GetDefinitions() -> list of strings\n"
  "\n"
  "Return all defined expressions\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetDefinitions
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetDefinitions( PyVGX_Graph *pygraph  ) {
  PyObject *py_ret = NULL;
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Return functions in a dict
  if( (py_ret = PyDict_New()) == NULL ) {
    return NULL;
  }

  // Get all evaluator objects
  int64_t sz = 0;
  vgx_Evaluator_t **evaluators = NULL;
  BEGIN_PYVGX_THREADS {
    evaluators = CALLABLE( graph )->simple->GetEvaluators( graph, &sz );
  } END_PYVGX_THREADS;
  if( evaluators == NULL ) {
    PyVGX_DECREF( py_ret );
    return NULL;
  }

  // Build return dict { name : expression }
  vgx_Evaluator_t **cursor = evaluators;
  vgx_Evaluator_t *e;
  while( (e = *cursor++) != NULL ) {
    if( e->rpn_program.CSTR__expression && e->rpn_program.CSTR__assigned ) {
      const char *name = CStringValue( e->rpn_program.CSTR__assigned );
      const char *expression = CStringValue( e->rpn_program.CSTR__expression );
      int64_t sz_expression = CStringLength( e->rpn_program.CSTR__expression );
      PyObject *py_expr = PyUnicode_FromStringAndSize( expression, sz_expression );
      iPyVGXBuilder.DictMapStringToPyObject( py_ret, name, &py_expr );
    }
    // TODO: Don't do it this way in and out of thread state
    BEGIN_PYVGX_THREADS {
      COMLIB_OBJECT_DESTROY( e );
    } END_PYVGX_THREADS;
  }

  // Free
  free( evaluators );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__NewVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( NewVertex__doc__,
  "NewVertex( id, type=None, lifespan=-1, properties={}, timeout=0 ) -> vertex\n"
  "\n"
  "This creates a new vertex in the graph if it does not already exist, then acquires\n"
  "write access and returns the vertex object.\n"
  "\n"
  "id         :  Unique ID string for the vertex to be created, or opened\n"
  "              (in write mode) if it does not already exist\n"
  "type       :  Type name of the vertex to be created, or None to create a typeless vertex\n"
  "lifespan   :  Number of seconds until vertex is automatically deleted\n"
  "properties :  Dict of properties to set on vertex \n"
  "timeout    :  Timeout in milliseconds for aquiring writable access to the vertex\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__NewVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewVertex( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"id", "type", "lifespan", "properties", "timeout", NULL};
  PyObject *py_vertex_id = NULL;
  PyObject *py_vertex_type = NULL;
  PyObject *py_lifespan = NULL;
  PyObject *py_properties = NULL;
  PyObject *py_timeout_ms = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|OOOO", kwlist, &py_vertex_id, &py_vertex_type, &py_lifespan, &py_properties, &py_timeout_ms ) ) {
    return NULL;
  }

  PyObject *vcargs[] = {
    NULL,
    (PyObject*)pygraph, // graph
    py_vertex_id,       // id
    py_vertex_type,     // type
    g_py_char_w,        // mode
    py_lifespan,        // lifespan
    py_timeout_ms       // timeout
  };

  PyObject *py_vertex = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vertex__VertexType, vcargs+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  // Set properties if provided
  if( py_properties && py_vertex ) {
    static PyObject *py_SetProperties = NULL;
    if( py_SetProperties == NULL ) {
      py_SetProperties = PyUnicode_FromString( "SetProperties" );
    }
    if( py_SetProperties ) {
      PyObject *py_ret = PyObject_CallMethodObjArgs( py_vertex, py_SetProperties, py_properties, NULL );
      if( py_ret == NULL ) {
        Py_DECREF( py_vertex );
        py_vertex = NULL;
      }
      else {
        Py_DECREF( py_ret );
      }
    }
  }

  return py_vertex;
}



/******************************************************************************
 * PyVGX_Graph__CreateVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CreateVertex__doc__,
  "CreateVertex( id, type=None, lifespan=-1, properties={} ) -> 1 if vertex was created, 0 if vertex already exists.\n"
  "\n"
  "Creates a new vertex in the graph.\n"
  "\n"
  "id         :  Unique ID string for the vertex to be created\n"
  "type       :  Type name of the vertex to be created or None\n"
  "lifespan   :  Number of seconds until vertex is automatically deleted\n"
  "properties :  Dict of properties to set on vertex \n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__CreateVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__CreateVertex( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_ret = NULL;

  // Args
  static char *kwlist[] = {"id", "type", "lifespan", "properties", NULL};
  PyObject *py_id = NULL;
  const char *vertex_type = NULL;
  int64_t sz_vertex_type = 0;
  int lifespan = -1;
  PyObject *py_properties = NULL;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "O|z#iO", kwlist, &py_id, &vertex_type, &sz_vertex_type, &lifespan, &py_properties ) ) {
    return NULL;
  }

  if( !PyVGX_PyObject_CheckString( py_id ) ) {
    PyErr_SetString( PyExc_TypeError, "a string or bytes-like object is required" );
    return NULL;
  }

  int n_created = 0;

  CString_t *CSTR__error = NULL;
  
  const char *vertex_id = NULL;
  Py_ssize_t id_len = 0;
  Py_ssize_t id_ucsz = 0;
  if( (vertex_id = PyVGX_PyObject_AsUTF8AndSize( py_id, &id_len, &id_ucsz, NULL )) == NULL ) {
    return NULL;
  }
  else if( id_len > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
    PyErr_Format( PyVGX_VertexError, "Vertex identifier too long (%lld), max length is %d", id_len, _VXOBALLOC_CSTRING_MAX_LENGTH );
    return NULL;
  }

  vgx_Vertex_t *vertex = NULL;

  BEGIN_PYVGX_THREADS {

    CString_t *CSTR__vertex_id = NULL;
    CString_t *CSTR__vertex_type = NULL;

    XTRY {
      vgx_AccessReason_t reason;
      
      if( (CSTR__vertex_id = NewEphemeralCStringLen( graph, vertex_id, (int)id_len, (int)id_ucsz )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x231 );
      }

      if( vertex_type && (CSTR__vertex_type = NewEphemeralCString( graph, vertex_type )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x232 );
      }

      const objectid_t *vertex_obid = CStringObid( CSTR__vertex_id );
      GRAPH_LOCK( graph ) {
        // Simple
        if( lifespan < 0 && py_properties == NULL ) {
          n_created = CALLABLE(graph)->advanced->CreateVertex_CS( graph, vertex_obid, CSTR__vertex_id, CSTR__vertex_type, 0, &reason, &CSTR__error );
        }
        // More
        else {
          if( (n_created = CALLABLE(graph)->advanced->CreateReturnVertex_CS( graph, vertex_obid, CSTR__vertex_id, CSTR__vertex_type, &vertex, 1000, &reason, &CSTR__error )) >= 0 && vertex != NULL ) {
            if( lifespan > -1 ) {
              uint32_t now = _vgx_graph_seconds( graph );
              uint32_t tmx = now + lifespan;
              if( CALLABLE( vertex )->SetExpirationTime( vertex, tmx ) < 0 ) {
                reason = VGX_ACCESS_REASON_ERROR;
                CSTR__error = CStringNewFormat( "failed to set expiration time: %u", tmx );
                n_created = -1;
              }
            }
            if( py_properties ) {
              GRAPH_SUSPEND_LOCK( graph ) {
                if( pyvgx_SetVertexProperties( vertex, py_properties ) < 0 ) {
                  n_created = -1;
                }
              } GRAPH_RESUME_LOCK;
            }
            CALLABLE( graph )->advanced->ReleaseVertex_CS( graph, &vertex );
          }
        }
      } GRAPH_RELEASE;

      if( n_created < 0 ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( vertex_id, reason, &CSTR__error );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x233 );
      }    
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_ret );
      } END_PYTHON_INTERPRETER;
      n_created = -1;
    }
    XFINALLY {
      iString.Discard( &CSTR__vertex_id );
      iString.Discard( &CSTR__vertex_type );
      iString.Discard( &CSTR__error );
    }
  } END_PYVGX_THREADS;


  if( n_created >= 0 ) {
    py_ret = PyLong_FromLong( n_created );
  }

  return py_ret;

}



/******************************************************************************
 * PyVGX_Graph__DeleteVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DeleteVertex__doc__,
  "DeleteVertex( id, timeout=0 ) -> 1 if vertex was deleted, 0 if vertex does not exist.\n"
  "\n"
  "Delete a vertex from the graph.\n"
  "\n"
  "id      :  Unique string identifier for vertex\n"
  "timeout :  Timeout in milliseconds for aquiring writable access to the vertex\n"
  "\n"
  ""
);

/**************************************************************************//**
 * PyVGX_Graph__DeleteVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DeleteVertex( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"id", "timeout", NULL};
  const char *vertex_id = NULL;
  int64_t sz_vertex_id = 0;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#|i", kwlist, &vertex_id, &sz_vertex_id, &timeout_ms) ) {
    return NULL;
  }

  PyObject *py_ret = NULL;
  int n_deleted = 0;

  CString_t *CSTR__error = NULL;

  BEGIN_PYVGX_THREADS {

    CString_t *CSTR__vertex_id = NULL;

    XTRY {
      vgx_AccessReason_t reason;

      if( (CSTR__vertex_id = NewEphemeralCString( graph, vertex_id )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x241 );
      }

      n_deleted = CALLABLE(graph)->simple->DeleteVertex( graph, CSTR__vertex_id, timeout_ms, &reason, &CSTR__error );

      if( n_deleted < 0 ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( vertex_id, reason, &CSTR__error );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x242 );
      }
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_ret );
      } END_PYTHON_INTERPRETER;
      n_deleted = -1;
    }
    XFINALLY {
      iString.Discard( &CSTR__vertex_id );
      iString.Discard( &CSTR__error );
    }
  } END_PYVGX_THREADS;

  if( n_deleted >= 0 ) {
    py_ret = PyLong_FromLong( n_deleted );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__HasVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( HasVertex__doc__,
  "HasVertex( id ) -> True if vertex exists, False if vertex does not exist.\n"
  "\n"
  "Check if the named vertex exists in the graph.\n"
  "\n"
  "id      :  Unique string identifier for vertex\n"
);

/**************************************************************************//**
 * PyVGX_Graph__HasVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__HasVertex( PyVGX_Graph *pygraph, PyObject *py_id ) {
  int exists = sq_contains_PyVGX_Graph( pygraph, py_id );
  if( exists >= 0 ) {
    PyObject *py_ret = exists ? Py_True : Py_False;
    Py_INCREF( py_ret );
    return py_ret;
  }

  return NULL;

}



/******************************************************************************
 * PyVGX_Graph__OpenVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( OpenVertex__doc__,
  "OpenVertex( id, mode='a', timeout=0 ) -> Vertex object\n"
  "\n"
  "Return a vertex object with the requested access mode.\n"
  "\n"
  "id      : Unique ID string (or memory address) of the vertex to be opened\n"
  "mode    : Access mode is one of\n"
  "          'r'=readonly\n"
  "          'w'=writable (and create typeless)\n"
  "          'a'=writable (do not create)\n"
  "timeout : Timeout in milliseconds for acquiring the requested access to the vertex\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__OpenVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__OpenVertex( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"id", "mode", "timeout", NULL};
  PyObject *py_id;
  PyObject *py_mode = NULL;
  PyObject *py_timeout = NULL;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &py_id, &py_mode, &py_timeout ) ) {
    return NULL;
  }

  PyObject *vcargs[] = {
    NULL,
    (PyObject*)pygraph, // graph
    py_id,              // id
    NULL,               // type
    py_mode,            // mode
    g_py_minus_one,     // lifespan
    py_timeout          // timeout
  };

  PyObject *py_vertex = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vertex__VertexType, vcargs+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  return py_vertex;
}



/******************************************************************************
 * PyVGX_Graph__CloseVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CloseVertex__doc__,
  "CloseVertex( vertex_object ) -> bool\n"
  "\n"
  "Release access lock and commit any vertex changes.\n"
  "\n"
  "vertex_object:    Vertex object to close\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__CloseVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__CloseVertex( PyVGX_Graph *pygraph, PyVGX_Vertex *pyvertex ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Ignore if vertex object was None
  if( (PyObject*)pyvertex == Py_None ) {
    Py_RETURN_NONE;
  }

  bool released = false;

  if( !PyVGX_Vertex_CheckExact( pyvertex ) ) {
    PyVGXError_SetString( PyExc_ValueError, "Argument must be a vertex object" );
    return NULL;
  }

  // Close the vertex if we haven't already done so
  if( pyvertex->vertex != NULL ) {
    vgx_Vertex_t *vertex = pyvertex->vertex; // use a copy since CloseVertex sets the pointer to NULL
    if( (graph = __get_vertex_graph( pygraph, pyvertex )) == NULL ) {
      return NULL;
    }
    BEGIN_PYVGX_THREADS {
      released = CALLABLE( graph )->simple->CloseVertex( graph, &vertex );
    } END_PYVGX_THREADS;
  }

  // Replace the vertex with the null vertex
  if( released == true ) {
    pyvertex->vertex = NULL;
  }

  return PyBool_FromLong( released );
}



/******************************************************************************
 * PyVGX_Graph__CloseAll
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CloseAll__doc__,
  "CloseAll() -> count\n"
  "\n"
  "Close all vertices opened by current thread.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__CloseAll
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__CloseAll( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int64_t n;

  // New unique generation guard for this thread
  _next_pyvertex_generation_guard();

  BEGIN_PYVGX_THREADS {
    n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
  } END_PYVGX_THREADS;
 
  return PyLong_FromLongLong( n );
}



/******************************************************************************
 * PyVGX_Graph__CommitAll
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CommitAll__doc__,
  "CommitAll() -> count\n"
  "\n"
  "Commit all vertices opened writable by current thread.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__CommitAll
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__CommitAll( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int64_t n;

  BEGIN_PYVGX_THREADS {
    n = CALLABLE( graph )->advanced->CommitWritableVertices( graph );
  } END_PYVGX_THREADS;
 
  return PyLong_FromLongLong( n );
}



/******************************************************************************
 * PyVGX_Graph__EscalateVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EscalateVertex__doc__,
  "EscalateVertex( readonly_vertex_object, timeout=0 ) -> None\n"
  "\n"
  "Promote vertex access from readonly to writable.\n"
  "\n"
  "readonly_vertex_object  : Vertex instance whose access is readonly and\n"
  "                          should be promoted to writable\n"
  "timeout                 : Acquisition timeout (in milliseconds)\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EscalateVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EscalateVertex( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"readonly_vertex_object", "timeout", NULL};
  PyObject *py_vertex_RO_1;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|i", kwlist, &py_vertex_RO_1, &timeout_ms ) ) {
    return NULL;
  }

  if( !PyVGX_Vertex_CheckExact( py_vertex_RO_1 ) ) {
    PyErr_Format( PyExc_ValueError, "Vertex instance expected, got %s", py_vertex_RO_1->ob_type->tp_name );
    return NULL;
  }

  if( timeout_ms < 0 ) {
    PyVGXError_SetString( PyExc_ValueError, "Infinite timeout not allowed for this operation" );
    return NULL;
  }
  
  vgx_Vertex_t *V_RO_1 = ((PyVGX_Vertex*)py_vertex_RO_1)->vertex;
  if( (graph = __get_vertex_graph( pygraph, (PyVGX_Vertex*)py_vertex_RO_1 )) == NULL ) {
    return NULL;
  }
  CString_t *CSTR__error = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  vgx_Vertex_t *vertex_WL;
  BEGIN_PYVGX_THREADS {
    vertex_WL = CALLABLE( graph )->advanced->EscalateReadonlyToWritable( graph, V_RO_1, timeout_ms, &reason, &CSTR__error );
  } END_PYVGX_THREADS;

  if( vertex_WL == NULL ) { 
    iPyVGXBuilder.SetPyErrorFromAccessReason( CALLABLE( V_RO_1 )->IDString( V_RO_1 ), reason, &CSTR__error );
    return NULL;
  }

  // Success. Vertex is now writable.
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__RelaxVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( RelaxVertex__doc__,
  "RelaxVertex( writable_vertex_object ) -> None\n"
  "\n"
  "Relax a writable vertex to readonly access.\n"
  "\n"
  "writable_vertex_object  : Vertex instance whose access is writable and\n"
  "                          should be relaxed to readonly\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__RelaxVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__RelaxVertex( PyVGX_Graph *pygraph, PyVGX_Vertex *pyvertex ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( !PyVGX_Vertex_CheckExact( pyvertex ) ) {
    PyVGXError_SetString( PyExc_ValueError, "Argument must be a vertex object" );
    return NULL;
  }
  
  vgx_Vertex_t *vertex = pyvertex->vertex;
  if( (graph = __get_vertex_graph( pygraph, pyvertex )) == NULL ) {
    return NULL;
  }

  int relaxed;
  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *vertex_RO;
    if( (vertex_RO = CALLABLE( graph )->advanced->RelaxWritableToReadonly( graph, vertex )) == NULL ) {
      relaxed = -1;
    }
    else {
      relaxed = __vertex_is_readonly( vertex_RO );
    }
  } END_PYVGX_THREADS;

  if( relaxed < 0 ) {
    PyVGXError_SetString( PyExc_Exception, "Vertex could not be released. Check error logs." );
    return NULL;
  }
  else if( relaxed > 0 ) {
    Py_INCREF( Py_True );
    return Py_True;
  }
  else {
    Py_INCREF( Py_False );
    return Py_False;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_VertexList_t * __open_vertices( PyVGX_Graph *pygraph, PyObject *py_single, PyObject *py_list, vgx_VertexAccessMode_t access_mode, int timeout_ms ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  vgx_VertexList_t *vertices = NULL;

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;

  pyvgx_VertexIdentifier_t *ident_list = NULL, *s_cur, *s_end;

  int64_t sz_list = PyList_GET_SIZE( py_list );

  int64_t n_vertices = sz_list;
  if( py_single ) {
    ++n_vertices;
  }

  // List of raw string data and size
  if( (ident_list = calloc( n_vertices, sizeof( pyvgx_VertexIdentifier_t ) )) == NULL ) {
    PyErr_SetString( PyExc_MemoryError, "out of memory" );
    return NULL;
  }
  s_end = ident_list + n_vertices;

  XTRY {

    // Get all vertex identifiers
    s_cur = ident_list;

    int i = 0;
    PyObject *py_vertex;
    while( s_cur < s_end ) {
      if( py_single ) {
        py_vertex = py_single;
        py_single = NULL;
      }
      else {
        py_vertex = PyList_GET_ITEM( py_list, i );
        ++i;
      }

      if( iPyVGXParser.GetVertexID( pygraph, py_vertex, s_cur++, NULL, true, NULL ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x001 );
      }

    }

  }
  XCATCH( errcode ) {
    free( ident_list );
    ident_list = NULL;
  }
  XFINALLY {
  }

  if( ident_list == NULL ) {
    return NULL;
  }

  //
  // Perform atomic acquisition of all vertices
  //
  int memerr = 0;
  BEGIN_PYVGX_THREADS {
    // Allocate list of cstrings
    vgx_VertexIdentifiers_t *identifiers = iVertex.Identifiers.New( graph, n_vertices );
    if( identifiers ) {
      s_cur = ident_list;

      int i = 0;
      while( s_cur < s_end ) {
        // Add next cstring instance to list
        if( iVertex.Identifiers.SetIdLen( identifiers, i++, s_cur->id, s_cur->len ) == NULL ) {
          iVertex.Identifiers.Delete( &identifiers );
          break;
        }
        ++s_cur;
      }
    }

    if( identifiers ) {
      int partial_timeout_ms = 250;
      int remain_timeout_ms = timeout_ms;
      int retry;
      do {
        retry = 0;

        if( partial_timeout_ms > remain_timeout_ms ) {
          partial_timeout_ms = remain_timeout_ms;
        }

        // Acquire
        switch( access_mode ) {
        // Writable
        case VGX_VERTEX_ACCESS_WRITABLE_NOCREATE:
          vertices = CALLABLE( graph )->advanced->AtomicAcquireVerticesWritable( graph, identifiers, partial_timeout_ms, &reason, &CSTR__error );
          break;
        // Readonly
        case VGX_VERTEX_ACCESS_READONLY:
          vertices = CALLABLE( graph )->advanced->AtomicAcquireVerticesReadonly( graph, identifiers, partial_timeout_ms, &reason, &CSTR__error );
          break;
        // ???
        default:
          break;
        }

        if( vertices == NULL && remain_timeout_ms > 0 && __is_access_reason_transient( reason ) ) {
          retry = 1;
          if( reason == VGX_ACCESS_REASON_OPFAIL ) {
            sleep_milliseconds( 20 );
            remain_timeout_ms -= 20;
          }
          else {
            remain_timeout_ms -= partial_timeout_ms;
          }
        }

      } while( retry );

      iVertex.Identifiers.Delete( &identifiers );
    }
    else {
      memerr = -1;
    }
    free( ident_list );
    ident_list = NULL;

  } END_PYVGX_THREADS;

  // Error?
  if( vertices == NULL ) {
    if( memerr < 0 ) {
      PyErr_SetString( PyExc_MemoryError, "out of memory" );
    }
    else {
      iPyVGXBuilder.SetPyErrorFromAccessReason( NULL, reason, &CSTR__error );
    }
  }
 
  iString.Discard( &CSTR__error );

  return vertices;
}



/******************************************************************************
 * PyVGX_Graph__OpenVertices
 *
 ******************************************************************************
 */
PyDoc_STRVAR( OpenVertices__doc__,
  "OpenVertices( idlist, mode='a', timeout=0 ) -> List of acquired vertices\n"
  "\n"
  "Return a list of vertex objects with the requested access mode.\n"
  "\n"
  "idlist  : List of unique ID strings or vertex instances of the vertices to be acquired\n"
  "mode    : Access mode is one of\n"
  "          'r'=readonly\n"
  "          'a'=writable\n"
  "timeout : Timeout in milliseconds for acquiring the requested access to all vertices\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__OpenVertices
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__OpenVertices( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  //
  PyObject *py_vertices = NULL;

  // Args
  static char *kwlist[] = {"idlist", "mode", "timeout", NULL};
  PyObject *py_idlist;
  PyObject *py_mode = NULL;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "O|Oi", kwlist, &py_idlist, &py_mode, &timeout_ms ) ) {
    return NULL;
  }

  // ID list must be list
  if( !PyList_CheckExact( py_idlist ) ) {
    PyErr_SetString( PyExc_TypeError, "list required for idlist" );
    return NULL;
  }
  int64_t n_vertices = PyList_GET_SIZE( py_idlist );

  vgx_VertexAccessMode_t access_mode = VGX_VERTEX_ACCESS_WRITABLE_NOCREATE;

  if( py_mode ) {
    if( !PyVGX_PyObject_CheckString( py_mode ) ) {
      PyErr_SetString( PyExc_TypeError, "a string or bytes-like object is required" );
      return NULL;
    }

    Py_ssize_t msz;
    Py_ssize_t ucsz;
    const char *mode;
    if( (mode = PyVGX_PyObject_AsStringAndSize( py_mode, &msz, &ucsz )) == NULL ) {
      return NULL;
    }

    if( msz == 1 && *mode == 'r' ) {
      access_mode = VGX_VERTEX_ACCESS_READONLY;
    }
    else if( msz != 1 || *mode != 'a' ) {
      PyErr_SetString( PyExc_ValueError, "mode must be 'a' or 'r'" );
      return NULL;
    }
  }

  vgx_VertexList_t *vertices = NULL;

  XTRY {
    // Acquire vertices
    if( (vertices = __open_vertices( pygraph, NULL, py_idlist, access_mode, timeout_ms )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }
   
    // Build result
    if( (py_vertices = PyList_New( n_vertices )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    int64_t gen = _get_pyvertex_generation_guard();

    for( int64_t i=0; i<n_vertices; i++ ) {
      PyObject *py_vertex = p_PyVGX_Vertex__VertexType->tp_alloc( p_PyVGX_Vertex__VertexType, 0 );
      if( py_vertex == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      ((PyVGX_Vertex*)py_vertex)->vertex = iVertex.List.Get( vertices, i );
      ((PyVGX_Vertex*)py_vertex)->pygraph = pygraph;
      ((PyVGX_Vertex*)py_vertex)->gen_guard = gen;
      PyList_SET_ITEM( py_vertices, i, py_vertex );
    }
  }
  XCATCH( errcode ) {
    if( py_vertices ) {
      PyVGX_DECREF( py_vertices );
      py_vertices = NULL;
    }
    if( vertices ) {
      int64_t sz = iVertex.List.Size( vertices );
      for( int64_t i=0; i<sz; i++ ) {
        vgx_Vertex_t *vertex = iVertex.List.Get( vertices, i );
        if( vertex ) {
          CALLABLE( graph )->advanced->ReleaseVertex( graph, &vertex );
        }
      }
    }

  }
  XFINALLY {
    iVertex.List.Delete( &vertices );
  }

  return py_vertices;

}



/******************************************************************************
 * PyVGX_Graph__CloseVertices
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CloseVertices__doc__,
  "CloseVertices( vertices ) -> number of vertices released\n"
  "\n"
  "Close vertex objects\n"
  "\n"
  "vertices  : List of vertex objects to close\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__CloseVertices
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__CloseVertices( PyVGX_Graph *pygraph, PyObject *py_vertices ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  vgx_VertexList_t *vertices = NULL;

  int64_t n_released = 0;

  XTRY {
    // Vertices must be list
    if( !PyList_CheckExact( py_vertices ) ) {
      PyErr_SetString( PyExc_TypeError, "a list is required" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    // Allocate vertex list
    int64_t sz_list = PyList_Size( py_vertices );
    if( (vertices = iVertex.List.New( sz_list )) == NULL ) {
      PyErr_SetString( PyExc_MemoryError, "out of memory" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Populate vertex list with those vertices that are not already closed
    vgx_Vertex_t *vertex;
    int64_t vidx = 0;
    for( int64_t i=0; i<sz_list; i++ ) {
      PyVGX_Vertex *py_vertex = (PyVGX_Vertex*)PyList_GET_ITEM( py_vertices, i );
      // Ignore None objects
      if( (PyObject*)py_vertex == Py_None ) {
        continue;
      }
      if( !py_vertex || !PyVGX_Vertex_CheckExact( py_vertex ) ) {
        PyErr_SetString( PyExc_TypeError, "list item must be vertex object" );
        THROW_SILENT( CXLIB_ERR_API, 0x003 );
      }
      // NOTE: vertex may be NULL if it is already closed
      // We only add non-NULL vertices to list, and truncate size at the end
      if( (vertex = py_vertex->vertex) != NULL ) {
        // Make sure vertex belongs to current graph
        if( __get_vertex_graph( pygraph, py_vertex ) == NULL ) {
          THROW_SILENT( CXLIB_ERR_API, 0x004 );
        }
        // Populate list. 
        iVertex.List.Set( vertices, vidx, vertex );

        // Inc
        ++vidx;
      }
    }
    iVertex.List.Truncate( vertices, vidx );

    BEGIN_PYVGX_THREADS {
      n_released = CALLABLE( graph )->advanced->AtomicReleaseVertices( graph, &vertices );
    } END_PYVGX_THREADS;

    if( n_released < 0 ) {
      PyErr_SetString( PyVGX_AccessError, "one or more vertices not closable" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Invalidate vertex references in all python vertices
    for( int64_t i=0; i<sz_list; i++ ) {
      PyVGX_Vertex *py_vertex = (PyVGX_Vertex*)PyList_GET_ITEM( py_vertices, i );
      py_vertex->vertex = NULL;
    }

  }
  XCATCH( errcode ) {
    n_released = -1;
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "unknown internal error" );
    }
  }
  XFINALLY {
    iVertex.List.Delete( &vertices );
  }

  if( n_released < 0 ) {
    return NULL;
  }
  else {
    return PyLong_FromLongLong( n_released );
  }

}



/******************************************************************************
 * PyVGX_Graph__GetVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetVertex__doc__,
  "GetVertex( id ) -> Vertex object\n"
  "\n"
  "Get the vertex from the graph in readonly mode.\n"
  "This is equivalent to calling OpenVertex( id, mode='r' )\n"
  "\n"
  "id       :  Unique string identifier for vertex\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetVertex( PyVGX_Graph *pygraph, PyObject *py_id ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *vcargs[] = {
    NULL,
    (PyObject*)pygraph, // graph
    py_id,              // id
    NULL,               // type
    g_py_char_r,        // mode
    NULL,               // lifespan
    NULL                // timeout
  };

  PyObject *py_vertex = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vertex__VertexType, vcargs+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  return py_vertex;
}



/******************************************************************************
 * PyVGX_Graph__VertexIdByAddress
 *
 ******************************************************************************
 */
PyDoc_STRVAR( VertexIdByAddress__doc__,
  "VertexIdByAddress( address ) -> identifier\n"
  "\n"
  "Return the identifier of vertex at memory address\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__VertexIdByAddress
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__VertexIdByAddress( PyVGX_Graph *pygraph, PyObject *py_address ) {
  PyObject *py_id = NULL;

  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  QWORD addr;

  if( PyLong_CheckExact( py_address ) ) {
    addr = PyLong_AsLongLong( py_address );
  }
  else {
    PyErr_SetString( PyExc_TypeError, "an integer is required" );
    return NULL;
  }

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  CString_t *CSTR__id = NULL;
  
  BEGIN_PYVGX_THREADS {
    CSTR__id = CALLABLE( graph )->advanced->GetVertexIDByAddress( graph, addr, &reason );
  } END_PYVGX_THREADS;

  if( CSTR__id ) {
    py_id = PyUnicode_FromStringAndSize( CStringValue( CSTR__id ), CStringLength( CSTR__id ) );
    iString.Discard( &CSTR__id );
  }
  else {
    iPyVGXBuilder.SetPyErrorFromAccessReason( "?", reason, NULL );
  }

  return py_id;
}



/******************************************************************************
 * PyVGX_Graph__Connect
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Connect__doc__,
  "Connect( initial, arc, terminals, lifespan=-1, condition=None, timeout=0 ) -> 1 if new arc created, 0 if arc already exists or its value updated\n"
  "\n"
  "Create explicit connection(s) between vertices by inserting an arc from the\n"
  "initial vertex to each vertex in terminals.\n"
  "\n"
  "initial   : Unique ID string or vertex instance opened for writable access\n"
  "arc       : Arc specification for the connection from initial to terminal(s)\n"
  "terminals : List of terminals or single terminal, where a terminal is either\n"
  "            a unique ID string or a vertex instance opened for writable access\n"
  "lifespan  : Number of seconds arc will exist until it is automatically deleted\n"
  "            (implicitly adds M_TMC, M_TMM and M_TMX to form a multiple arc)\n"
  "condition : Perform connect only when arc condition is met, where condition is\n"
  "            is specified using arc filter syntax\n"
  "timeout   : Timeout (in milliseconds) for acquiring writable access to both\n"
  "            initial and terminal vertices\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Connect
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Connect( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"initial", "arc", "terminal", "lifespan", "condition", "timeout", NULL};

  PyObject *py_ret = NULL;

  // ------------
  // 1. Arguments
  // ------------

  PyObject *py_initial = NULL;
  PyObject *py_arc;
  PyObject *py_terminal = NULL;
  int lifespan = -1;
  PyObject *py_arc_condition = NULL;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "OOO|iOi", kwlist, &py_initial, &py_arc, &py_terminal, &lifespan, &py_arc_condition, &timeout_ms ) ) {
    return NULL;
  }

  // -------------------------------------------------------------
  // 2. Get the vertex IDs from python strings or vertex instances
  // -------------------------------------------------------------
  vgx_Arc_t arc = {0};
  pyvgx_VertexIdentifier_t initial_ident;
  pyvgx_VertexIdentifier_t terminal_ident;

  __pyvgx_reset_vertex_identifier( &initial_ident );
  __pyvgx_reset_vertex_identifier( &terminal_ident );

  vgx_VertexList_t *vertices_WL = NULL;
  int64_t sz_list = 0;
  // Parse head vertex/vertices argument

  int64_t n_acq = 0;

  // We have a list of terminals
  if( PyList_Check( py_terminal ) ) {
    if( PyList_GET_SIZE( py_terminal ) == 0 ) {
      return PyLong_FromLong( 0 );
    }
    // Open the initial and the list of terminals atomically.
    if( (vertices_WL = __open_vertices( pygraph, py_initial, py_terminal, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, timeout_ms )) == NULL ) {
      return NULL;
    }
    n_acq = sz_list = iVertex.List.Size( vertices_WL );
  }

  // Single terminal
  else {
    // Parse tail vertex argument
    bool use_obid = PyLong_CheckExact( py_initial );
    if( iPyVGXParser.GetVertexID( pygraph, py_initial, &initial_ident, &arc.tail, use_obid, "Initial ID" ) < 0 ) {
      return NULL;
    } 
    // Parse head vertex argument
    if( iPyVGXParser.GetVertexID( pygraph, py_terminal, &terminal_ident, &arc.head.vertex, false, "Terminal ID" ) < 0 ) {
      return NULL;
    } 

    if( arc.tail ) {
      // Make sure initial is writable if vertex object was supplied
      if( !__vertex_is_locked_writable_by_current_thread( arc.tail ) ) {
        PyErr_Format( PyVGX_AccessError, "Initial vertex not writable: '%s'", initial_ident.id );
        return NULL;
      }
      ++n_acq;
    }

    if( arc.head.vertex ) {
      // Make sure terminal is writable if vertex object was supplied
      if( !__vertex_is_locked_writable_by_current_thread( arc.head.vertex ) ) {
        PyErr_Format( PyVGX_AccessError, "Terminal vertex not writable: '%s'", terminal_ident.id );
        return NULL;
      }
      ++n_acq;
    }
  }

  // Arc Condition
  vgx_ArcConditionSet_t *arc_condition_set = NULL;
  if( py_arc_condition ) {
    if( (arc_condition_set = iPyVGXParser.NewArcConditionSet( graph, py_arc_condition, VGX_ARCDIR_OUT )) == NULL ) {
      return NULL;
    }
  }

  vgx_Relation_t *relation = NULL;
  CString_t *CSTR__error = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  int n_connected = 0;

  int err = 0;
  const char *err_id = NULL;


  if( n_acq >= 2 ) {
    // Create the relation without vertex names
    relation = iPyVGXParser.NewRelation( graph, NULL, py_arc, NULL );
  }
  else if( initial_ident.id && terminal_ident.id ) {
    // Create the relation
    relation = iPyVGXParser.NewRelation( graph, initial_ident.id, py_arc, terminal_ident.id );
  }


  BEGIN_PYVGX_THREADS {

    XTRY {
      if( relation == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x261 );
      }
      // Lifespan implies timestamps
      else if( lifespan > -1 ) {
        iRelation.AutoTimestamps( relation );
      }

      // List of terminals and all vertices acquired writelocked
      if( vertices_WL ) {
        arc.tail = iVertex.List.Get( vertices_WL, 0 );
        int nc;
        for( int64_t i=1; i<sz_list; i++ ) {
          arc.head.vertex = iVertex.List.Get( vertices_WL, i );
          if( (nc = CALLABLE( graph )->advanced->Connect_WL( graph, &relation->relationship, &arc, lifespan, arc_condition_set, &reason, &CSTR__error )) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x262 );
          }
          n_connected += nc;
        }
      }
      // Both vertex arguments are writable vertex objects
      else if( n_acq == 2 ) {
        // Execute connect
        n_connected = CALLABLE( graph )->advanced->Connect_WL( graph, &relation->relationship, &arc, lifespan, arc_condition_set, &reason, &CSTR__error );
      }
      // One or both vertex arguments are string identifiers
      else {
        // Execute connect
        n_connected = CALLABLE( graph )->simple->Connect( graph, relation, lifespan, arc_condition_set, timeout_ms, &reason, &CSTR__error );
      }

      // Check error
      if( n_connected < 0 ) {
        __py_set_vertex_pair_error_ident( pygraph, py_initial, py_terminal, reason, &CSTR__error );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x265 );
      }

    }
    XCATCH( errcode ) {
      err = errcode;
      n_connected = -1;
    }
    XFINALLY {
      iArcConditionSet.Delete( &arc_condition_set );
      iRelation.Delete( &relation );
      if( vertices_WL ) {
        int64_t n_rel;
        if( (n_rel = CALLABLE( graph )->advanced->AtomicReleaseVertices( graph, &vertices_WL )) != sz_list ) {
          reason = VGX_ACCESS_REASON_ERROR;
          __format_error_string( &CSTR__error, "Failed to release vertices (%lld) after operation completed, released %lld", sz_list, n_rel );
          n_connected = -1;
        }
      }
    }
  } END_PYVGX_THREADS;

  if( n_connected >= 0 ) {
    py_ret = PyLong_FromLong( n_connected );
  }
  else if( !PyErr_Occurred() ) {
    iPyVGXBuilder.SetPyErrorFromAccessReason( err_id, reason, &CSTR__error );
  }

  iString.Discard( &CSTR__error );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__Disconnect
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Disconnect__doc__,
  "Disconnect( id, arc=\"*\", neighbor=\"*\", timeout=0 ) -> n\n"
  "\n"
  "Remove one or more arcs incident on a specified vertex\n"
  "\n"
  "id       : Unique ID string or vertex instance opened for writable access.\n"
  "           This is the vertex whose inarcs and/or outarcs should be\n"
  "           removed.\n"
  "arc      : Arc specification for arcs to be removed. By default all arcs\n"
  "           are removed.\n"
  "neighbor : Condition for terminal vertex for arcs to be removed.\n"
  "           No condition by default. NOTE: Exact ID match is currently\n"
  "           the only supported vertex condition.\n"
  "timeout  : Timeout (in milliseconds) for acquiring writable access to\n"
  "           anchor vertex, and optionally for the neighbor vertex if\n"
  "           vertex condition is specified.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Disconnect
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Disconnect( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Args
  static char *kwlist[] = {"id", "arc", "neighbor", "timeout", NULL};

  PyObject *py_result = NULL;

  // ------------
  // 1. Arguments
  // ------------

  PyObject *py_anchor = NULL;
  PyObject *py_arc_condition = NULL;
  PyObject *py_vertex_condition = NULL;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "O|OOi", kwlist, &py_anchor, &py_arc_condition, &py_vertex_condition, &timeout_ms ) ) {
    return NULL;
  }

  // ----------------------------------------------------------
  // 2. Get the vertex ID from python string or vertex instance
  // ----------------------------------------------------------
  pyvgx_VertexIdentifier_t anchor_ident;
  vgx_Vertex_t *anchor = NULL;
  if( iPyVGXParser.GetVertexID( pygraph, py_anchor, &anchor_ident, &anchor, true, "Vertex ID" ) < 0 ) {
    return NULL;
  }

  if( anchor ) {
    // Make sure vertex is writable if vertex object was supplied
    if( !__vertex_is_locked_writable_by_current_thread( anchor ) ) {
      PyErr_Format( PyVGX_AccessError, "Vertex not writable: '%s'", CALLABLE( anchor )->IDString( anchor ) );
      return NULL;
    }
  }

  int64_t n_disconnected = 0;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;
  const char *err_id = NULL;

  BEGIN_PYVGX_THREADS {
    vgx_VertexCondition_t *vertex_condition = NULL;
    vgx_ArcConditionSet_t *arc_condition_set = NULL;
    vgx_AdjacencyQuery_t *query = NULL;

    XTRY {
      vgx_IGraphSimple_t *igraph = CALLABLE(graph)->simple;

      // Construct the arc condition if specified
      if( py_arc_condition ) {
        arc_condition_set = iPyVGXParser.NewArcConditionSet( graph, py_arc_condition, VGX_ARCDIR_OUT );
      }
      // Otherwise construct a default wildcard arc condition
      else {
        arc_condition_set = iArcConditionSet.NewEmpty( graph, true, VGX_ARCDIR_ANY );
      }

      if( arc_condition_set == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x271 );
      }

      // Construct the neighbor condition if specified
      if( py_vertex_condition ) {
        if( (vertex_condition = iPyVGXParser.NewVertexCondition( graph, py_vertex_condition, VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST )) == NULL ) { // TODO: support probe vector and similarity??
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x272 );
        }
      }

      // If anchor is * and simple non-wildcard terminal given (without other conditions), flip the two and invert direction
      if( CharsEqualsConst( anchor_ident.id, "*" ) ) {
        // Non-wildcard terminal ID given
        vgx_StringList_t *list;
        const CString_t *CSTR__id;
        if( vertex_condition && 
            (list=vertex_condition->CSTR__idlist) != NULL && 
            iString.List.Size( list ) == 1 &&
            (CSTR__id = iString.List.GetItem( list, 0 )) != NULL && 
            !CStringContains( CSTR__id, "*" ) )
        {
          // TODO!!!: Check if other neighbor conditions exist and terminate query if those conditions are not met.
          //          Right now any conditions will be IGNORED

          // Create new query starting at terminal instead and reverse the direction
          if( (query = iGraphQuery.NewAdjacencyQuery( graph, CStringValue( CSTR__id ), &CSTR__error )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x273 );
          }

          // Delete the vertex condition (it is IGNORED in this special case, see above for TODO)
          iVertexCondition.Delete( &vertex_condition );

          // Reverse direction of arc
          if( arc_condition_set->arcdir == VGX_ARCDIR_OUT ) {
            arc_condition_set->arcdir = VGX_ARCDIR_IN;
          }
          else if( arc_condition_set->arcdir == VGX_ARCDIR_IN ) {
            arc_condition_set->arcdir = VGX_ARCDIR_OUT;
          }
          else {
            // ANY direction stays the same
          }
        }
        // *->* not allowed
        else {
          PyVGXError_SetString( PyVGX_QueryError, "disconnect conditions too general" );
          THROW_SILENT( CXLIB_ERR_API, 0x274 );
        }
      }
      else {
        if( (query = iGraphQuery.NewAdjacencyQuery( graph, anchor_ident.id, &CSTR__error )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x275 );
        }
      }

      // Set query timeout
      CALLABLE( query )->SetTimeout( query, timeout_ms, false );

      // Set the arc condition
      CALLABLE( query )->AddArcConditionSet( query, &arc_condition_set );

      // Set any neighbor vertex condition we may have
      if( vertex_condition ) {
        CALLABLE( query )->AddVertexCondition( query, &vertex_condition );
      }

      // Execute disconnect
      n_disconnected = igraph->Disconnect( graph, query );
      
      if( n_disconnected < 0 || query->CSTR__error ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x276 );
      }
    }
    XCATCH( errcode ) {
      if( query && query->access_reason != VGX_ACCESS_REASON_NONE ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( anchor_ident.id, query->access_reason, &query->CSTR__error );
      }
      else if( query && query->CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue(query->CSTR__error) );
      }
      else if( CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue(CSTR__error) );
      }
      else if( err_id ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( err_id, reason, &CSTR__error );
      }
      else {
        PyVGX_SetPyErr( errcode );
      }
      iVertexCondition.Delete( &vertex_condition );
      iArcConditionSet.Delete( &arc_condition_set );

      n_disconnected = -1;
    }
    XFINALLY {
      iGraphQuery.DeleteAdjacencyQuery( &query );
    }
  } END_PYVGX_THREADS;

  if( n_disconnected >= 0 ) {
    py_result = PyLong_FromLongLong( n_disconnected );
  }
  iString.Discard( &CSTR__error );

  return py_result;
}



/******************************************************************************
 * PyVGX_Graph__Neighborhood
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Neighborhood( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Neighborhood( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__NewNeighborhoodQuery
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewNeighborhoodQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewNeighborhoodQuery( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Adjacent
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Adjacent( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Adjacent( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__NewAdjacencyQuery
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewAdjacencyQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewAdjacencyQuery( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Aggregate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Aggregate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Aggregate( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__NewAggregatorQuery
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewAggregatorQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewAggregatorQuery( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Inarcs
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Inarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Inarcs( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Outarcs
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Outarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Outarcs( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Initials
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Initials( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Initials( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Terminals
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Terminals( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Terminals( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Arcs
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Arcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Arcs( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__NewArcsQuery
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewArcsQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewArcsQuery( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__Vertices
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Vertices( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_Vertices( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__NewVerticesQuery
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__NewVerticesQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewVerticesQuery( pygraph, args, kwds );
}



/******************************************************************************
 * PyVGX_Graph__VerticesType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( VerticesType__doc__,
  "VerticesType( type ) -> list of vertex names of given type, type=None specifies the untyped vertex\n"
  "\n"
  "Return a list of vertex names of the given type in the graph.\n"
  "\n"
 );

/**************************************************************************//**
 * PyVGX_Graph__VerticesType
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__VerticesType( PyVGX_Graph *pygraph, PyObject *py_type ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_vertices = NULL;

  const char *type_name = NULL;

  // A type string (including '*' as an explicit way to get all types)
  if( PyVGX_PyObject_CheckString( py_type ) ) {
    type_name =  PyVGX_PyObject_AsString( py_type );
  }
  // None = the default typeless vertex
  else if( py_type == Py_None ) {
    type_name = NULL;
  }
  // Error
  else {
    PyVGXError_SetString( PyExc_ValueError, "String or None expected" );
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    
    vgx_GlobalQuery_t *query = NULL;
    vgx_VertexCondition_t *vertex_condition = NULL;
    vgx_RankingCondition_t *ranking_condition = NULL;

    CString_t *CSTR__error = NULL;
    XTRY {
      vgx_IGraphSimple_t *igraph = CALLABLE(graph)->simple;

      // Create new query
      if( (query = iGraphQuery.NewGlobalQuery( graph, VGX_COLLECTOR_MODE_COLLECT_VERTICES, &CSTR__error )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x291 );
      }

      // Set Response Format
      CALLABLE( query )->SetResponseFormat( query, VGX_RESPONSE_SHOW_AS_STRING | VGX_RESPONSE_ATTR_ID );

      // New default vertex condition
      if( (vertex_condition = iVertexCondition.New( true )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x292 );
      }
      
      // Set the type filter in the vertex condition
      if( iVertexCondition.RequireType( vertex_condition, graph, VGX_VALUE_EQU, type_name ) < 0 ) {
        PyVGXError_SetString( PyExc_Exception, "internal error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x293 );
      }

      // Hand over ownership of vertex condition to the query object
      CALLABLE( query )->AddVertexCondition( query, &vertex_condition );

      // Create the ranking condition
      if( (ranking_condition = iRankingCondition.NewDefault()) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x294 );
      }

      // Hand over ownership of ranking condition to the query object
      CALLABLE( query )->AddRankingCondition( query, &ranking_condition );

      // ---------------------
      // Execute vertex search
      // ---------------------
      if( igraph->Vertices( graph, query ) < 0 ) {
        PyVGXError_SetString( PyVGX_SearchError, query->CSTR__error ? CStringValue(query->CSTR__error) : "unknown error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x295 );
      }

      if( (py_vertices = iPyVGXSearchResult.PyResultList_FromSearchResult( query->search_result, false, -1 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x297 );
      }

    }
    XCATCH( errcode ) {
      iVertexCondition.Delete( &vertex_condition );
      iRankingCondition.Delete( &ranking_condition );
      BEGIN_PYTHON_INTERPRETER {
        if( CSTR__error ) {
          PyVGXError_SetString( PyVGX_QueryError, CStringValue(CSTR__error) );
        }
        else {
          PyVGX_SetPyErr( errcode );
        }
        PyVGX_XDECREF( py_vertices );
      } END_PYTHON_INTERPRETER;
      py_vertices = NULL;
    }
    XFINALLY {
      // Delete the query
      iGraphQuery.DeleteGlobalQuery( &query );
      // Clean up any error
      if( CSTR__error ) {
        CStringDelete( CSTR__error );
      }
    }
  } END_PYVGX_THREADS;

  return py_vertices;
}



/******************************************************************************
 * PyVGX_Graph__Search
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Search__doc__,
  "Search( anchor, arc=\"*\", neighbor=\"*\", vector=[], sortby=S_NONE, offset=0, hits=25 )\n"
  "\n"
  "Perform a graph neighborhood search around anchor vertex and return a human-readable result list.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Search
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Search( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = {"anchor", "arc", "collect", "neighbor", "condition", "vector", "rank", "sortby", "offset", "hits", NULL};
  static const char *line = "--------------------------------------------------------------------------------\n";
  static char buffer[512] = {'\0'};

  static const char *archead_out = "->";
  static const char *archead_in = "-";
  static const char *arctail_out = "-";
  static const char *arctail_in = "<-";

#define __GET_STRING( Dict, Key ) PyVGX_PyObject_AsString( PyDict_GetItemString( Dict, Key ) )
#define __GET_LONGLONG( Dict, Key ) PyLong_AsLongLong( PyDict_GetItemString( Dict, Key ) )
#define __GET_LONG( Dict, Key ) PyLong_AsLong( PyDict_GetItemString( Dict, Key ) )
#define __GET_DOUBLE( Dict, Key ) PyFloat_AsDouble( PyDict_GetItemString( Dict, Key ) )

  // Argument list for validation only
  const char *anchor = NULL;
  int64_t sz_anchor = 0;
  PyObject *py_dummy = NULL;
  int int_dummy = 0;
  int64_t hits = 25;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|s#OiOOOOiiL", kwlist, &anchor, &sz_anchor, &py_dummy, &int_dummy, &py_dummy, &py_dummy, &py_dummy, &py_dummy, &int_dummy, &int_dummy, &hits ) ) {
    return NULL;
  }

  PyObject *py_search_result = NULL;
  PyObject *py_ret = NULL;
  PyObject *py_repr = NULL;

  XTRY {

    PyObject *py_results = NULL;

    // Set the result specifier in the kwds dict
    int err = 0;
    if( kwds == NULL ) {
      if( (kwds = PyDict_New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x2A1 );
      }
    }
    else {
      Py_INCREF( kwds );
    }
    err += iPyVGXBuilder.DictMapStringToLongLong( kwds, "result", VGX_RESPONSE_SHOW_AS_DICT | VGX_RESPONSE_SHOW_WITH_METAS );
    err += iPyVGXBuilder.DictMapStringToLongLong( kwds, "fields", VGX_RESPONSE_ATTRS_FULL ^ VGX_RESPONSE_ATTRS_DETAILS );
    err += iPyVGXBuilder.DictMapStringToLongLong( kwds, "hits", hits );

    if( err ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x2A2 );
    }

    // Execute
    if( anchor ) {
      if( (py_search_result = pyvgx_Neighborhood( pygraph, args, kwds )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2A3 );
      }
      py_results = PyDict_GetItemString( py_search_result, "neighborhood" );
    }
    else {
      if( (py_search_result = PyVGX_Graph__Vertices( pygraph, args, kwds )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2A4 );
      }
      py_results = PyDict_GetItemString( py_search_result, "vertices" );
    }

    // Format result
    if( py_results ) {
      // Header
      // ---------
      PRINT_OSTREAM( "%s", line );
      if( anchor ) {
        PRINT_OSTREAM( "Neighborhood of: %s\n", anchor );
      }
      else {
        PRINT_OSTREAM( "Global search\n" );
      }
      PRINT_OSTREAM( "%s", line );

      // Result list
      PyObject *py_item;
      Py_ssize_t n_items = PyList_Size( py_results );
      for( int n=0; n<n_items; n++ ) {
        py_item = PyList_GET_ITEM( py_results, n );
        const char *initial = "";
        int distance = 0;
        if( anchor ) {
          initial = __GET_STRING( py_item, "anchor" );
          distance = __GET_LONG( py_item, "distance" );
        }
        const char *id = __GET_STRING( py_item, "id" );
        const char *obid = __GET_STRING( py_item, "internalid" );
        const char *type = __GET_STRING( py_item, "type" );
        int64_t degree = __GET_LONGLONG( py_item, "degree" );
        int64_t indegree = __GET_LONGLONG( py_item, "indegree" );
        int64_t outdegree = __GET_LONGLONG( py_item, "outdegree" );
        PyObject *py_vector = PyDict_GetItemString( py_item, "vector" );
        PyObject *py_properties = PyDict_GetItemString( py_item, "properties" );
        float sim = 100.0f * (float)__GET_DOUBLE( py_item, "similarity" );
        int ham = (int)__GET_LONG( py_item, "hamming-distance" );


        PRINT_OSTREAM( "  %d:\n", n+1 );
        PRINT_OSTREAM( "     #intid   %s\n", obid );
        PRINT_OSTREAM( "     #name    %s\n", id );
        PRINT_OSTREAM( "     #type    %s\n", type );

        if( anchor ) {
          PyObject *py_arc_dict = PyDict_GetItemString( py_item, "arc" );
          const char *dir = "";
          const char *rel = "";
          const char *mod = "";
          const char *val = "";
          const char *arctail = arctail_out;
          const char *archead = archead_in;
          PyObject *py_val_str = NULL;
          if( py_arc_dict ) {
            PyObject *py_arc_direction = PyDict_GetItemString( py_arc_dict, "direction" );
            PyObject *py_arc_relationship = PyDict_GetItemString( py_arc_dict, "relationship" );
            PyObject *py_arc_modifier = PyDict_GetItemString( py_arc_dict, "modifier" );
            PyObject *py_arc_value = PyDict_GetItemString( py_arc_dict, "value" );
            if( py_arc_direction && PyVGX_PyObject_CheckString( py_arc_direction ) ) {
              dir = PyVGX_PyObject_AsString( py_arc_direction );
            }
            if( py_arc_relationship && PyVGX_PyObject_CheckString( py_arc_relationship ) ) {
              rel = PyVGX_PyObject_AsString( py_arc_relationship );
            }
            if( py_arc_modifier && PyVGX_PyObject_CheckString( py_arc_modifier ) ) {
              mod = PyVGX_PyObject_AsString( py_arc_modifier );
            }
            if( py_arc_value ) {
              if( (py_val_str = PyObject_Str( py_arc_value )) != NULL ) {
                val = PyVGX_PyObject_AsString( py_val_str );
              }
            }
          }

          if( CharsEqualsConst( dir, "D_IN" ) ) {
            arctail = arctail_in;
          }
          else if( CharsEqualsConst( dir, "D_OUT" ) ) {
            archead = archead_out;
          }
          else if( CharsEqualsConst( dir, "D_BOTH" ) ) {
            arctail = arctail_in;
            archead = archead_out;
          }

          PRINT_OSTREAM( "     #dist    %d\n", distance );
          PRINT_OSTREAM( "     #arc     " );
          if( distance > 1 ) {
            for( int d=1; d<distance; d++ ) {
              PRINT_OSTREAM( "[...]-" );
            }
          }
          PRINT_OSTREAM( "(%s)%s[ %s (%s=%s) ]%s(%s)\n", initial, arctail, rel, mod, val, archead, id );

          PyVGX_XDECREF( py_val_str );
        }

        PRINT_OSTREAM( "     #degree  %lld  %lld->(%s)->%lld\n", degree, indegree, id, outdegree );
        PRINT_OSTREAM( "     #vector  sim=%6.2f%%  ham=%2d  vec=", sim, ham );
        if( (py_repr = PyObject_Repr( py_vector )) != NULL ) {
          PRINT_OSTREAM( PyVGX_PyObject_AsString( py_repr ) );
          PyVGX_DECREF( py_repr );
        }
        PRINT_OSTREAM( "\n" );
        PRINT_OSTREAM( "     #fields  " );
        if( (py_repr = PyObject_Repr( py_properties )) != NULL ) {
          PRINT_OSTREAM( PyVGX_PyObject_AsString( py_repr ) );
          PyVGX_DECREF( py_repr );
        }
        PRINT_OSTREAM( "\n\n" );
      }
      
      // ----------
      PRINT_OSTREAM( "%s", line );
      // timing
      PyObject *py_timing = PyDict_GetItemString( py_search_result, "time" );
      if( py_timing ) {
        double t_total = PyFloat_AsDouble( PyDict_GetItemString( py_timing, "total" ) );
        double t_search = PyFloat_AsDouble( PyDict_GetItemString( py_timing, "search" ) );
        double t_result = PyFloat_AsDouble( PyDict_GetItemString( py_timing, "result" ) );
        PRINT_OSTREAM( "time:      %.5f  (search=%.5f result=%.5f)\n", t_total, t_search, t_result );
      }
      // counts
      PyObject *py_counts = PyDict_GetItemString( py_search_result, "counts" );
      if( py_counts ) {
        if( anchor ) {
          int64_t n_neighbors = PyLong_AsLongLong( PyDict_GetItemString( py_counts, "neighbors" ) );
          int64_t n_arcs = PyLong_AsLongLong( PyDict_GetItemString( py_counts, "arcs" ) );
          PRINT_OSTREAM( "arcs:      %lld\n", n_arcs );
          PRINT_OSTREAM( "neighbors: %lld\n", n_neighbors );
        }
        else {
          int64_t n_vertices = PyLong_AsLongLong( PyDict_GetItemString( py_counts, "vertices" ) );
          PRINT_OSTREAM( "vertices:  %lld\n", n_vertices );
        }
      }

      // ---------
      PRINT_OSTREAM( "%s", line );

    }

    // Ignore all render errors than may have occurred
    PyErr_Clear();

  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
    PyVGX_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {
    PyVGX_DECREF( kwds );
    PyVGX_XDECREF( py_search_result );
  }

  if( !PyErr_Occurred() ) {
    py_ret = Py_None;
    Py_INCREF( Py_None );
  }

  return py_ret;

#undef __GET_STRING
#undef __GET_LONGLONG
#undef __GET_LONG
#undef __GET_DOUBLE
}



/******************************************************************************
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx__enumerator_as_dict(
  vgx_Graph_t *graph,
  int (*GetStrings)( vgx_Graph_t *graph, CtptrList_t **CSTR__strings ),
  bool use_qwo,
  f_generic_enum_encoder EncodeString_CS )
{
  static const char *encstr = "<encoded>";

  PyObject *py_dict = PyDict_New();
  if( !py_dict ) {
    return NULL;
  }

  CtptrList_t *CSTR__list = NULL;
  int64_t sz = 0;

  struct s_enum_list {
    const char *string;
    uint64_t encoded_string;
    int64_t refc;
  } *enum_list = NULL;

  BEGIN_PYVGX_THREADS {
    XTRY {
      if( GetStrings( graph, &CSTR__list ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2B1 );
      }
      CtptrList_vtable_t *ilist = CALLABLE( CSTR__list );
      sz = ilist->Length( CSTR__list );
      if( (enum_list = calloc( sz, sizeof( struct s_enum_list ) )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_MEMORY, 0x2B2 );
      }
      GRAPH_LOCK( graph ) {
        for( int64_t i=0; i<sz; i++ ) {
          tptr_t data;
          const CString_t *CSTR__str;
          if( ilist->Get( CSTR__list, i, &data ) == 1 ) {
            CSTR__str = (const CString_t*)(use_qwo ? TPTR_GET_POINTER( &data ) : TPTR_GET_PTR56( &data ));
            CString_attr attr = CStringAttributes( CSTR__str );
            enum_list[i].string = CStringValue( CSTR__str );
            // Encoding = enumeration
            if( EncodeString_CS ) {
              enum_list[i].encoded_string = EncodeString_CS( graph, CSTR__str );
            }
            // Encoding = object address
            else {
              enum_list[i].encoded_string = (uint64_t)CSTR__str; // ADDRESS!
              if( attr ) {
                // Override with simple marker if this string is binary encoding of some other object
                enum_list[i].string = encstr;
              }
            }
            enum_list[i].refc = _cxmalloc_object_refcnt_nolock( CSTR__str );
          }
        }
      } GRAPH_RELEASE;
    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
    }
    XFINALLY {

    }
  } END_PYVGX_THREADS;

  PyObject *py_tuple;
  PyObject *py_val;
  PyObject *py_refc;
  char buf[33];
  for( int64_t i=0; i<sz; i++ ) {
    const char *key = enum_list[i].string;
    uint64_t val = enum_list[i].encoded_string;
    int64_t refc = enum_list[i].refc;
    int ret = -1; // prove success below
    if( (py_tuple = PyTuple_New( 2 )) != NULL ) {
      if( EncodeString_CS ) {
        py_val = PyLong_FromUnsignedLongLong( val );
      }
      else {
        const CString_t *CSTR__val = (CString_t*)val;
        if( CSTR__val != NULL && COMLIB_OBJECT_CLASSMATCH( CSTR__val, COMLIB_CLASS_CODE( CString_t ) ) ) {
          const objectid_t *obid = COMLIB_OBJECT_GETID( CSTR__val );
          py_val = PyUnicode_FromStringAndSize( idtostr( buf, obid ), 32 );
        }
        else {
          py_val = Py_None;
          Py_INCREF( Py_None );
        }
      }
      py_refc = PyLong_FromLongLong( refc );
      if( py_val && py_refc ) {
        PyTuple_SET_ITEM( py_tuple, 0, py_val );
        PyTuple_SET_ITEM( py_tuple, 1, py_refc );
        ret = PyVGX_DictStealItemString( py_dict, key, py_tuple ); // success if ret=0
      }
      else {
        PyVGX_XDECREF( py_val );
        PyVGX_XDECREF( py_refc );
      }
    }

    if( ret < 0 ) {
      PyVGX_DECREF( py_dict );
      break;
    }
  }

  if( CSTR__list ) {
    COMLIB_OBJECT_DESTROY( CSTR__list );
  }

  if( enum_list ) {
    free( enum_list );
  }

  return py_dict;
}



/******************************************************************************
 *
 ******************************************************************************
 */
DLL_HIDDEN int64_t pyvgx__enumerator_size( vgx_Graph_t *graph, int64_t (*GetSize)( vgx_Graph_t *graph ) ) {
  int64_t sz = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {
      sz = GetSize( graph );
  } GRAPH_RELEASE;
  } END_PYVGX_THREADS;
  return sz;
}



/******************************************************************************
 * PyVGX_Graph_EnumRelationship
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EnumRelationship__doc__,
  "EnumRelationship( type[, enum] ) -> enum\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EnumRelationship
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EnumRelationship( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "type", "enum", NULL };

  const char *rel;
  int64_t sz_rel;
  int code = -1;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#|i", kwlist, &rel, &sz_rel, &code ) ) {
    return NULL;
  }

  vgx_predicator_rel_enum relcode = VGX_PREDICATOR_REL_NONE;

  BEGIN_PYVGX_THREADS {
    CString_t *CSTR__relationship = NewEphemeralCStringLen( graph, rel, (int)sz_rel, 0 );
    if( CSTR__relationship ) {
      QWORD relhash = CStringHash64( CSTR__relationship );

      // Implicit code assignment
      if( code < 0 ) {
        CString_t *CSTR__mapped_instance = NULL; 
        relcode = iEnumerator_OPEN.Relationship.Encode( graph, CSTR__relationship, &CSTR__mapped_instance, true );
      }
      else if( __relationship_in_user_range( code ) ) {
        relcode = iEnumerator_OPEN.Relationship.Set( graph, relhash, CSTR__relationship, (vgx_predicator_rel_enum)code );
      }
      else {
        relcode = VGX_PREDICATOR_REL_INVALID;
      }
      CStringDelete( CSTR__relationship );
    }

  } END_PYVGX_THREADS;

  switch( relcode ) {
  case VGX_PREDICATOR_REL_NONE:
    PyErr_SetString( PyExc_MemoryError, "string error" );
    return NULL;
  case VGX_PREDICATOR_REL_INVALID:
    PyErr_SetString( PyVGX_EnumerationError, "invalid relationship" );
    return NULL;
  case VGX_PREDICATOR_REL_COLLISION:
    PyErr_SetString( PyVGX_EnumerationError, "redefinition" );
    return NULL;
  case VGX_PREDICATOR_REL_NO_MAPPING:
    PyErr_SetString( PyVGX_EnumerationError, "enumeration type space exhausted" );
    return NULL;
  case VGX_PREDICATOR_REL_LOCKED:
    PyErr_SetString( PyVGX_EnumerationError, "enumerator locked" );
    return NULL;
  default:
    if( !__relationship_enumeration_in_user_range( relcode ) ) { 
      PyErr_Format( PyVGX_EnumerationError, "%x", relcode );
      return NULL;
    }
  }

  return PyLong_FromLong( relcode );
}



/******************************************************************************
 * PyVGX_Graph_EnumVertexType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EnumVertexType__doc__,
  "EnumVertexType( type[, enum] ) -> enum\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EnumVertexType
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EnumVertexType( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "type", "enum", NULL };

  const char *vxtype;
  int64_t sz_vxtype;
  int code = -1;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#|i", kwlist, &vxtype, &sz_vxtype, &code ) ) {
    return NULL;
  }

  vgx_vertex_type_t typecode = VERTEX_TYPE_ENUMERATION_NONE;

  BEGIN_PYVGX_THREADS {
    CString_t *CSTR__vertex_type_name = NewEphemeralCStringLen( graph, vxtype, (int)sz_vxtype, 0 );
    if( CSTR__vertex_type_name ) {
      QWORD typehash = CStringHash64( CSTR__vertex_type_name );

      // Implicit code assignment
      if( code < 0 ) {
        CString_t *CSTR__mapped_instance = NULL; 
        typecode = iEnumerator_OPEN.VertexType.Encode( graph, CSTR__vertex_type_name, &CSTR__mapped_instance, true );
      }
      else if( __vertex_type_in_user_range( code ) ) {
        typecode = iEnumerator_OPEN.VertexType.Set( graph, typehash, CSTR__vertex_type_name, (vgx_vertex_type_t)code );
      }
      else {
        typecode = VERTEX_TYPE_ENUMERATION_INVALID;
      }
      CStringDelete( CSTR__vertex_type_name );
    }

  } END_PYVGX_THREADS;

  switch( typecode ) {
  case VERTEX_TYPE_ENUMERATION_NONE:
    PyErr_SetString( PyExc_MemoryError, "string error" );
    return NULL;
  case VERTEX_TYPE_ENUMERATION_INVALID:
    PyErr_SetString( PyVGX_EnumerationError, "invalid vertex type" );
    return NULL;
  case VERTEX_TYPE_ENUMERATION_COLLISION:
    PyErr_SetString( PyVGX_EnumerationError, "redefinition" );
    return NULL;
  case VERTEX_TYPE_ENUMERATION_NO_MAPPING:
    PyErr_SetString( PyVGX_EnumerationError, "enumeration type space exhausted" );
    return NULL;
  case VERTEX_TYPE_ENUMERATION_LOCKED:
    PyErr_SetString( PyVGX_EnumerationError, "enumerator locked" );
    return NULL;
  default:
    if( !__vertex_type_enumeration_valid( typecode ) ) { 
      PyErr_Format( PyVGX_EnumerationError, "%x", typecode );
      return NULL;
    }
  }

  return PyLong_FromLong( typecode );
}



/******************************************************************************
 * PyVGX_Graph_EnumDimension
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EnumDimension__doc__,
  "EnumDimension( dim[, enum] ) -> enum\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EnumDimension
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EnumDimension( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "dim", "enum", NULL };

  ENSURE_FEATURE_VECTORS_OR_RETURN_NULL;

  const char *dim;
  int64_t sz_dim;
  int code = -1;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#|i", kwlist, &dim, &sz_dim, &code ) ) {
    return NULL;
  }

  feature_vector_dimension_t dimcode = FEATURE_VECTOR_DIMENSION_NONE;

  vgx_Similarity_t *simobj = graph->similarity;
  if( simobj == NULL ) {
    PyErr_SetString( PyVGX_InternalError, "graph has no similarity object" );
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    CString_t *CSTR__dimension = NewEphemeralCStringLen( graph, dim, (int)sz_dim, 0 );
    if( CSTR__dimension ) {
      // Implicit code assignment
      if( code < 0 ) {
        dimcode = iEnumerator_OPEN.Dimension.EncodeChars( simobj, dim, NULL, true );
      }
      else if( __vector_dimension_in_range( code ) ) {
        int sz;
        QWORD dimhash = _vgx_enum__dimhash( CStringValue( CSTR__dimension ), &sz );
        dimcode = iEnumerator_OPEN.Dimension.Set( simobj, dimhash, CSTR__dimension, (feature_vector_dimension_t)code );
      }
      else {
        dimcode = FEATURE_VECTOR_DIMENSION_INVALID;
      }
      CStringDelete( CSTR__dimension );
    }
  } END_PYVGX_THREADS;

  switch( dimcode ) {
  case FEATURE_VECTOR_DIMENSION_NONE:
    PyErr_SetString( PyExc_MemoryError, "string error" );
    return NULL;
  case FEATURE_VECTOR_DIMENSION_LOCKED:
    PyErr_SetString( PyVGX_EnumerationError, "enumerator locked" );
    return NULL;
  case FEATURE_VECTOR_DIMENSION_INVALID:
    PyErr_SetString( PyVGX_EnumerationError, "invalid dimension" );
    return NULL;
  case FEATURE_VECTOR_DIMENSION_COLLISION:
    PyErr_SetString( PyVGX_EnumerationError, "redefinition" );
    return NULL;
  case FEATURE_VECTOR_DIMENSION_NOENUM:
    PyErr_SetString( PyVGX_EnumerationError, "enumeration type space exhausted" );
    return NULL;
  default:
    if( !__vector_dimension_in_range( dimcode ) ) { 
      PyErr_Format( PyVGX_EnumerationError, "%x", dimcode );
      return NULL;
    }
  }

  return PyLong_FromLong( dimcode );
}



/******************************************************************************
 * PyVGX_Graph_EnumKey
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EnumKey__doc__,
  "EnumKey( key ) -> keyhash\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EnumKey
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EnumKey( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "key", NULL };

  const char *key;
  int64_t sz_key;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist, &key, &sz_key ) ) {
    return NULL;
  }

  CString_t *CSTR__error = NULL;
  shortid_t keyhash = 0;

  BEGIN_PYVGX_THREADS {

    // Set key
    keyhash = iEnumerator_OPEN.Property.Key.SetChars( graph, key );
    
    // Invalid key?
    if( keyhash == 0 ) {
      if( !iString.Validate.StorableKey( key ) ) {
        CSTR__error = CStringNew( "invalid key" );
      }
      else if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
        CSTR__error = CStringNew( "enumerator locked" );
      }
      else {
        CSTR__error = CStringNew( "key enumerator error" );
      }
    }

  } END_PYVGX_THREADS;


  if( CSTR__error ) {
    PyErr_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
    CStringDelete( CSTR__error );
    return NULL;
  }
  else {
    return PyLong_FromUnsignedLongLong( keyhash );
  }

}



/******************************************************************************
 * PyVGX_Graph_EnumValue
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EnumValue__doc__,
  "EnumValue( value ) -> objectid\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EnumValue
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EnumValue( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "value", NULL };

  const char *value;
  int64_t sz_value;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist, &value, &sz_value ) ) {
    return NULL;
  }

  if( sz_value > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
    PyErr_SetString( PyVGX_EnumerationError, "value string too large" );
    return NULL;
  }

  CString_t *CSTR__error = NULL;
  CString_t *CSTR__instance = NULL;

  BEGIN_PYVGX_THREADS {

    // Set value
    if( (CSTR__instance = iEnumerator_OPEN.Property.Value.StoreChars( graph, value, (int)sz_value )) == NULL ) {
      if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
        CSTR__error = CStringNew( "enumerator locked" );
      }
      else {
        CSTR__error = CStringNew( "value enumerator error" );
      }
    }

  } END_PYVGX_THREADS;

  if( CSTR__instance ) {
    char buf[33];
    return PyUnicode_FromStringAndSize( idtostr( buf, CStringObid( CSTR__instance ) ), 32 );
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
    CStringDelete( CSTR__error );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Graph__Relationship
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Relationship__doc__,
  "Relationship( enum ) -> relationship\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Relationship
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Relationship( PyVGX_Graph *pygraph, PyObject *py_enum ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( !PyLong_Check( py_enum ) ) {
    PyErr_SetString( PyVGX_EnumerationError, "an integer is required" );
    return NULL;
  }

  PyObject *py_rel = NULL;

  int e = (int)PyLong_AsLong( py_enum );

  if( __relationship_enumeration_valid( e ) ) {
    vgx_predicator_rel_enum relcode = (vgx_predicator_rel_enum)e;
    const CString_t *CSTR__relationship = NULL;
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( graph ) {
        if( iEnumerator_CS.Relationship.ExistsEnum( graph, relcode ) ) {
          CSTR__relationship = iEnumerator_CS.Relationship.Decode( graph, relcode );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;
    if( CSTR__relationship ) {
      py_rel = PyUnicode_FromStringAndSize( CStringValue( CSTR__relationship ), CStringLength( CSTR__relationship ) );
    }
    else {
      PyErr_SetString( PyVGX_EnumerationError, "undefined enumeration" );
    }
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, "invalid enumeration" );
  }

  return py_rel;
}



/******************************************************************************
 * PyVGX_Graph__VertexType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( VertexType__doc__,
  "VertexType( enum ) -> vertex_type\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__VertexType
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__VertexType( PyVGX_Graph *pygraph, PyObject *py_enum ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( !PyLong_Check( py_enum ) ) {
    PyErr_SetString( PyVGX_EnumerationError, "an integer is required" );
    return NULL;
  }

  PyObject *py_vertex_type_name = NULL;

  int e = (int)PyLong_AsLong( py_enum );

  if( __vertex_type_enumeration_valid( e ) ) {
    vgx_vertex_type_t vxtype = (vgx_vertex_type_t)e;
    const CString_t *CSTR__vertex_type_name = NULL;
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( graph ) {
        if( iEnumerator_CS.VertexType.ExistsEnum( graph, vxtype ) ) {
          CSTR__vertex_type_name = iEnumerator_CS.VertexType.Decode( graph, vxtype );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;
    if( CSTR__vertex_type_name ) {
      py_vertex_type_name = PyUnicode_FromStringAndSize( CStringValue( CSTR__vertex_type_name ), CStringLength( CSTR__vertex_type_name ) );
    }
    else {
      PyErr_SetString( PyVGX_EnumerationError, "undefined enumeration" );
    }
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, "invalid enumeration" );
  }

  return py_vertex_type_name;
}



/******************************************************************************
 * PyVGX_Graph__Dimension
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Dimension__doc__,
  "Dimension( enum ) -> dimension\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Dimension
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Dimension( PyVGX_Graph *pygraph, PyObject *py_enum ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  ENSURE_FEATURE_VECTORS_OR_RETURN_NULL;

  if( !PyLong_Check( py_enum ) ) {
    PyErr_SetString( PyVGX_EnumerationError, "an integer is required" );
    return NULL;
  }

  PyObject *py_dimension = NULL;

  int e = (int)PyLong_AsLong( py_enum );

  if( __vector_dimension_in_range( e ) ) {
    feature_vector_dimension_t dim = (feature_vector_dimension_t)e;
    const CString_t *CSTR__dimension = NULL;
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( graph ) {
        vgx_Similarity_t *sim = graph->similarity;
        if( iEnumerator_CS.Dimension.ExistsEnum( sim, dim ) ) {
          CSTR__dimension = iEnumerator_CS.Dimension.Decode( sim, dim );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;
    if( CSTR__dimension ) {
      py_dimension = PyUnicode_FromStringAndSize( CStringValue( CSTR__dimension ), CStringLength( CSTR__dimension ) );
    }
    else {
      PyErr_SetString( PyVGX_EnumerationError, "undefined enumeration" );
    }
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, "invalid enumeration" );
  }

  return py_dimension;
}



/******************************************************************************
 * PyVGX_Graph__Key
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Key__doc__,
  "Key( enum ) -> key\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Key
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Key( PyVGX_Graph *pygraph, PyObject *py_enum ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( !PyLong_Check( py_enum ) ) {
    PyErr_SetString( PyVGX_EnumerationError, "an integer is required" );
    return NULL;
  }

  PyObject *py_key = NULL;

  shortid_t keyhash = PyLong_AsUnsignedLongLongMask( py_enum );

  if( keyhash > 0 ) {
    const CString_t *CSTR__key = NULL;
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( graph ) {
        CSTR__key = iEnumerator_CS.Property.Key.Decode( graph, keyhash );
      } GRAPH_RELEASE;
      if( CSTR__key && !iString.Validate.StorableKey( CStringValue( CSTR__key ) ) ) {
        CSTR__key = NULL;
      }
    } END_PYVGX_THREADS;
    if( CSTR__key ) {
      py_key = PyUnicode_FromStringAndSize( CStringValue( CSTR__key ), CStringLength( CSTR__key ) );
    }
    else {
      PyErr_SetString( PyVGX_EnumerationError, "undefined enumeration" );
    }
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, "invalid enumeration" );
  }

  return py_key;
}



/******************************************************************************
 * PyVGX_Graph__Value
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Value__doc__,
  "Value( obid ) -> value\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Value
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Value( PyVGX_Graph *pygraph, PyObject *py_obid ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( !PyVGX_PyObject_CheckString( py_obid ) ) {
    PyErr_SetString( PyExc_TypeError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *s_obid;
  Py_ssize_t sz_s_obid;
  Py_ssize_t ucsz;

  if( (s_obid = PyVGX_PyObject_AsStringAndSize( py_obid, &sz_s_obid, &ucsz )) == NULL ) {
    return NULL;
  }

  if( sz_s_obid != 32 ) {
    PyErr_SetString( PyVGX_EnumerationError, "objectid string must be 32 hex digits" );
    return NULL;
  }

  PyObject *py_value = NULL;

  const CString_t *CSTR__value = NULL;
  BEGIN_PYVGX_THREADS {
    objectid_t obid = strtoid( s_obid );
    if( idnone( &obid ) ) {
      s_obid = NULL;
    }
    else {
      CSTR__value = iEnumerator_OPEN.Property.Value.Get( graph, &obid );
    }
  } END_PYVGX_THREADS;
  if( CSTR__value ) {
    py_value = PyUnicode_FromStringAndSize( CStringValue( CSTR__value ), CStringLength( CSTR__value ) );
  }
  else if( s_obid ) {
    PyErr_SetString( PyVGX_EnumerationError, "undefined enumeration" );
  }
  else {
    PyErr_SetString( PyVGX_EnumerationError, "invalid objectid" );
  }

  return py_value;
}



/******************************************************************************
 * PyVGX_Graph__Relationships
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Relationships__doc__,
  "Relationships() -> dict of {relationship:enumeration} used in graph\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Relationships
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Relationships( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->Relationships, false, (f_generic_enum_encoder)iEnumerator_CS.Relationship.GetEnum );
}



/******************************************************************************
 * PyVGX_Graph__VertexTypes
 *
 ******************************************************************************
 */
PyDoc_STRVAR( VertexTypes__doc__,
  "VertexTypes() -> dict of {vertex_type:enumeration} used in graph\n"
);

/**************************************************************************//**
 * PyVGX_Graph__VertexTypes
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__VertexTypes( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->VertexTypes, false, (f_generic_enum_encoder)iEnumerator_CS.VertexType.GetEnum );
}



/******************************************************************************
 * PyVGX_Graph__PropertyKeys
 *
 ******************************************************************************
 */
PyDoc_STRVAR( PropertyKeys__doc__,
  "PropertyKeys() -> dict of {property_key:enumeration} used in graph\n"
);

/**************************************************************************//**
 * PyVGX_Graph__PropertyKeys
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__PropertyKeys( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->PropertyKeys, false, (f_generic_enum_encoder)iEnumerator_CS.Property.Key.GetEnum );
}



/******************************************************************************
 * PyVGX_Graph__PropertyStringValues
 *
 ******************************************************************************
 */
PyDoc_STRVAR( PropertyStringValues__doc__,
  "PropertyStringValues() -> dict of {property_string_value:enumeration} used in graph\n"
);

/**************************************************************************//**
 * PyVGX_Graph__PropertyStringValues
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__PropertyStringValues( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->PropertyStringValues, true, NULL );
}



/******************************************************************************
 * PyVGX_Graph__Truncate
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Truncate__doc__,
  "Truncate( [type] ) -> number of vertices removed\n"
  "\n"
  "Erase the entire graph, or if type is specified erase vertices\n"
  "of that type. All arcs incident on the removed vertices will also\n"
  "be removed.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Truncate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Truncate( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( iSystem.IsSystemGraph( graph ) ) {
    PyErr_SetString( PyExc_Exception, "cannot truncate system graph!" );
    return NULL;
  }

  PyObject *py_n_removed = NULL;
  PyObject *py_type = NULL;

  if( !PyArg_ParseTuple( args, "|O", &py_type) ) {
    return NULL;
  }

  CString_t *CSTR__vertex_type = NULL;
  CString_t *CSTR__error = NULL;
  int64_t n_removed = 0;

  XTRY {
    bool alltypes = false;
    if( py_type ) {
      // A type string (including '*' as an explicit way to remove all types)
      if( PyVGX_PyObject_CheckString( py_type ) ) {
        const char *typestr =  PyVGX_PyObject_AsString( py_type );
        if( (CSTR__vertex_type = NewEphemeralCString( graph, typestr )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x2C1 );
        }
      }
      // None = the default typeless vertex
      else if( py_type == Py_None ) {
      }
      // Error
      else {
        PyVGXError_SetString( PyExc_ValueError, "String or None expected" );
        THROW_ERROR( CXLIB_ERR_API, 0x2C2 );
      }
    }
    else {
      alltypes = true;
    }

    BEGIN_PYVGX_THREADS {
      if( alltypes ) {
        n_removed = CALLABLE( graph )->simple->Truncate( graph, &CSTR__error );
      }
      else {
        n_removed = CALLABLE( graph )->simple->TruncateType( graph, CSTR__vertex_type, &CSTR__error );
      }
    } END_PYVGX_THREADS;
  }
  XCATCH( errcode ) {
    n_removed = -1;
  }
  XFINALLY {
    if( CSTR__vertex_type ) {
      CStringDelete( CSTR__vertex_type );
    }
  }

  if( n_removed >= 0 ) {
    py_n_removed = PyLong_FromLongLong( n_removed );
  }
  else {
    if( CSTR__error ) {
      PyVGXError_SetString( PyVGX_AccessError, CStringValue( CSTR__error ) );
      CStringDelete( CSTR__error );
    }
  }

  return py_n_removed;
}



/******************************************************************************
 * PyVGX_Graph__ResetSerial
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ResetSerial__doc__,
  "ResetSerial( [sn] ) -> long\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ResetSerial
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ResetSerial( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int64_t sn = 0;

  if( !PyArg_ParseTuple( args, "|L", &sn) ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    sn = CALLABLE( graph )->advanced->ResetSerial( graph, sn );
  } END_PYVGX_THREADS;

  return PyLong_FromLongLong( sn );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_name( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  const char *name = CStringValue( CALLABLE( graph )->Name( graph ) );

  return PyUnicode_FromString( name );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_path( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  const char *path = CALLABLE( graph )->FullPath( graph );

  return PyUnicode_FromString( path );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_size( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return PyLong_FromUnsignedLongLong( GraphSize( graph ) );
}



/******************************************************************************
 * PyVGX_Graph__Size
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Size__doc__,
  "Size() -> Number of arcs in graph\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Size
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Size( PyVGX_Graph *pygraph ) {
  return __PyVGX_Graph__get_size( pygraph, NULL );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_order( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
  return PyLong_FromUnsignedLongLong( GraphOrder( graph ) );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_objcnt( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int64_t order = GraphOrder( graph );
  int64_t size = GraphSize( graph );
  int64_t properties = GraphPropCount( graph );
  int64_t vectors = GraphVectorCount( graph );

  return Py_BuildValue( "{sLsLsLsL}", "order", order, "size", size, "properties", properties, "vectors", vectors );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__get_ts( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int64_t tms = _vgx_graph_milliseconds( graph );
  double t = tms / 1000.0;

  return PyFloat_FromDouble( t );
}



/******************************************************************************
 * PyVGX_Graph__Order
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Order__doc__,
  "Order( [type] ) -> Number of vertices in graph, optionally filtered by type\n"
);
//static PyObject * PyVGX_Graph__Order( PyVGX_Graph *pygraph, PyObject *args ) {

/**************************************************************************//**
 * PyVGX_Graph__Order
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Order( PyVGX_Graph *pygraph, PyObject *const *args, Py_ssize_t nargs ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  CString_t *CSTR__vertex_type = NULL;
  int64_t order = 0;
  XTRY {
    if( nargs == 0 ) {
      order = GraphOrder( graph );
    }
    else if( nargs == 1 ) {

      PyObject *py_type = args[0];

      // A type string (including '*' as an explicit way to specify all types)
      if( PyVGX_PyObject_CheckString( py_type ) ) {
        const char *typestr =  PyVGX_PyObject_AsString( py_type );
        if( (CSTR__vertex_type = NewEphemeralCString( graph, typestr )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x2D1 );
        }
      }
      // None = the default typeless vertex
      else if( py_type == Py_None ) {
      }
      // Error
      else {
        PyVGXError_SetString( PyExc_ValueError, "String or None expected" );
        THROW_ERROR( CXLIB_ERR_API, 0x2D2 );
      }

      BEGIN_PYVGX_THREADS {
        order = CALLABLE( graph )->simple->OrderType( graph, CSTR__vertex_type );
      } END_PYVGX_THREADS;

      if( order < 0 ) {
        PyErr_Format( PyExc_ValueError, "Invalid vertex type: '%s'", CStringValue( CSTR__vertex_type ) );
        THROW_SILENT( CXLIB_ERR_API, 0x2D3 );
      }
    }
    else {
      PyErr_SetString( PyExc_TypeError, "too many arguments" );
      THROW_SILENT( CXLIB_ERR_API, 0x2D4 );
    }
  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
    order = -1;
  }
  XFINALLY {
    if( CSTR__vertex_type ) {
      CStringDelete( CSTR__vertex_type );
    }
  }

  if( order >= 0 ) {
    return PyLong_FromUnsignedLongLong( order );
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
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Graph__sim( PyVGX_Graph *pygraph, void *closure ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( pygraph->py_sim ) {
    Py_INCREF( pygraph->py_sim );
    return (PyObject*)pygraph->py_sim;
  }
  else {
    PyObject *py_sim = NULL;
    PyObject *py_tuple = PyTuple_New(1);
    if( py_tuple ) {
      PyTuple_SET_ITEM( py_tuple, 0, PyVGX_PyCapsule_NewNoErr( pygraph->graph->similarity, NULL, NULL ) );
      py_sim = PyObject_CallObject( (PyObject*)p_PyVGX_Similarity__SimilarityType, py_tuple );
      PyVGX_DECREF( py_tuple );
    }
    return py_sim;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_accumulate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, vgx_predicator_modifier_enum modifier ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = {"initial", "relationship", "terminal", "delta", "timeout", NULL};

  PyObject *py_ret = NULL;

  // ------------
  // 1. Arguments
  // ------------

  PyObject *py_initial = NULL;
  const char *relationship = NULL;
  int64_t sz_relationship = 0;
  PyObject *py_terminal = NULL;
  PyObject *py_delta = NULL;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "Os#O|Oi", kwlist, &py_initial, &relationship, &sz_relationship, &py_terminal, &py_delta, &timeout_ms ) ) {
    return NULL;
  }

  // -------------------------------------------------------------
  // 2. Get the vertex IDs from python strings or vertex instances
  // -------------------------------------------------------------

  pyvgx_VertexIdentifier_t initial_ident;
  pyvgx_VertexIdentifier_t terminal_ident;
  vgx_Vertex_t *initial = NULL;
  vgx_Vertex_t *terminal = NULL;

  if( iPyVGXParser.GetVertexID( pygraph, py_initial, &initial_ident, &initial, true, "Initial ID" ) < 0 ) {
    return NULL;
  }

  if( iPyVGXParser.GetVertexID( pygraph, py_terminal, &terminal_ident, &terminal, true, "Terminal ID" ) < 0 ) {
    return NULL;
  }

  // Require initial vertex instance when system is attached
  if( initial ) {
    // Make sure initial vertex is writable if vertex object was supplied
    if( !__vertex_is_locked_writable_by_current_thread( initial ) ) {
      PyErr_Format( PyVGX_AccessError, "Initial vertex not writable: '%s'", CALLABLE( initial )->IDString( initial ) );
      return NULL;
    }
  }

  // Require terminal vertex instance when system is attached
  if( terminal ) {
    // Make sure terminal vertex is writable if vertex object was supplied
    if( !__vertex_is_locked_writable_by_current_thread( terminal ) ) {
      PyErr_Format( PyVGX_AccessError, "Terminal vertex not writable: '%s'", CALLABLE( terminal )->IDString( terminal ) );
      return NULL;
    }
  }

  vgx_predicator_val_t value;
  int err = 0;

  // Validate and set arcspec delta value
  switch( modifier ) {
  case VGX_PREDICATOR_MOD_COUNTER:
    if( py_delta == NULL ) {
      value.integer = 1; // default
    }
    else if( PyLong_CheckExact( py_delta ) ) {
      int ovf;
      value.integer = PyLong_AsLongAndOverflow( py_delta, &ovf );
      if( ovf != 0 ) {
        err = -1;
      }
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "delta must be integer" );
      err = -1;
    }
    break;
  case VGX_PREDICATOR_MOD_ACCUMULATOR:
    if( py_delta == NULL ) {
      value.real = 1.0f; // default
    }
    else if( PyNumber_Check( py_delta ) ) {
      value.real = (float)PyFloat_AsDouble( py_delta );
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "delta must be numeric" );
      err = -1;
    }
    break;
  default:
    PyVGXError_SetString( PyExc_ValueError, "invalid accumulator type" );
    err = -1;
  }

  if( err < 0 ) {
    return NULL;
  }

  CString_t *CSTR__error = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Relation_t *relation = NULL;
  const char *err_id = NULL;


  BEGIN_PYVGX_THREADS {

    vgx_AdjacencyQuery_t *query = NULL;
    vgx_VertexCondition_t *terminal_id_condition = NULL;
    vgx_ArcConditionSet_t *arc_condition_set = NULL;

    XTRY {
      int n_conn;
      vgx_IGraphSimple_t *igraph = CALLABLE(graph)->simple;

      // ===================
      // PHASE 1: ACCUMULATE
      // ===================

      // Create new relation
      if( (relation = iRelation.New( graph, initial_ident.id, terminal_ident.id, relationship, modifier, &value.bits )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E1 );
      }

      // Automatic timestamps?
      if( _auto_arc_timestamps ) {
        iRelation.AutoTimestamps( relation );
      }

      // -------------------------
      // EXECUTE CORE ACCUMULATION
      // -------------------------
      n_conn = igraph->Connect( graph, relation, -1, NULL, timeout_ms, &reason, &CSTR__error );

      if( n_conn < 0 ) {
        BEGIN_PYTHON_INTERPRETER {
          //iPyVGXBuilder.SetPyErrorFromAccessReason( initial_ident, reason, &CSTR__error );
          __py_set_vertex_pair_error_ident( pygraph, py_initial, py_terminal, reason, &CSTR__error );
        } END_PYTHON_INTERPRETER;
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E2 );
      }
      iRelation.Delete( &relation );

      // ========================
      // PHASE 2: VALUE RETRIEVAL
      // ========================
      
      // Create new query
      if( (query = iGraphQuery.NewAdjacencyQuery( graph, initial_ident.id, &CSTR__error )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E3 );
      }

      // Set query timeout
      CALLABLE( query )->SetTimeout( query, timeout_ms, false );

      // Set up the terminal ID condition
      if( (terminal_id_condition = iVertexCondition.New( true )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E4 );
      }
      if( iVertexCondition.RequireIdentifier( terminal_id_condition, VGX_VALUE_EQU, terminal_ident.id ) < 0 ) {
        if( terminal_id_condition->CSTR__error ) {
          CSTR__error = (CString_t*)terminal_id_condition->CSTR__error;
          terminal_id_condition->CSTR__error = NULL;
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E5 );
      }
      CALLABLE( query )->AddVertexCondition( query, &terminal_id_condition );

      // Set up the arc condition set
      if( (arc_condition_set= iArcConditionSet.NewSimple( graph, VGX_ARCDIR_OUT, true, relationship, modifier, VGX_VALUE_ANY, NULL, NULL )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E6 );
      }
      CALLABLE( query )->AddArcConditionSet( query, &arc_condition_set );

      // -------------------------
      // Get the accumulator value
      // -------------------------

      relation = igraph->HasAdjacency( graph, query );
      if( relation == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x2E7 );
      }

    }
    XCATCH( errcode ) {
      iRelation.Delete( &relation );
      if( CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__error ) );
      }
      else if( query && query->CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue( query->CSTR__error ) );
      }
      else if( err_id ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( err_id, reason, &CSTR__error );
      }
      else {
        PyVGX_SetPyErr( errcode );
      }
    }
    XFINALLY {
      // Delete any unused terminal condition
      iVertexCondition.Delete( &terminal_id_condition );
      // Delete any unused arc condition
      iArcConditionSet.Delete( &arc_condition_set );
      // Delete the query
      iGraphQuery.DeleteAdjacencyQuery( &query );
    }
  } END_PYVGX_THREADS;

  if( relation ) {
    switch( modifier ) {
    case VGX_PREDICATOR_MOD_COUNTER:
      py_ret = PyLong_FromUnsignedLong( relation->relationship.value.uinteger );
      break;
    case VGX_PREDICATOR_MOD_ACCUMULATOR:
      py_ret = PyFloat_FromDouble( relation->relationship.value.real );
      break;
    default:
      PyErr_SetString( PyVGX_QueryError, "Internal error" );
      break;
    }
    iRelation.Delete( &relation );
  }

  iString.Discard( &CSTR__error );


  return py_ret;
}


/******************************************************************************
 * PyVGX_Graph__Count
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Count__doc__,
  "Count( initial, relationship, terminal, delta=1, timeout=0 ) -> count_value\n"
  "\n"
  "Auto-increment relationship value with type M_CNT of arc going from\n"
  "initial to terminal vertex.\n"
  "\n"
  "initial      : Unique ID string or instance opened for writable access\n"
  "relationship : Relationship of arc whose value will be incremented\n"
  "terminal     : Unique ID string or instance opened for writable access\n"
  "delta        : Increment the arc value by this amount. If the arc does\n"
  "               not exist it is created and initialized to the specified value.\n"
  "timeout      : Timeout (in milliseconds) for acquiring writable access\n"
  "               to both initial and terminal vertices.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Count
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Count( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return __py_accumulate( pygraph, args, kwds, VGX_PREDICATOR_MOD_COUNTER );
}



/******************************************************************************
 * PyVGX_Graph__Accumulate
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Accumulate__doc__,
  "Accumulate( initial, relationship, terminal, delta=1.0, timeout=0 ) -> new_value\n"
  "\n"
  "Auto-accumulate a floating point relationship value with type M_ACC of arc\n"
  "going from initial to terminal vertex.\n"
  "\n"
  "initial      : Unique ID string or instance opened for writable access\n"
  "relationship : Relationship of arc whose value will be accumulated\n"
  "terminal     : Unique ID string or instance opened for writable access\n"
  "delta        : Add this amount to the arc value. If the arc does not exist\n"
  "               it is created and initialized to the specified value.\n"
  "timeout      : Timeout (in milliseconds) for acquiring writable access\n"
  "               to both initial and terminal vertices.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Accumulate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Accumulate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return __py_accumulate( pygraph, args, kwds, VGX_PREDICATOR_MOD_ACCUMULATOR );
}



/******************************************************************************
 * PyVGX_Graph__ShowVertex
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ShowVertex__doc__,
  "ShowVertex( id, timeout=0 ) -> None\n"
  "\n"
  "Prints a representation of the internal vertex data. Useful for debugging.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ShowVertex
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ShowVertex( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  //PyObject *py_result = NULL;
  vgx_IGraphSimple_t *igraph = CALLABLE(graph)->simple;
  vgx_Vertex_t *vertex_RO;
  PyObject *py_vertex = NULL;
  int timeout_ms = 0;
  vgx_AccessReason_t reason;

  if( !PyArg_ParseTuple( args, "O|i", &py_vertex, &timeout_ms ) ) {
    return NULL;
  }

  pyvgx_VertexIdentifier_t ident;
  if( iPyVGXParser.GetVertexID( pygraph, py_vertex, &ident, NULL, true, "Vertex ID" ) < 0 ) {
    return NULL;
  }

  const CString_t *CSTR__vertex_name = NewEphemeralCString( graph, ident.id );

  if( CSTR__vertex_name == NULL ) {
    PyErr_SetNone( PyExc_MemoryError);
    return NULL;
  }

  int err = 0;

  BEGIN_PYVGX_THREADS {
    // Open the vertex for reading
    if( (vertex_RO = igraph->OpenVertex( graph, CSTR__vertex_name, VGX_VERTEX_ACCESS_READONLY, timeout_ms, &reason, NULL )) != NULL ) {
      COMLIB_OBJECT_PRINT( vertex_RO );
      cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex_RO );
      int32_t true_refcnt = linehead->data.refc - 1;
      PYVGX_API_INFO( "graph", 0, "ACTUAL REFCOUNT: %ld", true_refcnt );
      igraph->CloseVertex( graph, &vertex_RO );
    }
    else {
      err = -1;
    }
    CStringDelete( CSTR__vertex_name );
  } END_PYVGX_THREADS;

  if( err == 0 ) {
    Py_RETURN_NONE;
  }
  else {
    iPyVGXBuilder.SetPyErrorFromAccessReason( ident.id, reason, NULL );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Graph__VertexDescriptor
 *
 ******************************************************************************
 */
PyDoc_STRVAR( VertexDescriptor__doc__,
  "VertexDescriptor( id ) -> descriptor string\n"
  "\n"
  "Return the vertex descriptor. Useful for debugging.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__VertexDescriptor
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__VertexDescriptor( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_desc = NULL;


  // ------------
  // 1. Arguments
  // ------------

  PyObject *py_vertex = NULL;

  if( !PyArg_ParseTuple( args, "O", &py_vertex ) ) {
    return NULL;
  }

  // ----------------------------------------------------------
  // 2. Get the vertex ID from python string or vertex instance
  // ----------------------------------------------------------

  pyvgx_VertexIdentifier_t ident;
  if( iPyVGXParser.GetVertexID( pygraph, py_vertex, &ident, NULL, true, "Vertex ID" ) < 0 ) {
    return NULL;
  }

  char *str = NULL;

  BEGIN_PYVGX_THREADS {

    vgx_Vertex_t *vertex_RO = NULL;
    const CString_t *CSTR__vertex_name = NULL;
    CStringQueue_t *strQ = NULL;
    vgx_IGraphSimple_t *igraph = CALLABLE(graph)->simple;

    XTRY {
      vgx_AccessReason_t reason;
      // Convert vertex name to internal string
      if( (CSTR__vertex_name = NewEphemeralCString( graph, ident.id )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x2F1 );
      }
      // Open the vertex in readonly mode
      if( (vertex_RO = igraph->OpenVertex( graph, CSTR__vertex_name, VGX_VERTEX_ACCESS_READONLY, -1, &reason, NULL )) == NULL ) {
        iPyVGXBuilder.SetPyErrorFromAccessReason( ident.id, reason, NULL );
        THROW_SILENT( CXLIB_ERR_LOOKUP, 0x2F2 );
      }
      // Make the output string queue
      if( (strQ = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x2F3 ); 
      }
      // Write the descriptor info into the string queue
      if( CALLABLE( vertex_RO )->Descriptor( vertex_RO, strQ ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x2F4 );
      }
      CALLABLE( strQ )->NulTermNolock( strQ );
      // Get the string queue contents
      if( CALLABLE( strQ )->ReadNolock( strQ, (void**)&str, -1 ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x2F5 );
      }

    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      if( str ) {
        ALIGNED_FREE( str );
        str = NULL;
      }
    }
    XFINALLY {
      if( vertex_RO ) {
        igraph->CloseVertex( graph, &vertex_RO );
      }
      if( CSTR__vertex_name ) {
        CStringDelete( CSTR__vertex_name );
      }
      if( strQ ) {
        COMLIB_OBJECT_DESTROY( strQ );
      }
    }
  } END_PYVGX_THREADS;

  // Create a Python return string
  if( str ) {
    py_desc = PyUnicode_FromString( str );
    ALIGNED_FREE( str );
  }

  return py_desc;
}



/******************************************************************************
 * PyVGX_Graph__GetMemoryUsage
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetMemoryUsage__doc__,
  "GetMemoryUsage() -> dict\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetMemoryUsage
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetMemoryUsage( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_meminfo = NULL;

  const char *item = NULL;
  int64_t sz_item = 0;

  if( args && !PyArg_ParseTuple( args, "|s#", &item, &sz_item ) ) {
    return NULL;
  }

  XTRY {

    vgx_MemoryInfo_t meminfo, *pmeminfo = NULL;
    int64_t value = -1;

    BEGIN_PYVGX_THREADS {
      const char *cursor = item;
      if( item == NULL || CharsStartsWithConst( item, "vgx." ) ) {
        GRAPH_LOCK( graph ) {
          meminfo = CALLABLE( graph )->advanced->GetMemoryInfo( graph );
          pmeminfo = &meminfo;
        } GRAPH_RELEASE;

        // Specific vgx metric
        if( item != NULL ) {
          // vgx
          cursor += 4;
          // vgx.vertex
          if( CharsStartsWithConst( cursor, "vertex." ) ) {
            cursor += 7;
            if( CharsEqualsConst( cursor, "object" ) ) {
              value = meminfo.pooled.vertex.object.bytes;
            }
            else if( CharsEqualsConst( cursor, "arcvector" ) ) {
              value = meminfo.pooled.vertex.arcvector.bytes;
            }
            else if( CharsEqualsConst( cursor, "property" ) ) {
              value = meminfo.pooled.vertex.property.bytes;
            }
          }
          // vgx.string
          else if( CharsEqualsConst( cursor, "string" ) ) {
            value = meminfo.pooled.string.data.bytes;
          }
          // vgx.index
          else if( CharsStartsWithConst( cursor, "index." ) ) {
            cursor += 6;
            if( CharsEqualsConst( cursor, "global" ) ) {
              value = meminfo.pooled.index.global.bytes;
            }
            else if( CharsEqualsConst( cursor, "type" ) ) {
              value = meminfo.pooled.index.type.bytes;
            }
          }
          // vgx.codec
          else if( CharsStartsWithConst( cursor, "codec." ) ) {
            cursor += 6;
            if( CharsEqualsConst( cursor, "vertextype" ) ) {
              value = meminfo.pooled.codec.vxtype.bytes;
            }
            else if( CharsEqualsConst( cursor, "relationship" ) ) {
              value = meminfo.pooled.codec.rel.bytes;
            }
            else if( CharsEqualsConst( cursor, "vertexprop" ) ) {
              value = meminfo.pooled.codec.vxprop.bytes;
            }
            else if( CharsEqualsConst( cursor, "dimension" ) ) {
              value = meminfo.pooled.codec.dim.bytes;
            }
          }
          // vgx.vector
          else if( CharsStartsWithConst( cursor, "vector." ) ) {
            cursor += 7;
            if( CharsEqualsConst( cursor, "internal" ) ) {
              value = meminfo.pooled.vector.internal.bytes;
            }
            else if( CharsEqualsConst( cursor, "external" ) ) {
              value = meminfo.pooled.vector.external.bytes;
            }
            else if( CharsEqualsConst( cursor, "dimension" ) ) {
              value = meminfo.pooled.vector.dimension.bytes;
            }
          }
          // vgx.ephemeral
          else if( CharsStartsWithConst( cursor, "ephemeral." ) ) {
            cursor += 10;
            if( CharsEqualsConst( cursor, "string" ) ) {
              value = meminfo.pooled.ephemeral.string.bytes;
            }
            else if( CharsEqualsConst( cursor, "vector" ) ) {
              value = meminfo.pooled.ephemeral.vector.bytes;
            }
            else if( CharsEqualsConst( cursor, "vertexmap" ) ) {
              value = meminfo.pooled.ephemeral.vtxmap.bytes;
            }
          }
          // vgx.event.schedule
          else if( CharsEqualsConst( cursor, "event.schedule" ) ) {
            value = meminfo.pooled.schedule.total.bytes;
          }
          // vgx.runtime
          else if( CharsEqualsConst( cursor, "runtime" ) ) {
            value = meminfo.system.process.use.bytes - meminfo.pooled.total.bytes;
          }
        }
      }
      else if( CharsStartsWithConst( item, "system." ) ) {
        cursor += 7;
        if( CharsEqualsConst( cursor, "available" ) ) {
          int64_t phys = 0;
          int64_t pct = 0;
          get_system_physical_memory( &phys, &pct, NULL );
          value = (int64_t)(((100.0 - pct) * phys)/100.0);
        }
        else if( CharsEqualsConst( cursor, "process" ) ) {
          get_system_physical_memory( NULL, NULL, &value );
        }
        else if( CharsEqualsConst( cursor, "global" ) ) {
          get_system_physical_memory( &value, NULL, NULL );
        }
      }
    } END_PYVGX_THREADS;

    if( item != NULL && value < 0 ) {
      PyErr_SetString( PyExc_ValueError, "unknown metric" );
      THROW_SILENT( CXLIB_ERR_LOOKUP, 0x001 );
    }

    // Already got a value
    if( value >= 0 ) {
      py_meminfo = PyLong_FromLongLong( value );
    }
    // Get all
    else if( pmeminfo  ) {
      py_meminfo = PyDict_New();
      if( !py_meminfo ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vertex.object", pmeminfo->pooled.vertex.object.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vertex.arcvector", pmeminfo->pooled.vertex.arcvector.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vertex.property", pmeminfo->pooled.vertex.property.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.string", pmeminfo->pooled.string.data.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.index.global", pmeminfo->pooled.index.global.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.index.type", pmeminfo->pooled.index.type.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.codec.vertextype", pmeminfo->pooled.codec.vxtype.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.codec.relationship", pmeminfo->pooled.codec.rel.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.codec.vertexprop", pmeminfo->pooled.codec.vxprop.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.codec.dimension", pmeminfo->pooled.codec.dim.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vector.internal", pmeminfo->pooled.vector.internal.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vector.external", pmeminfo->pooled.vector.external.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.vector.dimension", pmeminfo->pooled.vector.dimension.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.event.schedule", pmeminfo->pooled.schedule.total.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.ephemeral.string", pmeminfo->pooled.ephemeral.string.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.ephemeral.vector", pmeminfo->pooled.ephemeral.vector.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.ephemeral.vertexmap", pmeminfo->pooled.ephemeral.vtxmap.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "vgx.runtime", pmeminfo->system.process.use.bytes - pmeminfo->pooled.total.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "system.process", pmeminfo->system.process.use.bytes );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "system.available", (int64_t)((1.0 - pmeminfo->system.global.physical.utilization) * pmeminfo->system.global.physical.bytes) );
      iPyVGXBuilder.DictMapStringToLongLong( py_meminfo, "system.global", pmeminfo->system.global.physical.bytes );
    }

    if( !py_meminfo ) {
      PyErr_SetString( PyExc_ValueError, "unknown metric" );
      THROW_SILENT( CXLIB_ERR_LOOKUP, 0x003 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_meminfo );
    py_meminfo = NULL;
  }
  XFINALLY {
  }

  return py_meminfo;
}



/******************************************************************************
 * PyVGX_Graph__Status
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Status__doc__,
  "Status() -> dict\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Status
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Status( PyVGX_Graph *pygraph, PyObject *args ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  return pyvgx_GraphStatus( graph, args );
}



/******************************************************************************
 * PyVGX_Graph__ResetCounters
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ResetCounters__doc__,
  "ResetCounters() -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ResetCounters
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ResetCounters( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {

      // Reset query counters
      CALLABLE( graph )->ResetQueryCountNolock( graph );

    } GRAPH_RELEASE; 
  } END_PYVGX_THREADS;

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__Close
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Close__doc__,
  "Close() -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Close
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Close( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Only close vgx graph if it's owned by python graph wrapper
  if( pygraph->is_owner == false ) {
    PyErr_SetString( PyVGX_AccessError, "Permission denied: Borrowed graph cannot be accessed this way" );
    return NULL;
  }

  // Ensure graph object is valid
  if( !COMLIB_OBJECT_CLASSMATCH( graph, COMLIB_CLASS_CODE( vgx_Graph_t ) ) ) {
    PyErr_SetString( PyExc_Exception, "internal error: bad graph object" );
    return NULL;
  }

  // Ensure current thread owns the inner graph object
  uint32_t tid = 0;
  uint32_t owner = 0;

  BEGIN_PYVGX_THREADS {
    tid = GET_CURRENT_THREAD_ID();
    GRAPH_LOCK( graph ) {
      owner = _vgx_graph_owner_CS( graph );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( tid != owner ) {
    PyErr_Format( PyVGX_AccessError, "Internal permission error: Unexpected graph owner thread %u, expected %u", owner, tid );
    return NULL;
  }

  // Close all vertices held by current thread if this is the last instance
  if( graph->recursion_count == 1 ) {
    PyVGX_Graph__CloseAll( pygraph );
    // Clear owner
    CALLABLE( graph )->ClearExternalOwner( graph );
  }

  BEGIN_PYVGX_THREADS {
    // Release one level of recursion
    igraphfactory.CloseGraph( &graph, &owner );
  } END_PYVGX_THREADS;

  // Invalidate graph
  pygraph->graph = NULL;

  // Remove from open graph register (if indexed)
  __PyVGX_Graph__OGR_del( pygraph );

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__Erase
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Erase__doc__,
  "Erase() -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Erase
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Erase( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( iSystem.IsSystemGraph( graph ) ) {
    PyErr_SetString( PyExc_Exception, "cannot erase system graph!" );
    return NULL;
  }

  // -----------
  // Remove data
  // -----------
  CString_t *CSTR__error = NULL;
  int64_t ret = 0;
  PyObject *py_exc = PyExc_Exception;
  BEGIN_PYVGX_THREADS {
    XTRY {
      // Check allocators
      if( (ret = CALLABLE( graph )->advanced->DebugCheckAllocators( graph, NULL )) < 0 ) {
        __set_error_string( &CSTR__error, "ALLOCATOR CORRUPTION!" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Close all vertices
      if( (ret = CALLABLE( graph )->advanced->CloseOpenVertices( graph )) < 0 ) {
        __set_error_string( &CSTR__error, "Failed to close open vertices" );
        py_exc = PyVGX_AccessError;
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
      
      // Truncate
      if( (ret = CALLABLE( graph )->simple->Truncate( graph, &CSTR__error )) < 0 ) {
        __set_error_string( &CSTR__error, "Failed to truncate graph" );
        py_exc = PyVGX_AccessError;
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Reset serial number
      CALLABLE( graph )->advanced->ResetSerial( graph, 0 );

      // Save to remove data from disk
      if( (ret = iPyVGXPersist.Serialize( graph, 2000, true, true )) < 0 ) {
        __set_error_string( &CSTR__error, "Failed to persist truncated graph" );
        py_exc = PyVGX_OperationTimeout;
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
    }
    XCATCH( errcode ) {
    }
    XFINALLY {
    }
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown error";
    PyErr_SetString( py_exc, serr );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  CString_t *CSTR__name = iString.Clone( graph->CSTR__name );
  CString_t *CSTR__path = iString.Clone( graph->CSTR__path );
  if( CSTR__name == NULL || CSTR__path == NULL ) {
    iString.Discard( &CSTR__name );
    iString.Discard( &CSTR__path );
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }

  // -----------
  // Close graph
  // -----------
  PyObject *py_closed = PyVGX_Graph__Close( pygraph );
  if( py_closed ) {
    Py_DECREF( py_closed );

    // --------------------------
    // Remove graph from registry
    // --------------------------
    BEGIN_PYVGX_THREADS {
      ret = igraphfactory.DeleteGraph( CSTR__path, CSTR__name, 2000, false, &CSTR__error );
    } END_PYVGX_THREADS;

    if( ret < 0 ) {
      const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown error";
      PyErr_SetString( PyVGX_OperationTimeout, serr );
      iString.Discard( &CSTR__error );
    }
  }
  else {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    ret = -1;
  }

  iString.Discard( &CSTR__name );
  iString.Discard( &CSTR__path );

  if( ret < 0 ) {
    return NULL;
  }

  Py_RETURN_NONE;

}



/******************************************************************************
 * PyVGX_Graph__Save
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Save__doc__,
  "Save( timeout=1000, force=False, remote=False ) -> long\n"
  "\n"
  "Persist graph to disk. The optional timeout (in milliseconds) allows blocking\n"
  "while waiting for the graph to become idle in order for the operation to proceed.\n"
  "The default is nonblocking.\n"
  "\n"
  "The graph is readonly during the persist operation. It is possible for other\n"
  "threads to run queries while the graph is being persisted, but it is not\n"
  "possible to modify the graph.\n"
  "\n"
  "Data is normally saved incrementally, i.e. only modified structures are written\n"
  "to disk. To perform a complete serialization set force=True.\n"
  "\n"
  "By default data is persisted on the local host only. To trigger persist in\n"
  "attached instances set remote=True.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Save
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Save( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "timeout", "force", "remote", NULL };

  int timeout_ms = 1000;
  int force = false;
  int remote = false;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|iii", kwlist, &timeout_ms, &force, &remote ) ) {
    return NULL;
  }

  PyObject *py_ret = NULL;

  int64_t nqwords = 0;

  XTRY {
    nqwords = iPyVGXPersist.Serialize( graph, timeout_ms, force > 0, remote > 0 );

    if( nqwords < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x301 );
    }

  }
  XCATCH( errcode ) {
    PyVGX_SetPyErr( errcode );
    nqwords = -1;
  }
  XFINALLY {
  }

  if( nqwords >= 0 ) {
    py_ret = PyLong_FromLongLong( nqwords );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__Sync
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Sync__doc__,
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Sync
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Sync( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "hard", "timeout", NULL };

  int hard = false;
  int timeout_ms = 30000;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|ii", kwlist, &hard, &timeout_ms ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return NULL;
  }

  int err = 0;
  CString_t *CSTR__error = NULL;
  BEGIN_PYVGX_THREADS {
    // Make sure sync can start
    GRAPH_LOCK( SYSTEM ) {
      if( !SYSTEM->OP.system.state_CS.flags.sync_in_progress ) {
        SYSTEM->OP.system.state_CS.flags.sync_in_progress = true;
        SYSTEM->OP.system.progress_CS->state = VGX_OPERATION_SYSTEM_STATE__SYNC_Begin;
      }
      else {
        err = -1;
        PyErr_SetString( PyVGX_AccessError, "Synchronization already in progress" );
      }
    } GRAPH_RELEASE;

    // No errors, continue
    if( !err ) {
      // Sync single graph
      err = iOperation.Graph_OPEN.Sync( graph, hard, timeout_ms, &CSTR__error );

      // End sync
      GRAPH_LOCK( SYSTEM ) {
        SYSTEM->OP.system.state_CS.flags.sync_in_progress = false;
        SYSTEM->OP.system.progress_CS->state = VGX_OPERATION_SYSTEM_STATE__SYNC_End;
      } GRAPH_RELEASE;
    }
  } END_PYVGX_THREADS;

  if( err < 0 ) {
    if( CSTR__error ) {
      PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    else if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "operation failed" );
    }
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__GetOpenVertices
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetOpenVertices__doc__,
  "GetOpenVertices( [threadid] ) -> list\n"
  "\n"
  "Return a list of open vertices in this graph\n"
  "\n"
  "threadid :  When >0, return vertices currently owned by this thread.\n"
  "            The default is 0, which returns open vertices for all threads.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetOpenVertices
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetOpenVertices( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "threadid", NULL };
  
  int thread_id = -1;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &thread_id ) ) {
    return NULL;
  }

  typedef struct __s_Entry {
    CString_t *CSTR__id;
    int64_t tid;
    char label[3];
  } __Entry;

  PyObject *py_open_vertices = NULL;
  PyObject *py_entry = NULL;
  PyObject *py_vid = NULL;
  PyObject *py_tid = NULL;
  PyObject *py_lbl = NULL;
  
  Key64Value56List_t *readonly = NULL;
  Key64Value56List_t *writable = NULL;

  __Entry *entries = NULL;
  int64_t n_entries = 0;

  // Get all
  if( thread_id == -1 ) {
    thread_id = 0; // Internal code for all
  }
  // Get for current thread
  else if( thread_id == 0 ) {
    thread_id = GET_CURRENT_THREAD_ID();
  }

  XTRY {
    // Generate the internal lists
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( graph ) {
        if( CALLABLE( graph )->advanced->GetOpenVertices( graph, thread_id, &readonly, &writable ) >= 0 && readonly && writable ) {
          // Create the output list
          int64_t r = CALLABLE( readonly )->Length( readonly );
          int64_t w = CALLABLE( writable )->Length( writable );
          n_entries = r + w;
          const char *sRO = "RO";
          const char *sWL = "WL";
          const char *label = sRO;
          VertexAndInt64List_t *src = readonly;
          int64_t n_src = r;
          if( (entries = calloc( n_entries+1, sizeof( __Entry ) )) != NULL ) {
            __Entry *dest = entries;
            while( src ) {
              for( int64_t n=0; n<n_src; n++ ) {
                VertexAndInt64_t entry;
                CALLABLE( src )->Get( src, n, &entry.m128 );
                dest->CSTR__id = CALLABLE( graph )->advanced->GetVertexIDByAddress( graph, (QWORD)entry.vertex, NULL );
                dest->tid = entry.value;
                strcpy( dest->label, label );
                ++dest;
              }
              if( src == readonly ) {
                src = writable;
                n_src = w;
                label = sWL;
              }
              else {
                src = NULL;
              }
            }
            dest->tid = -1; // end
          }
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

    if( entries == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x311 );
    }

    if( (py_open_vertices = PyList_New( n_entries )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x312 );
    }

    __Entry *item = entries;
    for( int64_t idx=0; idx<n_entries; idx++, item++ ) {
      if( (py_entry = PyTuple_New( 3 )) != NULL ) {
        const char *str = item->CSTR__id ? CStringValue( item->CSTR__id ) : "?";
        py_vid = PyUnicode_FromString( str );
        py_tid = PyLong_FromLongLong( item->tid );
        py_lbl = PyUnicode_FromString( item->label );
        if( py_vid && py_tid && py_lbl ) {
          PyTuple_SET_ITEM( py_entry, 0, py_vid );
          py_vid = NULL;
          PyTuple_SET_ITEM( py_entry, 1, py_tid );
          py_tid = NULL;
          PyTuple_SET_ITEM( py_entry, 2, py_lbl );
          py_lbl = NULL;
        }
        else {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x313 );
        }
        PyList_SET_ITEM( py_open_vertices, idx, py_entry );
        py_entry = NULL;
      }
      else {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x314 );
      }
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_vid );
    PyVGX_XDECREF( py_tid );
    PyVGX_XDECREF( py_lbl );
    PyVGX_XDECREF( py_entry );
    PyVGX_XDECREF( py_open_vertices );
    py_open_vertices = NULL;
  }
  XFINALLY {
    if( entries ) {
      __Entry *item = entries;
      for( int64_t n=0; n<n_entries; n++, item++ ) {
        iString.Discard( &item->CSTR__id );
      }
      free( entries );
    }
    if( readonly ) {
      COMLIB_OBJECT_DESTROY( readonly );
    }
    if( writable ) {
      COMLIB_OBJECT_DESTROY( writable );
    }
  }

  return py_open_vertices;
}



/******************************************************************************
 * PyVGX_Graph__ShowOpenVertices
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ShowOpenVertices__doc__,
  "ShowOpenVertices() -> None\n"
  "Print a summary of all vertices in graph that have been acquired.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ShowOpenVertices
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ShowOpenVertices( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    CALLABLE( graph )->advanced->DebugPrintVertexAcquisitionMaps( graph );
  } END_PYVGX_THREADS;

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_populate_perfcounter( _framehash_counters_t *src ) {
  PyObject *py_counter = NULL;
  if( src->opcount > 0 ) {
    double opcnt = (double)src->opcount;
    double hitrate = src->cache.hits == 0 ? 0.0 : src->cache.hits / (double)(src->cache.hits + src->cache.misses);
    double avg_depth          = src->probe.depth / opcnt; 
    double avg_nCL            = src->probe.nCL / opcnt; 
    double avg_ncachecells    = src->probe.ncachecells / opcnt; 
    double avg_nleafcells     = src->probe.nleafcells / opcnt; 
    double avg_nleafzones     = src->probe.nleafzones / opcnt; 
    double avg_nbasementcells = src->probe.nbasementcells / opcnt; 

    py_counter = Py_BuildValue( "{s:K s:{s:d} s:{s:d s:d s:d s:d s:d s:d}}", 
                                  "opcount", src->opcount,
                                      "cache", 
                                         "hitrate", hitrate,
                                              "probe",
                                                 "depth",          avg_depth,
                                                 "cpu_cachlines",  avg_nCL,
                                                 "cache_cells",    avg_ncachecells,
                                                 "leaf_cells",     avg_nleafcells,
                                                 "leaf_zones",     avg_nleafzones,
                                                 "basement_cells", avg_nbasementcells
                                  );
  }
  else {
    py_counter = Py_BuildValue( "{s:K}", "opcount", 0 );
  }
  return py_counter;
}



/******************************************************************************
 * PyVGX_Graph__GetIndexCounters
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetIndexCounters__doc__,
  "GetIndexCounters( [type] ) -> dict\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetIndexCounters
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetIndexCounters( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "type", NULL };
  
  const char *type_name = NULL;
  Py_ssize_t sz_type_name;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "z#", kwlist, &type_name, &sz_type_name ) ) {
    return NULL;
  }

  CString_t *CSTR__vertex_type = NewEphemeralCString( graph, type_name );
  if( CSTR__vertex_type == NULL ) {
    PyErr_SetString( PyExc_MemoryError, "out of memory" );
    return NULL;
  }

  framehash_perfcounters_t counters = {0};
  CString_t *CSTR__error = NULL;
  int err;
  if( (err = _vxgraph_vxtable__get_index_counters_OPEN( graph, &counters, CSTR__vertex_type, &CSTR__error )) < 0 ) {
    if( CSTR__error ) {
      PyErr_SetString( PyVGX_QueryError, CStringValue( CSTR__error ) );
    }
    else {
      PyErr_SetString( PyVGX_InternalError, "internal error" );
    }
  }
  iString.Discard( &CSTR__error );
  iString.Discard( &CSTR__vertex_type );

  if( err < 0 ) {
    return NULL;
  }

  PyObject *py_read = __py_populate_perfcounter( &counters.read );
  PyObject *py_write = __py_populate_perfcounter( &counters.write );
  return Py_BuildValue( "{s:N,s:N}", "read", py_read, "write", py_write );
}



/******************************************************************************
 * PyVGX_Graph__ResetIndexCounters
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ResetIndexCounters__doc__,
  "ResetIndexCounters() -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ResetIndexCounters
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ResetIndexCounters( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  if( _vxgraph_vxtable__reset_index_counters_OPEN( graph ) < 0 ) {
    PyErr_SetString( PyVGX_InternalError, "internal error" );
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__EventBacklog
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EventBacklog__doc__,
  "EventBacklog() -> info string\n"
  "\n"
  "Return a string representation of the current state of the\n"
  "internal event processor.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EventBacklog
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EventBacklog( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_info = NULL;
  CString_t *CSTR__backlog = NULL;
  
  BEGIN_PYVGX_THREADS {
    CSTR__backlog = iGraphEvent.FormatBacklogInfo( graph, NULL );
  } END_PYVGX_THREADS;

  if( CSTR__backlog ) {
    py_info = PyUnicode_FromString( CStringValue( CSTR__backlog ) );
    CStringDelete( CSTR__backlog );
  }
  else {
    PyVGXError_SetString( PyVGX_InternalError, "Event processor: no info available" );
  }

  return py_info;
}



/******************************************************************************
 * PyVGX_Graph__EventEnable
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EventEnable__doc__,
  "EventEnable() -> None\n"
  "\n"
  "Start the internal event processor. This resumes time-to-live (TTL)\n"
  "processing of vertices and arcs.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EventEnable
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EventEnable( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int enabled = 0;
  int readonly;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {
      readonly = _vgx_is_readonly_CS( &graph->readonly );
      if( !readonly ) {
        if( !iGraphEvent.IsEnabled( graph ) ) {
          GRAPH_SUSPEND_LOCK( graph ) {
            vgx_ExecutionTimingBudget_t long_timeout = _vgx_get_graph_execution_timing_budget( graph, 10000 );
            enabled = iGraphEvent.NOCS.Enable( graph, &long_timeout );
          } GRAPH_RESUME_LOCK;
        }
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( readonly ) {
    PyErr_SetString( PyExc_Exception, "Event processor cannot be enabled for readonly graph" );
    return NULL;
  }
  else if( enabled < 0 ) {
    PyErr_SetString( PyExc_Exception, "Failed to enable event processor (timed out)" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Graph__EventDisable
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EventDisable__doc__,
  "EventDisable() -> None\n"
  "\n"
  "Disable the internal event processor. This halts time-to-live (TTL)\n"
  "processing of vertices and arcs.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EventDisable
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EventDisable( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int disabled = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {
      // Readonly graph (EVP is paused)
      if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
        // Reset the "resume EVP on leaving readonly mode" flag.
        _vgx_readonly_suspend_EVP_CS( &graph->readonly, false );
      }
      if( iGraphEvent.IsEnabled( graph ) ) {
        GRAPH_SUSPEND_LOCK( graph ) {
          vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( graph, 5000 );
          disabled = iGraphEvent.NOCS.Disable( graph, &disable_budget );
        } GRAPH_RESUME_LOCK;
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( disabled < 0 ) {
    PyErr_SetString( PyExc_Exception, "Failed to disable event processor (timed out)" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Graph__EventFlush
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EventFlush__doc__,
  "EventFlush() -> None\n"
  "\n"
  "Manually flush all pending internal events from queues to their\n"
  "respective schedules.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EventFlush
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EventFlush( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int flushed = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {
      if( iGraphEvent.IsEnabled( graph ) ) {
        GRAPH_SUSPEND_LOCK( graph ) {
          vgx_ExecutionTimingBudget_t flush_budget = _vgx_get_graph_execution_timing_budget( graph, 5000 );
          flushed = iGraphEvent.NOCS.Flush( graph, &flush_budget );
        } GRAPH_RESUME_LOCK;
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( flushed < 0 ) {
    PyErr_SetString( PyExc_Exception, "Failed to flush event processor (timed out)" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Graph__EventParam
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EventParam__doc__,
  "EventParam() -> dict\n"
  "\n"
  "Return a dictionary of parameters currently in effect for\n"
  "the internal event processor.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__EventParam
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__EventParam( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  PyObject *py_info = PyDict_New();
  PyObject *py_state = PyDict_New();
  PyObject *py_input = PyDict_New();
  PyObject *py_long = PyDict_New();
  PyObject *py_long_map = PyDict_New();
  PyObject *py_medium = PyDict_New();
  PyObject *py_medium_map = PyDict_New();
  PyObject *py_short = PyDict_New();
  PyObject *py_short_map = PyDict_New();
  PyObject *py_executor = PyDict_New();
  PyObject *py_executor_map = PyDict_New();

  XTRY {
    if( !py_info || !py_state || !py_input || !py_long || !py_long_map || !py_medium || !py_medium_map || !py_short || !py_short_map || !py_executor || !py_executor_map ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    vgx_EventBacklogInfo_t backlog;
    BEGIN_PYVGX_THREADS {
      backlog = iGraphEvent.BacklogInfo( graph );
    } END_PYVGX_THREADS;

    if( backlog.filled ) {
      typedef struct __s_entry {
        PyObject *dict1;
        PyObject *dict2;
        vgx_EventScheduleInfo_t *sched;
        int64_t *n_items;
      } __entry;

      __entry entries[] = {
        { py_long,      py_long_map,      &backlog.param.LongTerm,    &backlog.n_long    },
        { py_medium,    py_medium_map,    &backlog.param.MediumTerm,  &backlog.n_med     },
        { py_short,     py_short_map,     &backlog.param.ShortTerm,   &backlog.n_short   },
        { py_executor,  py_executor_map,  &backlog.param.Executor,    &backlog.n_current },
        { 0 }
      };
      __entry *cursor = entries;
      __entry *entry;
      while( (entry = cursor++)->dict1 ) {
        vgx_EventScheduleInfo_t *s = entry->sched;
        iPyVGXBuilder.DictMapStringToFloat(    entry->dict1,  "migration_cycle",      s->migration_cycle_tms/1000.0 );
        iPyVGXBuilder.DictMapStringToFloat(    entry->dict1,  "migration_margin",     s->migration_margin_tms/1000.0 );
        iPyVGXBuilder.DictMapStringToFloat(    entry->dict1,  "insertion_threshold",  s->insertion_threshold_tms/1000.0 );
        iPyVGXBuilder.DictMapStringToLongLong( entry->dict1,  "items",                *entry->n_items );
        iPyVGXBuilder.DictMapStringToFloat(    entry->dict2,  "interval",             s->partial_interval_tms/1000.0 );
        iPyVGXBuilder.DictMapStringToInt(      entry->dict2,  "order",                s->map_order );
        iPyVGXBuilder.DictMapStringToInt(      entry->dict2,  "size",                 s->partials );
      }

      iPyVGXBuilder.DictMapStringToLongLong( py_executor, "completed", backlog.n_exec );
      iPyVGXBuilder.DictMapStringToLongLong( py_input, "api", backlog.n_api );
      iPyVGXBuilder.DictMapStringToLongLong( py_input, "monitor", backlog.n_input );
      iPyVGXBuilder.DictMapStringToInt( py_state, "running", backlog.flags.is_running );
      iPyVGXBuilder.DictMapStringToInt( py_state, "paused", backlog.flags.is_paused );

    }

    iPyVGXBuilder.DictMapStringToPyObject( py_info, "input", &py_input );
    iPyVGXBuilder.DictMapStringToPyObject( py_info, "state", &py_state );

    iPyVGXBuilder.DictMapStringToPyObject( py_long, "partial", &py_long_map );
    iPyVGXBuilder.DictMapStringToPyObject( py_info, "long", &py_long );

    iPyVGXBuilder.DictMapStringToPyObject( py_medium, "partial", &py_medium_map );
    iPyVGXBuilder.DictMapStringToPyObject( py_info, "medium", &py_medium );

    iPyVGXBuilder.DictMapStringToPyObject( py_short, "partial", &py_short_map );
    iPyVGXBuilder.DictMapStringToPyObject( py_info, "short", &py_short );

    iPyVGXBuilder.DictMapStringToPyObject( py_executor, "partial", &py_executor_map );
    iPyVGXBuilder.DictMapStringToPyObject( py_info, "executor", &py_executor );

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_info );
    PyVGX_XDECREF( py_state );
    PyVGX_XDECREF( py_input );
    PyVGX_XDECREF( py_long );
    PyVGX_XDECREF( py_long_map );
    PyVGX_XDECREF( py_medium );
    PyVGX_XDECREF( py_medium_map );
    PyVGX_XDECREF( py_short );
    PyVGX_XDECREF( py_short_map );
    PyVGX_XDECREF( py_executor );
    PyVGX_XDECREF( py_executor_map );
  }
  XFINALLY {
  }

  return py_info;
}



/******************************************************************************
 * PyVGX_Graph__SetGraphReadonly
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetGraphReadonly__doc__,
  "SetGraphReadonly( timeout=1000, force=False ) -> None\n"
  "\n"
  "Make the graph readonly. The optional timeout (in milliseconds) allows blocking\n"
  "while waiting for the graph to become idle. The default is nonblocking.\n"
  "No vertices can be acquired writable when a graph is readonly.\n"
  "\n"
  "The internal event processor (including TTL) is suspended when a graph is readonly.\n"
  "Vertices and arcs that expire while a graph is readonly will not be removed until\n"
  "the graph becomes writable again.\n"
  "\n"
  "Queries are more efficient in readonly mode. Graphs that are not updated frequently\n"
  "should be set readonly whenever possible to optimize query performance.\n"
  "\n"
  "NOTE: Repeated calls to this method will acquire the graph readonly recursively.\n"
  "An equal number of calls to ClearGraphReadonly() will be required to make the\n"
  "graph writable again.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__SetGraphReadonly
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__SetGraphReadonly( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  static __THREAD objectid_t _ftoken = {0};
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "timeout", "force", "forcetoken", NULL };

  int timeout_ms = 1000;
  int force = 0;
  const char *forcetoken = NULL;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|iis", kwlist, &timeout_ms, &force, &forcetoken ) ) {
    return NULL;
  }

  if( force ) {
    if( idnone( &_ftoken ) ) {
      _ftoken.L = ihash64( __GET_CURRENT_NANOSECOND_TICK() );
      _ftoken.H = ihash64( _ftoken.L ^ rand64() );
    }
    objectid_t f = strtoid( forcetoken ? forcetoken : "" );
    if( !idmatch( &f, &_ftoken) ) {
      char idbuf[33];
      PyErr_Format( PyExc_PermissionError, "Supply forcetoken %s to indicate intent to force readonly", idtostr( idbuf, &_ftoken ) );
      return NULL;
    }
    idunset( &_ftoken );
  }

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  int ret;
  CString_t *CSTR__writable = NULL;

  int64_t nw = 0;
  BEGIN_PYVGX_THREADS {
    if( (ret = CALLABLE( graph )->advanced->AcquireGraphReadonly( graph, timeout_ms, force, &reason )) < 0 ) {
      CSTR__writable = CALLABLE( graph )->advanced->GetWritableVerticesAsCString( graph, &nw );
    }
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    if( CSTR__writable ) {
      PyErr_Format( PyVGX_OperationTimeout, "Current thread holds %lld writable vertices: %s", nw, CStringValue( CSTR__writable ) );
      CStringDelete( CSTR__writable );
    }
    else {
      PyVGXError_SetString( PyVGX_OperationTimeout, "Unable to set graph readonly mode" );
    }
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Graph__IsGraphReadonly
 *
 ******************************************************************************
 */
PyDoc_STRVAR( IsGraphReadonly__doc__,
  "IsGraphReadonly() -> bool\n"
  "\n"
  "Return True if the graph is currently readonly, otherwise False.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__IsGraphReadonly
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__IsGraphReadonly( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int is_RO;
  BEGIN_PYVGX_THREADS {
    is_RO = CALLABLE( graph )->advanced->IsGraphReadonly( graph );
  } END_PYVGX_THREADS;

  if( is_RO ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Graph__ClearGraphReadonly
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ClearGraphReadonly__doc__,
  "ClearGraphReadonly() -> None\n"
  "\n"
  "Make a readonly graph writable.\n"
  "\n"
  "NOTE: If SetGraphReadonly() was called multiple times it is necessary\n"
  "to call ClearGraphReadonly() an equal number of times to exit readonly\n"
  "\nmode."
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__ClearGraphReadonly
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__ClearGraphReadonly( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  int released_one;
  BEGIN_PYVGX_THREADS {
    released_one = CALLABLE( graph )->advanced->ReleaseGraphReadonly( graph );
  } END_PYVGX_THREADS;

  if( released_one == 0 ) {
  }
  else if( released_one < 0 ) {
    PyVGXError_SetString( PyVGX_OperationTimeout, "Could not clear readonly state (thread now owner?)" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__IsGraphLocal
 *
 ******************************************************************************
 */
PyDoc_STRVAR( IsGraphLocal__doc__,
  "IsGraphLocal() -> bool\n"
  "\n"
  "Return True if the graph is local only, otherwise False.\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__IsGraphLocal
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__IsGraphLocal( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  bool local;
  BEGIN_PYVGX_THREADS {
    local = _vgx_graph_is_local_only_OPEN( graph );
  } END_PYVGX_THREADS;

  if( local ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Graph__DebugDumpGraph
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugDumpGraph__doc__,
  "DebugDumpGraph() -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__DebugDumpGraph
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DebugDumpGraph( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
 
  CALLABLE( graph )->Dump( graph );

  Py_RETURN_NONE;

}



/******************************************************************************
 * PyVGX_Graph__DebugPrintAllocators
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugPrintAllocators__doc__,
  "DebugPrintAllocators( [name] ) -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__DebugPrintAllocators
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DebugPrintAllocators( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "name", NULL };

  const char *alloc_name = NULL;
  int64_t sz_alloc_name = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s#", kwlist, &alloc_name, &sz_alloc_name ) ) {
    return NULL;
  }


  CALLABLE( graph )->advanced->DebugPrintAllocators( graph, alloc_name );

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Graph__DebugCheckAllocators
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugCheckAllocators__doc__,
  "DebugCheckAllocators( [name] ) -> None\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__DebugCheckAllocators
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DebugCheckAllocators( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static char *kwlist[] = { "name", NULL };

  const char *alloc_name = NULL;
  int64_t sz_alloc_name = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s#", kwlist, &alloc_name, &sz_alloc_name ) ) {
    return NULL;
  }

  int err = 0;
  BEGIN_PYVGX_THREADS {
    err = CALLABLE( graph )->advanced->DebugCheckAllocators( graph, alloc_name );
  } END_PYVGX_THREADS;

  if( err < 0 ) {
    PyVGXError_SetString( PyExc_Exception, "ALLOCATOR CORRUPTION!" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Graph__DebugGetObjectByAddress
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugGetObjectByAddress__doc__,
  "DebugGetObjectByAddress( address ) -> object_description\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__DebugGetObjectByAddress
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DebugGetObjectByAddress( PyVGX_Graph *pygraph, PyObject *py_address ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
 
  uint64_t address = 0;

  if( PyLong_Check( py_address ) ) {
    address = PyLong_AsUnsignedLongLong( py_address );
  }
  else {
    PyErr_SetString( PyExc_ValueError, "address must be integer" );
    return NULL;
  }

  comlib_object_t *obj;
  BEGIN_PYVGX_THREADS {
    obj = CALLABLE( graph )->advanced->DebugGetObjectAtAddress( graph, address );
  } END_PYVGX_THREADS;
  
  if( obj ) {
    PyObject *py_repr = __get_object_repr( obj );
    return py_repr;
  }
  else {
    PyErr_SetString( PyExc_KeyError, "Invalid address" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Graph__DebugFindObjectByObid
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugFindObjectByIdentifier__doc__,
  "DebugFindObjectByIdentifier( identifier ) -> object_description\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__DebugFindObjectByIdentifier
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__DebugFindObjectByIdentifier( PyVGX_Graph *pygraph, PyObject *py_identifier ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }
 
  if( !PyVGX_PyObject_CheckString( py_identifier ) ) {
    PyErr_SetString( PyExc_ValueError, "identifier must be a string or bytes-like object" );
    return NULL;
  }

  const char *identifier = PyVGX_PyObject_AsString( py_identifier );
  
  comlib_object_t *obj = CALLABLE( graph )->advanced->DebugFindObjectByIdentifier( graph, identifier );
  if( obj ) {
    PyObject *py_repr = __get_object_repr( obj );
    return py_repr;
  }
  else {
    PyErr_SetString( PyExc_KeyError, "object not found" );
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_Graph__GetVertexID
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetVertexID__doc__,
  "GetVertexID( [offset] ) -> string\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__GetVertexID
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__GetVertexID( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "offset",  NULL };

  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  // Default: random
  PyObject *py_offset = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|O", kwlist, &py_offset ) ) {
    return NULL;
  }

  int64_t *poffset = NULL;
  int64_t offset = 0;
  if( py_offset && PyLong_Check( py_offset ) ) {
    offset = PyLong_AsLongLong( py_offset );
    poffset = &offset;
  }

  CString_t *CSTR__id = CALLABLE( graph )->advanced->GetVertexIDByOffset( graph, poffset );
 
  if( CSTR__id ) {
    PyObject *py_id = PyUnicode_FromStringAndSize( CStringValue( CSTR__id ), CStringLength( CSTR__id ) );
    iString.Discard( &CSTR__id );
    return py_id;
  }
  else {
    PyErr_SetString( PyExc_IndexError, "Vertex allocator offset out of range" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Graph__Lock
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Lock__doc__,
  "Lock( [id[, linger[, timeout]]]  ) -> lock_vertex\n"
  "\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Lock
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Lock( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  static __THREAD uint64_t auto_seed = 0;
  if( auto_seed == 0 ) {
    auto_seed = ihash64( __GET_CURRENT_NANOSECOND_TICK() + ihash64( GET_CURRENT_THREAD_ID() ) );
  }

  PyObject *py_lock = NULL;
  // Args
  static char *kwlist[] = { "id", "linger", "timeout", NULL };
  const char *lock_id = NULL;
  Py_ssize_t sz_lock_id = 0;
  PyObject *py_linger = NULL;
  PyObject *py_timeout_ms = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|z#O!O", kwlist, &lock_id, &sz_lock_id, &PyLong_Type, &py_linger, &py_timeout_ms ) ) {
    return NULL;
  }

  // Lock ID
  PyObject *py_lock_id;
  if( lock_id == NULL ) {
    // Auto lock id
    uint64_t tick_hash = ihash64( __GET_CURRENT_NANOSECOND_TICK() ^ auto_seed );
    uint64_t auto_hash = ihash64( auto_seed ^ tick_hash );
    objectid_t auto_obid = {
      .H = tick_hash,
      .L = auto_hash,
    };
    auto_seed = auto_hash;
    char auto_lock_id[33];
    py_lock_id = PyBytes_FromString( idtostr( auto_lock_id, &auto_obid ) );
  }
  else {
    const char *gname = CStringValue( CALLABLE( graph )->Name( graph ) );
    py_lock_id = PyBytes_FromFormat( "lock_::%s::%s", gname, lock_id );
  }

  // Lock type
  const CString_t *CSTR__lock_type = iEnumerator_OPEN.VertexType.Reserved.LockObject( graph );
  PyObject *py_lock_type = PyUnicode_FromStringAndSize( CStringValue( CSTR__lock_type ), CStringLength( CSTR__lock_type ) );

  PyObject *vcargs[] = {
    NULL,               // -1
    (PyObject*)pygraph, // graph
    py_lock_id,         // id
    py_lock_type,       // type
    g_py_char_w,        // mode
    py_linger ? py_linger : g_py_zero, // lifespan
    py_timeout_ms       // timeout
  };

  py_lock = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vertex__VertexType, vcargs+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  Py_XDECREF( py_lock_id );
  Py_XDECREF( py_lock_type );

  // Error
  if( py_lock == NULL ) {
    return NULL;
  }

  // This is required to trigger the necessary internals for freezing the emitter until lock is released
  uint64_t gen = 0;
  int err = 0;
  CString_t *CSTR__error = NULL;
  BEGIN_PYVGX_THREADS {
    vgx_VertexList_t *vertices = iVertex.List.New( 1 );
    if( vertices ) {
      vgx_Vertex_t *vertex = ((PyVGX_Vertex*)py_lock)->vertex;
      // Capture lock operation
      iVertex.List.Set( vertices, 0, vertex );
      GRAPH_LOCK( graph ) {
        if( iOperation.Lock_CS.AcquireWL( graph, vertices, &CSTR__error ) < 0 ) {
          err = -1;
        }
      } GRAPH_RELEASE;
      gen = _get_pyvertex_generation_guard();
      iVertex.List.Delete( &vertices );
    }
    else {
      err = -1;
    }
  } END_PYVGX_THREADS;

  // Error
  if( err < 0 ) {
    Py_DECREF( py_lock );
    py_lock = NULL;
    if( !PyErr_Occurred() ) {
      if( CSTR__error ) {
        PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
      }
      else {
        PyErr_SetString( PyExc_Exception, "internal error" );
      }
    }
  }
  else {
    // Set generation guard
    ((PyVGX_Vertex*)py_lock)->gen_guard = gen;
  }

  iString.Discard( &CSTR__error );

  return py_lock;
}



/******************************************************************************
 * PyVGX_Graph__Synchronized
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Synchronized__doc__,
  "Synchronized( function, *args, **kwds )\n"
  "\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_Graph__Synchronized
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__Synchronized( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  static PyObject *py_lock_args = NULL;
  if( py_lock_args == NULL ) {
    if( (py_lock_args = PyTuple_New( 3 )) == NULL ) {
      return NULL;
    }
    PyObject *py_mutex_id = PyBytes_FromStringAndSize( "_SYN", 4 );
    PyObject *py_linger = PyLong_FromLong( -1 ); // infinite lifespan
    PyObject *py_timeout = PyLong_FromLong( 5000 ); // 5 sec timeout
    if( py_mutex_id == NULL || py_linger == NULL || py_timeout == NULL ) {
      return NULL;
    }
    PyTuple_SET_ITEM( py_lock_args, 0, py_mutex_id );
    PyTuple_SET_ITEM( py_lock_args, 1, py_linger );
    PyTuple_SET_ITEM( py_lock_args, 2, py_timeout );
  }

  // Args
  PyObject *py_callable = NULL;
  int64_t sz_args = 0;
  if( !args || !PyTuple_Check( args ) || (sz_args = PyTuple_GET_SIZE( args )) < 1 || !PyCallable_Check( (py_callable = PyTuple_GET_ITEM( args, 0 )) ) ) {
    PyErr_SetString( PyExc_ValueError, "First argument must be a callable object" );
    return NULL;
  }

  PyObject *py_ret = NULL;

  // New args to be supplied to the synchronized function
  PyObject *syn_args = PyTuple_GetSlice( args, 1, sz_args );
  if( syn_args ) {
    // Acquire mutex
    Py_INCREF( py_lock_args );
    PyVGX_Vertex *py_lock = (PyVGX_Vertex*)PyVGX_Graph__Lock( pygraph, py_lock_args, NULL );
    if( py_lock ) {

      // Call function to be synchronized
      py_ret = PyObject_Call( py_callable, syn_args, kwds );

      // Release mutex
      PyVGX_Graph__CloseVertex( pygraph, py_lock );
    }
    Py_DECREF( py_lock_args );
    Py_DECREF( syn_args );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Graph__repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Graph__repr( PyVGX_Graph *pygraph ) {
  static const char *readonly = " (READONLY)";
  static const char *writable = "";

  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return NULL;
  }

  vgx_Graph_vtable_t *igraph = CALLABLE(graph);

  int64_t order;
  int64_t size;
  const char *name;
  const char *ro;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( graph ) {
      order = GraphOrder( graph );
      size = GraphSize( graph );
      name = CStringValue( igraph->Name( graph ) );
      if( igraph->advanced->IsGraphReadonly( graph ) ) {
        ro = readonly;
      }
      else {
        ro = writable;
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  return PyUnicode_FromFormat( "<PyVGX_Graph: name=%s order=%lld size=%lld%s>", name, order, size, ro );
}



/******************************************************************************
 * PyVGX_Graph__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Graph__dealloc( PyVGX_Graph *pygraph ) {
  pygraph->caller_is_dealloc = true;
  PyObject *py_ret = PyVGX_Graph__Close( pygraph );
  if( py_ret ) {
    Py_DECREF( py_ret );
  }
  // Already closed or no permission to close
  else if( pygraph->constructor_complete ) {
    if( PyErr_Occurred() ) {
      PyErr_Clear();
    }
  }

  // Discard similarity object
  Py_XDECREF( pygraph->py_sim );
  // Discard name
  Py_XDECREF( pygraph->py_name );
  // Free python graph
  Py_TYPE( pygraph )->tp_free( pygraph );
}



/******************************************************************************
 * PyVGX_Graph__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Graph__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_Graph *pygraph;

  pygraph = (PyVGX_Graph*)type->tp_alloc(type, 0);
  pygraph->graph = NULL;
  pygraph->py_sim = NULL;
  pygraph->py_name = NULL;
  pygraph->is_owner = false;
  pygraph->constructor_complete = true;
  pygraph->caller_is_dealloc = false;

  return (PyObject *)pygraph;
}



/**************************************************************************//**
 * __remove_graph_reference
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __remove_graph_reference( vgx_Graph_t *graph, void *pygraph ) {
  if( pygraph ) {
    ((PyVGX_Graph*)pygraph)->graph = NULL;
  }
}



/******************************************************************************
 * PyVGX_Graph__init
 *
 ******************************************************************************
 */
static int PyVGX_Graph__init( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"name", "path", "local", "timeout", NULL};
  char *name = NULL;
  int64_t sz_name = 0;
  char *path = NULL;
  int64_t sz_path = 0;
  int local = 0;
  int timeout = 0;

  char buffer[100];
  snprintf( buffer, 99, "[PyVGX_Graph @ %p]", pygraph );
  pygraph->constructor_complete = false;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "s#|s#ii", kwlist, &name, &sz_name, &path, &sz_path, &local, &timeout ) ) {
    return -1;
  }

  if( pygraph->graph ) {
    PyErr_Format( PyExc_RuntimeError, "Graph instance is already initialized" );
    return -1;
  }

  CString_t *CSTR__name = NULL;
  CString_t *CSTR__path = NULL;

  int ret = 0;
  vgx_Graph_t *graph = NULL;

  vgx_StringTupleList_t *messages = NULL;

  // Check if initialized
  if( igraphfactory.IsInitialized() == false ) {
    if( pyvgx_System_Initialize_Default() < 0 ) {
      return -1;
    }
  }

  if( (pygraph->py_name = PyUnicode_FromString( name )) == NULL ) {
    return -1;
  }

  BEGIN_PYVGX_THREADS {
    XTRY {
      // System Graph
      if( path && CharsEqualsConst( path, "_system" ) && name && CharsEqualsConst( name, "system" ) ) {
        // Python can't own the system graph. We don't go via the registry.
        graph = iSystem.GetSystemGraph();
      }
      // User Graph
      else {
        if( (CSTR__name = CStringNew( name )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x322 );
        }

        if( path ) {
          if( (CSTR__path = CStringNew( path )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x323 );
          }
        }

        // Open graph
        bool local_only = local > 0 ? true : false;
        if( (graph = igraphfactory.OpenGraph( CSTR__path, CSTR__name, local_only, &messages, timeout )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x324 );
        }

        // Set destructor hook
        CALLABLE( graph )->SetDestructorHook( graph, pygraph, __remove_graph_reference );

        // Enable shutdown hook
        _module_owns_registry = true;
      }
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        if( messages && !PyErr_Occurred() ) {
          iPyVGXBuilder.SetErrorFromMessages( messages );
        }
        PyVGX_SetPyErr( errcode );
      } END_PYTHON_INTERPRETER;
      // Close graph
      if( graph ) {
        uint32_t owner;
        igraphfactory.CloseGraph( &graph, &owner );
      }
      ret = -1;
    }
    XFINALLY {
      iString.Discard( &CSTR__name );
      iString.Discard( &CSTR__path );
      _vgx_delete_string_tuple_list( &messages );
    }
  } END_PYVGX_THREADS;

  if( ret == 0 ) {
    pygraph->graph = graph;
    pygraph->is_owner = true;
    _registry_loaded = true;

    if( (pygraph->py_sim = (PyVGX_Similarity*)__PyVGX_Graph__sim( pygraph, NULL )) == NULL ) {
      ret = -1;
    }
    else if( !iSystem.IsSystemGraph( pygraph->graph ) ) {
      // Enter this object instance into the open graph register
      __PyVGX_Graph__OGR_add( pygraph );
    }
    pygraph->constructor_complete = true;
  }


  return ret;
}



/******************************************************************************
 * PyVGX_Graph__new_from_capsule
 *
 ******************************************************************************
 */
PyDoc_STRVAR( __new_from_capsule__doc__,
  "*** for internal use only ***\n"
  "\n"
);
static PyObject * PyVGX_Graph__new_from_capsule( PyTypeObject *cls,
    PyObject *args ) {

  PyObject *graph_capsule = NULL;

  if ( !PyArg_ParseTuple( args, "O", &graph_capsule ) ) {
    return NULL;
  }

  if ( !PyCapsule_CheckExact( graph_capsule ) ) {
    PyVGXError_SetString( PyExc_ValueError, "Argument is not a capsule" );
    return NULL;
  }

  vgx_Graph_t* graph = (vgx_Graph_t*)PyCapsule_GetPointer( graph_capsule, NULL );

  if ( !graph ) {
    PyErr_Format( PyExc_ValueError, "Capsule contains a null pointer" );
    return NULL;
  }

  if( !COMLIB_OBJECT_ISINSTANCE( graph, vgx_Graph_t ) ) {
    PyVGXError_SetString( PyExc_ValueError, "Capsule does not contain a graph object" );
    return NULL;
  }

  PyVGX_Graph *pygraph = (PyVGX_Graph*)cls->tp_new( cls, NULL, NULL );

  if ( !pygraph ) {
    PyVGXError_SetString( PyExc_RuntimeError, "Could not allocate memory for PyVGX graph" );
    return NULL;
  }

  pygraph->graph = graph;
  pygraph->is_owner = false;
  _registry_loaded = true;
  if( (pygraph->py_sim = (PyVGX_Similarity*)__PyVGX_Graph__sim( pygraph, NULL )) == NULL ) {
    PyVGX_DECREF( pygraph );
    pygraph = NULL;
  }

  // Set destructor hook
  if( pygraph ) {
    CALLABLE( pygraph->graph )->SetDestructorHook( pygraph->graph, pygraph, __remove_graph_reference );
  }

  return (PyObject*)pygraph;
}



/******************************************************************************
 * sq_contains_PyVGX_Graph
 *
 ******************************************************************************
 */
static int sq_contains_PyVGX_Graph( PyVGX_Graph *pygraph, PyObject *py_vertex_name ) {
  int exists = 0;
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return -1;
  }
  const char *vertex_id = NULL;
  Py_ssize_t sz = 0;
  Py_ssize_t ucsz = 0;
  QWORD address = 0;

  if( PyLong_CheckExact( py_vertex_name ) ) {
    address = PyLong_AsLongLong( py_vertex_name );
  }
  else if( (vertex_id = PyVGX_PyObject_AsUTF8AndSize( py_vertex_name, &sz, &ucsz, NULL )) == NULL ) {
    if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_TypeError, "Vertex name must be a string or bytes-like object" );
    }
    return -1;
  }

  BEGIN_PYVGX_THREADS {
    objectid_t obid = {0};
    if( address ) {
      vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
      CALLABLE( graph )->advanced->GetVertexInternalidByAddress( graph, address, &obid, &reason );
    }
    else if( vertex_id ) {
      obid = smartstrtoid( vertex_id, (int)sz );
    }

    if( !idnone( &obid ) ) {
      exists = CALLABLE( graph )->advanced->HasVertex( graph, NULL, &obid );
    }

  } END_PYVGX_THREADS;


  return exists;
}



/******************************************************************************
 * py_sq_contains_PyVGX_Graph
 *
 ******************************************************************************
 */
static PyObject * py_sq_contains_PyVGX_Graph( PyVGX_Graph *pygraph, PyObject *py_vertex_name ) {
  int c = sq_contains_PyVGX_Graph( pygraph, py_vertex_name );
  if( c == 0 ) {
    Py_RETURN_FALSE;
  }
  else if( c > 0 ) {
    Py_RETURN_TRUE;
  }
  else {
    return NULL;
  }
}



/******************************************************************************
 * mp_length_PyVGX_Graph
 *
 ******************************************************************************
 */
static Py_ssize_t mp_length_PyVGX_Graph( PyVGX_Graph *pygraph ) {
  vgx_Graph_t *graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph );
  if( !graph ) {
    return -1;
  }
  return GraphOrder( graph );
}



/******************************************************************************
 * mp_subscript_PyVGX_Graph
 *
 ******************************************************************************
 */
static PyObject * mp_subscript_PyVGX_Graph( PyVGX_Graph *pygraph, PyObject *py_vertex_name ) {
  return PyVGX_Graph__GetVertex( pygraph, py_vertex_name );
}



/******************************************************************************
 * PyVGX_Graph__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Graph__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
/* Hack to implement "key in table" - stolen from dictobject.c */
static PySequenceMethods tp_as_sequence_PyVGX_Graph = {
    .sq_length          = 0,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)sq_contains_PyVGX_Graph,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0
};



/******************************************************************************
* PyVGX_Graph__getset
*
******************************************************************************
*/
static PyGetSetDef PyVGX_Graph__getset[] = {
  {"name",      (getter)__PyVGX_Graph__get_name,           (setter)NULL,                                       "name",     NULL },
  {"path",      (getter)__PyVGX_Graph__get_path,           (setter)NULL,                                       "path",     NULL },
  {"size",      (getter)__PyVGX_Graph__get_size,           (setter)NULL,                                       "size",     NULL },
  {"order",     (getter)__PyVGX_Graph__get_order,          (setter)NULL,                                       "order",    NULL },
  {"objcnt",    (getter)__PyVGX_Graph__get_objcnt,         (setter)NULL,                                       "objcnt",   NULL },
  {"ts",        (getter)__PyVGX_Graph__get_ts,             (setter)NULL,                                       "ts",       NULL },
  {"sim",       (getter)__PyVGX_Graph__sim,                (setter)NULL,                                       "sim",      NULL },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * tp_as_mapping_PyVGX_Graph
 *
 ******************************************************************************
 */
static PyMappingMethods tp_as_mapping_PyVGX_Graph = {
  .mp_length        = (lenfunc)mp_length_PyVGX_Graph,
  .mp_subscript     = (binaryfunc)mp_subscript_PyVGX_Graph,
  .mp_ass_subscript = (objobjargproc)NULL
};



/******************************************************************************
 * PyVGX_Graph__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Graph__methods[] = {
    // Coexist methods
    {"__contains__",          (PyCFunction)py_sq_contains_PyVGX_Graph,          METH_O       | METH_COEXIST,  NULL },
    {"__getitem__",           (PyCFunction)mp_subscript_PyVGX_Graph,            METH_O       | METH_COEXIST,  NULL },

    // Special hidden methods not to be used by Python interpreter
    {"__new_from_capsule__",  (PyCFunction)PyVGX_Graph__new_from_capsule,       METH_VARARGS | METH_CLASS,    __new_from_capsule__doc__ },

    // SPECIAL METHODS
    {"Define",                (PyCFunction)PyVGX_Graph__Define,                 METH_O                      , Define__doc__ },
    {"Evaluate",              (PyCFunction)PyVGX_Graph__Evaluate,               METH_VARARGS | METH_KEYWORDS, Evaluate__doc__ },
    {"Memory",                (PyCFunction)PyVGX_Graph__Memory,                 METH_O                      , Memory__doc__ },
    {"GetDefinition",         (PyCFunction)PyVGX_Graph__GetDefinition,          METH_O                      , GetDefinition__doc__ },
    {"GetDefinitions",        (PyCFunction)PyVGX_Graph__GetDefinitions,         METH_NOARGS                 , GetDefinitions__doc__ },

    // VERTEX METHODS
    {"CreateVertex",          (PyCFunction)PyVGX_Graph__CreateVertex,           METH_VARARGS | METH_KEYWORDS, CreateVertex__doc__ },
    {"NewVertex",             (PyCFunction)PyVGX_Graph__NewVertex,              METH_VARARGS | METH_KEYWORDS, NewVertex__doc__ },
    {"DeleteVertex",          (PyCFunction)PyVGX_Graph__DeleteVertex,           METH_VARARGS | METH_KEYWORDS, DeleteVertex__doc__ },
    {"OpenVertex",            (PyCFunction)PyVGX_Graph__OpenVertex,             METH_VARARGS | METH_KEYWORDS, OpenVertex__doc__  },
    {"CloseVertex",           (PyCFunction)PyVGX_Graph__CloseVertex,            METH_O,                       CloseVertex__doc__  },
    {"CloseAll",              (PyCFunction)PyVGX_Graph__CloseAll,               METH_NOARGS,                  CloseAll__doc__  },
    {"CommitAll",             (PyCFunction)PyVGX_Graph__CommitAll,              METH_NOARGS,                  CommitAll__doc__  },
    {"EscalateVertex",        (PyCFunction)PyVGX_Graph__EscalateVertex,         METH_VARARGS | METH_KEYWORDS, EscalateVertex__doc__  },
    {"RelaxVertex",           (PyCFunction)PyVGX_Graph__RelaxVertex,            METH_O,                       RelaxVertex__doc__  },

    {"OpenVertices",          (PyCFunction)PyVGX_Graph__OpenVertices,           METH_VARARGS | METH_KEYWORDS, OpenVertices__doc__  },
    {"CloseVertices",         (PyCFunction)PyVGX_Graph__CloseVertices,          METH_O,                       CloseVertices__doc__  },

    {"GetOpenVertices",       (PyCFunction)PyVGX_Graph__GetOpenVertices,        METH_VARARGS | METH_KEYWORDS, GetOpenVertices__doc__  },

    // ARC METHODS
    {"Connect",               (PyCFunction)PyVGX_Graph__Connect,                METH_VARARGS | METH_KEYWORDS, Connect__doc__ },
    {"Disconnect",            (PyCFunction)PyVGX_Graph__Disconnect,             METH_VARARGS | METH_KEYWORDS, Disconnect__doc__ },
    {"Count",                 (PyCFunction)PyVGX_Graph__Count,                  METH_VARARGS | METH_KEYWORDS, Count__doc__  },
    {"Accumulate",            (PyCFunction)PyVGX_Graph__Accumulate,             METH_VARARGS | METH_KEYWORDS, Accumulate__doc__  },

    // QUERY METHODS
    {"Arcs",                  (PyCFunction)PyVGX_Graph__Arcs,                   METH_VARARGS | METH_KEYWORDS, pyvgx_Arcs__doc__ },
    {"NewArcsQuery",          (PyCFunction)PyVGX_Graph__NewArcsQuery,           METH_VARARGS | METH_KEYWORDS, pyvgx_Arcs__doc__ },
    {"Vertices",              (PyCFunction)PyVGX_Graph__Vertices,               METH_VARARGS | METH_KEYWORDS, pyvgx_Vertices__doc__ },
    {"NewVerticesQuery",      (PyCFunction)PyVGX_Graph__NewVerticesQuery,       METH_VARARGS | METH_KEYWORDS, pyvgx_Vertices__doc__ },
    {"VerticesType",          (PyCFunction)PyVGX_Graph__VerticesType,           METH_O,                       VerticesType__doc__ },
    {"HasVertex",             (PyCFunction)PyVGX_Graph__HasVertex,              METH_O,                       HasVertex__doc__  },
    {"GetVertex",             (PyCFunction)PyVGX_Graph__GetVertex,              METH_O,                       GetVertex__doc__  },
    {"VertexIdByAddress",     (PyCFunction)PyVGX_Graph__VertexIdByAddress,      METH_O,                       VertexIdByAddress__doc__  },
    {"Neighborhood",          (PyCFunction)PyVGX_Graph__Neighborhood,           METH_VARARGS | METH_KEYWORDS, pyvgx_Neighborhood__doc__ },
    {"NewNeighborhoodQuery",  (PyCFunction)PyVGX_Graph__NewNeighborhoodQuery,   METH_VARARGS | METH_KEYWORDS, pyvgx_Neighborhood__doc__ },
    {"Adjacent" ,             (PyCFunction)PyVGX_Graph__Adjacent,               METH_VARARGS | METH_KEYWORDS, pyvgx_Adjacent__doc__ },
    {"NewAdjacencyQuery",     (PyCFunction)PyVGX_Graph__NewAdjacencyQuery,      METH_VARARGS | METH_KEYWORDS, pyvgx_Adjacent__doc__ },
    {"OpenNeighbor" ,         (PyCFunction)pyvgx_OpenNeighbor,                  METH_FASTCALL | METH_KEYWORDS, pyvgx_OpenNeighbor__doc__ },
    {"Aggregate",             (PyCFunction)PyVGX_Graph__Aggregate,              METH_VARARGS | METH_KEYWORDS, pyvgx_Aggregate__doc__ },
    {"NewAggregatorQuery",    (PyCFunction)PyVGX_Graph__NewAggregatorQuery,     METH_VARARGS | METH_KEYWORDS, pyvgx_Aggregate__doc__ },
    {"ArcValue",              (PyCFunction)pyvgx_ArcValue,                      METH_FASTCALL | METH_KEYWORDS, pyvgx_ArcValue__doc__ },
    {"Degree",                (PyCFunction)pyvgx_Degree,                        METH_FASTCALL | METH_KEYWORDS, pyvgx_Degree__doc__ },
    {"Inarcs",                (PyCFunction)PyVGX_Graph__Inarcs,                 METH_VARARGS | METH_KEYWORDS, pyvgx_Inarcs__doc__ },
    {"Outarcs",               (PyCFunction)PyVGX_Graph__Outarcs,                METH_VARARGS | METH_KEYWORDS, pyvgx_Outarcs__doc__ },
    {"Initials",              (PyCFunction)PyVGX_Graph__Initials,               METH_VARARGS | METH_KEYWORDS, pyvgx_Initials__doc__ },
    {"Terminals",             (PyCFunction)PyVGX_Graph__Terminals,              METH_VARARGS | METH_KEYWORDS, pyvgx_Terminals__doc__ },
    {"Search",                (PyCFunction)PyVGX_Graph__Search,                 METH_VARARGS | METH_KEYWORDS, Search__doc__ },

    // MANAGEMENT METHODS
    {"Order",                 (PyCFunction)PyVGX_Graph__Order,                  METH_FASTCALL,                Order__doc__ },
    {"Size",                  (PyCFunction)PyVGX_Graph__Size,                   METH_NOARGS,                  Size__doc__ },
    {"Save",                  (PyCFunction)PyVGX_Graph__Save,                   METH_VARARGS | METH_KEYWORDS, Save__doc__  },
    {"Sync",                  (PyCFunction)PyVGX_Graph__Sync,                   METH_VARARGS | METH_KEYWORDS, Sync__doc__  },
    {"Truncate",              (PyCFunction)PyVGX_Graph__Truncate,               METH_VARARGS,                 Truncate__doc__ },
    {"ResetSerial",           (PyCFunction)PyVGX_Graph__ResetSerial,            METH_VARARGS,                 ResetSerial__doc__ },
    {"SetGraphReadonly",      (PyCFunction)PyVGX_Graph__SetGraphReadonly,       METH_VARARGS | METH_KEYWORDS, SetGraphReadonly__doc__  },
    {"IsGraphReadonly",       (PyCFunction)PyVGX_Graph__IsGraphReadonly,        METH_NOARGS,                  IsGraphReadonly__doc__  },
    {"ClearGraphReadonly",    (PyCFunction)PyVGX_Graph__ClearGraphReadonly,     METH_NOARGS,                  ClearGraphReadonly__doc__  },
    {"IsGraphLocal",          (PyCFunction)PyVGX_Graph__IsGraphLocal,           METH_NOARGS,                  IsGraphLocal__doc__  },
    {"GetMemoryUsage",        (PyCFunction)PyVGX_Graph__GetMemoryUsage,         METH_VARARGS,                 GetMemoryUsage__doc__ },
    {"Status",                (PyCFunction)PyVGX_Graph__Status,                 METH_VARARGS,                 Status__doc__ },
    {"ResetCounters",         (PyCFunction)PyVGX_Graph__ResetCounters,          METH_NOARGS,                  ResetCounters__doc__ },
    {"Close",                 (PyCFunction)PyVGX_Graph__Close,                  METH_NOARGS,                  Close__doc__ },
    {"Erase",                 (PyCFunction)PyVGX_Graph__Erase,                  METH_NOARGS,                  Erase__doc__ },

    // ENUMERATION METHODS
    {"EnumRelationship",      (PyCFunction)PyVGX_Graph__EnumRelationship,       METH_VARARGS | METH_KEYWORDS, EnumRelationship__doc__ },
    {"EnumVertexType",        (PyCFunction)PyVGX_Graph__EnumVertexType,         METH_VARARGS | METH_KEYWORDS, EnumVertexType__doc__ },
    {"EnumDimension",         (PyCFunction)PyVGX_Graph__EnumDimension,          METH_VARARGS | METH_KEYWORDS, EnumDimension__doc__ },
    {"EnumKey",               (PyCFunction)PyVGX_Graph__EnumKey,                METH_VARARGS | METH_KEYWORDS, EnumKey__doc__ },
    {"EnumValue",             (PyCFunction)PyVGX_Graph__EnumValue,              METH_VARARGS | METH_KEYWORDS, EnumValue__doc__ },
    {"Relationship",          (PyCFunction)PyVGX_Graph__Relationship,           METH_O,                       Relationship__doc__  },
    {"VertexType",            (PyCFunction)PyVGX_Graph__VertexType,             METH_O,                       VertexType__doc__  },
    {"Dimension",             (PyCFunction)PyVGX_Graph__Dimension,              METH_O,                       Dimension__doc__  },
    {"Key",                   (PyCFunction)PyVGX_Graph__Key,                    METH_O,                       Key__doc__  },
    {"Value",                 (PyCFunction)PyVGX_Graph__Value,                  METH_O,                       Value__doc__  },
    {"Relationships",         (PyCFunction)PyVGX_Graph__Relationships,          METH_NOARGS,                  Relationships__doc__ },
    {"VertexTypes",           (PyCFunction)PyVGX_Graph__VertexTypes,            METH_NOARGS,                  VertexTypes__doc__ },
    {"PropertyKeys",          (PyCFunction)PyVGX_Graph__PropertyKeys,           METH_NOARGS,                  PropertyKeys__doc__ },
    {"PropertyStringValues",  (PyCFunction)PyVGX_Graph__PropertyStringValues,   METH_NOARGS,                  PropertyStringValues__doc__ },

    // EVENT PROCESSOR METHODS
    {"EventBacklog",          (PyCFunction)PyVGX_Graph__EventBacklog,           METH_NOARGS,                  EventBacklog__doc__  },
    {"EventEnable",           (PyCFunction)PyVGX_Graph__EventEnable,            METH_NOARGS,                  EventEnable__doc__  },
    {"EventDisable",          (PyCFunction)PyVGX_Graph__EventDisable,           METH_NOARGS,                  EventDisable__doc__  },
    {"EventFlush",            (PyCFunction)PyVGX_Graph__EventFlush,             METH_NOARGS,                  EventFlush__doc__  },
    {"EventParam",            (PyCFunction)PyVGX_Graph__EventParam,             METH_NOARGS,                  EventParam__doc__  },

    // MISC.
    {"ShowVertex",                  (PyCFunction)PyVGX_Graph__ShowVertex,                   METH_VARARGS,                 ShowVertex__doc__  },
    {"VertexDescriptor",            (PyCFunction)PyVGX_Graph__VertexDescriptor,             METH_VARARGS,                 VertexDescriptor__doc__  },
    {"ShowOpenVertices",            (PyCFunction)PyVGX_Graph__ShowOpenVertices,             METH_NOARGS,                  ShowOpenVertices__doc__  },
    {"GetIndexCounters",            (PyCFunction)PyVGX_Graph__GetIndexCounters,             METH_VARARGS | METH_KEYWORDS, GetIndexCounters__doc__  },
    {"ResetIndexCounters",          (PyCFunction)PyVGX_Graph__ResetIndexCounters,           METH_NOARGS,                  ResetIndexCounters__doc__  },
    {"DebugPrintAllocators",        (PyCFunction)PyVGX_Graph__DebugPrintAllocators,         METH_VARARGS | METH_KEYWORDS, DebugPrintAllocators__doc__  },
    {"DebugCheckAllocators",        (PyCFunction)PyVGX_Graph__DebugCheckAllocators,         METH_VARARGS | METH_KEYWORDS, DebugCheckAllocators__doc__  },
    {"DebugGetObjectByAddress",     (PyCFunction)PyVGX_Graph__DebugGetObjectByAddress,      METH_O,                       DebugGetObjectByAddress__doc__ },
    {"DebugFindObjectByIdentifier", (PyCFunction)PyVGX_Graph__DebugFindObjectByIdentifier,  METH_O,                       DebugFindObjectByIdentifier__doc__ },
    {"DebugDumpGraph",              (PyCFunction)PyVGX_Graph__DebugDumpGraph,               METH_NOARGS,                  DebugDumpGraph__doc__  },

    {"GetVertexID",                 (PyCFunction)PyVGX_Graph__GetVertexID,                  METH_VARARGS | METH_KEYWORDS, GetVertexID__doc__  },

    {"Lock",                        (PyCFunction)PyVGX_Graph__Lock,                         METH_VARARGS | METH_KEYWORDS, Lock__doc__  },
    {"Synchronized",                (PyCFunction)PyVGX_Graph__Synchronized,                 METH_VARARGS | METH_KEYWORDS, Synchronized__doc__  },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_Graph__GraphType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Graph__GraphType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Graph",
    .tp_basicsize       = sizeof(PyVGX_Graph),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Graph__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Graph__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &tp_as_sequence_PyVGX_Graph,
    .tp_as_mapping      = &tp_as_mapping_PyVGX_Graph,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_FINALIZE,
    .tp_doc             = "PyVGX Graph objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Graph__methods,
    .tp_members         = PyVGX_Graph__members,
    .tp_getset          = PyVGX_Graph__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Graph__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Graph__new,
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
    .tp_vectorcall      = (vectorcallfunc)0


};



DLL_HIDDEN PyTypeObject * p_PyVGX_Graph__GraphType = &PyVGX_Graph__GraphType;
