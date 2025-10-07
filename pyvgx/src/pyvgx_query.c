/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_query.c
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


#define TEST_PARENT_GRAPH( PyQueryObj )  ((PyQueryObj)->py_parent != NULL && (PyQueryObj)->parent == (PyQueryObj)->py_parent->graph)

#define ASSERT_PARENT_GRAPH( PyQueryObj )                 \
  if( !TEST_PARENT_GRAPH( PyQueryObj ) ) {                \
    PyErr_SetString( PyExc_Exception, "no graph" );     \
    return NULL;                                        \
  }


#define PyVGX_MEMORY_LEN( PyQueryObj )  (1LL << (PyQueryObj)->evalmem->order)




/******************************************************************************
 * PyVGX_Query__get_error
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_error( PyVGX_Query *py_query, void *closure ) {
  if( py_query->py_error ) {
    Py_INCREF( py_query->py_error );
    return py_query->py_error;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Query__get_reason
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_reason( PyVGX_Query *py_query, void *closure ) {
  vgx_AdjacencyQuery_t *query = PyVGX_PyQuery_As_AdjacencyQuery( py_query );
  int reason = 0;
  if( query ) {
    reason = query->access_reason;
  }
  return PyLong_FromLong( reason );
}



/******************************************************************************
 * PyVGX_Query__get_type
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_type( PyVGX_Query *py_query, void *closure ) {
  switch( py_query->qtype ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    return PyUnicode_FromString( "Adjacency" );
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    return PyUnicode_FromString( "Neighborhood" );
  case VGX_QUERY_TYPE_AGGREGATOR:
    return PyUnicode_FromString( "Aggregator" );
  case VGX_QUERY_TYPE_GLOBAL:
    return PyUnicode_FromString( "Global" );
  default:
    return PyUnicode_FromString( "" );
  }
}



/******************************************************************************
 * PyVGX_Query__get_opid
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_opid( PyVGX_Query *py_query, void *closure ) {
  return PyLong_FromLongLong( py_query->query->parent_opid  );
}



/******************************************************************************
 * PyVGX_Query__get_texec
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_texec( PyVGX_Query *py_query, void *closure ) {
  double t = 0.0;
  vgx_SearchResult_t *SR;
  if( (SR = py_query->query->search_result) != NULL ) {
    t = SR->exe_time.t_search + SR->exe_time.t_result;
  }
  return PyFloat_FromDouble( t );
}



/******************************************************************************
 * __adjacency_set_arc_value
 *
 ******************************************************************************
 */
static int __adjacency_set_arc_value( PyVGX_Query *py_query, PyObject *py_arcval ) {
  __adjacency_query_args *param;
  vgx_AdjacencyQuery_t *query = PyVGX_AdjacencyQuery_From_PyQuery( py_query, param );
  if( query ) {
    if( query->arc_condition_set == NULL || query->arc_condition_set->set == NULL || query->arc_condition_set->set[0] == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "query has no first-level arc filter" );
      return -1;
    }
    if( py_arcval == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "cannot remove arc value from arc filter" );
      return -1;
    }

    vgx_ArcCondition_t *arc_condition = query->arc_condition_set->set[0];
    vgx_predicator_t pred = {0};
    pred.mod.bits = arc_condition->modifier.bits;
    vgx_predicator_modifier_enum mod_enum = _vgx_predicator_as_modifier_enum( pred );
    vgx_predicator_val_t min_val = {0};
    vgx_predicator_val_t max_val = {0};
    vgx_predicator_val_type val_type = _vgx_predicator_value_range( &min_val.bits, &max_val.bits, mod_enum );
    const char *smod = _vgx_modifier_as_string( pred.mod );
    if( val_type == VGX_PREDICATOR_VAL_TYPE_REAL ) {
      if( !(PyFloat_Check( py_arcval ) || PyLong_Check( py_arcval )) ) {
        PyErr_Format( PyExc_ValueError, "arc value must be numeric for modifier %s", smod );
        return -1;
      }
      double arcval;
      if( PyFloat_Check( py_arcval ) ) {
        arcval = PyFloat_AS_DOUBLE( py_arcval );
      }
      else {
        arcval = (double)PyLong_AsLongLong( py_arcval );
      }
      if( arcval > max_val.real || arcval < min_val.real ) {
        PyErr_Format( PyExc_ValueError, "arc value out of range for modifier %s", smod );
        return -1;
      }
      arc_condition->value1.real = (float)arcval;
    }
    else if( val_type == VGX_PREDICATOR_VAL_TYPE_INTEGER ) {
      if( !PyLong_Check( py_arcval ) ) {
        PyErr_Format( PyExc_ValueError, "arc value must be integer for modifier %s", smod );
        return -1;
      }
      int64_t arcval = PyLong_AsLongLong( py_arcval );
      if( arcval > max_val.integer || arcval < min_val.integer ) {
        PyErr_Format( PyExc_ValueError, "arc value out of range for modifier %s", smod );
        return -1;
      }
      arc_condition->value1.integer = (int32_t)arcval;
    }
    else if( val_type == VGX_PREDICATOR_VAL_TYPE_UNSIGNED ) {
      if( !PyLong_Check( py_arcval ) ) {
        PyErr_Format( PyExc_ValueError, "arc value must be integer for modifier %s", smod );
        return -1;
      }
      int64_t arcval = PyLong_AsLongLong( py_arcval );
      if( arcval > max_val.uinteger || arcval < min_val.uinteger ) {
        PyErr_Format( PyExc_ValueError, "arc value out of range for modifier %s", smod );
        return -1;
      }
      arc_condition->value1.uinteger = (uint32_t)arcval;
    }
    else {
      PyErr_Format( PyExc_ValueError, "arc value not supported for modifier %s", smod );
      return -1;
    }
    // Invalidate any prior result
    __invalidate_query_cache( py_query );

    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * PyVGX_Query__get_arcval
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_arcval( PyVGX_Query *py_query, void *closure ) {
  vgx_AdjacencyQuery_t *query = PyVGX_PyQuery_As_AdjacencyQuery( py_query );
  if( query ) {
    if( query->arc_condition_set == NULL || query->arc_condition_set->set == NULL || query->arc_condition_set->set[0] == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "query has no first-level arc filter" );
      return NULL;
    }
    vgx_ArcCondition_t *arc_condition = query->arc_condition_set->set[0];

    if( arc_condition->modifier.probe.f ) {
      return PyFloat_FromDouble( arc_condition->value1.real );
    }
    else {
      return PyLong_FromLong( arc_condition->value1.integer );
    }
  }
  // error
  PyErr_SetString( PyExc_AttributeError, "arcval" );
  return NULL;
}



/******************************************************************************
 * PyVGX_Query__set_arcval
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Query__set_arcval( PyVGX_Query *py_query, PyObject *py_arcval, void *closure ) {
  return __adjacency_set_arc_value( py_query, py_arcval );
}



/******************************************************************************
 * __adjacency_set_arc_lsh
 *
 ******************************************************************************
 */
static int __adjacency_set_arc_lsh( PyVGX_Query *py_query, PyObject *py_arclsh ) {
  __adjacency_query_args *param;
  vgx_AdjacencyQuery_t *query = PyVGX_AdjacencyQuery_From_PyQuery( py_query, param );
  if( query ) {
    if( query->arc_condition_set == NULL || query->arc_condition_set->set == NULL || query->arc_condition_set->set[0] == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "query has no first-level arc filter" );
      return -1;
    }
    if( py_arclsh == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "cannot remove arc lsh probe from arc filter" );
      return -1;
    }
    if( !PyTuple_CheckExact( py_arclsh ) || PyTuple_GET_SIZE( py_arclsh ) != 2 ) {
      PyErr_Format( PyExc_ValueError, "a tuple is required: (bits, distance)" );
      return -1;
    }

    PyObject *py_bits = PyTuple_GET_ITEM( py_arclsh, 0 );
    PyObject *py_dist = PyTuple_GET_ITEM( py_arclsh, 1 );
    if( !PyLong_CheckExact( py_bits ) || !PyLong_CheckExact( py_dist ) ) {
      PyErr_Format( PyExc_ValueError, "(bits, distance) must be integers" );
      return -1;
    }

    vgx_ArcCondition_t *arc_condition = query->arc_condition_set->set[0];
    vgx_predicator_t pred = {0};
    pred.mod.bits = arc_condition->modifier.bits;
    vgx_predicator_modifier_enum mod_enum = _vgx_predicator_as_modifier_enum( pred );
    if( mod_enum != VGX_PREDICATOR_MOD_LSH ) {
      PyErr_Format( PyExc_ValueError, "incompatible query (M_LSH required)" );
      return -1;
    }

    uint64_t ubits64 = PyLong_AsUnsignedLongLong( py_bits );
    int64_t idist64 = PyLong_AsLongLong( py_dist );

    if( ubits64 > UINT_MAX ) {
      PyErr_SetString( PyExc_ValueError, "bits out of range (max 0xFFFFFFFF)" );
      return -1;
    }
    if( idist64 > 15 ) {
      idist64 = 15;
    }
    else if( idist64 < 0 ) {
      idist64 = 0;
    }

    arc_condition->value1.uinteger = (uint32_t)ubits64;
    arc_condition->value2.uinteger = (uint32_t)idist64;

    // Invalidate any prior result
    __invalidate_query_cache( py_query );

    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * PyVGX_Query__get_arclsh
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_arclsh( PyVGX_Query *py_query, void *closure ) {
  vgx_AdjacencyQuery_t *query = PyVGX_PyQuery_As_AdjacencyQuery( py_query );
  if( query ) {
    if( query->arc_condition_set == NULL || query->arc_condition_set->set == NULL || query->arc_condition_set->set[0] == NULL ) {
      PyErr_SetString( PyVGX_QueryError, "query has no first-level arc filter" );
      return NULL;
    }
    vgx_ArcCondition_t *arc_condition = query->arc_condition_set->set[0];

    if( arc_condition->modifier.probe.type != VGX_PREDICATOR_MOD_LSH ) {
      PyErr_SetString( PyExc_AttributeError, "incompatible query (M_LSH required)" );
      return NULL;
    }

    PyObject *py_arclsh = PyTuple_New(2);
    PyObject *py_bits = PyLong_FromUnsignedLong( arc_condition->value1.uinteger );
    PyObject *py_dist = PyLong_FromUnsignedLong( arc_condition->value2.uinteger );
    if( py_arclsh == NULL || py_bits == NULL || py_dist == NULL ) {
      Py_XDECREF( py_arclsh );
      Py_XDECREF( py_bits );
      Py_XDECREF( py_dist );
      return NULL;
    }

    PyTuple_SET_ITEM( py_arclsh, 0, py_bits );
    PyTuple_SET_ITEM( py_arclsh, 1, py_dist );

    return py_arclsh;
  }
  // error
  PyErr_SetString( PyExc_AttributeError, "arclsh" );
  return NULL;
}



/******************************************************************************
 * PyVGX_Query__set_arclsh
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Query__set_arclsh( PyVGX_Query *py_query, PyObject *py_arclsh, void *closure ) {
  return __adjacency_set_arc_lsh( py_query, py_arclsh );
}



/******************************************************************************
 * __adjacency_set_anchor
 *
 ******************************************************************************
 */
static int __adjacency_set_anchor( PyVGX_Query *py_query, PyObject *py_anchor ) {
  __adjacency_query_args *param;
  vgx_AdjacencyQuery_t *query = PyVGX_AdjacencyQuery_From_PyQuery( py_query, param );
  if( query ) {
    if( py_anchor == NULL ) {
      param->anchor.id = NULL;
    }
    else if( iPyVGXParser.GetVertexID( py_query->py_parent, py_anchor, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
      return -1;
    }

    CString_t *CSTR__error = NULL;
    if( CALLABLE( query )->SetAnchor( query, param->anchor.id, &CSTR__error ) < 0 ) {
      const char *s_err = CSTR__error ? CStringValue( CSTR__error ) : "internal error";
      PyErr_SetString( PyVGX_QueryError, s_err );
      iString.Discard( &CSTR__error );
      return -1;
    }

    // Invalidate any prior result
    __invalidate_query_cache( py_query );

    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * PyVGX_Query__get_id
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_id( PyVGX_Query *py_query, void *closure ) {
  vgx_AdjacencyQuery_t *query = PyVGX_PyQuery_As_AdjacencyQuery( py_query );
  if( query ) {
    CString_t *CSTR__id = query->CSTR__anchor_id;
    if( CSTR__id ) {
      return PyUnicode_FromStringAndSize( CStringValue( CSTR__id ), CStringLength( CSTR__id ) );
    }
  }
  // error
  PyErr_SetString( PyExc_AttributeError, "id" );
  return NULL;
}



/******************************************************************************
 * PyVGX_Query__set_id
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Query__set_id( PyVGX_Query *py_query, PyObject *py_id, void *closure ) {
  return __adjacency_set_anchor( py_query, py_id );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
#define UPDATE_QUERY_PARAMETER( QueryStruct, PyQuery, ParamStruct, Param, Name, Value ) \
  if( ( ((QueryStruct*)((PyQuery)->query))->Name != (Value) ) )  {                      \
    ((QueryStruct*)((PyQuery)->query))->Name = ((ParamStruct*)(Param))->Name = (Value); \
    __invalidate_query_cache( PyQuery );                                                \
  }

#define GET_QUERY_PARAMETER( QueryStruct, Query, Name )    (((QueryStruct*)(Query))->Name)



/******************************************************************************
 * __base_set_hits
 *
 ******************************************************************************
 */
static int __base_set_hits( PyVGX_Query *py_query, PyObject *py_hits ) {
  __base_query_args *param;
  vgx_BaseQuery_t *query = PyVGX_BaseQuery_From_PyQuery( py_query, param );
  if( query == NULL ) {
    return -1;
  }
  int64_t hits = -1;
  if( py_hits ) {
    if( (hits = PyLong_AsLongLong( py_hits )) < 0 && PyErr_Occurred() ) {
      return -1;
    }
  }
  switch( py_query->qtype ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    UPDATE_QUERY_PARAMETER( vgx_NeighborhoodQuery_t, py_query, __neighborhood_query_args, param, hits, hits );
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    UPDATE_QUERY_PARAMETER( vgx_GlobalQuery_t, py_query, __global_query_args, param, hits, hits );
    break;
  default:
    PyErr_SetString( PyVGX_QueryError, "invalid query" );
    return -1;
  }
  return 0;
}



/******************************************************************************
 * __base_set_offset
 *
 ******************************************************************************
 */
static int __base_set_offset( PyVGX_Query *py_query, PyObject *py_offset ) {
  __base_query_args *param;
  vgx_BaseQuery_t *query = PyVGX_BaseQuery_From_PyQuery( py_query, param );
  if( query == NULL ) {
    return -1;
  }
  int offset = 0;
  if( py_offset ) {
    if( (offset = PyLong_AsLong( py_offset )) < 0 ) {
      if( !PyErr_Occurred() ) {
        PyErr_SetString( PyVGX_QueryError, "offset cannot be negative" );
      }
      return -1;
    }
  }
  switch( py_query->qtype ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    UPDATE_QUERY_PARAMETER( vgx_NeighborhoodQuery_t, py_query, __neighborhood_query_args, param, offset, offset );
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    UPDATE_QUERY_PARAMETER( vgx_GlobalQuery_t, py_query, __global_query_args, param, offset, offset );
    break;
  default:
    PyErr_SetString( PyVGX_QueryError, "invalid query" );
    return -1;
  }
  return 0;
}



/******************************************************************************
 * __base_set_timeout_limexec
 *
 ******************************************************************************
 */
static int __base_set_timeout_limexec( PyVGX_Query *py_query, PyObject *py_timeout, PyObject *py_limexec ) {
  __base_query_args *param;
  vgx_BaseQuery_t *query = PyVGX_BaseQuery_From_PyQuery( py_query, param );
  if( query == NULL ) {
    return -1;
  }
  int timeout_ms = py_timeout ? PyLong_AsLong( py_timeout ) : param->timeout_ms;
  int limexec = py_limexec ? PyLong_AsLong( py_limexec ) : param->limexec;
  if( (timeout_ms < 0 || limexec < 0) && PyErr_Occurred() ) {
    return -1;
  }
  param->timeout_ms = timeout_ms;
  param->limexec = limexec;
  CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec > 0 );
  return 0;
}



/******************************************************************************
 * PyVGX_Query__get_timeout
 * PyVGX_Query__set_timeout
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_timeout( PyVGX_Query *py_query, void *closure ) {
  vgx_BaseQuery_t *base = py_query->query;
  if( base ) {
    if( base->timing_budget.flags.is_infinite ) {
      return PyLong_FromLong( -1 );
    }
    else {
      return PyLong_FromLong( base->timing_budget.timeout_ms );
    }
  }
  PyErr_SetString( PyExc_AttributeError, "timeout" );
  return NULL;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Query__set_timeout( PyVGX_Query *py_query, PyObject *py_timeout, void *closure ) {
  return __base_set_timeout_limexec( py_query, py_timeout, NULL );
}



/******************************************************************************
 * PyVGX_Query__get_limexec
 * PyVGX_Query__set_limexec
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Query__get_limexec( PyVGX_Query *py_query, void *closure ) {
  vgx_BaseQuery_t *base = py_query->query;
  if( base ) {
    return PyBool_FromLong( base->timing_budget.flags.is_exe_limited );
  }
  PyErr_SetString( PyExc_AttributeError, "limexec" );
  return NULL;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Query__set_limexec( PyVGX_Query *py_query, PyObject *py_limexec, void *closure ) {
  return __base_set_timeout_limexec( py_query, NULL, py_limexec );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __clear_args_stub( __base_query_args *param ) {
}



/******************************************************************************
 *
 ******************************************************************************
 */
static void __delete_query( PyVGX_Query *py_query ) {
  // Destroy query if it exists
  iGraphQuery.DeleteQuery( &py_query->query );
  py_query->qtype = VGX_QUERY_TYPE_NONE;

  // Clear parameters
  if( py_query->p_args ) {
    // Clear
    py_query->p_args->implied.clear( py_query->p_args );
    // Free
    free( py_query->p_args );
    py_query->p_args = NULL;
  }
}



/******************************************************************************
 * PyVGX_Query__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Query__dealloc( PyVGX_Query *py_query ) {
  if( py_query->threadid == GET_CURRENT_THREAD_ID() ) {
    __delete_query( py_query );
    Py_XDECREF( py_query->py_error );
    Py_TYPE( py_query )->tp_free( py_query );
  }
}



/******************************************************************************
 * PyVGX_Query__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Query__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_Query *py_query;

  if( (py_query = (PyVGX_Query*)type->tp_alloc(type, 0)) == NULL ) {
    return NULL;
  }

  py_query->py_parent = NULL;
  py_query->parent = NULL;
  py_query->threadid = 0;
  py_query->query = NULL;
  py_query->qtype = VGX_QUERY_TYPE_NONE;
  py_query->p_args = NULL;
  py_query->cache_opid = -1;
  py_query->py_error = NULL;

  return (PyObject *)py_query;
}



/******************************************************************************
 * PyVGX_Query__init
 *
 ******************************************************************************
 */
static int PyVGX_Query__init( PyVGX_Query *py_query, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "graph", "query", "params", NULL };
  PyObject *pygraph = NULL;
  PyObject *py_capsule_query = NULL;
  PyObject *py_capsule_params = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "OOO", kwlist, &pygraph, &py_capsule_query, &py_capsule_params ) ) {
    return -1;
  }

  // 1. graph
  //
  if( PyVGX_Graph_Check(pygraph) ) {
    py_query->py_parent = (PyVGX_Graph*)pygraph;
  }
  else if( PyCapsule_CheckExact(pygraph) ) {
    py_query->py_parent = (PyVGX_Graph*)PyCapsule_GetPointer( pygraph, NULL );
    if( py_query->py_parent == NULL || py_query->py_parent->graph == NULL || !COMLIB_OBJECT_ISINSTANCE( py_query->py_parent->graph, vgx_Graph_t ) ) {
      PyErr_SetString( PyExc_ValueError, "graph capsule does not contain a graph object" );
      return -1;
    }
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Invalid graph object" );
    return -1;
  }

  // Delete any previous query object (just in case)
  iGraphQuery.DeleteQuery( &py_query->query );

  // 2. query
  // Query passed in capsule
  if( PyCapsule_CheckExact( py_capsule_query ) ) {
    // Big assumption: The capsule pointer points to a vgx query instance.
    py_query->query = (vgx_BaseQuery_t*)PyCapsule_GetPointer( py_capsule_query, NULL );
    py_query->qtype = py_query->query->type;
  }
  else {
    PyErr_SetString( PyExc_NotImplementedError, "Query object cannot be directly instantiated" );
    return -1;
  }

  // 3. params
  // Params passed in capsule
  if( PyCapsule_CheckExact( py_capsule_params ) ) {
    // NOTE: Query object becomes owner of the encapsulated __base_query_args struct and must
    //       clear and free the struct upon pyobject dealloc
    py_query->p_args = (__base_query_args*)PyCapsule_GetPointer( py_capsule_params, NULL );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Invalid query parameters object" );
    return -1;
  }

  // Only this thread is allowed to operate on this query
  py_query->threadid = GET_CURRENT_THREAD_ID();

  // Safeguard: py_parent's graph reference should remain consistent
  if( py_query->py_parent) {
    py_query->parent = py_query->py_parent->graph;
  }

  return 0;
}



/******************************************************************************
 * PyVGX_Query__Execute
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Execute__doc__,
  "Execute() -> Result\n"
);
//static PyObject * PyVGX_Query__Execute( PyVGX_Query *py_query, PyObject *args, PyObject *kwds ) {

/**************************************************************************//**
 * PyVGX_Query__Execute
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Query__Execute( PyVGX_Query *py_query, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "hits",
    "offset",
    "timeout",
    "limexec",
    "cache",
    NULL
  };

  typedef union u_execute_args {
    PyObject *args[5];
    struct {
      PyObject *py_hits;
      PyObject *py_offset;
      PyObject *py_timeout;
      PyObject *py_limexec;
      PyObject *py_cache;
    };
  } execute_args;

  execute_args vcargs = {0};

  const char **pkwlist;
  Py_ssize_t sz_kwlist;

  if( (py_query->qtype & __VGX_QUERY_FEATURE_SET_RESULT) ) {
    pkwlist = kwlist;
    sz_kwlist = 5;
  }
  else {
    pkwlist = kwlist+2;
    sz_kwlist = 3;
  }

  if( __parse_vectorcall_args( args, nargs, kwnames, pkwlist, sz_kwlist, vcargs.args ) < 0 ) {
    return NULL;
  }

  __base_query_args *param;
  vgx_BaseQuery_t *query = PyVGX_BaseQuery_From_PyQuery( py_query, param );
  if( query == NULL ) {
    return NULL;
  }

  if( (py_query->qtype & __VGX_QUERY_FEATURE_SET_RESULT) ) {
    // offset
    if( __base_set_offset( py_query, vcargs.py_offset ) < 0 ) {
      return NULL;
    }

    // hits
    if( __base_set_hits( py_query, vcargs.py_hits ) < 0 ) {
      return NULL;
    }
  }

  // timeout
  // limexec
  if( vcargs.py_timeout || vcargs.py_limexec ) {
    int timeout_ms = vcargs.py_timeout ? PyLong_AsLong( vcargs.py_timeout ) : param->timeout_ms;
    int limexec = vcargs.py_limexec ? PyLong_AsLong( vcargs.py_limexec ) : param->limexec;
    if( (timeout_ms < 0 || limexec < 0) && PyErr_Occurred() ) {
      return NULL;
    }
    param->timeout_ms = timeout_ms;
    param->limexec = limexec;
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec > 0 );
  }

  // cache
  int cache = 0;
  if( vcargs.py_cache && (cache = PyLong_AsLong( vcargs.py_cache )) < 0 && PyErr_Occurred() ) {
    return NULL;
  }
  if( cache == 0 ) {
    __invalidate_query_cache( py_query );
  }

  if( py_query->py_error ) {
    Py_DECREF( py_query->py_error );
    py_query->py_error = NULL;
  }

  switch( py_query->qtype ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    return pyvgx_ExecuteAdjacencyQuery( py_query );
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    return pyvgx_ExecuteNeighborhoodQuery( py_query );
  case VGX_QUERY_TYPE_AGGREGATOR:
    return pyvgx_ExecuteAggregatorQuery( py_query );
  case VGX_QUERY_TYPE_GLOBAL:
    return pyvgx_ExecuteGlobalQuery( py_query );
  default:
    PyErr_SetNone( PyExc_NotImplementedError );
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_Query__repr
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Query__repr( PyVGX_Query *py_query ) {
  CString_t *CSTR__id = NULL;
  if( (py_query->qtype & __VGX_QUERY_FEATURE_TYPE_ANCHOR) && py_query->query ) {
    CSTR__id = ((vgx_AdjacencyQuery_t*)py_query->query)->CSTR__anchor_id;
  }
  switch( py_query->qtype ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    return PyUnicode_FromFormat( "<pyvgx.AdjacencyQuery id=\"%s\">", CSTR__id ? CStringValue( CSTR__id ) : "" );
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    return PyUnicode_FromFormat( "<pyvgx.NeighborhoodQuery id=\"%s\">", CSTR__id ? CStringValue( CSTR__id ) : "" );
  case VGX_QUERY_TYPE_AGGREGATOR:
    return PyUnicode_FromFormat( "<pyvgx.AggregatorQuery id=\"%s\">", CSTR__id ? CStringValue( CSTR__id ) : "" );
  case VGX_QUERY_TYPE_GLOBAL:
    return PyUnicode_FromString( "<pyvgx.GlobalQuery>" );
  default:
    return PyUnicode_FromString( "<pyvgx.Query>" );
  }
}



/******************************************************************************
 * PyVGX_Query__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Query__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Query__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_Query__getset[] = {
  {"error",               (getter)__PyVGX_Query__get_error,           NULL,                               "error",    NULL },
  {"reason",              (getter)__PyVGX_Query__get_reason,          NULL,                               "reason",   NULL },
  {"type",                (getter)__PyVGX_Query__get_type,            NULL,                               "type",     NULL },
  {"opid",                (getter)__PyVGX_Query__get_opid,            NULL,                               "opid",     NULL },
  {"texec",               (getter)__PyVGX_Query__get_texec,           NULL,                               "texec",    NULL },

  {"arcval",              (getter)__PyVGX_Query__get_arcval,          (setter)__PyVGX_Query__set_arcval,  "arcval",   NULL },
  {"arclsh",              (getter)__PyVGX_Query__get_arclsh,          (setter)__PyVGX_Query__set_arclsh,  "arclsh",   NULL },
  {"id",                  (getter)__PyVGX_Query__get_id,              (setter)__PyVGX_Query__set_id,      "id",       NULL },
  
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Query__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Query__methods[] = {

    {"Execute",             (PyCFunction)PyVGX_Query__Execute,               METH_FASTCALL | METH_KEYWORDS,   Execute__doc__  },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_Query_as_sequence
 *
 ******************************************************************************
 */
static PySequenceMethods PyVGX_Query_as_sequence = {
    .sq_length          = (lenfunc)0,
    .sq_concat          = (binaryfunc)0,
    .sq_repeat          = (ssizeargfunc)0,
    .sq_item            = (ssizeargfunc)0,
    .was_sq_slice       = 0,
    .sq_ass_item        = (ssizeobjargproc)0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)0,
    .sq_inplace_concat  = (binaryfunc)0,
    .sq_inplace_repeat  = (ssizeargfunc)0,
};



/******************************************************************************
 * PyVGX_Query_as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_Query_as_mapping = {
    .mp_length          = (lenfunc)0,
    .mp_subscript       = (binaryfunc)0,
    .mp_ass_subscript   = (objobjargproc)0
};



/******************************************************************************
 * PyVGX_Query__QueryType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Query__QueryType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Query",
    .tp_basicsize       = sizeof(PyVGX_Query),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Query__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Query__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &PyVGX_Query_as_sequence,
    .tp_as_mapping      = &PyVGX_Query_as_mapping,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Query objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Query__methods,
    .tp_members         = PyVGX_Query__members,
    .tp_getset          = PyVGX_Query__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Query__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Query__new,
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


DLL_HIDDEN PyTypeObject * p_PyVGX_Query__QueryType = &PyVGX_Query__QueryType;
