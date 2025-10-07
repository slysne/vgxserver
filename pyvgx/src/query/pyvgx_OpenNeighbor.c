/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_OpenNeighbor.c
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



PyVGX_DOC( pyvgx_OpenNeighbor__doc__,
  "OpenNeighbor( id, arc=(None, D_OUT), mode='r', timeout=0 ) -> vertex_instance\n"
  "\n"
  "Return first vertex instance which is a neighbor of 'id' with matching arc\n"
  "\n"
);



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_AdjacencyQuery_t * _pyvgx_OpenNeighbor__get_adjacency_query( __open_neighbor_query_args *param );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_OpenNeighbor__clear_params( __base_query_args *base ) {
  if( base ) {
    __open_neighbor_query_args *param = (__open_neighbor_query_args*)base;
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



/******************************************************************************
 * _pyvgx_OpenNeighbor__parse_params
 *
 ******************************************************************************
 */
static __open_neighbor_query_args * _pyvgx_OpenNeighbor__parse_params( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames, __open_neighbor_query_args *param ) {

  static const char *kwlist[] = {
    "id",
    "arc",
    "mode",
    "timeout",
    NULL
  };

  union {
    PyObject *args[4];
    struct {
      PyObject *py_id;
      PyObject *py_arc;
      PyObject *py_mode;
      PyObject *py_timeout;
    };
  } vcargs = {0};

  PyVGX_Graph *pygraph;
  int ret;
  if( PyVGX_Vertex_CheckExact( self ) ) {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist+1, 3, vcargs.args+1 );
    pygraph = ((PyVGX_Vertex*)self)->pygraph;
    vcargs.py_id = self;
  }
  else {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist, 4, vcargs.args );
    pygraph = (PyVGX_Graph*)self;
  }
  if( ret < 0 ) {
    return NULL;
  }

  // --
  // id
  // --
  if( vcargs.py_id == NULL || vcargs.py_id == Py_None ) {
    PyVGX_ReturnError( PyExc_TypeError, "Vertex required" );
  }
  if( iPyVGXParser.GetVertexID( pygraph, vcargs.py_id, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
    return NULL;
  }

  // ---
  // arc
  // ---
  if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, vcargs.py_arc, param->implied.default_arcdir )) == NULL ) {
    return NULL;
  }
  param->modifier = iArcConditionSet.Modifier( param->arc_condition_set );

  // ----
  // mode
  // ----
  param->readonly = true;
  if( vcargs.py_mode ) {
    const char *mode = PyUnicode_AsUTF8( vcargs.py_mode );
    if( mode == NULL ) {
      return NULL;
    }
    if( CharsEqualsConst( mode, "a" ) ) {
      param->readonly = false;
    }
    else if( !CharsEqualsConst( mode, "r" ) ) {
      PyErr_SetString( PyExc_ValueError, "mode must be 'r' or 'a'" );
      return NULL;
    }
  }

  // -------
  // timeout
  // -------
  if( vcargs.py_timeout ) {
    if( (param->timeout_ms = PyLong_AsLong( vcargs.py_timeout )) < 0 ) {
      if( !PyLong_Check( vcargs.py_timeout ) ) {
        return NULL;
      }
    }
  }


  return param;
}



/******************************************************************************
 * pyvgx_OpenNeighbor
 *
 ******************************************************************************
 */
//DLL_HIDDEN PyObject * pyvgx_OpenNeighbor( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
DLL_HIDDEN extern PyObject * pyvgx_OpenNeighbor( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  PyObject *py_neighbor = NULL;

  __open_neighbor_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  PyVGX_Graph *pygraph;
  if( PyVGX_Vertex_CheckExact( self ) ) {
    pygraph = ((PyVGX_Vertex*)self)->pygraph;
  }
  else if( PyVGX_Graph_Check( self ) ) {
    pygraph = (PyVGX_Graph*)self;
  }
  else {
    PyVGX_ReturnError( PyExc_TypeError, "graph or vertex required" );
  }


  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_OpenNeighbor__parse_params( self, args, nargs, kwnames, &param ) != NULL ) {

      // -------------------------
      // Execute
      // -------------------------
      vgx_Vertex_t *neighbor_LCK = NULL;
      BEGIN_PYVGX_THREADS {
        // Construct query
        vgx_AdjacencyQuery_t *query = _pyvgx_OpenNeighbor__get_adjacency_query( &param );
        if( query ) {
          // Execute query
          if( (neighbor_LCK = CALLABLE( param.implied.graph )->simple->OpenNeighbor( param.implied.graph, query, param.readonly )) == NULL ) {
            PyVGX_CAPTURE_QUERY_ERROR( query, &param );
          }
          iGraphQuery.DeleteAdjacencyQuery( &query );
        }
      } END_PYVGX_THREADS;

      // -----------------------------------------------
      // Create Python Vertex
      // -----------------------------------------------
      if( neighbor_LCK ) {
        py_neighbor = PyVGX_Vertex__FromInstance( pygraph, neighbor_LCK );
      }

      // -------------------------
      // Handle error
      // -------------------------
      if( py_neighbor == NULL ) {
        PyVGX_SET_QUERY_ERROR( NULL, &param, param.anchor.id );
      }
    }

    // -------------------------
    // Clean up
    // -------------------------
    _pyvgx_OpenNeighbor__clear_params( (__base_query_args*)&param );

  }
  return py_neighbor;
}



/******************************************************************************
 * _pyvgx_OpenNeighbor__get_adjacency_query
 *
 ******************************************************************************
 */
static vgx_AdjacencyQuery_t * _pyvgx_OpenNeighbor__get_adjacency_query( __open_neighbor_query_args *param ) { 

  vgx_AdjacencyQuery_t *query = NULL;

  XTRY {
    // Construct adjacency query object
    if( (query = iGraphQuery.NewAdjacencyQuery( param->implied.graph, param->anchor.id, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB11 );
    }
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec );

    // Assign arc condition set (steal)
    if( param->arc_condition_set ) {
      CALLABLE( query )->AddArcConditionSet( query, &param->arc_condition_set );
    }

  }
  XCATCH( errcode ) {
    param->implied.py_err_class = PyVGX_QueryError;
    iGraphQuery.DeleteAdjacencyQuery( &query );
  }
  XFINALLY {
  }

  return query;
}
