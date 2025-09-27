/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_Degree.c
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

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


PyVGX_DOC( pyvgx_Degree__doc__,
  "Degree( id, arc=(None, D_ANY), filter=None, timeout=0 ) -> Vertex degree\n"
  "\n"
  "id         : Unique string identifier for vertex, or pyvgx.Vertex instance\n"
  "arc        : Arcs matching this filter contribute to the computed degree\n"
  "filter     : Filter expression must evaluate to a positive integer to match\n"
  "timeout_ms : Vertex acquisition timeout in milliseconds\n"
  "limexec    : Timeout applies to overall execution (not just acquisition)\n"
  "\n"
);



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_degree_pyargs {
  pyvgx_VertexIdentifier_t vertex;                      // O id -> parsed from pyobject to string
  // --------------------------------------------------
  vgx_ArcConditionSet_t *arc_condition_set;   // O arc -> parsed into vgx_ArcConditionSet_t
  const char *filter_expr;                    // z filter
  int timeout_ms;                             // i timeout
  int limexec;                                // i limexec
  // EXTRA
  struct {
    vgx_Graph_t *graph;                               // graph
    int arcdir;                                       // arcdir
    CString_t *CSTR__error;
    vgx_AccessReason_t reason;
    PyObject *py_err_class;
  } implied;
} __degree_pyargs;



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_AggregatorQuery_t * _pyvgx_Degree__get_aggregator_query( __degree_pyargs *param );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_Degree__clear_params( __degree_pyargs *param ) {
  if( param ) {
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



/******************************************************************************
 * _pyvgx_Degree__parse_params
 *
 ******************************************************************************
 */
static __degree_pyargs * _pyvgx_Degree__parse_params( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames, __degree_pyargs *param ) {

  static const char *kwlist[] = {
    "id",
    "arc",
    "filter",
    "timeout",
    "limexec",
    NULL
  };

  union {
    PyObject *args[5];
    struct {
      PyObject *py_id;
      PyObject *py_arc;
      PyObject *py_filter;
      PyObject *py_timeout;
      PyObject *py_limexec;
    };
  } vcargs = {0};

  PyVGX_Graph *pygraph;
  int ret;
  if( PyVGX_Vertex_CheckExact( self ) ) {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist+1, 4, vcargs.args+1 );
    pygraph = ((PyVGX_Vertex*)self)->pygraph;
    vcargs.py_id = self;
  }
  else {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist, 5, vcargs.args );
    pygraph = (PyVGX_Graph*)self;
  }
  if( ret < 0 ) {
    return NULL;
  }

  // [REQUIRED]
  // ----------
  // id
  // ----------
  if( vcargs.py_id == NULL ) {
    PyVGX_ReturnError( PyExc_TypeError, "Vertex required" );
  }
  if( iPyVGXParser.GetVertexID( pygraph, vcargs.py_id, &param->vertex, NULL, true, "Vertex ID" ) < 0 ) {
    return NULL;
  }

  // ------
  // filter
  // ------
  if( vcargs.py_filter) {
    if( (param->filter_expr = PyUnicode_AsUTF8( vcargs.py_filter )) == NULL ) {
      return NULL;
    }
  }

  // ---
  // arc
  // ---
  if( param->filter_expr ) {
    param->implied.arcdir = -1;
  }
  else if( vcargs.py_arc ) {
    if( PyLong_CheckExact( vcargs.py_arc ) ) {
      if( !_vgx_arcdir_valid( (param->implied.arcdir = PyLong_AsLong( vcargs.py_arc )) ) ) {
        PyVGXError_SetString( PyExc_ValueError, "Invalid arc direction" );
        return NULL;
      }
    }
    else {
      param->implied.arcdir = -1;
    }
  }

  if( param->implied.arcdir < 0 ) {
    if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, vcargs.py_arc, VGX_ARCDIR_ANY )) == NULL ) {
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

  // -------
  // limexec
  // -------
  if( vcargs.py_limexec ) {
    if( (param->limexec = PyLong_AsLong( vcargs.py_limexec )) < 0 ) {
      if( !PyLong_Check( vcargs.py_limexec ) ) {
        return NULL;
      }
    }
  }

  return param;
}



/******************************************************************************
 * pyvgx_Degree
 *
 ******************************************************************************
 */
//DLL_HIDDEN PyObject * pyvgx_Degree( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
DLL_HIDDEN PyObject * pyvgx_Degree( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  PyObject *py_degree = NULL;

  __degree_pyargs param = {0};
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
    if( _pyvgx_Degree__parse_params( self, args, nargs, kwnames, &param ) != NULL ) {

      int64_t degree = -1;
      // -------------------------
      // Direct lookup
      // -------------------------
      if( param.arc_condition_set == NULL ) {
        BEGIN_PYVGX_THREADS {
          CString_t *CSTR__vertex_name = NewEphemeralCString( param.implied.graph, param.vertex.id );
          if( CSTR__vertex_name ) {
            degree = CALLABLE( param.implied.graph )->simple->Degree( param.implied.graph, CSTR__vertex_name, param.implied.arcdir, param.timeout_ms, &param.implied.reason );
            CStringDelete( CSTR__vertex_name );
          }
        } END_PYVGX_THREADS;
      }
      // --------------------------------
      // Calculate degree via aggregation
      // --------------------------------
      else {
        // -------------------------
        // Create query
        // -------------------------
        BEGIN_PYVGX_THREADS {
          vgx_AggregatorQuery_t *query;
          if( (query = _pyvgx_Degree__get_aggregator_query( &param )) != NULL ) {
            if( CALLABLE( param.implied.graph )->simple->Aggregate( param.implied.graph, query ) >= 0 ) {
              degree = query->n_arcs;
            }
            else {
              param.implied.CSTR__error = query->CSTR__error;
              query->CSTR__error = NULL;
              param.implied.reason = query->access_reason;
            }
            iGraphQuery.DeleteAggregatorQuery( &query );
          }
        } END_PYVGX_THREADS;
      }

      if( degree >= 0 ) {
        py_degree = PyLong_FromLongLong( degree );
      }
      else if( !iPyVGXBuilder.SetPyErrorFromAccessReason( param.vertex.id, param.implied.reason, &param.implied.CSTR__error ) ) {
        const char *errstr = param.implied.CSTR__error ? CStringValue( param.implied.CSTR__error ) : "internal error";
        PyErr_SetString( param.implied.py_err_class, errstr );
      }
    }

    // -------------------------
    // Clean up
    // -------------------------
    _pyvgx_Degree__clear_params( &param );

  }

  return py_degree;
}



/******************************************************************************
 * _pyvgx_Degree__get_aggregator_query
 *
 ******************************************************************************
 */
static vgx_AggregatorQuery_t * _pyvgx_Degree__get_aggregator_query( __degree_pyargs *param ) { 
  vgx_AggregatorQuery_t *query = NULL;

  XTRY {
    // Construct aggregator query object
    if( (query = iGraphQuery.NewAggregatorQuery( param->implied.graph, param->vertex.id, NULL, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC11 );
    }
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec );

    // Assign arc condition set (steal)
    if( param->arc_condition_set ) {
      CALLABLE( query )->AddArcConditionSet( query, &param->arc_condition_set );
    }

    // Assign filter expression
    if( param->filter_expr ) {
      if( CALLABLE( query )->AddFilter( query, param->filter_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC13 );
      }
    }

  }
  XCATCH( errcode ) {
    param->implied.py_err_class = PyVGX_QueryError;
    iGraphQuery.DeleteAggregatorQuery( &query );
  }
  XFINALLY {
  }

  return query;
}
