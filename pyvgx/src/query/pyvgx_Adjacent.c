/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_Adjacent.c
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



PyVGX_DOC( pyvgx_Adjacent__doc__,
  "Adjacent( id, arc=(None, D_OUT), neighbor=\"*\", pre=None, filter=None, post=None, memory=4, timeout=0, limexec=False ) -> boolean\n"
  "\n"
  "Test if vertex is adjacent to one or more vertices.\n"
  "\n"
);



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_AdjacencyQuery_t * _pyvgx_Adjacent__get_adjacency_query( __adjacency_query_args *param );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_Adjacent__clear_params( __base_query_args *base ) {
  if( base ) {
    __adjacency_query_args *param = (__adjacency_query_args*)base;
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }
    if( param->vertex_condition ) {
      iVertexCondition.Delete( &param->vertex_condition );
    }
    if( param->evalmem ) {
      iEvaluator.DiscardMemory( &param->evalmem );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



/******************************************************************************
 * _pyvgx_Adjacent__parse_params
 *
 ******************************************************************************
 */
static __adjacency_query_args * _pyvgx_Adjacent__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __adjacency_query_args *param ) {
  static char *kwlist[] = {
    "id",
    "arc",
    "neighbor",
    "pre",
    "filter",
    "post",
    "memory",
    "timeout",
    "limexec",
    NULL
  };

  static char *fmt = "|OOOz#z#z#Oii";

  int64_t sz_pre = 0;
  int64_t sz_filter = 0;
  int64_t sz_post = 0;

  PyObject *py_anchor = NULL;
  PyObject *py_arc_condition = NULL;
  PyObject *py_vertex_condition = NULL;
  PyObject *py_evalmem = NULL;

  // Parse   
  if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
    &py_anchor,                     // O id
    &py_arc_condition,              // O arc
    &py_vertex_condition,           // O neighbor
    &param->pre_expr,               // z pre
    &sz_pre,                        // # pre
    &param->filter_expr,            // z filter
    &sz_filter,                     // # filter
    &param->post_expr,              // z post
    &sz_post,                       // # post
    &py_evalmem,                    // O memory
    &param->timeout_ms,             // i timeout
    &param->limexec )               // i limexec
  )
  {
    return NULL;
  }
  else {

    // --
    // id
    // --
    if( py_anchor && py_anchor != Py_None ) {
      if( iPyVGXParser.GetVertexID( pygraph, py_anchor, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
        return NULL;
      }
    }

    // ---
    // arc
    // ---
    if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_arc_condition, param->implied.default_arcdir )) == NULL ) {
      return NULL;
    }
    param->modifier = iArcConditionSet.Modifier( param->arc_condition_set );

    // --------
    // neighbor
    // --------
    if( py_vertex_condition ) {
      if( (param->vertex_condition = iPyVGXParser.NewVertexCondition( param->implied.graph, py_vertex_condition, param->implied.collector_mode )) == NULL ) {
        return NULL;
      }
    }

    // ------
    // memory
    // ------
    if( py_evalmem && py_evalmem != Py_None ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        return NULL;
      }
    }

    return param;
  }
}



/******************************************************************************
 * pyvgx_Adjacent
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Adjacent( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_adjacent = NULL;

  __adjacency_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_Adjacent__parse_params( pygraph, args, kwds, &param ) != NULL ) {

      // -------------------------
      // Execute
      // -------------------------
      vgx_Relation_t *relation = NULL;
      BEGIN_PYVGX_THREADS {
        // Construct query
        vgx_AdjacencyQuery_t *query = _pyvgx_Adjacent__get_adjacency_query( &param );
        if( query ) {
          // Execute query
          if( (relation = CALLABLE( param.implied.graph )->simple->HasAdjacency( param.implied.graph, query )) == NULL ) {
            PyVGX_CAPTURE_QUERY_ERROR( query, &param );
          }
          iGraphQuery.DeleteAdjacencyQuery( &query );
        }
      } END_PYVGX_THREADS;

      // -----------------------------------------------
      // Build Python response object from relation
      // -----------------------------------------------
      if( relation ) {
        Py_INCREF( py_adjacent = relation->relationship.CSTR__name ? Py_True : Py_False );
        iRelation.Delete( &relation );
      }

      // -------------------------
      // Handle error
      // -------------------------
      if( py_adjacent == NULL ) {
        PyVGX_SET_QUERY_ERROR( NULL, &param, param.anchor.id );
      }
    }

    // -------------------------
    // Clean up
    // -------------------------
    _pyvgx_Adjacent__clear_params( (__base_query_args*)&param );

  }
  return py_adjacent;
}



/******************************************************************************
 * _pyvgx_Adjacent__get_adjacency_query
 *
 ******************************************************************************
 */
static vgx_AdjacencyQuery_t * _pyvgx_Adjacent__get_adjacency_query( __adjacency_query_args *param ) { 

  vgx_AdjacencyQuery_t *query = NULL;

  XTRY {
    // Construct adjacency query object
    if( (query = iGraphQuery.NewAdjacencyQuery( param->implied.graph, param->anchor.id, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB11 );
    }
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec );

    // Evaluator Memory (query owns +1 ref)
    if( param->evalmem ) {
      CALLABLE( query )->SetMemory( query, param->evalmem );
    }

    // Assign arc condition set (steal)
    if( param->arc_condition_set ) {
      CALLABLE( query )->AddArcConditionSet( query, &param->arc_condition_set );
    }

    // Assign pre filter expression
    if( param->pre_expr ) {
      if( CALLABLE( query )->AddPreFilter( query, param->pre_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xB14 );
      }
    }

    // Assign filter expression
    if( param->filter_expr ) {
      if( CALLABLE( query )->AddFilter( query, param->filter_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xB15 );
      }
    }

    // Assign post filter expression
    if( param->post_expr ) {
      if( CALLABLE( query )->AddPostFilter( query, param->post_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xB16 );
      }
    }

    // Assign vertex condition (steal)
    if( param->vertex_condition ) {
      CALLABLE( query )->AddVertexCondition( query, &param->vertex_condition );
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



/******************************************************************************
 * pyvgx_NewAdjacencyQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_NewAdjacencyQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph;
  if( (graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) == NULL ) {
    return NULL;
  }

  PyVGX_Query *py_query = NULL;
  __adjacency_query_args *param = NULL;
  vgx_AdjacencyQuery_t *query = NULL;

  XTRY {

    // New args
    if( (param = PyVGX_NewQueryArgs( __adjacency_query_args, graph, VGX_ARCDIR_OUT, VGX_COLLECTOR_MODE_COLLECT_ARCS, _pyvgx_Adjacent__clear_params )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Parse Parameters
    if( _pyvgx_Adjacent__parse_params( pygraph, args, kwds, param ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Construct query
    if( (query = _pyvgx_Adjacent__get_adjacency_query( param )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Create python query object and steal param 
    if( (py_query = PyVGX_PyQuery_From_BaseQuery( pygraph, query, &param )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

  }
  XCATCH( errcode ) {
    if( param ) {
      // Destroy query if it exists
      iGraphQuery.DeleteAdjacencyQuery( &query );
      // Clear parameters
      param->implied.clear( (__base_query_args*)param );
      free( param );
      param = NULL;
    }

    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "unknown error" );
    }
  }
  XFINALLY {
  }
 
  return (PyObject*)py_query;
}



/******************************************************************************
 * pyvgx_ExecuteAdjacencyQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_ExecuteAdjacencyQuery( PyVGX_Query *py_query ) {

  PyObject *py_adjacent = NULL;
  vgx_AdjacencyQuery_t *query = (vgx_AdjacencyQuery_t*)py_query->query;

  if( py_query->qtype != VGX_QUERY_TYPE_ADJACENCY || query == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "internal error, invalid query" );
    return NULL;
  }

  __adjacency_query_args *param = (__adjacency_query_args*)py_query->p_args;
  if( param == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "Missing query parameters" );
    return NULL;
  }

  vgx_Graph_t *graph = param->implied.graph;

  // -------------------------
  // Execute
  // -------------------------
  vgx_Relation_t *relation = NULL;

  BEGIN_PYVGX_THREADS {
    // First clean up any previous result object and collector object since we will generate a new result object by running the query again
    iGraphQuery.EmptyAdjacencyQuery( query );

    // Execute
    if( (relation = CALLABLE( graph )->simple->HasAdjacency( graph, query )) == NULL ) {
      PyVGX_CAPTURE_QUERY_ERROR( query, param );
    }

    // Set cache opid
    __set_query_cache( py_query );
  } END_PYVGX_THREADS;

  // ------------------------------------------
  // Build Python response object from relation
  // ------------------------------------------
  if( relation ) {
    Py_INCREF( py_adjacent = relation->relationship.CSTR__name ? Py_True : Py_False );
    iRelation.Delete( &relation );
  }

  // -------------------------
  // Handle error
  // -------------------------
  if( py_adjacent == NULL ) {
    param->anchor.id = query->CSTR__anchor_id ? CStringValue( query->CSTR__anchor_id ) : "?";
    PyVGX_SET_QUERY_ERROR( py_query, param, param->anchor.id );
  }

  iString.Discard( &param->implied.CSTR__error );

  return py_adjacent;
}
