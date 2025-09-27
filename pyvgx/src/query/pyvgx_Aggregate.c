/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_Aggregate.c
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

PyVGX_DOC( pyvgx_Aggregate__doc__,
  "Aggregate( id, arc=(None, D_OUT), pre=None, filter=None, post=None, collect=C_COLLECT, neighbor=\"*\", result=R_COUNTS, fields=F_NONE, memory=4, timeout=0, limexec=False ) -> dict\n"
  "\n"
  "Perform field value aggregation in specific vertex neighborhood.\n"
  "\n"
);



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_AggregatorQuery_t * _pyvgx_Aggregate__get_aggregator_query( __aggregator_query_args *param );
static PyObject * _pyvgx_Aggregate__get_aggregation_result( vgx_AggregatorQuery_t *query, __aggregator_query_args *param, PyObject **py_timing );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_Aggregate__clear_params( __base_query_args *base ) {
  if( base ) {
    __aggregator_query_args *param = (__aggregator_query_args*)base;
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }
    if( param->vertex_condition ) {
      iVertexCondition.Delete( &param->vertex_condition );
    }
    if( param->collect_arc_condition_set ) {
      iArcConditionSet.Delete( &param->collect_arc_condition_set );
    }
    if( param->evalmem ) {
      iEvaluator.DiscardMemory( &param->evalmem );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



/******************************************************************************
 * _pyvgx_Aggregate__parse_params
 *
 ******************************************************************************
 */
static __aggregator_query_args * _pyvgx_Aggregate__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __aggregator_query_args *param ) {
  static char *kwlist[] = {
    "id",
    "arc",
    "pre",
    "filter",
    "post",
    "collect",
    "neighbor",
    "result",
    "fields",
    "memory",
    "timeout",
    "limexec",
    NULL
  };

  static char *fmt = "|OOz#z#z#OOIIOii";

  // Set nonzero defaults
  param->result_format = VGX_RESPONSE_SHOW_AS_DICT | VGX_RESPONSE_SHOW_WITH_COUNTS;
  param->result_attrs = VGX_RESPONSE_ATTRS_NONE;

  int64_t sz_pre = 0;
  int64_t sz_filter = 0;
  int64_t sz_post = 0;

  PyObject *py_anchor = NULL;
  PyObject *py_arc_condition = NULL;
  PyObject *py_vertex_condition = NULL;
  PyObject *py_collect = NULL;
  PyObject *py_evalmem = NULL;

  // Parse   
  if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
    &py_anchor,                     // O id
    &py_arc_condition,              // O arc
    &param->pre_expr,               // z pre
    &sz_pre,                        // # pre
    &param->filter_expr,            // z filter
    &sz_filter,                     // # filter
    &param->post_expr,              // z post
    &sz_post,                       // # post
    &py_collect,                    // O collect
    &py_vertex_condition,           // O neighbor
    &param->result_format,          // I result
    &param->result_attrs,           // I fields
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

    // -------
    // collect
    // -------
    if( py_collect ) {
      if( py_collect == Py_True ) {
        // in the context of creating a NewNeighborhoodQuery, NULL-condition means collect all
        param->collect_arc_condition_set = NULL;
      }
      else if( py_collect == Py_False ) {
        // Create inverted wildcard condition set (i.e. nothing will match, collect nothing)
        if( (param->collect_arc_condition_set = iArcConditionSet.NewEmpty( param->implied.graph, false, VGX_ARCDIR_ANY )) == NULL ) {
          PyErr_SetNone( PyExc_MemoryError );
          return NULL;
        }
      }
      else {
        if( (param->collect_arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_collect, VGX_ARCDIR_ANY )) == NULL ) {
          return NULL;
        }
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

    // Verify modifier
    if( (param->result_attrs & VGX_RESPONSE_ATTR_VALUE) && param->modifier == VGX_PREDICATOR_MOD_NONE ) {
      PyErr_SetString( PyVGX_QueryError, "modifier filter required to aggregate on predicator value" );
      return NULL;
    }

    // ----------------------------------------------------------
    // Set default aggregation fields if none specified
    // ----------------------------------------------------------
    if( (param->result_attrs & VGX_RESPONSE_ATTRS_MASK) == VGX_RESPONSE_ATTRS_NONE ) {
      // Non-wildcard arc condition given: aggregate arc value
      if( param->modifier != VGX_PREDICATOR_MOD_NONE ) {
        param->result_attrs |= VGX_RESPONSE_ATTR_VALUE;
      }
      // One or more vertex degree conditions given: aggregate on those specified
      if( param->vertex_condition && param->vertex_condition->spec & (unsigned)VGX_RESPONSE_ATTRS_DEGREES ) {
        param->result_attrs |= (   ((unsigned)VGX_RESPONSE_ATTR_DEGREE    * _vgx_vertex_condition_has_degree( param->vertex_condition->spec ))
                                 | ((unsigned)VGX_RESPONSE_ATTR_INDEGREE  * _vgx_vertex_condition_has_indegree( param->vertex_condition->spec ))
                                 | ((unsigned)VGX_RESPONSE_ATTR_OUTDEGREE * _vgx_vertex_condition_has_outdegree( param->vertex_condition->spec ))
                               );
      }
    }



    return param;
  }
}



/******************************************************************************
 * pyvgx_Aggregate
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Aggregate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_aggregation = NULL;

  __aggregator_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    PyObject *py_timing = NULL;
    __PY_START_TIMED_BLOCK( &py_timing, "total" ) {

      // -------------------------
      // Parse Parameters
      // -------------------------
      if( _pyvgx_Aggregate__parse_params( pygraph, args, kwds, &param ) != NULL ) {

        // -------------------------
        // Execute
        // -------------------------
        int64_t n_aggr = -1;
        vgx_AggregatorQuery_t *query;
        BEGIN_PYVGX_THREADS {
          // Create Aggregator Query
          if( (query = _pyvgx_Aggregate__get_aggregator_query( &param )) != NULL ) {
            // Execute query
            if( (n_aggr = CALLABLE( param.implied.graph )->simple->Aggregate( param.implied.graph, query )) < 0 ) {
              PyVGX_CAPTURE_QUERY_ERROR( query, &param );
            }
          }
        } END_PYVGX_THREADS;


        // -----------------------------------------------
        // Build Python response object from search result
        // -----------------------------------------------
        if( n_aggr >= 0 ) {
          py_aggregation = _pyvgx_Aggregate__get_aggregation_result( query, &param, &py_timing );
        }

        // -------------------------
        // Delete query
        // -------------------------
        iGraphQuery.DeleteAggregatorQuery( &query );

        // -------------------------
        // Handle error
        // -------------------------
        if( py_aggregation == NULL ) {
          PyVGX_SET_QUERY_ERROR( NULL, &param, param.anchor.id );
        }
      }

      // -------------------------
      // Cleanup
      // -------------------------
      _pyvgx_Aggregate__clear_params( (__base_query_args*)&param );

    } __PY_END_TIMED_BLOCK;
  }

  return py_aggregation;
}



/******************************************************************************
 * _pyvgx_Aggregate__get_aggregator_query
 *
 ******************************************************************************
 */
static vgx_AggregatorQuery_t * _pyvgx_Aggregate__get_aggregator_query( __aggregator_query_args *param ) { 

  vgx_AggregatorQuery_t *query = NULL;

  XTRY {

    // Construct aggregator query object
    if( (query = iGraphQuery.NewAggregatorQuery( param->implied.graph, param->anchor.id, &param->collect_arc_condition_set, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xB82 );
    }
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec );
    
    if( param->result_format & VGX_RESPONSE_SHOW_WITH_TIMING ) {
      query->exe_time.pt_search = &query->exe_time.t_search;
    }

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
        THROW_SILENT( CXLIB_ERR_API, 0xB85 );
      }
    }

    // Assign filter expression
    if( param->filter_expr ) {
      if( CALLABLE( query )->AddFilter( query, param->filter_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xB86 );
      }
    }

    // Assign post filter expression
    if( param->post_expr ) {
      if( CALLABLE( query )->AddPostFilter( query, param->post_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xB87 );
      }
    }

    // Assign vertex condition (steal)
    if( param->vertex_condition ) {
      CALLABLE( query )->AddVertexCondition( query, &param->vertex_condition );
    }

    // ----------------------------------------------------------
    // Patch in aggregator fields as requested
    // ----------------------------------------------------------
    if( (param->result_attrs & VGX_RESPONSE_ATTR_VALUE) ) {
      CALLABLE( query )->AggregatePredicatorValue( query );
    }
    if( (param->result_attrs & VGX_RESPONSE_ATTR_DEGREE) ) {
      CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_ANY );
    }
    if( (param->result_attrs & VGX_RESPONSE_ATTR_INDEGREE) ) {
      CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_IN );
    }
    if( (param->result_attrs & VGX_RESPONSE_ATTR_OUTDEGREE) ) {
      CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_OUT );
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



/******************************************************************************
 * pyvgx_Aggregate__get_aggregation_result
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Aggregate__get_aggregation_result( vgx_AggregatorQuery_t *query, __aggregator_query_args *param, PyObject **py_timing ) {
  vgx_aggregator_predicator_value_t val = CALLABLE( query )->AggregatePredicatorValue( query );
  vgx_predicator_t pred = VGX_PREDICATOR_NONE;
  if( query->arc_condition_set && query->arc_condition_set->set && *query->arc_condition_set->set ) {
    pred.mod.bits = (*query->arc_condition_set->set)->modifier.bits;
  }

  // Return number
  if( (param->result_format & VGX_RESPONSE_SHOW_MASK) == VGX_RESPONSE_SHOW_WITH_NONE && POPCNT32( param->result_attrs ) == 1 ) {
    int dir = -1;
    switch( param->result_attrs ) {
    case VGX_RESPONSE_ATTR_VALUE:
      if( _vgx_predicator_value_is_float( pred ) ) {
        return PyFloat_FromDouble( val.real );
      }
      else {
        return PyLong_FromLongLong( val.integer );
      }
    case VGX_RESPONSE_ATTR_DEGREE:
      dir = VGX_ARCDIR_ANY;
    case VGX_RESPONSE_ATTR_INDEGREE:
      dir = dir < 0 ? VGX_ARCDIR_IN : dir;
    case VGX_RESPONSE_ATTR_OUTDEGREE:
      dir = dir < 0 ? VGX_ARCDIR_OUT : dir;
      return PyLong_FromLongLong( CALLABLE( query )->AggregateDegree( query, dir ) );
    default:
      PyErr_SetString( PyExc_Exception, "internal error" );
      return NULL;
    }
  }
  // Return dict
  else {
    PyObject *py_aggregation;
    if( (py_aggregation = PyDict_New()) != NULL ) {

      double t_pyresult = 0.0;
      double *pt_pyresult = NULL;
      bool with_timing = (param->result_format & VGX_RESPONSE_SHOW_WITH_TIMING) != 0;
      
      if( with_timing ) {
        pt_pyresult = &t_pyresult;
      }

      __START_TIMED_BLOCK( pt_pyresult, NULL, NULL ) {
        // Total scanned
        if( param->result_format & VGX_RESPONSE_SHOW_WITH_COUNTS ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "neighbors", query->n_neighbors );
          iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "arcs", query->n_arcs );
        }

        // Predicator value aggregation was performed, extract value
        if( param->result_attrs & VGX_RESPONSE_ATTR_VALUE ) {
          if( _vgx_predicator_value_is_float( pred ) ) {
            iPyVGXBuilder.DictMapStringToFloat( py_aggregation, "predicator_value", val.real );
          }
          else {
            iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "predicator_value", val.integer );
          }
        }

        // Vertex Degree aggregation
        if( param->result_attrs & VGX_RESPONSE_ATTRS_DEGREES ) {
          if( param->result_attrs & VGX_RESPONSE_ATTR_DEGREE ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "degree", CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_ANY ) );
          }
          if( param->result_attrs & VGX_RESPONSE_ATTR_INDEGREE ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "indegree", CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_IN ) );
          }
          if( param->result_attrs & VGX_RESPONSE_ATTR_OUTDEGREE ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_aggregation, "outdegree", CALLABLE( query )->AggregateDegree( query, VGX_ARCDIR_OUT ) );
          }
        }
      } __END_TIMED_BLOCK;

      // Add timing
      if( param->result_format & VGX_RESPONSE_SHOW_WITH_TIMING ) {
        PyObject *py_wrap = PyDict_New();
        PyObject *py_tdict = PyDict_New();
        if( py_wrap && py_tdict ) {
          *py_timing = py_tdict; // BORROWED REF
          iPyVGXBuilder.DictMapStringToFloat( py_tdict, "search", query->exe_time.t_search );
          iPyVGXBuilder.DictMapStringToFloat( py_tdict, "result", query->exe_time.t_result + t_pyresult );
          if( iPyVGXBuilder.DictMapStringToPyObject( py_wrap, "time", &py_tdict ) < 0 ) {
            *py_timing = NULL;
          }
          iPyVGXBuilder.DictMapStringToPyObject( py_wrap, "aggregation", &py_aggregation );
          py_aggregation = py_wrap;
        }

      }
    }
    return py_aggregation;
  }
}



/******************************************************************************
 * pyvgx_NewAggregatorQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_NewAggregatorQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph;
  if( (graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) == NULL ) {
    return NULL;
  }

  PyVGX_Query *py_query = NULL;
  __aggregator_query_args *param = NULL;
  vgx_AggregatorQuery_t *query = NULL;

  XTRY {

    // New args
    if( (param = PyVGX_NewQueryArgs( __aggregator_query_args, graph, VGX_ARCDIR_OUT, VGX_COLLECTOR_MODE_COLLECT_ARCS, _pyvgx_Aggregate__clear_params )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Parse Parameters
    if( _pyvgx_Aggregate__parse_params( pygraph, args, kwds, param ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Construct query
    if( (query = _pyvgx_Aggregate__get_aggregator_query( param )) == NULL ) {
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
      iGraphQuery.DeleteAggregatorQuery( &query );
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
 * pyvgx_ExecuteAggregatorQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_ExecuteAggregatorQuery( PyVGX_Query *py_query ) {

  PyObject *py_aggregation = NULL;
  vgx_AggregatorQuery_t *query = (vgx_AggregatorQuery_t*)py_query->query;

  if( py_query->qtype != VGX_QUERY_TYPE_AGGREGATOR || query == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "internal error, invalid query" );
    return NULL;
  }

  __aggregator_query_args *param = (__aggregator_query_args*)py_query->p_args;
  if( param == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "Missing query parameters" );
    return NULL;
  }

  vgx_Graph_t *graph = param->implied.graph;

  PyObject *py_timing = NULL;
  __PY_START_TIMED_BLOCK( &py_timing, "total" ) {

    // -------------------------
    // Execute
    // -------------------------
    int64_t n_aggr = 0;
    BEGIN_PYVGX_THREADS {
      // First clean up any previous result object and collector object since we will generate a new result object by running the query again
      iGraphQuery.EmptyAggregatorQuery( query );

      // Execute query
      if( (n_aggr = CALLABLE( graph )->simple->Aggregate( graph, query )) < 0 ) {
        PyVGX_CAPTURE_QUERY_ERROR( query, param );
      }
    } END_PYVGX_THREADS;

    // -----------------------------------------------
    // Build Python response object from search result
    // -----------------------------------------------
    if( n_aggr >= 0 ) {
      py_aggregation = _pyvgx_Aggregate__get_aggregation_result( query, param, &py_timing );
    }

  } __PY_END_TIMED_BLOCK;


  // -------------------------
  // Handle error
  // -------------------------
  if( py_aggregation == NULL ) {
    param->anchor.id = query->CSTR__anchor_id ? CStringValue( query->CSTR__anchor_id ) : "?";
    PyVGX_SET_QUERY_ERROR( py_query, param, param->anchor.id );
  }

  iString.Discard( &param->implied.CSTR__error );

  return py_aggregation;


}
