/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_Global.c
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



/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Global__perform( __global_query_args *param, PyObject **py_timing );
static vgx_GlobalQuery_t * _pyvgx_Global__get_global_query( __global_query_args *param );
static PyObject * _pyvgx_Global__get_result( vgx_SearchResult_t *search_result, vgx_collector_mode_t mode, PyObject **py_timing );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_Global__clear_params( __base_query_args *base ) {
  if( base ) {
    __global_query_args *param = (__global_query_args*)base;
    if( param->vertex_condition ) {
      iVertexCondition.Delete( &param->vertex_condition );
    }
    if( param->ranking_condition ) {
      iRankingCondition.Delete( &param->ranking_condition );
    }
    if( param->evalmem ) {
      iEvaluator.DiscardMemory( &param->evalmem );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



PyVGX_DOC( pyvgx_Vertices__doc__,
  "Vertices( condition=None, vector=[], result=R_STR, fields=F_ID, select=None, rank=None, sortby=S_NONE, memory=4, offset=0, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Perform a global search for vertices matching the given condition. By default all vertices are returned.\n"
  "\n"
);
PyVGX_DOC( pyvgx_Arcs__doc__,
  "Arcs( condition=None, vector=[], result=R_STR, fields=F_ID, select=None, rank=None, sortby=S_NONE, memory=4, offset=0, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Perform a global search for arcs matching the given condition. By default all arcs are returned.\n"
  "\n"
);

/**************************************************************************//**
 * _pyvgx_Global__parse_params
 *
 ******************************************************************************
 */
static __global_query_args * _pyvgx_Global__parse_params( PyObject *args, PyObject *kwds, __global_query_args *param, bool reusable ) {
  static char *fmt_reusable = "|OOIIz#OIOi";
  static char *kwlist_reusable[] = {
    "condition",  //  O
    "vector",     //  O
    "result",     //  I
    "fields",     //  I
    "select",     //  z#
    "rank",       //  O
    "sortby",     //  I
    "memory",     //  O
    "__debug",    //  i
    NULL
  };

  static char *fmt = "|OOIIz#OIOiLiii";
  static char *kwlist[] = {
    "condition",  //  O
    "vector",     //  O
    "result",     //  I
    "fields",     //  I
    "select",     //  z#
    "rank",       //  O
    "sortby",     //  I
    "memory",     //  O
    "offset",     //  i
    "hits",       //  L
    "timeout",    //  i
    "limexec",    //  i
    "__debug",    //  i
    NULL
  };

  // Set nonzero defaults
  param->result_format = VGX_RESPONSE_SHOW_AS_STRING;
  param->hits = -1;

  int64_t sz_select = 0;

  PyObject *py_vertex_condition = NULL;
  PyObject *py_rank_vector_object = NULL;
  PyObject *py_rankspec = NULL;
  PyObject *py_evalmem = NULL;

  if( reusable ) {
    // Parser, reusable context
    if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt_reusable, kwlist_reusable,
      &py_vertex_condition,       //  O condition
      &py_rank_vector_object,     //  O vector
      &param->result_format,      //  I result
      &param->result_attrs,       //  I fields
      &param->select_statement,   //  z select
      &sz_select,                 //  # select
      &py_rankspec,               //  O rank
      &param->sortspec,           //  I sortby
      &py_evalmem,                //  O memory
      &param->implied.__debug )
    )
    {
      return NULL;
    }
  }
  else {
    // Parse
    if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
      &py_vertex_condition,       //  O condition
      &py_rank_vector_object,     //  O vector
      &param->result_format,      //  I result
      &param->result_attrs,       //  I fields
      &param->select_statement,   //  z select
      &sz_select,                 //  # select
      &py_rankspec,               //  O rank
      &param->sortspec,           //  I sortby
      &py_evalmem,                //  O memory
      &param->offset,             //  i offset
      &param->hits,               //  L hits
      &param->timeout_ms,         //  i timeout
      &param->limexec,            //  i limexec
      &param->implied.__debug )
    )
    {
      return NULL;
    }
  }

  XTRY {

    // ---------
    // condition
    // ---------
    if( py_vertex_condition || param->implied.collector_mode == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
      if( (param->vertex_condition = iPyVGXParser.NewVertexCondition( param->implied.graph, py_vertex_condition, param->implied.collector_mode )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      // Global arc search
      if( param->implied.collector_mode == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
        // No arc conditions specified
        if( !iVertexCondition.HasArcTraversal( param->vertex_condition ) ) {
          // Collect all outarcs
          vgx_ArcConditionSet_t *any_out = iArcConditionSet.NewEmpty( param->implied.graph, true, VGX_ARCDIR_OUT );
          if( any_out ) {
            iVertexCondition.RequireArcTraversal( param->vertex_condition, &any_out );
          }
          else {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
          }
        }
        // Collect arcs
        iVertexCondition.CollectNeighbors( param->vertex_condition, NULL, VGX_COLLECTOR_MODE_COLLECT_ARCS );
      }
    }

    // ------
    // result
    // fields
    // ------

    // Make sure we include properties when select statement is used
    if( param->select_statement != NULL ) {
      param->result_attrs |= VGX_RESPONSE_ATTR_PROPERTY;
    }
    else if( vgx_response_attrs( param->result_attrs ) == VGX_RESPONSE_ATTRS_NONE ) {
      switch( param->implied.collector_mode ) {
      case VGX_COLLECTOR_MODE_COLLECT_ARCS:
        param->result_attrs |= VGX_RESPONSE_ATTRS_ANCHORED_ARC;
        break;
      case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
        param->result_attrs |= VGX_RESPONSE_ATTR_ID;
        break;
      default:
        break;
      }
    }

    // Add the fields to the result format
    param->result_format |= param->result_attrs;

    // Special handling of sorting if counts are requested
    if( (param->result_format & VGX_RESPONSE_SHOW_WITH_COUNTS) && param->sortspec == VGX_SORTBY_NONE ) {
      // IMPORTANT: This needs to be set before we create the ranking condition below!
      param->sortspec = VGX_SORTBY_MEMADDRESS; // we do this to force deep counts
    }

    // ----
    // rank
    // ----
    // Disallow certain sortby
    if( param->implied.collector_mode == VGX_COLLECTOR_MODE_COLLECT_VERTICES && param->sortspec == VGX_SORTBY_PREDICATOR ) {
      PyVGXError_SetString( PyVGX_QueryError, "Invalid sortby specified: predicator value not available in this context" );
      THROW_SILENT( CXLIB_ERR_API, 0x003 );
    }

    if( (param->ranking_condition = iPyVGXParser.NewRankingConditionEx( param->implied.graph, py_rankspec, NULL, param->sortspec, VGX_PREDICATOR_MOD_NONE, py_rank_vector_object, param->vertex_condition )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }

    // ------
    // memory
    // ------
    if( py_evalmem && py_evalmem != Py_None ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
      }
    }
  }
  XCATCH( errcode ) {
    param = NULL;
  }
  XFINALLY {
  }

  return param;
}



/******************************************************************************
 * _pyvgx_Global
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Global( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, vgx_collector_mode_t mode ) {
  PyObject *py_global = NULL;

  __global_query_args param = {0};
  param.implied.collector_mode = mode;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    PyObject *py_timing = NULL;
    __PY_START_TIMED_BLOCK( &py_timing, "total" ) {
      
      // -------------------------
      // Parse Parameters
      // -------------------------
      if( _pyvgx_Global__parse_params( args, kwds, &param, false ) != NULL ) {

        // -------
        // Perform
        // -------
        py_global = _pyvgx_Global__perform( &param, &py_timing );
      }

      // -------------------------
      // Clean up
      // -------------------------
      _pyvgx_Global__clear_params( (__base_query_args*)&param );

    } __PY_END_TIMED_BLOCK;
  }

  return py_global;
}



/******************************************************************************
 * pyvgx_Vertices
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Vertices( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return _pyvgx_Global( pygraph, args, kwds, VGX_COLLECTOR_MODE_COLLECT_VERTICES );
}



/******************************************************************************
 * pyvgx_Arcs
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Arcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return _pyvgx_Global( pygraph, args, kwds, VGX_COLLECTOR_MODE_COLLECT_ARCS );
}



/******************************************************************************
 * _pyvgx_Global__perform
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Global__perform( __global_query_args *param, PyObject **py_timing ) {

  PyObject *py_result = NULL;

  // -------------------------
  // Execute
  // -------------------------
  vgx_SearchResult_t *search_result = NULL;
  BEGIN_PYVGX_THREADS {
    int64_t n = 0;
    // Construct query
    vgx_GlobalQuery_t *query = _pyvgx_Global__get_global_query( param );
    if( query ) {
      // Execute query
      switch( param->implied.collector_mode ) {
      case VGX_COLLECTOR_MODE_COLLECT_ARCS:
        n = CALLABLE( param->implied.graph )->simple->Arcs( param->implied.graph, query );
        break;
      case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
        n = CALLABLE( param->implied.graph )->simple->Vertices( param->implied.graph, query );
        break;
      default:
        break;
      }

      if( n < 0 ) {
        PyVGX_CAPTURE_QUERY_ERROR( query, param );
      }
      // Steal the result from the query
      else {
        search_result = CALLABLE( query )->YankSearchResult( query );
      }
      // Delete query
      iGraphQuery.DeleteGlobalQuery( &query );
    }
  } END_PYVGX_THREADS;

  // -----------------------------------------------
  // Build Python response object from search result
  // -----------------------------------------------
  if( search_result ) {
    py_result = _pyvgx_Global__get_result( search_result, param->implied.collector_mode, py_timing );
    BEGIN_PYVGX_THREADS {
      iGraphResponse.DeleteSearchResult( &search_result );
    } END_PYVGX_THREADS;
  }

  // -------------------------
  // Handle error
  // -------------------------
  if( py_result == NULL ) {
    PyVGX_SET_QUERY_ERROR( NULL, param, NULL );
  }

  return py_result;
}



/******************************************************************************
 * _pyvgx_Global__get_global_query
 *
 ******************************************************************************
 */
static vgx_GlobalQuery_t * _pyvgx_Global__get_global_query( __global_query_args *param ) { 

  vgx_GlobalQuery_t *query = NULL;

  XTRY {

    // Construct neighborhood query object (steals collect_condition_set)
    if( (query = iGraphQuery.NewGlobalQuery( param->implied.graph, param->implied.collector_mode, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
    CALLABLE( query )->SetResponseFormat( query, param->result_format );
    query->hits = param->hits;
    query->offset = param->offset;
    CALLABLE( query )->SetTimeout( query, param->timeout_ms, param->limexec > 0 );

    CALLABLE( query )->SetDebug( query, param->implied.__debug );

    // Evaluator Memory (query owns +1 ref)
    if( param->evalmem ) {
      CALLABLE( query )->SetMemory( query, param->evalmem );
    }

    // Assign vertex condition (steal)
    if( param->vertex_condition ) {
      CALLABLE( query )->AddVertexCondition( query, &param->vertex_condition );
    }

    // Assign ranking condition (steal)
    if( param->ranking_condition ) {
      CALLABLE( query )->AddRankingCondition( query, &param->ranking_condition );
    }

    // Select statement
    if( param->select_statement ) {
      if( CALLABLE( query )->SelectStatement( query, param->implied.graph, param->select_statement, &param->implied.CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x002 );
      }
    }

    // Debug pre
    if( query->debug & VGX_QUERY_DEBUG_QUERY_PRE ) {
      PRINT( query );
    }

  }
  XCATCH( errcode ) {
    param->implied.py_err_class = PyVGX_QueryError;
    iGraphQuery.DeleteGlobalQuery( &query );
  }
  XFINALLY {
  }

  return query;
}



/******************************************************************************
 * _pyvgx_Global__get_result
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Global__get_result( vgx_SearchResult_t *search_result, vgx_collector_mode_t mode, PyObject **py_timing ) {
  PyObject *py_global_result = NULL;

  PyObject *py_result_objects;
  vgx_SearchResult_t *SR = search_result;
  if( (py_result_objects = iPyVGXSearchResult.PyResultList_FromSearchResult( SR, false, -1 )) != NULL ) {
    // Returned object will be a dict
    if( SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_METAS ) {
      static const char *label_arcs = "arcs";
      static const char *label_vertices = "vertices";
      const char *label = NULL;
      int64_t n_items = 0;
      switch( mode ) {
      case VGX_COLLECTOR_MODE_COLLECT_ARCS:
        label = label_arcs;
        n_items = search_result->total_arcs;
        break;
      case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
        label = label_vertices;
        n_items = search_result->total_vertices;
        break;
      default:
        break;
      }

      if( label && (py_global_result = PyDict_New()) != NULL ) {
        // Add the result list under appropriate key ("arcs" or "vertices") 
        iPyVGXBuilder.DictMapStringToPyObject( py_global_result, label, &py_result_objects );
        // Add counts
        if( SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_COUNTS ) {
          PyObject *py_counts;
          if( (py_counts = PyDict_New()) != NULL ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_counts, label, n_items );
            iPyVGXBuilder.DictMapStringToPyObject( py_global_result, "counts", &py_counts );
          }
        }
        // Add timing
        if( py_timing && SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_TIMING ) {
          PyObject *py_tdict = PyDict_New();
          if( py_tdict ) {
            *py_timing = py_tdict; // BORROWED REF
            iPyVGXBuilder.DictMapStringToFloat( *py_timing, "search", SR->exe_time.t_search );
            iPyVGXBuilder.DictMapStringToFloat( *py_timing, "result", SR->exe_time.t_result );
            if( iPyVGXBuilder.DictMapStringToPyObject( py_global_result, "time", &py_tdict ) < 0 ) {
              *py_timing = NULL;
            }
          }
        }
      }
      else {
        PyVGX_DECREF( py_result_objects ); // error cleanup
      }
    }
    // Returned object will be a simple list
    else {
      py_global_result = py_result_objects;
    }
  }

  return py_global_result;
}



/******************************************************************************
 * pyvgx_NewGlobalQuery
 *
 ******************************************************************************
 */
static PyObject * pyvgx_NewGlobalQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, vgx_collector_mode_t mode ) {
  vgx_Graph_t *graph;
  if( (graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) == NULL ) {
    return NULL;
  }

  PyVGX_Query *py_query = NULL;
  __global_query_args *param = NULL;
  vgx_GlobalQuery_t *query = NULL;

  XTRY {

    // New args
    if( (param = PyVGX_NewQueryArgs( __global_query_args, graph, VGX_ARCDIR_ANY, mode, _pyvgx_Global__clear_params )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Parse Parameters
    if( _pyvgx_Global__parse_params( args, kwds, param, true ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Construct query
    if( (query = _pyvgx_Global__get_global_query( param )) == NULL ) {
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
      iGraphQuery.DeleteGlobalQuery( &query );
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
 * pyvgx_NewArcsQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_NewArcsQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewGlobalQuery( pygraph, args, kwds, VGX_COLLECTOR_MODE_COLLECT_ARCS );
}



/******************************************************************************
 * pyvgx_NewVerticesQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_NewVerticesQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  return pyvgx_NewGlobalQuery( pygraph, args, kwds, VGX_COLLECTOR_MODE_COLLECT_VERTICES );
}



/******************************************************************************
 * pyvgx_ExecuteGlobalQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_ExecuteGlobalQuery( PyVGX_Query *py_query ) {

  PyObject *py_global_result = NULL;
  vgx_GlobalQuery_t *query = (vgx_GlobalQuery_t*)py_query->query;

  if( py_query->qtype != VGX_QUERY_TYPE_GLOBAL || query == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "internal error, invalid query" );
    return NULL;
  }

  __global_query_args *param = (__global_query_args*)py_query->p_args;
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
    int64_t n_hits = 0;

    BEGIN_PYVGX_THREADS {
      // Skip query execution if query was already executed and nothing has changed
      if( query->search_result && __query_result_cache_valid__NOGIL( py_query ) ) {
        // TODO: What about queries with side-effects such as running evaluator
        //       expressions that are expected to modify a memory object, etc.
        //       We can't silently skip such queries.
        //       We should detect if query may have such side effects and
        //       disable caching.
        static const vgx_ExecutionTime_t zero_exe_time = {0};
        query->exe_time = zero_exe_time;
      }
      // Run query
      else {
        // First clean up any previous result object and collector object since we will generate a new result object by running the query again
        iGraphQuery.EmptyGlobalQuery( query );

        // Execute
        switch( param->implied.collector_mode ) {
        case VGX_COLLECTOR_MODE_COLLECT_ARCS:
          n_hits = CALLABLE( graph )->simple->Arcs( graph, query );
          break;
        case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
          n_hits = CALLABLE( graph )->simple->Vertices( graph, query );
          break;
        default:
          break;
        }

        if( n_hits < 0 ) {
          PyVGX_CAPTURE_QUERY_ERROR( query, param );
        }
        
        // Set cache opid
        __set_query_cache( py_query );
      }
    } END_PYVGX_THREADS;

    // -----------------------------------------------
    // Build Python response object from search result
    // -----------------------------------------------
    if( n_hits >= 0 && query->search_result ) {
      query->search_result->exe_time = query->exe_time;
      py_global_result = _pyvgx_Global__get_result( query->search_result, param->implied.collector_mode, &py_timing );
    }

  } __PY_END_TIMED_BLOCK;


  // -------------------------
  // Handle error
  // -------------------------
  if( py_global_result == NULL ) {
    PyVGX_SET_QUERY_ERROR( py_query, param, NULL );
  }

  iString.Discard( &param->implied.CSTR__error );

  return py_global_result;
}
