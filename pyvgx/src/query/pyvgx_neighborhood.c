/*######################################################################
 *#
 *# pyvgx_Neighborhood.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );




/******************************************************************************
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Neighborhood__perform( __neighborhood_query_args *param, PyObject **py_timing );
static vgx_NeighborhoodQuery_t * _pyvgx_Neighborhood__get_neighborhood_query( __neighborhood_query_args *param );
static PyObject * _pyvgx_Neighborhood__get_neighborhood_result( vgx_SearchResult_t *search_result, bool nested, int64_t nested_hits, PyObject **py_timing );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Neighborhood__prepare_nested_query( int nesting, PyObject *py_arc_condition, PyObject *py_filter_expr, PyObject *py_vertex_condition ) {

  PyObject *py_vertex = NULL;
  PyObject *py_arc = NULL;
  PyObject *py_assert = NULL;
  PyObject *py_filter = NULL;
  PyObject *py_traverse = NULL;
  PyObject *py_neighbor = NULL;
  PyObject *py_collect = NULL;

  int err = 0;
  XTRY {

    if( nesting > 0 ) {

      if( py_vertex_condition ) {
        py_vertex = py_vertex_condition;
      }
      else {
        if( (py_vertex = PyDict_New()) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }
      }

      // Add 'traverse' if not already present
      if( (py_traverse = PyDict_GetItemString( py_vertex, "traverse" )) == NULL ) {
        if( (py_traverse = PyDict_New()) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
        err = PyDict_SetItemString( py_vertex, "traverse", py_traverse );
        Py_DECREF( py_traverse ); // borrow
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
      }

      // Default 'traverse': { 'collect':True }
      if( !PyDict_GetItemString( py_traverse, "collect" ) ) {
        // Use current level's shortcut collect (will be moved to traverse dict)
        if( (py_collect = PyDict_GetItemString( py_vertex, "collect" )) != NULL ) {
          Py_INCREF( py_collect );
          PyDict_DelItemString( py_vertex, "collect" );
        }
        else {
          py_collect = Py_True;
          Py_INCREF( py_collect );
        }

        err = PyDict_SetItemString( py_traverse, "collect", py_collect );
        Py_DECREF( py_collect );
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }
      }

      // Default 'traverse': { 'assert': True }
      if( (py_assert = PyDict_GetItemString( py_traverse, "assert" )) == NULL ) {
        // Use current level's shortcut collect (will be moved to traverse dict)
        if( (py_assert = PyDict_GetItemString( py_vertex, "assert" )) != NULL ) {
          Py_INCREF( py_assert );
          PyDict_DelItemString( py_vertex, "assert" );
        }
        else {
          py_assert = Py_True;
          Py_INCREF( py_assert );
        }

        err = PyDict_SetItemString( py_traverse, "assert", py_assert );
        Py_DECREF( py_assert );
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }
      }

      // Default 'traverse': { 'arc': INHERIT_PREVIOUS_LEVEL }
      if( (py_arc = PyDict_GetItemString( py_traverse, "arc" )) == NULL ) {
        // Use current level's shortcut arc (will be moved to traverse dict)
        if( (py_arc = PyDict_GetItemString( py_vertex, "arc" )) != NULL ) {
          Py_INCREF( py_arc );
          PyDict_DelItemString( py_vertex, "arc" );
        }
        // Use previous level's arc
        else if( py_arc_condition ) {
          py_arc = py_arc_condition;
          Py_INCREF( py_arc );
        }
        // Create new arc:D_OUT
        else if( (py_arc = PyLong_FromLong( VGX_ARCDIR_OUT )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }

        err = PyDict_SetItemString( py_traverse, "arc", py_arc );
        Py_DECREF( py_arc ); // borrow
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
        }
      }

      // Default 'traverse': { 'filter': INHERIT_PREVIOUS_LEVEL }
      if( ((py_filter = PyDict_GetItemString( py_traverse, "filter" )) == NULL) && py_filter_expr != NULL ) {
        py_filter = py_filter_expr; // borrow
        err = PyDict_SetItemString( py_traverse, "filter", py_filter ); // becomes owner
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
        }
      }

      // Default 'traverse': { 'neighbor': ??? }
      if( (py_neighbor = PyDict_GetItemString( py_traverse, "neighbor" )) == NULL ) {
        // Use current level's shortcut neighbor (will be moved to traverse dict)
        if( (py_neighbor = PyDict_GetItemString( py_vertex, "neighbor" )) != NULL ) {
          Py_INCREF( py_neighbor );
          PyDict_DelItemString( py_vertex, "neighbor" );
        }
        // Create new neighbor:{}
        else if( (py_neighbor = PyDict_New()) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
        }
        err = PyDict_SetItemString( py_traverse, "neighbor", py_neighbor );
        Py_DECREF( py_neighbor ); // borrow
        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
        }
      }

      // Recurse in
      if( _pyvgx_Neighborhood__prepare_nested_query( nesting-1, py_arc, py_filter, py_neighbor ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
      }

    }
    // End of recursion
    else {
      py_vertex = py_vertex_condition;
    }

  }
  XCATCH( errcode ) {
    if( py_vertex != py_vertex_condition ) {
      Py_XDECREF( py_vertex );
      py_vertex = NULL;
    }
  }
  XFINALLY {
  }

  return py_vertex;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_Neighborhood__clear_params( __base_query_args *base ) {
  if( base ) {
    __neighborhood_query_args *param = (__neighborhood_query_args*)base;
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }
    if( param->vertex_condition ) {
      iVertexCondition.Delete( &param->vertex_condition );
    }
    if( param->ranking_condition ) {
      iRankingCondition.Delete( &param->ranking_condition );
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



PyVGX_DOC( pyvgx_Neighborhood__doc__,
  "Neighborhood( id, arc=(None,D_OUT), pre=None, filter=None, post=None, neighbor=\"*\", vector=[], collect=C_COLLECT, result=R_STR, fields=F_ID, nest=0, nested_hits=-1, select=None, rank=None, sortby=S_NONE, aggregate=None, memory=4, offset=0, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Perform a neighborhood search around vertex 'id'.\n"
  "\n"
);
static __neighborhood_query_args * _pyvgx_Neighborhood__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __neighborhood_query_args *param, bool reusable ) {
  static char *fmt = "|OOz#z#z#OOOIIiLz#OIOOiLiii";
  static char *kwlist[] = {
    "id",         //  O
    "arc",        //  O
    "pre",        //  z
    "filter",     //  z
    "post",       //  z
    "neighbor",   //  O
    "vector",     //  O
    "collect",    //  O
    "result",     //  I
    "fields",     //  I
    "nest",       //  i
    "nested_hits",//  L
    "select",     //  z
    "rank",       //  O
    "sortby",     //  I
    "aggregate",  //  O
    "memory",     //  O
    "offset",     //  i
    "hits",       //  L
    "timeout",    //  i
    "limexec",    //  i
    "__debug",    //  i
    NULL
  };
  static char *fmt_reusable = "|OOz#z#z#OOOIIiLz#OIOOi";
  static char *kwlist_reusable[] = {
    "id",         //  O
    "arc",        //  O
    "pre",        //  z
    "filter",     //  z
    "post",       //  z
    "neighbor",   //  O
    "vector",     //  O
    "collect",    //  O
    "result",     //  I
    "fields",     //  I
    "nest",       //  i
    "nested_hits",//  L
    "select",     //  z
    "rank",       //  O
    "sortby",     //  I
    "aggregate",  //  O
    "memory",     //  O
    "__debug",    //  i
    NULL
  };


  // Set nonzero defaults
  param->result_format = VGX_RESPONSE_SHOW_AS_STRING;
  param->hits = -1;
  param->nested_hits = -1;

  int64_t sz_pre = 0;
  int64_t sz_filter = 0;
  int64_t sz_post = 0;
  int64_t sz_select = 0;

  PyObject *py_anchor = NULL;
  PyObject *py_arc_condition = NULL;
  PyObject *py_vertex_condition = NULL;
  PyObject *py_rank_vector_object = NULL;
  PyObject *py_rankspec = NULL;
  PyObject *py_aggregate = NULL;
  PyObject *py_collect = NULL;
  PyObject *py_evalmem = NULL;

  PyObject *py_filter = NULL;
  
  if( reusable ) {
    // Parse, reusable context
    if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt_reusable, kwlist_reusable,
      &py_anchor,                     // O id
      &py_arc_condition,              // O arc
      &param->pre_expr,               // z pre
      &sz_pre         ,               // # pre
      &param->filter_expr,            // z filter
      &sz_filter,                     // # filter
      &param->post_expr,              // z post
      &sz_post,                       // # post
      &py_vertex_condition,           // O neighbor
      &py_rank_vector_object,         // O vector
      &py_collect,                    // O collect
      &param->result_format,          // I result
      &param->result_attrs,           // I fields
      &param->nest,                   // i nest
      &param->nested_hits,            // L nested_hits
      &param->select_statement,       // z select
      &sz_select,                     // # select
      &py_rankspec,                   // O rank
      &param->sortspec,               // I sortby
      &py_aggregate,                  // O aggregate
      &py_evalmem,                    // O memory
      &param->implied.__debug )
    )
    {
      return NULL;
    }
  }
  else {
    // Parse
    if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
      &py_anchor,                     // O id
      &py_arc_condition,              // O arc
      &param->pre_expr,               // z pre
      &sz_pre         ,               // # pre
      &param->filter_expr,            // z filter
      &sz_filter,                     // # filter
      &param->post_expr,              // z post
      &sz_post,                       // # post
      &py_vertex_condition,           // O neighbor
      &py_rank_vector_object,         // O vector
      &py_collect,                    // O collect
      &param->result_format,          // I result
      &param->result_attrs,           // I fields
      &param->nest,                   // i nest
      &param->nested_hits,            // L nested_hits
      &param->select_statement,       // z select
      &sz_select,                     // # select
      &py_rankspec,                   // O rank
      &param->sortspec,               // I sortby
      &py_aggregate,                  // O aggregate
      &py_evalmem,                    // O memory
      &param->offset,                 // i offset
      &param->hits,                   // L hits
      &param->timeout_ms,             // i timeout
      &param->limexec,                // i limexec
      &param->implied.__debug )
    )
    {
      return NULL;
    }
  }

  PyObject *py_vertex_condition_orig = py_vertex_condition;
  XTRY {

    bool nested = param->nest > 0;
    if( nested ) {
      if( param->filter_expr ) {
        if( (py_filter = PyUnicode_FromStringAndSize( param->filter_expr, sz_filter )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
        }
      }
      if( (py_vertex_condition = _pyvgx_Neighborhood__prepare_nested_query( param->nest, py_arc_condition, py_filter, py_vertex_condition_orig )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }
      if( param->implied.__debug > 0 ) {
        printf( "\nAUTO NEIGHBOR CONDITION:\nneighbor = " );
        PyObject_Print( py_vertex_condition, stdout, 0 );
        printf( "\n\n" );
      }
    }


    // --
    // id
    // --
    if( py_anchor && py_anchor != Py_None ) {
      if( iPyVGXParser.GetVertexID( pygraph, py_anchor, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }
    }

    // ---
    // arc
    // ---
    if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_arc_condition, param->implied.default_arcdir )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }
    param->modifier = iArcConditionSet.Modifier( param->arc_condition_set );

    // --------
    // neighbor
    // --------
    if( py_vertex_condition ) {
      if( (param->vertex_condition = iPyVGXParser.NewVertexCondition( param->implied.graph, py_vertex_condition, param->implied.collector_mode )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
      }
    }

    // ------
    // result
    // fields
    // ------

    // No fields specified, default to vertex id
    if( param->select_statement == NULL && param->result_attrs == VGX_RESPONSE_ATTRS_NONE ) {
      param->result_attrs = VGX_RESPONSE_ATTR_ID;
    }

    // Nested results, require at least anchor, arc value and id, and force DICT response entries
    if( nested ) { 
      param->result_attrs |= VGX_RESPONSE_ATTR_ANCHOR | VGX_RESPONSE_ATTR_RELTYPE | VGX_RESPONSE_ATTR_VALUE | VGX_RESPONSE_ATTR_ID;
      param->result_format &= ~VGX_RESPONSE_SHOW_AS_MASK;
      param->result_format |= VGX_RESPONSE_SHOW_AS_DICT;
    }

    // Add the fields to the result format
    param->result_format |= param->result_attrs;

    // Default show result entries as strings
    if( vgx_response_show_as(param->result_format) == VGX_RESPONSE_SHOW_AS_NONE ) {
      param->result_format |= VGX_RESPONSE_SHOW_AS_STRING;
    }

    // Special handling of sorting if counts are requested
    if( (param->result_format & VGX_RESPONSE_SHOW_WITH_COUNTS) && param->sortspec == VGX_SORTBY_NONE ) {
      // IMPORTANT: This needs to be set before we create the ranking condition below!
      param->sortspec = VGX_SORTBY_MEMADDRESS; // we do this to force deep counts
    }

    // ----
    // rank
    // ----
    if( (param->ranking_condition = iPyVGXParser.NewRankingConditionEx( param->implied.graph, py_rankspec, py_aggregate, param->sortspec, param->modifier, py_rank_vector_object, param->vertex_condition )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
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
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x007 );
        }
      }
      else {
        if( (param->collect_arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_collect, VGX_ARCDIR_ANY )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x008 );
        }
      }
    }

    // ------
    // memory
    // ------
    if( py_evalmem && py_evalmem != Py_None ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x009 );
      }
    }
  }
  XCATCH( errcode ) {
    param = NULL;
  }
  XFINALLY {
    if( py_vertex_condition != py_vertex_condition_orig ) {
      Py_XDECREF( py_vertex_condition );
    }
    Py_XDECREF( py_filter );
  }

  return param;
}



PyVGX_DOC( pyvgx_Terminals__doc__,
  "Terminals( id, pre=None, filter=None, post=None, neighbor=\"*\", rank=None, sortby=S_NONE, memory=4, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Return a simple list of terminal vertices for which given vertex is an initial.\n"
  "\n"
);
PyVGX_DOC( pyvgx_Initials__doc__, 
  "Initials( id, pre=None, filter=None, post=None, neighbor=\"*\", rank=None, sortby=S_NONE, memory=4, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Return a simple list of initial vertices for which given vertex is a terminal.\n"
  "\n"
);
static __neighborhood_query_args * _pyvgx_initials_terminals__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __neighborhood_query_args *param ) {
  static char *kwlist[] = {
    "id",
    "pre",
    "filter",
    "post",
    "neighbor",
    "rank",
    "sortby",
    "memory",
    "hits",
    "timeout",
    "limexec",
    "__debug",
    NULL
  };

  static char *fmt = "O|z#z#z#OOIOLiii";

  // Set nonzero defaults
  param->result_format = VGX_RESPONSE_SHOW_AS_STRING;
  param->result_attrs = VGX_RESPONSE_ATTR_ID;
  param->hits = -1;

  int64_t sz_pre = 0;
  int64_t sz_filter = 0;
  int64_t sz_post = 0;

  PyObject *py_anchor = NULL;
  PyObject *py_vertex_condition = NULL;
  PyObject *py_rankspec = NULL;
  PyObject *py_evalmem = NULL;

  // Parse   
  if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
    &py_anchor,                     // O id
    // ------------------------------------------
    &param->pre_expr,               // z pre
    &sz_pre         ,               // # pre
    &param->filter_expr,            // z filter
    &sz_filter,                     // # filter
    &param->post_expr,              // z post
    &sz_post,                       // # post
    &py_vertex_condition,           // O neighbor
    &py_rankspec,                   // O rank
    &param->sortspec,               // I sortby
    &py_evalmem,                    // O memory
    &param->hits,                   // L hits
    &param->timeout_ms,             // i timeout
    &param->limexec,                // i limexec
    &param->implied.__debug )
  )
  {
    return NULL;
  }
  else {

    // --
    // id
    // --
    if( iPyVGXParser.GetVertexID( pygraph, py_anchor, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
      return NULL;
    }

    // ---
    // arc
    // ---
    if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, NULL, param->implied.default_arcdir )) == NULL ) {
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

    // ----
    // rank
    // ----
    if( (param->ranking_condition = iPyVGXParser.NewRankingConditionEx( param->implied.graph, py_rankspec, NULL, param->sortspec, param->modifier, NULL, param->vertex_condition )) == NULL ) {
      return NULL;
    }

    if( param->sortspec & VGX_SORTBY_PREDICATOR ) {
      PyErr_SetString( PyVGX_QueryError, "Sort by arc value not supported for this method" );
      return NULL;
    }

    param->result_format |= param->result_attrs;

    // Interpret evalmem
    if( py_evalmem && py_evalmem != Py_None ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        return NULL;
      }
    }

    return param;
  }
}



PyVGX_DOC( pyvgx_Outarcs__doc__,
  "Outarcs( id, hits=-1, timeout=0, limexec=False ) -> Simple list of arcs originating at vertex.\n"
  "\n"
  "This is a simplified version of the Neighborhood() method.\n"
  "\n"
);
PyVGX_DOC( pyvgx_Inarcs__doc__,
  "Inarcs( id, hits=-1, timeout=0, limexec=False ) -> Simple list of arcs terminating at vertex.\n"
  "\n"
  "This is a simplified version of the Neighborhood() method.\n"
  "\n"
);
static __neighborhood_query_args * _pyvgx_inarcs_outarcs__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __neighborhood_query_args *param ) {
  static char *kwlist[] = {
    "id",
    "hits",
    "timeout",
    "limexec",
    "__debug",
    NULL
  };

  static char *fmt = "O|Liii";

  // Set nonzero defaults
  param->result_format = VGX_RESPONSE_SHOW_AS_LIST;
  param->result_attrs = VGX_RESPONSE_ATTRS_ARC;
  param->hits = -1;

  PyObject *py_anchor = NULL;

  // Parse   
  if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt, kwlist,
    &py_anchor,                     // O id
    // ------------------------------------------
    &param->hits,                   // L hits
    &param->timeout_ms,             // i timeout
    &param->limexec,                // i limexec
    &param->implied.__debug )
  )
  {
    return NULL;
  }
  else {
    if( iPyVGXParser.GetVertexID( pygraph, py_anchor, &param->anchor, NULL, true, "Vertex ID" ) < 0 ) {
      return NULL;
    }

    if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, NULL, param->implied.default_arcdir )) == NULL ) {
      return NULL;
    }
    param->modifier = iArcConditionSet.Modifier( param->arc_condition_set );

    if( (param->ranking_condition = iPyVGXParser.NewRankingCondition( param->implied.graph, NULL, NULL, VGX_SORTBY_NONE, param->modifier, NULL )) == NULL ) {
      return NULL;
    }

    param->result_format |= param->result_attrs;
    return param;
  }
}



/******************************************************************************
 * pyvgx_Neighborhood
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Neighborhood( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_neighborhood = NULL;

  __neighborhood_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    PyObject *py_timing = NULL;
    __PY_START_TIMED_BLOCK( &py_timing, "total" ) {
      
      // -------------------------
      // Parse Parameters
      // -------------------------
      if( _pyvgx_Neighborhood__parse_params( pygraph, args, kwds, &param, false ) != NULL ) {

        // -------
        // Perform
        // -------
        py_neighborhood = _pyvgx_Neighborhood__perform( &param, &py_timing );
      }

      // -------------------------
      // Clean up
      // -------------------------
      _pyvgx_Neighborhood__clear_params( (__base_query_args*)&param );

    } __PY_END_TIMED_BLOCK;
  }

  return py_neighborhood;
}



/******************************************************************************
 * pyvgx_Initials
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Initials( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_initials = NULL;

  __neighborhood_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_IN;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_VERTICES;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_initials_terminals__parse_params( pygraph, args, kwds, &param ) != NULL ) {
      
      // -------
      // Perform
      // -------
      py_initials = _pyvgx_Neighborhood__perform( &param, NULL );
    }

    // -------------------------
    // Cleanup
    // -------------------------
    _pyvgx_Neighborhood__clear_params( (__base_query_args*)&param );

  }

  return py_initials;
}



/******************************************************************************
 * pyvgx_Terminals
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Terminals( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_terminals = NULL;

  __neighborhood_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_VERTICES;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_initials_terminals__parse_params( pygraph, args, kwds, &param ) != NULL ) {

      // -------
      // Perform
      // -------
      py_terminals = _pyvgx_Neighborhood__perform( &param, NULL );
    }

    // -------------------------
    // Cleanup
    // -------------------------
    _pyvgx_Neighborhood__clear_params( (__base_query_args*)&param );

  }

  return py_terminals;
}



/******************************************************************************
 * pyvgx_Inarcs
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Inarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_inarcs = NULL;

  __neighborhood_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_IN;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_inarcs_outarcs__parse_params( pygraph, args, kwds, &param ) != NULL ) {

      // -------
      // Perform
      // -------
      py_inarcs = _pyvgx_Neighborhood__perform( &param, NULL );
    }

    // -------------------------
    // Cleanup
    // -------------------------
    _pyvgx_Neighborhood__clear_params( (__base_query_args*)&param );

  }

  return py_inarcs;
}



/******************************************************************************
 * pyvgx_Outarcs
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_Outarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  PyObject *py_outarcs = NULL;

  __neighborhood_query_args param = {0};
  param.implied.default_arcdir = VGX_ARCDIR_OUT;
  param.implied.collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
  param.implied.py_err_class = PyExc_Exception;

  if( (param.implied.graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) != NULL ) {

    // -------------------------
    // Parse Parameters
    // -------------------------
    if( _pyvgx_inarcs_outarcs__parse_params( pygraph, args, kwds, &param ) != NULL ) {

      // -------
      // Perform
      // -------
      py_outarcs = _pyvgx_Neighborhood__perform( &param, NULL );
    }

    // -------------------------
    // Cleanup
    // -------------------------
    _pyvgx_Neighborhood__clear_params( (__base_query_args*)&param );

  }

  return py_outarcs;
}



/******************************************************************************
 * _pyvgx_Neighborhood__perform
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Neighborhood__perform( __neighborhood_query_args *param, PyObject **py_timing ) {

  PyObject *py_result = NULL;

  // -------------------------
  // Execute
  // -------------------------
  vgx_SearchResult_t *search_result = NULL;
  BEGIN_PYVGX_THREADS {
    // Construct query
    vgx_NeighborhoodQuery_t *query = _pyvgx_Neighborhood__get_neighborhood_query( param );
    if( query ) {
      // Execute query
      if( CALLABLE( param->implied.graph )->simple->Neighborhood( param->implied.graph, query ) < 0 ) {
        PyVGX_CAPTURE_QUERY_ERROR( query, param );
      }
      // Steal the result from the query
      else {
        search_result = CALLABLE( query )->YankSearchResult( query );
      }
      // Delete query
      iGraphQuery.DeleteNeighborhoodQuery( &query );
    }
  } END_PYVGX_THREADS;

  // -----------------------------------------------
  // Build Python response object from search result
  // -----------------------------------------------
  if( search_result ) {
    bool nested = param->nest > 0;
    py_result = _pyvgx_Neighborhood__get_neighborhood_result( search_result, nested, param->nested_hits, py_timing );
    BEGIN_PYVGX_THREADS {
      iGraphResponse.DeleteSearchResult( &search_result );
    } END_PYVGX_THREADS;
  }

  // -------------------------
  // Handle error
  // -------------------------
  if( py_result == NULL ) {
    PyVGX_SET_QUERY_ERROR( NULL, param, param->anchor.id );
  }

  return py_result;
}



/******************************************************************************
 * _pyvgx_Neighborhood__get_neighborhood_query
 *
 ******************************************************************************
 */
static vgx_NeighborhoodQuery_t * _pyvgx_Neighborhood__get_neighborhood_query( __neighborhood_query_args *param ) { 

  vgx_NeighborhoodQuery_t *query = NULL;

  XTRY {

    // Construct neighborhood query object (steals collect_condition_set)
    if( (query = iGraphQuery.NewNeighborhoodQuery( param->implied.graph, param->anchor.id, &param->collect_arc_condition_set, param->implied.collector_mode, &param->implied.CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC82 );
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

    // Assign arc condition set (steal)
    if( param->arc_condition_set ) {
      CALLABLE( query )->AddArcConditionSet( query, &param->arc_condition_set );
    }

    // Assign pre-filter expression
    if( param->pre_expr ) {
      if( CALLABLE( query )->AddPreFilter( query, param->pre_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC85 );
      }
    }

    // Assign filter expression
    if( param->filter_expr ) {
      if( CALLABLE( query )->AddFilter( query, param->filter_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC86 );
      }
    }

    // Assign post-filter expression
    if( param->post_expr ) {
      if( CALLABLE( query )->AddPostFilter( query, param->post_expr ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC87 );
      }
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
        THROW_SILENT( CXLIB_ERR_API, 0xC88 );
      }
    }

    // If multiple fields and predicator info requested and we're showing string entries, make sure the arc direction is included
    if( POPCNT32( vgx_response_attrs( query->fieldmask ) ) > 1 && (query->fieldmask & VGX_RESPONSE_ATTRS_PREDICATOR) && vgx_response_show_as_string( query->fieldmask ) ) {
      query->fieldmask |= VGX_RESPONSE_ATTR_ARCDIR;
    }

    // Debug pre
    if( query->debug & VGX_QUERY_DEBUG_QUERY_PRE ) {
      PRINT( query );
    }

  }
  XCATCH( errcode ) {
    param->implied.py_err_class = PyVGX_QueryError;
    iGraphQuery.DeleteNeighborhoodQuery( &query );
  }
  XFINALLY {
  }

  return query;
}



/******************************************************************************
 * _pyvgx_Neighborhood__get_neighborhood_result
 *
 ******************************************************************************
 */
static PyObject * _pyvgx_Neighborhood__get_neighborhood_result( vgx_SearchResult_t *search_result, bool nested, int64_t nested_hits, PyObject **py_timing ) {
  PyObject *py_neighborhood = NULL;

  PyObject *py_result_objects;
  vgx_SearchResult_t *SR = search_result;
  if( (py_result_objects = iPyVGXSearchResult.PyResultList_FromSearchResult( SR, nested, nested_hits )) != NULL ) {
    // Returned object will be a dict
    if( SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_METAS ) {

      if( (py_neighborhood = PyDict_New()) != NULL ) {
        // Add neighborhood
        iPyVGXBuilder.DictMapStringToPyObject( py_neighborhood, "neighborhood", &py_result_objects );
        // Add counts
        if( SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_COUNTS ) {
          PyObject *py_counts;
          if( (py_counts = PyDict_New()) != NULL ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_counts, "neighbors", SR->total_neighbors );
            iPyVGXBuilder.DictMapStringToLongLong( py_counts, "arcs", SR->total_arcs );
            if( SR->n_excluded ) {
              iPyVGXBuilder.DictMapStringToLongLong( py_counts, "excluded_arcs", SR->n_excluded );
            }
            iPyVGXBuilder.DictMapStringToPyObject( py_neighborhood, "counts", &py_counts );
          }
        }
        // Add timing
        if( py_timing && SR->list_fields.fastmask & VGX_RESPONSE_SHOW_WITH_TIMING ) {
          PyObject *py_tdict = PyDict_New();
          if( py_tdict ) {
            *py_timing = py_tdict; // BORROWED REF
            iPyVGXBuilder.DictMapStringToFloat( *py_timing, "search", SR->exe_time.t_search );
            iPyVGXBuilder.DictMapStringToFloat( *py_timing, "result", SR->exe_time.t_result );
            if( iPyVGXBuilder.DictMapStringToPyObject( py_neighborhood, "time", &py_tdict ) < 0 ) {
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
      py_neighborhood = py_result_objects;
    }
  }

  return py_neighborhood;
}



/******************************************************************************
 * pyvgx_NewNeighborhoodQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_NewNeighborhoodQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
  vgx_Graph_t *graph;
  if( (graph = __PyVGX_Graph_as_vgx_Graph_t( pygraph )) == NULL ) {
    return NULL;
  }

  PyVGX_Query *py_query = NULL;
  __neighborhood_query_args *param = NULL;
  vgx_NeighborhoodQuery_t *query = NULL;

  XTRY {

    // New args
    if( (param = PyVGX_NewQueryArgs( __neighborhood_query_args, graph, VGX_ARCDIR_OUT, VGX_COLLECTOR_MODE_COLLECT_ARCS, _pyvgx_Neighborhood__clear_params )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Parse Parameters
    if( _pyvgx_Neighborhood__parse_params( pygraph, args, kwds, param, true ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Construct query
    if( (query = _pyvgx_Neighborhood__get_neighborhood_query( param )) == NULL ) {
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
      iGraphQuery.DeleteNeighborhoodQuery( &query );
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
 * pyvgx_ExecuteNeighborhoodQuery
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_ExecuteNeighborhoodQuery( PyVGX_Query *py_query ) {

  PyObject *py_neighborhood = NULL;
  vgx_NeighborhoodQuery_t *query = (vgx_NeighborhoodQuery_t*)py_query->query;

  if( py_query->qtype != VGX_QUERY_TYPE_NEIGHBORHOOD || query == NULL ) {
    PyErr_SetString( PyVGX_QueryError, "internal error, invalid query" );
    return NULL;
  }

  __neighborhood_query_args *param = (__neighborhood_query_args*)py_query->p_args;
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
        iGraphQuery.EmptyNeighborhoodQuery( query );

        // Execute
        if( (n_hits = CALLABLE( graph )->simple->Neighborhood( graph, query )) < 0 ) {
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
      bool nested = param->nest > 0;
      query->search_result->exe_time = query->exe_time;
      py_neighborhood = _pyvgx_Neighborhood__get_neighborhood_result( query->search_result, nested, param->nested_hits, &py_timing );
    }

  } __PY_END_TIMED_BLOCK;


  // -------------------------
  // Handle error
  // -------------------------
  if( py_neighborhood == NULL ) {
    param->anchor.id = query->CSTR__anchor_id ? CStringValue( query->CSTR__anchor_id ) : "?";
    PyVGX_SET_QUERY_ERROR( py_query, param, param->anchor.id );
  }

  iString.Discard( &param->implied.CSTR__error );

  return py_neighborhood;
}







