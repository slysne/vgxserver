/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_ArcValue.c
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


PyVGX_DOC( pyvgx_ArcValue__doc__,
  "ArcValue( initial, arc, terminal, timeout=0 ) -> number\n"
  "\n"
  "Return the value of the specified arc from initial to terminal vertex\n"
  "\n"
);


typedef struct __s_arcvalue_pyargs {
  pyvgx_VertexIdentifier_t initial;                   // O id -> parsed from pyobject to string
  vgx_ArcConditionSet_t *arc_condition_set;           // O arc -> parsed into vgx_ArcConditionSet_t
  pyvgx_VertexIdentifier_t terminal;                  // O id -> parsed from pyobject to string
  // --------------------------------------------------
  int timeout_ms;                                     // i timeout
  // EXTRA
  struct {
    vgx_Graph_t *graph;                               // graph
    vgx_arc_direction default_arcdir;                 // default_arcdir
    CString_t *CSTR__error;
    vgx_AccessReason_t reason;
    PyObject *py_err_class;
  } implied;

} __arcvalue_pyargs;



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_Relation_t * _pyvgx_ArcValue__get_arcvalue_relation( vgx_Graph_t *graph, const char *initial_id, const char *terminal_id, __arcvalue_pyargs *param );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _pyvgx_ArcValue__clear_params( __arcvalue_pyargs *param ) {
  if( param ) {
    if( param->arc_condition_set ) {
      iArcConditionSet.Delete( &param->arc_condition_set );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}



/******************************************************************************
 * _pyvgx_ArcValue__parse_params
 *
 ******************************************************************************
 */
static __arcvalue_pyargs * _pyvgx_ArcValue__parse_params( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames, __arcvalue_pyargs *param ) {

  static const char *kwlist[] = {
    "initial",
    "arc",
    "terminal",
    "timeout",
    NULL
  };

  union {
    PyObject *args[4];
    struct {
      PyObject *py_initial;
      PyObject *py_arc;
      PyObject *py_terminal;
      PyObject *py_timeout;
    };
  } vcargs = {0};

  PyVGX_Graph *pygraph;
  int ret;
  if( PyVGX_Vertex_CheckExact( self ) ) {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist+1, 3, vcargs.args+1 );
    pygraph = ((PyVGX_Vertex*)self)->pygraph;
    vcargs.py_initial = self;
  }
  else {
    ret = __parse_vectorcall_args( args, nargs, kwnames, kwlist, 4, vcargs.args );
    pygraph = (PyVGX_Graph*)self;
  }
  if( ret < 0 ) {
    return NULL;
  }

  // -------
  // initial
  // -------
  if( vcargs.py_initial == NULL || vcargs.py_initial == Py_None ) {
    PyVGX_ReturnError( PyExc_TypeError, "Initial required" );
  }
  if( iPyVGXParser.GetVertexID( pygraph, vcargs.py_initial, &param->initial, NULL, true, "Initial ID" ) < 0 ) {
    return NULL;
  }

  // ---
  // arc
  // ---
  if( vcargs.py_arc == NULL ) {
    PyVGX_ReturnError( PyExc_TypeError, "Arc required" );
  }
  if( (param->arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, vcargs.py_arc, param->implied.default_arcdir )) == NULL ) {
    return NULL;
  }

  // --------
  // terminal
  // --------
  if( vcargs.py_terminal == NULL ) {
    PyVGX_ReturnError( PyExc_TypeError, "Terminal required" );
  }
  if( iPyVGXParser.GetVertexID( pygraph, vcargs.py_terminal, &param->terminal, NULL, true, "Terminal ID" ) < 0 ) {
    return NULL;
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
 * pyvgx_ArcValue
 *
 ******************************************************************************
 */
//DLL_HIDDEN PyObject * pyvgx_ArcValue( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds ) {
DLL_HIDDEN extern PyObject * pyvgx_ArcValue( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  PyObject *py_value = NULL;

  __arcvalue_pyargs param = {0};
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
    if( _pyvgx_ArcValue__parse_params( self, args, nargs, kwnames, &param ) != NULL ) {

      // -------------------------
      // Execute
      // -------------------------

      vgx_ArcHead_t arc_head = {0};
      BEGIN_PYVGX_THREADS {
        vgx_Relation_t *relation = _pyvgx_ArcValue__get_arcvalue_relation( param.implied.graph, param.initial.id, param.terminal.id, &param );
        if( relation ) {
          arc_head = CALLABLE( param.implied.graph )->simple->ArcValue( param.implied.graph, relation, param.timeout_ms, &param.implied.reason );
          iRelation.Delete( &relation );
        }
      } END_PYVGX_THREADS;

      // ----------------------------
      // Build Python response object
      // ----------------------------
      if( arc_head.vertex ) {
        py_value = iPyVGXSearchResult.PyPredicatorValue_FromArcHead( &arc_head );
      }
      else if( !iPyVGXBuilder.SetPyErrorFromAccessReason( param.initial.id, param.implied.reason, &param.implied.CSTR__error ) ) {
        const char *errstr = param.implied.CSTR__error ? CStringValue( param.implied.CSTR__error ) : "no arc";
        PyErr_SetString( param.implied.py_err_class, errstr );
      }
    }

    // -------------------------
    // Clean up
    // -------------------------
    _pyvgx_ArcValue__clear_params( &param );

  }

  return py_value;
}



/******************************************************************************
 * _pyvgx_ArcValue__get_arcvalue_relation
 *
 ******************************************************************************
 */
static vgx_Relation_t * _pyvgx_ArcValue__get_arcvalue_relation( vgx_Graph_t *graph, const char *initial_id, const char *terminal_id, __arcvalue_pyargs *param ) { 
  vgx_Relation_t *relation = NULL;

  //
  if( param->arc_condition_set ) {
    // Reverse direction?
    if( param->arc_condition_set->arcdir == VGX_ARCDIR_IN ) {
      const char *temp = initial_id;
      initial_id = terminal_id;
      terminal_id = temp;
      param->arc_condition_set->arcdir = VGX_ARCDIR_OUT;
    }

    vgx_ArcCondition_t *arc_condition = param->arc_condition_set->set ? *param->arc_condition_set->set : NULL;
    
    // Set relation from vertices and (first) parsed arc
    if( arc_condition && arc_condition->vcomp == VGX_VALUE_ANY ) {
      vgx_predicator_modifier_enum modifier = arc_condition->modifier.bits;
      if( (relation = iRelation.New( graph, initial_id, terminal_id, NULL, modifier, NULL )) != NULL ) {
        iRelation.AddRelationship( relation, (CString_t**)&arc_condition->CSTR__relationship );
      }
      else {
        __set_error_string( &param->implied.CSTR__error, "internal error" );
        param->implied.py_err_class = PyVGX_QueryError;
      }
    }
    else {
      __set_error_string( &param->implied.CSTR__error, "Unsupported arc condition for this function" );
      param->implied.py_err_class = PyVGX_QueryError;
    }
  }

  return relation;
}
