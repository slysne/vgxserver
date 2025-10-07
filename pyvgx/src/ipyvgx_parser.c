/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    ipyvgx_parser.c
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
#include "_vxsim.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


static int _ipyvgx_parser__initialize( void );
static void _ipyvgx_parser__destroy( void );
static int _ipyvgx_parser__parse_external_map_elements( PyObject *py_elements, ext_vector_feature_t **parsed_elements );
static int _ipyvgx_parser__parse_external_euclidean_elements( PyObject *py_elements, float **parsed_elements );
static vgx_value_type_t __parse_arc_value( vgx_predicator_val_t *pval, const vgx_predicator_t predicator, PyObject *py_value );
static vgx_value_type_t __parse_float_value( vgx_predicator_val_t *pval, PyObject *py_value );
static vgx_value_type_t __parse_int32_value( vgx_predicator_val_t *pval, PyObject *py_value );
static int _ipyvgx_parser__get_vertex_id( PyVGX_Graph *py_graph, PyObject *py_vertex, pyvgx_VertexIdentifier_t *ident, vgx_Vertex_t **vertex, bool use_obid, const char *context );
static vgx_Relation_t * _ipyvgx_parser__new_relation( vgx_Graph_t *graph, const char *initial, PyObject *py_arc, const char *terminal );
static int __parse_arc_relationship( PyObject *py_arc_condition, const char **relationship );
static int __parse_arc_direction( PyObject *py_arc_condition, vgx_arc_direction *direction );
static int __parse_arc_modifier( PyObject *py_arc_condition, vgx_predicator_modifier_enum *modifier );
static int __parse_arc_comparison( PyObject *py_arc_condition, vgx_value_comparison *vcomp );
static int __set_default_arc_values( const vgx_value_comparison vcomp, DWORD *value1, DWORD *value2 );
static vgx_value_type_t __parse_value_tuple( PyObject *py_value, vgx_predicator_t *predA, vgx_predicator_t *predB, vgx_value_type_t require_type );
static vgx_value_type_t __parse_arc_values( PyObject *py_arc_condition, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp,  DWORD *value1, DWORD *value2 );
static vgx_ArcConditionSet_t * _ipyvgx_parser__new_arc_condition_set( vgx_Graph_t *graph, PyObject *py_arc_condition, vgx_arc_direction default_direction );
static bool __unwrap_signed_condition( bool *sign, PyObject **py_condition );
static vgx_value_comparison __comparison_code_static( PyObject *py_tuple, PyObject **py_value );
static vgx_VertexCondition_t * _ipyvgx_parser__new_vertex_condition( vgx_Graph_t *graph, PyObject *py_vertex_condition, vgx_collector_mode_t collector_mode );
static vgx_VertexCondition_t * __recursive_new_vertex_condition( vgx_Graph_t *graph, PyObject *py_vertex_condition, vgx_collector_mode_t collector_mode, int rcnt );
static framehash_cell_t * __new_aggregation_mode_keymap( framehash_dynamic_t *dyn );
static vgx_RankingCondition_t * _ipyvgx_parser__new_ranking_condition( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, vgx_Vector_t *probe_vector );
static vgx_RankingCondition_t * _ipyvgx_parser__new_ranking_condition_ex( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, PyObject *py_rank_vector_object, vgx_VertexCondition_t *vertex_condition );
static vgx_ExpressEvalMemory_t * _ipyvgx_parser__new_express_eval_memory( vgx_Graph_t *graph, PyObject *py_object );
static vgx_Vector_t * _ipyvgx_parser__internal_vector_from_pyobject( vgx_Similarity_t *simcontext, PyObject *py_object, PyObject *py_alpha, bool ephemeral );
static vgx_StringList_t * _ipyvgx_parser__new_string_list_from_vertex_pylist( PyObject *py_list );
static void _ipyvgx_parser__delete_string_list( CString_t ***list );
static framehash_cell_t * __new_vertex_condition_keymap( framehash_dynamic_t *dyn );
static vgx_value_comparison _ipyvgx_parser__parse_value_condition( PyObject *py_valcond, vgx_value_condition_t *value_condition, const vgx_value_constraint_t vconstraint, const vgx_value_comparison vcomp_default );




typedef enum __e_recursion_mode {
  RECURSIVE_TRAVERSAL,
  RECURSIVE_CONDITION
} __recursion_mode;


typedef enum __e_traversal_spec {
  TRAVERSAL_NONE      = 0x00,
  TRAVERSAL_EXPLICIT  = 0x01,
  TRAVERSAL_IMPLICIT  = 0x02,
  TRAVERSAL_ARC       = 0x10,
  TRAVERSAL_NEIGHBOR  = 0x20,
  TRAVERSAL_FILTER    = 0x40,
  TRAVERSAL_COLLECT   = 0x80
} __traversal_spec;


typedef struct __s_probe_condition_context_t {
  const char *name;
  vgx_Graph_t *graph;
  vgx_collector_mode_t collector_mode;
  __recursion_mode recursion_mode;
  int recursion_count;
  __traversal_spec spec;
} __probe_condition_context_t;


typedef int (*__set_probe_condition)( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );


static int __set_probe_condition__virtual(            PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__type(               PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__degree_general(     PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, vgx_arc_direction direction );
static int __set_probe_condition__degree(             PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__indegree(           PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context);
static int __set_probe_condition__outdegree(          PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context);
static int __set_probe_condition__similarity(         PyObject *py_similarity, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__id(                 PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__id_list(            PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__time_general(       PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, int64_t ref_ts );
static int __set_probe_condition__abstime(            PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__reltime(            PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__property(           PyObject *py_property_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__local_filter(       PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__post(               PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__filter(             PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );

static int __set_probe_condition__traverse(           PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__arc(                PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__neighbor(           PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__collect(            PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__assert(             PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );

static int __set_probe_condition__adjacent(           PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__adjacent_arc(       PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__adjacent_neighbor(  PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );
static int __set_probe_condition__adjacent_assert(    PyObject *py_value,     vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );


static int __set_probe_condition__recursive(          PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context );




static framehash_cell_t *g_vertex_condition_keymap = NULL;
static framehash_dynamic_t g_vertex_condition_keymap_dyn = {0};

static framehash_cell_t *g_aggregation_mode_keymap = NULL;
static framehash_dynamic_t g_aggregation_mode_keymap_dyn = {0};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_parser__initialize( void ) {
  int i = 0;
  if( !g_vertex_condition_keymap ) {
    if( (g_vertex_condition_keymap = __new_vertex_condition_keymap( &g_vertex_condition_keymap_dyn )) == NULL ) {
      return -1; // error
    }
    ++i;
  }

  if( !g_aggregation_mode_keymap ) {
    if( (g_aggregation_mode_keymap = __new_aggregation_mode_keymap( &g_aggregation_mode_keymap_dyn )) == NULL ) {
      return -1; // error
    }
    ++i;
  }

  return i;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _ipyvgx_parser__destroy( void ) {
  if( g_vertex_condition_keymap ) {
    iMapping.DeleteIntegerMap( &g_vertex_condition_keymap, &g_vertex_condition_keymap_dyn );
    iFramehash.dynamic.ClearDynamic( &g_vertex_condition_keymap_dyn );
  }
  if( g_aggregation_mode_keymap ) {
    iMapping.DeleteIntegerMap( &g_aggregation_mode_keymap, &g_aggregation_mode_keymap_dyn );
    iFramehash.dynamic.ClearDynamic( &g_aggregation_mode_keymap_dyn );
  }

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_parser__parse_external_euclidean_elements( PyObject *py_elements, float **parsed_elements ) {
  int64_t length = -1;
  
  BEGIN_PYTHON_INTERPRETER {
    if( py_elements == NULL ) {
      length = 0;
    }
    else if( PyTuple_Check(py_elements) || PyList_Check(py_elements) ) {
      XTRY {
        if( (length = PySequence_Size( py_elements )) > MAX_EUCLIDEAN_VECTOR_SIZE ) {
          PyErr_Format( PyExc_ValueError, "vector length %lld exceeds max euclidean vector length %d", length, MAX_EUCLIDEAN_VECTOR_SIZE );
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
        }
        if( (*parsed_elements = (float*)malloc( (length+1) * sizeof(float) )) == NULL ) {
          PyVGXError_SetString( PyExc_MemoryError, "failed to allocate external vector elements" );
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
        float *cursor = *parsed_elements;
        float *end = cursor + length;
        int64_t i = 0;
        while( cursor < end ) {
          PyObject *py_elem = PySequence_Fast_GET_ITEM( py_elements, i );
          double d = PyFloat_AsDouble( py_elem );
          if( d == -1.0 && PyErr_Occurred()) {
            PyVGXError_SetString( PyExc_ValueError, "element must be float" );
            THROW_SILENT( CXLIB_ERR_API, 0x003 );
          }
          *cursor++ = (float)d;
          ++i;
        }
      }
      XCATCH( errcode ) {
        if( *parsed_elements ) {
          free( *parsed_elements );
          *parsed_elements = NULL;
        }
        length = -1;
      }
      XFINALLY {}
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "list expected" );
      length = -1;
    }
  } END_PYTHON_INTERPRETER;

  return (int)length;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_parser__parse_external_map_elements( PyObject *py_elements, ext_vector_feature_t **parsed_elements ) {
  PyObject *py_elem;
  PyObject *py_term;
  PyObject *py_weight;
  const char *term;
  Py_ssize_t sz_term;
  Py_ssize_t _ign;
  int64_t length = -1;
  float weight;
  
  BEGIN_PYTHON_INTERPRETER {
    if( py_elements == NULL ) {
      length = 0;
    }
    else if( PyTuple_Check(py_elements) || PyList_Check(py_elements) ) {
      ext_vector_feature_t *cursor;
      XTRY {
        if( (length = PySequence_Size( py_elements )) > MAX_FEATURE_VECTOR_SIZE ) {
          length = MAX_FEATURE_VECTOR_SIZE;
        }
        if( (*parsed_elements = (ext_vector_feature_t*)malloc( (length+1) * sizeof(ext_vector_feature_t) )) == NULL ) {
          PyVGXError_SetString( PyExc_MemoryError, "failed to allocate external vector elements" );
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x701 );
        }
        cursor = *parsed_elements;
        for( int64_t i=0; i < length; i++, cursor++ ) { 
          py_elem = PySequence_Fast_GET_ITEM( py_elements, i );
          if( (PyTuple_Check( py_elem ) && PyTuple_GET_SIZE( py_elem ) == 2)
              ||
              (PyList_Check( py_elem ) && PyList_GET_SIZE( py_elem ) == 2) )
          {
            py_term = PySequence_Fast_GET_ITEM( py_elem, 0 );
            py_weight = PySequence_Fast_GET_ITEM( py_elem, 1 );

            if( (term = PyVGX_PyObject_AsUTF8AndSize( py_term, &sz_term, &_ign, NULL )) == NULL ) {
              PyVGXError_SetString( PyExc_ValueError, "term must be string or bytes-like object" );
              THROW_SILENT( CXLIB_ERR_API, 0x702 );
            }

            if( (weight = (float)PyFloat_AsDouble( py_weight )) == -1.0f && PyErr_Occurred() ) {
              PyVGXError_SetString( PyExc_ValueError, "weight must be numeric" );
              THROW_SILENT( CXLIB_ERR_API, 0x703 );
            }
            cursor->weight = _vxsim_quantize_weight( weight );
            if( sz_term > MAX_FEATURE_VECTOR_TERM_LEN ) {
              sz_term = MAX_FEATURE_VECTOR_TERM_LEN;
            }
            strncpy( cursor->term, term, sz_term );
            cursor->term[ sz_term ] = '\0';
          }
          else {
            PyVGXError_SetString( PyExc_ValueError, "element must be tuple: (term,weight)" );
            THROW_SILENT( CXLIB_ERR_API, 0x704 );
          }
        }
        cursor->term[0] = '\0'; // empty string marks end of element array
        cursor->weight = 0.0f;
      }
      XCATCH( errcode ) {
        if( *parsed_elements ) {
          free( *parsed_elements );
          *parsed_elements = NULL;
        }
        length = -1;
      }
      XFINALLY {}
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "list expected" );
      length = -1;
    }
  } END_PYTHON_INTERPRETER;

  return (int)length;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_type_t __parse_arc_value( vgx_predicator_val_t *pval, const vgx_predicator_t predicator, PyObject *py_value ) {
  vgx_value_type_t vtype = VGX_VALUE_TYPE_NULL;
  vgx_predicator_mod_t mod = predicator.mod;
  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      QWORD value = 0;
      void *pvalue = &value;

      switch( _vgx_predicator_value_range( NULL, NULL, mod.bits ) ) {
      case VGX_PREDICATOR_VAL_TYPE_NONE:
        if( PyLong_Check( py_value ) ) {
          *((uint32_t*)pvalue) = PyLong_AsUnsignedLong( py_value );
          mod.bits = VGX_PREDICATOR_MOD_UNSIGNED;
          vtype = VGX_VALUE_TYPE_INTEGER;
        }
        else if( PyFloat_Check( py_value ) ) {
          *((float*)pvalue) = (float)PyFloat_AS_DOUBLE( py_value );
          mod.bits = VGX_PREDICATOR_MOD_FLOAT;
          vtype = VGX_VALUE_TYPE_REAL;
        }
        else {
          PyVGXError_SetString( PyExc_ValueError, "Predicator value must be numeric" );
          THROW_SILENT( CXLIB_ERR_API, 0x711 );
        }
        break;
      case VGX_PREDICATOR_VAL_TYPE_INTEGER:
        if( PyLong_Check( py_value ) ) {
          *((int32_t*)pvalue) = PyLong_AS_LONG( py_value );
        }
        else if( PyFloat_Check( py_value ) ) {
          *((int32_t*)pvalue) = (int32_t)PyFloat_AS_DOUBLE( py_value );
        }
        else {
          PyVGXError_SetString( PyExc_ValueError, "Predicator value must be integer (32_bit) for this modifier" );
          THROW_SILENT( CXLIB_ERR_API, 0x712 );
        }
        vtype = VGX_VALUE_TYPE_INTEGER;
        break;
      case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
        {
          int64_t val;
          if( PyLong_Check( py_value ) ) {
            val = PyLong_AsLongLong( py_value );
          }
          else if( PyFloat_Check( py_value ) ) {
            double dval = PyFloat_AsDouble( py_value );
            val = (int64_t)dval;
          }
          else {
            PyVGXError_SetString( PyExc_ValueError, "Predicator value must be unsigned integer (32-bit) for this modifier" );
            THROW_SILENT( CXLIB_ERR_API, 0x713 );
          }
          if( mod.bits == VGX_PREDICATOR_MOD_TIME_EXPIRES ) {
            uint32_t tmx;
            // Relative expiration
            if( val < 0 ) {
              tmx = __SECONDS_SINCE_1970() - (int32_t)val; // future, val seconds from now
            }
            else {
              tmx = (int32_t)val;
            }
            *((uint32_t*)pvalue) = tmx;
          }
          else if( val >= 0 ) {
            *((uint32_t*)pvalue) = val <= UINT32_MAX ? (uint32_t)val : UINT32_MAX;
          }
          else {
            PyVGXError_SetString( PyExc_ValueError, "Predicator value must be unsigned integer (32-bit) for this modifier" );
            THROW_SILENT( CXLIB_ERR_API, 0x714 );
          }
          vtype = VGX_VALUE_TYPE_INTEGER;
        }
        break;
      case VGX_PREDICATOR_VAL_TYPE_REAL:
        if( (*((float*)pvalue) = (float)PyFloat_AsDouble( py_value )) == -1.0 && PyErr_Occurred() ) {
          PyVGXError_SetString( PyExc_ValueError, "Predicator value must be numeric for this modifier" );
          THROW_SILENT( CXLIB_ERR_API, 0x715 );
        }
        vtype = VGX_VALUE_TYPE_REAL;
        break;
      default:
        PyVGXError_SetString( PyVGX_ArcError, "Invalid modifier value" );
        THROW_SILENT( CXLIB_ERR_API, 0x716 );
      }

      // Validate the supplied value range for this modifier and place into pval
      if( iRelation.ParseModifierValue( mod.bits, &value, pval ) != 0 ) {
        PyVGXError_SetString( PyVGX_ArcError, "Value out of range for this modifier" );
        THROW_SILENT( CXLIB_ERR_API, 0x717 );
      }
    }
    XCATCH( errcode ) {
      vtype = VGX_VALUE_TYPE_NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return vtype;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_type_t __parse_float_value( vgx_predicator_val_t *pval, PyObject *py_value ) {
  vgx_value_type_t vtype = VGX_VALUE_TYPE_REAL;
  BEGIN_PYTHON_INTERPRETER {
    PyObject *py_float = NULL;
    XTRY {
      if( (py_float = PyNumber_Float( py_value)) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x721 );
      }
      pval->real = (float)PyFloat_AS_DOUBLE( py_float );
    }
    XCATCH( errcode ) {
      vtype = VGX_VALUE_TYPE_NULL;
    }
    XFINALLY {
      PyVGX_XDECREF( py_float );
    }
  } END_PYTHON_INTERPRETER;
  return vtype;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_type_t __parse_int32_value( vgx_predicator_val_t *pval, PyObject *py_value ) {
  vgx_value_type_t vtype = VGX_VALUE_TYPE_INTEGER;
  BEGIN_PYTHON_INTERPRETER {
    PyObject *py_int32 = NULL;
    XTRY {
      if( (py_int32 = PyNumber_Long( py_value)) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x731 );
      }
      int ovf;
      pval->integer = PyLong_AsLongAndOverflow( py_int32, &ovf );
      if( ovf != 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x732 );
      }
    }
    XCATCH( errcode ) {
      vtype = VGX_VALUE_TYPE_NULL;
    }
    XFINALLY {
      PyVGX_XDECREF( py_int32 );
    }
  } END_PYTHON_INTERPRETER;
  return vtype;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _ipyvgx_parser__get_vertex_id( PyVGX_Graph *py_graph, PyObject *py_vertex, pyvgx_VertexIdentifier_t *ident, vgx_Vertex_t **vertex, bool use_obid, const char *context ) {
  if( PyVGX_Vertex_CheckExact( py_vertex ) ) {
    // Safeguard graph-vertex match if graph provided
    if( py_graph == NULL || __get_vertex_graph( py_graph, (PyVGX_Vertex*)py_vertex ) != NULL ) {
      vgx_Vertex_t *V = ((PyVGX_Vertex*)py_vertex)->vertex;
      if( use_obid ) {
        ident->id = idtostr( ident->_idbuf, COMLIB_OBJECT_GETID( V ) );
        ident->len = 32;
      }
      else {
        ident->id = CALLABLE( V )->IDString( V );
        ident->len = -1;
      }
      if( vertex ) {
        *vertex = V;
      }
      return 0;
    }
  }
  else if( PyVGX_PyObject_CheckString( py_vertex ) ) {
    Py_ssize_t sz = 0;
    Py_ssize_t ucsz = 0;
    if( (ident->id = PyVGX_PyObject_AsUTF8AndSize( py_vertex, &sz, &ucsz, context )) != NULL ) {
      ident->len = (int)sz;
      return 0;
    }
  }
  else if( PyLong_CheckExact( py_vertex ) && py_graph && py_graph->graph && use_obid) {
    QWORD address = PyLong_AsLongLong( py_vertex );
    vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
    int ret;
    BEGIN_PYVGX_THREADS {
      vgx_Graph_t *graph = py_graph->graph;
      objectid_t obid = {0};
      if( (ret = CALLABLE( graph )->advanced->GetVertexInternalidByAddress( graph, address, &obid, &reason )) >= 0 ) {
        ident->id = idtostr( ident->_idbuf, &obid );
        ident->len = 32;
      }
    } END_PYVGX_THREADS;
    if( ret < 0 ) {
      iPyVGXBuilder.SetPyErrorFromAccessReason( NULL, reason, NULL );
    }
    else {
      return 0;
    }
  }
  else {
    if( context ) {
      PyErr_Format( PyExc_ValueError, "%s: must be a string, bytes-like object, or vertex instance", context );
    }
    else {
      PyErr_SetString( PyExc_ValueError, "Vertex identifier must be a string, bytes-like object, or vertex instance" );
    }
  }

  // error
  if( !PyErr_Occurred() ) {
    if( context ) {
      PyErr_Format( PyExc_ValueError, "Invalid %s", context );
    }
    else {
      PyErr_SetString( PyExc_ValueError, "Invalid vertex ID" );
    }
  }
  ident->id = NULL;
  return -1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_Relation_t * _ipyvgx_parser__new_relation( vgx_Graph_t *graph, const char *initial, PyObject *py_arc, const char *terminal ) {
  vgx_Relation_t *relation = NULL;

  const char *relationship = NULL;

  int mod_int = 0;

  // Create the relation object (with default arc)
  if( (relation = iRelation.New( graph, initial, terminal, NULL, VGX_PREDICATOR_MOD_STATIC, NULL )) == NULL ) {
    return NULL;
  }

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
   
      // Parse non-default relationship and update relation object as necessary
      if( py_arc && py_arc != Py_None ) {


        // SIMPLE STRING: arc is relationship string only
        if( PyVGX_PyObject_CheckString( py_arc ) ) {
          // A-[rel]->
          relationship = PyVGX_PyObject_AsString( py_arc );
        }
        // TUPLE: arc is a tuple (relationship, modifier=STATIC, value==)
        else if( PyTuple_Check( py_arc ) ) {
          int sz = (int)PyTuple_Size( py_arc );

          // A-[ rel, ?, ? ]->
          if( sz > 0 ) {
            vgx_predicator_val_t value = { .bits = VGX_PREDICATOR_VAL_ZERO };
            vgx_predicator_t predicator = {0};
            // Extract the relationship (None => NULL is default)
            PyObject *py_rel = PyTuple_GET_ITEM( py_arc, 0 );
            if( py_rel != Py_None ) {
              if( (relationship = PyVGX_PyObject_AsString( py_rel )) == NULL ) {
                THROW_SILENT( CXLIB_ERR_API, 0x741 );
              }
            }

            // A-[ rel, mod, ? ]->
            if( sz > 1 ) {
              // Extract the modifier
              mod_int = PyLong_AsLong( PyTuple_GET_ITEM( py_arc, 1 ) );
              if( mod_int == -1 && PyErr_Occurred() ) {
                THROW_SILENT( CXLIB_ERR_API, 0x742 );
              }
              predicator.mod.bits = (uint8_t)mod_int;

              // A-[rel, mod ]->
              // Set a default value if predicator is accumulator
              if( sz == 2 ) {
                // Set the default value to 1 if modifier is an accumulator type (default increment = 1)
                if( _vgx_predicator_value_is_accumulator( predicator ) ) {
                  vgx_predicator_val_type val_type = _vgx_predicator_value_range( NULL, NULL, predicator.mod.bits );
                  if( val_type == VGX_PREDICATOR_VAL_TYPE_REAL ) {
                    value.real = 1.0;
                  }
                  else {
                    value.integer = 1;
                  }
                }
              }

              // A-[rel, mod, val]->  
              // Parse the non-default value
              else if( sz == 3 ) {
                if( __parse_arc_value( &value, predicator, PyTuple_GET_ITEM( py_arc, 2 ) ) == VGX_VALUE_TYPE_NULL ) {
                  THROW_SILENT( CXLIB_ERR_GENERAL, 0x743 );
                }
              }

              // Too many elements in tuple
              else {
                PyVGXError_SetString( PyVGX_ArcError, "Invalid arc tuple, expected (relationship, modifier, value)" );
                THROW_SILENT( CXLIB_ERR_API, 0x744 );
              }
            }

            // Add the parsed modifier and value to relation
            if( iRelation.SetModifierAndValue( relation, predicator.mod.bits, &value.bits ) == NULL ) {
              PyVGXError_SetString( PyVGX_ArcError, "Invalid arc modifier" );
              THROW_SILENT( CXLIB_ERR_API, 0x745 );
            }
          }
        }
        // invalid arc object
        else {
          PyVGXError_SetString( PyVGX_ArcError, "Invalid arc, must be string or tuple" );
          THROW_SILENT( CXLIB_ERR_API, 0x746 );
        }
      }
      else {
      }

    }
    XCATCH( errcode ) {
      iRelation.Delete( &relation );
    }
    XFINALLY {
    }

  } END_PYTHON_INTERPRETER;


  if( relation ) {
    if( iRelation.SetRelationship( relation, relationship ) == NULL ) {
      iRelation.Delete( &relation );
    }

    // Forward only arc
    if( (mod_int & _VGX_PREDICATOR_MOD_FORWARD_ONLY) ) {
      iRelation.ForwardOnly( relation );
    }
    // Automatic timestamps?
    else if( _auto_arc_timestamps || (mod_int & _VGX_PREDICATOR_MOD_AUTO_TM) ) {
      iRelation.AutoTimestamps( relation );
    }
  }

  return relation;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __parse_arc_relationship( PyObject *py_arc_condition, const char **relationship ) {
#define __ARG_RELATIONSHIP 0
  int ret = 0;
  BEGIN_PYTHON_INTERPRETER {
    // Extract the relationship (None => NULL is default)
    PyObject *py_rel = PyTuple_GET_ITEM( py_arc_condition, __ARG_RELATIONSHIP );
    if( py_rel != Py_None ) {
      if( (*relationship = PyVGX_PyObject_AsString( py_rel )) == NULL ) {
        ret = -1;
      }
    }
  } END_PYTHON_INTERPRETER;

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __parse_arc_direction( PyObject *py_arc_condition, vgx_arc_direction *direction ) {
#define __ARG_DIRECTION    1
  int ret = 0;
  BEGIN_PYTHON_INTERPRETER {
    // Extract the direction
    int dir_int = PyLong_AsLong( PyTuple_GET_ITEM( py_arc_condition, __ARG_DIRECTION ) );
    if( dir_int == -1 && PyErr_Occurred() ) {
      ret = -1; // error
    }
    else {
      // Validate direction
      if( !_vgx_arcdir_valid( dir_int ) ) {
        PyVGXError_SetString( PyVGX_QueryError, "Invalid arc direction" );
        ret = -1; // error
      }
      else {
        // Set direction
        *direction = dir_int;
      }
    }
  } END_PYTHON_INTERPRETER;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __parse_arc_modifier( PyObject *py_arc_condition, vgx_predicator_modifier_enum *modifier ) {
#define __ARG_MODIFIER     2
  int ret = 0;
  BEGIN_PYTHON_INTERPRETER {
    int mod_int;
    // Extract the modifier
    if( (mod_int = PyLong_AsLong( PyTuple_GET_ITEM( py_arc_condition, __ARG_MODIFIER ) )) == -1 && PyErr_Occurred() ) {
      ret = -1; // error
    }
    else {
      *modifier = mod_int;
      // Validate modifier
      if( !_vgx_modifier_is_valid( *modifier ) ) {
        PyVGXError_SetString( PyVGX_QueryError, "Invalid arc modifier" );
        ret = -1;
      }
    }
  } END_PYTHON_INTERPRETER;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __parse_arc_comparison( PyObject *py_arc_condition, vgx_value_comparison *vcomp ) {
#define __ARG_CONDITION_1  3
  int ret = 0;
  BEGIN_PYTHON_INTERPRETER {
    int cond_int;
    // Extract the condition
    if( (cond_int = PyLong_AsLong( PyTuple_GET_ITEM( py_arc_condition, __ARG_CONDITION_1 ) )) == -1 && PyErr_Occurred() ) {
      ret = -1; // error;
    }
    else {
      // Set and validate condition code
      *vcomp = cond_int;
      if( !_vgx_is_valid_value_comparison( *vcomp ) ) {
        PyVGXError_SetString( PyVGX_QueryError, "Invalid arc value comparison code" );
        ret = -1; // error
      }
    }
  } END_PYTHON_INTERPRETER;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int __set_default_arc_values( const vgx_value_comparison vcomp, DWORD *value1, DWORD *value2 ) {
  int ret = 0;
  BEGIN_PYTHON_INTERPRETER {
    //if( _vgx_is_basic_value_comparison( vcomp ) || _vgx_is_value_hamdist_comparison( vcomp ) ) {
    if( _vgx_is_basic_value_comparison( vcomp ) ) {
      PyVGXError_SetString( PyVGX_QueryError, "Arc comparator requires value" );
      ret = -1;
    }
    else {
      if( _vgx_is_value_range_comparison( vcomp ) ) {
        PyVGXError_SetString( PyVGX_QueryError, "Arc range comparator requires value range as 2-tuple" );
        ret = -1;
      }
      else {
        if( _vgx_is_dynamic_value_comparison( vcomp ) ) {
          // dynamic filter value deltas default to 0
          *value1 = 0;
          *value2 = 0;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_type_t __parse_value_tuple( PyObject *py_value, vgx_predicator_t *predA, vgx_predicator_t *predB, vgx_value_type_t require_type ) {
  vgx_value_type_t vtypeA = VGX_VALUE_TYPE_NULL;
  vgx_value_type_t vtypeB = VGX_VALUE_TYPE_NULL;
  BEGIN_PYTHON_INTERPRETER {
    if( PyTuple_Check( py_value ) && PyTuple_GET_SIZE( py_value ) == 2 ) {
      PyObject *pyA = PyTuple_GET_ITEM( py_value, 0 );
      PyObject *pyB = PyTuple_GET_ITEM( py_value, 1 );
      if( require_type != VGX_VALUE_TYPE_NULL ) {
        switch( require_type ) {
        case VGX_VALUE_TYPE_INTEGER:
          vtypeA = __parse_int32_value( &predA->val, pyA );
          vtypeB = __parse_int32_value( &predB->val, pyB );
          break;
        case VGX_VALUE_TYPE_REAL:
          vtypeA = __parse_float_value( &predA->val, pyA );
          vtypeB = __parse_float_value( &predB->val, pyB );
          break;
        default:
          PyVGXError_SetString( PyVGX_QueryError, "Illegal value type" );
        }
      }
      else {
        vtypeA = __parse_arc_value( &predA->val, *predA, pyA );
        vtypeB = __parse_arc_value( &predB->val, *predB, pyB );
      }
      if( vtypeA != VGX_VALUE_TYPE_NULL && vtypeB != VGX_VALUE_TYPE_NULL ) {
        if( vtypeA != vtypeB && !PyErr_Occurred() ) {
          PyVGXError_SetString( PyVGX_QueryError, "Values in tuple must have the same type" );
          vtypeA = vtypeB = VGX_VALUE_TYPE_NULL;
        }
      }
    }
  } END_PYTHON_INTERPRETER;
  return vtypeA;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_type_t __parse_arc_values( PyObject *py_arc_condition, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp,  DWORD *value1, DWORD *value2 ) {
#define __ARG_VALUE_1      4
#define __ARG_VALUE_2      5
  vgx_value_type_t vtype = VGX_VALUE_TYPE_NULL;
  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      PyObject *py_value = PyTuple_GET_ITEM( py_arc_condition, __ARG_VALUE_1 );

      vgx_predicator_t pred1 = VGX_PREDICATOR_NONE;
      pred1.mod.bits = modifier;

      // ... (value1, value2) ...
      if( _vgx_is_value_range_comparison( vcomp ) ) {
        vgx_predicator_t pred2 = pred1;
        // Make sure range is given as (low,high)
        if( !PyTuple_Check( py_value ) || PyTuple_GET_SIZE( py_value ) != 2 ) {
          PyVGXError_SetString( PyVGX_QueryError, "Invalid arc value range: Must be 2-tuple" );
          THROW_SILENT( CXLIB_ERR_API, 0x751 );
        }

        // Static range
        // RANGE or NRANGE
        if( vcomp == VGX_VALUE_RANGE || vcomp == VGX_VALUE_NRANGE ) {
          if( (vtype = __parse_value_tuple( py_value, &pred1, &pred2, VGX_VALUE_TYPE_NULL )) == VGX_VALUE_TYPE_NULL ) {
            THROW_SILENT( CXLIB_ERR_API, 0x752 );
          }
        }
        // Dynamic delta range
        // VGX_VALUE_DYN_RANGE
        else if( vcomp == VGX_VALUE_DYN_RANGE ) {
          vgx_value_type_t require_type = pred1.mod.probe.f ? VGX_VALUE_TYPE_REAL : VGX_VALUE_TYPE_INTEGER;
          if( (vtype = __parse_value_tuple( py_value, &pred1, &pred2, require_type )) == VGX_VALUE_TYPE_NULL ) {
            THROW_SILENT( CXLIB_ERR_API, 0x753 );
          }
        }
        // Dynamic ratio range
        // VGX_VALUE_DYN_RANGE_R
        else {
          if( (vtype = __parse_value_tuple( py_value, &pred1, &pred2, VGX_VALUE_TYPE_REAL )) != VGX_VALUE_TYPE_REAL ) {
            if( !PyErr_Occurred() ) {
              PyVGXError_SetString( PyVGX_QueryError, "Float values are required" );
            }
            THROW_SILENT( CXLIB_ERR_API, 0x754 );
          }
        }

        // Set the values
        *value1 = pred1.val.bits;
        *value2 = pred2.val.bits;
      }
      // ... (value1, value2) ...
      else if( modifier == VGX_PREDICATOR_MOD_LSH ) {
        // Make sure tuple (bits, distance)
        if( !PyTuple_Check( py_value ) || PyTuple_GET_SIZE( py_value ) != 2 ) {
          PyVGXError_SetString( PyVGX_QueryError, "Invalid LSH arc probe: (bits, distance) required" );
          THROW_SILENT( CXLIB_ERR_API, 0x755 );
        }
        // Extract pred1, pred2 = (bits, distance)
        vgx_predicator_t pred2 = VGX_PREDICATOR_NONE;
        if( (vtype = __parse_value_tuple( py_value, &pred1, &pred2, VGX_VALUE_TYPE_NULL )) != VGX_VALUE_TYPE_INTEGER ) {
          if( !PyErr_Occurred() ) {
            PyVGXError_SetString( PyVGX_QueryError, "Integer values are required" );
          }
          THROW_SILENT( CXLIB_ERR_API, 0x756 );
        }
        if( pred2.val.integer < 0 || pred2.val.integer > 15 ) {
          PyVGXError_SetString( PyVGX_QueryError, "LSH hamming distance must be 0 - 15" );
          THROW_SILENT( CXLIB_ERR_API, 0x757 );
        }

        // Set the values
        *value1 = pred1.val.bits;
        *value2 = pred2.val.bits;
      }
      // ... value1 ...
      else {
        if( (vtype = __parse_arc_value( &pred1.val, pred1, py_value )) == VGX_VALUE_TYPE_NULL ) {
          THROW_SILENT( CXLIB_ERR_API, 0x758 );
        }

        // Set the value
        *value1 = pred1.val.bits;
      }
    }
    XCATCH( errcode ) {
      vtype = VGX_VALUE_TYPE_NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return vtype;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_ArcConditionSet_t * _ipyvgx_parser__new_arc_condition_set( vgx_Graph_t *graph, PyObject *py_arc_condition, vgx_arc_direction default_direction ) {

  // Quick empty condition set by default
  if( py_arc_condition == NULL ) {
    return iArcConditionSet.NewEmpty( graph, true, default_direction );
  }


  vgx_ArcConditionSet_t *arc_condition_set = NULL;

  // Default arc condition elements
  vgx_arc_direction direction = default_direction;
  const char *relationship = NULL;
  vgx_predicator_modifier_enum modifier = VGX_PREDICATOR_MOD_WILDCARD;
  vgx_value_comparison vcomp = VGX_VALUE_ANY;
  DWORD value1 = 0;
  DWORD value2 = 0;
  DWORD *pvalue1 = NULL;
  DWORD *pvalue2 = NULL;

  int err = 0;

  /*
  Possible valid python objects for the arc condition:

  <arc_condition>
    None                                  # Wildcard (i.e. matches all arcs)
    True                                  # Wildcard (i.e. matches all arcs)
    ()                                    # Wildcard (i.e. matches all arcs)
    False                                 # Inverted wildcard (i.e. matches no arcs)
    <rel>                                 # Any outarc with relationship type <rel> (wildcard if <rel>=="*")
    <dir>                                 # Any arc with direction <dir>
    (<rel>)                               # Any arc with relationship type <rel> (wildcard if <rel>=="*")
    (<rel>, <dir>)                        # Any arc with relationship type <rel> AND direction <dir>
    (<rel>, <dir>, <mod>)                 # Any arc with relationship type <rel> AND direction <dir> AND modifier <mod>
    (<rel>, <dir>, <mod>, <cond>, <val>)  # Any arc with relationship type <rel> AND direction <dir> AND modifier <mod> AND value <val> satisfying condition <cond>

  and any of the above wrapped in another tuple for positive/negative
  ( <bool>, <arc_condition> )   # When <bool> is True, matches if <arc_condition> is met (default)
                                # When <bool> is False, matches if <arc_condition> is not met



  */

  bool accept_set = true;
  bool positive_arc = true;

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      bool parsing = true;

      while( parsing ) {

        // Arc is specified, now parse it
        if( py_arc_condition != NULL && py_arc_condition != Py_None ) {
          // STRING: arc is relationship string only, outbound arc, any modifier, any value
          // <rel>
          // anchor-[<rel>,M_STAT,*]->*
          if( PyVGX_PyObject_CheckString( py_arc_condition ) ) {
            relationship = PyVGX_PyObject_AsString( py_arc_condition );
          }

          // BOOLEAN: arc will either match everything or nothing
          else if( PyBool_Check( py_arc_condition ) ) {
            accept_set = py_arc_condition == Py_True;
          }

          // INTEGER: arc is direction code only, any relationship, any modifier, any value
          else if( PyLong_CheckExact( py_arc_condition ) ) {
            direction = PyLong_AsLong( py_arc_condition );
            if( !_vgx_arcdir_valid( direction ) ) {
              PyVGXError_SetString( PyVGX_QueryError, "Invalid arc direction" );
              THROW_SILENT( CXLIB_ERR_API, 0x761 );
            }
          }
          
          // TUPLE: arc is a tuple (relationship, direction=BOTH, modifier=*, condition=*, value=*)
          // (relationship, direction, modifier, condition, value)
          // ?<-[?,?,?]-anchor-[?,?,?]->?
          else if( PyTuple_Check( py_arc_condition ) ) {
            int sz = (int)PyTuple_Size( py_arc_condition );

            // ( bool, ... )
            // Unpack the positive/negative condition from the outer wrapper
            if( sz == 2 && PyBool_Check( PyTuple_GET_ITEM( py_arc_condition, 0 ) ) ) {
              if( PyTuple_GET_ITEM( py_arc_condition, 0 ) == Py_False ) {
                positive_arc = !positive_arc;
              }
              // unwrap condition
              py_arc_condition = PyTuple_GET_ITEM( py_arc_condition, 1 );
              continue; // back to the top
            }
         
            // ( relationship, ?, ?, ?, ? )
            if( sz >= 1 ) {
              if( __parse_arc_relationship( py_arc_condition, &relationship ) < 0 ) {
                THROW_SILENT( CXLIB_ERR_API, 0x762 );
              }

              // ( relationship, direction, ?, ?, ? )
              if( sz >= 2 ) {
                if( __parse_arc_direction( py_arc_condition, &direction ) < 0 ) {
                  THROW_SILENT( CXLIB_ERR_API, 0x763 );
                }

                // ( relationship, direction, modifier, ?, ? )
                if( sz >= 3 ) {
                  if( __parse_arc_modifier( py_arc_condition, &modifier ) < 0 ) {
                    THROW_SILENT( CXLIB_ERR_API, 0x764 );
                  }

                  // ( relationship, direction, modifier, condition, ? )
                  if( sz >= 4 ) {
                    if( modifier == VGX_PREDICATOR_MOD_NONE ) {
                      PyVGXError_SetString( PyVGX_QueryError, "Invalid arc: value condition not supported without modifier" );
                      THROW_SILENT( CXLIB_ERR_API, 0x765 );
                    }

                    if( __parse_arc_comparison( py_arc_condition, &vcomp ) < 0 ) {
                      THROW_SILENT( CXLIB_ERR_API, 0x766 );
                    }

                    pvalue1 = &value1;
                    pvalue2 = &value2;
                    
                    if( sz == 4 ) {
                      if( __set_default_arc_values( vcomp, pvalue1, pvalue2 ) < 0 ) {
                        THROW_SILENT( CXLIB_ERR_API, 0x767 );
                      }

                    }
                    // ( relationship, direction, modifier, condition, value )
                    // ( relationship, direction, modifier, condition, (value1,value2) )
                    else if( sz >= 5 ) {
                      if( sz == 5 ) {
                        if( __parse_arc_values( py_arc_condition, modifier, vcomp, pvalue1, pvalue2 ) == VGX_VALUE_TYPE_NULL ) {
                          THROW_SILENT( CXLIB_ERR_API, 0x768 );
                        }
                      } // end sz == 5
                      else {
                        PyVGXError_SetString( PyVGX_QueryError, "Invalid arc: too many values" );
                        THROW_SILENT( CXLIB_ERR_API, 0x769 );
                      }
                    } // end sz >= 5
                  } // end sz >= 4
                } // end sz >= 3
              } // end sz >= 2
            } // end sz >= 1
          } // end arc is TUPLE

          // Invalid arc
          else {
            PyVGXError_SetString( PyVGX_QueryError, "Invalid arc" );
            THROW_SILENT( CXLIB_ERR_API, 0x76A );
          }
        } // end py_arc_condition is specified
        
        // *<-[*,*,*]-anchor-[*,*,*]->*
        else {
          ; // do nothing
        }

        // parsing complete
        parsing = false;
      }
    }
    XCATCH( errcode ) {
      if( !PyErr_Occurred() ) {
        PyErr_SetString( PyExc_Exception, "internal error" );
      }
      err = -1;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  // Create the new arc condition set from parsed elements
  if( err == 0 ) {
    if( relationship != NULL || modifier != VGX_PREDICATOR_MOD_WILDCARD || vcomp != VGX_VALUE_ANY ) {
      arc_condition_set = iArcConditionSet.NewSimple( graph, direction, positive_arc, relationship, modifier, vcomp, pvalue1, pvalue2 );
    }
    else {
      arc_condition_set = iArcConditionSet.NewEmpty( graph, accept_set, direction );
    }
  }

  return arc_condition_set;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static bool __unwrap_signed_condition( bool *sign, PyObject **py_condition ) {
  // Condition wrapped in positive/negative tuple
  if( PyTuple_Check( *py_condition ) && PyTuple_Size( *py_condition ) == 2 && PyBool_Check( PyTuple_GET_ITEM( *py_condition, 0 ) ) ) {
    if( PyTuple_GET_ITEM( *py_condition, 0 ) == Py_False ) {
      *sign = *sign ? false : true; // <- INVERT the matching logic
    }
    // unwrap
    *py_condition = PyTuple_GET_ITEM( *py_condition, 1 );
    return true;
  }
  // No wrapping, no action taken
  else {
    return false;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_comparison __comparison_code_static( PyObject *py_tuple, PyObject **py_value ) {
  vgx_value_comparison vcomp = VGX_VALUE_NONE;
  XTRY {
    // Make sure it's a tuple (VCOMP, value)
    if( PyTuple_Size( py_tuple ) != 2 ) {
      PyVGXError_SetString( PyVGX_QueryError, "condition must be 2-tuple" );
      THROW_SILENT( CXLIB_ERR_API, 0x771 );
    }
    // Get comparison code
    if( (vcomp = PyLong_AsLong( PyTuple_GET_ITEM( py_tuple, 0 ) )) == -1 && PyErr_Occurred() ) {
      THROW_SILENT( CXLIB_ERR_API, 0x772 );
    }

    // Extract the value
    *py_value = PyTuple_GET_ITEM( py_tuple, 1 );

    // Range comparison code
    if( vcomp == VGX_VALUE_RANGE || vcomp == VGX_VALUE_NRANGE ) {
      if( !PyTuple_Check( *py_value ) || PyTuple_GET_SIZE( *py_value ) != 2 ) {
        PyVGXError_SetString( PyVGX_QueryError, "range comparison requires value tuple (min, max)" );
        THROW_SILENT( CXLIB_ERR_API, 0x773 );
      }
    }
    // Make sure non-range comparison code is valid
    else if( vcomp > VGX_VALUE_MASK || (vcomp & 1) ) {
      PyVGXError_SetString( PyVGX_QueryError, "invalid value comparison code" );
      THROW_SILENT( CXLIB_ERR_API, 0x775 );
    }

  }
  XCATCH( errcode ) {
    vcomp = VGX_VALUE_NONE;
    *py_value = NULL;
  }
  XFINALLY {
  }
  return vcomp;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __bool_condition( PyObject *py_value, bool *parsed_value ) {
  // Single value, default cmp
  if( !PySequence_Check(py_value) ) {
    if( PyObject_IsTrue(py_value) ) {
      *parsed_value = true;
    }
    else {
      *parsed_value = false;
    }
    return 0;
  }
  else {
    PyVGXError_SetString( PyVGX_QueryError, "invalid condition, must be boolean" );
    return -1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_value_comparison __string_condition( PyObject *py_value, const char **parsed_value, vgx_value_comparison vcomp_default ) {
  vgx_value_comparison vcomp;

  // None is treated as NULL-pointer
  if( py_value == Py_None ) {
    *parsed_value = NULL;
    vcomp = vcomp_default;
  }
  // Single value, default cmp
  else if( PyVGX_PyObject_CheckString( py_value ) ) {
    *parsed_value =  PyVGX_PyObject_AsString( py_value );
    vcomp = vcomp_default;
  }
  // Tuple value (cmp,val)
  else if( PyTuple_Check( py_value ) ) {
    PyObject *py_string;
    if( (vcomp = __comparison_code_static( py_value, &py_string )) != VGX_VALUE_NONE ) {
      if( __string_condition( py_string, parsed_value, vcomp ) == VGX_VALUE_NONE ) {
        return VGX_VALUE_NONE; // error
      }
    }
  }
  else {
    PyVGXError_SetString( PyVGX_QueryError, "invalid condition, must be string or (comparison, string)" );
    return VGX_VALUE_NONE; // error
  }

  // Check wildcard
  if( *parsed_value != NULL && **parsed_value == '*' && *(*parsed_value+1) == '\0' ) {
    *parsed_value = NULL;
    vcomp = VGX_VALUE_ANY;
  }

  return vcomp;
}



/******************************************************************************
 *
 *
 *
 * LEAK WARNING: If value condition is a string the caller WILL OWN A CString_t
 *               which is placed in value_condition->value1.data.simple.CSTR__string !!!
 ******************************************************************************
 */
static vgx_value_comparison _ipyvgx_parser__parse_value_condition( PyObject *py_valcond, vgx_value_condition_t *value_condition, const vgx_value_constraint_t value_constraint, const vgx_value_comparison vcomp_default ) {

  vgx_value_comparison vcomp = vcomp_default;

  BEGIN_PYTHON_INTERPRETER {

    // Any value
    if( py_valcond == Py_None ) {
      value_condition->vcomp = VGX_VALUE_ANY;
      value_condition->value1.type = VGX_VALUE_TYPE_NULL;
      value_condition->value1.data.bits = 0;
    }

    // Single integer value, default cmp
    else if( PyLong_Check(py_valcond) ) {
      int64_t ival = PyLong_AsLongLong( py_valcond );
      value_condition->vcomp = vcomp;
      // Constraint accepts integer
      if( value_constraint.type == VGX_VALUE_TYPE_NULL || value_constraint.type == VGX_VALUE_TYPE_BOOLEAN || value_constraint.type == VGX_VALUE_TYPE_INTEGER ) {
        // Check if value is in range
        int inrange = 1;
        if( value_constraint.type != VGX_VALUE_TYPE_NULL ) {
          inrange = !(ival > value_constraint.maxval.integer || ival < value_constraint.minval.integer);
        }
        // Set value
        if( inrange ) {
          value_condition->value1.data.simple.integer = ival;
          value_condition->value1.type = VGX_VALUE_TYPE_INTEGER;
        }
        // Not in range
        else {
          PyErr_Format( PyVGX_QueryError, "integer value '%lld' out of range, expected (%lld, %lld)", ival, value_constraint.minval.integer, value_constraint.maxval.integer );
          vcomp = VGX_VALUE_NONE;
        }
      }
      // Constraint requires double (convert parsed integer to double)
      else if( value_constraint.type == VGX_VALUE_TYPE_REAL ) {
        double dval = (double)ival;
        // Check if value is in range and set value
        if( !(dval > value_constraint.maxval.real || dval < value_constraint.minval.real) ) {
          value_condition->value1.data.simple.real = dval;
          value_condition->value1.type = VGX_VALUE_TYPE_REAL;
        }
        // Not in range
        else {
          CString_t *CSTR__err = CStringNewFormat( "integer value '%lld' out of range, expected (%#g, %#g)", ival, value_constraint.minval.real, value_constraint.maxval.real );
          if( CSTR__err ) {
            PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__err ) );
            CStringDelete( CSTR__err );
          }
          vcomp = VGX_VALUE_NONE;
        }
      }
      // Constraint not compatible with integer
      else {
        PyErr_Format( PyVGX_QueryError, "integer value '%lld' not compatible with value condition", ival );
        vcomp = VGX_VALUE_NONE;
      }
    }

    // Single real value, default cmp
    else if( PyFloat_Check(py_valcond) ) {
      double dval = PyFloat_AS_DOUBLE( py_valcond );
      value_condition->vcomp = vcomp;

      // Constraint accepts double
      if( value_constraint.type == VGX_VALUE_TYPE_NULL || value_constraint.type == VGX_VALUE_TYPE_REAL ) {
        // Check if value is in range
        int inrange = 1;
        if( value_constraint.type != VGX_VALUE_TYPE_NULL ) {
          inrange = !(dval > value_constraint.maxval.real || dval < value_constraint.minval.real);
        }
        // Set value
        if( inrange ) {
          value_condition->value1.data.simple.real = dval;
          value_condition->value1.type = VGX_VALUE_TYPE_REAL;
        }
        // Not in range
        else {
          CString_t *CSTR__err = CStringNewFormat( "float value '%#g' out of range, expected (%#g, %#g)", dval, value_constraint.minval.real, value_constraint.maxval.real );
          if( CSTR__err ) {
            PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__err ) );
            CStringDelete( CSTR__err );
          }
          vcomp = VGX_VALUE_NONE;
        }
      }
      // Constraint not compatible with float
      else {
        CString_t *CSTR__err = CStringNewFormat( "float value '%#g' not compatible with value condition", dval );
        if( CSTR__err ) {
          PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__err ) );
          CStringDelete( CSTR__err );
        }
        vcomp = VGX_VALUE_NONE;
      }
    }

    // Single string value, default cmp
    else if( PyVGX_PyObject_CheckString(py_valcond) ) {
      Py_ssize_t sz;
      Py_ssize_t ucsz;
      const char *string_value = PyVGX_PyObject_AsStringAndSize( py_valcond, &sz, &ucsz );
      // Check if string length is in range
      if( value_constraint.type == VGX_VALUE_TYPE_CSTRING ) {
        if( sz > value_constraint.maxval.integer || sz < value_constraint.minval.integer ) {
          PyErr_Format( PyVGX_QueryError, "string value length '%lld' out of range, expected (%lld, %lld)", sz, value_constraint.minval.integer, value_constraint.maxval.integer );
          vcomp = VGX_VALUE_NONE;
        }
      }
      // Constraint not compatible with string
      else if( value_constraint.type != VGX_VALUE_TYPE_NULL ) {        
        PyVGXError_SetString( PyVGX_QueryError, "string value not compatible with value condition" );
        vcomp = VGX_VALUE_NONE;
      }

      // ok to proceed
      if( vcomp != VGX_VALUE_NONE ) {
        CString_t *CSTR__error = NULL;
        CString_t *CSTR__parsed = iString.Parse.AllowPrefixWildcard( NULL, string_value, &vcomp, &CSTR__error ); // <= this may modify the vcomp if prefix search
        if( CSTR__parsed == NULL ) {
          if( CSTR__error ) {
            PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__error ) );
            CStringDelete( CSTR__error );
          }
          else {
            PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
          }
        }
        else {
          // Large string, it's going to be compressed in the stored property
          if( iPyVGXCodec.IsCStringCompressible( CSTR__parsed ) ) {
            // Exact match, we compress the probe, this will match the compressed stored property
            if( _vgx_is_exact_value_comparison( vcomp ) ) {

              CString_t *CSTR__compressed;
              BEGIN_PYVGX_THREADS {
                CSTR__compressed = CALLABLE( CSTR__parsed )->Compress( CSTR__parsed, CSTRING_COMPRESS_MODE_LZ4 );
              } END_PYVGX_THREADS;

              if( CSTR__compressed ) {
                CStringDelete( CSTR__parsed );
                CSTR__parsed = CSTR__compressed;
              }
              // error
              else {
                CStringDelete( CSTR__parsed );
                CSTR__parsed = NULL;
                PyVGXError_SetString( PyVGX_QueryError, "internal error" );
                vcomp = VGX_VALUE_NONE;
              }
            }
            // Prefix match, not supported for large probes.
            else {
              CStringDelete( CSTR__parsed );
              CSTR__parsed = NULL;
              PyVGXError_SetString( PyVGX_QueryError, "string too large, approximate match not supported" );
              vcomp = VGX_VALUE_NONE;
            }
          }

          if( CSTR__parsed ) {
            if( iVertexProperty.AddCStringValue( &value_condition->value1, &CSTR__parsed ) < 0 ) {
              CStringDelete( CSTR__parsed );
              PyVGXError_SetString( PyVGX_QueryError, "internal error" );
              vcomp = VGX_VALUE_NONE;
            }
            value_condition->vcomp = vcomp;
          }
        }
      }
    }

    // Tuple value (cmp,val) or (cmp,(v1,v2))
    else if( PyTuple_Check( py_valcond ) ) {
      PyObject *py_value = NULL;
      if( (vcomp = __comparison_code_static( py_valcond, &py_value )) != VGX_VALUE_NONE ) {
        // (cmp, (v1,v2))
        if( vcomp == VGX_VALUE_RANGE || vcomp == VGX_VALUE_NRANGE ) {
          // Unpack the inner tuple of lo/hi and parse each individually
          PyObject *py_low  = PyTuple_GET_ITEM( py_value, 0 );
          PyObject *py_high = PyTuple_GET_ITEM( py_value, 1 );
          vgx_value_comparison orig_vcomp = vcomp;
          vgx_value_comparison upper_vcomp = orig_vcomp & VGX_VALUE_NEG ? VGX_VALUE_GT : VGX_VALUE_LTE;
          vgx_value_comparison lower_vcomp = orig_vcomp & VGX_VALUE_NEG ? VGX_VALUE_LT : VGX_VALUE_GTE;
          // Set the high value condition first
          if( (vcomp = _ipyvgx_parser__parse_value_condition( py_high, value_condition, value_constraint, upper_vcomp )) != VGX_VALUE_NONE ) {
            // Move parsed value to 2nd slot in condition
            value_condition->value2 = value_condition->value1;
            value_condition->value1 = DEFAULT_VGX_VALUE;
            // Set the low value condition
            if( (vcomp = _ipyvgx_parser__parse_value_condition( py_low, value_condition, value_constraint, lower_vcomp )) != VGX_VALUE_NONE ) {
              vcomp = value_condition->vcomp = orig_vcomp; // ok!
            }
          }
        }
        // (V_ANY, *)
        else if( vcomp == VGX_VALUE_ANY ) {
          value_condition->vcomp = VGX_VALUE_ANY;
          value_condition->value1.type = VGX_VALUE_TYPE_NULL;
          value_condition->value1.data.bits = 0;
        }
        // (cmp, val)
        else {
          vcomp = _ipyvgx_parser__parse_value_condition( py_value, value_condition, value_constraint, vcomp );
        }
      }
    }

    // Invalid
    else {
      PyVGXError_SetString( PyVGX_QueryError, "invalid condition, must be <value> or (comparison, <value>) or (range_comparison, (<low>,<high>) )" );
      vcomp = VGX_VALUE_NONE; // error
    }
  } END_PYTHON_INTERPRETER;

  return vcomp;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__virtual( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  bool is_virtual;
  if( __bool_condition( py_value, &is_virtual ) < 0 ) {
    return -1;
  }
  vgx_VertexStateContext_man_t manifestation = is_virtual ? VERTEX_STATE_CONTEXT_MAN_VIRTUAL : VERTEX_STATE_CONTEXT_MAN_REAL;
  iVertexCondition.RequireManifestation( vertex_condition, manifestation );
  return 0;       
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__type( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  const char *type = NULL;
  vgx_value_comparison vcomp = __string_condition( py_value, &type, VGX_VALUE_EQU );
  return iVertexCondition.RequireType( vertex_condition, context->graph, vcomp, type );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__degree_general( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, vgx_arc_direction direction ) {
  int ret = 0;

  PyObject *py_valuecond = NULL;
  int is_range = 0;
  int is_arc_conditional = 0;

  vgx_value_constraint_t degree_constraint = _vgx_integer_value_constraint( 0, LLONG_MAX );

  // Advanced degree 
  // degree=( V_RANGE, (<low>, <high>) )
  // degree=( V_NRANGE, (<low>, <high>) )
  // degree=( <arcspec>, <num> )
  // degree=( <arcspec>, (<cond>, <val>) )
  // degree=( <arcspec>, (V_RANGE, ( <low>, <high> )) )
  //
  if( PyTuple_Check( py_value ) && PyTuple_GET_SIZE( py_value ) == 2 ) {
    PyObject *py_degreespec_0 = PyTuple_GET_ITEM( py_value, 0 );
    // degree=( V_RANGE, (<low>, <high>) )
    // degree=( V_NRANGE, (<low>, <high>) )
    if( PyLong_Check( py_degreespec_0 ) 
        &&
        ( PyLong_AS_LONG( py_degreespec_0 ) == VGX_VALUE_RANGE
          ||
          PyLong_AS_LONG( py_degreespec_0 ) == VGX_VALUE_NRANGE
        )
      )
    {
      is_range = 1;
      py_valuecond = py_value;
    }
    // degree=( <arcspec>, ... )
    else if( PyTuple_Check( py_degreespec_0 )         // e.g. arc=("relname", D_IN, M_INT)
             ||                                       // OR
             PyVGX_PyObject_CheckString( py_degreespec_0 )  // e.g. arc="relname"
             ||                                       // OR
             py_degreespec_0 == Py_None               // e.g. arc=None
           )
    {
      is_arc_conditional = 1;
      py_valuecond = PyTuple_GET_ITEM( py_value, 1 ); // e.g. 17, or (V_GT,17), or (V_RANGE, (50,20))
    }
  }

  if( is_range || is_arc_conditional ) {
    vgx_DegreeCondition_t *degree_condition = NULL;
    
    XTRY {
      // Allocate, will be stolen by the vertex condition
      if( (degree_condition = calloc( 1, sizeof( vgx_DegreeCondition_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x781 );
      }

      if( is_arc_conditional ) {
        PyObject *py_arc = PyTuple_GET_ITEM( py_value, 0 );
        // The arc condition used to compute the degree
        if( (degree_condition->arc_condition_set = iPyVGXParser.NewArcConditionSet( graph, py_arc, VGX_ARCDIR_ANY )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x782 );
        }

        // Make sure the arc direction specified matches the intended degree direction
        switch( degree_condition->arc_condition_set->arcdir ) {
        case VGX_ARCDIR_ANY:
          degree_condition->arc_condition_set->arcdir = direction;
          break;
        case VGX_ARCDIR_IN:
          /* FALLTHRU */
        case VGX_ARCDIR_OUT:
          if( direction != VGX_ARCDIR_ANY && direction != degree_condition->arc_condition_set->arcdir ) {
            PyVGXError_SetString( PyVGX_QueryError, "Arc direction mismatch for degree condition" );
            THROW_SILENT( CXLIB_ERR_API, 0x783 );
          }
          break;
        default:
          PyVGXError_SetString( PyVGX_QueryError, "Invalid arc direction for degree condition" );
          THROW_SILENT( CXLIB_ERR_API, 0x784 );
          break;
        }
      }

      // The value comparison
      if( iPyVGXParser.ParseValueCondition( py_valuecond, &degree_condition->value_condition, degree_constraint, VGX_VALUE_EQU ) == VGX_VALUE_NONE ) {
        THROW_SILENT( CXLIB_ERR_API, 0x785 );
      }

      // Require vertex degree condition (this steals the degree condition)
      if( iVertexCondition.RequireConditionalDegree( vertex_condition, &degree_condition ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x786 );
      }
    }
    XCATCH( errcode ) {
      if( degree_condition ) {
        iArcConditionSet.Delete( &degree_condition->arc_condition_set );
        iVertexProperty.ClearValueCondition( &degree_condition->value_condition );
        free( degree_condition );
      }
      ret = -1;
    }
    XFINALLY {
    }
  }

  // Basic degree
  // degree=<val>
  // degree=( <cond>, <val> )
  else {
    vgx_value_condition_t condition = DEFAULT_VGX_VALUE_CONDITION;
    if( iPyVGXParser.ParseValueCondition( py_value, &condition, degree_constraint, VGX_VALUE_EQU ) == VGX_VALUE_NONE ) {
      ret = -1;
    }
    else {
      ret = iVertexCondition.RequireDegree( vertex_condition, condition.vcomp, direction, condition.value1.data.simple.integer );
    }
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__degree( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  return __set_probe_condition__degree_general( py_value, vertex_condition, context->graph, VGX_ARCDIR_ANY );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__indegree( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context) {
  return __set_probe_condition__degree_general( py_value, vertex_condition, context->graph, VGX_ARCDIR_IN );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__outdegree( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context) {
  return __set_probe_condition__degree_general( py_value, vertex_condition, context->graph, VGX_ARCDIR_OUT );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__similarity( PyObject *py_similarity, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {

  int ret = 0;
  /*

  py_similarity:
    {
      'vector'  : <probe_vector>,
      'score'   : <value_condition>,
      'hamdist' : <value_condition>
    }

  <value_condition>:
    <number>
    ( <COND>, <number> )
    ( V_RANGE, ( <low>, <high> ) )

  <probe_vector>:
    [ ( <dim>, <mag> ), ...]
    <PyVGX_Vector instance>
    "tail"
  */
  
  vgx_SimilarityCondition_t *similarity_condition = NULL;

  XTRY {
    // Create the condition
    if( (similarity_condition = calloc( 1, sizeof( vgx_SimilarityCondition_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x791
      );
    }
    // Default positive
    similarity_condition->positive = true;

    // Unwrap tuple (if applicable) to get he similarity condition dict, and invert sign if False
    __unwrap_signed_condition( &similarity_condition->positive, &py_similarity );

    // Check dict
    if( !PyDict_Check( py_similarity ) ) {
      PyVGXError_SetString( PyVGX_QueryError, "similarity condition must be dict" );
      THROW_SILENT( CXLIB_ERR_API, 0x792 );
    }

    // Now we iterate over the items in the dict
    Py_ssize_t pos = 0;
    PyObject *py_key;
    PyObject *py_value;

    // Iterate over all items in the dict
    while( PyDict_Next( py_similarity, &pos, &py_key, &py_value ) ) {
      // Get next time condition key
      const char *key = PyVGX_PyObject_AsString( py_key );
      if( key == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x793 );
      }

      // probe vector
      if( CharsEqualsConst( key, "vector" ) ) {
        if( PyVGX_PyObject_CheckString( py_value ) ) {
          // Special case: Use the tail vertex's vector as probe vector for the neighbors.
          // If the tail has no vector it is a NULL vector and automatic miss.
          const char *str = PyVGX_PyObject_AsString( py_value );
          if( CharsEqualsConst( str, "tail" ) ) {
            // When the probe vector is a NULL-vector object (not a NULL pointer!) it is interpreted later to fall back to the anchor vertex vector,
            // or for multi-neighborhood traversal the node's vector whose neighborhood is about to be traversed.
            vgx_Similarity_t *simcontext = context->graph->similarity;
            similarity_condition->probevector = CALLABLE( simcontext )->NewInternalVector( simcontext, NULL, 1.0f, 0, true );
          }
          else {
            PyErr_Format( PyVGX_QueryError, "invalid vector: '%s'", str );
            THROW_SILENT( CXLIB_ERR_API, 0x794 );
          }
        }
        else if( (similarity_condition->probevector = iPyVGXParser.InternalVectorFromPyObject( context->graph->similarity, py_value, NULL, true )) == NULL ) { 
          THROW_SILENT( CXLIB_ERR_API, 0x795 );
        }
      }
      // similarity score
      else if( CharsEqualsConst( key, "score" ) ) {
        vgx_value_constraint_t score_constraint = _vgx_real_value_constraint( -1.0, 1.0 );
        if( iPyVGXParser.ParseValueCondition( py_value, &similarity_condition->simval_condition, score_constraint, VGX_VALUE_GTE ) == VGX_VALUE_NONE ) {
          THROW_SILENT( CXLIB_ERR_API, 0x796 );
        }

      }
      // hamming distance
      else if( CharsEqualsConst( key, "hamdist" ) ) {
        vgx_value_constraint_t hamdist_constraint = _vgx_integer_value_constraint( 0, 64 );
        if( iPyVGXParser.ParseValueCondition( py_value, &similarity_condition->hamval_condition, hamdist_constraint, VGX_VALUE_LTE ) == VGX_VALUE_NONE ) {
          THROW_SILENT( CXLIB_ERR_API, 0x797 );
        }
      }
      // invalid
      else {
        PyErr_Format( PyVGX_QueryError, "invalid entry for similarity condition: '%s'", key );
        THROW_SILENT( CXLIB_ERR_API, 0x798 );
      }
    }

    // Make sure we have a probe vector
    if( similarity_condition->probevector == NULL ) {
      PyVGXError_SetString( PyVGX_QueryError, "probe vector required for similarity condition" );
      THROW_SILENT( CXLIB_ERR_API, 0x799 );
    }

    // Require vertex similarity condition (this steals the similarity condition)
    if( iVertexCondition.RequireSimilarity( vertex_condition, &similarity_condition ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x79A );
    }

  }
  XCATCH( errcode ) {
    if( similarity_condition ) {
      if( similarity_condition->probevector ) {
        BEGIN_PYVGX_THREADS {
          CALLABLE( similarity_condition->probevector )->Decref( similarity_condition->probevector );
        } END_PYVGX_THREADS;
      }
      free( similarity_condition );
    }
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__id( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  vgx_value_comparison vcomp = VGX_VALUE_EQU; // default

  // Multiple distinct IDs
  if( PyList_CheckExact( py_value ) ) {
    return __set_probe_condition__id_list( py_value, vertex_condition, context );
  }
  // Single ID (possibly prefix wildcard)
  else {
    pyvgx_VertexIdentifier_t ident;
    if( iPyVGXParser.GetVertexID( NULL, py_value, &ident, NULL, true, "Probe ID" ) < 0 ) {
      return -1;
    }

    // Not a string or vertex, try more parsing
    if( ident.id == NULL || *ident.id == '*' ) {
      vcomp = __string_condition( py_value, &ident.id, vcomp );
    }

    // Add the parsed string to the vertex condition
    if( iVertexCondition.RequireIdentifier( vertex_condition, vcomp, ident.id ) < 0 ) {
      if( vertex_condition->CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue( vertex_condition->CSTR__error ) );
      }
      else {
        PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
      }
      return -1;
    }
    return 0;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__id_list( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  vgx_StringList_t *list = iPyVGXParser.NewStringListFromVertexPyList( py_value );
  if( list ) {
    if( iVertexCondition.RequireIdentifierList( vertex_condition, &list ) < 0 ) { // <- steals the list on success
      if( vertex_condition->CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue( vertex_condition->CSTR__error ) );
      }
      else {
        PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
      }
      return -1;
    }
    iString.List.Discard( &list );
  }
  else {
    return -1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__time_general( PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, int64_t ref_ts ) {
  int ret = 0;
  /*
  py_time_conditions = {
    'created'   : (V_COND,<int>),     or (V_RANGE, (<low>,<high>))
    'modified'  : (V_COND,<int>),     or (V_RANGE, (<low>,<high>))
    'expires'   : (V_COND,<int>),     or (V_RANGE, (<low>,<high>))
  }
  */

  XTRY {
    // Default positive match
    bool positive = true;

    // Unwrap sign tuple (if applicable) and invert sign if false
    __unwrap_signed_condition( &positive, &py_time_conditions );

    // Must be a dict
    if( !PyDict_Check( py_time_conditions ) ) {
      PyVGXError_SetString( PyVGX_QueryError, "time condition must be dict" );
      THROW_SILENT( CXLIB_ERR_API, 0x7A1 );
    }

    // Now we iterate over the items in the dict
    Py_ssize_t pos = 0;
    PyObject *py_key;
    PyObject *py_valcond;

    // Initialize timestamp condition
    iVertexCondition.InitTimestampConditions( vertex_condition, positive );

    // Iterate over all items in the dict
    while( PyDict_Next( py_time_conditions, &pos, &py_key, &py_valcond ) ) {

      // Get next time condition key
      const char *key = PyVGX_PyObject_AsString( py_key );
      if( key == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x7A2 );
      }

      // Get next time condition value
      // NOTE: Default is "AFTER time "
      vgx_value_condition_t time_condition = DEFAULT_VGX_VALUE_CONDITION;

      // Offset the permitted value range by the reference timestamp
      vgx_value_constraint_t time_constraint = _vgx_integer_value_constraint( TIMESTAMP_MIN, TIMESTAMP_MAX );
      time_constraint.maxval.integer -= ref_ts;
      time_constraint.minval.integer -= ref_ts;

      if( iPyVGXParser.ParseValueCondition( py_valcond, &time_condition, time_constraint, VGX_VALUE_GTE ) == VGX_VALUE_NONE ) {
        THROW_SILENT( CXLIB_ERR_API, 0x7A3 );
      }

      // Offset the parsed value range by the reference timestamp 
      time_condition.value1.data.simple.integer += ref_ts;
      time_condition.value2.data.simple.integer += ref_ts;

      // Set the vertex condition

      // TMC
      if( CharsEqualsConst( key, "created" ) ) {
        iVertexCondition.RequireCreationTime( vertex_condition, time_condition );
      }
      // TMM
      else if( CharsEqualsConst( key, "modified" ) ) {
        iVertexCondition.RequireModificationTime( vertex_condition, time_condition );
      }
      // TMX
      else if( CharsEqualsConst( key, "expires" ) ) {
        iVertexCondition.RequireExpirationTime( vertex_condition, time_condition );
      }
      // Invalid
      else {
        PyErr_Format( PyVGX_QueryError, "Invalid time condition: %s", key );
        THROW_ERROR( CXLIB_ERR_API, 0x7A4 );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__abstime( PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  return __set_probe_condition__time_general( py_time_conditions, vertex_condition, 0 );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__reltime( PyObject *py_time_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  int64_t ref_ts = _vgx_graph_seconds( context->graph );
  return __set_probe_condition__time_general( py_time_conditions, vertex_condition, ref_ts );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__property( PyObject *py_property_conditions, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  int ret = 0;
  // py_property_conditions = { 'color':'red', 'name':'ab*', 'size':(VGX_VALUE_GT,17), ... }
  // py_property_conditions = ( False, { 'color':'red', 'name':'ab*', 'size':(VGX_VALUE_GT,17), ... } )
  //

  vgx_VertexProperty_t *vertex_property = NULL;
  vgx_value_condition_t *value_condition = NULL;

  XTRY {
    // Default positive match
    bool positive = true;

    // Get the overall match sign
    __unwrap_signed_condition( &positive, &py_property_conditions );

    // Must be a dict
    if( !PyDict_Check( py_property_conditions ) ) {
      PyVGXError_SetString( PyVGX_QueryError, "property condition must be dict" );
      THROW_SILENT( CXLIB_ERR_API, 0x7B1 );
    }

    // Now we iterate over the items in the dict
    Py_ssize_t pos = 0;
    PyObject *py_key;
    PyObject *py_valcond;

    // Initialize the property condition set
    iVertexCondition.InitPropertyConditionSet( vertex_condition, positive );

    // Iterate over all items in the dict
    while( PyDict_Next( py_property_conditions, &pos, &py_key, &py_valcond ) ) {

      // Get next property condition key
      const char *key = PyVGX_PyObject_AsString( py_key );
      if( key == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x7B2 );
      }

      // Validate key
      if( !iString.Validate.SelectKey( key ) ) {
        PyErr_Format( PyExc_ValueError, "invalid property key: '%s'", key );
        THROW_SILENT( CXLIB_ERR_API, 0x7B3 );
      }

      // Create and parse the value condition
      if( (value_condition = iVertexProperty.NewValueCondition()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7B4 );
      }
      vgx_value_constraint_t property_constraint = _vgx_no_value_constraint(); // we don't know the type because it's dynamic per vertex
      if( iPyVGXParser.ParseValueCondition( py_valcond, value_condition, property_constraint, VGX_VALUE_EQU ) == VGX_VALUE_NONE ) {
        THROW_SILENT( CXLIB_ERR_API, 0x7B5 );
      }

      // Create property condition from value condition (steals the value condition)
      if( (vertex_property = iVertexProperty.NewFromValueCondition( NULL, key, &value_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x7B6 );
      }

      // Hand over the vertex property to the vertex condition (will be stolen and set to NULL here)
      if( iVertexCondition.RequireProperty( vertex_condition, &vertex_property ) < 0 ) {
        PyVGXError_SetString( PyExc_Exception, "internal error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x7B7 );
      }

    }
  }
  XCATCH( errcode ) {
    iVertexProperty.DeleteValueCondition( &value_condition );
    iVertexProperty.Delete( &vertex_property );
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__local_filter( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  // Assign local filter expression
  const char *local_filter_expression = PyVGX_PyObject_AsString( py_value );
  if( local_filter_expression == NULL ) {
    return -1;
  }

  int set = -1;
  BEGIN_PYVGX_THREADS {
    set = iVertexCondition.RequireLocalFilter( vertex_condition, context->graph, local_filter_expression );
  } END_PYVGX_THREADS;
  
  if( set < 0 ) {
    if( vertex_condition->CSTR__error ) {
      PyVGXError_SetString( PyVGX_QueryError, CStringValue( vertex_condition->CSTR__error ) );
    }
    else {
      PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
    }
    return -1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__post( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  // Assign post filter expression
  const char *post_filter_expression = PyVGX_PyObject_AsString( py_value );
  if( post_filter_expression == NULL ) {
    return -1;
  }

  int set = -1;
  BEGIN_PYVGX_THREADS {
    set = iVertexCondition.RequirePostFilter( vertex_condition, context->graph, post_filter_expression );
  } END_PYVGX_THREADS;

  if( set < 0 ) {
    if( vertex_condition->CSTR__error ) {
      PyVGXError_SetString( PyVGX_QueryError, CStringValue( vertex_condition->CSTR__error ) );
    }
    else {
      PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
    }
    return -1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __set_probe_condition__filter( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  // Assign main filter expression
  const char *filter_expression = PyVGX_PyObject_AsString( py_value );
  if( filter_expression == NULL ) {
    return -1;
  }

  int set = -1;
  BEGIN_PYVGX_THREADS {
    switch( context->recursion_mode ) {
    case RECURSIVE_TRAVERSAL:
      set = iVertexCondition.RequireTraversalFilter( vertex_condition, context->graph, filter_expression );
      break;
    case RECURSIVE_CONDITION:
      set = iVertexCondition.RequireConditionFilter( vertex_condition, context->graph, filter_expression );
      break;
    }
  } END_PYVGX_THREADS;

  if( set < 0 ) {
    if( vertex_condition->CSTR__error ) {
      PyVGXError_SetString( PyVGX_QueryError, CStringValue( vertex_condition->CSTR__error ) );
    }
    else {
      PyVGXError_SetString( PyVGX_QueryError, "unknown error" );
    }
    return -1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__arc( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  if( context->recursion_mode == RECURSIVE_TRAVERSAL && iVertexCondition.HasArcTraversal( vertex_condition ) ) {
    PyVGXError_SetString( PyVGX_QueryError, "traverse arc re-definition" );
    return -1;
  }

  // Require the neighbor vertex to have some relationship to another vertex to pass the search filter
  vgx_ArcConditionSet_t *arc_condition_set = iPyVGXParser.NewArcConditionSet( context->graph, py_value, VGX_ARCDIR_ANY );
  if( arc_condition_set == NULL ) {
    return -1;
  }
  switch( context->recursion_mode ) {
  case RECURSIVE_TRAVERSAL:
    iVertexCondition.RequireArcTraversal( vertex_condition, &arc_condition_set );
    return 0;
  case RECURSIVE_CONDITION:
    iVertexCondition.RequireArcCondition( vertex_condition, &arc_condition_set );
    return 0;
  default:
    PyVGXError_SetString( PyVGX_InternalError, "invalid recursion context" );
    return -1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__adjacent_arc( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  context->recursion_mode = RECURSIVE_CONDITION;
  return __set_probe_condition__arc( py_value, vertex_condition, context );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__neighbor( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  if( context->recursion_mode == RECURSIVE_TRAVERSAL && iVertexCondition.HasRecursiveTraversal( vertex_condition ) ) {
    PyVGXError_SetString( PyVGX_QueryError, "traverse neighbor re-definition" );
    return -1;
  }

  // Check recursion
  if( context->recursion_count == 0 ) {
    PyVGXError_SetString( PyVGX_QueryError, "Vertex condition recursion limit reached" );
    return -1;
  }

  vgx_VertexCondition_t *neighbor_condition = NULL;
  
  // ID list
  if( PyList_CheckExact( py_value ) ) {
    neighbor_condition = iVertexCondition.New( true ); // Default matching is positive
    if( __set_probe_condition__id_list( py_value, neighbor_condition, context ) < 0 ) {
      return -1;
    }
  }
  // Recursive neighbor filter
  else {
    if( (neighbor_condition = __recursive_new_vertex_condition( context->graph, py_value, context->collector_mode, context->recursion_count-1 )) == NULL ) {
      return -1;
    }
  }

  // Default to no arc collection
  // NOTE: If 'collect':True was specified the collector_mode will be set to collect arcs but collect_condition_set will be NULL.
  //       This indicates the collector should follow the arc traversal filter. Therefore we do not make the "no collection filter"
  //       below so that the search probe creation later can pick up on the NULL set and patch in the superfilter which will then
  //       equal the arc traversal filter. This is a bit convoluted so consider cleaning up in the future.
  if( neighbor_condition->advanced.recursive.collector_mode == VGX_COLLECTOR_MODE_NONE_CONTINUE 
      &&
      neighbor_condition->advanced.recursive.collect_condition_set == NULL )
  {
    if( (neighbor_condition->advanced.recursive.collect_condition_set = iArcConditionSet.NewEmpty( context->graph, false, VGX_ARCDIR_ANY )) == NULL ) {
      iVertexCondition.Delete( &neighbor_condition );
      return -1;
    }
  }

  switch( context->recursion_mode ) {
  case RECURSIVE_TRAVERSAL:
    iVertexCondition.RequireRecursiveTraversal( vertex_condition, &neighbor_condition ); 
    return 0;
  case RECURSIVE_CONDITION:
    iVertexCondition.RequireRecursiveCondition( vertex_condition, &neighbor_condition ); 
    return 0;
  default:
    PyVGXError_SetString( PyVGX_InternalError, "invalid recursion context" );
    return -1;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__adjacent_neighbor( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  context->recursion_mode = RECURSIVE_CONDITION;
  return __set_probe_condition__neighbor( py_value, vertex_condition, context );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__collect( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  if( iVertexCondition.HasCollector( vertex_condition ) ) {
    PyVGXError_SetString( PyVGX_QueryError, "traverse collect re-definition" );
    return -1;
  }

  // in recursive conditions collect=None is interpreted as the default "don't collect" as opposed to the arc=None "no filter"
  if( py_value != Py_None ) {
    vgx_ArcConditionSet_t *collect_arc_condition_set = NULL;
    if( py_value == Py_True ) {
      // Experiment: see if this generates arc traverser that will follow arc filter for collection
    }
    else {
      // Require the neighbor vertex to have some relationship to another vertex to pass the collection filter
      if( (collect_arc_condition_set = iPyVGXParser.NewArcConditionSet( context->graph, py_value, VGX_ARCDIR_ANY )) == NULL ) {
        return -1;
      }
    }
    iVertexCondition.CollectNeighbors( vertex_condition, &collect_arc_condition_set, context->collector_mode );
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__assert( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  vgx_ArcFilter_match assert_match;
  // Assert True
  if( py_value == Py_True || (PyLong_CheckExact( py_value ) && PyLong_AsLong( py_value ) > 0 ) ) {
    assert_match = VGX_ARC_FILTER_MATCH_HIT;
  }
  // Assert False
  else if( py_value == Py_False || (PyLong_CheckExact( py_value ) && PyLong_AsLong( py_value ) < 1 ) ) {
    assert_match = VGX_ARC_FILTER_MATCH_MISS;
  }
  // Assert None
  else if( py_value == Py_None ) {
    return 0; // do nothing
  }
  // Error
  else {
    PyVGXError_SetString( PyExc_ValueError, "a boolean is required" );
    return -1;
  }

  switch( context->recursion_mode ) {
  case RECURSIVE_TRAVERSAL:
    iVertexCondition.SetAssertTraversal( vertex_condition, assert_match );
    break;
  case RECURSIVE_CONDITION:
    iVertexCondition.SetAssertCondition( vertex_condition, assert_match );
    break;
  }

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__adjacent_assert( PyObject *py_value, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  context->recursion_mode = RECURSIVE_CONDITION;
  return __set_probe_condition__assert( py_value, vertex_condition, context );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__recursive( PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  PyObject *py_arc = NULL;
  PyObject *py_neighbor = NULL;
  PyObject *py_filter = NULL;
  PyObject *py_collect = NULL;
  PyObject *py_assert = NULL;

  // Wildcard, do nothing
  if( py_recursive == NULL || py_recursive == Py_None ) {
    return 0;
  }
  // Plain string: Interpret as neighbor vertex ID (D_ANY implied)
  else if( PyVGX_PyObject_CheckString( py_recursive ) ) {
    py_neighbor = py_recursive;
  }
  // List: Interpret as list of neighbor vertex IDs (D_ANY implied)
  //       with short-circuiting OR-logic applied in order specified
  else if( PyList_CheckExact( py_recursive ) ) {
    py_neighbor = py_recursive;
  }
  // Tuple: Interpret as arc
  else if( PyTuple_CheckExact( py_recursive ) ) {
    py_arc = py_recursive;
  }
  // Dict: Complex condition
  else if( PyDict_CheckExact( py_recursive ) ) {
    Py_ssize_t pos = 0;
    PyObject *py_name;
    PyObject *py_value;
    const char *name;
    // Iterate over all items in the dict
    while( PyDict_Next( py_recursive, &pos, &py_name, &py_value ) ) {
      // key must be string
      if( (name = PyVGX_PyObject_AsString( py_name )) == NULL ) {
        return -1;
      }
      // arc
      if( CharsEqualsConst( name, "arc" ) ) {
        py_arc = py_value;
      }
      // neighbor
      else if( CharsEqualsConst( name, "neighbor" ) ) {
        py_neighbor = py_value;
      }
      // filter
      else if( CharsEqualsConst( name, "filter" ) ) {
        py_filter = py_value;
      }
      // collect
      else if( CharsEqualsConst( name, "collect" ) && context->recursion_mode == RECURSIVE_TRAVERSAL ) {
        py_collect = py_value;
      }
      // assert
      else if( CharsEqualsConst( name, "assert" ) ) {
        py_assert = py_value;
      }
      // error
      else {
        if( context->recursion_mode == RECURSIVE_TRAVERSAL ) {
          PyErr_Format( PyVGX_QueryError, "Invalid recursive traversal parameter: '%s'", name );
        }
        else if( context->recursion_mode == RECURSIVE_CONDITION ) {
          PyErr_Format( PyVGX_QueryError, "Invalid adjacency condition parameter: '%s'", name );
        }
        else {
          PyErr_Format( PyVGX_QueryError, "Invalid recursion mode %x (internal error)", context->recursion_mode );
        }
        return -1;
      }
    }
  }
  // Error
  else {
    PyVGXError_SetString( PyVGX_QueryError, "recursive condition error: list or tuple expected" );
    return -1;
  }

  // ARC
  if( py_arc && __set_probe_condition__arc( py_arc, vertex_condition, context ) < 0 ) {
    return -1;
  }

  // NEIGHBOR
  if( py_neighbor && __set_probe_condition__neighbor( py_neighbor, vertex_condition, context ) < 0 ) {
    return -1;
  }

  // FILTER
  if( py_filter && __set_probe_condition__filter( py_filter, vertex_condition, context ) < 0 ) {
    return -1;
  }

  // COLLECT
  if( py_collect && __set_probe_condition__collect( py_collect, vertex_condition, context ) < 0 ) {
    return -1;
  }
  
  // ASSERT
  if( py_assert && __set_probe_condition__assert( py_assert, vertex_condition, context ) < 0 ) {
    return -1;
  }

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__adjacent( PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  __recursion_mode orig = context->recursion_mode;
  context->recursion_mode = RECURSIVE_CONDITION;
  int ret = __set_probe_condition__recursive( py_recursive, vertex_condition, context );
  context->recursion_mode = orig;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __set_probe_condition__traverse( PyObject *py_recursive, vgx_VertexCondition_t *vertex_condition, __probe_condition_context_t *context ) {
  __recursion_mode orig = context->recursion_mode;
  context->recursion_mode = RECURSIVE_TRAVERSAL;
  context->spec |= TRAVERSAL_EXPLICIT;
  int ret = __set_probe_condition__recursive( py_recursive, vertex_condition, context );
  context->recursion_mode = orig;
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t * __new_vertex_condition_keymap( framehash_dynamic_t *dyn ) {
  framehash_cell_t *map = NULL;
  if( (map = iMapping.NewIntegerMap( dyn, "vertex_condition_keymap.dyn" )) != NULL ) {
    iMapping.IntegerMapAdd( &map, dyn, "virtual",    (int64_t)__set_probe_condition__virtual );
    iMapping.IntegerMapAdd( &map, dyn, "type",       (int64_t)__set_probe_condition__type );
    iMapping.IntegerMapAdd( &map, dyn, "degree",     (int64_t)__set_probe_condition__degree );
    iMapping.IntegerMapAdd( &map, dyn, "indegree",   (int64_t)__set_probe_condition__indegree );
    iMapping.IntegerMapAdd( &map, dyn, "outdegree",  (int64_t)__set_probe_condition__outdegree );
    iMapping.IntegerMapAdd( &map, dyn, "similarity", (int64_t)__set_probe_condition__similarity );
    iMapping.IntegerMapAdd( &map, dyn, "id",         (int64_t)__set_probe_condition__id );
    iMapping.IntegerMapAdd( &map, dyn, "abstime",    (int64_t)__set_probe_condition__abstime );
    iMapping.IntegerMapAdd( &map, dyn, "reltime",    (int64_t)__set_probe_condition__reltime );
    iMapping.IntegerMapAdd( &map, dyn, "property",   (int64_t)__set_probe_condition__property );
    
    iMapping.IntegerMapAdd( &map, dyn, "traverse",   (int64_t)__set_probe_condition__traverse );
    // These are aliases for the same keywords within 'traverse':{ } 
    iMapping.IntegerMapAdd( &map, dyn, "arc",        (int64_t)__set_probe_condition__arc );       // alias 'traverse':{ 'arc':... }
    iMapping.IntegerMapAdd( &map, dyn, "neighbor",   (int64_t)__set_probe_condition__neighbor );  // alias 'traverse':{ 'neighbor':... }
    iMapping.IntegerMapAdd( &map, dyn, "collect",    (int64_t)__set_probe_condition__collect );   // alias 'traverse':{ 'collect':... }
    iMapping.IntegerMapAdd( &map, dyn, "assert",     (int64_t)__set_probe_condition__assert );    // alias 'traverse':{ 'assert':... }
    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    iMapping.IntegerMapAdd( &map, dyn, "adjacent",   (int64_t)__set_probe_condition__adjacent );
    // These are aliases for the same keywords within 'adjacent':{ } 
    iMapping.IntegerMapAdd( &map, dyn, "has_arc",       (int64_t)__set_probe_condition__adjacent_arc );       // alias 'adjacent':{ 'arc':... }
    iMapping.IntegerMapAdd( &map, dyn, "has_neighbor",  (int64_t)__set_probe_condition__adjacent_neighbor );  // alias 'adjacent':{ 'neighbor':... }
    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    iMapping.IntegerMapAdd( &map, dyn, "filter",     (int64_t)__set_probe_condition__local_filter );
    iMapping.IntegerMapAdd( &map, dyn, "post",       (int64_t)__set_probe_condition__post );
    if( !(iMapping.IntegerMapSize( map ) > 0) ) {
      iMapping.DeleteIntegerMap( &map, dyn );
    }
  }
  return map;
}



/******************************************************************************
 *
 *
 * NOTE: caller will own vertex_condition->id and vertex_condition->vertex_type if not NULL
 ******************************************************************************
 */
static vgx_VertexCondition_t * _ipyvgx_parser__new_vertex_condition( vgx_Graph_t *graph, PyObject *py_vertex_condition, vgx_collector_mode_t collector_mode ) {
  vgx_VertexCondition_t *vertex_condition = __recursive_new_vertex_condition( graph, py_vertex_condition, collector_mode, PyVGX_VertexConditionMaxRecursion );
  if( vertex_condition ) {
    vgx_Vector_t *vector;
    if( vertex_condition->advanced.similarity_condition && (vector = vertex_condition->advanced.similarity_condition->probevector) != NULL ) {
      vgx_Evaluator_t *local_filter = vertex_condition->advanced.local_evaluator.filter;
      vgx_Evaluator_t *post_filter = vertex_condition->advanced.local_evaluator.post;
      vgx_Evaluator_t *traversing_filter = vertex_condition->advanced.recursive.traversing.evaluator;
      vgx_Evaluator_t *conditional_filter = vertex_condition->advanced.recursive.conditional.evaluator;

      if( local_filter ) {
        CALLABLE( local_filter )->SetVector( local_filter, vector ); 
      }
      if( post_filter ) {
        CALLABLE( post_filter )->SetVector( post_filter, vector ); 
      }
      if( traversing_filter ) {
        CALLABLE( traversing_filter )->SetVector( traversing_filter, vector );
      }
      if( conditional_filter ) {
        CALLABLE( conditional_filter )->SetVector( conditional_filter, vector );
      }
    }
  }
  return vertex_condition;
}



/******************************************************************************
 *
 *
 * NOTE: caller will own vertex_condition->id and vertex_condition->vertex_type if not NULL
 ******************************************************************************
 */
static vgx_VertexCondition_t * __recursive_new_vertex_condition( vgx_Graph_t *graph, PyObject *py_vertex_condition, vgx_collector_mode_t collector_mode, int rcnt ) {

  if( rcnt < 0 ) {
    return NULL;
  }

  vgx_VertexCondition_t *vertex_condition = iVertexCondition.New( true ); // Default matching is positive
  if( vertex_condition == NULL ) {
    return NULL;
  }

  __probe_condition_context_t context = {
    .name             = NULL,
    .graph            = graph,
    .collector_mode   = collector_mode,
    .recursion_mode   = RECURSIVE_TRAVERSAL,
    .recursion_count  = rcnt,
    .spec             = TRAVERSAL_NONE
  };

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      // Parse input vertex probe and populate vertex condition
      while( py_vertex_condition && py_vertex_condition != Py_None ) {
        // Simple string or vertex instance interpreted as vertex ID condition
        if( PyVGX_PyObject_CheckString( py_vertex_condition ) || PyVGX_Vertex_CheckExact( py_vertex_condition ) ) {
          if( __set_probe_condition__id( py_vertex_condition, vertex_condition, NULL ) < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x7C1 );
          }
        }
        // List of strings or vertex instances interpreted as multiple vertex ID condition (OR-logic)
        else if( PyList_Check( py_vertex_condition ) ) {
          if( __set_probe_condition__id_list( py_vertex_condition, vertex_condition, NULL ) < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x7C2 );
          }
        }
        // Parse conditions
        else {
          // Unwrap signed tuple and start parsing from the top again
          if( __unwrap_signed_condition( &vertex_condition->positive, &py_vertex_condition ) ) {
            continue;
          }
          // Expect dict at this point
          if( !PyDict_Check( py_vertex_condition ) ) {
            PyVGXError_SetString( PyVGX_QueryError, "Invalid vertex condition, must be string or dict" );
            THROW_SILENT( CXLIB_ERR_API, 0x7C3 );
          }
          Py_ssize_t pos = 0;
          PyObject *py_name;
          PyObject *py_value;
          __set_probe_condition set_condition;
          // Iterate over all items in the dict
          while( PyDict_Next( py_vertex_condition, &pos, &py_name, &py_value ) ) {
            if( (context.name = PyVGX_PyObject_AsString( py_name )) != NULL ) {
              int64_t funcaddr;
              if( iMapping.IntegerMapGet( g_vertex_condition_keymap, &g_vertex_condition_keymap_dyn, context.name, &funcaddr ) ) {
                if( (set_condition = (__set_probe_condition)funcaddr) != NULL ) {
                  if( set_condition( py_value, vertex_condition, &context ) == 0 ) {
                    continue; // Next item in dict
                  }
                  THROW_SILENT( CXLIB_ERR_API, 0x7C4 );
                }
              }
              // Help with deprecated API:
              if( CharsEqualsConst( context.name, "arc" ) || CharsEqualsConst( context.name, "neighbor" ) || CharsEqualsConst( context.name, "collect" ) ) {
                PyErr_Format( PyVGX_QueryError, "Invalid vertex condition: '%s' ( try 'traverse':{ '%s':... } ? )", context.name, context.name );
              }
              else {
                PyErr_Format( PyVGX_QueryError, "Invalid vertex condition: '%s'", context.name );
              }
              THROW_SILENT( CXLIB_ERR_API, 0x7C5 );
            }
            THROW_SILENT( CXLIB_ERR_API, 0x7C6 );
          }
        }
        // Done parsing
        py_vertex_condition = NULL;
      }
    }
    XCATCH( errcode ) {
      if( !PyErr_Occurred() ) {
        PyErr_Format( PyVGX_QueryError, "Invalid specification for vertex condition '%s", context.name ? context.name : "???" );
      }
      iVertexCondition.Delete( &vertex_condition );
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  return vertex_condition;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t * __new_aggregation_mode_keymap( framehash_dynamic_t *dyn ) {
  framehash_cell_t *map = NULL;
  if( (map = iMapping.NewIntegerMap( dyn, "aggregation_mode_keymap.dyn" )) != NULL ) {
    iMapping.IntegerMapAdd( &map, dyn, "first",     VGX_AGGREGATE_ARC_FIRST_VALUE );
    iMapping.IntegerMapAdd( &map, dyn, "max",       VGX_AGGREGATE_ARC_MAX_VALUE );
    iMapping.IntegerMapAdd( &map, dyn, "min",       VGX_AGGREGATE_ARC_MIN_VALUE );
    iMapping.IntegerMapAdd( &map, dyn, "average",   VGX_AGGREGATE_ARC_AVERAGE_VALUE );
    iMapping.IntegerMapAdd( &map, dyn, "count",     VGX_AGGREGATE_ARC_COUNT );
    iMapping.IntegerMapAdd( &map, dyn, "sum",       VGX_AGGREGATE_ARC_ADD_VALUES );
    iMapping.IntegerMapAdd( &map, dyn, "sqsum",     VGX_AGGREGATE_ARC_ADD_SQ_VALUES );
    iMapping.IntegerMapAdd( &map, dyn, "product",   VGX_AGGREGATE_ARC_MULTIPLY_VALUES );
    if( !(iMapping.IntegerMapSize( map ) > 0) ) {
      iMapping.DeleteIntegerMap( &map, dyn );
    }
  }
  return map;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static vgx_RankingCondition_t * _ipyvgx_parser__new_ranking_condition( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, vgx_Vector_t *probe_vector ) {
  CString_t *CSTR__error = NULL;
  vgx_RankingCondition_t *ranking_condition = NULL;
  
  const char *expression = NULL;
  vgx_value_condition_t *valcond = NULL;


  vgx_ArcConditionSet_t *aggregate_condition_set = NULL;
  int64_t aggregate_deephits = 0;

  BEGIN_PYTHON_INTERPRETER {

    XTRY {
      // TODO: More fancy parsing etc, for now just pass the raw expression to the core
      if( py_rankspec && py_rankspec != Py_None ) {
        if( (expression = PyVGX_PyObject_AsString( py_rankspec )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_API, 0x7D1 );
        }
      }

      // Post-collect predicator filter for aggregated arcs
      if( py_aggregate && py_aggregate != Py_None ) {
        // Now we iterate over the items in the dict
        Py_ssize_t pos = 0;
        PyObject *py_key;
        PyObject *py_value;
        const char *key;

        if( !PyDict_Check( py_aggregate ) ) {
          PyVGXError_SetString( PyVGX_QueryError, "aggregate must be dict" );
          THROW_SILENT( CXLIB_ERR_API, 0x7D5 );
        }

        // Default aggregation is counting
        vgx_sortspec_t aggregate_mode = VGX_AGGREGATE_ARC_COUNT;

        // Iterate over all items in the dict
        while( PyDict_Next( py_aggregate, &pos, &py_key, &py_value ) ) {
          // get next property condition key
          if( (key = PyVGX_PyObject_AsString( py_key )) == NULL ) {
            THROW_SILENT( CXLIB_ERR_API, 0x7D6 );
          }

          // arc
          if( CharsEqualsConst( key, "arc" ) ) {
            // Create the aggregate condition
            if( (aggregate_condition_set = iPyVGXParser.NewArcConditionSet( graph, py_value, VGX_ARCDIR_ANY )) == NULL ) {
              THROW_SILENT( CXLIB_ERR_GENERAL, 0x7D7 );
            }
            if( aggregate_condition_set->arcdir != VGX_ARCDIR_ANY ) {
              PYVGX_API_WARNING( "parser", 0x7D8, "Arc direction ignored for aggregation arc" );
              aggregate_condition_set->arcdir = VGX_ARCDIR_ANY;
            }
            if( aggregate_condition_set->set ) {
              if( (*aggregate_condition_set->set)->modifier.bits == VGX_PREDICATOR_MOD_NONE ) {
                (*aggregate_condition_set->set)->modifier.bits = VGX_PREDICATOR_MOD_INT_AGGREGATOR;
              }
            }
          }

          // mode
          else if( CharsEqualsConst( key, "mode" ) ) {
            const char *mode = PyVGX_PyObject_AsString( py_value );
            if( mode == NULL ) {
              PyErr_SetString( PyVGX_QueryError, "Aggregation mode must be string" );
              THROW_SILENT( CXLIB_ERR_API, 0x7D8 );
            }

            int64_t am;
            if( !iMapping.IntegerMapGet( g_aggregation_mode_keymap, &g_aggregation_mode_keymap_dyn, mode, &am ) ) {
              PyErr_Format( PyVGX_QueryError, "Invalid aggregation mode: '%s'", mode );
              THROW_SILENT( CXLIB_ERR_API, 0x7D9 );
            }
            aggregate_mode = (vgx_sortspec_t)am;
          }

          // deephits
          else if( CharsEqualsConst( key, "deephits" ) ) {
            if( PyLong_Check( py_value ) ) {
              aggregate_deephits = PyLong_AsLongLong( py_value );
            }
            else {
              PyVGXError_SetString( PyVGX_QueryError, "Aggregation deephits must be integer" );
              THROW_SILENT( CXLIB_ERR_API, 0x7DA );
            }
          }
          // *invalid*
          else {
            PyVGXError_SetString( PyVGX_QueryError, "Invalid aggregation argument" );
            THROW_SILENT( CXLIB_ERR_API, 0x7DB );
          }
        }

        // Superimpose the aggregation mode on the sortspec
        sortspec |= aggregate_mode;
      }

      // Create the ranking condition
      if( (ranking_condition = iRankingCondition.New( graph, expression, sortspec, modifier, probe_vector, &aggregate_condition_set, aggregate_deephits, &CSTR__error )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x7DC );
      }
    }
    XCATCH( errcode ) {
      if( CSTR__error ) {
        PyVGXError_SetString( PyVGX_QueryError, CStringValue( CSTR__error ) );
        CStringDelete( CSTR__error );
      }
      if( valcond ) {
        free( valcond );
      }
    }
    XFINALLY {
      // iVertexProperty.Delete( &rankspec.prop );
      iArcConditionSet.Delete( &aggregate_condition_set );
    }
  } END_PYTHON_INTERPRETER;
  
  return ranking_condition;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static vgx_RankingCondition_t * _ipyvgx_parser__new_ranking_condition_ex( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, PyObject *py_rank_vector_object, vgx_VertexCondition_t *vertex_condition ) {
  vgx_RankingCondition_t *ranking_condition = NULL;
  vgx_Vector_t *rank_vector = NULL;

  // Create an internal vector from the supplied python object (list or other pyvector)
  if( py_rank_vector_object ) {
    if( (rank_vector = iPyVGXParser.InternalVectorFromPyObject( graph->similarity, py_rank_vector_object, NULL, true )) == NULL ) {
      return NULL;
    }
  }
  // Fallback to vertex similarity probe vector if one exists, and own a reference
  else if( vertex_condition ) {
    rank_vector = iVertexCondition.OwnSimilarityVector( vertex_condition );
  }

  // Create the ranking condition
  ranking_condition = iPyVGXParser.NewRankingCondition( graph, py_rankspec, py_aggregate, sortspec, modifier, rank_vector );
  
  // Clean up rank vector if used
  if( rank_vector ) {
    CALLABLE( rank_vector )->Decref( rank_vector );
  }

  return ranking_condition;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static vgx_ExpressEvalMemory_t * _ipyvgx_parser__new_express_eval_memory( vgx_Graph_t *graph, PyObject *py_object ) {
  vgx_ExpressEvalMemory_t *evalmem = NULL;
  if( PyVGX_Memory_CheckExact( py_object ) ) {
    PyVGX_Memory *pymem = (PyVGX_Memory*)py_object;
    if( pymem->py_parent->graph != graph ) {
      PyVGXError_SetString( PyVGX_AccessError, "invalid memory object (graph mismatch)" );
      return NULL;
    }
    else if( pymem->threadid != GET_CURRENT_THREAD_ID() ) {
      PyVGXError_SetString( PyVGX_AccessError, "invalid evaluator memory (thread mismatch)" );
      return NULL;
    }
    else {
      iEvaluator.OwnMemory( pymem->evalmem );
      return pymem->evalmem;
    }
  }
  else if( PyCapsule_CheckExact( py_object ) ) {
    // ASSUMPTION: Capsule passed by trusted internal code
    if( (evalmem = PyCapsule_GetPointer( py_object, NULL )) == NULL ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
      return NULL;
    }
    iEvaluator.OwnMemory( evalmem );
    return evalmem;
  }
  else if( PyList_CheckExact( py_object ) ) {
    int64_t sz = PyList_Size( py_object );
    int order = imag2( sz );
    if( order < 2 ) {
      order = 2;
    }
    if( (evalmem = iEvaluator.NewMemory( order )) == NULL ) {
      PyVGXError_SetString( PyExc_MemoryError, "Out of memory" );
      return NULL;
    }
    if( sz > 0 ) {
      vgx_EvalStackItem_t *cursor = evalmem->data;
      vgx_EvalStackItem_t *last = cursor + evalmem->mask;
      int64_t idx = 0;
      do {
        PyObject *py_item = PyList_GET_ITEM( py_object, idx );
        if( PyFloat_CheckExact( py_item ) ) {
          cursor->type = STACK_ITEM_TYPE_REAL;
          cursor->real = PyFloat_AS_DOUBLE( py_item );
        }
        else if( PyLong_CheckExact( py_item ) ) {
          cursor->type = STACK_ITEM_TYPE_INTEGER;
          cursor->integer = PyLong_AsLongLong( py_item );
        }
        ++idx;
      } while( cursor++ < last && idx < sz );
    }
  }
  else {
    int64_t sz = -1;
    if( PyLong_CheckExact( py_object ) ) {
      sz = PyLong_AsLongLong( py_object );
    }

    if( sz < 4 ) {
      PyVGXError_SetString( PyExc_ValueError, "size must be at least 4" );
      return NULL;
    }

    if( (evalmem = iEvaluator.NewMemory( imag2(sz) )) == NULL ) {
      PyVGXError_SetString( PyExc_MemoryError, "Out of memory" );
      return NULL;
    }
  }

  return evalmem;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static vgx_Vector_t * _ipyvgx_parser__internal_vector_from_pyobject( vgx_Similarity_t *simcontext, PyObject *py_object, PyObject *py_alpha, bool ephemeral ) {
  vgx_Vector_t *vector = NULL;

  if( py_object != NULL ) {

    CString_t *CSTR__error = NULL;

    // Object is a PyVGX_Vector: extract the vector and own a reference
    if( PyVGX_Vector_CheckExact( py_object ) ) {
      if( (vector = ((PyVGX_Vector*)py_object)->vint) != NULL ) {
        vgx_Similarity_t *vector_simcontext = CALLABLE( vector )->Context( vector )->simobj;
        if( vector_simcontext == simcontext ) {
          BEGIN_PYVGX_THREADS {
            CALLABLE( vector )->Incref( vector );
          } END_PYVGX_THREADS;
        }
        else {
          PyVGXError_SetString( PyExc_ValueError, "Incompatible similarity context for vector object!" );
          vector = NULL;
        }
      }
      else {
        PyVGXError_SetString( PyExc_ValueError, "Unable to extract internal vector from vector object" );
      }
    }
    // Object is a PyVGX_Vertex: extract the vector from the vertex and own a reference
    else if( PyVGX_Vertex_CheckExact( py_object ) ) {
      vgx_Vertex_t *vertex = ((PyVGX_Vertex*)py_object)->vertex;
      if( vertex ) {
        if( (vector = vertex->vector) != NULL && !CALLABLE(vector)->IsNull(vector) ) {
          vgx_Similarity_t *vector_simcontext = CALLABLE( vector )->Context( vector )->simobj;
          if( vector_simcontext == simcontext ) {
            BEGIN_PYVGX_THREADS {
              CALLABLE( vector )->Incref( vector );
            } END_PYVGX_THREADS;
          }
          else {
            PyVGXError_SetString( PyExc_ValueError, "Incompatible similarity context for vector object!" );
            vector = NULL;
          }
        }
        else {
          PyVGXError_SetString( PyExc_ValueError, "Vertex has NULL-vector" );
        }
      }
      else {
        PyVGXError_SetString( PyVGX_AccessError, "Vertex is not accessible" );
      }
    }
    // Object is bytes or bytearray: reconstruct vector bytes
    else if( PyBytes_Check( py_object ) || PyByteArray_Check( py_object ) ) {
      if( !igraphfactory.EuclideanVectors() ) {
        PyVGXError_SetString( PyExc_TypeError, "Bytes not allowed in non-Euclidean vector mode" );
        return NULL;
      }
      const char *data = NULL;
      Py_ssize_t sz = 0;
      if( PyBytes_Check( py_object ) ) {
        PyBytes_AsStringAndSize( py_object, (char**)&data, &sz );
      }
      else {
        data = PyByteArray_AsString( py_object );
        sz = PyByteArray_Size( py_object );
      }
      if( data == NULL ) {
        PyVGXError_SetString( PyExc_ValueError, "Invalid bytes" );
        return NULL;
      }
      double alpha = 1.0;
      if( py_alpha ) {
        if( PyFloat_Check( py_alpha ) ) {
          alpha = PyFloat_AsDouble( py_alpha );
        }
        else if( PyLong_Check( py_alpha ) ) {
          alpha = (double)PyLong_AsLongLong( py_alpha );
        }
        else {
          PyVGXError_SetString( PyExc_TypeError, "alpha must be numeric" );
          return NULL;
        }
      }
      BEGIN_PYVGX_THREADS {
        // Either ephemeral or persistent vector
        vector = CALLABLE( simcontext )->NewInternalVector( simcontext, data, (float)alpha, (uint16_t)sz, ephemeral );
      } END_PYVGX_THREADS;
      if( vector == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Unable to create new internal vector from bytes" );
      }
    }
    //
    else {
      int vlen = 0;
      void *external_elements = NULL;
      if( py_object != Py_None ) {
        // EUCLIDEAN VECTOR
        if( igraphfactory.EuclideanVectors() ) {
          // Object (other than Py_None) must now be a list of floats [f0,f1,f2,...] to be successfully parsed
          if( PySequence_Check( py_object ) ) {
            vlen = _ipyvgx_parser__parse_external_euclidean_elements( py_object, (float**)&external_elements );
          }
          else {
            vlen = -1;
          }
        }
        // FEATURE VECTOR
        else {
          // Object must now be a list [ ( <dim>, <mag> ), ... ] to be successfully parsed
          if( PySequence_Check( py_object ) ) {
            vlen = _ipyvgx_parser__parse_external_map_elements( py_object, (ext_vector_feature_t**)&external_elements );
          }
          else {
            vlen = -1;
          }
        }
      }
      if( vlen >= 0 ) {
        BEGIN_PYVGX_THREADS {
          // Either ephemeral or persistent vector
          vector = CALLABLE( simcontext )->NewInternalVectorFromExternal( simcontext, external_elements, (uint16_t)vlen, ephemeral, &CSTR__error );
        } END_PYVGX_THREADS;
        if( vector == NULL ) {
          if( CSTR__error ) {
            PyVGXError_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
            CStringDelete( CSTR__error );
          }
          else {
            PyVGXError_SetString( PyExc_Exception, "Unable to create new internal vector from external elements" );
          }
        }
      }
      else {
        PyVGXError_SetString( PyVGX_QueryError, "Vector must be given as list, tuple, bytes, pyvgx.Vector, or pyvgx.Vertex" );
      }
      if( external_elements ) {
        free( external_elements );
      }
    }
  }
  else {
    PyVGXError_SetString( PyVGX_QueryError, "Internal error (NULL-object)" );
  }

  return vector;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static vgx_StringList_t * _ipyvgx_parser__new_string_list_from_vertex_pylist( PyObject *py_list ) {
  vgx_StringList_t *list = NULL;
  int64_t sz;
  if( (sz = PyList_Size( py_list )) >= 0 ) {
    if( (list = iString.List.New( NULL, sz )) == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      return NULL;
    }

    XTRY {
      for( int64_t i=0; i<sz; i++ ) {
        PyObject *py_item = PyList_GET_ITEM( py_list, i );
        pyvgx_VertexIdentifier_t ident;
        if( iPyVGXParser.GetVertexID( NULL, py_item, &ident, NULL, true, "Vertex ID" ) < 0 ) {
          PyErr_SetString( PyExc_ValueError, "a string or vertex instance is required" );
          THROW_SILENT( CXLIB_ERR_API, 0x001 );
        }

        if( iString.List.SetItem( list, i, ident.id ) == NULL ) {
          PyErr_SetNone( PyExc_MemoryError );
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
      }
    }
    XCATCH( errcode ) {
      iString.List.Discard( &list );
    }
    XFINALLY {
    }

  }
  return list;
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static void _ipyvgx_parser__delete_string_list( CString_t ***plist ) {
  if( plist && *plist ) {
    CString_t **list = *plist;
    CString_t **cursor = list;
    CString_t *CSTR__str;
    while( (CSTR__str = *cursor++) != NULL ) {
      CStringDelete( CSTR__str );
    }
    free( *plist );
    *plist = NULL;
  }
}




DLL_HIDDEN IPyVGXParser iPyVGXParser = {
  .Initialize                     = _ipyvgx_parser__initialize,
  .Destroy                        = _ipyvgx_parser__destroy,
  .GetVertexID                    = _ipyvgx_parser__get_vertex_id,
  .NewRelation                    = _ipyvgx_parser__new_relation,
  .ParseValueCondition            = _ipyvgx_parser__parse_value_condition,
  .NewArcConditionSet             = _ipyvgx_parser__new_arc_condition_set,
  .NewVertexCondition             = _ipyvgx_parser__new_vertex_condition,
  .NewRankingCondition            = _ipyvgx_parser__new_ranking_condition,
  .NewRankingConditionEx          = _ipyvgx_parser__new_ranking_condition_ex,
  .NewExpressEvalMemory           = _ipyvgx_parser__new_express_eval_memory,
  .ExternalMapElements            = _ipyvgx_parser__parse_external_map_elements,
  .ExternalEuclideanElements      = _ipyvgx_parser__parse_external_euclidean_elements,
  .InternalVectorFromPyObject     = _ipyvgx_parser__internal_vector_from_pyobject,
  .NewStringListFromVertexPyList  = _ipyvgx_parser__new_string_list_from_vertex_pylist
};
