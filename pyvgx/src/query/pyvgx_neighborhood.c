/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_neighborhood.c
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
__inline static int64_t __recursion_s2bw( int64_t shadow_size ) { 
#define sqrt_2 1.4142135623730951
  int64_t bw = (int64_t)round( sqrt_2 * log2( (double)shadow_size ) ) - 5;
  return maximum_value( bw, 2 );
}


 
/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static double __recursion_s2delta( int64_t shadow_size ) { 
#define s2delta_s0    175.0
#define s2delta_w     2.0
#define s2delta_A     1.6
#define s2delta_B     0.6 
#define s2delta_d_min -0.7
#define s2delta_d_max 1.0
    
  double x = log2((double)shadow_size) - log2(s2delta_s0);
  double x2 = x * x;
  double delta = s2delta_A * exp( -x2 / s2delta_w ) - s2delta_B;

  return clamp_value(delta, s2delta_d_min, s2delta_d_max);
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static double __recursion_b2epsilon( double x ) { 
#define b2epsilon_min   -0.00130
#define b2epsilon_max    0.00100
#define b2epsilon_a      0.00150
#define b2epsilon_b      0.00090
#define b2epsilon_c     -0.00080
#define b2epsilon_d     -0.00055 
#define X_3(x) (x*x*x)
#define X_2(x) (x*x)

  double epsilon = b2epsilon_a * X_3(x) + b2epsilon_b * X_2(x) + b2epsilon_c * x + b2epsilon_d;

  return clamp_value( epsilon, b2epsilon_min, b2epsilon_max );

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static double __recursion_bias2omega( double bias ) {
#define bias2omega_x0  -100.0
#define bias2omega_x1   -50.0
#define bias2omega_x2     0.0
#define bias2omega_x3    75.0
#define bias2omega_x4    95.0
#define bias2omega_x5   100.0

#define bias2omega_o0     0.9
#define bias2omega_o1     0.9
#define bias2omega_o2     0.9
#define bias2omega_o3     0.7
#define bias2omega_o4     0.5
#define bias2omega_o5     0.5

#define bias2omega_a0   ((bias2omega_o1 - bias2omega_o0) / (bias2omega_x1 - bias2omega_x0))
#define bias2omega_a1   ((bias2omega_o2 - bias2omega_o1) / (bias2omega_x2 - bias2omega_x1))
#define bias2omega_a2   ((bias2omega_o3 - bias2omega_o2) / (bias2omega_x3 - bias2omega_x2))
#define bias2omega_a3   ((bias2omega_o4 - bias2omega_o3) / (bias2omega_x4 - bias2omega_x3))
#define bias2omega_a4   ((bias2omega_o5 - bias2omega_o4) / (bias2omega_x5 - bias2omega_x4))

#define bias2omega_b0   (bias2omega_o1 - bias2omega_a0 * bias2omega_x1)
#define bias2omega_b1   (bias2omega_o2 - bias2omega_a1 * bias2omega_x2)
#define bias2omega_b2   (bias2omega_o3 - bias2omega_a2 * bias2omega_x3)
#define bias2omega_b3   (bias2omega_o4 - bias2omega_a3 * bias2omega_x4)
#define bias2omega_b4   (bias2omega_o5 - bias2omega_a4 * bias2omega_x5)

  double a, b;

  bias = clamp_value( bias, -100.0, 100.0 );

  // -100 to -40
  if( bias < bias2omega_x1 ) {
    a = bias2omega_a0;
    b = bias2omega_b0;
  }
  // -40 to 0
  else if( bias < bias2omega_x2 ) {
    a = bias2omega_a1;
    b = bias2omega_b1;
  }
  // 0 - 40
  else if( bias < bias2omega_x3 ) {
    a = bias2omega_a2;
    b = bias2omega_b2;
  }
  // 40 - 90
  else if( bias < bias2omega_x4 ) {
    a = bias2omega_a3;
    b = bias2omega_b3;
  }
  // 90 - 100
  else {
    a = bias2omega_a4;
    b = bias2omega_b4;
  }

  return a * bias + b;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __recursion_auto_param( double search_bias, vgx_recursion_config_t *config ) {
  double normalized_bias = search_bias / 100.0;
  double b = clamp_value( normalized_bias, -1.0, 1.0 );

  // shadow_size
  // Length of delay line for threshold EMA tap
  /*
  Map search_bias:
  
       0.0 - 1.0:  2 ** (1+b2**2)
      -1.0 - 0.0:  2 ** (1-b2**2)
  
   Log plot: flat around 0.0->256 and steeper around -1.0->1 and +1.0->65536
   Linear plot (-1.0 to 0.0 range): gentle S-curve
  
     b   shadow_size
    -1.0 1
    -0.9 3
    -0.8 7
    -0.7 17
    -0.6 35
    -0.5 64
    -0.4 105
    -0.3 155
    -0.2 205
    -0.1 242
     0.0 256
     0.1 271
     0.2 320
     0.3 422
     0.4 622
     0.5 1024
     0.6 1885
     0.7 3875
     0.8 8903
     0.9 22851
     1.0 65536
  */


  // Normal recall regime
  if( b > -0.9 ) {
    // Shadow size is the main tuning knob
    config->shadow.size = (int64_t)round( exp2( 8 + 8 * b * fabs(b) ) );

    // Limit depth to avoid runaway recursion in pathological graphs
    config->limit.depth = 1024;
  
    // Beam width grows slowly from min=2 and up
    config->beam.width = __recursion_s2bw( config->shadow.size );
    config->beam.min_width = config->beam.width;
    config->beam.max_width = 16 * config->beam.width * config->beam.width;
    config->init.select = config->beam.width;

    // Beam controller most sensitive in the low-mid recall regime, less sensitive for junk (low) recall and extreme (high) recall
    config->tune.delta = __recursion_s2delta( config->shadow.size );

    // Score contribution threshold
    config->tune.epsilon = __recursion_b2epsilon( b );

  }
  // Junk recall regime (-1.0 to -0.9)
  else {
    // shadow: 0 - 5
    double fshw =  50.0 * fabs(1.0 + b);
    config->shadow.size = (int64_t)round( fshw );
    
    // depth: 4 - 19
    config->limit.depth = 4 + (int64_t)round(3.0 * fshw);

    // visits: 64 - 6464
    config->limit.visit = 64 + (int64_t)round( 64000.0 * fabs(1.0 + b) );

    // Fixed beam
    config->beam.width = 2;
    config->beam.min_width = 2;
    config->beam.min_width = 2;
    config->beam.adaptive_taper = false;
    config->init.select = 2;

    // EMA alpha: 0.5 - 0.2
    config->tune.zeta = 0.5 - 3 * fabs(1.0 + b);
  }

  // Apply global optimization weight
  double omega = clamp_value( config->tune.omega, 0.05, 2.0 );
  if( fabs(omega - 1.0) > 1e-6 ) {
    config->tune.alpha *= omega;
    config->tune.beta *= omega;
    config->tune.gamma *= omega;
    config->tune.delta *= omega;
    config->tune.epsilon *= omega;
    config->tune.zeta *= omega;
  }

  return 0;
}




struct s_recursion_config_param;

typedef int (*f_parse_recursion_parameter)( PyVGX_Graph *pygraph, PyObject *py_value, const struct s_recursion_config_param *cursor, vgx_recursion_config_t *target );
typedef void (*f_init_recursion_parameter)( const struct s_recursion_config_param *cursor, vgx_recursion_config_t *target );


typedef struct s_recursion_config_param {
  const char *name;
  f_parse_recursion_parameter parse;
  f_init_recursion_parameter initialize;
  int target_offset;
  union {
    struct {
      const int64_t dflt;
      const int64_t minval;
      const int64_t maxval;
    } i64;
    struct {
      const double dflt;
      const double minval;
      const double maxval;
    } f64;
    struct {
      const bool dflt;
      const bool __rsv1;
      const QWORD __rsv2;
      const QWORD __rsv3;
    } b32;
    struct {
      const QWORD __rsv1;
      const QWORD __rsv2;
      const QWORD __rsv3;
    } obj;
  } value;
} recursion_config_param;



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int64_t set_i64_target( vgx_recursion_config_t *recursion, const recursion_config_param *cursor, int64_t value ) {
  return *(int64_t*)((char*)recursion + cursor->target_offset) = value;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static double set_f64_target( vgx_recursion_config_t *recursion, const recursion_config_param *cursor, double value ) {
  return *(double*)((char*)recursion + cursor->target_offset) = value;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static bool set_bool_target( vgx_recursion_config_t *recursion, const recursion_config_param *cursor, bool value ) {
  return *(bool*)((char*)recursion + cursor->target_offset) = value;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static void * set_object_target( vgx_recursion_config_t *recursion, const recursion_config_param *cursor, void *value ) {
  return *(void**)((char*)recursion + cursor->target_offset) = value;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int parse_recursion_parameter_i64( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( !PyLong_Check(py_value) ) {
    PyErr_Format( PyExc_TypeError, "recursive search invalid %s: %R (int required)", cursor->name, py_value );
    return -1;
  }
  int64_t value = PyLong_AsLongLong( py_value );
  if( value > cursor->value.i64.maxval || value < cursor->value.i64.minval ) {
    CString_t *CSTR__range = CStringNewFormat( "not in [%lld, %lld]", cursor->value.i64.minval, cursor->value.i64.maxval );
    PyErr_Format( PyExc_TypeError, "recursive search %s out of range: %R %s", cursor->name, py_value, CStringValueDefault(CSTR__range,"") );
    iString.Discard( &CSTR__range );
    return -1;
  }
  set_i64_target( target, cursor, value );
  return 0;
}

static void init_recursion_parameter_i64( const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  set_i64_target( target, cursor, cursor->value.i64.dflt );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int parse_recursion_parameter_f64( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( !PyNumber_Check(py_value) ) {
    PyErr_Format( PyExc_TypeError, "recursive search invalid %s: %R (number required)", cursor->name, py_value );
    return -1;
  }
  double value = PyFloat_Check(py_value) ? PyFloat_AsDouble(py_value) : (double)PyLong_AsLongLong(py_value);
  if( value > cursor->value.f64.maxval || value < cursor->value.f64.minval ) {
    CString_t *CSTR__range = CStringNewFormat( "not in [%g, %g]", cursor->value.f64.minval, cursor->value.f64.maxval );
    PyErr_Format( PyExc_TypeError, "recursive search %s out of range: %R %s", cursor->name, py_value, CStringValueDefault(CSTR__range,"") ); \
    iString.Discard( &CSTR__range );
    return -1;
  }
  set_f64_target( target, cursor, value );
  return 0;
}

static void init_recursion_parameter_f64( const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  set_f64_target( target, cursor, cursor->value.f64.dflt );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int parse_recursion_parameter_b32( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( py_value == Py_True || (PyLong_Check(py_value) && PyLong_AS_LONG(py_value) > 0) ) {
    set_bool_target( target, cursor, true );
    return 0;
  }
  else if( py_value == Py_False || (PyLong_Check(py_value) && PyLong_AS_LONG(py_value) <= 0) ) {
    set_bool_target( target, cursor, false );
    return 0;
  }
  else {
    PyErr_Format( PyExc_TypeError, "recursive search invalid %s: %R (bool or int required)", cursor->name, py_value );
    return -1;
  }
}

static void init_recursion_parameter_b32( const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  set_bool_target( target, cursor, cursor->value.b32.dflt );
}




/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int parse_recursion_vector( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( py_value == Py_None ) {
    return 0;
  }
  vgx_Vector_t *vector = iPyVGXParser.InternalVectorFromPyObject( pygraph->graph->similarity, py_value, NULL, true, true );
  if( vector == NULL ) {
    PyVGXError_Format( PyExc_TypeError, "recursive search invalid %s: %R", cursor->name, py_value );
    return -1;
  }
  set_object_target( target, cursor, vector );
  return 0;
}
 


/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int parse_recursion_filter( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( py_value == Py_None ) {
    return 0;
  }
  if( !PyUnicode_Check( py_value ) ) {
    PyVGXError_Format( PyExc_TypeError, "recursive search invalid %s: %R", cursor->name, py_value );
    return -1;
  }
  const char *recursion_filter = PyUnicode_AsUTF8( py_value );
  if( recursion_filter == NULL ) {
    return -1;
  }
  CString_t *CSTR__filter = iString.New( NULL, recursion_filter );
  if( CSTR__filter == NULL ) {
    return -1;
  }
  set_object_target( target, cursor, CSTR__filter );
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int parse_recursion_parameter_obj( PyVGX_Graph *pygraph, PyObject *py_value, const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  if( CharsEqualsConst( cursor->name, "vector" ) ) {
    return parse_recursion_vector( pygraph, py_value, cursor, target );
  }
  else if( CharsEqualsConst( cursor->name, "filter" ) ) {
    return parse_recursion_filter( pygraph, py_value, cursor, target );
  }
  PyErr_Format( PyExc_ValueError, "unexpected object key: %s", cursor->name );
  return -1;
}

static void init_recursion_parameter_obj( const recursion_config_param *cursor, vgx_recursion_config_t *target ) {
  set_object_target( target, cursor, NULL );
}




    
/******************************************************************************
 *
 ******************************************************************************
 */
#define INT64_FIELD( Name, Field, DefaultVal, MinVal, MaxVal ) \
{ .name = Name, \
  .parse = parse_recursion_parameter_i64, \
  .initialize = init_recursion_parameter_i64, \
  .target_offset = offsetof(vgx_recursion_config_t, Field), \
  .value = { .i64 = { .dflt = (DefaultVal), .minval = (MinVal), .maxval = (MaxVal) } } }


/******************************************************************************
 *
 ******************************************************************************
 */
#define DOUBLE_FIELD( Name, Field, DefaultVal, MinVal, MaxVal ) \
{ .name = Name, \
  .parse = parse_recursion_parameter_f64, \
  .initialize = init_recursion_parameter_f64, \
  .target_offset = offsetof(vgx_recursion_config_t, Field), \
  .value = { .f64 = { .dflt = (DefaultVal), .minval = (MinVal), .maxval = (MaxVal) } } }


/******************************************************************************
 *
 ******************************************************************************
 */
#define BOOL_FIELD( Name, Field, DefaultVal ) \
{ .name = Name, \
  .parse = parse_recursion_parameter_b32, \
  .initialize = init_recursion_parameter_b32, \
  .target_offset = offsetof(vgx_recursion_config_t, Field), \
  .value = { .b32 = { .dflt = (DefaultVal) } } }


/******************************************************************************
 *
 ******************************************************************************
 */
#define OBJECT_FIELD( Name, Field ) \
{ .name = Name, \
  .parse = parse_recursion_parameter_obj, \
  .initialize = init_recursion_parameter_obj, \
  .target_offset = offsetof(vgx_recursion_config_t, Field) }


/******************************************************************************
 *
 ******************************************************************************
 */
static const recursion_config_param g_config[] = {
  INT64_FIELD("heap_size",        heap.size,            1,        0,      VGX_RECURSION_HEAP_SIZE_MAX),  // default 1=follow hits
  INT64_FIELD("shadow_size",      shadow.size,          -1,       -1,     VGX_RECURSION_HEAP_SHADOW_MAX),  // default -1=auto
  INT64_FIELD("frontier_limit",   limit.frontier,       0,        0,      VGX_RECURSION_FRONTIER_SIZE_MAX),  // default 0=auto
  INT64_FIELD("expansion_limit",  limit.expansion,      INT_MAX,  0,      INT_MAX),
  INT64_FIELD("depth_limit",      limit.depth,          INT_MAX,  0,      INT_MAX),
  INT64_FIELD("exec_ms_limit",    limit.exec_ms,        -1,       -1,     LLONG_MAX),                // default -1=unlimited
  INT64_FIELD("visit_limit",      limit.visit,          INT_MAX,  0,      INT_MAX),
  INT64_FIELD("beam_width",       beam.width,           0,        0,      VGX_RECURSION_BEAM_SIZE_MAX),  // default 0=off
  INT64_FIELD("beam_min",         beam.min_width,       1,        1,      VGX_RECURSION_BEAM_SIZE_MAX),
  INT64_FIELD("beam_max",         beam.max_width,       0,        0,      VGX_RECURSION_BEAM_SIZE_MAX),  // default 0=auto
  INT64_FIELD("init_select",      init.select,          0,        0,      1024),                         // default 0=off
  INT64_FIELD("kappa",            tune.kappa,           0,        0,      256),          // default 0
  INT64_FIELD("lambda",           tune.lambda,          0,        0,      256),          // default 0
  
  DOUBLE_FIELD("beam_curve",      beam.curve,           0.99,     0.0,    1.0),         // default 0.99=gentle taper
  DOUBLE_FIELD("alpha",           tune.alpha,           -0.32,    -10.0,  10.0),        // default -0.32 (depth discount for expansion threshold)
  DOUBLE_FIELD("beta",            tune.beta,            0.0,      -10.0,  10.0),        // default 0.0 (evals discount for expansion threshold)
  DOUBLE_FIELD("gamma",           tune.gamma,           0.0,      -10.0,  10.0),        // default 0.0 (global threshold offset, positive means more expansions)
  DOUBLE_FIELD("delta",           tune.delta,           0.0,      -1.0,   10.0),        // default 0.0 (beam controller reactivity)
  DOUBLE_FIELD("epsilon",         tune.epsilon,         0.0,      -1.0,   1.0),         // default 0.0 (score contribution threshold discount)
  DOUBLE_FIELD("zeta",            tune.zeta,            0.2,      0.0,    1.0),         // default 0.2 (threshold EMA alpha)
  
  BOOL_FIELD("reset_metrics",     visit.reset_metrics,  true),
  BOOL_FIELD("reset_map",         visit.reset_map,      true),
  BOOL_FIELD("adaptive_taper",    beam.adaptive_taper,  true),
  
  OBJECT_FIELD("vector",          probe),
  OBJECT_FIELD("filter",          visit.CSTR__filter),

  {0}
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const recursion_config_param g_pre_config[] = {
  DOUBLE_FIELD("bias",            bias,                 0.0,      -100.0, 100.0),       // default 0.0 (balanced high recall good QPS)
  DOUBLE_FIELD("omega",           tune.omega,           1.0,      0.0,    2.0),         // default 0.7 (all optimizations weight)
  {0}
};
    

/******************************************************************************
 *
 ******************************************************************************
 */
static const recursion_config_param *g_configs[] = {
  g_config,
  g_pre_config,
  NULL
 };



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int invalid_recursion_parameter( PyObject *py_recursion ) {
  Py_ssize_t pos = 0;
  PyObject *py_key, *py_value;
  while( PyDict_Next(py_recursion, &pos, &py_key, &py_value) ) {
    if( !PyUnicode_Check( py_key ) ) {
      PyErr_Format( PyExc_TypeError, "recursive search invalid parameter name: %R (string required)", py_key );
      return -1;
    }
    const char *key = PyUnicode_AsUTF8( py_key );
    const recursion_config_param **pconfig = g_configs;
    const recursion_config_param *cursor;
    while( (cursor = *pconfig++) != NULL ) {
      while( cursor->name ) {
        if( CharsEqualsConst( key, cursor->name ) ) {
          goto found;
        }
        ++cursor;
      }
    }
    PyErr_Format( PyExc_ValueError, "recursive search unknown parameter name: %R", py_key );
    return -1;
  found:
    continue;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _pyvgx_Neighborhood__parse_recursion( PyVGX_Graph *pygraph, PyObject *py_recursion, __neighborhood_query_args *param ) {
  int ret = 0;
  XTRY {

    // Sorting required
    if( _vgx_sortby( param->sortspec ) == VGX_SORTBY_NONE || _vgx_sortspec_dontcare( param->sortspec ) ) {
      PyErr_SetString( PyExc_ValueError, "recursive search requires specific sortby" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    // Query arc direction must be D_OUT
    if( param->arc_condition_set->arcdir != VGX_ARCDIR_OUT ) {
      PyErr_SetString( PyExc_ValueError, "recursive search requires arc direction D_OUT" );
      THROW_SILENT( CXLIB_ERR_API, 0x002 );
    }

    // Populate default values
    const recursion_config_param **pconfig = g_configs;
    const recursion_config_param *cursor;
    while( (cursor = *pconfig++) != NULL ) {
      while( cursor->name ) {
        cursor->initialize( cursor, &param->recursion );
        ++cursor;
      }
    }
    
    bool omega_override = false;
    int64_t nparams = 0;

    // Parse the preconfig
    if( PyDict_Check( py_recursion ) ) {
      nparams = PyDict_Size( py_recursion );
      cursor = g_pre_config;
      const char *key;
      PyObject *py_value;
      while( (key=cursor->name) != NULL ) {
        if( (py_value = PyDict_GetItemString(py_recursion, key)) != NULL ) {
          --nparams;
          if( cursor->parse( pygraph, py_value, cursor, &param->recursion ) < 0 ) {
            THROW_SILENT( CXLIB_ERR_API, 0x009 );
          }
          if( CharsEqualsConst(key, "omega") ) {
            omega_override = true;
          }
        }
        ++cursor;
      }
    }

    if( !omega_override ) {
      param->recursion.tune.omega = __recursion_bias2omega( param->recursion.bias );
    }

    // Auto config
    __recursion_auto_param( param->recursion.bias, &param->recursion );

    // Simple auto-config
    if( py_recursion == Py_True ) {
      // use defaults
    }
    // No recursion
    else if( py_recursion == Py_False ) {
      param->recursion.mode = VGX_RECURSION_MODE_NONE;
    }
    // { "heap_shadow": 256, "frontier_limit": 1024, ... }
    else if( PyDict_Check( py_recursion) ) {
      cursor = g_config;
      const char *key;
      PyObject *py_value;
      while( (key=cursor->name) != NULL ) {
        if( (py_value = PyDict_GetItemString(py_recursion, key)) != NULL ) {
          --nparams;
          if( cursor->parse( pygraph, py_value, cursor, &param->recursion ) < 0 ) {
            THROW_SILENT( CXLIB_ERR_API, 0x00A );
          }
        }
        ++cursor;
      }

      // Some supplied params were not recognized
      if( nparams != 0 ) {
        if( invalid_recursion_parameter( py_recursion ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_API, 0x00D );
        }
      }

    }
    // unsupported
    else {
      PyErr_Format( PyExc_ValueError, "recursive search parameter must be bool or dict" );
      THROW_SILENT( CXLIB_ERR_API, 0x00E );
    }
          
    // Beam mode
    if( param->recursion.limit.frontier == 0 && param->recursion.beam.width > 0 ) {
      param->recursion.mode = VGX_RECURSION_MODE_BEAM_PROGRESSIVE;
    }
    // Frontier queue mode
    else if( param->recursion.limit.frontier > 0 ) {
      param->recursion.beam.width = 0;
      param->recursion.mode = VGX_RECURSION_MODE_BFS_PROGRESSIVE;
    }
    else {
      PyErr_Format( PyExc_ValueError, "positive beam_width or frontier_limit required" );
      THROW_SILENT( CXLIB_ERR_API, 0x00F );
    }

    // No recursion vector specified, inherit from ranking condition if we have one
    if( param->recursion.probe == NULL ) {
      if( param->ranking_condition && param->ranking_condition->vector ) {
        param->recursion.probe = param->ranking_condition->vector;
        CALLABLE(param->recursion.probe)->Incref(param->recursion.probe);
      }
    }

  }
  XCATCH( errcode ) {
    iString.Discard( &param->recursion.visit.CSTR__filter );
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
    if( param->recursion.probe ) {
      CALLABLE(param->recursion.probe)->Decref(param->recursion.probe);
      param->recursion.probe = NULL;
    }
    if( param->recursion.visit.CSTR__filter ) {
      iString.Discard( &param->recursion.visit.CSTR__filter );
    }

    iString.Discard( &param->implied.CSTR__error );
  }
}







PyVGX_DOC( pyvgx_Neighborhood__doc__,
  "Neighborhood( id, arc=(None,D_OUT), pre=None, filter=None, post=None, neighbor=\"*\", vector=[], collect=C_COLLECT, recursion=False, result=R_STR, fields=F_ID, nest=0, nested_hits=-1, select=None, rank=None, sortby=S_NONE, aggregate=None, memory=4, offset=0, hits=-1, timeout=0, limexec=False ) -> list\n"
  "\n"
  "Perform a neighborhood search around vertex 'id'.\n"
  "\n"
);

/**************************************************************************//**
 * _pyvgx_Neighborhood__parse_params
 *
 ******************************************************************************
 */
static __neighborhood_query_args * _pyvgx_Neighborhood__parse_params( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds, __neighborhood_query_args *param, bool reusable ) {

  #define NEIGHBORHOOD_BASE_FORMAT "|OOz#z#z#OOOOIIiLz#OIOOi"

  static char fmt[] = NEIGHBORHOOD_BASE_FORMAT "Liii";
  static char *kwlist[] = {
    "id",         //  O
    "arc",        //  O
    "pre",        //  z
    "filter",     //  z
    "post",       //  z
    "neighbor",   //  O
    "vector",     //  O
    "collect",    //  O
    "recursion",  //  O
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

  static char fmt_reusable[] = NEIGHBORHOOD_BASE_FORMAT;
  static char *kwlist_reusable[] = {
    "id",         //  O
    "arc",        //  O
    "pre",        //  z
    "filter",     //  z
    "post",       //  z
    "neighbor",   //  O
    "vector",     //  O
    "collect",    //  O
    "recursion",  //  O
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
  PyObject *py_recursion = NULL;
  PyObject *py_evalmem = NULL;

  PyObject *py_filter = NULL;
  
  if( reusable ) {
    // Parse, reusable context
    if( !PyArg_ParseTupleAndKeywords( args, kwds, fmt_reusable, kwlist_reusable,
      &py_anchor,                     // O id
      &py_arc_condition,              // O arc
      &param->pre_expr,               // z pre
      &sz_pre,                        // # pre
      &param->filter_expr,            // z filter
      &sz_filter,                     // # filter
      &param->post_expr,              // z post
      &sz_post,                       // # post
      &py_vertex_condition,           // O neighbor
      &py_rank_vector_object,         // O vector
      &py_collect,                    // O collect
      &py_recursion,                  // O recursion
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
      &sz_pre,                        // # pre
      &param->filter_expr,            // z filter
      &sz_filter,                     // # filter
      &param->post_expr,              // z post
      &sz_post,                       // # post
      &py_vertex_condition,           // O neighbor
      &py_rank_vector_object,         // O vector
      &py_collect,                    // O collect
      &py_recursion,                  // O recursion
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

    // ------
    // memory
    // ------
    if( py_evalmem || param->filter_expr || py_vertex_condition || py_recursion ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00E );
      }
    }

    // --------
    // neighbor
    // --------
    if( py_vertex_condition ) {
      if( (param->vertex_condition = iPyVGXParser.NewVertexCondition( param->implied.graph, py_vertex_condition, param->evalmem, param->implied.collector_mode )) == NULL ) {
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

    // Special handling of unspecified sorting 
    // IMPORTANT: This needs to be set before we create the ranking condition below!
    if( param->sortspec == VGX_SORTBY_NONE ) {
      if( py_recursion ) {
        param->sortspec = VGX_SORTBY_REAL_PREDICATOR | VGX_SORT_DIRECTION_DESCENDING;
      }
      // Special handling of sorting if counts are requested
      else if( (param->result_format & VGX_RESPONSE_SHOW_WITH_COUNTS) ) {
        param->sortspec = VGX_SORTBY_MEMADDRESS; // we do this to force deep counts
      }
    }

    // ----
    // rank
    // ----
    if( (param->ranking_condition = iPyVGXParser.NewRankingConditionEx( param->implied.graph, py_rankspec, py_aggregate, param->sortspec, param->modifier, py_rank_vector_object, param->vertex_condition )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
    }

    // ---------
    // recursive
    // ---------
    if( py_recursion ) {
      if( _pyvgx_Neighborhood__parse_recursion( pygraph, py_recursion, param ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x007 );
      }
      if( __is_recursion_enabled( &param->recursion ) ) {
        if( py_collect ) {
          PyErr_SetString( PyExc_ValueError, "Collect mode implied with recursion (don't specify collect)" );
          THROW_SILENT( CXLIB_ERR_API, 0x008 );
        }
        static PyObject *py_C_SCAN = NULL;
        if( py_C_SCAN == NULL ) {
          py_C_SCAN = PyTuple_New( 2 );
          if( py_C_SCAN == NULL ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x009 );
          }
          Py_INCREF( Py_False );
          if( PyTuple_SetItem( py_C_SCAN, 0, Py_False ) < 0 ||
              PyTuple_SetItem( py_C_SCAN, 1, PyVGX_PyUnicode_FromStringNoErr( "*" ) ) < 0 )
          {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x00A );
          }
        }
        if( (param->collect_arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_C_SCAN, VGX_ARCDIR_ANY )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x00B );
        }
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
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x00C );
        }
      }
      else {
        if( (param->collect_arc_condition_set = iPyVGXParser.NewArcConditionSet( param->implied.graph, py_collect, VGX_ARCDIR_ANY )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x00D );
        }
      }
    }

  }
  XCATCH( errcode ) {

    _pyvgx_Neighborhood__clear_params( (__base_query_args*)param );

    // TODO: Check that we really delete any allocated objects inside param
    //       It looks like many things can go wrong above and the objects are not freed
    //       here before we set param to NULL.
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

/**************************************************************************//**
 * _pyvgx_initials_terminals__parse_params
 *
 ******************************************************************************
 */
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
    
    // ------
    // memory
    // ------
    if( py_evalmem || py_vertex_condition ) {
      if( (param->evalmem = iPyVGXParser.NewExpressEvalMemory( param->implied.graph, py_evalmem )) == NULL ) {
        return NULL;
      }
    }

    // --------
    // neighbor
    // --------
    if( py_vertex_condition ) {
      if( (param->vertex_condition = iPyVGXParser.NewVertexCondition( param->implied.graph, py_vertex_condition, param->evalmem, param->implied.collector_mode )) == NULL ) {
        return NULL;
      }
    }

    // ----
    // rank
    // ----
    if( (param->ranking_condition = iPyVGXParser.NewRankingConditionEx( param->implied.graph, py_rankspec, NULL, param->sortspec, param->modifier, NULL, param->vertex_condition )) == NULL ) {
      return NULL;
    }

    if( param->sortspec & VGX_SORTBY_PREDICATOR || param->sortspec & VGX_SORTBY_REAL_PREDICATOR ) {
      PyErr_SetString( PyVGX_QueryError, "Sort by arc value not supported for this method" );
      return NULL;
    }

    param->result_format |= param->result_attrs;

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

/**************************************************************************//**
 * _pyvgx_inarcs_outarcs__parse_params
 *
 ******************************************************************************
 */
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
      XDO {

        // Require positive hits for recursive search
        if( __is_recursion_enabled( &query->recursion_config ) && query->hits <= 0 ) {
          PyVGXError_SetString( PyExc_ValueError, "recursive search requires non-negative hits parameter" );
          XBREAK;
        }

        // Execute query
        if( CALLABLE( param->implied.graph )->simple->Neighborhood( param->implied.graph, query ) < 0 ) {
          PyVGX_CAPTURE_QUERY_ERROR( query, param );
        }
        // Steal the result from the query
        else {
          search_result = CALLABLE( query )->YankSearchResult( query );
        }
      }
      XFINALLY {
        // Delete query
        iGraphQuery.DeleteNeighborhoodQuery( &query );
      }
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
    if( (query = iGraphQuery.NewNeighborhoodQuery( param->implied.graph, param->anchor.id, &param->collect_arc_condition_set, param->implied.collector_mode, &param->recursion, &param->implied.CSTR__error )) == NULL ) {
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

    // Assign recursion filter
    if( param->recursion.visit.CSTR__filter ) {
      if( CALLABLE( query )->AddRecursionFilter( query, CStringValue(param->recursion.visit.CSTR__filter) ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC88 );
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
      if( CALLABLE( query )->SelectStatement( query, param->implied.graph, param->select_statement, param->evalmem, &param->implied.CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0xC89 );
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
