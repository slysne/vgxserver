/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_op.c
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
 *
 ******************************************************************************
 */
static int __map_opcounters( PyObject *py_map, const vgx_OperationCounters_t *counters ) {
  int64_t tx = counters->n_transactions;
  int64_t OP = counters->n_operations;
  int64_t opcodes = counters->n_opcodes;
  int64_t bytes = counters->n_bytes;
  double opcodes_per_OP = OP > 0 ? (double)opcodes / OP : 0.0;
  double OP_per_tx = tx > 0 ? (double)OP / tx : 0.0;
  double bytes_per_tx = tx > 0 ? (double)bytes / tx : 0.0;

  iPyVGXBuilder.DictMapStringToLongLong( py_map, "transactions", tx );
  iPyVGXBuilder.DictMapStringToLongLong( py_map, "operations", OP );
  iPyVGXBuilder.DictMapStringToLongLong( py_map, "opcodes", opcodes );
  iPyVGXBuilder.DictMapStringToLongLong( py_map, "bytes", bytes );
  iPyVGXBuilder.DictMapStringToFloat( py_map, "opcodes_per_operation", opcodes_per_OP );
  iPyVGXBuilder.DictMapStringToFloat( py_map, "operations_per_transaction", OP_per_tx );
  iPyVGXBuilder.DictMapStringToFloat( py_map, "bytes_per_transaction", bytes_per_tx );

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __map_opbacklog( PyObject *py_map, const vgx_OperationBacklog_t *b ) {

  iPyVGXBuilder.DictMapStringToLongLong( py_map, "tx", b->n_tx );
  iPyVGXBuilder.DictMapStringToLongLong( py_map, "bytes", b->n_bytes );
  iPyVGXBuilder.DictMapStringToFloat( py_map, "latency", b->latency_ms / 1000.0 );

  return 0;
}





/******************************************************************************
 * Opcodes
 *
 ******************************************************************************
 */
static vgx_KeyVal_char_int64_t opcodes[] = {
    { .key = "OP_DENY_consumer",     .value = ((int64_t)OP_PROFILE_ID__CONSUMER_DENY) << 32 },

    { .key = "OP_PROFILE_consumer",     .value = ((int64_t)OP_PROFILE_ID__CONSUMER_ALLOW) << 32 },

    { .key = "OP_nop",            .value = OPCODE_NONE },
    { .key = "OP_vxn",            .value = OPCODE_VERTEX_NEW },
    { .key = "OP_vxd",            .value = OPCODE_VERTEX_DELETE },
    { .key = "OP_vxr",            .value = OPCODE_VERTEX_SET_RANK },
    { .key = "OP_vxt",            .value = OPCODE_VERTEX_SET_TYPE },
    { .key = "OP_vxx",            .value = OPCODE_VERTEX_SET_TMX },
    { .key = "OP_vxc",            .value = OPCODE_VERTEX_CONVERT },

    { .key = "OP_vps",            .value = OPCODE_VERTEX_SET_PROPERTY },
    { .key = "OP_vpd",            .value = OPCODE_VERTEX_DELETE_PROPERTY },
    { .key = "OP_vpc",            .value = OPCODE_VERTEX_CLEAR_PROPERTIES },

    { .key = "OP_vvs",            .value = OPCODE_VERTEX_SET_VECTOR },
    { .key = "OP_vvd",            .value = OPCODE_VERTEX_DELETE_VECTOR },

    { .key = "OP_vod",            .value = OPCODE_VERTEX_DELETE_OUTARCS },
    { .key = "OP_vid",            .value = OPCODE_VERTEX_DELETE_INARCS },
  
    { .key = "OP_val",            .value = OPCODE_VERTEX_ACQUIRE },
    { .key = "OP_vrl",            .value = OPCODE_VERTEX_RELEASE },

    { .key = "OP_arc",            .value = OPCODE_ARC_CONNECT },
    { .key = "OP_ard",            .value = OPCODE_ARC_DISCONNECT },

    { .key = "OP_sya",            .value = OPCODE_SYSTEM_ATTACH },
    { .key = "OP_syd",            .value = OPCODE_SYSTEM_DETACH },
    { .key = "OP_rcl",            .value = OPCODE_SYSTEM_CLEAR_REGISTRY },
    { .key = "OP_scf",            .value = OPCODE_SYSTEM_SIMILARITY },
    { .key = "OP_grn",            .value = OPCODE_SYSTEM_CREATE_GRAPH },
    { .key = "OP_grd",            .value = OPCODE_SYSTEM_DELETE_GRAPH },
    { .key = "OP_com",            .value = OPCODE_SYSTEM_SEND_COMMENT },
    { .key = "OP_dat",            .value = OPCODE_SYSTEM_SEND_RAW_DATA },
    { .key = "OP_clg",            .value = OPCODE_SYSTEM_CLONE_GRAPH },

    { .key = "OP_grt",            .value = OPCODE_GRAPH_TRUNCATE },
    { .key = "OP_grp",            .value = OPCODE_GRAPH_PERSIST },
    { .key = "OP_grs",            .value = OPCODE_GRAPH_STATE },

    { .key = "OP_grr",            .value = OPCODE_GRAPH_READONLY },
    { .key = "OP_grw",            .value = OPCODE_GRAPH_READWRITE },
    { .key = "OP_gre",            .value = OPCODE_GRAPH_EVENTS },
    { .key = "OP_gri",            .value = OPCODE_GRAPH_NOEVENTS },
    { .key = "OP_tic",            .value = OPCODE_GRAPH_TICK },
    { .key = "OP_evx",            .value = OPCODE_GRAPH_EVENT_EXEC },

    { .key = "OP_lxw",            .value = OPCODE_VERTICES_ACQUIRE_WL },
    { .key = "OP_ulv",            .value = OPCODE_VERTICES_RELEASE },
    { .key = "OP_ula",            .value = OPCODE_VERTICES_RELEASE_ALL },

    { .key = "OP_vea",            .value = OPCODE_ENUM_ADD_VXTYPE },
    { .key = "OP_ved",            .value = OPCODE_ENUM_DELETE_VXTYPE },
    { .key = "OP_rea",            .value = OPCODE_ENUM_ADD_REL },
    { .key = "OP_red",            .value = OPCODE_ENUM_DELETE_REL },
    { .key = "OP_dea",            .value = OPCODE_ENUM_ADD_DIM },
    { .key = "OP_ded",            .value = OPCODE_ENUM_DELETE_DIM },
    { .key = "OP_kea",            .value = OPCODE_ENUM_ADD_KEY },
    { .key = "OP_ked",            .value = OPCODE_ENUM_DELETE_KEY },
    { .key = "OP_sea",            .value = OPCODE_ENUM_ADD_STRING },
    { .key = "OP_sed",            .value = OPCODE_ENUM_DELETE_STRING },

    { .key = "OP_ACTION_assign",  .value = __OPCODE_PROBE__ACTION | __OPCODE_ACTION__ASSIGN },
    { .key = "OP_ACTION_create",  .value = __OPCODE_PROBE__ACTION | __OPCODE_ACTION__CREATE },
    { .key = "OP_ACTION_delete",  .value = __OPCODE_PROBE__ACTION | __OPCODE_ACTION__DELETE },
    { .key = "OP_ACTION_event",   .value = __OPCODE_PROBE__ACTION | __OPCODE_ACTION__EVENT },

    { .key = "OP_SCOPE_single",   .value = __OPCODE_PROBE__SCOPE  | __OPCODE_SCOPE__SINGLE },
    { .key = "OP_SCOPE_multiple", .value = __OPCODE_PROBE__SCOPE  | __OPCODE_SCOPE__MULTIPLE },

    { .key = "OP_TARGET_vertex",  .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__VERTEX },
    { .key = "OP_TARGET_arc",     .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__ARC },
    { .key = "OP_TARGET_system",  .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__SYSTEM },
    { .key = "OP_TARGET_graph",   .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__GRAPH },
    { .key = "OP_TARGET_rw",      .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__RW },
    { .key = "OP_TARGET_evp",     .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__EVP },
    { .key = "OP_TARGET_time",    .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__TIME },
    { .key = "OP_TARGET_exec",    .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__EXEC },
    { .key = "OP_TARGET_acquire", .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__ACQUIRE },
    { .key = "OP_TARGET_enum",    .value = __OPCODE_PROBE__TARGET | __OPCODE_TARGET__ENUM },

    { .key = "OP_ANY",            .value = __OPCODE_PROBE__ANY },

    { .key = NULL,                .value = 0 }
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * PyVGX_Operation_GetNewConstantsDict( void ) {
  PyObject *py_opcodes = PyDict_New();
  if( py_opcodes ) {
    // Map opcode enums
    if( iPyVGXBuilder.MapIntegerConstants( py_opcodes, opcodes ) < 0 ) {
      PyVGX_DECREF( py_opcodes );
      py_opcodes = NULL;
    }
  }
  return py_opcodes;
}



/******************************************************************************
 * PyVGX_Operation__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Operation__dealloc( PyVGX_Operation *py_op ) {
  Py_TYPE( py_op )->tp_free( py_op );
}



/******************************************************************************
 * PyVGX_Operation__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_Operation *py_op = NULL;
  py_op = (PyVGX_Operation*)type->tp_alloc( type, 0 );
  return (PyObject *)py_op;
}



/******************************************************************************
 * PyVGX_Operation__init
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int PyVGX_Operation__init( PyVGX_Operation *py_op, PyObject *args, PyObject *kwds ) {
  return 0;
}



/******************************************************************************
 * PyVGX_Operation__repr
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__repr( PyVGX_Operation *py_op ) {
  return PyUnicode_FromString( "pyvgx.Operation" );
}



/******************************************************************************
 * PyVGX_Operation__attach
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__attach( PyObject *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "uris", "handshake", "timeout", NULL }; 

  PyObject *py_URIs = NULL;
  int handshake = 1;
  int timeout_ms = 30000;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|Oii", kwlist, &py_URIs, &handshake, &timeout_ms ) ) {
    return NULL;
  }

  bool do_handshake = handshake > 0;

  PyObject *py_ret = NULL;

  vgx_StringList_t *URIs = NULL;
  int64_t sz;
  const char *uri;
  CString_t *CSTR__error = NULL;

  XTRY {

    // Attach to default URIs
    if( py_URIs == NULL ) {
      if( (URIs = iString.List.Clone( _default_URIs )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }
    }
    // Attach to specific URIs
    else {
      // Single URI as string
      if( PyUnicode_Check( py_URIs ) ) {
        if( (URIs = iString.List.New( NULL, 1 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
        if( iString.List.SetItem( URIs, 0, PyUnicode_AsUTF8( py_URIs ) ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
        }
      }
      // Multiple URIs as list of strings
      else if( PyList_Check( py_URIs ) ) {
        sz = PyList_Size( py_URIs );
        if( (URIs = iString.List.New( NULL, sz )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
        }
        for( int64_t i=0; i<sz; i++ ) {
          if( (uri = PyUnicode_AsUTF8( PyList_GetItem( py_URIs, i ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_API, 0x005 );
          }
          if( iString.List.SetItem( URIs, i, uri ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
          }
        }
      }
      // Error
      else {
        PyErr_SetString( PyExc_TypeError, "URI must be string or list" );
      }
    }


    int attached = 0;

    if( !igraphfactory.IsInitialized() ) {
      PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x007 );
    }

    BEGIN_PYVGX_THREADS {
      if( URIs ) {
        attached = iSystem.AttachOutput( URIs, TX_ATTACH_MODE_NORMAL, do_handshake, timeout_ms, &CSTR__error );
      }
    } END_PYVGX_THREADS;

    if( attached == 1 ) {
      py_ret = Py_None;
      Py_INCREF( Py_None );
    }
    else {
      PyErr_Format( PyExc_Exception, "Failed to attach %S: %s", py_URIs, CStringValueDefault( CSTR__error, "?" ) );
    }

  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    if( igraphfactory.IsInitialized() ) {
      BEGIN_PYVGX_THREADS {
        iSystem.DetachOutput( URIs, true, true, timeout_ms, NULL );
      } END_PYVGX_THREADS;
    }
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
    iString.List.Discard( &URIs );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__attached
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__attached( PyVGX_Operation *py_op ) {

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_StringList_t *subscribers = NULL;

  BEGIN_PYVGX_THREADS {
    subscribers = iSystem.AttachedOutputs( NULL, 5000 );
  } END_PYVGX_THREADS;

  if( subscribers == NULL ) {
    PyVGXError_SetString( PyVGX_InternalError, "Unknown internal error" );
    return NULL;
  }

  int64_t sz = iString.List.Size( subscribers );

  PyObject *py_ret = PyList_New( sz );
  if( py_ret ) {
    for( int64_t i=0; i<sz; i++ ) {
      const CString_t *CSTR__subscriber = iString.List.GetItem( subscribers, i );
      if( CSTR__subscriber ) {
        PyObject *py_sub = PyUnicode_FromString( CStringValue( CSTR__subscriber ) );
        if( py_sub ) {
          PyList_SetItem( py_ret, i, py_sub );
        }
        else {
          PyList_SetItem( py_ret, i, Py_None );
          Py_INCREF( Py_None );
        }
      }
    }
  }

  iString.List.Discard( &subscribers );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__detach
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__detach( PyObject *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "uri", "force", "timeout", NULL }; 

  const char *uri = NULL;
  int force = false;
  int timeout_ms = 30000;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|zii", kwlist, &uri, &force, &timeout_ms ) ) {
    return NULL;
  }

  if( force > 0 ) {
    force = true;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_StringList_t *singleton_uri = NULL;
  if( uri ) {
    if( (singleton_uri = iString.List.New( NULL, 1 )) == NULL ) {
      PyErr_SetString( PyExc_MemoryError, "out of memory" );
      return NULL;
    }
    if( iString.List.SetItem( singleton_uri, 0, uri ) == NULL ) {
      iString.List.Discard( &singleton_uri );
      PyErr_SetString( PyExc_MemoryError, "out of memory" );
      return NULL;
    }
  }

  PyObject *py_ret = NULL;
  CString_t *CSTR__error = NULL;
  int ret = 0;
  BEGIN_PYVGX_THREADS {
    // Detach single (if given) all if no uri specified
    ret = iSystem.DetachOutput( singleton_uri, force, force, timeout_ms, &CSTR__error );
  } END_PYVGX_THREADS;

  iString.List.Discard( &singleton_uri );

  if( ret < 0 ) {
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown error";
    PyErr_Format( PyExc_Exception, "Detach failed: %s", serr );
  }
  else {
    py_ret = PyLong_FromLong( ret );
  }

  iString.Discard( &CSTR__error );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__suspend
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__suspend( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "timeout", NULL }; 
  int timeout_ms = 60000;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  int suspended = 0;
  int64_t n_writable = 0;

  BEGIN_PYVGX_THREADS {
    if( (n_writable = iOperation.WritableVertices()) == 0 ) {
      suspended = iOperation.Suspend( timeout_ms );
    }
  } END_PYVGX_THREADS;

  // OK
  if( suspended > 0  ) {
    Py_RETURN_NONE;
  }
  // Timeout (or error)
  else {
    //
    // TODO:   When timeout exception, make sure everything is actually running.
    //
    if( suspended < 0 ) {
      PyErr_SetString( PyVGX_OperationTimeout, "Output agent was not suspended" );
    }
    // Writable vertices
    else if( n_writable > 0 ) {
      PyErr_Format( PyVGX_AccessError, "Cannot suspend operation emitters with %lld open writable vertices", n_writable );
    }
    // Agent not running
    else {
      PyErr_SetString( PyVGX_InternalError, "Output agent not running" );
    }
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Operation__resume
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__resume( PyObject *self ) {
  int resumed;
  BEGIN_PYVGX_THREADS {
    resumed = iOperation.Resume();
  } END_PYVGX_THREADS;

  // OK
  if( resumed > 0  ) {
    Py_RETURN_NONE;
  }
  // Timeout (or error)
  else if( resumed < 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "Output agent was not resumed" );
    return NULL;
  }
  // Agent not running
  else {
    PyErr_SetString( PyVGX_InternalError, "Output agent not running" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Operation__fence
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__fence( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "timeout", NULL }; 
  int timeout_ms = 60000;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  int fence = 0;
  int64_t n_writable = 0;

  BEGIN_PYVGX_THREADS {
    if( (n_writable = iOperation.WritableVertices()) == 0 ) {
      fence = iOperation.Fence( timeout_ms );
    }
  } END_PYVGX_THREADS;

  // SUCCESS
  if( fence > 0 ) {
    Py_RETURN_NONE;
  }
  // ERROR
  else {
    // Error
    if( fence < 0 ) {
      PyErr_SetString( PyVGX_OperationTimeout, "Error (timeout?)" );
    }
    // Writable vertices
    else if( n_writable > 0 ) {
      PyErr_Format( PyVGX_AccessError, "Cannot perform operation fence with %lld open writable vertices", n_writable );
    }
    // No output agent
    else {
      PyErr_SetString( PyVGX_InternalError, "No output agent" );
    }
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Operation__assert
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__assert( PyObject *self ) {

  int asserted = 0;
  int64_t n_writable = 0;
  BEGIN_PYVGX_THREADS {
    if( (n_writable = iOperation.WritableVertices()) == 0 ) {
      asserted = iOperation.AssertState();
    }
  } END_PYVGX_THREADS;

  // SUCCESS
  if( asserted > 0 ) {
    Py_RETURN_NONE;
  }
  // ERROR
  else {
    // Error
    if( asserted < 0 ) {
      PyErr_SetString( PyVGX_OperationTimeout, "Error (timeout?)" );
    }
    // Writable vertices
    else if( n_writable > 0 ) {
      PyErr_Format( PyVGX_AccessError, "Cannot assert state with %lld open writable vertices", n_writable );
    }
    // No output agent
    else {
      PyErr_SetString( PyVGX_InternalError, "No output agent" );
    }
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Operation__counters
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__counters( PyObject *self ) {
  vgx_OperationCounters_t out_counters = {0};
  vgx_OperationCounters_t in_counters = {0};
  vgx_OperationBacklog_t output_backlog = {0};

  vgx_Graph_t *sys = iSystem.GetSystemGraph();
  if( sys ) {
    BEGIN_PYVGX_THREADS {
      out_counters = iOperation.Counters.Outstream( sys );
      in_counters = iOperation.Counters.Instream( sys );
      output_backlog = iOperation.Counters.OutputBacklog( sys );
    } END_PYVGX_THREADS;
  }
  else {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  PyObject *py_counters = NULL;
  PyObject *py_out = NULL;
  PyObject *py_out_backlog = NULL;
  PyObject *py_in = NULL;

  XTRY {
    py_counters = PyDict_New();
    py_out = PyDict_New();
    py_out_backlog = PyDict_New();
    py_in = PyDict_New();
    if( !py_counters || !py_out || !py_out_backlog || !py_in ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // out
    __map_opcounters( py_out, &out_counters );
    __map_opbacklog( py_out_backlog, &output_backlog );
    if( iPyVGXBuilder.DictMapStringToPyObject( py_out, "backlog", &py_out_backlog ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }
    if( iPyVGXBuilder.DictMapStringToPyObject( py_counters, "out", &py_out ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // in
    __map_opcounters( py_in, &in_counters );
    if( iPyVGXBuilder.DictMapStringToPyObject( py_counters, "in", &py_in ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }


  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_out );
    PyVGX_XDECREF( py_out_backlog );
    PyVGX_XDECREF( py_in );
    PyVGX_XDECREF( py_counters );
    py_counters = NULL;
  }
  XFINALLY {
  }
  
  return py_counters;
}



/******************************************************************************
 * PyVGX_Operation__heartbeat
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__heartbeat( PyObject *self, PyObject *py_flag ) {

  if( !PyBool_Check( py_flag ) ) {
    PyErr_SetString( PyExc_TypeError, "true or false required" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( SYSTEM ) {
        if( py_flag == Py_True ) {
          iOperation.Emitter_CS.HeartbeatEnable( SYSTEM );
        }
        else {
          iOperation.Emitter_CS.HeartbeatDisable( SYSTEM );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;
  }
  else {
    PyErr_SetString( PyExc_TypeError, "System not initialized?" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__strict_serial
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__strict_serial( PyObject *self, PyObject *py_flag ) {

  if( !PyBool_Check( py_flag ) ) {
    PyErr_SetString( PyExc_TypeError, "true or false required" );
    return NULL;
  }

  bool enable = py_flag == Py_True;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    vgx_OperationParser_t *sysparser = &SYSTEM->OP.parser;
    BEGIN_PYVGX_THREADS {
      iOperation.Parser.EnableStrictSerial( sysparser, enable );
    } END_PYVGX_THREADS;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__verify_crc
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__verify_crc( PyObject *self, PyObject *py_flag ) {

  if( !PyBool_Check( py_flag ) ) {
    PyErr_SetString( PyExc_TypeError, "true or false required" );
    return NULL;
  }

  bool enable = py_flag == Py_True;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    vgx_OperationParser_t *sysparser = &SYSTEM->OP.parser;
    BEGIN_PYVGX_THREADS {
      iOperation.Parser.EnableCRC( sysparser, enable );
    } END_PYVGX_THREADS;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__deny
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__deny( PyObject *self, PyObject *py_opcode ) {

  if( !PyLong_Check( py_opcode ) ) {
    PyErr_SetString( PyExc_TypeError, "a long is required" );
    return NULL;
  }

  int64_t opcode_filter = __get_opcode_filter( PyLong_AsLongLong( py_opcode ) );

  int n = 0;
  BEGIN_PYVGX_THREADS {
    n = iOperation.Parser.AddFilter( opcode_filter );
  } END_PYVGX_THREADS;

  if( n < 0 ) {
    PyErr_SetString( PyExc_ValueError, "invalid opcode filter" );
    return NULL;
  }

  return PyLong_FromLong( n );
}



/******************************************************************************
 * PyVGX_Operation__allow
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__allow( PyObject *self, PyObject *py_opcode ) {

  if( !PyLong_Check( py_opcode ) ) {
    PyErr_SetString( PyExc_TypeError, "a long is required" );
    return NULL;
  }

  int64_t opcode_filter = __get_opcode_filter( PyLong_AsLongLong( py_opcode ) );

  int n = 0;
  BEGIN_PYVGX_THREADS {
    n = iOperation.Parser.RemoveFilter( opcode_filter );
  } END_PYVGX_THREADS;

  if( n < 0 ) {
    PyErr_SetString( PyExc_ValueError, "invalid opcode filter" );
    return NULL;
  }

  return PyLong_FromLong( n );
}



/******************************************************************************
 * PyVGX_Operation__profile
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__profile( PyObject *self, PyObject *py_profile ) {

  if( !PyLong_Check( py_profile ) ) {
    PyErr_SetString( PyExc_TypeError, "a long is required" );
    return NULL;
  }

  int64_t profile = __get_opcode_filter( PyLong_AsLongLong( py_profile ) );

  int n = 0;
  BEGIN_PYVGX_THREADS {
    n = iOperation.Parser.ApplyProfile( profile );
  } END_PYVGX_THREADS;

  if( n < 0 ) {
    PyErr_SetString( PyExc_ValueError, "invalid profile" );
    return NULL;
  }

  return PyLong_FromLong( n );
}



/******************************************************************************
 * PyVGX_Operation__reset
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__reset( PyObject *self ) {
  int ret = -1;
  
  // Reset system parser
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    vgx_OperationParser_t *sysparser = &SYSTEM->OP.parser;
    BEGIN_PYVGX_THREADS {
      ret = iOperation.Parser.Reset( sysparser );
    } END_PYVGX_THREADS;
  }
  
  // Error
  if( ret < 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "failed to reset parser" );
    return NULL;
  }

  // OK
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__data_crc32c
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__data_crc32c( PyObject *self, PyObject *py_str ) {
  if( !PyVGX_PyObject_CheckString( py_str ) ) {
    PyVGXError_SetString( PyExc_ValueError, "a string or bytes-like object is required" );
    return NULL;
  }

  const char *str = PyVGX_PyObject_AsString( py_str );
  if( !str ) {
    return NULL;
  }
  unsigned int crc = iOperation.Parser.Checksum( str );
  return PyLong_FromUnsignedLong( crc );
}



/******************************************************************************
 * PyVGX_Operation__bind
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__bind( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "port", "durable", "snapshot_threshold", NULL }; 
  int port = 0;
  int txlog = 0;
  int64_t snapshot_threshold = -1;
  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "i|iL", kwlist, &port, &txlog, &snapshot_threshold ) ) {
    return NULL;
  }

  if( port < 1 ) {
    PyVGXError_SetString( PyExc_ValueError, "Bind port must be greater than 0" );
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  uint16_t uport = (uint16_t)port;
  if( uport != port ) {
    PyErr_SetString( PyExc_ValueError, "invalid port number" );
    return NULL;
  }

  bool durable = txlog > 0;
  if( durable && uport == 0 ) {
    PyErr_SetString( PyExc_Exception, "transaction log requires bind" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  if( iOperation.System_OPEN.ConsumerService.Start( SYSTEM, uport, durable, snapshot_threshold ) < 0 ) {
    PyErr_SetString( PyExc_Exception, "failed to start consumer service" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__bound
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__bound( PyObject *self ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  unsigned txport = iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM );
  int durable = iOperation.System_OPEN.ConsumerService.IsDurable( SYSTEM );

  PyObject *py_tuple = PyTuple_New( 2 );
  if( py_tuple ) {
    PyTuple_SetItem( py_tuple, 0, PyLong_FromUnsignedLong( txport ) );
    PyObject *py_durable = durable > 0 ? Py_True : Py_False;
    Py_INCREF( py_durable );
    PyTuple_SetItem( py_tuple, 1, py_durable );
  }

  return py_tuple;
}



/******************************************************************************
 * PyVGX_Operation__unbind
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__unbind( PyObject *self ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  if( iOperation.System_OPEN.ConsumerService.Stop( SYSTEM ) < 0 ) {
    PyErr_SetString( PyExc_Exception, "failed to stop consumer service" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__subscribe
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__subscribe( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "address", "hardsync", "timeout", NULL }; 
  PyObject *py_address = NULL;
  int hardsync = false;
  int timeout_ms = 5000;
  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "O|ii", kwlist, &py_address, &hardsync, &timeout_ms ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = NULL;
  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  const char *provider_host = NULL;
  int provider_port = 0;

  if( PyTuple_Check( py_address ) && PyTuple_Size( py_address ) == 2 ) {
    PyObject *py_host = PyTuple_GetItem( py_address, 0 );
    PyObject *py_port = PyTuple_GetItem( py_address, 1 );
    if( (provider_host = PyUnicode_AsUTF8( py_host )) != NULL ) {
      provider_port = PyLong_AsLong( py_port );
    }
  }

  if( provider_host == NULL || provider_port <= 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_ValueError, "Provider address must be tuple (host, port)" );
    }
    return NULL;
  }

  // Validate provider port
  uint16_t provider_uport = (uint16_t)provider_port;
  if( provider_uport != provider_port ) {
    PyErr_SetString( PyExc_ValueError, "Invalid port number" );
    return NULL;
  }

  // Validate provider host
  const char *p = provider_host;
  char c;
  while( (c = *p++) != '\0' ) {
    if( !isalpha(c) && !isdigit(c) && c != '-' && c != '.' && p-provider_host > 253 ) {
      PyErr_SetString( PyExc_ValueError, "Invalid host name" );
      return NULL;
    }
  }

  int ret = 0;
  CString_t *CSTR__error = NULL;
  BEGIN_PYVGX_THREADS {
    ret = iOperation.System_OPEN.ConsumerService.Subscribe( SYSTEM, provider_host, provider_uport, hardsync, timeout_ms, &CSTR__error );
  } END_PYVGX_THREADS;

  PyObject *py_ret = NULL;
  if( ret < 0 ) {
    if( CSTR__error ) {
      PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
    }
    else if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
  }
  else {
    Py_INCREF( Py_None );
    py_ret = Py_None;
  }

  iString.Discard( &CSTR__error );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__unsubscribe
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__unsubscribe( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { NULL }; 
  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "", kwlist ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = NULL;
  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  int ret = 0;
  BEGIN_PYVGX_THREADS {
    ret = iOperation.System_OPEN.ConsumerService.Unsubscribe( SYSTEM );
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyExc_Exception, "Operation did not complete" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Operation__consume
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__consume( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "data", "timeout", NULL }; 
  PyObject *py_data;
  int timeout_ms = 0;
  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "O|i", kwlist, &py_data, &timeout_ms ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  if( igraphfactory.EventsEnabled() ) {
    // TODO: Proper warning
  }

  CString_t *CSTR__error = NULL;

  vgx_ByteArrayList_t list = { 0 };
  vgx_ByteArray_t single = { 0 };

  // String
  if( PyVGX_PyObject_CheckString( py_data ) ) {
    Py_ssize_t sz;
    Py_ssize_t ucsz;
    if( (single.data = (BYTE*)PyVGX_PyObject_AsStringAndSize( py_data, &sz, &ucsz )) == NULL ) {
      return NULL;
    }
    single.len = sz;
    list.entries = &single;
    list.sz = 1;
  }
  // List
  else if( PyList_Check( py_data ) ) {
    list.sz = PyList_Size( py_data );

    // Allocate list
    if( (list.entries = calloc( list.sz, sizeof( vgx_ByteArray_t ) )) == NULL ) {
      return NULL;
    }

    // Populate list
    vgx_ByteArray_t *dest = list.entries;
    vgx_ByteArray_t *end = dest + list.sz;
    PyObject *py_str;
    int64_t i = 0;
    while( dest < end ) {
      if( PyVGX_PyObject_CheckString( (py_str = PyList_GET_ITEM( py_data, i )) ) ) {
        Py_ssize_t sz;
        Py_ssize_t ucsz;
        if( (dest->data = (BYTE*)PyVGX_PyObject_AsStringAndSize( py_str, &sz, &ucsz )) == NULL ) {
          free( list.entries );
          return NULL;
        }
        dest->len = sz;
      }
      else {
        PyErr_SetString( PyExc_TypeError, "list element must be string" );
        free( list.entries );
        return NULL;
      }
      ++dest;
      ++i;
    }
  }

  PyObject *py_ret = NULL;

  XTRY {
    int64_t n_data = 0;
    int64_t n_pending = 0;

    if( list.sz > 0 ) {
      BEGIN_PYVGX_THREADS {
        vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
        if( SYSTEM ) {
          vgx_OperationParser_t *sysparser = &SYSTEM->OP.parser;
          iOperation.Parser.EnableExecution( sysparser, true );
          
          // Keep trying as long as temporary failure to submit
          while( (n_data = iOperation.Parser.SubmitData( sysparser, &list, &CSTR__error )) == 0 ) {
            const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
            PYVGX_API_WARNING( "op", 0x001, "Temporary overload: %s", msg ); 
            iString.Discard( &CSTR__error );
          }

          if( n_data > 0 && timeout_ms != 0 ) {
            bool pending = true;
            BEGIN_TIME_LIMITED_WHILE( pending, timeout_ms, NULL ) {
              if( (n_pending = iOperation.Parser.Pending( sysparser )) < 1 ) {
                pending = false;
              }
            } END_TIME_LIMITED_WHILE;
          }

        }
      } END_PYVGX_THREADS;

      //
      // TODO: BETTER EXCEPTIONS FOR ACTIONABLE vs NONACTIONABLE ERRORS
      //

      if( n_data < 0 || n_pending < 0 ) {
        if( CSTR__error ) {
          PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
          CStringDelete( CSTR__error );
        }
        else {
          PyErr_SetString( PyExc_Exception, "unknown error" );
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }
      else if( n_pending > 0 ) {
        PyErr_Format( PyVGX_OperationTimeout, "%lld bytes pending consumption, %lld bytes submitted", n_pending, n_data );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }
    }

    py_ret = PyLong_FromLongLong( n_data );

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {
    if( list.entries && list.entries != &single ) {
      free( list.entries );
    }
  }

  return py_ret;

}



/******************************************************************************
 * PyVGX_Operation__pending
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__pending( PyObject *self ) {
  int64_t n_pending = -1;
  
  // System parser
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    vgx_OperationParser_t *sysparser = &SYSTEM->OP.parser;
    BEGIN_PYVGX_THREADS {
      n_pending = iOperation.Parser.Pending( sysparser );
    } END_PYVGX_THREADS;
  }

  if( n_pending < 0 ) {
    PyErr_SetString( PyVGX_InternalError, "internal error" );
    return NULL;
  }
  else {
    return PyLong_FromLongLong( n_pending );
  }
}



/******************************************************************************
 * PyVGX_Operation__throttle
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__throttle( PyObject *self, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = { "rate", "unit", NULL }; 

  PyObject *py_rate = NULL;
  const char *unit = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|Os", kwlist, &py_rate, &unit ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = NULL;
  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_OperationFeedRates_t *limits_CS = SYSTEM->OP.system.in_feed_limits_CS;
  if( limits_CS == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "Internal error" );
    return NULL;
  }

  vgx_OperationFeedRates_t min_limits = {
    .tpms = 8     / 1000.0,
    .opms = 128   / 1000.0,
    .cpms = 256   / 1000.0,
    .bpms = 65536 / 1000.0,
    .refresh = 0
  };

  vgx_OperationFeedRates_t limits = {0};

  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      limits = *limits_CS;
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  CString_t *CSTR__error = NULL;

  XTRY {
    double rate_sec;

    if( py_rate == NULL ) {
      rate_sec = -1.0;
    }
    else if( py_rate == Py_None ) {
      rate_sec = 0.0;
    }
    else {
      if( (rate_sec = PyFloat_AsDouble( py_rate )) < 0.0 ) {
        CSTR__error = CStringNew( "rate cannot be negative" );
        THROW_SILENT( CXLIB_ERR_API, 0x001 );
      }
    }

    // Convert rate to milliseconds
    double rate_ms = rate_sec / 1000.0;

    if( rate_ms >= 0.0 ) {
      // Clear all
      if( rate_ms == 0.0 && unit == NULL ) {
        unit = "all_unlimited";
      }
      // Update
      if( unit == NULL || CharsEqualsConst( unit, "bytes" ) ) {
        if( (limits.bpms = rate_ms) < min_limits.bpms && rate_ms > 0.0 ) {
          CSTR__error = CStringNewFormat( "bytes minimum rate: %.0f/s", min_limits.bpms*1000 );
          THROW_SILENT( CXLIB_ERR_API, 0x002 );
        }
      }
      else if( CharsEqualsConst( unit, "opcodes" ) ) {
        if( (limits.cpms = rate_ms) < min_limits.cpms && rate_ms > 0.0 ) {
          CSTR__error = CStringNewFormat( "opcodes minimum rate: %.0f/s", min_limits.cpms*1000 );
          THROW_SILENT( CXLIB_ERR_API, 0x003 );
        }
      }
      else if( CharsEqualsConst( unit, "operations" ) ) {
        if( (limits.opms = rate_ms) < min_limits.opms && rate_ms > 0.0 ) {
          CSTR__error = CStringNewFormat( "operations minimum rate: %.0f/s", min_limits.opms*1000 );
          THROW_SILENT( CXLIB_ERR_API, 0x004 );
        }
      }
      else if( CharsEqualsConst( unit, "transactions" ) ) {
        if( (limits.tpms = rate_ms) < min_limits.tpms && rate_ms > 0.0 ) {
          CSTR__error = CStringNewFormat( "transactions minimum rate: %.0f/s", min_limits.tpms*1000 );
          THROW_SILENT( CXLIB_ERR_API, 0x005 );
        }
      }
      else if( CharsEqualsConst( unit, "all_unlimited" ) ) {
        limits.bpms = 0.0;
        limits.cpms = 0.0;
        limits.opms = 0.0;
        limits.tpms = 0.0;
      }
      else {
        CSTR__error = CStringNew( "unit must be one of ['bytes', 'opcodes', 'operations', 'transactions']" );
        THROW_SILENT( CXLIB_ERR_API, 0x006 );
      }
    }
  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_ValueError, CSTR__error ? CStringValue( CSTR__error ) : "unknown error" );
    }
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
  }

  if( PyErr_Occurred() ) {
    return NULL;
  }


  PyObject *py_ret = PyDict_New();
  struct s_entry {
    const char *label;
    double rate_ms;
  } entries[] = {
    { .label="bytes_per_second",        .rate_ms=limits.bpms*1000.0 },
    { .label="opcodes_per_second",      .rate_ms=limits.cpms*1000.0 },
    { .label="operations_per_second",   .rate_ms=limits.opms*1000.0 },
    { .label="transactions_per_second", .rate_ms=limits.tpms*1000.0 },
    { .label=NULL,                      .rate_ms=0 },
  };

  if( py_ret ) {
    struct s_entry *entry = entries;
    while( entry->label ) {
      if( entry->rate_ms > 0.0 ) {
        iPyVGXBuilder.DictMapStringToFloat( py_ret, entry->label, entry->rate_ms );
      }
      else {
        PyObject *py_none = Py_None;
        Py_INCREF( py_none );
        iPyVGXBuilder.DictMapStringToPyObject( py_ret, entry->label, &py_none );
      }
      ++entry;
    }

    // Update
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( SYSTEM ) {
        *limits_CS = limits;
        limits_CS->refresh = true;
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__produce_comment
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__produce_comment( PyObject *self, PyObject *py_comment ) {
  const char *comment = NULL;
  Py_ssize_t sz = 0;
  Py_ssize_t ucsz = 0;
  if( (comment = PyVGX_PyObject_AsUTF8AndSize( py_comment, &sz, &ucsz, NULL )) == NULL ) {
    return NULL;
  }
  if( sz > SHRT_MAX ) {
    PyErr_SetString( PyExc_ValueError, "string too large" );
    return NULL;
  }

  int ret = 0;
  BEGIN_PYVGX_THREADS {
    vgx_Graph_t *sys = iSystem.GetSystemGraph();
    CString_t *CSTR__comment = sys ? NewEphemeralCStringLen( sys, comment, (int)sz, (int)ucsz ) : NULL;
    if( CSTR__comment ) {
      GRAPH_LOCK( sys ) {
        if( (ret = iOperation.System_SYS_CS.SendComment( sys, CSTR__comment )) == 1 ) {
          iOperation.Graph_CS.SetModified( sys );
          COMMIT_GRAPH_OPERATION_CS( sys );
        }
      } GRAPH_RELEASE;
      CStringDelete( CSTR__comment );
    }
    else {
      ret = -1;
    }
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyExc_ValueError, "internal error" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 * PyVGX_Operation__produce_raw
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__produce_raw( PyObject *self, PyObject *py_data ) {
  const char *data = NULL;
  Py_ssize_t sz = 0;
  Py_ssize_t ucsz = 0;
  if( (data = PyVGX_PyObject_AsStringAndSize( py_data, &sz, &ucsz )) == NULL ) {
    return NULL;
  }

  int ret = -1;
  BEGIN_PYVGX_THREADS {
    vgx_Graph_t *sys = iSystem.GetSystemGraph();
    if( sys ) {
      GRAPH_LOCK( sys ) {
        if( (ret = iOperation.System_SYS_CS.SendRawData( sys, data, sz )) == 1 ) {
          iOperation.Graph_CS.SetModified( sys );
          COMMIT_GRAPH_OPERATION_CS( sys );
        }
      } GRAPH_RELEASE;
      ret = 0;
    }
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyExc_ValueError, "internal error" );
    return NULL;
  }
  else {
    Py_RETURN_NONE;
  }
}



typedef enum __e_tx_stream_direction {
  TX_STREAM_DIRECTION__OUT = 0,
  TX_STREAM_DIRECTION__IN  = 1
} __tx_stream_direction;



typedef enum __e_tx_stream_state {
  TX_STREAM_STATE__CLOSE = 0,
  TX_STREAM_STATE__OPEN  = 1
} __tx_stream_state;



/******************************************************************************
 * PyVGX_Operation__suspend_tx_in
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __stream_tx( PyObject *self, PyObject *args, PyObject *kwdict, __tx_stream_state state, __tx_stream_direction txdir ) {
  static char *kwlist[] = { "timeout", NULL }; 
  int timeout_ms = 60000;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  if( timeout_ms < 0 ) {
    PyErr_SetString( PyExc_ValueError, "Infinite timeout not allowed" ); 
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "Internal error" );
    return NULL;
  }

  int ret = 0;

  switch( txdir ) {
  // OUTPUT
  case TX_STREAM_DIRECTION__OUT:
    GRAPH_LOCK( SYSTEM ) {
      switch( state ) {
      case TX_STREAM_STATE__OPEN:
        ret = iOperation.System_SYS_CS.ResumeAgent( SYSTEM, timeout_ms );
        break;
      case TX_STREAM_STATE__CLOSE:
        ret = iOperation.System_SYS_CS.SuspendAgent( SYSTEM, timeout_ms );
        break;
      }
    } GRAPH_RELEASE;
    break;
  // INPUT
  case TX_STREAM_DIRECTION__IN:
    // Open
    switch( state ) {
    case TX_STREAM_STATE__OPEN:
      if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) && iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) ) {
        ret = iOperation.System_OPEN.ConsumerService.ResumeExecution( SYSTEM, timeout_ms );
      }
      break;
    case TX_STREAM_STATE__CLOSE:
      if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) && !iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) ) {
        ret = iOperation.System_OPEN.ConsumerService.SuspendExecution( SYSTEM, timeout_ms );
      }
      break;
    }
    break;
  // Invalid
  default:
    PyErr_SetString( PyExc_ValueError, "Direction must be 0 (out) or 1 (in)" );
    return NULL;
  }

  if( ret >= 0 ) {
    return PyLong_FromLong( ret );
  }
  else {
    PyErr_Format( PyVGX_OperationTimeout, "Failed to %s transaction %s service", state == TX_STREAM_STATE__OPEN ? "resume" : "suspend", txdir == TX_STREAM_DIRECTION__IN ? "input" : "output" );
    return NULL;
  }

}



/******************************************************************************
 * PyVGX_Operation__suspend_tx_in
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Operation__suspend_tx_in( PyObject *self, PyObject *args, PyObject *kwdict ) {
  // Don't stream the input service
  return __stream_tx( self, args, kwdict, TX_STREAM_STATE__CLOSE, TX_STREAM_DIRECTION__IN ); 
}



/******************************************************************************
 * PyVGX_Operation__resume_tx_in
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Operation__resume_tx_in( PyObject *self, PyObject *args, PyObject *kwdict ) {
  // Stream the input service
  return __stream_tx( self, args, kwdict, TX_STREAM_STATE__OPEN, TX_STREAM_DIRECTION__IN ); 
}



/******************************************************************************
 * PyVGX_Operation__suspend_tx_out
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Operation__suspend_tx_out( PyObject *self, PyObject *args, PyObject *kwdict ) {
  // Don't stream the output service
  return __stream_tx( self, args, kwdict, TX_STREAM_STATE__CLOSE, TX_STREAM_DIRECTION__OUT );
}



/******************************************************************************
 * PyVGX_Operation__resume_tx_out
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Operation__resume_tx_out( PyObject *self, PyObject *args, PyObject *kwdict ) {
  // Stream the output service
  return __stream_tx( self, args, kwdict, TX_STREAM_STATE__OPEN, TX_STREAM_DIRECTION__OUT ); 
}



/******************************************************************************
 * PyVGX_Operation__URI
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__URI( PyObject *py_system, PyObject *py_URI ) {
  PyObject *py_ret = NULL;
  const char *uri = PyUnicode_AsUTF8( py_URI );
  if( !uri ) {
    return NULL;
  }
  CString_t *CSTR__error = NULL;
  
#define __string_or_none( CString ) ((CString) ? PyUnicode_FromString( CStringValue( CString ) ) : PyNone_New())

  vgx_URI_t *URI = NULL;
  BEGIN_PYVGX_THREADS {
    URI = iURI.New( uri, &CSTR__error );
  } END_PYVGX_THREADS;

  if( URI ) {
    if( (py_ret = PyList_New( 7 )) != NULL ) {
      if( PyList_SetItem( py_ret, 0, __string_or_none( URI->CSTR__scheme ) ) < 0    ||
          PyList_SetItem( py_ret, 1, __string_or_none( URI->CSTR__userinfo ) ) < 0  ||
          PyList_SetItem( py_ret, 2, __string_or_none( URI->CSTR__host ) ) < 0      ||
          PyList_SetItem( py_ret, 3, __string_or_none( URI->CSTR__port ) ) < 0      ||
          PyList_SetItem( py_ret, 4, __string_or_none( URI->CSTR__path ) ) < 0      ||
          PyList_SetItem( py_ret, 5, __string_or_none( URI->CSTR__query ) ) < 0     ||
          PyList_SetItem( py_ret, 6, __string_or_none( URI->CSTR__fragment ) ) < 0 )
      {
        Py_DECREF( py_ret );
        py_ret = NULL;
      }

    }
    iURI.Delete( &URI );
  }
  else {
    const char *s_err = CSTR__error ? CStringValue( CSTR__error ) : "?";
    PyErr_SetString( PyExc_Exception, s_err );
  }

  iString.Discard( &CSTR__error );

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__SetDefaultURIs
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__SetDefaultURIs( PyObject *py_system, PyObject *py_URIs ) {
  PyObject *py_ret = NULL;

  vgx_StringList_t *default_URIs = NULL;
  int64_t sz = 0;
  const char *uri;

  XTRY {

    if( PyUnicode_Check( py_URIs ) ) {
      if( (default_URIs = iString.List.New( NULL, 1 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }
      uri = PyUnicode_AsUTF8( py_URIs );
      if( iString.List.SetItem( default_URIs, 0, uri ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }
    }
    else if( PyList_Check( py_URIs ) ) {
      sz = PyList_Size( py_URIs );
      if( (default_URIs = iString.List.New( NULL, sz )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      for( int64_t i=0; i<sz; i++ ) {
        if( (uri = PyUnicode_AsUTF8( PyList_GetItem( py_URIs, i ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_API, 0x004 );
        }
        if( iString.List.SetItem( default_URIs, i, uri ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
        }
      }
    }
    else if( py_URIs == Py_None ) {
      if( (default_URIs = iString.List.New( NULL, 0 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
      }
    }
    else {
      PyErr_SetString( PyExc_TypeError, "URI must be string, list or None" );
      THROW_ERROR( CXLIB_ERR_API, 0x007 );
    }

    // Validate URIs
    CString_t *CSTR__error = NULL;
    for( int64_t i=0; i<sz; i++ ) {
      uri = iString.List.GetChars( default_URIs, i );
      vgx_URI_t *URI = NULL;
      if( (URI = iURI.New( uri, &CSTR__error )) != NULL ) {
        // ok!
        iURI.Delete( &URI );
      }
      else {
        const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
        PyErr_Format( PyExc_ValueError, "Invalid URI '%s' (%s)", uri, serr );
        iString.Discard( &CSTR__error );
        THROW_ERROR( CXLIB_ERR_API, 0x008 );
      }
    }

    // Delete old instance (if any)
    __delete_default_URIs();

    // Set new string list
    _default_URIs = default_URIs;
    py_ret = Py_None;
    Py_INCREF( py_ret );
  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    iString.List.Discard( &default_URIs );
  }
  XFINALLY {
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Operation__GetDefaultURIs
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Operation__GetDefaultURIs( PyObject *py_system ) {
  if( _default_URIs ) {
    int64_t sz = iString.List.Size( _default_URIs );
    PyObject *py_list = PyList_New( sz );
    if( py_list ) {
      const char *uri;
      for( int64_t i=0; i<sz; i++ ) {
        uri = iString.List.GetChars( _default_URIs, i );
        if( PyList_SetItem( py_list, i, PyUnicode_FromString( uri ) ) < 0 ) {
          PyVGX_DECREF( py_list );
          return NULL;
        }
      }
    }
    return py_list;
  }
  else {
    PyErr_SetString( PyExc_Exception, "No default URI" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Operation__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Operation__members[] = {
  {NULL}  /* Sentinel */
};



SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Operation__attr( PyVGX_Operation *py_op, void *closure ) {
  return PyLong_FromLongLong( 0 );
}



/******************************************************************************
 * PyVGX_Operation__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_Operation__getset[] = {
  {"zero",       (getter)__PyVGX_Operation__attr,     (setter)NULL,   "",    NULL  },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PySequenceMethods tp_as_sequence_PyVGX_Operation = {
    .sq_length          = 0,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = 0,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0 
};



/******************************************************************************
 * PyVGX_Operation__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Operation__methods[] = {
  { "Attach",             (PyCFunction)PyVGX_Operation__attach,          METH_VARARGS | METH_KEYWORDS,   "Attach( [URI1, URI2, ...], handshake=True, timeout=30000 ) -> None" },
  { "Attached",           (PyCFunction)PyVGX_Operation__attached,        METH_NOARGS,                    "Attached() -> list" },
  { "Detach",             (PyCFunction)PyVGX_Operation__detach,          METH_VARARGS | METH_KEYWORDS,   "Detach( [URI [, force]] ) -> None" },

  { "Suspend",            (PyCFunction)PyVGX_Operation__suspend,         METH_VARARGS | METH_KEYWORDS,   "Suspend( timeout=60000 ) -> None" },
  { "Resume",             (PyCFunction)PyVGX_Operation__resume,          METH_NOARGS,                    "Resume() -> None" },
  { "Fence",              (PyCFunction)PyVGX_Operation__fence,           METH_VARARGS | METH_KEYWORDS,   "Fence( timeout=60000 ) -> None" },
  { "Assert",             (PyCFunction)PyVGX_Operation__assert,          METH_NOARGS,                    "Assert() -> None" },
  { "Counters",           (PyCFunction)PyVGX_Operation__counters,        METH_NOARGS,                    "Counters() -> dict" },
  { "Heartbeat",          (PyCFunction)PyVGX_Operation__heartbeat,       METH_O,                         "Heartbeat( bool ) -> None" },
  { "StrictSerial",       (PyCFunction)PyVGX_Operation__strict_serial,   METH_O,                         "StrictSerial( bool ) -> None" },
  { "VerifyCRC",          (PyCFunction)PyVGX_Operation__verify_crc,      METH_O,                         "VerifyCRC( bool ) -> None" },

  { "Deny",               (PyCFunction)PyVGX_Operation__deny,            METH_O,                         "Deny( opcode )" },
  { "Allow",              (PyCFunction)PyVGX_Operation__allow,           METH_O,                         "Allow( opcode )" },
  { "Profile",            (PyCFunction)PyVGX_Operation__profile,         METH_O,                         "Profile( op_profile )" },
  // TODO:
  // "SuspendParser()"
  // "ResumeParser()"

  { "Reset",              (PyCFunction)PyVGX_Operation__reset,           METH_NOARGS,                    "Reset()" },
  { "DataCRC32c",         (PyCFunction)PyVGX_Operation__data_crc32c,     METH_O,                         "DataCRC32c( data ) -> long"  },

  { "Bind",               (PyCFunction)PyVGX_Operation__bind,            METH_VARARGS | METH_KEYWORDS,   "Bind( port[, durable[, snapshot_threshold]] )" },
  { "Bound",              (PyCFunction)PyVGX_Operation__bound,           METH_NOARGS,                    "Bound() -> (port, durable)" },
  { "Unbind",             (PyCFunction)PyVGX_Operation__unbind,          METH_NOARGS,                    "Unbind()" },
  { "Subscribe",          (PyCFunction)PyVGX_Operation__subscribe,       METH_VARARGS | METH_KEYWORDS,   "Subscribe( (host, port), hardsync=False, timeout=5000 )" },
  { "Unsubscribe",        (PyCFunction)PyVGX_Operation__unsubscribe,     METH_VARARGS | METH_KEYWORDS,   "Unsubscribe()" },
  { "Consume",            (PyCFunction)PyVGX_Operation__consume,         METH_VARARGS | METH_KEYWORDS,   "Consume( data, timeout=0 )" },
  { "Pending",            (PyCFunction)PyVGX_Operation__pending,         METH_NOARGS,                    "Pending() -> n" },
  { "Throttle",           (PyCFunction)PyVGX_Operation__throttle,        METH_VARARGS | METH_KEYWORDS,   "Throttle() -> None" },

  { "ProduceComment",     (PyCFunction)PyVGX_Operation__produce_comment, METH_O,                         "ProduceComment( string ) -> None" },
  { "ProduceRaw",         (PyCFunction)PyVGX_Operation__produce_raw,     METH_O,                         "ProduceRaw( data ) -> None" },

  { "SuspendTxInput",     (PyCFunction)PyVGX_Operation__suspend_tx_in,   METH_VARARGS | METH_KEYWORDS,   "SuspendTxInput( [timeout_ms] )" },
  { "ResumeTxInput",      (PyCFunction)PyVGX_Operation__resume_tx_in,    METH_VARARGS | METH_KEYWORDS,   "ResumeTxInput( [timeout_ms] )" },
  { "SuspendTxOutput",    (PyCFunction)PyVGX_Operation__suspend_tx_out,  METH_VARARGS | METH_KEYWORDS,   "SuspendTxOutput( [timeout_ms] )" },
  { "ResumeTxOutput",     (PyCFunction)PyVGX_Operation__resume_tx_out,   METH_VARARGS | METH_KEYWORDS,   "ResumeTxOutput( [timeout_ms] )" },

  { "URI",                (PyCFunction)PyVGX_Operation__URI,             METH_O,                         "URI( uri_string ) -> list" },
  { "SetDefaultURIs",     (PyCFunction)PyVGX_Operation__SetDefaultURIs,  METH_O,                         "SetDefaultURIs( [URI1, URI2, ...] ) -> None" },
  { "GetDefaultURIs",     (PyCFunction)PyVGX_Operation__GetDefaultURIs,  METH_NOARGS,                    "GetDefaultURIs() -> list" },

  {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_Operation__OperationType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Operation__OperationType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Operation",
    .tp_basicsize       = sizeof(PyVGX_Operation),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Operation__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0, //(getattrfunc)PyVGX_Operation__getattr,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Operation__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &tp_as_sequence_PyVGX_Operation,
    .tp_as_mapping      = 0,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Operation objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Operation__methods,
    .tp_members         = PyVGX_Operation__members,
    .tp_getset          = PyVGX_Operation__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Operation__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Operation__new,
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


DLL_HIDDEN PyTypeObject * p_PyVGX_Operation__OperationType = &PyVGX_Operation__OperationType;
