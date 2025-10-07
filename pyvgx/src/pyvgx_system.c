/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_system.c
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





static PyVGX_Vertex *   __system_property_vertex( int timeout_ms );
static PyObject *       __system_property( PyVGX_System *py_system, PyObject *py_key, PyObject *py_value, bool count, bool del, int timeout_ms );
static int              __system_property_callback( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, CString_t *CSTR__payload, CString_t **CSTR__error );
static int              __system_aux_command_callback( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, vgx_StringList_t *data, CString_t **CSTR__error );
static int              __get_current_log_names( const char *path, char *systemlog, char *accesslog, int maxpath, int rotate_interval_seconds );
static int              __rotate_logs( const char *logpath, int rotate_interval_seconds );
static int              __system_contains( PyVGX_System *py_system, PyObject *py_key );
static PyObject *       __system_keys_and_values( bool keys, bool values );


static PyObject * PyVGX_System__IsInitialized( PyObject *py_system );
static PyObject * PyVGX_System__Initialize( PyObject *py_system, PyObject *args, PyObject *kwdict );
static PyObject * PyVGX_System__Unload( PyObject *py_system, PyObject *args, PyObject *kwdict );
static PyObject * PyVGX_System__Registry( PyObject *py_system );
static PyObject * PyVGX_System__Persist( PyObject *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__Sync( PyObject *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__GetGraph( PyVGX_System *py_system, PyObject *py_name );
static PyObject * PyVGX_System__DeleteGraph( PyObject *py_system, PyObject *args, PyObject *kwdict );

static PyObject * PyVGX_System__SuspendEvents( PyVGX_System *py_system );
static PyObject * PyVGX_System__EventsResumable( PyVGX_System *py_system );
static PyObject * PyVGX_System__ResumeEvents( PyVGX_System *py_system );
static PyObject * PyVGX_System__SetReadonly( PyVGX_System *py_system );
static PyObject * PyVGX_System__CountReadonly( PyVGX_System *py_system );
static PyObject * PyVGX_System__ClearReadonly( PyVGX_System *py_system );

static PyObject * PyVGX_System__Root( PyVGX_System *py_system );
static PyObject * PyVGX_System__System( PyVGX_System *py_system );
static PyObject * PyVGX_System__Status( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__Fingerprint( PyVGX_System *py_system );
static PyObject * PyVGX_System__DurabilityPoint( PyVGX_System *py_system );
static PyObject * PyVGX_System__WritableVertices( PyVGX_System *py_system );

static PyObject * PyVGX_System__StartHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__StopHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__ServiceInHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__ServerMetrics( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__ServerPorts( PyVGX_System *py_system );
static PyObject * PyVGX_System__ServerHost( PyVGX_System *py_system );
static PyObject * PyVGX_System__RequestRate( PyVGX_System *py_system );
static PyObject * PyVGX_System__ResetMetrics( PyVGX_System *py_system );
static PyObject * PyVGX_System__AddPlugin( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__RemovePlugin( PyVGX_System *py_system, PyObject *py_name );
static PyObject * PyVGX_System__GetPlugins( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__GetBuiltins( PyVGX_System *py_system, PyObject *args, PyObject *kwds );

static PyObject * PyVGX_System__RequestHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds );

static PyObject * PyVGX_System__RunServer( PyVGX_System *py_system, PyObject *args, PyObject *kwds );

static PyObject * PyVGX_System__SetProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__GetProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__HasProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__RemoveProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__SetProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__GetProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__NumProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_System__RemoveProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds );

static PyObject * PyVGX_System__items( PyVGX_System *py_system );
static PyObject * PyVGX_System__keys( PyVGX_System *py_system );
static PyObject * PyVGX_System__values( PyVGX_System *py_system );

static PyObject * PyVGX_System__Meminfo( PyObject *self );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int64_t pyvgx_SystemProfile( int64_t n_hash, int64_t n_mem, bool quiet ) {
  int64_t ret = 0;

  char *hashbuf = NULL;
  QWORD *mem = NULL;

#define print_message( Component, Code, Format, ... )  \
do {              \
  if( !quiet ) {  \
    __PYVGX_MESSAGE( INFO, Component, Code, Format, ##__VA_ARGS__ );  \
  }               \
} WHILE_ZERO

  int64_t t0 = __GET_CURRENT_MICROSECOND_TICK();

  XTRY {
    print_message( "system", 0, "Profile: Starting" );
    print_message( "system", 0, "Profile: Baseline scores = 100 (higher is better)" );
    print_message( "system", 0, "Measuring hash score:" );
    int64_t g_phys = 0;
    int64_t proc_phys = 0;
    get_system_physical_memory( &g_phys, NULL, &proc_phys );

    int64_t tms = __MILLISECONDS_SINCE_1970();
    int64_t szbuf = 1LL << 21;
    if( (hashbuf = malloc( szbuf + 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    memset( hashbuf, 0, szbuf + 1 );
    QWORD *q = (QWORD*)hashbuf;
    QWORD *qend = q + szbuf / sizeof( QWORD );
    // Initial entropy
    uint64_t tid = GET_CURRENT_THREAD_ID();
    uint64_t pid = GET_CURRENT_PROCESS_ID();
    __lfsr64( ihash64( (t0 ^ tms) + tid * pid ) );
    while( q < qend ) {
      *q++ = rand64();
    }

    // Compute the hash rounds
    //
    //
    int64_t t1 = __GET_CURRENT_MICROSECOND_TICK();
    for( int64_t x=0; x<n_hash; x++ ) {
      const sha256_t s = sha256( hashbuf );
      const objectid_t obid = obid_from_string( hashbuf );
      char *p = hashbuf;
      p = write_hex_qword( p, ihash64( obid.H ) );
      p = write_hex_qword( p, ihash64( obid.L ) );
      p = write_hex_qword( p, ihash64( s.A ) );
      p = write_hex_qword( p, ihash64( s.B ) );
      p = write_hex_qword( p, ihash64( s.C ) );
      p = write_hex_qword( p, ihash64( s.D ) );
      *p = '\0'; // shorter buf for all but the initial round
    }
    int64_t t2 = __GET_CURRENT_MICROSECOND_TICK();

    double tcpu = (double)(t2 - t1) / 1000.0;
    double hash_per_ms = n_hash / tcpu;

    static const double baseline_hash_per_ms = 1072.0;

    int hscore = (int)((hash_per_ms / baseline_hash_per_ms) * 100);
    print_message( "system", 0, "Profile: HSCORE  = %3d", hscore );

    // Memory access
    //
    // 
    print_message( "system", 0, "Measuring memory access score:" );
    if( n_mem > 0 && g_phys > (1LL << 32) ) {
      int bx = 0;
      double baseline[] = { 
                            5460,     // 16G
                            6050,     // 8G
                            6530,     // 4G
                            7260,     // 2G
                            7960,     // 1G
                            8910,     // 512M
                            9100,     // 256M
                            9680,     // 128M
                            11200,    // 64M
                            16000,    // 32M
                            29300,    // 16M
                            47600,    // 8M
                            59600,    // 4M
                            78800,    // 2M
                            204000,   // 1M
                            240000,   // 512k
                            327000,   // 256k
                            341000,   // 128k
                            429000,   // 64k
                            750000,   // 32k
                            752000,   // 16k
                            752000,   // 8k
                            753000    // 4k
                          };

      // 16 GB but go lower as needed
      int64_t szmem = 1LL << 34;
      int64_t nqw = szmem / sizeof( QWORD );
      print_message( "system", 0, "Allocating %lldG", szmem>>30 );
      while( CALIGNED_ARRAY( mem, QWORD, nqw ) == NULL ) {
        print_message( "system", 0, "(failed)" );
        szmem >>= 1;  // try again with half
        nqw >>= 1;
        ++bx;         // skip to next baseline slot
        if( szmem < (1LL << 32) ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
        print_message( "system", 0, "Allocating %lldG", szmem>>30 );
      }
      int64_t mask = nqw - 1;
      QWORD *memend = mem + nqw;
      q = mem;
      int64_t idx = rand64();
      while( q < memend ) {
        *q++ = idx;
        idx = ihash64( idx );
      }
      

      double mscore_total = 0.0;
      int buckets = 0;
      q = mem;
      int64_t nextidx = 0;
      int stride = 1; 
      while( mask >= 511 ) {
        int64_t t3 = __GET_CURRENT_MICROSECOND_TICK();
        int64_t t4 = 0;
        int64_t n_iter = 0;
        do {
          for( int64_t i=0; i<n_mem; i++ ) {
            // Point to the next random location
            q = mem + nextidx;
            // Use the number in this location to generate
            // the next offset
            nextidx = *q & mask;
            // Overwrite the just visited location with
            // a new randomized number
            *q *= 0x2127599bf4325c37ULL;
            *q ^= *q >> 23;
            *q *= 0x880355f21e6d1965ULL;
            *q ^= *q >> 47;
          }
          t4 = __GET_CURRENT_MICROSECOND_TICK();
          n_iter += n_mem;
          if( t4 - t3 < 100 ) {
            n_mem *= 4;
          }
        } while( t4 - t3 < 1000000 );
        double tmem = (double)(t4 - t3) / 1000.0;
        double mem_per_ms = n_iter / tmem;
        int64_t span_kb = ((mask + 1) * sizeof( QWORD )) >> 10;
        double mscore = mem_per_ms / baseline[bx];
        mscore_total += mscore;
        int imscore = (int)(mscore * 100);
        ++buckets;
        bx += stride;

        if( span_kb < 1000 ) {
          print_message( "system", 0, "Profile: MSCORE  = %3d [span %5lld k] [%lld mem/s]", imscore, span_kb, (int64_t)mem_per_ms );
        }
        else {
          print_message( "system", 0, "Profile: MSCORE  = %3d [span %5lld M] [%lld mem/s]", imscore, span_kb >> 10, (int64_t)mem_per_ms );
        }
        mask >>= stride;
      }
      print_message( "system", 0, "Profile: MSCORE  = %3d", (int)round(100.0 * mscore_total / buckets) );
      print_message( "system", 0, "\n" );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( hashbuf ) {
      free( hashbuf );
    }
    if( mem ) {
      ALIGNED_FREE( mem );
    }
  }
  int64_t t5 = __GET_CURRENT_MICROSECOND_TICK();
  ret = t5 - t0;

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_KeyVal_char_int64_t system_constants[] = {
    { .key = "ONE",               .value = 1 },
    { .key = NULL,                .value = 0 }
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * PyVGX_System_GetNewConstantsDict( void ) {
  PyObject *py_constants = PyDict_New();
  if( py_constants ) {
    // Map constants
    if( iPyVGXBuilder.MapIntegerConstants( py_constants, system_constants ) < 0 ) {
      PyVGX_DECREF( py_constants );
      py_constants = NULL;
    }
  }
  return py_constants;
}



/******************************************************************************
 * PyVGX_System__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_System__dealloc( PyVGX_System *py_system ) {
  Py_TYPE( py_system )->tp_free( py_system );
}



/******************************************************************************
 * PyVGX_System__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_System *py_system = NULL;
  py_system = (PyVGX_System*)type->tp_alloc( type, 0 );
  return (PyObject *)py_system;
}



/******************************************************************************
 * PyVGX_System__init
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int PyVGX_System__init( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  return 0;
}



/******************************************************************************
 * PyVGX_System__repr
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__repr( PyVGX_System *py_system ) {
  return PyUnicode_FromString( "pyvgx.System" );
}



/******************************************************************************
 * __PyVGX_System__root
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_System__root( PyVGX_System *py_system, void *closure ) {
  const CString_t * CSTR__sysroot = igraphfactory.SystemRoot();
  if( CSTR__sysroot ) {
    return PyUnicode_FromString( CStringValue( CSTR__sysroot ) );
  }
  else {
    Py_RETURN_NONE;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyVGX_Vertex * __system_property_vertex( int timeout_ms ) {
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "No system graph (system not initialized?)" );
    return NULL;
  }

  PyObject *pygraphcap = PyVGX_PyCapsule_NewNoErr( SYSTEM, NULL, NULL );
  PyObject *py_timeout = timeout_ms != 0 ? PyLong_FromLong( timeout_ms ) : NULL;

  PyObject *vcargs[] = {
    NULL,                   // -1
    (PyObject*)pygraphcap,  // graph
    g_py_SYS_prop_id,       // id
    g_py_SYS_prop_type,     // type
    g_py_SYS_prop_mode,     // mode
    g_py_minus_one,         // lifespan
    py_timeout              // timeout
  };

  PyObject *py_prop_vertex = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vertex__VertexType, vcargs+1, 6 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  Py_XDECREF( pygraphcap );
  Py_XDECREF( py_timeout);

  return (PyVGX_Vertex*)py_prop_vertex;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __send_system_aux_command( OperationProcessorAuxCommand cmd, CString_t *CSTR__payload ) {
  int ret = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( !SYSTEM ) {
    return -1;
  }

  XTRY {

    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( SYSTEM ) {
        if( (ret = iOperation.System_SYS_CS.SendSimpleAuxCommand( SYSTEM, cmd, CSTR__payload )) == 1 ) {
          iOperation.Graph_CS.SetModified( SYSTEM );
          COMMIT_GRAPH_OPERATION_CS( SYSTEM );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

    if( ret != 1 ) {
      int64_t sz = CStringLength( CSTR__payload );
      PyErr_Format( PyExc_Exception, "Failed to send raw data (%lld bytes)", sz );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }

  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
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
static int __send_system_property_command( OperationProcessorAuxCommand prop_cmd, PyObject *py_key, PyObject *py_value ) {
  int ret = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( !SYSTEM ) {
    return -1;
  }

  CString_t *CSTR__payload = NULL;

  XTRY {
    if( py_key && !PyUnicode_Check( py_key ) && !PyBytes_CheckExact( py_key ) ) {
      PyErr_SetString( PyExc_TypeError, "string or bytes required" );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( py_value == NULL ) {
      py_value = Py_None;
    }

    if( (CSTR__payload = iPyVGXCodec.NewEncodedObjectFromPyObject( py_key, py_value, SYSTEM->ephemeral_string_allocator_context, false )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    if( (ret = __send_system_aux_command( prop_cmd, CSTR__payload )) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }
  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_Format( PyExc_Exception, "internal error %03x", errcode );
    }
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__payload );
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __system_property( PyVGX_System *py_system, PyObject *py_key, PyObject *py_value, bool count, bool del, int timeout_ms ) {
  static PyObject *py_SetProperties = NULL;
  static PyObject *py_GetProperties = NULL;
  static PyObject *py_NumProperties = NULL;
  static PyObject *py_RemoveProperties = NULL;

  PyObject *py_ret = NULL;
  PyObject *py_prop_vertex = (PyObject*)__system_property_vertex( timeout_ms );
  if( py_prop_vertex ) {
    // Operate on single property
    if( py_key ) {
      if( py_value == NULL ) {
        // Delete system property
        if( del ) {
          if( PyObject_DelItem( py_prop_vertex, py_key ) == 0 ) {
            if( __send_system_property_command( OPAUX_SYSTEM_DEL_PROPERTY, py_key, NULL ) == 1 ) {
              py_ret = Py_None;
              Py_INCREF( py_ret );
            }
          }
        }
        // 1 or 0
        else if( count ) {
          py_ret = PyLong_FromLong( PySequence_Contains( (PyObject*)py_prop_vertex, py_key ) );
        }
        // Get system property
        else {
          py_ret = PyObject_GetItem( py_prop_vertex, py_key );
        }
      }
      // Set system property
      else {
        if( PyObject_SetItem( py_prop_vertex, py_key, py_value ) == 0 ) {
          if( __send_system_property_command( OPAUX_SYSTEM_SET_PROPERTY, py_key, py_value ) == 1 ) {
            py_ret = Py_None;
            Py_INCREF( py_ret );
          }
        }
      }
    }
    // Many properties
    else {
      if( del ) {
        if( py_RemoveProperties == NULL ) {
          py_RemoveProperties = PyUnicode_FromString( "RemoveProperties" );
        }
        if( py_RemoveProperties ) {
          if( (py_ret = PyObject_CallMethodObjArgs( py_prop_vertex, py_RemoveProperties, NULL )) != NULL ) {
            if( __send_system_property_command( OPAUX_SYSTEM_DEL_PROPERTIES, NULL, NULL ) != 1 ) {
              Py_DECREF( py_ret );
              py_ret = NULL;
            }
          }
        }
      }
      else if( count ) {
        if( py_NumProperties == NULL ) {
          py_NumProperties = PyUnicode_FromString( "NumProperties" );
        }
        if( py_NumProperties ) {
          py_ret = PyObject_CallMethodObjArgs( py_prop_vertex, py_NumProperties, NULL );
        }
      }
      else if( py_value ) {
        if( py_SetProperties == NULL ) {
          py_SetProperties = PyUnicode_FromString( "SetProperties" );
        }
        if( py_SetProperties ) {
          if( (py_ret = PyObject_CallMethodObjArgs( py_prop_vertex, py_SetProperties, py_value, NULL )) != NULL ) {
            if( __send_system_property_command( OPAUX_SYSTEM_SET_PROPERTIES, NULL, py_value ) != 1 ) {
              Py_DECREF( py_ret );
              py_ret = NULL;
            }
          }
        }
      }
      else {
        if( py_GetProperties == NULL ) {
          py_GetProperties = PyUnicode_FromString( "GetProperties" );
        }
        if( py_GetProperties ) {
          py_ret = PyObject_CallMethodObjArgs( py_prop_vertex, py_GetProperties, NULL );
        }
      }
    }
    Py_DECREF( py_prop_vertex );
  }

  if( py_ret == NULL ) {
    if( PyErr_Occurred() ) {
      PyObject *py_typ=NULL, *py_val=NULL, *py_tb=NULL;
      PyErr_Fetch( &py_typ, &py_val, &py_tb );
      if( py_typ == PyExc_LookupError || py_typ == PyExc_KeyError) {
        if( py_key ) {
          PyErr_Format( PyExc_KeyError, "Property does not exist: %R", py_key );
        }
        else if( py_val ) {
          PyErr_Format( PyExc_KeyError, "Property error: %R", py_val );
        }
        else {
          PyErr_SetString( PyExc_KeyError, "Unknown property error" );
        }
        Py_XDECREF( py_typ );
        Py_XDECREF( py_val );
        Py_XDECREF( py_tb );
      }
      else {
        PyErr_Restore( py_typ, py_val, py_tb );
      }
    }
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_System__set_item
 *
 ******************************************************************************
 */
static int PyVGX_System__set_item( PyVGX_System *py_system, PyObject *py_key, PyObject *py_value ) {
  bool del = py_value == NULL;
  PyObject *py_obj = __system_property( py_system, py_key, py_value, false, del, 1000 );
  if( py_obj ) {
    Py_DECREF( py_obj );
    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * PyVGX_System__get_item
 *
 ******************************************************************************
 */
static PyObject * PyVGX_System__get_item( PyVGX_System *py_system, PyObject *py_key ) {
  return __system_property( py_system, py_key, NULL, false, false, 1000 );
}



/******************************************************************************
 * PyVGX_System__len
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static Py_ssize_t PyVGX_System__len( PyVGX_System *py_system ) {
  PyVGX_Vertex *py_prop = __system_property_vertex( 1000 );
  int64_t sz = 0;
  if( py_prop ) {
    sz = CALLABLE( py_prop->vertex )->NumProperties( py_prop->vertex );
    Py_DECREF( py_prop );
  }
  return sz;
}



/******************************************************************************
 * PyVGX_System__IsInitialized
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER 
static PyObject * PyVGX_System__IsInitialized( PyObject *py_system ) {
  if( igraphfactory.IsInitialized() ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static int __system_property_callback( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, CString_t *CSTR__payload, CString_t **CSTR__error ) {
  int ret = 0;

  GRAPH_LOCK( SYSTEM ) {
    GRAPH_SUSPEND_LOCK( SYSTEM ) {

      BEGIN_PYTHON_INTERPRETER {
        PyObject *py_key = NULL;
        PyObject *py_value = NULL;

        XTRY {

          if( (py_value = iPyVGXCodec.NewPyObjectFromEncodedObject( CSTR__payload, &py_key )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
          }

          PyObject *py_ret = NULL;
          switch( cmd ) {
          case OPAUX_SYSTEM_DEL_PROPERTY:
            py_ret = __system_property( _global_system_object, py_key, NULL, false, true, 10000 );
            break;
          case OPAUX_SYSTEM_SET_PROPERTY:
            py_ret = __system_property( _global_system_object, py_key, py_value, false, false, 10000 );
            break;
          case OPAUX_SYSTEM_DEL_PROPERTIES:
            py_ret = __system_property( _global_system_object, NULL, NULL, false, true, 10000 );
            break;
          case OPAUX_SYSTEM_SET_PROPERTIES:
            py_ret = __system_property( _global_system_object, NULL, py_value, false, false, 10000 );
            break;
          default:
            PyErr_Format( PyExc_Exception, "bad command: %08x", cmd );
          }

          // ok
          if( py_ret ) {
            Py_DECREF( py_ret );
            ret = 1;
          }
          else {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
          }
        }
        XCATCH( errcode ) {
          iPyVGXBuilder.CatchPyExceptionIntoOutput( NULL, NULL, CSTR__error, NULL );
          ret = -1;
        }
        XFINALLY {
          Py_XDECREF( py_key );
          Py_XDECREF( py_value );
        }
      } END_PYTHON_INTERPRETER;

    } GRAPH_RESUME_LOCK;
  } GRAPH_RELEASE;

  return ret;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static int __system_aux_command_callback( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, vgx_StringList_t *data, CString_t **CSTR__error ) {

  if( (cmd & __OPAUX_MASK__PROPERTY) && iString.List.Size( data ) == 1 ) {
    CString_t *CSTR__payload = iString.List.GetItem( data, 0 );
    return __system_property_callback( SYSTEM, cmd, CSTR__payload, CSTR__error );
  }
  else {
    return 0;
  }

}



/******************************************************************************
 *
 ******************************************************************************
 */
static int __system_property_sync_callback( vgx_Graph_t *SYSTEM, CString_t **CSTR__error ) {
  int ret = 0;

  GRAPH_LOCK( SYSTEM ) {
    GRAPH_SUSPEND_LOCK( SYSTEM ) {

      BEGIN_PYTHON_INTERPRETER {
        PyObject *py_properties = NULL;

        XTRY {
          // Retrieve all local system properties
          if( (py_properties = __system_property( _global_system_object, NULL, NULL, false, false, 10000 )) == NULL ) {
            __set_error_string( CSTR__error, "Failed to retrieve local system properties" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
          }

          // Send all properties to attached destination(s)
          // One by one to ensure correct encoding
          Py_ssize_t pos = 0;
          PyObject *py_dictkey;
          PyObject *py_dictval;
          // Process all items in dict
          while( PyDict_Next( py_properties, &pos, &py_dictkey, &py_dictval ) ) {
            if( __send_system_property_command( OPAUX_SYSTEM_SET_PROPERTY, py_dictkey, py_dictval ) != 1 ) {
              __set_error_string( CSTR__error, "Failed to transmit local system properties to attached destination(s)" );
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
            }
          }
        }
        XCATCH( errcode ) {
          PyErr_Clear();
          ret = -1;
        }
        XFINALLY {
          Py_XDECREF( py_properties );
        }
      } END_PYTHON_INTERPRETER;

    } GRAPH_RESUME_LOCK;
  } GRAPH_RELEASE;

  return ret;
}



/******************************************************************************
 * PyVGX_System__Initialize
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Initialize( PyObject *py_system, PyObject *args, PyObject *kwdict ) {
  static bool __plugin_init = false;
  static char *kwlist[] = {"vgxroot", "http", "attach", "bind", "durable", "events", "idle", "readonly", "euclidean", NULL}; 
  const char *vgxroot = NULL;
  int64_t sz_vgxroot = 0;
  int http_port = 0;
  PyObject *py_attach = NULL;
  int bind = 0;
  int txlog = 0;
  int events = 1;
  int idle = 0;
  int readonly = 0;
  int euclidean = 1;
  PyObject *py_ret = NULL;

  vgx_StringTupleList_t *messages = NULL;
  CString_t *CSTR__error = NULL;
  const vgx_Graph_t **graphs = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|s#iOiiiiii", kwlist, &vgxroot, &sz_vgxroot, &http_port, &py_attach, &bind, &txlog, &events, &idle, &readonly, &euclidean ) ) {
    return NULL;
  }

  uint16_t uhttp_port = (uint16_t)http_port;
  if( uhttp_port != http_port ) {
    PyErr_SetString( PyExc_ValueError, "invalid http port number" );
    return NULL;
  }

  uint16_t ubind_port = (uint16_t)bind;
  if( ubind_port != bind ) {
    PyErr_SetString( PyExc_ValueError, "cannot bind, invalid port number" );
    return NULL;
  }

  if( txlog > 0 && ubind_port == 0 ) {
    PyErr_SetString( PyExc_Exception, "transaction log requires bind" );
    return NULL;
  }

  vector_type_t vtype = euclidean < 1 ? __VECTOR__MASK_FEATURE : VECTOR_TYPE_NULL;

  vgx_StringList_t *attach_URIs = NULL;
  int init = 0;

  XTRY {
    bool require_attach = false;

    // Get the system root directory if supplied
    if( vgxroot ) {
      strncpy( _vgx_context->sysroot, vgxroot, 254 );
    }

    // 'attach' parameter given
    if( py_attach ) {
      // URI string specified
      if( PyUnicode_Check( py_attach ) ) {
        if( (attach_URIs = iString.List.New( NULL, 1 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
        }
        if( iString.List.SetItem( attach_URIs, 0, PyUnicode_AsUTF8( py_attach ) ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
        require_attach = true;
      }
      // List of URI strings specified
      else if( PyList_Check( py_attach ) ) {
        int64_t n = PyList_Size( py_attach );
        if( (attach_URIs = iString.List.New( NULL, n )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
        }
        const char *uri;
        for( int64_t i=0; i<n; i++ ) {
          if( (uri = PyUnicode_AsUTF8( PyList_GetItem( py_attach, i ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_API, 0x004 );
          }
          if( iString.List.SetItem( attach_URIs, i, uri ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
          }
        }
        require_attach = true;
      }
      // URI explicitly set to None
      else if( py_attach == Py_None ) {
        attach_URIs = NULL;
      }
      // Error
      else {
        PyErr_SetString( PyExc_TypeError, "URI must be string, list or None" );
      }
    }
    // No default URI(s)
    else if( iString.List.Size( _default_URIs ) == 0 ) {
      attach_URIs = NULL;
    }
    // No 'attach' parameter: Try to connect to default URI(s)
    else if( (attach_URIs = iString.List.Clone( _default_URIs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
    }

    int attached = 0;
    int bound = 0;
    int http_start_code = 0;

    // Must be true for temporary vertices to be allowed to close!
    _registry_loaded = true;

    bool prev_euclidean = igraphfactory.EuclideanVectors();
    bool prev_feature = igraphfactory.FeatureVectors();
    
    BEGIN_PYVGX_THREADS {

      if( !igraphfactory.IsInitialized() ) {
        // events
        if( events > 0 ) {
          igraphfactory.EnableEvents();
        }
        else {
          igraphfactory.DisableEvents();
        }

        // Initialize
        if( (init = igraphfactory.Initialize( _vgx_context, vtype, idle>0, readonly>0, &messages )) == 1 ) {
          
          vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();

          CALLABLE( SYSTEM )->SetSystemAuxCommandCallback( SYSTEM, __system_aux_command_callback );
          CALLABLE( SYSTEM )->SetSystemSyncCallback( SYSTEM, __system_property_sync_callback );

          BEGIN_PYTHON_INTERPRETER {
            // Initialize plugins on first system initialization
            if( __plugin_init == false ) {
              if( __pyvgx_plugin__init_pyvgx() >= 0 && __pyvgx_plugin__init_plugins() > 0 ) {
                __plugin_init = true;
              }
            }
          } END_PYTHON_INTERPRETER;
          
          // Attach
          if( attach_URIs ) {
            attached = iSystem.AttachOutput( attach_URIs, TX_ATTACH_MODE_NORMAL, true, 30000, &CSTR__error );
          }

          // Bind
          if( bind > 0 ) {
            bool durable = txlog > 0;
            bound = iOperation.System_OPEN.ConsumerService.Start( SYSTEM, ubind_port, durable, LLONG_MAX ); // No auto snapshots
          }

          // Start HTTP Server in service out mode until plugin framework is ready
          if( http_port  > 0 ) {
            GRAPH_LOCK( SYSTEM ) {
              f_vgx_ServicePluginCall plugin_call = __pyvgx_plugin__get_call();
              http_start_code = iVGXServer.Service.StartNew( SYSTEM, NULL, uhttp_port, vgxroot, false, plugin_call, NULL );
            } GRAPH_RELEASE;
          }
        }
      }

    } END_PYVGX_THREADS;

    if( init == 1 ) {
      // Verify plugin framework
      if( __plugin_init == false ) {
        THROW_ERROR( CXLIB_ERR_API, 0x009 );
      }

      // Verify attached if required
      if( attach_URIs && require_attach && attached < 1 ) {
        PyErr_Format( PyExc_Exception, "Failed to attach: %s", CStringValueDefault( CSTR__error, "?" ) );
        THROW_ERROR( CXLIB_ERR_API, 0x00A );
      }

      // Verify bound if required
      if( bind > 0 && bound < 1 ) {
        PyErr_SetString( PyExc_Exception, "failed to start consumer service" );
        THROW_ERROR( CXLIB_ERR_API, 0x00B );
      }

      // Verify HTTP Server if required
      if( http_port > 0 && http_start_code < 0 ) {
        PyErr_SetString( PyExc_Exception, "Failed to start HTTP Server" );
        THROW_ERROR( CXLIB_ERR_API, 0x00C );
      }

      // Refresh global similarity if different mode
      bool euclidean_mode = igraphfactory.EuclideanVectors();
      bool feature_mode = igraphfactory.FeatureVectors();

      /*
      * 
      *   pE  E   pF  F  redo?
      * ---------------------
      * | 0 | 0 | 0 | 1 | Y |
      * | 0 | 1 | 0 | 0 | Y |
      * | 0 | 1 | 1 | 0 | Y |
      * | 1 | 0 | 0 | 1 | Y |
      * 
      */
      if( (euclidean_mode && !prev_euclidean) || (feature_mode && !prev_feature) ) {
        PyVGX_Similarity *default_similarity = (PyVGX_Similarity*)PyObject_CallObject( (PyObject*)p_PyVGX_Similarity__SimilarityType, NULL );
        // Update global similarity
        if( PyModule_AddObject( g_pyvgx, "DefaultSimilarity", (PyObject*)default_similarity ) < 0 ) {
          Py_DECREF( default_similarity );
          THROW_ERROR( CXLIB_ERR_API, 0x00D );
        }
        // TODO: Why not decref before re-assign?
        _global_default_similarity = default_similarity;
      }

      // Initialize graph module
      if( PyVGX_Graph_Init() < 0 ) {
        THROW_ERROR( CXLIB_ERR_API, 0x00E );
      }

      // OK
      _module_owns_registry = true;
      py_ret = Py_None;
      Py_INCREF( Py_None );
      if( http_port > 0 ) {
        BEGIN_PYVGX_THREADS {
          iVGXServer.Service.In( iSystem.GetSystemGraph() );
        } END_PYVGX_THREADS;
        INFO( 0x000, "VGXServer READY on port %d", http_port );
      }
      INFO( 0x000, "PyVGX READY" );
    }
    else if( init == 0 ) {
      PyErr_Format( PyExc_RuntimeError, "PyVGX already initialized, cannot re-initialize" );
      THROW_ERROR( CXLIB_ERR_API, 0x00F );
    }
    else {
      iPyVGXBuilder.SetErrorFromMessages( messages );
      THROW_ERROR( CXLIB_ERR_API, 0x010 );
    }
  }
  XCATCH( errcode ) {
    BEGIN_PYVGX_THREADS {
      if( init != 0 ) {
        igraphfactory.Shutdown();
      }
    } END_PYVGX_THREADS;
    
    // Forget vgxroot string
    _vgx_context->sysroot[0] = '\0';

    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    PyVGX_XDECREF( py_ret );
    py_ret = NULL;
    _registry_loaded = false;
  }
  XFINALLY {
    if( graphs ) {
      free( (void*)graphs );
      graphs = NULL;
    }
    _vgx_delete_string_tuple_list( &messages );
    iString.Discard( &CSTR__error );
    if( attach_URIs ) {
      iString.List.Discard( &attach_URIs );
    }
  }

  return py_ret;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int pyvgx_System_Initialize_Default( void ) {
  PyObject *py_noargs = PyTuple_New(0);
  PyObject *py_ret = PyVGX_System__Initialize( NULL, py_noargs, NULL );
  if( py_ret == NULL ) {
    return -1;
  }
  else {
    Py_DECREF( py_ret );
    return 0;
  }
}



/******************************************************************************
 * PyVGX_System__Unload
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Unload( PyObject *py_system, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = {"persist", NULL}; 
  int persist = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "|i", kwlist, &persist ) ) {
    return NULL;
  }

  if( PyVGX_Graph_Unload() < 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    CALLABLE( SYSTEM )->ClearSystemAuxCommandCallback( SYSTEM );
    CALLABLE( SYSTEM )->ClearSystemSyncCallback( SYSTEM );
  }

  // Persist all loaded graphs
  if( _module_owns_registry ) {
    int err = 0;
    if( persist ) {
      BEGIN_PYVGX_THREADS {
        GRAPH_FACTORY_ACQUIRE {
          vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
          if( graphs ) {
            vgx_Graph_t **cursor = graphs;
            vgx_Graph_t *graph;
            while( (graph = *cursor++) != NULL ) {
              if( CALLABLE( graph )->BulkSerialize( graph, 10000, true, true, NULL, NULL ) < 0 ) {
                ++err;
              }
            }
            free( (void*)graphs );
          }
        } GRAPH_FACTORY_RELEASE;
      } END_PYVGX_THREADS;
    }

    if( err ) {
      PyVGXError_SetString( PyExc_Exception, "Failed to serialize one or more graphs, unload not completed." );
      return NULL;
    }


    BEGIN_PYVGX_THREADS {
      igraphfactory.Shutdown();
    } END_PYVGX_THREADS;


    _module_owns_registry = false;
    _registry_loaded = false;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__Registry
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Registry( PyObject *py_system ) {
  PyObject *py_ret = PyDict_New();

  typedef struct __s_entry {
    CString_t *CSTR__name;
    int64_t order;
    int64_t size;
  } __entry;

  __entry *entries = NULL;
  int64_t sz = -1;

  if( py_ret ) {
    XTRY {
      PyObject *py_tuple;

      BEGIN_PYVGX_THREADS {
        if( igraphfactory.IsInitialized( ) ) {
          GRAPH_FACTORY_ACQUIRE {
            const vgx_Graph_t **graphs;
            if( (graphs = igraphfactory.ListGraphs( &sz )) != NULL ) {
              if( (entries = calloc( sz+1, sizeof( __entry ) )) != NULL ) {
                const vgx_Graph_t **cursor = graphs;
                __entry *entry = entries;
                const vgx_Graph_t *graph;
                while( (graph = *cursor++) != NULL ) {
                  vgx_Graph_t *g = (vgx_Graph_t*)graph;
                  const CString_t *CSTR__name = CALLABLE( g )->Name( g );
                  entry->CSTR__name = CALLABLE( CSTR__name )->CloneAlloc( CSTR__name, NULL );
                  entry->order = GraphOrder( g );
                  entry->size = GraphSize( g );
                  ++entry;
                }
              }
              free( (void*)graphs );
            }
          } GRAPH_FACTORY_RELEASE;
        }
      } END_PYVGX_THREADS;

      if( sz < 0 ) {
        PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x121 );
      }

      if( entries == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Internal error" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x122 );
      }

      const __entry *cursor = entries;
      const __entry *entry;

      while( (entry = cursor++)->CSTR__name ) {
        if( (py_tuple = PyTuple_New(2)) != NULL ) {
          PyVGX_DictStealItemString( py_ret, CStringValue( entry->CSTR__name ), py_tuple );
          PyTuple_SET_ITEM( py_tuple, 0, PyVGX_PyLong_FromLongLongNoErr( entry->order ) );
          PyTuple_SET_ITEM( py_tuple, 1, PyVGX_PyLong_FromLongLongNoErr( entry->size ) );
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x123 );
        }
      }
    }
    XCATCH( errcode ) {
      PyVGX_DECREF( py_ret );
      py_ret = NULL;
    }
    XFINALLY {
      free( entries );
    }
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_System__Persist
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Persist( PyObject *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "timeout", "force", "remote", NULL };
  int timeout_ms = 1000;
  int iforce = 0;
  int iremote = 0;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|iii", kwlist, &timeout_ms, &iforce, &iremote ) ) {
    return NULL;
  }

  int ret = 0;
  int err = 0;

  BEGIN_PYVGX_THREADS {
    bool force = iforce > 0;
    bool remote = iremote > 0;
    int64_t nq;
    XTRY {
      if( igraphfactory.IsInitialized() ) {
        // Persist all user graphs
        GRAPH_FACTORY_ACQUIRE {
          const vgx_Graph_t **graphs;
          if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) { 
            const vgx_Graph_t **cursor = graphs;
            const vgx_Graph_t *graph;
            while( (graph = *cursor++) != NULL ) {
              if( (nq = iPyVGXPersist.Serialize( (vgx_Graph_t*)graph, timeout_ms, force, remote )) < 0 ) {
                err = -1;
                break;
              }
            }
            free( (void*)graphs );
          }
        } GRAPH_FACTORY_RELEASE;

        if( err < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }

        // Persist SYSTEM
        vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
        if( SYSTEM ) {
          if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
            PYVGX_API_WARNING( "system", 0x000, "Skipping SYSTEM graph persist while consumer service is running (system.Unbind() then try again?)" );
          }
          else if( iPyVGXPersist.Serialize( SYSTEM, timeout_ms, force, remote ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
          }
        }
      }
    }
    XCATCH( errcode ) {
      ret = -1;
      err = errcode;
    }
    XFINALLY {
    }
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyVGX_SetPyErr( err );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__Sync
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Sync( PyObject *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "hard", "timeout", NULL };
  int hard = 0;
  int timeout_ms = 30000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|ii", kwlist, &hard, &timeout_ms ) ) {
    return NULL;
  }

  int err = 0;
  CString_t *CSTR__error = NULL;
  
  _pyvgx_api_enabled = false;

  BEGIN_PYVGX_THREADS {
    err = igraphfactory.SyncAllGraphs( hard, timeout_ms, &CSTR__error );
  } END_PYVGX_THREADS;

  if( err < 0 ) {
    if( CSTR__error ) {
      PyErr_Format( PyExc_Exception, "Synchronize operation failed: %s", CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    else {
      PyErr_SetString( PyExc_Exception, "Synchronize operation failed" );
    }
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__CancelSync
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__CancelSync( PyObject *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { NULL };
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "", kwlist ) ) {
    return NULL;
  }

  int sync_stopped;
  BEGIN_PYVGX_THREADS {
    sync_stopped = igraphfactory.CancelSync();
  } END_PYVGX_THREADS;

  if( sync_stopped < 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "Failed to cancel sync" );
    return NULL;
  }

  return PyLong_FromLong( sync_stopped ); 
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__GetGraph( PyVGX_System *py_system, PyObject *py_name ) {
  if( !py_name || !PyUnicode_Check( py_name ) ) {
    PyErr_SetString( PyExc_TypeError, "Graph name must be a string" );
    return NULL;
  }

  if( CharsEqualsConst( PyUnicode_AsUTF8( py_name ), "system" ) ) {
    return PyVGX_System__System( py_system );
  }

  return PyVGX_Graph__get_open( py_name );
}



/******************************************************************************
 * PyVGX_System__DeleteGraph
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__DeleteGraph( PyObject *py_system, PyObject *args, PyObject *kwdict ) {
  PyObject *py_ret = NULL;
  static char *kwlist[] = {"name", "path", "timeout", NULL}; 
  const char *name = NULL;
  int64_t sz_name = 0;
  const char *path = NULL;
  int64_t sz_path = 0;
  int timeout_ms = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "s#|s#i", kwlist, &name, &sz_name, &path, &sz_path, &timeout_ms ) ) {
    return NULL;
  }

  const CString_t *CSTR__name = CStringNew( name );
  const CString_t *CSTR__path = path ? CStringNew( path ) : CStringNew( name ); // if no path given, use name as path

  int ret;
  CString_t *CSTR__reason = NULL;
  BEGIN_PYVGX_THREADS {
    ret = igraphfactory.DeleteGraph( CSTR__path, CSTR__name, timeout_ms, false, &CSTR__reason );
  } END_PYVGX_THREADS;

  // Graph removed from registry
  if( ret == 1 ) {
    py_ret = Py_True;
  }
  // Graph not in registry
  else if( ret == 0 ) {
    py_ret = Py_False;
  }
  // Error
  else {
    if( CSTR__reason ) {
      PyErr_Format( PyExc_Exception, "Cannot delete graph: %s", CStringValue( CSTR__reason ) );
    }
    else {
      PyVGXError_SetString( PyExc_Exception, "Cannot delete graph: unknown reason" );
    }
  }

  if( py_ret ) {
    Py_INCREF( py_ret );
  }

  if( CSTR__reason ) {
    CStringDelete( CSTR__reason );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_System__SuspendEvents
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__SuspendEvents( PyVGX_System *py_system ) {
  if( igraphfactory.SuspendEvents( 30000 ) != 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "Failed to suspend events" );
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__EventsResumable
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__EventsResumable( PyVGX_System *py_system ) {
  PyObject *py_ret = NULL;
  int ret = igraphfactory.EventsResumable();
  if( ret > 0 ) {
    py_ret = Py_True;
  }
  else if( ret == 0 ) {
    py_ret = Py_False;
  }
  else {
    PyErr_SetString( PyVGX_InternalError, "Event state not available" );
  }
  if( py_ret ) {
    Py_INCREF( py_ret );
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_System__ResumeEvents
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ResumeEvents( PyVGX_System *py_system ) {
  if( igraphfactory.ResumeEvents() != 0 ) {
    PyErr_SetString( PyVGX_InternalError, "internal error" );
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__SetReadonly
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__SetReadonly( PyVGX_System *py_system ) {
  if( iSystem.IsAttached() ) {
    PyErr_SetString( PyVGX_InternalError, "Readonly not allowed with attached subscribers" );
    return NULL;
  }
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  if( igraphfactory.SetAllReadonly( 5000, &reason ) < 0 ) {
    if( reason != VGX_ACCESS_REASON_NONE ) {
      PyErr_Format( PyVGX_AccessError, "Access error reason: %d", reason );
    }
    else {
      PyErr_SetString( PyVGX_InternalError, "internal error" );
    }
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__CountReadonly
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__CountReadonly( PyVGX_System *py_system ) {
  int nRO = igraphfactory.CountAllReadonly();
  if( nRO < 0 ) {
    PyErr_SetString( PyVGX_InternalError, "internal error" );
    return NULL;
  }
  return PyLong_FromLong( nRO );
}



/******************************************************************************
 * PyVGX_System__ClearReadonly
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ClearReadonly( PyVGX_System *py_system ) {
  if( igraphfactory.ClearAllReadonly() != 0 ) {
    PyErr_SetString( PyVGX_InternalError, "internal error" );
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__Root
 *
 ******************************************************************************
 */
static PyObject * PyVGX_System__Root( PyVGX_System *py_system ) {
  return __PyVGX_System__root( py_system, NULL );
}



/******************************************************************************
 * PyVGX_System__System
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__System( PyVGX_System *py_system ) {
  PyObject *py_graph = NULL;
  PyObject *py_args = NULL;
  PyObject *py_path = NULL;
  PyObject *py_name = NULL;
  PyObject *py_local = NULL;
  PyObject *py_timeout = NULL;

  XTRY {
    if( !igraphfactory.IsInitialized() ) {
      PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Args
    py_args = PyTuple_New( 4 );
    py_name = PyBytes_FromString( "system" );
    py_path = PyBytes_FromString( "_system" );
    py_local = Py_False;
    Py_INCREF( py_local );
    py_timeout = PyLong_FromLong( 5000 );
    if( py_args == NULL || py_name == NULL || py_path == NULL || py_timeout == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    PyTuple_SET_ITEM( py_args, 0, py_name );
    PyTuple_SET_ITEM( py_args, 1, py_path );
    PyTuple_SET_ITEM( py_args, 2, py_local );
    PyTuple_SET_ITEM( py_args, 3, py_timeout );
    py_name = NULL;
    py_path = NULL;
    py_local = NULL;
    py_timeout = NULL;
    
    // Get graph
    if( (py_graph = PyObject_CallObject( (PyObject*)p_PyVGX_Graph__GraphType, py_args )) != NULL ) {
      PYVGX_API_WARNING( "system", 0x000, "Direct SYSTEM graph access may destabilize VGX instance. Be careful." );
    }

    // Clean up
    PyVGX_DECREF( py_args );
    py_args = NULL;

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_args );
    PyVGX_XDECREF( py_name );
    PyVGX_XDECREF( py_path );
    PyVGX_XDECREF( py_local );
    PyVGX_XDECREF( py_timeout );
    PyVGX_XDECREF( py_graph );
  }
  XFINALLY {
  }

  return py_graph;
}



/******************************************************************************
 * pyvgx_GraphStatus
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx_GraphStatus( vgx_Graph_t *graph, PyObject *args ) {

  PyObject *py_status = NULL;

  const char *item = NULL;
  int64_t sz_item = 0;
  int simple = 0;
  
  if( args ) {
    if( !PyArg_ParseTuple( args, "|z#i", &item, &sz_item, &simple ) ) {
      return NULL;
    }
  }

  bool full_status = simple > 0 ? false : true;

  // Graph status struct
  typedef struct __s_status_t {
    // Graph full path
    const char *path;
    // Graph base counts
    struct {
      const char *name;
      int64_t order;
      int64_t size;
      int64_t nprop;
      int64_t nvec;
    } base;
    // Graph enumerator counts
    struct {
      struct {
        int64_t rel;
        int64_t vtx;
        int64_t dim;
        int64_t val;
        int64_t key;
      } count;
    } enums;
    // Graph memory cost
    struct {
      struct {
        int64_t bytes;
        double overhead;
      } vertex;
      struct {
        int64_t bytes;
        double overhead;
      } prop;
      struct {
        int64_t bytes;
        double overhead;
      } vector;
      struct {
        int64_t bytes;
        double overhead;
      } arc;
    } memcost;
    // Graph timestamps
    struct {
      int64_t now_ts;
      int64_t age;
    } time;
    // Graph op count
    struct {
      int64_t count;
    } op;
    // Graph transaction counters
    struct {
      struct {
        objectid_t id;
        int64_t serial;
        int64_t count;
      } out;
      struct {
        objectid_t id;
        int64_t serial;
        int64_t count;
      } in;
    } tx;
    // Graph query info
    struct {
      int64_t count;
      double time_average;
    } query;
    
    // Graph memory info
    vgx_MemoryInfo_t meminfo;

  } __status_t;

  __status_t status = {0};


  XTRY {
    if( (py_status = PyDict_New()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    BEGIN_PYVGX_THREADS {
      // Path
      status.path = CALLABLE( graph )->FullPath( graph );

      // Name
      status.base.name = CStringValue( CALLABLE( graph )->Name( graph ) );

      // Order and Size
      status.base.order = GraphOrder( graph );
      status.base.size = GraphSize( graph );
      status.base.nprop = GraphPropCount( graph );
      status.base.nvec = GraphVectorCount( graph );

      GRAPH_LOCK( graph ) {
        // Time
        status.time.now_ts = _vgx_graph_seconds( graph );
        status.time.age = status.time.now_ts - _vgx_graph_inception( graph );
        
        // Operation counter
        status.op.count = iOperation.GetId_LCK( &graph->operation );

        // Recent transaction ID out
        idcpy( &status.tx.out.id, &graph->tx_id_out );

        // Recent serial number out
        status.tx.out.serial = graph->tx_serial_out;

        // Recent transaction count out
        status.tx.out.count = graph->tx_count_out;

        // Recent transaction ID in
        idcpy( &status.tx.in.id, &graph->tx_id_in );

        // Recent serial number in
        status.tx.in.serial = graph->tx_serial_in;

        // Recent transaction count in
        status.tx.in.count = graph->tx_count_in;

        // Enum counts
        if( full_status ) {
          if( graph->similarity && graph->similarity->parent == graph ) {
            status.enums.count.rel = pyvgx__enumerator_size( graph, iEnumerator_CS.Relationship.Count );
            status.enums.count.vtx = pyvgx__enumerator_size( graph, iEnumerator_CS.VertexType.Count );
            if( !igraphfactory.EuclideanVectors() ) {
              status.enums.count.dim = iEnumerator_CS.Dimension.Count( graph->similarity );
            }
          }
          status.enums.count.key = pyvgx__enumerator_size( graph, iEnumerator_CS.Property.Key.Count );
          status.enums.count.val = pyvgx__enumerator_size( graph, iEnumerator_CS.Property.Value.Count );

          // Memory
          status.meminfo = CALLABLE( graph )->advanced->GetMemoryInfo( graph );

          // Query
          status.query.count = CALLABLE( graph )->QueryCountNolock( graph );
          status.query.time_average = CALLABLE( graph )->QueryTimeAverageNolock( graph );
        }

      } GRAPH_RELEASE;

    } END_PYVGX_THREADS;

    // Path
    iPyVGXBuilder.DictMapStringToString( py_status, "path", status.path );

    // base
    PyObject *py_status_base;
    if( (py_status_base = PyDict_New()) != NULL ) {
      iPyVGXBuilder.DictMapStringToString( py_status_base, "name", status.base.name );
      iPyVGXBuilder.DictMapStringToLongLong( py_status_base, "order", status.base.order );
      iPyVGXBuilder.DictMapStringToLongLong( py_status_base, "size", status.base.size );
      iPyVGXBuilder.DictMapStringToLongLong( py_status_base, "properties", status.base.nprop );
      iPyVGXBuilder.DictMapStringToLongLong( py_status_base, "vectors", status.base.nvec );
      iPyVGXBuilder.DictMapStringToPyObject( py_status, "base", &py_status_base );
    }

    // time
    if( full_status ) {
      PyObject *py_status_time;
      if( (py_status_time = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToLongLong( py_status_time, "current", status.time.now_ts );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_time, "age", status.time.age );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "time", &py_status_time );
      }
    }

    // op
    if( full_status ) {
      PyObject *py_status_op;
      if( (py_status_op = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToLongLong( py_status_op, "count", status.op.count );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "op", &py_status_op );
      }
    }

    // tx
    if( full_status ) {
      PyObject *py_status_tx;
      if( (py_status_tx = PyDict_New()) != NULL ) {
        char idbuf[33];

        PyObject *py_tx;
        if( (py_tx = PyDict_New()) != NULL ) {
          idtostr( idbuf, &status.tx.out.id );
          iPyVGXBuilder.DictMapStringToString( py_tx, "id", idbuf );
          iPyVGXBuilder.DictMapStringToLongLong( py_tx, "serial", status.tx.out.serial );
          iPyVGXBuilder.DictMapStringToLongLong( py_tx, "count", status.tx.out.count );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_tx, "out", &py_tx );
        }

        if( (py_tx = PyDict_New()) != NULL ) {
          idtostr( idbuf, &status.tx.in.id );
          iPyVGXBuilder.DictMapStringToString( py_tx, "id", idbuf );
          iPyVGXBuilder.DictMapStringToLongLong( py_tx, "serial", status.tx.in.serial );
          iPyVGXBuilder.DictMapStringToLongLong( py_tx, "count", status.tx.in.count );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_tx, "in", &py_tx );
        }

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "transaction", &py_status_tx );
      }
    }

    // enum
    if( full_status ) {
      PyObject *py_status_enum;
      if( (py_status_enum = PyDict_New()) != NULL ) {

        PyObject *py_enum_details;
        PyObject *py_enum;

        // rel
        if( (py_enum = PyDict_New()) != NULL ) {
          if( (py_enum_details = pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->Relationships, false, (f_generic_enum_encoder)iEnumerator_CS.Relationship.GetEnum )) != NULL ) {
            iPyVGXBuilder.DictMapStringToPyObject( py_enum, "codec", &py_enum_details );
          }
          iPyVGXBuilder.DictMapStringToLongLong( py_enum, "count", status.enums.count.rel );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_enum, "relationship", &py_enum );
        }

        // vtx
        if( (py_enum = PyDict_New()) != NULL ) {
          if( (py_enum_details = pyvgx__enumerator_as_dict( graph, CALLABLE(graph)->simple->VertexTypes, false, (f_generic_enum_encoder)iEnumerator_CS.VertexType.GetEnum )) != NULL ) {
            iPyVGXBuilder.DictMapStringToPyObject( py_enum, "codec", &py_enum_details );
          }
          iPyVGXBuilder.DictMapStringToLongLong( py_enum, "count", status.enums.count.vtx );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_enum, "vertex_type", &py_enum );
        }

        // dim
        if( (py_enum = PyDict_New()) != NULL ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_enum, "count", status.enums.count.dim );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_enum, "dimension", &py_enum );
        }

        // property
        PyObject *py_prop;
        if( (py_prop = PyDict_New()) != NULL ) {
          // key
          if( (py_enum = PyDict_New()) != NULL ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_enum, "count", status.enums.count.key );
            iPyVGXBuilder.DictMapStringToPyObject( py_prop, "key", &py_enum );
          }

          // value
          if( (py_enum = PyDict_New()) != NULL ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_enum, "count", status.enums.count.val );
            iPyVGXBuilder.DictMapStringToPyObject( py_prop, "string_value", &py_enum );
          }

          iPyVGXBuilder.DictMapStringToPyObject( py_status_enum, "property", &py_prop );
        }

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "enum", &py_status_enum );
      }
    }

    // memcost
    if( full_status ) {
      PyObject *py_status_memcost;
      if( (py_status_memcost = PyDict_New()) != NULL ) {

        int64_t vertex_bytes = status.meminfo.pooled.vertex.object.bytes
                             + status.meminfo.pooled.index.global.bytes
                             + status.meminfo.pooled.index.type.bytes
                             + status.meminfo.pooled.codec.vxtype.bytes;
          
        int64_t property_bytes = status.meminfo.pooled.codec.vxprop.bytes
                               + status.meminfo.pooled.string.data.bytes
                               + status.meminfo.pooled.vertex.property.bytes;

        int64_t vector_bytes = status.meminfo.pooled.codec.dim.bytes
                             + status.meminfo.pooled.vector.dimension.bytes
                             + status.meminfo.pooled.vector.external.bytes
                             + status.meminfo.pooled.vector.internal.bytes;

        int64_t arc_bytes = status.meminfo.pooled.vertex.arcvector.bytes
                          + status.meminfo.pooled.codec.rel.bytes;

        if( status.base.order > 0 ) {
          status.memcost.vertex.bytes = iround( (double)vertex_bytes / status.base.order );
          status.memcost.vertex.overhead = (double)vertex_bytes / (status.base.order * sizeof(vgx_AllocatedVertex_t) );
        }

        if( status.base.nprop > 0 ) {
          status.memcost.prop.bytes = iround( (double)property_bytes / status.base.nprop );
          status.memcost.prop.overhead = (double)property_bytes / (status.base.nprop * sizeof( framehash_cell_t ));
        }

        if( status.base.nvec > 0 ) {
          status.memcost.vector.bytes = iround( (double)vector_bytes / status.base.nvec );
          status.memcost.vector.overhead = (double)vector_bytes / (status.base.nvec * sizeof( vgx_AllocatedVector_t ));
        }

        if( status.base.size > 0 ) {
          status.memcost.arc.bytes = iround( (double)arc_bytes / status.base.size );
          status.memcost.arc.overhead = (double)arc_bytes / (status.base.size * sizeof( framehash_cell_t ));
        }

        PyObject *py_entry;
        if( (py_entry = PyDict_New()) != NULL ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_entry, "bytes", status.memcost.vertex.bytes );
          iPyVGXBuilder.DictMapStringToFloat( py_entry, "overhead", status.memcost.vertex.overhead );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_memcost, "vertex", &py_entry );
        }

        if( (py_entry = PyDict_New()) != NULL ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_entry, "bytes", status.memcost.prop.bytes );
          iPyVGXBuilder.DictMapStringToFloat( py_entry, "overhead", status.memcost.prop.overhead );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_memcost, "property", &py_entry );
        }

        if( (py_entry = PyDict_New()) != NULL ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_entry, "bytes", status.memcost.vector.bytes );
          iPyVGXBuilder.DictMapStringToFloat( py_entry, "overhead", status.memcost.vector.overhead );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_memcost, "vector", &py_entry );
        }

        if( (py_entry = PyDict_New()) != NULL ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_entry, "bytes", status.memcost.arc.bytes );
          iPyVGXBuilder.DictMapStringToFloat( py_entry, "overhead", status.memcost.arc.overhead );
          iPyVGXBuilder.DictMapStringToPyObject( py_status_memcost, "arc", &py_entry );
        }

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "memcost", &py_status_memcost );
      }
    }

    // memory
    if( full_status ) {
      PyObject *py_status_memory;
      if( (py_status_memory = PyDict_New()) != NULL ) {
        int64_t vertex_bytes = status.meminfo.pooled.vertex.object.bytes;

        int64_t arc_bytes = status.meminfo.pooled.vertex.arcvector.bytes;

        int64_t prop_bytes = status.meminfo.pooled.vertex.property.bytes;

        int64_t index_bytes = status.meminfo.pooled.index.global.bytes
                            + status.meminfo.pooled.index.type.bytes
                            + status.meminfo.pooled.ephemeral.vtxmap.bytes;

        int64_t codec_bytes = status.meminfo.pooled.codec.rel.bytes
                            + status.meminfo.pooled.codec.vxtype.bytes
                            + status.meminfo.pooled.codec.dim.bytes
                            + status.meminfo.pooled.codec.vxprop.bytes;

        int64_t event_bytes = status.meminfo.pooled.schedule.total.bytes;

        int64_t vector_bytes = status.meminfo.pooled.vector.dimension.bytes
                             + status.meminfo.pooled.vector.external.bytes
                             + status.meminfo.pooled.vector.internal.bytes
                             + status.meminfo.pooled.ephemeral.vector.bytes;

        int64_t string_bytes = status.meminfo.pooled.string.data.bytes
                             + status.meminfo.pooled.ephemeral.string.bytes;

        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "vertex", vertex_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "arc", arc_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "property", prop_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "index", index_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "codec", codec_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "event", event_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "vector", vector_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "string", string_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "total", status.meminfo.pooled.total.bytes );

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "memory", &py_status_memory );
      }
    }

    // queries
    if( full_status ) {
      PyObject *py_status_query;
      if( (py_status_query = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToLongLong( py_status_query, "count", status.query.count );
        iPyVGXBuilder.DictMapStringToFloat( py_status_query, "latency_ms", round(status.query.time_average * 10000) / 10 );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "query", &py_status_query );
      }
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_status );
    py_status = NULL;
  }
  XFINALLY {
  }

  return py_status;
}



/******************************************************************************
 * PyVGX_System__Status
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Status__doc__,
  "Status( [graph[, simple]] ) -> dict\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_System__Status
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Status( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "graph", "simple", NULL };
  
  const char *graph_name = NULL;
  int simple = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|zi", kwlist, &graph_name, &simple ) ) {
    return NULL;
  }

  bool full_status = simple > 0 ? false : true;

  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  PyObject *py_status = NULL;

  // Specific graph status requested
  if( graph_name ) {
    PyObject *py_args = Py_BuildValue( "zi", NULL, simple );
    if( py_args == NULL ) {
      return NULL;
    }
    bool exists = false;
    CString_t *CSTR__name = CStringNew( graph_name );
    if( CSTR__name ) {
      vgx_Graph_t *graph = NULL;
      if( CStringEqualsChars( CSTR__name, "system" ) ) {
        graph = iSystem.GetSystemGraph();
      }
      else {
        objectid_t gid = igraphinfo.GraphID( NULL, CSTR__name );
        if( !idnone( &gid ) ) {
          graph = igraphfactory.GetGraphByObid( &gid );
        }
      }
      // Graph exists
      if( graph ) {
        exists = true;
        py_status = pyvgx_GraphStatus( graph, py_args );
      }
      iString.Discard( &CSTR__name );
    }
    Py_DECREF( py_args );

    // Success
    if( py_status ) {
      return py_status;
    }
    // Error
    else {
      if( exists == false ) {
        PyErr_SetString( PyExc_KeyError, graph_name );
      }
      else if( !PyErr_Occurred() ) {
        PyErr_SetString( PyExc_Exception, "unknown internal error" );
      }
      return NULL;
    }
  }


  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;

  PyObject *py_status_graphs = NULL;
  PyObject *py_status_time = NULL;
  PyObject *py_status_host = NULL;
  PyObject *py_status_tx = NULL;
  PyObject *py_status_http = NULL;
  PyObject *py_status_query = NULL;
  PyObject *py_status_durable = NULL;
  PyObject *py_status_memory = NULL;

  CString_t *CSTR__version = NULL;
  CString_t *CSTR__fqdn = NULL;
  char *ip = NULL;

  // Basic graph summary
  typedef struct __s_graphsum_t {
    char path[ MAX_PATH ];
    int64_t order;
    int64_t size;
    int64_t nprop;
    int64_t nvec;
    int64_t pooled_bytes;
  } __graphsum_t;

  // Transactional producer/consumer info
  typedef struct __s_tx_t {
    objectid_t id;
    int64_t serial;
    int64_t count;
    struct {
      vgx_StringList_t *uris;
      vgx_StringList_t *descriptions;
      int64_t bytes;
      int64_t operations;
      int64_t opcodes;
      int64_t transactions;
    } attached;
  } __tx_t;

  // HTTP server info
  typedef struct __s_http_t {
    CString_t *server_uri;
    vgx_StringList_t *client_uris;
    vgx_VGXServerPerfCounters_t counters;
    struct {
      double current;
      double alltime;
    } latency_80th;
    struct {
      double current;
      double alltime;
    } latency_95th;
    struct {
      double current;
      double alltime;
    } latency_99th;
  } __http_t;

  // System graph summary
  typedef struct __s_status_t {
    const char *version;

    struct {
      int64_t count;
    } prop;

    struct {
      int64_t count;
      double time_average;
    } query;

    struct {
      int64_t vgx_up;
      int64_t now_ts;
      int64_t age;
    } time;

    struct {
      const char *name;
      const char *ip;
    } host;

    struct {
      __tx_t out;
      __tx_t in;
      CString_t *CSTR__subscriber;
      struct {
        objectid_t txid;
        int64_t sn;
        int64_t ts;
        uint32_t age;
        int n_serializing;
      } durability;
    } tx;

    __http_t http;

    __graphsum_t *graphsum;

    vgx_MemoryInfo_t meminfo;

  } __status_t;

  __status_t status = {0};

  XTRY {
    if( (py_status = PyDict_New()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    int64_t uptime = __SECONDS_SINCE_1970() - _time_init;
    status.time.vgx_up = uptime;

    BEGIN_PYVGX_THREADS {


      // All graphs
      GRAPH_FACTORY_ACQUIRE {
        const vgx_Graph_t **graphs;
        if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) {
          const vgx_Graph_t **cursor = graphs;
          vgx_Graph_t *graph;
          int64_t n = 0;
          while( *cursor++ ) {
            ++n;
          }
          status.graphsum = calloc( n+2, sizeof( __graphsum_t ) );
          cursor = graphs;
          __graphsum_t *gs = status.graphsum;
          graph = SYSTEM;
          do {
            bool acquired;
            const char *path = CALLABLE( graph )->FullPath( graph );
            strncpy( gs->path, path, MAX_PATH-1 );
            gs->order = GraphOrder( graph );
            gs->size = GraphSize( graph );
            gs->nprop = GraphPropCount( graph );
            gs->nvec = GraphVectorCount( graph );
            TRY_GRAPH_LOCK( graph, 1000, acquired ) {
              if( full_status ) {
                vgx_MemoryInfo_t graph_meminfo = CALLABLE( graph )->advanced->GetMemoryInfo( graph );
                gs->pooled_bytes = graph_meminfo.pooled.total.bytes;
              }
            } GRAPH_RELEASE;
            if( !acquired ) {
              gs->order = -1;
              gs->size = -1;
              gs->nprop = -1;
              gs->nvec = -1;
              gs->pooled_bytes = -1;
            }
            ++gs;
          } while( (graph = (vgx_Graph_t*)*cursor++) != NULL );
          free( (void*)graphs );
        }
      } GRAPH_FACTORY_RELEASE;

      // Version
      CSTR__version = igraphinfo.Version(1);
      status.version = CSTR__version ? CStringValue( CSTR__version ) : "?";
      
      // FQDN and IP
      CSTR__fqdn = iURI.NewFqdn();
      status.host.name = CStringValueDefault( CSTR__fqdn, "?" );
      ip = cxgetip( NULL );
      status.host.ip = ip ? ip : "?.?.?.?";


      GRAPH_LOCK( SYSTEM ) {

        // Time
        status.time.now_ts = _vgx_graph_seconds( SYSTEM );
        status.time.age = status.time.now_ts - _vgx_graph_inception( SYSTEM );

        // Recent transaction ID out
        idcpy( &status.tx.out.id, &SYSTEM->tx_id_out );

        // Recent serial number out
        status.tx.out.serial = SYSTEM->tx_serial_out;

        // Total transaction count out
        status.tx.out.count = SYSTEM->tx_count_out;

        // Attached output stats
        status.tx.out.attached.bytes = agentsys->out_counters_CS.n_bytes;
        status.tx.out.attached.operations = agentsys->out_counters_CS.n_operations;
        status.tx.out.attached.opcodes = agentsys->out_counters_CS.n_opcodes;
        status.tx.out.attached.transactions = agentsys->out_counters_CS.n_transactions;

        // Recent transaction ID in
        idcpy( &status.tx.in.id, &SYSTEM->tx_id_in );

        // Recent serial number in
        status.tx.in.serial = SYSTEM->tx_serial_in;

        // Recent transaction count in
        status.tx.in.count = SYSTEM->tx_count_in;

        // Attached input stats
        status.tx.in.attached.bytes = agentsys->in_counters_CS.n_bytes;
        status.tx.in.attached.operations = agentsys->in_counters_CS.n_operations;
        status.tx.in.attached.opcodes = agentsys->in_counters_CS.n_opcodes;
        status.tx.in.attached.transactions = agentsys->in_counters_CS.n_transactions;

        // HTTP Server
        if( full_status ) {
          if( SYSTEM->vgxserverA ) {
            iVGXServer.Counters.Get( SYSTEM, &status.http.counters, 5000 );
            iVGXServer.Counters.GetLatencyPercentile( &status.http.counters, 0.80f, &status.http.latency_80th.current, &status.http.latency_80th.alltime );
            iVGXServer.Counters.GetLatencyPercentile( &status.http.counters, 0.95f, &status.http.latency_95th.current, &status.http.latency_95th.alltime );
            iVGXServer.Counters.GetLatencyPercentile( &status.http.counters, 0.99f, &status.http.latency_99th.current, &status.http.latency_99th.alltime );

            if( SYSTEM->vgxserverA->Listen ) {
              int s_port = iURI.PortInt( SYSTEM->vgxserverA->Listen );
              status.http.server_uri = CStringNewFormat( "http://%s:%d", status.host.ip, s_port );
            }
          }

          // Memory
          status.meminfo = CALLABLE( SYSTEM )->advanced->GetMemoryInfo( SYSTEM );
        }

      } GRAPH_RELEASE;

      // TX Durability
      igraphfactory.DurabilityPoint( &status.tx.durability.txid, &status.tx.durability.sn, &status.tx.durability.ts, &status.tx.durability.n_serializing );

      // Query
      status.query.count = iSystem.QueryCount();
      status.query.time_average = iSystem.QueryTimeAverage();

      // Attached
      status.tx.out.attached.uris = iSystem.AttachedOutputs( &status.tx.out.attached.descriptions, 5000 );
      status.tx.CSTR__subscriber = iSystem.InputAddress();
      if( (status.tx.in.attached.uris = iString.List.New( NULL, 0 )) != NULL ) {
        CString_t *CSTR__input = iSystem.AttachedInput();
        if( CSTR__input ) {
          iString.List.AppendSteal( status.tx.in.attached.uris, &CSTR__input );
        }
      }

    } END_PYVGX_THREADS;

    int64_t total_pooled_bytes = 0;

    // version
    iPyVGXBuilder.DictMapStringToString( py_status, "version", status.version );

    // graphs
    if( (py_status_graphs = PyDict_New()) != NULL ) {
      __graphsum_t *gs = status.graphsum;
      while( *gs->path ) {
        PyObject *py_dict = PyDict_New();
        if( py_dict ) {
          iPyVGXBuilder.DictMapStringToLongLong( py_dict, "order", gs->order );
          iPyVGXBuilder.DictMapStringToLongLong( py_dict, "size", gs->size );
          iPyVGXBuilder.DictMapStringToLongLong( py_dict, "properties", gs->nprop );
          iPyVGXBuilder.DictMapStringToLongLong( py_dict, "vectors", gs->nvec );
          if( full_status ) {
            iPyVGXBuilder.DictMapStringToLongLong( py_dict, "pooled_bytes", gs->pooled_bytes );
            iPyVGXBuilder.DictMapStringToFloat( py_dict, "pooled_to_vgx_process_ratio", round( 1000 * (double)gs->pooled_bytes / status.meminfo.system.process.use.bytes ) / 1000 );
            total_pooled_bytes += gs->pooled_bytes;
          }
          iPyVGXBuilder.DictMapStringToPyObject( py_status_graphs, gs->path, &py_dict );
        }
        ++gs;
      }
      iPyVGXBuilder.DictMapStringToPyObject( py_status, "graphs", &py_status_graphs );
    }

    // time
    if( full_status ) {
      if( (py_status_time = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToLongLong( py_status_time, "vgx_uptime", status.time.vgx_up );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_time, "vgx_current", status.time.now_ts );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_time, "vgx_age", status.time.age );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "time", &py_status_time );
      }
    }
    else {
      iPyVGXBuilder.DictMapStringToLongLong( py_status, "uptime", status.time.vgx_up );
    }

    // host
    if( full_status ) {
      if( (py_status_host = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToString( py_status_host, "name", status.host.name );
        iPyVGXBuilder.DictMapStringToString( py_status_host, "ip", status.host.ip );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "host", &py_status_host );
      }
    }

    // http
    if( (py_status_http = PyDict_New()) != NULL ) {
      if( full_status ) {
        if( status.http.server_uri ) {
          const char *uri = CStringValue( status.http.server_uri );
          PyObject *py_uri = PyUnicode_FromString( uri );
          if( py_uri ) {
            iPyVGXBuilder.DictMapStringToPyObject( py_status_http, "server", &py_uri );
          }
        }

        iPyVGXBuilder.DictMapStringToLongLong( py_status_http, "bytes_in", status.http.counters.bytes_in );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_http, "bytes_out", status.http.counters.bytes_out );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_http, "total_requests", status.http.counters.request_count_total );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_http, "connected_clients", status.http.counters.connected_clients );
      }

      PyObject *py_qps = PyDict_New();
      if( py_qps ) {
        iPyVGXBuilder.DictMapStringToFloat( py_qps, "current", round( status.http.counters.average_rate_short * 1000 ) / 1000 );
        if( full_status ) {
          iPyVGXBuilder.DictMapStringToFloat( py_qps, "all", round( status.http.counters.average_rate_long * 1000 ) / 1000 );
        }
        iPyVGXBuilder.DictMapStringToPyObject( py_status_http, "request_rate", &py_qps );
      }

      PyObject *py_latency = PyDict_New();
      if( py_latency ) {
        PyObject *py_current = PyDict_New();
        if( py_current ) {
          iPyVGXBuilder.DictMapStringToFloat( py_current, "mean", round(status.http.counters.average_duration_short * 100000) / 100 );
          iPyVGXBuilder.DictMapStringToFloat( py_current, "80th", round(status.http.latency_80th.current * 100000) / 100 );
          iPyVGXBuilder.DictMapStringToFloat( py_current, "95th", round(status.http.latency_95th.current * 100000) / 100 );
          iPyVGXBuilder.DictMapStringToFloat( py_current, "99th", round(status.http.latency_99th.current * 100000) / 100 );
          iPyVGXBuilder.DictMapStringToPyObject( py_latency, "current", &py_current );
        }
        if( full_status ) {
          PyObject *py_all = PyDict_New();
          if( py_all ) {
            iPyVGXBuilder.DictMapStringToFloat( py_all, "mean", round(status.http.counters.average_duration_long * 100000) / 100 );
            iPyVGXBuilder.DictMapStringToFloat( py_all, "80th", round(status.http.latency_80th.alltime * 100000) / 100 );
            iPyVGXBuilder.DictMapStringToFloat( py_all, "95th", round(status.http.latency_95th.alltime * 100000) / 100 );
            iPyVGXBuilder.DictMapStringToFloat( py_all, "99th", round(status.http.latency_99th.alltime * 100000) / 100 );
            iPyVGXBuilder.DictMapStringToPyObject( py_latency, "all", &py_all );
          }
        }
        iPyVGXBuilder.DictMapStringToPyObject( py_status_http, "latency_ms", &py_latency );
      }

      if( status.http.client_uris ) {
        int64_t sz = iString.List.Size( status.http.client_uris );
        PyObject *py_uri_list = PyList_New( sz );
        if( py_uri_list ) {
          for( int64_t i=0; i<sz; i++ ) {
            const char *uri = iString.List.GetChars( status.http.client_uris, i );
            PyObject *py_uri = PyUnicode_FromString( uri ? uri : "?" );
            if( py_uri == NULL ) {
              // fallback worst case
              py_uri = Py_None;
              Py_INCREF( py_uri );
            }
            PyList_SetItem( py_uri_list, i, py_uri );
          }
          iPyVGXBuilder.DictMapStringToPyObject( py_status_http, "clients", &py_uri_list );
        }
      }
      iPyVGXBuilder.DictMapStringToPyObject( py_status, "httpserver", &py_status_http );
    }

    // queries
    if( full_status ) {
      if( (py_status_query = PyDict_New()) != NULL ) {
        iPyVGXBuilder.DictMapStringToLongLong( py_status_query, "internal_queries", status.query.count );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_query, "average_internal_latency_us", (int64_t)(status.query.time_average * 1000000));
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "performance", &py_status_query );
      }
    }

    // tx
    if( full_status ) {
      if( (py_status_tx = PyDict_New()) != NULL ) {
        char idbuf[33];

        struct { __tx_t *ptx; const char *label; } TX[] = {
          { &status.tx.out, "out" },
          { &status.tx.in,  "in"  },
          { NULL, NULL }
        }, *tx = TX;

        while( tx->ptx ) {
          PyObject *py_tx;
          if( (py_tx = PyDict_New()) != NULL ) {
            idtostr( idbuf, &tx->ptx->id );
            iPyVGXBuilder.DictMapStringToString( py_tx, "id", idbuf );
            iPyVGXBuilder.DictMapStringToLongLong( py_tx, "serial", tx->ptx->serial );
            iPyVGXBuilder.DictMapStringToLongLong( py_tx, "total", tx->ptx->count );
            if( tx->ptx == &status.tx.in && status.tx.CSTR__subscriber ) {
              const char *subscriber = CStringValue( status.tx.CSTR__subscriber );
              PyObject *py_subscriber = PyUnicode_FromString( subscriber );
              if( py_subscriber ) {
                iPyVGXBuilder.DictMapStringToPyObject( py_tx, "subscriber", &py_subscriber );
              }
            }
            PyObject *py_attached;
            if( (py_attached = PyDict_New()) != NULL ) {
              iPyVGXBuilder.DictMapStringToLongLong( py_attached, "bytes", tx->ptx->attached.bytes );
              iPyVGXBuilder.DictMapStringToLongLong( py_attached, "operations", tx->ptx->attached.operations );
              iPyVGXBuilder.DictMapStringToLongLong( py_attached, "opcodes", tx->ptx->attached.opcodes );
              iPyVGXBuilder.DictMapStringToLongLong( py_attached, "transactions", tx->ptx->attached.transactions );

              vgx_StringList_t *descriptions;
              const char *label;
              if( tx->ptx == &status.tx.out ) {
                descriptions = tx->ptx->attached.descriptions;
                label = "subscribers";
              }
              else {
                descriptions = tx->ptx->attached.uris;
                label = "provider";
              }

              int64_t n_descriptions = 0;
              if( descriptions ) {
                n_descriptions = iString.List.Size( descriptions );
              }
              PyObject *py_descriptions;
              if( (py_descriptions = PyList_New( n_descriptions )) != NULL ) {
                for( int64_t i=0; i<n_descriptions; i++ ) {
                  CString_t *CSTR__description = iString.List.GetItem( descriptions, i );
                  PyObject *py_name = PyUnicode_FromStringAndSize( CStringValue( CSTR__description ), CStringLength( CSTR__description ) );
                  if( py_name ) {
                    PyList_SET_ITEM( py_descriptions, i, py_name );
                  }
                  else {
                    PyErr_Clear();
                    Py_INCREF( Py_None );
                    PyList_SET_ITEM( py_descriptions, i, Py_None );
                  }
                }
                iPyVGXBuilder.DictMapStringToPyObject( py_attached, label, &py_descriptions );
              }


              iPyVGXBuilder.DictMapStringToPyObject( py_tx, "attached", &py_attached );
            }
            iPyVGXBuilder.DictMapStringToPyObject( py_status_tx, tx->label, &py_tx );
          }
          ++tx;
        }

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "transaction", &py_status_tx );
      }
    }

    // durable
    if( full_status ) {
      if( (py_status_durable = PyDict_New()) != NULL ) {
        char idbuf[33];
        iPyVGXBuilder.DictMapStringToString( py_status_durable, "id", idtostr( idbuf, &status.tx.durability.txid ) );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_durable, "serial", status.tx.durability.sn );
        iPyVGXBuilder.DictMapStringToPyObject( py_status, "durable", &py_status_durable );
      }
    }

    // memory
    if( full_status ) {
      if( (py_status_memory = PyDict_New()) != NULL ) {
        int64_t prop_bytes = status.meminfo.pooled.vertex.property.bytes
                           + status.meminfo.pooled.codec.vxprop.bytes
                           + status.meminfo.pooled.string.data.bytes
                           + status.meminfo.pooled.ephemeral.string.bytes;

        int64_t pooled_bytes = status.meminfo.pooled.total.bytes;
        total_pooled_bytes += pooled_bytes;

        int64_t process_bytes = status.meminfo.system.process.use.bytes;

        int64_t nonpooled_bytes = process_bytes - total_pooled_bytes;

        int64_t physical_bytes = status.meminfo.system.global.physical.bytes;

        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "system_property_bytes", prop_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "system_pooled_bytes", pooled_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "vgx_pooled_bytes", total_pooled_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "vgx_process_bytes", process_bytes );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "vgx_nonpooled_bytes", nonpooled_bytes );
        iPyVGXBuilder.DictMapStringToFloat(    py_status_memory, "vgx_pooled_to_process_ratio", round( 1000 * (double)total_pooled_bytes / process_bytes ) / 1000 );
        iPyVGXBuilder.DictMapStringToLongLong( py_status_memory, "host_physical_bytes", physical_bytes );
        iPyVGXBuilder.DictMapStringToFloat(    py_status_memory, "vgx_process_to_host_physical_ratio", round( 1000* (double)process_bytes / physical_bytes ) / 1000 );

        iPyVGXBuilder.DictMapStringToPyObject( py_status, "memory", &py_status_memory );
      }
    }
  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_status );
    py_status = NULL;
  }
  XFINALLY {
    iString.Discard( &CSTR__version );
    iString.Discard( &CSTR__fqdn );
    free( ip );
    free( (void*)status.graphsum );
    iString.List.Discard( &status.tx.out.attached.uris );
    iString.List.Discard( &status.tx.out.attached.descriptions );
    iString.List.Discard( &status.tx.in.attached.uris );
    iString.List.Discard( &status.tx.in.attached.descriptions );
    iString.Discard( &status.tx.CSTR__subscriber );
    iString.Discard( &status.http.server_uri );
    iString.List.Discard( &status.http.client_uris );
  }

  return py_status;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Fingerprint( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  objectid_t obid = {0};
  char fingerprint[33] = {0};

  BEGIN_PYVGX_THREADS {
    obid = igraphfactory.Fingerprint();
  } END_PYVGX_THREADS;

  return PyUnicode_FromString( idtostr( fingerprint, &obid ) );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__UniqueLabel( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }
  char label[33] = {0};
  objectid_t obid = igraphfactory.UniqueLabel();
  return PyUnicode_FromString( idtostr( label, &obid ) );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__DurabilityPoint( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  objectid_t txid = {0};
  int64_t sn = 0;
  int64_t ts = 0;
  int nser = 0;
  int err = 0;

  BEGIN_PYVGX_THREADS {
    err = igraphfactory.DurabilityPoint( &txid, &sn, &ts, &nser );
  } END_PYVGX_THREADS;

  if( err < 0 ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  PyObject *py_ret = PyTuple_New( 3 );
  if( py_ret ) {
    char idbuf[33];
    PyObject *py_txid = PyUnicode_FromString( idtostr( idbuf, &txid ) );
    PyObject *py_sn = PyLong_FromLongLong( sn );
    PyObject *py_ts = PyLong_FromLongLong( ts );
    if( py_txid && py_sn && py_ts ) {
      PyTuple_SetItem( py_ret, 0, py_txid );
      PyTuple_SetItem( py_ret, 1, py_sn );
      PyTuple_SetItem( py_ret, 2, py_ts );
    }
    else {
      Py_XDECREF( py_txid );
      Py_XDECREF( py_sn );
      Py_XDECREF( py_ts );
      Py_DECREF( py_ret );
      py_ret = NULL;
    }
  }

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__WritableVertices( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  int64_t n_writable;
  BEGIN_PYVGX_THREADS {
    n_writable = iOperation.WritableVertices();
  } END_PYVGX_THREADS;

  return PyLong_FromLongLong( n_writable );
}



/******************************************************************************
 *
 *
 *
 *
 ******************************************************************************
 */
static vgx_VGXServerDispatcherConfig_t * __system__new_dispatcher_config( PyObject *py_dispatcher ) {
  /*
  
    {

      "options" : {
        "allow-incomplete: True
      },

      "replicas": [
        { "channels": 30 },
        { "channels": 30 },
        { "channels": 30, "priority":32, "primary":True }
      ],

      "partitions": [
        [
          { "host": "localhost", "port":9500 },
          { "host": "localhost", "port":9600 },
          { "host": "localhost", "port":9700 }
        ]
      ]

    }

  */

  vgx_VGXServerDispatcherConfig_t *cf = NULL;

  static const char *options_keys[] = {"allow-incomplete", NULL};
  static const char *replicas_keys[] = {"channels", "priority", "primary", NULL};
  static const char *partitions_keys[] = {"host", "port", NULL};

  XTRY {
    if( py_dispatcher == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( !PyDict_Check( py_dispatcher ) ) {
      PyErr_SetString( PyExc_TypeError, "dispatcher config must be dict" );
      THROW_SILENT( CXLIB_ERR_API, 0x002 );
    }

    // Number of partitions
    int width = 0;

    // Number of replicas per partition
    int height = 0;

    // Default port
    int dflt_port = -1;

    // Default channels
    int dflt_channels = -1;


    // ------------------------------------------------------------
    // Get replicas configuration and validate keys
    // ------------------------------------------------------------
    PyObject *py_replicas = PyDict_GetItemString( py_dispatcher, "replicas" );
    if( py_replicas == NULL ) {
      PyErr_SetString( PyExc_ValueError, "dispatcher replicas information missing" );
      THROW_SILENT( CXLIB_ERR_API, 0x003 );
    }
    if( !PyList_Check( py_replicas ) ) {
      PyErr_SetString( PyExc_TypeError, "dispatcher replicas must be list" );
      THROW_SILENT( CXLIB_ERR_API, 0x004 );
    }
    // Get number of replicas
    if( (height = (int)PyList_Size( py_replicas )) < 1 ) {
      PyErr_SetString( PyExc_ValueError, "dispatcher replicas cannot be empty" );
      THROW_SILENT( CXLIB_ERR_API, 0x005 );
    }
    // Make sure each replica config is a dict and contains no invalid keys
    for( int k=0; k<height; k++ ) {
      PyObject *py_entry = PyList_GetItem( py_replicas, k );
      if( !PyDict_Check( py_entry ) ) {
        PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d must be dict", k );
        THROW_SILENT( CXLIB_ERR_API, 0x006 );
      }
      const char **key = replicas_keys;
      int n_unmatched = (int)PyDict_Size( py_entry );
      while( *key != NULL ) {
        if( PyDict_GetItemString( py_entry, *key ) ) {
          --n_unmatched;
        }
        ++key;
      }
      if( n_unmatched > 0 ) {
        PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d contains %d invalid key(s)", k, n_unmatched );
        THROW_SILENT( CXLIB_ERR_API, 0x007 );
      }
    }


    // ------------------------------------------------------------
    // Get default port
    // ------------------------------------------------------------
    PyObject *py_dflt_port = PyDict_GetItemString( py_dispatcher, "port" );
    if( py_dflt_port ) {
      if( !PyLong_Check( py_dflt_port ) ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher default port must be int" );
        THROW_SILENT( CXLIB_ERR_API, 0x008 );
      }
      dflt_port = PyLong_AsLong( py_dflt_port );
      if( dflt_port < 1 || dflt_port > 65535 ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher default port out of range" );
        THROW_SILENT( CXLIB_ERR_API, 0x009 );
      }
    }


    // ------------------------------------------------------------
    // Get default channels
    // ------------------------------------------------------------
    PyObject *py_dflt_channels = PyDict_GetItemString( py_dispatcher, "channels" );
    if( py_dflt_channels ) {
      if( !PyLong_Check( py_dflt_channels ) ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher default channels must be int" );
        THROW_SILENT( CXLIB_ERR_API, 0x00A );
      }
      dflt_channels = PyLong_AsLong( py_dflt_port );
      if( dflt_channels < 1 || dflt_channels > 127 ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher default channels out of range" );
        THROW_SILENT( CXLIB_ERR_API, 0x00B );
      }
    }


    // ------------------------------------------------------------
    // Get partitions configuration and validate keys
    // ------------------------------------------------------------
    PyObject *py_partitions = PyDict_GetItemString( py_dispatcher, "partitions" );
    if( py_partitions == NULL ) {
      PyErr_SetString( PyExc_ValueError, "dispatcher partitions information missing" );
      THROW_SILENT( CXLIB_ERR_API, 0x010 );
    }
    if( !PyList_Check( py_partitions ) ) {
      PyErr_SetString( PyExc_TypeError, "dispatcher partitions must be list" );
      THROW_SILENT( CXLIB_ERR_API, 0x011 );
    }
    // Get number of partitions
    if( (width = (int)PyList_Size( py_partitions )) < 1 ) {
      PyErr_SetString( PyExc_ValueError, "dispatcher partitions cannot be empty" );
      THROW_SILENT( CXLIB_ERR_API, 0x012 );
    }
    // Make sure each partition entry is a list the same length as number of replicas, and
    // that each item in the list is a dict and contains no invalid keys
    for( int i=0; i<width; i++ ) {
      PyObject *py_part = PyList_GetItem( py_partitions, i );
      if( !PyList_Check( py_part ) ) {
        PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d must be list", i );
        THROW_SILENT( CXLIB_ERR_API, 0x013 );
      }
      if( PyList_Size( py_part ) != height ) {
        PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d incorrect number of hosts", i );
        THROW_SILENT( CXLIB_ERR_API, 0x014 );
      }
      for( int k=0; k<height; k++ ) {
        PyObject *py_entry = PyList_GetItem( py_part, k );
        if( !PyDict_Check( py_entry ) ) {
          PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d must be dict", i, k );
          THROW_SILENT( CXLIB_ERR_API, 0x015 );
        }
        const char **key = partitions_keys;
        int n_unmatched = (int)PyDict_Size( py_entry );
        while( *key != NULL ) {
          if( PyDict_GetItemString( py_entry, *key ) ) {
            --n_unmatched;
          }
          ++key;
        }
        if( n_unmatched > 0 ) {
          PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d contains %d invalid key(s)", i, k, n_unmatched );
          THROW_SILENT( CXLIB_ERR_API, 0x014 );
        }
      }
    }

    // ------------------------------------------------------------
    // Create default dispatcher config
    // ------------------------------------------------------------
    if( (cf = iVGXServer.Config.Dispatcher.New( width, height )) == NULL ) {
      PyErr_Format( PyExc_TypeError, "dispatcher configuration memory error" );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x020 );
    }

    // ------------------------------------------------------------
    // Get options configuration
    // ------------------------------------------------------------
    PyObject *py_options = PyDict_GetItemString( py_dispatcher, "options" );
    if( py_options ) {
      if( !PyDict_Check( py_options ) ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher options must be dict" );
        THROW_SILENT( CXLIB_ERR_API, 0x021 );
      }

      PyObject *py_allow_incomplete = PyDict_GetItemString( py_options, "allow-incomplete" );
      if( py_allow_incomplete ) {
        if( !PyBool_Check( py_allow_incomplete ) ) {
          PyErr_SetString( PyExc_TypeError, "dispatcher option allow-incomplete must be bool" );
          THROW_SILENT( CXLIB_ERR_API, 0x022 );
        }
        cf->allow_incomplete = py_allow_incomplete == Py_True;
      }
    }

    // ------------------------------------------------------------
    // Process each replica configuration
    // ------------------------------------------------------------
    int count_primary = 0;
    for( int k=0; k<height; k++ ) {
      // Replica config
      PyObject *py_replica_entry = PyList_GetItem( py_replicas, k );
      // Channels
      PyObject *py_channels = PyDict_GetItemString( py_replica_entry, "channels" );
      if( py_channels || dflt_channels > 0 ) {
        int channels = dflt_channels;
        if( py_channels ) {
          if( !PyLong_Check( py_channels ) ) {
            PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d channels must be int", k );
            THROW_SILENT( CXLIB_ERR_API, 0x023 );
          }
          channels = (int)PyLong_AsLong( py_channels );
        }
        // Configure replica channels
        if( iVGXServer.Config.Dispatcher.SetReplicaChannels( cf, k, channels ) < 0 ) {
          PyErr_Format( PyExc_ValueError, "dispatcher replicas entry %d channels %d out of range", k, channels );
          THROW_SILENT( CXLIB_ERR_API, 0x024 );
        }
      }
      // Priority
      PyObject *py_priority = PyDict_GetItemString( py_replica_entry, "priority" );
      if( py_priority ) {
        int priority = -1;
        if( !PyLong_Check( py_priority ) ) {
          PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d priority must be int", k );
          THROW_SILENT( CXLIB_ERR_API, 0x025 );
        }
        priority = (int)PyLong_AsLong( py_priority );
        // Configure replica priority
        if( iVGXServer.Config.Dispatcher.SetReplicaPriority( cf, k, priority ) < 0 ) {
          PyErr_Format( PyExc_ValueError, "dispatcher replicas entry %d priority %d out of range", k, priority );
          THROW_SILENT( CXLIB_ERR_API, 0x026 );
        }
      }
      // Primary
      PyObject *py_primary = PyDict_GetItemString( py_replica_entry, "primary" );
      if( py_primary ) {
        bool primary = false;
        if( PyBool_Check( py_primary ) ) {
          primary = py_primary == Py_True ? true : false;
        }
        else if( PyLong_Check( py_primary ) ) {
          primary = PyLong_AsLong( py_primary ) > 0 ? true : false;
        }
        else {
          PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d primary must be int or bool", k );
          THROW_SILENT( CXLIB_ERR_API, 0x027 );
        }
        if( count_primary++ > 0 ) {
          PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d at most one primary allowed", k );
          THROW_SILENT( CXLIB_ERR_API, 0x028 );
        }
        // Configure replica primary
        if( iVGXServer.Config.Dispatcher.SetReplicaAccess( cf, k, primary ) < 0 ) {
          PyErr_Format( PyExc_TypeError, "dispatcher replicas entry %d primary error", k );
          THROW_SILENT( CXLIB_ERR_API, 0x029 );
        }
      }

      // Process each partition for current replica
      for( int i=0; i<width; i++ ) {
        const char *host;
        int port;
        PyObject *py_part = PyList_GetItem( py_partitions, i );
        PyObject *py_entry = PyList_GetItem( py_part, k );
        // Get host
        PyObject *py_host = PyDict_GetItemString( py_entry, "host" );
        if( py_host == NULL ) {
          PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d host missing", i, k );
          THROW_SILENT( CXLIB_ERR_API, 0x02A );
        }
        if( !PyUnicode_Check( py_host ) ) {
          PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d host must be string", i, k );
          THROW_SILENT( CXLIB_ERR_API, 0x02B );
        }
        host = PyUnicode_AsUTF8( py_host );
        // Get port
        PyObject *py_port = PyDict_GetItemString( py_entry, "port" );
        if( py_port ) {
          if( !PyLong_Check( py_port ) ) {
            PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d port must be int", i, k );
            THROW_SILENT( CXLIB_ERR_API, 0x02C );
          }
          port = PyLong_AsLong( py_port );
          if( port < 1 || port > 65535 ) {
            PyErr_Format( PyExc_ValueError, "dispatcher partitions part %d entry %d port out of range", i, k );
            THROW_SILENT( CXLIB_ERR_API, 0x02D );
          }
        }
        else if( dflt_port > 0 ) {
          port = dflt_port;
        }
        else {
          PyErr_Format( PyExc_TypeError, "dispatcher partitions part %d entry %d port missing", i, k );
          THROW_SILENT( CXLIB_ERR_API, 0x02E );
        }
        // Configure replica host/port
        CString_t *CSTR__error = NULL;
        if( iVGXServer.Config.Dispatcher.SetReplicaAddress( cf, i, k, host, port, &CSTR__error ) < 0 ) {
          PyErr_Format( PyExc_ValueError, "dispatcher partitions part %d entry %d address error: %s", i, k, CSTR__error ? CStringValue( CSTR__error ) : "?" );
          iString.Discard( &CSTR__error );
          THROW_SILENT( CXLIB_ERR_MEMORY, 0x02F );
        }
      }
    }
    // No primary defined, set first replica primary
    if( count_primary == 0 ) {
      // Configure replica primary
      if( iVGXServer.Config.Dispatcher.SetReplicaAccess( cf, 0, true ) < 0 ) {
        PyErr_SetString( PyExc_TypeError, "dispatcher replicas set default primary error" );
        THROW_SILENT( CXLIB_ERR_API, 0x030 );
      }
    }


  }
  XCATCH( errcode ) {
    iVGXServer.Config.Dispatcher.Delete( &cf );
  }
  XFINALLY {
  }

  return cf;
}




/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__StartHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "port", "ip", "prefix", "servicein", "dispatcher", NULL };
  
  int port = 0;
  const char *ip = NULL;
  const char *prefix = NULL;
  int servicein = 1;
  PyObject *py_dispatcher = NULL;
  vgx_VGXServerDispatcherConfig_t *cf_dispatcher = NULL;
  int err = 0;
  PyObject *py_add = NULL;
  PyObject *py_addplugs = NULL;

  PyObject *py_cfdispatcher_json = NULL;

  XTRY {

    if( !PyArg_ParseTupleAndKeywords( args, kwds, "i|zziO", kwlist, &port, &ip, &prefix, &servicein, &py_dispatcher ) ) {
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    if( py_dispatcher == Py_None ) {
      py_dispatcher = NULL;
    }
    
    if( py_dispatcher ) {
      if( (py_cfdispatcher_json = iPyVGXCodec.NewJsonPyStringFromPyObject( py_dispatcher )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_API, 0x002 );
      }
    }

    if( !igraphfactory.IsInitialized() ) {
      PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
      THROW_SILENT( CXLIB_ERR_API, 0x003 );
    }

    uint16_t uport = (uint16_t)port;
    if( uport != port ) {
      PyErr_SetString( PyExc_ValueError, "invalid port number" );
      THROW_SILENT( CXLIB_ERR_API, 0x004 );
    }

    if( ip ) {
      if( !cxisvalidip( ip ) ) {
        PyErr_SetString( PyExc_ValueError, "invalid ip address" );
        THROW_SILENT( CXLIB_ERR_API, 0x005 );
      }
      char *nameinfo = cxgetnameinfo( ip );
      if( nameinfo == NULL ) {
        PyErr_SetString( PyExc_ValueError, "ip address does not resolve" );
        THROW_SILENT( CXLIB_ERR_API, 0x006 );
      }
      free( nameinfo );
    }

    if( prefix ) {
      if( !iString.Validate.UriPathSegment( prefix ) ) {
        PyErr_SetString( PyExc_ValueError, "invalid prefix" );
        THROW_SILENT( CXLIB_ERR_API, 0x007 );
      }
    }

    if( py_dispatcher ) {
      if( (cf_dispatcher = __system__new_dispatcher_config( py_dispatcher )) == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Unknown dispatcher configuration error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
      }
    }

    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    if( SYSTEM == NULL ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
    }

    int ret = 0;
    int prev_port = 0;
    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( SYSTEM ) {
        if( (prev_port = iVGXServer.Service.GetPort( SYSTEM )) < 0 ) {
          f_vgx_ServicePluginCall plugin_call = __pyvgx_plugin__get_call();
          bool service_in = servicein > 0 ? true : false;
          ret = iVGXServer.Service.StartNew( SYSTEM, ip, uport, prefix, service_in, plugin_call, &cf_dispatcher );
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

    if( prev_port > 0 ) {
      PyErr_Format( PyExc_Exception, "HTTP Server already running on port %d", prev_port );
      THROW_SILENT( CXLIB_ERR_API, 0x00A );
    }
    else if( ret < 0 ) {
      PyErr_SetString( PyExc_Exception, "Failed to start HTTP Server" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
    }

    const char *method_name = py_dispatcher ? "__matrix__AddDispatcherPlugins" : "__matrix__AddEnginePlugins";

    if( (py_addplugs  = PyObject_GetAttrString( (PyObject*)g_pyvgx, method_name )) == NULL ) {
      PyErr_Format( PyExc_LookupError, "unknown internal name: %s", method_name );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00C );
    }

    if( (py_add = PyObject_CallObject( py_addplugs, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00D );
    }

    if( py_cfdispatcher_json ) {
      Py_XDECREF( g_py_cfdispatcher );
      g_py_cfdispatcher = py_cfdispatcher_json;
      py_cfdispatcher_json = NULL;
    }

  }
  XCATCH( errcode ) {
    err = -1;
  }
  XFINALLY {
    iVGXServer.Config.Dispatcher.Delete( &cf_dispatcher );
    Py_XDECREF( py_add );
    Py_XDECREF( py_addplugs );
    Py_XDECREF( py_cfdispatcher_json );
  }

  if( err < 0 ) {
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__StopHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { NULL };

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "", kwlist ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  int ret = 0;
  int port = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      if( (port = iVGXServer.Service.GetPort( SYSTEM )) > 0 ) {
        ret = iVGXServer.Service.StopDelete( SYSTEM );
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  const char *method_name = "__matrix__RemovePlugins";
  PyObject *py_remplugs = PyObject_GetAttrString( (PyObject*)g_pyvgx, method_name );
  if( py_remplugs == NULL ) {
    PyErr_Format( PyExc_LookupError, "unknown internal name: %s", method_name );
    return NULL;
  }

  PyObject *py_rem = PyObject_CallObject( py_remplugs, NULL );
  Py_DECREF( py_remplugs );
  if( py_rem == NULL ) {
    return NULL;
  }
  Py_DECREF( py_rem );

  if( ret < 0 ) {
    PyErr_SetString( PyExc_Exception, "Failed to stop HTTP Server" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RestartHTTP( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  int ret = 0;
  CString_t *CSTR__error = NULL;

  BEGIN_PYVGX_THREADS {
    ret = iVGXServer.Service.Restart( SYSTEM, &CSTR__error );
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown internal error";
    PyErr_SetString( PyExc_Exception, serr );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__DispatcherConfig( PyVGX_System *py_system ) {
  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  if( g_py_cfdispatcher == NULL ) {
    Py_RETURN_NONE;
  }

  return iPyVGXCodec.NewPyObjectFromJsonPyString( g_py_cfdispatcher );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServiceInHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "service_in", NULL };

  int service_in = 1;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &service_in ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  int ret = 0;
  int port = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      if( (port = iVGXServer.Service.GetPort( SYSTEM )) > 0 ) {
        if( service_in > 0 ) {
          ret = iVGXServer.Service.In( SYSTEM );
        }
        else {
          ret = iVGXServer.Service.Out( SYSTEM );
        }
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( port < 0 ) {
    PyErr_SetString( PyExc_Exception, "The HTTP Server was not running" );
    return NULL;
  }
  else if( ret < 0 ) {
    PyErr_SetString( PyExc_Exception, "Failed to" );
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_System__ServerMetrics
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ServerMetrics__doc__,
  "ServerMetrics() -> dict\n"
  "\n"
);

/**************************************************************************//**
 * PyVGX_System__ServerMetrics
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServerMetrics( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "percentiles", NULL };

  PyObject *py_percentiles = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O", kwlist, &py_percentiles ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  typedef struct __s_latency_bucket_t {
    float pct;
    double current;
    double alltime;
  } __latency_bucket_t;

  // HTTP server info
  typedef struct __s_http_t {
    int serving;
    vgx_VGXServerPerfCounters_t counters;
    __latency_bucket_t *latency;
  } __http_t;

  __latency_bucket_t default_buckets[] = {
    { 0.800f, 0.0, 0.0 },
    { 0.950f, 0.0, 0.0 },
    { 0.990f, 0.0, 0.0 },
    {0}
  };

  PyObject *py_http = NULL;

  __http_t http = {
    .serving = 0,
    .counters = {0},
    .latency = default_buckets
  };

  XTRY {
    if( py_percentiles ) {
      if( !PyList_Check( py_percentiles ) ) {
        PyErr_SetString( PyExc_TypeError, "a list is required" );
        THROW_SILENT( CXLIB_ERR_API, 0x001 );
      }

      int64_t n = PyList_Size( py_percentiles );
      if( (http.latency = calloc( n+1, sizeof( __latency_bucket_t ) )) == NULL ) {
        PyErr_SetNone( PyExc_MemoryError );
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

      for( int64_t i=0; i<n; i++ ) {
        PyObject *py_item = PyList_GetItem( py_percentiles, i );
        float pct = 0.0;
        if( PyLong_Check( py_item ) ) {
          pct = (float)(PyLong_AsLongLong( py_item ) / 100.0);
        }
        else if( PyFloat_Check( py_item ) ) {
          pct = (float)(PyFloat_AsDouble( py_item ) / 100.0);
        }
        else {
          PyErr_SetString( PyExc_TypeError, "percentile must numeric" );
          THROW_SILENT( CXLIB_ERR_API, 0x003 );
        }

        if( pct <= 0.0f || pct >= 1.0f ) {
          PyErr_SetString( PyExc_ValueError, "percentile must be in range <0,100>" );
          THROW_SILENT( CXLIB_ERR_API, 0x004 );
        }
      }

      PyList_Sort( py_percentiles );

      __latency_bucket_t *bucket = http.latency;
      for( int64_t i=0; i<n; i++ ) {
        PyObject *py_item = PyList_GetItem( py_percentiles, i );
        if( PyLong_Check( py_item ) ) {
          bucket->pct = (float)(PyLong_AsLongLong( py_item ) / 100.0);
        }
        else {
          bucket->pct = (float)(PyFloat_AsDouble( py_item ) / 100.0);
        }
        ++bucket;
      }
    }

    if( (py_http = PyDict_New()) == NULL ) {
      PyErr_SetNone( PyExc_MemoryError );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
    }

    BEGIN_PYVGX_THREADS {
      GRAPH_LOCK( SYSTEM ) {
        // HTTP Server
        vgx_VGXServer_t *server = SYSTEM->vgxserverA;
        if( server ) {
          GRAPH_SUSPEND_LOCK( SYSTEM ) {
            COMLIB_TASK_LOCK( server->TASK ) {
              http.serving = server->control.public_service_in_TCS;
            } COMLIB_TASK_RELEASE;
          } GRAPH_RESUME_LOCK;
          iVGXServer.Counters.Get( SYSTEM, &http.counters, 5000 );
          __latency_bucket_t *bucket = http.latency;
          while( bucket->pct > 0.0f ) {
            iVGXServer.Counters.GetLatencyPercentile( &http.counters, bucket->pct, &bucket->current, &bucket->alltime );
            ++bucket;
          }
        }
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

    // http
    iPyVGXBuilder.DictMapStringToLongLong( py_http, "bytes_in", http.counters.bytes_in );
    iPyVGXBuilder.DictMapStringToLongLong( py_http, "bytes_out", http.counters.bytes_out );
    iPyVGXBuilder.DictMapStringToLongLong( py_http, "serving", http.serving );
    iPyVGXBuilder.DictMapStringToLongLong( py_http, "total_requests", http.counters.request_count_total );
    iPyVGXBuilder.DictMapStringToLongLong( py_http, "connected_clients", http.counters.connected_clients );

    PyObject *py_qps = PyDict_New();
    if( py_qps ) {
      iPyVGXBuilder.DictMapStringToFloat( py_qps, "current", round( http.counters.average_rate_short * 1000 ) / 1000 );
      iPyVGXBuilder.DictMapStringToFloat( py_qps, "all", round( http.counters.average_rate_long * 1000 ) / 1000 );
      iPyVGXBuilder.DictMapStringToPyObject( py_http, "request_rate", &py_qps );
    }

    PyObject *py_latency = PyDict_New();
    if( py_latency ) {
      PyObject *py_current = PyDict_New();
      PyObject *py_all = PyDict_New();
      if( py_current && py_all ) {
        iPyVGXBuilder.DictMapStringToFloat( py_current, "mean", round(http.counters.average_duration_short * 100000) / 100 );
        iPyVGXBuilder.DictMapStringToFloat( py_all, "mean", round(http.counters.average_duration_long * 100000) / 100 );
        __latency_bucket_t *bucket = http.latency;
        char key[8] = {0};
        while( bucket->pct > 0.0f ) {
          snprintf( key, 7, "%.1f", ((double)bucket->pct * 100.0) );
          iPyVGXBuilder.DictMapStringToFloat( py_current, key, round(bucket->current * 100000) / 100 );
          iPyVGXBuilder.DictMapStringToFloat( py_all, key, round(bucket->alltime * 100000) / 100 );
          ++bucket;
        }
        iPyVGXBuilder.DictMapStringToPyObject( py_latency, "current", &py_current );
        iPyVGXBuilder.DictMapStringToPyObject( py_latency, "all", &py_all );
      }
      iPyVGXBuilder.DictMapStringToPyObject( py_http, "latency_ms", &py_latency );
    }

  }
  XCATCH( errcode ) {
    PyVGX_XDECREF( py_http );
    py_http = NULL;
  }
  XFINALLY {
    if( http.latency != default_buckets ) {
      free( http.latency );
    }
  }

  return py_http;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServerPorts( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  int baseport = 0;
  int adminport = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      baseport = iVGXServer.Service.GetPort( SYSTEM );
      adminport = iVGXServer.Service.GetAdminPort( SYSTEM );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;


  PyObject *py_ports = PyDict_New();
  int err = 0;
  if( py_ports ) {
    err += iPyVGXBuilder.DictMapStringToInt( py_ports, "base", baseport );
    err += iPyVGXBuilder.DictMapStringToInt( py_ports, "admin", adminport );
    if( err < 0 ) {
      Py_DECREF( py_ports );
      py_ports = NULL;
    }
  }

  return py_ports;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServerHost( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  CString_t *CSTR__fqdn = NULL;
  const char *name;
  char *ip;

  BEGIN_PYVGX_THREADS {
    CSTR__fqdn = iURI.NewFqdn();
    name = CStringValueDefault( CSTR__fqdn, "?" );
    ip = cxgetip( NULL );
  } END_PYVGX_THREADS;

  PyObject *py_host = Py_BuildValue( "{ssss}", "host", name, "ip", ip?ip:"?.?.?.?" );
  free( ip );
  iString.Discard( &CSTR__fqdn );

  return py_host;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServerPrefix( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  const char *prefix = NULL;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      prefix = iVGXServer.Service.GetPrefix( SYSTEM );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( prefix == NULL ) {
    Py_RETURN_NONE;
  }

  return PyUnicode_FromString( prefix );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ServerAdminIP( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  const char *admin_ip = NULL;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      admin_ip = iVGXServer.Service.GetAdminIP( SYSTEM );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( admin_ip == NULL ) {
    Py_RETURN_NONE;
  }

  return PyUnicode_FromString( admin_ip );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RequestRate( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  int ret = 0;
  vgx_VGXServerPerfCounters_t counters = {0};

  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      // Main HTTP Server
      if( SYSTEM->vgxserverA ) {
        ret = iVGXServer.Counters.Get( SYSTEM, &counters, 5000 );
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "Could not get http counters" );
    return NULL;
  }
 
  return PyFloat_FromDouble( counters.average_rate_short );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__ResetMetrics( PyVGX_System *py_system ) {
  vgx_Graph_t *SYSTEM = NULL;

  if( !igraphfactory.IsInitialized() || (SYSTEM = iSystem.GetSystemGraph()) == NULL ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  int ret = 0;

  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      // HTTP Server
      if( SYSTEM->vgxserverA ) {
        ret = iVGXServer.Counters.Reset( SYSTEM );
      }
      // Transaction Counters
      iOperation.Counters.Reset( SYSTEM );
    } GRAPH_RELEASE;
    // Exception Counters
    cxlib_exception_counters_reset();
  } END_PYVGX_THREADS;

  if( ret < 0 ) {
    PyErr_SetString( PyVGX_OperationTimeout, "Could not reset counters" );
    return NULL;
  }
 
  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __plugin_function_name( PyObject *py_plugin ) {
  if( !PyFunction_Check( py_plugin ) ) {
    PyErr_Format( PyExc_TypeError, "plugin must be a function, not %s", Py_TYPE(py_plugin)->tp_name );
    return NULL;
  }
  return PyObject_GetAttrString( py_plugin, "__name__" );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__AddPlugin( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {

  static char *kwlist[] = { "plugin", "name", "graph", "engine", "pre", "post", NULL };

  PyObject *py_plugin = NULL;
  const char *plugin_name = NULL;
  int64_t sz_name = 0;
  PyObject *py_bound_graph = NULL;
  PyObject *py_engine = NULL;
  PyObject *py_pre = NULL;
  PyObject *py_post = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|Os#OOOO", kwlist, &py_plugin, &plugin_name, &sz_name, &py_bound_graph, &py_engine, &py_pre, &py_post ) ) {
    return NULL;
  }

  if( py_plugin == Py_None ) {
    py_plugin = NULL;
  }
  if( py_engine == Py_None ) {
    py_engine = NULL;
  }
  if( py_pre == Py_None ) {
    py_pre = NULL;
  }
  if( py_post == Py_None ) {
    py_post = NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyErr_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  if( py_plugin && py_engine ) {
    PyErr_SetString( PyExc_ValueError, "'plugin' is an alias for 'engine', cannot use both" );
    return NULL;
  }

  if( py_engine ) {
    py_plugin = py_engine;
  }

  if( py_plugin && (py_pre || py_post) ) {
    PyErr_SetString( PyExc_ValueError, "'pre'/'post' cannot be combined with 'plugin'" );
    return NULL;
  }

  if( !py_plugin && !py_pre && !py_post ) {
    PyErr_SetString( PyExc_ValueError, "At least one of 'plugin', 'pre' or 'post' is required" );
    return NULL;
  }

  // Determine name automatically from first plugin function
  if( plugin_name == NULL ) {
    PyObject *py_name = NULL;
    if( py_plugin ) {
      py_name = __plugin_function_name( py_plugin );
    }
    else if( py_pre ) {
      py_name = __plugin_function_name( py_pre );
    }
    else if( py_post ) {
      py_name = __plugin_function_name( py_post );
    }
    if( py_name  ) {
      plugin_name = PyUnicode_AsUTF8( py_name );
    }
  }

  if( plugin_name == NULL ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "unknown internal error" );
    }
    return NULL;
  }

  // Main exec plugin
  if( py_plugin ) {
    if( __pyvgx_plugin__add( plugin_name, VGX_SERVER_PLUGIN_PHASE__EXEC, py_plugin, py_bound_graph ) < 0 ) {
      return NULL;
    }
  }
  else {
    // PRE plugin
    if( py_pre ) {
      if( __pyvgx_plugin__add( plugin_name, VGX_SERVER_PLUGIN_PHASE__PRE, py_pre, py_bound_graph ) < 0 ) {
        return NULL;
      }
    }
    // POST plugin
    if( py_post ) {
      if( __pyvgx_plugin__add( plugin_name, VGX_SERVER_PLUGIN_PHASE__POST, py_post, py_bound_graph ) < 0 ) {
        return NULL;
      }
    }
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RemovePlugin( PyVGX_System *py_system, PyObject *py_name ) {
  const char *name = PyUnicode_AsUTF8( py_name );
  if( name == NULL ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  if( __pyvgx_plugin__remove( name ) < 0 ) {
    return NULL;
  }

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__GetPlugins( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "plugin", NULL };

  const char *plugin = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s", kwlist, &plugin ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  return __pyvgx_plugin__get_plugins( true, plugin );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__GetBuiltins( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "plugin", NULL };
  
  const char *plugin = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s", kwlist, &plugin ) ) {
    return NULL;
  }

  if( !igraphfactory.IsInitialized() ) {
    PyVGXError_SetString( PyExc_Exception, "No registry (system not initialized?)" );
    return NULL;
  }

  return __pyvgx_plugin__get_plugins( false, plugin );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RequestHTTP( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "address", "path", "query", "content", "headers", "timeout", NULL }; 
  PyObject *py_address = NULL;
  const char *path = "/";
  PyObject *py_query = NULL;
  const char *content = NULL;
  int64_t sz_content = 0;
  PyObject *py_headers = NULL;
  int timeout_ms = 5000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|sOs#Oi", kwlist, &py_address, &path, &py_query, &content, &sz_content, &py_headers, &timeout_ms ) ) {
    return NULL;
  }

  const char *host = NULL;
  int port = 0;

  if( PyTuple_Check( py_address ) && PyTuple_Size( py_address ) == 2 ) {
    PyObject *py_host = PyTuple_GetItem( py_address, 0 );
    PyObject *py_port = PyTuple_GetItem( py_address, 1 );
    if( (host = PyUnicode_AsUTF8( py_host )) != NULL ) {
      port = PyLong_AsLong( py_port );
    }
  }

  if( host == NULL || port <= 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_ValueError, "address must be tuple (host, port)" );
    }
    return NULL;
  }

  // Validate port
  uint16_t uport = (uint16_t)port;
  if( uport != port ) {
    PyErr_SetString( PyExc_ValueError, "invalid port number" );
    return NULL;
  }

  // Validate host
  const char *hp = host;
  char c;
  while( (c = *hp++) != '\0' ) {
    if( !isalpha(c) && !isdigit(c) && c != '-' && c != '.' && hp-host > 253 ) {
      PyErr_SetString( PyExc_ValueError, "invalid host name" );
      return NULL;
    }
  }

#define __max_query 1024
  char query[ __max_query ];
  char encoding_buffer[ __max_query ];
  query[ 0 ] = 0;
  query[ __max_query - 1 ] = 0;
  encoding_buffer[ __max_query - 1 ] = 0;
  int max_percent_encoded = __max_query / 3;
  PyObject *py_iter = NULL;

  if( py_query ) {
    // String (URI-encoded implied)
    if( PyUnicode_Check( py_query ) ) {
      Py_ssize_t qsz = 0;
      const char *q = PyUnicode_AsUTF8AndSize( py_query, &qsz );
      if( qsz >= __max_query ) {
        PyErr_SetString( PyExc_ValueError, "query too long" );
        return NULL;
      }
      memcpy( query, q, qsz );
      query[ qsz ] = 0;
    }
    // List of strings or key,val tuples
    else if( (py_iter = PyObject_GetIter( py_query )) != NULL ) {
      bool is_dict = PyDict_Check( py_query );
      char *p = query;
      *p = '\0';
      int64_t remain = __max_query - 1;
      PyObject *py_param = NULL;
      int err = 0;
      while( (py_param = PyIter_Next( py_iter )) != NULL ) {

        if( is_dict || (PyTuple_Check( py_param ) && PyTuple_Size( py_param ) == 2) ) {
          PyObject *py_key;
          PyObject *py_val;
          if( is_dict ) {
            py_key = py_param;
            py_val = PyDict_GetItem( py_query, py_key );
          }
          else {
            py_key = PyTuple_GetItem( py_param, 0 );
            py_val = PyTuple_GetItem( py_param, 1 );
          }

          if( !PyUnicode_Check( py_key ) || !PyUnicode_Check( py_val ) ) {
            PyErr_SetString( PyExc_ValueError, "parameter key and val must be strings" );
            err = -1;
            break;
          }
          Py_ssize_t sz_val = 0;
          const char *sval = PyUnicode_AsUTF8AndSize( py_val, &sz_val );
          if( sz_val > max_percent_encoded ) { // 2048 / 3
            PyErr_SetString( PyExc_ValueError, "parameter value too long" );
            err = -1;
            break;
          }
           
          int64_t sz_encoded = encode_percent_plus( sval, sz_val, encoding_buffer );
          Py_ssize_t sz_key = 0;
          const char *key = PyUnicode_AsUTF8AndSize( py_key, &sz_key );
          int64_t sz_keyval = sz_key + sz_encoded + 2; // key=val&
          if( sz_keyval > remain ) {
            PyErr_SetString( PyExc_ValueError, "query too long" );
            err = -1;
            break;
          }
          memcpy( p, key, sz_key );
          p += sz_key;
          *p++ = '=';
          memcpy( p, encoding_buffer, sz_encoded );
          p += sz_encoded;
          *p++ = '&';
          *p = '\0';
          remain -= sz_keyval;
        }
        else if( PyUnicode_Check( py_param ) ) {
          Py_ssize_t sz_param = 0;
          const char *param = PyUnicode_AsUTF8AndSize( py_param, &sz_param );
          if( sz_param + 1 > remain ) {
            PyErr_SetString( PyExc_ValueError, "query too long" );
            err = -1;
            break;
          }
          memcpy( p, param, sz_param );
          p += sz_param;
          *p++ = '&';
          *p = '\0';
          remain -= (sz_param + 1);
        }
        else {
          PyErr_SetString( PyExc_ValueError, "parameter must be string or 2-tuple" );
          err = -1;
          break;
        }

        Py_DECREF( py_param );
      }

      Py_XDECREF( py_param );

      if( err < 0 ) {
        return NULL;
      }

      // erase trailing &
      if( p > query && *(p-1) == '&' ) {
        *(--p) = '\0';
      }

      Py_DECREF( py_iter );
    }
    else {
      PyErr_SetString( PyExc_TypeError, "string, list or tuple required" );
      return NULL;
    }
  }


  vgx_VGXServerRequest_t *request = NULL;
  if( sz_content == 0 )  {
    request = iVGXServer.Request.New( HTTP_GET, NULL );
  }
  else {
    request = iVGXServer.Request.New( HTTP_POST, NULL );
  }

  if( request == NULL ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }

  request->accept_type = MEDIA_TYPE__application_json;

  if( py_headers ) {
    if( !PyDict_Check( py_headers ) ) {
      PyErr_SetString( PyExc_TypeError, "headers must be dict" );
      return NULL;
    }

    int err = 0;
    Py_ssize_t pos = 0;
    PyObject *py_key;
    PyObject *py_val;
    // Iterate over all items in the dict
    while( PyDict_Next( py_headers, &pos, &py_key, &py_val ) ) {
      if( PyUnicode_Check( py_key ) && PyUnicode_Check( py_val ) ) {
        if( iVGXServer.Request.AddHeader( request, PyUnicode_AsUTF8( py_key ), PyUnicode_AsUTF8( py_val ) ) < 0 ) {
          if( !PyErr_Occurred() ) {
            PyErr_SetString( PyExc_Exception, "internal error" );
            err = -1;
            break;
          }
        }
      }
      else {
        PyErr_SetString( PyExc_TypeError, "header entries must be strings" );
        err = -1;
        break;
      }
    }
    if( err < 0 ) {
      iVGXServer.Request.Delete( &request );
      return NULL;
    }
  }

  PyObject *py_response = NULL;
  const char *segment = NULL;
  int64_t sz_segment = 0;
  vgx_VGXServerResponse_t response = {0};

  CString_t *CSTR__error = NULL;

  BEGIN_PYVGX_THREADS {
    vgx_URI_t *URI = NULL;

    XTRY {

      // Add content if we have any
      if( request->method == HTTP_POST ) {
        if( iVGXServer.Request.AddContent( request, content, sz_content ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
        }
      }

      // Create URI
      if( (URI = iURI.NewElements( "http", NULL, host, uport, path, query, NULL, &CSTR__error )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x008 );
      }

      // Connect
      if( iURI.Connect( URI, timeout_ms, &CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_API, 0x009 );
      }

      // Send request
      if( iVGXServer.Util.SendAll( URI, request, timeout_ms ) < 0 ) {
        __set_error_string( &CSTR__error, "failed to send request" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
      }

      // Get response
      if( iVGXServer.Util.ReceiveAll( URI, &response, timeout_ms ) < 0 ) {
        __set_error_string( &CSTR__error, "failed to receive response" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00B );
      }

      if( response.status.code != HTTP_STATUS__OK ) {
        __format_error_string( &CSTR__error, "Response code: %03d ", response.status.code );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00C );
      }

      if( response.buffers.content == NULL ) {
        __set_error_string( &CSTR__error, "internal response error" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00D );
      }

      // Assume single segment
      sz_segment = iStreamBuffer.ReadableSegment( response.buffers.content, response.content_length, &segment, NULL );
      if( sz_segment != response.content_length ) {
        __set_error_string( &CSTR__error, "internal response error" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x00E );
      }
    }
    XCATCH( errcode ) {
    }
    XFINALLY {
      iVGXServer.Request.Delete( &request );
      iURI.Delete( &URI ); 
    }
  } END_PYVGX_THREADS;

  if( segment ) {
    py_response = PyUnicode_FromStringAndSize( segment, sz_segment );
  }
  iStreamBuffer.Delete( &response.buffers.content );

  // handle error 
  if( CSTR__error ) {
    CString_t *CSTR__prefix = CALLABLE( CSTR__error )->Prefix( CSTR__error, 256 );
    if( CSTR__prefix ) {
      CString_t *CSTR__tmp = CSTR__error;
      CSTR__error = CSTR__prefix;
      iString.Discard( &CSTR__tmp );
    }
    PyErr_SetString( PyExc_Exception, CStringValue( CSTR__error ) );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  if( py_response == NULL && !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
  }

  return py_response;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __get_current_log_names( const char *path, char *systemlog, char *accesslog, int maxpath, int rotate_interval_seconds ) {
  uint32_t seconds_since_epoch = __SECONDS_SINCE_1970();
  time_t seconds_current_interval = seconds_since_epoch - seconds_since_epoch % rotate_interval_seconds;
  struct tm *current_hour = localtime( &seconds_current_interval );
  char tbuf[32] = {0};
  if( current_hour ) {
    strftime( tbuf, 31, "%Y-%m-%d-%H%M%S", current_hour );
    snprintf( systemlog, maxpath, "%s/vgx.%s", path, tbuf );
    snprintf( accesslog, maxpath, "%s/access.%s", path, tbuf );
  }
  else {
    snprintf( systemlog, maxpath, "%s/vgx.%lld", path, seconds_current_interval );
    snprintf( accesslog, maxpath, "%s/access.%lld", path, seconds_current_interval );
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __delete_old_log_files( const char *path, int64_t max_age_seconds ) {
  // Delete old system logs
  delete_matching_files( path, "vgx.*", max_age_seconds );
  // Delete old access logs
  delete_matching_files( path, "access.*", max_age_seconds );
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __rotate_logs( const char *logpath, int rotate_interval_seconds ) {
  int ret = 0;
  int cleanup_cutoff = rotate_interval_seconds * 24 * 30; // 720 log rotations = 1 month if rotation is hourly
  CString_t *CSTR__error = NULL;
  char _systemlog[MAX_PATH+1] = {0};
  char _accesslog[MAX_PATH+1] = {0};
  const char *systemlog = NULL;
  const char *accesslog = NULL;
  XTRY {

    // Get full name of output stream file
    if( logpath ) {
      if( !dir_exists( logpath ) ) {
        if( create_dirs( logpath ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
        }
      }
      __get_current_log_names( logpath, _systemlog, _accesslog, MAX_PATH, rotate_interval_seconds );
      systemlog = _systemlog;
      accesslog = _accesslog;

      __delete_old_log_files( logpath, cleanup_cutoff );
    }

    // Set the output stream (or close if NULL)
    if( __pyvgx_set_output_stream( systemlog, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Open the accesslog (or close if NULL)
    if( __pyvgx_open_access_log( accesslog, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    PYVGX_API_INFO( "system", 0, "==================== NEW LOG ====================" );
  }
  XCATCH( errcode ) {
    ret = -1;
    if( logpath ) {
      __rotate_logs( NULL, rotate_interval_seconds );
    }
    PYVGX_API_REASON( "system", 0, "Log rotate failed: %s", CStringValueDefault( CSTR__error, "unknown" ) );
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __execute_watchdog( PyObject *py_watchdog ) {
  BEGIN_PYTHON_INTERPRETER {
    PyObject *py_wdret = PyObject_Call( py_watchdog, g_py_noargs, NULL );
    if( py_wdret ) {
      Py_DECREF( py_wdret );
    }
    else if( PyErr_Occurred() ) {
      CString_t *CSTR__error = NULL;
      iPyVGXBuilder.CatchPyExceptionIntoOutput( NULL, NULL, &CSTR__error, NULL );
      REASON( 0x002, "Server watchdog error: %s", CStringValueDefault( CSTR__error, "unknown" ) );
      iString.Discard( &CSTR__error );
    }
  } END_PYTHON_INTERPRETER;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __run_until_signal( const char *logpath, int rotate_interval_seconds, PyObject *py_watchdog, int watchdog_interval_ms ) {

  int signal = 0;

  int rotate_interval_ms = rotate_interval_seconds * 1000;

  int64_t now = __MILLISECONDS_SINCE_1970();
  int64_t next_rotate_deadline_ms = now + rotate_interval_ms - (now % rotate_interval_ms);
  int64_t next_watchdog_deadline_ms = now + watchdog_interval_ms;

  do {
    // Go to sleep
    BEGIN_PYVGX_THREADS {
      sleep_milliseconds( 333 );

      // Current time
      now = __MILLISECONDS_SINCE_1970();

      // Time to rotate logs ?
      if( logpath && now > next_rotate_deadline_ms ) {
        if( __rotate_logs( logpath, rotate_interval_seconds ) < 0 ) {
          REASON( 0x001, "Log rotation failed" );
        }
        next_rotate_deadline_ms += rotate_interval_ms;
      }

      // Run watchdog once
      if( py_watchdog && now > next_watchdog_deadline_ms ) {
        // Bump the next deadline by one interval
        next_watchdog_deadline_ms += watchdog_interval_ms;

        // Execute watchdog
        __execute_watchdog( py_watchdog );

        // Prevent non-stop watchdog in case interval is set too short
        if( (now = __MILLISECONDS_SINCE_1970()) > next_watchdog_deadline_ms ) {
          next_watchdog_deadline_ms = now + watchdog_interval_ms;
        }
      }
    } END_PYVGX_THREADS;

    // Check signals
    if( !signal ) {
      BEGIN_PYTHON_INTERPRETER {
        signal = PyErr_CheckSignals();
      } END_PYTHON_INTERPRETER;
      if( signal ) {
        PYVGX_API_INFO( "system", 0, "Exit signal received" );
        PYVGX_API_INFO( "system", 0, "Redirecting logs to: stdout" );
        __rotate_logs( NULL, rotate_interval_seconds );
      }
    }
  } while( !signal );
  
  return signal;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RunServer( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "name", "watchdog", "interval", "logpath", NULL }; 
  const char *service_name = NULL;
  int64_t sz_service_name = 0;
  PyObject *py_watchdog = NULL;
  int rotate_interval_seconds = 3600; // Rotate logs every hour
  int watchdog_interval_ms = 5000;
  const char *logpath = NULL;
  int64_t sz_logpath = 0;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|z#Oiz#", kwlist, &service_name, &sz_service_name, &py_watchdog, &watchdog_interval_ms, &logpath, &sz_logpath ) ) {
    return NULL;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    PyErr_SetString( PyExc_Exception, "No system graph (system not initialized?)" );
    return NULL;
  }

  if( watchdog_interval_ms < 1000 ) {
    PyErr_SetString( PyExc_ValueError, "interval must be at least 1000 milliseconds" );
    return NULL;
  }

  if( service_name ) {
    PYVGX_API_INFO( "system", 0, "Enter RunServer( name=\"%s\" )", service_name );
  }
  else {
    PYVGX_API_INFO( "system", 0, "Enter RunServer()" );
  }

  if( py_watchdog && py_watchdog != Py_None ) {
    if( !PyFunction_Check( py_watchdog ) ) {
      PyObject *py_repr = PyObject_Repr( (PyObject*)py_watchdog->ob_type );
      if( py_repr ) {
        PyErr_Format( PyExc_TypeError, "watchdog must be a function, not %s", PyUnicode_AsUTF8( py_repr ) );
        Py_DECREF( py_repr );
      }
      return NULL;
    }

    // Get function's full argspec 
    PyObject *py_spec = __pyvgx_plugin__get_argspec( py_watchdog );
    if( !py_spec ) {
      return NULL;
    }

    PyObject *py_args_list = PyTuple_GetItem( py_spec, 0 );
    if( PyList_Size( py_args_list ) != 0 ) {
      Py_DECREF( py_spec );
      PyErr_SetString( PyExc_TypeError, "Invalid watchdog signature. Watchdog function must take exactly zero arguments." );
      return NULL;
    }
    Py_DECREF( py_spec );

    const char *name = PyUnicode_AsUTF8( ((PyFunctionObject*)py_watchdog)->func_name );
    PYVGX_API_INFO( "system", 0, "Running watchdog '%s()' at %d ms interval", name, watchdog_interval_ms );
  }

  if( logpath ) {
    if( !dir_exists( logpath ) ) {
      if( create_dirs( logpath ) < 0 ) {
        PyErr_Format( PyExc_TypeError, "Invalid log path: %s", logpath );
        return NULL;
      }
    }
    // Initial file
    PYVGX_API_INFO( "system", 0, "Redirecting logs to: %s", logpath );
    if( __rotate_logs( logpath, rotate_interval_seconds ) < 0 ) {
      if( !PyErr_Occurred() ) {
        PyErr_SetString( PyExc_Exception, "Failed to initialize output stream" );
        return NULL;
      }
    }
  }

  // Try to get instance ID if set
  char identbuf[32] = {0};
  PyObject *py_ident_key = PyUnicode_FromString( "SYSTEM_DescriptorIdent" );
  if( py_ident_key ) {
    PyObject *py_ident = __system_property( py_system, py_ident_key, NULL, false, false, 5000 );
    if( py_ident ) {
      const char *ident = PyUnicode_AsUTF8( py_ident );
      strncpy( identbuf, ident, 31 );
      Py_DECREF( py_ident );
    }
    else {
      PyErr_Clear();
    }
    Py_DECREF( py_ident_key );
  }
  else {
    PyErr_Clear();
  }
  
  // Set service name
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      char namebuf[64] = {0};
      snprintf( namebuf, 63, "%s: %s", identbuf, (service_name ? service_name : "") );
      iVGXServer.Service.SetName( SYSTEM, namebuf );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  // Run until signal
  __run_until_signal( logpath, rotate_interval_seconds, py_watchdog, watchdog_interval_ms );

  // Clear service name
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( SYSTEM ) {
      iVGXServer.Service.SetName( SYSTEM, NULL );
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;

  PYVGX_API_INFO( "system", 0, "Exit RunServer()" );

  PyErr_Clear();

  Py_RETURN_NONE;
}

 

/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__SetProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "key", "value", "timeout", NULL };
  PyObject *py_key = NULL;
  PyObject *py_value = NULL;
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "OO|i", kwlist, &py_key, &py_value, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, py_key, py_value, false, false, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__GetProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "key", "default", "timeout", NULL };
  PyObject *py_key = NULL;
  PyObject *py_default = NULL;
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|Oi", kwlist, &py_key, &py_default, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, py_key, NULL, false, false, timeout_ms );
  if( py_ret == NULL && py_default != NULL && PyErr_Occurred() && PyErr_ExceptionMatches( PyExc_LookupError ) ) {
    PyErr_Clear();
    Py_INCREF( py_default );
    py_ret = py_default;
  }

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__HasProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "key", "timeout", NULL };
  PyObject *py_key = NULL;
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|i", kwlist, &py_key, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, py_key, NULL, true, false, timeout_ms );
  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RemoveProperty( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "key", "timeout", NULL };
  PyObject *py_key = NULL;
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|i", kwlist, &py_key, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, py_key, NULL, false, true, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__SetProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "properties", "timeout", NULL };
  PyObject *py_prop_dict = NULL;
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "O|i", kwlist, &py_prop_dict, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, NULL, py_prop_dict, false, false, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__GetProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "timeout", NULL };
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, NULL, NULL, false, false, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__NumProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "timeout", NULL };
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, NULL, NULL, true, false, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__RemoveProperties( PyVGX_System *py_system, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "timeout", NULL };
  int timeout_ms = 1000;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  PyObject *py_ret = __system_property( py_system, NULL, NULL, false, true, timeout_ms );

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __system_contains( PyVGX_System *py_system, PyObject *py_key ) {
  int ret = 0;
  PyVGX_Vertex *py_prop = __system_property_vertex( 1000 );
  if( py_prop ) {
    ret = pyvgx__vertex_contains( py_prop, py_key );
    Py_DECREF( py_prop );
  }
  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __system_keys_and_values( bool keys, bool values ) {
  PyObject *py_ret = NULL;
  PyVGX_Vertex *py_prop = __system_property_vertex( 1000 );
  if( py_prop ) {
    py_ret = pyvgx__vertex_keys_and_values( py_prop, keys, values );
    Py_DECREF( py_prop );
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_System__items
 *
 ******************************************************************************
 */
PyDoc_STRVAR( items__doc__,
  "items() -> [(key,val), ...]\n"
);

/**************************************************************************//**
 * PyVGX_System__items
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__items( PyVGX_System *py_system ) {
  return __system_keys_and_values( true, true );
}



/******************************************************************************
 * PyVGX_System__keys
 *
 ******************************************************************************
 */
PyDoc_STRVAR( keys__doc__,
  "keys() -> [key1, key2, ...]\n"
);

/**************************************************************************//**
 * PyVGX_System__keys
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__keys( PyVGX_System *py_system ) {
  return __system_keys_and_values( true, false );
}



/******************************************************************************
 * PyVGX_System__values
 *
 ******************************************************************************
 */
PyDoc_STRVAR( values__doc__,
  "values() -> [val1, val2, ...]\n"
);

/**************************************************************************//**
 * PyVGX_System__values
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__values( PyVGX_System *py_system ) {
  return __system_keys_and_values( false, true );
}



/******************************************************************************
 * PyVGX_System__Meminfo
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_System__Meminfo( PyObject *self ) {
  PyObject *py_ret = PyTuple_New( 2 );
  int64_t g_phys = 0;
  int64_t proc_phys = 0;
  BEGIN_PYVGX_THREADS {
    get_system_physical_memory( &g_phys, NULL, &proc_phys );
  } END_PYVGX_THREADS;
  if( py_ret ) {
    PyTuple_SetItem( py_ret, 0, PyLong_FromLongLong( g_phys ) );
    PyTuple_SetItem( py_ret, 1, PyLong_FromLongLong( proc_phys ) );
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_System__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_System__members[] = {
  {NULL}  /* Sentinel */
};



/**************************************************************************//**
 * __PyVGX_System__attr
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_System__attr( PyVGX_System *py_system, void *closure ) {
  return PyLong_FromLongLong( 0 );
}



/******************************************************************************
 * PyVGX_System__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_System__getset[] = {
  {"root",       (getter)__PyVGX_System__root,     (setter)NULL,   "vgxroot",    NULL  },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PySequenceMethods tp_as_sequence_PyVGX_System = {
    .sq_length          = 0,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)__system_contains,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0 
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyMappingMethods tp_as_mapping_PyVGX_System = {
    .mp_length          = (lenfunc)PyVGX_System__len,
    .mp_subscript       = (binaryfunc)PyVGX_System__get_item,
    .mp_ass_subscript   = (objobjargproc)PyVGX_System__set_item
};



/******************************************************************************
 * PyVGX_System__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_System__methods[] = {
  { "IsInitialized",     (PyCFunction)PyVGX_System__IsInitialized,      METH_NOARGS,                    "IsInitialized() -> bool"     },
  { "Initialize",        (PyCFunction)PyVGX_System__Initialize,         METH_VARARGS | METH_KEYWORDS,   "Initialize( vgxroot=\".\", http=0, attach=None, bind=0, durable=False, events=True, idle=False, readonly=False, euclidean=True ) -> None" },
  { "Unload",            (PyCFunction)PyVGX_System__Unload,             METH_VARARGS | METH_KEYWORDS,   "Unload( persist=False ) -> None" },
  { "Registry",          (PyCFunction)PyVGX_System__Registry,           METH_NOARGS,                    "Registry() -> dict"  },
  { "Persist",           (PyCFunction)PyVGX_System__Persist,            METH_VARARGS | METH_KEYWORDS,   "Persist( timeout=1000, force=False, remote=False ) -> None"  },
  { "Sync",              (PyCFunction)PyVGX_System__Sync,               METH_VARARGS | METH_KEYWORDS,   "Sync( hard=False, timeout=30000 ) -> None"  },
  { "CancelSync",        (PyCFunction)PyVGX_System__CancelSync,         METH_VARARGS | METH_KEYWORDS,   "CancelSync() -> int" },
  { "GetGraph",          (PyCFunction)PyVGX_System__GetGraph,           METH_O,                         "GetGraph( name ) -> graph_object" },
  { "DeleteGraph",       (PyCFunction)PyVGX_System__DeleteGraph,        METH_VARARGS | METH_KEYWORDS,   "DeleteGraph( name, path=name, timeout=0 ) -> None" },

  { "SuspendEvents",     (PyCFunction)PyVGX_System__SuspendEvents,      METH_NOARGS,                    "SuspendEvents() -> None"  },
  { "EventsResumable",   (PyCFunction)PyVGX_System__EventsResumable,    METH_NOARGS,                    "EventsResumable() -> bool"  },
  { "ResumeEvents",      (PyCFunction)PyVGX_System__ResumeEvents,       METH_NOARGS,                    "ResumeEvents() -> None"  },
  { "SetReadonly",       (PyCFunction)PyVGX_System__SetReadonly,        METH_NOARGS,                    "SetReadonly() -> None"  },
  { "CountReadonly",     (PyCFunction)PyVGX_System__CountReadonly,      METH_NOARGS,                    "CountReadonly() -> num_readonly_graphs"  },
  { "ClearReadonly",     (PyCFunction)PyVGX_System__ClearReadonly,      METH_NOARGS,                    "ClearReadonly() -> None"  },

  { "Root",              (PyCFunction)PyVGX_System__Root,               METH_NOARGS,                    "Root() -> vgxroot string" },
  { "System",            (PyCFunction)PyVGX_System__System,             METH_NOARGS,                    "System() -> system graph" },
  { "Status",            (PyCFunction)PyVGX_System__Status,             METH_VARARGS | METH_KEYWORDS,   "Status( [graph[, simple]] ) -> dict" },
  { "Fingerprint",       (PyCFunction)PyVGX_System__Fingerprint,        METH_NOARGS,                    "Fingerprint() -> str" },
  { "UniqueLabel",       (PyCFunction)PyVGX_System__UniqueLabel,        METH_NOARGS,                    "UniqueLabel() -> str" },
  { "DurabilityPoint",   (PyCFunction)PyVGX_System__DurabilityPoint,    METH_NOARGS,                    "DurabilityPoint() -> (transaction, serial, timestamp)" },
  { "WritableVertices",  (PyCFunction)PyVGX_System__WritableVertices,   METH_NOARGS,                    "WritableVertices() -> int" },

  { "StartHTTP",         (PyCFunction)PyVGX_System__StartHTTP,          METH_VARARGS | METH_KEYWORDS,   "StartHTTP( port[, ip[, prefix[, servicein[, dispatcher]]]] ) -> None" },
  { "StopHTTP",          (PyCFunction)PyVGX_System__StopHTTP,           METH_VARARGS | METH_KEYWORDS,   "StopHTTP() -> None" },
  { "RestartHTTP",       (PyCFunction)PyVGX_System__RestartHTTP,        METH_NOARGS,                    "RestartHTTP() -> None" },
  { "DispatcherConfig",  (PyCFunction)PyVGX_System__DispatcherConfig,   METH_NOARGS,                    "DispatcherConfig() -> dict_or_None" },
  { "ServiceInHTTP",     (PyCFunction)PyVGX_System__ServiceInHTTP,      METH_VARARGS | METH_KEYWORDS,   "ServiceInHTTP( service_in ) -> None" },
  { "ServerMetrics",     (PyCFunction)PyVGX_System__ServerMetrics,      METH_VARARGS | METH_KEYWORDS,   "ServerMetrics( percentiles ) -> dict" },
  { "ServerPorts",       (PyCFunction)PyVGX_System__ServerPorts,        METH_NOARGS,                    "ServerPorts() -> dict" },
  { "ServerHost",        (PyCFunction)PyVGX_System__ServerHost,         METH_NOARGS,                    "ServerHost() -> dict" },
  { "ServerPrefix",      (PyCFunction)PyVGX_System__ServerPrefix,       METH_NOARGS,                    "ServerPrefix() -> str_or_None" },
  { "ServerAdminIP",     (PyCFunction)PyVGX_System__ServerAdminIP,      METH_NOARGS,                    "ServerAdminIP() -> str_or_None" },
  { "RequestRate",       (PyCFunction)PyVGX_System__RequestRate,        METH_NOARGS,                    "RequestRate() -> float" },
  { "ResetMetrics",      (PyCFunction)PyVGX_System__ResetMetrics,       METH_NOARGS,                    "ResetMetrics() -> None" },
  { "AddPlugin",         (PyCFunction)PyVGX_System__AddPlugin,          METH_VARARGS | METH_KEYWORDS,   "AddPlugin( plugin, name, graph, engine, pre, post ) -> None" },
  { "RemovePlugin",      (PyCFunction)PyVGX_System__RemovePlugin,       METH_O,                         "RemovePlugin( name ) -> None" },
  { "GetPlugins",        (PyCFunction)PyVGX_System__GetPlugins,         METH_VARARGS | METH_KEYWORDS,   "GetPlugins() -> dict" },
  { "GetBuiltins",       (PyCFunction)PyVGX_System__GetBuiltins,        METH_VARARGS | METH_KEYWORDS,   "GetBuiltins() -> dict" },

  { "RequestHTTP",       (PyCFunction)PyVGX_System__RequestHTTP,        METH_VARARGS | METH_KEYWORDS,   "RequestHTTP( address, path, query, content, headers, timeout )" },

  { "RunServer",         (PyCFunction)PyVGX_System__RunServer,          METH_VARARGS | METH_KEYWORDS,   "RunServer( name=None, watchdog=None, interval=5000, logpath=None ) -> None" },

  { "SetProperty",       (PyCFunction)PyVGX_System__SetProperty,        METH_VARARGS | METH_KEYWORDS,   "SetProperty( key, value[, timeout] ) -> None" },
  { "GetProperty",       (PyCFunction)PyVGX_System__GetProperty,        METH_VARARGS | METH_KEYWORDS,   "GetProperty( key[, default[, timeout]] ) -> property" },
  { "HasProperty",       (PyCFunction)PyVGX_System__HasProperty,        METH_VARARGS | METH_KEYWORDS,   "HasProperty( key[, timeout] ) -> bool" },
  { "RemoveProperty",    (PyCFunction)PyVGX_System__RemoveProperty,     METH_VARARGS | METH_KEYWORDS,   "RemoveProperty( key[, timeout] ) -> None" },
  { "SetProperties",     (PyCFunction)PyVGX_System__SetProperties,      METH_VARARGS | METH_KEYWORDS,   "SetProperties( properties[, timeout] ) -> None" },
  { "GetProperties",     (PyCFunction)PyVGX_System__GetProperties,      METH_VARARGS | METH_KEYWORDS,   "GetProperties( [timeout] ) -> dict" },
  { "NumProperties",     (PyCFunction)PyVGX_System__NumProperties,      METH_VARARGS | METH_KEYWORDS,   "NumProperties( [timeout] ) -> number" },
  { "RemoveProperties",  (PyCFunction)PyVGX_System__RemoveProperties,   METH_VARARGS | METH_KEYWORDS,   "RemoveProperties( [timeout] ) -> number" },

  { "items",             (PyCFunction)PyVGX_System__items,              METH_NOARGS,                    "items() -> [(key,val), ...]" },
  { "keys",              (PyCFunction)PyVGX_System__keys,               METH_NOARGS,                    "keys() -> [key1, key2, ...]" },
  { "values",            (PyCFunction)PyVGX_System__values,             METH_NOARGS,                    "values() -> [val1, val2, ...]" },

  { "Meminfo",           (PyCFunction)PyVGX_System__Meminfo,            METH_NOARGS,                    "Meminfo() -> (total, process) " },

  {NULL}  /* Sentinel */
};
RESUME_WARNINGS


/******************************************************************************
 * PyVGX_System__SystemType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_System__SystemType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.System",
    .tp_basicsize       = sizeof(PyVGX_System),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_System__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_System__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &tp_as_sequence_PyVGX_System,
    .tp_as_mapping      = &tp_as_mapping_PyVGX_System,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX System objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_System__methods,
    .tp_members         = PyVGX_System__members,
    .tp_getset          = PyVGX_System__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_System__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_System__new,
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


DLL_HIDDEN PyTypeObject * p_PyVGX_System__SystemType = &PyVGX_System__SystemType;
