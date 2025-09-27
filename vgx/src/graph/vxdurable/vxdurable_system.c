/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_system.c
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

#include "_vxoperation.h"
#include "_vxenum.h"
#include "_vxarcvector.h"
#include "versiongen.h"
#include <signal.h>


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static vgx_Graph_t *g_SYSTEM = NULL;
static objectid_t g_SYSTEM_ID = {0};
static bool g_SYSTEM_READY = false;

static objectid_t g_SYSTEM_UNIQUE_IDENTIFIER = {0};

static bool g_SYSTEM_ATTACHED = false;

static const char * g_system_vertex_type_name = VGX_SYSTEM_TYPE_PREFIX;
static const char * g_system_root_vertex_name = VGX_SYSTEM_VERTEX_PREFIX "root";
static const char * g_system_property_vertex_name = VGX_SYSTEM_VERTEX_PREFIX "prop";
static const char * g_system_instance_vertex_fmt = VGX_SYSTEM_VERTEX_PREFIX "instance_%016llx";

static objectid_t g_system_property_vertex_obid = {0};


static vgx_Vertex_t * __open_system_vertex( vgx_Graph_t *graph, CString_t **CSTR__name, CString_t **CSTR__type );

static vgx_TransactionalProducer_t * __add_and_connect_producer_SYS_CS( vgx_Graph_t *sys, vgx_URI_t *URI, vgx_TransactionalProducerAttachMode mode, bool handshake, CString_t **CSTR__error );
static bool           __ensure_graph_emitter_CS( vgx_Graph_t *graph, CString_t **CSTR__error );
static int            __stop_graph_emitter_CS( vgx_Graph_t *graph );
static bool           __producer_connected_SYS_CS( vgx_Graph_t *sys, vgx_TransactionalProducer_t *target );




static bool          System__ready( void );
static int           System__create( void );
static int           System__destroy( void );
static int           System__attach_output( vgx_StringList_t *uri_strings, vgx_TransactionalProducerAttachMode mode, bool handshake, int timeout_ms, CString_t **CSTR__error );
static bool          System__is_attached( void );
static vgx_StringList_t * System__attached_outputs( vgx_StringList_t **descriptions, int timeout_ms );
static vgx_StringList_t * System__attached_outputs_simple( void );
static CString_t *   System__input_address( void );
static CString_t *   System__attached_input( void );
static int           System__detach_output( const vgx_StringList_t *detach_subscribers, bool remove_disconnected, bool force, int timeout_ms, CString_t **CSTR__error );
static bool          System__is_system_graph( vgx_Graph_t *graph );
static vgx_Graph_t * System__get_system_graph( void );
static int64_t       System__master_serial( void );
static vgx_Graph_t * System__get_graph( const objectid_t *obid );
static int           System__capture_create_graph( vgx_Graph_t *graph, vgx_TransactionalProducer_t *producer );
static int           System__capture_destroy_graph( const objectid_t *obid, const char *path );
static bool          System__begin_query( bool pri );
static void          System__end_query( bool pri, int64_t q_time_nanosec);
static int           System__get_query_pri_req( void );
static int64_t       System__query_count( void );
static int64_t       System__query_time_nanosec_acc( void );
static double        System__query_time_average( void );

static int           System__property_set_int( const char *key, int64_t value );
static int           System__property_get_int( const char *key, int64_t *rvalue );
static int           System__property_set_real( const char *key, double value );
static int           System__property_get_real( const char *key, double *rvalue );
static int           System__property_set_string( const char *key, const char *value );
static int           System__property_format_string( const char *key, const char *valfmt, ... );
static int           System__property_get_string( const char *key, CString_t **CSTR__rvalue );
static int           System__property_delete( const char *key );
static bool          System__property_exists( const char *key );

static const char *  System__get_system_vertex_type_name( void );
static const char *  System__get_system_root_vertex_name( void );
static const char *  System__get_system_property_vertex_name( void );




/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "SY::DUR(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXDURABLE_SYSTEM_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SYSTEM_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SYSTEM_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SYSTEM_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SYSTEM_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_SYSTEM_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )





DLL_EXPORT vgx_ISystem_t iSystem = {
  .Ready                  = System__ready,
  .Create                 = System__create,
  .Destroy                = System__destroy,
  .AttachOutput           = System__attach_output,
  .IsAttached             = System__is_attached,
  .AttachedOutputs        = System__attached_outputs,
  .AttachedOutputsSimple  = System__attached_outputs_simple,
  .InputAddress           = System__input_address,
  .AttachedInput          = System__attached_input,
  .DetachOutput           = System__detach_output,
  .IsSystemGraph          = System__is_system_graph,
  .GetSystemGraph         = System__get_system_graph,
  .MasterSerial           = System__master_serial,
  .GetGraph               = System__get_graph,
  .CaptureCreateGraph     = System__capture_create_graph,
  .CaptureDestroyGraph    = System__capture_destroy_graph,
  .BeginQuery             = System__begin_query,
  .EndQuery               = System__end_query,
  .GetQueryPriReq         = System__get_query_pri_req,
  .QueryCount             = System__query_count,
  .QueryTimeNanosecAcc    = System__query_time_nanosec_acc,
  .QueryTimeAverage       = System__query_time_average,
  .Property = {
    .SetInt               = System__property_set_int,
    .GetInt               = System__property_get_int,
    .SetReal              = System__property_set_real,
    .GetReal              = System__property_get_real,
    .SetString            = System__property_set_string,
    .FormatString         = System__property_format_string,
    .GetString            = System__property_get_string,
    .Delete               = System__property_delete,
    .Exists               = System__property_exists
  },
  .Name = {
    .VertexType           = System__get_system_vertex_type_name,
    .RootVertex           = System__get_system_root_vertex_name,
    .PropertyVertex       = System__get_system_property_vertex_name
  }
};




/*******************************************************************//**
 * NOTE: This function STEALS the CStrings!
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __open_system_vertex( vgx_Graph_t *graph, CString_t **CSTR__id, CString_t **CSTR__type ) {
  vgx_Vertex_t *vertex = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;
  XTRY {
    // Vertex id is required
    if( CSTR__id == NULL || *CSTR__id == NULL ) {
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_GENERAL, 0x001, "(SYSTEM) vertex id was NULL" );
    }
    objectid_t obid = *CStringObid( *CSTR__id );
    // Create
    if( CSTR__type != NULL  ) {
      if( *CSTR__type == NULL ) {
        THROW_CRITICAL_MESSAGE( CXLIB_ERR_GENERAL, 0x002, "(SYSTEM) vertex type was NULL" );
      }
      // New (or open) vertex
      GRAPH_LOCK( graph ) {
        vertex = CALLABLE( graph )->advanced->NewVertex_CS( graph, &obid, *CSTR__id, *CSTR__type, 1000, &reason, &CSTR__error );
      } GRAPH_RELEASE;
      if( vertex == NULL ) {
        THROW_CRITICAL( 0x003, CXLIB_ERR_GENERAL );
      }
    }
    // Open
    else {
      if( (vertex = CALLABLE( graph )->simple->OpenVertex( graph, *CSTR__id, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, 1000, &reason, &CSTR__error )) == NULL ) {
        THROW_CRITICAL( 0x004, CXLIB_ERR_GENERAL );
      }
    }

    // Open vertex
  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
      VXDURABLE_SYSTEM_CRITICAL( graph, 0x005, "Failed to open system vertex \"%s\": error=\"%s\", reason=%03x", CStringValue( *CSTR__id ), CStringValue( CSTR__error ), reason );
    }
  }
  XFINALLY {
    iString.Discard( CSTR__id );
    iString.Discard( CSTR__type );
    iString.Discard( &CSTR__error );
  }
  return vertex;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_TransactionalProducer_t * __add_and_connect_producer_SYS_CS( vgx_Graph_t *sys, vgx_URI_t *URI, vgx_TransactionalProducerAttachMode mode, bool handshake, CString_t **CSTR__error ) {

  vgx_OperationSystem_t *opsys = &sys->OP.system;
  vgx_TransactionalProducer_t *P = NULL;
  vgx_TransactionalProducer_t *producer = NULL;
  const char *uri = iURI.URI( URI );
  int added = 0;
  int connected = 0; 
  int64_t n_attached = 0;

  GRAPH_SUSPEND_LOCK( sys ) {
    XTRY {
      // Add producer for this URI
      // Create new producer
      if( (producer = iTransactional.Producer.New( opsys->graph, uri )) == NULL ) {
        __format_error_string( CSTR__error, "Failed create new producer for URI '%s'", uri );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Set the URI
      if( iTransactional.Producer.Attach( producer, URI, mode, handshake, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Verify URI set
      if( producer->URI == NULL ) {
        __format_error_string( CSTR__error, "Unexpected failure to set producer URI '%s'", uri );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Add the producer to list
      P = producer;
      // Steal producer (if added)
      if( (added = iTransactional.Producers.Add( opsys->producers, &producer )) < 0 ) {
        __format_error_string( CSTR__error, "Failed to add subscriber: %s", uri );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
      // New URI (was stolen)
      if( added > 0 ) {
        VXDURABLE_SYSTEM_VERBOSE( sys, 0x005, "Added producer for URI: %s", uri );
        // Connect producer
        TRANSACTIONAL_LOCK( P ) {
          connected = iTransactional.Producer.Connect( P, CSTR__error );
          // Clear producer's init state to allow deletion when no longer attached
          P->flags.init = false;
        } TRANSACTIONAL_RELEASE;
        if( connected < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }
      }
      // Existing URI
      else {
        __format_error_string( CSTR__error, "Subscriber already exists: %s", uri );
        P = NULL;
      }

    }
    XCATCH( errcode ) {
      if( added > 0 ) {
        iTransactional.Producers.Remove( opsys->producers, uri );
      }
      P = NULL;
    }
    XFINALLY {
      // Clean up any producer that was not added to list
      if( producer ) {
        iTransactional.Producer.Delete( &producer);
      }
      // Mark as attached if any producers exist
      n_attached = iTransactional.Producers.Length( opsys->producers );
    }
  } GRAPH_RESUME_LOCK;

  if( P && P->URI == NULL ) {
    __format_error_string( CSTR__error, "Producer unexpectedly detached after successful connect: %s", uri );
    return NULL;
  }

  g_SYSTEM_ATTACHED = n_attached > 0;
  return P;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool __ensure_graph_emitter_CS( vgx_Graph_t *graph, CString_t **CSTR__error ) {
  bool running = false;

  vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( graph, 30000 );
  vgx_ExecutionTimingBudget_t enable_budget = _vgx_get_graph_execution_timing_budget( graph, 30000 );

  GRAPH_LOCK( graph ) {
    const char *name = CALLABLE( graph )->FullPath( graph );
    
    // Suspend event processor while we transition to a running emitter
    int disabled_here = 0;
    GRAPH_SUSPEND_LOCK( graph ) {
      if( iGraphEvent.IsReady( graph ) && iGraphEvent.IsEnabled( graph ) ) {
        disabled_here = iGraphEvent.NOCS.Disable( graph, &disable_budget );
      }
    } GRAPH_RESUME_LOCK;

    if( disabled_here < 0 ) {
      __format_error_string( CSTR__error, "Failed to suspend event processor for graph %s", name );
    }
    else {

      // Start graph emitter (if not already started)
      if( iOperation.Emitter_CS.Start( graph ) < 0 ) {
        __format_error_string( CSTR__error, "Failed to start emitter for graph %s", name );
      }
      else {
        running = true;
      }
      
      // Re-enable event processor if suspended above
      if( disabled_here > 0 ) {
        GRAPH_SUSPEND_LOCK( graph ) {
          iGraphEvent.NOCS.Enable( graph, &enable_budget );
        } GRAPH_RESUME_LOCK;
      }
    }
  } GRAPH_RELEASE;

  // Graph emitter confirmed running and graph operation confirmed open.
  return running;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stop_graph_emitter_CS( vgx_Graph_t *graph ) {
  int stopped = 0;
  const char *name = CALLABLE( graph )->FullPath( graph );
  
  GRAPH_LOCK( graph ) {
    stopped = iOperation.Emitter_CS.Stop( graph );
  } GRAPH_RELEASE;

  if( stopped > 0 ) {
    VXDURABLE_SYSTEM_INFO( graph, 0x001, "Detach: Stopped operation emitter for graph %s", name );
  }
  else if( stopped == 0 ) {
    VXDURABLE_SYSTEM_VERBOSE( graph, 0x002, "Detach: Emitter for graph %s was not running", name );
  }
  else {
    VXDURABLE_SYSTEM_CRITICAL( graph, 0x003, "Detach: Failed to stop emitter for graph %s", name );
  }
  return stopped;
}



/*******************************************************************//**
 *
 * Returns true if any producer is connected or specified target producer is connected
 *
 ***********************************************************************
 */
static bool __producer_connected_SYS_CS( vgx_Graph_t *sys, vgx_TransactionalProducer_t *target ) {
  vgx_OperationSystem_t *opsys = &sys->OP.system;
  bool exists = false;
  vgx_TransactionalProducersIterator_t iter = {0};
  GRAPH_SUSPEND_LOCK( sys ) {
    if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
      vgx_TransactionalProducer_t *producer;
      while( !exists && (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
        if( producer == target || target == NULL ) {
          BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
            exists = iTransactional.Producer.Connected( producer );
          } END_TRANSACTIONAL_IF_ATTACHED;
        }
      }
      iTransactional.Producers.Iterator.Clear( &iter );
    }
  } GRAPH_RESUME_LOCK;

  return exists;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool System__ready( void ) {
  return g_SYSTEM_READY;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int System__create( void ) {
  CString_t *CSTR__system = NULL;
  CString_t *CSTR__vgx = NULL;
  CString_t *CSTR__error = NULL;
  CString_t *CSTR__datetime = NULL;

  CString_t *CSTR__vertex_id = NULL;
  CString_t *CSTR__vertex_type = NULL;

  object_allocator_context_t *alloc = NULL;

  vgx_Vertex_t *vgx_instance = NULL;
  vgx_Vertex_t *root = NULL;
  vgx_Vertex_t *prop = NULL;

  vgx_Relation_t *relation = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  int64_t tms_now = __MILLISECONDS_SINCE_1970();
  uint32_t ts_now = __SECONDS_SINCE_1970();
  vgx_Graph_t *sys = NULL;
  //
  vgx_Graph_vtable_t *igraph = (vgx_Graph_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Graph_t );

  static const int EXACTLY_TWELVE = 12;

  XTRY {
    if( (CSTR__system = CStringNew( VGX_SYSTEM_GRAPH_PATH )) == NULL ||
        (CSTR__vgx = CStringNew( VGX_SYSTEM_GRAPH_NAME )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

#if defined VGX_GRAPH_VERTEX_BLOCK_ORDER
    const int block_order = VGX_GRAPH_VERTEX_BLOCK_ORDER;
#else
    const int block_order = EXACTLY_TWELVE;
#endif

    // Make sure we can create a system graph
    vgx_Graph_constructor_args_t graph_args = {
      .CSTR__graph_path     = CSTR__system,
      .CSTR__graph_name     = CSTR__vgx,
      .vertex_block_order   = block_order,
      .graph_t0             = __SECONDS_SINCE_1970(),
      .start_opcount        = -1, // Opcount not known
      .simconfig            = NULL,
      .with_event_processor = true,
      .idle_event_processor = false,
      .force_readonly       = false,
      .force_writable       = true,
      .local_only           = false
    };
    if( (sys = COMLIB_OBJECT_NEW( vgx_Graph_t, NULL, &graph_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    alloc = sys->ephemeral_string_allocator_context;

    // -----------------------
    // SET GLOBAL SYSTEM GRAPH
    // -----------------------
    g_SYSTEM = sys;
    objectid_t *sysid = COMLIB_OBJECT_GETID( g_SYSTEM );
    idcpy( &g_SYSTEM_ID, sysid );

    // Initial master serial number is microseconds since 1970
    g_SYSTEM->sysmaster_tx_serial_0 = __MILLISECONDS_SINCE_1970() * 1000LL;
    g_SYSTEM->sysmaster_tx_serial_count = 0;
    g_SYSTEM->sysmaster_tx_serial = g_SYSTEM->sysmaster_tx_serial_0;

    // Inner plugin tracking resources
    if( iVGXServer.Resource.Plugin.Init() < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    //
    // Start agent
    //
    int a;
    GRAPH_LOCK( g_SYSTEM ) {
      a = iOperation.System_SYS_CS.StartAgent( g_SYSTEM );
    } GRAPH_RELEASE;
    if( a < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    //
    // Start parser
    //
    if( iOperation.Parser.Initialize( g_SYSTEM, &g_SYSTEM->OP.parser, true ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    //
    // Create validator
    //
    if( (g_SYSTEM->OP.system.validator = iTransactional.Validator.New( g_SYSTEM )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
    }

    //
    // Create current instance node
    //
    CSTR__vertex_id = CStringNewFormatAlloc( alloc, g_system_instance_vertex_fmt, tms_now );
    CSTR__vertex_type = CStringNewFormatAlloc( alloc, iSystem.Name.VertexType() );
    if( (vgx_instance = __open_system_vertex( sys, &CSTR__vertex_id, &CSTR__vertex_type )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }
    const char *vgx_instance_id = CALLABLE( vgx_instance )->IDString( vgx_instance );
    // Set start time property (int)
    if( CALLABLE( vgx_instance )->SetIntProperty( vgx_instance, "tms_t0", tms_now ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
    }
    // Set start time property (datetime string)
    if( (CSTR__datetime = igraphinfo.CTime( tms_now, true )) != NULL ) {
      if( CALLABLE( vgx_instance )->SetStringProperty( vgx_instance, "datetime_t0", CStringValue( CSTR__datetime ) ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
      }
      iString.Discard( &CSTR__datetime );
    }

    //
    // Create system root
    //
    CSTR__vertex_id = CStringNewFormatAlloc( alloc, iSystem.Name.RootVertex() );
    CSTR__vertex_type = CStringNewFormatAlloc( alloc, iSystem.Name.VertexType() );
    if( (root = __open_system_vertex( sys, &CSTR__vertex_id, &CSTR__vertex_type )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
    }
    const char *root_id = CALLABLE( root )->IDString( root );
    // Set name of current instance node
    if( CALLABLE( root )->SetStringProperty( root, "current_instance", vgx_instance_id ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
    }

    //
    // Connect SYSTEM -> vgx_instance_xxx
    //
    if( (relation = iRelation.New( sys, root_id, vgx_instance_id, "vgx_instance", VGX_PREDICATOR_MOD_TIME_CREATED, &ts_now )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00C );
    }
    if( igraph->simple->Connect( sys, relation, -1, NULL, 0, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00D );
    }
    iRelation.Delete( &relation );

    //
    // Create properties node
    //
    CSTR__vertex_id = CStringNewFormatAlloc( alloc, iSystem.Name.PropertyVertex() );
    CSTR__vertex_type = CStringNewFormatAlloc( alloc, iSystem.Name.VertexType() );
    if( (prop = __open_system_vertex( sys, &CSTR__vertex_id, &CSTR__vertex_type )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00E );
    }
    idcpy( &g_system_property_vertex_obid, __vertex_internalid( prop ) );

    //
    // Close
    //
    igraph->simple->CloseVertex( sys, &vgx_instance );
    igraph->simple->CloseVertex( sys, &root );
    igraph->simple->CloseVertex( sys, &prop );

    VXDURABLE_SYSTEM_VERBOSE( sys, 0x00F, "order=%lld size=%lld", GraphOrder( sys ), GraphSize( sys ) );

    //
    // Persist
    //
    if( igraph->BulkSerialize( sys, 30000, true, false, NULL, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x010 );
    }

    // OK!
    g_SYSTEM_READY = true;


  } 
  XCATCH( errcode ) {
    if( CSTR__error ) {
      VXDURABLE_SYSTEM_REASON( sys, errcode, "%s", CStringValue( CSTR__error ) );
    }
    if( sys ) {
      GRAPH_LOCK( sys ) {
        iOperation.Parser.Destroy( &sys->OP.parser );
      } GRAPH_RELEASE;
    }
  }
  XFINALLY {
    if( sys ) {
      if( vgx_instance ) {
        igraph->simple->CloseVertex( sys, &vgx_instance );
      }
      if( root ) {
        igraph->simple->CloseVertex( sys, &root );
      }
      if( prop ) {
        igraph->simple->CloseVertex( sys, &prop );
      }
    }

    iRelation.Delete( &relation );

    iString.Discard( &CSTR__system );
    iString.Discard( &CSTR__vgx );

    iString.Discard( &CSTR__vertex_id );
    iString.Discard( &CSTR__vertex_type );

    iString.Discard( &CSTR__datetime );

    iString.Discard( &CSTR__error );
  }

  if( g_SYSTEM_READY == false && sys != NULL ) {
    System__destroy();
  }

  return g_SYSTEM ? 0 : -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__destroy( void ) {
  int ret = 0;

  if( g_SYSTEM == NULL ) {
    return -1;
  }
  
  vgx_Graph_t *sys = g_SYSTEM;
  vgx_Graph_vtable_t *igraph = CALLABLE( sys );
  object_allocator_context_t *alloc = sys->ephemeral_string_allocator_context;

  CString_t *CSTR__error = NULL;

  CString_t *CSTR__vertex_id = NULL;

  vgx_Vertex_t *root = NULL;
  vgx_Vertex_t *vgx_instance = NULL;

  const char *vgx_instance_id = NULL;
  CString_t *CSTR__vgx_instance_id = NULL;

  int64_t tms_t0 = 0;
  int64_t tms_now = __MILLISECONDS_SINCE_1970();

  CString_t *CSTR__datetime = NULL;

  XTRY {

    //
    // Flush and disable system's event processor
    //
    vgx_ExecutionTimingBudget_t flush_budget = _vgx_get_execution_timing_budget( tms_now, 30000 );
    vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_execution_timing_budget( tms_now, 30000 );
    iGraphEvent.NOCS.Flush( sys, &flush_budget );
    iGraphEvent.NOCS.Disable( sys, &disable_budget ); 


    //
    // Detach everything
    //
    if( iSystem.DetachOutput( NULL, true, false, 30000, &CSTR__error ) < 0 ) {
      if( CSTR__error ) {
        VXDURABLE_SYSTEM_REASON( sys, 0x001, "%s", CStringValue( CSTR__error ) );
      }
    }

    //
    // Stop the system I/O agent
    //
    GRAPH_LOCK( sys ) {
      iOperation.System_SYS_CS.StopAgent( sys );
    } GRAPH_RELEASE;

    //
    // Destroy validator
    //
    iTransactional.Validator.Delete( &sys->OP.system.validator );

    //
    // Destroy inner plugin tracking resources
    //
    iVGXServer.Resource.Plugin.Clear();

    //
    // Open system root
    //
    CSTR__vertex_id = CStringNewFormatAlloc( alloc, iSystem.Name.RootVertex() );
    if( (root = __open_system_vertex( sys, &CSTR__vertex_id, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    // Retrieve the current vgx instance identifier
    if( CALLABLE( root )->GetStringProperty( root, "current_instance", &CSTR__vgx_instance_id ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    //
    // Open current vgx instance
    //
    CSTR__vertex_id = CStringNewFormatAlloc( alloc, CStringValue( CSTR__vgx_instance_id ) );
    if( (vgx_instance = __open_system_vertex( sys, &CSTR__vertex_id, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }
    vgx_instance_id = CALLABLE( vgx_instance )->IDString( vgx_instance );
    // Get start time property
    if( CALLABLE( vgx_instance )->GetIntProperty( vgx_instance, "tms_t0", &tms_t0 ) == 0 ) {
      int64_t uptime = (int64_t)round( (tms_now - tms_t0) / 1000.0 );
      // Set duration of current vgx instance
      CALLABLE( vgx_instance )->SetIntProperty( vgx_instance, "ts_uptime", uptime );
      // Set duration of current vgx instance (human readable)
      CALLABLE( vgx_instance )->FormatStringProperty( vgx_instance, "uptime", "%lldd %lldh %lldm %llds", uptime/86400, (uptime%86400)/3600, (uptime%3600)/60, uptime%60 );
    }

    // Set end time property (int)
    if( CALLABLE( vgx_instance )->SetIntProperty( vgx_instance, "tms_t1", tms_now ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Set end time property (datetime string)
    if( (CSTR__datetime = igraphinfo.CTime( tms_now, true )) != NULL ) {
      if( CALLABLE( vgx_instance )->SetStringProperty( vgx_instance, "datetime_t1", CStringValue( CSTR__datetime ) ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }
      iString.Discard( &CSTR__datetime );
    }

    // Set query count property
    if( CALLABLE( vgx_instance )->SetIntProperty( vgx_instance, "query_count", iSystem.QueryCount() ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }

    // Set query time nanosecond accumulation
    if( CALLABLE( vgx_instance )->SetIntProperty( vgx_instance, "query_time_nanosec_acc", iSystem.QueryTimeNanosecAcc() ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
    }

    // Set query time average
    if( CALLABLE( vgx_instance )->SetRealProperty( vgx_instance, "query_time_average", iSystem.QueryTimeAverage() ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
    }

    //
    // Close
    //
    igraph->simple->CloseVertex( sys, &vgx_instance );
    igraph->simple->CloseVertex( sys, &root );

    //
    // Persist
    //
    if( igraph->BulkSerialize( sys, 30000, true, false, NULL, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
    }

  } 
  XCATCH( errcode ) {
    if( CSTR__error ) {
      VXDURABLE_SYSTEM_REASON( sys, errcode, "%s", CStringValue( CSTR__error ) );
    }
    ret = -1;
  }
  XFINALLY {
    if( sys && vgx_instance ) {
      igraph->simple->CloseVertex( sys, &vgx_instance );
    }
    if( sys && root ) {
      igraph->simple->CloseVertex( sys, &root );
    }

    if( CSTR__vgx_instance_id ) {
      iEnumerator_OPEN.Property.Value.Discard( sys, CSTR__vgx_instance_id );
    }

    iString.Discard( &CSTR__vertex_id );

    iString.Discard( &CSTR__datetime );

    iString.Discard( &CSTR__error );
  }

  g_SYSTEM_READY = false;

  COMLIB_OBJECT_DESTROY( g_SYSTEM );
  g_SYSTEM = NULL;




  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int System__attach_output( vgx_StringList_t *uri_strings, vgx_TransactionalProducerAttachMode mode, bool handshake, int timeout_ms, CString_t **CSTR__error ) {
  /* Example
  *  -------
  *
  *    vgx://127.0.0.1:8099
  * 
  */
  int attached = -1;

  if( timeout_ms < 5000 ) {
    timeout_ms = 5000;
  }

  // Get system graph
  vgx_Graph_t *SYSTEM = System__get_system_graph();
  if( SYSTEM == NULL ) {
    __format_error_string( CSTR__error, "No system graph" );
    return -1;
  }

  char *local_ip = cxgetip( NULL );
  if( local_ip == NULL ) {
    __format_error_string( CSTR__error, "Failed to get local IP address" );
    return -1;
  }

  BEGIN_OPSYS_STATE_CHANGE( SYSTEM, timeout_ms ) {

    int64_t sz = iString.List.Size( uri_strings );
    vgx_URI_t **URI_list = NULL;
    vgx_URI_t **cursor;
    vgx_URI_t *URI;

    vgx_TransactionalProducer_t *producer = NULL;
    bool sys_emitter_running = false;

    char *remote_ip = NULL;

    XTRY {
      uint16_t txport = 0;
      GRAPH_LOCK( SYSTEM ) {
        vgx_TransactionalConsumerService_t *consumer_service = SYSTEM->OP.system.consumer;
        if( consumer_service ) {
          txport = consumer_service->bind_port;
        }
      } GRAPH_RELEASE;

      vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;

      // URI list
      if( (URI_list = calloc( sz+1, sizeof( vgx_URI_t* ) )) == NULL ) {
        __format_error_string( CSTR__error, "Memory error" );
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // Build and validate URI list
      for( int64_t i=0; i<sz; i++ ) {
        const char *uri = iString.List.GetChars( uri_strings, i );
        // Verify that URI string is valid by creating a URI object
        if( (URI_list[i] = iURI.New( uri, CSTR__error )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 ); // invalid uri
        }

        // Detect local loop
        URI = URI_list[i];
        if( URI_SCHEME__vgx( iURI.Scheme( URI ) ) ) {
          // Get remote IP address
          if( (remote_ip = cxgetip( iURI.Host( URI ) )) == NULL ) {
            iURI.Address.Error( CSTR__error, -1 );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 ); // failed to resolve remote IP
          }

          uint16_t port = iURI.PortInt( URI );
          if( CharsEqualsConst( remote_ip, local_ip ) && port == txport ) {
            __format_error_string( CSTR__error, "Cannot attach to self" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 ); // Self-loop forbidden
          }
        }

        int already;
        GRAPH_LOCK( SYSTEM ) {
          already = iTransactional.Producers.Contains( opsys->producers, uri );
        } GRAPH_RELEASE;

        if( !already ) {
          // Try connect 
          VXDURABLE_SYSTEM_VERBOSE( SYSTEM, 0x005, "Will open and close URI to validate" );
          if( iURI.Connect( URI_list[i], timeout_ms, CSTR__error ) < 0 ) {
            // TODO:
            // The entire attach will fail for all URI if only a single destination is
            // unreachable. We need to allow connect to others even if one fails.
            // We would remove the failed destination from the list of URIs and proceed
            // with the others. We will log the error.
            //
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 ); // connect failed
          }
          // Close
          iURI.Close( URI_list[i] );
          VXDURABLE_SYSTEM_VERBOSE( SYSTEM, 0x007, "URI validated" );
        }
      }


      int err = 0;
      GRAPH_LOCK( SYSTEM ) {
        // Add producers for all URIs
        cursor = URI_list;
        while( (URI = *cursor++) != NULL ) {
          // Add producer for this URI
          if( (producer = __add_and_connect_producer_SYS_CS( SYSTEM, URI, mode, handshake, CSTR__error )) != NULL ) {
            if( producer->URI == NULL ) {
              __format_error_string( CSTR__error, "Transactional producer unexpectedly detached after successful attach" );
              --err;
            }
            else if( (sys_emitter_running = __ensure_graph_emitter_CS( SYSTEM, CSTR__error )) == true ) {
              if( producer->URI == NULL ) {
                __format_error_string( CSTR__error, "Transactional producer detached during system emitter startup" );
                --err;
              }
              else {
                // Capture system attach message (say hello)
                if( iOperation.System_SYS_CS.Attach( SYSTEM ) < 1 ) {
                  __format_error_string( CSTR__error, "Failed to capture Attach operation" );
                  --err;
                }
                if( iOperation.Graph_CS.SetModified( SYSTEM ) < 1 ) {
                  __format_error_string( CSTR__error, "Failed to capture modified operation" );
                  --err;
                }
                if( COMMIT_GRAPH_OPERATION_CS( SYSTEM ) < 1 ) {
                  __format_error_string( CSTR__error, "Failed to commit graph operation" );
                  --err;
                }
                if( OPERATION_IS_OPEN( &SYSTEM->operation ) == false ) {
                  __format_error_string( CSTR__error, "Graph operation capture was closed" );
                  --err;
                }
                if( producer->URI == NULL ) {
                  __format_error_string( CSTR__error, "Transactional producer detached during system attach operation capture" );
                  --err;
                }
              }
            }
          }
          // Make sure producer was added and emitter (still) running,
          // then configure all user graphs with this producer
          if( producer && sys_emitter_running && err == 0 ) {
            GRAPH_SUSPEND_LOCK( SYSTEM ) {
              // Configure user graphs
              GRAPH_FACTORY_ACQUIRE {
                if( producer->URI == NULL ) {
                  const char *s_err = "Transactional producer no longer attached!";
                  REASON( 0x000, "%s", s_err );
                  __format_error_string( CSTR__error, "%s", s_err );
                  err = -1;
                }
                else {
                  vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
                  if( graphs ) {
                    vgx_Graph_t **curgraph = graphs;
                    vgx_Graph_t *graph;
                    while( (graph = *curgraph++) != NULL ) {
                      // Capture graph creation to system emitter and make sure graph emitter is running and ready
                      if( iSystem.CaptureCreateGraph( graph, producer ) < 0 ) {
                        __format_error_string( CSTR__error, "Failed to capture graph creation to system emitter" );
                        err = -1;
                        break;
                      }
                      if( _vgx_graph_is_local_only_OPEN( graph ) ) {
                        VXDURABLE_SYSTEM_INFO( graph, 0x008, "Local graph not attached to output stream" );
                      }
                      else {
                        VXDURABLE_SYSTEM_INFO( graph, 0x009, "Attach: '%s'", iURI.URI( producer->URI ) );
                      }
                    }
                    free( (void*)graphs );
                  }
                }
              } GRAPH_FACTORY_RELEASE;
            } GRAPH_RESUME_LOCK;
          }
          else {
            err = -1;
            break;
          }
        }
      } GRAPH_RELEASE;

      if( err < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
      }

      if( iOperation.Fence( timeout_ms ) < 0 ) {
        const char *s_err = "Operation fence timeout, cannot attach at this time";
        __format_error_string( CSTR__error, "%s", s_err );
        VXDURABLE_SYSTEM_INFO( SYSTEM, 0x00B, "%s", s_err );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x00C );
      }

      //
      attached = 1;

    }
    XCATCH( errcode ) {
      __format_error_string( CSTR__error, "unknown error" );
      attached = -1;
    }
    XFINALLY {
      if( URI_list ) {
        cursor = URI_list;
        while( (URI = *cursor++) != NULL ) {
          iURI.Delete( &URI );
        }
        free( URI_list );
      }
      free( local_ip );
      free( remote_ip );
    }
  } END_OPSYS_STATE_CHANGE;

  return attached;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool System__is_attached( void ) {
  // TODO: should we lock the system?
  return g_SYSTEM_ATTACHED;
}



#define __STATE_CONNECTED     "CONNECTED"
#define __STATE_DISCONNECTED  "DISCONNECTED"
#define __MODE_NORMAL         "NORMAL"
#define __MODE_SYNC           "SYNCHRONIZING"
#define __MODE_UNKNOWN        "UNKNOWN"

#define __dkey_host           "host"
#define __dkey_ip             "ip"
#define __dkey_adminport      "adminport"
#define __dkey_txport         "txport"
#define __dkey_mode           "mode"
#define __dkey_status         "status"
#define __dkey_digest         "digest"
#define __dkey_master_serial  "master-serial"
#define __dkey_lag            "lag"
#define __dkey_ms             "ms"
#define __dkey_tx             "tx"
#define __dkey_lag_ms         "lag_ms"
#define __dkey( KEY )         "\"" KEY "\": "
#define __dsval               "\"%s\""
#define __dival               "%lld"
#define __disval              "\"%lld\""
#define __dsitem( KEY )       __dkey( KEY ) __dsval
#define __diitem( KEY )       __dkey( KEY ) __dival
#define __disitem( KEY )      __dkey( KEY ) __disval
#define __dopen               "{"
#define __dclose              "}"
#define __dsep                ", "



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_StringList_t * System__attached_outputs( vgx_StringList_t **descriptions, int timeout_ms ) {

#define __DESCRIPTION_FORMAT          \
  __dopen                             \
    __dsitem( __dkey_host )           \
    __dsep                            \
    __dsitem( __dkey_ip )             \
    __dsep                            \
    __diitem( __dkey_adminport )      \
    __dsep                            \
    __diitem( __dkey_txport )         \
    __dsep                            \
    __dsitem( __dkey_mode )           \
    __dsep                            \
    __dsitem( __dkey_status )         \
    __dsep                            \
    __dsitem( __dkey_digest )         \
    __dsep                            \
    __disitem( __dkey_master_serial ) \
    __dsep                            \
    __dkey( __dkey_lag  )             \
      __dopen                         \
        __diitem( __dkey_ms )         \
        __dsep                        \
        __diitem( __dkey_tx )         \
      __dclose                        \
  __dclose

#define __NEW_DESCRIPTION( SubscriberHost, SubscriberIP, AdminPort, TXPort, Mode, Status, Digest, MasterSerial, Latency, Length )  \
  CStringNewFormat( __DESCRIPTION_FORMAT,     \
                    SubscriberHost,           \
                    SubscriberIP,             \
                    (int64_t)(AdminPort),     \
                    (int64_t)(TXPort),        \
                    Mode,                     \
                    Status,                   \
                    Digest,                   \
                    MasterSerial,             \
                    Latency,                  \
                    Length )


  vgx_StringList_t *outputs = NULL;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    int64_t tms_now = _vgx_graph_milliseconds( SYSTEM ); 
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalProducersIterator_t iter = {0};
    if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
      outputs = iString.List.New( NULL, 0 );
      if( descriptions && *descriptions == NULL ) {
        *descriptions = iString.List.New( NULL, 0 );
      }
      vgx_TransactionalProducer_t *producer;
      while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
        BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
          const char *hostname = iURI.Host( producer->URI );
          // Try to resolve IP address if URI uses IP
          if( isdigit( *hostname ) ) {
            const char *nameinfo = iURI.NameInfo( producer->URI );
            if( *nameinfo ) {
              hostname = nameinfo;
            }
          }
          int port = (int)iURI.PortInt( producer->URI );
          bool is_connected = iTransactional.Producer.Connected( producer );
          int64_t latency_ms = iTransactional.Producer.QueueLatency( producer, tms_now );
          int64_t length = iTransactional.Producer.Length( producer );
          if( outputs ) {
            CString_t *CSTR__name = CStringNewFormat( "vgx://%s:%d", hostname, port );
            if( CSTR__name ) {
              iString.List.AppendSteal( outputs, &CSTR__name );
            }
          }
          if( descriptions && *descriptions ) {
            const char *subscriber_host = iURI.Host( producer->URI );
            const char *subscriber_ip = iURI.HostIP( producer->URI );
            unsigned adminport = producer->subscriber.adminport;
            unsigned txport = iURI.PortInt( producer->URI );
            const char *s_mode;
            switch( iTransactional.Producer.Mode( producer ) ) {
            case TX_ATTACH_MODE_NORMAL:
              s_mode = __MODE_NORMAL;
              break;
            case TX_ATTACH_MODE_SYNC_NEW_SUBSCRIBER:
              s_mode = __MODE_SYNC;
              break;
            default:
              s_mode = __MODE_UNKNOWN;
            }
            const char *status = is_connected ? __STATE_CONNECTED : __STATE_DISCONNECTED;
            char digest[33] = {0};
            idtostr( digest, &producer->subscriber.fingerprint );
            int64_t master_serial = producer->subscriber.master_serial;
            CString_t *CSTR__description = __NEW_DESCRIPTION( subscriber_host, subscriber_ip, adminport, txport, s_mode, status, digest, master_serial, latency_ms, length );
            if( CSTR__description ) {
              iString.List.AppendSteal( *descriptions, &CSTR__description );
            }
          }
        } END_TRANSACTIONAL_IF_ATTACHED;
      }
      iTransactional.Producers.Iterator.Clear( &iter );
    }
  }
  return outputs;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_StringList_t * System__attached_outputs_simple( void ) {

#define __DESCRIPTION_FORMAT_SHORT    \
  __dopen                             \
    __dsitem( __dkey_ip )             \
    __dsep                            \
    __diitem( __dkey_adminport )      \
    __dsep                            \
    __diitem( __dkey_txport )         \
    __dsep                            \
    __dsitem( __dkey_status )         \
    __dsep                            \
    __diitem( __dkey_lag_ms )         \
  __dclose

#define __NEW_DESCRIPTION_SHORT( SubscriberIP, AdminPort, TXPort, Status, Latency )  \
  CStringNewFormat( __DESCRIPTION_FORMAT_SHORT, \
                    SubscriberIP,               \
                    (int64_t)(AdminPort),       \
                    (int64_t)(TXPort),          \
                    Status,                     \
                    Latency )


  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return NULL;
  }
  vgx_StringList_t *output_list = iString.List.New( NULL, 0 );
  if( output_list == NULL ) {
    return NULL;
  }
  int64_t tms_now = _vgx_graph_milliseconds( SYSTEM ); 
  vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
  vgx_TransactionalProducersIterator_t iter = {0};
  if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
    vgx_TransactionalProducer_t *producer;
    while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
      char subscriber_ip[16] = {0};
      unsigned adminport = 0;
      unsigned txport = 0;
      bool is_connected = false;
      int64_t latency_ms = 0;
      BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
        strncpy( subscriber_ip, iURI.HostIP( producer->URI ), 15 );
        adminport = producer->subscriber.adminport;
        txport = iURI.PortInt( producer->URI );
        is_connected = iTransactional.Producer.Connected( producer );
        latency_ms = iTransactional.Producer.QueueLatency( producer, tms_now );
      } END_TRANSACTIONAL_IF_ATTACHED;
      if( adminport > 0 ) {
        const char *status = is_connected ? __STATE_CONNECTED : __STATE_DISCONNECTED;
        CString_t *CSTR__description = __NEW_DESCRIPTION_SHORT( subscriber_ip, adminport, txport, status, latency_ms );
        if( CSTR__description ) {
          iString.List.AppendSteal( output_list, &CSTR__description );
        }
      }
    }
    iTransactional.Producers.Iterator.Clear( &iter );
  }
  return output_list;
}



/*******************************************************************//**
 * Return the uri of our own input address, or NULL if not bound.
 * Caller owns the returned cstring.
 * 
 ***********************************************************************
 */
static CString_t * System__input_address( void ) {
  CString_t *CSTR__input = NULL;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
      vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
      CSTR__input = _vxdurable_operation_consumer_service__get_input_uri_SYS_CS( consumer_service );
    } GRAPH_RELEASE;
  }
  return CSTR__input;
}



/*******************************************************************//**
 * Return the uri of the attached provider, or NULL if no provider.
 * Caller owns the returned cstring.
 * 
 ***********************************************************************
 */
static CString_t * System__attached_input( void ) {
  CString_t *CSTR__provider = NULL;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
      vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
      CSTR__provider = _vxdurable_operation_consumer_service__get_provider_uri_SYS_CS( consumer_service );
    } GRAPH_RELEASE;
  }
  return CSTR__provider;
}



/*******************************************************************//**
 *
 * Disconnect everything. Stop all emitters. Delete all producers.
 * 
 *
 ***********************************************************************
 */
static int System__detach_output( const vgx_StringList_t *detach_subscribers, bool remove_disconnected, bool force, int timeout_ms, CString_t **CSTR__error ) {

  // This procedure requires a good timeout setting
  if( timeout_ms < 5000 ) {
    timeout_ms = 5000;
  }

  // Get all currently attached outputs. We will disconnect selected subscribers (or all if NULL)
  // and then re-attach outputs that should not be detached.
  vgx_StringList_t *current_subscribers = iSystem.AttachedOutputs( NULL, timeout_ms );
  if( current_subscribers == NULL ) {
    __set_error_string( CSTR__error, "memory error" );
    return -1;
  }

  vgx_Graph_t *SYSTEM = System__get_system_graph();
  if( SYSTEM == NULL ) {
    iString.List.Discard( &current_subscribers );
    return -1;
  }


  // Set all or specified producers that are currently not connected defunct if requested
  if( remove_disconnected || force ) {
    vgx_TransactionalProducersIterator_t iter = {0};
    if( iTransactional.Producers.Iterator.Init( SYSTEM->OP.system.producers, &iter ) ) {
      vgx_TransactionalProducer_t *producer;
      while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
        TRANSACTIONAL_LOCK( producer ) {
          if( producer->URI ) {
            // Connected producer will be forced detached if requested
            if( iTransactional.Producer.Connected( producer ) && force ) {
              // Either we are detaching all or a specific URI
              if( detach_subscribers == NULL ||
                  iString.List.Contains( detach_subscribers, iURI.URI( producer->URI ), true ) )
              {
                VXDURABLE_SYSTEM_WARNING( SYSTEM, 0x000, "Force detach: %s", iURI.URI( producer->URI ) );
                iTransactional.Producer.Abandon( producer );
              }
            }
          }

          if( producer->URI ) {
            // Producer socket is not connected
            if( !iTransactional.Producer.Connected( producer ) && remove_disconnected ) {
              // Either we are detaching all or a specific URI
              if( detach_subscribers == NULL ||
                  iString.List.Contains( detach_subscribers, iURI.URI( producer->URI ), true ) )
              {
                iTransactional.Producer.SetDefunct( producer );
              }
            }
          }
        } TRANSACTIONAL_RELEASE;
      }
      iTransactional.Producers.Iterator.Clear( &iter );
    }
  }

  if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
    if( iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) == 0 ) {
      VXDURABLE_SYSTEM_WARNING( SYSTEM, 0x001, "Emitter state will remain unchanged while input server is running." );
      iString.List.Discard( &current_subscribers );
      return 0;
    }
  }

  if( iOperation.Fence( timeout_ms ) != 1 ) {
    __set_error_string( CSTR__error, "Operation fence timeout, cannot detach at this time" );
    iString.List.Discard( &current_subscribers );
    return -1;
  }

  // Assume failure until success
  int ret = -1;

  BEGIN_OPSYS_STATE_CHANGE( SYSTEM, timeout_ms ) {
    int err = 0;

    // ---------------------------
    // First disconnect everything
    // ---------------------------

    // Stop user graph emitters
    GRAPH_FACTORY_ACQUIRE {
      vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
      if( graphs ) {
        vgx_Graph_t **cursor = graphs;
        vgx_Graph_t *graph;
        while( err == 0 && (graph = *cursor++) != NULL ) {
          GRAPH_LOCK( graph ) {
            int64_t n_writable;
            // If current thread holds writable vertices we can't continue
            if( (n_writable = _vxgraph_tracker__num_writable_locks_CS( graph )) > 0 ) {
              __format_error_string( CSTR__error, "Cannot detach with %lld writable vertices held by current thread in graph '%s'", n_writable, CALLABLE( graph )->FullPath( graph ) );
              err = -1;
            }
            else {
              // All writable vertices held by other threads must be released before we can continue detach
              GRAPH_WAIT_UNTIL( graph, (n_writable = _vgx_graph_get_vertex_WL_count_CS( graph )) == 0, timeout_ms );
              if( n_writable == 0 ) {
                if( iOperation.Emitter_CS.IsRunning( graph ) ) {
                  __stop_graph_emitter_CS( graph );
                }
              }
              else {
                __format_error_string( CSTR__error, "Cannot detach with %lld writable vertices in graph '%s'", n_writable, CALLABLE( graph )->FullPath( graph ) );
                err = -1;
              }
            }
          } GRAPH_RELEASE;
        }
        free( (void*)graphs );
      }
    } GRAPH_FACTORY_RELEASE;

    // Disconnect system
    if( err == 0 ) {
      int64_t n_pending = 0;
      GRAPH_LOCK( SYSTEM ) {
        int64_t n_writable;
        // If current thread holds writable vertices we can't continue
        if( (n_writable = _vxgraph_tracker__num_writable_locks_CS( SYSTEM )) > 0 ) {
          __format_error_string( CSTR__error, "Cannot detach with %lld writable vertices held by current thread in SYSTEM graph", n_writable );
          err = -1;
        }
        else {
          // All writable vertices held by other threads must be released before we can continue detach
          GRAPH_WAIT_UNTIL( SYSTEM, (n_writable = _vgx_graph_get_vertex_WL_count_CS( SYSTEM )) == 0, timeout_ms );
          if( n_writable == 0 ) {
            // Be polite and say goodbye
            iOperation.System_SYS_CS.Detach( SYSTEM );
            // Stop the system emitter
            if( iOperation.Emitter_CS.IsRunning( SYSTEM ) ) {
              __stop_graph_emitter_CS( SYSTEM );
            }
            // Wait for all transactions to complete
            GRAPH_WAIT_UNTIL( SYSTEM, (n_pending = iOperation.System_OPEN.TransPending( SYSTEM )) == 0, timeout_ms );
            if( n_pending > 0 ) {
              __format_error_string( CSTR__error, "Will detach with %lld unconfirmed transactions pending", n_pending );
              err = -1;
            }
            GRAPH_SUSPEND_LOCK( SYSTEM ) {
              if( iTransactional.Producers.Length( SYSTEM->OP.system.producers ) > 0 ) {
                // Clear all producers
                iTransactional.Producers.Clear( SYSTEM->OP.system.producers );
                // Reset validator
                iTransactional.Validator.Reset( SYSTEM->OP.system.validator );
                VXDURABLE_SYSTEM_INFO( SYSTEM, 0x003, "Detached" );
              }
            } GRAPH_RESUME_LOCK;
          }
          else {
            __format_error_string( CSTR__error, "Cannot detach with %lld writable vertices in SYSTEM graph", n_writable );
            err = -1;
          }
        }
      } GRAPH_RELEASE;


      // All disconnected at this point
      if( err == 0 ) {
        // We wanted to detach specific subscriber(s)
        if( current_subscribers && detach_subscribers ) {
          // We will populate new list with subscribers that should be re-attached
          vgx_StringList_t *reattach_subscribers = iString.List.New( NULL, 0 );
          if( reattach_subscribers == NULL ) {
            __set_error_string( CSTR__error, "memory error" );
            err = -1;
          }
          else {
            // Go through all originally attached subscribers (that are now detached because we just did that)
            int64_t orig_sz = iString.List.Size( current_subscribers );
            for( int64_t i=0; i<orig_sz; i++ ) {
              const char *attached_uri = iString.List.GetChars( current_subscribers, i );
              // Originally attached subscriber was NOT included in the detach list, we need to re-attach it.
              if( !iString.List.Contains( detach_subscribers, attached_uri, true ) ) {
                CString_t *CSTR__subscriber = CStringNew( attached_uri );
                if( CSTR__subscriber ) {
                  iString.List.AppendSteal( reattach_subscribers, &CSTR__subscriber );
                }
                else {
                  __set_error_string( CSTR__error, "memory error" );
                  err = -1;
                  break;
                }
              }
            }
            // Now reattach
            if( err == 0 && iString.List.Size( reattach_subscribers ) > 0 ) {
              if( iSystem.AttachOutput( reattach_subscribers, TX_ATTACH_MODE_NORMAL, false, timeout_ms, CSTR__error ) != 1 ) {
                __set_error_string( CSTR__error, "Failed to re-attach subscribers subset after detach" );
                err = -1;
              }
            }
            // Clean up
            iString.List.Discard( &reattach_subscribers );
          }
        }
      }

      // Return 1 if no errors occurred
      if( err == 0 ) {
        ret = 1;
      }
    }
  } END_OPSYS_STATE_CHANGE;

  iString.List.Discard( &current_subscribers );

  int64_t n_attached = iTransactional.Producers.Length( SYSTEM->OP.system.producers );

  GRAPH_LOCK( SYSTEM ) {
    g_SYSTEM_ATTACHED = n_attached > 0;
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool System__is_system_graph( vgx_Graph_t *graph ) {
  return graph == g_SYSTEM;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * System__get_system_graph( void ) {
  return g_SYSTEM;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t System__master_serial( void ) {
  int64_t sn = 0;
  if( g_SYSTEM ) {
    GRAPH_LOCK( g_SYSTEM ) {
      sn = g_SYSTEM->sysmaster_tx_serial;
    } GRAPH_RELEASE;
  }
  return sn;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * System__get_graph( const objectid_t *obid ) {
  if( obid == NULL || idmatch( obid, &g_SYSTEM_ID ) ) {
    return g_SYSTEM;
  }
  else {
    return igraphfactory.GetGraphByObid( obid );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int System__capture_create_graph( vgx_Graph_t *graph, vgx_TransactionalProducer_t *target_producer ) {
  int ret = 0;
  GRAPH_LOCK( graph ) {
    if( !_vgx_graph_is_local_only_CS( graph ) ) {
      const objectid_t *obid = &graph->obid;
      const char *name = CStringValue( graph->CSTR__name );
      const char *path = CStringValue( graph->CSTR__path );
      int block_order = ivertexalloc.BlockOrder( graph->vertex_allocator );
      uint32_t t0 = _vgx_graph_inception( graph );
      int64_t opid = iOperation.GetId_LCK( &graph->operation );
      vgx_Similarity_config_t *simconfig = graph->similarity ? &graph->similarity->params : NULL;
      bool exists_connected_producer = false;
      GRAPH_SUSPEND_LOCK( graph ) {
        if( g_SYSTEM ) {
          GRAPH_LOCK( g_SYSTEM ) {
            // Make sure system is not readonly
            if( iOperation.Readonly_CS.Check( g_SYSTEM ) ) {
              VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x001, "Cannot capture graph creation operation for %s/%s [SYSTEM IS READONLY]", path, name );
              ret = -1;
            }
            // Capture graph creation if system emitter exists
            else if( iOperation.Emitter_CS.IsRunning( g_SYSTEM ) ) {

              if( iOperation.System_SYS_CS.CreateGraph( g_SYSTEM, obid, name, path, block_order, t0, opid, simconfig ) < 0 ) {
                VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x002, "Failed to capture graph creation operation for %s/%s", path, name );
                ret = -1;
              }

              if( COMMIT_GRAPH_OPERATION_CS( g_SYSTEM ) < 0 ) {
                VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x003, "Failed to commit graph creation operation for %s/%s", path, name );
                ret = -1;
              }

              // Connection?
              exists_connected_producer = __producer_connected_SYS_CS( g_SYSTEM, target_producer );

            }
            // No system emitter, presumably system is not attached to any destination and capturing is disabled
            else {
              VXDURABLE_SYSTEM_VERBOSE( g_SYSTEM, 0x005, "Capture create graph '%s/%s' local only", path, name );
            }
          } GRAPH_RELEASE;
        }
        else {
          // NO SYSTEM!!!
           VXDURABLE_SYSTEM_CRITICAL( NULL, 0x006, "SYSTEM DOES NOT EXIST!" );
           ret = -1;
        }
      } GRAPH_RESUME_LOCK;
      
      // If at least one healthy producer exists make sure the graph's emitter is running
      // and set the system buffered output writer for the emitter.
      if( exists_connected_producer ) {
        CString_t *CSTR__error = NULL;
        if( __ensure_graph_emitter_CS( graph, &CSTR__error ) == false ) {
          const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
          VXDURABLE_SYSTEM_CRITICAL( graph, 0x007, "%s", msg );
          ret = -1;
        }
        iString.Discard( &CSTR__error );

        // Acquire readonly
        vgx_AccessReason_t reason = 0;
        if( CALLABLE( graph )->advanced->AcquireGraphReadonly( graph, 10000, false, &reason ) > 0 ) {
          // Capture graph state
          vgx_graph_base_counts_t counts = {0};
          iOperation.Graph_ROG.GetBaseCounts( graph, &counts );
          // Release readonly
          CALLABLE( graph )->advanced->ReleaseGraphReadonly( graph );

          // Capture state
          iOperation.Graph_CS.State( graph, &counts );
          COMMIT_GRAPH_OPERATION_CS( graph );
        }
        else {
          VXDURABLE_SYSTEM_WARNING( graph, 0x008, "Unable to capture graph state for '%s/%s' at time of creation", path, name );
        }

      }
      else if( target_producer ) {
        const char *target = iTransactional.Producer.Name( target_producer );
        VXDURABLE_SYSTEM_REASON( graph, 0x009, "Transactional producer '%s' is not registered", target ? target : "?" );
        ret = -1;
      }
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__capture_destroy_graph( const objectid_t *obid, const char *path ) {
  int ret = 0;

  if( g_SYSTEM ) {
    GRAPH_LOCK( g_SYSTEM ) {
      if( !iOperation.Readonly_CS.Check( g_SYSTEM ) ) {
        if( iOperation.System_SYS_CS.DeleteGraph( g_SYSTEM, obid ) < 0 ) {
          VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x001, "Failed to capture graph destruction operation for %s", path );
          ret = -1;
        }

        if( COMMIT_GRAPH_OPERATION_CS( g_SYSTEM ) < 0 ) {
          VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x002, "Failed to commit graph destruction operation for %s", path );
          ret = -1;
        }
      }
      else {
        VXDURABLE_SYSTEM_CRITICAL( g_SYSTEM, 0x003, "Cannot capture graph destruction operation for %s [SYSTEM IS READONLY]", path );
        ret = -1;
      }
    } GRAPH_RELEASE;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool System__begin_query( bool pri ) {
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  g_SYSTEM->q_pri_req += pri;
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  return pri;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void System__end_query( bool pri, int64_t q_time_nanosec ) {
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  g_SYSTEM->q_pri_req -= pri;
  if( q_time_nanosec >= 0 ) {
    g_SYSTEM->q_count++;
    g_SYSTEM->q_time_nanosec_acc += q_time_nanosec;
  }
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__get_query_pri_req( void ) {
  int pri_req;
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  pri_req = g_SYSTEM->q_pri_req;
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  return pri_req;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t System__query_count( void ) {
  int64_t count;
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  count = g_SYSTEM->q_count;
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  return count;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t System__query_time_nanosec_acc( void ) {
  int64_t ns_acc;
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  ns_acc = g_SYSTEM->q_time_nanosec_acc;
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  return ns_acc;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double System__query_time_average( void ) {
  double avg;
  ENTER_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  if( g_SYSTEM->q_count > 0 ) {
    avg = (g_SYSTEM->q_time_nanosec_acc / 1e9) / g_SYSTEM->q_count;
  }
  else {
    avg = 0.0;
  }
  LEAVE_CRITICAL_SECTION( &g_SYSTEM->q_lock.lock );
  return avg;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __open_property_vertex( bool writable ) {
  vgx_Vertex_t *prop_WL = NULL;
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( g_SYSTEM, 30000 );
  GRAPH_LOCK( g_SYSTEM ) {
    if( writable ) {
      CString_t *CSTR__error = NULL;
      prop_WL = _vxgraph_state__acquire_writable_vertex_CS( g_SYSTEM, NULL, &g_system_property_vertex_obid, VERTEX_STATE_CONTEXT_MAN_NULL, &timing_budget, &CSTR__error );
      iString.Discard( &CSTR__error );
    }
    else {
      prop_WL = _vxgraph_state__acquire_readonly_vertex_CS( g_SYSTEM, &g_system_property_vertex_obid, &timing_budget );
    }
  } GRAPH_RELEASE;
  return prop_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __close_property_vertex( vgx_Vertex_t **prop_LCK ) {
  return _vxgraph_state__release_vertex_OPEN_LCK( g_SYSTEM, prop_LCK );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_set_int( const char *key, int64_t value ) {
  int ret = -1;
  vgx_Vertex_t *prop_WL = __open_property_vertex( true );
  if( prop_WL ) {
    ret = CALLABLE( prop_WL )->SetIntProperty( prop_WL, key, value );
    __close_property_vertex( &prop_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_get_int( const char *key, int64_t *rvalue ) {
  int ret = -1;
  vgx_Vertex_t *prop_RO = __open_property_vertex( false );
  if( prop_RO ) {
    ret = CALLABLE( prop_RO )->GetIntProperty( prop_RO, key, rvalue );
    __close_property_vertex( &prop_RO );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_set_real( const char *key, double value ) {
  int ret = -1;
  vgx_Vertex_t *prop_WL = __open_property_vertex( true );
  if( prop_WL ) {
    ret = CALLABLE( prop_WL )->SetRealProperty( prop_WL, key, value );
    __close_property_vertex( &prop_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_get_real( const char *key, double *rvalue ) {
  int ret = -1;
  vgx_Vertex_t *prop_RO = __open_property_vertex( false );
  if( prop_RO ) {
    ret = CALLABLE( prop_RO )->GetRealProperty( prop_RO, key, rvalue );
    __close_property_vertex( &prop_RO );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_set_string( const char *key, const char *value ) {
  int ret = -1;
  vgx_Vertex_t *prop_WL = __open_property_vertex( true );
  if( prop_WL ) {
    ret = CALLABLE( prop_WL )->SetStringProperty( prop_WL, key, value );
    __close_property_vertex( &prop_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_format_string( const char *key, const char *valfmt, ... ) {
  char buffer[ 4096 ];

  va_list args;
  va_start( args, valfmt );
  int nw = vsnprintf( buffer, 4095, valfmt, args );
  va_end( args );

  if( nw > 4094 || nw < 0 ) {
    return -1;
  }

  return System__property_set_string( key, buffer );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_get_string( const char *key, CString_t **CSTR__rvalue ) {
  int ret = -1;
  vgx_Vertex_t *prop_RO = __open_property_vertex( false );
  if( prop_RO ) {
    ret = CALLABLE( prop_RO )->GetStringProperty( prop_RO, key, CSTR__rvalue );
    __close_property_vertex( &prop_RO );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int System__property_delete( const char *key ) {
  int ret = -1;
  vgx_Vertex_t *prop_WL = __open_property_vertex( true );
  if( prop_WL ) {
    ret = CALLABLE( prop_WL )->RemovePropertyKey( prop_WL, key );
    __close_property_vertex( &prop_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool System__property_exists( const char *key ) {
  int ret = -1;
  vgx_Vertex_t *prop_RO = __open_property_vertex( false );
  if( prop_RO ) {
    ret = CALLABLE( prop_RO )->HasPropertyKey( prop_RO, key );
    __close_property_vertex( &prop_RO );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * System__get_system_vertex_type_name( void ) {
  return g_system_vertex_type_name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * System__get_system_root_vertex_name( void ) {
  return g_system_root_vertex_name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * System__get_system_property_vertex_name( void ) {
  return g_system_property_vertex_name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_system.h"
  
test_descriptor_t _vgx_vxdurable_system_tests[] = {
  { "VGX System Graph Tests", __utest_vxdurable_system },
  {NULL}
};
#endif
