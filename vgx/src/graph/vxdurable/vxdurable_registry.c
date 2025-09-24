/*######################################################################
 *#
 *# vxdurable_registry.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxenum.h"
#include "_vxarcvector.h"
#include "versiongen.h"
#include <signal.h>

#ifndef VGX_VERSION
#define VGX_VERSION ?.?.?
#endif

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

/* general module initialization flag */
static int __g_vxgraph_api_initialized = 0;

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "vgx", VERSIONGEN_XSTR( VGX_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "vgx", VERSIONGEN_XSTR( VGX_VERSION ) );

static framehash_t *g_graph_registry = NULL;
static CString_t *CSTR__g_system_root = NULL;

static int g_registry_readonly_RCS = 0;

static bool g_allow_event_processor = true;

static vector_type_t g_global_vector_type = VECTOR_TYPE_NULL;
static bool g_force_events_idle_deserialize = false;
static bool g_force_readonly_deserialize = false;

static objectid_t g_instance_unique_label = {0};

static objectid_t g_fingerprint = {0};
static int64_t g_last_fingerprint_change_tms = 0;

static int __validate_class_definitions_ROPEN( const CString_t *CSTR__system_root );
static int __vector_settings_ROPEN( const CString_t *CSTR__system_root, vector_type_t global_vtype );
static int __initialize_graph_registry_ROPEN( const CString_t *CSTR__system_root );
static int __destroy_graph_registry_ROPEN( void );
static const vgx_Graph_t ** __list_graphs_RCS( int64_t *n_graphs );

static bool __is_registry_readonly( void );
static bool __wait_for_writable_registry_RCS( int timeout_ms );

static const objectid_t __graph_id_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name );
static vgx_Graph_t * __new_graph_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages );
static vgx_Graph_t * __get_graph_by_obid_RCS( const objectid_t *obid );
static vgx_Graph_t * __get_graph_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name );
static int __set_graph_ROPEN( const vgx_Graph_t *graph, vgx_StringTupleList_t **messages );
static void __noop_graph_destructor( comlib_object_t *obj );
static int __remove_and_destroy_graph_RCS( vgx_Graph_t **graph );
static int __del_graph_RCS( vgx_Graph_t **graph, bool erase );

static int __acquire_graph_RCS( vgx_Graph_t *graph, vgx_StringTupleList_t **messages );
static int __release_graph_RCS( vgx_Graph_t *graph, uint32_t *owner );

static vgx_StringTupleList_t * __append_trace_to_messages( vgx_StringTupleList_t *message_list );

static vgx_Graph_t *  __retrieve_graph_by_path_and_name_RCS( const CString_t *CSTR__path, const CString_t *CSTR__name );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__reg_path( void ) {
  return CSTR__g_system_root ? CStringValue( CSTR__g_system_root ) : "";
}

#define __MESSAGE( LEVEL, Code, Format, ... ) LEVEL( Code, "VX::REG(%s): " Format, __reg_path(), ##__VA_ARGS__ )

#define VXDURABLE_REGISTRY_VERBOSE( Code, Format, ... )   __MESSAGE( VERBOSE, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_REGISTRY_INFO( Code, Format, ... )      __MESSAGE( INFO, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_REGISTRY_WARNING( Code, Format, ... )   __MESSAGE( WARN, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_REGISTRY_REASON( Code, Format, ... )    __MESSAGE( REASON, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_REGISTRY_CRITICAL( Code, Format, ... )  __MESSAGE( CRITICAL, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_REGISTRY_FATAL( Code, Format, ... )     __MESSAGE( FATAL, Code, Format, ##__VA_ARGS__ )



/* ---------- */
/* Graph Info */
/* ---------- */

static CString_t * GraphInfo_graph_path( const vgx_Graph_t *graph );
static int GraphInfo_new_name_and_path( const CString_t *CSTR__args_path, const CString_t *CSTR__args_name, CString_t **CSTR__computed_path, CString_t **CSTR__computed_name, vgx_StringTupleList_t **messages );
static objectid_t GraphInfo_graph_id( const CString_t *CSTR__path, const CString_t *CSTR__name );
static CString_t * GraphInfo_version( int verbosity );
static CString_t * GraphInfo_ctime( int64_t tms, bool millisec );


/* ------------- */
/* Graph Factory */
/* ------------- */
static int GraphFactory_acquire_readonly( void );
static int GraphFactory_release( void );
static const CString_t * GraphFactory_system_root( void );
static const CString_t * GraphFactory_set_system_root( const char *sysroot );
static bool GraphFactory_disable_events( void );
static bool GraphFactory_enable_events( void );
static bool GraphFactory_events_enabled( void );
static int GraphFactory_set_vector_type( vector_type_t vtype );
static void GraphFactory_unset_vector_type( void );
static bool GraphFactory_feature_vectors( void );
static bool GraphFactory_euclidean_vectors( void );
static bool GraphFactory_is_events_idle_deserialize( void );
static bool GraphFactory_is_readonly_deserialize( void );
static int GraphFactory_suspend_events( int timeout_ms );
static int GraphFactory_events_resumable( void );
static int GraphFactory_resume_events( void );
static int GraphFactory_set_all_readonly( int timeout_ms, vgx_AccessReason_t *reason );
static int GraphFactory_count_all_readonly( void );
static int GraphFactory_clear_all_readonly( void );
static bool GraphFactory_is_initialized( void );
static int GraphFactory_initialize( vgx_context_t *context, vector_type_t global_vtype, bool events_idle, bool readonly, vgx_StringTupleList_t **messages );
static int64_t GraphFactory_persist( void );
static int GraphFactory_register_graph( const vgx_Graph_t *graph, vgx_StringTupleList_t **messages );
static bool GraphFactory_has_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name );
static vgx_Graph_t * GraphFactory_new_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages );
static vgx_Graph_t * GraphFactory_open_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages, int timeout_ms );
static vgx_Graph_t * GraphFactory_get_graph_by_obid( const objectid_t *obid );
static int GraphFactory_del_graph_by_obid( const objectid_t *obid );
static int GraphFactory_close_graph( vgx_Graph_t **graph, uint32_t *owner );
static int GraphFactory_delete_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, int timeout_ms, bool erase, CString_t **CSTR__reason );
static int GraphFactory_size( void );
static const vgx_Graph_t ** GraphFactory_list_graphs( int64_t *n_graphs );
static int GraphFactory_sync_all_graphs( bool hard, int timeout_ms, CString_t **CSTR__error );
static int GraphFactory_cancel_sync( void );
static bool GraphFactory_is_sync_active( void );
static objectid_t GraphFactory_unique_label( void );
static objectid_t GraphFactory_fingerprint( void );
static int64_t GraphFactory_fingerprint_age( void );
static int GraphFactory_earliest_durable_transaction( objectid_t *txid, int64_t *sn, int64_t *ts, int *n_serializing );
static void GraphFactory_shutdown( void );


DLL_EXPORT vgx_IGraphInfo_t igraphinfo = {
  .GraphPath          = GraphInfo_graph_path,
  .NewNameAndPath     = GraphInfo_new_name_and_path,
  .GraphID            = GraphInfo_graph_id,
  .Version            = GraphInfo_version,
  .CTime              = GraphInfo_ctime
};


DLL_EXPORT vgx_IGraphFactory_t igraphfactory = {
  .Acquire                  = GraphFactory_acquire_readonly,
  .Release                  = GraphFactory_release,
  .SystemRoot               = GraphFactory_system_root,
  .SetSystemRoot            = GraphFactory_set_system_root,
  .DisableEvents            = GraphFactory_disable_events,
  .EnableEvents             = GraphFactory_enable_events,
  .EventsEnabled            = GraphFactory_events_enabled,
  .SetVectorType            = GraphFactory_set_vector_type,
  .UnsetVectorType          = GraphFactory_unset_vector_type,
  .FeatureVectors           = GraphFactory_feature_vectors,
  .EuclideanVectors         = GraphFactory_euclidean_vectors,
  .IsEventsIdleDeserialize  = GraphFactory_is_events_idle_deserialize,
  .IsReadonlyDeserialize    = GraphFactory_is_readonly_deserialize,
  .SuspendEvents            = GraphFactory_suspend_events,
  .EventsResumable          = GraphFactory_events_resumable,
  .ResumeEvents             = GraphFactory_resume_events,
  .SetAllReadonly           = GraphFactory_set_all_readonly,
  .CountAllReadonly         = GraphFactory_count_all_readonly,
  .ClearAllReadonly         = GraphFactory_clear_all_readonly,
  .IsInitialized            = GraphFactory_is_initialized,
  .Initialize               = GraphFactory_initialize,
  .Persist                  = GraphFactory_persist,
  .RegisterGraph            = GraphFactory_register_graph,
  .HasGraph                 = GraphFactory_has_graph,
  .NewGraph                 = GraphFactory_new_graph,
  .OpenGraph                = GraphFactory_open_graph,
  .CloseGraph               = GraphFactory_close_graph,
  .DeleteGraph              = GraphFactory_delete_graph,
  .GetGraphByObid           = GraphFactory_get_graph_by_obid,
  .DelGraphByObid           = GraphFactory_del_graph_by_obid,
  .Size                     = GraphFactory_size,
  .ListGraphs               = GraphFactory_list_graphs,
  .SyncAllGraphs            = GraphFactory_sync_all_graphs,
  .CancelSync               = GraphFactory_cancel_sync,
  .IsSyncActive             = GraphFactory_is_sync_active,
  .UniqueLabel              = GraphFactory_unique_label,
  .Fingerprint              = GraphFactory_fingerprint,
  .FingerprintAge           = GraphFactory_fingerprint_age,
  .DurabilityPoint          = GraphFactory_earliest_durable_transaction,
  .Shutdown                 = GraphFactory_shutdown
};


/* */
static int g_registry_init = 0;
static CS_LOCK g_registry_lock = {0};
static int16_t g_registry_lock_count = 0;
static CS_COND g_registry_graph_availability = {0};




__inline static int16_t __enter_registry_CS( void ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &g_registry_lock.lock );
  // SAFE HERE
  return ++g_registry_lock_count;
}


__inline static int16_t __leave_registry_CS( void ) {
  // SAFE HERE
  int16_t c = --g_registry_lock_count;
  LEAVE_CRITICAL_SECTION( &g_registry_lock.lock );
  // UNSAFE HERE
  return c;
}



#define REGISTRY_LOCK                 \
  do {                                \
    __enter_registry_CS();            \
    do


#define REGISTRY_RELEASE              \
    WHILE_ZERO;                       \
    __leave_registry_CS();            \
  } WHILE_ZERO



#define REGISTRY_SUSPEND_LOCK                                       \
  do {                                                              \
    /* SAFE HERE */                                                 \
    int16_t __presus_reg_recursion__ = g_registry_lock_count;       \
    while( __leave_registry_CS() > 0 );                             \
    /* UNSAFE HERE */                                               \
    do


#define REGISTRY_RESUME_LOCK                                    \
    /* UNSAFE HERE */                                           \
    WHILE_ZERO;                                                 \
    while( __enter_registry_CS() < __presus_reg_recursion__ );  \
    /* SAFE HERE */                                             \
  } WHILE_ZERO




#define WAIT_FOR_REGISTRY_AVAILABLE( TimeoutMilliseconds )          \
  do {                                                              \
    /* SAFE HERE */                                                 \
    int16_t __prewait_reg_recursion__ = g_registry_lock_count;      \
    while( g_registry_lock_count > 1 ) {                            \
      __leave_registry_CS();                                        \
    }                                                               \
    g_registry_lock_count--; /* down to 0 */                        \
    /* Release one lock and sleep until condition or timeout */     \
    TIMED_WAIT_CONDITION_CS( &(g_registry_graph_availability.cond), &(g_registry_lock.lock), TimeoutMilliseconds );  \
    /* One lock now re-acquired */                                  \
    /* SAFE HERE */                                                 \
    g_registry_lock_count++; /* up to 1 */                          \
    while( g_registry_lock_count < __prewait_reg_recursion__ ) {    \
      __enter_registry_CS();                                        \
    }                                                               \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __noop_graph_destructor( comlib_object_t *obj ) {
  vgx_Graph_t *graph = (vgx_Graph_t*)obj;
  VXDURABLE_REGISTRY_INFO( 0x001, "Removed from registry: %s", CALLABLE( graph )->FullPath( graph ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __remove_and_destroy_graph_RCS( vgx_Graph_t **graph ) {
  int deleted = 0;
  objectid_t obid;
  objectid_t *graphkey = NULL;
  f_object_destructor_t destroy = NULL;
  framehash_valuetype_t vtype;

  // Hide graph destructor before we remove graph from registry to avoid auto-destruction by framehash
  REGISTRY_SUSPEND_LOCK {
    GRAPH_LOCK( *graph ) {
      graphkey = idcpy( &obid, &(*graph)->obid );
      destroy = CALLABLE( *graph )->vm_destroy;
      CALLABLE( *graph )->vm_destroy = __noop_graph_destructor;
    } GRAPH_RELEASE;
  } REGISTRY_RESUME_LOCK;

  // Block until writable or timeout
  if( !__wait_for_writable_registry_RCS( 30000 ) ) {
    VXDURABLE_REGISTRY_REASON( 0x000, "Registry busy (readonly), cannot remove graph." );
    return -1;
  }

#ifdef VGX_CONSISTENCY_CHECK
  if( CALLABLE( *graph )->advanced->DebugCheckAllocators( *graph, NULL ) < 0 ) {
    VXDURABLE_REGISTRY_CRITICAL( 0x001, "Bad." );
  }
#endif

  // Remove graph from registry (will call the NOOP destructor patched in above)
  if( (vtype = CALLABLE( g_graph_registry )->Delete( g_graph_registry, CELL_KEY_TYPE_HASH128, graphkey )) == CELL_VALUE_TYPE_OBJECT128 ) {
    deleted = 1;
  }
  // Graph did not exist in the registry
  else if( vtype == CELL_VALUE_TYPE_NULL ) {
    deleted = 0;
  }
  // Error
  else {
    deleted = -1;
  }

  // Restore graph destructor and destroy
  REGISTRY_SUSPEND_LOCK {
    GRAPH_LOCK( *graph ) {
      CALLABLE( *graph )->vm_destroy = destroy;
    } GRAPH_RELEASE;
    // Destroy graph only if it was removed from registry above
    if( deleted == 1 ) {
      COMLIB_OBJECT_DESTROY( *graph );
      *graph = NULL;
    }
  } REGISTRY_RESUME_LOCK;

  return deleted;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int GraphFactory_acquire_readonly( void ) {
  int ro;
  REGISTRY_LOCK {
    ro = ++g_registry_readonly_RCS;
  } REGISTRY_RELEASE;
  return ro;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int GraphFactory_release( void ) {
  int ro = 0;
  REGISTRY_LOCK {
    if( g_registry_readonly_RCS > 0 ) {
      ro = --g_registry_readonly_RCS;
    }
  } REGISTRY_RELEASE;
  return ro;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool __is_registry_readonly( void ) {
  bool ro;;
  REGISTRY_LOCK {
    ro = g_registry_readonly_RCS > 0;
  } REGISTRY_RELEASE;
  return ro;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool __wait_for_writable_registry_RCS( int timeout_ms ) {
  // readonly
  if( g_registry_readonly_RCS > 0 ) {
    int64_t deadline = __GET_CURRENT_MILLISECOND_TICK() + timeout_ms;
    do {
      REGISTRY_SUSPEND_LOCK {
        sleep_milliseconds( 10 );
      } REGISTRY_RESUME_LOCK;
      if( __GET_CURRENT_MILLISECOND_TICK() > deadline ) {
        return g_registry_readonly_RCS == 0;
      }
    } while( g_registry_readonly_RCS > 0 );
  }
  
  // writable
  return true;
}



/*******************************************************************//**
 *
 * NOTE: caller owns new CString_t object!
 ***********************************************************************
 */
static CString_t * GraphInfo_graph_path( const vgx_Graph_t *graph ) {
  const CString_t *CSTR__system_root = GraphFactory_system_root();
  if( graph->CSTR__path ) {
    CString_t *CSTR__path = CStringNewFormat( "%s/%s", CSTR__system_root ? CStringValue(CSTR__system_root) : ".", CStringValue(graph->CSTR__path) );
    return CSTR__path;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphInfo_new_name_and_path( const CString_t *CSTR__args_path, const CString_t *CSTR__args_name, CString_t **CSTR__computed_path, CString_t **CSTR__computed_name, vgx_StringTupleList_t **messages ) {
  int ret = 0;
  CString_t *CSTR__error = NULL;
  XTRY {
    if( CSTR__args_name == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x581, "graph name is null" );
    }

    if( CSTR__computed_name ) {
      *CSTR__computed_name = iString.Clone( CSTR__args_name );
    }

    if( CSTR__computed_path ) {
      // Check which graph path to use
      if( CSTR__args_path != NULL ) {
        const char *path = CStringValue( CSTR__args_path );
        if( path_isabs( path ) ) {
          const char *s_err = "?";
          // graph path must be relative, and it's relative to vgx system root
          if( (CSTR__error = CStringNewFormat( "graph path must be relative, got '%s'", path )) != NULL ) {
            s_err = CStringValue( CSTR__error );
            if( messages ) {
              if( *messages == NULL ) {
                *messages = _vgx_new_empty_string_tuple_list();
              }
              _vgx_string_tuple_list_append_item( *messages, "trace", s_err );
            }
          }
          THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x582, s_err ); 
        }
        // Use provided (relative) path
        *CSTR__computed_path = iString.Clone( CSTR__args_path );
      }
      else {
        // Default: graph dir = graph name
        *CSTR__computed_path = iString.Clone( CSTR__args_name );
      }
    }
  }
  XCATCH( errcode ) {
    iString.Discard( CSTR__computed_name );
    iString.Discard( CSTR__computed_path );
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
  }
  
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t GraphInfo_graph_id( const CString_t *CSTR__path, const CString_t *CSTR__name ) {
  CString_t *CSTR__computed_path = NULL;
  objectid_t obid;
  if( GraphInfo_new_name_and_path( CSTR__path, CSTR__name, &CSTR__computed_path, NULL, NULL ) < 0 ) {
    idunset( &obid ); // error
  }
  else {
    obid = __graph_id_ROPEN( CSTR__computed_path, CSTR__name );
    CStringDelete( CSTR__computed_path );
  }
  return obid;
}



/*******************************************************************//**
 * 
 * WARNING: Caller owns returned string!
 ***********************************************************************
 */
static CString_t * GraphInfo_version( int verbosity ) {
  switch( verbosity ) {
  case 0:
    return CStringNew( g_version_info );
  case 1:
    return CStringNew( g_version_info_ext );
  default:
    return CStringNewFormat( "%s\n%s\n%s\n%s\n%s",
                      g_version_info_ext,
                      iFramehash.Version( true ),
                      cxmalloc_version( true ),
                      comlib_version( true  ),
                      cxlib_version( true )
                    );
  }
}



/*******************************************************************//**
 * 
 * WARNING: Caller owns returned string!
 ***********************************************************************
 */
static CString_t * GraphInfo_ctime( int64_t tms, bool millisec ) {
  char tbuf[32];
  CString_constructor_args_t cargs = {
    .string      = tbuf,
    .len         = 23,
    .ucsz        = 0,
    .format      = NULL,
    .format_args = NULL,
    .alloc       = NULL
  };

  if( tms < 0 ) {
    tms = __MILLISECONDS_SINCE_1970();
  }
  time_t ts = tms / 1000;
  struct tm *_tm = localtime( &ts );
  if( _tm == NULL ) {
    return NULL;
  }
  if( millisec ) {
    char *pmilli = (char*)cargs.string + 20; // (see why below)
    strftime( (char*)cargs.string, 31, "%Y-%m-%d %H:%M:%S.", _tm ); 
    //                 2016-07-07 12:34:56.---
    //                 0123456789..........^
    //                                     20
    sprintf( pmilli, "%03lld", tms % 1000 ); // since system start (not epoch) but good enough for logging, we just want more resolution between seconds
  }
  else {
    strftime( (char*)cargs.string, 31, "%Y-%m-%d %H:%M:%S", _tm ); 
  }
  return COMLIB_OBJECT_NEW( CString_t, NULL, &cargs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int vgx_GRAPH_INIT( void ) { 
  if( !__g_vxgraph_api_initialized ) {
    SET_EXCEPTION_CONTEXT
    vgx_Vertex_RegisterClass();
    vgx_AdjacencyQuery_RegisterClass();
    vgx_NeighborhoodQuery_RegisterClass();
    vgx_GlobalQuery_RegisterClass();
    vgx_AggregatorQuery_RegisterClass();
    vgx_Evaluator_RegisterClass();
    Graph_RegisterClass();
    framehash_INIT(); 
    _vxenum__initialize();
    iEvaluator.Initialize();
    iGraphResponse.Initialize();
    __g_vxgraph_api_initialized = 1;
    return 1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void vgx_GRAPH_DESTROY( void ) { 
  if( __g_vxgraph_api_initialized ) {
    iGraphResponse.Destroy();
    iEvaluator.Destroy();
    _vxenum__destroy();
    vgx_Vertex_UnregisterClass();
    vgx_AdjacencyQuery_UnregisterClass();
    vgx_NeighborhoodQuery_UnregisterClass();
    vgx_GlobalQuery_UnregisterClass();
    vgx_AggregatorQuery_UnregisterClass();
    vgx_Evaluator_UnregisterClass();
    Graph_UnregisterClass();
    framehash_DESTROY(); 

    __g_vxgraph_api_initialized = 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * GraphFactory_system_root( void ) {
  return CSTR__g_system_root;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * GraphFactory_set_system_root( const char *sysroot ) {
  if( CSTR__g_system_root ) {
    CStringDelete( CSTR__g_system_root );
  }
  if( sysroot ) {
    CSTR__g_system_root = CStringNew( sysroot );
  }
  else {
    CSTR__g_system_root = CStringNew( "." );
  }
  return CSTR__g_system_root;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_StringTupleList_t * __append_trace_to_messages( vgx_StringTupleList_t *message_list ) {
  vgx_StringTupleList_t *messages = message_list;
  char **trace;
  // Get current trace
  if( (trace = cxlib_trace_get_msglist()) != NULL ) {
    // If no message list was supplied, create a new list
    if( messages == NULL ) {
      messages = _vgx_new_empty_string_tuple_list();
    }
    if( messages ) {
      // Process each item in trace and add it to messages
      char **cursor = trace;
      const char *msg;
      CString_t *CSTR__msg;
      CString_t **CSTR__parts;
      while( (msg = *cursor++) != NULL ) {
        const char *key = "trace";
        if( (CSTR__msg = CStringNew( msg )) != NULL ) {
          int32_t sz;
          // If trace message contains a tab, split on tab and append the first two parts as key/value,
          // otherwise use the default key and the original message as value.
          if( (CSTR__parts = CALLABLE( CSTR__msg )->Split( CSTR__msg, "\t", &sz )) != NULL ) {
            if( sz > 1 ) {
              key = CStringValue( CSTR__parts[0] );
              msg = CStringValue( CSTR__parts[1] );
            }
            _vgx_string_tuple_list_append_item( messages, key, msg );
            CStringDeleteList( &CSTR__parts );
          }
          CStringDelete( CSTR__msg );
        }
      }
    }
    free( trace );
  }
  return messages;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_disable_events( void ) {
  VXDURABLE_REGISTRY_INFO( 0x000, "Event Processor globally disabled" );
  g_allow_event_processor = false;
  return g_allow_event_processor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_enable_events( void ) {
  g_allow_event_processor = true;
  return g_allow_event_processor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_events_enabled( void ) {
  return g_allow_event_processor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_set_vector_type( vector_type_t vtype ) {
  int ret = 0;
  XTRY {
    // We are setting the type
    if( vtype != VECTOR_TYPE_NULL ) {
      // Type already set
      if( g_global_vector_type != VECTOR_TYPE_NULL ) {
        // Supplied type is different from already set type
        if( g_global_vector_type != vtype ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x001, "Cannot set vector type %02x, already initialized with vector type %02x", vtype, g_global_vector_type );
        }
      }
      // Must be euclidean or feature
      if( vtype == __VECTOR__MASK_EUCLIDEAN || vtype == __VECTOR__MASK_FEATURE ) {
        g_global_vector_type = vtype;
      }
      else {
        THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x002, "Invalid vector type %02x", vtype );
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



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void GraphFactory_unset_vector_type( void ) {
  g_global_vector_type = VECTOR_TYPE_NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_feature_vectors( void ) {
  return (g_global_vector_type & __VECTOR__MASK_FEATURE) != 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_euclidean_vectors( void ) {
  return (g_global_vector_type & __VECTOR__MASK_EUCLIDEAN) != 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_is_events_idle_deserialize( void ) {
  return g_force_events_idle_deserialize;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_is_readonly_deserialize( void ) {
  return g_force_readonly_deserialize;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_suspend_events( int timeout_ms ) {
  int ret = 0;
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        vgx_Graph_t *graph;
        vgx_Graph_t **cursor;

        // Count graphs
        int n_graphs = 0;
        cursor = graph_list;
        while( (graph = *cursor++) != NULL ) {
          ++n_graphs;
        }

        if( n_graphs ) {
          // Allocate state tracker
          vgx_Graph_t **tracker = calloc( n_graphs, sizeof( vgx_Graph_t* ) );
          if( tracker ) {
            // Suspend events for all graphs
            cursor = graph_list;
            int i = 0;
            while( ret == 0 && (graph = *cursor++) != NULL ) {
              GRAPH_LOCK( graph ) {
                // Readonly graph (EVP is paused)
                if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
                  // Reset the "resume EVP on leaving readonly mode" flag.
                  _vgx_readonly_suspend_EVP_CS( &graph->readonly, false );
                  tracker[i] = graph;
                }
                if( iGraphEvent.IsEnabled( graph ) ) {
                  GRAPH_SUSPEND_LOCK( graph ) {
                    vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( graph, timeout_ms );
                    if( iGraphEvent.NOCS.Disable( graph, &disable_budget ) < 0 ) {
                      ret = -1;
                    }
                    else {
                      tracker[i] = graph;
                    }
                  } GRAPH_RESUME_LOCK;
                }
              } GRAPH_RELEASE;
              ++i;
            }

            // Resume events for graphs that had events suspended if
            // we did not suspend events for all graphs. 
            if( ret < 0 ) {
              for( i=0; i<n_graphs; i++ ) {
                if( (graph = tracker[i]) != NULL ) {
                  GRAPH_LOCK( graph ) {
                    // Readonly graph (EVP is paused)
                    if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
                      // Set the "resume EVP on leaving readonly mode" flag.
                      _vgx_readonly_suspend_EVP_CS( &graph->readonly, true );
                    }
                    if( !iGraphEvent.IsEnabled( graph ) ) {
                      GRAPH_SUSPEND_LOCK( graph ) {
                        vgx_ExecutionTimingBudget_t enable_budget = _vgx_get_graph_execution_timing_budget( graph, 30000 );
                        iGraphEvent.NOCS.Enable( graph, &enable_budget );
                      } GRAPH_RESUME_LOCK;
                    }
                  } GRAPH_RELEASE;
                }
              }
            }

            free( tracker );
          }
          else {
            ret = -1;
          }
        }

        // Free graph list
        free( (void*)graph_list );
      }
      else {
        ret = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_events_resumable( void ) {
  int ret = 0;
  if( GraphFactory_is_initialized() ) {
    if( igraphfactory.EventsEnabled() ) {
      vgx_Graph_t **graph_list = NULL;
      REGISTRY_LOCK {
        // Get all graphs from registry
        if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
          // Resume events for all graphs
          vgx_Graph_t **cursor = graph_list;
          vgx_Graph_t *graph;
          while( ret == 0 && (graph = *cursor++) != NULL ) {
            if( iGraphEvent.IsReady( graph ) && !iGraphEvent.IsEnabled( graph ) ) {
              ret = 1;
            }
          }
          // Free graph list
          free( (void*)graph_list );
        }
        else {
          ret = -1;
        }
      } REGISTRY_RELEASE;
    }
  }
  else {
    ret = -1;
  }
  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_resume_events( void ) {
  int ret = 0;
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        // Resume events for all graphs
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        while( ret == 0 && (graph = *cursor++) != NULL ) {
          if( iGraphEvent.IsReady( graph ) && !iGraphEvent.IsEnabled( graph ) ) {
            vgx_ExecutionTimingBudget_t enable_budget = _vgx_get_graph_execution_timing_budget( graph, 30000 );
            if( iGraphEvent.NOCS.Enable( graph, &enable_budget ) < 0 ) {
              ret = -1;
            }
          }
        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        ret = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_set_all_readonly( int timeout_ms, vgx_AccessReason_t *reason ) {
  int ret = 0;
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        // Acquire readonly for all graphs
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        while( ret >= 0 && (graph = *cursor++) != NULL ) {
          ret = CALLABLE( graph )->advanced->AcquireGraphReadonly( graph, timeout_ms, false, reason );
        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        ret = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_count_all_readonly( void ) {
  int nRO = 0;
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        // Count number of readonly graphs
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        while( (graph = *cursor++) != NULL ) {
          if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) > 0 ) {
            ++nRO;
          }
        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        nRO = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    nRO = -1;
  }
  return nRO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_clear_all_readonly( void ) {
  int ret = 0;
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        // Release readonly for all graphs
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        while( ret == 0 && (graph = *cursor++) != NULL ) {
          if( CALLABLE( graph )->advanced->ReleaseGraphReadonly( graph ) < 0 ) {
            ret = -1;
          }
        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        ret = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_is_initialized( void ) {
  return g_graph_registry != NULL ? true : false;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_initialize( vgx_context_t *context, vector_type_t global_vtype, bool events_idle, bool readonly, vgx_StringTupleList_t **messages ) {

  // Use a default root directory if not defined by context
  if( strlen( context->sysroot ) == 0 ) {
    strcpy( context->sysroot, "vgxroot" );
  }

  CString_t *CSTR__system_root = NULL;
  int retcode = 0;

  vgx_StringTupleList_t *info = _vgx_new_empty_string_tuple_list();
        
  cxlib_trace_reset();

  XTRY {
    if( info == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x591 );
    }

    // Ensure root dir exists
    if( !dir_exists( context->sysroot ) ) {
      if( create_dirs( context->sysroot ) < 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x592, "Invalid directory: \"%s\"", context->sysroot );
      }
    }

    // System root
    if( (CSTR__system_root = CStringNew( context->sysroot )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x593, "Memory error" );
    }

    // Validate class definitions
    if( __validate_class_definitions_ROPEN( CSTR__system_root ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x594, "Internal error: Invalid class definitions" );
    }

    if( __vector_settings_ROPEN( CSTR__system_root, global_vtype ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x595 );
    }

    g_force_events_idle_deserialize = events_idle;
    g_force_readonly_deserialize = readonly;

    // Initialize the graph registry
    if( (retcode = __initialize_graph_registry_ROPEN( CSTR__system_root )) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x596, "Registry error: Failed to initialize" );
    }
    
    // Already initialized
    else if( retcode == 0 ) {
      VXDURABLE_REGISTRY_INFO( 0x597, "Graph registry already initialized" );
    }

    // CREATE SYSTEM GRAPH
    if( !iSystem.Ready() ) {
      if( iSystem.Create() < 0 ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x598 );
      }
    }

    // Generate a suitably unique instance label
    uint64_t system_id = iSystem.GetSystemGraph()->instance_id;
    __lfsr_init( __GET_CURRENT_NANOSECOND_TICK() );
    uint64_t r63 = rand63();
    char *ip = cxgetip( NULL );
    int64_t g_phys = 0;
    int64_t proc_phys = 0;
    int64_t r20 = rand20();
    int64_t rn = 0;
    char *rmem = malloc( 1 + r20 );
    if( rmem ) {
      const char *rend = rmem + r20;
      char *r = rmem;
      while( r < rend ) {
        rn += *r++;
      }
    }
    get_system_physical_memory( &g_phys, NULL, &proc_phys );
    char buffer[128] = {0};
    snprintf( buffer, 127, "%llu %llu %s %lld %lld %lld", system_id, r63, ip ? ip : "0", rn, g_phys, proc_phys );
    free(ip);
    free(rmem);
    g_instance_unique_label = obid_from_string( buffer );


  }
  XCATCH( errcode ) {
    retcode = -1;
    _vgx_string_tuple_list_append_item( info, "system_root", context->sysroot );
  }
  XFINALLY {
    // Clean up if needed
    if( CSTR__system_root ) {
      CStringDelete( CSTR__system_root );
    }
    // Return captured messages if wanted
    if( messages ) {
      *messages = __append_trace_to_messages( info );
    }
    // Delete any uncaptured messages
    else {
      _vgx_delete_string_tuple_list( &info );
    }
  }

  cxlib_trace_disable();

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t GraphFactory_persist( void ) {
  int64_t ret = -1;
  if( g_graph_registry ) {
    REGISTRY_LOCK {
      framehash_vtable_t *imap = CALLABLE( g_graph_registry );
      ret = imap->BulkSerialize( g_graph_registry, true );
    } REGISTRY_RELEASE;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_register_graph( const vgx_Graph_t *graph, vgx_StringTupleList_t **messages ) {
  return __set_graph_ROPEN( graph, messages );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_has_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name ) {
  bool has_graph = false;

  REGISTRY_LOCK {
    if( __retrieve_graph_by_path_and_name_RCS( CSTR__graph_path, CSTR__graph_name ) ) {
      has_graph = true;
    }
  } REGISTRY_RELEASE;

  return has_graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * GraphFactory_new_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages ) {
  CString_t *CSTR__computed_path = NULL;
  vgx_Graph_t *graph = NULL;
  if( GraphInfo_new_name_and_path( CSTR__graph_path, CSTR__graph_name, &CSTR__computed_path, NULL, messages ) == 0 ) {
    if( (graph = __new_graph_ROPEN( CSTR__computed_path, CSTR__graph_name, local, messages )) != NULL ) {
      GRAPH_LOCK( graph ) {
        graph->owner_threadid = GET_CURRENT_THREAD_ID();
        graph->recursion_count = 1;
      } GRAPH_RELEASE;
    }
    CStringDelete( CSTR__computed_path );
  }
  return graph;
}



// TODO: Add CSTR__error param
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * GraphFactory_open_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages, int timeout_ms ) {

  vgx_Graph_t *graph = NULL;
  int acquired = 0;

  // Check that registry is initialized
  if( !GraphFactory_is_initialized() ) {
    return NULL;
  }

  cxlib_trace_reset();

  REGISTRY_LOCK {

    // Loop until graph can be acquired by this thread
    BEGIN_TIME_LIMITED_WHILE( acquired == 0, timeout_ms, NULL ) {

      // Check graph registry for the named graph.
      // If it does not exist in the graph registry, construct
      // a new graph and enter it into the graph registry.

      // No graph in registry, construct a new graph - current thread becomes owner
      if( (graph = __retrieve_graph_by_path_and_name_RCS( CSTR__graph_path, CSTR__graph_name )) == NULL ) {
        // Construct a new graph
        REGISTRY_SUSPEND_LOCK {
          if( (graph = GraphFactory_new_graph( CSTR__graph_path, CSTR__graph_name, local, messages )) == NULL ) {
            acquired = -1; // error
          }
          else {
            acquired = 1; // ok
          }
        } REGISTRY_RESUME_LOCK;
      }
      // Graph is available - current thread becomes owner
      else {
        if( (acquired = __acquire_graph_RCS( graph, messages )) < 0 ) {
          // Error
          graph = NULL;
        }
      }
    } END_TIME_LIMITED_WHILE;

  } REGISTRY_RELEASE;


  if( acquired < 1 ) {
    graph = NULL;
  }


  if( graph ) {
    // Capture (non-system) graph creation
    if( !iSystem.IsSystemGraph( graph ) && iSystem.CaptureCreateGraph( graph, NULL ) < 0 ) {
      REGISTRY_LOCK {
        __del_graph_RCS( &graph, false ); // should call graph destructor and set graph to NULL
      } REGISTRY_RELEASE;
      graph = NULL; // but be sure
      *messages = __append_trace_to_messages( NULL );
    }
  }
  else if( messages != NULL && *messages == NULL ) {
    if( (*messages = _vgx_new_empty_string_tuple_list()) != NULL ) {
      _vgx_string_tuple_list_append_item( *messages, "trace", "Could not acquire graph" );
    }
  }


  return graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * GraphFactory_get_graph_by_obid( const objectid_t *obid ) {
  vgx_Graph_t *graph = NULL;
  REGISTRY_LOCK {
    graph = __get_graph_by_obid_RCS( obid );
  } REGISTRY_RELEASE;
  return graph;
}



/*******************************************************************//**
 *
 * Returns: 1 if graph deleted
 *          0 if graph does not exist
 *         -1 if graph is in use (by any thread, including ourselves)
 *
 ***********************************************************************
 */
static int GraphFactory_del_graph_by_obid( const objectid_t *obid ) {

  int retcode = 0;

  REGISTRY_LOCK {
    vgx_Graph_t *graph = __get_graph_by_obid_RCS( obid );

    if( graph ) {
      bool idle;
      GRAPH_LOCK( graph ) {
        idle = _vgx_graph_owner_CS( graph ) == 0;
      } GRAPH_RELEASE;
      // Remove graph from registry if no thread owns it
      if( idle ) {
        retcode = __del_graph_RCS( &graph, false );
      }
      // Error if any thread owns graph
      else {
        // Can't delete open graph
        retcode = -1;
        graph = NULL;
      }
    }

  } REGISTRY_RELEASE;

  return retcode;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __unlock_vertices( vgx_Graph_t *self ) {
  GRAPH_LOCK( self ) {
    // Close all open vertices
    int done = 0;
    // Keep unlocking vertices until tracker counts go to zero
    while( !done ) {
      VertexAndInt64List_t *writable = NULL;
      VertexAndInt64List_t *readonly = NULL;
      // Get open vertices
      uint32_t owner = _vgx_graph_owner_CS( self );
      if( _vxgraph_tracker__get_open_vertices_CS( self, owner, &readonly, &writable ) < 0 ) {
        done = 1; // error
      }
      else {
        // No open vertices
        if( (writable == NULL || CALLABLE(writable)->Length(writable) == 0 ) 
            && 
            (readonly == NULL || CALLABLE(readonly)->Length(readonly) == 0 ) 
          ) 
        {
          done = 1;
        }
        // Open vertices exist
        else {
          // Release all writable (once)
          if( writable ) {
            int64_t n_writable = CALLABLE(writable)->Length(writable);
            for( int64_t n = 0; n < n_writable; n++ ) {
              VertexAndInt64_t entry;
              CALLABLE(writable)->Get( writable, n, &entry.m128 );
              _vxgraph_state__unlock_vertex_CS_LCK( self, &entry.vertex, VGX_VERTEX_RECORD_ALL );
            }
            COMLIB_OBJECT_DESTROY( writable );
          }
          // Release all readonly (once)
          if( readonly ) {
            int64_t n_readonly = CALLABLE(readonly)->Length(readonly);
            for( int64_t n = 0; n < n_readonly; n++ ) {
              VertexAndInt64_t entry;
              CALLABLE(readonly)->Get( readonly, n, &entry.m128 );
              _vxgraph_state__unlock_vertex_CS_LCK( self, &entry.vertex, VGX_VERTEX_RECORD_ALL );
            }
            COMLIB_OBJECT_DESTROY( readonly );
          }
        }
      }
    } 
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 * Returns:  0 when graph has been closed
 *          >0 when current thread still owns graph recursively
 *          <0 when current thread not allowed to close graph
 ***********************************************************************
 */
static int GraphFactory_close_graph( vgx_Graph_t **graph, uint32_t *owner ) {

  int retcode;

  if( graph == NULL || *graph == NULL ) {
    return -1;
  }

  REGISTRY_LOCK {

    if( (retcode = __release_graph_RCS( *graph, owner )) >= 0 ) {
      *graph = NULL;
    }

  } REGISTRY_RELEASE;

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_delete_graph( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, int timeout_ms, bool erase, CString_t **CSTR__reason ) {

  int retcode = -1;
  bool trying = true;

  // Check that registry is initialized
  if( !GraphFactory_is_initialized() ) {
    return -1;
  }

  // Default path same as registry
  if( CSTR__graph_path == NULL ) {
    CSTR__graph_path = GraphFactory_system_root();
  }

  REGISTRY_LOCK {
    vgx_Graph_t *graph = NULL;

    // Loop until graph can be acquired by this thread
    BEGIN_TIME_LIMITED_WHILE( graph == NULL && trying, timeout_ms, NULL ) {

      // Check graph registry for the named graph.
      // If it does not exist we can't delete it.
      graph = __retrieve_graph_by_path_and_name_RCS( CSTR__graph_path, CSTR__graph_name );

      // No graph in registry
      if( graph == NULL ) {
        retcode = 0;
        trying = false;
      }
      else {
        uint32_t owner;
        GRAPH_LOCK( graph ) {
          owner = _vgx_graph_owner_CS( graph );
        } GRAPH_RELEASE;
        // Remove graph from registry if no thread owns it
        if( owner == 0 ) {
          retcode = __del_graph_RCS( &graph, erase );
          trying = false;
        }
        // Error if current thread owns this graph
        else if( owner == GET_CURRENT_THREAD_ID() ) {
          // Can't delete graph when we have it open
          retcode = -1;
          trying = false;
          if( CSTR__reason ) {
            *CSTR__reason = CStringNew( "Graph object is in use. Close graph and try again." );
          }
        }
        // Another thread owns graph, we have to wait
        else {
          graph = NULL;
          // Release api lock and wait for availability signal (max 100 ms)
          WAIT_FOR_REGISTRY_AVAILABLE( 100 );
        }
      }
    } END_TIME_LIMITED_WHILE;

  } REGISTRY_RELEASE;
 
  // Error, and we want to return a reason but no reason has been set:  Timeout occurred
  if( retcode < 0 && CSTR__reason && *CSTR__reason == NULL ) {
    *CSTR__reason = CStringNew( "Graph object is in use by another thread" );
  }

  return retcode;
}



/*******************************************************************//**
 *
 * NOTE: Caller owns returned list
 ***********************************************************************
 */
static const vgx_Graph_t ** GraphFactory_list_graphs( int64_t *n_graphs ) {
  const vgx_Graph_t **graphs;
  REGISTRY_LOCK {
    graphs = __list_graphs_RCS( n_graphs );
  } REGISTRY_RELEASE;
  return graphs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_sync_all_graphs( bool hard, int timeout_ms, CString_t **CSTR__error ) {
  if( !GraphFactory_is_initialized() ) {
    return -1;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  int ret = 0;
  int err = 0;

  GRAPH_LOCK( SYSTEM ) {
    if( !SYSTEM->OP.system.state_CS.flags.sync_in_progress ) {
      SYSTEM->OP.system.state_CS.flags.sync_in_progress = true;
      SYSTEM->OP.system.progress_CS->state = VGX_OPERATION_SYSTEM_STATE__SYNC_Begin;
    }
    else {
      ret = -1;
      __set_error_string( CSTR__error, "Synchronization already in progress" );
    }
  } GRAPH_RELEASE;

  if( ret < 0 ) {
    return ret;
  }

  int tx_input_suspended_here = 0;

  XTRY {

    // Suspend transaction input if consumer service is running and not already suspended
    if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
      if( iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) == 0 ) {
        if( (tx_input_suspended_here = iOperation.System_OPEN.ConsumerService.SuspendExecution( SYSTEM, timeout_ms )) < 1 ) {
          __set_error_string( CSTR__error, "Failed to suspend transaction input: Sync cannot proceed" );
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
        }
      }
    }


    // -------------------------
    // Hard sync: Clear Registry
    // -------------------------
    if( hard ) {
      VXDURABLE_REGISTRY_INFO( 0x002, "SYSTEM HARD SYNC: Start" );
      if( iOperation.System_SYS_CS.ClearRegistry( SYSTEM ) < 1 ) {
        __set_error_string( CSTR__error, "Failed to initiate hard sync: Clear Registry could not be performed" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }
    }
    else {
      VXDURABLE_REGISTRY_INFO( 0x004, "SYSTEM SYNC: Start" );
    }

    // Sync system properties
    if( SYSTEM->system_sync_callback( SYSTEM, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Sync all user graphs
    GRAPH_FACTORY_ACQUIRE {
      const vgx_Graph_t **graphs = NULL;
      if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) { 
        vgx_Graph_t **cursor = (vgx_Graph_t**)graphs;
        vgx_Graph_t *graph;

        int64_t obj_total = 0;
        while( (graph = *cursor++) != NULL ) {
          GRAPH_LOCK( graph ) {
            obj_total += GraphOrder(graph);
            obj_total += GraphSize(graph);
            if( !igraphfactory.EuclideanVectors() ) {
              obj_total += iEnumerator_CS.Dimension.Count( graph->similarity );
            }
            obj_total += iEnumerator_CS.Property.Key.Count(graph);
            obj_total += iEnumerator_CS.Property.Value.Count(graph);
          } GRAPH_RELEASE;
        }

        GRAPH_LOCK( SYSTEM ) {
          SYSTEM->OP.system.progress_CS->obj_counter = 0;
          SYSTEM->OP.system.progress_CS->obj_total = obj_total;
        } GRAPH_RELEASE;

        cursor = (vgx_Graph_t**)graphs;
        while( err == 0 && (graph = *cursor++) != NULL ) {
          // Perform sync
          if( iOperation.Graph_OPEN.Sync( graph, hard, timeout_ms, CSTR__error ) != 0 ) {
            --err;
          }
        }

        free( (void*)graphs );
        graphs = NULL;
      }
    } GRAPH_FACTORY_RELEASE;

    if( err < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }

    VXDURABLE_REGISTRY_INFO( 0x004, "SYSTEM SYNC: Complete" );

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( tx_input_suspended_here ) {
      if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
        if( iOperation.System_OPEN.ConsumerService.ResumeExecution( SYSTEM, timeout_ms ) != 1 ) {
          CRITICAL( 0x005, "Failed to resume transaction consumer service after sync" );
        }
      }
    }
  }

  GRAPH_LOCK( SYSTEM ) {
    SYSTEM->OP.system.state_CS.flags.sync_in_progress = false;
    SYSTEM->OP.system.state_CS.flags.request_cancel_sync = false;
    SYSTEM->OP.system.progress_CS->state = VGX_OPERATION_SYSTEM_STATE__SYNC_End;
  } GRAPH_RELEASE;


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_cancel_sync( void ) {
  int ret = 0;

  if( !GraphFactory_is_initialized() ) {
    return -1;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  GRAPH_LOCK( SYSTEM ) {
    // Set flag to request cancel
    if( SYSTEM->OP.system.state_CS.flags.sync_in_progress ) {
      ret = 1;
      SYSTEM->OP.system.state_CS.flags.request_cancel_sync = true;
    }
    // Wait for sync to terminate
    BEGIN_TIME_LIMITED_WHILE( SYSTEM->OP.system.state_CS.flags.sync_in_progress, 60000, NULL ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        sleep_milliseconds( 1000 );
      } GRAPH_RESUME_LOCK;
    } END_TIME_LIMITED_WHILE;
    // Sync termination timed out
    if( SYSTEM->OP.system.state_CS.flags.sync_in_progress ) {
      ret = -1;
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool GraphFactory_is_sync_active( void ) {
  bool active = false;

  if( !GraphFactory_is_initialized() ) {
    return false;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return false;
  }

  GRAPH_LOCK( SYSTEM ) {
    if( SYSTEM->OP.system.state_CS.flags.sync_in_progress ) {
      active = true;
    }
  } GRAPH_RELEASE;

  return active;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t GraphFactory_unique_label( void ) {
  return g_instance_unique_label;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t GraphFactory_fingerprint( void ) {

  objectid_t obid = {0};

  char digest[33] = {0};
  char buffer[512] = {0};

#define DIGEST_ADD_STRING( String ) \
  do {                              \
    snprintf( buffer, 511, "%s%s", (String), idtostr( digest, &obid ) ); \
    obid = obid_from_string( buffer );     \
  } WHILE_ZERO

#define DIGEST_ADD_INT( Integer )             \
  do {                                        \
    char _buf[17] = {0};                      \
    snprintf( _buf, 16, "%llx", (Integer) );  \
    DIGEST_ADD_STRING( _buf );                \
  } WHILE_ZERO


  if( !GraphFactory_is_initialized() ) {
    return obid;
  }

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return obid;
  }

  DIGEST_ADD_STRING( "init" );

  int64_t tms = __MILLISECONDS_SINCE_1970();

  GRAPH_FACTORY_ACQUIRE {
    vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
    if( graphs ) {
      vgx_Graph_t **cursor = graphs;
      vgx_Graph_t *graph;
      while( (graph = *cursor++) != NULL ) {
        // Graph name
        const char *name = CStringValue( CALLABLE( graph )->Name( graph ) );
        DIGEST_ADD_STRING( name );

        GRAPH_LOCK( graph ) {
          
          int64_t order = GraphOrder( graph );
          int64_t size = GraphSize( graph );
          int64_t nproperties = GraphPropCount( graph );
          int64_t nvectors = GraphVectorCount( graph );
          
          // Order
          DIGEST_ADD_INT( order );
          // Size
          DIGEST_ADD_INT( size );
          // Property count
          DIGEST_ADD_INT( nproperties );
          // Vector count
          DIGEST_ADD_INT( nvectors );

          // Enumerators
          if( graph->similarity && graph->similarity->parent == graph ) {
            // Relationship count
            DIGEST_ADD_INT( iEnumerator_CS.Relationship.Count( graph ) );
            // Vertex type count
            int64_t effective_vtype_cnt = iEnumerator_CS.VertexType.Count( graph ) - (int)iEnumerator_CS.VertexType.ExistsEnum( graph, VERTEX_TYPE_ENUMERATION_LOCKOBJECT );
            DIGEST_ADD_INT( effective_vtype_cnt );
            // Dimension count
            if( !igraphfactory.EuclideanVectors() ) {
              DIGEST_ADD_INT( iEnumerator_CS.Dimension.Count( graph->similarity ) );
            }
          }
          // Key enumeration count
          DIGEST_ADD_INT( iEnumerator_CS.Property.Key.Count( graph ) );
          // Property string value enumeration count
          DIGEST_ADD_INT( iEnumerator_CS.Property.Value.Count( graph ) );
        } GRAPH_RELEASE;
      }
      free( (void*)graphs );
    }
    if( !idmatch( &g_fingerprint, &obid ) ) {
      g_fingerprint = obid;
      g_last_fingerprint_change_tms = tms;
    }
  } GRAPH_FACTORY_RELEASE;

  return obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t GraphFactory_fingerprint_age( void ) {
  int64_t age = 0;
  int64_t tms = __MILLISECONDS_SINCE_1970();
  GRAPH_FACTORY_ACQUIRE {
    age = tms - g_last_fingerprint_change_tms;
  } GRAPH_FACTORY_RELEASE;
  return age;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_earliest_durable_transaction( objectid_t *txid, int64_t *sn, int64_t *ts, int *n_serializing ) {
  int ret = 0;
  idunset( txid );
  *sn = -1;
  if( n_serializing ) {
    *n_serializing = 0;
  }
  if( GraphFactory_is_initialized() ) {
    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        while( (graph = *cursor++) != NULL ) {
          GRAPH_LOCK( graph ) {
            // Get durability point
            if( *sn < 0 || graph->durable_tx_serial < *sn ) {
              *sn = graph->durable_tx_serial;
              idcpy( txid, &graph->durable_tx_id );
              if( ts ) {
                *ts = graph->persisted_ts;
              }
            }
            // Count number of serializing graphs
            if( n_serializing && _vgx_is_serializing_CS( &graph->readonly ) ) {
              ++(*n_serializing);
            }
          } GRAPH_RELEASE;
        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        ret = -1;
      }
    } REGISTRY_RELEASE;
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void GraphFactory_shutdown( void ) {
  if( GraphFactory_is_initialized() ) {

    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    if( SYSTEM ) {
      int64_t nw;
      if( (nw = iOperation.WritableVertices()) > 0 ) {
        VXDURABLE_REGISTRY_WARNING( 0x5A1, "Unclean shutdown with %lld writable vertices.", nw );
      }

      // System parser
      bool done = false;
      BEGIN_TIME_LIMITED_WHILE( !done, 10000, NULL ) {
        if( iOperation.Parser.Pending( &SYSTEM->OP.parser ) < 1 ) {
          done = true;
        }
      } END_TIME_LIMITED_WHILE;

      // VGX Server
      iVGXServer.Service.StopDelete( SYSTEM );

      // Destroy VGX Server Artifacts
      iVGXServer.Artifacts.Destroy();

      // Destroy VGX Server Resources
      iVGXServer.Resource.Clear();

      // Unsubscribe if not durable
      if( !iOperation.System_OPEN.ConsumerService.IsDurable( SYSTEM ) ) {
        iOperation.System_OPEN.ConsumerService.Unsubscribe( SYSTEM );
      }

      // Stop consumer service
      iOperation.System_OPEN.ConsumerService.Stop( SYSTEM );

      GRAPH_LOCK( SYSTEM ) {

        // Stop the parser
        iOperation.Parser.Destroy( &SYSTEM->OP.parser );
        
      } GRAPH_RELEASE;
    }

    vgx_Graph_t **graph_list = NULL;
    REGISTRY_LOCK {
      // Get all graphs from registry
      if( (graph_list = (vgx_Graph_t**)__list_graphs_RCS( NULL )) != NULL ) {
        // Close all graphs
        vgx_Graph_t **cursor = graph_list;
        vgx_Graph_t *graph;
        uint32_t owner;
        while( (graph = *cursor++) != NULL ) {
          GRAPH_LOCK( graph ) {
            owner = _vgx_graph_owner_CS( graph );
          } GRAPH_RELEASE;
          if( owner != 0 ) {
            // Current thread steals ownership of graph
            graph->owner_threadid = GET_CURRENT_THREAD_ID();

            // Close graph until no more references
            bool graph_open = true;
            while( graph_open ) {
              vgx_Graph_t *graph_copy = graph;
              int close_retcode = GraphFactory_close_graph( &graph_copy, &owner );
              if( close_retcode == 0 ) {
                graph_open = false;
              }
              else if( close_retcode < 0 ) {
                VXDURABLE_REGISTRY_REASON( 0x5A2, "Cannot close graph '%s' cleanly", CStringValue( CALLABLE( graph )->Name( graph ) ) );
                break;
              }
            }
          }

          // External destructor callback
          VERBOSE( 0x000, "CALLING DESTRUCTOR CALLBACK %p FOR: %s", graph->destructor_callback_hook, CStringValue(graph->CSTR__fullpath) );
          if( graph->destructor_callback_hook ) {
            graph->destructor_callback_hook( graph, graph->external_owner_object );
            graph->destructor_callback_hook = NULL;
          }

        }
        // Free graph list
        free( (void*)graph_list );
      }
      else {
        VXDURABLE_REGISTRY_VERBOSE( 0x5A3, "No graph registry" );
      }

    } REGISTRY_RELEASE;

    // Destroy the registry
    if( __destroy_graph_registry_ROPEN() != 1 ) {
      VXDURABLE_REGISTRY_REASON( 0x5A4, "Unclean graph registry destruction" );
    }

    igraphfactory.UnsetVectorType();
  }
}



typedef struct __s_vgx_classdefs {
  QWORD start[4];
  int64_t ts_created;
  char dir_created[ 1024 ];
  char version_created[ 1024 ];
  object_classname_t classnames[ COMLIB_MAX_CLASS ];
  QWORD end[4];
  QWORD term;
} __vgx_classdefs;



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static __vgx_classdefs * __get_vgx_classdefs( void ) {
  __vgx_classdefs *classdefs = calloc( 1, sizeof( __vgx_classdefs ) );
  if( classdefs ) {
    memset( classdefs->start, 0xFF, sizeof( classdefs->start ) );
    memset( classdefs->end, 0xFF, sizeof( classdefs->end ) );
    memcpy( classdefs->classnames, OBJECT_CLASSNAME, sizeof( OBJECT_CLASSNAME ) );
  }
  return classdefs;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int __validate_class_definitions_ROPEN( const CString_t *CSTR__system_root ) {
  int ret = 0;
  const char *root = CStringValue( CSTR__system_root );
  const char registry_path[] = VGX_PATHDEF_REGISTRY;
  const char fname[] = VGX_PATHDEF_CLASSDEFS;

  CString_t *CSTR__all_dirs = NULL;
  CString_t *CSTR__fullpath = NULL;

  FILE *file = NULL;
  __vgx_classdefs *classdefs = NULL; 
  __vgx_classdefs *classdefs_found = NULL;

  XTRY {
    if( (CSTR__all_dirs = CStringNewFormat( "%s/%s", root, registry_path )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5B1 );
    }

    if( (CSTR__fullpath = CStringNewFormat( "%s/%s", CStringValue( CSTR__all_dirs ), fname )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5B2 );
    }

    // Proceed if system root has been created
    if( dir_exists( root ) ) {
      const char *all_dirs = CStringValue( CSTR__all_dirs );
      const char *fullpath = CStringValue( CSTR__fullpath );

      if( create_dirs( all_dirs ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5B3 );
      }

      if( (classdefs = __get_vgx_classdefs()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x5B4 );
      }
      if( (classdefs_found = calloc( 1, sizeof( __vgx_classdefs ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x5B5 );
      }

      // Previous file exists, verify class definitions match
      if( file_exists( fullpath ) ) {
        VXDURABLE_REGISTRY_VERBOSE( 0x5B6, "Validating class definitions." );
        // Open file and read data
        if( (file = CX_FOPEN( fullpath, "rb" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5B7 );
        }
        if( CX_FREAD( classdefs_found, sizeof(__vgx_classdefs ), 1, file ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5B8 );
        }
        // Verify valid file start/end
        if( memcmp( classdefs_found->start, classdefs->start, sizeof(classdefs->start) ) ||
            memcmp( classdefs_found->end, classdefs->end, sizeof(classdefs->end) ) )
        {
          THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x5B9 );
        }
        // Display time created
        char tmbuf[512];
        time_t ts = classdefs_found->ts_created / 1000;
        struct tm *_tm = localtime( &ts );
        if( _tm == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x5BA );
        }
        strftime( tmbuf, 20, "%Y-%m-%d %H:%M:%S", _tm );
        int64_t delta = (__MILLISECONDS_SINCE_1970() - classdefs_found->ts_created) / 1000;
        CString_t *CSTR__delta = CStringNewFormat( "%dd:%dh:%dm:%ds", delta/86400, (delta%86400)/3600, (delta%3600)/60, delta%60 );
        if( CSTR__delta ) {
          VXDURABLE_REGISTRY_VERBOSE( 0x5BB, "Created: %s (%s ago)", tmbuf, CStringValue( CSTR__delta ) );
          iString.Discard( &CSTR__delta );
        }
        // Display original directory
        VXDURABLE_REGISTRY_VERBOSE( 0x5BC, "Original directory: %s", classdefs_found->dir_created );
        // Check class definitions in file against expected definitions
        for( int c=0; c<COMLIB_MAX_CLASS; c++ ) {
          // A class we care about
          if( strlen(classdefs->classnames[c].classname) ) {
            // Verify same definition
            if( !CharsEqualsConst(classdefs_found->classnames[c].classname, classdefs->classnames[c].classname) ) {
              // ERROR: Incompatible class definitions
              VXDURABLE_REGISTRY_REASON( 0x5BD, "Found version info: %s", classdefs_found->version_created );
              CString_t *CSTR__current_version = GraphInfo_version( 2 );
              if( CSTR__current_version ) {
                VXDURABLE_REGISTRY_REASON( 0x5BE, "Current version info: %s", CStringValue( CSTR__current_version ) );
                iString.Discard( &CSTR__current_version );
              }
              THROW_ERROR_MESSAGE( CXLIB_ERR_FORMAT, 0x5BF, "Incompatible core class definitions detected. Cannot continue." );
            }
          }
        }
        // Success if here.
      }
      // No previous file, write file to disk
      else {
        VXDURABLE_REGISTRY_VERBOSE( 0x5C0, "Initializing class definitions." );
        // timestamp
        classdefs->ts_created = __MILLISECONDS_SINCE_1970();
        // original system root
        strncpy( classdefs->dir_created, root, 1023 );
        // version
        CString_t *CSTR__version = GraphInfo_version( 2 );
        if( CSTR__version ) {
          strncpy( classdefs->version_created, CStringValue( CSTR__version ), 1023 );
          iString.Discard( &CSTR__version );
        }
        // Open file and write data
        if( (file = CX_FOPEN( fullpath, "wb" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5C1 );
        }
        if( CX_FWRITE( classdefs, sizeof( __vgx_classdefs ), 1, file ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5C2 );
        }
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( file ) {
      CX_FCLOSE( file );
    }
    if( classdefs ) {
      free( classdefs );
    }
    if( classdefs_found ) {
      free( classdefs_found );
    }
    iString.Discard( &CSTR__fullpath );
    iString.Discard( &CSTR__all_dirs );
  }

  return ret;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int __vector_settings_ROPEN( const CString_t *CSTR__system_root, vector_type_t global_vtype ) {
  int ret = 0;
  const char *root = CStringValue( CSTR__system_root );
  const char registry_path[] = VGX_PATHDEF_REGISTRY;
  const char fname[] = VGX_PATHDEF_VECTOR_SETTINGS;

  CString_t *CSTR__all_dirs = NULL;
  CString_t *CSTR__fullpath = NULL;

  FILE *vxvector = NULL;

  XTRY {
    if( (CSTR__all_dirs = CStringNewFormat( "%s/%s", root, registry_path )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5C6 );
    }

    if( (CSTR__fullpath = CStringNewFormat( "%s/%s", CStringValue( CSTR__all_dirs), fname )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5C7 );
    }

    // Set the specified vector type (if given)
    if( igraphfactory.SetVectorType( global_vtype ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5C8 );
    }

    // Proceed if system root has been created
    if( dir_exists( root ) ) {
      const char *all_dirs = CStringValue( CSTR__all_dirs );
      const char *fullpath = CStringValue( CSTR__fullpath );

      if( create_dirs( all_dirs ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5C9 );
      }

      // Vector settings file exists
      if( file_exists( fullpath ) ) {
        // Open file and read data
        if( (vxvector = CX_FOPEN( fullpath, "rb" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5CA );
        }
        vector_type_t vtype;
        if( CX_FREAD( &vtype, sizeof( vector_type_t ), 1, vxvector ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5CB );
        }

        // This will either set vtype to system's previous setting (if not specifically selected) or validate match
        if( igraphfactory.SetVectorType( vtype ) < 0 ) {
          const char *prev_mode = vtype == __VECTOR__MASK_EUCLIDEAN ? "euclidean" : vtype == __VECTOR__MASK_FEATURE ? "feature" : "invalid";
          const char *this_mode = igraphfactory.EuclideanVectors() ? "euclidean" : igraphfactory.FeatureVectors() ? "feature": "invalid";
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5CC, "Cannot switch vector mode from '%s' to '%s' for existing system", prev_mode, this_mode );
        }

      }
      // No previous vector settings, write current settings to file
      else {
        // No vector type selected on first initialization, default euclidean
        if( global_vtype == VECTOR_TYPE_NULL ) {
          global_vtype = __VECTOR__MASK_EUCLIDEAN;
          igraphfactory.SetVectorType( global_vtype );
        }

        // Open file and write data
        if( (vxvector = CX_FOPEN( fullpath, "wb" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5CD );
        }

        if( CX_FWRITE( &global_vtype, sizeof( vector_type_t ), 1, vxvector ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x5CE );
        }

      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( vxvector ) {
      CX_FCLOSE( vxvector );
    }
    iString.Discard( &CSTR__fullpath );
    iString.Discard( &CSTR__all_dirs );
  }

  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1:  Initialized OK
 *          0:  Already initialized
 *         -1:  Initialization ERROR
 * 
 ***********************************************************************
 */
static int __initialize_graph_registry_ROPEN( const CString_t *CSTR__system_root ) {
  int ret = 0;

  if( !g_registry_init && ++g_registry_init == 1 ) {
    INIT_CRITICAL_SECTION( &g_registry_lock.lock );
    INIT_CONDITION_VARIABLE( &g_registry_graph_availability.cond );
  }

  CString_t *CSTR__registry = NULL;

  REGISTRY_LOCK {
    XTRY {
      // Check if already initialized
      if( g_graph_registry ) {
        XBREAK; // no action
      }

      iString.Discard( &CSTR__g_system_root );

      if( (CSTR__g_system_root = iString.Clone( CSTR__system_root )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x5D1 );
      }

      const char *root = CStringValue( CSTR__g_system_root );

      if( (CSTR__registry = CStringNewFormat( "%s/%s", root, VGX_PATHDEF_REGISTRY )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x5D2 );
      }

      // Prepare to construct registry
      framehash_constructor_args_t registry_args = FRAMEHASH_DEFAULT_ARGS;
      registry_args.param.order         = 0;
      registry_args.param.synchronized  = true;
      registry_args.param.cache_depth   = 1;
      registry_args.dirpath             = CStringValue( CSTR__registry );    // directory on disk where map will be persisted
      registry_args.name                = VGX_PATHDEF_REGISTRY_PREFIX;

      objectid_t registry_id = smartstrtoid( "VGX_Graph_Registry", -1 );

      VXDURABLE_REGISTRY_VERBOSE( 0x5D3, "Creating graph registry." );

      // Construct registry
      if( (g_graph_registry = COMLIB_OBJECT_NEW( framehash_t, &registry_id, &registry_args )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D4 );
      }

      VXDURABLE_REGISTRY_VERBOSE( 0x5D5, "Graph registry: '%s'", CStringValue( CALLABLE(g_graph_registry)->Masterpath(g_graph_registry) ) );

      // ok, we have a new registry
      ret = 1;
    }
    XCATCH( errcode ) {
      iString.Discard( &CSTR__g_system_root );
      ret = -1;
    }
    XFINALLY {
      iString.Discard( &CSTR__registry );
    }
  } REGISTRY_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __destroy_graph_registry_ROPEN( void ) {

  if( g_graph_registry != NULL ) {

    REGISTRY_LOCK {

      if( !__wait_for_writable_registry_RCS( 30000 ) ) {
        VXDURABLE_REGISTRY_REASON( 0x5D6, "Registry busy (readonly), cannot destroy." );
      }
      else {
        // Controlled destruction of graphs
        vgx_Graph_t **graphs = (vgx_Graph_t**)__list_graphs_RCS( NULL );

        if( graphs ) {
          vgx_Graph_t **cursor = graphs;
          vgx_Graph_t *graph = NULL;
          char path[256] = {0};

          while( (graph = *cursor++) != NULL ) {
            strncpy( path, CALLABLE( graph )->FullPath( graph ), 255 );
            if( __remove_and_destroy_graph_RCS( &graph ) == 1 ) {
              VXDURABLE_REGISTRY_INFO( 0x5D7, "Graph %s was destroyed and removed from the registry", path );
            }
            else {
              VXDURABLE_REGISTRY_REASON( 0x5D8, "Graph %s could not be removed from the registry and was not destroyed", path );
            }
          }
          free( graphs );
        }

        REGISTRY_SUSPEND_LOCK {
          // Destroy any previous system graph
          if( iSystem.Ready() ) {
            iSystem.Destroy();
          }
        } REGISTRY_RESUME_LOCK;


        // Registry
        COMLIB_OBJECT_DESTROY( g_graph_registry );
        g_graph_registry = NULL;

        // Path string
        iString.Discard( &CSTR__g_system_root );
      }
    } REGISTRY_RELEASE;

    if( g_registry_lock_count == 0 ) {
      DEL_CRITICAL_SECTION( &g_registry_lock.lock );
    }
    else {
      VXDURABLE_REGISTRY_CRITICAL( 0x5D9, "Registry lock still held in graph factory destructor" );
    }

    DEL_CONDITION_VARIABLE( &g_registry_graph_availability.cond );

    g_registry_init = 0;

  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const objectid_t __graph_id_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name ) {
  objectid_t obid = {0};
  CString_t *CSTR__fullname = NULL;
  
  XTRY {
    const char *name = CStringValue(CSTR__graph_name);

    // Validate graph name and compute name length
    if( !iString.Validate.StorableKey( name ) ) {
      VXDURABLE_REGISTRY_REASON( 0x5DA, "Not a valid graph name: '%s'", name );
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5D2 );
    }
    
    // Build the full name
    if( (CSTR__fullname =  iString.NewFormat( NULL, "%s#%s", CStringValue(CSTR__graph_path), CStringValue(CSTR__graph_name) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5DB );
    }

    // Get the obid from the fullname
    idcpy( &obid, CStringObid( CSTR__fullname ) );
  }
  XCATCH( errcode ) {
    idunset( &obid );
  }
  XFINALLY {
    iString.Discard( &CSTR__fullname );
  }

  return obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __new_graph_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages ) {
  vgx_Graph_t *graph = NULL;
  CString_vtable_t *istring = CALLABLE( CSTR__graph_name );
#if defined VGX_GRAPH_VERTEX_BLOCK_ORDER
  const int block_order = VGX_GRAPH_VERTEX_BLOCK_ORDER;
#else
  // Pick a very deliberate block order of 19 to ensure ~512k vertices per block.
  // This leads to ~64k of bitvector data needed per term in the inverted index.
  // When building/decompressing the bit set at query time we do so block by block,
  // and since traversal of the (sparse, framehash-mapped) bit set is non-sequential
  // with respect to the vertex offset we reduce the memory range random writes down
  // to an amount that can fit entirely in L2 cache (256k).
  const int block_order = imag2( _VXOBALLOC_CSTRING_MAX_LENGTH * 8 );
  // The actual number of vertices per block is less than 2**19 (512k)
  // This is restricted by the number of bits we can pack into the maximum size CString.
  // The largest CString object is exactly 64kiB less the object header (64 bytes) plus one 
  // final zero qword, leaving 65464 bytes for string data, or 523768 bits.
  // 
  // The maximum number per block is therefore 523768 vertices.
#endif

  // Named graph does not already exist, create new graph and enter it into the registry
  if( (graph = __get_graph_ROPEN( CSTR__graph_path, CSTR__graph_name )) == NULL ) {
    vgx_Graph_constructor_args_t graph_args = {
      .CSTR__graph_path     = CSTR__graph_path,
      .CSTR__graph_name     = CSTR__graph_name,
      .vertex_block_order   = block_order,
      .graph_t0             = __SECONDS_SINCE_1970(),
      .start_opcount        = -1, // No start opcount known
      .simconfig            = NULL,
      .with_event_processor = igraphfactory.EventsEnabled(),
      .idle_event_processor = igraphfactory.IsEventsIdleDeserialize(),
      .force_readonly       = false,
      .force_writable       = false,
      .local_only           = local
    };
    if( (graph = COMLIB_OBJECT_NEW( vgx_Graph_t, NULL, &graph_args )) == NULL ) {
      if( messages ) {
        *messages = __append_trace_to_messages( NULL );
      }
    }
    else if( __set_graph_ROPEN( graph, messages ) < 0 ) {
      COMLIB_OBJECT_DESTROY( graph );
      graph = NULL;
    }
  }
  // Named graph already exists, verify that it can be used as specified
  else {
    // Basic graph parameter do not match, error
    if( !istring->Equals( CSTR__graph_path, graph->CSTR__path ) || !istring->Equals( CSTR__graph_name, graph->CSTR__name ) ) {
      if( messages ) {
        if( (*messages = _vgx_new_empty_string_tuple_list()) != NULL ) {
          _vgx_string_tuple_list_append_item( *messages, "trace", "path/name mismatch" );
        }
      }
      graph = NULL; // error
    }
  }
  return graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __get_graph_by_obid_RCS( const objectid_t *obid ) {
  vgx_Graph_t *graph = NULL;

  // Perform framehash lookup of graph object pointer by obid
  framehash_valuetype_t vtype = CALLABLE( g_graph_registry )->Get( g_graph_registry, CELL_KEY_TYPE_HASH128, obid, &graph );

  if( vtype == CELL_VALUE_TYPE_OBJECT128 ) {
    return graph;
  }
  else if( vtype == CELL_VALUE_TYPE_NULL ) {
    return NULL;
  }
  else {
    // TODO: handle error?
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __get_graph_ROPEN( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name ) {
  vgx_Graph_t *graph = NULL;
  objectid_t obid = __graph_id_ROPEN( CSTR__graph_path, CSTR__graph_name );
  
  // Check id validity
  if( idnone( &obid ) ) {
    return NULL;
  }

  REGISTRY_LOCK {
    // Retrieve graph object from registry
    graph = __get_graph_by_obid_RCS( &obid );
  } REGISTRY_RELEASE;

  return graph;
}



/*******************************************************************//**
 *
 * NOTE: Caller owns returned memory
 ***********************************************************************
 */
static const vgx_Graph_t ** __list_graphs_RCS( int64_t *n_graphs ) {

  vgx_Graph_t **graphs = NULL;

  if( g_graph_registry ) {
    CtptrList_t *output = g_graph_registry->_dynamic.ref_list;
    CtptrList_vtable_t *ioutput = CALLABLE( output );
    XTRY {
      framehash_vtable_t *imap = CALLABLE(g_graph_registry);
      int64_t n_values;
    
      if( ioutput->Length( output ) != 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5E1, "Internal error: registry output list not empty" );
      }

      if( (n_values = imap->GetValues( g_graph_registry )) < 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5E2, "Internal error: failed to retrieve graphs from registry" );
      }

      if( (graphs = (vgx_Graph_t**)calloc( n_values + 1, sizeof( vgx_Graph_t* ) )) != NULL ) {
        tptr_t value;
        for( int64_t n=0; n < n_values; n++ ) {
          if( ioutput->Get( output, n, &value ) ) {
            if( TPTR_IS_POINTER( &value ) ) {
              graphs[n] = (vgx_Graph_t*)TPTR_GET_POINTER( &value );
            }
            else {
              THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5E3, "Internal error: registry corruption" );
            }
          }
        }
      }

      if( n_graphs ) {
        *n_graphs = n_values;
      }
    }
    XCATCH( errcode ) {
      if( graphs ) {
        free( graphs );
        graphs = NULL;
      }
    }
    XFINALLY {
      ioutput->Clear( output );
    }
  }

  return (const vgx_Graph_t**)graphs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_graph_ROPEN( const vgx_Graph_t *graph, vgx_StringTupleList_t **messages ) {
  int ret = 0;
  objectid_t *obid = COMLIB_OBJECT_GETID( graph );

  REGISTRY_LOCK {

    if( !__wait_for_writable_registry_RCS( 30000 ) ) {
      VXDURABLE_REGISTRY_REASON( 0x000, "Registry busy (readonly), cannot add graph." );
      ret = -1;
    }
    else {
      // Perform framehash insertion of graph object pointer keyed by obid
      // NOTE: Framehash takes ownership of comlib objects when inserted. This means the
      //       inserted object's DESTRUCTOR IS CALLED WHEN REMOVED FROM FRAMEHASH in the future.
      XTRY {
        framehash_vtable_t *imap = CALLABLE(g_graph_registry);
        framehash_valuetype_t vtype = imap->Set( g_graph_registry, CELL_KEY_TYPE_HASH128, CELL_VALUE_TYPE_OBJECT128, obid, graph );

        if( vtype == CELL_VALUE_TYPE_OBJECT128 ) {
          // registry insertion ok, now persist registry
          if( igraphfactory.Persist() < 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5F1, "Internal error: failed to serialize registry" );
          }
        }
        else {
          // insertion into registry failed
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5F2, "Internal error: failed to add graph to registry" );
        }
      }
      XCATCH( errcode ) {
        if( messages ) {
          *messages = __append_trace_to_messages( NULL );
        }
        ret = -1;
      }
      XFINALLY {
      }
    }
  } REGISTRY_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __del_graph_RCS( vgx_Graph_t **graph, bool erase ) {
  int ret = 0;
  if( graph && *graph ) {
    XTRY {
      // Need local copy of obid and path to outlive the graph
      bool local = _vgx_graph_is_local_only_OPEN( *graph );
      char path[256] = {0};
      objectid_t graphkey;
      idcpy( &graphkey, &(*graph)->obid );
      strncpy( path, CALLABLE( *graph )->FullPath( *graph ), 255 );
      int deleted;
      // Remove and destroy graph
      if( (deleted = __remove_and_destroy_graph_RCS( graph )) == 1 ) {
        // Capture
        if( !local ) {
          iSystem.CaptureDestroyGraph( &graphkey, path );
        }
        // Persist registry
        if( igraphfactory.Persist() < 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x601, "Internal error: failed to serialize registry" );
        }
        // Remove from disk
        if( erase ) {
          delete_dir( path );
        }
        // ok!
        ret = 1;
      }
      // Graph does not exist in registry
      else if( deleted == 0 ) {
        ret = 0;
      }
      // Error
      else {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x602, "Internal error: failed to remove graph %s from registry", path );
      }
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __acquire_graph_RCS( vgx_Graph_t *graph, vgx_StringTupleList_t **messages ) {
  int acquired = 0;
  uint32_t tid = GET_CURRENT_THREAD_ID();
  // Claim new ownership
  uint32_t owner;
  GRAPH_LOCK( graph ) {
    owner = _vgx_graph_owner_CS( graph );
  } GRAPH_RELEASE;
  if( owner == 0 ) {
    graph->owner_threadid = tid;
    graph->recursion_count = 1;
    acquired = 1;
  }
  // Graph is locked by current thread - own again
  else if( owner == tid ) {
    // Check for overflow
    if( graph->recursion_count < UINT8_MAX ) {
      ++graph->recursion_count;
      acquired = 1;
    }
    // Overflow
    else {
      VXDURABLE_REGISTRY_REASON( 0x571, "Recursion count overflow for graph '%s'", CStringValue( CALLABLE( graph )->Name( graph ) ) );
      if( messages ) {
        *messages = __append_trace_to_messages( NULL );
      }
      acquired = -1;
    }
  }
  // Graph is locked by other thread
  else {
    // Release api lock and wait for availability signal (max 10 ms)
    WAIT_FOR_REGISTRY_AVAILABLE( 10 );
  }
  return acquired;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __release_graph_RCS( vgx_Graph_t *graph, uint32_t *owner ) {
  int ret;
  uint32_t tid = GET_CURRENT_THREAD_ID();

  GRAPH_LOCK( graph ) {
    *owner = _vgx_graph_owner_CS( graph );
  } GRAPH_RELEASE;

  // Current thread owns graph
  if( *owner == tid ) {
    // Release graph ownership once
    if( --(graph->recursion_count) == 0 ) {
      // Current thread has now given up all ownership
      graph->owner_threadid = 0;
      *owner = 0;
      // Signal in case other threads are waiting for this graph
      SIGNAL_ALL_CONDITION( &(g_registry_graph_availability.cond) );
    }
    //
    ret = graph->recursion_count;
  }
  // Illegal attempt to close graph we don't own
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __retrieve_graph_by_path_and_name_RCS( const CString_t *CSTR__path, const CString_t *CSTR__name) {
  CString_t *CSTR__computed_path = NULL;
  vgx_Graph_t *graph = NULL;
  if( CSTR__path && CStringEqualsChars( CSTR__path, VGX_SYSTEM_GRAPH_PATH ) && CSTR__name && CStringEqualsChars( CSTR__name, VGX_SYSTEM_GRAPH_NAME ) ) {
    graph = iSystem.GetSystemGraph();
  }
  else if( GraphInfo_new_name_and_path( CSTR__path, CSTR__name, &CSTR__computed_path, NULL, NULL ) == 0 ) {
    graph = __get_graph_ROPEN( CSTR__computed_path, CSTR__name );
    CStringDelete( CSTR__computed_path );
  }
  return graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int GraphFactory_size( void ) {
  int size;
  REGISTRY_LOCK {
    size = (int)CALLABLE( g_graph_registry )->Items( g_graph_registry );
  } REGISTRY_RELEASE;
  return size;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_registry.h"
  
test_descriptor_t _vgx_vxdurable_registry_tests[] = {
  { "VGX Graph Registry Tests", __utest_vxdurable_registry },
  {NULL}
};
#endif
