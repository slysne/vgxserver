/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_object.c
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

#include "_vgx.h"
#include "_vgx_serialization.h"
#include "_vxevent.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

DLL_VISIBLE comlib_object_typeinfo_t _vgx_Graph_t_typeinfo = {0}; // Will be set when class is registered


static const int64_t g_initial_opcnt = 10000000000000000ULL;


/* --------- */
/* Graph API */
/* --------- */

/* common comlib_object_vtable_t interface */
static int Graph_cmpid( const vgx_Graph_t *self, const void *idptr );
static objectid_t * Graph_getid( vgx_Graph_t *self );
static int64_t Graph_serialize( vgx_Graph_t *self, CQwordQueue_t *out_queue );
static vgx_Graph_t * Graph_deserialize( vgx_Graph_t *container, CQwordQueue_t *in_queue );
static vgx_Graph_t * Graph_constructor( const void *identifier, vgx_Graph_constructor_args_t *args );
static void Graph_destructor( vgx_Graph_t *self );
static CStringQueue_t * Graph_representer( vgx_Graph_t *self, CStringQueue_t *output );

static const CString_t * Graph_name( const vgx_Graph_t *self );

static void Graph__count_query_nolock( vgx_Graph_t *self, int64_t q_time_nanosec );
static void Graph__reset_query_count_nolock( vgx_Graph_t *self );
static int64_t Graph__query_count_nolock( vgx_Graph_t *self );
static int64_t Graph__query_time_nanosec_acc_nolock( vgx_Graph_t *self );
static double Graph__query_time_average_nolock( vgx_Graph_t *self );

static void Graph_set_destructor_hook( vgx_Graph_t *self, void *external_owner, void (*destructor_callback_hook)( vgx_Graph_t *self, void *external_owner ) );
static void Graph_clear_external_owner( vgx_Graph_t *self );
static void Graph_set_sysaux_command_callback( vgx_Graph_t *self, f_vgx_SystemAuxCommandCallback sysaux_cmd_callback );
static void Graph_clear_sysaux_command_callback( vgx_Graph_t *self );
static void Graph_set_system_sync_callback( vgx_Graph_t *self, f_vgx_SystemSyncCallback system_sync_callback );
static void Graph_clear_system_sync_callback( vgx_Graph_t *self );

static const char * Graph_full_path( const vgx_Graph_t *self );

static int64_t Graph_bulk_serialize( vgx_Graph_t *self, int timeout_ms, bool force, bool remote, vgx_AccessReason_t *reason, CString_t **CSTR__error );

static void Graph_dump( vgx_Graph_t *self );

static int Graph_system_aux_command_noop( vgx_Graph_t *graph, OperationProcessorAuxCommand cmd, vgx_StringList_t *data, CString_t **CSTR__error );
static int Graph_system_sync_noop( vgx_Graph_t *graph, CString_t **CSTR__error );


static vgx_IGraphSimple_t SimpleAPI_Empty = {NULL};


static vgx_IGraphAdvanced_t AdvancedAPI_Empty = {NULL};


/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "VX::OBJ(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXGRAPH_OBJECT_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_OBJECT_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_OBJECT_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_OBJECT_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_OBJECT_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_OBJECT_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )



static vgx_Graph_vtable_t Graph_Methods = {
  /* COMLIB OBJECT INTERFACE (comlib_object_vtable_t) */
  .vm_cmpid       = (f_object_comparator_t)Graph_cmpid,
  .vm_getid       = (f_object_identifier_t)Graph_getid,
  .vm_serialize   = (f_object_serializer_t)Graph_serialize,
  .vm_deserialize = (f_object_deserializer_t)Graph_deserialize,
  .vm_construct   = (f_object_constructor_t)Graph_constructor,
  .vm_destroy     = (f_object_destructor_t)Graph_destructor,
  .vm_represent   = (f_object_representer_t)Graph_representer,
  .vm_allocator   = NULL,

  /* */
  .Name                           = Graph_name,
  .CountQueryNolock               = Graph__count_query_nolock,
  .ResetQueryCountNolock          = Graph__reset_query_count_nolock,
  .QueryCountNolock               = Graph__query_count_nolock,
  .QueryTimeNanosecAccNolock      = Graph__query_time_nanosec_acc_nolock,
  .QueryTimeAverageNolock         = Graph__query_time_average_nolock,
  .SetDestructorHook              = Graph_set_destructor_hook,
  .ClearExternalOwner             = Graph_clear_external_owner,
  .SetSystemAuxCommandCallback    = Graph_set_sysaux_command_callback,
  .ClearSystemAuxCommandCallback  = Graph_clear_sysaux_command_callback,
  .SetSystemSyncCallback          = Graph_set_system_sync_callback,
  .ClearSystemSyncCallback        = Graph_clear_system_sync_callback,
  .FullPath                       = Graph_full_path,
  .BulkSerialize                  = Graph_bulk_serialize,
  .Dump                           = Graph_dump,

  /* PLACEHOLDER */
  .simple                         = &SimpleAPI_Empty,

  /* PLACEHOLDER */
  .advanced                       = &AdvancedAPI_Empty


};


// Constants for serialization
#define __GRAPH_REFERENCE "GRAPHREF"
#define __GRAPH_name      "name"
#define __GRAPH_path      "path"
#define __GRAPH_vxorder   "vxorder"
#define __GRAPH_opcount   "opcount"


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t __graph_id( const CString_t *CSTR__graph_name, const CString_t *CSTR__graph_path ) {
  objectid_t obid = *CStringObid( CSTR__graph_name );       // no 128-bit identifier, compute from name string
  if( CSTR__graph_path ) {
    // Continue digest
    const objectid_t *dirid = CStringObid( CSTR__graph_path );
    obid.H ^= dirid->H;
    obid.L ^= dirid->L;
  }
  return obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void Graph_RegisterClass( void ) {

  // Finish the graph api initialization
  Graph_Methods.simple = _vxapi_simple;
  Graph_Methods.advanced = _vxapi_advanced;
  
  // Register the graph class
  COMLIB_REGISTER_CLASS( vgx_Graph_t, CXLIB_OBTYPE_GRAPH, &Graph_Methods, OBJECT_IDENTIFIED_BY_OBJECTID, -1 );
  
  ASSERT_TYPE_SIZE( cxlib_thread_t,            1 * sizeof( QWORD       ) );
  ASSERT_TYPE_SIZE( vgx_readonly_state_t,      2 * sizeof( QWORD       ) );
  ASSERT_TYPE_SIZE( vgx_GraphTimer_t,          5 * sizeof( QWORD       ) );
  ASSERT_TYPE_SIZE( vgx_OperatorCapture_t,     1 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_OperationEmitter_t,    3 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_OperationSystem_t,     3 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_OperationParser_t,     4 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_OperationProcessor_t, 10 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_EventProcessor_t,      4 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( vgx_Graph_t,              40 * sizeof( cacheline_t ) );

  _vgx_Graph_t_typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Graph_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void Graph_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Graph_t );
}





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_cmpid( const vgx_Graph_t *self, const void *idptr ) {
  return idcmp( Graph_getid( (vgx_Graph_t*)self), (const objectid_t*)idptr );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static objectid_t * Graph_getid( vgx_Graph_t *self ) {
  return &self->obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_serialize( vgx_Graph_t *self, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  // NOTE: COMLIB serialization API is used by framehash when serializing
  // objects. 

  // COMLIB serialization of a graph simply streams the graph path and name.
  // It is used by the registry to keep track of graphs in the system.
  // The graph path will contain files that describe the graph so it can
  // be restored through some other process.

  CString_t *CSTR__GRAPH_REF = NULL;
  CString_t *CSTR__GRAPH_NAME = NULL;
  CString_t *CSTR__GRAPH_PATH = NULL;
  CString_t *CSTR__GRAPH_VXORDER = NULL;
  
  CString_t *CSTR__vertex_block_order = NULL;

  XTRY {
    // Keys
    CSTR__GRAPH_REF = iString.New( NULL, __GRAPH_REFERENCE );
    CSTR__GRAPH_NAME = iString.New( NULL, __GRAPH_name );
    CSTR__GRAPH_PATH = iString.New( NULL, __GRAPH_path );
    CSTR__GRAPH_VXORDER = iString.New( NULL, __GRAPH_vxorder );

    // Values
    CSTR__vertex_block_order = iString.NewFormat( NULL, "%lld", (int64_t)ivertexalloc.BlockOrder( self->vertex_allocator ) );
    
    if( !CSTR__GRAPH_REF || !CSTR__GRAPH_NAME || !CSTR__GRAPH_PATH || !CSTR__GRAPH_VXORDER || !CSTR__vertex_block_order ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5A1 );
    }

    // Stream reference info about ourselves into the __OUTPUT, which belongs to the graph REGISTRY.
    EVAL_OR_THROW( iSerialization.WriteBeginSection( CSTR__GRAPH_REF, __OUTPUT ), 0x5A2 );
    EVAL_OR_THROW( iSerialization.WriteKeyValue( CSTR__GRAPH_NAME, self->CSTR__name, __OUTPUT ), 0x5A3 );
    EVAL_OR_THROW( iSerialization.WriteKeyValue( CSTR__GRAPH_PATH, self->CSTR__path, __OUTPUT ), 0x5A4 );
    EVAL_OR_THROW( iSerialization.WriteKeyValue( CSTR__GRAPH_VXORDER, CSTR__vertex_block_order, __OUTPUT ), 0x5A5 );

    // NOTE: Serialization of the actual graph data is implemented in a separate method.
    // The separate implementation is necessary because the standard COMLIB serialize method
    // is used by the registry when the registry serializes itself. The registry is a 
    // framehash instance holding object pointers to graphs. When the registry is serialized
    // it will recursively serialize its contained objects (graphs). Serializing the registry
    // needs to be a light-weight operation and therefore we store only the path and name
    // to the contained graphs. However, when the registry is deserialized on startup, each
    // path/name reference will be used to completely restore each graph, so deserialization
    // of the registry is not a light-weight operation!

  }
  XCATCH( errcode ) {
    __NQWORDS = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__vertex_block_order );
    iString.Discard( &CSTR__GRAPH_VXORDER );
    iString.Discard( &CSTR__GRAPH_PATH );
    iString.Discard( &CSTR__GRAPH_NAME );
    iString.Discard( &CSTR__GRAPH_REF );
  }
  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Graph_t * Graph_deserialize( vgx_Graph_t *container, CQwordQueue_t *input ) {
  int64_t __NQWORDS = 0;

  vgx_Graph_constructor_args_t graph_args = {0};
  vgx_Graph_t *graph = NULL;

  CString_t *CSTR__key = NULL;
  CString_t *CSTR__name = NULL;
  CString_t *CSTR__path = NULL;
  CString_t *CSTR__vertex_block_order = NULL;
  CString_t *CSTR__GRAPH_REF = NULL;
  CString_t *CSTR__GRAPH_NAME = NULL;
  CString_t *CSTR__GRAPH_PATH = NULL;
  CString_t *CSTR__GRAPH_VXORDER = NULL;

  XTRY {
    CSTR__GRAPH_REF = iString.New( NULL, __GRAPH_REFERENCE );
    CSTR__GRAPH_NAME = iString.New( NULL, __GRAPH_name );
    CSTR__GRAPH_PATH = iString.New( NULL, __GRAPH_path );
    CSTR__GRAPH_VXORDER = iString.New( NULL, __GRAPH_vxorder );
    if( !CSTR__GRAPH_REF || !CSTR__GRAPH_NAME || !CSTR__GRAPH_PATH || !CSTR__GRAPH_VXORDER ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5B1 );
    }

    // Begin graph section
    EVAL_OR_THROW( iSerialization.ExpectBeginSection( CSTR__GRAPH_REF, __INPUT ), 0x5B2 );

    // Graph Name
    EVAL_OR_THROW( iSerialization.ReadKeyValue( &CSTR__key, &CSTR__name, __INPUT ), 0x5B3 ) ;
    if( !CALLABLE( CSTR__key )->Equals( CSTR__key, CSTR__GRAPH_NAME ) ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x5B4 );
    }
    iString.Discard( &CSTR__key );
    graph_args.CSTR__graph_name = CSTR__name;

    // Graph Path
    EVAL_OR_THROW( iSerialization.ReadKeyValue( &CSTR__key, &CSTR__path, __INPUT ), 0x5B5 ) ;
    if( !CALLABLE( CSTR__key )->Equals( CSTR__key, CSTR__GRAPH_PATH ) ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x5B6 );
    }
    iString.Discard( &CSTR__key );
    graph_args.CSTR__graph_path = CSTR__path;

    // Graph Vertex Block Order
    EVAL_OR_THROW( iSerialization.ReadKeyValue( &CSTR__key, &CSTR__vertex_block_order, __INPUT ), 0x5B7 ) ;
    if( !CALLABLE( CSTR__key )->Equals( CSTR__key, CSTR__GRAPH_VXORDER ) ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x5B8 );
    }
    iString.Discard( &CSTR__key );
    graph_args.vertex_block_order = (int)CALLABLE( CSTR__vertex_block_order )->AsInteger( CSTR__vertex_block_order );

    iString.Discard( &CSTR__key );
    graph_args.start_opcount = -1;

    // T0
    graph_args.graph_t0 = __SECONDS_SINCE_1970();

    // TODO:
    //   We need to use the name and path to look for graph data files and then reconstruct the graph
    //   from those files. For now we just create a brand new graph ignoring files on disk, just to 
    //   test everything
    
    vgx_Similarity_config_t simconfig = {0};
    if( igraphfactory.EuclideanVectors() ) {
      memcpy( &simconfig, &DEFAULT_EUCLIDEAN_SIMCONFIG, sizeof(vgx_Similarity_config_t) );
    }
    else if( igraphfactory.FeatureVectors() ) {
      memcpy( &simconfig, &DEFAULT_FEATURE_SIMCONFIG, sizeof(vgx_Similarity_config_t) );
    }
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5B9 );
    }

    graph_args.simconfig = &simconfig;

    graph_args.with_event_processor = igraphfactory.EventsEnabled();
    graph_args.idle_event_processor = igraphfactory.IsEventsIdleDeserialize();
    graph_args.force_readonly       = igraphfactory.IsReadonlyDeserialize();
    graph_args.force_writable       = false;

    if( (graph = COMLIB_OBJECT_NEW( vgx_Graph_t, NULL, &graph_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5BA );
    }

  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    iString.Discard( &CSTR__GRAPH_VXORDER );
    iString.Discard( &CSTR__GRAPH_PATH );
    iString.Discard( &CSTR__GRAPH_NAME );
    iString.Discard( &CSTR__GRAPH_REF );
    iString.Discard( &CSTR__vertex_block_order );
    iString.Discard( &CSTR__path );
    iString.Discard( &CSTR__name );
    iString.Discard( &CSTR__key );
  }

  return graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * Graph_constructor( const void *identifier, vgx_Graph_constructor_args_t *args ) {
  vgx_Graph_t *self = NULL;
  graph_state_t *state = NULL;
  objectid_t obid;
  CString_t *CSTR__backlog = NULL;
  CString_t *CSTR__tmpstr = NULL;
  CString_t *CSTR__name = NULL;
  CString_t *CSTR__path = NULL;
  const char *graph_name = NULL;
  const char *graph_path = NULL;
  const char *graph_fullpath = NULL;

  if( args == NULL ) {
    return NULL;
  }

  XTRY {

    // DO NOT USE the supplied cstrings directly. They may be allocated by external allocators
    // that we cannot depend on to outlive the graph instance. There may be implicit string Clone()
    // calls that occur in the constructor, and Clone() will use whatever string allocator is
    // referenced in the string instance.
    graph_name = CharsNew( args->CSTR__graph_name ? CStringValue( args->CSTR__graph_name ) : "" );
    graph_path = CharsNew( args->CSTR__graph_path ? CStringValue( args->CSTR__graph_path ) : "" );

    CString_t *CSTR__args_graph_name = (graph_name && strlen( graph_name ) > 0) ? CStringNew( graph_name ) : NULL;
    CString_t *CSTR__args_graph_path = (graph_path && strlen( graph_path ) > 0) ? CStringNew( graph_path ) : NULL;

    // Determine path and name (new allocations)
    if( igraphinfo.NewNameAndPath( CSTR__args_graph_path, CSTR__args_graph_name, &CSTR__path, &CSTR__name, NULL ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5C1 );
    }


    // Determine the graph objectid
    if( identifier ) {
      obid = *((objectid_t*)identifier);    // 128-bit identifier supplied from the outside
    }
    else if( args ) {
      obid = igraphinfo.GraphID( CSTR__path, CSTR__name );
      if( idnone( &obid ) ) {
        THROW_ERROR( CXLIB_ERR_API, 0x5C2 );  // invalid name
      }
    }
    else {
      THROW_ERROR( CXLIB_ERR_API, 0x5C3 );  // can't construct a graph if we don't supply and id info
    }

    if( CSTR__args_graph_path ) {
      VXGRAPH_OBJECT_VERBOSE( NULL, 0x5C4, "Constructing graph: %s", CStringValue( CSTR__args_graph_path ) );
    }

    // =====================
    // Allocate graph object
    // =====================
    
    // Everything is zero after this except the vtable, typeinfo and object ID which
    // are set as part of graph object allocation.
    // [Q1.1] vtable
    // [Q1.2] object typeinfo
    // [Q1.3-4] ID
    if( (self = igraphobject.New( &obid )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5C5 );
    }

    // [Q1.5] Instance ID
    int64_t tms = __GET_CURRENT_MICROSECOND_TICK();
    int64_t addr = (int64_t)self;
    self->instance_id = ihash64( tms + addr ); // good enough?

    // [Q1.6] Graph name
    if( CSTR__name && CStringLength( CSTR__name ) > VGX_MAX_GRAPH_NAME ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5C6, "Graph name too long" );
    }
    self->CSTR__name = CSTR__name;
    CSTR__name = NULL;

    // [Q1.7] Graph base directory
    if( CSTR__path && CStringLength( CSTR__path ) > VGX_MAX_GRAPH_PATH ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5C7, "Graph path too long" );
    }
    self->CSTR__path = CSTR__path;
    CSTR__path = NULL;

    // [Q1.8] Graph full path
    if( (self->CSTR__fullpath = igraphinfo.GraphPath( self )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5C8 );
    }
    graph_fullpath = CharsNew( CStringValue( self->CSTR__fullpath ) );


    // ===============================
    // Locks and condition variables
    // ===============================

    // [Q3] Graph state lock
    INIT_SPINNING_CRITICAL_SECTION( &self->state_lock.lock, 4000 );

    // [Q18.1-6] Vertex availability condition
    INIT_CONDITION_VARIABLE( &self->vertex_availability.cond );

    // [Q19] Query state lock
    INIT_SPINNING_CRITICAL_SECTION( &self->q_lock.lock, 4000 );

    // [Q22.1-6] Inarcs return condition
    INIT_CONDITION_VARIABLE( &self->return_inarcs.cond );

    // [Q39.1-6] Wake event condition
    INIT_CONDITION_VARIABLE( &self->wake_event.cond );

    // [Q40.5]
    self->sysaux_cmd_callback = Graph_system_aux_command_noop;

    // [Q40.6]
    self->system_sync_callback = Graph_system_sync_noop;

    // [Q40.7] External owner object
    self->external_owner_object = NULL;

    // [Q40.8] Destructor callback hook
    self->destructor_callback_hook = NULL;

    // [Q4.1.1] Owner thread ID
    self->owner_threadid = 0;
   
    // [Q4.1.2.1] Recursion count
    self->recursion_count = 0;

    // [Q4.1.2.2]
    self->__rsv_4_1_2_2 = 0;

    // [Q4.1.2.3-4] State lock recursion counter
    self->__state_lock_count = 0;

    // [Q4.2.1]
    memset( &self->control, 0, sizeof(self->control) );

    // [Q4.2.2]
    self->__rsv_4_2_2 = 0;

    // [Q4.3-4]
    memset( &self->readonly, 0, sizeof(self->readonly) );

    // [Q4.5] Graph operation reset
    iOperation.InitId( &self->operation, 0 );

    // [Q4.6] Current writelocks
    self->count_vtx_WL = 0;

    // [Q4.7] Current readlocks
    self->count_vtx_RO = 0;

    // [Q4.8] Graph order (number of vertices)
    self->_order_atomic = 0;

    // [Q2.7] Similarity
    vgx_Similarity_constructor_args_t simargs = {
      .params = args->simconfig,
      .parent = self
    };
    if( (self->similarity = COMLIB_OBJECT_NEW( vgx_Similarity_t, NULL, &simargs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5C9 );
    }

    // [Q5-7] Graph's arcvector framehash dynamic
    iString.Discard( &CSTR__tmpstr );
    if( (CSTR__tmpstr = iString.NewFormat( NULL, "Arcvector Framehash Dynamic for Graph '%s'" , CStringValue(self->CSTR__name) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CA );
    }
    if( iFramehash.dynamic.InitDynamicSimple( &self->arcvector_fhdyn, CStringValue( CSTR__tmpstr ), 23 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CB );
    }

    // [Q8.1] Graph size (number or arcs)
    self->_size_atomic = 0;

    // [Q8.2] Vector count
    self->_nvectors_atomic = 0;

    // [Q8.3] Property count
    self->_nproperties_atomic = 0;

    // [Q8.4] Graph reverse size
    self->rev_size_atomic = 0;

    // [Q8.5]
    self->__rsv_8_5 = 0;

    // [Q8.5]
    self->__rsv_8_6 = 0;

    // [Q8.5]
    self->__rsv_8_7 = 0;
    
    // [Q8.5]
    self->__rsv_8_8 = 0;

    // [Q9-11] Graph's property framehash dynamic
    iString.Discard( &CSTR__tmpstr );
    if( (CSTR__tmpstr = iString.NewFormat( NULL, "Property Framehash Dynamic for Graph '%s'" , CStringValue(self->CSTR__name) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CC );
    }
    if( iFramehash.dynamic.InitDynamicSimple( &self->property_fhdyn, CStringValue( CSTR__tmpstr ), 23 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CD );
    }
    
    // [Q15-17] Graph's vertex map framehash dynamic
    iString.Discard( &CSTR__tmpstr );
    if( (CSTR__tmpstr = iString.NewFormat( NULL, "Vertex Map Framehash Dynamic for Graph '%s'" , CStringValue(self->CSTR__name) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CE );
    }
    if( iFramehash.dynamic.InitDynamicSimple( &self->vtxmap_fhdyn, CStringValue( CSTR__tmpstr ), 21 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5CF );
    }

    // [Q12.1] Ephemeral String Allocator Context
    if( (self->ephemeral_string_allocator_context = icstringalloc.NewContext( self, NULL, NULL, NULL, "ephemeral string allocator" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D0 );
    }
 
    // Property Allocator Context
#ifdef CSTRING_INDEX_PERSIST
    if( (self->property_allocator_context = icstringalloc.NewContext( self, NULL, NULL, graph_fullpath, VGX_PATHDEF_STRING_PROPERTY_DATA_DIRNAME )) == NULL )
#else
    // Pass in pointer to the cstring value map in order to trigger index rebuild from allocator. Index will NOT be loaded from disk.
    // [Q12.2]
    // [Q12.7]
    // [Q12.8]
    if( (self->property_allocator_context = icstringalloc.NewContext( self, &self->vxprop_keymap, &self->vxprop_valmap, graph_fullpath, VGX_PATHDEF_STRING_PROPERTY_DATA_DIRNAME )) == NULL )
#endif
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D1 );
    }
    if( icstringalloc.RestoreObjects( self->property_allocator_context ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D2 );
    }

    // [Q12.3-4] Vertex type enumerator (vxtype_encoder + vxtype_decoder)
    if( _vxenum_vtx__create_enumerator( self ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5D3, "Failed to create graph vertex type enumerator" );
    }

    // [Q12.5-6] Relationship enumerator (rel_encoder + rel_decoder)
    if( _vxenum_rel__create_enumerator( self ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5D4, "Failed to create graph relationship enumerator" );
    }

    // [Q12.7 Q12.8] Property key/value maps (if not already created above)
    if( _vxenum_prop__create_enumerator( self ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5D5, "Failed to create graph property maps" );
    }
   
    // [Q13-14] Virtual Properties
    if( _vxvertex_property__virtual_properties_init( self ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5D6, "Failed initialize virtual properties" );
    }

    // [Q2.6] Stack evaluator contexts (depends on ephemeral string allocator)
    if( iEvaluator.CreateEvaluators( self ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D7 );
    }

    // [Q23-26] Initialize Event Processor, prepare it to be populated during vertex object restore below
    if( iGraphEvent.Initialize( self, args->with_event_processor ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5D8 );
    }

    // [Q2.1] Vertex allocator
    // [Q2.2] Vertex index
    // [Q2.3] Vertex type index
#ifdef VXTABLE_PERSIST
    if( (self->vertex_allocator = ivertexalloc.New( self, args->vertex_block_order, NULL, NULL )) == NULL )
#else
    if( (self->vertex_allocator = ivertexalloc.New( self, args->vertex_block_order, &self->vxtable, &self->vxtypeidx )) == NULL )
#endif
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5D9 );
    }
    if( CALLABLE( self->vertex_allocator )->RestoreObjects( self->vertex_allocator ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5DA );
    }

#ifdef VXTABLE_PERSIST
    if( _vxgraph_vxtable__create_index_OPEN( self ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5DB, "Failed to create graph vertex index" );
    }
#endif

    // Event Backlog
    if( args->with_event_processor ) {
      if( (CSTR__backlog = iGraphEvent.FormatBacklogInfo( self, NULL )) != NULL ) {
        VXGRAPH_OBJECT_INFO( self, 0x5DC, "%s", CStringValue( CSTR__backlog ) ); // message may contain '%' so we have to supply it as arg
      }
    }

    // ---------------------------------
    // Validate/repair allocated objects
    // ---------------------------------
    int64_t vtx_fix = ivertexalloc.Verify( self );
    if( vtx_fix < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5DD, "Vertex allocator corrupted" );
    }
    else if( vtx_fix > 0 ) {
      VXGRAPH_OBJECT_INFO( self, 0x5DE, "Repaired %lld vertex objects", vtx_fix );
      GRAPH_LOCK( self ) {
        iOperation.Graph_CS.SetModified( self );
      } GRAPH_RELEASE;
    }
    int64_t cstr_fix = icstringalloc.Verify( self->property_allocator_context );
    if( cstr_fix < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5DF, "Property allocator corrupted" );
    }
    else if( cstr_fix > 0 ) {
      VXGRAPH_OBJECT_INFO( self, 0x5E0, "Repaired %lld string objects", cstr_fix );
      GRAPH_LOCK( self ) {
        iOperation.Graph_CS.SetModified( self );
      } GRAPH_RELEASE;
    }
    int64_t sim_fix = CALLABLE( self->similarity )->VerifyAllocators( self->similarity );
    if( sim_fix < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5E1, "Similarity vector allocators corrupted" );
    }
    else if( sim_fix > 0 ) {
      VXGRAPH_OBJECT_INFO( self, 0x5E2, "Repaired %lld vector objects", sim_fix );
      GRAPH_LOCK( self ) {
        iOperation.Graph_CS.SetModified( self );
      } GRAPH_RELEASE;
    }

    // [Q2.4] Acquired vertex map WL
    if( (self->vtxmap_WL = iFramehash.simple.New( &self->vtxmap_fhdyn )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5E3, "Failed to create vertex acquisition maps" );
    }

    // [Q2.5] Acquired vertex map RO
    if( (self->vtxmap_RO = iFramehash.simple.New( &self->vtxmap_fhdyn )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x5E4, "Failed to create vertex acquisition maps" );
    }

    // [Q18.7] Vertex list utility
    Cm256iList_constructor_args_t vertex_list_args = { .element_capacity = 8, .comparator = NULL };
    if( (self->vertex_list = COMLIB_OBJECT_NEW( Cm256iList_t, NULL, &vertex_list_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5E5 );
    }

    // [Q18.8] Vertex heap utility
    Cm256iHeap_constructor_args_t vertex_heap_args = { .element_capacity = 8, .comparator = NULL };  // <= TODO: define the comparator for min or max heap
    if( (self->vertex_heap = COMLIB_OBJECT_NEW( Cm256iHeap_t, NULL, &vertex_heap_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5E6 );
    }

    // [Q22.7] Arc list utility
    Cm256iList_constructor_args_t arc_list_args = { .element_capacity = 4, .comparator = NULL };
    if( (self->arc_list = COMLIB_OBJECT_NEW( Cm256iList_t, NULL, &arc_list_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5E7 );
    }

    // [Q22.8] Arc heap utility
    Cm256iHeap_constructor_args_t arc_heap_args = { .element_capacity = 4, .comparator = NULL }; // <= TODO: define comparator to get min or max heap
    if( (self->arc_heap = COMLIB_OBJECT_NEW( Cm256iHeap_t, NULL, &arc_heap_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x5E8 );
    }

    // [Q20.1.1]
    self->q_pri_req = 0;

    // [Q20.1.2]
    self->__rsv_20_1_2 = 0;

    // [Q20.2]
    self->q_count = 0;

    // [Q20.3]
    self->q_time_nanosec_acc = 0;

    // [Q20.4]
    self->__rsv_20_4 = 0;
    
    // [Q20.5]
    self->__rsv_20_5 = 0;
    
    // [Q20.6]
    self->__rsv_20_6 = 0;
    
    // [Q20.7]
    self->__rsv_20_7 = 0;

    // [Q20.8]
    self->__rsv_20_8 = 0;

    // [Q21.1-5]
    int tick_start;
    GRAPH_LOCK( self ) {
      // Graph initial creation time specified by constructor args (may be restored)
      tick_start = _vxgraph_tick__initialize_CS( self, args->graph_t0 );
    } GRAPH_RELEASE;
    if( tick_start < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5E9 );
    }

    // [Q21.6]
    self->__rsv_21_6 = 0;

    // [Q21.7]
    self->__rsv_21_7 = 0;

    // [Q21.8]
    self->__rsv_21_8 = 0;

    // ----------------
    // Prepare vertices
    // ----------------
    int64_t prep = 0;
    GRAPH_LOCK( self ) {
      _vgx_graph_allow_vertex_constructor_CS( self );
      _vgx_graph_allow_WL_CS( self );
      prep = _vxgraph_vxtable__prepare_vertices_CS_NT( self );
    } GRAPH_RELEASE;
    if( prep < 0 ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x5EA );
    }

    // -----------------------------------
    // Operation id initialized to default
    // -----------------------------------
    // [Q4.5]
    int64_t opid = iOperation.SetId( &self->operation, g_initial_opcnt );

    // ---------------------
    // Load state and verify
    // ---------------------
    if( (state = iSerialization.LoadState( self )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x5EB );
    }
    // Verify graph name
    if( !CharsEqualsConst( CStringValue( self->CSTR__name ), state->graph.name ) ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5EC, "Incorrect graph name '%s' (expected '%s')", CStringValue( self->CSTR__name ), state->graph.name );
    }
    
    // -----------------------------------
    // Transaction and Operation Processor
    // -----------------------------------

    // [Q27.1-2] Restore last transaction ID OUT
    idcpy( &self->tx_id_out, &state->graph.tx_id_out );

    // [Q27.3] Restore last transaction serial number OUT
    self->tx_serial_out = state->graph.tx_serial_out;

    // [Q27.4] Restore last transaction count OUT
    self->tx_count_out = state->graph.tx_count_out;

    // [Q27.5-6] Restore last transaction ID IN
    idcpy( &self->tx_id_in, &state->graph.tx_id_in );

    // [Q27.7] Restore last transaction serial number IN
    self->tx_serial_in = state->graph.tx_serial_in;

    // [Q27.8 Restore last transaction count IN]
    self->tx_count_in = state->graph.tx_count_in;

    if( state->graph.tx_count_out > 0 ) {
      VXGRAPH_OBJECT_INFO( self, 0x5ED, "Initial transaction OUT: id=%016llx%016llx serial=%lld count=%lld", self->tx_id_out.H, self->tx_id_out.L, self->tx_serial_out, self->tx_count_out );
    }
    if( state->graph.tx_count_in > 0 ) {
      VXGRAPH_OBJECT_INFO( self, 0x5EE, "Initial transaction IN: id=%016llx%016llx serial=%lld count=%lld", self->tx_id_in.H, self->tx_id_in.L, self->tx_serial_in, self->tx_count_in );
    }

    // Durability point
    // [Q28.1-2]
    idcpy( &self->durable_tx_id, &self->tx_id_in );
    
    // [Q28.3]
    self->durable_tx_serial = self->tx_serial_in;

    // [Q28.4]
    self->sysmaster_tx_serial_0 = 0;

    // [Q28.5]
    self->sysmaster_tx_serial = 0;

    // [Q28.6]
    self->sysmaster_tx_serial_count = 0;

    // [Q28.7]
    self->persisted_ts = state->time.persist_t1;

    // [Q28.8]
    self->tx_input_lag_ms_CS = 0;

    // Did we restore without errors?
    int clean_restore = !iOperation.IsDirty( &self->operation );
    // Persisted state opcount does not match graph opcount supplied to graph constructor
    int64_t state_opid = state->graph.opcount;
    // Opid supplied to constructor is unknown
    if( state_opid >= opid ) {
      opid = state_opid;
      VXGRAPH_OBJECT_VERBOSE( self, 0x5EF, "Opcount initialized to: %016llX", opid );
    }
    else {
      VXGRAPH_OBJECT_FATAL( self, 0x05F0, "Opcount mismatch: state=%016llx < %016llx", state_opid, opid );
    }

    // Local?
    if( args->local_only == true || state->graph.local_only != 0 ) {
      self->control.local_only = true;
      if( state->graph.local_only != 0 && args->local_only == false ) {
        VXGRAPH_OBJECT_WARNING( self, 0x5F1, "Graph is local only (from saved state)" );
      }
    }

    // -------------------------------
    // Initialize Operation Processing
    // -------------------------------
    //
    // [Q29-38]
    if( iOperation.Initialize( self, opid ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5F2 );
    }
    if( self->control.local_only ) {
      iOperation.Emitter_CS.Disable( self );
    }

    // [Q39.7]
    self->__rsv_39_7 = 0;
    
    // [Q39.8]
    self->__rsv_39_8 = 0;

    // [Q40.1]
    self->vgxserverA = NULL;

    // [Q40.2]
    self->vgxserverB = NULL;

    // [Q40.3]
    self->__rsv_40_3 = 0;

    // [Q40.4]
    self->__rsv_40_4 = 0;

    // -----------------------------------
    // Finalize construction
    // 
    // -----------------------------------

    // Open graph operation
    int op_open;
    GRAPH_LOCK( self ) {
      //op_open = iOperation.Open_CS( self, &self->operation, COMLIB_OBJECT( self ), true );
      op_open = OPEN_GRAPH_OPERATION_CS( self );
    } GRAPH_RELEASE;
    if( op_open < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5F3 );
    }

    // Readonly until constructor is complete
    vgx_AccessReason_t reason;
    if( CALLABLE( self )->advanced->AcquireGraphReadonly( self, 10000, false, &reason ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5F4 );
    }

    // Mark graph dirty if we made repairs during restore
    if( !clean_restore ) {
      GRAPH_LOCK( self ) {
        iOperation.Graph_CS.SetModified( self );
      } GRAPH_RELEASE;
    }

    // Verify size
    if( GraphSize( self ) != state->graph.size ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5F5, "Incorrect graph size %lld (expected %lld)", GraphSize( self ), state->graph.size );
    }
    // Verify order
    if( GraphOrder( self ) != state->graph.order ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5F6, "Incorrect graph order %lld' (expected %lld)", GraphOrder( self ), state->graph.order );
    }
    // Verify property count
    int64_t nproperties = GraphPropCount( self );
    if( nproperties != state->vertex_property.nprop ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5F7, "Incorrect graph property count %lld' (expected %lld)", nproperties, state->vertex_property.nprop );
    }
    // Verify vector count
    int64_t nvectors = GraphVectorCount( self );
    if( nvectors != state->vector.nvectors ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x5F8, "Incorrect graph vector count %lld' (expected %lld)", nvectors, state->vector.nvectors );
    }

    // Set inception time
    uint32_t inception_t0 = (uint32_t)state->time.graph_t0;
    ATOMIC_ASSIGN_u32( &self->TIC.inception_t0_atomic, inception_t0 );
    
    // Print object summary for restored graphs
    if( state->graph.n_ops > 0 ) {
      iSerialization.PrintGraphCounts_ROG( self, "Object Summary" );
    }

    // Graph access state is readonly: Remain in readonly mode unless forced writable
    bool readonly = state->graph.readonly || args->force_readonly;
    if( readonly && !args->force_writable ) {
      VXGRAPH_OBJECT_WARNING( self, 0x5F9, "Graph access state is READONLY" );
    }
    // Graph access state is writable: Leave readonly mode and start event processor
    else {
      if( CALLABLE( self )->advanced->ReleaseGraphReadonly( self ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5FA );
      
      }
    }
    
    // Initial persist
    if( state->time.persist_n == 0 ) {
      CString_t *CSTR__err = NULL;
      if( CALLABLE( self )->BulkSerialize( self, 10000, true, false, NULL, &CSTR__err ) < 0 ) {
        VXGRAPH_OBJECT_REASON( self, 0x5FB, "Initial persist failed: %s", CSTR__err ? CStringValue( CSTR__err ) : "?" );
        iString.Discard( &CSTR__err );
      }
    }

    // Start the event processor
    if( args->with_event_processor ) { 
      bool event_enable = (readonly || args->idle_event_processor) ? false : true;
      if( iGraphEvent.Start( self, event_enable ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x5FC );
      }

      // Indicate activities suspended if graph is restored in a readonly state
      bool EVP_suspended = readonly ? true : false;
      _vgx_readonly_suspend_EVP_CS( &self->readonly, EVP_suspended );

      // Event Backlog
      iString.Discard( &CSTR__backlog );
      if( (CSTR__backlog = iGraphEvent.FormatBacklogInfo( self, NULL )) != NULL ) {
        VXGRAPH_OBJECT_INFO( self, 0x5FD, "%s", CStringValue( CSTR__backlog ) ); // message may contain '%' so supply it as arg 
      }
    }

  }
  XCATCH( errcode ) {
    // Trace
    cxlib_msg_set( NULL, 0, "graph_constructor_error\t%03X", cxlib_exc_code( errcode ) );
    if( graph_name ) {
      cxlib_msg_set( NULL, 0, "graph_name\t%s", graph_name );
      CharsDelete( graph_name );
    }
    if( graph_path ) {
      cxlib_msg_set( NULL, 0, "graph_path\t%s", graph_path );
      CharsDelete( graph_path );
    }
    if( graph_fullpath ) {
      cxlib_msg_set( NULL, 0, "graph_fullpath\t%s", graph_fullpath );
      CharsDelete( graph_fullpath );
    }

    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
    iString.Discard( &CSTR__name );
    iString.Discard( &CSTR__path );
    iString.Discard( &CSTR__tmpstr );
  }
  XFINALLY {
    if( state ) {
      free( state );
    }
    iString.Discard( &CSTR__backlog );
  }

  // Ready
  if( self ) {
    GRAPH_LOCK( self ) {
      _vgx_graph_set_ready_CS( self );
    } GRAPH_RELEASE;
  }

  return self;
}



/*******************************************************************//**
 * CELL PROCESSOR
 * Set thread local vertex maps to NULL
 ***********************************************************************
 */
static int64_t __release_vertex_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  Key64Value56_t item = {
    .key = cell->annotation,            // tracked vertex address
    .value56 = APTR_AS_INTEGER( cell )  // acquisition count
  };

  // Delete map
  vgx_Vertex_t *vertex = (vgx_Vertex_t*)item.key;
  int64_t lock_count = item.value56;
  if( vertex != NULL && lock_count > 0 ) {
    int64_t *counter = (int64_t*)processor->processor.output;
    int semcnt = __vertex_get_semaphore_count( vertex );
    // Force lock counter unwind
    if( counter ) {
      *counter -= lock_count; 
    }
    // Force erase the entry in the vertex:lock_count map
    FRAMEHASH_PROCESSOR_DELETE_CELL( processor, cell );
    // Force vertex unlock
    __vertex_set_unlocked( vertex );
    __vertex_clear_writable( vertex );
    __vertex_clear_writer_thread( vertex );
    __vertex_clear_semaphore_count( vertex );
    // Force refcount reduction
    for( int r=0; r<semcnt; r++ ) {
      Vertex_DECREF_WL( vertex );
    }
  }
  return 1;
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}



/*******************************************************************//**
 * CELL PROCESSOR
 * Set thread local vertex maps to NULL
 ***********************************************************************
 */
static int64_t __delete_vertex_map_CS( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  Key64Value56_t item = {
    .key = cell->annotation,            // thread id
    .value56 = APTR_AS_INTEGER( cell )  // vertex map address
  };

  // Delete map
  framehash_cell_t *vertex_tracker = (framehash_cell_t*)item.value56;
  if( vertex_tracker ) {
    vgx_Graph_t *self = (vgx_Graph_t*)processor->processor.input;
    int64_t *counter = (int64_t*)processor->processor.output;
    framehash_dynamic_t *dyn = &self->vtxmap_fhdyn;
    // Release all vertices tracked by this vertex tracker and erase their entries
    iFramehash.simple.Process( vertex_tracker, __release_vertex_CS, self, counter );
    // Destroy the vertex tracker we just emptied
    iFramehash.simple.Destroy( &vertex_tracker, dyn );
    // Force delete this entry in the graph's thread:vertex_tracker map
    FRAMEHASH_PROCESSOR_DELETE_CELL( processor, cell );
  }
  return 1;
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_destructor( vgx_Graph_t *self ) {
  if( self ) {

    // Serialization in progress? If so wait until complete.
    GRAPH_LOCK( self ) {
      // Another thread is currently serializing this graph, we have to wait.
      while( _vgx_is_serializing_CS( &self->readonly ) ) {
        WAIT_FOR_VERTEX_AVAILABLE( self, 250 );
      }

      GRAPH_SUSPEND_LOCK( self ) {
        iGraphEvent.NOCS.SetDefunct( self, -1 );
      } GRAPH_RESUME_LOCK;

      // Release any remaining readonly locks
      if( _vgx_is_readonly_CS( &self->readonly ) ) {
        while( _vxgraph_state__release_graph_readonly_CS( self ) > 0 );
      }

      // It's over.
      _vgx_graph_clear_ready_CS( self );

      // Stop Event Processor
      iGraphEvent.Destroy( self );
      //
      // ------------------

      // TODO: Wait for all query activity in this graph to complete
      // TODO: Wait for all acquired vertices in this graph to be released?


      // Closed for business
      CLOSE_GRAPH_OPERATION_CS( self );

      // [...] Stop Operation Processor
      //
      // NOTE: The processor may still be working on emitting operation data in the
      //       background. This call will not return until all operations have been
      //       emitted.
      iOperation.Destroy( self );

      // Stop clock
      _vxgraph_tick__destroy_CS( self );

      // ---
      // If provided, call the callback for any external cleanup
      // ---
      if( self->destructor_callback_hook ) {
        self->destructor_callback_hook( self, self->external_owner_object );
      }

      // External owner object
      self->external_owner_object = NULL;

      // Destructor callback hook
      self->destructor_callback_hook = NULL;

      // Property counter
      self->_nproperties_atomic = 0;

      // Vector counter
      self->_nvectors_atomic = 0;

      // Readonly
      _vgx_lock_readonly_CS( &self->readonly, false, false, false );

      // Arc heap utility
      if( self->arc_heap ) {
        COMLIB_OBJECT_DESTROY( self->arc_heap );
        self->arc_heap = NULL;
      }

      // Arc list utility
      if( self->arc_list ) {
        COMLIB_OBJECT_DESTROY( self->arc_list );
        self->arc_list = NULL;
      }

      // Vertex heap utility
      if( self->vertex_heap ) {
        COMLIB_OBJECT_DESTROY( self->vertex_heap );
        self->vertex_heap = NULL;
      }

      // Vertex list utility
      if( self->vertex_list ) {
        COMLIB_OBJECT_DESTROY( self->vertex_list );
        self->vertex_list = NULL;
      }

      // Acquired vertex map RO
      if( self->vtxmap_RO ) {
        iFramehash.simple.Process( self->vtxmap_RO, __delete_vertex_map_CS, self, &self->count_vtx_RO );
        iFramehash.simple.Destroy( &self->vtxmap_RO, &self->vtxmap_fhdyn );
      }

      // Acquired vertex map WL
      if( self->vtxmap_WL ) {
        iFramehash.simple.Process( self->vtxmap_WL, __delete_vertex_map_CS, self, &self->count_vtx_WL );
        iFramehash.simple.Destroy( &self->vtxmap_WL, &self->vtxmap_fhdyn );
      }

      // Evaluators
      iEvaluator.DestroyEvaluators( self );

      // Vertex type index directory
      // Vertex index
      _vxgraph_vxtable__destroy_index_CS( self );

      // Vertex allocator
      ivertexalloc.Delete( self );

      // Virtual Properties
      _vxvertex_property__virtual_properties_destroy( self );

      // Vertex property key/value maps
      _vxenum_prop__destroy_enumerator( self );
      
      // Relationship enumerator
      _vxenum_rel__destroy_enumerator( self );

      // Vertex type enumerator
      _vxenum_vtx__destroy_enumerator( self );

      // Property Allocator Context
      icstringalloc.DeleteContext( &self->property_allocator_context );

      // Ephemeral String Allocator Context
      icstringalloc.DeleteContext( &self->ephemeral_string_allocator_context );
      
      // Vertex Map Framehash dynamic
      iFramehash.dynamic.ClearDynamic( &self->vtxmap_fhdyn );

      // Property Framehash dynamic
      iFramehash.dynamic.ClearDynamic( &self->property_fhdyn );
      
      // Arcvector Framehash dynamic
      iFramehash.dynamic.ClearDynamic( &self->arcvector_fhdyn );

      // Similarity
      if( self->similarity ) {
        COMLIB_OBJECT_DESTROY( self->similarity );
        self->similarity = NULL;
      }

      // Recursion count
      self->recursion_count = 0;

      // State lock recursion counter
      self->__state_lock_count = 0;

      // Owner thread
      self->owner_threadid = 0;

      // Graph reverse size
      self->rev_size_atomic = 0;

      // Graph size
      self->_size_atomic = 0;

      // Graph order
      self->_order_atomic = 0;

      // Graph full path
      iString.Discard( &self->CSTR__fullpath );

      // Graph path
      iString.Discard( &self->CSTR__path );

      // Graph name
      iString.Discard( &self->CSTR__name );

    } GRAPH_RELEASE;

    // Wake event
    DEL_CONDITION_VARIABLE( &self->wake_event.cond );

    // Inarcs return condition
    DEL_CONDITION_VARIABLE( &self->return_inarcs.cond );

    // Vertex availability condition
    DEL_CONDITION_VARIABLE( &self->vertex_availability.cond );

    // Graph state lock
    DEL_CRITICAL_SECTION( &self->state_lock.lock );

    // Query counter lock
    DEL_CRITICAL_SECTION( &self->q_lock.lock );

    // Graph ID
    // Object typeinfo
    // Vtable
    igraphobject.Delete( self );

    // Graph has been deleted
    self = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Graph_representer( vgx_Graph_t *self, CStringQueue_t *output ) {
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )

  COMLIB_DefaultRepresenter( (const comlib_object_t*)self, output );
  PUT( "\n" );
  PUT( "<vgx_Graph_t at 0x%llx ", self );
  PUT( "name=%s ", CStringValue(self->CSTR__name) );
  if( self->CSTR__path )
    PUT( "path=%s ", CStringValue(self->CSTR__path) );
  else
    PUT( "path=<NOT PERSISTED> " );
  PUT( "vertices=%lld >", _vxgraph_vxtable__len_CS( self, VERTEX_TYPE_ENUMERATION_WILDCARD ) );
  PUT( "\n" );
  return output;
#undef PUT
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * Graph_name( const vgx_Graph_t *self ) {
  return self->CSTR__name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph__count_query_nolock( vgx_Graph_t *self, int64_t q_time_nanosec ) {
  if( q_time_nanosec >=0 ) {
    self->q_count++;
    self->q_time_nanosec_acc += q_time_nanosec;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph__reset_query_count_nolock( vgx_Graph_t *self ) {
  self->q_count = 0;
  self->q_time_nanosec_acc = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph__query_count_nolock( vgx_Graph_t *self ) {
  return self->q_count;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph__query_time_nanosec_acc_nolock( vgx_Graph_t *self ) {
  return self->q_time_nanosec_acc;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double Graph__query_time_average_nolock( vgx_Graph_t *self ) {
  double avg;
  if( self->q_count > 0 ) {
    avg = (self->q_time_nanosec_acc / 1e9) / self->q_count;
  }
  else {
    avg = 0.0;
  }
  return avg;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_set_destructor_hook( vgx_Graph_t *self, void *external_owner, void (*destructor_callback_hook)( vgx_Graph_t *self, void *external_owner ) ) {
  self->external_owner_object = external_owner;
  self->destructor_callback_hook = destructor_callback_hook;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_clear_external_owner( vgx_Graph_t *self ) {
  self->external_owner_object = NULL;
  self->destructor_callback_hook = NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_set_sysaux_command_callback( vgx_Graph_t *self, f_vgx_SystemAuxCommandCallback sysaux_cmd_callback ) {
  self->sysaux_cmd_callback = sysaux_cmd_callback;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_clear_sysaux_command_callback( vgx_Graph_t *self ) {
  self->sysaux_cmd_callback = Graph_system_aux_command_noop;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_set_system_sync_callback( vgx_Graph_t *self, f_vgx_SystemSyncCallback system_sync_callback ) {
  self->system_sync_callback = system_sync_callback;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_clear_system_sync_callback( vgx_Graph_t *self ) {
  self->system_sync_callback = Graph_system_sync_noop;
}



/*******************************************************************//**
 *
 * NOTE: Caller owns returned string
 ***********************************************************************
 */
static const char * Graph_full_path( const vgx_Graph_t *self ) {
  return CStringValue( self->CSTR__fullpath );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_bulk_serialize( vgx_Graph_t *self, int timeout_ms, bool force, bool remote, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
#ifdef HASVERBSOE
  VXGRAPH_OBJECT_VERBOSE( self, 0x5FF, "Persisting" );
#endif
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_execution_timing_budget( 0, timeout_ms );
  int64_t nqwords = iSerialization.BulkSerialize( self, &timing_budget, force, remote, CSTR__error );
  if( _vgx_is_execution_halted( &timing_budget ) ) {
    __set_access_reason( reason, timing_budget.reason );
  }
  return nqwords;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_dump( vgx_Graph_t *self ) {
  GRAPH_LOCK( self ) {
    const size_t ncl = qwsizeof( vgx_Graph_t ) / qwsizeof( cacheline_t );
    QWORD *qwords = (QWORD*)self;
    QWORD *qcur = qwords;
    QWORD *Q;
    cxmalloc_family_vtable_t *iAlloc = (cxmalloc_family_vtable_t*)COMLIB_CLASS_VTABLE( cxmalloc_family_t );
    cxmalloc_family_t *alloc;
    int64_t bytes;
    int64_t active;
    double util;
#define GetSequenceLength( S ) ((S) ? CALLABLE(S)->Length(S) : 0)
#define GetFramehashItems( F ) ((F) ? CALLABLE(F)->Items(F) : 0)
#define GetOpBufferSize( B ) ((B) ? iOpBuffer.Size(B) : 0)

    BEGIN_CXLIB_OBJ_DUMP( vgx_Graph_t, self ) {
      CXLIB_OSTREAM( "********** GRAPH DUMP **********" );
      CXLIB_OSTREAM( "RAW DATA" );
      CXLIB_OSTREAM( "-- | ---------------- ---------------- ---------------- ---------------- ---------------- ---------------- ---------------- ----------------" );
      for( unsigned i=0; i<ncl; i++ ) {
        PRINT_OSTREAM( "%2u |", i+1 );
        for( size_t j=0; j<qwsizeof( cacheline_t ); j++ ) {
          PRINT_OSTREAM( " %016llX", *qcur++ );
        }
        CXLIB_OSTREAM( "" );
      }
      CXLIB_OSTREAM( "-- | ---------------- ---------------- ---------------- ---------------- ---------------- ---------------- ---------------- ----------------" );
      CXLIB_OSTREAM( "" );
      CXLIB_OSTREAM( " 1: -------- OBJECT BASE -------------" );
      CXLIB_OSTREAM( "vtable              : %p", self->vtable );
      CXLIB_OSTREAM( "typeinfo            : %02x %02x %02x %02x %05x %05x ", self->typeinfo.tp_basetype, self->typeinfo.tp_flags, self->typeinfo.tp_class, self->typeinfo.__tp_rsv, self->typeinfo.tp_idtype, self->typeinfo.tp_idstrlen );
      CXLIB_OSTREAM( "obid                : %016llx%016llx", self->obid.H, self->obid.L );
      CXLIB_OSTREAM( "instance_id         : %llu", self->instance_id );
      CXLIB_OSTREAM( "CSTR__name          : (CString_t*) %llp (\"%s\")", self->CSTR__name, CStringValueDefault(self->CSTR__name, "") );
      CXLIB_OSTREAM( "CSTR__path          : (CString_t*) %llp (\"%s\")", self->CSTR__path, CStringValueDefault(self->CSTR__path, "") );
      CXLIB_OSTREAM( "CSTR__fullpath      : (CString_t*) %llp (\"%s\")", self->CSTR__fullpath, CStringValueDefault(self->CSTR__fullpath, "") );


      CXLIB_OSTREAM( " 2: -------- INDEX / EVAL/ SIM -------" );
      if( (alloc = self->vertex_allocator) != NULL ) {
        CXLIB_OSTREAM( "vertex_allocator    : (cxmalloc_family_t) %p [%lld bytes, %lld active items, %.2f%% util]", alloc, iAlloc->Bytes( alloc ), iAlloc->Active( alloc ), 100.0*iAlloc->Utilization( alloc ) );
      }
      else {
        CXLIB_OSTREAM( "vertex_allocator    : NULL" );
      }
      CXLIB_OSTREAM( "vxtable             : (framehash_t*) %llp [%lld items]", self->vxtable, GetFramehashItems(self->vxtable) );
      CXLIB_OSTREAM( "vxtypeidx           : (framehash_t**) %llp", self->vxtypeidx );
      CXLIB_OSTREAM( "vtxmap_WL           : (framehash_cell_t*) %llp", self->vtxmap_WL, self->vtxmap_WL ? iFramehash.simple.Length( self->vtxmap_WL ) : 0 );
      CXLIB_OSTREAM( "vtxmap_RO           : (framehash_cell_t*) %llp", self->vtxmap_RO, self->vtxmap_RO ? iFramehash.simple.Length( self->vtxmap_RO ) : 0 );
      CXLIB_OSTREAM( "evaluators          : (framehash_t*) %llp", self->evaluators );
      CXLIB_OSTREAM( "similarity          : (vgx_Similarity_t*) %llp", self->similarity );
      CXLIB_OSTREAM( "__rsv_2_8           : %llu", self->__rsv_2_8 );


      CXLIB_OSTREAM( " 3: -------- STATE LOCK -------" );
      Q = (QWORD*)&self->state_lock;
      CXLIB_OSTREAM( "state_lock          : (CS_LOCK) %016llX %016llX %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5], Q[6], Q[7] );


      CXLIB_OSTREAM( " 4: -------- STATE -------" );
      CXLIB_OSTREAM( "owner_threadid          : %u", (unsigned int)_vgx_graph_owner_CS( self ) );
      CXLIB_OSTREAM( "recursion_count         : %u", (unsigned int)self->recursion_count );
      CXLIB_OSTREAM( "__rsv_4_1_2_2   t       : %u", (unsigned int)self->__rsv_4_1_2_2 );
      CXLIB_OSTREAM( "__statelock_count       : %d", (int)self->__state_lock_count );
      vgx_control_state_t *ctrl = &self->control;
      CXLIB_OSTREAM( "control" );
      CXLIB_OSTREAM( "  .disallow_WL          : %d", (int)ctrl->disallow_WL );
      CXLIB_OSTREAM( "  .ready                : %d", (int)ctrl->ready );
      CXLIB_OSTREAM( "  .vtx_busy             : %d", (int)ctrl->vtx_busy );
      CXLIB_OSTREAM( "  .__rsv3               : %d", (int)ctrl->__rsv3 );
      CXLIB_OSTREAM( "  .__rsv4               : %d", (int)ctrl->__rsv4 );
      CXLIB_OSTREAM( "  .local_only           : %d", (int)ctrl->local_only );
      CXLIB_OSTREAM( "  .__rsv6               : %d", (int)ctrl->__rsv6 );
      CXLIB_OSTREAM( "  .__rsv7               : %d", (int)ctrl->__rsv7 );
      CXLIB_OSTREAM( "  .__rsv8               : %d", (int)ctrl->__rsv8);
      CXLIB_OSTREAM( "  .__rsv                : %u", (unsigned)ctrl->__rsv);
      CXLIB_OSTREAM( "__rsv_4_2_2             : %u", (unsigned int)self->__rsv_4_2_2 );
      vgx_readonly_state_t *ro = &self->readonly;
      CXLIB_OSTREAM( "readonly" );
      CXLIB_OSTREAM( "  .__n_disallowed       : %d", (int)ro->__n_disallowed );
      CXLIB_OSTREAM( "  .__n_readers          : %d", (int)ro->__n_readers );
      CXLIB_OSTREAM( "  .__n_explicit         : %d", (int)ro->__n_explicit );
      CXLIB_OSTREAM( "  .__n_roreq            : %d", (int)ro->__n_roreq );
      CXLIB_OSTREAM( "  .__n_writers_writing  : %d", (int)ro->__n_writers_waiting );
      CXLIB_OSTREAM( "  .__x_thread_id        : %u", ro->__xthread_id );
      CXLIB_OSTREAM( "  .__flags" );
      CXLIB_OSTREAM( "    .is_locked          : %u", (unsigned)ro->__flags.is_locked );
      CXLIB_OSTREAM( "    .xlock              : %u", (unsigned)ro->__flags.xlock );
      CXLIB_OSTREAM( "    .bit" );
      CXLIB_OSTREAM( "      .EVP_suspended    : %u", (unsigned)ro->__flags.bit.EVP_suspended );
      CXLIB_OSTREAM( "      .OP_suspended     : %u", (unsigned)ro->__flags.bit.OP_suspended );
      CXLIB_OSTREAM( "      .TX_in_suspended  : %u", (unsigned)ro->__flags.bit.TX_in_suspended );
      CXLIB_OSTREAM( "      .__rsv4           : %u", (unsigned)ro->__flags.bit.__rsv4 );
      CXLIB_OSTREAM( "      .is_serializing   : %u", (unsigned)ro->__flags.bit.is_serializing );
      CXLIB_OSTREAM( "      .__rsv6           : %u", (unsigned)ro->__flags.bit.__rsv6 );
      CXLIB_OSTREAM( "      .__rsv7           : %u", (unsigned)ro->__flags.bit.__rsv7 );
      CXLIB_OSTREAM( "      .__rsv8           : %u", (unsigned)ro->__flags.bit.__rsv8 );
      CXLIB_OSTREAM( "operation               : (tptr_t) %016llx %lld 0x%x", self->operation.qword, TPTR_AS_INTEGER( &self->operation ), TPTR_AS_TAG( &self->operation ) );
      CXLIB_OSTREAM( "count_vtx_WL            : %lld", _vgx_graph_get_vertex_WL_count_CS( self ) );
      CXLIB_OSTREAM( "count_vtx_RO            : %lld", _vgx_graph_get_vertex_RO_count_CS( self ) );
      CXLIB_OSTREAM( "_order_atomic           : %lld", GraphOrder( self ) );
                

      CXLIB_OSTREAM( " 5-7: ------ ARCVECTOR DYNAMIC --------" );
      CXLIB_OSTREAM( "arcvector_fhdyn     : (framehash_dynamic_t %d bytes)", (int)sizeof( framehash_dynamic_t ) );

      CXLIB_OSTREAM( " 8: ------ COUNTERS ---------" );
      CXLIB_OSTREAM( "_size_atomic        : %lld", GraphSize( self ) );
      CXLIB_OSTREAM( "_nvectors_atomic    : %lld", GraphVectorCount( self ) );
      CXLIB_OSTREAM( "_nproperties_atomic : %lld", GraphPropCount( self ) );
      CXLIB_OSTREAM( "rev_size_atomic     : %lld", ATOMIC_READ_i64( &self->rev_size_atomic ) );
      CXLIB_OSTREAM( "__rsv_8_5           : %llu", self->__rsv_8_5 );
      CXLIB_OSTREAM( "__rsv_8_6           : %llu", self->__rsv_8_6 );
      CXLIB_OSTREAM( "__rsv_8_7           : %llu", self->__rsv_8_7 );
      CXLIB_OSTREAM( "__rsv_8_8           : %llu", self->__rsv_8_8 );


      CXLIB_OSTREAM( " 9-11: ------ PROPERTY DYNAMIC ---------" );
      CXLIB_OSTREAM( "property_fhdyn      : (framehash_dynamic_t %d bytes)", (int)sizeof( framehash_dynamic_t ) );


      CXLIB_OSTREAM( "12: -------- ALLOCATORS / CODECS ------ " );
      CXLIB_OSTREAM( "vxtype_encoder      : (framehash_t*) %llp [%lld items]", self->vxtype_encoder, GetFramehashItems(self->vxtype_encoder) );
      CXLIB_OSTREAM( "vxtype_decoder      : (framehash_t*) %llp [%lld items]", self->vxtype_decoder, GetFramehashItems(self->vxtype_decoder) );
      CXLIB_OSTREAM( "rel_encoder         : (framehash_t*) %llp [%lld items]", self->rel_encoder, GetFramehashItems(self->rel_encoder) );
      CXLIB_OSTREAM( "rel_decoder         : (framehash_t*) %llp [%lld items]", self->rel_decoder, GetFramehashItems(self->rel_decoder) );
      if( self->ephemeral_string_allocator_context && (alloc = (cxmalloc_family_t*)self->ephemeral_string_allocator_context->allocator) != NULL ) {
        bytes = iAlloc->Bytes(alloc);
        active = iAlloc->Active(alloc);
        util = 100.0 * iAlloc->Utilization(alloc);
        CXLIB_OSTREAM( "ephemeral_string_allocator_context  : (object_allocator_context_t*) %llp [%lld bytes, %lld active items, %.2f%% util]", self->ephemeral_string_allocator_context, bytes, active, util );
      }
      else {
        CXLIB_OSTREAM( "ephemeral_string_allocator_context  : (object_allocator_context_t*) NULL" );
      }
      if( self->property_allocator_context && (alloc = (cxmalloc_family_t*)self->property_allocator_context->allocator) != NULL ) {
        bytes = iAlloc->Bytes(alloc);
        active = iAlloc->Active(alloc);
        util = 100.0 * iAlloc->Utilization(alloc);
        CXLIB_OSTREAM( "property_allocator_context          : (object_allocator_context_t*) %llp [%lld bytes, %lld active items, %.2f%% util]", self->property_allocator_context, bytes, active, util );
      }
      else {
        CXLIB_OSTREAM( "property_allocator_context          : (object_allocator_context_t*) NULL" );
      }
      CXLIB_OSTREAM( "vxprop_keymap       : (framehash_t*) %p [%lld items]", self->vxprop_keymap, GetFramehashItems(self->vxprop_keymap) );
      CXLIB_OSTREAM( "vxprop_valmap       : (framehash_t*) %p [%lld items]", self->vxprop_valmap, GetFramehashItems(self->vxprop_valmap) );


      CXLIB_OSTREAM( "13-14: -------- VIRTUAL PROPERTIES ------ " );
      Q = (QWORD*)&self->vprop.lock;
      CXLIB_OSTREAM( "vprop.lock          : (CS_LOCK) %016llX %016llX %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5], Q[6], Q[7] );
      CXLIB_OSTREAM( "vprop.fd            : %d", self->vprop.fd );
      CXLIB_OSTREAM( "vprop.ready         : %d", self->vprop.ready );
      CXLIB_OSTREAM( "vprop.commit        : %lld", self->vprop.commit );
      CXLIB_OSTREAM( "vprop.bytes         : %lld", self->vprop.bytes );
      CXLIB_OSTREAM( "vprop.count         : %lld", self->vprop.count );
      CXLIB_OSTREAM( "vprop.__rsv_14_5    : %lld", self->vprop.__rsv_14_5 );
      CXLIB_OSTREAM( "vprop.__rsv_14_6    : %lld", self->vprop.__rsv_14_6 );
      CXLIB_OSTREAM( "vprop.__rsv_14_7    : %lld", self->vprop.__rsv_14_7 );
      CXLIB_OSTREAM( "vprop.__rsv_14_8    : %lld", self->vprop.__rsv_14_8 );


      CXLIB_OSTREAM( "15-17: ------ VERTEX MAP DYNAMIC ------- " );
      CXLIB_OSTREAM( "vtxmap_fhdyn        : (framehash_dynamic_t %d bytes)", (int)sizeof( framehash_dynamic_t ) );


      CXLIB_OSTREAM( "18: ------- VERTEX SIGNAL / SUPPORT ---" );
      Q = (QWORD*)&self->vertex_availability.cond;
      CXLIB_OSTREAM( "vertex_availability: (CS_COND) %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5] );
      CXLIB_OSTREAM( "vertex_list         : (Cm256iList_t*) %llp [%lld items]", self->vertex_list, GetSequenceLength(self->vertex_list) );
      CXLIB_OSTREAM( "vertex_heap         : (Cm256iHeap_t*) %llp [%lld items]", self->vertex_heap, GetSequenceLength(self->vertex_heap) );


      CXLIB_OSTREAM( "19-20: ------- QUERY ---" );
      Q = (QWORD*)&self->q_lock;
      CXLIB_OSTREAM( "q_lock              : (CS_LOCK) %016llX %016llX %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5], Q[6], Q[7] );
      CXLIB_OSTREAM( "q_pri_req           : %d", self->q_pri_req );
      CXLIB_OSTREAM( "__rsv_20_1_2        : %d", self->__rsv_20_1_2 );
      CXLIB_OSTREAM( "q_count             : %lld", self->q_count );
      CXLIB_OSTREAM( "q_time_nanosec_acc  : %lld", self->q_time_nanosec_acc );
      CXLIB_OSTREAM( "__rsv_20_4          : %llu", self->__rsv_20_4 );
      CXLIB_OSTREAM( "__rsv_20_5          : %llu", self->__rsv_20_5 );
      CXLIB_OSTREAM( "__rsv_20_6          : %llu", self->__rsv_20_6 );
      CXLIB_OSTREAM( "__rsv_20_7          : %llu", self->__rsv_20_7 );
      CXLIB_OSTREAM( "__rsv_20_8          : %llu", self->__rsv_20_8 );


      CXLIB_OSTREAM( "21: ------- TIMER / READONLY STATE ----" );
      CXLIB_OSTREAM( "TIC.TASK                : (comlib_task_t*) %llp", self->TIC.TASK );
      CXLIB_OSTREAM( "TIC.graph               : (vgx_Graph_t*) %llp", self->TIC.graph );
      CXLIB_OSTREAM( "TIC.tms_atomic          : %lld", _vgx_graph_milliseconds( self ) );
      CXLIB_OSTREAM( "TIC.ts_atomic           : %u", _vgx_graph_seconds( self ) );
      CXLIB_OSTREAM( "TIC.t0_atomic           : %u", _vgx_graph_t0( self ) );
      CXLIB_OSTREAM( "TIC.offset_tms_atomic   : %d", ATOMIC_READ_i32( &self->TIC.offset_tms_atomic ) );
      CXLIB_OSTREAM( "TIC.inception_t0_atomic : %u", ATOMIC_READ_u32( &self->TIC.inception_t0_atomic ) );
      CXLIB_OSTREAM( "__rsv_21_6              : %llu", self->__rsv_21_6 );
      CXLIB_OSTREAM( "__rsv_21_7              : %llu", self->__rsv_21_7 );
      CXLIB_OSTREAM( "__rsv_21_8              : %llu", self->__rsv_21_8 );


      CXLIB_OSTREAM( "22: ------- INARCS SIGNAL / SUPPORT ---" );
      Q = (QWORD*)&self->return_inarcs;
      CXLIB_OSTREAM( "return_inarcs       : (CS_COND) %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5] );
      CXLIB_OSTREAM( "arc_list            : (Cm256iList_t*) %llp [%lld items]", self->arc_list, GetSequenceLength(self->arc_list) );
      CXLIB_OSTREAM( "arc_heap            : (Cm256iHeap_t*) %llp [%lld items]", self->arc_heap, GetSequenceLength(self->arc_heap) );


      CXLIB_OSTREAM( "23-26: ------- EVENT PROCESSOR -----------" );
      CXLIB_OSTREAM( "-- 1 -- (23)" );
      CXLIB_OSTREAM( "EVP.TASK                        : (comlib_task_t*) %llp", self->EVP.TASK );
      CXLIB_OSTREAM( "EVP.graph                       : (vgx_Graph_t*) %llp", self->EVP.graph );
      CXLIB_OSTREAM( "EVP.EXECUTOR                    : (vgx_ExecutionJobDescriptor_t*) %llp", self->EVP.EXECUTOR );
      CXLIB_OSTREAM( "EVP.executor_thread_id          : %u", (unsigned int)self->EVP.executor_thread_id );
      CXLIB_OSTREAM( "EVP.ready                       : %d", self->EVP.ready );
      CXLIB_OSTREAM( "EVP.params" );
      CXLIB_OSTREAM( "  .operation_timeout_ms         : %d", self->EVP.params.operation_timeout_ms );
      CXLIB_OSTREAM( "  .task_WL.state                : %u", (unsigned)self->EVP.params.task_WL.state );
      CXLIB_OSTREAM( "  .task_WL.flags" );
      CXLIB_OSTREAM( "    .req_flush                  : %u", (unsigned)self->EVP.params.task_WL.flags.req_flush );
      CXLIB_OSTREAM( "    .__rsv2                     : %u", (unsigned)self->EVP.params.task_WL.flags.__rsv_2 );
      CXLIB_OSTREAM( "    .__rsv3                     : %u", (unsigned)self->EVP.params.task_WL.flags.__rsv_3 );
      CXLIB_OSTREAM( "    .__rsv4                     : %u", (unsigned)self->EVP.params.task_WL.flags.__rsv_4 );
      CXLIB_OSTREAM( "    .ack_flushed                : %u", (unsigned)self->EVP.params.task_WL.flags.ack_flushed );
      CXLIB_OSTREAM( "    .__rsv6                     : %u", (unsigned)self->EVP.params.task_WL.flags.__rsv_6 );
      CXLIB_OSTREAM( "    .__rsv7                     : %u", (unsigned)self->EVP.params.task_WL.flags.__rsv_7 );
      CXLIB_OSTREAM( "    .defunct                    : %u", (unsigned)self->EVP.params.task_WL.flags.defunct );
      CXLIB_OSTREAM( "  .graph_CS.__bits              : %u", (unsigned)self->EVP.params.graph_CS.__bits );
      CXLIB_OSTREAM( "EVP.Schedule.LongTerm           : (framehash_t*) %llp [%lld items]", self->EVP.Schedule.LongTerm, GetFramehashItems(self->EVP.Schedule.LongTerm) );
      CXLIB_OSTREAM( "EVP.Schedule.MediumTerm         : (framehash_t*) %llp [%lld items]", self->EVP.Schedule.MediumTerm, GetFramehashItems(self->EVP.Schedule.MediumTerm) );
      CXLIB_OSTREAM( "EVP.Schedule.ShortTerm          : (framehash_t*) %llp [%lld items]", self->EVP.Schedule.ShortTerm, GetFramehashItems(self->EVP.Schedule.ShortTerm) );
      CXLIB_OSTREAM( "-- 2 -- (24)" );
      CXLIB_OSTREAM( "EVP.Monitor.Queue               : (vgx_VertexEventQueue_t*) %llp [%lld items]", self->EVP.Monitor.Queue, GetSequenceLength(self->EVP.Monitor.Queue) );
      CXLIB_OSTREAM( "EVP.Executor.Queue              : (vgx_VertexEventQueue_t*) %llp [%lld items]", self->EVP.Executor.Queue, GetSequenceLength(self->EVP.Executor.Queue) );
      CXLIB_OSTREAM( "EVP.ActionTriggers              : (vgx_EventActionTrigger_t**) %llp", self->EVP.ActionTriggers );
      CXLIB_OSTREAM( "EVP.__rsv_2_4                   : %llu", self->EVP.__rsv_2_4 );
      CXLIB_OSTREAM( "EVP.__rsv_2_5                   : %llu", self->EVP.__rsv_2_5 );
      CXLIB_OSTREAM( "EVP.__rsv_2_6                   : %llu", self->EVP.__rsv_2_6 );
      CXLIB_OSTREAM( "EVP.__rsv_2_7                   : %llu", self->EVP.__rsv_2_7 );
      CXLIB_OSTREAM( "EVP.__rsv_2_8                   : %llu", self->EVP.__rsv_2_8 );
      CXLIB_OSTREAM( "-- 3 -- (25)" );
      Q = (QWORD*)&self->EVP.PublicAPI.Lock;
      CXLIB_OSTREAM( "EVP.PublicAPI.Lock              : (CS_LOCK) %016llX %016llX %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5], Q[6], Q[7] );
      CXLIB_OSTREAM( "-- 4 -- (26)" );
      CXLIB_OSTREAM( "EVP.PublicAPI.Queue             : (vgx_VertexEventQueue_t*) %llp [%lld items]", self->EVP.PublicAPI.Queue, GetSequenceLength(self->EVP.PublicAPI.Queue) );
      CXLIB_OSTREAM( "EVP.__rsv_4_2                   : %llu", self->EVP.PublicAPI.__rsv_4_2 );
      CXLIB_OSTREAM( "EVP.__rsv_4_3                   : %llu", self->EVP.PublicAPI.__rsv_4_3 );
      CXLIB_OSTREAM( "EVP.__rsv_4_4                   : %llu", self->EVP.PublicAPI.__rsv_4_4 );
      CXLIB_OSTREAM( "EVP.__rsv_4_5                   : %llu", self->EVP.PublicAPI.__rsv_4_5 );
      CXLIB_OSTREAM( "EVP.__rsv_4_6                   : %llu", self->EVP.PublicAPI.__rsv_4_6 );
      CXLIB_OSTREAM( "EVP.__rsv_4_7                   : %llu", self->EVP.PublicAPI.__rsv_4_7 );
      CXLIB_OSTREAM( "EVP.__rsv_4_8                   : %llu", self->EVP.PublicAPI.__rsv_4_8 );


      CXLIB_OSTREAM( "27: ------- TRANSACTION ---------" );
      CXLIB_OSTREAM( "tx_id_out                       : %016llx%016llx", self->tx_id_out.H, self->tx_id_out.L );
      CXLIB_OSTREAM( "tx_serial_out                   : %lld", self->tx_serial_out );
      CXLIB_OSTREAM( "tx_count_out                    : %lld", self->tx_count_out );
      CXLIB_OSTREAM( "tx_id_in                        : %016llx%016llx", self->tx_id_in.H, self->tx_id_in.L );
      CXLIB_OSTREAM( "tx_serial_in                    : %lld", self->tx_serial_in );
      CXLIB_OSTREAM( "tx_count_in                     : %lld", self->tx_count_in );


      CXLIB_OSTREAM( "28: ------- TRANSACTION ---------" );
      CXLIB_OSTREAM( "durable_tx_id                   : %016llx%016llx", self->durable_tx_id.H, self->durable_tx_id.L );
      CXLIB_OSTREAM( "durable_tx_serial               : %llu", self->durable_tx_serial );
      CXLIB_OSTREAM( "sysmaster_tx_serial_0           : %llu", self->sysmaster_tx_serial_0 );
      CXLIB_OSTREAM( "sysmaster_tx_serial             : %llu", self->sysmaster_tx_serial );
      CXLIB_OSTREAM( "sysmaster_tx_serial_count       : %llu", self->sysmaster_tx_serial_count );
      CXLIB_OSTREAM( "persisted_ts                    : %lld", self->persisted_ts );
      CXLIB_OSTREAM( "tx_input_lag_ms_CS              : %lld", self->tx_input_lag_ms_CS );


      CXLIB_OSTREAM( "29-31: ------- OPERATION EMITTER ---------" );
      CXLIB_OSTREAM( "-- 1 -- (29)" );
      CXLIB_OSTREAM( "OP.emitter.TASK                 : (comlib_task_t*) %p", self->OP.emitter.TASK );
      CXLIB_OSTREAM( "OP.emitter.control" );
      CXLIB_OSTREAM( "  .throttle_CS                  : %d", self->OP.emitter.control.throttle_CS );
      CXLIB_OSTREAM( "  .drain_TCS                    : %d", (int)self->OP.emitter.control.drain_TCS );
      CXLIB_OSTREAM( "  .defercommit_CS               : %d", (int)self->OP.emitter.control.defercommit_CS );
      CXLIB_OSTREAM( "  .flags_CS" );
      CXLIB_OSTREAM( "    .defunct                    : %u", self->OP.emitter.control.flag_CS.defunct );
      CXLIB_OSTREAM( "    .flush                      : %u", self->OP.emitter.control.flag_CS.flush );
      CXLIB_OSTREAM( "    .suspended                  : %u", self->OP.emitter.control.flag_CS.suspended );
      CXLIB_OSTREAM( "    .disabled                   : %u", self->OP.emitter.control.flag_CS.disabled );
      CXLIB_OSTREAM( "    .ready                      : %u", self->OP.emitter.control.flag_CS.ready );
      CXLIB_OSTREAM( "    .running                    : %u", self->OP.emitter.control.flag_CS.running );
      CXLIB_OSTREAM( "    .opmuted                    : %u", self->OP.emitter.control.flag_CS.opmuted );
      CXLIB_OSTREAM( "    .heartbeat                  : %u", self->OP.emitter.control.flag_CS.heartbeat );
      CXLIB_OSTREAM( "OP.emitter.graph                : (vgx_Graph_t*) %llp", self->OP.emitter.graph );
      CXLIB_OSTREAM( "OP.emitter.opid                 : %lld", self->OP.emitter.opid );
      CXLIB_OSTREAM( "OP.emitter.op_buffer            : (vgx_OperationBuffer_t*) %llp [%lld items]", self->OP.emitter.op_buffer, GetOpBufferSize(self->OP.emitter.op_buffer) );
      CXLIB_OSTREAM( "OP.emitter.n_inflight_CS        : %lld", self->OP.emitter.n_inflight_CS );
      CXLIB_OSTREAM( "OP.emitter.lxw_balance          : %lld", self->OP.emitter.lxw_balance );
      CXLIB_OSTREAM( "OP.emitter.commit_deadline_ms   : %lld", self->OP.emitter.commit_deadline_ms );
      CXLIB_OSTREAM( "-- 2 -- (30)" );
      CXLIB_OSTREAM( "OP.emitter.private_commit       : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.private_commit, GetSequenceLength(self->OP.emitter.private_commit) );
      CXLIB_OSTREAM( "OP.emitter.private_retire       : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.private_retire, GetSequenceLength(self->OP.emitter.private_retire) );
      CXLIB_OSTREAM( "OP.emitter.capture_pool_CS      : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.capture_pool_CS, GetSequenceLength(self->OP.emitter.capture_pool_CS) );
      CXLIB_OSTREAM( "OP.emitter.commit_pending_CS    : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.commit_pending_CS, GetSequenceLength(self->OP.emitter.commit_pending_CS) );
      CXLIB_OSTREAM( "OP.emitter.commit_swap          : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.commit_swap, GetSequenceLength(self->OP.emitter.commit_swap) );
      CXLIB_OSTREAM( "OP.emitter.cstring_discard      : (CQwordQueue_t*) %llp [%lld items]", self->OP.emitter.cstring_discard, GetSequenceLength(self->OP.emitter.cstring_discard) );
      CXLIB_OSTREAM( "OP.emitter.opid_last_emit       : %lld", self->OP.emitter.opid_last_emit );
      CXLIB_OSTREAM( "OP.emitter.opid_last_commit_CS  : %lld", self->OP.emitter.opid_last_commit_CS );
      CXLIB_OSTREAM( "-- 3 -- (31)" );
      Q = (QWORD*)&self->OP.emitter.opstream_ready;
      CXLIB_OSTREAM( "OP.emitter.opstream_ready       : (CS_COND) %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5] );
      CXLIB_OSTREAM( "OP.emitter.n_uncommitted" );
      CXLIB_OSTREAM( "  .operations                   : %d", self->OP.emitter.n_uncommitted.operations );
      CXLIB_OSTREAM( "  .opcodes                      : %d", self->OP.emitter.n_uncommitted.opcodes );
      CXLIB_OSTREAM( "OP.emitter.sz_private_commit_CS : %lld", self->OP.emitter.sz_private_commit_CS );


      CXLIB_OSTREAM( "32-34: ------- OPERATION SYSTEM OUTPUT ---" );
      CXLIB_OSTREAM( "-- 1 -- (32)" );
      CXLIB_OSTREAM( "OP.system.TASK                 : (comlib_task_t*) %p", self->OP.system.TASK );
      CXLIB_OSTREAM( "OP.system.state_CS" );
      CXLIB_OSTREAM( "  .state_change_owner          : %u", self->OP.system.state_CS.state_change_owner );
      CXLIB_OSTREAM( "  .readonly                    : %d", (int)self->OP.system.state_CS.readonly );
      CXLIB_OSTREAM( "  .state_change_recursion      : %d", (int)self->OP.system.state_CS.state_change_recursion );
      CXLIB_OSTREAM( "  .flags" );
      CXLIB_OSTREAM( "    .sync_in_progress          : %u", (unsigned)self->OP.system.state_CS.flags.sync_in_progress );
      CXLIB_OSTREAM( "    .request_cancel_sync       : %u", (unsigned)self->OP.system.state_CS.flags.request_cancel_sync );
      CXLIB_OSTREAM( "    .__rsv_3                   : %u", (unsigned)self->OP.system.state_CS.flags._rsv3 );
      CXLIB_OSTREAM( "    .__rsv_4                   : %u", (unsigned)self->OP.system.state_CS.flags._rsv4 );
      CXLIB_OSTREAM( "    .runnning                  : %u", (unsigned)self->OP.system.state_CS.flags.running );
      CXLIB_OSTREAM( "    .__rsv_6                   : %u", (unsigned)self->OP.system.state_CS.flags._rsv6 );
      CXLIB_OSTREAM( "    .__rsv_7                   : %u", (unsigned)self->OP.system.state_CS.flags._rsv7 );
      CXLIB_OSTREAM( "    .__rsv_8                   : %u", (unsigned)self->OP.system.state_CS.flags._rsv8 );
      CXLIB_OSTREAM( "OP.system.progress_CS          : (vgx_OperationSystemProgressCounters_t*) %llp", self->OP.system.progress_CS );
      CXLIB_OSTREAM( "OP.system.in_feed_limits_CS    : (vgx_OperationFeedRates_t*) %llp", self->OP.system.in_feed_limits_CS );
      CXLIB_OSTREAM( "OP.system.running_tx_input_rate_CS  : %g", self->OP.system.running_tx_input_rate_CS );
      CXLIB_OSTREAM( "OP.system.running_tx_output_rate_CS : %g", self->OP.system.running_tx_output_rate_CS );
      CXLIB_OSTREAM( "OP.system.set.prefix           : %016llX", self->OP.system.set.prefix );
      CXLIB_OSTREAM( "OP.system.set.id               : %016llX", self->OP.system.set.id );
      CXLIB_OSTREAM( "OP.system.set.id_offset        : %llu", self->OP.system.set.id_offset );
      CXLIB_OSTREAM( "-- 2 -- (33)" );
      CXLIB_OSTREAM( "OP.system.graph                : (vgx_Graph_t*) %llp", self->OP.system.graph );
      CXLIB_OSTREAM( "OP.system.producers            : (vgx_TransactionalProducers_t*) %llp", self->OP.system.producers );
      CXLIB_OSTREAM( "OP.system.consumer             : (vgx_TransactionalConsumerService_t*) %llp", self->OP.system.consumer );
      CXLIB_OSTREAM( "OP.system.validator            : (vgx_OperationParser_t*) %llp", self->OP.system.validator );
      CXLIB_OSTREAM( "OP.system.out_backlog" );
      CXLIB_OSTREAM( "  .n_bytes                     : %lld", self->OP.system.out_backlog.n_bytes );
      CXLIB_OSTREAM( "  .n_tx                        : %lld", self->OP.system.out_backlog.n_tx );
      CXLIB_OSTREAM( "  .latency_ms                  : %lld", self->OP.system.out_backlog.latency_ms );
      CXLIB_OSTREAM( "  .y                           : %lld", self->OP.system.out_backlog.y );
      CXLIB_OSTREAM( "-- 3 --(34)" );
      vgx_OperationCounters_t *opcnt = &self->OP.system.out_counters_CS;
      CXLIB_OSTREAM( "OP.system.out_counters_CS" );
      CXLIB_OSTREAM( "  .n_transactions              : %lld", opcnt->n_transactions );
      CXLIB_OSTREAM( "  .n_operations                : %lld", opcnt->n_operations );
      CXLIB_OSTREAM( "  .n_opcodes                   : %lld", opcnt->n_opcodes );
      CXLIB_OSTREAM( "  .n_bytes                     : %lld", opcnt->n_bytes );
      opcnt = &self->OP.system.in_counters_CS;
      CXLIB_OSTREAM( "OP.system.in_counters_CS" );
      CXLIB_OSTREAM( "  .n_transactions              : %lld", opcnt->n_transactions );
      CXLIB_OSTREAM( "  .n_operations                : %lld", opcnt->n_operations );
      CXLIB_OSTREAM( "  .n_opcodes                   : %lld", opcnt->n_opcodes );
      CXLIB_OSTREAM( "  .n_bytes                     : %lld", opcnt->n_bytes );


      CXLIB_OSTREAM( "35-38: ------- OPERATION PARSER ----------" );
      CXLIB_OSTREAM( "-- 1 -- (35)" );
      CXLIB_OSTREAM( "OP.parser.state                : (OperationProcessorParserState) %08x", self->OP.parser.state );
      CXLIB_OSTREAM( "OP.parser.optype               : (OperationProcessorOpType) %08x", self->OP.parser.optype );
      CXLIB_OSTREAM( "OP.parser.op_graph             : (vgx_Graph_t*) %llp", self->OP.parser.op_graph );
      CXLIB_OSTREAM( "OP.parser.op_vertex_WL         : (vgx_Vertex_t*) %llp", self->OP.parser.op_vertex_WL );
      CXLIB_OSTREAM( "OP.parser.opid                 : %lld", self->OP.parser.opid );
      CXLIB_OSTREAM( "OP.parser.parsef               : (f_parse_operator) %llp", self->OP.parser.parsef );
      CXLIB_OSTREAM( "OP.parser.execf                : (f_execute_operator) %llp", self->OP.parser.execf );
      CXLIB_OSTREAM( "OP.parser.retiref              : (f_retire_operator) %llp", self->OP.parser.retiref );
      CXLIB_OSTREAM( "OP.parser.__locked_graph       : (vgx_Graph_t*) %llp", self->OP.parser.__locked_graph );
      CXLIB_OSTREAM( "-- 2 -- (36)" );
      CXLIB_OSTREAM( "OP.parser.OPERATOR" );
      CXLIB_OSTREAM( "  .op.code                     : %08x", self->OP.parser.OPERATOR.op.code );
      CXLIB_OSTREAM( "  .op.name                     : %s", self->OP.parser.OPERATOR.op.name );
      for( int i=0; i<7; i++ ) {
        CXLIB_OSTREAM( "  ._data[%d]                   : %016llx", i, self->OP.parser.OPERATOR._data[ i ] );
      }
      CXLIB_OSTREAM( "-- 3 -- (37)" );
      CXLIB_OSTREAM( "OP.parser.token" );
      CXLIB_OSTREAM( "  .data                        : %s", self->OP.parser.token.data );
      CXLIB_OSTREAM( "  .len                         : %d", self->OP.parser.token.len );
      CXLIB_OSTREAM( "  .flags" );
      CXLIB_OSTREAM( "    .suspend                   : %u", (unsigned)self->OP.parser.token.flags.suspend );
      CXLIB_OSTREAM( "    .pending                   : %u", (unsigned)self->OP.parser.token.flags.pending );
      CXLIB_OSTREAM( "    ._rsv3                     : %u", (unsigned)self->OP.parser.token.flags._rsv3 );
      CXLIB_OSTREAM( "    ._rsv4                     : %u", (unsigned)self->OP.parser.token.flags._rsv4 );
      CXLIB_OSTREAM( "OP.parser.field                : %d", self->OP.parser.field );
      CXLIB_OSTREAM( "OP.parser.op_mnemonic          : %s", self->OP.parser.op_mnemonic );
      CXLIB_OSTREAM( "OP.parser.CSTR__error          : (CString_t*) %llp (\"%s\")", self->OP.parser.CSTR__error, CStringValueDefault( self->OP.parser.CSTR__error, "" ) );
      CXLIB_OSTREAM( "OP.parser.reason               : %08x", self->OP.parser.reason );
      CXLIB_OSTREAM( "OP.parser.crc                  : %08x", self->OP.parser.crc );
      CXLIB_OSTREAM( "OP.parser.TASK                 : (comlib_task_t*) %llp", self->OP.parser.TASK );
      CXLIB_OSTREAM( "OP.parser.control" );
      CXLIB_OSTREAM( "  .n_locks                     : %d", self->OP.parser.control.n_locks );
      CXLIB_OSTREAM( "  .exe                         : %d", (int)self->OP.parser.control.exe );
      CXLIB_OSTREAM( "  .snchk                       : %d", (int)self->OP.parser.control.snchk );
      CXLIB_OSTREAM( "  .crc                         : %d", (int)self->OP.parser.control.crc );
      CXLIB_OSTREAM( "  .ena_validate                : %u", (unsigned)self->OP.parser.control.ena_validate );
      CXLIB_OSTREAM( "  .ena_execute                 : %u", (unsigned)self->OP.parser.control.ena_execute );
      CXLIB_OSTREAM( "  .__rsv3                      : %u", (unsigned)self->OP.parser.control.__rsv3 );
      CXLIB_OSTREAM( "  .mute_regression             : %u", (unsigned)self->OP.parser.control.mute_regression );
      CXLIB_OSTREAM( "  .strict_serial               : %u", (unsigned)self->OP.parser.control.strict_serial );
      CXLIB_OSTREAM( "  .ena_crc                     : %u", (unsigned)self->OP.parser.control.ena_crc );
      CXLIB_OSTREAM( "  .__rsv7                      : %u", (unsigned)self->OP.parser.control.__rsv7 );
      CXLIB_OSTREAM( "  .trg_reset                   : %u", (unsigned)self->OP.parser.control.trg_reset );
      CXLIB_OSTREAM( "OP.parser.parent               : (vgx_Graph_t*) %llp", self->OP.parser.parent );
      CXLIB_OSTREAM( "-- 4 -- (38)" );
      CXLIB_OSTREAM( "OP.parser.input                : (CByteList_t*) %llp [%lld items]", self->OP.parser.input, GetSequenceLength(self->OP.parser.input) );
      CXLIB_OSTREAM( "OP.parser.private_input        : (CByteList_t*) %llp [%lld items]", self->OP.parser.private_input, GetSequenceLength(self->OP.parser.private_input) );
      CXLIB_OSTREAM( "OP.parser.n_pending_bytes      : %lld", self->OP.parser.n_pending_bytes );
      CXLIB_OSTREAM( "OP.parser.property_allocator_ref:(object_allocator_context_t*) %llp", self->OP.parser.property_allocator_ref );
      CXLIB_OSTREAM( "OP.parser.string_allocator     : (object_allocator_context_t*) %llp", self->OP.parser.string_allocator );
      CXLIB_OSTREAM( "OP.parser.sn                   : %lld", self->OP.parser.sn );
      CXLIB_OSTREAM( "OP.parser.transid              : %016llx%016llx", self->OP.parser.transid.H, self->OP.parser.transid.L );


      CXLIB_OSTREAM( "39: ------- MISC EVENT ---" );
      Q = (QWORD*)&self->wake_event;
      CXLIB_OSTREAM( "wake_event                     : (CS_COND) %016llX %016llX %016llX %016llX %016llX %016llX", Q[0], Q[1], Q[2], Q[3], Q[4], Q[5] );
      CXLIB_OSTREAM( "__rsv_39_7                     : %llu", self->__rsv_39_7);
      CXLIB_OSTREAM( "__rsv_39_8                     : %llu", self->__rsv_39_8 );


      CXLIB_OSTREAM( "40: ------- SERVER / HOOKS ------" );
      CXLIB_OSTREAM( "vgxserverA                     : (vgx_VGXServer_t*) %llp", self->vgxserverA );
      CXLIB_OSTREAM( "vgxserverB                     : (vgx_VGXServer_t*) %llp", self->vgxserverB );
      CXLIB_OSTREAM( "__rsv_40_3                     : %llu", self->__rsv_40_3 );
      CXLIB_OSTREAM( "__rsv_40_4                     : %llu", self->__rsv_40_4 );
      CXLIB_OSTREAM( "sysaux_cmd_callback            : int (*)(vgx_Graph_t*, OperationProcessorAuxCommand, vgx_StringList_t*, CString_T**) [%llp]", self->sysaux_cmd_callback );
      CXLIB_OSTREAM( "system_sync_callback           : int (*)(vgx_Graph_t*, CString_t**) [%llp]", self->system_sync_callback );
      CXLIB_OSTREAM( "external_owner_object          : (void*) %llp", self->external_owner_object );
      CXLIB_OSTREAM( "destructor_callback_hook       : void (*)(vgx_Graph_t*, void*) [%llp]", self->destructor_callback_hook );


    } END_CXLIB_OBJ_DUMP;

  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Graph_system_aux_command_noop( vgx_Graph_t *graph, OperationProcessorAuxCommand cmd, vgx_StringList_t *data, CString_t **CSTR__error ) {
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int Graph_system_sync_noop( vgx_Graph_t *graph, CString_t **CSTR__error ) {
  return 0;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_object.h"

test_descriptor_t _vgx_vxgraph_object_tests[] = {
  { "VGX Graph Object Tests", __utest_vxgraph_object },
  {NULL}
};
#endif
