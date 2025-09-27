/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




static int _vxdurable_operation__initialize( vgx_Graph_t *graph, int64_t init_opid );
static int _vxdurable_operation__destroy( vgx_Graph_t *graph );



static int _vxdurable_operation__is_open( vgx_Operation_t *operation );
static int _vxdurable_operation__open_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, const comlib_object_t *obj, bool hold_CS );
static int _vxdurable_operation__graph_open_CS( vgx_Graph_t *graph );


static int _vxdurable_operation__suspend( int timeout_ms );
static int _vxdurable_operation__resume( void );
static int64_t _vxdurable_operation__writable_vertices( void );
static int _vxdurable_operation__fence( int timeout_ms );
static int _vxdurable_operation__assert_state( void );

static int _vxdurable_operation__sync_CS( vgx_Graph_t *graph, bool hard, int timeout_ms, CString_t **CSTR__error );
static int _vxdurable_operation__sync( vgx_Graph_t *graph, bool hard, int timeout_ms, CString_t **CSTR__error );

static int _vxdurable_operation__enter_readonly_CS( vgx_Graph_t *graph );
static int _vxdurable_operation__leave_readonly_CS( vgx_Graph_t *graph );
static int _vxdurable_operation__check_readonly_CS( vgx_Graph_t *graph );

static int _vxdurable_operation__graph_set_modified_CS( vgx_Graph_t *graph );

static int64_t _vxdurable_operation__get_id_LCK( const vgx_Operation_t *operation_LCK );
static int64_t _vxdurable_operation__set_id( vgx_Operation_t *operation, int64_t opid );
static int64_t _vxdurable_operation__init_id( vgx_Operation_t *operation, int64_t opid );

static void _vxdurable_operation__dump_CS( vgx_Operation_t *operation );




__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "TX::OPR(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXDURABLE_OPERATION_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_OPERATION_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_OPERATION_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_OPERATION_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_OPERATION_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXDURABLE_OPERATION_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )



#define SYSTEM_ENTER_READONLY_CS( ProcessorSystem )        (++(ProcessorSystem)->state_CS.readonly)
#define SYSTEM_LEAVE_READONLY_CS( ProcessorSystem )        (--(ProcessorSystem)->state_CS.readonly)
#define SYSTEM_READONLY_RECURSION_CS( ProcessorSystem )    ((ProcessorSystem)->state_CS.readonly)
#define SYSTEM_CHECK_READONLY_CS( ProcessorSystem )        (SYSTEM_READONLY_RECURSION_CS( ProcessorSystem ) > 0 )
#define SYSTEM_READONLY_SET_MIN_CS( ProcessorSystem )      ((ProcessorSystem)->state_CS.readonly = 0)
#define SYSTEM_READONLY_SET_MAX_CS( ProcessorSystem )      ((ProcessorSystem)->state_CS.readonly = SHRT_MAX)




/*******************************************************************//**
 * 
 * 
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IOperation_t iOperation = {

  .Initialize           = _vxdurable_operation__initialize,
  .Destroy              = _vxdurable_operation__destroy,

  .Emitter_CS = {
    .Initialize         = _vxdurable_operation_emitter__initialize_CS,
    .IsInitialized      = _vxdurable_operation_emitter__is_initialized_CS,
    .Start              = _vxdurable_operation_emitter__start_CS,
    .Stop               = _vxdurable_operation_emitter__stop_CS,
    .IsRunning          = _vxdurable_operation_emitter__is_running_CS,
    .Suspend            = _vxdurable_operation_emitter__suspend_CS,
    .IsSuspended        = _vxdurable_operation_emitter__is_suspended_CS,
    .Resume             = _vxdurable_operation_emitter__resume_CS,
    .IsReady            = _vxdurable_operation_emitter__is_ready_CS,
    .HasPending         = _vxdurable_operation_emitter__has_pending_CS,
    .GetPending         = _vxdurable_operation_emitter__get_pending_CS,
    .Fence              = _vxdurable_operation_emitter__fence_CS,
    .Enable             = _vxdurable_operation_emitter__enable_CS,
    .Disable            = _vxdurable_operation_emitter__disable_CS,
    .IsEnabled          = _vxdurable_operation_emitter__is_enabled_CS,
    .HeartbeatEnable    = _vxdurable_operation_emitter__heartbeat_enable_CS,
    .HeartbeatDisable   = _vxdurable_operation_emitter__heartbeat_disable_CS
  },

  .Parser = {
    .Initialize         = _vxdurable_operation_parser__initialize_OPEN,
    .Destroy            = _vxdurable_operation_parser__destroy_OPEN,
    .SubmitData         = _vxdurable_operation_parser__submit_data,
    .Feed               = _vxdurable_operation_parser__feed_operation_data_OPEN,
    .Reset              = _vxdurable_operation_parser__reset_OPEN,
    .EnableValidation   = _vxdurable_operation_parser__enable_validation,
    .EnableExecution    = _vxdurable_operation_parser__enable_execution,
    .SkipRegression     = _vxdurable_operation_parser__silent_skip_regression,
    .EnableCRC          = _vxdurable_operation_parser__enable_crc,
    .EnableStrictSerial = _vxdurable_operation_parser__enable_strict_serial,
    .Pending            = _vxdurable_operation_parser__get_pending,
    .Suspend            = _vxdurable_operation_parser__suspend,
    .IsSuspended        = _vxdurable_operation_parser__is_suspended,
    .Resume             = _vxdurable_operation_parser__resume,
    .AddFilter          = _vxdurable_operation_parser__add_opcode_filter,
    .RemoveFilter       = _vxdurable_operation_parser__remove_opcode_filter,
    .ApplyProfile       = _vxdurable_operation_parser__apply_opcode_profile,
    .Checksum           = _vxdurable_operation_parser__checksum
  },

  .IsOpen               = _vxdurable_operation__is_open,

  .Open_CS              = _vxdurable_operation__open_CS,
  .Commit_CS            = _vxdurable_operation_capture__commit_CS,
  .Close_CS             = _vxdurable_operation_capture__close_CS,

  .GraphOpen_CS         = _vxdurable_operation__graph_open_CS,
  .GraphCommit_CS       = _vxdurable_operation_capture__graph_commit_CS,
  .GraphClose_CS        = _vxdurable_operation_capture__graph_close_CS,

  .Suspend              = _vxdurable_operation__suspend,
  .Resume               = _vxdurable_operation__resume,
  .WritableVertices     = _vxdurable_operation__writable_vertices,
  .Fence                = _vxdurable_operation__fence,
  .AssertState          = _vxdurable_operation__assert_state,
 
  .Counters = {
    .Outstream          = _vxdurable_operation_system__counters_outstream_OPEN,
    .Instream           = _vxdurable_operation_system__counters_instream_OPEN,
    .Reset              = _vxdurable_operation_system__reset_counters_OPEN,
    .OutputBacklog      = _vxdurable_operation_system__get_output_backlog_OPEN,
  },

  .Readonly_CS = {
    .Enter              = _vxdurable_operation__enter_readonly_CS,
    .Leave              = _vxdurable_operation__leave_readonly_CS,
    .Check              = _vxdurable_operation__check_readonly_CS
  },

  .IsDirty              = _vxdurable_operation__is_dirty,
  .SetDirty             = _vxdurable_operation__set_dirty,

  .GetId_LCK            = _vxdurable_operation__get_id_LCK,
  .SetId                = _vxdurable_operation__set_id,
  .InitId               = _vxdurable_operation__init_id,

  .Dump_CS              = _vxdurable_operation__dump_CS,

  .Vertex_CS = {
    .New                = _vxdurable_operation_capture__vertex_new_CS,
    .Delete             = _vxdurable_operation_capture__vertex_delete_CS
  },

  .Vertex_WL = {
    .SetRank            = _vxdurable_operation_capture__vertex_set_rank_WL,
    .ChangeType         = _vxdurable_operation_capture__vertex_change_type_WL,
    .SetTMX             = _vxdurable_operation_capture__vertex_set_tmx_WL,
    .Convert            = _vxdurable_operation_capture__vertex_convert_WL,
    .SetProperty        = _vxdurable_operation_capture__vertex_set_property_WL,
    .DelProperty        = _vxdurable_operation_capture__vertex_del_property_WL,
    .DelProperties      = _vxdurable_operation_capture__vertex_del_properties_WL,
    .SetVector          = _vxdurable_operation_capture__vertex_set_vector_WL,
    .DelVector          = _vxdurable_operation_capture__vertex_del_vector_WL,
    .RemoveOutarcs      = _vxdurable_operation_capture__vertex_remove_outarcs_WL,
    .RemoveInarcs       = _vxdurable_operation_capture__vertex_remove_inarcs_WL
  },

  .Arc_WL = {
    .Connect            = _vxdurable_operation_capture__arc_connect_WL,
    .Disconnect         = _vxdurable_operation_capture__arc_disconnect_WL
  },

  .System_SYS_CS = {
    .StartAgent             = _vxdurable_operation_system__start_agent_SYS_CS,
    .SuspendAgent           = _vxdurable_operation_system__suspend_agent_SYS_CS,
    .IsAgentSuspended       = _vxdurable_operation_system__is_agent_suspended_SYS_CS,
    .ResumeAgent            = _vxdurable_operation_system__resume_agent_SYS_CS,
    .StopAgent              = _vxdurable_operation_system__stop_agent_SYS_CS,
    .Attach                 = _vxdurable_operation_capture__system_attach_SYS_CS,
    .Detach                 = _vxdurable_operation_capture__system_detach_SYS_CS,
    .ClearRegistry          = _vxdurable_operation_capture__system_clear_registry_SYS_CS,
    .CreateGraph            = _vxdurable_operation_capture__system_create_graph_SYS_CS,
    .DeleteGraph            = _vxdurable_operation_capture__system_delete_graph_SYS_CS,
    .Tick                   = _vxdurable_operation_capture__system_tick_SYS_CS,
    .SendComment            = _vxdurable_operation_capture__system_send_comment_SYS_CS,
    .SendSimpleAuxCommand   = _vxdurable_operation_capture__system_send_simple_aux_command_SYS_CS,
    .ForwardAuxCommand      = _vxdurable_operation_capture__system_forward_aux_command_SYS_CS,
    .SendRawData            = _vxdurable_operation_capture__system_send_raw_data_SYS_CS,
    .CloneGraph             = _vxdurable_operation_capture__system_clone_graph_SYS_CS
  },

  .System_OPEN = {
    .ConsumerService = {
      .Start                = _vxdurable_operation_system__start_consumer_service_OPEN,
      .BoundPort            = _vxdurable_operation_system__bound_port_consumer_service_OPEN,
      .IsDurable            = _vxdurable_operation_consumer_service__is_durable_OPEN,
      .Stop                 = _vxdurable_operation_system__stop_consumer_service_OPEN,
      .Subscribe            = _vxdurable_operation_consumer_service__subscribe_OPEN,
      .Unsubscribe          = _vxdurable_operation_consumer_service__unsubscribe_OPEN,
      .SuspendExecution     = _vxdurable_operation_consumer_service__suspend_tx_execution_OPEN,
      .IsExecutionSuspended = _vxdurable_operation_consumer_service__is_suspended_tx_execution_OPEN,
      .ResumeExecution      = _vxdurable_operation_consumer_service__resume_tx_execution_OPEN,
      .IsInitializing       = _vxdurable_operation_consumer_service__is_initializing_tx_execution_OPEN,
      .Suspend              = _vxdurable_operation_consumer_service__suspend_OPEN,
      .IsSuspended          = _vxdurable_operation_consumer_service__is_suspended_OPEN,
      .Resume               = _vxdurable_operation_consumer_service__resume_OPEN
    },
    .TransPending           = _vxdurable_operation_system__pending_transactions_OPEN,
    .BytesPending           = _vxdurable_operation_system__pending_bytes_OPEN
  },

  .Graph_CS = {
    .SetModified        = _vxdurable_operation__graph_set_modified_CS,
    .Truncate           = _vxdurable_operation_capture__graph_truncate_CS,
    .Persist            = _vxdurable_operation_capture__graph_persist_CS,
    .State              = _vxdurable_operation_capture__graph_state_CS,
    .AssertState        = _vxdurable_operation_capture__graph_assert_state_CS
  },

  .Graph_OPEN = {
    .Sync               = _vxdurable_operation__sync
  },

  .Graph_ROG = {
    .GetBaseCounts      = _vxdurable_operation_capture__graph_get_base_counts_ROG
  },

  .State_CS = {
    .Readonly           = _vxdurable_operation_capture__graph_readonly_CS,
    .Readwrite          = _vxdurable_operation_capture__graph_readwrite_CS,
    .Events             = _vxdurable_operation_capture__graph_events_CS,
    .NoEvents           = _vxdurable_operation_capture__graph_noevents_CS
  },

  .Lock_CS = {
    .AcquireWL          = _vxdurable_operation_capture__vertices_acquire_CS_WL,
  },

  .Unlock_CS = {
    .ReleaseLCK         = _vxdurable_operation_capture__vertices_release_CS_LCK,
    .All                = _vxdurable_operation_capture__vertices_release_all_CS
  },

  .Execute_CS = {
    .Events             = _vxdurable_operation_capture__graph_event_exec_CS
  },

  .Enumerator_CS = {
    .AddVertexType      = _vxdurable_operation_capture__enumerator_add_vertextype_CS,
    .RemoveVertexType   = _vxdurable_operation_capture__enumerator_remove_vertextype_CS,
    .AddRelationship    = _vxdurable_operation_capture__enumerator_add_relationship_CS,
    .RemoveRelationship = _vxdurable_operation_capture__enumerator_remove_relationship_CS,
    .AddDimension       = _vxdurable_operation_capture__enumerator_add_dimension_CS,
    .RemoveDimension    = _vxdurable_operation_capture__enumerator_remove_dimension_CS,
    .AddPropertyKey     = _vxdurable_operation_capture__enumerator_add_propertykey_CS,
    .RemovePropertyKey  = _vxdurable_operation_capture__enumerator_remove_propertykey_CS,
    .AddStringValue     = _vxdurable_operation_capture__enumerator_add_stringvalue_CS,
    .RemoveStringValue  = _vxdurable_operation_capture__enumerator_remove_stringvalue_CS
  }


};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Operation_t * __OPERATION_INIT( vgx_Operation_t *operation, int64_t opid ) {
  OPERATION_ZERO_ALL( operation );
  OPERATION_OPID_SET_INTEGER( operation, opid );
  OPERATION_SET_CLEAN( operation );
  OPERATION_SET_LOCAL_ONLY( operation );
  return operation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Operation_t * __OPERATION_OPEN( vgx_Operation_t *operation, vgx_OperatorCapture_t *capture ) {
  if( OPERATION_CAPTURE_HAS_OBJECT( operation ) ) {
    // Previous capture object would be overwritten
    VXDURABLE_OPERATION_FATAL( capture ? capture->graph : NULL, 0x999, "Active capture object leak!" );
    return NULL;
  }
  OPERATION_CAPTURE_SET_OBJECT( operation, capture );
  OPERATION_SET_CLEAN( operation );
  return operation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Operation_t * __OPERATION_CLOSE( vgx_Operation_t *operation, int64_t opid ) {
  if( OPERATION_CAPTURE_HAS_OBJECT( operation ) ) {
    // Previous capture object would be overwritten
    VXDURABLE_OPERATION_FATAL( NULL, 0x999, "Active capture object leak!" );
    return NULL;
  }
  OPERATION_OPID_SET_INTEGER( operation, opid );
  OPERATION_SET_CLEAN( operation );
  return operation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_OperatorCapture_t * __OPERATION_GET_CAPTURE( vgx_Operation_t *operation ) {
  if( OPERATION_IS_OPEN( operation ) && OPERATION_IS_EMITTABLE( operation ) ) {
    vgx_OperatorCapture_t *capture = OPERATION_CAPTURE_GET_OBJECT( operation );
    if( capture ) {
      return capture;
    }
    else {
      VXDURABLE_OPERATION_FATAL( NULL, 0x999, "Capture object NULL!" );
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_OperatorCapture_t * __OPERATION_POP_CAPTURE( vgx_Operation_t *operation ) {
  vgx_OperatorCapture_t *capture = __OPERATION_GET_CAPTURE( operation );
  OPERATION_CAPTURE_SET_OBJECT( operation, NULL );
  return capture;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t __OPERATION_GET_OPID( const vgx_Operation_t *operation_LCK ) {
  if( OPERATION_IS_EMITTABLE( operation_LCK ) && OPERATION_IS_OPEN( operation_LCK ) ) {
    vgx_OperatorCapture_t *capture = OPERATION_CAPTURE_GET_OBJECT( operation_LCK );
    if( capture ) {
      return capture->opid;
    }
    else {
      VXDURABLE_OPERATION_FATAL( NULL, 0x999, "Capture object NULL!" );
      return -1;
    }
  }
  else {
    return OPERATION_OPID_GET_INTEGER( operation_LCK );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t __OPERATION_SET_OPID( vgx_Operation_t *operation, int64_t opid ) {
  if( OPERATION_IS_EMITTABLE( operation ) && OPERATION_IS_OPEN( operation ) ) {
    vgx_OperatorCapture_t *capture = OPERATION_CAPTURE_GET_OBJECT( operation );
    if( capture ) {
      capture->opid = opid;
    }
    else {
      VXDURABLE_OPERATION_FATAL( NULL, 0x999, "Capture object NULL!" );
      return -1;
    }
  }
  else {
    OPERATION_OPID_SET_INTEGER( operation, opid );
  }
  return opid;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation__is_dirty( const vgx_Operation_t *operation ) {
  return OPERATION_IS_DIRTY( operation );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation__set_dirty( vgx_Operation_t *operation ) {
  return (int)OPERATION_SET_DIRTY( operation );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static int64_t _vxdurable_operation__get_id_LCK( const vgx_Operation_t *operation_LCK ) {
  return OPERATION_GET_OPID( operation_LCK );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static int64_t _vxdurable_operation__set_id( vgx_Operation_t *operation, int64_t opid ) {
  OPERATION_SET_OPID( operation, opid );
  OPERATION_SET_DIRTY( operation );
  return opid;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static int64_t _vxdurable_operation__init_id( vgx_Operation_t *operation, int64_t opid ) {
  OPERATION_INIT( operation, opid );
  return OPERATION_GET_OPID( operation );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static void _vxdurable_operation__dump_CS( vgx_Operation_t *operation ) {
  int closed = OPERATION_IS_CLOSED( operation );
  int dirty = OPERATION_IS_DIRTY( operation );
  int emittable = OPERATION_IS_EMITTABLE( operation );
  int64_t opid = OPERATION_GET_OPID( operation );
  
  printf( "------------------------\n" );
  printf( "OPERATION:\n" );
  printf( "  state      : %s\n", closed ? "closed" : "open" );
  printf( "  dirty      : %d\n", dirty );
  printf( "  emittable  : %d\n", emittable );
  printf( "  opid       : %lld\n", opid );
  printf( "------------------------\n" );
  if( !closed && emittable ) {
    vgx_OperatorCapture_t *capture = OPERATION_GET_CAPTURE( operation );
    _vxdurable_operation_capture__dump_capture_object_CS( capture );
  }

}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation__trap_error( vgx_Graph_t *graph, bool fatal, int errcode, const char *msg ) {
  if( fatal ) {
    const char *m = msg ? msg : "unknown";
    VXDURABLE_OPERATION_FATAL( graph, errcode, "Unrecoverable error: %s", m );
  }
  if( msg ) {
    if( errcode > 0 ) {
      VXDURABLE_OPERATION_CRITICAL( graph, errcode, "%s", msg );
    }
    else {
      VXDURABLE_OPERATION_REASON( graph, errcode, "%s", msg );
    }
  }
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_capture__dump_capture_object_CS( vgx_OperatorCapture_t *capture ) {

  vgx_Graph_t *graph = capture->graph;
  if( graph ) {
    CQwordBuffer_t *Q = capture->opdatabuf;
    typedef union __u_dump {
      QWORD          qword;
      unsigned int   dword[2];
      unsigned short word[4];
      char           byte[8];
    } __dump;

    __dump dump;


    vgx_OperationProcessor_t *processor = &graph->OP;

    int n = 0;
    printf( "\n" );
    printf( "CAPTURE OBJECT @ %p:\n", capture );
    printf( "  opid       = %lld\n", capture->opid );
    printf( "  graph      = %s\n", capture->graph ? CALLABLE( capture->graph )->FullPath( capture->graph ) : "NULL" );
    printf( "  processor  = %p\n", processor );
    printf( "  emitter    = %p\n", &processor->emitter );
    printf( "  objectid   = %016llx%016llx\n", capture->inheritable.objectid.H, capture->inheritable.objectid.L );
    printf( "  tp_class   = %d\n", (int)capture->inheritable.object_typeinfo.tp_class );
    printf( "  xrecursion = %d\n", capture->inheritable.xrecursion );
    printf( "  opdata     = \n" );
    printf( "      _size  = %lld\n", capture->opdatabuf->_size );
    printf( "    _buffer  = %p\n", capture->opdatabuf->_buffer );
    printf( "        _wp  = %p\n", capture->opdatabuf->_wp );
    printf( "        _rp  = %p\n", capture->opdatabuf->_rp );
    printf( "  --- ----------------\n" );
    printf( "   n       opdata     \n" );
    printf( "  --- ----------------\n" );
    while( CALLABLE( Q )->Next( Q, &dump.qword ) == 1 ) {
      printf( "  %3d %016llX Q[%20llu] D[%10u %10u] W[%5u %5u %5u %5u] ", n++, dump.qword, dump.qword, dump.dword[0], dump.dword[1], dump.word[0], dump.word[1], dump.word[2], dump.word[3] );
      for( int i=0; i<8; i++ ) {
        char c = dump.byte[i];
        printf( "%c", c < 32 || c > 127 ? '.' : c );
      }
      printf( "\n" );
    }

    _vxdurable_operation_emitter__dump_queues_CS( graph, capture );

  }
}



/*******************************************************************//**
 *
 * Returns: 1: Operation opened
 *          0: Operation was already open, no action
 *         -1: Error
 ***********************************************************************
 */
static int _vxdurable_operation__open_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, const comlib_object_t *obj, bool hold_CS ) {
  // Emitter enabled, prepare operation with new capture object
  if( OPERATION_CAPTURE_ENABLED_CS( graph ) ) {
    // Operation closed
    if( OPERATION_IS_CLOSED( operation ) ) {
      ALIGNED_VAR( vgx_OperationCaptureInheritable_t, data, 32 ) = {0};
      idcpy( &data.objectid, COMLIB_OBJECT_GETID( obj ) );
      data.object_typeinfo = COMLIB_OBJECT_TYPEINFO( obj );
      vgx_Operation_t *new_operation = _vxdurable_operation_emitter__next_operation_CS( graph, operation, &data, hold_CS );
      if( new_operation ) {
        OPERATION_SET_EMITTABLE( new_operation );
        return 1;
      }
      else {
        return _vxdurable_operation__trap_error( graph, false, 0x001, "Operation could not be opened" );
      }
    }
    // Already open!
    else {
      return 0;
    }
  }
  // No emitter
  else {
    OPERATION_SET_LOCAL_ONLY( operation );
    return 1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxdurable_operation__graph_open_CS( vgx_Graph_t *graph ) {
  int ret = _vxdurable_operation__open_CS( graph, &graph->operation, COMLIB_OBJECT( graph ), true );
  return ret;
}




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxdurable_operation__is_open( vgx_Operation_t *operation ) {
  return OPERATION_IS_OPEN( operation );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__enter_readonly_CS( vgx_Graph_t *graph ) {
  vgx_OperationSystem_t *system = &graph->OP.system;
  if( SYSTEM_ENTER_READONLY_CS( system ) < 0 ) {
    SYSTEM_READONLY_SET_MAX_CS( system );
    VXDURABLE_OPERATION_CRITICAL( graph, 0x001, "Readonly state max recursion" );
  }
  return SYSTEM_READONLY_RECURSION_CS( system );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__leave_readonly_CS( vgx_Graph_t *graph ) {
  vgx_OperationSystem_t *system = &graph->OP.system;
  if( SYSTEM_LEAVE_READONLY_CS( system ) < 0 ) {
    SYSTEM_READONLY_SET_MIN_CS( system );
    VXDURABLE_OPERATION_CRITICAL( graph, 0x001, "Negative readonly state" );
  }
  return SYSTEM_READONLY_RECURSION_CS( system );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__check_readonly_CS( vgx_Graph_t *graph ) {
  return SYSTEM_CHECK_READONLY_CS( &graph->OP.system );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__graph_set_modified_CS( vgx_Graph_t *graph ) {
  vgx_Operation_t *operation = &graph->operation;
  return _vxdurable_operation__set_dirty( operation );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__initialize( vgx_Graph_t *graph, int64_t init_opid ) {
  int ret = 0;

  GRAPH_LOCK( graph ) {
    vgx_OperationProcessor_t *processor = &graph->OP;

    XTRY {
      // Reset data
      memset( processor, 0, sizeof( vgx_OperationProcessor_t ) );

      // ===============================
      // SYSTEM (init only)
      // ===============================
      if( _vxdurable_operation_system__initialize_CS( graph, &processor->system ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
      }

      // ===============================
      // EMITTER (init only)
      // ===============================
      if( iOperation.Emitter_CS.Initialize( graph, init_opid ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
      }

      // ===============================
      // 
      //
      // ===============================

      // Initialize graph operation id to emitter's ID, force clean
      OPERATION_SET_LOCAL_ONLY( &graph->operation );
      _vxdurable_operation__set_id( &graph->operation, init_opid );
      // force clean after set id makes it dirty
      OPERATION_SET_CLEAN( &graph->operation );

      VXDURABLE_OPERATION_INFO( graph, 0x004, "Ready" );

    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__destroy( vgx_Graph_t *self ) {
  int ret = 0;


  GRAPH_LOCK( self ) {

    // ===============================
    // EMITTER (stop thread)
    // ===============================
    _vxdurable_operation_emitter__destroy_CS( self );
    
    if( self->OP.system.progress_CS ) {
      free( self->OP.system.progress_CS );
      self->OP.system.progress_CS = NULL;
    }

    if( self->OP.system.in_feed_limits_CS ) {
      free( self->OP.system.in_feed_limits_CS );
      self->OP.system.in_feed_limits_CS = NULL;
    }


    // TODO: Anything else ?
    //

  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_get_base_counts_ROG( vgx_Graph_t *graph, vgx_graph_base_counts_t *counts ) {

  GRAPH_LOCK( graph ) {

    // order
    counts->order   = GraphOrder( graph );

    // size
    counts->size    = GraphSize( graph );

    // nkey
    counts->nkey    = iEnumerator_CS.Property.Key.Count( graph );

    // nstrval
    counts->nstrval = iEnumerator_CS.Property.Value.Count( graph );

    // nprop
    counts->nprop   = GraphPropCount( graph );

    // nvec
    counts->nvec    = GraphVectorCount( graph );

    // ndim
    counts->ndim    = (graph->similarity && !igraphfactory.EuclideanVectors()) ? (int)iEnumerator_CS.Dimension.Count( graph->similarity ) : 0;

    // nrel
    counts->nrel    = (short)iEnumerator_CS.Relationship.Count( graph );

    // ntype
    counts->ntype   = (BYTE)iEnumerator_CS.VertexType.Count( graph );

  } GRAPH_RELEASE;

  return 0;

}



/*******************************************************************//**
 *
 * Suspend all graph emitters and the system output agent
 *
 * Returns:   1 : success
 *           -1 : error
 *
 ***********************************************************************
 */
static int _vxdurable_operation__suspend( int timeout_ms ) {
  int suspended = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  // TODO: Specified timeout applies to all components of the suspend.
  //       It should really be a budget that is consumed by each component.

  GRAPH_LOCK( SYSTEM ) {
    XTRY {

      // Suspend emitters for all graphs
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        GRAPH_FACTORY_ACQUIRE {
          vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
          if( graphs ) {
            vgx_Graph_t **cursor = graphs;
            vgx_Graph_t *graph;
            while( (graph = *cursor++) != NULL && suspended == 0 ) {
              GRAPH_LOCK( graph ) {
                iOperation.Graph_CS.SetModified( graph );
                if( iOperation.Emitter_CS.IsReady( graph ) ) {
                  iOperation.Emitter_CS.Fence( graph, 0, timeout_ms );
                  if( iOperation.Emitter_CS.Suspend( graph, timeout_ms ) != 1 ) {
                    // Give up on first failure
                    suspended = -1;
                  }
                }
              } GRAPH_RELEASE;
            }
            free( (void*)graphs );
          }
        } GRAPH_FACTORY_RELEASE;
      } GRAPH_RESUME_LOCK;

      if( suspended < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Suspend emitter for system graph
      iOperation.Graph_CS.SetModified( SYSTEM );
      if( iOperation.Emitter_CS.IsReady( SYSTEM ) ) {
        iOperation.Emitter_CS.Fence( SYSTEM, 0, timeout_ms );
        if( iOperation.Emitter_CS.Suspend( SYSTEM, timeout_ms ) != 1 ) {
          // Give up on failure
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
      }

      // Suspend output agent
      if( (suspended = iOperation.System_SYS_CS.SuspendAgent( SYSTEM, timeout_ms )) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

    }
    XCATCH( errcode ) {

      // Best effort to resume if we failed to suspend everything
      if( iOperation.Resume() != 1 ) {
        VXDURABLE_OPERATION_CRITICAL( SYSTEM, 0x004, "Failed to resume suspended emitters" );
      }

      suspended = -1;

    }
    XFINALLY {
    }

  } GRAPH_RELEASE;

  return suspended;
}



/*******************************************************************//**
 *
 * Resume system output agent and all graph emitters
 *
 * Returns:  1 : success
 *          -1 : failed to resume
 *
 ***********************************************************************
 */
static int _vxdurable_operation__resume( void ) {
  int resumed = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }


  GRAPH_LOCK( SYSTEM ) {
    XTRY {
      // Resume output agent
      if( iOperation.System_SYS_CS.ResumeAgent( SYSTEM, 15000 ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Resume emitter for system graph
      if( iOperation.Emitter_CS.IsSuspended( SYSTEM ) ) {
        if( iOperation.Emitter_CS.Resume( SYSTEM ) != 1 ) {
          // Give up on failure
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
      }

      // Resume emitters for all graphs
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        GRAPH_FACTORY_ACQUIRE {
          vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
          if( graphs ) {
            vgx_Graph_t **cursor = graphs;
            vgx_Graph_t *graph;
            while( (graph = *cursor++) != NULL && resumed == 0 ) {
              GRAPH_LOCK( graph ) {
                if( iOperation.Emitter_CS.IsSuspended( graph ) ) {
                  if( iOperation.Emitter_CS.Resume( graph ) != 1 ) {
                    // Give up on first failure
                    resumed = -1;
                  }
                }
              } GRAPH_RELEASE;
            }
            free( (void*)graphs );
          }
        } GRAPH_FACTORY_RELEASE;
      } GRAPH_RESUME_LOCK;

      if( resumed < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      // OK
      resumed = 1;
    }
    XCATCH( errcode ) {
      resumed = -1;
    }
    XFINALLY {
    }

  } GRAPH_RELEASE;

  return resumed;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxdurable_operation__writable_vertices( void ) {
  int64_t n_writable = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  int64_t n;

  GRAPH_LOCK( SYSTEM ) {
    
    if( (n = _vgx_graph_get_vertex_WL_count_CS( SYSTEM )) > 0 ) {
      n_writable += n;
    }

    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      GRAPH_FACTORY_ACQUIRE {
        vgx_Graph_t **graphs;
        if( (graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL )) != NULL ) {
          vgx_Graph_t **cursor = graphs;
          vgx_Graph_t *graph;
          while( (graph = *cursor++) != NULL ) {
            GRAPH_LOCK( graph ) {
              if( (n = _vgx_graph_get_vertex_WL_count_CS( graph )) > 0 ) {
                n_writable += n;
              }
            } GRAPH_RELEASE;
          }
          free( (void*)graphs );
        }
      } GRAPH_FACTORY_RELEASE;
    } GRAPH_RESUME_LOCK;
  } GRAPH_RELEASE;

  return n_writable;
}



/*******************************************************************//**
 * Flush all operation pipelines
 *
 *
 * Returns:  1 : Success
 *          -1 : Failed to suspend (flush not confirmed) or failed to 
 *               resume after flush
 ***********************************************************************
 */
static int _vxdurable_operation__fence( int timeout_ms ) {
  int fence = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  GRAPH_LOCK( SYSTEM ) {
    // Suspend to flush all operation pipelines
    if( (fence = _vxdurable_operation__suspend( timeout_ms )) == 1 ) {
      // Resume
      fence = _vxdurable_operation__resume();
    }
    else {
      fence = -1;
    }
  } GRAPH_RELEASE;

  return fence;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__assert_state( void ) {
  int fence = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  const int timeout_ms = 60000;
  GRAPH_LOCK( SYSTEM ) {

    XTRY {

      // Fence
      if( _vxdurable_operation__fence( timeout_ms ) < 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Assert state for all graphs
      int err = 0;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        vgx_AccessReason_t reason = 0;
        GRAPH_FACTORY_ACQUIRE {
          vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
          if( graphs ) {
            vgx_Graph_t **cursor = graphs;
            vgx_Graph_t *graph;
            while( (graph = *cursor++) != NULL && err == 0 ) {
              GRAPH_LOCK( graph ) {
                // Acquire readonly
                if( CALLABLE( graph )->advanced->AcquireGraphReadonly( graph, timeout_ms, false, &reason ) > 0 ) {
                  // Capture graph state
                  vgx_graph_base_counts_t counts = {0};
                  iOperation.Graph_ROG.GetBaseCounts( graph, &counts );
                  // Release readonly
                  CALLABLE( graph )->advanced->ReleaseGraphReadonly( graph );
                  // Send Assert State
                  if( iOperation.Graph_CS.AssertState( graph, &counts ) == 1 ) {
                    iOperation.Graph_CS.SetModified( graph );
                    COMMIT_GRAPH_OPERATION_CS( graph );
                  }
                  else {
                    VXDURABLE_OPERATION_REASON( graph, 0x002, "Failed to capture assert state operation" );
                    err = -1;
                  }
                }
                // Failed to acquire readonly
                else {
                  int64_t W = 0;
                  CString_t *CSTR__writable = CALLABLE( graph )->advanced->GetWritableVerticesAsCString( graph, &W );
                  if( W > 0 && reason == VGX_ACCESS_REASON_BAD_CONTEXT && CSTR__writable ) {
                    VXDURABLE_OPERATION_REASON( graph, 0x002, "%lld writable vertices held by current thread: %s", W, CStringValue( CSTR__writable ) );
                  }
                  else {
                    VXDURABLE_OPERATION_REASON( graph, 0x003, "Failed to acquire readonly (reason=%03X)", reason );
                  }
                  iString.Discard( &CSTR__writable );
                  err = -1;
                }
              } GRAPH_RELEASE;
            }
            free( (void*)graphs );
          }
        } GRAPH_FACTORY_RELEASE;
      } GRAPH_RESUME_LOCK;

      // Check
      if( err != 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }

      // Fence
      if( _vxdurable_operation__fence( timeout_ms ) < 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }

      fence = 1;

    }
    XCATCH( errcode ) {
      fence = -1;
    }
    XFINALLY {
    }

  } GRAPH_RELEASE;

  return fence;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation__emitter_checkpoint_CS( vgx_Graph_t *graph, cxmalloc_object_processing_context_t *sync_context, int64_t obj_cnt ) {
  static int64_t chkmask = (1LL << 17) - 1LL;
  static int64_t emitter_oplim = 1LL << 12;
  static int64_t output_szlim = __TX_MAX_SIZE;
  int64_t ret = 0;
  *(int64_t*)sync_context->input += obj_cnt;
  int64_t obj = *(int64_t*)sync_context->input;
  int64_t *cnt = (int64_t*)sync_context->output;
  // Make sure we don't process objects faster than we can drain to output
  if( (++(*cnt) & chkmask) == 0 || obj > chkmask ) {
    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
    int64_t t1 = t0;
    int64_t t_max_idle = 60000;
    int64_t pending_ops = LLONG_MAX;
    int64_t pending_bytes = LLONG_MAX;
    while( pending_ops > emitter_oplim || pending_bytes > output_szlim ) {
      int64_t ops_0 = iOperation.Emitter_CS.GetPending( graph );
      int64_t bytes_0;
      GRAPH_SUSPEND_LOCK( graph ) {
        bytes_0 = iOperation.System_OPEN.BytesPending( SYSTEM );
        sleep_milliseconds( 100 );
        pending_bytes = iOperation.System_OPEN.BytesPending( SYSTEM );
      } GRAPH_RESUME_LOCK;
      pending_ops = iOperation.Emitter_CS.GetPending( graph );
      // No progress was made above (and non-empty output)
      bool bytes_frozen = pending_bytes > 0 && (pending_bytes == bytes_0);
      bool ops_frozen = pending_ops > 0 && (pending_ops == ops_0);
      if( bytes_frozen || ops_frozen ) {
        t1 = __GET_CURRENT_MILLISECOND_TICK();
        // No progress for a long time
        if( t1 - t0 > t_max_idle ) {
          // emitter+agent frozen after max idle time, give up
          REASON( 0x000, "Emitter/Output not draining (%lld emitter ops, %lld system output bytes)", pending_ops, pending_bytes );
          ret = -1;
          break;
        }
      }
      else {
        t0 = __GET_CURRENT_MILLISECOND_TICK();
      }
    }

    GRAPH_SUSPEND_LOCK( graph ) {
      GRAPH_LOCK( SYSTEM ) {
        int64_t *delta = (int64_t*)sync_context->input;
        SYSTEM->OP.system.progress_CS->obj_counter += *delta;
        *delta = 0;
        if( SYSTEM->OP.system.state_CS.flags.request_cancel_sync ) {
          INFO( 0x001, "Synchronize operation cancelled" );
          ret = -1;
        }
      } GRAPH_RELEASE;
    } GRAPH_RESUME_LOCK;

    ret = pending_bytes;

  }
  return ret;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__sync_CS( vgx_Graph_t *graph, bool hard, int timeout_ms, CString_t **CSTR__error ) {
  int ret = 0;
  int suspended_eventproc_here = 0;
  vgx_EventProcessor_t *EVP_WL = NULL;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();

  if( timeout_ms < 5000 ) {
    timeout_ms = 5000;
  }

  // Temporarily enter readonly mode as a reasonable validation that we should be
  // able to proceed with sync.
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  if( CALLABLE( graph )->advanced->AcquireGraphReadonly( graph, timeout_ms, false, &reason ) > 0 ) {
    // Release readonly. We must be writable for sync to work.
    CALLABLE( graph )->advanced->ReleaseGraphReadonly( graph );
  }
  else {
    __set_error_string_from_reason( CSTR__error, graph->CSTR__name, reason );
    return -1;
  }

#define UPDATE_SYNC_STATE( State )  \
  GRAPH_SUSPEND_LOCK( graph ) {     \
    GRAPH_LOCK( SYSTEM ) {          \
      SYSTEM->OP.system.progress_CS->state = (State); \
    } GRAPH_RELEASE;                \
  } GRAPH_RESUME_LOCK

#define UPDATE_SYNC_OBJ_TOTAL( Count )  \
  GRAPH_SUSPEND_LOCK( graph ) {     \
    GRAPH_LOCK( SYSTEM ) {          \
      SYSTEM->OP.system.progress_CS->obj_counter += (Count); \
    } GRAPH_RELEASE;                \
  } GRAPH_RESUME_LOCK

  UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_Begin );

  GRAPH_LOCK( graph ) {
    if( _vgx_is_writable_CS( &graph->readonly ) ) {
      BEGIN_DISALLOW_READONLY_CS( &graph->readonly ) {
        XTRY {
          // -----------------------------
          // Ensure zero writable vertices
          // -----------------------------
          int64_t n_WL = _vgx_graph_get_vertex_WL_count_CS( graph );
          int64_t n_RO = _vgx_graph_get_vertex_RO_count_CS( graph );
          if( n_WL != 0 || n_RO != 0 ) {
            __format_error_string( CSTR__error, "Cannot sync while vertices are acquired (writable:%lld, readonly:%lld)", n_WL, n_RO );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
          }

          // ---------------------------------------------------
          // Stop Event Processor activity before operation sync
          // ---------------------------------------------------
          GRAPH_SUSPEND_LOCK( graph ) {
            vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( graph, timeout_ms );
            if( (suspended_eventproc_here = iGraphEvent.NOCS.Disable( graph, &disable_budget )) < 0 ) {
              __format_error_string( CSTR__error, "Unable to stop event processor (timeout after %d ms), cannot synchronize at this time.", disable_budget.timeout_ms );
              ret = -1;
            }
          } GRAPH_RESUME_LOCK;
          if( ret < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
          }

          // -----------------------
          // Acquire event processor
          // -----------------------
          if( iGraphEvent.IsReady( graph ) ) {
            GRAPH_SUSPEND_LOCK( graph ) {
              if( (EVP_WL = iGraphEvent.Acquire( graph, timeout_ms )) == NULL ) {
                __set_error_string( CSTR__error, "Unable to acquire event processor lock, cannot synchronize." );
                ret = -1;
              }
            } GRAPH_RESUME_LOCK;
          }
          if( ret < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
          }

          // -------------------
          // Sync graph instance
          // -------------------
          const objectid_t *obid = &graph->obid;
          const char *name = CStringValue( graph->CSTR__name );
          const char *path = CStringValue( graph->CSTR__path );
          int block_order = ivertexalloc.BlockOrder( graph->vertex_allocator );
          uint32_t t_incept = _vgx_graph_inception( graph );
          int64_t opid = iOperation.GetId_LCK( &graph->operation );
          vgx_Similarity_config_t *simconfig = graph->similarity ? &graph->similarity->params : NULL;
          VXDURABLE_OPERATION_INFO( graph, 0x004, "Synchronize graph (%s)", hard ? "truncate and populate" : "overwrite" );
          GRAPH_SUSPEND_LOCK( graph ) {
            GRAPH_LOCK( SYSTEM) {
              if( (ret = iOperation.System_SYS_CS.ResumeAgent( SYSTEM, timeout_ms )) < 0 ) {
                __set_error_string( CSTR__error, "SYSTEM Output agent is suspended" );
                ret = -1;
              }
              else if( (ret = iOperation.System_SYS_CS.CreateGraph( SYSTEM, obid, name, path, block_order, t_incept, opid, simconfig )) < 0 ) {
                __set_error_string( CSTR__error, "Failed to capture sync graph instance" );
                ret = -1;
              }
              else if( COMMIT_GRAPH_OPERATION_CS( SYSTEM ) < 0 ) {
                __set_error_string( CSTR__error, "Failed to commit sync graph instance" );
                ret = -1;
              }
            } GRAPH_RELEASE;
          } GRAPH_RESUME_LOCK;
          if( ret < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
          }

          // -------------------
          // Hard sync: Truncate
          // -------------------
          if( hard ) {
            int64_t order = GraphOrder( graph );
            if( (ret = iOperation.Graph_CS.Truncate( graph, VERTEX_TYPE_ENUMERATION_WILDCARD, order )) < 0 ) {
              __set_error_string( CSTR__error, "Failed to capture hard sync graph truncation" );
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
            }
          }

          // ----------------------
          // Sync relationship enum
          // ----------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_EnumRelationship );
          VXDURABLE_OPERATION_INFO( graph, 0x007, "Synchronize Enumerator (Relationship)" );
          if( iEnumerator_CS.Relationship.OpSync( graph ) < 0 ) {
            __set_error_string( CSTR__error, "Failed to sync relationships" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
          }

          // ---------------------
          // Sync vertex type enum
          // ---------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_EnumVertexType );
          VXDURABLE_OPERATION_INFO( graph, 0x009, "Synchronize Enumerator (Vertex Type)" );
          if( iEnumerator_CS.VertexType.OpSync( graph ) < 0 ) {
            __set_error_string( CSTR__error, "Failed to sync vertex types" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
          }

          if( !igraphfactory.EuclideanVectors() ) {
            // --------------------------
            // Sync vector dimension enum
            // --------------------------
            UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_EnumDimension );
            VXDURABLE_OPERATION_INFO( graph, 0x00B, "Synchronize Enumerator (Vector Dimension)" );
            if( iEnumerator_CS.Dimension.OpSync( graph->similarity ) < 0 ) {
              __set_error_string( CSTR__error, "Failed to sync vector dimensions" );
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x00C );
            }
            int64_t n_dim = iEnumerator_CS.Dimension.Count( graph->similarity );
            UPDATE_SYNC_OBJ_TOTAL( n_dim );
          }

          // ----------------------
          // Sync property key enum
          // ----------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_EnumPropertyKey );
          VXDURABLE_OPERATION_INFO( graph, 0x00D, "Synchronize Enumerator (Property Key)" );
          if( iEnumerator_CS.Property.Key.OpSync( graph ) < 0 ) {
            __set_error_string( CSTR__error, "Failed to sync property keys" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x00E );
          }
          int64_t n_key = iEnumerator_CS.Property.Key.Count(graph);
          UPDATE_SYNC_OBJ_TOTAL( n_key );

          // ------------------------
          // Sync property value enum
          // ------------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_EnumPropertyValue );
          VXDURABLE_OPERATION_INFO( graph, 0x00F, "Synchronize Enumerator (Property String Value)" );
          if( iEnumerator_CS.Property.Value.OpSync( graph ) < 0 ) {
            __set_error_string( CSTR__error, "Failed to sync property string values" );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x010 );
          }
          int64_t n_val = iEnumerator_CS.Property.Value.Count(graph);
          UPDATE_SYNC_OBJ_TOTAL( n_val );

          // -------------
          // Sync vertices
          // -------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_Vertices );
          VXDURABLE_OPERATION_INFO( graph, 0x011, "Synchronize Vertex (%lld)", GraphOrder( graph ) );
          if( _vxgraph_vxtable__operation_sync_vertices_CS_NT_NOROG( graph ) < 0 ) {
            //__set_error_string( CSTR__error, "Failed to sync vertices" );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x012 );
          }

          // ---------
          // Sync arcs
          // ---------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_Arcs );
          VXDURABLE_OPERATION_INFO( graph, 0x013, "Synchronize Arcs (%lld)", GraphSize( graph ) );
          if( _vxgraph_vxtable__operation_sync_arcs_CS_NT_NOROG( graph ) < 0 ) {
            //__set_error_string( CSTR__error, "Failed to sync vertices" );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x014 );
          }

          // ---------------------------------
          // Sync deletes for virtual vertices
          // ---------------------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_VirtualDeletes );
          VXDURABLE_OPERATION_INFO( graph, 0x015, "Synchronize Deletes for Virtual Vertices" );
          if( _vxgraph_vxtable__operation_sync_virtual_vertices_CS_NT_NOROG( graph ) < 0 ) {
            //__set_error_string( CSTR__error, "Failed to sync deletes for virtual vertices" );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x016 );
          }

          // -----------------------------
          // Hold while output is draining
          // -----------------------------
          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_Emit0 );
          VXDURABLE_OPERATION_INFO( graph, 0x017, "Draining emitter" );
          int64_t E0 = iOperation.Emitter_CS.GetPending( graph );
          int64_t E1 = E0;
          int64_t A0 = iOperation.System_OPEN.TransPending( SYSTEM );
          int64_t A1 = A0;
          int64_t E2 = 0;
          int64_t A2 = 0;
          int64_t t0 = __MILLISECONDS_SINCE_1970();
          int64_t t1 = t0;
          int remain_range = (int)VGX_OPERATION_SYSTEM_STATE__SYNC_Emit100 - (int)VGX_OPERATION_SYSTEM_STATE__SYNC_Emit0;
          do {
            GRAPH_SUSPEND_LOCK( graph ) {
              sleep_milliseconds( 3000 );
              A2 = iOperation.System_OPEN.TransPending( SYSTEM );
            } GRAPH_RESUME_LOCK;
            E2 = iOperation.Emitter_CS.GetPending( graph );
            // Progress
            double remain_ratio = (A2+E2) / (double)(A0+E0);
            int pct_delta = abs(remain_range - (int)round( remain_ratio * remain_range ));
            if( pct_delta > 100 ) {
              pct_delta = 100;
            }
            vgx_OperationSystemState state_pct = (vgx_OperationSystemState)((int)VGX_OPERATION_SYSTEM_STATE__SYNC_Emit0 + pct_delta);
            UPDATE_SYNC_STATE( state_pct );

            VXDURABLE_OPERATION_INFO( graph, 0x018, "Operations remaining: E=%lld A=%lld", E2, A2 );
            t1 = __MILLISECONDS_SINCE_1970();
            // No progress since last measurement
            if( E2 == E1 && A2 == A1 ) {
              if( t1 - t0 > timeout_ms ) {
                VXDURABLE_OPERATION_WARNING( graph, 0x019, "Operations not draining, holding at E=%lld A=%lld for the last %d seconds", E2, A2, timeout_ms/1000 );
                break;
              }
            }
            else {
              E1 = E2;
              A1 = A2;
              t0 = t1;
            }
          } while( E2+A2 > 0 );

          GRAPH_SUSPEND_LOCK( graph ) {
            if( iOperation.Fence( timeout_ms ) != 1 ) {
              VXDURABLE_OPERATION_REASON( graph, 0x01A, "Operation fence timeout after graph synchronization" );
            }
          } GRAPH_RESUME_LOCK;

          E2 = iOperation.Emitter_CS.GetPending( graph );
          A2 = iOperation.System_OPEN.TransPending( SYSTEM );

          if( E2+A2 > 0 ) {
            VXDURABLE_OPERATION_WARNING( graph, 0x01B, "Operation output at E=%lld A=%lld after graph synchronization", E2, A2 );
          }

          UPDATE_SYNC_STATE( VGX_OPERATION_SYSTEM_STATE__SYNC_Emit100 );

          ret = 0;

        }
        XCATCH( errcode ) {
          ret = -1;
        }
        XFINALLY {

          // -----------------------
          // Release event processor
          // -----------------------
          if( EVP_WL ) {
            iGraphEvent.Release( &EVP_WL );
          }

          // -------------------------------
          // Resume Event Processor activity
          // -------------------------------
          if( suspended_eventproc_here > 0 ) {
            vgx_ExecutionTimingBudget_t long_timeout = _vgx_get_execution_timing_budget( 0, timeout_ms );
            GRAPH_SUSPEND_LOCK( graph ) {
              if( iGraphEvent.NOCS.Enable( graph, &long_timeout ) < 0 ) {
                VXDURABLE_OPERATION_CRITICAL( graph, 0x01C, "Failed to resume Event Processor after graph synchronization" );
              }
            } GRAPH_RESUME_LOCK;
          }

        }
      } END_DISALLOW_READONLY_CS;

      // Assert state
      GRAPH_SUSPEND_LOCK( graph ) {
        if( iOperation.AssertState() < 0 ) {
          ret = -1;
        }
      } GRAPH_RESUME_LOCK;

    }
    else {
      _vgx_request_write_CS( &graph->readonly );
      ret = -1;
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__sync( vgx_Graph_t *graph, bool hard, int timeout_ms, CString_t **CSTR__error ) {
  int ret;
  GRAPH_LOCK( graph ) {
    ret = _vxdurable_operation__sync_CS( graph, hard, timeout_ms, CSTR__error );
  } GRAPH_RELEASE;
  return ret;
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation.h"
  
test_descriptor_t _vgx_vxdurable_operation_tests[] = {
  { "VGX Graph Durable Operation Tests", __utest_vxdurable_operation },
  {NULL}
};
#endif
