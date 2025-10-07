/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_service.c
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

#include "_vgx.h"
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static vgx_StringList_t *           __service__get_all_client_uris_TCS( vgx_VGXServer_t *server );
static int                          __service__check_runstate_TCS( comlib_task_t *self, vgx_VGXServer_t *server );
static void                         __service__handle_suspend_request_TCS( comlib_task_t *self, vgx_VGXServer_t *server );
static void                         __service__handle_stop_request_TCS( comlib_task_t *self, vgx_VGXServer_t *server );
static void                         __service__control_init_SYS_CS( vgx_VGXServer_t *server );
static int                          __service__initialize_SYS_CS( vgx_Graph_t *SYSTEM, vgx_VGXServer_t *server, vgx_VGXServerConfig_t **cf );
static int                          __service__initialize_OPEN( vgx_Graph_t *SYSTEM, vgx_VGXServer_t *server, vgx_VGXServerConfig_t **cf );
static int                          __service__destroy_SYS_CS( vgx_VGXServer_t *server );
static int                          __service__destroy_OPEN( vgx_VGXServer_t *server );
static int                          __service__start_new_vgxserver( vgx_Graph_t *SYSTEM, const char *ip, uint16_t port, const char *prefix, bool service_in, f_vgx_ServicePluginCall pluginf, vgx_VGXServerDispatcherConfig_t **cf_dispatcher );
static int                          __service__stop_delete_vgxserver( vgx_Graph_t *SYSTEM );
static int                          __service__restart_vgxserver( vgx_Graph_t *SYSTEM, CString_t **CSTR__error );
static int                          __service__get_port( const vgx_Graph_t *SYSTEM );
static const char *                 __service__get_admin_ip( const vgx_Graph_t *SYSTEM );
static int                          __service__get_admin_port( const vgx_Graph_t *SYSTEM );
static const char *                 __service__get_prefix( const vgx_Graph_t *SYSTEM );
static int                          __service__set_name( vgx_Graph_t *SYSTEM, const char *name );
static CString_t *                  __service__get_name( vgx_Graph_t *SYSTEM );
static vgx_StringList_t *           __service__get_all_client_uris( vgx_Graph_t *SYSTEM, int timeout_ms );
static int                          __service__in( vgx_Graph_t *SYSTEM );
static int                          __service__out( vgx_Graph_t *SYSTEM );

static unsigned                     __service__task_initialize( comlib_task_t *self );
static unsigned                     __service__task_shutdown( comlib_task_t *self );
DECLARE_COMLIB_TASK(                __service__task_vgxserver );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IVGXServer_t iVGXServer = {
  .Artifacts = {
    .Create               = vgx_server_artifacts__create,
    .Destroy              = vgx_server_artifacts__destroy,
  },

  .Resource = {
    .Init                 = vgx_server_resource__init,
    .Clear                = vgx_server_resource__clear,
    .Plugin = {
      .Init               = vgx_server_resource__plugin_init,
      .Clear              = vgx_server_resource__plugin_clear,
      .Register           = vgx_server_resource__add_plugin,
      .Unregister         = vgx_server_resource__del_plugin,
      .IsRegistered       = vgx_server_resource__get_plugin_phases
    }
  },

  .Service = {
    .StartNew             = __service__start_new_vgxserver,
    .StopDelete           = __service__stop_delete_vgxserver,
    .Restart              = __service__restart_vgxserver,
    .GetPort              = __service__get_port,
    .GetAdminIP           = __service__get_admin_ip,
    .GetAdminPort         = __service__get_admin_port,
    .GetPrefix            = __service__get_prefix,
    .SetName              = __service__set_name,
    .GetName              = __service__get_name,
    .GetAllClientURIs     = __service__get_all_client_uris,
    .In                   = __service__in,
    .Out                  = __service__out,
  },

  .Counters = {
    .Get                  = vgx_server_counters__get,
    .Reset                = vgx_server_counters__reset,
    .GetLatencyPercentile = vgx_server_counters__get_latency_percentile,
    .GetMatrixInspect     = vgx_server_counters__inspect_matrix
  },

  .Request = {
    .New                  = vgx_server_request__new,
    .Delete               = vgx_server_request__delete,
    .AddHeader            = vgx_server_request__add_header,
    .AddContent           = vgx_server_request__add_content,
  },

  .Response = {
    .New                  = vgx_server_response__new,
    .Delete               = vgx_server_response__delete,
    .PrepareBody          = vgx_server_response__prepare_body,
    .PrepareBodyError     = vgx_server_response__prepare_body_error
  },

  .Util = {
    .SendAll              = vgx_server_util__sendall,
    .ReceiveAll           = vgx_server_util__recvall
  },

  .Config = {
    .New                  = vgx_server_config__new,
    .Delete               = vgx_server_config__delete,
    .Clone                = vgx_server_config__clone,
    .SetFront             = vgx_server_config__set_front,
    .SetDispatcher        = vgx_server_config__set_dispatcher,
    .Get                  = vgx_server_config__get,
    .SetExecutorPluginEntrypoint = vgx_server_config__set_executor_plugin_entrypoint,
    
    .Front = {
      .New                = vgx_server_config__front_new,
      .Delete             = vgx_server_config__front_delete
    },

    .Dispatcher = {
      .New                = vgx_server_config__dispatcher_new,
      .Delete             = vgx_server_config__dispatcher_delete,
      .SetReplicaAddress  = vgx_server_config__dispatcher_set_replica_address,
      .SetReplicaAccess   = vgx_server_config__dispatcher_set_replica_access,
      .SetReplicaChannels = vgx_server_config__dispatcher_set_replica_channels,
      .SetReplicaPriority = vgx_server_config__dispatcher_set_replica_priority,
      .Verify             = vgx_server_config__dispatcher_verify,
      .Channels           = vgx_server_config__dispatcher_channels,
      .Dump               = vgx_server_config__dispatcher_dump
    }
  },

  .Dispatcher = {
    .Matrix = {
      .Init               = vgx_server_dispatcher_matrix__init,
      .Clear              = vgx_server_dispatcher_matrix__clear,
      .Width              = vgx_server_dispatcher_matrix__width,
      .BacklogSize        = vgx_server_dispatcher_dispatch__backlog_size,
      .BacklogCount       = vgx_server_dispatcher_dispatch__backlog_count
    }
  }

};




#define __VGXSERVER_MESSAGE( LEVEL, VGXServer, Code, Format, ... ) LEVEL( Code, "IO::VGX::%c(%s): " Format, __ident_letter( VGXServer ), __full_path( VGXServer ), ##__VA_ARGS__ )

#define VGXSERVER_VERBOSE( VGXServer, Code, Format, ... )   __VGXSERVER_MESSAGE( VERBOSE, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGXSERVER_INFO( VGXServer, Code, Format, ... )      __VGXSERVER_MESSAGE( INFO, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGXSERVER_WARNING( VGXServer, Code, Format, ... )   __VGXSERVER_MESSAGE( WARN, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGXSERVER_REASON( VGXServer, Code, Format, ... )    __VGXSERVER_MESSAGE( REASON, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGXSERVER_CRITICAL( VGXServer, Code, Format, ... )  __VGXSERVER_MESSAGE( CRITICAL, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGXSERVER_FATAL( VGXServer, Code, Format, ... )     __VGXSERVER_MESSAGE( FATAL, VGXServer, Code, Format, ##__VA_ARGS__ )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __open_server( vgx_VGXServer_t *server, CString_t **CSTR__error ) {
  static const int BACKLOG = 128;

  int ret = 0;
  XTRY {
    // Bind
    if( iURI.Bind( server->Listen, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Listen
    if( iURI.Listen( server->Listen, BACKLOG, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
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
static unsigned __service__task_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_VGXServer_t *server = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = server->sysgraph;

  CString_t *CSTR__error = NULL;

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {

      XTRY {
        // Create server socket, bind, and listen
        if( __open_server( server, &CSTR__error ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }

#ifdef VGXSERVER_USE_LINUX_EVENTFD
        VGXSERVER_INFO( server, 0x000, "Using eventfd" );
#else
        // Start internal completion monitor
        if( vgx_server_dispatch__start_monitor( server, &CSTR__error ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
#endif

        // Assert listen exists
        if( server->Listen == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
      }
      XCATCH( errcode ) {
        if( CSTR__error ) {
          VGXSERVER_REASON( server, 0x010, "Failed to initialize VGX Server: %s", CStringValue( CSTR__error ) );
        }
        ret = 1; // nonzero = error
      }
      XFINALLY {
        iString.Discard( &CSTR__error );
      }
    } COMLIB_TASK_RELEASE;
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __service__task_shutdown( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_VGXServer_t *server = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = server->sysgraph;

  // =======================================
  // ======== SERVER SHUTTING DOWN =========
  // =======================================

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {
      
      COMLIB_TASK__ClearState_Busy( self );

      // [Q1.8] Wake
      iURI.Delete( &server->dispatch.completion.monitor );
      iURI.Delete( &server->dispatch.completion.signal );

      // [Q1.4] Listen
      iURI.Delete( &server->Listen );

    } COMLIB_TASK_RELEASE;

  } GRAPH_RELEASE;

  server->resource.artifacts = NULL;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_StringList_t * __service__get_all_client_uris_TCS( vgx_VGXServer_t *server ) {
  int n_clients = vgx_server_client__count_TCS( server );
  vgx_StringList_t *client_uris = iString.List.New( server->sysgraph->ephemeral_string_allocator_context, n_clients );
  if( client_uris ) {
    vgx_VGXServerClient_t *io_client = server->pool.clients.iolist.head;
    int n = 0;
    while( io_client && n < n_clients ) {
      if( io_client->URI ) {
        iString.List.SetItem( client_uris, n++, iURI.URI( io_client->URI ) );
      }
      io_client = io_client->chain.next;
    }
  }
  return client_uris;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__check_runstate_TCS( comlib_task_t *self, vgx_VGXServer_t *server ) {
  // Stop requested? Accept request and enter stopping state
  if( COMLIB_TASK__IsRequested_Stop( self ) ) {
    COMLIB_TASK__SetState_Stopping( self );
    server->control.flag_TCS.suspended = false;
    server->control.flag_TCS.suspend_request = false;
    server->control.flag_TCS.stop_requested = true;
    VGXSERVER_GLOBAL_SERVICE_OUT( server );
    return 1;
  }
  else {
    // Suspend requested?
    if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
      COMLIB_TASK__AcceptRequest_Suspend( self );
      if( !server->control.flag_TCS.suspended ) {
        COMLIB_TASK__SetState_Suspending( self );
        server->control.flag_TCS.suspend_request = true;
      }
      return 1;
    }
    // Resume requested ?
    if( COMLIB_TASK__IsRequested_Resume( self ) ) {
      COMLIB_TASK__AcceptRequest_Resume( self );
      COMLIB_TASK__ClearState_Suspending( self );
      COMLIB_TASK__ClearState_Suspended( self );
      server->control.flag_TCS.suspended = false;
      server->control.flag_TCS.suspend_request = false;
      VGXSERVER_GLOBAL_SERVICE_IN( server );
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __service__handle_suspend_request_TCS( comlib_task_t *self, vgx_VGXServer_t *server ) {
  // Suspend if requested
  if( server->control.flag_TCS.suspend_request ) {
    COMLIB_TASK__ClearState_Busy( self );
    server->control.flag_TCS.suspend_request = false;
    // Enter suspended state only if the suspending state is still in effect
    if( COMLIB_TASK__IsSuspending( self ) ) {
      COMLIB_TASK__SetState_Suspended( self );
      COMLIB_TASK__ClearState_Suspending( self );
      server->control.flag_TCS.suspended = true;
      VGXSERVER_GLOBAL_SERVICE_OUT( server );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __service__handle_stop_request_TCS( comlib_task_t *self, vgx_VGXServer_t *server ) {
  if( COMLIB_TASK__IsStopping( self ) ) {
    int active = vgx_server_client__count_active_TCS( server );
    VERBOSE( 0x000, "%d active clients, task busy?=%d", active, COMLIB_TASK__IsBusy( self ) );
    if( active == 0 ) {
      COMLIB_TASK__AcceptRequest_Stop( self );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_VGXServer_t,
                   server,
                   __service__task_vgxserver,
                   CXLIB_THREAD_PRIORITY_ABOVE_NORMAL,
                   "vgxserver/" )
{
#define PERF_COUNTER_REFRESH_INTERVAL 1000000000LL /* 1 second */
#define IO_LOOP_MAX_DURATION           250000000LL /* 250 ms */

  char label[] = "vgx_api_service_X";
  char server_X = __ident_letter( server );
  label[sizeof(label)-2] = server_X;

  SET_CURRENT_THREAD_LABEL( label );
  
  vgx_Graph_t *SYSTEM = server->sysgraph;
  
  char namebuf[512] = {0};
  snprintf( namebuf, 511, "%c/%s", server_X, CStringValue( CALLABLE( SYSTEM )->Name( SYSTEM ) ) );

  APPEND_THREAD_NAME( namebuf );
  COMLIB_TASK__AppendDescription( self, namebuf );

  // ------------------------

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

  server->counters.perf->server_t0 = __GET_CURRENT_NANOSECOND_TICK();
  server->tbase_ns = __MILLISECONDS_SINCE_1970() * 1000000LL - server->counters.perf->server_t0;

  // One second
  int64_t measure_deadline = server->counters.perf->server_t0 + PERF_COUNTER_REFRESH_INTERVAL;

  bool reset = false;

  const char *nameinfo = iURI.NameInfo( server->Listen );
  unsigned port = iURI.PortInt( server->Listen );
  const char *role = server->config.flag.is_main ? "Main" : "Admin";
  VGXSERVER_INFO( server, 0x000, "VGX Server %c running: http://%s:%u (%s)", server_X, nameinfo, port, role );

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
    
    loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

    int64_t loop_t0 = __GET_CURRENT_NANOSECOND_TICK();

    if( loop_t0 > measure_deadline ) {
      vgx_server_counters__measure( server, loop_t0, reset );
      reset = false;
      measure_deadline = loop_t0 + PERF_COUNTER_REFRESH_INTERVAL;
    }


    COMLIB_TASK_LOCK( self ) {

      // Check for server state change
      //
      __service__check_runstate_TCS( self, server );
      //
      // -----------------------------

      // Update S-IN/S-OUT state
      server->control.serving_public = server->control.public_service_in_TCS;

      // Update overall server state
      VGXSERVER_GLOBAL_SERVICE_SET( server, server->control.any_service_in_TCS );

      // No change pending
      server->control.flag_TCS.change_pending = 0;

      vgx_URI_t *Listen = server->Listen;
      CXSOCKET *pserver_sock = Listen ? iURI.Sock.Input.Get( Listen ) : NULL;
      if( pserver_sock ) {
        COMLIB_TASK__SetState_Busy( self );
        COMLIB_TASK_SUSPEND_LOCK( self ) {

          // -------------------------------------
          // Process I/O in tight loop for a while
          // -------------------------------------
          vgx_server_io__process_socket_events( server, IO_LOOP_MAX_DURATION );

        } COMLIB_TASK_RESUME_LOCK;
        COMLIB_TASK__ClearState_Busy( self );
      }
      // Server not open
      else {
        loop_delay = COMLIB_TASK_LOOP_DELAY( 500 );
      }

      // Snapshot requested
      if( server->control.flag_TCS.snapshot_request ) {
        if( server->control.flag_TCS.reset_counters ) {
          server->control.flag_TCS.reset_counters = false;
          reset = true;
        }
        vgx_server_counters__take_snapshot( server->counters.perf );
        server->control.flag_TCS.snapshot_request = false;
        COMLIB_TASK_SIGNAL_WAKE( self );
      }

      // Inspect requested
      if( server->control.flag_TCS.inspect_request ) {
        vgx_server_counters__inspect_TCS( server );
        server->control.flag_TCS.inspect_request = false;
      }
      
      // ----------------------------
      //
      __service__handle_suspend_request_TCS( self, server );
      __service__handle_stop_request_TCS( self, server );
      //
      // ----------------------------

    } COMLIB_TASK_RELEASE;
  } END_COMLIB_TASK_MAIN_LOOP;

  vgx_server_dispatch__dispose_and_disconnect_TCS( self, server );

  VGXSERVER_INFO( server, 0x000, "VGX Server %c stopped: http://%s:%u (%s)", server_X, nameinfo, port, role );

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __service__control_init_SYS_CS( vgx_VGXServer_t *server ) {
  server->control.serving_public = false;
  server->control.serving_any = true;
  server->control.__rsv_1_2_1_3 = 0;
  server->control.__rsv_1_2_1_4 = 0;
  server->control.polling_CCS = false;
  server->control.public_service_in_TCS = false;
  server->control.any_service_in_TCS = true;
  server->control.flag_TCS.change_pending = false;
  server->control.flag_TCS.snapshot_request = false;
  server->control.flag_TCS.reset_counters = false;
  server->control.flag_TCS.stop_requested = false;
  server->control.flag_TCS.inspect_request = false;
  server->control.flag_TCS._rsv_1_2_2_5_6 = false;
  server->control.flag_TCS.suspended = false;
  server->control.flag_TCS.suspend_request = false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __create_WEB_ROOT( void ) {
  // System root
  const CString_t *CSTR__root = igraphfactory.SystemRoot();
  if( CSTR__root == NULL ) {
    return -1;
  }

  // Absolute path to system root
  char *abs_vgxroot = get_abspath( CStringValue( CSTR__root ) );
  if( abs_vgxroot == NULL ) {
    return -1;
  }

  // Absolute path to web root
  CString_t *CSTR__webroot = CStringNewFormat( "%s/WEB-ROOT", abs_vgxroot );
  free( abs_vgxroot );
  if( CSTR__webroot == NULL ) {
    return -1;
  }

  int ret = create_dirs( CStringValue( CSTR__webroot ) );
  iString.Discard( &CSTR__webroot );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__initialize_SYS_CS( vgx_Graph_t *SYSTEM, vgx_VGXServer_t *server, vgx_VGXServerConfig_t **cf ) {
  int ret = 0;

  // Reset entire object to zero
  memset( server, 0, sizeof( vgx_VGXServer_t ) );

  // Create WEB-ROOT directory
  if( __create_WEB_ROOT() < 0 ) {
    return -1;
  }

  // Steal the configuration
  if( cf && *cf ) {
    // [Q3.1]
    server->config.cf_server = *cf;
    *cf = NULL;
  }
  else {
    return -1;
  }

  // Pool capacities
  struct {
    int n_executors;
    int n_clients;
    int n_channels;
    int n_fd;
  } capacity = {0};

  switch( __server_port_offset( server ) ) {
  case 0:
    capacity.n_executors = A_EXECUTOR_POOL_SIZE;
    capacity.n_clients = A_NCLIENT_SLOTS;
    break;
  case 1:
    capacity.n_executors = B_EXECUTOR_POOL_SIZE;
    capacity.n_clients = B_NCLIENT_SLOTS;
    break;
  default:
    capacity.n_executors = C_EXECUTOR_POOL_SIZE;
    capacity.n_clients = C_NCLIENT_SLOTS;
  }
  if( server->config.cf_server->dispatcher ) {
    capacity.n_channels = iVGXServer.Config.Dispatcher.Channels( server->config.cf_server->dispatcher );
  }
  capacity.n_fd = capacity.n_clients + capacity.n_channels;
  
  CString_t *CSTR__error = NULL;


  XTRY {

    // -------------------------------------------------------------------
    // [Q1.1]
    // Task will be created on main thread startup later
    server->TASK = NULL;

    // [Q1.2]
    // Initialize control parameters
    __service__control_init_SYS_CS( server );

    // [Q1.3]
    // Reference to systen graph
    server->sysgraph = SYSTEM;

    // [Q1.4-7]
    // Create client pool
    if( vgx_server_client__create_pool( server, capacity.n_clients ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // [Q1.8]
    // Create executor pool (threads not yet started)
    if( (server->pool.executors = vgx_server_executor__new_pool( server, capacity.n_executors )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    // -------------------------------------------------------------------


    // -------------------------------------------------------------------
    // [Q2.1]
    // Socket input buffer for reading raw bytes from socket before parsing
    PALIGNED_ZARRAY_THROWS( server->io.buffer, char, HTTP_LINE_MAX, 0x003 );

    // [Q2.2]
    // We preallocate X slots for pollfd:
    // Slot 0:           The server listen socket
    // Slot 1:           The server wake monitor socket
    // Slot 2 and up:    Clients + Channels
    //
    if( (server->io.pollfd_list = calloc( capacity.n_fd + 1LL, sizeof( struct pollfd ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }

    // [Q2.3]
    // Front clients with I/O ready sockets
    if( (server->io.ready.front = calloc( capacity.n_clients + 1LL, sizeof( vgx_VGXServerFrontIOReady_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
    }

    // [Q2.4]
    // Dispatcher channels with I/O ready sockets
    if( (server->io.ready.dispatcher = calloc( capacity.n_channels + 1LL, sizeof( vgx_VGXServerDispatcherIOReady_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
    }

    // [Q2.5]
    // Create performance counters
    if( (server->counters.perf = vgx_server_counters__new()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x007 );
    }
    server->counters.perf->n_executors = server->pool.executors->sz;

    // [Q2.6]
    // Process memory usage will be measured once main thread starts
    server->counters.mem_max_process_use = 0;

    // [Q2.7]
    // Not in use
    server->counters.client_uris_snapshot = NULL;

    // [Q2.8]
    // Misc. counter bytes
    server->counters.byte.mem_max_use_pct = 0;
    server->counters.byte.__rsv_2_8_2 = 0;
    server->counters.byte.__rsv_2_8_3 = 0;
    server->counters.byte.__rsv_2_8_4 = 0;
    server->counters.byte.__rsv_2_8_5 = 0;
    server->counters.byte.__rsv_2_8_6 = 0;
    server->counters.byte.__rsv_2_8_7 = 0;
    server->counters.byte.__rsv_2_8_8 = 0;
    // -------------------------------------------------------------------


    // -------------------------------------------------------------------
    // [Q3.1]
    // Server not yet bound
    if( (server->Listen = iURI.NewElements( "http", NULL, "0.0.0.0", __server_port( server ), NULL, NULL, NULL, &CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
    }

    // [Q3.2]
    // Main configuration already set

    // [Q3.3]
    // Assigned by application as needed
    server->config.CSTR__service_name = NULL;

    // [Q3.4]
    server->config.flag.is_main = (__server_port_offset( server ) == 0);
    server->config.flag.__rsv_3_5_2 = 0;
    server->config.flag.__rsv_3_5_3 = 0;
    server->config.flag.__rsv_3_5_4 = 0;
    server->config.flag.__rsv_3_5_5 = 0;
    server->config.flag.__rsv_3_5_6 = 0;
    server->config.flag.__rsv_3_5_7 = 0;
    server->config.flag.__rsv_3_5_8 = 0;

    // [Q3.5]
    // Initialize request serial number to microseconds since 1970
    server->req_sn = __MILLISECONDS_SINCE_1970() * 1000LL;

    // [Q3.6]
    // Time reference (system start in ns since 1970) will be initialized when we enter server loop
    server->tbase_ns = 0;

    // [Q3.7]
    // Set executor plugin entrypoint
    server->resource.pluginf = server->config.cf_server->executor_pluginf;

    // [Q3.8]
    // Builtin pages and resources
    if( (server->resource.artifacts = vgx_server_artifacts__map) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
    }

    // -------------------------------------------------------------------

    // -------------------------------------------------------------------
    // [Q4, Q5, Q6, Q7]
    // Dispatch
    if( vgx_server_dispatch__create( server, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x009 );
    }
    // -------------------------------------------------------------------


    // -------------------------------------------------------------------
    // Q8 Matrix
    if( iVGXServer.Dispatcher.Matrix.Init( server, &CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
    }

    // -------------------------------------------------------------------


    // ===============================
    // START EXECUTOR THREADS
    // ===============================
    if( vgx_server_executor__start_all( server ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x00B );
    }

    // =======================
    // START VGX SERVER THREAD
    // =======================

    VGXSERVER_VERBOSE( server, 0, "Starting VGX Server..." );

    // [Q1.1] TASK
    if( (server->TASK = COMLIB_TASK__New( __service__task_vgxserver, __service__task_initialize, __service__task_shutdown, server )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00C );
    }

    int started;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      started = COMLIB_TASK__Start( server->TASK, 10000 );
    } GRAPH_RESUME_LOCK;

    if( started == 1 ) {
      VGXSERVER_VERBOSE( server, 0, "Started" );
    }
    else {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00D );
    }


  }
  XCATCH( errcode ) {
    if( server->TASK ) {
      COMLIB_TASK__Delete( &server->TASK );
    }
    if( CSTR__error ) {
      REASON( 0x000, "%s", CStringValue(CSTR__error) );
    }

    ret = -1;
  }
  XFINALLY {
    if( CSTR__error ) {
      CStringDelete( CSTR__error );
    }
  }


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__initialize_OPEN( vgx_Graph_t *SYSTEM, vgx_VGXServer_t *server, vgx_VGXServerConfig_t **cf ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    ret = __service__initialize_SYS_CS( SYSTEM, server, cf );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__destroy_SYS_CS( vgx_VGXServer_t *server ) {
  int ret = 0;

  // Start out with assumption that server is stopped
  int stopped = 1;

  // Task exists, stop it
  if( server->TASK ) {
    vgx_Graph_t *SYSTEM = server->sysgraph;
    comlib_task_t *task = server->TASK;
    int timeout_ms = 30000;
    // Server is not stopped, do work to stop it
    stopped = 0;
    if( SYSTEM ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        COMLIB_TASK_LOCK( task ) {
          // Halt all traffic and wait for shutdown
          server->control.any_service_in_TCS = false;
          server->control.serving_any = false; // <- !!!
          if( (stopped = COMLIB_TASK__Stop( task, vgx_server_dispatch__drain_pct, timeout_ms )) < 0 ) {
            VGXSERVER_WARNING( server, 0x001, "Forcing exit" );
            stopped = COMLIB_TASK__ForceExit( task, 30000 );
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
    if( stopped != 1 ) {
      VGXSERVER_CRITICAL( server, 0x002, "Unresponsive VGX Server thread" );
      ret = -1;
    }
  }

  // Server is stoppped, clean up
  if( stopped == 1 ) {
    // [Q1.1] TASK
    COMLIB_TASK__Delete( &server->TASK );
  }

  // [Q8]
  // Destroy the matrix
  INFO( 0x003, "Destroying dispatcher matrix" );
  iVGXServer.Dispatcher.Matrix.Clear( server );

  // [Q1.8]
  // Stop all executors
  INFO( 0x004, "Stopping executors" );
  vgx_server_executor__delete_pool( server, &server->pool.executors );

  // [Q1.4-7]
  // Destroy client pool
  INFO( 0x005, "Destroying client pool" );
  vgx_server_client__destroy_pool( server );

  // [Q2.1]
  // Destroy socket input buffer
  if( server->io.buffer ) {
    ALIGNED_FREE( server->io.buffer );
  }

  // [Q2.2]
  // Destroy file descriptor list
  if( server->io.pollfd_list ) {
    free( server->io.pollfd_list );
  }

  // [Q2.3]
  // Destroy list of front clients with I/O ready sockets
  if( server->io.ready.front ) {
    free( server->io.ready.front );
  }

  // [Q2.4]
  // Destroy list of dispatcher channels with I/O ready sockets
  if( server->io.ready.dispatcher ) {
    free( server->io.ready.dispatcher );
  }

  // [Q2.5]
  // Delete performance counters
  vgx_server_counters__delete( &server->counters.perf );

  // [Q2.7]
  iString.List.Discard( &server->counters.client_uris_snapshot );

  // [Q3.1]
  // Close and destroy server socket
  if( server->Listen ) {
    iURI.Delete( &server->Listen );
  }

  // [Q3.2]
  // Delete server config
  iVGXServer.Config.Delete( &server->config.cf_server );

  // [Q3.3]
  // Delete friendly service name
  iString.Discard( &server->config.CSTR__service_name );

  // [Q4] [Q5] [Q6] [Q7]
  // Destroy the dispatch
  INFO( 0x006, "Destroying server dispatch" );
  vgx_server_dispatch__destroy( server );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__destroy_OPEN( vgx_VGXServer_t *server ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      ret = __service__destroy_SYS_CS( server );
    } GRAPH_RELEASE;
  }
  else {
    ret = __service__destroy_SYS_CS( server );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __service__new_prefix( const char *prefix ) {
  char *prefix_trim = NULL;
  if( prefix ) {
    // make sure prefix starts with / and has no trailing /
    // allow to prepend / and append nul
    if( (prefix_trim = calloc( strlen(prefix)+2, sizeof(char) )) != NULL ) {
      const char *p = prefix;
      char *wp = prefix_trim;
      // prepend / if necessary
      if( *p != '/' ) {
        *wp++ = '/';
      }
      // copy path until end
      while( *p != '\0' ) {
        *wp++ = *p++;
      }
      // erase trailing / if any
      while( *(wp-1) == '/' ) {
        --wp;
      }
      *wp = '\0';
    }
  }
  return prefix_trim;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__start_new_vgxserver( vgx_Graph_t *SYSTEM, const char *ip, uint16_t port, const char *prefix, bool service_in, f_vgx_ServicePluginCall pluginf, vgx_VGXServerDispatcherConfig_t **cf_dispatcher ) {
  int ret = 0;

  char *service_prefix = __service__new_prefix( prefix );

  vgx_server_endpoint__init();

  CString_t *CSTR__error = NULL;

  vgx_VGXServerConfig_t *cf = NULL;
  vgx_VGXServerFrontConfig_t *ucf = NULL;

  XTRY {
    // Remove any previous server
    if( iVGXServer.Service.StopDelete( SYSTEM ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( vgx_server_artifacts__map == NULL ) {
      if( iVGXServer.Artifacts.Create( service_prefix ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

    if( iVGXServer.Resource.Init() < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    vgx_VGXServer_t **server_instances[] = {
      &SYSTEM->vgxserverA,
      &SYSTEM->vgxserverB,
      NULL
    }, ***ps = server_instances, **s;

    uint16_t offset = 0;
    while( (s=*ps++) != NULL ) {
      // Allocate server object 
      if( (*s = calloc( 1, sizeof( vgx_VGXServer_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }

      // Server config
      if( (cf = iVGXServer.Config.New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
      }

      // Set executor plugin entrypoint
      iVGXServer.Config.SetExecutorPluginEntrypoint( cf, pluginf );

      // Create and set (steal) front config
      if( (ucf = iVGXServer.Config.Front.New( ip, port, offset, service_prefix )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
      }
      iVGXServer.Config.SetFront( cf, &ucf );

      // Set (steal) dispatcher config for the main server.
      // All port+n server(s) are local only.
      if( offset == 0 && cf_dispatcher && *cf_dispatcher ) {
        if( iVGXServer.Config.Dispatcher.Verify( *cf_dispatcher, &CSTR__error ) == false ) {
          THROW_ERROR( CXLIB_ERR_CONFIG, 0x009 );
        }

        // Steal dispatcher config
        iVGXServer.Config.SetDispatcher( cf, cf_dispatcher );
      }

      // Initialize and start service
      if( __service__initialize_OPEN( SYSTEM, *s, &cf ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
      }

      ++offset;
    }

    // All servers running S-OUT
    // Now S-IN if requested
    if( service_in ) {
      iVGXServer.Service.In( SYSTEM );
    }


    // 
    //
    vgx_VGXServerRequest_t *init_request = iVGXServer.Request.New( HTTP_GET, "/vgx/builtin/init" );
    vgx_VGXServerResponse_t *init_response = iVGXServer.Response.New( "initial_request" );
    if( SYSTEM->vgxserverB->resource.pluginf( "sysplugin__init", false, NULL, init_request, init_response, &CSTR__error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x00B, "VGX Server init failed: %s", CSTR__error ? CStringValue( CSTR__error ) : "?" );
    }
    iVGXServer.Request.Delete( &init_request );
    iVGXServer.Response.Delete( &init_response );

  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
      REASON( 0x000, "%s", CStringValue( CSTR__error ) );
    }
    iVGXServer.Config.Delete( &cf );
    iVGXServer.Config.Front.Delete( &ucf );
    iVGXServer.Service.StopDelete( SYSTEM );
    ret = -1;
  }
  XFINALLY {
    free( service_prefix );
    iString.Discard( &CSTR__error );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__stop_delete_vgxserver( vgx_Graph_t *SYSTEM ) {
  int ret = 0;

  // Shut down in reverse order
  vgx_VGXServer_t **server_instances[] = {
    &SYSTEM->vgxserverB,
    &SYSTEM->vgxserverA, // A must be last!
    NULL
  }, ***ps = server_instances, **s;

  while( (s = *ps++) != NULL ) {
    if( *s ) {
      if( (ret = __service__destroy_OPEN( *s )) < 0 ) {
        ret = -1;
      }
      free( *s );
      *s = NULL;
    }
  }

  iVGXServer.Artifacts.Destroy();

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__restart_vgxserver( vgx_Graph_t *SYSTEM, CString_t **CSTR__error ) {
  int ret = 0;

  GRAPH_LOCK( SYSTEM ) {
    vgx_VGXServerConfig_t *cf = NULL;

    XTRY {
      // Main server must be running
      if( SYSTEM->vgxserverA == NULL ) {
        __set_error_string( CSTR__error, "server was not running" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Clone server config
      if( (cf = iVGXServer.Config.Clone( SYSTEM->vgxserverA )) == NULL ) {
        __set_error_string( CSTR__error, "server config error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Extract necessary config to start new instance
      const char *ip = cf->front->CSTR__ip ? CStringValue( cf->front->CSTR__ip ) : NULL;
      uint16_t port = cf->front->port.base;
      const char *prefix = cf->front->CSTR__prefix ? CStringValue( cf->front->CSTR__prefix ) : NULL;
      f_vgx_ServicePluginCall pluginf = cf->executor_pluginf;

      // Shut down old instance
      if( __service__stop_delete_vgxserver( SYSTEM ) < 0 ) {
        __set_error_string( CSTR__error, "failed to stop server" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Start new instance
      if( __service__start_new_vgxserver( SYSTEM, ip, port, prefix, false, pluginf, &cf->dispatcher ) < 0 ) {
        __set_error_string( CSTR__error, "failed to start server" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
      iVGXServer.Config.Delete( &cf );
    }

  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__get_port( const vgx_Graph_t *SYSTEM ) {
  // "A" instance
  if( SYSTEM->vgxserverA ) {
    return __server_port( SYSTEM->vgxserverA );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __service__get_admin_ip( const vgx_Graph_t *SYSTEM ) {
  // "B" instance
  if( SYSTEM->vgxserverB ) {
    return CStringValueDefault( SYSTEM->vgxserverB->config.cf_server->front->CSTR__ip, NULL );
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
static int __service__get_admin_port( const vgx_Graph_t *SYSTEM ) {
  // "B" instance
  if( SYSTEM->vgxserverB ) {
    return __server_port( SYSTEM->vgxserverB );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __service__get_prefix( const vgx_Graph_t *SYSTEM ) {
  // "A" instance
  if( SYSTEM->vgxserverA ) {
    return __server_prefix( SYSTEM->vgxserverA );
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
static int __service__set_name( vgx_Graph_t *SYSTEM, const char *name ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    // Only "A" instance
    vgx_VGXServer_t *server = SYSTEM->vgxserverA;
    if( server ) {
      if( server->config.CSTR__service_name ) {
        iString.Discard( &server->config.CSTR__service_name );
      }
      if( name ) {
        if( (server->config.CSTR__service_name = CStringNew( name )) == NULL ) {
          ret = -1;
        }
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
static CString_t * __service__get_name( vgx_Graph_t *SYSTEM ) {
  CString_t *CSTR__name = NULL;
  GRAPH_LOCK( SYSTEM ) {
    // Only "A" instance
    vgx_VGXServer_t *server = SYSTEM->vgxserverA;
    if( server && server->config.CSTR__service_name ) {
      const char *name = CStringValue( server->config.CSTR__service_name );
      CSTR__name = CStringNew( name );
    }
  } GRAPH_RELEASE;
  return CSTR__name;
}



/*******************************************************************//**
 *
 * Return a string list of all connected uris. (NULL on failure)
 * WARNING: Caller owns string list!
 *
 ***********************************************************************
 */
static vgx_StringList_t * __service__get_all_client_uris( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  vgx_StringList_t *client_uris = NULL;
  // We need to lock so we can safely suspend in order to safely get task lock
  GRAPH_LOCK( SYSTEM ) {
    // Only "A" instance
    vgx_VGXServer_t *server = SYSTEM->vgxserverA;
    if( server ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t deadline = t0 + timeout_ms;
        COMLIB_TASK_LOCK( server->TASK ) {
          // Request a new snapshot of client uris from vgxserver main loop
          server->control.flag_TCS.snapshot_request = true;
          // Wait until a snapshot has been built
          while( VGXSERVER_GLOBAL_IS_SERVING( server ) && server->control.flag_TCS.snapshot_request && __GET_CURRENT_MILLISECOND_TICK() < deadline ) {
            COMLIB_TASK_WAIT_FOR_WAKE( server->TASK, 10 );
          }
          // Set back to false to prevent main loop from triggering new snapshot in case we timed out.
          server->control.flag_TCS.snapshot_request = false;
          // Steal snapshot
          client_uris = server->counters.client_uris_snapshot;
          server->counters.client_uris_snapshot = NULL;
            
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
  return client_uris;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__set_in( vgx_Graph_t *SYSTEM, int state ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_VGXServer_t *servers[] = {
      SYSTEM->vgxserverA,
      SYSTEM->vgxserverB,
      NULL
    };
    vgx_VGXServer_t **s = servers;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      while(*s && (*s)->TASK) {
        vgx_VGXServer_t *server = *s;
        COMLIB_TASK_LOCK( server->TASK ) {
          server->control.flag_TCS.change_pending = 1;
          server->control.public_service_in_TCS = (bool)state;
          // Hold here until server state change confirmed
          BEGIN_TIME_LIMITED_WHILE( server->TASK && server->control.flag_TCS.change_pending, 5000, NULL ) {
            COMLIB_TASK_SUSPEND_MILLISECONDS( server->TASK, 5 );
          } END_TIME_LIMITED_WHILE;
          if( server->control.flag_TCS.change_pending ) {
            ret = -1;
          }
        } COMLIB_TASK_RELEASE;
        ++s;
      }
    } GRAPH_RESUME_LOCK;
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__in( vgx_Graph_t *SYSTEM ) {
  return __service__set_in( SYSTEM, 1 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __service__out( vgx_Graph_t *SYSTEM ) {
  return __service__set_in( SYSTEM, 0 );
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vgx_server.h"
  
test_descriptor_t _vgx_server_tests[] = {
  { "VGX Server Tests", __utest_vgx_server },
  {NULL}
};
#endif
