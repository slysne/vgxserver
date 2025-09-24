/*######################################################################
 *#
 *# vgx_server_executor.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxserver.h"



/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




#define __VGX_SERVER_EXECUTOR_MESSAGE( LEVEL, VGXServer, Executor, Code, Format, ... ) LEVEL( Code, "IO::VGX::%c(%s)::Executor(%d): " Format, __ident_letter( VGXServer ), __full_path( VGXServer ), (Executor)->id, ##__VA_ARGS__ )

#define VGX_SERVER_EXECUTOR_VERBOSE(  VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( VERBOSE,  VGXServer, Executor, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_EXECUTOR_INFO(     VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( INFO,     VGXServer, Executor, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_EXECUTOR_WARNING(  VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( WARN,     VGXServer, Executor, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_EXECUTOR_REASON(   VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( REASON,   VGXServer, Executor, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_EXECUTOR_CRITICAL( VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( CRITICAL, VGXServer, Executor, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_EXECUTOR_FATAL(    VGXServer, Executor, Code, Format, ... )   __VGX_SERVER_EXECUTOR_MESSAGE( FATAL,    VGXServer, Executor, Code, Format, ##__VA_ARGS__ )



static unsigned                     __executor__init( comlib_task_t *self );
static unsigned                     __executor__shutdown( comlib_task_t *self );
static float                        __executor__drain_pct( comlib_task_t *self );
static vgx_VGXServerExecutor_t *    __executor__new( vgx_VGXServer_t *server, int executor_id, int sz_pool );
static int                          __executor__start( vgx_VGXServer_t *server, int executor_id );
static void                         __executor__signal_shutdown( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t **executor );
static int                          __executor__destroy( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t **executor );

DECLARE_COMLIB_TASK(                __executor__task );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __executor__error( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, CString_t *CSTR__error ) {
  vgx_VGXServerResponse_t *response = &client->response;

  response->info.error.svc_exe = true;

  XTRY {
    bool unknown = response->info.http_errcode == HTTP_STATUS__NONE;
    bool empty = iStreamBuffer.Size( client->response.buffers.content ) == 0;

    // No error written yet, do it now
    if( unknown || empty ) {
      // 500 unless already set
      if( unknown ) {
        response->info.http_errcode = HTTP_STATUS__InternalServerError;
      }

      // Prepare a controlled error response
      if( vgx_server_response__prepare_body_error( response, CSTR__error ) < 0 ) {
        response->info.error.mem_err = true;
        // ??? HANDLE
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }
    }

    // Complete error response
    if( response->mediatype == MEDIA_TYPE__application_json ) {
      response->info.execution.nometas = 0;
      vgx_server_response__complete_body( server, client );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    CLIENT_DESTROY_HEADERS_CAPSULE( client );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __executor__ready( vgx_VGXServerClient_t *client, vgx_URIQueryParameters_t *params ) {
  vgx_VGXServerRequest_t *request = &client->request;
  vgx_VGXServerResponse_t *response = &client->response;

  if( iURI.Parse.SetQueryParam( request->path, params ) < 0 ) {
    response->info.http_errcode = HTTP_STATUS__BadRequest;
    return false;
  }

  return true;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __executor__produce_response( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  vgx_VGXServerResponse_t *response = &client->response;

  // Complete the request by producing a response
  if( response->info.http_errcode == HTTP_STATUS__NONE && response->status.code == HTTP_STATUS__NONE ) {
    // 200 OK
    // (NOTE: We produce HTTP 200 OK even if the plugin failed. In this case an error message body has been prepared)
    if( vgx_server_response__produce( server, client, HTTP_STATUS__OK ) < 0 ) {
      // 500 Internal Server Error
      response->info.http_errcode = HTTP_STATUS__InternalServerError;
      // Memory error
      response->info.error.mem_err = true;
      // Discard everything from the socket response buffer before we try to generate a 500 error
      iStreamBuffer.Clear( client->response.buffers.stream );
    }
  }

  CLIENT_DESTROY_HEADERS_CAPSULE( client );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __executor__preprocess( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, const char *preproc_name ) {
  int ret = 0;
  CString_t *CSTR__error = NULL;
  if( preproc_name == NULL ) {
    goto invalid_plugin_name;
  }

  // No action if preprocessor plugin is undefined
  if( !vgx_server_resource__has_pre_plugin( preproc_name ) ) {
    return 0;
  }

  vgx_URIQueryParameters_t params = {0};
  if( __executor__ready( client, &params ) ) {
    // Copy front request into dispatcher request (all except front stream buffer which is basically junk)
    vgx_VGXServerRequest_t *front_request = &client->request;
    vgx_VGXServerRequest_t *matrix_request = client->dispatcher.streams->prequest;
    vgx_server_request__copy_clean( matrix_request, front_request );
    // Swap content buffers (avoid copy content data)
    iStreamBuffer.Swap( matrix_request->buffers.content, front_request->buffers.content );

    // We also supply the front response in case preprocessor decides to terminate request early
    vgx_VGXServerResponse_t *front_response = &client->response;

    // Call preprocessor plugin
    front_response->info.execution.plugin = true;
    if( (ret = server->resource.pluginf( preproc_name, false, &params, matrix_request, front_response, &CSTR__error )) < 0 ) {
      __executor__error( server, client, CSTR__error );
    }

  }
  else {
    __set_error_string( &CSTR__error, "bad request parameter" );
    __executor__error( server, client, CSTR__error );
    ret = -1;
  }

  iString.Discard( &CSTR__error );
  iURI.Parse.ClearQueryParam( &params );

  if( ret < 0 ) {
    return ret;
  }

  // Accumulate execution time
  int64_t exec_t1_ns = __GET_CURRENT_NANOSECOND_TICK();
  int64_t exec_ns = exec_t1_ns - client->request.exec_t0_ns;
  client->response.exec_ns += exec_ns;
  // Next exec time segment starts now
  client->request.exec_t0_ns = exec_t1_ns;

  return ret;

invalid_plugin_name:
  ret = -1;
  __set_error_string( &CSTR__error, "invalid plugin name" );
  __executor__error( server, client, CSTR__error );
  iString.Discard( &CSTR__error );
  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __executor__postprocess( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, const char *plugin_name ) {
  // Non-internal response format
  bool finalize = client->response.mediatype != MEDIA_TYPE__application_x_vgx_partial;
  // Post processor plugin
  const char *post_plugin = vgx_server_resource__has_post_plugin( plugin_name ) ? plugin_name : NULL;

  const char *data;
  int64_t sz = iStreamBuffer.ReadableSegment( client->response.buffers.content, LLONG_MAX, &data, NULL );

  // Perform processing
  if( finalize || post_plugin ) {

    if( finalize ) {
      vgx_server_response__prepare_body( &client->response );
    }

    client->response.content_length = sz;
    CString_t *CSTR__error = NULL;
    vgx_VGXServerRequest_t *front_request = &client->request;
    vgx_VGXServerRequest_t *matrix_request = client->dispatcher.streams->prequest;
    // Clone the front request into matrix request (if not done so earlier in a pre-processor)
    if( matrix_request->headers->sn != front_request->headers->sn ) {
      vgx_server_request__copy_clean( matrix_request, front_request );
      // Swap content buffers (avoid copy content data)
      iStreamBuffer.Swap( matrix_request->buffers.content, front_request->buffers.content );
    }
    else {
      matrix_request->state = front_request->state;
    }

    vgx_VGXServerResponse_t *front_response = &client->response;
    
    // We operate on the matrix request and the front response
    int ret = server->resource.pluginf( post_plugin, true, NULL, matrix_request, front_response, &CSTR__error );
    if( ret < 0 ) {
      __executor__error( server, client, CSTR__error );
    }
    iString.Discard( &CSTR__error );
    if( ret < 0 ) {
      return;
    }

    if( matrix_request->headers->control.resubmit ) {
      CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__RESUBMIT );
      iStreamBuffer.Clear( client->response.buffers.content );
      return;
    }

  }

  // Produce response
  __executor__produce_response( server, client );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __executor__execute( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  vgx_VGXServerRequest_t *request = &client->request;
  vgx_VGXServerResponse_t *response = &client->response;

  vgx_URIQueryParameters_t params = {0};

  int ret = 0;
  CString_t *CSTR__error = NULL;
  if( __executor__ready( client, &params ) ) {
    if( (ret = vgx_server_resource__call( server, &params, request, response, &CSTR__error )) < 0 ) {
      __executor__error( server, client, CSTR__error );
    }
  }
  else {
    __set_error_string( &CSTR__error, "bad request parameter" );
    __executor__error( server, client, CSTR__error );
    ret = -1;
  }
  iString.Discard( &CSTR__error );
  iURI.Parse.ClearQueryParam( &params );

  if( ret < 0 ) {
    return;
  }

  // Produce response
  __executor__produce_response( server, client );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __executor__merge( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  CString_t *CSTR__error = NULL;

  if( client->dispatcher.streams == NULL ) {
    CSTR__error = CStringNewFormat( "internal merge error (streams=NULL)" );
    goto error;
  }

  if( client->dispatcher.streams->prequest->accept_type != MEDIA_TYPE__application_x_vgx_partial ) {
    CSTR__error = CStringNewFormat( "internal merge error (accept_type=%08X)", client->dispatcher.streams->prequest->accept_type );
    goto error;
  }

  if( vgx_server_dispatcher_partial__aggregate_partials( client, &CSTR__error ) < 0 ) {
    // Indicate error already caught and written to output buffer
    if( client->response.mediatype == MEDIA_TYPE__application_x_vgx_partial ) {
      client->response.info.http_errcode = HTTP_STATUS__InternalServerError;
    }
    goto error;
  }

  return 0;

error:
  __executor__error( server, client, CSTR__error );
  iString.Discard( &CSTR__error );
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_VGXServerExecutor_t,
                   executor,
                   __executor__task,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "executor/" )
{

  vgx_VGXServer_t *server = executor->server;

  int wident = executor->id;

  char numbuf[16] = {0};
  snprintf( numbuf, 15, "%c/#%d", __ident_letter( server ), wident );

  APPEND_THREAD_NAME( numbuf );
  COMLIB_TASK__AppendDescription( self, numbuf );
  
  // Initialize random generator
  __lfsr_init( ihash64( wident ) );

  // ------------------------

  comlib_task_delay_t no_delay = COMLIB_TASK_LOOP_DELAY_NANOSECONDS( 0 );

  int err;
  bool running = true;
  int64_t t_loop_slept_ns = 0;

  COMLIB_TASK_LOCK( self ) {
    executor->idle_TCS = false;
  } COMLIB_TASK_RELEASE;

  BEGIN_COMLIB_TASK_MAIN_LOOP( no_delay ) {
    int n_waiting = 0;
    if( running ) {
      executor->last_alive_ts = __GET_CURRENT_NANOSECOND_TICK();

      vgx_VGXServerClient_t *client;
      do {
        int64_t t_slept_ns = 0;
        client = vgx_server_dispatch__fetch( server, executor, &n_waiting, &t_slept_ns );
        t_loop_slept_ns += t_slept_ns;

        // No jobs waiting, exit inner loop
        if( client == NULL ) {
          break;
        }

        client->request.exec_t0_ns = __GET_CURRENT_NANOSECOND_TICK();
        // NOTES:
        //
        //  1.  Main server loop sets client->request.state to one of the executor states after it has
        //      confirmed the client on the dispatch queue. The main server loop yanks the client
        //      out of the poll list, giving up ownership.
        //  2.  When client->request.state is one of the executor states
        //      the executor thread owns the client buffers.
        //  3.  Executor is never allowed to modify client->request.state.
        //

        // Execute request
        vgx_server_pathspec_t pathspec;
        const char *plugin_name;
        switch( client->request.state ) {
        case VGXSERVER_CLIENT_STATE__PREPROCESS:
          // Preprocess then dispatch to matrix (user plugins only)
          vgx_server_resource__init_pathspec( &pathspec, server, &client->request );

          if( pathspec.type == VGX_SERVER_PLUGIN_TYPE__USER ||
              pathspec.type == VGX_SERVER_PLUGIN_TYPE__BUILTIN ||
              pathspec.type == VGX_SERVER_PLUGIN_TYPE__CUSTOM_PREFIX )
          {
            // Get the preprocessor plugin name
            plugin_name = vgx_server_resource__plugin_name( &pathspec );
            // Execute preprocessor
            if( __executor__preprocess( server, client, plugin_name ) < 0 ) {
              // Error, prepare to collect and respond
              client->response.info.execution.complete = true;
              break;
            }
            // Preprocessor produced a response
            if( client->response.info.execution.complete ) {
              __executor__produce_response( server, client );
            }
            break;
          }
          // All other resources are local
          CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__EXECUTE );
          /* FALLTHRU */
        case VGXSERVER_CLIENT_STATE__EXECUTE:
          __executor__execute( server, client );
          break;
        case VGXSERVER_CLIENT_STATE__MERGE:
          if( __executor__merge( server, client ) < 0 ) {
            break;
          }
          /* FALLTHRU */
        case VGXSERVER_CLIENT_STATE__POSTPROCESS:
          CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__POSTPROCESS );
          vgx_server_resource__init_pathspec( &pathspec, server, &client->request );
          plugin_name = vgx_server_resource__plugin_name( &pathspec );
          __executor__postprocess( server, client, plugin_name );
          break;
        default:
          break;
        }

        // We now send the client back to main server loop
        if( (err = vgx_server_dispatch__return( server, client )) < 1 ) {
          VGX_SERVER_EXECUTOR_CRITICAL( server, executor, 0x999, "Failed to communicate work completion (err=%d)", err );
        }

      } while( client );
    }

    // becomes something random 0-255
    executor->max_sleep += (rand15() & 0x1f);

    // Accept stop request if executor has been asked to exit
    COMLIB_TASK_LOCK( self ) {
      self->total_sleep_ns += t_loop_slept_ns;
      if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
        running = false;
      }
      if( COMLIB_TASK__IsStopping( self ) ) {
        COMLIB_TASK__AcceptRequest_Stop( self );
        VGX_SERVER_EXECUTOR_VERBOSE( server, executor, 0, "Stop request accepted" );
      }

      // No clients on queue, and many workers already waiting for signal on the queue condition
      executor->idle_TCS = true;
      COMLIB_TASK_WAIT_FOR_WAKE( self, 25 + executor->max_sleep );
      executor->idle_TCS = false;

    } COMLIB_TASK_RELEASE;

  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __executor__init( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_VGXServerExecutor_t *executor = COMLIB_TASK__GetData( self );
  vgx_VGXServer_t *server = executor->server;
  vgx_Graph_t *SYSTEM = server->sysgraph;

  // =============================================
  // ======== VGX SERVER EXECUTOR STARTING =========
  // =============================================

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {
      VGX_SERVER_EXECUTOR_VERBOSE( server, executor, 0, "RUNNING" );
    } COMLIB_TASK_RELEASE;
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __executor__shutdown( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_VGXServerExecutor_t *executor = COMLIB_TASK__GetData( self );
  vgx_VGXServer_t *server = executor->server;
  vgx_Graph_t *SYSTEM = server->sysgraph;

  // ==================================================
  // ======== VGX SERVER EXECUTOR SHUTTING DOWN =========
  // ==================================================

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {
      VGX_SERVER_EXECUTOR_VERBOSE( server, executor, 0, "SHUTDOWN" );
      COMLIB_TASK__ClearState_Busy( self );
    } COMLIB_TASK_RELEASE;
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static float __executor__drain_pct( comlib_task_t *self ) {

  // TODO: improve
  int n_work = 0;
  int n_max = 1;

  // First call sets max
  float pct;
  if( n_work > 0 ) {
    if( (pct = 100.0f * (float)n_work / n_max) > 100.0 ) {
      pct = 100.0f;
    }
  }
  else {
    pct = 100.0f;
  }

  return pct;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerExecutor_t * __executor__new( vgx_VGXServer_t *server, int executor_id, int sz_pool ) {

  vgx_VGXServerExecutor_t *executor = NULL;

  XTRY {

    //
    if( (executor = calloc( 1, sizeof( vgx_VGXServerExecutor_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // [Q1.2]
    // Server
    executor->server = server;

    // [Q1.3.1]
    // Executor ID
    *((int8_t*)&executor->id) = (int8_t)executor_id;

    // [Q1.3.2]
    executor->idle_TCS = true;

    // [Q1.3.3]
    executor->max_sleep = 127; // will constantly be updated randomly and roll over

    // [Q1.3.4]
    executor->priority = 0;

    // [Q1.3.2]
    executor->flags.bits = 0;

    // [Q1.4]
    // Executor last alive timestamp
    executor->last_alive_ts = 0;

    // [Q1.5]
    executor->__snapshot_ts = 0;

    // [Q1.6]
    // *future*
    executor->ns_offset = 0;

    // [Q1.7]
    int jobq_i;
    if( sz_pool > 15 ) {
      if( executor->id < 3 ) {
        jobq_i = 0;
      }
      else if( executor->id < 8 ) {
        jobq_i = 1;
      }
      else if( executor->id < 15 ) {
        jobq_i = 2;
      }
      else {
        jobq_i = 3;
      }
    }
    else {
      if( executor->id < 2 ) {
        jobq_i = 0;
      }
      else if( executor->id < 4 ) {
        jobq_i = 1;
      }
      else if( executor->id < 6 ) {
        jobq_i = 2;
      }
      else {
        jobq_i = 3;
      }
    }
    executor->jobQ = &server->dispatch.Q[jobq_i];

    // [Q1.8]
    executor->count_atomic = 0;

  }
  XCATCH( errcode ) {
    __executor__destroy( server, &executor );
  }
  XFINALLY {
  }

  return executor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __executor__start( vgx_VGXServer_t *server, int executor_id ) {
  int ret = 0;

  vgx_VGXServerExecutor_t *executor = server->pool.executors->executors[ executor_id ];

  vgx_Graph_t *SYSTEM = server->sysgraph;

  VGX_SERVER_EXECUTOR_VERBOSE( server, executor, 0, "Starting..." );

  XTRY {

    // TASK
    if( (executor->TASK = COMLIB_TASK__New( __executor__task, __executor__init, __executor__shutdown, executor )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
    }

    int started;
    GRAPH_LOCK( SYSTEM ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        started = COMLIB_TASK__Start( executor->TASK, 10000 );
      } GRAPH_RESUME_LOCK;
    } GRAPH_RELEASE;

    if( started == 1 ) {
      VGX_SERVER_EXECUTOR_VERBOSE( server, executor, 0, "Started" );
    }
    else {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
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
static void __executor__signal_shutdown( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t **executor ) {
  if( executor && *executor ) {
    vgx_VGXServerExecutor_t *EXEC = *executor;
    // Executor task exists, signal shutdown
    if( EXEC->TASK ) {
      vgx_Graph_t *SYSTEM = server->sysgraph;
      comlib_task_t *task = EXEC->TASK;
      if( SYSTEM ) {
        GRAPH_LOCK( SYSTEM ) {
          GRAPH_SUSPEND_LOCK( SYSTEM ) {
            COMLIB_TASK_LOCK( task ) {
              COMLIB_TASK__Request_Suspend( task );
            } COMLIB_TASK_RELEASE;
            // Fill dispatch pipeline with no-ops and signal to wake up executors
            vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;

            for( int qi=0; qi < DISPATCH_QUEUE_COUNT; ++qi ) {
              vgx_VGXServerDispatchQueue_t *job = &dispatch->Q[qi];
              if( job->flag.init.d_lock && job->flag.init.d_cond && job->queue ) {
                SYNCHRONIZE_ON( job->lock ) {
                  QWORD zero = 0;
                  for( int i=0; i<100; i++ ) {
                    CALLABLE( job->queue )->AppendNolock( job->queue, &zero );
                  }
                  SIGNAL_ALL_CONDITION( &(job->wake.cond) );
                } RELEASE;
              }
            }
          } GRAPH_RESUME_LOCK;
        } GRAPH_RELEASE;
      }
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __executor__destroy( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t **executor ) {
  int ret = 0;

  if( executor && *executor ) {

    vgx_VGXServerExecutor_t *EXEC = *executor;

    // Start out with assumption that executor is stopped
    int stopped = 1;

    // Executor task exists, stop it
    if( EXEC->TASK ) {
      vgx_Graph_t *SYSTEM = server->sysgraph;
      comlib_task_t *task = EXEC->TASK;
      int timeout_ms = 30000;
      // executor is not stopped, do work to stop it
      stopped = 0;
      if( SYSTEM ) {
        GRAPH_LOCK( SYSTEM ) {
          GRAPH_SUSPEND_LOCK( SYSTEM ) {
            // Shut down
            COMLIB_TASK_LOCK( task ) {
              // progressf
              if( (stopped = COMLIB_TASK__Stop( task, __executor__drain_pct, timeout_ms )) < 0 ) {
                VGX_SERVER_EXECUTOR_WARNING( server, EXEC, 0x001, "Forcing exit" );
                stopped = COMLIB_TASK__ForceExit( task, 30000 );
              }
            } COMLIB_TASK_RELEASE;
          } GRAPH_RESUME_LOCK;
        } GRAPH_RELEASE;
      }
      if( stopped != 1 ) {
        VGX_SERVER_EXECUTOR_CRITICAL( server, EXEC, 0x002, "Unresponsive executor thread #%d", (int)EXEC->id );
        ret = -1;
      }
    }

    // Executor is stoppped, clean up
    if( stopped == 1 ) {
      COMLIB_TASK__Delete( &EXEC->TASK );
      free( *executor );
      *executor = NULL;
    }

  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerExecutorPool_t * vgx_server_executor__new_pool( vgx_VGXServer_t *server, int sz_pool ) {
  vgx_VGXServerExecutorPool_t *pool = NULL;

  XTRY {
    if( (pool = calloc( 1, sizeof( vgx_VGXServerExecutorPool_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    pool->sz = sz_pool;

    if( (pool->executors = calloc( pool->sz, sizeof( vgx_VGXServerExecutor_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    for( int i=0; i<pool->sz; i++ ) {
      if( (pool->executors[i] = __executor__new( server, i, sz_pool )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
    }
    
  }
  XCATCH( errcode ) {
    vgx_server_executor__delete_pool( server, &pool );
  }
  XFINALLY {
  }

  return pool;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_executor__start_all( vgx_VGXServer_t *server ) {
  int ret = 0;

  vgx_VGXServerExecutorPool_t *executors = server->pool.executors;

  XTRY {

    for( int16_t i=0; i<executors->sz; i++ ) {
      if( __executor__start( server, i ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
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
DLL_HIDDEN void vgx_server_executor__delete_pool( vgx_VGXServer_t *server, vgx_VGXServerExecutorPool_t **executor_pool ) {
  if( executor_pool && *executor_pool ) {
    vgx_VGXServerExecutorPool_t *pool = *executor_pool;
    if( pool->executors ) {
      int sz = pool->sz;
      for( int i=0; i<sz; i++ ) {
        __executor__signal_shutdown( server, &pool->executors[i] );
      }
      for( int i=0; i<sz; i++ ) {
        __executor__destroy( server, &pool->executors[i] );
      }
      free( (*executor_pool)->executors );
    }
    free( *executor_pool );
    *executor_pool = NULL;
  }
}

