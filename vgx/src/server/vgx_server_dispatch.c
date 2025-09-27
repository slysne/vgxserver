/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_dispatch.c
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
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int64_t                      __dispatch__length( vgx_VGXServerWorkDispatch_t *dispatch );
static void                         __dispatch__drain( vgx_VGXServerWorkDispatch_t *dispatch );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __dispatch__length( vgx_VGXServerWorkDispatch_t *dispatch ) {
  int64_t sz = 0;

  for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {
    vgx_VGXServerDispatchQueue_t *job = &dispatch->Q[i];
    SYNCHRONIZE_ON( job->lock ) {
      sz += ComlibSequenceLength( job->queue );
    } RELEASE;
  }

  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __dispatch__drain( vgx_VGXServerWorkDispatch_t *dispatch ) {
  for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {
    vgx_VGXServerDispatchQueue_t *job = &dispatch->Q[i];

    SYNCHRONIZE_ON( job->lock ) {
      uintptr_t client_addr = 0;
      while( ComlibSequenceLength( job->queue ) > 0 ) {
        CALLABLE( job->queue )->NextNolock( job->queue, (QWORD*)&client_addr );
        ATOMIC_DECREMENT_i32( &job->length_atomic );
      }
    } RELEASE;
  }

}



/*******************************************************************//**
 *
 * It is possible for executor threads to execute plugin code that might
 * result in thread termination on 
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatch__dispose_and_disconnect_TCS( comlib_task_t *self, vgx_VGXServer_t *server ) {
  static __THREAD int64_t stop_ts = -1;

  // Set the stop requested timestamps
  if( stop_ts < 0 ) {
    stop_ts = __MILLISECONDS_SINCE_1970();
    vgx_VGXServerExecutor_t **executor = server->pool.executors->executors;
    vgx_VGXServerExecutor_t **end = executor + server->pool.executors->sz;
    while( executor < end ) {
      (*executor)->__snapshot_ts = (*executor)->last_alive_ts;
      ++executor;
    }
  }
  // Check if executors exist
  else if( (__MILLISECONDS_SINCE_1970() - stop_ts) > 5000 ) {
    // Count executors who have now shown sign of life in the past 5 seconds
    int presumed_dead = 0;
    COMLIB_TASK__SetState_Busy( self );
    COMLIB_TASK_SUSPEND_LOCK( self ) {
      vgx_VGXServerExecutor_t **executor = server->pool.executors->executors;
      vgx_VGXServerExecutor_t **end = executor + server->pool.executors->sz;
      while( executor < end ) {
        if( (*executor)->__snapshot_ts == (*executor)->last_alive_ts ) {
          ++presumed_dead;
          COMLIB_TASK__SetState_Dead( (*executor)->TASK );
        }
        ++executor;
      }

      // All executors seem to have disappeared
      if( presumed_dead == server->pool.executors->sz ) {
        // Dispose of all pipelines
        __dispatch__drain( &server->dispatch );
        // Disconnect all clients
        vgx_server_client__close_all( server );
      }
    } COMLIB_TASK_RESUME_LOCK;
    COMLIB_TASK__ClearState_Busy( self );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN float vgx_server_dispatch__drain_pct( comlib_task_t *self ) {
  static int64_t pseudo_max = 100;

  vgx_VGXServer_t *server = COMLIB_TASK__GetData( self );

  int64_t n = __dispatch__length( &server->dispatch );

  float pct;

  if( n > 0 ) {
    if( (pct = 100.0f * (float)n / pseudo_max) > 100.0 ) {
      pct = 100.0;
    }
  }
  else {
    pct = 0.0;
  }

  return pct;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __dispatch__init_signal_monitor( vgx_VGXServer_t *server, CString_t **CSTR__error ) {
  int ret = 0;

  // Close any previous signal
  iURI.Close( server->dispatch.completion.signal );

  // Delete any previous monitor
  iURI.Delete( &server->dispatch.completion.monitor );

  XTRY {

    // Permanent connection to self
    if( iURI.Connect( server->dispatch.completion.signal, 10000, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Accept our internal connection
    struct pollfd fd;
    cxpollfd_pollinit( &fd, iURI.Sock.Input.Get( server->Listen ), true, false );
    if( cxpoll( &fd, 1, 1000 ) != 1 || !cxpollfd_readable( &fd ) ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // [Q3.8] Internal Wake Monitor
    if( (server->dispatch.completion.monitor = iURI.Accept( server->Listen, "http", CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

  }
  XCATCH( errcode ) {
    iURI.Delete( &server->dispatch.completion.monitor );
    iURI.Close( server->dispatch.completion.signal );
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
static int __dispatch__verify_signal_monitor( vgx_VGXServerWorkDispatch_t *dispatch ) {
  int ret = 0;

  int sz_init = 1<<12;
  BYTE *INIT = NULL;

  XTRY {
    // Initial stream of bytes to send to ourselves
    if( (INIT = malloc( sz_init )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    for( int i=0; i<sz_init; i++ ) {
      INIT[i] = (BYTE)i;
    }

    // Get signal and monitor sockets
    CXSOCKET *signal = iURI.Sock.Output.Get( dispatch->completion.signal );
    CXSOCKET *monitor = iURI.Sock.Input.Get( dispatch->completion.monitor );
    if( signal == NULL || monitor == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Send bytes to ourselves
    int64_t n_sent;
    if( (n_sent = cxsendall( signal, (const char*)INIT, sz_init, 10000 )) != sz_init ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Read all bytes sent to ourselves
    struct pollfd fd;
    for( int i=0; i<sz_init; i++ ) {
      cxpollfd_pollinit( &fd, monitor, true, false );
      if( cxpoll( &fd, 1, 1000 ) != 1 || !cxpollfd_readable( &fd ) ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
      // Read one BYTE at a time to really verify the poll mechanism
      BYTE c;
      if( cxrecv( monitor, (char*)&c, 1, 0 ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
      // Verify
      if( c != (BYTE)i ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }
    }

    // Expect wake monitor to be drained, poll should now time out.
    cxpollfd_pollinit( &fd, monitor, true, false );
    if( cxpoll( &fd, 1, 1000 ) != 0 || cxpollfd_readable( &fd ) ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( INIT );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatch__create( vgx_VGXServer_t *server, CString_t **CSTR__error ) {
  int ret = 0;

  vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;
  vgx_VGXServerExecutorCompletion_t *completion = &dispatch->completion;

  dispatch->ready = false;
  for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {
    // [Q1.8.2]
    dispatch->Q[i].flag.bits = 0;
  }
  completion->lock_init = false;

  XTRY {

    for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {
      vgx_VGXServerDispatchQueue_t *job = &dispatch->Q[i];

      // [Q1]
      // Dispatch lock
      INIT_SPINNING_CRITICAL_SECTION( &job->lock.lock, 4000 );
      // [Q3.2.1]
      job->flag.init.d_lock = 1;

      // [Q2.1-6]
      INIT_CONDITION_VARIABLE( &job->wake.cond );
      // [Q3.2.1]
      job->flag.init.d_cond = 1;

      // [Q2.7]
      // Dispatch Queue
      if( (job->queue = CQwordQueueNew( 64 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // [Q2.8.1]
      job->length_atomic = 0;

      // [Q2.8.2]
      job->n_waiting_atomic = 0;

      // [Q3.1] Last collect timestamp
      job->ts_last_collect = 0;

      // [Q3.2.2]
      job->__rsv_3_2_2 = 0;

      // [Q3.3-8]
      job->__rsv_3_3 = 0;
      job->__rsv_3_4 = 0;
      job->__rsv_3_5 = 0;
      job->__rsv_3_6 = 0;
      job->__rsv_3_7 = 0;
      job->__rsv_3_8 = 0;
    }

    // [Q15.1]
    dispatch->n_total = 0;

    // [Q15.2.1]
    dispatch->n_current = 0;

    // [Q15.3-8]
    dispatch->__rsv_15_3 = 0;
    dispatch->__rsv_15_4 = 0;
    dispatch->__rsv_15_5 = 0;
    dispatch->__rsv_15_6 = 0;
    dispatch->__rsv_15_7 = 0;
    dispatch->__rsv_15_8 = 0;

    // [Q16]
    memset( dispatch->__rsv_16, 0, sizeof(dispatch->__rsv_16) );

    // ----------
    // completion
    // ----------

    // [Q1]
    // Completion lock
    INIT_SPINNING_CRITICAL_SECTION( &completion->lock.lock, 4000 );

    // [Q2.7.1]
    completion->lock_init = true;

    // [Q2.1]
    // Completion queue
    if( (completion->queue = CQwordQueueNew( 64 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // [Q2.2.1]
    completion->length_atomic = 0;

    // [Q2.2.2]
    completion->poll_blocked_atomic = 0;

    // [Q2.3]
#ifdef VGXSERVER_USE_LINUX_EVENTFD
    // Not used on linux (we use eventfd)
    completion->signal = NULL;
#else
    // Internal Wake Signal
    if( (completion->signal = iURI.NewElements( "http", NULL, "127.0.0.1", __server_port( server ), NULL, NULL, NULL, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
#endif
    
    // [Q2.4]
    // Will be set once we accept internal connection (except linux which uses eventfd)
    completion->monitor = NULL;

    // [Q2.5]
    completion->completion_count = 0;

    // [Q2.6]
    completion->n_blocked_poll_signals = 0;
    
    // [Q2.4.2]
#ifdef VGXSERVER_USE_LINUX_EVENTFD
    VERBOSE( 0x000, "eventfd() enabled" );
    if( (completion->efd = eventfd( 0, EFD_NONBLOCK )) == -1 ) { // linux only
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
#else
    completion->efd = -1; // not used on windows
#endif
    
    // [Q2.8]
    completion->__rsv_2_8 = 0;

    // [Q15.2.2]
    dispatch->ready = true;

  }
  XCATCH( errcode ) {
    vgx_server_dispatch__destroy( server );
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
DLL_HIDDEN int vgx_server_dispatch__start_monitor( vgx_VGXServer_t *server, CString_t **CSTR__error ) {
  int ret = 0;

  XTRY {

    // [Q1.8 Q2.1]
    if( __dispatch__init_signal_monitor( server, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( __dispatch__verify_signal_monitor( &server->dispatch ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
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
DLL_HIDDEN void vgx_server_dispatch__destroy( vgx_VGXServer_t *server ) {
  if( server ) {
    vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;
    vgx_VGXServerExecutorCompletion_t *completion = &dispatch->completion;

    dispatch->ready = false;
    completion->poll_blocked_atomic = 0;

    for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {

      vgx_VGXServerDispatchQueue_t *job = &dispatch->Q[i];

      // [Q1.1/2/3/4/5]
      if( job->flag.init.d_lock ) {
        DEL_CRITICAL_SECTION( &job->lock.lock );
        job->flag.init.d_lock = 0;
      }

      // [Q1.6]
      if( job->queue ) {
        COMLIB_OBJECT_DESTROY( job->queue );
        job->queue = NULL;
      }

      // [Q2.1/2/3/4/5/6]
      if( job->flag.init.d_cond ) {
        DEL_CONDITION_VARIABLE( &job->wake.cond );
        job->flag.init.d_cond = 0;
      }
    }

    // [Q3.1/2/3/4/5]
    if( completion->lock_init ) {
      DEL_CRITICAL_SECTION( &completion->lock.lock );
      completion->lock_init = false;
    }

    // [Q3.6]
    if( completion->queue ) {
      COMLIB_OBJECT_DESTROY( completion->queue );
      completion->queue = NULL;
    }

#ifdef VGXSERVER_USE_LINUX_EVENTFD
    if( completion->efd > 0 ) {
      close( completion->efd );
      completion->efd = -1;
    }
#else
    // [Q3.8]
    iURI.Delete( &completion->signal );

    // [Q4.1]
    iURI.Delete( &completion->monitor );
#endif
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage_executor_handle_request( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  // We must yank client out of poll chain before dispatching to another component
  vgx_server_client__yank_front( server, client );
  // Preprocess before forwarding to dispatcher matrix
  if( DISPATCHER_MATRIX_ENABLED( server ) && CLIENT_HAS_PRE_PROCESSOR( client ) && client->request.method != HTTP_OPTIONS ) {
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__PREPROCESS );
  }
  // Local execution only
  else {
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__EXECUTE );
  }

  // Time of dispatch of a new request into executor
  int64_t tx_ns = __GET_CURRENT_NANOSECOND_TICK();
  // Measure elapse time, executor thread will add to this
  client->response.exec_ns = tx_ns - client->io_t0_ns;

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage_executor_post_notarget( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  if( DISPATCHER_MATRIX_MULTI_PARTIAL( &server->matrix ) || CLIENT_HAS_ANY_PROCESSOR( client ) ) {
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__MERGE );
    return 1; // merge start using the 2nd job queue
  }
  else {
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__POSTPROCESS );
    return 0; // postprocess start at 1st job queue
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage_executor_post_target( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_VGXServerResponse_t *partial_response ) {
  // Response ok
  if( partial_response->status.code == HTTP_STATUS__OK ) {
    // Inherit content offset
    client->response.content_offset = partial_response->content_offset;
    // Swap partial response content buffer into client response content buffer and apply offset
    iStreamBuffer.Swap( partial_response->buffers.content, client->response.buffers.content );
    iStreamBuffer.AdvanceRead( client->response.buffers.content, client->response.content_offset );
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__POSTPROCESS );
    return 0; // postprocess start at 1st job queue
  }
  // Response error: send into executor for error processing
  else if( DISPATCHER_MATRIX_MULTI_PARTIAL( &server->matrix ) || CLIENT_HAS_ANY_PROCESSOR( client ) ) {
    CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__MERGE );
    return 1; // merge start using the 2nd job queue
  }
  else {
    return DISPATCH_QUEUE_BYPASS; // bypass to front
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage_executor_dispatch_complete( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  // Time of dispatcher matrix completion
  int64_t tc_ns = __GET_CURRENT_NANOSECOND_TICK();
  // Add matrix execution time to elapse time
  client->response.exec_ns += tc_ns - client->request.exec_t0_ns;
  // Set start of next phase of execution
  client->request.exec_t0_ns = tc_ns;

  // Move to next phase
  vgx_VGXServerDispatcherStreamSet_t *streams = client->dispatcher.streams;
  vgx_VGXServerRequest_t *matrix_request = streams->prequest;

  // Not a specific partial target
  int target_partial = matrix_request->target_partial;
  if( target_partial < 0 ) {
    return __stage_executor_post_notarget( server, client );
  }

  // (Valid) specific partial target 
  if( target_partial < streams->responses.len ) {
    vgx_VGXServerResponse_t *partial_response = streams->responses.list[ target_partial ];
    return __stage_executor_post_target( server, client, partial_response );
  }

  // Invalid partial target
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage_executor_queue( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, int queue_index ) {

  // Select optimal queue: waterfall
  bool signal_sent = false;
  int staged = 0;
  vgx_VGXServerDispatchQueue_t *job = NULL;
  while( queue_index < DISPATCH_QUEUE_COUNT ) {
    job = &server->dispatch.Q[queue_index++];
    // Use this queue since enough available workers are waiting to process entire queue, or it's the last queue
    if( ATOMIC_READ_i32( &job->length_atomic ) < ATOMIC_READ_i32( &job->n_waiting_atomic ) || queue_index == DISPATCH_QUEUE_COUNT ) {
      SYNCHRONIZE_ON( job->lock ) {
        uintptr_t client_addr = (uintptr_t)client;
        staged = CALLABLE( job->queue )->AppendNolock( job->queue, (QWORD*)&client_addr );
        ATOMIC_INCREMENT_i32( &job->length_atomic );
        // Workers are waiting on job queue, wake up one of them
        if( ATOMIC_READ_i32( &job->n_waiting_atomic ) > 0 ) {
          SIGNAL_ONE_CONDITION( &(job->wake.cond) );
          signal_sent = true;
        }
      } RELEASE;
      if( signal_sent ) {
        return staged;
      }
      break;
    }
  }

  if( staged < 1 ) {
    return -1;
  }

  // No workers are waiting on job queue, either all are busy or sleeping. Wake up one sleeping worker.
  vgx_VGXServerExecutorPool_t *pool = server->pool.executors;
  vgx_VGXServerExecutor_t **executors = pool->executors;
  vgx_VGXServerExecutor_t **end = executors + pool->sz;
  while( !signal_sent && executors < end ) {
    vgx_VGXServerExecutor_t *executor = *executors++;
    // Not interested in this executor since we didn't dispatch to its queue
    if( executor->jobQ != job ) {
      continue;
    }
    // Candidate for wakup
    if( COMLIB_TASK_TRY_LOCK( executor->TASK ) ) {
      // Sleeping, wake up
      if( executor->idle_TCS ) {
        COMLIB_TASK_SIGNAL_WAKE( executor->TASK );
        signal_sent = true;
      }
      COMLIB_TASK_RELEASE_LOCK( executor->TASK );
    }
  }

  return staged;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatch__stage_executor( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;
  int err = 0;

  int jobq_i = 0;

  // New request
  if( client->request.state == VGXSERVER_CLIENT_STATE__HANDLE_REQUEST ) {
    jobq_i = __stage_executor_handle_request( server, client );
  }
  // Process response(s) from matrix backend(s)
  else if( client->request.state == VGXSERVER_CLIENT_STATE__DISPATCH_COMPLETE ) {
    jobq_i = __stage_executor_dispatch_complete( server, client );
    if( jobq_i == DISPATCH_QUEUE_BYPASS ) {
      return vgx_server_dispatch__bypass_to_front( server, client );
    }
    if( jobq_i < 0 ) {
      goto error;
    }
  }
  // Invalid state
  else {
    goto error;
  }

  // Special request: minimum executor queue
  if( jobq_i < client->request.min_executor_pool ) {
    jobq_i = minimum_value( client->request.min_executor_pool, DISPATCH_QUEUE_COUNT-1 );
  }

  // Send client to optimal executor queue
  if( __stage_executor_queue( server, client, jobq_i ) < 1 ) {
    goto error;
  }

  // Another client dispatched
  dispatch->n_total++;
  dispatch->n_current++;

  return 0;

error:
  // Dispatch failed, we return client to poll chain to proceed with error response
  CLIENT_STATE__SET_ERROR( client );
  vgx_server_client__append_front( server, client );
  if( err < 0 ) {
    return err;
  }

  return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Internal error in request handler", true );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
*/
DLL_HIDDEN int vgx_server_dispatch__dispatch( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {

  // Client back-reference must match
  if( client->request.headers->client != client ) {
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Internal client mismatch", true );
  }

  // If here it means we have received a complete HTTP request and can
  // proceed to dispatch the request to an appropriate executor

  // In case socket request buffer is empty reset all pointers to initial positions.
  // This prevents unnecessary expansion of buffer in case we receive more data on the
  // socket that we can't process quickly while the client is dispatched.
  if( iStreamBuffer.Empty( client->request.buffers.stream ) ) {
    iStreamBuffer.Clear( client->request.buffers.stream );
  }

  // Ensure linear content buffer
  if( !iStreamBuffer.IsSingleSegment( client->request.buffers.content ) ) {
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Internal buffer error", true );
  }

  // Request will be sent directly to dispatcher matrix
  if( CLIENT_IS_DIRECT( client ) ) {
    // Give matrix direct access to the original client request
    // Matrix uses CONTENT buffer for the entire request, which is prefilled with all data at this point
    vgx_server_dispatcher_streams__set_request( client->dispatcher.streams, &client->request );
    // Time of forwarding a new request to dispatcher matrix
    client->request.exec_t0_ns = __GET_CURRENT_NANOSECOND_TICK();
    // Measure elapse time, subsequent steps will add to this
    client->response.exec_ns = client->request.exec_t0_ns - client->io_t0_ns;
    return vgx_server_dispatcher_dispatch__forward( server, client );
  }
  // Request will be (pre)processed by an executor
  else {
    return vgx_server_dispatch__stage_executor( server, client );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static void __await_dispatch_DQCS( vgx_VGXServerExecutor_t *executor, int64_t *t_slept_ns ) {
  vgx_VGXServerDispatchQueue_t *jobQ = executor->jobQ;
  *t_slept_ns = 0;
  // Wait unless many others are already waiting
  if( ATOMIC_READ_i32( &jobQ->n_waiting_atomic ) < EXECUTOR_DISPATCH_QUEUE_MAX_WAITING ) {
    // Sleep and wait for signal
    ATOMIC_INCREMENT_i32( &jobQ->n_waiting_atomic );
    *t_slept_ns = TIMED_WAIT_CONDITION_CS( &jobQ->wake.cond, &jobQ->lock.lock, 25 + executor->max_sleep );
    ATOMIC_DECREMENT_i32( &jobQ->n_waiting_atomic );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClient_t * __client_from_address_DQCS( vgx_VGXServerExecutor_t *executor, QWORD addr ) {
  vgx_VGXServerClient_t *client = (vgx_VGXServerClient_t*)addr;
  if( client == NULL ) {
    return NULL;
  }

  // Executor thread is acceptable for this request
  client->request.executor_id = executor->id;
  ATOMIC_INCREMENT_i64( &executor->count_atomic );
  return client;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerClient_t * vgx_server_dispatch__fetch( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t *executor, int *n_waiting, int64_t *t_slept_ns )  {
  vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;

  // Dispatch not ready
  if( !dispatch->ready ) {
    sleep_milliseconds(1);
    return NULL;
  }

  vgx_VGXServerClient_t *client = NULL;

  vgx_VGXServerDispatchQueue_t *jobQ = executor->jobQ;
  CQwordQueue_t *Q = jobQ->queue;

  SYNCHRONIZE_ON( jobQ->lock ) {
    
    // Empty queue - go to sleep until signal or timeout
    if( ComlibSequenceLength( Q ) == 0 ) {
      __await_dispatch_DQCS( executor, t_slept_ns );
    }

    // At least one client in queue
    if( ComlibSequenceLength( Q ) > 0 ) {
      uintptr_t client_addr = 0;
      CALLABLE( Q )->NextNolock( Q, (QWORD*)&client_addr );
      ATOMIC_DECREMENT_i32( &jobQ->length_atomic );
      client = __client_from_address_DQCS( executor, client_addr );
      // Signal if queue not empty and at least one executor needs to be woken up
      if( ComlibSequenceLength( Q ) > 0 && ATOMIC_READ_i32( &jobQ->n_waiting_atomic ) > 0 ) {
        SIGNAL_ONE_CONDITION( &(jobQ->wake.cond) );
      }
    }

    // Inform caller of the current number executors waiting for a client to be dispatached
    *n_waiting = ATOMIC_READ_i32( &jobQ->n_waiting_atomic );

  } RELEASE;

  return client;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatch__return( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
#ifdef VGXSERVER_USE_LINUX_EVENTFD
  static uint64_t noop = 1;
#else
  static char noop[1] = {1};
#endif

  // Pass the completed client back to the I/O loop via the completion queue
  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;
  uintptr_t client_addr = (uintptr_t)client;
  CQwordQueue_t *Q = completion->queue;
  int dispatched;

  SYNCHRONIZE_ON( completion->lock ) {
    dispatched = CALLABLE( Q )->AppendNolock( Q, (QWORD*)&client_addr );
    ATOMIC_INCREMENT_i32( &completion->length_atomic );
  } RELEASE;

  // If the I/O loop is currently blocked on poll() due to no current I/O activity
  // we must signal the poll to wake up. We signal by sending a dummy byte on the
  // internal signal connection, which is monitored by the blocking poll().
  if( ATOMIC_READ_i32( &completion->poll_blocked_atomic ) ) {
#ifdef VGXSERVER_USE_LINUX_EVENTFD
    if( write( completion->efd, &noop, sizeof(noop) ) == -1 ) {
      // ignore
    }
#else
    CXSOCKET *signal = iURI.Sock.Output.Get( completion->signal );
    cxsend( signal, noop, 1, 0 ); // best effort, signal only
#endif
  }

  return dispatched;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __collect( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {

  // Inject the backlog counter into the response stream buffer
  if( client->p_x_vgx_backlog_word ) {
    write_HEX_word( client->p_x_vgx_backlog_word, (WORD)server->dispatch.n_current );
  }

  // Return any remaining dispatcher resources
  vgx_server_dispatcher_dispatch__complete( server, client );

  // Clear the request content but NOT the buffer request
  // since it may have data for the next request already.
  iStreamBuffer.Clear( client->request.buffers.content );

  // Clear any header capsule that was not cleaned up in the executor
  CLIENT_DESTROY_HEADERS_CAPSULE( client );
  
  // Put the client back on the list for poll monitoring and transition state to collected
  // Poll loop may now monitor client's socket and perform I/O again
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__COLLECTED );
  vgx_server_client__append_front( server, client );
  
  // Detect error
  if( client->response.info.http_errcode ) {
    vgx_server_response__produce_error( server, client, client->response.info.http_errcode, NULL, false );
  }

  // May or may not send anything
  vgx_server_io__front_send( server, client );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatch__executor_complete( vgx_VGXServer_t *server ) {

  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;

  if( ATOMIC_READ_i32( &completion->length_atomic ) == 0 ) {
    return 0;
  }

  // Retrieve up to a maximum of completed clients
  CQwordQueue_t *Q = completion->queue;
  int64_t max_exec = server->pool.executors->sz;
  vgx_VGXServerClient_t *completed[MAX_EXECUTOR_POOL_SIZE];
  QWORD *addr = (QWORD*)completed;
  int n_completed = 0;
  SYNCHRONIZE_ON( completion->lock ) {
    int64_t sz_Q = ComlibSequenceLength( Q );
    if( sz_Q > 0 ) {
      n_completed = (int)CALLABLE( Q )->ReadNolock( Q, (void**)&addr, minimum_value( max_exec, sz_Q ) );
      ATOMIC_SUB_i32( &completion->length_atomic, n_completed );
    }
  } RELEASE;
  
  if( n_completed == 0 ) {
    return 0;
  }

  // Decrement current dispatch counter
  server->dispatch.n_current -= n_completed;

  // Increment completion counter
  completion->completion_count += n_completed;

  // Process each completed client
  vgx_VGXServerClient_t **pclient = completed;
  vgx_VGXServerClient_t **end = completed + n_completed;
  vgx_VGXServerClient_t *client;
  while( pclient < end ) {
    client = *pclient++;

    vgx_VGXServerRequest_t *matrix_request;
    vgx_VGXServerRequest_t *front_request;
    vgx_HTTPHeaders_t *matrix_headers;
    vgx_HTTPHeaders_t *front_headers;

    switch( client->request.state ) {
    // Forward modified request to dispatcher
    case VGXSERVER_CLIENT_STATE__PREPROCESS:
      if( !client->response.info.execution.complete && DISPATCHER_MATRIX_ENABLED( server ) ) {
        // Executor has been operating on the dispatcher's request object
        matrix_request = client->dispatcher.streams->prequest;
        matrix_headers = matrix_request->headers;
        // Front
        front_request = &client->request;
        front_headers = front_request->headers;
        // Time of forwarding to dispatcher matrix
        front_request->exec_t0_ns = __GET_CURRENT_NANOSECOND_TICK();
        // Swap buffers since preprocessor has been working to fill STREAM, while matrix reads only CONTENT
        iStreamBuffer.Swap( matrix_request->buffers.content, matrix_request->buffers.stream );
        // Capture the signature into the front request object for potential later use
        idcpy( &front_headers->signature, &matrix_headers->signature );
        // Capture user flag for potential later use
        front_headers->flag.__bits = matrix_headers->flag.__bits;
        // Steal capsule
        front_headers->capsule = matrix_headers->capsule;
        memset( matrix_headers->capsule.__bits, 0, sizeof(vgx_HTTPHeadersCapsule_t) );
        // Forward to matrix
        vgx_server_dispatcher_dispatch__forward( server, client );
        break;
      }
      /* FALLTHRU */
    // Request is complete
    case VGXSERVER_CLIENT_STATE__EXECUTE:
    case VGXSERVER_CLIENT_STATE__MERGE:
    case VGXSERVER_CLIENT_STATE__POSTPROCESS:
      __collect( server, client );
      break;
    case VGXSERVER_CLIENT_STATE__RESUBMIT:
      // Executor has been operating on the dispatcher's request object
      matrix_request = client->dispatcher.streams->prequest;
      matrix_headers = matrix_request->headers;
      // Send back to matrix for resubmit
      if( ++(matrix_headers->nresubmit) <= REQUEST_MAX_RESUBMIT ) {
        // Keep front request up to date
        front_request = &client->request;
        front_request->headers->nresubmit = matrix_headers->nresubmit;
        // Swap buffers since postprocessor has been working to fill STREAM, while matrix reads only CONTENT
        iStreamBuffer.Swap( matrix_request->buffers.content, matrix_request->buffers.stream );
        // Forward to matrix
        vgx_server_dispatcher_dispatch__forward( server, client );
      }
      // Resubmit limit reached
      else {
        client->response.info.http_errcode = HTTP_STATUS__LoopDetected;
        __collect( server, client );
      }
      break;
    default:
      CRITICAL( 0x001, "Invalid client state after collect: %08x", client->request.state );
      // Close socket immediately. No response is sent. This is a very bad internal error.
      vgx_server_client__close( server, client );
      continue;
    }
  }

  return n_completed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatch__bypass_to_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  vgx_VGXServerWorkDispatch_t *dispatch = &server->dispatch;

  dispatch->n_total++;

  // 
  vgx_server_dispatcher_dispatch__complete( server, client );

  // Clear the request content but NOT the buffer request
  // since it may have data for the next request already.
  iStreamBuffer.Clear( client->request.buffers.content );

  // Put the client back on the list for poll monitoring and transition state to collected
  // Poll loop may now monitor client's socket and perform I/O again
  CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__COLLECTED );
  vgx_server_client__append_front( server, client );
  
  // Detect error
  if( client->response.info.http_errcode ) {
    vgx_server_response__produce_error( server, client, client->response.info.http_errcode, NULL, false );
  }
  
  // May or may not send anything
  vgx_server_io__front_send( server, client );

  return 0;
}
