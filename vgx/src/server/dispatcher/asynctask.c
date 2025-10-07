/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    asynctask.c
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

#include "_vxserver_dispatcher.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static unsigned __asynctask__init( comlib_task_t *self );
static unsigned __asynctask__shutdown( comlib_task_t *self );
DECLARE_COMLIB_TASK( __asynctask__task );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __asynctask__log_error( vgx_VGXServerDispatcherChannel_t *channel, struct pollfd *pfd, int err ) {
  CString_t *CSTR__error = NULL;

  if( err == 0 ) {
    err = iURI.Address.Error( &CSTR__error, -1 );
  }

  const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
  struct addrinfo *addr = channel->parent.permanent.replica->addrinfo_MCS;
  CString_t *CSTR__addr = NULL;
  if( addr ) {
    struct addrinfo *a = cxlowaddr( addr );
    if( a ) {
      const char *hostname = a->ai_canonname ? a->ai_canonname : "?";
      if( a->ai_family == AF_INET ) {
        const char *ip = inet_ntoa( ((struct sockaddr_in*) a->ai_addr)->sin_addr );
        CSTR__addr = CStringNewFormat( "%s (%s)", hostname, ip );
      }
    }
  }

  const char *str_addr = CSTR__addr ? CStringValue( CSTR__addr ) : "?";
  CString_t *CSTR__dispatcher = CStringNewFormat( "%s channel=%d.%d.%d", str_addr, channel->id.partition, channel->id.replica, channel->id.channel );
  const char *s_dispatcher = CSTR__dispatcher ? CStringValue( CSTR__dispatcher ) : "?";

  if( pfd && pfd->revents == POLLHUP ) {
    VGX_SERVER_DISPATCHER_WARNING( 0x001, "Disconnected: %s", s_dispatcher );
  }
  else if( (pfd && pfd->revents == POLLERR) || err == ECONNRESET ) {
    VGX_SERVER_DISPATCHER_WARNING( 0x002, "Disconnected: %s %s", s_dispatcher, serr );
  }
  else if( err == 0 ) {
    VGX_SERVER_DISPATCHER_WARNING( 0x003, "%s %s", s_dispatcher, serr );
  }
  else {
    VGX_SERVER_DISPATCHER_REASON(0x004, "I/O Exception %s %s", s_dispatcher, serr );
  }
  iString.Discard( &CSTR__error );
  iString.Discard( &CSTR__addr );
  iString.Discard( &CSTR__dispatcher );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __asynctask__log_address_error( const vgx_VGXServerDispatcherReplica_t *replica, CString_t *CSTR__error ) {
  const char *serr = __get_error_string( &CSTR__error, "?" );
  const char *host = iURI.Host( replica->remote );
  int port = iURI.PortInt( replica->remote );
  VGX_SERVER_DISPATCHER_WARNING( 0x001, "Failed to resolve (%s,%d): %s", host, port, serr );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __asynctask__resolve_replica( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica ) {
  static char HC_REQUEST[] = "GET /vgx/hc HTTP/1.1" CRLF CRLF;
  static int64_t SZ_HC_REQUEST = sizeof(HC_REQUEST) - 1;
  static char HC_RESPONSE[] = VGX_SERVER_HEADER;
  static int64_t SZ_HC_RESPONSE = sizeof(HC_RESPONSE) - 1;

  CXSOCKET socket = {0};
  cxsocket_invalidate( &socket );
  CXSOCKET *psock = NULL;

  CString_t *CSTR__error = NULL;
  struct addrinfo *addrinfo = NULL;
  vgx_VGXServerResponse_t response = {0};

  bool is_initial = false;
  char ident[64];
  snprintf( ident, 63, "<Partition %d.%d>", replica->id.partition, replica->id.replica );
  const char *uri = NULL;

  XTRY {
    bool is_caught = false;
    bool is_defunct = false;

    // We need the remote URI
    if( replica->remote ) {
      uri = iURI.URI( replica->remote );
    }
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    SYNCHRONIZE_ON( matrix->lock ) {
      if( matrix->flags.enabled ) {
        is_initial = replica->flags_MCS.initial_attempt;
        is_caught = replica->flags_MCS.defunct_caught;
        is_defunct = REPLICA_IS_DEFUNCT_MCS( replica );
        if( is_defunct ) {
          replica->flags_MCS.initial_attempt = false;
          replica->flags_MCS.defunct_caught = true;
          if( replica->addrinfo_MCS ) {
            iURI.DeleteAddrInfo( &replica->addrinfo_MCS );
          }
        }
      }
    } RELEASE;

    if( !is_defunct ) {
      XBREAK;
    }

    if( !is_initial && !is_caught ) {
      VGX_SERVER_DISPATCHER_REASON( 0x003, "Lost connection to %s %s", ident, uri );
    }

    // Try to resolve the address given by replica remote URI
    // This will work even if the dispatcher service is not started
    // This will fail if the host is unreachable
    if( (addrinfo = iURI.NewAddrInfo( replica->remote, &CSTR__error )) == NULL ) {
      __asynctask__log_address_error( replica, CSTR__error );
      XBREAK;
    }

    // Create a new socket (blocking by default)
    psock = &socket;
    if( cxsocket( psock, AF_INET, SOCK_STREAM, IPPROTO_TCP, false, 0, false ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Try to connect (blocking) using last address in list
    struct addrinfo *a = cxlowaddr( addrinfo );
    short revents = 0;
    if( cxconnect( psock, a->ai_addr, a->ai_addrlen, 1000, &revents ) < 1 ) {
      if( !is_caught ) {
        iURI.Sock.Error( replica->remote, &CSTR__error, -1, revents );
        const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
        VGX_SERVER_DISPATCHER_REASON( 0x005, "%s: %s", ident, serr );
      }
      XBREAK;
    }

    // Send hc request
    if( cxsendall( psock, HC_REQUEST, SZ_HC_REQUEST, 4000 ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
    }

    // Wait to receive hc response
    if( vgx_server_util__recvall_sock( psock, &response, 4000 ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }

    // Close HC socket
    cxclose( &psock );
    
    // 200 OK
    switch( response.status.code ) {
    case HTTP_STATUS__OK:
      break;
    case HTTP_STATUS__ServiceUnavailable:
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x008 );
    case HTTP_STATUS__TooManyRequests:
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x009 );
    default:
      VGX_SERVER_DISPATCHER_WARNING( 0x00A, "Unexpected response from %s %s: %d %s", ident, uri, response.status.code, __get_http_response_reason( response.status.code ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
    }
    
    // Expect the Server: header as content
    const char *segment;
    int64_t sz_segment = iStreamBuffer.ReadableSegment( response.buffers.content, 63, &segment, NULL );
    if( sz_segment != SZ_HC_RESPONSE || memcmp( segment, HC_RESPONSE, SZ_HC_RESPONSE ) ) {
      VGX_SERVER_DISPATCHER_REASON( 0x00C, "Unexpected response from %s %s: %lld bytes", ident, uri, sz_segment );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00D );
    }

    // Success - replica no longer defunct
    SYNCHRONIZE_ON( matrix->lock ) {
      // Reset cost to base
      replica->resource.cost = replica->resource.priority.base;
      // Steal address
      replica->addrinfo_MCS = addrinfo;
      addrinfo = NULL;
      REPLICA_CLEAR_DEFUNCT_MCS( replica );
      replica->flags_MCS.defunct_caught = false;
    } RELEASE;

    VGX_SERVER_DISPATCHER_INFO( 0x00E, "Connected: %s %s", ident, uri );
  }
  XCATCH( errcode ) {
    SYNCHRONIZE_ON( matrix->lock ) {
      replica->flags_MCS.defunct_caught = true;
    } RELEASE;

    if( is_initial ) {
      VGX_SERVER_DISPATCHER_WARNING( 0x00F, "Unavailable: %s %s", ident, uri ? uri : "<?>" );
    }

  }
  XFINALLY {
    iString.Discard( &CSTR__error );
    if( addrinfo ) {
      iURI.DeleteAddrInfo( &addrinfo );
    }
    cxclose( &psock );
    iStreamBuffer.Delete( &response.buffers.content );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __asynctask__heal_defunct_replicas( vgx_VGXServer_t *server ) {
  int ret = 0;

  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;
  vgx_VGXServerDispatcherPartition_t *partition, *end_partition;
  vgx_VGXServerDispatcherReplica_t *replica, *end_replica;

  partition = matrix->partition.list;
  end_partition = partition + matrix->partition.width;
  while( partition < end_partition ) {
    replica = partition->replica.list;
    end_replica = replica + partition->replica.height;
    while( replica < end_replica ) {
      // Check for defunct marker in non-locked context (will confirm in locked context)
      if( REPLICA_IS_DEFUNCT_MCS( replica ) ) {
        if( matrix->flags.enabled ) {
          bool defunct = false;
          SYNCHRONIZE_ON( matrix->lock ) {
            // Now MCS, recheck
            if( matrix->flags.enabled ) {
              defunct = REPLICA_IS_DEFUNCT_MCS( replica );
            }
          } RELEASE;
          if( defunct ) {
            __asynctask__resolve_replica( matrix, replica );
          }
          sleep_milliseconds(1);
        }
      }
      ++replica;
    }
    ++partition;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __close_channel_MCS( vgx_VGXServerDispatcherChannel_t *channel ) {
  // Chances are now reasonably good we won't interrupt a new request, since the I/O
  // loop would have to make another iteration to be able to dispatch the channel we
  // are about to close.
  CHANNEL_UPDATE_STATE( channel, VGXSERVER_CHANNEL_STATE__RESET );
  CXSOCKET *psock = &channel->socket;
  cxclose( &psock );
  channel->flag.connected_MCS = false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __asynctask__close_unused_channels( vgx_VGXServer_t *server ) {
  int ret = 0;


  // NOTE: We will be comparing ticks measured by different threads, which differ by some amount.
  //       On the timescales we're deadling with here this difference is small enough to ignore.
  int64_t t1_ns = __GET_CURRENT_NANOSECOND_TICK();

  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;
  vgx_VGXServerDispatcherPartition_t *partition, *end_partition;
  vgx_VGXServerDispatcherReplica_t *replica, *end_replica;
  vgx_VGXServerDispatcherChannel_t *channel, *first_channel;
  int64_t age_ns;

  uint16_t nopen_channels = 0;
  uint16_t nmax_channels = 0;

  // Scan all partitions
  partition = matrix->partition.list;
  end_partition = partition + matrix->partition.width;
  while( partition < end_partition ) {
    // Scan all replicas in each partition
    replica = partition->replica.list;
    end_replica = replica + partition->replica.height;
    while( replica < end_replica ) {
      int64_t max_idle_ns = CHANNEL_MAX_IDLE_SECONDS * 1000000000LL;
      // Scan all channels in each replica IN REVERSE, except first channel
      first_channel = replica->channel_pool.data;
      channel = first_channel + replica->depth - 1;
      while( channel > first_channel ) {
        // Approximate (see note above) time since channel was last assigned to a client request
        // WARNING: We are accessing channel data without mutex. We may risk closing the channel
        //          in the middle of a new request. Attempt to minimize this risk by also looking
        //          at the channel before this channel and require it to be flagged as non-busy.
        age_ns = t1_ns - channel->t0_ns;
        if( age_ns > max_idle_ns && channel->flag.busy == false ) {
          // Connection state quick check without mutex, will be re-checked 
          if( channel->flag.connected_MCS ) {
            SYNCHRONIZE_ON( matrix->lock ) {
              if( channel->flag.connected_MCS ) {
                // Now look at the channel one higher on the stack and confirm its busy flag is false,
                // which means it is (most likely) idle and will be used before our channel in question
                // will be used. Still, no guarantees, only probabilities.
                if( (channel-1)->flag.busy == false ) {
                  // This channel is old, not busy, and has a channel before it that is not busy
                  __close_channel_MCS( channel );
                  max_idle_ns += 250000000LL; // Next channel allowed a little more idle time
                }
              }
            } RELEASE;
            sleep_milliseconds( 1 );
          }
        }
        // Approximate count because we don't lock
        if( channel->flag.connected_MCS ) {
          ++nopen_channels;
        }
        ++nmax_channels;
        --channel;
      }
      // Examine first channel, it has a longer idle threshold
      // First channel allowed to remain idle longer before we close it
      age_ns = t1_ns - channel->t0_ns;
      if( age_ns > 4*max_idle_ns && channel->flag.busy == false ) {
        if( channel->flag.connected_MCS ) {
          SYNCHRONIZE_ON( matrix->lock ) {
            if( channel->flag.connected_MCS ) {
              __close_channel_MCS( channel );
            }
          } RELEASE;
        }
      }
      // Approximate count because we don't lock
      if( channel->flag.connected_MCS ) {
        ++nopen_channels;
      }
      ++nmax_channels;

      ++replica;
    }
    ++partition;
  }

  SYNCHRONIZE_ON( matrix->lock ) {
    matrix->partition.nopen_channels_MCS = nopen_channels;
    matrix->partition.nmax_channels_MCS = nmax_channels;
  } RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __asynctask__remove_temporary_deboost( vgx_VGXServer_t *server ) {
  int ret = 0;

  vgx_VGXServerDispatcherMatrix_t *matrix = &server->matrix;
  vgx_VGXServerDispatcherPartition_t *partition, *end_partition;
  vgx_VGXServerDispatcherReplica_t *replica, *end_replica;

  partition = matrix->partition.list;
  end_partition = partition + matrix->partition.width;
  while( partition < end_partition ) {
    replica = partition->replica.list;
    end_replica = replica + partition->replica.height;
    while( replica < end_replica ) {
      // Check for deboost marker in non-locked context (will confirm in locked context)
      if( REPLICA_IS_TMP_DEBOOST_MCS( replica ) ) {
        if( matrix->flags.enabled ) {
          SYNCHRONIZE_ON( matrix->lock ) {
            // Now MCS, recheck and clear
            if( matrix->flags.enabled && REPLICA_IS_TMP_DEBOOST_MCS( replica ) ) {
              REPLICA_CLEAR_TMP_DEBOOST_MCS( replica );
            }
          } RELEASE;
          sleep_milliseconds(1);
        }
      }
      ++replica;
    }
    ++partition;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __asynctask__perform_subtask( vgx_VGXServerDispatcherAsyncSubtask_t *subtask ) {
  // Now
  int64_t t = __GET_CURRENT_MILLISECOND_TICK();
  // Initialize next deadline to the future
  if( subtask->next < 0 ) {
    subtask->next = t + subtask->interval;
  }
  // Execute subtask if deadline passed
  if( t > subtask->next ) {
    subtask->func( subtask->server );
    sleep_milliseconds(10);
    // Schedule next
    if( subtask->interval > 0 ) {
      while( t > subtask->next ) {
        subtask->next += subtask->interval;
      }
    }
    else {
      subtask->next = t + 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __asynctask__perform_subtasks( vgx_VGXServerDispatcherAsyncSubtask_t **subtasks ) {
  vgx_VGXServerDispatcherAsyncSubtask_t *subtask, **cursor = subtasks;
  while( (subtask = *cursor++) != NULL ) {
    __asynctask__perform_subtask( subtask );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerDispatcherAsyncSubtask_t * __asynctask__new_subtask( vgx_VGXServer_t *server, int (*func)(vgx_VGXServer_t*), int64_t interval ) {
  vgx_VGXServerDispatcherAsyncSubtask_t *subtask = calloc( 1, sizeof( vgx_VGXServerDispatcherAsyncSubtask_t ) );
  if( subtask ) {
    subtask->interval = interval;
    subtask->next = -1;
    subtask->server = server;
    subtask->func = func;
  }
  return subtask;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __asynctask__delete_subtask( vgx_VGXServerDispatcherAsyncSubtask_t **subtask ) {
  if( subtask && *subtask ) {
    free( *subtask );
    *subtask = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_VGXServerDispatcherAsyncTask_t,
                   asynctask,
                   __asynctask__task,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "asynctask/" )
{ 

  vgx_VGXServer_t *server = asynctask->server;

  char buf[16] = {0};
  snprintf( buf, 15, "%c/dispatcher", __ident_letter( server ) );

  APPEND_THREAD_NAME( buf );
  COMLIB_TASK__AppendDescription( self, buf );


  VGX_SERVER_DISPATCHER_VERBOSE( 0x001, "AsyncTask started" );

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 137 );
  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {

    __asynctask__perform_subtasks( asynctask->subtasks );

    // Accept stop request if asynctask has been asked to exit
    if( COMLIB_TASK__IsStopping( self ) ) {
      COMLIB_TASK__AcceptRequest_Stop( self );
      VGX_SERVER_DISPATCHER_VERBOSE( 0x0EE, "AsyncTask stop request accepted" );
    }

  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __asynctask__init( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_VGXServerDispatcherAsyncTask_t *asynctask = COMLIB_TASK__GetData( self );
  vgx_VGXServer_t *server = asynctask->server;

  // ================================================
  // ======== DISPATCHER ASYNCTASK STARTING =========
  // ================================================
  XTRY {
    if( (asynctask->subtasks = calloc( 5, sizeof( vgx_VGXServerDispatcherAsyncSubtask_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x000 );
    }

    // --------------------------------------------------------
    // 1. Close idle channels 
    // --------------------------------------------------------
    int64_t close_interval = 1777;
    if( (asynctask->subtasks[0] = __asynctask__new_subtask( server, __asynctask__close_unused_channels, close_interval )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // --------------------------------------------------------
    // 2. Attempt (re)connect defunct replicas
    // --------------------------------------------------------
    int64_t reconnect_interval = 827;
    if( (asynctask->subtasks[1] = __asynctask__new_subtask( server, __asynctask__heal_defunct_replicas, reconnect_interval )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // --------------------------------------------------------
    // 3. Clear temporary replica deboost
    // --------------------------------------------------------
    int64_t restore_priority_interval = 1409;
    if( (asynctask->subtasks[3] = __asynctask__new_subtask( server, __asynctask__remove_temporary_deboost, restore_priority_interval )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }

    
    VGX_SERVER_DISPATCHER_VERBOSE( 0, "AsyncTask initialized" );
  }
  XCATCH( errcode ) {
    ret = 1; // nonzero = error
    VGX_SERVER_DISPATCHER_REASON( 0, "AsyncTask initialization failed" );
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
static unsigned __asynctask__shutdown( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_VGXServerDispatcherAsyncTask_t *asynctask = COMLIB_TASK__GetData( self );

  // =====================================================
  // ======== DISPATCHER ASYNCTASK SHUTTING DOWN =========
  // =====================================================

  if( asynctask->subtasks ) {
    vgx_VGXServerDispatcherAsyncSubtask_t *subtask, **cursor = asynctask->subtasks;
    while( (subtask = *cursor++) != NULL ) {
      __asynctask__delete_subtask( &subtask );
    }
    free( asynctask->subtasks );
  }


  VGX_SERVER_DISPATCHER_VERBOSE( 0, "AsyncTask shutdown" );
  COMLIB_TASK__ClearState_Busy( self );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherAsyncTask_t * vgx_server_dispatcher_asynctask__new( vgx_VGXServer_t *server ) {

  vgx_VGXServerDispatcherAsyncTask_t *asynctask = NULL;

  XTRY {

    if( (asynctask = calloc( 1, sizeof( vgx_VGXServerDispatcherAsyncTask_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // [Q1.2]
    // Server
    asynctask->server = server;

    // [Q1.1]
    // TASK
    if( (asynctask->TASK = COMLIB_TASK__New( __asynctask__task, __asynctask__init, __asynctask__shutdown, asynctask )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
    }

    int started;
    GRAPH_LOCK( server->sysgraph ) {
      GRAPH_SUSPEND_LOCK( server->sysgraph ) {
        started = COMLIB_TASK__Start( asynctask->TASK, 10000 );
      } GRAPH_RESUME_LOCK;
    } GRAPH_RELEASE;

    if( started != 1 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
    }

  }
  XCATCH( errcode ) {
    vgx_server_dispatcher_asynctask__destroy( server, &asynctask );
  }
  XFINALLY {
  }

  return asynctask;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_asynctask__destroy( vgx_VGXServer_t *server, vgx_VGXServerDispatcherAsyncTask_t **asynctask ) {
  int ret = 0;

  if( asynctask && *asynctask ) {

    vgx_VGXServerDispatcherAsyncTask_t *ASYNCTASK = *asynctask;

    // Start out with assumption that asynctask is stopped
    int stopped = 1;

    // AsyncTask task exists, stop it
    if( ASYNCTASK->TASK ) {
      comlib_task_t *task = ASYNCTASK->TASK;
      int timeout_ms = 30000;
      // asynctask is not stopped, do work to stop it
      stopped = 0;

      GRAPH_LOCK( server->sysgraph ) {
        GRAPH_SUSPEND_LOCK( server->sysgraph ) {

          SYNCHRONIZE_ON( server->matrix.lock ) {
            server->matrix.flags.enabled = false;
          } RELEASE;

          // Shut down
          COMLIB_TASK_LOCK( task ) {
            if( (stopped = COMLIB_TASK__Stop( task, NULL, timeout_ms )) < 0 ) {
              VGX_SERVER_DISPATCHER_WARNING( 0x001, "AsyncTask: Forcing exit" );
              stopped = COMLIB_TASK__ForceExit( task, 30000 );
            }
          } COMLIB_TASK_RELEASE;

          if( stopped != 1 ) {
            VGX_SERVER_DISPATCHER_CRITICAL( 0x002, "Unresponsive asynctask thread" );
            ret = -1;
          }
        } GRAPH_RESUME_LOCK;
      } GRAPH_RELEASE;
    }

    // AsyncTask is stoppped, clean up
    if( stopped == 1 ) {
      COMLIB_TASK__Delete( &ASYNCTASK->TASK );
      free( *asynctask );
      *asynctask = NULL;
    }

  }

  return ret;
}
