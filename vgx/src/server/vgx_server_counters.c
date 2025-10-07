/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_counters.c
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


static double                       __get_duration_by_bucket( int bucket );
static double                       __duration_percentile_short( const vgx_VGXServerPerfCounters_t *counters, float pctX, bool long_window );
static void                         __reset( vgx_VGXServerPerfCounters_t *counters );





/*
* Request duration bucket is determined directly from the
* floating point value's raw bit. We first establish a 
* baseline minimum duration represented by bucket 0.
* We use the floating point bits of this minimum value
* shifted right by a number of bits suitable for getting
* reasonably good resolution in the number of buckets 
* we choose to have, over the range of durations we want
* to sample.
* 
* Example:
*   int n_buckets = 64;
*   double min_duration = 0.001;
*   int fpshift = 50;
* 
*   QWORD fptoraw( double x ) {
*     return *(QWORD*)&x;
*   }
* 
*   QWORD base = fptoraw( min_duration ) >> fpshift;
*   
*   int duration_to_bucket( double x ) {
*     return (int)((fptoraw( x ) >> fpshift) - base);
*   }
* 
*   int64_t bucket_1 = duration_to_bucket( 0.001 ); // 0
*   int64_t bucket_2 = duration_to_bucket( 0.035 ); // 20
*   int64_t bucket_3 = duration_to_bucket( 0.753 ); // 38
*   int64_t bucket_4 = duration_to_bucket( 4.321 ); // 48
*   int64_t bucket_5 = duration_to_bucket( 37.11 ); // 60
* 
* 
*   double bucket_to_duration( int bucket ) {
*     // bucket = raw(x) >> s - base
*     // (bucket + base) << s = raw(x)
* 
*     QWORD raw = (bucket + base) << fpshift;
*     return *(double*)&raw;
* 
*   }
* 
* 
* 
* 
*/


/*******************************************************************//**
 * Return a bucket number between 0 and (max-1) for the request
 * duration given in seconds.
 * 
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_counters__get_bucket_by_duration( double duration ) {
  static double min_duration = REQUEST_DURATION_SAMPLE_BASE;
  QWORD base = *(QWORD*)&min_duration >> REQUEST_DURATION_SAMPLE_FPSHIFT;
  int64_t computed_bucket = (*(QWORD*)&duration >> REQUEST_DURATION_SAMPLE_FPSHIFT) - base;
  int bucket = (int)minimum_value( maximum_value( computed_bucket, 0 ), REQUEST_DURATION_SAMPLE_BUCKETS-1 );
  return bucket;
}



/*******************************************************************//**
 * Return a bucket number between 0 and (max-1) for the request
 * duration given in seconds.
 * 
 * 
 * 
 ***********************************************************************
 */
static double __get_duration_by_bucket( int bucket ) {
  static double min_duration = REQUEST_DURATION_SAMPLE_BASE;
  QWORD base = *(QWORD*)&min_duration >> REQUEST_DURATION_SAMPLE_FPSHIFT;
  QWORD raw = (base + bucket) << REQUEST_DURATION_SAMPLE_FPSHIFT;
  return *(double*)&raw;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static double __duration_percentile_short( const vgx_VGXServerPerfCounters_t *counters, float pctX, bool long_window ) {

  if( pctX < 0.0f ) {
    pctX = 0.0f;
  }
  else if( pctX > 1.0f ) {
    pctX = 1.0f;
  }

  // Compute cumulative counts (non-integer because counts have been diffused by moving average)
  double sum = 0.0;

  if( long_window ) {
    int64_t acc = 0;
    const int64_t *src = counters->duration_long_buckets;
    const int64_t *end = src + REQUEST_DURATION_SAMPLE_BUCKETS;
    while( src < end ) {
      acc += *src++;
    }
    sum = (double)acc;
  }
  else {
    const float *src = counters->duration_short_buckets;
    const float *end = src + REQUEST_DURATION_SAMPLE_BUCKETS;
    while( src < end ) {
      sum += *src++;
    }
  }

  // The percentile cutoff relative to cumulative counts
  double cutoff = sum * pctX;
  double cumulL = 0.0;
  double cumulH = 0.0;
  int bucket = 0;

  // Locate the two buckets right above and below our cutoff
  if( long_window ) {
    int64_t accL = 0;
    int64_t accH = 0;
    const int64_t *src = counters->duration_long_buckets;
    const int64_t *end = src + REQUEST_DURATION_SAMPLE_BUCKETS;
    while( src < end ) {
      int64_t cnt = *src++;
      // This is the upper bucket
      if( (double)(accL + cnt) > cutoff ) {
        accH = accL + cnt;
        break;
      }
      accL += cnt;
      ++bucket;
    }
    cumulL = (double)accL;
    cumulH = (double)accH;
  }
  else {
    const float *src = counters->duration_short_buckets;
    const float *end = src + REQUEST_DURATION_SAMPLE_BUCKETS;
    while( src < end ) {
      float cnt = *src++;
      // This is the upper bucket
      if( cumulL + cnt > cutoff ) {
        cumulH = cumulL + cnt;
        break;
      }
      cumulL += cnt;
      ++bucket;
    }
  }


  // Compute the durations represented by the two buckets
  double durL = __get_duration_by_bucket( bucket - 1 ); // ok if -1
  double durH = __get_duration_by_bucket( bucket ); // ok if max+1

  // Linear interpolation to estimate percentile duration
  // y = a*x + b

  // a = (y2-y1)/(x2-x1)
  double a = (cumulH - cumulL) / (durH - durL);
  // b = y1 - a*x1
  double b = cumulL - a * durL;

  // x = (y - b) / a
  double dur;
  if( a > 0.0 ) {
    dur = (cutoff - b) / a;
  }
  else {
    dur = 0.0;
  }

  return dur;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __clear_client_inspect( vgx_VGXServerClientInspect_t *client_inspect ) {
  if( client_inspect == NULL ) {
    return;
  }

  // Delete uri
  iURI.Delete( &client_inspect->uri );

  // Free path
  free( client_inspect->request.path );

  // Delete inspect channels list
  free( client_inspect->channels );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __init_client_inspect( vgx_VGXServerClientInspect_t *client_inspect, const vgx_VGXServerClient_t *client ) {
  int ret = 0;

  // Count active channels in client
  int n_channels = 0;
  vgx_VGXServerDispatcherChannel_t *channel = client->dispatcher.channels.head;
  while( channel ) {
    ++n_channels;
    channel = channel->chain.next;
  }

  XTRY {
    // Populate inspect client from client
    client_inspect->id = client->id;
    if( (client_inspect->uri = iURI.Clone( client->URI )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    client_inspect->socket = *iURI.Sock.Input.Get( client->URI );
    client_inspect->io_t0_ns = client->io_t0_ns;
    client_inspect->io_t1_ns = client->io_t1_ns;
    client_inspect->delta_t_ns = __GET_CURRENT_NANOSECOND_TICK() - client->io_t0_ns; 
    client_inspect->request.state = client->request.state;
    client_inspect->request.method = client->request.method;
    client_inspect->request.executor_id = client->request.executor_id;
    client_inspect->request.replica_affinity = client->request.replica_affinity;
    client_inspect->request.target_partial = client->request.target_partial;
    if( (client_inspect->request.path = calloc( client->request.sz_path+1, 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    strncpy( client_inspect->request.path, client->request.path, client->request.sz_path );
    client_inspect->request.headers.sz = client->request.headers->sz;
    client_inspect->request.headers.content_length = client->request.headers->content_length;
    idcpy( &client_inspect->request.headers.signature, &client->request.headers->signature );
    client_inspect->request.headers.sn = client->request.headers->sn;
    client_inspect->request.headers.data = client->request.headers->_buffer; // <- careful !
    client_inspect->request.headers._end = client->request.headers->_end;
    client_inspect->request.content_type = client->request.content_type;
    client_inspect->request.accept_type = client->request.accept_type;
    client_inspect->partial_ident = client->partial_ident;
    client_inspect->partial_level = client->dispatch_metas.level;
    client_inspect->n_channels = n_channels;

    // Allocate inspect channel list
    if( (client_inspect->channels = calloc( n_channels, sizeof( vgx_VGXServerChannelInspect_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // Populate inspect channel list from client's channels
    const char *start_sendbuf = NULL;
    vgx_VGXServerDispatcherStreamSet_t *streams = client->dispatcher.streams;
    if( streams ) { // <- should always be set, be safe
      vgx_StreamBuffer_t *stream = streams->prequest->buffers.content;
      iStreamBuffer.ReadableSegment( stream, LLONG_MAX, &start_sendbuf, NULL );
    }

    int i = 0;
    channel = client->dispatcher.channels.head;
    while( channel ) {
      vgx_VGXServerChannelInspect_t *channel_inspect = &client_inspect->channels[i];
      // Copy inspect fields from channel object to inspect object
      channel_inspect->channel = channel->id.channel;
      channel_inspect->replica = channel->id.replica;
      channel_inspect->partition = channel->id.partition;
      channel_inspect->state = channel->state;
      channel_inspect->socket = channel->socket;
      channel_inspect->cost = channel->request.cost;
      channel_inspect->n_requests = channel->request.counter;
      channel_inspect->n_bytes_sent = channel->request.read - start_sendbuf;
      channel_inspect->n_bytes_recv = iStreamBuffer.Size( channel->response->buffers.content );

      // Next channel
      ++i;
      channel = channel->chain.next;
    }

  }
  XCATCH( errcode ) {
    __clear_client_inspect( client_inspect );
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
DLL_HIDDEN void vgx_server_counters__delete_matrix_inspect( vgx_VGXServerMatrixInspect_t **inspect ) {

  if( inspect && *inspect ) {

    if( (*inspect)->clients ) {
      // Clear all inspect clients
      for( int i=0; i<(*inspect)->n_clients; i++ ) {
        __clear_client_inspect( &(*inspect)->clients[i] );
      }

      // Delete inspect clients array
      free( (*inspect)->clients );
    }

    // Delete main inspect object
    free( *inspect );
    *inspect = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerMatrixInspect_t * vgx_server_counters__new_matrix_inspect( vgx_VGXServer_t *server ) {

  vgx_VGXServerMatrixInspect_t *inspect = NULL;

  // Count active clients
  int n_clients = 0;
  const vgx_VGXServerClient_t *start = &server->pool.clients.clients[ CLIENT_FD_START ];
  const vgx_VGXServerClient_t *end = server->pool.clients.clients + server->pool.clients.capacity;
  const vgx_VGXServerClient_t *client = start;
  while( client < end ) {
    if( client->URI ) {
      ++n_clients;
    }
    ++client;
  }

  XTRY {
    // Allocate main inspect struct
    if( (inspect = calloc( 1, sizeof(vgx_VGXServerMatrixInspect_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    inspect->n_clients = n_clients;

    // Allocate client list
    if( (inspect->clients = calloc( n_clients, sizeof( vgx_VGXServerClientInspect_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Initialize each client
    vgx_VGXServerClientInspect_t *inspect_client = inspect->clients;;
    client = start;
    while( client < end ) {
      if( client->URI ) {
        if( __init_client_inspect( inspect_client++, client ) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
        }
      }
      ++client;
    }

  }
  XCATCH( errcode ) {
    vgx_server_counters__delete_matrix_inspect( &inspect );
  }
  XFINALLY {
  }

  return inspect;

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerPerfCounters_t * vgx_server_counters__new( void ) {
  vgx_VGXServerPerfCounters_t *counters = calloc( 1, sizeof( vgx_VGXServerPerfCounters_t ) );
  if( counters == NULL ) {
    return NULL;
  }
  if( (counters->readonly_snapshot = calloc( 1, sizeof( vgx_VGXServerPerfCounters_t ) )) == NULL ) {
    free( counters );
    return NULL;
  }
  return counters;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __reset( vgx_VGXServerPerfCounters_t *counters ) {
  if( counters ) {
    counters->bytes_in = 0;
    counters->bytes_out = 0;
    counters->total_clients = 0;
    counters->request_count_plugin = 0;
    counters->request_count_total = 0;
    counters->duration_total = 0.0;
    counters->average_duration_long = 0.0;
    counters->average_rate_long = 0.0;
    counters->average_duration_short = 0.0;
    counters->average_rate_short = 0.0;
    memset( counters->__duration_sample_buckets, 0, sizeof( int ) * REQUEST_DURATION_SAMPLE_BUCKETS );
    memset( counters->duration_short_buckets, 0, sizeof( float ) * REQUEST_DURATION_SAMPLE_BUCKETS );
    memset( counters->duration_long_buckets, 0, sizeof( int64_t ) * REQUEST_DURATION_SAMPLE_BUCKETS );
    memset( counters->__http_400_527, 0, sizeof( uint16_t ) * 128 );
    counters->error_count.http = 0;
    counters->error_count.service = 0;
    counters->error_rate = 0.0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_counters__delete( vgx_VGXServerPerfCounters_t **counters ) {
  if( counters && *counters ) {
    if( (*counters)->readonly_snapshot ) {
      free( (void*)(*counters)->readonly_snapshot );
    }
    if( (*counters)->inspect ) {

    }
    free( *counters );
    *counters = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN const vgx_VGXServerPerfCounters_t * vgx_server_counters__take_snapshot( vgx_VGXServerPerfCounters_t *counters ) {
  memcpy( (vgx_VGXServerPerfCounters_t*)counters->readonly_snapshot, counters, sizeof( vgx_VGXServerPerfCounters_t ) );
  ((vgx_VGXServerPerfCounters_t*)counters->readonly_snapshot)->readonly_snapshot = NULL;
  return counters->readonly_snapshot;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_counters__measure( vgx_VGXServer_t *server, int64_t now_ns, bool reset ) {
  static __THREAD int64_t measure_t0 = 0;
  static __THREAD double request_duration_0 = 0.0;
  static __THREAD int64_t request_count_n0 = 0;
  static __THREAD int64_t error_count_n0 = 0;

  vgx_VGXServerPerfCounters_t *COUNT = server->counters.perf;

  if( reset ) {
    measure_t0 = now_ns;
    request_count_n0 = 0;
    request_duration_0 = 0;
    error_count_n0 = 0;
    __reset( COUNT );
    return;
  }
  else if( measure_t0 == 0 ) {
    measure_t0 = now_ns;
    return;
  }
  else if( measure_t0 >= now_ns ) {
    measure_t0 = 0;
    return;
  }

  // Uptime
  COUNT->server_uptime_ns = now_ns - COUNT->server_t0;

  COUNT->sz_dispatch = 0;
  for( int i=0; i < DISPATCH_QUEUE_COUNT; ++i ) {
    vgx_VGXServerDispatchQueue_t *job = &server->dispatch.Q[i];
    COUNT->sz_dispatch += ATOMIC_READ_i32( &job->length_atomic );
  }

  COUNT->sz_working = server->dispatch.n_current - COUNT->sz_dispatch;

  // Recent
  double delta_t = (now_ns - measure_t0) / 1e9;
  int64_t request_count_delta = COUNT->request_count_total - request_count_n0;
  double request_duration_delta = COUNT->duration_total - request_duration_0;
  int64_t error_count_delta = COUNT->error_count.http + COUNT->error_count.service - error_count_n0;

  double alpha = 1.0;
  double sample;

  // Recent QPS (and determine alpha for all smoothing)
  sample = request_count_delta / delta_t;
  double H = maximum_value( sample, COUNT->average_rate_short );
  double L = minimum_value( sample, COUNT->average_rate_short );
  if( H > L ) {
    alpha = 1 - L/H;
    if( alpha < 0.1 ) {
      alpha = 0.1;
    }
    else if( alpha > 0.9 ) {
      alpha = 0.9;
    }
  }
  COUNT->average_rate_short = alpha * sample + (1.0-alpha) * COUNT->average_rate_short;

  // Recent average duration
  if( request_count_delta > 0 ) {
    sample = request_duration_delta / request_count_delta;
  }
  else {
    sample = COUNT->average_duration_short;
  }
  COUNT->average_duration_short = alpha * sample + (1.0-alpha) * COUNT->average_duration_short;

  // Recent error rate
  sample = error_count_delta / delta_t;
  COUNT->error_rate = 0.1 * sample + 0.9 * COUNT->error_rate;

  // Aggregate samples buckets into short and long
  int *src = COUNT->__duration_sample_buckets;
  float *dest_short = COUNT->duration_short_buckets;
  int64_t *dest_long = COUNT->duration_long_buckets;
  int *end = src + REQUEST_DURATION_SAMPLE_BUCKETS;
  while( src < end ) {
    // Short buckets are time-averaged floats
    *dest_short = (float)(alpha * (*src) + (1.0-alpha) * (*dest_short));
    dest_short++;
    // Long buckets are aggregated counts
    *dest_long++ += *src;
    // Reset sample bucket
    *src++ = 0;
  }

  // All-time
  double all_delta_t = COUNT->server_uptime_ns / 1e9;
  // All-time QPS
  COUNT->average_rate_long = COUNT->request_count_total / all_delta_t;
  // All-time average duration
  if( COUNT->request_count_total > 0 ) {
    COUNT->average_duration_long = COUNT->duration_total / COUNT->request_count_total;
  }

  // Update references
  measure_t0 = now_ns;
  request_count_n0 = COUNT->request_count_total;
  request_duration_0 = COUNT->duration_total;
  error_count_n0 = COUNT->error_count.http + COUNT->error_count.service;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_counters__inspect_TCS( vgx_VGXServer_t *server ) {

  vgx_VGXServerPerfCounters_t *COUNT = server->counters.perf;
  // Delete any previous inspect object
  if( COUNT->inspect ) {
    vgx_server_counters__delete_matrix_inspect( &COUNT->inspect );
  }

  // Create new inspect object
  COUNT->inspect = vgx_server_counters__new_matrix_inspect( server );

}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_counters__get( vgx_Graph_t *SYSTEM, vgx_VGXServerPerfCounters_t *counters, int timeout_ms ) {
  int ret = 0;
  // We need to lock so we can safely suspend in order to safely get task lock
  GRAPH_LOCK( SYSTEM ) {
    vgx_VGXServer_t *server = SYSTEM->vgxserverA;
    if( server ) {
      int service_in = false;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t t1 = t0;
        int64_t deadline = t0 + timeout_ms;
        COMLIB_TASK_LOCK( server->TASK ) {
          // Request a new snapshot to be performed in main loop
          server->control.flag_TCS.snapshot_request = true;
          // Wait until a snapshot has been built
          while( VGXSERVER_GLOBAL_IS_SERVING( server ) && server->control.flag_TCS.snapshot_request && (t1 = __GET_CURRENT_MILLISECOND_TICK()) < deadline ) {
            COMLIB_TASK_WAIT_FOR_WAKE( server->TASK, 250 );
          }
          // Set back to false to prevent main loop from triggering new snapshot in case we timed out.
          if( server->control.flag_TCS.snapshot_request ) {
            server->control.flag_TCS.snapshot_request = false;
            ret = -1;
          }
          // Capture s-in state
          service_in = server->control.public_service_in_TCS;
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;

      if( ret == 0 && SYSTEM->vgxserverA && SYSTEM->vgxserverA->counters.perf ) {
        memcpy( counters, server->counters.perf->readonly_snapshot, sizeof( vgx_VGXServerPerfCounters_t ) );
        counters->service_in = service_in;
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
DLL_HIDDEN int vgx_server_counters__reset( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  // We need to lock so we can safely suspend in order to safely get task lock
  GRAPH_LOCK( SYSTEM ) {
    vgx_VGXServer_t *server = SYSTEM->vgxserverA;
    if( server ) {
      server->counters.mem_max_process_use = 0;
      server->counters.byte.mem_max_use_pct = 0;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t t1 = t0;
        int64_t deadline = t0 + 10000;
        COMLIB_TASK_LOCK( server->TASK ) {
          // Request counter reset to be performed in main loop
          server->control.flag_TCS.snapshot_request = true;
          server->control.flag_TCS.reset_counters = true;
          // Wait until a snapshot has been built
          while( VGXSERVER_GLOBAL_IS_SERVING( server ) && server->control.flag_TCS.reset_counters && (t1 = __GET_CURRENT_MILLISECOND_TICK()) < deadline ) {
            COMLIB_TASK_WAIT_FOR_WAKE( server->TASK, 250 );
          }

          // Set back to false to prevent main loop from triggering new snapshot in case we timed out.
          if( server->control.flag_TCS.snapshot_request || server->control.flag_TCS.reset_counters ) {
            server->control.flag_TCS.snapshot_request = false;
            server->control.flag_TCS.reset_counters = false;
            ret = -1;
          }

        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_counters__get_latency_percentile( const vgx_VGXServerPerfCounters_t *counters, float pctX, double *pctX_short, double *pctX_long ) {
  if( pctX_short ) {
    *pctX_short = __duration_percentile_short( counters, pctX, false );
  }
  if( pctX_long ) {
    *pctX_long = __duration_percentile_short( counters, pctX, true );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerMatrixInspect_t * vgx_server_counters__inspect_matrix( vgx_Graph_t *SYSTEM, int port_offset, int timeout_ms ) {
  vgx_VGXServerMatrixInspect_t *inspect = NULL;
  // We need to lock so we can safely suspend in order to safely get task lock
  GRAPH_LOCK( SYSTEM ) {
    vgx_VGXServer_t *server = NULL;
    if( port_offset == 0 ) {
      server = SYSTEM->vgxserverA;
    }
    else if( port_offset == 1 ) {
      server = SYSTEM->vgxserverB;
    }
    if( server ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t t1 = t0;
        int64_t deadline = t0 + timeout_ms;
        int ret = 0;
        COMLIB_TASK_LOCK( server->TASK ) {
          // Request inspect to be performed in main loop
          server->control.flag_TCS.inspect_request = true;
          // Wait until a snapshot has been built
          while( VGXSERVER_GLOBAL_IS_SERVING( server ) && server->control.flag_TCS.inspect_request && (t1 = __GET_CURRENT_MILLISECOND_TICK()) < deadline ) {
            COMLIB_TASK_WAIT_FOR_WAKE( server->TASK, 250 );
          }
          // Set back to false to prevent main loop from triggering new snapshot in case we timed out.
          if( server->control.flag_TCS.inspect_request ) {
            server->control.flag_TCS.inspect_request = false;
            ret = -1;
          }
          // Steal the inspect snapshot
          else {
            inspect = server->counters.perf->inspect;
            server->counters.perf->inspect = NULL;
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;

  return inspect;
}
