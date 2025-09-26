/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_emitter.c
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


static short                    __compute_drain( int64_t total, int64_t remain );
static float                    __drain_as_percentage_TCS( comlib_task_t *task );
static int                      __backoff_CS( vgx_Graph_t *graph );
static void                     __halt_backoff_CS( vgx_Graph_t *graph, int backoff_level );
static int                      __throttle_CS( vgx_Graph_t *graph, int backoff_level );
static bool                     __is_operation_open_after_backoff_CS( vgx_Graph_t *graph, int backoff_level );

static int64_t                  __inc_inflight_CS( vgx_OperationEmitter_t *emitter );
static int64_t                  __dec_inflight_CS( vgx_OperationEmitter_t *emitter );
static int64_t                  __set_inflight_CS( vgx_OperationEmitter_t *emitter, int64_t n );
static int64_t                  __get_inflight_CS( const vgx_OperationEmitter_t *emitter );

static void                     __delete_operator_capture_OPEN( vgx_OperatorCapture_t **capture );
static vgx_OperatorCapture_t *  __new_operator_capture_OPEN( vgx_OperationEmitter_t *emitter );

static void                     __delete_operator_capture_queue_QLCK( CQwordQueue_t **pool );
static CQwordQueue_t *          __new_operator_capture_queue_OPEN( vgx_OperationEmitter_t *emitter, int64_t size );

static void                     __dump_capture_queue_CS( vgx_OperationEmitter_t *emitter, CQwordQueue_t *queue, const char *label, const vgx_OperatorCapture_t *search_capture_obj );

static int64_t                  __return_retired_capture_objects_to_pool_OPEN( vgx_OperationEmitter_t *emitter, CQwordQueue_t *retired );
static void                     __discard_queued_cstrings_CS( CQwordQueue_t *cstring_discard );

static void                     __system_sleep_OPEN( vgx_TransactionalProducers_t *producers, int64_t tms, int sleep_ms, bool log );
static void                     __throttle_commit_OPEN( vgx_TransactionalProducers_t *producers, int64_t tms, int64_t max_pending, int64_t max_bytes );

static int64_t                  __commit_to_system_OPEN( vgx_OperationEmitter_t *emitter, int64_t tms );
static int64_t                  __try_commit_to_system_OPEN( vgx_OperationEmitter_t *emitter, int64_t now_tms );
static int64_t                  __emit_queued_operations_OPEN( vgx_OperationEmitter_t *emitter, int64_t now_tms, int64_t max_commit, CQwordQueue_t *commit_queue, CQwordQueue_t *retired_queue );

static unsigned                 __operation_emitter_initialize( comlib_task_t *self );
static unsigned                 __operation_emitter_shutdown( comlib_task_t *self );





/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "TX::GEN(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define EMITTER_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define EMITTER_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define EMITTER_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define EMITTER_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define EMITTER_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define EMITTER_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )


#define EMITTER_SET_DRAIN_TCS( Emitter, Total, Remain )   ((Emitter)->control.drain_TCS = (Total) > 0 ? __compute_drain( Total, Remain ) : 0 )


#define EMITTER_THROTTLE_DOWN_CS( Graph )         ( --(Graph)->OP.emitter.control.throttle_CS     )
#define EMITTER_THROTTLE_UP_CS( Graph )           ( ++(Graph)->OP.emitter.control.throttle_CS     )
#define EMITTER_IS_THROTTLED_CS( Graph )          (   (Graph)->OP.emitter.control.throttle_CS < 0 )

#define EMITTER_SET_DEFUNCT_CS( Emitter )         ((Emitter)->control.flag_CS.defunct = true)
#define EMITTER_CLEAR_DEFUNCT_CS( Emitter )       ((Emitter)->control.flag_CS.defunct = false)
#define EMITTER_IS_DEFUNCT_CS( Emitter )          ((Emitter)->control.flag_CS.defunct)

#define EMITTER_SET_SUSPENDED_CS( Emitter )       ((Emitter)->control.flag_CS.suspended = true)
#define EMITTER_CLEAR_SUSPENDED_CS( Emitter )     ((Emitter)->control.flag_CS.suspended = false)
#define EMITTER_IS_SUSPENDED_CS( Emitter )        ((Emitter)->control.flag_CS.suspended)

#define EMITTER_DISABLE_CS( Emitter )             ((Emitter)->control.flag_CS.disabled = true)
#define EMITTER_ENABLE_CS( Emitter )              ((Emitter)->control.flag_CS.disabled = false)
#define EMITTER_IS_DISABLED_CS( Emitter )         ((Emitter)->control.flag_CS.disabled)

#define EMITTER_SET_READY_CS( Emitter )           ((Emitter)->control.flag_CS.ready = true)
#define EMITTER_CLEAR_READY_CS( Emitter )         ((Emitter)->control.flag_CS.ready = false)
#define EMITTER_IS_READY_CS( Emitter )            ((Emitter)->control.flag_CS.ready)

#define EMITTER_SET_RUNNING_CS( Emitter )         ((Emitter)->control.flag_CS.running = true)
#define EMITTER_CLEAR_RUNNING_CS( Emitter )       ((Emitter)->control.flag_CS.running = false)
#define EMITTER_IS_RUNNING_CS( Emitter )          ((Emitter)->control.flag_CS.running)

#define EMITTER_SET_OPMUTED_CS( Emitter )         ((Emitter)->control.flag_CS.opmuted = true)
#define EMITTER_CLEAR_OPMUTED_CS( Emitter )       ((Emitter)->control.flag_CS.opmuted = false)
#define EMITTER_IS_OPMUTED_CS( Emitter )          ((Emitter)->control.flag_CS.opmuted)

#define EMITTER_ENABLE_HEARTBEAT_CS( Emitter )    ((Emitter)->control.flag_CS.heartbeat = true)
#define EMITTER_DISABLE_HEARTBEAT_CS( Emitter )   ((Emitter)->control.flag_CS.heartbeat = false)
#define EMITTER_HEARTBEAT_CS( Emitter )           ((Emitter)->control.flag_CS.heartbeat)

#define EMITTER_HEARTBEAT_INTERVAL_MS             15000


/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
#define BEGIN_EMITTER_THROTTLE_CS( Graph )                    \
  BEGIN_DISALLOW_READONLY_CS( &(Graph)->readonly ) {          \
    vgx_Graph_t *__thr_graph__ = Graph;                       \
    EMITTER_THROTTLE_DOWN_CS( __thr_graph__ );                \
    do


#define END_EMITTER_THROTTLE_CS                               \
    WHILE_ZERO;                                               \
    if( EMITTER_THROTTLE_UP_CS( __thr_graph__ ) > 0 ) {       \
      EMITTER_FATAL( NULL, 0x999, "BEYOND MAX THROTTLE!" );   \
    }                                                         \
    if( !EMITTER_IS_THROTTLED_CS( __thr_graph__ ) ) {         \
      SIGNAL_ALL_CONDITION( &(__thr_graph__->OP.emitter.opstream_ready.cond) );  \
    }                                                         \
  } END_DISALLOW_READONLY_CS



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __next_template_TCS( Name, Type )                           \
  __inline static Type * __NEXT_##Name ( CQwordQueue_t *Q ) {       \
    QWORD address = 0;                                              \
    CALLABLE( Q )->NextNolock( Q, &address );                       \
    return (Type*)address; /* NULL if queue was empty */            \
  }



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __append_template( Name, Type )                                   \
  __inline static int __APPEND_##Name( CQwordQueue_t *Q, Type *Name ) {   \
    QWORD address = (QWORD)Name;                                          \
    return CALLABLE( Q )->AppendNolock( Q, &address );                    \
  }



__next_template_TCS( capture, vgx_OperatorCapture_t )
__next_template_TCS( cstring, CString_t )

__append_template( capture, vgx_OperatorCapture_t )
__append_template( cstring, CString_t )


#define COMMIT_QUANT                    0x100       /* Number of capture objects per emit chunk */
#define SZ_CAPTURE_POOL                 0x100000    /* Number of capture objects in pool */
#define CAPTURE_OBJECT_INIT_CAPACITY    8           /* Initial capacity of a new capture object */
#define CAPTURE_OBJECT_MAX_CAPACITY     64          /* Max capacity for a pooled capture object */



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __limit_discard_capture_object_size( vgx_Graph_t *graph, vgx_OperatorCapture_t *capture ) {
  CQwordBuffer_t *B = capture->opdatabuf;
  int64_t zero = ComlibSequenceLength( B );
  if( zero != 0 ) {
#ifdef VGX_CONSISTENCY_CHECK
    FATAL( 0x000, "Non-empty capture object discarded!" );
#endif
    EMITTER_CRITICAL( graph, 0x000, "Discarding non-empty capture object (%lld operators lost)", zero );
  }
  if( CALLABLE( B )->Capacity( B ) > CAPTURE_OBJECT_MAX_CAPACITY ) {
    CALLABLE( B )->Reset( B );
    return 1;
  }
  return 0;
}



#define __INFLIGHT_BACKOFF_THRESHOLD_HIGH ( SZ_CAPTURE_POOL * 0.99 )
#define __INFLIGHT_BACKOFF_THRESHOLD_LOW  ( SZ_CAPTURE_POOL * 0.80 )
#define __INFLIGHT_RECOVER_THRESHOLD      ( SZ_CAPTURE_POOL * 0.40 )
#define __INFLIGHT_HEADROOM               ( SZ_CAPTURE_POOL - (int64_t)__INFLIGHT_BACKOFF_THRESHOLD_HIGH )

static const int64_t INFLIGHT_BACKOFF_THRESHOLD_HIGH  = (int64_t)__INFLIGHT_BACKOFF_THRESHOLD_HIGH;
static const int64_t INFLIGHT_BACKOFF_THRESHOLD_LOW   = (int64_t)__INFLIGHT_BACKOFF_THRESHOLD_LOW;
static const int64_t INFLIGHT_RECOVER_THRESHOLD       = (int64_t)__INFLIGHT_RECOVER_THRESHOLD;
static const int64_t INFLIGHT_HEADROOM                = (int64_t)__INFLIGHT_HEADROOM;





/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static short __compute_drain( int64_t total, int64_t remain ) {
  double r15 = 0x7FFF * remain / (double)total;
  int64_t i15 = (int64_t)r15 & 0x7FFF;
  short drain = (short)i15;
  return drain;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static float __drain_as_percentage_TCS( comlib_task_t *task ) {
  vgx_OperationEmitter_t *emitter = COMLIB_TASK__GetData( task );
  bool progress = true; // TODO: Interrogate the emitter to detect whether progress is being made and expected to complete
  if( emitter && progress ) {
    double pct = 100.0 * (double)emitter->control.drain_TCS / 0x7FFF;
    return (float)pct;
  }
  else {
    return -1.0;
  }
}



/*******************************************************************//**
 * Return a value 0 - 16383 indicating amount of backoff
 *
 ***********************************************************************
 */
#define __RESOLUTION_BITS           40
#define __BACKOFF_STEP_RESOLUTION   14 /* 0 -  */
#define __BACKOFF_MASK              ((1LL << __BACKOFF_STEP_RESOLUTION) - 1)
#define __BACKOFF_SHIFT             (__RESOLUTION_BITS - __BACKOFF_STEP_RESOLUTION)

static const int64_t HEADROOM_FACTOR = (1LL << __RESOLUTION_BITS) / (int64_t)__INFLIGHT_HEADROOM;

__inline static int __backoff_CS( vgx_Graph_t *graph ) {
  static __THREAD int64_t prev_inflight = 0;
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
  int backoff = 0;
  if( emitter->n_inflight_CS > INFLIGHT_BACKOFF_THRESHOLD_LOW ) {
    if( emitter->n_inflight_CS > INFLIGHT_BACKOFF_THRESHOLD_HIGH ) {
      return 16383;
    }
    if( emitter->n_inflight_CS > prev_inflight+1 ) {
      prev_inflight = emitter->n_inflight_CS;
      int64_t n_over = emitter->n_inflight_CS - INFLIGHT_BACKOFF_THRESHOLD_LOW;
      backoff = (int)(((n_over * HEADROOM_FACTOR) >> __BACKOFF_SHIFT ) & __BACKOFF_MASK);
    }
  }
  else {
    prev_inflight = 0;
  }
  return backoff;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __halt_backoff_CS( vgx_Graph_t *graph, int backoff_level ) {
  __assert_state_lock( graph );
  // THROTTLE --------------
  BEGIN_EMITTER_THROTTLE_CS( graph ) {
    if( backoff_level < 10 ) {
      GRAPH_SUSPEND_LOCK( graph ) {
        COMLIB__Spin( backoff_level );
      } GRAPH_RESUME_LOCK;
    }
    else {
      WAIT_FOR_EMITTER_OPSTREAM_READY( graph, 1000 ); // Wait for recovery signal, or max 1 second
    }
  } END_EMITTER_THROTTLE_CS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __throttle_CS( vgx_Graph_t *graph, int backoff_level ) {
  __assert_state_lock( graph );
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  // This graph is overloaded with operations, the emitter can't keep up.
  // We have to back off.
  if( backoff_level > 0 ) {
    comlib_task_t *task = emitter->TASK;
    BEGIN_EMITTER_THROTTLE_CS( graph ) {
      GRAPH_SUSPEND_LOCK( graph ) {
        COMLIB_TASK_LOCK( task ) {
          // We have an emitter
          if( COMLIB_TASK__IsAlive( task ) ) {
            // Can emitter make progress? Only then sleep.
            if( COMLIB_TASK__IsNotSuspended( task ) ) {
              COMLIB_TASK_SUSPEND_LOCK( task ) {
                GRAPH_LOCK( graph ) {
                  __halt_backoff_CS( graph, backoff_level );
                } GRAPH_RELEASE;
              } COMLIB_TASK_RESUME_LOCK;
            }
            // Emitter is suspended, we don't throttle in this case
            else {
              // TODO: Limit capture objects so we don't grow unbounded
            }
          }
          // Emitter is dead, we need to emit synchronously
          else {
            CQwordQueue_t *private_commit = __new_operator_capture_queue_OPEN( emitter, 0 );
            if( private_commit ) {
              CQwordQueue_t *pending;
              COMLIB_TASK_SUSPEND_LOCK( task ) {
                GRAPH_LOCK( graph ) {
                  pending = emitter->commit_pending_CS;
                  emitter->commit_pending_CS = private_commit;
                } GRAPH_RELEASE;
                int64_t tms = _vgx_graph_milliseconds( graph );
                __emit_queued_operations_OPEN( emitter, tms, LLONG_MAX, pending, NULL );
                COMLIB_OBJECT_DESTROY( pending ); 
              } COMLIB_TASK_RESUME_LOCK;
            }
            else {
              EMITTER_CRITICAL( graph, 0x001, "Memory error" );
            }
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    } END_EMITTER_THROTTLE_CS;
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool __is_operation_open_after_backoff_CS( vgx_Graph_t *graph, int backoff_level ) {
  __halt_backoff_CS( graph, backoff_level );
  bool operation_is_open = OPERATION_IS_OPEN( &graph->operation );
  return operation_is_open;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __inc_inflight_CS( vgx_OperationEmitter_t *emitter ) {
  return ++emitter->n_inflight_CS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __dec_inflight_CS( vgx_OperationEmitter_t *emitter ) {
  return --emitter->n_inflight_CS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __set_inflight_CS( vgx_OperationEmitter_t *emitter, int64_t n ) {
  return emitter->n_inflight_CS = n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __get_inflight_CS( const vgx_OperationEmitter_t *emitter ) {
  return emitter->n_inflight_CS;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_operator_capture_OPEN( vgx_OperatorCapture_t **capture ) {
  if( capture && *capture ) {
    
    if( (*capture)->opdatabuf ) {
      COMLIB_OBJECT_DESTROY( (*capture)->opdatabuf );
    }

    ALIGNED_FREE( *capture );
    *capture = NULL;

  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperatorCapture_t * __new_operator_capture_OPEN( vgx_OperationEmitter_t *emitter ) {
  vgx_OperatorCapture_t *capture = NULL;

  XTRY {
    // Allocate
    CALIGNED_MALLOC_THROWS( capture, vgx_OperatorCapture_t, 0x001 );

    // Create operator queue
    CQwordBuffer_constructor_args_t args = {
      .element_capacity = CAPTURE_OBJECT_INIT_CAPACITY
    };
    if( (capture->opdatabuf = COMLIB_OBJECT_NEW( CQwordBuffer_t, NULL, &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Initialize with no opid
    capture->opid = 0;

    // Graph
    capture->graph = emitter->graph;

    // Operation emitter
    capture->emitter = emitter;

    // Inheritable data
    memset( &capture->inheritable, 0, sizeof( vgx_OperationCaptureInheritable_t ) );

  }
  XCATCH( errcode ) {
    __delete_operator_capture_OPEN( &capture );
  }
  XFINALLY {
  }

  return capture;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_operator_capture_queue_QLCK( CQwordQueue_t **queue ) {
  if( queue && *queue ) {
    CQwordQueue_t *Q = *queue;
    vgx_OperatorCapture_t *capture;
    while( (capture = __NEXT_capture( Q )) != NULL ) {
      __delete_operator_capture_OPEN( &capture ); 
    }
    COMLIB_OBJECT_DESTROY( *queue );
    *queue = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CQwordQueue_t * __new_operator_capture_queue_OPEN( vgx_OperationEmitter_t *emitter, int64_t size ) {

  CQwordQueue_t *Q = NULL;

  XTRY {
    vgx_OperatorCapture_t *capture;

    if( (Q = CQwordQueueNew( size )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    int captured;
    for( int64_t n=0; n<size; n++ ) {
      if( (capture = __new_operator_capture_OPEN( emitter )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

      if( (captured = __APPEND_capture( Q, capture )) != 1 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
    }
  }
  XCATCH( errcode ) {
    __delete_operator_capture_queue_QLCK( &Q );
  }
  XFINALLY {
  }

  return Q;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_capture_queue_CS( vgx_OperationEmitter_t *emitter, CQwordQueue_t *pool, const char *label, const vgx_OperatorCapture_t *search_capture_obj ) {
  if( pool ) {
    CALLABLE( pool )->DumpNolock( pool );
    vgx_Graph_t *graph = emitter->graph;
    printf( "CAPTURE QUEUE ANALYSIS( %s / %s ):\n", CALLABLE(graph)->FullPath(graph), label );
    printf( "  _size   = %lld\n", pool->_size );
    printf( "  _buffer = %p\n", pool->_buffer );
    printf( "  _wp     = %p\n", pool->_wp );
    printf( "  _rp     = %p\n", pool->_rp );
    if( pool->_wp < pool->_rp ) {
      printf( "  [  %llu  <wp>  %llu  <rp>  %llu  ]\n", pool->_wp - pool->_buffer, pool->_rp - pool->_wp, (pool->_buffer + pool->_mask + 1) - pool->_rp );
    }
    else {
      printf( "  [  %llu  <rp>  %llu  <wp>  %llu  ]\n", pool->_rp - pool->_buffer, pool->_wp - pool->_rp, (pool->_buffer + pool->_mask + 1) - pool->_wp );
    }

    int i=0;
    QWORD item;
    while( CALLABLE( pool )->GetNolock( pool, i, &item ) == 1 ) {
      vgx_OperatorCapture_t *queued_capture = (vgx_OperatorCapture_t*)item;
      uint64_t offset = (QWORD)(pool->_rp + i) & pool->_mask;
      if( queued_capture == search_capture_obj ) {
        printf( "capture object %p was also in queue '%s' at index %d (offset=%llu)\n", search_capture_obj, label, i, offset );
      }
      if( queued_capture->opdatabuf == search_capture_obj->opdatabuf ) {
        printf( "capture->opdata %p was also in queue '%s' at index %d (offset=%llu)\n", search_capture_obj->opdatabuf, label, i, offset );
      }
      ++i;
    }
    printf( "\n" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __return_retired_capture_objects_to_pool_OPEN( vgx_OperationEmitter_t *emitter, CQwordQueue_t *retired ) {
  int64_t n_inflight = 0;
  int64_t n_retired = ComlibSequenceLength( retired );
  GRAPH_LOCK( emitter->graph ) {
    if( emitter->capture_pool_CS ) {
      int64_t pool_space = SZ_CAPTURE_POOL - ComlibSequenceLength( emitter->capture_pool_CS );
      // Return retired capture objects to pool if there is room in pool
      if( pool_space > 0 ) {
        CALLABLE( emitter->capture_pool_CS )->AbsorbNolock( emitter->capture_pool_CS, retired, minimum_value( n_retired, pool_space ) );
      }
      n_inflight = __set_inflight_CS( emitter, __get_inflight_CS( emitter ) - n_retired );
    }
  } GRAPH_RELEASE;

  // Any remaining retired capture objects after partial absorb are discarded
  int64_t n_remain = ComlibSequenceLength( retired );

  vgx_OperatorCapture_t *capture;
  while( (capture = __NEXT_capture( retired )) != NULL ) {
    __delete_operator_capture_OPEN( &capture );
  }
  if( n_remain > 0 ) {
    EMITTER_VERBOSE( emitter->graph, 0x001, "Discarded %lld non-poolable capture objects", n_remain );
  }

  return n_inflight;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __discard_queued_cstrings_CS( CQwordQueue_t *cstring_discard ) {
  if( cstring_discard ) {
    CString_t *CSTR__str;
    while( (CSTR__str = __NEXT_cstring( cstring_discard )) != NULL ) {
      if( CSTR__str->allocator_context ) {
        icstringobject.DecrefNolock( CSTR__str );
      }
      else {
        iString.Discard( &CSTR__str );
      }
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __system_sleep_OPEN( vgx_TransactionalProducers_t *producers, int64_t tms, int sleep_ms, bool log ) {
  if( sleep_ms > 0 ) {
    // Max sleep
    if( sleep_ms > 5000 ) {
      sleep_ms = 5000; // max 5 seconds
    }

    // Log info
    if( log ) {
      vgx_TransactionalProducersIterator_t iter = {0};
      if( iTransactional.Producers.Iterator.Init( producers, &iter ) ) {
        vgx_TransactionalProducer_t *producer;
        while( (producer = iTransactional.Producers.Iterator.Next( &iter ) ) != NULL ) {
          BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
            int64_t latency_ms, timespan_ms, n_pending, n_bytes;
            n_pending = iTransactional.Producer.Length( producer );
            n_bytes = iTransactional.Producer.Bytes( producer );
            timespan_ms = iTransactional.Producer.Timespan( producer );
            latency_ms = iTransactional.Producer.QueueLatency( producer, tms );
          } END_TRANSACTIONAL_IF_ATTACHED;
        }
        iTransactional.Producers.Iterator.Clear( &iter );
      }
    }

    //
    // SLEEP
    //
    sleep_milliseconds( sleep_ms );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __throttle_commit_OPEN( vgx_TransactionalProducers_t *producers, int64_t tms, int64_t max_pending, int64_t max_bytes ) {
  static int minlog = 1;
  static int logcnt = 0;

  int sleep_ms;

  // throttle if buffer too large (1ms per 1MiB over limit)
  if( max_bytes > THROTTLE_SYSOUT ) {
    sleep_ms = (int)((max_bytes - THROTTLE_SYSOUT) / (1<<20));
  }
  // throttle if too many pending transactions (1ms per 1 transaction over limit)
  else if( max_pending > THROTTLE_TRANSACTION ) {
    sleep_ms = (int)(max_pending - THROTTLE_TRANSACTION);
  }
  else {
    minlog = 1;
    logcnt = 0;
    sleep_ms = 0;
  }

  if( sleep_ms > 0 ) {
    bool log = sleep_ms > minlog;
    minlog *= 2;
    __system_sleep_OPEN( producers, tms, sleep_ms, log );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __commit_to_system_OPEN( vgx_OperationEmitter_t *emitter, int64_t tms ) {
  int64_t n_commit = 0;
  int err = 0;

  // Commit emitter buffer to system output if requested and buffer has data
  vgx_OperationBuffer_t *buffer = emitter->op_buffer;
  if( (n_commit = iOpBuffer.Readable( buffer )) > 0 ) {
    vgx_Graph_t *graph = emitter->graph;
    // System graph is agent
    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
    int64_t max_pending = 0;
    int64_t max_bytes = 0;
    int64_t tx_bytes = 0;
    // Commit contents of operation buffer into all producers
    vgx_TransactionalProducersIterator_t iter = {0};
    if( iTransactional.Producers.Iterator.Init( agentsys->producers, &iter ) ) {
      int64_t master_serial;
      GRAPH_LOCK( SYSTEM ) {
        // No tx input service, we generate master serial here
        if( SYSTEM->OP.system.consumer == NULL ) {
          SYSTEM->sysmaster_tx_serial = ++(SYSTEM->sysmaster_tx_serial_count) + SYSTEM->sysmaster_tx_serial_0;
        }
        // Master serial will be used by all producers
        master_serial = SYSTEM->sysmaster_tx_serial;
      } GRAPH_RELEASE;
      bool VALIDATE = false; // Validation is bad to perform within a transactional lock.
      vgx_TransactionalProducer_t *producer;
      while( (producer = iTransactional.Producers.Iterator.Next( &iter ) ) != NULL ) {
        objectid_t tx_id = {0};
        int64_t tx_serial = -1;

        BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
          // 
          int64_t n;
          // Commit buffer to producer
          if( (n = iTransactional.Producer.Commit( producer, buffer, tms, VALIDATE, master_serial, &tx_id, &tx_serial )) < 0 ) {
            const char *uri = iURI.URI( producer->URI );
            EMITTER_CRITICAL( emitter->graph, 0x001, "[URI=%s] Output backlog full. This subscriber will be disconnected.", uri );
            int64_t n_purged = iTransactional.Producer.Abandon( producer );
            EMITTER_INFO( emitter->graph, 0x002, "Tried to commit %lld bytes, purged %lld bytes before disconnect.", n_commit, n_purged );
            --err;
          }
          // Record the transaction size
          else if( n > tx_bytes ) {
            tx_bytes = n;
          }

          // Record the max number of pending transactions
          int64_t n_pending = iTransactional.Producer.Length( producer );
          if( n_pending > max_pending ) {
            max_pending = n_pending;
          }
          // Record the max number of pending bytes
          int64_t n_bytes = iTransactional.Producer.Bytes( producer );
          if( n_bytes > max_bytes ) {
            max_bytes = n_bytes;
          }

          // Turn off validation for all but the first producer
          VALIDATE = false;

        } END_TRANSACTIONAL_IF_ATTACHED;

        if( tx_serial >=0 ) {
          GRAPH_LOCK( graph ) {
            idcpy( &graph->tx_id_out, &tx_id );
            graph->tx_serial_out = tx_serial;
            graph->tx_count_out++;
          } GRAPH_RELEASE;
        }

      }
      iTransactional.Producers.Iterator.Clear( &iter );
    } 

    // Clear operation buffer now that is has been committed to producer(s)
    iOpBuffer.Clear( buffer );

    if( err < 0 ) {
      n_commit = -1;
    }
    else {
      // Record stats
      GRAPH_LOCK( agentsys->graph ) {
        agentsys->out_counters_CS.n_transactions++;
        agentsys->out_counters_CS.n_operations += emitter->n_uncommitted.operations;
        agentsys->out_counters_CS.n_opcodes += emitter->n_uncommitted.opcodes;
        agentsys->out_counters_CS.n_bytes += tx_bytes;
      } GRAPH_RELEASE;

      // Reset
      emitter->n_uncommitted.operations = 0;
      emitter->n_uncommitted.opcodes = 0;

      // Enforce output limits
      __throttle_commit_OPEN( agentsys->producers, tms, max_pending, max_bytes );
    }

  }


  return n_commit;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __try_commit_to_system_OPEN( vgx_OperationEmitter_t *emitter, int64_t now_tms ) {
  int64_t n_bytes_committed = 0;

  int64_t sz_output = iOpBuffer.Readable( emitter->op_buffer );
  int64_t n_lock = emitter->lxw_balance;

  // Graph is stable when vertex lock count is zero
  // NOTE: We may be forced to commit even when lock count is non-zero due to transaction size limit.
  //       This situation is a symptom of inappropriate use of the API where vertices are held locked
  //       for extended periods of time with many operations performed.
  if( n_lock == 0 || sz_output > TX_FLUSH_LIM )  {

    // Commit operations to system when buffer size threshold is reached
    // or when buffer exceeds age limit
    if( sz_output > TX_COMMIT_SIZE_LIMIT || now_tms >= emitter->commit_deadline_ms ) {

      // Forced commit. This will result in vertex locks being held longer on the consumer side.
      // If transaction output is suspended or delayed after this commit vertex locks will be held
      // indefinitely on the consumer side, leading to a system that cannot respond to queries.
      if( n_lock > 0 ) {
        EMITTER_WARNING( emitter->graph, 0x001, "Forcing transaction commit due to size (%lld) while %lld vertex lock(s) held", sz_output, n_lock );
      }

      if( __commit_to_system_OPEN( emitter, now_tms ) < 0 ) {
        EMITTER_CRITICAL( emitter->graph, 0x002, "Commit to system failed (%lld bytes)", sz_output );
        n_bytes_committed = -1;
      }
      // Successful commit will consume entire buffer
      else {
        n_bytes_committed = sz_output;
        // Advance the last committed opid record to the last emitted opid, since 
        // we just committed everything that had been emitted.
        GRAPH_LOCK( emitter->graph ) {
          emitter->opid_last_commit_CS = emitter->opid_last_emit;
        } GRAPH_RELEASE;
      }
      // Update next deadline
      emitter->commit_deadline_ms = now_tms + TX_COMMIT_AGE_LIMIT;
    }
  }
  // Positive lock balance, we cannot commit to system with the emitter in an excited state
  else if( n_lock > 0 ) {
    // However, we must record the advancement of the commit opid to allow
    // operation fence to be unblocked.
    // NOTE: The operation is technically not committed, but we have no choice but to record it as such.
    // TODO: Improve this.
    GRAPH_LOCK( emitter->graph ) {
      emitter->opid_last_commit_CS = emitter->opid_last_emit;
    } GRAPH_RELEASE;
  }
  // Negative lock count is fatal error
  else {
    EMITTER_FATAL( emitter->graph, 0x003, "Negative vertex lock balance: %lld", n_lock );
    emitter->lxw_balance = 0;
  }
  return n_bytes_committed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __emit_queued_operations_OPEN( vgx_OperationEmitter_t *emitter, int64_t now_tms, int64_t max_commit, CQwordQueue_t *commit_queue, CQwordQueue_t *retired_queue ) {
  int64_t n = 0;
  vgx_OperatorCapture_t *capture;
  vgx_Graph_t *graph = emitter->graph;
  while( n < max_commit && (capture = __NEXT_capture( commit_queue )) != NULL ) {
    // Fully emit the capture buffer
    while( ComlibSequenceLength( capture->opdatabuf ) ) {
      // EMIT
      if( _vxdurable_operation_produce_op__emit_OPEN( emitter, now_tms, capture ) < 0 ) {
        EMITTER_CRITICAL( graph, 0x001, "Emit operation failed" );
      }

      // Commit buffer to system if appropriate
      __try_commit_to_system_OPEN( emitter, now_tms );
    }

    // Record the opid of the most recently emitted operation
    emitter->opid_last_emit = capture->opid;

    // RETIRE capture object
    if( retired_queue ) {
      __limit_discard_capture_object_size( graph, capture );
      __APPEND_capture( retired_queue, capture );
    }
    else {
      _vxdurable_operation_emitter__return_operator_capture_to_pool_OPEN( &capture );
    }
    ++n;
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_emitter_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_OperationEmitter_t *emitter = COMLIB_TASK__GetData( self );

  COMLIB_TASK_LOCK( self ) {
    COMLIB_TASK_SUSPEND_LOCK( self ) {
      GRAPH_LOCK( emitter->graph ) {

        CString_t *CSTR__bufname = NULL;

        XTRY {

          if( (CSTR__bufname = CStringNewFormat( "op_buffer/%s", CALLABLE( emitter->graph )->FullPath( emitter->graph ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
          }

          // [Q1.5]
          if( (emitter->op_buffer = iOpBuffer.New( 8, CStringValue( CSTR__bufname ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
          }

          // [Q2.1] Private commit queue
          if( (emitter->private_commit = __new_operator_capture_queue_OPEN( emitter, 0 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
          }

          // [Q2.2] Private retire queue
          if( (emitter->private_retire = __new_operator_capture_queue_OPEN( emitter, 0 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
          }

          // [Q2.3] operator capture pool
          if( (emitter->capture_pool_CS = __new_operator_capture_queue_OPEN( emitter, SZ_CAPTURE_POOL )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
          }

          // [Q2.4] commit queue
          if( (emitter->commit_pending_CS = __new_operator_capture_queue_OPEN( emitter, 0 )) == NULL ) { // <== commit queue starts out empty of course
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
          }

          // [Q2.5] commit temp swap
          if( (emitter->commit_swap = __new_operator_capture_queue_OPEN( emitter, 0 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x007 );
          }

          // [Q2.6] cstring discard queue
          if( (emitter->cstring_discard = CQwordQueueNew( 4 )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x008 );
          }

        }
        XCATCH( errcode ) {
          iOpBuffer.Delete( &emitter->op_buffer );
          __delete_operator_capture_queue_QLCK( &emitter->private_commit );
          __delete_operator_capture_queue_QLCK( &emitter->private_retire );
          __delete_operator_capture_queue_QLCK( &emitter->capture_pool_CS );
          __delete_operator_capture_queue_QLCK( &emitter->commit_pending_CS );
          __delete_operator_capture_queue_QLCK( &emitter->commit_swap );
          if( emitter->cstring_discard ) {
            COMLIB_OBJECT_DESTROY( emitter->cstring_discard );
            emitter->cstring_discard = NULL;
          }
          ret = 1; // error
        }
        XFINALLY {
          iString.Discard( &CSTR__bufname );
        }
      } GRAPH_RELEASE;
    } COMLIB_TASK_RESUME_LOCK;
  } COMLIB_TASK_RELEASE;


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_emitter_shutdown( comlib_task_t *self ) {
  vgx_OperationEmitter_t *emitter = COMLIB_TASK__GetData( self );
  vgx_Graph_t *graph = emitter->graph;

  // =================================================
  // ======= OPERATION EMITTER SHUTTING DOWN =======
  // =================================================
  COMLIB_TASK__ClearState_Busy( self );
  int64_t n;

  GRAPH_LOCK( graph ) {
    // Execute all remaining cstring discards
    __discard_queued_cstrings_CS( emitter->cstring_discard );

    // Assert no inflight operations
    if( __get_inflight_CS( emitter ) != 0 ) {
      EMITTER_CRITICAL( graph, 0x001, "%lld inflight operations on shutdown", __get_inflight_CS( emitter ) );
    }

    // [Q2.3] Delete capture pool
    __delete_operator_capture_queue_QLCK( &emitter->capture_pool_CS );

    // [Q2.4] Delete commit queue
    if( (n = ComlibSequenceLength( emitter->commit_pending_CS )) > 0 ) {
      EMITTER_CRITICAL( graph, 0x002, "Lost %lld commits (commit_pending)", n );
    }
    __delete_operator_capture_queue_QLCK( &emitter->commit_pending_CS );

  } GRAPH_RELEASE;

  COMLIB_TASK_LOCK( self ) {
    
    // [Q2.1] Delete private commit queue
    if( (n = ComlibSequenceLength( emitter->private_commit )) > 0 ) {
      EMITTER_CRITICAL( graph, 0x003, "Lost %lld commits (private_commit)", n );
    }
    __delete_operator_capture_queue_QLCK( &emitter->private_commit );

    // [Q2.5] Delete commit queue
    if( (n = ComlibSequenceLength( emitter->commit_swap )) > 0 ) {
      EMITTER_CRITICAL( graph, 0x004, "Lost %lld commits (commit_swap)", n );
    }
    __delete_operator_capture_queue_QLCK( &emitter->commit_swap );

    // [Q2.2] Delete private retire queue
    if( (n = ComlibSequenceLength( emitter->private_retire )) > 0 ) {
      EMITTER_CRITICAL( graph, 0x005, "Leaked %lld commits (private_retire)", n );
    }
    __delete_operator_capture_queue_QLCK( &emitter->private_retire );
    
    // [Q]
    if( (n = iOpBuffer.Readable( emitter->op_buffer )) > 0 ) {
      EMITTER_CRITICAL( graph, 0x006, "Lost %lld uncommitted bytes (op_buffer)", n );
    }
    iOpBuffer.Delete( &emitter->op_buffer );

    // [Q2.6] Delete cstring discard queue
    if( emitter->cstring_discard ) {
      COMLIB_OBJECT_DESTROY( emitter->cstring_discard );
      emitter->cstring_discard = NULL;
    }

  } COMLIB_TASK_RELEASE;

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_OperationEmitter_t,
                   emitter,
                   __operation_emitter_monitor,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "operation_emitter_monitor/" )
{
  vgx_Graph_t *graph = emitter->graph;
  const char *graph_name = CStringValue( CALLABLE( graph )->Name( graph ) );
  APPEND_THREAD_NAME( graph_name );
  COMLIB_TASK__AppendDescription( self, graph_name );

  bool suspend_request = false;
  bool suspended = false;

  // ------------------------
  // Use default CString serialization
  CStringSetSerializationCurrentThread( NULL, NULL );

  // Tick interval when no other activity
  int idle_interval_ms = 4000;

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
  int64_t tms = 0;
  int64_t next_tick_deadline_tms = 0;
  int64_t n_pending = 0;
  int64_t n_commit_pending = 0;
  bool throttled = false;
  int running = false;

  GRAPH_LOCK( graph ) {
    throttled = EMITTER_IS_THROTTLED_CS( graph );
    n_commit_pending = ComlibSequenceLength( emitter->commit_pending_CS );
    tms = _vgx_graph_milliseconds( graph );
    emitter->commit_deadline_ms = tms + TX_COMMIT_AGE_LIMIT;
    if( iSystem.IsSystemGraph( graph ) ) {
      EMITTER_ENABLE_HEARTBEAT_CS( emitter );
    }
    // Mark emitter as ready and running
    EMITTER_SET_READY_CS( emitter );
    EMITTER_SET_RUNNING_CS( emitter );

    // Open graph operation
    if( OPEN_GRAPH_OPERATION_CS( graph ) < 0 ) {
      EMITTER_CRITICAL( graph, 0x000, "Failed to open graph operation" );
      // Mark emitter as ready and running
      EMITTER_CLEAR_READY_CS( emitter );
      EMITTER_CLEAR_RUNNING_CS( emitter );
    }
    else {
      running = true;
    }
  } GRAPH_RELEASE;

  // Emitter is running, make sure graph operation is open
  if( running ) {
    BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
      
      bool inactive = false;

      GRAPH_LOCK( graph ) {
        throttled = EMITTER_IS_THROTTLED_CS( graph );
        n_commit_pending = ComlibSequenceLength( emitter->commit_pending_CS );
        emitter->sz_private_commit_CS = ComlibSequenceLength( emitter->private_commit );
      } GRAPH_RELEASE;

      tms = _vgx_graph_milliseconds( graph );

      COMLIB_TASK_LOCK( self ) {
        n_pending = ComlibSequenceLength( emitter->private_commit ) + n_commit_pending;

        // Stop requested?
        if( COMLIB_TASK__IsRequested_Stop( self ) ){
          COMLIB_TASK__AcceptRequest_Stop( self );
          COMLIB_TASK__SetState_Stopping( self );
          suspended = false;
          suspend_request = false;
        }
        else {
          // Suspend requested?
          if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
            COMLIB_TASK__AcceptRequest_Suspend( self );
            EMITTER_VERBOSE( graph, 0x001, "Suspend requested" );
            if( !suspended ) {
              COMLIB_TASK__SetState_Suspending( self );
              suspend_request = true;
            }
            else {
              EMITTER_VERBOSE( graph, 0x002, "Suspend requested (already suspended)" );
            }
          }
          // Resume requested ?
          if( COMLIB_TASK__IsRequested_Resume( self ) ) {
            COMLIB_TASK__AcceptRequest_Resume( self );
            COMLIB_TASK__ClearState_Suspending( self );
            COMLIB_TASK__ClearState_Suspended( self );
            suspended = false;
            suspend_request = false;
            COMLIB_TASK_SUSPEND_LOCK( self ) {
              GRAPH_LOCK( graph ) {
                EMITTER_CLEAR_SUSPENDED_CS( emitter );
                EMITTER_SET_READY_CS( emitter );
              } GRAPH_RELEASE;
            } COMLIB_TASK_RESUME_LOCK;
            EMITTER_VERBOSE( graph, 0x003, "Resumed" );
          }
        }

        // TODO: Implement proper signals to wake when commits are pending,
        //       instead of this polling
        //
        if( n_pending == 0 ) {

          COMLIB_TASK__ClearState_Busy( self );
          COMLIB_TASK__SignalIdle( self );

          // Don't wait if throttled - it means we need to work as fast as possible to consume queues so feeders may resume asap
          bool timeout = false;
          bool wait = throttled ? false : true;
          BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotStopping( self ) && COMLIB_TASK__IsNotSuspending( self ) && wait, idle_interval_ms, &timeout ) {
            // Short sleep
            COMLIB_TASK_IDLE_SLEEP( self, 10 );
            COMLIB_TASK_SUSPEND_LOCK( self ) {
              // Refresh
              GRAPH_LOCK( graph ) {
                n_commit_pending = ComlibSequenceLength( emitter->commit_pending_CS );
                if( emitter->control.flag_CS.flush ) {
                  emitter->control.flag_CS.flush = false;
                  wait = false;
                  emitter->commit_deadline_ms = 0;
                  SIGNAL_WAKE_EVENT( graph );
                }
              } GRAPH_RELEASE;
              tms = _vgx_graph_milliseconds( graph );
              // Commit buffer data to system if age limit reached
              if( tms >= emitter->commit_deadline_ms ) {
                __try_commit_to_system_OPEN( emitter, tms );
              }
            } COMLIB_TASK_RESUME_LOCK;

            // Exit wait loop if we have new data or the emitter run state is changing
            n_pending = ComlibSequenceLength( emitter->private_commit ) + n_commit_pending;

            if( n_pending > 0 || COMLIB_TASK__IsStateChangeRequested( self ) ) {
              wait = false;
            }

          } END_TIME_LIMITED_WHILE;

          // Loop exited due to inactivity
          if( timeout ) {
            inactive = true;
          }
        }

        if( n_pending > 0 ) {
          COMLIB_TASK__SetState_Busy( self );
        }

        if( !suspended ) {

          // Sanity check: the swap must be empty here.
          if( ComlibSequenceLength( emitter->commit_swap ) != 0 ) {
            EMITTER_CRITICAL( graph, 0x004, "Lost %lld commits (commit_swap)", ComlibSequenceLength( emitter->commit_swap ) );
            CALLABLE( emitter->commit_swap )->DiscardNolock( emitter->commit_swap, -1 );
          }
      
          COMLIB_TASK_SUSPEND_LOCK( self ) {
            tms = _vgx_graph_milliseconds( graph );
            GRAPH_LOCK( graph ) {
              
              // System graph's emitter sends tick when no other activity and no transaction input service is running
              if( inactive && EMITTER_HEARTBEAT_CS( emitter ) && iSystem.IsSystemGraph(graph) && tms > next_tick_deadline_tms ) {
                next_tick_deadline_tms = tms + EMITTER_HEARTBEAT_INTERVAL_MS;
                vgx_Graph_t *SYSTEM_CS = graph;
                bool synchronizing = SYSTEM_CS->OP.system.state_CS.flags.sync_in_progress;
                bool writable = _vgx_is_writable_CS( &SYSTEM_CS->readonly );
                bool bound = iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM_CS ) > 0;
                if( synchronizing == false && writable == true && bound == false ) {
                  iOperation.System_SYS_CS.Tick( SYSTEM_CS, tms );
                }
              }
              
              // Swap the public pending queue that is CS protected
              CQwordQueue_t *pending = emitter->commit_pending_CS;
              emitter->commit_pending_CS = emitter->commit_swap; // This is empty and ready for new commits
              emitter->commit_swap = pending; // This is now safe in the emitter and will be absorbed below
              emitter->sz_private_commit_CS = ComlibSequenceLength( emitter->private_commit ) + ComlibSequenceLength( emitter->commit_swap );
            } GRAPH_RELEASE;
          } COMLIB_TASK_RESUME_LOCK;

          // Transfer capture objects from emitter into private queue
          // Private queue is empty, we do a quick swap
          if( ComlibSequenceLength( emitter->private_commit ) == 0 ) {
            CQwordQueue_t *swap = emitter->commit_swap;
            emitter->commit_swap = emitter->private_commit; // This is empty
            emitter->private_commit = swap; // This is now the pending queue with fresh capture objects from the outside commit pending
          }
          // Private queue is populated (because we didn't fully consume it), we transfer data into it from commit queue
          else {
            int64_t n_transfer = ComlibSequenceLength( emitter->commit_swap );
            CALLABLE( emitter->private_commit )->AbsorbNolock( emitter->private_commit, emitter->commit_swap, n_transfer );
          }

          // Emit everything in commit queue
          // Emit operations in chunks
          int n_chunks = 0;
          int64_t n_emitted = 0;
          int64_t n_commit = 0;
          int64_t n_remain = n_commit = ComlibSequenceLength( emitter->private_commit );
          EMITTER_SET_DRAIN_TCS( emitter, n_commit, n_remain );

          int64_t n;
          COMLIB_TASK_SUSPEND_LOCK( self ) {
            while( (n = __emit_queued_operations_OPEN( emitter, tms, COMMIT_QUANT, emitter->private_commit, emitter->private_retire )) > 0 ) {
              n_emitted += n;
              ++n_chunks;
              int64_t n_inflight = __return_retired_capture_objects_to_pool_OPEN( emitter, emitter->private_retire );

              GRAPH_LOCK( graph ) {
                emitter->sz_private_commit_CS = ComlibSequenceLength( emitter->private_commit );
                __discard_queued_cstrings_CS( emitter->cstring_discard );
                // Saturation recovery, wake up throttled thread(s)
                if( EMITTER_IS_THROTTLED_CS( graph ) && n_inflight < INFLIGHT_RECOVER_THRESHOLD ) {
                  SIGNAL_ALL_CONDITION( &(graph->OP.emitter.opstream_ready.cond) );
                }
              } GRAPH_RELEASE;

              COMLIB_TASK_LOCK( self ) {
                n_remain = ComlibSequenceLength( emitter->private_commit );
                EMITTER_SET_DRAIN_TCS( emitter, n_commit, n_remain );
              } COMLIB_TASK_RELEASE;
            }

            // Commit before suspend
            if( suspend_request ) {
              emitter->commit_deadline_ms = tms;
              __try_commit_to_system_OPEN( emitter, tms );
            }

          } COMLIB_TASK_RESUME_LOCK;

          if( suspend_request ) {
            COMLIB_TASK__ClearState_Busy( self );
            suspend_request = false;
            // Enter suspended state only if the suspending state is still in effect
            if( COMLIB_TASK__IsSuspending( self ) ) {
              EMITTER_VERBOSE( graph, 0x005, "Suspended" );
              if( emitter->lxw_balance != 0 ) {
                int64_t sz_output_remain = iOpBuffer.Readable( emitter->op_buffer );
                if( sz_output_remain > 0 ) {
                  EMITTER_REASON( graph, 0x006, "Vertex lock balance at %lld at time of suspend. Pending operations (%lld bytes) cannot be committed.", emitter->lxw_balance, sz_output_remain );
                }
                COMLIB_TASK_SUSPEND_LOCK( self ) {
                  GRAPH_LOCK( graph ) {
                    // Reset lock balance counter if it has gotten out of sync for some reason
                    if( _vgx_graph_get_vertex_WL_count_CS( graph ) == 0 ) {
                      EMITTER_REASON( graph, 0x007, "Emitter lxw_balance=%lld with zero writable vertices in graph. Forcing lxw_balance=0.", emitter->lxw_balance );
                      emitter->lxw_balance = 0;
                    }
                  } GRAPH_RELEASE;
                } COMLIB_TASK_RESUME_LOCK;
              }
              COMLIB_TASK__SetState_Suspended( self );
              COMLIB_TASK__ClearState_Suspending( self );
              suspended = true;
              COMLIB_TASK_SUSPEND_LOCK( self ) {
                GRAPH_LOCK( graph ) {
                  EMITTER_SET_SUSPENDED_CS( emitter );
                  EMITTER_CLEAR_READY_CS( emitter );
                } GRAPH_RELEASE;
              } COMLIB_TASK_RESUME_LOCK;

              // CLEANUP
              iOpBuffer.Trim( emitter->op_buffer, 512<<10 );
              CALLABLE( emitter->private_commit )->OptimizeNolock( emitter->private_commit );
              CALLABLE( emitter->private_retire )->OptimizeNolock( emitter->private_retire );
              CALLABLE( emitter->commit_swap )->OptimizeNolock( emitter->commit_swap );
              CALLABLE( emitter->cstring_discard )->OptimizeNolock( emitter->cstring_discard );
              COMLIB_TASK_SUSPEND_LOCK( self ) {
                GRAPH_LOCK( graph ) {
                  CALLABLE( emitter->commit_pending_CS )->OptimizeNolock( emitter->commit_pending_CS );
                } GRAPH_RELEASE;
              } COMLIB_TASK_RESUME_LOCK;

            }
            // A previous suspend request was withdrawn
            else {
              EMITTER_VERBOSE( graph, 0x008, "Suspend request was cancelled" );
            }
          }
        }
      } COMLIB_TASK_RELEASE;

    } END_COMLIB_TASK_MAIN_LOOP;

    GRAPH_LOCK( graph ) {
      EMITTER_CLEAR_READY_CS( emitter );
      EMITTER_CLEAR_RUNNING_CS( emitter );
    } GRAPH_RELEASE;

    // Commit any remaining buffer data to system
    int64_t sz_output_remain = iOpBuffer.Readable( emitter->op_buffer );

    if( emitter->lxw_balance == 0 ) {
      // Set deadline to zero to trigger commit
      emitter->commit_deadline_ms = 0;
      int64_t n_committed = __try_commit_to_system_OPEN( emitter, tms );

      if( n_committed == sz_output_remain ) {
        EMITTER_VERBOSE( graph, 0x009, "Committed %lld remaining bytes of operation data on exit", n_committed );
      }
      else if( n_committed < 0 ) {
        EMITTER_CRITICAL( graph, 0x00A, "Failed to commit %lld remaining bytes of operation data on exit, operations are lost!", sz_output_remain );
      }
      else {
        EMITTER_CRITICAL( graph, 0x00B, "Committed %lld of %lld remaining bytes of operation data on exit, operations are lost!", n_committed, sz_output_remain );
      }
    }
    else {
      EMITTER_CRITICAL( graph, 0x00C, "Non-zero vertex lock balance (%lld), discarding %lld bytes of uncommitted operation data", emitter->lxw_balance, sz_output_remain );
    }

    GRAPH_LOCK( graph ) {
      emitter->sz_private_commit_CS = 0;
    } GRAPH_RELEASE;

  }

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__dump_queues_CS( vgx_Graph_t *graph, const vgx_OperatorCapture_t *capture ) {
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
  __dump_capture_queue_CS( emitter, emitter->capture_pool_CS, "CAPTURE_POOL", capture );
  __dump_capture_queue_CS( emitter, emitter->commit_pending_CS, "COMMIT_PENDING", capture );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxdurable_operation_emitter__wait_available_graph_operation_CS( vgx_Graph_t *graph ) {
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
  bool operation_is_open = OPERATION_IS_OPEN( &graph->operation );
  if( _vgx_graph_is_ready_CS( graph ) ) {
    if( EMITTER_IS_OPMUTED_CS( emitter ) ) {
      EMITTER_WARNING( graph, 0x000, "Graph operation emitter not accepting operation capture at this time" );
      operation_is_open = false;
    }
    else {
      // Processor is running, let's see what could be going on
      if( iOperation.Emitter_CS.IsRunning( graph ) ) {
        // Overload. We should wait a bit.
        if( EMITTER_IS_THROTTLED_CS( graph ) ) {
          // Go to sleep for as long as running emitter is throttled
          int backoff_level;
          while( operation_is_open == false && EMITTER_IS_THROTTLED_CS( graph ) && iOperation.Emitter_CS.IsRunning( graph ) ) {
            backoff_level = __backoff_CS( graph );
            operation_is_open = __is_operation_open_after_backoff_CS( graph, backoff_level );
          }

          // No longer throttled, still running?
          if( iOperation.Emitter_CS.IsRunning( graph ) ) {
            // ERROR: Operation is closed, we can't proceed
            if( (operation_is_open = OPERATION_IS_OPEN( &graph->operation )) == false ) {
              EMITTER_CRITICAL( graph, 0x001, "Graph operation unexpectedly closed after recovering from overload" );
            }
          }
          // ERROR: Processor is not running, we can't proceed
          else {
            EMITTER_CRITICAL( graph, 0x003, "Graph operation emitter not running after recovering from overload" );
            operation_is_open = false;
          }
        }
        // Emitter is running and there is no overload, expect operation to be open
        else {
          if( (operation_is_open = OPERATION_IS_OPEN( &graph->operation )) == false ) {
            EMITTER_CRITICAL( graph, 0x005, "Graph operation unexpectedly closed" );
          }
        }
      }
      // ERROR: Processor is not running, we can't proceed
      else {
        EMITTER_CRITICAL( graph, 0x007, "Graph operation emitter not running" );
        operation_is_open = false;
      }
    }
  }
  else {
    EMITTER_REASON( graph, 0x008, "Graph not ready" );
    operation_is_open = false;
  }

  return operation_is_open;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Operation_t * _vxdurable_operation_emitter__next_operation_CS( struct s_vgx_Graph_t *graph, vgx_Operation_t *closed_operation, const vgx_OperationCaptureInheritable_t *inheritable, bool hold_CS ) {
  __assert_state_lock( graph );

  vgx_Operation_t *new_operation = NULL;

  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  vgx_OperatorCapture_t *capture = NULL;

  int backoff_level = 0;

  // Grab next capture object from pool, if available
  if( (capture = __NEXT_capture( emitter->capture_pool_CS )) != NULL ) {
    // Validate clean object
    if( ComlibSequenceLength( capture->opdatabuf ) != 0 ) {
      _vxdurable_operation_capture__dump_capture_object_CS( capture );
    }
  }

  // Check for backoff level (0 if no backoff)
  backoff_level = __backoff_CS( graph );

  // Handle overload if needed
  // NOTE 1:  Operation is CLOSED at this point. If thread goes to sleep
  //          because of temporary overload another thread will get CS and
  //          potentially try to perform operation capture. Because the operation
  //          is closed it must wait until the operation is re-opened.
  // NOTE2:   If don't throttle if we are forbidden from leaving CS
  if( backoff_level > 0 && hold_CS == false ) {
    // This will spin if backoff is small or sleep if backoff is large (yielding CS in both cases)
    __throttle_CS( graph, backoff_level );
  }

  // If we could not get a next capture object from the pool
  // we need to make one ourselves (and it may be joining
  // the pool later when we're done with it)
  if( capture == NULL ) {
    // Create a new capture object
    capture = __new_operator_capture_OPEN( emitter );
  }

  if( capture != NULL ) {
    // One more inflight
    __inc_inflight_CS( emitter );

    // Initialize operation id from current (closed) operation
    capture->opid = OPERATION_GET_OPID( closed_operation );

    // Inherit data if available
    if( inheritable ) {
      capture->inheritable = *inheritable;
    }
    else {
      memset( &capture->inheritable, 0, sizeof( vgx_OperationCaptureInheritable_t ) );
    }

    // Open the operation with the new capture object
    new_operation = OPERATION_OPEN( closed_operation, capture );
  }

  return new_operation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__return_operator_capture_to_pool_OPEN( vgx_OperatorCapture_t **pcapture ) {
  int ret = 0;
  vgx_OperatorCapture_t *capture = *pcapture;
  vgx_OperationEmitter_t *emitter = capture->emitter;
  // Buffer should be empty
  int64_t zero = ComlibSequenceLength( capture->opdatabuf );
  vgx_Graph_t *graph = emitter->graph;
  GRAPH_LOCK( graph ) {
    __dec_inflight_CS( emitter );
    if( emitter->capture_pool_CS ) {
      // We return capture object to pool unless the pool is sufficiently large
      if( ComlibSequenceLength( emitter->capture_pool_CS ) < SZ_CAPTURE_POOL ) { 
        if( capture->graph == graph && zero == 0 ) {
          __limit_discard_capture_object_size( graph, capture ); 
          if( __APPEND_capture( emitter->capture_pool_CS, capture ) == 1 ) {
            *pcapture = NULL;
          }
        }
        else {
          EMITTER_CRITICAL( graph, 0x001, "Invalid capture object cannot be returned to pool (%lld operators lost)", zero );
          ret = _vxdurable_operation__trap_error( graph, true, 0x001, "invalid capture object" );
        }
      }
    }
  } GRAPH_RELEASE;
  // If the capture object could not be returned to the emitter we delete it here
  if( *pcapture ) {
    if( zero != 0 ) {
      EMITTER_CRITICAL( graph, 0x002, "Discarding non-empty capture object (%lld operators lost)", zero );
    }
    __delete_operator_capture_OPEN( pcapture );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_emitter__next_operation_id_CS( vgx_OperationEmitter_t *emitter_CS ) {
  int64_t next = ++emitter_CS->opid;
  return next;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_emitter__submit_to_pending_CS( vgx_OperatorCapture_t **pcapture ) {
  if( pcapture && *pcapture ) {
    vgx_OperatorCapture_t *capture = *pcapture;
    vgx_OperationEmitter_t *emitter = capture->emitter;
    int64_t opid = capture->opid = _vxdurable_operation_emitter__next_operation_id_CS( emitter );
    // Emitter is running
    if( emitter->commit_pending_CS ) {
      if( __APPEND_capture( emitter->commit_pending_CS, capture ) != 1 ) {
        return -1;
      }
    }
    // No emitter running (detached before operation was submitted)
    else {
      __delete_operator_capture_OPEN( pcapture );
    }
    *pcapture = NULL; // submitted!
    return opid;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxdurable_operation_emitter__has_pending_CS( vgx_Graph_t *graph ) {

  // Emitter has pending operations if operations exist in any of the emitter stages
  if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
    // PRE EMITTER
    // Operations waiting to be processed by emitter
    if( emitter->sz_private_commit_CS + ComlibSequenceLength( emitter->commit_pending_CS ) > 0 ) {
      return true;
    }

    // IN EMITTER
    // Operations are being processed by emitter
    bool is_busy;
    GRAPH_SUSPEND_LOCK( graph ) {
      is_busy = COMLIB_TASK__IsBusy( emitter->TASK );
    } GRAPH_RESUME_LOCK;
    if( is_busy ) {
      return true;
    }

    // POST EMITTER
    // Operations have been emitted to output buffer
    if( iOperation.Emitter_CS.IsRunning( graph ) ) {
      if( !iOpBuffer.Empty( emitter->op_buffer ) ) {
        return true;
      }
    }
  }

  // No operations pending
  return false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_emitter__get_pending_CS( vgx_Graph_t *graph ) {
  int64_t n_pending_op = 0;

  // Emitter has pending operations if operations exist in any of the emitter stages
  if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
    // IN EMITTER
    // Operations waiting to be processed by emitter
    n_pending_op = emitter->sz_private_commit_CS + ComlibSequenceLength( emitter->commit_pending_CS );
      
    // POST EMITTER
    // Operations have been emitted to output buffer
    if( !iOpBuffer.Empty( emitter->op_buffer ) ) {
      ++n_pending_op; // add one to indicate nonzero ops in output buffer
    }
  }

  return n_pending_op;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxdurable_operation_emitter__has_pending_OPEN( vgx_Graph_t *graph ) {
  bool has_pending;
  GRAPH_LOCK( graph ) {
    has_pending = _vxdurable_operation_emitter__has_pending_CS( graph );
  } GRAPH_RELEASE;
  return has_pending;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__fence_CS( vgx_Graph_t *graph, int64_t opid, int timeout_ms ) {
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    bool timeout = false;
    bool wait = true;
    BEGIN_TIME_LIMITED_WHILE( 
      wait
      && iOperation.Emitter_CS.IsRunning( graph )
      && (emitter->opid_last_commit_CS < opid || iOperation.Emitter_CS.GetPending( graph ) > 0),
      timeout_ms,
      &timeout )
    {
      graph->OP.emitter.control.flag_CS.flush = true;
      GRAPH_SUSPEND_LOCK( graph ) {

        COMLIB_TASK_LOCK( emitter->TASK ) {
          if( COMLIB_TASK__IsSuspended( emitter->TASK ) ) {
            wait = false;
          }
          else {
            COMLIB_TASK_SIGNAL_WAKE( emitter->TASK );
            COMLIB_TASK_WAIT_FOR_IDLE( emitter->TASK, 5 );
          }
        } COMLIB_TASK_RELEASE;

      } GRAPH_RESUME_LOCK;
    } END_TIME_LIMITED_WHILE;
    
    // Timeout, operation not committed yet
    if( timeout ) {
      return -1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__heartbeat_enable_CS( struct s_vgx_Graph_t *graph ) {
  GRAPH_LOCK( graph ) {
    EMITTER_ENABLE_HEARTBEAT_CS( &graph->OP.emitter );
    EMITTER_INFO( graph, 0x001, "Heartbeat ON" );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__heartbeat_disable_CS( struct s_vgx_Graph_t *graph ) {
  GRAPH_LOCK( graph ) {
    EMITTER_DISABLE_HEARTBEAT_CS( &graph->OP.emitter );
    EMITTER_INFO( graph, 0x001, "Heartbeat OFF" );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_emitter__inflight_capacity_CS( vgx_Graph_t *graph ) {
  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;
  int64_t available = INFLIGHT_BACKOFF_THRESHOLD_HIGH - __get_inflight_CS( emitter );
  return available;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__initialize_CS( vgx_Graph_t *graph, int64_t init_opid ) {

  int init = 0;

  // Already initialized?
  if( iOperation.Emitter_CS.IsInitialized( graph ) ) {
    return 0;
  }

  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  XTRY {

    if( emitter->TASK ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
    }

    EMITTER_VERBOSE( graph, 0, "Initializing..." );

    // Reset data
    memset( emitter, 0, sizeof( vgx_OperationEmitter_t ) );

    // [Q1.1] TASK
    emitter->TASK = NULL;

    // [Q1.2] control
    emitter->control._bits = 0;

    // [Q1.3] graph
    emitter->graph = graph;

    // [Q1.4] Opid
    emitter->opid = init_opid;

    // [Q1.5] Operation buffer (constructed in emitter thread)
    emitter->op_buffer = NULL;

    // [Q1.6] Inflight operation counter
    emitter->n_inflight_CS = 0;

    // [Q1.7] Exclusive write lock balance (for transaction commit point detection)
    emitter->lxw_balance = 0;

    // [Q1.8] Transaction commit must occur when graph time passes deadline
    emitter->commit_deadline_ms = 0;

    // [Q2.1] Private commit (constructed in emitter thread)
    emitter->private_commit = NULL;

    // [Q2.2] Private retire (constructed in emitter thread)
    emitter->private_retire = NULL;

    // [Q2.3] Capture pool (constructed in emitter thread)
    emitter->capture_pool_CS = NULL;

    // [Q2.4] Commit pending (constructed in emitter thread)
    emitter->commit_pending_CS = NULL;

    // [Q2.5] Commit swap (constructed in emitter thread)
    emitter->commit_swap = NULL;

    // [Q2.6] CString discard (constructed in emitter thread)
    emitter->cstring_discard = NULL;

    // [Q2.7]
    emitter->opid_last_emit = 0;

    // [Q2.8]
    emitter->opid_last_commit_CS = 0;

    // [Q3.1-6] Operation stream ready
    INIT_CONDITION_VARIABLE( &emitter->opstream_ready.cond );

    // [Q3.7]
    emitter->n_uncommitted.operations = 0;
    emitter->n_uncommitted.opcodes = 0;
    
    // [Q3.8]
    emitter->sz_private_commit_CS = 0;

  }
  XCATCH( errcode ) {
    init = -1;
  }
  XFINALLY {
  }

  return init;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__is_initialized_CS( vgx_Graph_t *self ) {
  if( self->OP.emitter.graph == self ) {
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__start_CS( vgx_Graph_t *graph ) {

  int started = 0;

  // Not initialized?
  if( !iOperation.Emitter_CS.IsInitialized( graph ) ) {
    REASON( 0x001, "Cannot start uninitialized emitter for graph '%s'", CALLABLE( graph )->FullPath( graph ) );
    return -1;
  }
  // Already running?
  else if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    return 0;
  }

  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  XTRY {

    // ===============================
    // START EMITTER THREAD
    // ===============================

    EMITTER_VERBOSE( graph, 0, "Starting..." );

    if( (emitter->TASK = COMLIB_TASK__New( __operation_emitter_monitor, __operation_emitter_initialize, __operation_emitter_shutdown, emitter )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
    }

    GRAPH_SUSPEND_LOCK( graph ) {
      started = COMLIB_TASK__Start( emitter->TASK, 30000 );
    } GRAPH_RESUME_LOCK;

    if( started == 1 ) {
      EMITTER_CLEAR_DEFUNCT_CS( emitter );
      EMITTER_CLEAR_OPMUTED_CS( emitter );
      EMITTER_VERBOSE( graph, 0, "Started" );
    }
    else {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
    }

  }
  XCATCH( errcode ) {
    if( emitter->TASK ) {
      COMLIB_TASK__Delete( &emitter->TASK );
    }

    started = -1;
  }
  XFINALLY {
  }

  return started;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__stop_CS( vgx_Graph_t *graph ) {
  int stopped = 0;

  vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

  if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    
    int64_t n_writable;
    if( (n_writable = _vgx_graph_get_vertex_WL_count_CS( graph )) != 0 ) {
      EMITTER_CRITICAL( graph, 0x001, "Stopping emitter for graph '%s' with %lld vertices still open. Forcing commit.", CALLABLE( graph )->FullPath( graph ), n_writable );
      _vxgraph_tracker__commit_writable_vertices_CS( graph, 0 );
    }

    // Mark emitter as not accepting any more operation captures
    EMITTER_SET_OPMUTED_CS( emitter );

    // Close graph operation if open
    if( iOperation.IsOpen( &graph->operation ) ) {
      // TODO: Verify that this will flush all operations in buffer into emitter,
      // and that emitter picks up all the operations and wait until there are
      // no more inflight operations!!!!
      // 
      //
      // # This calls:
      //    _vxdurable_operation_capture__close_CS( graph, operation=&graph->operation, bool hold_CS=false );
      //    # For system graph:
      //      # this gets the opid out of the operation
      //      # then calls: 
      //        next = __commit_operation_CS( graph, operation, close=true, hold_CS=false );
      //        # which, if operation is dirty or has nonempty buffer, calls:
      //          opid = _vxdurable_operation_emitter__submit_to_pending_CS( &capture ); // capture object popped from operation
      //          # which ends up calling (and returns the next opid):
      //            __APPEND_capture( emitter->commit_pending_CS, capture );
      CLOSE_GRAPH_OPERATION_CS( graph );
      // emitter->commit_pending_CS now has one more capture object
      // We need to make sure the emitter keeps running until nothing is pending.
    }

    // Hold here until emitter has nothing pending
    int timeout_ms = 300000;
    GRAPH_WAIT_UNTIL( graph, iOperation.Emitter_CS.HasPending( graph ) == false, timeout_ms );

    // Mark defunct
    EMITTER_SET_DEFUNCT_CS( emitter );

    // Stop
    comlib_task_t *task = emitter->TASK;
    GRAPH_SUSPEND_LOCK( graph ) {
      COMLIB_TASK_LOCK( task ) {
        if( (stopped = COMLIB_TASK__Stop( task, __drain_as_percentage_TCS, timeout_ms )) < 0 ) {
          EMITTER_WARNING( graph, 0x002, "Forcing exit" );
          stopped = COMLIB_TASK__ForceExit( task, 300000 );
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;

    // Signal and brief pause before destruction.
    // This is a way to ensure already started fence requests
    // do not suddenly find the task structure gone and access
    // invalid memory. 
    // !!! This is not a good way to do it !!!
    // TODO: Make it better
    for( int i=0; i<10; i++ ) {
      GRAPH_SUSPEND_LOCK( graph ) {
        COMLIB_TASK_LOCK( task ) {
          COMLIB_TASK_SIGNAL_IDLE( task );
        } COMLIB_TASK_RELEASE;
        sleep_milliseconds( 25 );
      } GRAPH_RESUME_LOCK;
    }

    if( stopped == 1 ) {
      COMLIB_TASK__Delete( &emitter->TASK );
    }
    else {
      EMITTER_CRITICAL( graph, 0x003, "Unresponsive emitter thread" );
    }
  }

  return stopped;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__destroy_CS( vgx_Graph_t *graph ) {
  int ret = 0;

  if( iOperation.Emitter_CS.IsInitialized( graph ) ) {
    vgx_OperationEmitter_t *emitter = &graph->OP.emitter;

    // Emitter is running
    if( iOperation.Emitter_CS.IsRunning( graph ) ) {
      // Stop emitter
      if( iOperation.Emitter_CS.Stop( graph ) == 1 ) {

        // Delete condition variable
        DEL_CONDITION_VARIABLE( &emitter->opstream_ready.cond );

        // Reset all data
        memset( emitter, 0, sizeof( vgx_OperationEmitter_t ) );

        // Emitter stopped and all data erased
        ret = 1;
      }
      // Failed to stop
      else {
        ret = -1;
      }
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__is_running_CS( vgx_Graph_t *self ) {
  vgx_OperationEmitter_t *emitter = &self->OP.emitter;
  if( emitter->TASK && !EMITTER_IS_DEFUNCT_CS( emitter ) && EMITTER_IS_RUNNING_CS( emitter ) ) {
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__suspend_CS( vgx_Graph_t *self, int timeout_ms ) {
  int ret = 0;
  if( iOperation.Emitter_CS.IsRunning( self ) ) {
    int64_t nW = _vgx_graph_get_vertex_WL_count_CS( self );
    if( nW > 0 ) {
      EMITTER_REASON( self, 0x001, "Cannot suspend emitter with %lld writable vertices", nW );
      ret = -1;
    }
    else {
      GRAPH_SUSPEND_LOCK( self ) {
        vgx_OperationEmitter_t *emitter = &self->OP.emitter;
        COMLIB_TASK_LOCK( emitter->TASK ) {
          if( COMLIB_TASK__IsAlive( emitter->TASK ) ) {
            ret = COMLIB_TASK__Suspend( emitter->TASK, __drain_as_percentage_TCS, timeout_ms );
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  }

  return ret;
}
  


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__is_suspended_CS( vgx_Graph_t *self ) {
  int suspended = 0;
  if( iOperation.Emitter_CS.IsRunning( self ) ) {
    GRAPH_SUSPEND_LOCK( self ) {
      vgx_OperationEmitter_t *emitter = &self->OP.emitter;
      suspended = COMLIB_TASK__IsSuspended( emitter->TASK );
    } GRAPH_RESUME_LOCK;
  }
  return suspended;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__resume_CS( vgx_Graph_t *self ) {
  int ret = 0;

  if( iOperation.Emitter_CS.IsRunning( self ) ) {
    GRAPH_SUSPEND_LOCK( self ) {
      vgx_OperationEmitter_t *emitter = &self->OP.emitter;
      COMLIB_TASK_LOCK( emitter->TASK ) {
        if( COMLIB_TASK__IsAlive( emitter->TASK ) ) {
          ret = COMLIB_TASK__Resume( emitter->TASK, 10000 );
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_emitter__is_ready_CS( vgx_Graph_t *self ) {
  vgx_OperationEmitter_t *emitter = &self->OP.emitter;
  // Ready when running and not suspended
  return (int)(emitter->TASK && EMITTER_IS_READY_CS( emitter ));
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__enable_CS( vgx_Graph_t *self ) {
  EMITTER_ENABLE_CS( &self->OP.emitter );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__disable_CS( vgx_Graph_t *self ) {
  EMITTER_DISABLE_CS( &self->OP.emitter );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxdurable_operation_emitter__is_enabled_CS( vgx_Graph_t *self ) {
  vgx_OperationEmitter_t *emitter = &self->OP.emitter;
  if( emitter->TASK && EMITTER_IS_RUNNING_CS( emitter ) && !EMITTER_IS_DISABLED_CS( emitter ) ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__set_opmuted_CS( vgx_Graph_t *self ) {
  EMITTER_SET_OPMUTED_CS( &self->OP.emitter );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_emitter__clear_opmuted_CS( vgx_Graph_t *self ) {
  EMITTER_CLEAR_OPMUTED_CS( &self->OP.emitter );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxdurable_operation_emitter__is_opmuted_CS( vgx_Graph_t *self ) {
  return EMITTER_IS_OPMUTED_CS( &self->OP.emitter );
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_emitter.h"
  
test_descriptor_t _vgx_vxdurable_operation_emitter_tests[] = {
  { "VGX Graph Durable Operation Tests", __utest_vxdurable_operation_emitter },
  {NULL}
};
#endif
