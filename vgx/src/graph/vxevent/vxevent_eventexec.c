/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxevent_eventexec.c
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

#include "_vxevent.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

#define LATE_EVENT_THRESHOLD 30 // 30 seconds overdue is considered late

__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "EV::EXE(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define EXECUTOR_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define EXECUTOR_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define EXECUTOR_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define EXECUTOR_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define EXECUTOR_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define EXECUTOR_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )


__inline static void __reschedule_event_WL( vgx_Graph_t *graph, vgx_Vertex_t *vertex_WL, vgx_vertex_expiration_t *expiration ) {
  uint32_t tmx;
  if( expiration ) {
    tmx = minimum_value( expiration->vertex_ts, expiration->arc_ts );
  }
  else {
    tmx = minimum_value( vertex_WL->TMX.vertex_ts, vertex_WL->TMX.arc_ts );
  }

  // Something expires in the future
  if( tmx < TIME_EXPIRES_NEVER ) {
    if( iGraphEvent.ScheduleVertexEvent.Expiration_WL( graph, vertex_WL, tmx ) < 0 ) {
      EXECUTOR_CRITICAL( graph, 0x301, "Failed to re-schedule next event at ts=%u for vertex '%s'", tmx, CALLABLE( vertex_WL )->IDString( vertex_WL ) );
    }
  }
  // Nothing expires in the future
  else {
    __vertex_all_clear_expiration( vertex_WL );
    iGraphEvent.CancelVertexEvent.RemoveSchedule_WL( graph, vertex_WL );
  }
}


/*******************************************************************//**
 * Perform arc expiration action on the vertex.
 * 
 * The outarcs arcvector will be scanned for expired arcs 
 * (arcs/multiarcs with M_TMX in the past) and such arcs will be removed.
 * 
 * If after removal of expired arcs the vertex has future expiring arcs
 * or the vertex itself expires in the future another event will
 * automatically be scheduled.
 * 
 * If no future vertex events exist all expiration TMX will be cleared from
 * the vertex and it will be removed from the EVP.
 *
 * Returns:
 *          1 : arc(s) deleted
 *          0 : nothing deleted (new event may or may not be scheduled)
 *         -1 : error
 *
 ***********************************************************************
 */
static int __execute_arc_expiration_event_CS_WL( vgx_Graph_t *graph, vgx_Vertex_t *vertex_WL ) {
  int ret = 0;

  // Let's release the state lock to avoid unnecessary blocking of other threads during expensive arcvector scan
  GRAPH_SUSPEND_LOCK( graph ) {
    uint32_t now_ts = _vgx_graph_seconds( graph );
    vgx_vertex_expiration_t next_expiration = {
      .vertex_ts  = __vertex_get_expiration_ts( vertex_WL ),
      .arc_ts     = TIME_EXPIRES_NEVER
    };

    // Perform arc expiration (may or may not remove anything), and possibly getting an earlier next timestamp if arcs exist that expire before the vertex.
    __check_vertex_consistency_WL( vertex_WL );
    int64_t n_expired = iarcvector.Expire( &graph->arcvector_fhdyn, vertex_WL, now_ts, &next_expiration.arc_ts );
    
    // Critical error, remove all expiration and remove from EVP
    if( n_expired < 0 ) {
      ret = -1;
      next_expiration.vertex_ts = TIME_EXPIRES_NEVER;
      next_expiration.arc_ts = TIME_EXPIRES_NEVER;
    }
    else {
      ret = n_expired > 0;
    }

    __reschedule_event_WL( graph, vertex_WL, &next_expiration ); 
    
  } GRAPH_RESUME_LOCK;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __consume_bad_event( vgx_VertexStorableEvent_t *ev, vgx_Vertex_t *vertex_WL ) {
  vgx_Graph_t *graph = vertex_WL->graph;
  __vertex_clear_event_scheduled( vertex_WL );
  Vertex_DECREF_WL( vertex_WL );
  EXECUTOR_CRITICAL( graph, 0x311, "Invalid event type 0x%02x in Executor", ev->event_val.type );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __is_vertex_object( void *o ) {
  comlib_object_t *obj = COMLIB_OBJECT( o );
  int is_vertex = COMLIB_OBJECT_CLASSMATCH( obj, COMLIB_CLASS_CODE( vgx_Vertex_t ) )
                  && 
                  CALLABLE( obj ) == COMLIB_CLASS_VTABLE( vgx_Vertex_t )
                  && 
                  !__vertex_is_defunct( (vgx_Vertex_t*)obj );
  return is_vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_vertex_expiration_event( vgx_Graph_t *graph, vgx_Vertex_t *vertex, vgx_VertexStorableEvent_t *ev, vgx_AccessReason_t *reason ) {
  int executed = 0;

  GRAPH_LOCK( graph ) {
    // Safeguard: Proceed only if the object address contains something that looks like a vertex and vertex is not defunct
    //            It is possible that the vertex has been deleted by the time this event triggers.
    //            
    if( __is_vertex_object( vertex ) ) {
      // Acquire writelock, nonblocking
      vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( graph );
      vgx_Vertex_t *vertex_WL = _vxgraph_state__lock_vertex_writable_CS( graph, vertex, &zero_timeout, VGX_VERTEX_RECORD_EVENTEXEC );
      __set_access_reason( reason, zero_timeout.reason );
      if( vertex_WL ) {
        __check_vertex_consistency_WL( vertex_WL );
        // At this point the vertex event is considered to be completed by the Event Processor.
        // Last minute check to ensure vertex still has event scheduled and only then will Event processor
        // give up its ownership and execute the event.
        if( __vertex_has_event_scheduled( vertex_WL ) ) {
          // Execute expiration event. A follow-up event may be scheduled automatically.
          if( __is_expiration_event( ev ) ) {
            // Vertex is expired
            if( __vertex_is_expired( vertex_WL, _vgx_graph_seconds( graph ) ) ) {
              // Delete vertex (will consume the event on success)
              if( (executed = _vxdurable_commit__delete_vertex_CS_WL( graph, &vertex_WL, &zero_timeout, VGX_VERTEX_RECORD_EVENTEXEC )) < 0 ) {
                // Negative return code, event NOT consumed. Executor will re-queue and try again.
                __set_access_reason( reason, zero_timeout.reason );
              }
            }
            // Arcs have expired, remove and discover future arc expiration
            else if( __vertex_arcvector_is_expired( vertex_WL, _vgx_graph_seconds( graph ) ) ) {
              if( (executed = __execute_arc_expiration_event_CS_WL( graph, vertex_WL )) < 0 ) {
                // Negative return code (bad error, not a timeout)
                // Assign a reason that prevents Executor from re-queuing
                __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
              }
            }
            // Nothing has expired (premature event)
            else {
              __reschedule_event_WL( graph, vertex_WL, NULL );
            }
          }
          // Bad event
          else {
            __consume_bad_event( ev, vertex_WL );
          }
        }

        // Manually release vertex if not already released
        if( vertex_WL ) {
          _vxgraph_state__unlock_vertex_CS_LCK( graph, &vertex_WL, VGX_VERTEX_RECORD_EVENTEXEC );
        }

      }
      // Can't acquire vertex at this time. Re-try later.
      else {
        executed = -1;
      }
    }
  } GRAPH_RELEASE;

#ifndef NDEBUG
#endif

  return executed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __executor_get_due_batch( vgx_ExecutionJobDescriptor_t *job, uint32_t ts_now ) {
  int64_t sz_batch = 0;
  COMLIB_TASK_LOCK( job->TASK ) {
    // Transfer any almost-due events from before back into the input queue
    ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK( job->Queue.pri_inp, job->Queue.lin_imm, -1 );
    // Transfer due events from input queue into the new execution batch
    // Batch now includes all events that are due immediately (up to a max batch size)
    int64_t sz_input = LENGTH_HEAP_VERTEX_EVENTS( job->Queue.pri_inp );
    if( sz_input > 0 ) {
      sz_batch = ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK( job->Queue.lin_bat, job->Queue.pri_inp, ts_now, job->batch_size );
    }
    // Set the number of events currently managed by the executor
    job->n_current = (int)sz_input;
  } COMLIB_TASK_RELEASE;
  return sz_batch;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __executor_handle_permanent_error( vgx_ExecutionJobDescriptor_t *job, vgx_VertexStorableEvent_t *ev, vgx_AccessReason_t reason, vgx_Vertex_t *vertex, bool shutdown ) {
  EXECUTOR_VERBOSE( job->graph, 0x324, "Non-transient failure to execute event type %u for vertex @ 0x%016llx", ev->event_val.type, vertex );
  COMLIB_TASK_LOCK( job->TASK ) {
    // Best effort
    if( !APPEND_QUEUE_VERTEX_EVENT_NOLOCK( job->Queue.lin_pen, ev ) ) {
      EXECUTOR_CRITICAL( job->graph, 0x325, "Permanent failure to execute event type %u for vertex @ 0x%016llx", ev->event_val.type, vertex );
    }
  } COMLIB_TASK_RELEASE;
  if( shutdown ) {
    EXECUTOR_VERBOSE( job->graph, 0x322, "Unexpected readonly graph (reason 0x%03X), executor terminating after %.2f seconds", reason, COMLIB_TASK__AgeSeconds( job->TASK ) );
    COMLIB_TASK__Request_Stop( job->TASK );
  }
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __executor_handle_transient_error( vgx_ExecutionJobDescriptor_t *job, vgx_VertexStorableEvent_t *ev, vgx_AccessReason_t reason, vgx_Vertex_t *vertex ) {
  // Put event with transient error into the imminent queue with a deferred execution 1 second later
  ++ev->event_val.ts_exec;
  if( !APPEND_QUEUE_VERTEX_EVENT_NOLOCK( job->Queue.lin_imm, ev ) ) {
    // Failed to re-queue it on the inside, move back to outside
    return __executor_handle_permanent_error( job, ev, reason, vertex, false );
  }
  return 0;
}



/*******************************************************************//**
 *
 * Returns:
 *    1   Event processed
 *    0   Event not processed due to a transient error
 *   -1   Event not processed due to a permanent error
 *
 ***********************************************************************
 */
static int __executor_process_event( vgx_ExecutionJobDescriptor_t *job, vgx_VertexStorableEvent_t *ev ) {
  vgx_Graph_t *graph = job->graph;
  cxmalloc_family_t *valloc = graph->vertex_allocator;
  vgx_Vertex_t *vertex;
  int n_exec = 0;

  // Convert vertex key to vertex address
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  // Translate the event key back to vertex object (may no longer be valid)
  if( (vertex = __event_key_as_vertex( ev, valloc )) != NULL && _cxmalloc_is_object_active( vertex ) ) {
    if( (n_exec = __execute_vertex_expiration_event( graph, vertex, ev, &reason )) < 0 ) {
      // Failed to execute event
      // Transient failure
      switch( reason ) {
      case VGX_ACCESS_REASON_LOCKED:           // FALLTHRU 
      case VGX_ACCESS_REASON_TIMEOUT:          // FALLTHRU
      case VGX_ACCESS_REASON_VERTEX_ARC_ERROR:        // FALLTHRU
      case VGX_ACCESS_REASON_RO_DISALLOWED:    // FALLTHRU   ???
        // Transient error, keep the vertex in our job queue
        return __executor_handle_transient_error( job, ev, reason, vertex );
      case VGX_ACCESS_REASON_READONLY_GRAPH:   // FALLTHRU
      case VGX_ACCESS_REASON_READONLY_PENDING: // FALLTHRU
        // Readonly graph, shut down executor
        return __executor_handle_permanent_error( job, ev, reason, vertex, true );
      default:
        // Any other reason is a non-transient error, but leave the executor running
        return __executor_handle_permanent_error( job, ev, reason, vertex, false );
      }
    }
  }

  // Event processed or ignored or re-scheduled
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __executor_update( vgx_ExecutionJobDescriptor_t *job, int n_batch_exec ) {
  int64_t n_pending = 0;
  COMLIB_TASK_LOCK( job->TASK ) {
    if( n_batch_exec > 0 ) {
      int64_t n = job->n_exec + n_batch_exec;
      job->n_exec = n;
    }

    if( COMLIB_TASK__IsStopping( job->TASK ) ) {
      // Pending executions still exist, transfer to failed queue so we can re-scheduled them
      int64_t n_defer = LENGTH_HEAP_VERTEX_EVENTS( job->Queue.pri_inp ) + LENGTH_QUEUE_VERTEX_EVENTS( job->Queue.lin_imm );
      if( n_defer > 0 ) {
        ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK( job->Queue.pri_inp, job->Queue.lin_imm, -1 );
        ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK( job->Queue.lin_pen, job->Queue.pri_inp, TIMESTAMP_MAX, -1 );
      }
    }
    n_pending = LENGTH_QUEUE_VERTEX_EVENTS( job->Queue.lin_pen );
  } COMLIB_TASK_RELEASE;
  return n_pending;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __executor_get_loop_delay( vgx_ExecutionJobDescriptor_t *job, int n_batch_exec, int64_t sz_batch, int n_transients ) {
  int64_t loop_delay = 0;

  // We are not making progress
  if( n_batch_exec == 0 ) {
    // Transient errors (typically locked vertices), ease off.
    if( n_transients > 0 ) {
      // Longer sleep if we are not making progress
      loop_delay = COMLIB_TASK_LOOP_DELAY( n_transients );
    }
    // Brief nap if we encountered any not-yet due events.
    else if( LENGTH_QUEUE_VERTEX_EVENTS( job->Queue.lin_imm ) ) {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 1 );
    }
    // Input exhausted
    else if( sz_batch == 0 ) {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 333 );
    }
    // No progress for unknown reason
    else {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 1 );
    }
  }
  // We are making progress, go as fast as possible
  else {
    // Transient errors (typically locked ve rtices), ease off.
    if( n_transients > 0 ) {
      // Short sleep when we are making progress
      loop_delay = COMLIB_TASK_LOOP_DELAY( 1 );
    }
    else {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
    }
  }

  return loop_delay;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_ExecutionJobDescriptor_t,
                   job,
                   __execution_job,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "event_executor/" )
{

#define __nanoseconds_from_ms( Milliseconds ) ((Milliseconds) * 1000000)

  vgx_Graph_t *graph = job->graph;
  const char *graph_name = CStringValue( CALLABLE(graph)->Name(graph) );

  APPEND_THREAD_NAME( graph_name );
  COMLIB_TASK__AppendDescription( self, graph_name );

  EXECUTOR_VERBOSE( graph, 0, "New execution thread created" );

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

  // Run event execution until input queue is exhausted
  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
    XTRY {

      // Get next batch of events
      int n_batch_exec = 0;
      int64_t n_pending = 0;
      uint32_t ts_now = _vgx_graph_seconds( graph );
      int64_t sz_batch;
      // Flag if we should sleep a little before executing next batch
      int n_transients_occurred = 0;

      // Transfer a batch of due events into the job's batch queue, then execute those events
      if( (sz_batch = __executor_get_due_batch( job, ts_now )) > 0 ) {
        vgx_VertexStorableEvent_t ev;
        int proc;
        // Process batch
        while( NEXT_QUEUE_VERTEX_EVENT_NOLOCK( job->Queue.lin_bat, &ev ) == 1 ) {
          // Process event
          if( (proc = __executor_process_event( job, &ev )) > 0 ) {
            ++n_batch_exec;
          }
          // Transient event error
          else if( proc == 0 ) {
            ++n_transients_occurred;
          }
          // Permanent event error
          else {
          }
        }
      }
      // Error
      else if( sz_batch < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x321 );
      }

      // Update counts and check if execution job has been asked to terminate early
      n_pending = __executor_update( job, n_batch_exec );

      // Determine how fast executor should run based on processing results for last batch
      loop_delay = __executor_get_loop_delay( job, n_batch_exec, sz_batch, n_transients_occurred );

      // Input exhausted or controlled termination
      if( COMLIB_TASK__IsStopping( self ) ) {
        COMLIB_TASK__AcceptRequest_Stop( self );
      }

    }
    XCATCH( errcode ) {
      COMLIB_TASK__Request_Stop( self );
      COMLIB_TASK__SetReturnCode( self, -1 );
    }
    XFINALLY {
    }

  } END_COMLIB_TASK_MAIN_LOOP;

  EXECUTOR_VERBOSE( graph, 0x327, "Execution thread terminating" );

  // Done
  COMLIB_TASK_LOCK( self ) {
    job->n_current = 0;
  } COMLIB_TASK_RELEASE;

} END_COMLIB_TASK;



/*******************************************************************//**
 * Stop the execution thread if one exists and is running.
 *
 * Return a queue of vertex addresses of vertices that are still
 * pending execution.
 *
 * Return NULL on error.
 *
 * WARNING: Caller owns returned queue!
 ***********************************************************************
 */
 vgx_VertexEventQueue_t * __eventexec_cancel_execution_job_WL( vgx_ExecutionJobDescriptor_t **job, int timeout_ms ) {

  if( job == NULL || *job == NULL ) {
    return NULL;
  }
  else {
    vgx_ExecutionJobDescriptor_t *executor = *job;

    int64_t n_pre = 0;
    int64_t n_post = 0;

    // Request shutdown of executor and wait for execution to complete
    COMLIB_TASK_LOCK( executor->TASK ) {
      // Number of executor events before shutdown
      n_pre = executor->n_current;
      // Stop
      if( COMLIB_TASK__Stop( executor->TASK, NULL, timeout_ms ) < 0 ) {
        EXECUTOR_WARNING( executor->graph, 0x330, "Forcing exit" );
        COMLIB_TASK__ForceExit( executor->TASK, 30000 );
      }
      // Number of executor events after shutdown (or timeout)
      n_post = executor->n_current;
    } COMLIB_TASK_RELEASE;


    // Executor has terminated
    if( COMLIB_TASK__IsDead( executor->TASK ) ) {
      vgx_VertexEventQueue_t *Qpen = COMLIB_OBJECT_NEW_DEFAULT( Cm128iQueue_t );
      // Gather all pending events for rescheduling
      int64_t n_pending = LENGTH_QUEUE_VERTEX_EVENTS( executor->Queue.lin_pen );
      ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK( Qpen, executor->Queue.lin_pen, n_pending );

      // Join and delete the thread
      COMLIB_TASK__Delete( &executor->TASK );

      // Destroy the execution queue
      if( executor->Queue.pri_inp ) {
        COMLIB_OBJECT_DESTROY( executor->Queue.pri_inp );
      }

      // Destroy the batch queue
      if( executor->Queue.lin_bat ) {
        COMLIB_OBJECT_DESTROY( executor->Queue.lin_bat );
      }

      // Destroy the imminent queue
      if( executor->Queue.lin_imm ) {
        COMLIB_OBJECT_DESTROY( executor->Queue.lin_imm );
      }

      // Destroy the pending queue
      if( executor->Queue.lin_pen ) {
        COMLIB_OBJECT_DESTROY( executor->Queue.lin_pen );
      }

      // Deallocate
      free( executor );

      // Erase event monitor's reference
      *job = NULL;

      return Qpen;
    }
    // Executor did not terminate (still processing)
    else {
      EXECUTOR_WARNING( executor->graph, 0x331, "Event Executor still running after %d ms (%lld -> %lld executing events)", timeout_ms, n_pre, n_post );
      return NULL;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
vgx_ExecutionJobDescriptor_t * __eventexec_new_execution_job_WL( vgx_EventProcessor_t *processor_WL ) {
  // Start a new execution thread

  vgx_ExecutionJobDescriptor_t *job = NULL;
  vgx_Graph_t *graph = processor_WL->graph;
  
  XTRY {
    // Allocate the job descriptor
    if( (job = calloc( 1, sizeof( vgx_ExecutionJobDescriptor_t ))) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x341 );
    }

    // New task
    if( (job->TASK = COMLIB_TASK__New( __execution_job, NULL, NULL, job )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x342 );
    }

    // Initialize data
    job->n_current = 0;
    job->n_exec = 0;
    job->ontime_rate = 1.0;
    job->batch_size = 512;

    // Priority Queue Constructor args
    Cm128iHeap_constructor_args_t priority_queue_args = {
      .element_capacity = job->batch_size,
      .comparator = (f_Cm128iHeap_comparator_t)__cmp_event
    };

    // Linear Queue Constructor args
    Cm128iQueue_constructor_args_t linear_queue_args = {
      .element_capacity = job->batch_size,
      .comparator = NULL
    };

    // Create queue for holding events that will be executed by execution job
    if( (job->Queue.pri_inp = COMLIB_OBJECT_NEW( Cm128iHeap_t, NULL, &priority_queue_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x342 );
    }

    // Create internal batch queue
    if( (job->Queue.lin_bat = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &linear_queue_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x343 );
    }

    // Create backlog for almost-due events
    if( (job->Queue.lin_imm = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &linear_queue_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x344 );
    }
    
    // Create queue for holding events that did not get executed by execution job
    if( (job->Queue.lin_pen = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &linear_queue_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x345 );
    }
    
    // Parent graph of executor is parent graph of event processor
    job->graph = graph;
    

    // Start task
    if( COMLIB_TASK__Start( job->TASK, 10000 ) != 1 ) {
      EXECUTOR_CRITICAL( graph, 0x346, "Failed to start execution background job" );
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x346 );
    }

  }
  XCATCH( errcode ) {
    __eventexec_cancel_execution_job_WL( &job, -1 );
  }
  XFINALLY {
  }

  return job;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxevent_eventexec.h"

test_descriptor_t _vgx_vxevent_eventexec_tests[] = {
  { "VGX Event Execution Tests", __utest_vxevent_eventexec },

  {NULL}
};
#endif
