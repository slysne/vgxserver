/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxevent_eventapi.c
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



static framehash_t * __new_event_map_CS( vgx_Graph_t *self, __map_term term );

static void __init_eventproc_parameters( vgx_eventproc_parameters_t *params );

static int _vxevent_eventapi__initialize( vgx_Graph_t *self, bool run_daemon );
static int _vxevent_eventapi__start( vgx_Graph_t *self, bool enable );
static int _vxevent_eventapi__destroy( vgx_Graph_t *self );
static int _vxevent_eventapi__schedule( vgx_Graph_t *self );
static int _vxevent_eventapi__is_ready( vgx_Graph_t *self );
static int _vxevent_eventapi__is_enabled( vgx_Graph_t *self );
static vgx_EventProcessor_t * _vxevent_eventapi__acquire( vgx_Graph_t *self, int timeout_ms );
static int _vxevent_eventapi__release( vgx_EventProcessor_t **processor_WL );
static int _vxevent_eventapi__flush_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
static int _vxevent_eventapi__enable_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
static int _vxevent_eventapi__disable_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
static int _vxevent_eventapi__set_defunct_NOCS( vgx_Graph_t *self, int timeout_ms );
static vgx_EventBacklogInfo_t _vxevent_eventapi__backlog_info( vgx_Graph_t *self );
static CString_t * _vxevent_eventapi__format_backlog_info( vgx_Graph_t *self, vgx_EventBacklogInfo_t *backlog );

// Schedule Vertex Event
static int _vxevent_eventapi__schedule_vertex_event_expiration_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, uint32_t tmx );

// Cancel Vertex Event
static void _vxevent_eventapi__remove_schedule_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
static void _vxevent_eventapi__drop_vertex_event_CS_WL_NT( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );

static int _vxevent_eventapi__exists_in_schedule_WL_NT( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );

static uint32_t _vxevent_eventapi__executor_thread_id( vgx_Graph_t *self );


DLL_EXPORT vgx_IGraphEvent_t iGraphEvent = {
  .Initialize                     = _vxevent_eventapi__initialize,
  .Start                          = _vxevent_eventapi__start,
  .Destroy                        = _vxevent_eventapi__destroy,
  .Schedule                       = _vxevent_eventapi__schedule,
  .IsReady                        = _vxevent_eventapi__is_ready,
  .IsEnabled                      = _vxevent_eventapi__is_enabled,
  .Acquire                        = _vxevent_eventapi__acquire,
  .Release                        = _vxevent_eventapi__release,

  .NOCS = {
    .Flush                        = _vxevent_eventapi__flush_NOCS,
    .Enable                       = _vxevent_eventapi__enable_NOCS,
    .Disable                      = _vxevent_eventapi__disable_NOCS,
    .SetDefunct                   = _vxevent_eventapi__set_defunct_NOCS
  },

  .BacklogInfo                    = _vxevent_eventapi__backlog_info,
  .FormatBacklogInfo              = _vxevent_eventapi__format_backlog_info,

  .ScheduleVertexEvent = {
    .Expiration_WL                = _vxevent_eventapi__schedule_vertex_event_expiration_WL
  },

  .CancelVertexEvent = {
    .RemoveSchedule_WL            = _vxevent_eventapi__remove_schedule_WL,
    .ImmediateDrop_CS_WL_NT       = _vxevent_eventapi__drop_vertex_event_CS_WL_NT
  },

  .ExistsInSchedule_WL_NT         = _vxevent_eventapi__exists_in_schedule_WL_NT,
  .ExecutorThreadId               = _vxevent_eventapi__executor_thread_id

};

__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "EV::API(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define EVENTAPI_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTAPI_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTAPI_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTAPI_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTAPI_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTAPI_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_t * __new_event_map_CS( vgx_Graph_t *self, __map_term term ) {
  const char *graph_name = CStringValue( CALLABLE(self)->Name(self) );
  framehash_t *event_map = NULL;
  CString_t *CSTR__name = NULL;
  XTRY {

    // Create or restore the index
    int8_t map_order = 0;
    vgx_mapping_spec_t map_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;
    switch( term ) {
    case LONG_TERM:
      map_order = g_pevent_param->LongTerm.map_order;
      CSTR__name = CStringNewFormat( "event_long_[%d]_[%s]", map_order, graph_name );
      break;
    case MEDIUM_TERM:
      map_order = g_pevent_param->MediumTerm.map_order;
      CSTR__name = CStringNewFormat( "event_med_[%d]_[%s]", map_order, graph_name );
      break;
    case SHORT_TERM:
      map_order = g_pevent_param->ShortTerm.map_order;
      CSTR__name = CStringNewFormat( "event_short_[%d]_[%s]", map_order, graph_name );
      break;
    default:
      THROW_ERROR( CXLIB_ERR_API, 0x101 );
    }

    if( (event_map = iMapping.NewMap( NULL, CSTR__name, MAPPING_SIZE_UNLIMITED, (int8_t)map_order, map_spec, CLASS_NONE )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x102 );
    }

  }
  XCATCH( errcode ) {
    iMapping.DeleteMap( &event_map );
  }
  XFINALLY {
    if( CSTR__name ) {
      CStringDelete( CSTR__name );
    }
  }

  return event_map;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __init_eventproc_parameters( vgx_eventproc_parameters_t *params ) {

  vgx_eventproc_parameters_t default_params = {0};
  default_params.operation_timeout_ms = 0;
  default_params.task_WL.state = VGX_EVENTPROC_STATE_INITIALIZE;

  // Initialize
  *params = default_params;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__initialize( vgx_Graph_t *self, bool run_daemon ) {
  int ret = 0;

  GRAPH_LOCK( self ) {
    XTRY {
      vgx_EventProcessor_t *processor = &self->EVP;

      // Zero all
      memset( processor, 0, sizeof( vgx_EventProcessor_t ) );

      // [Q1.2] Parent graph
      processor->graph = self;

      if( run_daemon ) {
        // [Q1.1] Task
        if( __create_eventproc_task( processor ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x111 );
        }

        // [Q1.3] Execution job descriptor
        processor->EXECUTOR = NULL;

        // [Q1.4.1] Executor thread ID
        processor->executor_thread_id = 0;

        // [Q1.5] Parameters
        __init_eventproc_parameters( &processor->params );

        // [Q1.6] Create long term event map
        if( (processor->Schedule.LongTerm = __new_event_map_CS( self, LONG_TERM )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x113 );
        }

        // [Q1.7] Create medium term event map
        if( (processor->Schedule.MediumTerm = __new_event_map_CS( self, MEDIUM_TERM )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x114 );
        }

        // [Q1.8] Create short term event map
        if( (processor->Schedule.ShortTerm = __new_event_map_CS( self, SHORT_TERM )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x115 );
        }

        // Constructor args for queues
        Cm128iQueue_constructor_args_t eventproc_queue_args = { .element_capacity = 1, .comparator = NULL };

        // [Q2.1] Monitor queue
        if( (processor->Monitor.Queue = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &eventproc_queue_args )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x112 );
        }

        // [Q2.2] Imminent execution queue
        if( (processor->Executor.Queue = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &eventproc_queue_args )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x116 );
        }

        // [Q2.3] Action triggers
        processor->ActionTriggers = NULL;

        // Public API
        // [Q3] Lock
        INIT_CRITICAL_SECTION( &processor->PublicAPI.Lock.lock );

        // [Q4.1] Input queue
        if( (processor->PublicAPI.Queue = COMLIB_OBJECT_NEW( Cm128iQueue_t, NULL, &eventproc_queue_args )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x111 );
        }


        // [Q1.4.2] 
        processor->ready = true;
      }

      // [Q2.4-8]
      processor->__rsv_2_4 = 0;
      processor->__rsv_2_5 = 0;
      processor->__rsv_2_6 = 0;
      processor->__rsv_2_7 = 0;
      processor->__rsv_2_8 = 0;

      // [Q4.2-8]
      processor->PublicAPI.__rsv_4_2 = 0;
      processor->PublicAPI.__rsv_4_3 = 0;
      processor->PublicAPI.__rsv_4_4 = 0;
      processor->PublicAPI.__rsv_4_5 = 0;
      processor->PublicAPI.__rsv_4_6 = 0;
      processor->PublicAPI.__rsv_4_7 = 0;
      processor->PublicAPI.__rsv_4_8 = 0;


    }
    XCATCH( errcode ) {
      _vxevent_eventapi__destroy( self );
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
static int _vxevent_eventapi__start( vgx_Graph_t *self, bool enable ) {
  int ret = 0;
  vgx_EventProcessor_t *processor = &self->EVP;
  if( _vxevent_eventapi__is_ready( self ) ) {
    processor->graph = self;
    ret = __start_eventproc( processor, enable );;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__destroy( vgx_Graph_t *self ) {
  int ret = 0;
  vgx_EventProcessor_t *processor = &self->EVP;
  if( processor->TASK ) {
    ret = __stop_delete_eventproc( processor );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__flush_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int flushed = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {

    EVENTAPI_VERBOSE( self, 0x121, "Flush requested" );

    // First acquire the processor
    vgx_EventProcessor_t *processor_WL = NULL;
    GRAPH_LOCK( self ) {
      BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
        BEGIN_EXECUTION_BLOCKED_WHILE( processor_WL == NULL ) {
          GRAPH_SUSPEND_LOCK( self ) {
            processor_WL = __acquire_eventproc( &self->EVP, 100 );
          } GRAPH_RESUME_LOCK;
        } END_EXECUTION_BLOCKED_WHILE;
      } END_EXECUTE_WITH_TIMING_BUDGET_CS;
    } GRAPH_RELEASE;

    // Proceed 
    if( processor_WL != NULL ) {
      // Set the flush request
      __set_req_flush_WL( processor_WL, (int)timing_budget->t_remain_ms );
      // Release the processor
      __release_eventproc( &processor_WL );
      // Wait for flush confirmation
      GRAPH_LOCK( self ) {
        BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
          bool defunct = false;
          BEGIN_EXECUTION_BLOCKED_WHILE( flushed == 0 && !defunct ) {
            GRAPH_SUSPEND_LOCK( self ) {
              // Give it time to work
              BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
                sleep_milliseconds( 10 );
              } END_WAIT_FOR_EXECUTION_RESOURCE;
              // Check
              if( (processor_WL = __acquire_eventproc( &self->EVP, 100 )) != NULL ) {
                flushed = __consume_is_ack_flushed_WL( processor_WL );
                defunct = __eventproc_state_is_defunct_WL( processor_WL );
                __release_eventproc( &processor_WL );
              }
            } GRAPH_RESUME_LOCK;
          } END_EXECUTION_BLOCKED_WHILE;
          // Detect defunct state
          if( defunct ) {
            flushed = -1;
          }
        } END_EXECUTE_WITH_TIMING_BUDGET_CS;

        // Timeout
        if( flushed == 0 ) {
          __consume_is_req_flush_WL( processor_WL ); // clear
          __consume_is_ack_flushed_WL( processor_WL ); // clear
          EVENTAPI_WARNING( self, 0x122, "Unable to flush (timed out)" );
          flushed = -1;
        }
        else if( flushed > 0 ) {
          EVENTAPI_VERBOSE( self, 0x123, "Flush completed" );
        }
        else {
          EVENTAPI_REASON( self, 0x124, "Event processor not running" );
        }
      } GRAPH_RELEASE;
    }
  }

  return flushed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__schedule( vgx_Graph_t *self ) {
  int ret = 0;
  if( _vxevent_eventapi__is_ready( self ) ) {
    vgx_EventProcessor_t *processor_WL = __acquire_eventproc( &self->EVP, 0 );
    if( processor_WL ) {
      ret = __schedule_events_WL( processor_WL, NULL );
      __release_eventproc( &processor_WL );
    }
    else {
      ret = -1;
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__enable_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int enabled = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {

    EVENTAPI_VERBOSE( self, 0x131, "Waiting to resume event processor" );

    GRAPH_LOCK( self ) {
      if( _vgx_is_writable_CS( &self->readonly ) ) {
        // Disallow graph readonly mode for as long as we try to enable event processor
        BEGIN_DISALLOW_READONLY_CS( &self->readonly ) {
          GRAPH_SUSPEND_LOCK( self ) {
            comlib_task_t *task = self->EVP.TASK;
            // Make sure task exists and is running
            if( task && COMLIB_TASK__IsAlive( task ) ) {
              // Proceed if suspended
              if( COMLIB_TASK__IsSuspended( task ) ) {
                // Request resume
                COMLIB_TASK__Request_Resume( task );
                // Wait
                GRAPH_LOCK( self ) {
                  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
                    bool defunct = false;
                    BEGIN_EXECUTION_BLOCKED_WHILE( enabled == 0 && !defunct ) {
                      GRAPH_SUSPEND_LOCK( self ) {
                        // Give it time to work
                        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
                          sleep_milliseconds( 10 );
                        } END_WAIT_FOR_EXECUTION_RESOURCE;
                        // Check
                        enabled = COMLIB_TASK__IsNotSuspended( task );
                        defunct = COMLIB_TASK__IsDead( task );
                      } GRAPH_RESUME_LOCK;
                    } END_EXECUTION_BLOCKED_WHILE;
                    // Detect defunct state
                    if( defunct ) {
                      enabled = -1;
                    }
                  } END_EXECUTE_WITH_TIMING_BUDGET_CS;
                  // Timeout
                  if( enabled == 0 ) {
                    EVENTAPI_WARNING( self, 0x132, "Timed out while waiting to resume. Processor may resume at any time after this." );
                    enabled = -1;
                  }
                  else if( enabled > 0 ) {
                    if( iOperation.State_CS.Events( self ) < 0 ) {
                      EVENTAPI_WARNING( self, 0x133, "Resume operation not captured (graph operation closed)" );
                    }
                    EVENTAPI_VERBOSE( self, 0x134, "Event processor resumed" );
                  }
                  else {
                    EVENTAPI_REASON( self, 0x135, "Event processor stopped, unable to resume." );
                  }
                } GRAPH_RELEASE;
              }
              // No action, processor already running
              else {
                EVENTAPI_VERBOSE( self, 0x136, "Event processor already running" );
              }
            }
            // Processor task is not running
            else {
              EVENTAPI_VERBOSE( self, 0x137, "Defunct event processor" );
            }
          } GRAPH_RESUME_LOCK;
        } END_DISALLOW_READONLY_CS;
      }
      else {
        EVENTAPI_WARNING( self, 0x138, "Unable to resume event processor (graph is readonly)" );
      }
    } GRAPH_RELEASE;
  }

  return enabled;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__is_ready( vgx_Graph_t *self ) {
  return self->EVP.ready;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__is_enabled( vgx_Graph_t *self ) {
  int enabled = 0;
  if( _vxevent_eventapi__is_ready( self ) ) {
    comlib_task_t *task = self->EVP.TASK;
    if( task && COMLIB_TASK__IsAlive( task ) ) {
      enabled = COMLIB_TASK__IsNotSuspended( task );
    }
  }
  return enabled;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__disable_NOCS( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int disabled = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {

    EVENTAPI_VERBOSE( self, 0x141, "Waiting to suspend event processor" );

    // Ensure at least 5 seconds here
    const int minimum_ms = 5000;
    int64_t remain_ms = _vgx_get_execution_timing_budget_remaining( timing_budget );
    if( remain_ms < minimum_ms ) {
      _vgx_add_execution_timing_budget( timing_budget, minimum_ms - (int)remain_ms );
    }

    GRAPH_LOCK( self ) {
      GRAPH_SUSPEND_LOCK( self ) {
        comlib_task_t *task = self->EVP.TASK;
        // Make sure task exists and is running
        if( task && COMLIB_TASK__IsAlive( task ) ) {
          // Proceed if enabled
          if( COMLIB_TASK__IsNotSuspended( task ) ) {
            // Request suspend
            COMLIB_TASK__Request_Suspend( task );
            // Wait
            GRAPH_LOCK( self ) {
              BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
                bool defunct = false;
                BEGIN_EXECUTION_BLOCKED_WHILE( disabled == 0 && !defunct ) {
                  GRAPH_SUSPEND_LOCK( self ) {
                    // Give it time to work
                    BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
                      sleep_milliseconds( 10 );
                    } END_WAIT_FOR_EXECUTION_RESOURCE;
                    // Check
                    disabled = COMLIB_TASK__IsSuspended( task );
                    defunct = COMLIB_TASK__IsDead( task );
                  } GRAPH_RESUME_LOCK;
                } END_EXECUTION_BLOCKED_WHILE;
                // Detect defunct state
                if( defunct ) {
                  disabled = -1;
                }
              } END_EXECUTE_WITH_TIMING_BUDGET_CS;
              // Timeout
              if( disabled == 0 ) {
                EVENTAPI_WARNING( self, 0x142, "Timed out while waiting to suspend. Processor may suspend at any time after this." );
                disabled = -1;
              }
              else if( disabled > 0 ) {
                if( iOperation.IsOpen( &self->operation ) ) {
                  iOperation.State_CS.NoEvents( self );
                }
                else if( iOperation.Emitter_CS.IsRunning( self ) ) {
                  EVENTAPI_WARNING( self, 0x143, "Suspend operation not captured (graph operation closed)" );
                }
                EVENTAPI_VERBOSE( self, 0x144, "Event processor suspended" );
              }
              else {
                EVENTAPI_REASON( self, 0x145, "Processor stopped, unable to suspend." );
              }
            } GRAPH_RELEASE;
          }
          // Already suspended
          else {
            EVENTAPI_VERBOSE( self, 0x146, "Event processor already suspended" );
          }
        } // Processor not running
        else {
          EVENTAPI_INFO( self, 0x147, "Event processor not running" );
          disabled = 1;
        }
      } GRAPH_RESUME_LOCK;
    } GRAPH_RELEASE;
  }

  return disabled;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__set_defunct_NOCS( vgx_Graph_t *self, int timeout_ms ) {
  int ret = 0;
  if( _vxevent_eventapi__is_ready( self ) ) {
    vgx_EventProcessor_t *processor = &self->EVP;
    ret = __eventproc_state_set_defunct( processor, timeout_ms );
  }
  return ret;
}



/*******************************************************************//**
 * Acquire the event processor lock
 ***********************************************************************
 */
static vgx_EventProcessor_t * _vxevent_eventapi__acquire( vgx_Graph_t *self, int timeout_ms ) {
  vgx_EventProcessor_t *processor_WL = NULL;
  if( _vxevent_eventapi__is_ready( self ) ) {
    processor_WL = __acquire_eventproc( &self->EVP, timeout_ms );
  }
  return processor_WL;
}



/*******************************************************************//**
 * Release the event processor lock.
 ***********************************************************************
 */
static int _vxevent_eventapi__release( vgx_EventProcessor_t **processor_WL ) { 
  int ret = __release_eventproc( processor_WL );
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_EventBacklogInfo_t _vxevent_eventapi__backlog_info( vgx_Graph_t *self ) {
  vgx_EventBacklogInfo_t backlog = {0};

  if( _vxevent_eventapi__is_ready( self ) ) {

    // Get endpoint state (api + exec)
    backlog.n_api = __length_Qapi( self );

    vgx_EventProcessor_t *processor_WL = NULL;

    // Get event processor's internal state
    if( (processor_WL = __acquire_eventproc( &self->EVP, 2000 )) != NULL ) {
      comlib_task_t *task = processor_WL->TASK;
      backlog.param = *g_pevent_param;
      backlog.flags.is_running = task != NULL && COMLIB_TASK__IsAlive( task );
      backlog.flags.is_paused = COMLIB_TASK__IsSuspended( task );

      if( processor_WL->EXECUTOR && processor_WL->EXECUTOR->TASK ) {
        COMLIB_TASK_LOCK( processor_WL->EXECUTOR->TASK ) {
          backlog.n_exec = processor_WL->EXECUTOR->n_exec;
          backlog.ontime_rate = processor_WL->EXECUTOR->ontime_rate;
          backlog.n_current = processor_WL->EXECUTOR->n_current;
        } COMLIB_TASK_RELEASE;
      }
      else {
        backlog.n_exec = 0;
        backlog.ontime_rate = 0.0f;
        backlog.n_current = (int)LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue );
      }

      backlog.n_input = LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Monitor.Queue );
      backlog.n_long = EVENTMAP_SIZE( processor_WL->Schedule.LongTerm );
      backlog.n_med = EVENTMAP_SIZE( processor_WL->Schedule.MediumTerm );
      backlog.n_short = EVENTMAP_SIZE( processor_WL->Schedule.ShortTerm );
      __release_eventproc( &processor_WL );
      backlog.filled = true;
    }
  }

  return backlog;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CString_t * _vxevent_eventapi__format_backlog_info( vgx_Graph_t *self, vgx_EventBacklogInfo_t *backlog ) {

  const char *name = CStringValue( CALLABLE(self)->Name(self) );
  vgx_EventBacklogInfo_t local;
  if( backlog == NULL ) {
    if( _vxevent_eventapi__is_ready( self ) ) {
      local = _vxevent_eventapi__backlog_info( self );
      backlog = &local;
    }
    else {
      return CStringNewFormat( "Event Processor Backlog(%s): Graph has no event processor", name );
    }
  }

  const char *runstate = backlog->flags.is_running ? (backlog->flags.is_paused ? "PAUSED" : "RUNNING") : "HALTED";
  char late[32] = {0};
  if( backlog->n_exec > 0 && backlog->ontime_rate < 1.0 ) {
    float late_pct = 100.0f * (1.0f - backlog->ontime_rate);
    snprintf( late, 31, "(%7.4f%% late)", late_pct );
  }

  // EVP internal busy
  if( backlog->filled == false ) {
    return CStringNewFormat( "Event Processor Backlog(%s): IN[ %lld ] -> SCH[ *BUSY* ] -> EXEC[ %lld ] <%s>",
                                                      name,
                                                               backlog->n_api,
                                                                                              backlog->n_exec,
                                                                                                        runstate
                           );
  }
  // All data retrieved
  else {
    // Event Processor Backlog(g): IN[ 0 ] -> SCH[ {L:0 M:0 S:0} -> X:0 ] -> EXEC[ 0 ]
    return CStringNewFormat( "Event Processor Backlog(%s): IN[ %lld ] -> SCH[ {L:%lld M:%lld S:%lld} -> X:%lld ] -> EXEC[ %lld %s] <%s>",
                                                      name,
                                                               backlog->n_api,
                                                                                 backlog->n_long,
                                                                                        backlog->n_med,
                                                                                               backlog->n_short,
                                                                                                          backlog->n_current,
                                                                                                                          backlog->n_exec,
                                                                                                                                late,
                                                                                                                                  runstate
                           );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __enqueue_event( vgx_Graph_t *self, vgx_VertexStorableEvent_t *ev ) {
  int n;
  SYNCHRONIZE_ON( self->EVP.PublicAPI.Lock ) {
    n = APPEND_QUEUE_VERTEX_EVENT_NOLOCK( self->EVP.PublicAPI.Queue, ev );
  } RELEASE;
  return n;
}



/*******************************************************************//**
 *
 * Schedule an expiration event for the vertex at (or shortly after) the
 * given execution timestamp.
 *
 * The event processor system now becomes an independent owner of the vertex and
 * only gives up ownership after the scheduled event has been executed.
 *
 * The vertex is placed into a processing queue for the event monitor 
 * to pick up when it runs.
 *
 ***********************************************************************
 */
static int _vxevent_eventapi__schedule_vertex_event_expiration_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, uint32_t tmx ) {
  int ret = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {
    // Schedule event (if ts=never then schedule event for immediate execution to clear it out)
    uint32_t ts_exec = tmx < TIME_EXPIRES_NEVER ? tmx : TIMESTAMP_MIN;

    // Queue expiration event
    vgx_VertexStorableEvent_t ev = __get_vertex_expiration_event( vertex_WL, ts_exec );
    if( __enqueue_event( self, &ev ) > 0 ) {
      // Mark vertex as being owned by the Event Processor (unless already owned)
      if( !__vertex_has_event_scheduled( vertex_WL ) ) {
        __vertex_set_event_scheduled( vertex_WL );
        Vertex_INCREF_WL( vertex_WL );
      }
    }
    else {
      ret = -1;
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
__inline static vgx_VertexStorableEvent_t __get_remove_schedule_event( vgx_Vertex_t *vertex_WL ) {
  vgx_VertexStorableEvent_t ev = {0};
  ev.event_key = _cxmalloc_object_as_handle( vertex_WL ),   // Convert vertex pointer to handle so we can store it
  ev.event_val.ts_exec = TIME_EXPIRES_NEVER;
  ev.event_val.type = VGX_EVENTPROC_VERTEX_REMOVE_SCHEDULE;
  return ev;
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static void _vxevent_eventapi__remove_schedule_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  if( _vxevent_eventapi__is_ready( self ) ) {
    // Drop event for this vertex if one exists
    if( __vertex_has_event_scheduled( vertex_WL ) ) {
      __vertex_clear_event_scheduled( vertex_WL );
      Vertex_DECREF_WL( vertex_WL );
    }

    // Queue the event to trigger internal schedule removal
    vgx_VertexStorableEvent_t ev = __get_remove_schedule_event( vertex_WL );
    __enqueue_event( self, &ev );
  }
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static void _vxevent_eventapi__drop_vertex_event_CS_WL_NT( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  if( _vxevent_eventapi__is_ready( self ) ) {
    // Vertex is indeed scheduled - proceeed with drastic measures to drop vertex from schedule
    if( __vertex_has_event_scheduled( vertex_WL ) ) {
      // Clear event scheduled flag
      __vertex_clear_event_scheduled( vertex_WL );
      __vertex_all_clear_expiration( vertex_WL );
      Vertex_DECREF_WL( vertex_WL );
    }

    // Forcefully remove event from all schedules.
    // (This is ok to do here because it is assumed we have suspended the event processor and also have acquired the event processor)
    vgx_VertexStorableEvent_t ev = __get_remove_schedule_event( vertex_WL );
    EVENTMAP_DELETE( self->EVP.Schedule.LongTerm, &ev );    // may or may not remove anything
    EVENTMAP_DELETE( self->EVP.Schedule.MediumTerm, &ev );     // may or may not remove anything
    EVENTMAP_DELETE( self->EVP.Schedule.ShortTerm, &ev );   // may or may not remove anything
  }
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static int _vxevent_eventapi__exists_in_schedule_WL_NT( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  int occ = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {
    vgx_VertexStorableEvent_t ev = {
      .event_key = _cxmalloc_object_as_handle( vertex_WL ),
      .event_val = {0}
    };

    occ += EVENTMAP_CONTAINS( self->EVP.Schedule.LongTerm, &ev );
    occ += EVENTMAP_CONTAINS( self->EVP.Schedule.MediumTerm, &ev );
    occ += EVENTMAP_CONTAINS( self->EVP.Schedule.ShortTerm, &ev );
  }

  return occ;
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static uint32_t _vxevent_eventapi__executor_thread_id( vgx_Graph_t *self ) {
  uint32_t tid = 0;

  if( _vxevent_eventapi__is_ready( self ) ) {
    GRAPH_LOCK( self ) {
      tid = self->EVP.executor_thread_id;
    } GRAPH_RELEASE;
  }

  return tid;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxevent_eventapi.h"

test_descriptor_t _vgx_vxevent_eventapi_tests[] = {
  { "VGX Event API Tests", __utest_vxevent_eventapi },

  {NULL}
};
#endif
