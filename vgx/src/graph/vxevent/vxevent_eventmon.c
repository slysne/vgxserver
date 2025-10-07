/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxevent_eventmon.c
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

#include "_vxevent.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "EV::MON(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define EVENTMONITOR_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTMONITOR_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTMONITOR_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTMONITOR_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTMONITOR_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define EVENTMONITOR_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )



#define COMPLETION_THRESHOLD              2     // 2s             x
#define EXE_INSERTION_THRESHOLD           30    // 30s            x15
#define SHORT_INSERTION_THRESHOLD         480   // 480s = 8m      x16
#define MEDIUM_INSERTION_THRESHOLD        7680  // 7680s = 2h8m   x16

#define COMPLETION_DEADLINE               0
#define EXE_ARRIVAL_DEADLINE              (COMPLETION_THRESHOLD - COMPLETION_DEADLINE)            // 2s
#define SHORT_ARRIVAL_DEADLINE            (EXE_INSERTION_THRESHOLD - EXE_ARRIVAL_DEADLINE)        // 30-2 = 28s 
#define MEDIUM_ARRIVAL_DEADLINE           (SHORT_INSERTION_THRESHOLD - SHORT_ARRIVAL_DEADLINE)    // 480 - 28 = 452 = 7m32s
#define LONG_ARRIVAL_DEADLINE             (MEDIUM_INSERTION_THRESHOLD - MEDIUM_ARRIVAL_DEADLINE)  // 7680 - 452 = 7228 = 2h0m28s
static const int exe_arrival_deadline     = EXE_ARRIVAL_DEADLINE;
static const int short_arrival_deadline   = SHORT_ARRIVAL_DEADLINE;
static const int medium_arrival_deadline  = MEDIUM_ARRIVAL_DEADLINE;
static const int long_arrival_deadline    = LONG_ARRIVAL_DEADLINE;


#define _900_MS                           900LL                                   // Not quite 1 second, use to shorten cycles to leave extra safety margin

// LONG
#define LONG_INSERTION_THRESHOLD          LLONG_MAX                               //
#define LONG_MIGRATION_CYCLE_MS           (LONG_ARRIVAL_DEADLINE * _900_MS)       // Long -> Medium complete migration cycle
#define LONG_MIGRATION_MARGIN_MS          (MEDIUM_ARRIVAL_DEADLINE * 1000LL)      //
#define LONG_MAP_ORDER                    10                                      // 2**10 = 1024 partitions
static const int64_t long_migration_cycle_ms = LONG_MIGRATION_CYCLE_MS;

// MEDIUM
#define MEDIUM_INSERTION_THRESHOLD_MS     (MEDIUM_INSERTION_THRESHOLD * 1000LL)   //
#define MEDIUM_MIGRATION_CYCLE_MS         (MEDIUM_ARRIVAL_DEADLINE * _900_MS)     // Medium -> Short complete migration cycle
#define MEDIUM_MIGRATION_MARGIN_MS        (SHORT_ARRIVAL_DEADLINE * 1000LL)       //
#define MEDIUM_MAP_ORDER                  7                                       // 2**7 = 128 partitions
static const int64_t medium_migration_cycle_ms = MEDIUM_MIGRATION_CYCLE_MS;

// SHORT
#define SHORT_INSERTION_THRESHOLD_MS      (SHORT_INSERTION_THRESHOLD * 1000LL)    //
#define SHORT_MIGRATION_CYCLE_MS          (SHORT_ARRIVAL_DEADLINE * _900_MS)      // Short -> Executor complete migration cycle
#define SHORT_MIGRATION_MARGIN_MS         (EXE_ARRIVAL_DEADLINE * 1000LL)         //
#define SHORT_MAP_ORDER                   4                                       // 2**4 = 16 partitions
static const int64_t short_migration_cycle_ms = SHORT_MIGRATION_CYCLE_MS;

// EXECUTOR
#define EXECUTOR_INSERTION_THRESHOLD_MS   EXE_INSERTION_THRESHOLD * 1000LL        //


static vgx_EventParamInfo_t default_event_param = {
  .LongTerm = {
    .insertion_threshold_tms = LONG_INSERTION_THRESHOLD,
    .migration_cycle_tms     = LONG_MIGRATION_CYCLE_MS,
    .migration_margin_tms    = LONG_MIGRATION_MARGIN_MS,
    .map_order               = LONG_MAP_ORDER,
    .partials                = 1 << LONG_MAP_ORDER,
    .partial_interval_tms    = (int64_t)(LONG_MIGRATION_CYCLE_MS / (double)(1 << LONG_MAP_ORDER))
  },
  .MediumTerm = {
    .insertion_threshold_tms = MEDIUM_INSERTION_THRESHOLD_MS,
    .migration_cycle_tms     = MEDIUM_MIGRATION_CYCLE_MS,
    .migration_margin_tms    = MEDIUM_MIGRATION_MARGIN_MS,
    .map_order               = MEDIUM_MAP_ORDER,
    .partials                = 1 << MEDIUM_MAP_ORDER,
    .partial_interval_tms    = (int64_t)(MEDIUM_MIGRATION_CYCLE_MS / (double)(1 << MEDIUM_MAP_ORDER))
  },
  .ShortTerm = {
    .insertion_threshold_tms = SHORT_INSERTION_THRESHOLD_MS,
    .migration_cycle_tms     = SHORT_MIGRATION_CYCLE_MS,
    .migration_margin_tms    = SHORT_MIGRATION_MARGIN_MS,
    .map_order               = SHORT_MAP_ORDER,
    .partials                = 1 << SHORT_MAP_ORDER,
    .partial_interval_tms    = (int64_t)(SHORT_MIGRATION_CYCLE_MS / (double)(1 << SHORT_MAP_ORDER))
  },
  .Executor = {
    .insertion_threshold_tms = EXECUTOR_INSERTION_THRESHOLD_MS,
    .migration_cycle_tms     = 0,
    .migration_margin_tms    = 0,
    .map_order               = 0,
    .partials                = 1, 
    .partial_interval_tms    = 0
  }
};


// Event parameters
vgx_EventParamInfo_t *g_pevent_param = &default_event_param;


// Schedule
static const int64_t g_schedule_interval_tms = 1000;



// Event Processor Action Triggers
static int64_t __refresh_action_triggers_WL( vgx_EventProcessor_t *processor_WL );
static void __reset_action_triggers_WL( vgx_EventProcessor_t *processor_WL );
static void __delete_action_triggers_WL( vgx_EventProcessor_t *processor_WL );
static vgx_EventActionTrigger_t ** __new_action_triggers_WL( vgx_EventProcessor_t *processor_WL, int count );
static int __set_action_trigger_WL( vgx_EventProcessor_t *processor_WL, int trigger_slot, vgx_EventActionTrigger_t *source );
static int __perform_eventproc_action_WL( vgx_EventActionTrigger_t *trigger );
static int __eventproc_action__Schedule_WL( vgx_EventActionTrigger_t *trigger );
static int __eventproc_action__MigrateLong_WL( vgx_EventActionTrigger_t *trigger );
static int __eventproc_action__MigrateMedium_WL( vgx_EventActionTrigger_t *trigger );
static int __eventproc_action__Execute_WL( vgx_EventActionTrigger_t *trigger );

static int64_t __medium_term_cutoff_ms( void );
static int64_t __short_term_cutoff_ms( void );
static int64_t __executor_cutoff_ms( void );

// Migrate
static int64_t __queue_migration_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __migrate_schedule_WL( vgx_EventProcessor_t *processor_WL, uint32_t cutoff_ts, framehash_t *src, framehash_t *dest, uint64_t subtree_selector );
static int64_t __migrate_schedules_full_WL( vgx_EventProcessor_t *processor_WL );

// Schedule
static int __insert_event_into_schedule_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexStorableEvent_t *ev, uint32_t executor_cutoff_ts, uint32_t short_term_cutoff_ts, uint32_t medium_term_cutoff_ts );
static int __remove_event_from_schedule_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexStorableEvent_t *ev );

// Execute
static int64_t __has_executable_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int __has_executable_events( framehash_t *schedule, uint64_t selector );
static int64_t __queue_execution_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __reschedule_pending_events_imminent_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexEventQueue_t *Qpen );
static int64_t __cancel_execution_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job, int timeout_ms );
static int __transfer_scheduled_events_to_execution_job_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job, uint64_t subtree_selector );
static int __transfer_imminent_events_to_execution_job_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __refresh_action_triggers_WL( vgx_EventProcessor_t *processor_WL ) {
  int64_t tms_now = __MILLISECONDS_SINCE_1970();
  vgx_EventActionTrigger_t **cursor = processor_WL->ActionTriggers;
  vgx_EventActionTrigger_t *trigger;
  while( (trigger = *cursor++) != NULL ) {
    trigger->tms.now = tms_now;
    trigger->tms.delta = (int64_t)round( trigger->schedule.time_factor * (*trigger->schedule.interval) );
  }
  return tms_now;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __reset_action_triggers_WL( vgx_EventProcessor_t *processor_WL ) {
  vgx_EventActionTrigger_t **cursor = processor_WL->ActionTriggers;
  vgx_EventActionTrigger_t *trigger;
  while( (trigger = *cursor++) != NULL ) {
    trigger->tms.at = trigger->tms.now;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_action_triggers_WL( vgx_EventProcessor_t *processor_WL ) {
  if( processor_WL->ActionTriggers ) {
    vgx_EventActionTrigger_t **cursor = processor_WL->ActionTriggers;
    vgx_EventActionTrigger_t *trigger;
    while( (trigger = *cursor++) != NULL ) {
      free( trigger );
    }
    free( processor_WL->ActionTriggers );
    processor_WL->ActionTriggers = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_EventActionTrigger_t ** __new_action_triggers_WL( vgx_EventProcessor_t *processor_WL, int count ) {
  if( processor_WL->ActionTriggers ) {
    __delete_action_triggers_WL( processor_WL );
  }

  // Allocate the pointers first
  if( (processor_WL->ActionTriggers = calloc( count + 1, sizeof( vgx_EventActionTrigger_t * ) )) == NULL ) {
    return NULL;
  }

  // Allocate triggers
  for( int i=0; i<count; i++ ) {
    if( (processor_WL->ActionTriggers[i] = calloc( 1, sizeof( vgx_EventActionTrigger_t ) )) == NULL ) {
      __delete_action_triggers_WL( processor_WL );
      return NULL;
    }
  }

  return processor_WL->ActionTriggers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_action_trigger_WL( vgx_EventProcessor_t *processor_WL, int trigger_slot, vgx_EventActionTrigger_t *source ) {
  memcpy( processor_WL->ActionTriggers[ trigger_slot ], source, sizeof( vgx_EventActionTrigger_t ) );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __perform_eventproc_action_WL( vgx_EventActionTrigger_t *trigger ) {
  int ret = 0;
  // ----------------------------------
  // Is it time to perform this action?
  // ----------------------------------
  if( trigger->tms.now > trigger->tms.at ) {
    // Perform the action
    ret = trigger->action.perform( trigger );

    // Increment the trigger counter
    trigger->action.counter++;

    // Set the time for next trigger (regular ticks)
    trigger->tms.at += trigger->tms.delta;
    
    // If we missed tick deadline(s) we skip ahead to the next on the grid (if allowed)
    if( trigger->schedule.can_skip_tick ) {
      if( trigger->tms.at < trigger->tms.now ) {
        trigger->tms.at = trigger->tms.now + trigger->tms.delta;
      }
    }
    
    if( trigger->schedule.post_action_sleep_ms > 0 ) {
      // Short nap after action
      sleep_milliseconds( trigger->schedule.post_action_sleep_ms );
    }
  }

  return ret;
}



/*******************************************************************//**
 * Action: MigrateLong
 *
 ***********************************************************************
 */
static int __eventproc_action__MigrateLong_WL( vgx_EventActionTrigger_t *trigger ) {
  int ret = 1;
  vgx_EventProcessor_t *processor_WL = (vgx_EventProcessor_t*)trigger->action.input;
  // Migrate any events from the long term schedule to the medium term schedule
  __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_MIGRATE_LONG );
  uint32_t cutoff_ts = (uint32_t)(__medium_term_cutoff_ms() / 1000);
  if( __migrate_schedule_WL( processor_WL, cutoff_ts, processor_WL->Schedule.LongTerm, processor_WL->Schedule.MediumTerm, trigger->action.counter ) < 0 ) {
    EVENTMONITOR_CRITICAL( processor_WL->graph, 0x201, "Long term event migration failed" );
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 * Action: MigrateMedium
 *
 ***********************************************************************
 */
static int __eventproc_action__MigrateMedium_WL( vgx_EventActionTrigger_t *trigger ) {
  int ret = 1;
  vgx_EventProcessor_t *processor_WL = (vgx_EventProcessor_t*)trigger->action.input;
  // Migrate any events from the medium term schedule to the short term schedule
  __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_MIGRATE_MEDIUM );
  uint32_t cutoff_ts = (uint32_t)(__short_term_cutoff_ms() / 1000);
  if( __migrate_schedule_WL( processor_WL, cutoff_ts, processor_WL->Schedule.MediumTerm, processor_WL->Schedule.ShortTerm, trigger->action.counter ) < 0 ) {
    EVENTMONITOR_CRITICAL( processor_WL->graph, 0x211, "Medium term event migration failed" );
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 * Action: Schedule
 *
 ***********************************************************************
 */
static int __eventproc_action__Schedule_WL( vgx_EventActionTrigger_t *trigger ) {
  vgx_EventProcessor_t *processor_WL = (vgx_EventProcessor_t*)trigger->action.input;
  vgx_ExecutionJobDescriptor_t **executor = (vgx_ExecutionJobDescriptor_t**)trigger->action.output;

  // Move events from the API input to the internal event schedule
  __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_SCHEDULE );
  return __schedule_events_WL( processor_WL, executor );
}



/*******************************************************************//**
 * Action: Execute
 *
 ***********************************************************************
 */
static int __eventproc_action__Execute_WL( vgx_EventActionTrigger_t *trigger ) {
  int ret = 1;
  vgx_EventProcessor_t *processor_WL = (vgx_EventProcessor_t*)trigger->action.input;
  vgx_ExecutionJobDescriptor_t **executor = (vgx_ExecutionJobDescriptor_t**)trigger->action.output;
  // Transfer all events that are due to execute now from the short term schedule
  // into the background execution job.
  if( __has_executable_events( processor_WL->Schedule.ShortTerm, trigger->action.counter ) ) {
    __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_EXECUTE );
    if( __transfer_scheduled_events_to_execution_job_WL( processor_WL, executor, trigger->action.counter ) < 0 ) {
      EVENTMONITOR_CRITICAL( processor_WL->graph, 0x221, "Failed to transfer events to execution job" );
      ret = -1;
    }
  }
#ifndef NDEBUG
#endif
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __set_operation_timeout_WL( vgx_EventProcessor_t *processor_WL, int timeout_ms ) {
  processor_WL->params.operation_timeout_ms = timeout_ms;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void __set_req_flush_WL( vgx_EventProcessor_t *processor_WL, int timeout_ms ) {
  __set_operation_timeout_WL( processor_WL, timeout_ms );
  processor_WL->params.task_WL.flags.req_flush = 1;
  processor_WL->params.task_WL.flags.ack_flushed = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int __is_req_flush_WL( vgx_EventProcessor_t *processor_WL ) {
  return processor_WL->params.task_WL.flags.req_flush;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int __consume_is_req_flush_WL( vgx_EventProcessor_t *processor_WL ) {
  int flush =  __is_req_flush_WL( processor_WL );
  if( flush ) {
    processor_WL->params.task_WL.flags.req_flush = 0;
  }
  return flush;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void __set_ack_flushed_WL( vgx_EventProcessor_t *processor_WL ) {
  processor_WL->params.task_WL.flags.ack_flushed = 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int __is_ack_flushed_WL( vgx_EventProcessor_t *processor_WL ) {
  return processor_WL->params.task_WL.flags.ack_flushed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int __consume_is_ack_flushed_WL( vgx_EventProcessor_t *processor_WL ) {
  int flushed = __is_ack_flushed_WL( processor_WL );
  if( flushed ) {
    processor_WL->params.task_WL.flags.ack_flushed = 0;
  }
  return flushed;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __event_initialize( comlib_task_t *self ) {
  int ret = 0;

  vgx_EventProcessor_t *EVP = COMLIB_TASK__GetData( self );
  vgx_Graph_t *graph = EVP->graph;

  EVENTMONITOR_VERBOSE( graph, 0, "Starting" );

  vgx_EventProcessor_t *processor_WL;

  XTRY {
    // -------------------------------------------
    // 1: Verify acquisition cycle
    // -------------------------------------------
    if( (processor_WL = __acquire_eventproc( EVP, 10000 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
    }
    if( processor_WL->params.task_WL.state != VGX_EVENTPROC_STATE_BUSY ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x004 );
    }
    if( COMLIB_TASK__Acquisitions( self ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x005 );
    }
    if( __release_eventproc( &processor_WL ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x006 );
    }
    if( processor_WL != NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x007 );
    }

    // -------------------------------------------
    // 2: Acquire
    // -------------------------------------------
    if( (processor_WL = __acquire_eventproc( EVP, 10000 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x008 );
    }

    // -------------------------------------------
    // 3: Migrate schedules
    // -------------------------------------------
    __migrate_schedules_full_WL( processor_WL );

    // -------------------------------------------
    // 4: Action triggers
    // -------------------------------------------
    if( __new_action_triggers_WL( processor_WL, 4 ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x009 );
    }

    // Action 1:
    // Migrate events from long term to medium term schedule.
    vgx_EventActionTrigger_t MigrateLong = {
      .tms = {
        .now                  = 0,
        .at                   = 0,
        .delta                = 0
      },
      .action = {
        .input                = processor_WL,
        .output               = NULL,
        .perform              = __eventproc_action__MigrateLong_WL,
        .counter              = 0
      },
      .schedule = {
        .time_factor          = 1.0 / g_pevent_param->LongTerm.partials,
        .interval             = &g_pevent_param->LongTerm.migration_cycle_tms,
        .post_action_sleep_ms = 0,
        .can_skip_tick        = false
      }
    };
    __set_action_trigger_WL( processor_WL, 0, &MigrateLong );


    // Action 2:
    // Migrate events from medium term to short term schedule.
    vgx_EventActionTrigger_t MigrateMedium = {
      .tms = {
        .now                  = 0,
        .at                   = 0,
        .delta                = 0
      },
      .action = {
        .input                = processor_WL,
        .output               = NULL,
        .perform              = __eventproc_action__MigrateMedium_WL,
        .counter              = 0
      },
      .schedule = {
        .time_factor          = 1.0 / g_pevent_param->MediumTerm.partials,
        .interval             = &g_pevent_param->MediumTerm.migration_cycle_tms,
        .post_action_sleep_ms = 0,
        .can_skip_tick        = false
      }
    };
    __set_action_trigger_WL( processor_WL, 1, &MigrateMedium );


    // Action 3:
    // Schedule events to be executed in the future
    vgx_EventActionTrigger_t Schedule = {
      .tms = {
        .now                  = 0,
        .at                   = 0,
        .delta                = 0
      },
      .action = {
        .input                = processor_WL,
        .output               = &processor_WL->EXECUTOR,
        .perform              = __eventproc_action__Schedule_WL,
        .counter              = 0
      },
      .schedule = {
        .time_factor          = 1.0,
        .interval             = &g_schedule_interval_tms,
        .post_action_sleep_ms = 1,
        .can_skip_tick        = true
      }
    };
    __set_action_trigger_WL( processor_WL, 2, &Schedule );


    // Action 4:
    // Execute events by sending them to execution job
    vgx_EventActionTrigger_t Execute = {
      .tms = {
        .now                  = 0,
        .at                   = 0,
        .delta                = 0
      },
      .action = {
        .input                = processor_WL,
        .output               = &processor_WL->EXECUTOR,
        .perform              = __eventproc_action__Execute_WL,
        .counter              = 0
      },
      .schedule = {
        .time_factor          = 1.0 / g_pevent_param->ShortTerm.partials,
        .interval             = &g_pevent_param->ShortTerm.migration_cycle_tms,
        .post_action_sleep_ms = 1,
        .can_skip_tick        = false
      }
    };
    __set_action_trigger_WL( processor_WL, 3, &Execute );


    // ---------------------------------------------------
    // 5: Initialize Event Processor Action timestamps
    // ---------------------------------------------------
    __refresh_action_triggers_WL( processor_WL );
    __reset_action_triggers_WL( processor_WL );


    // ---------------------------------------------------
    // 6: Log flow info
    // ---------------------------------------------------
    EVENTMONITOR_INFO( graph, 0, "Flow [%4.1f sec cycle (%3d x %5lld msec)] Input->Schedule", (*Schedule.schedule.interval)/1000.0, 1, Schedule.tms.delta );

    EVENTMONITOR_INFO( graph, 0, "Flow [%4.1f min cycle (%3d x %5lld msec)] Long->Medium",
                                         g_pevent_param->LongTerm.migration_cycle_tms/60000.0,
                                                          g_pevent_param->LongTerm.partials,
                                                                g_pevent_param->LongTerm.partial_interval_tms );
    
    EVENTMONITOR_INFO( graph, 0, "Flow [%4.1f min cycle (%3d x %5lld msec)] Medium->Short",
                                         g_pevent_param->MediumTerm.migration_cycle_tms/60000.0,
                                                          g_pevent_param->MediumTerm.partials,
                                                                g_pevent_param->MediumTerm.partial_interval_tms );
    
    EVENTMONITOR_INFO( graph, 0, "Flow [%4.1f sec cycle (%3d x %5lld msec)] Short->Execute",
                                         g_pevent_param->ShortTerm.migration_cycle_tms/1000.0,
                                                          g_pevent_param->ShortTerm.partials,
                                                                g_pevent_param->ShortTerm.partial_interval_tms );

    // ---------------------------------------------------
    // 7: Ready
    // ---------------------------------------------------
    EVENTMONITOR_INFO( graph, 0, "Ready" );

  }
  XCATCH( errcode ) {
    if( processor_WL ) {
      __delete_action_triggers_WL( processor_WL );
    }
    ret = 1; // error
  }
  XFINALLY {
    if( processor_WL ) {
      __release_eventproc( &processor_WL );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_EventProcessor_t,
                   EVP,
                   __event_monitor,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "event_monitor/" )
{

  vgx_Graph_t *graph = EVP->graph;
  const char *name = CStringValue( CALLABLE(graph)->Name(graph) );
  APPEND_THREAD_NAME( name );
  COMLIB_TASK__AppendDescription( self, name );

  vgx_EventProcessor_t *processor_WL = NULL;
  int64_t tms_now = 0;

  int64_t loop_delay = COMLIB_TASK_LOOP_DELAY( 100 );

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {

    // --------------------------------------
    // 1: Acquire processor
    // --------------------------------------
    while( (processor_WL = __acquire_eventproc( EVP, 1000 )) == NULL ) {

      // TODO: Could not acquire processors
      //
      // Break out and terminate after some TBD condition is met
      //
      if( COMLIB_TASK__IsRequested_ForceExit( self ) ) {
        break;
      }
    }

    if( processor_WL ) {

      // --------------------------------------------
      // 2: Refresh timestamps in action triggers
      // --------------------------------------------
      tms_now = __refresh_action_triggers_WL( processor_WL );


      // --------------------------------------------
      // 3: Resume after suspend: migrate schedules
      // --------------------------------------------
      if( COMLIB_TASK__IsRequested_Resume( self ) ) {
        // Accept resume request
        COMLIB_TASK__AcceptRequest_Resume( self );
        // Resume if suspended
        if( COMLIB_TASK__IsSuspended( self ) ) {
          // Set resuming state
          COMLIB_TASK__SetState_Resuming( self );
          // Migrate and reset
          __migrate_schedules_full_WL( processor_WL );
          __reset_action_triggers_WL( processor_WL );
          // Clear resuming state
          COMLIB_TASK__ClearState_Resuming( self );
          // Clear suspended state (resumed)
          COMLIB_TASK__ClearState_Suspended( self );
        } 
      }


      // ------------------------------------------------------------------
      // 4: Processor flush
      // ------------------------------------------------------------------

      // Count flush requests
      int processor_flush_requests = 0;

      // Suspend request
      if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
        COMLIB_TASK__AcceptRequest_Suspend( self );
        COMLIB_TASK__SetState_Suspending( self );
        // Flush implied
        ++processor_flush_requests;
      }
        
      // Explicit flush request
      if( __consume_is_req_flush_WL( processor_WL ) ) {
        ++processor_flush_requests;
      }

      // Flush if needed
      if( processor_flush_requests ) {
        // Consume input queue into schedule without sending any imminent events to executor
        EVENTMONITOR_VERBOSE( graph, 0x231, "Flushing" );
        __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_FLUSH );
        // Schedule all new events

#ifdef HASVERBOSE
        int64_t n_Qapi_pre = __length_Qapi( graph );
#endif
        __schedule_events_WL( processor_WL, NULL );

#ifdef HASVERBOSE
        int64_t n_Qapi_post = __length_Qapi( graph );
        int64_t n_Mall = EVENTMAP_SIZE( processor_WL->Schedule.LongTerm ) + EVENTMAP_SIZE( processor_WL->Schedule.MediumTerm ) + EVENTMAP_SIZE( processor_WL->Schedule.ShortTerm );
        EVENTMONITOR_VERBOSE( graph, 0x232, "IN[ %lld ] -(%lld)-> SCH[ {%lld} ] ", n_Qapi_post, n_Qapi_pre-n_Qapi_post, n_Mall );
#endif
        // Re-schedule imminent events
        __reschedule_pending_events_imminent_WL( processor_WL, NULL );
        if(  LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Monitor.Queue ) > 0 ) {
          EVENTMONITOR_CRITICAL( graph, 0x233, "Failed to flush events Monitor.Queue=%lld", LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Monitor.Queue ) );
        }
        // Processor will be disabled
        if( COMLIB_TASK__IsSuspending( self ) ) {
          // Shut down executor and move any pending events into short term map (including Executor.Queue events)
          if( processor_WL->EXECUTOR ) {
            EVENTMONITOR_VERBOSE( graph, 0x234, "Stopping event executor" );
            if( __cancel_execution_WL( processor_WL, &processor_WL->EXECUTOR, 60000 ) < 0 ) {
              EVENTMONITOR_WARNING( graph, 0x235, "Failed to stop event executor" );
            }
            else {
              EVENTMONITOR_VERBOSE( graph, 0x236, "Event executor stopped" );
            }
          }
          // Executor does not exist, execution queue should be empty (re-scheduled into)
          if( processor_WL->EXECUTOR == NULL && LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue ) > 0 ) {
            EVENTMONITOR_CRITICAL( graph, 0x237, "Failed to re-schedule events Executor.Queue=%lld", LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue ) );
          }
          // Suspended
          COMLIB_TASK__ClearState_Suspending( self );
          COMLIB_TASK__SetState_Suspended( self );
          --processor_flush_requests;
        }

        // Confirm explicit flush
        if( processor_flush_requests ) {
          __set_ack_flushed_WL( processor_WL );
        }
      }


      // -----------------------------------------------------------------
      // 5: Perform Event Processor work unless we're currently paused
      // -----------------------------------------------------------------
      if( COMLIB_TASK__IsNotSuspended( self ) ) {
        for( vgx_EventActionTrigger_t **action=processor_WL->ActionTriggers; *action != NULL; action++ ) {
          __perform_eventproc_action_WL( *action );
        }
      }


      // --------------------------------------------------------------------------
      // 6: Check if any background execution job is running and how it's doing
      // --------------------------------------------------------------------------
      if( processor_WL->EXECUTOR ) {
        // A previously started execution thread has completed and should be joined and cleaned up
        if( COMLIB_TASK__IsDead( processor_WL->EXECUTOR->TASK ) ) {
          if( __cancel_execution_WL( processor_WL, &processor_WL->EXECUTOR, 60000 ) < 0 ) {
            EVENTMONITOR_CRITICAL( graph, 0x238, "Failed to clean up event executor" );
          }
        }
      }

      // --------------------------------------------------------------------------
      // 7: Release processor
      // --------------------------------------------------------------------------

      __release_eventproc( &processor_WL );
    }


    if( COMLIB_TASK__IsStopping( self ) ) {
      COMLIB_TASK__AcceptRequest_Stop( self );
    }

  } END_COMLIB_TASK_MAIN_LOOP;


  // =============================================
  // ======= EVENT PROCESSOR SHUTTING DOWN =======
  // =============================================


  // ----------------------------------------------------
  // POST 1: Make sure execution job completes if running
  // ----------------------------------------------------

  if( (processor_WL = __acquire_eventproc( EVP, 10000 )) != NULL ) {
    if( processor_WL->EXECUTOR && __cancel_execution_WL( processor_WL, &processor_WL->EXECUTOR, 60000 ) < 0 ) {
      EVENTMONITOR_CRITICAL( graph, 0x239, "Failed to clean up event executor" );
    }
    __release_eventproc( &processor_WL );
  }
  else {
    EVENTMONITOR_CRITICAL( graph, 0x23A, "Failed to acquire processor on shutdown" );
  }


  EVENTMONITOR_VERBOSE( graph, 0x23A, "Terminating" );

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static int64_t __reschedule_pending_events_imminent_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexEventQueue_t *Qpen ) {
  int64_t n_resched = 0;
  vgx_Graph_t *graph = processor_WL->graph;
  vgx_VertexStorableEvent_t ev;
  
  int64_t n_Qpen = 0;
  int64_t n_Qpen_to_Qexe = 0;

#ifdef HASVERBOSE
  int64_t n_Qexe_pre = LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue );
#endif

  // Reverse any pending events from the execution job into the monitor's execution queue
  if( Qpen ) {
    n_Qpen = LENGTH_QUEUE_VERTEX_EVENTS( Qpen );
    if( (n_Qpen_to_Qexe = ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK( processor_WL->Executor.Queue, Qpen, -1 )) < 0 ) {
      EVENTMONITOR_CRITICAL( graph, 0x241, "Failed to re-schedule pending events" );
    }
    // Done with the pending queue
    COMLIB_OBJECT_DESTROY( Qpen );
  }

  // Reverse any events from monitor's execution queue into short term map
  while( NEXT_QUEUE_VERTEX_EVENT_NOLOCK( processor_WL->Executor.Queue, &ev ) ) {
    // Put back in short term map, immediate re-execution when possible
    EVENTMAP_SET( processor_WL->Schedule.ShortTerm, &ev );
    ++n_resched;
  }

#ifdef HASVERBOSE
  int64_t n_Qexe_post = LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue );
  int64_t n_Mshort_post = EVENTMAP_SIZE( processor_WL->Schedule.ShortTerm );
  EVENTMONITOR_VERBOSE( graph, 0x242, "SCH[ {... S:%lld} <-(%lld)- X:%lld ] <-(%lld)- EXEC[ ]", n_Mshort_post, n_resched, n_Qexe_post - n_Qexe_pre, n_Qpen_to_Qexe );
#endif

  return n_resched;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cancel_execution_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job, int timeout_ms ) {
  int64_t n_resched;
  __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_CANCEL_EXECUTION );
  vgx_VertexEventQueue_t *Qpen = __eventexec_cancel_execution_job_WL( job, timeout_ms );
  // Terminated ok
  if( Qpen ) {
    n_resched = __reschedule_pending_events_imminent_WL( processor_WL, Qpen );
    processor_WL->executor_thread_id = 0;
  }
  // Error
  else {
    n_resched = -1;
  }
  return n_resched;
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static int __remove_event_from_schedule_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexStorableEvent_t *ev ) {
  // NOTE: Event could exist in multiple maps. Remove from all.
  int n = 0;
  n += EVENTMAP_DELETE( processor_WL->Schedule.ShortTerm, ev );
  n += EVENTMAP_DELETE( processor_WL->Schedule.MediumTerm, ev );
  n += EVENTMAP_DELETE( processor_WL->Schedule.LongTerm, ev );
  // Return number of schedule entries removed for this event.
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __medium_term_cutoff_ms( void ) {
  int64_t tms_future = __MILLISECONDS_SINCE_1970() + g_pevent_param->MediumTerm.insertion_threshold_tms;
  return tms_future;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __short_term_cutoff_ms( void ) {
  int64_t tms_future = __MILLISECONDS_SINCE_1970() + g_pevent_param->ShortTerm.insertion_threshold_tms;
  return tms_future;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __executor_cutoff_ms( void ) {
  int64_t tms_future = __MILLISECONDS_SINCE_1970() + g_pevent_param->Executor.insertion_threshold_tms;
  return tms_future;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __queue_migration_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  __cutoff_timespec *T = (__cutoff_timespec*)processor->processor.input;
  vgx_EventValue_t evalue = { .bits = APTR_AS_INTEGER( fh_cell ) };
  // Move item from this map to destination map if cutoff time has been crossed
  if( evalue.ts_exec < T->cutoff_ts ) {
    // Insert item into destination map if item does not already exist in destination map
    QWORD key = APTR_AS_ANNOTATION( fh_cell );
    if( EVENTMAP_ENSURE_KEY( (framehash_t*)processor->processor.output, key, evalue.ival ) ) {
      // Mark item as deleted in the processed map
      FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
#ifndef NDEBUG
      // Already expired, this migration occurred to late
      if( T->now_ts > evalue.ts_exec ) {
      }
#endif
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
static int64_t __migrate_schedule_WL( vgx_EventProcessor_t *processor_WL, uint32_t cutoff_ts, framehash_t *src, framehash_t *dest, uint64_t subtree_selector ) {
  int64_t n_migrated = 0;

  __cutoff_timespec T = {
    .cutoff_ts  = cutoff_ts,
    .now_ts     = __SECONDS_SINCE_1970()
  };

  // Configure the migration candidate processor
  framehash_processing_context_t queue_migration_processing_context = FRAMEHASH_PROCESSOR_NEW_CONTEXT( NULL, NULL, __queue_migration_processor );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &queue_migration_processing_context );
  FRAMEHASH_PROCESSOR_SET_IO( &queue_migration_processing_context, &T, dest );

  // Run migration candidate processor
  if( (n_migrated = CALLABLE( src )->ProcessPartial( src, &queue_migration_processing_context, subtree_selector )) < 0 ) {
    EVENTMONITOR_CRITICAL( processor_WL->graph, 0x251, "Migration processor failed to collect migration candidates" );
    return -1;
  }
  // At least one event migrated, attempt compaction of source
  else if( n_migrated > 0 ) {
#ifndef NDEBUG
#endif
    CALLABLE( src )->CompactifyPartial( src, subtree_selector );
  }

  return n_migrated;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int64_t __migrate_schedules_full_WL( vgx_EventProcessor_t *processor_WL ) {
  int64_t n_migrated = 0;
  int64_t n_long_to_med = 0;
  int64_t n_med_to_short = 0;
  int n_long_subtrees = 1 << processor_WL->Schedule.LongTerm->_order;
  int n_med_subtrees = 1 << processor_WL->Schedule.MediumTerm->_order;
  uint32_t cutoff_med_ts = (uint32_t)(__medium_term_cutoff_ms() / 1000);
  uint32_t cutoff_short_ts = (uint32_t)(__short_term_cutoff_ms() / 1000);
  // Long -> Medium
  for( int n=0; n<n_long_subtrees; n++ ) {
    n_long_to_med += __migrate_schedule_WL( processor_WL, cutoff_med_ts, processor_WL->Schedule.LongTerm, processor_WL->Schedule.MediumTerm, n );
  }
  // Medium -> Short
  for( int n=0; n<n_med_subtrees; n++ ) {
    n_med_to_short += __migrate_schedule_WL( processor_WL, cutoff_short_ts, processor_WL->Schedule.MediumTerm, processor_WL->Schedule.ShortTerm, n );
  }
  n_migrated = n_long_to_med + n_med_to_short;

#ifdef HASVERBOSE
  int64_t n_long = EVENTMAP_SIZE( processor_WL->Schedule.LongTerm );
  int64_t n_med = EVENTMAP_SIZE( processor_WL->Schedule.MediumTerm );
  int64_t n_short = EVENTMAP_SIZE( processor_WL->Schedule.ShortTerm );

  EVENTMONITOR_VERBOSE( processor_WL->graph, 0x261, "Migration SCH[ {L:%lld -(%lld)-> M:%lld -(%lld)-> S:%lld} ]", n_long, n_long_to_med, n_med, n_med_to_short, n_short );
#endif

  return n_migrated;
}



/*******************************************************************//**
 *
 *
 * 
 ***********************************************************************
 */
static int __insert_event_into_schedule_WL( vgx_EventProcessor_t *processor_WL, vgx_VertexStorableEvent_t *ev, uint32_t executor_cutoff_ts, uint32_t short_term_cutoff_ts, uint32_t medium_term_cutoff_ts ) {
  int ret;
  // Send event straight to execution job
  if( ev->event_val.ts_exec < executor_cutoff_ts ) {
    ret = APPEND_QUEUE_VERTEX_EVENT_NOLOCK( processor_WL->Executor.Queue, ev );
  }
  // Schedule event in short term map
  else if( ev->event_val.ts_exec < short_term_cutoff_ts ) {
    ret = EVENTMAP_SET( processor_WL->Schedule.ShortTerm, ev );
  }
  // Schedule event in medium term map
  else if( ev->event_val.ts_exec < medium_term_cutoff_ts ) {
    ret = EVENTMAP_SET( processor_WL->Schedule.MediumTerm, ev );
  }
  // Schedule event in long term map
  else {
    ret = EVENTMAP_SET( processor_WL->Schedule.LongTerm, ev );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __is_event_earlier_than( vgx_EventValue_t *new_ev, vgx_EventValue_t *old_ev ) {
  return new_ev->ts_exec < old_ev->ts_exec;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __swap_inputs_WL( vgx_EventProcessor_t *processor_WL ) {
  int64_t n_events;
  SYNCHRONIZE_ON( processor_WL->PublicAPI.Lock ) {
    // Swap the API and MON inputs
    vgx_VertexEventQueue_t *Qapi = processor_WL->PublicAPI.Queue;
    if( (n_events = LENGTH_QUEUE_VERTEX_EVENTS( Qapi )) > 0 ) {
      processor_WL->PublicAPI.Queue = processor_WL->Monitor.Queue;
      processor_WL->Monitor.Queue = Qapi;
    }
  } RELEASE;
  return n_events;
}



/*******************************************************************//**
 *
 *
 *  Insert new event into schedule
 *  NOTE: Several internal schedule maps may contain events for the same vertex.
 *        This is ok because the vertex itself will know whether to actually perform anything
 *        when its event execution method is called. The vertex may do nothing if a spurious (premature)
 *        event is triggered due to re-scheduling for a later time (in a different schedule map),
 *        and will then re-insert its correct event time via the event API. If the most recently
 *        scheduled event is earlier than another event in a longer term map we could have a situation
 *        where eventually the long-term event triggers. It could trigger on a vertex that no longer
 *        exists or on a vertex that exists but has no event scheduled. In the first case there are
 *        protections in the Executor to verify the integrity of a vertex object before trying to
 *        execute the event. The event is simply discarded if it cannot be executed. In the second case
 *        the vertex will do nothing upon event trigger and not re-schedule anything either.
 *        In short, it is ok to have duplicates in the event schedule because last-minute checks
 *        are performed before executing the event and because the vertex will re-schedule an event
 *        for the correct time if it needs to.
 * 
 ***********************************************************************
 */
int __schedule_events_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job ) {
  vgx_Graph_t *graph = processor_WL->graph;

  int ret = 0;

  if( __swap_inputs_WL( processor_WL ) > 0 ) {
    vgx_VertexStorableEvent_t inevent;
    // If no executor job should be used then we will never send imminent events directly to executor
    // A "now" of 0 will result in event insertion below always use the schedule maps
    uint32_t ts_executor_cutoff     = (uint32_t)(__executor_cutoff_ms()     / 1000);
    uint32_t ts_short_term_cutoff   = (uint32_t)(__short_term_cutoff_ms()   / 1000);
    uint32_t ts_medium_term_cutoff  = (uint32_t)(__medium_term_cutoff_ms()  / 1000);

    // Consume all the MON input queue
    while( NEXT_QUEUE_VERTEX_EVENT_NOLOCK( processor_WL->Monitor.Queue, &inevent ) ) {
      // Remove event from all schedules
      if( __is_event_removal( &inevent ) ) {
        __remove_event_from_schedule_WL( processor_WL, &inevent );
      }
      // Insert/update event
      else if( __insert_event_into_schedule_WL( processor_WL, &inevent, ts_executor_cutoff, ts_short_term_cutoff, ts_medium_term_cutoff ) != 1 ) {
        cxmalloc_handle_t h = inevent.event_key;
        vgx_EventValue_t ev = inevent.event_val;
        EVENTMONITOR_CRITICAL( graph, 0x271, "Unable to schedule event [type=%02x texec=%u metas=%04x] for vertex [aidx=%u bidx=%u offset=%u]",
          ev.type, ev.ts_exec, ev.metas, h.aidx, h.bidx, h.offset
        );
        ret = -1;
      }
    }
  }

  // Immediately transfer imminent events to executor
  if( job && LENGTH_QUEUE_VERTEX_EVENTS( processor_WL->Executor.Queue ) > 0 ) {
    if( __transfer_imminent_events_to_execution_job_WL( processor_WL, job ) < 0 ) {
      // Failed to transfer to executor job, put back into the current input queue
      // which will eventually be scanned again at some point in the future.
      // (Events will be late but not missed.)
      EVENTMONITOR_WARNING( graph, 0x272, "Imminent events postponed, transfer to execution job failed (will re-try)" );
      if( ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK( processor_WL->Monitor.Queue, processor_WL->Executor.Queue, -1 ) < 0 ) {
        EVENTMONITOR_CRITICAL( graph, 0x273, "Imminent events leaked" );
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
static int64_t __has_executable_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  __cutoff_timespec *T = (__cutoff_timespec*)processor->processor.input;
  vgx_EventValue_t event_val = { .bits = APTR_AS_INTEGER( fh_cell ) };
  if( event_val.ts_exec < T->cutoff_ts ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return 1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __has_executable_events( framehash_t *schedule, uint64_t selector ) {
  // Get the timestamp used to determine whether events are soon due for execution
  __cutoff_timespec T = __get_executable_cutoff();

  // Configure the framehash processor that will scan for due events
  framehash_processing_context_t has_executable_processing_context = FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( NULL, NULL, __has_executable_processor, 1 );
  FRAMEHASH_PROCESSOR_SET_IO( &has_executable_processing_context, &T, NULL );
  return CALLABLE( schedule )->ProcessPartial( schedule, &has_executable_processing_context, selector ) > 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __queue_execution_processor( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  __cutoff_timespec *T = (__cutoff_timespec*)processor->processor.input;
  vgx_VertexStorableEvent_t ev = {
    .event_val = { .bits = APTR_AS_INTEGER( fh_cell ) }
  };

  // Move item from this map to destination queue if cutoff time has been crossed
  if( ev.event_val.ts_exec < T->cutoff_ts ) {
    // Insert item into destination queue
    ev.event_key.allocdata = APTR_AS_ANNOTATION( fh_cell );
    if( PUSH_HEAP_VERTEX_EVENT_NOLOCK( (vgx_VertexEventHeap_t*)processor->processor.output, &ev ) ) {
      // Mark item as deleted in the processed map
      FRAMEHASH_PROCESSOR_DELETE_CELL( processor, fh_cell );
#ifndef NDEBUG
      // Already expired, this execution transfer occurred to late
      if( T->now_ts > ev.event_val.ts_exec ) {
      }
#endif
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
static int __transfer_scheduled_events_to_execution_job_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job, uint64_t subtree_selector ) {
  int ret = 0;
  vgx_ExecutionJobDescriptor_t *executor = *job;

  // Create new background job if it does not exist
  if( executor == NULL ) {
    if( (*job = executor = __eventexec_new_execution_job_WL( processor_WL )) == NULL ) {
      return -1;
    }

    processor_WL->executor_thread_id = COMLIB_TASK__ThreadId( executor->TASK );
  }

  COMLIB_TASK_LOCK( executor->TASK ) {
    // Events due for execution will be found in the short term map.
    framehash_t *schedule = processor_WL->Schedule.ShortTerm;

    // Get the cutoff timestamp used to determine which events are soon due for execution.
    // Events that are imminent will be transferred to the execution job.
    __cutoff_timespec T = __get_executable_cutoff();

    // Configure the framehash processor that will scan for due events and populate the job execution queue.
    framehash_processing_context_t queue_execution_processing_context = FRAMEHASH_PROCESSOR_NEW_CONTEXT( NULL, NULL, __queue_execution_processor );
    FRAMEHASH_PROCESSOR_MAY_MODIFY( &queue_execution_processing_context );
    FRAMEHASH_PROCESSOR_SET_IO( &queue_execution_processing_context, &T, executor->Queue.pri_inp );

    // Run timestamp scan, collect vertices into execution queue
    int64_t n_events = 0;

    if( (n_events = CALLABLE( schedule )->ProcessPartial( schedule, &queue_execution_processing_context, subtree_selector )) < 0 ) {
      EVENTMONITOR_CRITICAL( processor_WL->graph, 0x281, "Processor failed to collect events for execution" );
      ret = -1;
    }
    // At least one event submitted for execution
    else if( n_events > 0 ) {
#ifndef NDEBUG
#endif
      CALLABLE( schedule )->CompactifyPartial( schedule, subtree_selector );
    }
  } COMLIB_TASK_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __transfer_imminent_events_to_execution_job_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job ) {
  int ret = 0;
  vgx_ExecutionJobDescriptor_t *executor = *job;

  // Create new background job if it does not exist
  if( executor == NULL ) {
    if( (*job = executor = __eventexec_new_execution_job_WL( processor_WL )) == NULL ) {
      return -1;
    }

    processor_WL->executor_thread_id = COMLIB_TASK__ThreadId( executor->TASK );
  }

  COMLIB_TASK_LOCK( executor->TASK ) {
    if( !ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK( executor->Queue.pri_inp, processor_WL->Executor.Queue, -1 ) ) {
      ret = -1;
    }
    else {
      executor->n_current = (int)LENGTH_HEAP_VERTEX_EVENTS( executor->Queue.pri_inp );
    }
  } COMLIB_TASK_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __length_Qapi( vgx_Graph_t *self ) {
  int64_t n_events;
  SYNCHRONIZE_ON( self->EVP.PublicAPI.Lock ) {
    n_events = LENGTH_QUEUE_VERTEX_EVENTS( self->EVP.PublicAPI.Queue );
  } RELEASE;
  return n_events;
}



/*******************************************************************//**
 * __acquire_eventproc
 *
 * Return pointer to processor if acquired
 * Return  NULL if we can't acquire
 *
 ***********************************************************************
 */
vgx_EventProcessor_t * __acquire_eventproc( vgx_EventProcessor_t *processor, int timeout_ms ) {
  // Acquire processor
  if( COMLIB_TASK__Acquire( processor->TASK, timeout_ms ) ) {
    // OK!
    vgx_EventProcessor_t *processor_WL = processor;
    // Set BUSY state if processor is operational
    if( __eventproc_state_is_operational_WL( processor_WL ) ) {
      processor_WL->params.task_WL.state = VGX_EVENTPROC_STATE_BUSY;
    }
    return processor_WL;
  }
  // Acquisition timeout
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * __release_eventproc
 *
 * Returns the number of remaining acquisitions after release, 0 when no longer acquired.
 * Set the supplied processor pointer to NULL if one level of recursion was
 * successfully given up.
 * 
 *
 ***********************************************************************
 */
int __release_eventproc( vgx_EventProcessor_t **processor_WL ) {
  int released = -1;
  // Release processor
  if( processor_WL && *processor_WL ) {
    comlib_task_t *task = (*processor_WL)->TASK;
    if( task ) {
      // Will fully release
      if( COMLIB_TASK__Acquisitions( task ) == 1 ) {
        (*processor_WL)->params.task_WL.state = VGX_EVENTPROC_STATE_IDLE;
      }
      // Release one
      if( (released = COMLIB_TASK__Release( task )) >= 0 ) {
        *processor_WL = NULL; 
      }
    }
  }
  return released;
}



/*******************************************************************//**
 * __create_eventproc_task
 *
 * Create the event processor in the suspended state
 *
 ***********************************************************************
 */
int __create_eventproc_task( vgx_EventProcessor_t *processor ) {

  // Create task
  if( (processor->TASK = COMLIB_TASK__New( __event_monitor, __event_initialize, NULL, processor )) == NULL ) {
    return -1;
  }

  // Acquire
  vgx_EventProcessor_t *processor_WL = __acquire_eventproc( processor, 0 );
  if( processor_WL == NULL ) {
    COMLIB_TASK__Delete( &processor->TASK );
    return -1;
  }
    
  // Initialize state suspended
  __update_eventproc_state_WL( processor_WL, VGX_EVENTPROC_STATE_IDLE );
  COMLIB_TASK__SetState_Suspended( processor_WL->TASK );

  // Release
  __release_eventproc( &processor_WL );

  return 0;
}



/*******************************************************************//**
 * __start_eventproc
 *
 * Start the event processor
 *
 ***********************************************************************
 */
int __start_eventproc( vgx_EventProcessor_t *processor, bool enable ) {
  int ret = 0;

  // Event processor task does not exist
  if( processor->TASK == NULL ) {
    return -1;
  }

  // Enable if requested, otherwise leave suspended
  if( enable ) {
    COMLIB_TASK__Request_Resume( processor->TASK );
  }

  // Start task
  if( COMLIB_TASK__Start( processor->TASK, 10000 ) < 1 ) {
    ret = -1;
  }

  return ret;
}



/*******************************************************************//**
 * __stop_delete_eventproc
 *
 ***********************************************************************
 */
int __stop_delete_eventproc( vgx_EventProcessor_t *processor ) {
  int ret = 0;
  if( processor && processor->graph && processor->TASK ) {
    comlib_task_t *task = processor->TASK;
    vgx_Graph_t *graph = processor->graph;
    GRAPH_LOCK( graph ) {

      EVENTMONITOR_VERBOSE( graph, 0x290, "Stopping..." );

      int timeout_ms = 60000;
      int stopped;
      GRAPH_SUSPEND_LOCK( graph ) {
        if( (stopped = COMLIB_TASK__Stop( task, NULL, timeout_ms )) < 0 ) {
          EVENTMONITOR_WARNING( graph, 0x291, "Forcing exit" );
          stopped = COMLIB_TASK__ForceExit( task, 30000 );
        }
      } GRAPH_RESUME_LOCK;
        
      if( stopped == 1 ) {
        // Acquire
        vgx_EventProcessor_t *processor_WL = __acquire_eventproc( processor, 10000 );
        if( processor_WL ) {

          // [11]
          __delete_action_triggers_WL( processor_WL );

          // [10]
          if( processor_WL->Executor.Queue ) {
            COMLIB_OBJECT_DESTROY( processor_WL->Executor.Queue );
            processor_WL->Executor.Queue = NULL;
          }
          // [9]
          if( processor_WL->Monitor.Queue ) {
            COMLIB_OBJECT_DESTROY( processor_WL->Monitor.Queue );
            processor_WL->Monitor.Queue = NULL;
          }
          // [8b]
          if( processor_WL->PublicAPI.Queue ) {
            COMLIB_OBJECT_DESTROY( processor_WL->PublicAPI.Queue );
            processor_WL->PublicAPI.Queue = NULL;
          }
          // [8a]
          DEL_CRITICAL_SECTION( &processor_WL->PublicAPI.Lock.lock );
          // [7c]
          iMapping.DeleteMap( &processor_WL->Schedule.ShortTerm );
          // [7b]
          iMapping.DeleteMap( &processor_WL->Schedule.MediumTerm );
          // [7a]
          iMapping.DeleteMap( &processor_WL->Schedule.LongTerm );

          // Release
          __release_eventproc( &processor_WL );

          COMLIB_TASK__Delete( &processor_WL->TASK );
          EVENTMONITOR_VERBOSE( graph, 0x293, "Removed" );
        }
        else {
          EVENTMONITOR_REASON( graph, 0x292, "Failed acquire processor, unable to erase structures" );
        }
      }
      else {
        EVENTMONITOR_REASON( graph, 0x294, "Failed to stop" );
        ret = -1;
      }
    } GRAPH_RELEASE;
  }

  return ret;

}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxevent_eventmon.h"

test_descriptor_t _vgx_vxevent_eventmon_tests[] = {
  { "VGX Event Monitor Tests", __utest_vxevent_eventmon },

  {NULL}
};
#endif
