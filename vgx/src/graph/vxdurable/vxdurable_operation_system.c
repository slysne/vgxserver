/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_system.c
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

#include "_vxoperation.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "TX::SYS(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define SYSTEM_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define SYSTEM_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define SYSTEM_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define SYSTEM_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define SYSTEM_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define SYSTEM_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )




static unsigned       __operation_system_initialize( comlib_task_t *self );
static unsigned       __operation_system_shutdown( comlib_task_t *self );

static void           __dump_unconfirmed_OPEN( vgx_OperationSystem_t *opsys );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float __drain_as_percentage_TCS( comlib_task_t *task ) {
  vgx_OperationSystem_t *agentsys = COMLIB_TASK__GetData( task );
  if( agentsys ) {
    int64_t n_bytes_pending = 0;
    COMLIB_TASK_SUSPEND_LOCK( task ) {
      n_bytes_pending += iOperation.System_OPEN.BytesPending( agentsys->graph );
    } COMLIB_TASK_RESUME_LOCK;

    static const int64_t pseudo_max_bytes = 512 << 20; // 512MiB
    float pseudo_pct = 100.0f * n_bytes_pending / (float)pseudo_max_bytes;
    return pseudo_pct < 100.0 ? pseudo_pct : 100.0f;
  }
  else {
    return -1.0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_system_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_OperationSystem_t *opsys = COMLIB_TASK__GetData( self );
  vgx_Graph_t *agent = opsys->graph;

  if( !iSystem.IsSystemGraph( agent ) ) {
    SYSTEM_CRITICAL( agent, 0x001, "System output not valid for non-system graph '%s'", CALLABLE( agent )->FullPath( agent ) );
    // Non-system graph!
    return 1;
  }

  COMLIB_TASK_LOCK( self ) {
    XTRY {

      // [10] Transactional producers
      if( (opsys->producers = iTransactional.Producers.New()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }

    }
    XCATCH( errcode ) {
      ret = 1; // error
    }
    XFINALLY {
    }
  } COMLIB_TASK_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_system_shutdown( comlib_task_t *self ) {
  vgx_OperationSystem_t *opsys = COMLIB_TASK__GetData( self );
  vgx_Graph_t *agent = opsys->graph;

  // =================================================
  // ======= OPERATION SYSTEM SHUTTING DOWN =======
  // =================================================

  COMLIB_TASK__ClearState_Busy( self );

  if( opsys->producers ) {
    // Make sure all producers are empty, otherwise log error
    vgx_TransactionalProducersIterator_t iter = {0};
    if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
      objectid_t fingerprint = igraphfactory.Fingerprint();
      vgx_TransactionalProducer_t *producer;
      while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
        BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
          producer->local.fingerprint = fingerprint;
          if( !iTransactional.Producer.Connected( producer ) ) {
            int64_t tms_now = __MILLISECONDS_SINCE_1970();
            iTransactional.Producer.Reconnect( producer, tms_now, 5000 );
          }
          int64_t n = iTransactional.Producer.Length( producer );
          if( n > 0 ) {
            SYSTEM_CRITICAL( agent, 0x001, "Operation transaction list not empty on shutdown. The following %lld transactions are lost.", n );
            iTransactional.Producer.DumpState( producer, NULL );
          }
        } END_TRANSACTIONAL_IF_ATTACHED;
      }
      iTransactional.Producers.Iterator.Clear( &iter );
    }

    // Destroy
    iTransactional.Producers.Delete( &opsys->producers );
  }

  if( opsys->consumer ) {
    iTransactional.Consumer.Delete( &opsys->consumer );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __manage_loop_progress( vgx_TransactionalProducer_t *producer, int64_t tms_now, int64_t *n_tx_bytes_pending, int64_t *n_tx_pending, int64_t *queue_latency_ms, int64_t *stage_latency_ms ) {
  int64_t bytes = iTransactional.Producer.Bytes( producer );
  int64_t length = iTransactional.Producer.Length( producer );
  int64_t s_latency = iTransactional.Producer.StageLatency( producer, tms_now );
  int64_t q_latency = iTransactional.Producer.QueueLatency( producer, tms_now );
  *n_tx_bytes_pending += bytes;
  *n_tx_pending += length;
  if( s_latency > *stage_latency_ms ) {
    *stage_latency_ms = s_latency;
  }
  if( q_latency > *queue_latency_ms ) {
    *queue_latency_ms = q_latency;
  }

  int64_t loop_delay;

  // Attached and connected
  if( iTransactional.Producer.Connected( producer ) ) {
    // Data remains to be transmitted and we're not in a resync situation
    if( bytes > 0 && producer->resync_transaction == NULL ) {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
    }
    // Error condition
    else if( bytes < 0 ) {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 1000 );
    }
    // All pending data has been transmitted or we are resyncing (and taking it slow)
    else {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 50 );
    }
  }
  // Attached but not connected
  else if( iTransactional.Producer.Attached( producer ) ) {
    uint32_t now_ts = (uint32_t)(tms_now / 1000);
    if( now_ts > producer->connection_ts.attempt ) {
      int timeout_ms = 10 + ((int)rand15() & 0xFF);
      iTransactional.Producer.Reconnect( producer, tms_now, timeout_ms );
    }


    // Immediate progress if output pending
    if( length > 0 || bytes > 0 ) {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
    }
    // Short delay if no output pending
    else {
      loop_delay = COMLIB_TASK_LOOP_DELAY( 100 );
    }
  }
  // Detached mode, just idle loop with a 2 second dummy interval
  else {
    loop_delay = COMLIB_TASK_LOOP_DELAY( 250 );
  }

  return loop_delay;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double __refresh_tx_output_rate( int64_t sum_lifetime_bytes ) {
  double rate = __refresh_data_rate( sum_lifetime_bytes );
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      SYSTEM->OP.system.running_tx_output_rate_CS = (float)rate;
    } GRAPH_RELEASE;
  }
  return rate;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_unconfirmed_OPEN( vgx_OperationSystem_t *opsys ) {
  vgx_TransactionalProducersIterator_t iter = {0};
  if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
    vgx_TransactionalProducer_t *producer;
    while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
      BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
        iTransactional.Producer.DumpUnconfirmed( producer );
      } END_TRANSACTIONAL_IF_ATTACHED;
    }
    iTransactional.Producers.Iterator.Clear( &iter );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_OperationSystem_t,
                   opsys,
                   __operation_system_monitor,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "operation_system_monitor/" )
{
  vgx_Graph_t *agent = opsys->graph;
  const char *agent_name = CStringValue( CALLABLE( agent )->Name( agent ) );
  APPEND_THREAD_NAME( agent_name );
  COMLIB_TASK__AppendDescription( self, agent_name );

  // THIS THREAD OWNS THE SYSTEM GRAPH UNTIL SHUTDOWN

  if( igraphfactory.OpenGraph( agent->CSTR__path, agent->CSTR__name, true, NULL, 0 ) == agent ) {

    bool suspend_request = false;
    bool suspended = false;
    int64_t suspend_request_at_tms = -1;

    comlib_task_delay_t continuous = COMLIB_TASK_LOOP_DELAY( 0 );
    int64_t tms = 0;

    objectid_t fingerprint = igraphfactory.Fingerprint();
    int64_t next_fingerprint_tms = -1;

    comlib_task_delay_t sleep_ms = 0;
    comlib_task_delay_t max_sleep = 250;

    int64_t sum_accepted_bytes = 0;
    
    int64_t last_tx_bytes_pending = 0;

    GRAPH_LOCK( agent ) {
      opsys->state_CS.flags.running = true;
    } GRAPH_RELEASE;

    BEGIN_COMLIB_TASK_MAIN_LOOP( continuous ) {

      tms = _vgx_graph_milliseconds( agent );

      int64_t n_tx_bytes_pending = 0;
      int64_t n_tx_pending = 0;
      int64_t sysout_readable = 0;
      int64_t stage_latency_ms = 0;
      int64_t queue_latency_ms = 0;

      // Initialize to max
      sleep_ms = max_sleep;

      if( !suspended ) {
        // Refresh fingerprint?
        if( tms > next_fingerprint_tms ) {
          next_fingerprint_tms = tms + 3000;
          objectid_t current_fingerprint = igraphfactory.Fingerprint();
          if( !idmatch( &current_fingerprint, &fingerprint ) ) {
            fingerprint = current_fingerprint;
           
          }
        }

        // Busy
        COMLIB_TASK__SetState_Busy( self );

        // -------------------------------------
        // Perform I/O
        // -------------------------------------
        comlib_task_delay_t producer_delay;
        vgx_TransactionalProducersIterator_t iter = {0};
        if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
          vgx_TransactionalProducer_t *producer;
          vgx_OperationTransaction_t *R = NULL;
          sum_accepted_bytes = 0; // count each time
          while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
            BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
              producer->local.fingerprint = fingerprint;
              // Send and receive 
              if( iTransactional.Producer.Perform.Exchange( producer, tms ) < 0 ) {
                SYSTEM_REASON( agent, 0x001, "I/O Error" );
                // Consider all sent but unconfirmed data lost. Restore read position
                // to the confirmed position. When we re-connect all unconfirmed data will
                // be re-transmitted. The receive buffer will be cleared.
                iTransactional.Producer.Perform.Rollback( producer, tms );
              }

              // Sum of all producers' lifetime sum of transaction bytes
              sum_accepted_bytes += producer->accepted_bytes;

              // Pick the smallest delay
              if( (producer_delay = __manage_loop_progress( producer, tms, &n_tx_bytes_pending, &n_tx_pending, &queue_latency_ms, &stage_latency_ms )) < sleep_ms ) {
                sleep_ms = producer_delay;
              }

              // Stuck after resync? All resync data sent but no got response after 60 sec.
              if( (R = producer->resync_transaction) != NULL ) {
                // No data remains to be sent for resync but we have not cleared resync state yet
                if( producer->resync_remain == 0 ) {
                  // It's been more than a minute since resync and server has not yet responded
                  if( stage_latency_ms > 60000 ) {
                    const char *uri = iURI.URI( producer->URI );
                    SYSTEM_WARNING( agent, 0x003, "Unresponsive '%s' after resync. Disconnecting.", uri );
                    iTransactional.Producer.Perform.Rollback( producer, tms );
                  }
                }
              }

              // For suspending, let's try to measure the bytes in the actual sysout buffer, not just the transaction bytes pending
              // If sysout remains empty while transactions are not getting accepted (or rejected) by peer we may be stuck forever 
              // and never be able to suspend. This is an issue with the remote peer and not with us so we need a way to recover.
              //
              sysout_readable += iOpBuffer.Readable( producer->buffer.sysout );

            } END_TRANSACTIONAL_IF_ATTACHED;
          }
          // Clear iterator and perform only if fragmentation was detected during iteration. (Very rarely.)
          iTransactional.Producers.Iterator.Clear( &iter );
        }
      }

      int64_t now_tms = __GET_CURRENT_MILLISECOND_TICK();

      // Unconfirmed transactions still exist
      bool tx_pending = n_tx_bytes_pending > 0 || n_tx_pending > 0;

      // No progress since last iteration 
      if( n_tx_bytes_pending == last_tx_bytes_pending ) {
        // Loop progress indicates no delay
        if( sleep_ms == 0 ) {
          // Relax for 1 ms
          sleep_ms = 1;
        }
      }

      // Update
      last_tx_bytes_pending = n_tx_bytes_pending;
      __refresh_tx_output_rate( sum_accepted_bytes );


      COMLIB_TASK_LOCK( self ) {
        opsys->out_backlog.n_tx = n_tx_pending;
        opsys->out_backlog.n_bytes = n_tx_bytes_pending;
        opsys->out_backlog.latency_ms = queue_latency_ms;

        // Not busy when no I/O pending (or long delay detected)
        if( sysout_readable == 0 ) {
          COMLIB_TASK__ClearState_Busy( self );
          COMLIB_TASK__SignalIdle( self );
          // Suspend if requested
          if( suspend_request ) {
            // Either all transactions confirmed, or we have waited longer than usual for transactions to be confirmed
            if( tx_pending == false || now_tms - suspend_request_at_tms > 15000 ) {
              suspend_request = false;
              COMLIB_TASK__SetState_Suspended( self );
              COMLIB_TASK__ClearState_Suspending( self );
              // Now suspended
              suspended = true;
              if( tx_pending ) {
                SYSTEM_WARNING( agent, 0x003, "Forcing suspend with unconfirmed transactions (ts=%lld/bytes=%lld)", n_tx_pending, n_tx_bytes_pending );
                COMLIB_TASK_SUSPEND_LOCK( self ) {
                  __dump_unconfirmed_OPEN( opsys );
                } COMLIB_TASK_RESUME_LOCK;
              }
              // CLEANUP
              COMLIB_TASK_SUSPEND_LOCK( self ) {
                vgx_TransactionalProducersIterator_t iter = {0};
                if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
                  vgx_TransactionalProducer_t *producer;
                  while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
                    BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
                      iOpBuffer.Trim( producer->buffer.sysout, 512<<10 );
                    } END_TRANSACTIONAL_IF_ATTACHED;
                  }
                  iTransactional.Producers.Iterator.Clear( &iter );
                }
              } COMLIB_TASK_RESUME_LOCK;
            }
          }
        }

        // Stop requested?
        if( COMLIB_TASK__IsRequested_Stop( self ) ) {
          COMLIB_TASK__AcceptRequest_Stop( self );
          COMLIB_TASK__SetState_Stopping( self );
        }
        // Suspend requested?
        else if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
          COMLIB_TASK__AcceptRequest_Suspend( self );
          COMLIB_TASK__SetState_Suspending( self );
          suspend_request = true;
          suspend_request_at_tms = now_tms;
          SYSTEM_VERBOSE( agent, 0x004, "Suspending" );
        }
        // Resume requested ?
        else if( COMLIB_TASK__IsRequested_Resume( self ) ) {
          COMLIB_TASK__AcceptRequest_Resume( self );
          COMLIB_TASK__ClearState_Suspending( self );
          COMLIB_TASK__ClearState_Suspended( self );
          suspended = false;
          suspend_request = false;
          SYSTEM_VERBOSE( agent, 0x005, "Resumed" );
        }
        // Sleep according to current load
        else if( sleep_ms > 0 ) {
          int s = (int)sleep_ms;
          COMLIB_TASK_IDLE_SLEEP( self, s );
        }
      } COMLIB_TASK_RELEASE;
      

    } END_COMLIB_TASK_MAIN_LOOP;

    GRAPH_LOCK( agent ) {
      opsys->state_CS.flags.running = false;
    } GRAPH_RELEASE;

  }

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_OperationCounters_t _vxdurable_operation_system__counters_outstream_OPEN( vgx_Graph_t *SYSTEM ) {
  vgx_OperationCounters_t out_counters;
  GRAPH_LOCK( SYSTEM ) {
    out_counters = SYSTEM->OP.system.out_counters_CS;
  } GRAPH_RELEASE;
  return out_counters;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_OperationCounters_t _vxdurable_operation_system__counters_instream_OPEN( vgx_Graph_t *SYSTEM ) {
  vgx_OperationCounters_t in_counters;
  GRAPH_LOCK( SYSTEM ) {
    in_counters = SYSTEM->OP.system.in_counters_CS;
  } GRAPH_RELEASE;
  return in_counters;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_system__reset_counters_OPEN( vgx_Graph_t *SYSTEM ) {
  GRAPH_LOCK( SYSTEM ) {
    memset( &SYSTEM->OP.system.out_counters_CS, 0, sizeof( vgx_OperationCounters_t ) );
    memset( &SYSTEM->OP.system.in_counters_CS, 0, sizeof( vgx_OperationCounters_t ) );
    SYSTEM->tx_input_lag_ms_CS = 0;
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_OperationBacklog_t _vxdurable_operation_system__get_output_backlog_OPEN( vgx_Graph_t *SYSTEM ) {
  vgx_OperationBacklog_t backlog = {0};
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
    if( agentsys->TASK ) {
      comlib_task_t *task = agentsys->TASK;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        COMLIB_TASK_LOCK( task ) {
          backlog = agentsys->out_backlog;
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
  return backlog;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__initialize_CS( vgx_Graph_t *graph, vgx_OperationSystem_t *system ) {
  // Reset data
  memset( system, 0, sizeof( vgx_OperationSystem_t ) );

  // [Q1.1] Task will be created later
  system->TASK = NULL;

  // [Q1.2] Reset all state data
  system->state_CS._bits = 0;

  // [Q1.3]
  if( system->progress_CS ) {
    free( system->progress_CS );
  }
  if( (system->progress_CS = calloc( 1, sizeof(vgx_OperationSystemProgressCounters_t) )) == NULL ) {
    return -1;
  }

  // [Q1.4]
  if( system->in_feed_limits_CS ) {
    free( system->in_feed_limits_CS );
  }
  if( (system->in_feed_limits_CS = calloc( 1, sizeof(vgx_OperationFeedRates_t) )) == NULL ) {
    return -1;
  }

  // [Q1.5.1]
  system->running_tx_input_rate_CS = 0.0;

  // [Q1.5.2]
  system->running_tx_output_rate_CS = 0.0;

  // [Q1.6] Set Prefix, unique each time
  system->set.prefix = graph->instance_id;

  // [Q1.7] Set ID
  system->set.id = 0;

  // [Q1.8] Set initial ID, start count at current milliseconds since 1970
  system->set.id_offset = __MILLISECONDS_SINCE_1970();

  // [Q2.1]
  system->graph = graph;

  // [Q2.2] Transactional producers (SYSTEM ONLY!)
  system->producers = NULL;

  // [Q2.3] Transactional consumer (SYSTEM ONLY!)
  system->consumer = NULL;

  // [Q2.4] Validating parser (SYSTEM ONLY!)
  system->validator =NULL;

  // [Q2.5-8]
  memset( &system->out_backlog, 0, sizeof( vgx_OperationBacklog_t ) );

  // [Q3.1-4]
  memset( &system->out_counters_CS, 0, sizeof( vgx_OperationCounters_t ) );

  // [Q3.5-8]
  memset( &system->in_counters_CS, 0, sizeof( vgx_OperationCounters_t ) );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__start_agent_SYS_CS( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;

  // ===============================
  // START SYSTEM AGENT
  // ===============================
  XTRY {

    if( (agentsys->TASK = COMLIB_TASK__New( __operation_system_monitor, __operation_system_initialize, __operation_system_shutdown, agentsys )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
    }

    int started;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      started = COMLIB_TASK__Start( agentsys->TASK, 10000 );
    } GRAPH_RESUME_LOCK;

    if( started == 1 ) {
      int alive = 1;
      while( alive && agentsys->state_CS.flags.running == false ) {
        GRAPH_SUSPEND_LOCK( SYSTEM ) {
          alive = COMLIB_TASK__IsAlive( agentsys->TASK );
        } GRAPH_RESUME_LOCK;
      }
      if( alive ) {
        SYSTEM_VERBOSE( SYSTEM, 0, "Started" );
      }
      else {
        SYSTEM_CRITICAL( SYSTEM, 0, "Terminated shortly after start" );
      }
    }
    else {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
    }
  }
  XCATCH( errcode ) {
    if( agentsys->TASK ) {
      COMLIB_TASK__Delete( &agentsys->TASK );
    }

    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 * Suspend output agent
 *
 * Return:  1: Suspended
 *          0: Agent isn't running
 *         -1: Error / timeout
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__suspend_agent_SYS_CS( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  int suspended = 0;
  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
  if( agentsys->TASK ) {
    comlib_task_t *task = agentsys->TASK;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      COMLIB_TASK_LOCK( task ) {
        if( COMLIB_TASK__IsAlive( task ) ) {
          // Already suspended?
          if( COMLIB_TASK__IsSuspended( task ) ) {
            suspended = 1;
          }
          // Try to suspend
          else if( (suspended = COMLIB_TASK__Suspend( task, __drain_as_percentage_TCS, timeout_ms )) == 1 ) {
            SYSTEM_VERBOSE( SYSTEM, 0x001, "Output agent suspended" );
          }
          // Error or timeout
          else {
            SYSTEM_WARNING( SYSTEM, 0x002, "Failed to suspend output agent" );
          }
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;
  }
  return suspended;
}



/*******************************************************************//**
 * Check output agent status
 * 
 * Return:  1 if system output agent is suspended
 *          0 if system output agent is not suspended
 *         -1 if no system output agent running
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__is_agent_suspended_SYS_CS( vgx_Graph_t *SYSTEM ) {
  int suspended = -1;
  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
  if( agentsys->TASK ) {
    comlib_task_t *task = agentsys->TASK;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      COMLIB_TASK_LOCK( task ) {
        if( COMLIB_TASK__IsAlive( task ) ) {
          suspended =  COMLIB_TASK__IsSuspended( task );
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;
  }
  return suspended;
}



/*******************************************************************//**
 * Resume output agent
 *
 * Return:  1: Resumed (i.e. not suspended)
 *          0:  
 *         -1: Error / timeout
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__resume_agent_SYS_CS( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  int resumed = 0;
  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
  if( agentsys->TASK ) {
    comlib_task_t *task = agentsys->TASK;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      COMLIB_TASK_LOCK( task ) {
        if( COMLIB_TASK__IsAlive( task ) ) {
          // Not suspended
          if( COMLIB_TASK__IsNotSuspended( task ) ) {
            resumed = 1;
          }
          // Try to resume
          else if( (resumed = COMLIB_TASK__Resume( task, timeout_ms )) == 1 ) {
            SYSTEM_VERBOSE( SYSTEM, 0x001, "Output agent resumed" );
          }
          // Error or timeout
          else {
            SYSTEM_WARNING( SYSTEM, 0x002, "Failed to resume output agent" );
          }
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;
  }
  return resumed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__stop_agent_SYS_CS( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  vgx_OperationSystem_t *agentsys = &SYSTEM->OP.system;
  if( agentsys->TASK ) {
    comlib_task_t *task = agentsys->TASK;
    int timeout_ms = 30000;
    int stopped = 0;
    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      COMLIB_TASK_LOCK( task ) {
        if( (stopped = COMLIB_TASK__Stop( task, __drain_as_percentage_TCS, timeout_ms )) < 0 ) {
          SYSTEM_WARNING( SYSTEM, 0x001, "Forcing output agent exit" );
          stopped = COMLIB_TASK__ForceExit( task, 30000 );
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;

    if( stopped == 1 ) {
      COMLIB_TASK__Delete( &agentsys->TASK );
    }
    else {
      SYSTEM_CRITICAL( SYSTEM, 0x001, "Unresponsive system output agent thread" );
      ret = -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_system__pending_transactions_OPEN( vgx_Graph_t *SYSTEM ) {
  int64_t n = 0;
  vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;

  vgx_TransactionalProducersIterator_t iter = {0};
  if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
    vgx_TransactionalProducer_t *producer;
    while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
      BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
        n += iTransactional.Producer.Length( producer );
      } END_TRANSACTIONAL_IF_ATTACHED;
    }
    iTransactional.Producers.Iterator.Clear( &iter );
  }

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_system__pending_bytes_OPEN( vgx_Graph_t *SYSTEM ) {
  int64_t n = 0;
  vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;

  vgx_TransactionalProducersIterator_t iter = {0};
  if( iTransactional.Producers.Iterator.Init( opsys->producers, &iter ) ) {
    vgx_TransactionalProducer_t *producer;
    while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
      BEGIN_TRANSACTIONAL_IF_ATTACHED( producer ) {
        n += iTransactional.Producer.Bytes( producer );
      } END_TRANSACTIONAL_IF_ATTACHED;
    }
    iTransactional.Producers.Iterator.Clear( &iter );
  }

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__start_consumer_service_OPEN( vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold ) {
  int ret = -1;

  // Stop any previous consumer before starting new one
  iOperation.System_OPEN.ConsumerService.Stop( SYSTEM );

  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    if( opsys->TASK ) {
      comlib_task_t *task = opsys->TASK;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        COMLIB_TASK_LOCK( task ) {
          // Create new service
          if( (opsys->consumer = iTransactional.Consumer.New( SYSTEM, port, durable, snapshot_threshold )) != NULL ) {
            ret = 1;
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
DLL_HIDDEN unsigned _vxdurable_operation_system__bound_port_consumer_service_OPEN( vgx_Graph_t *SYSTEM ) {
  unsigned port = 0;

  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    if( opsys->TASK ) {
      comlib_task_t *task = opsys->TASK;
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        COMLIB_TASK_LOCK( task ) {
          if( opsys->consumer && opsys->consumer->TASK ) {
            port = opsys->consumer->bind_port;
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;

  return port;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_system__stop_consumer_service_OPEN( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    if( opsys->TASK ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        ret = iTransactional.Consumer.Delete( &opsys->consumer );
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
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int64_t _vxdurable_operation_system__return_zero( vgx_Graph_t *graph ) {
  return 0;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_system.h"
  
test_descriptor_t _vgx_vxdurable_operation_system_tests[] = {
  { "VGX Graph Durable Operation Tests", __utest_vxdurable_operation_system },
  {NULL}
};
#endif
