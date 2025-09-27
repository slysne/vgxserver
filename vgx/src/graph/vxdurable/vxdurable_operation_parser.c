/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_parser.c
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




static void             __operation_parser_operation_reset( vgx_OperationParser_t *parser );
static vgx_Graph_t *    __operation_parser_get_graph( vgx_OperationParser_t *parser, objectid_t *graph_obid );
static const char *     __next_token( vgx_OperationParser_t *parser, const char *buffer, int64_t *cnt );
static void             __reset_tokenizer( vgx_OperationParser_t *parser );
static bool             __match_token( operation_parser_token *token, const char *probe );

static unsigned         __operation_parser_initialize( comlib_task_t *self );
static unsigned         __operation_parser_shutdown( comlib_task_t *self );

static int64_t          __force_release_all_thread_vertices( vgx_OperationParser_t *parser );

static int              _vxdurable_operation_parser__reset( vgx_OperationParser_t *parser );

static int              _vxdurable_operation_parser__initialize_CS( struct s_vgx_Graph_t *graph, vgx_OperationParser_t *parser, bool start_thread );
static int              _vxdurable_operation_parser__destroy_CS( vgx_OperationParser_t *parser );
static int              _vxdurable_operation_parser__suspend_CS( vgx_Graph_t *self, int timeout_ms );
static int              _vxdurable_operation_parser__resume_CS( vgx_Graph_t *self );







#define PARSER_ENABLE_VALIDATION_TCS( Parser )        ((Parser)->control.ena_validate = 1)
#define PARSER_DISABLE_VALIDATION_TCS( Parser )       ((Parser)->control.ena_validate = 0)
#define PARSER_IS_VALIDATION_ENABLED_TCS( Parser )    ((Parser)->control.ena_validate != 0)

#define PARSER_ENABLE_EXECUTION_TCS( Parser )         ((Parser)->control.ena_execute = 1)
#define PARSER_DISABLE_EXECUTION_TCS( Parser )        ((Parser)->control.ena_execute = 0)
#define PARSER_IS_EXECUTION_ENABLED_TCS( Parser )     ((Parser)->control.ena_execute != 0)

#define PARSER_MUTE_REGRESSION_TCS( Parser )          ((Parser)->control.mute_regression = 1)
#define PARSER_CATCH_REGRESSION_TCS( Parser )         ((Parser)->control.mute_regression = 0)
#define PARSER_IS_REGRESSION_MUTED_TCS( Parser )      ((Parser)->control.mute_regression != 0)

#define PARSER_ENABLE_STRICT_SERIAL_TCS( Parser )     ((Parser)->control.strict_serial = 1)
#define PARSER_DISABLE_STRICT_SERIAL_TCS( Parser )    ((Parser)->control.strict_serial = 0)
#define PARSER_IS_STRICT_SERIAL_ENABLED_TCS( Parser ) ((Parser)->control.strict_serial != 0)

#define PARSER_ENABLE_CRC_TCS( Parser )               ((Parser)->control.ena_crc = 1)
#define PARSER_DISABLE_CRC_TCS( Parser )              ((Parser)->control.ena_crc = 0)
#define PARSER_IS_CRC_ENABLED_TCS( Parser )           ((Parser)->control.ena_crc != 0)

#define PARSER_SET_RESET_TRIGGER_TCS( Parser )        ((Parser)->control.trg_reset = 1)
#define PARSER_CLEAR_RESET_TRIGGER_TCS( Parser )      ((Parser)->control.trg_reset = 0)
#define PARSER_HAS_RESET_TRIGGER_TCS( Parser )        ((Parser)->control.trg_reset != 0)

#define PARSER_HAS_LOCKED_GRAPH( Parser )             ((Parser)->__locked_graph != NULL)



/*******************************************************************//**
 * 
 * All opcodes are defined by the following include
 * 
 * 
 * 
 * 
 ***********************************************************************
 */
#include "op/opcodes.h"
#include "op/profiles.h"



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __operation_parser_operation_reset( vgx_OperationParser_t *parser ) {
  // Close operation vertex, if we have one acquired
  __operation_parser_close_vertex( parser );
  // Reset opid
  parser->opid = 0;
  // Reset opcode
  __reset_opcode( parser );
  // Release graph, if we have it locked
  __operation_parser_release_graph( parser );
  parser->op_graph = NULL;
  // Reset tokenizer
  __reset_tokenizer( parser );
  // Clear errors
  iString.Discard( &parser->CSTR__error );
  parser->reason = VGX_ACCESS_REASON_NONE;
  // Restore normal execution
  if( PARSER_CONTROL_OPERATION_SKIPPED( parser ) ) {
    parser->control.exe = OPEXEC_NORMAL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __operation_parser_disable_events( vgx_Graph_t *graph ) {
  if( !iSystem.IsSystemGraph( graph ) && iGraphEvent.IsEnabled( graph ) ) {
    vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( graph, 30000 );
    iGraphEvent.NOCS.Disable( graph, &disable_budget );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __operation_parser_get_graph( vgx_OperationParser_t *parser, objectid_t *graph_obid ) {
  // Get graph object (system graph or user graph from registry)
  if( (parser->op_graph = iSystem.GetGraph( graph_obid )) == NULL ) {
    char buf[33];
    PARSER_FORMAT_ERROR( parser, "Unknown graph id: %s", idtostr( buf, graph_obid ) );
  }
  else {
    if( PARSER_CONTROL_LOAD_ALLOCATOR( parser ) ) {
      parser->property_allocator_ref = parser->op_graph->property_allocator_context;
      if( igraphfactory.EventsEnabled() ) {
        __operation_parser_disable_events( parser->op_graph );
      }
    }
    else {
      parser->property_allocator_ref = parser->op_graph->ephemeral_string_allocator_context;
    }
  }

  return parser->op_graph;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static const char * __next_token( vgx_OperationParser_t *parser, const char *buffer, int64_t *cnt ) {
  const char *cursor = buffer;
  const char *end = NULL;
  char c;

  parser->token.data = cursor;
  parser->token.len = 0;

  while( (c = *cursor++) != '\0' ) {

    switch( c ) {

    // Newline
    case '\n':
      // No token built, the newline becomes the token
      if( end == NULL ) {
        end = cursor;
        parser->token.data = cursor - 1;
      }
      parser->token.flags.suspend = false;
      goto TOKEN;

    // Skip
    case '#':
      parser->token.flags.suspend = true;
    case '\t':
    case ' ':
      // No token yet: advance token start
      if( end == NULL ) {
        parser->token.data = cursor;
        continue;
      }
      // Token complete
      else {
        goto TOKEN;
      }

    // Accept next token character
    default:
      if( !parser->token.flags.suspend ) {
        end = cursor;
      }

    }
  }

TOKEN:
  if( end != NULL ) {
    parser->token.len = (int)(end - parser->token.data);
    if( !parser->token.flags.suspend ) {
      __opdata_crc32c( parser->token.data, parser->token.len, &parser->crc );
    }
  }

  // Number of bytes processed
  *cnt += cursor - buffer;

  return end;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __reset_tokenizer( vgx_OperationParser_t *parser ) {
  parser->state = OPSTATE_EXPECT_OP;
  parser->token.data = NULL;
  parser->token.len = 0;
  parser->token.flags.suspend = false;
  parser->token.flags.pending = false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __match_token( operation_parser_token *token, const char *probe ) {
  // NOTE: probe is nul-term, token has explicit length
  int n = token->len;
  const char *t = token->data;
  const char *p = probe;

  // All chars in token
  while( n > 0 ) {
    // Probe must not be exhausted and the token char must match the probe char
    if( *p != '\0' && *t++ == *p++ ) {
      --n;
    }
    else {
      return false;
    }
  }

  // End of probe must now be reached
  return *p == '\0';
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __throttle_internal_feed_OPEN( int64_t t0_ns, int64_t *t1_ns, WAITABLE_TIMER *Timer, vgx_OperationCounters_t *counters, vgx_OperationFeedRates_t *limits, vgx_OperationFeedRates_t *current_rates ) {
  int64_t delay_ns = 0;
  if( limits && counters && current_rates ) {

    bool throttle = false;
    bool refresh_t1 = false;
    bool t1_stale = true;

    int64_t t_call_ns = 0;

    do {
      // Refresh t1 ?
      if( refresh_t1 ) {
        *t1_ns = __GET_CURRENT_NANOSECOND_TICK();
        if( t_call_ns == 0 ) {
          t_call_ns = *t1_ns;
        }
        if( t1_stale == false ) {
          delay_ns = *t1_ns - t_call_ns;
        }
        refresh_t1 = false;
        t1_stale = false;
      }

      // Compute elapse time
      double t_elapse_ms = (*t1_ns - t0_ns) / 1e6;

      // Safeguard
      if( t_elapse_ms > 60e3 ) {
        return -1;
      }
      // Measure counters per millisecond and throttle if needed
      else {
        double inv_t_ms = t_elapse_ms > 0 ? 1.0 / t_elapse_ms : 10000.0;
        current_rates->tpms = counters->n_transactions * inv_t_ms;
        current_rates->opms = counters->n_operations * inv_t_ms;
        current_rates->cpms = counters->n_opcodes * inv_t_ms;
        current_rates->bpms = counters->n_bytes * inv_t_ms;
        // Throttle if measured rate(s) higher than limit(s)
        throttle = (limits->tpms > 0.0 && current_rates->tpms > limits->tpms) ||
                   (limits->opms > 0.0 && current_rates->opms > limits->opms) ||
                   (limits->cpms > 0.0 && current_rates->cpms > limits->cpms) ||
                   (limits->bpms > 0.0 && current_rates->bpms > limits->bpms);
        if( throttle ) {
          if( t1_stale == false ) {
            if( Timer ) {
              sleep_nanoseconds( *Timer, 10000 );
            }
            else {
              sleep_milliseconds( 1 );
            }
          }
          refresh_t1 = true;
        }
      }
    } while( throttle );
  }
  return delay_ns;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_parser__feed_operation_data_OPEN( vgx_OperationParser_t *parser, const char *input, const char **next, WAITABLE_TIMER *Timer, vgx_OperationCounters_t *counters_SYS_CS, CString_t **CSTR__error, vgx_op_parser_error_t *perr ) {

  vgx_OperationCounters_t local_counters = {0};

  int64_t ret_nops = 0;

  const char *cursor = input;

#define MAX_FEED_BATCH_BEFORE_YIELD (1LL<<26)

#define STATE (parser->state)
#define JUMP_STATE( Label ) goto Label
#define NEXT_STATE( State ) (parser->state = (State)); JUMP_STATE( NEXT_TOKEN )


#define TRANSIENT_ERROR( Errcode, BackoffMillisec ) do {   \
  errcode = (Errcode);                      \
  ++(perr->n_transient);                    \
  perr->errstate = OPSTATE_OPERR_TRANSIENT; \
  perr->backoff_ms = BackoffMillisec;       \
  JUMP_STATE( OPERR_TRANSIENT );            \
} WHILE_ZERO


#define PERMANENT_ERROR( Errcode ) do {   \
  errcode = (Errcode);                    \
  ++(perr->n_permanent);                  \
  STATE = OPSTATE_OPERR_PERMANENT;        \
  perr->errstate = STATE;                 \
  JUMP_STATE( OPERR_PERMANENT );          \
} WHILE_ZERO


#define TRANSACTION_ERROR( Errcode ) do { \
  errcode = (Errcode);                    \
  ++(perr->n_transaction);                \
  STATE = OPSTATE_OPERR_TRANSACTION;      \
  perr->errstate = STATE;                 \
  JUMP_STATE( OPERR_TRANSACTION );        \
} WHILE_ZERO


#define GET_ERRSTATE()            perr->errstate

#define CLEAR_ERRSTATE()          (perr->errstate = OPSTATE_EXPECT_ANY)

#define RECOVER_STATE( Label )    CLEAR_ERRSTATE(); JUMP_STATE( Label )


  DWORD CRC = 0;

  int errcode = 0;

#define NEXT_STATE_CRC( State )   (CRC = parser->crc); NEXT_STATE( State )


#define tokdata           parser->token.data
#define TOKEN( ConstStr ) __match_token( &parser->token, ConstStr )

  int synerr = 0;

#define SYNTAX_ERROR( Message ) do {                              \
  ++(perr->n_syntax);                                             \
  (synerr = __operation_parser_syntax_error( parser, Message ));  \
  STATE = OPSTATE_OPERR_RECOVERY;                                 \
  perr->errstate = STATE;                                         \
  JUMP_STATE( OPERR_RECOVERY );                                   \
} WHILE_ZERO


  objectid_t obid;
  objectid_t transid;
  int64_t sn = 0;
  int64_t mstr_sn = 0;
  int64_t tms = 0;
  QWORD qword;
  DWORD dword;
  WORD word;

  int operator_complete;

  char idbufA[33];
  char idbufB[33];

  const char *errstr = NULL;
  

  vgx_OperationFeedRates_t current_rates = {0};
  vgx_OperationFeedRates_t current_feed_limits = {0};
  vgx_OperationFeedRates_t *p_feed_limits = NULL;
  bool log_throttle_refresh = false;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      vgx_OperationFeedRates_t *limits = SYSTEM->OP.system.in_feed_limits_CS;
      if( limits ) {
        if( limits->refresh ) {
          limits->refresh = false;
          log_throttle_refresh = true;
        }
        if( limits->tpms > 0 || limits->opms > 0 || limits->cpms > 0 || limits->bpms > 0 ) {
          current_feed_limits = *limits;
          p_feed_limits = &current_feed_limits;
        }
      }
    } GRAPH_RELEASE;
  }

  if( log_throttle_refresh ) {
    if( p_feed_limits ) {
      double tps = p_feed_limits->tpms > 0.0 ? p_feed_limits->tpms * 1000 : INFINITY;
      double ops = p_feed_limits->opms > 0.0 ? p_feed_limits->opms * 1000 : INFINITY;
      double cps = p_feed_limits->cpms > 0.0 ? p_feed_limits->cpms * 1000 : INFINITY;
      double bps = p_feed_limits->bpms > 0.0 ? p_feed_limits->bpms * 1000 : INFINITY;
      PARSER_INFO( parser, 0x000, "Feed throttle: tps=%.0f ops=%.0f cps=%.0f bps=%.0f", tps, ops, cps, bps );
    }
    else {
      PARSER_INFO( parser, 0x000, "Feed throttle: unlimited" );
    }
  }

  XTRY {

    if( cursor ) {

      // Reset warning mute counters
      warn_ignore_tail_count = 0;
      warn_ignore_head_count = 0;

      // Timestamps
      int64_t t0_ns = __GET_CURRENT_NANOSECOND_TICK();
      int64_t t1_ns = t0_ns;

      // Process all tokens
      while( (cursor = __next_token( parser, cursor, &local_counters.n_bytes )) != NULL ) {

        if( TOKEN( "\n" ) ) {
          continue;
        }

        switch( STATE ) {
        // ----------------------------------------------------------------
        //
        // TRANSACTION <transid> <sn> <master_sn>
        // ^^^^^^^^^^^
        //
        case OPSTATE_EXPECT_TRANSACTION:
        OPSTATE_TRANSACTION:
          // TRANSACTION
          if( TOKEN( kwd_TRANSACTION ) ) {
            if( !idnone( &parser->transid ) ) {
              PARSER_WARNING( parser, 0xA01, "previous transaction truncated: %s", idtostr( idbufA, &parser->transid ) );
              idunset( &parser->transid );
            }


            if( PARSER_HAS_VERTEX_LOCKS( parser ) ) {
              int64_t n = __force_release_all_thread_vertices( parser );
              if( n > 0 ) {
                PARSER_CRITICAL( parser, 0xA04, "%lld vertex locks at beginning of transaction, forcing release", n );
                TRANSIENT_ERROR( 0xE01, 5 );
              }
              else {
                PARSER_WARNING( parser, 0xA05, "Incorrect vertex lock counter=%d at beginning of transaction, resetting", PARSER_GET_VERTEX_LOCKS( parser ) );
                PARSER_RESET_VERTEX_LOCKS( parser );
              }
            }

            NEXT_STATE( OPSTATE_EXPECT_TRANSACTION_ID );
          }
          // OP (non-transactional operation)
          else if( TOKEN( kwd_OP ) ) {
            PARSER_VERBOSE( parser, 0x004, "non-transactional %s encountered", kwd_OP );
            JUMP_STATE( OPSTATE_OP );
          }
          // IDLE
          else if( TOKEN( kwd_IDLE ) ) {
            JUMP_STATE( OPSTATE_IDLE );
          }
          // RESYNC
          else if( TOKEN( kwd_RESYNC ) ) {
            JUMP_STATE( OPSTATE_RESYNC );
          }
          // ATTACH
          else if( TOKEN( kwd_ATTACH ) ) {
            JUMP_STATE( OPSTATE_ATTACH );
          }
          // SYNTAX ERROR
          else {
            SYNTAX_ERROR( "expected 'TRANSACTION ...'" );
          }

        // ----------------------------------------------------------------
        //
        // TRANSACTION <transid> <sn> <master_sn>
        //             ^^^^^^^^^
        //
        case OPSTATE_EXPECT_TRANSACTION_ID:
          // <transid>
          transid = strtoid( tokdata );
          if( idnone( &transid ) ) {
            SYNTAX_ERROR( "expected transaction <transid>" );
          }
          idcpy( &parser->transid, &transid );
          NEXT_STATE( OPSTATE_EXPECT_TRANSACTION_SN );

        // ----------------------------------------------------------------
        //
        // TRANSACTION <transid> <sn> <master_sn>
        //                       ^^^^
        //
        case OPSTATE_EXPECT_TRANSACTION_SN:
          // <sn>
          if( hex_to_QWORD( tokdata, &qword ) ) {
            sn = (int64_t)qword;
            // Check that serial number is exactly one greater than previous
            if( PARSER_CONTROL_CHECK_REGRESSION( parser ) && parser->sn != 0 && sn != parser->sn + 1 ) {
              // Serial number has skipped a range
              if( sn > parser->sn ) {
                int64_t gap = sn - parser->sn - 1;
                PARSER_INFO( parser, 0xA05, "Serial number gap (%lld transactions) between [%lld - %lld] for transaction %s", gap, parser->sn, sn, idtostr( idbufA, &parser->transid ) );
              }
              // Already applied transaction, skip
              else {
                int64_t regress = sn - parser->sn;
                PARSER_FORMAT_ERROR( parser, "Regression at sn=%lld (%lld steps)", sn, regress );
                TRANSACTION_ERROR( 0xE02 );
              }
            }
            // Update
            parser->sn = sn;
          }
          else {
            SYNTAX_ERROR( "expected transaction <sn>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_TRANSACTION_MSTRSN );

        // ----------------------------------------------------------------
        //
        // TRANSACTION <transid> <sn> <master_sn>
        //                            ^^^^^^^^^^^
        //
        case OPSTATE_EXPECT_TRANSACTION_MSTRSN:
          // <master_sn>
          if( hex_to_QWORD( tokdata, &qword ) ) {
            mstr_sn = (int64_t)qword;
            // We don't process the master serial here. It has been processed
            // in the consumer service.
          }
          else {
            SYNTAX_ERROR( "expected transaction <master_sn>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_OP );


        // ----------------------------------------------------------------
        //
        // IDLE <tms>
        // ^^^^
        //
        case OPSTATE_EXPECT_IDLE:
        OPSTATE_IDLE:
          // IDLE
          if( TOKEN( kwd_IDLE ) ) {
            NEXT_STATE( OPSTATE_EXPECT_IDLE_TMS );
          }
          else {
            SYNTAX_ERROR( "expected 'IDLE ...'" );
          }

        // ----------------------------------------------------------------
        //
        // IDLE <tms>
        //       ^^^
        //
        case OPSTATE_EXPECT_IDLE_TMS:
          // <tms>
          if( !hex_to_QWORD( tokdata, &qword ) ) {
            SYNTAX_ERROR( "expected idle <tms>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_TRANSACTION );


        // ----------------------------------------------------------------
        //
        // RESYNC <resyncid> <ndiscarded>
        // ^^^^^^
        //
        case OPSTATE_EXPECT_RESYNC:
        OPSTATE_RESYNC:
          // RESYNC
          if( TOKEN( kwd_RESYNC ) ) {
            NEXT_STATE( OPSTATE_EXPECT_RESYNC_ID );
          }
          else {
            SYNTAX_ERROR( "expected 'RESYNC ...'" );
          }

        // ----------------------------------------------------------------
        //
        // RESYNC <resyncid> <ndiscarded>
        //         ^^^^^^^^
        //
        case OPSTATE_EXPECT_RESYNC_ID:
          // <resyncid>
          transid = strtoid( tokdata );
          if( idnone( &transid ) ) {
            SYNTAX_ERROR( "expected resyncid" );
          }
          NEXT_STATE( OPSTATE_EXPECT_RESYNC_BSZ );

        // ----------------------------------------------------------------
        //
        // RESYNC <resyncid> <ndiscarded>
        //                    ^^^^^^^^^^
        //
        case OPSTATE_EXPECT_RESYNC_BSZ:
          // <ndiscarded>
          if( !hex_to_QWORD( tokdata, &qword ) ) {
            SYNTAX_ERROR( "expected resync <ndiscarded>" );
          }
          PARSER_VERBOSE( parser, 0x007, "Operation stream RESYNC %s %llu", idtostr( idbufA, &transid ), qword );
          NEXT_STATE( OPSTATE_EXPECT_TRANSACTION );

        // ----------------------------------------------------------------
        //
        // ATTACH <protocol> <version>
        // ^^^^^^
        //
        case OPSTATE_EXPECT_ATTACH:
        OPSTATE_ATTACH:
          // ATTACH
          if( TOKEN( kwd_ATTACH ) ) {
            NEXT_STATE( OPSTATE_EXPECT_ATTACH_PROTO );
          }
          else {
            SYNTAX_ERROR( "expected 'ATTACH ...'" );
          }

        // ----------------------------------------------------------------
        //
        // ATTACH <protocol> <version>
        //         ^^^^^^^^
        //
        case OPSTATE_EXPECT_ATTACH_PROTO:
          // <protocol>
          // TODO: Validate protocol
          if( !hex_to_DWORD( tokdata, &dword ) ) {
            SYNTAX_ERROR( "expected <protocol>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_ATTACH_VER );

        // ----------------------------------------------------------------
        //
        // ATTACH <protocol> <version>
        //                    ^^^^^^^
        //
        case OPSTATE_EXPECT_ATTACH_VER:
          // <version>
          // TODO: Validate version
          if( !hex_to_DWORD( tokdata, &dword ) ) {
            SYNTAX_ERROR( "expected <version>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_TRANSACTION );

        // ----------------------------------------------------------------
        //
        // COMMIT <transid> <tms> <crc>
        // ^^^^^^
        //
        case OPSTATE_EXPECT_COMMIT:
        OPSTATE_COMMIT:
          // COMMIT
          if( TOKEN( kwd_COMMIT ) ) {
            if( idnone( &parser->transid ) ) {
              PARSER_REASON( parser, 0xE08, "unexpected %s after non-transactional operation sequence", kwd_COMMIT );
            }
            NEXT_STATE( OPSTATE_EXPECT_COMMIT_ID );
          }
          // SYNTAX ERROR
          else {
            SYNTAX_ERROR( "expected 'COMMIT ...'" );
          }

        // ----------------------------------------------------------------
        //
        // COMMIT <transid> <tms> <crc>
        //        ^^^^^^^^^
        //
        case OPSTATE_EXPECT_COMMIT_ID:
          // <transid>
          transid = strtoid( tokdata );
          if( idnone( &transid ) ) {
            SYNTAX_ERROR( "expected transid" );
          }
          else if( !idmatch( &transid, &parser->transid ) ) {
            PARSER_WARNING( parser, 0xA09, "transaction id mismatch: current transaction '%s' terminated by '%s'", idtostr( idbufA, &parser->transid ), idtostr( idbufB, &transid ) );
          }
          NEXT_STATE( OPSTATE_EXPECT_COMMIT_TMS );

        // ----------------------------------------------------------------
        //
        // COMMIT <transid> <tms> <crc>
        //                  ^^^^^
        //
        case OPSTATE_EXPECT_COMMIT_TMS:
          // <tms>
          if( hex_to_QWORD( tokdata, &qword ) ) {
            tms = (int64_t)qword;
          }
          else {
            SYNTAX_ERROR( "expected commit <tms>" );
          }
          NEXT_STATE( OPSTATE_EXPECT_COMMIT_CRC );

        // ----------------------------------------------------------------
        //
        // COMMIT <transid> <tms> <crc>
        //                        ^^^^^
        //
        case OPSTATE_EXPECT_COMMIT_CRC:
          // <crc>
          if( !hex_to_DWORD( tokdata, &dword ) ) {
            SYNTAX_ERROR( "expected commit <crc>" );
          }

          if( parser->control.crc ) {
            // TODO: Verify CRC
          }

          // TODO:
          //   Transactions are currently only part of the syntax
          //   and not really used for anything.
          //
          //   Implement Transactions!
          //
          idunset( &parser->transid );

          local_counters.n_transactions++;

          int64_t lag = _vgx_graph_milliseconds( SYSTEM ) - tms;
          GRAPH_LOCK( SYSTEM ) {
            SYSTEM->tx_input_lag_ms_CS = lag > 0 ? lag : 0;
          } GRAPH_RELEASE;

          if( PARSER_HAS_VERTEX_LOCKS( parser ) ) {
            PARSER_CRITICAL( parser, 0xA0A, "%d vertex locks remain at end of transaction ", PARSER_GET_VERTEX_LOCKS( parser ) );
          }

          if( cursor - input < MAX_FEED_BATCH_BEFORE_YIELD ) {
            NEXT_STATE( OPSTATE_EXPECT_TRANSACTION );
          }
          else {
            JUMP_STATE( OPERR_TRANSIENT );
          }



        // ----------------------------------------------------------------
        //
        // OP <optype> [ <graphid> [ <vertexid> ] ]
        // ^^
        //
        case OPSTATE_EXPECT_OP:
        OPSTATE_OP:
          // OP
          if( TOKEN( kwd_OP ) ) {
            parser->op_graph = NULL;
            parser->op_vertex_WL = NULL; // __n_disallowed ??????
            parser->property_allocator_ref = NULL;
            parser->crc = 0;
            __opdata_crc32c( kwd_OP, sz_OP, &parser->crc );

            // Throttle if nothing is locked
            if( p_feed_limits ) {
              if( !PARSER_HAS_VERTEX_LOCKS( parser ) && !PARSER_HAS_LOCKED_GRAPH( parser ) ) {
                int64_t delay_ns;
                if( (delay_ns = __throttle_internal_feed_OPEN( t0_ns, &t1_ns, Timer, &local_counters, p_feed_limits, &current_rates )) < 0 ) {
                  TRANSIENT_ERROR( 0xE03, 5 );
                }
              }
            }

            NEXT_STATE( OPSTATE_EXPECT_OPTYPE );
          }
          // COMMIT
          else if( TOKEN( kwd_COMMIT ) ) {
            JUMP_STATE( OPSTATE_COMMIT );
          }
          // SYNTAX ERROR
          else {
            SYNTAX_ERROR( "expected operation block 'OP ...'" );
          }

        // ----------------------------------------------------------------
        //
        // OP <optype> [ <graphid> [ <vertexid> ] ]
        //    ^^^^^^^^
        //
        case OPSTATE_EXPECT_OPTYPE:
          // <optype>
          if( hex_to_WORD( tokdata, &word ) ) {
            parser->optype = word;
            switch( parser->optype ) {
            case OPTYPE_SYSTEM:
              // SYSTEM graph
              if( PARSER_CONTROL_LOAD_GRAPH( parser ) ) {
                if( __operation_parser_get_graph( parser, NULL ) == NULL ) {
                  PERMANENT_ERROR( 0xE04 );
                }
              }
              NEXT_STATE( OPSTATE_EXPECT_OPERATOR );
            case OPTYPE_GRAPH_OBJECT:
            case OPTYPE_GRAPH_STATE:
            case OPTYPE_VERTEX_OBJECT:
            case OPTYPE_VERTEX_LOCK:
            case OPTYPE_VERTEX_RELEASE:
              NEXT_STATE( OPSTATE_EXPECT_GRAPH_OBID );
            default:
              SYNTAX_ERROR( "undefined operation type" );
            }
          }
          SYNTAX_ERROR( "expected <optype>" );

        // ----------------------------------------------------------------
        //
        // OP <optype> <graphid> [ <vertexid> ]
        //             ^^^^^^^^^
        //
        case OPSTATE_EXPECT_GRAPH_OBID:
          obid = strtoid( tokdata );
          if( idnone( &obid ) ) {
            SYNTAX_ERROR( "expected graph objectid" );
          }
          // graph <obid>
          if( PARSER_CONTROL_LOAD_GRAPH( parser ) ) {
            if( __operation_parser_get_graph( parser, &obid ) == NULL ) {
              PERMANENT_ERROR( 0xE05 );
            }
          }

          // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          //
          // TODO TODO TODO TODO
          // 
          // ENSURE op_graph is writable, and prevent op_graph from 
          //        becoming readonly while we operate on it!!!!
          // 
          //
          // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

          switch( parser->optype ) {
          // GRAPH operation
          case OPTYPE_GRAPH_OBJECT:
          case OPTYPE_GRAPH_STATE:
          case OPTYPE_VERTEX_LOCK:
          case OPTYPE_VERTEX_RELEASE:
            // Proceed to operator
            NEXT_STATE( OPSTATE_EXPECT_OPERATOR );
          // VERTEX object operation
          case OPTYPE_VERTEX_OBJECT:
            // Now get vertex obid
            NEXT_STATE( OPSTATE_EXPECT_VERTEX_OBID );
          default:
            SYNTAX_ERROR( "unexpected operation parameter" );
          }

        // ----------------------------------------------------------------
        //
        // OP <optype> <graphid> <vertexid>
        //                       ^^^^^^^^^^
        //
        case OPSTATE_EXPECT_VERTEX_OBID:
          obid = strtoid( tokdata );
          if( idnone( &obid ) ) {
            SYNTAX_ERROR( "expected vertex objectid" );
          }

          if( PARSER_CONTROL_OPEN_VERTEX( parser ) ) {
            if( __operation_parser_open_vertex( parser, &obid ) == NULL ) {
              // Timeout, we must abort parsing at time point
              // NOTE: It seems like this will never trigger since the open vertex call above will
              //       keep trying indefinitely.
              if( __is_access_reason_transient( parser->reason ) ) {
                TRANSIENT_ERROR( 0xE06, 1 );
              }
              // Vertex does not exist: ignore operator
              else if( __is_access_reason_noexist( parser->reason ) ) {
                if( warn_ignore_tail_count++ < MAX_IGNORE_WARNINGS ) {
                  PARSER_WARNING( parser, 0x00E, "Operation ignored, vertex '%016llx%016llx' no longer exists (tx=%016llx%016llx)", obid.H, obid.L, parser->transid.H, parser->transid.L );
                }
                // *** RETIRE ***
                parser->retiref( parser );
                // Reset
                __operation_parser_operation_reset( parser );
                // Wait for next "OP"
                STATE = OPSTATE_OPERR_RECOVERY;
                JUMP_STATE( OPERR_RECOVERY );
              }
              // Some other error was generated when attempting to open vertex (typically regression)
              else if( __is_access_reason_error( parser->reason ) ) {
                TRANSACTION_ERROR( 0xE07 );
              }
              // Something that should not happen
              else {
                TRANSACTION_ERROR( 0xE08 );
              }
            }
          }

          // Proceed to operator
          NEXT_STATE( OPSTATE_EXPECT_OPERATOR );

        // ----------------------------------------------------------------
        //
        // opn <opcode> ...
        //
        //
        case OPSTATE_EXPECT_OPERATOR:
          // ENDOP
          if( TOKEN( kwd_ENDOP ) ) {
            JUMP_STATE( OPSTATE_ENDOP );
          }
          // opn
          else {
            JUMP_STATE( OPSTATE_OPNAME );
          }

        // ----------------------------------------------------------------
        //
        // opn <opcode> ...
        // ^^^
        //
        case OPSTATE_EXPECT_OPNAME:
        OPSTATE_OPNAME:
          // Record the mnemonic into parser
          *(DWORD*)parser->op_mnemonic = 0;
          strncpy( (char*)parser->op_mnemonic, tokdata, 3 );
          NEXT_STATE( OPSTATE_EXPECT_OPCODE );

        // ----------------------------------------------------------------
        //
        // opn <opcode> ...
        //     ^^^^^^^^
        //
        case OPSTATE_EXPECT_OPCODE:
          // Decode opcode and map it to the appropriate parser function
          if( __decode_opcode( parser ) < 0 ) {
            PERMANENT_ERROR( 0xE09 );
          }
          JUMP_STATE( OPSTATE_OPARG );

        // ----------------------------------------------------------------
        //
        // opn <opcode> ...
        //              ^^^
        //
        case OPSTATE_EXPECT_OPARG:
        OPSTATE_OPARG:
          // Parse operator argument
          if( (operator_complete = parser->parsef( parser )) > 0 ) {
            // Operator ready to execute
            // *** EXECUTE ***
            if( PARSER_CONTROL_EXEC_OPCODE( parser ) ) {
              if( parser->execf( parser ) < 0 ) {
                // Something is READONLY. Transient error with a long backoff
                if( __is_access_reason_readonly( parser->reason ) ) {
                  TRANSIENT_ERROR( 0xE0A, 5000 );
                }
                // Transient access error with a short backoff
                else if( __is_access_reason_transient( parser->reason ) ) {
                  TRANSIENT_ERROR( 0xE0B, 5 );
                }
                // An error that affects the entire transaction
                else if( __is_access_reason_error( parser->reason ) ) {
                  TRANSACTION_ERROR( 0xE0C );
                }
                // A permanent operator error
                else {
                  errstr = parser->CSTR__error ? CStringValue( parser->CSTR__error ) : "?";
                  __format_error_string( CSTR__error, "Execution error (operator:'%s', error:'%s', reason:'%03X')", parser->OPERATOR.op.name, errstr, parser->reason );
                  PERMANENT_ERROR( 0xE0D );
                }
              }
            }
            // *** RETIRE ***
            parser->retiref( parser );
            // Count
            local_counters.n_opcodes++;
            // Complete, proceed to next operator
            NEXT_STATE( OPSTATE_EXPECT_OPERATOR );
          }
          // Continue parsing operator
          else if( operator_complete == 0 ) {
            NEXT_STATE( OPSTATE_EXPECT_OPARG );
          }
          // Failed to parse operator
          else {
            // Details available
            if( __has_access_reason( &parser->reason ) || parser->CSTR__error ) {
              // Transation error (most likely regression)
              if( parser->reason == VGX_ACCESS_REASON_BAD_CONTEXT ) {
                TRANSACTION_ERROR( 0xE0E );
              }
              // Operator permanent error
              else {
                errstr = parser->CSTR__error ? CStringValue( parser->CSTR__error ) : "";
                __format_error_string( CSTR__error, "Parser error (operator:'%s', error:'%s', reason:'%03X')", parser->OPERATOR.op.name, errstr, parser->reason );
                PERMANENT_ERROR( 0xE0F );
              }
            }
            // No details available, syntax error
            else {
              SYNTAX_ERROR( "operator argument" );
            }
          }

        // ----------------------------------------------------------------
        //
        // ENDOP [<opid> <tms>] <crc>
        // ^^^^^ 
        //
        case OPSTATE_EXPECT_ENDOP:
        OPSTATE_ENDOP:
          switch( parser->optype ) {
          case OPTYPE_GRAPH_OBJECT:
          case OPTYPE_VERTEX_OBJECT:
            NEXT_STATE( OPSTATE_EXPECT_OPID );
          case OPTYPE_SYSTEM:
          case OPTYPE_GRAPH_STATE:
          case OPTYPE_VERTEX_LOCK:
          case OPTYPE_VERTEX_RELEASE:
            NEXT_STATE_CRC( OPSTATE_EXPECT_CRC );
          default:
            PERMANENT_ERROR( 0xE10 );
          }

        // ----------------------------------------------------------------
        //
        // ENDOP <opid> <tms> <crc>
        //       ^^^^^^  
        //
        case OPSTATE_EXPECT_OPID:
          // <opid>
          if( hex_to_QWORD( tokdata, &qword ) ) {
            parser->opid = (int64_t)qword;
            NEXT_STATE( OPSTATE_EXPECT_TMS );
          }
          else {
            SYNTAX_ERROR( "expected <opid>" );
          }

        // ----------------------------------------------------------------
        //
        // ENDOP <opid> <tms> <crc>
        //              ^^^^^
        //
        case OPSTATE_EXPECT_TMS:
          // <tms>
          if( hex_to_QWORD( tokdata, &qword ) ) {
            // TODO: what to do with tms?
            NEXT_STATE_CRC( OPSTATE_EXPECT_CRC );
          }
          else {
            SYNTAX_ERROR( "expected <tms>" );
          }

        // ----------------------------------------------------------------
        //
        // ENDOP [<opid> <tms>] <crc>
        //                      ^^^^^
        //
        case OPSTATE_EXPECT_CRC:
          // <crc>
          if( hex_to_DWORD( tokdata, &dword ) ) {
            if( parser->control.crc && dword != CRC ) {
              PARSER_CRITICAL( parser, 0x014, "CRC mismatch: expected %08X, got %08X", dword, CRC );
              PERMANENT_ERROR( 0xE11 );
            }
            __operation_parser_operation_reset( parser );
            local_counters.n_operations++;
            NEXT_STATE( OPSTATE_EXPECT_OP );
          }
          else {
            SYNTAX_ERROR( "expected <crc>" );
          }

        // ----------------------------------------------------------------
        // Operation error. We need to stay in this state until we
        // encounter the next "OP" token.
        //
        //
        case OPSTATE_OPERR_RECOVERY:
          // Transaction level error: stay in recovery until next transaction
          if( GET_ERRSTATE() == OPSTATE_OPERR_TRANSACTION ) {
            // Reached new "TRANSACTION"
            if( TOKEN( kwd_TRANSACTION ) ) {
              // Recovery
              RECOVER_STATE( OPSTATE_TRANSACTION );
            }
          }
          else {
            // Reached new "TRANSACTION"
            if( TOKEN( kwd_TRANSACTION ) ) {
              // Recovery
              RECOVER_STATE( OPSTATE_TRANSACTION );
            }
            // First "OP" encountered after error in previous OP
            else if( TOKEN( kwd_OP ) ) {
              // Recovery
              RECOVER_STATE( OPSTATE_OP );
            }
            // Reached "COMMIT"
            else if( TOKEN( kwd_COMMIT ) ) {
              // Recovery
              RECOVER_STATE( OPSTATE_COMMIT );
            }
          }
          OPERR_RECOVERY:
          // We are still in error region.
          JUMP_STATE( NEXT_TOKEN );

        // ----------------------------------------------------------------
        // Transient error. We need to stop parsing and execution
        // temporarily and try again later. Set current token as
        // pending to indicate this state. Reset the cursor back
        // to the start of the pending token. When parsing resumes
        // in the future it will start by parsing this token again
        // and retry the same operation.
        case OPSTATE_OPERR_TRANSIENT:
        OPERR_TRANSIENT:
          JUMP_STATE( OPSTATE_YIELD );

        case OPSTATE_PARSER_YIELD:
        OPSTATE_YIELD:
          // Back up cursor to the pending token before we exit
          cursor = parser->token.data;
          // Terminate
          JUMP_STATE( EXIT_PARSER );

        // ----------------------------------------------------------------
        // Permanent error occurred in the current operation. We will
        // abandon all processing for this operation ignore all future tokens
        // until the next "OP" is encountered.
        //
        case OPSTATE_OPERR_PERMANENT:
        OPERR_PERMANENT:
          errstr = parser->CSTR__error ? CStringValue( parser->CSTR__error ) : "unknown error";
          PARSER_REASON( parser, 0x019, "Error %03X operator:'%s' message:'%s' reason=0x%03X", errcode, parser->OPERATOR.op.name, errstr, parser->reason );
          goto OPERR_ENTER_RECOVERY;
        case OPSTATE_OPERR_TRANSACTION:
        OPERR_TRANSACTION:
          errstr = parser->CSTR__error ? CStringValue( parser->CSTR__error ) : "unknown error";
          PARSER_REASON( parser, 0x01A, "Transaction error %03X tx=%s: %s", errcode, idtostr( idbufA, &parser->transid ), errstr );
          idunset( &parser->transid );
          goto OPERR_ENTER_RECOVERY;
        OPERR_ENTER_RECOVERY:
          // *** RETIRE ***
          parser->retiref( parser );
          // Reset parser to recover at next TRANSACTION
          _vxdurable_operation_parser__reset( parser );
          STATE = OPSTATE_OPERR_RECOVERY;
          JUMP_STATE( OPERR_RECOVERY );


        // ----------------------------------------------------------------
        //
        //
        //
        //
        default:
          THROW_CRITICAL_MESSAGE( CXLIB_ERR_GENERAL, 0x01B, "Internal parser error, bad state: %x", STATE );
        }

      NEXT_TOKEN:
        continue;

      EXIT_PARSER:
        break;
      }

    }
    else {
      cursor = NULL; // force all input consumed
    }

    ret_nops = local_counters.n_operations;

  }
  XCATCH( errocde ) {
    // *** RETIRE ***
    parser->retiref( parser );
    // Reset
    __operation_parser_operation_reset( parser );
    ret_nops = -1;
  }
  XFINALLY {
    __operation_parser_release_graph( parser );
  }


  if( warn_ignore_tail_count > MAX_IGNORE_WARNINGS ) {
    int64_t n = warn_ignore_tail_count - MAX_IGNORE_WARNINGS;
    PARSER_WARNING( parser, 0x00E, "%lld more similar to: Operation ignored, vertex '...' no longer exists (tx=...)", n );
  }
  if( warn_ignore_head_count > MAX_IGNORE_WARNINGS ) {
    int64_t n = warn_ignore_head_count - MAX_IGNORE_WARNINGS;
    OPEXEC_WARNING( parser, "%lld more similar to: (...)-[...]->(...) ignored, head vertex no longer exists", n );
  }

  if( counters_SYS_CS && SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      counters_SYS_CS->n_transactions += local_counters.n_transactions;
      counters_SYS_CS->n_opcodes += local_counters.n_opcodes;
      counters_SYS_CS->n_operations += local_counters.n_operations;
      counters_SYS_CS->n_bytes += local_counters.n_bytes;
    } GRAPH_RELEASE;
  }

  // Return the input position after we've made as much progress
  // as we can. This will be NULL if everything was consumed.
  *next = cursor;

  return ret_nops;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__reset_OPEN( vgx_OperationParser_t *parser ) {
  int ret = 0;
  comlib_task_t *task = parser->TASK;

  // Wait for parser task to become idle
  if( task ) {
    COMLIB_TASK_LOCK( task ) {
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsBusy( task ), 10000, NULL ) {
        COMLIB_TASK_SUSPEND_MILLISECONDS( task, 10 );
      } END_TIME_LIMITED_WHILE;
      // Idle, now reset
      if( !COMLIB_TASK__IsBusy( task ) ) {
        PARSER_SET_RESET_TRIGGER_TCS( parser );
        BEGIN_TIME_LIMITED_WHILE( PARSER_HAS_RESET_TRIGGER_TCS( parser ), 10000, NULL ) {
          COMLIB_TASK_SUSPEND_MILLISECONDS( task, 10 );
        } END_TIME_LIMITED_WHILE;
        // Reset not performed
        if( PARSER_HAS_RESET_TRIGGER_TCS( parser ) ) {
          PARSER_CLEAR_RESET_TRIGGER_TCS( parser );
          ret = -1;
        }
        else {
          ret = 0;
        }
      }
      // Timeout
      else {
        ret = -1;
      }
    } COMLIB_TASK_RELEASE;
  }
  // We have no parser task, just reset
  else {
    ret = _vxdurable_operation_parser__reset( parser );
  }
  
  return ret;
}



#define __SOFT_INPUT_THRESHOLD_MIN  (1LL << 26) /*  64 MB soft limit lower */
#define __SOFT_INPUT_THRESHOLD_MAX  (1LL << 29) /* 512 MB soft limit upper */
#define __HARD_INPUT_THRESHOLD      (1LL << 30) /*   1 GB hard limit */

/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __submit_to_parser_input( CByteList_t *parser_input, vgx_ByteArrayList_t *list, CString_t **CSTR__error ) {
  int64_t n_submitted = 0;
  BYTE newline = '\n';
  CByteList_vtable_t *iList = CALLABLE( parser_input );
  const vgx_ByteArray_t *src = list->entries;
  const vgx_ByteArray_t *end = src + list->sz;
  while( src < end ) {
    int64_t n = iList->Extend( parser_input, src->data, src->len );
    if( n < 0 ) {
      __format_error_string( CSTR__error, "internal buffer capacity exceeded (%lld bytes attempted)", src->len );
      return -1;
    }
    if( src->data[ src->len - 1 ] != '\n' ) {
      if( iList->Append( parser_input, &newline ) == 1 ) {
        ++n;
      }
    }
    n_submitted += n;
    ++src;
  }
  return n_submitted;
}



/*******************************************************************//**
 *
 * Return: >0 number of bytes consumed (OK)
 *          0 nothing could be consumed due to throttle (temporary error)
 *         -1 permanent error (data should be discarded)
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_parser__submit_data( vgx_OperationParser_t *parser, vgx_ByteArrayList_t *list, CString_t **CSTR__error ) {
  int64_t n_submitted = 0;
  int ms_sleep = 0;
  int64_t n0 = 0;
  int64_t n1 = 0;
  bool complete = false;
  comlib_task_t *task = parser->TASK;

  if( task == NULL || COMLIB_TASK__IsNotAlive( task ) || list->sz < 1 ) {
    if( task == NULL ) {
      __set_error_string( CSTR__error, "internal error" );
    }
    else if( list->sz < 1 ) {
      __set_error_string( CSTR__error, "empty data" );
    }
    else {
      __set_error_string( CSTR__error, "parser not ready" );
    }
    return -1;
  }

  while( !complete ) {
    
    COMLIB_TASK_LOCK( task ) {

      CByteList_t *input = parser->input;

      int64_t sz_input = ComlibSequenceLength( input );

      // Submit data without delay
      if( sz_input < __SOFT_INPUT_THRESHOLD_MIN ) {
        n_submitted = __submit_to_parser_input( parser->input, list, CSTR__error );
        complete = true;
      }
      // Short sleep then submit
      else if( sz_input < __SOFT_INPUT_THRESHOLD_MAX ) {
        COMLIB_TASK_SUSPEND_LOCK( task ) {
          sleep_milliseconds( 100 );
        } COMLIB_TASK_RESUME_LOCK;
        n_submitted = __submit_to_parser_input( parser->input, list, CSTR__error );
        complete = true;
      }
      // Delay submission due to size
      else {
        // In SOFT limit region, sleep and try again as long as parser is making progress
        if( sz_input < __HARD_INPUT_THRESHOLD ) {
          n1 = parser->n_pending_bytes;
          // Parser is making progress
          if( n1 != n0 ) {
            n0 = n1;
            ms_sleep = 50;
          }
          // Sleep longer until retry
          else if( ms_sleep < 1000 ) {
            ms_sleep += 100;
          }
          // Give up
          else {
            complete = true;
            __format_error_string( CSTR__error, "input throttle (input queue size: %lld)", sz_input );
          }
        }
        // HARD limit reached, fail immediately
        else {
          complete = true;
          __format_error_string( CSTR__error, "input hard limit reached (input queue size: %lld)", sz_input );
          // Nothing could be submitted because input buffer is too full. Try submitting again later.
          n_submitted = 0;
        }
      }
    } COMLIB_TASK_RELEASE;

    if( !complete ) {
      sleep_milliseconds( ms_sleep );
    }
  }
  return n_submitted;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_parser__enable_validation( vgx_OperationParser_t *parser, bool enable ) {
  COMLIB_TASK_LOCK( parser->TASK ) {
    if( enable ) {
      PARSER_ENABLE_VALIDATION_TCS( parser );
    }
    else {
      PARSER_DISABLE_VALIDATION_TCS( parser );
    }
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_parser__enable_execution( vgx_OperationParser_t *parser, bool enable ) {
  COMLIB_TASK_LOCK( parser->TASK ) {
    if( enable ) {
      PARSER_ENABLE_EXECUTION_TCS( parser );
    }
    else {
      PARSER_DISABLE_EXECUTION_TCS( parser );
    }
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_parser__silent_skip_regression( vgx_OperationParser_t *parser, bool silent_skip ) {
  COMLIB_TASK_LOCK( parser->TASK ) {
    if( silent_skip ) {
      PARSER_MUTE_REGRESSION_TCS( parser );
    }
    else {
      PARSER_CATCH_REGRESSION_TCS( parser );
    }
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_parser__enable_crc( vgx_OperationParser_t *parser, bool enable ) {
  COMLIB_TASK_LOCK( parser->TASK ) {
    if( enable ) {
      PARSER_ENABLE_CRC_TCS( parser );
    }
    else {
      PARSER_DISABLE_CRC_TCS( parser );
    }
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_parser__enable_strict_serial( vgx_OperationParser_t *parser, bool enable ) {
  COMLIB_TASK_LOCK( parser->TASK ) {
    if( enable ) {
      PARSER_ENABLE_STRICT_SERIAL_TCS( parser );
    }
    else {
      PARSER_DISABLE_STRICT_SERIAL_TCS( parser );
    }
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_parser__get_pending( vgx_OperationParser_t *parser ) {
  int64_t n_pending = 0;
  comlib_task_t *task = parser->TASK;

  if( task == NULL || COMLIB_TASK__IsNotAlive( task ) ) {
    return 0;
  }

  COMLIB_TASK_LOCK( task ) {
    // Pending bytes as last recorded by parser loop, including recently
    // submitted new parser input plus existing backlog within parser loop.
    // It is possible for total pending to be greater than this number if
    // new data was submitted a moment ago before the parser loop had
    // a chance to update its pending bytes sample value.
    if( (n_pending = parser->n_pending_bytes) == 0 ) {
      // Parser loop recently recorded no pending bytes.
      // We need to check the parser input. It is important
      // never to return zero from this function if anywhere in
      // the feed chain there is data that might reach the parser.
      n_pending = ComlibSequenceLength( parser->input );
    }
  } COMLIB_TASK_RELEASE;

  return n_pending;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__add_opcode_filter( int64_t opcode_filter ) {
  int count = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      if( _vxdurable_operation_parser__suspend_CS( SYSTEM, 10000 ) == 1 ) {
        count = __deny_execf_by_opcode_SYS_CS( opcode_filter );
        _vxdurable_operation_parser__resume_CS( SYSTEM );
      }
    } GRAPH_RELEASE;
  }
  return count;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__remove_opcode_filter( int64_t opcode_filter ) {
  int count = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      if( _vxdurable_operation_parser__suspend_CS( SYSTEM, 10000 ) == 1 ) {
        count = __allow_execf_by_opcode_SYS_CS( opcode_filter );
        _vxdurable_operation_parser__resume_CS( SYSTEM );
      }
    } GRAPH_RELEASE;
  }
  return count;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__apply_opcode_profile( int64_t profile_id ) {
  int count = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      if( _vxdurable_operation_parser__suspend_CS( SYSTEM, 10000 ) == 1 ) {
        OperationProcessorOpProfileID profile = (int)(profile_id >> 32);
        count = __apply_execf_profile_SYS_CS( profile );
        _vxdurable_operation_parser__resume_CS( SYSTEM );
      }
    } GRAPH_RELEASE;
  }
  return count;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN unsigned int _vxdurable_operation_parser__checksum( const char *data ) {
  vgx_OperationParser_t dummy_parser = {0};
  const char *cursor = data;
  unsigned int prev = 0;
  unsigned int crc = 0;
  int64_t n_bytes = 0;
  while( (cursor = __next_token( &dummy_parser, cursor, &n_bytes )) != NULL ) {
    prev = crc;
    crc = dummy_parser.crc;
  }
  return crc;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_parser_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_OperationParser_t *parser = COMLIB_TASK__GetData( self );
  vgx_Graph_t *graph = parser->parent;

  GRAPH_LOCK( graph ) {
    COMLIB_TASK_LOCK( self ) {
      XTRY {

        // Create internal buffers
        if( (parser->input = COMLIB_OBJECT_NEW_DEFAULT( CByteList_t )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
        }

        if( (parser->private_input = COMLIB_OBJECT_NEW_DEFAULT( CByteList_t )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
        }

        // Enable strict serial numbers by default
        iOperation.Parser.EnableStrictSerial( parser, true );

        // Enable CRC verification by default
        iOperation.Parser.EnableCRC( parser, true );

      }
      XCATCH( errcode ) {
        if( parser->input ) {
          COMLIB_OBJECT_DESTROY( parser->input );
          parser->input = NULL;
        }
        if( parser->private_input ) {
          COMLIB_OBJECT_DESTROY( parser->private_input );
          parser->private_input = NULL;
        }
      }
      XFINALLY {
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
static unsigned __operation_parser_shutdown( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_OperationParser_t *parser = COMLIB_TASK__GetData( self );
  vgx_Graph_t *graph = parser->parent;

  // =================================================
  // ======== OPERATION PARSER SHUTTING DOWN =========
  // =================================================

  if( PARSER_HAS_VERTEX_LOCKS( parser ) ) {
    __force_release_all_thread_vertices( parser );
  }

  GRAPH_LOCK( graph ) {
    COMLIB_TASK_LOCK( self ) {
      
      COMLIB_TASK__ClearState_Busy( self );

      int64_t n;

      // Delete public input
      if( (n = ComlibSequenceLength( parser->input )) > 0 ) {
        PARSER_CRITICAL( parser, 0x001, "Lost %lld bytes from external input", n );
      }
      COMLIB_OBJECT_DESTROY( parser->input );
      parser->input = NULL;

      // Delete private input
      if( (n = ComlibSequenceLength( parser->private_input )) > 0 ) {
        PARSER_CRITICAL( parser, 0x002, "Lost %lld bytes from internal input", n );
      }
      COMLIB_OBJECT_DESTROY( parser->private_input );
      parser->private_input = NULL;


    } COMLIB_TASK_RELEASE;

  } GRAPH_RELEASE;

  // =================================================
  // ========== FREE ANY HELD VERTEX LOCKS ===========
  // =================================================
  int64_t n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
  if( n > 0 ) {
    PARSER_DEC_VERTEX_LOCKS( parser, n );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __force_release_all_thread_vertices( vgx_OperationParser_t *parser ) {
  // Release any vertices held by current thread
  int64_t n = 0;
  GRAPH_FACTORY_ACQUIRE {
    const vgx_Graph_t **graphs = NULL;
    if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) {
      // System graph
      vgx_Graph_t *graph = iSystem.GetSystemGraph();
      if( graph ) {
        n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
        if( n > 0 ) {
          PARSER_DEC_VERTEX_LOCKS( parser, n );
        }
      }
      // User graphs
      const vgx_Graph_t **cursor = graphs;
      while( (graph = (vgx_Graph_t*)*cursor++) != NULL ) {
        n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
        if( n > 0 ) {
          PARSER_DEC_VERTEX_LOCKS( parser, n );
        }
      }
      free( (void*)graphs );
    }
  } GRAPH_FACTORY_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__reset( vgx_OperationParser_t *parser ) {
    
  __operation_parser_operation_reset( parser );
  idunset( &parser->transid );
  parser->sn = 0;
  parser->state = OPSTATE_EXPECT_TRANSACTION;

  __force_release_all_thread_vertices( parser );


  PARSER_RESET_VERTEX_LOCKS( parser );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_OperationParser_t,
                   parser,
                   __operation_parser_monitor,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "operation_parser_monitor/" )
{
  SET_CURRENT_THREAD_LABEL( "vgx_opparse" );
  
  vgx_Graph_t *graph = parser->parent;
  
  const char *graph_name = CStringValue( CALLABLE( graph )->Name( graph ) );
  
  APPEND_THREAD_NAME( graph_name );
  COMLIB_TASK__AppendDescription( self, graph_name );


  int64_t n_ops_total = 0;
  int64_t n_ops_last = 0;

  CByteList_vtable_t *iList = (CByteList_vtable_t*)COMLIB_CLASS_VTABLE( CByteList_t );

  // ------------------------

  const BYTE zero = 0;
  int64_t n_parsed_bytes = 0;

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

  CString_t *CSTR__error = NULL;

  bool suspend_request = false;
  bool suspended = false;

  int64_t tms = 0;
  int64_t tfeed_last_ms = 0;
  int64_t tfeed_complete_ms = 0;

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {

    loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

    COMLIB_TASK_LOCK( self ) {

      if( PARSER_HAS_RESET_TRIGGER_TCS( parser ) ) {
        PARSER_INFO( parser, 0x001, "Reset" );
        _vxdurable_operation_parser__reset( parser );
        PARSER_CLEAR_RESET_TRIGGER_TCS( parser );
      }

      // Idle
      COMLIB_TASK__ClearState_Busy( self );

      // Stop requested? Accept request and enter stopping state
      if( COMLIB_TASK__IsRequested_Stop( self ) ) {
        COMLIB_TASK__AcceptRequest_Stop( self );
        COMLIB_TASK__SetState_Stopping( self );
        suspended = false;
        suspend_request = false;
      }
      else {
        // Suspend requested?
        if( COMLIB_TASK__IsRequested_Suspend( self ) ) {
          COMLIB_TASK__AcceptRequest_Suspend( self );
          if( !suspended ) {
            COMLIB_TASK__SetState_Suspending( self );
            suspend_request = true;
          }
        }
        // Resume requested ?
        if( COMLIB_TASK__IsRequested_Resume( self ) ) {
          COMLIB_TASK__AcceptRequest_Resume( self );
          COMLIB_TASK__ClearState_Suspending( self );
          COMLIB_TASK__ClearState_Suspended( self );
          suspended = false;
          suspend_request = false;
        }
      }

      // Wait for input
      int64_t n_input = ComlibSequenceLength( parser->input ) + ComlibSequenceLength( parser->private_input );
      bool timeout = false;
      bool wait = n_input == 0 ? true : false;
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotStopping( self ) && COMLIB_TASK__IsNotSuspending( self ) && wait, 5000, &timeout ) {
        COMLIB_TASK_SUSPEND_MILLISECONDS( self, 10 );
        n_input = ComlibSequenceLength( parser->input ) + ComlibSequenceLength( parser->private_input );
        if( n_input > 0 || COMLIB_TASK__IsStateChangeRequested( self ) || PARSER_HAS_RESET_TRIGGER_TCS( parser ) ) {
          wait = false;
        }
      } END_TIME_LIMITED_WHILE;

      if( timeout ) {
        // Then what?
      }

      if( !suspended ) {

        // Do work if we have any input
        if( n_input > 0 ) {
          // Execution
          if( PARSER_IS_EXECUTION_ENABLED_TCS( parser ) ) {
            parser->control.exe = OPEXEC_NORMAL;
          }
          else {
            parser->control.exe = OPEXEC_NONE;
          }

          // Validation
          bool validate = PARSER_IS_VALIDATION_ENABLED_TCS( parser );

          // Regression Check
          if( PARSER_IS_STRICT_SERIAL_ENABLED_TCS( parser ) ) {
            if( PARSER_IS_REGRESSION_MUTED_TCS( parser ) ) {
              parser->control.snchk = OPSERIAL_CHECK_SILENT;
            }
            else {
              parser->control.snchk = OPSERIAL_CHECK_STRICT;
            }
          }

          // CRC Check
          parser->control.crc = PARSER_IS_CRC_ENABLED_TCS( parser );

          bool ro_disallowed = false;

          // Disallow readonly when execution is enabled
          if( PARSER_CONTROL_FEED( parser ) ) {
            COMLIB_TASK_SUSPEND_LOCK( self ) {
              tms = _vgx_graph_milliseconds( graph );
              GRAPH_LOCK( graph ) {
                // Proceed writable with readonly disallowed
                if( _vgx_is_writable_CS( &graph->readonly ) ) {
                  // Execution requires validation
                  validate = true;
                  _vgx_inc_disallow_readonly_CS( &graph->readonly );
                  ro_disallowed = true;
                }
                // Readonly graph, no work can be done yet
                else {
                  parser->control.exe = OPEXEC_NONE;
                  validate = false;
                }
              } GRAPH_RELEASE;
            } COMLIB_TASK_RESUME_LOCK;
          }

          // Perform work on input data
          if( PARSER_CONTROL_FEED( parser ) || validate ) {
            // Busy
            COMLIB_TASK__SetState_Busy( self );

            CByteList_t *public_input = parser->input;
            // Private input is empty, we do a quick swap
            if( ComlibSequenceLength( parser->private_input ) == 0 ) {
              parser->input = parser->private_input;
              parser->private_input = public_input;
              iList->Append( parser->private_input, &zero );
              parser->n_pending_bytes = ComlibSequenceLength( parser->private_input );
            }
            // Private input still being consumed, keep parser input as is, building up
            else {
              parser->n_pending_bytes = ComlibSequenceLength( parser->private_input ) + ComlibSequenceLength( parser->input );
            }

            // Private input ready for feed
            const char *data = (char*)iList->Cursor( parser->private_input, 0 );
            int64_t sz_data = ComlibSequenceLength( parser->private_input );

            // Reset byte counter
            n_parsed_bytes = 0;

            // ----------------------------------------------
            // Now do heavy lifting parsing with no lock
            // ----------------------------------------------
            COMLIB_TASK_SUSPEND_LOCK( self ) {
              const char *cursor = NULL;

              vgx_op_parser_error_t parser_error = {0};
              vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
              vgx_OperationCounters_t *counters_SYS_CS = SYSTEM ? &SYSTEM->OP.system.in_counters_CS : NULL;

              WAITABLE_TIMER *Timer = COMLIB_TASK_TIMER;

              n_ops_last = iOperation.Parser.Feed( parser, data, &cursor, Timer, counters_SYS_CS, &CSTR__error, &parser_error );

              // A really bad thing happened in the parser, internal error
              if( n_ops_last < 0 ) {
                const char *s_err = CSTR__error ? CStringValue( CSTR__error ) : "?";
                PARSER_CRITICAL( parser, 0x003, "INTERNAL PARSER ERROR: %s", s_err );
                iString.Discard( &CSTR__error );
                cursor = NULL; // Trigger full discard of data
              }
              else if( n_ops_last > 0 ) {
                n_ops_total += n_ops_last;
                tfeed_last_ms = tms;
              }

              // All input consumed
              if( cursor == NULL ) {
                n_parsed_bytes = sz_data;  
                iList->Clear( parser->private_input );
                tfeed_complete_ms = tms;
              }
              // Input bytes remain
              else {
                n_parsed_bytes = cursor - data;
                iList->Discard( parser->private_input, n_parsed_bytes );
                if( parser_error.backoff_ms > 0 ) {
                  loop_delay = COMLIB_TASK_LOOP_DELAY( parser_error.backoff_ms );
                }
              }

            } COMLIB_TASK_RESUME_LOCK;

            // Update pending bytes
            parser->n_pending_bytes = ComlibSequenceLength( parser->input ) + (sz_data - n_parsed_bytes);

          }
          else {
            iList->Discard( parser->input, -1 );
          }

          // Re-allow readonly
          if( ro_disallowed ) {
            COMLIB_TASK_SUSPEND_LOCK( self ) {
              GRAPH_LOCK( graph ) {
                _vgx_dec_disallow_readonly_CS( &graph->readonly );
              } GRAPH_RELEASE;
            } COMLIB_TASK_RESUME_LOCK;
          }
        }

        // Suspend if requested
        if( suspend_request ) {

          COMLIB_TASK_SUSPEND_LOCK( self ) {
            if( PARSER_HAS_VERTEX_LOCKS( parser ) ) {
              PARSER_WARNING( parser, 0x004, "%d vertex locks at time of suspend, forcing parser reset", PARSER_GET_VERTEX_LOCKS( parser ) );
              _vxdurable_operation_parser__reset( parser );
            }
          } COMLIB_TASK_RESUME_LOCK;

          COMLIB_TASK__ClearState_Busy( self );
          suspend_request = false;
          // Enter suspended state only if the suspending state is still in effect
          if( COMLIB_TASK__IsSuspending( self ) ) {
            COMLIB_TASK__SetState_Suspended( self );
            COMLIB_TASK__ClearState_Suspending( self );
            suspended = true;
          }
        }

      }

    } COMLIB_TASK_RELEASE;
  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__suspend_CS( vgx_Graph_t *self, int timeout_ms ) {
  int ret = 0;
  vgx_OperationParser_t *parser = &self->OP.parser;
  if( parser->TASK ) {
    GRAPH_SUSPEND_LOCK( self ) {
      COMLIB_TASK_LOCK( parser->TASK ) {
        if( COMLIB_TASK__IsAlive( parser->TASK ) ) {
          ret = COMLIB_TASK__Suspend( parser->TASK, NULL, timeout_ms );
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
DLL_HIDDEN int _vxdurable_operation_parser__suspend( vgx_Graph_t *self, int timeout_ms ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxdurable_operation_parser__suspend_CS( self, timeout_ms );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__is_suspended_CS( vgx_Graph_t *self ) {
  int suspended = 0;
  vgx_OperationParser_t *parser = &self->OP.parser;
  if( parser->TASK ) {
    GRAPH_SUSPEND_LOCK( self ) {
      COMLIB_TASK_LOCK( parser->TASK ) {
        if( COMLIB_TASK__IsAlive( parser->TASK ) ) {
          suspended = COMLIB_TASK__IsSuspended( parser->TASK );
        }
      } COMLIB_TASK_RELEASE;
    } GRAPH_RESUME_LOCK;
  }
  return suspended;
}
  


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__is_suspended( vgx_Graph_t *self ) {
  int suspended = 0;
  GRAPH_LOCK( self ) {
    suspended = _vxdurable_operation_parser__is_suspended_CS( self );
  } GRAPH_RELEASE;
  return suspended;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__resume_CS( vgx_Graph_t *self ) {
  int ret = 0;
  vgx_OperationParser_t *parser = &self->OP.parser;
  if( parser->TASK ) {
    GRAPH_SUSPEND_LOCK( self ) {
      COMLIB_TASK_LOCK( parser->TASK ) {
        if( COMLIB_TASK__IsAlive( parser->TASK ) ) {
          ret = COMLIB_TASK__Resume( parser->TASK, 10000 );
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
DLL_HIDDEN int _vxdurable_operation_parser__resume( vgx_Graph_t *self ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    ret = _vxdurable_operation_parser__resume_CS( self );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__initialize_CS( vgx_Graph_t *graph, vgx_OperationParser_t *parser, bool start_thread ) {
  int ret = 0;

  __init_execf_map();

  memset( parser, 0, sizeof( vgx_OperationParser_t ) );

  XTRY {

    // [1] operation parser state
    parser->state = OPSTATE_EXPECT_TRANSACTION;

    // [2] operation parser optype
    parser->optype = OPTYPE_NONE;

    // [3] Operation graph
    parser->op_graph = NULL;

    // [4] Operation vertex
    parser->op_vertex_WL = NULL;

    // [5] Operation ID
    parser->opid = 0;

    // [6] Operator parse
    parser->parsef = __parse_op_none;

    // [7] Operator execute
    parser->execf = __execute_op_none;

    // [8] Operator retire
    parser->retiref = __retire_op_none;

    // [9] Locked graph pointer
    parser->__locked_graph = NULL; 

    // [10] Operator buffer
    memset( parser->OPERATOR.qwords, 0, sizeof( op_BASE ) );

    // [11]
    parser->token.data = NULL;
    parser->token.len = 0;
    parser->token.flags.suspend = false;
    parser->token.flags.pending = false;

    // [12] Operator field number
    parser->field = 0;

    // [13] Operator mnemonic
    strcpy( parser->op_mnemonic, OP_NAME_NOP );

    // [14] Error string
    parser->CSTR__error = NULL;

    // [15] Error reason
    parser->reason = VGX_ACCESS_REASON_NONE;

    // [16] CRC
    parser->crc = 0;

    // [17] Parser task
    parser->TASK = NULL;

    // [18] Parser control reset
    parser->control._bits = 0;

    // [19] Parent graph
    parser->parent = graph;

    // [20] Public input
    parser->input = NULL;

    // [21] Private input
    parser->private_input = NULL;

    // [22] Bytes in input queues waiting to be processed
    parser->n_pending_bytes = 0;

    // [23] Property allocator reference (ref. graph's allocator)
    parser->property_allocator_ref = NULL;

    // [24] String Allocator Context
    if( (parser->string_allocator = icstringalloc.NewContext( iSystem.GetSystemGraph(), NULL, NULL, NULL, "operation parser string allocator" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // [25] Serial number
    parser->sn = 0;

    // [26] Transaction ID
    idunset( &parser->transid );

    if( start_thread ) {
      // ===============================
      // START PARSER THREAD
      // ===============================

      PARSER_VERBOSE( parser, 0, "Starting input monitor..." );

      // [17]
      if( (parser->TASK = COMLIB_TASK__New( __operation_parser_monitor, __operation_parser_initialize, __operation_parser_shutdown, parser )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
      }

      int started;
      GRAPH_SUSPEND_LOCK( graph ) {
        started = COMLIB_TASK__Start( parser->TASK, 10000 );
      } GRAPH_RESUME_LOCK;

      if( started == 1 ) {
        PARSER_VERBOSE( parser, 0, "Started" );
      }
      else {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
      }
    }

  }
  XCATCH( errcode ) {
    if( parser->TASK ) {
      COMLIB_TASK__Delete( &parser->TASK );
    }

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
DLL_HIDDEN int _vxdurable_operation_parser__initialize_OPEN( vgx_Graph_t *graph, vgx_OperationParser_t *parser, bool start_thread ) {
  int ret = 0;
  GRAPH_LOCK( graph ) {
    ret = _vxdurable_operation_parser__initialize_CS( graph, parser, start_thread );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation_parser__destroy_CS( vgx_OperationParser_t *parser ) {
  int ret = 0;

  // Start out with assumption that parser is stopped
  int stopped = 1;

  // Task exists, stop it
  if( parser->TASK ) {
    vgx_Graph_t *graph = parser->parent;
    comlib_task_t *task = parser->TASK;
    int timeout_ms = 30000;
    // Parser is not stopped, do work to stop it
    stopped = 0;
    if( graph ) {
      GRAPH_SUSPEND_LOCK( graph ) {
        COMLIB_TASK_LOCK( task ) {
          if( (stopped = COMLIB_TASK__Stop( task, NULL, timeout_ms )) < 0 ) {
            PARSER_WARNING( parser, 0x001, "Forcing exit" );
            stopped = COMLIB_TASK__ForceExit( task, 30000 );
          }
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
    if( stopped != 1 ) {
      PARSER_CRITICAL( parser, 0x002, "Unresponsive parser thread" );
      ret = -1;
    }
  }

  // Parser is stoppped, clean up
  if( stopped == 1 ) {

    // [17]
    COMLIB_TASK__Delete( &parser->TASK );

    // [24]
    icstringalloc.DeleteContext( &parser->string_allocator );

  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_parser__destroy_OPEN( vgx_OperationParser_t *parser ) {
  int ret = 0;
  vgx_Graph_t *graph = parser->parent;
  if( graph ) {
    GRAPH_LOCK( graph ) {
      ret = _vxdurable_operation_parser__destroy_CS( parser );
    } GRAPH_RELEASE;
  }
  else {
    ret = _vxdurable_operation_parser__destroy_CS( parser );
  }
  return ret;
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_parser.h"
  
test_descriptor_t _vgx_vxdurable_operation_parser_tests[] = {
  { "VGX Graph Durable Operation Parser Tests", __utest_vxdurable_operation_parser },
  {NULL}
};
#endif
