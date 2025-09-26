/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_consumer_service.c
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


static const char *         __full_path( vgx_TransactionalConsumerService_t *consumer_service );

static bool                 __txlog_enabled( const vgx_TransactionalConsumerService_t *consumer_service );
static const char *         __get_tx_filepath( const char *path, int64_t sn_0, char *buffer, const char **fname );
static int                  __rotate_current_tx_output( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error );
static vgx_Vertex_t *       __open_transaction_vertex( vgx_TransactionalConsumerService_t *consumer_service, objectid_t *txid, bool writable );
static int                  __discard_to_durability_point( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __open_tx_output( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error );
static int64_t              __index_tx( vgx_TransactionalConsumerService_t *consumer_service, int64_t offset, CString_t **CSTR__error );

static unsigned             __operation_consumer_service_initialize( comlib_task_t *self );
static unsigned             __operation_consumer_service_shutdown( comlib_task_t *self );
static void                 __check_request__stop_suspend_resume_TCS( vgx_TransactionalConsumerService_t *consumer_service, bool *suspend_request, bool *suspended );
static void                 __suspend_if_requested( vgx_TransactionalConsumerService_t *consumer_service, bool *suspend_request, bool *suspended );
static int                  __operation_consumer__prepare_poll( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __operation_consumer__try_accept( vgx_TransactionalConsumerService_t *consumer_service );

static int64_t              __write_response( vgx_TransactionalConsumerService_t *consumer_service, const char *response, int64_t sz );
static int64_t              __produce_response_IDLE( vgx_TransactionalConsumerService_t *consumer_service, int64_t tick_tms );
static int64_t              __produce_response_ACCEPTED( vgx_TransactionalConsumerService_t *consumer_service );
static int64_t              __produce_response_RETRY( vgx_TransactionalConsumerService_t *consumer_service, DWORD reason );
static int64_t              __produce_response_REJECTED( vgx_TransactionalConsumerService_t *consumer_service, DWORD reason );
static int64_t              __produce_response_SUSPEND( vgx_TransactionalConsumerService_t *consumer_service, int suspend_ms );
static int64_t              __produce_response_RESUME( vgx_TransactionalConsumerService_t *consumer_service );
static int64_t              __produce_response_ATTACH( vgx_TransactionalConsumerService_t *consumer_service, DWORD protocol, DWORD version );
static int64_t              __produce_response_DETACH( vgx_TransactionalConsumerService_t *consumer_service );

static vgx_OpKeyword        __get_request_KWD( const char *line );
static int                  __parse_request_IDLE( const char *line, int64_t *rtms, objectid_t *rfingerprint );
static int                  __parse_request_TRANSACTION( const char *line, vgx_OperationTransaction_t *rtx, int64_t *rmaster_sn );
static int                  __parse_request_COMMIT( const char *line, objectid_t *rtx_id, int64_t *rtms, DWORD *rcrc );
static int                  __parse_request_OP( const char *line, OperationProcessorOpType *roptype, objectid_t *ropgraph, objectid_t *robid );
static int                  __parse_request_ENDOP( const char *line, int64_t *ropid, int64_t *rtms, DWORD *rcrc );
static int                  __parse_request_RESYNC( const char *line, objectid_t *rid, int64_t *rnrollback );
static int                  __parse_request_ATTACH( const char *line, DWORD *rprotocol, DWORD *rversion, objectid_t *rfingerprint, WORD *radminport );
static int                  __parse_request_DETACH( const char *line );

static int64_t              __handle_request_IDLE( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_COMMIT( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int                  __handle_request_OP( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_ENDOP( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_RESYNC( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_TRANSACTION( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_ATTACH( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static int64_t              __handle_request_DETACH( vgx_TransactionalConsumerService_t *consumer_service, const char *line );
static void                 __reset_tx( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __open_tx_input( vgx_TransactionalConsumerService_t *consumer_service, int64_t sn0, int64_t offset );
static int                  __close_tx_input( vgx_TransactionalConsumerService_t *consumer_service );
static int64_t              __get_next_tx( vgx_TransactionalConsumerService_t *consumer_service, const objectid_t *this_txid, objectid_t *next_txid, vgx_OperationBuffer_t *dest );
static bool                 __is_rotation_due( const vgx_TransactionalConsumerService_t *consumer_service );
static int64_t              __write_tx_output( vgx_TransactionalConsumerService_t *consumer_service, vgx_ByteArrayList_t *txdata, int64_t *n_written, CString_t **CSTR__error );
static int64_t              __commit_tx( vgx_TransactionalConsumerService_t *consumer_service, int64_t orig_tms );
static int64_t              __append_tx( vgx_TransactionalConsumerService_t *consumer_service, BYTE *data, int64_t sz );
static void                 __random_noise_inject( vgx_TransactionalConsumerService_t *consumer_service, const char *line, int64_t sz_line );
static int64_t              __operation_consumer__handle_request( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __operation_consumer__client_close( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __operation_consumer__client_exception( vgx_TransactionalConsumerService_t *consumer_service, int err );
static int64_t              __operation_consumer__client_recv( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __operation_consumer__client_send( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __get_num_readonly_graphs( void );
static int                  __suspend_tx_execution_TCS( comlib_task_t *self, vgx_TransactionalConsumerService_t *consumer_service );
static int                  __resume_tx_execution_TCS( vgx_TransactionalConsumerService_t *consumer_service );
static int                  __backlog_query( vgx_TransactionalConsumerService_t *consumer_service, vgx_ExpressEvalMemory_t *memory, const char *filter, const char *rank, vgx_sortspec_t sortdir, objectid_t *tx );
static int                  __get_discardable_backlog( vgx_TransactionalConsumerService_t *consumer_service, int64_t durable_sn, objectid_t *tx_head, int64_t *n_tx_discardable, int64_t *sz_discardable );
static int                  __get_backlog_tail( vgx_TransactionalConsumerService_t *consumer_service, int64_t durable_sn, objectid_t *tx_tail, int64_t *n_tx_backlog, int64_t *sz_backlog );
static int64_t              __get_tx_backlog( vgx_TransactionalConsumerService_t *consumer_service, objectid_t *tx_start, int64_t *sn_start, int64_t *n_tx );
static int                  __execute_tx_backlog( vgx_TransactionalConsumerService_t *consumer_service, int64_t tx_limit_bytes );
static double               __refresh_tx_input_rate( vgx_TransactionalConsumerService_t *consumer_service );

DECLARE_COMLIB_TASK(        __operation_consumer_service_monitor );


static unsigned             __snapshot_persister_initialize( comlib_task_t *self );
static unsigned             __snapshot_persister_shutdown( comlib_task_t *self );
DECLARE_COMLIB_TASK(        __snapshot_persister );


// Number of transaction backlog bytes to process in one pass
#define TX_BACKLOG_PROCESS_CHUNK_SIZE (1ULL << 26)

// Maximum number of transaction entries (lines) based on max total size and 
// worst case compactness of just the operator mnemonic plus opcode, like:
//
// "    nop 1000001E"
//
// This is overly conservative since it would imply a single OP/ENDOP 
// containing ~8MiB of repeated nop operators.
// Real-world ratio is around 40-80 characters per line on average.
// We need this safety margin to make absolutely sure no transaction will
// ever contain more than TX_MAX_ENTRIES lines.
//
#define TX_AVG_LINE_SIZE_ESTIMATE 16
#define TX_MAX_ENTRIES (TX_MAX_SIZE / TX_AVG_LINE_SIZE_ESTIMATE)



#ifndef TRANSACTION_LOG_MAX_LIFESPAN_DAYS
#define TRANSACTION_LOG_MAX_LIFESPAN_DAYS         7
#endif
#define TRANSACTION_LOG_MAX_LIFESPAN_HOURS        (TRANSACTION_LOG_MAX_LIFESPAN_DAYS * 24)
#define TRANSACTION_LOG_MAX_LIFESPAN_MINUTES      (TRANSACTION_LOG_MAX_LIFESPAN_HOURS * 60)
#define TRANSACTION_LOG_MAX_LIFESPAN_SECONDS      (TRANSACTION_LOG_MAX_LIFESPAN_MINUTES * 60)

#ifndef TRANSACTION_LOG_ROTATE_MAX_MINUTES
#define TRANSACTION_LOG_ROTATE_MAX_MINUTES        15
#endif
#define TRANSACTION_LOG_ROTATE_MAX_SECONDS        (TRANSACTION_LOG_ROTATE_MAX_MINUTES * 60)
#define TRANSACTION_LOG_ROTATE_MAX_MILLISECONDS   (TRANSACTION_LOG_ROTATE_MAX_SECONDS * 1000)

#define TRANSACTION_LOG_ROTATE_MAX_BYTES          (1LL << 29)

#define DEFAULT_SNAPSHOT_THRESHOLD                (TRANSACTION_LOG_ROTATE_MAX_BYTES * 16)

#define SYS_STR_PROP_current_proxy    VGX_SYSTEM_PROPERTY_PREFIX "current.proxy"
#define SYS_TX_ROOT                   VGX_SYSTEM_VERTEX_PREFIX "TX_ROOT"
#define TX_EXT                        ".tx"

static objectid_t g_TX_ROOT_obid = {0};
#define SYS_TX_ROOT_obid (&g_TX_ROOT_obid)
#define SYS_INT_PROP_master_serial    "master.sn"

#define TX_TYPE_transaction           "type.tx"
#define TX_TYPE_proxy                 "type.proxy"
#define TX_TYPE_root                  "type.root"

#define TX_ARC_timestamp              "arc.ts"
#define TX_ARC_next                   "arc.next"
#define TX_ARC_contained_in_file      "arc.in"

#define TX_INT_PROP_crc               "tx.crc"
#define TX_INT_PROP_transaction_size  "tx.size"
#define TX_INT_PROP_serial_number     "tx.sn"
#define TX_INT_PROP_timestamp         "tx.tms"
#define TX_INT_PROP_file_serial0      "f.sn0"
#define TX_INT_PROP_file_offset       "f.ofs"

#define TX_STR_PROP_filename          "f.name"
#define TX_STR_PROP_next              "tx.next"
#define TX_STR_PROP_previous          "tx.prev"


#define IDLE_STATUS_PERIOD_MS         2000
#define RESUME_PERIOD_MS              11000


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_TransactionalConsumerService_t *consumer_service ) {
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  return SYSTEM ? CALLABLE( SYSTEM )->FullPath( SYSTEM ) : "?";
}

#define __CONSUMER_SERVICE_MESSAGE( LEVEL, ConsumerService, Code, Format, ... ) LEVEL( Code, "TX::INP(%s): " Format, __full_path( ConsumerService ), ##__VA_ARGS__ )

#define CONSUMER_SERVICE_VERBOSE( ConsumerService, Code, Format, ... )   __CONSUMER_SERVICE_MESSAGE( VERBOSE, ConsumerService, Code, Format, ##__VA_ARGS__ )
#define CONSUMER_SERVICE_INFO( ConsumerService, Code, Format, ... )      __CONSUMER_SERVICE_MESSAGE( INFO, ConsumerService, Code, Format, ##__VA_ARGS__ )
#define CONSUMER_SERVICE_WARNING( ConsumerService, Code, Format, ... )   __CONSUMER_SERVICE_MESSAGE( WARN, ConsumerService, Code, Format, ##__VA_ARGS__ )
#define CONSUMER_SERVICE_REASON( ConsumerService, Code, Format, ... )    __CONSUMER_SERVICE_MESSAGE( REASON, ConsumerService, Code, Format, ##__VA_ARGS__ )
#define CONSUMER_SERVICE_CRITICAL( ConsumerService, Code, Format, ... )  __CONSUMER_SERVICE_MESSAGE( CRITICAL, ConsumerService, Code, Format, ##__VA_ARGS__ )
#define CONSUMER_SERVICE_FATAL( ConsumerService, Code, Format, ... )     __CONSUMER_SERVICE_MESSAGE( FATAL, ConsumerService, Code, Format, ##__VA_ARGS__ )



#define CONSUMER_SERVICE_REQUEST_DISK_CLEANUP_TCS( ConsumerService )        ((ConsumerService)->control.flag_TCS.tx_disk_cleanup = 1)
#define CONSUMER_SERVICE_CLEAR_DISK_CLEANUP_REQUEST_TCS( ConsumerService )  ((ConsumerService)->control.flag_TCS.tx_disk_cleanup = 0)
#define CONSUMER_SERVICE_IS_DISK_CLEANUP_REQUESTED_TCS( ConsumerService )   ((ConsumerService)->control.flag_TCS.tx_disk_cleanup != 0)

#define CONSUMER_SERVICE_SET_FLUSH_PARSER_TCS( ConsumerService )        ((ConsumerService)->control.flag_TCS.tx_flush_parser = 1)
#define CONSUMER_SERVICE_CLEAR_FLUSH_PARSER_TCS( ConsumerService )      ((ConsumerService)->control.flag_TCS.tx_flush_parser = 0)
#define CONSUMER_SERVICE_IS_SET_FLUSH_PARSER_TCS( ConsumerService )     ((ConsumerService)->control.flag_TCS.tx_flush_parser != 0)

#define CONSUMER_SERVICE_REQUEST_SUSPEND_TX_EXECUTION_TCS( ConsumerService )        ((ConsumerService)->control.flag_TCS.tx_suspend_request = 1)
#define CONSUMER_SERVICE_CLEAR_TX_EXECUTION_SUSPEND_REQUEST_TCS( ConsumerService )  ((ConsumerService)->control.flag_TCS.tx_suspend_request = 0)
#define CONSUMER_SERVICE_IS_SUSPEND_TX_EXECUTION_REQUESTED_TCS( ConsumerService )   ((ConsumerService)->control.flag_TCS.tx_suspend_request != 0)

#define CONSUMER_SERVICE_REQUEST_RESUME_TX_EXECUTION_TCS( ConsumerService )         ((ConsumerService)->control.flag_TCS.tx_resume_request = 1)
#define CONSUMER_SERVICE_CLEAR_TX_EXECUTION_RESUME_REQUEST_TCS( ConsumerService )   ((ConsumerService)->control.flag_TCS.tx_resume_request = 0)
#define CONSUMER_SERVICE_IS_RESUME_TX_EXECUTION_REQUESTED_TCS( ConsumerService )    ((ConsumerService)->control.flag_TCS.tx_resume_request != 0)

#define CONSUMER_SERVICE_REQUEST_DETACH_TCS( ConsumerService )        ((ConsumerService)->control.flag_TCS.detach_request = 1)
#define CONSUMER_SERVICE_CLEAR_DETACH_REQUEST_TCS( ConsumerService )  ((ConsumerService)->control.flag_TCS.detach_request = 0)
#define CONSUMER_SERVICE_IS_DETACH_REQUESTED_TCS( ConsumerService )   ((ConsumerService)->control.flag_TCS.detach_request != 0)




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef OPSTREAM_DUMP_TX_IO

/**************************************************************************//**
 * __dump_tx_request_recv
 *
 ******************************************************************************
 */
static void __dump_tx_request_recv( const char *data, int64_t sz ) {
  static vgx_URI_t *out = NULL;
  if( out == NULL ) {
    const CString_t * CSTR__sysroot = igraphfactory.SystemRoot();
    CString_t *CSTR__txout = CStringNewFormat( "%s/request_recv" TX_EXT, CStringValue( CSTR__sysroot ) );
    CString_t *CSTR__error = NULL;
    out = iURI.NewElements( "file", NULL, NULL, 0, CStringValue( CSTR__txout ), NULL, NULL, &CSTR__error );
    if( CSTR__error ) {
      REASON( 0x000, "%s", CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    iString.Discard( &CSTR__txout );
    if( out == NULL ) {
      return;
    }
    if( iURI.OpenFile( out, &CSTR__error ) < 0 ) {
      iURI.Delete( &out );
      return;
    }
  }

  int fd = iURI.File.GetDescriptor( out );
  if( fd > 0 ) {
    CX_WRITE( data, 1, sz, fd );
  }
}
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef OPSTREAM_DUMP_TX_IO

/**************************************************************************//**
 * __dump_tx_response_sent
 *
 ******************************************************************************
 */
static void __dump_tx_response_sent( const char *data, int64_t sz ) {
  static vgx_URI_t *out = NULL;
  if( out == NULL ) {
    const CString_t * CSTR__sysroot = igraphfactory.SystemRoot();
    CString_t *CSTR__txout = CStringNewFormat( "%s/response_sent" TX_EXT, CStringValue( CSTR__sysroot ) );
    CString_t *CSTR__error = NULL;
    out = iURI.NewElements( "file", NULL, NULL, 0, CStringValue( CSTR__txout ), NULL, NULL, &CSTR__error );
    if( CSTR__error ) {
      REASON( 0x000, "%s", CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    iString.Discard( &CSTR__txout );
    if( out == NULL ) {
      return;
    }
    if( iURI.OpenFile( out, &CSTR__error ) < 0 ) {
      iURI.Delete( &out );
      return;
    }
  }

  int fd = iURI.File.GetDescriptor( out );
  if( fd > 0 ) {
    CX_WRITE( data, 1, sz, fd );
  }
}
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __txlog_enabled( const vgx_TransactionalConsumerService_t *consumer_service ) {
  return consumer_service->CSTR__tx_location != NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __get_tx_filepath( const char *path, int64_t sn_0, char *buffer, const char **fname ) {
  // 16ED86BD280E9C96.tx
  int n = snprintf( buffer, MAX_PATH, "%s/%016llX" TX_EXT, path, sn_0 );
  *fname = buffer + n - 19;
  return buffer;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __rotate_current_tx_output( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Vertex_t *TX_log = NULL;
  XTRY {
    if( consumer_service->tx_raw_storage == NULL || !__txlog_enabled( consumer_service ) ) {
      __set_error_string( CSTR__error, "No transaction storage" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Close output file and delete its URI object
    CONSUMER_SERVICE_INFO( consumer_service, 0x002, "Close TX output: %s", iURI.Path( consumer_service->tx_raw_storage ) );
    iURI.Close( consumer_service->tx_raw_storage );

    // Convert transaction log proxy to virtual node.
    // It will be deleted when the last inbound transaction arc is removed.
    // This will again trigger 
    if( consumer_service->CSTR__tx_fname ) {
      if( (TX_log = Simple->OpenVertex( SYSTEM, consumer_service->CSTR__tx_fname, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, 30000, &reason, CSTR__error )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
    }

    // Delete output file's URI object
    iURI.Delete( &consumer_service->tx_raw_storage );

    // Open new output
    if( __open_tx_output( consumer_service, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( TX_log ) {
      Simple->CloseVertex( SYSTEM, &TX_log );
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __open_transaction_vertex( vgx_TransactionalConsumerService_t *consumer_service, objectid_t *txid, bool writable ) {
  vgx_Vertex_t *TX = NULL;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( SYSTEM, 30000 );
  GRAPH_LOCK( SYSTEM ) {
    if( writable ) {
      CString_t *CSTR__error = NULL;
      if( (TX = _vxgraph_state__acquire_writable_vertex_CS( SYSTEM, NULL, txid, VERTEX_STATE_CONTEXT_MAN_NULL, &timing_budget, &CSTR__error )) == NULL ) {
        const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
        char idbuf[33];
        CONSUMER_SERVICE_REASON( consumer_service, 0x001, "Cannot open transaction '%s' %s: %s", idtostr( idbuf, txid ), writable ? "writable" : "readonly", serr );
      }
      iString.Discard( &CSTR__error );
    }
    else {
      TX = _vxgraph_state__acquire_readonly_vertex_CS( SYSTEM, txid, &timing_budget );
    }
  } GRAPH_RELEASE;
  return TX;
}



/*******************************************************************//**
 *
 *
 *
 *
 *
 ***********************************************************************
 */
static int __discard_to_durability_point( vgx_TransactionalConsumerService_t *consumer_service ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  char idbuf[33];
  int64_t tx_log_since_sn;
  uint32_t now_ts = _vgx_graph_seconds( SYSTEM );
  GRAPH_LOCK( SYSTEM ) {
    tx_log_since_sn = consumer_service->tx_log_since_sn_SYS_CS;
  } GRAPH_RELEASE;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( SYSTEM )->advanced;
  vgx_Vertex_t *TX = NULL;

  // Expire transactions that are no longer needed
  objectid_t durable_tx = {0};
  int64_t durable_sn = 0;
  int64_t durable_ts = 0;
  int n_serializing = 0;
  igraphfactory.DurabilityPoint( &durable_tx, &durable_sn, &durable_ts, &n_serializing );

  XTRY {
    // Trace backwards from durability point
    objectid_t cursor;
    idcpy( &cursor, &durable_tx );
    if( !idnone( &cursor ) && Advanced->HasVertex( SYSTEM, NULL, &cursor ) ) {

      // Open (readonly) the transaction node representing the durability point. This is the
      // earliest transaction that is guaranteed to have been persisted to snapshot
      // along with all transactions preceding it.
      if( (TX = __open_transaction_vertex( consumer_service, &cursor, false )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // We will now expire all transactions UP TO BUT NOT INCLUDING the durability point.
      // Even though the durability point is durable, we need to keep it around so we don't
      // risk losing continuity.
      objectid_t last_discardable_tx = {0};
      int64_t n_tx_discardable = 0;
      int64_t sz_discardable = 0;
      if( __get_discardable_backlog( consumer_service, durable_sn, &last_discardable_tx, &n_tx_discardable, &sz_discardable ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      const char *idstr = NULL;
      CString_t *CSTR__prev = NULL;
      objectid_t prev = {0};

      if( n_tx_discardable > 0 && CALLABLE( TX )->GetStringProperty( TX, TX_STR_PROP_previous, &CSTR__prev ) == 0 ) {

        uint32_t tmc = CALLABLE( TX )->CreationTime( TX ); 
        CONSUMER_SERVICE_INFO( consumer_service, 0x003, "Snapshot @ tx=%s sn=%lld age=%u", idtostr( idbuf, &durable_tx ), durable_sn, now_ts - tmc );

        // Verify that durability point transaction has a previous transaction
        // matching the query result from above.
        idstr = CStringValue( CSTR__prev );
        prev = strtoid( idstr );
        if( !idmatch( &prev, &last_discardable_tx ) ) {
          char idbuf2[33];
          CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "Transaction chain corruption. Expected last discardable tx=%s, got %s.", idtostr( idbuf, &last_discardable_tx ), idtostr( idbuf2, &prev ) );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }

        // Close durability point transaction
        Simple->CloseVertex( SYSTEM, &TX );

        // Set cursor to last discardable transaction
        idcpy( &cursor, &prev );

        // Open last discardable transaction (WRITABLE, so we can set expiration)
        if( (TX = __open_transaction_vertex( consumer_service, &cursor, true )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }

        // Spread background deletion of expired transactions over a constant period of time (60 seconds)
        double exp_ts_delta = 60.0 / n_tx_discardable; // expiration timestamp delta in sec/tx
        
        // We are setting expiration by traversing backwards.
        double exp_ts = now_ts + n_tx_discardable * exp_ts_delta;

        do {
          // Get previous transaction
          if( CALLABLE( TX )->GetStringProperty( TX, TX_STR_PROP_previous, &CSTR__prev ) < 0 ) {
            // We have reached the first transaction in the chain
            idunset( &cursor );
          }
          else {
            // Convert property string to object id and sanity check
            idstr = CStringValue( CSTR__prev );
            prev = strtoid( idstr );
            // Bad object id, or self-reference
            if( idnone( &prev ) || idmatch( &cursor, &prev ) ) {
              CONSUMER_SERVICE_REASON( consumer_service, 0x007, "Invalid transaction ID '%s' encountered during cleanup", idstr );
              idunset( &cursor );
            }
            else {
              // Verify that previous actually exists
              if( Advanced->HasVertex( SYSTEM, NULL, &prev ) ) {
                // Update cursor
                idcpy( &cursor, &prev );
              }
              // Previous no longer exists, terminate scan and remove prev reference
              else {
                CALLABLE( TX )->RemovePropertyKey( TX, TX_STR_PROP_previous );
                idunset( &cursor );
              }
            }
            // Discard property value
            iEnumerator_OPEN.Property.Value.Discard( SYSTEM, CSTR__prev );
          }
          // Decrement total tx log size
          int64_t tx_sz = 0;
          int64_t tx_sn = 0;
          CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_transaction_size, &tx_sz );
          CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_serial_number, &tx_sn );
          GRAPH_LOCK( SYSTEM ) {
            if( tx_sn > tx_log_since_sn ) {
              consumer_service->tx_log_total_sz_SYS_CS -= tx_sz;
            }
          } GRAPH_RELEASE;
          // Expire transaction
          uint32_t expire_at = (uint32_t)exp_ts;
          CALLABLE( TX )->SetExpirationTime( TX, expire_at );
          // Close transaction node
          Simple->CloseVertex( SYSTEM, &TX );

          // Update expiration time for next transaction
          exp_ts -= exp_ts_delta;

          // Reached the beginning, no more transactions
          if( idnone( &cursor ) ) {
            TX = NULL;
          }
          // Open transaction node immediately preceding the one we just processed
          else {
            TX = __open_transaction_vertex( consumer_service, &cursor, true );
          }

        } while( TX );
      }

    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( TX ) {
      Simple->CloseVertex( SYSTEM, &TX );
    }
  }

  GRAPH_LOCK( SYSTEM ) {
    if( consumer_service->tx_log_total_sz_SYS_CS < 0 ) {
      CONSUMER_SERVICE_WARNING( consumer_service, 0x008, "txlog total size negative? %lld (reset to 0)", consumer_service->tx_log_total_sz_SYS_CS );
      consumer_service->tx_log_total_sz_SYS_CS = 0;
    }

    consumer_service->tx_log_since_sn_SYS_CS = durable_sn;
  } GRAPH_RELEASE;


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __open_tx_output( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error ) {
  if( !__txlog_enabled( consumer_service ) ) {
    return -1;
  }

  int ret = 0;
  char buffer[ MAX_PATH + 1 ] = {0};
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  
  CString_t *CSTR__prev_tx_fname = NULL;
  
  const char *tx_next_filepath = NULL;

  const char *tx_next_fname = NULL;

  vgx_Vertex_t *TX_log = NULL;

  XTRY {
    // Name of existing transaction log if any
    CSTR__prev_tx_fname = consumer_service->CSTR__tx_fname;
    consumer_service->CSTR__tx_fname = NULL;

    // Check transaction log directory
    const char *tx_location = CStringValue( consumer_service->CSTR__tx_location );
    if( !dir_exists( tx_location ) ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x001, "Transaction storage location no longer exists: %s", tx_location );
      if( create_dirs( tx_location ) < 0 ) {
        __format_error_string( CSTR__error, "Failed to recreate transaction storage location: %s", tx_location );
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x002 );
      }
    }

    // Serial number of first transaction to go into the new transaction log
    consumer_service->tx_out_sn0 = consumer_service->transaction.serial;
    // Transaction timestamp for the first transaction
    consumer_service->tx_out_t0 = consumer_service->transaction.tms;
    // Keep track of output file size
    consumer_service->tx_out_sz = 0;

    // Transaction log filename
    tx_next_filepath = __get_tx_filepath( tx_location, consumer_service->tx_out_sn0, buffer, &tx_next_fname );
    if( (consumer_service->CSTR__tx_fname = iString.New( SYSTEM->ephemeral_string_allocator_context, tx_next_fname )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Create URI representing transaction log file
    if( (consumer_service->tx_raw_storage = iURI.NewElements( "file", NULL, NULL, 0, tx_next_filepath, NULL, NULL, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Open transaction log file
    if( iURI.OpenFile( consumer_service->tx_raw_storage, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    CONSUMER_SERVICE_INFO( consumer_service, 0x006, "Open TX output: %s", iURI.Path( consumer_service->tx_raw_storage ) );

    // Create proxy vertex representing transaction log
    if( (TX_log = Simple->NewVertex( SYSTEM, tx_next_fname, TX_TYPE_proxy, TRANSACTION_LOG_MAX_LIFESPAN_SECONDS, 60000, &reason, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
    }

    // Set the ondelete event, which will remove the transaction log from disk once the proxy vertex expires
    CALLABLE( TX_log )->SetOnDelete( TX_log, VertexOnDeleteAction_RemoveFile, iURI.Path( consumer_service->tx_raw_storage ) );       

    // Store the current proxy name in the system graph
    if( iSystem.Property.SetString( SYS_STR_PROP_current_proxy, tx_next_fname ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
    }

    // Connect previous transaction log proxy to the next log proxy
    if( CSTR__prev_tx_fname ) {
      // Convert to virtual (or delete if zero inbound transactions)
      if( Simple->DeleteVertex( SYSTEM, CSTR__prev_tx_fname, 30000, &reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
      }
    }

  }
  XCATCH( errcode ) {
    iURI.Delete( &consumer_service->tx_raw_storage );
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__prev_tx_fname );
    if( TX_log ) {
      Simple->CloseVertex( SYSTEM, &TX_log );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __index_tx( vgx_TransactionalConsumerService_t *consumer_service, int64_t offset, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( SYSTEM )->advanced;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  char txid[33];
  vgx_Vertex_t *TX_this = NULL;
  vgx_VertexIdentifiers_t *pair = NULL;
  vgx_VertexList_t *acquired_pair = NULL;

  XTRY {
    vgx_Vertex_t *A, *B;
    objectid_t *prev = &consumer_service->last_txid_commit;
    vgx_OperationTransaction_t *tx = &consumer_service->transaction;
    int64_t tx_n = tx->serial - consumer_service->tx_out_sn0;
    idtostr( txid, &tx->id );

    // Record the initial transaction id
    if( idnone( &consumer_service->initial_txid ) ) {
      idcpy( &consumer_service->initial_txid, &tx->id );
    }

    // Transaction Node
    // V.id           : transactionID
    // V.type         : "type.tx"
    // V['tx.sn']     : INT56 serial number
    // V['tx.tms']    : INT56 timestamp
    // V['tx.crc']    : INT56 crc
    // V['tx.size']   : INT56 transaction size in bytes
    // V['f.name']    : STR filename (base)
    // V['f.sn0']     : INT56 serial number of first transaction in file
    // V['f.ofs']     : INT56 file offset
    // V['tx.next']   : transactionID of the next transaction (if any)
    // V['tx.prev']   : transactionID of the previous transaction (if any)
    //

    // this, TX_ROOT
    if( (pair = iVertex.Identifiers.NewObidPair( SYSTEM, &tx->id, SYS_TX_ROOT_obid )) == NULL ) {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Create transaction node (ThisTX)
    if( Simple->CreateVertexLifespan( SYSTEM, txid, TX_TYPE_transaction, TRANSACTION_LOG_MAX_LIFESPAN_SECONDS, 60000, &reason, CSTR__error ) < 0 ) {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Acquire (ThisTX, TX_ROOT)
    if( (acquired_pair = Advanced->AtomicAcquireVerticesWritable( SYSTEM, pair, 30000, &reason, CSTR__error )) == NULL ) {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x003 );
    }

    // (ThisTX) -[arc.ts]-> (TX_ROOT)
    A = iVertex.List.Get( acquired_pair, 0 );
    B = iVertex.List.Get( acquired_pair, 1 );
    int tx_ts = (int)(tx->tms / 1000);
    if( Advanced->Connect_M_INT_WL( SYSTEM, A, B, TX_ARC_timestamp, tx_ts, &reason, CSTR__error ) < 0 ) {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Release (ThisTX, TX_ROOT)
    Advanced->AtomicReleaseVertices( SYSTEM, &acquired_pair );
    iVertex.Identifiers.Delete( &pair );

    // Acquire (ThisTX)
    if( (TX_this = Advanced->AcquireVertexWritableNocreate( SYSTEM, &tx->id, 30000, &reason, CSTR__error )) == NULL ) {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Special case: We previously output partial transaction data.
    //               We need to update vertex
    if( idmatch( prev, &tx->id ) ) {
      // V['tx.crc']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_crc, tx->crc );
      // V['tx.size']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_transaction_size, tx->tsize );
    }
    // Normal case: Set properties
    else {
      // V['tx.sn']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_serial_number, tx->serial );
      // V['tx.tms']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_timestamp, tx->tms );
      // V['tx.size']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_crc, tx->crc );
      // V['tx.size']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_transaction_size, tx->tsize );

      const char *tx_fname = CStringValue( consumer_service->CSTR__tx_fname );
      // V['f.name']
      CALLABLE( TX_this )->SetStringProperty( TX_this, TX_STR_PROP_filename, tx_fname );
      // V['f.sn0']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_file_serial0, consumer_service->tx_out_sn0 );
      // V['f.name']
      CALLABLE( TX_this )->SetIntProperty( TX_this, TX_INT_PROP_file_offset, offset );
    }

    Simple->CloseVertex( SYSTEM, &TX_this );

    // (PrevTX) -[arc.next]-> (ThisTX)
    //
    if( !idnone( prev ) && !idmatch( prev, &tx->id ) && Advanced->HasVertex( SYSTEM, NULL, prev ) ) {
      // prev, this
      if( (pair = iVertex.Identifiers.NewObidPair( SYSTEM, prev, &tx->id )) == NULL ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x006 );
      }
      // Acquire (PrevTX, ThisTX)
      if( (acquired_pair = Advanced->AtomicAcquireVerticesWritable( SYSTEM, pair, 30000, &reason, CSTR__error )) == NULL ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x007 );
      }
      // Connect previous to current transaction
      // (PrevTX) -[arc.next]-> (ThisTX)
      A = iVertex.List.Get( acquired_pair, 0 );
      B = iVertex.List.Get( acquired_pair, 1 );
      if( Advanced->Connect_M_INT_WL( SYSTEM, A, B, TX_ARC_next, 0, &reason, CSTR__error ) < 0 ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x008 );
      }

      // PrevTX["tx.next"] = ThisTX.id
      CALLABLE( A )->SetStringProperty( A, TX_STR_PROP_next, idtostr( txid, &tx->id ) );
      // ThisTX["tx.prev"] = PrevTX.id
      CALLABLE( B )->SetStringProperty( B, TX_STR_PROP_previous, idtostr( txid, prev ) );

      Advanced->AtomicReleaseVertices( SYSTEM, &acquired_pair );
      iVertex.Identifiers.Delete( &pair );
    }

    // Hook up transaction to its log proxy
    // (ThisTX) -[arc.in]-> (LogProxy)
    if( consumer_service->CSTR__tx_fname ) {
      // this, logproxy
      if( (pair = iVertex.Identifiers.NewObidPair( SYSTEM, &tx->id, CStringObid( consumer_service->CSTR__tx_fname ) )) == NULL ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x009 );
      }
      // Acquire (ThisTX, LogProxy)
      if( (acquired_pair = Advanced->AtomicAcquireVerticesWritable( SYSTEM, pair, 30000, &reason, CSTR__error )) == NULL ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x00A );
      }
      // Connect current transaction to log node
      // (ThisTX) -[arc.in]-> (LogProxy)
      A = iVertex.List.Get( acquired_pair, 0 );
      B = iVertex.List.Get( acquired_pair, 1 );
      if( Advanced->Connect_M_INT_WL( SYSTEM, A, B, TX_ARC_contained_in_file, (int)tx_n, &reason, CSTR__error ) < 0 ) {
        THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x00B );
      }

      Advanced->AtomicReleaseVertices( SYSTEM, &acquired_pair );
      iVertex.Identifiers.Delete( &pair );
    }

    // Update last transaction ID to be the current ID
    idcpy( prev, &tx->id );

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( TX_this ) {
      Simple->CloseVertex( SYSTEM, &TX_this );
    }
    if( acquired_pair ) {
      Advanced->AtomicReleaseVertices( SYSTEM, &acquired_pair );
    }
    if( pair ) {
      iVertex.Identifiers.Delete( &pair );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_consumer_service_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_TransactionalConsumerService_t *consumer_service = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;

  CString_t *CSTR__error = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Vertex_t *TX_ROOT = NULL;

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {
      XTRY {

        // [Q1.4] URI
        if( iTransactional.Consumer.Open( consumer_service, consumer_service->bind_port, &CSTR__error ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }

        // Mark service as initializing
        consumer_service->tx_state.public_TCS.initializing = true;

        // Load the last known transaction log proxy name (if any)
        CString_t *CSTR__tx_fname = NULL;
        iSystem.Property.GetString( SYS_STR_PROP_current_proxy, &CSTR__tx_fname );
        if( CSTR__tx_fname ) {
          if( CALLABLE( SYSTEM )->simple->HasVertex( SYSTEM, CSTR__tx_fname ) ) {
            if( (consumer_service->CSTR__tx_fname = CALLABLE( CSTR__tx_fname )->CloneAlloc( CSTR__tx_fname, SYSTEM->ephemeral_string_allocator_context )) == NULL ) {
              CONSUMER_SERVICE_REASON( consumer_service, 0x002, "Failed to restore transaction proxy reference for %s", CStringValue( CSTR__tx_fname ) );
            }
          }
          else {
            CONSUMER_SERVICE_WARNING( consumer_service, 0x003, "Proxy vertex for transaction log %s does not exist", CStringValue( CSTR__tx_fname ) );
          }
          iEnumerator_CS.Property.Value.Discard( SYSTEM, CSTR__tx_fname );
        }

        // Create transaction root
        if( (TX_ROOT = CALLABLE( SYSTEM )->simple->NewVertex( SYSTEM, SYS_TX_ROOT, TX_TYPE_root, -1, 10000, &reason, &CSTR__error )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }

        // Get the latest known master serial
        if( CALLABLE( TX_ROOT )->GetIntProperty( TX_ROOT, SYS_INT_PROP_master_serial, &consumer_service->provider.master_serial ) == 0 ) {
          consumer_service->sysgraph->sysmaster_tx_serial = consumer_service->provider.master_serial;
        }

        g_TX_ROOT_obid = CALLABLE( TX_ROOT )->InternalID( TX_ROOT );

      }
      XCATCH( errcode ) {
        if( CSTR__error ) {
          CONSUMER_SERVICE_REASON( consumer_service, 0x005, "Failed to open consumer service: %s", CStringValue( CSTR__error ) );
        }
        ret = 1;
      }
      XFINALLY {
        iString.Discard( &CSTR__error );
        if( TX_ROOT ) {
          CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX_ROOT );
        }
      }
    } COMLIB_TASK_RELEASE;

    if( ret == 0 && consumer_service->Listen ) {
      consumer_service->CSTR__subscriber_uri_SYS_CS = iString.New( NULL, iURI.URI( consumer_service->Listen ) );
    }

  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __operation_consumer_service_shutdown( comlib_task_t *self ) {
  unsigned ret = 0;
  vgx_TransactionalConsumerService_t *consumer_service = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_Vertex_t *TX_ROOT = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;

  // ===============================================================
  // ======== TRANSACTIONAL CONSUMER SERVICE SHUTTING DOWN =========
  // ===============================================================
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      if( consumer_service->Listen ) {
        iString.Discard( &consumer_service->CSTR__subscriber_uri_SYS_CS );
      }

      COMLIB_TASK_LOCK( self ) {
        
        COMLIB_TASK__ClearState_Busy( self );

        if( consumer_service->tx_raw_storage ) {
          const char *filepath = iURI.Path( consumer_service->tx_raw_storage );
          vgx_Vertex_t *TX_log = CALLABLE( SYSTEM )->simple->OpenVertex( SYSTEM, consumer_service->CSTR__tx_fname, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, 2000, &reason, &CSTR__error );
          if( TX_log ) {
            // Set the ondelete event, which will remove the transaction log from disk next time a new file is opened
            CALLABLE( TX_log )->SetOnDelete( TX_log, VertexOnDeleteAction_RemoveFile, filepath );
            CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX_log );
          }
          else {
            CONSUMER_SERVICE_WARNING( consumer_service, 0x000, "Manual cleanup of file %s required", filepath );
          }

          iURI.Close( consumer_service->tx_raw_storage );
        }

        // [Q1.4] URI
        iTransactional.Consumer.Close( consumer_service );

        // Persist master serial to transaction root
        if( (TX_ROOT = CALLABLE( SYSTEM )->advanced->AcquireVertexWritableNocreate_CS( SYSTEM, SYS_TX_ROOT_obid, 5000, &reason, &CSTR__error )) == NULL ) {
          CONSUMER_SERVICE_REASON( consumer_service, 0x000, "Failed to open transaction root on shutdown" );
        }
        else if( CALLABLE( TX_ROOT )->SetIntProperty( TX_ROOT, SYS_INT_PROP_master_serial, consumer_service->provider.master_serial ) < 0 ) {
          CONSUMER_SERVICE_REASON( consumer_service, 0x000, "Failed to persist master serial on shutdown" );
        }

        if( TX_ROOT ) {
          CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX_ROOT );
        }

      } COMLIB_TASK_RELEASE;

      SYSTEM->OP.system.running_tx_input_rate_CS = 0.0;

    } GRAPH_RELEASE;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __check_request__stop_suspend_resume_TCS( vgx_TransactionalConsumerService_t *consumer_service, bool *suspend_request, bool *suspended ) {
  comlib_task_t *task = consumer_service->TASK;
  if( task ) {
    // Stop requested? Accept request and enter stopping state
    if( COMLIB_TASK__IsRequested_Stop( task ) ) {
      COMLIB_TASK__AcceptRequest_Stop( task );
      COMLIB_TASK__SetState_Stopping( task );
      *suspended = false;
      *suspend_request = false;
      COMLIB_TASK_SUSPEND_LOCK( task ) {
        vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
        if( SYSTEM ) {
          GRAPH_LOCK( SYSTEM ) {
            consumer_service->snapshot.allowed_SYS_CS = false;
          } GRAPH_RELEASE;
        }
      } COMLIB_TASK_RESUME_LOCK;
    }
    else {
      // Suspend requested?
      if( COMLIB_TASK__IsRequested_Suspend( task ) ) {
        COMLIB_TASK__AcceptRequest_Suspend( task );
        if( !*suspended ) {
          COMLIB_TASK__SetState_Suspending( task );
          *suspend_request = true;
        }
      }
      // Resume requested ?
      if( COMLIB_TASK__IsRequested_Resume( task ) ) {
        COMLIB_TASK__AcceptRequest_Resume( task );
        COMLIB_TASK__ClearState_Suspending( task );
        COMLIB_TASK__ClearState_Suspended( task );
        *suspended = false;
        *suspend_request = false;
        CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Resumed" );
      }
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __suspend_if_requested( vgx_TransactionalConsumerService_t *consumer_service, bool *suspend_request, bool *suspended ) {
  comlib_task_t *task = consumer_service->TASK;
  // Suspend if requested
  if( *suspend_request && task ) {
    COMLIB_TASK__ClearState_Busy( task );
    *suspend_request = false;
    // Enter suspended state only if the suspending state is still in effect
    if( COMLIB_TASK__IsSuspending( task ) ) {
      COMLIB_TASK__SetState_Suspended( task );
      COMLIB_TASK__ClearState_Suspending( task );
      *suspended = true;
      CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Suspended" );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __operation_consumer__prepare_poll( vgx_TransactionalConsumerService_t *consumer_service ) {
  int nfds = 0;

  CXSOCKET *psock = iURI.Sock.Input.Get( consumer_service->Listen );
  if( psock == NULL ) {
    return -1;
  }

  struct pollfd *plisten = consumer_service->plisten;
  struct pollfd *pclient = consumer_service->pclient;

  // Monitor readable event on server listen socket
  cxpollfd_pollinit( plisten, psock, true, false );
  ++nfds;

  psock = iURI.Sock.Input.Get( consumer_service->TransactionProducerClient );
  // Client connected
  if( psock ) {
    // Monitor events on producer client socket
    bool pollwrite = iOpBuffer.Readable( consumer_service->buffer.response ) > 0;
    cxpollfd_pollinit( pclient, psock, true, pollwrite );
    ++nfds;
  }
  // No client connected
  else {
    cxpollfd_invalidate( pclient );
  }

  return nfds;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __operation_consumer__try_accept( vgx_TransactionalConsumerService_t *consumer_service ) {
  int ret;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  vgx_URI_t *Listen = consumer_service->Listen;

  // ACCEPT AND LOG
  CString_t *CSTR__error = NULL;
  vgx_URI_t *ProducerClient = iURI.Accept( Listen, "vgx", &CSTR__error );

  if( ProducerClient ) {
    const char *uri = iURI.URI( ProducerClient );

    if( consumer_service->TransactionProducerClient ) {
      CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "Existing Subscription Provider will be disconnected: %s", iURI.URI( consumer_service->TransactionProducerClient ) );
      __operation_consumer__client_close( consumer_service );
    }
    CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Provider accepted: %s", uri );

    consumer_service->TransactionProducerClient = ProducerClient;

    GRAPH_LOCK( SYSTEM ) {
      consumer_service->CSTR__provider_uri_SYS_CS = iString.New( NULL, uri );
    } GRAPH_RELEASE;

    // Require ATTACH handshake
    consumer_service->control.attached = false;

    // Clear resync flag (if set)
    consumer_service->resync_pending = false;

    // New client
    ret = 1;
  }
  else {
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
    CONSUMER_SERVICE_REASON( consumer_service, 0x003, "Socket accept error: %s", serr );
    ret = -1;
  }

  iString.Discard( &CSTR__error );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __write_response( vgx_TransactionalConsumerService_t *consumer_service, const char *response, int64_t sz ) {
  // Write response to buffer
  int64_t n = iOpBuffer.Write( consumer_service->buffer.response, response, sz );
  if( n < 0 ) {
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0x001, "Failed to write response - out of memory" );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_IDLE( vgx_TransactionalConsumerService_t *consumer_service, int64_t tick_tms ) {
  // IDLE <tms> <fingerprint> <master_serial>\n
  //
#define __IDLE_RESPONSE KWD_IDLE " 0000000000000000 00000000000000000000000000000000 0000000000000000 # vgx\n"
  static __THREAD char IDLE_response[] = __IDLE_RESPONSE;
  static const int offset_tms = sizeof( KWD_IDLE );             // points to first hex digit in timestamp (sizeof includes nul which account for the space)
  static const int offset_fp = sizeof( KWD_IDLE ) + 17;         // points to first hex digit in fingerprint
  static const int offset_master_sn = sizeof( KWD_IDLE ) + 50;  // points to first hex digit in master_serial
  static const int64_t sz_nonull = sizeof( IDLE_response ) - 1;

  int64_t local_tms = __MILLISECONDS_SINCE_1970();
  objectid_t local_fingerprint = igraphfactory.Fingerprint();
  int64_t provider_master_serial = consumer_service->provider.master_serial;

  // Fill in response fields
  write_HEX_qword( IDLE_response + offset_tms, local_tms );
  write_hex_objectid( IDLE_response + offset_fp, &local_fingerprint );
  write_HEX_qword( IDLE_response + offset_master_sn, provider_master_serial );

  // Write
  if( __write_response( consumer_service, IDLE_response, sz_nonull ) < 0 ) {
    return -1;
  }

  // Schedule next
  consumer_service->status_response_deadline_tms = tick_tms + IDLE_STATUS_PERIOD_MS;

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_ACCEPTED( vgx_TransactionalConsumerService_t *consumer_service ) {
#define __ACCEPTED_RESPONSE KWD_ACCEPTED " 00000000000000000000000000000000 00000000 # sz=               \n"
  static __THREAD char ACCEPTED_response[] = __ACCEPTED_RESPONSE;
  static const int offset_tx = sizeof( KWD_ACCEPTED ); // points to first tx hex digit (sizeof includes nul which account for the space)
  static const int offset_crc = sizeof( KWD_ACCEPTED ) + 33;
  static const int offset_sz = sizeof( KWD_ACCEPTED ) + 47;
  static const int64_t sz_nonull = sizeof( ACCEPTED_response ) - 1;

  vgx_OperationTransaction_t *tx = &consumer_service->transaction;

  // Fill in response fields
  write_hex_objectid( ACCEPTED_response + offset_tx, &tx->id );
  write_HEX_dword( ACCEPTED_response + offset_crc, tx->crc );
  write_decimal( ACCEPTED_response + offset_sz, tx->tsize );

  // Write
  if( __write_response( consumer_service, ACCEPTED_response, sz_nonull ) < 0 ) {
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_RETRY( vgx_TransactionalConsumerService_t *consumer_service, DWORD reason ) {
#define __RETRY_RESPONSE KWD_RETRY " 00000000000000000000000000000000 00000000 # vgx\n"
  static __THREAD char RETRY_response[] = __RETRY_RESPONSE;
  static const int offset_tx = sizeof( KWD_RETRY ); // points to first hex digit in transaction id (sizeof includes nul which account for the space)
  static const int offset_reason = sizeof( KWD_RETRY ) + 33;
  static const int64_t sz_nonull = sizeof( RETRY_response ) - 1;

  // Fill in response fields
  write_hex_objectid( RETRY_response + offset_tx, &consumer_service->transaction.id );
  write_HEX_dword( RETRY_response + offset_reason, reason );

  // Write
  if( __write_response( consumer_service, RETRY_response, sz_nonull ) < 0 ) {
    return -1;
  }

  // Mark the expected resync transaction and enter resync mode
  idcpy( &consumer_service->resync_tx, &consumer_service->transaction.id );
  consumer_service->resync_pending = 1;

  // Reset
  __reset_tx( consumer_service );

  objectid_t *tx = &consumer_service->resync_tx;

  vgx_OpSuspendCode code = __suspend_code_from_reason( reason );
  switch( code ) {
  case OP_SUSPEND_CODE_AUTORESUME_TIMEOUT:
    CONSUMER_SERVICE_WARNING( consumer_service, 0x001, "[Send RETRY] tx=%016llx%016llx", tx->H, tx->L );
    break;
  case OP_SUSPEND_CODE_INDEFINITE:
    CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "[Send RETRY] tx=%016llx%016llx (indefinite SUSPEND until RESUME)", tx->H, tx->L );
    break;
  default:
    CONSUMER_SERVICE_WARNING( consumer_service, 0x003, "[Send RETRY] tx=%016llx%016llx (unknown reason)", tx->H, tx->L );
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_REJECTED( vgx_TransactionalConsumerService_t *consumer_service, DWORD reason ) {
#define __REJECTED_RESPONSE KWD_REJECTED " 00000000000000000000000000000000 00000000 # vgx\n"
  static __THREAD char REJECTED_response[] = __REJECTED_RESPONSE;
  static const int offset_tx = sizeof( KWD_REJECTED ); // points to first hex digit in transaction id (sizeof includes nul which account for the space)
  static const int offset_reason = sizeof( KWD_REJECTED ) + 33;
  static const int64_t sz_nonull = sizeof( REJECTED_response ) - 1;

  vgx_OperationTransaction_t *tx = &consumer_service->transaction;

  // Fill in response fields
  write_hex_objectid( REJECTED_response + offset_tx, &tx->id );
  write_HEX_dword( REJECTED_response + offset_reason, reason );

  // Write
  if( __write_response( consumer_service, REJECTED_response, sz_nonull ) < 0 ) {
    return -1;
  }

  // Reset
  __reset_tx( consumer_service );

  CONSUMER_SERVICE_WARNING( consumer_service, 0x001, "[Send REJECTED] tx=%016llx%016llx", tx->id.H, tx->id.L );

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_SUSPEND( vgx_TransactionalConsumerService_t *consumer_service, int suspend_ms ) {
#define __SUSPEND_RESPONSE KWD_SUSPEND " 00000000 # vgx\n"
  static __THREAD char SUSPEND_response[] = __SUSPEND_RESPONSE;
  static const DWORD offset_reason = sizeof( KWD_SUSPEND ); // points to first hex digit in reason (sizeof includes nul which account for the space)
  static const int64_t sz_nonull = sizeof( SUSPEND_response ) - 1;

  DWORD reason = __suspend_reason( suspend_ms );

  vgx_OpSuspendCode code = __suspend_code_from_reason( reason );
  switch( code ) {
  case OP_SUSPEND_CODE_AUTORESUME_TIMEOUT:
    CONSUMER_SERVICE_VERBOSE( consumer_service, 0x001, "[Send SUSPEND] Auto-resume after %d ms", suspend_ms );
    break;
  case OP_SUSPEND_CODE_INDEFINITE:
    CONSUMER_SERVICE_VERBOSE( consumer_service, 0x002, "[Send SUSPEND] Indefinite suspend until explicit resume" );
    break;
  default:
    CONSUMER_SERVICE_VERBOSE( consumer_service, 0x003, "[Send SUSPEND] %d ms", suspend_ms );
  }

  // Fill in response fields and 
  write_HEX_dword( SUSPEND_response + offset_reason, reason );

  // Write
  if( __write_response( consumer_service, SUSPEND_response, sz_nonull ) < 0 ) {
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_RESUME( vgx_TransactionalConsumerService_t *consumer_service ) {
#define __RESUME_RESPONSE KWD_RESUME " # vgx\n"
  static __THREAD char RESUME_response[] = __RESUME_RESPONSE;
  static const int64_t sz_nonull = sizeof( RESUME_response ) - 1;

  CONSUMER_SERVICE_VERBOSE( consumer_service, 0x001, "[Send RESUME]" );

  // Write
  if( __write_response( consumer_service, RESUME_response, sz_nonull ) < 0 ) {
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_ATTACH( vgx_TransactionalConsumerService_t *consumer_service, DWORD protocol, DWORD version ) {
  // ATTACH 00000000 00000000 00000000000000000000000000000000 0000 # vgx\n\n
  // 0000000000111111111122222222223
  // 0123456789012345678901234567890
  // x      x                 x
#define __ATTACH_RESPONSE KWD_ATTACH " 00000000 00000000 00000000000000000000000000000000 0000 # vgx\n\n"
  static __THREAD char ATTACH_response[] = __ATTACH_RESPONSE;
  static const DWORD offset_protocol = sizeof( KWD_ATTACH );        // points to first hex digit in protocol
  static const DWORD offset_version = sizeof( KWD_ATTACH ) + 9;     // points to first hex digit in version
  static const DWORD offset_fp = sizeof( KWD_ATTACH ) + 18;         // points to first hex digit in fingerprint
  static const DWORD offset_adminport = sizeof( KWD_ATTACH ) + 51;  // points to first hex digit in adminport
  static const int64_t sz_nonull = sizeof( ATTACH_response ) - 1;

  objectid_t fingerprint = igraphfactory.Fingerprint();
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  int adminport = -1;
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      adminport = iVGXServer.Service.GetAdminPort( SYSTEM ); 
    } GRAPH_RELEASE;
  }

  CONSUMER_SERVICE_VERBOSE( consumer_service, 0x001, "[Send ATTACH] %08X %08X", protocol, version );

  // Fill in response fields and 
  write_HEX_dword( ATTACH_response + offset_protocol, protocol );
  write_HEX_dword( ATTACH_response + offset_version, version );
  write_hex_objectid( ATTACH_response + offset_fp, &fingerprint );
  if( adminport < 0 || adminport > 0xFFFF ) {
    adminport = 0;
  }
  write_HEX_word( ATTACH_response + offset_adminport, (WORD)adminport );

  // Write
  if( __write_response( consumer_service, ATTACH_response, sz_nonull ) < 0 ) {
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __produce_response_DETACH( vgx_TransactionalConsumerService_t *consumer_service ) {
#define __DETACH_RESPONSE KWD_DETACH " # vgx\n"
  static __THREAD char DETACH_response[] = __DETACH_RESPONSE;
  static const int64_t sz_nonull = sizeof( DETACH_response ) - 1;

  CONSUMER_SERVICE_VERBOSE( consumer_service, 0x001, "[Send DETACH]" );

  // Write
  if( __write_response( consumer_service, DETACH_response, sz_nonull ) < 0 ) {
    return -1;
  }

  return 1;
}





#define __skip_spaces( ptr ) while( *ptr && *ptr <= 32 ) { ++ptr; }
#define __skip_line( ptr ) while( *ptr && *ptr != '\n' ) { ++ptr; }
#define __is_end_or_comment( ptr ) (!*ptr || *ptr == '#')


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OpKeyword __get_request_KWD( const char *line ) {
  __skip_spaces( line );
  vgx_OpKeyword op_kwd;
  const char *kwd;
  size_t sz;
  switch( *line ) {
  // OP
  case 'O':
    kwd = kwd_OP;
    sz = sz_OP;
    op_kwd = OP_KWD_OP;
    break;
  // ENDOP
  case 'E':
    kwd = kwd_ENDOP;
    sz = sz_ENDOP;
    op_kwd = OP_KWD_ENDOP;
    break;
  // TRANSACTION
  case 'T':
    kwd = kwd_TRANSACTION;
    sz = sz_TRANSACTION;
    op_kwd = OP_KWD_TRANSACTION;
    break;
  // COMMIT
  case 'C':
    kwd = kwd_COMMIT;
    sz = sz_COMMIT;
    op_kwd = OP_KWD_COMMIT;
    break;
  // RESYNC
  case 'R':
    kwd = kwd_RESYNC;
    sz = sz_RESYNC;
    op_kwd = OP_KWD_RESYNC;
    break;
  // IDLE
  case 'I':
    kwd = kwd_IDLE;
    sz = sz_IDLE;
    op_kwd = OP_KWD_IDLE;
    break;
  // ATTACH
  case 'A':
    kwd = kwd_ATTACH;
    sz = sz_ATTACH;
    op_kwd = OP_KWD_ATTACH;
    break;
  // DETACH
  case 'D':
    kwd = kwd_DETACH;
    sz = sz_DETACH;
    op_kwd = OP_KWD_DETACH;
    break;
  // Comment or empty line
  case '#':
  case '\0':
    return OP_KWD_none;
  // operator
  default:
    return OP_KWD_operator;
  }

  if( !strncmp( line, kwd, sz ) ) {
    return op_kwd;
  }
  else {
    return OP_KWD_none;
  }

}



/*******************************************************************//**
 *
 * Check if line is IDLE line of the form:
 *
 * IDLE <tms> <fingerprint>\n
 * -------------------------------------------------------------
 * IDLE 00000178131E0F18 0348a020e49760fc382f1569552eff5f\n
 *
 * Returns: 0 if not IDLE line
 *          1 if IDLE line
 *         -1 if bad IDLE line
 *
 ***********************************************************************
 */
static int __parse_request_IDLE( const char *line, int64_t *rtms, objectid_t *rfingerprint ) {
  // Skip over any leading whitespace
  __skip_spaces( line );
  // Check if IDLE
  if( strncmp( line, kwd_IDLE, sz_IDLE ) ) {
    // not IDLE
    return 0;
  }
  line += sz_IDLE;
  __skip_spaces( line );
  // Parse timestamp
  QWORD tms = 0;
  if( (line = hex_to_QWORD( line, &tms )) == NULL ) {
    // Bad tms
    return -1;
  }
  __skip_spaces( line );
  // Parse fingerprint
  objectid_t fingerprint = strtoid( line );
  if( idnone( &fingerprint ) ) {
    // Bad fingerprint
    return -1;
  }
  line += 32;
  // Verify newline
  __skip_line( line );
  if( *line != '\n' ) {
    // No newline, IDLE not acceptable
    return -1;
  }
  // IDLE <tms> <fingerprint>\n
  *rtms = tms;
  *rfingerprint = fingerprint;
  return 1;
}



/*******************************************************************//**
 *
 * Check if line is TRANSACTION line of the form:
 *
 * TRANSACTION <transid>                        <serial>         <master_serial>\n
 * ------------------------------------------------------------------------------
 * TRANSACTION 2b398fcb29553c91c18769c38db6260c 16E26E341DB85FC5 16E26E341DB85FC5\n
 *
 * Returns: 0 if not TRANSACTION line
 *          1 if TRANSACTION line
 *         -1 if bad TRANSACTION line
 *
 ***********************************************************************
 */
static int __parse_request_TRANSACTION( const char *line, vgx_OperationTransaction_t *rtx, int64_t *rmaster_sn ) {
  const char *cp = line;
  // Skip over any leading whitespace
  __skip_spaces( cp );
  // Check if TRANSACTION
  if( strncmp( cp, kwd_TRANSACTION, sz_TRANSACTION ) ) {
    // not TRANSACTION
    return 0;
  }
  cp += sz_TRANSACTION;
  __skip_spaces( cp );
  // Parse transaction id
  objectid_t tx_id = strtoid( cp );
  // Bad id
  if( idnone( &tx_id ) ) {
    return -1;
  }
  cp += 32;
  __skip_spaces( cp );
  // Parse serial
  QWORD sn = 0;
  if( (cp = hex_to_QWORD( cp, &sn )) == NULL ) {
    // Bad serial
    return -1;
  }
  __skip_spaces( cp );
  // Parse master serial
  QWORD master_sn = 0;
  if( (cp = hex_to_QWORD( cp, &master_sn )) == NULL ) {
    // Bad master serial
    return -1;
  }
  // Verify newline
  __skip_line( cp );
  if( *cp++ != '\n' ) {
    // No newline, TRANSACTION not acceptable
    return -1;
  }
  // TRANSACTION <transid> <serial> <master_serial>\n
  idcpy( &rtx->id, &tx_id );
  rtx->serial = sn;
  *rmaster_sn = master_sn;

  return 1;
}



/*******************************************************************//**
 *
 * Check if line is COMMIT line of the form:
 *
 * COMMIT <transid> <tms> <crc32c>\r\n
 * -------------------------------------------------------------
 * COMMIT f0863bfb838b30363d7844fdbc943877 00000181C60995E2 A09D1D50\r\n
 *
 * Returns: 0 if not COMMIT line
 *          1 if COMMIT line
 *         -1 if bad COMMIT line
 *
 ***********************************************************************
 */
static int __parse_request_COMMIT( const char *line, objectid_t *rtx_id, int64_t *rtms, DWORD *rcrc ) {
  // Skip over any leading whitespace
  __skip_spaces( line );
  // Check if COMMIT
  if( strncmp( line, kwd_COMMIT, sz_COMMIT ) ) {
    // not COMMIT
    return 0;
  }
  line += sz_COMMIT;
  __skip_spaces( line );
  // Parse transaction id
  objectid_t tx_id = strtoid( line );
  // Bad id
  if( idnone( &tx_id ) ) {
    return -1;
  }
  line += 32;
  __skip_spaces( line );
  // Parse TMS
  QWORD tms;
  if( (line = hex_to_QWORD( line, &tms )) == NULL ) {
    // Bad tms
    return -1;
  }
  __skip_spaces( line );
  // Parse CRC32C
  DWORD crc = 0;
  if( (line = hex_to_DWORD( line, &crc )) == NULL ) {
    // Bad crc
    return -1;
  }
  // Verify CRLF
  __skip_line( line );
  if( *(line-1) != '\r' || *line != '\n' ) {
    // No CRLF terminator, COMMIT not acceptable
    return -1;
  }
  // COMMIT <transid> <tms> <crc32c>\r\n
  idcpy( rtx_id, &tx_id );
  *rtms = tms;
  *rcrc = crc;
  return 1;
}



/*******************************************************************//**
 *
 * Check if line is OP line of the form:
 *
 * OP <optype> [ <opgraph> [ <objectid> ]] \n
 * -------------------------------------------------------------
 * OP 0001 \n
 * OP 200A 31e259a88d0d4db53a4dbb629ded46ff \n
 * OP 2001 31e259a88d0d4db53a4dbb629ded46ff 9238b8c482371600b4448da21405865a \n
 *
 * Returns: 0 if not OP line
 *          1 if OP line
 *         -1 if bad OP line
 *
 ***********************************************************************
 */
static int __parse_request_OP( const char *line, OperationProcessorOpType *roptype, objectid_t *ropgraph, objectid_t *robid ) {
  // Skip over any leading whitespace
  __skip_spaces( line );
  // Check if OP
  if( strncmp( line, kwd_OP, sz_OP ) ) {
    // not OP
    return 0;
  }
  line += sz_OP;
  __skip_spaces( line );
  // Parse optype
  WORD optype = 0;
  if( (line = hex_to_WORD( line, &optype )) == NULL ) {
    // bad optype
    return -1;
  }
  __skip_spaces( line );

  // OP <optype> \n
  if( __is_end_or_comment( line ) ) {
    *roptype = optype;
    return 1;
  }
  // Parse opgraph
  objectid_t graphid = strtoid( line );
  // Bad id
  if( idnone( &graphid ) ) {
    return -1;
  }
  line += 32;
  __skip_spaces( line );

  // OP <optype> <opgraph> \n
  if( __is_end_or_comment( line ) ) {
    *roptype = optype;
    idcpy( ropgraph, &graphid );
    return 1;
  }
  // Parse objectid
  objectid_t obid = strtoid( line );
  // Bad id
  if( idnone( &obid ) ) {
    return -1;
  }
  line += 32;
  __skip_spaces( line );

  // OP <optype> <opgraph> <objectid> \n
  if( __is_end_or_comment( line ) ) {
    *roptype = optype;
    idcpy( ropgraph, &graphid );
    idcpy( robid, &obid );
    return 1;
  }

  // Invalid data beyond end, not acceptable
  return -1;

}



/*******************************************************************//**
 *
 * Check if line is ENDOP line of the form:
 *
 * ENDOP [ <opid> <tms> ] <crc32c>\n
 * -------------------------------------------------------------
 * ENDOP 002386F26FC5DF43 0000017FE29C0367 FB25A45E \n
 * ENDOP 5B91FA01 \n
 * 
 * Returns: 0 if not ENDOP line
 *          1 if ENDOP line
 *         -1 if bad ENDOP line
 *
 ***********************************************************************
 */
static int __parse_request_ENDOP( const char *line, int64_t *ropid, int64_t *rtms, DWORD *rcrc ) {
  QWORD opid = 0;
  QWORD tms = 0;
  DWORD crc = 0;
  // Skip over any leading whitespace
  __skip_spaces( line );
  // Check if ENDOP
  if( strncmp( line, kwd_ENDOP, sz_ENDOP ) ) {
    // not ENDOP
    return 0;
  }
  line += sz_ENDOP;
  __skip_spaces( line );
  // Try to parse opid
  const char *tmp;
  if( (tmp = hex_to_QWORD( line, &opid )) == NULL ) {
    // not opid, try crc
    goto parse_crc;
  }
  line = tmp;
  __skip_spaces( line );
  // Parse tms
  if( (line = hex_to_QWORD( line, &tms )) == NULL ) {
    // bad tms
    return -1;
  }
  __skip_spaces( line );
parse_crc:
  // Parse crc
  if( (line = hex_to_DWORD( line, &crc )) == NULL ) {
    // bad crc
    return -1;
  }
  // Verify newline
  __skip_line( line );
  if( *line != '\n' ) {
    // No newline, ENDOP not acceptable
    return -1;
  }
  *rcrc = crc;
  // ENDOP <crc>\n
  if( opid == 0 ) {
    return 1;
  }
  // ENDOP <opid> <tms> <crc>\n
  *ropid = opid;
  *rtms = tms;
  return 1;
}



/*******************************************************************//**
 *
 * Check if line is RESYNC line of the form:
 *
 * RESYNC <transid> <nrollback>\n
 * -------------------------------------------------------------
 * RESYNC af40be46919f0c960f666ea2319a6e6b 00000000000C1410\n
 *
 * Returns: 0 if not RESYNC line
 *          1 if RESYNC line
 *         -1 if bad RESYNC line
 *
 ***********************************************************************
 */
static int __parse_request_RESYNC( const char *line, objectid_t *rid, int64_t *rnrollback ) {
  const char *cp = line;
  // Skip over any leading whitespace
  __skip_spaces( cp );
  // Check if RESYNC
  if( strncmp( cp, kwd_RESYNC, sz_RESYNC ) ) {
    // not RESYNC
    return 0;
  }
  cp += sz_RESYNC;
  __skip_spaces( cp );
  // Parse transaction id
  objectid_t tx_id = strtoid( cp );
  // Bad id
  if( idnone( &tx_id ) ) {
    return -1;
  }
  cp += 32;
  __skip_spaces( cp );
  // Parse nrollback
  QWORD nrollback = 0;
  if( (cp = hex_to_QWORD( cp, &nrollback )) == NULL ) {
    // Bad serial
    return -1;
  }
  // Verify newline
  __skip_line( cp );
  if( *cp++ != '\n' ) {
    // No newline, RESYNC not acceptable
    return -1;
  }
  // RESYNC <transid> <nrollback>\n
  idcpy( rid, &tx_id );
  *rnrollback = nrollback;

  return 1;
}



/*******************************************************************//**
 *
 * Check if line is ATTACH line of the form:
 *
 * ATTACH <protocol> <version> <fingerprint> <adminport>\n
 * -------------------------------------------------------------
 * ATTACH 00010000 00010000 0348a020e49760fc382f1569552eff5f 1234\n
 *
 * Returns: 0 if not ATTACH line
 *          1 if ATTACH line
 *         -1 if bad ATTACH line
 *
 ***********************************************************************
 */
static int __parse_request_ATTACH( const char *line, DWORD *rprotocol, DWORD *rversion, objectid_t *rfingerprint, WORD *radminport ) {
  const char *cp = line;
  // Skip over any leading whitespace
  __skip_spaces( cp );
  // Check if ATTACH
  if( strncmp( cp, kwd_ATTACH, sz_ATTACH ) ) {
    // not ATTACH
    return 0;
  }
  cp += sz_ATTACH;
  __skip_spaces( cp );
  
  // <protocol>
  DWORD protocol;
  if( (cp = hex_to_DWORD( cp, &protocol )) == NULL ) {
    return -1; // bad protocol
  }
  __skip_spaces( cp );

  // <version>
  DWORD version;
  if( (cp = hex_to_DWORD( cp, &version )) == NULL ) {
    return -1; // bad version
  }
  __skip_spaces( cp );

  // <fingerprint>
  objectid_t fingerprint = strtoid( cp );
  if( idnone( &fingerprint ) ) {
    return -1; // bad fingerprint
  }
  cp += 32;
  __skip_spaces( cp );

  // <adminport>
  WORD adminport;
  if( (cp = hex_to_WORD( cp, &adminport )) == NULL ) {
    return -1; // bad version
  }

  // Verify newline
  __skip_line( cp );
  if( *cp != '\n' ) {
    // No newline, ATTACH not acceptable
    return -1;
  }
  // ATTACH <protocol> <version> <fingerprint> <adminport>\n
  *rprotocol = protocol;
  *rversion = version;
  *rfingerprint = fingerprint;
  *radminport = adminport;
  return 1;
}



/*******************************************************************//**
 *
 * Check if line is DETACH line of the form:
 *
 * DETACH\n
 * -------------------------------------------------------------
 * DETACH\n
 *
 * Returns: 0 if not DETACH line
 *          1 if DETACH line
 *         -1 if bad DETACH line
 *
 ***********************************************************************
 */
static int __parse_request_DETACH( const char *line ) {
  // Skip over any leading whitespace
  __skip_spaces( line );
  // Check if DETACH
  if( strncmp( line, kwd_DETACH, sz_DETACH ) ) {
    // not DETACH
    return 0;
  }
  line += sz_DETACH;
  // Verify newline
  __skip_line( line );
  if( *line != '\n' ) {
    // No newline, DETACH not acceptable
    return -1;
  }
  // DETACH\n
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_IDLE( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_IDLE[] = "[Recv IDLE]";

  objectid_t provider_fingerprint = {0};
  if( __parse_request_IDLE( line, &consumer_service->provider.state_tms, &provider_fingerprint ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_IDLE, line );
    return -1;
  }

  GRAPH_LOCK( consumer_service->sysgraph ) {
    consumer_service->provider.fingerprint = provider_fingerprint;
  } GRAPH_RELEASE;

  return __produce_response_IDLE( consumer_service, __GET_CURRENT_MILLISECOND_TICK() );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_COMMIT( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_COMMIT[] = "[Recv COMMIT]";

  int64_t n_submitted = 0;

  vgx_ByteArrayList_t *txdata = &consumer_service->txdata;

  if( txdata->sz > 0 ) {
    objectid_t commit_id = {0};
    int64_t orig_tms = 0;
    DWORD crc = 0;

    // Parse the COMMIT line
    if( __parse_request_COMMIT( line, &commit_id, &orig_tms, &crc ) <= 0 ) {
      CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_COMMIT, line );
      return -1;
    }

    vgx_OperationTransaction_t *tx = &consumer_service->transaction;

    // Make sure COMMIT txid matches TRANSACTION txid
    if( !idmatch( &commit_id, &tx->id ) ) {
      CONSUMER_SERVICE_REASON( consumer_service, 0x002, "%s mismatch tx=%016llx%016llx, expected tx=%016llx%016llx", recv_COMMIT, commit_id.H, commit_id.L, tx->id.H, tx->id.L );
      return -1;
    }

    // Verify CRC
    if( tx->crc != crc ) {
      CONSUMER_SERVICE_REASON( consumer_service, 0x003, "%s crc mismatch for tx=%016llx%016llx, got crc=%08X, expected crc=%08X", recv_COMMIT, tx->id.H, tx->id.L, tx->crc, crc );
      return -1;
    }

    // Commit transaction data into the system
    if( (n_submitted = __commit_tx( consumer_service, orig_tms )) < 0 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "%s Failed to submit transaction for processing", recv_COMMIT );
      // Failure to submit is a critical event which results in the consumer service detaching itself
      // from the producer.
      __produce_response_DETACH( consumer_service );
      // We return 0 to avoid triggering a RETRY.
      // We will expect the producer to send us a final DETACH confirmation
      return 0;
    }
    // ...then on success, respond with ACCEPTED
    else if( __produce_response_ACCEPTED( consumer_service ) < 0 ) {
      return -1;
    }

    // Clear transaction after success
    memset( &consumer_service->transaction, 0, sizeof( vgx_OperationTransaction_t ) );
  }
  return n_submitted;
}



/*******************************************************************//**
 *
 * Returns: 1: OP can proceed
 *          0: OP cannot proceed at this time (RETRY later)
 *         -1: Invalid OP
 *
 ***********************************************************************
 */
static int __handle_request_OP( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_OP[] = "[Recv OP]";
  OperationProcessorOpType optype = OPTYPE_NONE;
  objectid_t opgraph = {0};
  objectid_t obid = {0};
  if( __parse_request_OP( line, &optype, &opgraph, &obid ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_OP, line );
    return -1;
  }
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_ENDOP( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_ENDOP[] = "[Recv ENDOP]";
  int64_t opid = 0;
  DWORD crc = 0;
  int64_t tms = 0;
  if( __parse_request_ENDOP( line, &opid, &tms, &crc ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_ENDOP, line );
    return -1;
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_RESYNC( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_RESYNC[] = "[Recv RESYNC]";
  objectid_t rtx = {0};
  int64_t nrollback = 0;
  if( __parse_request_RESYNC( line, &rtx, &nrollback ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_RESYNC, line );
    return -1;
  }

  if( consumer_service->resync_pending ) {
    objectid_t *etx = &consumer_service->resync_tx;
    if( !idmatch( &rtx, etx) ) {
      CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "%s mismatch tx=%016llx%016llx, expected tx=%016llx%016llx", recv_RESYNC, rtx.H, rtx.L, etx->H, etx->L );
    }

    consumer_service->resync_pending = 0;

    __reset_tx( consumer_service );

    CONSUMER_SERVICE_WARNING( consumer_service, 0x003, "%s tx=%016llx%016llx", recv_RESYNC, rtx.H, rtx.L );
  }
  else {
    CONSUMER_SERVICE_WARNING( consumer_service, 0x004, "%s unexpected for tx=%016llx%016llx - ignoring", recv_RESYNC, rtx.H, rtx.L );
  }

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_TRANSACTION( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_TRANSACTION[] = "[Recv TRANSACTION]";
  vgx_OperationTransaction_t *tx = &consumer_service->transaction;
  if( __parse_request_TRANSACTION( line, tx, &consumer_service->provider.master_serial ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_TRANSACTION, line );
    return -1;
  }

  // Clear resync transaction id if we have one
  objectid_t *rtx = &consumer_service->resync_tx;
  if( !idnone( rtx ) ) {
    if( !idmatch( rtx, &tx->id ) ) {
      CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "%s RESYNC mismatch tx=%016llx%016llx, expected tx=%016llx%016llx", recv_TRANSACTION, tx->id.H, tx->id.L, rtx->H, rtx->L );
    }
    idunset( rtx );
  }

  tx->tms = _vgx_graph_milliseconds( consumer_service->sysgraph );

  tx->crc = 0;
  tx->tsize = 0;

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_ATTACH( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_ATTACH[] = "[Recv ATTACH]";

  DWORD protocol;
  DWORD version;
  objectid_t fingerprint;
  WORD adminport;

  if( __parse_request_ATTACH( line, &protocol, &version, &fingerprint, &adminport ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_ATTACH, line );
    return -1;
  }

  // TOOD: Proper implementation of protocol and version validation
  DWORD server_protocol = 0x00010000;
  DWORD server_version = 0x00010000;
  DWORD client_protocol = protocol & 0xFFFF0000;
  DWORD client_version = version & 0xFFFF0000;

  int nRO = igraphfactory.CountAllReadonly();
  if( nRO != 0 ) {
    vgx_OperationTransaction_t *tx = &consumer_service->transaction;
    idset( &tx->id, ULLONG_MAX, ULLONG_MAX );
    __produce_response_REJECTED( consumer_service, OP_REJECT_CODE_READONLY );
    return __produce_response_DETACH( consumer_service );
  }

  if( client_protocol == server_protocol ) { // Server protocol and client protocol must match
    if( client_version <= server_version ) { // Server version must be at least as high as client version
      if( consumer_service->TransactionProducerClient ) {
        consumer_service->control.attached = true;
        const char *client = iURI.URI( consumer_service->TransactionProducerClient );
        CONSUMER_SERVICE_INFO( consumer_service, 0x002, "%s VGX Provider @ %s protocol=%08X version=%08X", recv_ATTACH, client, protocol, version );
        return __produce_response_ATTACH( consumer_service, server_protocol, server_version );
      }
    }
  }

  // Unacceptable protocol/version: DETACH
  return __produce_response_DETACH( consumer_service );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_request_DETACH( vgx_TransactionalConsumerService_t *consumer_service, const char *line ) {
  static const char recv_DETACH[] = "[Recv DETACH]";

  if( __parse_request_DETACH( line ) <= 0 ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "%s invalid: %s", recv_DETACH, line );
    return -1;
  }

  return __operation_consumer__client_close( consumer_service );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __reset_tx( vgx_TransactionalConsumerService_t *consumer_service ) {
  vgx_ByteArrayList_t *txdata = &consumer_service->txdata;
  txdata->sz = 0;
  consumer_service->tx_cursor = txdata->entries;
  consumer_service->tx_slab_cursor = consumer_service->tx_slab;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __open_tx_input( vgx_TransactionalConsumerService_t *consumer_service, int64_t sn0, int64_t offset ) {
  if( !__txlog_enabled( consumer_service ) ) {
    return -1;
  }

  char buffer[ MAX_PATH + 1 ] = {0};

  // Generate full path to input file based on sn0
  const char *fname;
  const char *fpath = __get_tx_filepath( CStringValue( consumer_service->CSTR__tx_location ), sn0, buffer, &fname );

  // Close existing output file and delete its URI object if we need to open
  // the same file for reading. (A new output file will be opened for writing
  // whenever more data needs to be written.)
  if( consumer_service->tx_out_sn0 == sn0 ) {
    iURI.Delete( &consumer_service->tx_raw_storage );
  }

  // Open input file
  if( OPEN_R_SEQ( &consumer_service->tx_in_fd, fpath ) != 0 ) {
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0x001, "Failed to open transaction input file: %s", fpath );
    return -1;
  }

  consumer_service->tx_in_sn0 = sn0;

  CONSUMER_SERVICE_INFO( consumer_service, 0x002, "Applying TX input: %s @ offset=%lld (%lld bytes remain)", fpath, offset, consumer_service->tx_backlog_bytes );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __close_tx_input( vgx_TransactionalConsumerService_t *consumer_service ) {
  if( consumer_service->tx_in_fd > 0 ) {
    CX_CLOSE( consumer_service->tx_in_fd );
    consumer_service->tx_in_fd = 0;
  }
  consumer_service->tx_in_sn0 = 0;
  return 0;
}



/*******************************************************************//**
 *
 * Load transaction data from this transaction.
 * 
 * Data is written to dest, if not NULL.
 * 
 * Returns transaction size:
 *   > 0 : size of transaction found and loaded into dest if provided
 *  <= 0 : error
 *
 ***********************************************************************
 */
static int64_t __load_tx( vgx_TransactionalConsumerService_t *consumer_service, const objectid_t *txid, vgx_OperationBuffer_t *dest ) {
  int64_t tx_sz = 0;

  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( SYSTEM )->advanced;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  char idbuf[33];

  vgx_Vertex_t *TX = NULL;

  XTRY {
    // Open transaction
    if( (TX = Advanced->AcquireVertexReadonly( SYSTEM, txid, 60000, &reason )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x001, "Failed to open transaction tx:%s reason:%03X", idtostr( idbuf, txid ), reason ); // failed to open transaction
    }

    // Get the transaction size
    CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_transaction_size, &tx_sz );

    // Get serial number
    int64_t sn0 = 0;
    CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_file_serial0, &sn0 );

    // Get file offset
    int64_t offset = 0;
    CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_file_offset, &offset );

    // Prepare destination to be able to accommodate the transation data
    char *segment = NULL;
    while( iOpBuffer.WritableSegment( dest, tx_sz, &segment, NULL ) < tx_sz ) {
      if( iOpBuffer.Expand( dest ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 ); // memory error
      }
    }

    // Input file not already open, open it now
    if( sn0 != consumer_service->tx_in_sn0 || consumer_service->tx_in_fd == 0 ) {
      // Close previous input file if open
      __close_tx_input( consumer_service );

      // Open current input file
      if( __open_tx_input( consumer_service, sn0, offset ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x003 ); // could not open file
      }
    }
  
    // Locate the transaction
    CX_SEEK( consumer_service->tx_in_fd, offset, SEEK_SET );

    // Read transaction data from file into destination buffer
    if( CX_READ( segment, 1, tx_sz, consumer_service->tx_in_fd ) != (size_t)tx_sz ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x004, "File I/O Error: Failed to read %lld bytes", tx_sz ); // could not read from file
    }

    // Advance write pointer in destination
    iOpBuffer.AdvanceWrite( dest, tx_sz );
  }
  XCATCH( errcode ) {
    tx_sz = -1;
  }
  XFINALLY {
    if( TX ) {
      Simple->CloseVertex( SYSTEM, &TX );
    }
  }

  return tx_sz;
}



/*******************************************************************//**
 *
 * Return the ID of the transaction following this_txid. If found, the
 * next transaction ID is returned in next_txid.
 * 
 * Returns transaction size:
 *     1 : Next ID found and returned in next_txid
 *     0 : Transaction was last in the chain and has no next
 *    -1 : error
 *
 ***********************************************************************
 */
static int __next_tx( vgx_TransactionalConsumerService_t *consumer_service, const objectid_t *this_txid, objectid_t *next_txid ) {
  int hasnext = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( SYSTEM )->advanced;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  char idbuf[33];

  vgx_Vertex_t *TX = NULL;
  CString_t *CSTR__next = NULL;

  XTRY {
    // Open transaction
    if( (TX = Advanced->AcquireVertexReadonly( SYSTEM, this_txid, 60000, &reason )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x001, "Failed to open transaction tx:%s reason:%03X", idtostr( idbuf, this_txid ), reason ); // failed to open transaction
    }

    // Get next transaction from property
    int err = CALLABLE( TX )->GetStringProperty( TX, TX_STR_PROP_next, &CSTR__next );
    Simple->CloseVertex( SYSTEM, &TX );

    // Next transaction exists
    if( !err ) {
      const char *next = CStringValue( CSTR__next );
      *next_txid = strtoid( next );
      if( idnone( next_txid ) ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x002, "Empty transaction ID" ); // empty id
      }
      if( idmatch( next_txid, this_txid ) ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x003, "Transaction self-reference tx:%s", idtostr( idbuf, next_txid ) ); // self-reference
      }
      hasnext = 1;
    }
    // We have reached the last transaction, nothing more to load
    else {
      hasnext = 0;
    }
  }
  XCATCH( errcode ) {
    hasnext = -1;
  }
  XFINALLY {
    if( CSTR__next ) {
      iEnumerator_OPEN.Property.Value.Discard( SYSTEM, CSTR__next );
    }
    if( TX ) {
      Simple->CloseVertex( SYSTEM, &TX );
    }
  }

  return hasnext;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __is_rotation_due( const vgx_TransactionalConsumerService_t *consumer_service ) {
  bool expired = consumer_service->transaction.tms - consumer_service->tx_out_t0 > TRANSACTION_LOG_ROTATE_MAX_MILLISECONDS;
  bool too_big = consumer_service->tx_out_sz > TRANSACTION_LOG_ROTATE_MAX_BYTES;
  return expired || too_big;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __write_tx_output( vgx_TransactionalConsumerService_t *consumer_service, vgx_ByteArrayList_t *txdata, int64_t *n_written, CString_t **CSTR__error ) {

  if( !__txlog_enabled( consumer_service ) ) {
    return -1;
  }

  // No output file, open new
  if( consumer_service->tx_raw_storage == NULL ) {
    if( __open_tx_output( consumer_service, CSTR__error ) < 0 ) {
      return -1;
    }
  }
  // Rotate output file
  else if( __is_rotation_due( consumer_service ) ) {
    if( __rotate_current_tx_output( consumer_service, CSTR__error ) < 0 ) {
      return -1;
    }
  }

  // Output file descriptor
  int fd = iURI.File.GetDescriptor( consumer_service->tx_raw_storage );
  
  // Transaction will be written to this file offset
  int64_t offset = CX_TELL( fd );

  // Write all lines in batch to output file
  for( int64_t i=0; i<txdata->sz; i++ ) {
    vgx_ByteArray_t *entry = &txdata->entries[i];
    int64_t n = CX_WRITE( entry->data, 1, entry->len, fd );
    *n_written += n;
    if( n != entry->len ) {
      // BAD
      const char *uri = iURI.URI( consumer_service->tx_raw_storage );
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x001, "File I/O Error. Wrote %lld of %lld bytes to %s", n, entry->len, uri ); 
    }
  }

  // Keep track of total output size
  consumer_service->tx_out_sz += *n_written;

  // Index transaction for future lookup
  if( __index_tx( consumer_service, offset, CSTR__error ) < 0 ) {
    return -1;
  }

  return offset;
}



/*******************************************************************//**
 *
 * Commit data for exactly one complete transaction.
 *
 ***********************************************************************
 */
static int64_t __commit_tx( vgx_TransactionalConsumerService_t *consumer_service, int64_t orig_tms ) {
  int64_t n_commit = 0;

  char txid[33];
  vgx_OperationTransaction_t *tx = &consumer_service->transaction;

  vgx_ByteArrayList_t *txdata = &consumer_service->txdata;
  CString_t *CSTR__error = NULL;

  // Stream transaction data to output log
  if( __txlog_enabled( consumer_service ) ) {
    // Write data to file
    int64_t offset;
    if( (offset = __write_tx_output( consumer_service, txdata, &n_commit, &CSTR__error )) < 0 ) {
      const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x001, "Transaction %s data lost: %s", idtostr( txid, &tx->id ), msg ); 
      iString.Discard( &CSTR__error );
    }
    //
    GRAPH_LOCK( consumer_service->sysgraph ) {
      consumer_service->tx_log_total_sz_SYS_CS += n_commit;
    } GRAPH_RELEASE;
  }

  // Parser is not able to execute transactions at the moment.
  // Un-executed transaction data will be submitted in bulk once parser is
  // able to execute at a future point in time.
  if( consumer_service->tx_state.local.exec_suspended || consumer_service->tx_state.local.has_backlog ) {
    // Increase backlog bytes by amount committed but not fed to parser
    consumer_service->tx_backlog_bytes += n_commit;
    int64_t delta = tx->tms > orig_tms ? tx->tms - orig_tms : 0;
    GRAPH_LOCK( consumer_service->sysgraph ) {
      consumer_service->sysgraph->tx_input_lag_ms_CS = delta;
    } GRAPH_RELEASE;
  }
  else {
    vgx_OperationParser_t *parser = &consumer_service->sysgraph->OP.parser;
    int64_t n_submitted = 0;

    // Submit data to parser
    n_submitted = iOperation.Parser.SubmitData( parser, txdata, &CSTR__error );

    // Keep trying if we failed to submit
    if( n_submitted == 0 ) {
      // Max 2 minutes
      int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
      int64_t deadline = t0 + 2 * 60 * 1000;
      while( (n_submitted = iOperation.Parser.SubmitData( parser, txdata, &CSTR__error )) == 0 ) {
        const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
        CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "Temporary overload (%s)", msg ); 
        iString.Discard( &CSTR__error );
        if( __GET_CURRENT_MILLISECOND_TICK() > deadline ) {
          CSTR__error = CStringNewFormat( "Unable to submit data to parser after %.0f seconds", (deadline - t0)/1000.0 );
          n_submitted = -1;
          break;
        }
        else if( COMLIB_TASK__IsRequested_ForceExit( consumer_service->TASK ) ) {
          CSTR__error = CStringNew( "Forced exit while parser is unable to absorb data" );
          n_submitted = -1;
          break;
        }
      }
    }

    // Unrecoverable parser error. All we can do is log. Data is lost.
    if( n_submitted < 0 ) {
      const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "Transaction %s parser permanent error: %s", idtostr( txid, &tx->id ), msg );
      iString.Discard( &CSTR__error );
      n_commit = -1;
    }
    // Data submitted for execution
    else {
      // Sanity check size
      if( n_submitted != n_commit ) {
        // Output written to file should match data submitted to parser
        if( __txlog_enabled( consumer_service ) ) {
          CONSUMER_SERVICE_CRITICAL( consumer_service, 0x005, "Transaction data commit mismatch: commit=%lld submit=%lld", n_commit, n_submitted );
        }
        n_commit = n_submitted;
      }

      // Keep track of last executed transaction
      idcpy( &consumer_service->last_txid_executed, &tx->id );

      // After confirmed commit we transfer master serial to SYSTEM
      GRAPH_LOCK( consumer_service->sysgraph ) {
        consumer_service->sysgraph->sysmaster_tx_serial = consumer_service->provider.master_serial;
      } GRAPH_RELEASE;
    }
  }

  // Capture timestamp of last transaction
  consumer_service->last_tx_tms = tx->tms;

  // Reset and ready for next transaction
  __reset_tx( consumer_service );

  return n_commit;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __append_tx( vgx_TransactionalConsumerService_t *consumer_service, BYTE *data, int64_t sz ) {
  vgx_ByteArray_t **cursor = &consumer_service->tx_cursor;
  (*cursor)->data = data;
  (*cursor)->len = sz;
  ++(*cursor);
  return ++(consumer_service->txdata.sz);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef OPSTREAM_INJECT_RANDOM_ERROR_RATE 

/**************************************************************************//**
 * __random_noise_inject
 *
 ******************************************************************************
 */
static void __random_noise_inject( vgx_TransactionalConsumerService_t *consumer_service, const char *line, int64_t sz_line ) {
  // Corrupt inbound data at a low random rate to exercise the error detection / retry logic.
  if( randfloat() < (double)(OPSTREAM_INJECT_RANDOM_ERROR_RATE) ) {
    BYTE *pcorrupt = (BYTE*)line;
    int64_t x = rand63() % sz_line;
    if( (pcorrupt[x] = rand8()) > 250 ) {
      CONSUMER_SERVICE_INFO( consumer_service, 0x000, "*** Simulated disconnect follows ***" );
      iURI.Close( consumer_service->TransactionProducerClient );
    }
    else {
      CONSUMER_SERVICE_INFO( consumer_service, 0x000, "*** Simulated data corruption follows ***" );
    }
  }
}
#define ASSERT_ROBUST_FLOW( ConsumerService, Line, LineSize ) __random_noise_inject( ConsumerService, Line, LineSize )
#else
#define ASSERT_ROBUST_FLOW( ConsumerService, Line, LineSize ) ((void)0)
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __operation_consumer__handle_request( vgx_TransactionalConsumerService_t *consumer_service ) {

  static __THREAD int idle_resync_cnt = 0; 

  vgx_OperationTransaction_t *tx = &consumer_service->transaction;

  vgx_ByteArrayList_t *txdata = &consumer_service->txdata;
  const vgx_ByteArray_t *end_cursor = txdata->entries + TX_MAX_ENTRIES;

  int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
  int64_t t1 = t0;
  int64_t t_max = t0 + 1000;

  const BYTE *end_slab = consumer_service->tx_slab + TX_MAX_SIZE;

  int64_t sz_line = 0;

  vgx_OpKeyword OP_KWD = OP_KWD_none;

  int64_t slab_remain;
  while( (slab_remain = end_slab - consumer_service->tx_slab_cursor) > 0 && (consumer_service->tx_cursor < end_cursor) ) {
    BYTE *data = consumer_service->tx_slab_cursor;
    sz_line = iOpBuffer.ReadUntil( consumer_service->buffer.request, slab_remain, (char**)&data, '\n' );
    // Complete line and inspect
    if( sz_line > 0 ) {
      const char *line = (const char*)data;
      iOpBuffer.Confirm( consumer_service->buffer.request, sz_line );

      consumer_service->tx_lifetime_bytes += sz_line;

      ASSERT_ROBUST_FLOW( consumer_service, line, sz_line );

      // What kind of data line is this?
      OP_KWD = __get_request_KWD( line );

      // ATTACH handshake is required upon new connection
      if( consumer_service->control.attached == false ) {
        if( OP_KWD == OP_KWD_ATTACH ) {
          // Proper ATTACH message will set control.attached = true
          return __handle_request_ATTACH( consumer_service, line );
        }
        else {
          return __produce_response_DETACH( consumer_service );
        }
      }

      // If in resync state ignore all data until we encounter RESYNC.
      if( consumer_service->resync_pending && OP_KWD != OP_KWD_RESYNC ) {
        // We're only getting idle messages, no resync seems to be happening
        if( OP_KWD == OP_KWD_IDLE && ++idle_resync_cnt > 10 ) {
          idle_resync_cnt = 0;
          DWORD reason = __suspend_reason( 1 );
          return __produce_response_RETRY( consumer_service, reason );
        }
        // We're letting all other ops fly by while we wait for resync
        else {
          idle_resync_cnt = 0;
          return 1;
        }
      }

      switch( OP_KWD ) {
      case OP_KWD_operator:
        break;
      case OP_KWD_OP:
        // If graph OP, make sure graph is WRITABLE and enter readonly-disallowed mode.
        if( __handle_request_OP( consumer_service, line ) < 0 ) {
          DWORD reason = __suspend_reason( 1 );
          return __produce_response_RETRY( consumer_service, reason );
        }
        break;
      case OP_KWD_ENDOP:
        if( __handle_request_ENDOP( consumer_service, line ) < 0 ) {
          DWORD reason = __suspend_reason( 1 );
          return __produce_response_RETRY( consumer_service, reason );
        }
        break;
      case OP_KWD_TRANSACTION:
        // Begin a new transaction
        if( __handle_request_TRANSACTION( consumer_service, line ) < 0 ) {
          DWORD reason = __suspend_reason( 1 );
          return __produce_response_RETRY( consumer_service, reason );
        }
        break;
      case OP_KWD_none:
        // Comment or empty line outside of a transaction, ignore
        if( idnone( &tx->id ) ) {
          return 1;
        }
        break;
      case OP_KWD_RESYNC:
        // Resync, resume at next transaction
        idle_resync_cnt = 0;
        return __handle_request_RESYNC( consumer_service, line );
      case OP_KWD_IDLE:
        // Connection is idle, respond
        return __handle_request_IDLE( consumer_service, line );
      case OP_KWD_ATTACH:
        // Attach (repeated handshake while already attached, is ok)
        return __handle_request_ATTACH( consumer_service, line );
      case OP_KWD_DETACH:
        // Detach
        return __handle_request_DETACH( consumer_service, line );
      default:
        break;
      }

      // This data will eventually reach the parser
      __append_tx( consumer_service, data, sz_line );

      // Update transaction size
      tx->tsize += sz_line;

      // End of transaction reached, commit and respond
      if( OP_KWD == OP_KWD_COMMIT ) {

        // We can't handle input if execution is suspended and we don't have disk-backed transaction log enabled
        if( consumer_service->tx_state.local.exec_suspended && !__txlog_enabled( consumer_service ) ) {
          DWORD reason = __suspend_reason( -1 );
          __produce_response_RETRY( consumer_service, reason );
          return 0;
        }

        // Commit
        if( __handle_request_COMMIT( consumer_service, line ) < 0 ) {
          DWORD reason = __suspend_reason( 1 );
          return __produce_response_RETRY( consumer_service, reason );
        }

        t1 = __GET_CURRENT_MILLISECOND_TICK();
      }
      // Update running transaction crc
      else {
        tx->crc = crc32c( tx->crc, line, sz_line );
      }

      // Continue to next line
      consumer_service->tx_slab_cursor += sz_line + 1; // advance offset into slab by line size plus the null terminator
    }
    // No more lines
    else if( sz_line == 0 ) {
      return 0;
    }
    // Out of slab space
    else {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE01, "Out of transaction slab space" );
      break;
    }

    // Yield
    if( t1 - t0 > t_max ) {
      return 0;
    }

  }

  // To be here means
  //  1. Max txdata entries reached
  //  2. Max txdata data limit reached
  //
  // We need to flush what has been collected into txdata.
  // This is most likely not a complete transaction, but we have
  // no other option than to submit and reset the txdata to continue
  // reading request data.

  if( txdata->sz == 0 ) {
    // No lines completed, op buffer contains oversized line which must be handled separately
    // TODO: HANDLE OVERSIZED LINE
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE02, "Oversized transaction line (%lld bytes). Rejecting transaction." );
    // DISCARD EVERYTHING. THIS WILL LEAD TO ERRORS BUT SHOULD RECOVER. DATA FOR THIS LINE IS OF COURSE LOST.
    iOpBuffer.Clear( consumer_service->buffer.request );
    //  ...
    return __produce_response_REJECTED( consumer_service, OP_REJECT_CODE_OVERSIZED_TX_LINE );
  }
  else if( slab_remain <= 0 ) {
    int64_t byte_span = consumer_service->tx_slab_cursor - consumer_service->tx_slab;
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE03, "Oversized transaction data (%lld bytes) permanently rejected.", byte_span );
    // We are unable to handle the request.
    // This request is permanently rejected.
    return __produce_response_REJECTED( consumer_service, OP_REJECT_CODE_OVERSIZED_TX_DATA );
  }
  else {
    CONSUMER_SERVICE_REASON( consumer_service, 0xE04, "Unknown transaction error." );
    // We are unable to handle the request.
    // This request is permanently rejected.
    return __produce_response_REJECTED( consumer_service, OP_REJECT_CODE_GENERAL );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __operation_consumer__client_close( vgx_TransactionalConsumerService_t *consumer_service ) {
  if( consumer_service->TransactionProducerClient ) {
    const char *client = iURI.URI( consumer_service->TransactionProducerClient );
    CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Closing socket for client: %s", client );
    iOpBuffer.Clear( consumer_service->buffer.request );
    iOpBuffer.Clear( consumer_service->buffer.response );
    iURI.Delete( &consumer_service->TransactionProducerClient );
    consumer_service->control.attached = false;
    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    if( SYSTEM ) {
      GRAPH_LOCK( SYSTEM ) {
        iString.Discard( &consumer_service->CSTR__provider_uri_SYS_CS );
      } GRAPH_RELEASE;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __operation_consumer__client_exception( vgx_TransactionalConsumerService_t *consumer_service, int err ) {
  CString_t *CSTR__error = NULL;
  // ... because of socket error (disconnected or other I/O error)
  if( err ) {
    // I/O error
    iURI.Sock.Error( consumer_service->TransactionProducerClient, &CSTR__error, -1, 0 );
  }
  else {
    // Bad fd
    iURI.Sock.Error( consumer_service->TransactionProducerClient, &CSTR__error, EBADF, 0 );
  }
  const char *client = iURI.URI( consumer_service->TransactionProducerClient );
  CONSUMER_SERVICE_REASON( consumer_service, 0x001, "Socket I/O error for client: %s [%s]", client, CSTR__error ? CStringValue( CSTR__error ) : "?" );
  iString.Discard( &CSTR__error );
  __operation_consumer__client_close( consumer_service );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __operation_consumer__client_recv( vgx_TransactionalConsumerService_t *consumer_service ) {
#define RECV_CHUNK_SZ (1<<15)
#define MAX_RECV_HANDLE (1<<21)
#define RECV_CHOKE_SUSPEND_THRESHOLD      (1<<27)
#define RECV_CHOKE_RESUME_THRESHOLD       MAX_RECV_HANDLE
#define PARSER_PENDING_SUSPEND_THRESHOLD  (1<<26)
#define PARSER_PENDING_RESUME_THRESHOLD   (PARSER_PENDING_SUSPEND_THRESHOLD >> 1)

  static __THREAD int suspend_ms = 0;
  int64_t n_total = 0;
  int64_t n_pending = 0;
  int64_t sz_buffer = 0;
  int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
  int64_t t1 = t0;
  int64_t t_max = t0 + 3000;

  if( consumer_service->TransactionProducerClient == NULL ) {
    return -1;
  }

  CXSOCKET *psock = iURI.Sock.Input.Get( consumer_service->TransactionProducerClient );
  if( psock == NULL ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "no input socket" );
    return -1;
  }

  const char *client = iURI.URI( consumer_service->TransactionProducerClient );

  struct pollfd *pclient = consumer_service->pclient;

  // Sanity check client file descriptor
  if( pclient->fd != psock->s ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x002, "Unexpected client socket: %s", client );
    return -1;
  }

  vgx_OperationParser_t *parser = &consumer_service->sysgraph->OP.parser;

  bool readable;
  do {
    n_pending = iOperation.Parser.Pending( parser );
    sz_buffer = iOpBuffer.Readable( consumer_service->buffer.request );
    if( n_pending > PARSER_PENDING_SUSPEND_THRESHOLD || sz_buffer > RECV_CHOKE_SUSPEND_THRESHOLD ) {
      if( sz_buffer > n_pending ) {
        suspend_ms = (int)(sz_buffer / RECV_CHOKE_RESUME_THRESHOLD);
      }
      else {
        suspend_ms = (int)(n_pending / PARSER_PENDING_RESUME_THRESHOLD);
      }
    }
    if( suspend_ms > 0 ) {
      CONSUMER_SERVICE_VERBOSE( consumer_service, 0x003, "Input overload: buffer:%lld parser:%lld", sz_buffer, n_pending );
      __produce_response_SUSPEND( consumer_service, suspend_ms );
      if( n_pending < PARSER_PENDING_RESUME_THRESHOLD && sz_buffer < RECV_CHOKE_SUSPEND_THRESHOLD ) {
        __produce_response_RESUME( consumer_service );
        suspend_ms = 0;
      }
    }

    // Initialize to "no longer readable"
    int64_t n_recv = -1;
    readable = false;

    // Read bytes from socket into buffer until socket is no longer readable
    // (into linear segments of ring buffer)

    char *segment;
    int64_t sz_segment;
    do {
      // Get a linear region of request buffer into which we are able to receive bytes from socket
      if( (sz_segment = iOpBuffer.WritableSegment( consumer_service->buffer.request, RECV_CHUNK_SZ, &segment, NULL )) < 1 ) {
        // Expand request buffer if needed and try again
        if( iOpBuffer.Expand( consumer_service->buffer.request ) < 0 ) {
          CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "request buffer cannot be expanded beyond %lld bytes capacity", iOpBuffer.Capacity( consumer_service->buffer.request ) );
          return -1;
        }
        // Try again
        continue;
      }

      // Receive bytes from socket into request buffer and advance the request buffer write pointer
      if( (n_recv = cxrecv( psock, segment, sz_segment, 0 )) > 0 ) {
#ifdef OPSTREAM_DUMP_TX_IO
        __dump_tx_request_recv( segment, n_recv );
#endif
        readable = true;
        n_total += n_recv;
        if( iOpBuffer.AdvanceWrite( consumer_service->buffer.request, n_recv ) != n_recv ) {
          CONSUMER_SERVICE_CRITICAL( consumer_service, 0x005, "internal request buffer error: %s", client );
          return -1;
        }
      }
    } while( n_recv > 0 );

    // No longer readable
    if( n_recv < 0 ) {
      int err = errno;
      // ...because there is no data to read at the moment
      if( iURI.Sock.Busy( err ) ) {
        // Process all received data
        while( __operation_consumer__handle_request( consumer_service ) > 0 );
        // Client was closed
        if( consumer_service->TransactionProducerClient == NULL ) {
          return 0;
        }
      }
      else {
        __operation_consumer__client_exception( consumer_service, err );
        return -1;
      }
    }
    // Socket was closed
    else {
      __operation_consumer__client_close( consumer_service );
      return 0;
    }

    t1 = __GET_CURRENT_MILLISECOND_TICK();
    if( t1 > consumer_service->status_response_deadline_tms ) {
      __produce_response_IDLE( consumer_service, t1 );
    }

  } while( readable && n_total < MAX_RECV_HANDLE && t1 - t0 < t_max );

  return n_total;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __operation_consumer__client_send( vgx_TransactionalConsumerService_t *consumer_service ) {
#define SEND_CHUNK_SZ 8192

  if( consumer_service->TransactionProducerClient == NULL ) {
    return -1;
  }

  const char *segment;
  int64_t sz_segment;

  CXSOCKET *psock = iURI.Sock.Input.Get( consumer_service->TransactionProducerClient );
  if( psock == NULL ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x001, "no input socket" );
    return -1;
  }
  const char *client = iURI.URI( consumer_service->TransactionProducerClient );

  struct pollfd *pclient = consumer_service->pclient;

  // Sanity check client file descriptor
  if( pclient->fd != psock->s ) {
    CONSUMER_SERVICE_REASON( consumer_service, 0x002, "Unexpected client socket: %s", client );
    return -1;
  }

  do {
    if( (sz_segment = iOpBuffer.ReadableSegment( consumer_service->buffer.response, SEND_CHUNK_SZ, &segment, NULL )) > 0 ) {
#ifdef OPSTREAM_DUMP_TX_IO
      __dump_tx_response_sent( segment, sz_segment );
#endif
      int64_t n_sent = cxsend( psock, segment, sz_segment, 0 );
      if( n_sent > 0 ) {
        iOpBuffer.AdvanceRead( consumer_service->buffer.response, n_sent );
        iOpBuffer.Confirm( consumer_service->buffer.response, n_sent );
      }
      else {
        sz_segment = 0;
      }
    }
  } while( sz_segment > 0 );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_num_readonly_graphs( void ) {

  int n = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  if( CALLABLE( SYSTEM )->advanced->IsGraphReadonly( SYSTEM ) ) {
    ++n;
  }

  GRAPH_FACTORY_ACQUIRE {
    const vgx_Graph_t **graphs = NULL;
    if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) { 
      vgx_Graph_t **cursor = (vgx_Graph_t**)graphs;
      vgx_Graph_t *graph;
      while( (graph = *cursor++) != NULL ) {
        if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
          ++n;
        }
      }
      free( (void*)graphs );
      graphs = NULL;
    }
  } GRAPH_FACTORY_RELEASE;

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __suspend_tx_execution_TCS( comlib_task_t *self, vgx_TransactionalConsumerService_t *consumer_service ) {
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_OperationParser_t *parser = &SYSTEM->OP.parser;
  if( !consumer_service->tx_state.local.exec_suspended ) {
    // If parser flush is required for suspend to be confirmed we must wait
    // until parser reports zero bytes pending.
    if( CONSUMER_SERVICE_IS_SET_FLUSH_PARSER_TCS( consumer_service ) ) {
      const int64_t max_no_progress_ms = 15000;
      int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
      int64_t t1 = t0;
      int64_t n_pending_0 = 0;
      int64_t n_pending_1 = n_pending_0;
      // Hold here until transaction input parser is idle
      while( (n_pending_1 = iOperation.Parser.Pending( parser )) > 0 ) {
        COMLIB_TASK_SUSPEND_MILLISECONDS( self, 10 );
        t1 = __GET_CURRENT_MILLISECOND_TICK();
        // Progress is being made
        if( n_pending_1 > n_pending_0 ) {
          n_pending_0 = n_pending_1;
          t0 = t1;
        }
        // Too much time has passed since last time parser made progress
        else if( t1 - t0 > max_no_progress_ms ) {
          break;
        }
      }
      // Parser is idle, suspend tx execution
      if( iOperation.Parser.Pending( parser ) == 0 ) {
        consumer_service->tx_state.local.exec_suspended = true;
        consumer_service->tx_state.public_TCS.suspended = true;
        CONSUMER_SERVICE_CLEAR_FLUSH_PARSER_TCS( consumer_service );
      }
    }
    // Suspend tx execution
    else {
      consumer_service->tx_state.local.exec_suspended = true;
      consumer_service->tx_state.public_TCS.suspended = true;
    }
  }

  if( consumer_service->tx_state.local.exec_suspended ) {
    CONSUMER_SERVICE_CLEAR_TX_EXECUTION_SUSPEND_REQUEST_TCS( consumer_service );
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
static int __resume_tx_execution_TCS( vgx_TransactionalConsumerService_t *consumer_service ) {
  if( consumer_service->tx_state.local.exec_suspended ) {
    consumer_service->tx_state.local.exec_suspended = false;
    consumer_service->tx_state.public_TCS.suspended = false;
  }
  CONSUMER_SERVICE_CLEAR_TX_EXECUTION_RESUME_REQUEST_TCS( consumer_service );

  return 1;
}



/*******************************************************************//**
 *
 * Execute neighborhood query for TX_ROOT's transaction nodes matching
 * filter criteria. We will return a single hit into '*tx' being the
 * top transaction as ranked by formula 'rank' sorted ascending or
 * descending according to 'sortdir'.
 *
 ***********************************************************************
 */
static int __backlog_query( vgx_TransactionalConsumerService_t *consumer_service, vgx_ExpressEvalMemory_t *memory, const char *filter, const char *rank, vgx_sortspec_t sortdir, objectid_t *tx ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;

  CString_t *CSTR__error = NULL;

  vgx_NeighborhoodQuery_t *query = NULL;
  vgx_ArcConditionSet_t *arc_condition_set = NULL;
  vgx_RankingCondition_t *ranking_condition = NULL;
  vgx_SearchResult_t *search_result = NULL;
  XTRY {
    // Neighborhood( 
    //    id      = "sys_::TX_ROOT",
    //    hits    = 1,
    //    offset  = 0,
    //    result  = R_STR,
    //    fields  = F_ID,
    //    timeout = 5000,
    //    arc     = ("tx_ts", D_IN, M_INT),
    //    memory  = ...,
    //    filter  = ...,
    //    rank    = ...,
    //    sortby  = S_RANK | sortdir
    // )

    // Create query object
    if( (query = iGraphQuery.NewNeighborhoodQuery( SYSTEM, SYS_TX_ROOT, NULL, VGX_COLLECTOR_MODE_COLLECT_ARCS, &CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // hits = 1
    query->hits = 1;
    
    // offset = 0
    query->offset = 0;

    // result = R_STR
    // fields = F_ID
    vgx_ResponseAttrFastMask response_format = VGX_RESPONSE_SHOW_AS_STRING | VGX_RESPONSE_ATTR_ID;
    CALLABLE( query )->SetResponseFormat( query, response_format );
    
    // timeout = 5000
    CALLABLE( query )->SetTimeout( query, 5000, false );

    // arc = ("tx_ts", D_IN, M_INT)
    if( (arc_condition_set = iArcConditionSet.NewSimple( SYSTEM, VGX_ARCDIR_IN, true, TX_ARC_timestamp, VGX_PREDICATOR_MOD_INTEGER, VGX_VALUE_ANY, NULL, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    // Assign arc condition to query (steal)
    CALLABLE( query )->AddArcConditionSet( query, &arc_condition_set );

    // memory = ...
    CALLABLE( query )->SetMemory( query, memory );
    
    // filter = "..."
    CALLABLE( query )->AddFilter( query, filter );

    // rank = "..."
    // sortby = S_RANK | sortdir
    vgx_sortspec_t sortby = VGX_SORTBY_RANKING | sortdir;
    if( (ranking_condition = iRankingCondition.New( SYSTEM, rank, sortby, VGX_PREDICATOR_MOD_NONE, NULL, NULL, 0, &CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }
    // Assign ranking condition to query (steal)
    CALLABLE( query )->AddRankingCondition( query, &ranking_condition );

    // ------------------
    // Execute query
    // ------------------
    if( CALLABLE( SYSTEM )->simple->Neighborhood( SYSTEM, query ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Steal search result from query
    if( (search_result = CALLABLE( query )->YankSearchResult( query )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    // We have a result
    if( search_result->list_length > 0 ) {
      // Assert string result
      if( search_result->list_width != 1 || vgx_response_show_as( search_result->list_fields.fastmask ) != VGX_RESPONSE_SHOW_AS_STRING ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }

      // This is the earliest transaction in the backlog
      const char *name = CStringValue( search_result->list[0].value.CSTR__str );
      if( name ) {
        *tx = strtoid( name );
      }
      
      ret = 1;
    }

  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
      CONSUMER_SERVICE_REASON( consumer_service, 0x007, "%s", CStringValue( CSTR__error ) );
    }
    if( query && query->CSTR__error ) {
      CONSUMER_SERVICE_REASON( consumer_service, 0x008, "%s", CStringValue( query->CSTR__error ) );
    }
    ret = -1;
  }
  XFINALLY {
    iGraphResponse.DeleteSearchResult( &search_result );
    iRankingCondition.Delete( &ranking_condition );
    iArcConditionSet.Delete( &arc_condition_set );
    iGraphQuery.DeleteNeighborhoodQuery( &query );
    iString.Discard( &CSTR__error );
  }

  return ret;
}



/*******************************************************************//**
 *
 * Get the transaction ID just before the last durable transaction.
 * If discardable backlog exists, place
 *   - result into tx_head
 *   - the number of discardable transactions into n_tx_discardable
 *   - total size of discardable backlog bytes into sz_discardable
 *
 * The last discardable transaction is the transaction with the highest
 * serial number less than durable_sn.
 * 
 * Returns  1 : discardable backlog exists and result parameters are populated
 *          0 : no discardable backlog
 *         -1 : error
 *
 ***********************************************************************
 */
static int __get_discardable_backlog( vgx_TransactionalConsumerService_t *consumer_service, int64_t durable_sn, objectid_t *tx_head, int64_t *n_tx_discardable, int64_t *sz_discardable ) {
  int ret = 0;

  // Evaluator memory
  vgx_ExpressEvalMemory_t *memory = NULL;
    
  XTRY {
    
    if( (memory = iEvaluator.NewMemory( -1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    // Serial number must be less than durable_sn, and if so the discardable
    // transaction is counted in R1 and total size in bytes accumulated into R2.
    char filter[192];
    snprintf( filter, 191, "match = .property('%s') < %lld; returnif( !match, false ); inc( R1 ); add( R2, .property('%s') ); true", 
                                               TX_INT_PROP_serial_number, durable_sn, TX_INT_PROP_transaction_size );


    // Rank by serial number, descending. (Return tx with highest serial number less than durable_sn.)
    char rank[32];
    snprintf( rank, 31, ".property('%s')", TX_INT_PROP_serial_number );
    vgx_sortspec_t sortdir = VGX_SORT_DIRECTION_DESCENDING;

    // Execute query
    if( (ret = __backlog_query( consumer_service, memory, filter, rank, sortdir, tx_head )) > 0 ) {
      // Number of backlog transactions have been counted into R1
      // Total backlog size has been accumulated into register R2
      vgx_EvalStackItem_t R1 = memory->data[ EXPRESS_EVAL_MEM_REGISTER_R1 & memory->mask ];
      vgx_EvalStackItem_t R2 = memory->data[ EXPRESS_EVAL_MEM_REGISTER_R2 & memory->mask ];
      if( n_tx_discardable ) {
        *n_tx_discardable = R1.integer;
      }
      if( sz_discardable ) {
        *sz_discardable = R2.integer;
      }
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iEvaluator.DiscardMemory( &memory );
  }

  return ret;
}



/*******************************************************************//**
 *
 * Find the next transaction in txfile and return
 *   1 if next transaction was found
 *   0 if no more transactions in file
 *  -1 if bad transaction found
 *
 * When 1 is returned, the return variables rtx and roffset are populated.
 * 
 ***********************************************************************
 */
static int __find_next_transaction( FILE *txfile, vgx_OperationTransaction_t *rtx, int64_t *roffset ) {
  // TRANSACTION 8d17d82f4d21d0f5ed8e0554f37aae96 0006000FCF28C5D6 0006000FCCEAE3C6\n
  // ...
  // COMMIT f0863bfb838b30363d7844fdbc943877 00000181C60995E2 A09D1D50\n

  char buffer[128];

  memset( rtx, 0, sizeof( vgx_OperationTransaction_t ) );

  int64_t master_sn = 0;
  int parsed;
  int64_t offset;
  int64_t sz_line;
  int64_t tx_offset = 0;
  objectid_t commit_id = {0};

  while( (offset = CX_FTELL( txfile )) >= 0 && (sz_line = file_readline( txfile, buffer, 128 )) > 0 ) {
    // TRANSACTION
    if( (parsed = __parse_request_TRANSACTION( buffer, rtx, &master_sn )) != 0 ) {
      if( parsed < 0 ) {
        goto bad_data;
      }
      // Skip over any leading whitespace to get the exact offset of the leading 'T'
      const char *cp = buffer;
      __skip_spaces( cp );
      int64_t skip = cp - buffer;
      // Record the transaction offset
      tx_offset = offset + skip;
      // Size of the initial line
      rtx->tsize = sz_line - skip;
    }
    // COMMIT
    else if( (parsed = __parse_request_COMMIT( buffer, &commit_id, &rtx->tms, (DWORD*)&rtx->crc )) != 0 ) {
      if( parsed < 0 || !idmatch( &rtx->id, &commit_id ) ) {
        goto bad_data;
      }
      // Add size of commit line
      rtx->tsize += sz_line;
      // Finalize and return
      *roffset = tx_offset;
      //
      // Transaction complete
      //
      return 1;
    }
    // Any other data within a transaction
    else if( rtx->serial > 0 ) {
      // Add size of data within transaction
      rtx->tsize += sz_line;
    }
  }

  // No more transactions in file
  return 0;

bad_data:
  return -1;
}



/*******************************************************************//**
 *
 * 
 *
 ***********************************************************************
 */
static int __restore_tx_log_proxy( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error ) {
  int ret = 0;

  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Vertex_t *TX_log = NULL;

  XTRY {
    char fpath[MAX_PATH+1] = {0};
    const char *tx_location = CStringValue( consumer_service->CSTR__tx_location );
    const char *tx_next_fname = CStringValue( consumer_service->CSTR__tx_fname );
    snprintf( fpath, MAX_PATH, "%s/%s", tx_location, tx_next_fname );

    // Create proxy vertex representing transaction log
    if( (TX_log = Simple->NewVertex( SYSTEM, tx_next_fname, TX_TYPE_proxy, TRANSACTION_LOG_MAX_LIFESPAN_SECONDS, 60000, &reason, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Set the ondelete event, which will remove the transaction log from disk once the proxy vertex expires
    CALLABLE( TX_log )->SetOnDelete( TX_log, VertexOnDeleteAction_RemoveFile, fpath );

    // Store the current proxy name in the system graph
    if( iSystem.Property.SetString( SYS_STR_PROP_current_proxy, tx_next_fname ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( TX_log ) {
      Simple->CloseVertex( SYSTEM, &TX_log );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 * 
 *
 ***********************************************************************
 */
static int __virtualize_tx_log_proxy( vgx_TransactionalConsumerService_t *consumer_service, CString_t **CSTR__error ) {
  int ret = 0;

  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_IGraphSimple_t *Simple = CALLABLE( SYSTEM )->simple;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Vertex_t *TX_log = NULL;

  XTRY {
    if( consumer_service->CSTR__tx_fname ) {
      if( (TX_log = Simple->OpenVertex( SYSTEM, consumer_service->CSTR__tx_fname, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, 30000, &reason, CSTR__error )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Convert to virtual (or delete if zero inbound transactions)
      if( Simple->DeleteVertex( SYSTEM, consumer_service->CSTR__tx_fname, 30000, &reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( TX_log ) {
      Simple->CloseVertex( SYSTEM, &TX_log );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 * 
 *
 ***********************************************************************
 */
static int __restore_backlog( vgx_TransactionalConsumerService_t *consumer_service, objectid_t *durable_tx, int64_t durable_sn ) {

  if( consumer_service->CSTR__tx_location == NULL ) {
    return -1;
  }

  CString_t *CSTR__error = NULL;
  const char *tx_location = CStringValue( consumer_service->CSTR__tx_location );
  vgx_StringList_t *txlist = NULL;
  int64_t n_tx_files = 0;
  char fpath[MAX_PATH+1] = {0};
  FILE *txfile = NULL;

  int64_t total_tsize = 0;

  XTRY {

    // Initialize
    idcpy( &consumer_service->last_txid_executed, durable_tx );
    
    // Find all .tx files in transaction location
    if( iString.Utility.ListDir( tx_location, "*" TX_EXT, &txlist ) < 0 ) {
      __format_error_string( &CSTR__error, "No such directory: %s", tx_location );
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
    }

    // Sort .tx files ascending
    iString.List.Sort( txlist, true );

    // Process each file
    n_tx_files = iString.List.Size( txlist );
    for( int i=0; i<n_tx_files; i++ ) {
      // Reset file's initial serial
      consumer_service->tx_out_sn0 = 0;
      // Open file
      consumer_service->CSTR__tx_fname = iString.List.GetItem( txlist, i );
      const char *name = CStringValue( consumer_service->CSTR__tx_fname );
      snprintf( fpath, MAX_PATH, "%s/%s", tx_location, name );

      SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED
      if( (txfile = CX_FOPEN( fpath, "rb" )) == NULL ) {
        __format_error_string( &CSTR__error, "Could not open file: %s", fpath );
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
      }

      if( __restore_tx_log_proxy( consumer_service, &CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }

      // Reconstruct system graph's backlog from file
      //
      int64_t durability_point_offset = -1;
      vgx_OperationTransaction_t *ptx = &consumer_service->transaction;
      int64_t tx_offset;
      int rnext;

      // First transaction in file. Record initial serial and timestamp
      if( __find_next_transaction( txfile, ptx, &tx_offset ) > 0 ) {
        consumer_service->tx_out_sn0 = ptx->serial;
        consumer_service->tx_out_t0 = ptx->tms;
        // Reset to beginning of file and process all transactions in file
        CX_FSEEK( txfile, 0, SEEK_SET );
        while( (rnext = __find_next_transaction( txfile, ptx, &tx_offset )) > 0 ) {
          if( ptx->serial >= durable_sn ) {
            if( ptx->serial == durable_sn ) {
              durability_point_offset = tx_offset + ptx->tsize;
            }
            else {
              total_tsize += ptx->tsize;
            }
            // Index
            if( __index_tx( consumer_service, tx_offset, &CSTR__error ) < 0 ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
            }
          }
        }
      }

      CX_FCLOSE( txfile );
      txfile = NULL;

      if( __virtualize_tx_log_proxy( consumer_service, &CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }

      if( durability_point_offset >= 0 ) {
        INFO( 0x000, "TX backlog: %s  Total bytes: %lld  Durability point at offset: %lld", name, total_tsize, durability_point_offset );
      }
      else if( total_tsize > 0 ) {
        INFO( 0x000, "TX backlog: %s  Total bytes: %lld", name, total_tsize );
      }
      else {
        INFO( 0x000, "TX backlog: %s  (ignored)", name );
      }

    }
  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
    }
  }
  XFINALLY {
    if( txfile ) {
      CX_FCLOSE( txfile );
    }
    iString.List.Discard( &txlist );
    iString.Discard( &CSTR__error );
    consumer_service->CSTR__tx_fname = NULL;
  }
  return 0;

}



/*******************************************************************//**
 *
 * Get the earliest transaction ID. 
 * If backlog exists, place 
 *   - result into tx_tail
 *   - the number of backlog transactions into n_tx_backlog
 *   - total size of backlog bytes into sz_backlog
 *
 * The earliest transaction in the backlog is the transaction immediately
 * following the one with serial number durable_sn.
 * 
 * Returns  1 : backlog exists and result parameters are populated
 *          0 : backlog is empty
 *         -1 : error
 *
 ***********************************************************************
 */
static int __get_backlog_tail( vgx_TransactionalConsumerService_t *consumer_service, int64_t durable_sn, objectid_t *tx_tail, int64_t *n_tx_backlog, int64_t *sz_backlog ) {
  int ret = 0;

  // Evaluator memory
  vgx_ExpressEvalMemory_t *memory = NULL;
    
  XTRY {
    if( (memory = iEvaluator.NewMemory( -1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Serial number must be greater than durable_sn, and if so the transaction
    // is counted in R1 and total size in bytes accumulated into R2.
    char filter[192];
    snprintf( filter, 191, "match = .property('%s') > %lld; returnif( !match, false ); inc( R1 ); add( R2, .property('%s') ); true",
                                               TX_INT_PROP_serial_number, durable_sn, TX_INT_PROP_transaction_size );

    // Rank by serial number, ascending. (Return tx with lowest serial number greater than durable_sn.)
    char rank[32];
    snprintf( rank, 31, ".property('%s')", TX_INT_PROP_serial_number );
    vgx_sortspec_t sortdir = VGX_SORT_DIRECTION_ASCENDING;

    // Execute query
    if( (ret = __backlog_query( consumer_service, memory, filter, rank, sortdir, tx_tail )) > 0 ) {
      // Number of backlog transactions have been counted into R1
      // Total backlog size has been accumulated into register R2
      vgx_EvalStackItem_t R1 = memory->data[ EXPRESS_EVAL_MEM_REGISTER_R1 & memory->mask ];
      vgx_EvalStackItem_t R2 = memory->data[ EXPRESS_EVAL_MEM_REGISTER_R2 & memory->mask ];
      if( n_tx_backlog ) {
        *n_tx_backlog = R1.integer;
      }
      if( sz_backlog ) {
        *sz_backlog = R2.integer;
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iEvaluator.DiscardMemory( &memory );
  }

  return ret;
}



/*******************************************************************//**
 *
 * Compute total size of transaction backlog
 * 
 * Returns: > 0: Total number of bytes in backlog
 *            0: No backlog
 *           -1: Error
 *
 ***********************************************************************
 */
static int64_t __get_tx_backlog( vgx_TransactionalConsumerService_t *consumer_service, objectid_t *tx_start, int64_t *sn_start, int64_t *n_tx ) {
  int64_t sz_backlog = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;

  /*
                                [___________BACKLOG___________]
    tx0 -> tx1 -> tx2 -> tx3 -> tx4 -> tx5 -> tx6 -> tx8 -> tx9
     ^                    ^
     |                    |
   tx_tail          durability_point


    [_________________________BACKLOG_________________________]
    tx0 -> tx1 -> tx2 -> tx3 -> tx4 -> tx5 -> tx6 -> tx8 -> tx9
     ^
     |
   tx_tail
               ?
               |
        durability_point


    NOTE: Durability point is the earliest durable transaction of
          all the graphs in the registry. The backlog starts at
          the first transaction after the durability point.
          If the durability point transaction does not exist
          it means the backlog starts at tx0.

  */

  // Get durability point
  objectid_t durable_tx = {0};
  int64_t durable_sn = 0;
  igraphfactory.DurabilityPoint( &durable_tx, &durable_sn, NULL, NULL );


  __restore_backlog( consumer_service, &durable_tx, durable_sn );


  // Get tx_tail and total backlog size
  objectid_t tx_tail = {0};
  int has_backlog = __get_backlog_tail( consumer_service, durable_sn, &tx_tail, n_tx, &sz_backlog );
  if( has_backlog < 0 ) {
    return -1; // error
  }
  else if( has_backlog == 0 ) {
    return 0;
  }
  
  vgx_Vertex_t *TX = NULL;
  int64_t n_chain_bytes = 0;

  XTRY {
    // Traverse backlog to verify chain intact
    objectid_t __tx;
    objectid_t *cursor = &__tx;
    idcpy( cursor, &tx_tail );
    
    int64_t tx_sz = 0;
    int64_t sn_tail = -1;

    do {

      // Open transaction
      vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
      if( (TX = CALLABLE( SYSTEM )->advanced->AcquireVertexReadonly( SYSTEM, cursor, 5000, &reason )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Capture tail into return args
      if( sn_tail < 0 ) {
        if( CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_serial_number, &sn_tail ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
        *tx_start = tx_tail;
        *sn_start = sn_tail;
      }

      // Get the transaction size
      if( CALLABLE( TX )->GetIntProperty( TX, TX_INT_PROP_transaction_size, &tx_sz ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
      }
      n_chain_bytes += tx_sz;

      // We have next
      objectid_t next = {0};
      if( __next_tx( consumer_service, cursor, &next ) > 0 ) {
        // Sanity check
        if( idmatch( cursor, &next ) ) {
          char idbuf[33];
          CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "Self-reference transaction tx=%s", idtostr( idbuf, &next ) );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }
        // Advance
        idcpy( cursor, &next );
      }
      // End reached
      else {
        idunset( cursor );
      }

      CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX );

    } while( !idnone( cursor ) );
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( TX ) {
      CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX );
    }
  }

  // Error
  if( sz_backlog != n_chain_bytes ) {
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0x006, "Inconsistent transaction backlog: sz=%lld, chain=%lld", sz_backlog, n_chain_bytes );
    return -1;
  }

  // Return the bytes found in the actual chain since this is
  // what we will use when executing the backlog.
  return n_chain_bytes;
}



/*******************************************************************//**
 *
 * Submit transaction backlog to parser for execution.
 *
 * Up to tx_limit transactions will be processed. If not all transactions
 * were processed because the limit was reached, 1 is returned. If no
 * transactions remain, 0 is returned.
 * 
 * Returns 1 : Transactions remain to be executed
 *         0 : All transactions executed
 *        -1 : Error
 *
 ***********************************************************************
 */
static int __execute_tx_backlog( vgx_TransactionalConsumerService_t *consumer_service, int64_t tx_limit_bytes ) {
  int hasnext = 0;

  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_OperationParser_t *parser = &SYSTEM->OP.parser;

  CString_t *CSTR__error = NULL;

  char idbuf[33];

  vgx_ByteArray_t entry = {
    .len = 0,
    .data = NULL
  };
  vgx_ByteArrayList_t backlog = {
    .sz = 1,
    .entries = &entry
  };

  objectid_t __tx = {0};
  objectid_t *txid = &__tx;

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  // Last executed transaction is known. We will start execution at the next transaction.
  if( !idnone( &consumer_service->last_txid_executed ) ) {
    // Make sure last executed transaction still exists and remains in the chain until
    // we can grab the next transaction from it.
    vgx_Vertex_t *TX_last_exe;
    if( (TX_last_exe = CALLABLE( SYSTEM )->advanced->AcquireVertexReadonly( SYSTEM, &consumer_service->last_txid_executed, 60000, &reason )) == NULL ) {
      // ...if not we need to perform a query to find the next transaction,
      // which will be the earliest transaction in the chain.
      if( __is_access_reason_noexist( reason ) ) {
        // Run query to find earliest transaction in chain, which should be the next
        // one in line for processing since the last executed transaction had been
        // removed from the chain.
        __get_backlog_tail( consumer_service, 0, txid, NULL, NULL );
      }
      else {
        CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE01, "Failed to retrieve last executed tx=%s, reason=%03X", idtostr( idbuf, &consumer_service->last_txid_executed ), reason );
      }
    }
    else {
      __next_tx( consumer_service, &consumer_service->last_txid_executed, txid );
      CALLABLE( SYSTEM )->simple->CloseVertex( SYSTEM, &TX_last_exe );
    }
  }
  // No known last transaction executed. We will set cursor to initial transaction
  else {
    idcpy( txid, &consumer_service->initial_txid );
  }

  int64_t sz_processed = 0;
  //vgx_OperationTransaction_t *tx = &consumer_service->transaction;
  vgx_OperationTransaction_t transaction = {0};
  vgx_OperationTransaction_t *tx = &transaction;

  // Cursor now points to the first transaction we will process.
  iOpBuffer.Clear( consumer_service->tx_in_buffer );
  while( !idnone( txid ) ) {
    int64_t tx_sz = __load_tx( consumer_service, txid, consumer_service->tx_in_buffer );
    if( tx_sz <= 0 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE02, "Failed to load transaction tx=%s", idtostr( idbuf, txid ) );
      hasnext = -1;
      break;
    }

    // Get the segment containing transaction data
    if( (entry.len = iOpBuffer.ReadableSegment( consumer_service->tx_in_buffer, LLONG_MAX, (const char**)&entry.data, NULL )) != tx_sz ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE03, "Internal buffer error: segment size=%lld, expected=%lld", entry.len, tx_sz );
      hasnext = -1;
      break;
    }

    // Verify start of transaction
    int64_t master_serial = 0;
    if( __parse_request_TRANSACTION( (const char*)entry.data, tx, &master_serial ) < 1 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE04, "Invalid transaction data for tx=%s", idtostr( idbuf, txid ) );
      hasnext = -1;
      break;
    }

    // Submit batch
    // Keep trying as long as temporary failure to submit
    int64_t n;
    while( (n = iOperation.Parser.SubmitData( parser, &backlog, &CSTR__error )) == 0 ) {
      iString.Discard( &CSTR__error );
      if( COMLIB_TASK__IsRequested_ForceExit( consumer_service->TASK ) ) {
        CSTR__error = CStringNew( "Forced exit while parser is unable to absorb data" );
        n = -1;
        break;
      }
    }

    // Parser error
    if( n < 0 ) {
      const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE04, "Transaction parser permanent error while processing backlog: %s", msg );
      iString.Discard( &CSTR__error );
      hasnext = -1;
      break;
    }
    
    // Mark segment as consumed and check
    iOpBuffer.AdvanceRead( consumer_service->tx_in_buffer, entry.len );
    if( iOpBuffer.Readable( consumer_service->tx_in_buffer ) > 0 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE05, "Internal buffer error while processing transaction backlog" );
      hasnext = -1;
      break;
    }

    // Record last transaction executed
    idcpy( &consumer_service->last_txid_executed, txid );

    // Decrease backlog bytes by amount just fed to parser
    consumer_service->tx_backlog_bytes -= entry.len;

    // Accumulate total bytes processed
    sz_processed += entry.len;

    consumer_service->provider.master_serial = master_serial;

    // Next transaction
    objectid_t next;
    if( (hasnext = __next_tx( consumer_service, txid, &next )) > 0 ) {
      // Stop if over limit
      if( sz_processed >= tx_limit_bytes ) {
        break;
      }
      // Advance
      *txid = next;
      // Make sure buffer write/read linear segments only
      iOpBuffer.Clear( consumer_service->tx_in_buffer );
    }
    // End of backlog or error
    else {
      idunset( txid );
      break;
    }
  }

  // Close input
  __close_tx_input( consumer_service );

  // Error
  if( hasnext < 0 ) {
    const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "?";
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0xE06, "Unable to process transaction backlog: %s", msg );
    iString.Discard( &CSTR__error );
    return -1;
  }

  // After confirmed commit we transfer master serial to SYSTEM
  GRAPH_LOCK( consumer_service->sysgraph ) {
    consumer_service->sysgraph->sysmaster_tx_serial = consumer_service->provider.master_serial;
  } GRAPH_RELEASE;

  CONSUMER_SERVICE_INFO( consumer_service, 0x002, "Backlog applied (%lld bytes remain)", consumer_service->tx_backlog_bytes );

  return hasnext;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double __refresh_tx_input_rate( vgx_TransactionalConsumerService_t *consumer_service ) {
  double rate = __refresh_data_rate( consumer_service->tx_lifetime_bytes );
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      SYSTEM->OP.system.running_tx_input_rate_CS = (float)rate;
    } GRAPH_RELEASE;
  }
  return rate;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __process_transaction_backlog( vgx_TransactionalConsumerService_t *consumer_service, vgx_OperationParser_t *parser ) {
  int err = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  // Get size of backlog and initial transaction and serial number
  objectid_t tx_start = {0};
  int64_t sn_start = 0;
  int64_t n_tx = 0;
  int64_t sz_backlog = __get_tx_backlog( consumer_service, &tx_start, &sn_start, &n_tx );

  // Now process backlog
  if( sz_backlog > 0 ) {
    char idbuf[33];
    CONSUMER_SERVICE_INFO( consumer_service, 0xC01, "Applying transaction backlog @ tx=%s sn=%016llX count=%lld bytes=%lld", idtostr(idbuf, &tx_start), sn_start, n_tx, sz_backlog );

    // Some transactions may already be applied since multiple graphs may exist and have different durability points.
    iOperation.Parser.SkipRegression( parser, true );

    // Initialize the backlog size
    consumer_service->tx_backlog_bytes = sz_backlog;
    GRAPH_LOCK( SYSTEM ) {  
      consumer_service->tx_log_total_sz_SYS_CS = sz_backlog;
    } GRAPH_RELEASE;

    // Set initial transaction to the beginning of the backlog
    consumer_service->initial_txid = tx_start;

    // Execute entire backlog
    if( __execute_tx_backlog( consumer_service, sz_backlog ) != 0 || consumer_service->tx_backlog_bytes != 0 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0xC02, "Transaction backlog consumption incomplete, %lld bytes remaining", consumer_service->tx_backlog_bytes );
      err = -1;
    }

    // Catch up and be ready for new transactions
    idcpy( &consumer_service->last_txid_commit, &consumer_service->last_txid_executed );

    // Wait for operation parser to complete processing of all transactions
    sleep_milliseconds( 1000 );
    int64_t n_pending;
    while( (n_pending = iOperation.Parser.Pending( parser )) > 0 ) {
      CONSUMER_SERVICE_INFO( consumer_service, 0xC03, "Waiting for transaction backlog processing to complete (%lld bytes remaining)", n_pending );
      sleep_milliseconds( 3000 );
    }

    // Check if error occurred earlier
    if( err == 0 ) {
      CONSUMER_SERVICE_INFO( consumer_service, 0xC04, "Transaction backlog successfully applied" );
      consumer_service->tx_state.local.has_backlog = false;
    }
    
    // Resume normal regression reporting
    iOperation.Parser.SkipRegression( parser, false );

  }
  // Empty backlog
  else if( sz_backlog == 0 ) {
    CONSUMER_SERVICE_INFO( consumer_service, 0xC05, "Transaction backlog is empty" );
  }
  // Failed to get size of backlog
  else {
    CONSUMER_SERVICE_REASON( consumer_service, 0xC06, "Transaction backlog error" );
  }

  // Post-restore cleanup
  __discard_to_durability_point( consumer_service );


}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __process_state_requests_TCS( comlib_task_t *self, vgx_TransactionalConsumerService_t *consumer_service ) {

  // Check if disk cleanup is requested
  if( CONSUMER_SERVICE_IS_DISK_CLEANUP_REQUESTED_TCS( consumer_service ) ) {
    COMLIB_TASK_SUSPEND_LOCK( self ) {
      __discard_to_durability_point( consumer_service );
    } COMLIB_TASK_RESUME_LOCK;
    CONSUMER_SERVICE_CLEAR_DISK_CLEANUP_REQUEST_TCS( consumer_service );
  }

  // Check for detach request
  if( CONSUMER_SERVICE_IS_DETACH_REQUESTED_TCS( consumer_service ) ) {
    if( consumer_service->TransactionProducerClient ) {
      if( consumer_service->TransactionProducerClient ) {
        __produce_response_DETACH( consumer_service );
      }
    }
    CONSUMER_SERVICE_CLEAR_DETACH_REQUEST_TCS( consumer_service );
  }

  // Check for transaction execution state change from running to suspended
  if( CONSUMER_SERVICE_IS_SUSPEND_TX_EXECUTION_REQUESTED_TCS( consumer_service ) ) {
    __suspend_tx_execution_TCS( self, consumer_service );
  }

  // Check for transaction execution resume
  if( CONSUMER_SERVICE_IS_RESUME_TX_EXECUTION_REQUESTED_TCS( consumer_service ) ) {
    __resume_tx_execution_TCS( consumer_service );
    // Speculate that backlog now exists on disk if transaction log is enabled
    if( __txlog_enabled( consumer_service ) ) {
      consumer_service->tx_state.local.has_backlog = true;
    }
    if( consumer_service->TransactionProducerClient ) {
      __produce_response_RESUME( consumer_service );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __handle_client_exception( vgx_TransactionalConsumerService_t *consumer_service, struct pollfd *pclient ) {
  const char *client = consumer_service->TransactionProducerClient ? iURI.URI( consumer_service->TransactionProducerClient ) : NULL;
  int provider_detached = false;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      if( consumer_service->provider_attached_SYS_CS == false ) {
        provider_detached = true;
      }
    } GRAPH_RELEASE;
  }

  if( provider_detached ) {
    CONSUMER_SERVICE_INFO( consumer_service, 0xC07, "Detached: VGX Subscription Provider @ %s", client );
  }
  else {
    const char *reason;
    switch( pclient->revents ) {
    case POLLERR:
      reason = "socket error";
      break;
    case POLLHUP:
      reason = "socket disconnect";
      break;
    case POLLNVAL:
      reason = "invalid socket";
      break;
    default:
      reason = "?";
      break;
    }
    CONSUMER_SERVICE_REASON( consumer_service, 0xC08, "Client socket exception '%s' for client %s", reason, client ? client : "?" );
  }
  __operation_consumer__client_close( consumer_service );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __handle_client_event( vgx_TransactionalConsumerService_t *consumer_service ) {
  int ret = 0;
  struct pollfd *plisten = &consumer_service->poll_fd[0];
  struct pollfd *pclient = &consumer_service->poll_fd[1];

  // New connection request
  if( cxpollfd_readable( plisten ) ) {
    if( __operation_consumer__try_accept( consumer_service ) < 0 ) {
      ret = -1;
    }
    __produce_response_RESUME( consumer_service );
  }
  // No new connections, handle existing connections
  else {
    // Client socket exists and has events
    if( cxpollfd_valid( pclient ) ) {
      // Client socket is writable
      if( cxpollfd_writable( pclient ) ) {
        if( __operation_consumer__client_send( consumer_service ) < 0 ) {
          ret = -1;
        }
      }

      // Client socket is readable
      if( cxpollfd_readable( pclient ) ) {
        if( __operation_consumer__client_recv( consumer_service ) < 0 ) {
          ret = -1;
        }

        // Compute data receive rate
        __refresh_tx_input_rate( consumer_service );
      }

      // Client socket has exception
      if( cxpollfd_exception( pclient ) ) {
        __handle_client_exception( consumer_service, pclient );
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
BEGIN_COMLIB_TASK( self, 
                   vgx_TransactionalConsumerService_t,
                   consumer_service,
                   __operation_consumer_service_monitor,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "operation_consumer_service_monitor/" ) 
{
  SET_CURRENT_THREAD_LABEL( "vgx_opservice" );
  
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  vgx_OperationParser_t *parser = &SYSTEM->OP.parser;
  iOperation.Parser.EnableExecution( parser, true );
  
  const char *graph_name = CStringValue( CALLABLE( SYSTEM )->Name( SYSTEM ) );

  APPEND_THREAD_NAME( graph_name );
  COMLIB_TASK__AppendDescription( self, graph_name );

  // ------------------------
  // APPLY BACKLOG ON STARTUP
  if( __txlog_enabled( consumer_service ) ) {
    __process_transaction_backlog( consumer_service, parser );
  }
  // ------------------------

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );

  bool suspend_request = false;
  bool suspended = false;

  int64_t last_resume_tms = 0;

  int nfds = 0;

  COMLIB_TASK_LOCK( self ) {
    consumer_service->tx_state.public_TCS.initializing = false;
  } COMLIB_TASK_RELEASE;


  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
    loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
    COMLIB_TASK_LOCK( self ) {
      // Idle
      COMLIB_TASK__ClearState_Busy( self );

      vgx_URI_t *Listen = consumer_service->Listen;
      CXSOCKET *pserver_sock = Listen ? iURI.Sock.Input.Get( Listen ) : NULL;

      // Check various requests
      __process_state_requests_TCS( self, consumer_service );

      // Check for server state change
      __check_request__stop_suspend_resume_TCS( consumer_service, &suspend_request, &suspended );

      // Poll loop timeout
      int poll_timeout_ms;
      if( consumer_service->tx_state.local.has_backlog ) {
        poll_timeout_ms = 0; // Non-blocking while transaction backlog remains
      }
      else {
        poll_timeout_ms = 50; // Blocking when all transaction input comes from socket
      }

      if( pserver_sock && !suspended ) {
        COMLIB_TASK__SetState_Busy( self );
        COMLIB_TASK_SUSPEND_LOCK( self ) {

          GRAPH_LOCK( SYSTEM ) {
            consumer_service->snapshot.allowed_SYS_CS = true;
          } GRAPH_RELEASE;

          if( __operation_consumer__prepare_poll( consumer_service ) > 0 ) {

            // Monitor socket events
            nfds = cxpoll( consumer_service->poll_fd, consumer_service->sz_poll_fd, poll_timeout_ms );

            // One or more events on sockets
            if( nfds > 0 ) {
              if( __handle_client_event( consumer_service ) < 0 ) {
                loop_delay = COMLIB_TASK_LOOP_DELAY( 500 );
              }
            }
            // Poll Error
            else if( nfds < 0 ) {
              CONSUMER_SERVICE_REASON( consumer_service, 0xC09, "poll() error" );
              iURI.Sock.Error( Listen, NULL, -1, 0 );
              loop_delay = COMLIB_TASK_LOOP_DELAY( 2000 );
            }
            // Poll Timeout
            else if( consumer_service->TransactionProducerClient ) {
              int64_t tms_now = __GET_CURRENT_MILLISECOND_TICK();
              int64_t last_tx_age = tms_now - consumer_service->last_tx_tms;
              int64_t last_resume_age = tms_now - last_resume_tms;
              // Send RESUME if we are not receiving input
              if( last_tx_age > RESUME_PERIOD_MS && last_resume_age > RESUME_PERIOD_MS ) {
                last_resume_tms = tms_now;
                if( !consumer_service->tx_state.local.exec_suspended ) {
                  __produce_response_RESUME( consumer_service );
                }
              }
              // Send status
              if( tms_now > consumer_service->status_response_deadline_tms ) {
                __produce_response_IDLE( consumer_service, tms_now );
              }
              // Compute data receive rate
              __refresh_tx_input_rate( consumer_service );
            }

            // Backlog exists
            if( consumer_service->tx_state.local.has_backlog ) {
              // Disallow snapshots while backlog exists
              GRAPH_LOCK( SYSTEM ) {
                consumer_service->snapshot.allowed_SYS_CS = false;
              } GRAPH_RELEASE;
              // Execute some of the remaining backlog if execution is not currently suspended
              if( !consumer_service->tx_state.local.exec_suspended ) {
                CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Process partial TX backlog (total %lld bytes)", consumer_service->tx_backlog_bytes );
                if( __execute_tx_backlog( consumer_service, TX_BACKLOG_PROCESS_CHUNK_SIZE ) < 1 ) {
                  // No more backlog (or error) - no longer in backlog mode.
                  // NOTE: If error we have other problems which will likely show up
                  //       elsewhere and we will deal with it there.
                  consumer_service->tx_state.local.has_backlog = false;
                  // Re-allow snapshots when backlog is empty
                  GRAPH_LOCK( SYSTEM ) {
                    consumer_service->snapshot.allowed_SYS_CS = true;
                  } GRAPH_RELEASE;
                  if( consumer_service->tx_backlog_bytes != 0 ) {
                    CONSUMER_SERVICE_CRITICAL( consumer_service, 0xC0A, "Backlog execution data imbalance upon completion: %lld unprocessed bytes", consumer_service->tx_backlog_bytes );
                    consumer_service->tx_backlog_bytes = 0; // force to zero
                  }
                }
              }
            }

          }
        } COMLIB_TASK_RESUME_LOCK;
      }
      else {
        GRAPH_LOCK( SYSTEM ) {
          consumer_service->snapshot.allowed_SYS_CS = false;
        } GRAPH_RELEASE;
      }

      // Server not open, or server suspended, or no client connected
      if( pserver_sock == NULL || suspended || consumer_service->TransactionProducerClient == NULL )  {
        loop_delay = COMLIB_TASK_LOOP_DELAY( 100 );
        GRAPH_LOCK( SYSTEM ) {
          SYSTEM->OP.system.running_tx_input_rate_CS = 0.0;
          SYSTEM->tx_input_lag_ms_CS = 0;
        } GRAPH_RELEASE;
      }

      if( !suspended ) {
        // Check for suspend request
        __suspend_if_requested( consumer_service, &suspend_request, &suspended );
      }
      
    } COMLIB_TASK_RELEASE;
  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 * Returns 1 : Execution suspended confirmed
 *         0 : Nothing is running
 *        -1 : Failed to suspend (timeout)
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__suspend_tx_execution_OPEN( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_OperationParser_t *parser = &SYSTEM->OP.parser;
    bool require_parser_flush;
    // Do not require parser flush if current thread is the parser thread (i.e. parser itself suspends consumer service)
    if( parser->TASK && COMLIB_TASK__ThreadId( parser->TASK ) == GET_CURRENT_THREAD_ID() ) {
      require_parser_flush = false;
    }
    // Threads other than parser must require parser flush before suspend is confirmed
    else {
      require_parser_flush = true;
    }
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t deadline = t0 + timeout_ms;
        bool defunct = false;
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            // Request suspension of tx execution
            CONSUMER_SERVICE_REQUEST_SUSPEND_TX_EXECUTION_TCS( consumer_service );
            if( require_parser_flush ) {
              CONSUMER_SERVICE_SET_FLUSH_PARSER_TCS( consumer_service );
            }
            // Wait until request has been cleared by main loop
            while( consumer_service->TASK && CONSUMER_SERVICE_IS_SUSPEND_TX_EXECUTION_REQUESTED_TCS( consumer_service ) && __GET_CURRENT_MILLISECOND_TICK() < deadline ) {
              if( (defunct = consumer_service->control.flag_TCS.defunct) == true ) {
                break;
              }
              COMLIB_TASK_WAIT_FOR_WAKE( consumer_service->TASK, 10 );
              if( consumer_service->TASK == NULL || COMLIB_TASK__IsDead( consumer_service->TASK ) ) {
                break;
              }
            }
            if( defunct || consumer_service->TASK == NULL ) {
              ret = -1;
            }
            else {
              // Confirm suspended
              if( consumer_service->tx_state.public_TCS.suspended == true ) {
                ret = 1;
              }
              // Not suspended, timeout
              else {
                ret = -1;
                CONSUMER_SERVICE_CLEAR_TX_EXECUTION_SUSPEND_REQUEST_TCS( consumer_service );
                if( require_parser_flush ) {
                  CONSUMER_SERVICE_CLEAR_FLUSH_PARSER_TCS( consumer_service );
                }
              }
            }
          } COMLIB_TASK_RELEASE;
        }
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
DLL_HIDDEN int _vxdurable_operation_consumer_service__is_suspended_tx_execution_OPEN( vgx_Graph_t *SYSTEM ) {
  int suspended = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            suspended = consumer_service->tx_state.public_TCS.suspended;
          } COMLIB_TASK_RELEASE;
        }
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
  return suspended;
}


  
/*******************************************************************//**
 *
 * Returns 1 : Execution resume confirmed
 *         0 : Nothing is running
 *        -1 : Failed to resume (timeout)
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__resume_tx_execution_OPEN( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
        int64_t deadline = t0 + timeout_ms;
        bool defunct = false;
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            // Request resume tx execution
            CONSUMER_SERVICE_REQUEST_RESUME_TX_EXECUTION_TCS( consumer_service );
            // Wait until resume request has been cleared by main loop
            while( consumer_service->TASK && CONSUMER_SERVICE_IS_RESUME_TX_EXECUTION_REQUESTED_TCS( consumer_service ) && __GET_CURRENT_MILLISECOND_TICK() < deadline ) {
              if( (defunct = consumer_service->control.flag_TCS.defunct) == true ) {
                break;
              }
              COMLIB_TASK_WAIT_FOR_WAKE( consumer_service->TASK, 10 );
              if( consumer_service->TASK == NULL || COMLIB_TASK__IsDead( consumer_service->TASK ) ) {
                break;
              }
            }
            if( consumer_service->TASK && !defunct ) {
              // Confirm resumed ok
              if( consumer_service->tx_state.public_TCS.suspended == false ) {
                ret = 1;
              }
              // Not resumed, timeout
              else {
                ret = -1;
                CONSUMER_SERVICE_CLEAR_TX_EXECUTION_RESUME_REQUEST_TCS( consumer_service );
              }
            }
          } COMLIB_TASK_RELEASE;
        }
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
DLL_HIDDEN int _vxdurable_operation_consumer_service__is_initializing_tx_execution_OPEN( vgx_Graph_t *SYSTEM ) {
  int initializing = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            initializing = consumer_service->tx_state.public_TCS.initializing;
          } COMLIB_TASK_RELEASE;
        }
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
  return initializing;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__suspend_OPEN( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  int ret = -1;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            if( COMLIB_TASK__IsAlive( consumer_service->TASK ) ) {
              ret = COMLIB_TASK__Suspend( consumer_service->TASK, NULL, timeout_ms );
            }
          } COMLIB_TASK_RELEASE;
        }
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
DLL_HIDDEN int _vxdurable_operation_consumer_service__is_suspended_OPEN( vgx_Graph_t *SYSTEM ) {
  int suspended = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            if( COMLIB_TASK__IsAlive( consumer_service->TASK ) ) {
              suspended = COMLIB_TASK__IsSuspended( consumer_service->TASK );
            }
          } COMLIB_TASK_RELEASE;
        }
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
  return suspended;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__resume_OPEN( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            if( COMLIB_TASK__IsAlive( consumer_service->TASK ) ) {
              ret = COMLIB_TASK__Resume( consumer_service->TASK, 10000 );
            }
          } COMLIB_TASK_RELEASE;
        }
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
DLL_HIDDEN int _vxdurable_operation_consumer_service__initialize_SYS_CS( vgx_Graph_t *SYSTEM, vgx_TransactionalConsumerService_t *consumer_service, uint16_t port, bool durable, int64_t snapshot_threshold ) {
  int ret = 0;


  memset( consumer_service, 0, sizeof( vgx_TransactionalConsumerService_t ) );

  const CString_t *CSTR__root = igraphfactory.SystemRoot();
  if( CSTR__root == NULL ) {
    return -1;
  }
  char *syspath = get_abspath( CStringValue( CSTR__root ) );
  if( syspath == NULL ) {
    return -1;
  }

  XTRY {

    // [Q1.1]
    consumer_service->TASK = NULL;

    // [Q1.2]
    consumer_service->control._bits = 0;

    // [Q1.3]
    consumer_service->sysgraph = SYSTEM;

    // [Q1.4]
    consumer_service->Listen = NULL;

    // [Q1.5.1.1]
    consumer_service->bind_port = port;

    // [Q1.5.1.2]
    consumer_service->__rsv_1_5_1_2 = 0;

    // We allocate exactly two slots for pollfd:
    // 1. The server listen socket
    // 2. One connected vgx transaction producer
    //
    // [Q1.5.2]
    consumer_service->sz_poll_fd = 2;

    // [Q1.6]
    if( (consumer_service->poll_fd = calloc( consumer_service->sz_poll_fd, sizeof( struct pollfd ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // [Q1.7]
    consumer_service->plisten = &consumer_service->poll_fd[0];
    
    // [Q1.8]
    consumer_service->pclient = &consumer_service->poll_fd[1];

    // ---------------------------

    // [Q2.1]
    consumer_service->TransactionProducerClient = NULL;

    // [Q2.2]
    // Request buffer, initial size 1024
    if( (consumer_service->buffer.request = iOpBuffer.New( 10, "consumer_service_request" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // [Q2.3]
    // Response buffer, initial size 1024
    if( (consumer_service->buffer.response = iOpBuffer.New( 10, "consumer_service_response" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // [Q2.4,5,6,7,8]
    // Make two static allocations:
    //  1. txdata with a maximum number of entries, all initialized to zero/unused.
    //  2. slab of bytes which will may be reference on demand using appropriate offsets
    // The actual size limit for a txdata is determined either by the maximum number of
    // entries being reached or the maximum number of total bytes begin reached,
    // whichever comes first.
    // A transaction with a line exceeding the total size of the slab will have to be
    // handled separately as a special case.
    CALIGNED_ARRAY_THROWS( consumer_service->txdata.entries, vgx_ByteArray_t, TX_MAX_ENTRIES, 0x004 );
    CALIGNED_ARRAY_THROWS( consumer_service->tx_slab, BYTE, TX_MAX_SIZE, 0x005 );
    consumer_service->tx_slab_cursor = consumer_service->tx_slab;
    // Initialize all reference to zero
    consumer_service->txdata.sz = 0;
    for( int64_t i=0; i<consumer_service->txdata.sz; i++ ) {
      consumer_service->txdata.entries[i].len = 0;
      consumer_service->txdata.entries[i].data = NULL;
    }
    // Cursor to start
    consumer_service->tx_cursor = consumer_service->txdata.entries;

    // ---------------------------

    // [Q3.1-8]
    consumer_service->transaction.tms = 0;
    consumer_service->transaction.serial = 0;
    idunset( &consumer_service->transaction.id );
    consumer_service->transaction.crc = 0;
    consumer_service->transaction.tsize = 0;
    // (other transaction members ignored)

    // ---------------------------

    // [Q4.1-2]
    idunset( &consumer_service->resync_tx );

    // [Q4.3.1]
    consumer_service->resync_pending = 0;

    // [Q4.3.2]
    consumer_service->provider_attached_SYS_CS = false;

    // [Q4.4]
    consumer_service->CSTR__subscriber_uri_SYS_CS = NULL;

    // [Q4.5]
    consumer_service->CSTR__provider_uri_SYS_CS = NULL;
    //consumer_service->CSTR__provider_uri_snapshot_TCS = NULL;

    // [Q4.6]
    consumer_service->last_tx_tms = 0;

    // [Q4.7-8]
    idunset( &consumer_service->last_txid_executed );

    // ----- Transaction Persistent Output
    // [Q5.1]
    consumer_service->tx_raw_storage = NULL;
    
    // [Q5.2]
    if( durable ) {
      if( (consumer_service->CSTR__tx_location = CStringNewFormat( "%s/%s", syspath, VGX_PATHDEF_DURABLE_TXLOG )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x006 );
      }
      // Create transaction log directory if necessary
      const char *tx_location = CStringValue( consumer_service->CSTR__tx_location );
      if( !dir_exists( tx_location ) && create_dirs( tx_location ) < 0 ) {
        CONSUMER_SERVICE_REASON( consumer_service, 0x007, "Failed to create transaction storage location: %s", tx_location );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
      }
    }

    // [Q5.3]
    consumer_service->CSTR__tx_fname = NULL;

    // [Q5.4]
    consumer_service->tx_out_sn0 = 0;

    // [Q5.5]
    consumer_service->tx_out_t0 = 0;

    // [Q5.6]
    consumer_service->tx_out_sz = 0;

    // [Q5.7-8]
    idunset( &consumer_service->last_txid_commit );

    // ---------------------------

    // [Q6.1.1]
    consumer_service->tx_state.__bits = 0;

    // [Q6.1.2]
    consumer_service->tx_in_fd = 0;

    // [Q6.2]
    consumer_service->tx_in_sn0 = 0;

    // [Q6.3]
    if( (consumer_service->tx_in_buffer = iOpBuffer.New( 10, "TransactionBacklog" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x009 );
    }

    // [Q6.4]
    consumer_service->__rsv_6_4 = 0;

    // [Q6.5]
    consumer_service->tx_backlog_bytes = 0;

    // [Q6.6]
    consumer_service->tx_lifetime_bytes = 0;

    // [Q6.7-8]
    idunset( &consumer_service->initial_txid );

    // ---------------------------

    // [Q7.1-2]
    idunset( &consumer_service->provider.fingerprint );

    // [Q7.3]
    consumer_service->provider.state_tms = 0;

    // [Q7.4]
    consumer_service->provider.master_serial = 0;

    // [Q7.5]
    consumer_service->status_response_deadline_tms = 0;

    // [Q7.6]
    consumer_service->__rsv_7_6 = 0;
   
    // [Q7.7]
    consumer_service->__rsv_7_7 = 0;

    // [Q7.8]
    consumer_service->__rsv_7_8 = 0;

    // ---------------------------

    // [Q8.1]
    consumer_service->snapshot.TASK = NULL;

    // [Q8.2]
    if( snapshot_threshold < 0 ) {
      snapshot_threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    }
    consumer_service->snapshot.tx_log_threshold_SYS_CS = snapshot_threshold;

    // [Q8.3.1]
    consumer_service->snapshot.running_TCS = false;

    // [Q8.3.2]
    consumer_service->snapshot.allowed_SYS_CS = false;

    // [Q8.4]
    consumer_service->tx_log_total_sz_SYS_CS = 0;

    // [Q8.5]
    consumer_service->tx_log_since_sn_SYS_CS = 0;

    // [Q8.6]
    consumer_service->__rsv_8_6 = 0;

    // [Q8.7]
    consumer_service->__rsv_8_7 = 0;

    // [Q8.8]
    consumer_service->__rsv_8_8 = 0;


    // ===============================
    // START CONSUMER SERVICE THREAD
    // ===============================

    CONSUMER_SERVICE_VERBOSE( consumer_service, 0, "Starting consumer service..." );

    int unstarted = 0;

    // [Q1.1] TASK
    if( (consumer_service->TASK = COMLIB_TASK__New( __operation_consumer_service_monitor, __operation_consumer_service_initialize, __operation_consumer_service_shutdown, consumer_service )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00A );
    }
    ++unstarted;

    if( durable ) {
      // [Q7.6] snapshot_TASK
      if( (consumer_service->snapshot.TASK = COMLIB_TASK__New( __snapshot_persister, __snapshot_persister_initialize, __snapshot_persister_shutdown, consumer_service )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00B );
      }
      ++unstarted;
    }

    GRAPH_SUSPEND_LOCK( SYSTEM ) {
      if( COMLIB_TASK__Start( consumer_service->TASK, 10000 ) > 0 ) {
        --unstarted;
      }
      if( durable && COMLIB_TASK__Start( consumer_service->snapshot.TASK, 10000 ) > 0 ) {
        --unstarted;
      }
    } GRAPH_RESUME_LOCK;

    if( unstarted == 0 ) {
      CONSUMER_SERVICE_VERBOSE( consumer_service, 0, "Started" );
      // Erase all master serial info. We will get master serial from provider from now on.
      SYSTEM->sysmaster_tx_serial = 0;
      SYSTEM->sysmaster_tx_serial_0 = 0;
      SYSTEM->sysmaster_tx_serial_count = 0;
    }
    else {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00B );
    }


  }
  XCATCH( errcode ) {
    if( consumer_service->TASK ) {
      COMLIB_TASK__Delete( &consumer_service->TASK );
    }

    ret = -1;
  }
  XFINALLY {
    free( syspath );
  }


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__initialize_OPEN( vgx_Graph_t *SYSTEM, vgx_TransactionalConsumerService_t *consumer_service, uint16_t port, bool durable, int64_t snapshot_threshold ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    ret = _vxdurable_operation_consumer_service__initialize_SYS_CS( SYSTEM, consumer_service, port, durable, snapshot_threshold );
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__destroy_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service ) {
  int ret = 0;

  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  int timeout_ms = 30000;

  int snapshot_stopped = consumer_service->snapshot.TASK == NULL;
  int consumer_stopped = consumer_service->TASK == NULL;

  // Any task need stopping?
  if( !snapshot_stopped || !consumer_stopped ) {

    if( SYSTEM ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {

        // Stop snapshot task
        if( consumer_service->snapshot.TASK ) {
          COMLIB_TASK_LOCK( consumer_service->snapshot.TASK ) {
            // Wait for any ongoing snapshot activity to complete
            while( consumer_service->snapshot.TASK && !COMLIB_TASK__IsDefunct( consumer_service->snapshot.TASK ) && consumer_service->snapshot.running_TCS ) {
              COMLIB_TASK_SUSPEND_MILLISECONDS( consumer_service->snapshot.TASK, 1000 );
            }
            // No ongoing snapshot, stop task
            if( consumer_service->snapshot.TASK && (snapshot_stopped = COMLIB_TASK__Stop( consumer_service->snapshot.TASK, NULL, 10000 )) < 0 ) {
              CONSUMER_SERVICE_WARNING( consumer_service, 0x001, "Snapshot persist task not responding, forcing exit" );
              snapshot_stopped = COMLIB_TASK__ForceExit( consumer_service->snapshot.TASK, 30000 );
              // Give the unjammed snapshot task some time to unwind and shut down
              COMLIB_TASK_SUSPEND_MILLISECONDS( consumer_service->snapshot.TASK, 10000 );
            }
          } COMLIB_TASK_RELEASE;
        }

        // Stop consumer service task
        if( consumer_service->TASK ) {
          COMLIB_TASK_LOCK( consumer_service->TASK ) {
            COMLIB_TASK_SUSPEND_MILLISECONDS( consumer_service->TASK, 2000 );
            if( consumer_service->TASK && (consumer_stopped = COMLIB_TASK__Stop( consumer_service->TASK, NULL, timeout_ms )) < 0 ) {
              CONSUMER_SERVICE_WARNING( consumer_service, 0x002, "Forcing exit" );
              consumer_stopped = COMLIB_TASK__ForceExit( consumer_service->TASK, 30000 );
              // Give the unjammed consumer some time to unwind and shut down
              COMLIB_TASK_SUSPEND_MILLISECONDS( consumer_service->TASK, 10000 );
            }
          } COMLIB_TASK_RELEASE;
        }

      } GRAPH_RESUME_LOCK;
    }

    if( snapshot_stopped != 1 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x003, "Unresponsive snapshot persist thread" );
      ret = -1;
    }

    if( consumer_stopped != 1 ) {
      CONSUMER_SERVICE_CRITICAL( consumer_service, 0x004, "Unresponsive consumer service thread" );
      ret = -1;
    }

  }


  if( ret < 0 ) {
    CONSUMER_SERVICE_CRITICAL( consumer_service, 0x005, "Consumer service thread not destroyed" );
    return ret;
  }


  // Consumer service is stoppped, clean up
  if( consumer_stopped == 1 ) {
    // [Q1.1] TASK
    COMLIB_TASK__Delete( &consumer_service->TASK );

    // [Q1.2]
    consumer_service->control._bits = 0;

    // [Q1.3]
    consumer_service->sysgraph = NULL;

    // [Q1.4]
    if( consumer_service->Listen ) {
      iURI.Delete( &consumer_service->Listen );
    }

    // [Q1.5.1.1]
    consumer_service->bind_port = 0;

    // [Q1.5.1.2]
    consumer_service->__rsv_1_5_1_2 = 0;

    // [Q1.5.2]
    consumer_service->sz_poll_fd = 0;

    // [Q1.6]
    if( consumer_service->poll_fd ) {
      free( consumer_service->poll_fd );
    }

    // [Q1.7]
    consumer_service->plisten = NULL;

    // [Q1.8]
    consumer_service->pclient = NULL;

    // [Q2.1]
    if( consumer_service->TransactionProducerClient ) {
      iURI.Delete( &consumer_service->TransactionProducerClient );
    }

    // [Q2.2]
    iOpBuffer.Delete( &consumer_service->buffer.request );

    // [Q2.3]
    iOpBuffer.Delete( &consumer_service->buffer.response );

    // [Q2.4-5]
    consumer_service->txdata.sz = 0;
    if( consumer_service->txdata.entries ) {
      ALIGNED_FREE( consumer_service->txdata.entries );
      consumer_service->txdata.entries = NULL;
    }
    
    // [Q2.6]
    consumer_service->tx_cursor = NULL;

    // [Q2.7]
    if( consumer_service->tx_slab ) {
      ALIGNED_FREE( consumer_service->tx_slab );
      consumer_service->tx_slab = NULL;
    }

    // [Q2.8]
    consumer_service->tx_slab_cursor = NULL;

    // [Q3.1-8]
    consumer_service->transaction.tms = 0;
    consumer_service->transaction.serial = 0;
    idunset( &consumer_service->transaction.id );
    consumer_service->transaction.crc = 0;
    consumer_service->transaction.stage_dms = 0;
    consumer_service->transaction.tsize = 0;
    consumer_service->transaction.next = NULL;
    consumer_service->transaction.prev = NULL;

    // [Q4.1-2]
    idunset( &consumer_service->resync_tx );

    // [Q4.3.1]
    consumer_service->resync_pending = false;

    // [Q4.3.2]
    consumer_service->provider_attached_SYS_CS = false;

    // [Q4.4]
    iString.Discard( &consumer_service->CSTR__subscriber_uri_SYS_CS );

    // [Q4.5]
    iString.Discard( &consumer_service->CSTR__provider_uri_SYS_CS );

    // [Q4.6]
    consumer_service->last_tx_tms = 0;

    // [Q4.7-8]
    idunset( &consumer_service->last_txid_executed );

    // [Q5.1]
    iURI.Delete( &consumer_service->tx_raw_storage );

    // [Q5.2]
    iString.Discard( &consumer_service->CSTR__tx_location );

    // [Q5.3]
    iString.Discard( &consumer_service->CSTR__tx_fname );

    // [Q5.4]
    consumer_service->tx_out_sn0 = 0;

    // [Q5.5]
    consumer_service->tx_out_t0 = 0;

    // [Q5.6]
    consumer_service->tx_out_sz = 0;

    // [Q5.7-8]
    idunset( &consumer_service->last_txid_commit );

    // [Q6.1.1]
    consumer_service->tx_state.__bits = 0;

    // [Q6.1.2]
    if( consumer_service->tx_in_fd > 0 ) {
      CX_CLOSE( consumer_service->tx_in_fd );
      consumer_service->tx_in_fd = 0;
    }

    // [Q6.2]
    consumer_service->tx_in_sn0 = 0;

    // [Q6.3]
    iOpBuffer.Delete( &consumer_service->tx_in_buffer );

    // [Q6.5]
    consumer_service->tx_backlog_bytes = 0;

    // [Q6.6]
    consumer_service->tx_lifetime_bytes = 0;

    // [Q6.7-8]
    idunset( &consumer_service->initial_txid );

    // [Q8.1] TASK
    COMLIB_TASK__Delete( &consumer_service->snapshot.TASK );

    // [Q8.4]
    consumer_service->tx_log_total_sz_SYS_CS = 0;

    // [Q8.5]
    consumer_service->tx_log_since_sn_SYS_CS = 0;
  }


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__destroy_OPEN( vgx_TransactionalConsumerService_t *consumer_service ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;
  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      ret = _vxdurable_operation_consumer_service__destroy_SYS_CS( consumer_service );
      // We will generate our own master serial from now on
      SYSTEM->sysmaster_tx_serial_0 = __MILLISECONDS_SINCE_1970() * 1000LL;
      SYSTEM->sysmaster_tx_serial_count = 0;
      SYSTEM->sysmaster_tx_serial = SYSTEM->sysmaster_tx_serial_0;
    } GRAPH_RELEASE;
  }
  else {
    ret = _vxdurable_operation_consumer_service__destroy_SYS_CS( consumer_service );
  }
  return ret;
}



/*******************************************************************//**
 *
 * Return the URI string for our input address, or NULL if not bound.
 * Client owns returned string.
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxdurable_operation_consumer_service__get_input_uri_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service ) {
  CString_t *CSTR__input= NULL;
  if( consumer_service && consumer_service->CSTR__subscriber_uri_SYS_CS ) {
    CSTR__input = iString.Clone( consumer_service->CSTR__subscriber_uri_SYS_CS );
  }
  return CSTR__input;
}



/*******************************************************************//**
 *
 * Return the URI string for the attached provider, or NULL if no provider.
 * Client owns returned string.
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxdurable_operation_consumer_service__get_provider_uri_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service ) {
  CString_t *CSTR__provider = NULL;
  if( consumer_service && consumer_service->CSTR__provider_uri_SYS_CS ) {
    CSTR__provider = iString.Clone( consumer_service->CSTR__provider_uri_SYS_CS );
  }
  return CSTR__provider;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxdurable_operation_consumer_service__perform_disk_cleanup_OPEN( vgx_Graph_t *SYSTEM ) {
  GRAPH_LOCK( SYSTEM ) {
    vgx_TransactionalConsumerService_t *consumer_service = SYSTEM->OP.system.consumer;
    if( consumer_service && consumer_service->TASK && consumer_service->CSTR__tx_location != NULL ) {
      GRAPH_SUSPEND_LOCK( SYSTEM ) {
        COMLIB_TASK_LOCK( consumer_service->TASK ) {
          CONSUMER_SERVICE_REQUEST_DISK_CLEANUP_TCS( consumer_service );
        } COMLIB_TASK_RELEASE;
      } GRAPH_RESUME_LOCK;
    }
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__is_durable_OPEN( vgx_Graph_t *SYSTEM ) {
  int durable = 0;

  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      durable = __txlog_enabled( consumer_service );
    }
  } GRAPH_RELEASE;

  return durable;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _vxdurable_operation_consumer_service__subscribe_OPEN( vgx_Graph_t *SYSTEM, const char *host, uint16_t port, bool hardsync, int timeout_ms, CString_t **CSTR__error ) {
  int ret = 0;
  char request_query[512] = {0};
  CString_t *CSTR__subscriber = NULL;
  vgx_URI_t *provider_URI = NULL;

  vgx_VGXServerRequest_t request = {0};
  request.method = HTTP_GET;
  request.accept_type = MEDIA_TYPE__application_json;

  vgx_VGXServerResponse_t response = {0};

  XTRY {

    // Get own bind address
    CSTR__subscriber = iSystem.InputAddress();
    if( CSTR__subscriber == NULL ) {
      __set_error_string( CSTR__error, "No VGX Transaction service running" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    // Build subscribe request
    const char *subscriber = CStringValue( CSTR__subscriber );
    snprintf( request_query, 511, "uri=%s&timeout=%d", subscriber, timeout_ms );

    // Get subscriber uri
    if( (provider_URI = iURI.NewElements( "http", NULL, host, port, "/vgx/subscribe", request_query, NULL, CSTR__error )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_API, 0x002 );
    }

    // Connect to provider
    if( iURI.Connect( provider_URI, timeout_ms > 0 ? timeout_ms : 5000, CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_API, 0x003 );
    }

    // Send request
    if( iVGXServer.Util.SendAll( provider_URI, &request, timeout_ms ) < 0 ) {
      __set_error_string( CSTR__error, "Failed to send subscription request" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Get response
    if( iVGXServer.Util.ReceiveAll( provider_URI, &response, 10000 ) < 0 ) {
      __set_error_string( CSTR__error, "failed to receive response from provider" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
    }

    if( response.status.code != HTTP_STATUS__OK ) {
      __format_error_string( CSTR__error, "Provider response code: %03d ", response.status.code );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
    }

    const char *segment;
    int64_t sz_segment = iStreamBuffer.ReadableSegment( response.buffers.content, 63, &segment, NULL );
    if( sz_segment != 2 || memcmp( segment, "OK", 2 ) ) {
      char buffer[64] = {0};
      memcpy( buffer, segment, sz_segment );
      __format_error_string( CSTR__error, "Provider error: %s ", buffer );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x007 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iStreamBuffer.Delete( &response.buffers.content );
    iString.Discard( &CSTR__subscriber );
    iURI.Delete( &provider_URI ); 
  }

  return ret;
}



/*******************************************************************//**
 *
 * Return:  1: Unsubscribed
 *          0: No action
 *         -1: Failed
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_consumer_service__unsubscribe_OPEN( vgx_Graph_t *SYSTEM ) {
  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    vgx_OperationSystem_t *opsys = &SYSTEM->OP.system;
    vgx_TransactionalConsumerService_t *consumer_service = opsys->consumer;
    if( consumer_service ) {
      if( consumer_service->TransactionProducerClient ) {
        GRAPH_SUSPEND_LOCK( SYSTEM ) {
          int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
          int64_t deadline = t0 + 30000;
          bool defunct = false;
          if( consumer_service->TASK ) {
            COMLIB_TASK_LOCK( consumer_service->TASK ) {
              // Request detach
              CONSUMER_SERVICE_REQUEST_DETACH_TCS( consumer_service );
              while( consumer_service->TransactionProducerClient && __GET_CURRENT_MILLISECOND_TICK() < deadline ) {
                if( (defunct = consumer_service->control.flag_TCS.defunct) == true ) {
                  break;
                }
                COMLIB_TASK_WAIT_FOR_WAKE( consumer_service->TASK, 10 );
                if( consumer_service->TASK == NULL || COMLIB_TASK__IsDead( consumer_service->TASK ) ) {
                  break;
                }
              }
              if( defunct || consumer_service->TASK == NULL ) {
                ret = -1;
              }
              else {
                if( CONSUMER_SERVICE_IS_DETACH_REQUESTED_TCS( consumer_service ) || consumer_service->TransactionProducerClient ) {
                  ret = -1;
                }
                else {
                  ret = 1; // ok
                }
              }
            } COMLIB_TASK_RELEASE;
          }
        } GRAPH_RESUME_LOCK;
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
static unsigned __snapshot_persister_initialize( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_TransactionalConsumerService_t *consumer_service = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;

  GRAPH_LOCK( SYSTEM ) {
    COMLIB_TASK_LOCK( self ) {
    } COMLIB_TASK_RELEASE;
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __snapshot_persister_shutdown( comlib_task_t *self ) {
  unsigned ret = 0;

  vgx_TransactionalConsumerService_t *consumer_service = COMLIB_TASK__GetData( self );
  vgx_Graph_t *SYSTEM = consumer_service->sysgraph;

  if( SYSTEM ) {
    GRAPH_LOCK( SYSTEM ) {
      COMLIB_TASK_LOCK( self ) {
        COMLIB_TASK__ClearState_Busy( self );
      } COMLIB_TASK_RELEASE;
    } GRAPH_RELEASE;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_TransactionalConsumerService_t,
                   consumer_service,
                   __snapshot_persister,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "snapshot_persister/" )
{

#define MIN_PERSIST_INTERVAL 60

  SET_CURRENT_THREAD_LABEL( "vgx_snapshot" );
  
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  
  const char *graph_name = CStringValue( CALLABLE( SYSTEM )->Name( SYSTEM ) );
  APPEND_THREAD_NAME( graph_name );
  COMLIB_TASK__AppendDescription( self, graph_name );

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 1000 );

  int64_t current_txlog_size;
  int64_t max_txlog_size;
  int snapshot_allowed;
  int64_t persist_allowed_after_ts = (int64_t)__SECONDS_SINCE_1970() + MIN_PERSIST_INTERVAL;

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
    int64_t now = __SECONDS_SINCE_1970();

    // Get current and max sizes of txlog
    GRAPH_LOCK( SYSTEM ) {
      current_txlog_size = consumer_service->tx_log_total_sz_SYS_CS;
      max_txlog_size = consumer_service->snapshot.tx_log_threshold_SYS_CS;
      snapshot_allowed = consumer_service->snapshot.allowed_SYS_CS;
    } GRAPH_RELEASE;

    // Persist if backlog is too large
    if( snapshot_allowed && current_txlog_size > max_txlog_size && now > persist_allowed_after_ts ) {
      CONSUMER_SERVICE_INFO( consumer_service, 0x001, "Persisting all graphs (TX log size=%lld bytes exceeds limit=%lld)", current_txlog_size, max_txlog_size );
      COMLIB_TASK__SetState_Busy( self );
      GRAPH_FACTORY_ACQUIRE {
        const vgx_Graph_t **graphs;
        if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) { 
          const vgx_Graph_t **cursor = graphs;
          vgx_Graph_t *graph;
          while( (graph = (vgx_Graph_t*)*cursor++) != NULL ) {
            vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
            CString_t *CSTR__error = NULL;
            if( CALLABLE(graph)->BulkSerialize( graph, 10000, false, false, &reason, &CSTR__error ) < 0 ) {
              const char *serr = CSTR__error ? CStringValue(CSTR__error) : "no details";
              CONSUMER_SERVICE_REASON( consumer_service, 0x002, "Graph '%s' snapshot failed: %s", CALLABLE(graph)->FullPath(graph), serr );
            }
          }
          free( (void*)graphs );
        }
      } GRAPH_FACTORY_RELEASE;
      persist_allowed_after_ts = (int64_t)__SECONDS_SINCE_1970() + MIN_PERSIST_INTERVAL;
      CONSUMER_SERVICE_INFO( consumer_service, 0x003, "Persist complete" );
    }

    // Idle
    COMLIB_TASK__ClearState_Busy( self );

    // Stop requested? Accept request and enter stopping state
    if( COMLIB_TASK__IsRequested_Stop( self ) ) {
      COMLIB_TASK__AcceptRequest_Stop( self );
      COMLIB_TASK__SetState_Stopping( self );
    }

  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_consumer_service.h"
  
test_descriptor_t _vgx_vxdurable_operation_consumer_service_tests[] = {
  { "VGX Graph Durable Operation Consumer Service Tests", __utest_vxdurable_operation_consumer_service },
  {NULL}
};
#endif
