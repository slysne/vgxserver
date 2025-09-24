/*######################################################################
 *#
 *# vxdurable_operation_transaction.c
 *#
 *#
 *######################################################################
 */


#include "_vxoperation.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


#define __MESSAGE( LEVEL, Producer, Code, Format, ... ) do {    \
  vgx_URI_t *_u = (Producer)->URI;                              \
  const char *_scheme = _u ? iURI.Scheme( _u ) : "";            \
  if( URI_SCHEME__vgx( _scheme ) ) {                            \
    const char *_host = _u ? iURI.Host( _u ) : "?";             \
    int _port = _u ? iURI.PortInt( _u ) : 0;                    \
    LEVEL( Code, "TX::OUT(vgx://%s:%d): " Format, _host, _port, ##__VA_ARGS__ ); \
  }                                                             \
  else if( URI_SCHEME__file( _scheme ) ) {                      \
    const char *_path = _u ? iURI.Path( _u ) : "?";             \
    LEVEL( Code, "TX::OUT(file:///%s): " Format, _path, ##__VA_ARGS__ ); \
  }                                                             \
} WHILE_ZERO

#define PRODUCER_VERBOSE( Producer, Code, Format, ... )   __MESSAGE( VERBOSE, Producer, Code, Format, ##__VA_ARGS__ )
#define PRODUCER_INFO( Producer, Code, Format, ... )      __MESSAGE( INFO, Producer, Code, Format, ##__VA_ARGS__ )
#define PRODUCER_WARNING( Producer, Code, Format, ... )   __MESSAGE( WARN, Producer, Code, Format, ##__VA_ARGS__ )
#define PRODUCER_REASON( Producer, Code, Format, ... )    __MESSAGE( REASON, Producer, Code, Format, ##__VA_ARGS__ )
#define PRODUCER_CRITICAL( Producer, Code, Format, ... )  __MESSAGE( CRITICAL, Producer, Code, Format, ##__VA_ARGS__ )
#define PRODUCER_FATAL( Producer, Code, Format, ... )     __MESSAGE( FATAL, Producer, Code, Format, ##__VA_ARGS__ )




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef OPSTREAM_DUMP_TX_IO
static void __dump_tx_request_sent( const char *data, int64_t sz ) {
  static vgx_URI_t *out = NULL;
  if( out == NULL ) {
    const CString_t * CSTR__sysroot = igraphfactory.SystemRoot();
    CString_t *CSTR__txout = CStringNewFormat( "%s/request_sent.tx", CStringValue( CSTR__sysroot ) );
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
static void __dump_tx_response_recv( const char *data, int64_t sz ) {
  static vgx_URI_t *out = NULL;
  if( out == NULL ) {
    const CString_t * CSTR__sysroot = igraphfactory.SystemRoot();
    CString_t *CSTR__txout = CStringNewFormat( "%s/response_recv.tx", CStringValue( CSTR__sysroot ) );
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




static int64_t  __producer_parse_ACCEPTED( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_RETRY( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_REJECTED( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_IDLE( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_SUSPEND( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_RESUME( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_ATTACH( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );
static int64_t  __producer_parse_DETACH( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms );

static int64_t  __producer_recv_parse( vgx_TransactionalProducer_t *producer, int64_t tms );
static int64_t  __producer_send_to_socket( vgx_TransactionalProducer_t *producer, const char *data, int64_t sz );
static int64_t  __producer_perform_attach_handshake( vgx_TransactionalProducer_t *producer );
static int      __producer_send_noblock( vgx_TransactionalProducer_t *producer, CXSOCKET *psock, int64_t tms );
static int      __producer_recv_noblock( vgx_TransactionalProducer_t *producer, CXSOCKET *psock );

static int      __producer_write_file( vgx_TransactionalProducer_t *producer, int fd );    
static int      __producer_write_null( vgx_TransactionalProducer_t *producer );    



static int      __producer_idle( vgx_TransactionalProducer_t *producer, int64_t tms );
static int64_t  __producer_send_transaction_remainder( vgx_TransactionalProducer_t *producer );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationTransaction_t *         _operation_transaction__new( vgx_TransactionalProducer_t *producer, int64_t tms );
static void                                 _operation_transaction__delete( vgx_OperationTransaction_t **transaction );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducer_t *        _transactional_producer__new( vgx_Graph_t *graph, const char *ident );
static void                                 _transactional_producer__delete( vgx_TransactionalProducer_t **producer );
static int64_t                              _transactional_producer__commit( vgx_TransactionalProducer_t *producer, vgx_OperationBuffer_t *data, int64_t tms, bool validate, int64_t master_serial, objectid_t *ptx_id, int64_t *ptx_serial );
static int                                  _transactional_producer__attach( vgx_TransactionalProducer_t *producer, vgx_URI_t *URI, vgx_TransactionalProducerAttachMode mode, bool handshake, CString_t **CSTR__error );
static const char *                         _transactional_producer__name( const vgx_TransactionalProducer_t *producer );
static vgx_TransactionalProducerAttachMode  _transactional_producer__mode( const vgx_TransactionalProducer_t *producer );
static int                                  _transactional_producer__connect( vgx_TransactionalProducer_t *producer, CString_t **CSTR__error );
static int                                  _transactional_producer__reconnect( vgx_TransactionalProducer_t *producer, int64_t tms_now, int timeout_ms );
static bool                                 _transactional_producer__attached( const vgx_TransactionalProducer_t *producer );
static bool                                 _transactional_producer__connected( const vgx_TransactionalProducer_t *producer );
static bool                                 _transactional_producer__connected_remote( const vgx_TransactionalProducer_t *producer );
static void                                 _transactional_producer__disconnect( vgx_TransactionalProducer_t *producer );
static void                                 _transactional_producer__set_defunct( vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__purge( vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__abandon( vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__length( const vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__bytes( const vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__timespan( const vgx_TransactionalProducer_t *producer );
static int64_t                              _transactional_producer__stage_latency( const vgx_TransactionalProducer_t *producer, int64_t tms_now );
static int64_t                              _transactional_producer__queue_latency( const vgx_TransactionalProducer_t *producer, int64_t tms_now );
static vgx_OperationTransactionIterator_t * _transactional_producer__transaction_iterator_init( const vgx_TransactionalProducer_t *producer, vgx_OperationTransactionIterator_t *iterator );
static const vgx_OperationTransaction_t *   _transactional_producer__transaction_iterator_next( vgx_OperationTransactionIterator_t *iterator );

static int64_t                              _transactional_producer__recv_buffer_writable( const vgx_TransactionalProducer_t *producer, int64_t max, char **segment );
static int64_t                              _transactional_producer__recv_buffer_advance_write( vgx_TransactionalProducer_t *producer, int64_t amount );
static int64_t                              _transactional_producer__recv_buffer_readable( const vgx_TransactionalProducer_t *producer, int64_t max, const char **segment );
static int64_t                              _transactional_producer__recv_buffer_advance_read( vgx_TransactionalProducer_t *producer, int64_t amount );
static int64_t                              _transactional_producer__recv_buffer_readline( vgx_TransactionalProducer_t *producer, int64_t max, char **data );

static int64_t                              _transactional_producer__handle_ACCEPTED( vgx_TransactionalProducer_t *producer, const objectid_t *transid, unsigned int crc, int64_t tms );
static int64_t                              _transactional_producer__handle_REJECTED( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms );
static int64_t                              _transactional_producer__handle_RETRY( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms );
static int64_t                              _transactional_producer__handle_SUSPEND( vgx_TransactionalProducer_t *producer, DWORD reason, int64_t tms );
static int64_t                              _transactional_producer__handle_RESUME( vgx_TransactionalProducer_t *producer, int64_t tms );
static int64_t                              _transactional_producer__handle_ATTACH( vgx_TransactionalProducer_t *producer, DWORD protocol, DWORD version, objectid_t *fingerprint, WORD adminport );
static int64_t                              _transactional_producer__handle_DETACH( vgx_TransactionalProducer_t *producer, int64_t tms );

static int                                  _transactional_producer__perform_exchange( vgx_TransactionalProducer_t *producer, int64_t tms );
static int64_t                              _transactional_producer__perform_rollback( vgx_TransactionalProducer_t *producer, int64_t tms );

static void                                 _transactional_producer__dump_state( const vgx_TransactionalProducer_t *producer, const objectid_t *focus );
static void                                 _transactional_producer__dump_unconfirmed( const vgx_TransactionalProducer_t *producer );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationParser_t *  _transactional_validator__new( vgx_Graph_t *graph );
static void                     _transactional_validator__delete( vgx_OperationParser_t **validator );
static int64_t                  _transactional_validator__validate( vgx_OperationParser_t *validator, vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *buffer, CString_t **CSTR__error );
static int                      _transactional_validator__reset( vgx_OperationParser_t *validator );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducers_t *         _transactional_producers__new( void );
static void                                   _transactional_producers__delete( vgx_TransactionalProducers_t **producers );
static int                                    _transactional_producers__clear( vgx_TransactionalProducers_t *producers );
static int                                    _transactional_producers__add( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducer_t **producer );
static int                                    _transactional_producers__remove( vgx_TransactionalProducers_t *producers, const char *uri );
static int                                    _transactional_producers__contains( vgx_TransactionalProducers_t *producers, const char *uri );
static int                                    _transactional_producers__defragment( vgx_TransactionalProducers_t *producers );
static int64_t                                _transactional_producers__length( vgx_TransactionalProducers_t *producers );
static vgx_TransactionalProducersIterator_t * _transactional_producers__iterator_init( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducersIterator_t *iterator );
static int64_t                                _transactional_producers__iterator_length( const vgx_TransactionalProducersIterator_t *iterator );
static vgx_TransactionalProducer_t *          _transactional_producers__iterator_next( vgx_TransactionalProducersIterator_t *iterator );
static void                                   _transactional_producers__iterator_clear( vgx_TransactionalProducersIterator_t *iterator );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalConsumerService_t *   _transactional_consumer_service__new( vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold );
static int                                    _transactional_consumer_service__open( vgx_TransactionalConsumerService_t *service, uint16_t port, CString_t **CSTR__error );
static int                                    _transactional_consumer_service__close( vgx_TransactionalConsumerService_t *service );
static int                                    _transactional_consumer_service__delete( vgx_TransactionalConsumerService_t **service );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_ITransactional_t iTransactional = {
  .Producer = {
    .New              = _transactional_producer__new,
    .Delete           = _transactional_producer__delete,
    .Commit           = _transactional_producer__commit,
    .Attach           = _transactional_producer__attach,
    .Name             = _transactional_producer__name,
    .Mode             = _transactional_producer__mode,
    .Connect          = _transactional_producer__connect,
    .Reconnect        = _transactional_producer__reconnect,
    .Attached         = _transactional_producer__attached,
    .Connected        = _transactional_producer__connected,
    .ConnectedRemote  = _transactional_producer__connected_remote,
    .Disconnect       = _transactional_producer__disconnect,
    .SetDefunct       = _transactional_producer__set_defunct,
    .Purge            = _transactional_producer__purge,
    .Abandon          = _transactional_producer__abandon,
    .Length           = _transactional_producer__length,
    .Bytes            = _transactional_producer__bytes,
    .Timespan         = _transactional_producer__timespan,
    .StageLatency     = _transactional_producer__stage_latency,
    .QueueLatency     = _transactional_producer__queue_latency,
    .Transaction  = {
      .Iterator = {
        .Init         = _transactional_producer__transaction_iterator_init,
        .Next         = _transactional_producer__transaction_iterator_next,
      }
    },

    .Receive = {
      .Writable       = _transactional_producer__recv_buffer_writable,
      .AdvanceWrite   = _transactional_producer__recv_buffer_advance_write,
      .Readable       = _transactional_producer__recv_buffer_readable,
      .AdvanceRead    = _transactional_producer__recv_buffer_advance_read,
      .Readline       = _transactional_producer__recv_buffer_readline,
    },

    .Handle = {
      .ACCEPTED       = _transactional_producer__handle_ACCEPTED,
      .REJECTED       = _transactional_producer__handle_REJECTED,
      .RETRY          = _transactional_producer__handle_RETRY,
      .SUSPEND        = _transactional_producer__handle_SUSPEND,
      .RESUME         = _transactional_producer__handle_RESUME,
      .ATTACH         = _transactional_producer__handle_ATTACH,
      .DETACH         = _transactional_producer__handle_DETACH
    },

    .Perform = {
      .Exchange       = _transactional_producer__perform_exchange,
      .Rollback       = _transactional_producer__perform_rollback
    },
    
    .DumpState        = _transactional_producer__dump_state,
    .DumpUnconfirmed  = _transactional_producer__dump_unconfirmed
  },

  .Validator = {
    .New              = _transactional_validator__new,
    .Delete           = _transactional_validator__delete,
    .Validate         = _transactional_validator__validate,
    .Reset            = _transactional_validator__reset
  },

  .Producers = {
    .New              = _transactional_producers__new,
    .Delete           = _transactional_producers__delete,
    .Clear            = _transactional_producers__clear,
    .Add              = _transactional_producers__add,
    .Remove           = _transactional_producers__remove,
    .Contains         = _transactional_producers__contains,
    .Defragment       = _transactional_producers__defragment,
    .Length           = _transactional_producers__length,
    .Iterator = {
      .Init           = _transactional_producers__iterator_init,
      .Length         = _transactional_producers__iterator_length,
      .Next           = _transactional_producers__iterator_next,
      .Clear          = _transactional_producers__iterator_clear
    }
  },

  .Consumer = {
    .New          = _transactional_consumer_service__new,
    .Open         = _transactional_consumer_service__open,
    .Close        = _transactional_consumer_service__close,
    .Delete       = _transactional_consumer_service__delete
  }

};



static int64_t                      __next_transaction_id( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction );
static int64_t                      __begin_transaction( vgx_OperationTransaction_t *transaction, int64_t master_serial, vgx_OperationBuffer_t *sysout );
static int64_t                      __copy_transaction_data( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout, vgx_OperationBuffer_t *data );
static int64_t                      __end_transaction( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout );
static int                          __append_output_timestamp( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout, int64_t sn );
static void                         __assert_tsize( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout );
static void                         __assert_producer( vgx_TransactionalProducer_t *producer, int64_t sz_tx, int64_t sz_sysout_pre );
static int64_t                      __attach_transaction( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction, int64_t sz_sysout_pre );
static vgx_OperationTransaction_t * __detach_transaction( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction, int64_t tms );
static vgx_OperationTransaction_t * __get_transaction( vgx_TransactionalProducer_t *producer, const objectid_t *transid, int64_t *offset );
static int64_t                      __purge_transactions( vgx_TransactionalProducer_t *producer );
static vgx_OperationBuffer_t *      __new_named_buffer( int order, const char *name, const char *ident, vgx_Graph_t *graph );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __next_transaction_id( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction ) {
  // Serial number
  transaction->serial = ++(producer->sn) + producer->serial0;
  // Transaction ID
  char buffer[64];
  int n = snprintf( buffer, 63, "serial=%llx,salt=%llx", transaction->serial, producer->tid_salt );
  transaction->id = obid_from_string_len( buffer, n );
  // Return serial number
  return transaction->serial;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __begin_transaction( vgx_OperationTransaction_t *transaction, int64_t master_serial, vgx_OperationBuffer_t *sysout ) {
  char buf[96], *b = buf;
  // TRANSACTION <txid> <sn> <master_sn>\n
  // TRANSACTION xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 0123456789ABCDEF 0123456789ABCDEF\n
  b = __writebuf_objectid( &b, KWD_TRANSACTION " ", &transaction->id, NULL );
  b = __writebuf_qword( &b, SEP, transaction->serial, NULL );
  b = __writebuf_qword( &b, SEP, master_serial, KWD_LF );
  // Output
  int64_t sz = b - buf;
  *b = '\0';
  if( iOpBuffer.WriteCrc( sysout, buf, sz, &transaction->crc ) == sz ) {
    transaction->tsize += sz;
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __copy_transaction_data( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout, vgx_OperationBuffer_t *data ) {
  int64_t sz_data = iOpBuffer.CopyCrc( sysout, data, &transaction->crc );
  if( sz_data > 0 ) {
    transaction->tsize += sz_data;
  }
  return sz_data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __end_transaction( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout ) {
  char buf[96], *b = buf;
  // COMMIT xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 00000181C60995E2 FEDCBA98\r\n
  b = __writebuf_objectid( &b, KWD_COMMIT " ", &transaction->id, NULL );
  b = __writebuf_qword( &b, SEP, transaction->tms, NULL );
  b = __writebuf_dword( &b, SEP, transaction->crc, KWD_EOTX );
  // Output
  int64_t sz = b - buf;
  *b = '\0';
  if( iOpBuffer.Write( sysout, buf, sz ) == sz ) {
    transaction->tsize += sz;
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __append_output_timestamp( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout, int64_t sn ) {
  int ret = -1;
  CString_t *CSTR__now = igraphinfo.CTime( -1, true );
  if( CSTR__now ) {
    CString_t *CSTR__commit = CStringNewFormat( "# [%s] [sn=%lld]\n", CStringValue( CSTR__now ), sn );
    if( CSTR__commit ) {
      const char *scommit = CStringValue( CSTR__commit );
      int64_t sz = CStringLength( CSTR__commit );
      if( iOpBuffer.WriteCrc( sysout, scommit, sz, &transaction->crc ) == sz ) {
        transaction->tsize += sz;
        ret = 0; // ok!
      }
      CStringDelete( CSTR__commit );
    }
    CStringDelete( CSTR__now );
  }
  return ret;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __assert_tsize( vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *sysout ) {
  int64_t readable = iOpBuffer.Readable( sysout );
  int64_t tsize = transaction->tsize;
  if( readable < tsize ) {
    iOpBuffer.DumpState( sysout );
    FATAL( 0x001, "readable %lld < tsize %lld", readable, tsize );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __assert_producer( vgx_TransactionalProducer_t *producer, int64_t sz_tx, int64_t sz_sysout_pre ) {
  if( producer->sequence.head ) {
    int64_t bytes = 0;
    int64_t length = 0;
    vgx_OperationTransactionIterator_t iter;
    const vgx_OperationTransaction_t *node;
    iTransactional.Producer.Transaction.Iterator.Init( producer, &iter );
    while( (node = iTransactional.Producer.Transaction.Iterator.Next( &iter )) != NULL ) {
      ++length;
      bytes += node->tsize;
    }

    if( length != producer->sequence.length ) {
      iTransactional.Producer.DumpState( producer, NULL );
      PRODUCER_FATAL( producer, 0x001, "transaction sequence mismatch: producer->sequence.length=%lld, found %lld", producer->sequence.length, length );
    }

    if( bytes != producer->sequence.bytes ) {
      iTransactional.Producer.DumpState( producer, NULL );
      PRODUCER_FATAL( producer, 0x002, "transaction sequence mismatch: producer->sequence.bytes=%lld, found %lld", producer->sequence.bytes, bytes );
    }

    int64_t sz_sysout = iOpBuffer.Size( producer->buffer.sysout );
    if( sz_sysout != sz_sysout_pre + sz_tx ) {
      iTransactional.Producer.DumpState( producer, NULL );
      PRODUCER_FATAL( producer, 0x003, "transaction size (%lld) does not match sysout size delta (%lld-%lld=%lld)", sz_tx, sz_sysout, sz_sysout_pre, sz_sysout-sz_sysout_pre );
    }

  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __stage_transaction( vgx_OperationTransaction_t *transaction, int64_t tms_stage ) {
  // Millisecond delta between now and when transaction object was created
  int64_t delta = tms_stage - transaction->tms;

  // We can get staging time by add this delta to the creation timestamp.
  transaction->stage_dms = (int)delta;

  return tms_stage;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __get_stage_timestamp( const vgx_OperationTransaction_t *transaction ) {
  int64_t tms_stage = transaction->tms + transaction->stage_dms;
  return tms_stage;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __attach_transaction( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction, int64_t sz_sysout_pre ) {
  vgx_OperationTransaction_t *T = transaction;

  //
  //  (T)
  //   |
  //   0
  //
  T->next = NULL;

  //    
  // (E) -> (N)
  //        ||
  //        ||
  //        (T)
  //         |
  //         0
  //
  if( producer->sequence.tail ) {
    vgx_OperationTransaction_t *node = producer->sequence.tail;
    node->next = T;
    T->prev = node;
  }
  //  
  //         0
  //         |
  // (H) -> (T)
  //         |
  //         0
  //    
  else {
    // List was empty, we become the only head
    producer->sequence.head = T;
    T->prev = NULL;
  }

  //
  //        ..
  //        ..
  // (E) -> (T)
  //         |
  //         0
  //
  producer->sequence.tail = T;

  // Update counters
  producer->sequence.bytes += T->tsize;
  ++(producer->sequence.length);

  __assert_producer( producer, T->tsize, sz_sysout_pre );

  // Return new length
  return producer->sequence.length;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationTransaction_t * __detach_transaction( vgx_TransactionalProducer_t *producer, vgx_OperationTransaction_t *transaction, int64_t tms ) {
  vgx_OperationTransaction_t *T = transaction;

  // P <- T -> N
  //  \_______/^
  //
  if( T->prev ) {
    (T->prev)->next = T->next;
  }
  
  // P <- T -> N
  // ^\_______/ 
  //
  if( T->next ) {
    (T->next)->prev = T->prev;
  }

  // H -> T 
  //  \    `-> N
  //   \______/^
  //      
  if( T == producer->sequence.head ) {
    vgx_OperationTransaction_t *N = T->next;
    if( N ) {
      // Next transaction staged x milliseconds after its creation
      __stage_transaction( N, tms );
    }
    producer->sequence.head = N; // could be NULL (i.e. list now empty)
  }

  //    P <-\
  //   /^    T
  //  /     /^
  // E ----'
  if( T == producer->sequence.tail ) {
    producer->sequence.tail = T->prev; // could be NULL (i.e. list now empty)
  }

  // Detached
  T->next = NULL;
  T->prev = NULL;

  // Update counters
  producer->sequence.bytes -= T->tsize;
  --(producer->sequence.length);

  return T;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationTransaction_t * __get_transaction( vgx_TransactionalProducer_t *producer, const objectid_t *transid, int64_t *offset ) {
  // Search from head
  vgx_OperationTransaction_t *node = producer->sequence.head;
  int match = 0;
  while( node && (match = idmatch( &node->id, transid )) == 0 ) {
    *offset += node->tsize;
    node = node->next;
  }

  // Hit
  if( match ) {
    return node;
  }
  // Miss
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __purge_transactions( vgx_TransactionalProducer_t *producer ) {
  int64_t n = 0;
  if( producer->sequence.head ) {
    vgx_OperationTransaction_t *node = producer->sequence.head;
    vgx_OperationTransaction_t *T;
    while( node ) { 
      ++n;
      T = node;
      node = node->next;
      _operation_transaction__delete( &T );
    }
    producer->sequence.head = NULL;
  }
  producer->sequence.tail = NULL;

  producer->sequence.bytes = 0;
  producer->sequence.length = 0;

  producer->resync_transaction = NULL;
  producer->resync_remain = 0;

  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_OperationBuffer_t * __new_named_buffer( int order, const char *name, const char *ident, vgx_Graph_t *graph ) {
  vgx_OperationBuffer_t *buffer = NULL;
  CString_t *CSTR__name = CStringNewFormat( "%s/%s/%s", name, ident, CALLABLE( graph )->FullPath( graph ) );
  if( CSTR__name ) {
    buffer = iOpBuffer.New( order, CStringValue( CSTR__name ) );
    CStringDelete( CSTR__name );
  }
  return buffer;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducer_t * _transactional_producer__new( vgx_Graph_t *graph, const char *ident ) {
  vgx_TransactionalProducer_t *producer = NULL;
  
  XTRY {

    if( !iSystem.IsSystemGraph( graph ) ) {
      THROW_FATAL( CXLIB_ERR_ASSERTION, 0x001 );
    }

  
    if( (producer = calloc( 1, sizeof( vgx_TransactionalProducer_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // [Q1.1]
    producer->sequence.head = NULL;
    
    // [Q1.2]
    producer->sequence.tail = NULL;

    // [Q1.3]
    producer->sequence.length = 0;
    
    // [Q1.4]
    producer->sequence.bytes = 0;

    // [Q1.5]
    // Output buffer, initial size 256
    if( (producer->buffer.sysout = __new_named_buffer( 8, "transaction_sysout", ident, graph )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // [Q1.6]
    // Input buffer, initial size 1024
    if( (producer->buffer.sysin = __new_named_buffer( 10, "transaction_sysin", ident, graph )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }

    // [Q1.7]
    producer->URI = NULL;

    // [Q1.8]
    producer->connection_ts.attempt = 0;
    producer->connection_ts.down = 0;

    // [Q2.1]
    producer->graph = graph;

    // [Q2.2]
    producer->accepted_transactions = 0;

    // [Q2.3]
    producer->accepted_bytes = 0;

    // [Q2.4]
    producer->tid_salt = graph->instance_id;

    // [Q2.5]
    // Initial serial number is microseconds since 1970
    producer->serial0 = __MILLISECONDS_SINCE_1970() * 1000LL;

    // [Q2.6]
    // Serial delta, using serial0 as base
    producer->sn = 0;

    // [Q2.7]
    producer->exchange_tms = 0;

    // [Q2.8]
    producer->suspend_tx_until_tms = 0;

    // [Q3.1-5]
    INIT_CRITICAL_SECTION( &producer->lock.lock );

    // [Q4.1.1]
    producer->lock_count = 0;

    // [Q4.1.2]
    producer->flags._rsv16 = 0;
    producer->flags.mode = TX_ATTACH_MODE_NONE;
    producer->flags.confirmable = false;
    producer->flags.defunct = false;
    producer->flags.abandoned = false;
    producer->flags.handshake = false;
    producer->flags.init = true;
    producer->flags._rsv6 = false;
    producer->flags._rsv7 = false;
    producer->flags.muted = false;

    // [Q4.2]
    producer->resync_transaction = NULL;

    // [Q4.3-4]
    producer->local.fingerprint = igraphfactory.Fingerprint();

    // [Q4.5]
    producer->local.state_tms = 0;

    // [Q4.6]
    producer->local.__rsv_4_6 = 0;

    // [Q4.7]
    producer->resync_remain = 0;

    // [Q4.8]
    producer->__rsv_4_8 = 0;

    // [Q5.1-2]
    idunset( &producer->subscriber.fingerprint );

    // [Q5.3]
    producer->subscriber.master_serial = 0;

    // [Q5.4.1]
    producer->subscriber.adminport = 0;

    // [Q5.4.2]
    producer->subscriber.__rsv_5_4_2 = 0;

  }
  XCATCH( errcode ) {
    if( producer ) {
      iOpBuffer.Delete( &producer->buffer.sysout );
      iOpBuffer.Delete( &producer->buffer.sysin );
      free( producer );
      producer = NULL;
    }
  }
  XFINALLY {
  }
  return producer;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _transactional_producer__delete( vgx_TransactionalProducer_t **producer ) {
  if( producer && *producer && (*producer)->flags.init == false ) {

    __purge_transactions( *producer );

    // [Q1.5]
    iOpBuffer.Delete( &(*producer)->buffer.sysout );

    // [Q1.6]
    iOpBuffer.Delete( &(*producer)->buffer.sysin );

    // [Q1.7]
    _transactional_producer__disconnect( *producer );

    // [Q1.8]
    (*producer)->graph = NULL;

    // [Q2.1-5]
    if( (*producer)->lock_count == 0 ) {
      DEL_CRITICAL_SECTION( &((*producer)->lock.lock) );
    }

    free( *producer );
    *producer = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool __committable( int64_t sz_required, int64_t *limit, vgx_OperationBuffer_t *sysout ) {
  if( sz_required > iOpBuffer.Writable( sysout ) && iOpBuffer.Capacity( sysout ) >= *limit ) {
    // Update limit to a lower number if we fail
    *limit = SYSOUT_LIMIT - THROTTLE_SYSOUT;
    return false;
  }
  else {
    return true;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__commit( vgx_TransactionalProducer_t *producer, vgx_OperationBuffer_t *data, int64_t tms, bool validate, int64_t master_serial, objectid_t *ptx_id, int64_t *ptx_serial ) {

  int64_t sysout_limit = SYSOUT_LIMIT;
  int64_t dlen = iOpBuffer.Readable( data );
  int64_t sz_required = dlen + 512; // data plus transaction overhead with padding

  // Hold here until sysout is able to accept more data
  TRANSACTIONAL_WAIT_UNTIL( producer, __committable( sz_required, &sysout_limit, producer->buffer.sysout ), 60000 );
  if( !__committable( sz_required, &sysout_limit, producer->buffer.sysout ) ) {
    // Buffer never became available
    return -1;
  }

  CString_t *CSTR__error = NULL;

  int64_t sz_sysout_pre = iOpBuffer.Size( producer->buffer.sysout );
  
  vgx_OperationTransaction_t *transaction = NULL;
  XTRY {
    // Create a new transaction with unique ID and serial
    if( (transaction = _operation_transaction__new( producer, tms )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Output transaction header to sysout
    if( __begin_transaction( transaction, master_serial, producer->buffer.sysout ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Copy source data into sysout
    if( __copy_transaction_data( transaction, producer->buffer.sysout, data ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Append a human readable comment
    if( __append_output_timestamp( transaction, producer->buffer.sysout, producer->sn ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Output transaction commit to sysout
    if( __end_transaction( transaction, producer->buffer.sysout ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Run validation
    if( validate ) {
      vgx_OperationParser_t *validator = producer->graph->OP.system.validator;
      if( iTransactional.Validator.Validate( validator, transaction, producer->buffer.sysout, &CSTR__error ) < 0 ) {
        const char *msg = CSTR__error ? CStringValue( CSTR__error ) : "unknown validation error";
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x006, msg );
      }
    }

    if( transaction->tsize >= TX_MAX_SIZE ) {
      PRODUCER_WARNING( producer, 0x007, "Generating oversized transaction tx=%016llx%016llx (%lld bytes)", transaction->id.H, transaction->id.L, transaction->tsize );
    }

    // Attach transaction and complete the commit.
    // Transaction data is now streamed to sysout.
    // The new transaction entry in the list keeps track of what to
    // do once the transaction has been sent to the remote end
    // and the remote end has responded.
    if( producer->flags.confirmable ) {
      __attach_transaction( producer, transaction, sz_sysout_pre );
    }

    if( ptx_id ) {
      idcpy( ptx_id, &transaction->id );
    }
    if( ptx_serial ) {
      *ptx_serial = transaction->serial;
    }

  }
  XCATCH( errcode ) {
    if( transaction ) {
      iOpBuffer.Unwrite( producer->buffer.sysout, transaction->tsize );
      _operation_transaction__delete( &transaction );
    }
    iString.Discard( &CSTR__error );
  }
  XFINALLY {
    
  }

  // Return transaction size in bytes (or -1 on error)
  return transaction ? transaction->tsize : -1;

 }



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _transactional_producer__attach( vgx_TransactionalProducer_t *producer, vgx_URI_t *URI, vgx_TransactionalProducerAttachMode mode, bool handshake, CString_t **CSTR__error ) {
  int ret = 0;

  vgx_URI_t *tmpURI = NULL;

  XTRY {
    if( (tmpURI = iURI.Clone( URI )) == NULL ) {
      __set_error_string( CSTR__error, "internal error" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    const char *scheme = iURI.Scheme( tmpURI );

    // Scheme: vgx
    if( URI_SCHEME__vgx( scheme ) ) {
      producer->flags.confirmable = true;
#ifdef HASVERBOSE
      const char *host = iURI.Host( tmpURI );
      int port = iURI.PortInt( tmpURI );
      VERBOSE( 0x002, "Transactional Producer attached to service vgx://%s:%d", host, port );
#endif
    }
    // Scheme: file
    else if( URI_SCHEME__file( scheme ) ) {
      producer->flags.confirmable = false;
#ifdef HASVERBOSE
      const char *path = iURI.Path( tmpURI );
      VERBOSE( 0x003, "Transactional Producer attached to output file '%s'", path );
#endif
    }
    // Scheme: null
    else if( URI_SCHEME__null( scheme ) ) {
      producer->flags.confirmable = false;
      VERBOSE( 0x004, "Transactional Producer attached to null output" );
    }
    // Unsupported scheme
    else {
      __format_error_string( CSTR__error, "Unsupported URI scheme: %s", scheme );
      THROW_SILENT( CXLIB_ERR_API, 0x007 );
    }

    producer->flags.handshake = handshake;

    producer->flags.mode = mode;

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( ret == 0 ) {
      // Disconnect and remove any previous URI before we set the new one
      iURI.Delete( &producer->URI );
      producer->URI = tmpURI;
      tmpURI = NULL;

      // Reset connect time
      producer->connection_ts.attempt = 0;
      producer->connection_ts.down = 0;

      producer->flags.defunct = false;
    }

    if( tmpURI ) {
      iURI.Delete( &tmpURI );
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
static const char * _transactional_producer__name( const vgx_TransactionalProducer_t *producer ) {
  if( producer->URI ) {
    return iURI.URI( producer->URI );
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducerAttachMode _transactional_producer__mode( const vgx_TransactionalProducer_t *producer ) {
  return producer->flags.mode;
}



/*******************************************************************//**
 *
 * Returns: 1 if new connection successful
 *          0 if already connected
 *         -1 on error
 *
 ***********************************************************************
 */
static int _transactional_producer__connect( vgx_TransactionalProducer_t *producer, CString_t **CSTR__error ) {
  int connected = 0;

  XTRY {
    if( producer->URI == NULL ) {
      __set_error_string( CSTR__error, "not attached" );
      THROW_SILENT( CXLIB_ERR_API, 0x001 );
    }

    // Not already connected
    if( !iURI.IsConnected( producer->URI ) ) {
      const char *scheme = iURI.Scheme( producer->URI );

      // Scheme: vgx
      if( URI_SCHEME__vgx( scheme ) ) {
        const char *host = iURI.Host( producer->URI );
        int port = iURI.PortInt( producer->URI );

        // Open socket connection within URI object
        int err = 0;
        if( iURI.ConnectInetStreamTCP( producer->URI, 5000, CSTR__error, &err ) == NULL ) {
          if( CSTR__error ) {
            const char *detail = *CSTR__error ? CStringValue( *CSTR__error ) : "none";
            CString_t *CSTR__info = CStringNewFormat( "Transactional Producer could not open URI vgx://%s:%d (%d %s)", host, port, err, detail );
            if( CSTR__info ) {
              iString.Discard( CSTR__error );
              *CSTR__error = CSTR__info;
            }
          }
          THROW_SILENT( CXLIB_ERR_API, 0x002 );
        }

        if( __producer_perform_attach_handshake( producer ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }

        PRODUCER_INFO( producer, 0x004, "Connected" );
      }
      // Scheme: file
      else if( URI_SCHEME__file( scheme ) ) {
        if( iURI.OpenFile( producer->URI, CSTR__error ) != 0 ) {
          THROW_SILENT( CXLIB_ERR_FILESYSTEM, 0x005 );
        }

        PRODUCER_INFO( producer, 0x006, "Opened writable" );
      }
      // Scheme: null
      else if( URI_SCHEME__null( scheme ) ) {
        if( iURI.OpenNull( producer->URI, CSTR__error ) != 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x007 );
        }
      }
      // Unsupported scheme
      else {
        __format_error_string( CSTR__error, "Unsupported URI scheme: %s", scheme );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
      }
      // OK!
      connected = 1;
    }
  }
  XCATCH( errcode ) {
    producer->connection_ts.attempt = 0;
    producer->connection_ts.down = 0;
    connected = -1;
  }
  XFINALLY {
    if( connected > 0 ) {
      // Set time of connect/open
      producer->connection_ts.attempt = _vgx_graph_seconds( producer->graph );
      producer->connection_ts.down = 0;
    }
  }

  return connected;
}



/*******************************************************************//**
 *
 *
 *
 * On error, returns errno with negative sign
 ***********************************************************************
 */
static int _transactional_producer__reconnect( vgx_TransactionalProducer_t *producer, int64_t tms_now, int timeout_ms ) {
  // URI exists
  if( producer->URI ) {
    const char *scheme = iURI.Scheme( producer->URI );
    // Scheme: vgx
    if( URI_SCHEME__vgx( scheme ) ) {
      uint32_t now_ts = (uint32_t)(tms_now / 1000);

      // Close existing connection
      if( producer->URI->output.psock != NULL ) {
        iURI.Close( producer->URI );
        producer->connection_ts.down = now_ts;
      }

      // Not connected
      if( producer->URI->output.psock == NULL ) {
        PRODUCER_INFO( producer, 0x001, "Reconnecting..." );
        CString_t *CSTR__error = NULL;
        int err;

        vgx_URI_t *temp_URI = iURI.Clone( producer->URI );
        if( temp_URI == NULL ) {
          PRODUCER_REASON( producer, 0x002, "Memory error" );
          return -ENOMEM;
        }

        TRANSACTIONAL_SUSPEND_LOCK( producer ) {
          err = iURI.Connect( temp_URI, timeout_ms, &CSTR__error );
        } TRANSACTIONAL_RESUME_LOCK;
        
        if( CSTR__error ) {
          const char *detail = CStringValue( CSTR__error );
          PRODUCER_WARNING( producer, 0x003, "Reconnect failed: %s", detail );
          CStringDelete( CSTR__error );
        }

        // Failed reconnect
        if( err < 0 ) {
          iURI.Delete( &temp_URI );
          // Next connection attempt depends on how long we have been down
          uint32_t delta = now_ts - producer->connection_ts.down;
          if( delta > 10 ) {
            delta = 10;
          }
          producer->connection_ts.attempt = now_ts + delta;
          return err;
        }
        // Successful reconnect to socket, now perform handshake
        else {
          // First make sure no other connection took place while lock was suspended above
          if( producer->URI->output.psock != NULL ) {
            // Another thread already connected
            iURI.Delete( &temp_URI );
            return 0;
          }

          // Steal temp URI's socket before deleting temp URI
          producer->URI->_sock = temp_URI->_sock;
          producer->URI->output.psock = &producer->URI->_sock;
          temp_URI->output.psock = NULL;
          iURI.Delete( &temp_URI );

          // Perform handshake
          producer->connection_ts.attempt = now_ts;
          return (int)__producer_perform_attach_handshake( producer );
        }
      }
      // Already connected
      else {
        return 0;
      }
    }
    // No action for other schemes
    else {
      return 0;
    }
  }
  // No URI
  else {
    return -EINVAL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool _transactional_producer__attached( const vgx_TransactionalProducer_t *producer ) {
  if( producer->URI ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool _transactional_producer__connected( const vgx_TransactionalProducer_t *producer ) {
  return iURI.IsConnected( producer->URI );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool _transactional_producer__connected_remote( const vgx_TransactionalProducer_t *producer ) {
  return iURI.Sock.Output.Get( producer->URI ) != NULL;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _transactional_producer__disconnect( vgx_TransactionalProducer_t *producer ) {
  iURI.Delete( &producer->URI );
  producer->flags.confirmable = false;
  producer->connection_ts.attempt = 0;
  producer->connection_ts.down = 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _transactional_producer__set_defunct( vgx_TransactionalProducer_t *producer ) {
  producer->flags.defunct = true;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__purge( vgx_TransactionalProducer_t *producer ) {
  __purge_transactions( producer );
  return iOpBuffer.Clear( producer->buffer.sysout );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__abandon( vgx_TransactionalProducer_t *producer ) {
  int64_t purged = iTransactional.Producer.Purge( producer );
  iTransactional.Producer.Disconnect( producer );
  producer->flags.abandoned = true;
  return purged;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__length( const vgx_TransactionalProducer_t *producer ) {
  return producer->sequence.length;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__bytes( const vgx_TransactionalProducer_t *producer ) {
  return producer->sequence.bytes;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__timespan( const vgx_TransactionalProducer_t *producer ) {
  if( producer->sequence.head && producer->sequence.tail ) {
    return producer->sequence.tail->tms - producer->sequence.head->tms;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * Return the number of milliseconds between now and when the earliest
 * unconfirmed transaction was staged.
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__stage_latency( const vgx_TransactionalProducer_t *producer, int64_t tms_now ) {
  const vgx_OperationTransaction_t *H;
  if( (H = producer->sequence.head) != NULL ) {
    int64_t staged_tms = __get_stage_timestamp( H );
    // Milliseconds this transaction has been in the staged state (i.e. head)
    return tms_now - staged_tms;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * Return the number of milliseconds between now and when the earliest
 * unconfirmed transaction was created.
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__queue_latency( const vgx_TransactionalProducer_t *producer, int64_t tms_now ) {
  const vgx_OperationTransaction_t *H;
  if( (H = producer->sequence.head) != NULL ) {
    // Milliseconds since the oldest transaction (i.e. head) was created
    return tms_now - H->tms;
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
static vgx_OperationTransactionIterator_t * _transactional_producer__transaction_iterator_init( const vgx_TransactionalProducer_t *producer, vgx_OperationTransactionIterator_t *iterator ) {
  iterator->producer = producer;
  iterator->node = producer->sequence.head;
  return iterator;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static const vgx_OperationTransaction_t * _transactional_producer__transaction_iterator_next( vgx_OperationTransactionIterator_t *iterator ) {
  const vgx_OperationTransaction_t *node = iterator->node;
  if( node ) {
    iterator->node = node->next;
  }
  return node;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__recv_buffer_writable( const vgx_TransactionalProducer_t *producer, int64_t max, char **segment ) {
  return iOpBuffer.WritableSegment( producer->buffer.sysin, max, segment, NULL ); 
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__recv_buffer_advance_write( vgx_TransactionalProducer_t *producer, int64_t amount ) {
  return iOpBuffer.AdvanceWrite( producer->buffer.sysin, amount );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__recv_buffer_readable( const vgx_TransactionalProducer_t *producer, int64_t max, const char **segment ) {
  return iOpBuffer.ReadableSegment( producer->buffer.sysin, max, segment, NULL ); 
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__recv_buffer_advance_read( vgx_TransactionalProducer_t *producer, int64_t amount ) {
  return iOpBuffer.AdvanceRead( producer->buffer.sysin, amount );
}



/*******************************************************************//**
 * Return a string of characters into *data terminated by newline '\n', or
 * up to a maximum of max-1 characters.
 *
 * If a positive integer is returned the newline was found and the return
 * value is the length of the string up to and including the newline. The
 * string will be '\0' terminated in this case.
 * 
 * If 0 is returned no newline character was found in the entire
 * buffer, but characters may have been written to *data (without a '\0'
 * terminator.)
 * 
 * If a negative integer is returned the newline was not found after
 * exceeding the max characters limit, but characters may have been
 * written to *data (without a '\0' terminator.)
 * 
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__recv_buffer_readline( vgx_TransactionalProducer_t *producer, int64_t max, char **data ) {
  int64_t n;
  if( (n = iOpBuffer.ReadUntil( producer->buffer.sysin, max, data, '\n' )) > 0 ) {
    // Auto-confirm what we just read
    iOpBuffer.Confirm( producer->buffer.sysin, n );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __is_response_handleable( vgx_TransactionalProducer_t *producer, const objectid_t *transid ) {
  // Check if resync is currently expected
  if( producer->resync_transaction ) {
    int64_t sn = producer->resync_transaction->serial;
    char buf1[33];
    char buf2[33];
    // Response is handleable if we are waiting for resync and this transaction is the one we are waiting for
    if( idmatch( &producer->resync_transaction->id, transid ) ) {
      producer->resync_transaction = NULL; // clear resync
      producer->resync_remain = 0;
      PRODUCER_INFO( producer, 0x001, "[%s] Success at tx=%s sn=%lld", kwd_RESYNC, idtostr( buf2, transid ), sn );
      return 1;
    }
    // Response is not handleable if we are waiting for resync and this transaction is not the one we are waiting for
    else {
      PRODUCER_WARNING( producer, 0x002, "[%s] Progress at tx=%s sn=%lld, ignoring response for tx=%s", kwd_RESYNC, idtostr( buf1, &producer->resync_transaction->id ), sn, idtostr( buf2, transid ) );
      return 0;
    }
  }

  // Transaction response is handleable
  return 1;
}



/*******************************************************************//**
 *
 *
 *
 * On error, returns errno with negative sign
 ***********************************************************************
 */
static int64_t _transactional_producer__handle_ACCEPTED( vgx_TransactionalProducer_t *producer, const objectid_t *transid, unsigned int crc, int64_t tms ) {
  static const char recv_ACCEPTED[] = "[Recv ACCEPTED]";
  char buf1[33];
  char buf2[33];

  // Ignore if we're currently not handling responses
  if( !__is_response_handleable( producer, transid ) ) {
    return 0;
  }

  vgx_OperationTransaction_t *expected = producer->sequence.head;

  // This is the expected situation: The accepted ID matches the oldest transaction in the opsys transaction producer
  if( expected && idmatch( &expected->id, transid ) ) {
    // Now verify crc
    if( crc == expected->crc ) {
      // Now detach from list
      vgx_OperationTransaction_t *detached = __detach_transaction( producer, expected, tms );
      
      // SANITY CHECK: sysout should always point to the beginning of a transaction (the one we are accepting)
      if( *producer->buffer.sysout->cp != 'T' ) {
        PRODUCER_CRITICAL( producer, 0x001, "%s Transaction output buffer corruption (not aligned on transaction boundary)", recv_ACCEPTED );
        // We must clear the corrupted buffer to get back on track, but data is likely lost.
        iOpBuffer.Clear( producer->buffer.sysout );
        // Discard transaction
        _operation_transaction__delete( &detached );
        return -EINVAL;
      }

      // Confirm the transaction in sysout buffer
      int64_t tsz = detached->tsize;
      int64_t unconfirmed = iOpBuffer.Unconfirmed( producer->buffer.sysout );
      if( tsz > unconfirmed ) {
        PRODUCER_CRITICAL( producer, 0x002, "%s Transaction output buffer corruption @ tx=%s (tsz=%lld > unconfirmed=%lld)", recv_ACCEPTED, idtostr(buf1, transid), tsz, unconfirmed );
        iTransactional.Producer.DumpUnconfirmed( producer );
        iOpBuffer.DumpState( producer->buffer.sysout );
        return -EINVAL;
      }
      else {
        int64_t bsz = iOpBuffer.Confirm( producer->buffer.sysout, tsz );
        if( bsz == tsz ) {
          // Transaction is now confirmed and can be discarded.
          _operation_transaction__delete( &detached );
          // Inc counters
          ++(producer->accepted_transactions);
          producer->accepted_bytes += tsz;
          // Complete. Return the size of the accepted transaction.
          return tsz;
        }
        // ERROR: Transaction size invalid
        else {
          PRODUCER_CRITICAL( producer, 0x003, "%s Size mismatch for tx=%s tsz=%lld confirmable=%lld", recv_ACCEPTED, idtostr( buf1, transid ), tsz, bsz );
          return -EINVAL;
        }
      }
    }
    // ERROR: CRC mismatch
    else {
      PRODUCER_CRITICAL( producer, 0x004, "%s CRC mismatch for tx=%s (got %08X, expected %08X)", recv_ACCEPTED, idtostr( buf1, transid ), crc, expected->crc );
      return -EINVAL;
    }
  }
  // ERROR: Transaction ID does not match head
  else if( expected ) {
    int64_t offset = 0;
    // Got response for an unknown transaction, most likely a double accept of an already confirmed transaction
    if( __get_transaction( producer, transid, &offset ) == NULL ) {
      PRODUCER_WARNING( producer, 0x005, "%s Non-pending tx=%s, next pending tx=%s", recv_ACCEPTED, idtostr( buf1, transid ), idtostr( buf2, &expected->id ) );
      return 0;
    }
    // Out of order
    else {
      PRODUCER_REASON( producer, 0x006, "%s Out of order for tx=%s at buffer offset %lld bytes, next pending tx=%s", recv_ACCEPTED, idtostr( buf1, transid ), offset, idtostr( buf2, &expected->id ) );
      return iTransactional.Producer.Handle.RETRY( producer, &expected->id, 3, tms );
    }
  }
  // No transactions pending
  else {
    PRODUCER_WARNING( producer, 0x007, "%s Non-pending tx=%s, no pending transactions", recv_ACCEPTED, idtostr( buf1, transid ) );
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 *
 * On error, returns errno with negative sign
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t _transactional_producer__handle_REJECTED( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms ) {
  static const char recv_REJECTED[] = "[Recv REJECTED]";
  const char *host;
  if( producer->URI ) {
    host = iURI.Host( producer->URI );
  }

  const char *sreason = NULL;
  switch( reason ) {
  case OP_REJECT_CODE_PROTOCOL_VERSION:
    sreason = "protocol version mismatch";
    break;
  case OP_REJECT_CODE_READONLY:
    sreason = "subscriber readonly";
    break;
  case OP_REJECT_CODE_OVERSIZED_TX_LINE:
    sreason = "oversized transaction line";
    break;
  case OP_REJECT_CODE_OVERSIZED_TX_DATA:
    sreason = "oversized transaction data";
    break;
  case OP_REJECT_CODE_GENERAL:
    sreason = "unspecified";
    break;
  default:
    sreason = "none";
  }
  PRODUCER_WARNING( producer, 0x001, "%s tx=%016llx%016llx reason=%08X (%s)", recv_REJECTED, transid->H, transid->L, reason, sreason );

  char buf1[33];
  char buf2[33];

  // Ignore if we're currently not handling responses
  if( !__is_response_handleable( producer, transid ) ) {
    return 0;
  }

  vgx_OperationTransaction_t *expected = producer->sequence.head;

  // This is the expected situation: The accepted ID matches the oldest transaction in the opsys transaction producer
  if( expected && idmatch( &expected->id, transid ) ) {
    // Now detach from list
    vgx_OperationTransaction_t *detached = __detach_transaction( producer, expected, tms );
    // Confirm (i.e. DISCARD) the transaction in sysout buffer
    int64_t tsz = detached->tsize;
    int64_t discardable = iOpBuffer.Unconfirmed( producer->buffer.sysout );
    int64_t bsz = iOpBuffer.Confirm( producer->buffer.sysout, tsz );
    if( bsz == tsz ) {
      // Transaction is now rejected and will be discarded.
      _operation_transaction__delete( &detached );
      // Return the size of the rejected transaction.
      return tsz;
    }
    // ERROR: Transaction size invalid
    else {
      PRODUCER_CRITICAL( producer, 0x002, "%s Size mismatch for tx=%s tsz=%lld discarded=%lld discardable=%lld", recv_REJECTED, idtostr( buf1, transid ), tsz, bsz, discardable );
      return -EINVAL;
    }
  }
  // ERROR: Transaction ID does not match head
  else if( expected ) {
    int64_t offset = 0;
    // Got response for an unknown transaction, most likely already rejected (or accepted) transaction
    if( __get_transaction( producer, transid, &offset ) == NULL ) {
      PRODUCER_WARNING( producer, 0x003, "%s Non-pending tx=%s, next pending tx=%s", recv_REJECTED, idtostr( buf1, transid ), idtostr( buf2, &expected->id ) );
      return 0;
    }
    // Out of order
    else {
      PRODUCER_REASON( producer, 0x004, "%s Out of order for tx=%s at buffer offset %lld bytes, next pending tx=%s", recv_REJECTED, idtostr( buf1, transid ), offset, idtostr( buf2, &expected->id ) );
      return 0;
    }
  }
  // No transactions pending
  else {
    PRODUCER_WARNING( producer, 0x005, "%s Non-pending tx=%s, no pending transactions", recv_REJECTED, idtostr( buf1, transid ) );
    return 0;
  }

}



/*******************************************************************//**
 * Roll back to the given transaction.
 *
 * On error, returns errno with negative sign
 ***********************************************************************
 */
static int64_t _transactional_producer__handle_RETRY( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms ) {
  static const char recv_RETRY[] = "[Recv RETRY]";
  static const char send_RESYNC[] = "[Send RESYNC]";

  PRODUCER_WARNING( producer, 0x001, "%s tx=%016llx%016llx reason=%08X", recv_RETRY, transid->H, transid->L, reason );

  // Ignore if we're currently not handling responses
  if( !__is_response_handleable( producer, transid ) ) {
    PRODUCER_WARNING( producer, 0x002, "%s during ongoing RESYNC - ignored.", recv_RETRY );
    return 0;
  }

  // Retarget head
  vgx_OperationTransaction_t *target = producer->sequence.head;

  // Roll back to target
  if( target ) {
    
    // Re-stage
    __stage_transaction( target, tms );

    CXSOCKET *psock;
    if( (psock = iURI.Sock.Output.Get( producer->URI )) != NULL ) {
      // Consume all responses and throw them away by reading
      // anything else that may be readable on the socket
      int readable;
      while( (readable = __producer_recv_noblock( producer, psock )) > 0 );
      // I/O Error
      if( readable < 0 ) {
        return readable; // -errno
      }

      // Flush remainder of any partially sent transaction
      int64_t sz_partial_remain = __producer_send_transaction_remainder( producer );
      if( sz_partial_remain < 0 ) {
        PRODUCER_REASON( producer, 0x002, "%s Failed to flush transaction remainder before resync", recv_RETRY );
        return sz_partial_remain; // -errno
      }

      // Write resync marker to socket
      //
      // """
      // 
      // RESYNC <transid> <ndiscard>
      //
      // """
      //
      int64_t n_unconfirmed = iOpBuffer.Unconfirmed( producer->buffer.sysout );
      int64_t n_discard = n_unconfirmed + sz_partial_remain;
      char resync[ 64 ] = {0};
      char *p = resync;
      __writebuf_objectid( &p, KWD_EOM KWD_RESYNC " ", &target->id, NULL );
      __writebuf_qword( &p, SEP, n_discard, KWD_EOM );
      int64_t sz = p - resync;
      int64_t err;
      if( (err = __producer_send_to_socket( producer, resync, sz )) < 0 ) {
        return err; // -errno
      }
      PRODUCER_INFO( producer, 0x003, "%s Begin RESYNC at tx=%016llx%016llx (discard %lld bytes)", send_RESYNC, target->id.H, target->id.L, n_discard );
    }

    // Clear input buffer
    iOpBuffer.Clear( producer->buffer.sysin );

    // SANITY CHECK
    if( *producer->buffer.sysout->cp != 'T' ) {
      PRODUCER_CRITICAL( producer, 0x004, "%s Transaction output buffer corruption (not aligned on transaction boundary)", recv_RETRY );
      // All we can do is clear the corrupted buffer. We will lose data.
      iOpBuffer.Clear( producer->buffer.sysout );
    }

    // Roll output back to beginning of unconfirmed data
    iOpBuffer.Rollback( producer->buffer.sysout );

    // Make note of next expected transaction. All future ACCEPT or RETRY responses
    // are ignored until an ACCEPT (or RETRY) for this transaction has been received.
    producer->resync_transaction = target;
    producer->resync_remain = target->tsize;

    // Suspend transaction output according to reason code
    iTransactional.Producer.Handle.SUSPEND( producer, reason, tms );

    return 0;
  }
  // Empty list!
  else {
    return -EINVAL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__handle_SUSPEND( vgx_TransactionalProducer_t *producer, DWORD reason, int64_t tms ) {
  static const char recv_SUSPEND[] = "[Recv SUSPEND]";
  
  // reason encodes how the suspend should behave
  // 
  //        __ x number of milliseconds requested
  //       | 
  //     __|_ 
  // rrrrmmmmh
  // ____
  //   ^
  //   |
  //    `-- reason
  //        codes:
  //
  //  code   | meaning
  //  -------+-----------
  //  0x0000 | suspend for x milliseconds then auto-resume
  //  0x0001 | suspend until resume is requested
  //
  // 



  vgx_OpSuspendCode code = __suspend_code_from_reason( reason );

  if( code == OP_SUSPEND_CODE_AUTORESUME_TIMEOUT ) {
    int x_ms = __suspend_milliseconds_from_reason( reason );
    int64_t until_ms = tms + x_ms;
    if( producer->suspend_tx_until_tms < until_ms ) {
      producer->suspend_tx_until_tms = until_ms;
#ifdef HASVERBOSE
      if( x_ms > 1000 ) {
        time_t ts = until_ms / 1000;
        struct tm *_tm = localtime( &ts );
        char buf[32] = {0};
        if( _tm ) {
          strftime( buf, 20, "%Y-%m-%d %H:%M:%S", _tm );
        }
        PRODUCER_VERBOSE( producer, 0x001, "%s Resume at [%s.%03lld] %.3f seconds from now", recv_SUSPEND, buf, ts%1000, x_ms/1000.0 );
      }
      else {
        PRODUCER_VERBOSE( producer, 0x001, "%s Suspend %d ms", recv_SUSPEND, x_ms );
      }
#endif
    }
  }
  else if( code == OP_SUSPEND_CODE_INDEFINITE ) {
    producer->suspend_tx_until_tms = LLONG_MAX;
    PRODUCER_WARNING( producer, 0x002, "%s Producer suspended indefinitely", recv_SUSPEND );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t _transactional_producer__handle_RESUME( vgx_TransactionalProducer_t *producer, int64_t tms ) {
  static const char recv_RESUME[] = "[Recv RESUME]";
  producer->suspend_tx_until_tms = 0;
#ifdef HASVERBOSE
  PRODUCER_VERBOSE( producer, 0x001, "%s Producer resumed", recv_RESUME );
#endif
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__handle_ATTACH( vgx_TransactionalProducer_t *producer, DWORD protocol, DWORD version, objectid_t *fingerprint, WORD adminport ) {
  static const char recv_ATTACH[] = "[Recv ATTACH]";
  // TOOD: Proper implementation of protocol and version validation
  DWORD client_protocol = 0x00010000;
  DWORD client_version = 0x00010000;
  DWORD server_protocol = protocol & 0xFFFF0000;
  DWORD server_version = version & 0xFFFF0000;
  if( server_protocol == client_protocol ) { // Client protocol and server protocol must match
    if( server_version >= client_version ) { // Client version cannot be higher than server version
      PRODUCER_INFO( producer, 0x001, "%s Successful producer handshake protocol=%08X version=%08X", recv_ATTACH, protocol, version );
      if( !idmatch( fingerprint, &producer->local.fingerprint ) ) {
        PRODUCER_WARNING( producer, 0x002, "%s Handshake fingerprint mismatch at time of connect", recv_ATTACH );
      }
      producer->subscriber.adminport = adminport;
      return 1;
    }
  }
  PRODUCER_REASON( producer, 0x003, "%s Handshake mismatch protocol=%08X/%08X version=%08X/%08X", recv_ATTACH, protocol, client_protocol, version, client_version );
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t _transactional_producer__handle_DETACH( vgx_TransactionalProducer_t *producer, int64_t tms ) {
  static const char recv_DETACH[] = "[Recv DETACH]";
  static const char send_DETACH[] = "[Send DETACH]";
  PRODUCER_INFO( producer, 0x001, "%s Producer will detach", recv_DETACH );

  CXSOCKET *psock;
  if( (psock = iURI.Sock.Output.Get( producer->URI )) != NULL ) {
    // Consume everything else readable on the socket and throw it away
    while( __producer_recv_noblock( producer, psock ) > 0 );

    // Flush remainder of any partially sent transaction (best effort)
    __producer_send_transaction_remainder( producer );

    // Write 
    //
    // """
    // 
    // DETACH
    //
    // """
    //
    const char detach[] = KWD_EOM KWD_DETACH KWD_EOM;
    __producer_send_to_socket( producer, detach, strlen( detach ) );
    PRODUCER_INFO( producer, 0x002, "%s Confirmed", send_DETACH );
  }

  iTransactional.Producer.Abandon( producer );

  return 1;
}





#define LineStartswith( Line, Char )    ( *(Line) == (Char)  )
#define SkipCount( Cursor, Count )      ( (Cursor) += (Count) )
#define SkipSpaces( Cursor )            while( *(Cursor) == 0x20 ) { ++(Cursor); }



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __producer_parse_ACCEPTED( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // ACCEPTED <transid> <crc32c>\n
  // -------------------------------------------------------------
  // ACCEPTED f0863bfb838b30363d7844fdbc943877 A09D1D50 \n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_ACCEPTED );
  SkipSpaces( cursor )
  // <transid>
  objectid_t transid = strtoid( cursor );
  if( !idnone( &transid ) ) {
    SkipCount( cursor, 32 );
    SkipSpaces( cursor )
    // <crc32c>
    DWORD crc;
    if( hex_to_DWORD( cursor, &crc ) ) {
      // Parsed ok, now complete the transaction
      return iTransactional.Producer.Handle.ACCEPTED( producer, &transid, crc, tms );
    }
    // Invalid <crc>
    else {
      PRODUCER_REASON( producer, 0x001, "Invalid transaction response (bad crc): '%s'", linebuf );
      return -EINVAL;
    }
  }
  // Invalid <transid>
  else {
    PRODUCER_REASON( producer, 0x002, "Invalid transaction response (bad transid): '%s'", linebuf );
    return -EINVAL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __producer_parse_RETRY( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // RETRY <transid> <reason>\n
  // -------------------------------------------------------------
  // RETRY f0863bfb838b30363d7844fdbc943877 00000001 \n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_RETRY );
  SkipSpaces( cursor )
  // <transid>
  objectid_t transid = strtoid( cursor );
  if( !idnone( &transid ) ) {
    SkipCount( cursor, 32 );
    SkipSpaces( cursor )
    // <reason>
    DWORD reason;
    if( hex_to_DWORD( cursor, &reason ) ) {
      // Parsed ok, now prepare to retry
      return iTransactional.Producer.Handle.RETRY( producer, &transid, reason, tms );
    }
    // Invalid <reason>
    else {
      PRODUCER_REASON( producer, 0x001, "Invalid transaction response (bad reason): '%s'", linebuf );
      return -EINVAL;
    }
  }
  // Invalid <transid>
  else {
    PRODUCER_REASON( producer, 0x002, "Invalid transaction response (bad transid): '%s'", linebuf );
    return -EINVAL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_REJECTED( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // REJECTED <transid> <reason>\n
  // -------------------------------------------------------------
  // REJECTED f0863bfb838b30363d7844fdbc943877 00000001 \n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_REJECTED );
  SkipSpaces( cursor )
  // <transid>
  objectid_t transid = strtoid( cursor );
  if( !idnone( &transid ) ) {
    SkipCount( cursor, 32 );
    SkipSpaces( cursor )
    // <reason>
    DWORD reason;
    if( hex_to_DWORD( cursor, &reason ) ) {
      return iTransactional.Producer.Handle.REJECTED( producer, &transid, reason, tms );
    }
    // Invalid <reason>
    else {
      PRODUCER_REASON( producer, 0x001, "Invalid transaction response (bad reason): '%s'", linebuf );
      return -EINVAL;
    }
  }
  // Invalid <transid>
  else {
    PRODUCER_REASON( producer, 0x002, "Invalid transaction response (bad transid): '%s'", linebuf );
    return -EINVAL;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_IDLE( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // IDLE <tms> <fingerprint> <master_serial>\n
  // -------------------------------------------------------------
  // IDLE 00000178131E0F18 0348a020e49760fc382f1569552eff5f 0123456789ABCDEF\n
  const char *cursor = linebuf;
  SkipCount( cursor, sz_IDLE );
  SkipSpaces( cursor )
  // <tms>
  QWORD idle_tms;
  if( hex_to_QWORD( cursor, &idle_tms ) == NULL ) {
    PRODUCER_REASON( producer, 0x001, "Invalid timestamp: '%s'", linebuf );
    return -EINVAL;
  }
  SkipCount( cursor, 16 );
  SkipSpaces( cursor );

  // <fingerprint>
  producer->subscriber.fingerprint = strtoid( cursor );
  if( idnone( &producer->subscriber.fingerprint ) ) {
    PRODUCER_REASON( producer, 0x002, "Invalid fingerprint: '%s'", linebuf );
    return -EINVAL;
  }
  SkipCount( cursor, 32 );
  SkipSpaces( cursor );

  // <master_serial>
  QWORD master_serial;
  if( hex_to_QWORD( cursor, &master_serial ) == NULL ) {
    PRODUCER_REASON( producer, 0x003, "Invalid master_serial: '%s'", linebuf );
    return -EINVAL;
  }
  producer->subscriber.master_serial = master_serial;

  // Return delta between now and the response tms
  int64_t delta = tms - idle_tms;
  if( delta < 0 ) {
    delta = 0;
  }
  return delta;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_SUSPEND( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // SUSPEND <reason>\n
  // -------------------------------------------------------------
  // SUSPEND 00000001 \n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_SUSPEND );
  SkipSpaces( cursor )
  // <reason>
  DWORD reason;
  if( hex_to_DWORD( cursor, &reason ) ) {
    return iTransactional.Producer.Handle.SUSPEND( producer, reason, tms );
  }
  // Invalid <reason>
  else {
    return -EINVAL;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_RESUME( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // RESUME\n
  // -------------------------------------------------------------
  // RESUME\n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_RESUME );
  return iTransactional.Producer.Handle.RESUME( producer, tms );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_ATTACH( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // ATTACH <protocol> <version> <fingerprint> <adminport>\n
  // -------------------------------------------------------------
  // ATTACH 00010000 00010000 0348a020e49760fc382f1569552eff5f 0x2329\n
  const char *cursor = linebuf;
  SkipCount( cursor, sz_ATTACH );
  SkipSpaces( cursor )
  // <protocol>
  DWORD protocol;
  if( hex_to_DWORD( cursor, &protocol ) == NULL ) {
    return -EINVAL;
  }

  SkipCount( cursor, 8 );
  SkipSpaces( cursor )
  // <version>
  DWORD version;
  if( hex_to_DWORD( cursor, &version ) == NULL ) {
    return -EINVAL;
  }

  SkipCount( cursor, 8 );
  SkipSpaces( cursor )
  // <fingerprint>
  objectid_t fingerprint = strtoid( cursor );
  if( idnone( &fingerprint ) ) {
    return -EINVAL;
  }

  SkipCount( cursor, 32 );
  SkipSpaces( cursor )
  // <adminport>
  WORD adminport;
  if( hex_to_WORD( cursor, &adminport ) == NULL ) {
    return -EINVAL;
  }

  return iTransactional.Producer.Handle.ATTACH( producer, protocol, version, &fingerprint, adminport );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __producer_parse_DETACH( vgx_TransactionalProducer_t *producer, const char *linebuf, int64_t tms ) {
  // DETACH\n
  // -------------------------------------------------------------
  // DETACH\n

  const char *cursor = linebuf;
  SkipCount( cursor, sz_DETACH );
  return iTransactional.Producer.Handle.DETACH( producer, tms );

}



/*******************************************************************//**
 *
 *
 * Return < 0 on error (errno with negative sign)
 *        >=0 on success
 ***********************************************************************
 */
static int64_t __producer_recv_parse( vgx_TransactionalProducer_t *producer, int64_t tms ) {
#define SZ_LINEBUF 1024
  char linebuf[ SZ_LINEBUF ];

  int64_t ret = 0;

  int64_t sz_line = 0;
  char *dest = linebuf; 
  *dest = '\0'; // good practice

  // Get the next complete line (terminated by '\n')
  while( ret >= 0 && (sz_line = _transactional_producer__recv_buffer_readline( producer, SZ_LINEBUF, &dest )) > 0 ) { 
    //
    // "ACCEPTED"
    //
    if( CharsStartsWithConst( linebuf, kwd_ACCEPTED ) ) {
      ret = __producer_parse_ACCEPTED( producer, linebuf, tms );
    }
    //
    // "IDLE"
    //
    else if( CharsStartsWithConst( linebuf, kwd_IDLE ) ) {
      ret = __producer_parse_IDLE( producer, linebuf, tms );
    }
    //
    // "RETRY"
    //
    else if( CharsStartsWithConst( linebuf, kwd_RETRY ) ) {
      ret = __producer_parse_RETRY( producer, linebuf, tms );
    }
    //
    // "REJECTED"
    //
    else if( CharsStartsWithConst( linebuf, kwd_REJECTED ) ) {
      if( (ret = __producer_parse_REJECTED( producer, linebuf, tms )) > 0 ) {
        PRODUCER_WARNING( producer, 0x001, "REJECTED transaction is a non-recoverable event" );
        producer->flags.defunct = true;
      }
    }
    //
    // "SUSPEND"
    //
    else if( CharsStartsWithConst( linebuf, kwd_SUSPEND ) ) {
      ret = __producer_parse_SUSPEND( producer, linebuf, tms );
    }
    //
    // "RESUME"
    //
    else if( CharsStartsWithConst( linebuf, kwd_RESUME ) ) {
      ret = __producer_parse_RESUME( producer, linebuf, tms );
    }
    //
    // "ATTACH"
    //
    else if( CharsStartsWithConst( linebuf, kwd_ATTACH ) ) {
      ret = __producer_parse_ATTACH( producer, linebuf, tms );
    }
    //
    // "DETACH"
    //
    else if( CharsStartsWithConst( linebuf, kwd_DETACH ) ) {
      ret = __producer_parse_DETACH( producer, linebuf, tms );
    }
    //
    // Silently ignore comments
    //
    else if( LineStartswith( linebuf, '#' ) ) {
    }
    //
    // Silently ignore extra newlines
    //
    else if( LineStartswith( linebuf, '\n' ) ) {
    }
    //
    // Otherwise log the unknown response
    //
    else {
      PRODUCER_WARNING( producer, 0x002, "Ignored transaction response: '%s'", linebuf );
    }

  }

  return ret;
}



/*******************************************************************//**
 *
 * Returns  >=0 number of bytes sent
 *           <0 errno with negative sign
 ***********************************************************************
 */
static int64_t __producer_send_to_socket( vgx_TransactionalProducer_t *producer, const char *data, int64_t sz ) {
  CXSOCKET *psock = iURI.Sock.Output.Get( producer->URI );
  if( psock == NULL ) {
    vgx_Graph_t *agent = producer->graph;
    int64_t tms = _vgx_graph_milliseconds( agent );
    int err;
    if( (err = iTransactional.Producer.Reconnect( producer, tms, 2000 )) < 0 ) {
      return err;
    }
    if( (psock = iURI.Sock.Output.Get( producer->URI )) == NULL ) {
      return -EBADF;
    }
  }

  const char *cursor = data;
  int64_t remain = sz;

  do {
    // Try to send remain bytes
    int64_t nsent = cxsend( psock, cursor, remain, 0 );
    // One or more bytes sent
    if( nsent > 0 ) {
#ifdef OPSTREAM_DUMP_TX_IO
    __dump_tx_request_sent( cursor, nsent );
#endif
      remain -= nsent;
      cursor += nsent;
    }
    // Socket busy, we will retry
    else {
      int err = errno;
      if( nsent < 0 && iURI.Sock.Busy( err ) ) {
        TRANSACTIONAL_SUSPEND_LOCK( producer ) {
          sleep_milliseconds( 10 );
        } TRANSACTIONAL_RESUME_LOCK;
      }
      // Give up
      else {
        PRODUCER_REASON( producer, 0x001, "I/O ERROR: %lld bytes could not be sent", sz );
        if( err ) {
          return -err;
        }
        else {
          return -EBADF;
        }
      }
    }
    // Keep going as long as bytes remain
  } while( remain > 0 );

  // All data sent
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t  __producer_perform_attach_handshake( vgx_TransactionalProducer_t *producer ) {
  // Consume anything readable on the socket and throw it away
  CXSOCKET *psock = iURI.Sock.Output.Get( producer->URI );
  while( __producer_recv_noblock( producer, psock ) > 0 );

  char buf[128], *b = buf;
  // ATTACH 00010000 00010000 0348a020e49760fc382f1569552eff5f 1234 \n\n
  DWORD protocol = 0x00010000;
  DWORD version = 0x00010000;
  WORD adminport = 0;
  TRANSACTIONAL_SUSPEND_LOCK( producer ) {
    vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
    if( SYSTEM ) {
      GRAPH_LOCK( SYSTEM ) {
        int p = iVGXServer.Service.GetAdminPort( SYSTEM );
        if( p > 0 && p < 0x10000 ) {
          adminport = (WORD)p;
        }
      } GRAPH_RELEASE;
    }
    producer->local.fingerprint = igraphfactory.Fingerprint();
  } TRANSACTIONAL_RESUME_LOCK;

  b = __writebuf_dword( &b, KWD_ATTACH KWD_SEP, protocol, NULL );
  b = __writebuf_dword( &b, SEP, version, NULL );
  b = __writebuf_objectid( &b, SEP, &producer->local.fingerprint, NULL );
  b = __writebuf_word( &b, SEP, adminport, NULL );
  b = __writebuf_string( &b, KWD_EOM );
  // Output
  int64_t sz = b - buf;
  *b = '\0';
  __producer_send_to_socket( producer, buf, sz );

  // Polite pause 1 sec before checking response
  sleep_milliseconds( 1000 );

  // Check response only if we are configured to expect a full handshake
  if( producer->flags.handshake ) {
    // Wait for server to respond with something, max 10 sec
    int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
    int ret;
    while( (ret = __producer_recv_noblock( producer, psock )) < 1 ) {
      int64_t t1 = __GET_CURRENT_MILLISECOND_TICK();
      // Not readable and no response yet
      if( ret == 0 && t1 - t0 < 10000 ) {
        sleep_milliseconds( 333 );
      }
      // Either timeout or socket error
      else {
        return -1;
      }
    }

    // Something has been read from socket. Assume the full response line is available to read,
    // which is normally the case. If not we will fail and the handshake attempt must be repeated.
    return __producer_recv_parse( producer, 0 );
  }
  // Continue without expecting response
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 * Returns  1 : All data sent and socket still writable
 *          0 : Less than all data sent and socket not writable at the moment
 *         <0 : Less than all data sent and socket I/O error (errno with negative sign)
 *
 ***********************************************************************
 */
static int __producer_send_noblock( vgx_TransactionalProducer_t *producer, CXSOCKET *psock, int64_t tms ) {
#define __SEND_MAX (1 << 17)
  int64_t sz_max = __SEND_MAX;

  // We are resync'ing. 
  if( producer->resync_transaction ) {

    // Wait for explicit RESUME before trying the resync if currently suspended indefinitely
    if( producer->suspend_tx_until_tms == LLONG_MAX ) {
      return 0;
    }

    // Any suspend period is automatically cancelled if not infinite
    producer->suspend_tx_until_tms = 0;

    // Data should be sent
    if( producer->resync_remain > 0 ) {
      sz_max = producer->resync_remain;
    }
    // We should not send more data until resync is confirmed
    else {
      return 0;
    }


  }
  // Sending is temporarily suspended
  else if( tms < producer->suspend_tx_until_tms ) {
    return 0;
  }


  int64_t sz_segment;
  const char *segment;

  while( (sz_segment = iOpBuffer.ReadableSegment( producer->buffer.sysout, sz_max, &segment, NULL )) > 0 ) {
    // Send chunk
    //
    // TODO: Consider setting flag MSG_MORE if remain > __send_max
    //

    // AMBD-6266:
    // Transaction commit ends with two newlines after the commit. We manage to send the transaction below but without the
    // second newline. Consumer responds with accepted because it only requires one newline. We receive the accepted and
    // try to confirm the size of the transaction, which is one byte larger than what we are able to confirm since we didn't
    // manage to send that second new line. Hence we trigger a buffer corruption error.
    // Solution: 
    
    // SEND TO SOCKET
    int64_t nsent = cxsend( psock, segment, sz_segment, 0 );

    // At least some of the data was sent
    if( nsent > 0 ) {
#ifdef OPSTREAM_DUMP_TX_IO
    __dump_tx_request_sent( segment, nsent );
#endif
      // Advance the buffer's read pointer by amount actually sent (which may be less than the full segment)
      iOpBuffer.AdvanceRead( producer->buffer.sysout, nsent );

      // Update exchange timestamp
      producer->exchange_tms = tms;

      // Update remaining size if we are resyncing
      if( producer->resync_transaction && producer->resync_remain > 0 ) {
        producer->resync_remain -= nsent;
        // Entire resync transaction has now been sent
        if( (sz_max = producer->resync_remain) <= 0 ) {
          producer->resync_remain = 0;
          return 1;
        }
      }

    }
    // Unable to send right now... (some data may have been sent previously in the loop)
    else if( nsent < 0 ) {
      int err = errno;
      // ...because socket is temporarily non-writable.
      if( iURI.Sock.Busy( err ) ) {
        return 0; // temporarily non-writable
      }
      // ...because of socket error (disconnected or other I/O error)
      else if( err ) {
        return -err; // I/O error
      }
      else {
        return -EBADF;
      }
    }
    // send() returned 0
    else {
      return 0;
    }
  }
  return 1;
}



/*******************************************************************//**
 *
 * Returns  1 : One or more bytes read into sysin, all readable input exhausted
 *          0 : Nothing read from socket
 *         <0 : error (errno with negative sign)
 *
 ***********************************************************************
 */
static int __producer_recv_noblock( vgx_TransactionalProducer_t *producer, CXSOCKET *psock ) {
#define __RECV_MAX (1 << 10)
  
  int readable = 0;
  char *segment;
  int64_t sz_segment;
  int64_t nrecv = 0;
  // Receive as much data as we can until socket is no longer readable
  while( (sz_segment = iTransactional.Producer.Receive.Writable( producer, __RECV_MAX, &segment )) > 0
         &&
         (nrecv = cxrecv( psock, segment, sz_segment, 0 )) > 0 )
  {
#ifdef OPSTREAM_DUMP_TX_IO
    __dump_tx_response_recv( segment, nrecv );
#endif
    readable = 1;
    if( iTransactional.Producer.Receive.AdvanceWrite( producer, nrecv ) != nrecv ) {
      // TODO: More robust handling
      PRODUCER_CRITICAL( producer, 0x001, "Response data lost!" );
      return -1; // internal error
    }
  }

  // No longer readable
  if( nrecv < 0 ) {
    int err = errno;
    // ...because there is no data to read at the moment
    if( iURI.Sock.Busy( err ) ) {
      return readable; // 1 if anything was read, 0 if nothing was read
    }
    // ... because of socket error (disconnected or other I/O error)
    else if( err ) {
      return -err; // I/O error
    }
    else {
      return -EBADF;
    }
  }
  // Socket was gracefully closed
  else {
    return readable; // 1 if anything was read, 0 if nothing was read
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __producer_write_file( vgx_TransactionalProducer_t *producer, int fd ) {
#define __WRITE_MAX (1 << 20)
  int64_t sz_segment;
  const char *segment;
  while( (sz_segment = iOpBuffer.ReadableSegment( producer->buffer.sysout, __WRITE_MAX, &segment, NULL )) > 0 ) {
    // CX_WRITE TO FILE
    int64_t nwritten = CX_WRITE( segment, 1, sz_segment, fd );
    // At least some of the data was written
    if( nwritten > 0 ) {
      // Advance and confirm
      iOpBuffer.AdvanceRead( producer->buffer.sysout, nwritten );
      iOpBuffer.Confirm( producer->buffer.sysout, nwritten );
    }
  }
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __producer_write_null( vgx_TransactionalProducer_t *producer ) {
  int64_t sz_segment;
  const char *segment;
  while( (sz_segment = iOpBuffer.ReadableSegment( producer->buffer.sysout, LLONG_MAX, &segment, NULL )) > 0 ) {
    // Advance and confirm
    iOpBuffer.AdvanceRead( producer->buffer.sysout, sz_segment );
    iOpBuffer.Confirm( producer->buffer.sysout, sz_segment );
  }
  return 1;
}



/*******************************************************************//**
 * 
 *  
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __producer_idle( vgx_TransactionalProducer_t *producer, int64_t tms ) {
  // IDLE <tms> <fingerprint>\n
  // --------------------------------------------------------
  // IDLE 00000178131E0F18 0348a020e49760fc382f1569552eff5f\n
#ifndef NOPSTREAM_IDLE
  if( iOpBuffer.Readable( producer->buffer.sysout ) > 0 ) {
    return 0;
  }
  // idle messages bypass the transaction output buffer
  char idle[64] = {0}, *p = idle;
  __writebuf_qword( &p, KWD_EOM KWD_IDLE KWD_SEP, tms, NULL );
  __writebuf_objectid( &p, SEP, &producer->local.fingerprint, KWD_EOM );
  int64_t sz = p - idle;
  int64_t sent = __producer_send_to_socket( producer, idle, sz );
  return (int)sent;
#else
  return 1;
#endif
}



/*******************************************************************//**
 * 
 *  
 ***********************************************************************
 */
static int64_t __producer_send_transaction_remainder( vgx_TransactionalProducer_t *producer ) {
  // Flush enough data in buffer to the end of the current transaction to
  // avoid triggering a syntax error in the server. It's easy for us to
  // do this here and makes it easier for the server to recover.
  // Best effort only.
  int64_t n_sent = iOpBuffer.Unconfirmed( producer->buffer.sysout );
  int64_t tsize_sum = 0;
  vgx_OperationTransaction_t *node = producer->sequence.head;
  while( node ) {
    tsize_sum += node->tsize;
    int64_t sz_trans_tail = tsize_sum - n_sent;
    if( sz_trans_tail > 0 ) {
      char *tail = malloc( sz_trans_tail + 1 );
      if( tail == NULL ) {
        return -ENOMEM;
      }
      iOpBuffer.ReadUntil( producer->buffer.sysout, sz_trans_tail+1, &tail, '\0' );
      int64_t sent = __producer_send_to_socket( producer, tail, sz_trans_tail );
      free( tail );
      if( sent < 0 ) {
        return sent; // -errno
      }
      // Comment
      char buf[64] = {0};
      char *p = buf;
      __writebuf_qword( &p, "# FLUSH ", sz_trans_tail, KWD_EOM );
      int64_t sz_comment = p - buf;
      if( (sent = __producer_send_to_socket( producer, buf, sz_comment )) < 0 ) {
        return sent; // -errno
      }
      // Total sent
      return sz_trans_tail + sz_comment;
    }
    node = node->next;
  }

  return 0;
}



/*******************************************************************//**
 * Returns    0 : success 
 *           -1 : error 
 * 
 *  
 ***********************************************************************
 */
static int _transactional_producer__perform_exchange( vgx_TransactionalProducer_t *producer, int64_t tms ) {
  // Not attached to remote end, discard all data
  if( producer->URI == NULL || producer->flags.defunct ) {
    iTransactional.Producer.Purge( producer );
    return 0;
  }
  // Attached, expect to be able to perform IO
  else {
    CXSOCKET *psock;
    int fd;

    // We have a socket, perform socket IO
    if( (psock = iURI.Sock.Output.Get( producer->URI )) != NULL ) {

      int writable = 0;
      int readable = 0;
      int64_t parsed = 0;

      // **************************************************************************************
      //
      // SEND/RECV LOOP
      //
      // Write as much as we can to the socket, and read as much as we can from the socket.
      // Stay in loop as long as we have data to write and the socket is writable, or the
      // socket is readable. When nothing can be written and nothing can be read we exit loop.
      // We also exit loop on any socket error.
      // **************************************************************************************

      int err = 0;
      
      do {
        // ------------------------------------
        // Send idle message at regular intervals
        // ------------------------------------
        if( tms - producer->local.state_tms > 7000 ) {
          int n = __producer_idle( producer, tms );
          if( n > 0 ) {
            producer->local.state_tms = tms;
          }
          else {
            producer->local.state_tms = tms - 6000;
            if( n < 0 ) {
              err = -n;
              break;
            }
          }
        }

        // ------------------------------------
        // Send any remaining data non-blocking
        // ------------------------------------
        if( iOpBuffer.Readable( producer->buffer.sysout ) > 0 ) {
          // Send data unless output is muted
          if( producer->flags.muted == false ) {
            // Send as much as possible to the socket
            if( (writable = __producer_send_noblock( producer, psock, tms )) < 0 ) {
              err = -writable;
              break;
            }
          }
        }

        // ------------------------------------
        // Receive any available data
        // ------------------------------------
        if( (readable = __producer_recv_noblock( producer, psock )) < 0 ) {
          err = -readable;
          break;
        }

        // Repeat I/O loop as long as 
        //   the socket is writable and unsent data remains
        //    OR 
        //   the socket is readable
        // Loop exits when no more I/O is immediately possible 
      } while( (writable > 0 && iOpBuffer.Readable( producer->buffer.sysout ) > 0) || readable > 0 );


      // ********************************************************
      //
      // HANDLE ALL RECEIVED RESPONSES
      //
      // ********************************************************
      if( (parsed = __producer_recv_parse( producer, tms )) < 0 ) {
        PRODUCER_REASON( producer, 0x001, "Server response error" );
        if( err == 0 ) {
          err = -(int)parsed;
        }
      }

      // Socket had error and was closed. All current unconfirmed transactions will be
      // purged. It is possible that some or all of the purged transactions have been processed
      // successfully by the remote end but no response has been received.
      //
      if( err != 0 ) {
        // Log error and close socket
        iURI.Sock.Error( producer->URI, NULL, err, 0 );
        return -1;
      }
      else if( producer->flags.defunct ) {
        PRODUCER_WARNING( producer, 0x002, "Defunct producer" );
        iTransactional.Producer.Disconnect( producer );
      }
    }
    // We have a file, perform file IO
    else if( (fd = iURI.File.GetDescriptor( producer->URI )) != 0 ) {
      __producer_write_file( producer, fd );
    }
    // We have a null output
    else if( iURI.File.IsNullOutput( producer->URI ) ) {
      __producer_write_null( producer );
    }
    else {
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producer__perform_rollback( vgx_TransactionalProducer_t *producer, int64_t tms_now ) {
  // Roll output back to prepare for resending all unconfirmed
  int64_t n_rollback = iOpBuffer.Rollback( producer->buffer.sysout );
#ifdef HASVERBOSE
  // Amount of data in buffer after rollback
  int64_t n_readable = iOpBuffer.Readable( producer->buffer.sysout );
  PRODUCER_VERBOSE( producer, 0x001, "Rolled back %lld unconfirmed bytes, now %lld bytes in output buffer.", n_rollback, n_readable );

  // Clear input buffer
  PRODUCER_VERBOSE( producer, 0x002, "Discarding %lld bytes from input buffer", iOpBuffer.Readable( producer->buffer.sysin ) );
#endif
  iOpBuffer.Clear( producer->buffer.sysin );

  // Close connection, will force system agent to reconnect
  if( iURI.IsConnected( producer->URI ) ) {
    iURI.Close( producer->URI );
    producer->connection_ts.down = (uint32_t)(tms_now / 1000);
  }
  // Clear resync since we closed the connection
  producer->resync_transaction = NULL;
  producer->resync_remain = 0;

#ifdef HASVERBOSE
  PRODUCER_VERBOSE( producer, 0x003, "Connection closed after rollback" );
#endif

  // Brief pause
  TRANSACTIONAL_SUSPEND_LOCK( producer ) {
    sleep_milliseconds( 500 );
  } TRANSACTIONAL_RESUME_LOCK;
  
  // Amount of data in buffer after brief pause
  int64_t n_readable2 = iOpBuffer.Readable( producer->buffer.sysout );
  
  PRODUCER_INFO( producer, 0x004, "Transaction rollback: %lld bytes, now %lld bytes in output buffer.", n_rollback, n_readable2 );
  return n_rollback;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_OperationTransaction_t * _operation_transaction__new( vgx_TransactionalProducer_t *producer, int64_t tms ) {

  vgx_OperationTransaction_t *transaction = NULL;

  XTRY {
    // Allocate object
    if( (transaction = calloc( 1, sizeof( vgx_OperationTransaction_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Creation timestamp
    transaction->tms = tms;

    // Assign transaction ID and serial number
    __next_transaction_id( producer, transaction );

    // Reset crc
    transaction->crc = 0;

    // Reset stage time delta
    transaction->stage_dms = 0;

    // Reset size
    transaction->tsize = 0;

    // Not part of a list
    transaction->next = NULL;
    transaction->prev = NULL;

  }
  XCATCH( errcode ) {
    _operation_transaction__delete( &transaction );
  }
  XFINALLY {
  }

  return transaction;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _operation_transaction__delete( vgx_OperationTransaction_t **transaction ) {
  if( transaction && *transaction ) {
    free( *transaction );
    *transaction = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _transactional_producer__dump_state( const vgx_TransactionalProducer_t *producer, const objectid_t *focus ) {
  vgx_OperationTransactionIterator_t iter;
  iTransactional.Producer.Transaction.Iterator.Init( producer, &iter );
  const vgx_OperationTransaction_t *node;
  char tid[33];

  vgx_OperationBuffer_t *sysout = producer->buffer.sysout;
  vgx_OperationBuffer_t *sysin = producer->buffer.sysin;

  const char *uri = producer->URI ? iURI.URI( producer->URI ) : "?";

  CString_t *CSTR__connect_time = igraphinfo.CTime( producer->connection_ts.attempt*1000LL, true );
  CString_t *CSTR__disconnect_time = igraphinfo.CTime( producer->connection_ts.down*1000LL, true );
  const char *sconnect = CSTR__connect_time ? CStringValue( CSTR__connect_time ) : "?";
  const char *sdisconnect = CSTR__disconnect_time ? CStringValue( CSTR__disconnect_time ) : "?";

  BEGIN_CXLIB_OBJ_DUMP( vgx_TransactionalProducer_t, producer ) {
    CXLIB_OSTREAM( "" );
    CXLIB_OSTREAM( "  URI            : %s", uri );
    CXLIB_OSTREAM( "  Connected at   : %s", sconnect );
    CXLIB_OSTREAM( "  Disconnected at: %s", sdisconnect );
    CXLIB_OSTREAM( "  Pending        : %lld (%lld bytes)", producer->sequence.length, producer->sequence.bytes );
    CXLIB_OSTREAM( "  Accepted       : %lld (%lld bytes)", producer->accepted_transactions, producer->accepted_bytes );
    CXLIB_OSTREAM( "  Sysout" );
    CXLIB_OSTREAM( "     name        : %s",   iOpBuffer.Name( sysout, NULL ) );
    CXLIB_OSTREAM( "     capacity    : %lld", iOpBuffer.Capacity( sysout ) );
    CXLIB_OSTREAM( "     used        : %lld", iOpBuffer.Size( sysout ) );
    CXLIB_OSTREAM( "     remaining   : %lld", iOpBuffer.Writable( sysout ) );
    CXLIB_OSTREAM( "     readable    : %lld", iOpBuffer.Readable( sysout ) );
    CXLIB_OSTREAM( "     unconfirmed : %lld", iOpBuffer.Unconfirmed( sysout ) );
    CXLIB_OSTREAM( "  Sysin" );
    CXLIB_OSTREAM( "     name        : %s",   iOpBuffer.Name( sysin, NULL ) );
    CXLIB_OSTREAM( "     capacity    : %lld", iOpBuffer.Capacity( sysin ) );
    CXLIB_OSTREAM( "     used        : %lld", iOpBuffer.Size( sysin ) );
    CXLIB_OSTREAM( "     remaining   : %lld", iOpBuffer.Writable( sysin ) );
    CXLIB_OSTREAM( "     readable    : %lld", iOpBuffer.Readable( sysin ) );
    CXLIB_OSTREAM( "     unconfirmed : %lld", iOpBuffer.Unconfirmed( sysin ) );
    CXLIB_OSTREAM( "" );
    CXLIB_OSTREAM( "  -------------------------------- ------------------- -------- ---------- ----------" );
    CXLIB_OSTREAM( "  Transaction ID                   Serial              CRC32    Size       Cumulative" );
    CXLIB_OSTREAM( "  -------------------------------- ------------------- -------- ---------- ----------" );
    const char spaces[] = "  ";
    const char arrow[] = "->";
    const char *sfocus;
    int64_t cumsz = 0;
    int found = 0;
    while( (node = iTransactional.Producer.Transaction.Iterator.Next( &iter ) ) != NULL ) {
      if( focus && idmatch( focus, &node->id ) ) {
        sfocus = arrow;
        ++found;
      }
      else {
        sfocus = spaces;
      }
      cumsz += node->tsize;
      CXLIB_OSTREAM( "%s%s %019lld %08X %10lld %10lld", sfocus, idtostr( tid, &node->id ), node->serial, node->crc, node->tsize, cumsz );
    }
    CXLIB_OSTREAM( "  -----------------------------------------------------------------------------------" );
    if( focus ) {
      if( found == 0 ) {
        CXLIB_OSTREAM( "  Searched ID %s not found!", idtostr( tid, focus ) );
      }
      else if( found > 1 ) {
        CXLIB_OSTREAM( "  Searched ID %s has %lld duplicates in list!", idtostr( tid, focus ), found );
      }
    }
    CXLIB_OSTREAM( "" );

  } END_CXLIB_OBJ_DUMP;

  iString.Discard( &CSTR__connect_time );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _transactional_producer__dump_unconfirmed( const vgx_TransactionalProducer_t *producer ) {
  char idbuf[33];
  char *buffer = NULL;
  int64_t sz_buf;
  if( (sz_buf = iOpBuffer.GetUnconfirmed( producer->buffer.sysout, &buffer )) < 0 ) {
    PRODUCER_REASON( producer, 0x000, "Memory error" );
  }
  else {
    BEGIN_CXLIB_OBJ_DUMP( vgx_TransactionalProducer_t, producer ) {
      CXLIB_OSTREAM( "sequence.head         : %llp", producer->sequence.head );
      CXLIB_OSTREAM( "sequence.tail         : %llp", producer->sequence.tail );
      CXLIB_OSTREAM( "sequence.length       : %lld", producer->sequence.length );
      CXLIB_OSTREAM( "sequence.bytes        : %lld", producer->sequence.bytes );
      if( producer->buffer.sysout ) {
      CXLIB_OSTREAM( "buffer.sysout         : unconfirmed=%lld readable=%lld writable=%lld", iOpBuffer.Unconfirmed( producer->buffer.sysout ), iOpBuffer.Readable( producer->buffer.sysout ), iOpBuffer.Writable( producer->buffer.sysout ) );
      }
      if( producer->buffer.sysin ) {
      CXLIB_OSTREAM( "buffer.sysin          : unconfirmed=%lld readable=%lld writable=%lld", iOpBuffer.Unconfirmed( producer->buffer.sysin ), iOpBuffer.Readable( producer->buffer.sysin ), iOpBuffer.Writable( producer->buffer.sysin ) );
      }
      CXLIB_OSTREAM( "uri                   : %s", producer->URI ? iURI.URI( producer->URI ) : "?" );
      CXLIB_OSTREAM( "connection_ts.attempt : %u", producer->connection_ts.attempt );
      CXLIB_OSTREAM( "connection_ts.down    : %u", producer->connection_ts.down );
      CXLIB_OSTREAM( "graph                 : %llp", producer->graph );
      CXLIB_OSTREAM( "accepted_transactions : %lld", producer->accepted_transactions );
      CXLIB_OSTREAM( "accepted_bytes        : %lld", producer->accepted_bytes );
      CXLIB_OSTREAM( "tid_salt              : %llu %llX", producer->tid_salt, producer->tid_salt );
      CXLIB_OSTREAM( "serial0               : %lld %llX", producer->serial0, producer->serial0 );
      CXLIB_OSTREAM( "sn                    : %lld %llX", producer->sn, producer->sn );
      CXLIB_OSTREAM( "exchange_tms          : %lld", producer->exchange_tms );
      CXLIB_OSTREAM( "suspended_tx_until_tms: %lld", producer->suspend_tx_until_tms );
      CXLIB_OSTREAM( "lock_count            : %d", producer->lock_count  );
      CXLIB_OSTREAM( "flags.confirmable     : %d", (int)producer->flags.confirmable );
      CXLIB_OSTREAM( "flags.defunct         : %d", (int)producer->flags.defunct );
      CXLIB_OSTREAM( "flags.abandoned       : %d", (int)producer->flags.abandoned );
      CXLIB_OSTREAM( "flags.handshake       : %d", (int)producer->flags.handshake );
      CXLIB_OSTREAM( "flags.muted           : %d", (int)producer->flags.muted );
      CXLIB_OSTREAM( "flags.mode            : %d", (int)producer->flags.mode );
      CXLIB_OSTREAM( "resync_transaction    : %llp", producer->resync_transaction );
      CXLIB_OSTREAM( "resync_remain         : %lld", producer->resync_remain );
      CXLIB_OSTREAM( "local.fingerprint     : %s", idtostr( idbuf, &producer->local.fingerprint ) );
      CXLIB_OSTREAM( "local.state_tms       : %lld", producer->local.state_tms );
      CXLIB_OSTREAM( "subscriber.fingerprint: %s", idtostr( idbuf, &producer->subscriber.fingerprint ) );
      CXLIB_OSTREAM( "subscriber.adminport  : %u", producer->subscriber.adminport );
      CXLIB_OSTREAM( "BEGIN UNCONFIRMED DATA: %lld", sz_buf );
      CXLIB_OSTREAM( ">>>>>>>>>>>>>>>>>>>>>>" );
      if( sz_buf < 8192 ) {
        CXLIB_OSTREAM( "%s", buffer );
      }
      else {
        buffer[4000] = '\0';
        const char *head = buffer;
        const char *end = buffer + sz_buf;
        const char *tail = end - 4000;
        CXLIB_OSTREAM( "%s", head );
        CXLIB_OSTREAM( ".....(%lld bytes).....", sz_buf-8000 );
        CXLIB_OSTREAM( "%s", tail );
      }
      CXLIB_OSTREAM( "<<<<<<<<<<<<<<<<<<<<<<" );
      CXLIB_OSTREAM( "END UNCONFIRMED DATA" );
    } END_CXLIB_OBJ_DUMP;
    free( buffer );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationParser_t * _transactional_validator__new( vgx_Graph_t *graph ) {

  vgx_OperationParser_t *validator = NULL;


  XTRY {

    if( (validator = calloc( 1, sizeof( vgx_OperationParser_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    
    if( iOperation.Parser.Initialize( graph, validator, false ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

  }
  XCATCH( errcode ) {
    iTransactional.Validator.Delete( &validator );
  }
  XFINALLY {
  }

  return validator;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _transactional_validator__delete( vgx_OperationParser_t **validator ) {
  if( validator && *validator ) {

    _vxdurable_operation_parser__destroy_OPEN( *validator );


    free( *validator );
    *validator = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_validator__validate( vgx_OperationParser_t *validator, vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *buffer, CString_t **CSTR__error ) {

  int64_t valid_bytes = 0;

  vgx_op_parser_error_t perr = {0};
  char *data = NULL;

  XTRY {

    // !!!!!!!!!!!!!!!!!!!!
    // TODO: Fix the parser so we don't need to make a copy of the sysout buffer.
    //       The parser should instead take in an operation buffer and be able
    //       to get data from it in a non-destructive way.
    //
    int64_t tail_size = transaction->tsize;
    if( iOpBuffer.GetTail( buffer, &data, tail_size ) < 0 ) {
      __set_error_string( CSTR__error, "Unable to extract transaction data from buffer" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    const char *cursor = data;
    const char *end = NULL;

    int64_t n;
    do {
      end = NULL;
      if( (n = iOperation.Parser.Feed( validator, cursor, &end, NULL, NULL, CSTR__error, &perr )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
      cursor = end;
      valid_bytes += n;
    } while( n >= 0 && cursor != NULL );

  }
  XCATCH( errcode ) {
    valid_bytes = -1;
  }
  XFINALLY {
    free( data );
  }

  return valid_bytes;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _transactional_validator__reset( vgx_OperationParser_t *validator ) {
  return iOperation.Parser.Reset( validator );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducers_t * _transactional_producers__new( void ) {
  vgx_TransactionalProducers_t *producers = NULL;
  XTRY {
    if( (producers = calloc( 1, sizeof( vgx_TransactionalProducers_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    producers->lock_count = -1;

    // [Q1]
    INIT_CRITICAL_SECTION( &producers->lock.lock );

    // [Q2.1.1]
    producers->lock_count = 0;

    // [Q2.1.2]
    producers->readers = 0;

    // [Q2.2]
    if( (producers->list = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // [Q2.3-8]
    producers->__rsv_2_3 = 0;
    producers->__rsv_2_4 = 0;
    producers->__rsv_2_5 = 0;
    producers->__rsv_2_6 = 0;
    producers->__rsv_2_7 = 0;
    producers->__rsv_2_8 = 0;
  }
  XCATCH( errcode ) {
    iTransactional.Producers.Delete( &producers );
  }
  XFINALLY {
  }
  return producers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _transactional_producers__delete( vgx_TransactionalProducers_t **producers ) {
  if( producers && *producers ) {
    // [Q1.7]
    if( (*producers)->list ) {
      iTransactional.Producers.Clear( *producers );
      COMLIB_OBJECT_DESTROY( (*producers)->list );
    }

    // [Q1.1-5]
    if( (*producers)->lock_count == 0 ) {
      DEL_CRITICAL_SECTION( &((*producers)->lock.lock) );
    }

    free( *producers );
    *producers = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __wait_no_readers( vgx_TransactionalProducers_t *producers ) {
  TRANSACTIONAL_PRODUCERS_WAIT_UNTIL( producers, producers->readers == 0, 10000 );
  if( producers->readers > 0 ) {
    VERBOSE( 0x000, "producers->readers = %d", producers->readers );
  }
  return producers->readers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _transactional_producers__clear( vgx_TransactionalProducers_t *producers ) {
  int cleared = 0;

  TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
    XTRY {
      // Readers ?
      if( __wait_no_readers( producers ) > 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Delete all producer instances
      vgx_TransactionalProducersIterator_t iter = {0};
      if( iTransactional.Producers.Iterator.Init( producers, &iter ) ) {
        vgx_TransactionalProducer_t *producer;
        while( (producer = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
          iTransactional.Producer.Delete( &producer );
        }
        iTransactional.Producers.Iterator.Clear( &iter );
      }

      // Clear the list
      CALLABLE( producers->list )->Clear( producers->list );
      cleared = 1;

    }
    XCATCH( errcode ) {
      cleared = -1;
    }
    XFINALLY {
    }
  } TRANSACTIONAL_PRODUCERS_RELEASE;
  
  return cleared;
}



/*******************************************************************//**
 * Add producer to list, which becomes owner.
 *
 * Return 1 if producer was added, 0 if it already existed, or -1 on error/timeout.
 *
 ***********************************************************************
 */
static int _transactional_producers__add( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducer_t **producer ) {
  if( *producer == NULL ) {
    return -1;
  }

  int added = 0;

  TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
    vgx_TransactionalProducersIterator_t iter = {0};
    XTRY {
      // Readers?
      if( __wait_no_readers( producers ) > 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      if( iTransactional.Producers.Iterator.Init( producers, &iter ) ) {
        vgx_TransactionalProducer_t *existing;
        while( (existing = iTransactional.Producers.Iterator.Next( &iter )) != NULL ) {
          bool exists;
          TRANSACTIONAL_LOCK( existing ) {
            exists = iURI.Match( existing->URI, (*producer)->URI );
          } TRANSACTIONAL_RELEASE;
          // Producer already exists in list, no action
          if( exists ) {
            XBREAK;
          }
        }
      }

      // Producer not found in list, append it (and steal)
      QWORD addr = (QWORD)(*producer);
      if( CALLABLE( producers->list )->Append( producers->list, &addr ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Success
      *producer = NULL; // stolen
      added = 1;
    }
    XCATCH( errcode ) {
      added = -1;
    }
    XFINALLY {
      iTransactional.Producers.Iterator.Clear( &iter );
    }
  } TRANSACTIONAL_PRODUCERS_RELEASE;
  
  return added;
}



/*******************************************************************//**
 * Remove producer identified by uri string.
 *
 * Returns 1 if the producer existed and was removed, 0 if it didn't exist,
 *         or return -1 on error/timeout
 ***********************************************************************
 */
static int _transactional_producers__remove( vgx_TransactionalProducers_t *producers, const char *uri ) {
  int removed = 0;

  TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
    XTRY {
      // Readers ?
      if( __wait_no_readers( producers ) > 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      CQwordList_t *P;
      if( (P = producers->list) != NULL ) {
        int64_t sz = ComlibSequenceLength( P );
        QWORD addr = 0;
        for( int64_t i=0; i<sz; i++ ) {
          CALLABLE( P )->Get( P, i, &addr );
          vgx_TransactionalProducer_t *producer = (vgx_TransactionalProducer_t*)addr;
          if( producer && producer->URI && CharsEqualsConst( iURI.URI( producer->URI ), uri )) {
            addr = 0;
            CALLABLE( P )->Set( P, i, &addr );
            iTransactional.Producer.Delete( &producer );
            removed = 1;
            XBREAK;
          }
        }
      }
    }
    XCATCH( errcode ) {
      removed = -1;
    }
    XFINALLY {
    }
  } TRANSACTIONAL_PRODUCERS_RELEASE;

  return removed;
}



/*******************************************************************//**
 * Check if producer identified by uri string exists in list
 *
 * Returns 1 if the producer exists, 0 if it does not exist, -1 on error.
 ***********************************************************************
 */
static int _transactional_producers__contains( vgx_TransactionalProducers_t *producers, const char *uri ) {
  int contains = 0;

  CString_t *CSTR__error = NULL;
  vgx_URI_t *checkURI = iURI.New( uri, &CSTR__error );
  iString.Discard( &CSTR__error );
  if( checkURI == NULL ) {
    return -1;
  }
  char *checkIP = cxgetip( iURI.Host( checkURI ) );
  if( checkIP == NULL ) {
    iURI.Delete( &checkURI );
    return -1;
  }
  unsigned short checkPort = iURI.PortInt( checkURI );

  TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
    CQwordList_t *P;
    if( (P = producers->list) != NULL ) {
      int64_t sz = ComlibSequenceLength( P );
      QWORD addr = 0;
      for( int64_t i=0; i<sz; i++ ) {
        CALLABLE( P )->Get( P, i, &addr );
        vgx_TransactionalProducer_t *producer = (vgx_TransactionalProducer_t*)addr;
        if( producer && producer->URI ) {
          char *existingIP = cxgetip( iURI.Host( producer->URI ) );
          if( existingIP ) {
            if( iURI.PortInt( producer->URI ) == checkPort && CharsEqualsConst( existingIP, checkIP ) ) {
              contains = 1;
            }
            free( existingIP );
          }
          else {
            contains = -1;
          }
        }
        if( contains != 0 ) {
          break;
        }
      }
    }
  } TRANSACTIONAL_PRODUCERS_RELEASE;

  iURI.Delete( &checkURI );
  free( checkIP );

  return contains;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _transactional_producers__defragment( vgx_TransactionalProducers_t *producers ) {
  int ret = 0;
  TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
    vgx_TransactionalProducer_t *producer;
    XTRY {
      // Readers?
      if( __wait_no_readers( producers ) > 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      CQwordList_t *P = producers->list;
      int64_t sz = ComlibSequenceLength( P );

      // Check if defrag is needed
      bool do_defrag = false;
      for( int64_t i=0; i<sz; i++ ) {
        CALLABLE( P )->Get( P, i, (QWORD*)&producer );
        if( producer == NULL || producer->URI == NULL || producer->flags.defunct || producer->flags.abandoned ) {
          do_defrag = true;
          break;
        }
      }

      // Defrag is needed
      if( do_defrag ) {
        // Create new list
        CQwordList_t *DP = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t );
        if( DP == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
        // Transfer active producers from old to new list
        for( int64_t i=0; i<sz; i++ ) {
          CALLABLE( P )->Get( P, i, (QWORD*)&producer );
          if( producer && producer->URI && !producer->flags.defunct && !producer->flags.abandoned ) {
            QWORD addr = (QWORD)producer;
            if( CALLABLE( DP )->Append( DP, &addr ) != 1 ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
            }
          }
          else {
            iTransactional.Producer.Delete( &producer );
          }
        }
        // Delete old list
        COMLIB_OBJECT_DESTROY( producers->list );
        // Replace with new defragmented list
        producers->list = DP;
      }
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
    }
  } TRANSACTIONAL_PRODUCERS_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producers__length( vgx_TransactionalProducers_t *producers ) {
  int64_t sz = 0;
  if( producers ) {
    TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
      sz = CALLABLE( producers->list )->Length( producers->list );
    } TRANSACTIONAL_PRODUCERS_RELEASE;
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducersIterator_t * _transactional_producers__iterator_init( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducersIterator_t *iterator ) {
  vgx_TransactionalProducersIterator_t *iter = NULL;
  if( producers ) {
    TRANSACTIONAL_PRODUCERS_LOCK( producers ) {
      iterator->list = producers->list;
      iterator->idx = 0;
      iterator->sz = CALLABLE( iterator->list )->Length( iterator->list );
      iterator->parent = producers;
      iterator->fragmented = false;
      if( iterator->list ) {
        // Increment readers, until this iterator has been exhausted
        ++(iterator->parent->readers);
        iter = iterator;
      }
    } TRANSACTIONAL_PRODUCERS_RELEASE;
  }
  return iter;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _transactional_producers__iterator_length( const vgx_TransactionalProducersIterator_t *iterator ) {
  return iterator->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalProducer_t * _transactional_producers__iterator_next( vgx_TransactionalProducersIterator_t *iterator ) {
  vgx_TransactionalProducer_t *producer = NULL;
  if( iterator->list && iterator->idx < iterator->sz ) {
    CQwordList_t *P = iterator->list;

    // Get next, skipping over empty slots and defunct instances
    do {
      CALLABLE( P )->Get( P, iterator->idx++, (QWORD*)&producer );
      if( producer ) {
        if( (producer->flags.defunct || producer->flags.abandoned) ) {
          producer = NULL; // defunct or detached producer is ignored
          iterator->fragmented = true;
        }
      }
      else {
        iterator->fragmented = true;
      }
    } while( producer == NULL && iterator->idx < iterator->sz );

    // Not NULL if we found next
    if( producer == NULL ) {
      // Clear iterator once exhausted
      iTransactional.Producers.Iterator.Clear( iterator );
    }
  }
  return producer;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _transactional_producers__iterator_clear( vgx_TransactionalProducersIterator_t *iterator ) {
  if( iterator->list ) {
    // Decrement readers
    TRANSACTIONAL_PRODUCERS_LOCK( iterator->parent ) {
      --(iterator->parent->readers);
      if( iterator->fragmented ) {
        _transactional_producers__defragment( iterator->parent );
      }
      iterator->list = NULL;
      iterator->idx = 0;
      iterator->sz = 0;
      iterator->parent = NULL;
    } TRANSACTIONAL_PRODUCERS_RELEASE;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_TransactionalConsumerService_t * _transactional_consumer_service__new( vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold ) {
  vgx_TransactionalConsumerService_t *consumer_service = NULL;

  XTRY {
    if( (consumer_service = calloc( 1, sizeof( vgx_TransactionalConsumerService_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( _vxdurable_operation_consumer_service__initialize_OPEN( SYSTEM, consumer_service, port, durable, snapshot_threshold ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

  }
  XCATCH( errcode ) {
    iTransactional.Consumer.Delete( &consumer_service );
  }
  XFINALLY {
  }

  return consumer_service;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _transactional_consumer_service__delete( vgx_TransactionalConsumerService_t **service ) {
  int ret = 0;
  if( service && *service ) {
    ret = _vxdurable_operation_consumer_service__destroy_OPEN( *service );
    free( *service );
    *service = NULL;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _transactional_consumer_service__open( vgx_TransactionalConsumerService_t *service, uint16_t port, CString_t **CSTR__error ) {
  static const int BACKLOG = 32;

  int ret = 0;

  // Disconnect and delete if we already have a URI
  iTransactional.Consumer.Close( service );

  // Set default port
  if( port == 0 ) {
    port = 8099;
  }

  XTRY {

    // New URI
    if( (service->Listen = iURI.NewElements( "vgx", NULL, "0.0.0.0", port, NULL, NULL, NULL, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Bind
    if( iURI.Bind( service->Listen, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Listen
    if( iURI.Listen( service->Listen, BACKLOG, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

  }
  XCATCH( errcode ) {
    iTransactional.Consumer.Close( service );
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
static int _transactional_consumer_service__close( vgx_TransactionalConsumerService_t *service ) {

  if( service->Listen ) {
    iURI.Delete( &service->Listen );
  }

  return 0;
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_transaction.h"
  
test_descriptor_t _vgx_vxdurable_operation_transaction_tests[] = {
  { "VGX Graph Durable Operation Transaction Tests", __utest_vxdurable_operation_transaction },
  {NULL}
};

#endif
