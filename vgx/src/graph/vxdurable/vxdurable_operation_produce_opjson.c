/*######################################################################
 *#
 *# vxdurable_operation_produce_opjson.c
 *#
 *#
 *######################################################################
 */


#include "_vxoperation.h"


#ifdef OPSTREAM_INLINE_JSON

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static int64_t  __ostream_string( vgx_Graph_t *graph, const char *str );
static int64_t  __ostream_stringsz( vgx_Graph_t *graph, const char *str, int64_t n );
static int64_t  __ostream_cstring( vgx_Graph_t *graph, const CString_t *CSTR__str );
static int64_t  __ostream_obid( vgx_Graph_t *graph, const objectid_t *obid );
static int64_t  __ostream_datetime( vgx_Graph_t *graph, int64_t ms );
static int64_t  __ostream_cstringmax( vgx_Graph_t *graph, const CString_t *CSTR__str, int64_t n );
static int64_t  __ostream_obidlist( vgx_Graph_t *graph, objectid_t **obid_list, int64_t count, int64_t *lxw, unsigned int *pcrc );
static char *   __reset_buffer( char *buffer, char **cursor );
static int64_t  __ostream_buffer( vgx_Graph_t *graph, char *buffer, char **cursor );
static int64_t  __ostream_buffer_crc( vgx_Graph_t *graph, char *buffer, char **cursor, unsigned int *pcrc );
static char **  __buffer_OP( char **cursor, OperationProcessorOpType optype );
static char **  __buffer_ENDOP( char **cursor );

static void     __queue_cstring_for_discard_OPEN( vgx_Graph_t *graph, CQwordQueue_t *cstring_discard, CString_t *CSTR__str );
static int64_t  __err_write_op( vgx_Graph_t *graph, int64_t dlen );
static int64_t  __write_op( vgx_Graph_t *graph, const char *data, int64_t dlen );
static size_t   __write_op_crc( vgx_Graph_t *graph, const char *buf, int64_t n, unsigned int *pcrc );
static size_t   __write_op_cstring_crc( vgx_OperationEmitter_t *emitter, const char *separator, CString_t *CSTR__string, unsigned int *pcrc );
static size_t   __term_buffer( char *buf, const char *cursor );
static void     __emit_comment( vgx_OperationEmitter_t *emitter, const char *label, CString_t *CSTR__text );
static void     __emit_comment_with_timestamp( vgx_OperationEmitter_t *emitter, int64_t tms, const char *label, CString_t *CSTR__text );
static void     __emit_json( vgx_Graph_t *graph, op_BASE *op );
static int64_t  __set_commit_deadline( vgx_OperationEmitter_t *emitter, int64_t tms );
static int64_t  __consume( CQwordBuffer_t *B, QWORD *p, size_t n );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_string( vgx_Graph_t *graph, const char *str ) {
  return __write_op( graph, str, strlen( str ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_stringsz( vgx_Graph_t *graph, const char *str, int64_t n ) {
  return __write_op( graph, str, n < 0 ? strlen(str) : n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_cstring( vgx_Graph_t *graph, const CString_t *CSTR__str ) {
  return __write_op( graph, CStringValue( CSTR__str ), CStringLength( CSTR__str ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_obid( vgx_Graph_t *graph, const objectid_t *obid ) {
  char idbuf[33];
  return __write_op( graph, idtostr( idbuf, obid ), 32 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_datetime( vgx_Graph_t *graph, int64_t ms ) {
  CString_t *CSTR__datetime = igraphinfo.CTime( ms, true );
  if( CSTR__datetime ) {
    int64_t n = __write_op( graph, CStringValue( CSTR__datetime ), CStringLength( CSTR__datetime ) );
    iString.Discard( &CSTR__datetime );
    return n;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_cstringmax( vgx_Graph_t *graph, const CString_t *CSTR__str, int64_t n ) {
  if( CStringLength( CSTR__str ) <= n ) {
    return __ostream_cstring( graph, CSTR__str );
  }
  else {
    int64_t a = __ostream_stringsz( graph, CStringValue( CSTR__str ), n );
    int64_t b = __write_op( graph, "...", 3 );
    if( a > 0 && b > 0 ) {
      return a + b;
    }
    else {
      return -1;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_obidlist( vgx_Graph_t *graph, objectid_t **obid_list, int64_t count, int64_t *lxw, unsigned int *pcrc ) {
  objectid_t *obid = *obid_list;
  int64_t sz = count > 0 ? count : -count;
  objectid_t *end = *obid_list + sz;
  char id_data[34];
  id_data[0] = SEP[0];
  id_data[33] = '\0';
  char *idbuf = id_data + 1;
  while( obid < end ) {
    write_hex_objectid( idbuf, obid++ );
    __write_op_crc( graph, id_data, 33, pcrc );
  }
  free( *obid_list );
  *obid_list = NULL;
  *lxw += count;
  return count;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __reset_buffer( char *buffer, char **cursor ) {
  *cursor = buffer;
  return *cursor;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_buffer( vgx_Graph_t *graph, char *buffer, char **cursor ) {
  size_t n = __term_buffer( buffer, *cursor );
  __write_op( graph, buffer, n );
  __reset_buffer( buffer, cursor );
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_buffer_crc( vgx_Graph_t *graph, char *buffer, char **cursor, unsigned int *pcrc ) {
  size_t n = __term_buffer( buffer, *cursor ); 
  __write_op_crc( graph, buffer, n, pcrc );
  __reset_buffer( buffer, cursor );
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char ** __buffer_OP( char **cursor, OperationProcessorOpType optype ) {
  __writebuf_string( cursor, INDENT2 );
  __writebuf_string( cursor, KWD_OP );
  __writebuf_op_word( cursor, (WORD)optype );
  return cursor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char ** __buffer_ENDOP( char **cursor ) {
  __writebuf_string( cursor, INDENT2 );
  __writebuf_string( cursor, KWD_ENDOP );
  return cursor;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define _emitter_instance                 __emitter
#define _graph_instance                   __graph
#define _opdatabuf                        __dbuf
#define _buffer_instance                  __buffer
#define _buffer_cursor                    __cursor
#define _operator                         __op
#define _crc                              __crc

#define _OSTREAM_CONST( String )          __write_op( _graph_instance, (String), sizeof(String)-1 )
#define _OSTREAM_STRING( Chars )          __ostream_string( _graph_instance, Chars )
#define _OSTREAM_STRINGSZ( Chars, Sz )    __ostream_stringsz( _graph_instance, Chars, Sz )
#define _OSTREAM_CSTRING( CSTR_x )        __ostream_cstring( _graph_instance, CSTR_x )
#define _OSTREAM_OBID( Obid )             __ostream_obid( _graph_instance, Obid )
#define _OSTREAM_DATETIME( Millisec )     __ostream_datetime( _graph_instance, Millisec )
#define _OSTREAM_CSTRINGMAX( CSTR_x, N )  __ostream_cstringmax( _graph_instance, CSTR_x, N )
#define _OSTREAM_OBIDLIST( Obids, Delta ) __ostream_obidlist( _graph_instance, &(Obids), Delta, &_emitter_instance->lxw_balance, &_crc );
#define _OSTREAM_FORMAT( Format, ... )    __writebuf_format( &_buffer_cursor, Format, ##__VA_ARGS__ ); __ostream_buffer( _graph_instance, _buffer_instance, &_buffer_cursor )
#define _OSTREAM_OPBUFFER()               __ostream_buffer_crc( _graph_instance, _buffer_instance, &_buffer_cursor, &_crc )
#define _OSTREAM_OPDATA( Buffer, Size )   __write_op_crc( _graph_instance, Buffer, Size, &_crc )
#define _OSTREAM_OPCSTRING( Sep, CSTR_x ) __write_op_cstring_crc( _emitter_instance, Sep, CSTR_x, &_crc )

#define RESET_BUFFER()                    __reset_buffer( _buffer_instance, &_buffer_cursor )
#define BUFFER_OBID( Obid )               __writebuf_op_objectid( &_buffer_cursor, Obid )
#define BUFFER_64( Qword )                __writebuf_op_qword( &_buffer_cursor, Qword )
#define BUFFER_32( Dword )                __writebuf_op_dword( &_buffer_cursor, Dword )
#define BUFFER_16( Word )                 __writebuf_op_word( &_buffer_cursor, Word )
#define BUFFER_08( Byte )                 __writebuf_op_byte( &_buffer_cursor, Byte )
#define BUFFER_STR( Str )                 __writebuf_string( &_buffer_cursor, Str )

#define BUFFER_OP( OpType )               __buffer_OP( &_buffer_cursor, OpType )
#define BUFFER_ENDOP()                    __buffer_ENDOP( &_buffer_cursor )

#define INC_LXW_BALANCE()                 (_emitter_instance->lxw_balance++)
#define DEC_LXW_BALANCE()                 (_emitter_instance->lxw_balance--)



#ifdef OPSTREAM_INLINE_JSON
#define EMIT_JSON( Graph, Operator )      __emit_json( Graph, (op_BASE*)&(Operator) )
#define EMIT_JSON_SNIPPET_CONST           _OSTREAM_CONST
#define EMIT_JSON_SNIPPET_STRING          _OSTREAM_STRING
#define EMIT_JSON_SNIPPET_CSTRING         _OSTREAM_CSTRING
#define EMIT_JSON_SNIPPET_OBID            _OSTREAM_OBID
#define EMIT_JSON_SNIPPET_FORMAT          _OSTREAM_FORMAT
#define EMIT_JSON_SNIPPET_DATETIME        _OSTREAM_DATETIME
#else
#define EMIT_JSON( Graph, Operator )            ((void)0)
#define EMIT_JSON_SNIPPET_CONST( String )       ((void)0)
#define EMIT_JSON_SNIPPET_STRING( Chars )       ((void)0)
#define EMIT_JSON_SNIPPET_CSTRING( CSTR_x )     ((void)0)
#define EMIT_JSON_SNIPPET_OBID( Obid )          ((void)0)
#define EMIT_JSON_SNIPPET_FORMAT( Format, ... ) ((void)0)
#define EMIT_JSON_SNIPPET_DATETIME( Millisec )  ((void)0)
#endif


#define CAPTURE_OBJECT_RESET_THRESHOLD  64          /* Capture object buffer is reset after emit if greater than this size */




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __queue_cstring_for_discard_OPEN( vgx_Graph_t *graph, CQwordQueue_t *cstring_discard, CString_t *CSTR__str ) {
  // Add cstring instance to queue for discard later
  if( cstring_discard ) {
    QWORD address = (QWORD)CSTR__str;
    if( CALLABLE( cstring_discard )->AppendNolock( cstring_discard, &address ) == 1 ) {
      return; // queued for discard
    }
  }

  // Could not queue string for discard, discard immediately
  GRAPH_LOCK( graph ) {
    icstringobject.DecrefNolock( CSTR__str );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

static int64_t __err_write_op( vgx_Graph_t *graph, int64_t dlen ) {
  CRITICAL( 0x001, "Failed to write %lld bytes to emitter op_buffer in graph '%s'", dlen, CALLABLE( graph )->FullPath( graph ) );
  iOpBuffer.DumpState( graph->OP.emitter.op_buffer );
  return -1;
}



/*******************************************************************//**
 * Returns number of bytes transferred into the operation buffer,
 * or negative on error.
 ***********************************************************************
 */
static int64_t __write_op( vgx_Graph_t *graph, const char *data, int64_t dlen ) {
  int64_t n_bytes = 0;
  if( data && (n_bytes = iOpBuffer.Write( graph->OP.emitter.op_buffer, data, dlen )) < 0 ) {
    return __err_write_op( graph, dlen );
  }
  return n_bytes;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static size_t __write_op_crc( vgx_Graph_t *graph, const char *buf, int64_t n, unsigned int *pcrc ) {
  __opdata_crc32c( buf, n, pcrc );
  return __write_op( graph, buf, n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static size_t __write_op_cstring_crc( vgx_OperationEmitter_t *emitter, const char *separator, CString_t *CSTR__string, unsigned int *pcrc ) {
  size_t n_bytes = 0;
  vgx_Graph_t *graph = emitter->graph;

  char *output = NULL;
  int64_t sz_ser;
  if( (sz_ser = icstringobject.Serialize( &output, CSTR__string )) < 0 ) {
    CRITICAL( 0xFFF, "out of memory" );
    return 0;
  }
  
  n_bytes = __write_op_crc( graph, separator, 1, pcrc );
  n_bytes += __write_op_crc( graph, output, sz_ser, pcrc );

  free( output );

  __queue_cstring_for_discard_OPEN( graph, emitter->cstring_discard, CSTR__string );

  return n_bytes;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static size_t __term_buffer( char *buf, const char *cursor ) {
  size_t n = cursor - buf;
  buf[ n ] = '\0';
  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __emit_comment( vgx_OperationEmitter_t *emitter, const char *label, CString_t *CSTR__text ) {
  vgx_Graph_t *_graph_instance = emitter->graph;
  char _buffer_instance[1024];
  char *_buffer_cursor = _buffer_instance;
  if( label ) {
    _OSTREAM_FORMAT( "    # [%s] ", label );
  }
  else {
    _OSTREAM_CONST( "    # " );
  }
  if( CSTR__text ) {
    CString_t *CSTR__safe = CALLABLE( CSTR__text )->Replace( CSTR__text, "\n", "\n# " );
    if( CSTR__safe ) {
      _OSTREAM_CSTRING( CSTR__safe );
      CStringDelete( CSTR__safe );
    }
  }
  _OSTREAM_CONST( "\n" );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __emit_comment_with_timestamp( vgx_OperationEmitter_t *emitter, int64_t tms, const char *label, CString_t *CSTR__text ) {
  vgx_Graph_t *_graph_instance = emitter->graph;
  char _buffer_instance[1024];
  char *_buffer_cursor = _buffer_instance;
  _OSTREAM_CONST( "# [" );
  _OSTREAM_DATETIME( tms );
  if( label ) {
    _OSTREAM_FORMAT( "] [%s] ", label );
  }
  else {
    _OSTREAM_CONST( "] " );
  }
  if( CSTR__text ) {
    _OSTREAM_CSTRING( CSTR__text );
  }
  _OSTREAM_CONST( "\n" );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __emit_json( vgx_Graph_t *_graph_instance, op_BASE *op ) {
  char _buffer_instance[1024];
  char *_buffer_cursor = _buffer_instance;

  static const char prefix[] = __JSON_PREFIX "{ \"op\": ";
  _OSTREAM_CONST( prefix );

  switch( op->op.code ) {
  case OPCODE_VERTEX_NEW:
    _OSTREAM_CONST( "\"createVertex\"" );
    {
      op_vertex_new *vxn = (op_vertex_new*)op;
      CString_t *CSTR__id = _vxgraph_vxtable__get_idstring_OPEN( _graph_instance, &vxn->obid, vxn->vxtype );
      if( CSTR__id ) {
        _OSTREAM_CONST( ", \"id\": \"" );
        _OSTREAM_CSTRING( CSTR__id );
        _OSTREAM_FORMAT( "\", \"type\": %d", (int)vxn->vxtype );
      }
    }
    break;
  case OPCODE_VERTEX_DELETE:
    _OSTREAM_CONST( "\"deleteVertex\"" );
    {
      op_vertex_delete *vxd = (op_vertex_delete*)op;
      _OSTREAM_CONST( ", \"obid\": \"" );
      _OSTREAM_OBID( &vxd->obid );
      _OSTREAM_CONST( "\"" );
      if( vxd->eventexec ) {
        _OSTREAM_CONST( ", \"ttl\": true" );
      }
    }
    break;
  case OPCODE_VERTEX_SET_RANK:
    _OSTREAM_CONST( "\"vertexSetRank\"" );
    {
      op_vertex_set_rank *vxr = (op_vertex_set_rank*)op;
      _OSTREAM_FORMAT( ", \"c1\": %.8e, \"c0\": %.8e", vxr->rank.c1, vxr->rank.c0 );
    }
    break;
  case OPCODE_VERTEX_SET_TYPE:
    _OSTREAM_FORMAT( "\"vertexSetType\", \"type\": %d", ((op_vertex_set_type*)op)->vxtype );
    break;
  case OPCODE_VERTEX_SET_TMX:
    _OSTREAM_FORMAT( "\"vertexSetTmx\"" );
    {
      op_vertex_set_tmx *vxx = (op_vertex_set_tmx*)op;
      _OSTREAM_FORMAT( ", \"tmx\": %u, \"datetime\": \"", vxx->tmx );
      _OSTREAM_DATETIME( 1000 * (int64_t)vxx->tmx );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_VERTEX_CONVERT:
    _OSTREAM_FORMAT( "\"vertexConvert\", \"man\": \"%s\"", _vgx_manifestation_as_string( ((op_vertex_convert*)op)->manifestation ) );
    break;
  case OPCODE_VERTEX_SET_PROPERTY:
    _OSTREAM_CONST( "\"vertexSetProperty\"" );
    {
      op_vertex_set_property *vps = (op_vertex_set_property*)op;
      vgx_value_t value;
      value.type = vps->vtype;

      _OSTREAM_FORMAT( ", \"key\": %llu, \"value\": ", vps->key );
      switch( vps->vtype ) {
      case VGX_VALUE_TYPE_BOOLEAN:
        _OSTREAM_CONST( "true, \"type\": \"boolean\"" );
        break;
      case VGX_VALUE_TYPE_INTEGER:
        value.data.bits = vps->dataL;
        _OSTREAM_FORMAT( "%lld, \"type\": \"integer\"", value.data.simple.integer );
        break;
      case VGX_VALUE_TYPE_REAL:
        value.data.bits = vps->dataL;
        _OSTREAM_FORMAT( "%.8e, \"type\": \"real\"", value.data.simple.real );
        break;
      case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      case VGX_VALUE_TYPE_CSTRING:
        _OSTREAM_CONST( "\"" );
        _OSTREAM_OBID( &vps->stringid );
        _OSTREAM_CONST( "\", \"type\": \"string\"" );
        break;
      default:
        _OSTREAM_CONST( "false" );
      }
    }
    break;
  case OPCODE_VERTEX_DELETE_PROPERTY:
    _OSTREAM_CONST( "\"vertexDeleteProperty\"" );
    {
      op_vertex_delete_property *vpd = (op_vertex_delete_property*)op;
      _OSTREAM_FORMAT( ", \"key\": %llu", vpd->key );
    }
    break;
  case OPCODE_VERTEX_CLEAR_PROPERTIES:
    _OSTREAM_CONST( "\"vertexClearProperties\"" );
    break;
  case OPCODE_VERTEX_SET_VECTOR:
    _OSTREAM_CONST( "\"vertexSetVector\"" );
    {
      op_vertex_set_vector *vvs = (op_vertex_set_vector*)op;
      // TODO!!!
      _OSTREAM_CONST( ", \"raw\": \"" );
      _OSTREAM_CSTRING( vvs->CSTR__vector );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_VERTEX_DELETE_VECTOR:
    _OSTREAM_CONST( "\"vertexDeleteVector\"" );
    break;
  case OPCODE_VERTEX_DELETE_OUTARCS:
    _OSTREAM_FORMAT( "\"vertexDeleteOutarcs\", \"n\": %lld", ((op_vertex_delete_outarcs*)op)->n_removed );
    break;
  case OPCODE_VERTEX_DELETE_INARCS:
    _OSTREAM_FORMAT( "\"vertexDeleteInarcs\", \"n\": %lld", ((op_vertex_delete_inarcs*)op)->n_removed );
    break;
  case OPCODE_VERTEX_ACQUIRE:
    _OSTREAM_CONST( "\"vertexAcquire\"" );
    break;
  case OPCODE_VERTEX_RELEASE:
    _OSTREAM_CONST( "\"vertexRelease\"" );
    break;
  case OPCODE_ARC_CONNECT:
    _OSTREAM_CONST( "\"arcConnect\"" );
    {
      op_arc_connect *arc = (op_arc_connect*)op;
      vgx_predicator_t pred = arc->pred;

      _OSTREAM_FORMAT( ", \"rel\": %u, \"mod\": %u, \"val\": ", pred.rel.enc, pred.mod.bits );

      switch( _vgx_predicator_value_range( NULL, NULL, pred.mod.bits ) ) {
      case VGX_PREDICATOR_VAL_TYPE_UNITY:
        _OSTREAM_CONST( "1" );
        break;
      case VGX_PREDICATOR_VAL_TYPE_INTEGER:
        _OSTREAM_FORMAT( "%d", pred.val.integer );
        break;
      case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
        _OSTREAM_FORMAT( "%u", pred.val.uinteger );
        break;
      case VGX_PREDICATOR_VAL_TYPE_REAL:
        _OSTREAM_FORMAT( "%.8e", pred.val.real );
        break;
      default:
        _OSTREAM_CONST( "0" );
      }

      _OSTREAM_CONST( ", \"head\": \"" );
      CString_t *CSTR__head = _vxgraph_vxtable__get_idstring_OPEN( _graph_instance, &arc->headobid, VERTEX_TYPE_ENUMERATION_WILDCARD );
      if( CSTR__head ) {
        _OSTREAM_CSTRING( CSTR__head );
        iString.Discard( &CSTR__head );
      }
      else {
        _OSTREAM_OBID( &arc->headobid );
      }
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_ARC_DISCONNECT:
    _OSTREAM_CONST( "\"arcDisconnect\"" );
    {
      op_arc_disconnect *ard = (op_arc_disconnect*)op;
      vgx_predicator_t pred = ard->pred;

      _OSTREAM_FORMAT( ", \"probe\": { \"rel\": %u, \"mod\": %u, \"val\": ", pred.rel.enc, pred.mod.bits );

      switch( _vgx_predicator_value_range( NULL, NULL, pred.mod.bits ) ) {
      case VGX_PREDICATOR_VAL_TYPE_UNITY:
        _OSTREAM_CONST( "1" );
        break;
      case VGX_PREDICATOR_VAL_TYPE_INTEGER:
        _OSTREAM_FORMAT( "%d", pred.val.integer );
        break;
      case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
        _OSTREAM_FORMAT( "%u", pred.val.uinteger );
        break;
      case VGX_PREDICATOR_VAL_TYPE_REAL:
        _OSTREAM_FORMAT( "%.8e", pred.val.real );
        break;
      default:
        _OSTREAM_CONST( "0" );
      }

      _OSTREAM_CONST( " }" );

      _OSTREAM_CONST( ", \"head\": \"" );
      CString_t *CSTR__head = _vxgraph_vxtable__get_idstring_OPEN( _graph_instance, &ard->headobid, VERTEX_TYPE_ENUMERATION_WILDCARD );
      if( CSTR__head ) {
        _OSTREAM_CSTRING( CSTR__head );
        iString.Discard( &CSTR__head );
      }
      else {
        _OSTREAM_OBID( &ard->headobid );
      }
      _OSTREAM_CONST( "\"" );

    }
    break;
  case OPCODE_SYSTEM_SIMILARITY:
    _OSTREAM_CONST( "\"simConfig\"" );
    {
      op_system_similarity *scf = (op_system_similarity*)op;
      vgx_Similarity_fingerprint_config_t *fingerprint = &scf->sim.fingerprint;
      vgx_Similarity_vector_config_t *vector = &scf->sim.vector;
      vgx_Similarity_threshold_config_t *threshold = &scf->sim.threshold;

      _OSTREAM_FORMAT( ", \"fingerprint\": { \"nsegm\": %d, \"nsign\": %d }", fingerprint->nsegm, fingerprint->nsign );
      
      _OSTREAM_FORMAT( ", \"vector\": { \"maxSize\": %u, \"minIntersect\": %d, \"minCosine\": %.8e, \"minJaccard\": %.8e, \"cosineExponent\": %.8e, \"jaccardExponent\": %.8e }", vector->max_size, vector->min_intersect, vector->min_cosine, vector->min_jaccard, vector->cosine_exponent, vector->jaccard_exponent );

      _OSTREAM_FORMAT( ", \"threshold\": { \"hamming\": %d, \"similarity\": %.8e }", threshold->hamming, threshold->similarity );
    }
    break;
  case OPCODE_SYSTEM_ATTACH:
    _OSTREAM_CONST( "\"systemAttach\"" );
    {
      op_system_attach *sya = (op_system_attach*)op;
      _OSTREAM_FORMAT( ", \"tms\": %lld, \"datetime\": \"", sya->tms );
      _OSTREAM_DATETIME( sya->tms );
      _OSTREAM_CONST( "\", \"originHost\": \"" );
      _OSTREAM_CSTRING( sya->CSTR__origin_host );
      _OSTREAM_CONST( "\", \"originVersion\": \"" );
      _OSTREAM_CSTRING( sya->CSTR__origin_version );
      _OSTREAM_FORMAT( ", \"status\": %d", sya->status );
    }
    break;
  case OPCODE_SYSTEM_DETACH:
    _OSTREAM_CONST( "\"systemDetach\"" );
    {
      op_system_detach *syd = (op_system_detach*)op;
      _OSTREAM_FORMAT( ", \"tms\": %lld, \"datetime\": \"", syd->tms );
      _OSTREAM_DATETIME( syd->tms );
      _OSTREAM_CONST( "\", \"originHost\": \"" );
      _OSTREAM_CSTRING( syd->CSTR__origin_host );
      _OSTREAM_CONST( "\", \"originVersion\": \"" );
      _OSTREAM_CSTRING( syd->CSTR__origin_version );
      _OSTREAM_FORMAT( ", \"status\": %d", syd->status );
    }
    break;
  case OPCODE_SYSTEM_CLEAR_REGISTRY:
    _OSTREAM_CONST( "\"systemClearRegistry\"" );
    break;
  case OPCODE_SYSTEM_CREATE_GRAPH:
    _OSTREAM_CONST( "\"systemCreateGraph\"" );
    {
      op_system_create_graph *grn = (op_system_create_graph*)op;
      _OSTREAM_FORMAT( ", \"vertexBlockOrder\": %d, \"t0\": %u, \"startOpid\": %lld, \"obid\": \"%016llx%016llx\", \"path\": \"", grn->vertex_block_order, grn->graph_t0, grn->start_opcount, grn->obid.H, grn->obid.L );
      _OSTREAM_CSTRING( grn->CSTR__path );
      _OSTREAM_CONST( "\", \"name\": \"" );
      _OSTREAM_CSTRING( grn->CSTR__name );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_SYSTEM_DELETE_GRAPH:
    _OSTREAM_CONST( "\"systemDeleteGraph\"" );
    {
      op_system_delete_graph *grd = (op_system_delete_graph*)op;
      _OSTREAM_CONST( ", \"obid\": \"" );
      _OSTREAM_OBID( &grd->obid );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_SYSTEM_SEND_COMMENT:
    _OSTREAM_CONST( "\"systemSendComment\"" );
    {
      op_system_send_comment *com = (op_system_send_comment*)op;
      _OSTREAM_CONST( ", \"comment\": \"" );
      _OSTREAM_CSTRING( com->CSTR__comment );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_SYSTEM_SEND_RAW_DATA:
    _OSTREAM_CONST( "\"systemSendRawData\"" );
    {
      op_system_send_raw_data *dat = (op_system_send_raw_data*)op;
      _OSTREAM_FORMAT( ", \"n_parts\": %lld, \"part_id\": %lld, \"sz_datapart\": %lld, \"obid\": \"", dat->n_parts, dat->part_id, dat->sz_datapart );
      _OSTREAM_OBID( &dat->obid );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_GRAPH_TRUNCATE:
    _OSTREAM_CONST( "\"graphTruncate\"" );
    {
      op_graph_truncate *grt = (op_graph_truncate*)op;
      _OSTREAM_FORMAT( ", \"type\": %d, \"n\": %lld", grt->vxtype, grt->n_discarded );
    }
    break;
  case OPCODE_GRAPH_PERSIST:
    _OSTREAM_CONST( "\"graphPersist\"" );
    {
      op_graph_persist *grp = (op_graph_persist*)op;
      vgx_graph_base_counts_t *c = &grp->counts;
      _OSTREAM_FORMAT( ", \"order\": %lld, \"size\": %lld, \"nkey\": %lld, \"nstrval\": %lld, \"nprop\": %lld, \"nvec\": %lld, \"ndim\": %d, \"nrel\": %d, \"ntype\": %d, \"flags\": %d",
                                     c->order,       c->size,        c->nkey,           c->nstrval,      c->nprop,       c->nvec,        (int)c->ndim, (int)c->nrel,  (int)c->ntype, (int)c->flags._bits );
    }
    break;
  case OPCODE_GRAPH_STATE:
    _OSTREAM_CONST( "\"graphState\"" );
    {
      op_graph_state *grs = (op_graph_state*)op;
      vgx_graph_base_counts_t *c = &grs->counts;
      _OSTREAM_FORMAT( ", \"order\": %lld, \"size\": %lld, \"nkey\": %lld, \"nstrval\": %lld, \"nprop\": %lld, \"nvec\": %lld, \"ndim\": %d, \"nrel\": %d, \"ntype\": %d, \"flags\": %d",
                                     c->order,       c->size,        c->nkey,           c->nstrval,      c->nprop,       c->nvec,        (int)c->ndim, (int)c->nrel,  (int)c->ntype, (int)c->flags._bits );
    }
    break;
  case OPCODE_VERTICES_ACQUIRE_WL:
    _OSTREAM_CONST( "\"verticesAcquireWL\"" );
    goto VERTICES_LOCKSTATE;
  case OPCODE_VERTICES_RELEASE:
    _OSTREAM_CONST( "\"verticesRelease\"" );
    goto VERTICES_LOCKSTATE;
  case OPCODE_VERTICES_RELEASE_ALL:
    _OSTREAM_CONST( "\"verticesReleaseAll\"" );
    goto VERTICES_LOCKSTATE;
  VERTICES_LOCKSTATE:
    {
      op_vertices_lockstate *lck = (op_vertices_lockstate*)op;
      if( lck->obid_list ) {
        _OSTREAM_CONST( ", \"idList\": [" );
        const objectid_t *item = lck->obid_list;
        const objectid_t *end = item + lck->count;
        while( item < end ) {
          _OSTREAM_CONST( "\"" );
          CString_t *CSTR__id = _vxgraph_vxtable__get_idstring_OPEN( _graph_instance, item, VERTEX_TYPE_ENUMERATION_WILDCARD );
          if( CSTR__id ) {
            _OSTREAM_CSTRING( CSTR__id );
            iString.Discard( &CSTR__id );
          }
          else {
            _OSTREAM_OBID( item );
          }
          _OSTREAM_CONST( "\"" );
          if( ++item < end ) {
            _OSTREAM_CONST( ", " );
          }
        }
        _OSTREAM_CONST( "]" );
      }
    }
    break;
  case OPCODE_GRAPH_READONLY:
    _OSTREAM_CONST( "\"graphReadonly\"" );
    break;
  case OPCODE_GRAPH_READWRITE:
    _OSTREAM_CONST( "\"graphReadwrite\"" );
    break;
  case OPCODE_GRAPH_EVENTS:
    _OSTREAM_CONST( "\"graphEvents\"" );
    break;
  case OPCODE_GRAPH_NOEVENTS:
    _OSTREAM_CONST( "\"graphNoevents\"" );
    break;
  case OPCODE_GRAPH_TICK:
    _OSTREAM_CONST( "\"graphTick\"" );
    {
      op_graph_tic *tic = (op_graph_tic*)op;
      _OSTREAM_FORMAT( ", \"tms\": %lld, \"order\": %lld, \"size\": %lld", tic->tms, tic->order, tic->size );
    }
    break;
  case OPCODE_GRAPH_EVENT_EXEC:
    _OSTREAM_CONST( "\"graphEventExec\"" );
    {
      op_graph_event_exec *tic = (op_graph_event_exec*)op;
      _OSTREAM_FORMAT( ", \"ts\": %u, \"tmx\": %u", tic->ts, tic->tmx );
    }
    break;
  case OPCODE_ENUM_ADD_VXTYPE:
    _OSTREAM_CONST( "\"enumAddVxtype\"" );
    goto ENUM_ADD;
  case OPCODE_ENUM_ADD_REL:
    _OSTREAM_CONST( "\"enumAddRel\"" );
    goto ENUM_ADD;
  case OPCODE_ENUM_ADD_DIM:
    _OSTREAM_CONST( "\"enumAddDim\"" );
    goto ENUM_ADD;
  case OPCODE_ENUM_ADD_KEY:
    _OSTREAM_CONST( "\"enumAddKey\"" );
    goto ENUM_ADD;
  ENUM_ADD:
    {
      op_enum_add_string64 *ea = (op_enum_add_string64*)op;
      _OSTREAM_FORMAT( ", \"hash\": %llu, \"enum\": %llu, \"value\": \"", ea->hash, ea->encoded );
      _OSTREAM_CSTRING( ea->CSTR__value );
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_ENUM_DELETE_VXTYPE:
    _OSTREAM_CONST( "\"enumDeleteVxtype\"" );
    goto ENUM_DELETE;
  case OPCODE_ENUM_DELETE_REL:
    _OSTREAM_CONST( "\"enumDeleteRel\"" );
    goto ENUM_DELETE;
  case OPCODE_ENUM_DELETE_DIM:
    _OSTREAM_CONST( "\"enumDeleteDim\"" );
    goto ENUM_DELETE;
  case OPCODE_ENUM_DELETE_KEY:
    _OSTREAM_CONST( "\"enumDeleteKey\"" );
    goto ENUM_DELETE;
  ENUM_DELETE:
    {
      op_enum_delete_string64 *ed = (op_enum_delete_string64*)op;
      _OSTREAM_FORMAT( ", \"hash\": %llu, \"enum\": %llu", ed->hash, ed->encoded );
    }
    break;
  case OPCODE_ENUM_ADD_STRING:
    _OSTREAM_CONST( "\"enumAddString\"" );
    {
      op_enum_add_string128 *ea = (op_enum_add_string128*)op;
      _OSTREAM_CONST( ", \"obid\": \"" );
      _OSTREAM_OBID( &ea->obid );
      _OSTREAM_CONST( "\", \"value\": \"" );
      CString_t *CSTR__b16 = CStringB16Encode( ea->CSTR__value );
      if( CSTR__b16 ) {
        _OSTREAM_CSTRING( CSTR__b16 );
        iString.Discard( &CSTR__b16 );
      }
      _OSTREAM_CONST( "\"" );
    }
    break;
  case OPCODE_ENUM_DELETE_STRING:
    _OSTREAM_CONST( "\"enumDeleteString\"" );
    {
      op_enum_delete_string128 *ed = (op_enum_delete_string128*)op;
      _OSTREAM_CONST( ", \"obid\": \"" );
      _OSTREAM_OBID( &ed->obid );
      _OSTREAM_CONST( "\"" );
    }
    break;
  default:
    _OSTREAM_CONST( "\"noOp\"" );
    break;
  }
  _OSTREAM_CONST( " },\n" );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __set_commit_deadline( vgx_OperationEmitter_t *emitter, int64_t tms ) {
  return emitter->commit_deadline_ms = tms;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __consume( CQwordBuffer_t *B, QWORD *p, size_t n ) {
  return CALLABLE( B )->Read( B, (void**)&p, n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char ** __begin_operator( OperationProcessorOperator_t *op, char **cursor ) {
  __writebuf_string( cursor, INDENT4 );
  __writebuf_string( cursor, op->name );
  __writebuf_op_dword( cursor, op->code );
  return cursor;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_OPERATION(  ) \
do {                        \
  RESET_BUFFER();           \
  do

#define END_OPERATION       \
  WHILE_ZERO;               \
  BUFFER_32( _crc );        \
  BUFFER_STR( "\n" );       \
} WHILE_ZERO



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_OPERATOR( OpType, Data )                        \
do {                                                          \
  RESET_BUFFER();                                             \
  __begin_operator( &_operator, &_buffer_cursor );            \
  OpType Data;                                                \
  (Data).op.code = _operator.code;                            \
  __consume( _opdatabuf, (Data).qwords+1, qwsizeof(Data)-1 ); \
  EMIT_JSON( _graph_instance, data );


#define END_OPERATOR            \
  _OSTREAM_OPDATA( "\n", 1 );   \
} WHILE_ZERO;                   \
break



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_EMIT_CRC  \
do {

#define END_EMIT_CRC    \
  _OSTREAM_OPBUFFER();  \
} WHILE_ZERO





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static OperationProcessorOpType __get_optype( OperationProcessorOperator_t *op ) {
  switch( op->code ) {
  case OPCODE_SYSTEM_ATTACH:
  case OPCODE_SYSTEM_DETACH:
  case OPCODE_SYSTEM_CLEAR_REGISTRY:
  case OPCODE_SYSTEM_SEND_COMMENT:
  case OPCODE_SYSTEM_SEND_RAW_DATA:
  case OPCODE_SYSTEM_CLONE_GRAPH:
  case OPCODE_SYSTEM_SIMILARITY:
  case OPCODE_SYSTEM_CREATE_GRAPH:
  case OPCODE_SYSTEM_DELETE_GRAPH:
    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( " \"system\"" );
    #endif
    return OPTYPE_SYSTEM;
    
  case OPCODE_GRAPH_TICK:
  case OPCODE_GRAPH_EVENT_EXEC:
  case OPCODE_GRAPH_READONLY:
  case OPCODE_GRAPH_READWRITE:
  case OPCODE_GRAPH_EVENTS:
  case OPCODE_GRAPH_NOEVENTS:
    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( " \"graphState\"" );
    #endif
    return OPTYPE_GRAPH_STATE;

  case OPCODE_VERTICES_ACQUIRE_WL:
    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( " \"vertexLock\"" );
    #endif
    return OPTYPE_VERTEX_LOCK;

  case OPCODE_VERTICES_RELEASE:
    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( " \"vertexRelease\"" );
    #endif
    return OPTYPE_VERTEX_RELEASE;

  case OPCODE_VERTEX_NEW:
  case OPCODE_VERTEX_DELETE:
  default:
    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( " \"graphObject\"" );
    #endif
    return OPTYPE_GRAPH_OBJECT;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_produce_op__emit_json_OPEN( vgx_OperationEmitter_t *_emitter_instance, int64_t tms, vgx_OperatorCapture_t *capture ) {

  vgx_Graph_t *_graph_instance = _emitter_instance->graph;
  char _buffer_instance[1024];
  char *_buffer_cursor;
  unsigned int _crc = 0;

  CQwordBuffer_t *_opdatabuf = capture->opdatabuf;
  CQwordBuffer_vtable_t *iBuf = CALLABLE( _opdatabuf );
  if( ComlibSequenceLength( _opdatabuf ) < 1 ) {
    return 0;
  }


  BEGIN_OPERATION() {

    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( "# { \"operation\": { \"type\":" );
    #endif

    OperationProcessorOperator_t _operator = {0};
    iBuf->Get( _opdatabuf, 0, &_operator.data );

    OperationProcessorOpType optype = 0;

    int tp_class = capture->inheritable.object_typeinfo.tp_class;
    switch( tp_class ) {
    case COMLIB_CLASS_CODE( vgx_Graph_t ):
      optype = __get_optype( &_operator );
      if( optype != OPTYPE_SYSTEM ) {
        #ifdef OPSTREAM_INLINE_JSON
        EMIT_JSON_SNIPPET_FORMAT( ", \"graph\": \"%s\" },\n", CALLABLE( capture->graph )->FullPath( capture->graph ) );
        #endif

        BUFFER_OP( optype );
        BUFFER_OBID( &capture->graph->obid );
      }
      else {
        #ifdef OPSTREAM_INLINE_JSON
        EMIT_JSON_SNIPPET_FORMAT( " },\n" );
        #endif

        BUFFER_OP( optype );
      }

      break;
    case COMLIB_CLASS_CODE( vgx_Vertex_t ):
      {
        const objectid_t *vertex_obid = &capture->inheritable.objectid;

        #ifdef OPSTREAM_INLINE_JSON
        EMIT_JSON_SNIPPET_FORMAT( " \"vertexObject\", \"graph\": \"%s\", \"id\": \"", CALLABLE( capture->graph )->FullPath( capture->graph ) );
        CString_t *CSTR__vertex_id = _vxgraph_vxtable__get_idstring_OPEN( capture->graph, vertex_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
        if( CSTR__vertex_id ) {
          EMIT_JSON_SNIPPET_CSTRING( CSTR__vertex_id );
          iString.Discard( &CSTR__vertex_id );
        }
        else {
          EMIT_JSON_SNIPPET_OBID( &capture->objectid );
        }
        EMIT_JSON_SNIPPET_CONST( "\" },\n" );
        #endif
        
        //
        // TODO: Make sure the "lock/release" operators are single ops within operation!
        //
        optype = OPTYPE_VERTEX_OBJECT;
        BUFFER_OP( optype );
        BUFFER_OBID( &capture->graph->obid );
        BUFFER_OBID( vertex_obid );
      }
      break;
    default:
      iBuf->Clear( _opdatabuf );
      return 0;
    }

    BUFFER_STR( "\n" );

    _OSTREAM_OPBUFFER();

    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( "#   \"operators\": [\n" );
    #endif

    // Translate all data in the operation data input to operator output format
    //
    while( iBuf->Next( _opdatabuf, &_operator.data ) == 1 ) {
      
      switch( _operator.code ) {

      // op_vertex_new
      case OPCODE_VERTEX_NEW:
        BEGIN_OPERATOR( op_vertex_new, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_OBID( &data.obid );
            BUFFER_08( (BYTE)data.vxtype );
            BUFFER_32( data.TMC );
            BUFFER_32( data.TMX.vertex_ts );
            BUFFER_32( data.TMX.arc_ts );
            BUFFER_64( data.rank.bits );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__id );
          _OSTREAM_CONST( " # " );
          _OSTREAM_CSTRINGMAX( data.CSTR__id, 40 );
        } END_OPERATOR;

      // op_vertex_delete
      case OPCODE_VERTEX_DELETE:
        BEGIN_OPERATOR( op_vertex_delete, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_OBID( &data.obid );
            BUFFER_08( (BYTE)data.eventexec );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_set_rank
      case OPCODE_VERTEX_SET_RANK:
        BEGIN_OPERATOR( op_vertex_set_rank, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.rank.bits );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_set_type
      case OPCODE_VERTEX_SET_TYPE:
        BEGIN_OPERATOR( op_vertex_set_type, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_08( (BYTE)data.vxtype);
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_set_tmx
      case OPCODE_VERTEX_SET_TMX:
        BEGIN_OPERATOR( op_vertex_set_tmx, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_32( data.tmx );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_convert
      case OPCODE_VERTEX_CONVERT:
        BEGIN_OPERATOR( op_vertex_convert, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_08( data.manifestation );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_set_property
      case OPCODE_VERTEX_SET_PROPERTY:
        BEGIN_OPERATOR( op_vertex_set_property, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.key );
            BUFFER_08( data.vtype );
            BUFFER_64( data.dataH );
            BUFFER_64( data.dataL );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_delete_property
      case OPCODE_VERTEX_DELETE_PROPERTY:
        BEGIN_OPERATOR( op_vertex_delete_property, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.key );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_clear_properties
      case OPCODE_VERTEX_CLEAR_PROPERTIES:
        BEGIN_OPERATOR( op_vertex_clear_properties, data ) {
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_vertex_set_vector
      case OPCODE_VERTEX_SET_VECTOR:
        BEGIN_OPERATOR( op_vertex_set_vector, data ) {
          _OSTREAM_OPBUFFER();
          _OSTREAM_OPCSTRING( SEP, data.CSTR__vector );
        } END_OPERATOR;

      // op_vertex_delete_vector
      case OPCODE_VERTEX_DELETE_VECTOR:
        BEGIN_OPERATOR( op_vertex_delete_vector, data ) {
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_vertex_delete_outarcs
      case OPCODE_VERTEX_DELETE_OUTARCS:
        BEGIN_OPERATOR( op_vertex_delete_outarcs, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.n_removed );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_delete_inarcs
      case OPCODE_VERTEX_DELETE_INARCS:
        BEGIN_OPERATOR( op_vertex_delete_inarcs, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.n_removed );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertex_acquire
      case OPCODE_VERTEX_ACQUIRE:
        BEGIN_OPERATOR( op_vertex_acquire, data ) {
          _OSTREAM_OPBUFFER();
          INC_LXW_BALANCE();
        } END_OPERATOR;

      // op_vertex_release
      case OPCODE_VERTEX_RELEASE:
        BEGIN_OPERATOR( op_vertex_release, data ) {
          _OSTREAM_OPBUFFER();
          DEC_LXW_BALANCE();
        } END_OPERATOR;

      // op_arc_connect
      case OPCODE_ARC_CONNECT:
        BEGIN_OPERATOR( op_arc_connect, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.pred.data );
            BUFFER_OBID( &data.headobid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_arc_disconnect
      case OPCODE_ARC_DISCONNECT:
        BEGIN_OPERATOR( op_arc_disconnect, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_08( (BYTE)data.eventexec );
            BUFFER_64( data.n_removed );
            BUFFER_64( data.pred.data );
            BUFFER_OBID( &data.headobid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_system_attach
      case OPCODE_SYSTEM_ATTACH:
        BEGIN_OPERATOR( op_system_attach, data ) {
          __emit_comment_with_timestamp( _emitter_instance, data.tms, "ATTACH", NULL );
          __emit_comment( _emitter_instance, "ORIGIN ", data.CSTR__origin_host );
          __emit_comment( _emitter_instance, "VERSION", data.CSTR__origin_version );
          BEGIN_EMIT_CRC {
            BUFFER_64( data.tms );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__via_uri );
          _OSTREAM_OPCSTRING( SEP, data.CSTR__origin_host );
          _OSTREAM_OPCSTRING( SEP, data.CSTR__origin_version );
          BEGIN_EMIT_CRC {
            BUFFER_32( data.status );
          } END_EMIT_CRC;
          // Immediate commit
          __set_commit_deadline( _emitter_instance, tms );
        } END_OPERATOR;

      // op_system_detach
      case OPCODE_SYSTEM_DETACH:
        BEGIN_OPERATOR( op_system_detach, data ) {
          __emit_comment_with_timestamp( _emitter_instance, data.tms, "DETACH", NULL );
          __emit_comment( _emitter_instance, "ORIGIN ", data.CSTR__origin_host );
          __emit_comment( _emitter_instance, "VERSION", data.CSTR__origin_version );
          BEGIN_EMIT_CRC {
            BUFFER_64( data.tms );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__origin_host );
          _OSTREAM_OPCSTRING( SEP, data.CSTR__origin_version );
          BEGIN_EMIT_CRC {
            BUFFER_32( data.status );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_system_clear_registry
      case OPCODE_SYSTEM_CLEAR_REGISTRY:
        BEGIN_OPERATOR( op_system_clear_registry, data ) {
          __emit_comment( _emitter_instance, "CLEAR REGISTRY", NULL );
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_system_similarity
      case OPCODE_SYSTEM_SIMILARITY:
        BEGIN_OPERATOR( op_system_similarity, data ) {
          BEGIN_EMIT_CRC {
            vgx_Similarity_config_t *s = &data.sim;
            BUFFER_32( s->fingerprint.nsegm );
            BUFFER_32( s->fingerprint.nsign );
            BUFFER_32( s->vector.max_size );
            BUFFER_32( s->vector.min_intersect );
            BUFFER_32( *(DWORD*)&s->vector.min_cosine );
            BUFFER_32( *(DWORD*)&s->vector.min_jaccard );
            BUFFER_32( *(DWORD*)&s->vector.cosine_exponent );
            BUFFER_32( *(DWORD*)&s->vector.jaccard_exponent );
            BUFFER_32( s->threshold.hamming );
            BUFFER_32( *(DWORD*)&s->threshold.similarity );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_system_create_graph
      case OPCODE_SYSTEM_CREATE_GRAPH:
        BEGIN_OPERATOR( op_system_create_graph, data ) {
          __emit_comment( _emitter_instance, "CREATE GRAPH", data.CSTR__name );
          __emit_comment( _emitter_instance, "PATH", data.CSTR__path );
          BEGIN_EMIT_CRC {
            BUFFER_32( data.vertex_block_order );
            BUFFER_32( data.graph_t0 );
            BUFFER_64( data.start_opcount );
            BUFFER_OBID( &data.obid );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__path );
          _OSTREAM_OPCSTRING( SEP, data.CSTR__name );
        } END_OPERATOR;

      // op_system_delete_graph
      case OPCODE_SYSTEM_DELETE_GRAPH:
        BEGIN_OPERATOR( op_system_delete_graph, data ) {
          __emit_comment( _emitter_instance, "DELETE GRAPH", NULL );
          BEGIN_EMIT_CRC {
            BUFFER_OBID( &data.obid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_system_send_comment
      case OPCODE_SYSTEM_SEND_COMMENT:
        BEGIN_OPERATOR( op_system_send_comment, data ) {
          __emit_comment( _emitter_instance, NULL, data.CSTR__comment );
          __queue_cstring_for_discard_OPEN( _emitter_instance->graph, _emitter_instance->cstring_discard, data.CSTR__comment );
        } END_OPERATOR;

      //
      case OPCODE_SYSTEM_SEND_RAW_DATA:
        BEGIN_OPERATOR( op_system_send_raw_data, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.n_parts );
            BUFFER_64( data.part_id );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__datapart );
          BEGIN_EMIT_CRC {
            BUFFER_64( data.sz_datapart );
            BUFFER_OBID( &data.obid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_graph_truncate
      case OPCODE_GRAPH_TRUNCATE:
        BEGIN_OPERATOR( op_graph_truncate, data ) {
          __emit_comment( _emitter_instance, "CX_TRUNCATE GRAPH", NULL );
          BEGIN_EMIT_CRC {
            BUFFER_08( (BYTE)data.vxtype );
            BUFFER_64( data.n_discarded );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_graph_persist
      case OPCODE_GRAPH_PERSIST:
        BEGIN_OPERATOR( op_graph_persist, data ) {
          BEGIN_EMIT_CRC {
            __emit_comment( _emitter_instance, "PERSIST GRAPH", NULL );
            vgx_graph_base_counts_t *c = &data.counts;
            BUFFER_64( c->order );
            BUFFER_64( c->size );
            BUFFER_64( c->nkey );
            BUFFER_64( c->nstrval );
            BUFFER_64( c->nprop );
            BUFFER_64( c->nvec );
            BUFFER_32( c->ndim );
            BUFFER_16( c->nrel );
            BUFFER_08( c->ntype );
            BUFFER_08( c->flags._bits );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_graph_state
      case OPCODE_GRAPH_STATE:
        BEGIN_OPERATOR( op_graph_state, data ) {
          BEGIN_EMIT_CRC {
            vgx_graph_base_counts_t *c = &data.counts;
            BUFFER_64( c->order );
            BUFFER_64( c->size );
            BUFFER_64( c->nkey );
            BUFFER_64( c->nstrval );
            BUFFER_64( c->nprop );
            BUFFER_64( c->nvec );
            BUFFER_32( c->ndim );
            BUFFER_16( c->nrel );
            BUFFER_08( c->ntype );
            BUFFER_08( c->flags._bits );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_vertices_lockstate
      case OPCODE_VERTICES_ACQUIRE_WL:
      case OPCODE_VERTICES_RELEASE:
        BEGIN_OPERATOR( op_vertices_lockstate, data ) {
          int64_t delta = (_operator.code == OPCODE_VERTICES_ACQUIRE_WL) ? data.count : -data.count;
          BEGIN_EMIT_CRC {
            BUFFER_32( data.count );
          } END_EMIT_CRC;
          _OSTREAM_OBIDLIST( data.obid_list, delta );
        } END_OPERATOR;

      // op_vertices_lockstate
      case OPCODE_VERTICES_RELEASE_ALL:
        BEGIN_OPERATOR( op_vertices_lockstate, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_32( data.count );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_graph_readonly
      case OPCODE_GRAPH_READONLY:
        BEGIN_OPERATOR( op_graph_readonly, data ) {
          __emit_comment( _emitter_instance, "READONLY GRAPH", NULL );
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_graph_readwrite
      case OPCODE_GRAPH_READWRITE:
        BEGIN_OPERATOR( op_graph_readwrite, data ) {
          __emit_comment( _emitter_instance, "READWRITE GRAPH", NULL );
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_graph_events
      case OPCODE_GRAPH_EVENTS:
        BEGIN_OPERATOR( op_graph_events, data ) {
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_graph_noevents
      case OPCODE_GRAPH_NOEVENTS:
        BEGIN_OPERATOR( op_graph_noevents, data ) {
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      // op_graph_tic
      case OPCODE_GRAPH_TICK:
        BEGIN_OPERATOR( op_graph_tic, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.tms );
            BUFFER_64( data.order );
            BUFFER_64( data.size );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_graph_event_exec
      case OPCODE_GRAPH_EVENT_EXEC:
        BEGIN_OPERATOR( op_graph_event_exec, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_32( data.ts );
            BUFFER_32( data.tmx );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_enum_add_string64
      case OPCODE_ENUM_ADD_VXTYPE:
      case OPCODE_ENUM_ADD_REL:
      case OPCODE_ENUM_ADD_DIM:
      case OPCODE_ENUM_ADD_KEY:
        BEGIN_OPERATOR( op_enum_add_string64, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.hash );
            BUFFER_64( data.encoded );
          } END_EMIT_CRC;
          _OSTREAM_OPCSTRING( SEP, data.CSTR__value );
        } END_OPERATOR;

      // op_enum_delete_string64
      case OPCODE_ENUM_DELETE_VXTYPE:
      case OPCODE_ENUM_DELETE_REL:
      case OPCODE_ENUM_DELETE_DIM:
      case OPCODE_ENUM_DELETE_KEY:
        BEGIN_OPERATOR( op_enum_delete_string64, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_64( data.hash );
            BUFFER_64( data.encoded );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_enum_add_string128
      case OPCODE_ENUM_ADD_STRING:
        BEGIN_OPERATOR( op_enum_add_string128, data ) {
          _OSTREAM_OPBUFFER();
          _OSTREAM_OPCSTRING( SEP, data.CSTR__value );
          BEGIN_EMIT_CRC {
            BUFFER_OBID( &data.obid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_enum_delete_string128
      case OPCODE_ENUM_DELETE_STRING:
        BEGIN_OPERATOR( op_enum_delete_string128, data ) {
          BEGIN_EMIT_CRC {
            BUFFER_OBID( &data.obid );
          } END_EMIT_CRC;
        } END_OPERATOR;

      // op_none
      default:
        BEGIN_OPERATOR( op_none, data ) {
          _OSTREAM_OPBUFFER();
        } END_OPERATOR;

      }

      // Break to commit
      if( _emitter_instance->commit_deadline_ms <= tms ) {
        break;
      }
    }

    // Reset operator buffer if fully consumed and capacity above threshold
    if( iBuf->Length( _opdatabuf ) == 0 ) {
      if( iBuf->Capacity( _opdatabuf ) > CAPTURE_OBJECT_RESET_THRESHOLD ) {
        iBuf->Reset( _opdatabuf );
      }
    }

    #ifdef OPSTREAM_INLINE_JSON
    EMIT_JSON_SNIPPET_CONST( "#     { \"op\": \"noOp\" }\n" );
    EMIT_JSON_SNIPPET_CONST( "#   ],\n" );
    #endif
    
    RESET_BUFFER();

    #ifdef OPSTREAM_INLINE_JSON
    {
      EMIT_JSON_SNIPPET_FORMAT( "#   \"commit\": { \"opid\": %lld, \"datetime\": \"", capture->opid );
      EMIT_JSON_SNIPPET_DATETIME( tms );
      EMIT_JSON_SNIPPET_CONST( "\" }\n# }\n" );
    }
    #endif

    BUFFER_ENDOP();

    BEGIN_EMIT_CRC {
      switch( optype ) {
      case OPTYPE_GRAPH_OBJECT:
      case OPTYPE_VERTEX_OBJECT:
        BUFFER_64( capture->opid );
        BUFFER_64( tms );
        break;
      case OPTYPE_SYSTEM:
      case OPTYPE_GRAPH_STATE:
      case OPTYPE_VERTEX_LOCK:
      case OPTYPE_VERTEX_RELEASE:
        break;
      default:
        break;
      }
    } END_EMIT_CRC;

  } END_OPERATION;


  // Transfer local buffer to emitter operation buffer
  __write_op( _graph_instance, _buffer_instance, __term_buffer( _buffer_instance, _buffer_cursor ) );

  #ifdef OPSTREAM_INLINE_JSON
  EMIT_JSON_SNIPPET_CONST( "#####\n" );
  #endif

  // One committed operation was emitted
  return 1;
}
#endif
