/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_produce_op.c
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




static int64_t  __ostream_string( vgx_Graph_t *graph, const char *str );
static int64_t  __ostream_stringsz( vgx_Graph_t *graph, const char *str, int64_t n );
static int64_t  __ostream_cstring( vgx_Graph_t *graph, const CString_t *CSTR__str );
static int64_t  __ostream_obid( vgx_Graph_t *graph, const objectid_t *obid );
static int64_t  __ostream_datetime( vgx_Graph_t *graph, int64_t ms );
static int64_t  __ostream_cstringmax( vgx_Graph_t *graph, const CString_t *CSTR__str, int64_t n );
static int64_t  __ostream_idlist_crc( vgx_Graph_t *graph, objectid_t **obid_list, int64_t count, int64_t *lxw, unsigned int *pcrc );
static char *   __reset_buffer( char *buffer, char **cursor );
static int64_t  __ostream_buffer( vgx_Graph_t *graph, char *buffer, char **cursor );
static int64_t  __ostream_buffer_crc( vgx_Graph_t *graph, char *buffer, char **cursor, unsigned int *pcrc );
static char **  __buffer_OP( char **cursor, OperationProcessorOpType optype );
static char **  __buffer_ENDOP( char **cursor );

static void     __queue_cstring_for_discard_OPEN( vgx_OperationEmitter_t *emitter, CString_t *CSTR__str );
static int64_t  __err_ostream( vgx_Graph_t *graph, int64_t dlen );
static int64_t  __ostream_data_sz( vgx_Graph_t *graph, const char *data, int64_t dlen );
static size_t   __ostream_opdata_crc( vgx_Graph_t *graph, const char *buf, int64_t n, unsigned int *pcrc );
static size_t   __ostream_opcstring_crc( vgx_OperationEmitter_t *emitter, const char *separator, CString_t *CSTR__string, unsigned int *pcrc );
static size_t   __term_buffer( char *buf, const char *cursor );
static void     __ostream_comment( vgx_OperationEmitter_t *emitter, int indent, const char *label, CString_t *CSTR__text );
static void     __ostream_comment_with_timestamp( vgx_OperationEmitter_t *emitter, int64_t tms, int indent, const char *label, CString_t *CSTR__text );
static int64_t  __set_commit_deadline( vgx_OperationEmitter_t *emitter, int64_t tms );
static bool     __commit_due( const vgx_OperationEmitter_t *emitter, int64_t tms );
static int64_t  __consume( CQwordBuffer_t *B, QWORD *p, size_t n );
static char **  __begin_operator( OperationProcessorOperator_t *op, char **cursor );
static OperationProcessorOpType __get_optype( comlib_object_typeinfo_t *typeinfo, OperationProcessorOperator_t *op );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_string( vgx_Graph_t *graph, const char *str ) {
  return __ostream_data_sz( graph, str, strlen( str ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_stringsz( vgx_Graph_t *graph, const char *str, int64_t n ) {
  return __ostream_data_sz( graph, str, n < 0 ? strlen(str) : n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_cstring( vgx_Graph_t *graph, const CString_t *CSTR__str ) {
  return __ostream_data_sz( graph, CStringValue( CSTR__str ), CStringLength( CSTR__str ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_obid( vgx_Graph_t *graph, const objectid_t *obid ) {
  char idbuf[33];
  return __ostream_data_sz( graph, idtostr( idbuf, obid ), 32 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_datetime( vgx_Graph_t *graph, int64_t ms ) {
  CString_t *CSTR__datetime = igraphinfo.CTime( ms, true );
  if( CSTR__datetime ) {
    int64_t n = __ostream_data_sz( graph, CStringValue( CSTR__datetime ), CStringLength( CSTR__datetime ) );
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
  int64_t bytes = CStringLength( CSTR__str );
  if( bytes <= n ) {
    return __ostream_cstring( graph, CSTR__str );
  }
  else {
    const BYTE *utf8 = (const BYTE*)CStringValue( CSTR__str );
    int64_t errpos = 0;
    int64_t uc;
    int64_t max_byte_offset = COMLIB_offset_utf8( utf8, -1, n, &uc, &errpos ); // max codepoint n -> max byte offset
    int64_t n_written = __ostream_stringsz( graph, CStringValue( CSTR__str ), max_byte_offset );
    if( n_written > 0 && max_byte_offset < bytes ) {
      if( __ostream_data_sz( graph, "...", 3 ) < 0 ) {
        return -1;
      }
      n_written += 3;
    }
    return n_written;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ostream_idlist_crc( vgx_Graph_t *graph, objectid_t **obid_list, int64_t count, int64_t *lxw, unsigned int *pcrc ) {
  objectid_t *obid = *obid_list;
  int64_t sz = count > 0 ? count : -count;
  objectid_t *end = *obid_list + sz;
  char id_data[34];
  id_data[0] = SEP[0];
  id_data[33] = '\0';
  char *idbuf = id_data + 1;
  while( obid < end ) {
    write_hex_objectid( idbuf, obid++ );
    __ostream_opdata_crc( graph, id_data, 33, pcrc );
  }
  free( *obid_list );
  *obid_list = NULL;
  if( *lxw + count < 0 ) {
    CRITICAL( 0x000, "Attempting to decrement lxw_balance from %lld to %lld. Setting to zero.", *lxw, *lxw + count );
    *lxw = 0;
  }
  else {
    *lxw += count;
  }
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
  __ostream_data_sz( graph, buffer, n );
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
  __ostream_opdata_crc( graph, buffer, n, pcrc );
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
#define _emitter_instance                     __emitter
#define _graph_instance                       __graph
#define _opdatabuf                            __dbuf
#define _buffer_instance                      __buffer
#define _buffer_cursor                        __cursor
#define _operator                             __op
#define _crc                                  __crc
#define _indent                               __indent

#define _OSTREAM_CONST( String )              __ostream_data_sz( _graph_instance, (String), sizeof(String)-1 )
#define _OSTREAM_STRING( Chars )              __ostream_string( _graph_instance, Chars )
#define _OSTREAM_STRINGSZ( Chars, Sz )        __ostream_stringsz( _graph_instance, Chars, Sz )
#define _OSTREAM_CSTRING( CSTR_x )            __ostream_cstring( _graph_instance, CSTR_x )
#define _OSTREAM_OBID( Obid )                 __ostream_obid( _graph_instance, Obid )
#define _OSTREAM_DATETIME( Millisec )         __ostream_datetime( _graph_instance, Millisec )
#define _OSTREAM_CSTRINGMAX( CSTR_x, N )      __ostream_cstringmax( _graph_instance, CSTR_x, N )

#define _OSTREAM_FORMAT( Format, ... )        __writebuf_format( &_buffer_cursor, Format, ##__VA_ARGS__ ); __ostream_buffer( _graph_instance, _buffer_instance, &_buffer_cursor )



#define _OSTREAM_BUFFER_CRC()                 __ostream_buffer_crc( _graph_instance, _buffer_instance, &_buffer_cursor, &_crc )
#define _OSTREAM_OPDATA_CRC( Buffer, Size )   __ostream_opdata_crc( _graph_instance, Buffer, Size, &_crc )
#define _OSTREAM_OPCSTRING_CRC( Sep, CSTR_x ) __ostream_opcstring_crc( _emitter_instance, Sep, CSTR_x, &_crc )



/**************************************************************************//**
 * __DEC_LXW_BALANCE
 *
 ******************************************************************************
 */
static int64_t __DEC_LXW_BALANCE( vgx_OperationEmitter_t *emitter ) {
  if( emitter->lxw_balance > 0 ) {
    return emitter->lxw_balance--;
  }
  else {
    CRITICAL( 0x000, "Attempting to decrement lxw_balance below zero" );
    return -1;
  }
}


#define _OSTREAM_IDLIST_CRC( Obids, Delta )   __ostream_idlist_crc( _graph_instance, &(Obids), Delta, &_emitter_instance->lxw_balance, &_crc )
#define INC_LXW_BALANCE()                     (_emitter_instance->lxw_balance++)
#define DEC_LXW_BALANCE()                     __DEC_LXW_BALANCE( _emitter_instance )

#define _OSTREAM_COMMENT( Label, CSTR_cmnt )  __ostream_comment( _emitter_instance, _indent, Label, CSTR_cmnt )

#define _OSTREAM_TIMESTAMPED_COMMENT( Tms, Label, CSTR_cmnt )  __ostream_comment_with_timestamp( _emitter_instance, tms, _indent, Label, CSTR_cmnt )

#define RESET_BUFFER()                        __reset_buffer( _buffer_instance, &_buffer_cursor )
#define BUFFER_OBID( Obid )                   __writebuf_op_objectid( &_buffer_cursor, Obid )
#define BUFFER_64( Qword )                    __writebuf_op_qword( &_buffer_cursor, Qword )
#define BUFFER_32( Dword )                    __writebuf_op_dword( &_buffer_cursor, Dword )
#define BUFFER_16( Word )                     __writebuf_op_word( &_buffer_cursor, Word )
#define BUFFER_08( Byte )                     __writebuf_op_byte( &_buffer_cursor, Byte )
#define BUFFER_STR( Str )                     __writebuf_string( &_buffer_cursor, Str )

#define BUFFER_OP( OpType )                   __buffer_OP( &_buffer_cursor, OpType )
#define BUFFER_ENDOP()                        __buffer_ENDOP( &_buffer_cursor )

#define DISCARD_CSTRING( CSTR_x )             __queue_cstring_for_discard_OPEN( _emitter_instance, CSTR_x )

#define BEGIN_OWN_CSTRING( CSTR_x )         \
do {                                        \
  CString_t *__CSTR__x = CSTR_x;            \
  icstringobject.IncrefNolock( __CSTR__x ); \
  do


#define END_OWN_CSTRING                     \
  WHILE_ZERO;                               \
  icstringobject.DecrefNolock( __CSTR__x ); \
}                                           \
WHILE_ZERO


#define CAPTURE_OBJECT_RESET_THRESHOLD        64          /* Capture object buffer is reset after emit if greater than this size */



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __queue_cstring_for_discard_OPEN( vgx_OperationEmitter_t *emitter, CString_t *CSTR__str ) {
  CQwordQueue_t *discard = emitter->cstring_discard;
  // Add cstring instance to queue for discard later
  if( discard ) {
    QWORD address = (QWORD)CSTR__str;
    if( CALLABLE( discard )->AppendNolock( discard, &address ) == 1 ) {
      return; // queued for discard
    }
  }

  // Could not queue string for discard, discard immediately
  GRAPH_LOCK( emitter->graph ) {
    icstringobject.DecrefNolock( CSTR__str );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

static int64_t __err_ostream( vgx_Graph_t *graph, int64_t dlen ) {
  CRITICAL( 0x001, "Failed to write %lld bytes to emitter op_buffer in graph '%s'", dlen, CALLABLE( graph )->FullPath( graph ) );
  iOpBuffer.DumpState( graph->OP.emitter.op_buffer );
  return -1;
}



/*******************************************************************//**
 * Returns number of bytes transferred into the operation buffer,
 * or negative on error.
 ***********************************************************************
 */
static int64_t __ostream_data_sz( vgx_Graph_t *graph, const char *data, int64_t dlen ) {
  int64_t n_bytes = 0;
  if( data && (n_bytes = iOpBuffer.Write( graph->OP.emitter.op_buffer, data, dlen )) < 0 ) {
    return __err_ostream( graph, dlen );
  }
  return n_bytes;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static size_t __ostream_opdata_crc( vgx_Graph_t *graph, const char *buf, int64_t n, unsigned int *pcrc ) {
  __opdata_crc32c( buf, n, pcrc );
  return __ostream_data_sz( graph, buf, n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static size_t __ostream_opcstring_crc( vgx_OperationEmitter_t *_emitter_instance, const char *separator, CString_t *CSTR__string, unsigned int *pcrc ) {
  size_t n_bytes = 0;
  vgx_Graph_t *graph = _emitter_instance->graph;

  char localbuf[512];
  char *output = NULL;
  if( icstringobject.SerializedSize( CSTR__string ) < 512 ) {
    output = localbuf;
  }

  int64_t sz_ser;
  if( (sz_ser = icstringobject.Serialize( &output, CSTR__string )) < 0 ) {
    CRITICAL( 0xFFF, "out of memory" );
    return 0;
  }
  
  n_bytes = __ostream_opdata_crc( graph, separator, 1, pcrc );
  n_bytes += __ostream_opdata_crc( graph, output, sz_ser, pcrc );

  if( output != localbuf ) {
    free( output );
  }

  DISCARD_CSTRING( CSTR__string );

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
static void __ostream_comment( vgx_OperationEmitter_t *emitter, int indent, const char *label, CString_t *CSTR__text ) {
  vgx_Graph_t *_graph_instance = emitter->graph;
  char _buffer_instance[1024];
  char *_buffer_cursor = _buffer_instance;
  char spaces[] = "           ";
  int i = minimum_value( indent, ((int)sizeof(spaces)-4) );
  spaces[i] = '\0';
  if( label ) {
    _OSTREAM_FORMAT( "%s# [%s] ", spaces, label );
  }
  else {
    _OSTREAM_FORMAT( "%s# ", spaces );
  }
  if( CSTR__text ) {
    spaces[0] = '\n';
    spaces[i++] = ' ';
    spaces[i++] = '#';
    spaces[++i] = '\0';
    CString_t *CSTR__safe = CALLABLE( CSTR__text )->Replace( CSTR__text, "\n", spaces );
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
static void __ostream_comment_with_timestamp( vgx_OperationEmitter_t *emitter, int64_t tms, int indent, const char *label, CString_t *CSTR__text ) {
  vgx_Graph_t *_graph_instance = emitter->graph;
  char _buffer_instance[1024];
  char *_buffer_cursor = _buffer_instance;
  char prefix[] = "           ";
  int i = minimum_value( indent, ((int)sizeof(prefix)-4) );
  prefix[i++] = '#';
  prefix[++i] = '[';
  prefix[++i] = '\0';
  _OSTREAM_STRINGSZ( prefix, i );
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
static bool __commit_due( const vgx_OperationEmitter_t *emitter, int64_t tms ) {
  return tms > emitter->commit_deadline_ms;
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
 ***********************************************************************
 */
static OperationProcessorOpType __get_optype( comlib_object_typeinfo_t *typeinfo, OperationProcessorOperator_t *op ) {
  if( typeinfo->tp_class == COMLIB_CLASS_CODE( vgx_Graph_t ) ) {
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
      return OPTYPE_SYSTEM;
      
    case OPCODE_GRAPH_TICK:
    case OPCODE_GRAPH_EVENT_EXEC:
    case OPCODE_GRAPH_READONLY:
    case OPCODE_GRAPH_READWRITE:
    case OPCODE_GRAPH_EVENTS:
    case OPCODE_GRAPH_NOEVENTS:
      return OPTYPE_GRAPH_STATE;

    case OPCODE_VERTICES_ACQUIRE_WL:
      return OPTYPE_VERTEX_LOCK;

    case OPCODE_VERTICES_RELEASE:
      return OPTYPE_VERTEX_RELEASE;

    case OPCODE_VERTEX_NEW:
    case OPCODE_VERTEX_DELETE:
    default:
      return OPTYPE_GRAPH_OBJECT;
    }
  }
  else if( typeinfo->tp_class == COMLIB_CLASS_CODE( vgx_Vertex_t ) ) {
    return OPTYPE_VERTEX_OBJECT;
  }
  else {
    return OPTYPE_NONE;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_OSTREAM     \
do {                      \
  do

  /* { ... } */

#define END_OSTREAM       \
  WHILE_ZERO;             \
  _OSTREAM_BUFFER_CRC();  \
} WHILE_ZERO



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_OPERATION_SWITCH( OpDataBuf, TypeInfoPtr, Opid, Tms )   \
do {                                                \
  vgx_Graph_t *_graph_instance = _emitter_instance->graph; \
  char _buffer_instance[1024];                      \
  char *_buffer_cursor;                             \
  unsigned int _crc = 0;                            \
  int _indent = 0;                                  \
  CQwordBuffer_t *_opdatabuf = OpDataBuf;           \
  OperationProcessorOperator_t _operator = {0};     \
  CQwordBuffer_vtable_t *__iBuf = CALLABLE( _opdatabuf ); \
  __iBuf->Get( _opdatabuf, 0, &_operator.data );      \
  OperationProcessorOpType __optype = __get_optype( TypeInfoPtr, &_operator );  \
  int64_t __opid = Opid;                            \
  int64_t __tms = Tms;                              \
  RESET_BUFFER();                                   \
  BUFFER_OP( __optype );                            \
  if( __optype != OPTYPE_SYSTEM ) {                 \
    BUFFER_OBID( &capture->graph->obid );           \
  }                                                 \
  if( __optype == OPTYPE_VERTEX_OBJECT ) {          \
    BUFFER_OBID( &capture->inheritable.objectid );  \
  }                                                 \
  BUFFER_STR( "\n" );                               \
  _OSTREAM_BUFFER_CRC();                            \
  do {                                              \
    __iBuf->Next( _opdatabuf, &_operator.data );    \
    switch( _operator.code ) /* {

      case x:
        continue;

      case y:
        continue;

    } */

#define END_OPERATION_SWITCH                              \
  } while( __iBuf->Length( _opdatabuf ) > 0               \
           &&                                             \
           !__commit_due( _emitter_instance, __tms ) );   \
  /* Reset operator buffer if fully consumed and capacity above threshold */ \
  if( __iBuf->Length( _opdatabuf ) == 0 ) {               \
    if( __iBuf->Capacity( _opdatabuf ) > CAPTURE_OBJECT_RESET_THRESHOLD ) { \
      __iBuf->Reset( _opdatabuf );                        \
    }                                                     \
  }                                                       \
  RESET_BUFFER();                                         \
  BUFFER_ENDOP();                                         \
  _emitter_instance->n_uncommitted.operations++;          \
  BEGIN_OSTREAM {                                         \
    if( __optype == OPTYPE_GRAPH_OBJECT                   \
        ||                                                \
        __optype == OPTYPE_VERTEX_OBJECT )                \
    {                                                     \
      BUFFER_64( __opid );                                \
      BUFFER_64( __tms );                                 \
    }                                                     \
  } END_OSTREAM;                                          \
  BUFFER_32( _crc );                                      \
  BUFFER_STR( "\n" );                                     \
  /* Transfer local buffer to emitter operation buffer */ \
  __ostream_data_sz( _graph_instance, _buffer_instance, __term_buffer( _buffer_instance, _buffer_cursor ) ); \
} WHILE_ZERO



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define OPCODE_CASE( OPCODE )   case OPCODE




#define OPERATOR_CASE( OpType, Data )                           \
  do {                                                          \
    RESET_BUFFER();                                             \
    __begin_operator( &_operator, &_buffer_cursor );            \
    OpType Data;                                                \
    (Data).op.code = _operator.code;                            \
    __consume( _opdatabuf, (Data).qwords+1, qwsizeof(Data)-1 ); \
    do /* {
    

    } */


#define END_CASE                                  \
    WHILE_ZERO;                                   \
    _OSTREAM_OPDATA_CRC( "\n", 1 );               \
    _emitter_instance->n_uncommitted.opcodes++;   \
  } WHILE_ZERO;                                   \
  continue



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_OPERATOR( OPCODE, OpType, Data )  \
case OPCODE:                                    \
  _indent = 4;                                  \
  OPERATOR_CASE( OpType, Data )

#define END_OPERATOR  END_CASE



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define BEGIN_DEFAULT( OpType, Data ) \
default:                              \
  OPERATOR_CASE( OpType, Data )

#define END_DEFAULT  END_CASE



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_produce_op__emit_OPEN( vgx_OperationEmitter_t *_emitter_instance, int64_t tms, vgx_OperatorCapture_t *capture ) {

  if( ComlibSequenceLength( capture->opdatabuf ) < 1 ) {
    return 0;
  }

  BEGIN_OPERATION_SWITCH( capture->opdatabuf, &capture->inheritable.object_typeinfo, capture->opid, tms ) {

    // --------------------------------
    // vxn
    BEGIN_OPERATOR( OPCODE_VERTEX_NEW, op_vertex_new, data ) {
      BEGIN_OSTREAM {
        BUFFER_OBID( &data.obid );
        BUFFER_08( (BYTE)data.vxtype );
        BUFFER_32( data.TMC );
        BUFFER_32( data.TMX.vertex_ts );
        BUFFER_32( data.TMX.arc_ts );
        BUFFER_64( data.rank.bits );
      } END_OSTREAM;
      BEGIN_OWN_CSTRING( data.CSTR__id ) {
        _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__id );
        _OSTREAM_CONST( " # " );
        _OSTREAM_CSTRINGMAX( data.CSTR__id, 40 );
      } END_OWN_CSTRING;
    } END_OPERATOR;


    // --------------------------------
    // vxd
    BEGIN_OPERATOR( OPCODE_VERTEX_DELETE, op_vertex_delete, data ) {
      BEGIN_OSTREAM {
        BUFFER_OBID( &data.obid );
        BUFFER_08( (BYTE)data.eventexec );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vxr
    BEGIN_OPERATOR( OPCODE_VERTEX_SET_RANK, op_vertex_set_rank, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.rank.bits );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vxt
    BEGIN_OPERATOR( OPCODE_VERTEX_SET_TYPE, op_vertex_set_type, data ) {
      BEGIN_OSTREAM {
        BUFFER_08( (BYTE)data.vxtype);
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vxx
    BEGIN_OPERATOR( OPCODE_VERTEX_SET_TMX, op_vertex_set_tmx, data ) {
      BEGIN_OSTREAM {
        BUFFER_32( data.tmx );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vxc
    BEGIN_OPERATOR( OPCODE_VERTEX_CONVERT, op_vertex_convert, data ) {
      BEGIN_OSTREAM {
        BUFFER_08( data.manifestation );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vps
    BEGIN_OPERATOR( OPCODE_VERTEX_SET_PROPERTY, op_vertex_set_property, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.key );
        BUFFER_08( data.vtype );
        BUFFER_64( data.dataH );
        BUFFER_64( data.dataL );
      } END_OSTREAM;
      if( data.CSTR__value ) {
        _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
      }
    } END_OPERATOR;


    // --------------------------------
    // vpd
    BEGIN_OPERATOR( OPCODE_VERTEX_DELETE_PROPERTY, op_vertex_delete_property, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.key );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vpc
    BEGIN_OPERATOR( OPCODE_VERTEX_CLEAR_PROPERTIES, op_vertex_clear_properties, data ) {
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;


    // --------------------------------
    // vvs
    BEGIN_OPERATOR( OPCODE_VERTEX_SET_VECTOR, op_vertex_set_vector, data ) {
      _OSTREAM_BUFFER_CRC();
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__vector );
    } END_OPERATOR;


    // --------------------------------
    // vvd
    BEGIN_OPERATOR( OPCODE_VERTEX_DELETE_VECTOR, op_vertex_delete_vector, data ) {
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;


    // --------------------------------
    // vod
    BEGIN_OPERATOR( OPCODE_VERTEX_DELETE_OUTARCS, op_vertex_delete_outarcs, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.n_removed );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vid
    BEGIN_OPERATOR( OPCODE_VERTEX_DELETE_INARCS, op_vertex_delete_inarcs, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.n_removed );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // val
    BEGIN_OPERATOR( OPCODE_VERTEX_ACQUIRE, op_vertex_acquire, data ) {
      _OSTREAM_BUFFER_CRC();
      INC_LXW_BALANCE();
    } END_OPERATOR;


    // --------------------------------
    // vrl
    BEGIN_OPERATOR( OPCODE_VERTEX_RELEASE, op_vertex_release, data ) {
      _OSTREAM_BUFFER_CRC();
      DEC_LXW_BALANCE();
    } END_OPERATOR;


    // --------------------------------
    // arc
    BEGIN_OPERATOR( OPCODE_ARC_CONNECT, op_arc_connect, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.pred.data );
        BUFFER_OBID( &data.headobid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // ard
    BEGIN_OPERATOR( OPCODE_ARC_DISCONNECT, op_arc_disconnect, data ) {
      BEGIN_OSTREAM {
        BUFFER_08( (BYTE)data.eventexec );
        BUFFER_64( data.n_removed );
        BUFFER_64( data.pred.data );
        BUFFER_OBID( &data.headobid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // sya
    BEGIN_OPERATOR( OPCODE_SYSTEM_ATTACH, op_system_attach, data ) {
      _OSTREAM_TIMESTAMPED_COMMENT( data.tms, "ATTACH", NULL );
      _OSTREAM_COMMENT( "ORIGIN ", data.CSTR__origin_host );
      _OSTREAM_COMMENT( "VERSION", data.CSTR__origin_version );
      BEGIN_OSTREAM {
        BUFFER_64( data.tms );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__via_uri );
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__origin_host );
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__origin_version );
      BEGIN_OSTREAM {
        BUFFER_32( data.status );
      } END_OSTREAM;
      // Immediate commit
      __set_commit_deadline( _emitter_instance, tms );
    } END_OPERATOR;

    
    // --------------------------------
    // syd
    BEGIN_OPERATOR( OPCODE_SYSTEM_DETACH, op_system_detach, data ) {
      _OSTREAM_TIMESTAMPED_COMMENT( data.tms, "DETACH", NULL );
      _OSTREAM_COMMENT( "ORIGIN ", data.CSTR__origin_host );
      _OSTREAM_COMMENT( "VERSION", data.CSTR__origin_version );
      BEGIN_OSTREAM {
        BUFFER_64( data.tms );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__origin_host );
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__origin_version );
      BEGIN_OSTREAM {
        BUFFER_32( data.status );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // rcl
    BEGIN_OPERATOR( OPCODE_SYSTEM_CLEAR_REGISTRY, op_system_clear_registry, data ) {
      _OSTREAM_COMMENT( "CLEAR REGISTRY", NULL );
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;


    // --------------------------------
    // scf
    BEGIN_OPERATOR( OPCODE_SYSTEM_SIMILARITY, op_system_similarity, data ) {
      BEGIN_OSTREAM {
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
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // grn
    BEGIN_OPERATOR( OPCODE_SYSTEM_CREATE_GRAPH, op_system_create_graph, data ) {
      _OSTREAM_COMMENT( "CREATE GRAPH", data.CSTR__name );
      _OSTREAM_COMMENT( "PATH", data.CSTR__path );
      BEGIN_OSTREAM {
        BUFFER_32( data.vertex_block_order );
        BUFFER_32( data.graph_t0 );
        BUFFER_64( data.start_opcount );
        BUFFER_OBID( &data.obid );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__path );
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__name );
    } END_OPERATOR;


    // --------------------------------
    // grd
    BEGIN_OPERATOR( OPCODE_SYSTEM_DELETE_GRAPH, op_system_delete_graph, data ) {
      _OSTREAM_COMMENT( "DELETE GRAPH", NULL );
      BEGIN_OSTREAM {
        BUFFER_OBID( &data.obid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // com
    BEGIN_OPERATOR( OPCODE_SYSTEM_SEND_COMMENT, op_system_send_comment, data ) {
      _OSTREAM_BUFFER_CRC();
      BEGIN_OWN_CSTRING( data.CSTR__comment ) {
        _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__comment );
        _OSTREAM_CONST( " # " );
        _OSTREAM_CSTRINGMAX( data.CSTR__comment, 1024 );
      } END_OWN_CSTRING;
    } END_OPERATOR;


    // --------------------------------
    // dat
    BEGIN_OPERATOR( OPCODE_SYSTEM_SEND_RAW_DATA, op_system_send_raw_data, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.n_parts );
        BUFFER_64( data.part_id );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__datapart );
      BEGIN_OSTREAM {
        BUFFER_64( data.sz_datapart );
        BUFFER_32( data.cmd );
        BUFFER_32( data._rsv );
        BUFFER_OBID( &data.obid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // grt
    BEGIN_OPERATOR( OPCODE_GRAPH_TRUNCATE, op_graph_truncate, data ) {
      _OSTREAM_COMMENT( "CX_TRUNCATE GRAPH", NULL );
      BEGIN_OSTREAM {
        BUFFER_08( (BYTE)data.vxtype );
        BUFFER_64( data.n_discarded );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // grp
    BEGIN_OPERATOR( OPCODE_GRAPH_PERSIST, op_graph_persist, data ) {
      BEGIN_OSTREAM {
        _OSTREAM_COMMENT( "PERSIST GRAPH", NULL );
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
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // grs
    BEGIN_OPERATOR( OPCODE_GRAPH_STATE, op_graph_state, data ) {
      BEGIN_OSTREAM {
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
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // lxw
    BEGIN_OPERATOR( OPCODE_VERTICES_ACQUIRE_WL, op_vertices_lockstate, data ) {
      BEGIN_OSTREAM {
        BUFFER_32( data.count );
      } END_OSTREAM;
      _OSTREAM_IDLIST_CRC( data.obid_list, data.count );
    } END_OPERATOR;


    // --------------------------------
    // ulv
    BEGIN_OPERATOR( OPCODE_VERTICES_RELEASE, op_vertices_lockstate, data ) {
      BEGIN_OSTREAM {
        BUFFER_32( data.count );
      } END_OSTREAM;
      _OSTREAM_IDLIST_CRC( data.obid_list, -data.count );
    } END_OPERATOR;


    // --------------------------------
    // ula
    BEGIN_OPERATOR( OPCODE_VERTICES_RELEASE_ALL, op_vertices_lockstate, data ) {
      BEGIN_OSTREAM {
        BUFFER_32( data.count );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // grr
    BEGIN_OPERATOR( OPCODE_GRAPH_READONLY, op_graph_readonly, data ) {
      _OSTREAM_COMMENT( "READONLY GRAPH", NULL );
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;

    
    // --------------------------------
    // grw
    BEGIN_OPERATOR( OPCODE_GRAPH_READWRITE, op_graph_readwrite, data ) {
      _OSTREAM_COMMENT( "READWRITE GRAPH", NULL );
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;

    
    // --------------------------------
    // gre
    BEGIN_OPERATOR( OPCODE_GRAPH_EVENTS, op_graph_events, data ) {
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;

    
    // --------------------------------
    // gri
    BEGIN_OPERATOR( OPCODE_GRAPH_NOEVENTS, op_graph_noevents, data ) {
      _OSTREAM_BUFFER_CRC();
    } END_OPERATOR;


    // --------------------------------
    // tic
    BEGIN_OPERATOR( OPCODE_GRAPH_TICK, op_graph_tic, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.tms );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // evx
    BEGIN_OPERATOR( OPCODE_GRAPH_EVENT_EXEC, op_graph_event_exec, data ) {
      BEGIN_OSTREAM {
        BUFFER_32( data.ts );
        BUFFER_32( data.tmx );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // vea
    BEGIN_OPERATOR( OPCODE_ENUM_ADD_VXTYPE, op_enum_add_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
    } END_OPERATOR;


    // --------------------------------
    // ved
    BEGIN_OPERATOR( OPCODE_ENUM_DELETE_VXTYPE, op_enum_delete_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // rea
    BEGIN_OPERATOR( OPCODE_ENUM_ADD_REL, op_enum_add_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
    } END_OPERATOR;


    // --------------------------------
    // red
    BEGIN_OPERATOR( OPCODE_ENUM_DELETE_REL, op_enum_delete_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // dea
    BEGIN_OPERATOR( OPCODE_ENUM_ADD_DIM, op_enum_add_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
    } END_OPERATOR;


    // --------------------------------
    // ded
    BEGIN_OPERATOR( OPCODE_ENUM_DELETE_DIM, op_enum_delete_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // kea
    BEGIN_OPERATOR( OPCODE_ENUM_ADD_KEY, op_enum_add_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
    } END_OPERATOR;


    // --------------------------------
    // ked
    BEGIN_OPERATOR( OPCODE_ENUM_DELETE_KEY, op_enum_delete_string64, data ) {
      BEGIN_OSTREAM {
        BUFFER_64( data.hash );
        BUFFER_64( data.encoded );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // sea
    BEGIN_OPERATOR( OPCODE_ENUM_ADD_STRING, op_enum_add_string128, data ) {
      _OSTREAM_BUFFER_CRC();
      _OSTREAM_OPCSTRING_CRC( SEP, data.CSTR__value );
      BEGIN_OSTREAM {
        BUFFER_OBID( &data.obid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // sed
    BEGIN_OPERATOR( OPCODE_ENUM_DELETE_STRING, op_enum_delete_string128, data ) {
      BEGIN_OSTREAM {
        BUFFER_OBID( &data.obid );
      } END_OSTREAM;
    } END_OPERATOR;


    // --------------------------------
    // nop
    BEGIN_DEFAULT( op_none, data ) {
      _OSTREAM_BUFFER_CRC();
    } END_DEFAULT;

  } END_OPERATION_SWITCH;

  // One operation emitted
  return 1;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_produce_op.h"
  
test_descriptor_t _vgx_vxdurable_operation_produce_op_tests[] = {
  { "VGX Graph Durable Operation Produce Operation Tests", __utest_vxdurable_operation_produce_op },
  {NULL}
};
#endif
