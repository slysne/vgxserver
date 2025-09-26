/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    opcodes.h
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

#ifndef _VGX_VXDURABLE_OPCODES_H
#define _VGX_VXDURABLE_OPCODES_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define LONG_TIMEOUT 10000



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const int64_t MAX_IGNORE_WARNINGS = 16;
static __THREAD int64_t warn_ignore_tail_count = 0;
static __THREAD int64_t warn_ignore_head_count = 0;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define PARSER_INC_VERTEX_LOCKS( Parser, N )          ((Parser)->control.n_locks += (int)(N))
#define PARSER_DEC_VERTEX_LOCKS( Parser, N )          ((Parser)->control.n_locks -= (int)(N))
#define PARSER_HAS_VERTEX_LOCKS( Parser )             ((Parser)->control.n_locks != 0)
#define PARSER_GET_VERTEX_LOCKS( Parser )             ((Parser)->control.n_locks)
#define PARSER_RESET_VERTEX_LOCKS( Parser )           ((Parser)->control.n_locks = 0)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define PARSER_SET_REASON( Parser, Reason )                         __set_access_reason( &(Parser)->reason, Reason )
#define PARSER_SET_ERROR( Parser, Message )                         __set_error_string( &(Parser)->CSTR__error, Message )
#define PARSER_FORMAT_ERROR( Parser, Format, ... )                  __format_error_string( &(Parser)->CSTR__error, Format, ##__VA_ARGS__ )
#define PARSER_SET_ERROR_REASON( Parser, Message, Reason )          PARSER_SET_ERROR( Parser, Message ); PARSER_SET_REASON( Parser, Reason )
#define PARSER_FORMAT_ERROR_REASON( Parser, Reason, Format, ... )   PARSER_FORMAT_ERROR( Parser, Format, ##__VA_ARGS__ ); PARSER_SET_REASON( Parser, Reason )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __PARSER_MESSAGE( LEVEL, Parser, Code, Format, ... ) LEVEL( Code, "TX::PAR(%s): " Format, __full_path_parser( Parser ), ##__VA_ARGS__ )
#define PARSER_VERBOSE( Parser, Code, Format, ... )   __PARSER_MESSAGE( VERBOSE, Parser, Code, Format, ##__VA_ARGS__ )
#define PARSER_INFO( Parser, Code, Format, ... )      __PARSER_MESSAGE( INFO, Parser, Code, Format, ##__VA_ARGS__ )
#define PARSER_WARNING( Parser, Code, Format, ... )   __PARSER_MESSAGE( WARN, Parser, Code, Format, ##__VA_ARGS__ )
#define PARSER_REASON( Parser, Code, Format, ... )    __PARSER_MESSAGE( REASON, Parser, Code, Format, ##__VA_ARGS__ )
#define PARSER_CRITICAL( Parser, Code, Format, ... )  __PARSER_MESSAGE( CRITICAL, Parser, Code, Format, ##__VA_ARGS__ )
#define PARSER_FATAL( Parser, Code, Format, ... )     __PARSER_MESSAGE( FATAL, Parser, Code, Format, ##__VA_ARGS__ )



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __operation_parser_syntax_error( vgx_OperationParser_t *parser, const char *msg ) {
  // Log error
  char *token = calloc( parser->token.len + 1, 1 );
  if( token ) {
    strncpy( token, parser->token.data, parser->token.len );
  }
  PARSER_CRITICAL( parser, 0x001, "Bad input at token '%s', %s. (state=%x optype=%x op=%s)", token ? token : "???", msg, parser->state, parser->optype, parser->op_mnemonic );
  free( token );
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __trap_operator_error( op_BASE *op, const char *funcname, int line ) {
  if( op ) {
    REASON( 0x001, "(trap error) %s:%d %08X %s", funcname, line, op->op.code, op->op.name );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef HASVERBOSE
__inline static void __trap_operator_ignore( op_BASE *op, const char *funcname, int line ) {
  if( op ) {
    VERBOSE( 0x001, "(trap ignore) %s:%d %08X %s", funcname, line, op->op.code, op->op.name );
  }
}
#else
#define __trap_operator_ignore( op, funcname, line )  ((void)0)
#endif



#define OPERATOR_RETURN( Operator, Value )  __retcode__ = (Value); goto __end_op_return__
#define OPERATOR_CONTINUE( Operator )       OPERATOR_RETURN( Operator, 0 )
#define OPERATOR_COMPLETE( Operator )       OPERATOR_RETURN( Operator, 1 )
#define OPERATOR_ERROR( Operator )          __trap_operator_error( (op_BASE*)(Operator), __FUNCTION__, __LINE__ ); OPERATOR_RETURN( Operator, -1 )
#define OPERATOR_IGNORE( Operator )         __trap_operator_ignore( (op_BASE*)(Operator), __FUNCTION__, __LINE__ ); OPERATOR_RETURN( Operator, 0 )
#define OPERATOR_SUCCESS( Operator )        OPERATOR_RETURN( Operator, 0 )

#define END_OPERATOR_RETURN                 \
  else {                                    \
    if( CharsEqualsConst( op->op.name, OP_NAME_NOP ) ) { \
      OPERATOR_COMPLETE( op );              \
    }                                       \
    else if( *op->op.name == 0 ) {          \
      OPERATOR_CONTINUE( op );              \
    }                                       \
    else {                                  \
      OPERATOR_ERROR( op );                 \
    }                                       \
  }                                         \
__end_op_return__:                          \
  return __retcode__;                       \
} WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __op_name( vgx_OperationParser_t *parser ) {
  return parser->OPERATOR.op.name;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __op_vertex( vgx_OperationParser_t *parser ) {
  return parser->op_vertex_WL ? CALLABLE( parser->op_vertex_WL )->IDPrefix( parser->op_vertex_WL ) : "";
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef HASVERBOSE
__inline static void __trap_parser_error( const char *funcname, int line ) {
  VERBOSE( 0x001, "%s:%d", funcname, line );
}
#else
#define __trap_parser_error( funcname, line ) ((void)0)
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __OPEXEC_MESSAGE( LEVEL, IsError, Trap, Parser, Format, ... )   \
  do {                                                                  \
    char __tx[33];                                                      \
    idtostr( __tx, &(Parser)->transid );                                \
    const char *__vx = __op_vertex( Parser );                           \
    const char *__op = __op_name( Parser );                             \
    if( strlen( __vx ) > 0 ) {                                          \
      LEVEL( 0, "TX::EXE(%s): tx='%s' vx='%s' op='%s' " Format, __full_path_parser( Parser ), __tx, __vx, __op, ##__VA_ARGS__ );  \
    }                                                                   \
    else {                                                              \
      LEVEL( 0, "TX::EXE(%s): tx='%s' op='%s' " Format, __full_path_parser( Parser ), __tx, __op, ##__VA_ARGS__ );  \
    }                                                                   \
    if( Trap ) {                                                        \
      __trap_parser_error( __FUNCTION__, __LINE__ );                    \
    }                                                                   \
    if( IsError ) {                                                     \
      __format_error_string( &(Parser)->CSTR__error, "%s:%d", __FUNCTION__, __LINE__ );  \
    }                                                                   \
  } WHILE_ZERO


#define OPEXEC_VERBOSE( Parser, Format, ... )   __OPEXEC_MESSAGE( VERBOSE, false, false, Parser, Format, ##__VA_ARGS__ )
#define OPEXEC_INFO( Parser, Format, ... )      __OPEXEC_MESSAGE( INFO, false, false, Parser, Format, ##__VA_ARGS__ )
#define OPEXEC_WARNING( Parser, Format, ... )   __OPEXEC_MESSAGE( WARN, false, true, Parser, Format, ##__VA_ARGS__ )
#define OPEXEC_REASON( Parser, Format, ... )    __OPEXEC_MESSAGE( REASON, true, true, Parser, Format, ##__VA_ARGS__ )
#define OPEXEC_CRITICAL( Parser, Format, ... )  __OPEXEC_MESSAGE( CRITICAL, true, true, Parser, Format, ##__VA_ARGS__ )
#define OPEXEC_FATAL( Parser, Format, ... )     __OPEXEC_MESSAGE( FATAL, true, true, Parser, Format, ##__VA_ARGS__ )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#include "execf.h"
#include "oputil.h"



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __operation_parser_lock_graph( vgx_OperationParser_t *parser ) {
  if( parser->__locked_graph == NULL ) {
    vgx_Graph_t *graph = parser->op_graph;

    // *** LOCK ***
    parser->__locked_graph = GRAPH_ENTER_CRITICAL_SECTION( graph );

    // Make sure graph is writable and transaction serial number is acceptable
    if( !__operation_parser_gate_check_CS( parser, parser->__locked_graph ) ) {
      // *** UNLOCK ***
      GRAPH_LEAVE_CRITICAL_SECTION( &parser->__locked_graph );
      parser->__locked_graph = NULL;
    }
  }
  return parser->__locked_graph;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __operation_parser_release_graph( vgx_OperationParser_t *parser ) {
  if( parser->__locked_graph ) {
    __operation_parser_gate_exit_CS( parser, parser->__locked_graph );
  }
  // *** UNLOCK ***
  GRAPH_LEAVE_CRITICAL_SECTION( &parser->__locked_graph );
  parser->__locked_graph = NULL;
}



/*******************************************************************//**
 * Acquired graph context
 *
 ***********************************************************************
 */
#define BEGIN_ACQUIRE_GRAPH( Parser )       \
do {                                        \
  vgx_Graph_t *graph_CS = __operation_parser_lock_graph( Parser ); \
  if( graph_CS ) /* { 
    CODE HERE 
  } */

#define END_ACQUIRE_GRAPH                   \
  else {                                    \
    return _vxdurable_operation__trap_error( NULL, false, 0, "failed to acquire graph lock" );  \
  }                                         \
} WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __assert_operator_vertex( vgx_OperationParser_t *parser ) {
  if( parser->op_vertex_WL && __vertex_is_locked_writable_by_current_thread( parser->op_vertex_WL ) ) {
    return parser->op_vertex_WL;
  }
  else {
    PARSER_SET_ERROR( parser, "bad operator context, vertex is NULL" );
    _vxdurable_operation__trap_error( NULL, false, 0, "bad operator context" );
    return NULL;
  }
}



/*******************************************************************//**
 * Writelocked vertex context
 *
 ***********************************************************************
 */
#define BEGIN_WRITELOCKED_VERTEX( Parser )      \
do {                                            \
  vgx_Vertex_t *vertex_WL = __assert_operator_vertex( Parser );  \
  if( vertex_WL ) /* { 
    CODE HERE 
  } */


#define END_WRITELOCKED_VERTEX                \
  else {                                      \
    return _vxdurable_operation__trap_error( NULL, false, 0, "vertex lock not owned by operation parser" );   \
  }                                           \
} WHILE_ZERO



/*******************************************************************//**
 * Operator context
 *
 ***********************************************************************
 */
#define BEGIN_OPERATOR( Struct, Parser )    \
do {                                        \
  int __retcode__ = 0;                      \
  Struct *op = (Struct*)&parser->OPERATOR;  \
  if( op->op.code != OPCODE_NONE ) /* { 
    CODE HERE 
  } */


#define END_OPERATOR                        \
} WHILE_ZERO



/*******************************************************************//**
 * Graph operator context
 *
 ***********************************************************************
 */
#define BEGIN_GRAPH_OPERATOR( Struct, Parser )  \
  BEGIN_OPERATOR( Struct, Parser ) {            \
    BEGIN_ACQUIRE_GRAPH( Parser ) /* { 
      CODE HERE 
    } */

#define END_GRAPH_OPERATOR                      \
    END_ACQUIRE_GRAPH;                          \
  } END_OPERATOR

#define END_GRAPH_OPERATOR_RETURN               \
    END_ACQUIRE_GRAPH;                          \
  } END_OPERATOR_RETURN




/*******************************************************************//**
 * Vertex operator context
 *
 ***********************************************************************
 */
#define BEGIN_VERTEX_OPERATOR( Struct, Parser ) \
  BEGIN_OPERATOR( Struct, Parser ) {            \
    BEGIN_WRITELOCKED_VERTEX( Parser ) /* { 
      CODE HERE 
    } */


#define END_VERTEX_OPERATOR                     \
    END_WRITELOCKED_VERTEX;                     \
  } END_OPERATOR


#define END_VERTEX_OPERATOR_RETURN              \
    END_WRITELOCKED_VERTEX;                     \
  } END_OPERATOR_RETURN






#define VertexSetRank( Vertex, Rank )                   ((Vertex)->rank.bits = (Rank)->bits)
#define VertexSetType( Vertex, Type )                   _vxgraph_vxtable__reindex_vertex_type_CS_WL( (Vertex)->graph, Vertex, Type )
#define VertexSetTMX( Vertex, TMX )                     CALLABLE( Vertex )->SetExpirationTime( Vertex, TMX )
#define VertexConvert( Vertex, Man )                    _vxgraph_state__convert_WL( (Vertex)->graph, Vertex, Man, VGX_VERTEX_RECORD_OPERATION )
#define VertexSetProperty( Vertex, Property )           CALLABLE( Vertex )->SetProperty( Vertex, &(Property) )
#define VertexDeleteProperty( Vertex, Property )        CALLABLE( Vertex )->RemoveProperty( Vertex, &(Property) )
#define VertexClearProperties( Vertex )                 CALLABLE( Vertex )->RemoveProperties( Vertex )
#define VertexSetVector( Vertex, pVector )              CALLABLE( Vertex )->SetVector( Vertex, pVector )
#define VertexDeleteVector( Vertex )                    CALLABLE( Vertex )->RemoveVector( Vertex )
#define VertexDeleteOutarcs( Vertex )                   CALLABLE( Vertex )->RemoveOutarcs( Vertex )
#define VertexOutDegree( Vertex )                       CALLABLE( Vertex )->OutDegree( Vertex )
#define VertexDeleteInarcs( Vertex )                    CALLABLE( Vertex )->RemoveInarcs( Vertex )
#define VertexInDegree( Vertex )                        CALLABLE( Vertex )->InDegree( Vertex )
#define VertexIdentifier( Vertex )                      CALLABLE( Vertex )->IDString( Vertex )
#define VertexIdPrefix( Vertex )                        CALLABLE( Vertex )->IDPrefix( Vertex )






#include "nop/_op_nop.h"
#include "nop/_op_null.h"
#include "nop/_op_deny.h"

#include "vertex/_op_vxn.h"
#include "vertex/_op_vxd.h"
#include "vertex/_op_vxr.h"
#include "vertex/_op_vxt.h"
#include "vertex/_op_vxx.h"
#include "vertex/_op_vxc.h"

#include "property/_op_vps.h"
#include "property/_op_vpd.h"
#include "property/_op_vpc.h"

#include "vector/_op_vvs.h"
#include "vector/_op_vvd.h"

#include "arc/_op_vod.h"
#include "arc/_op_vid.h"

#include "lock/_op_val.h"
#include "lock/_op_vrl.h"

#include "arc/_op_arc.h"
#include "arc/_op_ard.h"

#include "system/_op_sya.h"
#include "system/_op_syd.h"

#include "system/_op_rcl.h"
#include "system/_op_scf.h"

#include "graph/_op_grn.h"
#include "graph/_op_grd.h"

#include "system//_op_com.h"
#include "system/_op_dat.h"

#include "graph/_op_clg.h"

#include "graph/_op_grt.h"
#include "graph/_op_grp.h"
#include "graph/_op_grs.h"

#include "graph/_op_grr.h"
#include "graph/_op_grw.h"
#include "graph/_op_gre.h"
#include "graph/_op_gri.h"

#include "system/_op_tic.h"
#include "graph/_op_evx.h"

#include "lock/_op_lockstate.h"
#include "lock/_op_lxw.h"
#include "lock/_op_lsr.h"
#include "lock/_op_ulv.h"
#include "lock/_op_ula.h"

#include "enum/_op_add_string64.h"
#include "enum/_op_del_string64.h"
#include "enum/_op_add_string128.h"
#include "enum/_op_del_string128.h"
#include "enum/_op_vea.h"
#include "enum/_op_ved.h"
#include "enum/_op_rea.h"
#include "enum/_op_red.h"
#include "enum/_op_dea.h"
#include "enum/_op_ded.h"
#include "enum/_op_kea.h"
#include "enum/_op_ked.h"
#include "enum/_op_sea.h"
#include "enum/_op_sed.h"




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int g_execf_map_init_SYS_CS = 0;




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __execf_map g_execf_map = {
  .operator__op_none                       = { .execf = { 0 },  .op = OPERATOR_NONE },
  .operator__op_vertex_new                 = { .execf = { 0 },  .op = OPERATOR_VERTEX_NEW },
  .operator__op_vertex_delete              = { .execf = { 0 },  .op = OPERATOR_VERTEX_DELETE },
  .operator__op_vertex_set_rank            = { .execf = { 0 },  .op = OPERATOR_VERTEX_SET_RANK },
  .operator__op_vertex_set_type            = { .execf = { 0 },  .op = OPERATOR_VERTEX_SET_TYPE },
  .operator__op_vertex_set_tmx             = { .execf = { 0 },  .op = OPERATOR_VERTEX_SET_TMX },
  .operator__op_vertex_convert             = { .execf = { 0 },  .op = OPERATOR_VERTEX_CONVERT },
  .operator__op_vertex_set_property        = { .execf = { 0 },  .op = OPERATOR_VERTEX_SET_PROPERTY },
  .operator__op_vertex_delete_property     = { .execf = { 0 },  .op = OPERATOR_VERTEX_DELETE_PROPERTY },
  .operator__op_vertex_clear_properties    = { .execf = { 0 },  .op = OPERATOR_VERTEX_CLEAR_PROPERTIES },
  .operator__op_vertex_set_vector          = { .execf = { 0 },  .op = OPERATOR_VERTEX_SET_VECTOR },
  .operator__op_vertex_delete_vector       = { .execf = { 0 },  .op = OPERATOR_VERTEX_DELETE_VECTOR },
  .operator__op_vertex_delete_outarcs      = { .execf = { 0 },  .op = OPERATOR_VERTEX_DELETE_OUTARCS },
  .operator__op_vertex_delete_inarcs       = { .execf = { 0 },  .op = OPERATOR_VERTEX_DELETE_INARCS },
  .operator__op_vertex_acquire             = { .execf = { 0 },  .op = OPERATOR_VERTEX_ACQUIRE },
  .operator__op_vertex_release             = { .execf = { 0 },  .op = OPERATOR_VERTEX_RELEASE },
  .operator__op_arc_connect                = { .execf = { 0 },  .op = OPERATOR_ARC_CONNECT },
  .operator__op_arc_disconnect             = { .execf = { 0 },  .op = OPERATOR_ARC_DISCONNECT },
  .operator__op_system_similarity          = { .execf = { 0 },  .op = OPERATOR_SYSTEM_SIMILARITY },
  .operator__op_system_attach              = { .execf = { 0 },  .op = OPERATOR_SYSTEM_ATTACH },
  .operator__op_system_detach              = { .execf = { 0 },  .op = OPERATOR_SYSTEM_DETACH },
  .operator__op_system_clear_registry      = { .execf = { 0 },  .op = OPERATOR_SYSTEM_CLEAR_REGISTRY },
  .operator__op_system_create_graph        = { .execf = { 0 },  .op = OPERATOR_SYSTEM_CREATE_GRAPH },
  .operator__op_system_delete_graph        = { .execf = { 0 },  .op = OPERATOR_SYSTEM_DELETE_GRAPH },
  .operator__op_system_send_comment        = { .execf = { 0 },  .op = OPERATOR_SYSTEM_SEND_COMMENT },
  .operator__op_system_send_raw_data       = { .execf = { 0 },  .op = OPERATOR_SYSTEM_SEND_RAW_DATA },
  .operator__op_graph_truncate             = { .execf = { 0 },  .op = OPERATOR_GRAPH_TRUNCATE },
  .operator__op_graph_persist              = { .execf = { 0 },  .op = OPERATOR_GRAPH_PERSIST },
  .operator__op_graph_state                = { .execf = { 0 },  .op = OPERATOR_GRAPH_STATE },
  .operator__op_graph_readonly             = { .execf = { 0 },  .op = OPERATOR_GRAPH_READONLY },
  .operator__op_graph_readwrite            = { .execf = { 0 },  .op = OPERATOR_GRAPH_READWRITE },
  .operator__op_graph_events               = { .execf = { 0 },  .op = OPERATOR_GRAPH_EVENTS },
  .operator__op_graph_noevents             = { .execf = { 0 },  .op = OPERATOR_GRAPH_NOEVENTS },
  .operator__op_graph_tic                  = { .execf = { 0 },  .op = OPERATOR_GRAPH_TICK },
  .operator__op_graph_event_exec           = { .execf = { 0 },  .op = OPERATOR_GRAPH_EVENT_EXEC },
  .operator__op_vertices_acquire_wl        = { .execf = { 0 },  .op = OPERATOR_VERTICES_ACQUIRE_WL },
  .operator__op_vertices_release           = { .execf = { 0 },  .op = OPERATOR_VERTICES_RELEASE },
  .operator__op_vertices_release_all       = { .execf = { 0 },  .op = OPERATOR_VERTICES_RELEASE_ALL },
  .operator__op_enum_add_vxtype            = { .execf = { 0 },  .op = OPERATOR_ENUM_ADD_VXTYPE },
  .operator__op_enum_add_rel               = { .execf = { 0 },  .op = OPERATOR_ENUM_ADD_REL },
  .operator__op_enum_add_dim               = { .execf = { 0 },  .op = OPERATOR_ENUM_ADD_DIM },
  .operator__op_enum_add_key               = { .execf = { 0 },  .op = OPERATOR_ENUM_ADD_KEY },
  .operator__op_enum_delete_vxtype         = { .execf = { 0 },  .op = OPERATOR_ENUM_DELETE_VXTYPE },
  .operator__op_enum_delete_rel            = { .execf = { 0 },  .op = OPERATOR_ENUM_DELETE_REL },
  .operator__op_enum_delete_dim            = { .execf = { 0 },  .op = OPERATOR_ENUM_DELETE_DIM },
  .operator__op_enum_delete_key            = { .execf = { 0 },  .op = OPERATOR_ENUM_DELETE_KEY },
  .operator__op_enum_add_string            = { .execf = { 0 },  .op = OPERATOR_ENUM_ADD_STRING },
  .operator__op_enum_delete_string         = { .execf = { 0 },  .op = OPERATOR_ENUM_DELETE_STRING },
  .operator__op_null                       = { .execf = { 0 },  .op = OPERATOR_NONE },
  .__end                                   = {0}
};




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_execute_operator __init_execf_entry_SYS_CS( __execf_entry *entry, f_execute_operator execf  ) {
  entry->execf.patched = __execute_op_none;
  return entry->execf.normal = execf;
}

#define INIT_EXECF_ENTRY( Name )  __init_execf_entry_SYS_CS( &g_execf_map.operator__##Name , __execute_##Name )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_execute_operator __reset_execf_entry_SYS_CS( __execf_entry *entry  ) {
  return entry->execf.patched = entry->execf.normal;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __reset_execf_map( void ) {
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  GRAPH_LOCK( SYSTEM ) {
    __execf_entry *cursor = (__execf_entry*)&g_execf_map;
    while( __reset_execf_entry_SYS_CS( cursor++ ) );
  } GRAPH_RELEASE;
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __init_execf_map( void ) {
  int ret = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  GRAPH_LOCK( SYSTEM ) {
    if( !g_execf_map_init_SYS_CS ) {
      INIT_EXECF_ENTRY( op_none );
      INIT_EXECF_ENTRY( op_vertex_new );
      INIT_EXECF_ENTRY( op_vertex_delete );
      INIT_EXECF_ENTRY( op_vertex_set_rank );
      INIT_EXECF_ENTRY( op_vertex_set_type );
      INIT_EXECF_ENTRY( op_vertex_set_tmx );
      INIT_EXECF_ENTRY( op_vertex_convert );
      INIT_EXECF_ENTRY( op_vertex_set_property );
      INIT_EXECF_ENTRY( op_vertex_delete_property );
      INIT_EXECF_ENTRY( op_vertex_clear_properties );
      INIT_EXECF_ENTRY( op_vertex_set_vector );
      INIT_EXECF_ENTRY( op_vertex_delete_vector );
      INIT_EXECF_ENTRY( op_vertex_delete_outarcs );
      INIT_EXECF_ENTRY( op_vertex_delete_inarcs );
      INIT_EXECF_ENTRY( op_vertex_acquire );
      INIT_EXECF_ENTRY( op_vertex_release );
      INIT_EXECF_ENTRY( op_arc_connect );
      INIT_EXECF_ENTRY( op_arc_disconnect );
      INIT_EXECF_ENTRY( op_system_similarity );
      INIT_EXECF_ENTRY( op_system_attach );
      INIT_EXECF_ENTRY( op_system_detach );
      INIT_EXECF_ENTRY( op_system_clear_registry );
      INIT_EXECF_ENTRY( op_system_create_graph );
      INIT_EXECF_ENTRY( op_system_delete_graph );
      INIT_EXECF_ENTRY( op_system_send_comment );
      INIT_EXECF_ENTRY( op_system_send_raw_data );
      INIT_EXECF_ENTRY( op_graph_truncate );
      INIT_EXECF_ENTRY( op_graph_persist );
      INIT_EXECF_ENTRY( op_graph_state );
      INIT_EXECF_ENTRY( op_graph_readonly );
      INIT_EXECF_ENTRY( op_graph_readwrite );
      INIT_EXECF_ENTRY( op_graph_events );
      INIT_EXECF_ENTRY( op_graph_noevents );
      INIT_EXECF_ENTRY( op_graph_tic );
      INIT_EXECF_ENTRY( op_graph_event_exec );
      INIT_EXECF_ENTRY( op_vertices_acquire_wl );
      INIT_EXECF_ENTRY( op_vertices_release );
      INIT_EXECF_ENTRY( op_vertices_release_all );
      INIT_EXECF_ENTRY( op_enum_add_vxtype );
      INIT_EXECF_ENTRY( op_enum_add_rel );
      INIT_EXECF_ENTRY( op_enum_add_dim );
      INIT_EXECF_ENTRY( op_enum_add_key );
      INIT_EXECF_ENTRY( op_enum_delete_vxtype );
      INIT_EXECF_ENTRY( op_enum_delete_rel );
      INIT_EXECF_ENTRY( op_enum_delete_dim );
      INIT_EXECF_ENTRY( op_enum_delete_key );
      INIT_EXECF_ENTRY( op_enum_add_string );
      INIT_EXECF_ENTRY( op_enum_delete_string );
      INIT_EXECF_ENTRY( op_null );
      __reset_execf_map();
      g_execf_map_init_SYS_CS = 1;
      ret = 1;
    }
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __reset_opcode( vgx_OperationParser_t *parser ) {
  parser->optype = OPTYPE_NONE;
  parser->parsef = __parse_op_null;
  parser->execf = __execute_op_null;
  parser->retiref = __retire_op_null;
  *(DWORD*)parser->op_mnemonic = 0;
  parser->OPERATOR.op.data = 0;
  parser->field = 0;
  parser->crc = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __decode_opcode( vgx_OperationParser_t *parser ) {

  static op_BASE INIT_OP = {
    .op = OPERATOR_NONE,
    {0,0,0,0,0,0,0}
  };


#define __CONFIG_OP_ENGINE( OpStruct ) {                            \
    parser->execf = g_execf_map.operator__##OpStruct.execf.patched; \
    parser->retiref = __retire_##OpStruct;                          \
    parser->parsef = __parse_##OpStruct;                            \
  }                                                                 \
  return 0


#define __CONFIG_OP_ENGINE_EXECF( OpStruct, Execf ) {               \
    parser->execf = g_execf_map.operator__##Execf.execf.patched;    \
    parser->retiref = __retire_##OpStruct;                          \
    parser->parsef = __parse_##OpStruct;                            \
  }                                                                 \
  return 0


  DWORD opcode;
  if( !hex_to_DWORD( parser->token.data, &opcode ) ) {
    __operation_parser_syntax_error( parser, "expected opcode" );
    return -1;
  }

  parser->OPERATOR = INIT_OP;

  parser->OPERATOR.op.code = (OperationProcessorOpCode)opcode;
  *(DWORD*)parser->OPERATOR.op.name = *(DWORD*)parser->op_mnemonic;
  parser->field = 0;

  switch( parser->OPERATOR.op.code ) {
  case OPCODE_NONE:
    __CONFIG_OP_ENGINE( op_none );
  case OPCODE_VERTEX_NEW:
    __CONFIG_OP_ENGINE( op_vertex_new );
  case OPCODE_VERTEX_DELETE:
    __CONFIG_OP_ENGINE( op_vertex_delete );
  case OPCODE_VERTEX_SET_RANK:
    __CONFIG_OP_ENGINE( op_vertex_set_rank );
  case OPCODE_VERTEX_SET_TYPE:
    __CONFIG_OP_ENGINE( op_vertex_set_type );
  case OPCODE_VERTEX_SET_TMX:
    __CONFIG_OP_ENGINE( op_vertex_set_tmx );
  case OPCODE_VERTEX_CONVERT:
    __CONFIG_OP_ENGINE( op_vertex_convert );
  case OPCODE_VERTEX_SET_PROPERTY:
    __CONFIG_OP_ENGINE( op_vertex_set_property );
  case OPCODE_VERTEX_DELETE_PROPERTY:
    __CONFIG_OP_ENGINE( op_vertex_delete_property );
  case OPCODE_VERTEX_CLEAR_PROPERTIES:
    __CONFIG_OP_ENGINE( op_vertex_clear_properties );
  case OPCODE_VERTEX_SET_VECTOR:
    __CONFIG_OP_ENGINE( op_vertex_set_vector );
  case OPCODE_VERTEX_DELETE_VECTOR:
    __CONFIG_OP_ENGINE( op_vertex_delete_vector );
  case OPCODE_VERTEX_DELETE_OUTARCS:
    __CONFIG_OP_ENGINE( op_vertex_delete_outarcs );
  case OPCODE_VERTEX_DELETE_INARCS:
    __CONFIG_OP_ENGINE( op_vertex_delete_inarcs );
  case OPCODE_VERTEX_ACQUIRE:
    __CONFIG_OP_ENGINE( op_vertex_acquire );
  case OPCODE_VERTEX_RELEASE:
    __CONFIG_OP_ENGINE( op_vertex_release );
  case OPCODE_ARC_CONNECT:
    __CONFIG_OP_ENGINE( op_arc_connect );
  case OPCODE_ARC_DISCONNECT:
    __CONFIG_OP_ENGINE( op_arc_disconnect );
  case OPCODE_SYSTEM_SIMILARITY:
    __CONFIG_OP_ENGINE( op_system_similarity );
  case OPCODE_SYSTEM_ATTACH:
    __CONFIG_OP_ENGINE( op_system_attach );
  case OPCODE_SYSTEM_DETACH:
    __CONFIG_OP_ENGINE( op_system_detach );
  case OPCODE_SYSTEM_CLEAR_REGISTRY:
    __CONFIG_OP_ENGINE( op_system_clear_registry );
  case OPCODE_SYSTEM_CREATE_GRAPH:
    __CONFIG_OP_ENGINE( op_system_create_graph );
  case OPCODE_SYSTEM_DELETE_GRAPH:
    __CONFIG_OP_ENGINE( op_system_delete_graph );
  case OPCODE_SYSTEM_SEND_COMMENT:
    __CONFIG_OP_ENGINE( op_system_send_comment );
  case OPCODE_SYSTEM_SEND_RAW_DATA:
    __CONFIG_OP_ENGINE( op_system_send_raw_data );
  case OPCODE_GRAPH_TRUNCATE:
    __CONFIG_OP_ENGINE( op_graph_truncate );
  case OPCODE_GRAPH_PERSIST:
    __CONFIG_OP_ENGINE( op_graph_persist );
  case OPCODE_GRAPH_STATE:
    __CONFIG_OP_ENGINE( op_graph_state );
  case OPCODE_GRAPH_READONLY:
    __CONFIG_OP_ENGINE( op_graph_readonly );
  case OPCODE_GRAPH_READWRITE:
    __CONFIG_OP_ENGINE( op_graph_readwrite );
  case OPCODE_GRAPH_EVENTS:
    __CONFIG_OP_ENGINE( op_graph_events );
  case OPCODE_GRAPH_NOEVENTS:
    __CONFIG_OP_ENGINE( op_graph_noevents );
  case OPCODE_GRAPH_TICK:
    __CONFIG_OP_ENGINE( op_graph_tic );
  case OPCODE_GRAPH_EVENT_EXEC:
    __CONFIG_OP_ENGINE( op_graph_event_exec );
  case OPCODE_VERTICES_ACQUIRE_WL:
    __CONFIG_OP_ENGINE_EXECF( op_vertices_lockstate, op_vertices_acquire_wl );
  case OPCODE_VERTICES_RELEASE:
    __CONFIG_OP_ENGINE_EXECF( op_vertices_lockstate, op_vertices_release );
  case OPCODE_VERTICES_RELEASE_ALL:
    __CONFIG_OP_ENGINE_EXECF( op_vertices_lockstate, op_vertices_release_all );
  case OPCODE_ENUM_ADD_VXTYPE:
    __CONFIG_OP_ENGINE_EXECF( op_enum_add_string64, op_enum_add_vxtype );
  case OPCODE_ENUM_ADD_REL:
    __CONFIG_OP_ENGINE_EXECF( op_enum_add_string64, op_enum_add_rel );
  case OPCODE_ENUM_ADD_DIM:
    __CONFIG_OP_ENGINE_EXECF( op_enum_add_string64, op_enum_add_dim );
  case OPCODE_ENUM_ADD_KEY:
    __CONFIG_OP_ENGINE_EXECF( op_enum_add_string64, op_enum_add_key );
  case OPCODE_ENUM_DELETE_VXTYPE:
    __CONFIG_OP_ENGINE_EXECF( op_enum_delete_string64, op_enum_delete_vxtype );
  case OPCODE_ENUM_DELETE_REL:
    __CONFIG_OP_ENGINE_EXECF( op_enum_delete_string64, op_enum_delete_rel );
  case OPCODE_ENUM_DELETE_DIM:
    __CONFIG_OP_ENGINE_EXECF( op_enum_delete_string64, op_enum_delete_dim );
  case OPCODE_ENUM_DELETE_KEY:
    __CONFIG_OP_ENGINE_EXECF( op_enum_delete_string64, op_enum_delete_key );
  case OPCODE_ENUM_ADD_STRING:
    __CONFIG_OP_ENGINE_EXECF( op_enum_add_string128, op_enum_add_string );
  case OPCODE_ENUM_DELETE_STRING:
    __CONFIG_OP_ENGINE_EXECF( op_enum_delete_string128, op_enum_delete_string );
  default:
    __operation_parser_syntax_error( parser, "unknown operator" );
    __reset_opcode( parser );
    return -1;
  }
}






#endif
