/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vxd.h
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

#ifndef _VXDURABLE_OP_VXD_H
#define _VXDURABLE_OP_VXD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete get__op_vertex_delete( const vgx_Vertex_t *vertex ) {

  int eventexec = vertex->descriptor.writer.threadid == iGraphEvent.ExecutorThreadId( vertex->graph );

  op_vertex_delete opdata = {
    .op         = OPERATOR_VERTEX_DELETE,
    .eventexec  = eventexec,
    .obid       = *__vertex_internalid( vertex )
  };

  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete, parser ) {
    BYTE b;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // obid
    case 1:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // eventexec
    case 2:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->eventexec = (int)b;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_delete: %s %08x %02x %016llx%016llx", op->op.name, op->op.code, op->eventexec, op->obid.H, op->obid.L );
  #endif
      // TODO
      OPERATOR_COMPLETE( op );

    // ERROR
    default:
      OPERATOR_ERROR( op );
    }
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertex_delete( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_vertex_delete, parser ) {
    bool deleted = false; 
    BEGIN_EXECUTION_ATTEMPT( parser, deleted == true ) {
      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_execution_timing_budget( 0, LONG_TIMEOUT );
      // (1) Vertex deleted, or (0) vertex did not exist
      if( _vxdurable_commit__delete_vertex_OPEN( parser->op_graph, NULL, &op->obid, &timing_budget, &parser->reason, &parser->CSTR__error ) >= 0 ) {
        deleted = true;
      }
      // (-1) and arc error, means vertex is scheduled for deletion in the background.
      else if( __is_access_reason_arc_error( parser->reason ) ) {
        deleted = true;
      }
    } END_EXECUTION_ATTEMPT;
    if( !deleted ) {
      OPERATOR_ERROR( op );
    }
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete, parser ) {
  } END_OPERATOR_RETURN;
}







#endif
