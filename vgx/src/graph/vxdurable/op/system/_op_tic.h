/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_tic.h
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

#ifndef _VXDURABLE_OP_TIC_H
#define _VXDURABLE_OP_TIC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_tic get__op_graph_tic( int64_t tms ) {
  op_graph_tic opdata = {
    .op     = OPERATOR_GRAPH_TICK,
    .tms    = tms
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_tic, parser ) {

    switch( parser->field++ ) {

    QWORD qw;

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // tms
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tms = (int64_t)qw;

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_tic: %s %08x tms=%lld order=%lld size=%lld", op->op.name, op->op.code, op->tms );
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
static int __execute_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_tic, parser ) {
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got %s", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }
    iOperation.System_SYS_CS.Tick( SYSTEM_CS, op->tms );
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_tic, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
