/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_evx.h
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

#ifndef _VXDURABLE_OP_EVX_H
#define _VXDURABLE_OP_EVX_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_event_exec get__op_graph_event_exec( uint32_t ts, uint32_t tmx ) {
  op_graph_event_exec opdata = {
    .op  = OPERATOR_GRAPH_EVENT_EXEC,
    .ts  = ts,
    .tmx = tmx
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_event_exec, parser ) {

    switch( parser->field++ ) {

    DWORD dw;

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // ts
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->ts = dw;
      OPERATOR_CONTINUE( op );

    // tmx
    case 2:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tmx = dw;
      // *** EXECUTE ***
    #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_event_exec: %s %08x ts=%d tmx=%d", op->op.name, op->op.code, op->ts, op->tmx );
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
static int __execute_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_event_exec, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_event_exec, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
