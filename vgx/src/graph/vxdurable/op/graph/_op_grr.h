/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grr.h
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

#ifndef _VXDURABLE_OP_GRR_H
#define _VXDURABLE_OP_GRR_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_readonly get__op_graph_readonly( void ) {
  op_graph_readonly opdata = {
    .op = OPERATOR_GRAPH_READONLY
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_readonly( vgx_OperationParser_t *parser ) {
  //
  // NOTE: Setting the system readonly is not possible since the
  //       operation parser explicitly disallows readonly while
  //       executing. Furthermore, if we somehow managed to set
  //       graph readonly via hacks, it would be permanently
  //       readonly since parser would be prevented from doing
  //       anything else from that point on.
  //
  // TODO: Special handling for entering readonly mode.
  //
  // TODO: Special handling for exiting readonly mode.
  //

  BEGIN_OPERATOR( op_graph_readonly, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_readonly: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_graph_readonly( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_readonly, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_readonly( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_readonly, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
