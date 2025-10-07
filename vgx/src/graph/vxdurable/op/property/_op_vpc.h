/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vpc.h
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

#ifndef _VXDURABLE_OP_VPC_H
#define _VXDURABLE_OP_VPC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_clear_properties get__op_vertex_clear_properties( void ) {
  op_vertex_clear_properties opdata = {
    .op = OPERATOR_VERTEX_CLEAR_PROPERTIES
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_clear_properties, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_clear_properties: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_clear_properties, parser ) {
    if( VertexClearProperties( vertex_WL ) < 0 ) {
      OPEXEC_REASON( parser, "failed to remove vertex properties" );
      OPERATOR_ERROR( op );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_clear_properties, parser ) {
  } END_OPERATOR_RETURN;
}





#endif
