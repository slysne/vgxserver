/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vvd.h
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

#ifndef _VXDURABLE_OP_VVD_H
#define _VXDURABLE_OP_VVD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_vector get__op_vertex_delete_vector( void ) {
  op_vertex_delete_vector opdata = {
    .op = OPERATOR_VERTEX_DELETE_VECTOR
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_vector, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_vector: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_vector, parser ) {
    if( VertexDeleteVector( vertex_WL ) < 0 ) {
      OPERATOR_ERROR( op );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_vector, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
