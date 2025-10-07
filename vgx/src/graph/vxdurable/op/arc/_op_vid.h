/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vid.h
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

#ifndef _VXDURABLE_OP_VID_H
#define _VXDURABLE_OP_VID_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_inarcs get__op_vertex_delete_inarcs( int64_t n_removed ) {
  op_vertex_delete_inarcs opdata = {
    .op        = OPERATOR_VERTEX_DELETE_INARCS,
    .n_removed = n_removed
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_inarcs( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_inarcs, parser ) {
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // n_removed
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->n_removed = (int64_t)qw;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_inarcs: %s %08x n_removed=%lld", op->op.name, op->op.code, op->n_removed );
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
static int __execute_op_vertex_delete_inarcs( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_inarcs, parser ) {
    int64_t n;
    if( (n = VertexDeleteInarcs( vertex_WL )) < 0 ) {
      OPEXEC_REASON( parser, "failed to remove vertex inarcs" );
      OPERATOR_ERROR( op );
    }
    else if( n != op->n_removed ) {
      OPEXEC_WARNING( parser, "removed=%lld, expected=%lld, remaining indegree=%lld", n, op->n_removed, VertexInDegree( vertex_WL ) );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_inarcs( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_inarcs, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
