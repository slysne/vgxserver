/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vxr.h
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

#ifndef _VXDURABLE_OP_VXR_H
#define _VXDURABLE_OP_VXR_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_set_rank get__op_vertex_set_rank( const vgx_Rank_t *rank ) {
  op_vertex_set_rank opdata = {
    .op   = OPERATOR_VERTEX_SET_RANK,
    .rank = *rank
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_rank, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // rank
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->rank.bits ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_set_rank: %s %08x rank=(%g,%g)", op->op.name, op->op.code, op->rank.c1, op->rank.c0 );
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
static int __execute_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_set_rank, parser ) {
    VertexSetRank( vertex_WL, &op->rank );
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_rank, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
