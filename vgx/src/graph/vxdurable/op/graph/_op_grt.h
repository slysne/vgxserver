/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grt.h
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

#ifndef _VXDURABLE_OP_GRT_H
#define _VXDURABLE_OP_GRT_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_truncate get__op_graph_truncate( vgx_vertex_type_t vxtype, int64_t n_discarded ) {
  op_graph_truncate opdata = {
    .op          = OPERATOR_GRAPH_TRUNCATE,
    .vxtype      = vxtype,
    .n_discarded = n_discarded
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_truncate( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_truncate, parser ) {

    BYTE b;
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // vxtype
    case 1:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->vxtype = b;
      OPERATOR_CONTINUE( op );

    // n_discaded
    case 2:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->n_discarded = (int64_t)qw;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        PARSER_VERBOSE( parser, 0x001, "op_graph_truncate: %s %08x type=%s n_discarded=%lld", op->op.name, op->op.code, CStringValue( iEnumerator_OPEN.VertexType.Decode( parser->op_graph, op->vxtype ) ), op->n_discarded );
      }
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
static int __execute_op_graph_truncate( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_truncate, parser ) {
    int64_t n;
    if( (n = _vxgraph_vxtable__truncate_OPEN( graph_CS, op->vxtype, &parser->CSTR__error )) < 0 ) {
      OPEXEC_REASON( parser, "failed to truncate graph" );
      CALLABLE( graph_CS )->advanced->DebugPrintVertexAcquisitionMaps( graph_CS );
      OPERATOR_ERROR( op );
    }
    else if( n != op->n_discarded ) {
      OPEXEC_WARNING( parser, "truncated=%lld, expected=%lld", n, op->n_discarded );
    }
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_truncate( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_truncate, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
