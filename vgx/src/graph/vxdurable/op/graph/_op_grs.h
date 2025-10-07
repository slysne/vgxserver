/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grs.h
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

#ifndef _VXDURABLE_OP_GRS_H
#define _VXDURABLE_OP_GRS_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_state get__op_graph_state( const vgx_graph_base_counts_t *counts, bool assert ) {
  op_graph_state opdata = {
    .op          = OPERATOR_GRAPH_STATE,
    .counts = {
      .order    = counts->order,
      .size     = counts->size,
      .nkey     = counts->nkey,
      .nstrval  = counts->nstrval,
      .nprop    = counts->nprop,
      .nvec     = counts->nvec,
      .ndim     = counts->ndim,
      .nrel     = counts->nrel,
      .ntype    = counts->ntype,
      .flags    = {
        .force      = false,
        .assert     = assert,
        .fwdpersist = false,
        ._rsv4      = 0,
        ._rsv5      = 0,
        ._rsv6      = 0,
        ._rsv7      = 0,
        ._rsv8      = 0
      }
    }
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_state( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_state, parser ) {

    BYTE b;
    WORD w;
    DWORD dw;
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // order
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.order = qw;
      OPERATOR_CONTINUE( op );

    // size
    case 2:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.size = qw;
      OPERATOR_CONTINUE( op );

    // nkey
    case 3:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.nkey = qw;
      OPERATOR_CONTINUE( op );

    // nstrval
    case 4:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.nstrval = qw;
      OPERATOR_CONTINUE( op );

    // nprop
    case 5:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.nprop = qw;
      OPERATOR_CONTINUE( op );

    // nvec
    case 6:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.nvec = qw;
      OPERATOR_CONTINUE( op );

    // ndim
    case 7:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.ndim = dw;
      OPERATOR_CONTINUE( op );

    // nrel
    case 8:
      if( !hex_to_WORD( parser->token.data, &w ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.nrel = w;
      OPERATOR_CONTINUE( op );

    // ntype
    case 9:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.ntype = b;
      OPERATOR_CONTINUE( op );

    // flags
    case 10:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->counts.flags._bits = b;

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        vgx_graph_base_counts_t *c = &op->counts;
        PARSER_VERBOSE( parser, 0x001, "op_graph_state: %s %08x order=%lld size=%lld nkey=%lld nstrval=%lld nprop=%lld nvec=%lld ndim=%d nrel=%d ntype=%d flags=%02X", 
                                                        op->op.name, 
                                                           op->op.code,
                                                                      c->order, c->size, c->nkey, c->nstrval, c->nprop, c->nvec, c->ndim, c->nrel, c->ntype, c->flags._bits
          );
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
static int __execute_op_graph_state( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_state, parser ) {

    vgx_graph_base_counts_t local_counts = {0};
    vgx_graph_base_counts_t *local = &local_counts;
    vgx_graph_base_counts_t *expected = &op->counts;

    // If assert flag is set we will have a strong reaction to state mismatch
    bool do_assert = expected->flags.assert;

    // Get counts without readonly (property count will not be available)
    iOperation.Graph_ROG.GetBaseCounts( graph_CS, &local_counts );

    // Ignore flags
    local->flags._bits = 0;
    expected->flags._bits = 0;

    bool diff = false;

#define __ENTRY( Name, Field )                        \
    do {                                              \
      int64_t delta = (int64_t)local->Field - (int64_t)expected->Field; \
      OPEXEC_INFO( parser, "%-14s| %10lld | %10lld | %10lld | %3s |", Name, (int64_t)expected->Field, (int64_t)local->Field, delta, delta ? " ! " : "" );  \
    } WHILE_ZERO

    OPEXEC_INFO( parser, "[ASSERT STATE]" );
    OPEXEC_INFO( parser, "[%s]", CALLABLE( graph_CS )->FullPath( graph_CS ) );
    OPEXEC_INFO( parser, "--------------+------------+------------+------------+-----+" );
    OPEXEC_INFO( parser, "Metric        | Expected   | Actual     | Delta      |     |" );
    OPEXEC_INFO( parser, "--------------+------------+------------+------------+-----+" );
    __ENTRY( "Vertices", order );
    __ENTRY( "Types", ntype );
    __ENTRY( "Arcs", size );
    __ENTRY( "Relationships", nrel );
    __ENTRY( "Properties", nprop );
    __ENTRY( "Keys", nkey );
    __ENTRY( "Strings", nstrval );
    __ENTRY( "Vectors", nvec );
    __ENTRY( "Dimensions", ndim );
    OPEXEC_INFO( parser, "--------------+------------+------------+------------+-----+" ) ;

    if( do_assert && diff ) {
      OPEXEC_CRITICAL( parser, "State mismatch." );;
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_state( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_state, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
