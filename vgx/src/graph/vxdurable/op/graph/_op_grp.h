/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grp.h
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

#ifndef _VXDURABLE_OP_GRP_H
#define _VXDURABLE_OP_GRP_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_persist get__op_graph_persist( const vgx_graph_base_counts_t *counts, bool force ) {
  op_graph_persist opdata = {
    .op       = OPERATOR_GRAPH_PERSIST,
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
        .force      = force,
        .assert     = 0,
        .fwdpersist = true,
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
static int __parse_op_graph_persist( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_persist, parser ) {

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
        PARSER_VERBOSE( parser, 0x001, "op_graph_persist: %s %08x order=%lld size=%lld nkey=%lld nstrval=%lld nprop=%lld nvec=%lld ndim=%d nrel=%d ntype=%d flags=%02X", 
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



#define VGX_ENABLE_REMOTE_PERSIST
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_graph_persist( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_persist, parser ) {
#ifdef VGX_ENABLE_REMOTE_PERSIST

    vgx_graph_base_counts_t graph_counts = {0};

    vgx_graph_base_counts_t *gc = &graph_counts;
    vgx_graph_base_counts_t *cnt = &op->counts;

    int64_t nqw = 0;
    // Temporarily allow readonly since persist operation requires it
    vgx_readonly_state_t *ro = &graph_CS->readonly;
    int pre_recursion = _vgx_get_disallow_readonly_recursion_CS( ro );
    _vgx_set_readonly_allowed_CS( ro );

    XTRY {
      // ------ READONLY ------
      //        vvvvvvvv
      //
      // Enter readonly mode
      if( CALLABLE( graph_CS )->advanced->AcquireGraphReadonly( graph_CS, -1, false, &parser->reason ) < 0 ) {
        OPEXEC_REASON( parser, "Failed to enter readonly mode" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Get pre-counts (requires readonly)
      iOperation.Graph_ROG.GetBaseCounts( graph_CS, &graph_counts );

      // Leave readonly mode
      CALLABLE( graph_CS )->advanced->ReleaseGraphReadonly( graph_CS );
      //
      //        ^^^^^^^^
      // ------ readonly ------

      // EXECUTE PERSIST
      if( (nqw = CALLABLE( graph_CS )->BulkSerialize( graph_CS, -1, cnt->flags.force, cnt->flags.fwdpersist, NULL, &parser->CSTR__error )) < 0 ) {
        OPEXEC_CRITICAL( parser, "failed to persist graph" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

    }
    XCATCH( errcode ) {
    }
    XFINALLY {
      // Restore disallow readonly
      int post_recursion = _vgx_get_disallow_readonly_recursion_CS( ro );
      _vgx_set_readonly_disallowed_CS( ro, pre_recursion + post_recursion );
    }

    // CHECK: order
    if( gc->order != cnt->order ) {
      OPEXEC_WARNING( parser, "Order mismatch: current=%lld expected=%lld", gc->order, cnt->order );
    }
    // CHECK: size
    if( gc->size != cnt->size ) {
      OPEXEC_WARNING( parser, "Size mismatch: current=%lld expected=%lld", gc->size, cnt->size );
    }
    // CHECK: nkey
    if( gc->nkey != cnt->nkey ) {
      OPEXEC_WARNING( parser, "Key count mismatch: current=%lld expected=%lld", gc->nkey, cnt->nkey );
    }
    // CHECK: nstrval
    if( gc->nstrval != cnt->nstrval ) {
      OPEXEC_WARNING( parser, "String value count mismatch: current=%lld expected=%lld", gc->nstrval, cnt->nstrval );
    }
    // CHECK: nprop
    if( gc->nprop != cnt->nprop ) {
      OPEXEC_WARNING( parser, "Property count mismatch: current=%lld expected=%lld", gc->nprop, cnt->nprop );
    }
    // CHECK: nvec
    if( gc->nvec != cnt->nvec ) {
      OPEXEC_WARNING( parser, "Vector count mismatch: current=%lld expected=%lld", gc->nvec, cnt->nvec );
    }
    // CHECK: ndim
    if( gc->ndim != cnt->ndim ) {
      OPEXEC_WARNING( parser, "Dimension count mismatch: current=%d expected=%d", gc->ndim, cnt->ndim );
    }
    // CHECK: nrel
    if( gc->nrel != cnt->nrel ) {
      OPEXEC_WARNING( parser, "Relationship count mismatch: current=%d expected=%d", (int)gc->nrel, (int)cnt->nrel );
    }
    // CHECK: ntype
    if( gc->ntype != cnt->ntype ) {
      OPEXEC_WARNING( parser, "Vertex type count mismatch: current=%d expected=%d", (int)gc->ntype, (int)cnt->ntype );
    }
#endif
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_persist( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_persist, parser ) {
  } END_OPERATOR_RETURN;
}





#endif
