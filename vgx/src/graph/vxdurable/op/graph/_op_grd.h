/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grd.h
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

#ifndef _VXDURABLE_OP_GRD_H
#define _VXDURABLE_OP_GRD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_delete_graph get__op_system_delete_graph( const objectid_t *obid ) {
  op_system_delete_graph opdata = {
    .op   = OPERATOR_SYSTEM_DELETE_GRAPH,
    .obid = *obid
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_delete_graph( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_delete_graph, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // obid
    case 1:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_delete_graph: %s %08x %016llx%016llx", op->op.name, op->op.code, op->obid.H, op->obid.L );
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
static int __execute_op_system_delete_graph( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_delete_graph, parser ) {
    // NOTE: graph_CS should be SYSTEM
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }

    int err = 0;
    GRAPH_SUSPEND_LOCK( SYSTEM_CS ) {
      vgx_Graph_t *delgraph = igraphfactory.GetGraphByObid( &op->obid );
      if( delgraph ) {
        CString_t *CSTR__err = NULL;
        if( __delete_graph( delgraph, &CSTR__err ) < 0 ) {
          const char *msg = CSTR__err ? CStringValue( CSTR__err ) : "?";
          OPEXEC_REASON( parser, "%s", msg );
          iString.Discard( &CSTR__err );
          err = -1;
        }
      }
    } GRAPH_RESUME_LOCK;

    if( err < 0 ) {
      OPERATOR_ERROR( op );
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_delete_graph( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_delete_graph, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
