/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_rcl.h
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

#ifndef _VXDURABLE_OP_RCL_H
#define _VXDURABLE_OP_RCL_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_clear_registry get__op_system_clear_registry( void ) {
  op_system_clear_registry opdata = {
    .op   = OPERATOR_SYSTEM_CLEAR_REGISTRY
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_clear_registry( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_clear_registry, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_clear_registry: %s %08x", op->op.name, op->op.code );
  #endif
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
static int __execute_op_system_clear_registry( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_clear_registry, parser ) {
    // NOTE: graph_CS should be SYSTEM
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }

    // Can't hold SYS_CS while clearing registry. Lots of async stuff will occur that may
    // require SYS_CS in other threads.
    GRAPH_SUSPEND_LOCK( SYSTEM_CS ) {
      vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
      if( graphs ) {
        vgx_Graph_t **cursor = graphs;
        vgx_Graph_t *graph;
        while( (graph = *cursor++) != NULL ) {
          CString_t *CSTR__err = NULL;
          if( __delete_graph( graph, &CSTR__err ) < 0 ) {
            const char *msg = CSTR__err ? CStringValue( CSTR__err ) : "?";
            OPEXEC_WARNING( parser, "%s", msg );
            iString.Discard( &CSTR__err );
          }
        }
        free( (void*)graphs );
      }
    } GRAPH_RESUME_LOCK;
  } END_GRAPH_OPERATOR_RETURN;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_clear_registry( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_clear_registry, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
