/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_ula.h
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

#ifndef _VXDURABLE_OP_ULA_H
#define _VXDURABLE_OP_ULA_H

static int __execute_op_vertices_release_all( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_release_all( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    vgx_Graph_t *graph = parser->op_graph;
    int64_t n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    if( n < 0 ) {
      OPEXEC_CRITICAL( parser, "Failed to release all open vertices" );
      OPERATOR_ERROR( op );
    }
    else {
      PARSER_DEC_VERTEX_LOCKS( parser, n );
    }
  } END_OPERATOR_RETURN;
}



#endif
