/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_lxw.h
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

#ifndef _VXDURABLE_OP_LXW_H
#define _VXDURABLE_OP_LXW_H


static int __execute_op_vertices_acquire_wl( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_acquire_wl( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    int ret = 0;
    vgx_VertexIdentifiers_t *identifiers = NULL;
    vgx_VertexList_t *vertices = NULL;
    XTRY {
      vgx_Graph_t *graph = parser->op_graph;
      
      if( (identifiers = iVertex.Identifiers.NewObids( graph, op->obid_list, op->count )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // Keep trying to execute forever until vertices acquired or some other unrecoverable exit condition occurs
      BEGIN_EXECUTION_ATTEMPT( parser, vertices != NULL ) {
        vertices = CALLABLE( graph )->advanced->AtomicAcquireVerticesWritable( graph, identifiers, LONG_TIMEOUT, &parser->reason, &parser->CSTR__error );
      } END_EXECUTION_ATTEMPT;

    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
      iVertex.Identifiers.Delete( &identifiers );
      if( vertices ) {
        PARSER_INC_VERTEX_LOCKS( parser, vertices->sz );
        // We have nowhere to store the returned vertex list, so delete it
        iVertex.List.Delete( &vertices );
      }
    }
    OPERATOR_RETURN( op, ret );
  } END_OPERATOR_RETURN;
}



#endif
