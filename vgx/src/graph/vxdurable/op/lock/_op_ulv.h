/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_ulv.h
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

#ifndef _VXDURABLE_OP_ULV_H
#define _VXDURABLE_OP_ULV_H



static int __execute_op_vertices_release( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_release( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    int ret = 0;
    vgx_VertexIdentifiers_t *identifiers = NULL;

    XTRY {
      vgx_Graph_t *graph = parser->op_graph;
      
      if( (identifiers = iVertex.Identifiers.NewObids( graph, op->obid_list, op->count )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      int64_t n = CALLABLE( graph )->advanced->AtomicUnlockByIdentifiers( graph, identifiers );
      if( n > 0 ) {
        PARSER_DEC_VERTEX_LOCKS( parser, n );
      }
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
      iVertex.Identifiers.Delete( &identifiers );
    }
    OPERATOR_RETURN( op, ret );
  } END_OPERATOR_RETURN;
}




#endif
