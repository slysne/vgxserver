/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vpd.h
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

#ifndef _VXDURABLE_OP_VPD_H
#define _VXDURABLE_OP_VPD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_property get__op_vertex_delete_property( shortid_t key ) {
  op_vertex_delete_property opdata = {
    .op   = OPERATOR_VERTEX_DELETE_PROPERTY,
    .key  = key
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_property, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // key
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->key ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        const char *s_key = CStringValue( iEnumerator_OPEN.Property.Key.Decode( parser->op_graph, op->key ) );
        PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_property: %s %08x %s", op->op.name, op->op.code, s_key );
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
static int __execute_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_property, parser ) {
    vgx_Graph_t *cs_graph = __operation_parser_lock_graph( parser );
    if( cs_graph == NULL ) {
      OPERATOR_ERROR( NULL );
    }
    vgx_VertexProperty_t vertex_property = {0};
    // Retrieve key string from enumerator
    // If key does not exist we ignore the operator.
    if( (vertex_property.key = iEnumerator_CS.Property.Key.Get( cs_graph, op->key )) == NULL ) {
      OPERATOR_IGNORE( op );
    }

    // Delete property
    if( VertexDeleteProperty( vertex_WL, vertex_property ) < 0 ) {
      OPERATOR_ERROR( op );
    }

  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_property, parser ) {
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}





#endif
