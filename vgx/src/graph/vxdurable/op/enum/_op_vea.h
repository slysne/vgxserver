/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_vea.h
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

#ifndef _VXDURABLE_OP_VEA_H
#define _VXDURABLE_OP_VEA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string64 get__op_vea( vgx_Graph_t *graph, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_ADD_VXTYPE;
  return get__op_enum_add_string64( graph, &op, typehash, typecode, CSTR__vertex_type_name );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_add_vxtype( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_add_string64, parser ) {
    vgx_vertex_type_t typecode = (vgx_vertex_type_t)op->encoded;
    if( __vertex_type_enumeration_in_user_range( typecode ) ) {
      vgx_vertex_type_t encoded = iEnumerator_CS.VertexType.Set( graph_CS, op->hash, op->CSTR__value, typecode );
      if( encoded == typecode ) {
        OPERATOR_SUCCESS( op );
      }
      else {
        const char *type = CStringValue( op->CSTR__value );
        OPEXEC_REASON( parser, "invalid type enumeration for %s: expected %02X, got %02X ", type, typecode, encoded );
        OPERATOR_ERROR( op );
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}




#endif
