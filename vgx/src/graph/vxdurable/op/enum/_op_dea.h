/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_dea.h
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

#ifndef _VXDURABLE_OP_DEA_H
#define _VXDURABLE_OP_DEA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string64 get__op_dea( vgx_Graph_t *graph, QWORD dimhash, const CString_t *CSTR__dimension, feature_vector_dimension_t dimcode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_ADD_DIM;
  return get__op_enum_add_string64( graph, &op, dimhash, dimcode, CSTR__dimension );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_add_dim( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_add_string64, parser ) {
    if( !igraphfactory.EuclideanVectors() ) {
      vgx_Similarity_t *sim = graph_CS->similarity;
      feature_vector_dimension_t dimcode = (feature_vector_dimension_t)op->encoded;
      feature_vector_dimension_t encoded = iEnumerator_CS.Dimension.Set( sim, op->hash, op->CSTR__value, dimcode );
      if( encoded == dimcode ) {
        OPERATOR_SUCCESS( op );
      }
      else {
        const char *dim = CStringValue( op->CSTR__value );
        OPEXEC_REASON( parser, "invalid dimension enumeration for '%s': expected %08X, got %08X ", dim, dimcode, encoded );
        OPERATOR_ERROR( op );
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
