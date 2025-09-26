/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_ded.h
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

#ifndef _VXDURABLE_OP_DED_H
#define _VXDURABLE_OP_DED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_ded( QWORD dimhash, feature_vector_dimension_t dimcode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_DIM;
  return get__op_enum_delete_string64( &op, dimhash, dimcode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_dim( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    if( !igraphfactory.EuclideanVectors() ) {
      vgx_Similarity_t *sim = graph_CS->similarity;
      feature_vector_dimension_t dimcode = (feature_vector_dimension_t)op->encoded;
      // Enumeration exists
      if( iEnumerator_CS.Dimension.ExistsEnum( sim, dimcode ) ) {
        // The 'ded' operator has no effect on the string unless the enumerator itself
        // is the sole owner of the string
        CString_t *CSTR__dimension = (CString_t*)iEnumerator_CS.Dimension.Decode( sim, dimcode );
        if( CSTR__dimension == NULL ) {
          OPERATOR_ERROR( op );
        }
        int64_t refcnt = icstringobject.RefcntNolock( CSTR__dimension );
        if( refcnt == 1 ) {
          if( (refcnt = iEnumerator_CS.Dimension.Discard( sim, dimcode )) == 0 ) {
            OPERATOR_SUCCESS( op );
          }
          else {
            OPEXEC_WARNING( parser, "dimension enumeration '%08X' not discarded: refcnt=%lld", dimcode, refcnt );
          }
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
