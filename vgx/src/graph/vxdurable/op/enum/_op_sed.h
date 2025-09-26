/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_sed.h
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

#ifndef _VXDURABLE_OP_SED_H
#define _VXDURABLE_OP_SED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string128 get__op_sed( const objectid_t *obid ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_STRING;
  return get__op_enum_delete_string128( &op, obid );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_string( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string128, parser ) {
    objectid_t *pobid = &op->obid;
    // Retrieve instance from enumerator (borrowed ref)
    CString_t *CSTR__value = iEnumerator_CS.Property.Value.Get( graph_CS, pobid );
    if( CSTR__value ) {
      // The 'sed' operator has no effect on the string unless the enumerator itself
      // is the sole owner of the string.
      int64_t refcnt = icstringobject.RefcntNolock( CSTR__value );
      if( refcnt == 1 ) {
        if( (refcnt = iEnumerator_CS.Property.Value.Discard( graph_CS, CSTR__value )) == 0 ) {
          OPERATOR_SUCCESS( op );
        }
        else {
          char buf[33];
          OPEXEC_WARNING( parser, "property value '%s' not discarded: refcnt=%lld", idtostr( buf, pobid ), refcnt );
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
