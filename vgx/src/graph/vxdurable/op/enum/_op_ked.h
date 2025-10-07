/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_ked.h
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

#ifndef _VXDURABLE_OP_KED_H
#define _VXDURABLE_OP_KED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_ked( shortid_t keyhash ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_KEY;
  return get__op_enum_delete_string64( &op, keyhash, keyhash );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_key( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    shortid_t keyhash = (shortid_t)op->encoded;
    // Retrieve instance from enumerator (borrowed ref)
    CString_t *CSTR__key = iEnumerator_CS.Property.Key.Get( graph_CS, keyhash );
    if( CSTR__key ) {
      // The 'ked' operator has no effect on the string unless the enumerator itself
      // is the sole owner of the string.
      int64_t refcnt = icstringobject.RefcntNolock( CSTR__key );
      if( refcnt == 1 ) {
        if( (refcnt = iEnumerator_CS.Property.Key.Discard( graph_CS, CSTR__key )) == 0 ) {
          OPERATOR_SUCCESS( op );
        }
        else {
          OPEXEC_WARNING( parser, "key enumeration '%016llX' not discarded: refcnt=%lld", keyhash, refcnt );
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}


#endif
