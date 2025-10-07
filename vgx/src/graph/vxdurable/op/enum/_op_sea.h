/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_sea.h
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

#ifndef _VXDURABLE_OP_SEA_H
#define _VXDURABLE_OP_SEA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string128 get__op_sea( vgx_Graph_t *graph, const objectid_t *obid, const CString_t *CSTR__string ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_ADD_STRING;
  return get__op_enum_add_string128( graph, &op, obid, CSTR__string );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __error_op_enum_add_string( vgx_OperationParser_t *parser, const objectid_t *obid ) {
  BEGIN_OPERATOR( op_enum_add_string128, parser ) {
    CString_t *CSTR__prefix = CStringPrefix( op->CSTR__value, 508 );
    const char *value = CSTR__prefix ? CStringValue( CSTR__prefix ) : "???";
    const char *dots = CStringLength( op->CSTR__value ) > 508 ? "..." : "";
    OPEXEC_REASON( parser, "invalid valid enumeration for '%s%s': expected %016llx%016llx, got %016llx%016llx ", value, dots, op->obid.H, op->obid.L, obid ? obid->H : 0, obid ? obid->L : 0 );
    iString.Discard( &CSTR__prefix );
    OPERATOR_ERROR( op );
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_add_string( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_add_string128, parser ) {
    // Add string to enumerator, which will own ONE reference.
    CString_t *CSTR__instance = iEnumerator_CS.Property.Value.Store( graph_CS, op->CSTR__value );
    const objectid_t *obid = CSTR__instance ? CStringObid( CSTR__instance ) : NULL;
    if( obid && idmatch( obid, &op->obid ) ) {
      OPERATOR_SUCCESS( op );
    }
    else {
      OPERATOR_RETURN( op, __error_op_enum_add_string( parser, obid ) );
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
