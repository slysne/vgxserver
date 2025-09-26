/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_add_string128.h
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

#ifndef _VXDURABLE_OP_STRING128_H
#define _VXDURABLE_OP_STRING128_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string128 get__op_enum_add_string128( vgx_Graph_t *graph, const OperationProcessorOperator_t *op, const objectid_t *obid, const CString_t *CSTR__str ) {
  object_allocator_context_t *ephemeral = graph->ephemeral_string_allocator_context;
  op_enum_add_string128 opdata = {
    .op          = *op,
    .CSTR__value = OwnOrCloneCString( CSTR__str, ephemeral ),
    .obid        = *obid
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_enum_add_string128( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_add_string128, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // CSTR__value
    case 1:
      if( parser->op_graph ) {
        if( __operation_parser_lock_graph( parser ) == NULL ) {
          OPERATOR_ERROR( NULL );
        }
      }

      if( (op->CSTR__value = icstringobject.Deserialize( parser->token.data, parser->property_allocator_ref )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // obid
    case 2:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op ); 
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_enum_add_string: %s %08x obid=%016llx%016llx value=%s", op->op.name, op->op.code, op->obid.H, op->obid.L, CStringValue( op->CSTR__value ) );
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
static int __retire_op_enum_add_string128( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_add_string128, parser ) {
    //op->op.data;
    iString.Discard( &op->CSTR__value );
    //op->obid;
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}



#endif
