/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_del_string64.h
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

#ifndef _VXDURABLE_OP_DEL_STRING64_H
#define _VXDURABLE_OP_DEL_STRING64_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_enum_delete_string64( OperationProcessorOperator_t *op, QWORD strhash, QWORD strcode ) {
  op_enum_delete_string64 opdata = {
    .op      = *op,
    .hash    = strhash,
    .encoded = strcode
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_enum_delete_string64( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_enum_delete_string64, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // hash
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->hash ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // encoded
    case 2:
      if( !hex_to_QWORD( parser->token.data, &op->encoded ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      {
        switch( op->op.code ) {
        case OPCODE_ENUM_DELETE_VXTYPE:
          PARSER_VERBOSE( parser, 0x001, "op_enum_delete_vxtype: %s %08x hash=%016llX encoded=%02llX", op->op.name, op->op.code, op->hash, op->encoded );
          break;
        case OPCODE_ENUM_DELETE_REL:
          PARSER_VERBOSE( parser, 0x002, "op_enum_delete_rel: %s %08x hash=%016llX encoded=%04llX", op->op.name, op->op.code, op->hash, op->encoded );
          break;
        case OPCODE_ENUM_DELETE_DIM:
          PARSER_VERBOSE( parser, 0x003, "op_enum_delete_dim: %s %08x hash=%016llX encoded=%08llX", op->op.name, op->op.code, op->hash, op->encoded );
          break;
        case OPCODE_ENUM_DELETE_KEY:
          PARSER_VERBOSE( parser, 0x004, "op_enum_delete_key: %s %08x hash=%016llX encoded=%016llX", op->op.name, op->op.code, op->hash, op->encoded );
          break;
        default:
          PARSER_VERBOSE( parser, 0x005, "???: %s %08x", op->op.name, op->op.code );
        }
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
static int __retire_op_enum_delete_string64( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_delete_string64, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
