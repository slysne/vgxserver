/*######################################################################
 *#
 *# _op_string64.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_STRING64_H
#define _VXDURABLE_OP_STRING64_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string64 get__op_enum_add_string64( vgx_Graph_t *graph, const OperationProcessorOperator_t *op, QWORD strhash, QWORD strcode, const CString_t *CSTR__str ) {
  object_allocator_context_t *ephemeral = graph->ephemeral_string_allocator_context;
  op_enum_add_string64 opdata = {
    .op          = *op,
    .hash        = strhash,
    .encoded     = strcode,
    .CSTR__value = OwnOrCloneCString( CSTR__str, ephemeral )
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_enum_add_string64( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_enum_add_string64, parser ) {

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
      OPERATOR_CONTINUE( op );

    // CSTR__value
    case 3:
      if( parser->op_graph ) {
        if( __operation_parser_lock_graph( parser ) == NULL ) {
          OPERATOR_ERROR( NULL );
        }
      }

      if( op->op.code == OPCODE_ENUM_ADD_DIM && parser->op_graph && parser->op_graph->similarity ) {
        if( parser->op_graph->similarity->dimension_allocator_context ) {
          if( (op->CSTR__value = icstringobject.Deserialize( parser->token.data, parser->op_graph->similarity->dimension_allocator_context )) == NULL ) {
            PARSER_SET_ERROR( parser, "string deserializer error" );
            OPERATOR_ERROR( op );
          }
        }
        else {
          PARSER_SET_ERROR( parser, "no dimension allocator" );
          OPERATOR_ERROR( op );
        }
      }
      else {
        if( (op->CSTR__value = icstringobject.Deserialize( parser->token.data, parser->property_allocator_ref )) == NULL ) {
          PARSER_SET_ERROR( parser, "string deserializer error" );
          OPERATOR_ERROR( op );
        }
      }
        // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        switch( op->op.code ) {
        case OPCODE_ENUM_ADD_VXTYPE:
          PARSER_VERBOSE( parser, 0x001, "op_enum_add_vxtype: %s %08x hash=%016llX encoded=%02llX type=%s", op->op.name, op->op.code, op->hash, op->encoded, CStringValue( op->CSTR__value ) );
          break;
        case OPCODE_ENUM_ADD_REL:
          PARSER_VERBOSE( parser, 0x002, "op_enum_add_rel: %s %08x hash=%016llX encoded=%04llX rel=%s", op->op.name, op->op.code, op->hash, op->encoded, CStringValue( op->CSTR__value ) );
          break;
        case OPCODE_ENUM_ADD_DIM:
          PARSER_VERBOSE( parser, 0x003, "op_enum_add_dim: %s %08x hash=%016llX encoded=%08llX dim=%s", op->op.name, op->op.code, op->hash, op->encoded, CStringValue( op->CSTR__value ) );
          break;
        case OPCODE_ENUM_ADD_KEY:
          PARSER_VERBOSE( parser, 0x004, "op_enum_add_key: %s %08x hash=%016llX encoded=%016llX key=%s", op->op.name, op->op.code, op->hash, op->encoded, CStringValue( op->CSTR__value ) );
          break;
        default:
          PARSER_VERBOSE( parser, 0x005, "???: %s %08x", op->op.name, op->op.code );
        }
      }
  #endif
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
static int __retire_op_enum_add_string64( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_add_string64, parser ) {
    //op->op.data;
    //op->hash;
    //op->encoded;
    iString.Discard( &op->CSTR__value );
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}





#endif
