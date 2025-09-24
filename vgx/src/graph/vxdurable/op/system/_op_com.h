/*######################################################################
 *#
 *# _op_com.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_COM_H
#define _VXDURABLE_OP_COM_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_send_comment get__op_system_send_comment( vgx_Graph_t *SYSTEM, CString_t *CSTR__comment ) {
  object_allocator_context_t *ephemeral = SYSTEM->ephemeral_string_allocator_context;
  op_system_send_comment opdata = {
    .op             = OPERATOR_SYSTEM_SEND_COMMENT,
    .CSTR__comment  = OwnOrCloneCString( CSTR__comment, ephemeral ) 
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_send_comment( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_send_comment, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // CSTR__comment
    case 1:
      if( (op->CSTR__comment = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_send_comment: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_system_send_comment( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_send_comment, parser ) {
    const char *comment = CStringValue( op->CSTR__comment );
    INFO( 0x001, "(REMOTE) %s", comment );
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_send_comment( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_send_comment, parser ) {
    iString.Discard( &op->CSTR__comment );
  } END_OPERATOR_RETURN;
}




#endif
