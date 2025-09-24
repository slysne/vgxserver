/*######################################################################
 *#
 *# _op_del_string128.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_DEL_STRING128_H
#define _VXDURABLE_OP_DEL_STRING128_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string128 get__op_enum_delete_string128( OperationProcessorOperator_t *op, const objectid_t *obid ) {
  op_enum_delete_string128 opdata = {
    .op   = *op,
    .obid = *obid
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_enum_delete_string128( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_delete_string128, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // obid
    case 1:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op ); 
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_enum_delete_string: %s %08x obid=%016llx%016llx", op->op.name, op->op.code, op->obid.H, op->obid.L );
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
static int __retire_op_enum_delete_string128( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_enum_delete_string128, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
