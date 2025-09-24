/*######################################################################
 *#
 *# _op_vpc.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VPC_H
#define _VXDURABLE_OP_VPC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_clear_properties get__op_vertex_clear_properties( void ) {
  op_vertex_clear_properties opdata = {
    .op = OPERATOR_VERTEX_CLEAR_PROPERTIES
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_clear_properties, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_clear_properties: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_clear_properties, parser ) {
    if( VertexClearProperties( vertex_WL ) < 0 ) {
      OPEXEC_REASON( parser, "failed to remove vertex properties" );
      OPERATOR_ERROR( op );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_clear_properties( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_clear_properties, parser ) {
  } END_OPERATOR_RETURN;
}





#endif
