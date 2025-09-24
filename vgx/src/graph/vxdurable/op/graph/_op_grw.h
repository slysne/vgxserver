/*######################################################################
 *#
 *# _op_grw.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_GRW_H
#define _VXDURABLE_OP_GRW_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_readwrite get__op_graph_readwrite( void ) {
  op_graph_readwrite opdata = {
    .op = OPERATOR_GRAPH_READWRITE
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_readwrite( vgx_OperationParser_t *parser ) {

  //
  // SEE NOTES FOR __parse_op_graph_readonly
  //

  BEGIN_OPERATOR( op_graph_readwrite, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_readwrite: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_graph_readwrite( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_readwrite, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_readwrite( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_readwrite, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
