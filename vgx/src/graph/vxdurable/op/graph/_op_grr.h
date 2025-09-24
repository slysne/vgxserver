/*######################################################################
 *#
 *# _op_grr.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_GRR_H
#define _VXDURABLE_OP_GRR_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_readonly get__op_graph_readonly( void ) {
  op_graph_readonly opdata = {
    .op = OPERATOR_GRAPH_READONLY
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_readonly( vgx_OperationParser_t *parser ) {
  //
  // NOTE: Setting the system readonly is not possible since the
  //       operation parser explicitly disallows readonly while
  //       executing. Furthermore, if we somehow managed to set
  //       graph readonly via hacks, it would be permanently
  //       readonly since parser would be prevented from doing
  //       anything else from that point on.
  //
  // TODO: Special handling for entering readonly mode.
  //
  // TODO: Special handling for exiting readonly mode.
  //

  BEGIN_OPERATOR( op_graph_readonly, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_readonly: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_graph_readonly( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_readonly, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_readonly( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_readonly, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
