/*######################################################################
 *#
 *# _op_gre.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_GRE_H
#define _VXDURABLE_OP_GRE_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_events get__op_graph_events( void ) {
  op_graph_events opdata = {
    .op = OPERATOR_GRAPH_EVENTS
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_events( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_graph_events, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_events: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_graph_events( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_events, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_events( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_events, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
