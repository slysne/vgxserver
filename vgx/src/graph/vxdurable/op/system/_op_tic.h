/*######################################################################
 *#
 *# _op_tic.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_TIC_H
#define _VXDURABLE_OP_TIC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_tic get__op_graph_tic( int64_t tms ) {
  op_graph_tic opdata = {
    .op     = OPERATOR_GRAPH_TICK,
    .tms    = tms
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_tic, parser ) {

    switch( parser->field++ ) {

    QWORD qw;

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // tms
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tms = (int64_t)qw;

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_tic: %s %08x tms=%lld order=%lld size=%lld", op->op.name, op->op.code, op->tms );
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
static int __execute_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_tic, parser ) {
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got %s", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }
    iOperation.System_SYS_CS.Tick( SYSTEM_CS, op->tms );
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_tic( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_tic, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
