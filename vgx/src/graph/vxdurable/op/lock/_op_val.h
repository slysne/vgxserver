/*######################################################################
 *#
 *# _op_val.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VAL_H
#define _VXDURABLE_OP_VAL_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_acquire get__op_vertex_acquire( void ) {
  op_vertex_acquire opdata = {
    .op = OPERATOR_VERTEX_ACQUIRE
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_acquire( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_acquire, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_acquire: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_vertex_acquire( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_acquire, parser ) {
    // Acquire recursive writelock
    GRAPH_LOCK( vertex_WL->graph ) {
      vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
      vertex_WL = _vxgraph_state__lock_vertex_writable_CS( vertex_WL->graph, parser->op_vertex_WL, &zero_timeout, VGX_VERTEX_RECORD_ACQUISITION );
    } GRAPH_RELEASE;
    if( vertex_WL ) {
      PARSER_INC_VERTEX_LOCKS( parser, 1 );
    }
    else {
      PARSER_SET_ERROR_REASON( parser, "recursion error", VGX_ACCESS_REASON_NOEXIST );
      OPERATOR_ERROR( op );
    }
    OPERATOR_SUCCESS( op );
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_acquire( vgx_OperationParser_t *parser )  {
  BEGIN_OPERATOR( op_vertex_acquire, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
