/*######################################################################
 *#
 *# _op_evx.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_EVX_H
#define _VXDURABLE_OP_EVX_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_graph_event_exec get__op_graph_event_exec( uint32_t ts, uint32_t tmx ) {
  op_graph_event_exec opdata = {
    .op  = OPERATOR_GRAPH_EVENT_EXEC,
    .ts  = ts,
    .tmx = tmx
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_event_exec, parser ) {

    switch( parser->field++ ) {

    DWORD dw;

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // ts
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->ts = dw;
      OPERATOR_CONTINUE( op );

    // tmx
    case 2:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tmx = dw;
      // *** EXECUTE ***
    #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_graph_event_exec: %s %08x ts=%d tmx=%d", op->op.name, op->op.code, op->ts, op->tmx );
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
static int __execute_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_graph_event_exec, parser ) {
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_graph_event_exec( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_graph_event_exec, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
