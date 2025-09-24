/*######################################################################
 *#
 *# _op_nop.h
 *#
 *#
 *######################################################################
 */



#ifndef _VXDURABLE_OP_NOP_H
#define _VXDURABLE_OP_NOP_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_none get__op_none( void ) {
  op_none opdata = {
    .op  = OPERATOR_NONE
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_none( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
    OPERATOR_COMPLETE( op );
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __execute_op_none( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_none( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
