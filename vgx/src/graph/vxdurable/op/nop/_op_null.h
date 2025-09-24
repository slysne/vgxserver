/*######################################################################
 *#
 *# _op_null.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_NULL_H
#define _VXDURABLE_OP_NULL_H



static int __parse_op_null( vgx_OperationParser_t *parser );
static int __execute_op_null( vgx_OperationParser_t *parser );
static int __retire_op_null( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __parse_op_null( vgx_OperationParser_t *parser ) {
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_null( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_null( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
