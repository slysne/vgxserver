/*######################################################################
 *#
 *# _op_deny.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_DENY_H
#define _VXDURABLE_OP_DENY_H



static int __execute_op_DENY( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __execute_op_DENY( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
    VERBOSE( 0x001, "DENY: %08X %s in tx=%016llx%016llx", op->op.code, op->op.name, parser->transid.H, parser->transid.L );
  } END_OPERATOR_RETURN;
}




#endif
