/*######################################################################
 *#
 *# _op_red.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_RED_H
#define _VXDURABLE_OP_RED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_red( QWORD relhash, vgx_predicator_rel_enum relcode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_REL;
  return get__op_enum_delete_string64( &op, relhash, relcode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_rel( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    //op->op.data;
    //op->hash;
    //op->encoded;
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
