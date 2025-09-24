/*######################################################################
 *#
 *# _op_vvd.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VVD_H
#define _VXDURABLE_OP_VVD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_vector get__op_vertex_delete_vector( void ) {
  op_vertex_delete_vector opdata = {
    .op = OPERATOR_VERTEX_DELETE_VECTOR
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_vector, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      // *** EXECUTE ***
      //op->op.code;
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_vector: %s %08x", op->op.name, op->op.code );
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
static int __execute_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_vector, parser ) {
    if( VertexDeleteVector( vertex_WL ) < 0 ) {
      OPERATOR_ERROR( op );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_vector( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_vector, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
