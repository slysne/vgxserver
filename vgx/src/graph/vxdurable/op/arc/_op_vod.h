/*######################################################################
 *#
 *# _op_vod.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VOD_H
#define _VXDURABLE_OP_VOD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_outarcs get__op_vertex_delete_outarcs( int64_t n_removed ) {
  op_vertex_delete_outarcs opdata = {
    .op        = OPERATOR_VERTEX_DELETE_OUTARCS,
    .n_removed = n_removed
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_outarcs( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_outarcs, parser ) {
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // n_removed
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->n_removed = (int64_t)qw;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_outarcs: %s %08x n_removed=%lld", op->op.name, op->op.code, op->n_removed );
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
static int __execute_op_vertex_delete_outarcs( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_outarcs, parser ) {
    int64_t n;
    if( (n = VertexDeleteOutarcs( vertex_WL )) < 0 ) {
      OPEXEC_REASON( parser, "failed to remove vertex outarcs" );
      OPERATOR_ERROR( op );
    }
    else if( n != op->n_removed ) {
      OPEXEC_WARNING( parser, "removed=%lld, expected=%lld, remaining outdegree=%lld", n, op->n_removed, VertexOutDegree( vertex_WL ) );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_outarcs( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_outarcs, parser ) {
  } END_OPERATOR_RETURN;
}





#endif
