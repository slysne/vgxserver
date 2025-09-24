/*######################################################################
 *#
 *# _op_vxr.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VXR_H
#define _VXDURABLE_OP_VXR_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_set_rank get__op_vertex_set_rank( const vgx_Rank_t *rank ) {
  op_vertex_set_rank opdata = {
    .op   = OPERATOR_VERTEX_SET_RANK,
    .rank = *rank
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_rank, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // rank
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->rank.bits ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_set_rank: %s %08x rank=(%g,%g)", op->op.name, op->op.code, op->rank.c1, op->rank.c0 );
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
static int __execute_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_set_rank, parser ) {
    VertexSetRank( vertex_WL, &op->rank );
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_set_rank( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_rank, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
