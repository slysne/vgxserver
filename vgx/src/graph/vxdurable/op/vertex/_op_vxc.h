/*######################################################################
 *#
 *# _op_vxc.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VXC_H
#define _VXDURABLE_OP_VXC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_convert get__op_vertex_convert( vgx_VertexStateContext_man_t manifestation ) {
  op_vertex_convert opdata = {
    .op            = OPERATOR_VERTEX_CONVERT,
    .manifestation = manifestation
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_convert( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_convert, parser ) {
    BYTE b;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // manifestation
    case 1:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->manifestation = b;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_convert: %s %08x man=%s", op->op.name, op->op.code, _vgx_manifestation_as_string( op->manifestation ) );
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
static int __execute_op_vertex_convert( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_convert, parser ) {
    VertexConvert( vertex_WL, op->manifestation );
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_convert( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_convert, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
