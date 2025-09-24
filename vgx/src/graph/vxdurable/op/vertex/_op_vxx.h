/*######################################################################
 *#
 *# _op_vxx.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VXX_H
#define _VXDURABLE_OP_VXX_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_set_tmx get__op_vertex_set_tmx( uint32_t tmx ) {
  op_vertex_set_tmx opdata = {
    .op  = OPERATOR_VERTEX_SET_TMX,
    .tmx = tmx
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_set_tmx( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_tmx, parser ) {

    DWORD dw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // tmx
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tmx = (uint32_t)dw;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_vertex_set_tmx: %s %08x tmx.vertex_ts=%u", op->op.name, op->op.code, op->tmx );
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
static int __execute_op_vertex_set_tmx( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_set_tmx, parser ) {
    if( VertexSetTMX( vertex_WL, op->tmx ) < 0 ) {
      OPERATOR_ERROR( op );
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_set_tmx( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_tmx, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
