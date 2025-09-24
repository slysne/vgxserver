/*######################################################################
 *#
 *# _op_vxt.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VXT_H
#define _VXDURABLE_OP_VXT_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_set_type get__op_vertex_set_type( vgx_vertex_type_t vxtype ) {
  op_vertex_set_type opdata = {
    .op     = OPERATOR_VERTEX_SET_TYPE,
    .vxtype = vxtype
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_set_type( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_type, parser ) {

    BYTE b;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // type
    case 1:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->vxtype = b;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        PARSER_VERBOSE( parser, 0x001, "op_vertex_set_type: %s %08x type=%s", op->op.name, op->op.code, CStringValue( iEnumerator_OPEN.VertexType.Decode( parser->op_graph, op->vxtype ) ) );
      }
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
static int __execute_op_vertex_set_type( vgx_OperationParser_t *parser ) { 
  BEGIN_VERTEX_OPERATOR( op_vertex_set_type, parser ) {
    vgx_vertex_type_t vxtype = (vgx_vertex_type_t)op->vxtype;
    // Type is different - update
    if( __vertex_get_type( vertex_WL ) != vxtype ) {
      if( VertexSetType( vertex_WL, vxtype ) < 0 ) {
        OPERATOR_ERROR( op );
      }
    }
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_set_type( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_type, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
