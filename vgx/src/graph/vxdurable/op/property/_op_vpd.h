/*######################################################################
 *#
 *# _op_vpd.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VPD_H
#define _VXDURABLE_OP_VPD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_delete_property get__op_vertex_delete_property( shortid_t key ) {
  op_vertex_delete_property opdata = {
    .op   = OPERATOR_VERTEX_DELETE_PROPERTY,
    .key  = key
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_property, parser ) {
    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // key
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->key ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        const char *s_key = CStringValue( iEnumerator_OPEN.Property.Key.Decode( parser->op_graph, op->key ) );
        PARSER_VERBOSE( parser, 0x001, "op_vertex_delete_property: %s %08x %s", op->op.name, op->op.code, s_key );
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
static int __execute_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_delete_property, parser ) {
    vgx_Graph_t *cs_graph = __operation_parser_lock_graph( parser );
    if( cs_graph == NULL ) {
      OPERATOR_ERROR( NULL );
    }
    vgx_VertexProperty_t vertex_property = {0};
    // Retrieve key string from enumerator
    // If key does not exist we ignore the operator.
    if( (vertex_property.key = iEnumerator_CS.Property.Key.Get( cs_graph, op->key )) == NULL ) {
      OPERATOR_IGNORE( op );
    }

    // Delete property
    if( VertexDeleteProperty( vertex_WL, vertex_property ) < 0 ) {
      OPERATOR_ERROR( op );
    }

  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_delete_property( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_delete_property, parser ) {
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}





#endif
