/*######################################################################
 *#
 *# _op_ved.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VED_H
#define _VXDURABLE_OP_VED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_ved( QWORD typehash, vgx_vertex_type_t typecode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_VXTYPE;
  return get__op_enum_delete_string64( &op, typehash, typecode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_vxtype( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    vgx_vertex_type_t typecode = (vgx_vertex_type_t)op->encoded;
    // Verify that vertex type has been removed
    if( iEnumerator_CS.VertexType.ExistsEnum( graph_CS, typecode ) ) {
      // Not removed
      const CString_t *CSTR__vxtype = iEnumerator_CS.VertexType.Decode( graph_CS, typecode );
      const char *type = CSTR__vxtype ? CStringValue( CSTR__vxtype ) : NULL;
      if( type ) {
        OPEXEC_WARNING( parser, "vertex type enumeration '%02X:%s' not removed", typecode, type );
      }
      else {
        OPEXEC_CRITICAL( parser, "vertex type enumeration '%02X' remains without reverse mapping", typecode );
        OPERATOR_ERROR( op );
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}


#endif
