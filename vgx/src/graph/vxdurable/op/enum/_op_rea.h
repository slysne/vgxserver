/*######################################################################
 *#
 *# _op_rea.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_REA_H
#define _VXDURABLE_OP_REA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string64 get__op_rea( vgx_Graph_t *graph, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_ADD_REL;
  return get__op_enum_add_string64( graph, &op, relhash, relcode, CSTR__relationship );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_add_rel( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_add_string64, parser ) {
    vgx_predicator_rel_enum relcode = (vgx_predicator_rel_enum)op->encoded;
    vgx_predicator_rel_enum encoded = iEnumerator_CS.Relationship.Set( graph_CS, op->hash, op->CSTR__value, relcode );
    if( encoded == relcode ) {
      OPERATOR_SUCCESS( op );
    }
    else {
      const char *rel = CStringValue( op->CSTR__value );
      OPEXEC_REASON( parser, "invalid relationship enumeration for '%s': expected %04X, got %04X ", rel, relcode, encoded );
      OPERATOR_ERROR( op );
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
