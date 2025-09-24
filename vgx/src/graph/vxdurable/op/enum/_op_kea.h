/*######################################################################
 *#
 *# _op_kea.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_KEA_H
#define _VXDURABLE_OP_KEA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_add_string64 get__op_kea( vgx_Graph_t *graph, shortid_t keyhash, const CString_t *CSTR__key ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_ADD_KEY;
  return get__op_enum_add_string64( graph, &op, keyhash, keyhash, CSTR__key );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_add_key( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_add_string64, parser ) {
    shortid_t encoded = iEnumerator_CS.Property.Key.Set( graph_CS, op->CSTR__value );
    if( encoded == op->encoded ) {
      OPERATOR_SUCCESS( op );
    }
    else {
      const char *key = CStringValue( op->CSTR__value );
      OPEXEC_REASON( parser, "invalid key enumeration for '%s': expected %016llX, got %016llX ", key, op->encoded, encoded );
      OPERATOR_ERROR( op );
    }
  } END_GRAPH_OPERATOR_RETURN;
}




#endif
