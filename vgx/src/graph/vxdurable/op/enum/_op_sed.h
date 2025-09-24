/*######################################################################
 *#
 *# _op_sed.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_SED_H
#define _VXDURABLE_OP_SED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string128 get__op_sed( const objectid_t *obid ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_STRING;
  return get__op_enum_delete_string128( &op, obid );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_string( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string128, parser ) {
    objectid_t *pobid = &op->obid;
    // Retrieve instance from enumerator (borrowed ref)
    CString_t *CSTR__value = iEnumerator_CS.Property.Value.Get( graph_CS, pobid );
    if( CSTR__value ) {
      // The 'sed' operator has no effect on the string unless the enumerator itself
      // is the sole owner of the string.
      int64_t refcnt = icstringobject.RefcntNolock( CSTR__value );
      if( refcnt == 1 ) {
        if( (refcnt = iEnumerator_CS.Property.Value.Discard( graph_CS, CSTR__value )) == 0 ) {
          OPERATOR_SUCCESS( op );
        }
        else {
          char buf[33];
          OPEXEC_WARNING( parser, "property value '%s' not discarded: refcnt=%lld", idtostr( buf, pobid ), refcnt );
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
