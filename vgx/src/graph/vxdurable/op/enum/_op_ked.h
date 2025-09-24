/*######################################################################
 *#
 *# _op_ked.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_KED_H
#define _VXDURABLE_OP_KED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_ked( shortid_t keyhash ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_KEY;
  return get__op_enum_delete_string64( &op, keyhash, keyhash );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_key( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    shortid_t keyhash = (shortid_t)op->encoded;
    // Retrieve instance from enumerator (borrowed ref)
    CString_t *CSTR__key = iEnumerator_CS.Property.Key.Get( graph_CS, keyhash );
    if( CSTR__key ) {
      // The 'ked' operator has no effect on the string unless the enumerator itself
      // is the sole owner of the string.
      int64_t refcnt = icstringobject.RefcntNolock( CSTR__key );
      if( refcnt == 1 ) {
        if( (refcnt = iEnumerator_CS.Property.Key.Discard( graph_CS, CSTR__key )) == 0 ) {
          OPERATOR_SUCCESS( op );
        }
        else {
          OPEXEC_WARNING( parser, "key enumeration '%016llX' not discarded: refcnt=%lld", keyhash, refcnt );
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}


#endif
