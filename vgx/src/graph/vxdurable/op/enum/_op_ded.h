/*######################################################################
 *#
 *# _op_ded.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_DED_H
#define _VXDURABLE_OP_DED_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_enum_delete_string64 get__op_ded( QWORD dimhash, feature_vector_dimension_t dimcode ) {
  OperationProcessorOperator_t op = OPERATOR_ENUM_DELETE_DIM;
  return get__op_enum_delete_string64( &op, dimhash, dimcode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_enum_delete_dim( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_enum_delete_string64, parser ) {
    if( !igraphfactory.EuclideanVectors() ) {
      vgx_Similarity_t *sim = graph_CS->similarity;
      feature_vector_dimension_t dimcode = (feature_vector_dimension_t)op->encoded;
      // Enumeration exists
      if( iEnumerator_CS.Dimension.ExistsEnum( sim, dimcode ) ) {
        // The 'ded' operator has no effect on the string unless the enumerator itself
        // is the sole owner of the string
        CString_t *CSTR__dimension = (CString_t*)iEnumerator_CS.Dimension.Decode( sim, dimcode );
        if( CSTR__dimension == NULL ) {
          OPERATOR_ERROR( op );
        }
        int64_t refcnt = icstringobject.RefcntNolock( CSTR__dimension );
        if( refcnt == 1 ) {
          if( (refcnt = iEnumerator_CS.Dimension.Discard( sim, dimcode )) == 0 ) {
            OPERATOR_SUCCESS( op );
          }
          else {
            OPEXEC_WARNING( parser, "dimension enumeration '%08X' not discarded: refcnt=%lld", dimcode, refcnt );
          }
        }
      }
    }
  } END_GRAPH_OPERATOR_RETURN;
}



#endif
