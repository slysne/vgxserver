/*######################################################################
 *#
 *# _op_ulv.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_ULV_H
#define _VXDURABLE_OP_ULV_H



static int __execute_op_vertices_release( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_release( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    int ret = 0;
    vgx_VertexIdentifiers_t *identifiers = NULL;

    XTRY {
      vgx_Graph_t *graph = parser->op_graph;
      
      if( (identifiers = iVertex.Identifiers.NewObids( graph, op->obid_list, op->count )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      int64_t n = CALLABLE( graph )->advanced->AtomicUnlockByIdentifiers( graph, identifiers );
      if( n > 0 ) {
        PARSER_DEC_VERTEX_LOCKS( parser, n );
      }
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
      iVertex.Identifiers.Delete( &identifiers );
    }
    OPERATOR_RETURN( op, ret );
  } END_OPERATOR_RETURN;
}




#endif
