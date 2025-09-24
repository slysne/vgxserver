/*######################################################################
 *#
 *# _op_ula.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_ULA_H
#define _VXDURABLE_OP_ULA_H

static int __execute_op_vertices_release_all( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_release_all( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    vgx_Graph_t *graph = parser->op_graph;
    int64_t n = CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    if( n < 0 ) {
      OPEXEC_CRITICAL( parser, "Failed to release all open vertices" );
      OPERATOR_ERROR( op );
    }
    else {
      PARSER_DEC_VERTEX_LOCKS( parser, n );
    }
  } END_OPERATOR_RETURN;
}



#endif
