/*######################################################################
 *#
 *# _op_lxw.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_LXW_H
#define _VXDURABLE_OP_LXW_H


static int __execute_op_vertices_acquire_wl( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_vertices_acquire_wl( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    int ret = 0;
    vgx_VertexIdentifiers_t *identifiers = NULL;
    vgx_VertexList_t *vertices = NULL;
    XTRY {
      vgx_Graph_t *graph = parser->op_graph;
      
      if( (identifiers = iVertex.Identifiers.NewObids( graph, op->obid_list, op->count )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }

      // Keep trying to execute forever until vertices acquired or some other unrecoverable exit condition occurs
      BEGIN_EXECUTION_ATTEMPT( parser, vertices != NULL ) {
        vertices = CALLABLE( graph )->advanced->AtomicAcquireVerticesWritable( graph, identifiers, LONG_TIMEOUT, &parser->reason, &parser->CSTR__error );
      } END_EXECUTION_ATTEMPT;

    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
      iVertex.Identifiers.Delete( &identifiers );
      if( vertices ) {
        PARSER_INC_VERTEX_LOCKS( parser, vertices->sz );
        // We have nowhere to store the returned vertex list, so delete it
        iVertex.List.Delete( &vertices );
      }
    }
    OPERATOR_RETURN( op, ret );
  } END_OPERATOR_RETURN;
}



#endif
