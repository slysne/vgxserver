/*######################################################################
 *#
 *# _op_clg.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_CLG_H
#define _VXDURABLE_OP_CLG_H


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_clone_graph get__op_system_clone_graph( vgx_Graph_t *graph ) {
  //  TODO: IMPLEMENT THIS!
  //
  //  The plan is to use the total raw data of source graph as it
  //  appears on disk for a fully serialized and persisted graph
  //  and transfer over to remote end which will the restore the
  //  graph from those exact bytes. Lots of work needed to make
  //  it happen.
  //
  op_system_clone_graph opdata = {
    .op             = OPERATOR_SYSTEM_CLONE_GRAPH,
    .graph          = graph
  };
  return opdata;
}



#endif
