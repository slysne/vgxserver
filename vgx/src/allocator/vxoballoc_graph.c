/*######################################################################
 *#
 *# vxoballoc_graph.c
 *#
 *#
 *######################################################################
 */


#include "_vxoballoc_graph.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __vxoballoc_graph__new( objectid_t *obid );
static void __vxoballoc_graph__delete( vgx_Graph_t *graph );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IGraphObject_t igraphobject = {
  .New    = __vxoballoc_graph__new,
  .Delete = __vxoballoc_graph__delete
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * __vxoballoc_graph__new( objectid_t *obid ) {
  vgx_Graph_t *graph = NULL;

  // Allocate graph object structure (aligned on page)
  if( PALIGNED_MALLOC( graph, vgx_Graph_t ) != NULL ) {
    // Set allocated object data to zero (except comlib_object_head_t)
    COMLIB_OBJECT_ZERO( vgx_Graph_t, graph );

    // Initialize graph object vtable and type, and set the objectid
    if( COMLIB_OBJECT_INIT( vgx_Graph_t, graph, obid ) == NULL ) {
      __vxoballoc_graph__delete( graph );
      graph = NULL;
    }
  }

  return graph;

}



/*******************************************************************//**
 * Delete the graph object
 * NOTE: this is a shallow delete, i.e. it will not traverse any inner
 * data structures that the graph may reference!
 ***********************************************************************
 */
static void __vxoballoc_graph__delete( vgx_Graph_t *graph ) {
  if( graph ) {
    // Zero the memory before freeing the graph.
    memset( graph, 0, sizeof( vgx_Graph_t ) );
    ALIGNED_FREE( graph );
  }
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxoballoc_graph.h"

test_descriptor_t _vgx_vxoballoc_graph_tests[] = {
  { "VGX Graph Object Allocation Tests", __utest_vxoballoc_graph },

  {NULL}
};
#endif

