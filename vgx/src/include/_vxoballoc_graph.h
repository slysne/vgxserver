/*
###################################################
#
# File:   _vxoballoc_graph.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXOBALLOC_GRAPH_H
#define _VGX_VXOBALLOC_GRAPH_H




#include "_cxmalloc.h"
#include "_vxprivate.h"
#include "vxgraph.h"



/*******************************************************************//**
 *
 * IGraphObject_t
 *
 ***********************************************************************
 */
typedef struct s_IGraphObject_t {
  vgx_Graph_t * (*New)( objectid_t *obid );
  void (*Delete)( vgx_Graph_t *graph );
} IGraphObject_t;


DLL_HIDDEN extern IGraphObject_t igraphobject;



#endif
