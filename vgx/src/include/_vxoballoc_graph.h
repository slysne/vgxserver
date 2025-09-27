/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxoballoc_graph.h
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

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
