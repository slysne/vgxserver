/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_clg.h
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
