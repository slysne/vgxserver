/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxtraverse.h
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

#ifndef _VGX_VXTRAVERSE_H
#define _VGX_VXTRAVERSE_H

#include "_vgx.h"



static int64_t __get_total_neighborhood_size( const vgx_Vertex_t *vertex_RO, vgx_arc_direction direction );



 /*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __get_total_neighborhood_size( const vgx_Vertex_t *vertex_RO, vgx_arc_direction direction ) {
  switch( direction ) {
  case VGX_ARCDIR_ANY:
    /* FALLTHRU */
  case VGX_ARCDIR_BOTH:
    return CALLABLE( vertex_RO )->Degree( vertex_RO );
  case VGX_ARCDIR_IN:
    return iarcvector.Degree( &vertex_RO->inarcs ); 
  case VGX_ARCDIR_OUT:
    return iarcvector.Degree( &vertex_RO->outarcs );
  default:
    return -1;
  }
}






#endif
