/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxalloc.h
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

#ifndef VGX_VXALLOC_H
#define VGX_VXALLOC_H

#include "cxmalloc.h"
#include "_vxprivate.h"
#include "vxbase.h"



CXMALLOC_HEADER_TEMPLATE( AptrAllocator, aptr_t, ap_vector )


CXMALLOC_HEADER_TEMPLATE( TptrAllocator, tptr_t, tp_vector )


CXMALLOC_HEADER_TEMPLATE( OffsetAllocator, weighted_offset_t, o_vector )



/*******************************************************************//**
 * Vertex Allocator
 ***********************************************************************
 */


#endif
