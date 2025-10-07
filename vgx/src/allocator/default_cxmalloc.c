/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    default_cxmalloc.c
 * Author:  Stian Lysne slysne.dev@gmail.com
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

#include "_cxmalloc.h"
#include "default_allocator.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );

/*******************************************************************//**
 * TestAllocator
 ***********************************************************************
 */
#define BLOCK_BYTES (1<<21)
CXMALLOC_TEMPLATE( DefaultAllocator, /* ATYPE:          type name */
                   char,             /* UTYPE:          unit type */
                   memory,           /* ARRAY:          array name */
                   BLOCK_BYTES,      /* BlockBytes:     bytes per block */
                   8192,             /* MaxArraySize:   max array size */
                   0,                /* HeaderSize:     extra header bytes for custom metas */
                   0,                /* Growth:         growth factor S */
                   1,                /* AllowOversized: oversized */
                   100               /* MaxAllocators:  max allocators */
                  )
#undef BLOCK_BYTES
