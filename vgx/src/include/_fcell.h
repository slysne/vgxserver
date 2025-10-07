/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _fcell.h
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

#ifndef _FCELL_H
#define _FCELL_H


#include "_fmacro.h"
#include "framehash.h"




/*******************************************************************//**
 * __set_cell
 *
 ***********************************************************************
 */
__inline static void __set_cell( framehash_context_t * const context, framehash_cell_t * cell ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    // NOOP for now, in future consider this to mean delete
    return;
  case CELL_VALUE_TYPE_MEMBER:
    _STORE_MEMBERSHIP( context, cell );
    return;
  case CELL_VALUE_TYPE_BOOLEAN:
    _STORE_BOOLEAN( context, cell );
    return;
  case CELL_VALUE_TYPE_UNSIGNED:
    _STORE_UNSIGNED( context, cell );
    return;
  case CELL_VALUE_TYPE_INTEGER:
    _STORE_INTEGER( context, cell );
    return;
  case CELL_VALUE_TYPE_REAL:
    _STORE_REAL( context, cell );
    return;
  case CELL_VALUE_TYPE_POINTER:
    _STORE_POINTER( context, cell );
    return;
  case CELL_VALUE_TYPE_OBJECT64:
    // any previous object is NOT destroyed
    _STORE_OBJECT64_POINTER( context, cell );
    return;
  case CELL_VALUE_TYPE_OBJECT128: /* COMLIB_VS_POINTER */
    // If a different object already exists in the target cell make sure we destroy it first
    _DESTROY_PREVIOUS_OBJECT128( context, cell );
    _STORE_OBJECT128_POINTER( context, cell );
    return;
  default:
    // Unknown vtype, do nothing.
    return;
  }
}




#endif
