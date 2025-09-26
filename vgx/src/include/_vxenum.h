/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxenum.h
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

#ifndef _VGX_VXENUM_H
#define _VGX_VXENUM_H


#include "_vgx.h"





#define __MAX_STRLEN 1023
DLL_HIDDEN extern int                           _vxenum__initialize( void );
DLL_HIDDEN extern int                           _vxenum__destroy( void );
DLL_HIDDEN extern void                          _vxenum__set_cstring_deserialization_allocator_context( object_allocator_context_t *context );
DLL_HIDDEN extern object_allocator_context_t *  _vxenum__get_cstring_deserialization_allocator_context( void );
DLL_HIDDEN extern int64_t                       _vxenum__serialize_cstring( const CString_t *CSTR__string, CQwordQueue_t *output );
DLL_HIDDEN extern CString_t *                   _vxenum__deserialize_cstring_nolock( comlib_object_t *container, CQwordQueue_t *input );
DLL_HIDDEN extern CString_t *                   _vxenum__new_string( object_allocator_context_t *allocator_context, const char *str );
DLL_HIDDEN extern CString_t *                   _vxenum__new_string_len( object_allocator_context_t *allocator_context, const char *str, int len );
DLL_HIDDEN extern int                           _vxenum__set_new_string( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *str );
DLL_HIDDEN extern CString_t *                   _vxenum__new_string_format( object_allocator_context_t *allocator_context, const char *format, ... );
DLL_HIDDEN extern int                           _vxenum__set_new_string_format( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, ... );
DLL_HIDDEN extern CString_t *                   _vxenum__new_string_vformat( object_allocator_context_t *allocator_context, const char *format, va_list *args );
DLL_HIDDEN extern int                           _vxenum__set_new_string_vformat( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, va_list *args );
DLL_HIDDEN extern CString_t *                   _vxenum__clone_string( const CString_t *CSTR__obj );
DLL_HIDDEN extern void                          _vxenum__discard_string( CString_t **CSTR__self );
DLL_HIDDEN extern CString_t *                   _vxenum__new_graph_map_name( vgx_Graph_t *self, const CString_t *CSTR__prefix );





#endif
