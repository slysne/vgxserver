/*
###################################################
#
# File:   _vxenum.h
# Author: Stian Lysne
#
###################################################
*/

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
