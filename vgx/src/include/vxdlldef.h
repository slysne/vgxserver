/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdlldef.h
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

#ifndef _VGX_VXDLLDEF_H
#define _VGX_VXDLLDEF_H

#ifndef DLL_VGX_PUBLIC
#ifdef _VGX_PRIVATE
#define DLL_VGX_PUBLIC DLL_EXPORT
#else
#define DLL_VGX_PUBLIC DLL_IMPORT
#endif
#endif

#ifndef DLL_VISIBLE
#ifndef _FORCE_VGX_IMPORT
#define DLL_VISIBLE DLL_VGX_PUBLIC
#else
#define DLL_VISIBLE DLL_IMPORT
#endif
#endif


// TODO: Make this better
#if defined __cplusplus
#define VGX_PUBLIC_API
#elif defined _FORCE_VGX_IMPORT
#define VGX_PRIVATE_API
#elif defined _VGX_PRIVATE
#define VGX_PRIVATE_API
#else
#define VGX_PUBLIC_API
#error "error"
#endif




#endif
