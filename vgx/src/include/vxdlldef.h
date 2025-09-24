/*
###################################################
#
# File:   vxdlldef.h
# Author: Stian Lysne
#
###################################################
*/

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

