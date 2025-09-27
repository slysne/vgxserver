/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    framehash.c
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

#include "_framehash.h"
#include "versiongen.h"

#ifndef FRAMEHASH_VERSION
#define FRAMEHASH_VERSION ?.?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "framehash", VERSIONGEN_XSTR( FRAMEHASH_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "framehash", VERSIONGEN_XSTR( FRAMEHASH_VERSION ) );

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );

static int g_initialized = 0;
static int g_local_cxmalloc_init = 0;

static const char * __version( bool ext );



/*******************************************************************//**
 * framehash_INIT
 *
 ***********************************************************************
 */
DLL_EXPORT int framehash_INIT( void ) {

  if( !g_initialized ) {
    // Default allocators
    const _framehash_frameallocator___interface_t *F_ALLOC = _framehash_frameallocator__Interface();
    const _framehash_basementallocator___interface_t *B_ALLOC = _framehash_basementallocator__Interface();

    SET_EXCEPTION_CONTEXT

    if( cxmalloc_INIT() > 0 ) {
      g_local_cxmalloc_init = 1;
    }

    // Initialize default allocators
    F_ALLOC->Init( );
    B_ALLOC->Init( );

    _framehash_api_class__register_class();
    _framehash_api_class__test_object_register_class();


#ifdef FRAMEHASH_INSTRUMENTATION
    WARN( 0xCCC, "INSTRUMENTED BUILD: Framehash" );
#endif

    g_initialized = 1;
    return 1;
  }

  return 0;
}



/*******************************************************************//**
 * framehash_DESTROY
 *
 ***********************************************************************
 */
DLL_EXPORT int framehash_DESTROY( void ) {
  if( g_initialized ) {
    const _framehash_frameallocator___interface_t *F_ALLOC = _framehash_frameallocator__Interface();
    const _framehash_basementallocator___interface_t *B_ALLOC = _framehash_basementallocator__Interface();

    F_ALLOC->Clear();
    B_ALLOC->Clear();

    _framehash_api_class__unregister_class();
    _framehash_api_class__test_object_unregister_class();

    if( g_local_cxmalloc_init ) {
      cxmalloc_DESTROY();
      g_local_cxmalloc_init = 0;
    }

    g_initialized = 0;
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * __version( bool ext ) {
  if( ext ) {
    return g_version_info_ext;
 }
  else {
    return g_version_info;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CString_t * __get_error( void ) {
  // TODO: Implment
  return NULL;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT IFramehash_t iFramehash = {
  // MEMORY
  .memory = {
    .NewFrame                   = _framehash_memory__new_frame,
    .DiscardFrame               = _framehash_memory__discard_frame,
    .DefaultFrameAllocator      = _framehash_frameallocator__Singleton,
    .DefaultBasementAllocator   = _framehash_basementallocator__Singleton,
    .NewFrameAllocator          = _framehash_memory__new_frame_allocator,
    .NewBasementAllocator       = _framehash_memory__new_basement_allocator,
    .IsFrameValid               = _framehash_frameallocator__is_frame_valid
  },
  // PROCESSING
  .processing = {
    .StandardCellProcessors     = &_framehash_processor__iStandardCellProcessors,
    .StandardSubtreeProcessors  = &_framehash_processor__iStandardSubtreeProcessors,
    .MathCellProcessors         = &_framehash_framemath__iMathCellProcessors,
    .ProcessNolockNocache       = _framehash_processor__process_nolock_nocache,
    .ProcessNolock              = _framehash_processor__process_nolock,
  },
  // DYNAMIC
  .dynamic = {
    .InitDynamic                = _framehash_dynamic__initialize,
    .InitDynamicSimple          = _framehash_dynamic__initialize_simple,
    .ResetDynamicSimple         = _framehash_dynamic__reset_simple,
    .ClearDynamic               = _framehash_dynamic__clear,
    .PrintAllocators            = _framehash_dynamic__print_allocators,
    .CheckAllocators            = _framehash_dynamic__check_allocators
  },
  // ACCESS
  .access = {
    .SetContext                 = _framehash_radix__set,
    .DelContext                 = _framehash_radix__del,
    .GetContext                 = _framehash_radix__get,
    .TopCell                    = _framehash_frameallocator__top_cell_from_slot_array,
    .UpdateTopCell              = _framehash_frameallocator__update_top_cell,
    .Compactify                 = _framehash_radix__compactify_subtree,
    .DeleteItem                 = _framehash_leaf__delete_item,
    .SubtreeLength              = _framehash_radix__subtree_length
  },
  // SIMPLE
  .simple = {
    .New                        = _framehash_api_simple__new,
    .Destroy                    = _framehash_api_simple__destroy,
    .Set                        = _framehash_api_simple__set,
    .SetInt                     = _framehash_api_simple__set_int,
    .Inc                        = _framehash_api_simple__inc,
    .IncInt                     = _framehash_api_simple__inc_int,
    .Dec                        = _framehash_api_simple__dec,
    .DecInt                     = _framehash_api_simple__dec_int,
    .Get                        = _framehash_api_simple__get,
    .GetInt                     = _framehash_api_simple__get_int,
    .GetIntHash64               = _framehash_api_simple__get_int_hash64,
    .GetHash64                  = _framehash_api_simple__get_hash64,
    .Has                        = _framehash_api_simple__has,
    .HasInt                     = _framehash_api_simple__has_int,
    .HasHash64                  = _framehash_api_simple__has_hash64,
    .Del                        = _framehash_api_simple__del,
    .DelInt                     = _framehash_api_simple__del_int,
    .Length                     = _framehash_api_simple__length,
    .Empty                      = _framehash_api_simple__empty,
    .SetReadonly                = _framehash_api_simple__set_readonly,
    .IsReadonly                 = _framehash_api_simple__is_readonly,
    .ClearReadonly              = _framehash_api_simple__clear_readonly,
    .IntItems                   = _framehash_api_simple__int_items,
    .Process                    = _framehash_api_simple__process
  },
  // BASIC
  .Version                      = __version,
  .GetError                     = __get_error,
  // TEST
  .test = {
    .GetTestNames               = _framehash_fhtest__get_unit_test_names,
    .RunUnitTests               = _framehash_fhtest__run_unit_tests
  }
};




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_framehash.h"

DLL_HIDDEN test_descriptor_t _framehash_framehash_tests[] = {
  { "framehash",   __utest_framehash_framehash },
  {NULL}
};
#endif
