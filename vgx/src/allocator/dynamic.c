/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    dynamic.c
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

#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_dynamic_t * _framehash_dynamic__initialize( framehash_dynamic_t *dynamic, const framehash_constructor_args_t *args ) {
  framehash_dynamic_t *dyn = dynamic;

  // Clean slate before we fill in the members
  memset( dynamic, 0, sizeof(framehash_dynamic_t) );
  
  XTRY {
    // 1. Assign hashing function
    dyn->hashf = args->shortid_hashfunction;

    // 2. Assign the frame allocator (steal)
    if( args->frame_allocator != NULL && *args->frame_allocator != NULL ) {
      dyn->falloc = *args->frame_allocator;
      *args->frame_allocator = NULL; // stolen
    }
    else if( (dyn->falloc = iFramehash.memory.DefaultFrameAllocator()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0xD01 );
    }

    // 3. Assign the basement allocator (steal)
    if( args->basement_allocator != NULL && *args->basement_allocator != NULL ) {
      dyn->balloc = *args->basement_allocator;
      *args->basement_allocator = NULL; // stolen
    }
    else if( (dyn->balloc = iFramehash.memory.DefaultBasementAllocator()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0xD02 );
    }

    // 4. Create the utility sequences
    if( (dyn->cell_list = COMLIB_OBJECT_NEW_DEFAULT( CaptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD03 );
    }
    if( (dyn->cell_heap = COMLIB_OBJECT_NEW_DEFAULT( CaptrHeap_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD04 );
    }
    if( (dyn->ref_list = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD05 );
    }
    if( (dyn->ref_heap = COMLIB_OBJECT_NEW_DEFAULT( CtptrHeap_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD06 );
    }
    if( (dyn->annotation_list = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD07 );
    }
    if( (dyn->annotation_heap = COMLIB_OBJECT_NEW_DEFAULT( CQwordHeap_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xD08 );
    }

    // 5. Initialize the framelock pointer (always initialized regardless of sychronization needs)
    INIT_CRITICAL_SECTION( &dyn->lock.lock );

    // 6. Assign the frame lock pointer if we need to synchronize things internally
    dyn->pflock = args->param.synchronized ? &dyn->lock : NULL;

    // 7. Set the cache depth
    dyn->cache_depth = FRAMEHASH_ARG_UNDEFINED;

  }
  XCATCH( errcode ) {
    _framehash_dynamic__clear( dyn );
    dyn = NULL;
  }
  XFINALLY {}

  return dyn;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_dynamic_t * _framehash_dynamic__initialize_simple( framehash_dynamic_t *dynamic, const char *name, int block_size_order ) {
  framehash_dynamic_t *dyn = NULL;
  framehash_constructor_args_t args = FRAMEHASH_DEFAULT_ARGS;
  cxmalloc_family_t *falloc = NULL;
  cxmalloc_family_t *balloc = NULL;
  
  XTRY {
    args.param.order = 0;
    args.param.cache_depth = 0;

    // Set up allocators in the args
    falloc = _framehash_memory__new_frame_allocator( name, 1ULL << block_size_order );
    balloc = _framehash_memory__new_basement_allocator( name, 1ULL << block_size_order );
    if( falloc && balloc ) {
      args.frame_allocator = &falloc;
      args.basement_allocator = &balloc;
    }
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD12 );
    }


    // This steals the allocators from the args!
    if( _framehash_dynamic__initialize( dynamic, &args ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD13 );
    }

    // Just to be totally clear: Simple dynamic does not allow caching anywhere
    dynamic->cache_depth = -1;

    // Success
    dyn = dynamic;

  }
  XCATCH( errcode ) {
    if( falloc ) {
      COMLIB_OBJECT_DESTROY( falloc );
    }
    if( balloc ) {
      COMLIB_OBJECT_DESTROY( balloc );
    }
    dyn = NULL;
  }
  XFINALLY {
  }

  return dyn;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_dynamic__reset_simple( framehash_dynamic_t *dynamic ) {
  if( dynamic == NULL ) {
    return -1;
  }
  int ret = 0;
  SYNCHRONIZE_ON( dynamic->lock ) {

    // Reset frame allocator
    if( dynamic->falloc ) {
      if( dynamic->falloc != iFramehash.memory.DefaultFrameAllocator() ) {
        // Get information
        longstring_t *ident = COMLIB_OBJECT_GETID( dynamic->falloc );
        size_t block_size = dynamic->falloc->descriptor->parameter.block_sz;
        // Make new frame allocator and destroy the old frame allocator
        cxmalloc_family_t *falloc = _framehash_memory__new_frame_allocator( ident->string, block_size );
        if( falloc ) {
          COMLIB_OBJECT_DESTROY( dynamic->falloc );
          dynamic->falloc = falloc;
        }
        // Error
        else {
          ret = -1;
        }
      }
    }

    // Reset basement allocator
    if( ret == 0 && dynamic->balloc ) {
      if( dynamic->balloc != iFramehash.memory.DefaultBasementAllocator() ) {
        // Get information
        longstring_t *ident = COMLIB_OBJECT_GETID( dynamic->balloc );
        size_t block_size = dynamic->balloc->descriptor->parameter.block_sz;
        // Make new basement allocator and destroy the old basement allocator
        cxmalloc_family_t *balloc = _framehash_memory__new_basement_allocator( ident->string, block_size );
        if( balloc ) {
          COMLIB_OBJECT_DESTROY( dynamic->balloc );
          dynamic->balloc = balloc;
        }
        // Error
        else {
          ret = -1;
        }
      }
    }

  } RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_dynamic__clear( framehash_dynamic_t *dynamic ) {
  if( dynamic ) {
    dynamic->hashf = NULL;
    if( dynamic->falloc ) {
      if( dynamic->falloc != iFramehash.memory.DefaultFrameAllocator() ) {
        COMLIB_OBJECT_DESTROY( dynamic->falloc );
      }
      dynamic->falloc = NULL;
    }
    if( dynamic->balloc ) {
      if( dynamic->balloc != iFramehash.memory.DefaultBasementAllocator() ) {
        COMLIB_OBJECT_DESTROY( dynamic->balloc );
      }
      dynamic->balloc = NULL;
    }
    if( dynamic->cell_list ) {
      COMLIB_OBJECT_DESTROY( dynamic->cell_list );
    }
    if( dynamic->cell_heap ) {
      COMLIB_OBJECT_DESTROY( dynamic->cell_heap );
    }
    if( dynamic->ref_list ) {
      COMLIB_OBJECT_DESTROY( dynamic->ref_list );
    }
    if( dynamic->ref_heap ) {
      COMLIB_OBJECT_DESTROY( dynamic->ref_heap );
    }
    if( dynamic->annotation_list ) {
      COMLIB_OBJECT_DESTROY( dynamic->annotation_list );
    }
    if( dynamic->annotation_heap ) {
      COMLIB_OBJECT_DESTROY( dynamic->annotation_heap );
    }
    dynamic->pflock = NULL;
    DEL_CRITICAL_SECTION( &dynamic->lock.lock );
    dynamic->cache_depth = 0;
  }
}



/*******************************************************************//**
 * _framehash_dynamic__print_allocators
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_dynamic__print_allocators( framehash_dynamic_t *dynamic ) {
  PRINT( dynamic->falloc );
  PRINT( dynamic->balloc );
}



/*******************************************************************//**
 * _framehash_dynamic__check_allocators
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_dynamic__check_allocators( framehash_dynamic_t *dynamic ) {
  int err = 0;
  err += CALLABLE( dynamic->falloc )->Check( dynamic->falloc );
  err += CALLABLE( dynamic->balloc )->Check( dynamic->balloc );
  return err;
}


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_dynamic.h"


DLL_HIDDEN test_descriptor_t _framehash_dynamic_tests[] = {
  { "dynamic",                    __utest_framehash_dynamic },
  {NULL}
};
#endif
