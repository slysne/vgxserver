/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_object_processor.c
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

#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );

static int64_t _cxmalloc_object_processor__process_family_FCS(    cxmalloc_family_t *family_CS, cxmalloc_object_processing_context_t *context );
static int64_t _cxmalloc_object_processor__process_allocator_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_object_processing_context_t *context );
static int64_t _cxmalloc_object_processor__process_block_ACS(     cxmalloc_block_t *block_CS, cxmalloc_object_processing_context_t *context );


DLL_HIDDEN _icxmalloc_object_processor_t _icxmalloc_object_processor = {
  .ProcessFamily_FCS    = _cxmalloc_object_processor__process_family_FCS,
  .ProcessAllocator_ACS = _cxmalloc_object_processor__process_allocator_ACS,
  .ProcessBlock_ACS     = _cxmalloc_object_processor__process_block_ACS
};




/*******************************************************************//**
 * Process Family
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_object_processor__process_family_FCS( cxmalloc_family_t *family_CS, cxmalloc_object_processing_context_t *context ) {

  int64_t n_obj = 0;

  XTRY {
    // Traverse all allocators in family
    for( int aidx=0; aidx < family_CS->size && context->completed == false; aidx++ ) {
      // Process the allocator
      cxmalloc_allocator_t *allocator = family_CS->allocators[ aidx ];
      if( allocator ) {
        int64_t n = 0;
        if( family_CS->readonly_cnt > 0 ) {
          n = _cxmalloc_object_processor__process_allocator_ACS( allocator, context );
        }
        else {
          SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
            n = _cxmalloc_object_processor__process_allocator_ACS( allocator_CS, context );
          } RELEASE_CXMALLOC_ALLOCATOR;
        }
        if( n < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x311 );
        }
        n_obj += n;
        context->n_allocators_active++;
      }
      context->n_allocators_processed++;
    }
  }
  XCATCH( errcode ) {
    n_obj = -1;
  }
  XFINALLY {
  }

  return n_obj;
}



/*******************************************************************//**
 * Process Allocator
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_object_processor__process_allocator_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_object_processing_context_t *context ) {

  int64_t n_obj = 0;

  XTRY {
    // Traverse all blocks in allocator
    cxmalloc_block_t **cursor_CS = allocator_CS->blocks;

    while( cursor_CS < allocator_CS->space && context->completed == false ) {
      cxmalloc_block_t *block_CS = *cursor_CS++;
      if( block_CS->linedata != NULL ) {
        int64_t n;
        if( (n = _cxmalloc_object_processor__process_block_ACS( block_CS, context )) < 0 ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x321 );
        }
        n_obj += n;
        context->n_blocks_active++;
      }
    }
    context->n_blocks_processed++;
  }
  XCATCH( errcode ) {
    n_obj = -1;
  }
  XFINALLY {
  }

  return n_obj;
}



/*******************************************************************//**
 * Process Block
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_object_processor__process_block_ACS( cxmalloc_block_t *block_CS, cxmalloc_object_processing_context_t *context ) {

  int64_t n_obj = 0;

  XTRY {

    // Extract block shape parameters
    cxmalloc_datashape_t *shape = &block_CS->parent->shape;
    n_obj = shape->blockmem.quant;
    int stride = shape->linemem.chunks;
    cxmalloc_linehead_t *line_cursor = (cxmalloc_linehead_t*)block_CS->linedata;

    object_class_t obclass = context->object_class;

    // Process all lines in block
    int64_t n_active_obj = 0;
    for( int64_t obj_num = 0; obj_num < n_obj && context->completed == false; obj_num++ ) {
      // object exists
      if( _cxmalloc_bitvector_is_set( &block_CS->active, obj_num ) ) {
        // process object
        comlib_object_t *obj = _cxmalloc_object_from_linehead( line_cursor );
        // TODO:  Check the context vtable against the obj vtable, then it's considered valid

        if( COMLIB_OBJECT_CLASSMATCH( obj, obclass ) ) {
          int64_t n = context->process_object( context, obj );
          if( n < 0 ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x331 );
          }
          n_active_obj += n;
        }
      }
      // advance to the beginning of next line
      line_cursor += stride;
    }

    // Update context
    context->n_objects_active += n_active_obj;
    context->n_objects_processed += n_obj;

  }
  XCATCH( errcode ) {
    n_obj = -1;
  }
  XFINALLY {
  }

  return n_obj;
}
