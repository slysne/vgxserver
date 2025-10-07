/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_caching.c
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

#include "_vgx.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




static vgx_QtoS_2way_set_associative_cache_t * __new_QtoS_2way_set_associative_cache( const char *name, int depth, int order );
static void __delete_QtoS_2way_set_associative_cache( vgx_QtoS_2way_set_associative_cache_t **cache );
static const CString_t * __encoding_as_string( vgx_Graph_t *self, const QWORD encoded, vgx_QtoS_2way_set_associative_cache_t *cache, f_vgx_CStringDecoder_t decoder );
static void __print_cache_stats( vgx_QtoS_2way_set_associative_cache_t *cache );


DLL_EXPORT vgx_ICache_t iCache = {
  .NewStringEncodingCache     = __new_QtoS_2way_set_associative_cache,
  .DeleteStringEncodingCache  = __delete_QtoS_2way_set_associative_cache,
  .EncodingAsString           = __encoding_as_string,
  .PrintStringEncodingCache   = __print_cache_stats
};




/**************************************************************************//**
 * __new_QtoS_2way_set_associative_cache
 *
 ******************************************************************************
 */
static vgx_QtoS_2way_set_associative_cache_t * __new_QtoS_2way_set_associative_cache( const char *name, int depth, int order ) {
  vgx_QtoS_2way_set_associative_cache_t *cache = NULL;
  
  XTRY {
    
    if( (cache = (vgx_QtoS_2way_set_associative_cache_t*)calloc( 1, sizeof( vgx_QtoS_2way_set_associative_cache_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xC01 );
    }

    if( (cache->CSTR__name = CStringNewFormat( name )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xC02 );
    }

    cache->order = order;
    cache->width = 1LL << order;
    cache->mask = cache->width - 1;
    cache->n_hit = 0;
    cache->n_miss = 0;
    cache->n_decode = 0;
    
    if( (cache->cells = (vgx_QtoS_2way_cache_cell_t*)calloc( cache->width, sizeof( vgx_QtoS_2way_cache_cell_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xC03 );
    }

    vgx_QtoS_2way_cache_cell_t *cell = cache->cells;
    vgx_QtoS_2way_cache_cell_t *end = cell + cache->width;
    while( cell < end ) {
      cell->mru = (uintptr_t)&cell->entryA;
      cell->lru = (uintptr_t)&cell->entryB;
      ++cell;
    }

    if( depth > 0 ) {
      if( (cache->next = __new_QtoS_2way_set_associative_cache( name, depth-1, order+3 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xC04 );
      }
    }

  }
  XCATCH( errcode ) {
    __delete_QtoS_2way_set_associative_cache( &cache );
  }
  XFINALLY {
  }
  return cache;
}



/**************************************************************************//**
 * __delete_QtoS_2way_set_associative_cache
 *
 ******************************************************************************
 */
static void __delete_QtoS_2way_set_associative_cache( vgx_QtoS_2way_set_associative_cache_t **cache ) {
  if( cache && *cache ) {
    if( (*cache)->next ) {
      __delete_QtoS_2way_set_associative_cache( &(*cache)->next );
    }
    if( (*cache)->cells ) {
      free( (*cache)->cells );
    }
    if( (*cache)->CSTR__name ) {
      CStringDelete( (*cache)->CSTR__name );
    }
    free( *cache );
    *cache = NULL;
  }
}



/**************************************************************************//**
 * __print_cache_stats
 *
 ******************************************************************************
 */
static void __print_cache_stats( vgx_QtoS_2way_set_associative_cache_t *cache ) {
  int level = 0;
  vgx_QtoS_2way_set_associative_cache_t *this_level = cache;

  float hitrate = 0.0f;

  printf( "==============================================\n" );
  printf( "%s\n", CStringValue( cache->CSTR__name ) );
  printf( "----------------------------------------------\n" );
  while( this_level ) {
    if( this_level->n_hit ) {
      hitrate = this_level->n_hit / (float)(this_level->n_hit + this_level->n_miss);
    }
    else {
      hitrate = 0.0;
    }
    printf( "L%d:  width=%4lld  hitrate=%6.2f%%  decode=%lld\n", level, this_level->width, 100.0*hitrate, this_level->n_decode );
    this_level = this_level->next;
    level++;
  }
  printf( "==============================================\n\n" );

}



static const CString_t * __encoding_as_string( vgx_Graph_t *self, const QWORD encoded, vgx_QtoS_2way_set_associative_cache_t *cache, f_vgx_CStringDecoder_t decoder ) {
  QWORD idx = encoded & cache->mask;
  vgx_QtoS_2way_cache_cell_t *cell = cache->cells + idx;

  // MRU match - return hit right away
  if( ((vgx_QtoS_cache_entry_t*)cell->mru)->encoding == encoded ) {
    cache->n_hit++;
    return ((vgx_QtoS_cache_entry_t*)cell->mru)->CSTR__string;
  }

  // LRU miss = cache miss - evict LRU and replace with decoded item
  if( ((vgx_QtoS_cache_entry_t*)cell->lru)->encoding != encoded ) {
    const CString_t *CSTR__string;
    cache->n_miss++;
    if( cache->next ) {
      CSTR__string = __encoding_as_string( self, encoded, cache->next, decoder );
    }
    else {
      CSTR__string = decoder( self, encoded );
      cache->n_decode++;
    }
    ((vgx_QtoS_cache_entry_t*)cell->lru)->CSTR__string = CSTR__string;
    ((vgx_QtoS_cache_entry_t*)cell->lru)->encoding = encoded;
  }

  // Swap LRU and MRU
  uintptr_t tmp = cell->mru;
  cell->mru = cell->lru;
  cell->lru = tmp;

  // Return the MRU entry
  return ((vgx_QtoS_cache_entry_t*)cell->mru)->CSTR__string;
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_caching.h"
  
test_descriptor_t _vgx_vxgraph_caching_tests[] = {
  { "VGX Graph Similarity Tests", __utest_vxgraph_caching },
  {NULL}
};
#endif
