/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_mapping.c
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


static framehash_t *      _vxgraph_mapping__new_map( const CString_t *CSTR__dirpath, const CString_t *CSTR__name, int64_t maxelem_hint, int8_t order_hint, vgx_mapping_spec_t spec, object_class_t obclass_changelog );
static void               _vxgraph_mapping__delete_map( framehash_t **map );

static framehash_cell_t * _vxgraph_mapping__new_integer_map( framehash_dynamic_t *dyn, const char *name );
static void               _vxgraph_mapping__delete_integer_map( framehash_cell_t **map, framehash_dynamic_t *dyn );

static int                _vxgraph_mapping__integer_map_add( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *keystr, int64_t value );
static int                _vxgraph_mapping__integer_map_del( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *keystr );
static int                _vxgraph_mapping__integer_map_get( framehash_cell_t *simple_map, framehash_dynamic_t *dyn, const char *keystr, int64_t *value );
static int64_t            _vxgraph_mapping__integer_map_size( framehash_cell_t *simple_map );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IMapping_t iMapping = {
  .NewMap           = _vxgraph_mapping__new_map,
  .DeleteMap        = _vxgraph_mapping__delete_map,
  .NewIntegerMap    = _vxgraph_mapping__new_integer_map,
  .DeleteIntegerMap = _vxgraph_mapping__delete_integer_map,
  .IntegerMapAdd    = _vxgraph_mapping__integer_map_add,
  .IntegerMapDel    = _vxgraph_mapping__integer_map_del,
  .IntegerMapGet    = _vxgraph_mapping__integer_map_get,
  .IntegerMapSize   = _vxgraph_mapping__integer_map_size
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_t * _vxgraph_mapping__new_map( const CString_t *CSTR__dirpath, const CString_t *CSTR__name, int64_t maxelem_hint, int8_t order_hint, vgx_mapping_spec_t spec, object_class_t obclass_changelog ) {
  static const int8_t default_order = 6; // default top-cache width = 2**6 = 64
  framehash_t *map = NULL;
  cxmalloc_family_t *falloc = NULL;
  cxmalloc_family_t *balloc = NULL;
  CString_t *CSTR__allocname = NULL;

  XTRY {
    int block_size_exp = 24;      // default 16 MB blocks
    int8_t cache_depth = 3;       // default cache frames all the way to domain=3
    int8_t order = default_order;

    if( order_hint >= 0 ) {
      order = order_hint;
    }

    if( maxelem_hint > 0 ) {
      // pick a good block size
      block_size_exp = imag2( maxelem_hint * sizeof( framehash_cell_t ) );

      if( (spec & VGX_MAPPING_OPTIMIZE_MASK) == VGX_MAPPING_OPTIMIZE_MEMORY ) {
        block_size_exp -= 2; // smaller blocks for less waste
      }

      if( block_size_exp > 26 ) {
        block_size_exp = 26;  // 64 MB blocks max
      }
      else if( block_size_exp < 16 ) {
        block_size_exp = 16;  // 16 kB blocks min
      }

      // select order to avoid too many domains (target depth=2) while also minimizing the top-domain overhead
      order = (int8_t)minimum_value( ilog2( 1 + maxelem_hint / 8192 ), 15 );

      if( (spec & VGX_MAPPING_OPTIMIZE_MASK) == VGX_MAPPING_OPTIMIZE_SPEED ) {
        order += 2; // wider top-cache, reduce depth
      }
    }
    else {
      if( (spec & VGX_MAPPING_OPTIMIZE_MASK) == VGX_MAPPING_OPTIMIZE_MEMORY ) {
        block_size_exp -= 2; // smaller blocks for less waste
      }
      if( (spec & VGX_MAPPING_OPTIMIZE_MASK) == VGX_MAPPING_OPTIMIZE_SPEED ) {
        order += 2; // wider top-cache, reduce depth
      }
    }

    // Ensure order limits
    if( order > 15 ) {
      order = 15;
    }
    else if( order < 0 ) {
      order = 9;
    }

    //
    if( CSTR__name == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x561 );
    }

    //
    if( (CSTR__allocname = CStringNewFormat( "%s [%s]", CStringValue(CSTR__name), CSTR__dirpath ? CStringValue(CSTR__dirpath) : "ephemeral" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x562 );
    }

    //
    if( (falloc = iFramehash.memory.NewFrameAllocator( CStringValue( CSTR__allocname ), 1ULL << block_size_exp )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x563 );
    }

    //
    if( (balloc = iFramehash.memory.NewBasementAllocator( CStringValue( CSTR__allocname ), 1ULL << (block_size_exp + 1) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x564 );
    }

    framehash_constructor_args_t fargs = FRAMEHASH_DEFAULT_ARGS;
    fargs.param.order             = order;
    fargs.param.synchronized      = (VGX_MAPPING_SYNCHRONIZATION_MASK & spec) == VGX_MAPPING_SYNCHRONIZATION_SYNC;  // threadsafe map?
    fargs.param.shortkeys         = (VGX_MAPPING_KEYTYPE_MASK & spec) != VGX_MAPPING_KEYTYPE_128BIT;  // unless 128-bit for safety, allow 64-bit keys to map comlib objects (risk collision...)
    fargs.param.cache_depth       = cache_depth;
    fargs.param.obclass_changelog = obclass_changelog;
    fargs.dirpath                 = CSTR__dirpath ? CStringValue(CSTR__dirpath) : NULL;    // directory on disk where map will be persisted
    fargs.name                    = CSTR__dirpath ? CStringValue(CSTR__name) : NULL;      // map name
    fargs.frame_allocator         = &falloc;    // private frame allocator
    fargs.basement_allocator      = &balloc;    // private basement allocator

    if( (map = COMLIB_OBJECT_NEW( framehash_t, NULL, &fargs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x565 );
    }

    if( CSTR__dirpath ) {
      VERBOSE( 0x566, "Created mapping ('%s', %lld items): %s", CStringValue(CSTR__name), CALLABLE(map)->Items(map), CStringValue( CALLABLE(map)->Masterpath(map) ) );
    }
    else {
      VERBOSE( 0x567, "Created ephemeral mapping '%s'", CStringValue(CSTR__name) );
    }

  }
  XCATCH( errcode ) {
    if( falloc ) {
      COMLIB_OBJECT_DESTROY( falloc );
    }
    if( balloc ) {
      COMLIB_OBJECT_DESTROY( balloc );
    }
    if( map ) {
      COMLIB_OBJECT_DESTROY( map );
      map = NULL;
    }
  }
  XFINALLY {
    iString.Discard( &CSTR__allocname );
  }

  return map;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxgraph_mapping__delete_map( framehash_t **map ) {
  if( map && *map ) {
    COMLIB_OBJECT_DESTROY( *map );
    *map = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * _vxgraph_mapping__new_integer_map( framehash_dynamic_t *dyn, const char *name ) {
  iFramehash.dynamic.InitDynamicSimple( dyn, name, 12 );
  framehash_cell_t *keymap = iFramehash.simple.New( dyn );
  return keymap;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxgraph_mapping__delete_integer_map( framehash_cell_t **map, framehash_dynamic_t *dyn ) {
  if( map && *map ) {
    iFramehash.simple.Destroy( map, dyn );
    map = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxgraph_mapping__integer_map_add( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *keystr, int64_t value ) {
  shortid_t id = CharsHash64( keystr );
  framehash_key_t fkey = &id;
  framehash_value_t fval = *((framehash_value_t*)(&value));
  return iFramehash.simple.Set( simple_map, dyn, CELL_KEY_TYPE_HASH64, fkey, CELL_VALUE_TYPE_INTEGER, fval );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxgraph_mapping__integer_map_del( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *keystr ) {
  shortid_t id = CharsHash64( keystr );
  framehash_key_t fkey = &id;
  return iFramehash.simple.Del( simple_map, dyn, CELL_KEY_TYPE_HASH64, fkey );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxgraph_mapping__integer_map_get( framehash_cell_t *simple_map, framehash_dynamic_t *dyn, const char *keystr, int64_t *value ) {
  if( keystr == NULL ) {
    return 0;
  }

  shortid_t id = CharsHash64( keystr );
  
  framehash_key_t fkey = &id;
  framehash_value_t fval;
  if( iFramehash.simple.Get( simple_map, dyn, CELL_KEY_TYPE_HASH64, fkey, &fval ) != CELL_VALUE_TYPE_INTEGER ) {
    return 0;
  }
  *value = *((int64_t*)&fval);

  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxgraph_mapping__integer_map_size( framehash_cell_t *simple_map ) {
  return iFramehash.simple.Length( simple_map );
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_mapping.h"
  
test_descriptor_t _vgx_vxgraph_mapping_tests[] = {
  { "VGX Graph Mapping Tests", __utest_vxgraph_mapping },
  {NULL}
};
#endif
