/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    __utest_framehash_memory.h
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

#ifndef __UTEST_FRAMEHASH_MEMORY_H
#define __UTEST_FRAMEHASH_MEMORY_H

BEGIN_UNIT_TEST( __utest_framehash_memory__leaf ) {

  const _framehash_frameallocator___interface_t *F_ALLOC = _framehash_frameallocator__Interface();
  TEST_ASSERTION( F_ALLOC->Bytes() > 0, "FrameAllocator is initialized" );

  NEXT_TEST_SCENARIO( true, "Create, populate and destroy leaf frames of different orders and domains" ) {
    framehash_cell_t _frameref;

#ifdef FRAMEHASH_INSTRUMENTATION
    framehash_instrument_t op_instrument = _FRAMEHASH_INIT_INSTRUMENT;
    framehash_context_t context = {
      .instrument=&op_instrument,
#else
    framehash_context_t context = {
#endif
      .frame = &_frameref,
      .dynamic=NULL,
    };

    framehash_context_t testobj_context = context;
    testobj_context.frame = NULL;

    framehash_cell_t *cell;
    framehash_metas_t *frame_metas;
    framehash_slot_t *slot_array;
    framehash_halfslot_t *halfslot;
    objectid_t obid = { .L=0x0, .H=0x0 };
    objectid_t *pobid = &obid;
    comlib_object_t *obj;
    size_t sz;
    for( int p=_FRAMEHASH_P_MIN; p <= _FRAMEHASH_P_MAX; p++ ) {
      for( int i=FRAME_DOMAIN_FIRST_FRAME; i <= FRAME_DOMAIN_LAST_FRAME; i++ ) {
        // create frameref
        slot_array = _framehash_memory__new_frame( &context, p, i, FRAME_TYPE_LEAF );
        TEST_ASSERTION( slot_array != NULL,                                     "new leaf order=%d, domain=%d", p, i );
        TEST_ASSERTION( _CELL_REFERENCE_EXISTS( context.frame ),                "context.frame contains a pointer" );
        TEST_ASSERTION( CELL_GET_FRAME_SLOTS( context.frame ) == slot_array,   "context.frame pointer == slot_array pointer" );
        //
        halfslot = _framehash_frameallocator__header_half_slot( context.frame );
        // check referring cell's metas
        frame_metas = CELL_GET_FRAME_METAS( context.frame );
        TEST_ASSERTION( frame_metas->order == p,                 "metas order == %d, found %d", p, frame_metas->order );
        TEST_ASSERTION( frame_metas->domain == i,                "metas domain == %d, found %d", i, frame_metas->domain );
        TEST_ASSERTION( frame_metas->ftype == FRAME_TYPE_LEAF,   "metas ftype == %d, found %d", FRAME_TYPE_LEAF, frame_metas->ftype );
        TEST_ASSERTION( frame_metas->nactive == 0,               "metas nactive == 0, found %d", frame_metas->nactive );
        TEST_ASSERTION( frame_metas->fileno == 0,                "metas fileno == 0, found %d", frame_metas->fileno );
        TEST_ASSERTION( frame_metas->chainbits == 0,             "metas chainbits == 0, found %d", frame_metas->chainbits );
        if( frame_metas->flags.cancache ) {
          TEST_ASSERTION( frame_metas->flags.cachetyp == FCACHE_TYPE_CMORPH,  "metas cachetype == %d, found %d", FCACHE_TYPE_CMORPH, frame_metas->flags.cachetyp );
        }
        else {
          TEST_ASSERTION( frame_metas->flags.cachetyp == FCACHE_TYPE_NONE,  "metas cachetype == %d, found %d", FCACHE_TYPE_NONE, frame_metas->flags.cachetyp );
        }

        // check frame's own metas
        TEST_ASSERTION( _framehash_frameallocator__get_metas( context.frame ).QWORD == frame_metas->QWORD, "frame's metas equals referring cell's metas" );
        // check frame size
        sz = F_ALLOC->LengthOf( slot_array );
        TEST_ASSERTION( _FRAME_NSLOTS(p) == sz,                  "length of slot array corresponds to frame order" );
        // check header's halfslot cells
        for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
          TEST_ASSERTION( _CELL_TYPE( &halfslot->cells[j] ) == CELL_TYPE_END, "initialized cell == CELL_TYPE_END" );
        }
        // check all zone cells
        for( unsigned fx=0; fx<sz; fx++ ) {
          for( unsigned j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
            TEST_ASSERTION( _CELL_TYPE( &slot_array[fx].cells[j] ) == CELL_TYPE_END, "initialized cell == CELL_TYPE_END" );
          }
        }
        
        // assign test objects to all cells
        testobj_context.frame = NULL;
        testobj_context.ktype = CELL_KEY_TYPE_HASH128;
        // header
        for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
          pobid->L = 1000 + j;
          pobid->H = 2000 + j;
          cell = &halfslot->cells[j];
          testobj_context.value.pobject = obj = (comlib_object_t*)COMLIB_OBJECT_NEW( FramehashTestObject_t, pobid, NULL );
          testobj_context.key.shortid = pobid->L;
          _STORE_OBJECT128_POINTER( &testobj_context, cell );
          TEST_ASSERTION( _CELL_IS_REFERENCE( cell ),               "header cell is a reference after store" );
          TEST_ASSERTION( _CELL_GET_OBJECT128( cell ) == obj,  "the object pointer was stored" );
          TEST_ASSERTION( _CELL_SHORTID( cell ) == pobid->L,        "the object's shortid was stored" );
        }
        // zones
        for( unsigned fx=0; fx<sz; fx++ ) {
          for( unsigned j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
            pobid->L = 3000 + fx;
            pobid->H = 4000 + j;
            cell = &slot_array[fx].cells[j];
            testobj_context.value.pobject = obj = (comlib_object_t*)COMLIB_OBJECT_NEW( FramehashTestObject_t, pobid, NULL );
            testobj_context.key.shortid = pobid->L;
            _STORE_OBJECT128_POINTER( &testobj_context, cell );
            TEST_ASSERTION( _CELL_IS_REFERENCE( cell ),               "cell is a reference after store" );
            TEST_ASSERTION( _CELL_GET_OBJECT128( cell ) == obj,  "the object pointer was stored" );
            TEST_ASSERTION( _CELL_SHORTID( cell ) == pobid->L,        "the object's shortid was stored" );
          }
        }
        // verify stored objects
        // header
        for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
          pobid->L = 1000 + j;
          pobid->H = 2000 + j;
          cell = &halfslot->cells[j];
          obj = _CELL_GET_OBJECT128( cell );
          TEST_ASSERTION( COMLIB_OBJECT_CMPID( obj, pobid ) == 0, "stored object matches expected object" );
        }
        // zones
        for( unsigned fx=0; fx<sz; fx++ ) {
          for( unsigned j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
            pobid->L = 3000 + fx;
            pobid->H = 4000 + j;
            cell = &slot_array[fx].cells[j];
            obj = _CELL_GET_OBJECT128( cell );
            TEST_ASSERTION( COMLIB_OBJECT_CMPID( obj, pobid ) == 0, "stored object matches expected object" );
          }
        }
        // destroy stored objects
        // header
        for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
          cell = &halfslot->cells[j];
          obj = _CELL_GET_OBJECT128( cell );
          TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),     "stored object is destructible" );
          COMLIB_OBJECT_DESTROY( obj );
          _DELETE_ITEM( cell );
          TEST_ASSERTION( _CELL_IS_EMPTY( cell ),               "cell is EMPTY after deletion" );
          TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( cell ),      "cell pointer is NULL after deletion" );
          TEST_ASSERTION( _CELL_SHORTID( cell ) == FRAMEHASH_KEY_NONE,           "cell shortid is NONE after deletion" );
        }
        // zones
        for( unsigned fx=0; fx<sz; fx++ ) {
          for( unsigned j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++ ) {
            cell = &slot_array[fx].cells[j];
            obj = _CELL_GET_OBJECT128( cell );
            TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),     "stored object is destructible" );
            COMLIB_OBJECT_DESTROY( obj );
            _DELETE_ITEM( cell );
            TEST_ASSERTION( _CELL_IS_EMPTY( cell ),               "cell is EMPTY after deletion" );
            TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( cell ),      "cell pointer is NULL after deletion" );
            TEST_ASSERTION( _CELL_SHORTID( cell ) == FRAMEHASH_KEY_NONE,           "cell shortid is NONE after deletion" );
          }
        }
        // destroy
        _framehash_memory__discard_frame( &context );
        TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( context.frame ),           "context.frame no longer points to frame after discard" );
        TEST_ASSERTION( CELL_GET_FRAME_METAS( context.frame )->QWORD == 0, "context.frame metas are reset after discard" );
      }
    }
  } END_TEST_SCENARIO

} END_UNIT_TEST



BEGIN_UNIT_TEST( __utest_framehash_memory__internal ) {

  NEXT_TEST_SCENARIO( true, "Create and destroy internal frames" ) {
    TEST_ASSERTION( 1 == 1 , "TODO!" );
  } END_TEST_SCENARIO

} END_UNIT_TEST



BEGIN_UNIT_TEST( __utest_framehash_memory__cache ) {

  framehash_cell_t _cacheref;
  
#ifdef FRAMEHASH_INSTRUMENTATION
  framehash_instrument_t op_instrument = _FRAMEHASH_INIT_INSTRUMENT;
  framehash_context_t context = {
    .instrument=&op_instrument,
#else
  framehash_context_t context = {
#endif
    .frame = &_cacheref,
    .dynamic=NULL,
  };

  framehash_slot_t *slot_array;
  framehash_cell_t *cell;
  framehash_metas_t *cache_metas;
  framehash_metas_t *chain_metas;
  _framehash_celltype_t celltype;
  QWORD qword;
  int sz;

  framehash_cell_t init_chain;
  _INITIALIZE_REFERENCE_CELL( &init_chain, 0xF, 0xF, FRAME_TYPE_NONE );
  framehash_metas_t *init_chain_metas = CELL_GET_FRAME_METAS( &init_chain );


  NEXT_TEST_SCENARIO( true, "Create and destroy cache frames of different orders and domains" ) {
    for( int p=0; p <= _FRAMEHASH_MAX_CACHE_FRAME_ORDER; p++ ) {
      for( int i=0; i <= FRAME_DOMAIN_LAST_FRAME; i++ ) {
        slot_array = _framehash_memory__new_frame( &context, p, i, FRAME_TYPE_CACHE );
        TEST_ASSERTION( slot_array != NULL,                               "new cache slot array" );
        TEST_ASSERTION( _CELL_REFERENCE_EXISTS( context.frame ),               "context.frame contains a pointer" );
        TEST_ASSERTION( CELL_GET_FRAME_SLOTS( context.frame ) == slot_array,   "context.frame points to slot array" );
        // check referring cell's metas
        cache_metas = CELL_GET_FRAME_METAS( context.frame );
        TEST_ASSERTION( cache_metas->order == p,                 "metas order == %d, found %d", p, cache_metas->order );
        TEST_ASSERTION( cache_metas->domain == i,                "metas domain == %d, found %d", i, cache_metas->domain );
        TEST_ASSERTION( cache_metas->ftype == FRAME_TYPE_CACHE,  "metas ftype == %d, found %d", FRAME_TYPE_CACHE, cache_metas->ftype );
        TEST_ASSERTION( cache_metas->nactive == 0,               "metas nactive == 0, found %d", cache_metas->nactive );
        TEST_ASSERTION( cache_metas->fileno == 0,                "metas fileno == 0, found %d", cache_metas->fileno );
        TEST_ASSERTION( cache_metas->nchains == 0,               "metas nchains == 0, found %d", cache_metas->nchains );
        TEST_ASSERTION( cache_metas->flags.cancache,             "metas cancache == 1, found %d", cache_metas->flags.cancache );
        if( cache_metas->domain == 0 ) {
          TEST_ASSERTION( cache_metas->flags.cachetyp == FCACHE_TYPE_STATIC, "metas cachetyp == %d, found %d", FCACHE_TYPE_STATIC, cache_metas->flags.cachetyp );
        }
        else {
          TEST_ASSERTION( cache_metas->flags.cachetyp == FCACHE_TYPE_CMORPH, "metas cachetyp == %d, found %d", FCACHE_TYPE_CMORPH, cache_metas->flags.cachetyp );
        }
        // check cache cells
        sz = _CACHE_FRAME_NSLOTS(p);
        for( int fx=0; fx<sz; fx++ ) {
          for( int j=0; j<_FRAMEHASH_CACHE_CELLS_PER_SLOT; j++ ) {
            cell = &slot_array[fx].cells[j];
            celltype = _CELL_TYPE(cell);
            TEST_ASSERTION( celltype == CELL_TYPE_EMPTY,                  "cache cell initialized to CELL_TYPE_EMPTY(%d), got (%d)", CELL_TYPE_EMPTY, celltype ); 
          }
          cell = &slot_array[fx].cells[_FRAMEHASH_CACHE_CHAIN_CELL_J];
          celltype = _CELL_TYPE(cell);
          TEST_ASSERTION( celltype == CELL_TYPE_FRAME,                    "cache chain initialized to type CELL_TYPE_FRAME(%d), got (%d)", CELL_TYPE_FRAME, celltype );
          TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( cell ),                "cache chain initialized to NULL pointer, got non-NULL" );
          chain_metas = CELL_GET_FRAME_METAS(cell);
          TEST_ASSERTION( chain_metas->QWORD == init_chain_metas->QWORD,  "cache chain initialized with metas == %08llx, got %08llx", init_chain_metas->QWORD, chain_metas->QWORD );
        }
        // discard cache frame
        _framehash_memory__discard_frame( &context );
        TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( context.frame ),              "discarded context.frame points to NULL" );
        qword = CELL_GET_FRAME_METAS( context.frame )->QWORD;
        TEST_ASSERTION( qword == 0,                                       "discarded context.frame has metas == 0, got %08llx", qword );
      }
    }
  } END_TEST_SCENARIO
} END_UNIT_TEST



BEGIN_UNIT_TEST( __utest_framehash_memory__basement ) {

  const _framehash_basementallocator___interface_t *B_ALLOC = _framehash_basementallocator__Interface();
  TEST_ASSERTION( B_ALLOC->Bytes() > 0, "BasementAllocator is initialized" );

  NEXT_TEST_SCENARIO( true, "Create, populate and destroy basements" ) {
    framehash_cell_t _basementref;

#ifdef FRAMEHASH_INSTRUMENTATION
    framehash_instrument_t op_instrument = _FRAMEHASH_INIT_INSTRUMENT;
    framehash_context_t context = {
      .instrument=&op_instrument,
#else
    framehash_context_t context = {
#endif
      .frame = &_basementref,
      .dynamic=NULL,
    };

    framehash_context_t testobj_context = context;
    testobj_context.frame = NULL;

    framehash_cell_t *cell;
    framehash_metas_t *basement_metas;
    framehash_cell_t *cell_array;
    objectid_t obid = { .L=0x0, .H=0x0 };
    objectid_t *pobid = &obid;
    comlib_object_t *obj;
    size_t sz;

    for( int i=FRAME_DOMAIN_FIRST_BASEMENT; i<=FRAME_DOMAIN_LAST_BASEMENT; i++ ) {
      // create basementref
      cell_array = _framehash_memory__new_basement( &context, i );
      TEST_ASSERTION( cell_array != NULL,                                 "new basement domain=%d", i );
      TEST_ASSERTION( _CELL_REFERENCE_EXISTS( context.frame ),              "context.frame contains a pointer" );
      TEST_ASSERTION( CELL_GET_FRAME_CELLS( context.frame ) == cell_array,  "context.frame pointer == cell_array pointer" );
      // check referring cell's metas
      basement_metas = CELL_GET_FRAME_METAS( context.frame );
      TEST_ASSERTION( basement_metas->domain == i,                        "metas domain == %d, found %d", i, basement_metas->domain );
      TEST_ASSERTION( basement_metas->ftype == FRAME_TYPE_BASEMENT,       "metas ftype == %d, found %d", FRAME_TYPE_BASEMENT, basement_metas->ftype );
      TEST_ASSERTION( basement_metas->nactive == 0,                       "metas nactive == 0, found %d", basement_metas->nactive );
      TEST_ASSERTION( basement_metas->fileno == 0,                        "metas fileno == 0, found %d", basement_metas->fileno );
      TEST_ASSERTION( basement_metas->hasnext == 0,                       "metas hasnext == 0, found %d", basement_metas->hasnext );
      TEST_ASSERTION( !basement_metas->flags.cancache,                    "no caching in basements, found %d", basement_metas->flags.cancache );
      // check basements's own metas
      cell = _framehash_basementallocator__get_chain_cell( context.frame );
      TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( cell ),                    "new basement has no chain pointer" );
      TEST_ASSERTION( CELL_GET_FRAME_METAS( cell )->QWORD == 0,           "new basement has no chain metas" );
      // check basement size
      sz = B_ALLOC->LengthOf( cell_array );
      TEST_ASSERTION( sz == _FRAMEHASH_BASEMENT_SIZE,                     "cell array length equals basement size" );
      // check all cells
      for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++ ) {
        cell = &cell_array[j];
        TEST_ASSERTION( _CELL_TYPE( cell ) == CELL_TYPE_END,              "initialized cell == CELL_TYPE_END" );
      }
      // assign test objects to all cells
      testobj_context.frame = NULL;
      testobj_context.ktype = CELL_KEY_TYPE_HASH128;
      for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++ ) {
        pobid->L = 1;
        pobid->H = 1 + j;
        cell = &cell_array[j];
        testobj_context.value.pobject = obj = (comlib_object_t*)COMLIB_OBJECT_NEW( FramehashTestObject_t, pobid, NULL );
        testobj_context.key.shortid = pobid->L;
        _STORE_OBJECT128_POINTER( &testobj_context, cell );
        TEST_ASSERTION( _CELL_IS_REFERENCE( cell ),                 "cell is a reference after store" );
        TEST_ASSERTION( _CELL_GET_OBJECT128( cell ) == obj,     "the object pointer was stored" );
        TEST_ASSERTION( _CELL_SHORTID( cell ) == pobid->L,          "the object's shortid was stored" );
      }
      // verify stored objects
      for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++ ) {
        pobid->L = 1;
        pobid->H = 1 + j;
        cell = &cell_array[j];
        obj = _CELL_GET_OBJECT128( cell );
        TEST_ASSERTION( COMLIB_OBJECT_CMPID( obj, pobid ) == 0,    "stored object matches expected object" );
      }
      // destroy stored objects
      for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++ ) {
        cell = &cell_array[j];
        obj = _CELL_GET_OBJECT128( cell );
        TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),             "stored object is destructible" );
        COMLIB_OBJECT_DESTROY( obj );
        _DELETE_ITEM( cell );
        TEST_ASSERTION( _CELL_IS_EMPTY( cell ),                       "cell is EMPTY after deletion" );
        TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( cell ),              "cell pointer is NULL after deletion" );
        TEST_ASSERTION( _CELL_SHORTID( cell ) == FRAMEHASH_KEY_NONE,  "cell shortid is NONE after deletion" );
      }
      // destroy
      _framehash_memory__discard_basement( &context );
      TEST_ASSERTION( _CELL_REFERENCE_IS_NULL( context.frame ),          "context.frame no longer points to basement after discard" );
      TEST_ASSERTION( CELL_GET_FRAME_METAS( context.frame )->QWORD == 0, "context.frame metas are reset after discard" );
    }
  } END_TEST_SCENARIO

} END_UNIT_TEST



#endif
