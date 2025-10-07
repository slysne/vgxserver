/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    __utest_framehash_api_simple.h
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

#ifndef __UTEST_FRAMEHASH_API_SIMPLE_H
#define __UTEST_FRAMEHASH_API_SIMPLE_H


BEGIN_UNIT_TEST( __utest_framehash_api_simple ) {

  framehash_dynamic_t dyn;

  NEXT_TEST_SCENARIO( true, "Test" ) {
    TEST_ASSERTION( 1 == 1, "" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Initialize Framehash Dynamic" ) {
    TEST_ASSERTION( iFramehash.dynamic.InitDynamicSimple( &dyn, "testdynamic", 20 ) != NULL,      "Dynamic initialized" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create and destroy" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    TEST_ASSERTION( map != NULL,                                                          "Map created" );
    TEST_ASSERTION( iFramehash.simple.Empty( map ) == true,                                   "Map is empty" );
    TEST_ASSERTION( iFramehash.simple.Length( map ) == 0,                                     "Map has zero items" );
    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == 0,                             "Map destroyed" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Set, Get" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;
    // Set and get
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 123, 1000 ) == 1,                   "Value was inserted" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 123, &val ) == 1,                    "Item found" );
    TEST_ASSERTION( val == 1000,                                                          "Correct value retrieved" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 456, &val ) == 0,                    "Item not found" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                         "One item indexed" );

    // Re-insert and get
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 123, 1000 ) == 0,                   "Same value was re-inserted" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 123, &val ) == 1,                    "Item found" );
    TEST_ASSERTION( val == 1000,                                                          "Correct value retrieved" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                         "One item indexed" );

    // Overwrite and get
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 123, 2000 ) == 0,                   "New value overwritten" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 123, &val ) == 1,                    "Item found" );
    TEST_ASSERTION( val == 2000,                                                          "Correct value retrieved" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                         "One item indexed" );

    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Has" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    // nothing
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 111 ) == 0,                          "Item 111 does not exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 111, &val ) == 0,                    "Item 111 not retrieved" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 0,                         "Zero items indexed" );

    // 111
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 111, 1000 ) == 1,                   "Item 111 inserted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                         "One item indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 111 ) == 1,                          "Item 111 exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 111, &val ) == 1,                    "Item 111 retrieved" );
    TEST_ASSERTION( val == 1000,                                                          "111 -> 1000" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 222 ) == 0,                          "Item 222 does not exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 222, &val ) == 0,                    "Item 222 not retrieved" );

    // 111 and 222
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 222, 2000 ) == 1,                   "Item 222 inserted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 2,                         "Two items indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 222 ) == 1,                          "Item 222 exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 222, &val ) == 1,                    "Item 222 retrieved" );
    TEST_ASSERTION( val == 2000,                                                          "222 -> 2000" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 333 ) == 0,                          "Item 333 does not exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 333, &val ) == 0,                    "Item 333 not retrieved" );

    // 111 and 222 and 333
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 333, 3000 ) == 1,                   "Item 333 inserted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 3,                         "Three items indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 333 ) == 1,                          "Item 333 exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 333, &val ) == 1,                    "Item 333 retrieved" );
    TEST_ASSERTION( val == 3000,                                                          "333 -> 3000" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 444 ) == 0,                          "Item 444 does not exists" );
    TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, 444, &val ) == 0,                    "Item 444 not retrieved" );

    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Del" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t n_items;

    // Set up
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 111, 1000 ) == 1,                   "Item 111 inserted" );
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 222, 1000 ) == 1,                   "Item 222 inserted" );
    TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, 333, 1000 ) == 1,                   "Item 333 inserted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 3,                         "Three items indexed" );

    // Delete 111
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 111 ) == 1,                         "Item 111 deleted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 2,                         "Two items indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 111 ) == 0,                          "Item 111 does not exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 222 ) == 1,                          "Item 222 exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 333 ) == 1,                          "Item 333 exists" );

    // Delete 222
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 111 ) == 0,                         "Item 111 not deleted, does not exist" );
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 222 ) == 1,                         "Item 222 deleted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                         "One item indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 111 ) == 0,                          "Item 111 does not exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 222 ) == 0,                          "Item 222 does not exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 333 ) == 1,                          "Item 333 exists" );

    // Delete 222
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 111 ) == 0,                         "Item 111 not deleted, does not exist" );
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 222 ) == 0,                         "Item 222 not deleted, does not exist" );
    TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, 333 ) == 1,                         "Item 333 deleted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 0,                         "Zero items indexed" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 111 ) == 0,                          "Item 111 does not exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 222 ) == 0,                          "Item 222 does not exists" );
    TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, 333 ) == 0,                          "Item 333 does not exists" );

    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Inc and Dec" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    // Set up
    TEST_ASSERTION( iFramehash.simple.IncInt( &map, &dyn, 111, 1000, &val ) == 1,               "Item 111 inserted" );
    TEST_ASSERTION( val == 1000,                                                            "Value initialized to inc amount" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                           "One item indexed" );

    // Inc by 0
    TEST_ASSERTION( iFramehash.simple.IncInt( &map, &dyn, 111, 0, &val ) == 0,                  "Existing 111 incremented" );
    TEST_ASSERTION( val == 1000,                                                            "Increment by 0 does not change value" );

    // Inc by 1
    TEST_ASSERTION( iFramehash.simple.IncInt( &map, &dyn, 111, 1, &val ) == 0,                  "Existing 111 incremented" );
    TEST_ASSERTION( val == 1001,                                                            "Value incremented to 1001" );

    // Dec by 2
    TEST_ASSERTION( iFramehash.simple.DecInt( &map, &dyn, 111, 2, &val, false, NULL ) == 0,     "Existing 111 decremented" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                           "One item indexed" );
    TEST_ASSERTION( val == 999,                                                             "Value decremented to 999" );

    // Dec by 900 (autodelete enabled, no trigger)
    bool deleted = false;
    TEST_ASSERTION( iFramehash.simple.DecInt( &map, &dyn, 111, 900, &val, true, &deleted ) == 0,  "Existing 111 decremented" );
    TEST_ASSERTION( deleted == false,                                                         "Not deleted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 1,                             "One item indexed" );
    TEST_ASSERTION( val == 99,                                                                "Value decremented to 99" );

    // Dec by 99 (autodelete enabled, trigger)
    TEST_ASSERTION( iFramehash.simple.DecInt( &map, &dyn, 111, 99, &val, true, &deleted ) == 0,   "Existing 111 decremented" );
    TEST_ASSERTION( deleted == true,                                                          "111 decremented to 0, autodeleted" );
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 0,                             "Zero items indexed" );
    TEST_ASSERTION( val == 0,                                                                 "Value decremented to 0" );


    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Insert many and retrieve all items" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    const int limit = 1000000;

    // Insert many
    for( int64_t i=0; i<limit; i++ ) {
      val = i*i;
      TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, i, val ) == 1,                    "Item %lld inserted", i );
    }
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == limit,                     "%lld item indexed", limit );

    // Retrieve all items
    Key64Value56List_t *output = iFramehash.simple.IntItems( map, &dyn, NULL );
    TEST_ASSERTION( output != NULL,                                                       "items retrieved" );
    Cm128iList_vtable_t *iList = CALLABLE( output );
    TEST_ASSERTION( iList->Length( output ) == limit,                                     "%lld items retrieved, got %lld", limit, iList->Length( output ) );

    // Verify all values
    for( int64_t i=0; i<limit; i++ ) {
      Key64Value56_t item;
      iList->Get( output, i, &item.m128 );
      int64_t n = item.key;
      int64_t expected_val = n*n;
      TEST_ASSERTION( item.value56 == expected_val,                                         "%lld -> %lld (got %lld)", n, expected_val, item.value56 );
    }

    // Retrieve all items again into existing list
    TEST_ASSERTION( iFramehash.simple.IntItems( map, &dyn, output ) != NULL,                  "items retrieved" );
    TEST_ASSERTION( iList->Length( output ) == 2*limit,                                   "%lld items in list, got %lld", 2*limit, iList->Length( output ) );

    // Destroy output list
    COMLIB_OBJECT_DESTROY( output );

    // Destroy map
    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Insert and remove many" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    const int limit = 1000000;

    // Insert many
    for( int64_t i=0; i<limit; i++ ) {
      val = i*i;
      TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, i, val ) == 1,                    "Item %lld inserted", i );
    }
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == limit,                     "%lld item indexed", limit );

    // Get many
    for( int64_t i=0; i<limit; i++ ) {
      TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, i, &val ) == 1,                    "Item %lld retrieved", i );
      TEST_ASSERTION( val == i*i,                                                         "Correct value retrieved" );
    }
    
    // Overwrite many
    for( int64_t i=0; i<limit; i++ ) {
      val = (i+1)*(i-1);
      TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, i, val ) == 0,                    "Item %lld overwritten", i );
    }
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == limit,                     "%lld item indexed", limit );

    // Get many
    for( int64_t i=0; i<limit; i++ ) {
      TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, i, &val ) == 1,                    "Item %lld retrieved", i );
      TEST_ASSERTION( val == (i+1)*(i-1),                                                 "Correct value retrieved" );
    }

    // Fail to get many
    for( int64_t i=limit; i<limit+1000; i++ ) {
      TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, i, &val ) == 0,                    "Item %lld does not exist", i );
    }

    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Insert many to reach basements" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    const int limit = 70000000; // should reach basements

    // Insert many
    printf( "\n" );
    printf( "Inserting %d items...\n", limit );
    int64_t pre_balloc_bytes = CALLABLE( dyn.balloc )->Bytes( dyn.balloc );
    for( int64_t i=0; i<limit; i++ ) {
      TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, i, i ) == 1,                      "Item %lld inserted", i );
    }
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == limit,                     "%lld items indexed", limit );
    int64_t post_balloc_bytes = CALLABLE( dyn.balloc )->Bytes( dyn.balloc );
    TEST_ASSERTION( post_balloc_bytes > pre_balloc_bytes,                                 "Basement allocator should be used" );

    // Verify all
    printf( "Verifying %d items...\n", limit );
    for( int64_t i=0; i<limit; i++ ) {
      TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, i, &val ) == 1,                    "%lld -> %lld", i, i );
    }

    // Delete all
    printf( "Deleting %d items...\n", limit );
    for( int64_t i=0; i<limit; i++ ) {
      TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, i ) == 1,                         "%lld deleted", i );
      TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, i, &val ) == 0,                    "%lld does not exist", i );
    }
    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 0,                         "Zero items indexed" );

    // Destroy map
    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Grow and shrink many times" ) {
    framehash_cell_t *map = iFramehash.simple.New( &dyn );
    int64_t val;
    int64_t n_items;

    const int limit = 1000000;
    const int iterations = 5;


    for( int i=0; i<iterations; i++ ) {

      printf( "iteration=%d\n", i );

      // Insert many
      for( int64_t n=0; n<limit; n++ ) {
        val = n*n;
        TEST_ASSERTION( iFramehash.simple.SetInt( &map, &dyn, n, val ) == 1,                    "Item %lld inserted", n );
        TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == n+1,                     "%lld items indexed", n+1 );
      }

      // Get many
      for( int64_t n=0; n<limit; n++ ) {
        TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, n, &val ) == 1,                    "Item %lld retrieved", n );
        TEST_ASSERTION( val == n*n,                                                         "Correct value retrieved" );
      }

      // Delete many
      for( int64_t n=0; n<limit; n++ ) {
        TEST_ASSERTION( iFramehash.simple.DelInt( &map, &dyn, n ) == 1,                         "Item %lld deleted", n );
        TEST_ASSERTION( iFramehash.simple.HasInt( map, &dyn, n ) == 0,                          "Item %lld does not exist", n );
        TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == limit-n-1,               "%lld items indexed", limit-n-1 );
      }

      // Fail to get many
      for( int64_t n=0; n<limit; n++ ) {
        TEST_ASSERTION( iFramehash.simple.GetInt( map, &dyn, n, &val ) == 0,                    "Item %lld does not exist", n );
      }

    }

    TEST_ASSERTION( (n_items = iFramehash.simple.Length( map )) == 0,                         "no items indexed" );
    TEST_ASSERTION( iFramehash.simple.Destroy( &map, &dyn ) == n_items,                       "Map destroyed" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   *
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Measure speed" ) {


    int64_t current_size = 1;
    int64_t val;
    int64_t limit = 200000000; // reach well into basement levels
    int64_t iterations = 10000000; // at least this many iterations
     
    printf( "\n" );

    printf( "Population Size, INSERT ns/i,     INSERT i/s,      RETRIEVE ns/r,   RETRIEVE r/s\n" );
    //       ..............., ...........,     ............,    .............,   ............
    //       15               15               15               15               15
    //       ..............., ..............., ..............., ..............., ...............
    while( current_size <= limit ) {
      framehash_dynamic_t local_dyn;
      framehash_dynamic_t *fhdyn = iFramehash.dynamic.InitDynamicSimple( &local_dyn, "test", 20 );
      framehash_cell_t *map = iFramehash.simple.New( fhdyn );
      int64_t n = 0;


      // Insert
      int64_t ts0 = __GET_CURRENT_NANOSECOND_TICK();
      while( n < iterations ) { // at least this many to get a good reading
        for( int64_t i=0; i<current_size; i++ ) {
          iFramehash.simple.SetInt( &map, fhdyn, i, i );
          n++;
        }
      }
      int64_t ts1 = __GET_CURRENT_NANOSECOND_TICK();
      int64_t insert_ns = ts1 - ts0;
      double ns_per_insertion = insert_ns / (double)n;
      int64_t insertions_per_sec = ((ns_per_insertion > 0.0) ? (int64_t)(1e9/ns_per_insertion) : 0);

      // Retrieve
      ts0 = __GET_CURRENT_NANOSECOND_TICK();
      n = 0;
      while( n < iterations ) { // at least this many to get a good reading
        for( int64_t i=0; i<current_size; i++ ) {
          iFramehash.simple.GetInt( map, fhdyn, i, &val );
          n++;
        }
      }
      ts1 = __GET_CURRENT_NANOSECOND_TICK();
      int64_t retrieve_ns = ts1 - ts0;
      double ns_per_retrieval = retrieve_ns / (double)n;
      int64_t retrievals_per_sec = ((ns_per_retrieval > 0.0) ? (int64_t)(1e9/ns_per_retrieval) : 0);

      printf( "%15lld, %15.3f, %15lld, %15.3f, %15lld\n", current_size, ns_per_insertion, insertions_per_sec, ns_per_retrieval, retrievals_per_sec );

      // Break down
      iFramehash.simple.Destroy( &map, fhdyn );
      iFramehash.dynamic.ClearDynamic( &local_dyn );
      fhdyn = NULL;

      // Next
      double new_size = current_size * 1.2;
      if( new_size < current_size + 1 ) {
        current_size++;
      }
      else if( current_size == limit ) {
        current_size++; // trigger end
      }
      else if( new_size > limit ) {
        current_size = limit;
      }
      else {
        current_size = (int64_t)new_size;
      }
    }

  } END_TEST_SCENARIO

} END_UNIT_TEST


#endif
