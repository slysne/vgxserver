/*######################################################################
 *#
 *# __utest_framehash_api_class.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_FRAMEHASH_API_CLASS_H
#define __UTEST_FRAMEHASH_API_CLASS_H


BEGIN_UNIT_TEST( __utest_framehash_api_class ) {

  NEXT_TEST_SCENARIO( true, "Framehash Object" ) {


    framehash_constructor_args_t args = FRAMEHASH_DEFAULT_ARGS;
    args.param.order = 0;

    framehash_t *F = COMLIB_OBJECT_NEW( framehash_t, NULL, &args );
    TEST_ASSERTION( F != NULL,  "Framehash object created" );

    framehash_vtable_t *iF = CALLABLE( F );

    int64_t key1 = 1234;
    int64_t key2 = 5678;
    int64_t value1 = 808;
    int64_t value2 = 909;

    int64_t ret = -1;

    framehash_valuetype_t t;

    // Try to get item1 (nonexist)
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key1, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL,    "item1 should not exist" );
    TEST_ASSERTION( ret == -1,                    "nothing returned" );

    // Insert item1 and item2
    iF->Set( F, CELL_KEY_TYPE_PLAIN64, CELL_VALUE_TYPE_INTEGER, &key1, &value1 );
    iF->Set( F, CELL_KEY_TYPE_PLAIN64, CELL_VALUE_TYPE_INTEGER, &key2, &value2 );
    
    // Verify items
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key1, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER,    "item1 should have integer value" );
    TEST_ASSERTION( ret == value1,                   "value1 should match" );
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key2, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER,    "item2 should have integer value" );
    TEST_ASSERTION( ret == value2,                   "value2 should match" );

    // Delete item2
    t = iF->Delete( F, CELL_KEY_TYPE_PLAIN64, &key2 );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER,    "item2 should be deleted" );
    t = iF->Delete( F, CELL_KEY_TYPE_PLAIN64, &key2 );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL,       "item2 already deleted" );

    // Verify items
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key1, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER,    "item1 should have integer value" );
    TEST_ASSERTION( ret == value1,                   "value1 should match" );
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key2, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL,       "item2 should not exist" );

    // Delete item1
    t = iF->Delete( F, CELL_KEY_TYPE_PLAIN64, &key1 );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER,    "item1 should be deleted" );

    // Verify no items
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key1, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL,       "item1 should not exist" );
    t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key2, &ret );
    TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL,       "item2 should not exist" );

    COMLIB_OBJECT_DESTROY( F );


  } END_TEST_SCENARIO




  NEXT_TEST_SCENARIO( true, "Exhaustive frame grow/shrink" ) {


    framehash_constructor_args_t args = FRAMEHASH_DEFAULT_ARGS;
    args.param.order = 0;

    framehash_t *F = COMLIB_OBJECT_NEW( framehash_t, NULL, &args );
    TEST_ASSERTION( F != NULL,  "Framehash object created" );

    framehash_vtable_t *iF = CALLABLE( F );


    int64_t limit = 256 * 256;
    int64_t val;
    int64_t ret;
    framehash_valuetype_t t;

    int64_t size = 1;
    while( size <= limit  ) {

      // Populate
      for( int64_t key=0; key<size; key++ ) {
        val = key * size;
        iF->Set( F, CELL_KEY_TYPE_PLAIN64, CELL_VALUE_TYPE_INTEGER, &key, &val );
      }

      // Verify all
      for( int64_t key=0; key<size; key++ ) {
        val = key * size;
        iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key, &ret );
        TEST_ASSERTION( ret == val, "" );
      }

      // Delete one at a time and verify
      for( int64_t key=0; key<size; key++ ) {
        val = key * size;
        iF->Delete( F, CELL_KEY_TYPE_PLAIN64, &key );
        t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key, &ret );
        TEST_ASSERTION( t == CELL_VALUE_TYPE_NULL, "" );
        int64_t key2 = key+1;
        if( key2 < size ) {
          int64_t val2 = key2 * size;
          t = iF->Get( F, CELL_KEY_TYPE_PLAIN64, &key2, &ret );
          TEST_ASSERTION( t == CELL_VALUE_TYPE_INTEGER && ret == val2, "" );
        }
      }

      size++;

    }

    COMLIB_OBJECT_DESTROY( F );


  } END_TEST_SCENARIO


} END_UNIT_TEST


#endif
