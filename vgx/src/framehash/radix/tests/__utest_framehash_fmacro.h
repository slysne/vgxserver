/*######################################################################
 *#
 *# __utest_framehash_fmacro.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_FRAMEHASH_FMACRO_H
#define __UTEST_FRAMEHASH_FMACRO_H




BEGIN_UNIT_TEST( __utest_framehash_fmacro__arithmetic_macros ) {
  IGNORE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT
  NEXT_TEST_SCENARIO( true, "Verify basic constants" ) {
    TEST_ASSERTION( FRAMEHASH_CELLS_PER_SLOT == 4,        "expected FRAMEHASH_CELLS_PER_SLOT==4, found %d", FRAMEHASH_CELLS_PER_SLOT );
    TEST_ASSERTION( FRAMEHASH_CELLS_PER_HALFSLOT == 2,    "expected FRAMEHASH_CELLS_PER_HALFSLOT==2, found %d", FRAMEHASH_CELLS_PER_HALFSLOT );
    TEST_ASSERTION( _FRAMEHASH_P_MAX == 6,                "expected _FRAMEHASH_P_MAX==6, found %d", _FRAMEHASH_P_MAX );
    TEST_ASSERTION( _FRAMEHASH_OFFSET_MASK == 0x3f,       "expected _FRAMEHASH_OFFSET_MASK==0x3f, found %x", _FRAMEHASH_OFFSET_MASK );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _CHAININDEX_SLOT_Q()" ) {
    TEST_ASSERTION( _CHAININDEX_SLOT_Q( 0x0 ) == 0x0,     "" );
    TEST_ASSERTION( _CHAININDEX_SLOT_Q( 0x1 ) == 0x0,     "" );
    TEST_ASSERTION( _CHAININDEX_SLOT_Q( 0x3 ) == 0x0,     "" );
    TEST_ASSERTION( _CHAININDEX_SLOT_Q( 0x7 ) == 0x1,     "" );
    TEST_ASSERTION( _CHAININDEX_SLOT_Q( 0x8 ) == 0x2,     "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _CHAININDEX_CELL_J()" ) {
    TEST_ASSERTION( _CHAININDEX_CELL_J( 0x0 ) == 0x0,     "" );
    TEST_ASSERTION( _CHAININDEX_CELL_J( 0x1 ) == 0x1,     "" );
    TEST_ASSERTION( _CHAININDEX_CELL_J( 0x3 ) == 0x3,     "" );
    TEST_ASSERTION( _CHAININDEX_CELL_J( 0x4 ) == 0x0,     "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _SLOT_Q_GET_CHAININDEX()" ) {
    TEST_ASSERTION( _SLOT_Q_GET_CHAININDEX( 0x0 ) == 0x0,  "" );
    TEST_ASSERTION( _SLOT_Q_GET_CHAININDEX( 0x1 ) == 0x4,  "" );
    TEST_ASSERTION( _SLOT_Q_GET_CHAININDEX( 0x2 ) == 0x8,  "" );
    TEST_ASSERTION( _SLOT_Q_GET_CHAININDEX( 0x3 ) == 0xC,  "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _CACHE_FRAME_NSLOTS()" ) {
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 0 ) == 1,        "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 1 ) == 2,        "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 2 ) == 4,        "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 3 ) == 8,        "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 4 ) == 0x10,     "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 5 ) == 0x20,     "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 6 ) == 0x40,     "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 7 ) == 0x80,     "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 8 ) == 0x100,    "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 9 ) == 0x200,    "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 10 ) == 0x400,   "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 11 ) == 0x800,   "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 12 ) == 0x1000,  "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 13 ) == 0x2000,  "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 14 ) == 0x4000,  "" );
    TEST_ASSERTION( _CACHE_FRAME_NSLOTS( 15 ) == 0x8000,  "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _CACHE_FRAME_INDEX_BITS()" ) {
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 0 ) == 0,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 1 ) == 1,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 2 ) == 2,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 3 ) == 3,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 4 ) == 4,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 5 ) == 5,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 6 ) == 6,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 7 ) == 7,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 8 ) == 8,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 9 ) == 9,    "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 10 ) == 10,  "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 11 ) == 11,  "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 12 ) == 12,  "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 13 ) == 13,  "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 14 ) == 14,  "" );
    TEST_ASSERTION( _CACHE_FRAME_INDEX_BITS( 15 ) == 15,  "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _ZONE_INDEXMASK()" ) {
    TEST_ASSERTION( _ZONE_INDEXMASK( 0 ) == 0x00,         "" );
    TEST_ASSERTION( _ZONE_INDEXMASK( 1 ) == 0x01,         "" );
    TEST_ASSERTION( _ZONE_INDEXMASK( 2 ) == 0x03,         "" );
    TEST_ASSERTION( _ZONE_INDEXMASK( 3 ) == 0x07,         "" );
    TEST_ASSERTION( _ZONE_INDEXMASK( 4 ) == 0x0f,         "" );
    TEST_ASSERTION( _ZONE_INDEXMASK( 5 ) == 0x1f,         "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _FRAME_NSLOTS()" ) {
    TEST_ASSERTION( _FRAME_NSLOTS( 0 ) == 0,              "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 1 ) == 1,              "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 2 ) == 3,              "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 3 ) == 7,              "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 4 ) == 15,             "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 5 ) == 31,             "" );
    TEST_ASSERTION( _FRAME_NSLOTS( 6 ) == 63,             "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _FRAME_NCHAINSLOTS()" ) {
    TEST_ASSERTION( _FRAME_NCHAINSLOTS( 6 ) == 16,        "" ); 
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _FRAME_INDEX_BITS()" ) {
    TEST_ASSERTION( _FRAME_INDEX_BITS( 0 ) == 0,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 1 ) == 1,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 2 ) == 2,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 3 ) == 3,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 4 ) == 4,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 5 ) == 5,          "" );
    TEST_ASSERTION( _FRAME_INDEX_BITS( 6 ) == 6,          "" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "Verify _FRAME_ZONESLOTS()" ) {
    TEST_ASSERTION( _FRAME_ZONESLOTS( 0 ) == 1,           "" );
    TEST_ASSERTION( _FRAME_ZONESLOTS( 1 ) == 2,           "" );
    TEST_ASSERTION( _FRAME_ZONESLOTS( 2 ) == 4,           "" );
    TEST_ASSERTION( _FRAME_ZONESLOTS( 3 ) == 8,           "" );
    TEST_ASSERTION( _FRAME_ZONESLOTS( 4 ) == 16,          "" );
    TEST_ASSERTION( _FRAME_ZONESLOTS( 5 ) == 32,          "" );
  } END_TEST_SCENARIO
  RESUME_WARNINGS
} END_UNIT_TEST



BEGIN_UNIT_TEST( __utest_framehash_fmacro__shortid ) {
  shortid_t id64 = framehash_hashing__short_hashkey( 12345 );
  objectid_t obid = { .L=0x1111, .H=0xABCDEF };
  shortid_t *pid64 = &id64;
  objectid_t *pobid = &obid;
  TEST_ASSERTION( _SHORTID( pid64, pobid ) == obid.L,     "shortid is the obid's lower bits when obid is given" );
  pobid = NULL;
  TEST_ASSERTION( _SHORTID( pid64, pobid ) == id64,       "shortid is the id64 when obid is NULL" );
} END_UNIT_TEST



BEGIN_UNIT_TEST( __utest_framehash_fmacro__cell_item_match ) {
  QWORD key = 12345;
  shortid_t id64 = framehash_hashing__short_hashkey( key );
  objectid_t obid = { .L=id64, .H=0xABCDEF };

  framehash_cell_t cell;
  bool match;

  // INTEGER
  NEXT_TEST_SCENARIO( true, "mismatch on plain key" ) {
    /* key mismatch */
    framehash_context_t context = { .key={ .plain=key, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_PLAIN64 };
    _CELL_KEY( &cell ) = id64; // <-- prepare cell for no match (we're matching on plain key, not hashed key)
    APTR_SET_INTEGER( &cell, 1000 );
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == false,        "no match on plain key" );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "match on plain key" ) {
    /* key match, integer cell */
    framehash_context_t context = { .key={ .plain=key, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_PLAIN64 };
    _CELL_KEY( &cell ) = key; // <-- prepare cell for match
    APTR_SET_INTEGER( &cell, 1000 );
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "match on plain key" );
  } END_TEST_SCENARIO

  // HASH64
  NEXT_TEST_SCENARIO( true, "mismatch on hash64 key" ) {
    /* shortid mismatch */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_HASH64 };
    _CELL_SHORTID( &cell ) = key; // <-- prepare cell for no match (we're matching on key hash, not original key)
    APTR_SET_INTEGER( &cell, 1000 );
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == false,        "no match on shortid" );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "match on hash64 key" ) {
    /* shortid match, integer cell */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_HASH64 };
    _CELL_SHORTID( &cell ) = id64; // <-- prepare cell for match
    APTR_SET_INTEGER( &cell, 1000 );
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "match on shortid" );
  } END_TEST_SCENARIO


  // HASH128
  NEXT_TEST_SCENARIO( true, "mismatch on hash128 in membership cell" ) {
    /* idH mismatch, membership cell */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=&obid, .ktype=CELL_KEY_TYPE_HASH128 };
    APTR_AS_ANNOTATION( &cell ) = id64;
    APTR_SET_IDHIGH( &cell, 0xFFFF );    // <-- prepare cell for mismatch on idH
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == false,        "no match on full object id in membership cell" );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "match on hash128 in membership cell" ) {
    /* idH match, membership cell */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=&obid, .ktype=CELL_KEY_TYPE_HASH128 };
    APTR_AS_ANNOTATION( &cell ) = id64;
    APTR_SET_IDHIGH( &cell, obid.H );    // <-- prepare cell for match on idH
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "match on full object id in membership cell" );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "object hash128 mismatch" ) {
    /* object mismatch */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=&obid, .ktype=CELL_KEY_TYPE_HASH128 };
    objectid_t different_obid = { .L=id64, .H=0xD1FFD1FF };
    // initialize test object
    FramehashTestObject_t *obj = COMLIB_OBJECT_NEW( FramehashTestObject_t, &different_obid, NULL );   // <-- no match
    TEST_ASSERTION( obj != NULL,                        "new test object created" );
    TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),    "test object is destructible" );
    // set cell
    APTR_AS_ANNOTATION( &cell ) = different_obid.L;     // prepare cell for mismatch
    APTR_SET_POINTER( &cell, obj );                     // assign object to cell
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == false,        "no object match" );
    COMLIB_OBJECT_DESTROY( obj );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "object hash128 match" ) {
    /* object match */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=&obid, .ktype=CELL_KEY_TYPE_HASH128 };
    // initialize test object
    FramehashTestObject_t *obj = COMLIB_OBJECT_NEW( FramehashTestObject_t, &obid, NULL );   // <-- match
    TEST_ASSERTION( obj != NULL,                        "new test object created" );
    TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),    "test object is destructible" );
    // set cell
    APTR_AS_ANNOTATION( &cell ) = obid.L;               // prepare cell for match
    APTR_SET_POINTER( &cell, obj );                     // assign object to cell
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "object match" );
    COMLIB_OBJECT_DESTROY( obj );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "object hash128 false positive match when context obid is NULL (revert 128->64 match)" ) {
    /* shortid context will match different object (accepted false positive risk) */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_HASH128 };
    objectid_t different_obid = { .L=id64, .H=0xD1FFD1FF };
    // initialize test object
    FramehashTestObject_t *obj = COMLIB_OBJECT_NEW( FramehashTestObject_t, &different_obid, NULL );   // <-- mismatch, but will get match (false positive)
    TEST_ASSERTION( obj != NULL,                        "new test object created" );
    TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),    "test object is destructible" );
    // set cell
    APTR_AS_ANNOTATION( &cell ) = different_obid.L;      // prepare cell for false positive match
    APTR_SET_POINTER( &cell, obj );                      // assign object to cell
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "match (false positive) since high ID not available in probe" );
    COMLIB_OBJECT_DESTROY( obj );
  } END_TEST_SCENARIO

  NEXT_TEST_SCENARIO( true, "object hash128 true positive match when context obid is NULL (revert 128->64 match)" ) {
    /* shortid context will match same object */
    framehash_context_t context = { .key={ .plain=0, .shortid=id64 }, .obid=NULL, .ktype=CELL_KEY_TYPE_HASH128 };
    // initialize test object
    FramehashTestObject_t *obj = COMLIB_OBJECT_NEW( FramehashTestObject_t, &obid, NULL );   // <-- match
    TEST_ASSERTION( obj != NULL,                        "new test object" );
    TEST_ASSERTION( COMLIB_OBJECT_DESTRUCTIBLE(obj),    "test object is destructible" );
    // set cell
    APTR_AS_ANNOTATION( &cell ) = obid.L;               // prepare cell for true positive match
    APTR_SET_POINTER( &cell, obj );                     // assign object to cell
    _CELL_ITEM_MATCH( &context, &cell, &match );
    TEST_ASSERTION( match == true,         "" );
    COMLIB_OBJECT_DESTROY( obj );
  } END_TEST_SCENARIO

} END_UNIT_TEST



#endif
