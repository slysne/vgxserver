/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    __utest_framehash_hashing.h
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

#ifndef __UTEST_FRAMEHASH_HASHING_H
#define __UTEST_FRAMEHASH_HASHING_H


int bitvector_set( QWORD V[], int64_t idx ) {
  int64_t qw = idx >> 6;
  int bit = idx & 0x3f;
  uint64_t mask = 1ULL << bit;
  int prev = V[qw] & mask ? 1 : 0;
  V[qw] |= mask;
  return prev;
}


BEGIN_UNIT_TEST( __utest_framehash_hashing__get_hashbits ) {
  NEXT_TEST_SCENARIO( true, "Bit extraction for domains 1-4" ) {
    TEST_ASSERTION( _framehash_hashing__get_hashbits( 1, 0xAAAABBBBCCCCDDDD ) == 0x5DDD,   "bits 14-0 for domain 1" );
    TEST_ASSERTION( _framehash_hashing__get_hashbits( 2, 0xAAAABBBBCCCCDDDD ) == 0x4CCC,   "bits 30-16 for domain 2" );
    TEST_ASSERTION( _framehash_hashing__get_hashbits( 3, 0xAAAABBBBCCCCDDDD ) == 0x3BBB,   "bits 46-32 for domain 3" );
    TEST_ASSERTION( _framehash_hashing__get_hashbits( 4, 0xAAAABBBBCCCCDDDD ) == 0x2AAA,   "bits 62-48 for domain 4" );
  } END_TEST_SCENARIO
  NEXT_TEST_SCENARIO( true, "No bit extraction for domain 0" ) {
    TEST_ASSERTION( _framehash_hashing__get_hashbits( 0, 0xAAAABBBBCCCCDDDD ) == HASHBITS_INVALID,   "hashbits are invalid for domain 0" );
  } END_TEST_SCENARIO
} END_UNIT_TEST


BEGIN_UNIT_TEST( __utest_framehash_hashing__short_hashkey ) {
  NEXT_TEST_SCENARIO( true, "Verify recursive key generation does not produce same key" ) {
#define TESTSIZE (1LL<<30)
    uint64_t start = 1000;
    uint64_t n = start;
    uint64_t m;
    for( int64_t i=0; i<TESTSIZE; i++ ) {
      m = framehash_hashing__short_hashkey( n );
      TEST_ASSERTION( m != n,     "hash value must be different from input value" );
      TEST_ASSERTION( m != start, "unexpected cycle" );
      n = m;
    }
#undef TESTSIZE
  } END_TEST_SCENARIO
} END_UNIT_TEST


BEGIN_UNIT_TEST( __utest_framehash_hashing__tiny_hashkey ) {
  NEXT_TEST_SCENARIO( true, "Measure point of first collision for 32-bit hash" ) {
#define TESTSIZE (1ULL<<32)
    QWORD *bitvector = (QWORD*)calloc( TESTSIZE/64, 8 );
    TEST_ASSERTION( bitvector != NULL,  "bitvector allocated" );
    uint32_t m;
    int prev;
    for( int64_t i=0; i<TESTSIZE; i++ ) {
      m = framehash_hashing__tiny_hashkey( (uint32_t)i );
      prev = bitvector_set( bitvector, m );
      if( prev ) {
        printf( "collision at framehash_hashing__tiny_hashkey(%lld)=%u, another input hashed to same output\n", i, m );
        printf( "collision occurred after %.3f%% of range\n", ((double)i/TESTSIZE)*100.0 );
        break;
      }
    }
    free( bitvector );
  } END_TEST_SCENARIO
} END_UNIT_TEST




#endif
