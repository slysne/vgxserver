/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxmath.h
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

#ifndef CXLIB_CXMATH_H
#define CXLIB_CXMATH_H

#include "cxplat.h"
#include "cxobjbase.h"
#include <limits.h>


#define HAMMING_METHOD_INTRINSIC 1
#define HAMMING_METHOD_LOOKUP    2
#define HAMMING_METHOD_PARALLEL  3


#define HAMMING_METHOD HAMMING_METHOD_INTRINSIC

#if HAMMING_METHOD == HAMMING_METHOD_INTRINSIC
#  if defined _WIN32 || defined _WIN64
unsigned __int64 __popcnt64( unsigned __int64 value );
#    pragma intrinsic ( __popcnt64 )
#  else
/* UNIX (gcc) */
#    define __popcnt64  __builtin_popcountll
#  endif
#  define hamdist64(a,b) (int)__popcnt64((a)^(b))
#elif HAMMING_METHOD == HAMMING_METHOD_LOOKUP
#  define hamdist64(a,b) hamming_distance_lookup(a,b)
#elif HAMMING_METHOD == HAMMING_METHOD_PARALLEL
#  define hamdist64(a,b) hamming_distance_parallel(a,b)
#else
#  error "Invalid HAMMING_METHOD definition"
#endif

#if defined CXPLAT_WINDOWS_X64
#define POPCNT16(X)  __popcnt16( (X) )
#define POPCNT32(X)  __popcnt( (X) )
#define POPCNT64(X)  __popcnt64( (X) )
#else
#if defined CXPLAT_LINUX_ANY
#pragma GCC target("popcnt,lzcnt,bmi,bmi2")
#endif
#define POPCNT16(X)  __builtin_popcount( (X) & 0xFFFF )
#define POPCNT32(X)  __builtin_popcount( (X) )
#define POPCNT64(X)  __builtin_popcountll( (X) )
#define _byteswap_uint64 __builtin_bswap64
#endif



/* math */
double fac( int n );
double comb( int n, int m );

int mag2( double x );
int32_t pow2log2( double x );

/* misc. */
const char * itob( uint64_t x );
int strtoint64( const char *str, int64_t *value );

/* LFSR */
uint8_t __lfsr3( uint8_t seed );
uint8_t __lfsr4( uint8_t seed );
uint8_t __lfsr5( uint8_t seed );
uint8_t __lfsr6( uint8_t seed );
uint8_t __lfsr7( uint8_t seed );
uint8_t __lfsr8( uint8_t seed );
uint16_t __lfsr9( uint16_t seed );
uint16_t __lfsr10( uint16_t seed );
uint16_t __lfsr11( uint16_t seed );
uint16_t __lfsr12( uint16_t seed );
uint16_t __lfsr13( uint16_t seed );
uint16_t __lfsr14( uint16_t seed );
uint16_t __lfsr15( uint16_t seed );
uint16_t __lfsr16( uint16_t seed );
uint32_t __lfsr17( uint32_t seed );
uint32_t __lfsr18( uint32_t seed );
uint32_t __lfsr19( uint32_t seed );
uint32_t __lfsr20( uint32_t seed );
uint32_t __lfsr21( uint32_t seed );
uint32_t __lfsr22( uint32_t seed );
uint32_t __lfsr23( uint32_t seed );
uint32_t __lfsr24( uint32_t seed );
uint32_t __lfsr25( uint32_t seed );
uint32_t __lfsr26( uint32_t seed );
uint32_t __lfsr27( uint32_t seed );
uint32_t __lfsr28( uint32_t seed );
uint32_t __lfsr29( uint32_t seed );
uint32_t __lfsr30( uint32_t seed );
uint32_t __lfsr31( uint32_t seed );
uint32_t __lfsr32( uint32_t seed );
uint64_t __lfsr33( uint64_t seed );
uint64_t __lfsr34( uint64_t seed );
uint64_t __lfsr35( uint64_t seed );
uint64_t __lfsr36( uint64_t seed );
uint64_t __lfsr37( uint64_t seed );
uint64_t __lfsr38( uint64_t seed );
uint64_t __lfsr39( uint64_t seed );
uint64_t __lfsr40( uint64_t seed );
uint64_t __lfsr41( uint64_t seed );
uint64_t __lfsr42( uint64_t seed );
uint64_t __lfsr43( uint64_t seed );
uint64_t __lfsr44( uint64_t seed );
uint64_t __lfsr45( uint64_t seed );
uint64_t __lfsr46( uint64_t seed );
uint64_t __lfsr47( uint64_t seed );
uint64_t __lfsr48( uint64_t seed );
uint64_t __lfsr49( uint64_t seed );
uint64_t __lfsr50( uint64_t seed );
uint64_t __lfsr51( uint64_t seed );
uint64_t __lfsr52( uint64_t seed );
uint64_t __lfsr53( uint64_t seed );
uint64_t __lfsr54( uint64_t seed );
uint64_t __lfsr55( uint64_t seed );
uint64_t __lfsr56( uint64_t seed );
uint64_t __lfsr57( uint64_t seed );
uint64_t __lfsr58( uint64_t seed );
uint64_t __lfsr59( uint64_t seed );
uint64_t __lfsr60( uint64_t seed );
uint64_t __lfsr61( uint64_t seed );
uint64_t __lfsr62( uint64_t seed );
uint64_t __lfsr63( uint64_t seed );
uint64_t __lfsr64( uint64_t seed );
void __lfsr_init( uint64_t seed );


/* pseudo-random */
uint8_t rand8(void);
uint16_t rand9( void );
uint16_t rand10( void );
uint16_t rand11( void );
uint16_t rand12( void );
uint16_t rand13( void );
uint16_t rand14( void );
uint16_t rand15( void );
uint16_t rand16( void );
uint32_t rand17( void );
uint32_t rand18( void );
uint32_t rand19( void );
uint32_t rand20( void );
uint32_t rand21( void );
uint32_t rand22( void );
uint32_t rand23( void );
uint32_t rand24( void );
uint32_t rand25( void );
uint32_t rand26( void );
uint32_t rand27( void );
uint32_t rand28( void );
uint32_t rand29( void );
uint32_t rand30( void );
uint32_t rand31( void );
uint32_t rand32( void );
uint64_t rand33( void );
uint64_t rand34( void );
uint64_t rand35( void );
uint64_t rand36( void );
uint64_t rand37( void );
uint64_t rand38( void );
uint64_t rand39( void );
uint64_t rand40( void );
uint64_t rand41( void );
uint64_t rand42( void );
uint64_t rand43( void );
uint64_t rand44( void );
uint64_t rand45( void );
uint64_t rand46( void );
uint64_t rand47( void );
uint64_t rand48( void );
uint64_t rand49( void );
uint64_t rand50( void );
uint64_t rand51( void );
uint64_t rand52( void );
uint64_t rand53( void );
uint64_t rand54( void );
uint64_t rand55( void );
uint64_t rand56( void );
uint64_t rand57( void );
uint64_t rand58( void );
uint64_t rand59( void );
uint64_t rand60( void );
uint64_t rand61( void );
uint64_t rand62( void );
uint64_t rand63( void );
uint64_t rand64( void );


double randfloat( void );


/* hashing */
int32_t hash32( const unsigned char * data, int32_t len );
int32_t strhash32( const unsigned char * data );
uint64_t ihash64( uint64_t n );
int64_t hash64( const unsigned char * data, int64_t len );
objectid_t hash128( const unsigned char *data, int64_t len );
int64_t strhash64( const unsigned char *data );
objectid_t strhash128( const unsigned char *data );


int64_t iround( double X );


int ilog2( uint64_t X );
int imag2( uint64_t X );
uint64_t ipow2log2( uint64_t X );


#endif
