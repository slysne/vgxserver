/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxmath.c
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

#include "cxmath.h"



/*******************************************************************//**
 *
 ***********************************************************************
 */
const char * itob( uint64_t x) {
  static char s[65];
  uint64_t mask = 0x8000000000000000ull;
  char *p;
  p = s;
  while(mask) {
    *p++ = x&mask ? '1' : '0';
    mask >>= 1;
  }
  *p = '\0';
  return s;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int strtoint64( const char *str, int64_t *value ) {
  int sign = 1;
  const char *p = str;
  // Flip sign?
  if( *p == '-' ) {
    sign = -1;
    ++p;
  }
  // Empty?
  if( *p == '\0' ) {
    return -1;
  }
  // Scan digits
  char c;
  int digit;
  *value = 0;
  while( (c=*p++) >= 48 && (digit = c & 0xCF) < 10 ) {
    *value = *value * 10 + digit;
  }
  // Apply sign
  *value *= sign;
  // Return 0 if scan made it to end without error, else -1
  return c == '\0' ? 0 : -1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
double fac( int n ) {
  if( n > 170 ) {
    return DBL_MAX;
  }
  if( n <= 0 ) {
    return 1.0;
  }
  return (double)n * fac(n-1);
}



/*******************************************************************//**
 * nCm
 * n! / ( m! * (n-m)! )
 *
 ***********************************************************************
 */
double comb( int n, int m ) {
  int d = n - m;
  if( d > 0 && d < n ) {
    // prod( [d+1, d+2, ..., n] ) / m!
    // prod( [m+1, m+2, ..., n] ) / d!
    // E.g. 10! / ( 3! * 7! )
    int q = (d > m) ? d : m;  // q = (7>3) ? 7 : 3; ==> 7
    int r = n - q;            // r = 10 - 7; ==> 3
    double p = ++q;           // p = 8
    while( q < n ) {         
      p *= ++q;               // p *= 9, p *= 10
    }
    double c = p / fac( r );      // (8*9*10) / (1*2*3)
    if( isfinite(c) ) {
      return c;
    }
    else {
      return DBL_MAX;
    }
  }
  else {
    return 1.0;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int mag2( double x ) {
  return (int)ceil( log2( x ) );
}



/*******************************************************************//**
 *
 * Return the smallest power of 2 larger than or equal to x.
 *
 ***********************************************************************
 */
int32_t pow2log2( double x ) {
  return (int32_t)pow(2,mag2(x));
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __MAGIC_20 1000003L /* 11110100001001000011   -- 11 0s, 9 1s */
int32_t hash32( const unsigned char *data, int32_t len ) { 
  const unsigned char *p = data; 
  register int32_t sz = len;
  register int32_t x; 
  x = *p << 7; 
  while( --sz >= 0 ) {
    x = (__MAGIC_20*x) ^ *p++; // magic 20-bit prime number
  }
  x ^= len;
  return x ? x : 1; // never 0
} 



/*******************************************************************//**
 *
 ***********************************************************************
 */
int32_t strhash32( const unsigned char *data ) { 
  const unsigned char *p = data;
  register int32_t sz = 0;
  register int32_t x;
  x = *p << 7;
  while( *p != '\0' ) {
    x = (__MAGIC_20*x) ^ *p++; // magic 20-bit prime number
    ++sz;
  }
  x ^= sz;
  return x ? x : 1; // never 0
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
//#define __MAGIC_52 3325997869054417LL /* 1011110100001111101000111001100000100011100111010001   -- 26 0s, 26 1s */
/*int64_t OLD_hash64( const unsigned char *data, int64_t len ) { 
  const unsigned char *p = data; 
  register int64_t sz = len;
  register int64_t x; 
  x = (int64_t)(*p) << 7; // fills 12 bits for lowest char 0x20, filling > 64 bits when multiplied by 52-bit number.
  while( --sz >= 0 ) {
    x = (__MAGIC_52*x) ^ *p++; // magic 52-bit prime number
  }
  x ^= len;
  return x ? x : 1; // never 0
} */



/*******************************************************************//**
 *
 ***********************************************************************
 *//*
int64_t OLD_strhash64( const unsigned char *data ) { 
  const unsigned char *p = data;
  register int64_t x; 
  x = (int64_t)(*p) << 7; // fills 12 bits for lowest char 0x20, filling > 64 bits when multiplied by 52-bit number.
  while( *p != '\0' ) {
    x = (__MAGIC_52*x) ^ *p++; // magic 52-bit prime number
  }
  x ^= (p - data);
  return x ? x : 1; // never 0
}*/



//#define o__M1 0xb13d211f7183d84fULL    /* 1011000100111101001000010001111101110001100000111101100001001111 */
//#define o__M2 0x00b49d63cc54e84dULL    /* 0000000010110100100111010110001111001100010101001110100001001101 */
//#define o__M3 0x0f3f081744f087e3ULL    /* 0000111100111111000010000001011101000100111100001000011111100011 */
//#define o__M4 0x9ccd8d547a7d9849ULL    /* 1001110011001101100011010101010001111010011111011001100001001001 */


#define ROTR(x,r) ((x >> r) | (x << (64 - r)))
#define ROTL(x,r) ((x << r) | (x >> (64 - r)))

#define __M1 0x9e3779b97f4a7c15ULL // golden ratio prime        1001111000110111011110011011100101111111010010100111110000010101  38
#define __M2 0xc6a4a7935bd1e995ULL // MurmurHash2 mix constant  1100011010100100101001111001001101011011110100011110100110010101  34
#define __M3 0x94d049bb133111ebULL // SplitMix64 constant       1001010011010000010010011011101100010011001100010001000111101011  29
#define __M4 0x2545f4914f6cdd1dULL // Xorshift* constant        0010010101000101111101001001000101001111011011001101110100011101  33

#define __N1 0xd6e8feb86659fd93ULL // Large prime from SplitMix 1101011011101000111111101011100001100110010110011111110110010011  39
#define __N2 0x5851f42d4c957f2dULL // PCG multiplier            0101100001010001111101000010110101001100100101010111111100101101  33
#define __N3 0x14057b7ef767814fULL // Prime-ish from other PRNG 0001010000000101011110110111111011110111011001111000000101001111  35
#define __N4 0x60bee2bee120fc15ULL // SplitMix constant         0110000010111110111000101011111011100001001000001111110000010101  32

/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline uint64_t ihash64( uint64_t n ) {
  uint64_t h = n ^ __M1; // initial scramble
  uint64_t b = n & 0x1FFFF; // extract 17 LSB
  b *= __M2;        // expand and mix extracted bits
  b ^= b >> 47;     // fold to diffuse
  h ^= b;           // inject, add entropy from b into main hash
  h *= __M3;        // non-linear mixing
  h ^= ROTR(h,23);  // bit spread
  h = ROTR(h,11);   // rotate for non-alignment
  h *= __M4;        // mix again
  h ^= ROTR(h,33);  // final avalanche spread
  return h;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static uint64_t ihash64v2(uint64_t n) {
  uint64_t h = n ^ __M1;
  uint64_t a = n * __M2;
  a ^= ROTR(a,47);
  h ^= a;
  h *= __M3;
  h ^= ROTR(h,23);
  h = ROTR(h,11);
  h *= __M4;
  h ^= ROTR(h,33);
  return h;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static uint64_t ihash64v3(uint64_t n) {
  uint64_t h = n ^ __N1;
  uint64_t a = n * __N2;
  a ^= ROTL(a,41);
  h ^= a;
  h *= __N3;
  h ^= ROTL(h,21);
  h = ROTL(h,7);
  h *= __N4;
  h ^= ROTL(h,19);
  return h;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int64_t hash64( const unsigned char *data, int64_t len ) {
  int64_t hash = 0;

  const unsigned char *p = data;

  // Number of chunks
  int64_t n = 0;

  // Chunk value
  uint64_t m;

  if( len > 7 ) {
    // Early mix in length
    hash = ihash64v2( len ^ __M1 );
    // Aligned path
    if( ((uint64_t)p & 7) == 0 ) {
      // Number of full 8 byte chunks in the input
      int64_t r = len / 8;
      // Process chunks of 8 bytes
      const uint64_t *u = (const uint64_t*)p;
      while( n < r ) {
        hash = ihash64v2( hash ^ *u++ ^ (++n * __M1) );
      }
      // Advance input by chunks processed
      p += 8*r;
    }
    // Unaligned path
    else {
      // Process chunks of 8 bytes
      const unsigned char *end = p + (len-7); // end before last full 8 byte chunk
      while( p < end ) {
        memcpy(&m, p, 8);
        hash = ihash64v2( hash ^ m ^ (++n * __M1) );
        p += 8;
      }
    }
  }
  
  // Populate last chunk with less-than 8 remaining bytes
  m = 0;
  int remain = (int)(len - (p - data));
  switch(remain) {
  case 7: m |= ((uint64_t)p[6]) << 48;
  case 6: m |= ((uint64_t)p[5]) << 40;
  case 5: m |= ((uint64_t)p[4]) << 32;
  case 4: m |= ((uint64_t)p[3]) << 24;
  case 3: m |= ((uint64_t)p[2]) << 16;
  case 2: m |= ((uint64_t)p[1]) << 8;
  case 1: m |= ((uint64_t)p[0]);
  }

  n += len;
  hash = ihash64v2( hash ^ m ^ ihash64v2(n) );

  return ihash64v2( hash );
} 



/*******************************************************************//**
* Fast 128-bit hash of arbitrary string input.
*
* NOTE: Remember this is *NOT* a cryptographic hash. 
*
***********************************************************************
*/
objectid_t hash128( const unsigned char *data, int64_t len ) {
  int64_t h1 = 0;
  int64_t h2 = 0;

  const unsigned char *p = data;

  // Number of chunks
  int64_t n = 0;

  // Chunk value
  uint64_t m;

  if( len > 7 ) {
    // Early mix in length
    h1 = ihash64v2( len ^ __M1 );
    h2 = ihash64v3( len ^ __N1 );
    // Aligned path
    if( ((uint64_t)p & 7) == 0 ) {
      // Number of full 8 byte chunks in the input
      int64_t r = len / 8;
      // Process chunks of 8 bytes
      const uint64_t *u = (const uint64_t*)p;
      while( n < r ) {
        h2 = ihash64v2( h1 ^ *u ^ (n * __M1) );
        h1 = ihash64v3( h2 ^ *u ^ (n * __N1) );
        ++u;
        ++n;
      }
      // Advance input by chunks processed
      p += 8*r;
    }
    // Unaligned path
    else {
      // Process chunks of 8 bytes
      const unsigned char *end = p + (len-7); // end before last full 8 byte chunk
      while( p < end ) {
        memcpy(&m, p, 8);
        h2 = ihash64v2( h1 ^ m ^ (n * __M1) );
        h1 = ihash64v3( h2 ^ m ^ (n * __N1) );
        ++n;
        p += 8;
      }
    }
  }
  
  // Populate last chunk with less-than 8 remaining bytes
  m = 0;
  int remain = (int)(len - (p - data));
  switch(remain) {
  case 7: m |= ((uint64_t)p[6]) << 48;
  case 6: m |= ((uint64_t)p[5]) << 40;
  case 5: m |= ((uint64_t)p[4]) << 32;
  case 4: m |= ((uint64_t)p[3]) << 24;
  case 3: m |= ((uint64_t)p[2]) << 16;
  case 2: m |= ((uint64_t)p[1]) << 8;
  case 1: m |= ((uint64_t)p[0]);
  }

  // Process final block, with overall length mixed in
  n += len;
  h1 = ihash64v2( h1 ^ m ^ ihash64v2(n) );
  h2 = ihash64v3( h2 ^ m ^ ihash64v3(n) );

  // Cross mix and finalize
  objectid_t h128 = {
    .L = ihash64v2( h2 ^ ihash64v3(h1 ^ __M1) ^ __N4 ),
    .H = ihash64v3( h1 ^ ihash64v2(h2 ^ __N1) ^ __M4 )
  };

  return h128;
} 



/*******************************************************************//**
 *
 ***********************************************************************
 */
int64_t strhash64( const unsigned char *data ) {
  return hash64( data, strlen((const char *)data) );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t strhash128( const unsigned char *data ) {
  return hash128( data, strlen((const char *)data) );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int64_t iround( double X ) {
  return (int64_t)roundl(X);
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int ilog2( uint64_t X ) {
#if defined CXPLAT_WINDOWS_X64
  unsigned long Y;
  return _BitScanReverse64( &Y, X ) > 0 ? (int)Y : -1;
#elif defined __GNUC__
  return X > 0 ? 63-__builtin_clzll( X ) : -1;
#else
  return (int)log2(X);
#endif
}


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int imag2( uint64_t X ) {
  return X > 1 ? ilog2( X-1 ) + 1 : X==1 ? 0 : -1;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
uint64_t ipow2log2( uint64_t X ) {
  return 1ULL << imag2( X );
}



#define __lfsr_2_xor( a, b )              ( (1ULL<<(a-1)) + (1ULL<<(b-1)) )
#define __lfsr_4_xor( a, b, c, d )        ( (1ULL<<(a-1)) + (1ULL<<(b-1)) + (1ULL<<(c-1)) + (1ULL<<(d-1)) )
#define __lfsr_6_xor( a, b, c, d, e, f )  ( (1ULL<<(a-1)) + (1ULL<<(b-1)) + (1ULL<<(c-1)) + (1ULL<<(d-1)) + (1ULL<<(e-1)) + (1ULL<<(f-1)) )


/*
  LFSR REF: http://www.xilinx.com/support/documentation/application_notes/xapp052.pdf
*/

const static uint8_t  __lfsr3_xor  =  (uint8_t)__lfsr_2_xor(    3,   2            );
const static uint8_t  __lfsr4_xor  =  (uint8_t)__lfsr_2_xor(    4,   3            );
const static uint8_t  __lfsr5_xor  =  (uint8_t)__lfsr_2_xor(    5,   3            );
const static uint8_t  __lfsr6_xor  =  (uint8_t)__lfsr_2_xor(    6,   5            );
const static uint8_t  __lfsr7_xor  =  (uint8_t)__lfsr_2_xor(    7,   6            );
const static uint8_t  __lfsr8_xor  =  (uint8_t)__lfsr_4_xor(    8,   6,   5,   4  );
const static uint16_t __lfsr9_xor  = (uint16_t)__lfsr_2_xor(    9,   5            );
const static uint16_t __lfsr10_xor = (uint16_t)__lfsr_2_xor(   10,   7            ); 
const static uint16_t __lfsr11_xor = (uint16_t)__lfsr_2_xor(   11,   9            );
const static uint16_t __lfsr12_xor = (uint16_t)__lfsr_4_xor(   12,   6,   4,   1  );
const static uint16_t __lfsr13_xor = (uint16_t)__lfsr_4_xor(   13,   4,   3,   1  );
const static uint16_t __lfsr14_xor = (uint16_t)__lfsr_4_xor(   14,   5,   3,   1  );
const static uint16_t __lfsr15_xor = (uint16_t)__lfsr_2_xor(   15,  14            );
const static uint16_t __lfsr16_xor = (uint16_t)__lfsr_4_xor(   16,  15,  13,   4  );
const static uint32_t __lfsr17_xor = (uint32_t)__lfsr_2_xor(   17,  14            );
const static uint32_t __lfsr18_xor = (uint32_t)__lfsr_2_xor(   18,  11            );
const static uint32_t __lfsr19_xor = (uint32_t)__lfsr_4_xor(   19,   6,   2,   1  );
const static uint32_t __lfsr20_xor = (uint32_t)__lfsr_2_xor(   20,  17            );
const static uint32_t __lfsr21_xor = (uint32_t)__lfsr_2_xor(   21,  19            );
const static uint32_t __lfsr22_xor = (uint32_t)__lfsr_2_xor(   22,  21            );
const static uint32_t __lfsr23_xor = (uint32_t)__lfsr_2_xor(   23,  18            );
const static uint32_t __lfsr24_xor = (uint32_t)__lfsr_4_xor(   24,  23,  22,  17  );
const static uint32_t __lfsr25_xor = (uint32_t)__lfsr_2_xor(   25,  22            );
const static uint32_t __lfsr26_xor = (uint32_t)__lfsr_4_xor(   26,   6,   2,   1  );
const static uint32_t __lfsr27_xor = (uint32_t)__lfsr_4_xor(   27,   5,   2,   1  );
const static uint32_t __lfsr28_xor = (uint32_t)__lfsr_2_xor(   28,  25            );
const static uint32_t __lfsr29_xor = (uint32_t)__lfsr_2_xor(   29,  27            );
const static uint32_t __lfsr30_xor = (uint32_t)__lfsr_4_xor(   30,   6,   4,   1  );
const static uint32_t __lfsr31_xor = (uint32_t)__lfsr_2_xor(   31,  28            );
const static uint32_t __lfsr32_xor = (uint32_t)__lfsr_4_xor(   32,  22,   2,   1  );
const static uint64_t __lfsr33_xor = (uint64_t)__lfsr_2_xor(   33,  20            );
const static uint64_t __lfsr34_xor = (uint64_t)__lfsr_4_xor(   34,  27,   2,   1  );
const static uint64_t __lfsr35_xor = (uint64_t)__lfsr_2_xor(   35,  33            );
const static uint64_t __lfsr36_xor = (uint64_t)__lfsr_2_xor(   36,  25            );
const static uint64_t __lfsr37_xor = (uint64_t)__lfsr_6_xor(   37,   5,   4,   3,   2,   1 );
const static uint64_t __lfsr38_xor = (uint64_t)__lfsr_4_xor(   38,   6,   5,   1  );
const static uint64_t __lfsr39_xor = (uint64_t)__lfsr_2_xor(   39,  35            );
const static uint64_t __lfsr40_xor = (uint64_t)__lfsr_4_xor(   40,  38,  21,  19  );
const static uint64_t __lfsr41_xor = (uint64_t)__lfsr_2_xor(   41,  38            );
const static uint64_t __lfsr42_xor = (uint64_t)__lfsr_4_xor(   42,  41,  20,  19  );
const static uint64_t __lfsr43_xor = (uint64_t)__lfsr_4_xor(   43,  42,  38,  37  );
const static uint64_t __lfsr44_xor = (uint64_t)__lfsr_4_xor(   44,  43,  18,  17  );
const static uint64_t __lfsr45_xor = (uint64_t)__lfsr_4_xor(   45,  44,  42,  41  );
const static uint64_t __lfsr46_xor = (uint64_t)__lfsr_4_xor(   46,  45,  26,  25  );
const static uint64_t __lfsr47_xor = (uint64_t)__lfsr_2_xor(   47,  42            );
const static uint64_t __lfsr48_xor = (uint64_t)__lfsr_4_xor(   48,  47,  21,  20  );
const static uint64_t __lfsr49_xor = (uint64_t)__lfsr_2_xor(   49,  40            );
const static uint64_t __lfsr50_xor = (uint64_t)__lfsr_4_xor(   50,  49,  24,  23  );
const static uint64_t __lfsr51_xor = (uint64_t)__lfsr_4_xor(   51,  50,  36,  35  );
const static uint64_t __lfsr52_xor = (uint64_t)__lfsr_2_xor(   52,  49            );
const static uint64_t __lfsr53_xor = (uint64_t)__lfsr_4_xor(   53,  52,  38,  37  );
const static uint64_t __lfsr54_xor = (uint64_t)__lfsr_4_xor(   54,  53,  18,  17  );
const static uint64_t __lfsr55_xor = (uint64_t)__lfsr_2_xor(   55,  31            );
const static uint64_t __lfsr56_xor = (uint64_t)__lfsr_4_xor(   56,  55,  35,  34  );
const static uint64_t __lfsr57_xor = (uint64_t)__lfsr_2_xor(   57,  50            );
const static uint64_t __lfsr58_xor = (uint64_t)__lfsr_2_xor(   58,  39            );
const static uint64_t __lfsr59_xor = (uint64_t)__lfsr_4_xor(   59,  58,  38,  37  );
const static uint64_t __lfsr60_xor = (uint64_t)__lfsr_2_xor(   60,  59            );
const static uint64_t __lfsr61_xor = (uint64_t)__lfsr_4_xor(   61,  60,  46,  45  );
const static uint64_t __lfsr62_xor = (uint64_t)__lfsr_4_xor(   62,  61,   6,   5  );
const static uint64_t __lfsr63_xor = (uint64_t)__lfsr_2_xor(   63,  62            );
const static uint64_t __lfsr64_xor = (uint64_t)__lfsr_4_xor(   64,  63,  61,  60  );



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __lfsrX8_func( N )                          \
  uint8_t __lfsr##N ( uint8_t seed ) {              \
    /* just something nonzero */                    \
    static __THREAD uint8_t state = 0x37 & ( 0xFFU >> (8-N) ); \
    if( seed ) {                                    \
      state = seed;                                 \
    }                                               \
    uint8_t lfsr = state;                           \
    uint8_t lsb = lfsr & 1;                         \
    lfsr >>= 1;                                     \
    if( lsb ) {                                     \
      lfsr ^= __lfsr##N##_xor;                      \
    }                                               \
    state = lfsr;                                   \
    return state;                                   \
  }



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __lfsrX16_func( N )                         \
  uint16_t __lfsr##N ( uint16_t seed ) {            \
    /* just something nonzero */                    \
    static __THREAD uint16_t state = 0x3ac9 & ( 0xFFFFU >> (16-N) ); \
    if( seed ) {                                    \
      state = seed;                                 \
    }                                               \
    uint16_t lfsr = state;                          \
    uint16_t lsb = lfsr & 1;                        \
    lfsr >>= 1;                                     \
    if( lsb ) {                                     \
      lfsr ^= __lfsr##N##_xor;                      \
    }                                               \
    state = lfsr;                                   \
    return state;                                   \
  }



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __lfsrX32_func( N )                         \
  uint32_t __lfsr##N ( uint32_t seed ) {            \
    /* just something nonzero */                    \
    static __THREAD uint32_t state = 0x5f01a347 & ( 0xFFFFFFFFUL >> (32-N) ); \
    if( seed ) {                                    \
      state = seed;                                 \
    }                                               \
    uint32_t lfsr = state;                          \
    uint32_t lsb = lfsr & 1;                        \
    lfsr >>= 1;                                     \
    if( lsb ) {                                     \
      lfsr ^= __lfsr##N##_xor;                      \
    }                                               \
    state = lfsr;                                   \
    return state;                                   \
  }



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __lfsrX64_func( N )                                 \
  uint64_t __lfsr##N ( uint64_t seed ) {                    \
    /* just something nonzero */                            \
    static __THREAD uint64_t state = 0x184a188dd01ca347ULL & ( 0xFFFFFFFFFFFFFFFFULL >> (64-N) ); \
    if( seed ) {                                            \
      state = seed;                                         \
    }                                                       \
    if( state == 0 ) {                                      \
    }                                                       \
    uint64_t lfsr = state;                                  \
    uint64_t lsb = lfsr & 1;                                \
    lfsr >>= 1;                                             \
    if( lsb ) {                                             \
      lfsr ^= __lfsr##N##_xor;                              \
    }                                                       \
    state = lfsr;                                           \
    return state;                                           \
  }

__lfsrX8_func(   3 )
__lfsrX8_func(   4 )
__lfsrX8_func(   5 )
__lfsrX8_func(   6 )
__lfsrX8_func(   7 )
__lfsrX8_func(   8 )

__lfsrX16_func(  9 )
__lfsrX16_func( 10 )
__lfsrX16_func( 11 )
__lfsrX16_func( 12 )
__lfsrX16_func( 13 )
__lfsrX16_func( 14 )
__lfsrX16_func( 15 )
__lfsrX16_func( 16 )

__lfsrX32_func( 17 )
__lfsrX32_func( 18 )
__lfsrX32_func( 19 )
__lfsrX32_func( 20 )
__lfsrX32_func( 21 )
__lfsrX32_func( 22 )
__lfsrX32_func( 23 )
__lfsrX32_func( 24 )
__lfsrX32_func( 25 )
__lfsrX32_func( 26 )
__lfsrX32_func( 27 )
__lfsrX32_func( 28 )
__lfsrX32_func( 29 )
__lfsrX32_func( 30 )
__lfsrX32_func( 31 )
__lfsrX32_func( 32 )

__lfsrX64_func( 33 )
__lfsrX64_func( 34 )
__lfsrX64_func( 35 )
__lfsrX64_func( 36 )
__lfsrX64_func( 37 )
__lfsrX64_func( 38 )
__lfsrX64_func( 39 )
__lfsrX64_func( 40 )
__lfsrX64_func( 41 )
__lfsrX64_func( 42 )
__lfsrX64_func( 43 )
__lfsrX64_func( 44 )
__lfsrX64_func( 45 )
__lfsrX64_func( 46 )
__lfsrX64_func( 47 )
__lfsrX64_func( 48 )
__lfsrX64_func( 49 )
__lfsrX64_func( 50 )
__lfsrX64_func( 51 )
__lfsrX64_func( 52 )
__lfsrX64_func( 53 )
__lfsrX64_func( 54 )
__lfsrX64_func( 55 )
__lfsrX64_func( 56 )
__lfsrX64_func( 57 )
__lfsrX64_func( 58 )
__lfsrX64_func( 59 )
__lfsrX64_func( 60 )
__lfsrX64_func( 61 )
__lfsrX64_func( 62 )
__lfsrX64_func( 63 )
__lfsrX64_func( 64 )



/*******************************************************************//**
 *
 ***********************************************************************
 */
void __lfsr_init( uint64_t seed ) {
  uint64_t h = ihash64( seed );
  uint8_t h8 = (uint8_t)(h & 0xFF);
  uint16_t h16 = (uint16_t)(h & 0xFFFF);
  uint32_t h32 = (uint32_t)(h & 0xFFFFFFFF);
  uint64_t h64 = (uint64_t)h;
  __lfsr3( h8 );
  __lfsr4( h8 );
  __lfsr5( h8 );
  __lfsr6( h8 );
  __lfsr7( h8 );
  __lfsr8( h8 );
  __lfsr9( h16 );
  __lfsr10( h16 );
  __lfsr11( h16 );
  __lfsr12( h16 );
  __lfsr13( h16 );
  __lfsr14( h16 );
  __lfsr15( h16 );
  __lfsr16( h16 );
  __lfsr17( h32 );
  __lfsr18( h32 );
  __lfsr19( h32 );
  __lfsr20( h32 );
  __lfsr21( h32 );
  __lfsr22( h32 );
  __lfsr23( h32 );
  __lfsr24( h32 );
  __lfsr25( h32 );
  __lfsr26( h32 );
  __lfsr27( h32 );
  __lfsr28( h32 );
  __lfsr29( h32 );
  __lfsr30( h32 );
  __lfsr31( h32 );
  __lfsr32( h32 );
  __lfsr33( h64 );
  __lfsr34( h64 );
  __lfsr35( h64 );
  __lfsr36( h64 );
  __lfsr37( h64 );
  __lfsr38( h64 );
  __lfsr39( h64 );
  __lfsr40( h64 );
  __lfsr41( h64 );
  __lfsr42( h64 );
  __lfsr43( h64 );
  __lfsr44( h64 );
  __lfsr45( h64 );
  __lfsr46( h64 );
  __lfsr47( h64 );
  __lfsr48( h64 );
  __lfsr49( h64 );
  __lfsr50( h64 );
  __lfsr51( h64 );
  __lfsr52( h64 );
  __lfsr53( h64 );
  __lfsr54( h64 );
  __lfsr55( h64 );
  __lfsr56( h64 );
  __lfsr57( h64 );
  __lfsr58( h64 );
  __lfsr59( h64 );
  __lfsr60( h64 );
  __lfsr61( h64 );
  __lfsr62( h64 );
  __lfsr63( h64 );
  __lfsr64( h64 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __randX16_func( X )                             \
  uint16_t rand##X ( void ) {                           \
    static __THREAD uint16_t mask = 0xFFFFU >> (16-X);  \
    return ihash64( __lfsr##X ( 0 ) ) & mask;           \
  }



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __randX32_func( X )                                 \
  uint32_t rand##X ( void ) {                               \
    static __THREAD uint32_t mask = 0xFFFFFFFFUL >> (32-X); \
    return ihash64( __lfsr##X ( 0 ) ) & mask;               \
  }



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __randX64_func( X )                                           \
  uint64_t rand##X ( void ) {                                         \
    static __THREAD uint64_t mask = 0xFFFFFFFFFFFFFFFFULL >> (64-X);  \
    return ihash64( __lfsr##X ( 0 ) ) & mask;                         \
  }



__randX16_func(  9 )
__randX16_func( 10 )
__randX16_func( 11 )
__randX16_func( 12 )
__randX16_func( 13 )
__randX16_func( 14 )
__randX16_func( 15 )
__randX16_func( 16 )
__randX32_func( 17 )
__randX32_func( 18 )
__randX32_func( 19 )
__randX32_func( 20 )
__randX32_func( 21 )
__randX32_func( 22 )
__randX32_func( 23 )
__randX32_func( 24 )
__randX32_func( 25 )
__randX32_func( 26 )
__randX32_func( 27 )
__randX32_func( 28 )
__randX32_func( 29 )
__randX32_func( 30 )
__randX32_func( 31 )
__randX32_func( 32 )
__randX64_func( 33 )
__randX64_func( 34 )
__randX64_func( 35 )
__randX64_func( 36 )
__randX64_func( 37 )
__randX64_func( 38 )
__randX64_func( 39 )
__randX64_func( 40 )
__randX64_func( 41 )
__randX64_func( 42 )
__randX64_func( 43 )
__randX64_func( 44 )
__randX64_func( 45 )
__randX64_func( 46 )
__randX64_func( 47 )
__randX64_func( 48 )
__randX64_func( 49 )
__randX64_func( 50 )
__randX64_func( 51 )
__randX64_func( 52 )
__randX64_func( 53 )
__randX64_func( 54 )
__randX64_func( 55 )
__randX64_func( 56 )
__randX64_func( 57 )
__randX64_func( 58 )
__randX64_func( 59 )
__randX64_func( 60 )
__randX64_func( 61 )
__randX64_func( 62 )
__randX64_func( 63 )
__randX64_func( 64 )



/*******************************************************************//**
 *
 ***********************************************************************
 */
uint8_t rand8( void ) {
  return (uint8_t)(rand() & 0xFF);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
double randfloat( void ) {
  static const double d = (double)LLONG_MAX;
  return rand63() / d;
}
