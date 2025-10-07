/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    sha256.c
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

#include "sha256.h"

// ---------------------------------------------------------------------
// NOTE: This is a non-optimized implementation, included for reference.
//       If performance critical sha256 hashing is needed, use a better
//       SIMD-accelerated implementation. Expect 10x speedup with a 
//       properly optimized version.
// ---------------------------------------------------------------------


#define ROTR(a,b)               (((a) >> (b)) | ((a) << (32-(b))))
#define BigSigma0(x)            (ROTR(x, 2) ^ ROTR(x,13) ^ ROTR(x,22))
#define BigSigma1(x)            (ROTR(x, 6) ^ ROTR(x,11) ^ ROTR(x,25))
#define SmallSigma0(x)          (ROTR(x, 7) ^ ROTR(x,18) ^ ((x) >> 3))
#define SmallSigma1(x)          (ROTR(x,17) ^ ROTR(x,19) ^ ((x) >> 10))

#define ChooseBitwise(x,y,z)    (((x) & (y)) ^ (~(x) & (z)))
#define BitwiseMajority(x,y,z)  (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __transform( cxlib_sha256_context_t *context ) {

  // Magic constants derived from the fractional parts of the first 64 prime numbers
  static const DWORD CubeRootFractional[] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
  };


  DWORD CALIGNED_ block[64];
  DWORD *mp = block;
  DWORD *zp = block;
  const BYTE *B = context->buffer;
  const BYTE *dp = B;
  const BYTE *dend = B + 64;
  while( dp < dend ) {
    mp[0] = (dp[0]  << 24) | (dp[1]  << 16) | (dp[2]  << 8) | (dp[3]);
    mp[1] = (dp[4]  << 24) | (dp[5]  << 16) | (dp[6]  << 8) | (dp[7]);
    mp[2] = (dp[8]  << 24) | (dp[9]  << 16) | (dp[10] << 8) | (dp[11]);
    mp[3] = (dp[12] << 24) | (dp[13] << 16) | (dp[14] << 8) | (dp[15]);
    mp += 4;
    dp += 16;
  }

  const DWORD *mend = block + 64;
  while( mp < mend ) {
    mp[0] = SmallSigma1(zp[14]) + zp[9]  + SmallSigma0(zp[1]) +  zp[0];
    mp[1] = SmallSigma1(zp[15]) + zp[10] + SmallSigma0(zp[2]) +  zp[1];
    mp[2] = SmallSigma1(zp[16]) + zp[11] + SmallSigma0(zp[3]) +  zp[2];
    mp[3] = SmallSigma1(zp[17]) + zp[12] + SmallSigma0(zp[4]) +  zp[3];
    mp += 4;
    zp += 4;
  }

  DWORD a = context->state[0];
  DWORD b = context->state[1];
  DWORD c = context->state[2];
  DWORD d = context->state[3];
  DWORD e = context->state[4];
  DWORD f = context->state[5];
  DWORD g = context->state[6];
  DWORD h = context->state[7];

  for( int i=0; i < 64; i++ ) {
    DWORD mxform_mix = h + BigSigma1(e) + ChooseBitwise(e,f,g) + CubeRootFractional[i] + block[i];
    DWORD state_mix = BigSigma0(a) + BitwiseMajority(a,b,c);
    h = g;
    g = f;
    f = e;
    e = d + mxform_mix;
    d = c;
    c = b;
    b = a;
    a = mxform_mix + state_mix;
  }

  context->state[0] += a;
  context->state[1] += b;
  context->state[2] += c;
  context->state[3] += d;
  context->state[4] += e;
  context->state[5] += f;
  context->state[6] += g;
  context->state[7] += h;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void cxlib_sha256_initialize( cxlib_sha256_context_t *context ) {
  static const DWORD INIT[8] = {
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
  };
  memcpy( context->state, INIT, sizeof(INIT) );
  context->wp = context->buffer;
  context->nbits = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void cxlib_sha256_update( cxlib_sha256_context_t *context, const BYTE data[], size_t sz ) {
  const BYTE *dp = data;
  const BYTE *dend = data + sz;
  BYTE *wp = context->wp;
  BYTE *end = context->buffer + 64;
  while( dp < dend ) {
    *wp++ = *dp++;
    if( wp < end ) {
      continue;
    }
    wp = context->buffer;
    context->nbits += 512;
    __transform( context );
  }
  context->wp = wp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
sha256_t * cxlib_sha256_finalize( cxlib_sha256_context_t *context ) {
  // Message length measured in bits
  context->nbits += (context->wp - context->buffer) * 8;
  uint64_t nbits = context->nbits;

  // Begin termination padding
  BYTE *B = context->buffer;
  BYTE *wp = context->wp;
  BYTE *final8 = B + 56;
  BYTE *end = B + 64;
  *wp++ = 0x80;

  // More than 8 unfilled bytes in buffer: pad up to 56 bytes (leave final 8)
  if( wp <= final8 ) {
    while( wp < final8 ) {
      *wp++ = 0x00;
    }
  }
  // Buffer does not have at least 8 unfilled bytes: pad to end, transform, then pad 56 bytes (leave final 8)
  else {
    while( wp < end ) {
      *wp++ = 0x00;
    }
    __transform( context );
    memset( B, 0, 56 );
  }

  // Final 8 bytes encode message length in bits
  final8[0] = (BYTE)((nbits >> 56) & 0xFF);
  final8[1] = (BYTE)((nbits >> 48) & 0xFF);
  final8[2] = (BYTE)((nbits >> 40) & 0xFF);
  final8[3] = (BYTE)((nbits >> 32) & 0xFF);
  final8[4] = (BYTE)((nbits >> 24) & 0xFF);
  final8[5] = (BYTE)((nbits >> 16) & 0xFF);
  final8[6] = (BYTE)((nbits >> 8) & 0xFF);
  final8[7] = (BYTE)(nbits & 0xFF);

  // Final transform
  __transform( context );

  // Little endian / Big endian
  DWORD *S = context->state;
  BYTE *H = (BYTE*)context->digest.str;

  for( int shift = 24; shift >= 0; shift -= 8 ) {
    H[0]  = (S[0] >> shift) & 0xFF;
    H[4]  = (S[1] >> shift) & 0xFF;
    H[8]  = (S[2] >> shift) & 0xFF;
    H[12] = (S[3] >> shift) & 0xFF;
    H[16] = (S[4] >> shift) & 0xFF;
    H[20] = (S[5] >> shift) & 0xFF;
    H[24] = (S[6] >> shift) & 0xFF;
    H[28] = (S[7] >> shift) & 0xFF;
    ++H; // add one byte offset for next iteration
  }

  return &context->digest;
}
