/*######################################################################
 *#
 *# sha256.h
 *#
 *#
 *######################################################################
 */

#ifndef CXLIB_SHA256_H
#define CXLIB_SHA256_H

#include "cxmem.h"



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union u_sha256_t {
  #if defined CXPLAT_ARCH_X64
  __m256i id256;
  #elif defined CXPLAT_ARCH_ARM64
  QWORD id256[4];
  #endif
  struct {
    uint64_t A;
    uint64_t B;
    uint64_t C;
    uint64_t D;
  };
  char str[32];
} sha256_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_cxlib_sha256_context_t {
  BYTE buffer[64];
  DWORD state[8];
  BYTE *wp;
  uint64_t nbits;
  QWORD _rsv2[6];
  sha256_t digest;
} cxlib_sha256_context_t;


void cxlib_sha256_initialize( cxlib_sha256_context_t *context );
void cxlib_sha256_update( cxlib_sha256_context_t *context, const BYTE data[], size_t sz );
sha256_t * cxlib_sha256_finalize( cxlib_sha256_context_t *context );


#endif