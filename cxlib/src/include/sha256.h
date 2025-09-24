/*********************************************************************
* Filename:   sha256.h
* Author:     Brad Conte (brad AT bradconte.com)
* Copyright:
* Disclaimer: This code is presented "as is" without any guarantees.
* Details:    Defines the API for the corresponding SHA1 implementation.
*********************************************************************/

#ifndef SHA256_H
#define SHA256_H

#include <stddef.h>

#define SHA256_BLOCK_SIZE 32            // SHA256 outputs a 32 byte digest

typedef struct {
  BYTE data[64];
  DWORD datalen;
  QWORD bitlen;
  DWORD state[8];
} SHA256_CTX;

void sha256_init( SHA256_CTX *ctx );
void sha256_update( SHA256_CTX *ctx, const BYTE data[], size_t len );
void sha256_final( SHA256_CTX *ctx, BYTE hash[] );

#endif


