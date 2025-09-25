/*
 * cxid.c
 *
 *
*/

#include "cxid.h"


typedef union {
  uint64_t value;
  unsigned char bytes[8];
} bytes64_t;


typedef union {
  uint32_t value;
  unsigned char bytes[4];
} bytes32_t;


typedef union {
  uint16_t value;
  unsigned char bytes[2];
} bytes16_t;



/*******************************************************************//**
 *
 * Return object ID as digest of string of given length
 *
 * NOTE: Neither QWORD will ever be zero
 ***********************************************************************
 */
const objectid_t obid_from_string_len( const char *string, unsigned int len ) {
  objectid_t obid = hash128( (unsigned char*)string, len );
  obid.H |= -(obid.H == 0);
  obid.L |= -(obid.L == 0);
  return obid;
}



/*******************************************************************//**
 *
 * Return object ID as digest of string
 *
 ***********************************************************************
 */
const objectid_t obid_from_string( const char *string ) {
  return obid_from_string_len( string, (int)strlen( string ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
const sha256_t sha256_len( const char *string, size_t len ) {
  cxlib_sha256_context_t CALIGNED_ context;
  cxlib_sha256_initialize( &context );
  cxlib_sha256_update( &context, (BYTE*)string, len );
  sha256_t *phash = cxlib_sha256_finalize( &context );
  sha256_t hash = *phash;
  return hash;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
const sha256_t sha256( const char *string ) {
  return sha256_len( string, strlen( string ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
const objectid_t ihash128( const uint64_t x ) {
  return obid_from_string_len( (char*)&x, (int)sizeof( uint64_t ) );
}



/*******************************************************************//**
 * NOTE: On x64 computes CRC32-C 
 *       On ARM64 Computes CRC32
 ***********************************************************************
 */
unsigned int crc32c( unsigned int crc, const char *data, int64_t sz ) {
#define chunksize sizeof( uint64_t )

  const char *cursor = data;
  const char *end = cursor + sz;
  const char *end_head = (char*)ceilmultpow2( (uintptr_t)cursor, chunksize );
  const char *end_main = (char*)floormultpow2( (uintptr_t)end, chunksize );

  crc ^= 0xFFFFFFFF;

  // End of head cannot be beyond actual end
  if( end_head > end ) {
    end_head = end;
  }

  // First consume data until cursor is aligned on 64-bit boundary
  while( cursor < end_head ) {
    #if defined CXPLAT_ARCH_X64
    crc = _mm_crc32_u8( crc, *cursor++ );
    #elif defined CXPLAT_ARCH_ARM64
    crc = __crc32cb( crc, *cursor++ );
    #endif
  }

  // Consume data in 64-bit chunks as long as possible
  while( cursor < end_main ) {
    #if defined CXPLAT_ARCH_X64
    crc = (unsigned int)_mm_crc32_u64( crc, *(uint64_t*)cursor );
    #elif defined CXPLAT_ARCH_ARM64
    crc = __crc32cd( crc, *(uint64_t*)cursor );
    #endif
    cursor += chunksize;
  }

  // Finally consume any trailing bytes
  while( cursor < end ) {
    #if defined CXPLAT_ARCH_X64
    crc = _mm_crc32_u8( crc, *cursor++ );
    #elif defined CXPLAT_ARCH_ARM64
    crc = __crc32cb( crc, *cursor++ );
    #endif
  }

  crc ^= 0xFFFFFFFF;

  return crc;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int idcmp( const objectid_t *a, const objectid_t *b ) {
  if( a->H == b->H )
    return (a->L > b->L) - (a->L < b->L);
  else
    return (a->H > b->H) - (a->H < b->H);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int idmatch( const objectid_t *a, const objectid_t *b ) {
  return a->L == b->L && a->H == b->H;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t * idset( objectid_t *dest, uint64_t H, uint64_t L ) {
  dest->L = L;
  dest->H = H;
  return dest;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t * idcpy( objectid_t *dest, const objectid_t *src ) {
  dest->L = src->L;
  dest->H = src->H;
  return dest;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t * idunset( objectid_t *dest ) {
  if( dest ) {
    dest->L = 0;
    dest->H = 0;
  }
  return dest;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
int idnone( const objectid_t *id ) {
  if( id->H == 0 && id->L == 0 ) {
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t strtoid( const char *str ) {
  objectid_t id = {.L=0, .H=0};
  QWORD A[2] = {0,0};
  char c;
  BYTE v;
  for( int i=0; i<2; i++ ) {
    for( int b=60; b>=0; b -= 4 ) {
      if( (c = *(str++)) == '\0' ) {
        return id;
      }
      if( hex_digit_byte( c, &v ) ) {
        A[i] += (QWORD)v << b;
      }
      else {
        return id;
      }
    }
  }
  id.H = A[0];
  id.L = A[1];
  return id;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t smartstrtoid( const char *str, int len ) {
  const char *p = str;
  objectid_t id = {.L=0, .H=0};
  if( str ) {
    // Try to parse the string as a string of 32 hex digits unless length indicates otherwise
    if( len == 32 || len < 0 ) {
      uint64_t A[2] = {0,0};
      for( int i=0; i<2; i++ ) {
        uint64_t x = 0;
        char c;
        BYTE v;
        for( int b=60; b>=0; b -= 4 ) {
          if( (c = *p) == '\0' ) {
            goto compute;  // early termination, str is too short to represent a 128-bit hash
          }
          if( hex_digit_byte( c, &v ) ) {
            x += (uint64_t)v << b;
          }
          else {
            goto compute; // not a hex digit, str does not represent a 128-bit hash
          }
          ++p;
        }
        A[i] = x;
      }
      if( *p != '\0' ) {
        goto compute;  // str too long to represent a 128-bit hash
      }
      // If we make it all the way here we have built the objectid from a string of 32 hex characters
      id.H = A[0];
      id.L = A[1];
      return id;
    }

compute:
    // If we end up here we have to compute the hash of the string
    {
      int sz = len;
      // Length was not specified, we have to compute it
      if( sz < 0 ) {
        // Start counting from position already scanned to earlier during hex parsing
        sz = (int)(p - str);
        while( *(p++) != '\0' ) { // we need to find the length first
          ++sz;
        }
      }

      // Compute digest of string
      return obid_from_string_len( str, sz );
    }
  }
  else {
    return id; // zero
  }
}



static char hexdata[] =  "000102030405060708090a0b0c0d0e0f"
                         "101112131415161718191a1b1c1d1e1f"
                         "202122232425262728292a2b2c2d2e2f"
                         "303132333435363738393a3b3c3d3e3f"
                         "404142434445464748494a4b4c4d4e4f"
                         "505152535455565758595a5b5c5d5e5f"
                         "606162636465666768696a6b6c6d6e6f"
                         "707172737475767778797a7b7c7d7e7f"
                         "808182838485868788898a8b8c8d8e8f"
                         "909192939495969798999a9b9c9d9e9f"
                         "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
                         "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
                         "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
                         "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
                         "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
                         "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

static char HEXdata[] =  "000102030405060708090A0B0C0D0E0F"
                         "101112131415161718191A1B1C1D1E1F"
                         "202122232425262728292A2B2C2D2E2F"
                         "303132333435363738393A3B3C3D3E3F"
                         "404142434445464748494A4B4C4D4E4F"
                         "505152535455565758595A5B5C5D5E5F"
                         "606162636465666768696A6B6C6D6E6F"
                         "707172737475767778797A7B7C7D7E7F"
                         "808182838485868788898A8B8C8D8E8F"
                         "909192939495969798999A9B9C9D9E9F"
                         "A0A1A2A3A4A5A6A7A8A9AAABACADAEAF"
                         "B0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
                         "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECF"
                         "D0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
                         "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEF"
                         "F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";


/*******************************************************************//**
 *
 ***********************************************************************
 */
char * idtostr( char *buf, const objectid_t *objectid ) {
  /*
  Custom implementation is about 3x faster than:
    snprintf( buf, 33, "%016llx%016llx", objectid->H, objectid->L );
  */
  bytes64_t high, low;
  char *cp = buf, *h;
  int i = 8;
  high.value = objectid->H;
  low.value = objectid->L;
  while( i > 0 ) {
    h = hexdata + ((uintptr_t)(high.bytes[--i]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  i = 8;
  while( i > 0 ) {
    h = hexdata + ((uintptr_t)(low.bytes[--i]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  *cp = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * sha256tostr( char *buf, const sha256_t *hash ) {
  char *cp = buf, *h;
  bytes64_t A = { .value = hash->A };
  bytes64_t B = { .value = hash->B };
  bytes64_t C = { .value = hash->C };
  bytes64_t D = { .value = hash->D };
  int i;

  // A
  i = 0;
  while( i < 8 ) {
    h = hexdata + ((uintptr_t)(A.bytes[i++]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  // B
  i = 0;
  while( i < 8 ) {
    h = hexdata + ((uintptr_t)(B.bytes[i++]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  // C
  i = 0;
  while( i < 8 ) {
    h = hexdata + ((uintptr_t)(C.bytes[i++]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  // D
  i = 0;
  while( i < 8 ) {
    h = hexdata + ((uintptr_t)(D.bytes[i++]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }

  *cp = '\0';
  return buf;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_HEX_qword( char *buf, QWORD qword ) {
  bytes64_t n = { .value = qword };
  char *cp = buf, *h;
  int i = 8;
  while( i > 0 ) {
    h = HEXdata + ((uintptr_t)(n.bytes[--i]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_hex_qword( char *buf, QWORD qword ) {
  bytes64_t n = { .value = qword };
  char *cp = buf, *h;
  int i = 8;
  while( i > 0 ) {
    h = hexdata + ((uintptr_t)(n.bytes[--i]) << 1);
    *cp++ = *h++;
    *cp++ = *h;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_HEX_dword( char *buf, DWORD dword ) {
  bytes32_t n = { .value = dword };
  char *cp = buf, *h;

  h = HEXdata + ((uintptr_t)(n.bytes[3]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = HEXdata + ((uintptr_t)(n.bytes[2]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = HEXdata + ((uintptr_t)(n.bytes[1]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = HEXdata + ((uintptr_t)(n.bytes[0]) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_hex_dword( char *buf, DWORD dword ) {
  bytes32_t n = { .value = dword };
  char *cp = buf, *h;

  h = hexdata + ((uintptr_t)(n.bytes[3]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = hexdata + ((uintptr_t)(n.bytes[2]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = hexdata + ((uintptr_t)(n.bytes[1]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = hexdata + ((uintptr_t)(n.bytes[0]) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_HEX_word( char *buf, WORD word ) {
  bytes16_t n = { .value = word };
  char *cp = buf, *h;
  
  h = HEXdata + ((uintptr_t)(n.bytes[1]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = HEXdata + ((uintptr_t)(n.bytes[0]) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_hex_word( char *buf, WORD word ) {
  bytes16_t n = { .value = word };
  char *cp = buf, *h;
  
  h = hexdata + ((uintptr_t)(n.bytes[1]) << 1);
  *cp++ = *h++;
  *cp++ = *h;
  h = hexdata + ((uintptr_t)(n.bytes[0]) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_HEX_byte( char *buf, BYTE byte ) {
  char *cp = buf, *h;
  
  h = HEXdata + ((uintptr_t)(byte) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_hex_byte( char *buf, BYTE byte ) {
  char *cp = buf, *h;
  
  h = hexdata + ((uintptr_t)(byte) << 1);
  *cp++ = *h++;
  *cp++ = *h;

  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_decimal( char *buf, int64_t value ) {
  char dec[20]; // max digits for int64
  snprintf( dec, 20, "%lld", value );
  const char *src = dec;
  char *cp = buf;
  // write digits only (no terminator)
  while( *src != '\0' ) {
    *cp++ = *src++;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_double( char *buf, double value, int decimals ) {
  char tmp[32];
  const char fmt[] = "%.0f";
  if( decimals > 0 && decimals <= 9 ) {
    *(char*)(fmt + 2) = (char)('0' + decimals);
  }
  SUPPRESS_WARNING_FORMAT_STRING_NOT_LITERAL
  snprintf( tmp, 32, fmt, value );
  const char *src = tmp;
  char *cp = buf;
  while( *src ) {
    *cp++ = *src++;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static char bindata[] = "0000" "0001" "0010" "0011"
                        "0100" "0101" "0110" "0111"
                        "1000" "1001" "1010" "1011"
                        "1100" "1101" "1110" "1111";
#define __WBIT( Bit, CP ) *(CP)++ = *(Bit)++
#define __WNIB( Bit, CP ) __WBIT( Bit, CP ); __WBIT( Bit, CP ); __WBIT( Bit, CP ); __WBIT( Bit, CP )
#define __HNIB( Byte, CP ) do { char *b = &bindata[ ((Byte) >> 4) << 2 ]; __WNIB( b, CP ); } WHILE_ZERO
#define __LNIB( Byte, CP ) do { char *b = &bindata[ ((Byte) & 0xF) << 2 ]; __WNIB( b, CP ); } WHILE_ZERO
#define __NIBSEP( Nibsep, CP ) do { if( Nibsep ) { *(CP)++ = *(Nibsep); } } WHILE_ZERO



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_bin_qword( char *buf, QWORD qword, const char *nibsep ) {
  bytes64_t n = { .value = qword };
  char *cp = buf;
  for( int i=7; i>=0; i-- ) {
    if( cp > buf ) {
      __NIBSEP( nibsep, cp );
    }
    __HNIB( n.bytes[i], cp );
    __NIBSEP( nibsep, cp );
    __LNIB( n.bytes[i], cp );
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_bin_dword( char *buf, DWORD dword, const char *nibsep ) {
  bytes32_t n = { .value = dword };
  char *cp = buf;
  for( int i=3; i>=0; i-- ) {
    if( cp > buf ) {
      __NIBSEP( nibsep, cp );
    }
    __HNIB( n.bytes[i], cp );
    __NIBSEP( nibsep, cp );
    __LNIB( n.bytes[i], cp );
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_bin_word( char *buf, WORD word, const char *nibsep ) {
  bytes16_t n = { .value = word };
  char *cp = buf;
  for( int i=1; i>=0; i-- ) {
    if( cp > buf ) {
      __NIBSEP( nibsep, cp );
    }
    __HNIB( n.bytes[i], cp );
    __NIBSEP( nibsep, cp );
    __LNIB( n.bytes[i], cp );
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_bin_byte( char *buf, BYTE byte, const char *nibsep ) {
  char *cp = buf;
  __HNIB( byte, cp );
  __NIBSEP( nibsep, cp );
  __LNIB( byte, cp );
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_chars( char *buf, const char *data ) {
  char *cp = buf;
  const char *rp = data;
  while( *rp != '\0' ) {
    *cp++ = *rp++;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_term( char *buf ) {
  char *cp = buf;
  *cp++ = '\0';
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define HEX_TO_VALUE( Type )                                    \
const char * hex_to_##Type( const char *input, Type *value ) {  \
  Type nib;                                                     \
  Type hex;                                                     \
  Type acc = 0;                                                 \
  const char *p = input;                                        \
  for( int shift=(int)(8*sizeof( Type )-4); shift>=0; shift -= 4 ) { \
    if( (hex = *(p++)) == '\0' ) {                              \
      return NULL;                                              \
    }                                                           \
    if( (nib = hex-48) < 10 || ((nib = hex-87)-10) < 6 || ((nib = hex-55)-10) < 6 ) { /* nib = 0-15 */  \
      acc += nib << shift;                                      \
    }                                                           \
    else {                                                      \
      return NULL;                                              \
    }                                                           \
  }                                                             \
  *value = acc;                                                 \
  return p;                                                     \
}


HEX_TO_VALUE( QWORD )
HEX_TO_VALUE( DWORD )
HEX_TO_VALUE( WORD )
HEX_TO_VALUE( BYTE )



/*******************************************************************//**
 *
 ***********************************************************************
 */
const char * decimal_to_integer( const char *input, int64_t *value ) {
  int64_t v = 0;
  int64_t s = 1;
  if( *input == '-' ) {
    s = -1;
    ++input;
  }
  while( *input > 32 ) {
    BYTE d = *input++ - 48; // unsigned, so negative becomes very positive and we can do a single check below
    if( d > 9 ) {
      return NULL; // not a proper decimal integer
    }
    v *= 10;
    v += d;
  }
  *value = s * v;
  return input;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
const char * decimal_to_double( const char *input, double *value ) {
  return NULL;
  // TODO: IMPLEMENT
}




/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_HEX_objectid( char *buf, const objectid_t *objectid ) {
  return write_HEX_qword( write_HEX_qword( buf, objectid->H ), objectid->L );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * write_hex_objectid( char *buf, const objectid_t *objectid ) {
  return write_hex_qword( write_hex_qword( buf, objectid->H ), objectid->L );
}



/*******************************************************************//**
 *
 *
 * WARNING: If *buffer == NULL it will be auto-allocated and caller
 *          becomes owner!
 *
 * Returns : Number of hex digits (bytes) written to output buffer.
 ***********************************************************************
 */
int bytestohex( char **buffer, const unsigned char *bytes, int len ) {
  int szhex = len * 2;
  if( *buffer == NULL ) {
    if( (*buffer = malloc( (size_t)szhex + 1 )) == NULL ) {
      return -1;
    }
  }

  const unsigned char *src = bytes;
  const unsigned char *end = src + len;
  char *dest = *buffer;
  char *h;
  while( src < end ) {
    h = hexdata + ((uintptr_t)(*src++) << 1);
    *dest++ = *h++;
    *dest++ = *h;
  }
  *dest = '\0';

  return szhex;
}



/*******************************************************************//**
 *
 *
 * WARNING: If *buffer == NULL it will be auto-allocated and caller
 *          becomes owner!
 *
 * Returns : Number of hex digits (bytes) written to output buffer.
 ***********************************************************************
 */
int bytes_to_escaped_hex( char **buffer, const unsigned char *bytes, int len ) {
  int szhex = len * 4;
  if( *buffer == NULL ) {
    if( (*buffer = malloc( (size_t)szhex + 1 )) == NULL ) {
      return -1;
    }
  }

  const unsigned char *src = bytes;
  const unsigned char *end = src + len;
  char *dest = *buffer;
  char *h;
  while( src < end ) {
    h = hexdata + ((uintptr_t)(*src++) << 1);
    *dest++ = '\\';
    *dest++ = 'x';
    *dest++ = *h++;
    *dest++ = *h;
  }
  *dest = '\0';

  return szhex;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
char * objectid_longstring_from_string( objectid_t *obid, const char *string ) {
  size_t len = strnlen( string, OBJECTID_LONGSTRING_MAX );
  idunset( obid );
  if( len == 0 ) return NULL;
  if( (obid->longstring.string = (char*)malloc( len + 1 )) == NULL ) return NULL;
  strncpy( obid->longstring.prefix, string, len < 8 ? len : 8 );
  return strncpy( obid->longstring.string, string, len + 1 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
void objectid_destroy_longstring( objectid_t *obid ) {
  free( obid->longstring.string );
  obid->longstring.string = NULL;
  *(QWORD*)obid->longstring.prefix = 0; // all 8 bytes to '\0'
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
objectid_t new_subid( void ) {
  static char buf[33];
  static uint64_t counter = 0;
  static CS_LOCK lock;
  objectid_t subid;

  if( counter == 0 ) {
    // NOTE: we do this to get a unique starting point in case multiple copies of the library are in use
    // which might be the case with static linkage. TODO: find a way to avoid.
    INIT_CRITICAL_SECTION( &lock.lock );
    counter = hash64( (unsigned char*)&counter, 8 ); // use counter's address as basis for its initial value
  }

  time( (time_t*)(&subid.L) );
  SYNCHRONIZE_ON( lock ) {
    subid.H = counter;
    counter = hash64( (unsigned char*)counter, 8 );
  } RELEASE;
  return obid_from_string_len( idtostr( buf, &subid ), 32 );
}



/*******************************************************************//**
 *
 *
 * %E3%81%93%E3%82%8C%E3%81%AF%E6%A5%BD%E3%81%97%E3%81%84%E3%81%A7%E3%81%99
 * 
 * 
 ***********************************************************************
 */
int decode_percent_plus_inplace( char *data ) {
  static const char lookup[] = {
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,  0, 1, 2, 3, 4, 5, 6, 7,  8, 9,-1,-1,-1,-1,-1,-1,
    -1,10,11,12,13,14,15,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,10,11,12,13,14,15,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1, -1,-1,-1,-1,-1,-1,-1,-1
  };

  if( data ) {
    const char *p = data;
    char *dest = data;
    char H, L;

    while( (*dest = *p++) != '\0' ) {
      if( *dest == '+' ) {
        *dest = ' ';
      }
      else if( *dest == '%' ) {
        // Expect percent encoding
        if( (H = lookup[(BYTE)*p++]) < 0 ||
            (L = lookup[(BYTE)*p++]) < 0 )
        {
          return -1;
        }
        *dest = (H<<4) + L;
      }
      ++dest;
    }
    *dest = '\0';
  }
  return 0;
}



static const char __pct_enc_lookup[] = {
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0, '-', '.', '/',
  '0', '1', '2', '3', '4', '5', '6', '7',
  '8', '9',   0,   0,   0,   0,   0,   0,
    0, 'A', 'B', 'C', 'D', 'E', 'F', 'G',
  'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
  'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
  'X', 'Y', 'Z',   0,   0,   0,   0, '_',
    0, 'a', 'b', 'c', 'd', 'e', 'f', 'g',
  'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
  'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
  'x', 'y', 'z',   0,   0,   0, '~',   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0
};



/*******************************************************************//**
 *
 *
 * 
 * 
 ***********************************************************************
 */
int64_t requires_percent_encoding( const char *data_utf8, int64_t sz_data ) {
  const char *rp = data_utf8;
  const char *end = data_utf8 + sz_data;
  while( rp < end ) {
    if( __pct_enc_lookup[(BYTE)*rp++] == 0 ) {
      goto encoding_required;
    }
  }
  return 0; // no encoding required

encoding_required:
  --rp;
  // Return worst case buffer size needed to encode
  return (rp - data_utf8) + 3 * (end - rp);
}



/*******************************************************************//**
 *
 *
 * 
 * 
 ***********************************************************************
 */
int64_t encode_percent_plus( const char *data_utf8, int64_t sz_data, char *encoding_buffer ) {
  const char *rp = data_utf8;
  const char *end = data_utf8 + sz_data;
  char *wp = encoding_buffer;
  while( rp < end ) {
    char c = *rp++;
    // Use character as-is
    if( __pct_enc_lookup[(BYTE)c] != 0 ) {
      *wp++ = c;
    }
    // Convert space to +
    else if( c == ' ') {
      *wp++ = '+';
    }
    // Percent encode everything else
    else {
      BYTE b = (BYTE)c;
      char *h = HEXdata + ((uintptr_t)(b) << 1);
      *wp++ = '%';
      *wp++ = *h++;
      *wp++ = *h;
    }
  }
  *wp = '\0';

  // Number of bytes written to buffer
  return (wp - encoding_buffer);
}



/*******************************************************************//**
 *
 *
 * 
 * 
 ***********************************************************************
 */
char * lower_inplace( char *ascii, int64_t sz_ascii ) {
  static const char upper_to_lower[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
    0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
    0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
    0x40,  'a',  'b',  'c',  'd',  'e',  'f',  'g',
     'h',  'i',  'j',  'k',  'l',  'm',  'n',  'o',
     'p',  'q',  'r',  's',  't',  'u',  'v',  'w',
     'x',  'y',  'z', 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,
    0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67,
    0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F,
    0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77,
    0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
    0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,
    0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F,
    0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
    0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
    0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7,
    0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
    0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7,
    0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF,
    0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7,
    0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,
    0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7,
    0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7,
    0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF
  };

  const char *end = ascii + sz_ascii;
  for( char *p = ascii; p < end; ++p ) {
    *p = upper_to_lower[*p];
  }
  return ascii;
}



/*******************************************************************//**
 *
 *
 * 
 * 
 ***********************************************************************
 */
char * upper_inplace( char *ascii, int64_t sz_ascii ) {
  static const char lower_to_upper[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
    0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
    0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
    0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57,
    0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,
    0x60,  'A',  'B',  'C',  'D',  'E',  'F',  'G',
     'H',  'I',  'J',  'K',  'L',  'M',  'N',  'O',
     'P',  'Q',  'R',  'S',  'T',  'U',  'V',  'W',
     'X',  'Y',  'Z', 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
    0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,
    0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F,
    0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
    0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
    0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7,
    0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
    0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7,
    0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF,
    0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7,
    0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,
    0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7,
    0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7,
    0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF
  };

  const char *end = ascii + sz_ascii;
  for( char *p = ascii; p < end; ++p ) {
    *p = lower_to_upper[*p];
  }
  return ascii;
}


