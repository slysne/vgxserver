/*
###################################################
#
# File:   cxid.h
#
###################################################
*/
#ifndef CXLIB_CXID_H
#define CXLIB_CXID_H

#include "cxobjbase.h"
#include "cxmem.h"
#include "sha256.h"

#ifdef __cplusplus
extern "C" {
#endif



const objectid_t obid_from_string_len( const char *string, unsigned int len );
const objectid_t obid_from_string( const char *string );

const sha256_t sha256_len( const char *string, size_t len );
const sha256_t sha256( const char *string );

const objectid_t ihash128( const uint64_t x );
unsigned int crc32c( unsigned int crc, const char *data, int64_t sz );
int idcmp( const objectid_t *a, const objectid_t *b );
int idmatch( const objectid_t *a, const objectid_t *b );
int idnone( const objectid_t *id );
objectid_t strtoid( const char *str );
objectid_t smartstrtoid( const char *str, int len );
char * idtostr( char *buf, const objectid_t *objectid );
char * sha256tostr( char *buf, const sha256_t *hash );

char * write_HEX_qword( char *buf, QWORD qword );
char * write_hex_qword( char *buf, QWORD qword );
char * write_HEX_dword( char *buf, DWORD dword );
char * write_hex_dword( char *buf, DWORD dword );
char * write_HEX_word( char *buf, WORD word );
char * write_hex_word( char *buf, WORD word );
char * write_HEX_byte( char *buf, BYTE byte );
char * write_hex_byte( char *buf, BYTE byte );

char * write_decimal( char *buf, int64_t value );
char * write_double( char *buf, double value, int decimals );

char * write_bin_qword( char *buf, QWORD qword, const char *nibsep );
char * write_bin_dword( char *buf, DWORD dword, const char *nibsep );
char * write_bin_word( char *buf, WORD word, const char *nibsep );
char * write_bin_byte( char *buf, BYTE byte, const char *nibsep );

char * write_chars( char *buf, const char *data );
char * write_term( char *buf );

const char * hex_to_QWORD( const char *input, QWORD *value );
const char * hex_to_DWORD( const char *input, DWORD *value );
const char * hex_to_WORD( const char *input, WORD *value );
const char * hex_to_BYTE( const char *input, BYTE *value );

const char * decimal_to_integer( const char *input, int64_t *value );
const char * decimal_to_double( const char *input, double *value );


char * write_HEX_objectid( char *buf, const objectid_t *objectid );
char * write_hex_objectid( char *buf, const objectid_t *objectid );
int bytestohex( char **buffer, const unsigned char *bytes, int len );
int bytes_to_escaped_hex( char **buffer, const unsigned char *bytes, int len );
objectid_t * idset( objectid_t *dest, uint64_t H, uint64_t L );
objectid_t * idcpy( objectid_t *dest, const objectid_t *src );
objectid_t * idunset( objectid_t *dest );
objectid_t new_subid( void );

char * objectid_longstring_from_string( objectid_t *obid, const char *string );
void objectid_destroy_longstring( objectid_t *obid );

int decode_percent_plus_inplace( char *data );
int64_t requires_percent_encoding( const char *data_utf8, int64_t sz_data );
int64_t encode_percent_plus( const char *data_utf8, int64_t sz_data, char *encoding_buffer );
char * lower_inplace( char *ascii, int64_t sz_ascii );
char * upper_inplace( char *ascii, int64_t sz_ascii );


__inline static bool hex_digit_byte( char c, BYTE *x ) {
  if( (*x = c-48) < 10              // [0-9]
       ||
      ((*x = (c&0xDF)-55)-10) < 6 ) // [A-Fa-f]
  {
    return true;
  }
  return false;
}



#ifdef __cplusplus
}
#endif

#endif

