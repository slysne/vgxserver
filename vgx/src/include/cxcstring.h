/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxcstring.h
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

#ifndef COMLIB_CXCSTRING_H
#define COMLIB_CXCSTRING_H


#include "objectmodel.h"


struct s_CString_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_CString_compress_mode {
  CSTRING_COMPRESS_MODE_NONE  = 0x00,
  CSTRNIG_COMPRESS_MODE_ZLIB  = 0x01,
  CSTRING_COMPRESS_MODE_LZ4   = 0x02,
  CSTRING_COMPRESS_MODE_LZ4HC = 0x03
} CString_compress_mode;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_CString_attr {
  CSTRING_ATTR_NONE           = 0x00,
  CSTRING_ATTR_SERIALIZED_TXT = 0x01, // cstring represents serialized object when set (text mode)
  CSTRING_ATTR_COMPRESSED     = 0x02, // cstring is compressed
  CSTRING_ATTR_BYTES          = 0x04, // cstring represents raw data
  CSTRING_ATTR_BYTEARRAY      = 0x08, // cstring represents a bytearray
  CSTRING_ATTR_KEYVAL         = 0x10, // cstring represents a supported object wrapped in tuple (key, object)
  CSTRING_ATTR_CALLABLE       = 0x20, // cstring represents a callable object with default parameters and annotations
  CSTRING_ATTR_ARRAY_INT      = 0x40, // cstring represents an array of integers
  CSTRING_ATTR_ARRAY_FLOAT    = 0x80, // cstring represents an array of floats 
  CSTRING_ATTR_ARRAY_MAP      = 0xC0, // cstring represents a hash table mapping int32 -> f32
  __CSTRING_ATTR_ARRAY_MASK   = 0xC0 
} CString_attr;



#define CSTRING_MIN_COMPRESS_SZ 512 

#define CSTRING_COMPRESS_MAGIC 0xab8a9b7f73cd0311



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef union u_CString_compress_header_t {
  QWORD bits[4];
  struct {
    QWORD magic;
    struct {
      CString_compress_mode mode;
      int sz;
      int ucsz;
    } orig;
    struct {
      CString_compress_mode mode;
      int sz;
      int _rsv;
    } compressed;
  };
} CString_compress_header_t;



/******************************************************************************
 * CString_t
 *
 ******************************************************************************
 */
typedef struct s_CString_vtable_t {
  // vtable head
  COMLIB_VTABLE_HEAD

  //
  int32_t (*Length)( const struct s_CString_t *CSTR__self );
  int32_t (*Codepoints)( const struct s_CString_t *CSTR__self );
  void (*GetTrueLength)( const struct s_CString_t *CSTR__self, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );
  const char * (*Value)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*Ascii)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*B16Encode)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*B16Decode)( const struct s_CString_t *CSTR__self );
  int64_t (*AsInteger)( const struct s_CString_t *CSTR__self );
  double (*AsReal)( const struct s_CString_t *CSTR__self );
  QWORD * (*ModifiableQwords)( struct s_CString_t *CSTR__self );
  int (*ToBytes)( const struct s_CString_t *CSTR__self, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );
  int (*Compare)( const struct s_CString_t *CSTR__self, const struct s_CString_t *other );
  int (*CompareChars)( const struct s_CString_t *CSTR__self, const char *other );
  bool (*Equals)( const struct s_CString_t *CSTR__self, const struct s_CString_t *other );
  bool (*EqualsChars)( const struct s_CString_t *CSTR__self, const char *other );
  int32_t (*Find)( const struct s_CString_t *CSTR__self, const char *probe, int32_t startindex );
  bool (*Contains)( const struct s_CString_t *CSTR__self, const char *probe );

  struct s_CString_t * (*Slice)( const struct s_CString_t *CSTR__self, int32_t *p_start, int32_t *p_end );
  struct s_CString_t * (*SliceAlloc)( const struct s_CString_t *CSTR__self, int32_t *p_start, int32_t *p_end, object_allocator_context_t *alloc );

  bool (*StartsWith)( const struct s_CString_t *CSTR__self, const char *probe );
  bool (*EndsWith)( const struct s_CString_t *CSTR__self, const char *probe );
  struct s_CString_t * (*Replace)( const struct s_CString_t *CSTR__self, const char *probe, const char *subst );
  struct s_CString_t * (*ReplaceAlloc)( const struct s_CString_t *CSTR__self, const char *probe, const char *subst, object_allocator_context_t *alloc );
  struct s_CString_t ** (*Split)( const struct s_CString_t *CSTR__self, const char *splitstr, int32_t *sz );
  struct s_CString_t * (*Join)( const struct s_CString_t *CSTR__self, const struct s_CString_t **CSTR__list );
  struct s_CString_t * (*JoinAlloc)( const struct s_CString_t *CSTR__self, const struct s_CString_t **CSTR__list, object_allocator_context_t *alloc );
  const objectid_t * (*Obid)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*Clone)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*CloneAlloc)( const struct s_CString_t *CSTR__self, object_allocator_context_t *alloc );
  struct s_CString_t * (*Compress)( const struct s_CString_t *CSTR__self, CString_compress_mode mode );
  struct s_CString_t * (*CompressAlloc)( const struct s_CString_t *CSTR__self, object_allocator_context_t *alloc, CString_compress_mode mode );
  struct s_CString_t * (*Decompress)( const struct s_CString_t *CSTR__self );
  int (*DecompressToBytes)( const struct s_CString_t *CSTR__self, char *buffer, int32_t bsz, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );
  bool (*IsCompressed)( const struct s_CString_t *CSTR__self );
  CString_compress_header_t * (*CompressedHeader)( const struct s_CString_t *CSTR__self );
  struct s_CString_t * (*PrefixAlloc)( const struct s_CString_t *CSTR__self, int32_t sz, object_allocator_context_t *alloc );
  struct s_CString_t * (*Prefix)( const struct s_CString_t *CSTR__self, int32_t sz );
  Unicode (*DecodeUTF8)( const struct s_CString_t *CSTR__self );
  void (*Print)( const struct s_CString_t *CSTR__self );
} CString_vtable_t;




typedef union u_CString_flags_t {
  DWORD _flags;
  struct {
    struct {
      struct {
        uint16_t init    : 1;    // true when initialized
        uint16_t shrt    : 1;    // true when string value exists in the short area
        uint16_t _rsv2   : 1;    //
        uint16_t _rsv3   : 1;    //
        uint16_t ucsz    : 12;   // Unicode Size (number of code points if > 0)
      };
    } priv;
    struct {
      struct {
        uint8_t user0   : 1;
        uint8_t user1   : 1;
        uint8_t user2   : 1;
        uint8_t user3   : 1;
        uint8_t user4   : 1;
        uint8_t user5   : 1;
        uint8_t user6   : 1;
        uint8_t user7   : 1;
      };
      uint8_t data8;
    } user;
  } state;
} CString_flags_t;



typedef union u_CString_meta_t {
  QWORD _bits;
  struct {
    const int32_t size;     // 0.5Q   string length
    CString_flags_t flags;  // 0.5Q
  };
} CString_meta_t;






/******************************************************************************
 * CString_t
 *
 ******************************************************************************
 */
typedef struct s_CString_t {
  COMLIB_OBJECT_HEAD( CString_vtable_t )      // 2Q
  union {
    __m128i objmetas;
    struct {
      CString_meta_t meta;                            // 1Q
      object_allocator_context_t *allocator_context;  // 1Q
    };
  };
} CString_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_CString_constructor_args_t {
  const char *string;                 // immutable string value - if NULL use format
  int32_t len;                        // Length, if known, or negative to have constructor compute length
  int ucsz;                           // Unicode codepoints
  const char *format;                 // format string to use if string is NULL
  va_list *format_args;               // args to use with format string
  object_allocator_context_t *alloc;  // when non-NULL, use this allocator instead of malloc
} CString_constructor_args_t;

typedef CString_t * (*f_CString_constructor_t)( const void *identifier, CString_constructor_args_t *args );

typedef int64_t (*f_CString_serializer_t)( const CString_t *CSTR__self, CQwordQueue_t *output );
typedef CString_t * (*f_CString_deserializer_t)( comlib_object_t *container, CQwordQueue_t *input );


#define __CSTRING_VALUE_FROM_OBJECT( ObjPtr ) ((char*)(ObjPtr) + sizeof(CString_t))
#define __CSTRING_SHORT_VALUE_FROM_OBJECT( ObjPtr ) ((char*)(ObjPtr) - 16 )
#define __CSTRING_MAX_SHORT_STRING_QWORDS qwsizeof( __m128i )
#define __CSTRING_MAX_SHORT_STRING_BYTES (int32_t)sizeof( __m128i )


#define __CStringReset( CString, Size ) \
  do { \
    *((int32_t*)&(CString)->meta.size) = (int32_t)(Size); \
    (CString)->meta.flags.state.priv.init = 0; \
    (CString)->meta.flags.state.priv.shrt = 0; \
  } WHILE_ZERO



#define __CStringInitComplete( CString, Size ) \
  do { \
    *((int32_t*)&(CString)->meta.size) = (int32_t)(Size); \
    (CString)->meta.flags.state.priv.init = 1; \
    (CString)->meta.flags.state.priv.shrt = ((CString)->allocator_context == NULL && (Size) < __CSTRING_MAX_SHORT_STRING_BYTES); \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * CStringValue( const CString_t *CSTR__str ) {
  return CSTR__str->meta.flags.state.priv.shrt ? __CSTRING_SHORT_VALUE_FROM_OBJECT( CSTR__str ) : __CSTRING_VALUE_FROM_OBJECT( CSTR__str );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static QWORD CStringReinterpretAsQWORD( const CString_t *CSTR__str ) {
  // Little endian
  QWORD qw = *(QWORD*)CStringValue( CSTR__str );
  int64_t shift = 64 - 8 * (int64_t)CSTR__str->meta.size;
  if( shift > 0 ) {
    qw <<= shift;
  }
  uint64_t val = _byteswap_uint64( qw );
  return val;
}



void CString_RegisterClass( void );
void CString_UnregisterClass( void );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __cxcstring_chars_occ_const_within( const char *str1, const char *str2, size_t n ) {
  const char *p;
  return (p = strstr( str1, str2 )) != NULL && p <= str1+n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __cxcstring_chars_ends_with_const( const char *str1, const char *str2 ) {
  size_t sz1 = strlen(str1);
  size_t sz2 = strlen(str2);
  if( sz2 <= sz1 ) {
    size_t pos = sz1 - sz2;
    return strcmp( str1 + pos, str2 ) == 0;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __cxcstring_chars_new( const char *str ) {
  char *new_str = NULL;
  if( str ) {
    size_t sz = strlen( str ) + 1;
    if( (new_str = (char*)malloc( sz )) != NULL ) {
      memcpy( new_str, str, sz );
    }
  }
  return new_str;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __cxcstring_cstring_convert_to_chars( CString_t **CSTR__string ) {
  int32_t sz = (*CSTR__string)->meta.size + 1;
  char *str = (char*)malloc( sz );
  if( str ) {
    char *dest = str;
    const char *src = CStringValue( *CSTR__string );
    const char *end = src + sz;
    while( src < end ) {
      *dest++ = *src++;
    }
    COMLIB_OBJECT_DESTROY( *CSTR__string );
    *CSTR__string = NULL;
  }
  return str;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __cxcstring_cstring_delete_list( CString_t ***CSTR__plist ) {
  if( CSTR__plist && *CSTR__plist ) {
    CString_t **CSTR__cursor = *CSTR__plist;
    CString_t *CSTR__str;
    while( (CSTR__str = *CSTR__cursor++) != NULL ) {
      COMLIB_OBJECT_DESTROY( CSTR__str );
    }
    free( *CSTR__plist );
    *CSTR__plist = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __cxcstring_chars_probably_compressed( const char *data, int sz ) {
  CString_compress_header_t *compressed_header = (CString_compress_header_t*)data;
  return sz > (int)sizeof( CString_compress_header_t ) && compressed_header->magic == CSTRING_COMPRESS_MAGIC;
}



#define CStringUserFlag0( CString )             ( CString )->meta.flags.state.user.user0
#define CStringUserFlag1( CString )             ( CString )->meta.flags.state.user.user1
#define CStringUserFlag2( CString )             ( CString )->meta.flags.state.user.user2
#define CStringUserFlag3( CString )             ( CString )->meta.flags.state.user.user3
#define CStringUserFlag4( CString )             ( CString )->meta.flags.state.user.user4
#define CStringUserFlag5( CString )             ( CString )->meta.flags.state.user.user5
#define CStringUserFlag6( CString )             ( CString )->meta.flags.state.user.user6
#define CStringUserFlag7( CString )             ( CString )->meta.flags.state.user.user7

#define CStringAttributes( CString )            ( CString )->meta.flags.state.user.data8
#define CStringSetAttribute( CString, attr )    (CStringAttributes( CString ) |= (attr))
#define CStringDelAttribute( CString, attr )    (CStringAttributes( CString ) &= ~(attr))
#define CStringHasAttribute( CString, attr )    ((CStringAttributes( CString ) & (attr)) != 0)



#define CharsEqualsConst( str, const_str )        (strcmp( str, const_str ) == 0)
#define CharsEqualsConstNocase( str, const_str )  (strcasecmp( str, const_str ) == 0)
#define CharsStartsWithConst( str, const_str )    ((strstr( str, const_str ) == str) || (*const_str == '\0'))
#define CharsContainsConst( str, const_str )      (strstr( str, const_str ) != NULL)
#define CharsOccConstWithin( str, const_str, n )  __cxcstring_chars_occ_const_within( str, const_str, n )
#define CharsEndsWithConst( str, const_str )      __cxcstring_chars_ends_with_const( str, const_str )
#define CharsHash64( str )                        strhash64( (const unsigned char*)str )

#define CharsNew( Chars )                       __cxcstring_chars_new( Chars )
#define CharsDelete( Chars )                    free( (char*)(Chars) )

#define CStringNew( Chars )                     COMLIB_OBJECT_NEW( CString_t, Chars, NULL )
#define CStringClone( CString )                 CALLABLE( CString )->Clone( CString )
#define CStringCloneAlloc( CString, Context )   CALLABLE( CString )->CloneAlloc( CString, Context )
#define CStringPrefix( CString, Size )          CALLABLE( CString )->Prefix( CString, Size )
#define CStringDelete( CString )                COMLIB_OBJECT_DESTROY( CString )
#define CStringDeleteList( CStringListPtr )     __cxcstring_cstring_delete_list( CStringListPtr )
#define CStringLength( CString )                ((CString)->meta.size)
#define CStringCodepoints( CString )            ((int)(CString)->meta.flags.state.priv.ucsz)
#define CStringValueDefault( CString, Default ) ((CString) != NULL ? CStringValue( CString ) : (Default))
#define CStringAscii( CString )                 CALLABLE( CString )->Ascii( CString )
#define CStringB16Encode( CString )             CALLABLE( CString )->B16Encode( CString )
#define CStringB16Decode( CString )             CALLABLE( CString )->B16Decode( CString )
#define CStringValueAsQwords( CString )         ((const QWORD*)CStringValue(CString))
#define CStringQwordLength( CString )           qwcount(((size_t)(CString)->meta.size) + 1ULL)
#define CStringMetaAsQword( CString )           ((CString)->meta._bits)
#define CStringCompare( CString1, CString2 )    CALLABLE( CString1 )->Compare( CString1, CString2 )
#define CStringCompareChars( CString, Chars )   CALLABLE( CString )->CompareChars( CString, Chars )
#define CStringEquals( CString1, CString2 )     CALLABLE( CString1 )->Equals( CString1, CString2 )
#define CStringEqualsChars( CString, Chars )    CALLABLE( CString )->EqualsChars( CString, Chars )
#define CStringFind( CString, Chars, StartAt )  CALLABLE( CString )->Find( CString, Chars, StartAt )
#define CStringAtIndex( CString, Chars, Idx )   (CALLABLE( CString )->Find( CString, Chars, Idx ) == 0)
#define CStringContains( CString, Chars )       CALLABLE( CString )->Contains( CString, Chars )
#define CStringSlice( CString, Start, End )     CALLABLE( CString )->Slice( CString, Start, End )
#define CStringStartsWith( CString, Chars )     CALLABLE( CString )->StartsWith( CString, Chars )
#define CStringEndsWith( CString, Chars )       CALLABLE( CString )->EndsWith( CString, Chars )
#define CStringReplace( CString, Probe, Subst ) CALLABLE( CString )->Replace( CString, Probe, Subst )
#define CStringSplit( CString, SplitStr, pSz )  CALLABLE( CString )->Split( CString, SplitStr, pSz )
#define CStringJoin( CString, CStringList )     CALLABLE( CString )->Join( CString, CStringList )
#define CStringObid( CString )                  CALLABLE( CString )->Obid( CString )
#define CStringHash64( CString )                hash64( (const unsigned char*)CStringValue( CString ), CStringLength( CString ) )
#define CStringParentAsType( Type, CString )    ((Type*)((CString)->parent))
#define CStringDecodeUTF8( CString )            CALLABLE( CString )->DecodeUTF8( CString )
#define CStringPrint( CString )                 CALLABLE( CString )->Print( CString )
#define CStringConvertToChars( CString )        __cxcstring_cstring_convert_to_chars( &(CString) )
#define CStringCompress( CString, Mode )        CALLABLE( CString )->Compress( CString, Mode )
#define CStringCompressAlloc( CString, Alloc, Mode )  CALLABLE( CString )->CompressAlloc( CString, Alloc, Mode )
#define CStringDecompress( CString )            CALLABLE( CString )->Decompress( CString )
#define CStringIsCompressed( CString )          ( CStringHasAttribute( CString, CSTRING_ATTR_COMPRESSED ) && CALLABLE( CString )->IsCompressed( CString ) )
#define CStringCharsNotCompressed( Data, Size ) (!__cxcstring_chars_probably_compressed( Data, (int)(Size) ))



DLL_COMLIB_PUBLIC extern CString_t * CStringNewFormat( const char *format, ... );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewFormatAlloc( object_allocator_context_t *context, const char *format, ... );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewReplace( const char *string, const char *probe, const char *subst );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewReplaceAlloc( const char *string, int32_t sz_string, const char *probe, const char *subst, object_allocator_context_t *alloc );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewAlloc( const char *value, object_allocator_context_t *context );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewSizeAttrAlloc( const char *value, int sz, int ucsz, object_allocator_context_t *context, CString_attr attr );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewSliceAlloc( const char *string, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewPrefixAlloc( const char *string, int32_t sz, object_allocator_context_t *allocator );
DLL_COMLIB_PUBLIC extern CString_t * CStringConcat( const CString_t *CSTR__A, const CString_t *CSTR__B, const char *sep );
DLL_COMLIB_PUBLIC extern CString_t * CStringConcatAlloc( const CString_t *CSTR__A, const CString_t *CSTR__B, const char *sep, object_allocator_context_t *context );
DLL_COMLIB_PUBLIC extern CString_t * CStringNewCompressedAlloc( const char *data, int sz, int ucsz, object_allocator_context_t *context, CString_attr attr, CString_compress_mode mode );
DLL_COMLIB_PUBLIC extern int CStringDecompressBytesToBytes( const char *compressed, int32_t csz, char *buffer, int32_t bsz, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );

DLL_COMLIB_PUBLIC extern void CStringSetSerializationCurrentThread( f_CString_serializer_t serializer, f_CString_deserializer_t deserializer );
DLL_COMLIB_PUBLIC extern void CStringGetSerializationCurrentThread( f_CString_serializer_t *serializer, f_CString_deserializer_t *deserializer );
DLL_COMLIB_PUBLIC extern void CStringSetAllocatorContext( object_allocator_context_t *context );
DLL_COMLIB_PUBLIC extern CString_t * CStringFromUnicode( Unicode unicode );



#define CStringCheck( obj )   (( (obj) != NULL && COMLIB_OBJECT_CLASSMATCH( COMLIB_OBJECT(obj), COMLIB_CLASS_CODE( CString_t ) ) ) ? (const CString_t*)(obj) : NULL)



#endif
