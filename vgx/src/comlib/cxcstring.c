/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    cxcstring.c
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

#include "_comlib.h"
#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXSTRING );



// Base
static int CString_cmpid( const CString_t *CSTR__self, const void *idptr );
static objectid_t * CString_getid( CString_t *CSTR__self );
static int64_t CString_serialize( const CString_t *CSTR__self, CQwordQueue_t *output );
static CString_t * CString_deserialize( comlib_object_t *container, CQwordQueue_t *input );
static CString_t * CString_constructor( const void *identifier, CString_constructor_args_t *args );
static void CString_destructor( CString_t *CSTR__self );
static CStringQueue_t * CString_representer( CString_t *CSTR__self, CStringQueue_t *output );
static comlib_object_t * CString_allocator( CString_t *CSTR__self );

// API
static int32_t CString_length( const CString_t *CSTR__self );
static int32_t CString_codepoints( const CString_t *CSTR__self );
static void CString_get_true_length( const CString_t *CSTR__self, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );
static const char * CString_value( const CString_t *CSTR__self );
static CString_t * CString_ascii( const CString_t *CSTR__self );
static CString_t * CString_b16encode( const CString_t *CSTR__self );
static CString_t * CString_b16decode( const CString_t *CSTR__self );
static int64_t CString_as_integer( const CString_t *CSTR__self );
static double CString_as_real( const CString_t *CSTR__self );
static QWORD * CString_modifiable_qwords( CString_t *CSTR__self );
static int CString_to_bytes( const CString_t *CSTR__str, char **data, int32_t *sz, int32_t *ucsz, CString_attr *attr );
static int CString_compare( const CString_t *CSTR__self, const CString_t *CSTR__other );
static int CString_compare_chars( const CString_t *CSTR__self, const char *other );
static bool CString_equals( const CString_t *CSTR__self, const CString_t *CSTR__other );
static bool CString_equals_chars( const CString_t *CSTR__self, const char *other );
static int32_t CString_find( const CString_t *CSTR__self, const char *probe, int32_t startindex );
static bool CString_contains( const CString_t *CSTR__self, const char *probe );
static CString_t * CString_slice( const CString_t *CSTR__self, int32_t *p_start, int32_t *p_end );
static CString_t * CString_slice_with_allocator( const CString_t *CSTR__self, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator );
static bool CString_startswith( const CString_t *CSTR__self, const char *probe );
static bool CString_endswith( const CString_t *CSTR__self, const char *probe );
static CString_t * CString_replace( const CString_t *CSTR__self, const char *probe, const char *subst );
static CString_t * CString_replace_with_allocator( const CString_t *CSTR__self, const char *probe, const char *subst, object_allocator_context_t *alloc );
static CString_t ** CString_split( const CString_t *CSTR__self, const char *splitstr, int32_t *sz );
static CString_t * CString_join( const CString_t *CSTR__self, const CString_t **CSTR__list );
static CString_t * CString_join_with_allocator( const CString_t *CSTR__self, const CString_t **CSTR__list, object_allocator_context_t *alloc );
static const objectid_t * CString_obid( const CString_t *CSTR__self );
static CString_t * CString_clone( const CString_t *CSTR__self );
static CString_t * CString_clone_with_allocator( const CString_t *CSTR__self, object_allocator_context_t *alloc );
static CString_t * CString_compress( const CString_t *CSTR__self, CString_compress_mode mode );
static CString_t * CString_compress_with_allocator( const CString_t *CSTR__self, object_allocator_context_t *alloc, CString_compress_mode mode );
static CString_t * CString_decompress( const CString_t *CSTR__compressed );
static int CString_decompress_to_bytes( const CString_t *CSTR__compressed, char *buffer, int32_t bsz, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr );
static bool CString_is_compressed( const CString_t *CSTR__self );
static CString_compress_header_t * CString_compressed_header( const CString_t *CSTR__self );
static CString_t * CString_prefix_with_allocator( const CString_t *CSTR__self, int32_t sz, object_allocator_context_t *alloc );
static CString_t * CString_prefix( const CString_t *CSTR__self, int32_t sz );
static Unicode CString_decode_utf8( const CString_t *CSTR__self );
static void CString_print( const CString_t *CSTR__self );
static void CString_set_alloc_context( object_allocator_context_t *context );


// internal
static int __compute_min_buffer_size( int string_sz );
static CString_t * __new_from_format( CString_constructor_args_t *input_args );
static CString_t * __string_replace( const char *string, int32_t sz_string, const char *probe, const char *subst, object_allocator_context_t *allocator_context );
static CString_t * __slice( const char *str, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator );
static CString_t * __prefix( const char *data, int32_t len, int32_t uclen, int32_t sz, object_allocator_context_t *alloc );

static const QWORD DEFAULT_SERIALIZER = (0xDEFADEFAULL << 32) | CLASS_CString_t;


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int32_t __abs_index_bytes( const char *bytes, int32_t index ) {
  int64_t errpos;
  int64_t uc;
  return (int32_t)COMLIB_offset_utf8( (const BYTE*)bytes, -1, index, &uc, &errpos );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int32_t __abs_index( const CString_t *CSTR__self, int32_t index ) {
  const char *bytes = CStringValue( CSTR__self );
  return __abs_index_bytes( bytes, index );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __is_consistent( const CString_t *CSTR__self ) {

  if( CSTR__self == NULL ) {
    return false;
  }

  const char *data = CStringValue( CSTR__self );

  // Length of character data
  int64_t len = strlen( data );
  if( len != CSTR__self->meta.size ) {
    printf( "CString inconsistent: size=%d, expected=%llu", CSTR__self->meta.size, len );
    return false;
  }

  // Location of character data
  if( CSTR__self->meta.flags.state.priv.shrt ) {
    if( (uintptr_t)data > (uintptr_t)CSTR__self ) {
      printf( "CString inconsistent: short flag set for long string" );
      return false;
    }
  }
  else {
    if( (uintptr_t)data < (uintptr_t)CSTR__self ) {
      printf( "CString inconsistent: short flag not set for short string" );
      return false;
    }
  }

  return true;
}

#define decomp_bsz ARCH_PAGE_SIZE
static __THREAD char PALIGNED_ gt_decomp_buffer[decomp_bsz];


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __extract_bytes( const CString_t *CSTR__self, char *buffer, int32_t bsz, char **decompressed, int32_t *rlen, int32_t *ruclen ) {
  const char *data = CStringValue( CSTR__self );
  int32_t len = CStringLength( CSTR__self );
  int32_t uclen = CStringCodepoints( CSTR__self );

  CString_compress_header_t *compressed = CString_compressed_header( CSTR__self );
  if( !compressed ) {
    goto end;
  }

  CString_attr attr;
  if( CString_decompress_to_bytes( CSTR__self, buffer, bsz, decompressed, &len, &uclen, &attr ) < 0 ) {
    return NULL;
  }

  data = *decompressed;

end:

  if( rlen ) {
    *rlen = len;
  }

  if( ruclen ) {
    *ruclen = uclen;
  }

  return data;
}



#define BEGIN_EXTRACTED( CString, XData )  \
  do { \
    const char *XData; \
    SUPPRESS_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL \
    char *_cstring__extracted = NULL;  \
    XData = __extract_bytes( (CString), gt_decomp_buffer, decomp_bsz, &_cstring__extracted, NULL, NULL ); \
    do

#define BEGIN_EXTRACTED_LENGTH( CString, XData, XLength, XCodepoints )  \
  do { \
    const char *XData; \
    int32_t XLength; \
    int32_t XCodepoints; \
    SUPPRESS_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL \
    char *_cstring__extracted = NULL;  \
    XData = __extract_bytes( (CString), gt_decomp_buffer, decomp_bsz, &_cstring__extracted, &(XLength), &(XCodepoints) ); \
    do


#define END_EXTRACTED \
    WHILE_ZERO; \
    if( _cstring__extracted && _cstring__extracted != gt_decomp_buffer ) { \
      ALIGNED_FREE( _cstring__extracted ); \
    } \
  } WHILE_ZERO




/*

+----------------+----------------+--------+--------+----+----+--------+--------+--------+--------+--------+--------
|      obid      | short space OR | vtable | typinf | sz |flgs| alloc  |           character data  ....
|                |  allocator     |        |        |    |    |        |                                            
+----------------+----------------+--------+--------+----+----+--------+--------+--------+--------+--------+--------
|                                 \ ___________ CString_t ____________ /
|                                                                      |
\_____________  __CStringAllocated_t _________________________________ /


This layout is designed to allow CString objects to be allocated
either by standard malloc() or by using a custom allocator.
A custom allocator will typically be a cxmalloc family, which requires
this particular layout with 16 bytes of meta data first, then 16 bytes
of internal allocator space, then 32 bytes of object members, then
any number of allocation units for the object payload.

When malloc is used the 16-byte space named "short space" may be used
for character data if the length of the string is 15 or less.
(The \0 terminator is stored and requires the last byte.) In this case
only a single cacheline is allocated. When the string is larger than 
15 bytes (needing more than 16 bytes of storage) additional memory
is allocated in the "character data" region. Additional memory will
be allocated in QWORD multiples. The very last byte will always be 0.
The last QWORD may or may not contain character data. If the string 
length is a whole multiple of 8 the last QWORD is all zeros.

When a custom allocator (e.g. cxmalloc) is used the 16-byte region
named "allocator" is reserved for internal allocator data. In this case
additional character data is always required since there is no space
for short strings in the first cache line.

*/



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_CStringAllocated_t {
  union {
    __m256i __head;
    struct {
      objectid_t __obid;  // object id area (also meta portion of allocator when allocator is used)
      __m128i space;      // short string ( < 16 chars ) OR allocator reserved area 
    };
  };
  CString_t cstring_obj;
} __CStringAllocated_t;



#define POBID_FROM_OBJECT( CString )      ((objectid_t*)((char*)(CString) - sizeof(__m256i)))
#define ALLOCATED_FROM_OBJECT( CString )  ((__CStringAllocated_t*)((char*)(CString) - sizeof(__m256i)))



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_vtable_t CStringMethods = {
  /* common cxlib_object_vtable interface */
  .vm_cmpid             = (f_object_comparator_t)CString_cmpid,          /* vm_cmpid       */
  .vm_getid             = (f_object_identifier_t)CString_getid,          /* vm_getid       */
  .vm_serialize         = (f_object_serializer_t)CString_serialize,      /* vm_serialize   */
  .vm_deserialize       = (f_object_deserializer_t)CString_deserialize,  /* vm_deserialize */
  .vm_construct         = (f_object_constructor_t)CString_constructor,   /* vm_construct   */
  .vm_destroy           = (f_object_destructor_t)CString_destructor,     /* vm_destroy     */
  .vm_represent         = (f_object_representer_t)CString_representer,   /* vm_represent   */
  .vm_allocator         = (f_object_allocator_t)CString_allocator,       /* vm_allocator   */
  /* CString methods */
  .Length               = CString_length,
  .Codepoints           = CString_codepoints,
  .GetTrueLength        = CString_get_true_length,
  .Value                = CString_value,
  .Ascii                = CString_ascii,
  .B16Encode            = CString_b16encode,
  .B16Decode            = CString_b16decode,
  .AsInteger            = CString_as_integer,
  .AsReal               = CString_as_real,
  .ModifiableQwords     = CString_modifiable_qwords,
  .ToBytes              = CString_to_bytes,
  .Compare              = CString_compare,
  .CompareChars         = CString_compare_chars,
  .Equals               = CString_equals,
  .EqualsChars          = CString_equals_chars,
  .Find                 = CString_find,
  .Contains             = CString_contains,
  .Slice                = CString_slice,
  .SliceAlloc           = CString_slice_with_allocator,
  .StartsWith           = CString_startswith,
  .EndsWith             = CString_endswith,
  .Replace              = CString_replace,
  .ReplaceAlloc         = CString_replace_with_allocator,
  .Split                = CString_split,
  .Join                 = CString_join,
  .JoinAlloc            = CString_join_with_allocator,
  .Obid                 = CString_obid,
  .Clone                = CString_clone,
  .CloneAlloc           = CString_clone_with_allocator,
  .Compress             = CString_compress,
  .CompressAlloc        = CString_compress_with_allocator,
  .Decompress           = CString_decompress,
  .DecompressToBytes    = CString_decompress_to_bytes,
  .IsCompressed         = CString_is_compressed,
  .CompressedHeader     = CString_compressed_header,
  .PrefixAlloc          = CString_prefix_with_allocator,
  .Prefix               = CString_prefix,
  .DecodeUTF8           = CString_decode_utf8,
  .Print                = CString_print
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void CString_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( CString_t, CXLIB_OBTYPE_SEQUENCE, &CStringMethods, OBJECT_IDENTIFIED_BY_OBJECTID, 0 );

  ASSERT_TYPE_SIZE( CString_t,              sizeof(cacheline_t)/2   );
  ASSERT_TYPE_SIZE( __CStringAllocated_t,   sizeof(cacheline_t)     );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void CString_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( CString_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewFormat( const char *format, ... ) {
  va_list args;
  va_start( args, format );

  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = -1,
    .ucsz         = 0,
    .format       = format,
    .format_args  = &args,
    .alloc        = NULL
  };

  CString_t *CSTR__self = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args );

  va_end( args );

  return CSTR__self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewFormatAlloc( object_allocator_context_t *context, const char *format, ... ) {
  va_list args;
  va_start( args, format );

  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = -1,
    .ucsz         = 0,
    .format       = format,
    .format_args  = &args,
    .alloc        = context
  };

  CString_t *CSTR__self = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args );

  va_end( args );

  return CSTR__self;
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewReplace( const char *string, const char *probe, const char *subst ) {
  return __string_replace( string, (int32_t)strlen( string ), probe, subst, NULL );
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewReplaceAlloc( const char *string, int32_t sz_string, const char *probe, const char *subst, object_allocator_context_t *alloc ) {
  return __string_replace( string, sz_string, probe, subst, alloc );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewAlloc( const char *value, object_allocator_context_t *context ) {
  CString_constructor_args_t args = {
    .string = NULL,
    .len    = 0,
    .ucsz   = 0,
    .format = NULL,
    .format_args = NULL,
    .alloc = context
  };
  return COMLIB_OBJECT_NEW( CString_t, value, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewSizeAttrAlloc( const char *value, int sz, int ucsz, object_allocator_context_t *context, CString_attr attr ) {

  // Create string compressed
  if( (attr & CSTRING_ATTR_COMPRESSED) != 0 ) {
    return CStringNewCompressedAlloc( value, sz, ucsz, context, attr, CSTRING_COMPRESS_MODE_LZ4 );
  }

  // Create string
  CString_constructor_args_t args = {
    .string      = value,
    .len         = sz,
    .ucsz        = ucsz,
    .format      = NULL,
    .format_args = NULL,
    .alloc       = context
  };

  CString_t *CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  if( CSTR__str ) {
    CStringAttributes( CSTR__str ) = attr;
  }
  return CSTR__str;
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewSliceAlloc( const char *string, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator ) {
  return __slice( string, p_start, p_end, allocator );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __concat( const CString_t *CSTR__A, const CString_t *CSTR__B, const char *sep, object_allocator_context_t *allocator ) {
  CString_t *CSTR__cat = NULL;

  int32_t sz_sep = sep ? (int)strlen(sep) : 0;
  BEGIN_EXTRACTED_LENGTH( CSTR__A, src_A, sz_A, ucsz_A ) {
    BEGIN_EXTRACTED_LENGTH( CSTR__B, src_B, sz_B, ucsz_B ) {
      // Update codepoint counters by computing them as needed
      ucsz_A = CALLABLE( CSTR__A )->Codepoints( CSTR__A );
      ucsz_B = CALLABLE( CSTR__B )->Codepoints( CSTR__B );

      CString_constructor_args_t args = {
        .string       = NULL,
        .len          = sz_A + sz_B + sz_sep,
        .ucsz         = ucsz_A + ucsz_B + sz_sep,
        .format       = NULL,
        .format_args  = NULL,
        .alloc        = allocator
      };

      if( (CSTR__cat = CString_constructor( NULL, &args )) != NULL ) {
        // Copy both sources into destination with optional separator
        const char *end_A = src_A + sz_A;
        const char *end_B = src_B + sz_B;

        char *dest = (char*)CString_modifiable_qwords( CSTR__cat );

        // Copy A
        while( src_A < end_A ) {
          *dest++ = *src_A++;
        }

        // Copy separator
        if( sep ) {
          const char *src_sep = sep;
          const char *end_sep = sep + sz_sep;
          while( src_sep < end_sep ) {
            *dest++ = *src_sep++;
          }
        }

        // Copy B
        while( src_B < end_B ) {
          *dest++ = *src_B++;
        }

        // Inherit first string's user flags
        CSTR__cat->meta.flags.state.user = CSTR__A->meta.flags.state.user;
      }

    } END_EXTRACTED;
  } END_EXTRACTED;

  return CSTR__cat;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringConcat( const CString_t *CSTR__A, const CString_t *CSTR__B, const char *sep ) {
  return __concat( CSTR__A, CSTR__B, sep, CSTR__A->allocator_context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringConcatAlloc( const CString_t *CSTR__A, const CString_t *CSTR__B, const char *sep, object_allocator_context_t *context ) {
  return __concat( CSTR__A, CSTR__B, sep, context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewCompressedAlloc( const char *data, int sz, int ucsz, object_allocator_context_t *context, CString_attr attr, CString_compress_mode mode ) {

#define ACCELERATION 1

  CString_t *CSTR__compressed = NULL;
  char *compressed_buffer = NULL;
  XTRY {

    // Max size of compressed data
    int dst_capacity;
    switch( mode ) {
    case CSTRING_COMPRESS_MODE_NONE:
    dont_compress:
      attr &= ~CSTRING_ATTR_COMPRESSED; // safeguard
      CSTR__compressed = CStringNewSizeAttrAlloc( data, sz, ucsz, context, attr );
      XBREAK;
    case CSTRING_COMPRESS_MODE_LZ4:
    {
      // Too small?
      if( sz < CSTRING_MIN_COMPRESS_SZ ) {
        goto dont_compress;
      }
      // Find the allocator's lower bound for payload of this size
      // Ccompression attempt will fail if it can't fit
      uint32_t low;
      // We use allocator with quantized bucket sizes
      if( context ) {
        // Find total CString payload bytes needed for the uncompressed case
        uint32_t uncomp_sz = qwcount( sz+1 ) * sizeof( QWORD );
        // Compress only if data can fit in a smaller allocator bucket than the uncompressed data
        uint32_t high;
        context->bounds( context->allocator, uncomp_sz, &low, &high );
      }
      // We use malloc
      else {
        // Compress only if data can be reduced to at least 80% of its original size
        low = (uint32_t)((sz * 819ULL) >> 10);
      }
      // Account for header and cstring object's implicitly added nul term
      dst_capacity = (low - 1) - sizeof( CString_compress_header_t ) - 1;
      break;
    }
    case CSTRING_COMPRESS_MODE_LZ4HC:
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x001, "TODO: implement LZ4HC" );
    default:
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x002, "unsupported compression mode: %02x", (int)mode );
    }

    // Size of entire buffer including header (plus a safety qword)
    int sz_buffer = sizeof( CString_compress_header_t ) + dst_capacity + sizeof( QWORD );

    // Allocate buffer
    CALIGNED_ARRAY_THROWS( compressed_buffer, char, sz_buffer, 0x003 );

    // Set header
    CString_compress_header_t *compressed_header = (CString_compress_header_t*)compressed_buffer;
    compressed_header->magic = CSTRING_COMPRESS_MAGIC;
    compressed_header->orig.mode = CSTRING_COMPRESS_MODE_NONE; // may override below if data already compressed
    compressed_header->orig.sz = sz;
    compressed_header->orig.ucsz = ucsz;
    // data already compressed, expect data to contain header info
    if( (attr & CSTRING_ATTR_COMPRESSED) != 0 && sz > (int)sizeof( CString_compress_header_t ) ) {
      CString_compress_header_t *orig_header = (CString_compress_header_t*)data;
      if( orig_header->magic == CSTRING_COMPRESS_MAGIC ) {
        compressed_header->orig.mode = orig_header->compressed.mode;
      }
    }

    // Compress
    // Data destination right behind header
    compressed_header->compressed.mode = mode;
    switch( compressed_header->compressed.mode ) {
    case CSTRING_COMPRESS_MODE_LZ4:
    {
      char *cdest = compressed_buffer + sizeof( CString_compress_header_t );
      // Compress data into cdest and update header with number of compressed bytes
      // We expect this to fail if output isn't less then ~90% of input, and if so we will skip compression
      if( (compressed_header->compressed.sz = LZ4_compress_fast( data, cdest, sz, dst_capacity, ACCELERATION )) <= 0 ) {
        goto dont_compress;
      }
      break;
    }
    case CSTRING_COMPRESS_MODE_LZ4HC:
      THROW_ERROR( CXLIB_ERR_API, 0xDDD ); // TODO!
      break;
    default:
      break;
    }

    // Unhelpful compression, use original data instead
    if( compressed_header->compressed.sz > sz ) {
      goto dont_compress;
    }

    int objsz = sizeof( CString_compress_header_t ) + compressed_header->compressed.sz;

    // Allocate uninitialized cstring object
    CString_constructor_args_t args = {
      .string      = NULL,
      .len         = objsz,
      .ucsz        = 0,
      .format      = NULL,
      .format_args = NULL,
      .alloc       = context
    };

    if( (CSTR__compressed = COMLIB_OBJECT_NEW( CString_t, NULL, &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Set attributes
    attr |= CSTRING_ATTR_COMPRESSED;
    CStringAttributes( CSTR__compressed ) = attr;

    // Copy compressed data into cstring object
    QWORD *qdst = CALLABLE( CSTR__compressed )->ModifiableQwords( CSTR__compressed );
    QWORD *qsrc = (QWORD*)compressed_buffer;
    int qwsz = qwcount( objsz );
    QWORD *qend = qdst + qwsz;
    while( qdst < qend ) {
      *qdst++ = *qsrc++;
    }


  }
  XCATCH( errcode ) {
    if( CSTR__compressed ) {
      CStringDelete( CSTR__compressed );
      CSTR__compressed = NULL;
    }
  }
  XFINALLY {
    if( compressed_buffer ) {
      ALIGNED_FREE( compressed_buffer );
    }
  }

  return CSTR__compressed;
}



/*******************************************************************//**
 * Override the serializer and deserializer function for the current thread.
 * When non-NULL these will be called instead of the default implementations
 * when invoked from the current thread.
 ***********************************************************************
 */

static __THREAD f_CString_serializer_t gt_serializer = NULL;
static __THREAD f_CString_deserializer_t gt_deserializer = NULL;
static __THREAD object_allocator_context_t *gt_allocator_context = NULL;


/**************************************************************************//**
 * CStringSetSerializationCurrentThread
 *
 ******************************************************************************
 */
DLL_EXPORT void CStringSetSerializationCurrentThread( f_CString_serializer_t serializer, f_CString_deserializer_t deserializer ) {
  gt_serializer = serializer;
  gt_deserializer = deserializer;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void CStringGetSerializationCurrentThread( f_CString_serializer_t *serializer, f_CString_deserializer_t *deserializer ) {
  *serializer = gt_serializer;
  *deserializer = gt_deserializer;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void CStringSetAllocatorContext( object_allocator_context_t *context ) {
  gt_allocator_context = context;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringFromUnicode( Unicode unicode ) {
  CString_t *CSTR__str = NULL;
  int64_t len = 0;
  int64_t errpos = -1;
  BYTE * utf8_bytes = COMLIB_encode_utf8( unicode, &len, &errpos );
  int64_t ucsz = CALLABLE( unicode )->Length( unicode );
  if( utf8_bytes ) {
    CString_constructor_args_t args = {
      .string      = (char*)utf8_bytes,
      .len         = (int)len,
      .ucsz        = (int)ucsz,
      .format      = NULL,
      .format_args = NULL,
      .alloc       = NULL
    };
    CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
    ALIGNED_FREE( utf8_bytes );
  }
  return CSTR__str;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __compute_obid( objectid_t *dest, const CString_t *CSTR__str ) {
  BEGIN_EXTRACTED_LENGTH( CSTR__str, data, len, uclen ) {
    *dest = smartstrtoid( data, len );
    // USER DATA attr
    uint8_t attr = CSTR__str->meta.flags.state.user.data8;
    // Attributes exist, mix in attributes to create a different obid
    if( attr ) {
      uint64_t ax = ihash64( attr );
      dest->L ^= ax;
      dest->H ^= ax;
    }
  } END_EXTRACTED;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const objectid_t * __get_obid( const CString_t *CSTR__self ) {
  if( idnone( POBID_FROM_OBJECT(CSTR__self) ) && CSTR__self->meta.flags.state.priv.init ) {
    __compute_obid( POBID_FROM_OBJECT( CSTR__self ), CSTR__self );
  }
  return POBID_FROM_OBJECT(CSTR__self);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int CString_cmpid( const CString_t *CSTR__self, const void *idptr ) {
  return idcmp( CString_getid( (CString_t*)CSTR__self ), (const objectid_t*)idptr );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static objectid_t * CString_getid( CString_t *CSTR__self ) {
  return (objectid_t*)__get_obid( CSTR__self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t CString_serialize( const CString_t *CSTR__self, CQwordQueue_t *output ) {
  // Specialized thread local serializer
  if( gt_serializer ) {
    return gt_serializer( CSTR__self, output );
  }
  // Default serializer
  CQwordQueue_vtable_t *iq = CALLABLE( output );
  QWORD *data = (QWORD*)CStringValue( CSTR__self );
  int64_t n_meta = qwsizeof( CSTR__self->meta );
  int64_t n_data = qwcount( CSTR__self->meta.size + 1 ); // include terminator pad

  if( iq->WriteNolock( output, &DEFAULT_SERIALIZER, 1 ) != 1 ) {
    return -1;
  }

  if( iq->WriteNolock( output, &CSTR__self->meta._bits, n_meta ) != n_meta ) {
    return -1;
  }

  if( iq->WriteNolock( output, data, n_data ) != n_data ) {
    return -1;
  }

  return n_meta + n_data;
}



/*******************************************************************//**
 *
 *
 * NOTE: THIS DESERIALIZER WILL ONLY USE malloc().
 ***********************************************************************
 */
static CString_t * CString_deserialize( comlib_object_t *container, CQwordQueue_t *input ) {
  // Specialized thread local deserializer
  if( gt_deserializer ) {
    return gt_deserializer( container, input );
  }
  // Default deserializer
  CString_t *CSTR__self = NULL;
  __CStringAllocated_t *memory = NULL;

  XTRY {
    CQwordQueue_vtable_t *iq = CALLABLE( input );

    QWORD mark, *pmark=&mark;
    if( iq->ReadNolock( input, (void**)&pmark, 1 ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x501 );
    }
    else if( mark != DEFAULT_SERIALIZER ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x502 );
    }

    CString_t CSTR__buffer;
    QWORD *pmeta = &CSTR__buffer.meta._bits;
    int64_t n_meta = qwsizeof( CSTR__buffer.meta._bits );
    if( iq->ReadNolock( input, (void**)&pmeta, n_meta ) != n_meta ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x503 );
    }

    int64_t n_str_qwords = qwcount( CSTR__buffer.meta.size + 1 ); // include terminator pad
    size_t alloc_bytes = sizeof( __CStringAllocated_t ); // at least this
    if( n_str_qwords > __CSTRING_MAX_SHORT_STRING_QWORDS ) {
      // longer than what fits in the small space, we need a bigger allocation
      alloc_bytes += sizeof( QWORD ) * n_str_qwords; // additional cache line(s) for string characters
    }

    // Allocate object and populate
    if( CALIGNED_BYTES( memory, alloc_bytes ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x504 );
    }

    // Initialize object vtable and type
    CSTR__self = &memory->cstring_obj;
    if( COMLIB_OBJECT_INIT( CString_t, CSTR__self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x505 );
    }

    // No external allocator context
    CSTR__self->allocator_context = NULL;

    // Copy object meta data
    memcpy( &CSTR__self->meta, &CSTR__buffer.meta, sizeof( CString_meta_t ) );

    // Populate string data
    const char *value = CStringValue( CSTR__self );
    if( iq->ReadNolock( input, (void**)&value, n_str_qwords ) != n_str_qwords ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x506 );
    }
  }
  XCATCH( errcode ) {
    if( memory ) {
      ALIGNED_FREE( memory );
      CSTR__self = NULL;
    }
  }
  XFINALLY {
  }
  
  return CSTR__self;
  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_constructor( const void *identifier, CString_constructor_args_t *args ) {
  CString_t *CSTR__self = NULL;
  __CStringAllocated_t *memory = NULL;
  const char *initstring = NULL;
  int32_t len = -1;
  
  XTRY {

    // Set string data from identifier
    if( identifier ) {
      initstring = (const char*)identifier;
    }
    // Use args if no identifier
    else if( args ) {
      // Set string from args string
      if( args->string ) {
        initstring = args->string;
        // If non-negative length also specified then this is the assumed length of initstring
        if( args->len > -1 ) {
          len = args->len;
        }
      }
      // Set string from args format
      else if( args->format && args->format_args ) {
        CSTR__self = __new_from_format( args );
        XBREAK;
      }
      // Error: uninitialized string must know length at construction time
      else if( args->len < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x511 );
      }
      // String will be uninitialized
      else {
        len = args->len;
      }
    }
    // Can't create without identifier or args
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x512 );
    }

    // ------------------------------
    // Allocate and initialize object
    // ------------------------------
    bool is_short = true; // unless it isn't...

    size_t str_size_with_nul = ((len > -1) ? len : strnlen( initstring ? initstring : "", INT_MAX )) + 1; // include NUL terminator to make processing easier
    if( str_size_with_nul > INT_MAX ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x513 );
    }
    size_t n_str_qwords = qwcount( str_size_with_nul );

    // We are using malloc
    if( args == NULL || args->alloc == NULL ) {
      // Compute the amount of memory needed
      size_t alloc_bytes = sizeof( __CStringAllocated_t ); // at least this
      if( n_str_qwords > __CSTRING_MAX_SHORT_STRING_QWORDS ) {
        // longer than what fits in the small space, we need a bigger allocation
        alloc_bytes += sizeof( QWORD ) * n_str_qwords; // additional cache line(s) for string characters
        is_short = false; // not a short string with data embedded in first cacheline
      }

      // Allocate space for object data and string data
      if( CALIGNED_BYTES( memory, alloc_bytes ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x514 );
      }

      // Get the true object pointer from the allocated memory
      CSTR__self = &memory->cstring_obj;

      // no allocator
      CSTR__self->allocator_context = NULL;
    }
    // We have an externally supplied allocator context
    else {
      // The allocator will return the true object pointer, i.e. CString_t !
      object_allocator_context_t *context = args->alloc;
      size_t n_chars = n_str_qwords * sizeof( QWORD );
      if( (CSTR__self = (CString_t*)context->allocfunc( context->allocator, n_chars )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x515 );
      }
      
      // No room for short strings when we're using external allocator
      is_short = false;
      
      // Keep the allocator reference
      CSTR__self->allocator_context = context;
    }

    // Initialize object vtable and type
    if( COMLIB_OBJECT_INIT( CString_t, CSTR__self, NULL ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x516 );
    }

    // ------------------
    // Set flags and data
    // ------------------
    CSTR__self->meta.flags._flags = 0;
    
    // Do we have char data embedded in first cacheline ?
    CSTR__self->meta.flags.state.priv.shrt = is_short ? 1 : 0; 
    
    // Terminate: set the last qword in the string data to all zero bytes
    *((QWORD*)CStringValue( CSTR__self ) + n_str_qwords - 1) = 0;

    char *dst = (char*)CStringValue( CSTR__self ); // remove const, we are going to write to this value location

    // Copy string data into object if data was provided
    if( initstring != NULL ) {
      const char *src = initstring;

      // Copy entire initstring up to and including NUL term
      if( len < 0 ) {
        strcpy( dst, src );
      }
      // Copy the specified number of bytes from initstring, including NULs if they occur.
      else {
        memcpy( dst, src, len );
        dst[len] = '\0'; // Always terminate beyond last char
      }
      
      CSTR__self->meta.flags.state.priv.init = 1; // mark as initialized
    }
    // No initializer data provided
    else {
      *dst = '\0'; // Initialize to empty string
    }

    // -----------------
    // Set string length
    // -----------------
    // (bypass const qualifier to set it once)
    int32_t *psize = (int32_t*)&CSTR__self->meta.size;
    *psize = (int32_t)str_size_with_nul - 1; // string length does not include NUL term

    // ------------------------------------------------
    // Set number of Unicode codepoints (if applicable)
    // ------------------------------------------------
    if( args && args->ucsz > 0 && args->ucsz < (1<<12) ) { // <- 12 bits max storage for Unicode codepoint length
      CSTR__self->meta.flags.state.priv.ucsz = (uint16_t)(args->ucsz & 0xFFF);
    }

    // -------------------
    // Reset the object ID
    // -------------------
    idunset( POBID_FROM_OBJECT( CSTR__self ) ); // (will be computed later on first request)

  }
  XCATCH( errcode ) {
    if( memory ) {
      ALIGNED_FREE( memory );
      CSTR__self = NULL;
    }
    if( CSTR__self && args && args->alloc ) {
      object_allocator_context_t *context = args->alloc;
      context->deallocfunc( context->allocator, CSTR__self );
      CSTR__self = NULL;
    }
  }
  XFINALLY {
  }

  return CSTR__self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CString_destructor( CString_t* CSTR__self ) {
  object_allocator_context_t *context;
  if( CSTR__self ) {
    if( (context = CSTR__self->allocator_context) != NULL ) {
      context->deallocfunc( context->allocator, CSTR__self );
    }
    else {
      __CStringAllocated_t *memory = ALLOCATED_FROM_OBJECT( CSTR__self );
      ALIGNED_FREE( memory );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * CString_representer( CString_t *CSTR__self, CStringQueue_t *output ) {
  const char *data = CStringValue( CSTR__self );
  if( CALLABLE( output )->Write( output, data, CSTR__self->meta.size ) == CSTR__self->meta.size ) {
    return output;
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * CString_allocator( CString_t *CSTR__self ) {
  object_allocator_context_t *context = CSTR__self ? CSTR__self->allocator_context : gt_allocator_context;
  if( context == NULL ) {
    return NULL;
  }
  return context->allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int32_t CString_length( const CString_t *CSTR__self ) {
  return CSTR__self->meta.size;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int32_t CString_codepoints( const CString_t *CSTR__self ) {
  int ncp = CSTR__self->meta.flags.state.priv.ucsz;
  if( ncp > 0 ) {
    return ncp;
  }
  // codepoints not available, we have to count
  int64_t errpos;
  if( (ncp = (int)COMLIB_length_utf8( (const BYTE*)CStringValue( CSTR__self ), CSTR__self->meta.size, NULL, &errpos )) >= 0 ) {
    return ncp;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * CString_value( const CString_t *CSTR__self ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }
  return CStringValue( CSTR__self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_ascii( const CString_t *CSTR__self ) {
  CString_t *CSTR__ascii = NULL;
  if( CSTR__self ) {
    CString_constructor_args_t args = {
      .string       = NULL,
      .len          = CStringLength( CSTR__self ),
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = NULL
    };

    if( (CSTR__ascii = CString_constructor( NULL, &args )) != NULL ) {
      const BYTE *src = (BYTE*)CStringValue( CSTR__self );
      const BYTE *end = src + CStringLength( CSTR__self );
      char *dest = (char*)CString_modifiable_qwords( CSTR__ascii );
      while( src < end ) {
        BYTE c = *src++;
        if( c < 0x20 ) {
          c = '.';
        }
        else if( c == 0x22 ) { // " -> '
          c = '\'';
        }
        else if( c > 126 ) {
          c = '?';
        }
        *dest++ = c;
      }
    }
  }

  return CSTR__ascii;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_b16encode( const CString_t *CSTR__self ) {
  CString_t *CSTR__b16 = NULL;
  if( CSTR__self ) {
    CString_constructor_args_t args = {
      .string       = NULL,
      .len          = 2*CStringLength( CSTR__self ),
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = NULL
    };

    if( (CSTR__b16 = CString_constructor( NULL, &args )) != NULL ) {
      const BYTE *src = (BYTE*)CStringValue( CSTR__self );
      const BYTE *end = src + CStringLength( CSTR__self );
      char *dest = (char*)CString_modifiable_qwords( CSTR__b16 );
      while( src < end ) {
        dest = write_HEX_byte( dest, *src++ );
      }
      *dest = '\0';
    }

  }
  return CSTR__b16;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_b16decode( const CString_t *CSTR__self ) {
  CString_t *CSTR__decoded = NULL;
  if( CSTR__self ) {
    CString_constructor_args_t args = {
      .string       = NULL,
      .len          = CStringLength( CSTR__self ) / 2,
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = NULL
    };

    if( (CSTR__decoded = CString_constructor( NULL, &args )) != NULL ) {
      const char *src = CStringValue( CSTR__self );
      const char *end = src + CStringLength( CSTR__self );
      BYTE *dest = (BYTE*)CString_modifiable_qwords( CSTR__decoded );
      while( src < end ) {
        src = hex_to_BYTE( src, dest++ );
      }
      *dest = '\0';
    }

  }
  return CSTR__decoded;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t CString_as_integer( const CString_t *CSTR__self ) {
  if( CSTR__self  ) {
    const char *str = CStringValue( CSTR__self );
    char *end;
    int64_t val = strtoll( str, &end, 0 );
    if( *end == 0 ) {
      return val;
    }
    double rval = CString_as_real( CSTR__self );
    return rval < INFINITY ? (int64_t)rval : 0;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double CString_as_real( const CString_t *CSTR__self ) {
  if( CSTR__self  ) {
    const char *str = CStringValue( CSTR__self );
    char *end = NULL;
    double val = strtof( str, &end );
    if( !end || *end == '\0' ) {
      return val;
    }
  }

  return NAN;
}



/*******************************************************************//**
 * Use this method to set the string data (once) immediately following
 * object construction where only length was provided. It is an error
 * to call this method on a string object where string data was supplied
 * at construction time. It is also an error to call this method more
 * than once on the same object.
 *
 * NOTE: This method should be used with great care, only for things
 * like deserialization read buffers.
 ***********************************************************************
 */
static QWORD * CString_modifiable_qwords( CString_t *CSTR__self ) {
  if( CSTR__self->meta.flags.state.priv.init == 0 ) {
    CSTR__self->meta.flags.state.priv.init = 1;
    return (QWORD*)CStringValue( CSTR__self );
  }
  return NULL;
}



/*******************************************************************//**
 * Place the contents of cstring into the supplied destinations:
 *    data  : Automatically allocated (NOTE: Must use ALIGNED free)
 *            DO NOT supply a pointer to already allocated space
 *    sz    : Number of bytes in string
 *    ucsz  : Number of Unicode codepoint (or 0 if unknown/not Unicode)
 *    attr  : Any attributes that may be set on the cstring
 * 
 * Returns 0 on success, -1 on error
 * 
 ***********************************************************************
 */
static int CString_to_bytes( const CString_t *CSTR__str, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr ) {
  int ret = 0;

  QWORD *buffer = NULL;

  XTRY {

    int qlen = qwcount( CStringLength( CSTR__str ) + 1 );

    CALIGNED_ARRAY_THROWS( buffer, QWORD, qlen, 0x001 );
    // Ensure the last qword is zero before we copy
    buffer[qlen-1] = 0;

    // Copy
    QWORD *qsrc = (QWORD*)CStringValue( CSTR__str );
    QWORD *qdst = buffer;
    QWORD *qend = qdst + qlen;
    while( qdst < qend ) {
      *qdst++ = *qsrc++;
    }

    // Assign to return pointers
    *rdata = (char*)buffer;
    *rsz = CStringLength( CSTR__str );
    *rucsz = CStringCodepoints( CSTR__str );
    *rattr = CStringAttributes( CSTR__str );
  }
  XCATCH( errcode ) {
    if( buffer ) {
      ALIGNED_FREE( buffer );
    }
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int CString_compare( const CString_t *CSTR__self, const CString_t *CSTR__other ) {
  const char *self_value = CStringValue( CSTR__self );
  const char *other_value = CStringValue( CSTR__other );
  int32_t self_len = CStringLength( CSTR__self );
  int32_t other_len = CStringLength( CSTR__other );
  int32_t max_len = self_len > other_len ? self_len : other_len;
  return strncmp( self_value, other_value, max_len );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int CString_compare_chars( const CString_t *CSTR__self, const char *other ) {
  const char *self_value = CStringValue( CSTR__self );
  int32_t self_len = CStringLength( CSTR__self );
  int cmp = strncmp( self_value, other, self_len );
  // other may have more characters
  if( cmp == 0 && other[self_len] != '\0' ) {
    cmp = -1; // self is less than other since other has more characters
  }
  return cmp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_extracted_equals( const CString_t *CSTR__self, const CString_t *CSTR__other ) {
  bool is_equal = false;
  CString_attr attrA = CStringAttributes( CSTR__self );
  CString_attr attrB = CStringAttributes( CSTR__other );

  // We have to extract and fall back on plain string comparison
  BEGIN_EXTRACTED_LENGTH( CSTR__self, data_self, data_sz, data_ucsz ) {
    BEGIN_EXTRACTED_LENGTH( CSTR__other, data_other, other_sz, other_ucsz ) {
      do {
        // Size must match
        if( data_sz != other_sz ) {
          break;
        }
        // Attributes must match
        if( (attrA & ~CSTRING_ATTR_COMPRESSED) != (attrB & ~CSTRING_ATTR_COMPRESSED) ) {
          break;
        }
        // Compare
        if( memcmp( data_self, data_other, data_sz ) == 0 ) {
          is_equal = true;
        }
      } WHILE_ZERO;
    } END_EXTRACTED;
  } END_EXTRACTED;

  return is_equal;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_equals( const CString_t *CSTR__self, const CString_t *CSTR__other ) {
  CString_attr attrA = CStringAttributes( CSTR__self );
  CString_attr attrB = CStringAttributes( CSTR__other );

  // One or both compressed
  if( (attrA & CSTRING_ATTR_COMPRESSED) || (attrB & CSTRING_ATTR_COMPRESSED) ) {
    return CString_extracted_equals( CSTR__self, CSTR__other );
  }

  // Different length
  if( CSTR__self->meta.size != CSTR__other->meta.size ) {
    return false;
  }
  // Same length

  // Attributes must be the same
  if( attrA != attrB ) {
    return false;
  }
  
  // Use obids if set
  if( !idnone( POBID_FROM_OBJECT( CSTR__self ) ) && !idnone( POBID_FROM_OBJECT( CSTR__other ) ) ) {
    if( idmatch( POBID_FROM_OBJECT( CSTR__self ), POBID_FROM_OBJECT( CSTR__other ) ) ) {
      return true;    // ==
    }
    return false;   // !=
  }
  
  // Normal comparison
  if( CSTR__self->meta.flags.state.priv.init && CSTR__other->meta.flags.state.priv.init ) {
    // Use QWORDS for comparison
    const QWORD *a = CStringValueAsQwords( CSTR__self );
    const QWORD * const end = a + CStringQwordLength( CSTR__self );
    const QWORD *b = CStringValueAsQwords( CSTR__other );
    // Compare all QWORDS
    while( a < end && *a == *b++ ) {
      ++a;
    }
    // Equal
    if( a == end ) {
      return true;
    }
    // Different
    return false;
  }

  // One or both not initialized
  return false;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_equals_chars( const CString_t *CSTR__self, const char *other ) {
  if( CSTR__self->meta.flags.state.priv.init && (strcmp( CStringValue(CSTR__self), other ) == 0) ) {
    return true;
  }
  return false;
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 ***********************************************************************
 */
static int32_t CString_find( const CString_t *CSTR__self, const char *probe, int32_t startindex ) {
  // Automatic match for empty probe
  if( *probe == '\0' ) {
    return 0;
  }

  CString_compress_header_t *compressed = CString_compressed_header( CSTR__self );

  int32_t sz = compressed ? compressed->orig.sz : CStringLength( CSTR__self );

  // Out of bounds startindex, no match
  if( startindex >= sz || startindex < -sz ) {
    return -1;
  }

  const char *data;
  char *decompressed = NULL;

  // Normal string
  if( !compressed ) {

    // Compute the startindex
    startindex = __abs_index( CSTR__self, startindex );

    // start probe at computed offset
    data = CStringValue(CSTR__self) + startindex;
  }
  // Compressed string
  else {
    // Decompress
    int32_t rsz, rucsz;
    CString_attr rattr;
    if( CString_decompress_to_bytes( CSTR__self, gt_decomp_buffer, decomp_bsz, &decompressed, &rsz, &rucsz, &rattr ) < 0 ) {
      return -1;
    }

    // Compute startindex
    startindex = __abs_index_bytes( decompressed, startindex );

    // start probe at computed offset
    data = decompressed + startindex;
  }


  // look for first occurrence at offset
  const char *firstocc = strstr( data, probe );

  if( decompressed && decompressed != gt_decomp_buffer ) {
    ALIGNED_FREE( decompressed );
  }

  if( !firstocc ) {
    return -1; // not found
  }
  
  // return position relative to search index
  return (int32_t)(firstocc - data);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_contains( const CString_t *CSTR__self, const char *probe ) {
  return CString_find( CSTR__self, probe, 0 ) >= 0 ? true : false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __slice( const char *data, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator ) {
  if( data == NULL ) {
    return NULL;
  }

  // Default [:]
  int32_t start = 0;
  int32_t end = INT_MAX;
  if( p_start != NULL ) {
    start = *p_start;
  }
  if( p_end != NULL ) {
    end = *p_end;
  }

  if( end > 0 && end <= start ) {
    return CStringNewSizeAttrAlloc( "", 0, 0, allocator, CSTRING_ATTR_NONE );
  }

  int64_t errpos;
  int64_t len = -1;
  int64_t uclen = -1;
  if( start < 0 || end < 0 ) {
    if( (uclen = COMLIB_length_utf8( (const BYTE*)data, -1, &len, &errpos )) < 0 ) {
      return NULL;
    }
  }

  // [a:?]
  int64_t ucstart;
  int64_t byte_offset;
  if( (byte_offset = (int32_t)COMLIB_offset_utf8( (const BYTE*)data, uclen, start, &ucstart, &errpos )) < 0 ) {
    return NULL;
  }

  // first utf-8 byte of slice
  const char *slice = data + byte_offset;
  int64_t uclen_rest = uclen - ucstart;

  // [?:b]
  int32_t idx_slice = end < 0 ? end : end >= (int)ucstart ? end - (int)ucstart : 0 ;
  int64_t uclen_slice;
  int64_t len_slice;
  if( (len_slice = COMLIB_offset_utf8( (const BYTE*)slice, uclen_rest, idx_slice, &uclen_slice, &errpos )) < 0 ) {
    return NULL;
  }

  return CStringNewSizeAttrAlloc( slice, (int)len_slice, (int)uclen_slice, allocator, CSTRING_ATTR_NONE );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_slice_with_allocator( const CString_t *CSTR__self, int32_t *p_start, int32_t *p_end, object_allocator_context_t *allocator ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  CString_t *CSTR__slice;
  BEGIN_EXTRACTED( CSTR__self, data ) {
    CSTR__slice = __slice( data, p_start, p_end, allocator );
  } END_EXTRACTED;

  return CSTR__slice;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_slice( const CString_t *CSTR__self, int32_t *p_start, int32_t *p_end ) {
  return CString_slice_with_allocator( CSTR__self, p_start, p_end, CSTR__self->allocator_context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_startswith( const CString_t *CSTR__self, const char *probe ) {
  return CString_find( CSTR__self, probe, 0 ) == 0 ? true : false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_endswith( const CString_t *CSTR__self, const char *probe ) {
  // find size of probe (no more than our own length since that is a miss anyway)
  int32_t sz_probe = (int32_t)strnlen( probe, CSTR__self->meta.size+1 );
  if( sz_probe > CSTR__self->meta.size ) {
    return false;
  }
  // is the probe exactly at end of our data?
  return CString_find( CSTR__self, probe, -sz_probe ) == 0 ? true : false;
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
static CString_t * __string_replace( const char *string, int32_t sz_string, const char *probe, const char *subst, object_allocator_context_t *allocator_context ) {
  CString_t *CSTR__replaced = NULL;

  // Sizes of the probe and substitution strings
  int32_t plen = (int32_t)strlen( probe );
  int32_t slen = (int32_t)strlen( subst );

  // Current amount of allocated bytes for output buffer
  int32_t sz = sz_string;
  if( sz < 0 ) {
    sz = (int32_t)strlen( string ); // fallback if no length provided
  }

  // Current length of data in output buffer
  int32_t len = 0;

  // Output buffer
  char *buffer = NULL;

  XTRY {
    // Best guess starting length for the resulting string (may end up smaller or larger than this)
    if( (buffer = (char*)malloc( sz + 1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x521 );
    }

    const char *src = string;
    const char *end = src + sz;
    char *dest = buffer;

    char *match = NULL;
    do {
      int32_t run;
      int32_t chunk_sz;

      // Look for first occurrence of probe at current point in source string
      if( (match = strstr( src, probe )) != NULL ) {
        // Number of characters between current scan starting point and next match
        run = (int32_t)(match - src);
        chunk_sz = run + slen;
      }
      // No match
      else {
        // number of characters remaining in source from cursor
        run = (int32_t)(end - src);
        chunk_sz = run;
      }

      // Check if buffer still has room, if not expand output buffer a little bit
      if( len + chunk_sz >= sz ) {
        void *mem;
        int new_sz = sz;
        while( new_sz <= len + 1 + chunk_sz ) { // +1 is for the possible empty probe match resulting in one additional char insert
          new_sz += 64;
        }
        if( (mem = realloc( buffer, new_sz+1 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x522 );
        }
        buffer = (char*)mem;
        sz = new_sz;
        dest = buffer + len;
      }

      // Copy all characters from source cursor to output cursor, up until the first occurrence of probe
      memcpy( dest, src, run );

      // Move cursors 
      dest += run;
      src += run;

      // If match, copy the substitution to output cursor
      if( match ) {
        memcpy( dest, subst, slen );
        dest += slen;
        src += plen;
        if( plen == 0 ) {
          if( (*dest++ = *src++) == '\0' ) {
            match = NULL; // trigger end condition after src has been exhausted
          }
          else {
            len++;
          }
        }
      }

      // Update the buffer length
      len += chunk_sz;

    } while( match );

    // Terminate buffer
    buffer[ len ] = '\0';

    // Create a new CString from buffer
    CString_constructor_args_t args = {
      .string       = buffer,
      .len          = len,
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = allocator_context
    };

    if( (CSTR__replaced = CString_constructor( NULL, &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x523 );
    }

  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( buffer ) {
      free( buffer );
    }
  }

  return CSTR__replaced;
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
static CString_t * CString_replace( const CString_t *CSTR__self, const char *probe, const char *subst ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  return __string_replace( CStringValue( CSTR__self ), CStringLength( CSTR__self ), probe, subst, CSTR__self->allocator_context );
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
static CString_t * CString_replace_with_allocator( const CString_t *CSTR__self, const char *probe, const char *subst, object_allocator_context_t *alloc ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  return __string_replace( CStringValue( CSTR__self ), CStringLength( CSTR__self ), probe, subst, alloc );
}




/*******************************************************************//**
 * Split the CString on the supplied split string and return an array
 * of new CStrings. The length of the returned array is placed in 'sz'
 * if not NULL. If 'splitstr' is NULL or the empty string "" then the
 * CString is split on whitespace.
 *
 ***********************************************************************
 */
static CString_t ** CString_split( const CString_t *CSTR__self, const char *splitstr, int32_t *sz ) {
  CString_constructor_args_t args = {0};
  args.alloc = CSTR__self->allocator_context;
  CString_t **CSTR__list = NULL;
  int32_t len = 0;
  const char *self = CStringValue( CSTR__self );
  int32_t sz_self = CStringLength( CSTR__self );
  const char *cursor = self;
  char c;
  int tok = 0;
  QWORD *boundaries = NULL;

  // List of integers defining token boundaries in pairs of (start_address, end_address)
  // E.g. the string " aa  bbb cc" starting at memory location 0x1000 split on whitespace
  // will generate the boundary list: [0x1001, 0x1003, 0x1005, 0x1008, 0x1009, 0x100B]
  CQwordList_t *token_map = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t );


  XTRY {
    if( token_map == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x531 );
    }

    int (*append)( CQwordList_t *list, const QWORD* item ) = CALLABLE( token_map )->Append;
    QWORD start_address = *(QWORD*)self;
    QWORD end_address = start_address + sz_self;
    const char *empty = "";

    // Non-default split
    if( splitstr && splitstr[0] != '\0' ) {
      size_t splitlen = strlen( splitstr );

      // Build token map
      while( *cursor != '\0' ) {
        if( strstr( cursor, splitstr ) == cursor ) {
          // End a token
          if( tok ) {
            append( token_map, (QWORD*)&cursor );
            tok = 0;
          }
          // Add empty string as token
          else {
            append( token_map, (QWORD*)&empty );
            append( token_map, (QWORD*)&empty );
          }
          ++len;
          // Advance cursor beyond split string
          cursor += splitlen;
          continue;
        }
        // Start a new token
        else if( !tok ) {
          append( token_map, (QWORD*)&cursor );
          tok = 1;
        }
        ++cursor;
      }
      // Add empty string as last token if not inside a token
      if( !tok ) {
        append( token_map, (QWORD*)&empty );
        append( token_map, (QWORD*)&empty );
        ++len;
      }
    }
    // Default split on whitespace
    else {
      // Build token map
      while( (c = *cursor) != '\0' ) {
        if( isspace( c ) ) {
          // End a token
          if( tok ) {
            append( token_map, (QWORD*)&cursor );
            tok = 0;
            ++len;
          }
        }
        // Start a new token
        else if( !tok ) {
          append( token_map, (QWORD*)&cursor );
          tok = 1;
        }
        ++cursor;
      }
    }
    // End last token
    if( tok ) {
      append( token_map, (QWORD*)&cursor );
      ++len;
    }


    // Allocate output array
    if( (CSTR__list = calloc( len+1, sizeof( CString_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x532 );
    }

    // Create substrings from token map and populate output array
    QWORD *address;
    if( (address = boundaries = CALLABLE( token_map )->YankBuffer( token_map )) != NULL ) {
      for( int32_t i=0; i<len; i++ ) {
        start_address = *address++;
        end_address = *address++;
        args.string = (const char*)start_address;
        args.len = (int32_t)(end_address - start_address);
        if( (CSTR__list[i] = CString_constructor( NULL, &args )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x533 );
        }
      }
    }
  }
  XCATCH( errcode ) {
    if( CSTR__list ) {
      CString_t **CSTR__cursor = CSTR__list;
      CString_t *CSTR__str;
      while( (CSTR__str = *CSTR__cursor++) != NULL ) {
        CStringDelete( CSTR__str );
      }
      free( CSTR__list );
      CSTR__list = NULL;
    }
  }
  XFINALLY {
    if( token_map ) {
      COMLIB_OBJECT_DESTROY( token_map );
    }
    if( boundaries ) {
      ALIGNED_FREE( boundaries );
    }
  }

  // Set array size if output pointer provided
  if( CSTR__list && sz ) {
    *sz = len;
  }

  return CSTR__list;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __join( const CString_t *CSTR__self, const CString_t **CSTR__list, object_allocator_context_t *alloc ) {
  CString_t *CSTR__join = NULL;
  
  int32_t sz = 0;
  int n = 0;
  const CString_t **CSTR__cursor = CSTR__list;
  while( *CSTR__cursor != NULL ) {
    sz += CStringLength( *CSTR__cursor++ );
    ++n;
  }

  // Edge case #1: empty list returns a new empty string
  if( n == 0 ) {
    return CStringNewAlloc( "", alloc );
  }
  
  // Edge case #2: single-item list is clone of single item
  if( n == 1 ) {
    return CStringCloneAlloc( CSTR__list[0], alloc );
  }

  const char *sep = CStringValue( CSTR__self );
  int32_t sz_sep = CStringLength( CSTR__self );


  // Total join length includes n-1 separators
  sz += (n-1) * sz_sep;

  CString_constructor_args_t args = {
    .string       = NULL,
    .len          = sz,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = alloc
  };


  if( (CSTR__join = CString_constructor( NULL, &args )) != NULL ) {
    char *dest = (char*)CString_modifiable_qwords( CSTR__join );
    // First item
    CSTR__cursor = CSTR__list;
    const char *src = CStringValue( *CSTR__cursor );
    const char *end = src + CStringLength( *CSTR__cursor );
    while( src < end ) {
      *dest++ = *src++;
    }
    // Inherit first string's user flags
    CSTR__join->meta.flags.state.user = (*CSTR__cursor)->meta.flags.state.user;

    // Rest of list
    while( *(++CSTR__cursor) != NULL ) {
      // Separator
      src = sep;
      end = sep + sz_sep;
      while( src < end ) {
        *dest++ = *src++;
      }
      // String
      src = CStringValue( *CSTR__cursor );
      end = src + CStringLength( *CSTR__cursor );
      while( src < end ) {
        *dest++ = *src++;
      }
    }
  }

  return CSTR__join;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_join( const CString_t *CSTR__self, const CString_t **CSTR__list ) {
  return __join( CSTR__self, CSTR__list, CSTR__self->allocator_context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_join_with_allocator( const CString_t *CSTR__self, const CString_t **CSTR__list, object_allocator_context_t *alloc ) {
  return __join( CSTR__self, CSTR__list, alloc );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const objectid_t * CString_obid( const CString_t *CSTR__self ) {
  return __get_obid( CSTR__self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_clone_with_allocator( const CString_t *CSTR__self, object_allocator_context_t *alloc ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  CString_constructor_args_t args = {
    .string       = NULL,
    .len          = CStringLength( CSTR__self ),
    .ucsz         = CStringCodepoints( CSTR__self ),
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = alloc
  };
  CString_t *CSTR__clone = CString_constructor( NULL, &args );
  if( CSTR__clone ) {
    // Copy from source into clone
    const QWORD *src = CStringValueAsQwords( CSTR__self );
    const QWORD * const end = src + CStringQwordLength( CSTR__self );
    QWORD *dest = CString_modifiable_qwords( CSTR__clone );
    while( src < end ) {
      *dest++ = *src++;
    }
    idcpy( POBID_FROM_OBJECT( CSTR__clone ), POBID_FROM_OBJECT( CSTR__self ) );
    CSTR__clone->meta.flags.state.user = CSTR__self->meta.flags.state.user;
  }
  return CSTR__clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_compress( const CString_t *CSTR__self, CString_compress_mode mode ) {
  return CString_compress_with_allocator( CSTR__self, CSTR__self->allocator_context, mode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_compress_with_allocator( const CString_t *CSTR__self, object_allocator_context_t *alloc, CString_compress_mode mode ) {

  // Extract fields
  const char *data = CStringValue( CSTR__self );
  int sz = CStringLength( CSTR__self );
  int ucsz = CStringCodepoints( CSTR__self );
  CString_attr attr = CStringAttributes( CSTR__self );

  // Compress
  return CStringNewCompressedAlloc( data, sz, ucsz, alloc, attr, mode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_decompress( const CString_t *CSTR__compressed ) {
  if( CSTR__compressed == NULL ) {
    return NULL;
  }

  CString_t *CSTR__orig = NULL;

  CString_attr csattr = CStringAttributes( CSTR__compressed );

  // Not compressed, return a clone instead
  if( (csattr & CSTRING_ATTR_COMPRESSED) == 0 ) {
    return CString_clone( CSTR__compressed );
  }

  char *decompressed = NULL;
  int sz = 0;
  int ucsz = 0;
  CString_attr attr = CSTRING_ATTR_NONE;

  XTRY {
    // Decompress into output variables
    if( CString_decompress_to_bytes( CSTR__compressed, gt_decomp_buffer, decomp_bsz, &decompressed, &sz, &ucsz, &attr ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Create new cstring object from decompressed data
    if( (CSTR__orig = CStringNewSizeAttrAlloc( decompressed, sz, ucsz, CSTR__compressed->allocator_context, attr )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

  }
  XCATCH( errcode ) {
    if( CSTR__orig ) {
      CStringDelete( CSTR__orig );
      CSTR__orig = NULL;
    }
  }
  XFINALLY {
    if( decompressed && decompressed != gt_decomp_buffer ) {
      ALIGNED_FREE( decompressed );
    }
  }

  return CSTR__orig;

}



/*******************************************************************//**
 *
 * Place the decompressed data into the supplied destinations:
 *    rdata : Automatically allocated (NOTE: Must use ALIGNED free)
 *            unless buffer != NULL and whose bsz is large enough to
 *            hold the result.
 *            DO NOT supply a pointer to already allocated space in rdata.
 *            Caller must check if the memory pointed to by *rdata is
 *            buffer, and if not it must ALIGNED_FREE() the result.
 *    rsz   : Number of bytes in string
 *    rucsz : Number of Unicode codepoint (or 0 if unknown/not Unicode)
 *    rattr : Any attributes that may be set on the cstring after decompression
 * 
 * Returns 0 on success, -1 on error (or was not compressed)
 * 
 ***********************************************************************
 */
DLL_EXPORT int CStringDecompressBytesToBytes( const char *compressed, int32_t csz, char *buffer, int32_t bsz, char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr ) {
  int ret = 0;
  QWORD *decompressed_buffer = NULL;

  // Compressed data including header
  const char *compressed_buffer = compressed;
  // Compressed header
  CString_compress_header_t *compressed_header = (CString_compress_header_t*)compressed_buffer;

  // Make sure we are really compressed
  if( csz < (int)sizeof( CString_compress_header_t ) || compressed_header->magic != CSTRING_COMPRESS_MAGIC ) {
    return -1; 
  }

  XTRY {
    // Original size plus nul term
    int qlen = qwcount( compressed_header->orig.sz + 1 );
    // Use supplied buffer
    if( buffer && bsz >= (8*qlen) ) {
      decompressed_buffer = (QWORD*)buffer;
    }
    // Allocate decompressed buffer with space for nul term
    else {
      CALIGNED_ARRAY_THROWS( decompressed_buffer, QWORD, qlen, 0x002 );
    }
    // Ensure last qword is zero
    decompressed_buffer[qlen-1] = 0;

    // Compressed data starts right after the header
    const char *cdata = compressed_buffer + sizeof( CString_compress_header_t );

    char *decompress_dest = (char*)decompressed_buffer;

    // Select decompression mode
    switch( compressed_header->compressed.mode ) {
    case CSTRING_COMPRESS_MODE_LZ4:
      if( LZ4_decompress_safe( cdata, decompress_dest, compressed_header->compressed.sz, compressed_header->orig.sz ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x003 );
      }
      break;
    case CSTRING_COMPRESS_MODE_LZ4HC:
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0xDDD, "TODO!" );
      break;
    default:
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x002, "bad compressed data: invalid mode (%08x)", (int)compressed_header->compressed.mode );
    }

    // Allocate uninitialized destination object
    if( compressed_header->orig.mode == CSTRING_COMPRESS_MODE_NONE ) {
      *rattr &= ~CSTRING_ATTR_COMPRESSED; // remove compressed bit if original was not compressed (the norm)
    }

    // Assign to return pointers
    *rdata = (char*)decompressed_buffer;
    *rsz = compressed_header->orig.sz;
    *rucsz = compressed_header->orig.ucsz;

  }
  XCATCH( error ) {
    if( decompressed_buffer && decompressed_buffer != (QWORD*)buffer ) {
      ALIGNED_FREE( decompressed_buffer );
    }
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 * Place the decompressed contents of cstring into the supplied destinations:
 *    data  : Automatically allocated (NOTE: Must use ALIGNED free)
 *            DO NOT supply a pointer to already allocated space
 *    sz    : Number of bytes in string
 *    ucsz  : Number of Unicode codepoint (or 0 if unknown/not Unicode)
 *    attr  : Any attributes that may be set on the cstring
 * 
 * Returns 0 on success, -1 on error (or was not compressed)
 * 
 ***********************************************************************
 */
static int CString_decompress_to_bytes( const CString_t *CSTR__compressed, char *buffer, int32_t bsz,  char **rdata, int32_t *rsz, int32_t *rucsz, CString_attr *rattr ) {
  if( CSTR__compressed == NULL ) {
    return -1;
  }

  return CStringDecompressBytesToBytes( CStringValue( CSTR__compressed ), CStringLength( CSTR__compressed ), buffer, bsz, rdata, rsz, rucsz, rattr );

  /*

  int ret = 0;

  QWORD *decompressed_buffer = NULL;

  XTRY {

    // Compressed data including header
    const char *compressed_buffer = CStringValue( CSTR__compressed );
    // Compressed header
    CString_compress_header_t *compressed_header = (CString_compress_header_t*)compressed_buffer;

    // Make sure we are really compressed
    CString_attr attr = CStringAttributes( CSTR__compressed );
    if( (attr & CSTRING_ATTR_COMPRESSED) == 0 || CStringLength( CSTR__compressed ) < (int)sizeof( CString_compress_header_t ) ) {
      // Not compressed, return raw data
      ret = CString_to_bytes( CSTR__compressed, rdata, rsz, rucsz, rattr );
      XBREAK;
    }

    // Verify compressed header
    if( compressed_header->magic != CSTRING_COMPRESS_MAGIC ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x001, "bad compressed data: invalid header" );
    }

    // Allocate decompressed buffer with space for nul term
    int qlen = qwcount( compressed_header->orig.sz + 1 );
    CALIGNED_ARRAY_THROWS( decompressed_buffer, QWORD, qlen, 0x002 );
    // Ensure last qword is zero
    decompressed_buffer[qlen-1] = 0;

    // Compressed data starts right after the header
    const char *cdata = compressed_buffer + sizeof( CString_compress_header_t );

    char *decompress_dest = (char*)decompressed_buffer;

    // Select decompression mode
    switch( compressed_header->compressed.mode ) {
    case CSTRING_COMPRESS_MODE_LZ4:
      if( LZ4_decompress_safe( cdata, decompress_dest, compressed_header->compressed.sz, compressed_header->orig.sz ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x003 );
      }
      break;
    case CSTRING_COMPRESS_MODE_LZ4HC:
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0xDDD, "TODO!" );
      break;
    default:
      THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x002, "bad compressed data: invalid mode (%08x)", (int)compressed_header->compressed.mode );
    }

    // Allocate uninitialized destination object
    if( compressed_header->orig.mode == CSTRING_COMPRESS_MODE_NONE ) {
      attr ^= CSTRING_ATTR_COMPRESSED; // remove compressed bit if original was not compressed (the norm)
    }


    // Assign to return pointers
    *rdata = (char*)decompressed_buffer;
    *rsz = compressed_header->orig.sz;
    *rucsz = compressed_header->orig.ucsz;
    *rattr = attr;

  }
  XCATCH( error ) {
    if( decompressed_buffer ) {
      ALIGNED_FREE( decompressed_buffer );
    }
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
  */
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool CString_is_compressed( const CString_t *CSTR__self ) {
  return CStringHasAttribute( CSTR__self, CSTRING_ATTR_COMPRESSED ) &&
         CStringLength( CSTR__self ) > (int)sizeof( CString_compress_header_t ) &&
         ((CString_compress_header_t*)CStringValue( CSTR__self ))->magic == CSTRING_COMPRESS_MAGIC;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_compress_header_t * CString_compressed_header( const CString_t *CSTR__self ) {
  return CString_is_compressed( CSTR__self ) ? (CString_compress_header_t*)CStringValue( CSTR__self ) : NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CString_get_true_length( const CString_t *CSTR__self, int32_t *rsz, int32_t *rucsz, CString_attr *rattr ) {
  CString_attr attr = CStringAttributes( CSTR__self );
  CString_compress_header_t *compressed = CString_compressed_header( CSTR__self );
  // Not compressed
  if( !compressed ) {
    *rsz = CStringLength( CSTR__self );
    *rucsz = CStringCodepoints( CSTR__self );
    *rattr = attr;
  }
  // Compressed
  else {
    *rsz = compressed->orig.sz;
    *rucsz = compressed->orig.ucsz;
    *rattr = attr &= ~CSTRING_ATTR_COMPRESSED;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_clone( const CString_t *CSTR__self ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  return CString_clone_with_allocator( CSTR__self, CSTR__self->allocator_context );
}



/*******************************************************************//**
 * Return a new CString consisting of the up to `sz` first characters 
 * in data. If data has codepoint information indicating utf-8 encoding,
 * the first `sz` codepoints are included in the prefix, otherwise the
 * first `sz` bytes are included in the prefix.
 ***********************************************************************
 */
static CString_t * __prefix( const char *data, int32_t len, int32_t uclen, int32_t sz, object_allocator_context_t *alloc ) {

  int32_t sz_prefix = sz < len ? sz : len;
  int32_t ucsz_prefix = 0;

  // Unicode codepoints are known
  if( uclen > 0 ) {
    int64_t errpos;
    // Get number of utf-8 bytes needed for `sz` codepoints
    int64_t actual_prefix_codepoints;
    int64_t actual_prefix_bytes = COMLIB_offset_utf8( (const BYTE*)data, len, sz, &actual_prefix_codepoints, &errpos );
    // UTF-8 was ok
    if( actual_prefix_bytes > 0 ) {
      sz_prefix = (int)actual_prefix_bytes;
      ucsz_prefix = (int)actual_prefix_codepoints;
    }
  }

  CString_constructor_args_t args = {
    .string       = NULL,
    .len          = sz_prefix,
    .ucsz         = ucsz_prefix,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = alloc
  };
  CString_t *CSTR__prefix = CString_constructor( NULL, &args );
  if( CSTR__prefix ) {
    // Copy from source into prefix string
    const char *src = data;
    char *dest = (char*)CStringValue( CSTR__prefix ); // remove const, we are going to write to this value location
    const char *end = src + sz_prefix;
    while( src < end ) {
      *dest++ = *src++;
    }
    // Construction complete
    CSTR__prefix->meta.flags.state.priv.init = 1;
  }
  return CSTR__prefix;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_prefix_with_allocator( const CString_t *CSTR__self, int32_t sz, object_allocator_context_t *alloc ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  CString_t *CSTR__prefix;
  BEGIN_EXTRACTED_LENGTH( CSTR__self, data, len, uclen ) {
    CSTR__prefix = __prefix( data, len, uclen, sz, alloc );
  } END_EXTRACTED;

  return CSTR__prefix;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * CString_prefix( const CString_t *CSTR__self, int32_t sz ) {
  return CString_prefix_with_allocator( CSTR__self, sz, CSTR__self->allocator_context );
}



/*******************************************************************//**
 *
 * NOTE: This method only works with normal NUL-terminated strings.
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * CStringNewPrefixAlloc( const char *string, int32_t sz, object_allocator_context_t *allocator ) {
  int64_t errpos;
  int64_t len;
  int64_t uclen = COMLIB_length_utf8( (const BYTE*)string, -1, &len, &errpos );
  if( uclen < 0 ) {
    uclen = len = strlen( string );
  }
  return __prefix( string, (int)len, (int)uclen, sz, allocator );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static Unicode CString_decode_utf8( const CString_t *CSTR__self ) {
  if( CSTR__self == NULL ) {
    return NULL;
  }

  Unicode U;

  BEGIN_EXTRACTED( CSTR__self, data ) {
    int64_t errpos = -1;
    U = COMLIB_decode_utf8( (const BYTE*)data, &errpos );
  } END_EXTRACTED;

  return U;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CString_print( const CString_t *CSTR__self ) {
  fprintf( stdout, "%s", CStringValue( CSTR__self ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __compute_min_buffer_size( int string_sz ) {
  return (string_sz + 63) & ~63;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __new_from_format( CString_constructor_args_t *input_args ) {
  CString_t *CSTR__self = NULL;
  char stack_buffer[64];
  char *buffer = stack_buffer;

  XTRY {
    int sz = sizeof( stack_buffer ) - 1;
    int nfmt;
    do {

      // Make a copy of the args.
      // Do this so we can re-use the args list and manage our own copy without
      // affecting upstream functions.
      va_list args_copy;
      va_copy( args_copy, *input_args->format_args );

      // Try the formatting with current buffer
      nfmt = vsnprintf( buffer, sz, input_args->format, args_copy );
      va_end( args_copy );

      // if it didn't work, try again with a larger, malloc'd buffer
      if( nfmt >= sz ) {
        if( buffer != stack_buffer ) {
          free( buffer ); // free previous malloc'd buffer before trying a larger buffer
        }

        sz = __compute_min_buffer_size( nfmt + 1 ); // Add room for the terminating null char
        if( (buffer = malloc( sz )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x541 );
        }
      }
      else if( nfmt < 0 ) {
        // Encoding error
        // TODO: Log encoding error?
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x542 );
      }
      // Formatting worked - now construct string
      else {
        CString_constructor_args_t cargs = {
          .string      = buffer,
          .len         = nfmt,
          .ucsz        = 0,
          .format      = NULL,
          .format_args = NULL,
          .alloc       = input_args->alloc
        };

        CSTR__self = CString_constructor( NULL, &cargs );
      }
    } while( !CSTR__self );
  }
  XCATCH( errcode ) {
    if( CSTR__self ) {
      CString_destructor( CSTR__self );
      CSTR__self = NULL; 
    }
  }
  XFINALLY {
    if( buffer != stack_buffer ) {
      free( buffer ); // free malloc'd buffer
    }
  }

  return CSTR__self;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_cxcstring.h"

test_descriptor_t _comlib_cxcstring_tests[] = {
  { "CString_t Basic Tests",               __utest_cxcstring_basic },
  {NULL}
};
#endif
