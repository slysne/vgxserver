/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_serialization.c
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

#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );

static int64_t _cxmalloc_serialization__persist_family_OPEN(       cxmalloc_family_t *family, bool force );
static int64_t _cxmalloc_serialization__serialize_family_FRO(      cxmalloc_family_t *family_RO, CQwordQueue_t *output, const char *family_txt );
static int64_t _cxmalloc_serialization__restore_family_FCS(        cxmalloc_family_t *family_CS );
static int64_t _cxmalloc_serialization__deserialize_family_FCS(    cxmalloc_family_t *family_CS, CQwordQueue_t *input );

static int64_t _cxmalloc_serialization__persist_allocator_ARO(     cxmalloc_allocator_t *allocator_RO, bool force );
static int64_t _cxmalloc_serialization__serialize_allocator_ARO(   cxmalloc_allocator_t *allocator_RO, CQwordQueue_t *output );
static int64_t _cxmalloc_serialization__restore_allocator_ACS(     cxmalloc_allocator_t *allocator_CS );
static int64_t _cxmalloc_serialization__deserialize_allocator_ACS( cxmalloc_allocator_t *allocator_CS, CQwordQueue_t *input );

static int64_t _cxmalloc_serialization__persist_block_ARO(         cxmalloc_block_t *block_RO, bool force );
static int64_t _cxmalloc_serialization__serialize_block_ARO(       cxmalloc_block_t *block_RO, CQwordQueue_t *output, bool force );
static int64_t _cxmalloc_serialization__restore_block_ACS(         cxmalloc_block_t *block_CS );
static int64_t _cxmalloc_serialization__deserialize_block_ACS(     cxmalloc_block_t *block_CS, CQwordQueue_t *input );

static int64_t _cxmalloc_serialization__remove_block_ARO(          const cxmalloc_allocator_t *allocator_RO, const cxmalloc_bidx_t bidx );



static                                      void __delete_line_serialization_context(    cxmalloc_line_serialization_context_t **context );
static   cxmalloc_line_serialization_context_t * __new_line_serialization_context_ARO(   cxmalloc_block_t *block_RO );
static                                      void __delete_line_deserialization_context(  cxmalloc_line_deserialization_context_t **context );
static cxmalloc_line_deserialization_context_t * __new_line_deserialization_context_ACS( cxmalloc_block_t *block_CS );



static     int64_t __new_buffer_and_tap_ARO( const cxmalloc_block_t *block_RO, QWORD **buffer, cxmalloc_object_tap_t *tap );

static     int64_t __serialize_lines_ARO(    cxmalloc_block_t *block_RO, cxmalloc_line_serialization_context_t *context, CQwordQueue_t *output );
static     int64_t __deserialize_lines_ACS(  cxmalloc_block_t *block_CS, size_t n_active, cxmalloc_line_deserialization_context_t *context, CQwordQueue_t *input );

static CString_t * __get_family_path_FRO(     cxmalloc_family_t *family_RO, const char *suffix );
static CString_t * __get_allocator_dat_ARO(  cxmalloc_allocator_t *allocator_RO );



DLL_HIDDEN _icxmalloc_serialization_t _icxmalloc_serialization = {
  .PersistFamily_OPEN   = _cxmalloc_serialization__persist_family_OPEN,
  .PersistAllocator_ARO = _cxmalloc_serialization__persist_allocator_ARO,
  .PersistBlock_ARO     = _cxmalloc_serialization__persist_block_ARO,
  .RestoreFamily_FCS    = _cxmalloc_serialization__restore_family_FCS,
  .RestoreAllocator_ACS = _cxmalloc_serialization__restore_allocator_ACS,
  .RestoreBlock_ACS     = _cxmalloc_serialization__restore_block_ACS,
  .RemoveBlock_ARO      = _cxmalloc_serialization__remove_block_ARO
};



static const QWORD __START_FILE[4] = {
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL,
  0xA110C00000000000ULL
};

static const QWORD __END_FILE[4] = {
  0xA110C00000000000ULL,
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL
};

static const QWORD __OBJECT_SEPARATOR = 0x5555555500000000ULL;
static const QWORD __OBJECT_ACTIVE    = 0xAAAAAAAA00000000ULL;
static const QWORD __OBJECT_IDLE      = 0x1111111100000000ULL;
static const QWORD __NO_AIDX          = 0xAAAAAAAA00000000ULL;
static const QWORD __END_ALLOCATORS   = 0xEEEEEEEEAAAAAAAAULL;
static const QWORD __NO_BIDX          = 0xBBBBBBBB00000000ULL;
static const QWORD __NO_BLOCK_DATA    = 0xDDDDDDDD00000000ULL;


typedef union __u_family_header_t {
  QWORD qwords[128];  // 1024 bytes header
  struct {
    // [1] START FILE
    QWORD start_marker[4];  //  [4] __START_FILE
    QWORD size;             //  [1]
    QWORD __rsv1[3];        //  [3]

    // [2] DESCRIPTOR
    // NOTE: we don't include:  meta.initval
    QWORD meta_ser_sz;      //  [1]
    QWORD obj_sz;           //  [1]
    QWORD obj_ser_sz;       //  [1]
    QWORD unit_sz;          //  [1]
    QWORD unit_ser_sz;      //  [1]
    QWORD param_block_sz;   //  [1]
    QWORD param_line_limit; //  [1]
    QWORD param_subdue;     //  [1]
    // [3] DESCRIPTOR cont'd
    QWORD param_ovsz;       //  [1]
    QWORD param_max_alloc;  //  [1]
    QWORD __rsv2[6];        //  [6]
    // NOTE: we don't include:  persist.path
    // NOTE: we don't include:  auxiliary
    // ---

    QWORD __rsv4[128 - 24];
  };
} __family_header_t;


typedef union __u_allocator_header_t {
  QWORD qwords[128];  // 1024 bytes header
  struct {
    // [1]
    QWORD start_marker[4];  //  [4] __START_FILE
    QWORD __rsv1[4];        //  [4]

    // [2]
    QWORD aidx;             //  [1] the allocator number within family
    QWORD num_blocks;       //  [1] the number of active blocks in allocator
    QWORD head_bidx;        //  [1] the first block in the block chain (where next allocation will be made from)
    QWORD last_reuse_bidx;  //  [1] the last block in a reuse chain
    QWORD last_hole_bidx;   //  [1] the last block in a hole chain
    QWORD __rsv2[3];        //  [3]

    // [3]
    QWORD shape[8];         //  [8] the datashape for this allocator

    // ---
    QWORD __rsv4[128-24];
  };
} __allocator_header_t;


typedef union __u_block_header_t {
  QWORD qwords[128];  // 1024 bytes header
  struct {
    // [1]
    QWORD start_marker[4];  //  [4] __START_FILE
    QWORD __rsv1[4];        //  [4]

    // [2]
    QWORD bidx;             //  [1] the block number within allocator
    QWORD quant;            //  [1] number of lines in block
    QWORD nactive;          //  [1] number of active lines in block
    QWORD allocated;        //  [1] non-zero if block data is allocated
    QWORD __rsv2[4];        //  [4]
    // ---
    QWORD __rsv3[128-16];
  };
} __block_header_t;


typedef union __u_block_info_t {
  QWORD qwords[3];
  struct {
    QWORD bidx;
    QWORD n_active;
    QWORD allocated;
  };
} __block_info_t;


typedef union __u_object_marker_t {
  QWORD qwords[3];
  struct {
    QWORD separator;  //
    QWORD number;     //
    QWORD active;     // 
  };
} __object_marker_t;








#define __FLUSH_THRESHOLD (1L<<19)   /* 512k QWORDS = 4MB -- we flush buffer to file at this point */


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define WRITE_STRUCT_OR_THROW( Output, Struct, ErrCode )                                  \
  do {                                                                                    \
    const QWORD *Qwords = (QWORD*)&(Struct);                                              \
    int64_t n = CALLABLE( Output )->WriteNolock( Output, Qwords, qwsizeof(Struct) );      \
    if( n != qwsizeof(Struct) ) {                                                         \
      THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                          \
    }                                                                                     \
  } WHILE_ZERO


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define WRITE_ARRAY_OR_THROW( Output, QwordArray, N, ErrCode )                            \
  do {                                                                                    \
    int64_t nw = CALLABLE( Output )->WriteNolock( Output, QwordArray, N );                \
    if( nw != N ) {                                                                       \
      THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                          \
    }                                                                                     \
  } WHILE_ZERO


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define READ_STRUCT_OR_THROW( Input, Struct, ErrCode )                                    \
  do {                                                                                    \
    QWORD *ps = (QWORD*)&(Struct);                                                        \
    int64_t n = CALLABLE( Input )->ReadNolock( Input, (void**)&ps, qwsizeof(Struct) );    \
    if( n != qwsizeof(Struct) ) {                                                         \
      THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                          \
    }                                                                                     \
  } WHILE_ZERO


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define READ_ARRAY_OR_THROW( Input, QwordArray, N, ErrCode )                              \
  do {                                                                                    \
    QWORD *pa = QwordArray;                                                               \
    int64_t nr = CALLABLE( Input )->ReadNolock( Input, (void**)&pa, N );                  \
    if( nr != N ) {                                                                       \
      THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                          \
    }                                                                                     \
  } WHILE_ZERO


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define EXPECT_ARRAY_OR_THROW( QwordArray, Expected, N, ErrCode )                         \
  do {                                                                                    \
    SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY                                           \
    if( memcmp( (QwordArray), (Expected), sizeof(QWORD)*N ) ) {                           \
      THROW_ERROR( CXLIB_ERR_CORRUPTION, ErrCode );                                       \
    }                                                                                     \
  } WHILE_ZERO


/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define EXPECT_QWORD_OR_THROW( Qword, Expected, ErrCode )                                 \
  do {                                                                                    \
    SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY                                           \
    if( (Qword) != (Expected) ) {                                                         \
      THROW_ERROR( CXLIB_ERR_CORRUPTION, ErrCode );                                       \
    }                                                                                     \
  } WHILE_ZERO



/***********************************************************************/
/***********************************************************************/
/***** F A M I L Y *****************************************************/
/***********************************************************************/
/***********************************************************************/



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t * __get_family_path_FRO( cxmalloc_family_t *family_RO, const char *suffix ) {
  const char *prefix = "aset";
  CString_t *CSTR__family = NULL;
  XTRY {
    // Get the family data file full path
    const char *familypath = CStringValue( family_RO->descriptor->persist.CSTR__path );

    // Construct the full path to the family file
    if( (CSTR__family = CStringNewFormat( "%s/%s_[%u-%u].%s", familypath, prefix, family_RO->min_length, family_RO->max_length, suffix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x101 );
    }
  }
  XCATCH( errcode ) {
    if( CSTR__family ) {
      CStringDelete( CSTR__family );
      CSTR__family = NULL;
    }
  }
  XFINALLY {
  }
  return CSTR__family;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__persist_family_OPEN( cxmalloc_family_t *family, bool force ) {
  int64_t nqwords = 0;
  CString_t *CSTR__family_dat = NULL;
  CString_t *CSTR__family_txt = NULL;
  CQwordQueue_t *output = NULL;
  int64_t n;

  // Enter READONLY mode for family during serialization
  BEGIN_CXMALLOC_FAMILY_READONLY( family ) {

    XTRY {
      // Persist if we're in persistent mode
      if( family_RO->descriptor->persist.CSTR__path ) {

        // Get the family data file full path
        const char *familypath = CStringValue( family_RO->descriptor->persist.CSTR__path );

        // Create the family base directory if it doesn't exist
        if( !dir_exists( familypath ) ) {
          if( create_dirs( familypath ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x111 );
          }
        }
        
        // Get the family data file full path
        if( (CSTR__family_dat = __get_family_path_FRO( family_RO, "dat" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x112 );
        }
        // Get the family info file full path
        if( (CSTR__family_txt = __get_family_path_FRO( family_RO, "adoc" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x113 );
        }

        const char *family_dat = CStringValue( CSTR__family_dat );
        const char *family_txt = CStringValue( CSTR__family_txt );

        // Create family output queue and file
        if( (output = CQwordQueueNewOutput( 1L<<16, family_dat )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x114 );
        }

#ifdef CXMALLOC_CONSISTENCY_CHECK
        if( CALLABLE( family_RO )->Check( family_RO ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x115 );
        }
#endif

        // Run the family data serializer
        if( (n = _cxmalloc_serialization__serialize_family_FRO( family_RO, output, family_txt )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x116 );
        }
        nqwords += n;

        // Traverse all allocators in family
        for( int aidx=0; aidx < family_RO->size; aidx++ ) {
          cxmalloc_allocator_t *allocator = family_RO->allocators[ aidx ];
          if( allocator ) {
            BEGIN_CXMALLOC_ALLOCATOR_READONLY( allocator ) {
              // Clean up the READONLY allocator
              _icxmalloc_chain.RemoveHoles_OPEN( allocator_RO );
              
              // Persist the READONLY allocator
              n = _cxmalloc_serialization__persist_allocator_ARO( allocator_RO, force );
            } END_CXMALLOC_ALLOCATOR_READONLY;

            if( n < 0 ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x117 );
            }

            nqwords += n;
          }
        }
      }
    }
    XCATCH( errcode ) {
      nqwords = -1;
    }
    XFINALLY {
      if( output ) {
        COMLIB_OBJECT_DESTROY( output );
      }
      if( CSTR__family_dat ) {
        CStringDelete( CSTR__family_dat );
      }
      if( CSTR__family_txt ) {
        CStringDelete( CSTR__family_txt );
      }

    }
  } END_CXMALLOC_FAMILY_READONLY;

  return nqwords;
}



/*******************************************************************//**
 * Restore Family
 * 
 * Returns: > 0 when family was restored from file
 *            0 when no file exists
 *           -1 on error
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__restore_family_FCS( cxmalloc_family_t *family_CS ) {
  int64_t nqwords = 0;
  CString_t *CSTR__family_dat = NULL;
  CQwordQueue_t *input = NULL;
  int64_t n;

  XTRY {
    // Restore if we're in persistent mode
    if( family_CS->descriptor->persist.CSTR__path ) {

      // the family directory full path
      const char *familypath = CStringValue( family_CS->descriptor->persist.CSTR__path );

      // Does the family directory exist?
      if( dir_exists( familypath ) ) {

        // Get the family data file full path
        if( (CSTR__family_dat = __get_family_path_FRO( family_CS, "dat" )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x121 );
        }

        // the family data file full path
        const char *family_dat = CStringValue( CSTR__family_dat );

        // Does the family data file exist ?
        if( file_exists( family_dat ) ) {

          // PERSISTED FAMILY EXISTS
          // NOW RESTORE.

          CXMALLOC_VERBOSE( 0x122, "Restoring: %s", family_dat );

          // Create family input queue
          if( (input = CQwordQueueNewInput( 1L<<16, family_dat )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x122 );
          }

          // Run the family deserializer
          if( (n = _cxmalloc_serialization__deserialize_family_FCS( family_CS, input )) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x123 );
          }
          nqwords += n;
        }
      }
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( input ) {
      COMLIB_OBJECT_DESTROY( input );
    }
    if( CSTR__family_dat ) {
      CStringDelete( CSTR__family_dat );
    }
  }
  
  return nqwords;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static const char * __get_bar( const cxmalloc_block_t *block, const char *source, int64_t sz, int64_t count ) {
  double capacity = (double)block->capacity;
  int64_t offset = sz;
  if( capacity > 0 ) {
    offset = sz - (intptr_t)round( sz * (count < 0 ? 0 : count) / capacity );
  }
  return source + offset;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __adjust_bar( const char **segments[], int64_t maxbar, const char *inner, const char *outer ) {
  int64_t sumbar;
  while( (sumbar = (outer + maxbar - *segments[0]) + (inner + maxbar - *segments[1]) + (outer + maxbar - *segments[2])) != maxbar ) {
    int64_t delta = maxbar - sumbar;
    if( delta > 0 ) {
      if( *segments[0] > outer ) {
        --*segments[0];
      }
      else if( *segments[2] > outer ) {
        --*segments[2];
      }
      else if( *segments[1] > inner ) {
        --*segments[1];
      }
      else {
        break; // give up
      }
    }
    else {
      if( *segments[0] < outer + maxbar ) {
        ++*segments[0];
      }
      else if( *segments[2] < outer + maxbar ) {
        ++*segments[2];
      }
      else if( *segments[1] < inner + maxbar ) {
        ++*segments[1];
      }
      else {
        break; // give up
      }
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char * __final_bar( char *buffer, const char *on, const char *off, const char *bar ) {
  size_t sz_on = strlen( on );
  size_t sz_off = strlen( off );
  char *dest = buffer;
  const char *src = bar;
  char c;
  while( (c = *src++) != '\0' ) {
    switch( c ) {
    case '*':
      strcpy( dest, on );
      dest += sz_on;
      break;
    case '_':
      strcpy( dest, off );
      dest += sz_off;
      break;
    default:
      *dest++ = c;
    }
  }
  *dest = '\0';
  return buffer;
}



/*******************************************************************//**
 * Write human readable family summary
 * 
 ***********************************************************************
 */
static void _cxmalloc_serialization__write_family_summary_FRO( cxmalloc_family_t *family_RO, const char *filepath, __family_header_t *header ) {
  FILE *afile = NULL; // ascii file

#define WRITE_LINE( FormatStr, ... )  fprintf( afile, FormatStr"\n", ##__VA_ARGS__ )

#define WRITE_FORMAT( FormatStr, ... )  fprintf( afile, FormatStr, ##__VA_ARGS__ )

#define ADOC_TABLE_HEAD( Col1, Sz1, Col2, Sz2 )   \
  WRITE_LINE( "[cols=\"%d,%d\"]", Sz1, Sz2 );     \
  WRITE_LINE( "|===" );                           \
  WRITE_LINE( "|%s |%s", Col1, Col2 );            \
  WRITE_LINE( "" )

#define ADOC_TABLE_END  \
  WRITE_LINE( "|===" ); \
  WRITE_LINE( "" )

#define ADOC_ROW_INT( Key, IntVal )           \
  WRITE_LINE( "|%s", (Key) );                 \
  WRITE_LINE( "|%lld", (int64_t)(IntVal));    \
  WRITE_LINE( "" )

#define ADOC_ROW_UINT( Key, UIntVal )         \
  do {                                        \
    uint64_t val = UIntVal;                   \
    WRITE_LINE( "|%s", (Key) );               \
    WRITE_LINE( "|%llu", val );               \
    WRITE_LINE( "" );                         \
  } WHILE_ZERO

#define ADOC_ROW_STRING( Key, StrVal )  \
  WRITE_LINE( "|%s", (Key) );           \
  WRITE_LINE( "|%s", (StrVal) );        \
  WRITE_LINE( "" )

  // Open ascii file
  if( filepath ) {
    if( (afile = CX_FOPEN( filepath, "w" )) == NULL ) {
      return;
    }
  }
  // Use stderr
  else {
    afile = stderr;
  }

  // Write header
  WRITE_LINE( "[[ALLOCATOR_FAMILY]]" );
  WRITE_LINE( "= Allocator Family" );
  WRITE_LINE( "" );
  WRITE_LINE( "[[Header]]" );
  WRITE_LINE( "== Header" );
  ADOC_TABLE_HEAD(  "Attribute", 2,         "Value", 8 );
  ADOC_ROW_STRING(  "family",               family_RO->obid.longstring.string );
  ADOC_ROW_UINT(    "size",                 header->size );
  ADOC_ROW_UINT(    "meta_ser_sz",          header->meta_ser_sz );
  ADOC_ROW_UINT(    "obj_sz",               header->obj_sz );
  ADOC_ROW_UINT(    "obj_ser_sz",           header->obj_ser_sz );
  ADOC_ROW_UINT(    "unit_sz",              header->unit_sz );
  ADOC_ROW_UINT(    "unit_ser_sz",          header->unit_ser_sz );
  ADOC_ROW_UINT(    "param_block_sz",       header->param_block_sz );
  ADOC_ROW_UINT(    "param_line_limit",     header->param_line_limit );
  ADOC_ROW_UINT(    "param_subdue",         header->param_subdue );
  ADOC_ROW_UINT(    "param_ovsz",           header->param_ovsz );
  ADOC_ROW_UINT(    "param_max_alloc",      header->param_max_alloc );
  ADOC_TABLE_END;

  // Allocators
  WRITE_LINE( "[[Allocators]]" );
  WRITE_LINE( "== Allocators" );

  for( int aidx=0; aidx < family_RO->size; aidx++ ) {
    cxmalloc_allocator_t *allocator_RO = family_RO->allocators[ aidx ];
    if( allocator_RO ) {
      cxmalloc_datashape_t shape = allocator_RO->shape;
      WRITE_LINE( "[[Allocator_%d]]", aidx );
      WRITE_LINE( "=== Allocator %d", aidx );
      WRITE_LINE( "[[Shape_%d]]", aidx );
      WRITE_LINE( "==== Shape (aidx=%d)", aidx );
      ADOC_TABLE_HEAD(  "Attribute", 2,         "Value", 8 );
      ADOC_ROW_UINT(    "linemem.awidth",       shape.linemem.awidth );
      ADOC_ROW_UINT(    "linemem.unit_sz",      shape.linemem.unit_sz );
      ADOC_ROW_UINT(    "linemem.chunks",       shape.linemem.chunks );
      ADOC_ROW_UINT(    "linemem.pad",          shape.linemem.pad );
      ADOC_ROW_UINT(    "linemem.qwords",       shape.linemem.qwords );
      ADOC_ROW_UINT(    "blockmem.chunks",      shape.blockmem.chunks );
      ADOC_ROW_UINT(    "blockmem.pad",         shape.blockmem.pad );
      ADOC_ROW_UINT(    "blockmem.quant",       shape.blockmem.quant );
      ADOC_ROW_UINT(    "serialized.meta_sz",   shape.serialized.meta_sz );
      ADOC_ROW_UINT(    "serialized.obj_sz",    shape.serialized.obj_sz );
      ADOC_ROW_UINT(    "serialized.unit_sz",   shape.serialized.unit_sz );
      ADOC_TABLE_END;

      WRITE_LINE( "[[BlockChain_%d]]", aidx );
      WRITE_LINE( "==== Block Chain (aidx=%d)", aidx );
      const cxmalloc_block_t *block = NULL;
      // Scan back to start of chain
      if( allocator_RO->head && (block = *allocator_RO->head) != NULL ) {
        while( block->prev_block ) {
          block = block->prev_block;
        }
      }
      // Traverse chain until end
      ADOC_TABLE_HEAD(  "Role", 2,         "Block", 8 );
      while( block ) {
        char buffer[16];
        const char *mark = block == *allocator_RO->head ? "`HEAD`" : block == allocator_RO->last_reuse ? "`LAST_REUSE`" : block == allocator_RO->last_hole ? "`LAST_HOLE`" : "";
        snprintf( buffer, 15, "`[%04x]`", block->bidx );
        ADOC_ROW_STRING(  mark,               buffer );
        block = block->next_block;
      }
      ADOC_ROW_STRING(  "`NULL`",            "" );
      ADOC_TABLE_END;

      WRITE_LINE( "" );
      WRITE_LINE( "[[BlockRegisters_%d]]", aidx );
      WRITE_LINE( "==== Block Registers (aidx=%d )", aidx );

      static const char active[]   = "************************************************************";
      static const char inactive[] = "____________________________________________________________";
      static const char empty[]    = "";
      static const int64_t maxbar = sizeof( active ) - 1;
      const char *outer, *inner;
      cxmalloc_block_t **cursor = allocator_RO->blocks;
      cxmalloc_linehead_t **p1, **p2;
      const char *s1, *s2, *s3;
      const char **segments[] = { &s1, &s2, &s3 };
      if( cursor ) {
        WRITE_LINE( "[cols=\"1,16,1,1,1,1,1\"]" );
        WRITE_LINE( "|===" );
        WRITE_LINE( "|bidx |data |prev |next |use |use %% |role " );
        WRITE_LINE( "" );

        int64_t n_capacity = 0;
        int64_t n_active = 0;
        while( cursor < allocator_RO->space ) {
          block = *cursor++;

          int64_t cap = block->capacity;
          int64_t use = cap - block->available;

          if( block->reg.put < block->reg.get ) {
            p1 = block->reg.put;
            p2 = block->reg.get;
            outer = inactive;
            inner = active;
          }
          else {
            p1 = block->reg.get;
            p2 = block->reg.put;
            outer = active;
            inner = inactive;
          }
          if( p1 == NULL ) {
            p1 = p2;
          }
          s1 = __get_bar( block, outer, maxbar, p1 - block->reg.top );
          s2 = __get_bar( block, inner, maxbar, p2 - p1 );
          s3 = __get_bar( block, outer, maxbar, block->reg.bottom - p2 );
          if( cap > 0 ) {
            __adjust_bar( segments, maxbar, inner, outer );
          }
          else {
            s1 = s2 = s3 = empty;
          }

          WRITE_LINE( "|`%04x`", block->bidx );

          double use_pct = cap > 0 ? 100.0 * use / cap : 0.0;
          SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
          const char *mark = block == *(allocator_RO->head) ? "HEAD" : block == allocator_RO->last_reuse ? "LAST_REUSE" : block == allocator_RO->last_hole ? "LAST_HOLE" : cap > 0 ? "BLOCK" : "HOLE";
          // data
          if( cap > 0 ) {
            const char on[] = "&#9608;";
            const char off[] = "&#9617;";
            char buf1[512];
            char buf2[512];
            char buf3[512];
            WRITE_LINE( "|`+++%s%s%s+++`", __final_bar( buf1, on, off, s1 ), __final_bar( buf2, on, off, s2 ), __final_bar( buf3, on, off, s3 ) );
          }
          else {
            WRITE_LINE( "|" );
          }

          // prev
          if( block->prev_block ) {
            WRITE_FORMAT( "|`%04x`", block->prev_block->bidx );
          }
          else {
            WRITE_LINE( "|" );
          }

          // next
          if( block->next_block ) {
            WRITE_LINE( "|`%04x`", block->next_block->bidx );
          }
          else {
            WRITE_LINE( "|" );
          }

          // use
          WRITE_LINE( "|`%lld`", use );

          // use %
          WRITE_LINE( "|`%.1f%%`", use_pct );

          // role
          if( *mark ) {
            WRITE_LINE( "|`%s`", mark );
          }
          else {
            WRITE_LINE( "|" );
          }
          WRITE_LINE( "" );

          n_active += use;
          n_capacity += cap;
        }
        ADOC_TABLE_END;

        WRITE_LINE( "[cols=\"5,5,5\"]" );
        WRITE_LINE( "|===" );
        WRITE_LINE( "|Use |Capacity |Use %%" );
        WRITE_LINE( "" );

        WRITE_LINE( "|`%lld`", n_active );
        WRITE_LINE( "|`%lld`", n_capacity );
        WRITE_LINE( "|`%.1f%%`", n_capacity > 0 ? 100.0 * n_active / n_capacity : 0.0 );
        WRITE_LINE( "" );
        ADOC_TABLE_END;
      }

      WRITE_LINE( "" );
    }
  }

  if( filepath ) {
    CX_FCLOSE( afile );
  }

#undef WRITE_LINE
#undef ADOC_TABLE_HEAD
#undef ADOC_TABLE_END
#undef ADOC_ROW_INT
#undef ADOC_ROW_STRING
#undef ALLOCATOR_BYTES

}




/*******************************************************************//**
 * Serialize Family
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__serialize_family_FRO( cxmalloc_family_t *family_RO, CQwordQueue_t *output, const char *family_txt ) {
  int64_t nwritten = 0;
  __family_header_t *header = NULL;
  CString_t *CSTR__family_name = NULL;
  int64_t n;

  XTRY {
    const cxmalloc_descriptor_t *desc = family_RO->descriptor;

    if( (CSTR__family_name = CStringNew( family_RO->obid.longstring.string )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x131 );
    }

    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( header, __family_header_t );
    if( header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x132 );
    }
    memset( header, 0, sizeof( __family_header_t ) );

    // Start marker
    memcpy( header->start_marker, __START_FILE, sizeof( __START_FILE ) );
    // Size
    header->size = family_RO->size;
    // Descriptor
    header->meta_ser_sz = desc->meta.serialized_sz;
    header->obj_sz = desc->obj.sz;
    header->obj_ser_sz = desc->obj.serialized_sz;
    header->unit_sz = desc->unit.sz;
    header->unit_ser_sz = desc->unit.serialized_sz;
    header->param_block_sz = desc->parameter.block_sz;
    header->param_line_limit = desc->parameter.line_limit;
    header->param_subdue = desc->parameter.subdue;
    header->param_ovsz = desc->parameter.allow_oversized;
    header->param_max_alloc = desc->parameter.max_allocators;
    WRITE_ARRAY_OR_THROW( output, header->qwords, qwsizeof( __family_header_t ), 0x133 );
    nwritten += qwsizeof( __family_header_t );

    // -----------------
    // ACTIVE ALLOCATORS
    // -----------------
    for( int aidx=0; aidx < family_RO->size; aidx++ ) {
      cxmalloc_allocator_t *allocator_RO = family_RO->allocators[ aidx ];
      QWORD qword;
      if( allocator_RO ) {
        qword = aidx;
      }
      else {
        qword = __NO_AIDX;
      }
      WRITE_ARRAY_OR_THROW( output, &qword, 1, 0x134 );
      nwritten += 1;
    }
    WRITE_ARRAY_OR_THROW( output, &__END_ALLOCATORS, 1, 0x135 );
    nwritten += 1;

    // ----------------
    // FAMILY OBID NAME
    // ----------------
    if( (n = COMLIB_OBJECT_SERIALIZE( CSTR__family_name, output )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x136 );
    }
    nwritten += n;

    // ---
    // END
    // ---
    WRITE_ARRAY_OR_THROW( output, __END_FILE, qwsizeof( __END_FILE ), 0x137 );
    nwritten += qwsizeof( __END_FILE );


    // ------------------
    // CX_WRITE TEXT SUMMARY
    // ------------------
    _cxmalloc_serialization__write_family_summary_FRO( family_RO, family_txt, header );


  }
  XCATCH( errcode ) {
    nwritten = -1;
  }
  XFINALLY {
    if( header ) {
      ALIGNED_FREE( header );
    }
    if( CSTR__family_name ) {
      CStringDelete( CSTR__family_name );
    }
  }
  
  return nwritten;

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__deserialize_family_FCS( cxmalloc_family_t *family_CS, CQwordQueue_t *input ) {
  int64_t nread = 0;
  __family_header_t *header = NULL;
  CString_t *CSTR__family_name = NULL;
  CString_t *CSTR__stored_family_name = NULL;

  XTRY {
    int (*next)( CQwordQueue_t *input, QWORD *dest ) = CALLABLE( input )->Next;
    const cxmalloc_descriptor_t *desc = family_CS->descriptor;
    QWORD qword;

    if( (CSTR__family_name = CStringNew( family_CS->obid.longstring.string )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x141 );
    }

    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( header, __family_header_t );
    if( header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x142 );
    }
    memset( header, 0, sizeof( __family_header_t ) );

    READ_ARRAY_OR_THROW( input, header->qwords, qwsizeof( __family_header_t ), 0x143 );
    nread += qwsizeof( __family_header_t );
    // Start marker
    EXPECT_ARRAY_OR_THROW( header->start_marker, __START_FILE, qwsizeof( __START_FILE ), 0x144 );
    // Validate Size
    EXPECT_QWORD_OR_THROW( header->size, (QWORD)family_CS->size, 0x145 );
    // Validate Descriptor
    EXPECT_QWORD_OR_THROW( header->meta_ser_sz, desc->meta.serialized_sz, 0x146 );
    EXPECT_QWORD_OR_THROW( header->obj_sz, desc->obj.sz, 0x147 );
    EXPECT_QWORD_OR_THROW( header->obj_ser_sz, desc->obj.serialized_sz, 0x148 );
    EXPECT_QWORD_OR_THROW( header->unit_sz, desc->unit.sz, 0x149 );
    EXPECT_QWORD_OR_THROW( header->unit_ser_sz, desc->unit.serialized_sz, 0x14A );
    EXPECT_QWORD_OR_THROW( header->param_block_sz, desc->parameter.block_sz, 0x14B );
    EXPECT_QWORD_OR_THROW( header->param_line_limit, desc->parameter.line_limit, 0x14C );
    EXPECT_QWORD_OR_THROW( header->param_subdue, desc->parameter.subdue, 0x14D );
    EXPECT_QWORD_OR_THROW( header->param_ovsz, (QWORD)desc->parameter.allow_oversized, 0x14E );
    EXPECT_QWORD_OR_THROW( header->param_max_alloc, (QWORD)desc->parameter.max_allocators, 0x14F );

    // -----------------
    // ACTIVE ALLOCATORS
    // -----------------
    for( uint16_t aidx=0; aidx < family_CS->size; aidx++ ) {
      // Read the next allocator number
      if( next( input, &qword ) == 0 || qword == __END_ALLOCATORS ) {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x150 );
      }
      nread += 1;

      // Active allocator - restore it
      if( qword == aidx ) {
        if( (family_CS->allocators[ aidx ] = _icxmalloc_allocator.CreateAllocator_FCS( family_CS, aidx )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x151 );
        }
      }
      // Allocator not active - skip
      else if( qword == __NO_AIDX ) {
      }
      // Bad data
      else {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x152 );
      }
    }
    if( next( input, &qword ) == 0 ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x153 );
    }
    nread += 1;
    EXPECT_QWORD_OR_THROW( qword, __END_ALLOCATORS, 0x154 );

    // ----------------
    // FAMILY OBID NAME
    // ----------------
    // Load the stored family name
    CStringSetSerializationCurrentThread( NULL, NULL ); // use the defaults 
    if( (CSTR__stored_family_name = (CString_t*)COMLIB_CLASS_VTABLE( CString_t )->vm_deserialize( NULL, input )) == NULL ) { 
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x155 );
    }
    // Validate family name
    if( !CStringEquals( CSTR__stored_family_name, CSTR__family_name ) ) {
      CXMALLOC_WARNING( 0x156, "Deserializing allocator family, expected '%s', found '%s'.", CStringValue( CSTR__family_name ), CStringValue( CSTR__stored_family_name ) );
    }
    nread += qwsizeof( CString_meta_t ) + qwcount( CSTR__stored_family_name->meta.size + 1 ); // (hackish - find a better way to get the number of qwords)

    // ---
    // END
    // ---
    QWORD end[ qwsizeof(__END_FILE) ];
    READ_ARRAY_OR_THROW( input, end, qwsizeof(end), 0x157 );
    nread += qwsizeof(end);
    EXPECT_ARRAY_OR_THROW( end, __END_FILE, qwsizeof( __END_FILE ), 0x158 );
  }
  XCATCH( errcode ) {
    nread = -1;
  }
  XFINALLY {
    if( header ) {
      ALIGNED_FREE( header );
    }
    if( CSTR__family_name ) {
      CStringDelete( CSTR__family_name );
    }
    if( CSTR__stored_family_name ) {
      CStringDelete( CSTR__stored_family_name );
    }
  }
  
  return nread;
}



/***********************************************************************/
/***********************************************************************/
/***** A L L O C A T O R ***********************************************/
/***********************************************************************/
/***********************************************************************/



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t * __get_allocator_dat_ARO( cxmalloc_allocator_t *allocator_RO ) {
  const char *prefix = "ax";
  const char *suffix = "dat";
  CString_t *CSTR__allocator_dat = NULL;
  XTRY {
    // Get the allocator data file full path
    const char *allocdir = CStringValue( allocator_RO->CSTR__allocdir );
    if( (CSTR__allocator_dat = CStringNewFormat( "%s/%s_%04x.%s", allocdir, prefix, allocator_RO->aidx, suffix )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x161 );
    }
  }
  XCATCH( errcode ) {
    if( CSTR__allocator_dat ) {
      CStringDelete( CSTR__allocator_dat );
      CSTR__allocator_dat = NULL;
    }
  }
  XFINALLY {
  }
  return CSTR__allocator_dat;
}



/*******************************************************************//**
 * Persist Allocator
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__persist_allocator_ARO( cxmalloc_allocator_t *allocator_RO, bool force ) {
  int64_t nqwords = 0;
  CString_t *CSTR__allocator_dat = NULL;
  CQwordQueue_t *output = NULL;
  CQwordQueue_t *input = NULL;
  int64_t n;
  __allocator_header_t *prev_header = NULL;

  XTRY {
    // Persist if we're in persistent mode
    if( allocator_RO->CSTR__allocdir ) {

      const char *allocdir = CStringValue( allocator_RO->CSTR__allocdir );
      const char *allocfile = NULL;
      int n_prev_blocks = 0;

      // Create the allocator directory if it doesn't exist
      if( !dir_exists( allocdir ) ) {
        if( create_dirs( allocdir ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x171 );
        }
      }

      // Get the allocator data file full path
      if( (CSTR__allocator_dat = __get_allocator_dat_ARO( allocator_RO )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x172 );
      }

      allocfile = CStringValue( CSTR__allocator_dat );

      // Get info about any previously stored data
      if( file_exists( allocfile ) ) {
        if( (input = CQwordQueueNewInput( 1L<<16, allocfile )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x173 );
        }
        CALIGNED_MALLOC( prev_header, __allocator_header_t );
        if( prev_header == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x174 );
        }
        memset( prev_header, 0, sizeof( __allocator_header_t ) );

        // Read the header to get the number of blocks in previous snapshot
        READ_ARRAY_OR_THROW( input, prev_header->qwords, qwsizeof( __allocator_header_t ), 0x175 );
        EXPECT_ARRAY_OR_THROW( prev_header->start_marker, __START_FILE, qwsizeof( __START_FILE ), 0x176 );
        EXPECT_QWORD_OR_THROW( prev_header->aidx, allocator_RO->aidx, 0x177 );
        n_prev_blocks = (int)prev_header->num_blocks;
        COMLIB_OBJECT_DESTROY( input );
        input = NULL;
      }

      // Create allocator output queue and file
      if( (output = CQwordQueueNewOutput( 1L<<16, allocfile )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x178 );
      }

      // Run the allocator serializer
      if( (n = _cxmalloc_serialization__serialize_allocator_ARO( allocator_RO, output )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x179 );
      }
      nqwords += n;

      // Traverse all blocks in allocator
      cxmalloc_block_t **cursor_RO = allocator_RO->blocks;
      while( cursor_RO < allocator_RO->space ) {
        cxmalloc_block_t *block_RO = *cursor_RO++;
        if( force || block_RO->linedata == NULL || _icxmalloc_block.NeedsPersist_ARO( block_RO ) ) {
          if( (n = _cxmalloc_serialization__persist_block_ARO( block_RO, force )) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x17A );
          }
          nqwords += n;
        }
      }

      // Clean up any previous blocks
      while( cursor_RO < allocator_RO->end ) {
        cxmalloc_bidx_t bidx = (cxmalloc_bidx_t)(cursor_RO - allocator_RO->blocks);
        if( bidx < n_prev_blocks ) {
          if( _cxmalloc_serialization__remove_block_ARO( allocator_RO, bidx ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x17B );
          }
          cursor_RO++;
        }
        else {
          break;
        }
      }
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( input ) {
      COMLIB_OBJECT_DESTROY( input );
    }
    if( prev_header ) {
      ALIGNED_FREE( prev_header );
    }
    if( output ) {
      COMLIB_OBJECT_DESTROY( output );
    }
    if( CSTR__allocator_dat ) {
      CStringDelete( CSTR__allocator_dat );
    }
  }

  return nqwords;
}



/*******************************************************************//**
 * Restore Allocator
 * 
 * Returns: > 0 when allocator was restored from file
 *            0 when no file exists
 *           -1 on error
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__restore_allocator_ACS( cxmalloc_allocator_t *allocator_CS ) {
  int64_t nqwords = 0;
  CString_t *CSTR__allocator_dat = NULL;
  CQwordQueue_t *input = NULL;
  int64_t n;

  XTRY {
    // Restore if we're in persistent mode
    if( allocator_CS->CSTR__allocdir ) {

      // Get the allocator data file full path
      if( (CSTR__allocator_dat = __get_allocator_dat_ARO( allocator_CS )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x181 );
      }

      // the allocator data file
      const char *allocator_dat = CStringValue( CSTR__allocator_dat );

      // Does the allocator data file exist?
      if( file_exists( allocator_dat ) ) {

        // PERSISTED ALLOCATOR FOUND
        // NOW RESTORE.

        CXMALLOC_VERBOSE( 0x182, "Restoring: %s", allocator_dat );

        // Create allocator input queue and file
        if( (input = CQwordQueueNewInput( 1L<<16, allocator_dat )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x183 );
        }

        // Run the allocator deserializer
        if( (n = _cxmalloc_serialization__deserialize_allocator_ACS( allocator_CS, input )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x184 );
        }
        nqwords += n;
      }
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( input ) {
      COMLIB_OBJECT_DESTROY( input );
    }
    if( CSTR__allocator_dat ) {
      CStringDelete( CSTR__allocator_dat );
    }
  }

  return nqwords;
}



/*******************************************************************//**
 * Serialize Allocator
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__serialize_allocator_ARO( cxmalloc_allocator_t *allocator_RO, CQwordQueue_t *output ) {
  int64_t nqwords = 0;

  __allocator_header_t *header = NULL;

  XTRY {
    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( header, __allocator_header_t );
    if( header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x191 );
    }
    memset( header, 0, sizeof( __allocator_header_t ) );

    memcpy( header->start_marker, __START_FILE, sizeof( __START_FILE ) );
    header->aidx            = allocator_RO->aidx;
    header->num_blocks      = (size_t)(allocator_RO->space - allocator_RO->blocks);
    header->head_bidx       = allocator_RO->head && *allocator_RO->head ? (*allocator_RO->head)->bidx : __NO_BIDX;
    header->last_reuse_bidx = allocator_RO->last_reuse ? allocator_RO->last_reuse->bidx : __NO_BIDX;
    header->last_hole_bidx  = allocator_RO->last_hole ? allocator_RO->last_hole->bidx : __NO_BIDX;
    memcpy( header->shape, &allocator_RO->shape, sizeof( cxmalloc_datashape_t ) );
    WRITE_ARRAY_OR_THROW( output, header->qwords, qwsizeof( __allocator_header_t ), 0x192 );
    nqwords += qwsizeof( __allocator_header_t );

    // ----------
    // BLOCK INFO
    // ----------
    __block_info_t block_info;
    for( size_t i=0; i<header->num_blocks; i++ ) {
      cxmalloc_block_t *block_RO = allocator_RO->blocks[i];
      block_info.bidx = block_RO->bidx;
      block_info.n_active = block_RO->capacity - block_RO->available;
      size_t registry_count = _icxmalloc_block.ComputeActive_ARO( block_RO );
      if( registry_count != block_info.n_active ) {
        CXMALLOC_CRITICAL( 0x193, "Line registry and active counter out of sync: registry=%llu, counter=%llu", registry_count, block_info.n_active );
        CXMALLOC_INFO( 0x194, "Forcing counter to match registry" );
        block_info.n_active = registry_count;
        block_RO->available = block_RO->capacity - registry_count;
      }
      block_info.allocated = block_RO->linedata ? 1 : 0;
      WRITE_ARRAY_OR_THROW( output, block_info.qwords, qwsizeof( __block_info_t ), 0x195 );
      nqwords += qwsizeof( __block_info_t );
    }
    // end of block info
    block_info.bidx = __NO_BIDX;
    block_info.n_active = 0;
    block_info.allocated = 0;
    WRITE_ARRAY_OR_THROW( output, block_info.qwords, qwsizeof( __block_info_t ), 0x196 );
    nqwords += qwsizeof( __block_info_t );

    // -----------
    // BLOCK CHAIN
    // -----------
    if( allocator_RO->head && *allocator_RO->head && (*allocator_RO->head)->next_block ) {
      cxmalloc_block_t *block_in_chain_RO = (*allocator_RO->head)->next_block;
      while( block_in_chain_RO != NULL ) {
        block_info.bidx = block_in_chain_RO->bidx;
        block_info.n_active = block_in_chain_RO->capacity - block_in_chain_RO->available;
        size_t registry_count = _icxmalloc_block.ComputeActive_ARO( block_in_chain_RO );
        if( registry_count != block_info.n_active ) {
          CXMALLOC_CRITICAL( 0x197, "Line registry and active counter out of sync: registry=%llu, counter=%llu", registry_count, block_info.n_active );
          CXMALLOC_INFO( 0x198, "Forcing counter to match registry" );
          block_info.n_active = registry_count;
          block_in_chain_RO->available = block_in_chain_RO->capacity - registry_count;
        }
        block_info.allocated = block_in_chain_RO->linedata ? 1 : 0;
        WRITE_ARRAY_OR_THROW( output, block_info.qwords, qwsizeof( __block_info_t ), 0x199 );
        nqwords += qwsizeof( __block_info_t );
        block_in_chain_RO = block_in_chain_RO->next_block;
      }
    }
    // end of block chain
    block_info.bidx = __NO_BIDX;
    block_info.n_active = 0;
    block_info.allocated = 0;
    WRITE_ARRAY_OR_THROW( output, block_info.qwords, qwsizeof( __block_info_t ), 0x19A );
    nqwords += qwsizeof( __block_info_t );

    // ---
    // END
    // ---
    WRITE_ARRAY_OR_THROW( output, __END_FILE, qwsizeof(__END_FILE), 0x19B );
    nqwords += qwsizeof( __END_FILE );
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( header ) {
      ALIGNED_FREE( header );
    }
  }

  return nqwords;
}



/*******************************************************************//**
 * Deserialize Allocator
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__deserialize_allocator_ACS( cxmalloc_allocator_t *allocator_CS, CQwordQueue_t *input ) {
  int64_t nread = 0;

  __allocator_header_t *header = NULL;

  XTRY {
    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( header, __allocator_header_t );
    if( header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1A1 );
    }
    memset( header, 0, sizeof( __allocator_header_t ) );

    // Read the header
    READ_ARRAY_OR_THROW( input, header->qwords, qwsizeof( __allocator_header_t ), 0x1A2 );
    nread += qwsizeof( __allocator_header_t );
    // Verify start marker
    EXPECT_ARRAY_OR_THROW( header->start_marker, __START_FILE, qwsizeof( __START_FILE ), 0x1A3 );
    // Verify aidx
    EXPECT_QWORD_OR_THROW( header->aidx, allocator_CS->aidx, 0x1A4 );
    // Get number of blocks
    int n_blocks = (int)header->num_blocks;
    // Make sure we have blocks
    if( header->head_bidx == __NO_BIDX ) {
      XBREAK;
    }

    // Verify shape
    if( memcmp( header->shape, &allocator_CS->shape, sizeof( cxmalloc_datashape_t ) ) ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x1A5 );
    }

    // -------------
    // CREATE BLOCKS
    // -------------
    __block_info_t block_info;
    for( int i=0; i<n_blocks; i++ ) {
      cxmalloc_bidx_t bidx = (cxmalloc_bidx_t)i;
      READ_ARRAY_OR_THROW( input, block_info.qwords, qwsizeof( __block_info_t ), 0x1A6 );
      nread += qwsizeof( __block_info_t );
      EXPECT_QWORD_OR_THROW( block_info.bidx, (QWORD)bidx, 0x1A7 );
      bool allocate_data = block_info.allocated ? true : false;
      if( (allocator_CS->blocks[i] = _icxmalloc_block.New_ACS( allocator_CS, bidx, allocate_data )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x1A8 );
      }
      allocator_CS->space++;
    }
    READ_ARRAY_OR_THROW( input, block_info.qwords, qwsizeof( __block_info_t ), 0x1A9 );
    nread += qwsizeof( __block_info_t );
    EXPECT_QWORD_OR_THROW( block_info.bidx, __NO_BIDX, 0x1AA );
    EXPECT_QWORD_OR_THROW( block_info.n_active, 0, 0x1AB );
    EXPECT_QWORD_OR_THROW( block_info.allocated, 0, 0x1AC );
    
    // --------------------
    // Set the chain points
    // --------------------
    // Set head block
    allocator_CS->head = allocator_CS->blocks + (cxmalloc_bidx_t)header->head_bidx;
    
    // Set last reuse block
    if( header->last_reuse_bidx != __NO_BIDX ) {
      cxmalloc_bidx_t last_reuse_bidx = (cxmalloc_bidx_t)header->last_reuse_bidx;
      allocator_CS->last_reuse = allocator_CS->blocks[ last_reuse_bidx ];
    }
    else {
      allocator_CS->last_reuse = NULL;
    }

    // Set last hole block
    if( header->last_hole_bidx != __NO_BIDX ) {
      cxmalloc_bidx_t last_hole_bidx = (cxmalloc_bidx_t)header->last_hole_bidx;
      allocator_CS->last_hole = allocator_CS->blocks[ last_hole_bidx ];
    }
    else {
      allocator_CS->last_hole = NULL;
    }

    // ------------------
    // CREATE BLOCK CHAIN
    // ------------------
    cxmalloc_block_t *last_block_CS = *allocator_CS->head;
    do {
      READ_ARRAY_OR_THROW( input, block_info.qwords, qwsizeof( __block_info_t ), 0x1AD );
      nread += qwsizeof( __block_info_t );
      if( block_info.bidx != __NO_BIDX ) {
        cxmalloc_bidx_t bidx = (cxmalloc_bidx_t)block_info.bidx;
        cxmalloc_block_t *block_in_chain_CS = allocator_CS->blocks[ bidx ];
        last_block_CS = _icxmalloc_chain.AppendBlock_ACS( last_block_CS, block_in_chain_CS );
      }
    } while( block_info.bidx != __NO_BIDX );

    // ---
    // END
    // ---
    QWORD end[ qwsizeof( __END_FILE ) ];
    READ_ARRAY_OR_THROW( input, end, qwsizeof(end), 0x1AE );
    nread += qwsizeof(end);
    EXPECT_ARRAY_OR_THROW( end, __END_FILE, qwsizeof( __END_FILE ), 0x1AF );
  }
  XCATCH( errcode ) {
    cxmalloc_block_t **cursor_CS = allocator_CS->blocks;
    while( cursor_CS < allocator_CS->space ) {
      _icxmalloc_block.Delete_ACS( allocator_CS, *cursor_CS++ );
    }
    allocator_CS->space = allocator_CS->blocks;
    nread = -1;
  }
  XFINALLY {
    if( header ) {
      ALIGNED_FREE( header );
    }
  }

  return nread;
}



/***********************************************************************/
/***********************************************************************/
/***** B L O C K *******************************************************/
/***********************************************************************/
/***********************************************************************/


/*******************************************************************//**
 * Persist Block
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__persist_block_ARO( cxmalloc_block_t *block_RO, bool force ) {
  int64_t nqwords = 0;
  int64_t n;
  XTRY {
    // Make output buffer if we're in persistent mode
    if( block_RO->CSTR__base_path ) {
      // Get the block directory
      const char *blockdir = CStringValue( block_RO->CSTR__blockdir );

      // Create the block directory if it doesn't exist
      if( !dir_exists( blockdir ) ) {
        if( create_dirs( blockdir ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x1B1 );
        }
      }

      // Close any previous output
      if( block_RO->base_bulkout ) {
        COMLIB_OBJECT_DESTROY( block_RO->base_bulkout );
      }

      // Get the dat file path
      const char *base_path = CStringValue( block_RO->CSTR__base_path );
      
      // Create new output
      if( (block_RO->base_bulkout = CQwordQueueNewOutput( 1L<<20, base_path )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x1B2 );
      }
      
      // Serialize block data
      if( (n = _cxmalloc_serialization__serialize_block_ARO( block_RO, block_RO->base_bulkout, force )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x1B3 );
      }
      nqwords += n;
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( block_RO->base_bulkout ) {
      COMLIB_OBJECT_DESTROY( block_RO->base_bulkout );
      block_RO->base_bulkout = NULL;
    }
  }

  return nqwords;
}



/*******************************************************************//**
 * Restore Block
 * 
 * Returns: > 0 when block was restored from file
 *            0 when no file exists
 *           -1 on error
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__restore_block_ACS( cxmalloc_block_t *block_CS ) {
  int64_t nqwords = 0;
  int64_t n;
  XTRY {
    // Make input buffer if we're in persistent mode
    if( block_CS->CSTR__base_path ) {
      // Get the file path
      const char *base_path = CStringValue( block_CS->CSTR__base_path );
      // Do we have a block file on disk?
      if( file_exists( base_path ) ) {
        // Close any previous input
        if( block_CS->base_bulkin ) {
          COMLIB_OBJECT_DESTROY( block_CS->base_bulkin );
        }
        // Create new input
        if( (block_CS->base_bulkin = CQwordQueueNewInput( 1L<<20, base_path )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x1C1 );
        }
        // Deserialize block data
        if( (n = _cxmalloc_serialization__deserialize_block_ACS( block_CS, block_CS->base_bulkin )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x1C2 );
        }
        nqwords += n;
      }
    }
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( block_CS->base_bulkin ) {
      COMLIB_OBJECT_DESTROY( block_CS->base_bulkin );
      block_CS->base_bulkin = NULL;
    }

  }

  return nqwords;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__remove_block_ARO( const cxmalloc_allocator_t *allocator_RO, const cxmalloc_bidx_t bidx ) {
  int64_t retcode = 0;
  CString_t *CSTR__path = NULL;
  CString_t *CSTR__base_path = NULL;
  CString_t *CSTR__ext_path = NULL;
  XTRY {

    if( _icxmalloc_block.GetFilepaths_OPEN( allocator_RO->CSTR__allocdir, allocator_RO->aidx, bidx, &CSTR__path, &CSTR__base_path, &CSTR__ext_path ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x1D1 );
    }

    const char *base_path = CStringValue( CSTR__base_path );
    const char *ext_path = CStringValue( CSTR__ext_path );

    if( file_exists( base_path ) ) {
      if( remove( base_path ) != 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x1D2, "[%d] %s", errno, strerror(errno) );
      }
    }

    if( file_exists( ext_path ) ) {
      if( remove( ext_path ) != 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x1D3, "[%d] %s", errno, strerror(errno) );
      }
    }

  }
  XCATCH( errcode ) {
    retcode = -1;
  }
  XFINALLY {
    if( CSTR__path ) {
      CStringDelete( CSTR__path );
    }
    if( CSTR__base_path ) {
      CStringDelete( CSTR__base_path );
    }
    if( CSTR__ext_path ) {
      CStringDelete( CSTR__ext_path );
    }
  }

  return retcode;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __new_buffer_and_tap_ARO( const cxmalloc_block_t *block_RO, QWORD **buffer, cxmalloc_object_tap_t *tap ) {

  cxmalloc_datashape_t *shape_RO = &block_RO->parent->shape;
  int64_t meta_qwords   = qwcount( shape_RO->serialized.meta_sz );  // enough qwords to store the serialized meta bytes
  int64_t obj_qwords    = qwcount( shape_RO->serialized.obj_sz );   // enough qwords to store the serialized object bytes
  int64_t array_qwords  = qwcount( shape_RO->serialized.unit_sz * shape_RO->linemem.awidth ); // enough qwords to store all the serialized unit bytes
  int64_t line_qwords   = meta_qwords + obj_qwords + array_qwords;

  // Allocate the buffer
  TALIGNED_ARRAY( *buffer, QWORD, line_qwords );
  if( *buffer == NULL ) {
    return -1;
  }
  else {
    int64_t offset  = 0;
    memset( *buffer, 0, line_qwords*sizeof(QWORD) );

    // meta starts at the beginning
    tap->line_meta = *buffer + offset;
    offset += meta_qwords;

    // obj follows meta
    tap->line_obj = *buffer + offset;
    offset += obj_qwords;

    // array follows object
    tap->line_array = *buffer + offset;

    return line_qwords;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __delete_line_serialization_context( cxmalloc_line_serialization_context_t **context ) {
  if( *context ) {
    // buffer
    if( (*context)->__buffer ) {
      ALIGNED_FREE( (*context)->__buffer );
    }
    // ext output
    if( (*context)->out_ext ) {
      COMLIB_OBJECT_DESTROY( (*context)->out_ext );
    }
#ifdef CXMALLOC_CONSISTENCY_CHECK
    if( (*context)->objdump ) {
      CX_FCLOSE( (*context)->objdump );
    }
#endif
    // context
    free( *context );
    *context = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_line_serialization_context_t * __new_line_serialization_context_ARO( cxmalloc_block_t *block_RO ) {

  cxmalloc_line_serialization_context_t *context = NULL;

  XTRY {
    // Allocate context
    if( (context = (cxmalloc_line_serialization_context_t*)calloc( 1, sizeof( cxmalloc_line_serialization_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1E1 );
    }

    // Set the line serializer function
    if( (context->serialize = block_RO->parent->family->descriptor->serialize_line) == NULL ) {
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_BUG, 0x1E2, "serialize_line is undefined for %s", block_RO->parent->family->obid.longstring.string );
    }

    // Create buffer and buffer tap, and set number of qwords per line
    if( (context->line_qwords = __new_buffer_and_tap_ARO( block_RO, &context->__buffer, &context->tapout )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1E3 );
    }

    // Create the ext output
    if( (context->out_ext = CQwordQueueNewOutput( __FLUSH_THRESHOLD, CStringValue( block_RO->CSTR__ext_path ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1E4 );
    }
    context->ext_offset = 0;

#ifdef CXMALLOC_CONSISTENCY_CHECK
    CString_t *CSTR__dumpname = CStringNewFormat( "%s.txt", CStringValue( block_RO->CSTR__base_path ) );
    if( CSTR__dumpname ) {
      context->objdump = CX_FOPEN( CStringValue( CSTR__dumpname ), "w" );
      CStringDelete( CSTR__dumpname );
    }
    if( context->objdump == NULL ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x1E5 );
    }
#endif
  }
  XCATCH( errcode ) {
    __delete_line_serialization_context( &context );
  }
  XFINALLY {
  }

  return context;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __delete_line_deserialization_context( cxmalloc_line_deserialization_context_t **context ) {
  if( *context ) {
    // buffer
    if( (*context)->__buffer ) {
      ALIGNED_FREE( (*context)->__buffer );
    }
    // ext output
    if( (*context)->in_ext ) {
      COMLIB_OBJECT_DESTROY( (*context)->in_ext );
    }
    // context
    free( *context );
    *context = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_line_deserialization_context_t * __new_line_deserialization_context_ACS( cxmalloc_block_t *block_CS ) {

  cxmalloc_line_deserialization_context_t *context = NULL;
  cxmalloc_family_t *family_RO = block_CS->parent->family;

  XTRY {
    // Allocate context
    if( (context = (cxmalloc_line_deserialization_context_t*)calloc( 1, sizeof( cxmalloc_line_deserialization_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1F1 );
    }

    // Set the line deserializer function
    if( (context->deserialize = family_RO->descriptor->deserialize_line) == NULL ) {
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_BUG, 0x1F2, "deserialize_line is undefined for %s", block_CS->parent->family->obid.longstring.string );
    }

    // Create buffer and buffer tap, and set number of qwords per line
    if( (context->line_qwords = __new_buffer_and_tap_ARO( block_CS, &context->__buffer, &context->tapin )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1F3 );
    }

    // Create the ext input
    if( (context->in_ext = CQwordQueueNewInput( __FLUSH_THRESHOLD, CStringValue( block_CS->CSTR__ext_path ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1F4 );
    }
    context->ext_offset = 0;

    // Set the family
    context->family = family_RO;

    // Set the auxiliary objects
    memcpy( context->auxiliary, family_RO->descriptor->auxiliary.obj, 8 * sizeof(void*) );

  }
  XCATCH( errcode ) {
    __delete_line_deserialization_context( &context );
  }
  XFINALLY {
  }

  return context;
}



/*******************************************************************//**
 * Validate serialized counts
 * 
 ***********************************************************************
 */
static int __validate_serialized_counts_ARO( const cxmalloc_block_t *block_RO, int64_t count_active ) {
  int ret = 0;
  int64_t block_active_register = _icxmalloc_block.ComputeActive_ARO( block_RO );
  int64_t block_active = block_RO->capacity - block_RO->available;
  // Validate
  if( count_active != block_active || block_active != block_active_register ) {
    CXMALLOC_CRITICAL( 0x201, "Allocator block corruption, block active lines mismatch: count(line.refcnt>0)=%llu, block_active_register=%lld, block_active=%lld", count_active, block_active_register, block_active );

    CStringQueue_t *Q = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
    char *buffer = NULL;

    if( Q ) {
      // Dump previous block (if any)
      CALLABLE( Q )->Write( Q, "PREVIOUS BLOCK:\n", -1 );
      _icxmalloc_block.Repr_ARO( block_RO->prev_block, Q );
      CALLABLE( Q )->Write( Q, "\n  ^\n  |\n  |\n", -1 );

      // Dump corrupted block
      CALLABLE( Q )->Write( Q, "THIS BLOCK (CORRUPTED):\n", -1 );
      _icxmalloc_block.Repr_ARO( block_RO, Q );
      CALLABLE( Q )->Write( Q, "\n  |\n  |\n  V\n", -1 );

      // Dump next block (if any)
      CALLABLE( Q )->Write( Q, "NEXT BLOCK:\n", -1 );
      _icxmalloc_block.Repr_ARO( block_RO->next_block, Q );
      CALLABLE( Q )->Write( Q, "\n", -1 );

      // Write to stdout
      CALLABLE( Q )->NulTerm( Q );
      CALLABLE( Q )->Read( Q, (void**)&buffer, -1 );
      if( buffer ) {
        printf( "%s", buffer );
        ALIGNED_FREE( buffer );
      }
      COMLIB_OBJECT_DESTROY( Q );
    }

    if( block_active_register == block_active ) {
      CXMALLOC_INFO( 0x202, "Block line register and block counters agree (%lld)", block_active );
    }

    if( count_active < block_active_register ) {
      CXMALLOC_INFO( 0x203, "Lines leaked, block register lost track of %lld discarded lines", block_active_register - count_active );
    }

    if( count_active > block_active_register ) {
      CXMALLOC_INFO( 0x204, "Block register is tracking %lld lines have not yet been discarded", count_active - block_active_register );
    }

    // Get lost lines, either because registry has lost them (unregistered and zero refcnt), or because ownership conflict (registered and nonzero refcnt)
    cxmalloc_linehead_t ** lost_lines = _icxmalloc_block.FindLostLines_ACS( (cxmalloc_block_t*)block_RO, false );
    if( lost_lines ) {
      cxmalloc_linehead_t **cursor = lost_lines;
      cxmalloc_linehead_t *linehead;
      while( (linehead = *cursor++) != NULL ) {
        // Leaked
        if( linehead->data.refc == 0 ) {
          CXMALLOC_INFO( 0x205, "Leaked line @ %llp (memory is lost, no referrers.)", linehead );
        }
        // Conflict
        else {
          CXMALLOC_INFO( 0x206, "Dangling line @ %llp (conflicting referrers, memory discarded but still in use.)", linehead );
        }
        CXMALLOC_INFO( 0x207, "Object dump:" );
        // Try to get an object from this linehead (if allocator is used for comlib objects)
        comlib_object_t *obj = _icxmalloc_block.GetObjectAtAddress_ACS( block_RO, (QWORD)_cxmalloc_object_from_linehead( linehead ) );
        if( obj ) {
          PRINT( obj );
        }
        else {
          printf( "(array data only, not object)\n" );
        }
      }
      free( lost_lines );
    }
    else {
      CXMALLOC_INFO( 0x208, "Scan revealed no lost lines ???" );
    }
    ret = -1;
  }

  return ret;
}



/*******************************************************************//**
 * Serialize Lines
 * 
 ***********************************************************************
 */
static int64_t __serialize_lines_ARO( cxmalloc_block_t *block_RO, cxmalloc_line_serialization_context_t *context, CQwordQueue_t *output ) {
  int64_t nqwords = 0;
  QWORD *empty = NULL;

  XTRY {
    __object_marker_t object_marker = {
      .separator  = __OBJECT_SEPARATOR,
      .number     = 0,
      .active     = 0,
    };
    cxmalloc_datashape_t *shape = &block_RO->parent->shape;
    size_t quant = shape->blockmem.quant;
    int stride = shape->linemem.chunks;
    cxmalloc_linehead_t *line_RO = (cxmalloc_linehead_t*)block_RO->linedata;

    // Empty line
    size_t n_empty = context->line_qwords;
    CALIGNED_INITIALIZED_ARRAY_THROWS( empty, QWORD, n_empty, 0, 0x211 );

#ifdef CXMALLOC_CONSISTENCY_CHECK
    uint32_t now_ts = __SECONDS_SINCE_1970();
    fprintf( context->objdump, "# === cxmalloc object dump ===\n" );
    fprintf( context->objdump, "#   %s\n", block_RO->parent->family->obid.longstring.string );
    fprintf( context->objdump, "#   aidx=%u bidx=%u\n", block_RO->parent->aidx, block_RO->bidx );
    fprintf( context->objdump, "#   dumptime=%u\n", now_ts );
    fprintf( context->objdump, "# ============================\n" );
    fprintf( context->objdump, "# offset refc          address data\n" );
    //                          12345678 5555 1234567812345678 ........
#endif

    // Serialize all lines
    int64_t count_active = 0;
    for( size_t obj_num=0; obj_num<quant; obj_num++ ) {

      QWORD *buffer;

      // set object number
      object_marker.number = obj_num;
      
      // object exists
      if( line_RO->data.refc > 0 ) {
        // active object
        buffer = context->__buffer;
        object_marker.active = __OBJECT_ACTIVE;

        // write marker to ext output
        WRITE_STRUCT_OR_THROW( context->out_ext, object_marker, 0x212 );
        context->ext_offset += qwsizeof( object_marker );
        // NOTE: context.ext_offset now points to the QWORD offset in the ext file where the line serializer
        // can write variable data. The line serializer will be responsible for writing the data to the
        // context.out_ext queue and incrementing context.ext_offset by the total number of qwords written.

#ifdef CXMALLOC_CONSISTENCY_CHECK
        fprintf( context->objdump, "%8u %4u %p ", line_RO->data.offset, line_RO->data.refc, line_RO );
#endif

        // serialize object to active buffer (and possibly write additional ext data to ext output)
        context->linehead = line_RO;
        if( context->serialize( context ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x213 );
        }

#ifdef CXMALLOC_CONSISTENCY_CHECK
        fprintf( context->objdump, "\n" );
#endif
        ++count_active;
      }
      // empty
      else {
        // mark idle object
        buffer = empty;
        object_marker.active = __OBJECT_IDLE;
        // NOTE: no ext data written for idle objects!
      }

      // write marker to dat output
      WRITE_STRUCT_OR_THROW( output, object_marker, 0x214 );
      nqwords += qwsizeof( object_marker );

      // write buffer to dat output
      WRITE_ARRAY_OR_THROW( output, buffer, context->line_qwords, 0x215 );
      nqwords += context->line_qwords;

      // flush output to files when needed
      if( CALLABLE( output )->Length( output ) > __FLUSH_THRESHOLD ) {
        CALLABLE( output )->FlushNolock( output );
      }
      if( CALLABLE( context->out_ext )->Length( context->out_ext ) > __FLUSH_THRESHOLD ) {
        CALLABLE( context->out_ext )->FlushNolock( context->out_ext );
      }

      // clear modified flag
      line_RO->data.flags._mod = 0;

      // advance to the beginning of next line
      line_RO += stride;
    }

    // sanity check
    if( __validate_serialized_counts_ARO( block_RO, count_active ) < 0 ) {
      CXMALLOC_CRITICAL( 0x216, "Corruption detected, continuing anyway!" );
    }
  
  }
  XCATCH( errcode ) {

    nqwords = -1;
  }
  XFINALLY {
    if( empty ) {
      ALIGNED_FREE( empty );
    }
  }

  return nqwords;
}



/*******************************************************************//**
 * Deserialize Lines
 * 
 ***********************************************************************
 */
static int64_t __deserialize_lines_ACS( cxmalloc_block_t *block_CS, size_t n_active, cxmalloc_line_deserialization_context_t *context, CQwordQueue_t *input ) {
  int64_t nqwords = 0;
  size_t count_active = 0;
  cxmalloc_linehead_t *current_line = (cxmalloc_linehead_t*)block_CS->linedata;
  cxmalloc_allocator_t *allocator_CS = block_CS->parent;

  XTRY {
    if( block_CS->linedata == NULL ) {
      CXMALLOC_CRITICAL( 0x221, "Corruption detected, continuing anyway!" );
      CXMALLOC_INFO( 0x221, "Expected %llu lines (from header), but block data is empty in %s (a=%u b=%u)", n_active, CStringValue(block_CS->CSTR__blockdir), allocator_CS->aidx, block_CS->bidx );
      XBREAK;
    }

    __object_marker_t object_marker;
    __object_marker_t ext_marker;
    cxmalloc_datashape_t *shape = &block_CS->parent->shape;
    size_t quant = shape->blockmem.quant;
    int stride = shape->linemem.chunks;

    // First reset the line register as if all lines are checked out,
    // then start "putting back" all lines that are found to be idle
    // during deserialization below.
    cxmalloc_linehead_t **cursor = block_CS->reg.top;
    cxmalloc_linehead_t **end = block_CS->reg.bottom;
    while( cursor < end ) {
      *cursor++ = NULL;
    }
    block_CS->reg.get = NULL;
    block_CS->reg.put = block_CS->reg.top;

#ifndef NDEBUG
    size_t checkpoint_stride = quant/32;
    size_t next_obj_checkpoint = checkpoint_stride;
#endif

    // Deserialize all lines
    for( size_t obj_num=0; obj_num<quant; obj_num++ ) {

      // Read next object marker
      READ_STRUCT_OR_THROW( input, object_marker, 0x222 );
      nqwords += qwsizeof( object_marker );

      // Expect the next object number
      EXPECT_QWORD_OR_THROW( object_marker.separator, __OBJECT_SEPARATOR, 0x223 );
      EXPECT_QWORD_OR_THROW( object_marker.number, obj_num, 0x224 );

      // Found an active object
      if( object_marker.active == __OBJECT_ACTIVE ) {
        // read and validate next ext marker 
        READ_STRUCT_OR_THROW( context->in_ext, ext_marker, 0x225 );
        EXPECT_ARRAY_OR_THROW( ext_marker.qwords, object_marker.qwords, qwsizeof( __object_marker_t ), 0x226 );
        context->ext_offset += qwsizeof( __object_marker_t );

        // read data into context buffer
        READ_ARRAY_OR_THROW( input, context->__buffer, context->line_qwords, 0x227 );

        // object data will be deserialized into this line
        context->linehead = current_line;

        // deserialize buffer into line
        if( context->deserialize( context ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x228 );
        }

        // set the active flag
        current_line->data.flags._act = 1;
        _cxmalloc_bitvector_set( &block_CS->active, current_line );

        ++count_active;
      }
      // Found an idle object
      else {
        // skip over
        CALLABLE( input )->DiscardNolock( input, context->line_qwords );

        // put into line register as available
        *block_CS->reg.put++ = current_line;
        if( block_CS->reg.put == block_CS->reg.bottom ) {
          // ALL lines were put back, i.e. block has no active lines so nothing more can be put back;
          block_CS->reg.put = NULL; // empty block
          // Sanity check: can't have active objects and we must be at the last object
          if( count_active != 0 || obj_num != quant-1 ) {
            THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x229 );
          }
        }

        // clear the active flag
        current_line->data.flags._act = 0;
        _cxmalloc_bitvector_clear( &block_CS->active, current_line );
      }

#ifndef NDEBUG
      // print object checkpoint
      if( obj_num >= next_obj_checkpoint ) {
        CString_t *CSTR__checkpoint = CStringNewFormat(
          "%s "
          "aidx=%u "
          "bidx=%u "
          "obj=%llu/%llu "
          "act=%llu "
          "dat=%lld "
          "ext=%lld",
          CStringValue( block_CS->CSTR__blockdir ),
          allocator_CS->aidx,
          block_CS->bidx,
          obj_num+1,
          quant,
          count_active,
          nqwords,
          context->ext_offset
        );

        DEBUG( 0x22A, CStringValue( CSTR__checkpoint ) );
        CStringDelete( CSTR__checkpoint );
        next_obj_checkpoint += checkpoint_stride;
        if( next_obj_checkpoint >= quant ) {
          next_obj_checkpoint = quant-1;
        }
      }
#endif

      // count the data qwords
      nqwords += context->line_qwords;

      // next line
      current_line += stride;
    }

    // sanity check
    size_t bv_count = _cxmalloc_bitvector_count( &block_CS->active );
    if( n_active != count_active || n_active != bv_count ) {
      CXMALLOC_CRITICAL( 0x22B, "Corruption detected, continuing anyway!" );
      CXMALLOC_INFO( 0x22C, "Expected %llu lines (from header), loaded %llu active lines (bitvector count %llu)", n_active, count_active, bv_count );
    }

    CXMALLOC_INFO( 0x22D, "Restored: %s (a=%u b=%u o=%llu)", CStringValue(block_CS->CSTR__blockdir), allocator_CS->aidx, block_CS->bidx, n_active );

    // Unless ALL objects are active (i.e. block full) set the register get pointer to the top
    if( n_active < quant ) {
      block_CS->reg.get = block_CS->reg.top;
    }

    // set the available counter to reflect line register
    block_CS->available = _icxmalloc_block.ComputeAvailable_ARO( block_CS );
  }
  XCATCH( errcode ) {
    int bidx = block_CS->bidx;
    int aidx = block_CS->parent->aidx;
    const char *name = block_CS->parent->family->obid.longstring.string;
    const char *path = CStringValue( block_CS->CSTR__base_path );
    CXMALLOC_CRITICAL( errcode, "Failed to deserialize block (linedata=%llp, current_line=%llp, n_expected=%llu, n_actual=%llu)", block_CS->linedata, current_line, n_active, count_active );
    CXMALLOC_CRITICAL( errcode, "Allocator detail: aidx=%d bidx=%d name='%s' path='%s'", aidx, bidx, name, path );
    nqwords = -1;
  }
  XFINALLY {

  }

  return nqwords;
}



/*******************************************************************//**
 * Serialize Block
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__serialize_block_ARO( cxmalloc_block_t *block_RO, CQwordQueue_t *output, bool force ) {
  int64_t nwritten = 0;
  __block_header_t *header = NULL;
  cxmalloc_line_serialization_context_t *context = NULL;

  if( force ) {}

  XTRY {
    cxmalloc_datashape_t *shape = &block_RO->parent->shape;

    // Create the serialization context
    if( (context = __new_line_serialization_context_ARO( block_RO )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x231 );
    }

    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( header, __block_header_t );
    if( header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x232 );
    }
    memset( header, 0, sizeof( __block_header_t ) );

    memcpy( header->start_marker, __START_FILE, sizeof( __START_FILE ) );
    header->bidx        = block_RO->bidx;
    header->quant       = shape->blockmem.quant;
    header->nactive     = block_RO->capacity - block_RO->available;
    if( (QWORD)_icxmalloc_block.ComputeActive_ARO( block_RO ) != header->nactive ) {
      THROW_CRITICAL( CXLIB_ERR_CORRUPTION, 0x233 );
    }
    header->allocated   = block_RO->linedata ? 1 : 0;

    // write header to dat output
    WRITE_ARRAY_OR_THROW( output, header->qwords, qwsizeof( __block_header_t ), 0x234 );
    nwritten += qwsizeof( __block_header_t );

    // write header to ext output
    WRITE_ARRAY_OR_THROW( context->out_ext, header->qwords, qwsizeof( __block_header_t ), 0x235 );
    context->ext_offset += qwsizeof( __block_header_t );

    // ----------
    // DATA LINES
    // ----------
    if( header->allocated ) {
      int64_t n;
      if( (n = __serialize_lines_ARO( block_RO, context, output )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x236 );
      }
      nwritten += n;
    }
    else {
      WRITE_ARRAY_OR_THROW( output, &__NO_BLOCK_DATA, 1, 0x237 );
      nwritten += 1;
    }

    // ---------
    // Write END
    // ---------
    WRITE_ARRAY_OR_THROW( output, __END_FILE, qwsizeof(__END_FILE), 0x238 );
    nwritten += qwsizeof(__END_FILE);
    WRITE_ARRAY_OR_THROW( context->out_ext, __END_FILE, qwsizeof(__END_FILE), 0x239 );
    context->ext_offset += qwsizeof(__END_FILE);

  }
  XCATCH( errcode ) {
    nwritten = -1;
  }
  XFINALLY {
    __delete_line_serialization_context( &context );
    if( header ) {
      ALIGNED_FREE( header );
    }
  }

  return nwritten;
}



/*******************************************************************//**
 * Deserialize Block 
 * 
 ***********************************************************************
 */
static int64_t _cxmalloc_serialization__deserialize_block_ACS( cxmalloc_block_t *block_CS, CQwordQueue_t *input ) {
  int64_t nread = 0;
  __block_header_t *base_header = NULL;
  __block_header_t *ext_header = NULL;
  cxmalloc_line_deserialization_context_t *context = NULL;

  XTRY {
    // Create the deserialization context
    if( (context = __new_line_deserialization_context_ACS( block_CS )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x241 );
    }

    // ------
    // HEADER
    // ------
    CALIGNED_MALLOC( base_header, __block_header_t );
    if( base_header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x242 );
    }
    memset( base_header, 0, sizeof( __block_header_t ) );

    CALIGNED_MALLOC( ext_header, __block_header_t );
    if( ext_header == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x243 );
    }
    memset( ext_header, 0, sizeof( __block_header_t ) );

    // Read the DAT header
    READ_ARRAY_OR_THROW( input, base_header->qwords, qwsizeof( __block_header_t ), 0x244 );
    nread += qwsizeof( __block_header_t );
    // Verify start marker
    EXPECT_ARRAY_OR_THROW( base_header->start_marker, __START_FILE, qwsizeof(__START_FILE), 0x245 );
    // Verify bidx
    EXPECT_QWORD_OR_THROW( base_header->bidx, block_CS->bidx, 0x246 );
    // Verify number of lines in block
    EXPECT_QWORD_OR_THROW( base_header->quant, block_CS->parent->shape.blockmem.quant, 0x247 );
    // Get the number of active lines
    size_t n_active = (size_t)base_header->nactive;


    // Read the VAR header and expect it to be identical to the DAT header
    READ_ARRAY_OR_THROW( context->in_ext, ext_header->qwords, qwsizeof( __block_header_t ), 0x248 );
    context->ext_offset += qwsizeof( __block_header_t );
    EXPECT_ARRAY_OR_THROW( ext_header->qwords, base_header->qwords, qwsizeof( __block_header_t ), 0x249 );

    // ----------
    // DATA LINES
    // ----------
    if( base_header->allocated ) {
      if( block_CS->linedata ) {
        int64_t n;
        if( (n = __deserialize_lines_ACS( block_CS, n_active, context, input )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x24A );
        }
        nread += n;
      }
      else {
        CXMALLOC_CRITICAL( 0x24B, "Corruption detected, continuing anyway!" );
        CXMALLOC_INFO( 0x24C, "Block header reports %llu active lines, block data is empty in %s (a=%u b=%u)", n_active, CStringValue(block_CS->CSTR__blockdir), block_CS->parent->aidx, block_CS->bidx );
        XBREAK;
      }
    }
    else {
      QWORD qword;
      READ_ARRAY_OR_THROW( input, &qword, 1, 0x24D );
      EXPECT_QWORD_OR_THROW( qword, __NO_BLOCK_DATA, 0x24E );
      nread += 1;
    }

    //---------------

    // ---
    // END
    // ---
    QWORD end[ qwsizeof( __END_FILE ) ];
    // end of dat
    READ_ARRAY_OR_THROW( input, end, qwsizeof(end), 0x24F );
    nread += qwsizeof(end);
    EXPECT_ARRAY_OR_THROW( end, __END_FILE, qwsizeof( __END_FILE ), 0x250 );
    // end of ext
    READ_ARRAY_OR_THROW( context->in_ext, end, qwsizeof(end), 0x251 );
    context->ext_offset += qwsizeof(end);
    EXPECT_ARRAY_OR_THROW( end, __END_FILE, qwsizeof( __END_FILE ), 0x252 );
  }
  XCATCH( errcode ) {
    cxlib_msg_set( NULL, 0, "Allocator block error: %03X", cxlib_exc_code( errcode ) );
    nread = -1;
  }
  XFINALLY {
    __delete_line_deserialization_context( &context );
    if( base_header ) {
      ALIGNED_FREE( base_header );
    }
    if( ext_header ) {
      ALIGNED_FREE( ext_header );
    }
  }

  return nread;
}
