/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _cxmalloc.h
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

#ifndef _CXMALLOC_CXMALLOC_H
#define _CXMALLOC_CXMALLOC_H


#include "_comlib.h"
#include "cxmalloc.h"



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __CXMALLOC_MESSAGE( LEVEL, Code, Format, ... ) LEVEL( Code, "CX::MEM " Format, ##__VA_ARGS__ )

#define CXMALLOC_VERBOSE( Code, Format, ... )   __CXMALLOC_MESSAGE( VERBOSE, Code, Format, ##__VA_ARGS__ )
#define CXMALLOC_INFO( Code, Format, ... )      __CXMALLOC_MESSAGE( INFO, Code, Format, ##__VA_ARGS__ )
#define CXMALLOC_WARNING( Code, Format, ... )   __CXMALLOC_MESSAGE( WARN, Code, Format, ##__VA_ARGS__ )
#define CXMALLOC_REASON( Code, Format, ... )    __CXMALLOC_MESSAGE( REASON, Code, Format, ##__VA_ARGS__ )
#define CXMALLOC_CRITICAL( Code, Format, ... )  __CXMALLOC_MESSAGE( CRITICAL, Code, Format, ##__VA_ARGS__ )
#define CXMALLOC_FATAL( Code, Format, ... )     __CXMALLOC_MESSAGE( FATAL, Code, Format, ##__VA_ARGS__ )




/* Block index size is 16 bits, determines the size of certain fixed-size data structures */
typedef uint16_t cxmalloc_bidx_t;

static const size_t CXMALLOC_BLOCK_REGISTER_SIZE = (1ull << (8*sizeof(cxmalloc_bidx_t)));
static const time_t CXMALLOC_MIN_BLOCK_LIFETIME_SECONDS = 200;



/* Simalloc member attributes */
typedef struct s_cxmalloc_members_t {
  // -----------------------------------------
  // [Q1]
  CS_LOCK cs_cxmalloc;        // global mutex

  // -----------------------------------------
  // [Q2.1.1]
  int cxmalloc_init_ok;       // cxmalloc initialization flag

  // [Q2.1.2]
  DWORD __rsv_2_1_2;

  // [Q2.2-8]
  QWORD __rsv_2[7];

} cxmalloc_members_t;

extern cxmalloc_members_t g_CXMALLOC;


/*******************************************************************//**
 * cxmalloc_linebyte_t
 ***********************************************************************
 */
typedef unsigned char cxmalloc_linebyte_t;



/*******************************************************************//**
 * cxmalloc_linechunk_t
 ***********************************************************************
 */
typedef __m256i cxmalloc_linechunk_t;



/*******************************************************************//**
 * cxmalloc_linequant_t
 ***********************************************************************
 */
typedef cacheline_t cxmalloc_linequant_t;



/*******************************************************************//**
 * cxmalloc_linehead_t
 *
 ***********************************************************************
 */
ALIGNED_TYPE(union, 32) u_cxmalloc_linehead_t { // 32 = sizeof(cxmalloc_linechunk_t)
  cxmalloc_linechunk_t _chunk;
  struct {
    /*  Q1        Q2        Q3        Q4         */
    /*  [--------][--------][--------][--------] */
    /*      M1        M2     ax  ofs   size      */
    /*                         bx   Fs     refc  */
    /*                                           */
    
    union u_cxmalloc_metaflex_t metaflex;  /* flexible meta data */
    
    /* Q3:                                                [aabboooF]              */
    union {
      __m128i m128i;
      struct {
        union {
          QWORD q3;
          struct {
            union {
              struct {
                uint32_t __8    : 8;    /* (no access!) */
                uint32_t offset : 24;   /* line offset        [----ofs-][--------]    */
              };
              struct {
                union {
                  uint8_t bits;
                  struct {
                    uint8_t ovsz : 1;   /* oversized          [-------F][--------]    */
                    uint8_t invl : 1;   /* invalid            [-------F][--------]    */
                    uint8_t _chk : 1;   /* consistency check  [-------F][--------]    */
                    uint8_t _act : 1;   /* active             [-------F][--------]    */
                    uint8_t _mod : 1;   /* modified                                   */
                    uint8_t _rsv : 3;   /* reserved                                   */
                  };
                } flags;
                uint8_t __na[3];
              };
            };
            uint16_t bidx;              /* block index        [--bx----][--------]    */
            uint16_t aidx;              /* allocator index    [ax------][--------]    */
          };
          struct {
            uint64_t ___0__7  : 8;
            uint64_t handle   : 56;     /*                    [axbxooo-][--------]    */
          };
        };

        /* Q4:                                                          [ssssrrrr]    */
        int32_t refc;                   /* reference count    [--------][----refc]    */
        uint32_t size;                  /* line length        [--------][size----]    */
      };
    } data;
  };
} cxmalloc_linehead_t;



/*******************************************************************//**
 * cxmalloc_serialized_datashape_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_serialized_datashape_t {
  uint32_t meta_sz;   /* [9]  number of bytes of meta-data per serialized line */ 
  uint32_t obj_sz;    /* [10] number of bytes of extra header (object) bytes per serialized line */
  uint32_t unit_sz;   /* [11] number of bytes per array unit in the serialized line */
  uint32_t __rsv2;    /* reserved */
} cxmalloc_serialized_datashape_t;



/*******************************************************************//**
 * cxmalloc_datashape_t
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_cxmalloc_datashape_t {
  cacheline_t _CL;
  struct {
    struct {
      uint32_t awidth;    /* [1] number of allocation units per line */
      uint32_t unit_sz;   /* [2] number of bytes per allocation unit */
      uint32_t chunks;    /* [3] number of linechunk sized elements per line, including header */
      uint32_t pad;       /* [4] line end padding to ensure line size is multiple of CPU cache line size */
      uint32_t qwords;    /* [5] number of total qwords in the allocated array (excluding header and object bytes) */
      uint32_t __rsv1;    /* reserved */
    } linemem;
    struct {
      size_t chunks;      /* [6] number of linechunk sized elements in block */
      size_t pad;         /* [7] block end padding to ensure block ends on a page boundary */
      size_t quant;       /* [8] number of lines per block */
    } blockmem;
    cxmalloc_serialized_datashape_t serialized;
  };
} cxmalloc_datashape_t;



/*******************************************************************//**
 * cxmalloc_lineregister_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_lineregister_t {
  cxmalloc_linehead_t **top;      // [1] points to the register of line cursors
  cxmalloc_linehead_t **bottom;   // [2] points one beyond the end of the register
  cxmalloc_linehead_t **get;      // [3] points to the line cursor pointing to the next available line, or NULL if all lines are in use
  cxmalloc_linehead_t **put;      // [4] points to the line cursor at the beginning of checked-out region, or NULL if no lines are in use
} cxmalloc_lineregister_t;



/*******************************************************************//**
 * cxmalloc_bitvector_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_bitvector_t {
  QWORD *data;
  size_t nq;
} cxmalloc_bitvector_t;



/*******************************************************************//**
 * cxmalloc_block_t
 ***********************************************************************
 */
CALIGNED_TYPE(struct) s_cxmalloc_block_t {
  union {
    cacheline_t _CL1;
    struct {
      cxmalloc_lineregister_t reg;            /* [1] the register controlling allocation and deallocation within the line data segment */
      cxmalloc_linechunk_t *linedata;         /* [2] points to data segment holding all lines            */
      struct s_cxmalloc_allocator_t *parent;  /* [3] points to allocator owning this block               */
      struct s_cxmalloc_block_t *next_block;  /* [4] points to next block in the re-use/hole chain, or NULL if no outbound chain exists */
      struct s_cxmalloc_block_t *prev_block;  /* [5] points to previous block in the re-use/hole chain, or NULL if no inbound chain exists */
    };
  };
  union {
    cacheline_t _CL2;
    struct {
      int64_t capacity;                       /* [6] total block capacity */
      int64_t available;                      /* [7] lines available for allocation */
      time_t min_until;                       /* [8] block will not be recycled until this time threshold has passed */
      int64_t reuse_threshold;                /* [9] insert block into re-use chain when reaching this amount of free lines */
      int64_t defrag_threshold;               /* [10] allow migration of line from this block via renew() when reaching this amount of free lines */
      cxmalloc_bidx_t bidx;                   /* [11] block number in allocator */
      uint16_t __rsv1_3[3]; 
      cxmalloc_bitvector_t active;
    };
  };
  union {
    cacheline_t _CL3;
    struct {
      int base_fileno;                        /* [12] block data file descriptor                           */
      int ext_fileno;                         /* [13] variable data file descriptor                        */
      CString_t *CSTR__blockdir;              /* [14] the block directory full path                        */
      CString_t *CSTR__base_path;             /* [15] block data file path                                 */
      CString_t *CSTR__ext_path;              /* [16] variable data file path                              */
      CQwordQueue_t *base_bulkin;             /* [17] block data input buffer for bulk deserialization     */
      CQwordQueue_t *base_bulkout;            /* [18] block data output buffer for bulk serialization      */
      CQwordQueue_t *ext_bulkin;              /* [19] variable data input buffer for bulk deserialization  */
      CQwordQueue_t *ext_bulkout;             /* [20] variable data output buffer for bulk serialization   */
    };
  };
} cxmalloc_block_t;



/*******************************************************************//**
 * _cxmalloc_bitvector_set
 ***********************************************************************
 */
__inline static void _cxmalloc_bitvector_set( cxmalloc_bitvector_t *bitvector, const cxmalloc_linehead_t *linehead ) {
  // line number in block
  uint32_t i = linehead->data.offset;
  // bit mask
  uint64_t m = 1ULL << (i & 0x3f);
  // bitvector qword
  QWORD *q = bitvector->data + (i >> 6);
  // set bit
  *q |= m;
}



/*******************************************************************//**
 * _cxmalloc_bitvector_clear
 ***********************************************************************
 */
__inline static void _cxmalloc_bitvector_clear( cxmalloc_bitvector_t *bitvector, const cxmalloc_linehead_t *linehead ) {
  // line number in block
  uint64_t i = linehead->data.offset;
  // bit mask
  uint64_t m = 1ULL << (i & 0x3f);
  // bitvector qword
  QWORD *q = bitvector->data + (i >> 6);
  // clear bit
  *q &= ~m;
}



/*******************************************************************//**
 * _cxmalloc_bitvector_is_set
 ***********************************************************************
 */
__inline static bool _cxmalloc_bitvector_is_set( const cxmalloc_bitvector_t *bitvector, uint64_t i ) {
  // bit mask
  uint64_t m = 1ULL << (i & 0x3f);
  // bitvector qword
  QWORD *q = bitvector->data + (i >> 6);
  // check bit
  return !!(*q & m);
}



/*******************************************************************//**
 * _cxmalloc_bitvector_count
 ***********************************************************************
 */
__inline static uint64_t _cxmalloc_bitvector_count( const cxmalloc_bitvector_t *bitvector ) {
  uint64_t n = 0;
  QWORD *q = bitvector->data;
  QWORD *end = q + bitvector->nq;
  while( q < end ) {
    n += __popcnt64( *q );
    ++q;
  }
  return n;
}



/*******************************************************************//**
 * cxmalloc_allocator_t
 ***********************************************************************
 */
CALIGNED_TYPE(struct) s_cxmalloc_allocator_t {
  // ------------------------------------------------
  union {
    cacheline_t _CL1;
    // [Q1]
    CS_LOCK alock;                            /* [1] allocator mutex */
  };
  // ------------------------------------------------
  union {
    cacheline_t _CL2;
    struct {
      // [Q2.1]
      struct s_cxmalloc_block_t **blocks;     /* [2] block pointer register (fixed size) */
      // [Q2.2]
      struct s_cxmalloc_block_t **head;       /* [3] currently active block for new allocations */
      // [Q2.3]
      struct s_cxmalloc_block_t *last_reuse;  /* [4] points to last block in a re-use chain, or NULL if no re-use chain exists */
      // [Q2.4]
      struct s_cxmalloc_block_t *last_hole;   /* [5] points to last hole in a hole chain, or NULL if no hole chain exists */
      // [Q2.5]
      struct s_cxmalloc_block_t **space;      /* [6] one beyond last existing block, where next new block will be */
      // [Q2.6]
      struct s_cxmalloc_block_t **end;        /* [7] one beyond last entry in block register */
      // [Q2.7]
      struct s_cxmalloc_family_t *family;     /* [8] points back to the parent family */
      // [Q2.8.1.1]
      uint16_t aidx;                          /* [9] allocator index */
      // [Q2.8.1.2]
      uint16_t __rsv_2_8_1_2;
      // [Q2.8.2]
      uint32_t __rsv_2_8_2;
    };
  };
  // ------------------------------------------------
  union {
    cacheline_t _CL3;
    struct {
      // [Q3.1.1]
      ATOMIC_VOLATILE_i32( readonly_atomic ); /* [10] */
      // [Q3.1.2]
      int ready;                              /* [11] allocator construction complete if 1 */
      // [Q3.2]
      const CString_t *CSTR__allocdir;        /* [12] base directory for allocator */
      // [Q3.3]
      int64_t n_active;                       /* [13] number of active objects */
      // [Q3.4]
      QWORD __rsv_3_4;
      // [Q3.5]
      QWORD __rsv_3_5;
      // [Q3.6]
      QWORD __rsv_3_6;
      // [Q3.7]
      QWORD __rsv_3_7;
      // [Q3.8]
      QWORD __rsv_3_8;
    };
  };
  // ------------------------------------------------
  union {
    cacheline_t _CL4;
    // [Q4]
    union u_cxmalloc_datashape_t shape;       /* [14] data shape descriptor for blocks in this allocator */
  };
} cxmalloc_allocator_t;




#ifdef CXMALLOC_CONSISTENCY_CHECK
#define CXMALLOC_ASSERT( Condition )          \
  do {                                        \
    if( !(Condition) ) {                      \
      FATAL( 0xFFF, "cxmalloc corruption!" ); \
    }                                         \
  }                                           \
  WHILE_ZERO

typedef float CXMALLOC_NOT_A_POINTER; /* will trigger compilation error if used as allocator pointer */
#define BEGIN_CXMALLOC_HIDE_POINTER( PointerToHide, PointerVisible )    \
      /* We hide the original variable to enforce */              \
      /* explicitly clear usage of the locked */                  \
      /* allocator in application code */                         \
      SUPPRESS_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL           \
      SUPPRESS_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER       \
      const CXMALLOC_NOT_A_POINTER PointerToHide=0;               \
      do {                                                        \
         if(!PointerToHide && (PointerVisible) != NULL )  /* do this to get rid of warning */

#define END_CXMALLOC_HIDE_POINTER  } WHILE_ZERO
#else
#define CXMALLOC_ASSERT( Condition ) ((void)0)
#define BEGIN_CXMALLOC_HIDE_POINTER( PointerToHide, PointerVisible ) IGNORE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED do
#define END_CXMALLOC_HIDE_POINTER RESUME_WARNINGS WHILE_ZERO
#endif

#define CONCAT_NAME( a, b ) a ## b


#define BEGIN_CXMALLOC_ALLOCATOR_READONLY( Allocator )      \
  do {                                                      \
    cxmalloc_allocator_t *__allocator__ = Allocator;        \
    ATOMIC_INCREMENT_i32( &__allocator__->readonly_atomic );\
    cxmalloc_allocator_t *Allocator##_RO = __allocator__;   \
    BEGIN_CXMALLOC_HIDE_POINTER( Allocator, CONCAT_NAME( Allocator, _RO ) )


#ifdef CXMALLOC_CONSISTENCY_CHECK
#define __assert_non_negative_readonly( A ) \
do {                                        \
  int r = ATOMIC_READ_i32( &(A)->readonly_atomic ); \
  CXMALLOC_ASSERT( r >= 0 );                \
} WHILE_ZERO
#else
#define __assert_non_negative_readonly( A ) ((void)0)
#endif


#define END_CXMALLOC_ALLOCATOR_READONLY                     \
    END_CXMALLOC_HIDE_POINTER;                              \
    ATOMIC_DECREMENT_i32( &__allocator__->readonly_atomic );\
    __assert_non_negative_readonly( __allocator__ );        \
  } WHILE_ZERO




#define IF_CXMALLOC_ALLOCATOR_WRITABLE( Allocator )     \
  do {                                                  \
    cxmalloc_allocator_t *Allocator##_W = Allocator;    \
    if( !ATOMIC_READ_i32( &(Allocator##_W)->readonly_atomic ) ) { \
      BEGIN_CXMALLOC_HIDE_POINTER( Allocator, CONCAT_NAME( Allocator, _W ) )

#define ENDIF_CXMALLOC_ALLOCATOR_WRITABLE               \
      END_CXMALLOC_HIDE_POINTER;                        \
    }                                                   \
  } WHILE_ZERO



#define BEGIN_CXMALLOC_FAMILY_READONLY( Family )    \
  do {                                              \
    cxmalloc_family_t *__family__ = Family;         \
    SYNCHRONIZE_ON( __family__->lock ) {            \
      __family__->readonly_cnt++;                   \
    } RELEASE;                                      \
    cxmalloc_family_t *Family##_RO = __family__;    \
    BEGIN_CXMALLOC_HIDE_POINTER( Family, CONCAT_NAME( Family, _RO ) )


#define END_CXMALLOC_FAMILY_READONLY                \
    END_CXMALLOC_HIDE_POINTER;                      \
    SYNCHRONIZE_ON( __family__->lock ) {            \
      __family__->readonly_cnt--;                   \
      CXMALLOC_ASSERT( __family__->readonly_cnt >= 0 ); \
    } RELEASE;                                      \
  } WHILE_ZERO


#define CXMALLOC_FAMILY_IS_READONLY( FamilyPtr ) ((FamilyPtr)->readonly > 0)
#define CXMALLOC_FAMILY_IS_WRITABLE( FamilyPtr ) ((FamilyPtr)->readonly == 0)


#define IF_CXMALLOC_FAMILY_WRITABLE( Family )     \
  do {                                            \
    cxmalloc_family_t *Family##_W = Family;       \
    if( (Family##_W)->readonly_cnt == 0 ) {       \
      BEGIN_CXMALLOC_HIDE_POINTER( Family, CONCAT_NAME( Family, _W ) )

#define ENDIF_CXMALLOC_FAMILY_WRITABLE            \
      END_CXMALLOC_HIDE_POINTER;                  \
    }                                             \
  } WHILE_ZERO





#define SYNCHRONIZE_CXMALLOC_ALLOCATOR( Allocator )     \
  SYNCHRONIZE_ON( (Allocator)->alock ) {                \
    cxmalloc_allocator_t *Allocator##_CS = Allocator;   \
    do {                                                \
      BEGIN_CXMALLOC_HIDE_POINTER( Allocator, CONCAT_NAME( Allocator, _CS ) )


#define RELEASE_CXMALLOC_ALLOCATOR                      \
      END_CXMALLOC_HIDE_POINTER;                        \
    } WHILE_ZERO;                                       \
  } RELEASE


#define SYNCHRONIZE_CXMALLOC_FAMILY( Family )             \
  SYNCHRONIZE_ON( (Family)->lock ) {                      \
    cxmalloc_family_t *Family##_CS = Family;              \
    do {                                                  \
      BEGIN_CXMALLOC_HIDE_POINTER( Family, CONCAT_NAME( Family, _CS ) )

#define RELEASE_CXMALLOC_FAMILY                           \
      END_CXMALLOC_HIDE_POINTER;                          \
    } WHILE_ZERO;                                         \
  } RELEASE



#define IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( Allocator ) \
  SYNCHRONIZE_CXMALLOC_ALLOCATOR( Allocator ) {             \
    IF_CXMALLOC_ALLOCATOR_WRITABLE( Allocator##_CS )

#define END_SYNCHRONIZED_WRITABLE_ALLOCATOR                 \
    ENDIF_CXMALLOC_ALLOCATOR_WRITABLE;                      \
  } RELEASE_CXMALLOC_ALLOCATOR



#define IF_WRITABLE_FAMILY_THEN_SYNCHRONIZE( Family )     \
  SYNCHRONIZE_CXMALLOC_FAMILY( Family ) {                 \
    IF_CXMALLOC_FAMILY_WRITABLE( Family##_CS )

#define END_SYNCHRONIZED_WRITABLE_FAMILY                  \
    ENDIF_CXMALLOC_FAMILY_WRITABLE;                       \
  } RELEASE_CXMALLOC_FAMILY





/*******************************************************************//**
 * Allocator template
 ***********************************************************************
 */
#define CXMALLOC_TEMPLATE( ATYPE, UTYPE, ARRAY, BlockBytes, MaxArraySize, ObjectSize, Growth, AllowOversized, MaxAllocators )  \
                                                                                                          \
                                                                                                          \
static const cxmalloc_descriptor_t g_##ATYPE##_descriptor = {                                             \
  .meta = {                                                                                               \
    .initval          = {0},                        /* metaflex M1, M2  */                                \
    .serialized_sz    = 0                           /* TBD              */                                \
  },                                                                                                      \
  .obj = {                                                                                                \
    .sz               = (ObjectSize),               /* object_bytes     */                                \
    .serialized_sz    = 0                           /* TBD              */                                \
  },                                                                                                      \
  .unit = {                                                                                               \
    .sz               = (uint32_t)sizeof( UTYPE ),  /* unit_sz          */                                \
    .serialized_sz    = 0                           /* TBD              */                                \
  },                                                                                                      \
  .serialize_line     = NULL,                       /*                  */                                \
  .deserialize_line   = NULL,                       /*                  */                                \
  .parameter = {                                                                                          \
    .block_sz         = (BlockBytes),               /*                  */                                \
    .line_limit       = (MaxArraySize),             /*                  */                                \
    .subdue           = (Growth),                   /*                  */                                \
    .allow_oversized  = (AllowOversized),           /*                  */                                \
    .max_allocators   = (MaxAllocators)             /*                  */                                \
  },                                                                                                      \
  .persist = {                                                                                            \
    .CSTR__path            = NULL,                  /*                  */                                \
  },                                                                                                      \
  .auxiliary = {                                                                                          \
    NULL                                                                                                  \
  }                                                                                                       \
};                                                                                                        \
                                                                                                          \
                                                                                                          \
static cxmalloc_family_t *g_##ATYPE##_family = NULL;                                                      \
                                                                                                          \
                                                                                                          \
/* for the static allocator instance only */                                                              \
static int ATYPE##_Init( void ) {                                                                         \
  int retcode = 0;                                                                                        \
  XTRY {                                                                                                   \
    if( g_##ATYPE##_family == NULL ) {                                                                    \
      cxmalloc_family_constructor_args_t args;                                                            \
      args.family_descriptor = &g_##ATYPE##_descriptor;                                                   \
      if( (g_##ATYPE##_family = COMLIB_OBJECT_NEW( cxmalloc_family_t, #ATYPE, &args )) == NULL ) {        \
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xAAA );                                                           \
      }                                                                                                   \
    }                                                                                                     \
  }                                                                                                       \
  XCATCH( errcode ) {                                                                                      \
    retcode = -1;                                                                                         \
  }                                                                                                       \
  XFINALLY {}                                                                                              \
  return retcode;                                                                                         \
}                                                                                                         \
                                                                                                          \
                                                                                                          \
static void ATYPE##_Clear( void ) {                                                                       \
  if( g_##ATYPE##_family != NULL ) {                                                                      \
    COMLIB_OBJECT_DESTROY( g_##ATYPE##_family );                                                          \
    g_##ATYPE##_family = NULL;                                                                            \
  }                                                                                                       \
}                                                                                                         \
                                                                                                          \
/* Allocate */                                                                                                                                    \
static UTYPE * ATYPE##_New( uint32_t sz )                                 { return CALL( g_##ATYPE##_family, New, sz ); }                         \
/* Line array methods */                                                                                                                          \
static UTYPE * ATYPE##_Renew( UTYPE *ARRAY )                              { return CALL( g_##ATYPE##_family, Renew, ARRAY ); }                    \
static int64_t ATYPE##_Own( UTYPE *ARRAY )                                { return CALL( g_##ATYPE##_family, Own, ARRAY ); }                      \
static int64_t ATYPE##_Discard( UTYPE *ARRAY )                            { return CALL( g_##ATYPE##_family, Discard, ARRAY ); }                  \
static int64_t ATYPE##_RefCount( const UTYPE *ARRAY )                     { return CALL( g_##ATYPE##_family, RefCount, ARRAY ); }                 \
static uint32_t ATYPE##_PrevLength( const UTYPE *ARRAY )                  { return CALL( g_##ATYPE##_family, PrevLength, ARRAY ); }               \
static uint32_t ATYPE##_NextLength( const UTYPE *ARRAY )                  { return CALL( g_##ATYPE##_family, NextLength, ARRAY ); }               \
static uint32_t ATYPE##_LengthOf( const UTYPE *ARRAY )                    { return CALL( g_##ATYPE##_family, LengthOf, ARRAY ); }                 \
static uint16_t ATYPE##_IndexOf( const UTYPE *ARRAY )                     { return CALL( g_##ATYPE##_family, IndexOf, ARRAY ); }                  \
static void * ATYPE##_ObjectFromArray( const UTYPE *ARRAY )               { return CALL( g_##ATYPE##_family, ObjectFromArray, ARRAY ); }          \
static cxmalloc_handle_t ATYPE##_ArrayAsHandle( const UTYPE *ARRAY )      { return CALL( g_##ATYPE##_family, ArrayAsHandle, ARRAY ); }            \
static void * ATYPE##_HandleAsArray( cxmalloc_handle_t handle )           { return CALL( g_##ATYPE##_family, HandleAsArray, handle ); }           \
static cxmalloc_metaflex_t * ATYPE##_Meta( const UTYPE *ARRAY )           { return CALL( g_##ATYPE##_family, Meta, ARRAY ); }                     \
/* Object methods */                                                                                                                              \
static void * ATYPE##_RenewObject( void *obj )                            { return CALL( g_##ATYPE##_family, RenewObject, obj ); }                \
static int64_t ATYPE##_OwnObjectNolock( void *obj )                       { return CALL( g_##ATYPE##_family, OwnObjectNolock, obj ); }            \
static int64_t ATYPE##_OwnObject( void *obj )                             { return CALL( g_##ATYPE##_family, OwnObject, obj ); }                  \
static int64_t ATYPE##_OwnObjectByHandle( cxmalloc_handle_t handle )      { return CALL( g_##ATYPE##_family, OwnObjectByHandle, handle ); }       \
static int64_t ATYPE##_DiscardObjectNolock( void *obj )                   { return CALL( g_##ATYPE##_family, DiscardObjectNolock, obj ); }        \
static int64_t ATYPE##_DiscardObject( void *obj )                         { return CALL( g_##ATYPE##_family, DiscardObject, obj ); }              \
static int64_t ATYPE##_DiscardObjectByHandle( cxmalloc_handle_t handle )  { return CALL( g_##ATYPE##_family, DiscardObjectByHandle, handle ); }   \
static int64_t ATYPE##_RefCountObjectNolock( const void *obj )            { return CALL( g_##ATYPE##_family, RefCountObjectNolock, obj ); }       \
static int64_t ATYPE##_RefCountObject( const void *obj )                  { return CALL( g_##ATYPE##_family, RefCountObject, obj ); }             \
static int64_t ATYPE##_RefCountObjectByHandle( cxmalloc_handle_t handle ) { return CALL( g_##ATYPE##_family, RefCountObjectByHandle, handle ); }  \
static void * ATYPE##_ArrayFromObject( const void *obj )                  { return CALL( g_##ATYPE##_family, ArrayFromObject, obj ); }            \
static cxmalloc_handle_t ATYPE##_ObjectAsHandle( const void *obj )        { return CALL( g_##ATYPE##_family, ObjectAsHandle, obj ); }             \
static void * ATYPE##_HandleAsObjectNolock( cxmalloc_handle_t handle )    { return CALL( g_##ATYPE##_family, HandleAsObjectNolock, handle ); }    \
static void * ATYPE##_HandleAsObjectSafe( cxmalloc_handle_t handle )      { return CALL( g_##ATYPE##_family, HandleAsObjectSafe, handle ); }      \
/* Family methods */                                                                                                                              \
static uint32_t ATYPE##_MinLength( void )                                 { return CALL( g_##ATYPE##_family, MinLength ); }                       \
static uint32_t ATYPE##_MaxLength( void )                                 { return CALL( g_##ATYPE##_family, MaxLength ); }                       \
static uint16_t ATYPE##_SizeBounds( uint32_t sz, uint32_t *low, uint32_t *high ) { return CALL( g_##ATYPE##_family, SizeBounds, sz, low, high ); } \
static void ATYPE##_LazyDiscards( int use_lazy_discards )                 { CALL( g_##ATYPE##_family, LazyDiscards, use_lazy_discards ); }        \
static int ATYPE##_Check( void )                                          { return CALL( g_##ATYPE##_family, Check ); }                           \
static comlib_object_t * ATYPE##_GetObjectAtAddress( QWORD address )      { return CALL( g_##ATYPE##_family, GetObjectAtAddress, address ); }     \
static comlib_object_t * ATYPE##_FindObjectByObid( objectid_t obid )      { return CALL( g_##ATYPE##_family, FindObjectByObid, obid ); }          \
static comlib_object_t * ATYPE##_GetObjectByOffset( int64_t *poffset )      { return CALL( g_##ATYPE##_family, GetObjectByOffset, poffset ); }    \
static int64_t ATYPE##_Sweep( f_get_object_identifier get_object_identifier ) { return CALL( g_##ATYPE##_family, Sweep, get_object_identifier ); }  \
static int ATYPE##_Size( void )                                           { return CALL( g_##ATYPE##_family, Size ); }                            \
static int64_t ATYPE##_Bytes( void )                                      { return CALL( g_##ATYPE##_family, Bytes ); }                           \
static histogram_t * ATYPE##_Histogram( void )                            { return CALL( g_##ATYPE##_family, Histogram ); }                       \
static int64_t ATYPE##_Active( void )                                     { return CALL( g_##ATYPE##_family, Active ); }                          \
static double ATYPE##_Utilization( void )                                 { return CALL( g_##ATYPE##_family, Utilization ); }                     \
/* Readonly */                                                                                                                                    \
static int ATYPE##_SetReadonly( void )                                    { return CALL( g_##ATYPE##_family, SetReadonly );  }                    \
static int ATYPE##_IsReadonly( void )                                     { return CALL( g_##ATYPE##_family, IsReadonly );  }                     \
static int ATYPE##_ClearReadonly( void )                                  { return CALL( g_##ATYPE##_family, ClearReadonly );  }                  \
/* Serialization */                                                                                                                               \
static int64_t ATYPE##_BulkSerialize( bool force )                        { return CALL( g_##ATYPE##_family, BulkSerialize, force );  }           \
static int64_t ATYPE##_RestoreObjects( void )                             { return CALL( g_##ATYPE##_family, RestoreObjects );  }                 \
static int64_t ATYPE##_ProcessObjects( cxmalloc_object_processing_context_t *context )  { return CALL( g_##ATYPE##_family, ProcessObjects, context );  }   \
                                                                                                                                                  \
                                                                                                                                                  \
static const ATYPE##_interface_t g_##ATYPE##Interface = {                                                     \
  .Init                   = ATYPE##_Init,                                                                     \
  .Clear                  = ATYPE##_Clear,                                                                    \
/* Allocate */                                                                                                \
  .New                    = ATYPE##_New,                                                                      \
/* Line array methods */                                                                                      \
  .Renew                  = ATYPE##_Renew,                                                                    \
  .Own                    = ATYPE##_Own,                                                                      \
  .Discard                = ATYPE##_Discard,                                                                  \
  .RefCount               = ATYPE##_RefCount,                                                                 \
  .PrevLength             = ATYPE##_PrevLength,                                                               \
  .NextLength             = ATYPE##_NextLength,                                                               \
  .LengthOf               = ATYPE##_LengthOf,                                                                 \
  .IndexOf                = ATYPE##_IndexOf,                                                                  \
  .ObjectFromArray        = ATYPE##_ObjectFromArray,                                                          \
  .ArrayAsHandle          = ATYPE##_ArrayAsHandle,                                                            \
  .HandleAsArray          = ATYPE##_HandleAsArray,                                                            \
  .Meta                   = ATYPE##_Meta,                                                                     \
/* Object methods */                                                                                          \
  .RenewObject            = ATYPE##_RenewObject,                                                              \
  .OwnObjectNolock        = ATYPE##_OwnObjectNolock,                                                          \
  .OwnObject              = ATYPE##_OwnObject,                                                                \
  .OwnObjectByHandle      = ATYPE##_OwnObjectByHandle,                                                        \
  .DiscardObjectNolock    = ATYPE##_DiscardObjectNolock,                                                      \
  .DiscardObject          = ATYPE##_DiscardObject,                                                            \
  .DiscardObjectByHandle  = ATYPE##_DiscardObjectByHandle,                                                    \
  .RefCountObjectNolock   = ATYPE##_RefCountObjectNolock,                                                     \
  .RefCountObject         = ATYPE##_RefCountObject,                                                           \
  .RefCountObjectByHandle = ATYPE##_RefCountObjectByHandle,                                                   \
  .ArrayFromObject        = ATYPE##_ArrayFromObject,                                                          \
  .ObjectAsHandle         = ATYPE##_ObjectAsHandle,                                                           \
  .HandleAsObjectNolock   = ATYPE##_HandleAsObjectNolock,                                                     \
  .HandleAsObjectSafe     = ATYPE##_HandleAsObjectSafe,                                                       \
/* Family methods */                                                                                          \
  .MinLength              = ATYPE##_MinLength,                                                                \
  .MaxLength              = ATYPE##_MaxLength,                                                                \
  .SizeBounds             = ATYPE##_SizeBounds,                                                               \
  .LazyDiscards           = ATYPE##_LazyDiscards,                                                             \
  .Check                  = ATYPE##_Check,                                                                    \
  .GetObjectAtAddress     = ATYPE##_GetObjectAtAddress,                                                       \
  .FindObjectByObid       = ATYPE##_FindObjectByObid,                                                         \
  .GetObjectByOffset      = ATYPE##_GetObjectByOffset,                                                        \
  .Sweep                  = ATYPE##_Sweep,                                                                    \
  .Size                   = ATYPE##_Size,                                                                     \
  .Bytes                  = ATYPE##_Bytes,                                                                    \
  .Histogram              = ATYPE##_Histogram,                                                                \
  .Active                 = ATYPE##_Active,                                                                   \
  .Utilization            = ATYPE##_Utilization,                                                              \
/* Readonly */                                                                                                \
  .SetReadonly            = ATYPE##_SetReadonly,                                                              \
  .IsReadonly             = ATYPE##_IsReadonly,                                                               \
  .ClearReadonly          = ATYPE##_ClearReadonly,                                                            \
/* Serialization */                                                                                           \
  .BulkSerialize          = ATYPE##_BulkSerialize,                                                            \
  .RestoreObjects         = ATYPE##_RestoreObjects,                                                           \
  .ProcessObjects         = ATYPE##_ProcessObjects,                                                           \
/* Descriptor */                                                                                              \
  .descriptor             = &g_##ATYPE##_descriptor                                                           \
};                                                                                                            \
                                                                                                              \
                                                                                                              \
DLL_EXPORT extern const ATYPE##_interface_t * ATYPE##Interface( void ) { return &g_##ATYPE##Interface; }             \
DLL_EXPORT extern cxmalloc_family_t * ATYPE##Singleton( void ) { return g_##ATYPE##_family; }                        \

#define CXMALLOC_META_FROM_ARRAY( ATYPE, Mx, ArrayPtr )       (&CALLABLE(g_##ATYPE##_family)->Meta( g_##ATYPE##_family, ArrayPtr )->M##Mx)
#define CXMALLOC_TOPAPTR_FROM_ARRAY( ATYPE, ArrayPtr )        (&CALLABLE(g_##ATYPE##_family)->Meta( g_##ATYPE##_family, ArrayPtr )->aptr)
#define CXMALLOC_OBJECT_FROM_ARRAY( HTYPE, ATYPE, ArrayPtr )  ((HTYPE*)CALLABLE(g_##ATYPE##_family)->ObjectFromArray( g_##ATYPE##_family, ArrayPtr ))




/*******************************************************************//**
 * _cxmalloc_linehead_from_array
 ***********************************************************************
 */
__inline static cxmalloc_linehead_t * _cxmalloc_linehead_from_array( const cxmalloc_family_t *family, const void *line ) {
  return (cxmalloc_linehead_t*)( (char*)line - family->header_bytes );
}



/*******************************************************************//**
 * _cxmalloc_object_from_array
 ***********************************************************************
 */
__inline static void * _cxmalloc_object_from_array( const cxmalloc_family_t *family, const void *line ) {
  return (void*)( (char*)line - family->object_bytes );
}



/*******************************************************************//**
 * _cxmalloc_linehead_from_object
 ***********************************************************************
 */
__inline static cxmalloc_linehead_t * _cxmalloc_linehead_from_object( const void *obj ) {
  return (cxmalloc_linehead_t*)( (char*)obj - sizeof(cxmalloc_linehead_t) );
}



/*******************************************************************//**
 * _cxmalloc_is_object_active
 ***********************************************************************
 */
__inline static int _cxmalloc_is_object_active( const void *obj ) {
  return _cxmalloc_linehead_from_object( obj )->data.flags._act;
}



/*******************************************************************//**
 * _cxmalloc_array_from_object
 ***********************************************************************
 */
__inline static void * _cxmalloc_array_from_object( const cxmalloc_family_t *family, const void *obj ) {
  return (void*)( (char*)obj + family->object_bytes );
}



/*******************************************************************//**
 * _cxmalloc_object_from_linehead
 ***********************************************************************
 */
__inline static void * _cxmalloc_object_from_linehead( const cxmalloc_linehead_t *linehead ) {
  return (void*)( (char*)linehead + sizeof(cxmalloc_linehead_t) );
}



/*******************************************************************//**
 * _cxmalloc_array_from_linehead
 ***********************************************************************
 */
__inline static void * _cxmalloc_array_from_linehead( const cxmalloc_family_t *family, const cxmalloc_linehead_t *linehead ) {
  return (void*)( (char*)linehead + family->header_bytes );
}


/*******************************************************************//**
 * _cxmalloc_object_as_handle
 ***********************************************************************
 */
__inline static cxmalloc_handle_t _cxmalloc_object_as_handle( const void *obj ) {
  cxmalloc_handle_t handle = {0};
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );
  handle.allocdata = linehead->data.handle;
  return handle;
}


/*******************************************************************//**
 * _cxmalloc_object_incref_nolock
 ***********************************************************************
 */
__inline static int64_t _cxmalloc_object_incref_nolock( void *obj ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );
#ifndef CXMALLOC_CONSISTENCY_CHECK
  return ++(linehead->data.refc);
#else
  int32_t refc = ++(linehead->data.refc);
  if( refc < 0 ) {
    printf( "Refcount wrapped to negative!\n" );
  }
#if defined (CXMALLOC_MAX_REFCOUNT) && (CXMALLOC_MAX_REFCOUNT > 0)
  static const int32_t max_refc = CXMALLOC_MAX_REFCOUNT;
  if( refc > max_refc ) {
    printf( "Object @ %p refcount=%d: ", obj, refc );
    COMLIB_OBJECT_PRINT( obj );
  }
#endif
  return refc;
#endif
}


/*******************************************************************//**
 * _cxmalloc_object_incref_delta_nolock
 ***********************************************************************
 */
__inline static int64_t _cxmalloc_object_incref_delta_nolock( void *obj, unsigned delta ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );
  linehead->data.refc += delta;
#ifdef CXMALLOC_CONSISTENCY_CHECK
  if( linehead->data.refc < 0 ) {
    printf( "Refcount wrapped to negative!\n" );
  }
#if defined (CXMALLOC_MAX_REFCOUNT) && (CXMALLOC_MAX_REFCOUNT > 0)
  static const int32_t max_refc = CXMALLOC_MAX_REFCOUNT;
  if( linehead->data.refc > max_refc ) {
    printf( "Object @ %p refcount=%d: ", obj, linehead->data.refc );
    COMLIB_OBJECT_PRINT( obj );
  }
#endif
#endif
  return linehead->data.refc;
}


/*******************************************************************//**
 * _cxmalloc_object_refcnt_nolock
 ***********************************************************************
 */
__inline static int64_t _cxmalloc_object_refcnt_nolock( const void *obj ) {
  return _cxmalloc_linehead_from_object( obj )->data.refc;
}


/*******************************************************************//**
 * _cxmalloc_object_set_modified_nolock
 ***********************************************************************
 */
__inline static void _cxmalloc_object_set_modified_nolock( const void *obj ) {
  _cxmalloc_linehead_from_object( obj )->data.flags._mod = 1;
}


/*******************************************************************//**
 * _cxmalloc_id_string
 ***********************************************************************
 */
__inline static const char * _cxmalloc_id_string( const cxmalloc_family_t *family ) {
  return family->obid.longstring.string;
}



/*******************************************************************//**
 * _cxmalloc_id_prefix
 ***********************************************************************
 */
__inline static const char * _cxmalloc_id_prefix( const cxmalloc_family_t *family ) {
  return family->obid.longstring.prefix;
}



/*******************************************************************//**
 * _icxmalloc_chain
 ***********************************************************************
 */
typedef struct _s_icxmalloc_chain_t {
  cxmalloc_block_t * (*AppendBlock_ACS)(  cxmalloc_block_t *last_CS, cxmalloc_block_t *block_CS );
  cxmalloc_block_t * (*InsertBlock_ACS)(  cxmalloc_block_t *previous_CS, cxmalloc_block_t *block_CS );
  cxmalloc_block_t * (*RemoveBlock_ACS)(  cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS );
  void               (*ManageChain_ACS)(  cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS );
  cxmalloc_block_t * (*AdvanceBlock_ACS)( cxmalloc_allocator_t *allocator_CS );
  int                (*RemoveHoles_OPEN)( cxmalloc_allocator_t *allocator );
} _icxmalloc_chain_t;

DLL_HIDDEN extern _icxmalloc_chain_t _icxmalloc_chain;



/*******************************************************************//**
 * _icxmalloc_block
 ***********************************************************************
 */
typedef struct _s_icxmalloc_block_t {
  int                 (*GetFilepaths_OPEN)(       const CString_t *CSTR__allocator_path, const uint16_t aidx, const cxmalloc_bidx_t bidx, CString_t **CSTR__path, CString_t **CSTR__dat_path, CString_t **CSTR__var_path );
  cxmalloc_block_t *  (*New_ACS)(                 cxmalloc_allocator_t *allocator_CS, const cxmalloc_bidx_t bidx, bool allocate_data );
  void                (*Delete_ACS)(              cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t* block_CS );
  int                 (*CreateData_ACS)(          cxmalloc_block_t *block_CS );
  void                (*DestroyData_ACS)(         cxmalloc_block_t *block_CS );
  int64_t             (*ComputeAvailable_ARO)(    const cxmalloc_block_t *block_RO );
  int64_t             (*ComputeActive_ARO)(       const cxmalloc_block_t *block_RO );
  int64_t             (*CountActiveLines_ARO)(    const cxmalloc_block_t *block_RO );
  int64_t             (*CountRefcntLines_ARO)(    const cxmalloc_block_t *block_RO );
  int64_t             (*Modified_ARO)(            const cxmalloc_block_t *block_RO );
  bool                (*NeedsPersist_ARO)(        const cxmalloc_block_t *block_RO );
  int64_t             (*RestoreObjects_FCS_ACS)(  cxmalloc_block_t *block_CS );
  int64_t             (*ValidateRefcounts_ACS)(   const cxmalloc_block_t *block_CS, cxmalloc_linehead_t **bad_linehead );
  comlib_object_t *   (*GetObjectAtAddress_ACS)(  const cxmalloc_block_t *block_CS, QWORD address );
  comlib_object_t *   (*FindObjectByObid_ACS)(    const cxmalloc_block_t *block_CS, objectid_t obid );
  comlib_object_t *   (*GetObject_ACS)(           const cxmalloc_block_t *block_CS, int64_t n );
  int64_t             (*Sweep_FCS_ACS)(           cxmalloc_block_t *block_CS, f_get_object_identifier get_object_identifier );
  cxmalloc_linehead_t ** (*FindLostLines_ACS)(    cxmalloc_block_t *block_CS, int force );
  CStringQueue_t *    (*Repr_ARO)(                const cxmalloc_block_t *block_RO, CStringQueue_t *output );
} _icxmalloc_block_t;

DLL_HIDDEN extern _icxmalloc_block_t _icxmalloc_block;



/*******************************************************************//**
 * _icxmalloc_shape
 ***********************************************************************
 */
typedef struct _s_icxmalloc_shape_t {
  //uint16_t               (*GetAIDX_FRO)(        cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength, uint16_t *aidx );
  uint16_t               (*GetAIDX_FRO)(        cxmalloc_family_t *family_RO, const uint32_t sz, uint32_t *alength );
  uint32_t               (*GetLength_FRO)(      const cxmalloc_family_t *family_RO, const uint16_t aidx );
  cxmalloc_datashape_t * (*ComputeShape_FRO)(   cxmalloc_family_t *family_RO, const uint16_t aidx, cxmalloc_datashape_t *shape );
  size_t                 (*BlockBytes_ARO)(     cxmalloc_block_t *block_RO );
  size_t                 (*AllocatorBytes_ACS)( cxmalloc_allocator_t *allocator_CS );
  int64_t                (*FamilyBytes_FCS)(    cxmalloc_family_t *family_CS );
  histogram_t *          (*Histogram_FCS)(      cxmalloc_family_t *family_CS );
} _icxmalloc_shape_t;

DLL_HIDDEN extern _icxmalloc_shape_t _icxmalloc_shape;



/*******************************************************************//**
 * _icxmalloc_family
 ***********************************************************************
 */
typedef struct _s_icxmalloc_family_t {
  cxmalloc_family_t * (*NewFamily_OPEN)(          const char *id, const cxmalloc_descriptor_t *descriptor );
  void                (*DeleteFamily_OPEN)(       cxmalloc_family_t **family );
  int64_t             (*RestoreObjects_FCS)(      cxmalloc_family_t *family_CS );
  CStringQueue_t *    (*Repr_FCS)(                cxmalloc_family_t *family_CS, CStringQueue_t *output );
  int64_t             (*ValidateRefcounts_FCS)(   cxmalloc_family_t *family_CS, cxmalloc_linehead_t **bad_linehead );
  comlib_object_t *   (*GetObjectAtAddress_FCS)(  cxmalloc_family_t *family_CS, QWORD address );
  comlib_object_t *   (*FindObjectByObid_FCS)(    cxmalloc_family_t *family_CS, objectid_t obid );
  comlib_object_t *   (*GetObjectByOffset_FCS)(   cxmalloc_family_t *family_CS, int64_t *poffset );
  int64_t             (*Sweep_OPEN)(              cxmalloc_family_t *family, f_get_object_identifier get_object_identifier );
  int64_t             (*ComputeActive_OPEN)(      cxmalloc_family_t *family );
  int64_t             (*Active_OPEN)(             cxmalloc_family_t *family );
  double              (*Utilization_OPEN)(        cxmalloc_family_t *family );
} _icxmalloc_family_t;

DLL_HIDDEN extern _icxmalloc_family_t _icxmalloc_family;



/*******************************************************************//**
 * _icxmalloc_allocator
 ***********************************************************************
 */
typedef struct _s_icxmalloc_allocator_t {
  cxmalloc_allocator_t *  (*CreateAllocator_FCS)(     cxmalloc_family_t *family_CS, const uint16_t aidx );
  void                    (*DestroyAllocator_FCS)(    cxmalloc_family_t *family_CS, cxmalloc_allocator_t **ppallocator );
  cxmalloc_linehead_t *   (*New_ACS)(                 cxmalloc_allocator_t *allocator_CS );
  cxmalloc_block_t *      (*Delete_ACS)(              cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t *linehead );
  int64_t                 (*RestoreObjects_FCS_ACS)(  cxmalloc_allocator_t *allocator_CS );
  int64_t                 (*ValidateRefcounts_ACS)(   cxmalloc_allocator_t *allocator_CS, cxmalloc_linehead_t **bad_linehead );
  comlib_object_t *       (*GetObjectAtAddress_ACS)(  cxmalloc_allocator_t *allocator_CS, QWORD address );
  comlib_object_t *       (*FindObjectByObid_ACS)(    cxmalloc_allocator_t *allocator_CS, objectid_t obid );
  comlib_object_t *       (*GetObject_ACS)(           cxmalloc_allocator_t *allocator_CS, int64_t n );
  int64_t                 (*Sweep_FCS_ACS)(           cxmalloc_allocator_t *allocator_CS, f_get_object_identifier get_object_identifier );
} _icxmalloc_allocator_t;

DLL_HIDDEN extern _icxmalloc_allocator_t _icxmalloc_allocator;



/*******************************************************************//**
 * _icxmalloc_line
 ***********************************************************************
 */
typedef struct _s_icxmalloc_line_t {
  void * (*New_OPEN)( cxmalloc_family_t *family, uint32_t size );
  void * (*NewOversized_OPEN)( cxmalloc_family_t *family, uint32_t size );
  void * (*Renew_OPEN)( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
  int64_t (*Discard_OPEN)( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
  int64_t (*Discard_NOLOCK)( cxmalloc_family_t *family, cxmalloc_linehead_t *linehead );
  int (*ValidateInactive_BCS)( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead );
  int64_t (*ValidateRefcount_BCS)( const cxmalloc_block_t *block_CS, cxmalloc_linehead_t *linehead );
} _icxmalloc_line_t;

DLL_HIDDEN extern _icxmalloc_line_t _icxmalloc_line;



/*******************************************************************//**
 * _icxmalloc_serialization
 ***********************************************************************
 */
typedef struct _s_icxmalloc_serialization_t {
  int64_t (*PersistFamily_OPEN)(    cxmalloc_family_t *family, bool force );
  int64_t (*PersistAllocator_ARO)(  cxmalloc_allocator_t *allocator_RO, bool force );
  int64_t (*PersistBlock_ARO)(      cxmalloc_block_t *block_RO, bool force );
  int64_t (*RestoreFamily_FCS)(     cxmalloc_family_t *family_CS );
  int64_t (*RestoreAllocator_ACS)(  cxmalloc_allocator_t *allocator_CS );
  int64_t (*RestoreBlock_ACS)(      cxmalloc_block_t *block_CS );
  int64_t (*RemoveBlock_ARO)(       const cxmalloc_allocator_t *allocator_RO, const cxmalloc_bidx_t bidx );
} _icxmalloc_serialization_t;

DLL_HIDDEN extern _icxmalloc_serialization_t _icxmalloc_serialization;



/*******************************************************************//**
 * _icxmalloc_object_processor
 ***********************************************************************
 */
typedef struct _s_icxmalloc_object_processor_t {
  int64_t (*ProcessFamily_FCS)(     cxmalloc_family_t *family_CS, cxmalloc_object_processing_context_t *context );
  int64_t (*ProcessAllocator_ACS)(  cxmalloc_allocator_t *allocator_CS, cxmalloc_object_processing_context_t *context );
  int64_t (*ProcessBlock_ACS)(      cxmalloc_block_t *block_CS, cxmalloc_object_processing_context_t *context );
} _icxmalloc_object_processor_t;

DLL_HIDDEN extern _icxmalloc_object_processor_t _icxmalloc_object_processor;



DLL_HIDDEN extern cxmalloc_family_vtable_t cxmalloc_family_Methods;



#endif
