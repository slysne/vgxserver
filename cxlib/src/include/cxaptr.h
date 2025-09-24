/*
###################################################
#
# File:   cxaptr.h
#
###################################################
*/
#ifndef CXMLIB_CXAPTR_H
#define CXMLIB_CXAPTR_H

/* Set to 1 to replace functions with macros */
#define USE_APTR_MACROS 1

#include "cxexcept.h"

/*******************************************************************//**
 * Annotated pointer
 * 
 ***********************************************************************
 */

/* The number of address bits used by the target CPU */
#define __ARCH_ADDRESS_BITS 48


#if __ARCH_ADDRESS_BITS == 48
#define __ARCH_MEMORY_48  /* 256 TB*/
#elif __ARCH_ADDRESS_BITS == 52
#define __ARCH_MEMORY_52  /* 4 PB */
#else
#error "unsupported architecture"
#endif




typedef enum e_tptr_tagvalue_t {
                                          // N D T 
  TPTR_TAG_END                    = 0x0,  // 0 0 0  end marker in array of tagged pointers
  TPTR_TAG_ARRAY_POINTER          = 0x0,  // 0 0 0  pointer points to an array of some sort
  TPTR_TAG_SINGLETON_POINTER      = 0x1,  // 0 0 1  pointer points to a single element
  TPTR_TAG_DIRTY_INVALID_POINTER  = 0x2,  // 0 1 0  invalid pointer that needs to be persisted
  TPTR_TAG_DIRTY_VALID_POINTER    = 0x3,  // 0 1 1  valid pointer that needs to be persisted
  TPTR_TAG_EMPTY                  = 0x4,  // 1 0 0  unused slot in array of tagged pointers
  TPTR_TAG_NON_POINTER_DATA       = 0x5,  // 1 0 1  interpret as raw data, not address
  TPTR_TAG_DIRTY_INVALID_DATA     = 0x6,  // 1 1 0  invalid data that needs to be persisted
  TPTR_TAG_DIRTY_VALID_DATA       = 0x7   // 1 1 1  valid data that needs to be persisted
} tptr_tagvalue_t;

#define TPTR_TAG_MASK 0x7


typedef enum e_tagged_dtype_t {
  /* */
  TAGGED_DTYPE_ID56    = 0x0, //  000
  TAGGED_DTYPE_BOOL    = 0x1, //  001
  TAGGED_DTYPE_INT56   = 0x2, //  010
  TAGGED_DTYPE_UINT56  = 0x3, //  011
  TAGGED_DTYPE_REAL56  = 0x4, //  100
  TAGGED_DTYPE_PTR56   = 0x5, //  101
  TAGGED_DTYPE_OBJ56   = 0x6, //  110
  TAGGED_DTYPE_HANDLE  = 0x7, //  111
} tagged_dtype_t;



typedef enum e_tagged_dkey_t {
  /* */
  TAGGED_DKEY_NONE     = 0x0,  //  00
  TAGGED_DKEY_PLAIN64  = 0x1,  //  01
  TAGGED_DKEY_HASH64   = 0x2,  //  10
  TAGGED_DKEY_HASH128  = 0x3,  //  11
} tagged_dkey_t;



typedef union u_tptr_tag_t {
  /* tag view */
  struct {                    /* NDT                      NDT                                     */
                              /* ---                      ---                                     */
    uintptr_t valid     : 1;  /* xx0: data is invalid     xx1: data is valid                      */
    uintptr_t dirty     : 1;  /* x0x: data is clean       x1x: data is dirty (for use in caches)  */
    uintptr_t nonptr    : 1;  /* 0xx: data is a pointer   1xx: data is integer value              */
    uintptr_t ___       : 61; /* (don't touch) */
  } bit;
  struct {
    struct {
      uint32_t value    : 3;  /* -------- -------- -------- -----NDT */
      uint32_t ___      : 29; /* -------- -------- -------- -------- (don't touch) */
    };
    uint32_t ___32;           /* (don't touch) */
  };
} tptr_tag_t;



typedef struct s_tptr_metas_t {
  /* metas view */
  uint16_t ___[3];        /* (don't touch 48-bit address or handle data) */
  struct {
    uint8_t __52   : 4;   /* (don't touch 52-bit address) */
    uint8_t _rsv   : 4;   /* reserved */
  };
  uint8_t value;          /* metas */
} tptr_metas_t;



typedef struct s_tptr_class_t {
  /* class view */
  uint16_t ___[3];        /* (don't touch 48-bit address) */
  uint8_t code;           /* class code */
  uint8_t __56__63;       /* (don't touch) */
} tptr_class_t;



typedef struct s_tptr_ncptr_t {
  /* pointer view (non-canonical)*/
  struct {
    uintptr_t ___0__2     : 3;  /* (don't touch) */
#if defined __ARCH_MEMORY_48
    uintptr_t qwo         : 45; /* non-canonical 48-bit address's QWORD offset - must sign extend before dereference */
    uintptr_t ___48__51   : 4;  /* (don't touch) */
#elif defined __ARCH_MEMORY_52
    uintptr_t qwo         : 49; /* non-canonical 52-bit address's QWORD offset - must sign extend before dereference */
#else
#error "unsupported architecture"
#endif
    uintptr_t ___52__63   : 12; /* (don't touch) */
  };
} tptr_ncptr_t;



typedef union u_tptr_data_t {
  /* data view */
  struct {
    struct {
      uint32_t ___0__2    : 3;  /* (don't touch) */
      uint32_t type       : 3;  /* 3-bit custom data type field */
      uint32_t key        : 2;  /* 2-bit custom data key field */ 
      uint32_t ___24__31  : 24; /* (don't touch) */
    };
    uint32_t ___0__31;          /* (don't touch) */
  };
  struct {
    uint64_t ___0__7u     : 8;  /* (don't touch) */
    uint64_t uval56       : 56; /* used for 56-bit unsigned integer data, interpret as 56 LSB (truncate upper 8 bits) */
  };
  struct {
    int64_t ___0__7i      : 8;  /* (don't touch) */
    int64_t ival56        : 56; /* used for 56-bit signed integer data, interpret as 56 LSB (truncate upper 8 bits) */
  };
} tptr_data_t;



typedef struct s_tptr_handle_t {
  /* handle view  */
  union {
    struct {
      struct {
        uint32_t ___0__7      : 8;    /* (don't touch) */
        uint32_t offset       : 24;   /* offset within block */
      };
      uint16_t bidx;                  /* block number       */
      uint16_t aidx;                  /* allocator number   */
    };
    struct {
      uint64_t ___low         : 8;
      uint64_t value          : 56;
    };
  };
} tptr_handle_t;



typedef union u_tptr_t {
  QWORD qword;
  int64_t int64;
  tptr_tag_t tag;
  tptr_metas_t metas;
  tptr_ncptr_t ncptr;
  tptr_data_t data;
  tptr_handle_t handle;
} tptr_t;


typedef struct s_x2tptr_t {
  tptr_t t_1;
  tptr_t t_2;
} x2tptr_t;


typedef union u_aptr_t {
  union {
    struct {
      uint64_t annotation;
      tptr_t tptr;
    };
#if defined CXPLAT_ARCH_X64
    __m128i m128i;
#elif defined CXPLAT_ARCH_ARM64
    int64x2_t m128i;
#endif
  };
} aptr_t;



#if defined __ARCH_MEMORY_48
#define __TPTR_SIGN_EXTEND_SHIFT 16
#define __TPTR_PACK_MASK 0x00001FFFFFFFFFFFULL  /* 45 bits = 48 bit pointer without lower 3 tag bits */
#elif defined __ARCH_MEMORY_52
#define __TPTR_SIGN_EXTEND_SHIFT 12
#define __TPTR_PACK_MASK 0x0001FFFFFFFFFFFFULL  /* 49 bits = 52 bit pointer without lower 3 tag bits */
#else
#error "unsupported architecture"
#endif



#define INT56_MAX ((1LL<<55)-1) 
#define INT56_MIN (-(1LL<<55))
#define INT56_MASK ((1ULL<<56)-1)


extern double REINTERPRET_CAST_DOUBLE( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_16( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_20( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_24( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_28( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_32( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_36( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_40( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_44( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_48( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_52( QWORD qw );
extern double REINTERPRET_CAST_DOUBLE_56( QWORD qw );


typedef union u_double_bits_t {
  QWORD qw;
  // 16-bit double
  struct {
    QWORD __trunc48 : 48;
    QWORD d16       : 16;
  };
  // 20-bit double
  struct {
    QWORD __trunc44 : 44;
    QWORD d20       : 20;
  };
  // 24-bit double
  struct {
    QWORD __trunc40 : 40;
    QWORD d24       : 24;
  };
  // 28-bit double
  struct {
    QWORD __trunc36 : 36;
    QWORD d28       : 28;
  };
  // 32-bit double
  struct {
    QWORD __trunc32 : 32;
    QWORD d32       : 32;
  };
  // 36-bit double
  struct {
    QWORD __trunc28 : 28;
    QWORD d36       : 36;
  };
  // 40-bit double
  struct {
    QWORD __trunc24 : 24;
    QWORD d40       : 40;
  };
  // 44-bit double
  struct {
    QWORD __trunc20 : 20;
    QWORD d44       : 44;
  };
  // 48-bit double
  struct {
    QWORD __trunc16 : 16;
    QWORD d48       : 48;
  };
  // 52-bit double
  struct {
    QWORD __trunc12 : 12;
    QWORD d52       : 52;
  };
  // 56-bit double
  struct {
    QWORD __trunc8  : 8;
    QWORD d56       : 56;
  };
  double d;
} double_bits_t;


extern float REINTERPRET_CAST_FLOAT( DWORD dw );
extern float REINTERPRET_CAST_FLOAT_16( DWORD dw );
extern float REINTERPRET_CAST_FLOAT_20( DWORD dw );
extern float REINTERPRET_CAST_FLOAT_24( DWORD dw );
extern float REINTERPRET_CAST_FLOAT_28( DWORD dw );


typedef union u_float_bits_t {
  DWORD dw;
  // 16-bit float
  struct {
    DWORD __trunc16 : 16;
    DWORD f16       : 16;
  };
  // 20-bit float
  struct {
    DWORD __trunc12 : 12;
    DWORD f20       : 20;
  };
  // 24-bit float
  struct {
    DWORD __trunc8  :  8;
    DWORD f24       : 24;
  };
  // 28-bit float
  struct {
    DWORD __trunc4  :  4;
    DWORD f28       : 28;
  };
  float f;
} float_bits_t;



#define __TPTR_PACK( Ptr )                                (((intptr_t)(Ptr) >> 3) & __TPTR_PACK_MASK)
#define __TPTR_UNPACK( Bits )                             (((intptr_t)(Bits) << (3 + __TPTR_SIGN_EXTEND_SHIFT)) >> __TPTR_SIGN_EXTEND_SHIFT)


#define TPTR_AS_QWORD( TaggedPointer )                    ((TaggedPointer)->qword)
#define TPTR_AS_INT64( TaggedPointer )                    ((TaggedPointer)->int64)
#define TPTR_AS_TAG( TaggedPointer )                      ((TaggedPointer)->tag.value)
#define TPTR_AS_METAS( TaggedPointer )                    ((TaggedPointer)->metas.value)
#define TPTR_AS_DTYPE( TaggedPointer )                    ((TaggedPointer)->data.type)
#define TPTR_AS_DKEY( TaggedPointer )                     ((TaggedPointer)->data.key)
#define TPTR_AS_UNSIGNED( TaggedPointer )                 ((TaggedPointer)->data.uval56)
#define TPTR_AS_INTEGER( TaggedPointer )                  ((TaggedPointer)->data.ival56)
#define TPTR_AS_AIDX( TaggedPointer )                     ((TaggedPointer)->handle.aidx)
#define TPTR_AS_BIDX( TaggedPointer )                     ((TaggedPointer)->handle.bidx)
#define TPTR_AS_OFFSET( TaggedPointer )                   ((TaggedPointer)->handle.offset)
#define TPTR_AS_HANDLE( TaggedPointer )                   ((TaggedPointer)->handle.value)



#define APTR_AS_QWORD( AnnotatedPointer )                 TPTR_AS_QWORD( &(AnnotatedPointer)->tptr )
#define APTR_AS_INT64( AnnotatedPointer )                 TPTR_AS_INT64( &(AnnotatedPointer)->tptr )
#define APTR_AS_TAG( AnnotatedPointer )                   TPTR_AS_TAG( &(AnnotatedPointer)->tptr )
#define APTR_AS_METAS( AnnotatedPointer )                 TPTR_AS_METAS( &(AnnotatedPointer)->tptr )
#define APTR_AS_DTYPE( AnnotatedPointer )                 TPTR_AS_DTYPE( &(AnnotatedPointer)->tptr )
#define APTR_AS_DKEY( AnnotatedPointer )                  TPTR_AS_DKEY( &(AnnotatedPointer)->tptr )
#define APTR_AS_UNSIGNED( AnnotatedPointer )              TPTR_AS_UNSIGNED( &(AnnotatedPointer)->tptr )
#define APTR_AS_INTEGER( AnnotatedPointer )               TPTR_AS_INTEGER( &(AnnotatedPointer)->tptr )
#define APTR_AS_AIDX( AnnotatedPointer )                  TPTR_AS_AIDX( &(AnnotatedPointer)->tptr )
#define APTR_AS_BIDX( AnnotatedPointer )                  TPTR_AS_BIDX( &(AnnotatedPointer)->tptr )
#define APTR_AS_OFFSET( AnnotatedPointer )                TPTR_AS_OFFSET( &(AnnotatedPointer)->tptr )
#define APTR_AS_HANDLE( AnnotatedPointer )                TPTR_AS_HANDLE( &(AnnotatedPointer)->tptr )
#define APTR_AS_ANNOTATION( AnnotatedPointer )            (AnnotatedPointer)->annotation






/* tagged pointer initialization */
extern void _TPTR_INIT( tptr_t *TP );

/* annotated pointer initialization */
extern void _APTR_INIT( aptr_t *AP );

/* tagged pointer getters */
extern void * _TPTR_GET_POINTER( tptr_t *TP );
extern uint64_t _TPTR_GET_IDHIGH( tptr_t *TP );
extern bool _TPTR_GET_BOOLEAN( tptr_t *TP );
extern double _TPTR_GET_REAL( tptr_t *TP );
extern void * _TPTR_GET_PTR56( tptr_t *TP );
extern void * _TPTR_GET_OBJ56( tptr_t *TP );

/* tagged pointer setters */
extern void _TPTR_SET_POINTER( tptr_t *TP, const void *p );
extern void _TPTR_SET_POINTER_AND_TAG( tptr_t *TP, const void *p, int tag );
extern void _TPTR_SET_IDHIGH( tptr_t *TP, QWORD id );
extern void _TPTR_SET_IDHIGH_AND_TAG( tptr_t *TP, QWORD id, int tag );
extern void _TPTR_SET_BOOLEAN( tptr_t *TP, bool b );
extern void _TPTR_SET_BOOLEAN_AND_TAG( tptr_t *TP, bool b, int tag );
extern void _TPTR_SET_UNSIGNED( tptr_t *TP, uint64_t u );
extern void _TPTR_SET_UNSIGNED_AND_TAG( tptr_t *TP, uint64_t u, int tag );
extern void _TPTR_SET_INTEGER( tptr_t *TP, int64_t i );
extern void _TPTR_SET_INTEGER_AND_TAG( tptr_t *TP, int64_t i, int tag );
extern void _TPTR_SET_REAL( tptr_t *TP, double r );
extern void _TPTR_SET_REAL_AND_TAG( tptr_t *TP, double r, int tag );
extern void _TPTR_SET_PTR56( tptr_t *TP, const void *p );
extern void _TPTR_SET_PTR56_AND_TAG( tptr_t *TP, const void *p, int tag );
extern void _TPTR_SET_OBJ56( tptr_t *TP, const void *op );
extern void _TPTR_SET_OBJ56_AND_TAG( tptr_t *TP, const void *op, int tag );
extern void _TPTR_SET_HANDLE( tptr_t *TP, const uint64_t handle_value );
extern void _TPTR_SET_HANDLE_AND_TAG( tptr_t *TP, const uint64_t handle_value, int tag );

/* annotated pointer getters */
extern void * _APTR_GET_ANNOTATION_POINTER( aptr_t *AP );
extern void * _APTR_GET_POINTER( aptr_t *AP );
extern uint64_t _APTR_GET_IDHIGH( aptr_t *AP );
extern bool _APTR_GET_BOOLEAN( aptr_t *AP );
extern double _APTR_GET_REAL( aptr_t *AP );
extern void * _APTR_GET_PTR56( aptr_t *AP );
extern void * _APTR_GET_OBJ56( aptr_t *AP );

/* annotated pointer setters */
extern void _APTR_SET_POINTER( aptr_t *AP, const void *p );
extern void _APTR_SET_POINTER_AND_TAG( aptr_t *AP, const void *p, int tag );
extern void _APTR_SET_IDHIGH( aptr_t *AP, QWORD id );
extern void _APTR_SET_IDHIGH_AND_TAG( aptr_t *AP, QWORD id, int tag );
extern void _APTR_SET_BOOLEAN( aptr_t *AP, bool b );
extern void _APTR_SET_BOOLEAN_AND_TAG( aptr_t *AP, bool b, int tag );
extern void _APTR_SET_UNSIGNED( aptr_t *AP, uint64_t u );
extern void _APTR_SET_UNSIGNED_AND_TAG( aptr_t *AP, uint64_t u, int tag );
extern void _APTR_SET_INTEGER( aptr_t *AP, int64_t i );
extern void _APTR_SET_INTEGER_AND_TAG( aptr_t *AP, int64_t i, int tag );
extern void _APTR_SET_REAL( aptr_t *AP, double r );
extern void _APTR_SET_REAL_AND_TAG( aptr_t *AP, double r, int tag );
extern void _APTR_SET_PTR56( aptr_t *AP, const void *p );
extern void _APTR_SET_PTR56_AND_TAG( aptr_t *AP, const void *p, int tag );
extern void _APTR_SET_OBJ56( aptr_t *AP, const void *op );
extern void _APTR_SET_OBJ56_AND_TAG( aptr_t *AP, const void *op, int tag );
extern void _APTR_SET_HANDLE( aptr_t *AP, const uint64_t handle_value );
extern void _APTR_SET_HANDLE_AND_TAG( aptr_t *AP, const uint64_t handle_value, int tag );

/* tagged pointer matchers */
extern bool _TPTR_MATCH_POINTER( const tptr_t *TP, const void *p );
extern bool _TPTR_MATCH_IDHIGH( const tptr_t *TP, const QWORD id );
extern bool _TPTR_MATCH_BOOLEAN( const tptr_t *TP, const bool b );
extern bool _TPTR_MATCH_UNSIGNED( const tptr_t *TP, const uint64_t u );
extern bool _TPTR_MATCH_INTEGER( const tptr_t *TP, const int64_t i );
extern bool _TPTR_MATCH_REAL( const tptr_t *TP, const double r );
extern bool _TPTR_MATCH_PTR56( const tptr_t *TP, const void *p );
extern bool _TPTR_MATCH_OBJ56( const tptr_t *TP, const void *op );
extern bool _TPTR_MATCH_HANDLE( const tptr_t *TP, const uint64_t handle_value );
extern bool _TPTR_MATCH( const tptr_t *TP1, const tptr_t *TP2 );

/* annotated pointer matchers */
extern bool _APTR_MATCH_ANNOTATION( const aptr_t *AP, const QWORD a );
extern bool _APTR_MATCH_POINTER( const aptr_t *AP, const void *p );
extern bool _APTR_MATCH_IDHIGH( const aptr_t *AP, const QWORD id );
extern bool _APTR_MATCH_BOOLEAN( const aptr_t *AP, const bool b );
extern bool _APTR_MATCH_UNSIGNED( const aptr_t *AP, const uint64_t u );
extern bool _APTR_MATCH_INTEGER( const aptr_t *AP, const int64_t i );
extern bool _APTR_MATCH_REAL( const aptr_t *AP, const double r );
extern bool _APTR_MATCH_PTR56( const aptr_t *AP, const void *p );
extern bool _APTR_MATCH_OBJ56( const aptr_t *AP, const void *op );
extern bool _APTR_MATCH_HANDLE( const aptr_t *AP, const uint64_t handle_value );
extern bool _APTR_MATCH( const aptr_t *AP1, const aptr_t *AP2 );

/* tagged pointer tests */
extern bool _TPTR_IS_NULL( const tptr_t *TP );
extern bool _TPTR_IS_VALID( const tptr_t *TP );
extern bool _TPTR_IS_INVALID( const tptr_t *TP );
extern bool _TPTR_IS_DIRTY( const tptr_t *TP );
extern bool _TPTR_IS_CLEAN( const tptr_t *TP );
extern bool _TPTR_IS_NONPTR( const tptr_t *TP );
extern bool _TPTR_IS_POINTER( const tptr_t *TP );

/* annotated pointer tests */
extern bool _APTR_IS_NULL( const aptr_t *AP );
extern bool _APTR_IS_VALID( const aptr_t *AP );
extern bool _APTR_IS_INVALID( const aptr_t *AP );
extern bool _APTR_IS_DIRTY( const aptr_t *AP );
extern bool _APTR_IS_CLEAN( const aptr_t *AP );
extern bool _APTR_IS_NONPTR( const aptr_t *AP );
extern bool _APTR_IS_POINTER( const aptr_t *AP );

/* tagged pointer tag setters */
extern void _TPTR_MAKE_VALID( tptr_t *TP );
extern void _TPTR_MAKE_INVALID( tptr_t *TP );
extern void _TPTR_MAKE_DIRTY( tptr_t *TP );
extern void _TPTR_MAKE_CLEAN( tptr_t *TP );
extern void _TPTR_MAKE_NONPTR( tptr_t *TP );
extern void _TPTR_MAKE_POINTER( tptr_t *TP );

/* annotated pointer tag setters */
extern void _APTR_MAKE_VALID( aptr_t *AP );
extern void _APTR_MAKE_INVALID( aptr_t *AP );
extern void _APTR_MAKE_DIRTY( aptr_t *AP );
extern void _APTR_MAKE_CLEAN( aptr_t *AP );
extern void _APTR_MAKE_NONPTR( aptr_t *AP );
extern void _APTR_MAKE_POINTER( aptr_t *AP );

/* tagged pointer copy */
extern void _TPTR_COPY( tptr_t *Dest, const tptr_t *Src );

/* tagged pointer swap */
void _TPTR_SWAP( tptr_t *TP1, tptr_t *TP2 );

/* annotated pointer copy */
extern void _APTR_COPY( aptr_t *Dest, const aptr_t *Src );

/* annotated pointer swap */
extern void _APTR_SWAP( aptr_t *AP1, aptr_t *AP2 );


#if USE_APTR_MACROS

/* tagged pointer initialization */
#define TPTR_INIT( TaggedPointer )                              (TPTR_AS_QWORD( TaggedPointer ) = 0)

/* annotated pointer initialization */
#if defined CXPLAT_ARCH_X64
#define APTR_INIT( AnnotatedPointer )                           ((AnnotatedPointer)->m128i = _mm_xor_si128( (AnnotatedPointer)->m128i, (AnnotatedPointer)->m128i )); /* x^x = 0 */
#elif defined CXPLAT_ARCH_ARM64
#define APTR_INIT( AnnotatedPointer )                           ((AnnotatedPointer)->m128i = veorq_s64( (AnnotatedPointer)->m128i, (AnnotatedPointer)->m128i )); /* x^x = 0 */
#else
#error "Unsupported architecture"
#endif

/* tagged pointer getters */
#define TPTR_GET_POINTER( TaggedPointer )                       ((void*)__TPTR_UNPACK((TaggedPointer)->ncptr.qwo))   /* remove tag, sign extend, and cast data to void pointer */
#define TPTR_GET_IDHIGH( TaggedPointer )                        ((TaggedPointer)->data.uval56)
#define TPTR_GET_BOOLEAN( TaggedPointer )                       ((bool)((TaggedPointer)->data.uval56 != 0))
#define TPTR_GET_REAL( TaggedPointer )                          REINTERPRET_CAST_DOUBLE_56( (TaggedPointer)->data.uval56 )
#define TPTR_GET_PTR56( TaggedPointer )                         ((void*)((intptr_t)(TaggedPointer)->data.ival56))
#define TPTR_GET_OBJ56( TaggedPointer )                         ((void*)((intptr_t)(TaggedPointer)->data.ival56))

/* tagged pointer setters */
#define TPTR_SET_POINTER( TaggedPointer, Ptr )                  do { (TaggedPointer)->ncptr.qwo = __TPTR_PACK(Ptr); (TaggedPointer)->tag.bit.nonptr = 0; } WHILE_ZERO
#define TPTR_SET_POINTER_AND_TAG( TaggedPointer, Ptr, Tag )     do { (TaggedPointer)->ncptr.qwo = __TPTR_PACK(Ptr); (TaggedPointer)->tag.value = (Tag); } WHILE_ZERO

#define TPTR_SET_IDHIGH( TaggedPointer, Id56 )                  do { (TaggedPointer)->data.uval56 = (uint64_t)(Id56);   (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_ID56;   } WHILE_ZERO
#define TPTR_SET_IDHIGH_AND_TAG( TaggedPointer, Id56, Tag )     do { (TaggedPointer)->data.uval56 = (uint64_t)(Id56);   (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_ID56;   } WHILE_ZERO

#define TPTR_SET_BOOLEAN( TaggedPointer, Bool )                 do { (TaggedPointer)->data.uval56 = (uint64_t)(Bool);   (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_BOOL;   } WHILE_ZERO
#define TPTR_SET_BOOLEAN_AND_TAG( TaggedPointer, Bool, Tag )    do { (TaggedPointer)->data.uval56 = (uint64_t)(Bool);   (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_BOOL;   } WHILE_ZERO

#define TPTR_SET_UNSIGNED( TaggedPointer, UInt56 )              do { (TaggedPointer)->data.uval56 = (uint64_t)(UInt56); (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_UINT56; } WHILE_ZERO
#define TPTR_SET_UNSIGNED_AND_TAG( TaggedPointer, UInt56, Tag ) do { (TaggedPointer)->data.uval56 = (uint64_t)(UInt56); (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_UINT56; } WHILE_ZERO

#define TPTR_SET_INTEGER( TaggedPointer, Int56 )                do { (TaggedPointer)->data.ival56 = (int64_t)(Int56);  (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_INT56;  } WHILE_ZERO
#define TPTR_SET_INTEGER_AND_TAG( TaggedPointer, Int56, Tag )   do { (TaggedPointer)->data.ival56 = (int64_t)(Int56);  (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_INT56;  } WHILE_ZERO

#define TPTR_SET_REAL( TaggedPointer, Real56 )                  do { double d = (Real56); (TaggedPointer)->data.uval56 = ((double_bits_t*)&d)->d56; (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_REAL56; } WHILE_ZERO
#define TPTR_SET_REAL_AND_TAG( TaggedPointer, Real56, Tag )     do { double d = (Real56); (TaggedPointer)->data.uval56 = ((double_bits_t*)&d)->d56; (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_REAL56; } WHILE_ZERO

#define TPTR_SET_PTR56( TaggedPointer, PlainPointer )           do { (TaggedPointer)->data.ival56 = (int64_t)(PlainPointer);  (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_PTR56;  } WHILE_ZERO
#define TPTR_SET_PTR56_AND_TAG( TaggedPointer, PlainPointer, Tag ) do { (TaggedPointer)->data.ival56 = (int64_t)(PlainPointer);  (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_PTR56;  } WHILE_ZERO

#define TPTR_SET_OBJ56( TaggedPointer, ObjectPointer )          do { (TaggedPointer)->data.ival56 = (int64_t)(ObjectPointer);  (TaggedPointer)->tag.bit.nonptr = 1; (TaggedPointer)->data.type = TAGGED_DTYPE_OBJ56;  } WHILE_ZERO
#define TPTR_SET_OBJ56_AND_TAG( TaggedPointer, ObjectPointer, Tag ) do { (TaggedPointer)->data.ival56 = (int64_t)(ObjectPointer);  (TaggedPointer)->tag.value = (Tag); (TaggedPointer)->data.type = TAGGED_DTYPE_OBJ56;  } WHILE_ZERO

#define TPTR_SET_HANDLE( TaggedPointer, HandleValue )           do { (TaggedPointer)->handle.value = (uint64_t)(HandleValue); (TaggedPointer)->data.type = TAGGED_DTYPE_HANDLE; (TaggedPointer)->tag.bit.nonptr = 1; } WHILE_ZERO
#define TPTR_SET_HANDLE_AND_TAG( TaggedPointer, HandleValue, Tag ) do { (TaggedPointer)->handle.value = (uint64_t)(HandleValue); (TaggedPointer)->data.type = TAGGED_DTYPE_HANDLE; (TaggedPointer)->tag.value = (Tag); } WHILE_ZERO

/* annotated pointer getters */
#define APTR_GET_ANNOTATION_POINTER( AnnotatedPointer )                 ((void*)(&((AnnotatedPointer)->annotation)))
#define APTR_GET_POINTER( AnnotatedPointer )                            TPTR_GET_POINTER( &(AnnotatedPointer)->tptr )
#define APTR_GET_IDHIGH( AnnotatedPointer )                             TPTR_GET_IDHIGH( &(AnnotatedPointer)->tptr )
#define APTR_GET_BOOLEAN( AnnotatedPointer )                            TPTR_GET_BOOLEAN( &(AnnotatedPointer)->tptr )
#define APTR_GET_REAL( AnnotatedPointer )                               TPTR_GET_REAL( &(AnnotatedPointer)->tptr )
#define APTR_GET_PTR56( AnnotatedPointer )                              TPTR_GET_PTR56( &(AnnotatedPointer)->tptr )
#define APTR_GET_OBJ56( AnnotatedPointer )                              TPTR_GET_OBJ56( &(AnnotatedPointer)->tptr )

/* annotated pointer setters */
#define APTR_SET_POINTER( AnnotatedPointer, Ptr )                       TPTR_SET_POINTER( &(AnnotatedPointer)->tptr, Ptr )
#define APTR_SET_POINTER_AND_TAG( AnnotatedPointer, Ptr, Tag )          TPTR_SET_POINTER_AND_TAG( &(AnnotatedPointer)->tptr, Ptr, Tag )
#define APTR_SET_IDHIGH( AnnotatedPointer, Id56 )                       TPTR_SET_IDHIGH( &(AnnotatedPointer)->tptr, Id56 )
#define APTR_SET_IDHIGH_AND_TAG( AnnotatedPointer, Id56, Tag )          TPTR_SET_IDHIGH_AND_TAG( &(AnnotatedPointer)->tptr, Id56, Tag )
#define APTR_SET_BOOLEAN( AnnotatedPointer, Bool )                      TPTR_SET_BOOLEAN( &(AnnotatedPointer)->tptr, Bool )
#define APTR_SET_BOOLEAN_AND_TAG( AnnotatedPointer, Bool, Tag )         TPTR_SET_BOOLEAN_AND_TAG( &(AnnotatedPointer)->tptr, Bool, Tag )
#define APTR_SET_UNSIGNED( AnnotatedPointer, UInt56 )                   TPTR_SET_UNSIGNED( &(AnnotatedPointer)->tptr, UInt56 )
#define APTR_SET_UNSIGNED_AND_TAG( AnnotatedPointer, UInt56, Tag )      TPTR_SET_UNSIGNED_AND_TAG( &(AnnotatedPointer)->tptr, UInt56, Tag )
#define APTR_SET_INTEGER( AnnotatedPointer, Int56 )                     TPTR_SET_INTEGER( &(AnnotatedPointer)->tptr, Int56 )
#define APTR_SET_INTEGER_AND_TAG( AnnotatedPointer, Int56, Tag )        TPTR_SET_INTEGER_AND_TAG( &(AnnotatedPointer)->tptr, Int56, Tag )
#define APTR_SET_REAL( AnnotatedPointer, Real56 )                       TPTR_SET_REAL( &(AnnotatedPointer)->tptr, Real56 )
#define APTR_SET_REAL_AND_TAG( AnnotatedPointer, Real56, Tag )          TPTR_SET_REAL_AND_TAG( &(AnnotatedPointer)->tptr, Real56, Tag )
#define APTR_SET_PTR56( AnnotatedPointer, PlainPointer )                TPTR_SET_PTR56( &(AnnotatedPointer)->tptr, PlainPointer )
#define APTR_SET_PTR56_AND_TAG( AnnotatedPointer, PlainPointer, Tag )   TPTR_SET_PTR56_AND_TAG( &(AnnotatedPointer)->tptr, PlainPointer, Tag )
#define APTR_SET_OBJ56( AnnotatedPointer, ObjectPointer )               TPTR_SET_OBJ56( &(AnnotatedPointer)->tptr, ObjectPointer )
#define APTR_SET_OBJ56_AND_TAG( AnnotatedPointer, ObjectPointer, Tag )  TPTR_SET_OBJ56_AND_TAG( &(AnnotatedPointer)->tptr, ObjectPointer, Tag )
#define APTR_SET_HANDLE( AnnotatedPointer, HandleValue )                TPTR_SET_HANDLE( &(AnnotatedPointer)->tptr, HandleValue )
#define APTR_SET_HANDLE_AND_TAG( AnnotatedPointer, HandleValue, Tag )   TPTR_SET_HANDLE_AND_TAG( &(AnnotatedPointer)->tptr, HandleValue, Tag )

/* tagged pointer matchers */
#define TPTR_MATCH_POINTER( TaggedPointer, Ptr )                ( (void*)__TPTR_UNPACK((TaggedPointer)->ncptr.qwo) == (void*)(Ptr) )
#define TPTR_MATCH_IDHIGH( TaggedPointer, Id56 )                ((TaggedPointer)->data.uval56 == ((Id56) & 0x00FFFFFFFFFFFFFFULL) )   /* truncate to 56 LSB match */
#define TPTR_MATCH_BOOLEAN( TaggedPointer, Bool )               (((TaggedPointer)->data.uval56 != 0) == ((Bool) != 0) )               /* nonzero = true */
#define TPTR_MATCH_UNSIGNED( TaggedPointer, UInt56 )            ((TaggedPointer)->data.uval56 == ((UInt56) & 0x00FFFFFFFFFFFFFFULL) ) /* truncate to 56 LSB match */
#define TPTR_MATCH_INTEGER( TaggedPointer, Int56 )              ((TaggedPointer)->data.ival56 == (Int56) )                            /* sign extension should be implied */
#define TPTR_MATCH_REAL( TaggedPointer, Real56 )                ((TaggedPointer)->data.uval56 == ( ((double_bits_t*)&(Real56))->d56 ))/* effective epsilon: 2^-44  */
#define TPTR_MATCH_PTR56( TaggedPointer, PlainPointer )         ((TaggedPointer)->data.ival56 == (PlainPointer) )                     /* sign extension should be implied */
#define TPTR_MATCH_OBJ56( TaggedPointer, ObjectPointer )        ((TaggedPointer)->data.ival56 == (ObjectPointer) )                    /* sign extension should be implied */
#define TPTR_MATCH_HANDLE( TaggedPointer, HandleValue )         ((TaggedPointer)->handle.value == (HandleValue) )                     /*  */
#define TPTR_MATCH( TP1, TP2 )                                  ( (TP1)->qword == (TP2)->qword )

/* annotated pointer matchers */
#define APTR_MATCH_ANNOTATION( AnnotatedPointer, Value )        ((AnnotatedPointer)->annotation == (Value))
#define APTR_MATCH_POINTER( AnnotatedPointer, Ptr )             TPTR_MATCH_POINTER( &(AnnotatedPointer)->tptr, Ptr )
#define APTR_MATCH_IDHIGH( AnnotatedPointer, Id56 )             TPTR_MATCH_IDHIGH( &(AnnotatedPointer)->tptr, Id56 )
#define APTR_MATCH_BOOLEAN( AnnotatedPointer, Bool )            TPTR_MATCH_BOOLEAN( &(AnnotatedPointer)->tptr, Bool )
#define APTR_MATCH_UNSIGNED( AnnotatedPointer, UInt56 )         TPTR_MATCH_UNSIGNED( &(AnnotatedPointer)->tptr, UInt56 )
#define APTR_MATCH_INTEGER( AnnotatedPointer, Int56 )           TPTR_MATCH_INTEGER( &(AnnotatedPointer)->tptr, Int56 )
#define APTR_MATCH_REAL( AnnotatedPointer, Real56 )             TPTR_MATCH_REAL( &(AnnotatedPointer)->tptr, Real56 )
#define APTR_MATCH_PTR56( AnnotatedPointer, PlainPointer )      TPTR_MATCH_PTR56( &(AnnotatedPointer)->tptr, PlainPointer )
#define APTR_MATCH_OBJ56( AnnotatedPointer, ObjectPointer )     TPTR_MATCH_OBJ56( &(AnnotatedPointer)->tptr, ObjectPointer )
#define APTR_MATCH_HANDLE( AnnotatedPointer, HandleValue )      TPTR_MATCH_HANDLE( &(AnnotatedPointer)->tptr, HandleValue )
#define APTR_MATCH( AP1, AP2 )                                  ( (AP1)->annotation == (AP2)->annotation && TPTR_MATCH( &(AP1)->tptr, &(AP2)->tptr ) )

/* tagged pointer tests */
#define TPTR_IS_NULL( TaggedPointer )                     ((TaggedPointer)->ncptr.qwo == 0 && (TaggedPointer)->tag.bit.nonptr == 0)
#define TPTR_IS_VALID( TaggedPointer )                    ((TaggedPointer)->tag.bit.valid == 1)
#define TPTR_IS_INVALID( TaggedPointer )                  ((TaggedPointer)->tag.bit.valid == 0)
#define TPTR_IS_DIRTY( TaggedPointer )                    ((TaggedPointer)->tag.bit.dirty == 1)
#define TPTR_IS_CLEAN( TaggedPointer )                    ((TaggedPointer)->tag.bit.dirty == 0)
#define TPTR_IS_NONPTR( TaggedPointer )                   ((TaggedPointer)->tag.bit.nonptr == 1)
#define TPTR_IS_POINTER( TaggedPointer )                  ((TaggedPointer)->tag.bit.nonptr == 0)

/* annotated pointer tests */
#define APTR_IS_NULL( AnnotatedPointer )                  TPTR_IS_NULL( &(AnnotatedPointer)->tptr )
#define APTR_IS_VALID( AnnotatedPointer )                 TPTR_IS_VALID( &(AnnotatedPointer)->tptr )
#define APTR_IS_INVALID( AnnotatedPointer )               TPTR_IS_INVALID( &(AnnotatedPointer)->tptr )
#define APTR_IS_DIRTY( AnnotatedPointer )                 TPTR_IS_DIRTY( &(AnnotatedPointer)->tptr )
#define APTR_IS_CLEAN( AnnotatedPointer )                 TPTR_IS_CLEAN( &(AnnotatedPointer)->tptr )
#define APTR_IS_NONPTR( AnnotatedPointer )                TPTR_IS_NONPTR( &(AnnotatedPointer)->tptr )
#define APTR_IS_POINTER( AnnotatedPointer )               TPTR_IS_POINTER( &(AnnotatedPointer)->tptr )

/* tagged pointer tag setters */
#define TPTR_MAKE_VALID( TaggedPointer )                  ((TaggedPointer)->tag.bit.valid = 1)
#define TPTR_MAKE_INVALID( TaggedPointer )                ((TaggedPointer)->tag.bit.valid = 0)
#define TPTR_MAKE_DIRTY( TaggedPointer )                  ((TaggedPointer)->tag.bit.dirty = 1)
#define TPTR_MAKE_CLEAN( TaggedPointer )                  ((TaggedPointer)->tag.bit.dirty = 0)
#define TPTR_MAKE_NONPTR( TaggedPointer )                 ((TaggedPointer)->tag.bit.nonptr = 1)
#define TPTR_MAKE_POINTER( TaggedPointer )                ((TaggedPointer)->tag.bit.nonptr = 0)

/* annotated pointer tag setters */
#define APTR_MAKE_VALID( AnnotatedPointer )               TPTR_MAKE_VALID( &(AnnotatedPointer)->tptr )
#define APTR_MAKE_INVALID( AnnotatedPointer )             TPTR_MAKE_INVALID( &(AnnotatedPointer)->tptr )
#define APTR_MAKE_DIRTY( AnnotatedPointer )               TPTR_MAKE_DIRTY( &(AnnotatedPointer)->tptr )
#define APTR_MAKE_CLEAN( AnnotatedPointer )               TPTR_MAKE_CLEAN( &(AnnotatedPointer)->tptr )
#define APTR_MAKE_NONPTR( AnnotatedPointer )              TPTR_MAKE_NONPTR( &(AnnotatedPointer)->tptr )
#define APTR_MAKE_POINTER( AnnotatedPointer )             TPTR_MAKE_POINTER( &(AnnotatedPointer)->tptr )

/* tagged pointer copy */
#define TPTR_COPY( DestPtr, SrcPtr )                      ((DestPtr)->qword = (SrcPtr)->qword)
#define TPTR_SWAP( TP1, TP2 )                             elemswap( QWORD, (TP1)->qword, (TP2)->qword )

/* annotated pointer copy */
#define APTR_COPY( DestPtr, SrcPtr )                      do { (DestPtr)->annotation = (SrcPtr)->annotation; TPTR_COPY( &(DestPtr)->tptr, &(SrcPtr)->tptr ); } WHILE_ZERO
#define APTR_SWAP( AP1, AP2 )                             do { elemswap( uint64_t, (AP1)->annotation, (AP2)->annotation ); TPTR_SWAP( &(AP1)->tptr, &(AP2)->tptr ); } WHILE_ZERO



#else

/* tagged pointer initialization */
#define TPTR_INIT _TPTR_INIT

/* annotated pointer initialization */
#define APTR_INIT _APTR_INIT

/* tagged pointer getters */
#define TPTR_GET_POINTER          _TPTR_GET_POINTER
#define TPTR_GET_IDHIGH           _TPTR_GET_IDHIGH
#define TPTR_GET_BOOLEAN          _TPTR_GET_BOOLEAN
#define TPTR_GET_REAL             _TPTR_GET_REAL
#define TPTR_GET_PTR56            _TPTR_GET_PTR56
#define TPTR_GET_OBJ56            _TPTR_GET_OBJ56

/* tagged pointer setters */
#define TPTR_SET_POINTER          _TPTR_SET_POINTER
#define TPTR_SET_POINTER_AND_TAG  _TPTR_SET_POINTER_AND_TAG
#define TPTR_SET_IDHIGH           _TPTR_SET_IDHIGH
#define TPTR_SET_IDHIGH_AND_TAG   _TPTR_SET_IDHIGH_AND_TAG
#define TPTR_SET_BOOLEAN          _TPTR_SET_BOOLEAN
#define TPTR_SET_BOOLEAN_AND_TAG  _TPTR_SET_BOOLEAN_AND_TAG
#define TPTR_SET_UNSIGNED         _TPTR_SET_UNSIGNED
#define TPTR_SET_UNSIGNED_AND_TAG _TPTR_SET_UNSIGNED_AND_TAG
#define TPTR_SET_INTEGER          _TPTR_SET_INTEGER
#define TPTR_SET_INTEGER_AND_TAG  _TPTR_SET_INTEGER_AND_TAG
#define TPTR_SET_REAL             _TPTR_SET_REAL
#define TPTR_SET_REAL_AND_TAG     _TPTR_SET_REAL_AND_TAG
#define TPTR_SET_PTR56            _TPTR_SET_PTR56
#define TPTR_SET_PTR56_AND_TAG    _TPTR_SET_PTR56_AND_TAG
#define TPTR_SET_OBJ56            _TPTR_SET_OBJ56
#define TPTR_SET_OBJ56_AND_TAG    _TPTR_SET_OBJ56_AND_TAG
#define TPTR_SET_HANDLE           _TPTR_SET_HANDLE
#define TPTR_SET_HANDLE_AND_TAG   _TPTR_SET_HANDLE_AND_TAG

/* annotated pointer getters */
#define APTR_GET_ANNOTATION_POINTER _APTR_GET_ANNOTATION_POINTER
#define APTR_GET_POINTER            _APTR_GET_POINTER
#define APTR_GET_IDHIGH             _APTR_GET_IDHIGH
#define APTR_GET_BOOLEAN            _APTR_GET_BOOLEAN
#define APTR_GET_REAL               _APTR_GET_REAL
#define APTR_GET_PTR56              _APTR_GET_PTR56
#define APTR_GET_OBJ56              _APTR_GET_OBJ56

/* annotated pointer setters */
#define APTR_SET_POINTER          _APTR_SET_POINTER
#define APTR_SET_POINTER_AND_TAG  _APTR_SET_POINTER_AND_TAG
#define APTR_SET_IDHIGH           _APTR_SET_IDHIGH
#define APTR_SET_IDHIGH_AND_TAG   _APTR_SET_IDHIGH_AND_TAG
#define APTR_SET_BOOLEAN          _APTR_SET_BOOLEAN
#define APTR_SET_BOOLEAN_AND_TAG  _APTR_SET_BOOLEAN_AND_TAG
#define APTR_SET_UNSIGNED         _APTR_SET_UNSIGNED
#define APTR_SET_UNSIGNED_AND_TAG _APTR_SET_UNSIGNED_AND_TAG
#define APTR_SET_INTEGER          _APTR_SET_INTEGER
#define APTR_SET_INTEGER_AND_TAG  _APTR_SET_INTEGER_AND_TAG
#define APTR_SET_REAL             _APTR_SET_REAL
#define APTR_SET_REAL_AND_TAG     _APTR_SET_REAL_AND_TAG
#define APTR_SET_PTR56            _APTR_SET_PTR56
#define APTR_SET_PTR56_AND_TAG    _APTR_SET_PTR56_AND_TAG
#define APTR_SET_OBJ56            _APTR_SET_OBJ56
#define APTR_SET_OBJ56_AND_TAG    _APTR_SET_OBJ56_AND_TAG
#define APTR_SET_HANDLE           _APTR_SET_HANDLE
#define APTR_SET_HANDLE_AND_TAG   _APTR_SET_HANDLE_AND_TAG

/* tagged pointer matchers */
#define TPTR_MATCH_POINTER        _TPTR_MATCH_POINTER
#define TPTR_MATCH_IDHIGH         _TPTR_MATCH_IDHIGH
#define TPTR_MATCH_BOOLEAN        _TPTR_MATCH_BOOLEAN
#define TPTR_MATCH_UNSIGNED       _TPTR_MATCH_UNSIGNED
#define TPTR_MATCH_INTEGER        _TPTR_MATCH_INTEGER
#define TPTR_MATCH_REAL           _TPTR_MATCH_REAL
#define TPTR_MATCH_PTR56          _TPTR_MATCH_PTR56
#define TPTR_MATCH_OBJ56          _TPTR_MATCH_OBJ56
#define TPTR_MATCH_HANDLE         _TPTR_MATCH_HANDLE
#define TPTR_MATCH                _TPTR_MATCH

/* annotated pointer matchers */
#define APTR_MATCH_ANNOTATION     _APTR_MATCH_ANNOTATION
#define APTR_MATCH_POINTER        _APTR_MATCH_POINTER
#define APTR_MATCH_IDHIGH         _APTR_MATCH_IDHIGH
#define APTR_MATCH_BOOLEAN        _APTR_MATCH_BOOLEAN
#define APTR_MATCH_UNSIGNED       _APTR_MATCH_UNSIGNED
#define APTR_MATCH_INTEGER        _APTR_MATCH_INTEGER
#define APTR_MATCH_REAL           _APTR_MATCH_REAL
#define APTR_MATCH_PTR56          _APTR_MATCH_PTR56
#define APTR_MATCH_OBJ56          _APTR_MATCH_OBJ56
#define APTR_MATCH_HANDLE         _APTR_MATCH_HANDLE
#define APTR_MATCH                _APTR_MATCH

/* tagged pointer tests */
#define TPTR_IS_NULL              _TPTR_IS_NULL
#define TPTR_IS_VALID             _TPTR_IS_VALID
#define TPTR_IS_INVALID           _TPTR_IS_INVALID
#define TPTR_IS_DIRTY             _TPTR_IS_DIRTY
#define TPTR_IS_CLEAN             _TPTR_IS_CLEAN
#define TPTR_IS_NONPTR            _TPTR_IS_NONPTR
#define TPTR_IS_POINTER           _TPTR_IS_POINTER

/* annotated pointer tests */
#define APTR_IS_NULL              _APTR_IS_NULL
#define APTR_IS_VALID             _APTR_IS_VALID
#define APTR_IS_INVALID           _APTR_IS_INVALID
#define APTR_IS_DIRTY             _APTR_IS_DIRTY
#define APTR_IS_CLEAN             _APTR_IS_CLEAN
#define APTR_IS_NONPTR            _APTR_IS_NONPTR
#define APTR_IS_POINTER           _APTR_IS_POINTER

/* tagged pointer tag setters */
#define TPTR_MAKE_VALID           _TPTR_MAKE_VALID
#define TPTR_MAKE_INVALID         _TPTR_MAKE_INVALID
#define TPTR_MAKE_DIRTY           _TPTR_MAKE_DIRTY
#define TPTR_MAKE_CLEAN           _TPTR_MAKE_CLEAN
#define TPTR_MAKE_NONPTR          _TPTR_MAKE_NONPTR
#define TPTR_MAKE_POINTER         _TPTR_MAKE_POINTER

/* annotated pointer tag setters */
#define APTR_MAKE_VALID           _APTR_MAKE_VALID
#define APTR_MAKE_INVALID         _APTR_MAKE_INVALID
#define APTR_MAKE_DIRTY           _APTR_MAKE_DIRTY
#define APTR_MAKE_CLEAN           _APTR_MAKE_CLEAN
#define APTR_MAKE_NONPTR          _APTR_MAKE_NONPTR
#define APTR_MAKE_POINTER         _APTR_MAKE_POINTER

/* tagged pointer copy */
#define TPTR_COPY                 _TPTR_COPY

/* tagged pointer swap */
#define TPTR_SWAP                 _TPTR_SWAP

/* annotated pointer copy */
#define APTR_COPY                 _APTR_COPY

/* annotated pointer swap */
#define APTR_SWAP                 _APTR_SWAP



#endif



#endif

