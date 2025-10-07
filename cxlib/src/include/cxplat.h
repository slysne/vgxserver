/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxplat.h
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

#ifndef CXLIB_CXPLAT_H
#define CXLIB_CXPLAT_H



/* Ensure 64-bit file interfaces on UNIX */
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#define _GNU_SOURCE 1
#define _DARWIN_C_SOURCE 1
#define _USE_MATH_DEFINES



#define __macro__get_string(x) x
#define __macro__Xget_string(x) #x
#define __MACRO_TO_STRING(x) __macro__get_string( __macro__Xget_string( x ) )


/* PLATFORM DETECTION */

/* Require 64-bit platform */
#if !defined(_M_X64) && !defined(__x86_64__) && !defined(__aarch64__) && !defined(__arm64__)
#error "Unsupported architecture: 64-bit required."
#endif

#if defined(_M_X64) || defined(__x86_64__) || defined (__amd64__)
#define CXPLAT_ARCH_X64 1
#elif defined(__aarch64__) || defined (__arm64__)
#define CXPLAT_ARCH_ARM64 1
#else
#error "Unsupported architecture"
#endif


/* Require Windows, Linux, or MAC */
#if defined(_WIN32) && defined(_WIN64)
# define CXPLAT_WINDOWS_X64 1
#elif defined(__linux__) && defined(CXPLAT_ARCH_X64)
# define CXPLAT_LINUX_ANY 1
# define CXPLAT_LINUX_X64 1
#elif defined(__linux__) && defined(CXPLAT_ARCH_ARM64)
# define CXPLAT_LINUX_ANY 1
# define CXPLAT_LINUX_ARM64 1
#elif defined(__APPLE__) && defined(CXPLAT_ARCH_ARM64)
# define CXPLAT_MAC_ARM64 1
#else
# error "Unsupported platform."
#endif




#if defined CXPLAT_WINDOWS_X64
#define DISABLE_WARNING_NUMBER( w ) \
  __pragma(warning(disable:w))

#define IGNORE_WARNING_NUMBER( w ) \
  __pragma(warning(push)) \
  __pragma(warning(disable:w))
#define RESUME_WARNINGS \
  __pragma(warning(pop))
#define SUPPRESS_WARNING_NUMBER( w ) \
  __pragma(warning(suppress:w))
#define WARNING_IS_ERROR( w ) \
  __pragma(warning(error:w))
#define PUSH_WARNING_LEVEL( level ) \
  __pragma(warning(push,level))

#else
#define DISABLE_WARNING_NUMBER( w )
#define IGNORE_WARNING_NUMBER( w )
#define RESUME_WARNINGS
#define SUPPRESS_WARNING_NUMBER( w )
#define WARNING_IS_ERROR( w )
#define PUSH_WARNING_LEVEL( level )

#endif


#if defined CXPLAT_WINDOWS_X64
// ignore warnings
#define DISABLE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT          DISABLE_WARNING_NUMBER(   4127  )
#define IGNORE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT           IGNORE_WARNING_NUMBER(    4127  )
#define SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT         SUPPRESS_WARNING_NUMBER(  4127  )

#define DISABLE_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER             DISABLE_WARNING_NUMBER(   4820  )
#define IGNORE_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER              IGNORE_WARNING_NUMBER(    4820  )
#define SUPPRESS_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER            SUPPRESS_WARNING_NUMBER(  4820  )

#define DISABLE_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT              DISABLE_WARNING_NUMBER(   4324  )
#define IGNORE_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT               IGNORE_WARNING_NUMBER(    4324  )
#define SUPPRESS_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT             SUPPRESS_WARNING_NUMBER(  4324  )

#define DISABLE_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE        DISABLE_WARNING_NUMBER(   4214  )
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE         IGNORE_WARNING_NUMBER(    4214  )
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE       SUPPRESS_WARNING_NUMBER(  4214  )

#define DISABLE_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT       DISABLE_WARNING_NUMBER(   4201  )
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT        IGNORE_WARNING_NUMBER(    4201  )
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT      SUPPRESS_WARNING_NUMBER(  4201  )

#define DISABLE_WARNING_NONSTANDARD_EXTENSION_INITIALIZER           DISABLE_WARNING_NUMBER(   4204 4221)
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_INITIALIZER            IGNORE_WARNING_NUMBER(    4204 4221)
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_INITIALIZER          SUPPRESS_WARNING_NUMBER(  4204 4221)

#define DISABLE_WARNING_UNREFERENCED_FORMAL_PARAMETER               DISABLE_WARNING_NUMBER(   4100  )
#define IGNORE_WARNING_UNREFERENCED_FORMAL_PARAMETER                IGNORE_WARNING_NUMBER(    4100  )
#define SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER              SUPPRESS_WARNING_NUMBER(  4100  )

#define DISABLE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED               DISABLE_WARNING_NUMBER(   4189  )
#define IGNORE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED                IGNORE_WARNING_NUMBER(    4189  )
#define SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED              SUPPRESS_WARNING_NUMBER(  4189  )

#define DISABLE_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA            DISABLE_WARNING_NUMBER(   4242 4244  )
#define IGNORE_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA             IGNORE_WARNING_NUMBER(    4242 4244  )
#define SUPPRESS_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA           SUPPRESS_WARNING_NUMBER(  4242 4244  )

#define DISABLE_WARNING_SIGNED_UNSIGNED_MISMATCH                    DISABLE_WARNING_NUMBER(   4245  )
#define IGNORE_WARNING_SIGNED_UNSIGNED_MISMATCH                     IGNORE_WARNING_NUMBER(    4245  )
#define SUPPRESS_WARNING_SIGNED_UNSIGNED_MISMATCH                   SUPPRESS_WARNING_NUMBER(  4245  )

#define DISABLE_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                 DISABLE_WARNING_NUMBER(   4255  )
#define IGNORE_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                  IGNORE_WARNING_NUMBER(    4255  )
#define SUPPRESS_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                SUPPRESS_WARNING_NUMBER(  4255  )

#define DISABLE_WARNING_CAST_TRUNCATES_CONSTANT_VALUE               DISABLE_WARNING_NUMBER(   4310  )
#define IGNORE_WARNING_CAST_TRUNCATES_CONSTANT_VALUE                IGNORE_WARNING_NUMBER(    4310  )
#define SUPPRESS_WARNING_CAST_TRUNCATES_CONSTANT_VALUE              SUPPRESS_WARNING_NUMBER(  4310  )

#define DISABLE_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL            DISABLE_WARNING_NUMBER(   4456  )
#define IGNORE_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL             IGNORE_WARNING_NUMBER(    4456  )
#define SUPPRESS_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL           SUPPRESS_WARNING_NUMBER(  4456  )

#define DISABLE_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER        DISABLE_WARNING_NUMBER(   4457  )
#define IGNORE_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER         IGNORE_WARNING_NUMBER(    4457  )
#define SUPPRESS_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER       SUPPRESS_WARNING_NUMBER(  4457  )

#define DISABLE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT       DISABLE_WARNING_NUMBER(   4548  )
#define IGNORE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT        IGNORE_WARNING_NUMBER(    4548  )
#define SUPPRESS_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT      SUPPRESS_WARNING_NUMBER(  4548  )

#define DISABLE_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION    DISABLE_WARNING_NUMBER(   4706  )
#define IGNORE_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION     IGNORE_WARNING_NUMBER(    4706  )
#define SUPPRESS_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION   SUPPRESS_WARNING_NUMBER(  4706  )

#define DISABLE_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER     DISABLE_WARNING_NUMBER(   4054 4055)
#define IGNORE_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER      IGNORE_WARNING_NUMBER(    4054 4055)
#define SUPPRESS_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER    SUPPRESS_WARNING_NUMBER(  4054 4055)

#define DISABLE_WARNING_UNREACHABLE_CODE                            DISABLE_WARNING_NUMBER(   4702  )
#define IGNORE_WARNING_UNREACHABLE_CODE                             IGNORE_WARNING_NUMBER(    4702  )
#define SUPPRESS_WARNING_UNREACHABLE_CODE                           SUPPRESS_WARNING_NUMBER(  4702  )

#define DISABLE_WARNING_FUNCTION_NOT_INLINED                        DISABLE_WARNING_NUMBER(   4710  )
#define IGNORE_WARNING_FUNCTION_NOT_INLINED                         IGNORE_WARNING_NUMBER(    4710  )
#define SUPPRESS_WARNING_FUNCTION_NOT_INLINED                       SUPPRESS_WARNING_NUMBER(  4710  )

#define DISABLE_WARNING_AUTOMATIC_INLINE_EXPANSION                  DISABLE_WARNING_NUMBER(   4711  )
#define IGNORE_WARNING_AUTOMATIC_INLINE_EXPANSION                   IGNORE_WARNING_NUMBER(    4711  )
#define SUPPRESS_WARNING_AUTOMATIC_INLINE_EXPANSION                 SUPPRESS_WARNING_NUMBER(  4711  )

#define DISABLE_WARNING_FORMAT_STRING_NOT_LITERAL                   DISABLE_WARNING_NUMBER(   4774  )
#define IGNORE_WARNING_FORMAT_STRING_NOT_LITERAL                    IGNORE_WARNING_NUMBER(    4774  )
#define SUPPRESS_WARNING_FORMAT_STRING_NOT_LITERAL                  SUPPRESS_WARNING_NUMBER(  4774  )

#define DISABLE_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED             DISABLE_WARNING_NUMBER(   4091  )
#define IGNORE_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED              IGNORE_WARNING_NUMBER(    4091  )
#define SUPPRESS_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED            SUPPRESS_WARNING_NUMBER(  4091  )

#define DISABLE_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED       DISABLE_WARNING_NUMBER(   4061 4062 )
#define IGNORE_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED        IGNORE_WARNING_NUMBER(    4061 4062 )
#define SUPPRESS_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED      SUPPRESS_WARNING_NUMBER(  4061 4062 )

#define DISABLE_WARNING_COMPILER_SPECTRE_MITIGATION                 DISABLE_WARNING_NUMBER(   5045 )
#define IGNORE_WARNING_COMPILER_SPECTRE_MITIGATION                  IGNORE_WARNING_NUMBER(    5045 )
#define SUPPRESS_WARNING_COMPILER_SPECTRE_MITIGATION                SUPPRESS_WARNING_NUMBER(  5045 )

#define DISABLE_WARNING_DEREFERENCING_NULL_POINTER                  DISABLE_WARNING_NUMBER(   6011 28182 )
#define IGNORE_WARNING_DEREFERENCING_NULL_POINTER                   IGNORE_WARNING_NUMBER(    6011 28182 )
#define SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER                 SUPPRESS_WARNING_NUMBER(  6011 28182 )

#define DISABLE_WARNING_UNBALANCED_LOCK_RELEASE                     DISABLE_WARNING_NUMBER(   26115 26117 )
#define IGNORE_WARNING_UNBALANCED_LOCK_RELEASE                      IGNORE_WARNING_NUMBER(    26115 26117 )
#define SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE                    SUPPRESS_WARNING_NUMBER(  26115 26117 )

#define DISABLE_WARNING_UNSAFE_FUNCTION_POINTER_CAST                DISABLE_WARNING_NUMBER(   4191 )
#define IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST                 IGNORE_WARNING_NUMBER(    4191 )
#define SUPPRESS_WARNING_UNSAFE_FUNCTION_POINTER_CAST               SUPPRESS_WARNING_NUMBER(  4191 )

#define DISABLE_WARNING_USING_UNINITIALIZED_MEMORY                  DISABLE_WARNING_NUMBER(   6001 )
#define IGNORE_WARNING_USING_UNINITIALIZED_MEMORY                   IGNORE_WARNING_NUMBER(    6001 )
#define SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY                 SUPPRESS_WARNING_NUMBER(  6001 )

#define DISABLE_WARNING_STRING_NOT_ZERO_TERMINATED                  DISABLE_WARNING_NUMBER(   6053 )
#define IGNORE_WARNING_STRING_NOT_ZERO_TERMINATED                   IGNORE_WARNING_NUMBER(    6053 )
#define SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED                 SUPPRESS_WARNING_NUMBER(  6053 )

#define DISABLE_WARNING_LARGE_STACK_ALLOCATION                      DISABLE_WARNING_NUMBER(   6262 )
#define IGNORE_WARNING_LARGE_STACK_ALLOCATION                       IGNORE_WARNING_NUMBER(    6262 )
#define SUPPRESS_WARNING_LARGE_STACK_ALLOCATION                     SUPPRESS_WARNING_NUMBER(  6262 )




// escalate warnings
#define ESCALATE_WARNING_UNINITIALIZED_VARIABLE_USED                WARNING_IS_ERROR(         4701  )
#define ESCALATE_WARNING_NOT_ALL_CONTROL_PATHS_RETURN_A_VALUE       WARNING_IS_ERROR(         4715  )
//#define ESCALATE_WARNING_TOO_MANY_ACTUAL_PARAMETERS                 WARNING_IS_ERROR(         4020  )
#define ESCALATE_WARNING_DIFFERENT_INDIRECTION_LEVELS               WARNING_IS_ERROR(         4047  )
#define ESCALATE_WARNING_INCOMPATIBLE_TYPES                         WARNING_IS_ERROR(         4133  )


#else
// ignore warnings
#define DISABLE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT          /* TBD */
#define IGNORE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT           /* TBD */
#define SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT         /* TBD */
#define DISABLE_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER             /* TBD */
#define IGNORE_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER              /* TBD */
#define SUPPRESS_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER            /* TBD */
#define DISABLE_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT              /* TBD */
#define IGNORE_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT               /* TBD */
#define SUPPRESS_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT             /* TBD */
#define DISABLE_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE        /* TBD */
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE         /* TBD */
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE       /* TBD */
#define DISABLE_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT       /* TBD */
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT        /* TBD */
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT      /* TBD */
#define DISABLE_WARNING_NONSTANDARD_EXTENSION_INITIALIZER           /* TBD */
#define IGNORE_WARNING_NONSTANDARD_EXTENSION_INITIALIZER            /* TBD */
#define SUPPRESS_WARNING_NONSTANDARD_EXTENSION_INITIALIZER          /* TBD */
#define DISABLE_WARNING_UNREFERENCED_FORMAL_PARAMETER               /* TBD */
#define IGNORE_WARNING_UNREFERENCED_FORMAL_PARAMETER                /* TBD */
#define SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER              /* TBD */
#define DISABLE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED               /* TBD */
#define IGNORE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED                /* TBD */
#define SUPPRESS_WARNING_LOCAL_VARIABLE_NOT_REFERENCED              /* TBD */
#define DISABLE_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA            /* TBD */
#define IGNORE_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA             /* TBD */
#define SUPPRESS_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA           /* TBD */
#define DISABLE_WARNING_SIGNED_UNSIGNED_MISMATCH                    /* TBD */
#define IGNORE_WARNING_SIGNED_UNSIGNED_MISMATCH                     /* TBD */
#define SUPPRESS_WARNING_SIGNED_UNSIGNED_MISMATCH                   /* TBD */
#define DISABLE_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                 /* TBD */ 
#define IGNORE_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                  /* TBD */ 
#define SUPPRESS_WARNING_NO_FUNCTION_PROTOTYPE_GIVEN                /* TBD */ 
#define DISABLE_WARNING_CAST_TRUNCATES_CONSTANT_VALUE               /* TBD */
#define IGNORE_WARNING_CAST_TRUNCATES_CONSTANT_VALUE                /* TBD */
#define SUPPRESS_WARNING_CAST_TRUNCATES_CONSTANT_VALUE              /* TBD */
#define DISABLE_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL            /* TBD */
#define IGNORE_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL             /* TBD */
#define SUPPRESS_WARNING_DECLARATION_HIDES_PREVIOUS_LOCAL           /* TBD */
#define DISABLE_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER        /* TBD */
#define IGNORE_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER         /* TBD */
#define SUPPRESS_WARNING_DECLARATION_HIDES_FUNCTION_PARAMETER       /* TBD */
#define DISABLE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT       /* TBD */
#define IGNORE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT        /* TBD */
#define SUPPRESS_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT      /* TBD */
#define DISABLE_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION    /* TBD */
#define IGNORE_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION     /* TBD */
#define SUPPRESS_WARNING_ASSIGNMENT_WITHIN_CONDITIONAL_EXPRESSION   /* TBD */
#define DISABLE_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER     /* TBD */
#define IGNORE_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER      /* TBD */
#define SUPPRESS_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER    /* TBD */
#define DISABLE_WARNING_UNREACHABLE_CODE                            /* TBD */  
#define IGNORE_WARNING_UNREACHABLE_CODE                             /* TBD */  
#define SUPPRESS_WARNING_UNREACHABLE_CODE                           /* TBD */
#define DISABLE_WARNING_FUNCTION_NOT_INLINED                        /* TBD */
#define IGNORE_WARNING_FUNCTION_NOT_INLINED                         /* TBD */
#define SUPPRESS_WARNING_FUNCTION_NOT_INLINED                       /* TBD */
#define DISABLE_WARNING_AUTOMATIC_INLINE_EXPANSION                  /* TBD */
#define IGNORE_WARNING_AUTOMATIC_INLINE_EXPANSION                   /* TBD */
#define SUPPRESS_WARNING_AUTOMATIC_INLINE_EXPANSION                 /* TBD */
#define DISABLE_WARNING_FORMAT_STRING_NOT_LITERAL                   /* TBD */
#define IGNORE_WARNING_FORMAT_STRING_NOT_LITERAL                    /* TBD */
#define SUPPRESS_WARNING_FORMAT_STRING_NOT_LITERAL                  /* TBD */
#define DISABLE_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED             /* TBD */
#define IGNORE_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED              /* TBD */
#define SUPPRESS_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED            /* TBD */
#define DISABLE_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED       /* TBD */
#define IGNORE_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED        /* TBD */
#define SUPPRESS_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED      /* TBD */
#define DISABLE_WARNING_COMPILER_SPECTRE_MITIGATION                 /* TBD */
#define IGNORE_WARNING_COMPILER_SPECTRE_MITIGATION                  /* TBD */
#define SUPPRESS_WARNING_COMPILER_SPECTRE_MITIGATION                /* TBD */
#define DISABLE_WARNING_DEREFERENCING_NULL_POINTER                  /* TBD */ 
#define IGNORE_WARNING_DEREFERENCING_NULL_POINTER                   /* TBD */ 
#define SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER                 /* TBD */ 
#define DISABLE_WARNING_UNBALANCED_LOCK_RELEASE                     /* TBD */
#define IGNORE_WARNING_UNBALANCED_LOCK_RELEASE                      /* TBD */
#define SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE                    /* TBD */
#define DISABLE_WARNING_UNSAFE_FUNCTION_POINTER_CAST                /* TBD */
#define IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST                 /* TBD */
#define SUPPRESS_WARNING_UNSAFE_FUNCTION_POINTER_CAST               /* TBD */
#define DISABLE_WARNING_USING_UNINITIALIZED_MEMORY                  /* TBD */
#define IGNORE_WARNING_USING_UNINITIALIZED_MEMORY                   /* TBD */
#define SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY                 /* TBD */
#define DISABLE_WARNING_STRING_NOT_ZERO_TERMINATED                  /* TBD */
#define IGNORE_WARNING_STRING_NOT_ZERO_TERMINATED                   /* TBD */
#define SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED                 /* TBD */
#define DISABLE_WARNING_LARGE_STACK_ALLOCATION                      /* TBD */
#define IGNORE_WARNING_LARGE_STACK_ALLOCATION                       /* TBD */
#define SUPPRESS_WARNING_LARGE_STACK_ALLOCATION                     /* TBD */













// escalate warnings
#define ESCALATE_WARNING_UNINITIALIZED_VARIABLE_USED                /* TBD */
#define ESCALATE_WARNING_NOT_ALL_CONTROL_PATHS_RETURN_A_VALUE       /* TBD */
#define ESCALATE_WARNING_TOO_MANY_ACTUAL_PARAMETERS                 /* TBD */
#define ESCALATE_WARNING_DIFFERENT_INDIRECTION_LEVELS               /* TBD */
#define ESCALATE_WARNING_INCOMPATIBLE_TYPES                         /* TBD */
#endif



/*******************************************************************//**
 * DISABLE WARNINGS GLOBALLY
 ***********************************************************************
 */
DISABLE_WARNING_PADDING_ADDED_AFTER_DATA_MEMBER
DISABLE_WARNING_STRUCT_PADDED_DUE_TO_ALIGNMENT
DISABLE_WARNING_NONSTANDARD_EXTENSION_BIT_FIELD_TYPE
DISABLE_WARNING_NONSTANDARD_EXTENSION_NAMELESS_STRUCT
DISABLE_WARNING_NONSTANDARD_EXTENSION_INITIALIZER
DISABLE_WARNING_FUNCTION_NOT_INLINED
DISABLE_WARNING_AUTOMATIC_INLINE_EXPANSION
DISABLE_WARNING_TYPEDEF_IGNORED_NO_VAR_DECLARED
DISABLE_WARNING_ENUM_IN_SWITCH_NOT_EXPLICITLY_HANDLED
DISABLE_WARNING_COMPILER_SPECTRE_MITIGATION

//DISABLE_WARNING_SPECTRE_MITIGATION_FOR_MEMORY_LOAD


// Ignore platform header warnings - just noise and nothing we can do anyway
PUSH_WARNING_LEVEL( 0 )

#include <sys/types.h>
#include <sys/stat.h>

#if defined CXPLAT_LINUX_ANY
#include <sys/sysinfo.h>
#endif
#if defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#endif

#include <stdbool.h>
#include <stdio.h>

#if defined NDEBUG
#include <stdlib.h>
#elif defined CXPLAT_WINDOWS_X64
#define _DEBUG
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#else
#include <stdlib.h>
#endif

#include <stdint.h>
#include <float.h>
#include <string.h>
#include <math.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>

#define __INTEL_COMPILER_USE_INTRINSIC_PROTOTYPES 1

#if defined CXPLAT_WINDOWS_X64
#include <intrin.h>
/* Make the assumption: */
#define CXPLAT_ARCH_HASFMA 1
#endif


#if defined(CXPLAT_ARCH_X64) && defined(__FMA__) && !defined(CXPLAT_ARCH_HASFMA)
#define CXPLAT_ARCH_HASFMA 1
#endif


#if defined CXPLAT_ARCH_X64

# if defined __AVX512F__ && defined __AVX512DQ__ && defined __AVX512BW__
# define __cxlib_AVX512_MINIMUM__ 1
# include <immintrin.h>  //
# elif defined __AVX2__
# include <immintrin.h>  //
# elif defined __AVX__
# include <immintrin.h>  // 
# else
# error "This software requires AVX support"
# endif

// GCC has incomplete intrinsic headers
# ifndef _mm256_cvtsi256_si32
# define _mm256_cvtsi256_si32(a) (_mm_cvtsi128_si32(_mm256_castsi256_si128(a)))
# endif
# ifndef _mm512_cvtsi512_si32
# define _mm512_cvtsi512_si32(a) (_mm_cvtsi128_si32(_mm512_castsi512_si128(a)))
# endif

#elif defined CXPLAT_ARCH_ARM64
#include <arm_neon.h>
#include <arm_acle.h>
#include <sys/sysctl.h>
#include <mach/mach.h>
#include <mach/mach_host.h>
#endif



RESUME_WARNINGS

/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#if defined CXPLAT_ARCH_ARM64

typedef union __attribute__((aligned(16))) __m128 {
  float       m128_f32[4];
  uint64_t    m128_u64[2];
  int8_t      m128_i8[16];
  int16_t     m128_i16[8];
  int32_t     m128_i32[4];
  int64_t     m128_i64[2];
  uint8_t     m128_u8[16];
  uint16_t    m128_u16[8];
  uint32_t    m128_u32[4];
  float64x2_t vector_f64x2;
  float32x4_t vector_f32x4;
  float16x8_t vector_f16x8;
  uint64x2_t  vector_u64x2;
  uint32x4_t  vector_u32x4;
  uint16x8_t  vector_u16x8;
  uint8x16_t  vector_u8x16;
  int64x2_t   vector_i64x2;
  int32x4_t   vector_i32x4;
  int16x8_t   vector_i16x8;
  int8x16_t   vector_i8x16;
} __m128;


typedef union __attribute__((aligned(16))) __m128d {
  double m128d_f64[2];
  float64x2_t vector_f64x2;
  float32x4_t vector_f32x4;
  float16x8_t vector_f16x8;
} __m128d;


typedef union __attribute__((aligned(16))) __m128i {
  int8_t      m128i_i8[16];
  int16_t     m128i_i16[8];
  int32_t     m128i_i32[4];
  int64_t     m128i_i64[2];
  uint8_t     m128i_u8[16];
  uint16_t    m128i_u16[8];
  uint32_t    m128i_u32[4];
  uint64_t    m128i_u64[2];
  uint64x2_t  vector_u64x2;
  uint32x4_t  vector_u32x4;
  uint16x8_t  vector_u16x8;
  uint8x16_t  vector_u8x16;
  int64x2_t   vector_i64x2;
  int32x4_t   vector_i32x4;
  int16x8_t   vector_i16x8;
  int8x16_t   vector_i8x16;
} __m128i;


typedef union __attribute__((aligned(32))) __m256 {
  float       m256_f32[8];
  uint64_t    m256_u64[4];
  int8_t      m256_i8[32];
  int16_t     m256_i16[16];
  int32_t     m256_i32[8];
  int64_t     m256_i64[4];
  uint8_t     m256_u8[32];
  uint16_t    m256_u16[16];
  uint32_t    m256_u32[8];
  float64x2_t vector_f64x2[2];
  float32x4_t vector_f32x4[2];
  float16x8_t vector_f16x8[2];
  uint64x2_t  vector_u64x2[2];
  uint32x4_t  vector_u32x4[2];
  uint16x8_t  vector_u16x8[2];
  uint8x16_t  vector_u8x16[2];
  int64x2_t   vector_i64x2[2];
  int32x4_t   vector_i32x4[2];
  int16x8_t   vector_i16x8[2];
  int8x16_t   vector_i8x16[2];
} __m256;


typedef union __attribute__((aligned(32))) __m256d {
  double      m256d_f64[4];
  float64x2_t vector_f64x2[2];
  float32x4_t vector_f32x4[2];
  float16x8_t vector_f16x8[2];
} __m256d;


typedef union __attribute__((aligned(32))) __m256i {
  int8_t      m256i_i8[32];
  int16_t     m256i_i16[16];
  int32_t     m256i_i32[8];
  int64_t     m256i_i64[4];
  uint8_t     m256i_u8[32];
  uint16_t    m256i_u16[16];
  uint32_t    m256i_u32[8];
  uint64_t    m256i_u64[4];
  uint64x2_t  vector_u64x2[2];
  uint32x4_t  vector_u32x4[2];
  uint16x8_t  vector_u16x8[2];
  uint8x16_t  vector_u8x16[2];
  int64x2_t   vector_i64x2[2];
  int32x4_t   vector_i32x4[2];
  int16x8_t   vector_i16x8[2];
  int8x16_t   vector_i8x16[2];
} __m256i;


typedef union __attribute__((aligned(64))) __m512 {
  float       m512_f32[16];
  uint64_t    m512_u64[8];
  int8_t      m512_i8[64];
  int16_t     m512_i16[32];
  int32_t     m512_i32[16];
  int64_t     m512_i64[8];
  uint8_t     m512_u8[64];
  uint16_t    m512_u16[32];
  uint32_t    m512_u32[16];
  float64x2_t vector_f64x2[4];
  float32x4_t vector_f32x4[4];
  float16x8_t vector_f16x8[4];
  uint64x2_t  vector_u64x2[4];
  uint32x4_t  vector_u32x4[4];
  uint16x8_t  vector_u16x8[4];
  uint8x16_t  vector_u8x16[4];
  int64x2_t   vector_i64x2[4];
  int32x4_t   vector_i32x4[4];
  int16x8_t   vector_i16x8[4];
  int8x16_t   vector_i8x16[4];
} __m512;


typedef union __attribute__((aligned(64))) __m512d {
  double      m512d_f64[8];
  float64x2_t vector_f64x2[4];
  float32x4_t vector_f32x4[4];
  float16x8_t vector_f16x8[4];
} __m512d;


typedef union __attribute__((aligned(64))) __m512i {
  int8_t      m512i_i8[64];
  int16_t     m512i_i16[32];
  int32_t     m512i_i32[16];
  int64_t     m512i_i64[8];
  uint8_t     m512i_u8[64];
  uint16_t    m512i_u16[32];
  uint32_t    m512i_u32[16];
  uint64_t    m512i_u64[8];
  uint64x2_t  vector_u64x2[4];
  uint32x4_t  vector_u32x4[4];
  uint16x8_t  vector_u16x8[4];
  uint8x16_t  vector_u8x16[4];
  int64x2_t   vector_i64x2[4];
  int32x4_t   vector_i32x4[4];
  int16x8_t   vector_i16x8[4];
  int8x16_t   vector_i8x16[4];
} __m512i;


#endif




#if !defined(CXPLAT_WINDOWS_X64)
typedef uint8_t BYTE;
typedef uint16_t WORD;
typedef uint32_t DWORD;
#endif
typedef uint64_t QWORD;



#define floormultpow2( Val, MultPow2 )  ( (Val) & ~( (MultPow2) - 1 ))
#define ceilmultpow2( Val, MultPow2 )   (( (Val) + (MultPow2) - 1 ) & ~( (MultPow2) - 1 ))
#define elemcount( ElemType, Bytes )    ( ceilmultpow2( Bytes, sizeof( ElemType ) ) / sizeof( ElemType ) )
#define x_elemcount( ElemType, Bytes )  elemcount( ElemType, Bytes )
#define wcount( Bytes )                 elemcount( WORD, Bytes )
#define dwcount( Bytes )                elemcount( DWORD, Bytes )
#define qwcount( Bytes )                elemcount( QWORD, Bytes )
#define wsizeof(X)                      (sizeof(X)/sizeof(WORD))
#define dwsizeof(X)                     (sizeof(X)/sizeof(DWORD))
#define qwsizeof(X)                     (sizeof(X)/sizeof(QWORD))


// We need this because GCC defines these differently for 32-bit and 64-bit platforms
// In CXLIB a 'long' is always 32 bits
// In CXLIB a 'long long' is always 64 bits
#define CXLIB_LONG_MIN      (-2147483647L - 1)
#define CXLIB_LONG_MAX      2147483647L
#define CXLIB_ULONG_MAX     4294967295UL


#ifndef MAX_PATH
#define MAX_PATH 260
#endif



// Ignore platform header warnings - just noise and nothing we can do anyway
PUSH_WARNING_LEVEL( 0 )

/* Windows */
#if defined CXPLAT_WINDOWS_X64
#  define _WINSOCK_DEPRECATED_NO_WARNINGS 1
#  define WIN32_LEAN_AND_MEAN 1
#  define FD_SETSIZE 1024 // Align with unix fixed size
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  include <psapi.h>
#  include <WinNT.h>
#  ifndef snprintf
#    define snprintf _snprintf
#    define HAVE_SNPRINTF
#  endif
#  ifndef stat64
#    define stat64 _stat64
#  endif
#  ifndef fstat64
#    define fstat64 _fstat64
#  endif
#  ifndef strcasecmp
#    define strcasecmp _stricmp
#  endif
#  ifndef strncasecmp
#    define strncasecmp _strnicmp
#  endif
/* Linux */
#elif defined CXPLAT_LINUX_ANY
// TODO: gcc has __builtin_popcount, use it instead?
#  define __FUNCTION__ __func__
#  ifndef __USE_LARGEFILE64
#    error "Incorrect 64-bit file interface support setup. (__USE_LARGEFILE64 was not defined)"
#  endif
#elif defined CXPLAT_MAC_ARM64
#  define __FUNCTION__ __func__
#else
/* UNKNOWN PLATFORM */
#  error "Unsupported platform"
#endif
RESUME_WARNINGS




#if defined CXPLAT_WINDOWS_X64
/* WINDOWS */
#define DLL_EXPORT __declspec(dllexport)
#define DLL_IMPORT __declspec(dllimport)
#define DLL_HIDDEN
#elif defined(__GNUC__) || defined(__clang__)
/* UNIX-like */
#define DLL_EXPORT __attribute__ ((visibility("default")))
#define DLL_IMPORT __attribute__ ((visibility("default")))
#define DLL_HIDDEN __attribute__ ((visibility("hidden")))
#else
/* UNKNOWN PLATFORM */
#  error "Unsupported platform"
#endif

/*******************************************************************//**
 * C99-COMPLIANT VSNPRINTF
 ***********************************************************************
 */
#if defined(_MSC_VER) && (_MSC_VER < 1900)


#if defined vsnprintf
#undef vsnprintf
#endif

#define vsnprintf c99_vsnprintf

__inline int c99_vsnprintf( char *out_buf, size_t size, const char *format, va_list ap ) {
  int count = -1;

  if ( size != 0 ) {
    count = _vsnprintf_s( out_buf, size, _TRUNCATE, format, ap );
  }

  if ( count == -1 ) {
    count = _vscprintf( format, ap );
  }

  return count;
}

#endif


/*******************************************************************//**
 * SOME COMMON CONSTRUCTS
 ***********************************************************************
 */

// do {} while(0)
#define WHILE_ZERO \
  SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT \
  while(0)


// do {} while(1)
#define WHILE_ONE \
  SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT \
  while(1)


// if(1) {}
#define IF_ONE \
  SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT \
  if(1)


/*******************************************************************//**
 * GLOBAL MACROS
 ***********************************************************************
 */

// elemswap
#define elemswap( TYP, E1, E2 ) do { \
  TYP tmp = E1;   \
  E1 = E2;        \
  E2 = tmp;       \
} WHILE_ZERO


#define maximum_value(a,b) (((a) > (b)) ? (a) : (b))
#define minimum_value(a,b) (((a) < (b)) ? (a) : (b))



/*******************************************************************//**
 * THREADS
 ***********************************************************************
 */
#if defined CXPLAT_WINDOWS_X64
#define __THREAD __declspec( thread )
#else
#define __THREAD __thread
#endif


/*******************************************************************//**
 * ESCALATE WARNINGS GLOBALLY
 ***********************************************************************
 */
ESCALATE_WARNING_UNINITIALIZED_VARIABLE_USED
ESCALATE_WARNING_NOT_ALL_CONTROL_PATHS_RETURN_A_VALUE
ESCALATE_WARNING_DIFFERENT_INDIRECTION_LEVELS
ESCALATE_WARNING_INCOMPATIBLE_TYPES



/*******************************************************************//**
 * MACRO PUSH/POP
 ***********************************************************************
 */
#if defined CXPLAT_WINDOWS_X64
#define PUSH_MACRO( Name )  __pragma( push_macro( #Name ) )
#define POP_MACRO( Name )   __pragma( pop_macro( #Name ) )
#else
#define PUSH_MACRO( Name )  /* not supported */
#define POP_MACRO( Name )   /* not supported */
#endif


#if defined CXPLAT_ARCH_X64
int cxplat_cpuidex( unsigned int leaf, unsigned int subleaf, int *eax, int *ebx, int *ecx, int *edx );
int get_cpu_AVX_version( void );
#endif
char * get_new_cpu_instruction_set_extensions( int *avxcompat );

int get_system_physical_memory( int64_t *global_physical, int64_t *global_use_percent, int64_t *process_physical );
char * get_new_cpu_brand_string( void );
int has_cpu_feature_FMA( void );
int get_cpu_cores( int *cores, int *threads );
int get_cpu_L2_size( void );
int get_cpu_L2_associativity( void );
char * get_new_cpu_cache_info( void );
char * get_new_cpu_tlb_info( void );

char * get_error_reason( int __errnum, char *__buf, size_t __buflen );

#endif
