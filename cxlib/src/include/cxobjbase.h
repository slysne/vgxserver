/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxobjbase.h
 * Author:  Stian Lysne <...>
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

#ifndef CXLIB_CXOBJBASE_H
#define CXLIB_CXOBJBASE_H


#include "cxplat.h"


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef uint64_t shortid_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_longstring_t {
  char *string;
  char prefix[8];
} longstring_t;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union u_objectid_t {
#if defined CXPLAT_ARCH_X64
  __m128i id128;
#elif defined CXPLAT_ARCH_ARM64
  QWORD id128[2];
#endif
  struct {
    shortid_t L;
    uint64_t H;
  };
  char shortstring[16];
  longstring_t longstring;
} objectid_t;
#define OBJECTID_NONE 0x0u
#define OBJECTID_LONGSTRING_MAX 1023







/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_histogram_t {
  uint64_t size;
  uint64_t freq;
} histogram_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef uint64_t cxlib_handle_t;



/*******************************************************************//**
 * 
 * Fingerprint 64 bits - we may generalize later to allow different lengths
 * 
 ***********************************************************************
 */
typedef uint64_t FP_t;



/*******************************************************************//**
 * 
 * Fingerprint 128 bits
 * 
 ***********************************************************************
 */
typedef __m128i FP128_t;



/*******************************************************************//**
 * 
 * General operation counters
 * 
 ***********************************************************************
 */
#define CXLIB_OPERATION_START  1000000000ull
#define CXLIB_OPERATION_NONE   0x7FFFFFFFFFFFFFFFull



/*******************************************************************//**
 * Feature Vector element definition
 *
 * A feature vector element is 32 bits.
 * Dimension (feature)  : 26 bits
 * Magnitude            : 6 bits
 *
 * Using 8 bits for magnitude gives a reasonable resolution (255 non-zero levels.)
 * (Magnitude=0 means dimension is unused.)
 *
 ***********************************************************************
 */
#define MAX_FEATURE_VECTOR_SIZE 48       // 48=artificial limit, less than hard limit 520=floor( (2**15-1) / 63.0 ) -- fplsh.c dependency, implementation specific.
#define MAX_FEATURE_VECTOR_TERM_LEN 27
#define FEATURE_VECTOR_MAG_BITS 6u
#define FEATURE_VECTOR_MAG_MAX ((1<<FEATURE_VECTOR_MAG_BITS)-1)
#define FEATURE_VECTOR_DIM_BITS 26u
#define FEATURE_VECTOR_DIM_MAXVAL ((1<<FEATURE_VECTOR_DIM_BITS)-1)
#define FEATURE_VECTOR_DIM_MASK (FEATURE_VECTOR_DIM_MAXVAL << FEATURE_VECTOR_MAG_BITS)



/*******************************************************************//**
 * Euclidean Vector element definition
 *
 * A Euclidean vector element is 8 bits quantized
 *
 ***********************************************************************
 */
#define MAX_EUCLIDEAN_VECTOR_SIZE 65472   // 1024 cache lines minus 1 cache line for allocator and header = 1023*64=65472




typedef enum e_vector_type_t {
  __VECTOR__MASK_FEATURE          = 0x01,
  __VECTOR__MASK_EUCLIDEAN        = 0x02,
  __VECTOR__MASK_INTERNAL         = 0x04,
  __VECTOR__MASK_EXTERNAL         = 0x08,
  __VECTOR__MASK_CENTROID         = 0x10,
  VECTOR_TYPE_NULL                = 0x00,
  VECTOR_TYPE_INTERNAL_FEATURE    = __VECTOR__MASK_FEATURE | __VECTOR__MASK_INTERNAL,
  VECTOR_TYPE_EXTERNAL_FEATURE    = __VECTOR__MASK_FEATURE | __VECTOR__MASK_EXTERNAL,
  VECTOR_TYPE_CENTROID_FEATURE    = __VECTOR__MASK_FEATURE | __VECTOR__MASK_INTERNAL | __VECTOR__MASK_CENTROID,
  VECTOR_TYPE_INTERNAL_EUCLIDEAN  = __VECTOR__MASK_EUCLIDEAN | __VECTOR__MASK_INTERNAL,
  VECTOR_TYPE_EXTERNAL_EUCLIDEAN  = __VECTOR__MASK_EUCLIDEAN | __VECTOR__MASK_EXTERNAL,
  VECTOR_TYPE_CENTROID_EUCLIDEAN  = __VECTOR__MASK_EUCLIDEAN | __VECTOR__MASK_INTERNAL | __VECTOR__MASK_CENTROID
} vector_type_t;


typedef enum e_feature_vector_dimension_t {
  FEATURE_VECTOR_DIMENSION_NONE       = 0,
  FEATURE_VECTOR_DIMENSION_LOCKED     = 1,
  FEATURE_VECTOR_DIMENSION_INVALID    = 0xB,
  FEATURE_VECTOR_DIMENSION_COLLISION  = 0xC,
  FEATURE_VECTOR_DIMENSION_NOENUM     = 0xE,
  FEATURE_VECTOR_DIMENSION_ERROR      = 0xF,
  FEATURE_VECTOR_DIMENSION_MIN        = 0x1000,
  FEATURE_VECTOR_DIMENSION_MAX        = (1<<FEATURE_VECTOR_DIM_BITS)-0x1000,
} feature_vector_dimension_t;


typedef enum e_vector_magnitude_enum {
  FEATURE_VECTOR_MAGNITUDE_ZERO     = 0,
  FEATURE_VECTOR_MAGNITUDE_MIN      = 1,
  FEATURE_VECTOR_MAGNITUDE_MAX      = FEATURE_VECTOR_MAG_MAX
} feature_vector_magnitude_enum;


typedef union u_vector_feature_t {
  uint32_t data;        // convenient for quick reset and null checking
  struct {
    uint32_t mag : FEATURE_VECTOR_MAG_BITS;  // 0-63        (LSB)
    uint32_t dim : FEATURE_VECTOR_DIM_BITS;  // 0-0xffffff  (MSB)
  };
} vector_feature_t;


typedef union u_ext_vector_feature_t {
  __m256i data;
  struct {
    float weight;
    char term[MAX_FEATURE_VECTOR_TERM_LEN+1]; // 27 chars max + NUL
  };
} ext_vector_feature_t;





/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
// TODO: this is a stub and I don't think this is the best way to manage object types... :-)
typedef enum e_object_basetype_t {
  CXLIB_OBTYPE_NOTYPE           = 0x00,

  CXLIB_OBTYPE_GENERIC          = 0x01,

  CXLIB_OBTYPE_ALLOCATOR        = 0x0A,

  CXLIB_OBTYPE_PROCESSOR        = 0x0C,

  CXLIB_OBTYPE_MAP              = 0x0D,

  CXLIB_OBTYPE_SEQUENCE         = 0x0E,

  CXLIB_OBTYPE_MANAGER          = 0x0F,

  CXLIB_OBTYPE_GRAPH            = 0x11,
  CXLIB_OBTYPE_GRAPH_VERTEX     = 0x12,
  CXLIB_OBTYPE_GRAPH_EDGE       = 0x13,

  CXLIB_OBTYPE_SIGNATURE        = 0x21,
  CXLIB_OBTYPE_VECTOR           = 0x22,
  CXLIB_OBTYPE_QUERY            = 0x23,
  CXLIB_OBTYPE_INDEX            = 0x25,

  CXLIB_OBTYPE_PY_WRAPPER       = 0x71,
} object_basetype_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union u_object_flags_t {
  uint8_t bits;
  struct {
    uint8_t is_initialized  : 1;
    uint8_t is_clone        : 1;
    uint8_t _3 : 1;
    uint8_t _4 : 1;
    uint8_t _5 : 1;
    uint8_t _6 : 1;
    uint8_t _7 : 1;
    uint8_t _8 : 1;
  };
} object_flags_t;








#endif
