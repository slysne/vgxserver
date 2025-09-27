/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxtokenizer.h
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

#ifndef COMLIB_CXTOKENIZER_H
#define COMLIB_CXTOKENIZER_H

#include "objectmodel.h"
#include <ctype.h>


#define UNICODE_PLANE_MASK      0xFFFF0000
#define UNICODE_INTRAPLANE_MASK 0x0000FFFF

// Basic Multilingual Plane
#define UNICODE_PLANE_0_FIRST   0x00000000
#define UNICODE_PLANE_0_LAST    0x0000FFFF
#define UNICODE_BMP_OFFSET      UNICODE_PLANE_0_FIRST
#define UNICODE_BMP_END         UNICODE_PLANE_0_LAST
#define UNICODE_BMP_SIZE        (UNICODE_BMP_END - UNICODE_BMP_OFFSET + 1)
#define UNICODE_BMP_MASK        (UNICODE_BMP_SIZE - 1)
#define BMP_MAP_CODEPOINT( MapIndex )     ((MapIndex) + UNICODE_BMP_OFFSET)

// Supplementary Multilingual Plane
#define UNICODE_PLANE_1_FIRST   0x00010000
#define UNICODE_PLANE_1_LAST    0x0001FFFF
#define UNICODE_SMP_OFFSET      UNICODE_PLANE_1_FIRST
#define UNICODE_SMP_END         UNICODE_PLANE_1_LAST
#define UNICODE_SMP_SIZE        (UNICODE_SMP_END - UNICODE_SMP_OFFSET + 1)
#define UNICODE_SMP_MASK        (UNICODE_SMP_SIZE - 1)
#define SMP_MAP_CODEPOINT( MapIndex )     ((MapIndex) + UNICODE_SMP_OFFSET)

// Supplementary Ideographic Plane
#define UNICODE_PLANE_2_FIRST   0x00020000
#define UNICODE_PLANE_2_LAST    0x0002FFFF
#define UNICODE_SIP_OFFSET      UNICODE_PLANE_2_FIRST
#define UNICODE_SIP_END         UNICODE_PLANE_2_LAST
#define UNICODE_SIP_SIZE        (UNICODE_SIP_END - UNICODE_SIP_OFFSET + 1)
#define UNICODE_SIP_MASK        (UNICODE_SIP_SIZE - 1)
#define SIP_MAP_CODEPOINT( MapIndex )     ((MapIndex) + UNICODE_SIP_OFFSET)

// Supplementary Special-purpose Plane
#define UNICODE_PLANE_14_FIRST  0x000E0000
#define UNICODE_PLANE_14_LAST   0x000E0FFF
#define UNICODE_SSP_OFFSET      UNICODE_PLANE_14_FIRST
#define UNICODE_SSP_END         UNICODE_PLANE_14_LAST
#define UNICODE_SSP_SIZE        (UNICODE_SSP_END - UNICODE_SSP_OFFSET + 1)
#define UNICODE_SSP_MASK        (UNICODE_SSP_SIZE - 1)
#define SSP_MAP_CODEPOINT( MapIndex )     ((MapIndex) + UNICODE_SSP_OFFSET)

// Supplementary Private Use Area Planes
#define UNICODE_PLANE_15_FIRST  0x000F0000
#define UNICODE_PLANE_15_LAST   0x000FFFFF
#define UNICODE_PLANE_16_FIRST  0x00100000
#define UNICODE_PLANE_16_LAST   0x0010FFFF
#define UNICODE_SPUA_OFFSET     UNICODE_PLANE_15_FIRST
#define UNICODE_SPUA_END        UNICODE_PLANE_16_LAST
#define UNICODE_SPUA_SIZE       (UNICODE_SPUA_END - UNICODE_SPUA_OFFSET + 1)
#define UNICODE_SPUA_MASK       (UNICODE_SPUA_SIZE - 1)
#define SPUA_MAP_CODEPOINT( MapIndex )     ((MapIndex) + UNICODE_SPUA_OFFSET)



typedef DWORD unicode_codepoint_t;
typedef BYTE unicode_char_class_t;


typedef unicode_char_class_t unicode_BMP_class_t[ UNICODE_BMP_SIZE ];
typedef unicode_char_class_t unicode_SMP_class_t[ UNICODE_SMP_SIZE ];
typedef unicode_char_class_t unicode_SIP_class_t[ UNICODE_SIP_SIZE ];
typedef unicode_char_class_t unicode_SSP_class_t[ UNICODE_SSP_SIZE ];
typedef unicode_char_class_t unicode_SPUA_class_t[ UNICODE_SPUA_SIZE ];

typedef unicode_codepoint_t unicode_BMP_remap_t[ UNICODE_BMP_SIZE ];
typedef unicode_codepoint_t unicode_SMP_remap_t[ UNICODE_SMP_SIZE ];
typedef unicode_codepoint_t unicode_SIP_remap_t[ UNICODE_SIP_SIZE ];
typedef unicode_codepoint_t unicode_SSP_remap_t[ UNICODE_SSP_SIZE ];
typedef unicode_codepoint_t unicode_SPUA_remap_t[ UNICODE_SPUA_SIZE ];

typedef unicode_codepoint_t unicode_BMP_lower_t[ UNICODE_BMP_SIZE ];
typedef unicode_codepoint_t unicode_SMP_lower_t[ UNICODE_SMP_SIZE ];
typedef unicode_codepoint_t unicode_SIP_lower_t[ UNICODE_SIP_SIZE ];
typedef unicode_codepoint_t unicode_SSP_lower_t[ UNICODE_SSP_SIZE ];
typedef unicode_codepoint_t unicode_SPUA_lower_t[ UNICODE_SPUA_SIZE ];



struct s_CTokenizer_t;



typedef enum e_tokenizer_char_class {
  UNICODE_KEEP    = 0x0,  //  0000
  UNICODE_SKIP    = 0x1,  //  0001
  UNICODE_SPLIT   = 0x2,  //  0010
  UNICODE_IGNORE  = 0x4,  //  0100
  UNICODE_COMBINE = 0x8   //  1000
} tokenizer_char_class;



typedef enum e_tokenizer_tokmap_constant { 
  TOKENIZER_MAX_LEN = 1019,
  TOKENIZER_TOKMAP_INIT_SZMAP = 64,
  TOKENIZER_TOKMAP_INIT_SZDATA = 1024,
  TOKENIZER_TOKMAP_GROWTH_FACTOR = 2
} tokenizer_tokmap_constant;


typedef enum e_token_string_type {
  STRING_TYPE_NONE    = 0x00,
  STRING_TYPE_SPLIT   = 0x01,
  STRING_TYPE_INITL   = 0x02,
  STRING_TYPE_INITU   = 0x04,
  STRING_TYPE_LOWER   = 0x08,
  STRING_TYPE_CAPITAL = 0x10,
  STRING_TYPE_UPPER   = 0x20,
  STRING_TYPE_MIXED   = 0x40,
  STRING_TYPE_DIGIT   = 0x80,
  STRING_TYPE_DECIMAL = 0x82,
  STRING_TYPE_EXPON   = 0x84,
  STRING_TYPE_ENOTAT  = 0x86,

  STRING_MASK_NUMBER  = 0x86,
  STRING_MASK_WORD    = 0x38,   /*               upper, capital, lower               */
  STRING_MASK_EMPH    = 0x30,   /*               upper, capital                      */
  STRING_MASK_ALNUM   = 0xFE    /* digit, mixed, upper, capital, lower, initu, initl */
} token_string_type;


typedef enum e_token_flow_code { // TODO: think more about these
  TOKEN_FLOW_NONE  = 0x0, // 000
  TOKEN_FLOW_START = 0x1, // 001
  TOKEN_FLOW_CONT  = 0x2, // 010
  TOKEN_FLOW_SPLIT = 0x3, // 011
  TOKEN_FLOW_BREAK = 0x4, // 100
} token_flow_code;



IGNORE_WARNING_NUMBER(4201 4214) /* warning C4201: nonstandard extension used : nameless struct/union
                                     warning C4214: nonstandard extension used : bit field types other than int
                                  */
typedef union u_tokinfo_t {
  uint64_t bits;
  struct {
    uint16_t len;       // token length in bytes
    struct {
      uint16_t typ  : 8;  // string character type
      uint16_t cls  : 4;  // token class code
      uint16_t flw  : 3;  // token flow code
      uint16_t lin  : 1;  // true when first token in line, otherwise false
    };
    uint32_t soffset;   // surface offset for this token
  };
} tokinfo_t;
RESUME_WARNINGS


typedef struct s_tokenmap_t {
  int32_t ntok;     // Number of tokens in token map
  int32_t sztok;    // Current token info capacity of token map
  int32_t szdata;   // Current token capacity of token map
  int32_t __rsv;
  tokinfo_t *tok;   // Token info array
  tokinfo_t *tend;  // End of token info array
  tokinfo_t *tcur;  // Current token info
  BYTE *data;       // Output token array
  BYTE *dend;       // End of output token array
  BYTE *dcur;       // Current output token
  BYTE *dfin;       // Final token end
  BYTE *surface;    // Raw surface data (untokenized)
  uint32_t slen;    // Total bytes in surface data
} tokenmap_t;



typedef struct s_tokenizer_char_classes_t {
  unicode_codepoint_t *keep;
  unicode_codepoint_t *skip;
  unicode_codepoint_t *split;
  unicode_codepoint_t *ignore;
  unicode_codepoint_t *combine;
} tokenizer_char_classes_t;





/******************************************************************************
 * CTokenizer_t
 *
 ******************************************************************************
 */
typedef struct s_CTokenizer_vtable_t {
  // vtable head
  COMLIB_VTABLE_HEAD

  //
  void (*Load)( struct s_CTokenizer_t *self, const BYTE *text );
  BYTE * (*Next)( struct s_CTokenizer_t *self, tokinfo_t *tokinfo );
  tokenmap_t * (*Tokenize)( struct s_CTokenizer_t *self, const BYTE *text, CString_t **CSTR__error );
  int32_t (*Count)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap );
  int32_t (*Remain)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap );
  void (*DeleteTokenmap)( struct s_CTokenizer_t *self, tokenmap_t **tokenmap );
  const BYTE * (*PeekToken)( const struct s_CTokenizer_t *self, const tokenmap_t *tokmap );
  const BYTE * (*PeekTokenAndInfo)( const struct s_CTokenizer_t *self, const tokenmap_t *tokmap, tokinfo_t *tokinfo );
  const BYTE * (*GetToken)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap );
  const BYTE * (*GetTokenAndInfo)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap, tokinfo_t *tokinfo );
  int32_t (*Rewind)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap );
  int32_t (*Unget)( const struct s_CTokenizer_t *self, tokenmap_t *tokmap );
} CTokenizer_vtable_t;



/******************************************************************************
 * CTokenizer_t
 *
 ******************************************************************************
 */
typedef struct s_CTokenizer_t {
  // -----------------------------------------------
  // [Q1.1/2] vtable and typeinfo
  COMLIB_OBJECT_HEAD( CTokenizer_vtable_t )

  // [Q1.3/4] object id
  objectid_t obid;

  // [Q1.5] input text
  const BYTE *_input;

  // [Q1.6] input read pointer
  const BYTE *_irp;

  // [Q1.7] surface form write pointer
  BYTE *_swp;

  // [Q1.8] normal form write pointer
  BYTE *_nwp;

  // -----------------------------------------------

  // [Q2] tokenizer lock
  CS_LOCK _lock;

  // -----------------------------------------------

  // [Q3.1.1] 
  int32_t __rsv_3_1_1;

  // Q3.1.2.1] flags
  union {
    uint16_t bits;
    struct {
      uint16_t newline     : 1;
      uint16_t __rsv       : 15;
    };
  } _state;

  // [Q3.1.2.2] flags
  union {
    uint16_t bits;
    struct {
      uint16_t normalize   : 1;
      uint16_t lowercase   : 1;
      uint16_t keepdigits  : 1;
      uint16_t keepsplit   : 1;
      uint16_t strict_utf8 : 1;
      uint16_t __rsv       : 11;
    };
  } _flags;

  // [Q3.2] tokenmap
  tokenmap_t *_tokmap;

  // Basic multilingual plane
  // [Q3.3]
  unicode_BMP_class_t *_BMP_class;
  // [Q3.4]
  unicode_BMP_remap_t *_BMP_remap;
  // [Q3.5]
  unicode_BMP_lower_t *_BMP_lower;

  // Supplementary multilingual plane
  // [Q3.6]
  unicode_SMP_class_t *_SMP_class;
  // [Q3.7]
  unicode_SMP_remap_t *_SMP_remap;
  // [Q3.8]
  unicode_SMP_lower_t *_SMP_lower;

  // -----------------------------------------------

  // Supplementary ideographic plane
  // [Q4.1]
  unicode_SIP_class_t *_SIP_class; 
  // [Q4.2]
  unicode_SIP_remap_t *_SIP_remap; 
  // [Q4.3]
  unicode_SIP_lower_t *_SIP_lower; 
  
  // Supplementary special-purpose plane
  // [Q4.4]
  unicode_SSP_class_t *_SSP_class;
  // [Q4.5]
  unicode_SSP_remap_t *_SSP_remap;
  // [Q4.6]
  unicode_SSP_lower_t *_SSP_lower;
  
  // [Q4.7]
  QWORD __rsv_4_7;

  // [Q4.8]
  QWORD __rsv_4_8;

  // -----------------------------------------------

  // Supplementary private use area planes
  // [Q5.1]
  unicode_SPUA_class_t *_SPUA_class;
  // [Q5.2]
  unicode_SPUA_remap_t *_SPUA_remap;
  // [Q5.3]
  unicode_SPUA_lower_t *_SPUA_lower;

  // [Q5.4]
  QWORD __rsv_5_4;

  // [Q5.5]
  QWORD __rsv_5_5;

  // [Q5.6]
  QWORD __rsv_5_6;

  // [Q5.7]
  QWORD __rsv_5_7;

  // [Q5.8]
  QWORD __rsv_5_8;

  // -----------------------------------------------

  // [Q6-21] Surface form of current token
  BYTE _surface[TOKENIZER_MAX_LEN+4+1];

  // [Q22-37] Normal form of current token
  BYTE _normal[TOKENIZER_MAX_LEN+4+1];

} CTokenizer_t;




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_CTokenizer_constructor_args_t {
  int keep_digits;
  int normalize_accents;
  int lowercase;
  int keepsplit;
  int strict_utf8;

  tokenizer_char_classes_t overrides;


} CTokenizer_constructor_args_t;



void CTokenizer_RegisterClass( void );
void CTokenizer_UnregisterClass( void );







#endif
