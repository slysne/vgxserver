/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    cxtokenizer.c
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

#include "_comlib.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXTOKENIZER );

// Base
static longstring_t * CTokenizer__getid( CTokenizer_t *self );
static CTokenizer_t * CTokenizer__constructor( const void *identifier, CTokenizer_constructor_args_t *args );
static void CTokenizer__destructor( CTokenizer_t *self );

// API
static void CTokenizer__load( CTokenizer_t *self, const BYTE *text );
static BYTE * CTokenizer__next( CTokenizer_t *self, tokinfo_t *tokinfo );
static tokenmap_t * CTokenizer__tokenize( CTokenizer_t *self, const BYTE *utf8_text, CString_t **CSTR__error );
static int32_t CTokenizer__count( const CTokenizer_t *self, tokenmap_t *tokmap );
static int32_t CTokenizer__remain( const CTokenizer_t *self, tokenmap_t *tokmap );
static void CTokenizer__delete_tokenmap( CTokenizer_t *self, tokenmap_t **tokenmap );
static const BYTE * CTokenizer__peek_token( const CTokenizer_t *self, const tokenmap_t *tokmap );
static const BYTE * CTokenizer__peek_token_and_info( const CTokenizer_t *self, const tokenmap_t *tokmap, tokinfo_t *tokinfo );
static const BYTE * CTokenizer__get_token( const CTokenizer_t *self, tokenmap_t *tokmap );
static const BYTE * CTokenizer__get_token_and_info( const CTokenizer_t *self, tokenmap_t *tokmap, tokinfo_t *tokinfo );
static int32_t CTokenizer__rewind( const CTokenizer_t *self, tokenmap_t *tokmap );
static int32_t CTokenizer__unget( const CTokenizer_t *self, tokenmap_t *tokmap );


static CS_LOCK g_cs_general;

typedef enum e_utf8_states {
  UTF8_EXPECT_START = 1,
  UTF8_EXPECT_CONT1 = 2,
  UTF8_EXPECT_CONT2 = 3,
  UTF8_EXPECT_CONT3 = 4,
} utf8_states;


#define INVALID_CODEPOINT 0xFFFFFFFF

#define in_surrogate_range( C ) ((C & 0xFFF800) == 0xD800)


/* UTF-8 processing support lookup tables */
static int  g_utf8_startn[256] = {0}; // lookup for number of continuation bytes
static unicode_codepoint_t g_utf8_startc[256] = {0}; // lookup for lowest codepoint encoded by this start byte
static unicode_codepoint_t g_utf8_contc[256] = {INVALID_CODEPOINT};


// Basic Multilingual Plane
static void CTokenizer__set_default_BMP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides );
static void CTokenizer__set_default_BMP_remap( CTokenizer_t *self );
static void CTokenizer__set_default_BMP_lower( CTokenizer_t *self );
static void CTokenizer__set_ignore_BMP_digits( CTokenizer_t *self );


// Supplementary Multilingual Plane
static void CTokenizer__set_default_SMP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides );
static void CTokenizer__set_default_SMP_remap( CTokenizer_t *self );
static void CTokenizer__set_default_SMP_lower( CTokenizer_t *self );
static void CTokenizer__set_ignore_SMP_digits( CTokenizer_t *self );


// Supplementary Ideographic Plane
static void CTokenizer__set_default_SIP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides );
static void CTokenizer__set_default_SIP_remap( CTokenizer_t *self );
static void CTokenizer__set_default_SIP_lower( CTokenizer_t *self );
static void CTokenizer__set_ignore_SIP_digits( CTokenizer_t *self );


// Supplementary Special-purpose Plane
static void CTokenizer__set_default_SSP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides );
static void CTokenizer__set_default_SSP_remap( CTokenizer_t *self );
static void CTokenizer__set_default_SSP_lower( CTokenizer_t *self );
static void CTokenizer__set_ignore_SSP_digits( CTokenizer_t *self );


// Supplementary Private Use Are Planes
static void CTokenizer__set_default_SPUA_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides );
static void CTokenizer__set_default_SPUA_remap( CTokenizer_t *self );
static void CTokenizer__set_default_SPUA_lower( CTokenizer_t *self );
static void CTokenizer__set_ignore_SPUA_digits( CTokenizer_t *self );


typedef unicode_codepoint_t (*__f_getc_utf8)( const BYTE **input, BYTE bytes[], int *nbytes );

static unicode_codepoint_t __getc_utf8( const BYTE **data, BYTE bytes[], int *nbytes );
static unicode_codepoint_t __getc_utf8_strict( const BYTE **data, BYTE bytes[], int *nbytes );
static int __putc_utf8( unicode_codepoint_t codepoint, BYTE bytes[] );
static BYTE * __next_token_nolock( CTokenizer_t *self, tokinfo_t *tokinfo );
static tokenmap_t * __extend_tokenmap_nolock( tokenmap_t *tokenmap );
static void __delete_tokenmap( tokenmap_t **tokmap );
static token_string_type __string_type( const BYTE *string );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CTokenizer_vtable_t CTokenizerMethods = {
  /* common cxlib_object_vtable interface */
  .vm_cmpid           = NULL,
  .vm_getid           = (f_object_identifier_t)CTokenizer__getid,
  .vm_serialize       = NULL,
  .vm_deserialize     = NULL,
  .vm_construct       = (f_object_constructor_t)CTokenizer__constructor,
  .vm_destroy         = (f_object_destructor_t)CTokenizer__destructor,
  .vm_represent       = NULL,
  .vm_allocator       = NULL,
  /* CTokenizer methods */
  .Load               = CTokenizer__load,
  .Next               = CTokenizer__next,
  .Tokenize           = CTokenizer__tokenize,
  .Count              = CTokenizer__count,
  .Remain             = CTokenizer__remain,
  .DeleteTokenmap     = CTokenizer__delete_tokenmap,
  .PeekToken          = CTokenizer__peek_token,
  .PeekTokenAndInfo   = CTokenizer__peek_token_and_info,
  .GetToken           = CTokenizer__get_token,
  .GetTokenAndInfo    = CTokenizer__get_token_and_info,
  .Rewind             = CTokenizer__rewind,
  .Unget              = CTokenizer__unget
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void CTokenizer_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( CTokenizer_t, CXLIB_OBTYPE_PROCESSOR, &CTokenizerMethods, OBJECT_IDENTIFIED_BY_LONGSTRING, OBJECTID_LONGSTRING_MAX );

  INIT_CRITICAL_SECTION( &g_cs_general.lock );

  /* UTF-8 NUMBER OF CONTINUATION BYTES BY START BYTE (lookup table) */ 
  for( int c=0xC2; c<=0xDF; c++ ) g_utf8_startn[c] = 1; // start 2-byte sequence, so 1 cont byte expected
  for( int c=0xE0; c<=0xEF; c++ ) g_utf8_startn[c] = 2; // start 3-byte sequence, so 2 cont byte expected
  for( int c=0xF0; c<=0xF4; c++ ) g_utf8_startn[c] = 3; // start 4-byte sequence, so 3 cont byte expected
  // all others illegal

  /* UTF-8 LOWEST CODEPOINT BY START BYTE (lookup table) */
  for( int c=0xC2; c<=0xDF; c++ ) g_utf8_startc[c] = (c-0xC0)*0x40;     // 2-byte, C0/C1 illegal, range 0x80-0x7FF
  for( int c=0xE0; c<=0xEF; c++ ) g_utf8_startc[c] = (c-0xE0)*0x1000;   // 3-byte, < E0A0xx illegal, range 0x800 - 0xFFFF
  for( int c=0xF0; c<=0xF4; c++ ) g_utf8_startc[c] = (c-0xF0)*0x40000;  // 4-byte, < F090xx illegal, >= F490xxxx illegal,  range 0x10000 - 0x10FFFF
  // all others illegal

  /* UTF-8 CODEPOINT PAYLOAD BITS BY CONTINUATION BYTE (lookup table) */
  for( int c=0   ; c<0x80 ; c++ ) g_utf8_contc[c] = INVALID_CODEPOINT;  // not a cont byte
  for( int c=0x80; c<=0xBF; c++ ) g_utf8_contc[c] = c&0x3F;             // lookup cont byte payload value
  for( int c=0xC0; c<256  ; c++ ) g_utf8_contc[c] = INVALID_CODEPOINT;  // not a cont byte
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void CTokenizer_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( CTokenizer_t );
  DEL_CRITICAL_SECTION( &g_cs_general.lock );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static longstring_t * CTokenizer__getid( CTokenizer_t *self ) {
  return &self->obid.longstring;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CTokenizer_t * CTokenizer__constructor( const void *identifier, CTokenizer_constructor_args_t *args ) {

  CTokenizer_t *self = NULL;

  SYNCHRONIZE_ON( g_cs_general ) {

    XTRY {

      // [1] [2] [3] Allocate tokenizer object and set vtable, typeinfo and identifier
      PALIGNED_MALLOC_THROWS( self, CTokenizer_t, 0x101 );
      COMLIB_OBJECT_INIT( CTokenizer_t, self, identifier );

      // [4]
      self->_input = NULL;

      // [5]
      self->_irp = NULL;

      // [6]
      self->_swp = self->_surface;

      // [7]
      self->_nwp = self->_normal;

      // [8]
      INIT_CRITICAL_SECTION( &self->_lock.lock );

      // [9]
      self->__rsv_3_1_1 = 0;

      // [10]
      self->_state.bits = 0;
      self->_state.newline = 1;

      // [11]
      self->_flags.bits = 0;
      self->_flags.normalize = args->normalize_accents ? 1 : 0;
      self->_flags.lowercase = args->lowercase ? 1 : 0;
      self->_flags.keepdigits = args->keep_digits ? 1 : 0;
      self->_flags.keepsplit = args->keepsplit ? 1 : 0;
      self->_flags.strict_utf8 = args->strict_utf8 ? 1 : 0;

      // [13]
      self->_tokmap = NULL;

      // [14] [15] [16] Basic Multilingual Plane
      self->_BMP_class = NULL;
      self->_BMP_remap = NULL;
      self->_BMP_lower = NULL;

      // [17] [18] [19] Supplementary Multilingual Plane
      self->_SMP_class = NULL;
      self->_SMP_remap = NULL;
      self->_SMP_lower = NULL;

      // [20] [21] [22] Supplementary Ideographic Plane
      self->_SIP_class = NULL;
      self->_SIP_remap = NULL;
      self->_SIP_lower = NULL;

      // [23] [24] [25] Supplementary Special-purpose Plane
      self->_SSP_class = NULL;
      self->_SSP_remap = NULL;
      self->_SSP_lower = NULL;

      // [26] [27] [28] Supplementary Private Use Area Planes
      self->_SPUA_class = NULL;
      self->_SPUA_remap = NULL;
      self->_SPUA_lower = NULL;

      self->__rsv_4_7 = 0;
      self->__rsv_4_8 = 0;
      self->__rsv_5_4 = 0;
      self->__rsv_5_5 = 0;
      self->__rsv_5_6 = 0;
      self->__rsv_5_7 = 0;
      self->__rsv_5_8 = 0;

      // -----------------------------------------------
      // Initialize the default Basic Multilingual Plane
      PALIGNED_MALLOC_THROWS( self->_BMP_class, unicode_BMP_class_t, 0x102 );
      CTokenizer__set_default_BMP_class( self, &args->overrides );
      // Normalize?
      if( self->_flags.normalize ) {
        PALIGNED_MALLOC_THROWS( self->_BMP_remap, unicode_BMP_remap_t, 0x103 );
        CTokenizer__set_default_BMP_remap( self );
      }
      // Lowercase ?
      if( self->_flags.lowercase ) {
        PALIGNED_MALLOC_THROWS( self->_BMP_lower, unicode_BMP_lower_t, 0x104 );
        CTokenizer__set_default_BMP_lower( self );
      }
      // Keep digits?
      if( !self->_flags.keepdigits ) {
        CTokenizer__set_ignore_BMP_digits( self );
      }
      //
      // -----------------------------------------------


      // -------------------------------------------------------
      // Initialize the default Supplementary Multilingual Plane
      PALIGNED_MALLOC_THROWS( self->_SMP_class, unicode_SMP_class_t, 0x105 );
      CTokenizer__set_default_SMP_class( self, &args->overrides );
      // Normalize?
      if( self->_flags.normalize ) {
        PALIGNED_MALLOC_THROWS( self->_SMP_remap, unicode_SMP_remap_t, 0x106 );
        CTokenizer__set_default_SMP_remap( self );
      }
      // Lowercase ?
      if( self->_flags.lowercase ) {
        PALIGNED_MALLOC_THROWS( self->_SMP_lower, unicode_SMP_lower_t, 0x107 );
        CTokenizer__set_default_SMP_lower( self );
      }
      // Keep digits?
      if( !self->_flags.keepdigits ) {
        CTokenizer__set_ignore_SMP_digits( self );
      }
      //
      // -------------------------------------------------------


      // ------------------------------------------------------
      // Initialize the default Supplementary Ideographic Plane
      PALIGNED_MALLOC_THROWS( self->_SIP_class, unicode_SIP_class_t, 0x108 );
      CTokenizer__set_default_SIP_class( self, &args->overrides );
      // Normalize?
      if( self->_flags.normalize ) {
        PALIGNED_MALLOC_THROWS( self->_SIP_remap, unicode_SIP_remap_t, 0x109 );
        CTokenizer__set_default_SIP_remap( self );
      }
      // Lowercase ?
      if( self->_flags.lowercase ) {
        PALIGNED_MALLOC_THROWS( self->_SIP_lower, unicode_SIP_lower_t, 0x10A );
        CTokenizer__set_default_SIP_lower( self );
      }
      // Keep digits?
      if( !self->_flags.keepdigits ) {
        CTokenizer__set_ignore_SIP_digits( self );
      }
      //
      // ------------------------------------------------------


      // ----------------------------------------------------------
      // Initialize the default Supplementary Special-purpose Plane
      PALIGNED_MALLOC_THROWS( self->_SSP_class, unicode_SSP_class_t, 0x10B );
      CTokenizer__set_default_SSP_class( self, &args->overrides );
      // Normalize?
      if( self->_flags.normalize ) {
        PALIGNED_MALLOC_THROWS( self->_SSP_remap, unicode_SSP_remap_t, 0x10C );
        CTokenizer__set_default_SSP_remap( self );
      }
      // Lowercase ?
      if( self->_flags.lowercase ) {
        PALIGNED_MALLOC_THROWS( self->_SSP_lower, unicode_SSP_lower_t, 0x10D );
        CTokenizer__set_default_SSP_lower( self );
      }
      // Keep digits?
      if( !self->_flags.keepdigits ) {
        CTokenizer__set_ignore_SSP_digits( self );
      }
      //
      // ----------------------------------------------------------


      // ----------------------------------------------------------
      // Initialize the default Supplementary Private Use Are Planes
      PALIGNED_MALLOC_THROWS( self->_SPUA_class, unicode_SPUA_class_t, 0x10E );
      CTokenizer__set_default_SPUA_class( self, &args->overrides );
      // Normalize?
      if( self->_flags.normalize ) {
        PALIGNED_MALLOC_THROWS( self->_SPUA_remap, unicode_SPUA_remap_t, 0x10F );
        CTokenizer__set_default_SPUA_remap( self );
      }
      // Lowercase ?
      if( self->_flags.lowercase ) {
        PALIGNED_MALLOC_THROWS( self->_SPUA_lower, unicode_SPUA_lower_t, 0x110 );
        CTokenizer__set_default_SPUA_lower( self );
      }
      // Keep digits?
      if( !self->_flags.keepdigits ) {
        CTokenizer__set_ignore_SPUA_digits( self );
      }
      //
      // ----------------------------------------------------------

      // [30]
      memset( self->_surface, 0, sizeof( self->_surface ) );

      // [31]
      memset( self->_normal, 0, sizeof( self->_normal ) );

    }
    XCATCH( errcode ) {
      if( self ) {
        CTokenizer__destructor( self );
        self = NULL;
      }
    }
    XFINALLY {
    }
  } RELEASE;

  return self;
  
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__destructor( CTokenizer_t *self ) {

  // [Q5.3]
  if( self->_SPUA_lower ) {
    ALIGNED_FREE( self->_SPUA_lower );
  }
  // [Q5.2]
  if( self->_SPUA_remap ) {
    ALIGNED_FREE( self->_SPUA_remap );
  }
  // [Q5.1]
  if( self->_SPUA_class ) {
    ALIGNED_FREE( self->_SPUA_class );
  }

  // [Q4.6]
  if( self->_SSP_lower ) {
    ALIGNED_FREE( self->_SSP_lower );
  }
  // [Q4.5]
  if( self->_SSP_remap ) {
    ALIGNED_FREE( self->_SSP_remap );
  }
  // [Q4.4]
  if( self->_SSP_class ) {
    ALIGNED_FREE( self->_SSP_class );
  }

  // [Q4.3]
  if( self->_SIP_lower ) {
    ALIGNED_FREE( self->_SIP_lower );
  }
  // [Q4.2]
  if( self->_SIP_remap ) {
    ALIGNED_FREE( self->_SIP_remap );
  }
  // [Q4.1]
  if( self->_SIP_class ) {
    ALIGNED_FREE( self->_SIP_class );
  }

  // [Q3.8]
  if( self->_SMP_lower ) {
    ALIGNED_FREE( self->_SMP_lower );
  }
  // [Q3.7]
  if( self->_SMP_remap ) {
    ALIGNED_FREE( self->_SMP_remap );
  }
  // [Q3.6]
  if( self->_SMP_class ) {
    ALIGNED_FREE( self->_SMP_class );
  }

  // [Q3.5]
  if( self->_BMP_lower ) {
    ALIGNED_FREE( self->_BMP_lower );
  }
  // [Q3.4]
  if( self->_BMP_remap ) {
    ALIGNED_FREE( self->_BMP_remap );
  }
  // [Q3.3]
  if( self->_BMP_class ) {
    ALIGNED_FREE( self->_BMP_class );
  }

  // [Q3.2]
  __delete_tokenmap( &self->_tokmap );

  // [Q3.1.2.2]
  self->_flags.bits = 0;

  // [Q3.1.2.1]
  self->_state.bits = 0;

  // [Q2]
  DEL_CRITICAL_SECTION( &self->_lock.lock );

  // [Q1.8]
  self->_nwp = NULL;

  // [Q1.7]
  self->_swp = NULL;

  // [Q1.6]
  self->_irp = NULL;

  // [Q1.5]
  self->_input = NULL;

  // [Q1.3-4]
  objectid_destroy_longstring( &self->obid );

  // [Q1.2]
  self->typeinfo.qword = 0;

  // [Q1.1]
  self->vtable = NULL;

  ALIGNED_FREE( self );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static token_string_type __string_type( const BYTE *str ) {
  register BYTE c;
  register unsigned const char *p = str;
  register token_string_type type = STRING_TYPE_NONE;

  while( (c = *p++) != '\0' ) {

    if( islower(c) ) {
      switch( type ) {
      case STRING_TYPE_NONE:
        type = STRING_TYPE_INITL;   // initialize to INITL if first char
        break;
      case STRING_TYPE_INITL:
        type = STRING_TYPE_LOWER;   // continue as LOWER if second char
        break;
      case STRING_TYPE_INITU:
        type = STRING_TYPE_CAPITAL; // suggest PROPER if first char was UPPER
        break;
      case STRING_TYPE_LOWER:       // continue as LOWER
        /* FALLTHRU */
      case STRING_TYPE_CAPITAL:     // continue as PROPER
        break;
      case STRING_TYPE_DIGIT:
        /* FALLTHRU */
      case STRING_TYPE_DECIMAL:
        if( c == 'e' || c == 'E' ) {
          type = STRING_TYPE_EXPON;
        }
        else {
          type = STRING_TYPE_MIXED;
          goto conclusive;
        }
        break;
      default:
        type = STRING_TYPE_MIXED;   // evidence of MIXED.
        goto conclusive;            // STOP.
      }
    }
    else if( isupper(c) ) {
      switch( type ) {
      case STRING_TYPE_NONE:
        type = STRING_TYPE_INITU;   // initialize to INITU if first char
        break;
      case STRING_TYPE_INITU:
        type = STRING_TYPE_UPPER;   // continue as UPPER if second char
        break;
      case STRING_TYPE_UPPER:       // continue as UPPER
        break;
      default:
        type = STRING_TYPE_MIXED;   // evidence of MIXED.
        goto conclusive;            // STOP.
      }
    }
    else if( isdigit(c) ) {
      switch( type ) {
      case STRING_TYPE_NONE:
        type = STRING_TYPE_DIGIT;   // initialize to DIGIT if first char
        break;
      case STRING_TYPE_DIGIT:       // continue as DIGIT
        break;
      case STRING_TYPE_DECIMAL:     // continue as DECIMAL
        break;
      case STRING_TYPE_EXPON:       // become e-notation
        type = STRING_TYPE_ENOTAT;
        break;
      case STRING_TYPE_ENOTAT:      // continue as e-notation
        break;
      default:
        type = STRING_TYPE_MIXED;   // evidence of MIXED.
        goto conclusive;            // STOP.
      }
    }
    else if( type & STRING_MASK_NUMBER ) {
      if( c == '.' ) {
        if( type == STRING_TYPE_DIGIT ) {
          type = STRING_TYPE_DECIMAL;
        }
        else {
          type = STRING_TYPE_MIXED;
          goto conclusive;
        }
      }
      else if( c == '+' || c == '-' ) {
        if( type == STRING_TYPE_EXPON ) {
          type = STRING_TYPE_ENOTAT;
        }
        else {
          type = STRING_TYPE_MIXED;
          goto conclusive;
        }
      }
      else {
        type = STRING_TYPE_MIXED;
        goto conclusive;
      }
    }
    else if( type == STRING_TYPE_NONE ) {
      if( c == '+' || c == '-' ) {
        type = STRING_TYPE_DIGIT;
      }
    }
    else {
      type = STRING_TYPE_MIXED;
      goto conclusive;
    }
  }
conclusive:
  switch( type ) {
  case STRING_TYPE_INITL:
    type = STRING_TYPE_LOWER;
    break;
  case STRING_TYPE_INITU:
    type = STRING_TYPE_CAPITAL;
    break;
  default:
    break;
  }

  return type;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __apply_char_class_overrides( unicode_char_class_t *map, const unicode_codepoint_t uc_low, const unicode_codepoint_t uc_high, const tokenizer_char_classes_t *overrides ) {
  if( overrides ) {
    typedef struct __s_overrides_t {
      unicode_codepoint_t *codepoints;
      tokenizer_char_class char_class;
    } __overrides_t;

    __overrides_t process[] = {
      { overrides->keep,     UNICODE_KEEP },
      { overrides->skip,     UNICODE_SKIP },
      { overrides->split,    UNICODE_SPLIT },
      { overrides->ignore,   UNICODE_IGNORE },
      { overrides->combine,  UNICODE_COMBINE },
      { NULL, 0 }
    };

    __overrides_t *proc = process;
    while( proc->codepoints ) {
      unicode_codepoint_t *cursor = proc->codepoints;
      unicode_codepoint_t *puc;
      while( *(puc = cursor++) != 0 ) {
        unicode_codepoint_t uc = *puc;
        if( uc >= uc_low && uc <= uc_high ) {
          map[ uc - uc_low ] = proc->char_class;
        }
      }
      proc++;
    }
  }
}



#define CLASS_SINGLE( MapIndex, CharClass ) \
  (PLANE_class)[ MapIndex ] = CharClass

#define CLASS_RANGE( MapIndexLow, MapIndexHigh, CharClass ) \
  for( int i=(MapIndexLow); i<=(MapIndexHigh); i++ ) (PLANE_class)[ i ] = CharClass


#define REMAP_SINGLE( MapIndex, RemapCodepoint ) \
  (PLANE_remap)[ MapIndex ] = (RemapCodepoint)

#define REMAP_RANGE( MapIndexLow, MapIndexHigh, RemapCodepoint ) \
  for( int i=(MapIndexLow); i<=(MapIndexHigh); i++ ) (PLANE_remap)[ i ] = (RemapCodepoint)

#define REMAP_RANGE_ADJACENT( MapIndexLow, MapIndexHigh, RemapCodepointEven, RemapCodepointOdd ) \
  for( int i=(MapIndexLow); i<=(MapIndexHigh); i++ ) (PLANE_remap)[ i ] = i % 2 ? (RemapCodepointEven) : (RemapCodepointOdd)


#define LOWER_SINGLE( MapIndex, LowerCodepoint ) \
  (PLANE_lower)[ MapIndex ] = (LowerCodepoint)

#define LOWER_RANGE( MapIndecLow, MapIndecHigh, LowerCodepoint ) \
  for( i=(MapIndecLow); i<=(MapIndecHigh); i++ ) (PLANE_lower)[ i ] = (LowerCodepoint)






/*******************************************************************//**
 *
 * Basic Multilingual Plane 0000 - FFFF
 ***********************************************************************
 */
static void CTokenizer__set_default_BMP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides ) {
#define PLANE_class (*self->_BMP_class)

  // DEFAULT tokenization of the basic multilingual plane


  // Initialize
  CLASS_SINGLE( 0x0000,         UNICODE_SKIP );
  CLASS_RANGE(  0x0001, 0xFFFF, UNICODE_KEEP );

  // Configure defaults
  CLASS_RANGE(  0x0001, 0x0009, UNICODE_SKIP    );    // SKIP     white and control
  CLASS_RANGE(  0x000A, 0x000A, UNICODE_SPLIT   );    // SPLIT    \n
  CLASS_RANGE(  0x000B, 0x0020, UNICODE_SKIP    );    // SKIP     white and control
  CLASS_RANGE(  0x0021, 0x0022, UNICODE_SPLIT   );    // SPLIT    !  "
  CLASS_SINGLE( 0x0026,         UNICODE_SPLIT   );    // SPLIT    &
  CLASS_SINGLE( 0x0027,         UNICODE_COMBINE );    // COMBINE  '
  CLASS_RANGE(  0x0028, 0x002C, UNICODE_SPLIT   );    // SPLIT    (  )  *  +  ,
  CLASS_SINGLE( 0x002D,         UNICODE_COMBINE );    // COMBINE  -
  CLASS_SINGLE( 0x002E,         UNICODE_SPLIT   );    // SPLIT    .
  CLASS_SINGLE( 0x002F,         UNICODE_COMBINE );    // COMBINE  / 
  CLASS_RANGE(  0x003A, 0x003F, UNICODE_SPLIT   );    // SPLIT    :  ;  <  =  >  ?
  CLASS_RANGE(  0x005B, 0x005E, UNICODE_SPLIT   );    // SPLIT    [  \  ]  ^
  CLASS_SINGLE( 0x0060,         UNICODE_SPLIT   );    // SPLIT    `
  CLASS_RANGE(  0x007B, 0x007E, UNICODE_SPLIT   );    // SPLIT    {  |  }  ~
  CLASS_RANGE(  0x007F, 0x009F, UNICODE_SKIP    );    // SKIP     various Control characters
  CLASS_SINGLE( 0x00A0,         UNICODE_IGNORE  );    // IGNORE   non-breaking space
  CLASS_RANGE(  0x00A8, 0x00AC, UNICODE_SPLIT   );    // SPLIT    various Latin symbols
  CLASS_SINGLE( 0x00AD,         UNICODE_SKIP    );    // SKIP     soft hyphen
  CLASS_RANGE(  0x00AE, 0x00BF, UNICODE_SPLIT   );    // SPLIT    various Latin symbols
  CLASS_SINGLE( 0x00D7,         UNICODE_SPLIT   );    // SPLIT    x multiplication
  CLASS_SINGLE( 0x00F7,         UNICODE_SPLIT   );    // SPLIT    / division
  CLASS_RANGE(  0x01C0, 0x01C3, UNICODE_SPLIT   );    // SPLIT    various African letters CLASS_RANGE( ks
  CLASS_RANGE(  0x0242, 0x024F, UNICODE_SKIP    );    // SKIP     various Latin miscellaneous additions
  CLASS_RANGE(  0x02B9, 0x02FF, UNICODE_SPLIT   );    // SPLIT    various Spacing Modifier Letters
  CLASS_RANGE(  0x0370, 0x0373, UNICODE_SKIP    );    // SKIP     various Greek
  CLASS_RANGE(  0x0374, 0x0375, UNICODE_SPLIT   );    // SPLIT    various Greek numeral signs
  CLASS_RANGE(  0x0376, 0x0379, UNICODE_SKIP    );    // SKIP     various Greek archaic letters
  CLASS_SINGLE( 0x037A,         UNICODE_SPLIT   );    // SPLIT    Greek Ypogegrammeni
  CLASS_RANGE(  0x037B, 0x037D, UNICODE_SKIP    );    // SKIP     various Greek
  CLASS_SINGLE( 0x037E,         UNICODE_SPLIT   );    // SPLIT    Greek Question Mark
  CLASS_RANGE(  0x037F, 0x0383, UNICODE_SKIP    );    // SKIP     various Greek
  CLASS_RANGE(  0x0384, 0x0385, UNICODE_SPLIT   );    // SPLIT    various Greek spacing accents
  CLASS_SINGLE( 0x0387,         UNICODE_SPLIT   );    // SPLIT    Greek Ano Teleia
  CLASS_SINGLE( 0x038B,         UNICODE_SKIP    );    // SKIP     undefined
  CLASS_SINGLE( 0x038D,         UNICODE_SKIP    );    // SKIP     undefined
  CLASS_SINGLE( 0x03A2,         UNICODE_SKIP    );    // SKIP     undefined
  CLASS_SINGLE( 0x03CF,         UNICODE_SKIP    );    // SKIP     Greek Capital Kai Symbol
  CLASS_RANGE(  0x0482, 0x0486, UNICODE_SPLIT   );    // SPLIT    various Cyrillic
  CLASS_RANGE(  0x0487, 0x0489, UNICODE_SKIP    );    // SKIP     various Cyrillic
  CLASS_SINGLE( 0x04CF,         UNICODE_SKIP    );    // SKIP     Cyrillic Small Letter Palochka
  CLASS_RANGE(  0x04FA, 0x04FF, UNICODE_SKIP    );    // SKIP     various Cyrillic
  CLASS_SINGLE( 0xFEFF,         UNICODE_IGNORE  );    // IGNORE   UNICODE BOM

  __apply_char_class_overrides( PLANE_class, UNICODE_BMP_OFFSET, UNICODE_BMP_END, overrides );

#undef PLANE_class
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_BMP_remap( CTokenizer_t *self ) {
  // DEFAULT normalization remap of characters with accents and other marks in the basic multilingual plane to a normalized form in the ASCII range

#define PLANE_remap (*self->_BMP_remap)
#define PLANE_class (*self->_BMP_class)

  // Initialize
  for( int i=0; i<UNICODE_BMP_SIZE; i++ ) {
    REMAP_SINGLE( i, BMP_MAP_CODEPOINT( i ) );
  }

  // Configure
  REMAP_SINGLE(         0x0083,         'f'       );
  REMAP_SINGLE(         0x008A,         's'       );
  REMAP_SINGLE(         0x008C,         'o'       );
  REMAP_SINGLE(         0x008E,         'z'       );
  REMAP_SINGLE(         0x009A,         's'       );
  REMAP_SINGLE(         0x009C,         'o'       );
  REMAP_SINGLE(         0x009E,         'z'       );
  REMAP_SINGLE(         0x009F,         'y'       );
  REMAP_RANGE(          0x00C0, 0x00C6, 'A'       );
  REMAP_RANGE(          0x00E0, 0x00E6, 'a'       );
  REMAP_RANGE_ADJACENT( 0x0100, 0x0105, 'a', 'A'  );
  REMAP_SINGLE(         0x00C7,         'C'       );
  REMAP_SINGLE(         0x00E7,         'c'       );
  REMAP_RANGE_ADJACENT( 0x0106, 0x010D, 'c', 'C'  );
  REMAP_SINGLE(         0x00D0,         'D'       );
  REMAP_SINGLE(         0x00F0,         'd'       );
  REMAP_RANGE_ADJACENT( 0x010E, 0x0111, 'd', 'D'  );
  REMAP_RANGE(          0x00C8, 0x00CB, 'E'       );
  REMAP_RANGE(          0x00E8, 0x00EB, 'e'       );
  REMAP_RANGE_ADJACENT( 0x0112, 0x011B, 'e', 'E'  );
  REMAP_RANGE_ADJACENT( 0x011C, 0x0123, 'g', 'G'  );
  REMAP_RANGE_ADJACENT( 0x0124, 0x0127, 'h', 'H'  );
  REMAP_RANGE(          0x00CC, 0x00CF, 'I'       );
  REMAP_RANGE(          0x00EC, 0x00EF, 'i'       );
  REMAP_RANGE_ADJACENT( 0x0128, 0x0131, 'i', 'I'  );
  REMAP_RANGE_ADJACENT( 0x0132, 0x0135, 'j', 'J'  );
  REMAP_RANGE_ADJACENT( 0x0136, 0x0138, 'k', 'K'  );
  REMAP_RANGE_ADJACENT( 0x0139, 0x0142, 'L', 'l'  );
  REMAP_SINGLE(         0x00D1,         'N'       );
  REMAP_SINGLE(         0x00F1,         'n'       );
  REMAP_RANGE_ADJACENT( 0x0143, 0x0148, 'N', 'n'  );
  REMAP_RANGE_ADJACENT( 0x0149, 0x014B, 'n', 'N'  );
  REMAP_SINGLE(         0x00D8,         'O'       );
  REMAP_SINGLE(         0x00F8,         'o'       );
  REMAP_RANGE(          0x00D2, 0x00D6, 'O'       );
  REMAP_RANGE(          0x00F2, 0x00F6, 'o'       );
  REMAP_RANGE_ADJACENT( 0x014C, 0x0153, 'o', 'O'  );
  REMAP_RANGE_ADJACENT( 0x0154, 0x0159, 'r', 'R'  );
  REMAP_RANGE_ADJACENT( 0x015A, 0x0161, 's', 'S'  );
  REMAP_RANGE_ADJACENT( 0x0162, 0x0167, 't', 'T'  );
  REMAP_RANGE(          0x00D9, 0x00DC, 'U'       );
  REMAP_RANGE(          0x00F9, 0x00FC, 'u'       );
  REMAP_RANGE_ADJACENT( 0x0168, 0x0173, 'u', 'U'  );
  REMAP_RANGE_ADJACENT( 0x0174, 0x0175, 'w', 'W'  );
  REMAP_SINGLE(         0x00DD,         'Y'       );
  REMAP_SINGLE(         0x00FD,         'y'       );
  REMAP_SINGLE(         0x00FF,         'y'       );
  REMAP_RANGE_ADJACENT( 0x0176, 0x0178, 'y', 'Y'  );
  REMAP_RANGE_ADJACENT( 0x0179, 0x017e, 'Z', 'z'  );
  REMAP_SINGLE(         0x2019, 0x0027 ); // Right Single Quotation Mark -> '

  // remove combining diacritical marks
  CLASS_RANGE( 0x0300, 0x036F, UNICODE_IGNORE );    // IGNORE   combining diacritical marks

#undef PLANE_class
#undef PLANE_remap
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_BMP_lower( CTokenizer_t *self ) {
  // DEFAULT lowercase of basic multilingual plane characters considered uppercase to their lowercase form
#define PLANE_lower (*self->_BMP_lower)

  // Initialize
  for( int i=0; i<UNICODE_BMP_SIZE; i++ ) {
    LOWER_SINGLE( i, BMP_MAP_CODEPOINT( i ) );
  }
  
  // Configure
  for( int i=0x041; i<=0x05A; i++ )    PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 0x20;   // A->a, B->b, ... Z->z
  for( int i=0x0C0; i<=0x0D6; i++ )    PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 0x20;   // Map various accented upper to lower
  for( int i=0x0D8; i<=0x0DE; i++ )    PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 0x20;   // Map various accented upper to lower
  for( int i=0x100; i<=0x136; i+=2 )   PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 1;      // Map various accented upper to lower
  for( int i=0x139; i<=0x147; i+=2 )   PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 1;      // Map various accented upper to lower
  for( int i=0x14A; i<=0x176; i+=2 )   PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 1;      // Map various accented upper to lower
  for( int i=0x179; i<=0x17D; i+=2 )   PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 1;      // Map various accented upper to lower
  for( int i=0x187; i<=0x18B; i+=2 )   PLANE_lower[ i ] = BMP_MAP_CODEPOINT( i ) + 1;      // Map various accented upper to lower
  // TODO: still a few to go - and what about greek?

#undef PLANE_lower
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_ignore_BMP_digits( CTokenizer_t *self ) {
#define PLANE_class  (*self->_BMP_class)

  // 0 - 9
  for( int i='0'; i<='9'; i++ ) {
    CLASS_SINGLE( i, UNICODE_IGNORE );  // SKIP   0-9
  }

  // TODO: add other unicode digit codepoints
#undef PLANE_class
}



/*******************************************************************//**
 *
 * Supplementary Multilingual Plane 10000 - 1FFFF
 ***********************************************************************
 */
static void CTokenizer__set_default_SMP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides ) {
  // DEFAULT tokenization of the supplementary multilingual plane
#define PLANE_class (*self->_SMP_class)

  // Initialize
  for( int i=0; i<UNICODE_SMP_SIZE; i++ ) {
    CLASS_SINGLE( i, UNICODE_KEEP );
  }

  // Apply SMP overrides
  __apply_char_class_overrides( PLANE_class, UNICODE_SMP_OFFSET, UNICODE_SMP_END, overrides );

#undef PLANE_class
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SMP_remap( CTokenizer_t *self ) {
  // DEFAULT normalization remap of characters with accents and other marks in the supplementary multilingual plane to a normalized form

#define PLANE_remap (*self->_SMP_remap)

  // Initialize
  for( int i=0; i<UNICODE_SMP_SIZE; i++ ) {
    REMAP_SINGLE( i, SMP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_remap
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SMP_lower( CTokenizer_t *self ) {
  // DEFAULT lowercase of supplementary multilingual plane characters considered uppercase to their lowercase form

#define PLANE_lower (*self->_SMP_lower)

  // Initialize
  for( int i=0; i<UNICODE_SMP_SIZE; i++ ) {
    LOWER_SINGLE( i, SMP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_lower
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void CTokenizer__set_ignore_SMP_digits( CTokenizer_t *self ) {
}



/*******************************************************************//**
 *
 * Supplementary Ideographic Plane 20000 - 2FFFF
 ***********************************************************************
 */
static void CTokenizer__set_default_SIP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides ) {
  // DEFAULT tokenization of the supplementary ideographic plane
#define PLANE_class (*self->_SIP_class)

  // Initialize
  for( int i=0; i<UNICODE_SIP_SIZE; i++ ) {
    CLASS_SINGLE( i, UNICODE_KEEP );
  }

  __apply_char_class_overrides( PLANE_class, UNICODE_SIP_OFFSET, UNICODE_SIP_END, overrides );

#undef PLANE_class
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SIP_remap( CTokenizer_t *self ) {
  // DEFAULT normalization remap of characters with accents and other marks in the supplementary ideographic plane to a normalized form

#define PLANE_remap (*self->_SIP_remap)

  // Initialize
  for( int i=0; i<UNICODE_SIP_SIZE; i++ ) {
    REMAP_SINGLE( i, SIP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_remap
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SIP_lower( CTokenizer_t *self ) {
  // DEFAULT lowercase of supplementary ideographic plane characters considered uppercase to their lowercase form
#define PLANE_lower (*self->_SIP_lower)

  // Initialize
  for( int i=0; i<UNICODE_SIP_SIZE; i++ ) {
    LOWER_SINGLE( i, SIP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_lower
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void CTokenizer__set_ignore_SIP_digits( CTokenizer_t *self ) {
}



/*******************************************************************//**
 *
 * Supplementary Special-purpose Plane E0000 - E0FFF
 ***********************************************************************
 */
static void CTokenizer__set_default_SSP_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides ) {
  // DEFAULT tokenization of the supplementary special-purpose plane
#define PLANE_class (*self->_SSP_class)

  // Initialize
  for( int i=0; i<UNICODE_SSP_SIZE; i++ ) {
    CLASS_SINGLE( i, UNICODE_KEEP );
  }

  __apply_char_class_overrides( PLANE_class, UNICODE_SSP_OFFSET, UNICODE_SSP_END, overrides );

#undef PLANE_class
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SSP_remap( CTokenizer_t *self ) {
  // DEFAULT normalization remap of characters with accents and other marks in the supplementary special-purpose plane to a normalized form
#define PLANE_remap (*self->_SSP_remap)

  // Initialize
  for( int i=0; i<UNICODE_SSP_SIZE; i++ ) {
    REMAP_SINGLE( i, SSP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_remap
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SSP_lower( CTokenizer_t *self ) {
  // DEFAULT lowercase of supplementary special-purpose plane characters considered uppercase to their lowercase form
#define PLANE_lower (*self->_SSP_lower)

  // Initialize
  for( int i=0; i<UNICODE_SSP_SIZE; i++ ) {
    LOWER_SINGLE( i, SSP_MAP_CODEPOINT( i ) );
  }

#undef PLANE_lower
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void CTokenizer__set_ignore_SSP_digits( CTokenizer_t *self ) {
}



/*******************************************************************//**
 *
 * Supplementary Private Use Area Planes F0000 - 10FFFF
 ***********************************************************************
 */
static void CTokenizer__set_default_SPUA_class( CTokenizer_t *self, const tokenizer_char_classes_t *overrides ) {
  // DEFAULT tokenization of the supplementary private use area planes
#define PLANE_class (*self->_SPUA_class)

  // Initialize
  for( int i=0; i<UNICODE_SPUA_SIZE; i++ ) {
    CLASS_SINGLE( i, UNICODE_KEEP );
  }

  __apply_char_class_overrides( PLANE_class, UNICODE_SPUA_OFFSET, UNICODE_SPUA_END, overrides );

#undef PLANE_class
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SPUA_remap( CTokenizer_t *self ) {
  // DEFAULT normalization remap of characters with accents and other marks in the supplementary private use area plane to a normalized form
#define PLANE_remap (*self->_SPUA_remap)

  // Initialize
  for( int i=0; i<UNICODE_SPUA_SIZE; i++ ) {
    REMAP_SINGLE( i, SPUA_MAP_CODEPOINT( i ) );
  }

#undef PLANE_remap
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void CTokenizer__set_default_SPUA_lower( CTokenizer_t *self ) {
  // DEFAULT lowercase of supplementary private use area planes characters considered uppercase to their lowercase form

#define PLANE_lower (*self->_SPUA_lower)

  // Initialize
  for( int i=0; i<UNICODE_SPUA_SIZE; i++ ) {
    LOWER_SINGLE( i, SPUA_MAP_CODEPOINT( i ) );
  }

#undef PLANE_lower
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void CTokenizer__set_ignore_SPUA_digits( CTokenizer_t *self ) {
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static void CTokenizer__load( CTokenizer_t *self, const BYTE *text ) {
  self->_input = text;
  self->_irp = self->_input;
  self->_swp = self->_surface;
  self->_nwp = self->_normal;
  self->_state.newline = 1;
  self->_surface[0] = '\0';
  self->_normal[0] = '\0';
  __delete_tokenmap( &self->_tokmap );
}



/*******************************************************************//**
 * 
 * UTF-8 parser. Scan and validate UTF-8 bytes in the input and return
 * one codepoint from this input. The input pointer is advanced by the
 * number of read bytes.
 * 
 * UTF-8 is decoded to Unicode and the resulting codepoint returned.
 *
 * Return the Unicode codepoint, or -1 or fatal error.
 *
 ***********************************************************************
 */
static unicode_codepoint_t __getc_utf8( const BYTE **input, BYTE bytes[], int *nbytes ) {
  BYTE c;
  DWORD cpoint = 0;
  DWORD ccode;
  int utf8_state;
  BYTE *bp;
  int ncont = 0;

utf8_start:
  if( (c = *(*input)++) < 0x80 ) {
    *nbytes = 1;
    bytes[0] = c;
    return c;
  }
  else {
    utf8_state = UTF8_EXPECT_START;
    bp = bytes;
    do {
      switch( utf8_state ) {
      case UTF8_EXPECT_START:
        if( (ncont = g_utf8_startn[c]) == 0 ) {
          goto utf8_invalid_start;
        }
        cpoint = g_utf8_startc[c];
        *bp++ = c; // byte ok, save it
        utf8_state += ncont;
        break;
      case UTF8_EXPECT_CONT3: // 3rd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode << 12;
        if( (bytes[0] == 0xF0) && (cpoint < 0x10000) ) {
          goto utf8_invalid_cont; // overlong?
        }
        else if( (bytes[0] == 0xF4) && (cpoint > 0x10FFFF) ) {
          goto utf8_invalid_cont; // overlong?
        }
        *bp++ = c; // byte ok, save it 
        utf8_state = UTF8_EXPECT_CONT2;
        break;
      case UTF8_EXPECT_CONT2: // 2nd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode << 6;
        if( (bytes[0] == 0xE0) && (cpoint < 0x800) ) {
          goto utf8_invalid_cont; // overlong?
        }
        *bp++ = c; // byte ok, save it
        utf8_state = UTF8_EXPECT_CONT1;
        break;
      case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode;
        if( in_surrogate_range( cpoint ) ) {
          goto utf8_no_surrogates;
        }
        *bp++ = c; // byte ok, save it
        *nbytes = 1 + ncont;
        return cpoint; // <- DONE
      default:
        goto utf8_invalid;
      }
    } while( (c = *(*input)++) != '\0' );
  }
  
utf8_invalid_start:
  goto utf8_ffwd;
  
utf8_invalid_cont:
  (*input)--; // back up
  goto utf8_ffwd;

utf8_no_surrogates:
  goto utf8_ffwd;

utf8_ffwd:
  while( *(*input) & 0x80 ) {
    (*input)++;    // skip over junk in the broken utf-8 sequence
  }
  goto utf8_start;

utf8_invalid:
  return INVALID_CODEPOINT; // <- codepoint -1 will end parsing of current input
  
}



/*******************************************************************//**
 * 
 * UTF-8 parser. Scan and validate UTF-8 bytes in the input and return
 * one codepoint from this input. The input pointer is advanced by the
 * number of read bytes.
 * 
 * UTF-8 is decoded to Unicode and the resulting codepoint returned.
 *
 * Return the Unicode codepoint, or -1 or fatal error.
 *
 ***********************************************************************
 */
static unicode_codepoint_t __getc_utf8_strict( const BYTE **input, BYTE bytes[], int *nbytes ) {
  BYTE c;
  DWORD cpoint = 0;
  DWORD ccode;
  int utf8_state;
  BYTE *bp;
  int ncont = 0;
  const BYTE *rp = *input;

  if( (c = *(*input)++) < 0x80 ) {
    *nbytes = 1;
    bytes[0] = c;
    return c;
  }
  else {
    utf8_state = UTF8_EXPECT_START;
    bp = bytes;
    do {
      switch( utf8_state ) {
      case UTF8_EXPECT_START:
        if( (ncont = g_utf8_startn[c]) == 0 ) {
          goto utf8_invalid_start;
        }
        cpoint = g_utf8_startc[c];
        *bp++ = c; // byte ok, save it
        utf8_state += ncont;
        break;
      case UTF8_EXPECT_CONT3: // 3rd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode << 12;
        if( (bytes[0] == 0xF0) && (cpoint < 0x10000) ) {
          goto utf8_invalid_cont; // overlong?
        }
        else if( (bytes[0] == 0xF4) && (cpoint > 0x10FFFF) ) {
          goto utf8_invalid_cont; // overlong?
        }
        *bp++ = c; // byte ok, save it 
        utf8_state = UTF8_EXPECT_CONT2;
        break;
      case UTF8_EXPECT_CONT2: // 2nd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode << 6;
        if( (bytes[0] == 0xE0) && (cpoint < 0x800) ) {
          goto utf8_invalid_cont; // overlong?
        }
        if( in_surrogate_range( cpoint ) ) {
          goto utf8_no_surrogates;
        }
        *bp++ = c; // byte ok, save it
        utf8_state = UTF8_EXPECT_CONT1;
        break;
      case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_invalid_cont;
        }
        cpoint += ccode;
        *bp++ = c; // byte ok, save it
        *nbytes = 1 + ncont;
        return cpoint; // <- DONE
      default:
        goto utf8_invalid;
      }
    } while( (c = *(*input)++) != '\0' );
  }
  
utf8_invalid_start:
  goto utf8_invalid;
  
utf8_invalid_cont:
  goto utf8_invalid;

utf8_no_surrogates:
  goto utf8_invalid;

utf8_invalid:
  *input = rp;
  return INVALID_CODEPOINT; // <- codepoint -1 will end parsing of current input
  
}



/*******************************************************************//**
 * 
 * UTF-8 codepoint counter. Scan and validate UTF-8 bytes in the input.
 * 
 ***********************************************************************
 */
static int64_t __getsize_utf8( const BYTE *input, int64_t *rbytes, int64_t *errpos ) {
  DWORD cpoint = 0;
  DWORD ccode;
  int ncont = 0;
  int utf8_state = UTF8_EXPECT_START;
  int64_t n_cp = 0;
  BYTE z = 0;
  BYTE c;
  const BYTE *rp = input;
  while( (c = *rp++) != '\0' ) {
    if( c < 0x80 ) {
      ++n_cp;
    }
    else {
      switch( utf8_state ) {
      case UTF8_EXPECT_START:
        z = c;
        if( (ncont = g_utf8_startn[z]) == 0 ) {
          goto utf8_error;
        }
        cpoint = g_utf8_startc[z];
        utf8_state += ncont;
        continue;
      case UTF8_EXPECT_CONT3: // 3rd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 12;
        if( (z == 0xF0) && (cpoint < 0x10000) ) {
          goto utf8_error;
        }
        else if( (z == 0xF4) && (cpoint > 0x10FFFF) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT2;
        continue;
      case UTF8_EXPECT_CONT2: // 2nd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 6;
        if( (z == 0xE0) && (cpoint < 0x800) ) {
          goto utf8_error;
        }
        if( in_surrogate_range( cpoint ) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT1;
        continue;
      case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode;
        ++n_cp;
        utf8_state = UTF8_EXPECT_START;
        continue;
      default:
        goto utf8_error;
      }
    }
  }

  if( utf8_state != UTF8_EXPECT_START ) {
    goto utf8_error;
  }

  if( rbytes ) {
    *rbytes = (rp - input) - 1;
  }

  return n_cp;

utf8_error:
  if( errpos ) {
    *errpos = (rp - input) - 1;
  }

  return -1;
}



/*******************************************************************//**
 * 
 * UTF-8 codepoint counter. Scan and validate UTF-8 bytes in the input.
 * 
 ***********************************************************************
 */
static int64_t __getsize_sz_utf8( const BYTE *input, int64_t sz, int64_t *errpos ) {
  DWORD cpoint = 0;
  DWORD ccode;
  int ncont = 0;
  int utf8_state = UTF8_EXPECT_START;
  int64_t n_cp = 0;
  BYTE z = 0;
  BYTE c;
  const BYTE *rp = input;
  const BYTE *end = rp + sz;
  while( rp < end ) {
    if( (c = *rp++) < 0x80 ) {
      ++n_cp;
    }
    else {
      switch( utf8_state ) {
      case UTF8_EXPECT_START:
        z = c;
        if( (ncont = g_utf8_startn[z]) == 0 ) {
          goto utf8_error;
        }
        cpoint = g_utf8_startc[z];
        utf8_state += ncont;
        continue;
      case UTF8_EXPECT_CONT3: // 3rd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 12;
        if( (z == 0xF0) && (cpoint < 0x10000) ) {
          goto utf8_error;
        }
        else if( (z == 0xF4) && (cpoint > 0x10FFFF) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT2;
        continue;
      case UTF8_EXPECT_CONT2: // 2nd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 6;
        if( (z == 0xE0) && (cpoint < 0x800) ) {
          goto utf8_error;
        }
        if( in_surrogate_range( cpoint ) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT1;
        continue;
      case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode;
        ++n_cp;
        utf8_state = UTF8_EXPECT_START;
        continue;
      default:
        goto utf8_error;
      }
    }
  }

  if( utf8_state != UTF8_EXPECT_START ) {
    goto utf8_error;
  }

  return n_cp;

utf8_error:
  if( errpos ) {
    *errpos = (rp - input) - 1;
  }

  return -1;
}



/*******************************************************************//**
 * 
 * UTF-8 validator. Scan and validate UTF-8 bytes in the input.
 * 
 ***********************************************************************
 */
static bool __validate_utf8( const BYTE *input, int64_t *errpos ) {
  DWORD cpoint = 0;
  DWORD ccode;
  int ncont = 0;
  int utf8_state = UTF8_EXPECT_START;
  BYTE z = 0;
  BYTE c;
  const BYTE *rp = input;

  while( (c = *rp++) != '\0' ) {
    if( c < 0x80 ) {
      continue;
    }

    switch( utf8_state ) {
    case UTF8_EXPECT_START:
      z = c;
      if( (ncont = g_utf8_startn[z]) == 0 ) {
        goto utf8_error;
      }
      cpoint = g_utf8_startc[z];
      utf8_state += ncont;
      continue;
    case UTF8_EXPECT_CONT3: // 3rd to last cont
      if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
        goto utf8_error;
      }
      cpoint += ccode << 12;
      if( (z == 0xF0) && (cpoint < 0x10000) ) {
        goto utf8_error;
      }
      else if( (z == 0xF4) && (cpoint > 0x10FFFF) ) {
        goto utf8_error;
      }
      utf8_state = UTF8_EXPECT_CONT2;
      continue;
    case UTF8_EXPECT_CONT2: // 2nd to last cont
      if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
        goto utf8_error;
      }
      cpoint += ccode << 6;
      if( (z == 0xE0) && (cpoint < 0x800) ) {
        goto utf8_error;
      }
      if( in_surrogate_range( cpoint ) ) {
        goto utf8_error;
      }
      utf8_state = UTF8_EXPECT_CONT1;
      continue;
    case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
      if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
        goto utf8_error;
      }
      cpoint += ccode;
      utf8_state = UTF8_EXPECT_START;
      continue;
    default:
      goto utf8_error;
    }
  }

  if( utf8_state != UTF8_EXPECT_START ) {
    goto utf8_error;
  }

  return true;

utf8_error:
  if( errpos ) {
    *errpos = (rp - input) - 1;
  }

  return false;
}



/*******************************************************************//**
 * 
 * Validating UTF-8 copy. Validate and copy UTF-8 bytes from input to output
 * 
 * Return number of bytes copied, or -1 on error.
 * 
 ***********************************************************************
 */
static int64_t __copy_utf8( const BYTE *input, char *output, int64_t *errpos ) {
  DWORD cpoint = 0;
  DWORD ccode;
  int ncont = 0;
  int utf8_state = UTF8_EXPECT_START;
  BYTE z = 0;
  BYTE c;
  const BYTE *rp = input;
  char *wp = output;
  while( (c = *rp++) != '\0' ) {
    *wp++ = c;
    if( c >= 0x80 ) {
      switch( utf8_state ) {
      case UTF8_EXPECT_START:
        z = c;
        if( (ncont = g_utf8_startn[z]) == 0 ) {
          goto utf8_error;
        }
        cpoint = g_utf8_startc[z];
        utf8_state += ncont;
        continue;
      case UTF8_EXPECT_CONT3: // 3rd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 12;
        if( (z == 0xF0) && (cpoint < 0x10000) ) {
          goto utf8_error;
        }
        else if( (z == 0xF4) && (cpoint > 0x10FFFF) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT2;
        continue;
      case UTF8_EXPECT_CONT2: // 2nd to last cont
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode << 6;
        if( (z == 0xE0) && (cpoint < 0x800) ) {
          goto utf8_error;
        }
        if( in_surrogate_range( cpoint ) ) {
          goto utf8_error;
        }
        utf8_state = UTF8_EXPECT_CONT1;
        continue;
      case UTF8_EXPECT_CONT1: // last cont (utf-8 sequence complete)
        if( (ccode = g_utf8_contc[c]) == INVALID_CODEPOINT ) {
          goto utf8_error;
        }
        cpoint += ccode;
        utf8_state = UTF8_EXPECT_START;
        continue;
      default:
        goto utf8_error;
      }
    }
  }
  *wp = '\0';


  if( utf8_state != UTF8_EXPECT_START ) {
    goto utf8_error;
  }
  
  return wp - output;

utf8_error:
  if( errpos ) {
    *errpos = (rp - input) - 1;
  }

  return -1;
}



/***********************************************************************
 * Convert the unicode codepoint to a utf-8 sequence placed into bytes,
 * and return the number of bytes in the resulting utf-8 sequence.
 *
 * The bytes[] array must be allocated by caller to hold at least 4 bytes.
 *
 ***********************************************************************
 */
static int __putc_utf8( DWORD codepoint, BYTE bytes[] ) {

  // 1 byte
  if( codepoint <= 0x7F ) {
    bytes[0] = (BYTE)codepoint;
    return 1;
  }
  // 2 bytes
  else if( codepoint <= 0x7FF ) {
    bytes[0] = 0xC0 | (BYTE)(codepoint >> 6);     //  110xxxxx
    bytes[1] = 0x80 | (BYTE)(codepoint & 0x3F);   //  10xxxxxx
    return 2;
  }
  // 3 bytes
  else if( codepoint <= 0xFFFF ) {
    if( in_surrogate_range( codepoint ) ) {
      return -1;
    }
    bytes[0] = 0xE0 | (BYTE)(codepoint >> 12);          //  1110xxxx
    bytes[1] = 0x80 | (BYTE)((codepoint >> 6) & 0x3F);  //  10xxxxxx
    bytes[2] = 0x80 | (BYTE)(codepoint & 0x3f);         //  10xxxxxx
    return 3;
  }
  // 4 bytes
  else if( codepoint <= 0x10FFFF ) {
    bytes[0] = 0xF0 | (BYTE)(codepoint >> 18);          //  11110xxx
    bytes[1] = 0x80 | (BYTE)((codepoint >> 12) & 0x3F); //  10xxxxxx
    bytes[2] = 0x80 | (BYTE)((codepoint >> 6) & 0x3F);  //  10xxxxxx
    bytes[3] = 0x80 | (BYTE)(codepoint & 0x3F);         //  10xxxxxx
    return 4;
  }
  // error
  else {
    return -1;
  }

}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_length_utf8( const BYTE *utf8_text, int64_t sz, int64_t *rsz, int64_t *errpos ) {
  if( sz < 0 ) {
    return __getsize_utf8( utf8_text, rsz, errpos );
  }
  else {
    return __getsize_sz_utf8( utf8_text, sz, errpos );
  }
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT bool COMLIB_check_utf8( const BYTE *utf8_text, int64_t *errpos ) {
  return __validate_utf8( utf8_text, errpos );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_copy_utf8( const BYTE *utf8_text, char *output, int64_t *errpos ) {
  return __copy_utf8( utf8_text, output, errpos );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_offset_utf8( const BYTE *utf8_text, int64_t utf8_len, int64_t index, int64_t *rcp, int64_t *errpos ) {

  if( index < 0 ) {
    if( utf8_len < 0 ) {
      if( (utf8_len = COMLIB_length_utf8( utf8_text, -1, NULL, errpos )) < 0 ) {
        return -1;
      }
    }
    if( (index = utf8_len + index) < 0 ) {
      index = 0;
    }
  }

  BYTE c;
  BYTE utf8_bytes[4];
  int nbytes;
  const BYTE *rp = utf8_text;
  *rcp = index;
  while( (c = *rp) != '\0' && index-- > 0 ) {
    // ASCII range - faster
    if( (c & 0x80) == 0 ) {
      ++rp;
    }
    // Multi-byte UTF-8 sequence - slower
    else if( __getc_utf8_strict( &rp, utf8_bytes, &nbytes ) == INVALID_CODEPOINT ) {
      *errpos = rp - utf8_text;
      return -1;
    }
  }

  int64_t offset = rp - utf8_text;

  *rcp -= index;
  if( c != '\0' ) {
    --(*rcp);
  }

  return offset;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT Unicode COMLIB_decode_utf8( const BYTE *utf8_text, int64_t *errpos ) {
  Unicode unicode = NULL;
  
  XTRY {
    if( (unicode = NewUnicode()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x121 );
    }

    IUnicode iUni = CALLABLE( unicode );

    BYTE c;
    DWORD codepoint;
    BYTE utf8_bytes[4] = {0};
    int nbytes = 0;

    const BYTE *rp = utf8_text;

    while( (c = *rp) != '\0' ) {

      // ASCII range - faster
      if( (c & 0x80) == 0 ) {
        codepoint = c;
        rp++;
      }
      // Multi-byte UTF-8 sequence - slower
      else {
        // Use lenient utf8 codepoint decoder (skips over invalid utf8 sequences)
        if( (codepoint = __getc_utf8( &rp, utf8_bytes, &nbytes )) == INVALID_CODEPOINT ) {
          *errpos = rp - utf8_text;
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x122 );
        }
      }

      if( iUni->Append( unicode, &codepoint ) != 1 ) {
        *errpos = rp - utf8_text;
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x123 );
      }
    }

  }
  XCATCH( errcode ) {
    if( unicode ) {
      COMLIB_OBJECT_DESTROY( unicode );
      unicode = NULL;
    }
  }
  XFINALLY {
  }

  return unicode;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
DLL_EXPORT BYTE * COMLIB_encode_utf8( Unicode unicode, int64_t *len, int64_t *errpos ) {
  BYTE *utf8_text = NULL;
  
  IUnicode iUni = CALLABLE( unicode );
  int64_t sz = iUni->Length( unicode );
  CByteList_constructor_args_t args = {
    .element_capacity = sz, // start at same length as input, just enough if ascii, will expand later if not
    .comparator = NULL
  };
  CByteList_t *string = COMLIB_OBJECT_NEW( CByteList_t, NULL, &args );
  CByteList_vtable_t *ibytes = CALLABLE( string );   

  XTRY {
    
    const BYTE nul = '\0';
    BYTE utf8_bytes[4];
    int nbytes;
    DWORD codepoint;
    
    for( int64_t i=0; i<sz; i++ ) {
      iUni->Get( unicode, i, &codepoint );
      if( (nbytes = __putc_utf8( codepoint, utf8_bytes )) > 0 ) {
        ibytes->Extend( string, utf8_bytes, nbytes );
      }
      else {
        *errpos = i;
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x131 );
      }
    }

    *len = ibytes->Length( string );
    ibytes->Append( string, &nul );

    utf8_text = ibytes->YankBuffer( string );

  }
  XCATCH( errcode ) {
    if( utf8_text ) {
      ALIGNED_FREE( utf8_text );
      utf8_text = NULL;
    }
  }
  XFINALLY {
    if( string ) {
      COMLIB_OBJECT_DESTROY( string );
    }
  }

  return utf8_text;

}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static void __token_append_char( CTokenizer_t *self, DWORD codepoint, BYTE bytes[], int nbytes, int lowercase, uint16_t *toklen ) {
  // ASCII
  if( codepoint < 0x80 ) {
    ++(*toklen);
    *self->_swp++ = (BYTE)codepoint;
    if( lowercase ) {
      *self->_nwp++ = (BYTE)(*self->_BMP_lower)[ codepoint ];
    }
  } 
  // Multi-byte UTF-8 sequence
  else {
    *toklen += (uint16_t)nbytes;
    for( int n=0; n<nbytes; n++ ) {
      *self->_swp++ = bytes[n];
      if( lowercase ) {
        *self->_nwp++ = bytes[n]; // TODO: don't do this, lowercase instead
      }
    }
    // TODO: lowercase UTF-8 sequence
  }


}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static DWORD __remap_codepoint( CTokenizer_t *self, DWORD codepoint ) {
  switch( codepoint & UNICODE_PLANE_MASK ) {
  // Basic Multilingual Plane
  case UNICODE_PLANE_0_FIRST:
    return (*self->_BMP_remap)[ codepoint & UNICODE_BMP_MASK ];
  // Supplementary Multilingual Plane
  case UNICODE_PLANE_1_FIRST:
    return (*self->_SMP_remap)[ codepoint & UNICODE_SMP_MASK ];
  // Supplementary Ideographic Plane
  case UNICODE_PLANE_2_FIRST:
    return (*self->_SIP_remap)[ codepoint & UNICODE_SIP_MASK ];
  // Supplementary Special-purpose Plane
  case UNICODE_PLANE_14_FIRST:
    return (*self->_SSP_remap)[ codepoint & UNICODE_SSP_MASK ];
  // Supplementary Private Use Area Plane
  case UNICODE_PLANE_15_FIRST:
    return (*self->_SPUA_remap)[ codepoint & UNICODE_INTRAPLANE_MASK ];
  // Supplementary Private Use Area Plane
  case UNICODE_PLANE_16_FIRST:
    return (*self->_SPUA_remap)[ (codepoint & UNICODE_INTRAPLANE_MASK) | 0x10000 ];
  default:
    return codepoint;
  }
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static tokenizer_char_class __classify_codepoint( CTokenizer_t *self, DWORD codepoint ) {
  BYTE *_class;
  switch( codepoint >> 16 ) {
  // Supplementary Multilingual Plane
  case 0x01:
    _class = *self->_SMP_class;
    break;
  // Supplementary Ideographic Plane
  case 0x02:
    _class = *self->_SIP_class;
    break;
  // Supplementary Special-purpose Plane
  case 0x0E:
    _class = *self->_SSP_class;
    break;
  // Supplementary Private Use Area Planes
  case 0x0F:
    /* FALLTHRU */
  case 0x10:
    _class = *self->_SPUA_class;
    break;
  default:
    _class = *self->_BMP_class;
  }
  return _class[ codepoint & 0xFFFF ];
}



/***********************************************************************
 * 
 * Scan the input stream for the next token and place it in
 * the tokenizer's token buffer.
 *
 * NOT THREAD SAFE.
 *
 * Return pointer to token, or NULL on fatal error.
 *
 ***********************************************************************
 */
static BYTE * __next_token_nolock( CTokenizer_t *self, tokinfo_t *ret_tokinfo ) {

  BYTE c;
  DWORD codepoint;
  tokenizer_char_class char_class = UNICODE_KEEP;
  DWORD remapped;

  int lowercase = self->_flags.lowercase;
  tokenmap_t *tokmap = self->_tokmap;

  tokinfo_t tokeninfo = {
    .len = 0,
    .typ = STRING_TYPE_NONE,
    .cls = UNICODE_KEEP,
    .flw = TOKEN_FLOW_NONE,
    .lin = self->_state.newline // capture newline flag if set
  };

  self->_state.newline = 0;

  BYTE utf8_bytes[4] = {0};
  int nbytes = 0;

  DWORD *BMP_remap = *self->_BMP_remap;
  BYTE *BMP_class = *self->_BMP_class;

  self->_swp = self->_surface;
  self->_nwp = self->_normal;

  __f_getc_utf8 GetCodepoint = self->_flags.strict_utf8 ? __getc_utf8_strict : __getc_utf8;

  const BYTE *surface = self->_irp;
  const BYTE *pchr;
  while( (c = *(pchr=self->_irp)) != '\0' && tokeninfo.len < TOKENIZER_MAX_LEN ) {

    // ASCII range - faster
    if( (c & 0x80) == 0 ) {
      // (No Normalization of ASCII range!)
      // Classification
      char_class = BMP_class[ codepoint=c ];
      self->_irp++;
      // Reset newline flag, capture into tokeninfo if no token yet
      if( c == '\n' ) {
        self->_state.newline = 1;
        if( tokeninfo.len == 0 ) {
          tokeninfo.lin = 1;
        }
      }
    }

    // Multi-byte UTF-8 sequence - slower
    else {
      // Use lenient utf8 codepoint decoder (skips over invalid utf8 sequences)
      if( (codepoint = GetCodepoint( &self->_irp, utf8_bytes, &nbytes )) == INVALID_CODEPOINT ) {
        return NULL; // error
      }
      
      // Normalization
      if( self->_flags.normalize ) {
        // Remap Basic Multilingual Plane
        if( codepoint < UNICODE_PLANE_0_LAST ) {
          if( (remapped = BMP_remap[ codepoint ]) > 0 ) {
            codepoint = remapped;
          }
        }
        // Remap Higher planes
        else {
          codepoint = __remap_codepoint( self, codepoint );
        }
      }

      // Classification
      if( codepoint < UNICODE_PLANE_0_LAST ) {
        // Classify Basic Multilingual Plane
        char_class = BMP_class[ codepoint ];
      }
      else {
        // Classify Higher planes
        char_class = __classify_codepoint( self, codepoint );
      }
    }

    // Build token
    switch( char_class ) {

    /* KEEP */
    case UNICODE_KEEP:
      if( tokeninfo.flw == TOKEN_FLOW_SPLIT ) {
        tokeninfo.flw = TOKEN_FLOW_CONT;
        self->_irp = pchr; // reset to start of KEEP character
        goto end_of_token;
      }
      else if( tokeninfo.flw == TOKEN_FLOW_NONE ) {
        tokeninfo.flw = TOKEN_FLOW_START;
      }
      __token_append_char( self, codepoint, utf8_bytes, nbytes, lowercase, &tokeninfo.len );
      break;

    /* SKIP */
    case UNICODE_SKIP:
      /* skip character will end token in progress without being included, and input pointer advanced to next character */
      if( tokeninfo.len > 0 ) {
        if( tokeninfo.flw == TOKEN_FLOW_SPLIT ) {
          tokeninfo.flw = TOKEN_FLOW_BREAK;
        }
        goto end_of_token;
      }
      /* no token built yet, keep trying */
      surface = self->_irp;
      break;

    /* SPLIT */
    case UNICODE_SPLIT:
      /* no token built yet, token will be this single character */
      tokeninfo.flw = TOKEN_FLOW_SPLIT;
      if( tokeninfo.len == 0 ) {
      __token_append_char( self, codepoint, utf8_bytes, nbytes, lowercase, &tokeninfo.len );
        tokeninfo.cls = char_class;
        break;
      }
      /* split character will end token in progress without being included, and input pointer reset to start of SPLIT character */
      else {
        self->_irp = pchr;
        goto end_of_token;
      }

    /* IGNORE */
    case UNICODE_IGNORE:
      /* pretend this character didn't exist in the input and keep going */
      break;

    /* COMBINE */
    case UNICODE_COMBINE:
      /* treat as KEEP if in the middle of token, otherwise SPLIT */
      if( tokeninfo.len > 0 ) {  // conditionally treat as KEEP iff next char is a KEEP
        if( tokeninfo.flw == TOKEN_FLOW_SPLIT ) {
          tokeninfo.flw = TOKEN_FLOW_CONT;
          self->_irp = pchr; // reset to start of COMBINE character
          goto end_of_token;
        }
        // TODO: add conditional KEEP. For now "ab- c" will become ["ab-","c"] should be ["ab", "-", "c"].   FIX.
        /* we may be in the middle of a token */
      __token_append_char( self, codepoint, utf8_bytes, nbytes, lowercase, &tokeninfo.len );
        break;
      }
      else {  // treat as SPLIT
      __token_append_char( self, codepoint, utf8_bytes, nbytes, lowercase, &tokeninfo.len );
        tokeninfo.cls = char_class;
        goto end_of_token;
      }

    default:
      return NULL; // error
    }
  }

end_of_token:
  *self->_swp = '\0';
  *self->_nwp = '\0';

  // Set token info type
  if( tokeninfo.cls == UNICODE_SPLIT ) {
    tokeninfo.typ = STRING_TYPE_SPLIT;
  }
  else {
    tokeninfo.typ = __string_type( (BYTE*)self->_surface );
  }

  // If we are using a tokenmap, populate it
  if( tokmap ) {
    // Add the token to tokenmap
    if( tokeninfo.len > 0 ) { 
      if( (tokeninfo.cls != UNICODE_SPLIT || self->_flags.keepsplit) ) {
        // Surface offset of this token
        tokeninfo.soffset = (uint32_t)(surface - self->_input);
        // Extend map ?
        if( tokmap->dend - tokmap->dcur <= tokeninfo.len || tokmap->tend - tokmap->tcur < 1 ) {
          if( (self->_tokmap = __extend_tokenmap_nolock( tokmap )) == NULL ) {
            return NULL;
          }
        }
        *(tokmap->tcur++) = tokeninfo;
        // copy token to map
        if( lowercase ) {
          memcpy( tokmap->dcur, self->_normal, tokeninfo.len );
        }
        else {
          memcpy( tokmap->dcur, self->_surface, tokeninfo.len );
        }
        // advance and terminate
        tokmap->dcur += tokeninfo.len;
        *(tokmap->dcur++) = '\0'; // terminate each for easier consumption of tokens later
        tokmap->ntok++;
      }
    }
    // No more tokens, terminate
    else {
      // Set final size of input
      tokmap->slen = (uint32_t)(self->_irp - self->_input);
      // Terminate
      if( tokmap->tcur < tokmap->tend ) {
        tokmap->tcur->len = 0; // terminate token length map
      }
      tokmap->tend = tokmap->tcur; // end of token map at current token
    }
  }

  // Set return tokeninfo if requested
  if( ret_tokinfo ) {
    *ret_tokinfo = tokeninfo;
  }

  // Return token
  if( lowercase ) {
    return self->_normal;
  }
  else {
    return self->_surface;
  }
   
}



/***********************************************************************
 * 
 * Scan the input stream for the next token and place it in
 * the tokenizer's token buffer.
 *
 * Return pointer to token, or NULL on fatal error.
 *
 ***********************************************************************
 */
static BYTE * CTokenizer__next( CTokenizer_t *self, tokinfo_t *tokinfo ) {
  BYTE *token;

  SYNCHRONIZE_ON( self->_lock ) {
    token = __next_token_nolock( self, tokinfo );
  } RELEASE;

  return token;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
static tokenmap_t * __extend_tokenmap_nolock( tokenmap_t *tokenmap ) {
  tokenmap_t *tokmap = tokenmap;

  XTRY {
    // Create new map if it doesn't exist
    if( tokmap == NULL ) {
      if( (tokmap = (tokenmap_t*) calloc( 1, sizeof(tokenmap_t) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x141 );
      }
    }
    // Create new data if it doesn't exist
    if( tokmap->data == NULL ) {
      tokmap->szdata = TOKENIZER_TOKMAP_INIT_SZDATA;
      if( (tokmap->dcur = tokmap->data = (BYTE*) calloc( tokmap->szdata, sizeof(BYTE) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x142 );
      }
      tokmap->dend = tokmap->data + tokmap->szdata;
    }
    // Create new token length map if it doesn't exist
    if( tokmap->tok == NULL ) {
      tokmap->sztok = TOKENIZER_TOKMAP_INIT_SZMAP;
      if( (tokmap->tcur = tokmap->tok = (tokinfo_t*) calloc( tokmap->sztok, sizeof(tokinfo_t) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x143 );
      }
      tokmap->tend = tokmap->tok + tokmap->sztok;
    }
    // Extend data if remaining capacity less than worst-case token length
    if( tokmap->dend - tokmap->dcur < TOKENIZER_MAX_LEN ) {  // (ok not to account for 0-terminator since data is not 0-terminated...)
      intptr_t offset = tokmap->dcur - tokmap->data; // save cursor in case realloc moves the block
      int new_sz = tokmap->szdata * TOKENIZER_TOKMAP_GROWTH_FACTOR;
      BYTE *data = (BYTE*)realloc( tokmap->data, sizeof(BYTE) * new_sz );
      if( !data ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x144 );
      }
      tokmap->szdata = new_sz;
      tokmap->data = data;
      tokmap->dcur = tokmap->data + offset;
      tokmap->dend = tokmap->data + tokmap->szdata;
    }
    // Extend token length map if out of room
    if( tokmap->tend - tokmap->tcur < 1 ) {
      intptr_t offset = tokmap->tcur - tokmap->tok; // save cursor if case realloc moves the block
      int new_sz = tokmap->sztok * TOKENIZER_TOKMAP_GROWTH_FACTOR;
      tokinfo_t *tok = (tokinfo_t*)realloc( tokmap->tok, sizeof(tokinfo_t) * new_sz );
      if( !tok ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x145 );
      }
      tokmap->sztok = new_sz;
      tokmap->tok = tok;
      tokmap->tcur = tokmap->tok + offset;
      tokmap->tend = tokmap->tok + tokmap->sztok;
    }
  }
  XCATCH( errcode ) {
    __delete_tokenmap( &tokmap );
  }
  XFINALLY {}

  return tokmap;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
static void __delete_tokenmap( tokenmap_t **tokmap ) {
  if( tokmap && *tokmap ) {
    if( (*tokmap)->tok ) {
      free( (*tokmap)->tok );
    }
    if( (*tokmap)->data ) {
      free( (*tokmap)->data );
    }
    if( (*tokmap)->surface ) {
      free( (*tokmap)->surface );
    }
    free( *tokmap );
    *tokmap = NULL;
  }
}



/***********************************************************************
 * Tokenize the input text and return a pointer to a tokenmap of resulting
 * tokens.
 * 
 * Return: Pointer to tokenmap
 *
 * NOTE: CALLER OWNS RETURNED TOKENMAP!!
 ***********************************************************************
 */
static tokenmap_t * CTokenizer__tokenize( CTokenizer_t *self, const BYTE *utf8_text, CString_t **CSTR__error ) {
  tokenmap_t *tokmap = NULL;

  SYNCHRONIZE_ON( self->_lock ) {
    XTRY {
      BYTE *token;
      CTokenizer__load( self, utf8_text );
      if( (self->_tokmap = __extend_tokenmap_nolock( self->_tokmap )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x151 );
      }
      tokmap = self->_tokmap;
      // Run tokenization and populate tokenmap with all tokens from input
      while( (token = __next_token_nolock( self, NULL )) != NULL && token[0] != '\0' );
      if( !token ) {
        if( CSTR__error ) {
          int64_t offset = self->_irp - self->_input;
          *CSTR__error = CStringNewFormat( "invalid utf-8 sequence at offset %lld ('\\x%02x')", offset, *self->_irp );
        }
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x152 );
      }
      // Store the complete input in the tokenmap
      uint32_t sz = tokmap->slen + 1;
      if( (tokmap->surface = malloc( sz )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x153 );
      }
      memcpy( tokmap->surface, self->_input, sz );
    }
    XCATCH( errcode ) {
      __delete_tokenmap( &self->_tokmap );
      tokmap = NULL;
    }
    XFINALLY {
      // Steal the internal tokenmap and prepare for token consumption by caller
      if( tokmap ) {
        self->_tokmap = NULL;
        tokmap->tcur = tokmap->tok;
        tokmap->dfin = tokmap->dcur;
        tokmap->dcur = tokmap->data;
      }
    }
  } RELEASE;

  return tokmap;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int32_t CTokenizer__count( const CTokenizer_t *self, tokenmap_t *tokmap ) {
  return tokmap->ntok;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int32_t CTokenizer__remain( const CTokenizer_t *self, tokenmap_t *tokmap ) {
  return (int32_t)(tokmap->tend - tokmap->tcur);
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void CTokenizer__delete_tokenmap( CTokenizer_t *self, tokenmap_t **tokenmap ) {
  __delete_tokenmap( tokenmap );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static const BYTE * __peek_next_tokenmap_token( const tokenmap_t *tokmap, tokinfo_t *tokinfo ) {
  const BYTE *token = NULL;
  const tokinfo_t *ptok = tokmap->tcur;
  if( ptok < tokmap->tend ) {
    token = tokmap->dcur;
    if( tokinfo ) {
      *tokinfo = *ptok;
    }
  }
  return token;
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static const BYTE * __consume_token_from_tokenmap( tokenmap_t *tokmap, tokinfo_t *tokinfo ) {

  const BYTE *token = NULL;
  const tokinfo_t *ptok = tokmap->tcur;

  if( ptok < tokmap->tend ) {
    token = tokmap->dcur;
    tokmap->dcur += ptok->len + 1;
    if( tokinfo ) {
      *tokinfo = *ptok;
    }
    tokmap->tcur++;
  }

  return token;

}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
__inline static int32_t __rewind_tokenmap( tokenmap_t *tokmap, int32_t n_tokens ) {

  // Full reset
  if( n_tokens < 0 || n_tokens > tokmap->ntok ) {
    tokmap->tcur = tokmap->tok;
    tokmap->dcur = tokmap->data;
    return tokmap->ntok;
  }
  // Partial rewind
  else {
    int32_t n = n_tokens;
    while( n-- > 0 ) {
      tokmap->tcur--;
      tokmap->dcur -= tokmap->tcur->len+1;
    }
    return n_tokens;
  }
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static const BYTE * CTokenizer__peek_token( const CTokenizer_t *self, const tokenmap_t *tokmap ) {
  return __peek_next_tokenmap_token( tokmap, NULL );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static const BYTE * CTokenizer__peek_token_and_info( const CTokenizer_t *self, const tokenmap_t *tokmap, tokinfo_t *tokinfo ) {
  return __peek_next_tokenmap_token( tokmap, tokinfo );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static const BYTE * CTokenizer__get_token( const CTokenizer_t *self, tokenmap_t *tokmap ) {
  return __consume_token_from_tokenmap( tokmap, NULL );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static const BYTE * CTokenizer__get_token_and_info( const CTokenizer_t *self, tokenmap_t *tokmap, tokinfo_t *tokinfo ) {
  return __consume_token_from_tokenmap( tokmap, tokinfo );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int32_t CTokenizer__rewind( const CTokenizer_t *self, tokenmap_t *tokmap ) {
  return __rewind_tokenmap( tokmap, -1 );
}



/***********************************************************************
 * 
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int32_t CTokenizer__unget( const CTokenizer_t *self, tokenmap_t *tokmap ) {
  return __rewind_tokenmap( tokmap, 1 );
}
