/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _tokenizer.h
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

#ifndef _VGX_VXEVAL_PARSER_TOKENIZER_H
#define _VGX_VXEVAL_PARSER_TOKENIZER_H


#include "_synerr.h"


static CTokenizer_t *           __tokenizer__new( void );
static CTokenizer_t *           __tokenizer__new_normalizer( void );
static void                     __tokenizer__delete( CTokenizer_t **tokenizer );
static __tokenizer_context *    __tokenizer__new_context( CTokenizer_t *engine, const char *expression, CString_t **CSTR__error );
static void                     __tokenizer__delete_context( __tokenizer_context **tokenizer );
static int32_t                  __tokenizer__back_token( __tokenizer_context *tokenizer );
static const char *             __tokenizer__next_token( __tokenizer_context *tokenizer );
static CString_t *              __tokenizer__get_tokenized_cstring( vgx_Graph_t *graph, __tokenizer_context *tokenizer, const char quote );
static BYTE *                   __tokenizer__get_string_bytes( __tokenizer_context *tokenizer, const char *raw, size_t sz_raw, size_t *n_bytes, char term );
static const char *             __tokenizer__skip_ignorable( const char *p );

DLL_HIDDEN CTokenizer_t *_vxeval_parser__tokenizer = NULL;
DLL_HIDDEN CTokenizer_t *_vxeval_parser__normalizer = NULL;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static CTokenizer_t * __tokenizer__new( void ) {

  unicode_codepoint_t ucKEEP[] = {
    '`',  '@',  '#',  '$',
    '_',  '.',
    0
  };

  unicode_codepoint_t ucSKIP[] = {
    '\n', '\t', 0
  };

  unicode_codepoint_t ucSPLIT[] = { 
    '~',  '!',  '%',  '^',
    '&',  '*',  '(',  ')',
    '-',  '+',  '=',  '[',
    '{',  '}',  ']',  '\\',
    '|',  ':',  ';',  '"',
    '\'', '<',  '>',  ',',
    '/',  '?',
    0
  };

  unicode_codepoint_t ucIGNORE[] = {
    0
  };

  unicode_codepoint_t ucCOMBINE[] = {
    0
  };


  CTokenizer_constructor_args_t tokargs = {
    .keep_digits        = 1,
    .normalize_accents  = 0,
    .lowercase          = 0,
    .keepsplit          = 1,
    .strict_utf8        = 1,
    .overrides  = {
      .keep             = ucKEEP,
      .skip             = ucSKIP,
      .split            = ucSPLIT,
      .ignore           = ucIGNORE,
      .combine          = ucCOMBINE
    }
  };


  CTokenizer_t *tokenizer = COMLIB_OBJECT_NEW(  CTokenizer_t, "Expression Tokenizer", &tokargs );
  return tokenizer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static CTokenizer_t * __tokenizer__new_normalizer( void ) {

  unicode_codepoint_t ucKEEP[] ={
    '#',  '$', '@', '_',
    0
  };

  unicode_codepoint_t ucSKIP[] ={
    '\n', '\t', 0
  };

//  '!' '"' '#' '$' '%' '&' '\'' '(' ')' '*' '+' ',' '-' '.' '/' 
//  ':' ';' '<' '=' '>' '?'
//  @ A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
//  '[' '\\' ']' '^' '_' '`'
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  '{' '|' ''} '~'

  unicode_codepoint_t ucSPLIT[] ={
    '!',  '"',  
    '%',  '&',  '\'', '(',
    ')',  '*',  '+',  ',',
    '-',  '.',  '/',  ':',
    ';',  '<',  '=',  '>',
    '?',  '[',  '\\', ']',
    '^',        '`',  '{',
    '|',  '}',  '~',
    0
  };

  unicode_codepoint_t ucIGNORE[] ={
    0
  };

  unicode_codepoint_t ucCOMBINE[] ={
    0
  };


  CTokenizer_constructor_args_t tokargs = {
    .keep_digits        = 1,
    .normalize_accents  = 1,
    .lowercase          = 1,
    .keepsplit          = 0,
    .strict_utf8        = 1,
    .overrides  = {
      .keep             = ucKEEP,
      .skip             = ucSKIP,
      .split            = ucSPLIT,
      .ignore           = ucIGNORE,
      .combine          = ucCOMBINE
    }
  };


  CTokenizer_t *normalizer = COMLIB_OBJECT_NEW(  CTokenizer_t, "String Normalizer", &tokargs );
  return normalizer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __tokenizer__delete( CTokenizer_t **tokenizer ) {
  if( tokenizer && *tokenizer ) {
    COMLIB_OBJECT_DESTROY( *tokenizer );
    *tokenizer = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static __tokenizer_context * __tokenizer__new_context( CTokenizer_t *engine, const char *expression, CString_t **CSTR__error ) {
  // Allocate
  __tokenizer_context *tokenizer = calloc( 1, sizeof( __tokenizer_context ) );
  if( tokenizer ) {
    // Initialize
    tokenizer->engine = engine;
    tokenizer->expression = expression;
    tokenizer->lenexpr = (int)strlen( expression );
    tokenizer->escape = false;
    tokenizer->debug = false;
    tokenizer->CSTR__error = CSTR__error;
    // Run core tokenization
    if( (tokenizer->tokmap = CALLABLE( tokenizer->engine )->Tokenize( tokenizer->engine, (const BYTE*)tokenizer->expression, tokenizer->CSTR__error )) == NULL ) {
      if( tokenizer->CSTR__error && *tokenizer->CSTR__error ) {
        __synerr__syntax_error( tokenizer, CStringValue( *tokenizer->CSTR__error ) );
      }
      __tokenizer__delete_context( &tokenizer );
    }
    else if( (tokenizer->ntokens = CALLABLE( tokenizer->engine )->Count( tokenizer->engine, tokenizer->tokmap )) > 0 ) {
      tokenizer->hasnext = true;
    }
  }
  return tokenizer;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __tokenizer__delete_context( __tokenizer_context **tokenizer ) {
  if( tokenizer && *tokenizer ) {
    if( (*tokenizer)->tokmap ) {
      CALLABLE( (*tokenizer)->engine )->DeleteTokenmap( (*tokenizer)->engine, &(*tokenizer)->tokmap );
    }
    free( *tokenizer );
    *tokenizer = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */ 
static int32_t __tokenizer__back_token( __tokenizer_context *tokenizer ) {
  return CALLABLE( tokenizer->engine )->Unget( tokenizer->engine, tokenizer->tokmap );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */ 
static bool __tokenizer__is_next_token( __tokenizer_context *tokenizer, const char *str ) {
  tokinfo_t nextinfo;
  const char *peek = (char*)CALLABLE( tokenizer->engine )->PeekTokenAndInfo( tokenizer->engine, tokenizer->tokmap, &nextinfo );
  // No next token or not expected length or no match
  if( peek == NULL || nextinfo.len != strlen(str) || memcmp(str, peek, nextinfo.len) ) {
    return false;
  }
  // Match
  return true;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */ 
static bool __tokenizer__immediate_next_chars( __tokenizer_context *tokenizer, char chars[] ) {
  tokinfo_t nextinfo;
  const char *peek = (char*)CALLABLE( tokenizer->engine )->PeekTokenAndInfo( tokenizer->engine, tokenizer->tokmap, &nextinfo );
  // No next token or not single char or not exactly one character after current position
  if( peek == NULL || nextinfo.len != 1 || nextinfo.soffset != tokenizer->tokinfo.soffset + 1 ) {
    return false;
  }

  // Next token must match one of the supplied chars
  const char *p = chars;
  while( *p != '\0' ) {
    if( *peek == *p ) {
      return true;
    }
    ++p;
  }

  // No match
  return false;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */ 
static const char * __tokenizer__next_token( __tokenizer_context *tokenizer ) {
  uint16_t lin = 0;
  while( tokenizer->hasnext ) {
    //tokenizer->tokmap->tcur

    if( (tokenizer->token = (const char *)CALLABLE( tokenizer->engine )->GetTokenAndInfo( tokenizer->engine, tokenizer->tokmap, &tokenizer->tokinfo )) != NULL ) {
      tokenizer->initial = *tokenizer->token;
      // Interpret operator symbols (single or multiple)
      switch( tokenizer->initial ) { 
      case '<':
      case '>':
      case '=':
      case '!':
      case '?':
      case ':':
      case '*':
      case '|':
      case '&':
      case '/':
        // First character of operator - reset symbol
        if( tokenizer->tokinfo.flw == TOKEN_FLOW_SPLIT && tokenizer->psymbol == NULL ) {
          lin = tokenizer->tokinfo.lin;
          tokenizer->op.data = 0;
          tokenizer->psymbol = tokenizer->op.symbol;
          tokenizer->oplen = 0;
        }
        // Append operator character
        if( tokenizer->psymbol ) {
          *tokenizer->psymbol++ = tokenizer->initial;
          if( ++tokenizer->oplen < sizeof( tokenizer->op.symbol ) ) {
            if( tokenizer->oplen == 2 ) {
              const char c0 = tokenizer->op.symbol[0];
              const char c1 = tokenizer->op.symbol[1];
              // Max length of symbol token starting with '/' or equal to '*/' is 2
              if( c0 == '/' || ( c0 == '*' && c1 == '/' ) ) {
                break;
              }
            }

            // Detect difference between === and == =, etc.
            if( tokenizer->tokinfo.flw == TOKEN_FLOW_BREAK ) {
              break;
            }
            else {
              continue;
            }
          }
          // Ignore oversized symbol token in escape mode
          else if( tokenizer->escape ) {
            if( tokenizer->tokinfo.flw == TOKEN_FLOW_SPLIT ) {
              __tokenizer__back_token( tokenizer );
              break;
            }
            // Shift data, keep only last seven characters
            --tokenizer->oplen;
            tokenizer->op.data >>= 8;
            --tokenizer->psymbol;
            continue;
          }
          else {
            __synerr__syntax_error( tokenizer, "invalid operator" );
            tokenizer->psymbol = NULL;
            return NULL;
          }
        }
      // Not an operator symbol
      default:
        // Operator symbol was built before current token. Put back current token so we can evaluate operator first.
        if( tokenizer->psymbol ) {
          __tokenizer__back_token( tokenizer );
        }
      }
    }
    // No more tokens
    else {
      // Terminate parsing
      tokenizer->hasnext = false;
      // Exit loop if no operator
      if( !tokenizer->psymbol ) {
        continue;
      }
    }

    // An operator has been built, override the token with this operator
    if( tokenizer->psymbol ) {
      tokenizer->token = tokenizer->op.symbol;
      tokenizer->initial = *tokenizer->op.symbol;
      tokenizer->psymbol = NULL;
      tokenizer->tokinfo.len = tokenizer->oplen;
      tokenizer->tokinfo.typ = STRING_TYPE_SPLIT;
      tokenizer->tokinfo.flw = TOKEN_FLOW_BREAK;
      tokenizer->tokinfo.lin = lin;
    }
    
    if( tokenizer->debug ) {
      printf( "tokenizer: '%s'\n", tokenizer->token );
    }
    
    return tokenizer->token;
  }

  return NULL;
}



/*******************************************************************//**
 * 
 * NOTE: Caller owns the returned string!
 *
 * WARNING: THIS RETURNS A TOKENIZED VERSION OF THE PARSED STRING! NOT ALWAYS WHAT WE WANT.
 *
 ***********************************************************************
 */
static CString_t * __tokenizer__get_tokenized_cstring( vgx_Graph_t *graph, __tokenizer_context *tokenizer, const char quote ) {
  CString_t *CSTR__value = NULL;
#define MAX_STRING_SIZE 511
  char buf[ MAX_STRING_SIZE + 1];
  buf[ MAX_STRING_SIZE ] = '\0';
  char *p = buf;
  int remain = MAX_STRING_SIZE;

  // Get token(s) as tokenized string value until closing quote
  while( __tokenizer__next_token( tokenizer ) != NULL ) {
    int n = tokenizer->tokinfo.len;
    char c = tokenizer->initial;
    // End of string
    if( n == 1 && c == quote ) {
      *p = '\0';
      if( (CSTR__value = iString.New( graph->ephemeral_string_allocator_context, buf )) != NULL ) {
        return CSTR__value;
      }
      else {
        return __synerr__syntax_error( tokenizer, "unexpected internal error" );
      }
    }
    // Append token to buffer
    else if( remain > 0 ) {
      // Add space if appropriate
      if( p > buf ) {
        // Don't treat +, -, * as SPLIT, they become KEEP in this context. (Suffix only for *)
        if( !( *(p-1) == '-' || *(p-1) == '+' || c == '-' || c == '+' || c == '*') ) {
          *p++ = ' ';
          --remain;
        }
      }
      // Append token
      if( remain > n ) {
        strncpy( p, tokenizer->token, n );
        p += n;
        remain -= n;
        continue;
      }
    }

    return __synerr__syntax_error( tokenizer, "string too long" );
  }

  return __synerr__syntax_error( tokenizer, "unexpected end of expression" );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static BYTE * __tokenizer__get_string_bytes( __tokenizer_context *tokenizer, const char *raw, size_t sz_raw, size_t *n_bytes, char term ) {
  typedef enum e_State {
    EXPECT_CHAR,
    EXPECT_ESC_SEQ,
    EXPECT_HEX1,
    EXPECT_HEX2
  } State;
  State state = EXPECT_CHAR;
  BYTE *string = NULL;
  
  XTRY {
    if( (string = calloc( sz_raw + 1, sizeof(BYTE) )) == NULL ) {
      __synerr__syntax_error( tokenizer, "out of memory" );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    BYTE *dest = string;
    BYTE *end = dest + sz_raw;
    BYTE d1=0, d2=0;
    char c;
    const char *src = raw;
    while( (c = *src++) != term ) {
      switch( state ) {
      case EXPECT_CHAR:
        if( c == '\\' ) {
          state = EXPECT_ESC_SEQ;
          continue;
        }
        goto append;
      case EXPECT_ESC_SEQ:
        // Literal backslash
        if( c == '\\' ) {
          state = EXPECT_CHAR;
          goto append;
        }
        // Next parse escape sequence
        else if( c == 'x' ) {
          state = EXPECT_HEX1;
          continue;
        }
        break;
      case EXPECT_HEX1:
        if( hex_digit_byte( c, &d1 ) ) {
          state = EXPECT_HEX2;
          continue;
        }
        break;
      case EXPECT_HEX2:
        if( hex_digit_byte( c, &d2 ) ) {
          c = (d1<<4) + d2;
          state = EXPECT_CHAR;
          goto append;
        }
        break;
      }

      __synerr__syntax_error( tokenizer, "invalid escape sequence" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );

append:
      if( dest < end ) {
        *dest++ = c;
      }
    }
    // ok
    if( state == EXPECT_CHAR ) {
      *n_bytes = dest - string;
      *dest++ = '\0';
    }
    else {
      __synerr__syntax_error( tokenizer, "unexpected EOL" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }
  }
  XCATCH( errcode ) {
    if( string ) {
      free( string );
      string = NULL;
    }
  }
  XFINALLY {
  }

  return string;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char * __tokenizer__skip_ignorable( const char *p ) {
  // Skip spaces
  while( isspace(*p) ) {
    ++p;
  }
  // Skip comment(s) followed by space(s)
  while( *p == '/' && *(p+1) == '*' ) {
    p += 2;
    char c;
    while( (c=*p++) != '\0' ) {
      if( c=='*' && *p =='/' ) {
        ++p;
        break;
      }
      else if( *p == '\0' ) {
        return NULL;
      }
    }
    if( *p == '\0' ) {
      return NULL;
    }
    else {
      while( isspace(*p) ) {
        ++p;
      }
    }
  }
  return p;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int _vxeval_parser__init_tokenizer( void ) {
  if( _vxeval_parser__tokenizer == NULL ) {
    if( (_vxeval_parser__tokenizer = __tokenizer__new()) == NULL ) {
      return -1;
    }
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int _vxeval_parser__del_tokenizer( void ) {
  __tokenizer__delete( &_vxeval_parser__tokenizer );
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int _vxeval_parser__init_normalizer( void ) {
  if( _vxeval_parser__normalizer == NULL ) {
    if( (_vxeval_parser__normalizer = __tokenizer__new_normalizer()) == NULL ) {
      return -1;
    }
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int _vxeval_parser__del_normalizer( void ) {
  __tokenizer__delete( &_vxeval_parser__normalizer );
  return 0;
}



#endif
