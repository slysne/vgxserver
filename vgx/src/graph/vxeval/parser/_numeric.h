/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _numeric.h
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

#ifndef _VGX_VXEVAL_PARSER_NUMERIC_H
#define _VGX_VXEVAL_PARSER_NUMERIC_H

#include "_tokenizer.h"



static __rpn_operation *  __numeric__decimal_int( const char *token, vgx_EvalStackItem_t *stackitem );
static __rpn_operation *  __numeric__binhex_int( const char *token, vgx_EvalStackItem_t *stackitem );
static __rpn_operation *  __numeric__real( __tokenizer_context *tokenizer, vgx_EvalStackItem_t *stackitem );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static __rpn_operation * __numeric__decimal_int( const char *token, vgx_EvalStackItem_t *stackitem ) {
  const char *p = token;
  unsigned c, u;
  // Validate and compute value
  if( stackitem ) {
    int64_t val = 0;
    int64_t sign = 1;
    if( *p == '-' ) {
      sign = -1;
      p++;
    }
    while( (c = *p++) != 0 ) {
      if( (u = c-'0') < 10 ) { // 0 - 9
        val = 10*val + u;
      }
      else {
        return NULL;
      }
    }
    stackitem->integer = sign * val;
    stackitem->type = STACK_ITEM_TYPE_INTEGER;
  }
  // Just validate
  else {
    while( (c = *p++) != 0 ) {
      if( (u = c-'0') > 0 ) { // 0 - 9
        return NULL;
      }
    }
  }
  return &RpnPushConstantInt; // token is a valid integer
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static __rpn_operation * __numeric__binhex_int( const char *token, vgx_EvalStackItem_t *stackitem ) {
  const char *p = token;
  char c;
  BYTE v;
  int64_t sign = 1;
  int64_t val = 0;
  __rpn_operation *op = &RpnNoop;

  typedef enum __parser_state {
    EXPECT_0_or_neg,
    EXPECT_0,
    EXPECT_B_or_X,
    EXPECT_bin,
    EXPECT_hex
  } parser_state;
  
  parser_state state = EXPECT_0_or_neg;

  while( (c = *p) != 0 ) {
    ++p;
    switch( state ) {
    case EXPECT_0_or_neg:
      if( c == '0' ) {
        state = EXPECT_B_or_X;
      }
      else if( c == '-' ) {
        sign = -1;
        state = EXPECT_0;
      }
      else {
        return NULL;
      }
      break;
    case EXPECT_0:
      if( c == '0' ) {
        state = EXPECT_B_or_X;
      }
      else {
        return NULL;
      }
      break;
    case EXPECT_B_or_X:
      if( (c&0xDF) == 'X' ) {
        state = EXPECT_hex;
        stackitem->type = STACK_ITEM_TYPE_INTEGER;
        op = &RpnPushConstantInt;
      }
      else if( (c&0xDF) == 'B' ) {
        state = EXPECT_bin;
        stackitem->type = STACK_ITEM_TYPE_BITVECTOR;
        op = &RpnPushConstantBitvec;
      }
      else {
        return NULL;
      }
      break;
    case EXPECT_hex:
      if( hex_digit_byte( c, &v ) ) {
        val = (val<<4) + v;
        break;
      }
      else {
        return NULL;
      }
    case EXPECT_bin:
      if( (c&0xFE) == '0' ) { // 0 or 1
        val = (val<<1) + (c&1);
        break;
      }
      else {
        return NULL;
      }
    default:
      return NULL;
    }
  }
  if( p - token < 3 ) {
    return NULL;
  }
  stackitem->integer = sign * val;
  return op;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static __rpn_operation * __numeric__real( __tokenizer_context *tokenizer, vgx_EvalStackItem_t *stackitem ) {
  int n1 = tokenizer->tokinfo.len;
  const char *number = tokenizer->token;
  char *buf = NULL;
  switch( number[ n1-1 ] ) {
  // Digits end with e or E
  case 'e':
  case 'E':
    // Get the sign and the exponent
    if( __tokenizer__next_token( tokenizer ) != NULL ) {
      const char *digits = number;
      const char *sign = tokenizer->token;
      const char *exponent = NULL;
      if( tokenizer->tokinfo.len == 1 ) {
        switch( *sign ) {
        // Sign
        case '-':
        case '+':
          // Get the exponent, place in buffer, and replace token with buffer
          if( (exponent = __tokenizer__next_token( tokenizer )) != NULL ) {
            int n3 = tokenizer->tokinfo.len;
            int strsz = n1 + 1 + n3;
            // Exponent is a digit
            if( tokenizer->tokinfo.typ == STRING_TYPE_DIGIT ) {
              if( (buf = calloc( strsz + 1, 1 )) != NULL ) {
                char *p = buf;
                strncpy( p, digits, n1 );
                p += n1;
                strncpy( p++, sign, 1 );
                strncpy( p, exponent, n3 );
                buf[ strsz ] = '\0';
                number = buf;

              }
              else {
                return __synerr__syntax_error( tokenizer, "unexpected internal error" );
              }
            }
            // Exponent token is not a digit, the sign is not unary, roll back tokens
            else {
              // Put back the token that was not an exponent
              __tokenizer__back_token( tokenizer );
              // Put back the sign with will now be treated as binary infix instead
              __tokenizer__back_token( tokenizer );
            }
          }
          else {
            return __synerr__syntax_error( tokenizer, "unexpected end of expression" );
          }
          break;
        // Sign token not part of the number, put back token
        default:
          __tokenizer__back_token( tokenizer );
          break;
        }
      }
    }
  // Token is already a full representation ready to be converted
  default:
    break;
  }
  stackitem->real = atof( number );
  if( stackitem->real < INFINITY ) {
    stackitem->type = STACK_ITEM_TYPE_REAL;
  }
  else {
    SET_NAN( stackitem );
  }
  free( buf );
  return &RpnPushConstantReal; // ok!
}




#endif
