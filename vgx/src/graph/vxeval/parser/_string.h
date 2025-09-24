/*
###################################################
#
# File:   _string.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_PARSER_STRING_H
#define _VGX_VXEVAL_PARSER_STRING_H


static int          __string__initialize_constants( void );
static void         __string__destroy_constants( void );
static CString_t *  __parser__get_raw_cstring( vgx_Graph_t *graph, __tokenizer_context *tokenizer );



DLL_HIDDEN const CString_t *_vxeval_modifier_strings[16] = {0};
DLL_HIDDEN const CString_t *_vxeval_arcdir_strings[4] = {0};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __string__initialize_constants( void ) {
  int ret = 0;
  for( uint8_t i=0; i<16; i++ ) {
    vgx_predicator_mod_t mod = {
      .bits = i
    };
    if( (_vxeval_modifier_strings[ i ] = CStringNew( _vgx_modifier_as_string( mod ) )) == NULL ) {
      ret = -1;
    }
  }
  for( unsigned dir=0; dir<4; dir++ ) {
    if( (_vxeval_arcdir_strings[ dir ] = CStringNew( __reverse_arcdir_map[ dir ] )) == NULL ) {
      ret = -1;
    }
  }
  if( ret < 0 ) {
    __string__destroy_constants();
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __string__destroy_constants( void ) {
  for( unsigned mod=0; mod<16; mod++ ) {
    iString.Discard( (CString_t**)&_vxeval_modifier_strings[ mod ] );
  }
  for( unsigned dir=0; dir<4; dir++ ) {
    iString.Discard( (CString_t**)&_vxeval_arcdir_strings[ dir ] );
  }
}



/*******************************************************************//**
 * 
 * NOTE: Caller owns the returned string!
 *
 ***********************************************************************
 */
static CString_t * __parser__get_raw_cstring( vgx_Graph_t *graph, __tokenizer_context *tokenizer ) {
  CString_t *CSTR__value = NULL;
  bool bytes = tokenizer->initial == 'b';
  if( bytes ) {
    __tokenizer__next_token( tokenizer );
  }

  char quote = tokenizer->initial;
  uint32_t start = tokenizer->tokinfo.soffset + 1;

  CString_constructor_args_t cargs = {
    .string      = (const char*)tokenizer->tokmap->surface + start,
    .len         = 0,
    .ucsz        = 0,
    .format      = NULL,
    .format_args = NULL,
    .alloc       = graph->ephemeral_string_allocator_context
  };

  // Get token(s) until closing quote to compute surface span for the string
  while( __tokenizer__next_token( tokenizer ) != NULL ) {
    // End of string
    if( tokenizer->tokinfo.len == 1 && tokenizer->initial == quote ) {
      // Current token is the end quote, compute actual length of string within the two quotes
      uint32_t end = tokenizer->tokinfo.soffset;
      if( (cargs.len = end - start) > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
        return __synerr__syntax_error( tokenizer, "string literal too long" );
      }
      const char *data = (const char*)tokenizer->tokmap->surface + start;
      size_t n_bytes = 0;
      if( (cargs.string = (const char*)__tokenizer__get_string_bytes( tokenizer, data, cargs.len, &n_bytes, quote )) != NULL ) {
        cargs.len = (uint32_t)n_bytes;
        if( (CSTR__value = COMLIB_OBJECT_NEW( CString_t, NULL, &cargs )) != NULL ) {
          if( bytes ) {
            CStringAttributes( CSTR__value ) = CSTRING_ATTR_BYTES;
          }
        }
        else {
          __synerr__syntax_error( tokenizer, "unexpected internal error" );
        }
        free( (void*)cargs.string );
        cargs.string = NULL;
      }

      return CSTR__value;
    }
  }

  return __synerr__syntax_error( tokenizer, "unexpected end of expression" );
}




#endif
