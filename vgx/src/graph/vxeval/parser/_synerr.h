/*
###################################################
#
# File:   _synerr.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_PARSER_SYNERR_H
#define _VGX_VXEVAL_PARSER_SYNERR_H


static void *  __synerr__syntax_error( __tokenizer_context *tokenizer, const char *message );
static void *  __synerr__internal_error( __tokenizer_context *tokenizer, const char *message );




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void * __synerr__syntax_error( __tokenizer_context *tokenizer, const char *message ) {
  if( tokenizer->tokmap ) {
#define __sz_context 20LL
    tokenmap_t *tokmap = tokenizer->tokmap;
    tokinfo_t current = tokenizer->tokinfo;
    int soffset = current.soffset;
    int sz_data = tokmap->slen;
    const char *surface = (char*)tokmap->surface;

    char errtoken[32] = {0};
    char before[32] = {0};
    char after[32] = {0};
 
    // errtoken
    strncpy( errtoken, surface+soffset, minimum_value( current.len, 31 ) );

    // before
    if( soffset < __sz_context ) {
      strncpy( before, surface, soffset );
    }
    else {
      const char *head = surface + (soffset - __sz_context);
      strcpy( before, "..." );
      strncpy( before+3, head, __sz_context );
    }

    // after
    int tail_offset = soffset; // + sz_current;
    const char *tail = surface + tail_offset;
    if( tail_offset + __sz_context >= sz_data ) {
      int n = sz_data - tail_offset; 
      strncpy( after, tail, n );
    }
    else {
      strncpy( after, tail, __sz_context );
      strcpy( after + __sz_context, "..." );
    }

    __format_error_string( tokenizer->CSTR__error, "Syntax error [ %s ] at pos=%d ('%s'): %s<<<ERROR>>>%s", message, soffset+1, errtoken, before, after );
  }
  else {
    __format_error_string( tokenizer->CSTR__error, "Syntax error [ %s ]", message );
  }
  tokenizer->nerr++;
  tokenizer->hasnext = false;
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void * __synerr__internal_error( __tokenizer_context *tokenizer, const char *message ) {
  if( tokenizer->token ) {
    __format_error_string( tokenizer->CSTR__error, "<INTERNAL PARSER ERROR AT TOKEN: '%s'> : %s", tokenizer->token, message );
  }
  else {
    __format_error_string( tokenizer->CSTR__error, "<INTERNAL PARSER ERROR> : %s", message );
  }
  tokenizer->nerr++;
  tokenizer->hasnext = false;
  return NULL;
}





#endif
