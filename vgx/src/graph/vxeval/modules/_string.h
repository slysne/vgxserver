/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _string.h
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

#ifndef _VGX_VXEVAL_MODULES_STRING_H
#define _VGX_VXEVAL_MODULES_STRING_H


/*******************************************************************//**
 *
 ***********************************************************************
 */

static void __eval_unary_cast_str( vgx_Evaluator_t *self );
static void __eval_string_lower( vgx_Evaluator_t *self );
static void __eval_string_upper( vgx_Evaluator_t *self );
static void __eval_string_normalize( vgx_Evaluator_t *self );
static void __eval_string_join( vgx_Evaluator_t *self );
static void __eval_string_replace( vgx_Evaluator_t *self );
static void __eval_string_slice( vgx_Evaluator_t *self );
static void __eval_string_prefix( vgx_Evaluator_t *self );
static void __eval_string_index( vgx_Evaluator_t *self );
static void __eval_string_strcmp( vgx_Evaluator_t *self );
static void __eval_string_strcasecmp( vgx_Evaluator_t *self );

static void __eval_variadic_string_all( vgx_Evaluator_t *self );
static void __eval_variadic_string_any( vgx_Evaluator_t *self );
static void __eval_variadic_string_all_icase( vgx_Evaluator_t *self );
static void __eval_variadic_string_any_icase( vgx_Evaluator_t *self );

static void __eval_string_contains( vgx_Evaluator_t *self );
static void __eval_string_contains_icase( vgx_Evaluator_t *self );
static void __eval_string_startswith( vgx_Evaluator_t *self );
static void __eval_string_startswith_icase( vgx_Evaluator_t *self );
static void __eval_string_endswith( vgx_Evaluator_t *self );
static void __eval_string_endswith_icase( vgx_Evaluator_t *self );
static void __eval_string_indexof( vgx_Evaluator_t *self );
static void __eval_string_indexof_icase( vgx_Evaluator_t *self );

static void __eval_string_strftime( vgx_Evaluator_t *self );
static void __eval_string_modtostr( vgx_Evaluator_t *self );
static void __eval_string_dirtostr( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const CString_t *__new_scoped_cstring( vgx_Evaluator_t *self, const char *str ) {

  vgx_EvalStackItem_t scoped = {
    .type = STACK_ITEM_TYPE_CSTRING,
    .CSTR__str = NewEphemeralCString( self->graph, str )
  };

  if( scoped.CSTR__str == NULL || iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
    return NULL;
  }

  return scoped.CSTR__str;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t * __cast_str( vgx_Evaluator_t *self, vgx_EvalStackItem_t *item ) {
  if( item->type == STACK_ITEM_TYPE_CSTRING ) {
    return item;
  }

  object_allocator_context_t *alloc = self->graph->ephemeral_string_allocator_context;
  CString_t *CSTR__tmp = NULL;
  char idbuf[33];
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    CSTR__tmp = CStringNewFormatAlloc( alloc, "%lld", item->integer );
    break;
  case STACK_ITEM_TYPE_REAL:
    CSTR__tmp = CStringNewFormatAlloc( alloc, "%#g", item->real );
    break;
  case STACK_ITEM_TYPE_NAN:
    CSTR__tmp = NewEphemeralCStringLen( self->graph, "nan", 3, 0 );
    break;
  case STACK_ITEM_TYPE_NONE:
    CSTR__tmp = NewEphemeralCStringLen( self->graph, "null", 4, 0 );
    break;
  case STACK_ITEM_TYPE_KEYVAL:
    CSTR__tmp = CStringNewFormatAlloc( alloc, "{%d:%#g}", item->keyval.key, item->keyval.value );
    break;
  case STACK_ITEM_TYPE_VERTEX:
    CSTR__tmp = NewEphemeralCStringLen( self->graph, idtostr( idbuf, __vertex_internalid( item->vertex )), 32, 0 );
    break;
  case STACK_ITEM_TYPE_VERTEXID:
    if( item->vertexid->CSTR__idstr ) {
      CSTR__tmp = CloneCStringEphemeral( self->graph, item->vertexid->CSTR__idstr );
    }
    else {
      CSTR__tmp = NewEphemeralCString( self->graph, item->vertexid->idprefix.data );
    }
    break;
  case STACK_ITEM_TYPE_VECTOR:
    CSTR__tmp = CStringNewFormatAlloc( alloc, "<vector @ %llp>", item->vector );
    break;
  default:
    SET_NONE( item );
    return NULL;
  }

  if( CSTR__tmp == NULL ) {
    SET_NONE( item );
    return NULL;
  }

  vgx_EvalStackItem_t scoped = {
    .type = STACK_ITEM_TYPE_CSTRING,
    .CSTR__str = CSTR__tmp
  };

  if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
    SET_NONE( item );
    return NULL;
  }

  *item = scoped;
  return item;

}



/*******************************************************************//**
 * str( x ) -> str
 ***********************************************************************
 */
static void __eval_unary_cast_str( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  __cast_str( self, px );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const char * __stackitem_as_str( vgx_EvalStackItem_t *px, int32_t *sz_str, CString_attr *attr ) {
  if( px ) {
    int32_t __sz; // dummy
    if( sz_str == NULL ) {
      sz_str = &__sz;
    }
    switch( px->type ) {
    case STACK_ITEM_TYPE_CSTRING:
      *sz_str = CStringLength( px->CSTR__str );
      *attr = CStringAttributes( px->CSTR__str );
      return CStringValue( px->CSTR__str );
    case STACK_ITEM_TYPE_VERTEX:
      // Cast to vertex id
      px->vertexid = CALLABLE( px->vertex )->Identifier( px->vertex );
      px->type = STACK_ITEM_TYPE_VERTEXID;
      /* FALLTHRU */
    case STACK_ITEM_TYPE_VERTEXID:
      *attr = CSTRING_ATTR_NONE;
      if( px->vertexid->CSTR__idstr ) {
        *sz_str = CStringLength( px->vertexid->CSTR__idstr );
        return CStringValue( px->vertexid->CSTR__idstr );
      }
      else {
        *sz_str = (int32_t)strlen( px->vertexid->idprefix.data );
        return px->vertexid->idprefix.data;
      }
    default:
      return NULL;
    }
  }
  return NULL;
}



/*******************************************************************//**
 * lower( x ) -> str
 ***********************************************************************
 */
static void __eval_string_lower( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  int32_t sz_str = 0;
  CString_attr attr = CSTRING_ATTR_NONE;
  const char *str = __stackitem_as_str( px, &sz_str, &attr );

  if( str ) {
    CString_constructor_args_t args = {
      .string      = NULL,
      .len         = sz_str,
      .ucsz        = 0,
      .format      = NULL,
      .format_args = NULL,
      .alloc       = self->graph->ephemeral_string_allocator_context
    };
    CString_t *CSTR__lower = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
    if( CSTR__lower ) {
      char *data = (char*)CALLABLE( CSTR__lower )->ModifiableQwords( CSTR__lower );
      char *wp = data;
      const char *rp = str;
      const char *end = rp + sz_str;
      while( rp < end ) {
        *wp++ = (char)tolower((unsigned char)*rp++);
      }
      *wp = '\0';

      // Inherit attributes from original input
      CStringAttributes( CSTR__lower ) = attr;

      vgx_EvalStackItem_t scoped = {
        .type = STACK_ITEM_TYPE_CSTRING,
        .CSTR__str = CSTR__lower
      };

      if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
        *px = scoped; // return normalized
      }
    }
  }
}



/*******************************************************************//**
 * upper( x ) -> str
 ***********************************************************************
 */
static void __eval_string_upper( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  int32_t sz_str = 0;
  CString_attr attr = CSTRING_ATTR_NONE;
  const char *str = __stackitem_as_str( px, &sz_str, &attr );

  if( str ) {
    CString_constructor_args_t args = {
      .string      = NULL,
      .len         = sz_str,
      .ucsz        = 0,
      .format      = NULL,
      .format_args = NULL,
      .alloc       = self->graph->ephemeral_string_allocator_context
    };
    CString_t *CSTR__upper = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
    if( CSTR__upper ) {
      char *data = (char*)CALLABLE( CSTR__upper )->ModifiableQwords( CSTR__upper );
      char *wp = data;
      const char *rp = str;
      const char *end = rp + sz_str;
      while( rp < end ) {
        *wp++ = (char)toupper((unsigned char)*rp++);
      }
      *wp = '\0';

      // Inherit attributes from original input
      CStringAttributes( CSTR__upper ) = attr;

      vgx_EvalStackItem_t scoped = {
        .type = STACK_ITEM_TYPE_CSTRING,
        .CSTR__str = CSTR__upper
      };

      if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
        *px = scoped; // return normalized
      }
    }
  }
}



/*******************************************************************//**
 * normalize( x ) -> str
 ***********************************************************************
 */
static void __eval_string_normalize( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  CString_attr attr = CSTRING_ATTR_NONE;
  const char *str = __stackitem_as_str( px, NULL, &attr );

  if( str ) {
    CTokenizer_vtable_t *iTokenizer = CALLABLE( _vxeval_parser__normalizer );
    tokenmap_t *tokenmap = iTokenizer->Tokenize( _vxeval_parser__normalizer, (BYTE*)str, NULL );
    if( tokenmap != NULL ) {
      int32_t sz_data = (int32_t)(tokenmap->dfin - tokenmap->data) - 1;
      CString_constructor_args_t args = {
        .string      = NULL,
        .len         = sz_data,
        .ucsz        = 0,
        .format      = NULL,
        .format_args = NULL,
        .alloc       = self->graph->ephemeral_string_allocator_context
      };
      CString_t *CSTR__norm = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
      if( CSTR__norm ) {
        char *data = (char*)CALLABLE( CSTR__norm )->ModifiableQwords( CSTR__norm );
        char *wp = data;
        const char *rp = (const char*)tokenmap->data;
        const char *end = rp + sz_data;
        char c;
        while( rp < end ) {
          if( (c = *rp++) == '\0' ) {
            c = ' ';
          }
          *wp++ = c;
        }
        *wp = '\0';

        // Inherit attributes from original input
        CStringAttributes( CSTR__norm ) = attr;

        vgx_EvalStackItem_t scoped = {
          .type = STACK_ITEM_TYPE_CSTRING,
          .CSTR__str = CSTR__norm
        };

        if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
          *px = scoped; // return normalized
        }

      }
      iTokenizer->DeleteTokenmap( _vxeval_parser__normalizer, &tokenmap );
    }
  }
}



/*******************************************************************//**
 * join( sep,  x1, x2, ... ) -> str
 ***********************************************************************
 */
static void __eval_string_join( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  if( nargs == 0 ) {
    return;
  }

  // Number of strings to join
  int64_t n = nargs - 1;

  // The joined string
  CString_t *CSTR__join = NULL;

  // The separator string
  vgx_EvalStackItem_t *px = GET_PITEM( self ) - n;

  CString_attr attr = CSTRING_ATTR_NONE;
  if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    attr = CStringAttributes( px->CSTR__str );
  }
  else {
    __cast_str( self, px );
  }

  if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    const CString_t *CSTR__sep = px->CSTR__str;

    ++px;

    const CString_t *CSTR__list[4];
    const CString_t **p_CSTR__list;
    if( n < 4 ) {
      p_CSTR__list = CSTR__list;
    }
    else {
      p_CSTR__list = malloc( (n+1) * sizeof( CString_t* ) );
    }

    // Populate
    if( p_CSTR__list ) {
      for( int64_t i=0; i<n; i++ ) {
        if( px->type != STACK_ITEM_TYPE_CSTRING ) {
          __cast_str( self, px ); // if cast fails a null pointer is stored, which truncates the end result
        }
        p_CSTR__list[ i ] = px->CSTR__str;
        ++px;
      }
      // Terminate
      p_CSTR__list[ n ] = NULL;

      // Join
      CSTR__join = CALLABLE( CSTR__sep )->JoinAlloc( CSTR__sep, p_CSTR__list, self->graph->ephemeral_string_allocator_context );

      // Free dynamic list if used
      if( p_CSTR__list != CSTR__list ) {
        free( (void*)p_CSTR__list );
      }
    }
  }

  // Return slot
  DISCARD_ITEMS( self, n );
  px = GET_PITEM( self );

  // Put string on stack
  int64_t errpos;
  if( CSTR__join ) {
    if( !COMLIB_check_utf8( (const BYTE*)CStringValue( CSTR__join ), &errpos ) ) {
      CStringAttributes( CSTR__join ) = CSTRING_ATTR_BYTES;
    }
    else {
      CStringAttributes( CSTR__join ) = attr;
    }

    vgx_EvalStackItem_t scoped = {
      .type = STACK_ITEM_TYPE_CSTRING,
      .CSTR__str = CSTR__join
    };

    if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
      SET_NONE( px );
      return;
    }

    *px = scoped;
  }
  else {
    iString.Discard( &CSTR__join );
    SET_NONE( px );
  }

}



/*******************************************************************//**
 * replace( x, probe, subst ) -> str
 ***********************************************************************
 */
static void __eval_string_replace( vgx_Evaluator_t *self ) {

  vgx_EvalStackItem_t *ps = POP_PITEM( self );
  vgx_EvalStackItem_t *pp = POP_PITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  CString_attr subst_attr = CSTRING_ATTR_NONE;
  const char *subst;
  if( (subst = __stackitem_as_str( ps, NULL, &subst_attr )) == NULL ) {
    subst = __stackitem_as_str( __cast_str( self, ps ), NULL, &subst_attr );
  }

  CString_attr probe_attr = CSTRING_ATTR_NONE;
  const char *probe;
  if( (probe = __stackitem_as_str( pp, NULL, &probe_attr )) == NULL ) {
    probe = __stackitem_as_str( __cast_str( self, pp ), NULL, &probe_attr );
  }

  int32_t sz_x = 0;
  CString_attr x_attr = CSTRING_ATTR_NONE;
  const char *x;
  if( (x = __stackitem_as_str( px, &sz_x, &x_attr )) == NULL ) {
    x = __stackitem_as_str( __cast_str( self, px ), &sz_x, &x_attr );
  }

  if( subst && probe && x ) {
    CString_t *CSTR__repl = CStringNewReplaceAlloc( x, sz_x, probe, subst, self->graph->ephemeral_string_allocator_context );
    if( CSTR__repl ) {
      int64_t errpos;
      if( !COMLIB_check_utf8( (const BYTE*)CStringValue( CSTR__repl ), &errpos ) ) {
        CStringAttributes( CSTR__repl ) = CSTRING_ATTR_BYTES;
      }
      else {
        CStringAttributes( CSTR__repl ) = x_attr;
      }

      vgx_EvalStackItem_t scoped = {
        .type = STACK_ITEM_TYPE_CSTRING,
        .CSTR__str = CSTR__repl
      };

      if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
        *px = scoped; // replaced string returned
        return;
      }

    }
  }
  // failed
  SET_NONE( px );
}



/*******************************************************************//**
 * slice( x, a, b ) -> str
 ***********************************************************************
 */
static void __eval_string_slice( vgx_Evaluator_t *self ) {

  vgx_EvalStackItem_t *pb = POP_PITEM( self );
  vgx_EvalStackItem_t *pa = POP_PITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  int32_t ia, *a;
  int32_t ib, *b;

  // a is integer or null
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    a = NULL;
  }
  else if( pa->type == STACK_ITEM_TYPE_INTEGER ) {
    ia = (int)pa->integer;
    a = &ia;
  }
  else {
    return;
  }

  // b is integer or null
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    b = NULL;
  }
  else if( pb->type == STACK_ITEM_TYPE_INTEGER ) {
    ib = (int)pb->integer;
    b = &ib;
  }
  else {
    return;
  }

  CString_t *CSTR__slice = NULL;

  if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    CSTR__slice = CALLABLE( px->CSTR__str )->SliceAlloc( px->CSTR__str, a, b, self->graph->ephemeral_string_allocator_context );
  }
  else {
    // x must be string
    CString_attr x_attr = CSTRING_ATTR_NONE;
    const char *x;
    if( (x = __stackitem_as_str( px, NULL, &x_attr )) == NULL ) { 
      x = __stackitem_as_str( __cast_str( self, px ), NULL, &x_attr );
    }
    if( x ) {
      CSTR__slice = CStringNewSliceAlloc( x, a, b, self->graph->ephemeral_string_allocator_context );
    }
  }

  if( CSTR__slice ) {
    if( CStringLength( CSTR__slice ) > 0 && CStringCodepoints( CSTR__slice ) == 0 ) {
      CStringAttributes( CSTR__slice ) = CSTRING_ATTR_BYTES;
    }

    vgx_EvalStackItem_t scoped = {
      .type = STACK_ITEM_TYPE_CSTRING,
      .CSTR__str = CSTR__slice
    };

    if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
      *px = scoped; // slice string returned
      return;
    }

  }

  // failed
  SET_NONE( px );
}



/*******************************************************************//**
 * prefix( x, n ) -> str
 ***********************************************************************
 */
static void __eval_string_prefix( vgx_Evaluator_t *self ) {

  vgx_EvalStackItem_t *pn = POP_PITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  int32_t n;

  // n is integer
  if( pn->type == STACK_ITEM_TYPE_INTEGER ) {
    n = (int)pn->integer;
  }
  else {
    return;
  }

  CString_t *CSTR__prefix = NULL;

  if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    CSTR__prefix = CALLABLE( px->CSTR__str )->PrefixAlloc( px->CSTR__str, n, self->graph->ephemeral_string_allocator_context );
  }
  else {
    // x must be string
    CString_attr x_attr = CSTRING_ATTR_NONE;
    const char *x;
    if( (x = __stackitem_as_str( px, NULL, &x_attr )) == NULL ) { 
      x = __stackitem_as_str( __cast_str( self, px ), NULL, &x_attr );
    }
    if( x ) {
      CSTR__prefix = CStringNewPrefixAlloc( x, n, self->graph->ephemeral_string_allocator_context );
    }
  }

  if( CSTR__prefix ) {
    if( CStringLength( CSTR__prefix ) > 0 && CStringCodepoints( CSTR__prefix ) == 0 ) {
      CStringAttributes( CSTR__prefix ) = CSTRING_ATTR_BYTES;
    }

    vgx_EvalStackItem_t scoped = {
      .type = STACK_ITEM_TYPE_CSTRING,
      .CSTR__str = CSTR__prefix
    };

    if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) > 0 ) {
      *px = scoped; // slice string returned
      return;
    }

  }

  // failed
  SET_NONE( px );
}



/*******************************************************************//**
 * idx( x, i ) -> val
 ***********************************************************************
 */
static void __eval_string_index( vgx_Evaluator_t *self ) {

  vgx_EvalStackItem_t *pi = POP_PITEM( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );

  // must be idx( str, int )
  if( px->type != STACK_ITEM_TYPE_CSTRING || pi->type != STACK_ITEM_TYPE_INTEGER ) {
    STACK_RETURN_NONE( self );
  }

  // size of element
  int64_t q = 1;
  switch( CStringAttributes( px->CSTR__str ) ) {
  case CSTRING_ATTR_ARRAY_INT:
  case CSTRING_ATTR_ARRAY_FLOAT:
    q = sizeof(QWORD);
  }

  // element count
  int64_t sz = CStringLength( px->CSTR__str ) / q;
  if( sz < 1 ) {
    STACK_RETURN_NONE( self );
  }

  // modulo index
  int64_t i = pi->integer;
  if( i < 0 ) {
    i += sz * (1 + (-i) / sz);
  }
  i = i % sz;

  // lookup
  const char *data = CStringValue( px->CSTR__str );
  int64_t i64;
  double f64;
  switch( CStringAttributes( px->CSTR__str ) ) {
  case CSTRING_ATTR_ARRAY_INT:
    i64 = *((int64_t*)data + i);
    PUSH_INTEGER_VALUE( self, i64 );
    return;
  case CSTRING_ATTR_ARRAY_FLOAT:
    f64 = *((double*)data + i);
    PUSH_REAL_VALUE( self, f64 );
    return;
  default:
    i64 = *(data + i);
    PUSH_INTEGER_VALUE( self, i64 );
    return;
  }

}




/*******************************************************************//**
 * __pop_strings()
 ***********************************************************************
 */
static int __pop_strings( vgx_Evaluator_t *self, const char **a, const char **b ) {
  vgx_EvalStackItem_t *pb = POP_PITEM( self );
  vgx_EvalStackItem_t *pa = POP_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( pa, pb );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XSTR_YSTR:
    *a = CStringValue( pa->CSTR__str );
    *b = CStringValue( pb->CSTR__str );
    break;
  case STACK_PAIR_TYPE_XVID_YSTR:
    *a = pa->vertexid->CSTR__idstr ? CStringValue( pa->vertexid->CSTR__idstr ) : pa->vertexid->idprefix.data;
    *b = CStringValue( pb->CSTR__str );
    break;
  case STACK_PAIR_TYPE_XSTR_YVID:
    *a = CStringValue( pa->CSTR__str );
    *b = pb->vertexid->CSTR__idstr ? CStringValue( pb->vertexid->CSTR__idstr ) : pb->vertexid->idprefix.data;
    break;
  case STACK_PAIR_TYPE_XVID_YVID:
    *a = pa->vertexid->CSTR__idstr ? CStringValue( pa->vertexid->CSTR__idstr ) : pa->vertexid->idprefix.data;
    *b = pb->vertexid->CSTR__idstr ? CStringValue( pb->vertexid->CSTR__idstr ) : pb->vertexid->idprefix.data;
    break;
  default:
    *a = NULL;
    *b = NULL;
  }

  if( *a && *b ) {
    return 0;
  }

  return -1;

}



/*******************************************************************//**
 * __pop_cstrings()
 ***********************************************************************
 */
static int __pop_cstrings( vgx_Evaluator_t *self, const CString_t **CSTR__a, const CString_t **CSTR__b ) {
  vgx_EvalStackItem_t *pb = POP_PITEM( self );
  vgx_EvalStackItem_t *pa = POP_PITEM( self );
  vgx_StackPairType_t pair_type = PAIR_TYPE( pa, pb );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XSTR_YSTR:
    *CSTR__a = pa->CSTR__str;
    *CSTR__b = pb->CSTR__str;
    break;
  case STACK_PAIR_TYPE_XVID_YSTR:
    *CSTR__a = pa->vertexid->CSTR__idstr ? pa->vertexid->CSTR__idstr : __new_scoped_cstring( self, pa->vertexid->idprefix.data );
    *CSTR__b = pb->CSTR__str;
    break;
  case STACK_PAIR_TYPE_XSTR_YVID:
    *CSTR__a = pa->CSTR__str;
    *CSTR__b = pb->vertexid->CSTR__idstr ? pb->vertexid->CSTR__idstr : __new_scoped_cstring( self, pb->vertexid->idprefix.data );
    break;
  case STACK_PAIR_TYPE_XVID_YVID:
    *CSTR__a = pa->vertexid->CSTR__idstr ? pa->vertexid->CSTR__idstr : __new_scoped_cstring( self, pa->vertexid->idprefix.data );
    *CSTR__b = pb->vertexid->CSTR__idstr ? pb->vertexid->CSTR__idstr : __new_scoped_cstring( self, pb->vertexid->idprefix.data );
    break;
  default:
    *CSTR__a = NULL;
    *CSTR__b = NULL;
  }

  if( *CSTR__a && *CSTR__b ) {
    return 0;
  }

  return -1;

}



/*******************************************************************//**
 * __pop_string()
 ***********************************************************************
 */
static const char * __pop_string( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = POP_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_CSTRING:
    return CStringValue( px->CSTR__str );
  case STACK_ITEM_TYPE_VERTEXID:
    return px->vertexid->CSTR__idstr ? CStringValue( px->vertexid->CSTR__idstr ) : px->vertexid->idprefix.data;
  default:
    return NULL;
  }
}



/*******************************************************************//**
 * __pop_cstring()
 ***********************************************************************
 */
static const CString_t * __pop_cstring( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = POP_PITEM( self );
  switch( px->type ) {
  case STACK_ITEM_TYPE_CSTRING:
    return px->CSTR__str;
  case STACK_ITEM_TYPE_VERTEXID:
    return px->vertexid->CSTR__idstr ? px->vertexid->CSTR__idstr : __new_scoped_cstring( self, px->vertexid->idprefix.data );
  default:
    return NULL;
  }
}



/*******************************************************************//**
 * __item_as_cstring( self, pitem )
 ***********************************************************************
 */
static const CString_t * __item_as_cstring( vgx_Evaluator_t *self, vgx_EvalStackItem_t *pitem ) {
  switch( pitem->type ) {
  case STACK_ITEM_TYPE_CSTRING:
    return pitem->CSTR__str;
  case STACK_ITEM_TYPE_VERTEXID:
    return pitem->vertexid->CSTR__idstr ? pitem->vertexid->CSTR__idstr : __new_scoped_cstring( self, pitem->vertexid->idprefix.data );
  default:
    return NULL;
  }
}



/*******************************************************************//**
 * strcmp( a, b ) -> integer
 ***********************************************************************
 */
static void __eval_string_strcmp( vgx_Evaluator_t *self ) {
  const char *a, *b;
  if( __pop_strings( self, &a, &b ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    int cmp = strcmp( a, b );
    STACK_RETURN_INTEGER( self, cmp );
  }
}



/*******************************************************************//**
 * strcasecmp( a, b ) -> integer
 ***********************************************************************
 */
static void __eval_string_strcasecmp( vgx_Evaluator_t *self ) {
  const char *a, *b;
  if( __pop_strings( self, &a, &b ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    int cmp = strcasecmp( a, b );
    STACK_RETURN_INTEGER( self, cmp );
  }
}



/*******************************************************************//**
 * multimatch( target[, probe1[, probe2[, ...]]]  ) -> bool
 ***********************************************************************
 */
static void __eval_variadic_string_multimatch( vgx_Evaluator_t *self, bool mode_and, bool ignore_case ) {
  int64_t nargs = self->op->arg.integer;
  if( nargs < 1 ) {
    STACK_RETURN_INTEGER(self, 0 );
  }

  // Target string
  int64_t target_idx = nargs - 1;
  vgx_EvalStackItem_t *pt = IDX_PITEM( self, target_idx );
  const CString_t *CSTR__target = __item_as_cstring( self, pt );
  if( CSTR__target == NULL ) {
    STACK_RETURN_INTEGER(self, 0 );
  }
  --nargs;

  int match;
  const char *probe;

  // AND MODE
  if( mode_and ) {
    // Disprove it
    int all = 1;
    while( nargs-- > 0 ) {
      if( (probe = __pop_string( self )) == NULL ) {
        all = 0;
        break;
      }
      if( (all = CALLABLE( CSTR__target )->Contains( CSTR__target, probe, ignore_case )) == 0 ) {
        break;
      }
    }
    match = all;
  }
  // OR MODE
  else {
    // Any match
    int any = 0;
    while( nargs-- > 0 ) {
      if( (probe = __pop_string( self )) != NULL ) {
        if( (any = CALLABLE( CSTR__target )->Contains( CSTR__target, probe, ignore_case )) != 0 ) {
          break;
        }
      }
    }
    match = any;
  }

  // Discard unused varargs
  DISCARD_ITEMS( self, nargs );

  // Discard the last args (which is the target string)
  POP_PITEM(self);

  STACK_RETURN_INTEGER( self, match );
}



/*******************************************************************//**
 * strall( target[, probe1[, probe2[, ...]]]  ) -> bool
 ***********************************************************************
 */
static void __eval_variadic_string_all( vgx_Evaluator_t *self ) {
  __eval_variadic_string_multimatch( self, true, false );
}



/*******************************************************************//**
 * strany( target[, probe1[, probe2[, ...]]]  ) -> bool
 ***********************************************************************
 */
static void __eval_variadic_string_any( vgx_Evaluator_t *self ) {
  __eval_variadic_string_multimatch( self, false, false );
}


/*******************************************************************//**
 * striall( target[, probe1[, probe2[, ...]]]  ) -> bool
 ***********************************************************************
 */
static void __eval_variadic_string_all_icase( vgx_Evaluator_t *self ) {
  __eval_variadic_string_multimatch( self, true, true );
}



/*******************************************************************//**
 * striany( target[, probe1[, probe2[, ...]]]  ) -> bool
 ***********************************************************************
 */
static void __eval_variadic_string_any_icase( vgx_Evaluator_t *self ) {
  __eval_variadic_string_multimatch( self, false, true );
}



/*******************************************************************//**
 * __string_contains
 ***********************************************************************
 */
static void __string_contains( vgx_Evaluator_t *self, bool ignore_case ) {
  const CString_t *CSTR__string, *CSTR__probe;
  if( __pop_cstrings( self, &CSTR__string, &CSTR__probe ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    const char *probe = CStringValue( CSTR__probe );
    bool found = CALLABLE( CSTR__string )->Contains( CSTR__string, probe, ignore_case );
    STACK_RETURN_INTEGER( self, found );
  }
}



/*******************************************************************//**
 * contains( a, b ) -> bool
 ***********************************************************************
 */
static void __eval_string_contains( vgx_Evaluator_t *self ) {
  __string_contains( self, false );
}



/*******************************************************************//**
 * contains_icase( a, b ) -> bool
 ***********************************************************************
 */
static void __eval_string_contains_icase( vgx_Evaluator_t *self ) {
  __string_contains( self, true );
}



/*******************************************************************//**
 * __string_startswith
 ***********************************************************************
 */
static void __string_startswith( vgx_Evaluator_t *self, bool ignore_case ) {
  const CString_t *CSTR__string, *CSTR__probe;
  if( __pop_cstrings( self, &CSTR__string, &CSTR__probe ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    int at_0;
    const char *probe = CStringValue( CSTR__probe );
    if( CStringLength( CSTR__probe ) > CStringLength( CSTR__string ) ) {
      at_0 = 0;
    }
    else {
      at_0 = CALLABLE( CSTR__string )->Find( CSTR__string, probe, 0, ignore_case ) == 0;
    }
    STACK_RETURN_INTEGER( self, at_0 );
  }
}



/*******************************************************************//**
 * startswith( string, probe ) -> bool
 ***********************************************************************
 */
static void __eval_string_startswith( vgx_Evaluator_t *self ) {
  __string_startswith( self, false );
}



/*******************************************************************//**
 * startswith_icase( string, probe ) -> bool
 ***********************************************************************
 */
static void __eval_string_startswith_icase( vgx_Evaluator_t *self ) {
  __string_startswith( self, true );
}



/*******************************************************************//**
 * __string_endswith
 ***********************************************************************
 */
static void __string_endswith( vgx_Evaluator_t *self, bool ignore_case ) {
  const CString_t *CSTR__string, *CSTR__probe;
  if( __pop_cstrings( self, &CSTR__string, &CSTR__probe ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    int at_end;
    const char *probe = CStringValue( CSTR__probe );
    int32_t szp = CStringLength( CSTR__probe );
    if( szp > CStringLength( CSTR__string ) ) {
      at_end = 0;
    }
    else {
      at_end = CALLABLE( CSTR__string )->Find( CSTR__string, probe, -szp, ignore_case ) == 0;
    }
    STACK_RETURN_INTEGER( self, at_end );
  }
}



/*******************************************************************//**
 * endswith( string, probe ) -> bool
 ***********************************************************************
 */
static void __eval_string_endswith( vgx_Evaluator_t *self ) {
  __string_endswith( self, false );
}



/*******************************************************************//**
 * endswith_icase( string, probe ) -> bool
 ***********************************************************************
 */
static void __eval_string_endswith_icase( vgx_Evaluator_t *self ) {
  __string_endswith( self, true );
}



/*******************************************************************//**
 * __string_indexof
 ***********************************************************************
 */
static void __string_indexof( vgx_Evaluator_t *self, bool ignore_case ) {
  const CString_t *CSTR__string, *CSTR__probe;
  if( __pop_cstrings( self, &CSTR__string, &CSTR__probe ) < 0 ) {
    STACK_RETURN_NONE( self );
  }
  else {
    int found_at = -1;
    const char *probe = CStringValue( CSTR__probe );
    if( CStringLength( CSTR__probe ) <= CStringLength( CSTR__string ) ) {
      found_at = CALLABLE( CSTR__string )->Find( CSTR__string, probe, 0, ignore_case );
    }
    STACK_RETURN_INTEGER( self, found_at );
  }
}



/*******************************************************************//**
 * indexof( string, probe ) -> integer
 ***********************************************************************
 */
static void __eval_string_indexof( vgx_Evaluator_t *self ) {
  __string_indexof( self, false );
}



/*******************************************************************//**
 * indexof_icase( string, probe ) -> integer
 ***********************************************************************
 */
static void __eval_string_indexof_icase( vgx_Evaluator_t *self ) {
  __string_indexof( self, true );
}



/*******************************************************************//**
 * strftime( ts_or_tms ) -> str
 ***********************************************************************
 */
static void __eval_string_strftime( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );

  time_t ts;
  int64_t fraction = -1;

  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    if( px->integer > UINT32_MAX ) {
      ts = px->integer / 1000; // assume milliseconds
      fraction = px->integer % 1000;
    }
    else {
      ts = px->integer;
    }
  }
  else if( px->type == STACK_ITEM_TYPE_REAL ) {
    // assume seconds
    double i, f;
    f = modf( px->real, &i );
    ts = (time_t)i;
    fraction = (int64_t)(f * 1000);
  }
  else {
    ts = 0;
  }

  struct tm *_tm = localtime( &ts );
  if( _tm ) {
    char buf[32];
    size_t sz = 19;
    strftime( buf, sz+1, "%Y-%m-%d %H:%M:%S", _tm ); 
    if( fraction >= 0 ) {
      sprintf( buf+sz, ".%03lld", fraction );
      sz += 4;
    }

    CString_constructor_args_t args = {
      .string      = buf,
      .len         = (int)sz,
      .ucsz        = 0,
      .format      = NULL,
      .format_args = NULL,
      .alloc       = self->graph->ephemeral_string_allocator_context
    };

    vgx_EvalStackItem_t scoped = {
      .type = STACK_ITEM_TYPE_CSTRING,
      .CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, &args )
    };

    if( scoped.ptr == NULL ) {
      SET_NONE( px );
      return;
    }

    if( iEvaluator.LocalAutoScopeObject( self, &scoped, true ) < 0 ) {
      SET_NONE( px );
      return;
    }

    // Put time string on stack
    *px = scoped;

  }
  else {
    SET_NONE( px );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_string_modtostr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    px->CSTR__str = _vxeval_modifier_strings[ px->integer & _VGX_PREDICATOR_MOD_TYP_MASK ];
    px->type = STACK_ITEM_TYPE_CSTRING;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_string_dirtostr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    px->CSTR__str = _vxeval_arcdir_strings[ px->integer & 0x3 ];
    px->type = STACK_ITEM_TYPE_CSTRING;
  }
}



#endif
