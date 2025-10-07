/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _conditional.h
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

#ifndef _VGX_VXEVAL_MODULES_CONDITIONAL_H
#define _VGX_VXEVAL_MODULES_CONDITIONAL_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
// Equality
static void __eval_binary_equ( vgx_Evaluator_t *self );
static void __eval_binary_neq( vgx_Evaluator_t *self );

// Comparison
static void __eval_binary_gt( vgx_Evaluator_t *self );
static void __eval_binary_gte( vgx_Evaluator_t *self );
static void __eval_binary_lt( vgx_Evaluator_t *self );
static void __eval_binary_lte( vgx_Evaluator_t *self );

// In operator
static void __eval_binary_elementof( vgx_Evaluator_t *self );

// Notin operator
static void __eval_binary_notelementof( vgx_Evaluator_t *self );

// Ternary
static void __eval_ternary_condition( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __string_match( vgx_EvalStackItem_t *px, vgx_EvalStackItem_t *py, bool positive );




/*******************************************************************//**
 * __condition
 ***********************************************************************
 */
__inline static int __condition( vgx_EvalStackItem_t *c ) {
  return c->bits && c->type != STACK_ITEM_TYPE_NAN;
}



/*******************************************************************//**
 *    ab*ef
 ***********************************************************************
 */
__inline static int __chars_internal_wild( const char *data, int sz_data, const CString_t *CSTR__probe ) {
  // abc*
  const char *p = CStringValue( CSTR__probe );
  const char *s = data;
  // scan while chars remain in probe and string
  while( *p == *s && *s++ != '\0' ) {
    ++p;
  }
  // the internal * in probe should now be reached (represented by ASCII ETX=3)
  int sz_prefix = (int)(p - CStringValue( CSTR__probe ));
  if( *p++ != 3 ) { // ETX=3
    return 0; // MISS
  }
  // compute length of target if not known
  if( sz_data < 0 ) {
    sz_data = sz_prefix;
    while( *s++ != '\0' ) {
      ++sz_data;
    }
  }
  // *def
  int sz_suffix = CStringLength( CSTR__probe ) - 1 - sz_prefix; // account for the embedded * in probe
  int dx_end = sz_data - sz_suffix;
  // no match if data smaller than sum of prefix and suffix
  if( sz_prefix > dx_end ) {
    return 0; // MISS
  }
  const char *e = data + dx_end;
  // scan while chars remain in probe and string
  while( *p == *e && *e++ != '\0' ) {
    ++p;
  }
  // the end of probe should now be reached
  if( *p != '\0' ) {
    return 0; // MISS
  }

  // HIT
  return 1;
}



/*******************************************************************//**
 *    ab*ef
 ***********************************************************************
 */
__inline static int __cstring_internal_wild( const CString_t *CSTR__target, const CString_t *CSTR__probe ) {
  const char *data = CStringValue( CSTR__target );
  int sz_data = CStringLength( CSTR__target );
  return __chars_internal_wild( data, sz_data, CSTR__probe );
}



/*******************************************************************//**
 *    abc*
 ***********************************************************************
 */
__inline static int __cstring_prefix( const CString_t *CSTR__target, const CString_t *CSTR__probe ) {
  if( CStringLength( CSTR__probe ) <= CStringLength( CSTR__target ) ) {
    return CStringStartsWith( CSTR__target, CStringValue( CSTR__probe ) );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *    *ghi
 ***********************************************************************
 */
__inline static int __cstring_suffix( const CString_t *CSTR__target, const CString_t *CSTR__probe ) {
  if( CStringLength( CSTR__probe ) <= CStringLength( CSTR__target ) ) {
    return CStringEndsWith( CSTR__target, CStringValue( CSTR__probe ) );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *    *def*
 ***********************************************************************
 */
__inline static int __cstring_infix( const CString_t *CSTR__target, const CString_t *CSTR__probe ) {
  if( CStringLength( CSTR__probe ) <= CStringLength( CSTR__target ) ) {
    return CStringContains( CSTR__target, CStringValue( CSTR__probe ) );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *    
 ***********************************************************************
 */
__inline static int __cstring_wild_match( const CString_t *CSTR__target, const CString_t *CSTR__probe ) {
  if( IS_STRING_PREFIX( CSTR__probe ) ) {
    if( IS_STRING_SUFFIX( CSTR__probe ) ) {
      return __cstring_internal_wild( CSTR__target, CSTR__probe );
    }
    else {
      return __cstring_prefix( CSTR__target, CSTR__probe );
    }
  }
  else if( IS_STRING_SUFFIX( CSTR__probe ) ) {
    return __cstring_suffix( CSTR__target, CSTR__probe );
  }
  else if( IS_STRING_INFIX( CSTR__probe ) ) {
    return __cstring_infix( CSTR__target, CSTR__probe );
  }
  else {
    return 0; // ???
  }
}



/*******************************************************************//**
 *    
 ***********************************************************************
 */
__inline static int __chars_wild_match( const char *target, const CString_t *CSTR__probe ) {
  if( IS_STRING_PREFIX( CSTR__probe ) ) {
    if( IS_STRING_SUFFIX( CSTR__probe ) ) {
      return __chars_internal_wild( target, -1, CSTR__probe );
    }
    else {
      return CharsStartsWithConst( target, CStringValue( CSTR__probe ) );
    }
  }
  else if( IS_STRING_SUFFIX( CSTR__probe ) ) {
    return CharsEndsWithConst( target, CStringValue( CSTR__probe ) );
  }
  else if( IS_STRING_INFIX( CSTR__probe ) ) {
    return CharsContainsConst( target, CStringValue( CSTR__probe ) );
  }
  else {
    return 0; // ???
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __string_match( vgx_EvalStackItem_t *px, vgx_EvalStackItem_t *py, bool positive ) {
  static const vgx_vertex_probe_spec id_EQU = _VERTEX_PROBE_ID_EQU | _VERTEX_PROBE_ID_ENA;
  static const vgx_vertex_probe_spec id_NEQ = _VERTEX_PROBE_ID_NEQ | _VERTEX_PROBE_ID_ENA;
  static const vgx_vertex_probe_spec id_prefix = _VERTEX_PROBE_ID_LTE | _VERTEX_PROBE_ID_ENA;
  static const vgx_vertex_probe_spec neg =       VGX_VALUE_NEG << _VERTEX_PROBE_ID_OFFSET;
  int match = 0;
  vgx_StackPairType_t pair_type = PAIR_TYPE( px, py );
  switch( pair_type ) {
  case STACK_PAIR_TYPE_XSTR_YSTR:
    // No wildcards
    if( IS_STRING_WILDCARD( py->CSTR__str ) + IS_STRING_WILDCARD( px->CSTR__str ) == 0 ) {
      match = CStringEquals( py->CSTR__str, px->CSTR__str );
    }
    else {
      // y is wild
      if( IS_STRING_WILDCARD( py->CSTR__str ) ) {
        match = __cstring_wild_match( px->CSTR__str, py->CSTR__str );
      }
      // x is wild
      else {
        match = __cstring_wild_match( py->CSTR__str, px->CSTR__str );
      }
    }
    return positive ? match : !match;
  case STACK_PAIR_TYPE_XVID_YSTR:
    if( !IS_STRING_WILDCARD( py->CSTR__str ) ) {
      return vtxmatchfunc.Identifier( positive ? id_EQU : id_NEQ, py->CSTR__str, px->vertexid, NULL );
    }
    else {
      if( px->vertexid->CSTR__idstr ) {
        match = __cstring_wild_match( px->vertexid->CSTR__idstr, py->CSTR__str );
      }
      else {
        match = __chars_wild_match( px->vertexid->idprefix.data, py->CSTR__str );
      }
    }
    return positive ? match : !match;
  case STACK_PAIR_TYPE_XSTR_YVID:
    if( !IS_STRING_WILDCARD( px->CSTR__str ) ) {
      return vtxmatchfunc.Identifier( positive ? id_EQU : id_NEQ, px->CSTR__str, py->vertexid, NULL );
    }
    else {
      if( py->vertexid->CSTR__idstr ) {
        match = __cstring_wild_match( py->vertexid->CSTR__idstr, px->CSTR__str );
      }
      else {
        match = __chars_wild_match( py->vertexid->idprefix.data, px->CSTR__str );
      }
    }
    return positive ? match : !match;
  case STACK_PAIR_TYPE_XVID_YVID:
    match = px->vertexid == py->vertexid;
    return positive ? match : !match;
  default:
    return positive ? 0 : 1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __equ_vector( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_Similarity_t *sim = self->graph->similarity;
  return a->vector && b->vector && CALLABLE( sim )->Match( sim, a->vector, b->vector );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __equ( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  // Bit comparable
  if( __are_stack_item_types_bit_comparable( a->type, b->type ) ) {
    if( a->bits == b->bits ) {
      return 1;
    }
  }

  if( (a->type | b->type) == STACK_ITEM_TYPE_NONE ) {
    return 1;
  }

  if( a->type == STACK_ITEM_TYPE_REAL ) {
    if( b->type == STACK_ITEM_TYPE_REAL ) {
      return fabs( a->real - b->real ) < FLT_EPSILON;
    }
    if( __is_stack_item_type_bit_comparable( b->type ) ) {
      return fabs( a->real - (double)b->integer ) < FLT_EPSILON;
    }
    return b->type == STACK_ITEM_TYPE_WILD;
  }

  if( b->type == STACK_ITEM_TYPE_REAL ) {
    if( __is_stack_item_type_bit_comparable( a->type ) ) {
      return fabs( (double)a->integer - b->real ) < FLT_EPSILON;
    }
    return a->type == STACK_ITEM_TYPE_WILD;
  }

  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );

  if( pair_type == STACK_PAIR_TYPE_XVEC_YVEC ) {
    return __equ_vector( self, a, b );
  }

  if( __is_stack_pair_item_wild( pair_type ) ) {
    return 1;
  }

  return __string_match( a, b, true );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __neq( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  return !__equ( self, a, b );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __gt( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return a->integer > b->integer;
  }
  // Unsigned number tests
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XREA_YREA:
      return a->real > b->real;
    case STACK_PAIR_TYPE_XINT_YREA:
      return a->integer > b->real;
    case STACK_PAIR_TYPE_XREA_YINT:
      return a->real > b->integer;
    // qword test
    default:
      return a->bits > b->bits;
    }
  }
  // Keyval
  else if( pair_type == STACK_PAIR_TYPE_XKYV_YKYV ) {
    return a->keyval.value > b->keyval.value;
  }
  // String test
  else if( pair_type == STACK_PAIR_TYPE_XSTR_YSTR ) {
    const char *sa = CStringValue( a->CSTR__str );
    const char *sb = CStringValue( b->CSTR__str );
    return strcmp( sa, sb ) > 0;
  }
  // Vector test
  else if( self && pair_type == STACK_PAIR_TYPE_XVEC_YVEC ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    return a->vector && b->vector && CALLABLE( sim )->CompareVectors( sim, a->vector, b->vector ) > 0;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __gte( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return a->integer >= b->integer;
  }
  // Unsigned number tests
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XREA_YREA:
      return a->real >= b->real;
    case STACK_PAIR_TYPE_XINT_YREA:
      return a->integer >= b->real;
    case STACK_PAIR_TYPE_XREA_YINT:
      return a->real >= b->integer;
    // qword test
    default:
      return a->bits >= b->bits;
    }
  }
  // Keyval
  else if( pair_type == STACK_PAIR_TYPE_XKYV_YKYV ) {
    return a->keyval.value >= b->keyval.value;
  }
  // String test
  else if( pair_type == STACK_PAIR_TYPE_XSTR_YSTR ) {
    const char *sa = CStringValue( a->CSTR__str );
    const char *sb = CStringValue( b->CSTR__str );
    return strcmp( sa, sb ) >= 0;
  }
  // Vector test
  else if( self && pair_type == STACK_PAIR_TYPE_XVEC_YVEC ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    return a->vector && b->vector && CALLABLE( sim )->CompareVectors( sim, a->vector, b->vector ) >= 0;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __lt( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return a->integer < b->integer;
  }
  // Unsigned number tests
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XREA_YREA:
      return a->real < b->real;
    case STACK_PAIR_TYPE_XINT_YREA:
      return a->integer < b->real;
    case STACK_PAIR_TYPE_XREA_YINT:
      return a->real < b->integer;
    // qword test
    default:
      return a->bits < b->bits;
    }
  }
  // Keyval
  else if( pair_type == STACK_PAIR_TYPE_XKYV_YKYV ) {
    return a->keyval.value < b->keyval.value;
  }
  // String test
  else if( pair_type == STACK_PAIR_TYPE_XSTR_YSTR ) {
    const char *sa = CStringValue( a->CSTR__str );
    const char *sb = CStringValue( b->CSTR__str );
    return strcmp( sa, sb ) < 0;
  }
  // Vector test
  else if( self && pair_type == STACK_PAIR_TYPE_XVEC_YVEC ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    return a->vector && b->vector && CALLABLE( sim )->CompareVectors( sim, a->vector, b->vector ) < 0;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __lte( vgx_Evaluator_t *self, vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return a->integer <= b->integer;
  }
  // Unsigned number tests
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XREA_YREA:
      return a->real <= b->real;
    case STACK_PAIR_TYPE_XINT_YREA:
      return a->integer <= b->real;
    case STACK_PAIR_TYPE_XREA_YINT:
      return a->real <= b->integer;
    // qword test
    default:
      return a->bits <= b->bits;
    }
  }
  // Keyval
  else if( pair_type == STACK_PAIR_TYPE_XKYV_YKYV ) {
    return a->keyval.value <= b->keyval.value;
  }
  // String test
  else if( pair_type == STACK_PAIR_TYPE_XSTR_YSTR ) {
    const char *sa = CStringValue( a->CSTR__str );
    const char *sb = CStringValue( b->CSTR__str );
    return strcmp( sa, sb ) <= 0;
  }
  // Vector test
  else if( self && pair_type == STACK_PAIR_TYPE_XVEC_YVEC ) {
    vgx_Similarity_t *sim = self->graph->similarity;
    return a->vector && b->vector && CALLABLE( sim )->CompareVectors( sim, a->vector, b->vector ) <= 0;
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_equ( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int equ = __equ( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, equ );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_neq( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int neq = __neq( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, neq );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_gt( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int gt = __gt( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, gt );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_gte( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int gte = __gte( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, gte );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_lt( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int lt = __lt( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, lt );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_lte( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int lte = __lte( self, px, &y );
  SET_INTEGER_PITEM_VALUE( px, lte );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int __vertex_has_property( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_EvalStackItem_t *item ) {
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    // TODO: cache? &self->cache.VERTEX
    return __has_vertex_property( vertex, item->integer );
  case STACK_ITEM_TYPE_CSTRING:
    // Not ideal to compute this at runtime, but in some situation it's a necessary fallback
    return __has_vertex_property( vertex, CStringHash64( item->CSTR__str ) );
  default:
    return 0;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cstring_array_int_contains( const CString_t *CSTR__str, vgx_EvalStackItem_t *item ) {
  int64_t probe;
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    probe = item->integer;
    break;
  case STACK_ITEM_TYPE_REAL:
    probe = (int64_t)item->real;
    if( (double)probe != item->real ) {
      return 0;
    }
    break;
  case STACK_ITEM_TYPE_KEYVAL:
    probe = vgx_cstring_array_map_key( &item->bits );
    break;
  default:
    probe = item->integer;
  }

  int64_t *v = (int64_t*)CStringValue( CSTR__str );
  int64_t *end = v + VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
  while( v < end ) {
    if( probe == *v++ ) {
      return 1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cstring_array_float_contains( const CString_t *CSTR__str, vgx_EvalStackItem_t *item ) {
  double probe;
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    probe = (double)item->integer;
    break;
  case STACK_ITEM_TYPE_REAL:
    probe = item->real;
    break;
  default:
    return 0;
  }

  double *v = (double*)CStringValue( CSTR__str );
  double *end = v + VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
  while( v < end ) {
    if( fabs( probe - *v++ ) < FLT_EPSILON ) {
      return 1;
    }
  }
  return 0;

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cstring_array_map_contains( const CString_t *CSTR__str, vgx_EvalStackItem_t *item ) {
  QWORD *data = (QWORD*)CStringValue( CSTR__str );
  int key;
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    key = (int)item->integer;
    break;
  case STACK_ITEM_TYPE_KEYVAL:
    key = vgx_cstring_array_map_key( &item->bits );
    break;
  default:
    return 0;
  }

  return vgx_cstring_array_map_get( data, key ) != NULL;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cstring_contains( const CString_t *CSTR__str, vgx_EvalStackItem_t *item ) {
  CString_attr attr_array = CStringAttributes( CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK;
  // array
  if( attr_array ) {
    switch( attr_array ) {
    case CSTRING_ATTR_ARRAY_INT:
      return __cstring_array_int_contains( CSTR__str, item );
    case CSTRING_ATTR_ARRAY_FLOAT:
      return __cstring_array_float_contains( CSTR__str, item );
    case CSTRING_ATTR_ARRAY_MAP:
      return __cstring_array_map_contains( CSTR__str, item );
    default:
      return 0;
    }
  }
  // string
  else if( item->type == STACK_ITEM_TYPE_CSTRING ) {
    return CALLABLE( CSTR__str )->Contains( CSTR__str, CStringValue( item->CSTR__str ) );
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __vector_has_dimension_by_dimhash( const vgx_Vector_t *vector, vgx_EvalStackItem_t *item ) {
  // vector dim
  if( item->type == STACK_ITEM_TYPE_INTEGER && vector ) {
    uint32_t dim = (uint32_t)item->integer;
    vgx_Vector_vtable_t *iVec = CALLABLE( vector );
    if( iVec->IsInternal( vector ) ) {
      int sz = iVec->Length( vector );
      const vector_feature_t *elem = CALLABLE( vector )->Elements( vector );
      const vector_feature_t *end = elem + sz;
      while( elem < end ) {
        if( elem++->dim == dim ) {
          return 1;
        }
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __vertexid_has_substring( const vgx_VertexIdentifier_t *vid, vgx_EvalStackItem_t *item ) {
  // string
  if( item->type == STACK_ITEM_TYPE_CSTRING ) {
    const char *probe = CStringValue( item->CSTR__str );
    if( vid ) {
      // Long vertex ID
      if( vid->CSTR__idstr ) {
        return CALLABLE( vid->CSTR__idstr )->Contains( vid->CSTR__idstr, probe );
      }
      // Short vertex ID
      else {
        return CharsContainsConst( vid->idprefix.data, probe );
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __value_in_range( vgx_Evaluator_t *self, vgx_EvalStackItem_t **px ) {
  vgx_EvalStackItem_t *pU = self->sp--;
  vgx_EvalStackItem_t *pL = self->sp--;
  vgx_EvalStackItem_t *probe = *px = self->sp;
  // probe >= lower && probe < upper
  return __gte( self, probe, pL ) && __lt( self, probe, pU );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __item_in_set( vgx_Evaluator_t *self, int64_t sz, vgx_EvalStackItem_t **px ) {
  vgx_EvalStackItem_t *member = self->sp;
  self->sp -= sz;
  vgx_EvalStackItem_t *probe = *px = self->sp;
  while( member > probe ) {
    if( __equ( self, member--, probe ) ) {
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_elementof( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int match;
  switch( y.type ) {
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, __vertex_has_property( self, y.vertex, px ) );
    return;
  case STACK_ITEM_TYPE_RANGE:
    match = __value_in_range( self, &px );
    SET_INTEGER_PITEM_VALUE( px, match );
    return;
  case STACK_ITEM_TYPE_CSTRING:
    SET_INTEGER_PITEM_VALUE( px, __cstring_contains( y.CSTR__str, px ) );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    if( y.vector->metas.flags.ecl ) {
      SET_INTEGER_PITEM_VALUE( px, 0 );    
    }
    else {
      SET_INTEGER_PITEM_VALUE( px, __vector_has_dimension_by_dimhash( y.vector, px ) );
    }
    return;
  case STACK_ITEM_TYPE_BITVECTOR:
    match = (px->bits & y.bits) == px->bits;
    SET_INTEGER_PITEM_VALUE( px, match );
    return;
  case STACK_ITEM_TYPE_VERTEXID:
    SET_INTEGER_PITEM_VALUE( px, __vertexid_has_substring( y.vertexid, px ) );
    return;
  case STACK_ITEM_TYPE_SET:
    // y = the number of elements in set
    // x is found immediately before the y elements
    match = __item_in_set( self, y.integer, &px );
    SET_INTEGER_PITEM_VALUE( px, match );
    return;
  default:
    SET_INTEGER_PITEM_VALUE( px, 0 );    
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_binary_notelementof( vgx_Evaluator_t *self ) {
  __eval_binary_elementof( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  px->integer = !px->integer;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_ternary_condition( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t false_item = POP_ITEM( self );
  vgx_EvalStackItem_t true_item = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->bits != 0 ) {
    *px = true_item;
  }
  else {
    *px = false_item;
  }
}



#endif
