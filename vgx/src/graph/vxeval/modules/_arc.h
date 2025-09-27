/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _arc.h
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

#ifndef _VGX_VXEVAL_MODULES_ARC_H
#define _VGX_VXEVAL_MODULES_ARC_H


#include "_conditional.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */

// Debug Print
static void __debug_print( vgx_Evaluator_t *self );
static void __debug_printif( vgx_Evaluator_t *self );

// Debug Arc
static void __debug_arc( vgx_Evaluator_t *self );

// Push Previous Arc
static void __stack_push_arrive_value( vgx_Evaluator_t *self );
static void __stack_push_arrive_distance( vgx_Evaluator_t *self );
static void __stack_push_arrive_relcode( vgx_Evaluator_t *self );
static void __stack_push_arrive_direction( vgx_Evaluator_t *self );
static void __stack_push_arrive_modifier( vgx_Evaluator_t *self );
static void __stack_push_arrive_isfwdonly( vgx_Evaluator_t *self );
static void __stack_push_arrive_issyn( vgx_Evaluator_t *self );
static void __stack_push_arrive_raw( vgx_Evaluator_t *self );

// Push Next Arc
static void __stack_push_exit_value( vgx_Evaluator_t *self );
static void __stack_push_exit_distance( vgx_Evaluator_t *self );
static void __stack_push_exit_relcode( vgx_Evaluator_t *self );
static void __stack_push_exit_direction( vgx_Evaluator_t *self );
static void __stack_push_exit_modifier( vgx_Evaluator_t *self );
static void __stack_push_exit_isfwdonly( vgx_Evaluator_t *self );
static void __stack_push_exit_issyn( vgx_Evaluator_t *self );
static void __stack_push_exit_raw( vgx_Evaluator_t *self );

// Relationship Enum
static void __eval_string_relenc( vgx_Evaluator_t *self );
static void __eval_string_reldec( vgx_Evaluator_t *self );

// Arc Decay
static void __eval_synarc_xdecay( vgx_Evaluator_t *self );
static void __eval_synarc_decay( vgx_Evaluator_t *self );

// (Multiple) Arc Extraction
static void __eval_synarc_hasrel( vgx_Evaluator_t *self );
static void __eval_synarc_hasmod( vgx_Evaluator_t *self );
static void __eval_synarc_hasrelmod( vgx_Evaluator_t *self );
static void __eval_synarc_arcvalue( vgx_Evaluator_t *self );


#define __FUNCTION_IS_PUSH_RELCODE( Function ) ( (Function) == __stack_push_next_relcode || (Function) == __stack_push_prev_relcode )



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_predicator_rel_enum __encode_relationship( vgx_Evaluator_t *self, const CString_t *CSTR__rel, vgx_ExpressEvalRelEncCache_t *cache );
static vgx_StackItemType_t __get_predicator_value_as_item( vgx_predicator_t pred, vgx_EvalStackItem_t *item );



/*******************************************************************//**
 * Return the relationship enumeration for a string
 ***********************************************************************
 */
__inline static vgx_predicator_rel_enum __encode_relationship( vgx_Evaluator_t *self, const CString_t *CSTR__rel, vgx_ExpressEvalRelEncCache_t *cache ) {
  // Match on string instance
  if( cache->CSTR__rel != CSTR__rel ) {
    // Compute hash
    QWORD relhash = CStringHash64( CSTR__rel );
    // Update cached string
    cache->CSTR__rel = CSTR__rel;
    // Match on string value
    if( cache->relhash != relhash ) {
      // Encode
      vgx_predicator_rel_enum relenc = (vgx_predicator_rel_enum)iEnumerator_OPEN.Relationship.GetEnum( self->graph, CSTR__rel );
      if( relenc == VGX_PREDICATOR_REL_NONEXIST || !__relationship_enumeration_valid( relenc ) ) {
        relenc = VGX_PREDICATOR_REL_NONE;
      }
      // Update cache
      cache->relhash = relhash;
      cache->rel = relenc;
    }
  }
  return cache->rel;
}



/*******************************************************************//**
 * Return predicator value as double
 ***********************************************************************
 */
__inline static double __get_predicator_value_as_double( vgx_predicator_t pred ) {
  // Real (float to double)
  if( __VGX_PREDICATOR_FLT_MASK & pred.data ) {
    return pred.val.real;
  }
  // Signed Integer (int32_t to int64_t)
  else if( (__VGX_PREDICATOR_SMT_MASK & pred.data) == __VGX_PREDICATOR_SGN_INT ) {
    return (double)pred.val.integer;
  }
  // Unsigned Integer (uint32_t to int64_t)
  else {
    return (double)pred.val.uinteger;
  }
}



/*******************************************************************//**
 * Return predicator value as a stack item
 ***********************************************************************
 */
__inline static vgx_StackItemType_t __get_predicator_value_as_item( vgx_predicator_t pred, vgx_EvalStackItem_t *item ) {
  // Real (float to double)
  if( __VGX_PREDICATOR_FLT_MASK & pred.data ) {
    item->real = pred.val.real;
    return item->type = STACK_ITEM_TYPE_REAL;
  }
  // Signed Integer (int32_t to int64_t)
  else if( (__VGX_PREDICATOR_SMT_MASK & pred.data) == __VGX_PREDICATOR_SGN_INT ) {
    item->integer = pred.val.integer;
    return item->type = STACK_ITEM_TYPE_INTEGER;
  }
  // Unsigned Integer (uint32_t to int64_t)
  else {
    item->integer = pred.val.uinteger;
    return item->type = STACK_ITEM_TYPE_INTEGER;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __object_to_stderr( vgx_Evaluator_t *self, vgx_EvalStackItem_t *px ) {
  fprintf( stderr, "<%p ", self );
  CString_t *CSTR__str = NULL;
  switch( px->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    fprintf( stderr, "integer> %lld\n", px->integer );
    return;
  case STACK_ITEM_TYPE_REAL:
    fprintf( stderr, "real> %g\n", px->real );
    return;
  case STACK_ITEM_TYPE_NAN:
    fprintf( stderr, "nan>\n" );
    return;
  case STACK_ITEM_TYPE_VERTEX:
    fprintf( stderr, "vertex> " );
    if( px->vertex ) {
      CSTR__str = CALLABLE( px->vertex )->ShortRepr( px->vertex );
    }
    break;
  case STACK_ITEM_TYPE_CSTRING:
    fprintf( stderr, "string> \"%s\"\n", CStringValue( px->CSTR__str ) );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    fprintf( stderr, "vector> " );
    CSTR__str = CALLABLE( px->vector )->ShortRepr( px->vector );
    break;
  case STACK_ITEM_TYPE_BITVECTOR:
    fprintf( stderr, "bitvector> 0x%016llx\n", px->bits );
    return;
  case STACK_ITEM_TYPE_VERTEXID:
    if( px->vertexid->CSTR__idstr ) {
      fprintf( stderr, "vertexid> %s\n", CStringValue( px->vertexid->CSTR__idstr ) );
    }
    else {
      fprintf( stderr, "vertexid> %s\n", px->vertexid->idprefix.data );
    }
    return;
  default:
    fprintf( stderr, "qword> %llu\n", px->bits );
    return;
  }

  if( CSTR__str ) {
    fprintf( stderr, "%s\n", CStringValue( CSTR__str ) );
    CStringDelete( CSTR__str );
  }
  else {
    fprintf( stderr, "null\n" );
  }
}



/*******************************************************************//**
 * DEBUG.PRINT( object )
 ***********************************************************************
 */
static void __debug_print( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  __object_to_stderr( self, px );
}



/*******************************************************************//**
 * DEBUG.PRINTIF( condition, object )
 ***********************************************************************
 */
static void __debug_printif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t obj = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    __object_to_stderr( self, &obj );
  }
}



/*******************************************************************//**
 * Debug arc
 ***********************************************************************
 */
static void __debug_arc( vgx_Evaluator_t *self ) {
  vgx_Graph_t *graph = self->context.VERTEX->graph;
  // Vertices
  const char *tail_id = self->context.TAIL->identifier.idprefix.data;
  const char *this_id = self->context.VERTEX->identifier.idprefix.data;
  const char *head_id = self->context.HEAD->identifier.idprefix.data;
  
  // Previous arc
  vgx_predicator_t prev = self->context.arrive;
  const char *prev_mod = _vgx_modifier_as_string( prev.mod );
  const CString_t *CSTR__prev_rel = iEnumerator_OPEN.Relationship.Decode( graph, prev.rel.enc );
  const char *prev_rel = CSTR__prev_rel ? CStringValue( CSTR__prev_rel ) : "?";
  vgx_EvalStackItem_t prev_itemval;
  CString_t *CSTR__prev_value;
  if( __get_predicator_value_as_item( prev, &prev_itemval ) == STACK_ITEM_TYPE_INTEGER ) {
    CSTR__prev_value = iString.NewFormat( NULL, "%lld", prev_itemval.integer  );
  }
  else {
    CSTR__prev_value = iString.NewFormat( NULL, "%#g", prev_itemval.real  );
  }
  const char *prev_val = CSTR__prev_value ? CStringValue( CSTR__prev_value ) : "?";

  // Next arc
  vgx_predicator_t next = self->context.exit;
  const char *next_mod = _vgx_modifier_as_string( next.mod );
  const CString_t *CSTR__next_rel = iEnumerator_OPEN.Relationship.Decode( graph, next.rel.enc );
  const char *next_rel = CSTR__next_rel ? CStringValue( CSTR__next_rel ) : "?";
  vgx_EvalStackItem_t next_itemval;
  CString_t *CSTR__next_value;
  if( __get_predicator_value_as_item( prev, &next_itemval ) == STACK_ITEM_TYPE_INTEGER ) {
    CSTR__next_value = iString.NewFormat( NULL, "%lld", next_itemval.integer  );
  }
  else {
    CSTR__next_value = iString.NewFormat( NULL, "%#g", next_itemval.real  );
  }
  const char *next_val = CSTR__next_value ? CStringValue( CSTR__next_value ) : "?";

  // Output
  INFO( 0x111, "ARC: [%s] - (%s, %s, %s) -> [%s] - (%s, %s, %s) -> [%s]", tail_id, prev_rel, prev_mod, prev_val, this_id, next_rel, next_mod, next_val, head_id );

  iString.Discard( &CSTR__prev_value );
  iString.Discard( &CSTR__next_value );
}



/*******************************************************************//**
 * Push predicator value
 ***********************************************************************
 */
__inline static void __stack_push_predicator_value( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  __get_predicator_value_as_item( pred, item );
}



/*******************************************************************//**
 * Push predicator distance
 ***********************************************************************
 */
__inline static void __stack_push_predicator_distance( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  int distance = 0;
  if( pred.eph.type == VGX_PREDICATOR_EPH_TYPE_DISTANCE ) {
    distance = pred.eph.value;
  }
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( item, distance );
}



/*******************************************************************//**
 * Push predicator relationship
 ***********************************************************************
 */
__inline static void __stack_push_predicator_relcode( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = pred.rel.enc;
  item->type = STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 * Push predicator direction
 ***********************************************************************
 */
__inline static void __stack_push_predicator_direction( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = pred.rel.dir;
  item->type = STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 * Push predicator modifier
 ***********************************************************************
 */
__inline static void __stack_push_predicator_modifier( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = (pred.data & __VGX_PREDICATOR_SMT_MASK) >> 48;
  item->type = STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 * Push predicator is forward-only
 ***********************************************************************
 */
__inline static void __stack_push_predicator_isfwdonly( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = (pred.data & __VGX_PREDICATOR_FWD_MASK) == __VGX_PREDICATOR_FWD_MASK;
  item->type = STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 * Push predicator is synthetic
 ***********************************************************************
 */
__inline static void __stack_push_predicator_issyn( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->integer = _vgx_predicator_is_synthetic( pred );
  item->type = STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 * Push predicator raw
 ***********************************************************************
 */
__inline static void __stack_push_predicator_raw( vgx_Evaluator_t *self, const vgx_predicator_t pred ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( item, pred.data );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFINE_ARC_FUNCTIONS( Arc )                                                                                                 \
static void __stack_push_##Arc##_value(     vgx_Evaluator_t *self ) { __stack_push_predicator_value(     self, self->context.Arc ); } \
static void __stack_push_##Arc##_distance(  vgx_Evaluator_t *self ) { __stack_push_predicator_distance(  self, self->context.Arc ); } \
static void __stack_push_##Arc##_relcode(   vgx_Evaluator_t *self ) { __stack_push_predicator_relcode(   self, self->context.Arc ); } \
static void __stack_push_##Arc##_direction( vgx_Evaluator_t *self ) { __stack_push_predicator_direction( self, self->context.Arc ); } \
static void __stack_push_##Arc##_modifier(  vgx_Evaluator_t *self ) { __stack_push_predicator_modifier(  self, self->context.Arc ); } \
static void __stack_push_##Arc##_isfwdonly( vgx_Evaluator_t *self ) { __stack_push_predicator_isfwdonly( self, self->context.Arc ); } \
static void __stack_push_##Arc##_issyn(     vgx_Evaluator_t *self ) { __stack_push_predicator_issyn(     self, self->context.Arc ); } \
static void __stack_push_##Arc##_raw(       vgx_Evaluator_t *self ) { __stack_push_predicator_raw(       self, self->context.Arc ); }

__DEFINE_ARC_FUNCTIONS( arrive )
__DEFINE_ARC_FUNCTIONS( exit )



/*******************************************************************//**
 * Convert string to relationship enumeration
 ***********************************************************************
 */
static void __eval_string_relenc( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // Already encoded literal during parsing, or argument is zero (same as no rel)
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    return;
  }
  // Dynamic encoding during evaluation (slow!)
  else if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    SET_INTEGER_PITEM_VALUE( px, __encode_relationship( self, px->CSTR__str, &self->cache.relationship ) );
    return;
  }
  // Invalid
  SET_INTEGER_PITEM_VALUE( px, VGX_PREDICATOR_REL_NONE );
}



/*******************************************************************//**
 * Convert a relationship enumeration to string
 ***********************************************************************
 */
static void __eval_string_reldec( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // Dynamic decoding during evaluation (slow!)
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    if( (px->CSTR__str = _vxenum_rel__decode_OPEN( self->graph, (vgx_predicator_rel_enum)px->integer )) != NULL ) {
      px->type = STACK_ITEM_TYPE_CSTRING;
    }
  }
}



#define exp_cutoff  9.210340371976184  /* log( 1e4 ) */

/*******************************************************************//**
 * __exp_decay()
 * 
 * Compute and return:  Nt = N0 * exp( -lambda * t )
 * 
 * N0         : initial value at time t0
 * t          : time elapsed since time t0
 * lambda     : decay constant
 * Return Nt  : value at time t = t1 - t0
 * 
 * TODO: OPTIMIZE this to avoid the expf() call
 * 
 ***********************************************************************
 */
__inline static double __exp_decay( double N0, double t, double lambda ) {
  double x = lambda * t;
  if( x < exp_cutoff ) {
    return N0 * expf( -(float)x );   // :-( expensive math
  }
  else {
    return 0.0;
  }
}



/*******************************************************************//**
 * __exp_tmxdecay()
 * 
 * Compute exponential decay with a target expiration time.
 * 
 * N0         : intial value at time t0
 * t1         : now
 * t0         : time zero
 * tx         : expiration time when Nt = 0.0001 * N0
 * Return Nt  : value at time t = t1-t0
 * 
 * TODO: OPTIMIZE this to avoid the division
 * 
 ***********************************************************************
 */
__inline static double __exp_tmxdecay( double N0, double t1, int64_t t0, int64_t tx ) {
  double lambda = exp_cutoff / (tx - t0);     // :-( division
  return __exp_decay( N0, t1 - t0, lambda );
}



/*******************************************************************//**
 * __lin_decay()
 * 
 * Compute and return:  Nt = N0 - rate * t
 * 
 * N0         : initial value at time t0
 * t          : time elapsed since time t0
 * rate       : decay constant
 * Return Nt  : value at time t = t1 - t0
 * 
 ***********************************************************************
 */
__inline static double __lin_decay( double N0, double t, double rate ) {
  return N0 - rate * t;
}



/*******************************************************************//**
 * __lin_tmxdecay()
 * 
 * Compute linear decay with a target expiration time.
 * 
 * N0         : intial value at time t0
 * t1         : now
 * t0         : time zero
 * tx         : expiration time when Nt = 0
 * Return Nt  : value at time t = t1-t0
 * 
 * TODO: OPTIMIZE this to avoid the division
 * 
 ***********************************************************************
 */

__inline static double __lin_tmxdecay( double N0, double t1, int64_t t0, int64_t tx ) {
  double rate = N0 / (tx-t0);     // :-( division
  return __lin_decay( N0, t1 - t0, rate );
}



typedef struct __s_decayable_arc_t {
  double N0;
  int64_t t0;
  int64_t tx;
  double decay;
  vgx_predicator_rel_t rel;
} __decayable_arc;



/*******************************************************************//**
 * 
 * synarc.decay( relenc[, rate] )
 * synarc.xdecay( relenc[, rate] )
 * 
 * NOTES:
 *  This function uses the memory private working area (item offset
 *  is determined by the function call's position in the expression.)
 *  The item is formatted in a special way known to this function only:
 *    DWORD type            : relenc in upper 16 bits, 0xFFFF in lower 16 bits
 *    DWORD aux.bits        : arc TMX, initialized to 0 indicating not set
 *    uint32_t pair.uint32  : arc TMC or TMM, initialized to 0 indicating not set
 *    float pair.real32     : sum of all non-time arc values, initialized to NaN
 * 
 ***********************************************************************
 */
static int __capture_decayable_arc( vgx_Evaluator_t *self, __decayable_arc *decayable ) {

#define _type_initialized( Type )             (((DWORD)(Type) & STACK_ITEM_TYPE_INIT) == STACK_ITEM_TYPE_INIT)
#define _encode_rel_in_type( RelEnc )         (((DWORD)(RelEnc) << 16) | (DWORD)STACK_ITEM_TYPE_INIT)
#define _type_encodes_rel( Type )             (((DWORD)(Type) ^ (DWORD)STACK_ITEM_TYPE_INIT) != 0)
#define _decode_rel_in_type( Type )           (uint16_t)((DWORD)(Type) >> 16)

  static const vgx_EvalStackItem_t INIT = {
    .type = _encode_rel_in_type( VGX_PREDICATOR_REL_NONEXIST ),
    .aux  = {0},
    .pair = {
      .uint32 = 0,
      .real32 = NAN
    }
  };

// ready/reset
#define _arcdecay_ready( ItemPtr )            _type_initialized( (ItemPtr)->type )
#define _arcdecay_reset( ItemPtr )            (*(ItemPtr) = INIT)

// t0
#define _arcdecay_set_t0( ItemPtr, T0 )       (ItemPtr)->pair.uint32 = (DWORD)(T0)
#define _arcdecay_has_t0( ItemPtr )           ((ItemPtr)->pair.uint32 != 0)
#define _arcdecay_get_t0( ItemPtr )           (int64_t)(ItemPtr)->pair.uint32

// tx
#define _arcdecay_set_tx( ItemPtr, TX )       (ItemPtr)->aux.bits = (DWORD)(TX)
#define _arcdecay_has_tx( ItemPtr )           ((ItemPtr)->aux.bits != 0)
#define _arcdecay_get_tx( ItemPtr )           (int64_t)(ItemPtr)->aux.bits

// N0
#define _arcdecay_set_N0( ItemPtr, N0  )      (ItemPtr)->pair.real32 = (float)(N0)
#define _arcdecay_has_N0( ItemPtr )           (!isnan( (ItemPtr)->pair.real32 ))
#define _arcdecay_get_N0( ItemPtr )           (double)(ItemPtr)->pair.real32

// relenc
#define _arcdecay_set_rel( ItemPtr, RelEnc)   ((ItemPtr)->type = _encode_rel_in_type( RelEnc ) )
#define _arcdecay_has_rel( ItemPtr )          _type_encodes_rel( (ItemPtr)->type )
#define _arcdecay_get_rel( ItemPtr )          _decode_rel_in_type( (ItemPtr)->type )




  vgx_EvalStackItem_t *pdecay = NULL;
  vgx_EvalStackItem_t *prelenc = NULL;
  int64_t nargs = self->op->arg.integer;


  switch( nargs ) {
  case 2:
    pdecay = POP_PITEM( self );
    /* FALLTHRU */
  case 1:
    prelenc = POP_PITEM( self );
    break;
  default:
    // bad
    DISCARD_ITEMS( self, nargs );
    return 0;
  }

  vgx_EvalStackItem_t *item = vgx_evaluator_next_wreg( self );
  vgx_predicator_t pred = self->context.exit;

  // Format working item for special use
  if( !_arcdecay_ready( item ) ) {
    _arcdecay_reset( item );
  }

  // End of multiple arc: execute decay using gathered elements
  if( _vgx_predicator_is_synthetic( pred ) ) {
    if( (decayable->rel.enc = _arcdecay_get_rel( item )) != VGX_PREDICATOR_REL_NONEXIST ) {
      decayable->t0 = _arcdecay_get_t0( item );
      decayable->tx = _arcdecay_get_tx( item );
      decayable->N0 = _arcdecay_has_N0( item ) ? _arcdecay_get_N0( item ) : 1.0 * (int)( decayable->t0 > 0 || decayable->tx > 0 );
      if( pdecay ) {
        decayable->decay = pdecay->type == STACK_ITEM_TYPE_REAL ? pdecay->real : (double)pdecay->integer;
      }
      else {
        decayable->decay = 0.0;
      }
      return 1;
    }
    else {
      return 0;
    }
  }

  // Arc relationship type match
  if( pred.rel.enc == prelenc->integer ) {
    // Set t0 as M_TMC or M_TMM (the latter overrides)
    if( (pred.mod.bits == VGX_PREDICATOR_MOD_TIME_CREATED && !_arcdecay_has_t0( item ) )
        ||
        pred.mod.bits == VGX_PREDICATOR_MOD_TIME_MODIFIED
      )
    {
      _arcdecay_set_t0( item, pred.val.uinteger );
    }
    // Set tx
    else if( pred.mod.bits == VGX_PREDICATOR_MOD_TIME_EXPIRES ) {
      _arcdecay_set_tx( item, pred.val.uinteger );
    }
    // Set value (any non-time values are summed)
    else if( _vgx_predicator_value_is_float( pred ) ) {
      double v = pred.val.real;
      if( _arcdecay_has_N0( item ) ) {
        _arcdecay_set_N0( item, v + _arcdecay_get_N0( item ) );
      }
      else {
        _arcdecay_set_N0( item, v );
      }
    }
    // Capture relcode
    _arcdecay_set_rel( item, pred.rel.enc );
  }

  return 0;
#undef _type_initialized
#undef _encode_rel_in_type
#undef _type_encodes_rel
#undef _decode_rel_in_type
#undef _arcdecay_ready
#undef _arcdecay_reset
#undef _arcdecay_set_t0
#undef _arcdecay_has_t0
#undef _arcdecay_get_t0
#undef _arcdecay_set_tx
#undef _arcdecay_has_tx
#undef _arcdecay_get_tx
#undef _arcdecay_set_N0
#undef _arcdecay_get_N0

}



/*******************************************************************//**
 * 
 * synarc.xdecay( relenc[, rate] )
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_xdecay( vgx_Evaluator_t *self ) {
  __decayable_arc decayable;
  if( __capture_decayable_arc( self, &decayable ) ) {
    double Nt;
    // Use provided exponential decay rate (1/lifespan, 
    // where lifespan is seconds until 0.01% of initial value.)
    if( decayable.decay ) {
      // We have t0
      if( decayable.t0 ) {
        double t = self->current.tnow - decayable.t0;
        Nt = __exp_decay( decayable.N0, t, decayable.decay * exp_cutoff );
      }
      // We don't have t0, use tx to derive t
      else if( decayable.tx ) {
        // Trick:
        // I.    rate = 1/(tx-t0)       <- by definition, lifespan=tx-t0
        // I.    t0 = tx - 1/rate
        // II.   t = t1 - t0
        // II.   t = t1 - tx + 1/rate
        // III.  Nt = exp( -log(0.0001)*rate*t )
        // III.  Nt = exp( -log(0.0001) * (rate*t1 - rate*tx + 1) )
        // III.  Nt = exp( (rate*(t1-tx) + 1) * -log(0.0001) )
        double decay_t = decayable.decay * (self->current.tnow - decayable.tx) + 1;
        Nt = __exp_decay( decayable.N0, decay_t, exp_cutoff );
      }
      // Fallback t0 is inception time
      else {
        double t = self->current.tnow - self->current.t0;
        Nt = __exp_decay( decayable.N0, t, decayable.decay * exp_cutoff );
      }
    }
    // No decay constant, use expiration time tx
    else if( decayable.tx > decayable.t0 ) {
      // Compute decayed value Nt
      Nt = __exp_tmxdecay( decayable.N0, self->current.tnow, decayable.t0, decayable.tx );
    }
    // No expiration -> no decay
    else {
      Nt = decayable.N0;
    }
    self->context.larc->head.predicator.rel.enc = decayable.rel.enc;
    STACK_RETURN_REAL( self, Nt );
  }
  else {
    STACK_RETURN_NONE( self );
  }
}



/*******************************************************************//**
 * 
 * synarc.decay( relenc[, rate] )
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_decay( vgx_Evaluator_t *self ) {
  __decayable_arc decayable;
  if( __capture_decayable_arc( self, &decayable ) ) {
    double Nt;
    // Use provided linear decay rate (1/lifespan, 
    // where lifespan is seconds until value crosses zero.)
    if( decayable.decay ) {
      // We have t0
      if( decayable.t0 ) {
        double t = self->current.tnow - decayable.t0;
        Nt = __lin_decay( decayable.N0, t, decayable.N0 * decayable.decay );
      }
      // We don't have t0, use tx to derive t
      else if( decayable.tx ) {
        double t = self->current.tnow - decayable.tx;
        Nt = __lin_decay( 0, t, decayable.N0 * decayable.decay );
      }
      // Fallback t0 is inception time
      else {
        double t = self->current.tnow - self->current.t0;
        Nt = __lin_decay( decayable.N0, t, decayable.N0 * decayable.decay );
      }
    }
    // No decay constant, use expiration time tx
    else if( decayable.tx > decayable.t0 ) {
      // Compute decayed value Nt
      Nt = __lin_tmxdecay( decayable.N0, self->current.tnow, decayable.t0, decayable.tx );
    }
    // No expiration -> no decay
    else {
      Nt = decayable.N0;
    }
    self->context.larc->head.predicator.rel.enc = decayable.rel.enc;
    STACK_RETURN_REAL( self, Nt );
  }
  else {
    STACK_RETURN_NONE( self );
  }
}



/*******************************************************************//**
 * 
 * synarc.hasrel( type1, type2, ... ) -> bitvector
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_hasrel( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *bitvector_item = vgx_evaluator_next_wreg( self );
  vgx_predicator_t pred = self->context.exit;
  int64_t nargs = self->op->arg.integer;

  // End of multiple arc: return accumulated bitvector
  if( _vgx_predicator_is_synthetic( pred ) ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_BITVECTOR( self, bitvector_item->bits );
  }
  // Accumulate bitvector
  else {
    for( int64_t n=0; n<nargs; n++ ) {
      vgx_EvalStackItem_t *prelenc = POP_PITEM( self );
      // Bits are set right to left: rightmost reltype argument is LSB in bitvector
      if( pred.rel.enc == prelenc->integer ) {
        bitvector_item->bits |= (1ULL << n);
      }
    }
    STACK_RETURN_NONE( self );
  }
}



/*******************************************************************//**
 * 
 * synarc.hasmod( mod1, mod2, ... ) -> bitvector
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_hasmod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *bitvector_item = vgx_evaluator_next_wreg( self );
  vgx_predicator_t pred = self->context.exit;
  int64_t nargs = self->op->arg.integer;

  // End of multiple arc: return accumulated bitvector
  if( _vgx_predicator_is_synthetic( pred ) ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_BITVECTOR( self, bitvector_item->bits );
  }
  // Accumulate bitvector
  else {
    for( int64_t n=0; n<nargs; n++ ) {
      vgx_EvalStackItem_t *pmod = POP_PITEM( self );
      // Bits are set right to left: rightmost modifier argument is LSB in bitvector
      if( _vgx_predicator_as_modifier_enum( pred ) == pmod->integer ) {
        bitvector_item->bits |= (1ULL << n);
      }
    }
    STACK_RETURN_NONE( self );
  }
}



/*******************************************************************//**
 * 
 * synarc.hasrelmod( type1, mod1, type2, mod2, ... ) -> bitvector
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_hasrelmod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *bitvector_item = vgx_evaluator_next_wreg( self );
  vgx_predicator_t pred = self->context.exit;
  int64_t nargs = self->op->arg.integer;

  // End of multiple arc: return accumulated bitvector
  if( _vgx_predicator_is_synthetic( pred ) ) {
    DISCARD_ITEMS( self, nargs );
    STACK_RETURN_BITVECTOR( self, bitvector_item->bits );
  }
  // Accumulate bitvector
  else {
    int64_t npairs = nargs / 2; // any dangling reltype without modifier is ignored
    DISCARD_ITEMS( self, (nargs&1) );
    for( int64_t n=0; n<npairs; n++ ) {
      vgx_EvalStackItem_t *pmod = POP_PITEM( self );
      vgx_EvalStackItem_t *prelenc = POP_PITEM( self );
      // Relationship wildcard or match
      // Modifier wildcard or match
      if( ( prelenc->integer == 0 || pred.rel.enc == prelenc->integer )
          &&
          ( pmod->integer == 0 || _vgx_predicator_as_modifier_enum( pred ) == pmod->integer ) )
      
      {
        // Bits are set right to left: rightmost rel/mod argument pair is LSB in bitvector
        bitvector_item->bits |= (1ULL << n);
      }
    }
    STACK_RETURN_NONE( self );
  }
}



/*******************************************************************//**
 * 
 * synarc.arcvalue( type, mod ) -> synthetic arc value
 * 
 * 
 ***********************************************************************
 */
static void __eval_synarc_arcvalue( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *arcvalue_item = vgx_evaluator_next_wreg( self );
  vgx_predicator_t pred = self->context.exit;

  // End of multiple arc: return arcvalue if found
  if( _vgx_predicator_is_synthetic( pred ) ) {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_ITEM( self, arcvalue_item );
  }
  // Set arcvalue if match
  else {
    vgx_EvalStackItem_t *pmod = POP_PITEM( self );
    vgx_EvalStackItem_t *prelenc = POP_PITEM( self );
    // Relationship wildcard or match
    // Modifier wildcard or match
    if( ( prelenc->integer == 0 || pred.rel.enc == prelenc->integer )
        &&
        ( pmod->integer == 0 || _vgx_predicator_as_modifier_enum( pred ) == pmod->integer ) )
    
    {
        __get_predicator_value_as_item( pred, arcvalue_item );
    }
    STACK_RETURN_NONE( self );
  }
}




#endif
