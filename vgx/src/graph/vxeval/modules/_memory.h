/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _memory.h
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

#ifndef _VGX_VXEVAL_MODULES_MEMORY_H
#define _VGX_VXEVAL_MODULES_MEMORY_H

#include "_math.h"
#include "_conditional.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_memory_load_reg1( vgx_Evaluator_t *self );
static void __eval_memory_load_reg2( vgx_Evaluator_t *self );
static void __eval_memory_load_reg3( vgx_Evaluator_t *self );
static void __eval_memory_load_reg4( vgx_Evaluator_t *self );

static void __eval_memory_count( vgx_Evaluator_t *self );
static void __eval_memory_countif( vgx_Evaluator_t *self );
static void __eval_memory_store( vgx_Evaluator_t *self );
static void __eval_memory_rstore( vgx_Evaluator_t *self );
static void __eval_memory_storeif( vgx_Evaluator_t *self );
static void __eval_memory_rstoreif( vgx_Evaluator_t *self );
static void __eval_memory_load( vgx_Evaluator_t *self );
static void __eval_memory_rload( vgx_Evaluator_t *self );
static void __eval_memory_push( vgx_Evaluator_t *self );
static void __eval_memory_pushif( vgx_Evaluator_t *self );
static void __eval_memory_pop( vgx_Evaluator_t *self );

static void __eval_memory_write( vgx_Evaluator_t *self );
static void __eval_memory_writeif( vgx_Evaluator_t *self );
static void __eval_memory_rwrite( vgx_Evaluator_t *self );
static void __eval_memory_rwriteif( vgx_Evaluator_t *self );

static void __eval_memory_msort( vgx_Evaluator_t *self );
static void __eval_memory_msortrev( vgx_Evaluator_t *self );
static void __eval_memory_mrsort( vgx_Evaluator_t *self );
static void __eval_memory_mrsortrev( vgx_Evaluator_t *self );

static void __eval_memory_mreverse( vgx_Evaluator_t *self );

static void __eval_memory_mov( vgx_Evaluator_t *self );
static void __eval_memory_rmov( vgx_Evaluator_t *self );
static void __eval_memory_movif( vgx_Evaluator_t *self );
static void __eval_memory_rmovif( vgx_Evaluator_t *self );
static void __eval_memory_xcgh( vgx_Evaluator_t *self );
static void __eval_memory_rxcgh( vgx_Evaluator_t *self );
static void __eval_memory_xcghif( vgx_Evaluator_t *self );
static void __eval_memory_rxcghif( vgx_Evaluator_t *self );
static void __eval_memory_inc( vgx_Evaluator_t *self );
static void __eval_memory_rinc( vgx_Evaluator_t *self );
static void __eval_memory_incif( vgx_Evaluator_t *self );
static void __eval_memory_rincif( vgx_Evaluator_t *self );
static void __eval_memory_dec( vgx_Evaluator_t *self );
static void __eval_memory_rdec( vgx_Evaluator_t *self );
static void __eval_memory_decif( vgx_Evaluator_t *self );
static void __eval_memory_rdecif( vgx_Evaluator_t *self );

static void __eval_memory_equ( vgx_Evaluator_t *self );
static void __eval_memory_requ( vgx_Evaluator_t *self );
static void __eval_memory_neq( vgx_Evaluator_t *self );
static void __eval_memory_rneq( vgx_Evaluator_t *self );
static void __eval_memory_gt( vgx_Evaluator_t *self );
static void __eval_memory_rgt( vgx_Evaluator_t *self );
static void __eval_memory_gte( vgx_Evaluator_t *self );
static void __eval_memory_rgte( vgx_Evaluator_t *self );
static void __eval_memory_lt( vgx_Evaluator_t *self );
static void __eval_memory_rlt( vgx_Evaluator_t *self );
static void __eval_memory_lte( vgx_Evaluator_t *self );
static void __eval_memory_rlte( vgx_Evaluator_t *self );


static void __eval_memory_add( vgx_Evaluator_t *self );
static void __eval_memory_addif( vgx_Evaluator_t *self );
static void __eval_memory_sub( vgx_Evaluator_t *self );
static void __eval_memory_subif( vgx_Evaluator_t *self );
static void __eval_memory_mul( vgx_Evaluator_t *self );
static void __eval_memory_mulif( vgx_Evaluator_t *self );
static void __eval_memory_div( vgx_Evaluator_t *self );
static void __eval_memory_divif( vgx_Evaluator_t *self );
static void __eval_memory_mod( vgx_Evaluator_t *self );
static void __eval_memory_modif( vgx_Evaluator_t *self );
static void __eval_memory_shr( vgx_Evaluator_t *self );
static void __eval_memory_shrif( vgx_Evaluator_t *self );
static void __eval_memory_shl( vgx_Evaluator_t *self );
static void __eval_memory_shlif( vgx_Evaluator_t *self );
static void __eval_memory_and( vgx_Evaluator_t *self );
static void __eval_memory_andif( vgx_Evaluator_t *self );
static void __eval_memory_or( vgx_Evaluator_t *self );
static void __eval_memory_orif( vgx_Evaluator_t *self );
static void __eval_memory_xor( vgx_Evaluator_t *self );
static void __eval_memory_xorif( vgx_Evaluator_t *self );

static void __eval_memory_smooth( vgx_Evaluator_t *self );

static void __eval_memory_mint( vgx_Evaluator_t *self );
static void __eval_memory_mintr( vgx_Evaluator_t *self );
static void __eval_memory_mreal( vgx_Evaluator_t *self );
static void __eval_memory_mbits( vgx_Evaluator_t *self );

static void __eval_memory_minc( vgx_Evaluator_t *self );
static void __eval_memory_miinc( vgx_Evaluator_t *self );
static void __eval_memory_mrinc( vgx_Evaluator_t *self );
static void __eval_memory_mdec( vgx_Evaluator_t *self );
static void __eval_memory_midec( vgx_Evaluator_t *self );
static void __eval_memory_mrdec( vgx_Evaluator_t *self );

static void __eval_memory_madd( vgx_Evaluator_t *self );
static void __eval_memory_miadd( vgx_Evaluator_t *self );
static void __eval_memory_mradd( vgx_Evaluator_t *self );
static void __eval_memory_mvadd( vgx_Evaluator_t *self );

static void __eval_memory_msub( vgx_Evaluator_t *self );
static void __eval_memory_misub( vgx_Evaluator_t *self );
static void __eval_memory_mrsub( vgx_Evaluator_t *self );
static void __eval_memory_mvsub( vgx_Evaluator_t *self );

static void __eval_memory_mmul( vgx_Evaluator_t *self );
static void __eval_memory_mimul( vgx_Evaluator_t *self );
static void __eval_memory_mrmul( vgx_Evaluator_t *self );
static void __eval_memory_mvmul( vgx_Evaluator_t *self );

static void __eval_memory_mdiv( vgx_Evaluator_t *self );
static void __eval_memory_midiv( vgx_Evaluator_t *self );
static void __eval_memory_mrdiv( vgx_Evaluator_t *self );
static void __eval_memory_mvdiv( vgx_Evaluator_t *self );

static void __eval_memory_mmod( vgx_Evaluator_t *self );
static void __eval_memory_mimod( vgx_Evaluator_t *self );
static void __eval_memory_mrmod( vgx_Evaluator_t *self );
static void __eval_memory_mvmod( vgx_Evaluator_t *self );

static void __eval_memory_minv( vgx_Evaluator_t *self );
static void __eval_memory_mrinv( vgx_Evaluator_t *self );

static void __eval_memory_mpow( vgx_Evaluator_t *self );
static void __eval_memory_mrpow( vgx_Evaluator_t *self );
static void __eval_memory_msq( vgx_Evaluator_t *self );
static void __eval_memory_mrsq( vgx_Evaluator_t *self );
static void __eval_memory_msqrt( vgx_Evaluator_t *self );
static void __eval_memory_mrsqrt( vgx_Evaluator_t *self );

static void __eval_memory_mceil( vgx_Evaluator_t *self );
static void __eval_memory_mrceil( vgx_Evaluator_t *self );
static void __eval_memory_mfloor( vgx_Evaluator_t *self );
static void __eval_memory_mrfloor( vgx_Evaluator_t *self );
static void __eval_memory_mround( vgx_Evaluator_t *self );
static void __eval_memory_mrround( vgx_Evaluator_t *self );
static void __eval_memory_mabs( vgx_Evaluator_t *self );
static void __eval_memory_mrabs( vgx_Evaluator_t *self );
static void __eval_memory_msign( vgx_Evaluator_t *self );
static void __eval_memory_mrsign( vgx_Evaluator_t *self );

static void __eval_memory_mpopcnt( vgx_Evaluator_t *self );

static void __eval_memory_mlog2( vgx_Evaluator_t *self );
static void __eval_memory_mrlog2( vgx_Evaluator_t *self );
static void __eval_memory_mlog( vgx_Evaluator_t *self );
static void __eval_memory_mrlog( vgx_Evaluator_t *self );
static void __eval_memory_mlog10( vgx_Evaluator_t *self );
static void __eval_memory_mrlog10( vgx_Evaluator_t *self );

static void __eval_memory_mexp2( vgx_Evaluator_t *self );
static void __eval_memory_mrexp2( vgx_Evaluator_t *self );
static void __eval_memory_mexp( vgx_Evaluator_t *self );
static void __eval_memory_mrexp( vgx_Evaluator_t *self );
static void __eval_memory_mexp10( vgx_Evaluator_t *self );
static void __eval_memory_mrexp10( vgx_Evaluator_t *self );

static void __eval_memory_mrad( vgx_Evaluator_t *self );
static void __eval_memory_mrrad( vgx_Evaluator_t *self );
static void __eval_memory_mdeg( vgx_Evaluator_t *self );
static void __eval_memory_mrdeg( vgx_Evaluator_t *self );
static void __eval_memory_msin( vgx_Evaluator_t *self );
static void __eval_memory_mrsin( vgx_Evaluator_t *self );
static void __eval_memory_mcos( vgx_Evaluator_t *self );
static void __eval_memory_mrcos( vgx_Evaluator_t *self );
static void __eval_memory_mtan( vgx_Evaluator_t *self );
static void __eval_memory_mrtan( vgx_Evaluator_t *self );
static void __eval_memory_masin( vgx_Evaluator_t *self );
static void __eval_memory_mrasin( vgx_Evaluator_t *self );
static void __eval_memory_macos( vgx_Evaluator_t *self );
static void __eval_memory_mracos( vgx_Evaluator_t *self );
static void __eval_memory_matan( vgx_Evaluator_t *self );
static void __eval_memory_mratan( vgx_Evaluator_t *self );
static void __eval_memory_msinh( vgx_Evaluator_t *self );
static void __eval_memory_mrsinh( vgx_Evaluator_t *self );
static void __eval_memory_mcosh( vgx_Evaluator_t *self );
static void __eval_memory_mrcosh( vgx_Evaluator_t *self );
static void __eval_memory_mtanh( vgx_Evaluator_t *self );
static void __eval_memory_mrtanh( vgx_Evaluator_t *self );
static void __eval_memory_masinh( vgx_Evaluator_t *self );
static void __eval_memory_mrasinh( vgx_Evaluator_t *self );
static void __eval_memory_macosh( vgx_Evaluator_t *self );
static void __eval_memory_mracosh( vgx_Evaluator_t *self );
static void __eval_memory_matanh( vgx_Evaluator_t *self );
static void __eval_memory_mratanh( vgx_Evaluator_t *self );
static void __eval_memory_msinc( vgx_Evaluator_t *self );
static void __eval_memory_mrsinc( vgx_Evaluator_t *self );



static void __eval_memory_mshr( vgx_Evaluator_t *self );
static void __eval_memory_mvshr( vgx_Evaluator_t *self );
static void __eval_memory_mshl( vgx_Evaluator_t *self );
static void __eval_memory_mvshl( vgx_Evaluator_t *self );

static void __eval_memory_mand( vgx_Evaluator_t *self );
static void __eval_memory_mvand( vgx_Evaluator_t *self );
static void __eval_memory_mor( vgx_Evaluator_t *self );
static void __eval_memory_mvor( vgx_Evaluator_t *self );
static void __eval_memory_mxor( vgx_Evaluator_t *self );
static void __eval_memory_mvxor( vgx_Evaluator_t *self );

static void __eval_memory_mset( vgx_Evaluator_t *self );
static void __eval_memory_mreset( vgx_Evaluator_t *self );
static void __eval_memory_mcopy( vgx_Evaluator_t *self );
static void __eval_memory_mpwrite( vgx_Evaluator_t *self );
static void __eval_memory_mcopyobj( vgx_Evaluator_t *self );
static void __eval_memory_mterm( vgx_Evaluator_t *self );
static void __eval_memory_mlen( vgx_Evaluator_t *self );


static void __eval_memory_mrandomize( vgx_Evaluator_t *self );
static void __eval_memory_mrandbits( vgx_Evaluator_t *self );
static void __eval_memory_mhash( vgx_Evaluator_t *self );
static void __eval_memory_msum( vgx_Evaluator_t *self );
static void __eval_memory_mrsum( vgx_Evaluator_t *self );
static void __eval_memory_msumsqr( vgx_Evaluator_t *self );
static void __eval_memory_mrsumsqr( vgx_Evaluator_t *self );
static void __eval_memory_mstdev( vgx_Evaluator_t *self );
static void __eval_memory_mrstdev( vgx_Evaluator_t *self );
static void __eval_memory_minvsum( vgx_Evaluator_t *self );
static void __eval_memory_mrinvsum( vgx_Evaluator_t *self );
static void __eval_memory_mprod( vgx_Evaluator_t *self );
static void __eval_memory_mrprod( vgx_Evaluator_t *self );
static void __eval_memory_mmean( vgx_Evaluator_t *self );
static void __eval_memory_mrmean( vgx_Evaluator_t *self );
static void __eval_memory_mharmmean( vgx_Evaluator_t *self );
static void __eval_memory_mrharmmean( vgx_Evaluator_t *self );
static void __eval_memory_mgeomean( vgx_Evaluator_t *self );
static void __eval_memory_mrgeomean( vgx_Evaluator_t *self );
static void __eval_memory_mgeostdev( vgx_Evaluator_t *self );
static void __eval_memory_mrgeostdev( vgx_Evaluator_t *self );

static void __eval_memory_mmax( vgx_Evaluator_t *self );
static void __eval_memory_mmin( vgx_Evaluator_t *self );
static void __eval_memory_mcontains( vgx_Evaluator_t *self );
static void __eval_memory_mcount( vgx_Evaluator_t *self );
static void __eval_memory_mindex( vgx_Evaluator_t *self );

static void __eval_memory_mcmp( vgx_Evaluator_t *self );
static void __eval_memory_mcmpa( vgx_Evaluator_t *self );

static void __eval_memory_msubset( vgx_Evaluator_t *self );
static void __eval_memory_msubsetobj( vgx_Evaluator_t *self );
static void __eval_memory_msumprodobj( vgx_Evaluator_t *self );

static void __eval_memory_modindex( vgx_Evaluator_t *self );

static void __eval_memory_index( vgx_Evaluator_t *self );
static void __eval_memory_indexed( vgx_Evaluator_t *self );
static void __eval_memory_unindex( vgx_Evaluator_t *self );



__inline static double __radians( double deg ) {
  return RADIANS( deg );
}

__inline static double __degrees( double rad ) {
  return DEGREES( rad );
}

__inline static double __rinv( double x ) {
  return x != 0 ? 1.0 / x : FLT_MAX;
}

__inline static int64_t __isq( int64_t x ) {
  return x * x;
}

__inline static double __rsq( double x ) {
  return x * x;
}

#if defined CXPLAT_WINDOWS_X64 || defined CXPLAT_MAC_ARM64
__inline static double exp10( double x ) {
  return exp( x * M_LN10 );
}
#endif

__inline static double __sinc( double x ) {
  if( x ) {
    x *= M_PI;
    return sin( x ) / x;
  }
  else {
    return 1;
  }
}


__inline static int64_t __isign( int64_t x ) {
  return (x > 0LL) - (x < 0LL);
}


__inline static double __rsign( double x ) {
  return (x > 0.0) - (x < 0.0);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline double __cosine( double dp, double norm_ab ) {
  // Cosine
  if( norm_ab > 0.0 && norm_ab >= fabs(dp) ) {
    // Cosine
    return dp / norm_ab;
  }
  else if( dp > 0.0 ) {
    return 1.0;
  }
  else if( dp < 0.0 ) {
    return -1.0;
  }
  else {
    return 0.0;
  }
}



#define BEGIN_ARRAY_PROCESS( Evaluator, Cursor )  \
  vgx_Evaluator_t *__self = Evaluator;            \
  vgx_EvalStackItem_t *__p, *__end;                   \
  int64_t __n = __slice( __self, &__p, &__end );  \
  vgx_EvalStackItem_t *Cursor;                        \
  while( (Cursor=__p++) < __end )


#define END_ARRAY_PROCESS                         \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( __self );   \
  SET_INTEGER_PITEM_VALUE( __px, __n )


#define SKIP_ARRAY_ITEM (--__n)


#define ARRAY_APPLY_REAL_UNARY( Evaluator, Function ) \
  BEGIN_ARRAY_PROCESS( Evaluator, vx ) {              \
    vx->real = Function( vx->real );                  \
  } END_ARRAY_PROCESS


#define ARRAY_APPLY_REAL_BINARY( Evaluator, Function, Y ) \
  BEGIN_ARRAY_PROCESS( Evaluator, vx ) {                  \
    vx->real = Function( vx->real, Y );                   \
  } END_ARRAY_PROCESS


#define SAVE_STACK_1( Evaluator )                   \
  do {                                              \
    vgx_Evaluator_t *__self = Evaluator;            \
    vgx_EvalStackItem_t __item_1 = POP_ITEM( __self );  \

#define RESTORE_STACK_1                             \
    *NEXT_PITEM( __self ) = __item_1;               \
  } WHILE_ZERO



__inline static int __is_stack_item_storable( vgx_Evaluator_t *self, const vgx_EvalStackItem_t *item, bool incref ) {
  if( __is_stack_item_type_object( item->type ) ) {
    // bitvector or keyval
    if( (item->type & (STACK_ITEM_TYPE_INTEGER | STACK_ITEM_TYPE_REAL)) != 0 ) {
      return 1;
    }
    // maybe safe cstring, or if not item is not storable
    else if( item->type == STACK_ITEM_TYPE_CSTRING && item->CSTR__str->allocator_context == self->graph->ephemeral_string_allocator_context ) {
      if( incref ) {
        iEvaluator.StoreCString( self, item->CSTR__str );
      }
      return 1;
    }
    //
    else if( item->type == STACK_ITEM_TYPE_VECTOR ) {
      if( incref ) {
        iEvaluator.StoreVector( self, item->vector );
      }
      return 1;
    }
    return 0;
  }
  // numeric
  else {
    return 1; // numeric
  }
}






/*******************************************************************//**
 * __slice
 ***********************************************************************
 */
__inline static int64_t __slice( vgx_Evaluator_t *self, vgx_EvalStackItem_t **start, vgx_EvalStackItem_t **end ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t src_n = POP_ITEM( self );
  vgx_EvalStackItem_t src_1 = POP_ITEM( self );
  int64_t xn = src_n.integer & mem->mask;
  int64_t x1 = src_1.integer & mem->mask;
  *start = mem->data + x1;
  *end = mem->data + (xn+1);
  return *end > *start ? *end - *start : 0;
}



/*******************************************************************//**
 * __slice_cast_real
 ***********************************************************************
 */
__inline static void __slice_cast_real( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t xn = (GET_PITEM( self )->integer & mem->mask);
  int64_t x1 = ((GET_PITEM( self )-1)->integer & mem->mask);
  vgx_EvalStackItem_t *cursor = mem->data + x1;
  vgx_EvalStackItem_t *end = mem->data + (xn+1);
  while( cursor < end ) {
    if( cursor->type != STACK_ITEM_TYPE_REAL ) {
      SET_REAL_PITEM_VALUE( cursor, (double)cursor->integer );
    }
    ++cursor;
  }
}



/*******************************************************************//**
 * 
 *  __contiguous( A, B, n )
 *  
 ***********************************************************************
 */
static int64_t __contiguous( vgx_Evaluator_t *self, vgx_EvalStackItem_t **A, vgx_EvalStackItem_t **B ) {
  vgx_EvalStackItem_t *pn = POP_PITEM( self );
  vgx_EvalStackItem_t *pB = POP_PITEM( self );
  vgx_EvalStackItem_t *pA = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  int64_t sz = 1LL << mem->order;
  vgx_EvalStackItem_t *pm = mem->data;

  int64_t n = pn->integer;
  int64_t b = pB->integer & mask;
  int64_t a = pA->integer & mask;

  // Error: negative length or A or B extends beyond end of memory
  if( n < 0 || a + n > sz || b + n > sz ) {
    return -1;
  }
  // Zero length
  else if( n == 0 ) {
    return 0;
  }

  *A = pm + a;
  *B = pm + b;

  return n;
}



/*******************************************************************//**
 * r1
 ***********************************************************************
 */
static void __eval_memory_load_reg1( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  *px = mem->data[ mem->mask ];
}



/*******************************************************************//**
 * r2
 ***********************************************************************
 */
static void __eval_memory_load_reg2( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  *px = mem->data[ mem->mask-1 ];
}



/*******************************************************************//**
 * r3
 ***********************************************************************
 */
static void __eval_memory_load_reg3( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  *px = mem->data[ mem->mask-2 ];
}



/*******************************************************************//**
 * r4
 ***********************************************************************
 */
static void __eval_memory_load_reg4( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  *px = mem->data[ mem->mask-3 ];
}



/*******************************************************************//**
 * count( dest, ... )
 ***********************************************************************
 */
static void __eval_memory_count( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  // Consume and discard all arguments except first
  while( nargs > 1 ) {
    POP_PITEM( self );
    --nargs;
  }
  if( nargs > 0 ) {
    vgx_EvalStackItem_t *px = GET_PITEM( self );
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
    mx->type = STACK_ITEM_TYPE_INTEGER;
    mx->integer++;
    *px = *mx;
  }
}



/*******************************************************************//**
 * countif( condition, dest, ... ) -> condition
 ***********************************************************************
 */
static void __eval_memory_countif( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  // Consume and discard all arguments except two first
  while( nargs > 2 ) {
    POP_PITEM( self );
    --nargs;
  }
  // Two args remain  
  if( nargs > 1 ) {
    vgx_EvalStackItem_t *px = POP_PITEM( self );
    if( __condition( GET_PITEM( self ) ) ) {
      vgx_ExpressEvalMemory_t *mem = self->context.memory;
      vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
      mx->type = STACK_ITEM_TYPE_INTEGER;
      mx->integer++;
      *px = *mx;
    }
  }
}



/*******************************************************************//**
 * store( index, value ) -> value
 ***********************************************************************
 */
static void __eval_memory_store( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int64_t idx = px->integer & mem->mask;
  if( __is_stack_item_storable( self, &value, true ) ) {
    mem->data[ idx ] = value;
    *px = value;
  }
  else {
    //  TODO: This won't let us store( x, null ) 
    //  which is something we may want to be able
    //  to do since null is the min/max item for heaps
    //  and is also a special value used by msort()
    //
    //
    //
    //

    *px = SET_MEM_NAN( mem->data, idx );
  }
}



/*******************************************************************//**
 * rstore( [index], value ) -> value
 ***********************************************************************
 */
static void __eval_memory_rstore( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int64_t idx = data[ px->integer & mask ].integer & mask;
  if( __is_stack_item_storable( self, &value, true ) ) {
    data[ idx ] = value;
    *px = value;
  }
  else {
    *px = SET_MEM_NAN( data, idx );
  }
}



/*******************************************************************//**
 * storeif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_storeif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    int64_t idx = index.integer & mem->mask;
    if( __is_stack_item_storable( self, &value, true ) ) {
      mem->data[ idx ] = value;
    }
    else {
      SET_MEM_NAN( mem->data, idx );
    }
  }
}



/*******************************************************************//**
 * rstoreif( condition, [index], value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_rstoreif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *data = mem->data;
    uint64_t mask = mem->mask;
    int64_t idx = data[ index.integer & mask ].integer & mask;
    if( __is_stack_item_storable( self, &value, true ) ) {
      data[ idx ] = value;
    }
    else {
      SET_MEM_NAN( data, idx );
    }
  }
}



/*******************************************************************//**
 * load( index ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_load( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  *px = mem->data[ px->integer & mem->mask ];
}



/*******************************************************************//**
 * rload( [index] ) -> [[index]]
 ***********************************************************************
 */
static void __eval_memory_rload( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  *px = data[ data[ px->integer & mask ].integer & mask ];
}



/*******************************************************************//**
 * push( value ) -> value
 ***********************************************************************
 */
static void __eval_memory_push( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t idx = mem->sp-- & mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( __is_stack_item_storable( self, px, true ) ) {
    mem->data[ idx ] = *px;
  }
  else {
    *px = SET_MEM_NAN( mem->data, idx );
  }
}



/*******************************************************************//**
 * pushif( condition, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_pushif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    int64_t idx = mem->sp-- & mem->mask;
    if( __is_stack_item_storable( self, &value, true ) ) {
      mem->data[ idx ] = value;
    }
    else {
      SET_MEM_NAN( mem->data, idx );
    }
  }
}



/*******************************************************************//**
 * pop() -> [sp]
 ***********************************************************************
 */
static void __eval_memory_pop( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  *px = mem->data[ ++(mem->sp) & mem->mask ];
}



/*******************************************************************//**
 * popif( condition ) -> [sp]
 ***********************************************************************
 */
static void __eval_memory_popif( vgx_Evaluator_t *self ) {
  if( __condition( POP_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *px = NEXT_PITEM( self );
    *px = mem->data[ ++(mem->sp) & mem->mask ];
  }
  else {
    vgx_EvalStackItem_t *px = NEXT_PITEM( self );
    SET_NONE( px );
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t __memory_write( vgx_Evaluator_t *self, int64_t nargs ) {
  int64_t n = nargs - 1;
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self ) - n;
  int64_t cursor = px->integer & mask;
  int64_t remain = n;
  while( remain-- > 0 ) {
    vgx_EvalStackItem_t *w = ++px;
    int64_t idx = cursor++ & mask;
    if( __is_stack_item_storable( self, w, true ) ) {
      data[ idx ] = *w;
    }
    else {
      SET_NONE( data + idx );
    }
  }
  return n;
}



/*******************************************************************//**
 * write( index, ... ) -> n
 ***********************************************************************
 */
static void __eval_memory_write( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int64_t n = 0;
  if( nargs > 0 ) {
    n = __memory_write( self, nargs );
    DISCARD_ITEMS( self, nargs );
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * writeif( condition, index, ... ) -> condition
 ***********************************************************************
 */
static void __eval_memory_writeif( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  if( --nargs > 0 ) {
    vgx_EvalStackItem_t *cx = GET_PITEM( self ) - nargs;
    if( __condition( cx ) ) {
      __memory_write( self, nargs );
    }
    DISCARD_ITEMS( self, nargs );
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t __memory_rwrite( vgx_Evaluator_t *self, int64_t nargs ) {
  int64_t n = nargs - 1;
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self ) - n;
  int64_t r = px->integer & mask;
  vgx_EvalStackItem_t *pr = &data[ r ];
  int64_t rval = pr->integer;
  int64_t cursor = rval;
  while( --nargs > 0 ) {
    vgx_EvalStackItem_t *w = ++px;
    int64_t idx = cursor++ & mask;
    if( __is_stack_item_storable( self, w, true ) ) {
      data[ idx ] = *w;
    }
    else {
      SET_NONE( data + idx );
    }
  }
  SET_INTEGER_PITEM_VALUE( pr, rval + n );
  return n;
}



/*******************************************************************//**
 * rwrite( [index], ... ) -> n
 ***********************************************************************
 */
static void __eval_memory_rwrite( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  int64_t n = 0;
  if( nargs > 0 ) {
    n = __memory_rwrite( self, nargs );
    DISCARD_ITEMS( self, nargs );
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * rwriteif( condition, [index], ... ) -> n
 ***********************************************************************
 */
static void __eval_memory_rwriteif( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  if( --nargs > 0 ) {
    vgx_EvalStackItem_t *cx = GET_PITEM( self ) - nargs;
    if( __condition( cx ) ) {
      __memory_rwrite( self, nargs );
    }
    DISCARD_ITEMS( self, nargs );
  }
}




// for qsort
typedef int (*fcompare)( const void *, const void *);



/*******************************************************************//**
 * Returns: 
 *          1 if a > b
 *          0 if a == b
 *         -1 if a < b
 ***********************************************************************
 */
__inline static int __cmpsort_asc( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return pb->type != STACK_ITEM_TYPE_NONE; // (1) a is inf and thus always greater than b unless (0) b is also inf
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return -1; // a is less than inf and thus always less than b when b is inf
  }
  if( __gt( NULL, pa, pb ) ) {
    return 1;
  }
  if( __lt( NULL, pa, pb ) ) {
    return -1;
  }
  return 0;
}



/*******************************************************************//**
 * Returns: 
 *          1 if a < b
 *          0 if a == b
 *         -1 if a > b
 ***********************************************************************
 */
__inline static int __cmpsort_desc( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  if( pa->type == STACK_ITEM_TYPE_NONE ) {
    return pb->type != STACK_ITEM_TYPE_NONE; // (1) a is -inf and thus always less than b unless (0) b is also inf
  }
  if( pb->type == STACK_ITEM_TYPE_NONE ) {
    return -1; // a is greater than -inf and thus always greater than b when b is -inf
  }
  if( __lt( NULL, pa, pb ) ) {
    return 1;
  }
  if( __gt( NULL, pa, pb ) ) {
    return -1;
  }
  return 0;
}



/*******************************************************************//**
 * Returns: 
 *          1 if a > b
 *          0 if a == b
 *         -1 if a < b
 ***********************************************************************
 */
__inline static int __real_cmpsort_asc( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  return (pa->real > pb->real) - (pa->real < pb->real);
}



/*******************************************************************//**
 * Returns: 
 *          1 if a < b
 *          0 if a == b
 *         -1 if a > b
 ***********************************************************************
 */
__inline static int __real_cmpsort_desc( vgx_EvalStackItem_t *pa, vgx_EvalStackItem_t *pb ) {
  return (pa->real < pb->real) - (pa->real > pb->real);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __stack_sort( vgx_Evaluator_t *self, fcompare compare ) {
  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;

  int64_t x = px->integer;
  int64_t k = pk->integer;
  if( x + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    qsort( pmem + x, k, sizeof( vgx_EvalStackItem_t ), compare );
    // Search for first null
    //
    // a                                                    b
    // 0                                                    53
    // xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    //                           ^ 26
    //                                        ^ 39
    //                                               ^ 46
    //                                                  ^ 49
    //                                                    ^ 51
    //                                                     ^ 52
    //                       
    //                      
    int64_t a = x;
    int64_t b = k;
    int64_t i;
    while( (i = (a+b)/2) != a ) {
      // Too far
      if( pmem[i].type == STACK_ITEM_TYPE_NONE ) {
        b = i;
      }
      // Not far enough
      else {
        a = i;
      }
    }
    if( pmem[i].type != STACK_ITEM_TYPE_NONE && i < k ) {
      ++i;
    }
    STACK_RETURN_INTEGER( self, i );
  }
}



/*******************************************************************//**
 * msort( index, k ) -> n_numbers
 ***********************************************************************
 */
static void __eval_memory_msort( vgx_Evaluator_t *self ) {
  __stack_sort( self, (fcompare)__cmpsort_asc );
}



/*******************************************************************//**
 * msortrev( index, k ) -> n_numbers
 ***********************************************************************
 */
static void __eval_memory_msortrev( vgx_Evaluator_t *self ) {
  __stack_sort( self, (fcompare)__cmpsort_desc );
}



/*******************************************************************//**
 * mrsort( index, k ) -> k
 ***********************************************************************
 */
static void __eval_memory_mrsort( vgx_Evaluator_t *self ) {
  __stack_sort( self, (fcompare)__real_cmpsort_asc );
}



/*******************************************************************//**
 * mrsortrev( index, k ) -> k
 ***********************************************************************
 */
static void __eval_memory_mrsortrev( vgx_Evaluator_t *self ) {
  __stack_sort( self, (fcompare)__real_cmpsort_desc );
}



/*******************************************************************//**
 * mreverse( index, k ) -> k
 ***********************************************************************
 */
static void __eval_memory_mreverse( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *pk = POP_PITEM( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t msz = 1LL << mem->order;
  vgx_EvalStackItem_t *pmem = mem->data;

  int64_t x = px->integer;
  int64_t k = pk->integer;
  if( x + k > msz || k < 1 ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    vgx_EvalStackItem_t *pa = pmem + x;
    vgx_EvalStackItem_t *pb = pmem + x + (k-1);
    while( pa < pb ) {
      // Swap
      vgx_EvalStackItem_t tmp = *pa;
      *pa = *pb;
      *pb = tmp;
      // next
      ++pa;
      --pb;
    }
    STACK_RETURN_INTEGER( self, k );
  }
}




/*******************************************************************//**
 * mov( dest, src ) -> [dest]
 ***********************************************************************
 */
static void __eval_memory_mov( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t src = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *value = &mem->data[ src.integer & mask ];
  mem->data[ px->integer & mask ] = *value;
  *px = *value;
}



/*******************************************************************//**
 * rmov( [dest], [src] ) -> [[dest]]
 ***********************************************************************
 */
static void __eval_memory_rmov( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t src = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *value = &data[ data[ src.integer & mask ].integer & mask ];
  data[ data[ px->integer & mask ].integer & mask ] = *value;
  *px = *value;
}



/*******************************************************************//**
 * movif( condition, dest, src ) -> condition
 ***********************************************************************
 */
static void __eval_memory_movif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t src = POP_ITEM( self );
  vgx_EvalStackItem_t dest = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    uint64_t mask = mem->mask;
    mem->data[ dest.integer & mask ] = mem->data[ src.integer & mask ];
  }
}



/*******************************************************************//**
 * rmovif( condition, [dest], [src] ) -> condition
 ***********************************************************************
 */
static void __eval_memory_rmovif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t src = POP_ITEM( self );
  vgx_EvalStackItem_t dest = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *data = mem->data;
    uint64_t mask = mem->mask;
    data[ data[ dest.integer & mask ].integer & mask ] = data[ data[ src.integer & mask ].integer & mask ];
  }
}



/*******************************************************************//**
 * xchg( addr1, addr2 ) -> [addr1]
 ***********************************************************************
 */
static void __eval_memory_xchg( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t a2 = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t val2 = mem->data[ a2.integer & mask ];
  mem->data[ a2.integer & mask ] = mem->data[ px->integer & mask ];
  mem->data[ px->integer & mask ] = val2;
  *px = val2;
}



/*******************************************************************//**
 * rxchg( [addr1], [addr2] ) -> [[addr1]]
 ***********************************************************************
 */
static void __eval_memory_rxchg( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t a2 = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t val2 = data[ data[ a2.integer & mask ].integer & mask ];
  data[ data[ a2.integer & mask ].integer & mask ] = data[ data[ px->integer & mask ].integer & mask ];
  data[ data[ px->integer & mask ].integer & mask ] = val2;
  *px = val2;
}



/*******************************************************************//**
 * xchgif( condition, addr1, addr2 ) -> condition
 ***********************************************************************
 */
static void __eval_memory_xchgif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t a2 = POP_ITEM( self );
  vgx_EvalStackItem_t a1 = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    uint64_t mask = mem->mask;
    vgx_EvalStackItem_t val2 = mem->data[ a2.integer & mask ];
    mem->data[ a2.integer & mask ] = mem->data[ a1.integer & mask ];
    mem->data[ a1.integer & mask ] = val2;
  }
}



/*******************************************************************//**
 * rxchgif( condition, [addr1], [addr2] ) -> condition
 ***********************************************************************
 */
static void __eval_memory_rxchgif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t a2 = POP_ITEM( self );
  vgx_EvalStackItem_t a1 = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *data = mem->data;
    uint64_t mask = mem->mask;
    vgx_EvalStackItem_t val2 = data[ data[ a2.integer & mask ].integer & mask ];
    data[ data[ a2.integer & mask ].integer & mask ] = data[ data[ a1.integer & mask ].integer & mask ];
    data[ data[ a1.integer & mask ].integer & mask ] = val2;
  }
}



/*******************************************************************//**
 * get() -> [sp]
 ***********************************************************************
 */
static void __eval_memory_get( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  *px = mem->data[ (mem->sp+1) & mem->mask ];
}



/*******************************************************************//**
 * inc( index ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_inc( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  switch( mx->type  ) {
  case STACK_ITEM_TYPE_INTEGER:
    mx->integer++;
    break;
  case STACK_ITEM_TYPE_REAL:
    mx->real += 1.0;
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * rinc( [index] ) -> [[index]]
 ***********************************************************************
 */
static void __eval_memory_rinc( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &data[ data[ px->integer & mask ].integer & mask ];
  switch( mx->type  ) {
  case STACK_ITEM_TYPE_INTEGER:
    mx->integer++;
    break;
  case STACK_ITEM_TYPE_REAL:
    mx->real += 1.0;
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * incif( condition, index ) -> condition
 ***********************************************************************
 */
static void __eval_memory_incif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    switch( mx->type  ) {
    case STACK_ITEM_TYPE_INTEGER:
      mx->integer++;
      break;
    case STACK_ITEM_TYPE_REAL:
      mx->real += 1.0;
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * rincif( condition, [index] ) -> condition
 ***********************************************************************
 */
static void __eval_memory_rincif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *data = mem->data;
    uint64_t mask = mem->mask;
    vgx_EvalStackItem_t *mx = &data[ data[ index.integer & mask ].integer & mask ];
    switch( mx->type  ) {
    case STACK_ITEM_TYPE_INTEGER:
      mx->integer++;
      break;
    case STACK_ITEM_TYPE_REAL:
      mx->real += 1.0;
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * dec( index ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_dec( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  switch( mx->type  ) {
  case STACK_ITEM_TYPE_INTEGER:
    mx->integer--;
    break;
  case STACK_ITEM_TYPE_REAL:
    mx->real -= 1.0;
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * rdec( [index] ) -> [[index]]
 ***********************************************************************
 */
static void __eval_memory_rdec( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &data[ data[ px->integer & mask ].integer & mask ];
  switch( mx->type  ) {
  case STACK_ITEM_TYPE_INTEGER:
    mx->integer--;
    break;
  case STACK_ITEM_TYPE_REAL:
    mx->real -= 1.0;
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * decif( condition, index ) -> condition
 ***********************************************************************
 */
static void __eval_memory_decif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    switch( mx->type  ) {
    case STACK_ITEM_TYPE_INTEGER:
      mx->integer--;
      break;
    case STACK_ITEM_TYPE_REAL:
      mx->real -= 1.0;
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * rdecif( condition, [index] ) -> condition
 ***********************************************************************
 */
static void __eval_memory_rdecif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *data = mem->data;
    uint64_t mask = mem->mask;
    vgx_EvalStackItem_t *mx = &data[ data[ index.integer & mask ].integer & mask ];
    switch( mx->type  ) {
    case STACK_ITEM_TYPE_INTEGER:
      mx->integer--;
      break;
    case STACK_ITEM_TYPE_REAL:
      mx->real -= 1.0;
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * equ( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_equ( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __equ( self, ma, mb ) );
}



/*******************************************************************//**
 * requ( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_requ( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __equ( self, rma, rmb ) );
}



/*******************************************************************//**
 * neq( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_neq( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __neq( self, ma, mb ) );
}



/*******************************************************************//**
 * rneq( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_rneq( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __neq( self, rma, rmb ) );
}



/*******************************************************************//**
 * gt( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_gt( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __gt( self, ma, mb ) );
}



/*******************************************************************//**
 * rgt( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_rgt( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __gt( self, rma, rmb ) );
}



/*******************************************************************//**
 * gte( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_gte( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __gte( self, ma, mb ) );
}



/*******************************************************************//**
 * rgte( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_rgte( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __gte( self, rma, rmb ) );
}



/*******************************************************************//**
 * lt( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_lt( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __lt( self, ma, mb ) );
}



/*******************************************************************//**
 * rlt( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_rlt( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __lt( self, rma, rmb ) );
}



/*******************************************************************//**
 * lte( ax, bx ) -> bool
 ***********************************************************************
 */
static void __eval_memory_lte( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *ma = &data[ ax->integer & mask ];
  vgx_EvalStackItem_t *mb = &data[ bx.integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __lte( self, ma, mb ) );
}



/*******************************************************************//**
 * rlte( [ax], [bx] ) -> bool
 ***********************************************************************
 */
static void __eval_memory_rlte( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *data = mem->data;
  uint64_t mask = mem->mask;
  vgx_EvalStackItem_t bx = POP_ITEM( self );
  vgx_EvalStackItem_t *ax = GET_PITEM( self );
  vgx_EvalStackItem_t *rma = &data[ data[ ax->integer & mask ].integer & mask ];
  vgx_EvalStackItem_t *rmb = &data[ data[ bx.integer & mask ].integer & mask ];
  SET_INTEGER_PITEM_VALUE( ax, __lte( self, rma, rmb ) );
}



/*******************************************************************//**
 * add( index, value ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_add( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &y );
  switch( pair_type  ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    ADD_INTEGER_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( mx, (double)mx->integer + y.real );
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    ADD_REAL_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    ADD_REAL_PITEM_VALUE( mx, y.real );
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * addif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_addif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
    switch( pair_type  ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      ADD_INTEGER_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XINT_YREA:
      SET_REAL_PITEM_VALUE( mx, (double)mx->integer + value.real );
      break;
    case STACK_PAIR_TYPE_XREA_YINT:
      ADD_REAL_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XREA_YREA:
      ADD_REAL_PITEM_VALUE( mx, value.real );
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * sub( index, value ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_sub( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &y );
  switch( pair_type  ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    SUB_INTEGER_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( mx, (double)mx->integer - y.real );
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    SUB_REAL_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    SUB_REAL_PITEM_VALUE( mx, y.real );
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * subif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_subif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
    switch( pair_type  ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      SUB_INTEGER_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XINT_YREA:
      SET_REAL_PITEM_VALUE( mx, (double)mx->integer - value.real );
      break;
    case STACK_PAIR_TYPE_XREA_YINT:
      SUB_REAL_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XREA_YREA:
      SUB_REAL_PITEM_VALUE( mx, value.real );
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * mul( index, value ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_mul( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &y );
  switch( pair_type  ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    MUL_INTEGER_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    SET_REAL_PITEM_VALUE( mx, (double)mx->integer * y.real );
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    MUL_REAL_PITEM_VALUE( mx, y.integer );
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    MUL_REAL_PITEM_VALUE( mx, y.real );
    break;
  default:
    break;
  }
  *px = *mx;
}



/*******************************************************************//**
 * mulif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_mulif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
    switch( pair_type  ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      MUL_INTEGER_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XINT_YREA:
      SET_REAL_PITEM_VALUE( mx, (double)mx->integer * value.real );
      break;
    case STACK_PAIR_TYPE_XREA_YINT:
      MUL_REAL_PITEM_VALUE( mx, value.integer );
      break;
    case STACK_PAIR_TYPE_XREA_YREA:
      MUL_REAL_PITEM_VALUE( mx, value.real );
      break;
    default:
      break;
    }
  }
}



/*******************************************************************//**
 * div( index, value ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_div( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  if( y.bits == 0 ) {
    SET_NAN( mx );
  }
  else {
    vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &y );
    switch( pair_type  ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      DIV_INTEGER_PITEM_VALUE( mx, y.integer );
      break;
    case STACK_PAIR_TYPE_XINT_YREA:
      SET_REAL_PITEM_VALUE( mx, (double)mx->integer / y.real );
      break;
    case STACK_PAIR_TYPE_XREA_YINT:
      DIV_REAL_PITEM_VALUE( mx, y.integer );
      break;
    case STACK_PAIR_TYPE_XREA_YREA:
      DIV_REAL_PITEM_VALUE( mx, y.real );
      break;
    default:
      break;
    }
  }
  *px = *mx;
}



/*******************************************************************//**
 * divif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_divif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    if( value.bits == 0 ) {
      SET_NAN( mx );
    }
    else {
      vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
      switch( pair_type  ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        DIV_INTEGER_PITEM_VALUE( mx, value.integer );
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        SET_REAL_PITEM_VALUE( mx, (double)mx->integer / value.real );
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        DIV_REAL_PITEM_VALUE( mx, value.integer );
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        DIV_REAL_PITEM_VALUE( mx, value.real );
        break;
      default:
        break;
      }
    }
  }
}



/*******************************************************************//**
 * mod( index, value ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_mod( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t y = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  if( y.bits == 0 ) {
    SET_NAN( mx );
  }
  else {
    vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &y );
    switch( pair_type  ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      mx->integer = mx->integer % y.integer;
      break;
    case STACK_PAIR_TYPE_XINT_YREA:
      SET_REAL_PITEM_VALUE( mx, fmod( (double)mx->integer, y.real) );
      break;
    case STACK_PAIR_TYPE_XREA_YINT:
      DIV_REAL_PITEM_VALUE( mx, y.integer );
      mx->real = fmod( mx->real, (double)y.integer);
      break;
    case STACK_PAIR_TYPE_XREA_YREA:
      mx->real = fmod( mx->real, y.real);
      break;
    default:
      break;
    }
  }
  *px = *mx;
}



/*******************************************************************//**
 * modif( condition, index, value ) -> condition
 ***********************************************************************
 */
static void __eval_memory_modif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    if( value.bits == 0 ) {
      SET_NAN( mx );
    }
    else {
      vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
      switch( pair_type  ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        mx->integer = mx->integer % value.integer;
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        SET_REAL_PITEM_VALUE( mx, fmod( (double)mx->integer, value.real) );
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        DIV_REAL_PITEM_VALUE( mx, value.integer );
        mx->real = fmod( mx->real, (double)value.integer);
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        mx->real = fmod( mx->real, value.real);
        break;
      default:
        break;
      }
    }
  }
}



/*******************************************************************//**
 * shr( index, rshift ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_shr( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t rshift = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  mx->bits >>= rshift.integer;
  *px = *mx;
}



/*******************************************************************//**
 * shrif( condition, index, rshift ) -> condition
 ***********************************************************************
 */
static void __eval_memory_shrif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t rshift = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    mx->bits >>= rshift.integer;
  }
}



/*******************************************************************//**
 * shl( index, lshift ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_shl( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t lshift = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  mx->bits <<= lshift.integer;
  *px = *mx;
}



/*******************************************************************//**
 * shlif( condition, index, lshift ) -> condition
 ***********************************************************************
 */
static void __eval_memory_shlif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t lshift = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {    
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    mx->bits <<= lshift.integer;
  }
}



/*******************************************************************//**
 * and( index, mask ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_and( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  mx->bits &= mask.bits;
  *px = *mx;
}



/*******************************************************************//**
 * andif( condition, index, mask ) -> condition
 ***********************************************************************
 */
static void __eval_memory_andif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    mx->bits &= mask.bits;
  }
}



/*******************************************************************//**
 * or( index, mask ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_or( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  mx->bits |= mask.bits;
  *px = *mx;
}



/*******************************************************************//**
 * orif( condition, index, mask ) -> condition
 ***********************************************************************
 */
static void __eval_memory_orif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    mx->bits |= mask.bits;
  }
}



/*******************************************************************//**
 * xor( index, mask ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_xor( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  mx->bits ^= mask.bits;
  *px = *mx;
}



/*******************************************************************//**
 * xorif( condition, index, mask ) -> condition
 ***********************************************************************
 */
static void __eval_memory_xorif( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t index = POP_ITEM( self );
  if( __condition( GET_PITEM( self ) ) ) {
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ index.integer & mem->mask ];
    mx->bits ^= mask.bits;
  }
}



/*******************************************************************//**
 * smooth( index, value, alpha ) -> [index]
 ***********************************************************************
 */
static void __eval_memory_smooth( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t alpha = POP_ITEM( self );
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *mx = &mem->data[ px->integer & mem->mask ];
  double x;
  double v;
  vgx_StackPairType_t pair_type = PAIR_TYPE( mx, &value );
  switch( pair_type  ) {
  case STACK_PAIR_TYPE_XINT_YINT:
    x = (double)mx->integer;
    v = (double)value.integer;
    break;
  case STACK_PAIR_TYPE_XINT_YREA:
    x = (double)mx->integer;
    v = value.real;
    break;
  case STACK_PAIR_TYPE_XREA_YINT:
    x = mx->real;
    v = (double)value.integer;
    break;
  case STACK_PAIR_TYPE_XREA_YREA:
    x = mx->real;
    v = value.real;
    break;
  default:
    *px = *mx;
    return;
  }

  double a = alpha.type == STACK_ITEM_TYPE_REAL ? alpha.real : (double)(alpha.integer & 1);
  
  // Exponential smoothing
  x = x + a * ( v - x );

  SET_REAL_PITEM_VALUE( mx, x );
  *px = *mx;
}



/*******************************************************************//**
 * mint( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mint( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type == STACK_ITEM_TYPE_REAL ) {
      vx->integer = (int64_t)vx->real;
    }
    vx->type = STACK_ITEM_TYPE_INTEGER;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mintr( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mintr( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type == STACK_ITEM_TYPE_REAL ) {
      vx->integer = (int64_t)round( vx->real );
    }
    vx->type = STACK_ITEM_TYPE_INTEGER;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mreal( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mreal( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type != STACK_ITEM_TYPE_REAL ) {
      SET_REAL_PITEM_VALUE( vx, (double)vx->integer );
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mbits( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mbits( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->type = STACK_ITEM_TYPE_BITVECTOR;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * minc( src_1, src_n ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_minc( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      vx->integer++;
      continue;
    case STACK_ITEM_TYPE_REAL:
      vx->real += 1.0;
      continue;
    default:
      SKIP_ARRAY_ITEM;
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mdec( src_1, src_n ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mdec( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      vx->integer--;
      continue;
    case STACK_ITEM_TYPE_REAL:
      vx->real -= 1.0;
      continue;
    default:
      SKIP_ARRAY_ITEM;
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * madd( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_madd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = value.integer;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        vx->integer += x;
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real += x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else if( value.type == STACK_ITEM_TYPE_REAL ) {
    double x = value.real;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        SET_REAL_PITEM_VALUE( vx, vx->integer + x );
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real += x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
}



/*******************************************************************//**
 * msub( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_msub( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = value.integer;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        vx->integer -= x;
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real -= x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else if( value.type == STACK_ITEM_TYPE_REAL ) {
    double x = value.real;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        SET_REAL_PITEM_VALUE( vx, vx->integer - x );
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real -= x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
}



/*******************************************************************//**
 * mmul( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mmul( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = value.integer;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        vx->integer *= x;
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real *= x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else if( value.type == STACK_ITEM_TYPE_REAL ) {
    double x = value.real;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        SET_REAL_PITEM_VALUE( vx, vx->integer * x );
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real *= x;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
  else {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
}



/*******************************************************************//**
 * mdiv( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mdiv( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.bits == 0 ) {
    BEGIN_ARRAY_PROCESS( self, vx ) {
      SET_NAN( vx );
    } END_ARRAY_PROCESS;
  }
  else {
    double div = 1.0;
    if( value.type == STACK_ITEM_TYPE_INTEGER ) {
      div = 1.0 / value.integer;
    }
    else if( value.type == STACK_ITEM_TYPE_REAL ) {
      div = 1.0 / value.real;
    }
    else {
      DISCARD_ITEMS( self, 2 );
      STACK_RETURN_INTEGER( self, 0 );
    }
    BEGIN_ARRAY_PROCESS( self, vx ) {
      switch( vx->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        SET_REAL_PITEM_VALUE( vx, vx->integer * div );
        continue;
      case STACK_ITEM_TYPE_REAL:
        vx->real *= div;
        continue;
      default:
        SKIP_ARRAY_ITEM;
      }
    } END_ARRAY_PROCESS;
  }
}



/*******************************************************************//**
 * mmod( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mmod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.bits == 0 ) {
    BEGIN_ARRAY_PROCESS( self, vx ) {
      SET_NAN( vx );
    } END_ARRAY_PROCESS;
  }
  else {
    if( value.type == STACK_ITEM_TYPE_INTEGER ) {
      int64_t x = value.integer;
      BEGIN_ARRAY_PROCESS( self, vx ) {
        switch( vx->type ) {
        case STACK_ITEM_TYPE_INTEGER:
          vx->integer %= x;
          continue;
        case STACK_ITEM_TYPE_REAL:
          vx->real = fmod( vx->real, (double)x );
          continue;
        default:
          SKIP_ARRAY_ITEM;
        }
      } END_ARRAY_PROCESS;
    }
    else if( value.type == STACK_ITEM_TYPE_REAL ) {
      double x = value.real;
      BEGIN_ARRAY_PROCESS( self, vx ) {
        switch( vx->type ) {
        case STACK_ITEM_TYPE_INTEGER:
          SET_REAL_PITEM_VALUE( vx, fmod( (double)vx->integer, x ) );
          continue;
        case STACK_ITEM_TYPE_REAL:
          vx->real = fmod( vx->real, x );
          continue;
        default:
          SKIP_ARRAY_ITEM;
        }
      } END_ARRAY_PROCESS;
    }
    else {
      DISCARD_ITEMS( self, 2 );
      STACK_RETURN_INTEGER( self, 0 );
    }
  }
}



/*******************************************************************//**
 * miinc( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_miinc( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->integer++;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * midec( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_midec( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->integer--;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * miadd( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_miadd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t x = value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->integer += x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * misub( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_misub( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t x = value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->integer -= x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mimul( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mimul( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t x = value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->integer *= x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * midiv( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_midiv( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t x = value.integer;
  if( x == 0 ) {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    int64_t div = 1 + (1ULL << 32) / x;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      vx->integer = (vx->integer * div) >> 32;
    } END_ARRAY_PROCESS;
  }
}



/*******************************************************************//**
 * mimod( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mimod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t x = value.integer;
  if( x == 0 ) {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    BEGIN_ARRAY_PROCESS( self, vx ) {
      vx->integer %= x;
    } END_ARRAY_PROCESS;
  }
}



/*******************************************************************//**
 * mrinc( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrinc( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real += 1.0;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mrdec( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrdec( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real -= 1.0;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mradd( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mradd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  double x = value.type == STACK_ITEM_TYPE_REAL ? value.real : value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real += x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mvadd( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvadd( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      vgx_StackPairType_t pair_type = PAIR_TYPE( A, B );
      switch( pair_type ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        A->integer += B->integer;
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        SET_REAL_PITEM_VALUE( A, (double)A->integer + B->real );
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        A->real += B->integer;
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        A->real += B->real;
        break; 
      default:
        break;
      }
      ++A;
      ++B;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mrsub( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsub( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  double x = value.type == STACK_ITEM_TYPE_REAL ? value.real : value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real -= x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mvsub( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvsub( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      vgx_StackPairType_t pair_type = PAIR_TYPE( A, B );
      switch( pair_type ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        A->integer -= B->integer;
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        SET_REAL_PITEM_VALUE( A, (double)A->integer - B->real );
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        A->real -= B->integer;
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        A->real -= B->real;
        break;
      default:
        break;
      }
      ++A;
      ++B;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mrmul( src_1, src_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrmul( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  double x = value.type == STACK_ITEM_TYPE_REAL ? value.real : value.integer;
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real *= x;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mvmul( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvmul( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      vgx_StackPairType_t pair_type = PAIR_TYPE( A, B );
      switch( pair_type ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        A->integer *= B->integer;
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        SET_REAL_PITEM_VALUE( A, (double)A->integer * B->real );
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        A->real *= B->integer;
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        A->real *= B->real;
        break;
      default:
        break;
      }
      ++A;
      ++B;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mrdiv( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mrdiv( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.bits == 0 ) {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    double x = value.type == STACK_ITEM_TYPE_REAL ? value.real : (double)value.integer;
    double div = 1.0 / x;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      vx->real *= div;
    } END_ARRAY_PROCESS;
  }
}



/*******************************************************************//**
 * mvdiv( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvdiv( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    double x, y;
    while( B < end ) {
      vgx_StackPairType_t pair_type = PAIR_TYPE( A, B );
      switch( pair_type ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        x = (double)A->integer;
        y = (double)B->integer;
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        x = (double)A->integer;
        y = B->real;
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        x = A->real;
        y = (double)B->integer;
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        x = A->real;
        y = B->real;
        break;
      default:
        x = 0.0;
        y = 1.0;
      }

      if( B->bits == 0 ) {
        y = FLT_MIN;
      }

      SET_REAL_PITEM_VALUE( A, x / y );

      ++A;
      ++B;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mrmod( src_1, src_n, value ) -> n_proc
 ***********************************************************************
 */
static void __eval_memory_mrmod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  if( value.bits == 0 ) {
    DISCARD_ITEMS( self, 2 );
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    double x = value.type == STACK_ITEM_TYPE_REAL ? value.real : value.integer;
    BEGIN_ARRAY_PROCESS( self, vx ) {
      vx->real = fmod( vx->real, x );
    } END_ARRAY_PROCESS;
  }
}



/*******************************************************************//**
 * mvmod( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvmod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      vgx_StackPairType_t pair_type = PAIR_TYPE( A, B );
      switch( pair_type ) {
      case STACK_PAIR_TYPE_XINT_YINT:
        if( B->bits ) {
          A->integer %= B->integer;
        }
        else {
          A->integer = 0;
        }
        break;
      case STACK_PAIR_TYPE_XINT_YREA:
        if( B->bits ) {
          SET_REAL_PITEM_VALUE( A, fmod( (double)A->integer, B->real ) );
        }
        else {
          SET_REAL_PITEM_VALUE( A, fmod( (double)A->integer, FLT_MIN ) );
        }
        break;
      case STACK_PAIR_TYPE_XREA_YINT:
        if( B->bits ) {
          A->real = fmod( A->real, (double)B->integer );
        }
        else {
          A->real = fmod( A->real, FLT_MIN );
        }
        break;
      case STACK_PAIR_TYPE_XREA_YREA:
        if( B->bits ) {
          A->real = fmod( A->real, B->real );
        }
        else {
          A->real = fmod( A->real, FLT_MIN );
        }
        break;
      default:
        break;
      }

      ++A;
      ++B;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * minv( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_minv( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrinv( self );
}



/*******************************************************************//**
 * mrinv( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrinv( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __rinv );
}



/*******************************************************************//**
 * mpow( src_1, src_n, y ) -> n
 ***********************************************************************
 */
static void __eval_memory_mpow( vgx_Evaluator_t *self ) {
  SAVE_STACK_1( self ) {
    __slice_cast_real( self );
  } RESTORE_STACK_1;
  __eval_memory_mrpow( self );
}



/*******************************************************************//**
 * mrpow( src_1, src_n, y ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrpow( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *py = POP_PITEM( self );
  double y;
  if( py->type == STACK_ITEM_TYPE_REAL ) {
    y = py->real;
  }
  else {
    y = (double)py->integer;
  }
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real = pow( vx->real, y );
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * msq( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msq( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type == STACK_ITEM_TYPE_REAL ) {
      vx->real *= vx->real;
    }
    else {
      vx->integer *= vx->integer;
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mrsq( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsq( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __rsq );
}



/*******************************************************************//**
 * msqrt( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msqrt( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type == STACK_ITEM_TYPE_REAL ) {
      vx->real = sqrt( vx->real );
    }
    else {
      SET_REAL_PITEM_VALUE( vx, sqrt( (double)vx->integer ) );
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mrsqrt( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsqrt( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, sqrt );
}



/*******************************************************************//**
 * mceil( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mceil( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrceil( self );
}



/*******************************************************************//**
 * mrceil( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrceil( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, ceil );
}


/*******************************************************************//**
 * mfloor( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mfloor( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrfloor( self );
}



/*******************************************************************//**
 * mrfloor( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrfloor( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, floor );
}



/*******************************************************************//**
 * mround( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mround( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrround( self );
}



/*******************************************************************//**
 * mrround( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrround( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, round );
}



/*******************************************************************//**
 * mabs( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mabs( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, cursor ) {
    if( cursor->type == STACK_ITEM_TYPE_REAL ) {
      cursor->real = fabs( cursor->real );
    }
    else {
      cursor->integer = llabs( cursor->integer );
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mrabs( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrabs( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, fabs );
}



/*******************************************************************//**
 * msign( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msign( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    if( vx->type == STACK_ITEM_TYPE_REAL ) {
      vx->real = __rsign( vx->real );
    }
    else {
      vx->integer = __isign( vx->integer );
    }
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mrsign( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsign( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __rsign );
}



/*******************************************************************//**
 * mpopcnt( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mpopcnt( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    SET_INTEGER_PITEM_VALUE( vx, __popcnt64( vx->bits ) );
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mlog2( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mlog2( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrlog2( self );
}



/*******************************************************************//**
 * mrlog2( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrlog2( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real = vx->real > 0.0 ? log2( vx->real ) : -1024.0;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mlog( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mlog( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrlog( self );
}



/*******************************************************************//**
 * mrlog( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrlog( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real = vx->real > 0.0 ? log( vx->real ) : -745.0;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mlog10( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mlog10( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrlog10( self );
}



/*******************************************************************//**
 * mrlog10( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrlog10( vgx_Evaluator_t *self ) {
  BEGIN_ARRAY_PROCESS( self, vx ) {
    vx->real = vx->real > 0.0 ? log10( vx->real ) : -324.0;
  } END_ARRAY_PROCESS;
}



/*******************************************************************//**
 * mexp2( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mexp2( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrexp2( self );
}



/*******************************************************************//**
 * mrexp2( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrexp2( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, exp2 );
}



/*******************************************************************//**
 * mexp( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mexp( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrexp( self );
}



/*******************************************************************//**
 * mrexp( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrexp( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, exp );
}



/*******************************************************************//**
 * mexp10( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mexp10( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrexp10( self );
}



/*******************************************************************//**
 * mrexp10( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrexp10( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, exp10 );
}



/*******************************************************************//**
 * mrad( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrad( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrrad( self );
}



/*******************************************************************//**
 * mrrad( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrrad( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __radians );
}



/*******************************************************************//**
 * mdeg( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mdeg( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrdeg( self );
}



/*******************************************************************//**
 * mrdeg( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrdeg( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __degrees );
}



/*******************************************************************//**
 * msin( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msin( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrsin( self );
}



/*******************************************************************//**
 * mrsin( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsin( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, sin );
}



/*******************************************************************//**
 * mcos( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mcos( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrcos( self );
}



/*******************************************************************//**
 * mrcos( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrcos( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, cos );
}



/*******************************************************************//**
 * mtan( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mtan( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrtan( self );
}



/*******************************************************************//**
 * mrtan( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrtan( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, tan );
}



/*******************************************************************//**
 * masin( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_masin( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrasin( self );
}



/*******************************************************************//**
 * mrasin( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrasin( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, asin );
}



/*******************************************************************//**
 * macos( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_macos( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mracos( self );
}



/*******************************************************************//**
 * mracos( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mracos( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, acos );
}



/*******************************************************************//**
 * matan( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_matan( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mratan( self );
}



/*******************************************************************//**
 * mratan( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mratan( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, atan );
}



/*******************************************************************//**
 * msinh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msinh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrsinh( self );
}



/*******************************************************************//**
 * mrsinh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsinh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, sinh );
}



/*******************************************************************//**
 * mcosh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mcosh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrcosh( self );
}



/*******************************************************************//**
 * mrcosh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrcosh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, cosh );
}



/*******************************************************************//**
 * mtanh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mtanh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrtanh( self );
}



/*******************************************************************//**
 * mrtanh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrtanh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, tanh );
}



/*******************************************************************//**
 * masinh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_masinh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrasinh( self );
}



/*******************************************************************//**
 * mrasinh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrasinh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, asinh );
}



/*******************************************************************//**
 * macosh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_macosh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mracosh( self );
}



/*******************************************************************//**
 * mracosh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mracosh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, acosh );
}



/*******************************************************************//**
 * matanh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_matanh( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mratanh( self );
}



/*******************************************************************//**
 * mratanh( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mratanh( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, atanh );
}



/*******************************************************************//**
 * msinc( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_msinc( vgx_Evaluator_t *self ) {
  __slice_cast_real( self );
  __eval_memory_mrsinc( self );
}



/*******************************************************************//**
 * mrsinc( src_1, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrsinc( vgx_Evaluator_t *self ) {
  ARRAY_APPLY_REAL_UNARY( self, __sinc );
}




/*******************************************************************//**
 * mshr( src_1, src_n, rshift ) -> n
 ***********************************************************************
 */
static void __eval_memory_mshr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t rshift = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  int64_t r = rshift.integer;
  while( cursor < end ) {
    cursor++->bits >>= r;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mvshr A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvshr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      (A++)->bits >>= (B++)->integer;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mshl( src_1, src_n, lshift ) -> n
 ***********************************************************************
 */
static void __eval_memory_mshl( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t lshift = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  int64_t l = lshift.integer;
  while( cursor < end ) {
    cursor++->bits <<= l;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mvshl A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvshl( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      (A++)->bits <<= (B++)->integer;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mand( src_1, src_2, mask ) -> n
 ***********************************************************************
 */
static void __eval_memory_mand( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  QWORD m = mask.bits;
  while( cursor < end ) {
    cursor++->bits &= m;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mvand( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvand( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      (A++)->bits &= (B++)->bits;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mor( src_1, src_n, mask ) -> n
 ***********************************************************************
 */
static void __eval_memory_mor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  QWORD m = mask.bits;
  while( cursor < end ) {
    cursor++->bits |= m;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mvor( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      (A++)->bits |= (B++)->bits;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mxor( src_1, src_n, mask ) -> n
 ***********************************************************************
 */
static void __eval_memory_mxor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t mask = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  QWORD m = mask.bits;
  while( cursor < end ) {
    cursor++->bits ^= m;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mvxor( A, B, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mvxor( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  if( n > 0 ) {
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      (A++)->bits ^= (B++)->bits;
    }
  }
  STACK_RETURN_INTEGER( self, n );
}



/*******************************************************************//**
 * mset( dest_1, dest_n, value ) -> n
 ***********************************************************************
 */
static void __eval_memory_mset( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  int64_t n;
  if( __is_stack_item_storable( self, &value, true ) ) {
    vgx_EvalStackItem_t *cursor, *end;
    n = __slice( self, &cursor, &end );
    while( cursor < end ) {
      *cursor++ = value;
    }
  }
  else {
    n = 0;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mreset() -> n
 ***********************************************************************
 */
static void __eval_memory_mreset( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *cursor = mem->data;
  vgx_EvalStackItem_t *end = cursor + mem->mask + 1;
  int64_t sz = end - cursor;
  while( cursor < end ) {
    cursor->type = STACK_ITEM_TYPE_INTEGER;
    (cursor++)->integer = 0;
  }
  iEvaluator.ClearCStrings( mem );
  iEvaluator.ClearVectors( mem );
  iEvaluator.ClearDWordSet( mem );
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, sz );
}



/*******************************************************************//**
 * mcopy( dest_1, src_1, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mcopy( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *pn = POP_PITEM( self );
  vgx_EvalStackItem_t *psrc = POP_PITEM( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  int64_t sz = 1LL << mem->order;
  vgx_EvalStackItem_t *pm = mem->data;

  int64_t n = pn->integer;
  int64_t s = psrc->integer & mask;
  int64_t d = px->integer & mask;

  // Do nothing if n is not positive, or dest is the same as src,
  // or either last dest or src is beyond end of memory.
  if( n <= 0 || d == s || d + n > sz || s + n > sz ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    vgx_EvalStackItem_t *A = pm + d;
    vgx_EvalStackItem_t *B = pm + s;
    vgx_EvalStackItem_t *end = B + n;

    while( B < end ) {
      *A++ = *B++;
    }
   
    STACK_RETURN_INTEGER( self, n );
  }
}



/*******************************************************************//**
 * mpwrite( [dest], src_1, n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mpwrite( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *pn = POP_PITEM( self );
  vgx_EvalStackItem_t *psrc = POP_PITEM( self );
  vgx_EvalStackItem_t *px = POP_PITEM( self );

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  uint64_t mask = mem->mask;
  int64_t sz = 1LL << mem->order;
  vgx_EvalStackItem_t *pm = mem->data;

  int64_t n = pn->integer;
  int64_t s = psrc->integer & mask;
  int64_t r = px->integer & mask;
  int64_t d = pm[ r ].integer;

  // Do nothing if n is not positive, or dest is the same as src,
  // or either last dest or src is beyond end of memory.
  if( n <= 0 || d == s || d + n > sz || s + n > sz ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {

    vgx_EvalStackItem_t *A = pm + d;
    vgx_EvalStackItem_t *B = pm + s;
    vgx_EvalStackItem_t *end = B + n;

    while( B < end ) {
      *A++ = *B++;
    }

    pm[ r ].integer += n;
    
    STACK_RETURN_INTEGER( self, n );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __copyobj_cstring( vgx_EvalStackItem_t *dest, int64_t max_items, const CString_t *CSTR__str ) {
  int64_t n_items = 0;
  QWORD *data = (QWORD*)CStringValue( CSTR__str );

  CString_attr aattr = CStringAttributes( CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK;
  if( aattr == CSTRING_ATTR_ARRAY_MAP ) {
    // Copy string data as mapping of 32-but int to float
    if( vgx_cstring_array_map_len( data ) <= max_items ) {
      vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)data;
      vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)data + 1;
      vgx_cstring_array_map_cell_t *last = cell + header->mask;
      while( cell <= last ) {
        if( cell->key != VGX_CSTRING_ARRAY_MAP_KEY_NONE ) {
          dest->keyval.bits = cell->bits;
          dest++->type = STACK_ITEM_TYPE_KEYVAL;
          ++n_items;
        }
        ++cell;
      }
    }
  }
  else {
    // Number of qwords in string
    int64_t nq = VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
    // Check size
    if( nq <= max_items ) {
      // Copy string data as integer array
      if( aattr == CSTRING_ATTR_ARRAY_INT ) {
        n_items = nq;
        int64_t *src = (int64_t*)data;
        while( nq-- ) {
          dest->integer = *src++;
          dest++->type = STACK_ITEM_TYPE_INTEGER;
        }
      }
      // Copy string data as float array
      else if( aattr == CSTRING_ATTR_ARRAY_FLOAT ) {
        n_items = nq;
        double *src = (double*)data;
        while( nq-- ) {
          dest->real = *src++;
          dest++->type = STACK_ITEM_TYPE_REAL;
        }
      }
      // Copy string data raw
      else {
        n_items = nq;
        QWORD *src = (QWORD*)data;
        while( nq-- ) {
          dest->integer = *src++;
          dest++->type = STACK_ITEM_TYPE_INTEGER;
        }
      }
    }
  }

  return n_items;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __copyobj_vertex( vgx_EvalStackItem_t *dest, int64_t max_items, const vgx_Vertex_t *vertex ) {
  int64_t nq = qwsizeof( vgx_AllocatedVertex_t );
  int64_t n_items = 0;

  // Check size
  if( nq <= max_items ) {
    vgx_AllocatedVertex_t *vertex_memory = ivertexobject.AsAllocatedVertex( vertex );
    QWORD *src = (QWORD*)vertex_memory;
    n_items = nq;
    while( nq-- ) {
      dest->integer = *src++;
      dest++->type = STACK_ITEM_TYPE_INTEGER;
    }
  }

  return n_items;
}



/*******************************************************************//**
 * mcopyobj( dest, obj ) -> n
 ***********************************************************************
 */
static void __eval_memory_mcopyobj( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t obj = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  int64_t dx = px->integer & mem->mask;
  vgx_EvalStackItem_t *dest = mem->data + dx;
  int64_t max_items = (1ULL << mem->order) - dx;
  int64_t n;
  switch( obj.type ) {
  // cstring
  case STACK_ITEM_TYPE_CSTRING:
    n = __copyobj_cstring( dest, max_items, obj.CSTR__str );
    break;
  // vertex
  case STACK_ITEM_TYPE_VERTEX:
    n = __copyobj_vertex( dest, max_items, obj.vertex );
    break;
  // do nothing
  default:
    n = 0;
    break;
  }
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mterm( index ) -> index
 ***********************************************************************
 */
static void __eval_memory_mterm( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pitem = &mem->data[ px->integer & mem->mask ];
  SET_NONE( pitem ); 
}



/*******************************************************************//**
 * mlen( index ) -> n
 ***********************************************************************
 */
static void __eval_memory_mlen( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pstart = &mem->data[ px->integer & mem->mask ];
  vgx_EvalStackItem_t *pend = mem->data + mem->mask + 1;
  vgx_EvalStackItem_t *p = pstart;

  while( p < pend && p->type != STACK_ITEM_TYPE_NONE ) {
    ++p;
  }

  SET_INTEGER_PITEM_VALUE( px, p-pstart );
}



/*******************************************************************//**
 * mrandomize( src_0, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrandomize( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double div = 1.0 / (double)LLONG_MAX;
  while( cursor < end ) {
    SET_REAL_PITEM_VALUE( cursor, (double)rand63() * div );
    ++cursor;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mrandbits( src_0, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mrandbits( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  while( cursor < end ) {
    SET_BITVECTOR_PITEM_VALUE( cursor, rand64() );
    ++cursor;
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * mhash( src_0, src_n ) -> n
 ***********************************************************************
 */
static void __eval_memory_mhash( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    int64_t h;
    switch( vx->type ) {
    // ihash64() of integer
    case STACK_ITEM_TYPE_INTEGER:
      vx->integer = ihash64( vx->integer );
      continue;
    // ihash64() of bits if real or bitvector
    case STACK_ITEM_TYPE_REAL:
    case STACK_ITEM_TYPE_BITVECTOR:
      SET_INTEGER_PITEM_VALUE( vx, ihash64( vx->bits ) );
      continue;
    case STACK_ITEM_TYPE_KEYVAL:
      SET_INTEGER_PITEM_VALUE( vx, vgx_cstring_array_map_hashkey( &vx->bits ) );
      continue;
    // ihash64() of address
    case STACK_ITEM_TYPE_VERTEX:
      SET_INTEGER_PITEM_VALUE( vx, ihash64( (uint64_t)vx->vertex ) );
      continue;
    // hash64() of string
    case STACK_ITEM_TYPE_CSTRING:
      SET_INTEGER_PITEM_VALUE( vx, hash64( (unsigned char*)CStringValue( vx->CSTR__str ), CStringLength( vx->CSTR__str ) ) );
      continue;
    // Interpret hash(vector) as its fingerprint
    case STACK_ITEM_TYPE_VECTOR:
      h = CALLABLE( vx->vector )->Fingerprint( vx->vector );
      SET_INTEGER_PITEM_VALUE( vx, h );
      continue;
    // Interpret hash(vertex.id) as the low part of its internalid
    case STACK_ITEM_TYPE_VERTEXID:
      {
        vgx_Vertex_t *vertex = (vgx_Vertex_t*)((char*)vx->vertexid - offsetof( vgx_Vertex_t, identifier ));
        SET_INTEGER_PITEM_VALUE( vx, __vertex_internalid( vertex )->L );
      }
      continue;
    //
    default:
      SET_INTEGER_PITEM_VALUE( vx, 0 );
    }
  }
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, n );
}



/*******************************************************************//**
 * msum( dest, src_0, src_n ) -> sum
 ***********************************************************************
 */
static void __eval_memory_msum( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double sum = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      sum += vx->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      sum += vx->real;
      continue;
    default:
      continue;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, sum );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrsum( dest, src_0, src_n ) -> sum
 ***********************************************************************
 */
static void __eval_memory_mrsum( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double sum = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    sum += vx->real;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, sum );
  *pdest = *dx;
}



/*******************************************************************//**
 * msumsqr( dest, src_0, src_n ) -> sumsqr
 ***********************************************************************
 */
static void __eval_memory_msumsqr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double sumsqr = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      sumsqr += vx->integer * vx->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      sumsqr += vx->real * vx->real;
      continue;
    default:
      continue;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, sumsqr );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrsumsqr( dest, src_0, src_n ) -> sumsqr
 ***********************************************************************
 */
static void __eval_memory_mrsumsqr( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double sumsqr = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    sumsqr += vx->real * vx->real;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, sumsqr );
  *pdest = *dx;
}



/*******************************************************************//**
 * mstdev( dest, src_0, src_n ) -> stdev
 ***********************************************************************
 */
static void __eval_memory_mstdev( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double A = 0.0;
  double Q = 0.0;
  int64_t k = 0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double xk;
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      xk = (double)vx->integer;
      break;
    case STACK_ITEM_TYPE_REAL:
      xk = vx->real;
      break;
    default:
      continue;
    }
    // running update of stdev
    ++k;
    double Ak = A + (xk - A) / k;
    Q += (xk - A) * (xk - Ak);
    A = Ak;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double stdev = sqrt( k ? Q / k : 0.0 );
  SET_REAL_PITEM_VALUE( dx, stdev );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrstdev( dest, src_0, src_n ) -> stdev
 ***********************************************************************
 */
static void __eval_memory_mrstdev( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double A = 0.0;
  double Q = 0.0;
  int64_t k = 0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double xk = vx->real;
    // running update of stdev
    ++k;
    double Ak = A + (xk - A) / k;
    Q += (xk - A) * (xk - Ak);
    A = Ak;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double stdev = sqrt( k ? Q / k : 0.0 );
  SET_REAL_PITEM_VALUE( dx, stdev );
  *pdest = *dx;
}



/*******************************************************************//**
 * minvsum( dest, src_0, src_n ) -> invsum
 ***********************************************************************
 */
static void __eval_memory_minvsum( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double invsum = 0.0;
  vgx_EvalStackItem_t *vx;
  double dflt = 1.0 / FLT_MIN;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      invsum += vx->integer ? 1.0 / vx->integer : dflt;
      continue;
    case STACK_ITEM_TYPE_REAL:
      invsum += vx->real != 0 ? 1.0 / vx->real : dflt;
      continue;
    default:
      continue;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, invsum );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrinvsum( dest, src_0, src_n ) -> invsum
 ***********************************************************************
 */
static void __eval_memory_mrinvsum( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double invsum = 0.0;
  vgx_EvalStackItem_t *vx;
  double dflt = 1.0 / FLT_MIN;
  while( (vx=cursor++) < end ) {
    invsum += vx->real != 0 ? 1.0 / vx->real : dflt;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, invsum );
  *pdest = *dx;
}



/*******************************************************************//**
 * mprod( dest, src_0, src_n ) -> product
 ***********************************************************************
 */
static void __eval_memory_mprod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double prod = 1.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      prod *= vx->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      prod *= vx->real;
      continue;
    default:
      continue;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, prod );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrprod( dest, src_0, src_n ) -> product
 ***********************************************************************
 */
static void __eval_memory_mrprod( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double prod = 1.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    prod *= vx->real;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  SET_REAL_PITEM_VALUE( dx, prod );
  *pdest = *dx;
}



/*******************************************************************//**
 * mmean( dest, src_0, src_n ) -> mean
 ***********************************************************************
 */
static void __eval_memory_mmean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double sum = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      sum += vx->integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      sum += vx->real;
      continue;
    default:
      --n;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double mean = n > 0 ? sum / n : 0.0;
  SET_REAL_PITEM_VALUE( dx, mean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrmean( dest, src_0, src_n ) -> mean
 ***********************************************************************
 */
static void __eval_memory_mrmean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double sum = 0.0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    sum += vx->real;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double mean = n > 0 ? sum / n : 0.0;
  SET_REAL_PITEM_VALUE( dx, mean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mharmmean( dest, src_0, src_n ) -> harmonicmean
 ***********************************************************************
 */
static void __eval_memory_mharmmean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double invsum = 0.0;
  vgx_EvalStackItem_t *vx;
  double dflt = 1.0 / FLT_MIN;
  while( (vx=cursor++) < end ) {
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      invsum += vx->integer ? 1.0 / vx->integer : dflt;
      continue;
    case STACK_ITEM_TYPE_REAL:
      invsum += vx->real != 0 ? 1.0 / vx->real : dflt;
      continue;
    default:
      --n;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double harmmean = invsum != 0 ? n / invsum : 0.0;
  SET_REAL_PITEM_VALUE( dx, harmmean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrharmmean( dest, src_0, src_n ) -> harmonicmean
 ***********************************************************************
 */
static void __eval_memory_mrharmmean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double invsum = 0.0;
  vgx_EvalStackItem_t *vx;
  double dflt = 1.0 / FLT_MIN;
  while( (vx=cursor++) < end ) {
    invsum += vx->real != 0 ? 1.0 / vx->real : dflt;
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double harmmean = invsum != 0 ? n / invsum : 0.0;
  SET_REAL_PITEM_VALUE( dx, harmmean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mgeomean( dest, src_0, src_n ) -> geometricmean
 ***********************************************************************
 */
static void __eval_memory_mgeomean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double logsum = 0.0;
  int sign = 1;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double x;
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      x = (double)vx->integer;
      break;
    case STACK_ITEM_TYPE_REAL:
      x = vx->real;
      break;
    default:
      --n;
      continue;
    }
    if( x > 0 ) {
      logsum += log2( x );
    }
    else if( x < 0 ) {
      logsum += log2( x );
      sign = -sign;
    }
    else {
      n = 0;
      break;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double geomean = n ? pow( 2, logsum / n ) * sign : 0.0;
  SET_REAL_PITEM_VALUE( dx, geomean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mrgeomean( dest, src_0, src_n ) -> geometricmean
 ***********************************************************************
 */
static void __eval_memory_mrgeomean( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  int64_t n = __slice( self, &cursor, &end );
  double logsum = 0.0;
  int sign = 1;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double x = vx->real;
    if( x > 0 ) {
      logsum += log2( x );
    }
    else if( x < 0 ) {
      logsum += log2( x );
      sign = -sign;
    }
    else {
      n = 0;
      break;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double geomean = n ? pow( 2, logsum / n ) * sign : 0.0;
  SET_REAL_PITEM_VALUE( dx, geomean );
  *pdest = *dx;
}



/*******************************************************************//**
 * mgeostdev( dest, src_0, src_n ) -> geostdev
 ***********************************************************************
 */
static void __eval_memory_mgeostdev( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double A = 0.0;
  double Q = 0.0;
  int64_t k = 0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double xk;
    switch( vx->type ) {
    case STACK_ITEM_TYPE_INTEGER:
      xk = (double)vx->integer;
      break;
    case STACK_ITEM_TYPE_REAL:
      xk = vx->real;
      break;
    default:
      continue;
    }
    // running update of geostdev
    if( xk > 0 ) {
      double log_xk = log( xk );
      ++k;
      double Ak = A + (log_xk - A) / k;
      Q += (log_xk - A) * (log_xk - Ak);
      A = Ak;
    }
    else {
      k = 0;
      break;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double geostdev = exp( k ? sqrt( Q / k ) : 0.0 );
  SET_REAL_PITEM_VALUE( dx, geostdev );
  *pdest = *dx;
}


/*******************************************************************//**
 * mrgeostdev( dest, src_0, src_n ) -> geostdev
 ***********************************************************************
 */
static void __eval_memory_mrgeostdev( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  double A = 0.0;
  double Q = 0.0;
  int64_t k = 0;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    double xk = vx->real;
    // running update of geostdev
    if( xk > 0 ) {
      double log_xk = log( xk );
      ++k;
      double Ak = A + (log_xk - A) / k;
      Q += (log_xk - A) * (log_xk - Ak);
      A = Ak;
    }
    else {
      k = 0;
      break;
    }
  }
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *pdest = GET_PITEM( self );
  vgx_EvalStackItem_t *dx = &mem->data[ pdest->integer & mem->mask ];
  double geostdev = exp( k ? sqrt( Q / k ) : 0.0 );
  SET_REAL_PITEM_VALUE( dx, geostdev );
  *pdest = *dx;
}



/*******************************************************************//**
 * mmax( dest, src_0, src_n ) -> index
 ***********************************************************************
 */
static void __eval_memory_mmax( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  vgx_EvalStackItem_t maxitem = *cursor;
  int64_t index = cursor - mem->data;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    vgx_StackPairType_t pair_type = PAIR_TYPE( vx, &maxitem );
    switch( pair_type ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      if( vx->integer > maxitem.integer ) {
        maxitem.integer = vx->integer;
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XINT_YREA:
      if( (double)vx->integer > maxitem.real ) {
        SET_INTEGER_PITEM_VALUE( &maxitem, vx->integer );
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XREA_YINT:
      if( vx->real > (double)maxitem.integer ) {
        SET_REAL_PITEM_VALUE( &maxitem, vx->real );
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XREA_YREA:
      if( vx->real > maxitem.real ) {
        maxitem.real = vx->real;
        break;
      }
      continue;
    default:
      if( vx->bits > maxitem.bits ) {
        maxitem = *vx;
        break;
      }
      continue;
    }
    index = vx - mem->data;
  }
  vgx_EvalStackItem_t *px = GET_PITEM( self ); // dest
  mem->data[ px->integer & mem->mask ] = maxitem;
  SET_INTEGER_PITEM_VALUE( px, index );
}



/*******************************************************************//**
 * mmin( dest, src_0, src_n ) -> index
 ***********************************************************************
 */
static void __eval_memory_mmin( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  vgx_EvalStackItem_t minitem = *cursor;
  int64_t index = cursor - mem->data;
  vgx_EvalStackItem_t *vx;
  while( (vx=cursor++) < end ) {
    vgx_StackPairType_t pair_type = PAIR_TYPE( vx, &minitem );
    switch( pair_type ) {
    case STACK_PAIR_TYPE_XINT_YINT:
      if( vx->integer < minitem.integer ) {
        minitem.integer = vx->integer;
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XINT_YREA:
      if( (double)vx->integer < minitem.real ) {
        SET_INTEGER_PITEM_VALUE( &minitem, vx->integer );
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XREA_YINT:
      if( vx->real < (double)minitem.integer ) {
        SET_REAL_PITEM_VALUE( &minitem, vx->real );
        break;
      }
      continue;
    case STACK_PAIR_TYPE_XREA_YREA:
      if( vx->real < minitem.real ) {
        minitem.real = vx->real;
        break;
      }
      continue;
    default:
      if( vx->bits < minitem.bits ) {
        minitem = *vx;
        break;
      }
      continue;
    }
    index = vx - mem->data;
  }
  vgx_EvalStackItem_t *px = GET_PITEM( self ); // dest
  mem->data[ px->integer & mem->mask ] = minitem;
  SET_INTEGER_PITEM_VALUE( px, index );
}



/*******************************************************************//**
 * mcontains( src_1, src_n, value ) -> 1 or 0
 ***********************************************************************
 */
static void __eval_memory_mcontains( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  int match = 0;
  vgx_EvalStackItem_t *vx;

  if( __is_stack_item_type_bit_comparable( value.type ) ) {
    while( (vx=cursor++) < end ) {
      if( __is_stack_item_type_bit_comparable( vx->type ) ) {
        if( value.bits == vx->bits ) {
          match = 1;
          break;
        }
      }
      else if( __equ( self, vx, &value ) ) {
        match = 1;
        break;
      }
    }
  }
  else {
    while( (vx=cursor++) < end ) {
      if( __equ( self, vx, &value ) ) {
        match = 1;
        break;
      }
    }
  }

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, match );
}



/*******************************************************************//**
 * mcount( src_1, src_n, value ) -> 0 or more
 ***********************************************************************
 */
static void __eval_memory_mcount( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  int64_t count = 0;
  vgx_EvalStackItem_t *vx;

  if( __is_stack_item_type_bit_comparable( value.type ) ) {
    while( (vx=cursor++) < end ) {
      if( __is_stack_item_type_bit_comparable( vx->type ) ) {
        if( vx->bits == value.bits ) {
          ++count;
        }
      }
      else {
        if( __equ( self, vx, &value ) ) {
          ++count;
        }
      }
    }
  }
  else {
    while( (vx=cursor++) < end ) {
      if( __equ( self, vx, &value ) ) {
        ++count;
      }
    }
  }

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, count );
}



/*******************************************************************//**
 * mindex( src_1, src_n, value ) -> first occurrence index or -1
 ***********************************************************************
 */
static void __eval_memory_mindex( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  vgx_EvalStackItem_t value = POP_ITEM( self );
  vgx_EvalStackItem_t *cursor, *end;
  __slice( self, &cursor, &end );
  int64_t index = -1;
  vgx_EvalStackItem_t *vx;

  if( __is_stack_item_type_bit_comparable( value.type ) ) {
    while( (vx=cursor++) < end ) {
      if( __is_stack_item_type_bit_comparable( vx->type ) ) {
        if( vx->bits == value.bits ) {
          index = vx - mem->data;
          break;
        }
      }
      else {
        if( __equ( self, vx, &value ) ) {
          index = vx - mem->data;
          break;
        }
      }
    }
  }
  else {
    while( (vx=cursor++) < end ) {
      if( __equ( self, vx, &value ) ) {
        index = vx - mem->data;
        break;
      }
    }
  }

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_INTEGER_PITEM_VALUE( px, index );
}



#define __cmp_a_gt_b( a, b ) (int)( (a > b) - (a < b) )
/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cmp( vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return __cmp_a_gt_b( a->integer, b->integer );
  }
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XINT_YREA:
      return __cmp_a_gt_b( a->integer, b->real );
    case STACK_PAIR_TYPE_XREA_YINT:
      return __cmp_a_gt_b( a->real, b->integer );
    case STACK_PAIR_TYPE_XREA_YREA:
      return __cmp_a_gt_b( a->real, b->real );
    default:
      break;
    }
  }

  return __cmp_a_gt_b( a->bits, b->bits );
}



/*******************************************************************//**
 * Approximate cmp for reals
 ***********************************************************************
 */
__inline static int __cmpa( vgx_EvalStackItem_t *a, vgx_EvalStackItem_t *b ) {
  vgx_StackPairType_t pair_type = PAIR_TYPE( a, b );
  // Signed int test
  if( pair_type == STACK_PAIR_TYPE_XINT_YINT ) {
    return __cmp_a_gt_b( a->integer, b->integer );
  }
  else if( __is_stack_pair_type_numeric( pair_type ) ) {
    switch( pair_type ) {
    // real cases
    case STACK_PAIR_TYPE_XINT_YREA:
      if( fabs( (double)a->integer - b->real ) < FLT_EPSILON ) {
        return 0;
      }
      else {
        return __cmp_a_gt_b( a->integer, b->real );
      }
    case STACK_PAIR_TYPE_XREA_YINT:
      if( fabs( a->real - (double)b->integer ) < FLT_EPSILON ) {
        return 0;
      }
      else {
        return __cmp_a_gt_b( a->real, b->integer );
      }
    case STACK_PAIR_TYPE_XREA_YREA:
      if( fabs( a->real - b->real ) < FLT_EPSILON ) {
        return 0;
      }
      else {
        return __cmp_a_gt_b( a->real, b->real );
      }
    default:
      break;
    }
  }

  return __cmp_a_gt_b( a->bits, b->bits );
}



/*******************************************************************//**
 * mcmp( A, B, n ) -> -1, 0, 1
 ***********************************************************************
 */
static void __eval_memory_mcmp( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  // Treat as A<B (-1) if n is negative
  // or either last item in A or B is beyond end of memory.
  if( n < 0 ) {
    STACK_RETURN_INTEGER( self, -1 );
  }
  // Equal (0) if n is zero or A and B overlap exactly
  else if( n == 0 || A == B ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    int r = 0;
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      if( (r = __cmp( A++, B++ )) != 0 ) {
        break;
      }
    }
    STACK_RETURN_INTEGER( self, r );
  }
}



/*******************************************************************//**
 * mcmpa( A, B, n ) -> -1, 0, 1
 ***********************************************************************
 */
static void __eval_memory_mcmpa( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *A, *B;
  int64_t n = __contiguous( self, &A, &B );
  // Treat as A<B (-1) if n is negative
  // or either last item in A or B is beyond end of memory.
  if( n < 0 ) {
    STACK_RETURN_INTEGER( self, -1 );
  }
  // Equal (0) if n is zero or A and B overlap exactly
  else if( n == 0 || A == B ) {
    STACK_RETURN_INTEGER( self, 0 );
  }
  else {
    int r = 0;
    vgx_EvalStackItem_t *end = B + n;
    while( B < end ) {
      if( (r = __cmpa( A++, B++ )) != 0 ) {
        break;
      }
    }
    STACK_RETURN_INTEGER( self, r );
  }
}



/*******************************************************************//**
 * msubset( A1, An, B1, Bm ) -> 1 if all A in B, otherwise 0
 ***********************************************************************
 */
static void __eval_memory_msubset( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *start_B, *end_B;
  int64_t nB = __slice( self, &start_B, &end_B );
  vgx_EvalStackItem_t *start_A, *end_A;
  int64_t nA = __slice( self, &start_A, &end_A );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );

  if( nA > nB ) {
    SET_INTEGER_PITEM_VALUE( px, 0 ); // miss
    return;
  }

  vgx_EvalStackItem_t *ax = start_A;
  while( ax < end_A ) {
    vgx_EvalStackItem_t *bx = start_B;
    while( bx < end_B ) {
      if( ax->type == bx->type && ax->bits == bx->bits ) {
        goto next;
      }
      ++bx;
    }
    SET_INTEGER_PITEM_VALUE( px, 0 ); // miss
    return;
  next:
    ++ax;
  }
  SET_INTEGER_PITEM_VALUE( px, 1 ); // A is subset of B
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __subsetobj_cstring( vgx_EvalStackItem_t *start, vgx_EvalStackItem_t *end, const CString_t *CSTR__str ) {

  QWORD *data = (QWORD*)CStringValue( CSTR__str ); // data
  vgx_EvalStackItem_t *ax = start;  // probe

  CString_attr aattr = CStringAttributes( CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK;

  // Compare string data as map
  if( aattr == CSTRING_ATTR_ARRAY_MAP ) {
    // Iterate over probe elements
    // NOTE: probe keys are 32-bit integers!
    int key;
    while( ax < end ) {
      // Primary: probe item is keyval
      if( ax->type == STACK_ITEM_TYPE_KEYVAL ) {
        key = ax->keyval.key;
      }
      // Fallback: treat probe item as integer key
      else {
        key = (int)ax->integer;
      }

      if( vgx_cstring_array_map_get( data, key ) == NULL ) {
        return 0; // MISS
      }

      ++ax;
    }
    return 1; // subset
  }
  // Compare string data as list of keys (with implicit weight = 1.0)
  else if( aattr == CSTRING_ATTR_ARRAY_INT ) {
    // Number of qwords in string
    int64_t nq = VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
    // End of data
    int64_t *end_b = (int64_t*)data + nq;
    // Iterate over probe elements
    while( ax < end ) {
      int64_t *bx = (int64_t*)data;
      // Primary: probe item is keyval
      if( ax->type == STACK_ITEM_TYPE_KEYVAL ) {
        // Iterate over data
        while( bx < end_b ) {
          if( ax->keyval.key == *bx++ ) {
            goto next_probe;
          }
        }
      }
      // Fallback: treat probe item as integer (key) with unity weight
      else {
        // Iterate over data
        while( bx < end_b ) {
          if( ax->integer == *bx++ ) {
            goto next_probe;
          }
        }
      }

      // MISS
      return 0;
    next_probe:
      ++ax;
    }
    return 1; // subset 
  }
  // Incompatible, automatic miss
  else {
    return 0;
  }
}




/*******************************************************************//**
 * msubsetobj( m1, mn, obj ) -> 1 if all A in object obj, otherwise 0
 ***********************************************************************
 */
static void __eval_memory_msubsetobj( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t obj = POP_ITEM( self );

  vgx_EvalStackItem_t *start, *end;
  __slice( self, &start, &end );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );

  int is_subset;

  switch( obj.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    is_subset = __subsetobj_cstring( start, end, obj.CSTR__str );
    break;
  default:
    is_subset = 0;
    break;
  }

  SET_INTEGER_PITEM_VALUE( px, is_subset );

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __sumprodobj_cstring( vgx_EvalStackItem_t *start, vgx_EvalStackItem_t *end, const CString_t *CSTR__str ) {
  double sumprod = 0.0;

  QWORD *data = (QWORD*)CStringValue( CSTR__str ); // data
  vgx_EvalStackItem_t *ax = start;  // probe

  CString_attr aattr = CStringAttributes( CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK;

  // Compare string data as map
  if( aattr == CSTRING_ATTR_ARRAY_MAP ) {
    // Iterate over probe elements
    // NOTE: probe keys are 32-bit integers!
    vgx_cstring_array_map_cell_t *cell;
    int key;
    double val;
    while( ax < end ) {
      // Primary: probe item is keyval
      if( ax->type == STACK_ITEM_TYPE_KEYVAL ) {
        key = ax->keyval.key;
        val = ax->keyval.value;
      }
      // Fallback: treat probe item as integer (key) with unity weight
      else {
        key = (int)ax->integer;
        val = 1.0;
      }

      if( (cell = vgx_cstring_array_map_get( data, key )) != NULL ) {
        sumprod += val * cell->value;
      }

      ++ax; // next probe element
    }
  }
  // Compare string data as list of keys (with implicit weight = 1.0)
  else if( aattr == CSTRING_ATTR_ARRAY_INT ) {
    // Number of qwords in string
    int64_t nq = VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
    // End of data
    int64_t *end_b = (int64_t*)data + nq;
    // Iterate over probe elements
    while( ax < end ) {
      int64_t *bx = (int64_t*)data;
      // Primary: probe item is keyval
      if( ax->type == STACK_ITEM_TYPE_KEYVAL ) {
        // Iterate over data
        while( bx < end_b ) {
          if( ax->keyval.key == *bx++ ) {
            sumprod += ax->keyval.value;
          }
        }
      }
      // Fallback: treat probe item as integer (key) with unity weight
      else {
        // Iterate over data
        while( bx < end_b ) {
          if( ax->integer == *bx++ ) {
            sumprod += 1.0;
          }
        }
      }
      ++ax; // next probe element
    }
  }

  return sumprod;
}



/*******************************************************************//**
 * msumprodobj( m1, mn, obj ) -> float
 *
 * Compute the sum of products of all elements in memory locations
 * [m1 - mn] that exist in obj, where obj is an array of keyval entries
 * and all [mk] are keyval entries. A keyval entry is a tuple (key,val) 
 * where key is a 32-bit integer and val is a single precision float.
 *
 ***********************************************************************
 */
static void __eval_memory_msumprodobj( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t obj = POP_ITEM( self );

  vgx_EvalStackItem_t *start, *end;
  __slice( self, &start, &end );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );

  double sumprod = 0.0;

  switch( obj.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    sumprod = __sumprodobj_cstring( start, end, obj.CSTR__str );
    break;
  default:
    break;
  }

  SET_REAL_PITEM_VALUE( px, sumprod );

}



/*******************************************************************//**
 * modindex( idx, mod, offset ) -> array index
 ***********************************************************************
 */
static void __eval_memory_modindex( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t offset = POP_ITEM( self );
  vgx_EvalStackItem_t mod = POP_ITEM( self );
  vgx_EvalStackItem_t *idx = GET_PITEM( self );
  if( mod.integer > 0 ) {
    idx->integer = (idx->integer % mod.integer) + offset.integer;
    idx->type = STACK_ITEM_TYPE_INTEGER;
  }
}



/*******************************************************************//**
 * index( vertex ) -> 1 or 0
 ***********************************************************************
 */
static void __eval_memory_index( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_VERTEX ) {
    uint64_t index = __vertex_get_index( ivertexobject.AsAllocatedVertex(px->vertex) );
    uint64_t bitindex = index >> 6;
    uint64_t bitvector = 1ULL << (index & 0x3f);
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ bitindex & mem->mask ];
    if( !(mx->integer & bitvector) ) {
      mx->integer |= bitvector;
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * indexed( vertex ) -> 1 or 0
 ***********************************************************************
 */
static void __eval_memory_indexed( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_VERTEX ) {
    uint64_t index = __vertex_get_index( ivertexobject.AsAllocatedVertex(px->vertex) );
    uint64_t bitindex = index >> 6;
    uint64_t bitvector = 1ULL << (index & 0x3f);
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ bitindex & mem->mask ];
    if( mx->integer & bitvector ) {
      SET_INTEGER_PITEM_VALUE( px, 1 );
      return;
    }
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 * unindex( vertex ) -> 0
 ***********************************************************************
 */
static void __eval_memory_unindex( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_VERTEX ) {
    uint64_t index = __vertex_get_index( ivertexobject.AsAllocatedVertex(px->vertex) );
    uint64_t bitindex = index >> 6;
    uint64_t bitvector = 1ULL << (index & 0x3f);
    vgx_ExpressEvalMemory_t *mem = self->context.memory;
    vgx_EvalStackItem_t *mx = &mem->data[ bitindex & mem->mask ];
    mx->integer &= ~bitvector;
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



#endif
