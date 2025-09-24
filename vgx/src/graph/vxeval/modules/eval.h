/*
###################################################
#
# File:   eval.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_EVAL_H
#define _VGX_VXEVAL_MODULES_EVAL_H

#define SET_INTEGER_PITEM_VALUE( ItemPtr, Value )         ((ItemPtr)->integer = (Value)); (ItemPtr)->type = STACK_ITEM_TYPE_INTEGER
#define ADD_INTEGER_PITEM_VALUE( IntegerItemPtr, Value )  ((IntegerItemPtr)->integer += Value); (IntegerItemPtr)->type = STACK_ITEM_TYPE_INTEGER
#define SUB_INTEGER_PITEM_VALUE( IntegerItemPtr, Value )  ((IntegerItemPtr)->integer -= Value); (IntegerItemPtr)->type = STACK_ITEM_TYPE_INTEGER
#define MUL_INTEGER_PITEM_VALUE( IntegerItemPtr, Value )  ((IntegerItemPtr)->integer *= Value); (IntegerItemPtr)->type = STACK_ITEM_TYPE_INTEGER
#define DIV_INTEGER_PITEM_VALUE( IntegerItemPtr, Value )  ((IntegerItemPtr)->integer /= Value); (IntegerItemPtr)->type = STACK_ITEM_TYPE_INTEGER

#define SET_REAL_PITEM_VALUE( ItemPtr, Value )            ((ItemPtr)->real = (Value)); (ItemPtr)->type = STACK_ITEM_TYPE_REAL
#define ADD_REAL_PITEM_VALUE( RealItemPtr, Value )        ((RealItemPtr)->real += Value); (RealItemPtr)->type = STACK_ITEM_TYPE_REAL
#define SUB_REAL_PITEM_VALUE( RealItemPtr, Value )        ((RealItemPtr)->real -= Value); (RealItemPtr)->type = STACK_ITEM_TYPE_REAL
#define MUL_REAL_PITEM_VALUE( RealItemPtr, Value )        ((RealItemPtr)->real *= Value); (RealItemPtr)->type = STACK_ITEM_TYPE_REAL
#define DIV_REAL_PITEM_VALUE( RealItemPtr, Value )        ((RealItemPtr)->real /= Value); (RealItemPtr)->type = STACK_ITEM_TYPE_REAL

#define SET_BITVECTOR_PITEM_VALUE( ItemPtr, Value )       ((ItemPtr)->bits = (Value)); (ItemPtr)->type = STACK_ITEM_TYPE_BITVECTOR


#define PAIR_TYPE( ItemPtrX, ItemPtrY )             (((ItemPtrX)->type << 8) | (ItemPtrY)->type)

#define NEXT_PITEM( Context )                       (++((Context)->sp))
#define POP_ITEM( Context )                         (*((Context)->sp--))
#define POP_PITEM( Context )                        ((Context)->sp--)
#define GET_PITEM( Context )                        (Context)->sp
#define IDX_PITEM( Context, Idx )                   ((Context)->sp - (Idx))
#define DISCARD_ITEMS( Context, N )                 ((Context)->sp -= (N))

#define SET_INTEGER_VALUE( Context, Value  )        (GET_PITEM( Context )->integer = (Value)); GET_PITEM( Context )->type = STACK_ITEM_TYPE_INTEGER
#define SET_REAL_VALUE( Context, Value  )           (GET_PITEM( Context )->real = (Value)); GET_PITEM( Context )->type = STACK_ITEM_TYPE_REAL

#define PUSH_INTEGER_VALUE( Context, Value )        (NEXT_PITEM( Context )->integer = (Value)); GET_PITEM( Context )->type = STACK_ITEM_TYPE_INTEGER
#define PUSH_REAL_VALUE( Context, Value )           (NEXT_PITEM( Context )->real = (Value)); GET_PITEM( Context )->type = STACK_ITEM_TYPE_REAL

static vgx_EvalStackItem_t __NAN_ITEM__ = { .type=STACK_ITEM_TYPE_NAN, .aux={0}, .real=(double)NAN };
#define SET_MEM_NAN( MemData, Offset ) ( *((MemData)+(Offset)) = __NAN_ITEM__ )
#define SET_NAN( ItemPtr )             ( *(ItemPtr) = __NAN_ITEM__ )

static vgx_EvalStackItem_t __NONE_ITEM__ = { .type=STACK_ITEM_TYPE_NONE, .aux={0}, .bits=0 };
#define SET_MEM_NONE( MemData, Offset ) ( *((MemData)+(Offset)) = __NONE_ITEM__ )
#define SET_NONE( ItemPtr )             ( *(ItemPtr) = __NONE_ITEM__ )

#define STACK_RETURN_INTEGER( self, Value )     \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( self );   \
  SET_INTEGER_PITEM_VALUE( __px, Value );       \
  return

#define STACK_RETURN_REAL( self, Value )      \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( self ); \
  SET_REAL_PITEM_VALUE( __px, Value );        \
  return

#define STACK_RETURN_NONE( self )             \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( self ); \
  *__px = __NONE_ITEM__;                      \
  return

#define STACK_RETURN_BITVECTOR( self, Value )   \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( self );   \
  SET_BITVECTOR_PITEM_VALUE( __px, Value );     \
  return

#define STACK_RETURN_ITEM( self, StackItemPtr ) \
  vgx_EvalStackItem_t *__px = NEXT_PITEM( self );   \
  *__px = *(StackItemPtr);                      \
  return


#define SET_STRING_IS_DYNAMIC( CString )     (VGX_CSTRING_FLAG__DYNAMIC( CString ) = 1)
#define IS_STRING_DYNAMIC( CString )          VGX_CSTRING_FLAG__DYNAMIC( CString )
#define IS_STRING_OWNABLE( CString )         (VGX_CSTRING_FLAG__DYNAMIC( CString ) == 0)

#define SET_STRING_IS_LITERAL( CString )     (VGX_CSTRING_FLAG__LITERAL( CString ) = 1)
#define IS_STRING_LITERAL( CString )          VGX_CSTRING_FLAG__LITERAL( CString )

#define SET_STRING_IS_PREFIX( CString )      (VGX_CSTRING_FLAG__PREFIX( CString ) = VGX_CSTRING_FLAG__WILDCARD( CString ) = 1)
#define IS_STRING_PREFIX( CString )           VGX_CSTRING_FLAG__PREFIX( CString )

#define SET_STRING_IS_INFIX( CString )       (VGX_CSTRING_FLAG__INFIX( CString ) = VGX_CSTRING_FLAG__WILDCARD( CString ) = 1)
#define IS_STRING_INFIX( CString )            VGX_CSTRING_FLAG__INFIX( CString )

#define SET_STRING_IS_SUFFIX( CString )      (VGX_CSTRING_FLAG__SUFFIX( CString ) = VGX_CSTRING_FLAG__WILDCARD( CString ) = 1)
#define IS_STRING_SUFFIX( CString )           VGX_CSTRING_FLAG__SUFFIX( CString )

#define IS_STRING_WILDCARD( CString )         VGX_CSTRING_FLAG__WILDCARD( CString )


#define RADIANS( Deg )  ((M_PI / 180.0) * (Deg))
#define DEGREES( Rad )  (((Rad) * 180.0) / M_PI)


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_EvalStackItem_t * vgx_evaluator_next_wreg( vgx_Evaluator_t *E ) {
  return E->context.wreg.cursor++;
}




#include "_cast.h"
#include "_string.h"
#include "_vertex.h"
#include "_arc.h"
#include "_collect.h"
#include "_constant.h"
#include "_conditional.h"
#include "_logical.h"
#include "_math.h"
#include "_memory.h"
#include "_heap.h"
#include "_probe.h"
#include "_machine.h"
#include "_pi8.h"
#include "_scalar.h"
#if defined CXPLAT_ARCH_X64
#include "_avx2.h"
#include "_avx512.h"
#elif defined CXPLAT_ARCH_ARM64
#include "_neon.h"
#endif
#include "_vector.h"
#include "_geo.h"
#include "_bitwise.h"
#include "_random.h"
#include "_rank.h"
#include "_stack.h"
#include "_object.h"
#include "_maps.h"




#endif
