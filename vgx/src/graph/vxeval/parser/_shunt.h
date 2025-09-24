/*
###################################################
#
# File:   _shunt.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_PARSER_SHUNT_H
#define _VGX_VXEVAL_PARSER_SHUNT_H



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static __shunt_stack *    __shunt__new( int maxdepth );
static void               __shunt__delete( __shunt_stack **stack );
static void               __shunt__debug( __shunt_stack *stack, const __rpn_operation *push, const __rpn_operation *pop );
static void               __shunt__push_operator( __shunt_stack *stack, const __rpn_operation *op );
static __rpn_operation *  __shunt__pop_operator( __shunt_stack *stack );
static int                __shunt__top_is_operator_greater_precedence( const __shunt_stack *stack, const __rpn_operation *op );
static int                __shunt__top_is_prefix_operator( const __shunt_stack *stack );
static int                __shunt__top_is_open_group( const __shunt_stack *stack );
static int                __shunt__top_is_group_separator( const __shunt_stack *stack );
static int                __shunt__top_is_open_bracket( const __shunt_stack *stack );
static int                __shunt__top_is_open_brace( const __shunt_stack *stack );
static int                __shunt__top_is_contains_operator( const __shunt_stack *stack );
static int                __shunt__top_is_call( const __shunt_stack *stack, f_evaluator *call );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static __shunt_stack * __shunt__new( int maxdepth ) {
  __shunt_stack *stack = calloc( 1, sizeof( __shunt_stack ) );
  if( stack ) {
    if( (stack->slots = calloc( 1 + maxdepth, sizeof( __rpn_operation ) )) == NULL ) { // dummy slot + max
      __shunt__delete( &stack );
      return NULL;
    }
    stack->sp = stack->slots;
    stack->depth = 0;
    stack->cnt = 0; // count the number of actions performed on the stack
    stack->debug = false;
    *stack->sp = __rpn_operation_noop; // noop as 0th item
  }
  return stack;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __shunt__delete( __shunt_stack **stack ) {
  if( stack && *stack ) {
    free( (*stack)->slots );
    free( *stack );
    *stack = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __shunt__debug( __shunt_stack *stack, const __rpn_operation *push, const __rpn_operation *pop ) {
  const char *token;
  if( push ) {
    token = push->surface.token;
    printf( "stack(%s) ", token ? token : "#" );
  }
  else if( pop ) {
    token = pop->surface.token;
    printf( "%s stack ", token ? token : "#" );
  }
  else {
    printf( "stack " );
  }

  __rpn_operation *dcur = stack->slots + 1; // skip initial dummy slot
  while( dcur <= stack->sp ) {
    if( dcur->surface.token ) {
      printf( "%s ", dcur->surface.token );
    }
    else {
      printf( "<0x%04X> ", dcur->type );
    }
    ++dcur;
  }
  printf( "\n" );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __shunt__push_operator( __shunt_stack *stack, const __rpn_operation *op ) {
  *++(stack->sp) = *op;
  stack->depth++;
  stack->cnt++;

  // DEBUG
  if( stack->debug ) {
    __shunt__debug( stack, op, NULL );
  }

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static __rpn_operation * __shunt__pop_operator( __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    stack->depth--;
    stack->cnt++;
    __rpn_operation *op = stack->sp--;

    // DEBUG
    if( stack->debug ) {
      __shunt__debug( stack, NULL, op );
    }

    return op;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_operator_greater_precedence( const __shunt_stack *stack, const __rpn_operation *op ) {
  if( stack->depth > 0 && !__is_group_operator( stack->sp ) ) {
    int opp_stack = __get_precedence( stack->sp );
    int opp_this = __get_precedence( op );
    if( __is_left_associative( op ) ) {
      return opp_stack >= opp_this;
    }
    else {
      return opp_stack > opp_this;
    }
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_prefix_operator( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return (stack->sp->type & __OP_CLS_MASK) == __OP_CLS_PREFIX;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_open_group( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return stack->sp->type == OP_LEFT_PAREN;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_group_separator( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return stack->sp->type == OP_COMMA || stack->sp->type == OP_LEFT_PAREN || stack->sp->type == OP_LEFT_BRACE;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_open_bracket( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return stack->sp->type == OP_LEFT_BRACKET;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_open_brace( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return stack->sp->type == OP_LEFT_BRACE;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_contains_operator( const __shunt_stack *stack ) {
  if( stack->depth > 0 ) {
    return __eq_operation( stack->sp, &RpnBinaryElementOf ) || __eq_operation( stack->sp, &RpnBinaryNotElementOf );
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __shunt__top_is_call( const __shunt_stack *stack, f_evaluator *call )  {
  if( stack->depth > 1 && stack->sp->type == OP_LEFT_PAREN ) {
    *call = (stack->sp-1)->function.eval;
    return 1;
  }
  return 0;
}

#endif
