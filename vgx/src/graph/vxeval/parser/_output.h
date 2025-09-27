/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _output.h
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

#ifndef _VGX_VXEVAL_PARSER_OUTPUT_H
#define _VGX_VXEVAL_PARSER_OUTPUT_H

static void __output__emit_operand( vgx_ExpressEvalProgram_t *program, __rpn_operation *op, vgx_EvalStackItem_t *stackitem );
static void __output__emit_operator( vgx_ExpressEvalProgram_t *program, __rpn_operation *op );
static int  __output__emit_ternary_conditional( vgx_ExpressEvalProgram_t *program, __shunt_stack *shuntstack );
static void __output__emit( vgx_ExpressEvalProgram_t *program, const __rpn_operation *op, const vgx_EvalStackItem_t *oparg );
static void __output__debug_emit( vgx_ExpressEvalProgram_t *program, const __rpn_operation *op, const vgx_EvalStackItem_t *oparg );
static int  __output__peek( vgx_ExpressEvalProgram_t *program, int lookback, f_evaluator *rfunc, vgx_EvalStackItem_t **rarg );
static f_evaluator __output__peek_top_func( vgx_ExpressEvalProgram_t *program );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __output__emit_operand( vgx_ExpressEvalProgram_t *program, __rpn_operation *op, vgx_EvalStackItem_t *stackitem ) {
  // Write the operation to output if we have one
  if( op ) {
    __output__emit( program, op, stackitem );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __output__emit_operator( vgx_ExpressEvalProgram_t *program, __rpn_operation *op ) {
  if( op && op->function.eval != __stack_noop ) {
    __output__emit( program, op, NULL );
    // Clear enumeration mode
    if( __is_infix_cmp_operator( op ) ) {
      program->parser._current_string_enum_mode = STACK_ITEM_TYPE_NONE;
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __output__emit_ternary_conditional( vgx_ExpressEvalProgram_t *program, __shunt_stack *shuntstack ) {
  // :
  __rpn_operation *shunt_colon = __shunt__pop_operator( shuntstack );
  if( shunt_colon && shunt_colon->type == OP_TERNARY_COLON_INFIX ) {
    __output__emit_operator( program, shunt_colon );
    // ?
    __rpn_operation *shunt_cond  = __shunt__pop_operator( shuntstack );
    if( shunt_cond && shunt_cond->type == OP_TERNARY_COND_INFIX ) {
      __output__emit_operator( program, shunt_cond );
      return 0; // ok
    }
  }
  // parser error
  return -1;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __output__emit( vgx_ExpressEvalProgram_t *program, const __rpn_operation *op, const vgx_EvalStackItem_t *oparg ) {
  // DEBUG
  if( program->debug.enable ) {
    __output__debug_emit( program, op, oparg );
  }

  // Keep track of eval stack elements
  if( op->type != OP_PASSTHRU ) {
    // Operator consumes n eval stack elements
    if( !__is_operand( op ) ) {
      int op_n = oparg ? (int)oparg->integer : 0;
      int n = __is_variadic_prefix( op ) ? op_n : __operator_arity( op );
      program->stack.eval_depth.run -= n;
    }

    // Push operand or operator result on eval stack
    if( ++(program->stack.eval_depth.run) > program->stack.eval_depth.max ) {
      program->stack.eval_depth.max = program->stack.eval_depth.run;
    }

    // Count
    program->length++;
  }
  else {
    program->n_passthru++;
  }

  // Assign function and argument to operation
  program->parser._cursor->func = op->function.eval;
  if( oparg ) {
    program->parser._cursor->arg = *oparg;
  }
  else {
    SET_NONE( &program->parser._cursor->arg );
  }

  // Advance
  program->parser._cursor++;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __output__debug_emit( vgx_ExpressEvalProgram_t *program, const __rpn_operation *op, const vgx_EvalStackItem_t *oparg ) {
  if( program ) {
    if( program->debug.info == NULL ) {
      if( (program->debug.info = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t )) == NULL ) {
        return;
      }
    }
    CStringQueue_t *q = program->debug.info;
    CStringQueue_vtable_t *iq = CALLABLE( q );
    char buf[33];

    const char *token = op->surface.token;

    if( token ) {
      iq->FormatNolock( q, "%s ", op->surface.token );
    }
    else if( oparg ) {
      switch( oparg->type & __STACK_ITEM_TYPE_MASK ) {
      case STACK_ITEM_TYPE_INTEGER:
        if( oparg->type == __STACK_ITEM_VARIABLE) {
          iq->FormatNolock( q, "var(%lld) ", oparg->integer );
          token = "var";
        }
        else {
          iq->FormatNolock( q, "%lld ", oparg->integer );
          token = "int";
        }
        break;
      case STACK_ITEM_TYPE_REAL:
        iq->FormatNolock( q, "%#g ", oparg->real );
        token = "real";
        break;
      case STACK_ITEM_TYPE_NAN:
        iq->FormatNolock( q, "nan " );
        token = "nan";
        break;
      case STACK_ITEM_TYPE_NONE:
        iq->FormatNolock( q, "null " );
        token = "null";
        break;
      case STACK_ITEM_TYPE_VERTEX:
        iq->FormatNolock( q, "%s ", idtostr( buf, __vertex_internalid( oparg->vertex ) ) );
        token = "vertex";
        break;
      case STACK_ITEM_TYPE_CSTRING:
        switch( CStringAttributes( oparg->CSTR__str ) & __CSTRING_ATTR_ARRAY_MASK ) {
        case CSTRING_ATTR_ARRAY_INT:
          iq->FormatNolock( q, "intarray (%d) @ %llp ", VGX_CSTRING_ARRAY_LENGTH( oparg->CSTR__str ), oparg->CSTR__str );
          break;
        case CSTRING_ATTR_ARRAY_FLOAT:
          iq->FormatNolock( q, "floatarray (%d) @ %llp ", VGX_CSTRING_ARRAY_LENGTH( oparg->CSTR__str ), oparg->CSTR__str );
          break;
        case CSTRING_ATTR_ARRAY_MAP:
          iq->FormatNolock( q, "map (%d) @ %llp ", vgx_cstring_array_map_len( (QWORD*)CStringValue( oparg->CSTR__str ) ), oparg->CSTR__str ); // TOOD: Find a way to extract map length
          break;
        default:
          iq->FormatNolock( q, "%s ", CStringValue( oparg->CSTR__str ) );
        }
        token = "str";
        break;
      case STACK_ITEM_TYPE_VECTOR:
        iq->FormatNolock( q, "vector @ %llp ", oparg->vector );
        token = "vector";
        break;
      case STACK_ITEM_TYPE_BITVECTOR:
        iq->FormatNolock( q, "bitvector 0x%016llx ", oparg->bits );
        token = "bitvec";
        break;
      case STACK_ITEM_TYPE_KEYVAL:
        iq->FormatNolock( q, "keyval (%d,%g) ", vgx_cstring_array_map_key( &oparg->bits ), vgx_cstring_array_map_val( &oparg->bits ) );
        token = "keyval";
        break;
      case STACK_ITEM_TYPE_VERTEXID:
        iq->FormatNolock( q, "vertex_id @ %llp ", oparg->vertexid );
        token = "vertexid";
        break;
      default:
        iq->FormatNolock( q, "<INVALID> " );
        token = "?";
      }
    }
    else {
      iq->FormatNolock( q, "<0x%04X> ", op->type );
    }

    iq->NulTermNolock( q );
    char *operations = NULL;
    iq->GetValueNolock( q, (void**)&operations );
    iq->UnwriteNolock( q, 1 );
    if( operations ) {
      const char *t = token ? token : "#";
      char spaces[] = "        ";
      int64_t x = strlen(spaces) - strlen(t);
      if( x < 0 ) {
        x = 0;
      }
      spaces[x] = '\0';
      printf( "                                   RPN(%s) %s %s\n", t, spaces, operations );
      ALIGNED_FREE( operations );
    }

  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __output__peek( vgx_ExpressEvalProgram_t *program, int lookback, f_evaluator *rfunc, vgx_EvalStackItem_t **rarg ) {

  vgx_ExpressEvalOperation_t *slot = program->parser._cursor - 1 - lookback;
  if( slot < program->operations ) {
    return -1;
  }

  if( rfunc ) {
    *rfunc = slot->func;
  }
  if( rarg ) {
    *rarg = &slot->arg;
  }

  return lookback;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static f_evaluator __output__peek_top_func( vgx_ExpressEvalProgram_t *program ) {
  vgx_ExpressEvalOperation_t *slot = program->parser._cursor - 1;
  return slot->func;
}




#endif
