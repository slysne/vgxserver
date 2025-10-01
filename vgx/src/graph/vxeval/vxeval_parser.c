/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxeval_parser.c
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

#include "_vxeval.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

#include "modules/eval.h"
#include "rpndef/_rpndef.h"
#include "parser/_synerr.h"
#include "parser/_tokenizer.h"
#include "parser/_shunt.h"
#include "parser/_varmap.h"
#include "parser/_enum.h"
#include "parser/_numeric.h"
#include "parser/_string.h"
#include "parser/_output.h"





/******************************************************************************
 *
 ******************************************************************************
 */
DLL_HIDDEN express_eval_map *_vxeval_operations = NULL;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxeval_parser__initialize( void ) {
  int ret = 0;
  XTRY {

    // Tokenizer
    if( _vxeval_parser__init_tokenizer() < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x121 );
    }

    // String Normalizer
    if( _vxeval_parser__init_normalizer() < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x122 );
    }

    // String constants
    if( __string__initialize_constants() < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x123 );
    }

    // Opmap
    if( (_vxeval_operations = __rpndef__opmap_new( "evaluator rpn operations" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x125 );
    }
  }
  XCATCH( errcode ) {
    _vxeval_parser__destroy();
    ret = -1;
  }
  XFINALLY {
  }
  return ret;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxeval_parser__destroy( void ) {
  // Destroy the tokenizer
  _vxeval_parser__del_tokenizer();

  // Destroy the string normalizer
  _vxeval_parser__del_normalizer();

  // String constants
  __string__destroy_constants();

  // Destroy the operator map
  __rpndef__opmap_delete( &_vxeval_operations );

  return 0;
}



/*******************************************************************//**
 * Return   1: Cloned an existing evaluator and returned it into **evaluator
 *          0: No existing evaluator found
 *         -1: Error
 *                                                                   
 ***********************************************************************
 */
DLL_HIDDEN int _vxeval_parser__get_evaluator( vgx_Graph_t *graph, const char *infix_expression, vgx_Vector_t *vector, vgx_Evaluator_t **evaluator ) {
  int ret = 0;
  const char *funckey = __tokenizer__skip_ignorable( infix_expression );



  // Clone the existing operation list if it exists.
  if( funckey ) {

    // Find length of function name token
    const char *p = funckey;
    while( *p != '\0' && *p != ' ' && *p != '/' ) {
      ++p;
    }
    int64_t len = p - funckey;
    // Everything else after function name should be ignorable, otherwise invalid name lookup
    if( (p = __tokenizer__skip_ignorable( p )) != NULL && *p != '\0' ) {
      return 0;
    }

    vgx_Evaluator_t *eobj = NULL;
    framehash_t *E = graph->evaluators;
    objectid_t obid;
    obid.H = obid.L = hash64( (unsigned char*)funckey, len );
    GRAPH_LOCK( graph ) {
      if( CALLABLE( E )->GetObj128Nolock( E, &obid, (comlib_object_t**)&eobj ) == CELL_VALUE_TYPE_OBJECT128 ) {
        if( (*evaluator = CALLABLE( eobj )->Clone( eobj, vector )) == NULL ) {
          ret = -1;
        }
        else {
          ret = 1;
        }
      }
    } GRAPH_RELEASE;
  }
  return ret;
}



/*******************************************************************//**
 * 
 * Return   1: Created new rpn from expression
 *          0: Loaded from previous assignment
 *         -1: Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxeval_parser__create_rpn_from_infix( vgx_ExpressEvalProgram_t *program, vgx_Graph_t *graph, const char *infix_expression, CString_t **CSTR__error  ) {
  int ret = 0;

  __tokenizer_context *tokenizer = NULL;

  parser_state STATE = EXPECT_OPERAND;
  parser_state PREV_STATE = STATE;

#define SYNTAX_ERROR( Message ) __synerr__syntax_error( tokenizer, Message ); continue

#define INTERNAL_ERROR( Message ) __synerr__internal_error( tokenizer, Message ); continue

#define NEXT_TOKEN( State ) (STATE = State); continue

#define JUMP_STATE( Label ) goto Label

  __shunt_stack *shuntstack = NULL;

   CString_t *CSTR__literal = NULL;

  __varmap *varmap = NULL;

  __subexpression subexpression = {0};


  XTRY {
    // Parser metas
    program->parser._cursor = NULL;
    program->parser._sz = 0;
    program->parser._current_string_enum_mode = STACK_ITEM_TYPE_NONE;

    // Create tokenizer context and run core tokenization
    if( (tokenizer = __tokenizer__new_context( _vxeval_parser__tokenizer, infix_expression, CSTR__error )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x141 );
    }

    // Allocate stack for shunting algorithm - use number of tokens as size hint, will over-allocate
    if( (shuntstack = __shunt__new( 2*tokenizer->ntokens )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x142 );
    }

    // Create variable map
    if( (varmap = __varmap__new()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x143 );
    }

    // Allocate output for shunting algorithm - use number of tokens as size hint
    program->parser._sz = 2*tokenizer->ntokens + 1 + 1;
    CALIGNED_ARRAY_THROWS( program->operations, vgx_ExpressEvalOperation_t, program->parser._sz, 0x144 );
    memset( program->operations, 0, sizeof( vgx_ExpressEvalOperation_t ) * program->parser._sz );
    program->parser._cursor = program->operations;

    // Deref
    program->deref.tail = 0;
    program->deref.this = 0;
    program->deref.head = 0;
    program->deref.arc = 0;
    program->identifiers = 0;
    program->cull = 0;
    program->synarc_ops = 0;
    program->n_wreg = 0;

    __rpn_operation *mapped_rpn_operation = NULL;
    __rpn_operation *shunt_op = NULL;
    __rpn_operation *current_op = &RpnNoopPrefix;
    __rpn_operation openparen_op = __rpn_operation_open_paren;
    __rpn_operation openbracket_op = __rpn_operation_open_bracket;
    __rpn_operation openbrace_op = __rpn_operation_open_brace;
    __rpn_operation comma_op = __rpn_operation_comma;
    static const int max_args_variadic = INT_MAX;
    int group_nesting = 0;
    int brace_nesting = 0;
    int bracket_nesting = 0;
    int current_arg_idx = -1;
    int min_arg_idx = -1;
    int end_arg_idx = -1;
    int op_count = 0;


    const char *assign_name = NULL;


    vgx_EvalStackItem_t stackitem;

    f_evaluator op_function = NULL;

    // Iterate over each token
    subexpression.capture_at = true;
    subexpression.index = 1; // start at 1
    const char *token = NULL;
    const char *last_token = NULL;
    while( (token = __tokenizer__next_token( tokenizer )) != NULL ) {

      last_token = token;
      
      // Next subexpression
      if( subexpression.capture_at ) {
        __capture_next_subexpression( &subexpression, program, shuntstack );
      }
      
    SWITCH_STATE:
      switch( STATE ) {

      // ######################
      // ###            +   ###
      // ###            -   ###
      // ###            (   ###
      // ###            {   ###
      // ###    <literal>   ###
      // ###   <variable>   ###
      // ###     <prefix>   ###
      // ######################
      case EXPECT_OPERAND:
      case EXPECT_OPERAND_OR_END:
        switch( tokenizer->initial ) {
        case ')':
        case '}':
          SYNTAX_ERROR( "expected operand" );
        default:
          break;
        }
        /* FALLTHRU */

      // ######################
      // ###            +   ###
      // ###            -   ###
      // ###            (   ###
      // ###            )   ###
      // ###            {   ###
      // ###            }   ###
      // ###    <literal>   ###
      // ###   <variable>   ###
      // ###     <prefix>   ###
      // ######################
      case EXPECT_ANY:
        // Number
        if( tokenizer->tokinfo.typ & STRING_MASK_NUMBER || tokenizer->initial == '0' ) {
          JUMP_STATE( LITERAL );
        }
        else if( tokenizer->tokinfo.len ) {
          switch( tokenizer->initial ) {
          // Unary plus
          case '+':
            NEXT_TOKEN( EXPECT_OPERAND );
          // Unary minus
          case '-':
            current_op = &RpnUnaryMinus;
            __shunt__push_operator( shuntstack, current_op );
            NEXT_TOKEN( EXPECT_OPERAND );
          // String
          case '"':
          case '\'':
            JUMP_STATE( LITERAL );
          case 'b':
            if( __tokenizer__immediate_next_chars( tokenizer, "'\"" ) ) {
              JUMP_STATE( LITERAL );
            }
            break;
          // Open group
          case '(':
            current_op = &RpnGroup;
            JUMP_STATE( OPEN_PAREN );
          // Open set
          case '{':
            if( __eq_operation( current_op, &RpnBinaryElementOf ) || __eq_operation( current_op, &RpnBinaryNotElementOf ) ||  ( program->length == 0 && shuntstack->cnt == 0 ) ) {
              current_op = &RpnSet;
              JUMP_STATE( OPEN_BRACE );
            }
            else {
              SYNTAX_ERROR( "invalid set context" );
            }
          case ',':
          case '[':
          case ']':
            SYNTAX_ERROR( "expected operand" );
          }
        }
        
        /* FALLTHRU */

      // ######################
      // ###            )   ###
      // ###            }   ###
      // ###            ,   ###
      // ###            ;   ###
      // ###            [   ###
      // ###            ]   ###
      // ###   <variable>   ###
      // ###     <prefix>   ###
      // ###      <infix>   ###
      // ######################
      case EXPECT_INFIX:
        // Close group or group separator 
        switch( tokenizer->initial ) {
        case ')':
          JUMP_STATE( CLOSE_PAREN );
        case '}':
          JUMP_STATE( CLOSE_BRACE );
        case ',':
          current_op = &RpnComma;
          JUMP_STATE( COMMA );
        case ';':
          current_op = &RpnSemicolon;
          JUMP_STATE( SEMICOLON );
        case ']':
          JUMP_STATE( CLOSE_BRACKET );
        default:
          break;
        }

        // ---------------------------------------------
        // A vertex object was just pushed on the stack.
        // ---------------------------------------------
        if( __is_vertex_object_operand( current_op ) ) {
          // Pop the vertex from the stack for processing
          shunt_op = __shunt__pop_operator( shuntstack );
          // Vertex property lookup
          if( tokenizer->initial == '[' ) {
            // Tail subscript
            if( __eq_operation( shunt_op, &RpnPushTail ) ) {
              shunt_op = &RpnPushTailProperty;
              program->deref.tail++;
            }
            // This subscript
            else if( __eq_operation( shunt_op, &RpnPushThis ) ) {
              shunt_op = &RpnPushThisProperty;
              program->deref.this++;
            }
            // Head subscript (flag deref and traversal)
            else if( __eq_operation( shunt_op, &RpnPushHead ) ) {
              shunt_op = &RpnPushHeadProperty;
              program->deref.head++;
            }
            // Error
            else {
              INTERNAL_ERROR( "invalid subscript" );
            }
            // Push the subscript operation and parse
            __shunt__push_operator( shuntstack, shunt_op );
            JUMP_STATE( OPEN_BRACKET );
          }
          // Emit vertex object 
          else {
            __output__emit_operand( program, shunt_op, NULL );
          }
        }


        // ------------------------------------------------------------------------
        // Token is mapped to a pre-defined rpn operation with an assigned function
        // ------------------------------------------------------------------------
        if( (mapped_rpn_operation = __rpndef__opmap_get( _vxeval_operations, token )) != NULL ) {
          current_op = mapped_rpn_operation;
          if( (op_function = mapped_rpn_operation->function.eval) != NULL ) {
            // Passthru
            // Meta-instruction to be evaluated when executing rpn
            if( current_op->type == OP_PASSTHRU ) {
              __output__emit_operator( program, current_op );
              NEXT_TOKEN( STATE );
            }
            // Expect infix operator
            else if( STATE == EXPECT_INFIX ) {
              // Infix Operator (e.g. +, *, <<, ?, :, etc. )
              if( !__is_infix_operator( current_op ) ) {
                SYNTAX_ERROR( "expected infix operator" );
              }

              // Emit all greater precedence operators on stack before processing current operator
              while( __shunt__top_is_operator_greater_precedence( shuntstack, current_op ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
                __output__emit_operator( program, shunt_op );
              }

              // Ternary conditional processing: Before pushing ':' of current '?:' emit all preceding '?:'
              if( current_op->type == OP_TERNARY_COLON_INFIX ) {
                if( shuntstack->sp->type == OP_TERNARY_COND_INFIX || shuntstack->sp->type == OP_TERNARY_COLON_INFIX ) {
                  while( shuntstack->sp->type == OP_TERNARY_COLON_INFIX ) {
                    if( __output__emit_ternary_conditional( program, shuntstack ) < 0 ) {
                      SYNTAX_ERROR( "invalid ternary conditional '?:'" );
                    }
                  }
                }
                else {
                  SYNTAX_ERROR( "incomplete ternary conditional '?:'" );
                }
              }

              // Push operator
              __shunt__push_operator( shuntstack, current_op );

              // Operators other than unary infix require operand next
              if( __operator_arity( current_op ) > 1 ) {
                NEXT_TOKEN( EXPECT_OPERAND );
              }
              else {
                NEXT_TOKEN( EXPECT_INFIX );
              }
            }
            // Expect operand or prefix operator
            else {
              // Count operations requiring head dereference
              program->deref.tail += __is_tail_deref_operand( current_op );
              program->deref.this += __is_this_deref_operand( current_op );
              program->deref.head += __is_head_deref_operand( current_op );
              program->deref.arc += __is_traverse_operand( current_op );
              program->synarc_ops += __is_synarc_operand( current_op );

              // Variable
              // Symbolic or Literal Operand goes straight to output because it is fully defined without further parsing ( e.g. vertex.deg, next.type, next.arc.type, pi, etc. )
              if( __is_operand( current_op ) ) {
                __enum__conditional_args( graph, program, shuntstack, current_op, NULL );
                __output__emit_operand( program, current_op, NULL );
                NEXT_TOKEN( EXPECT_INFIX );
              }
              else {
                // Variable
                // Subscript Operand requires parsing of key before it can be completed and go to the output ( e.g. rel[ "knows" ], type[ "node" ], etc. )
                if( __is_subscript_operand( current_op ) ) {
                  __shunt__push_operator( shuntstack, current_op );
                  NEXT_TOKEN( EXPECT_OPEN_BRACKET );
                }
                // Variable
                // Object Pointer Operand can either stand on its own (address) or take a subscript (e.g. prev, vertex, next[ "prop1" ], etc. )
                else if( __is_object_operand( current_op ) ) {
                  // Enumerate property if "in" operator preceded object without subscript
                  if( !__tokenizer__is_next_token( tokenizer, "[" ) && __enum__conditional_args( graph, program, shuntstack, current_op, NULL ) ) {
                    // Account for the deref of head in this case, since by default the head address on its own does not require deref and was not accounted for above
                    if( current_op->type == OP_HEAD_VERTEX_OBJECT ) {
                      program->deref.head++;
                    }
                  }
                  if( __is_vertexid_object_operand( current_op ) ) {
                    program->identifiers++;
                  }
                  __shunt__push_operator( shuntstack, current_op );
                  NEXT_TOKEN( EXPECT_INFIX );
                }
                // Function
                // Prefix Operator ( e.g. sin(x), max(a, b), etc. )
                else if( __is_prefix_operator( current_op ) ) {
                  // Special verification for "in range(...)"
                  if( __eq_operation( current_op, &RpnBinaryRange ) && !__shunt__top_is_contains_operator( shuntstack ) ) {
                    SYNTAX_ERROR( "unexpected range()" );
                  }

                  // Expression contains the mcull operator
                  if( __eq_operation( current_op, &RpnMCull ) || __eq_operation( current_op, &RpnMCullIf ) ) {
                    // Multiple cull not allowed
                    if( program->cull > 0 ) {
                      SYNTAX_ERROR( "multiple mcull() not allowed" );
                    }
                    program->cull = 1; // Will be overridden with correct cull size later
                  }
                  
                  // Operator requires a memory work slot
                  if( __is_workslot_operator( current_op ) ) {
                    ++program->n_wreg;
                  }

                  __shunt__push_operator( shuntstack, current_op );
                  if( current_op->precedence == OPP_CALL ) {
                    NEXT_TOKEN( EXPECT_OPEN_PAREN );
                  }
                  else {
                    NEXT_TOKEN( EXPECT_OPERAND );
                  }
                }
                // Syntax Error
                else {
                  SYNTAX_ERROR( "expected operand or function call" );
                }
              }
            }
          }
          // Token is mapped to a pre-defined rpn operation without a function
          else {
            if( current_op->surface.token ) {
              if( __eq_operation( current_op, &RpnDebugTokenizer ) ) {
                tokenizer->debug = true;
                NEXT_TOKEN( STATE );
              }
              else if( __eq_operation( current_op, &RpnDebugStack ) ) {
                shuntstack->debug = true;
                NEXT_TOKEN( STATE );
              }
              else if( __eq_operation( current_op, &RpnDebugRpn ) ) {
                program->debug.enable = true;
                NEXT_TOKEN( STATE );
              }
              else if( __eq_operation( current_op, &RpnDebugMemory ) ) {
                // TODO!
                NEXT_TOKEN( STATE );
              }
              else if( __eq_operation( current_op, &RpnAssign ) ) {
                SYNTAX_ERROR( "invalid expression name assignment" );
              }
              else if( __eq_operation( current_op, &RpnAssignVariable ) ) {
                subexpression.at.program_length++;
                if( __is_at_subexpression( &subexpression, program, shuntstack ) && __output__peek_top_func( program ) == RpnPushVariable.function.eval ) {
                  SYNTAX_ERROR( "variable redefinition" );
                }
                else {
                  SYNTAX_ERROR( "invalid variable assignment" );
                }
              }
              else {
                INTERNAL_ERROR( "unexpected operator" );
              }
            }
          }
        }
        // Rest of line is comment
        else if( CharsEqualsConst( token, "//" ) ) {
          PREV_STATE = STATE;
          STATE = EXPECT_IGNORE_LINE;
          NEXT_TOKEN( STATE );
        }
        // Begin comment
        else if( CharsEqualsConst( token, "/*" ) ) {
          PREV_STATE = STATE;
          STATE = EXPECT_IGNORE;
          tokenizer->escape = true;
          NEXT_TOKEN( STATE );
        }
        // Token is not a known operation, but it is a storable key, which means
        // we are possibly assigning something or using a variable
        else if( iString.Validate.StorableKey( token ) ) {
          bool at_next = __is_at_subexpression( &subexpression, program, shuntstack );
          // Is this an existing variable?
          __rpn_variable *var = __varmap__variable_get( varmap, token );
          if( var ) {
            stackitem.integer = var->subexpr_idx;
            stackitem.type = __STACK_ITEM_VARIABLE;
            current_op = &RpnPushVariable;
            __output__emit_operand( program, current_op, &stackitem );
            NEXT_TOKEN( EXPECT_INFIX );
          }
          // Unknown variable, speculatively assume we are assigning if at start of next subexpression
          else if( at_next && subexpression.var == NULL ) {
            assign_name = token;
            NEXT_TOKEN( EXPECT_ASSIGNMENT );
          }
          else {
            SYNTAX_ERROR( "undefined variable" );
          }
        }
        else {
          SYNTAX_ERROR( "invalid expression" );
        }

      // #################
      // ### // ...... ###
      // #################
      case EXPECT_IGNORE_LINE:
        // First token in new line ends comment
        if( tokenizer->tokinfo.lin ) {
          if( (STATE = PREV_STATE) != EXPECT_IGNORE_LINE ) { // (safeguard)
            JUMP_STATE( SWITCH_STATE );
          }
        }
        NEXT_TOKEN( STATE );

      // #################
      // ### /* ... */ ###
      // #################
      case EXPECT_IGNORE:
        // End comment
        if( CharsEndsWithConst( token, "*/" ) ) {
          STATE = PREV_STATE;
          tokenizer->escape = false;
        }
        NEXT_TOKEN( STATE );

      // ###################
      // ###  :=  or  =  ###
      // ###################
      case EXPECT_ASSIGNMENT:
        if( assign_name == NULL || !_vxenum__is_valid_storable_key( assign_name ) ) {
          SYNTAX_ERROR( "invalid name" );
        }
        // :=  Assign entire expression to this function name for later recall
        if( __eq_operation( __rpndef__opmap_get( _vxeval_operations, token ), &RpnAssign ) ) {
          // Verify we are at the very beginning
          if( program->length > 0 || shuntstack->cnt > 0 ) {
            SYNTAX_ERROR( "cannot assign function within expression" );
          }
          // Already assigned
          if( program->CSTR__assigned ) {
            SYNTAX_ERROR( "invalid function assignment" );
          }
          // Assign expression to a name
          if( (program->CSTR__assigned = NewEphemeralCString( graph, assign_name )) == NULL ) {
            INTERNAL_ERROR( "out of memory" );
          }
          NEXT_TOKEN( EXPECT_OPERAND );
        }
        // =   Assign current variable name to denote the evaluated value of current subexpression 
        else if( __eq_operation( __rpndef__opmap_get( _vxeval_operations, token ), &RpnAssignVariable ) ) {
          // Is this a valid point in the expression for assigning a variable?
          if( !__is_at_subexpression( &subexpression, program, shuntstack ) ) {
            SYNTAX_ERROR( "cannot assign variable within expression" );
          }
          // Should not already exist
          if( __varmap__variable_get( varmap, assign_name ) ) {
            SYNTAX_ERROR( "duplicate variable assignment" );
          }
          // Current subexpression should not already be assigned to a variable
          if( subexpression.var != NULL ) {
            INTERNAL_ERROR( "variable already assigned for subexpression" );
          }
          // Make a new variable that represents current subexpression about to be parsed
          if( (subexpression.var = __varmap__variable_new( assign_name, subexpression.index )) == NULL ) {
            INTERNAL_ERROR( "out of memory" );
          }
          NEXT_TOKEN( EXPECT_OPERAND );
        }
        // Error
        if( program->length == 0 && shuntstack->cnt == 0 ) {
          SYNTAX_ERROR( "expected ':=' or '='" );
        }
        else {
          SYNTAX_ERROR( "undefined variable" );
        }

      // #############
      // ###   (   ###
      // #############
      case EXPECT_OPEN_PAREN:
        if( tokenizer->initial != '(' ) {
          SYNTAX_ERROR( "expected '('" );
        }
      OPEN_PAREN:
        if( !__is_prefix_operator( current_op ) ) {
          INTERNAL_ERROR( "open group not prefix" );
        }
        // Start new group
        if( __eq_operation( current_op, &RpnGroup ) ) {
          __shunt__push_operator( shuntstack, current_op );
        }
        // Save state information for the immediate outer group
        openparen_op.function.meta.current_arg_idx = current_arg_idx;
        openparen_op.function.meta.end_arg_idx = end_arg_idx;
        // Push the '(' open group operator on the stack
        __shunt__push_operator( shuntstack, &openparen_op );
        ++group_nesting;
        // The new group is an argument list for a prefix operator
        op_count = program->length;
        current_arg_idx = 0;
        // Variable number of arguments
        if( __is_variadic_prefix( current_op ) ) {
          __variadic_arg_counts( current_op, &min_arg_idx, &end_arg_idx );
          NEXT_TOKEN( EXPECT_ANY );
        }
        // One or more arguments, expect at least one operand
        else {
          if( (min_arg_idx = end_arg_idx = __operator_arity( current_op )) > 0 ) {
            NEXT_TOKEN( EXPECT_OPERAND );
          }
          else {
            NEXT_TOKEN( EXPECT_CLOSE_PAREN );
          }
        }
      
      // #############
      // ###   )   ###
      // #############
      case EXPECT_CLOSE_PAREN:
        if( tokenizer->initial != ')' ) {
          SYNTAX_ERROR( "expected ')'" );
        }
      CLOSE_PAREN:
        {
          int n_commas = 0;
          // Unwind stack until opening of group, output all operators found except commas which are discarded
          while( !__shunt__top_is_open_group( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
            if( __is_comma_operator( shunt_op ) ) {
              ++n_commas;
            }
            else {
              __output__emit_operator( program, shunt_op );
            }
          }
          // Discard the group opener, restore state of previous group, write any prefix operator to output
          if( __shunt__top_is_open_group( shuntstack ) ) {
            // Pop the '(' 
            __rpn_operation *open_group = __shunt__pop_operator( shuntstack );
            // Output immediate prefix operator if one exists - include the number of args received
            if( __shunt__top_is_prefix_operator( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
              // Complete the arg count if anything has been emitted since group was opened
              if( program->length > op_count ) {
                ++current_arg_idx;
              }
              // Check argument count
              if( end_arg_idx >= 0 && end_arg_idx < max_args_variadic && current_arg_idx != end_arg_idx ) {
                if( current_arg_idx < min_arg_idx ) {
                  SYNTAX_ERROR( "not enough arguments" );
                }
                else if( current_arg_idx > end_arg_idx ) {
                  SYNTAX_ERROR( "too many arguments" );
                }
              }
              // Group
              if( __eq_operation( shunt_op, &RpnGroup ) ) {
                if( current_arg_idx == 0 ) {
                  SYNTAX_ERROR( "empty group" );
                }
                // Mute operator if it has no effect
                if( n_commas == 0 ) {
                  shunt_op = NULL;
                }
              }
              // mcull( score, k )
              else if( __eq_operation( shunt_op, &RpnMCull ) || __eq_operation( shunt_op, &RpnMCullIf ) ) {
                f_evaluator rfunc; 
                vgx_EvalStackItem_t *rarg;
                // Retrieve k from emitted output
                if( __output__peek( program, 0, &rfunc, &rarg ) < 0 ) {
                  INTERNAL_ERROR( "mcull parser error" );
                }
                // Verify k
                if( rfunc != RpnPushConstantInt.function.eval || rarg->integer < 1 ) {
                  SYNTAX_ERROR( "mcull() size must be a positive integer literal" );
                }
                // Cull size = k
                program->cull = rarg->integer;
              }

              // Set actual number of arguments received
              stackitem.integer = current_arg_idx;
              stackitem.type = STACK_ITEM_TYPE_INTEGER;
            }
            // Error
            else {
              INTERNAL_ERROR( "shunt stack no prefix or empty" );
            }
            // Restore previous state
            current_arg_idx = open_group->function.meta.current_arg_idx;
            end_arg_idx = open_group->function.meta.end_arg_idx;
            // Clear enumeration mode
            program->parser._current_string_enum_mode = STACK_ITEM_TYPE_NONE;
            // Emit
            __output__emit_operand( program, shunt_op, &stackitem );
            --group_nesting;
            // Clear current op
            current_op = &RpnNoop;
            NEXT_TOKEN( EXPECT_INFIX );
          }
        }
        // Error
        SYNTAX_ERROR( "missing '('" );

      // #############
      // ###   ,   ###
      // #############
      case EXPECT_COMMA:
      COMMA:
        if( group_nesting < 1 && brace_nesting < 1 ) {
          SYNTAX_ERROR( "unexpected ','" );
        }
        // Process stack back to the previous separator or the opening of the group
        // Output all operators for current element in group
        while( !__shunt__top_is_group_separator( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
          __output__emit_operator( program, shunt_op );
        }
        // Push the comma on the stack
        __shunt__push_operator( shuntstack, &comma_op );
        // Advance
        if( end_arg_idx < 0 || ++current_arg_idx < end_arg_idx ) {
          NEXT_TOKEN( EXPECT_OPERAND );
        }
        SYNTAX_ERROR( "too many arguments" );

      // #############
      // ###   ;   ###
      // #############
      case EXPECT_SEMICOLON:
      SEMICOLON:
        if( group_nesting == 0 && brace_nesting == 0 ) {
          // Commit variable assignment
          if( __varmap__variable_add( varmap, __subexpression_pop_variable( &subexpression ) ) < 0 ) {
            INTERNAL_ERROR( "variable assignment error" );
          }
          // Advance subexpression
          __end_subexpression( &subexpression );
          // Process stack back to the previous separator or the opening of the group
          // Output all operators for current element in group
          while( !__shunt__top_is_group_separator( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
            __output__emit_operator( program, shunt_op );
          }
          current_op = &RpnNoopPrefix;
          NEXT_TOKEN( EXPECT_OPERAND_OR_END );
        }
        SYNTAX_ERROR( "invalid subexpression" );

      // #############
      // ###   {   ###
      // #############
      case EXPECT_OPEN_BRACE:
        if( tokenizer->initial != '{' ) {
          SYNTAX_ERROR( "expected '{'" );
        }
      OPEN_BRACE:
        if( !__is_prefix_operator( current_op ) ) {
          INTERNAL_ERROR( "open brace not prefix" );
        }
        // Start new set
        if( __eq_operation( current_op, &RpnSet ) ) {
          __shunt__push_operator( shuntstack, current_op );
        }
        // Save state information for the immediate outer group
        openbrace_op.function.meta.current_arg_idx = current_arg_idx;
        openbrace_op.function.meta.end_arg_idx = end_arg_idx;
        // Push the '{' open set operator on the stack
        __shunt__push_operator( shuntstack, &openbrace_op );
        ++brace_nesting;
        // The new set is an argument list for a prefix operator
        op_count = program->length;
        current_arg_idx = 0;
        end_arg_idx = max_args_variadic;
        NEXT_TOKEN( EXPECT_OPERAND );

      // #############
      // ###   }   ###
      // #############
      case EXPECT_CLOSE_BRACE:
        if( tokenizer->initial != '}' ) {
          SYNTAX_ERROR( "expected '}'" );
        }
      CLOSE_BRACE:
        {
          int n_commas = 0;
          // Unwind stack until opening of set, output all operators found except commas which are discarded
          while( !__shunt__top_is_open_brace( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
            if( __is_comma_operator( shunt_op ) ) {
              ++n_commas;
            }
            else {
              __output__emit_operator( program, shunt_op );
            }
          }
          // Discard the set opener, restore state of any previous group, write any prefix operator to output
          if( __shunt__top_is_open_brace( shuntstack ) ) {
            // Pop the '{' 
            __rpn_operation *open_set = __shunt__pop_operator( shuntstack );
            // Output immediate prefix operator if one exists - include the number of args received
            if( __shunt__top_is_prefix_operator( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
              // Complete the arg count if anything has been emitted since set was opened
              if( program->length > op_count ) {
                ++current_arg_idx;
              }
              // Confirm set op
              if( !__eq_operation( shunt_op, &RpnSet ) ) {
                INTERNAL_ERROR( "shunt stack missing set operator" );
              }
              // Confirm non-empty
              if( current_arg_idx == 0 ) {
                SYNTAX_ERROR( "empty set" );
              }
              // Set actual number of arguments received
              stackitem.integer = current_arg_idx;
              stackitem.type = STACK_ITEM_TYPE_SET;
            }
            // Error
            else {
              INTERNAL_ERROR( "shunt stack no prefix or empty" );
            }
            // Restore previous state
            current_arg_idx = open_set->function.meta.current_arg_idx;
            end_arg_idx = open_set->function.meta.end_arg_idx;
            // Clear enumeration mode
            program->parser._current_string_enum_mode = STACK_ITEM_TYPE_NONE;
            // Emit
            __output__emit_operand( program, shunt_op, &stackitem );
            --brace_nesting;
            NEXT_TOKEN( EXPECT_INFIX );
          }
        }
        // Error
        SYNTAX_ERROR( "missing '{'" );

      // #############
      // ###   [   ###
      // #############
      case EXPECT_OPEN_BRACKET:
        if( tokenizer->initial != '[' ) {
          SYNTAX_ERROR( "expected '['" );
        }
      OPEN_BRACKET:
        // Push the open bracket operator on the stack
        __shunt__push_operator( shuntstack, &openbracket_op );
        ++bracket_nesting;
        NEXT_TOKEN( EXPECT_OPERAND );

      // #############
      // ###   ]   ###
      // #############
      case EXPECT_CLOSE_BRACKET:
        if( tokenizer->initial != ']' ) {
          SYNTAX_ERROR( "expected ']'" );
        }
      CLOSE_BRACKET:
        // Unwind stack until open bracket, output all operators found
        while( !__shunt__top_is_open_bracket( shuntstack ) && (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
          __output__emit_operator( program, shunt_op );
        }
        // Discard the open bracket
        if( __shunt__top_is_open_bracket( shuntstack ) ) {
          shunt_op = __shunt__pop_operator( shuntstack ); // discard the '['
          // Top is now the subscript operator, which we emit
          shunt_op = __shunt__pop_operator( shuntstack );
          __output__emit_operator( program, shunt_op );
          --bracket_nesting;
          NEXT_TOKEN( EXPECT_INFIX );
        }
        // Error
        SYNTAX_ERROR( "missing '['" );

      // #####################
      // ###   <literal>   ###
      // #####################
      case EXPECT_LITERAL:
      LITERAL:
        // Integer Literal
        if( tokenizer->tokinfo.typ == STRING_TYPE_DIGIT && tokenizer->initial != '.' ) {
          __output__emit_operand( program, __numeric__decimal_int( tokenizer->token, &stackitem ), &stackitem );
          NEXT_TOKEN( EXPECT_INFIX );
        }
        // Real Literal
        else if( tokenizer->tokinfo.typ & STRING_MASK_NUMBER ) {
          __output__emit_operand( program, __numeric__real( tokenizer, &stackitem ), &stackitem );
          NEXT_TOKEN( EXPECT_INFIX );
        }
        // String
        else if( tokenizer->tokinfo.len == 1 && (tokenizer->initial == '\'' || tokenizer->initial == '"' || tokenizer->initial == 'b') ) {
          iString.Discard( &CSTR__literal );
          if( (CSTR__literal = __parser__get_raw_cstring( graph, tokenizer )) != NULL ) {

            // Operation immediately preceding the literal
            __rpn_operation *pre = (shuntstack->sp-1);
            // Literal is preceded by '[', we are parsing a subscript (presumably vertex property lookup)
            if( shuntstack->sp->type == OP_LEFT_BRACKET ) {
              // The subscript type is immediately before the '[' on the stack
              if( (stackitem.integer = __enum__key( graph, __STACK_ITEM_PROP_STR, CSTR__literal )) != 0 ) {
                stackitem.type = STACK_ITEM_TYPE_INTEGER;
                current_op = &RpnPushConstantInt;
                __output__emit_operand( program, current_op, &stackitem );
                NEXT_TOKEN( EXPECT_CLOSE_BRACKET ); // ]
              }
            }

            // (
            else if( shuntstack->sp->type == OP_LEFT_PAREN ) {
              // Literal is argument to a vertex property lookup operation
              if( __rpndef__is_property_lookup( pre ) ) {
                if( (stackitem.integer = __enum__key( graph, __STACK_ITEM_PROP_STR, CSTR__literal )) != 0 ) {
                  stackitem.type = STACK_ITEM_TYPE_INTEGER;
                  current_op = &RpnPushConstantInt;
                  __output__emit_operand( program, current_op, &stackitem );
                  NEXT_TOKEN( EXPECT_INFIX );
                }
              }
              // Literal is argument to relationship encode
              else if( __eq_operation( pre, &RpnEnumRelEnc ) ) {
                if( (stackitem.integer = __enum__key( graph, __STACK_ITEM_REL_STR, CSTR__literal )) < 0 ) {
                  SYNTAX_ERROR( "invalid relationship" );
                }
                if( stackitem.integer > 0 ) {
                  stackitem.type = STACK_ITEM_TYPE_INTEGER;
                  current_op = &RpnPushConstantInt;
                }
                else {
                  stackitem.type = STACK_ITEM_TYPE_WILD;
                  current_op = &RpnPushWild;
                }
                __output__emit_operand( program, current_op, &stackitem );
                NEXT_TOKEN( EXPECT_CLOSE_PAREN ); // )
              }
              // Literal is argument to vertex type encode
              else if( __eq_operation( pre, &RpnEnumTypeEnc ) ) {
                if( (stackitem.integer = __enum__key( graph, __STACK_ITEM_VTX_STR, CSTR__literal )) < 0 ) {
                  SYNTAX_ERROR( "invalid vertex type" );
                }
                if( stackitem.integer > 0 ) {
                  stackitem.type = STACK_ITEM_TYPE_INTEGER;
                  current_op = &RpnPushConstantInt;
                }
                else {
                  stackitem.type = STACK_ITEM_TYPE_WILD;
                  current_op = &RpnPushWild;
                }
                __output__emit_operand( program, current_op, &stackitem );
                NEXT_TOKEN( EXPECT_CLOSE_PAREN ); // )
              }
            }

            // Literal string needs further analysis
            stackitem.CSTR__str = CSTR__literal;
            stackitem.type = STACK_ITEM_TYPE_CSTRING;
            // Enumerate string if previous operations require it to be enumerated
            __rpn_operation *enum_op = __enum__conditional_args( graph, program, shuntstack, &RpnPushConstantString, &stackitem );
            if( enum_op != NULL ) {
              current_op = enum_op;
              __output__emit_operand( program, current_op, &stackitem );
              NEXT_TOKEN( EXPECT_INFIX );
            }
            // Plain string, save to list of literal strings and put its address on the stack
            else {
              // Make a new node and assign the string to the node
              vgx_ExpressEvalString_t *node = calloc( 1, sizeof( vgx_ExpressEvalString_t ) );
              if( !node ) {
                INTERNAL_ERROR( "out of memory" );
              }

              const char *probe = CStringValue( CSTR__literal );
              int sz_probe = CStringLength( CSTR__literal );
              char *pwild; 
              // Wildcard - replace the literal with a new string without the asterisk(s) and set the appropriate flag in the new string
              if( (pwild = strchr( probe, '*' )) != NULL ) {
                CString_t *CSTR__wild = NULL;
                // Skip ahead to first nonescaped asterisk
                while( pwild > probe && *(pwild-1) == '\\' ) {
                  pwild = strchr( pwild+1, '*' );
                }
                // Actual wildcard(s) exist and pwild points to the first of them
                if( pwild ) {
                  enum { WILD_NONE, WILD_PREFIX, WILD_SUFFIX, WILD_INFIX, WILD_INTERNAL } mode = WILD_NONE;

                  int64_t firstocc = pwild - probe;
                  // Convert to a char array we can operate on
                  char *wild = CStringConvertToChars( CSTR__literal );
                  if( wild == NULL ) {
                    INTERNAL_ERROR( "out of memory" );
                  }
                  probe = NULL;
                  // Redirect the pointer to the char array
                  pwild = wild + firstocc;
                  // Start and end
                  char *first = wild;
                  char *last = wild + sz_probe - 1;
                  // First char is wildcard
                  if( pwild == first ) {
                    ++pwild;
                    // Last char is unescaped * and at least one char in the middle
                    if( sz_probe > 2 && *last == '*' && *(last-1) != '\\' ) {
                      // Infix: *x*
                      // Chop off last char and set the infix flag
                      *last = '\0';
                      if( (CSTR__wild = CStringNewAlloc( pwild, graph->ephemeral_string_allocator_context )) != NULL ) {
                        mode = WILD_INFIX;
                      }
                    }
                    // Last char is not * or less than three chars total
                    else {
                      // Suffix: *x
                      if( (CSTR__wild = CStringNewAlloc( pwild, graph->ephemeral_string_allocator_context )) != NULL ) {
                        mode = WILD_SUFFIX;
                      }
                    }
                  }
                  // Last char is wildcard
                  else if( pwild == last ) {
                    // Prefix: x*
                    // Chop off last char and set the prefix flag
                    pwild = wild;
                    *last = '\0';
                    if( (CSTR__wild = CStringNewAlloc( pwild, graph->ephemeral_string_allocator_context )) != NULL ) {
                      mode = WILD_PREFIX;
                    }
                  }
                  // The actual wildcard must be internal
                  else {
                    // Internal: a*b
                    // Mark internal wild position as ETX ("end of text")
                    *pwild = 3; // ETX
                    pwild = wild;
                    if( (CSTR__wild = CStringNewAlloc( pwild, graph->ephemeral_string_allocator_context )) != NULL ) {
                      mode = WILD_INTERNAL;
                    }
                  }

                  // Verify no wildcards remain
                  const char *pw = pwild;
                  while( (pwild = strchr( pw, '*' )) != NULL ) {
                    if( pwild == wild || *(pwild-1) != '\\' ) {
                      break; // found wildcard (illegal)
                    }
                    pw = pwild + 1;
                  }

                  // Free the wild
                  free( wild );

                  // Illegal wildcard?
                  if( pwild ) {
                    iString.Discard( &CSTR__wild );
                    SYNTAX_ERROR( "illegal wildcard" );
                  }

                  // Replace all escaped wildcards with asterisks
                  if( CSTR__wild ) {
                    CString_t *CSTR__str = CStringReplace( CSTR__wild, "\\*", "*" );
                    iString.Discard( &CSTR__wild );
                    CSTR__wild = CSTR__str;
                  }

                  // Reassign literal from wild
                  if( (CSTR__literal = CSTR__wild) == NULL ) {
                    INTERNAL_ERROR( "out of memory" );
                  }

                  switch( mode ) {
                  case WILD_PREFIX:
                    SET_STRING_IS_PREFIX( CSTR__literal );
                    break;
                  case WILD_SUFFIX:
                    SET_STRING_IS_SUFFIX( CSTR__literal );
                    break;
                  case WILD_INFIX:
                    SET_STRING_IS_INFIX( CSTR__literal );
                    break;
                  case WILD_INTERNAL:
                    SET_STRING_IS_PREFIX( CSTR__literal );
                    SET_STRING_IS_SUFFIX( CSTR__literal );
                    break;
                  default:
                    break;
                  }

                }
              }

              // Add literal to list and emit
              if( CSTR__literal ) {
                // Plain string
                // Pre-compute the string's obid
                CStringObid( CSTR__literal );
                // Mark string as literal and put address on stack
                SET_STRING_IS_LITERAL( CSTR__literal );
                stackitem.CSTR__str = node->CSTR__literal = CSTR__literal;
                // Set the first node in list
                if( program->strings == NULL ) {
                  program->strings = node;
                }
                // Add node to list
                else {
                  // Find end
                  vgx_ExpressEvalString_t *cursor = program->strings;
                  while( cursor->next ) {
                    cursor = cursor->next;
                  }
                  // Attach new node to end
                  cursor->next = node;
                }
                // String stolen by list
                CSTR__literal = NULL;
                // Set operation
                current_op = &RpnPushConstantString;
                __output__emit_operand( program, current_op, &stackitem );
                NEXT_TOKEN( EXPECT_INFIX );
              }

            }
          }
        }
        // Try as other base integer literal
        else {
          __output__emit_operand( program, __numeric__binhex_int( tokenizer->token, &stackitem ), &stackitem );
          NEXT_TOKEN( EXPECT_INFIX );
        }
        
        SYNTAX_ERROR( "literal expected" );

      default:
        break;
      }
    }

    // Expected end state is infix, error if other state
    if( STATE == EXPECT_INFIX || STATE == EXPECT_OPERAND_OR_END || ( STATE == EXPECT_IGNORE_LINE && PREV_STATE == EXPECT_INFIX ) ) {
      /* ok */
    }
    else {
      // Capture last non-null token 
      if( tokenizer->token == NULL ) {
        tokenizer->token = last_token;
      }

      // Verify complete expression
      switch( STATE ) {
      case EXPECT_OPERAND:
        __synerr__syntax_error( tokenizer, "expected operand" );
        break;
      case EXPECT_LITERAL:
        __synerr__syntax_error( tokenizer, "expected literal" );
        break;
      case EXPECT_COLON:
        __synerr__syntax_error( tokenizer, "expected ':'" );
        break;
      case EXPECT_COMMA:
        __synerr__syntax_error(  tokenizer, "expected ','" );
        break;
      case EXPECT_OPEN_PAREN:
        __synerr__syntax_error( tokenizer, "expected '('" );
        break;
      case EXPECT_CLOSE_PAREN:
        __synerr__syntax_error( tokenizer, "expected ')'" );
        break;
      case EXPECT_OPEN_BRACKET:
        __synerr__syntax_error( tokenizer, "expected '['" );
        break;
      case EXPECT_CLOSE_BRACKET:
        __synerr__syntax_error( tokenizer, "expected ']'" );
        break;
      case EXPECT_OPEN_BRACE:
        __synerr__syntax_error( tokenizer, "expected '{'" );
        break;
      case EXPECT_CLOSE_BRACE:
        __synerr__syntax_error( tokenizer, "expected '}'" );
        break;
      case EXPECT_ASSIGNMENT:
        __synerr__syntax_error( tokenizer, "undefined variable" );
        break;
      case EXPECT_IGNORE:
        __synerr__syntax_error( tokenizer, "unterminated comment" );
        break;
      default:
        __synerr__syntax_error( tokenizer, "invalid expression" );
        break;
      }
    }

    // Verify complete argument list
    if( end_arg_idx > 0 ) {
      if( end_arg_idx - current_arg_idx > 1 ) {
        __synerr__syntax_error( tokenizer, "expected ','" );
      }
      else {
        __synerr__syntax_error( tokenizer, "expected ')'" );
      }
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x145 );
    }

    // Verify balanced groups
    if( group_nesting != 0 ) {
      __synerr__syntax_error( tokenizer, "expected ')'" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x146 );
    }

    // Verify balanced subscript
    if( bracket_nesting != 0 ) {
      __synerr__syntax_error( tokenizer, "expected ']'" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x147 );
    }

    // Verify balanced sets
    if( brace_nesting != 0 ) {
      __synerr__syntax_error( tokenizer, "expected '}'" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x148 );
    }

    // Flush rest of shunt stack to output
    if( tokenizer->nerr == 0 ) {
      while( (shunt_op = __shunt__pop_operator( shuntstack )) != NULL ) {
        __output__emit_operator( program, shunt_op );
      }
    }
    else {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x149 );
    }

    // Verify correctness of RPN output (NOTE: there may be multiple items on the stack)
    if( program->stack.eval_depth.run < 1 ) {
      __synerr__syntax_error( tokenizer, "invalid expression" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x14A );
    }

    // Set the tokenized expression
    if( (program->CSTR__expression = NewEphemeralCString( graph, infix_expression )) == NULL ) {
      if( strlen( infix_expression ) > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
        __synerr__syntax_error( tokenizer, "expression too long" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x14C );
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x14D );
      }
    }

    // Normalize all stack item arg types to within valid runtime range (mask out any parser-only info)
    vgx_ExpressEvalOperation_t *program_cur = program->operations;
    vgx_ExpressEvalOperation_t *program_end = program_cur + program->length + program->n_passthru;
    // Normalize
    while( program_cur < program_end ) {
      program_cur++->arg.type &= __STACK_ITEM_TYPE_MASK;
    }


    // New rpn created
    ret = 1;
    
  }
  XCATCH( errcode ) {
    if( program->operations ) {
      ALIGNED_FREE( program->operations );
      program->operations = NULL;
    }
    ret = -1;
  }
  XFINALLY {
    __tokenizer__delete_context( &tokenizer );
    __shunt__delete( &shuntstack );
    iString.Discard( &CSTR__literal );

    // Variable assignment var last subexpression ignored
    __rpn_variable *var = __subexpression_pop_variable( &subexpression );
    __varmap__variable_delete( &var );

    // Delete variable map
    __varmap__delete( &varmap );
  }

  return ret;
}
