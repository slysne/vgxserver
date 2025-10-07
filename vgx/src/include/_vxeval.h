/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxeval.h
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

#ifndef _VGX_VXEVAL_H
#define _VGX_VXEVAL_H

#include "_vgx.h"



/******************************************************************************
 *
 ******************************************************************************
 */
typedef enum __e_rpn_op_type {
  OP_NULL                   = 0x00000,
  OP_PASSTHRU               = 0x00010,

  //
  // mmm  : extra meta info
  // d    : dereference of object that must be locked
  // CC   : main category
  // t    : type within category
  // a    : number of operator arguments
  //                            mmmdCCta            
  //------------------------  ----------
  //                               |
  // Masks                         |CCta
  __OP_META_MASK            = 0x0FF00000,
  __OP_REF_MASK             = 0x000F0000,
  __OP_CLS_MASK             = 0x0000FF00,
  __OP_TYP_MASK             = 0x000000F0,
  __OP_ARG_MASK             = 0x0000000F,
  __OP_VARARG_MASK          = 0xF0000000,
  //                               |
  // Args                          |CCta
  __OP_ARGS_0               = 0x00000000,
  __OP_ARGS_1               = 0x00000001,
  __OP_ARGS_2               = 0x00000002,
  __OP_ARGS_3               = 0x00000003,
  __OP_ARGS_4               = 0x00000004,
  __OP_ARGS_5               = 0x00000005,
  __OP_ARGS_6               = 0x00000006,
  __OP_ARGS_7               = 0x00000007,
  __OP_ARGS_8               = 0x00000008,
  __OP_ARGS_9               = 0x00000009,
  __OP_ARGS_10              = 0x0000000A,
  __OP_ARGS_11              = 0x0000000B,
  __OP_ARGS_12              = 0x0000000C,
  __OP_ARGS_13              = 0x0000000D,
  __OP_ARGS_14              = 0x0000000E,
  __OP_ARGS_VAR             = 0x0000000F,
  //                               |
  // Match                         |CCta
  __OP_CLS_INFIX            = 0x00000100,
  __OP_CLS_PREFIX           = 0x00000200,
  __OP_CLS_GROUP            = 0x00000400,
  __OP_CLS_OBJECT           = 0x00000800,
  __OP_CLS_SUBSCRIPT        = 0x00001000,
  __OP_CLS_ASSIGN           = 0x00002000,
  __OP_CLS_SYMBOLIC         = 0x00004000,
  __OP_CLS_LITERAL          = 0x00008000,
  __OP_CLS_TRAVERSE         = 0x00010000,
  __OP_CLS_HEAD_DEREF       = 0x00020000,
  __OP_CLS_TAIL_DEREF       = 0x00040000,
  __OP_CLS_THIS_DEREF       = 0x00080000,
  // Extra metas                   |CCta
  __OP_META_VERTEX_TYPE     = 0x01000000,
  __OP_META_ARC_TYPE        = 0x02100000,
  __OP_META_ARC_MOD         = 0x02200000,
  __OP_META_ARC_DIR         = 0x02400000,
  __OP_META_ARC             = 0x02800000,
  __OP_META_SYNARC          = 0x04000000,
  __OP_META_WORKSLOT        = 0x08000000,
  //                               |
  // Combination masks             |
  __OP_CLS_OPERAND_MASK     = __OP_CLS_SYMBOLIC | __OP_CLS_LITERAL,
  //                               |
  // Infix Operators               |CCta
  OP_UNARY_INFIX            = 0x00000010 | __OP_ARGS_1 | __OP_CLS_INFIX,
  OP_BINARY_INFIX           = 0x00000020 | __OP_ARGS_2 | __OP_CLS_INFIX,
  OP_BINARY_CMP_INFIX       = 0x00000030 | __OP_ARGS_2 | __OP_CLS_INFIX,
  OP_TERNARY_COND_INFIX     = 0x00000040 | __OP_ARGS_3 | __OP_CLS_INFIX,
  OP_TERNARY_COLON_INFIX    = 0x00000050 | __OP_ARGS_2 | __OP_CLS_INFIX,
  OP_BITWISE_INFIX          = 0x00000060 | __OP_ARGS_2 | __OP_CLS_INFIX,
  //                               |
  // Prefix Operators              |CCta
  OP_NULLARY_PREFIX         = 0x00000010 | __OP_ARGS_0 | __OP_CLS_PREFIX, 
  OP_UNARY_PREFIX           = 0x00000020 | __OP_ARGS_1 | __OP_CLS_PREFIX, 
  OP_BINARY_PREFIX          = 0x00000030 | __OP_ARGS_2 | __OP_CLS_PREFIX,
  OP_TERNARY_PREFIX         = 0x00000040 | __OP_ARGS_3 | __OP_CLS_PREFIX,
  OP_QUATERNARY_PREFIX      = 0x00000050 | __OP_ARGS_4 | __OP_CLS_PREFIX,
  OP_QUINARY_PREFIX         = 0x00000050 | __OP_ARGS_5 | __OP_CLS_PREFIX,
  OP_VARIADIC_PREFIX        = 0x00000060 | __OP_ARGS_VAR | __OP_CLS_PREFIX, 
  OP_VARIADIC_TAIL_PREFIX   = 0x00000070 | __OP_ARGS_VAR | __OP_CLS_PREFIX | __OP_CLS_TAIL_DEREF,  // lookback
  OP_VARIADIC_ARC_PREFIX    = 0x00000080 | __OP_ARGS_VAR | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,  // traverse
  OP_VARIADIC_HEAD_PREFIX   = 0x00000090 | __OP_ARGS_VAR | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE | __OP_CLS_HEAD_DEREF,  // traverse and deref
  OP_BITWISE_PREFIX         = 0x000000A0 | __OP_ARGS_2 | __OP_CLS_PREFIX,
  OP_STRING_PREFIX          = 0x000000B0 | __OP_ARGS_1 | __OP_CLS_PREFIX,
  OP_OBJECT_PREFIX          = 0x000000C0 | __OP_ARGS_1 | __OP_CLS_PREFIX,
  OP_NULLARY_COLLECT        = 0x000000D0 | __OP_ARGS_0 | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_UNARY_COLLECT          = 0x000000D0 | __OP_ARGS_1 | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_BINARY_COLLECT         = 0x000000D0 | __OP_ARGS_2 | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_TERNARY_COLLECT        = 0x000000D0 | __OP_ARGS_3 | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_QUATERNARY_COLLECT     = 0x000000D0 | __OP_ARGS_4 | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_VARIADIC_COLLECT       = 0x000000D0 | __OP_ARGS_VAR | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  OP_VARIADIC_CULL          = 0x000000E0 | __OP_ARGS_VAR | __OP_CLS_PREFIX | __OP_CLS_TRAVERSE,
  //                               |
  // Group                         |CCta
  OP_LEFT_PAREN             = 0x00000010 | __OP_ARGS_VAR | __OP_CLS_GROUP,
  OP_LEFT_BRACKET           = 0x00000020 | __OP_ARGS_1 | __OP_CLS_GROUP,
  OP_LEFT_BRACE             = 0x00000030 | __OP_ARGS_1 | __OP_CLS_GROUP,
  OP_COMMA                  = 0x00000050 | __OP_ARGS_1 | __OP_CLS_GROUP,
  OP_SEMICOLON              = 0x00000060 | __OP_ARGS_1 | __OP_CLS_GROUP,
  OP_RIGHT_PAREN            = 0x00000070 | __OP_ARGS_VAR | __OP_CLS_GROUP,
  OP_RIGHT_BRACKET          = 0x00000080 | __OP_ARGS_1 | __OP_CLS_GROUP,
  OP_RIGHT_BRACE            = 0x00000090 | __OP_ARGS_1 | __OP_CLS_GROUP,
  //                               |
  // Object                        |CCta
  OP_TAIL_VERTEX_OBJECT     = 0x00000010 | __OP_CLS_OBJECT | __OP_CLS_TAIL_DEREF,  // lookback
  OP_THIS_VERTEX_OBJECT     = 0x00000020 | __OP_CLS_OBJECT | __OP_CLS_THIS_DEREF,
  OP_HEAD_VERTEX_OBJECT     = 0x00000030 | __OP_CLS_OBJECT | __OP_CLS_TRAVERSE,  // traverse and MAY NEED DEREF - detect depending on context
  OP_TAIL_VECTOR_OBJECT     = 0x00000040 | __OP_CLS_OBJECT | __OP_CLS_TAIL_DEREF,  // lookback
  OP_THIS_VECTOR_OBJECT     = 0x00000050 | __OP_CLS_OBJECT | __OP_CLS_THIS_DEREF,
  OP_HEAD_VECTOR_OBJECT     = 0x00000060 | __OP_CLS_OBJECT | __OP_CLS_TRAVERSE | __OP_CLS_HEAD_DEREF,  // traverse and deref
  OP_TAIL_VERTEXID_OBJECT   = 0x00000070 | __OP_CLS_OBJECT | __OP_CLS_TAIL_DEREF,  // lookback
  OP_THIS_VERTEXID_OBJECT   = 0x00000080 | __OP_CLS_OBJECT | __OP_CLS_THIS_DEREF,
  OP_HEAD_VERTEXID_OBJECT   = 0x00000090 | __OP_CLS_OBJECT | __OP_CLS_TRAVERSE | __OP_CLS_HEAD_DEREF,  // traverse and deref
  //                               |
  // Subscript                     |CCta
  OP_SUBSCRIPT              = 0x00000040 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  OP_HEAD_SUBSCRIPT         = 0x00000050 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  OP_RELATIONSHIP           = 0x00000060 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  OP_VERTEXTYPE             = 0x00000070 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  OP_VECTORDIM              = 0x00000080 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  OP_MEMORY                 = 0x00000090 | __OP_ARGS_1 | __OP_CLS_SUBSCRIPT,
  //                               |
  // Assignment                    |CCta
  OP_ASSIGN                 = 0x00000010 | __OP_ARGS_1 | __OP_CLS_ASSIGN,
  // Variable                      |CCta
  OP_VARIABLE               = 0x00000020 | __OP_ARGS_1 | __OP_CLS_ASSIGN,
  //                               |
  // Symbolic Operand              |CCta
  OP_TAIL_ATTR_OPERAND      = 0x00000010 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TAIL_DEREF,  // lookback
  OP_TAIL_TYPE_OPERAND      = 0x00000020 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TAIL_DEREF,  // lookback
  OP_TAIL_REL_OPERAND       = 0x00000030 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TAIL_DEREF,  // lookback
  OP_THIS_ATTR_OPERAND      = 0x00000040 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_THIS_DEREF,  //
  OP_THIS_TYPE_OPERAND      = 0x00000050 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_THIS_DEREF,  //
  OP_ARC_ATTR_OPERAND       = 0x00000060 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TRAVERSE,    // traverse
  OP_ARC_OPERAND            = 0x00000060 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TRAVERSE | __OP_META_ARC,    // traverse
  OP_ARC_MOD_OPERAND        = 0x00000060 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TRAVERSE | __OP_META_ARC_MOD,  // traverse
  OP_ARC_DIR_OPERAND        = 0x00000070 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_META_ARC_DIR | __OP_CLS_TRAVERSE,  // traverse
  OP_ARC_REL_OPERAND        = 0x00000080 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_META_ARC_TYPE | __OP_CLS_TRAVERSE,  // traverse
  OP_HEAD_ATTR_OPERAND      = 0x00000090 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TRAVERSE | __OP_CLS_HEAD_DEREF,  // traverse and deref
  OP_HEAD_TYPE_OPERAND      = 0x000000A0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC | __OP_CLS_TRAVERSE | __OP_CLS_HEAD_DEREF,  // traverse and deref
  OP_REGISTER_OPERAND       = 0x000000B0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC,  //
  OP_INTEGER_OPERAND        = 0x000000C0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC,  //
  OP_REAL_OPERAND           = 0x000000D0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC,  //
  OP_ARC_DIR_ENUM_OPERAND   = 0x000000E0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC,  //
  OP_ARC_MOD_ENUM_OPERAND   = 0x000000F0 | __OP_ARGS_1 | __OP_CLS_SYMBOLIC,  //
  //                               |
  // Numeric Literals              |CCta
  OP_NONE_LITERAL           = 0x00000000 | __OP_ARGS_1 | __OP_CLS_LITERAL,
  OP_INTEGER_LITERAL        = 0x00000010 | __OP_ARGS_1 | __OP_CLS_LITERAL,
  OP_REAL_LITERAL           = 0x00000020 | __OP_ARGS_1 | __OP_CLS_LITERAL,
  OP_ADDRESS_LITERAL        = 0x00000040 | __OP_ARGS_1 | __OP_CLS_LITERAL
  //                            |      |
  //                            |      | 
  //                            |      |
  // NOTE ON VARIADIC:          |      F
  // Function call is variadic  
  // when last hex digit is F.
  // If leftmost meta digit is 0
  // the function accepts any
  // number of arguments.
  // If leftmost meta digit is
  // nonzero the function takes
  // between min and max
  // number of args according
  // to the following encoding:
  // 
  // 0x0......F   0000   0 - ANY
  // 0x1......F   0001   0 - 1
  // 0x2......F   0010   0 - 2
  // 0x3......F   0011   0 - 3
  // 0x4......F   0100   0 - 4
  // 0x5......F   0101   0 - 5
  // 0x6......F   0110   1 - 2
  // 0x7......F   0111   1 - 3
  // 0x8......F   1000   1 - 4
  // 0x9......F   1001   1 - 5
  // 0xA......F   1010   2 - 3
  // 0xB......F   1011   2 - 4
  // 0xC......F   1100   2 - 5
  // 0xD......F   1101   3 - 4
  // 0xE......F   1110   3 - 5
  // 0xF......F   1111   4 - 5


} __rpn_op_type;




/******************************************************************************
 *
 ******************************************************************************
 */
typedef enum __e_operator_precedence {

  // Associativity LEFT  = 0010 = 2 \
  // Associativity RIGHT = 0001 = 1 -|
  // Associativity NONE  = 0000 = 0 /|
  //                                 |
  // Precedence_____________________ |
  //                          ||||||||
  //                          pppppppA
  __ASSOC_LEFT            = 0x00000002,
  __ASSOC_RIGHT           = 0x00000001,
  __ASSOC_NONE            = 0x00000000,

  __OPP_0                 = 0x01000000,
  OPP_NONE                = __OPP_0,

  __OPP_1                 = 0x00800000,
  OPP_CONSTANT            = __OPP_1,
  OPP_GROUP               = __OPP_1,
  
  __OPP_2                 = 0x00400000,
  OPP_CALL                = __OPP_2 | __ASSOC_LEFT,
  OPP_SUBSCRIPT           = __OPP_2 | __ASSOC_LEFT,
  OPP_ATTRIBUTE           = __OPP_2 | __ASSOC_LEFT,

  __OPP_3                 = 0x00200000,
  OPP_EXPONENTIATION      = __OPP_3 | __ASSOC_RIGHT,
  OPP_UNARY_PLUS          = __OPP_3 | __ASSOC_RIGHT,
  OPP_UNARY_MINUS         = __OPP_3 | __ASSOC_RIGHT,
  OPP_LOGICAL_NOT         = __OPP_3 | __ASSOC_RIGHT,
  OPP_BITWISE_NOT         = __OPP_3 | __ASSOC_RIGHT,

  __OPP_4                 = 0x00100000,
  OPP_SET_MEMBER          = __OPP_4 | __ASSOC_LEFT,

  __OPP_5                 = 0x00080000,
  OPP_MULTIPLICATION      = __OPP_5 | __ASSOC_LEFT,
  OPP_DIVISION            = __OPP_5 | __ASSOC_LEFT,
  OPP_MODULO              = __OPP_5 | __ASSOC_LEFT,

  __OPP_6                 = 0x00040000,
  OPP_ADDITION            = __OPP_6 | __ASSOC_LEFT,
  OPP_SUBTRACTION         = __OPP_6 | __ASSOC_LEFT,

  __OPP_7                 = 0x00020000,
  OPP_BITSHIFT            = __OPP_7 | __ASSOC_LEFT,

  __OPP_8                 = 0x00010000,
  OPP_COMPARISON          = __OPP_8 | __ASSOC_LEFT,

  __OPP_9                 = 0x00008000,
  OPP_EQUALITY            = __OPP_9 | __ASSOC_LEFT,

  __OPP_10                = 0x00004000,
  OPP_BITWISE_AND         = __OPP_10 | __ASSOC_LEFT,

  __OPP_11                = 0x00002000,
  OPP_BITWISE_XOR         = __OPP_11 | __ASSOC_LEFT,

  __OPP_12                = 0x00001000,
  OPP_BITWISE_OR          = __OPP_12 | __ASSOC_LEFT,

  __OPP_13                = 0x00000800,
  OPP_LOGICAL_AND         = __OPP_13 | __ASSOC_LEFT,

  __OPP_14                = 0x00000400,
  OPP_LOGICAL_OR          = __OPP_14 | __ASSOC_LEFT,

  __OPP_15                = 0x00000200,
  OPP_TERNARY_CONDITIONAL = __OPP_15 | __ASSOC_RIGHT,
  OPP_TERNARY_COLON       = __OPP_15 | __ASSOC_RIGHT,

  __OPP_16                = 0x00000100,
  OPP_ASSIGNMENT          = __OPP_16 | __ASSOC_RIGHT,

  __OPP_17                = 0x00000080,

  __OPP_18                = 0x00000040,
  OPP_COMMA               = __OPP_18 | __ASSOC_LEFT,
  OPP_SEMICOLON           = __OPP_18 | __ASSOC_LEFT,


  __OPP_MASK              = 0x7FFFFFF0,
  __ASSOC_MASK            = 0x0000000F

} __operator_precedence;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum __e_parser_state {
  EXPECT_ANY                = 0x00,
  EXPECT_INFIX              = 0x01,
  EXPECT_OPERAND            = 0x02,
  EXPECT_OPERAND_OR_END     = 0x03,
  EXPECT_LITERAL            = 0x04,
  EXPECT_COLON              = 0x05,
  EXPECT_COMMA              = 0x06,
  EXPECT_OPEN_PAREN         = 0x07,
  EXPECT_CLOSE_PAREN        = 0x08,
  EXPECT_SEMICOLON          = 0x09,
  EXPECT_OPEN_BRACKET       = 0x0A,
  EXPECT_CLOSE_BRACKET      = 0x0B,
  EXPECT_ASSIGNMENT         = 0x0C,
  EXPECT_VARIABLE           = 0x0D,
  EXPECT_OPEN_BRACE         = 0x0E,
  EXPECT_CLOSE_BRACE        = 0x0F,
  EXPECT_IGNORE_LINE        = 0x10,
  EXPECT_IGNORE             = 0x11,
} parser_state;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static const char *state_name[] = {
  "",               // EXPECT_ANY             0x0
  "<infix>",        // EXPECT_INFIX           0x1
  "<operand>",      // EXPECT_OPERAND         0x2
  "<operand/end>",  // EXPECT_OPERAND_OR_END  0x3
  "<literal>",      // EXPECT_LITERAL         0x4
  ":",              // EXPECT_COLON           0x5
  ",",              // EXPECT_COMMA           0x6
  "(",              // EXPECT_OPEN_PAREN      0x7
  ")",              // EXPECT_CLOSE_PAREN     0x8
  ";",              // EXPECT_SEMICOLON       0x9
  "[",              // EXPECT_OPEN_BRACKET    0xA
  "]",              // EXPECT_CLOSE_BRACKET   0xB
  ":=",             // EXPECT_ASSIGNMENT      0xC
  "=",              // EXPECT_VARIABLE        0xD
  "{",              // EXPECT_OPEN_BRACE      0xE
  "}",              // EXPECT_CLOSE_BRACE     0xF
  "<ignore_line>",  // EXPECT_IGNORE_LINE     0x10
  "<ignore>",       // EXPECT_IGNORE          0x11
  NULL
};



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static const char *__get_parser_state_name( parser_state state ) {
  return state_name[ state & 0xF ];
}



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct s_express_eval_map_t {
  framehash_cell_t *map;
  framehash_dynamic_t dyn;
  // Returns address of __rpn_operation into opaddr
  int (*get)( framehash_cell_t *map, framehash_dynamic_t *dyn, const char *key, int64_t *opaddr );
} express_eval_map;


/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_rpn_operator {
  union {
    QWORD bits;
    const char *token;
    int64_t integer;
    double real;
  } surface;
  union {
    QWORD bits;
    f_evaluator eval;
    struct {
      int current_arg_idx;
      int end_arg_idx;
    } meta;
  } function;
  __rpn_op_type type;
  __operator_precedence precedence;
} __rpn_operation;
 


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_left_associative( const __rpn_operation *op ) {
  return op->precedence & __ASSOC_LEFT;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_right_associative( const __rpn_operation *op ) {
  return op->precedence & __ASSOC_RIGHT;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __get_precedence( const __rpn_operation *op ) {
  return (int)(op->precedence & __OPP_MASK);
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */   
typedef struct __s_tokenizer_context {
  // Engine
  CTokenizer_t *engine;
  // Expression
  const char *expression;
  int lenexpr;
  int ntokens;
  // Token
  bool hasnext;
  const char *token;
  char initial;
  tokenmap_t *tokmap;
  tokinfo_t tokinfo;
  union {
    QWORD data;
    char symbol[ sizeof( QWORD ) ];
  } op;
  char *psymbol;
  uint16_t oplen;
  // Escape region
  bool escape;
  // Debug
  bool debug;
  // Errors
  int nerr;
  CString_t **CSTR__error;
} __tokenizer_context;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_shunt_stack {
  __rpn_operation *slots;
  __rpn_operation *sp;
  int depth;
  int cnt;
  bool debug;
} __shunt_stack;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_rpn_variable {
  char *name;
  int subexpr_idx;
} __rpn_variable;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_varmap_t {
  CQwordList_t *list;
} __varmap;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_subexpression {
  int index;              // program stack index (0 is always no item, data starts at 1)
  __rpn_variable *var;    // current variable being assigned
  struct {
    int program_length;   // program length as it was just before current subexpression
    int stack_cnt;        // stack operation counter as it was just before current subexpression
  } at;
  bool capture_at;        // true when program/stack state should be captured (begin new subexpression)
} __subexpression;



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static void __capture_next_subexpression( __subexpression *subexpression, const vgx_ExpressEvalProgram_t *program, const __shunt_stack *stack ) {
  if( program->length > subexpression->at.program_length ) {
    subexpression->index++;
    subexpression->at.program_length = program->length;
    subexpression->at.stack_cnt = stack->cnt;
  }
  subexpression->capture_at = false;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static void __end_subexpression( __subexpression *subexpression ) {
  subexpression->capture_at = true;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_at_subexpression( const __subexpression *subexpression, const vgx_ExpressEvalProgram_t *program, const __shunt_stack *stack ) {
  return program->length == subexpression->at.program_length && stack->cnt == subexpression->at.stack_cnt;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static __rpn_variable * __subexpression_pop_variable( __subexpression *subexpression ) {
  __rpn_variable *var = subexpression->var;
  subexpression->var = NULL;
  return var;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_infix_operator( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_INFIX;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_infix_cmp_operator( const __rpn_operation *op ) {
  return op->type == OP_BINARY_CMP_INFIX;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_prefix_operator( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_PREFIX;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_group_operator( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_GROUP;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_comma_operator( const __rpn_operation *op ) {
  return (op->type == OP_COMMA);
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_object_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_OBJECT;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_vertex_object_operand( const __rpn_operation *op ) {
  __rpn_op_type t = op->type;
  return __is_object_operand( op ) && ( t == OP_TAIL_VERTEX_OBJECT || t == OP_THIS_VERTEX_OBJECT || t == OP_HEAD_VERTEX_OBJECT );
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_vector_object_operand( const __rpn_operation *op ) {
  __rpn_op_type t = op->type;
  return __is_object_operand( op ) && ( t == OP_TAIL_VECTOR_OBJECT || t == OP_THIS_VECTOR_OBJECT || t == OP_HEAD_VECTOR_OBJECT );
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_vertexid_object_operand( const __rpn_operation *op ) {
  __rpn_op_type t = op->type;
  return __is_object_operand( op ) && ( t == OP_TAIL_VERTEXID_OBJECT || t == OP_THIS_VERTEXID_OBJECT || t == OP_HEAD_VERTEXID_OBJECT );
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_vertex_type_operand( const __rpn_operation *op ) {
  __rpn_op_type t = op->type;
  return t == OP_TAIL_TYPE_OPERAND || t == OP_THIS_TYPE_OPERAND || t == OP_HEAD_TYPE_OPERAND;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_subscript_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_SUBSCRIPT;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_symbolic_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_SYMBOLIC;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_literal_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_MASK) == __OP_CLS_LITERAL;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_OPERAND_MASK) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_this_deref_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_THIS_DEREF) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_head_deref_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_HEAD_DEREF) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_traverse_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_TRAVERSE) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_tail_deref_operand( const __rpn_operation *op ) {
  return (op->type & __OP_CLS_TAIL_DEREF) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_synarc_operand( const __rpn_operation *op ) {
  return (op->type & __OP_META_SYNARC) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __is_workslot_operator( const __rpn_operation *op ) {
  return (op->type & __OP_META_WORKSLOT) != 0;
}


/******************************************************************************
 *
 ******************************************************************************
 */
static int __operator_arity( const __rpn_operation *op ) {
  return (int)(op->type & __OP_ARG_MASK);
}


/******************************************************************************
 *
 ******************************************************************************
 */
static int __is_variadic_prefix( const __rpn_operation *op ) {
  static const int mask = __OP_CLS_PREFIX | __OP_ARG_MASK;
  return (int)(op->type & mask) == mask;
}






#define __PATCH_OPTYPE( OpType, Max, Offset )   ((OpType) | (((Max)+(Offset)) << 28))


#define ENCODE_VARIADIC_ARG_COUNTS( OpType, Min, Max ) (  \
  (Min) == 0                                              \
  ? __PATCH_OPTYPE( OpType, Max, 0 )                      \
  : ( (Min) == 1                                          \
      ? __PATCH_OPTYPE( OpType, Max, 4 )                  \
      : ( (Min) == 2                                      \
          ? __PATCH_OPTYPE( OpType, Max, 7 )              \
          : ( (Min) == 3                                  \
              ? __PATCH_OPTYPE( OpType, Max, 9 )          \
              : ( (Min) == 4                              \
                  ? __PATCH_OPTYPE( OpType, Max, 10 )     \
                  : 0                                     \
                )                                         \
            )                                             \
        )                                                 \
    )                                                     \
)



/******************************************************************************
 *
 ******************************************************************************
 */
static int __variadic_arg_counts( const __rpn_operation *op, int *min, int *max ) {
  if( !__is_variadic_prefix( op ) ) {
    *min = 0;
    *max = -1;
    return -1;
  }
  else {
    uint32_t code = (uint32_t)(op->type & __OP_VARARG_MASK) >> 28;
    switch( code ) {
    // ANY
    case 0b0000:
      *min = 0;
      *max = INT_MAX;
      break;
    // 0 or more
    case 0b0001:
      *min = 0;
      *max = 1; 
      break;
    case 0b0010:
      *min = 0;
      *max = 2;
      break;
    case 0b0011:
      *min = 0;
      *max = 3;
      break;
    case 0b0100:
      *min = 0;
      *max = 4;
      break;
    case 0b0101:
      *min = 0;
      *max = 5;
      break;
    // 1 or more
    case 0b0110:
      *min = 1;
      *max = 2;
      break;
    case 0b0111:
      *min = 1;
      *max = 3;
      break;
    case 0b1000:
      *min = 1;
      *max = 4;
      break;
    case 0b1001:
      *min = 1;
      *max = 5;
      break;
    // 2 or more
    case 0b1010:
      *min = 2;
      *max = 3;
      break;
    case 0b1011:
      *min = 2;
      *max = 4;
      break;
    case 0b1100:
      *min = 2;
      *max = 5;
      break;
    // 3 or more
    case 0b1101:
      *min = 3;
      *max = 4;
      break;
    case 0b1110:
      *min = 3;
      *max = 5;
      break;
    // 4 or more
    case 0b1111:
      *min = 4;
      *max = 5;
      break;
    }
    return 0;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static int __eq_operation( __rpn_operation *A, __rpn_operation *B ) {
  return A && B && ( A == B || ( A->surface.bits == B->surface.bits && A->function.bits == B->function.bits ) );
}


/******************************************************************************
 *
 ******************************************************************************
 */
static int __has_operator( express_eval_map *operators, const char *name ) {
  int64_t o;
  return operators->get( operators->map, &operators->dyn, name, &o );
}


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_noop = {
  .surface.token  = NULL,
  .function.eval  = NULL,
  .type           = OP_NULL,
  .precedence     = OPP_NONE
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_open_paren = {
  .surface.token  = "(",
  .function.eval  = NULL,
  .type           = OP_LEFT_PAREN,
  .precedence     = OPP_GROUP
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_close_paren = {
  .surface.token  = ")",
  .function.eval  = NULL,
  .type           = OP_RIGHT_PAREN,
  .precedence     = OPP_GROUP
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_open_bracket = {
  .surface.token  = "[",
  .function.eval  = NULL,
  .type           = OP_LEFT_BRACKET,
  .precedence     = OPP_SUBSCRIPT
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_close_bracket = {
  .surface.token  = "]",
  .function.eval  = NULL,
  .type           = OP_RIGHT_BRACKET,
  .precedence     = OPP_SUBSCRIPT
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_open_brace = {
  .surface.token  = "{",
  .function.eval  = NULL,
  .type           = OP_LEFT_BRACE,
  .precedence     = OPP_GROUP
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_close_brace = {
  .surface.token  = "}",
  .function.eval  = NULL,
  .type           = OP_RIGHT_BRACE,
  .precedence     = OPP_GROUP
};


/******************************************************************************
 *
 ******************************************************************************
 */
static const __rpn_operation __rpn_operation_comma = {
  .surface.token  = ",",
  .function.eval  = NULL,
  .type           = OP_COMMA,
  .precedence     = OPP_COMMA
};


DLL_HIDDEN extern CTokenizer_t *_vxeval_parser__tokenizer;
DLL_HIDDEN extern CTokenizer_t *_vxeval_parser__normalizer;
DLL_HIDDEN extern express_eval_map *_vxeval_operations;

DLL_HIDDEN extern const CString_t *_vxeval_modifier_strings[16];
DLL_HIDDEN extern const CString_t *_vxeval_arcdir_strings[4];


DLL_HIDDEN extern int _vxeval_parser__initialize( void );
DLL_HIDDEN extern int _vxeval_parser__destroy( void );
DLL_HIDDEN extern int _vxeval_parser__init_tokenizer( void );
DLL_HIDDEN extern int _vxeval_parser__del_tokenizer( void );
DLL_HIDDEN extern int _vxeval_parser__init_normalizer( void );
DLL_HIDDEN extern int _vxeval_parser__del_normalizer( void );

DLL_HIDDEN extern int _vxeval_parser__create_rpn_from_infix( vgx_ExpressEvalProgram_t *program, vgx_Graph_t *graph, const char *infix_expression, CString_t **CSTR__error  );
DLL_HIDDEN extern int _vxeval_parser__get_evaluator( vgx_Graph_t *graph, const char *infix_expression, vgx_Vector_t *vector, vgx_Evaluator_t **evaluator );


#endif
