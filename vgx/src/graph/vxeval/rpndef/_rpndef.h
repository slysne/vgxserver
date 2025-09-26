/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _rpndef.h
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

#ifndef _VGX_VXEVAL_RPNDEF_RPNDEF_H
#define _VGX_VXEVAL_RPNDEF_RPNDEF_H


#include "_rpnvertex.h"
#include "_rpnarc.h"
#include "_rpnconstant.h"


static express_eval_map * __rpndef__opmap_new( const char *name );
static void               __rpndef__opmap_delete( express_eval_map **operators );
static __rpn_operation *  __rpndef__opmap_get( express_eval_map *operations, const char *name );
static int                __rpndef__is_property_lookup( const __rpn_operation *op );


/******************************************************************************
 *
 ******************************************************************************
 */
static __rpn_operation RpnNoop               = { .surface.token=NULL,               .function.eval = NULL,                            .type = OP_NULL,                  .precedence = OPP_NONE  };
static __rpn_operation RpnNoopPrefix         = { .surface.token=NULL,               .function.eval = NULL,                            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_NONE  };
static __rpn_operation RpnDebugTokenizer     = { .surface.token="DEBUG.TOK",        .function.eval = NULL,                            .type = OP_NULL,                  .precedence = OPP_NONE  };
static __rpn_operation RpnDebugStack         = { .surface.token="DEBUG.STACK",      .function.eval = NULL,                            .type = OP_NULL,                  .precedence = OPP_NONE  };
static __rpn_operation RpnDebugRpn           = { .surface.token="DEBUG.RPN",        .function.eval = NULL,                            .type = OP_NULL,                  .precedence = OPP_NONE  };
static __rpn_operation RpnDebugMemory        = { .surface.token="DEBUG.MEM",        .function.eval = NULL,                            .type = OP_NULL,                  .precedence = OPP_NONE  };
static __rpn_operation RpnDebugEval          = { .surface.token="DEBUG.EVAL",       .function.eval = __debug_eval,                    .type = OP_PASSTHRU,              .precedence = OPP_NONE  };
static __rpn_operation RpnDebugArc           = { .surface.token="DEBUG.ARC",        .function.eval = __debug_arc,                     .type = OP_PASSTHRU,              .precedence = OPP_NONE  };
static __rpn_operation RpnDebugPrint         = { .surface.token="DEBUG.PRINT",      .function.eval = __debug_print,                   .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };
static __rpn_operation RpnDebugPrintIf       = { .surface.token="DEBUG.PRINTIF",    .function.eval = __debug_printif,                 .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL  };

static __rpn_operation RpnCPUKill            = { .surface.token="cpukill",          .function.eval = __eval_cpukill,                  .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };
static __rpn_operation RpnMemKill            = { .surface.token="memkill",          .function.eval = __eval_memkill,                  .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };

static __rpn_operation RpnReturn             = { .surface.token="return",           .function.eval = __eval_return,                   .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };
static __rpn_operation RpnReturnIf           = { .surface.token="returnif",         .function.eval = __eval_returnif,                 .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL  };
static __rpn_operation RpnRequire            = { .surface.token="require",          .function.eval = __eval_require,                  .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };

static __rpn_operation RpnHalt               = { .surface.token="halt",             .function.eval = __eval_halt,                     .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL  };
static __rpn_operation RpnHaltIf             = { .surface.token="haltif",           .function.eval = __eval_haltif,                   .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };
static __rpn_operation RpnHalted             = { .surface.token="halted",           .function.eval = __eval_halted,                   .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL  };
static __rpn_operation RpnContinue           = { .surface.token="continue",         .function.eval = __eval_continue,                 .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL  };
static __rpn_operation RpnContinueIf         = { .surface.token="continueif",       .function.eval = __eval_continueif,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL  };

// Context
static __rpn_operation RpnPushRank           = { .surface.token="context.rank",     .function.eval = __stack_push_context_rank,       .type = OP_REAL_OPERAND,          .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushRank          = { .surface.token=".rank",            .function.eval = __stack_push_context_rank,       .type = OP_REAL_OPERAND,          .precedence = OPP_ATTRIBUTE };

// System
static __rpn_operation RpnPushSysTick        = { .surface.token="sys.tick",         .function.eval = __stack_push_sys_tick,           .type = OP_REAL_OPERAND,          .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushSysUptime      = { .surface.token="sys.uptime",       .function.eval = __stack_push_sys_uptime,         .type = OP_REAL_OPERAND,          .precedence = OPP_ATTRIBUTE };

// Random
static __rpn_operation RpnPushRandomInt      = { .surface.token="randint",          .function.eval = __eval_binary_random_int,        .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnPushRandomBits     = { .surface.token="randbits",         .function.eval = __eval_nullary_random_bits,      .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnPushRandomReal     = { .surface.token="random",           .function.eval = __eval_nullary_random_real,      .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };


static __rpn_operation RpnPushReg1           = { .surface.token="r1",               .function.eval = __eval_memory_load_reg1,         .type = OP_REGISTER_OPERAND,      .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushReg2           = { .surface.token="r2",               .function.eval = __eval_memory_load_reg2,         .type = OP_REGISTER_OPERAND,      .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushReg3           = { .surface.token="r3",               .function.eval = __eval_memory_load_reg3,         .type = OP_REGISTER_OPERAND,      .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushReg4           = { .surface.token="r4",               .function.eval = __eval_memory_load_reg4,         .type = OP_REGISTER_OPERAND,      .precedence = OPP_CONSTANT };

static __rpn_operation RpnPushMemX           = { .surface.token="M",                .function.eval = __eval_memory_load,              .type = OP_MEMORY,                .precedence = OPP_SUBSCRIPT };

static __rpn_operation RpnPushEnumRelEnc     = { .surface.token="rel",              .function.eval = __stack_noop,                    .type = OP_RELATIONSHIP,          .precedence = OPP_SUBSCRIPT };
static __rpn_operation RpnPushEnumVtxType    = { .surface.token="type",             .function.eval = __stack_noop,                    .type = OP_VERTEXTYPE,            .precedence = OPP_SUBSCRIPT };

// Unary
static __rpn_operation RpnUnaryNot           = { .surface.token="!",                .function.eval = __eval_unary_not,                .type = OP_UNARY_PREFIX,          .precedence = OPP_LOGICAL_NOT };
static __rpn_operation RpnUnaryBitwiseNot    = { .surface.token="~",                .function.eval = __eval_unary_bitwise_not,        .type = OP_UNARY_PREFIX,          .precedence = OPP_BITWISE_NOT };
static __rpn_operation RpnUnaryMinus         = { .surface.token=NULL,               .function.eval = __eval_unary_neg,                .type = OP_UNARY_PREFIX,          .precedence = OPP_UNARY_MINUS };
static __rpn_operation RpnUnaryNeg           = { .surface.token="neg",              .function.eval = __eval_unary_neg,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastInt       = { .surface.token="int",              .function.eval = __eval_unary_cast_int,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastIntR      = { .surface.token="intr",             .function.eval = __eval_unary_cast_intr,          .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastAsInt     = { .surface.token="asint",            .function.eval = __eval_unary_cast_asint,         .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastAsBits    = { .surface.token="asbits",           .function.eval = __eval_unary_cast_asbits,        .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastReal      = { .surface.token="real",             .function.eval = __eval_unary_cast_real,          .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastAsReal    = { .surface.token="asreal",           .function.eval = __eval_unary_cast_asreal,        .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastStr       = { .surface.token="str",              .function.eval = __eval_unary_cast_str,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastBitvector = { .surface.token="bitvector",        .function.eval = __eval_unary_cast_bitvector,     .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnVariadicBytes      = { .surface.token="bytes",            .function.eval = __eval_variadic_bytes,           .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCastKeyVal    = { .surface.token="keyval",           .function.eval = __eval_binary_cast_keyval,       .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnUnaryHash          = { .surface.token="hash",             .function.eval = __eval_unary_hash,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

static __rpn_operation RpnStringStrcmp       = { .surface.token="strcmp",           .function.eval = __eval_string_strcmp,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringStrcasecmp   = { .surface.token="strcasecmp",       .function.eval = __eval_string_strcasecmp,        .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringStartswith   = { .surface.token="startswith",       .function.eval = __eval_string_startswith,        .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringEndswith     = { .surface.token="endswith",         .function.eval = __eval_string_endswith,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringNormalize    = { .surface.token="normalize",        .function.eval = __eval_string_normalize,         .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnStringJoin         = { .surface.token="join",             .function.eval = __eval_string_join,              .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnStringReplace      = { .surface.token="replace",          .function.eval = __eval_string_replace,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnStringSlice        = { .surface.token="slice",            .function.eval = __eval_string_slice,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnStringPrefix       = { .surface.token="prefix",           .function.eval = __eval_string_prefix,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringIndex        = { .surface.token="idx",              .function.eval = __eval_string_index,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnStringStrftime     = { .surface.token="strftime",         .function.eval = __eval_string_strftime,          .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

static __rpn_operation RpnUnaryIsNan         = { .surface.token="isnan",            .function.eval = __eval_unary_isnan,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsInf         = { .surface.token="isinf",            .function.eval = __eval_unary_isinf,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsInt         = { .surface.token="isint",            .function.eval = __eval_unary_isint,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsReal        = { .surface.token="isreal",           .function.eval = __eval_unary_isreal,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsBitvector   = { .surface.token="isbitvector",      .function.eval = __eval_unary_isbitvector,        .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsKeyVal      = { .surface.token="iskeyval",         .function.eval = __eval_unary_iskeyval,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsString      = { .surface.token="isstr",            .function.eval = __eval_unary_isstr,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsVector      = { .surface.token="isvector",         .function.eval = __eval_unary_isvector,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsBytearray   = { .surface.token="isbytearray",      .function.eval = __eval_unary_isbytearray,        .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsBytes       = { .surface.token="isbytes",          .function.eval = __eval_unary_isbytes,            .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsUTF8        = { .surface.token="isutf8",           .function.eval = __eval_unary_isutf8,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsArray       = { .surface.token="isarray",          .function.eval = __eval_unary_isarray,            .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryIsMap         = { .surface.token="ismap",            .function.eval = __eval_unary_ismap,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnAnyNan             = { .surface.token="anynan",           .function.eval = __eval_variadic_anynan,          .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnAllNan             = { .surface.token="allnan",           .function.eval = __eval_variadic_allnan,          .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };

static __rpn_operation RpnUnaryInv           = { .surface.token="inv",              .function.eval = __eval_unary_inv,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryLog2          = { .surface.token="log2",             .function.eval = __eval_unary_log2,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryLog           = { .surface.token="log",              .function.eval = __eval_unary_log,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryLog10         = { .surface.token="log10",            .function.eval = __eval_unary_log10,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryRad           = { .surface.token="rad",              .function.eval = __eval_unary_rad,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryDeg           = { .surface.token="deg",              .function.eval = __eval_unary_deg,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnarySin           = { .surface.token="sin",              .function.eval = __eval_unary_sin,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCos           = { .surface.token="cos",              .function.eval = __eval_unary_cos,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryTan           = { .surface.token="tan",              .function.eval = __eval_unary_tan,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryASin          = { .surface.token="asin",             .function.eval = __eval_unary_asin,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryACos          = { .surface.token="acos",             .function.eval = __eval_unary_acos,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryATan          = { .surface.token="atan",             .function.eval = __eval_unary_atan,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnarySinh          = { .surface.token="sinh",             .function.eval = __eval_unary_sinh,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCosh          = { .surface.token="cosh",             .function.eval = __eval_unary_cosh,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryTanh          = { .surface.token="tanh",             .function.eval = __eval_unary_tanh,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryASinh         = { .surface.token="asinh",            .function.eval = __eval_unary_asinh,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryACosh         = { .surface.token="acosh",            .function.eval = __eval_unary_acosh,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryATanh         = { .surface.token="atanh",            .function.eval = __eval_unary_atanh,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnarySinc          = { .surface.token="sinc",             .function.eval = __eval_unary_sinc,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryExp           = { .surface.token="exp",              .function.eval = __eval_unary_exp,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryAbs           = { .surface.token="abs",              .function.eval = __eval_unary_abs,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnarySqrt          = { .surface.token="sqrt",             .function.eval = __eval_unary_sqrt,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryCeil          = { .surface.token="ceil",             .function.eval = __eval_unary_ceil,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryFloor         = { .surface.token="floor",            .function.eval = __eval_unary_floor,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryRound         = { .surface.token="round",            .function.eval = __eval_unary_round,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnarySign          = { .surface.token="sign",             .function.eval = __eval_unary_sign,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryFactorial     = { .surface.token="fac",              .function.eval = __eval_unary_fac,                .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnUnaryPopcnt        = { .surface.token="popcnt",           .function.eval = __eval_unary_popcnt,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

// Binary
static __rpn_operation RpnBinaryAdd          = { .surface.token="+",                .function.eval = __eval_binary_add,               .type = OP_BINARY_INFIX,          .precedence = OPP_ADDITION };
static __rpn_operation RpnBinarySub          = { .surface.token="-",                .function.eval = __eval_binary_sub,               .type = OP_BINARY_INFIX,          .precedence = OPP_SUBTRACTION };
static __rpn_operation RpnBinaryMul          = { .surface.token="*",                .function.eval = __eval_binary_mul,               .type = OP_BINARY_INFIX,          .precedence = OPP_MULTIPLICATION };
static __rpn_operation RpnBinaryDiv          = { .surface.token="/",                .function.eval = __eval_binary_div,               .type = OP_BINARY_INFIX,          .precedence = OPP_DIVISION };
static __rpn_operation RpnBinaryMod          = { .surface.token="%",                .function.eval = __eval_binary_mod,               .type = OP_BINARY_INFIX,          .precedence = OPP_MODULO };
static __rpn_operation RpnBinaryPow          = { .surface.token="**",               .function.eval = __eval_binary_pow,               .type = OP_BINARY_INFIX,          .precedence = OPP_EXPONENTIATION };
static __rpn_operation RpnBinaryATan2        = { .surface.token="atan2",            .function.eval = __eval_binary_atan2,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryMax          = { .surface.token="max",              .function.eval = __eval_binary_max,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryMin          = { .surface.token="min",              .function.eval = __eval_binary_min,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryProx         = { .surface.token="prox",             .function.eval = __eval_binary_prox,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryApprox       = { .surface.token="approx",           .function.eval = __eval_binary_approx,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnBinaryRange        = { .surface.token="range",            .function.eval = __eval_binary_range,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryHamDist      = { .surface.token="hamdist",          .function.eval = __eval_binary_hamdist,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryEuclidean    = { .surface.token="euclidean",        .function.eval = __eval_binary_euclidean,         .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinarySimilarity   = { .surface.token="sim",              .function.eval = __eval_binary_sim,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryCosine       = { .surface.token="cosine",           .function.eval = __eval_binary_cosine,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryJaccard      = { .surface.token="jaccard",          .function.eval = __eval_binary_jaccard,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryComb         = { .surface.token="comb",             .function.eval = __eval_binary_comb,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnBinaryEqu          = { .surface.token="==",               .function.eval = __eval_binary_equ,               .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_EQUALITY };
static __rpn_operation RpnBinaryNeq          = { .surface.token="!=",               .function.eval = __eval_binary_neq,               .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_EQUALITY };
static __rpn_operation RpnBinaryGt           = { .surface.token=">",                .function.eval = __eval_binary_gt,                .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_COMPARISON };
static __rpn_operation RpnBinaryGte          = { .surface.token=">=",               .function.eval = __eval_binary_gte,               .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_COMPARISON };
static __rpn_operation RpnBinaryLt           = { .surface.token="<",                .function.eval = __eval_binary_lt,                .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_COMPARISON };
static __rpn_operation RpnBinaryLte          = { .surface.token="<=",               .function.eval = __eval_binary_lte,               .type = OP_BINARY_CMP_INFIX,      .precedence = OPP_COMPARISON };
static __rpn_operation RpnBinaryElementOf    = { .surface.token="in",               .function.eval = __eval_binary_elementof,         .type = OP_BINARY_INFIX,          .precedence = OPP_SET_MEMBER };
static __rpn_operation RpnBinaryNotElementOf = { .surface.token="notin",            .function.eval = __eval_binary_notelementof,      .type = OP_BINARY_INFIX,          .precedence = OPP_SET_MEMBER };

// Logical
static __rpn_operation RpnLogicalOr          = { .surface.token="||",               .function.eval = __eval_logical_or,               .type = OP_BINARY_INFIX,          .precedence = OPP_LOGICAL_OR };
static __rpn_operation RpnLogicalAnd         = { .surface.token="&&",               .function.eval = __eval_logical_and,              .type = OP_BINARY_INFIX,          .precedence = OPP_LOGICAL_AND };

// Ternary
static __rpn_operation RpnTernaryCondition   = { .surface.token="?",                .function.eval = __eval_ternary_condition,        .type = OP_TERNARY_COND_INFIX,    .precedence = OPP_TERNARY_CONDITIONAL };
static __rpn_operation RpnTernaryColon       = { .surface.token=":",                .function.eval = __stack_noop,                    .type = OP_TERNARY_COLON_INFIX,   .precedence = OPP_TERNARY_COLON };

// Quaternary
static __rpn_operation RpnQuaternaryHavDist  = { .surface.token="geodist",          .function.eval = __eval_quaternary_havdist,       .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };
static __rpn_operation RpnQuaternaryGeoProx  = { .surface.token="geoprox",          .function.eval = __eval_quaternary_geoprox,       .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };

// Bitwise
static __rpn_operation RpnBitwiseShiftLeft   = { .surface.token="<<",               .function.eval = __eval_bitwise_shl,              .type = OP_BITWISE_INFIX,         .precedence = OPP_BITSHIFT };
static __rpn_operation RpnBitwiseShiftRight  = { .surface.token=">>",               .function.eval = __eval_bitwise_shr,              .type = OP_BITWISE_INFIX,         .precedence = OPP_BITSHIFT };
static __rpn_operation RpnBitwiseOr          = { .surface.token="|",                .function.eval = __eval_bitwise_or,               .type = OP_BITWISE_INFIX,         .precedence = OPP_BITWISE_OR };
static __rpn_operation RpnBitwiseAnd         = { .surface.token="&",                .function.eval = __eval_bitwise_and,              .type = OP_BITWISE_INFIX,         .precedence = OPP_BITWISE_AND };
static __rpn_operation RpnBitwiseXor         = { .surface.token="^",                .function.eval = __eval_bitwise_xor,              .type = OP_BITWISE_INFIX,         .precedence = OPP_BITWISE_XOR };

// Assignment
static __rpn_operation RpnAssign             = { .surface.token=":=",               .function.eval = NULL,                            .type = OP_ASSIGN,                .precedence = OPP_ASSIGNMENT };

// Variable
static __rpn_operation RpnAssignVariable     = { .surface.token="=",                .function.eval = NULL,                            .type = OP_VARIABLE,              .precedence = OPP_ASSIGNMENT };
static __rpn_operation RpnPushVariable       = { .surface.token=NULL,               .function.eval = __stack_push_stackval,           .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };

// Comma
static __rpn_operation RpnComma              = { .surface.token=",",                .function.eval = NULL,                            .type = OP_COMMA,                 .precedence = OPP_COMMA };
static __rpn_operation RpnSemicolon          = { .surface.token=";",                .function.eval = NULL,                            .type = OP_SEMICOLON,             .precedence = OPP_SEMICOLON };

// Enum
static __rpn_operation RpnEnumRelEnc         = { .surface.token="relenc",           .function.eval = __eval_string_relenc,            .type = OP_STRING_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnEnumTypeEnc        = { .surface.token="typeenc",          .function.eval = __eval_string_typeenc,           .type = OP_STRING_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnEnumRelDec         = { .surface.token="reldec",           .function.eval = __eval_string_reldec,            .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnEnumTypeDec        = { .surface.token="typedec",          .function.eval = __eval_string_typedec,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnEnumModToStr       = { .surface.token="modtostr",         .function.eval = __eval_string_modtostr,          .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnEnumDirToStr       = { .surface.token="dirtostr",         .function.eval = __eval_string_dirtostr,          .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

// Memory single register
static __rpn_operation RpnMemoryStore        = { .surface.token="store",           .function.eval = __eval_memory_store,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefStore     = { .surface.token="rstore",          .function.eval = __eval_memory_rstore,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryStoreIf      = { .surface.token="storeif",         .function.eval = __eval_memory_storeif,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefStoreIf   = { .surface.token="rstoreif",        .function.eval = __eval_memory_rstoreif,          .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryWrite        = { .surface.token="write",           .function.eval = __eval_memory_write,             .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryWriteIf      = { .surface.token="writeif",         .function.eval = __eval_memory_writeif,           .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRWrite       = { .surface.token="rwrite",          .function.eval = __eval_memory_rwrite,            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRWriteIf     = { .surface.token="rwriteif",        .function.eval = __eval_memory_rwriteif,          .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };

static __rpn_operation RpnMemoryLoad         = { .surface.token="load",            .function.eval = __eval_memory_load,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefLoad      = { .surface.token="rload",           .function.eval = __eval_memory_rload,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

static __rpn_operation RpnMemoryPush         = { .surface.token="push",            .function.eval = __eval_memory_push,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryPushIf       = { .surface.token="pushif",          .function.eval = __eval_memory_pushif,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryPop          = { .surface.token="pop",             .function.eval = __eval_memory_pop,               .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryPopIf        = { .surface.token="popif",           .function.eval = __eval_memory_popif,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryGet          = { .surface.token="get",             .function.eval = __eval_memory_get,               .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMov          = { .surface.token="mov",             .function.eval = __eval_memory_mov,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefMov       = { .surface.token="rmov",            .function.eval = __eval_memory_rmov,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMovIf        = { .surface.token="movif",           .function.eval = __eval_memory_movif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefMovIf     = { .surface.token="rmovif",          .function.eval = __eval_memory_rmovif,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryXchg         = { .surface.token="xchg",            .function.eval = __eval_memory_xchg,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefXchg      = { .surface.token="rxchg",           .function.eval = __eval_memory_rxchg,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryXchgIf       = { .surface.token="xchgif",          .function.eval = __eval_memory_xchgif,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefXchgIf    = { .surface.token="rxchgif",         .function.eval = __eval_memory_rxchgif,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryInc          = { .surface.token="inc",             .function.eval = __eval_memory_inc,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefInc       = { .surface.token="rinc",            .function.eval = __eval_memory_rinc,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryIncIf        = { .surface.token="incif",           .function.eval = __eval_memory_incif,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefIncIf     = { .surface.token="rincif",          .function.eval = __eval_memory_rincif,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryDec          = { .surface.token="dec",             .function.eval = __eval_memory_dec,               .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefDec       = { .surface.token="rdec",            .function.eval = __eval_memory_rdec,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryDecIf        = { .surface.token="decif",           .function.eval = __eval_memory_decif,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefDecIf     = { .surface.token="rdecif",          .function.eval = __eval_memory_rdecif,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryEqu          = { .surface.token="equ",             .function.eval = __eval_memory_equ,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefEqu       = { .surface.token="requ",            .function.eval = __eval_memory_requ,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryNeq          = { .surface.token="neq",             .function.eval = __eval_memory_neq,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefNeq       = { .surface.token="rneq",            .function.eval = __eval_memory_rneq,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryGt           = { .surface.token="gt",              .function.eval = __eval_memory_gt,                .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefGt        = { .surface.token="rgt",             .function.eval = __eval_memory_rgt,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryGte          = { .surface.token="gte",             .function.eval = __eval_memory_gte,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefGte       = { .surface.token="rgte",            .function.eval = __eval_memory_rgte,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryLt           = { .surface.token="lt",              .function.eval = __eval_memory_lt,                .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefLt        = { .surface.token="rlt",             .function.eval = __eval_memory_rlt,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryLte          = { .surface.token="lte",             .function.eval = __eval_memory_lte,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryRefLte       = { .surface.token="rlte",            .function.eval = __eval_memory_rlte,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryAdd          = { .surface.token="add",             .function.eval = __eval_memory_add,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryAddIf        = { .surface.token="addif",           .function.eval = __eval_memory_addif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemorySub          = { .surface.token="sub",             .function.eval = __eval_memory_sub,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemorySubIf        = { .surface.token="subif",           .function.eval = __eval_memory_subif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMul          = { .surface.token="mul",             .function.eval = __eval_memory_mul,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMulIf        = { .surface.token="mulif",           .function.eval = __eval_memory_mulif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryDiv          = { .surface.token="div",             .function.eval = __eval_memory_div,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryDivIf        = { .surface.token="divif",           .function.eval = __eval_memory_divif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMod          = { .surface.token="mod",             .function.eval = __eval_memory_mod,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryModIf        = { .surface.token="modif",           .function.eval = __eval_memory_modif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryShr          = { .surface.token="shr",             .function.eval = __eval_memory_shr,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryShrIf        = { .surface.token="shrif",           .function.eval = __eval_memory_shrif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryShl          = { .surface.token="shl",             .function.eval = __eval_memory_shl,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryShlIf        = { .surface.token="shlif",           .function.eval = __eval_memory_shlif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryAnd          = { .surface.token="and",             .function.eval = __eval_memory_and,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryAndIf        = { .surface.token="andif",           .function.eval = __eval_memory_andif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryOr           = { .surface.token="or",              .function.eval = __eval_memory_or,                .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryOrIf         = { .surface.token="orif",            .function.eval = __eval_memory_orif,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryXor          = { .surface.token="xor",             .function.eval = __eval_memory_xor,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryXorIf        = { .surface.token="xorif",           .function.eval = __eval_memory_xorif,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemorySmooth       = { .surface.token="smooth",          .function.eval = __eval_memory_smooth,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryCount        = { .surface.token="count",           .function.eval = __eval_memory_count,             .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryCountIf      = { .surface.token="countif",         .function.eval = __eval_memory_countif,           .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };

#if defined CXPLAT_ARCH_X64
static __rpn_operation RpnMemory_ecld_pi8_512= { .surface.token="ecld_pi8_512",    .function.eval = __eval_avx512_ecld_pi8,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_512 = { .surface.token="ssq_pi8_512",     .function.eval = __eval_avx512_ssq_pi8,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_512 = { .surface.token="rsqrtssq_pi8_512",.function.eval = __eval_avx512_rsqrtssq_pi8, .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_512  = { .surface.token="dp_pi8_512",      .function.eval = __eval_avx512_dp_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_512 = { .surface.token="cos_pi8_512",     .function.eval = __eval_avx512_cos_pi8,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#elif defined CXPLAT_ARCH_ARM64
static __rpn_operation RpnMemory_ecld_pi8_512= { .surface.token="ecld_pi8_512",    .function.eval = __eval_neon_ecld_pi8,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_512 = { .surface.token="ssq_pi8_512",     .function.eval = __eval_neon_ssq_pi8,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_512 = { .surface.token="rsqrtssq_pi8_512",.function.eval = __eval_neon_rsqrtssq_pi8, .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_512  = { .surface.token="dp_pi8_512",      .function.eval = __eval_neon_dp_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_512 = { .surface.token="cos_pi8_512",     .function.eval = __eval_neon_cos_pi8,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#else
static __rpn_operation RpnMemory_ecld_pi8_512= { .surface.token="ecld_pi8_512",    .function.eval = __eval_scalar_ecld_pi8,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_512 = { .surface.token="ssq_pi8_512",     .function.eval = __eval_scalar_ssq_pi8,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_512 = { .surface.token="rsqrtssq_pi8_512",.function.eval = __eval_scalar_rsqrtssq_pi8, .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_512  = { .surface.token="dp_pi8_512",      .function.eval = __eval_scalar_dp_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_512 = { .surface.token="cos_pi8_512",     .function.eval = __eval_scalar_cos_pi8,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#endif

#if defined CXPLAT_ARCH_X64
static __rpn_operation RpnMemory_ecld_pi8_256= { .surface.token="ecld_pi8_256",    .function.eval = __eval_avx2_ecld_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_256 = { .surface.token="ssq_pi8_256",     .function.eval = __eval_avx2_ssq_pi8,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_256 = { .surface.token="rsqrtssq_pi8_256",.function.eval = __eval_avx2_rsqrtssq_pi8,   .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_256  = { .surface.token="dp_pi8_256",      .function.eval = __eval_avx2_dp_pi8,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_256 = { .surface.token="cos_pi8_256",     .function.eval = __eval_avx2_cos_pi8,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#elif defined CXPLAT_ARCH_ARM64
static __rpn_operation RpnMemory_ecld_pi8_256= { .surface.token="ecld_pi8_256",    .function.eval = __eval_neon_ecld_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_256 = { .surface.token="ssq_pi8_256",     .function.eval = __eval_neon_ssq_pi8,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_256 = { .surface.token="rsqrtssq_pi8_256",.function.eval = __eval_neon_rsqrtssq_pi8,   .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_256  = { .surface.token="dp_pi8_256",      .function.eval = __eval_neon_dp_pi8,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_256 = { .surface.token="cos_pi8_256",     .function.eval = __eval_neon_cos_pi8,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#else
static __rpn_operation RpnMemory_ecld_pi8_256= { .surface.token="ecld_pi8_256",    .function.eval = __eval_scalar_ecld_pi8,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8_256 = { .surface.token="ssq_pi8_256",     .function.eval = __eval_scalar_ssq_pi8,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_rsqrtssq_pi8_256 = { .surface.token="rsqrtssq_pi8_256",.function.eval = __eval_scalar_rsqrtssq_pi8, .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8_256  = { .surface.token="dp_pi8_256",      .function.eval = __eval_scalar_dp_pi8,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8_256 = { .surface.token="cos_pi8_256",     .function.eval = __eval_scalar_cos_pi8,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
#endif

static __rpn_operation RpnMemory_ecld_pi8    = { .surface.token="ecld_pi8",        .function.eval = __eval_ecld_pi8,                 .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ssq_pi8     = { .surface.token="ssq_pi8",         .function.eval = __eval_ssq_pi8,                  .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemory_dp_pi8      = { .surface.token="dp_pi8",          .function.eval = __eval_dp_pi8,                   .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_cos_pi8     = { .surface.token="cos_pi8",         .function.eval = __eval_cos_pi8,                  .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemory_ham_pi8     = { .surface.token="ham_pi8",         .function.eval = __eval_ham_pi8,                  .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryModIndex     = { .surface.token="modindex",        .function.eval = __eval_memory_modindex,          .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryIndex        = { .surface.token="index",           .function.eval = __eval_memory_index,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryIndexed      = { .surface.token="indexed",         .function.eval = __eval_memory_indexed,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryUnindex      = { .surface.token="unindex",         .function.eval = __eval_memory_unindex,           .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };



// Memory multi register
static __rpn_operation RpnMemoryMSet         = { .surface.token="mset",            .function.eval = __eval_memory_mset,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMReset       = { .surface.token="mreset",          .function.eval = __eval_memory_mreset,            .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRandomize   = { .surface.token="mrandomize",      .function.eval = __eval_memory_mrandomize,        .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRandbits    = { .surface.token="mrandbits",       .function.eval = __eval_memory_mrandbits,         .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCopy        = { .surface.token="mcopy",           .function.eval = __eval_memory_mcopy,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMPWrite      = { .surface.token="mpwrite",         .function.eval = __eval_memory_mpwrite,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCopyObj     = { .surface.token="mcopyobj",        .function.eval = __eval_memory_mcopyobj,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMTerm        = { .surface.token="mterm",           .function.eval = __eval_memory_mterm,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMLen         = { .surface.token="mlen",            .function.eval = __eval_memory_mlen,              .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

static __rpn_operation RpnMemoryHeapInit     = { .surface.token="mheapinit",       .function.eval = __eval_heap_init,                .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapPushMin  = { .surface.token="mheappushmin",    .function.eval = __eval_heap_pushmin,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapPushMax  = { .surface.token="mheappushmax",    .function.eval = __eval_heap_pushmax,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapWriteMin = { .surface.token="mheapwritemin",   .function.eval = __eval_heap_writemin,            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapWriteMax = { .surface.token="mheapwritemax",   .function.eval = __eval_heap_writemax,            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapifyMin   = { .surface.token="mheapifymin",     .function.eval = __eval_heap_heapifymin,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapifyMax   = { .surface.token="mheapifymax",     .function.eval = __eval_heap_heapifymax,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapSiftMin  = { .surface.token="mheapsiftmin",    .function.eval = __eval_heap_siftmin,             .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };
static __rpn_operation RpnMemoryHeapSiftMax  = { .surface.token="mheapsiftmax",    .function.eval = __eval_heap_siftmax,             .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMSort        = { .surface.token="msort",           .function.eval = __eval_memory_msort,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSortRev     = { .surface.token="msortrev",        .function.eval = __eval_memory_msortrev,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSort       = { .surface.token="mrsort",          .function.eval = __eval_memory_mrsort,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSortRev    = { .surface.token="mrsortrev",       .function.eval = __eval_memory_mrsortrev,         .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMReverse     = { .surface.token="mreverse",        .function.eval = __eval_memory_mreverse,          .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMInt         = { .surface.token="mint",            .function.eval = __eval_memory_mint,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIntR        = { .surface.token="mintr",           .function.eval = __eval_memory_mintr,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMReal        = { .surface.token="mreal",           .function.eval = __eval_memory_mreal,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMBits        = { .surface.token="mbits",           .function.eval = __eval_memory_mbits,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMInc         = { .surface.token="minc",            .function.eval = __eval_memory_minc,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIInc        = { .surface.token="miinc",           .function.eval = __eval_memory_miinc,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRInc        = { .surface.token="mrinc",           .function.eval = __eval_memory_mrinc,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMDec         = { .surface.token="mdec",            .function.eval = __eval_memory_mdec,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIDec        = { .surface.token="midec",           .function.eval = __eval_memory_midec,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRDec        = { .surface.token="mrdec",           .function.eval = __eval_memory_mrdec,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMAdd         = { .surface.token="madd",            .function.eval = __eval_memory_madd,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIAdd        = { .surface.token="miadd",           .function.eval = __eval_memory_miadd,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRAdd        = { .surface.token="mradd",           .function.eval = __eval_memory_mradd,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVAdd        = { .surface.token="mvadd",           .function.eval = __eval_memory_mvadd,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMSub         = { .surface.token="msub",            .function.eval = __eval_memory_msub,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMISub        = { .surface.token="misub",           .function.eval = __eval_memory_misub,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSub        = { .surface.token="mrsub",           .function.eval = __eval_memory_mrsub,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVSub        = { .surface.token="mvsub",           .function.eval = __eval_memory_mvsub,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMMul         = { .surface.token="mmul",            .function.eval = __eval_memory_mmul,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIMul        = { .surface.token="mimul",           .function.eval = __eval_memory_mimul,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRMul        = { .surface.token="mrmul",           .function.eval = __eval_memory_mrmul,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVMul        = { .surface.token="mvmul",           .function.eval = __eval_memory_mvmul,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMDiv         = { .surface.token="mdiv",            .function.eval = __eval_memory_mdiv,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIDiv        = { .surface.token="midiv",           .function.eval = __eval_memory_midiv,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRDiv        = { .surface.token="mrdiv",           .function.eval = __eval_memory_mrdiv,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVDiv        = { .surface.token="mvdiv",           .function.eval = __eval_memory_mvdiv,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMMod         = { .surface.token="mmod",            .function.eval = __eval_memory_mmod,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIMod        = { .surface.token="mimod",           .function.eval = __eval_memory_mimod,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRMod        = { .surface.token="mrmod",           .function.eval = __eval_memory_mrmod,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVMod        = { .surface.token="mvmod",           .function.eval = __eval_memory_mvmod,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMInv         = { .surface.token="minv",            .function.eval = __eval_memory_minv,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRInv        = { .surface.token="mrinv",           .function.eval = __eval_memory_mrinv,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMPow         = { .surface.token="mpow",            .function.eval = __eval_memory_mpow,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRPow        = { .surface.token="mrpow",           .function.eval = __eval_memory_mrpow,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSq          = { .surface.token="msq",             .function.eval = __eval_memory_msq,               .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSq         = { .surface.token="mrsq",            .function.eval = __eval_memory_mrsq,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSqrt        = { .surface.token="msqrt",           .function.eval = __eval_memory_msqrt,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSqrt       = { .surface.token="mrsqrt",          .function.eval = __eval_memory_mrsqrt,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMCeil        = { .surface.token="mceil",           .function.eval = __eval_memory_mceil,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRCeil       = { .surface.token="mrceil",          .function.eval = __eval_memory_mrceil,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMFloor       = { .surface.token="mfloor",          .function.eval = __eval_memory_mfloor,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRFloor      = { .surface.token="mrfloor",         .function.eval = __eval_memory_mrfloor,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRound       = { .surface.token="mround",          .function.eval = __eval_memory_mround,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRRound      = { .surface.token="mrround",         .function.eval = __eval_memory_mrround,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMAbs         = { .surface.token="mabs",            .function.eval = __eval_memory_mabs,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRAbs        = { .surface.token="mrabs",           .function.eval = __eval_memory_mrabs,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSign        = { .surface.token="msign",           .function.eval = __eval_memory_msign,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSign       = { .surface.token="mrsign",          .function.eval = __eval_memory_mrsign,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMLog2        = { .surface.token="mlog2",           .function.eval = __eval_memory_mlog2,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRLog2       = { .surface.token="mrlog2",          .function.eval = __eval_memory_mrlog2,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMLog         = { .surface.token="mlog",            .function.eval = __eval_memory_mlog,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRLog        = { .surface.token="mrlog",           .function.eval = __eval_memory_mrlog,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMLog10       = { .surface.token="mlog10",          .function.eval = __eval_memory_mlog10,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRLog10      = { .surface.token="mrlog10",         .function.eval = __eval_memory_mrlog10,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMExp2        = { .surface.token="mexp2",           .function.eval = __eval_memory_mexp2,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRExp2       = { .surface.token="mrexp2",          .function.eval = __eval_memory_mrexp2,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMExp         = { .surface.token="mexp",            .function.eval = __eval_memory_mexp,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRExp        = { .surface.token="mrexp",           .function.eval = __eval_memory_mrexp,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMExp10       = { .surface.token="mexp10",          .function.eval = __eval_memory_mexp10,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRExp10      = { .surface.token="mrexp10",         .function.eval = __eval_memory_mrexp10,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };


static __rpn_operation RpnMemoryMRad         = { .surface.token="mrad",            .function.eval = __eval_memory_mrad,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRRad        = { .surface.token="mrrad",           .function.eval = __eval_memory_mrrad,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMDeg         = { .surface.token="mdeg",            .function.eval = __eval_memory_mdeg,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRDeg        = { .surface.token="mrdeg",           .function.eval = __eval_memory_mrdeg,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSin         = { .surface.token="msin",            .function.eval = __eval_memory_msin,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSin        = { .surface.token="mrsin",           .function.eval = __eval_memory_mrsin,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCos         = { .surface.token="mcos",            .function.eval = __eval_memory_mcos,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRCos        = { .surface.token="mrcos",           .function.eval = __eval_memory_mrcos,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMTan         = { .surface.token="mtan",            .function.eval = __eval_memory_mtan,              .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRTan        = { .surface.token="mrtan",           .function.eval = __eval_memory_mrtan,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMASin        = { .surface.token="masin",           .function.eval = __eval_memory_masin,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRASin       = { .surface.token="mrasin",          .function.eval = __eval_memory_mrasin,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMACos        = { .surface.token="macos",           .function.eval = __eval_memory_macos,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRACos       = { .surface.token="mracos",          .function.eval = __eval_memory_mracos,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMATan        = { .surface.token="matan",           .function.eval = __eval_memory_matan,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRATan       = { .surface.token="mratan",          .function.eval = __eval_memory_mratan,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSinh        = { .surface.token="msinh",           .function.eval = __eval_memory_msinh,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSinh       = { .surface.token="mrsinh",          .function.eval = __eval_memory_mrsinh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCosh        = { .surface.token="mcosh",           .function.eval = __eval_memory_mcosh,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRCosh       = { .surface.token="mrcosh",          .function.eval = __eval_memory_mrcosh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMTanh        = { .surface.token="mtanh",           .function.eval = __eval_memory_mtanh,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRTanh       = { .surface.token="mrtanh",          .function.eval = __eval_memory_mrtanh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMASinh       = { .surface.token="masinh",          .function.eval = __eval_memory_masinh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRASinh      = { .surface.token="mrasinh",         .function.eval = __eval_memory_mrasinh,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMACosh       = { .surface.token="macosh",          .function.eval = __eval_memory_macosh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRACosh      = { .surface.token="mracosh",         .function.eval = __eval_memory_mracosh,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMATanh       = { .surface.token="matanh",          .function.eval = __eval_memory_matanh,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRATanh      = { .surface.token="mratanh",         .function.eval = __eval_memory_mratanh,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSinc        = { .surface.token="msinc",           .function.eval = __eval_memory_msinc,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSinc       = { .surface.token="mrsinc",          .function.eval = __eval_memory_mrsinc,            .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };


static __rpn_operation RpnMemoryMShr         = { .surface.token="mshr",            .function.eval = __eval_memory_mshr,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVShr        = { .surface.token="mvshr",           .function.eval = __eval_memory_mvshr,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMShl         = { .surface.token="mshl",            .function.eval = __eval_memory_mshl,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVShl        = { .surface.token="mvshl",           .function.eval = __eval_memory_mvshl,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMAnd         = { .surface.token="mand",            .function.eval = __eval_memory_mand,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVAnd        = { .surface.token="mvand",           .function.eval = __eval_memory_mvand,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMOr          = { .surface.token="mor",             .function.eval = __eval_memory_mor,               .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVOr         = { .surface.token="mvor",            .function.eval = __eval_memory_mvor,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMXor         = { .surface.token="mxor",            .function.eval = __eval_memory_mxor,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMVXor        = { .surface.token="mvxor",           .function.eval = __eval_memory_mvxor,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMPopcnt      = { .surface.token="mpopcnt",         .function.eval = __eval_memory_mpopcnt,           .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMHash        = { .surface.token="mhash",           .function.eval = __eval_memory_mhash,             .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMSum         = { .surface.token="msum",            .function.eval = __eval_memory_msum,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSum        = { .surface.token="mrsum",           .function.eval = __eval_memory_mrsum,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSumSqr      = { .surface.token="msumsqr",         .function.eval = __eval_memory_msumsqr,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRSumSqr     = { .surface.token="mrsumsqr",        .function.eval = __eval_memory_mrsumsqr,          .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMInvSum      = { .surface.token="minvsum",         .function.eval = __eval_memory_minvsum,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRInvSum     = { .surface.token="mrinvsum",        .function.eval = __eval_memory_mrinvsum,          .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMProd        = { .surface.token="mprod",           .function.eval = __eval_memory_mprod,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRProd       = { .surface.token="mrprod",          .function.eval = __eval_memory_mrprod,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMMean        = { .surface.token="mmean",           .function.eval = __eval_memory_mmean,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRMean       = { .surface.token="mrmean",          .function.eval = __eval_memory_mrmean,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMHarmMean    = { .surface.token="mharmmean",       .function.eval = __eval_memory_mharmmean,         .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRHarmMean   = { .surface.token="mrharmmean",      .function.eval = __eval_memory_mrharmmean,        .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMGeoMean     = { .surface.token="mgeomean",        .function.eval = __eval_memory_mgeomean,          .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRGeoMean    = { .surface.token="mrgeomean",       .function.eval = __eval_memory_mrgeomean,         .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMStdev       = { .surface.token="mstdev",          .function.eval = __eval_memory_mstdev,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRStdev      = { .surface.token="mrstdev",         .function.eval = __eval_memory_mrstdev,           .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMGeoStdev    = { .surface.token="mgeostdev",       .function.eval = __eval_memory_mgeostdev,         .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMRGeoStdev   = { .surface.token="mrgeostdev",      .function.eval = __eval_memory_mrgeostdev,        .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

static __rpn_operation RpnMemoryMMax         = { .surface.token="mmax",            .function.eval = __eval_memory_mmax,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMMin         = { .surface.token="mmin",            .function.eval = __eval_memory_mmin,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMContains    = { .surface.token="mcontains",       .function.eval = __eval_memory_mcontains,         .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCount       = { .surface.token="mcount",          .function.eval = __eval_memory_mcount,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMIndex       = { .surface.token="mindex",          .function.eval = __eval_memory_mindex,            .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCmp         = { .surface.token="mcmp",            .function.eval = __eval_memory_mcmp,              .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMCmpA        = { .surface.token="mcmpa",           .function.eval = __eval_memory_mcmpa,             .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSubset      = { .surface.token="msubset",         .function.eval = __eval_memory_msubset,           .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSubsetObj   = { .surface.token="msubsetobj",      .function.eval = __eval_memory_msubsetobj,        .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryMSumProdObj  = { .surface.token="msumprodobj",     .function.eval = __eval_memory_msumprodobj,       .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

// Fancy probes and filters
static __rpn_operation RpnMemoryProbeArray   = { .surface.token="probearray",      .function.eval = __eval_probe_probearray,         .type = OP_QUATERNARY_PREFIX,     .precedence = OPP_CALL };
static __rpn_operation RpnMemoryProbeAltArr  = { .surface.token="probealtarray",   .function.eval = __eval_probe_probealtarray,      .type = OP_QUINARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnMemoryProbeSuperArr= { .surface.token="probesuperarray", .function.eval = __eval_probe_probesuperarray,    .type = OP_QUINARY_PREFIX,        .precedence = OPP_CALL };

// Set (integer)
static __rpn_operation RpnISetAdd            = { .surface.token="iset.add",        .function.eval = __eval_maps_isetadd,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnISetDel            = { .surface.token="iset.del",        .function.eval = __eval_maps_isetdel,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnISetClr            = { .surface.token="iset.clr",        .function.eval = __eval_maps_xsetclr,             .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnISetHas            = { .surface.token="iset.has",        .function.eval = __eval_maps_isethas,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnISetLen            = { .surface.token="iset.len",        .function.eval = __eval_maps_xsetlen,             .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnISetIni            = { .surface.token="iset.ini",        .function.eval = __eval_maps_xsetini,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
// Set (vertex)
static __rpn_operation RpnVSetAdd            = { .surface.token="vset.add",        .function.eval = __eval_maps_vsetadd,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnVSetDel            = { .surface.token="vset.del",        .function.eval = __eval_maps_vsetdel,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnVSetClr            = { .surface.token="vset.clr",        .function.eval = __eval_maps_xsetclr,             .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnVSetHas            = { .surface.token="vset.has",        .function.eval = __eval_maps_vsethas,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };
static __rpn_operation RpnVSetLen            = { .surface.token="vset.len",        .function.eval = __eval_maps_xsetlen,             .type = OP_NULLARY_PREFIX,        .precedence = OPP_CALL };
static __rpn_operation RpnVSetIni            = { .surface.token="vset.ini",        .function.eval = __eval_maps_xsetini,             .type = OP_UNARY_PREFIX,          .precedence = OPP_CALL };

// Object
static __rpn_operation RpnObjectLen          = { .surface.token="len",              .function.eval = __eval_object_len,               .type = OP_OBJECT_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnObjectStrlen       = { .surface.token="strlen",           .function.eval = __eval_object_strlen,            .type = OP_OBJECT_PREFIX,         .precedence = OPP_CALL };

// Variadic
static __rpn_operation RpnGroup              = { .surface.token=NULL,               .function.eval = __eval_variadic_first,           .type = OP_VARIADIC_PREFIX,       .precedence = OPP_NONE };
static __rpn_operation RpnVarFirstValue      = { .surface.token="firstval",         .function.eval = __eval_variadic_firstval,        .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarLastValue       = { .surface.token="lastval",          .function.eval = __eval_variadic_lastval,         .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnSet                = { .surface.token=NULL,               .function.eval = __eval_variadic_set,             .type = OP_VARIADIC_PREFIX,       .precedence = OPP_NONE };
static __rpn_operation RpnVarRank            = { .surface.token="rank",             .function.eval = __eval_variadic_rank,            .type = OP_VARIADIC_HEAD_PREFIX,  .precedence = OPP_CALL };
static __rpn_operation RpnVarGeoRank         = { .surface.token="georank",          .function.eval = __eval_variadic_georank,         .type = OP_VARIADIC_HEAD_PREFIX,  .precedence = OPP_CALL };
static __rpn_operation RpnVarSum             = { .surface.token="sum",              .function.eval = __eval_variadic_sum,             .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarSumSquare       = { .surface.token="sumsqr",           .function.eval = __eval_variadic_sumsqr,          .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarStdev           = { .surface.token="stdev",            .function.eval = __eval_variadic_stdev,           .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarInvSum          = { .surface.token="invsum",           .function.eval = __eval_variadic_invsum,          .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarProduct         = { .surface.token="prod",             .function.eval = __eval_variadic_product,         .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarMean            = { .surface.token="mean",             .function.eval = __eval_variadic_mean,            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarHarmonicMean    = { .surface.token="harmmean",         .function.eval = __eval_variadic_harmmean,        .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarGeometricMean   = { .surface.token="geomean",          .function.eval = __eval_variadic_geomean,         .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarDo              = { .surface.token="do",               .function.eval = __eval_variadic_do,              .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };
static __rpn_operation RpnVarVoid            = { .surface.token="void",             .function.eval = __eval_variadic_void,            .type = OP_VARIADIC_PREFIX,       .precedence = OPP_CALL };

// Cull
static __rpn_operation RpnMCull              = { .surface.token="mcull",            .function.eval = __eval_mcull,                    .type = OP_BINARY_PREFIX,         .precedence = OPP_CALL };
static __rpn_operation RpnMCullIf            = { .surface.token="mcullif",          .function.eval = __eval_mcullif,                  .type = OP_TERNARY_PREFIX,        .precedence = OPP_CALL };

// Collectable
static __rpn_operation RpnPushCollectableReal= { .surface.token="collectable.real", .function.eval = __stack_push_collectable_real,   .type = OP_REAL_OPERAND,          .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushCollectableInt = { .surface.token="collectable.int",  .function.eval = __stack_push_collectable_int,    .type = OP_INTEGER_OPERAND,       .precedence = OPP_ATTRIBUTE };

// Collector
static __rpn_operation RpnStage              = { .surface.token="stage",            .function.eval = __eval_stage_var0_2,             .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_COLLECT, 0, 2 ),  .precedence = OPP_CALL };
static __rpn_operation RpnStageIf            = { .surface.token="stageif",          .function.eval = __eval_stageif_var1_3,           .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_COLLECT, 1, 3 ),  .precedence = OPP_CALL };
static __rpn_operation RpnUnstage            = { .surface.token="unstage",          .function.eval = __eval_unstage_var0_1,           .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_PREFIX, 0, 1 ),   .precedence = OPP_CALL };
static __rpn_operation RpnUnstageIf          = { .surface.token="unstageif",        .function.eval = __eval_unstageif_var1_2,         .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_PREFIX, 1, 2 ),   .precedence = OPP_CALL };
static __rpn_operation RpnCommit             = { .surface.token="commit",           .function.eval = __eval_commit_var0_1,            .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_PREFIX, 0, 1 ),   .precedence = OPP_CALL };
static __rpn_operation RpnCommitIf           = { .surface.token="commitif",         .function.eval = __eval_commitif_var1_2,          .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_PREFIX, 1, 2 ),   .precedence = OPP_CALL };
static __rpn_operation RpnCollect            = { .surface.token="collect",          .function.eval = __eval_collect_var0_1,           .type = ENCODE_VARIADIC_ARG_COUNTS(
                                                                                                                                              OP_VARIADIC_COLLECT, 0, 1 ),  .precedence = OPP_CALL };
static __rpn_operation RpnCollectIf          = { .surface.token="collectif",        .function.eval = __eval_collectif_var1_2,         .type = ENCODE_VARIADIC_ARG_COUNTS( 
                                                                                                                                              OP_VARIADIC_COLLECT, 1, 2 ),  .precedence = OPP_CALL };


static __rpn_operation *__rpn_definitions[] = {
      &RpnDebugTokenizer,
      &RpnDebugStack,
      &RpnDebugRpn,
      &RpnDebugMemory,
      &RpnDebugEval,
      &RpnDebugArc,
      &RpnDebugPrint,
      &RpnDebugPrintIf,

      &RpnCPUKill,
      &RpnMemKill,

      &RpnReturn,
      &RpnReturnIf,
      &RpnRequire,

      &RpnHalt,
      &RpnHaltIf,
      &RpnHalted,
      &RpnContinue,
      &RpnContinueIf,

      // Tail
      &RpnPushTail,
      &RpnPushTailFancyProp,
      &RpnPushTailPropCount,
      &RpnPushTailCoeff1,
      &RpnPushTailCoeff0,
      &RpnPushTailMan,
      &RpnPushTailId,
      &RpnPushTailObid,
      &RpnPushTailTypeCode,
      &RpnPushTailDegree,
      &RpnPushTailIndegree,
      &RpnPushTailOutdegree,
      &RpnPushTailTMC,
      &RpnPushTailTMM,
      &RpnPushTailTMX,
      &RpnPushTailVector,
      &RpnPushTailOperation,
      &RpnPushTailRefc,
      &RpnPushTailBidx,
      &RpnPushTailOidx,
      &RpnPushTailHandle,
      &RpnPushTailEnum,
      &RpnPushTailLocked,
      &RpnPushTailAddress,
      &RpnPushTailIndex,
      &RpnPushTailBitIndex,
      &RpnPushTailBitVector,
      // Prev
      &RpnPushPrevValue,
      &RpnPushPrevDistance,
      &RpnPushPrevRelcode,
      &RpnPushPrevDirection,
      &RpnPushPrevModifier,
      &RpnPushPrevIsFwdOnly,
      &RpnPushPrevIsSyn,
      &RpnPushPrevRaw,
      // This
      &RpnPushThis,
      &RpnPushThisFancyProp,
      &RpnPushThisPropCount,
      &RpnPushThisCoeff1,
      &RpnPushThisCoeff0,
      &RpnPushThisMan,
      &RpnPushThisId,
      &RpnPushThisObid,
      &RpnPushThisTypeCode,
      &RpnPushThisDegree,
      &RpnPushThisIndegree,
      &RpnPushThisOutdegree,
      &RpnPushThisTMC,
      &RpnPushThisTMM,
      &RpnPushThisTMX,
      &RpnPushThisVector,
      &RpnPushThisOperation,
      &RpnPushThisRefc,
      &RpnPushThisBidx,
      &RpnPushThisOidx,
      &RpnPushThisHandle,
      &RpnPushThisEnum,
      &RpnPushThisLocked,
      &RpnPushThisAddress,
      &RpnPushThisIndex,
      &RpnPushThisBitIndex,
      &RpnPushThisBitVector,
      // Next
      &RpnPushNextValue,
      &RpnPushNextDistance,
      &RpnPushNextRelcode,
      &RpnPushNextDirection,
      &RpnPushNextModifier,
      &RpnPushNextIsFwdOnly,
      &RpnPushNextIsSyn,
      &RpnPushNextRaw,
      &Rpn_PushNextValue,
      &Rpn_PushNextDistance,
      &Rpn_PushNextRelcode,
      &Rpn_PushNextDirection,
      &Rpn_PushNextModifier,
      &Rpn_PushNextIsFwdOnly,
      &Rpn_PushNextIsSyn,
      &Rpn_PushNextRaw,

      // Synthetic Arc
      // Arc Decay
      &RpnEvalSynArcDecay,
      &RpnEvalSynArcXDecay,
      // Multiple Arc Matching
      &RpnEvalSynArcHasRel,
      &RpnEvalSynArcHasMod,
      &RpnEvalSynArcHasRelMod,
      &RpnEvalSynArcValue,

      // Head
      &RpnPushHead,
      &RpnPushHeadFancyProp,
      &RpnPushHeadPropCount,
      &RpnPushHeadCoeff1,
      &RpnPushHeadCoeff0,
      &RpnPushHeadMan,
      &RpnPushHeadId,
      &RpnPushHeadObid,
      &RpnPushHeadTypeCode,
      &RpnPushHeadDegree,
      &RpnPushHeadIndegree,
      &RpnPushHeadOutdegree,
      &RpnPushHeadTMC,
      &RpnPushHeadTMM,
      &RpnPushHeadTMX,
      &RpnPushHeadVector,
      &RpnPushHeadOperation,
      &RpnPushHeadRefc,
      &RpnPushHeadBidx,
      &RpnPushHeadOidx,
      &RpnPushHeadHandle,
      &RpnPushHeadEnum,
      &RpnPushHeadLocked,
      &RpnPushHeadAddress,
      &RpnPushHeadIndex,
      &RpnPushHeadBitIndex,
      &RpnPushHeadBitVector,
      &Rpn_PushHeadFancyProp,
      &Rpn_PushHeadPropCount,
      &Rpn_PushHeadCoeff1,
      &Rpn_PushHeadCoeff0,
      &Rpn_PushHeadMan,
      &Rpn_PushHeadId,
      &Rpn_PushHeadObid,
      &Rpn_PushHeadTypeCode,
      &Rpn_PushHeadDegree,
      &Rpn_PushHeadIndegree,
      &Rpn_PushHeadOutdegree,
      &Rpn_PushHeadTMC,
      &Rpn_PushHeadTMM,
      &Rpn_PushHeadTMX,
      &Rpn_PushHeadVector,
      &Rpn_PushHeadOperation,
      &Rpn_PushHeadRefc,
      &Rpn_PushHeadBidx,
      &Rpn_PushHeadOidx,
      &Rpn_PushHeadHandle,
      &Rpn_PushHeadEnum,
      &Rpn_PushHeadLocked,
      &Rpn_PushHeadAddress,
      &Rpn_PushHeadIndex,
      &Rpn_PushHeadBitIndex,
      &Rpn_PushHeadBitVector,
      // Context
      &RpnPushRank,
      &Rpn_PushRank,
      // System
      &RpnPushSysTick,
      &RpnPushSysUptime,
      // Random
      &RpnPushRandomInt,
      &RpnPushRandomBits,
      &RpnPushRandomReal,
      // Constant
      &RpnPushNull,
      &RpnPushNan,
      &RpnPushInf,
      &RpnPushConstantString,
      &RpnPushConstantInt,
      &RpnPushConstantBitvec,
      &RpnPushConstantReal,
      &RpnPushConstantTrue,
      &RpnPushConstantFalse,
      &RpnPushConstantIncept,
      &RpnPushConstantEpoch,
      &RpnPushConstantAge,
      &RpnPushConstantOrder,
      &RpnPushConstantSize,
      &RpnPushConstantOpcnt,
      &RpnPushConstantVector,
      &RpnPush_T_NEVER,
      &RpnPush_T_MAX,
      &RpnPush_T_MIN,
      &RpnPush_D_ANY,
      &RpnPush_D_IN,
      &RpnPush_D_OUT,
      &RpnPush_D_BOTH,
      &RpnPush_M_ANY,
      &RpnPush_M_STAT,
      &RpnPush_M_SIM,
      &RpnPush_M_DIST,
      &RpnPush_M_LSH,
      &RpnPush_M_INT,
      &RpnPush_M_UINT,
      &RpnPush_M_FLT,
      &RpnPush_M_CNT,
      &RpnPush_M_ACC,
      &RpnPush_M_TMC,
      &RpnPush_M_TMM,
      &RpnPush_M_TMX,
      &RpnPushConstantPi,
      &RpnPushConstantE,
      &RpnPushConstantRoot2,
      &RpnPushConstantRoot3,
      &RpnPushConstantRoot5,
      &RpnPushConstantPhi,
      &RpnPushConstantZeta3,
      &RpnPushConstantGoogol,

      &RpnPush_R1,
      &RpnPush_R2,
      &RpnPush_R3,
      &RpnPush_R4,
      &RpnPush_C1,
      &RpnPush_C2,
      &RpnPush_C3,
      &RpnPush_C4,

      &RpnPushReg1,
      &RpnPushReg2,
      &RpnPushReg3,
      &RpnPushReg4,

      &RpnPushMemX,

      &RpnPushEnumRelEnc,
      &RpnPushEnumVtxType,
      // Unary
      &RpnUnaryNot,
      &RpnUnaryBitwiseNot,
      &RpnUnaryCastInt,
      &RpnUnaryCastIntR,
      &RpnUnaryCastAsInt,
      &RpnUnaryCastAsBits,
      &RpnUnaryCastReal,
      &RpnUnaryCastAsReal,
      &RpnUnaryCastStr,
      &RpnUnaryCastBitvector,
      &RpnVariadicBytes,
      &RpnUnaryCastKeyVal,
      &RpnUnaryHash,

      // String
      &RpnStringStrcmp,
      &RpnStringStrcasecmp,
      &RpnStringStartswith,
      &RpnStringEndswith,
      &RpnStringNormalize,
      &RpnStringJoin,
      &RpnStringReplace,
      &RpnStringSlice,
      &RpnStringPrefix,
      &RpnStringIndex,
      &RpnStringStrftime,

      &RpnUnaryNeg,
      &RpnUnaryIsNan,
      &RpnUnaryIsInf,
      &RpnUnaryIsInt,
      &RpnUnaryIsReal,
      &RpnUnaryIsBitvector,
      &RpnUnaryIsKeyVal,
      &RpnUnaryIsString,
      &RpnUnaryIsVector,
      &RpnUnaryIsBytearray,
      &RpnUnaryIsBytes,
      &RpnUnaryIsUTF8,
      &RpnUnaryIsArray,
      &RpnUnaryIsMap,
      &RpnAnyNan,
      &RpnAllNan,
      &RpnUnaryInv,
      &RpnUnaryLog2,
      &RpnUnaryLog,
      &RpnUnaryLog10,
      &RpnUnaryRad,
      &RpnUnaryDeg,
      &RpnUnarySin,
      &RpnUnaryCos,
      &RpnUnaryTan,
      &RpnUnaryASin,
      &RpnUnaryACos,
      &RpnUnaryATan,
      &RpnUnarySinh,
      &RpnUnaryCosh,
      &RpnUnaryTanh,
      &RpnUnaryASinh,
      &RpnUnaryACosh,
      &RpnUnaryATanh,
      &RpnUnarySinc,
      &RpnUnaryExp,
      &RpnUnaryAbs,
      &RpnUnarySqrt,
      &RpnUnaryCeil,
      &RpnUnaryFloor,
      &RpnUnaryRound,
      &RpnUnarySign,
      &RpnUnaryFactorial,
      &RpnUnaryPopcnt,
      // Binary
      &RpnBinaryAdd,
      &RpnBinarySub,
      &RpnBinaryMul,
      &RpnBinaryDiv,
      &RpnBinaryMod,
      &RpnBinaryPow,
      &RpnBinaryATan2,
      &RpnBinaryMax,
      &RpnBinaryMin,
      &RpnBinaryProx,
      &RpnBinaryApprox,
      &RpnBinaryRange,
      &RpnBinaryHamDist,
      &RpnBinaryEuclidean,
      &RpnBinarySimilarity,
      &RpnBinaryCosine,
      &RpnBinaryJaccard,
      &RpnBinaryComb,
      &RpnBinaryEqu,
      &RpnBinaryNeq,
      &RpnBinaryGt,
      &RpnBinaryGte,
      &RpnBinaryLt,
      &RpnBinaryLte,
      &RpnBinaryElementOf,
      &RpnBinaryNotElementOf,
      // Logical
      &RpnLogicalOr,
      &RpnLogicalAnd,
      // Ternary
      &RpnTernaryCondition,
      &RpnTernaryColon,
      // Quaternary
      &RpnQuaternaryHavDist,
      &RpnQuaternaryGeoProx,
      // Bitwise
      &RpnBitwiseShiftLeft,
      &RpnBitwiseShiftRight,
      &RpnBitwiseOr,
      &RpnBitwiseAnd,
      &RpnBitwiseXor,
      // Assignment
      &RpnAssign,
      &RpnAssignVariable,
      // Enum
      &RpnEnumRelEnc,
      &RpnEnumTypeEnc,
      &RpnEnumRelDec,
      &RpnEnumTypeDec,
      &RpnEnumModToStr,
      &RpnEnumDirToStr,

      // Memory single register
      &RpnMemoryStore,
      &RpnMemoryRefStore,
      &RpnMemoryStoreIf,
      &RpnMemoryRefStoreIf,
      &RpnMemoryWrite,
      &RpnMemoryRWrite,
      &RpnMemoryWriteIf,
      &RpnMemoryRWriteIf,

      &RpnMemoryLoad,
      &RpnMemoryRefLoad,

      &RpnMemoryPush,
      &RpnMemoryPushIf,
      &RpnMemoryPop,
      &RpnMemoryPopIf,
      &RpnMemoryGet,

      &RpnMemoryMov,
      &RpnMemoryRefMov,
      &RpnMemoryMovIf,
      &RpnMemoryRefMovIf,

      &RpnMemoryXchg,
      &RpnMemoryRefXchg,
      &RpnMemoryXchgIf,
      &RpnMemoryRefXchgIf,

      &RpnMemoryInc,
      &RpnMemoryRefInc,
      &RpnMemoryIncIf,
      &RpnMemoryRefIncIf,
      &RpnMemoryDec,
      &RpnMemoryRefDec,
      &RpnMemoryDecIf,
      &RpnMemoryRefDecIf,

      &RpnMemoryEqu,
      &RpnMemoryRefEqu,
      &RpnMemoryNeq,
      &RpnMemoryRefNeq,

      &RpnMemoryGt,
      &RpnMemoryRefGt,
      &RpnMemoryGte,
      &RpnMemoryRefGte,

      &RpnMemoryLt,
      &RpnMemoryRefLt,
      &RpnMemoryLte,
      &RpnMemoryRefLte,

      &RpnMemoryAdd,
      &RpnMemoryAddIf,
      &RpnMemorySub,
      &RpnMemorySubIf,
      &RpnMemoryMul,
      &RpnMemoryMulIf,
      &RpnMemoryDiv,
      &RpnMemoryDivIf,
      &RpnMemoryMod,
      &RpnMemoryModIf,

      &RpnMemoryShr,
      &RpnMemoryShrIf,
      &RpnMemoryShl,
      &RpnMemoryShlIf,
      &RpnMemoryAnd,
      &RpnMemoryAndIf,
      &RpnMemoryOr,
      &RpnMemoryOrIf,
      &RpnMemoryXor,
      &RpnMemoryXorIf,

      &RpnMemorySmooth,
      &RpnMemoryCount,
      &RpnMemoryCountIf,

      &RpnMemory_ecld_pi8_512,
      &RpnMemory_ssq_pi8_512,
      &RpnMemory_rsqrtssq_pi8_512,
      &RpnMemory_dp_pi8_512,
      &RpnMemory_cos_pi8_512,

      &RpnMemory_ecld_pi8_256,
      &RpnMemory_ssq_pi8_256,
      &RpnMemory_rsqrtssq_pi8_256,
      &RpnMemory_dp_pi8_256,
      &RpnMemory_cos_pi8_256,

      &RpnMemory_ecld_pi8,
      &RpnMemory_ssq_pi8,
      &RpnMemory_dp_pi8,
      &RpnMemory_cos_pi8,
      &RpnMemory_ham_pi8,

      &RpnMemoryModIndex,
      &RpnMemoryIndex,
      &RpnMemoryIndexed,
      &RpnMemoryUnindex,


      // Memory multi register
      &RpnMemoryMSet,
      &RpnMemoryMReset,
      &RpnMemoryMRandomize,
      &RpnMemoryMRandbits,
      &RpnMemoryMCopy,
      &RpnMemoryMPWrite,
      &RpnMemoryMCopyObj,

      &RpnMemoryMTerm,
      &RpnMemoryMLen,

      &RpnMemoryHeapInit,
      &RpnMemoryHeapPushMin,
      &RpnMemoryHeapPushMax,
      &RpnMemoryHeapWriteMin,
      &RpnMemoryHeapWriteMax,
      &RpnMemoryHeapifyMin,
      &RpnMemoryHeapifyMax,

      &RpnMemoryHeapSiftMin,
      &RpnMemoryHeapSiftMax,

      &RpnMemoryMSort,
      &RpnMemoryMSortRev,
      &RpnMemoryMRSort,
      &RpnMemoryMRSortRev,
      &RpnMemoryMReverse,

      &RpnMemoryMInt,
      &RpnMemoryMIntR,
      &RpnMemoryMReal,
      &RpnMemoryMBits,

      &RpnMemoryMInc,
      &RpnMemoryMIInc,
      &RpnMemoryMRInc,
      &RpnMemoryMDec,
      &RpnMemoryMIDec,
      &RpnMemoryMRDec,

      &RpnMemoryMAdd,
      &RpnMemoryMIAdd,
      &RpnMemoryMRAdd,
      &RpnMemoryMVAdd,

      &RpnMemoryMSub,
      &RpnMemoryMISub,
      &RpnMemoryMRSub,
      &RpnMemoryMVSub,

      &RpnMemoryMMul,
      &RpnMemoryMIMul,
      &RpnMemoryMRMul,
      &RpnMemoryMVMul,

      &RpnMemoryMDiv,
      &RpnMemoryMIDiv,
      &RpnMemoryMRDiv,
      &RpnMemoryMVDiv,

      &RpnMemoryMMod,
      &RpnMemoryMIMod,
      &RpnMemoryMRMod,
      &RpnMemoryMVMod,

      &RpnMemoryMInv,
      &RpnMemoryMRInv,

      &RpnMemoryMPow,
      &RpnMemoryMRPow,
      &RpnMemoryMSq,
      &RpnMemoryMRSq,
      &RpnMemoryMSqrt,
      &RpnMemoryMRSqrt,

      &RpnMemoryMCeil,
      &RpnMemoryMRCeil,
      &RpnMemoryMFloor,
      &RpnMemoryMRFloor,
      &RpnMemoryMRound,
      &RpnMemoryMRRound,
      &RpnMemoryMAbs,
      &RpnMemoryMRAbs,
      &RpnMemoryMSign,
      &RpnMemoryMRSign,

      &RpnMemoryMLog2,
      &RpnMemoryMRLog2,
      &RpnMemoryMLog,
      &RpnMemoryMRLog,
      &RpnMemoryMLog10,
      &RpnMemoryMRLog10,
      &RpnMemoryMExp2,
      &RpnMemoryMRExp2,
      &RpnMemoryMExp,
      &RpnMemoryMRExp,
      &RpnMemoryMExp10,
      &RpnMemoryMRExp10,


      &RpnMemoryMRad,
      &RpnMemoryMRRad,
      &RpnMemoryMDeg,
      &RpnMemoryMRDeg,
      &RpnMemoryMSin,
      &RpnMemoryMRSin,
      &RpnMemoryMCos,
      &RpnMemoryMRCos,
      &RpnMemoryMTan,
      &RpnMemoryMRTan,
      &RpnMemoryMASin,
      &RpnMemoryMRASin,
      &RpnMemoryMACos,
      &RpnMemoryMRACos,
      &RpnMemoryMATan,
      &RpnMemoryMRATan,
      &RpnMemoryMSinh,
      &RpnMemoryMRSinh,
      &RpnMemoryMCosh,
      &RpnMemoryMRCosh,
      &RpnMemoryMTanh,
      &RpnMemoryMRTanh,
      &RpnMemoryMASinh,
      &RpnMemoryMRASinh,
      &RpnMemoryMACosh,
      &RpnMemoryMRACosh,
      &RpnMemoryMATanh,
      &RpnMemoryMRATanh,
      &RpnMemoryMSinc,
      &RpnMemoryMRSinc,

      &RpnMemoryMShr,
      &RpnMemoryMVShr,
      &RpnMemoryMShl,
      &RpnMemoryMVShl,

      &RpnMemoryMAnd,
      &RpnMemoryMVAnd,
      &RpnMemoryMOr,
      &RpnMemoryMVOr,
      &RpnMemoryMXor,
      &RpnMemoryMVXor,
      &RpnMemoryMPopcnt,

      &RpnMemoryMHash,

      &RpnMemoryMSum,
      &RpnMemoryMRSum,
      &RpnMemoryMSumSqr,
      &RpnMemoryMRSumSqr,
      &RpnMemoryMInvSum,
      &RpnMemoryMRInvSum,
      &RpnMemoryMProd,
      &RpnMemoryMRProd,
      &RpnMemoryMMean,
      &RpnMemoryMRMean,
      &RpnMemoryMHarmMean,
      &RpnMemoryMRHarmMean,
      &RpnMemoryMGeoMean,
      &RpnMemoryMRGeoMean,
      &RpnMemoryMStdev,
      &RpnMemoryMRStdev,
      &RpnMemoryMGeoStdev,
      &RpnMemoryMRGeoStdev,

      &RpnMemoryMMax,
      &RpnMemoryMMin,
      &RpnMemoryMContains,
      &RpnMemoryMCount,
      &RpnMemoryMIndex,
      &RpnMemoryMCmp,
      &RpnMemoryMCmpA,
      &RpnMemoryMSubset,
      &RpnMemoryMSubsetObj,
      &RpnMemoryMSumProdObj,
      &RpnMemoryProbeArray,
      &RpnMemoryProbeAltArr,
      &RpnMemoryProbeSuperArr,

      // QWORD Set
      &RpnISetAdd,
      &RpnISetDel,
      &RpnISetClr,
      &RpnISetHas,
      &RpnISetLen,
      &RpnISetIni,

      // Vertex Set
      &RpnVSetAdd,
      &RpnVSetDel,
      &RpnVSetClr,
      &RpnVSetHas,
      &RpnVSetLen,
      &RpnVSetIni,

      // Object
      &RpnObjectLen,
      &RpnObjectStrlen,

      // Variadic
      &RpnVarFirstValue,
      &RpnVarLastValue,
      &RpnVarRank,
      &RpnVarGeoRank,
      &RpnVarSum,
      &RpnVarSumSquare,
      &RpnVarStdev,
      &RpnVarInvSum,
      &RpnVarProduct,
      &RpnVarMean,
      &RpnVarHarmonicMean,
      &RpnVarGeometricMean,
      &RpnVarDo,
      &RpnVarVoid,

      // Cull
      &RpnMCull,
      &RpnMCullIf,

      // Collectable
      &RpnPushCollectableReal,
      &RpnPushCollectableInt,

      // Collect
      &RpnStage,
      &RpnStageIf,
      &RpnUnstage,
      &RpnUnstageIf,
      &RpnCommit,
      &RpnCommitIf,
      &RpnCollect,
      &RpnCollectIf,

      NULL
    };



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static express_eval_map * __rpndef__opmap_new( const char *name ) {
  express_eval_map *rpn_operations = NULL;
  if( (rpn_operations = calloc( 1, sizeof( express_eval_map ) )) != NULL ) {
    if( (rpn_operations->map = iMapping.NewIntegerMap( &rpn_operations->dyn, name )) != NULL ) {
#define ADD_RPN_OPERATION( Name, RpnOperation ) iMapping.IntegerMapAdd( &rpn_operations->map, &rpn_operations->dyn, Name, (intptr_t)(RpnOperation) )
      __rpn_operation **cursor = __rpn_definitions;
      __rpn_operation *op;
      int64_t sz = 0;
      while( (op = *cursor++) != NULL ) {
        if( op->surface.token ) {
          ADD_RPN_OPERATION( op->surface.token, op );
          sz++;
        }
      }
      // Check that all mappings exist
      if( iMapping.IntegerMapSize( rpn_operations->map ) == sz ) {
        rpn_operations->get = iMapping.IntegerMapGet;
      }
      // Collisions occurred, we failed
      else {
        __rpndef__opmap_delete( &rpn_operations );
      }
    }
  }

  return rpn_operations;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __rpndef__opmap_delete( express_eval_map **operators ) {
  if( operators && *operators ) {
    if( (*operators)->map ) {
      iMapping.DeleteIntegerMap( &(*operators)->map, &(*operators)->dyn );
    }
    free( *operators );
    *operators = NULL;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static __rpn_operation * __rpndef__opmap_get( express_eval_map *operations, const char *name ) {
  int64_t opaddr;
  if( operations->get( operations->map, &operations->dyn, name, &opaddr ) ) {
    return (__rpn_operation*)opaddr;
  }
  return NULL;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static int __rpndef__is_property_lookup( const __rpn_operation *op ) {
  f_evaluator f = op->function.eval;
  return f == RpnPushHeadFancyProp.function.eval  ||
         f == Rpn_PushHeadFancyProp.function.eval ||
         f == RpnPushThisFancyProp.function.eval  ||
         f == RpnPushTailFancyProp.function.eval;
}









#endif
