/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _rpnconstant.h
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

#ifndef _VGX_VXEVAL_RPNDEF_RPNCONSTANT_H
#define _VGX_VXEVAL_RPNDEF_RPNCONSTANT_H

// Constant
static __rpn_operation RpnPushNone           = { .surface.token=NULL,               .function.eval = __stack_push_constant_none,      .type = OP_NONE_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushWild           = { .surface.token=NULL,               .function.eval = __stack_push_constant_wild,      .type = OP_NONE_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantString = { .surface.token=NULL,               .function.eval = __stack_push_constant_string,    .type = OP_ADDRESS_LITERAL,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantInt    = { .surface.token=NULL,               .function.eval = __stack_push_constant_integer,   .type = OP_INTEGER_LITERAL,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantBitvec = { .surface.token=NULL,               .function.eval = __stack_push_constant_bitvector, .type = OP_INTEGER_LITERAL,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantReal   = { .surface.token=NULL,               .function.eval = __stack_push_constant_real,      .type = OP_REAL_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushNull           = { .surface.token="null",             .function.eval = __stack_push_constant_none,      .type = OP_NONE_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushNan            = { .surface.token="nan",              .function.eval = __stack_push_constant_nan,       .type = OP_NONE_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushInf            = { .surface.token="inf",              .function.eval = __stack_push_constant_inf,       .type = OP_NONE_LITERAL,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantTrue   = { .surface.token="true",             .function.eval = __stack_push_constant_true,      .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantFalse  = { .surface.token="false",            .function.eval = __stack_push_constant_false,     .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantIncept = { .surface.token="graph.t0",         .function.eval = __stack_push_constant_inception, .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantEpoch  = { .surface.token="graph.ts",         .function.eval = __stack_push_constant_epoch,     .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantAge    = { .surface.token="graph.age",        .function.eval = __stack_push_constant_age,       .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantOrder  = { .surface.token="graph.order",      .function.eval = __stack_push_constant_order,     .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantSize   = { .surface.token="graph.size",       .function.eval = __stack_push_constant_size,      .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantOpcnt  = { .surface.token="graph.op",         .function.eval = __stack_push_constant_opcnt,     .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantVector = { .surface.token="vector",           .function.eval = __stack_push_constant_vector,    .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_T_NEVER       = { .surface.token="T_NEVER",          .function.eval = __stack_push_constant_T_NEVER,   .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_T_MAX         = { .surface.token="T_MAX",            .function.eval = __stack_push_constant_T_MAX,     .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_T_MIN         = { .surface.token="T_MIN",            .function.eval = __stack_push_constant_T_MIN,     .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_D_ANY         = { .surface.token="D_ANY",            .function.eval = __stack_push_constant_D_ANY,     .type = OP_ARC_DIR_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_D_IN          = { .surface.token="D_IN",             .function.eval = __stack_push_constant_D_IN,      .type = OP_ARC_DIR_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_D_OUT         = { .surface.token="D_OUT",            .function.eval = __stack_push_constant_D_OUT,     .type = OP_ARC_DIR_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_D_BOTH        = { .surface.token="D_BOTH",           .function.eval = __stack_push_constant_D_BOTH,    .type = OP_ARC_DIR_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_ANY         = { .surface.token="M_ANY",            .function.eval = __stack_push_constant_M_ANY,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_STAT        = { .surface.token="M_STAT",           .function.eval = __stack_push_constant_M_STAT,    .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_SIM         = { .surface.token="M_SIM",            .function.eval = __stack_push_constant_M_SIM,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_DIST        = { .surface.token="M_DIST",           .function.eval = __stack_push_constant_M_DIST,    .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_LSH         = { .surface.token="M_LSH",            .function.eval = __stack_push_constant_M_LSH,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_INT         = { .surface.token="M_INT",            .function.eval = __stack_push_constant_M_INT,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_UINT        = { .surface.token="M_UINT",           .function.eval = __stack_push_constant_M_UINT,    .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_FLT         = { .surface.token="M_FLT",            .function.eval = __stack_push_constant_M_FLT,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_CNT         = { .surface.token="M_CNT",            .function.eval = __stack_push_constant_M_CNT,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_ACC         = { .surface.token="M_ACC",            .function.eval = __stack_push_constant_M_ACC,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_TMC         = { .surface.token="M_TMC",            .function.eval = __stack_push_constant_M_TMC,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_TMM         = { .surface.token="M_TMM",            .function.eval = __stack_push_constant_M_TMM,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_M_TMX         = { .surface.token="M_TMX",            .function.eval = __stack_push_constant_M_TMX,     .type = OP_ARC_MOD_ENUM_OPERAND,  .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantPi     = { .surface.token="pi",               .function.eval = __stack_push_constant_pi,        .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantE      = { .surface.token="e",                .function.eval = __stack_push_constant_e,         .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantRoot2  = { .surface.token="root2",            .function.eval = __stack_push_constant_root2,     .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantRoot3  = { .surface.token="root3",            .function.eval = __stack_push_constant_root3,     .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantRoot5  = { .surface.token="root5",            .function.eval = __stack_push_constant_root5,     .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantPhi    = { .surface.token="phi",              .function.eval = __stack_push_constant_phi,       .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantZeta3  = { .surface.token="zeta3",            .function.eval = __stack_push_constant_zeta3,     .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };
static __rpn_operation RpnPushConstantGoogol = { .surface.token="googol",           .function.eval = __stack_push_constant_googol,    .type = OP_REAL_OPERAND,          .precedence = OPP_CONSTANT };

static __rpn_operation RpnPush_R1            = { .surface.token="R1",               .function.eval = __stack_push_constant_R1,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_R2            = { .surface.token="R2",               .function.eval = __stack_push_constant_R2,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_R3            = { .surface.token="R3",               .function.eval = __stack_push_constant_R3,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_R4            = { .surface.token="R4",               .function.eval = __stack_push_constant_R4,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };

static __rpn_operation RpnPush_C1            = { .surface.token="C1",               .function.eval = __stack_push_constant_C1,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_C2            = { .surface.token="C2",               .function.eval = __stack_push_constant_C2,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_C3            = { .surface.token="C3",               .function.eval = __stack_push_constant_C3,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };
static __rpn_operation RpnPush_C4            = { .surface.token="C4",               .function.eval = __stack_push_constant_C4,        .type = OP_INTEGER_OPERAND,       .precedence = OPP_CONSTANT };


#endif
