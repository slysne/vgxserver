/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _rpnarc.h
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

#ifndef _VGX_VXEVAL_RPNDEF_RPNARC_H
#define _VGX_VXEVAL_RPNDEF_RPNARC_H


// Previous Arc (from tail to this)
static __rpn_operation RpnPushPrevValue      = { .surface.token="prev.arc.value",   .function.eval = __stack_push_arrive_value,       .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevDistance   = { .surface.token="prev.arc.dist",    .function.eval = __stack_push_arrive_distance,    .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevRelcode    = { .surface.token="prev.arc.type",    .function.eval = __stack_push_arrive_relcode,     .type = OP_TAIL_REL_OPERAND,      .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevDirection  = { .surface.token="prev.arc.dir",     .function.eval = __stack_push_arrive_direction,   .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevModifier   = { .surface.token="prev.arc.mod",     .function.eval = __stack_push_arrive_modifier,    .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevIsFwdOnly  = { .surface.token="prev.arc.isfwd",   .function.eval = __stack_push_arrive_isfwdonly,   .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevIsSyn      = { .surface.token="prev.arc.issyn",   .function.eval = __stack_push_arrive_issyn,       .type = OP_TAIL_ATTR_OPERAND |__OP_META_SYNARC, .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushPrevRaw        = { .surface.token="prev.arc",         .function.eval = __stack_push_arrive_raw,         .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };


// Next Arc (from this to head)
static __rpn_operation RpnPushNextValue      = { .surface.token="next.arc.value",   .function.eval = __stack_push_exit_value,         .type = OP_ARC_ATTR_OPERAND,       .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextDistance   = { .surface.token="next.arc.dist",    .function.eval = __stack_push_exit_distance,      .type = OP_ARC_ATTR_OPERAND,       .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextRelcode    = { .surface.token="next.arc.type",    .function.eval = __stack_push_exit_relcode,       .type = OP_ARC_REL_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextDirection  = { .surface.token="next.arc.dir",     .function.eval = __stack_push_exit_direction,     .type = OP_ARC_DIR_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextModifier   = { .surface.token="next.arc.mod",     .function.eval = __stack_push_exit_modifier,      .type = OP_ARC_MOD_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextIsFwdOnly  = { .surface.token="next.arc.isfwd",   .function.eval = __stack_push_exit_isfwdonly,     .type = OP_ARC_MOD_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextIsSyn      = { .surface.token="next.arc.issyn",   .function.eval = __stack_push_exit_issyn,         .type = OP_ARC_MOD_OPERAND |__OP_META_SYNARC, .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushNextRaw        = { .surface.token="next.arc",         .function.eval = __stack_push_exit_raw,           .type = OP_ARC_OPERAND,            .precedence = OPP_ATTRIBUTE };


static __rpn_operation Rpn_PushNextValue     = { .surface.token=".arc.value",       .function.eval = __stack_push_exit_value,         .type = OP_ARC_ATTR_OPERAND,       .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextDistance  = { .surface.token=".arc.dist",        .function.eval = __stack_push_exit_distance,      .type = OP_ARC_ATTR_OPERAND,       .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextRelcode   = { .surface.token=".arc.type",        .function.eval = __stack_push_exit_relcode,       .type = OP_ARC_REL_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextDirection = { .surface.token=".arc.dir",         .function.eval = __stack_push_exit_direction,     .type = OP_ARC_DIR_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextModifier  = { .surface.token=".arc.mod",         .function.eval = __stack_push_exit_modifier,      .type = OP_ARC_MOD_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextIsFwdOnly = { .surface.token=".arc.isfwd",       .function.eval = __stack_push_exit_isfwdonly,     .type = OP_ARC_MOD_OPERAND,        .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextIsSyn     = { .surface.token=".arc.issyn",       .function.eval = __stack_push_exit_issyn,         .type = OP_ARC_MOD_OPERAND |__OP_META_SYNARC, .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushNextRaw       = { .surface.token=".arc",             .function.eval = __stack_push_exit_raw,           .type = OP_ARC_OPERAND,            .precedence = OPP_ATTRIBUTE };

// Synthetic Arc Processing
// Arc Decay
static __rpn_operation RpnEvalSynArcDecay    = { .surface.token="synarc.decay",     .function.eval = __eval_synarc_decay,             .type = ENCODE_VARIADIC_ARG_COUNTS( (OP_VARIADIC_PREFIX | __OP_META_WORKSLOT | __OP_META_SYNARC), 1, 2 ), .precedence = OPP_CALL };
static __rpn_operation RpnEvalSynArcXDecay   = { .surface.token="synarc.xdecay",    .function.eval = __eval_synarc_xdecay,            .type = ENCODE_VARIADIC_ARG_COUNTS( (OP_VARIADIC_PREFIX | __OP_META_WORKSLOT | __OP_META_SYNARC), 1, 2 ), .precedence = OPP_CALL };
// Multiple Arc Matching
static __rpn_operation RpnEvalSynArcHasRel   = { .surface.token="synarc.hasrel",    .function.eval = __eval_synarc_hasrel,            .type = OP_VARIADIC_PREFIX | __OP_META_WORKSLOT |__OP_META_SYNARC,        .precedence = OPP_CALL };
static __rpn_operation RpnEvalSynArcHasMod   = { .surface.token="synarc.hasmod",    .function.eval = __eval_synarc_hasmod,            .type = OP_VARIADIC_PREFIX | __OP_META_WORKSLOT |__OP_META_SYNARC,        .precedence = OPP_CALL };
static __rpn_operation RpnEvalSynArcHasRelMod= { .surface.token="synarc.hasrelmod", .function.eval = __eval_synarc_hasrelmod,         .type = OP_VARIADIC_PREFIX | __OP_META_WORKSLOT |__OP_META_SYNARC,        .precedence = OPP_CALL };
static __rpn_operation RpnEvalSynArcValue    = { .surface.token="synarc.value",     .function.eval = __eval_synarc_arcvalue,          .type = OP_BINARY_PREFIX | __OP_META_WORKSLOT |__OP_META_SYNARC,        .precedence = OPP_CALL };



#endif
