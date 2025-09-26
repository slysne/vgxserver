/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _rpnvertex.h
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

#ifndef _VGX_VXEVAL_RPNDEF_RPNVERTEX_H
#define _VGX_VXEVAL_RPNDEF_RPNVERTEX_H


// Tail Vertex
static __rpn_operation RpnPushTail           = { .surface.token="prev",             .function.eval = __stack_push_TAIL,               .type = OP_TAIL_VERTEX_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailProperty   = { .surface.token=NULL,               .function.eval = __stack_push_TAIL_prop,          .type = OP_SUBSCRIPT,             .precedence = OPP_SUBSCRIPT };
static __rpn_operation RpnPushTailFancyProp  = { .surface.token="prev.property",    .function.eval = __stack_push_TAIL_fancyprop,     .type = ENCODE_VARIADIC_ARG_COUNTS( OP_VARIADIC_TAIL_PREFIX, 1, 3 ), .precedence = OPP_CALL };
static __rpn_operation RpnPushTailPropCount  = { .surface.token="prev.propcount",   .function.eval = __stack_push_TAIL_propcount,     .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailCoeff1     = { .surface.token="prev.c1",          .function.eval = __stack_push_TAIL_c1,            .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailCoeff0     = { .surface.token="prev.c0",          .function.eval = __stack_push_TAIL_c0,            .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailMan        = { .surface.token="prev.virtual",     .function.eval = __stack_push_TAIL_virtual,       .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailId         = { .surface.token="prev.id",          .function.eval = __stack_push_TAIL_id,            .type = OP_TAIL_VERTEXID_OBJECT,  .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailObid       = { .surface.token="prev.internalid",  .function.eval = __stack_push_TAIL_obid,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailTypeCode   = { .surface.token="prev.type",        .function.eval = __stack_push_TAIL_typ,           .type = OP_TAIL_TYPE_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailDegree     = { .surface.token="prev.deg",         .function.eval = __stack_push_TAIL_deg,           .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailIndegree   = { .surface.token="prev.ideg",        .function.eval = __stack_push_TAIL_ideg,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailOutdegree  = { .surface.token="prev.odeg",        .function.eval = __stack_push_TAIL_odeg,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailTMC        = { .surface.token="prev.tmc",         .function.eval = __stack_push_TAIL_tmc,           .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailTMM        = { .surface.token="prev.tmm",         .function.eval = __stack_push_TAIL_tmm,           .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailTMX        = { .surface.token="prev.tmx",         .function.eval = __stack_push_TAIL_tmx,           .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailVector     = { .surface.token="prev.vector",      .function.eval = __stack_push_TAIL_vec,           .type = OP_TAIL_VECTOR_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailOperation  = { .surface.token="prev.op",          .function.eval = __stack_push_TAIL_op,            .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailRefc       = { .surface.token="prev.refc",        .function.eval = __stack_push_TAIL_refc,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailBidx       = { .surface.token="prev.bidx",        .function.eval = __stack_push_TAIL_bidx,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailOidx       = { .surface.token="prev.oidx",        .function.eval = __stack_push_TAIL_oidx,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailHandle     = { .surface.token="prev.handle",      .function.eval = __stack_push_TAIL_handle,        .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailEnum       = { .surface.token="prev.enum",        .function.eval = __stack_push_TAIL_enum,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailLocked     = { .surface.token="prev.locked",      .function.eval = __stack_push_TAIL_locked,        .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailAddress    = { .surface.token="prev.address",     .function.eval = __stack_push_TAIL_addr,          .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailIndex      = { .surface.token="prev.index",       .function.eval = __stack_push_TAIL_index,         .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailBitIndex   = { .surface.token="prev.bitindex",    .function.eval = __stack_push_TAIL_bitindex,      .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushTailBitVector  = { .surface.token="prev.bitvector",   .function.eval = __stack_push_TAIL_bitvector,     .type = OP_TAIL_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE }; 

// This Vertex
static __rpn_operation RpnPushThis           = { .surface.token="vertex",           .function.eval = __stack_push_VERTEX,             .type = OP_THIS_VERTEX_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisProperty   = { .surface.token=NULL,               .function.eval = __stack_push_VERTEX_prop,        .type = OP_SUBSCRIPT,             .precedence = OPP_SUBSCRIPT };
static __rpn_operation RpnPushThisFancyProp  = { .surface.token="vertex.property",  .function.eval = __stack_push_VERTEX_fancyprop,   .type = ENCODE_VARIADIC_ARG_COUNTS( OP_VARIADIC_PREFIX, 1, 3 ), .precedence = OPP_CALL };
static __rpn_operation RpnPushThisPropCount  = { .surface.token="vertex.propcount", .function.eval = __stack_push_VERTEX_propcount,   .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisCoeff1     = { .surface.token="vertex.c1",        .function.eval = __stack_push_VERTEX_c1,          .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisCoeff0     = { .surface.token="vertex.c0",        .function.eval = __stack_push_VERTEX_c0,          .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisMan        = { .surface.token="vertex.virtual",   .function.eval = __stack_push_VERTEX_virtual,     .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisId         = { .surface.token="vertex.id",        .function.eval = __stack_push_VERTEX_id,          .type = OP_THIS_VERTEXID_OBJECT,  .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisObid       = { .surface.token="vertex.internalid",.function.eval = __stack_push_VERTEX_obid,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisTypeCode   = { .surface.token="vertex.type",      .function.eval = __stack_push_VERTEX_typ,         .type = OP_THIS_TYPE_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisDegree     = { .surface.token="vertex.deg",       .function.eval = __stack_push_VERTEX_deg,         .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisIndegree   = { .surface.token="vertex.ideg",      .function.eval = __stack_push_VERTEX_ideg,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisOutdegree  = { .surface.token="vertex.odeg",      .function.eval = __stack_push_VERTEX_odeg,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisTMC        = { .surface.token="vertex.tmc",       .function.eval = __stack_push_VERTEX_tmc,         .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisTMM        = { .surface.token="vertex.tmm",       .function.eval = __stack_push_VERTEX_tmm,         .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisTMX        = { .surface.token="vertex.tmx",       .function.eval = __stack_push_VERTEX_tmx,         .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisVector     = { .surface.token="vertex.vector",    .function.eval = __stack_push_VERTEX_vec,         .type = OP_THIS_VECTOR_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisOperation  = { .surface.token="vertex.op",        .function.eval = __stack_push_VERTEX_op,          .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisRefc       = { .surface.token="vertex.refc",      .function.eval = __stack_push_VERTEX_refc,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisBidx       = { .surface.token="vertex.bidx",      .function.eval = __stack_push_VERTEX_bidx,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisOidx       = { .surface.token="vertex.oidx",      .function.eval = __stack_push_VERTEX_oidx,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisHandle     = { .surface.token="vertex.handle",    .function.eval = __stack_push_VERTEX_handle,      .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisEnum       = { .surface.token="vertex.enum",      .function.eval = __stack_push_VERTEX_enum,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisLocked     = { .surface.token="vertex.locked",    .function.eval = __stack_push_VERTEX_locked,      .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisAddress    = { .surface.token="vertex.address",   .function.eval = __stack_push_VERTEX_addr,        .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisIndex      = { .surface.token="vertex.index",     .function.eval = __stack_push_VERTEX_index,       .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisBitIndex   = { .surface.token="vertex.bitindex",  .function.eval = __stack_push_VERTEX_bitindex,    .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushThisBitVector  = { .surface.token="vertex.bitvector", .function.eval = __stack_push_VERTEX_bitvector,   .type = OP_THIS_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };

// Head Vertex
static __rpn_operation RpnPushHead           = { .surface.token="next",             .function.eval = __stack_push_HEAD,               .type = OP_HEAD_VERTEX_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadProperty   = { .surface.token=NULL,               .function.eval = __stack_push_HEAD_prop,          .type = OP_HEAD_SUBSCRIPT,        .precedence = OPP_SUBSCRIPT };
static __rpn_operation RpnPushHeadFancyProp  = { .surface.token="next.property",    .function.eval = __stack_push_HEAD_fancyprop,     .type = ENCODE_VARIADIC_ARG_COUNTS( OP_VARIADIC_HEAD_PREFIX, 1, 3 ), .precedence = OPP_CALL };
static __rpn_operation RpnPushHeadPropCount  = { .surface.token="next.propcount",   .function.eval = __stack_push_HEAD_propcount,     .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadCoeff1     = { .surface.token="next.c1",          .function.eval = __stack_push_HEAD_c1,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadCoeff0     = { .surface.token="next.c0",          .function.eval = __stack_push_HEAD_c0,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadMan        = { .surface.token="next.virtual",     .function.eval = __stack_push_HEAD_virtual,       .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadId         = { .surface.token="next.id",          .function.eval = __stack_push_HEAD_id,            .type = OP_HEAD_VERTEXID_OBJECT,  .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadObid       = { .surface.token="next.internalid",  .function.eval = __stack_push_HEAD_obid,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadTypeCode   = { .surface.token="next.type",        .function.eval = __stack_push_HEAD_typ,           .type = OP_HEAD_TYPE_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadDegree     = { .surface.token="next.deg",         .function.eval = __stack_push_HEAD_deg,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadIndegree   = { .surface.token="next.ideg",        .function.eval = __stack_push_HEAD_ideg,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadOutdegree  = { .surface.token="next.odeg",        .function.eval = __stack_push_HEAD_odeg,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadTMC        = { .surface.token="next.tmc",         .function.eval = __stack_push_HEAD_tmc,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadTMM        = { .surface.token="next.tmm",         .function.eval = __stack_push_HEAD_tmm,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadTMX        = { .surface.token="next.tmx",         .function.eval = __stack_push_HEAD_tmx,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadVector     = { .surface.token="next.vector",      .function.eval = __stack_push_HEAD_vec,           .type = OP_HEAD_VECTOR_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadOperation  = { .surface.token="next.op",          .function.eval = __stack_push_HEAD_op,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadRefc       = { .surface.token="next.refc",        .function.eval = __stack_push_HEAD_refc,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadBidx       = { .surface.token="next.bidx",        .function.eval = __stack_push_HEAD_bidx,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadOidx       = { .surface.token="next.oidx",        .function.eval = __stack_push_HEAD_oidx,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadHandle     = { .surface.token="next.handle",      .function.eval = __stack_push_HEAD_handle,        .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadEnum       = { .surface.token="next.enum",        .function.eval = __stack_push_HEAD_enum,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation RpnPushHeadLocked     = { .surface.token="next.locked",      .function.eval = __stack_push_HEAD_locked,        .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation RpnPushHeadAddress    = { .surface.token="next.address",     .function.eval = __stack_push_HEAD_addr,          .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation RpnPushHeadIndex      = { .surface.token="next.index",       .function.eval = __stack_push_HEAD_index,         .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation RpnPushHeadBitIndex   = { .surface.token="next.bitindex",    .function.eval = __stack_push_HEAD_bitindex,      .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation RpnPushHeadBitVector  = { .surface.token="next.bitvector",   .function.eval = __stack_push_HEAD_bitvector,     .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation Rpn_PushHeadFancyProp = { .surface.token=".property",        .function.eval = __stack_push_HEAD_fancyprop,     .type = ENCODE_VARIADIC_ARG_COUNTS( OP_VARIADIC_HEAD_PREFIX, 1, 3 ), .precedence = OPP_CALL };
static __rpn_operation Rpn_PushHeadPropCount = { .surface.token=".propcount",       .function.eval = __stack_push_HEAD_propcount,     .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadCoeff1    = { .surface.token=".c1",              .function.eval = __stack_push_HEAD_c1,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadCoeff0    = { .surface.token=".c0",              .function.eval = __stack_push_HEAD_c0,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadMan       = { .surface.token=".virtual",         .function.eval = __stack_push_HEAD_virtual,       .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadId        = { .surface.token=".id",              .function.eval = __stack_push_HEAD_id,            .type = OP_HEAD_VERTEXID_OBJECT,  .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadObid      = { .surface.token=".internalid",      .function.eval = __stack_push_HEAD_obid,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadTypeCode  = { .surface.token=".type",            .function.eval = __stack_push_HEAD_typ,           .type = OP_HEAD_TYPE_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadDegree    = { .surface.token=".deg",             .function.eval = __stack_push_HEAD_deg,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadIndegree  = { .surface.token=".ideg",            .function.eval = __stack_push_HEAD_ideg,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadOutdegree = { .surface.token=".odeg",            .function.eval = __stack_push_HEAD_odeg,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadTMC       = { .surface.token=".tmc",             .function.eval = __stack_push_HEAD_tmc,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadTMM       = { .surface.token=".tmm",             .function.eval = __stack_push_HEAD_tmm,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadTMX       = { .surface.token=".tmx",             .function.eval = __stack_push_HEAD_tmx,           .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadVector    = { .surface.token=".vector",          .function.eval = __stack_push_HEAD_vec,           .type = OP_HEAD_VECTOR_OBJECT,    .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadOperation = { .surface.token=".op",              .function.eval = __stack_push_HEAD_op,            .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadRefc      = { .surface.token=".refc",            .function.eval = __stack_push_HEAD_refc,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadBidx      = { .surface.token=".bidx",            .function.eval = __stack_push_HEAD_bidx,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadOidx      = { .surface.token=".oidx",            .function.eval = __stack_push_HEAD_oidx,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadHandle    = { .surface.token=".handle",          .function.eval = __stack_push_HEAD_handle,        .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadEnum      = { .surface.token=".enum",            .function.eval = __stack_push_HEAD_enum,          .type = OP_HEAD_ATTR_OPERAND,     .precedence = OPP_ATTRIBUTE };
static __rpn_operation Rpn_PushHeadLocked    = { .surface.token=".locked",          .function.eval = __stack_push_HEAD_locked,        .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation Rpn_PushHeadAddress   = { .surface.token=".address",         .function.eval = __stack_push_HEAD_addr,          .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation Rpn_PushHeadIndex     = { .surface.token=".index",           .function.eval = __stack_push_HEAD_index,         .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation Rpn_PushHeadBitIndex  = { .surface.token=".bitindex",        .function.eval = __stack_push_HEAD_bitindex,      .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!
static __rpn_operation Rpn_PushHeadBitVector = { .surface.token=".bitvector",       .function.eval = __stack_push_HEAD_bitvector,     .type = OP_ARC_ATTR_OPERAND,      .precedence = OPP_ATTRIBUTE }; // no deref needed!



#endif
