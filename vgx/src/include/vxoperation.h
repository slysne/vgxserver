/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxoperation.h
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

#ifndef VXOPERATION_H
#define VXOPERATION_H

#include "vxbase.h"


union u_vgx_OperatorCapture_t;
union u_vgx_OperationParser_t;
struct s_vgx_OperationTransaction_t;
struct s_vgx_TransactionalProducer_t;
struct s_vgx_TransactionalProducers_t;


#define GET_OPCODE( Type, Target, Scope, Action )  ( ( (Type) << 8 ) | (Target) | (Scope) | (Action) )


/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorOpCode {

  OPCODE_INVALID                  = 0xFFFFFFFF,
/* -----------------------------------^^^^^^^^
                                      ||\  /||
                                      || \/ |`- (C)reate, (D)elete, (A)ssign, (E)vent, (5)State
                                      ||  | `-- (1): affects single object, (F): affects one or more objects
                                      ||  `---- (xxxx): operation type
                                      |`------- (0): valid operation, (other): invalid operation
                                      `-------- (1): introduce something, (0): take something away

*/

  /*                                  A0TxxxMA*/
  __OPCODE_MASK__ALL              = 0x1FFFFFFF,
  __OPCODE_MASK__ACTION           = 0x0000000F,
  __OPCODE_MASK__SCOPE            = 0x000000F0,
  __OPCODE_MASK__TYPE             = 0x000FFF00,
  __OPCODE_MASK__TARGET           = 0x00F00000,
  __OPCODE_MASK__ZERO             = 0x0F000000,
  __OPCODE_MASK__ADDITIVE         = 0x10000000,

  __OPCODE_PROBE_MASK             = 0xE0000000,


  /*                                  A------A    */
  __OPCODE_ACTION__STATE_1        = 0x10000005,
  __OPCODE_ACTION__STATE_0        = 0x00000005,
  __OPCODE_ACTION__ASSIGN         = 0x1000000A,
  __OPCODE_ACTION__CREATE         = 0x1000000C,
  __OPCODE_ACTION__DELETE         = 0x0000000D,
  __OPCODE_ACTION__EVENT          = 0x1000000E,

  /*                                  ------S-    */
  __OPCODE_SCOPE__SINGLE          = 0x00000010,
  __OPCODE_SCOPE__MULTIPLE        = 0x000000F0,

  /*                                  --T-----    */
  __OPCODE_TARGET__VERTEX         = 0x00100000,
  __OPCODE_TARGET__ARC            = 0x00200000,
  __OPCODE_TARGET__SYSTEM         = 0x00300000,
  __OPCODE_TARGET__GRAPH          = 0x00400000,
  __OPCODE_TARGET__RW             = 0x00500000,
  __OPCODE_TARGET__EVP            = 0x00600000,
  __OPCODE_TARGET__TIME           = 0x00700000,
  __OPCODE_TARGET__EXEC           = 0x00800000,
  __OPCODE_TARGET__ACQUIRE        = 0x00A00000,
  __OPCODE_TARGET__ENUM           = 0x00E00000,

  /*                                  A-------    */
  __OPCODE_ADDITIVE               = 0x10000000,

  /*                                  P-------    */
  __OPCODE_PROBE__ANY             = 0x00000000, // 0000
  __OPCODE_PROBE__EXACT           = 0x20000000, // 0010
  __OPCODE_PROBE__ACTION          = 0x40000000, // 0100
  __OPCODE_PROBE__SCOPE           = 0x60000000, // 0110
  __OPCODE_PROBE__TYPE            = 0x80000000, // 1000
  __OPCODE_PROBE__TARGET          = 0xA0000000, // 1010
  __OPCODE_PROBE__ADDITIVE        = 0xC0000000, // 1100


  //                               
  //                               
  //                               
  //                               
  // NOP                                          000                             
  OPCODE_NONE                         = GET_OPCODE( 0x000, 0,                       __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ), // nop


  // -- VERTEX -------------------
  //
  // Vertex Lifetime                    1011
  OPCODE_VERTEX_NEW                   = GET_OPCODE( 0x011, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // vxn
  OPCODE_VERTEX_DELETE                = GET_OPCODE( 0x011, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // vxd
  
  // Rank                               1012
  OPCODE_VERTEX_SET_RANK              = GET_OPCODE( 0x012, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__ASSIGN ),    // vxr

  // Type                               1013
  OPCODE_VERTEX_SET_TYPE              = GET_OPCODE( 0x013, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__ASSIGN ),    // vxt

  // Expiration                         1014
  OPCODE_VERTEX_SET_TMX               = GET_OPCODE( 0x014, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__ASSIGN ),    // vxx

  // Convert                            1015
  OPCODE_VERTEX_CONVERT               = GET_OPCODE( 0x015, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__ASSIGN ),    // vxc

  // Properties                         1016
  OPCODE_VERTEX_SET_PROPERTY          = GET_OPCODE( 0x016, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // vps
  OPCODE_VERTEX_SET_PROPERTY_EXPLICIT = GET_OPCODE( 0x016, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // vpx
  OPCODE_VERTEX_DELETE_PROPERTY       = GET_OPCODE( 0x016, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // vpd
  OPCODE_VERTEX_CLEAR_PROPERTIES      = GET_OPCODE( 0x016, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__DELETE ),  // vpc
  
  // Vector                             1017
  OPCODE_VERTEX_SET_VECTOR            = GET_OPCODE( 0x017, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // vvs
  OPCODE_VERTEX_DELETE_VECTOR         = GET_OPCODE( 0x017, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // vvd
  
  // Outarcs                            1018
  OPCODE_VERTEX_DELETE_OUTARCS        = GET_OPCODE( 0x018, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__DELETE ),  // vod

  // Inarcs                             1019
  OPCODE_VERTEX_DELETE_INARCS         = GET_OPCODE( 0x019, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__DELETE ),  // vid

  // Explicit acquire                   101C
  OPCODE_VERTEX_ACQUIRE               = GET_OPCODE( 0x01C, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_1 ),   // val
 
  // Explicit release                   101C
  OPCODE_VERTEX_RELEASE               = GET_OPCODE( 0x01C, __OPCODE_TARGET__VERTEX, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_0 ),   // vrl
  //
  // -----------------------------
  
  // -- ARCS ---------------------
  //
  // Connect                            2001
  OPCODE_ARC_CONNECT                  = GET_OPCODE( 0x001, __OPCODE_TARGET__ARC, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),       // arc

  // Disconnect                         2002
  OPCODE_ARC_DISCONNECT               = GET_OPCODE( 0x002, __OPCODE_TARGET__ARC, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__DELETE ),     // ard
  //
  // -----------------------------
 
  // -- SYSTEM -------------------
  //

  // Attach                             3011
  OPCODE_SYSTEM_ATTACH                = GET_OPCODE( 0x011, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__STATE_1 ), // sya

  // Detach                             3012
  OPCODE_SYSTEM_DETACH                = GET_OPCODE( 0x012, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__STATE_0 ), // syd

  // Registry                           3021
  OPCODE_SYSTEM_CLEAR_REGISTRY        = GET_OPCODE( 0x021, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__DELETE ),  // rcl

  // Similarity                         3031
  OPCODE_SYSTEM_SIMILARITY            = GET_OPCODE( 0x031, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // scf

  // Send Comment                       30C0
  OPCODE_SYSTEM_SEND_COMMENT          = GET_OPCODE( 0x0C0, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),     // com

  // Send Raw Data                      30DA
  OPCODE_SYSTEM_SEND_RAW_DATA         = GET_OPCODE( 0x0DA, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),     // dat

  // Clone Graph                        30E1
  OPCODE_SYSTEM_CLONE_GRAPH           = GET_OPCODE( 0x0E1, __OPCODE_TARGET__SYSTEM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // clg


  //
  // -----------------------------

  // -- GRAPH --------------------
  //
  // Create Graph                       4051
  OPCODE_SYSTEM_CREATE_GRAPH          = GET_OPCODE( 0x051, __OPCODE_TARGET__GRAPH, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),     // grn
  
  // Delete Graph                       4052
  OPCODE_SYSTEM_DELETE_GRAPH          = GET_OPCODE( 0x052, __OPCODE_TARGET__GRAPH, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),     // grd

  // Truncate                           4053
  OPCODE_GRAPH_TRUNCATE               = GET_OPCODE( 0x053, __OPCODE_TARGET__GRAPH, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),     // grt

  // Persist                            4055
  OPCODE_GRAPH_PERSIST                = GET_OPCODE( 0x055, __OPCODE_TARGET__GRAPH, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),      // grp

  // State                              4056
  OPCODE_GRAPH_STATE                  = GET_OPCODE( 0x056, __OPCODE_TARGET__GRAPH, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),      // grs
  //
  // -----------------------------

  // -- EVENTS -------------------
  //
  // Set Readonly                       5001
  OPCODE_GRAPH_READONLY               = GET_OPCODE( 0x001, __OPCODE_TARGET__RW, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_0 ),       // grr

  // Clear Readonly                     5002
  OPCODE_GRAPH_READWRITE              = GET_OPCODE( 0x002, __OPCODE_TARGET__RW, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_1 ),       // grw

  // Enable Event Processor             6003
  OPCODE_GRAPH_EVENTS                 = GET_OPCODE( 0x003, __OPCODE_TARGET__EVP, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_1 ),      // gre

  // Disable Event Processor            6004
  OPCODE_GRAPH_NOEVENTS               = GET_OPCODE( 0x004, __OPCODE_TARGET__EVP, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__STATE_0 ),      // gri

  // Timer Tick                         7005
  OPCODE_GRAPH_TICK                   = GET_OPCODE( 0x005, __OPCODE_TARGET__TIME, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),       // tic

  // Event Execution Burst              8006
  OPCODE_GRAPH_EVENT_EXEC             = GET_OPCODE( 0x006, __OPCODE_TARGET__EXEC, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__EVENT ),       // evx
  //
  // -----------------------------

  // -- ACQUISITION---------------
  //
  // Atomic Writelocks                  A011
  OPCODE_VERTICES_ACQUIRE_WL          = GET_OPCODE( 0x011, __OPCODE_TARGET__ACQUIRE, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__STATE_1 ),  // lxw

  // Atomic Release                     A013
  OPCODE_VERTICES_RELEASE             = GET_OPCODE( 0x013, __OPCODE_TARGET__ACQUIRE, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__STATE_0 ),  // ulv

  // Release All                        A0AA
  OPCODE_VERTICES_RELEASE_ALL         = GET_OPCODE( 0x0AA, __OPCODE_TARGET__ACQUIRE, __OPCODE_SCOPE__MULTIPLE, __OPCODE_ACTION__STATE_0 ),  // ula
  //
  // -----------------------------


  // -- ENUMERATION --------------
  //
  // Vertex Type                        E001
  OPCODE_ENUM_ADD_VXTYPE              = GET_OPCODE( 0x001, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // vea
  OPCODE_ENUM_DELETE_VXTYPE           = GET_OPCODE( 0x001, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // ved

  // Relationship Type                  E002
  OPCODE_ENUM_ADD_REL                 = GET_OPCODE( 0x002, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // rea
  OPCODE_ENUM_DELETE_REL              = GET_OPCODE( 0x002, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // red

  // Dimension                          E003
  OPCODE_ENUM_ADD_DIM                 = GET_OPCODE( 0x003, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // dea
  OPCODE_ENUM_DELETE_DIM              = GET_OPCODE( 0x003, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // ded

  // Key                                E004
  OPCODE_ENUM_ADD_KEY                 = GET_OPCODE( 0x004, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // kea
  OPCODE_ENUM_DELETE_KEY              = GET_OPCODE( 0x004, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // ked

  // String                             E005
  OPCODE_ENUM_ADD_STRING              = GET_OPCODE( 0x005, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__CREATE ),    // sea
  OPCODE_ENUM_DELETE_STRING           = GET_OPCODE( 0x005, __OPCODE_TARGET__ENUM, __OPCODE_SCOPE__SINGLE, __OPCODE_ACTION__DELETE ),    // sed
  //
  // -----------------------------


} OperationProcessorOpCode;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorOpProfileID {
  OP_PROFILE_ID__NONE             = 0,
  OP_PROFILE_ID__CONSUMER_DENY    = 0x1D00C000,
  OP_PROFILE_ID__CONSUMER_ALLOW   = 0x1A00C000,
  OP_PROFILE_ID__DENY_DELETES     = 0x1D00000D,
} OperationProcessorOpProfileID;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorOpProfileMode {
  OP_PROFILE_MODE__DENY     = 0,
  OP_PROFILE_MODE__ALLOW    = 1
} OperationProcessorOpProfileMode;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_OperationProcessorOpProfile {
  OperationProcessorOpProfileMode mode;
  OperationProcessorOpProfileID id;
  const char *name;
  OperationProcessorOpCode opcodes[64];
} OperationProcessorOpProfile;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t __get_opcode_filter( int64_t spec ) {

  // Probe mask not set
  if( (spec & __OPCODE_PROBE_MASK) == 0 ) {
    // No filter
    if( spec == 0 ) {
      return __OPCODE_PROBE__ANY;
    }
    // Exact filter
    else {
      return spec | __OPCODE_PROBE__EXACT;
    }
  }
  // Probe mask is set
  else {
    return spec;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_graph_base_counts_t {
  int64_t order;
  // [2]
  int64_t size;
  // [3]
  int64_t nkey;
  // [4]
  int64_t nstrval;
  // [5]
  int64_t nprop;
  // [6]
  int64_t nvec;
  // [7]
  int ndim;
  // [8]
  short nrel;
  // [9]
  BYTE ntype;
  // [10]
  union {
    BYTE _bits;
    struct {
      BYTE force        : 1;
      BYTE assert       : 1;
      BYTE fwdpersist   : 1;
      BYTE _rsv4        : 1;
      BYTE _rsv5        : 1;
      BYTE _rsv6        : 1;
      BYTE _rsv7        : 1;
      BYTE _rsv8        : 1;
    };
  } flags;
} vgx_graph_base_counts_t;




/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef union s_OperationProcessorOperator_t {
  QWORD data;
  struct {
    OperationProcessorOpCode code;
    char name[4];
  };
} OperationProcessorOperator_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef union __u_op_BASE {
  QWORD qwords[8];
  struct {
    OperationProcessorOperator_t op;
    QWORD _data[7];
  };
} op_BASE;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorParserState {
  OPSTATE_EXPECT_ANY                = 0x0000,
  OPSTATE_EXPECT_OP                 = 0x0001,
  OPSTATE_EXPECT_OPTYPE             = 0x0002,
  OPSTATE_EXPECT_GRAPH_OBID         = 0x0003,
  OPSTATE_EXPECT_VERTEX_OBID        = 0x0004,
  OPSTATE_EXPECT_QWORD              = 0x0005,
  OPSTATE_EXPECT_DWORD              = 0x0006,
  OPSTATE_EXPECT_WORD               = 0x0007,
  OPSTATE_EXPECT_BYTE               = 0x0008,
  OPSTATE_EXPECT_CSTRING            = 0x0009,
  OPSTATE_EXPECT_OPERATOR           = 0x0011,
  OPSTATE_EXPECT_OPNAME             = 0x0021,
  OPSTATE_EXPECT_OPCODE             = 0x0022,
  OPSTATE_EXPECT_OPARG              = 0x0023,
  OPSTATE_EXPECT_ENDOP              = 0x0031,
  OPSTATE_EXPECT_OPID               = 0x0032,
  OPSTATE_EXPECT_TMS                = 0x0033,
  OPSTATE_EXPECT_CRC                = 0x0034,

  OPSTATE_EXPECT_IDLE               = 0x0A01,
  OPSTATE_EXPECT_IDLE_TMS           = 0x0A02,
  OPSTATE_EXPECT_TRANSACTION        = 0x0B01,
  OPSTATE_EXPECT_TRANSACTION_ID     = 0x0B02,
  OPSTATE_EXPECT_TRANSACTION_SN     = 0x0B03,
  OPSTATE_EXPECT_TRANSACTION_MSTRSN = 0x0B04,
  OPSTATE_EXPECT_COMMIT             = 0x0C01,
  OPSTATE_EXPECT_COMMIT_ID          = 0x0C02,
  OPSTATE_EXPECT_COMMIT_TMS         = 0x0C03,
  OPSTATE_EXPECT_COMMIT_CRC         = 0x0C04,
  OPSTATE_EXPECT_RESYNC             = 0x0D01,
  OPSTATE_EXPECT_RESYNC_ID          = 0x0D02,
  OPSTATE_EXPECT_RESYNC_BSZ         = 0x0D03,
  OPSTATE_EXPECT_ATTACH             = 0x0E01,
  OPSTATE_EXPECT_ATTACH_PROTO       = 0x0E02,
  OPSTATE_EXPECT_ATTACH_VER         = 0x0E03,


  OPSTATE_PARSER_YIELD              = 0x8000,

  OPSTATE_OPERR_RECOVERY            = 0xEE01,
  OPSTATE_OPERR_TRANSIENT           = 0xEE20,
  OPSTATE_OPERR_PERMANENT           = 0xEE40,
  OPSTATE_OPERR_TRANSACTION         = 0xEE80
} OperationProcessorParserState;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorOpType {
  OPTYPE_NONE                 = 0x0000,
  OPTYPE_SYSTEM               = 0x0001,
  OPTYPE_GRAPH_OBJECT         = 0x1001,
  OPTYPE_GRAPH_STATE          = 0x100A,
  OPTYPE_VERTEX_OBJECT        = 0x2001,
  OPTYPE_VERTEX_LOCK          = 0x200A,
  OPTYPE_VERTEX_RELEASE       = 0x200B
} OperationProcessorOpType;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorExecutionMode {
  __OPEXEC_MASK__FEED_PARSER  = 0x01,
  __OPEXEC_MASK__LOAD_ALLOC   = 0x02,
  __OPEXEC_MASK__LOAD_GRAPH   = 0x04,
  __OPEXEC_MASK__OPEN_VERTEX  = 0x08,
  __OPEXEC_MASK__EXEC_OPCODE  = 0x10,
  __OPEXEC_MASK_20            = 0x20,
  __OPEXEC_MASK__UNTIL_ENDOP  = 0x40,
  OPEXEC_NONE                 = 0x00,
  OPEXEC_NORMAL               = __OPEXEC_MASK__FEED_PARSER | __OPEXEC_MASK__LOAD_ALLOC  |
                                __OPEXEC_MASK__LOAD_GRAPH  | __OPEXEC_MASK__OPEN_VERTEX |
                                __OPEXEC_MASK__EXEC_OPCODE,
  OPEXEC_SIMULATE             = __OPEXEC_MASK__FEED_PARSER | __OPEXEC_MASK__LOAD_ALLOC  |
                                __OPEXEC_MASK__LOAD_GRAPH  | __OPEXEC_MASK__OPEN_VERTEX

} OperationProcessorExecutionMode;


#define PARSER_CONTROL_FEED( Parser )                   (((Parser)->control.exe & __OPEXEC_MASK__FEED_PARSER) != 0)
#define PARSER_CONTROL_LOAD_ALLOCATOR( Parser )         (((Parser)->control.exe & __OPEXEC_MASK__LOAD_ALLOC ) != 0)
#define PARSER_CONTROL_LOAD_GRAPH( Parser )             (((Parser)->control.exe & __OPEXEC_MASK__LOAD_GRAPH ) != 0)
#define PARSER_CONTROL_OPEN_VERTEX( Parser )            (((Parser)->control.exe & __OPEXEC_MASK__OPEN_VERTEX) != 0)
#define PARSER_CONTROL_EXEC_OPCODE( Parser )            (((Parser)->control.exe & __OPEXEC_MASK__EXEC_OPCODE) != 0)
#define PARSER_CONTROL_UNTIL_ENDOP( Parser )            (((Parser)->control.exe & __OPEXEC_MASK__UNTIL_ENDOP) != 0)
#define PARSER_CONTROL_SKIP_OPERATION( Parser )         ((Parser)->control.exe = OPEXEC_SIMULATE | __OPEXEC_MASK__UNTIL_ENDOP)
#define PARSER_CONTROL_OPERATION_SKIPPED( Parser )      ((Parser)->control.exe == (OPEXEC_SIMULATE | __OPEXEC_MASK__UNTIL_ENDOP))



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorSerialCheck {
  __OPSERIAL_MASK__CHECK_REGRESSION   = 0x01,
  __OPSERIAL_MASK__CATCH_REGRESSION   = 0x02,
  OPSERIAL_CHECK_NONE      = 0x00,
  OPSERIAL_CHECK_SILENT    = __OPSERIAL_MASK__CHECK_REGRESSION,
  OPSERIAL_CHECK_STRICT    = __OPSERIAL_MASK__CHECK_REGRESSION | __OPSERIAL_MASK__CATCH_REGRESSION
} OperationProcessorSerialCheck;


#define PARSER_CONTROL_CHECK_REGRESSION( Parser )       (((Parser)->control.snchk & __OPSERIAL_MASK__CHECK_REGRESSION) != 0)
#define PARSER_CONTROL_CATCH_REGRESSION( Parser )       (((Parser)->control.snchk & __OPSERIAL_MASK__CATCH_REGRESSION) != 0)



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_OperationProcessorAuxCommand {
  __OPAUX_MASK__NONE          = 0x00000000,
  __OPAUX_MASK__SYSTEM        = 0x00000010,
  __OPAUX_MASK__PROPERTY      = 0x00000100,
  __OPAUX_MASK__PROP_SET      = 0x00000200,
  __OPAUX_MASK__PROP_DEL      = 0x00000400,
  __OPAUX_MASK__PROP_MULTI    = 0x00000800,
  __OPAUX_MASK__FORWARD       = 0x00001000,
  OPAUX_NONE                  = 0,
  OPAUX_SYSTEM_PROPERTY       = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__PROPERTY,
  OPAUX_SYSTEM_SET_PROPERTY   = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__PROPERTY | __OPAUX_MASK__PROP_SET,
  OPAUX_SYSTEM_DEL_PROPERTY   = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__PROPERTY | __OPAUX_MASK__PROP_DEL,
  OPAUX_SYSTEM_SET_PROPERTIES = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__PROPERTY | __OPAUX_MASK__PROP_SET | __OPAUX_MASK__PROP_MULTI,
  OPAUX_SYSTEM_DEL_PROPERTIES = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__PROPERTY | __OPAUX_MASK__PROP_DEL | __OPAUX_MASK__PROP_MULTI,
  OPAUX_RAW_DATA              = __OPAUX_MASK__SYSTEM | __OPAUX_MASK__FORWARD
} OperationProcessorAuxCommand;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_operation_parser_token {
  const char *data;
  int len;
  struct {
    uint8_t suspend;
    uint8_t pending;
    uint8_t _rsv3;
    uint8_t _rsv4;
  } flags;
} operation_parser_token;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_op_parser_error_t {
  int64_t n_syntax;
  int64_t n_transient;
  int64_t n_permanent;
  int64_t n_transaction;
  OperationProcessorParserState errstate;
  int backoff_ms;
} vgx_op_parser_error_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef int (*f_parse_operator)( union u_vgx_OperationParser_t *parser );



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef int (*f_execute_operator)( union u_vgx_OperationParser_t *parser );



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef int (*f_retire_operator)( union u_vgx_OperationParser_t *parser );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationBuffer_t {
  // -------------------------------------------
  // [Q1]
  //
  CS_LOCK lock;

  // -------------------------------------------
  // [Q2.1]
  // Buffer
  char *data;
  
  // [Q2.2]
  //
  char *__allocated;

  // [Q2.3]
  // Capacity
  int64_t capacity;

  // [Q2.4]
  // Write pointer
  char *wp;

  // [Q2.5]
  // Read pointer
  const char *rp;

  // [Q2.6]
  // Commit pointer
  const char *cp;

  // [Q2.7]
  // Next buffer (in pool)
  QWORD __next;

  // [Q2.8
  // Previous buffer (in pool)
  QWORD __prev;

  // -------------------------------------------
  // [Q3.1.1]
  //
  int lock_count;

  // [Q3.1.2]
  //
  union {
    DWORD data;
    struct {
      uint8_t has_readable;
      uint8_t has_unconfirmed;
      uint8_t _rsv3;
      uint8_t _rsv4;
    };
  } flags_CS;

  // [Q3.2]
  //
  CString_t *CSTR__name;

  // [Q3.3]
  QWORD __rsv_3_3;

  // [Q3.4]
  QWORD __rsv_3_4;

  // [Q3.5]
  QWORD __rsv_3_5;

  // [Q3.6]
  QWORD __rsv_3_6;

  // [Q3.7]
  QWORD __rsv_3_7;

  // [Q3.8]
  QWORD __rsv_3_8;

} vgx_OperationBuffer_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int __enter_buffer_CS( vgx_OperationBuffer_t *buffer ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &buffer->lock.lock );
  // SAFE HERE
  return ++(buffer->lock_count);
}



/**************************************************************************//**
 * __leave_buffer_CS
 *
 ******************************************************************************
 */
__inline static int __leave_buffer_CS( vgx_OperationBuffer_t *buffer ) {
  // SAFE HERE
  int c = --(buffer->lock_count);
  LEAVE_CRITICAL_SECTION( &buffer->lock.lock );
  // UNSAFE HERE
  return c;
}



#define OPERATION_BUFFER_LOCK( OpBuffer )         \
  do {                                            \
    vgx_OperationBuffer_t *__opbuf__ = OpBuffer;  \
    __enter_buffer_CS( __opbuf__ );               \
    do


#define OPERATION_BUFFER_RELEASE       \
    WHILE_ZERO;                        \
    __leave_buffer_CS( __opbuf__ );    \
  } WHILE_ZERO




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IOperationBuffer_t {

  vgx_OperationBuffer_t * (*New)( int order, const char *name );
  void (*Delete)( vgx_OperationBuffer_t **buffer );
  const char * (*Name)( vgx_OperationBuffer_t *buffer, const char *name );

  int64_t (*Capacity)( const vgx_OperationBuffer_t *buffer );
  int64_t (*Size)( const vgx_OperationBuffer_t *buffer );
  bool (*Empty)( vgx_OperationBuffer_t *buffer );
  int64_t (*Readable)( const vgx_OperationBuffer_t *buffer );
  int64_t (*Writable)( const vgx_OperationBuffer_t *buffer );
  int64_t (*Unconfirmed)( const vgx_OperationBuffer_t *buffer );

  int64_t (*GetReadable)( const vgx_OperationBuffer_t *buffer, char **data );
  int64_t (*GetTail)( const vgx_OperationBuffer_t *buffer, char **data, int64_t n );
  int64_t (*ReadUntil)( vgx_OperationBuffer_t *buffer, int64_t max, char **data, const char probe );
  int64_t (*GetUnconfirmed)( const vgx_OperationBuffer_t *buffer, char **data );

  unsigned int (*CrcReadable)( const vgx_OperationBuffer_t *buffer, unsigned int crc );
  unsigned int (*CrcUnconfirmed)( const vgx_OperationBuffer_t *buffer, unsigned int crc );
  unsigned int (*CrcTail)( const vgx_OperationBuffer_t *buffer, int64_t n, unsigned int crc );

  int (*Expand)( vgx_OperationBuffer_t *buffer );
  int (*Trim)( vgx_OperationBuffer_t *buffer, int64_t max_sz );
  int64_t (*Write)( vgx_OperationBuffer_t *buffer, const char *data, int64_t n );
  int64_t (*WriteCrc)( vgx_OperationBuffer_t *buffer, const char *data, int64_t n, unsigned int *crc );
  int64_t (*Copy)( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other );
  int64_t (*CopyCrc)( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc );
  int64_t (*Absorb)( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other );
  int64_t (*AbsorbCrc)( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc );
  int64_t (*Unwrite)( vgx_OperationBuffer_t *buffer, int64_t n );
  int64_t (*WritableSegment)( vgx_OperationBuffer_t *buffer, int64_t max, char **segment, char **end );
  int64_t (*AdvanceWrite)( vgx_OperationBuffer_t *buffer, int64_t n );
  int64_t (*ReadableSegment)( vgx_OperationBuffer_t *buffer, int64_t max, const char **segment, const char **end );
  int64_t (*AdvanceRead)( vgx_OperationBuffer_t *buffer, int64_t n );
  int64_t (*Confirm)( vgx_OperationBuffer_t *buffer, int64_t n );
  int64_t (*Rollback)( vgx_OperationBuffer_t *buffer );
  int64_t (*Clear)( vgx_OperationBuffer_t *buffer );

  int (*Acquire)( vgx_OperationBuffer_t *buffer );
  int (*Release)( vgx_OperationBuffer_t *buffer );

  void (*DumpState)( vgx_OperationBuffer_t *buffer );

} vgx_IOperationBuffer_t;


DLL_VISIBLE extern vgx_IOperationBuffer_t iOpBuffer;





/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationBufferPool_t {
  // [Q1.1]
  int64_t capacity;

  // [Q1.2]
  int64_t remain;

  // [Q1.3]
  vgx_OperationBuffer_t **next;

  // [Q1.4]
  vgx_OperationBuffer_t **end;

  // [Q1.5.1]
  int init_order;

  // [Q1.5.2]
  int __rsv_1_2_2;

  // [Q1.6]
  vgx_OperationBuffer_t **_buffers;

  // [Q1.7]
  char *name;

  // [Q1.8]
  int __rsv_1_8;

} vgx_OperationBufferPool_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IOperationBufferPool_t {

  vgx_OperationBufferPool_t * (*New)( int64_t capacity, int order, const char *name );
  void (*Delete)( vgx_OperationBufferPool_t **pool );
  int64_t (*Capacity)( const vgx_OperationBufferPool_t *pool );
  int64_t (*Remain)( const vgx_OperationBufferPool_t *pool );

  vgx_OperationBuffer_t * (*GetBuffer)( vgx_OperationBufferPool_t *pool );
  int (*ReturnBuffer)( vgx_OperationBufferPool_t *pool, vgx_OperationBuffer_t **buffer );

} vgx_IOperationBufferPool_t;


DLL_HIDDEN extern vgx_IOperationBufferPool_t iOpBufferPool;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */


#define OPSYS_OP_BUFFER_LIMIT (1 << 30)



/*******************************************************************//**
 * vgx_OperationEmitter_t
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_OperationEmitter_t {
  cacheline_t __cl[3];
  struct {
    // -------- CL 1 --------

    // [Q1.1]
    comlib_task_t *TASK;

    // [Q1.2]
    union {
      QWORD _bits;
      struct {
        int throttle_CS; // NOTE: 0=max, then it goes negative for each throttledown
        short drain_TCS;
        int8_t defercommit_CS;
        struct {
          uint8_t defunct   : 1;
          uint8_t flush     : 1;
          uint8_t suspended : 1;
          uint8_t disabled  : 1;
          uint8_t ready     : 1;
          uint8_t running   : 1;
          uint8_t opmuted   : 1;
          uint8_t heartbeat : 1;
        } flag_CS;
      };
    } control;

    // [Q1.3]
    struct s_vgx_Graph_t *graph;

    // [Q1.4]
    int64_t opid;

    // [Q1.5]
    vgx_OperationBuffer_t *op_buffer;

    // [Q1.6]
    int64_t n_inflight_CS;

    // [Q1.7]
    int64_t lxw_balance;

    // [Q1.8]
    int64_t commit_deadline_ms;

    // -------- CL 2 --------
    // [Q2.1]
    CQwordQueue_t *private_commit;

    // [Q2.2]
    CQwordQueue_t *private_retire;

    // [Q2.3]
    CQwordQueue_t *capture_pool_CS;

    // [Q2.4]
    CQwordQueue_t *commit_pending_CS;

    // [Q2.5]
    CQwordQueue_t *commit_swap;

    // [Q2.6]
    CQwordQueue_t *cstring_discard;

    // [Q2.7]
    int64_t opid_last_emit;

    // [Q2.8]
    int64_t opid_last_commit_CS;

    // -------- CL 3 --------

    // [Q3.1-6]
    CS_COND opstream_ready;

    // [Q3.7]
    struct {
      int operations;
      int opcodes;
    } n_uncommitted;

    // [Q3.8]
    int64_t sz_private_commit_CS;

  };
} vgx_OperationEmitter_t;



/*******************************************************************//**
 * vgx_OperationCounters_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationCounters_t {
  int64_t n_transactions;
  int64_t n_operations;
  int64_t n_opcodes;
  int64_t n_bytes;
} vgx_OperationCounters_t;



/*******************************************************************//**
 * vgx_OperationFeedRates_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationFeedRates_t {
  double tpms; // transactions per millisecond
  double opms; // operations per millisecond
  double cpms; // opcodes per millisecond
  double bpms; // bytes per millisecond
  int refresh;
} vgx_OperationFeedRates_t;



/*******************************************************************//**
 * vgx_OperationBacklog_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationBacklog_t {
  int64_t n_bytes;
  int64_t n_tx;
  int64_t latency_ms;
  int64_t y;
} vgx_OperationBacklog_t;



/*******************************************************************//**
 * vgx_OperationSystemState
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_OperationSystemState {

  VGX_OPERATION_SYSTEM_STATE__None = 0x00000000,

  // Sync State                       ---1
  VGX_OPERATION_SYSTEM_STATE__SYNC_MASK_Pct             = 0x000000FF,
  VGX_OPERATION_SYSTEM_STATE__SYNC_MASK_Phase           = 0x7FFFFF00,
  VGX_OPERATION_SYSTEM_STATE__SYNC_Begin                = 0x00010100,   // 0-1%
  VGX_OPERATION_SYSTEM_STATE__SYNC_EnumRelationship     = 0x00010201,   // 1-2%
  VGX_OPERATION_SYSTEM_STATE__SYNC_EnumVertexType       = 0x00010302,   // 2-3%
  VGX_OPERATION_SYSTEM_STATE__SYNC_EnumDimension        = 0x00010403,   // 3-4%
  VGX_OPERATION_SYSTEM_STATE__SYNC_EnumPropertyKey      = 0x00010504,   // 4-5%
  VGX_OPERATION_SYSTEM_STATE__SYNC_EnumPropertyValue    = 0x00010605,   // 5-6%
  VGX_OPERATION_SYSTEM_STATE__SYNC_Vertices             = 0x00010706,   // 6-50%
  VGX_OPERATION_SYSTEM_STATE__SYNC_Arcs                 = 0x00010832,   // 50-95%
  VGX_OPERATION_SYSTEM_STATE__SYNC_VirtualDeletes       = 0x0001095F,   // 95-98%
  VGX_OPERATION_SYSTEM_STATE__SYNC_Emit0                = 0x00010E62,   // 98-99%
  VGX_OPERATION_SYSTEM_STATE__SYNC_Emit100              = 0x00010E63,   // 99-100%
  VGX_OPERATION_SYSTEM_STATE__SYNC_End                  = 0x0001FF64    // 100%

} vgx_OperationSystemState;



/*******************************************************************//**
 * vgx_OperationSystemProgressCounters_t
 * 
 * 
 ***********************************************************************
 */
typedef struct  s_vgx_OperationSystemProgressCounters_t {
  vgx_OperationSystemState state;
  int64_t obj_counter;
  int64_t obj_total;
} vgx_OperationSystemProgressCounters_t;



/*******************************************************************//**
 * vgx_OperationSystem_t
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_OperationSystem_t {
  cacheline_t __cl[3];

  struct {
    // -------- CL 1 --------

    // [Q1.1]
    comlib_task_t *TASK;

    // [Q1.2]
    union {
      QWORD _bits;
      struct {
        DWORD state_change_owner ;
        int16_t readonly;
        int8_t state_change_recursion;
        struct {
          uint8_t sync_in_progress : 1;
          uint8_t request_cancel_sync : 1;
          uint8_t _rsv3 : 1;
          uint8_t _rsv4 : 1;
          uint8_t running : 1;
          uint8_t _rsv6 : 1;
          uint8_t _rsv7 : 1;
          uint8_t _rsv8 : 1;
        } flags;
      };
    } state_CS;

    // [Q1.3]
    vgx_OperationSystemProgressCounters_t *progress_CS;

    // [Q1.4]
    vgx_OperationFeedRates_t *in_feed_limits_CS;

    // [Q1.5.1]
    float running_tx_input_rate_CS;

    // [Q1.5.2]
    float running_tx_output_rate_CS;

    // [Q1.6-8]
    struct {
      QWORD prefix;
      QWORD id;
      QWORD id_offset;
    } set;

    // -------- CL 2 --------

    // [Q2.1]
    struct s_vgx_Graph_t *graph;

    // [Q2.2]
    struct s_vgx_TransactionalProducers_t *producers;

    // [Q2.3]
    struct s_vgx_TransactionalConsumerService_t *consumer;

    // [Q2.4]
    union u_vgx_OperationParser_t *validator;

    // [Q2.5-8]
    vgx_OperationBacklog_t out_backlog;

    // -------- CL 3 --------

    // [Q3.1-4]
    vgx_OperationCounters_t out_counters_CS;

    // [Q3.5-8]
    vgx_OperationCounters_t in_counters_CS;

  };
} vgx_OperationSystem_t;




/*******************************************************************//**
 * vgx_OperationParser_t
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_OperationParser_t {
  cacheline_t __cl[4];
  struct {

    // -------- CL 1 --------

    // [1]
    // [Q1.1.1]
    OperationProcessorParserState state;
    
    // [2]
    // [Q1.1.2]
    OperationProcessorOpType optype;

    // [3]
    // [Q1.2]
    struct s_vgx_Graph_t *op_graph;
    
    // [4]
    // [Q1.3]
    vgx_Vertex_t *op_vertex_WL;
    
    // [5]
    // [Q1.4]
    int64_t opid;
    
    // [6]
    // [Q1.5]
    f_parse_operator parsef;

    // [7]
    // [Q1.6]
    f_execute_operator execf;

    // [8]
    // [Q1.7]
    f_retire_operator retiref;
    
    // [9]
    // [Q1.8]
    struct s_vgx_Graph_t *__locked_graph;

    // -------- CL 2 --------

    // [10]
    // [Q2.1-8]
    op_BASE OPERATOR;

    // -------- CL 3 --------

    // [11]
    // [Q3.1 Q3.2]
    operation_parser_token token;

    // [12]
    // [Q3.3.1]
    int field;

    // [13]
    // [Q3.3.2]
    char op_mnemonic[4];

    // [14]
    // [Q3.4]
    CString_t *CSTR__error;

    // [15]
    // [Q3.5.1]
    vgx_AccessReason_t reason;

    // [16]
    // [Q3.5.2]
    unsigned int crc;

    // [17]
    // [Q3.6]
    comlib_task_t *TASK;

    // [18]
    // [Q3.7]
    union {
      QWORD _bits;
      struct {
        int n_locks;
        int8_t exe;
        int8_t snchk;
        int8_t crc;
        struct {
          uint8_t ena_validate    : 1;
          uint8_t ena_execute     : 1;
          uint8_t __rsv3          : 1;
          uint8_t mute_regression : 1;
          uint8_t strict_serial   : 1;
          uint8_t ena_crc         : 1;
          uint8_t __rsv7          : 1;
          uint8_t trg_reset       : 1;
        };
      };
    } control;


    // [19]
    // [Q3.8]
    struct s_vgx_Graph_t *parent;

    // -------- CL 4 --------

    // [20]
    // [Q4.1]
    CByteList_t *input;

    // [21]
    // [Q4.2]
    CByteList_t *private_input;
    
    // [22]
    // [Q4.3]
    int64_t n_pending_bytes;

    // [23]
    // [Q4.4]
    object_allocator_context_t *property_allocator_ref;

    // [24]
    // [Q4.5]
    object_allocator_context_t *string_allocator;

    // [25]
    // [Q4.5]
    int64_t sn;

    // [26]
    // [Q4.7-8]
    objectid_t transid;


  };
} vgx_OperationParser_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationTransaction_t {

  // [Q1.1] Transaction created timestamp
  int64_t tms;
  
  // [Q1.2] Serial number
  int64_t serial;
  
  // [Q1.3-4] Transaction ID
  objectid_t id; 
  
  // [Q1.5.1] Checksum
  unsigned int crc;

  // [Q1.5.2] Staged as head this many milliseconds after created
  int stage_dms;
  
  // [Q1.6] Number of bytes in transaction
  int64_t tsize;

  // [Q1.7] List next node
  struct s_vgx_OperationTransaction_t *next;

  // [Q1.8] List previous node
  struct s_vgx_OperationTransaction_t *prev;

  // ---------------------------

} vgx_OperationTransaction_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_TransactionalProducerAttachMode {
  TX_ATTACH_MODE_NONE                 = 0x00,
  TX_ATTACH_MODE_NORMAL               = 0x11,
  TX_ATTACH_MODE_SYNC_NEW_SUBSCRIBER  = 0x71
} vgx_TransactionalProducerAttachMode;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_TransactionalProducer_t {

  // ---------------------------

  // [Q1.1-4]
  // Pending transactions
  struct {
    vgx_OperationTransaction_t *head;
    vgx_OperationTransaction_t *tail;
    int64_t length;
    int64_t bytes;
  } sequence;

  // [Q1.5-6]
  // I/O buffers
  struct {
    vgx_OperationBuffer_t *sysout;
    vgx_OperationBuffer_t *sysin;
  } buffer;

  // [Q1.7]
  // Attached URI
  vgx_URI_t *URI;

  // [Q1.8]
  struct {
    // Timestamp (seconds) when connection was last attempted or established, 0 when connection is not wanted
    uint32_t attempt;
    // Timestamp (seconds) when connection was lost, nonzero only when a wanted connection is not established
    uint32_t down;
  } connection_ts;

  // ---------------------------

  // [Q2.1]
  // Parent graph
  struct s_vgx_Graph_t *graph;
  
  // [Q2.2]
  // Number of accepted transactions
  int64_t accepted_transactions;

  // [Q2.3]
  // Number of accepted bytes
  int64_t accepted_bytes;

  // [Q2.4]
  // Transaction ID salt (based on graph instance)
  QWORD tid_salt;

  // [Q2.5]
  // Initial serial number for this producer instance
  int64_t serial0;

  // [Q2.6]
  // Current serial number
  int64_t sn;

  // [Q2.7]
  //
  int64_t exchange_tms;

  // [Q2.8]
  //
  int64_t suspend_tx_until_tms;

  // ---------------------------
  // [Q3]
  //
  CS_LOCK lock;

  // ---------------------------
  // [Q4.1.1]
  //
  int lock_count;

  // [Q4.1.2]
  //
  union {
    DWORD _bits;
    struct {
      uint16_t _rsv16;
      struct {
        uint8_t confirmable : 1;
        uint8_t defunct     : 1;
        uint8_t abandoned   : 1;
        uint8_t handshake   : 1;
        uint8_t init        : 1;
        uint8_t _rsv6       : 1;
        uint8_t _rsv7       : 1;
        uint8_t muted       : 1;
      };
      int8_t mode;
    };
  } flags;

  // [Q4.2]
  //
  vgx_OperationTransaction_t *resync_transaction;

  // [Q4.3-6]
  //
  struct {
    // [Q4.3-4]
    objectid_t fingerprint;
    // [Q4.5]
    int64_t state_tms;
    // [Q4.6]
    int64_t __rsv_4_6;
  } local;

  // [Q4.7]
  //
  int64_t resync_remain;

  // [Q4.8]
  QWORD __rsv_4_8;
 
  // ---------------------------
  // [Q5.1-4]
  //
  struct {
    // [Q5.1-2]
    objectid_t fingerprint;
    // [Q5.3]
    int64_t master_serial;
    // [Q5.4.1]
    unsigned adminport;
    // [Q5.4.2]
    unsigned __rsv_5_4_2;
  } subscriber;
  
  // [Q5.5]
  QWORD __rsv_5_5;

  // [Q5.6]
  QWORD __rsv_5_6;

  // [Q5.7]
  QWORD __rsv_5_7;
  
  // [Q5.8]
  QWORD __rsv_5_8;

  // ---------------------------

} vgx_TransactionalProducer_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_TransactionalProducers_t {

  // ---------------------------
  // [Q1]
  CS_LOCK lock;

  // ---------------------------
  // [Q2.1.1]
  int lock_count;

  // [Q2.1.2]
  int readers;

  // [Q2.2]
  CQwordList_t *list;

  // [Q2.3]
  QWORD __rsv_2_3;

  // [Q2.4]
  QWORD __rsv_2_4;

  // [Q2.5]
  QWORD __rsv_2_5;

  // [Q2.6]
  QWORD __rsv_2_6;

  // [Q2.7]
  QWORD __rsv_2_7;

  // [Q2.8]
  QWORD __rsv_2_8;

} vgx_TransactionalProducers_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_TransactionalProducersIterator_t {
  CQwordList_t *list;
  int64_t idx;
  int64_t sz;
  vgx_TransactionalProducers_t *parent;
  bool fragmented;
} vgx_TransactionalProducersIterator_t;




/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_OperationTransactionIterator_t {
  const vgx_TransactionalProducer_t *producer;
  const vgx_OperationTransaction_t *node;
} vgx_OperationTransactionIterator_t;




/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_TransactionalConsumerService_t {

  // ---------------------------
  // [Q1.1]
  comlib_task_t *TASK;

  // [Q1.2]
  union {
    QWORD _bits;
    struct {
      int _rsv32;
      short _rsv16;
      int8_t attached;
      struct {
        uint8_t snapshot_request    : 1;
        uint8_t _rsv1               : 1;
        uint8_t tx_disk_cleanup     : 1;
        uint8_t tx_flush_parser     : 1;
        uint8_t tx_suspend_request  : 1;
        uint8_t tx_resume_request   : 1;
        uint8_t defunct             : 1;
        uint8_t detach_request      : 1;
      } flag_TCS;
    };
  } control;
  
  // [Q1.3]
  // Parent graph
  struct s_vgx_Graph_t *sysgraph;

  // [Q1.4]
  vgx_URI_t *Listen;

  // [Q1.5.1.1]
  uint16_t bind_port;

  // [Q1.5.1.2]
  uint16_t __rsv_1_5_1_2;

  // [Q1.5.2]
  uint32_t sz_poll_fd;
 
  // [Q1.6]
  struct pollfd *poll_fd;

  // [Q1.7]
  struct pollfd *plisten;

  // [Q1.8]
  struct pollfd *pclient;

  // ---------------------------

  // [Q2.1]
  //
  vgx_URI_t *TransactionProducerClient;

  // [Q2.2-3]
  // I/O buffers
  struct {
    vgx_OperationBuffer_t *request;
    vgx_OperationBuffer_t *response;
  } buffer;
 
  // [Q2.4-5]
  //
  vgx_ByteArrayList_t txdata;

  // [Q2.6]
  vgx_ByteArray_t *tx_cursor;

  // [Q2.7]
  //
  BYTE *tx_slab;

  // [Q2.8]
  //
  BYTE *tx_slab_cursor;

  // ---------------------------

  // [Q3.1-8]
  vgx_OperationTransaction_t transaction;

  // ---------------------------

  // [Q4.1-2]
  //
  objectid_t resync_tx;

  // [Q4.3.1]
  //
  int resync_pending;

  // [Q4.3.2]
  //
  int provider_attached_SYS_CS;

  // [Q4.4]
  //
  CString_t *CSTR__subscriber_uri_SYS_CS;

  // [Q4.5]
  //
  CString_t *CSTR__provider_uri_SYS_CS;

  // [Q4.6]
  //
  int64_t last_tx_tms;

  // [Q4.7-8]
  //
  objectid_t last_txid_executed;

  // ---------------------------

  // [Q5.1]
  vgx_URI_t *tx_raw_storage;

  // [Q5.2]
  CString_t *CSTR__tx_location;

  // [Q5.3]
  CString_t *CSTR__tx_fname;

  // [Q5.4]
  int64_t tx_out_sn0;

  // [Q5.5]
  int64_t tx_out_t0;

  // [Q5.6]
  int64_t tx_out_sz;

  // [Q5.7-8]
  objectid_t last_txid_commit;

  // ---------------------------

  // [Q6.1.1]
  //
  union {
    DWORD __bits;
    struct {
      int16_t __rsv16;
      struct {
        uint8_t exec_suspended  : 1;
        uint8_t has_backlog     : 1;
        uint8_t __rsv_3         : 1;
        uint8_t __rsv_4         : 1;
        uint8_t __rsv_5         : 1;
        uint8_t __rsv_6         : 1;
        uint8_t __rsv_7         : 1;
        uint8_t __rsv_8         : 1;
      } local;
      struct {
        uint8_t suspended       : 1;
        uint8_t initializing    : 1;
        uint8_t __rsv_11        : 1;
        uint8_t __rsv_12        : 1;
        uint8_t __rsv_13        : 1;
        uint8_t __rsv_14        : 1;
        uint8_t __rsv_15        : 1;
        uint8_t __rsv_16        : 1;
      } public_TCS;
    };
  } tx_state;

  // [Q6.1.2]
  //
  int tx_in_fd;

  // [Q6.2]
  //
  int64_t tx_in_sn0;

  // [Q6.3]
  vgx_OperationBuffer_t *tx_in_buffer;

  // [Q6.4]
  QWORD __rsv_6_4;

  // [Q6.5]
  int64_t tx_backlog_bytes;

  // [Q6.6]
  int64_t tx_lifetime_bytes;
  
  // [Q6.7-8]
  objectid_t initial_txid;
 
  // ---------------------------

  // [Q7.1-4]
  //
  struct {
    // [Q4.1-2]
    objectid_t fingerprint;
    // [Q4.3]
    int64_t state_tms;
    // [Q4.4]
    int64_t master_serial;
  } provider;

  // [Q7.5]
  //
  int64_t status_response_deadline_tms;

  // [Q7.6]
  QWORD __rsv_7_6;
  
  // [Q7.7]
  QWORD __rsv_7_7;

  // [Q7.8]
  QWORD __rsv_7_8;

  // ---------------------------

  // [Q8.1-3]
  struct {
    // [Q8.1]
    comlib_task_t *TASK;

    // [Q8.2]
    int64_t tx_log_threshold_SYS_CS;

    // [Q8.3.1]
    int running_TCS;

    // [Q8.3.2]
    int allowed_SYS_CS;
  } snapshot;

  // [Q8.4]
  int64_t tx_log_total_sz_SYS_CS;

  // [Q8.5]
  int64_t tx_log_since_sn_SYS_CS;

  // [Q8.6]
  QWORD __rsv_8_6;

  // [Q8.7]
  QWORD __rsv_8_7;

  // [Q8.8]
  QWORD __rsv_8_8;

  // ---------------------------

} vgx_TransactionalConsumerService_t;




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ITransactional_t {
  struct {
    vgx_TransactionalProducer_t * (*New)( struct s_vgx_Graph_t *graph, const char *ident );
    void (*Delete)( vgx_TransactionalProducer_t **producer );
    int64_t (*Commit)( vgx_TransactionalProducer_t *producer, vgx_OperationBuffer_t *data, int64_t tms, bool validate, int64_t master_serial, objectid_t *ptx_id, int64_t *ptx_serial );
    int (*Attach)( vgx_TransactionalProducer_t *producer, vgx_URI_t *URI, vgx_TransactionalProducerAttachMode mode, bool handshake, CString_t **CSTR__error );
    const char * (*Name)( const vgx_TransactionalProducer_t *producer );
    vgx_TransactionalProducerAttachMode (*Mode)( const vgx_TransactionalProducer_t *producer );
    int (*Connect)( vgx_TransactionalProducer_t *producer, CString_t **CSTR__error );
    int (*Reconnect)( vgx_TransactionalProducer_t *producer, int64_t tms_now, int timeout_ms );
    bool (*Attached)( const vgx_TransactionalProducer_t *producer );
    bool (*Connected)( const vgx_TransactionalProducer_t *producer );
    bool (*ConnectedRemote)( const vgx_TransactionalProducer_t *producer );
    void (*Disconnect)( vgx_TransactionalProducer_t *producer );
    void (*SetDefunct)( vgx_TransactionalProducer_t *producer );
    int64_t (*Purge)( vgx_TransactionalProducer_t *producer );
    int64_t (*Abandon)( vgx_TransactionalProducer_t *producer );
    int64_t (*Length)( const vgx_TransactionalProducer_t *producer );
    int64_t (*Bytes)( const vgx_TransactionalProducer_t *producer );
    int64_t (*Timespan)( const vgx_TransactionalProducer_t *producer );
    int64_t (*StageLatency)( const vgx_TransactionalProducer_t *producer, int64_t tms_now );
    int64_t (*QueueLatency)( const vgx_TransactionalProducer_t *producer, int64_t tms_now );
    struct {
      struct {
        vgx_OperationTransactionIterator_t * (*Init)( const vgx_TransactionalProducer_t *producer, vgx_OperationTransactionIterator_t *iterator );
        const vgx_OperationTransaction_t * (*Next)( vgx_OperationTransactionIterator_t *iterator );
      } Iterator;
    } Transaction;
    
    struct {
      int64_t (*Writable)( const vgx_TransactionalProducer_t *producer, int64_t max, char **segment );
      int64_t (*AdvanceWrite)( vgx_TransactionalProducer_t *producer, int64_t amount );
      int64_t (*Readable)( const vgx_TransactionalProducer_t *producer, int64_t max, const char **segment );
      int64_t (*AdvanceRead)( vgx_TransactionalProducer_t *producer, int64_t amount );
      int64_t (*Readline)( vgx_TransactionalProducer_t *producer, int64_t max, char **data );
    } Receive;

    struct {
      int64_t (*ACCEPTED)( vgx_TransactionalProducer_t *producer, const objectid_t *transid, unsigned int crc, int64_t tms );
      int64_t (*REJECTED)( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms );
      int64_t (*RETRY)( vgx_TransactionalProducer_t *producer, const objectid_t *transid, DWORD reason, int64_t tms );
      int64_t (*SUSPEND)( vgx_TransactionalProducer_t *producer, DWORD reason, int64_t tms );
      int64_t (*RESUME)( vgx_TransactionalProducer_t *producer, int64_t tms );
      int64_t (*ATTACH)( vgx_TransactionalProducer_t *producer, DWORD protocol, DWORD version, objectid_t *fingerprint, WORD adminport );
      int64_t (*DETACH)( vgx_TransactionalProducer_t *producer, int64_t tms );
    } Handle;

    struct {
      int (*Exchange)( vgx_TransactionalProducer_t *producer, int64_t tms );
      int64_t (*Rollback)( vgx_TransactionalProducer_t *producer, int64_t tms );
    } Perform;

    void (*DumpState)( const vgx_TransactionalProducer_t *producer, const objectid_t *focus );
    void (*DumpUnconfirmed)( const vgx_TransactionalProducer_t *producer );
  } Producer;

  struct {
    vgx_OperationParser_t * (*New)( struct s_vgx_Graph_t *graph );
    void                    (*Delete)( vgx_OperationParser_t **validator );
    int64_t                 (*Validate)( vgx_OperationParser_t *validator, vgx_OperationTransaction_t *transaction, vgx_OperationBuffer_t *buffer, CString_t **CSTR__error );
    int                     (*Reset)( vgx_OperationParser_t *validator );
  } Validator;

  struct {
    vgx_TransactionalProducers_t *         (*New)( void );
    void                                   (*Delete)( vgx_TransactionalProducers_t **producers );
    int                                    (*Clear)( vgx_TransactionalProducers_t *producers );
    int                                    (*Add)( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducer_t **producer );
    int                                    (*Remove)( vgx_TransactionalProducers_t *producers, const char *uri );
    int                                    (*Contains)( vgx_TransactionalProducers_t *producers, const char *uri );
    int                                    (*Defragment)( vgx_TransactionalProducers_t *producers );
    int64_t                                (*Length)( vgx_TransactionalProducers_t *producers );
    struct {
      vgx_TransactionalProducersIterator_t * (*Init)( vgx_TransactionalProducers_t *producers, vgx_TransactionalProducersIterator_t *iterator );
      int64_t                                (*Length)( const vgx_TransactionalProducersIterator_t *iterator );
      vgx_TransactionalProducer_t *          (*Next)( vgx_TransactionalProducersIterator_t *iterator );
      void                                   (*Clear)( vgx_TransactionalProducersIterator_t *iterator );
    } Iterator;
  } Producers;

  struct {
    vgx_TransactionalConsumerService_t * (*New)( struct s_vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold );
    int (*Open)( vgx_TransactionalConsumerService_t *consumer_service, uint16_t port, CString_t **CSTR__error );
    int (*Close)( vgx_TransactionalConsumerService_t *consumer_service );
    int (*Delete)( vgx_TransactionalConsumerService_t **consumer_service );
  } Consumer;

} vgx_ITransactional_t;


DLL_HIDDEN extern vgx_ITransactional_t iTransactional;





/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int __enter_transactional_CS( vgx_TransactionalProducer_t *producer ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &producer->lock.lock );
  // SAFE HERE
  return ++(producer->lock_count);
}



/**************************************************************************//**
 * __leave_transactional_CS
 *
 ******************************************************************************
 */
__inline static int __leave_transactional_CS( vgx_TransactionalProducer_t *producer ) {
  // SAFE HERE
  int c = --(producer->lock_count);
  LEAVE_CRITICAL_SECTION( &producer->lock.lock );
  // UNSAFE HERE
  return c;
}



#define TRANSACTIONAL_LOCK( Producer )                            \
  do {                                                            \
    vgx_TransactionalProducer_t *__transprod__ = (Producer);      \
    __enter_transactional_CS( __transprod__ );                    \
    do



#define TRANSACTIONAL_RELEASE                   \
    WHILE_ZERO;                                 \
    __leave_transactional_CS( __transprod__ );  \
  } WHILE_ZERO



#define BEGIN_TRANSACTIONAL_IF_ATTACHED( P )        \
  do {                                              \
    if( iTransactional.Producer.Attached( P ) ) {   \
      TRANSACTIONAL_LOCK( P ) {                     \
        if( iTransactional.Producer.Attached( P ) ) /* { code } */



#define END_TRANSACTIONAL_IF_ATTACHED               \
        else { /* not attached */ }                 \
      } TRANSACTIONAL_RELEASE;                      \
    }                                               \
  } WHILE_ZERO



#define TRANSACTIONAL_SUSPEND_LOCK( Producer )                    \
  do {                                                            \
    /* SAFE HERE */                                               \
    vgx_TransactionalProducer_t *__transprod__ = (Producer);      \
    int __presus_recursion__ = __transprod__->lock_count;         \
    while( __leave_transactional_CS( __transprod__ ) > 0 );       \
    /* UNSAFE HERE */                                             \
    do



#define TRANSACTIONAL_RESUME_LOCK                                               \
    /* UNSAFE HERE */                                                           \
    WHILE_ZERO;                                                                 \
    while( __enter_transactional_CS( __transprod__ ) < __presus_recursion__ );  \
    /* SAFE HERE */                                                             \
  } WHILE_ZERO



#define TRANSACTIONAL_WAIT_UNTIL( Producer, Condition, TimeoutMilliseconds )  \
  do {                                                                        \
    BEGIN_TIME_LIMITED_WHILE( !(Condition), TimeoutMilliseconds, NULL ) {     \
      TRANSACTIONAL_SUSPEND_LOCK( Producer ) {                                \
        sleep_milliseconds( 1000 );                                           \
      } TRANSACTIONAL_RESUME_LOCK;                                            \
    } END_TIME_LIMITED_WHILE;                                                 \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int __enter_transactional_producers_CS( vgx_TransactionalProducers_t *producers ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &producers->lock.lock );
  // SAFE HERE
  return ++(producers->lock_count);
}



/**************************************************************************//**
 * __leave_transactional_producers_CS
 *
 ******************************************************************************
 */
__inline static int __leave_transactional_producers_CS( vgx_TransactionalProducers_t *producers ) {
  // SAFE HERE
  int c = --(producers->lock_count);
  LEAVE_CRITICAL_SECTION( &producers->lock.lock );
  // UNSAFE HERE
  return c;
}



#define TRANSACTIONAL_PRODUCERS_LOCK( Producers )                 \
  do {                                                            \
    vgx_TransactionalProducers_t *__producers__ = (Producers);    \
    __enter_transactional_producers_CS( __producers__ );          \
    do


#define TRANSACTIONAL_PRODUCERS_RELEASE                   \
    WHILE_ZERO;                                           \
    __leave_transactional_producers_CS( __producers__ );  \
  } WHILE_ZERO



#define TRANSACTIONAL_PRODUCERS_SUSPEND_LOCK( Producers )             \
  do {                                                                \
    /* SAFE HERE */                                                   \
    vgx_TransactionalProducers_t *__producers__ = (Producers);        \
    int __presus_recursion__ = __producers__->lock_count;             \
    while( __leave_transactional_producers_CS( __producers__ ) > 0 ); \
    /* UNSAFE HERE */                                                 \
    do


#define TRANSACTIONAL_PRODUCERS_RESUME_LOCK                                               \
    /* UNSAFE HERE */                                                                     \
    WHILE_ZERO;                                                                           \
    while( __enter_transactional_producers_CS( __producers__ ) < __presus_recursion__ );  \
    /* SAFE HERE */                                                                       \
  } WHILE_ZERO


#define TRANSACTIONAL_PRODUCERS_WAIT_UNTIL( Producers, Condition, TimeoutMilliseconds ) \
  do {                                                                                  \
    BEGIN_TIME_LIMITED_WHILE( !(Condition), TimeoutMilliseconds, NULL ) {               \
      TRANSACTIONAL_PRODUCERS_SUSPEND_LOCK( Producers ) {                               \
        sleep_milliseconds( 1000 );                                                     \
      } TRANSACTIONAL_PRODUCERS_RESUME_LOCK;                                            \
    } END_TIME_LIMITED_WHILE;                                                           \
  } WHILE_ZERO




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
ALIGNED_TYPE(union, 32) u_vgx_OperationCaptureInheritable_t {
  QWORD data[4];
  struct {
    objectid_t objectid;
    comlib_object_typeinfo_t object_typeinfo;
    int xrecursion;
    int __rsv;
  };
} vgx_OperationCaptureInheritable_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_OperatorCapture_t {
  cacheline_t __cl;
  struct {
    // [1]
    // Q1.1
    int64_t opid;

    // [2]
    // Q1.2
    struct s_vgx_Graph_t *graph;

    // [3]
    // Q1.3
    vgx_OperationEmitter_t *emitter;

    // [4]
    // Q1.4
    CQwordBuffer_t *opdatabuf;

    // [5]
    // Q1.5-8
    vgx_OperationCaptureInheritable_t inheritable;

  };
} vgx_OperatorCapture_t;



/*******************************************************************//**
 * vgx_OperationProcessor_t
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_OperationProcessor_t {
  cacheline_t __cl[10];
  struct {

    // -------- CL 1+2+3 --------
    vgx_OperationEmitter_t emitter;

    // -------- CL 4+5+6 --------
    vgx_OperationSystem_t system;
    
    // -------- CL 7+8+9+10 --------
    vgx_OperationParser_t parser;

  };
} vgx_OperationProcessor_t;



#endif
