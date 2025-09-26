/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxoperation.h
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

#ifndef _VXOPERATION_H
#define _VXOPERATION_H

#include "_vgx.h"


// Common
#define KWD_SEP           " "
#define KWD_LF            "\n"
#define KWD_EOM           "\n\n"
#define KWD_EOTX          "\r\n"

// Request
#define KWD_ATTACH        "ATTACH"
#define KWD_TRANSACTION   "TRANSACTION"
#define KWD_COMMIT        "COMMIT"
#define KWD_OP            "OP"
#define KWD_ENDOP         "ENDOP"
#define KWD_RESYNC        "RESYNC"
#define KWD_IDLE          "IDLE"

static const char kwd_ATTACH[]      = KWD_ATTACH;
static const char kwd_TRANSACTION[] = KWD_TRANSACTION;
static const char kwd_COMMIT[]      = KWD_COMMIT;
static const char kwd_OP[]          = KWD_OP;
static const char kwd_ENDOP[]       = KWD_ENDOP;
static const char kwd_RESYNC[]      = KWD_RESYNC;
static const char kwd_IDLE[]        = KWD_IDLE;

static const int sz_ATTACH      = (sizeof( KWD_ATTACH ) - 1);
static const int sz_TRANSACTION = (sizeof( KWD_TRANSACTION ) - 1);
static const int sz_COMMIT      = (sizeof( KWD_COMMIT ) - 1);
static const int sz_OP          = (sizeof( KWD_OP ) - 1);
static const int sz_ENDOP       = (sizeof( KWD_ENDOP ) - 1);
static const int sz_RESYNC      = (sizeof( KWD_RESYNC ) - 1);
static const int sz_IDLE        = (sizeof( KWD_IDLE ) - 1);


// Response
#define KWD_ACCEPTED      "ACCEPTED"
#define KWD_RETRY         "RETRY"
#define KWD_REJECTED      "REJECTED"
#define KWD_SUSPEND       "SUSPEND"
#define KWD_RESUME        "RESUME"
#define KWD_DETACH        "DETACH"

static const char kwd_ACCEPTED[]    = KWD_ACCEPTED;
static const char kwd_RETRY[]       = KWD_RETRY;
static const char kwd_REJECTED[]    = KWD_REJECTED;
static const char kwd_SUSPEND[]     = KWD_SUSPEND;
static const char kwd_RESUME[]      = KWD_RESUME;
static const char kwd_DETACH[]      = KWD_DETACH;

static const int sz_ACCEPTED    = (sizeof( KWD_ACCEPTED ) - 1);
static const int sz_RETRY       = (sizeof( KWD_RETRY ) - 1);
static const int sz_REJECTED    = (sizeof( KWD_REJECTED ) - 1);
static const int sz_SUSPEND     = (sizeof( KWD_SUSPEND ) - 1);
static const int sz_RESUME      = (sizeof( KWD_RESUME ) - 1);
static const int sz_DETACH      = (sizeof( KWD_DETACH ) - 1);


typedef enum e_vgx_OpKeyword {
  OP_KWD_none         = 0,
  OP_KWD_operator     = 1,
  OP_KWD_TRANSACTION  = 2,
  OP_KWD_COMMIT       = 3,
  OP_KWD_OP           = 4,
  OP_KWD_ENDOP        = 5,
  OP_KWD_RESYNC       = 6,
  OP_KWD_IDLE         = 7,
  OP_KWD_ACCEPTED     = 8,
  OP_KWD_RETRY        = 9,
  OP_KWD_REJECTED     = 10,
  OP_KWD_SUSPEND      = 11,
  OP_KWD_RESUME       = 12,
  OP_KWD_ATTACH       = 13,
  OP_KWD_DETACH       = 14
} vgx_OpKeyword;



typedef enum e_vgx_OpSuspendCode {
  OP_SUSPEND_CODE_AUTORESUME_TIMEOUT  = 0x0000,
  OP_SUSPEND_CODE_INDEFINITE          = 0x0001
} vgx_OpSuspendCode;



/**************************************************************************//**
 * __suspend_code_from_reason
 *
 ******************************************************************************
 */
static vgx_OpSuspendCode __suspend_code_from_reason( DWORD reason ) {
  return (vgx_OpSuspendCode)(reason >> 16);
}



/**************************************************************************//**
 * __suspend_milliseconds_from_reason
 *
 ******************************************************************************
 */
static int __suspend_milliseconds_from_reason( DWORD reason ) {
  return (int)(reason & 0xFFFF);
}



/**************************************************************************//**
 * __suspend_reason
 *
 ******************************************************************************
 */
static DWORD __suspend_reason( int suspend_ms ) {
  if( suspend_ms < 0 ) {
    // Infinite suspend. Explicit RESUME required.
    return (DWORD)(((int)OP_SUSPEND_CODE_INDEFINITE) << 16);
  }
  else {
    // Max auto-resume timeout is 65.535 seconds
    if( suspend_ms > 0xFFFF ) {
      suspend_ms = 0xFFFF;
    }
    return (DWORD)((((int)OP_SUSPEND_CODE_AUTORESUME_TIMEOUT) << 16) | suspend_ms);
  }
}



typedef enum e_vgx_OpRejectCode {
  OP_REJECT_CODE_NONE               = 0x0000,
  OP_REJECT_CODE_PROTOCOL_VERSION   = 0x0001,
  OP_REJECT_CODE_READONLY           = 0x0002,
  OP_REJECT_CODE_OVERSIZED_TX_LINE  = 0x0003,
  OP_REJECT_CODE_OVERSIZED_TX_DATA  = 0x0004,
  OP_REJECT_CODE_GENERAL            = 0x0005
} vgx_OpRejectCode;




#define __SEPSTR                        " "
#define __INDENTSTR0                    ""
#define __INDENTSTR2                    "  "
#define __INDENTSTR4                    "    "
#define __JSON_PREFIX                   "#     "
static const char SEP[] = __SEPSTR;
static const char INDENT0[] = __INDENTSTR0;
static const char INDENT2[] = __INDENTSTR2;
static const char INDENT4[] = __INDENTSTR4;



#ifndef __SYSOUT_LIMIT
#define __SYSOUT_LIMIT (1LL << 31)
#endif
#ifndef __THROTTLE_SYSOUT
#define __THROTTLE_SYSOUT (1LL << 29)
#endif
#ifndef __THROTTLE_TRANSACTION
#define __THROTTLE_TRANSACTION (1LL << 20)
#endif

static const int64_t SYSOUT_LIMIT = __SYSOUT_LIMIT;
static const int64_t THROTTLE_SYSOUT = __THROTTLE_SYSOUT;
static const int64_t THROTTLE_TRANSACTION = __THROTTLE_TRANSACTION;

 // Maximum number of bytes allowed for a transaction
#define __TX_MAX_SIZE (1LL << 23) /* 8 MiB */
#define __TX_COMMIT_SIZE_LIMIT (1LL << 16) /* 64 kiB */
 
static const int64_t TX_MAX_SIZE = __TX_MAX_SIZE;
static const int64_t TX_COMMIT_SIZE_LIMIT = __TX_COMMIT_SIZE_LIMIT;
static const int64_t TX_COMMIT_AGE_LIMIT = 300; // 0.3 second
static const int64_t TX_FLUSH_LIM = __TX_MAX_SIZE - 8*__TX_COMMIT_SIZE_LIMIT;




#define OP_NAME_NOP                           "nop"   // "No OPerator"

#define OP_NAME_VERTEX_NEW                    "vxn"   // "VerteX New"
#define OP_NAME_VERTEX_DELETE                 "vxd"   // "VerteX Delete"
#define OP_NAME_VERTEX_SET_RANK               "vxr"   // "VerteX Rank"
#define OP_NAME_VERTEX_SET_TYPE               "vxt"   // "VerteX Type"
#define OP_NAME_VERTEX_SET_TMX                "vxx"   // "VerteX eXpiration"
#define OP_NAME_VERTEX_CONVERT                "vxc"   // "VerteX Convert"

#define OP_NAME_VERTEX_SET_PROPERTY           "vps"   // "Vertex Property Set"
#define OP_NAME_VERTEX_DELETE_PROPERTY        "vpd"   // "Vertex Property Delete"
#define OP_NAME_VERTEX_CLEAR_PROPERTIES       "vpc"   // "Vertex Properties Clear"

#define OP_NAME_VERTEX_SET_VECTOR             "vvs"   // "Vertex Vector Set"
#define OP_NAME_VERTEX_DELETE_VECTOR          "vvd"   // "Vertex Vector Delete"

#define OP_NAME_VERTEX_DELETE_OUTARCS         "vod"   // "Vertex Outarcs Delete"
#define OP_NAME_VERTEX_DELETE_INARCS          "vid"   // "Vertex Inarcs Delete"
  
#define OP_NAME_VERTEX_ACQUIRE                "val"   // "Vertex Acquire Lock"
#define OP_NAME_VERTEX_RELEASE                "vrl"   // "Vertex Release Lock"

#define OP_NAME_ARC_CONNECT                   "arc"   // "ARc Connect"
#define OP_NAME_ARC_DISCONNECT                "ard"   // "ARc Disconnect"

#define OP_NAME_SYSTEM_ATTACH                 "sya"   // "SYstem Attach"
#define OP_NAME_SYSTEM_DETACH                 "syd"   // "SYstem Detach"
#define OP_NAME_SYSTEM_CLEAR_REGISTRY         "rcl"   // "Registry CLear"
#define OP_NAME_SYSTEM_SIMILARITY             "scf"   // "Similarity ConFig"
#define OP_NAME_SYSTEM_CREATE_GRAPH           "grn"   // "GRaph New"
#define OP_NAME_SYSTEM_DELETE_GRAPH           "grd"   // "GRaph Delete"
#define OP_NAME_SYSTEM_SEND_COMMENT           "com"   // "COMment"
#define OP_NAME_SYSTEM_SEND_RAW_DATA          "dat"   // "DATa"
#define OP_NAME_SYSTEM_CLONE_GRAPH            "clg"   // "GLone Graph"
#define OP_NAME_GRAPH_TRUNCATE                "grt"   // "GRaph Truncate"
#define OP_NAME_GRAPH_PERSIST                 "grp"   // "GRaph Persist"
#define OP_NAME_GRAPH_STATE                   "grs"   // "GRaph State"

#define OP_NAME_GRAPH_READONLY                "grr"   // "GRaph Readonly"
#define OP_NAME_GRAPH_READWRITE               "grw"   // "GRaph Writable"
#define OP_NAME_GRAPH_EVENTS                  "gre"   // "GRaph Events (events enabled)"
#define OP_NAME_GRAPH_NOEVENTS                "gri"   // "GRaph Idle (events disabled)
#define OP_NAME_GRAPH_TICK                    "tic"   // "TICk"
#define OP_NAME_GRAPH_EVENT_EXEC              "evx"   // "EVents eXecute"

#define OP_NAME_VERTICES_ACQUIRE_WL           "lxw"   // "Lock eXclusive Writable vertices"
#define OP_NAME_VERTICES_ACQUIRE_RO           "lsr"   // "Lock Shared Readonly vertices"

#define OP_NAME_VERTICES_RELEASE              "ulv"   // "UnLock Vertices"
#define OP_NAME_VERTICES_RELEASE_ALL          "ula"   // "UnLock All vertices"

#define OP_NAME_ENUM_ADD_VXTYPE               "vea"   // "Vertex Enumeration Add"
#define OP_NAME_ENUM_DELETE_VXTYPE            "ved"   // "Vertex Enumeration Delete"
#define OP_NAME_ENUM_ADD_REL                  "rea"   // "Relationship Enumeration Add"
#define OP_NAME_ENUM_DELETE_REL               "red"   // "Relationship Enumeration Delete"
#define OP_NAME_ENUM_ADD_DIM                  "dea"   // "Dimension Enumeration Add"
#define OP_NAME_ENUM_DELETE_DIM               "ded"   // "Dimension Enumeration Delete"
#define OP_NAME_ENUM_ADD_KEY                  "kea"   // "Key Enumeration Add"
#define OP_NAME_ENUM_DELETE_KEY               "ked"   // "Key Enumeration Delete"
#define OP_NAME_ENUM_ADD_STRING               "sea"   // "String Enumeration Add"
#define OP_NAME_ENUM_DELETE_STRING            "sed"   // "String Enumeration Delete"




#define OPERATOR_NONE                         { .code = OPCODE_NONE,                          .name = OP_NAME_NOP }

#define OPERATOR_VERTEX_NEW                   { .code = OPCODE_VERTEX_NEW,                    .name = OP_NAME_VERTEX_NEW }
#define OPERATOR_VERTEX_DELETE                { .code = OPCODE_VERTEX_DELETE,                 .name = OP_NAME_VERTEX_DELETE }
#define OPERATOR_VERTEX_SET_RANK              { .code = OPCODE_VERTEX_SET_RANK,               .name = OP_NAME_VERTEX_SET_RANK }
#define OPERATOR_VERTEX_SET_TYPE              { .code = OPCODE_VERTEX_SET_TYPE,               .name = OP_NAME_VERTEX_SET_TYPE }
#define OPERATOR_VERTEX_SET_TMX               { .code = OPCODE_VERTEX_SET_TMX,                .name = OP_NAME_VERTEX_SET_TMX }
#define OPERATOR_VERTEX_CONVERT               { .code = OPCODE_VERTEX_CONVERT,                .name = OP_NAME_VERTEX_CONVERT }

#define OPERATOR_VERTEX_SET_PROPERTY          { .code = OPCODE_VERTEX_SET_PROPERTY,           .name = OP_NAME_VERTEX_SET_PROPERTY }
#define OPERATOR_VERTEX_DELETE_PROPERTY       { .code = OPCODE_VERTEX_DELETE_PROPERTY,        .name = OP_NAME_VERTEX_DELETE_PROPERTY }
#define OPERATOR_VERTEX_CLEAR_PROPERTIES      { .code = OPCODE_VERTEX_CLEAR_PROPERTIES,       .name = OP_NAME_VERTEX_CLEAR_PROPERTIES }

#define OPERATOR_VERTEX_SET_VECTOR            { .code = OPCODE_VERTEX_SET_VECTOR,             .name = OP_NAME_VERTEX_SET_VECTOR }
#define OPERATOR_VERTEX_DELETE_VECTOR         { .code = OPCODE_VERTEX_DELETE_VECTOR,          .name = OP_NAME_VERTEX_DELETE_VECTOR }

#define OPERATOR_VERTEX_DELETE_OUTARCS        { .code = OPCODE_VERTEX_DELETE_OUTARCS,         .name = OP_NAME_VERTEX_DELETE_OUTARCS }
#define OPERATOR_VERTEX_DELETE_INARCS         { .code = OPCODE_VERTEX_DELETE_INARCS,          .name = OP_NAME_VERTEX_DELETE_INARCS }

#define OPERATOR_VERTEX_ACQUIRE               { .code = OPCODE_VERTEX_ACQUIRE,                .name = OP_NAME_VERTEX_ACQUIRE }
#define OPERATOR_VERTEX_RELEASE               { .code = OPCODE_VERTEX_RELEASE,                .name = OP_NAME_VERTEX_RELEASE }

#define OPERATOR_ARC_CONNECT                  { .code = OPCODE_ARC_CONNECT,                   .name = OP_NAME_ARC_CONNECT }
#define OPERATOR_ARC_DISCONNECT               { .code = OPCODE_ARC_DISCONNECT,                .name = OP_NAME_ARC_DISCONNECT }


#define OPERATOR_SYSTEM_ATTACH                { .code = OPCODE_SYSTEM_ATTACH,                 .name = OP_NAME_SYSTEM_ATTACH }
#define OPERATOR_SYSTEM_DETACH                { .code = OPCODE_SYSTEM_DETACH,                 .name = OP_NAME_SYSTEM_DETACH }
#define OPERATOR_SYSTEM_CLEAR_REGISTRY        { .code = OPCODE_SYSTEM_CLEAR_REGISTRY,         .name = OP_NAME_SYSTEM_CLEAR_REGISTRY }
#define OPERATOR_SYSTEM_SIMILARITY            { .code = OPCODE_SYSTEM_SIMILARITY,             .name = OP_NAME_SYSTEM_SIMILARITY }
#define OPERATOR_SYSTEM_CREATE_GRAPH          { .code = OPCODE_SYSTEM_CREATE_GRAPH,           .name = OP_NAME_SYSTEM_CREATE_GRAPH }
#define OPERATOR_SYSTEM_DELETE_GRAPH          { .code = OPCODE_SYSTEM_DELETE_GRAPH,           .name = OP_NAME_SYSTEM_DELETE_GRAPH }
#define OPERATOR_SYSTEM_SEND_COMMENT          { .code = OPCODE_SYSTEM_SEND_COMMENT,           .name = OP_NAME_SYSTEM_SEND_COMMENT }
#define OPERATOR_SYSTEM_SEND_RAW_DATA         { .code = OPCODE_SYSTEM_SEND_RAW_DATA,          .name = OP_NAME_SYSTEM_SEND_RAW_DATA }
#define OPERATOR_SYSTEM_CLONE_GRAPH           { .code = OPCODE_SYSTEM_CLONE_GRAPH,            .name = OP_NAME_SYSTEM_CLONE_GRAPH }

#define OPERATOR_GRAPH_TRUNCATE               { .code = OPCODE_GRAPH_TRUNCATE,                .name = OP_NAME_GRAPH_TRUNCATE }
#define OPERATOR_GRAPH_PERSIST                { .code = OPCODE_GRAPH_PERSIST,                 .name = OP_NAME_GRAPH_PERSIST }
#define OPERATOR_GRAPH_STATE                  { .code = OPCODE_GRAPH_STATE,                   .name = OP_NAME_GRAPH_STATE }

#define OPERATOR_GRAPH_READONLY               { .code = OPCODE_GRAPH_READONLY,                .name = OP_NAME_GRAPH_READONLY }
#define OPERATOR_GRAPH_READWRITE              { .code = OPCODE_GRAPH_READWRITE,               .name = OP_NAME_GRAPH_READWRITE }
#define OPERATOR_GRAPH_EVENTS                 { .code = OPCODE_GRAPH_EVENTS,                  .name = OP_NAME_GRAPH_EVENTS }
#define OPERATOR_GRAPH_NOEVENTS               { .code = OPCODE_GRAPH_NOEVENTS,                .name = OP_NAME_GRAPH_NOEVENTS }
#define OPERATOR_GRAPH_TICK                   { .code = OPCODE_GRAPH_TICK ,                   .name = OP_NAME_GRAPH_TICK }
#define OPERATOR_GRAPH_EVENT_EXEC             { .code = OPCODE_GRAPH_EVENT_EXEC ,             .name = OP_NAME_GRAPH_EVENT_EXEC }

#define OPERATOR_VERTICES_ACQUIRE_WL          { .code = OPCODE_VERTICES_ACQUIRE_WL,           .name = OP_NAME_VERTICES_ACQUIRE_WL }
#define OPERATOR_VERTICES_RELEASE             { .code = OPCODE_VERTICES_RELEASE,              .name = OP_NAME_VERTICES_RELEASE }
#define OPERATOR_VERTICES_RELEASE_ALL         { .code = OPCODE_VERTICES_RELEASE_ALL,          .name = OP_NAME_VERTICES_RELEASE_ALL }

#define OPERATOR_ENUM_ADD_VXTYPE              { .code = OPCODE_ENUM_ADD_VXTYPE,               .name = OP_NAME_ENUM_ADD_VXTYPE }
#define OPERATOR_ENUM_DELETE_VXTYPE           { .code = OPCODE_ENUM_DELETE_VXTYPE,            .name = OP_NAME_ENUM_DELETE_VXTYPE }
#define OPERATOR_ENUM_ADD_REL                 { .code = OPCODE_ENUM_ADD_REL,                  .name = OP_NAME_ENUM_ADD_REL }
#define OPERATOR_ENUM_DELETE_REL              { .code = OPCODE_ENUM_DELETE_REL,               .name = OP_NAME_ENUM_DELETE_REL }
#define OPERATOR_ENUM_ADD_DIM                 { .code = OPCODE_ENUM_ADD_DIM,                  .name = OP_NAME_ENUM_ADD_DIM }
#define OPERATOR_ENUM_DELETE_DIM              { .code = OPCODE_ENUM_DELETE_DIM,               .name = OP_NAME_ENUM_DELETE_DIM }
#define OPERATOR_ENUM_ADD_KEY                 { .code = OPCODE_ENUM_ADD_KEY,                  .name = OP_NAME_ENUM_ADD_KEY }
#define OPERATOR_ENUM_DELETE_KEY              { .code = OPCODE_ENUM_DELETE_KEY,               .name = OP_NAME_ENUM_DELETE_KEY }
#define OPERATOR_ENUM_ADD_STRING              { .code = OPCODE_ENUM_ADD_STRING,               .name = OP_NAME_ENUM_ADD_STRING }
#define OPERATOR_ENUM_DELETE_STRING           { .code = OPCODE_ENUM_DELETE_STRING,            .name = OP_NAME_ENUM_DELETE_STRING }


typedef union __u_op_none {
  QWORD qwords[1];
  struct {
    OperationProcessorOperator_t op;
  };
} op_none;


typedef union __u_op_vertex_new {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int vxtype;
    int _rsv1;
    // [2]
    objectid_t obid;
    // [3]
    uint32_t TMC;
    int _rsv2;
    // [4] [5]
    vgx_vertex_expiration_t TMX;
    // [6]
    vgx_Rank_t rank;
    // [7]
    CString_t *CSTR__id;
  };
} op_vertex_new;
static const size_t __qw_op_vertex_new = qwsizeof( op_vertex_new );



typedef union __u_op_vertex_delete {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int eventexec;
    DWORD _rsv;
    // [2]
    objectid_t obid;
  };
} op_vertex_delete;
static const size_t __qw_op_vertex_delete = qwsizeof( op_vertex_delete );



typedef union __u_op_vertex_set_rank {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_Rank_t rank;
  };
} op_vertex_set_rank;
static const size_t __qw_op_vertex_set_rank = qwsizeof( op_vertex_set_rank );



typedef union __u_op_vertex_set_type {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int vxtype;
    int _rsv;
  };
} op_vertex_set_type;
static const size_t __qw_op_vertex_set_type = qwsizeof( op_vertex_set_type );



typedef union __u_op_vertex_set_tmx {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    uint32_t tmx;
    int _rsv;
  };
} op_vertex_set_tmx;
static const size_t __qw_op_vertex_set_tmx = qwsizeof( op_vertex_set_tmx );



typedef union __u_op_vertex_convert {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_VertexStateContext_man_t manifestation;
    int _rsv;
  };
} op_vertex_convert;
static const size_t __qw_op_vertex_convert = qwsizeof( op_vertex_convert );



typedef union __u_op_vertex_set_property {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    shortid_t key;
    // [2]
    vgx_value_type_t vtype;
    int _rsv1;
    QWORD _rsv2;
    // [3]
    union {
      objectid_t stringid;
      struct {
        QWORD dataL;
        QWORD dataH;
      };
      struct {
        int i0;
        int i1;
        int i2;
        int i3;
      };
    };
    // [4]
    CString_t *CSTR__value;
    QWORD _rsv3;
  };
} op_vertex_set_property;
static const size_t __qw_op_vertex_set_property = qwsizeof( op_vertex_set_property );



typedef union __u_op_vertex_delete_property {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    shortid_t key;
  };
} op_vertex_delete_property;
static const size_t __qw_op_vertex_delete_property = qwsizeof( op_vertex_delete_property );



typedef union __u_op_vertex_clear_properties {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_vertex_clear_properties;
static const size_t __qw_op_vertex_clear_properties = qwsizeof( op_vertex_clear_properties );



typedef union __u_op_vertex_set_vector {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    CString_t *CSTR__vector;
  };
} op_vertex_set_vector;
static const size_t __qw_op_vertex_set_vector = qwsizeof( op_vertex_set_vector );



typedef union __u_op_vertex_delete_vector {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_vertex_delete_vector;
static const size_t __qw_op_vertex_delete_vector = qwsizeof( op_vertex_delete_vector );



typedef union __u_op_vertex_delete_outarcs {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t n_removed;
  };
} op_vertex_delete_outarcs;
static const size_t __qw_op_vertex_delete_outarcs = qwsizeof( op_vertex_delete_outarcs );



typedef union __u_op_vertex_delete_inarcs {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t n_removed;
  };
} op_vertex_delete_inarcs;
static const size_t __qw_op_vertex_delete_inarcs = qwsizeof( op_vertex_delete_inarcs );


typedef union __u_op_vertex_acquire {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_vertex_acquire;
static const size_t __qw_op_vertex_acquire = qwsizeof( op_vertex_acquire );



typedef union __u_op_vertex_release {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_vertex_release;
static const size_t __qw_op_vertex_release = qwsizeof( op_vertex_release );



typedef union __u_op_arc_connect {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_predicator_t pred;
    // [2]
    objectid_t headobid;
  };
} op_arc_connect;
static const size_t __qw_op_arc_connect = qwsizeof( op_arc_connect );



typedef union __u_op_arc_disconnect {
  QWORD qwords[6];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int eventexec;
    DWORD _rsv1;
    // [2]
    int64_t n_removed;
    // [3]
    vgx_predicator_t pred;
    // [4]
    objectid_t headobid;
  };
} op_arc_disconnect;
static const size_t __qw_op_arc_disconnect = qwsizeof( op_arc_disconnect );



typedef union __u_op_system_attach {
  QWORD qwords[6];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t tms;
    // [2]
    CString_t *CSTR__via_uri;
    // [3]
    CString_t *CSTR__origin_host;
    // [4]
    CString_t *CSTR__origin_version;
    // [5]
    int status;
    // [6]
    int _rsv;
  };
} op_system_attach;
static const size_t __qw_op_system_attach = qwsizeof( op_system_attach );



typedef union __u_op_system_detach {
  QWORD qwords[5];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t tms;
    // [2]
    CString_t *CSTR__origin_host;
    // [3]
    CString_t *CSTR__origin_version;
    // [4]
    int status;
    // [5]
    int _rsv;
  };
} op_system_detach;
static const size_t __qw_op_system_detach = qwsizeof( op_system_detach );



typedef union __u_op_system_clear_registry {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_system_clear_registry;
static const size_t __qw_op_system_clear_registry = qwsizeof( op_system_clear_registry );



typedef union __u_op_system_similarity {
  QWORD qwords[ 1 + qwsizeof(vgx_Similarity_config_t) ];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_Similarity_config_t sim;
  };
} op_system_similarity;
static const size_t __qw_op_system_similarity = qwsizeof( op_system_similarity );



typedef union __u_op_system_create_graph {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int vertex_block_order;
    // [2]
    uint32_t graph_t0;
    // [3]
    int64_t start_opcount;
    QWORD _rsv;
    // [4]
    objectid_t obid;
    // [5]
    CString_t *CSTR__path;
    // [6]
    CString_t *CSTR__name;
  };
} op_system_create_graph;
static const size_t __qw_op_system_create_graph = qwsizeof( op_system_create_graph );



typedef union __u_op_system_delete_graph {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    QWORD _rsv;
    // [1]
    objectid_t obid;
  };
} op_system_delete_graph;
static const size_t __qw_op_system_delete_graph = qwsizeof( op_system_delete_graph );



typedef union __u_op_system_send_comment {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    CString_t *CSTR__comment;
  };
} op_system_send_comment;
static const size_t __qw_op_system_send_comment = qwsizeof( op_system_send_comment );



typedef union __u_op_system_send_raw_data {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t n_parts;
    // [2]
    int64_t part_id;
    // [3]
    CString_t *CSTR__datapart;
    // [4]
    int64_t sz_datapart;
    // [5]
    OperationProcessorAuxCommand cmd;
    // [6]
    DWORD _rsv;
    // [7]
    objectid_t obid;
  };
} op_system_send_raw_data;
static const size_t __qw_op_system_send_raw_data = qwsizeof( op_system_send_raw_data );



typedef union __u_op_system_clone_graph {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    struct s_vgx_Graph_t *graph;
  };
} op_system_clone_graph;
static const size_t __qw_op_system_clone_graph = qwsizeof( op_system_clone_graph );



typedef union __u_op_graph_truncate {
  QWORD qwords[3];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int vxtype;
    int _rsv;
    // [2]
    int64_t n_discarded;
  };
} op_graph_truncate;
static const size_t __qw_op_graph_truncate = qwsizeof( op_graph_truncate );



typedef union __u_op_graph_persist {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_graph_base_counts_t counts;
  };
} op_graph_persist;
static const size_t __qw_op_graph_persist = qwsizeof( op_graph_persist );



typedef union __u_op_graph_state {
  QWORD qwords[8];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    vgx_graph_base_counts_t counts;
  };
} op_graph_state;
static const size_t __qw_op_graph_state = qwsizeof( op_graph_state );



typedef union __u_op_graph_readonly {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_graph_readonly;
static const size_t __qw_op_graph_readonly = qwsizeof( op_graph_readonly );



typedef union __u_op_graph_readwrite {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_graph_readwrite;
static const size_t __qw_op_graph_readwrite = qwsizeof( op_graph_readwrite );



typedef union __u_op_graph_events {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_graph_events;
static const size_t __qw_op_graph_events = qwsizeof( op_graph_events );



typedef union __u_op_graph_noevents {
  QWORD qwords[1];
  struct {
    // [0]
    OperationProcessorOperator_t op;
  };
} op_graph_noevents;
static const size_t __qw_op_graph_noevents = qwsizeof( op_graph_noevents );



typedef union __u_op_graph_tic {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int64_t tms;
  };
} op_graph_tic;
static const size_t __qw_op_graph_tic = qwsizeof( op_graph_tic );



typedef union __u_op_graph_event_exec {
  QWORD qwords[2];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    uint32_t ts;
    // [2]
    uint32_t tmx;
  };
} op_graph_event_exec;
static const size_t __qw_op_graph_event_exec = qwsizeof( op_graph_event_exec );



typedef union __u_op_vertices_lockstate {
  QWORD qwords[3];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    int count;
    int _rsv;
    // [2]
    objectid_t *obid_list;
  };
} op_vertices_lockstate;
static const size_t __qw_op_vertices_lockstate = qwsizeof( op_vertices_lockstate );



typedef union __u_op_enum_add_string64 {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    QWORD hash;
    // [2]
    QWORD encoded;
    // [3]
    CString_t *CSTR__value;
  };
} op_enum_add_string64;
static const size_t __qw_op_enum_add_string64 = qwsizeof( op_enum_add_string64 );



typedef union __u_op_enum_delete_string64 {
  QWORD qwords[3];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    QWORD hash;
    // [2]
    QWORD encoded;
  };
} op_enum_delete_string64;
static const size_t __qw_op_enum_delete_string64 = qwsizeof( op_enum_delete_string64 );



typedef union __u_op_enum_add_string128 {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    // [1]
    CString_t *CSTR__value;
    // [2]
    objectid_t obid;
  };
} op_enum_add_string128;
static const size_t __qw_op_enum_add_string128 = qwsizeof( op_enum_add_string128 );



typedef union __u_op_enum_delete_string128 {
  QWORD qwords[4];
  struct {
    // [0]
    OperationProcessorOperator_t op;
    QWORD _rsv1;
    // [1]
    objectid_t obid;
  };
} op_enum_delete_string128;
static const size_t __qw_op_enum_delete_string128 = qwsizeof( op_enum_delete_string128 );



// System
DLL_HIDDEN extern int _vxdurable_operation_system__sockerror( CString_t **CSTR__error );
DLL_HIDDEN extern int _vxdurable_operation_system__wouldblock( void );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static unsigned int __opdata_crc32c( const char *data, int64_t n, unsigned int *pcrc ) {
  const char *p = data;
  const char *e = data + n;
  *pcrc ^= 0xFFFFFFFF;
  while( p < e ) {
    unsigned char b = *p++;
    if( b > 0x20 ) {
      #if defined CXPLAT_ARCH_X64
      *pcrc = _mm_crc32_u8( *pcrc, b );
      #elif defined CXPLAT_ARCH_ARM64
      *pcrc = __crc32cb( *pcrc, b );
      #endif
    }
  }
  *pcrc ^= 0xFFFFFFFF;
  return *pcrc;
}


/* Operation capture is enabled for graph if the graph's emitter thread is running */
#define OPERATION_CAPTURE_ENABLED_CS( Graph )               _vxdurable_operation_emitter__is_enabled_CS( Graph )


#define OPERATION_ZERO_ALL( Operation )                     TPTR_INIT( Operation )
#define OPERATION_IS_OPEN( Operation )                      TPTR_IS_POINTER( Operation )
#define OPERATION_IS_CLOSED( Operation )                    TPTR_IS_NONPTR( Operation )
#define OPERATION_CAPTURE_GET_OBJECT( Operation )           ((vgx_OperatorCapture_t*)TPTR_GET_POINTER( Operation ))
#define OPERATION_CAPTURE_HAS_OBJECT( Operation )           (TPTR_IS_POINTER( Operation ) && TPTR_GET_POINTER( Operation ) != NULL )
#define OPERATION_CAPTURE_SET_OBJECT( Operation, Capture )  TPTR_SET_POINTER( Operation, Capture )
#define OPERATION_OPID_GET_INTEGER( Operation )             TPTR_AS_INTEGER( Operation )
#define OPERATION_OPID_SET_INTEGER( Operation, Opid )       TPTR_SET_INTEGER( Operation, Opid )

#define OPERATION_IS_DIRTY( Operation )                     TPTR_IS_DIRTY( Operation )
#define OPERATION_IS_CLEAN( Operation )                     TPTR_IS_CLEAN( Operation )
#define OPERATION_SET_DIRTY( Operation )                    TPTR_MAKE_DIRTY( Operation )
#define OPERATION_SET_CLEAN( Operation )                    TPTR_MAKE_CLEAN( Operation )


#define OPERATION_SET_LOCAL_ONLY( Operation )               TPTR_MAKE_INVALID( Operation )  /* "invalid" = local operation only     */
#define OPERATION_IS_LOCAL_ONLY( Operation )                TPTR_IS_INVALID( Operation )    /*  true operation is local only (no capture object) */
#define OPERATION_SET_EMITTABLE( Operation )                TPTR_MAKE_VALID( Operation )    /* "valid"   = operation emittable via capture object */
#define OPERATION_IS_EMITTABLE( Operation )                 TPTR_IS_VALID( Operation )      /*  true operation is emittable via capture object */


DLL_HIDDEN extern vgx_Operation_t * __OPERATION_INIT( vgx_Operation_t *operation, int64_t opid );
DLL_HIDDEN extern vgx_Operation_t * __OPERATION_OPEN( vgx_Operation_t *operation, vgx_OperatorCapture_t *capture );
DLL_HIDDEN extern vgx_Operation_t * __OPERATION_CLOSE( vgx_Operation_t *operation, int64_t opid );
DLL_HIDDEN extern vgx_OperatorCapture_t * __OPERATION_GET_CAPTURE( vgx_Operation_t *operation );
DLL_HIDDEN extern vgx_OperatorCapture_t * __OPERATION_POP_CAPTURE( vgx_Operation_t *operation );
DLL_HIDDEN extern int64_t __OPERATION_GET_OPID( const vgx_Operation_t *operation_LCK );
DLL_HIDDEN extern int64_t __OPERATION_SET_OPID( vgx_Operation_t *operation, int64_t opid );


#define OPERATION_INIT( Operation, Opid )         __OPERATION_INIT( Operation, Opid )
#define OPERATION_OPEN( Operation, Capture )      __OPERATION_OPEN( Operation, Capture )
#define OPERATION_CLOSE( Operation, Opid )        __OPERATION_CLOSE( Operation, Opid )
#define OPERATION_GET_CAPTURE( Operation )        __OPERATION_GET_CAPTURE( Operation )
#define OPERATION_POP_CAPTURE( Operation )        __OPERATION_POP_CAPTURE( Operation )
#define OPERATION_GET_OPID( Operation )           __OPERATION_GET_OPID( Operation )
#define OPERATION_SET_OPID( Operation, Opid )     __OPERATION_SET_OPID( Operation, Opid )





#define WAIT_FOR_EMITTER_OPSTREAM_READY( Graph, TimeoutMilliseconds ) \
  do {                                                                \
    /* SAFE HERE */                                                   \
    int16_t __prewait_recursion__ = (Graph)->__state_lock_count;      \
    while( (Graph)->__state_lock_count > 1 ) {                        \
      __leave_CS( Graph );                                            \
    }                                                                 \
    (Graph)->__state_lock_count--; /* down to 0 */                    \
    /* Release one lock and sleep until condition or timeout */       \
    TIMED_WAIT_CONDITION_CS( &((Graph)->OP.emitter.opstream_ready.cond), &((Graph)->state_lock.lock), TimeoutMilliseconds );  \
    /* One lock now re-acquired */                                    \
    /* SAFE HERE */                                                   \
    (Graph)->__state_lock_count++; /* up to 1 */                      \
    while( (Graph)->__state_lock_count < __prewait_recursion__ ) {    \
      __enter_CS( Graph );                                            \
    }                                                                 \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static double __refresh_data_rate( int64_t lifetime_bytes ) {
  
  static __THREAD int64_t sample_t0 = 0;
  static __THREAD int64_t sample_t1 = 0;
  static __THREAD int64_t running_bytes = 0;
  static __THREAD double running_rate = 0.0;
  static __THREAD int64_t zero_delta_n_count = 0;

  // Current time and initialize t0 if needed
  sample_t1 = __GET_CURRENT_NANOSECOND_TICK();
  if( sample_t0 == 0 ) {
    sample_t0 = sample_t1;
  }

  // Compute data rate every 1/4 second
  int64_t delta_t_ns = sample_t1 - sample_t0;
  if( delta_t_ns > 250000000 ) {
    // Compute bytes/second
    double delta_t = delta_t_ns / 1.0e9;
    int64_t delta_n = lifetime_bytes - running_bytes;
    double bytes_per_second = delta_n > 0 ? delta_n / delta_t : 0.0;

    // Compute smoothed rate (goes to zero after many consecutive zero samples)
    zero_delta_n_count = delta_n > 0 ? 0 : zero_delta_n_count + 1;
    running_rate = zero_delta_n_count > 20 ? 0.0 : (0.875 * running_rate + 0.125 * bytes_per_second);
    
    // Update state
    sample_t0 = sample_t1;
    running_bytes = lifetime_bytes;
  }
  return running_rate;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_string( char **p, const char *str ) {
  char c;
  while( (c = *str++) != '\0' ) {
    *(*p)++ = c;
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_format( char **p, const char *format, ... ) {
  va_list args;
  va_start( args, format );
  int n = vsprintf( *p, format, args );
  if( n > 0 ) {
    (*p) += n;
  }
  va_end( args );
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_objectid( char **p, const char *prefix, const objectid_t *obid, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }
  *p = write_hex_objectid( *p, obid );
  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_integer( char **p, const char *prefix, int64_t value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }

  (*p) += sprintf( *p, "%lld", value );

  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_real( char **p, const char *prefix, double value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }

  (*p) += sprintf( *p, "%.8e", value );

  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_qword( char **p, const char *prefix, QWORD value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }
  *p = write_HEX_qword( *p, value );
  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_dword( char **p, const char *prefix, DWORD value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }
  *p = write_HEX_dword( *p, value );
  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_word( char **p, const char *prefix, WORD value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }
  *p = write_HEX_word( *p, value );
  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_byte( char **p, const char *prefix, BYTE value, const char *suffix ) {
  if( prefix ) {
    __writebuf_string( p, prefix );
  }
  *p = write_HEX_byte( *p, value );
  if( suffix ) {
    __writebuf_string( p, suffix );
  }
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_op_objectid( char **p, const objectid_t *obid ) {
  __writebuf_string( p, SEP );
  *p = write_hex_objectid( *p, obid );
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_op_qword( char **p, QWORD value ) {
  __writebuf_string( p, SEP );
  *p = write_HEX_qword( *p, value );
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_op_dword( char **p, DWORD value ) {
  __writebuf_string( p, SEP );
  *p = write_HEX_dword( *p, value );
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_op_word( char **p, WORD value ) {
  __writebuf_string( p, SEP );
  *p = write_HEX_word( *p, value );
  return *p;
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __writebuf_op_byte( char **p, BYTE value ) {
  __writebuf_string( p, SEP );
  *p = write_HEX_byte( *p, value );
  return *p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char *__full_path_parser( vgx_OperationParser_t *parser ) {
  vgx_Graph_t *graph = parser->op_graph ? parser->op_graph : parser->parent;
  return graph ? CALLABLE( graph )->FullPath( graph ) : "?";
}





// OPERATION
DLL_HIDDEN extern int _vxdurable_operation__trap_error( struct s_vgx_Graph_t *graph, bool fatal, int errcode, const char *msg );
DLL_HIDDEN extern int _vxdurable_operation__set_dirty( vgx_Operation_t *operation );
DLL_HIDDEN extern int _vxdurable_operation__is_dirty( const vgx_Operation_t *operation );



// CAPTURE
DLL_HIDDEN extern void _vxdurable_operation_capture__dump_capture_object_CS( vgx_OperatorCapture_t *capture );
DLL_HIDDEN extern int64_t _vxdurable_operation_capture__commit_CS( struct s_vgx_Graph_t *graph, vgx_Operation_t *operation, bool hold_CS );
DLL_HIDDEN extern int64_t _vxdurable_operation_capture__close_CS( struct s_vgx_Graph_t *graph, vgx_Operation_t *operation, bool hold_CS );
DLL_HIDDEN extern int64_t _vxdurable_operation_capture__graph_commit_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int64_t _vxdurable_operation_capture__graph_close_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_new_CS( vgx_Vertex_t *vertex, vgx_Vertex_constructor_args_t *args );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_delete_CS( vgx_Vertex_t *vertex );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_set_rank_WL( vgx_Vertex_t *vertex, const vgx_Rank_t *rank );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_change_type_WL( vgx_Vertex_t *vertex, vgx_vertex_type_t new_type );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_set_tmx_WL( vgx_Vertex_t *vertex, uint32_t tmx );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_convert_WL( vgx_Vertex_t *vertex, vgx_VertexStateContext_man_t manifestation );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_set_property_WL( vgx_Vertex_t *vertex, const vgx_VertexProperty_t *prop );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_del_property_WL( vgx_Vertex_t *vertex, shortid_t key );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_del_properties_WL( vgx_Vertex_t *vertex );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_set_vector_WL( vgx_Vertex_t *vertex, const vgx_Vector_t *vector );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_del_vector_WL( vgx_Vertex_t *vertex );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_remove_outarcs_WL( vgx_Vertex_t *vertex, int64_t n_removed );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertex_remove_inarcs_WL( vgx_Vertex_t *vertex, int64_t n_removed );
DLL_HIDDEN extern int _vxdurable_operation_capture__arc_connect_WL( vgx_Arc_t *arc );
DLL_HIDDEN extern int _vxdurable_operation_capture__arc_disconnect_WL( vgx_Arc_t *arc, int64_t n_removed );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_attach_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_detach_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_clear_registry_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_create_graph_SYS_CS( struct s_vgx_Graph_t *SYSTEM, const objectid_t *obid, const char *name, const char *path, int vertex_block_order, uint32_t graph_t0, int64_t start_opcount, vgx_Similarity_config_t *simconfig );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_delete_graph_SYS_CS( struct s_vgx_Graph_t *SYSTEM, const objectid_t *obid );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_tick_SYS_CS( struct s_vgx_Graph_t *graph, int64_t tms );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_send_comment_SYS_CS( struct s_vgx_Graph_t *SYSTEM, CString_t *CSTR__comment );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_send_simple_aux_command_SYS_CS( struct s_vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, CString_t *CSTR__command );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_forward_aux_command_SYS_CS( struct s_vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, objectid_t cmd_id, vgx_StringList_t *cmd_data );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_send_raw_data_SYS_CS( struct s_vgx_Graph_t *SYSTEM, const char *data, int64_t dlen );
DLL_HIDDEN extern int _vxdurable_operation_capture__system_clone_graph_SYS_CS( struct s_vgx_Graph_t *SYSTEM, struct s_vgx_Graph_t *source );

DLL_HIDDEN extern int _vxdurable_operation_capture__graph_truncate_CS( struct s_vgx_Graph_t *graph, vgx_vertex_type_t vxtype, int64_t n_discarded );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_persist_CS( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts, bool force );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_state_CS( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_assert_state_CS( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_get_base_counts_ROG( struct s_vgx_Graph_t *graph, vgx_graph_base_counts_t *counts );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_readonly_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_readwrite_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_events_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_noevents_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__graph_event_exec_CS( struct s_vgx_Graph_t *graph, uint32_t ts, uint32_t tmx );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertices_acquire_CS_WL( struct s_vgx_Graph_t *graph, vgx_VertexList_t *vertices_WL, CString_t **CSTR__error );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertices_release_CS_LCK( struct s_vgx_Graph_t *graph, vgx_VertexList_t *vertices_LCK );
DLL_HIDDEN extern int _vxdurable_operation_capture__vertices_release_all_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_add_vertextype_CS( struct s_vgx_Graph_t *graph, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_remove_vertextype_CS( struct s_vgx_Graph_t *graph, QWORD typehash, vgx_vertex_type_t typecode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_add_relationship_CS( struct s_vgx_Graph_t *graph, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_remove_relationship_CS( struct s_vgx_Graph_t *graph, QWORD relhash, vgx_predicator_rel_enum relcode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_add_dimension_CS( vgx_Similarity_t *similarity, QWORD dimhash, const CString_t *CSTR__dimension, feature_vector_dimension_t dimcode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_remove_dimension_CS( vgx_Similarity_t *similarity, QWORD dimhash, feature_vector_dimension_t dimcode );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_add_propertykey_CS( struct s_vgx_Graph_t *graph, shortid_t keyhash, const CString_t *CSTR__key );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_remove_propertykey_CS( struct s_vgx_Graph_t *graph, shortid_t keyhash );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_add_stringvalue_CS( struct s_vgx_Graph_t *graph, const objectid_t *obid, const CString_t *CSTR__string );
DLL_HIDDEN extern int _vxdurable_operation_capture__enumerator_remove_stringvalue_CS( struct s_vgx_Graph_t *graph, const objectid_t *obid );



// Emitter
DLL_HIDDEN extern int                _vxdurable_operation_emitter__initialize_CS( struct s_vgx_Graph_t *graph, int64_t init_opid );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__is_initialized_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__start_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__stop_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__destroy_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__is_running_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__suspend_CS( struct s_vgx_Graph_t *graph, int timeout_ms );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__is_suspended_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__resume_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__is_ready_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern vgx_Operation_t *  _vxdurable_operation_emitter__next_operation_CS( struct s_vgx_Graph_t *graph, vgx_Operation_t *closed_operation, const vgx_OperationCaptureInheritable_t *inheritable, bool hold_CS );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__return_operator_capture_to_pool_OPEN( vgx_OperatorCapture_t **pcapture );
DLL_HIDDEN extern bool               _vxdurable_operation_emitter__wait_available_graph_operation_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int64_t            _vxdurable_operation_emitter__next_operation_id_CS( vgx_OperationEmitter_t *emitter_CS );
DLL_HIDDEN extern int64_t            _vxdurable_operation_emitter__submit_to_pending_CS( vgx_OperatorCapture_t **pcapture );
DLL_HIDDEN extern bool               _vxdurable_operation_emitter__has_pending_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int64_t            _vxdurable_operation_emitter__get_pending_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern bool               _vxdurable_operation_emitter__has_pending_OPEN( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int                _vxdurable_operation_emitter__fence_CS( struct s_vgx_Graph_t *graph, int64_t opid, int timeout_ms );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__enable_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__disable_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern bool               _vxdurable_operation_emitter__is_enabled_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__set_opmuted_CS( struct s_vgx_Graph_t *self );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__clear_opmuted_CS( struct s_vgx_Graph_t *self );
DLL_HIDDEN extern bool               _vxdurable_operation_emitter__is_opmuted_CS( struct s_vgx_Graph_t *self );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__heartbeat_enable_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__heartbeat_disable_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern int64_t            _vxdurable_operation_emitter__inflight_capacity_CS( struct s_vgx_Graph_t *graph );
DLL_HIDDEN extern void               _vxdurable_operation_emitter__dump_queues_CS( struct s_vgx_Graph_t *graph, const vgx_OperatorCapture_t *capture );


// Produce OP
DLL_HIDDEN extern int                _vxdurable_operation_produce_op__emit_OPEN( vgx_OperationEmitter_t *emitter, int64_t tms, vgx_OperatorCapture_t *capture );
DLL_HIDDEN extern int                _vxdurable_operation_produce_op__emit_json_OPEN( vgx_OperationEmitter_t *emitter, int64_t tms, vgx_OperatorCapture_t *capture );


// System
DLL_HIDDEN extern vgx_OperationCounters_t  _vxdurable_operation_system__counters_outstream_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern vgx_OperationCounters_t  _vxdurable_operation_system__counters_instream_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern void                     _vxdurable_operation_system__reset_counters_OPEN( vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern vgx_OperationBacklog_t   _vxdurable_operation_system__get_output_backlog_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                      _vxdurable_operation_system__initialize_CS( struct s_vgx_Graph_t *graph, vgx_OperationSystem_t *system );
DLL_HIDDEN extern int                      _vxdurable_operation_system__start_agent_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                      _vxdurable_operation_system__suspend_agent_SYS_CS( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
DLL_HIDDEN extern int                      _vxdurable_operation_system__is_agent_suspended_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                      _vxdurable_operation_system__resume_agent_SYS_CS( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
DLL_HIDDEN extern int                      _vxdurable_operation_system__stop_agent_SYS_CS( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int64_t                  _vxdurable_operation_system__pending_transactions_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int64_t                  _vxdurable_operation_system__pending_bytes_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                      _vxdurable_operation_system__start_consumer_service_OPEN( vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold );
DLL_HIDDEN extern unsigned                 _vxdurable_operation_system__bound_port_consumer_service_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                      _vxdurable_operation_system__stop_consumer_service_OPEN( struct s_vgx_Graph_t *SYSTEM );

// Parser
DLL_HIDDEN extern int          _vxdurable_operation_parser__initialize_OPEN( struct s_vgx_Graph_t *graph, vgx_OperationParser_t *parser, bool start_thread );
DLL_HIDDEN extern int          _vxdurable_operation_parser__destroy_OPEN( vgx_OperationParser_t *parser );
DLL_HIDDEN extern int64_t      _vxdurable_operation_parser__submit_data( vgx_OperationParser_t *parser, vgx_ByteArrayList_t *list, CString_t **CSTR__error );
DLL_HIDDEN extern void         _vxdurable_operation_parser__enable_validation( vgx_OperationParser_t *parser, bool enable );
DLL_HIDDEN extern void         _vxdurable_operation_parser__enable_execution( vgx_OperationParser_t *parser, bool enable );
DLL_HIDDEN extern void         _vxdurable_operation_parser__silent_skip_regression( vgx_OperationParser_t *parser, bool silent_skip );
DLL_HIDDEN extern void         _vxdurable_operation_parser__enable_crc( vgx_OperationParser_t *parser, bool enable );
DLL_HIDDEN extern void         _vxdurable_operation_parser__enable_strict_serial( vgx_OperationParser_t *parser, bool enable );
DLL_HIDDEN extern int64_t      _vxdurable_operation_parser__get_pending( vgx_OperationParser_t *parser );

DLL_HIDDEN extern int          _vxdurable_operation_parser__suspend( struct s_vgx_Graph_t *self, int timeout_ms );
DLL_HIDDEN extern int          _vxdurable_operation_parser__is_suspended( struct s_vgx_Graph_t *self );
DLL_HIDDEN extern int          _vxdurable_operation_parser__resume( struct s_vgx_Graph_t *self );
DLL_HIDDEN extern int          _vxdurable_operation_parser__add_opcode_filter( int64_t opcode_filter );
DLL_HIDDEN extern int          _vxdurable_operation_parser__remove_opcode_filter( int64_t opcode_filter );
DLL_HIDDEN extern int          _vxdurable_operation_parser__apply_opcode_profile( int64_t profile_id );
DLL_HIDDEN extern unsigned int _vxdurable_operation_parser__checksum( const char *data );
DLL_HIDDEN extern int64_t      _vxdurable_operation_parser__feed_operation_data_OPEN( vgx_OperationParser_t *parser, const char *input, const char **next, WAITABLE_TIMER *Timer, vgx_OperationCounters_t *counters, CString_t **CSTR__error, vgx_op_parser_error_t *perr );
DLL_HIDDEN extern int          _vxdurable_operation_parser__reset_OPEN( vgx_OperationParser_t *parser );


// Consumer Service
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__suspend_tx_execution_OPEN( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__is_suspended_tx_execution_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__resume_tx_execution_OPEN( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__is_initializing_tx_execution_OPEN( vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__suspend_OPEN( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__is_suspended_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__is_durable_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__resume_OPEN( struct s_vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__initialize_SYS_CS( struct s_vgx_Graph_t *SYSTEM, vgx_TransactionalConsumerService_t *consumer_service, uint16_t port, bool durable, int64_t snapshot_threshold );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__initialize_OPEN( struct s_vgx_Graph_t *SYSTEM, vgx_TransactionalConsumerService_t *consumer_service, uint16_t port, bool durable, int64_t snapshot_threshold );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__destroy_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__destroy_OPEN( vgx_TransactionalConsumerService_t *consumer_service );
DLL_HIDDEN extern CString_t *  _vxdurable_operation_consumer_service__get_input_uri_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service );
DLL_HIDDEN extern CString_t *  _vxdurable_operation_consumer_service__get_provider_uri_SYS_CS( vgx_TransactionalConsumerService_t *consumer_service );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__subscribe_OPEN( struct s_vgx_Graph_t *SYSTEM, const char *host, uint16_t port, bool hardsync, int timeout_ms, CString_t **CSTR__error );
DLL_HIDDEN extern int          _vxdurable_operation_consumer_service__unsubscribe_OPEN( struct s_vgx_Graph_t *SYSTEM );







#endif
