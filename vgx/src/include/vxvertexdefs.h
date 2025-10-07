/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxvertexdefs.h
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

#ifndef VGX_VXVERTEXDEFS_H
#define VGX_VXVERTEXDEFS_H

#include "vxbase.h"



/*******************************************************************//**
 * vgx_VertexWriterThreadId_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexWriterThreadId_t {
  VERTEX_WRITER_THREADID_NONE             = 0x00000000,   /* */
  VERTEX_WRITER_THREADID_INVALID          = 0xffffffff    /* */
} vgx_VertexWriterThreadId_t;



/*******************************************************************//**
 * vgx_VertexPropertyIndex_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexPropertyIndex_main_t {
  __VTX_INDEX_main_N                      = 0,
  __VTX_INDEX_main_Y                      = 1,
  VERTEX_PROPERTY_INDEX_MAIN_UNINDEXED    = __VTX_INDEX_main_N,
  VERTEX_PROPERTY_INDEX_MAIN_INDEXED      = __VTX_INDEX_main_Y,
} vgx_VertexPropertyIndex_main_t;


typedef enum e_vgx_VertexPropertyIndex_type_t {
  __VTX_INDEX_type_N                      = 0,
  __VTX_INDEX_type_Y                      = 1,
  VERTEX_PROPERTY_INDEX_TYPE_UNINDEXED    = __VTX_INDEX_type_N,
  VERTEX_PROPERTY_INDEX_TYPE_INDEXED      = __VTX_INDEX_type_Y
} vgx_VertexPropertyIndex_type_t;


typedef enum e_vgx_VertexPropertyIndex_t {
# define _type (1 << 1) // vertex is indexed in type index
# define _main (1 << 0) // vertex is indexed in main index
   /*                                                                                                 main type      */
  VERTEX_PROPERTY_INDEX_UNINDEXED         = __VTX_INDEX_type_N*_type  |  __VTX_INDEX_main_N*_main,  /*  0   0   00   */
  VERTEX_PROPERTY_INDEX_INDEXED_MAIN      = __VTX_INDEX_type_N*_type  |  __VTX_INDEX_main_Y*_main,  /*  0   1   01   */
  VERTEX_PROPERTY_INDEX_INDEXED_TYPE      = __VTX_INDEX_type_Y*_type  |  __VTX_INDEX_main_N*_main,  /*  1   0   10   */
  VERTEX_PROPERTY_INDEX_INDEXED           = __VTX_INDEX_type_Y*_type  |  __VTX_INDEX_main_Y*_main,  /*  1   1   11   */
  /* All other bit combinations are invalid will cause undefined behavior     */
# undef _type
# undef _main
} vgx_VertexPropertyIndex_t;



/*******************************************************************//**
 * vgx_VertexPropertyEvent_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexPropertyEvent_sch_t {
  __VTX_EVENT_sch_N                       = 0,
  __VTX_EVENT_sch_Y                       = 1,
  VERTEX_PROPERTY_EVENT_SCH_NONE          = __VTX_EVENT_sch_N,
  VERTEX_PROPERTY_EVENT_SCH_SCHEDULED     = __VTX_EVENT_sch_Y
} vgx_VertexPropertyEvent_sch_t;


typedef enum e_vgx_VertexPropertyEvent_t {
# define _sch (1 << 0) // vertex is scheduled for event
  /*                                                                     sch   */
  VERTEX_PROPERTY_EVENT_NONE              = __VTX_EVENT_sch_N*_sch,  /*   0    */
  VERTEX_PROPERTY_EVENT_SCHEDULED         = __VTX_EVENT_sch_Y*_sch,  /*   1    */
  /* All other bit combinations are invalid will cause undefined behavior      */
# undef _sch
} vgx_VertexPropertyEvent_t;



/*******************************************************************//**
 * vgx_VertexPropertyScope_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexPropertyScope_t {
  /*                                              local   */
  VERTEX_PROPERTY_SCOPE_GLOBAL            = 0,  /*   0    */
  VERTEX_PROPERTY_SCOPE_LOCAL             = 1   /*   1    */
} vgx_VertexPropertyScope_t;



/*******************************************************************//**
 * vgx_VertexPropertyVector_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexPropertyVector_vec_t {
  __VTX_VECTOR_vec_N                      = 0,
  __VTX_VECTOR_vec_Y                      = 1,
  VERTEX_PROPERTY_VECTOR_VEC_NONE         = __VTX_VECTOR_vec_N,
  VERTEX_PROPERTY_VECTOR_VEC_EXISTS       = __VTX_VECTOR_vec_Y,
} vgx_VertexPropertyVector_vec_t;


typedef enum e_vgx_VertexPropertyVector_ctr_t {
  __VTX_VECTOR_ctr_N                      = 0,
  __VTX_VECTOR_ctr_Y                      = 1,
  VERTEX_PROPERTY_VECTOR_CTR_STANDARD     = __VTX_VECTOR_ctr_N,
  VERTEX_PROPERTY_VECTOR_CTR_CENTROID     = __VTX_VECTOR_ctr_Y
} vgx_VertexPropertyVector_ctr_t;


typedef enum e_vgx_VertexPropertyVector_t {
# define _ctr (1 << 1) // vertex has centroid vector
# define _vec (1 << 0) // vertex has vector
  /*                                                                                                 ctr vec       */
  VERTEX_PROPERTY_VECTOR_NULL             = __VTX_VECTOR_ctr_N*_ctr  |  __VTX_VECTOR_vec_N*_vec,  /*  0   0   00   */
  VERTEX_PROPERTY_VECTOR_STANDARD         = __VTX_VECTOR_ctr_N*_ctr  |  __VTX_VECTOR_vec_Y*_vec,  /*  0   1   01   */
  VERTEX_PROPERTY_VECTOR_CENTROID         = __VTX_VECTOR_ctr_Y*_ctr  |  __VTX_VECTOR_vec_Y*_vec,  /*  1   1   11   */
  /* All other bit combinations are invalid will cause undefined behavior     */
# undef _ctr
# undef _vec
} vgx_VertexPropertyVector_t;



/*******************************************************************//**
 * vgx_VertexPropertyDegree_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexPropertyDegree_out_t {
  __VTX_DEGREE_out_N                      = 0,
  __VTX_DEGREE_out_Y                      = 1,
  VERTEX_PROPERTY_DEGREE_OUT_ZERO         = __VTX_DEGREE_out_N,
  VERTEX_PROPERTY_DEGREE_OUT_NONZERO      = __VTX_DEGREE_out_Y,
} vgx_VertexPropertyDegree_out_t;


typedef enum e_vgx_VertexPropertyDegree_in_t {
  __VTX_DEGREE_in_N                       = 0,
  __VTX_DEGREE_in_Y                       = 1,
  VERTEX_PROPERTY_DEGREE_IN_ZERO          = __VTX_DEGREE_in_N,
  VERTEX_PROPERTY_DEGREE_IN_NONZERO       = __VTX_DEGREE_in_Y,
} vgx_VertexPropertyDegree_in_t;


typedef enum e_vgx_VertexPropertyDegree_t {
# define _in  (1 << 1) // vertex has inarcs
# define _out (1 << 0) // vertex has outarcs
  /*                                                                                                 in  out       */
  VERTEX_PROPERTY_DEGREE_ISOLATED         = __VTX_DEGREE_in_N*_in  |  __VTX_DEGREE_out_N*_out,    /*  0   0   00   */
  VERTEX_PROPERTY_DEGREE_SOURCE           = __VTX_DEGREE_in_N*_in  |  __VTX_DEGREE_out_Y*_out,    /*  0   1   01   */
  VERTEX_PROPERTY_DEGREE_SINK             = __VTX_DEGREE_in_Y*_in  |  __VTX_DEGREE_out_N*_out,    /*  1   0   10   */
  VERTEX_PROPERTY_DEGREE_INTERNAL         = __VTX_DEGREE_in_Y*_in  |  __VTX_DEGREE_out_Y*_out,    /*  1   1   11   */
  /* All other bit combinations are invalid will cause undefined behavior     */
# undef _out
# undef _in
} vgx_VertexPropertyDegree_t;



/*******************************************************************//**
 * vgx_VertexStateLock_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexStateLock_lck_t {
  __VTX_STATE_lck_0                           = 0,
  __VTX_STATE_lck_1                           = 1,
  VERTEX_STATE_LOCK_LCK_OPEN                  = __VTX_STATE_lck_0,
  VERTEX_STATE_LOCK_LCK_LOCKED                = __VTX_STATE_lck_1
} vgx_VertexStateLock_lck_t;


typedef enum e_vgx_VertexStateLock_rwl_t {
  __VTX_STATE_rwl_0                           = 0,
  __VTX_STATE_rwl_1                           = 1,
  VERTEX_STATE_LOCK_RWL_NONE                  = __VTX_STATE_rwl_0,
  VERTEX_STATE_LOCK_RWL_WRITABLE              = __VTX_STATE_rwl_0,
  VERTEX_STATE_LOCK_RWL_READONLY              = __VTX_STATE_rwl_1
} vgx_VertexStateLock_rwl_t;


typedef enum e_vgx_VertexStateLock_wrq_t {
  __VTX_STATE_wrq_0                           = 0,
  __VTX_STATE_wrq_1                           = 1,
  VERTEX_STATE_LOCK_WRQ_NONE                  = __VTX_STATE_wrq_0,
  VERTEX_STATE_LOCK_WRQ_PENDING               = __VTX_STATE_wrq_1
} vgx_VertexStateLock_wrq_t;


typedef enum e_vgx_VertexStateLock_iny_t {
  __VTX_STATE_iny_0                           = 0,
  __VTX_STATE_iny_1                           = 1,
  VERTEX_STATE_LOCK_INY_INARCS_NORMAL         = __VTX_STATE_iny_0,
  VERTEX_STATE_LOCK_INY_INARCS_YIELDED        = __VTX_STATE_iny_1
} vgx_VertexStateLock_iny_t;


typedef enum e_vgx_VertexStateLock_yib_t {
  __VTX_STATE_yib_0                           = 0,
  __VTX_STATE_yib_1                           = 1,
  VERTEX_STATE_LOCK_YIB_INARCS_IDLE           = __VTX_STATE_yib_0,
  VERTEX_STATE_LOCK_YIB_INARCS_BUSY           = __VTX_STATE_yib_1
} vgx_VertexStateLock_yib_t;


typedef enum e_vgx_VertexStateLock_t {
# define _yib (1 << 4) // vertex's yielded inarcs busy (borrowed by another thread)
# define _iny (1 << 3) // vertex inarcs yielded
# define _wrq (1 << 2) // vertex write requested
# define _rwl (1 << 1) // vertex lock mode read or write
# define _lck (1 << 0) // vertex locked
  /* These are the legal states enumerated                                                                                                                                   yib iny wrq rwl lck          */
  VERTEX_STATE_LOCK_IDLE                  = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_0*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_0*_rwl | __VTX_STATE_lck_0*_lck,   /*  0   0   0   0   0   00000   */
  VERTEX_STATE_LOCK_WRITABLE              = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_0*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_0*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   0   0   0   1   00001   */
  VERTEX_STATE_LOCK_READONLY              = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_0*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   0   0   1   1   00011   */
  VERTEX_STATE_LOCK_READONLY_WREQ         = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_0*_iny | __VTX_STATE_wrq_1*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   0   1   1   1   00111   */
  VERTEX_STATE_LOCK_WRITABLE_YIELDIN      = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_0*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   1   0   0   1   01001   */
  VERTEX_STATE_LOCK_WRITABLE_INBUSY       = __VTX_STATE_yib_1*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_0*_rwl | __VTX_STATE_lck_1*_lck,   /*  1   1   0   0   1   11001   */
  VERTEX_STATE_LOCK_READONLY_YIELDIN      = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   1   0   1   1   01011   */
  VERTEX_STATE_LOCK_READONLY_INBUSY       = __VTX_STATE_yib_1*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_0*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  1   1   0   1   1   11011   */
  VERTEX_STATE_LOCK_READONLY_YIELDIN_WREQ = __VTX_STATE_yib_0*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_1*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  0   1   1   1   1   01111   */
  VERTEX_STATE_LOCK_READONLY_INBUSY_WREQ  = __VTX_STATE_yib_1*_yib | __VTX_STATE_iny_1*_iny | __VTX_STATE_wrq_1*_wrq | __VTX_STATE_rwl_1*_rwl | __VTX_STATE_lck_1*_lck,   /*  1   1   1   1   1   11111   */
  /* All other bit combinations are invalid will cause undefined behavior     */
# undef _lck
# undef _rwl
# undef _wrq
# undef _iny
# undef _yib
} vgx_VertexStateLock_t;



/*******************************************************************//**
 * vgx_VertexStateContex_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexStateContext_sus_t {
  __VTX_CTX_sus_N                           = 0, // 0=active ("no")
  __VTX_CTX_sus_Y                           = 1, // 1=suspended ("yes")
  VERTEX_STATE_CONTEXT_SUS_ACTIVE           = __VTX_CTX_sus_N,
  VERTEX_STATE_CONTEXT_SUS_SUSPENDED        = __VTX_CTX_sus_Y
} vgx_VertexStateContext_sus_t;


typedef enum e_vgx_VertexStateContext_man_t {
  __VTX_CTX_man_NULL                        = 0x0,  // Null vertex: a vertex that does not exist in graph
  __VTX_CTX_man_REAL                        = 0x1,  // Real vertex: a normal vertex
  __VTX_CTX_man_VIRT                        = 0x2,  // Virtual vertex: exists only as sink for one or more other vertices with outarcs to this vertex
  __VTX_CTX_man_ANY                         = 0x3,  // FOR PROBES ONLY: Both real and virtual will match probe
  VERTEX_STATE_CONTEXT_MAN_NULL             = __VTX_CTX_man_NULL,
  VERTEX_STATE_CONTEXT_MAN_REAL             = __VTX_CTX_man_REAL,
  VERTEX_STATE_CONTEXT_MAN_VIRTUAL          = __VTX_CTX_man_VIRT,
  VERTEX_STATE_CONTEXT_MAN_ANY              = __VTX_CTX_man_ANY
} vgx_VertexStateContext_man_t;


typedef enum e_vgx_VertexStateContext_t {
# define _man (1 << 1) // vertex manifestation
# define _sus (1 << 0) // vertex suspended
  /*                                                                                              man sus      */
  VERTEX_STATE_CONTEXT_NULL                 = __VTX_CTX_man_NULL*_man | __VTX_CTX_sus_N*_sus,  /*  00  0  000  */
  VERTEX_STATE_CONTEXT_PRE_REAL             = __VTX_CTX_man_REAL*_man | __VTX_CTX_sus_Y*_sus,  /*  01  1  011  */
  VERTEX_STATE_CONTEXT_REAL                 = __VTX_CTX_man_REAL*_man | __VTX_CTX_sus_N*_sus,  /*  01  0  010  */
  VERTEX_STATE_CONTEXT_PRE_VIRTUAL          = __VTX_CTX_man_VIRT*_man | __VTX_CTX_sus_Y*_sus,  /*  10  1  101  */
  VERTEX_STATE_CONTEXT_VIRTUAL              = __VTX_CTX_man_VIRT*_man | __VTX_CTX_sus_N*_sus,  /*  10  0  100  */
  /* All other bit combinations are invalid will cause undefined behavior     */
# undef _sus
# undef _man
} vgx_VertexStateContext_t;



static const char *__reverse_manifestation_map[] = {
  "NULL",     // 00
  "REAL",     // 01
  "VIRTUAL",  // 10
  "ANY"       // 11
};

__inline static const char * _vgx_manifestation_as_string( const vgx_VertexStateContext_man_t man ) {
  return __reverse_manifestation_map[ man & 3 ];
}




/*******************************************************************//**
 * vgx_VertexTypeEnumeration_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexTypeEnumeration_t {
  /* NONE / WILDCARD */
    VERTEX_TYPE_ENUMERATION_NONE                = 0x00,   //  0000 0000
    VERTEX_TYPE_ENUMERATION_WILDCARD            = 0x00,   //  0000 0000

  /* Reserved Range (15 entries) */
  __VERTEX_TYPE_ENUMERATION_START_RSV_RANGE     = 0x01,   //  0000 0001
    VERTEX_TYPE_ENUMERATION_TEST1               = 0x0a,   //  0000 1010
    VERTEX_TYPE_ENUMERATION_TEST2               = 0x0b,   //  0000 1011
    VERTEX_TYPE_ENUMERATION_TEST3               = 0x0c,   //  0000 1100
  __VERTEX_TYPE_ENUMERATION_END_RSV_RANGE       = 0x0f,   //  0000 1111

  /* System Range (16 entries) */
  __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE     = 0x10,   //  0001 0000
    VERTEX_TYPE_ENUMERATION_VERTEX              = 0x11,   //  0001 0001
    VERTEX_TYPE_ENUMERATION_LOCKOBJECT          = 0x12,   //  0001 0010
    VERTEX_TYPE_ENUMERATION_NONEXIST            = 0x19,   //  0001 1001
    VERTEX_TYPE_ENUMERATION_DEFUNCT             = 0x1d,   //  0001 1101
    VERTEX_TYPE_ENUMERATION_INCOMPLETE          = 0x1e,   //  0001 1110
    VERTEX_TYPE_ENUMERATION_NOACCESS            = 0x1f,   //  0001 1111
  __VERTEX_TYPE_ENUMERATION_END_SYS_RANGE       = 0x1f,   //  0001 1111

  /* User Range (208 entries) */
  __VERTEX_TYPE_ENUMERATION_START_USER_RANGE    = 0x20,   //  0010 0000
  __VERTEX_TYPE_ENUMERATION_END_USER_RANGE      = 0xef,   //  1110 1111
  __VERTEX_TYPE_ENUMERATION_USER_RANGE_SIZE     = __VERTEX_TYPE_ENUMERATION_END_USER_RANGE - __VERTEX_TYPE_ENUMERATION_START_USER_RANGE + 1,

  /* Exception Range (16 entries) */
  __VERTEX_TYPE_ENUMERATION_START_EXC_RANGE     = 0xf0,   //  1111 0000
    VERTEX_TYPE_ENUMERATION_NO_MAPPING          = 0xf1,   //  1111 0001
    VERTEX_TYPE_ENUMERATION_INVALID             = 0xf2,   //  1111 0010
    VERTEX_TYPE_ENUMERATION_LOCKED              = 0xf9,   //  1111 1001
    VERTEX_TYPE_ENUMERATION_COLLISION           = 0xfc,   //  1111 1100
    VERTEX_TYPE_ENUMERATION_ERROR               = 0xfe,   //  1111 1110 
  __VERTEX_TYPE_ENUMERATION_END_EXC_RANGE       = 0xff,   //  1111 1111

  /* */
  VERTEX_TYPE_ENUMERATION_MAX_ENTRIES           = 0x100,

} vgx_VertexTypeEnumeration_t;


DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_NONE_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_WILDCARD_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_VERTEX_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_SYSTEM_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_UNKNOWN_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VERTEX_TYPE_ENUMERATION_LOCKOBJECT_STRING;



/**************************************************************************//**
 * __vertex_type_enumeration_in_user_range
 *
 ******************************************************************************
 */
__inline static bool __vertex_type_enumeration_in_user_range( vgx_vertex_type_t vertex_type ) {
  return vertex_type >= __VERTEX_TYPE_ENUMERATION_START_USER_RANGE 
         &&
         vertex_type <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE
         ? true : false;
}




/**************************************************************************//**
 * __vertex_type_in_user_range
 *
 ******************************************************************************
 */
__inline static bool __vertex_type_in_user_range( int vertex_type ) {
  return (bool)(vertex_type >= (int)__VERTEX_TYPE_ENUMERATION_START_USER_RANGE && vertex_type <= (int)__VERTEX_TYPE_ENUMERATION_END_USER_RANGE);
}




/**************************************************************************//**
 * __vertex_type_enumeration_default
 *
 ******************************************************************************
 */
__inline static bool __vertex_type_enumeration_default( int vertex_type ) {
  return vertex_type == (int)VERTEX_TYPE_ENUMERATION_VERTEX;
}




/**************************************************************************//**
 * __vertex_type_enumeration_lockobject
 *
 ******************************************************************************
 */
__inline static bool __vertex_type_enumeration_lockobject( int vertex_type ) {
  return vertex_type == (int)VERTEX_TYPE_ENUMERATION_LOCKOBJECT;
}




/**************************************************************************//**
 * __vertex_type_enumeration_valid
 *
 ******************************************************************************
 */
__inline static bool __vertex_type_enumeration_valid( int vertex_type ) {
  return vertex_type == VERTEX_TYPE_ENUMERATION_WILDCARD || __vertex_type_enumeration_default( vertex_type ) || __vertex_type_enumeration_lockobject( vertex_type ) || __vertex_type_in_user_range( vertex_type );
}




/*******************************************************************//**
 * vgx_VertexSemaphoreCount_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexSemaphoreCount_t {
  VERTEX_SEMAPHORE_COUNT_ZERO               = 0x00,   //  0000 0000
  VERTEX_SEMAPHORE_COUNT_ONE                = 0x01,   //  0000 0001
  VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT   = 0x70,   //  0111 0000 (112)
  VERTEX_SEMAPHORE_COUNT_READERS_LIMIT      = 0x70,   //  0111 0000 (112)
  VERTEX_SEMAPHORE_COUNT_MAX                = 0x7f    //  0111 1111 (127)
} vgx_VertexSemaphoreCount_t;




/*******************************************************************//**
 * vgx_VertexDescriptor_t
 *
 ***********************************************************************
 */
typedef union u_vgx_VertexDescriptor_t {
  struct {
    /* writer */
    struct {
      DWORD threadid;     // Vertex ThreadID (current vertex owner for writing / write-requestor)
    } writer;

    /* property */
    union {
      uint8_t bits;

      // index
      union {
        struct {
          uint8_t bits : 2;
          uint8_t ___u : 6;
        };
        struct {
          uint8_t main  : 1;  // 1 = Vertex indexed in main index
          uint8_t type  : 1;  // 1 = Vertex indexed in type index
          uint8_t __6   : 6;  // * NO ACCESS *
        };
      } index;

      // event
      union {
        struct {
          uint8_t ___l : 2;
          uint8_t bits : 1;
          uint8_t ___u : 5;
        };
        struct {
          uint8_t __2 : 2;  // * NO ACCESS *
          uint8_t sch : 1;  // 1 = Vertex event scheduled
          uint8_t __5 : 5;  // * NO ACCESS *
        };
      } event;

      // scope
      union {
        struct {
          uint8_t ___l : 3;
          uint8_t bits : 1;
          uint8_t ___u : 4;
        };
        struct {
          uint8_t __3 : 3;  // * NO ACCESS *
          uint8_t def : 1;  // 1 = Vertex scope is local only, 0 = Vertex scope is global
          uint8_t __4 : 4;  // * NO ACCESS *
        };
      } scope;

      // vector
      union {
        struct {
          uint8_t ___l : 4;
          uint8_t bits : 2;
          uint8_t ___u : 2;
        };
        struct {
          uint8_t __4 : 4;  // * NO ACCESS *
          uint8_t vec : 1;  // 1 = Vertex has feature vector
          uint8_t ctr : 1;  // 1 = Feature vector is a centroid
          uint8_t __2 : 2;  // * NO ACCESS *
        };
      } vector;

      // degree
      union {
        struct {
          uint8_t ___l : 6;
          uint8_t bits : 2;
        };
        struct {
          uint8_t __6 : 6;  // * NO ACCESS *
          uint8_t out : 1;  // 1 = Vertex has one or more outarcs
          uint8_t in  : 1;  // 1 = Vertex has one or more inarcs
        };
      } degree;
    } property;

    /* state */
    union {
      uint8_t bits;

      // lock
      union {
        struct {
          uint8_t bits : 5;
          uint8_t ___u : 3;
        };
        struct {
          uint8_t lck : 1;  // 1 = Vertex is locked
          uint8_t rwl : 1;  // 0 = WRITABLE, 1 = READONLY
          uint8_t wrq : 1;  // 1 = Write requested, requesting thread entered in writer.threadid
          uint8_t iny : 1;  // 1 = Vertex inarcs yielded despite being locked
          uint8_t yib : 1;  // 1 = Yielded inarcs borrowed by other thread
          uint8_t __3 : 3;  // * NO ACCESS */
        };
      } lock;

      // context
      union {
        struct {
          uint8_t ___l : 5;
          uint8_t bits : 3;
        };
        struct {
          uint8_t __5 : 5;  // * NO ACCESS */
          uint8_t sus : 1;  // 1 = Vertex suspended, 0=normal
          uint8_t man : 2;  // 1 = Vertex manifestation (NULL, REAL, VIRT), see definition of vgx_VertexStateContext_man_t
        };
      } context;
    } state;

    /* type */
    union {
      uint8_t bits;
      vgx_vertex_type_t enumeration;
    } type;

    /* semaphore */
    union {
      uint8_t bits;
      int8_t count;
    } semaphore;
  };

  /* */
  QWORD bits;

} vgx_VertexDescriptor_t;



/*******************************************************************//**
 * vgx_DescriptorFieldOffset_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_DescriptorFieldOffset_t {
  VERTEX_OFFSET_WRITER                  = 0,
  VERTEX_OFFSET_WRITER_THREADID         = 0,
  VERTEX_OFFSET_PROPERTY                = 32,
  VERTEX_OFFSET_PROPERTY_INDEX          = 0,
  VERTEX_OFFSET_PROPERTY_INDEX_MAIN     = 0,
  VERTEX_OFFSET_PROPERTY_INDEX_TYPE     = 1,
  VERTEX_OFFSET_PROPERTY_EVENT          = 2,
  VERTEX_OFFSET_PROPERTY_EVENT_SCH      = 0,
  VERTEX_OFFSET_PROPERTY_FIELDS         = 3,
  VERTEX_OFFSET_PROPERTY_FIELDS_FLD     = 0,
  VERTEX_OFFSET_PROPERTY_VECTOR         = 4,
  VERTEX_OFFSET_PROPERTY_VECTOR_VEC     = 0,
  VERTEX_OFFSET_PROPERTY_VECTOR_CTR     = 1,
  VERTEX_OFFSET_PROPERTY_DEGREE         = 6,
  VERTEX_OFFSET_PROPERTY_DEGREE_OUT     = 0,
  VERTEX_OFFSET_PROPERTY_DEGREE_IN      = 1,
  VERTEX_OFFSET_STATE                   = 40,
  VERTEX_OFFSET_STATE_LOCK              = 0,
  VERTEX_OFFSET_STATE_LOCK_LCK          = 0,
  VERTEX_OFFSET_STATE_LOCK_RWL          = 1,
  VERTEX_OFFSET_STATE_LOCK_WRQ          = 2,
  VERTEX_OFFSET_STATE_LOCK_INY          = 3,
  VERTEX_OFFSET_STATE_LOCK_YIB          = 4,
  VERTEX_OFFSET_STATE_CONTEXT           = 5,
  VERTEX_OFFSET_STATE_CONTEXT_SUS       = 0,
  VERTEX_OFFSET_STATE_CONTEXT_MAN       = 1,
  VERTEX_OFFSET_TYPE                    = 48,
  VERTEX_OFFSET_TYPE_ENUMERATION        = 0,
  VERTEX_OFFSET_SEMAPHORE               = 52,
  VERTEX_OFFSET_SEMAPHORE_COUNT         = 0,
} vgx_DescriptorFieldOffset_t;

//   sem     enum  ctx lck property           writer thread
// 0-------EEEEEEEEXXX-----PPPPPPPP--------------------------------
//                         iocvfetm
// 0000000011111111111000001111101100000000000000000000000000000000
// 0   0   F   F   E   0   F   B   0   0   0   0   0   0   0   0   
const static QWORD VERTEX_DESCRIPTOR_NON_EPHEMERAL_DATA_MASK = 0x00FFE0FB00000000ULL;


DLL_VISIBLE extern vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_NULL( void );
DLL_VISIBLE extern vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_REAL( vgx_VertexTypeEnumeration_t vxtype );
DLL_VISIBLE extern vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_VIRTUAL( vgx_VertexTypeEnumeration_t vxtype );



/*******************************************************************//**
 * vgx_VertexProperty_t
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexProperty_t {
  CString_t *key;
  shortid_t keyhash;
  vgx_value_t val;
  vgx_value_condition_t condition; // for use in queries
} vgx_VertexProperty_t;


#define VGX_SYSTEM_TYPE_PREFIX        "sys_"
#define VGX_SYSTEM_VERTEX_PREFIX      VGX_SYSTEM_TYPE_PREFIX "::"
#define VGX_SYSTEM_PROPERTY_PREFIX    VGX_SYSTEM_TYPE_PREFIX "."

#define VGX_VERTEX_ONDELETE_PROPERTY  VGX_SYSTEM_PROPERTY_PREFIX "__del__"


/*******************************************************************//**
 * vgx_SelectProperties_t
 *
 ***********************************************************************
 */
typedef struct s_vgx_SelectProperties_t {
  int64_t len;
  vgx_VertexProperty_t *properties;
} vgx_SelectProperties_t;





#endif
