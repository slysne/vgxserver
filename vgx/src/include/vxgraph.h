/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph.h
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

#ifndef VGX_VXGRAPH_H
#define VGX_VXGRAPH_H

#include "vxbase.h"
#include "vxvertexdefs.h"

struct s_vgx_Graph_t;
struct s_vgx_Similarity_t;
struct s_vgx_Fingerprinter_t;
struct s_vgx_Vector_t;
struct s_vgx_Vertex_t;
struct s_vgx_Arc_t;
struct s_vgx_virtual_ArcFilter_context_t;
struct s_vgx_VertexFilter_context_t;
struct s_vgx_ArcCollector_context_t;
struct s_vgx_VertexCollector_context_t;

struct s_vgx_Vertex_constructor_args_t;
struct s_vgx_Graph_constructor_args_t;
union u_vgx_Similarity_config_t;

struct s_vgx_SearchResult_t;
struct s_vgx_BaseCollector_context_t;
struct s_vgx_Evaluator_t;
struct s_vgx_ExpressEvalMemory_t;
struct s_vgx_ExpressEvalWorkRegisters_t;

typedef const void * vgx_Comparable_t;




DLL_HIDDEN extern void Graph_RegisterClass( void );
DLL_HIDDEN extern void Graph_UnregisterClass( void );




/*******************************************************************//**
 * vgx_Fingerprinter_t
 *
 ***********************************************************************
 */
#define MAX_FP_PARTS 8
#define MIN_FP_PARTS 3

/* VTABLE */
typedef struct s_vgx_Fingerprinter_vtable_t {
  /* base methods */
  COMLIB_VTABLE_HEAD
  /* Fingerprinter methods */
  int (*Distance)( const struct s_vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );
  FP_t (*Compute)( const struct s_vgx_Fingerprinter_t *self, struct s_vgx_Vector_t *vector, int64_t seed, FP_t *rlcm );
  FP_t (*ComputeBytearray)( const struct s_vgx_Fingerprinter_t *self, const BYTE *bytes, int sz, int64_t seed, FP_t *rlcm );
  char * (*Projections)( const struct s_vgx_Fingerprinter_t *self, char buffer321[], const struct s_vgx_Vector_t *vector, FP_t lsh, FP_t lcm, WORD seed, int ksize, bool reduce, bool expand );
  int (*SegmValid)( const struct s_vgx_Fingerprinter_t *self, int nsegm, int nsign );
  int (*PnoValid)( const struct s_vgx_Fingerprinter_t *self, int pno, int nsegm, int nsign );
  int (*PureRegions)( const struct s_vgx_Fingerprinter_t *self, FP_t fp1, FP_t fp2 );

} vgx_Fingerprinter_vtable_t;

/* DATA */
typedef struct s_vgx_Fingerprinter_t {
  // CL1
  /* base members */
  COMLIB_OBJECT_HEAD( vgx_Fingerprinter_vtable_t )
  /* Fingerprinter members */
  unsigned char use_segm;
  unsigned char nsegm;
  unsigned char nsign;
  unsigned char _rsv_[5];
  unsigned char shifts[8];
  unsigned char bits[8];
  vgx_vector_dimension_encoder_t dimencode;
  const struct s_vgx_Similarity_t *parent;
  QWORD _pad[1];
  // CL2
  FP_t masks[8];
} vgx_Fingerprinter_t;


typedef struct s_vgx_Fingerprinter_constructor_args_t {
  int nsegm;
  int nsign;
  vgx_vector_dimension_encoder_t dimension_encoder;
  const struct s_vgx_Similarity_t *parent;
} vgx_Fingerprinter_constructor_args_t;



/*******************************************************************//**
 * vgx_Vector_t
 *
 ***********************************************************************
 */


typedef union u_vgx_VectorContext_t {
  cxmalloc_metaflex_t _M;
  struct {
    void *elements;
    struct s_vgx_Similarity_t *simobj;
  };
} vgx_VectorContext_t;



typedef union s_vgx_VectorFlags_t {
  uint8_t bits;
  struct {
    uint8_t nul  : 1; /* null vector */
    uint8_t pop  : 1; /* populated */
    uint8_t ext  : 1; /* external */
    uint8_t ecl  : 1; /* euclidean */
    uint8_t _r5  : 1; /* */
    uint8_t _r6  : 1; /* */
    uint8_t _r7  : 1; /* */
    uint8_t eph  : 1; /* ephemeral */
  };
  struct {
    uint8_t bits  : 4;
    uint8_t _5678 : 4;
  } compat;
  struct {
    uint8_t ____1 : 1;
    uint8_t bits  : 3;
    uint8_t _5678 : 4;
  } compat_nul;
} vgx_VectorFlags_t;


typedef union u_vgx_VectorMetas_t {
  QWORD qword;
  struct {
    // Q3.-------x
    vgx_VectorFlags_t flags;
    // Q3.------x-
    uint8_t type;
    // Q3.----xx--
    uint16_t vlen;
    // Q3.xxxx----
    union {
      // Vector magnitude used with feature vectors
      float norm;
      // Element scaling factor used with Euclidean vectors
      float factor;

      float XXX_TODO_SCALAR;

      // Bits used for (de)serialization
      DWORD bits;
    } scalar;
  };
} vgx_VectorMetas_t;


/* VTABLE */
typedef struct s_vgx_Vector_vtable_t {
  /* base methods */
  COMLIB_VTABLE_HEAD
  /* Vector methods */
  int64_t (*Incref)( struct s_vgx_Vector_t *self );
  int64_t (*Decref)( struct s_vgx_Vector_t *self );
  int64_t (*Refcnt)( const struct s_vgx_Vector_t *self );
  struct s_vgx_Vector_t * (*Set)( struct s_vgx_Vector_t *self, int nelem, float scale, const void *elements );
  struct s_vgx_Vector_t * (*Clone)( const struct s_vgx_Vector_t *self, bool ephemeral );
  struct s_vgx_Vector_t * (*OwnOrClone)( const struct s_vgx_Vector_t *self, bool ephemeral );
  struct s_vgx_Vector_t * (*Copy)( const struct s_vgx_Vector_t *self, struct s_vgx_Vector_t *other );
  void * (*Elements)( const struct s_vgx_Vector_t *self );
  union u_vgx_VectorContext_t * (*Context)( const struct s_vgx_Vector_t *self );
  int (*Length)( const struct s_vgx_Vector_t *self );
  float (*Magnitude)( const struct s_vgx_Vector_t *self );
  float (*Scaler)( const struct s_vgx_Vector_t *self );
  bool (*IsNull)( const struct s_vgx_Vector_t *self );
  bool (*IsInternal)( const struct s_vgx_Vector_t *self );
  bool (*IsCentroid)( const struct s_vgx_Vector_t *self );
  bool (*IsExternal)( const struct s_vgx_Vector_t *self );
  bool (*IsEphemeral)( const struct s_vgx_Vector_t *self );
  bool (*IsEuclidean)( const struct s_vgx_Vector_t *self );
  FP_t (*Fingerprint)( const struct s_vgx_Vector_t *self );
  objectid_t (*Identity)( const struct s_vgx_Vector_t *self );
  char * (*ToBuffer)( const struct s_vgx_Vector_t *self, int max_sz, char **buffer );
  CString_t * (*ShortRepr)( const struct s_vgx_Vector_t *self );

} vgx_Vector_vtable_t;



#define __VGX_VECTOR_HEAD_T                                 \
  /* base members */                                        \
  COMLIB_OBJECT_HEAD( vgx_Vector_vtable_t ) /*  2 QWORDS */ \
  /* Vector members */                                      \
  /* Note: simcontext and reserved data in allocator metaflex __m128i field */ \
  vgx_VectorMetas_t metas;                  /*  1 QWORD */  \
  FP_t fp;                                  /*  1 QWORD */


/* The vector object */
/* THIS _MUST_ BE EXACTLY 1/2 CACHELINE = 32 bytes */
/* It is the 2nd half of the first cacheline allocated */
typedef struct s_vgx_VectorHead_t {
  __VGX_VECTOR_HEAD_T
} vgx_VectorHead_t;


/* The official vector object */
typedef struct s_vgx_Vector_t {
  __VGX_VECTOR_HEAD_T
} vgx_Vector_t;


/* The allocated vector object */
typedef struct s_vgx_AllocatedVector_t {
  union {
    cxmalloc_header_t allocator;
    struct {
      vgx_VectorContext_t context;
      __m128i __internal;
    };
  };
  __VGX_VECTOR_HEAD_T
} vgx_AllocatedVector_t;



typedef struct s_vgx_Vector_constructor_args_t {
  uint16_t vlen;
  bool ephemeral;
  vector_type_t type;
  const void *elements;
  float scale;
  struct s_vgx_Similarity_t *simcontext;
} vgx_Vector_constructor_args_t;


DLL_HIDDEN extern vgx_Vector_constructor_args_t * centroid_constructor_args( vgx_Vector_constructor_args_t *cargs, uint16_t max_vlen, const vgx_Vector_t *vectors[] );

DLL_HIDDEN extern int _vxsim__compare_vectors( const vgx_Vector_t *A, const vgx_Vector_t *B );



typedef struct s_vgx_RankElement_t {
  DWORD b8  : 8;
  DWORD c24 : 24;
} vgx_RankElement_t;


/*******************************************************************//**
 * vgx_Rank_t
 *
 ***********************************************************************
 */
typedef union u_vgx_Rank_t {
  QWORD bits;
  struct {
    vgx_RankElement_t slope;
    vgx_RankElement_t offset;
  };
} vgx_Rank_t;


#define vgx_RankGetC0( Rank )  REINTERPRET_CAST_FLOAT_24( (Rank)->offset.c24 )
#define vgx_RankGetC1( Rank )  REINTERPRET_CAST_FLOAT_24( (Rank)->slope.c24 )
#define vgx_RankGetLongitude vgx_RankGetC0
#define vgx_RankGetLatitude vgx_RankGetC1


/**************************************************************************//**
 * __vgx_RankSetC
 *
 ******************************************************************************
 */
__inline static void __vgx_RankSetC( vgx_RankElement_t *e, float c ) {
  float_bits_t B = { .f = c };
  e->c24 = B.f24;
}
#define vgx_RankSetC0( Rank, C0 ) __vgx_RankSetC( &(Rank)->offset, C0 )
#define vgx_RankSetC1( Rank, C1 ) __vgx_RankSetC( &(Rank)->slope, C1 )
#define vgx_RankSetLongitude( Rank, Longitude ) vgx_RankSetC0( Rank, Longitude )
#define vgx_RankSetLatitude( Rank, Latitude ) vgx_RankSetC1( Rank, Latitude )
#define vgx_RankSetLatLon( Rank, Lat, Lon ) do { vgx_RankSetLatitude( Rank, Lat ); vgx_RankSetLongitude( Rank, Lon ); } WHILE_ZERO

#define vgx_RankGetB0( Rank ) (Rank)->offset.b8;
#define vgx_RankGetB1( Rank ) (Rank)->slope.b8;
#define vgx_RankSetB0( Rank, B0 ) ((Rank)->offset.b8 = (BYTE)(B0))
#define vgx_RankSetB1( Rank, B1 ) ((Rank)->slope.b8 = (BYTE)(B1))

#define vgx_RankGetANNSeedNumber vgx_RankGetB1
#define vgx_RankSetANNSeedNumber( Rank, SeedNumber ) vgx_RankSetB1( Rank, SeedNumber )
#define vgx_RankGetANNArcLSHRotate vgx_RankGetB0
#define vgx_RankSetANNArcLSHRotate( Rank, RR ) vgx_RankSetB0( Rank, RR )



/**************************************************************************//**
 * __vgx_Rank_INIT
 *
 ******************************************************************************
 */
__inline static vgx_Rank_t __vgx_Rank_INIT( float c1, float c0 ) {
  vgx_Rank_t rank;
  vgx_RankSetC1( &rank, c1 );
  vgx_RankSetC0( &rank, c0 );
  vgx_RankSetB1( &rank, 0 );
  vgx_RankSetB0( &rank, 0 );
  return rank;
}
#define vgx_Rank_INIT() __vgx_Rank_INIT( 1.0f, 0.0f )
#define vgx_Rank_INIT_SET( C1, C0 ) __vgx_Rank_INIT( C1, C0 )



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef uint8_t vgx_eventproc_state_t;




typedef union u_eventproc_parameters_t {
  QWORD bits;
  struct {
    // 32
    int operation_timeout_ms;
    // 16
    struct {
      vgx_eventproc_state_t state;
      struct {
        uint8_t req_flush   : 1;
        uint8_t __rsv_2     : 1;
        uint8_t __rsv_3     : 1;
        uint8_t __rsv_4     : 1;
        uint8_t ack_flushed : 1;
        uint8_t __rsv_6     : 1;
        uint8_t __rsv_7     : 1;
        uint8_t defunct     : 1;
      } flags;
    } task_WL;
    // 16
    union {
      uint16_t __bits;
      struct {
        uint16_t __g1         : 1;
        uint16_t __g2         : 1;
        uint16_t __g3         : 1;
        uint16_t __g4         : 1;
        uint16_t __g5         : 1;
        uint16_t __g6         : 1;
        uint16_t __g7         : 1;
        uint16_t __g8         : 1;
        uint16_t __g9         : 1;
        uint16_t __g10        : 1;
        uint16_t __g11        : 1;
        uint16_t __g12        : 1;
        uint16_t __g13        : 1;
        uint16_t __g14        : 1;
        uint16_t __g15        : 1;
        uint16_t __g16        : 1;
      };
    } graph_CS;
  };
} vgx_eventproc_parameters_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef union u_vgx_EventValue_t {
  uint64_t bits;  // 56-bit usable
  int64_t ival;  // 56-bit usable
  struct {
    uint32_t ts_exec;   // Execution time for the event (seconds since 1970)
    uint16_t metas;     // Arbitrary metadata for event
    uint8_t  type;      // Event type code
    uint8_t  __na;      // Not available
  };
} vgx_EventValue_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef union __u_VertexStorableEvent_t {
  __m128i m128;
  struct {
    cxmalloc_handle_t event_key;  // Vertex allocator handle
    vgx_EventValue_t event_val;
  };
} vgx_VertexStorableEvent_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_event_none( vgx_VertexStorableEvent_t *ev ) {
  return ev->event_val.type == VGX_EVENTPROC_NO_EVENT;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_expiration_event( vgx_VertexStorableEvent_t *ev ) {
  return ev->event_val.type == VGX_EVENTPROC_VERTEX_EXPIRATION_EVENT;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_event_removal( vgx_VertexStorableEvent_t *ev ) {
  return ev->event_val.type == VGX_EVENTPROC_VERTEX_REMOVE_SCHEDULE;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef Cm128iList_t vgx_VertexEventList_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef Cm128iQueue_t vgx_VertexEventQueue_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef Cm128iHeap_t vgx_VertexEventHeap_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_EventScheduleInfo_t {
  int64_t insertion_threshold_tms;
  int64_t migration_cycle_tms;
  int64_t migration_margin_tms;
  int8_t map_order;
  int partials;
  int64_t partial_interval_tms;
} vgx_EventScheduleInfo_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_EventParamInfo_t {
  vgx_EventScheduleInfo_t Executor;
  vgx_EventScheduleInfo_t ShortTerm;
  vgx_EventScheduleInfo_t MediumTerm;
  vgx_EventScheduleInfo_t LongTerm;
} vgx_EventParamInfo_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_EventBacklogInfo_t {
  bool filled;
  int64_t n_api;        // number of events in API input queue
  int64_t n_input;      // number of events in monitor's input queue
  int64_t n_long;       // number of events in long schedule
  int64_t n_med;        // number of events in medium schedule
  int64_t n_short;      // number of events in short schedule
  int64_t n_current;    // number of events currently being transferred for execution
  int64_t n_exec;       // number of completed events
  float ontime_rate;    // ontime execution
  struct {
    int is_running;  // event monitor thread running
    int is_paused;   // event monitor temporarily paused
  } flags;
  vgx_EventParamInfo_t param;
} vgx_EventBacklogInfo_t;



/*******************************************************************//**
 * vgx_Vertex_t
 * 
 * 
 ***********************************************************************
 */

// 40 bytes
//
typedef union u_vgx_VertexIdentifierPrefix_t {
  QWORD qwords[5];
  char data[40];
} vgx_VertexIdentifierPrefix_t;


// 48 bytes
//
typedef struct s_vgx_VertexIdentifier_t {
  vgx_VertexIdentifierPrefix_t idprefix;
  CString_t *CSTR__idstr;
} vgx_VertexIdentifier_t;


// 64 bytes
//
typedef struct s_vgx_VertexCompleteIdentifier_t {
  vgx_VertexIdentifier_t identifier;
  objectid_t internalid;
} vgx_VertexCompleteIdentifier_t;




/*******************************************************************//**
 * vgx_Operation_t
 *
 ***********************************************************************
 */
typedef tptr_t vgx_Operation_t;


typedef enum e_vgx_VertexOnDeleteAction {
  VertexOnDeleteAction_None,
  VertexOnDeleteAction_RemoveFile,
  VertexOnDeleteAction_LogMessage
} vgx_VertexOnDeleteAction;


typedef void (*f_vgx_VertexOnDelete)( struct s_vgx_Vertex_t *self_WL, const char *data );




/* VTABLE */
typedef struct s_vgx_Vertex_vtable_t {

  /* base methods */
  COMLIB_VTABLE_HEAD
  /* Vertex methods */
  //
  void (*Initialize_CS)( struct s_vgx_Vertex_t *self_WL );
  void (*Virtualize_CS)( struct s_vgx_Vertex_t *self_WL );

  //
  struct s_vgx_Graph_t * (*Parent)( struct s_vgx_Vertex_t *self_RO );
  bool (*IsStable_CS)( const struct s_vgx_Vertex_t *self_OPEN );
  bool (*Readonly)( const struct s_vgx_Vertex_t *self_OPEN );
  bool (*Readable)( const struct s_vgx_Vertex_t *self_OPEN );
  bool (*Writable)( const struct s_vgx_Vertex_t *self_OPEN );

  // Rank
  vgx_Rank_t (*SetRank)( struct s_vgx_Vertex_t *self_WL, float c1, float c0 );
  vgx_Rank_t (*GetRank)( const struct s_vgx_Vertex_t *self_RO );

  // Degree
  int64_t (*Degree)( const struct s_vgx_Vertex_t *self_RO );
  int64_t (*InDegree)( const struct s_vgx_Vertex_t *self_RO );
  int64_t (*OutDegree)( const struct s_vgx_Vertex_t *self_RO );

  // Arcs
  int64_t (*RemoveOutarcs)( struct s_vgx_Vertex_t *self_WL );
  int64_t (*RemoveInarcs)( struct s_vgx_Vertex_t *self_WL );
  int64_t (*RemoveArcs)( struct s_vgx_Vertex_t *self_WL );

  // Neighbors
  struct s_vgx_BaseCollector_context_t * (*Neighbors)( struct s_vgx_Vertex_t *self_RO );
  struct s_vgx_BaseCollector_context_t * (*Initials)( struct s_vgx_Vertex_t *self_RO );
  struct s_vgx_BaseCollector_context_t * (*Terminals)( struct s_vgx_Vertex_t *self_RO );

  // Names
  size_t (*IDLength)( const struct s_vgx_Vertex_t *self_RO );
  const vgx_VertexIdentifier_t * (*Identifier)( const struct s_vgx_Vertex_t *self_RO );
  const char * (*IDString)( const struct s_vgx_Vertex_t *self_RO );
  const char * (*IDPrefix)( const struct s_vgx_Vertex_t *self_RO );
  CString_t * (*IDCString)( const struct s_vgx_Vertex_t *self_RO );
  objectid_t (*InternalID)( const struct s_vgx_Vertex_t *self_RO );
  const vgx_vertex_type_t (*Type)( const struct s_vgx_Vertex_t *self_RO );
  const CString_t * (*TypeName)( const struct s_vgx_Vertex_t *self_RO );
  const CString_t * (*TypeName_CS)( const struct s_vgx_Vertex_t *self_RO );

  // Properties
  int (*SetProperty)( struct s_vgx_Vertex_t *self_WL, struct s_vgx_VertexProperty_t *prop );
  int (*SetPropertyKeyVal)( struct s_vgx_Vertex_t *self_WL, const char *key, vgx_value_t *value );
  int (*SetIntProperty)( struct s_vgx_Vertex_t *self_WL, const char *key, int64_t intval );
  int (*SetRealProperty)( struct s_vgx_Vertex_t *self_WL, const char *key, double realval );
  int (*SetStringProperty)( struct s_vgx_Vertex_t *self_WL, const char *key, const char *strval );
  int (*FormatStringProperty)( struct s_vgx_Vertex_t *self_WL, const char *key, const char *valfmt, ... );
  int (*SetOnDelete)( struct s_vgx_Vertex_t *self_WL, vgx_VertexOnDeleteAction action, const char *data );

  struct s_vgx_VertexProperty_t * (*IncProperty)( struct s_vgx_Vertex_t *self_WL, struct s_vgx_VertexProperty_t *prop );
  int32_t (*VertexEnum)( struct s_vgx_Vertex_t *self_LCK );
  struct s_vgx_VertexProperty_t * (*GetInternalAttribute)( struct s_vgx_Vertex_t *self_RO, struct s_vgx_VertexProperty_t *prop );
  
  struct s_vgx_VertexProperty_t * (*GetProperty)( struct s_vgx_Vertex_t *self_RO, struct s_vgx_VertexProperty_t *prop );
  int (*GetPropertyValue)( struct s_vgx_Vertex_t *self_RO, const char *key, vgx_value_t *rvalue );
  int (*GetIntProperty)( struct s_vgx_Vertex_t *self_RO, const char *key, int64_t *intval );
  int (*GetRealProperty)( struct s_vgx_Vertex_t *self_RO, const char *key, double *realval );
  int (*GetStringProperty)( struct s_vgx_Vertex_t *self_RO, const char *key, CString_t **CSTR__strval );

  struct s_vgx_SelectProperties_t * (*GetProperties)( struct s_vgx_Vertex_t *self_RO );
  bool (*HasProperty)( const struct s_vgx_Vertex_t *self_RO, const struct s_vgx_VertexProperty_t *prop );
  bool (*HasPropertyKey)( const struct s_vgx_Vertex_t *self_RO, const char *key );
  bool (*HasProperties)( struct s_vgx_Vertex_t *self_RO );
  int64_t (*NumProperties)( struct s_vgx_Vertex_t *self_RO );
  int (*RemoveProperty)( struct s_vgx_Vertex_t *self_WL, struct s_vgx_VertexProperty_t *prop );
  int (*RemovePropertyKey)( struct s_vgx_Vertex_t *self_WL, const char *key );
  int64_t (*RemoveProperties)( struct s_vgx_Vertex_t *self_WL );

  // Timestamps
  bool (*IsExpired)( const struct s_vgx_Vertex_t *self_RO );
  uint32_t (*CreationTime)( const struct s_vgx_Vertex_t *self_RO );
  uint32_t (*ModificationTime)( const struct s_vgx_Vertex_t *self_RO );
  uint32_t (*GetExpirationTime)( const struct s_vgx_Vertex_t *self_RO );
  int (*SetExpirationTime)( struct s_vgx_Vertex_t *self_WL, uint32_t new_tmx );

  // Vector
  int (*SetVector)( struct s_vgx_Vertex_t *self_WL, struct s_vgx_Vector_t **pvector );
  struct s_vgx_Vector_t * (*GetVector)( struct s_vgx_Vertex_t *self_RO );
  int (*RemoveVector)( struct s_vgx_Vertex_t *self_WL );

  // Misc
  CStringQueue_t * (*Descriptor)( struct s_vgx_Vertex_t *self_RO, CStringQueue_t *output );
  void (*PrintVertexAllocator)( struct s_vgx_Vertex_t *self_RO );
  CString_t * (*ShortRepr)( const struct s_vgx_Vertex_t *self_RO );

} vgx_Vertex_vtable_t;





#define __VGX_VERTEX_HEAD_T                                 \
  /* base members           */                              \
  /* [3] vtable             */                              \
  /* [Q1.5] COMLIB BASE     */                              \
  /* [4] object typeinfo    */                              \
  /* [Q1.6] COMLIB BASE     */                              \
  COMLIB_OBJECT_HEAD( vgx_Vertex_vtable_t )                 \
  /* Vertex members         */                              \
  /* Note: ID stored in allocator metaflex __m128i field */ \
  /* [5] operation          */                              \
  /* [Q1.7]                 */                              \
  vgx_Operation_t operation;                                \
  /* [6] descriptor         */                              \
  /* [Q1.8]                 */                              \
  vgx_VertexDescriptor_t descriptor;


/* DATA 1 - the allocator header */
/* THIS _MUST_ BE EXACTLY 1/2 CACHELINE = 32 BYTES! */
typedef struct s_vgx_VertexHead_t {
  __VGX_VERTEX_HEAD_T
} vgx_VertexHead_t;


typedef union u_vgx_vertex_expiration_t {
  QWORD bits;
  struct {
    uint32_t vertex_ts;
    uint32_t arc_ts;
  };
} vgx_vertex_expiration_t;


#define __VGX_VERTEX_DATA_T                                 \
  /* [7] graph              */                              \
  /* [Q2.1]                 */                              \
  struct s_vgx_Graph_t *graph;                              \
  /* [8] vector             */                              \
  /* [Q2.2]                 */                              \
  vgx_Vector_t *vector;                                     \
  /* [9] rank               */                              \
  /* [Q2.3]                 */                              \
  vgx_Rank_t rank;                                          \
  /* [10] inarcs            */                              \
  /* [Q2.4-5]               */                              \
  union u_vgx_ArcVector_cell_t inarcs;                      \
  /* [11] outarcs           */                              \
  /* [Q2.6-7]               */                              \
  union u_vgx_ArcVector_cell_t outarcs;                     \
  /* [12] properties        */                              \
  /* [Q2.8]                 */                              \
  framehash_cell_t *properties;                             \
  /* [13] [14] identifier   */                              \
  /* [Q3.1-6]               */                              \
  vgx_VertexIdentifier_t identifier;                        \
  /* [15]                   */                              \
  /* [Q3.7]                 */                              \
  vgx_vertex_expiration_t TMX;                              \
  /* [16]                   */                              \
  /* [Q3.8.1]               */                              \
  uint32_t TMC;                                             \
  /* [17]                   */                              \
  /* [Q3.8.2]               */                              \
  uint32_t TMM;



/* DATA 2 - the type of the allocator element (single) */
typedef struct s_vgx_VertexData_t {
  __VGX_VERTEX_DATA_T
} vgx_VertexData_t;


/* Unified vertex view */
typedef struct s_vgx_Vertex_t {
  __VGX_VERTEX_HEAD_T
  __VGX_VERTEX_DATA_T
} vgx_Vertex_t;


typedef struct s_vgx_AllocatedVertex_t {
  cxmalloc_header_t allocator;
  __VGX_VERTEX_HEAD_T
  __VGX_VERTEX_DATA_T
} vgx_AllocatedVertex_t;




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexList_t {
  int64_t sz;
  vgx_Vertex_t **vertex;
} vgx_VertexList_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexIdentifiers_t {
  int64_t sz;
  struct s_vgx_Graph_t *graph;
  vgx_VertexCompleteIdentifier_t *ids; /* --\   */
  QWORD __pad[5];                      /*   |   */
  /* DATA ALLOCATED HERE                    |   */
  /* ...                                    V   */
  /* ...                                        */
  /* ...                                        */
} vgx_VertexIdentifiers_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IVertex_t {
  struct {
    vgx_VertexList_t * (*New)( int64_t sz );
    void (*Delete)( vgx_VertexList_t **vertices );
    int64_t (*Truncate)( vgx_VertexList_t *vertices, int64_t n );
    vgx_Vertex_t * (*Get)( vgx_VertexList_t *vertices, int64_t i );
    const objectid_t * (*GetObid)( const vgx_VertexList_t *vertices, int64_t i );
    vgx_Operation_t * (*GetOperation)( vgx_VertexList_t *vertices, int64_t i );
    vgx_Vertex_t * (*Set)( vgx_VertexList_t *vertices, int64_t i, vgx_Vertex_t *vertex );
    int64_t (*Size)( const vgx_VertexList_t *vertices );
  } List;
  struct {
    vgx_VertexIdentifiers_t * (*New)( struct s_vgx_Graph_t *graph, int64_t sz );
    vgx_VertexIdentifiers_t * (*NewPair)( struct s_vgx_Graph_t *graph, const char *A, const char *B );
    vgx_VertexIdentifiers_t * (*NewObidPair)( struct s_vgx_Graph_t *graph, const objectid_t *obidA, const objectid_t *obidB );
    vgx_VertexIdentifiers_t * (*NewObids)( struct s_vgx_Graph_t *graph, const objectid_t *obid_list, int64_t sz );
    void (*Delete)( vgx_VertexIdentifiers_t **identifiers );
    int64_t (*Truncate)( vgx_VertexIdentifiers_t *identifiers, int64_t n );
    vgx_Vertex_t * (*GetVertex)( vgx_VertexIdentifiers_t *identifiers, int64_t i );
    objectid_t (*GetObid)( vgx_VertexIdentifiers_t *identifiers, int64_t i );
    const char * (*GetId)( vgx_VertexIdentifiers_t *identifiers, int64_t i );
    CString_t * (*SetCString)( vgx_VertexIdentifiers_t *identifiers, int64_t i, CString_t *CSTR__id );
    const char * (*SetId)( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id );
    const char * (*SetIdLen)( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id, int len );
    objectid_t (*SetObid)( vgx_VertexIdentifiers_t *identifiers, int64_t i, const objectid_t *obid );
    const vgx_Vertex_t * (*SetVertex)( vgx_VertexIdentifiers_t *identifiers, int64_t i, const vgx_Vertex_t *vertex );
    int64_t (*Size)( const vgx_VertexIdentifiers_t *identifiers );
    bool (*CheckUnique)( vgx_VertexIdentifiers_t *identifiers );
  } Identifiers;
} vgx_IVertex_t;




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexRef_t {
  vgx_Vertex_t *vertex;
  int refcnt;
  struct {
    int8_t locked;
    int8_t state;
    int8_t __rsv1;
    int8_t __rsv2;
  } slot;
} vgx_VertexRef_t;

typedef int8_t vgx_VertexRefLock_t;



/*******************************************************************//**
 * vgx_ArcHead_t
 * 
 ***********************************************************************
 */
typedef union  u_vgx_ArcHead_t {
  struct {
    QWORD qwords[2];
  } data;
  struct {
    vgx_Vertex_t *vertex;
    vgx_predicator_t predicator;
  };
} vgx_ArcHead_t;




#define BASE_ARC        \
  /* i- */              \
  vgx_Vertex_t *tail;   \
  /* -(p)->t */         \
  vgx_ArcHead_t head;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_VISIBLE extern void __trap_fatal_vertex_corruption( const vgx_Vertex_t *vertex );




/*******************************************************************//**
 * vgx_Arc_t
 *
 * Internal representation of a directed edge from vertex A to vertex B
 *
 * A-[pred]->B
 *
 * tail:        A     pointer to initial vertex
 * head:        B     pointer to terminal vertex
 * predicator:  pred  relationship specification
 *                    mode  : 8-bit relationship mode specifier
 *                    rel   : 16-bit relationship enumeration code
 *                    value : 32-bit general-purpose value associated with the relationship
 * 
 ***********************************************************************
 */
typedef struct s_vgx_Arc_t {
  BASE_ARC
} vgx_Arc_t;


#define VGX_ARCHEAD_INIT( Predicator, HeadPtr ) {   \
  .vertex = (HeadPtr),                              \
  .predicator = (Predicator)                        \
}


#define VGX_ARCHEAD_INIT_PREDICATOR_BITS( PredicatorBits, HeadPtr ) {    \
  .vertex = (HeadPtr),                                  \
  .predicator = { .data = (PredicatorBits) }            \
}


#define VGX_ARC_INIT( TailPtr, Predicator, HeadPtr ) {    \
  .tail = (TailPtr),                                      \
  .head = VGX_ARCHEAD_INIT( Predicator, HeadPtr )         \
}


#define VGX_ARC_INIT_PREDICATOR_BITS( TailPtr, PredicatorBits, HeadPtr ) {  \
  .tail = (TailPtr),                                                        \
  .head = VGX_ARCHEAD_INIT_PREDICATOR_BITS( PredicatorBits, HeadPtr )       \
}


//#define VGX_COPY_ARC( DestArcPtr, SrcArcPtr )  memcpy( DestArcPtr, SrcArcPtr, sizeof(vgx_Arc_t) )
#define VGX_COPY_ARC( DestArcPtr, SrcArcPtr ) (*(DestArcPtr) = *(SrcArcPtr))



/**************************************************************************//**
 * _vgx_arc_set_distance
 *
 ******************************************************************************
 */
__inline static void _vgx_arc_set_distance( vgx_Arc_t *arc, int distance ) {
  if( distance == 1 && arc->head.vertex == arc->tail ) {
    distance = 0;
  }
  _vgx_predicator_eph_set_distance( &arc->head.predicator, distance );
}



/*******************************************************************//**
 * vgx_LockableArc_t
 *
 * An arc that includes a lock counter, used for queries that may
 * require the arc to be locked at multiple independent layers.
 * To avoid locking/unlocking the head vertex with all the associated
 * Enter/Leave CS and possible waits to re-acquire, the first layer
 * that requires lock will do the acquisition and all subsequent layers
 * that require lock will just increment the safe counter. When leaving
 * a layer the safe counter is decremented. It the safe counter goes
 * to zero the head vertex lock is released.
 * 
 * If locking is not required for the head vertex the safe counter will
 * be initialized to a positive number, preventing any acquisition/release.
 * 
 ***********************************************************************
 */
typedef struct s_vgx_LockableArc_t {
  BASE_ARC
  _vgx_ArcVector_cell_type ctype;
  struct {
    vgx_VertexRefLock_t tail_lock;
    vgx_VertexRefLock_t head_lock;
  } acquired;
  struct {
    int8_t __rsv1;
    int8_t __rsv2;
  } flag;
#ifdef VGX_CONSISTENCY_CHECK
#endif
} vgx_LockableArc_t;



#define VGX_LOCKABLE_ARC_INIT( TailPtr, TailLocked, Predicator, HeadPtr, HeadLocked ) {   \
  .tail = (TailPtr),                                              \
  .head = {                                                       \
    .vertex = (HeadPtr),                                          \
    .predicator = (Predicator),                                   \
  },                                                              \
  .acquired = {                                                   \
    .tail_lock = TailLocked,                                      \
    .head_lock = HeadLocked                                       \
  }                                                               \
}



#define VGX_LOCKABLE_ARC_INIT_PREDICATOR_BITS( TailPtr, PredicatorBits, HeadPtr ) { \
  .tail = (TailPtr),                                      \
  .head = {                                               \
    .vertex = (HeadPtr),                                  \
    .predicator = { .data = (PredicatorBits) },           \
  },                                                      \
  .acquired = {                                           \
    .tail_lock = 0,                                       \
    .head_lock = 0                                        \
  }                                                       \
}



/*******************************************************************//**
 * vgx_Relationship_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_Relationship_t {
  CString_t *CSTR__name;
  vgx_predicator_rel_enum rel_enum;
  vgx_predicator_modifier_enum mod_enum;
  vgx_predicator_val_t value;
} vgx_Relationship_t;



/*******************************************************************//**
 * vgx_RelationshipVertex_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_RelationshipVertex_t {
  vgx_Vertex_t *vertex_WL;
  CString_t *CSTR__name;
} vgx_RelationshipVertex_t;



/*******************************************************************//**
 * vgx_Relation_t
 * 
 * (initial)-[relationship,modifier,value]->(terminal)
 * 
 * External facing specification of a relationship between two vertices in
 * a graph. The relation is a directed edge (arc) from the initial vertex (tail)
 * to the terminal vertex (head).  
 *
 * Vertices are identified by name (0-terminated string). An internal ID
 * will be generated from the name. If the given identifier is a 32-character
 * hex string it will be used directly as the internal ID.
 * 
 * The relationship is identified by name (0-terminated string). An integer
 * enumeration will be generated internally by mapping the name to an arbitrarily
 * chosen integer value. The details of the mapping and the enumeration value
 * are hidden from the outside. No external assumptions should be made about 
 * the enumerated relationship value, and it can change internally without notice.
 * 
 * The modifier specifies the nature of the relationship.
 * A relationship can be static, counted, have intensity or cost, be subject
 * to time-based expiration, etc.
 *
 * The value provides numeric data supporting modes that require additional
 * data, and is interpreted according to the specified mode.
 * 
 ***********************************************************************
 */
typedef struct s_vgx_Relation_t {
  // A-(relationship)->B
  //
  struct s_vgx_Graph_t *graph;
  vgx_RelationshipVertex_t initial;
  vgx_RelationshipVertex_t terminal;
  vgx_Relationship_t relationship;
} vgx_Relation_t;




/*******************************************************************//**
 * vgx_collector_mode_t
 *
 ***********************************************************************
 */


__inline static vgx_collector_mode_t _vgx_collector_mode_collect( vgx_collector_mode_t mode ) {
  return (vgx_collector_mode_t)((int)mode & __VGX_COLLECTOR_MODE_COLLECT_MASK);
}



/**************************************************************************//**
 * _vgx_collector_mode_is_deep_collect
 *
 ******************************************************************************
 */
__inline static bool _vgx_collector_mode_is_deep_collect( vgx_collector_mode_t mode ) {
  return ((int)mode & VGX_COLLECTOR_MODE_DEEP_COLLECT) ? true : false;
}



/**************************************************************************//**
 * _vgx_collector_mode_type
 *
 ******************************************************************************
 */
__inline static vgx_collector_mode_t _vgx_collector_mode_type( vgx_collector_mode_t mode ) {
  return (vgx_collector_mode_t)((int)mode & __VGX_COLLECTOR_MODE_TYPE_MASK);
}



/*******************************************************************//**
 * vgx_ArcCondition_t
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ArcCondition_t {
  bool positive;
  struct s_vgx_Graph_t *graph;
  const CString_t *CSTR__relationship;
  vgx_predicator_mod_t modifier;
  vgx_value_comparison vcomp;
  vgx_predicator_val_t value1;
  vgx_predicator_val_t value2;
} vgx_ArcCondition_t;

DLL_VISIBLE extern const vgx_ArcCondition_t DEFAULT_ARC_CONDITION;



/*******************************************************************//**
 * vgx_ArcConditionSet_t
 * 
 * Holds a set of arc conditions that are combined with specified boolean
 * logic. The set may include zero, one, or more arc conditions.
 * If accept is true the arc is accepted when condition(s) are met.
 * If accept is false the arc is rejected when condition(s) are met.
 *
 * No conditions
 * -------------
 * Interpreted as full wildcard, i.e. match anything if positive==true.
 * Setting positive=false makes the condition match nothing.
 *   set = NULL
 *   simple[0] = NULL
 *   simple[1] = NULL
 *   elem = <ignored>
 *
 * One condition
 * -------------
 * Only a single arc condition applied. Logic is ignored.
 *   set = &simple
 *   simple[0] = &elem
 *   simple[1] = NULL
 *   elem = <the arc condition>
 *
 * Multiple conditions
 * -------------------
 * Multiple conditions are applied, combined with the specified logic.
 *   set = <NULL-terminated array of arc condition pointers>
 *   simple[0] = NULL
 *   simple[1] = NULL
 *   elem = <ignored>
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ArcConditionSet_t {
  bool accept;                          //
  vgx_boolean_logic logic;              // Logic to apply when combining multiple arc conditions
  vgx_arc_direction arcdir;          // Arc direction is the same for all elements
  struct s_vgx_Graph_t *graph;          // Parent graph
  vgx_ArcCondition_t **set;             // NULL-terminated list of zero or more pointers to arc conditions that are combined with logic. (set=simple for single condition sets.)
  vgx_ArcCondition_t *simple[2];        // if first [0] element is not NULL it points to our own simple (single condition) element. [1] is always NULL.
  vgx_ArcCondition_t elem;              // Holds a single arc condition, used for sets with only one condition
  CString_t *CSTR__error;               //
} vgx_ArcConditionSet_t;

DLL_VISIBLE extern const vgx_ArcConditionSet_t DEFAULT_ARC_CONDITION_SET;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_DegreeCondition_t {
  vgx_ArcConditionSet_t *arc_condition_set;
  vgx_value_condition_t value_condition;
} vgx_DegreeCondition_t;
DLL_VISIBLE extern const vgx_DegreeCondition_t DEFAULT_VGX_DEGREE_CONDITION;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_SimilarityCondition_t {
  bool positive;
  vgx_Vector_t *probevector;
  vgx_value_condition_t simval_condition;
  vgx_value_condition_t hamval_condition;
} vgx_SimilarityCondition_t;
DLL_VISIBLE extern const vgx_SimilarityCondition_t DEFAULT_VGX_SIMILARITY_CONDITION;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_TimestampCondition_t {
  bool positive;
  vgx_value_condition_t tmc_valcond;
  vgx_value_condition_t tmm_valcond;
  vgx_value_condition_t tmx_valcond;
} vgx_TimestampCondition_t;
DLL_VISIBLE extern const vgx_TimestampCondition_t DEFAULT_VGX_TIMESTAMP_CONDITION;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef int64_t (*f_vgx_PropertyConditionSet_length)( CQwordList_t *properties );
typedef int (*f_vgx_PropertyConditionSet_get)( CQwordList_t *properties, int64_t idx, vgx_VertexProperty_t **prop_addr );
typedef int (*f_vgx_PropertyConditionSet_append)( CQwordList_t *properties, const vgx_VertexProperty_t **prop_addr );
typedef struct s_vgx_PropertyConditionSet_t {
  f_vgx_PropertyConditionSet_length Length;
  f_vgx_PropertyConditionSet_get Get;
  f_vgx_PropertyConditionSet_append Append;
  bool positive;
  CQwordList_t *__data;
  // TODO: what about the value conditions??
} vgx_PropertyConditionSet_t;
DLL_VISIBLE extern const vgx_PropertyConditionSet_t DEFAULT_VGX_PROPERTY_CONDITION_SET;



/*******************************************************************//**
 * vgx_RecursiveCondition_t
 * 
 ***********************************************************************
 */
typedef struct s_vgx_RecursiveCondition_t {
  struct s_vgx_Evaluator_t *evaluator;                      // filter expression
  struct s_vgx_VertexCondition_t *vertex_condition;         // standard vertex filter with recursion
  const struct s_vgx_ArcConditionSet_t *arc_condition_set;  // standard arc filter
  struct {
    bool enable;
    vgx_ArcFilter_match match;
  } override;
} vgx_RecursiveCondition_t;



/*******************************************************************//**
 * vgx_VertexCondition_t
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VertexCondition_t {
  // true if positive, false if inverse matching logic (NOT match = hit)
  bool positive;

  // spec
  vgx_vertex_probe_spec spec;

  // manifestation
  vgx_VertexStateContext_man_t manifestation;

  // values/thresholds
  const CString_t *CSTR__vertex_type;   // simple filter on vertex type
  int64_t degree;                       // simple filter on total degree
  int64_t indegree;                     // simple filter on indegree
  int64_t outdegree;                    // simple filter on outdegree

  vgx_StringList_t *CSTR__idlist;       // filter on one or more vertex IDs (OR LOGIC!)
  
  // advanced conditions
  struct {
    // local filter expressions
    struct {
      struct s_vgx_Evaluator_t *filter;
      struct s_vgx_Evaluator_t *post;
    } local_evaluator;
    // Advanced Degree filter
    vgx_DegreeCondition_t *degree_condition;
    // Similarity
    vgx_SimilarityCondition_t *similarity_condition;
    // Timestamp
    vgx_TimestampCondition_t *timestamp_condition;
    // Properties
    vgx_PropertyConditionSet_t *property_condition_set;
    // Recursion
    struct {
      vgx_RecursiveCondition_t conditional;
      vgx_RecursiveCondition_t traversing;
      struct s_vgx_ArcConditionSet_t *collect_condition_set;    // standard collect filter
      vgx_collector_mode_t collector_mode;                      // collector mode
    } recursive;
  } advanced;

  CString_t *CSTR__error;
} vgx_VertexCondition_t;

DLL_VISIBLE extern const vgx_VertexCondition_t DEFAULT_VERTEX_CONDITION;



/*******************************************************************//**
 * vgx_VertexSortValue_t
 *
 ***********************************************************************
 */
typedef union u_vgx_VertexSortValue_t {
  // 64 bits of sort data
  QWORD qword;
  // 8 bytes
  char bytes[8];
  // 8-char prefix plus string pointer for tie-break
  char prefix_string[8];
  // 64-bit internalid-high
  uint64_t internalid_H;
  // 32-bit signed int
  struct {
    int32_t value;
    char __rsv[4];
  } int32;
  // 64-bit signed int
  struct {
    int64_t value;
  } int64;
  // 32-bit unsigned int
  struct {
    uint32_t value;
    char __rsv[4];
  } uint32;
  // 64-bit unsigned int
  struct {
    uint64_t value;
  } uint64;
  // single precision float
  struct {
    float value;
    char __rsv[4];
  } flt32;
  // double precision float
  struct {
    double value;
  } flt64;
} vgx_VertexSortValue_t;




/*******************************************************************//**
 * vgx_vertex_rankspec_t 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_vertex_rankspec_t {
  const char *expression;
  size_t sz;
} vgx_vertex_rankspec_t;




/*******************************************************************//**
 * vgx_RankingCondition_t 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_RankingCondition_t {
  vgx_sortspec_t sortspec;
  vgx_predicator_mod_t modifier;
  struct s_vgx_Vector_t *vector;
  CString_t *CSTR__expression;
  vgx_ArcConditionSet_t *aggregate_condition_set;
  int64_t aggregate_deephits;
  CString_t *CSTR__error;
} vgx_RankingCondition_t;

DLL_VISIBLE extern const vgx_RankingCondition_t DEFAULT_RANKING_CONDITION;





struct s_vgx_BaseQuery_t;
struct s_vgx_AdjacencyQuery_t;
struct s_vgx_NeighborhoodQuery_t;
struct s_vgx_GlobalQuery_t;
struct s_vgx_AggregatorQuery_t;




/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int vgx_response_show_as_string( const vgx_ResponseAttrFastMask fastmask ) {
  return (fastmask & VGX_RESPONSE_SHOW_AS_MASK) == VGX_RESPONSE_SHOW_AS_STRING;
}


/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static vgx_ResponseAttrFastMask vgx_response_show_as( const vgx_ResponseAttrFastMask fastmask ) {
  return (vgx_ResponseAttrFastMask)((int)fastmask & (int)VGX_RESPONSE_SHOW_AS_MASK);
}


/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static vgx_ResponseAttrFastMask vgx_response_attrs( const vgx_ResponseAttrFastMask fastmask ) {
  return (vgx_ResponseAttrFastMask)((int)fastmask & (int)VGX_RESPONSE_ATTRS_MASK);
}


/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int vgx_is_response_attrs_valid( const vgx_ResponseAttrFastMask fastmask ) {
  return ((int)vgx_response_attrs( fastmask ) & ~(int)VGX_RESPONSE_ATTRS_FULL) == 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ExecutionTime_t {
  double t_total, *pt_total;
  double t_search, *pt_search;
  double t_result, *pt_result;
} vgx_ExecutionTime_t;



/******************************************************************************
 *
 * TIMED BLOCK - ASSIGN RESULT TO VARIABLE
 *
 ******************************************************************************
 */
#define __START_TIMED_BLOCK( SecondsDoublePtr, T0Ptr, T1Ptr )     \
do {                                                              \
  double **__measured_seconds__ = &(SecondsDoublePtr);            \
  int64_t __t0ns__;                                               \
  int64_t *__pt0ns__ = T0Ptr;                                     \
  int64_t *__pt1ns__ = T1Ptr;                                     \
  if( (SecondsDoublePtr) != NULL ) {                              \
    if( __pt0ns__ != NULL ) {                                     \
      __t0ns__ = *__pt0ns__;                                      \
    }                                                             \
    else {                                                        \
      __t0ns__ = __GET_CURRENT_NANOSECOND_TICK();                 \
    }                                                             \
  }                                                               \
  else {                                                          \
    __t0ns__ = 0;                                                 \
  }                                                               \
  do

#define __END_TIMED_BLOCK                                         \
  WHILE_ZERO;                                                     \
  if( __t0ns__ > 0 ) {                                            \
    int64_t __t1ns__ = __GET_CURRENT_NANOSECOND_TICK();           \
    **__measured_seconds__ = (__t1ns__ - __t0ns__)/1000000000.0;  \
    if( __pt1ns__ ) {                                             \
      *__pt1ns__ = __t1ns__;                                      \
    }                                                             \
  }                                                               \
} WHILE_ZERO



/*******************************************************************//**
 * vgx_BaseQuery_t
 * 
 ***********************************************************************
 */
#define __vgx_BaseQuery_vtable( Struct )                                                                        \
  COMLIB_VTABLE_HEAD                                                                                            \
  int (*SetDebug)( Struct *self, vgx_query_debug debug );                                                       \
  int (*SetTimeout)( Struct *self, int timeout_ms, bool limexec );                                              \
  struct s_vgx_ExpressEvalMemory_t * (*SetMemory)( Struct *self, struct s_vgx_ExpressEvalMemory_t *memory );    \
  const char * (*AddPreFilter)( Struct *self, const char *filter_expression );                                  \
  const char * (*AddFilter)( Struct *self, const char *filter_expression );                                     \
  const char * (*AddPostFilter)( Struct *self, const char *filter_expression );                                 \
  vgx_VertexCondition_t * (*AddVertexCondition)( Struct *self, vgx_VertexCondition_t **vertex_condition );      \
  vgx_RankingCondition_t * (*AddRankingCondition)( Struct *self, vgx_RankingCondition_t **ranking_condition );  \
  const CString_t * (*SetErrorString)( Struct *self, CString_t **CSTR__error );                                 \
  struct s_vgx_SearchResult_t * (*YankSearchResult)( Struct *self );                                            \
  vgx_ExecutionTime_t (*GetExecutionTime)( Struct *self );



#define __vgx_BaseQuery_members                       \
  vgx_QueryType type;                                 \
  vgx_query_debug debug;                              \
  vgx_ExecutionTimingBudget_t timing_budget;          \
  vgx_ExecutionTime_t exe_time;                       \
  struct s_vgx_Graph_t *graph;                        \
  CString_t *CSTR__error;                             \
  CString_t *CSTR__pre_filter;                        \
  CString_t *CSTR__vertex_filter;                     \
  CString_t *CSTR__post_filter;                       \
  vgx_VertexCondition_t *vertex_condition;            \
  vgx_RankingCondition_t *ranking_condition;          \
  struct s_vgx_ExpressEvalMemory_t *evaluator_memory; \
  struct s_vgx_SearchResult_t *search_result;         \
  int64_t parent_opid;


typedef struct s_vgx_BaseQuery_vtable_t {
  __vgx_BaseQuery_vtable( struct s_vgx_BaseQuery_t )
} vgx_BaseQuery_vtable_t;

typedef struct s_vgx_BaseQuery_t {
  COMLIB_OBJECT_HEAD( vgx_BaseQuery_vtable_t )
  __vgx_BaseQuery_members
} vgx_BaseQuery_t;





/*******************************************************************//**
 * vgx_AdjacencyQuery_t
 * 
 ***********************************************************************
 */
#define __vgx_AdjacencyQuery_vtable( Struct )                                         \
  __vgx_BaseQuery_vtable( Struct )                                                    \
  int (*SetAnchor)( Struct *self, const char *anchor_id, CString_t **CSTR__error );   \
  vgx_ArcConditionSet_t * (*AddArcConditionSet)( Struct *self, vgx_ArcConditionSet_t **arc_condition_set );

#define __vgx_AdjacencyQuery_members        \
  __vgx_BaseQuery_members                   \
  CString_t *CSTR__anchor_id;               \
  vgx_AccessReason_t access_reason;         \
  vgx_ArcConditionSet_t *arc_condition_set; \
  int64_t n_arcs;                           \
  union {                                   \
    int64_t n_neighbors;                    \
    int64_t n_vertices;                     \
  };                                        \
  bool is_safe_multilock;

#define __vgx_AdjacencyQuery_args           \
  struct s_vgx_Graph_t *graph;              \
  const char *anchor_id;                    \
  CString_t **CSTR__error;

// vtable
typedef struct s_vgx_AdjacencyQuery_vtable_t {
  __vgx_AdjacencyQuery_vtable( struct s_vgx_AdjacencyQuery_t )
} vgx_AdjacencyQuery_vtable_t;

// object
typedef struct s_vgx_AdjacencyQuery_t {
  COMLIB_OBJECT_HEAD( vgx_AdjacencyQuery_vtable_t )
  __vgx_AdjacencyQuery_members
} vgx_AdjacencyQuery_t;

// constructor args
typedef struct s_vgx_AdjacencyQuery_constructor_args_t {
  __vgx_AdjacencyQuery_args
} vgx_AdjacencyQuery_constructor_args_t;

DLL_HIDDEN extern void vgx_AdjacencyQuery_RegisterClass( void );
DLL_HIDDEN extern void vgx_AdjacencyQuery_UnregisterClass( void );



/*******************************************************************//**
 * 
 * ResultSetQuery members
 ***********************************************************************
 */
#define __vgx_ResultSetQuery_members      \
  vgx_ResponseAttrFastMask fieldmask;     \
  struct s_vgx_Evaluator_t *selector;     \
  int offset;                             \
  int64_t hits;                           \
  struct s_vgx_BaseCollector_context_t *collector;



/*******************************************************************//**
 * vgx_NeighborhoodQuery_t
 * 
 ***********************************************************************
 */
#define __vgx_NeighborhoodQuery_vtable( Struct )  \
  __vgx_AdjacencyQuery_vtable( Struct )           \
  int (*SetResponseFormat)( Struct *self, vgx_ResponseAttrFastMask format );  \
  int (*SelectStatement)( Struct *self, struct s_vgx_Graph_t *graph, const char *select_statement, CString_t **CSTR__error );

#define __vgx_NeighborhoodQuery_members             \
  __vgx_AdjacencyQuery_members                      \
  __vgx_ResultSetQuery_members                      \
  vgx_ArcConditionSet_t *collect_arc_condition_set; \
  vgx_collector_mode_t collector_mode; 

#define __vgx_NeighborhoodQuery_args                  \
  __vgx_AdjacencyQuery_args                           \
  vgx_ArcConditionSet_t **collect_arc_condition_set;  \
  vgx_collector_mode_t collector_mode; 


// vtable
typedef struct s_vgx_NeighborhoodQuery_vtable_t {
  __vgx_NeighborhoodQuery_vtable( struct s_vgx_NeighborhoodQuery_t )
} vgx_NeighborhoodQuery_vtable_t;

// object
typedef struct s_vgx_NeighborhoodQuery_t {
  COMLIB_OBJECT_HEAD( vgx_NeighborhoodQuery_vtable_t )
  __vgx_NeighborhoodQuery_members
} vgx_NeighborhoodQuery_t;

// constructor args
typedef struct s_vgx_NeighborhoodQuery_constructor_args_t {
  __vgx_NeighborhoodQuery_args
} vgx_NeighborhoodQuery_constructor_args_t;


DLL_HIDDEN extern void vgx_NeighborhoodQuery_RegisterClass( void );
DLL_HIDDEN extern void vgx_NeighborhoodQuery_UnregisterClass( void );





/*******************************************************************//**
 * 
 * GlobalQuery members
 ***********************************************************************
 */
#define __vgx_GlobalQuery_members     \
  __vgx_BaseQuery_members             \
  const CString_t *CSTR__vertex_id;   \
  int64_t n_items;                    \
  vgx_collector_mode_t collector_mode; 



/*******************************************************************//**
 * vgx_GlobalQuery_t
 * 
 ***********************************************************************
 */
#define __vgx_GlobalQuery_vtable( Struct )    \
  __vgx_BaseQuery_vtable( Struct )            \
  int (*SetResponseFormat)( Struct *self, vgx_ResponseAttrFastMask format );  \
  int (*SelectStatement)( Struct *self, struct s_vgx_Graph_t *graph, const char *select_statement, CString_t **CSTR__error );


#define __vgx_GlobalQuery_args              \
  struct s_vgx_Graph_t *graph;              \
  const char *vertex_id;                    \
  CString_t **CSTR__error;                  \
  vgx_collector_mode_t collector_mode; 


// vtable
typedef struct s_vgx_GlobalQuery_vtable_t {
  __vgx_GlobalQuery_vtable( struct s_vgx_GlobalQuery_t )
} vgx_GlobalQuery_vtable_t;

// object
typedef struct s_vgx_GlobalQuery_t {
  COMLIB_OBJECT_HEAD( vgx_GlobalQuery_vtable_t )
  __vgx_GlobalQuery_members
  __vgx_ResultSetQuery_members
} vgx_GlobalQuery_t;

// constructor args
typedef struct s_vgx_GlobalQuery_constructor_args_t {
  __vgx_GlobalQuery_args
} vgx_GlobalQuery_constructor_args_t;


DLL_HIDDEN extern void vgx_GlobalQuery_RegisterClass( void );
DLL_HIDDEN extern void vgx_GlobalQuery_UnregisterClass( void );



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef union u_vgx_aggregator_predicator_value_t {
  QWORD bits;
  double real;
  int64_t integer;
} vgx_aggregator_predicator_value_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_aggregator_field_data_t {
  vgx_aggregator_predicator_value_t predval;
  int64_t degree;
  int64_t indegree;
  int64_t outdegree;

  // TODO: Define more
  
} vgx_aggregator_field_data_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_aggregator_fields_t {
  // The field data
  vgx_aggregator_field_data_t data;

  // Current vertex being visited
  vgx_Vertex_t *_this_vertex;

  // Pointers to field data (NULL if not active)
  vgx_aggregator_predicator_value_t *predval;

  int64_t *degree;
  int64_t *indegree;
  int64_t *outdegree;

  // TODO: Define more
  
} vgx_aggregator_fields_t;



/*******************************************************************//**
 * 
 * AggregationQuery members
 ***********************************************************************
 */
#define __vgx_AggregationQuery_members      \
  vgx_aggregator_fields_t *fields;          \
  vgx_ArcConditionSet_t *collect_arc_condition_set;



/*******************************************************************//**
 * vgx_AggregatorQuery_t
 * 
 ***********************************************************************
 */
#define __vgx_AggregatorQuery_vtable( Struct )                              \
  __vgx_AdjacencyQuery_vtable( Struct )                                     \
  int64_t (*AggregateDegree)( Struct *self, vgx_arc_direction arcdir );  \
  vgx_aggregator_predicator_value_t (*AggregatePredicatorValue)( Struct *self );

#define __vgx_AggregatorQuery_members   \
  __vgx_AdjacencyQuery_members          \
  __vgx_AggregationQuery_members

#define __vgx_AggregatorQuery_args      \
  __vgx_AdjacencyQuery_args             \
  vgx_ArcConditionSet_t **collect_arc_condition_set;


// vtable
typedef struct s_vgx_AggregatorQuery_vtable_t {
  __vgx_AggregatorQuery_vtable( struct s_vgx_AggregatorQuery_t )
} vgx_AggregatorQuery_vtable_t;

// object
typedef struct s_vgx_AggregatorQuery_t {
  COMLIB_OBJECT_HEAD( vgx_AggregatorQuery_vtable_t )
  __vgx_AggregatorQuery_members
} vgx_AggregatorQuery_t;

// constructor args
typedef struct s_vgx_AggregatorQuery_constructor_args_t {
  __vgx_AggregatorQuery_args
} vgx_AggregatorQuery_constructor_args_t;


DLL_HIDDEN extern void vgx_AggregatorQuery_RegisterClass( void );
DLL_HIDDEN extern void vgx_AggregatorQuery_UnregisterClass( void );




/* Constructor args */
typedef struct s_vgx_Vertex_constructor_args_t {
  CString_t                   *CSTR__error;       // construction error message if any
  const CString_t             *CSTR__idstring;    // the real (external) ID of the vertex
  vgx_VertexTypeEnumeration_t vxtype;             // the encoded vertex type
  struct s_vgx_Graph_t        *graph;             // the graph owning the vertex
  vgx_VertexStateContext_man_t manifestation;     // REAL or VIRTUAL
  vgx_Rank_t                  rank;               // rank coefficients
  uint32_t                    ts;                 // Current graph time (seconds since 1970)
} vgx_Vertex_constructor_args_t;




DLL_HIDDEN extern void vgx_Vertex_RegisterClass( void );
DLL_HIDDEN extern void vgx_Vertex_UnregisterClass( void );





/**************************************************************************//**
 * __vgx_default_vertex_descriptor
 *
 ******************************************************************************
 */
__inline static vgx_VertexDescriptor_t __vgx_default_vertex_descriptor( vgx_VertexStateContext_man_t man, vgx_VertexTypeEnumeration_t vxtype ) {
  switch( man ) {
  case VERTEX_STATE_CONTEXT_MAN_REAL:
    return VERTEX_DESCRIPTOR_NEW_REAL( vxtype );
  case VERTEX_STATE_CONTEXT_MAN_VIRTUAL:
    return VERTEX_DESCRIPTOR_NEW_VIRTUAL( vxtype );
  default:
    return VERTEX_DESCRIPTOR_NEW_NULL();
  }
}




/*******************************************************************//**
 * mapping
 * 
 * 
 ***********************************************************************
 */


typedef int vgx_mapping_spec_t;


typedef struct s_vgx_IMapping_t {
  framehash_t * (*NewMap)( const CString_t *CSTR__dirpath, const CString_t *CSTR__name, int64_t maxelem_hint, int8_t order_hint, vgx_mapping_spec_t spec, object_class_t obclass_changelog );
  void (*DeleteMap)( framehash_t **map );
  framehash_cell_t * (*NewIntegerMap)( framehash_dynamic_t *dyn, const char *name );
  void (*DeleteIntegerMap)( framehash_cell_t **map, framehash_dynamic_t *dyn );
  int (*IntegerMapAdd)( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *key, int64_t value );
  int (*IntegerMapDel)( framehash_cell_t **simple_map, framehash_dynamic_t *dyn, const char *key );
  int (*IntegerMapGet)( framehash_cell_t *simple_map, framehash_dynamic_t *dyn, const char *key, int64_t *value );
  int64_t (*IntegerMapSize)( framehash_cell_t *simple_map );
} vgx_IMapping_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTuple_t * _vgx_new_string_tuple( const char *key, const char *value ) {
  vgx_StringTuple_t *tuple = (vgx_StringTuple_t*)calloc( 2, sizeof( CString_t ) );
  if( tuple ) {
    tuple->CSTR__key = CStringNew( key );
    tuple->CSTR__value = CStringNew( value );
  }
  return tuple;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void _vgx_delete_string_tuple( vgx_StringTuple_t **tuple ) {
  if( tuple && *tuple ) {
    if( (*tuple)->CSTR__key ) {
      CStringDelete( (*tuple)->CSTR__key );
    }
    if( (*tuple)->CSTR__value ) {
      CStringDelete( (*tuple)->CSTR__value );
    }
    free( *tuple );
    *tuple = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const char * _vgx_string_tuple_key( vgx_StringTuple_t *tuple ) {
  return CStringValue( tuple->CSTR__key );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const char * _vgx_string_tuple_value( vgx_StringTuple_t *tuple ) {
  return CStringValue( tuple->CSTR__value );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const CString_t * _vgx_string_tuple_key_cstring( vgx_StringTuple_t *tuple ) {
  return tuple->CSTR__key;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static const CString_t * _vgx_string_tuple_value_cstring( vgx_StringTuple_t *tuple ) {
  return tuple->CSTR__value;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTupleList_t * _vgx_new_string_tuple_list( int64_t sz ) {
  vgx_StringTupleList_t *list = (vgx_StringTupleList_t*)calloc( 1, sizeof( vgx_StringTupleList_t ) );
  if( sz > 0 && list ) {
    list->sz = sz;
    list->data = (vgx_StringTuple_t**)calloc( sz+1, sizeof( vgx_StringTuple_t* ) );
  }
  return list;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTupleList_t * _vgx_new_empty_string_tuple_list( void ) {
  return (vgx_StringTupleList_t*)calloc( 1, sizeof( vgx_StringTupleList_t ) );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void _vgx_delete_string_tuple_list( vgx_StringTupleList_t **list ) {
  if( list && *list ) {
    vgx_StringTuple_t **cursor = (*list)->data;
    vgx_StringTuple_t **end = cursor + (*list)->sz;
    while( cursor < end ) {
      _vgx_delete_string_tuple( cursor );
      ++cursor;
    }
    free( (*list)->data );
    free( *list );
    *list = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int64_t _vgx_string_tuple_list_size( vgx_StringTupleList_t *list ) {
  return list ? list->sz : 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTuple_t * _vgx_string_tuple_list_set_item( vgx_StringTupleList_t *list, int64_t idx, const char *key, const char *value ) {
  if( list && idx >= 0 && idx < list->sz ) {
    return list->data[ idx ] = _vgx_new_string_tuple( key, value );
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTuple_t * _vgx_string_tuple_list_append_item( vgx_StringTupleList_t *list, const char *key, const char *value ) {
  if( list == NULL ) {
    return NULL;
  }
  vgx_StringTuple_t **new_data;
  if( list->data == NULL ) {
    new_data = (vgx_StringTuple_t**)calloc( 1, sizeof( vgx_StringTuple_t* ) );
  }
  else {
    new_data = (vgx_StringTuple_t**)realloc( list->data, (list->sz + 1) * sizeof( vgx_StringTuple_t* ) );
  }

  if( new_data ) {
    list->data = new_data;
    return list->data[ list->sz++ ] = _vgx_new_string_tuple( key, value );
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_StringTuple_t * _vgx_string_tuple_list_get_item( vgx_StringTupleList_t *list, int64_t idx ) {
  if( list && idx >= 0 && idx < list->sz ) {
    return list->data[ idx ];
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * vgx_Similarity_t
 *
 ***********************************************************************
 */
typedef union u_vgx_Similarity_fingerprint_config_t {
  struct {
    int nsegm;
    int nsign;
  };
  struct {
    int nprojections;
    int ksize;
  } ann;
} vgx_Similarity_fingerprint_config_t;

typedef struct s_vgx_Similarity_vector_config_t {
  uint8_t __rsv1;
  uint8_t euclidean;
  uint16_t max_size;
  int min_intersect;
  float min_cosine;
  float min_jaccard;
  float cosine_exponent;
  float jaccard_exponent;
} vgx_Similarity_vector_config_t;

typedef struct s_vgx_Similarity_threshold_config_t {
  int hamming;
  float similarity;
} vgx_Similarity_threshold_config_t;


#define __SIMILARITY_CONFIG_QWSZ (                \
  qwsizeof(vgx_Similarity_fingerprint_config_t) + \
  qwsizeof(vgx_Similarity_vector_config_t)      + \
  qwsizeof(vgx_Similarity_threshold_config_t) )


typedef union u_vgx_Similarity_config_t {
  QWORD qwords[ __SIMILARITY_CONFIG_QWSZ ];
  struct {
    vgx_Similarity_fingerprint_config_t fingerprint;
    vgx_Similarity_vector_config_t vector;
    vgx_Similarity_threshold_config_t threshold;
  };
} vgx_Similarity_config_t;

typedef union u_vgx_Similarity_value_t {
  __m128i bits;
  struct {
    int8_t _rsv1;
    int8_t _rsv2;
    int8_t intersect;
    int8_t valid;
    float jaccard;
    float cosine;
    float similarity;
  };
} vgx_Similarity_value_t;





/**************************************************************************//**
 * update_simconfig
 *
 ******************************************************************************
 */
static vgx_Similarity_config_t * update_simconfig( vgx_Similarity_config_t *dest, const vgx_Similarity_config_t *src ) {
  if( src->fingerprint.nsegm >= 0 )           dest->fingerprint.nsegm       = src->fingerprint.nsegm;
  if( src->fingerprint.nsign >= 0 )           dest->fingerprint.nsign       = src->fingerprint.nsign;
  if( src->vector.max_size >= 0 )             dest->vector.max_size         = src->vector.max_size;
  if( src->vector.min_intersect >= 0 )        dest->vector.min_intersect    = src->vector.min_intersect;
  if( src->vector.min_cosine >= 0.0f )        dest->vector.min_cosine       = src->vector.min_cosine;
  if( src->vector.min_jaccard >= 0.0f )       dest->vector.min_jaccard      = src->vector.min_jaccard;
  if( src->vector.cosine_exponent >= 0.0f )   dest->vector.cosine_exponent  = src->vector.cosine_exponent;
  if( src->vector.jaccard_exponent >= 0.0f )  dest->vector.jaccard_exponent = src->vector.jaccard_exponent;
  if( src->threshold.hamming >= 0 )           dest->threshold.hamming       = src->threshold.hamming;
  if( src->threshold.similarity >= 0.0f )     dest->threshold.similarity    = src->threshold.similarity;
  return dest;
}



/* VTABLE */
typedef struct s_vgx_Similarity_vtable_t {
  /* base methods */
  COMLIB_VTABLE_HEAD
  /* Similarity methods */
  struct s_vgx_Similarity_t * (*Clone)( struct s_vgx_Similarity_t *self );
  struct s_vgx_Vector_t * (*NewInternalVector)( struct s_vgx_Similarity_t *self, const void *elements, float scale, uint16_t sz, bool ephemeral );
  struct s_vgx_Vector_t * (*NewExternalVector)( struct s_vgx_Similarity_t *self, const void *elements, uint16_t sz, bool ephemeral );
  struct s_vgx_Vector_t * (*NewInternalVectorFromExternal)( struct s_vgx_Similarity_t *self, const void *external_elements, uint16_t sz, bool ephemeral, CString_t **CSTR__error );
  struct s_vgx_Vector_t * (*NewEmptyInternalVector)( struct s_vgx_Similarity_t *self, uint16_t vlen, bool ephemeral );
  struct s_vgx_Vector_t * (*NewEmptyExternalVector)( struct s_vgx_Similarity_t *self, uint16_t vlen, bool ephemeral );
  struct s_vgx_Vector_t * (*InternalizeVector)( struct s_vgx_Similarity_t *self, struct s_vgx_Vector_t *src, bool ephemeral, CString_t **CSTR__error );
  struct s_vgx_Vector_t * (*ExternalizeVector)( struct s_vgx_Similarity_t *self, struct s_vgx_Vector_t *src, bool ephemeral );
  struct s_vgx_Vector_t * (*TranslateVector)( struct s_vgx_Similarity_t *self, struct s_vgx_Vector_t *src, bool ephemeral, CString_t **CSTR__error );
  struct s_vgx_Vector_t * (*NewCentroid)( struct s_vgx_Similarity_t *self, const struct s_vgx_Vector_t *vectors[], bool ephemeral );
  int (*HammingDistance)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  float (*EuclideanDistance)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  float (*Cosine)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  float (*Jaccard)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  int8_t (*Intersect)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  float (*Similarity)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  bool (*Valid)( struct s_vgx_Similarity_t *self );
  void (*Clear)( struct s_vgx_Similarity_t *self );
  union u_vgx_Similarity_value_t * (*Value)( struct s_vgx_Similarity_t *self );
  int (*Match)( struct s_vgx_Similarity_t *self, const vgx_Comparable_t A, const vgx_Comparable_t B );
  struct s_vgx_Vector_t * (*VectorArithmetic)( struct s_vgx_Similarity_t *self, const struct s_vgx_Vector_t *A, const struct s_vgx_Vector_t *B, bool subtract, CString_t **CSTR__error );
  struct s_vgx_Vector_t * (*VectorScalarMultiply)( struct s_vgx_Similarity_t *self, const struct s_vgx_Vector_t *A, double factor, CString_t **CSTR__error );
  double (*VectorDotProduct)( struct s_vgx_Similarity_t *self, const struct s_vgx_Vector_t *A, const struct s_vgx_Vector_t *B, CString_t **CSTR__error );
  int (*CompareVectors)( struct s_vgx_Similarity_t *self, const struct s_vgx_Vector_t *A, const struct s_vgx_Vector_t *B );
  int (*SetReadonly)( struct s_vgx_Similarity_t *self );
  int (*IsReadonly)( struct s_vgx_Similarity_t *self );
  int (*ClearReadonly)( struct s_vgx_Similarity_t *self );
  void (*PrintVectorAllocator)( struct s_vgx_Similarity_t *self, struct s_vgx_Vector_t *vector );
  void (*PrintAllocators)( struct s_vgx_Similarity_t *self );
  int (*CheckAllocators)( struct s_vgx_Similarity_t *self );
  int64_t (*VerifyAllocators)( struct s_vgx_Similarity_t *self );
  int64_t (*BulkSerialize)( struct s_vgx_Similarity_t *self, bool force );
} vgx_Similarity_vtable_t;





/* DATA */
typedef struct s_vgx_Similarity_t {
  // ===========
  // CACHELINE 1
  // ===========

  // [1] vtable
  // [2] object typeinfo 
  /* [Q1.1 Q1.2] COMLIB BASE */
  COMLIB_OBJECT_HEAD( vgx_Similarity_vtable_t )

  // [3] parent
  // [Q1.3]
  struct s_vgx_Graph_t *parent;

  // [4] fingerprinter
  // [Q1.4]
  struct s_vgx_Fingerprinter_t *fingerprinter;
  
  // [5] cstring_construct
  // [Q1.5]
  f_CString_constructor_t cstring_construct;

  // [6] dimension_allocator
  // [Q1.6]
  object_allocator_context_t *dimension_allocator_context;
  
  // [7] dim_encoder
  // [Q1.7]
  framehash_t *dim_encoder;  // Maps string to number  ("led display" -> 5287618)

  // [8] dim_decoder
  // [Q1.8]
  framehash_t *dim_decoder;  // Maps number to string  (5287618 -> "led display")

  // ===========
  // CACHELINE 2
  // ===========

  // [9] int_vector_allocator
  // [Q2.1]
  cxmalloc_family_t *int_vector_allocator;

  // [10] ext_vector_allocator
  // [Q2.2]
  cxmalloc_family_t *ext_vector_allocator;

  // [11] nullvector
  // [Q2.3]
  struct s_vgx_Vector_t *nullvector;

  // [12]
  // [Q2.4.1]
  DWORD __rsv_2_4_1;

  // [13] readonly
  // [Q2.4.2]
  int readonly;

  // [14] value
  // [Q2.5/6]
  union u_vgx_Similarity_value_t value;
  
  // [15] int_vector_ephemeral_allocator
  // [Q2.7]
  cxmalloc_family_t *int_vector_ephemeral_allocator;
  
  // [16] ext_vector_ephemeral_allocator
  // [Q2.8]
  cxmalloc_family_t *ext_vector_ephemeral_allocator;

  // ===========
  // CACHELINE 3
  // ===========
  
  // [17] params
  // [Q1.1/2/3/4/5]
  union u_vgx_Similarity_config_t params;

  // [18]
  // [Q3.6]
  QWORD __rsv_3_6;
  
  // [19]
  // [Q3.7]
  QWORD __rsv_3_7;
  
  // [20]
  // [Q3.8]
  QWORD __rsv_3_8;

} vgx_Similarity_t;


typedef struct s_vgx_Similarity_constructor_args_t {

  union u_vgx_Similarity_config_t *params;
  struct s_vgx_Graph_t *parent;

} vgx_Similarity_constructor_args_t;



/*******************************************************************//**
 * vgx_AllocatorInfo_t
 ***********************************************************************
 */
typedef struct s_vgx_AllocatorInfo_t {
  int64_t bytes;
  double utilization;
} vgx_AllocatorInfo_t;



/*******************************************************************//**
 * vgx_MemoryInfo_t
 ***********************************************************************
 */
typedef struct s_vgx_MemoryInfo_t {

  struct {
    struct {
      vgx_AllocatorInfo_t physical;
    } global;
    struct {
      vgx_AllocatorInfo_t use;
    } process;
  } system;

  struct {
    vgx_AllocatorInfo_t total;

    struct {
      vgx_AllocatorInfo_t object;
      vgx_AllocatorInfo_t arcvector;
      vgx_AllocatorInfo_t property;
    } vertex;

    struct {
      vgx_AllocatorInfo_t data;
    } string;
    
    struct {
      vgx_AllocatorInfo_t global;
      vgx_AllocatorInfo_t type;
    } index;

    struct {
      vgx_AllocatorInfo_t vxtype;
      vgx_AllocatorInfo_t rel;
      vgx_AllocatorInfo_t vxprop;
      vgx_AllocatorInfo_t dim;
    } codec;

    struct {
      vgx_AllocatorInfo_t internal;
      vgx_AllocatorInfo_t external;
      vgx_AllocatorInfo_t dimension;
    } vector;

    struct {
      vgx_AllocatorInfo_t total;
      struct {
        vgx_AllocatorInfo_t evplong;
        vgx_AllocatorInfo_t evpmedium;
        vgx_AllocatorInfo_t evpshort;
      } map;
    } schedule;

    struct {
      vgx_AllocatorInfo_t string;
      vgx_AllocatorInfo_t vector;
      vgx_AllocatorInfo_t vtxmap;
    } ephemeral;

  } pooled;
  

} vgx_MemoryInfo_t;




/*******************************************************************//**
 * vgx_IGraphSimple_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IGraphSimple_t {

  int (*CreateVertexSimple)( struct s_vgx_Graph_t *self, const char *name, const char *type );
  int (*CreateVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_type, vgx_AccessReason_t *reason, CString_t **CSTR__error );

  vgx_Vertex_t * (*OpenVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_VertexAccessMode_t mode, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  vgx_Vertex_t * (*NewVertex)( struct s_vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*CreateVertexLifespan)( struct s_vgx_Graph_t *self, const char *name, const char *type, int lifespan, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );

  bool (*CloseVertex)( struct s_vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );
  int (*DeleteVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  bool (*HasVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name );

  int (*Connect)( struct s_vgx_Graph_t *self, const vgx_Relation_t *relation, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int64_t (*Disconnect)( struct s_vgx_Graph_t *self, vgx_AdjacencyQuery_t *query );

  int64_t (*Degree)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_arc_direction direction, int timeout_ms, vgx_AccessReason_t *reason );
  int64_t (*VertexDegree)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );
  int64_t (*VertexInDegree)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );
  int64_t (*VertexOutDegree)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, vgx_AccessReason_t *reason );

  vgx_vertex_type_t (*VertexSetType)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_type, int timeout_ms, vgx_AccessReason_t *reason );
  const CString_t * (*VertexGetType)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, int timeout_ms, vgx_AccessReason_t *reason );

  vgx_Relation_t * (*HasAdjacency)( struct s_vgx_Graph_t *self, vgx_AdjacencyQuery_t *query );
  vgx_Vertex_t * (*OpenNeighbor)( struct s_vgx_Graph_t *self, vgx_AdjacencyQuery_t *query, bool readonly );
  vgx_ArcHead_t (*ArcValue)( struct s_vgx_Graph_t *self, const vgx_Relation_t *relation, int timeout_ms, vgx_AccessReason_t *reason );
  int64_t (*Neighborhood)( struct s_vgx_Graph_t *self, vgx_NeighborhoodQuery_t *query );
  int64_t (*Aggregate)( struct s_vgx_Graph_t *self, vgx_AggregatorQuery_t *query );
  int64_t (*Arcs)( struct s_vgx_Graph_t *self, vgx_GlobalQuery_t *query );
  int64_t (*Vertices)( struct s_vgx_Graph_t *self, vgx_GlobalQuery_t *query );
  int64_t (*OrderType)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_type );

  int (*Relationships)( struct s_vgx_Graph_t *self, CtptrList_t **CSTR__strings );
  int (*VertexTypes)( struct s_vgx_Graph_t *self, CtptrList_t **CSTR__strings );
  int (*PropertyKeys)( struct s_vgx_Graph_t *self, CtptrList_t **CSTR__strings );
  int (*PropertyStringValues)( struct s_vgx_Graph_t *self, CtptrList_t **CSTR__string_qwo_list );

  int64_t (*Truncate)( struct s_vgx_Graph_t *self, CString_t **CSTR__error );
  int64_t (*TruncateType)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_type, CString_t **CSTR__error );

  struct s_vgx_Evaluator_t * (*DefineEvaluator)( struct s_vgx_Graph_t *self, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error );
  struct s_vgx_Evaluator_t * (*GetEvaluator)( struct s_vgx_Graph_t *self, const char *name );
  struct s_vgx_Evaluator_t ** (*GetEvaluators)( struct s_vgx_Graph_t *self, int64_t *sz );

} vgx_IGraphSimple_t;



/*******************************************************************//**
 * vgx_IGraphAdvanced_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IGraphAdvanced_t {

  int (*AcquireGraphReadonly)( struct s_vgx_Graph_t *self, int timeout_ms, bool force, vgx_AccessReason_t *reason );
  int (*IsGraphReadonly)( struct s_vgx_Graph_t *self );
  int (*ReleaseGraphReadonly)( struct s_vgx_Graph_t *self );
  int (*FreezeGraphReadonly_CS)( struct s_vgx_Graph_t *self, CString_t **CSTR__error );
  int (*FreezeGraphReadonly_OPEN)( struct s_vgx_Graph_t *self, CString_t **CSTR__error );
  void (*UnfreezeGraphReadonly_CS)( struct s_vgx_Graph_t *self );
  void (*UnfreezeGraphReadonly_OPEN)( struct s_vgx_Graph_t *self );

  struct s_vgx_Vertex_t * (*AcquireVertexReadonly)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason );
  struct s_vgx_Vertex_t * (*AcquireVertexByAddressReadonly)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );

  struct s_vgx_Vertex_t * (*AcquireVertexWritableNocreate_CS)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  struct s_vgx_Vertex_t * (*AcquireVertexWritableNocreate)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  struct s_vgx_Vertex_t * (*AcquireVertexByAddressWritableNocreate)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );

  struct s_vgx_Vertex_t * (*AcquireVertexWritable)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  
  struct s_vgx_Vertex_t * (*EscalateReadonlyToWritable)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t *vertex_RO, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  struct s_vgx_Vertex_t * (*RelaxWritableToReadonly)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t *vertex_WL );

  struct s_vgx_VertexList_t * (*AtomicAcquireVerticesReadonly)( struct s_vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  struct s_vgx_VertexList_t * (*AtomicAcquireVerticesWritable)( struct s_vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int64_t                     (*AtomicReleaseVertices)( struct s_vgx_Graph_t *self, vgx_VertexList_t **vertices_LCK );
  int64_t                     (*AtomicUnlockByIdentifiers)( struct s_vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers_WL );

  bool (*HasVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );
  bool (*HasVertex_CS)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );
  struct s_vgx_Vertex_t * (*GetVertex_CS)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );

  CString_t * (*GetVertexIDByAddress)( struct s_vgx_Graph_t *self, QWORD address, vgx_AccessReason_t *reason );
  int (*GetVertexInternalidByAddress)( struct s_vgx_Graph_t *self, QWORD address, objectid_t *obid, vgx_AccessReason_t *reason );

  struct s_vgx_Vertex_t * (*AcquireVertexObjectReadonly)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
  struct s_vgx_Vertex_t * (*AcquireVertexObjectWritable)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
  struct s_vgx_Vertex_t * (*AcquireVertexObjectReadonly_CS)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
  struct s_vgx_Vertex_t * (*AcquireVertexObjectWritable_CS)( struct s_vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );

  struct s_vgx_Vertex_t * (*NewVertex)( struct s_vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  struct s_vgx_Vertex_t * (*NewVertex_CS)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*CreateVertex_CS)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*CreateReturnVertex_CS)( struct s_vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, vgx_Vertex_t **ret_vertex_WL, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int64_t (*CommitVertex)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t *vertex_WL );
  bool (*ReleaseVertex_CS)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t **vertex_LCK );
  bool (*ReleaseVertex)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t **vertex_LCK );

  int (*DeleteVertex)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t *vertex_ANY, int timeout_ms, vgx_AccessReason_t *reason );
  int (*DeleteIsolatedVertex_CS_WL)( struct s_vgx_Graph_t *self, struct s_vgx_Vertex_t *vertex );

  int (*Connect_WL)( struct s_vgx_Graph_t *self, vgx_Relationship_t *relationship, vgx_Arc_t *arc_WL, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*Connect_M_INT_WL)( struct s_vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, int32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*Connect_M_UINT_WL)( struct s_vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, uint32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*Connect_M_FLT_WL)( struct s_vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, float value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  int (*Connect_M_INT)( struct s_vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, int32_t value, int timeout_ms, CString_t **CSTR__error );
  int (*Connect_M_UINT)( struct s_vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, uint32_t uvalue, int timeout_ms, CString_t **CSTR__error );
  int (*Connect_M_FLT)( struct s_vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, float value, int timeout_ms, CString_t **CSTR__error );

  int64_t (*Disconnect_WL)( struct s_vgx_Graph_t *self, vgx_Arc_t *arc_WL );

  void (*DeleteCollector)( struct s_vgx_BaseCollector_context_t **collector );

  int (*GetOpenVertices)( struct s_vgx_Graph_t *self, int64_t thread_id_filter, Key64Value56List_t **readonly, Key64Value56List_t **writable );
  int64_t (*CloseOpenVertices)( struct s_vgx_Graph_t *self );
  int64_t (*CommitWritableVertices)( struct s_vgx_Graph_t *self );
  CString_t * (*GetWritableVerticesAsCString)( struct s_vgx_Graph_t *self, int64_t *n );

  vgx_MemoryInfo_t (*GetMemoryInfo)( struct s_vgx_Graph_t *self );

  int64_t (*ResetSerial)( struct s_vgx_Graph_t *self, int64_t sn );

  void (*DebugPrintVertexAcquisitionMaps)( struct s_vgx_Graph_t *self );
  void (*DebugPrintAllocators)( struct s_vgx_Graph_t *self, const char *alloc_name );
  int (*DebugCheckAllocators)( struct s_vgx_Graph_t *self, const char *alloc_name );
  comlib_object_t * (*DebugGetObjectAtAddress)( struct s_vgx_Graph_t *self, QWORD address );
  comlib_object_t * (*DebugFindObjectByIdentifier)( struct s_vgx_Graph_t *self, const char *identifier );

  CString_t * (*GetVertexIDByOffset)( struct s_vgx_Graph_t *self, int64_t *poffset );

} vgx_IGraphAdvanced_t;



/*******************************************************************//**
 * vgx_IVGXProfile_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IVGXProfile_t {



  struct {
    CString_t * (*GetBrandString)( void );
    int (*GetAVXVersion)( void );
    int (*HasFeatureFMA)( void );
    CString_t * (*GetInstructionSetExtensions)( int *avxcompat );
    #if defined CXPLAT_ARCH_X64
    int (*GetCoreCount)( int *cores, int *threads );
    #elif defined CXPLAT_ARCH_ARM64
    int (*GetCoreCount)( int *P_cores, int *E_cores );
    #else
    #endif
    CString_t * (*GetCacheInfo)( void );
    CString_t * (*GetTLBInfo)( void );
    int (*GetL2Size)( void );
    int (*GetL2Associativity)( void );
    bool (*IsAVXCompatible)( int cpu_avx_version );
    int (*GetRequiredAVXVersion)( void );
  } CPU;

} vgx_IVGXProfile_t;


#include "vxoperation.h"




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IOperation_t {

  int (*Initialize)( struct s_vgx_Graph_t *graph, int64_t init_opid );
  int (*Destroy)( struct s_vgx_Graph_t *graph );

  struct {
    int (*Initialize)( struct s_vgx_Graph_t *graph, int64_t init_opid );
    int (*IsInitialized)( struct s_vgx_Graph_t *graph );
    int (*Start)( struct s_vgx_Graph_t *graph );
    int (*Stop)( struct s_vgx_Graph_t *graph );
    int (*IsRunning)( struct s_vgx_Graph_t *graph );
    int (*Suspend)( struct s_vgx_Graph_t *graph, int timeout_ms );
    int (*IsSuspended)( struct s_vgx_Graph_t *graph );
    int (*Resume)( struct s_vgx_Graph_t *graph );
    int (*IsReady)( struct s_vgx_Graph_t *graph );
    bool (*HasPending)( struct s_vgx_Graph_t *graph );
    int64_t (*GetPending)( struct s_vgx_Graph_t *graph );
    int (*Fence)( struct s_vgx_Graph_t *graph, int64_t opid, int timeout_ms );
    void (*Enable)( struct s_vgx_Graph_t *graph );
    void (*Disable)( struct s_vgx_Graph_t *graph );
    bool (*IsEnabled)( struct s_vgx_Graph_t *graph );
    void (*HeartbeatEnable)( struct s_vgx_Graph_t *graph );
    void (*HeartbeatDisable)( struct s_vgx_Graph_t *graph );
  } Emitter_CS;

  struct {
    int (*Initialize)( struct s_vgx_Graph_t *graph, vgx_OperationParser_t *parser, bool start_thread );
    int (*Destroy)( vgx_OperationParser_t *parser );
    int64_t (*SubmitData)( vgx_OperationParser_t *parser, vgx_ByteArrayList_t *list, CString_t **CSTR__error );
    int64_t (*Feed)( vgx_OperationParser_t *parser, const char *input, const char **next, WAITABLE_TIMER *Timer, vgx_OperationCounters_t *counters, CString_t **CSTR__error, vgx_op_parser_error_t *perr );
    int (*Reset)( vgx_OperationParser_t *parser );
    void (*EnableValidation)( vgx_OperationParser_t *parser, bool enable );
    void (*EnableExecution)( vgx_OperationParser_t *parser, bool enable );
    void (*SkipRegression)( vgx_OperationParser_t *parser, bool silent_skip );
    void (*EnableCRC)( vgx_OperationParser_t *parser, bool enable );
    void (*EnableStrictSerial)( vgx_OperationParser_t *parser, bool enable );
    int64_t (*Pending)( vgx_OperationParser_t *parser );
    int (*Suspend)( struct s_vgx_Graph_t *graph, int timeout_ms );
    int (*IsSuspended)( struct s_vgx_Graph_t *graph );
    int (*Resume)( struct s_vgx_Graph_t *graph );
    int (*AddFilter)( int64_t opcode_filter );
    int (*RemoveFilter)( int64_t opcode_filter );
    int (*ApplyProfile)( int64_t profile_id );
    unsigned int (*Checksum)( const char *data );
  } Parser;

  int (*IsOpen)( vgx_Operation_t *op );

  int (*Open_CS)( struct s_vgx_Graph_t *graph, vgx_Operation_t *op, const comlib_object_t *obj, bool hold_CS );
  int64_t (*Commit_CS)( struct s_vgx_Graph_t *graph, vgx_Operation_t *op, bool hold_CS );
  int64_t (*Close_CS)( struct s_vgx_Graph_t *graph, vgx_Operation_t *op, bool hold_CS );

  int (*GraphOpen_CS)( struct s_vgx_Graph_t *graph );
  int64_t (*GraphCommit_CS)( struct s_vgx_Graph_t *graph );
  int64_t (*GraphClose_CS)( struct s_vgx_Graph_t *graph );

  int (*Suspend)( int timeout_ms );
  int (*Resume)( void );
  int64_t (*WritableVertices)( void );
  int (*Fence)( int timeout_ms );
  int (*AssertState)( void );

  struct {
    vgx_OperationCounters_t (*Outstream)( struct s_vgx_Graph_t *SYSTEM );
    vgx_OperationCounters_t (*Instream)( struct s_vgx_Graph_t *SYSTEM );
    void (*Reset)( struct s_vgx_Graph_t *SYSTEM );
    vgx_OperationBacklog_t (*OutputBacklog)( struct s_vgx_Graph_t *SYSTEM );
  } Counters;

  struct {
    int (*Enter)( struct s_vgx_Graph_t *graph );
    int (*Leave)( struct s_vgx_Graph_t *graph );
    int (*Check)( struct s_vgx_Graph_t *graph );
  } Readonly_CS;

  int (*IsDirty)( const vgx_Operation_t *op );
  int (*SetDirty)( vgx_Operation_t *op );

  int64_t (*GetId_LCK)( const vgx_Operation_t *op );
  int64_t (*SetId)( vgx_Operation_t *op, int64_t opid );
  int64_t (*InitId)( vgx_Operation_t *op, int64_t opid );

  void (*Dump_CS)( vgx_Operation_t *op );


  struct {
    int (*New)( vgx_Vertex_t *vertex, struct s_vgx_Vertex_constructor_args_t *args );
    int (*Delete)( vgx_Vertex_t *vertex );
  } Vertex_CS;


  struct {
    int (*SetRank)( vgx_Vertex_t *vertex, const vgx_Rank_t *rank );
    int (*ChangeType)( vgx_Vertex_t *vertex, vgx_vertex_type_t new_type );
    int (*SetTMX)( vgx_Vertex_t *vertex, uint32_t tmx );
    int (*Convert)( vgx_Vertex_t *vertex, vgx_VertexStateContext_man_t manifestatio );
    int (*SetProperty)( vgx_Vertex_t *vertex, const vgx_VertexProperty_t *prop );
    int (*DelProperty)( vgx_Vertex_t *vertex, shortid_t key );
    int (*DelProperties)( vgx_Vertex_t *vertex );
    int (*SetVector)( vgx_Vertex_t *vertex, const vgx_Vector_t *vector );
    int (*DelVector)( vgx_Vertex_t *vertex );
    int (*RemoveOutarcs)( vgx_Vertex_t *vertex, int64_t n_removed );
    int (*RemoveInarcs)( vgx_Vertex_t *vertex, int64_t n_removed );
  } Vertex_WL;


  struct {
    int (*Connect)( struct s_vgx_Arc_t *arc );
    int (*Disconnect)( struct s_vgx_Arc_t *arc, int64_t n_removed );
  } Arc_WL;


  struct {
    int     (*StartAgent)( struct s_vgx_Graph_t *SYSTEM );
    int     (*SuspendAgent)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
    int     (*IsAgentSuspended)( struct s_vgx_Graph_t *SYSTEM );
    int     (*ResumeAgent)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
    int     (*StopAgent)( struct s_vgx_Graph_t *SYSTEM );
    int     (*Attach)( struct s_vgx_Graph_t *SYSTEM );
    int     (*Detach)( struct s_vgx_Graph_t *SYSTEM );
    int     (*ClearRegistry)( struct s_vgx_Graph_t *SYSTEM );
    int     (*CreateGraph)( struct s_vgx_Graph_t *SYSTEM, const objectid_t *obid, const char *name, const char *path, int vertex_block_order, uint32_t graph_t0, int64_t start_opcount, union u_vgx_Similarity_config_t *simconfig );
    int     (*DeleteGraph)( struct s_vgx_Graph_t *SYSTEM, const objectid_t *obid );
    int     (*Tick)( struct s_vgx_Graph_t *SYSTEM, int64_t tms );
    int     (*SendComment)( struct s_vgx_Graph_t *SYSTEM, CString_t *CSTR__comment );
    int     (*SendSimpleAuxCommand)( struct s_vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, CString_t *CSTR__command );
    int     (*ForwardAuxCommand)( struct s_vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, objectid_t cmd_id, vgx_StringList_t *cmd_data );
    int     (*SendRawData)( struct s_vgx_Graph_t *SYSTEM, const char *data, int64_t dlen );
    int     (*CloneGraph)( struct s_vgx_Graph_t *SYSTEM, struct s_vgx_Graph_t *source );
  } System_SYS_CS;


  struct {
    struct {
      int (*Start)( struct s_vgx_Graph_t *SYSTEM, uint16_t port, bool durable, int64_t snapshot_threshold );
      unsigned (*BoundPort)( struct s_vgx_Graph_t *SYSTEM );
      int (*IsDurable)( struct s_vgx_Graph_t *SYSTEM );
      int (*Stop)( struct s_vgx_Graph_t *SYSTEM );
      int (*Subscribe)( struct s_vgx_Graph_t *SYSTEM, const char *host, uint16_t port, bool hardsync, int timeout_ms, CString_t **CSTR__error );
      int (*Unsubscribe)( struct s_vgx_Graph_t *SYSTEM );
      int (*SuspendExecution)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
      int (*IsExecutionSuspended)( struct s_vgx_Graph_t *SYSTEM );
      int (*ResumeExecution)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
      int (*IsInitializing)( struct s_vgx_Graph_t *SYSTEM );
      int (*Suspend)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
      int (*IsSuspended)( struct s_vgx_Graph_t *SYSTEM );
      int (*Resume)( struct s_vgx_Graph_t *SYSTEM );
    } ConsumerService;

    int64_t (*TransPending)( struct s_vgx_Graph_t *SYSTEM );
    int64_t (*BytesPending)( struct s_vgx_Graph_t *SYSTEM );
  } System_OPEN;


  struct {
    int (*SetModified)( struct s_vgx_Graph_t *graph );
    int (*Truncate)( struct s_vgx_Graph_t *graph, vgx_vertex_type_t vxtype, int64_t n_discarded );
    int (*Persist)( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts, bool force );
    int (*State)( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts );
    int (*AssertState)( struct s_vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts );
  } Graph_CS;

  struct {
    int (*Sync)( struct s_vgx_Graph_t *graph, bool hard, int timeout_ms, CString_t **CSTR__error );
  } Graph_OPEN;

  struct {
    int (*GetBaseCounts)( struct s_vgx_Graph_t *graph, vgx_graph_base_counts_t *counts );
  } Graph_ROG;


  struct {
    int (*Readonly)( struct s_vgx_Graph_t *graph );
    int (*Readwrite)( struct s_vgx_Graph_t *graph );
    int (*Events)( struct s_vgx_Graph_t *graph );
    int (*NoEvents)( struct s_vgx_Graph_t *graph );
  } State_CS;


  struct {
    int (*AcquireWL)( struct s_vgx_Graph_t *graph, vgx_VertexList_t *vertices_WL, CString_t **CSTR__error );
  } Lock_CS;


  struct {
    int (*ReleaseLCK)( struct s_vgx_Graph_t *graph, vgx_VertexList_t *vertices_LCK );
    int (*All)( struct s_vgx_Graph_t *graph );
  } Unlock_CS;


  struct {
    int (*Events)( struct s_vgx_Graph_t *graph, uint32_t ts, uint32_t tmx );
  } Execute_CS;


  struct {
    int (*AddVertexType)( struct s_vgx_Graph_t *graph, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
    int (*RemoveVertexType)( struct s_vgx_Graph_t *graph, QWORD typehash, vgx_vertex_type_t typecode );
    int (*AddRelationship)( struct s_vgx_Graph_t *graph, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
    int (*RemoveRelationship)( struct s_vgx_Graph_t *graph, QWORD relhash, vgx_predicator_rel_enum relcode );
    int (*AddDimension)( struct s_vgx_Similarity_t *similarity, QWORD dimhash, const CString_t *CSTR__dimension, feature_vector_dimension_t dimcode );
    int (*RemoveDimension)( struct s_vgx_Similarity_t *similarity, QWORD dimhash, feature_vector_dimension_t dimcode );
    int (*AddPropertyKey)( struct s_vgx_Graph_t *graph, shortid_t keyhash, const CString_t *CSTR__key );
    int (*RemovePropertyKey)( struct s_vgx_Graph_t *graph, shortid_t keyhash );
    int (*AddStringValue)( struct s_vgx_Graph_t *graph, const objectid_t *obid, const CString_t *CSTR__string );
    int (*RemoveStringValue)( struct s_vgx_Graph_t *graph, const objectid_t *obid );
  } Enumerator_CS;



} vgx_IOperation_t;



#define COMMIT_GRAPH_OPERATION_CS( Graph )     iOperation.GraphCommit_CS( Graph )
#define CLOSE_GRAPH_OPERATION_CS( Graph )      iOperation.GraphClose_CS( Graph )
#define OPEN_GRAPH_OPERATION_CS( Graph )       iOperation.GraphOpen_CS( Graph )




#define BEGIN_GRAPH_COMMIT_GROUP_CS( Graph )                      \
  do {                                                            \
    vgx_Graph_t *__graph__ = (Graph);                             \
    ++(__graph__->OP.emitter.control.defercommit_CS);             \
    do

#define END_GRAPH_COMMIT_GROUP_CS                                 \
    WHILE_ZERO;                                                   \
    if( --(__graph__->OP.emitter.control.defercommit_CS) == 0 ) { \
      COMMIT_GRAPH_OPERATION_CS( __graph__ );                     \
    }                                                             \
  } WHILE_ZERO
     
    


#define COMMIT_VERTEX_OPERATION_CS_WL( Vertex_WL, HoldCS )  iOperation.Commit_CS( (Vertex_WL)->graph, &(Vertex_WL)->operation, HoldCS )
#define CLOSE_VERTEX_OPERATION_CS_WL( Vertex_WL )           iOperation.Close_CS( (Vertex_WL)->graph, &(Vertex_WL)->operation, false )




/*******************************************************************//**
 *
 * vgx_ExecutionJobDescriptor_t
 *
 ***********************************************************************/
typedef struct s_vgx_ExecutionJobDescriptor_t {
  // [1]
  // Q1
  comlib_task_t *TASK;
  
  // [2]
  // Q2
  struct s_vgx_Graph_t *graph;

  // [3]
  // Q3-6
  struct {
    vgx_VertexEventHeap_t  *pri_inp;  // executor's input heap
    vgx_VertexEventQueue_t *lin_bat;  // execution batch
    vgx_VertexEventQueue_t *lin_imm;  // imminent execution (execute asap)
    vgx_VertexEventQueue_t *lin_pen;  // pending execution (execution should be re-tried)
  } Queue;

  // [5]
  // Q7.1
  int batch_size;

  // [6]
  // Q7.2
  int n_current;

  // [7]
  // Q8
  int64_t n_exec;

  // [8]
  // Q9
  float ontime_rate;


} vgx_ExecutionJobDescriptor_t;




struct s_vgx_EventActionTrigger_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef int (*f_EventProcessorAction)( struct s_vgx_EventActionTrigger_t *trigger );



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_vgx_EventActionTrigger_t {
  struct {
    int64_t now;
    int64_t at;
    int64_t delta;
  } tms;
  struct {
    void *input;
    void *output;
    f_EventProcessorAction perform;
    uint64_t counter;
  } action;
  struct {
    double time_factor;
    const int64_t *interval;
    int post_action_sleep_ms;
    int8_t can_skip_tick;
  } schedule;
} vgx_EventActionTrigger_t;



/*******************************************************************//**
 * vgx_EventProcessor_t
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_vgx_EventProcessor_t {
  cacheline_t __cl[3];
  struct {
    
    // -------- CL 1 --------

    // [Q1.1]
    comlib_task_t *TASK;

    // [Q1.2]
    struct s_vgx_Graph_t *graph;

    // [Q1.3]
    vgx_ExecutionJobDescriptor_t *EXECUTOR;
    
    // [Q1.4.1]
    DWORD executor_thread_id;

    // [Q1.4.2]
    int ready;

    // [Q1.5]
    vgx_eventproc_parameters_t params;

    // [Q1.6-8]
    struct {
      framehash_t *LongTerm;
      framehash_t *MediumTerm;
      framehash_t *ShortTerm;
    } Schedule;

    // -------- CL 2 --------

    // [Q2.1]
    struct {
      vgx_VertexEventQueue_t *Queue;  // Event Processor internal event queue
    } Monitor;

    // [Q2.2]
    struct {
      vgx_VertexEventQueue_t *Queue;  // Imminent events queued directly to executor
    } Executor;

    // [Q2.3]
    vgx_EventActionTrigger_t **ActionTriggers;

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

    // -------- CL 3-4 --------

    // [Q3-4]
    struct {
      // [Q3]
      CS_LOCK Lock;

      // [Q4.1] External API event queue
      vgx_VertexEventQueue_t *Queue;

      // [Q4.2]
      QWORD __rsv_4_2;
      
      // [Q4.3]
      QWORD __rsv_4_3;
      
      // [Q4.4]
      QWORD __rsv_4_4;
      
      // [Q4.5]
      QWORD __rsv_4_5;
      
      // [Q4.6]
      QWORD __rsv_4_6;
      
      // [Q4.7]
      QWORD __rsv_4_7;
      
      // [Q4.8]
      QWORD __rsv_4_8;

    } PublicAPI;
    
  };
} vgx_EventProcessor_t;





/*******************************************************************//**
 * vgx_GraphTimer_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_GraphTimer_t {

  // [Q1.1]
  comlib_task_t *TASK;

  // [Q1.2]
  struct s_vgx_Graph_t *graph;

  // [Q1.3] Current time tick in milliseconds since 1970
  ATOMIC_VOLATILE_i64 tms_atomic;

  // [Q1.4.1] Current time tick in seconds since 1970
  ATOMIC_VOLATILE_u32 ts_atomic;

  // [Q1.4.2]
  ATOMIC_VOLATILE_u32 t0_atomic;

  // [Q1.5.1]
  ATOMIC_VOLATILE_i32 offset_tms_atomic;

  // [Q1.5.2]
  ATOMIC_VOLATILE_u32 inception_t0_atomic;

} vgx_GraphTimer_t;




/*******************************************************************//**
 * vgx_readonly_state_t
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_readonly_state_t {
  // Q1
  int16_t __n_disallowed;
  int16_t __n_readers;
  int16_t __n_explicit;
  int8_t __n_roreq;
  int8_t __n_writers_waiting;
  // Q2
  uint32_t __xthread_id; // transition thread ID
  struct {
    uint8_t is_locked;
    uint8_t xlock; // transition lock
    struct {
      uint8_t EVP_suspended   : 1;
      uint8_t OP_suspended    : 1;
      uint8_t TX_in_suspended : 1;
      uint8_t __rsv4          : 1;
      uint8_t is_serializing  : 1;
      uint8_t __rsv6          : 1;
      uint8_t __rsv7          : 1;
      uint8_t __rsv8          : 1;
    } bit;
    uint8_t __rsv;
  } __flags;
} vgx_readonly_state_t;


#define READONLY_MAX_WRITERS_WAITING 100
#define READONLY_MAX_READERS 32000
#define READONLY_MAX_EXPLICIT 32000


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_lock_readonly_CS( vgx_readonly_state_t *state_CS, bool resume_EVP_on_unlock, bool resume_OP_on_unlock, bool resume_TX_in_on_unlock ) {
  state_CS->__n_readers = 0;
  state_CS->__n_disallowed = 0;
  state_CS->__n_writers_waiting = 0;
  state_CS->__flags.is_locked = true;
  state_CS->__flags.bit.EVP_suspended = resume_EVP_on_unlock;
  state_CS->__flags.bit.OP_suspended = resume_OP_on_unlock;
  state_CS->__flags.bit.TX_in_suspended = resume_TX_in_on_unlock;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_unlock_readonly_CS( vgx_readonly_state_t *state_CS, bool *resume_EVP, bool *resume_OP, bool *resume_TX_in ) {

  if( state_CS->__n_readers > 0 ) {
    COMLIB_Fatal( "Tried to unlock readonly while nreaders still nonzero" );
  }

  state_CS->__n_readers = 0;
  state_CS->__n_disallowed = 0;
  state_CS->__n_writers_waiting = 0;
  state_CS->__flags.is_locked = false;
  if( resume_EVP ) {
    *resume_EVP = state_CS->__flags.bit.EVP_suspended;
  }
  if( resume_OP ) {
    *resume_OP = state_CS->__flags.bit.OP_suspended;
  }
  if( resume_TX_in ) {
    *resume_TX_in = state_CS->__flags.bit.TX_in_suspended;
  }
  state_CS->__flags.bit.EVP_suspended = false;
  state_CS->__flags.bit.OP_suspended = false;
  state_CS->__flags.bit.TX_in_suspended = false;
}


DLL_HIDDEN extern int _vxgraph_state__inc_explicit_readonly_CS( vgx_readonly_state_t *state_CS );
DLL_HIDDEN extern int _vxgraph_state__dec_explicit_readonly_CS( vgx_readonly_state_t *state_CS );



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_serializing_CS( vgx_readonly_state_t *state_CS ) {
  if( !state_CS->__flags.bit.is_serializing ) {
    state_CS->__flags.bit.is_serializing = true;
    _vxgraph_state__inc_explicit_readonly_CS( state_CS );
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_clear_serializing_CS( vgx_readonly_state_t *state_CS ) {
  if( state_CS->__flags.bit.is_serializing ) {
    state_CS->__flags.bit.is_serializing = false;
    _vxgraph_state__dec_explicit_readonly_CS( state_CS );
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_serializing_CS( vgx_readonly_state_t *state_CS ) {
  return state_CS->__flags.bit.is_serializing != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_readonly_suspend_EVP_CS( vgx_readonly_state_t *state_CS, bool suspend ) {
  state_CS->__flags.bit.EVP_suspended = suspend;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_readonly_resume_EVP_on_unlock_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__flags.bit.EVP_suspended;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_readonly_suspend_OP_CS( vgx_readonly_state_t *state_CS, bool suspend ) {
  state_CS->__flags.bit.OP_suspended = suspend;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_readonly_resume_OP_on_unlock_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__flags.bit.OP_suspended;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_is_readonly_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__flags.is_locked;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_is_writable_CS( vgx_readonly_state_t *state_CS ) {
  return !state_CS->__flags.is_locked;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_enter_readonly_transition_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__xthread_id = GET_CURRENT_THREAD_ID();
  state_CS->__flags.xlock = 1;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_leave_readonly_transition_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__xthread_id = 0;
  state_CS->__flags.xlock = 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_has_readonly_transition_CS( vgx_readonly_state_t *state_CS ) {
  return state_CS->__flags.xlock != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_has_readonly_transition_by_thread_CS( vgx_readonly_state_t *state_CS, uint32_t threadid ) {
  return _vgx_has_readonly_transition_CS( state_CS ) && state_CS->__xthread_id == threadid;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_request_write_CS( vgx_readonly_state_t *state_CS ) {
  if( _vgx_is_readonly_CS( state_CS ) ) {
    // Readonly mode is long-term claimed, don't count readers waiting
    if( state_CS->__n_explicit > 0 || _vgx_is_serializing_CS( state_CS ) ) {
      state_CS->__n_writers_waiting = 0;
    }
    // Otherwise indicate waiting writers are accumulating
    else if( state_CS->__n_writers_waiting < READONLY_MAX_WRITERS_WAITING ) {
      ++state_CS->__n_writers_waiting;
    }
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_has_readonly_request_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_roreq > 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_readonly_not_requested_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_roreq == 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_enter_readonly_request_CS( vgx_readonly_state_t *state_CS ) {
  return ++state_CS->__n_roreq;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_leave_readonly_request_CS( vgx_readonly_state_t *state_CS ) {
  return --state_CS->__n_roreq;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_inc_disallow_readonly_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__n_disallowed++;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_dec_disallow_readonly_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__n_disallowed--;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_get_disallow_readonly_recursion_CS( vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_disallowed;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_readonly_disallowed_CS( vgx_readonly_state_t *state_CS, int recursion ) {
  state_CS->__n_disallowed = (int16_t)recursion;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_readonly_allowed_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__n_disallowed = 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_is_readonly_allowed_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_disallowed == 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_is_readonly_disallowed_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_disallowed != 0;
}



#define BEGIN_DISALLOW_READONLY_CS( ReadonlyStatePtr_CS )       \
  do {                                                          \
    vgx_readonly_state_t *__state_CS__ = ReadonlyStatePtr_CS;   \
    _vgx_inc_disallow_readonly_CS( __state_CS__ );              \
    do

#define END_DISALLOW_READONLY_CS                      \
    WHILE_ZERO;                                       \
    _vgx_dec_disallow_readonly_CS( __state_CS__ );    \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_readonly_acquire_CS( vgx_readonly_state_t *state_CS ) {
  if( state_CS->__n_readers < READONLY_MAX_READERS && state_CS->__n_writers_waiting < READONLY_MAX_WRITERS_WAITING ) {

    if( state_CS->__flags.is_locked == false ) {
      COMLIB_Fatal(  "readonly not locked but tried to inc readers" );
    }

    return ++state_CS->__n_readers;
  }
  else {
    return -1;
  }
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_readonly_release_CS( vgx_readonly_state_t *state_CS ) {
  return --state_CS->__n_readers;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_get_readonly_readers_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_readers;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_has_readonly_readers_CS( const vgx_readonly_state_t *state_CS ) {
  return state_CS->__n_readers > 0;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_get_readonly_writers_waiting_CS( const vgx_readonly_state_t *state_CS ) {
  return (int)state_CS->__n_writers_waiting;
}


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_clear_readonly_writers_waiting_CS( vgx_readonly_state_t *state_CS ) {
  state_CS->__n_writers_waiting = 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_control_state_t {
  int16_t disallow_WL;
  struct {
    uint8_t ready       : 1;
    uint8_t vtx_busy    : 1; 
    uint8_t __rsv3      : 1;
    uint8_t __rsv4      : 1;
    uint8_t local_only  : 1;
    uint8_t __rsv6      : 1;
    uint8_t __rsv7      : 1;
    uint8_t __rsv8      : 1;
  };
  uint8_t __rsv;
} vgx_control_state_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int (*f_vgx_SystemAuxCommandCallback)( struct s_vgx_Graph_t *sysgraph, OperationProcessorAuxCommand cmd, vgx_StringList_t *data, CString_t **CSTR__error );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int (*f_vgx_SystemSyncCallback)( struct s_vgx_Graph_t *sysgraph, CString_t **CSTR__error );



/*******************************************************************//**
 * vgx_Graph_t
 * 
 * 
 ***********************************************************************
 */
#define VGX_SYSTEM_GRAPH_PATH VGX_PATHDEF_SYSTEM
#define VGX_SYSTEM_GRAPH_NAME "system"

#define VGX_MAX_GRAPH_PATH 191
#define VGX_MAX_GRAPH_NAME 191


/* VTABLE */
typedef struct s_vgx_Graph_vtable_t {
  /* COMLIB OBJECT INTERFACE (comlib_object_vtable_t) */
  COMLIB_VTABLE_HEAD

  /* */
  const CString_t * (*Name)( const struct s_vgx_Graph_t *self );

  void (*CountQueryNolock)( struct s_vgx_Graph_t *self, int64_t q_time_nanosec );
  void (*ResetQueryCountNolock)( struct s_vgx_Graph_t *self );
  int64_t (*QueryCountNolock)( struct s_vgx_Graph_t *self );
  int64_t (*QueryTimeNanosecAccNolock)( struct s_vgx_Graph_t *self );
  double (*QueryTimeAverageNolock)( struct s_vgx_Graph_t *self );

  void (*SetDestructorHook)( struct s_vgx_Graph_t *self, void *external_owner, void (*destructor_callback_hook)( struct s_vgx_Graph_t *self, void *external_owner ) );
  void (*ClearExternalOwner)( struct s_vgx_Graph_t *self );
  void (*SetSystemAuxCommandCallback)( struct s_vgx_Graph_t *self, f_vgx_SystemAuxCommandCallback sysaux_cmd_callback );
  void (*ClearSystemAuxCommandCallback)( struct s_vgx_Graph_t *self );
  void (*SetSystemSyncCallback)( struct s_vgx_Graph_t *self, f_vgx_SystemSyncCallback system_sync_callback );
  void (*ClearSystemSyncCallback)( struct s_vgx_Graph_t *self );
  const char * (*FullPath)( const struct s_vgx_Graph_t *self );
  int64_t (*BulkSerialize)( struct s_vgx_Graph_t *self, int timeout_ms, bool force, bool remote, vgx_AccessReason_t *reason, CString_t **CSTR__error );
  void (*Dump)( struct s_vgx_Graph_t *self );
  
  /* Simple API */
  vgx_IGraphSimple_t *simple;

  /* Advanced API */
  vgx_IGraphAdvanced_t *advanced;

} vgx_Graph_vtable_t;




/* MEMBERS */
CALIGNED_TYPE(struct) s_vgx_Graph_t {

  // ===========
  // CACHELINE 1
  // ===========

  union {
    cacheline_t __cl1;
    struct {
      // [Q1.1] vtable
      // [Q1.2] object typeinfo 
      COMLIB_OBJECT_HEAD( vgx_Graph_vtable_t )

      // [Q1.3-4] Graph ID
      objectid_t obid;

      // [Q1.5] Unique graph instance ID, always different for each graph instance
      QWORD instance_id;

      // [Q1.6] Graph name
      CString_t *CSTR__name;

      // [Q1.7] Graph base directory
      CString_t *CSTR__path;

      // [Q1.8] Graph full path
      CString_t *CSTR__fullpath;
    };
  };

  // ============
  // CACHELINE 2
  // ============

  union {
    cacheline_t __cl2;
    struct {
      // [Q2.1] Vertex allocator
      cxmalloc_family_t *vertex_allocator;

      // [Q2.2] Vertex index
      framehash_t *vxtable;

      // [Q2.3] Vertex type index 
      framehash_t **vxtypeidx;

      // [Q2.4] Acquired vertex map WL
      framehash_cell_t *vtxmap_WL;

      // [Q2.5] Acquired vertex map RO
      framehash_cell_t *vtxmap_RO;
      
      // [Q2.6] Map of custom defined stack evaluator contexts
      framehash_t *evaluators;

      // [Q2.7] Similarity object
      vgx_Similarity_t *similarity;

      // [Q2.8]
      QWORD __rsv_2_8;
    };
  };

  // ===========
  // CACHELINE 3
  // ===========

  union {
    cacheline_t __cl3;
    struct {
      // [Q3] Graph state lock
      CS_LOCK state_lock;
    };
  };

  // ===========
  // CACHELINE 4
  // ===========

  union {
    cacheline_t __cl4;
    struct {
      // [Q4.1.1] Graph owner's thread ID
      DWORD owner_threadid;

      // [Q4.1.2.1] Recursion count for re-opening already open graph
      uint8_t recursion_count;
      
      // [Q4.1.2.2] *rsv*
      uint8_t __rsv_4_1_2_2;

      // [Q4.1.2.3-4] State lock recursion counter
      int16_t __state_lock_count;

      // [Q4.2.1] Control state
      vgx_control_state_t control;

      // [Q4.2.2]
      DWORD __rsv_4_2_2;

      // [Q4.3-4] Graph readonly management
      vgx_readonly_state_t readonly;

      // [Q4.5] Graph operation
      vgx_Operation_t operation;

      // [Q4.6] Total current vertex writelock acquisitions
      int64_t count_vtx_WL;

      // [Q4.7] Total current vertex readlock acquisitions
      int64_t count_vtx_RO;

      // [Q4.8] Graph order = number of vertices |V(G)|
      ATOMIC_VOLATILE_i64 _order_atomic;
    };
  };

  // ===============
  // CACHELINE 5,6,7
  // ===============

  union {
    struct {
      cacheline_t __cl5;
      cacheline_t __cl6;
      cacheline_t __cl7;
    };

    // [Q5-7] Arcvector Framehash dynamic
    framehash_dynamic_t arcvector_fhdyn;
  };

  // ============
  // CACHELINE 8
  // ============

  union {
    cacheline_t __cl8;
    struct {
      // [Q8.1] Graph size = number of arcs |E(G)|
      ATOMIC_VOLATILE_i64 _size_atomic;

      // [Q8.2]
      ATOMIC_VOLATILE_i64 _nvectors_atomic;

      // [Q8.3]
      ATOMIC_VOLATILE_i64 _nproperties_atomic;

      // Graph reverse size = number of reverse arcs |E(G)|
      // For debug arcvector consistency
      // [Q8.4]
      ATOMIC_VOLATILE_i64 rev_size_atomic;

      // [Q8.5]
      QWORD __rsv_8_5;

      // [Q8.6]
      QWORD __rsv_8_6;

      // [Q8.7]
      QWORD __rsv_8_7;

      // [Q8.8]
      QWORD __rsv_8_8;
    };
  };

  // =================
  // CACHELINE 9,10,11
  // =================

  union {
    struct {
      cacheline_t __cl9;
      cacheline_t __cl10;
      cacheline_t __cl11;
    };

    // [14] Property Framehash dynamic
    // [Q9-11]
    framehash_dynamic_t property_fhdyn;
  };

  // ============
  // CACHELINE 12
  // ============

  union {
    cacheline_t __cl12;
    struct {
      // [Q12.1]
      object_allocator_context_t *ephemeral_string_allocator_context;

      // [Q12.2]
      object_allocator_context_t *property_allocator_context;

      // [Q12.3] Vertex type encoder
      framehash_t *vxtype_encoder;  // Maps string to number  ("user" -> 47)
      
      // [Q12.4] Vertex type decoder
      framehash_t *vxtype_decoder;  // Maps number to string  (47 -> "user")

      // [Q12.5] Relationship encoder
      framehash_t *rel_encoder;   // Maps string to number  ("likes" -> 17341)

      // [Q12.6] Relationship decoder
      framehash_t *rel_decoder;   // Maps number to string  (17341 -> "likes")

      // [Q12.7] Property key map
      framehash_t *vxprop_keymap;

      // [Q12.8] Property value map
      framehash_t *vxprop_valmap;
    };
  };

  // ===============
  // CACHELINE 13,14 
  // ===============

  union {
    struct {
      cacheline_t __cl13;
      cacheline_t __cl14;
    };
    struct {
      // -------------------------
      // [Q13]
      CS_LOCK lock;

      // -------------------------
      // [Q14.1.1]
      int fd;

      // [Q14.1.2]
      int ready;

      // [Q14.2]
      int64_t commit;

      // [Q14.3]
      int64_t bytes;

      // [Q14.4]
      int64_t count;

      // [Q14.5]
      QWORD __rsv_14_5;
      
      // [Q14.6]
      QWORD __rsv_14_6;
      
      // [Q14.7]
      QWORD __rsv_14_7;

      // [Q14.8]
      QWORD __rsv_14_8;

    } vprop;
  };

  // ==================
  // CACHELINE 15,16,17
  // ==================

  union {
    struct {
      cacheline_t __cl15;
      cacheline_t __cl16;
      cacheline_t __cl17;
    };

    // [Q15-17] Vertex Map Framehash dynamic
    framehash_dynamic_t vtxmap_fhdyn;
  };

  // ============
  // CACHELINE 18
  // ============

  union {
    cacheline_t __cl18;
    struct {
      // [Q18.1/2/3/4/5/6] Vertex availability condition
      CS_COND vertex_availability;

      // [Q18.7] Vertex list utility
      Cm256iList_t *vertex_list;

      // [Q18.8] Vertex heap utility
      Cm256iHeap_t *vertex_heap;
    };
  };

  // ===============
  // CACHELINE 19,20
  // ===============

  union {
    struct {
      cacheline_t __cl19;
      cacheline_t __cl20;
    };
    struct {
      // [Q19]
      CS_LOCK q_lock;

      // [Q20.1.1]
      int q_pri_req;

      // [Q20.1.2]
      int __rsv_20_1_2;

      // [Q20.2]
      int64_t q_count;

      // [Q20.3]
      int64_t q_time_nanosec_acc;

      // [Q20.4]
      QWORD __rsv_20_4;

      // [Q20.5]
      QWORD __rsv_20_5;

      // [Q20.6]
      QWORD __rsv_20_6;

      // [Q20.7]
      QWORD __rsv_20_7;

      // [Q20.8]
      QWORD __rsv_20_8;
    };
  };

  // ============
  // CACHELINE 21
  // ============

  union {
    cacheline_t __cl21;
    struct {
      // [Q21.1-5] Graph Timer
      vgx_GraphTimer_t TIC;

      // [Q21.6]
      QWORD __rsv_21_6;
      
      // [Q21.7]
      QWORD __rsv_21_7;

      // [Q21.8]
      QWORD __rsv_21_8;
    };
  };

  // ============
  // CACHELINE 22
  // ============

  union {
    cacheline_t __cl22;
    struct {
      // [Q22.1/2/3/4/5/6] Inarcs return condition
      CS_COND return_inarcs;

      // [Q22.7] Arc list utility
      Cm256iList_t *arc_list;

      // [Q22.8] Arc heap utility
      Cm256iHeap_t *arc_heap;
    };
  };

  // ======================
  // CACHELINE 23,24,25,26
  // ======================

  union {
    struct {
      cacheline_t __cl23;
      cacheline_t __cl24;
      cacheline_t __cl25;
      cacheline_t __cl26;
    };
    
    // [Q23-26]
    vgx_EventProcessor_t EVP;
  };

  // ============
  // CACHELINE 27
  // ============

  union {
    cacheline_t __cl27;
    struct {
      // [Q27.1-2]
      objectid_t tx_id_out;

      // [Q27.3]
      int64_t tx_serial_out;

      // [Q27.4]
      int64_t tx_count_out;

      // [Q27.5-6]
      objectid_t tx_id_in;

      // [Q27.7]
      int64_t tx_serial_in;

      // [Q27.8]
      int64_t tx_count_in;
    };
  };

  // ============
  // CACHELINE 28
  // ============

  union {
    cacheline_t __cl28;
    struct {
      // [Q28.1-2]
      objectid_t durable_tx_id;

      // [Q28.3]
      int64_t durable_tx_serial;

      // [Q28.4]
      int64_t sysmaster_tx_serial_0;

      // [Q28.5]
      int64_t sysmaster_tx_serial;

      // [Q28.6]
      int64_t sysmaster_tx_serial_count;

      // [Q28.7]
      int64_t persisted_ts;

      // [Q28.8]
      int64_t tx_input_lag_ms_CS;
    };
  };


  // =======================================
  // CACHELINE 29,30,31,32,33,34,35,36,37,38
  // =======================================

  union {
    struct {
      cacheline_t __cl29;
      cacheline_t __cl30;
      cacheline_t __cl31;
      cacheline_t __cl32;
      cacheline_t __cl33;
      cacheline_t __cl34;
      cacheline_t __cl35;
      cacheline_t __cl36;
      cacheline_t __cl37;
      cacheline_t __cl38;
    };

    // [Q29 - Q38]
    vgx_OperationProcessor_t OP;
  };

  // ============
  // CACHELINE 39
  // ============

  union {
    cacheline_t __cl39;
    struct {
      // [Q39.1/2/3/4/5/6] Wake event
      CS_COND wake_event;
      
      // [Q39.7]
      QWORD __rsv_39_7;

      // [Q39.8]
      QWORD __rsv_39_8;
    };
  };

  // ============
  // CACHELINE 40
  // ============

  union {
    cacheline_t __cl40;
    struct {

      // [Q40.1] Main server
      struct s_vgx_VGXServer_t *vgxserverA;

      // [Q40.2] Admin server
      struct s_vgx_VGXServer_t *vgxserverB;

      // [Q40.3]
      QWORD __rsv_40_3;

      // [Q40.4]
      QWORD __rsv_40_4;

      // [Q40.5]
      f_vgx_SystemAuxCommandCallback sysaux_cmd_callback;

      // [Q40.6]
      f_vgx_SystemSyncCallback system_sync_callback;

      // [Q40.7] External owner object (may be NULL)
      void *external_owner_object;

      // [Q40.8] Destructor callback hook (may be NULL)
      void (*destructor_callback_hook)( struct s_vgx_Graph_t *self, void *external_owner_object );

    };
  };

} vgx_Graph_t;



/* CONSTRUCTOR ARGS */
typedef struct s_vgx_Graph_constructor_args_t {

  const CString_t *CSTR__graph_name;
  const CString_t *CSTR__graph_path;

  int vertex_block_order;

  uint32_t graph_t0;
  int64_t start_opcount;

  vgx_Similarity_config_t *simconfig;

  bool with_event_processor;
  bool idle_event_processor;
  bool force_readonly;
  bool force_writable;
  bool local_only;

} vgx_Graph_constructor_args_t;


#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_VISIBLE extern const vgx_Graph_t * __assert_state_lock( const vgx_Graph_t *graph );
#else
#define __assert_state_lock( graph ) ((void)0)
#endif


/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_set_ready_CS( vgx_Graph_t *self ) {
  self->control.ready = true;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_clear_ready_CS( vgx_Graph_t *self ) {
  self->control.ready = false;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_is_ready_CS( vgx_Graph_t *self ) {
  return self->control.ready;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_allow_vertex_constructor_CS( vgx_Graph_t *self ) {
  self->control.vtx_busy = false;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_block_vertex_constructor_CS( vgx_Graph_t *self ) {
  self->control.vtx_busy = true;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_is_vertex_constructor_blocked_CS( vgx_Graph_t *self ) {
  return self->control.vtx_busy;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_set_local_only_CS( vgx_Graph_t *self ) {
  self->control.local_only = true;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_graph_clear_local_only_CS( vgx_Graph_t *self ) {
  self->control.local_only = false;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_is_local_only_CS( vgx_Graph_t *self ) {
  return self->control.local_only;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_allow_WL_CS( vgx_Graph_t *self ) {
  if( self->control.disallow_WL > 0 ) {
    return --(self->control.disallow_WL);
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_disallow_WL_CS( vgx_Graph_t *self ) {
  if( self->control.disallow_WL < SHRT_MAX ) {
    return ++(self->control.disallow_WL);
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int _vgx_graph_is_WL_disallowed_CS( vgx_Graph_t *self ) {
  return self->control.disallow_WL > 0;
}





#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_VISIBLE extern int64_t __vertex_count_dec( int64_t *pcnt );
#else
#define __vertex_count_dec( CounterPtr ) ( --(*(CounterPtr)) )
#endif


#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_VISIBLE extern int64_t __vertex_count_dec_delta( int64_t *pcnt, int delta );
#else
#define __vertex_count_dec_delta( CounterPtr, Delta ) ( *(CounterPtr) -= (Delta) )
#endif



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_inc_vertex_WL_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return ++self->count_vtx_WL;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_inc_vertex_WL_count_delta_CS( vgx_Graph_t *self, int8_t delta ) {
  __assert_state_lock( self );
  self->count_vtx_WL += delta;
  return self->count_vtx_WL;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_dec_vertex_WL_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return __vertex_count_dec( &self->count_vtx_WL );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_dec_vertex_WL_count_delta_CS( vgx_Graph_t *self, int8_t delta ) {
  __assert_state_lock( self );
  __vertex_count_dec_delta( &self->count_vtx_WL, delta );
  return self->count_vtx_WL;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_get_vertex_WL_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return self->count_vtx_WL;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_inc_vertex_RO_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return ++self->count_vtx_RO;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_inc_vertex_RO_count_delta_CS( vgx_Graph_t *self, int8_t delta ) {
  __assert_state_lock( self );
  self->count_vtx_RO += delta;
  return self->count_vtx_RO;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_dec_vertex_RO_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return __vertex_count_dec( &self->count_vtx_RO );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_dec_vertex_RO_count_delta_CS( vgx_Graph_t *self, int8_t delta ) {
  __assert_state_lock( self );
  __vertex_count_dec_delta( &self->count_vtx_RO, delta );
  return self->count_vtx_RO;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_get_vertex_RO_count_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  return self->count_vtx_RO;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_graph_milliseconds( vgx_Graph_t *self ) {
  return  ATOMIC_READ_i64( &self->TIC.tms_atomic );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static uint32_t _vgx_graph_seconds( vgx_Graph_t *self ) {
  return ATOMIC_READ_u32( &self->TIC.ts_atomic );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static uint32_t _vgx_graph_t0( vgx_Graph_t *self ) {
  return ATOMIC_READ_u32( &self->TIC.t0_atomic );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static uint32_t _vgx_graph_inception( vgx_Graph_t *self ) {
  return ATOMIC_READ_u32( &self->TIC.inception_t0_atomic );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_VISIBLE extern void __trap_fatal_state_corruption( vgx_Graph_t *graph );



typedef struct s_vgx_component_context_t {
  int init;
  struct {
    const char *simple;
    const char *extended;
  } version;
} vgx_component_context_t;

typedef struct s_vgx_context_t {
  vgx_component_context_t cxlib;
  vgx_component_context_t comlib;
  vgx_component_context_t cxmalloc;
  vgx_component_context_t framehash;
  vgx_component_context_t graph;
  vgx_component_context_t similarity;
  vgx_component_context_t socket;
  char sysroot[255];
} vgx_context_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static void __assert_operation_NOCS( vgx_Graph_t *graph ) {
#if defined CXPLAT_WINDOWS_X64
  uint64_t tid = (uint64_t)GET_CURRENT_THREAD_ID();
  HANDLE emitter_owner = graph->OP.emitter.TASK->_lock.lock.OwningThread;
  HANDLE parser_owner = graph->OP.parser.TASK->_lock.lock.OwningThread;
  uint64_t e = (uint64_t)emitter_owner;
  uint64_t p = (uint64_t)parser_owner;
  if( tid == e || tid == p ) {
    __trap_fatal_state_corruption( graph );
  }
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int16_t __try_enter_CS_nonblocking( vgx_Graph_t *graph ) {
  // UNSAFE HERE
  if( !TRY_CRITICAL_SECTION( &graph->state_lock.lock ) ) {
    return -1;
  }
  // SAFE HERE
  return ++(graph->__state_lock_count);
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int16_t __try_enter_CS( vgx_Graph_t *graph, int timeout_ms ) {
  if( timeout_ms == 0 ) {
    return __try_enter_CS_nonblocking( graph );
  }

  // UNSAFE HERE
  int64_t deadline_ns = -1;
  while( !TRY_CRITICAL_SECTION( &graph->state_lock.lock ) ) {
    int64_t t_ns = __GET_CURRENT_NANOSECOND_TICK();
    // Initialize deadline if this is our first failed attempt
    if( deadline_ns < 0 ) {
      if( timeout_ms < 0 ) {
        deadline_ns = LLONG_MAX;
      }
      else {
        deadline_ns = t_ns + 1000000LL * (int64_t)timeout_ms;
      }
    }
    // Check for timeout
    else if( t_ns > deadline_ns ) {
      return -1;
    }
    // Spin
    else {
      int64_t z = 0;
      int64_t a = 0;
      // Do something that doesn't get optimized away
      while( z < (4000 + (int64_t)(timeout_ms&0xf))  ) {
        a += z++;
      }
      deadline_ns ^= (a & 1);
    }
  }

  // SAFE HERE
  return ++(graph->__state_lock_count);
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int16_t __enter_CS( vgx_Graph_t *graph ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &graph->state_lock.lock );
  // SAFE HERE
  return ++(graph->__state_lock_count);
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int16_t __leave_CS( vgx_Graph_t *graph ) {
  // SAFE HERE
  int16_t c = --(graph->__state_lock_count);
  LEAVE_CRITICAL_SECTION( &graph->state_lock.lock );
  // UNSAFE HERE
  return c;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define TRY_GRAPH_LOCK( Graph, Timeout, AcquiredBool )  \
  do {                                                  \
    vgx_Graph_t *__pgraph__ = Graph;                    \
    if( __try_enter_CS( __pgraph__, Timeout ) < 0 ) {   \
      (AcquiredBool) = false;                           \
      break;                                            \
    }                                                   \
    (AcquiredBool) = true;                              \
    do



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_LOCK_SPIN( Graph, SpinMillisec )              \
  do {                                                      \
    vgx_Graph_t *__pgraph__ = Graph;                        \
    if( __try_enter_CS( __pgraph__, SpinMillisec ) < 0 ) {  \
      __enter_CS( __pgraph__ ); /* probably go to sleep */  \
    }                                                       \
    do



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_LOCK( Graph )           \
  do {                                \
    vgx_Graph_t *__pgraph__ = Graph;  \
    __enter_CS( __pgraph__ );         \
    do



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_RELEASE                 \
    WHILE_ZERO;                       \
    __leave_CS( __pgraph__ );         \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_FACTORY_ACQUIRE \
  do {                        \
    igraphfactory.Acquire();  \
    do



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_FACTORY_RELEASE \
    WHILE_ZERO;               \
    igraphfactory.Release();  \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define FORCE_GRAPH_RELEASE __leave_CS( __pgraph__ )



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_SUSPEND_LOCK( Graph )                                 \
  do {                                                              \
    /* SAFE HERE */                                                 \
    vgx_Graph_t *__pgraph__ = Graph;                                \
    int16_t __presus_recursion__ = __pgraph__->__state_lock_count;  \
    while( __leave_CS( __pgraph__ ) > 0 );                          \
    /* UNSAFE HERE */                                               \
    do



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_RESUME_LOCK                                     \
    /* UNSAFE HERE */                                         \
    WHILE_ZERO;                                               \
    while( __enter_CS( __pgraph__ ) < __presus_recursion__ ); \
    /* SAFE HERE */                                           \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_WAIT_UNTIL( Graph, Condition, TimeoutMilliseconds )         \
  do {                                                                    \
    BEGIN_TIME_LIMITED_WHILE( !(Condition), TimeoutMilliseconds, NULL ) { \
      GRAPH_SUSPEND_LOCK( Graph ) {                                       \
        sleep_milliseconds(1);                                            \
      } GRAPH_RESUME_LOCK;                                                \
    } END_TIME_LIMITED_WHILE;                                             \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_WAIT_CONDITION( Graph, ConditionVar, TimeoutMilliseconds) \
  do {                                                              \
    /* SAFE HERE */                                                 \
    int16_t __prewait_recursion__ = (Graph)->__state_lock_count;    \
    while( (Graph)->__state_lock_count > 1 ) {                      \
      __leave_CS( Graph );                                          \
    }                                                               \
    (Graph)->__state_lock_count--; /* down to 0 */                  \
    /* Release one lock and sleep until condition or timeout */     \
    TIMED_WAIT_CONDITION_CS( &((ConditionVar)->cond), &((Graph)->state_lock.lock), TimeoutMilliseconds );  \
    /* One lock now re-acquired */                                  \
    /* SAFE HERE */                                                 \
    (Graph)->__state_lock_count++; /* up to 1 */                    \
    while( (Graph)->__state_lock_count < __prewait_recursion__ ) {  \
      __enter_CS( Graph );                                          \
    }                                                               \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define WAIT_FOR_VERTEX_AVAILABLE( Graph, TimeoutMilliseconds )   GRAPH_WAIT_CONDITION( Graph, &(Graph)->vertex_availability, TimeoutMilliseconds )
#define SIGNAL_VERTEX_AVAILABLE( Graph )                          SIGNAL_ALL_CONDITION( &((Graph)->vertex_availability.cond) )

#define WAIT_FOR_INARCS_AVAILABLE( Graph, TimeoutMilliseconds )   GRAPH_WAIT_CONDITION( Graph, &(Graph)->return_inarcs, TimeoutMilliseconds )
#define SIGNAL_INARCS_AVAILABLE( Graph )                          SIGNAL_ALL_CONDITION( &((Graph)->return_inarcs.cond) )

#define WAIT_FOR_WAKE_EVENT( Graph, TimeoutMilliseconds )         GRAPH_WAIT_CONDITION( Graph, &(Graph)->wake_event, TimeoutMilliseconds )
#define SIGNAL_WAKE_EVENT( Graph )                                SIGNAL_ALL_CONDITION( &((Graph)->wake_event.cond) )



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define GRAPH_YIELD_AND_SIGNAL( Graph )         \
  do {                                          \
    SIGNAL_ALL_CONDITION( &((Graph)->vertex_availability.cond) ); \
    GRAPH_SUSPEND_LOCK( Graph ) {               \
    } GRAPH_RESUME_LOCK;                        \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_Graph_t * GRAPH_ENTER_CRITICAL_SECTION( vgx_Graph_t *graph ) {
  // UNSAFE HERE
  __enter_CS( graph );
  // SAFE HERE
  return graph;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void GRAPH_LEAVE_CRITICAL_SECTION( vgx_Graph_t **graph ) {
  if( *graph ) {
    // SAFE HERE
    __leave_CS( *graph );
    // UNSAFE HERE
    *graph = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define BEGIN_STATIC_GRAPH_CS( Graph, TimingBudget )                                    \
  do {                                                                                  \
    vgx_Graph_t *__sgr__ = Graph;                                                       \
    vgx_ExecutionTimingBudget_t *__stb__ = TimingBudget;                                \
    /* Hold here until no other threads hold writable locks */                          \
    int64_t nWL = _vxgraph_tracker__num_writable_locks_CS( __sgr__ );                   \
    int64_t nWL_total = _vxgraph_state__wait_max_writable_CS( __sgr__, nWL, __stb__ );  \
    if( nWL_total < 0 ) {                                                               \
      __set_access_reason( &__stb__->reason, VGX_ACCESS_REASON_ERROR );          \
    }                                                                                   \
    else if( nWL_total != nWL ) {                                                       \
      __set_access_reason( &__stb__->reason, VGX_ACCESS_REASON_TIMEOUT );        \
    }                                                                                   \
    else {                                                                              \
      do /* { code here } */
  


/*******************************************************************//**
 * 
 ***********************************************************************
 */

#define END_STATIC_GRAPH_CS                                                             \
      WHILE_ZERO;                                                                       \
    }                                                                                   \
  } WHILE_ZERO



/*******************************************************************//**
 *
 ***********************************************************************
 */
static bool __opsys_acquire_state_change( vgx_Graph_t *SYSTEM, int timeout_ms ) {
  bool acquired = false;
  GRAPH_LOCK( SYSTEM ) {
    DWORD tid = GET_CURRENT_THREAD_ID();
    // No state change or we already own the state change: acquire
    if( SYSTEM->OP.system.state_CS.state_change_recursion == 0 ||
        SYSTEM->OP.system.state_CS.state_change_owner == tid )
    {
      SYSTEM->OP.system.state_CS.state_change_owner = tid;
      SYSTEM->OP.system.state_CS.state_change_recursion++;
      acquired = true;
    }
    // Other thread owns state change
    else {
      // Wait for state change to be available (or timeout)
      GRAPH_WAIT_UNTIL( SYSTEM, SYSTEM->OP.system.state_CS.state_change_recursion == 0, timeout_ms );
      // State change available ?
      if( SYSTEM->OP.system.state_CS.state_change_recursion == 0 ) {
        // Own 1 state change and set ourselves as owner
        SYSTEM->OP.system.state_CS.state_change_owner = tid;
        SYSTEM->OP.system.state_CS.state_change_recursion = 1;
        acquired = true;
      }
    }
  } GRAPH_RELEASE;
  return acquired;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __opsys_release_state_change( vgx_Graph_t *SYSTEM ) {
  GRAPH_LOCK( SYSTEM ) {
    DWORD tid = GET_CURRENT_THREAD_ID();
    // Release one state change 
    if( SYSTEM->OP.system.state_CS.state_change_recursion > 0 && SYSTEM->OP.system.state_CS.state_change_owner == tid ) {
      // If no more recursive locks clear give up ownership
      if( --SYSTEM->OP.system.state_CS.state_change_recursion == 0 ) {
        SYSTEM->OP.system.state_CS.state_change_owner = 0;
      }
    }
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define BEGIN_OPSYS_STATE_CHANGE( SystemGraph, TimeoutMilliseconds )      \
  do {                                                                    \
    vgx_Graph_t *__system_graph__ = (SystemGraph);                        \
    int __timeout__ = TimeoutMilliseconds;                                \
    if( __opsys_acquire_state_change( __system_graph__, __timeout__ ) ) { \
      do /* { ... } */



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define END_OPSYS_STATE_CHANGE          \
      WHILE_ZERO;                       \
      __opsys_release_state_change( __system_graph__ ); \
    }                                   \
    else if( __timeout__ > 1000 ) {     \
      /* failed to acquire */           \
      WARN( 0x000, "(SYSTEM) Busy (state change timeout)" );  \
    }                                   \
    else {                              \
      /* Silent timeout when short */   \
    }                                   \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static uint32_t _vgx_graph_owner_CS( const vgx_Graph_t *self ) {
  return self->owner_threadid;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static int _vgx_graph_is_local_only_OPEN( vgx_Graph_t *self ) {
  int local;
  GRAPH_LOCK( self ) {
    local = _vgx_graph_is_local_only_CS( self );
  } GRAPH_RELEASE;
  return local;
}





typedef struct s_vgx_IGraphInfo_t {
  CString_t * (*GraphPath)( const vgx_Graph_t *graph );
  int (*NewNameAndPath)( const CString_t *CSTR__args_path, const CString_t *CSTR__args_name, CString_t **CSTR__computed_path, CString_t **CSTR__computed_name, vgx_StringTupleList_t **messages );
  objectid_t (*GraphID)( const CString_t *CSTR__path, const CString_t *CSTR__name );
  CString_t * (*Version)( int verbosity );
  CString_t * (*CTime)( int64_t tms, bool millisec );
} vgx_IGraphInfo_t;


typedef struct s_vgx_IGraphFactory_t {
  int (*Acquire)( void );
  int (*Release)( void );
  const CString_t * (*SystemRoot)( void );
  const CString_t * (*SetSystemRoot)( const char *sysroot );
  bool (*DisableEvents)( void );
  bool (*EnableEvents)( void );
  bool (*EventsEnabled)( void );
  int (*SetVectorType)( vector_type_t vtype );
  void (*UnsetVectorType)( void );
  bool (*FeatureVectors)( void );
  bool (*EuclideanVectors)( void );
  bool (*IsEventsIdleDeserialize)( void );
  bool (*IsReadonlyDeserialize)( void );
  int (*SuspendEvents)( int timeout_ms );
  int (*EventsResumable)( void );
  int (*ResumeEvents)( void );
  int (*SetAllReadonly)( int timeout_ms, vgx_AccessReason_t *reason );
  int (*CountAllReadonly)( void );
  int (*ClearAllReadonly)( void );
  bool (*IsInitialized)( void );
  int (*Initialize)( vgx_context_t *context, vector_type_t global_vtype, bool events_idle, bool readonly, vgx_StringTupleList_t **messages );
  int64_t (*Persist)( void );
  int (*RegisterGraph)( const vgx_Graph_t *graph, vgx_StringTupleList_t **messages );
  bool (*HasGraph)( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name );
  vgx_Graph_t * (*NewGraph)( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages );
  vgx_Graph_t * (*OpenGraph)( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, bool local, vgx_StringTupleList_t **messages, int timeout_ms );
  int (*CloseGraph)( vgx_Graph_t **graph, uint32_t *owner );
  int (*DeleteGraph)( const CString_t *CSTR__graph_path, const CString_t *CSTR__graph_name, int timeout_ms, bool erase, CString_t **CSTR__reason );
  vgx_Graph_t * (*GetGraphByObid)( const objectid_t *obid );
  int (*DelGraphByObid)( const objectid_t *obid );
  int (*Size)( void );
  const vgx_Graph_t ** (*ListGraphs)( int64_t *n_graphs );
  int (*SyncAllGraphs)( bool hard, int timeout_ms, CString_t **CSTR__error );
  int (*CancelSync)( void );
  bool (*IsSyncActive)( void );
  objectid_t (*UniqueLabel)( void );
  objectid_t (*Fingerprint)( void );
  int64_t (*FingerprintAge)( void );
  int (*DurabilityPoint)( objectid_t *txid, int64_t *sn, int64_t *ts, int *n_serializing );
  void (*Shutdown)( void );
} vgx_IGraphFactory_t;


typedef struct s_vgx_ISystem_t {
  bool (*Ready)( void );
  int (*Create)( void );
  int (*Destroy)( void );
  int (*AttachOutput)( vgx_StringList_t *uri_strings, vgx_TransactionalProducerAttachMode mode, bool handshake, int timeout_ms, CString_t **CSTR__error );
  bool (*IsAttached)( void );
  vgx_StringList_t * (*AttachedOutputs)( vgx_StringList_t **descriptions, int timeout_ms );
  vgx_StringList_t * (*AttachedOutputsSimple)( void );
  CString_t * (*InputAddress)( void );
  CString_t * (*AttachedInput)( void );
  int (*DetachOutput)( const vgx_StringList_t *detach_subscribers, bool remove_disconnected, bool force, int timeout_ms, CString_t **CSTR__error );
  bool (*IsSystemGraph)( vgx_Graph_t *graph );
  vgx_Graph_t *(*GetSystemGraph)( void );
  int64_t (*MasterSerial)( void );
  vgx_Graph_t *(*GetGraph)( const objectid_t *obid );
  int (*CaptureCreateGraph)( vgx_Graph_t *graph, vgx_TransactionalProducer_t *producer );
  int (*CaptureDestroyGraph)( const objectid_t *obid, const char *path );
  bool (*BeginQuery)( bool pri );
  void (*EndQuery)( bool pri, int64_t q_time_nanosec );
  int (*GetQueryPriReq)( void );
  int64_t (*QueryCount)( void );
  int64_t (*QueryTimeNanosecAcc)( void );
  double (*QueryTimeAverage)( void );
  struct {
    int (*SetInt)( const char *key, int64_t value );
    int (*GetInt)( const char *key, int64_t *rvalue );
    int (*SetReal)( const char *key, double value );
    int (*GetReal)( const char *key, double *rvalue );
    int (*SetString)( const char *key, const char *value );
    int (*FormatString)( const char *key, const char *valfmt, ... );
    int (*GetString)( const char *key, CString_t **CSTR__rvalue );
    int (*Delete)( const char *key );
    bool (*Exists)( const char *key );
  } Property;
  struct {
    const char * (*VertexType)( void );
    const char * (*RootVertex)( void );
    const char * (*PropertyVertex)( void );
  } Name;
} vgx_ISystem_t;



typedef struct s_vgx_IGraphEvent_t {
  int (*Initialize)( vgx_Graph_t *self, bool run_daemon );
  int (*Start)( vgx_Graph_t *self, bool enable );
  int (*Destroy)( vgx_Graph_t *self );
  int (*Schedule)( vgx_Graph_t *self );
  int (*IsReady)( vgx_Graph_t *self );
  int (*IsEnabled)( vgx_Graph_t *self );
  vgx_EventProcessor_t * (*Acquire)( vgx_Graph_t *self, int timeout_ms );
  int (*Release)( vgx_EventProcessor_t **processor_WL );
  vgx_EventBacklogInfo_t (*BacklogInfo)( vgx_Graph_t *self );
  CString_t * (*FormatBacklogInfo)( vgx_Graph_t *self, vgx_EventBacklogInfo_t *backlog );

  struct {
    int (*Flush)( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
    int (*Enable)( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
    int (*Disable)( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
    int (*SetDefunct)( vgx_Graph_t *self, int timeout_ms );
  } NOCS;

  struct {
    int (*Expiration_WL)( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, uint32_t tmx );
  } ScheduleVertexEvent;

  struct {
    void (*RemoveSchedule_WL)( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
    void (*ImmediateDrop_CS_WL_NT)( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
  } CancelVertexEvent;

  int (*ExistsInSchedule_WL_NT)( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );

  uint32_t (*ExecutorThreadId)( vgx_Graph_t *self );

} vgx_IGraphEvent_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_execution_timing_budget( int64_t t_now_ms, int timeout_ms ) {
  vgx_ExecutionTimingBudget_t tb;
  tb.flags._bits = 0;
  tb.resource = NULL;
  tb.reason = VGX_ACCESS_REASON_NONE;
  tb.t0_ms = t_now_ms;
  // Nonblocking, zero budget
  if( (tb.timeout_ms = timeout_ms) == 0 ) {
    tb.t_remain_ms = 0;   // No wait for resources
    tb.tt_ms = tb.t0_ms;  // End time = start time
  }
  // Blocking
  else {
    tb.flags.is_blocking = true;
    // Limited wait time
    if( timeout_ms > 0 ) {
      tb.t_remain_ms = timeout_ms;          // Wait for resources up to the timeout
      tb.tt_ms = t_now_ms + tb.t_remain_ms; // End time = start time + timeout
    }
    // Infinite wait time
    else {
      tb.timeout_ms = INT_MAX;
      tb.flags.is_infinite = true;
      tb.t_remain_ms = LLONG_MAX; // Wait forever
      tb.tt_ms = LLONG_MAX;       // End time = never
    }
  }
  return tb;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_zero_execution_timing_budget( void ) {
  return _vgx_get_execution_timing_budget( 0, 0 );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_infinite_execution_timing_budget( void ) {
  return _vgx_get_execution_timing_budget( 0, -1 );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_reset_execution_timing_budget( vgx_ExecutionTimingBudget_t *tb ) {
  tb->flags.resource_blocked = false;
  tb->flags.is_halted = false;
  tb->flags.is_exe_limit_exceeded = false;
  tb->t0_ms = tb->tt_ms = 0;
  tb->t_remain_ms = tb->flags.is_infinite ? LLONG_MAX : tb->timeout_ms;
  if( tb->t_remain_ms > 0 ) {
    tb->flags.is_blocking = true;
  }
  tb->resource = NULL;
  tb->reason = VGX_ACCESS_REASON_NONE;
  return tb->t_remain_ms;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_add_execution_timing_budget( vgx_ExecutionTimingBudget_t *tb, int extra_ms ) {
  if( !tb->flags.is_infinite && extra_ms > 0 ) {
    tb->flags.is_blocking = true;   // Becomes blocking
    tb->flags.is_halted = false;    // Clear any halted flag
    tb->flags.is_exe_limit_exceeded = false;  // Clear any execution limit exceeded flag
    // Slide timestamps by the additional amount
    tb->timeout_ms += extra_ms;     // Timeout is larger
    tb->t_remain_ms += extra_ms;    // More remaining time 
    tb->tt_ms += extra_ms;          // End time is later
  }
  return tb->t_remain_ms;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_get_execution_timing_budget_remaining( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->t_remain_ms;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_graph_execution_timing_budget( vgx_Graph_t *self, int timeout_ms ) {
  return _vgx_get_execution_timing_budget( _vgx_graph_milliseconds( self ), timeout_ms );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_graph_zero_execution_timing_budget( vgx_Graph_t *self ) {
  return _vgx_get_execution_timing_budget( _vgx_graph_milliseconds( self ), 0 );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t _vgx_get_graph_infinite_execution_timing_budget( vgx_Graph_t *self ) {
  return _vgx_get_execution_timing_budget( _vgx_graph_milliseconds( self ), -1 );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t * _vgx_start_graph_execution_timing_budget( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *tb ) {
  tb->t0_ms = _vgx_graph_milliseconds( self );
  tb->tt_ms = tb->flags.is_infinite ? LLONG_MAX : tb->t0_ms + tb->t_remain_ms;
  return tb;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_execution_resource_blocked( vgx_ExecutionTimingBudget_t *tb, void *resource ) {
  tb->flags.resource_blocked = true;
  tb->resource = resource;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_execution_halted( vgx_ExecutionTimingBudget_t *tb, vgx_AccessReason_t reason ) {
  tb->flags.is_halted = true;
  tb->reason = reason;
  tb->t_remain_ms = 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_execution_halted( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->flags.is_halted != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_execution_explicit_halt( vgx_ExecutionTimingBudget_t *tb ) {
  tb->flags.is_halted = true;
  tb->flags.explicit_halt = true;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_execution_explicitly_halted( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->flags.is_halted && tb->flags.explicit_halt;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_clear_execution_explicit_halt( vgx_ExecutionTimingBudget_t *tb ) {
  if( _vgx_is_execution_explicitly_halted( tb ) ) {
    tb->flags.is_halted = false;
    tb->flags.explicit_halt = false;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_execution_blocking( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->flags.is_blocking != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_set_execution_limited( vgx_ExecutionTimingBudget_t *tb ) {
  tb->flags.is_exe_limited = true;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void _vgx_clear_execution_limited( vgx_ExecutionTimingBudget_t *tb ) {
  tb->flags.is_exe_limited = false;
  tb->flags.is_exe_limit_exceeded = false;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_execution_limited( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->flags.is_exe_limited != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_is_execution_limit_exceeded( const vgx_ExecutionTimingBudget_t *tb ) {
  return tb->flags.is_exe_limit_exceeded != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int64_t _vgx_update_graph_execution_timing_budget( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *tb, bool halt ) {
  int64_t delta_ms = tb->tt_ms - _vgx_graph_milliseconds( self );
  if( delta_ms < 0 && tb->flags.is_exe_limited ) {
    tb->flags.is_exe_limit_exceeded = true;
  }
  if( halt || delta_ms < 0 ) {
    tb->t_remain_ms = 0;
    if( tb->flags.resource_blocked ) {
      tb->flags.is_halted = true;
    }
    tb->flags.is_blocking = false; // will not block again if budget is used again
  }
  else {
    tb->t_remain_ms = delta_ms;
  }
  return delta_ms;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( TimingBudgetPtr, GraphCS )       \
  do {                                                                        \
    vgx_ExecutionTimingBudget_t *__tb__ = TimingBudgetPtr;                    \
    vgx_Graph_t *__gCS__ = GraphCS;                                           \
    bool __halted__ = false;                                                  \
    if( __tb__->t0_ms == 0 ) {                                                \
      _vgx_start_graph_execution_timing_budget( __gCS__, __tb__ );         \
    }                                                                         \
    do

#define EXECUTION_TIME_LIMIT (__tb__->t_remain_ms)
#define EXECUTION_BUDGET_TIMEOUT (__tb__->timeout_ms)
#define EXECUTION_BUDGET_IS_NONE (!__tb__->flags.is_blocking)
#define EXECUTION_HALTED (&__halted__)



/*******************************************************************//**
 * 
 ***********************************************************************
 */
#define BEGIN_EXECUTION_BLOCKED_WHILE( BlockedWhileCondition )  \
  BEGIN_TIME_LIMITED_WHILE( (BlockedWhileCondition), EXECUTION_TIME_LIMIT, EXECUTION_HALTED )


#define SET_EXECUTION_RESOURCE_BLOCKED( ResourcePtr )    _vgx_set_execution_resource_blocked( __tb__, (void*)(ResourcePtr) )


#define BEGIN_WAIT_FOR_EXECUTION_RESOURCE( ResourcePtr )  \
  do {                                                    \
    SET_EXECUTION_RESOURCE_BLOCKED( ResourcePtr );        \
    if( __tb__->flags.is_blocking )


#define END_WAIT_FOR_EXECUTION_RESOURCE  \
  } WHILE_ZERO


#define END_EXECUTION_BLOCKED_WHILE  \
  END_TIME_LIMITED_WHILE


#define END_EXECUTE_WITH_TIMING_BUDGET_CS                                        \
    WHILE_ZERO;                                                                  \
    _vgx_update_graph_execution_timing_budget( __gCS__, __tb__, __halted__ ); \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcFilter_match __arcfilter_error( void ) {
  return VGX_ARC_FILTER_MATCH_ERROR;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */

__inline static int __is_arcfilter_error( const vgx_ArcFilter_match m ) {
  return m == VGX_ARC_FILTER_MATCH_ERROR;
}





typedef int (*f_vgx_ArcFilter)( struct s_vgx_virtual_ArcFilter_context_t *context, vgx_LockableArc_t *arc, vgx_ArcFilter_match *match );

typedef int (*f_vgx_PredicatorMatchFunction)( const vgx_predicator_t probe, const vgx_predicator_t target );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define VGX_ARC_FILTER_HEAD                                   \
  /* Enumeration code for the filter instance type*/          \
  vgx_ArcFilter_type type;                                    \
  /* Positive/negative match logic */                         \
  bool positive_match;                                        \
  /* Next Head lock required */                               \
  bool arcfilter_locked_head_access;                          \
  /* Perform synthetic arc eval */                            \
  bool eval_synarc;                                           \
  /* Advanced filter */                                       \
  struct s_vgx_Evaluator_t *traversing_evaluator;             \
  /* Function returning true/false whether to include arc in output */ \
  f_vgx_ArcFilter filter;                                     \
  /* Timing budget */                                         \
  vgx_ExecutionTimingBudget_t *timing_budget;                 \
  /* Pointer to filter context in previous neighborhood that led us to the current neighborhood */ \
  const struct s_vgx_virtual_ArcFilter_context_t *previous_context;   \
  /* The anchor vertex whose neighborhood we're currently probing */ \
  vgx_Vertex_t *current_tail;                                 \
  /* The head of the arc currently being probed*/             \
  const vgx_ArcHead_t *current_head;                          \
  /* Inherit behavior from superfilter if self is allpass */  \
  struct s_vgx_virtual_ArcFilter_context_t *superfilter;




/*******************************************************************//**
 *
 * Base Context for arc filters.
 * 
 * THIS IS AN ABSTRACT CLASS - DO NOT ALLOCATE THIS TYPE DIRECTLY!
 *
 ***********************************************************************
 */
typedef struct s_vgx_virtual_ArcFilter_context_t {
  VGX_ARC_FILTER_HEAD
} vgx_virtual_ArcFilter_context_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_NoArcFilter_context_t {
  VGX_ARC_FILTER_HEAD
} vgx_NoArcFilter_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_recursive_probe_t {
  vgx_virtual_ArcFilter_context_t *arcfilter;   /* Arc Filter (for traversal and recursion) generated based on conditions */
  struct s_vgx_vertex_probe_t *vertex_probe;    /* Current vertex's neighbor(s)'s conditions */
  vgx_arc_direction arcdir;
  struct s_vgx_Evaluator_t *evaluator;
  struct {
    bool enable;                      /* When enable is true the result of recursive probe will be set to */
    vgx_ArcFilter_match match;        /* the specified match value (forced HIT or MISS)                   */
  } override;
} vgx_recursive_probe_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcFilter_match __arcfilter_MISS( const vgx_recursive_probe_t *recursive ) {
  return recursive->override.enable ? recursive->override.match : VGX_ARC_FILTER_MATCH_MISS;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ArcFilter_match __arcfilter_THRU( const vgx_recursive_probe_t *recursive, vgx_ArcFilter_match src ) {
  return recursive->override.enable ? ( __is_arcfilter_error( src ) ? src : recursive->override.match ) : src;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_neighborhood_probe_t {
  vgx_Graph_t *graph;                                       /* */
  vgx_recursive_probe_t conditional;
  vgx_recursive_probe_t traversing;
  int distance;                                             /* Number of arcs between anchor and neighbor being probed */
  bool readonly_graph;                                      /* Graph is readonly for the lifetime of this probe */
  vgx_collector_mode_t collector_mode;                      /* Collector mode for this neighborhood level */
  vgx_Vertex_t *current_tail_RO;                            /* Tail vertex for current neighborhood traversal */
  struct s_vgx_BaseCollector_context_t *common_collector;   /* Shared collector instance for all neighborhood levels (either arc or vertex collector) */
  struct s_vgx_Evaluator_t *pre_evaluator;
  struct s_vgx_Evaluator_t *post_evaluator;
  vgx_virtual_ArcFilter_context_t *collect_filter_context;  /* Collection filter (for populating search results) to use for this neighborhood level */
} vgx_neighborhood_probe_t;



/**************************************************************************//**
 * _vgx_is_neighborhood_probe_halted
 *
 ******************************************************************************
 */
__inline static bool _vgx_is_neighborhood_probe_halted( vgx_neighborhood_probe_t *probe ) {
  return _vgx_is_execution_halted( probe->conditional.arcfilter->timing_budget ) ||
         _vgx_is_execution_halted( probe->traversing.arcfilter->timing_budget );
}


typedef struct s_vgx_degree_probe_t {
  struct s_vgx_ArcCollector_context_t *collector;
  vgx_neighborhood_probe_t *neighborhood_probe;
  vgx_value_condition_t value_condition;
  int64_t current_value;
} vgx_degree_probe_t;



typedef struct s_vgx_timestamp_probe_t {
  bool positive;
  vgx_value_condition_t tmc_valcond;
  vgx_value_condition_t tmm_valcond;
  vgx_value_condition_t tmx_valcond;
} vgx_timestamp_probe_t;



typedef struct s_vgx_similarity_probe_t {
  bool positive;
  struct s_vgx_Similarity_t *simcontext;  // shared clone of graph's simcontext, for use during query
  struct s_vgx_Vector_t *probevector;     // independently owned vector reference, if condition requires a vector
  FP_t fingerprint;                       // fingerprint for hamdist condition
  vgx_value_condition_t simscore;         // 
  vgx_value_condition_t hamdist;          //
} vgx_similarity_probe_t;



typedef struct s_vgx_property_probe_t {
  bool positive_match;
  int64_t len;
  vgx_VertexProperty_t *condition_list;
} vgx_property_probe_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_vertex_probe_t {
  // Probe Spec
  vgx_vertex_probe_spec spec;

  // Vertex Manifestation
  vgx_VertexStateContext_man_t manifestation;

  // Time allowed for probe execution. When negative, search will terminate.
  vgx_ExecutionTimingBudget_t *timing_budget;

  // Vertex Filter generated based on conditions
  struct s_vgx_VertexFilter_context_t *vertexfilter_context;

  // Distance from anchor vertex
  int distance; // Number of arcs between anchor and neighbor being probed

  // Filter values/thresholds
  vgx_vertex_type_t vertex_type;
  int64_t degree;
  int64_t indegree;
  int64_t outdegree;
  vgx_StringList_t *CSTR__idlist;

  // Advanced conditions
  struct {
    struct {
      struct s_vgx_Evaluator_t *filter;
      struct s_vgx_Evaluator_t *post;
    } local_evaluator;
    vgx_degree_probe_t *degree_probe;
    vgx_timestamp_probe_t *timestamp_probe;
    vgx_similarity_probe_t *similarity_probe;
    vgx_property_probe_t *property_probe;
    struct {
      vgx_neighborhood_probe_t *neighborhood_probe;
    } next;
  } advanced;

} vgx_vertex_probe_t;




/**************************************************************************//**
 * _vgx_vertex_condition_full_wildcard
 *
 ******************************************************************************
 */
__inline static int _vgx_vertex_condition_full_wildcard( vgx_vertex_probe_spec spec, vgx_VertexStateContext_man_t manifestation ) {
  return manifestation == VERTEX_STATE_CONTEXT_MAN_ANY
         &&
         (spec & VERTEX_PROBE_WILDCARD_MASK) == VERTEX_PROBE_WILDCARD;
}


struct s_vgx_GenericArcFilter_context_t;

typedef int (*f_vgx_GenericArcFilter_callback_t)( struct s_vgx_GenericArcFilter_context_t *context, const vgx_LockableArc_t *arc );



typedef struct s_vgx_ArcFilterTerminal_t {
  const vgx_Vertex_t *current;
  const vgx_Vertex_t **list;
  vgx_boolean_logic logic;
} vgx_ArcFilterTerminal_t;


/*******************************************************************//**
 *
 * Generic ArcFilter - match on predicator condition(s) and/or vertex condition
 *
 ***********************************************************************
 */
typedef struct s_vgx_GenericArcFilter_context_t {
  VGX_ARC_FILTER_HEAD
  vgx_ArcFilterTerminal_t terminal;
  f_vgx_GenericArcFilter_callback_t arcfilter_callback;
  vgx_boolean_logic logic;
  vgx_predicator_t pred_condition1;
  vgx_predicator_t pred_condition2;
  const vgx_vertex_probe_t *vertex_probe;
  struct s_vgx_Evaluator_t *culleval;
  int locked_cull;
} vgx_GenericArcFilter_context_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static struct s_vgx_Evaluator_t * __arcfilter_get_cull_evaluator( vgx_virtual_ArcFilter_context_t *arcfilter_context ) {
  if( _vgx_arcfilter_has_vertex_probe( arcfilter_context->type ) ) {
    vgx_GenericArcFilter_context_t *GAF = (vgx_GenericArcFilter_context_t*)arcfilter_context;
    return GAF->culleval;
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_VertexFilter)( struct s_vgx_VertexFilter_context_t *context, vgx_Vertex_t *vertex, vgx_ArcFilter_match *match );

typedef int (*f_vgx_RecursiveVertexMatchFunction)( const vgx_virtual_ArcFilter_context_t *arcfilter_context, const vgx_vertex_probe_t *vertex_probe, vgx_Vertex_t *vertex_RO, vgx_ArcFilter_match *match );
typedef int (*f_vgx_VertexIdentifierMatchFunction)( const vgx_vertex_probe_spec spec, const CString_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid );
typedef int (*f_vgx_VertexIdentifierListMatchFunction)( const vgx_vertex_probe_spec spec, const vgx_StringList_t *CSTR__probe, const vgx_VertexIdentifier_t *vertex_identifier, const objectid_t *vertex_internalid );
typedef int (*f_vgx_VertexTypeMatchFunction)( const vgx_vertex_probe_spec spec, const vgx_vertex_type_t type_probe, const vgx_Vertex_t *vertex_RO );
typedef int (*f_vgx_VertexSimilarityMatchFunction)( const vgx_similarity_probe_t *similarity_probe, const vgx_Vector_t *vertex_vector );
typedef int (*f_vgx_VertexMatchFunction)( const vgx_vertex_probe_t *vertex_probe, const vgx_Vertex_t *vertex_RO );
typedef int (*f_vgx_VertexTimestampMatchFunction)( const vgx_timestamp_probe_t *timestamp_probe, const vgx_Vertex_t *vertex_RO );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define VGX_VERTEX_FILTER_HEAD                                  \
  /* Enumeration code for the filter instance type*/            \
  vgx_VertexFilter_type type;                                   \
  /* Positive/nagative matching logic */                        \
  bool positive_match;                                          \
  /* Advanced filter */                                         \
  struct {                                                      \
    struct s_vgx_Evaluator_t *pre;                              \
    struct s_vgx_Evaluator_t *main;                             \
    struct s_vgx_Evaluator_t *post;                             \
  } local_evaluator;                                            \
  /* ID of thread executing filter */                           \
  DWORD current_thread;                                         \
  /* Function returning true/false whether to include vertex in output */ \
  f_vgx_VertexFilter filter;                                    \
  /* Timing budget */                                           \
  vgx_ExecutionTimingBudget_t *timing_budget;                   \



/*******************************************************************//**
 *
 * Base Context for vertex filters.
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexFilter_context_t {
  VGX_VERTEX_FILTER_HEAD
} vgx_VertexFilter_context_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_NoVertexFilter_context_t {
  VGX_VERTEX_FILTER_HEAD
} vgx_NoVertexFilter_context_t;



/*******************************************************************//**
 *
 * Simple VertexFilter - exact match on single predicator relationship type
 *
 ***********************************************************************
 */
typedef struct s_vgx_GenericVertexFilter_context_t {
  VGX_VERTEX_FILTER_HEAD
  vgx_vertex_probe_t *vertex_probe;
} vgx_GenericVertexFilter_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */


struct s_vgx_ExpressEvalOperation_t;

typedef void (*f_evaluator)( struct s_vgx_Evaluator_t *self );



typedef struct s_vgx_StackItem_t {
  union {
    QWORD meta;
    struct {
      vgx_StackItemType_t type;
      union {
        DWORD bits;
        struct {
          union {
            uint16_t u16;
            int16_t i16;
          };
          union {
            uint8_t u8;
            uint8_t i8;
          };
          struct {
            uint8_t u0 : 1;
            uint8_t u1 : 1;
            uint8_t u2 : 1;
            uint8_t u3 : 1;
            uint8_t u4 : 1;
            uint8_t u5 : 1;
            uint8_t u6 : 1;
            uint8_t u7 : 1;
          } flag;
        };
      } aux;
    };
  };
  union {
    QWORD bits;
    struct {
      uint32_t uint32;
      float real32;
    } pair;
    int64_t integer;
    double real;
    const CString_t *CSTR__str;
    vgx_cstring_array_map_cell_t keyval;
    const void *ptr;
    const vgx_Vector_t *vector;
    const vgx_Vertex_t *vertex;
    const vgx_VertexIdentifier_t *vertexid;
  };
} vgx_EvalStackItem_t;



typedef struct s_vgx_ExpressEvalOperation_t {
  f_evaluator func;
  vgx_EvalStackItem_t arg;
} vgx_ExpressEvalOperation_t;


typedef struct s_vgx_ExpressEvalString_t {
  CString_t *CSTR__literal;
  struct s_vgx_ExpressEvalString_t *next;
} vgx_ExpressEvalString_t;


typedef struct s_vgx_ExpressEvalVector_t {
  vgx_Vector_t *vector;
  struct s_vgx_ExpressEvalVector_t *next;
} vgx_ExpressEvalVector_t;


typedef struct s_vgx_ExpressEvalStack_t {
  // Evaluation stack
  vgx_EvalStackItem_t *data;
  // Size
  int sz;
  // Evaluation depth
  struct {
    int max;
    int run;
  } eval_depth;
  // Parent
  struct {
    vgx_Graph_t *graph;
    vgx_SelectProperties_t *properties;
    struct s_vgx_Evaluator_t *evaluator;
  } parent;
} vgx_ExpressEvalStack_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_item_terminator( const vgx_EvalStackItem_t *x ) {
  return x->type == STACK_ITEM_TYPE_INTEGER && x->integer == 0;
}
#define EvalStackItemIsTerminator( Item ) __is_stack_item_terminator( Item )



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __set_stack_item_terminator( vgx_EvalStackItem_t *x ) {
  x->type = STACK_ITEM_TYPE_INTEGER;
  x->integer = 0;
}
#define EvalStackItemSetTerminator( Item ) __set_stack_item_terminator( Item )





typedef struct s_vgx_ExpressEvalProgram_t {
  // Operation data
  vgx_ExpressEvalOperation_t *operations;
  // Evaluation stack
  vgx_ExpressEvalStack_t stack;
  // Operation counter
  int length;
  // Passthru operations
  int n_passthru;

  struct {
    // Number of operations involving a tail vertex or arc from tail vertex to current vertex
    int tail;
    // Number of operations dereferencing local vertex
    int this;
    // Number of operations requiring head dereference
    int head;
    // Number of operations evaluating arc from this to head or any head evaluations
    int arc;
  } deref;

  //
  int identifiers;

  // Positive when expression contains the cull operator (this is the cull heap size)
  int64_t cull;

  // Number of operations requiring additional eval after traversing A =()=> B
  int synarc_ops;

  // Number of work register slots required by program
  int n_wreg;
  
  // Literal strings
  vgx_ExpressEvalString_t *strings;
  // Expression name
  CString_t *CSTR__assigned;
  // Expression
  CString_t *CSTR__expression;

  // Parser
  struct {
    // Operation cursor (for parsing/construction)
    vgx_ExpressEvalOperation_t *_cursor;
    // Number of operations (including dummy slot)
    int _sz;
    // 
    vgx_StackItemType_t _current_string_enum_mode;
  } parser;
  
  // Debug info
  struct {
    bool enable;
    CStringQueue_t *info;
  } debug;
} vgx_ExpressEvalProgram_t;


#define VGX_EXPRESS_EVAL_MEMORY_OSTATIC   3 /* 2**3 = 8 slots */


#define VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE (sizeof(cacheline_t)/sizeof(DWORD))

CALIGNED_TYPE(union) u_vgx_ExpressEvalDWordSetSlot_t {
  cacheline_t data;
  DWORD entries[ VGX_EXPRESS_EVAL_INTEGER_SET_SLOT_SIZE ];
} vgx_ExpressEvalDWordSetSlot_t;


typedef struct s_vgx_ExpressEvalDWordSet_t {
  vgx_ExpressEvalDWordSetSlot_t *slots;
  uint32_t mask;
  uint32_t sz;
} vgx_ExpressEvalDWordSet_t;


typedef struct s_vgx_ExpressEvalMemory_t {

  // ==== CL1 ====
  // Q1.1
  vgx_EvalStackItem_t *data;
  // Q1.2
  uint64_t mask;
  // Q1.3.1
  int order;
  // Q1.3.2
  int refc;
  // Q1.4
  int64_t sp;
  // Q1.5
  CQwordList_t *cstringref;
  // Q1.8
  CQwordList_t *vectorref;
  // Q1.7-8
  vgx_ExpressEvalDWordSet_t dwset;
  // ==== CL2+3 ====
  // Q2.1-8
  // Q3.1-8
  vgx_EvalStackItem_t __data[ 1<<VGX_EXPRESS_EVAL_MEMORY_OSTATIC ];

} vgx_ExpressEvalMemory_t;




typedef struct s_vgx_ExpressEvalWorkRegisters_t {
  int sz;
  int __rsv;
  int64_t ncall; 
  vgx_EvalStackItem_t *cursor;
  vgx_EvalStackItem_t single;
  vgx_EvalStackItem_t *data;
} vgx_ExpressEvalWorkRegisters_t;



typedef struct s_vgx_ArcHeadHeapItem_t {
  double score;
  const vgx_Vertex_t *vertex;
  vgx_predicator_t predicator;
} vgx_ArcHeadHeapItem_t;



typedef struct s_vgx_ExpressEvalContext_t {
  //
  //           
  // Context:  (tail) - [prev] -> (vertex) - [next] -> (head)
  //                                     
  //

  // The tail vertex of the current vertex
  const vgx_Vertex_t *TAIL;

  // Arc leading from previous vertex to current vertex
  vgx_predicator_t arrive;

  // Current vertex being evaluated
  const vgx_Vertex_t *VERTEX;

  // Arc leading from current vertex to next vertex
  vgx_predicator_t exit;
  
  // The head of the current vertex
  const vgx_Vertex_t *HEAD;

  // Current rankscore
  double rankscore;

  // Default property value when vertex does not have property
  vgx_EvalStackItem_t default_prop;

  // Special work area for storing and loading data
  vgx_ExpressEvalMemory_t *memory;

  // Array used for mcull
  vgx_ArcHeadHeapItem_t *cullheap; 

  // Local scope objects
  struct {
    vgx_EvalStackItem_t *objects;
    int sz;
    int idx;
  } local_scope;

  // Working registers
  vgx_ExpressEvalWorkRegisters_t wreg;

  // Lockable arc
  vgx_LockableArc_t *larc;

  // Collector
  struct s_vgx_BaseCollector_context_t *collector;

  // Collector Type
  vgx_CollectorType_t collector_type;

  // Global timing budget
  vgx_ExecutionTimingBudget_t *timing_budget;

  // Thread ID
  uint32_t threadid;

} vgx_ExpressEvalContext_t;



typedef struct s_vgx_ExpressEvalPropertyCache_t {
  const vgx_Vertex_t *vertex;
  shortid_t keyhash;
  vgx_EvalStackItem_t item;
} vgx_ExpressEvalPropertyCache_t;


typedef struct s_vgx_ExpressEvalRelEncCache_t {
  const CString_t *CSTR__rel;   // last lookup string (if address is same, hit)
  QWORD relhash;                // last lookup hash of string, hit if match
  vgx_predicator_rel_enum rel;  // cached encoding
} vgx_ExpressEvalRelEncCache_t;


typedef struct s_vgx_ExpressEvalTypeEncCache_t {
  const CString_t *CSTR__type;      // last lookup string (if address is same, hit)
  QWORD typehash;                   // last lookup hash of string, hit if match
  vgx_VertexTypeEnumeration_t vtx;  // cached encoding
} vgx_ExpressEvalTypeEncCache_t;


typedef struct s_vgx_ExpressEvalCache_t {
  vgx_ExpressEvalPropertyCache_t HEAD;
  vgx_ExpressEvalPropertyCache_t VERTEX;
  vgx_ExpressEvalPropertyCache_t TAIL;
  vgx_ExpressEvalRelEncCache_t relationship;
  vgx_ExpressEvalTypeEncCache_t vertextype;
  CString_t *CSTR__tmp_prop;
  int64_t sz_tmp_prop;
} vgx_ExpressEvalCache_t;


/* VTABLE */
typedef struct s_vgx_Evaluator_vtable_t {
  /* base methods */
  COMLIB_VTABLE_HEAD
  /* Evaluator methods */
  int (*Reset)( struct s_vgx_Evaluator_t *self );
  void (*SetContext)( struct s_vgx_Evaluator_t *self, const vgx_Vertex_t *tail, const vgx_ArcHead_t *arc, vgx_Vector_t *vector, double rankscore );
  void (*SetDefaultProp)( struct s_vgx_Evaluator_t *self, vgx_EvalStackItem_t *default_prop );
  void (*OwnMemory)( struct s_vgx_Evaluator_t *self, vgx_ExpressEvalMemory_t *memory );
  void (*SetVector)( struct s_vgx_Evaluator_t *self, vgx_Vector_t *vector );
  void (*SetCollector)( struct s_vgx_Evaluator_t *self, struct s_vgx_BaseCollector_context_t *collector );
  void (*SetTimingBudget)( struct s_vgx_Evaluator_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
  vgx_EvalStackItem_t * (*Eval)( struct s_vgx_Evaluator_t *self );
  vgx_EvalStackItem_t * (*EvalVertex)( struct s_vgx_Evaluator_t *self, const vgx_Vertex_t *vertex );
  vgx_EvalStackItem_t * (*EvalArc)( struct s_vgx_Evaluator_t *self, vgx_LockableArc_t *next );
  int (*PrevDeref)( const struct s_vgx_Evaluator_t *self );
  int (*ThisDeref)( const struct s_vgx_Evaluator_t *self );
  int (*HeadDeref)( const struct s_vgx_Evaluator_t *self );
  int (*Traversals)( const struct s_vgx_Evaluator_t *self );
  int (*ThisNextAccess)( const struct s_vgx_Evaluator_t *self );
  int (*Identifiers)( const struct s_vgx_Evaluator_t *self );
  bool (*HasCull)( const struct s_vgx_Evaluator_t *self );
  int (*SynArcOps)( const struct s_vgx_Evaluator_t *self );
  int (*WRegOps)( const struct s_vgx_Evaluator_t *self );
  int (*ClearWReg)( struct s_vgx_Evaluator_t *self );
  int64_t (*GetWRegNCall)( const struct s_vgx_Evaluator_t *self );
  void (*ClearMCull)( struct s_vgx_Evaluator_t *self );
  vgx_StackItemType_t (*ValueType)( struct s_vgx_Evaluator_t *self );
  struct s_vgx_Evaluator_t * (*Clone)( struct s_vgx_Evaluator_t *self, vgx_Vector_t *vector );
  struct s_vgx_Evaluator_t * (*Own)( struct s_vgx_Evaluator_t *self );
  void (*Discard)( struct s_vgx_Evaluator_t *self );
} vgx_Evaluator_vtable_t;



/* DATA */
typedef struct s_vgx_Evaluator_t {
  COMLIB_OBJECT_HEAD( vgx_Evaluator_vtable_t )
  objectid_t id;
  // Operation cursor ("program counter" during eval)
  vgx_ExpressEvalOperation_t *op;
  // Evaluation stack
  vgx_EvalStackItem_t *sp;
  // Parent graph instance
  vgx_Graph_t *graph;
  // Context
  vgx_ExpressEvalContext_t context;
  // Operations (the "program")
  vgx_ExpressEvalProgram_t rpn_program;
  // Evaluation caches
  vgx_ExpressEvalCache_t cache;
  // The following information is populated when context is created
  struct {
    double t0;      // graph creation time (seconds since 1970)
    double tnow;    // graph current time (seconds since 1970)
    int64_t order;  // graph order
    int64_t size;   // graph size
    int64_t op;     // graph operation
    vgx_Vector_t * vector;  // default reference vector
  } current;
  // Refcount
  ATOMIC_VOLATILE_i64 _refc_atomic;
  //
  struct {
    int64_t i64;
    double f64;
    void *p1;
    void *p2;
  } tmp;
  // DEBUG
} vgx_Evaluator_t;


typedef struct s_vgx_Evaluator_constructor_args_t {
  vgx_Graph_t *parent;
  const char *expression;
  vgx_Vector_t *vector;
  CString_t **CSTR__error;
} vgx_Evaluator_constructor_args_t;


DLL_HIDDEN extern void vgx_Evaluator_RegisterClass( void );
DLL_HIDDEN extern void vgx_Evaluator_UnregisterClass( void );


/*******************************************************************//**
 *
 * vgx_IEvaluator_t
 *
 ***********************************************************************
 */
typedef struct s_vgx_IEvaluator_t {
  int (*Initialize)( void );
  int (*Destroy)( void );
  int (*CreateEvaluators)( vgx_Graph_t *graph );
  void (*DestroyEvaluators)( vgx_Graph_t *graph );
  vgx_Evaluator_t * (*NewEvaluator)( vgx_Graph_t *graph, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error );
  void (*DiscardEvaluator)( vgx_Evaluator_t **evaluator );
  int (*IsPositive)( const vgx_EvalStackItem_t *item );
  int64_t (*GetInteger)( const vgx_EvalStackItem_t *item );
  double (*GetReal)( const vgx_EvalStackItem_t *item );
  vgx_EvalStackItem_t (*GetNaN)( void );
  vgx_ExpressEvalStack_t * (*NewKeyValStack_CS)( vgx_Graph_t *graph, vgx_SelectProperties_t *properties );
  vgx_ExpressEvalStack_t * (*CloneKeyValStack_CS)( vgx_Evaluator_t *evaluator_CS, const vgx_Vertex_t *tail_RO, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id );
  void (*DiscardStack)( vgx_ExpressEvalStack_t **stack );
  void (*DiscardStack_CS)( vgx_ExpressEvalStack_t **stack_CS );
  vgx_ExpressEvalMemory_t * (*NewMemory)( int order );
  vgx_ExpressEvalMemory_t * (*CloneMemory)( vgx_ExpressEvalMemory_t *other );
  int (*OwnMemory)( vgx_ExpressEvalMemory_t *mem );
  void (*DiscardMemory)( vgx_ExpressEvalMemory_t **mem );
  int (*StoreCString)( vgx_Evaluator_t *self, const CString_t *CSTR__str );
  int64_t (*ClearCStrings)( vgx_ExpressEvalMemory_t *memory );
  int (*StoreVector)( vgx_Evaluator_t *self, const vgx_Vector_t *vector );
  int64_t (*ClearVectors)( vgx_ExpressEvalMemory_t *memory );
  int (*LocalAutoScopeObject)( vgx_Evaluator_t *self, vgx_EvalStackItem_t *item, bool delete_on_fail );
  void (*ClearLocalScope)( vgx_Evaluator_t *self );
  void (*DeleteLocalScope)( vgx_Evaluator_t *self );
  int64_t (*ClearDWordSet)( vgx_ExpressEvalMemory_t *memory );
  vgx_StringList_t * (*GetRpnDefinitions)( void );
} vgx_IEvaluator_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ResponseAttrFastMask vgx_query_response_attr_fastmask( const vgx_BaseQuery_t *query ) {
  switch( query->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    return ((vgx_NeighborhoodQuery_t*)query)->fieldmask;
  case VGX_QUERY_TYPE_GLOBAL:
    return ((vgx_GlobalQuery_t*)query)->fieldmask;
  default:
    return VGX_RESPONSE_ATTRS_NONE;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_ExecutionTimingBudget_t * vgx_query_timing_budget( vgx_BaseQuery_t *query ) {
  return &query->timing_budget;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int vgx_query_response_head_deref( const vgx_BaseQuery_t *query ) {
  vgx_Evaluator_t *evaluator;
  vgx_ResponseAttrFastMask fieldmask;
  switch( query->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    evaluator = ((vgx_NeighborhoodQuery_t*)query)->selector;
    fieldmask = ((vgx_NeighborhoodQuery_t*)query)->fieldmask;
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    evaluator = ((vgx_GlobalQuery_t*)query)->selector;
    fieldmask = ((vgx_GlobalQuery_t*)query)->fieldmask;
    break;
  default:
    evaluator = NULL;
    fieldmask = VGX_RESPONSE_ATTRS_NONE;
  }

  // Potential head deref based on mask
  if( fieldmask & VGX_RESPONSE_ATTRS_HEAD_DEREF ) {
    // Head fields other than property or only property and the evaluator needs to deref head
    if( ((fieldmask & VGX_RESPONSE_ATTRS_MASK) ^ VGX_RESPONSE_ATTR_PROPERTY) != 0
        ||
        ( evaluator 
          &&
          ( CALLABLE( evaluator )->HeadDeref( evaluator ) || CALLABLE( evaluator )->Identifiers( evaluator ) )
        ) )
    {
      return 1;
    }
  }

  return 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static bool _vgx_vertex_condition_is_recursive( const vgx_VertexCondition_t *VC ) {
  bool recursive = false;
  if( VC && ( 
        VC->advanced.recursive.conditional.evaluator          || VC->advanced.recursive.traversing.evaluator          ||
        VC->advanced.recursive.conditional.arc_condition_set  || VC->advanced.recursive.traversing.arc_condition_set  ||
        VC->advanced.recursive.conditional.vertex_condition   || VC->advanced.recursive.traversing.vertex_condition   ||
        VC->advanced.recursive.conditional.override.enable    || VC->advanced.recursive.traversing.override.enable    ||
        _vgx_collector_mode_collect( VC->advanced.recursive.collector_mode )
  )) {
    recursive = true;
  }
  return recursive;
}



struct s_vgx_Ranker_t;
typedef vgx_VertexSortValue_t (*f_vgx_ComputeArcDynamicRank)( struct s_vgx_Ranker_t *ranker, vgx_LockableArc_t *arc );
typedef vgx_VertexSortValue_t (*f_vgx_ComputeVertexDynamicRank)( struct s_vgx_Ranker_t *ranker, vgx_Vertex_t *vertex );

typedef struct s_vgx_ArcRankScorer_t {
  f_vgx_ComputeArcDynamicRank by_nothing;
  f_vgx_ComputeArcDynamicRank by_simscore;
  f_vgx_ComputeArcDynamicRank by_hamdist;
  f_vgx_ComputeArcDynamicRank by_composite;
} vgx_ArcRankScorer_t;

typedef struct s_vgx_VertexRankScorer_t {
  f_vgx_ComputeVertexDynamicRank by_nothing;
  f_vgx_ComputeVertexDynamicRank by_simscore;
  f_vgx_ComputeVertexDynamicRank by_hamdist;
  f_vgx_ComputeVertexDynamicRank by_composite;
} vgx_VertexRankScorer_t;

typedef struct s_vgx_Ranker_t {
  vgx_Similarity_t *simcontext;
  vgx_Vector_t *probe;
  bool locked_head_access;
  vgx_Graph_t *graph;
  vgx_Evaluator_t *evaluator;
  vgx_ExecutionTimingBudget_t *timing_budget;
  f_vgx_ComputeArcDynamicRank arc_score;
  f_vgx_ComputeVertexDynamicRank vertex_score;
} vgx_Ranker_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_ArcCellComparator)( const vgx_ArcVector_cell_t *arc_cell1, const vgx_ArcVector_cell_t *arc_cell2 );

typedef struct s_vgx_ArcCellComparator_t {
  f_vgx_ArcCellComparator cmp_arc_cell_head_memaddress;
  f_vgx_ArcCellComparator cmp_arc_cell_int_predicator;
  f_vgx_ArcCellComparator cmp_arc_cell_real_predicator;
} vgx_ArcCellComparator_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_ArcCellCollector_context_t {
  void *sequence;
  f_Cx2tptrSequence_additem add_arc_cell;
  int64_t remain;
} vgx_ArcCellCollector_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
ALIGNED_TYPE( union, 32 ) u_vgx_CollectorItem_t {
  // Four qwords
  // Q0: tailref
  // Q1: headref
  // Q2: predicator
  // Q3: fast sort value
  __m256i item;
  struct {
    vgx_VertexRef_t *tailref;
    vgx_VertexRef_t *headref;
    vgx_predicator_t predicator;
    vgx_VertexSortValue_t sort;
  };
} vgx_CollectorItem_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_ArcComparator)( const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );

typedef struct s_vgx_ArcComparator_t {
  f_vgx_ArcComparator cmp_archead_always_1;
  f_vgx_ArcComparator cmp_archead_internalid;
  f_vgx_ArcComparator cmp_arctail_internalid;
  f_vgx_ArcComparator cmp_archead_identifier;
  f_vgx_ArcComparator cmp_arctail_identifier;
  f_vgx_ArcComparator cmp_archead_int32_rank;
  f_vgx_ArcComparator cmp_archead_uint32_rank;
  f_vgx_ArcComparator cmp_archead_int64_rank;
  f_vgx_ArcComparator cmp_archead_uint64_rank;
  f_vgx_ArcComparator cmp_archead_float_rank;
  f_vgx_ArcComparator cmp_archead_double_rank;
} vgx_ArcComparator_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_CollectArc)( struct s_vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc );

typedef struct s_vgx_ArcCollector_t {
  f_vgx_CollectArc no_collect;
  f_vgx_CollectArc into_unsorted_list;
  f_vgx_CollectArc into_first_value_map;
  f_vgx_CollectArc into_max_value_map;
  f_vgx_CollectArc into_min_value_map;
  f_vgx_CollectArc into_average_value_map;
  f_vgx_CollectArc into_counting_map;
  f_vgx_CollectArc into_aggregating_sum_map;
  f_vgx_CollectArc into_aggregating_sqsum_map;
  f_vgx_CollectArc into_aggregating_product_map;
  f_vgx_CollectArc to_sort_by_integer_predicator;
  f_vgx_CollectArc to_sort_by_unsigned_predicator;
  f_vgx_CollectArc to_sort_by_real_predicator;
  f_vgx_CollectArc to_sort_by_any_predicator;
  f_vgx_CollectArc to_sort_by_memaddress;
  f_vgx_CollectArc to_sort_by_internalid;
  f_vgx_CollectArc to_sort_by_identifier;
  f_vgx_CollectArc to_sort_by_tail_internalid;
  f_vgx_CollectArc to_sort_by_tail_identifier;
  f_vgx_CollectArc to_sort_by_degree;
  f_vgx_CollectArc to_sort_by_indegree;
  f_vgx_CollectArc to_sort_by_outdegree;
  f_vgx_CollectArc to_sort_by_simscore;
  f_vgx_CollectArc to_sort_by_hamdist;
  f_vgx_CollectArc to_sort_by_rankscore;
  f_vgx_CollectArc to_sort_by_tmc;
  f_vgx_CollectArc to_sort_by_tmm;
  f_vgx_CollectArc to_sort_by_tmx;
  f_vgx_CollectArc to_sort_by_native_order;
  f_vgx_CollectArc to_sort_by_random_order;
} vgx_ArcCollector_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_StageArc)( struct s_vgx_ArcCollector_context_t *collector, vgx_LockableArc_t *larc, int index, vgx_predicator_t *predicator_override );

typedef struct s_vgx_ArcStager_t {
  f_vgx_StageArc no_stage;
  f_vgx_StageArc unsorted;
  f_vgx_StageArc for_first_value_map;
  f_vgx_StageArc for_max_value_map;
  f_vgx_StageArc for_min_value_map;
  f_vgx_StageArc for_average_value_map;
  f_vgx_StageArc for_counting_map;
  f_vgx_StageArc for_aggregating_sum_map;
  f_vgx_StageArc for_aggregating_sqsum_map;
  f_vgx_StageArc for_aggregating_product_map;
  f_vgx_StageArc to_sort_by_integer_predicator;
  f_vgx_StageArc to_sort_by_unsigned_predicator;
  f_vgx_StageArc to_sort_by_real_predicator;
  f_vgx_StageArc to_sort_by_any_predicator;
  f_vgx_StageArc to_sort_by_memaddress;
  f_vgx_StageArc to_sort_by_internalid;
  f_vgx_StageArc to_sort_by_identifier;
  f_vgx_StageArc to_sort_by_tail_internalid;
  f_vgx_StageArc to_sort_by_tail_identifier;
  f_vgx_StageArc to_sort_by_degree;
  f_vgx_StageArc to_sort_by_indegree;
  f_vgx_StageArc to_sort_by_outdegree;
  f_vgx_StageArc to_sort_by_simscore;
  f_vgx_StageArc to_sort_by_hamdist;
  f_vgx_StageArc to_sort_by_rankscore;
  f_vgx_StageArc to_sort_by_tmc;
  f_vgx_StageArc to_sort_by_tmm;
  f_vgx_StageArc to_sort_by_tmx;
  f_vgx_StageArc to_sort_by_native_order;
  f_vgx_StageArc to_sort_by_random_order;
} vgx_ArcStager_t;




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_VertexComparator)( const vgx_CollectorItem_t *item1, const vgx_CollectorItem_t *item2 );

typedef struct s_vgx_VertexComparator_t {
  f_vgx_VertexComparator cmp_vertex_always_1;
  f_vgx_VertexComparator cmp_vertex_internalid;
  f_vgx_VertexComparator cmp_vertex_identifier;
  f_vgx_VertexComparator cmp_vertex_int32_rank;
  f_vgx_VertexComparator cmp_vertex_uint32_rank;
  f_vgx_VertexComparator cmp_vertex_int64_rank;
  f_vgx_VertexComparator cmp_vertex_uint64_rank;
  f_vgx_VertexComparator cmp_vertex_float_rank;
  f_vgx_VertexComparator cmp_vertex_double_rank;
} vgx_VertexComparator_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef double (*f_vgx_RankScoreFromItem)( const vgx_CollectorItem_t *x );

typedef struct s_vgx_RankScoreFromItem_t {
  f_vgx_RankScoreFromItem from_none;
  f_vgx_RankScoreFromItem from_predicator;
  f_vgx_RankScoreFromItem from_int32;
  f_vgx_RankScoreFromItem from_int64;
  f_vgx_RankScoreFromItem from_uint32;
  f_vgx_RankScoreFromItem from_uint64;
  f_vgx_RankScoreFromItem from_float;
  f_vgx_RankScoreFromItem from_double;
  f_vgx_RankScoreFromItem from_qword;
} vgx_RankScoreFromItem_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_CollectVertex)( struct s_vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc );

typedef struct s_vgx_VertexCollector_t {
  f_vgx_CollectVertex no_collect;
  f_vgx_CollectVertex into_unsorted_list;
  f_vgx_CollectVertex to_sort_by_memaddress;
  f_vgx_CollectVertex to_sort_by_internalid;
  f_vgx_CollectVertex to_sort_by_identifier;
  f_vgx_CollectVertex to_sort_by_degree;
  f_vgx_CollectVertex to_sort_by_indegree;
  f_vgx_CollectVertex to_sort_by_outdegree;
  f_vgx_CollectVertex to_sort_by_simscore;
  f_vgx_CollectVertex to_sort_by_hamdist;
  f_vgx_CollectVertex to_sort_by_rankscore;
  f_vgx_CollectVertex to_sort_by_tmc;
  f_vgx_CollectVertex to_sort_by_tmm;
  f_vgx_CollectVertex to_sort_by_tmx;
  f_vgx_CollectVertex to_sort_by_native_order;
  f_vgx_CollectVertex to_sort_by_random_order;
} vgx_VertexCollector_t;




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef int (*f_vgx_StageVertex)( struct s_vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc, int index );

typedef struct s_vgx_VertexStager_t {
  f_vgx_StageVertex no_stage;
  f_vgx_StageVertex unsorted;
  f_vgx_StageVertex to_sort_by_memaddress;
  f_vgx_StageVertex to_sort_by_internalid;
  f_vgx_StageVertex to_sort_by_identifier;
  f_vgx_StageVertex to_sort_by_degree;
  f_vgx_StageVertex to_sort_by_indegree;
  f_vgx_StageVertex to_sort_by_outdegree;
  f_vgx_StageVertex to_sort_by_simscore;
  f_vgx_StageVertex to_sort_by_hamdist;
  f_vgx_StageVertex to_sort_by_rankscore;
  f_vgx_StageVertex to_sort_by_tmc;
  f_vgx_StageVertex to_sort_by_tmm;
  f_vgx_StageVertex to_sort_by_tmx;
  f_vgx_StageVertex to_sort_by_native_order;
  f_vgx_StageVertex to_sort_by_random_order;
} vgx_VertexStager_t;



/*******************************************************************//**
 * vgx_ArcAggregationKey_t
 * The mapping key is the vertex reference address and the arc predicator
 * relationship type plus modifier. Thus each unique combination of
 * relationship type and modifier will be counted separately.
 *
 ***********************************************************************
 */
typedef union u_vgx_ArcAggregationKey_t {
  QWORD bits;
  struct {
    /* Packed vertex reference pointer: discard 16 upper bits (48-bit address) and 3 lower bits (all zero for qword-aligned data) */
    /* When reconstituting the pointer, remember to sign-extend, e.g. using __TPTR_UNPACK */
    uint64_t vertexref_qwo : 45;
    /* Portion of arc predicator encoding relationship type and modifier */
    uint64_t pred_key      : 19;
  };
} vgx_ArcAggregationKey_t;



/*******************************************************************//**
 * vgx_ArcAggregatorValue_t
 * The mapped value  
 *
 *
 *
 ***********************************************************************
 */
typedef union u_vgx_ArcAggregationValue_t {
  QWORD bits;
  int64_t ibits;

  struct {
    union {
      int32_t integer;
      uint32_t uinteger;
      float real;
    };
    struct {
      uint32_t __na   : 8;
      uint32_t count  : 24;
    };
  };

} vgx_ArcAggregationValue_t;



/*******************************************************************//**
 * vgx_ArcAggregationItem_t
 * Several arcs terminating at the same vertex are collected as one instance
 * into a mapping aggregator.
 ***********************************************************************
 */
typedef union u_vgx_ArcAggregationItem_t {
  __m128i bits;
  struct {
    vgx_ArcAggregationKey_t key;
    vgx_ArcAggregationValue_t value;
  };
} vgx_ArcAggregationItem_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define VGX_COLLECTOR_STAGE_SIZE 4


ALIGNED_TYPE( struct, 32 ) s_vgx_CollectorStage_t {
  vgx_CollectorItem_t slot[ VGX_COLLECTOR_STAGE_SIZE ];
} vgx_CollectorStage_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#define __vgx_BaseCollector_context_HEAD      \
  vgx_CollectorType_t type;                   \
  vgx_Graph_t *graph;                         \
  vgx_Ranker_t *ranker;                       \
  union {                                     \
    comlib_object_t *object;                  \
    union {                                   \
      Cm256iHeap_t *heap;                     \
      Cm256iList_t *list;                     \
    } sequence;                               \
    union {                                   \
      framehash_t *aggregator;                \
    } mapping;                                \
    vgx_aggregator_fields_t *fields;          \
  } container;                                \
  vgx_VertexRef_t *refmap;                    \
  int64_t sz_refmap;                          \
  vgx_CollectorStage_t *stage;                \
  Cm256iHeap_t *postheap;                     \
  int64_t size;                               \
  int64_t n_remain;                           \
  int64_t n_collectable;                      \
  bool locked_tail_access;                    \
  bool locked_head_access;                    \
  vgx_ResponseAttrFastMask fieldmask;         \
  vgx_ExecutionTimingBudget_t *timing_budget;


typedef struct s_vgx_BaseCollector_context_t {
  __vgx_BaseCollector_context_HEAD
} vgx_BaseCollector_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_ArcCollector_context_t {
  __vgx_BaseCollector_context_HEAD
  f_vgx_CollectArc collect_arc;
  f_vgx_StageArc stage_arc;
  f_vgx_CollectArc post_collect;
  f_vgx_StageArc post_stage;
  vgx_virtual_ArcFilter_context_t *postfilter;
  int64_t n_arcs;
  int64_t n_neighbors;
  bool counts_are_deep;
} vgx_ArcCollector_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexCollector_context_t {
  __vgx_BaseCollector_context_HEAD
  f_vgx_CollectVertex collect_vertex;
  f_vgx_StageVertex stage_vertex;
  int64_t n_vertices;
  bool counts_are_deep;
} vgx_VertexCollector_context_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static bool _vgx_collector_is_aggregation( vgx_BaseCollector_context_t *collector ) {
  return (int)collector->type & __VGX_COLLECTOR_AGGREGATION ? true : false;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static bool _vgx_collector_is_sorted( vgx_BaseCollector_context_t *collector ) {
  return (int)collector->type & __VGX_COLLECTOR_SORTED ? true : false;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_ranking_context_t {
  vgx_sortspec_t sortspec;
  vgx_predicator_mod_t modifier;
  vgx_Graph_t *graph;
  bool readonly_graph;
  struct s_vgx_Similarity_t *simcontext;
  struct s_vgx_Vector_t *vector;
  FP_t fingerprint;
  vgx_Evaluator_t *evaluator;
  vgx_ExecutionTimingBudget_t *timing_budget;
  vgx_virtual_ArcFilter_context_t *postfilter_context;
} vgx_ranking_context_t;



#define BASE_SEARCH_CONTEXT_HEAD              \
  vgx_search_type type;                       \
  vgx_query_debug debug;                      \
  vgx_ExecutionTimingBudget_t *timing_budget; \
  CString_t *CSTR__error;                     \
  vgx_Similarity_t *simcontext;               \
  vgx_Evaluator_t *pre_evaluator;             \
  vgx_Evaluator_t *vertex_evaluator;          \
  vgx_Evaluator_t *post_evaluator;            \
  vgx_ranking_context_t *ranking_context;


#define RESULT_SET_SEARCH_CONTEXT_HEAD    \
  int offset;                             \
  int64_t hits;                           \
  vgx_BaseCollector_context_t *result;


#define AGGREGATION_SEARCH_CONTEXT_HEAD   \
  vgx_aggregator_fields_t *fields;


#define ADJACENCY_SEARCH_CONTEXT_HEAD     \
  BASE_SEARCH_CONTEXT_HEAD                \
  vgx_Vertex_t *anchor;                   \
  int64_t n_arcs;                         \
  union {                                 \
    int64_t n_neighbors;                  \
    int64_t n_vertices;                   \
  };                                      \
  bool counts_are_deep;                   \
  vgx_neighborhood_probe_t *probe;


#define GLOBAL_SEARCH_CONTEXT_HEAD        \
  BASE_SEARCH_CONTEXT_HEAD                \
  int64_t n_items;                        \
  bool counts_are_deep;                   \
  vgx_vertex_probe_t *probe;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_base_search_context_t {
  BASE_SEARCH_CONTEXT_HEAD
} vgx_base_search_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_adjacency_search_context_t {
  ADJACENCY_SEARCH_CONTEXT_HEAD
} vgx_adjacency_search_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_neighborhood_search_context_t {
  ADJACENCY_SEARCH_CONTEXT_HEAD
  RESULT_SET_SEARCH_CONTEXT_HEAD
  //
  vgx_collector_mode_t collector_mode;    // collect on this level? if so collect arcs or vertices?
  vgx_BaseCollector_context_t *collector; // shared collector instance for all neighborhood levels
} vgx_neighborhood_search_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_global_search_context_t {
  GLOBAL_SEARCH_CONTEXT_HEAD
  RESULT_SET_SEARCH_CONTEXT_HEAD
  struct {
    vgx_collector_mode_t mode;
    union {
      vgx_VertexCollector_context_t *vertex;
      vgx_ArcCollector_context_t *arc;
      vgx_BaseCollector_context_t *base;
    };
  } collector;
} vgx_global_search_context_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_aggregator_search_context_t {
  ADJACENCY_SEARCH_CONTEXT_HEAD
  AGGREGATION_SEARCH_CONTEXT_HEAD
  //
  vgx_collector_mode_t collector_mode;    // collect on this level? if so collect arcs or vertices?
  vgx_BaseCollector_context_t *collector; // shared collector instance for all neighborhood levels
} vgx_aggregator_search_context_t;





/*******************************************************************//**
 * Callback function called when initial vertex connects to terminal
 * vertex with given predicator.
 ***********************************************************************
 */
typedef int (*f_Vertex_connect_event)( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, int terminal_refdelta );


/*******************************************************************//**
 * Callback function called when initial vertex disconnects from terminal
 * vertex with given predicator (or predicator NULL when all/multiple arcs
 * disconnected.)
 ***********************************************************************
 */
typedef int64_t (*f_Vertex_disconnect_event)( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, int64_t terminal_refdelta, vgx_ExecutionTimingBudget_t *timing_budget );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IArcVector_t {
  // TODO! Add filter functionality, pass in some kind of globally understood filter structure
  // that contains filtering criteria, boolean logic, etc. Add this to the methods that return stuff.

  vgx_ArcVector_cell_t * (*SetNoArc)(vgx_ArcVector_cell_t *V );
  bool (*HasNoArc)( const vgx_ArcVector_cell_t *V );
  _vgx_ArcVector_cell_type (*CellType)( const vgx_ArcVector_cell_t * const V );
  int64_t (*Degree)( const vgx_ArcVector_cell_t *V );
  int (*Add)( framehash_dynamic_t *dynamic, vgx_Arc_t * arc, f_Vertex_connect_event connect_event );
  int64_t (*Remove)( framehash_dynamic_t *dynamic, vgx_Arc_t *arc, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event disconnect_event );
  int64_t (*Expire)( framehash_dynamic_t *dynamic, vgx_Vertex_t *vertex_WL, uint32_t now_ts, uint32_t *next_ts );
  vgx_ArcVector_cell_t * (*GetArcCell)( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, const vgx_Vertex_t *KEY_vertex, vgx_ArcVector_cell_t *ret_arc_cell );
  vgx_predicator_t (*GetArcValue)( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, vgx_ArcHead_t *arc_head );
  vgx_ArcFilter_match (*GetArcs)( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
  vgx_ArcFilter_match (*GetArcsBidirectional)( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
  vgx_ArcFilter_match (*GetVertices)( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe );
  vgx_ArcFilter_match (*GetVerticesBidirectional)( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
  vgx_ArcFilter_match (*HasArc)( const vgx_ArcVector_cell_t *V, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_Arc_t *first_match );
  vgx_ArcFilter_match (*HasArcBidirectional)( const vgx_ArcVector_cell_t *V_IN, const vgx_ArcVector_cell_t *V_OUT, vgx_neighborhood_probe_t *neighborhood_probe );
  int64_t (*Serialize)( const vgx_ArcVector_cell_t *V, CQwordQueue_t *output );
  int64_t (*Deserialize)( vgx_Vertex_t *tail, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, vgx_ArcVector_cell_t *V, CQwordQueue_t *input );
  void (*PrintDebugDump)( const vgx_ArcVector_cell_t *V, const char *message );
} vgx_IArcVector_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_ArcFilterFunction_t {
  f_vgx_ArcFilter Pass;
  f_vgx_ArcFilter Stop;
  f_vgx_ArcFilter RelationshipFilter;
  f_vgx_ArcFilter ModifierFilter;
  f_vgx_ArcFilter ValueFilter;
  f_vgx_ArcFilter HamDistFilter;
  f_vgx_ArcFilter SpecificFilter;
  f_vgx_ArcFilter RelationshipValueFilter;
  f_vgx_ArcFilter RelationshipHamDistFilter;
  f_vgx_ArcFilter ModifierValueFilter;
  f_vgx_ArcFilter ModifierHamDistFilter;
  f_vgx_ArcFilter SpecificValueFilter;
  f_vgx_ArcFilter SpecificHamDistFilter;
  f_vgx_ArcFilter EvaluatorFilter;
  f_vgx_ArcFilter GenericArcFilter;
  f_vgx_ArcFilter GenPredLocEvalVertexArcFilter;
  f_vgx_ArcFilter GenLocEvalVertexArcFilter;
  f_vgx_ArcFilter GenPredVertexArcFilter;
  f_vgx_ArcFilter GenVertexArcFilter;
  f_vgx_ArcFilter DirectRecursionArcFilter;
  f_vgx_ArcFilter GenPredLocEvalArcFilter;
  f_vgx_ArcFilter GenLocEvalArcFilter;
  f_vgx_ArcFilter GenPredArcFilter;
} vgx_ArcFilterFunction_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexFilterFunction_t {
  f_vgx_VertexFilter Pass;
  f_vgx_VertexFilter GenericVertexFilter;
  f_vgx_VertexFilter EvaluatorVertexFilter;
} vgx_VertexFilterFunction_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_PredicatorMatchFunction_t {
  f_vgx_PredicatorMatchFunction Relationship;
  f_vgx_PredicatorMatchFunction Modifier;
  f_vgx_PredicatorMatchFunction Key;
  f_vgx_PredicatorMatchFunction Any;
  f_vgx_PredicatorMatchFunction Generic;
} vgx_PredicatorMatchFunction_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_VertexMatchFunction_t {
  f_vgx_VertexIdentifierMatchFunction Identifier;
  f_vgx_VertexIdentifierListMatchFunction IdentifierList;
  f_vgx_VertexTypeMatchFunction Type;
  f_vgx_VertexMatchFunction Degree;
  f_vgx_VertexTimestampMatchFunction Timestamp;
  f_vgx_VertexSimilarityMatchFunction Similarity;
} vgx_VertexMatchFunction_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IArcFilter_t {
  vgx_virtual_ArcFilter_context_t * (*New)( vgx_Graph_t *graph, bool readonly_graph, const vgx_ArcConditionSet_t *arc_condition_set, const vgx_vertex_probe_t *vertex_probe, vgx_Evaluator_t *traversing_evaluator, vgx_ExecutionTimingBudget_t *timing_budget );
  vgx_virtual_ArcFilter_context_t * (*Clone)( const vgx_virtual_ArcFilter_context_t *other );
  void (*Delete)( vgx_virtual_ArcFilter_context_t **filter );
  vgx_boolean_logic (*LogicFromPredicators)( const vgx_predicator_t predicator1, vgx_predicator_t const predicator2 );
  int (*ConfigurePredicatorsFromArcConditionSet)( vgx_Graph_t *self, const vgx_ArcConditionSet_t *arc_condition_set, vgx_predicator_t *predicator1, vgx_predicator_t *predicator2 );
} vgx_IArcFilter_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IVertexFilter_t {
  vgx_VertexFilter_context_t * (*New)( vgx_vertex_probe_t *vertex_probe, vgx_ExecutionTimingBudget_t *timing_budget );
  vgx_VertexFilter_context_t * (*Clone)( const vgx_VertexFilter_context_t *other );
  void (*Delete)( vgx_VertexFilter_context_t **filter );
} vgx_IVertexFilter_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IEnumerator_t {
  // Relationship
  struct {
    vgx_predicator_rel_enum (*Set)( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
    vgx_predicator_rel_enum (*Encode)( vgx_Graph_t *self, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance, bool create_nonexist );
    uint64_t (*GetEnum)( vgx_Graph_t *self, const CString_t *CSTR__relationship );
    const CString_t * (*Decode)( vgx_Graph_t *self, vgx_predicator_rel_enum enc );
    void (*ClearCache)( void );
    bool (*Exists)( vgx_Graph_t *self, const CString_t *CSTR__relationship );
    bool (*ExistsEnum)( vgx_Graph_t *self, vgx_predicator_rel_enum enc );
    int (*GetAll)( vgx_Graph_t *self, CtptrList_t *output );
    int (*GetEnums)( vgx_Graph_t *self, CtptrList_t *output );
    int64_t (*Count)( vgx_Graph_t *self );
    int64_t (*OpSync)( vgx_Graph_t *self );
    int (*Remove)( vgx_Graph_t *self, vgx_predicator_rel_enum enc );
  } Relationship;
  // Vertex Type
  struct {
    vgx_vertex_type_t (*Set)( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
    vgx_vertex_type_t (*Encode)( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance, bool create_nonexist );
    uint64_t (*GetEnum)( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
    const CString_t * (*Decode)( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
    void (*ClearCache)( void );
    bool (*Exists)( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
    bool (*ExistsEnum)( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
    int (*GetAll)( vgx_Graph_t *self, CtptrList_t *output );
    int (*GetEnums)( vgx_Graph_t *self, CtptrList_t *output );
    int64_t (*Count)( vgx_Graph_t *self );
    int64_t (*OpSync)( vgx_Graph_t *self );
    int (*Remove)( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
    struct {
      const CString_t * (*None)( vgx_Graph_t *self );
      const CString_t * (*Vertex)( vgx_Graph_t *self );
      const CString_t * (*System)( vgx_Graph_t *self );
      const CString_t * (*Unknown)( vgx_Graph_t *self );
      const CString_t * (*LockObject)( vgx_Graph_t *self );
    } Reserved;
  } VertexType;
  // Dimension
  struct {
    feature_vector_dimension_t (*Set)( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode );
    feature_vector_dimension_t (*EncodeChars)( vgx_Similarity_t *self, const char *dimension, QWORD *ret_dimhash, bool create_nonexist );
    const CString_t * (*Decode)( vgx_Similarity_t *self, feature_vector_dimension_t dim );
    int64_t (*Own)( vgx_Similarity_t *self, feature_vector_dimension_t dim );
    int64_t (*Discard)( vgx_Similarity_t *self, feature_vector_dimension_t dim );
    bool (*Exists)( vgx_Similarity_t *self, const char *dimension );
    bool (*ExistsEnum)( vgx_Similarity_t *self, feature_vector_dimension_t dim );
    int (*GetAll)( vgx_Similarity_t *self, CtptrList_t *output );
    int64_t (*Count)( vgx_Similarity_t *self );
    int64_t (*OpSync)( vgx_Similarity_t *self );
    int64_t (*Remain)( vgx_Similarity_t *self );
  } Dimension;
  // Property
  struct {
    // Key
    struct {
      shortid_t (*Set)( vgx_Graph_t *self, CString_t *CSTR__key );
      shortid_t (*SetChars)( vgx_Graph_t *self, const char *key );
      CString_t * (*Get)( vgx_Graph_t *self, shortid_t keyhash );
      CString_t * (*New)( vgx_Graph_t *self, const char *key );
      CString_t * (*NewSelect)( vgx_Graph_t *self, const char *key, shortid_t *keyhash );
      shortid_t (*Encode)( vgx_Graph_t *self, const CString_t *CSTR__key, CString_t **CSTR__mapped_instance, bool create_nonexist );
      uint64_t (*GetEnum)( vgx_Graph_t *self, const CString_t *CSTR__key );
      const CString_t * (*Decode)( vgx_Graph_t *self, shortid_t keyhash );
      int64_t (*Own)( vgx_Graph_t *self, CString_t *CSTR__key );
      int64_t (*OwnByHash)( vgx_Graph_t *self, shortid_t keyhash );
      int64_t (*Discard)( vgx_Graph_t *self, CString_t *CSTR__key );
      int64_t (*DiscardByHash)( vgx_Graph_t *self, shortid_t keyhash );
      int (*GetAll)( vgx_Graph_t *self, CtptrList_t *CSTR__output );
      int64_t (*Count)( vgx_Graph_t *self );
      int64_t (*OpSync)( vgx_Graph_t *self );
    } Key;
    // Value
    struct {
      CString_t * (*StoreChars)( vgx_Graph_t *self, const char *value, int32_t len );
      CString_t * (*Store)( vgx_Graph_t *self, const CString_t *CSTR__value );
      CString_t * (*Get)( vgx_Graph_t *self, const objectid_t *value_obid );
      int64_t (*Own)( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
      int64_t (*Discard)( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
      int (*GetAll)( vgx_Graph_t *self, CtptrList_t *CSTR__output );
      int64_t (*Count)( vgx_Graph_t *self );
      int64_t (*OpSync)( vgx_Graph_t *self );
    } Value;
  } Property;
} vgx_IEnumerator_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t _vgx_enum__dimhash( const char *data, int *sz ) {
  *sz = (int)strnlen( data, MAX_FEATURE_VECTOR_TERM_LEN );
  return hash64( (const unsigned char*)data, *sz );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IString_t {
  CString_t * (*New)( object_allocator_context_t *allocator_context, const char *str );
  CString_t * (*NewLen)( object_allocator_context_t *allocator_context, const char *str, int len );
  int (*SetNew)( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *str );
  CString_t * (*NewFormat)( object_allocator_context_t *allocator_context, const char *format, ... );
  int (*SetNewFormat)( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, ... );
  CString_t * (*NewVFormat)( object_allocator_context_t *allocator_context, const char *format, va_list *args );
  int (*SetNewVFormat)( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, va_list *args );
  CString_t * (*Clone)( const CString_t *CSTR__obj );
  void (*Discard)( CString_t **CSTR__obj );

  // List
  struct {
    vgx_StringList_t * (*New)( object_allocator_context_t *allocator_context, int64_t sz );
    CString_t * (*SetItem)( vgx_StringList_t *list, int64_t i, const char *str );
    CString_t * (*SetItemLen)( vgx_StringList_t *list, int64_t i, const char *str, int len );
    CString_t * (*SetItemSteal)( vgx_StringList_t *list, int64_t i, CString_t **CSTR__str );
    CString_t * (*GetItem)( vgx_StringList_t *list, int64_t i );
    CString_t * (*GetOwnItem)( vgx_StringList_t *list, int64_t i );
    const char * (*GetChars)( const vgx_StringList_t *list, int64_t i );
    const objectid_t * (*GetObid)( vgx_StringList_t *list, int64_t i );
    int (*Contains)( const vgx_StringList_t *list, const char *str, bool ignore_case );
    int (*RemoveItem)( vgx_StringList_t *list, const char *str, bool ignore_case );
    CString_t * (*Append)( vgx_StringList_t *list, const char *str );
    CString_t * (*AppendSteal)( vgx_StringList_t *list, CString_t **CSTR__str );
    CString_t ** (*Data)( vgx_StringList_t *list );
    int64_t (*Size)( const vgx_StringList_t *list );
    void (*Sort)( vgx_StringList_t *list, bool ascending );
    vgx_StringList_t * (*Clone)( const vgx_StringList_t *list );
    void (*Discard)( vgx_StringList_t **list );
  } List;

  // Serialization
  struct {
    void (*SetAllocatorContext)( object_allocator_context_t *context );
    object_allocator_context_t * (*GetAllocatorContext)( void );
    int64_t (*Serialize)( const CString_t *CSTR__string, CQwordQueue_t *output );
    CString_t * (*DeserializeNolock)( comlib_object_t *container, CQwordQueue_t *input );
    char * (*Dumps)( const CString_t *CSTR__string );
    CString_t * (*Loads)( const char *data );
  } Serialization;
  
  // Validate
  struct {
    int (*StorableKey)( const char *key );
    int (*SelectKey)( const char *key );
    int (*UriPathSegment)( const char *key );
  } Validate;
  
  // Utility
  struct {
    CString_t * (*NewGraphMapName)( vgx_Graph_t *self, const CString_t *CSTR__prefix );
    int (*ListDir)( const char *dirpath, const char *query, vgx_StringList_t **rlist );
  } Utility;
  
  // Parse
  struct {
    CString_t * (*AllowPrefixWildcard)( vgx_Graph_t *self, const char *probe, vgx_value_comparison *vcomp, CString_t **CSTR__error );
  } Parse;

} vgx_IString_t;



#define PUSH_STRING_ALLOCATOR_CONTEXT_CURRENT_THREAD( ObjectAllocatorContext )                                    \
  do {                                                                                                            \
    /* Save current state */                                                                                      \
    f_CString_serializer_t __previous_serializer__ = NULL;                                                        \
    f_CString_deserializer_t __previous_deserializer__ = NULL;                                                    \
    object_allocator_context_t *__previous_alloc__ = iString.Serialization.GetAllocatorContext();                 \
    /* Set new state */                                                                                           \
    iString.Serialization.SetAllocatorContext( ObjectAllocatorContext );                                          \
    CStringGetSerializationCurrentThread( &__previous_serializer__, &__previous_deserializer__ );                 \
    CStringSetSerializationCurrentThread( iString.Serialization.Serialize, iString.Serialization.DeserializeNolock );


#define POP_STRING_ALLOCATOR_CONTEXT                                                                \
    /* Restore previous state */                                                                    \
    CStringSetSerializationCurrentThread( __previous_serializer__, __previous_deserializer__ );     \
    iString.Serialization.SetAllocatorContext( __previous_alloc__ );                 \
  } WHILE_ZERO



/*******************************************************************//**
 * IRelation_t
 ***********************************************************************
 */
typedef struct s_vgx_IRelataion_t {

  int                       (*ParseModifierValue)(  const vgx_predicator_modifier_enum mod_enum, const void *value, vgx_predicator_val_t *pval );
  struct s_vgx_Relation_t * (*New)(                 vgx_Graph_t *graph, const char *initial, const char *terminal, const char *relationship, vgx_predicator_modifier_enum mod_enum, const void *value );
  void                      (*Delete)(              struct s_vgx_Relation_t **relation );
  struct s_vgx_Relation_t * (*ForwardOnly)(         struct s_vgx_Relation_t *relation );
  struct s_vgx_Relation_t * (*AutoTimestamps)(      struct s_vgx_Relation_t *relation );
  struct s_vgx_Relation_t * (*Set)(                 struct s_vgx_Relation_t *relation, const char *initial, const char *terminal, const char *relationship, const vgx_predicator_modifier_enum *modifier, const void *value );
  struct s_vgx_Relation_t * (*Add)(                 struct s_vgx_Relation_t *relation, CString_t **CSTR__initial, CString_t **CSTR__terminal, CString_t **CSTR__relationship, const vgx_predicator_modifier_enum *modifier, const void *value );
  struct s_vgx_Relation_t * (*Unset)(               struct s_vgx_Relation_t *relation );
  struct s_vgx_Relation_t * (*SetInitial)(          struct s_vgx_Relation_t *relation, const char *initial );
  struct s_vgx_Relation_t * (*AddInitial)(          struct s_vgx_Relation_t *relation, CString_t **CSTR__initial );
  struct s_vgx_Relation_t * (*SetTerminal)(         struct s_vgx_Relation_t *relation, const char *terminal );
  struct s_vgx_Relation_t * (*AddTerminal)(         struct s_vgx_Relation_t *relation, CString_t **CSTR__terminal );
  struct s_vgx_Relation_t * (*SetRelationship)(     struct s_vgx_Relation_t *relation, const char *relationship );
  struct s_vgx_Relation_t * (*AddRelationship)(     struct s_vgx_Relation_t *relation, CString_t **CSTR__relationship );
  struct s_vgx_Relation_t * (*SetModifierAndValue)( struct s_vgx_Relation_t *relation, const vgx_predicator_modifier_enum modifier, const void *value );
  struct s_vgx_Arc_t *      (*SetStoredRelationship)( vgx_Graph_t *graph, struct s_vgx_Arc_t *arc, struct s_vgx_Relationship_t *relationship );
  struct s_vgx_Arc_t *      (*SetStoredArc_CS)(     vgx_Graph_t *graph, struct s_vgx_Arc_t *arc, struct s_vgx_Vertex_t *initial, struct s_vgx_Vertex_t *terminal, const struct s_vgx_Relation_t *relation );
  struct s_vgx_Arc_t *      (*SetStoredArc_OPEN)(   vgx_Graph_t *graph, struct s_vgx_Arc_t *arc, struct s_vgx_Vertex_t *initial, struct s_vgx_Vertex_t *terminal, const struct s_vgx_Relation_t *relation );
} vgx_IRelation_t;





/*******************************************************************//**
 *
 * vxgraph_query
 *
 ***********************************************************************
 */


/*******************************************************************//**
 * IStringParser_t
 ***********************************************************************
 */
typedef struct s_vgx_IStringParser_t {
  CString_t * (*ParseAllowPrefixWildcard)( const char *probe, vgx_value_comparison *vcomp, const CString_t **CSTR__error );
} vgx_IStringParser_t;



/*******************************************************************//**
 * IGraphQuery_t
 ***********************************************************************
 */
typedef struct s_vgx_IGraphQuery_t {

  void (*EmptyAdjacencyQuery)(      vgx_AdjacencyQuery_t    *query );
  void (*EmptyNeighborhoodQuery)(   vgx_NeighborhoodQuery_t *query );
  void (*EmptyGlobalQuery)(         vgx_GlobalQuery_t       *query );
  void (*EmptyAggregatorQuery)(     vgx_AggregatorQuery_t   *query );
  void (*EmptyQuery)(               vgx_BaseQuery_t         *query );

  void (*ResetAdjacencyQuery)(      vgx_AdjacencyQuery_t    *query );
  void (*ResetNeighborhoodQuery)(   vgx_NeighborhoodQuery_t *query );
  void (*ResetGlobalQuery)(         vgx_GlobalQuery_t       *query );
  void (*ResetAggregatorQuery)(     vgx_AggregatorQuery_t   *query );
  void (*ResetQuery)(               vgx_BaseQuery_t         *query );

  void (*DeleteAdjacencyQuery)(     vgx_AdjacencyQuery_t    **query );
  void (*DeleteNeighborhoodQuery)(  vgx_NeighborhoodQuery_t **query );
  void (*DeleteGlobalQuery)(        vgx_GlobalQuery_t       **query );
  void (*DeleteAggregatorQuery)(    vgx_AggregatorQuery_t   **query );
  void (*DeleteQuery)(              vgx_BaseQuery_t         **query );

  vgx_AdjacencyQuery_t    * (*NewAdjacencyQuery)(     vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
  vgx_NeighborhoodQuery_t * (*NewNeighborhoodQuery)(  vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode, CString_t **CSTR__error );
  vgx_GlobalQuery_t       * (*NewGlobalQuery)(        vgx_Graph_t *graph, vgx_collector_mode_t collector_mode, CString_t **CSTR__error );
  vgx_AggregatorQuery_t   * (*NewAggregatorQuery)(    vgx_Graph_t *graph, const char *vertex_id, vgx_ArcConditionSet_t **collect_arc_condition_set, CString_t **CSTR__error );

  vgx_AdjacencyQuery_t    * (*NewDefaultAdjacencyQuery)(     vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
  vgx_NeighborhoodQuery_t * (*NewDefaultNeighborhoodQuery)(  vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );
  vgx_GlobalQuery_t       * (*NewDefaultGlobalQuery)(        vgx_Graph_t *graph, CString_t **CSTR__error );
  vgx_AggregatorQuery_t   * (*NewDefaultAggregatorQuery)(    vgx_Graph_t *graph, const char *vertex_id, CString_t **CSTR__error );

  vgx_AdjacencyQuery_t    * (*CloneAdjacencyQuery)(     const vgx_AdjacencyQuery_t    *other, CString_t **CSTR__error );
  vgx_NeighborhoodQuery_t * (*CloneNeighborhoodQuery)(  const vgx_NeighborhoodQuery_t *other, CString_t **CSTR__error );
  vgx_GlobalQuery_t       * (*CloneGlobalQuery)(        const vgx_GlobalQuery_t       *other, CString_t **CSTR__error );
  vgx_AggregatorQuery_t   * (*CloneAggregatorQuery)(    const vgx_AggregatorQuery_t   *other, CString_t **CSTR__error );

} vgx_IGraphQuery_t;



/*******************************************************************//**
 * IVertexCondition_t
 ***********************************************************************
 */
typedef struct s_vgx_IVertexCondition_t {

  // Lifecycle
  vgx_VertexCondition_t * (*New)( bool positive_match );
  vgx_VertexCondition_t * (*Clone)( const vgx_VertexCondition_t *other );
  void (*Delete)( vgx_VertexCondition_t **vertex_condition );
  void (*Reset)( vgx_VertexCondition_t *vertex_condition );

  // Conditions
  void (*RequireManifestation)(       vgx_VertexCondition_t *vertex_condition, vgx_VertexStateContext_man_t manifestation );
  int (*RequireType)(                 vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const vgx_value_comparison vcomp, const char *type );
  vgx_VertexTypeEnumeration_t (*GetTypeEnumeration)( vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph );
  int (*RequireLocalFilter)(          vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *local_filter_expression );
  int (*RequirePostFilter)(           vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *post_filter_expression );
  int (*RequireDegree)(               vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const vgx_arc_direction direction, const int64_t degree );
  int (*RequireConditionalDegree)(    vgx_VertexCondition_t *vertex_condition, vgx_DegreeCondition_t **degree_condition );
  int (*RequireSimilarity)(           vgx_VertexCondition_t *vertex_condition, vgx_SimilarityCondition_t **similarity_condition );
  int (*RequireIdentifier)(           vgx_VertexCondition_t *vertex_condition, const vgx_value_comparison vcomp, const char *identifier );
  int (*RequireIdentifierList)(       vgx_VertexCondition_t *vertex_condition, vgx_StringList_t **idlist );
  int (*InitPropertyConditionSet)(    vgx_VertexCondition_t *vertex_condition, bool positive );
  int (*RequireProperty)(             vgx_VertexCondition_t *vertex_condition, vgx_VertexProperty_t **vertex_property );
  int (*InitTimestampConditions)(     vgx_VertexCondition_t *vertex_condition, bool positive );
  int (*RequireCreationTime)(         vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmc_condition );
  int (*RequireModificationTime)(     vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmm_condition );
  int (*RequireExpirationTime)(       vgx_VertexCondition_t *vertex_condition, const vgx_value_condition_t tmx_condition );

  void (*RequireRecursiveCondition)(  vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition );
  void (*RequireArcCondition)(        vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set );
  int (*RequireConditionFilter)(      vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression );
  void (*SetAssertCondition)(          vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match );

  void (*RequireRecursiveTraversal)(  vgx_VertexCondition_t *vertex_condition, vgx_VertexCondition_t **neighbor_condition );
  void (*RequireArcTraversal)(        vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **arc_condition_set );
  int (*RequireTraversalFilter)(      vgx_VertexCondition_t *vertex_condition, vgx_Graph_t *graph, const char *filter_expression );
  void (*SetAssertTraversal)(          vgx_VertexCondition_t *vertex_condition, vgx_ArcFilter_match assert_match );

  void (*CollectNeighbors)(           vgx_VertexCondition_t *vertex_condition, vgx_ArcConditionSet_t **collect_arc_condition_set, vgx_collector_mode_t collector_mode );

  int (*HasManifestation)(            const vgx_VertexCondition_t *vertex_condition );
  int (*HasType)(                     const vgx_VertexCondition_t *vertex_condition );
  int (*HasLocalFilter)(              const vgx_VertexCondition_t *vertex_condition );
  int (*HasPostFilter)(               const vgx_VertexCondition_t *vertex_condition );
  int (*HasDegree)(                   const vgx_VertexCondition_t *vertex_condition );
  int (*HasConditionalDegree)(        const vgx_VertexCondition_t *vertex_condition );
  int (*HasSimilarity)(               const vgx_VertexCondition_t *vertex_condition );
  vgx_Vector_t * (*OwnSimilarityVector)( vgx_VertexCondition_t *vertex_condition );
  int (*HasIdentifier)(               const vgx_VertexCondition_t *vertex_condition );
  int (*HasProperty)(                 const vgx_VertexCondition_t *vertex_condition );
  int (*HasCreationTime)(             const vgx_VertexCondition_t *vertex_condition );
  int (*HasModificationTime)(         const vgx_VertexCondition_t *vertex_condition );
  int (*HasExpirationTime)(           const vgx_VertexCondition_t *vertex_condition );
  int (*HasRecursiveCondition)(       const vgx_VertexCondition_t *vertex_condition );
  int (*HasArcCondition)(             const vgx_VertexCondition_t *vertex_condition );
  int (*HasConditionFilter)(          const vgx_VertexCondition_t *vertex_condition );
  int (*HasAssertCondition)(          const vgx_VertexCondition_t *vertex_condition );
  int (*HasRecursiveTraversal)(       const vgx_VertexCondition_t *vertex_condition );
  int (*HasArcTraversal)(             const vgx_VertexCondition_t *vertex_condition );
  int (*HasTraversalFilter)(          const vgx_VertexCondition_t *vertex_condition );
  int (*HasAssertTraversal)(          const vgx_VertexCondition_t *vertex_condition );
  int (*HasCollector)(                const vgx_VertexCondition_t *vertex_condition );


} vgx_IVertexCondition_t;



/*******************************************************************//**
 * IRankingCondition_t
 ***********************************************************************
 */
typedef struct s_vgx_IRankingCondition_t {

  // Lifecycle
  vgx_RankingCondition_t * (*New)( vgx_Graph_t *graph, const char *expression, vgx_sortspec_t sortspec, const vgx_predicator_modifier_enum modifier, vgx_Vector_t *sort_vector, vgx_ArcConditionSet_t **aggregate_condition_set, int64_t aggregate_deephits, CString_t **CSTR__error );
  vgx_RankingCondition_t * (*NewDefault)( void );
  vgx_RankingCondition_t * (*Clone)( const vgx_RankingCondition_t *other );
  void (*Delete)( vgx_RankingCondition_t **ranking_condition );

} vgx_IRankingCondition_t;



/*******************************************************************//**
 * IArcCondition_t
 ***********************************************************************
 */
typedef struct s_vgx_IArcCondition_t {

  // Lifecycle
  vgx_ArcCondition_t * (*Set)( vgx_Graph_t *graph, vgx_ArcCondition_t *dest, bool positive, const char *relationship, const vgx_predicator_modifier_enum mod_enum, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
  void (*Clear)( vgx_ArcCondition_t *arc_condition );
  vgx_ArcCondition_t * (*New)( vgx_Graph_t *graph, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
  vgx_ArcCondition_t * (*Clone)( const vgx_ArcCondition_t *other );
  vgx_ArcCondition_t * (*Copy)( vgx_ArcCondition_t *dest, const vgx_ArcCondition_t *other );
  void (*Delete)( vgx_ArcCondition_t **arc_condition );
  int (*IsWild)( vgx_ArcCondition_t *arc_condition );
  vgx_predicator_modifier_enum (*Modifier)( const vgx_ArcCondition_t *arc_condition );
} vgx_IArcCondition_t;



/*******************************************************************//**
 * IArcConditionSet_t
 ***********************************************************************
 */
typedef struct s_vgx_IArcConditionSet_t {

  // Lifecycle
  vgx_ArcConditionSet_t * (*NewEmpty)( vgx_Graph_t *graph, bool accept, const vgx_arc_direction direction );
  vgx_ArcConditionSet_t * (*NewSimple)( vgx_Graph_t *graph, const vgx_arc_direction direction, bool positive, const char *relationship, const vgx_predicator_modifier_enum modifier, const vgx_value_comparison vcomp, const void *value1, const void *value2 );
  int (*Add)( vgx_ArcConditionSet_t *arc_condition_set, vgx_ArcCondition_t **arc_condition );
  vgx_ArcConditionSet_t * (*Clone)( const vgx_ArcConditionSet_t *other_set );
  void (*Clear)( vgx_ArcConditionSet_t *arc_condition_set );
  void (*Delete)( vgx_ArcConditionSet_t **arc_condition_set );
  vgx_predicator_modifier_enum (*Modifier)( const vgx_ArcConditionSet_t *arc_condition_set );

} vgx_IArcConditionSet_t;



/*******************************************************************//**
 * IVertexProperty
 ***********************************************************************
 */
typedef struct s_vgx_IVertexProperty_t {

  int (*SetProperty_WL)( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
  vgx_VertexProperty_t * (*IncProperty_WL)( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
  int32_t (*SetVertexEnum_WL)( vgx_Vertex_t *self_LCK, int32_t e32 );
  int32_t (*VertexEnum_LCK)( vgx_Vertex_t *self_LCK );
  vgx_VertexProperty_t * (*GetProperty_RO)( const vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop );
  vgx_SelectProperties_t * (*GetProperties_RO_CS)( vgx_Vertex_t *self_RO );
  vgx_SelectProperties_t * (*GetProperties_RO)( vgx_Vertex_t *self_RO );
  vgx_ExpressEvalStack_t * (*EvalProperties_RO_CS)( const vgx_CollectorItem_t *item_RO_CS, vgx_Evaluator_t *selecteval, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id );

  bool (*HasProperty_RO)( const vgx_Vertex_t *self_RO, const vgx_VertexProperty_t *prop );
  int64_t (*NumProperties_RO)( vgx_Vertex_t *self_RO );
  int (*DelProperty_WL)( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
  int64_t (*DelProperties_WL)( vgx_Vertex_t *self_WL );
  int64_t (*Serialize_RO_CS)( vgx_Vertex_t *self_RO, CQwordQueue_t *__OUTPUT );
  int64_t (*Deserialize_WL_CS)( vgx_Vertex_t *self_WL, CQwordQueue_t *__INPUT );
  
  void (*FreeSelectProperties_CS)( vgx_Graph_t *self, vgx_SelectProperties_t **selected );
  void (*ClearSelectProperty_CS)( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop );

  void (*FreeSelectProperties)( vgx_Graph_t *self, vgx_SelectProperties_t **selected );
  void (*ClearSelectProperty)( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop );

  int (*AddCStringValue)( vgx_value_t *dest, CString_t **CSTR__string );
  vgx_value_t * (*CloneValue)( const vgx_value_t *other );
  int (*CloneValueInto)( vgx_value_t *dest, const vgx_value_t *src );
  void (*DeleteValue)( vgx_value_t **value );
  void (*ClearValue)( vgx_value_t *value );

  vgx_value_condition_t * (*NewValueCondition)( void );
  int (*CloneValueConditionInto)( vgx_value_condition_t *dest, const vgx_value_condition_t *src );
  vgx_value_condition_t * (*CloneValueCondition)( const vgx_value_condition_t *other );
  void (*ClearValueCondition)( vgx_value_condition_t *value_condition );
  void (*DeleteValueCondition)( vgx_value_condition_t **value_condition );

  vgx_VertexProperty_t * (*NewFromValueCondition)( vgx_Graph_t *graph, const char *key, vgx_value_condition_t **value_condition );
  vgx_VertexProperty_t * (*NewDefault)( vgx_Graph_t *graph, const char *key );
  vgx_VertexProperty_t * (*NewInteger)( vgx_Graph_t *graph, const char *key, int64_t value );
  vgx_VertexProperty_t * (*NewReal)( vgx_Graph_t *graph, const char *key, double value );
  vgx_VertexProperty_t * (*NewString)( vgx_Graph_t *graph, const char *key, const char *value );
  vgx_VertexProperty_t * (*NewIntArray)( vgx_Graph_t *graph, const char *key, int64_t sz, const QWORD data[] );


  int (*CloneInto)( vgx_VertexProperty_t *dest, const vgx_VertexProperty_t *src );
  vgx_VertexProperty_t * (*Clone)( const vgx_VertexProperty_t *other );
  void (*Clear)( vgx_VertexProperty_t *property_condition );
  void (*Delete)( vgx_VertexProperty_t **property_condition );

  vgx_PropertyConditionSet_t * (*NewSet)( bool positive );
  int (*CloneSetInto)( vgx_PropertyConditionSet_t *dest, const vgx_PropertyConditionSet_t *src );
  vgx_PropertyConditionSet_t * (*CloneSet)( const vgx_PropertyConditionSet_t *other );
  void (*ClearSet)( vgx_PropertyConditionSet_t *property_condition_set );
  void (*DeleteSet)( vgx_PropertyConditionSet_t **property_condition_set );


} vgx_IVertexProperty_t;




/******************************************************************************
 * vgx_ResponseFields_t
 *
 ******************************************************************************
 */
typedef struct s_vgx_ResponseFields_t {

  // Basic attribute mask
  vgx_ResponseAttrFastMask fastmask;

  // Properties
  vgx_Evaluator_t *selecteval;

  // 
  bool include_mod_tm;


} vgx_ResponseFields_t;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef union u_vgx_ResponseFieldValue_t {
  QWORD bits;
  int64_t i64;
  vgx_predicator_t pred;
  double real;
  void *ptr;
  const CString_t *CSTR__str;
  const vgx_VertexCompleteIdentifier_t *ident;
  const vgx_Vector_t *vector;
  vgx_ExpressEvalStack_t *eval_properties;
  const vgx_Vertex_t *vertex;
} vgx_ResponseFieldValue_t;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_vgx_ResponseFieldData_t {
  vgx_ResponseAttrFastMask attr;
  vgx_ResponseFieldValue_t value;
} vgx_ResponseFieldData_t;


/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef void * (*f_ResponseValueRender)( vgx_ResponseFieldValue_t value, void *dest_obj );



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_vgx_ResponseFieldMap_t {
  int srcpos;
  vgx_ResponseAttrFastMask attr;
  f_ResponseValueRender render;
  const char *fieldname;
} vgx_ResponseFieldMap_t;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_vgx_SearchResult_t {
  // Seconds to execute neighborhood probe
  double t_probe;

  // Seconds to construct response
  double t_response;

  // Total number of neighbor vertices matching probe
  int64_t total_neighbors;

  // Total number of arcs leading to neighbors matching probe
  int64_t total_arcs;

  // Total number of vertices matching probe
  int64_t total_vertices;

  // Total number of arcs included in response
  int64_t response_arcs;

  // The response offset
  int response_offset;

  //
  vgx_Graph_t *graph;

  //
  const vgx_BaseQuery_t *query;

  //
  const vgx_ResponseFieldMap_t *fieldmap;
  vgx_ResponseFields_t list_fields;
  int list_width;
  int64_t list_length;
  int64_t n_excluded;
  vgx_VertexCompleteIdentifier_t *tail_identifiers;
  vgx_VertexCompleteIdentifier_t *head_identifiers;
  vgx_ResponseFieldData_t *list;

  vgx_ExecutionTime_t exe_time;
  //

} vgx_SearchResult_t;



/*******************************************************************//**
 * IGraphResponse_t
 ***********************************************************************
 */
typedef struct s_vgx_IGraphResponse_t {
  int (*Initialize)( void );
  void (*Destroy)( void );
  vgx_ResponseFieldMap_t * (*NewFieldMap)( const vgx_ResponseFieldData_t *entry, int entry_width, const vgx_ResponseFieldMap_t fieldmap_definition[] );
  int64_t (*BuildSearchResult)( vgx_Graph_t *self, const vgx_ResponseFields_t *fields, const vgx_ResponseFieldMap_t *fieldmap, vgx_BaseQuery_t *query );
  int64_t (*BuildDefaultSearchResult)( vgx_Graph_t *self, vgx_BaseQuery_t *query );
  void (*DeleteSearchResult)( vgx_SearchResult_t **search_result );
  void (*DeleteProperties)( vgx_Graph_t *self, vgx_SelectProperties_t **selected_properties );
  void (*FormatResultsToStream)( vgx_Graph_t *self, vgx_BaseQuery_t *query, FILE *output );
  vgx_VertexProperty_t * (*SelectProperty)( vgx_Graph_t *graph, const char *name, vgx_VertexProperty_t *prop );
  vgx_Evaluator_t * (*ParseSelectProperties)( vgx_Graph_t *graph, const char *select_statement, vgx_Vector_t *vector, CString_t **CSTR__error );
} vgx_IGraphResponse_t;


typedef struct s_vgx_QtoS_cache_entry_t {
  QWORD encoding;
  const CString_t *CSTR__string;
} vgx_QtoS_cache_entry_t;


typedef struct s_vgx_QtoS_2way_cache_cell_t {
  vgx_QtoS_cache_entry_t entryA;
  vgx_QtoS_cache_entry_t entryB;
  uintptr_t mru;
  uintptr_t lru;
} vgx_QtoS_2way_cache_cell_t;


typedef struct s_vgx_QtoS_2way_set_associative_cache_t {
  vgx_QtoS_2way_cache_cell_t *cells;
  struct s_vgx_QtoS_2way_set_associative_cache_t *next;
  QWORD mask;
  int64_t order;
  int64_t width;
  int64_t n_hit;
  int64_t n_miss;
  int64_t n_decode;
  const CString_t *CSTR__name;
} vgx_QtoS_2way_set_associative_cache_t;


typedef vgx_QtoS_2way_set_associative_cache_t vgx_CStringEncodingCache_t;


typedef const CString_t * (*f_vgx_CStringDecoder_t)( vgx_Graph_t *graph, QWORD encoding );

/*******************************************************************//**
 * ICache_t
 ***********************************************************************
 */
typedef struct s_vgx_ICache_t {

  vgx_CStringEncodingCache_t *  (*NewStringEncodingCache)( const char *name, int depth, int order );
  void                          (*DeleteStringEncodingCache)( vgx_CStringEncodingCache_t **cache );
  const CString_t *             (*EncodingAsString)( vgx_Graph_t *self, const QWORD encoded, vgx_CStringEncodingCache_t *cache, f_vgx_CStringDecoder_t decoder );
  void                          (*PrintStringEncodingCache)( vgx_CStringEncodingCache_t *cache );

} vgx_ICache_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef union u_VertexAndInt64_t {
  __m128i m128;
  struct {
    vgx_Vertex_t *vertex;
    int64_t value;
  };
} VertexAndInt64_t;


typedef Cm128iList_t VertexAndInt64List_t;

typedef Cm128iQueue_t VertexAndInt64Queue_t;


static uint64_t __vertex_get_index( const vgx_AllocatedVertex_t *V );
static uint64_t __vertex_get_bitindex( const vgx_AllocatedVertex_t *V );
static uint64_t __vertex_get_bitvector( const vgx_AllocatedVertex_t *V );


static int __vertex_get_semaphore_count( const vgx_Vertex_t *V );
static void __vertex_clear_semaphore_count( vgx_Vertex_t *V );
static int __vertex_inc_semaphore_count( vgx_Vertex_t *V );
static int __vertex_inc_semaphore_count_delta( vgx_Vertex_t *V, int8_t delta );
static int __vertex_dec_semaphore_count_delta( vgx_Vertex_t *V, int8_t delta );
static int __vertex_dec_semaphore_count( vgx_Vertex_t *V );
static bool __vertex_is_semaphore_writer_reentrant( const vgx_Vertex_t *V );
static bool __vertex_has_semaphore_reader_capacity( const vgx_Vertex_t *V );
static DWORD __vertex_get_writer_threadid( const vgx_Vertex_t *V );
static DWORD __vertex_set_writer_current_thread( vgx_Vertex_t *V );
static bool __vertex_is_writer_current_thread( const vgx_Vertex_t *V );
static bool __vertex_is_writer_thread( const vgx_Vertex_t *V, uint32_t threadid );
static void __vertex_clear_writer_thread( vgx_Vertex_t *V );
static bool __vertex_is_unlocked( const vgx_Vertex_t *V );
static bool __vertex_is_locked( const vgx_Vertex_t *V );
static void __vertex_set_unlocked( vgx_Vertex_t *V );
static void __vertex_set_locked( vgx_Vertex_t *V );
static bool __vertex_is_writable( const vgx_Vertex_t *V );
static bool __vertex_is_readonly( const vgx_Vertex_t *V );
static void __vertex_set_writable( vgx_Vertex_t *V );
static void __vertex_clear_writable( vgx_Vertex_t *V );
static void __vertex_set_readonly( vgx_Vertex_t *V );
static void __vertex_set_locked_readonly( vgx_Vertex_t *V );
static void __vertex_clear_readonly( vgx_Vertex_t *V );
static bool __vertex_is_locked_writable( const vgx_Vertex_t *V );
static bool __vertex_is_writer_reentrant( const vgx_Vertex_t *V );
static bool __vertex_is_locked_writable_by_current_thread( const vgx_Vertex_t *V );
static bool __vertex_is_locked_writable_by_thread( const vgx_Vertex_t *V, uint32_t threadid );
static bool __vertex_is_locked_writable_by_other_thread( const vgx_Vertex_t *V, uint32_t this_threadid );
static bool __vertex_is_not_locked_writable_by_other_thread( const vgx_Vertex_t *V, uint32_t this_threadid );
static int __vertex_get_writer_recursion( const vgx_Vertex_t *V );
static int __vertex_inc_writer_recursion_CS( vgx_Vertex_t *V );
static int __vertex_inc_writer_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta );
static int __vertex_dec_writer_recursion_CS( vgx_Vertex_t *V );
static int __vertex_dec_writer_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta );
static bool __vertex_is_locked_readonly( const vgx_Vertex_t *V );
static bool __vertex_has_reader_capacity( const vgx_Vertex_t *V );
static int __vertex_get_readers( const vgx_Vertex_t *V );
static int __vertex_inc_readers_CS( vgx_Vertex_t *V );
static int __vertex_inc_readonly_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta );
static int __vertex_dec_readers_CS( vgx_Vertex_t *V );
static int __vertex_dec_readonly_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta );
static bool __vertex_is_locked_safe_for_thread_CS( const vgx_Vertex_t *V, uint32_t this_threadid );
static bool __vertex_is_write_requested( const vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_set_write_requested( vgx_Vertex_t *V );
static void __vertex_set_writer_thread_or_redeem_write_request( vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_clear_write_requested( vgx_Vertex_t *V );
static bool __vertex_is_inarcs_yielded( const vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_set_yield_inarcs( vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_clear_yield_inarcs( vgx_Vertex_t *V );
static bool __vertex_is_borrowed_inarcs_busy( const vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_set_borrowed_inarcs_busy( vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_clear_borrowed_inarcs_busy( vgx_Vertex_t *V );
static bool __vertex_is_inarcs_available( const vgx_Vertex_t *V );
static bool __vertex_is_lockable_as_writable( const vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_lock_writable_CS( vgx_Vertex_t *V );
static int __vertex_unlock_writable_CS( vgx_Vertex_t *V );
static bool __vertex_is_lockable_as_readonly( const vgx_Vertex_t *V );
static vgx_Vertex_t * __vertex_lock_readonly_CS( vgx_Vertex_t *V );
static int __vertex_unlock_readonly_CS( vgx_Vertex_t *V );
static bool __vertex_is_readonly_lockable_as_writable( const vgx_Vertex_t *V );
static bool __vertex_is_active_context( const vgx_Vertex_t *V );
static bool __vertex_is_suspended_context( const vgx_Vertex_t *V );
static void __vertex_set_active_context( vgx_Vertex_t *V );
static void __vertex_set_suspended_context( vgx_Vertex_t *V );
static bool __vertex_is_manifestation_null( const vgx_Vertex_t *V );
static bool __vertex_is_manifestation_real( const vgx_Vertex_t *V );
static bool __vertex_is_manifestation_virtual( const vgx_Vertex_t *V );
static void __vertex_set_manifestation_null( vgx_Vertex_t *V );
static void __vertex_set_manifestation_real( vgx_Vertex_t *V );
static void __vertex_set_manifestation_virtual( vgx_Vertex_t *V );
static vgx_VertexStateContext_man_t __vertex_get_manifestation( const vgx_Vertex_t *V );
static void __vertex_set_manifestation( vgx_Vertex_t *V, vgx_VertexStateContext_man_t man );
static bool __vertex_is_indexed_main( const vgx_Vertex_t *V );
static void __vertex_set_indexed_main( vgx_Vertex_t *V );
static void __vertex_clear_indexed_main( vgx_Vertex_t *V );
static bool __vertex_is_indexed_type( const vgx_Vertex_t *V );
static void __vertex_set_indexed_type( vgx_Vertex_t *V );
static void __vertex_clear_indexed_type( vgx_Vertex_t *V );
static bool __vertex_is_indexed( const vgx_Vertex_t *V );
static bool __vertex_is_unindexed( const vgx_Vertex_t *V );
static void __vertex_clear_indexed( vgx_Vertex_t *V );
static bool __vertex_has_event_scheduled( const vgx_Vertex_t *V );
static void __vertex_set_event_scheduled( vgx_Vertex_t *V );
static void __vertex_clear_event_scheduled( vgx_Vertex_t *V );
static bool __vertex_scope_is_local( const vgx_Vertex_t *V );
static void __vertex_set_scope_local( vgx_Vertex_t *V );
static bool __vertex_scope_is_global( const vgx_Vertex_t *V );
static void __vertex_set_scope_global( vgx_Vertex_t *V );
static bool __vertex_has_vector( const vgx_Vertex_t *V );
static void __vertex_set_has_vector( vgx_Vertex_t *V );
static void __vertex_clear_has_vector( vgx_Vertex_t *V );
static bool __vertex_has_centroid( const vgx_Vertex_t *V );
static void __vertex_set_has_centroid( vgx_Vertex_t *V );
static void __vertex_clear_has_centroid( vgx_Vertex_t *V );
static bool __vertex_has_inarcs( const vgx_Vertex_t *V );
static void __vertex_set_has_inarcs( vgx_Vertex_t *V );
static void __vertex_clear_has_inarcs( vgx_Vertex_t *V );
static bool __vertex_has_outarcs( const vgx_Vertex_t *V );
static void __vertex_set_has_outarcs( vgx_Vertex_t *V );
static void __vertex_clear_has_outarcs( vgx_Vertex_t *V );
static void __vertex_set_isolated( vgx_Vertex_t *V );
static bool __vertex_is_isolated( const vgx_Vertex_t *V );
static bool __vertex_is_source( const vgx_Vertex_t *V );
static bool __vertex_is_sink( const vgx_Vertex_t *V );
static bool __vertex_is_internal( const vgx_Vertex_t *V );
static vgx_vertex_type_t __vertex_get_type( const vgx_Vertex_t *V );
static vgx_vertex_type_t __vertex_set_type( vgx_Vertex_t *V, vgx_vertex_type_t vt );
static void __vertex_clear_type( vgx_Vertex_t *V );
static void __vertex_set_defunct( vgx_Vertex_t *V );
static bool __vertex_is_defunct( const vgx_Vertex_t *V );
static void __vertex_set_expired( const vgx_Vertex_t *V );

static uint32_t __vertex_set_expiration_ts( vgx_Vertex_t *V, uint32_t tmx_s );
static int64_t __vertex_set_expiration_tms( vgx_Vertex_t *V, int64_t tmx_ms );
static uint32_t __vertex_get_expiration_ts( const vgx_Vertex_t *V );
static int64_t __vertex_get_expiration_tms( const vgx_Vertex_t *V );
static void __vertex_clear_expiration( vgx_Vertex_t *V );
static bool __vertex_has_expiration( const vgx_Vertex_t *V );
static bool __vertex_is_expired( const vgx_Vertex_t *V, uint32_t now_ts );

static uint32_t __vertex_arcvector_set_expiration_ts( vgx_Vertex_t *V, uint32_t tmx_s );
static int64_t __vertex_arcvector_set_expiration_tms( vgx_Vertex_t *V, int64_t tmx_ms );
static uint32_t __vertex_arcvector_get_expiration_ts( const vgx_Vertex_t *V );
static int64_t __vertex_arcvector_get_expiration_tms( const vgx_Vertex_t *V );
static void __vertex_arcvector_clear_expiration( vgx_Vertex_t *V );
static bool __vertex_arcvector_has_expiration( const vgx_Vertex_t *V );
static bool __vertex_arcvector_is_expired( const vgx_Vertex_t *V, uint32_t now_ts );
static bool __vertex_has_any_expiration( const vgx_Vertex_t *V );
static void __vertex_all_clear_expiration( vgx_Vertex_t *V );
static uint32_t __vertex_get_min_expiration_ts( const vgx_Vertex_t *V );
static int64_t __vertex_get_min_expiration_tms( const vgx_Vertex_t *V );



/* indexing */
__inline static uint64_t __vertex_get_index( const vgx_AllocatedVertex_t *V ) {
  return (uint64_t)V / 192;
}


/**************************************************************************//**
 * __vertex_get_bitindex
 *
 ******************************************************************************
 */
__inline static uint64_t __vertex_get_bitindex( const vgx_AllocatedVertex_t *V ) {
  return (uint64_t)V / 12288; // 192*64
}


/**************************************************************************//**
 * __vertex_get_bitvector
 *
 ******************************************************************************
 */
__inline static uint64_t __vertex_get_bitvector( const vgx_AllocatedVertex_t *V ) {
  return 1ULL << (((uint64_t)V / 192) & 0x3F);
}


/* semaphore.count */
__inline static int __vertex_get_semaphore_count( const vgx_Vertex_t *V  ) {
  return (int)V->descriptor.semaphore.count;
}


/**************************************************************************//**
 * __vertex_clear_semaphore_count
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_semaphore_count( vgx_Vertex_t *V ) {
  V->descriptor.semaphore.count = 0;
}


/**************************************************************************//**
 * __vertex_inc_semaphore_count
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_semaphore_count( vgx_Vertex_t *V ) {
  return (int)(++(V->descriptor.semaphore.count));
}


/**************************************************************************//**
 * __vertex_inc_semaphore_count_delta
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_semaphore_count_delta( vgx_Vertex_t *V, int8_t delta ) {
  V->descriptor.semaphore.count += delta;
  return (int)(V->descriptor.semaphore.count);
}


/**************************************************************************//**
 * __vertex_dec_semaphore_count_delta
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_semaphore_count_delta( vgx_Vertex_t *V, int8_t delta ) {
  V->descriptor.semaphore.count -= delta;
  return (int)(V->descriptor.semaphore.count);
}




/**************************************************************************//**
 * __vertex_dec_semaphore_count
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_semaphore_count( vgx_Vertex_t *V ) {
  if( V->descriptor.semaphore.count > 0 ) {
    return (int)(--(V->descriptor.semaphore.count));
  }
  else {
    return 0;
  }
}


/**************************************************************************//**
 * __vertex_is_semaphore_writer_reentrant
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_semaphore_writer_reentrant( const vgx_Vertex_t *V ){
  return V->descriptor.semaphore.count < VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT;
}


/**************************************************************************//**
 * __vertex_has_semaphore_reader_capacity
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_semaphore_reader_capacity( const vgx_Vertex_t *V ) {
  return V->descriptor.semaphore.count < VERTEX_SEMAPHORE_COUNT_READERS_LIMIT;
}


/* writer.threadid */
__inline static DWORD __vertex_get_writer_threadid( const vgx_Vertex_t *V ) {
  return V->descriptor.writer.threadid;
}


/**************************************************************************//**
 * __vertex_set_writer_current_thread
 *
 ******************************************************************************
 */
__inline static DWORD __vertex_set_writer_current_thread( vgx_Vertex_t *V ) {
  return (V->descriptor.writer.threadid = GET_CURRENT_THREAD_ID());
}


/**************************************************************************//**
 * __vertex_is_writer_current_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_writer_current_thread( const vgx_Vertex_t *V ) {
  return V->descriptor.writer.threadid == GET_CURRENT_THREAD_ID(); 
}


/**************************************************************************//**
 * __vertex_is_writer_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_writer_thread( const vgx_Vertex_t *V, uint32_t threadid ) {
  return V->descriptor.writer.threadid == threadid;
}


/**************************************************************************//**
 * __vertex_clear_writer_thread
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_writer_thread( vgx_Vertex_t *V ) {
  V->descriptor.writer.threadid = VERTEX_WRITER_THREADID_NONE;
}



/* state.lock */

// state.lock.lck
__inline static bool __vertex_is_unlocked( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.lck == VERTEX_STATE_LOCK_LCK_OPEN;
}


/**************************************************************************//**
 * __vertex_is_locked
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.lck == VERTEX_STATE_LOCK_LCK_LOCKED;
}


/**************************************************************************//**
 * __vertex_set_unlocked
 *
 ******************************************************************************
 */
__inline static void __vertex_set_unlocked( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_OPEN;
}


/**************************************************************************//**
 * __vertex_set_locked
 *
 ******************************************************************************
 */
__inline static void __vertex_set_locked( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED; 
}

// state.lock.rwl

/**************************************************************************//**
 * __vertex_is_writable
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_writable( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.rwl == VERTEX_STATE_LOCK_RWL_WRITABLE;
}


/**************************************************************************//**
 * __vertex_is_readonly
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_readonly( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.rwl == VERTEX_STATE_LOCK_RWL_READONLY;
}


/**************************************************************************//**
 * __vertex_set_writable
 *
 ******************************************************************************
 */
__inline static void __vertex_set_writable( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_WRITABLE;
}


/**************************************************************************//**
 * __vertex_clear_writable
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_writable( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_NONE;
} // same thing as writable, but for clarity


/**************************************************************************//**
 * __vertex_set_readonly
 *
 ******************************************************************************
 */
__inline static void __vertex_set_readonly( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
}


/**************************************************************************//**
 * __vertex_set_locked_readonly
 *
 ******************************************************************************
 */
__inline static void __vertex_set_locked_readonly( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.bits = VERTEX_STATE_LOCK_READONLY;
}


/**************************************************************************//**
 * __vertex_clear_readonly
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_readonly( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_NONE; 
} // same thing as writable, but for clarity

// writable:

/**************************************************************************//**
 * __vertex_is_locked_writable
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_writable( const vgx_Vertex_t *V ) {
  return __vertex_is_locked(V) && __vertex_is_writable(V);
}


/**************************************************************************//**
 * __vertex_is_writer_reentrant
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_writer_reentrant( const vgx_Vertex_t *V ) {
  return __vertex_is_writer_current_thread(V) && __vertex_is_semaphore_writer_reentrant(V);
}


/**************************************************************************//**
 * __vertex_is_locked_writable_by_current_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_writable_by_current_thread( const vgx_Vertex_t *V ) {
  return __vertex_is_locked_writable(V) && __vertex_is_writer_current_thread(V);
}


/**************************************************************************//**
 * __vertex_is_locked_writable_by_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_writable_by_thread( const vgx_Vertex_t *V, uint32_t threadid ) {
  return __vertex_is_locked_writable(V) && V->descriptor.writer.threadid == threadid; 
}


/**************************************************************************//**
 * __vertex_is_locked_writable_by_other_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_writable_by_other_thread( const vgx_Vertex_t *V, uint32_t this_threadid ) {
  return __vertex_is_locked_writable( V ) && !__vertex_is_writer_thread( V, this_threadid );
}


/**************************************************************************//**
 * __vertex_is_not_locked_writable_by_other_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_not_locked_writable_by_other_thread( const vgx_Vertex_t *V, uint32_t this_threadid ) {
  return __vertex_is_unlocked( V ) || __vertex_is_readonly( V ) || __vertex_is_writer_thread( V, this_threadid );
}


/**************************************************************************//**
 * __vertex_get_writer_recursion
 *
 ******************************************************************************
 */
__inline static int __vertex_get_writer_recursion( const vgx_Vertex_t *V ) {
  return __vertex_get_semaphore_count(V); 
}


/**************************************************************************//**
 * __vertex_inc_writer_recursion_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_writer_recursion_CS( vgx_Vertex_t *V ) {
  _vgx_graph_inc_vertex_WL_count_CS( V->graph );
  return __vertex_inc_semaphore_count(V);
}


/**************************************************************************//**
 * __vertex_inc_writer_recursion_delta_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_writer_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta ) {
  _vgx_graph_inc_vertex_WL_count_delta_CS( V->graph, delta );
  return __vertex_inc_semaphore_count_delta( V, delta );
}


/**************************************************************************//**
 * __vertex_dec_writer_recursion_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_writer_recursion_CS( vgx_Vertex_t *V ) {
  _vgx_graph_dec_vertex_WL_count_CS( V->graph );
  return __vertex_dec_semaphore_count(V);
}


/**************************************************************************//**
 * __vertex_dec_writer_recursion_delta_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_writer_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta ) {
  _vgx_graph_dec_vertex_WL_count_delta_CS( V->graph, delta );
  return __vertex_dec_semaphore_count_delta( V, delta );
}


// readonly:

/**************************************************************************//**
 * __vertex_is_locked_readonly
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_readonly( const vgx_Vertex_t *V ) {
  return __vertex_is_locked(V) && __vertex_is_readonly(V);
}


/**************************************************************************//**
 * __vertex_has_reader_capacity
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_reader_capacity( const vgx_Vertex_t *V ) {
  return __vertex_has_semaphore_reader_capacity(V);
}


/**************************************************************************//**
 * __vertex_get_readers
 *
 ******************************************************************************
 */
__inline static int __vertex_get_readers( const vgx_Vertex_t *V ) {
  return __vertex_get_semaphore_count(V);
}


/**************************************************************************//**
 * __vertex_inc_readers_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_readers_CS( vgx_Vertex_t *V ) {
  _vgx_graph_inc_vertex_RO_count_CS( V->graph );
  return __vertex_inc_semaphore_count(V);
}


/**************************************************************************//**
 * __vertex_inc_readonly_recursion_delta_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_inc_readonly_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta ) {
  _vgx_graph_inc_vertex_RO_count_delta_CS( V->graph, delta );
  return __vertex_inc_semaphore_count_delta( V, delta );
}


/**************************************************************************//**
 * __vertex_dec_readers_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_readers_CS( vgx_Vertex_t *V ) {
  _vgx_graph_dec_vertex_RO_count_CS( V->graph );
  return __vertex_dec_semaphore_count(V);
}


/**************************************************************************//**
 * __vertex_dec_readonly_recursion_delta_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_dec_readonly_recursion_delta_CS( vgx_Vertex_t *V, int8_t delta ) {
  _vgx_graph_dec_vertex_RO_count_delta_CS( V->graph, delta );
  return __vertex_dec_semaphore_count_delta( V, delta );
}


// safe in CS?

/**************************************************************************//**
 * __vertex_is_locked_safe_for_thread_CS
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_locked_safe_for_thread_CS( const vgx_Vertex_t *V, uint32_t this_threadid ) {
  return __vertex_is_locked( V ) && ( __vertex_is_readonly( V ) || __vertex_is_writer_thread( V, this_threadid ) );
}


// state.lock.wrq

/**************************************************************************//**
 * __vertex_is_write_requested
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_write_requested( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.wrq == VERTEX_STATE_LOCK_WRQ_PENDING;
}



/**************************************************************************//**
 * __vertex_is_write_requested_by_other_thread
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_write_requested_by_other_thread( const vgx_Vertex_t *V ) {
  return __vertex_is_write_requested( V ) && !__vertex_is_writer_current_thread( V );
}



/**************************************************************************//**
 * __vertex_set_write_requested
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_set_write_requested( vgx_Vertex_t *V ) {
  if( __vertex_is_readonly(V) && !__vertex_is_write_requested(V) ) {
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_PENDING;
    __vertex_set_writer_current_thread( V );
    return V;
  }
  else {
    return NULL; // not allowed to touch wrq unless currently readonly and no other thread has set wrq
  }
}


/**************************************************************************//**
 * __vertex_set_writer_thread_or_redeem_write_request
 *
 ******************************************************************************
 */
__inline static void __vertex_set_writer_thread_or_redeem_write_request( vgx_Vertex_t *V ) {
  if( __vertex_is_write_requested(V) && __vertex_is_writer_current_thread(V) ) {
    // THIS IS THE PROBLEM:
    // Someone holds the readonly only, then two writers want to write.
    // One of the writers will be able to set the WRQ flag and register its threadid as current (waiting) writer.
    // When the readonly thread closes the vertex it will become writable.
    // If the writer thread that "wins" the write lock is NOT the one who set the WRQ flag then the threadid
    // will be different, preventing the WRQ flag from being cleared.
    //
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_NONE;
  }
  else {
    __vertex_set_writer_current_thread( V );
  }
}


/**************************************************************************//**
 * __vertex_clear_write_requested
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_clear_write_requested( vgx_Vertex_t *V ) {
  if( __vertex_is_write_requested(V) && __vertex_is_writer_current_thread(V) ) {
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_NONE;
    __vertex_clear_writer_thread( V );
    return V;
  }
  else {
    return NULL;  // not allowed to clear wrq unless its already set and current thread is the original requestor
  }
}

// state.lock.iny

/**************************************************************************//**
 * __vertex_is_inarcs_yielded
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_inarcs_yielded( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.iny == VERTEX_STATE_LOCK_INY_INARCS_YIELDED;
}


/**************************************************************************//**
 * __vertex_set_yield_inarcs
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_set_yield_inarcs( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.iny = VERTEX_STATE_LOCK_INY_INARCS_YIELDED; return V;
}


/**************************************************************************//**
 * __vertex_clear_yield_inarcs
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_clear_yield_inarcs( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.iny = VERTEX_STATE_LOCK_INY_INARCS_NORMAL; return V;
}

// state.lock.yib

/**************************************************************************//**
 * __vertex_is_borrowed_inarcs_busy
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_borrowed_inarcs_busy( const vgx_Vertex_t *V ) {
  return V->descriptor.state.lock.yib == VERTEX_STATE_LOCK_YIB_INARCS_BUSY;
}


/**************************************************************************//**
 * __vertex_set_borrowed_inarcs_busy
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_set_borrowed_inarcs_busy( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.yib = VERTEX_STATE_LOCK_YIB_INARCS_BUSY; return V;
}


/**************************************************************************//**
 * __vertex_clear_borrowed_inarcs_busy
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_clear_borrowed_inarcs_busy( vgx_Vertex_t *V ) {
  V->descriptor.state.lock.yib = VERTEX_STATE_LOCK_YIB_INARCS_IDLE; return V;
}


/**************************************************************************//**
 * __vertex_is_inarcs_available
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_inarcs_available( const vgx_Vertex_t *V ) {
  // Inarcs are available if the vertex is not locked or it is locked and
  // it has yielded its inarcs and the inarcs are not borrowed by someone else.
  // Let L="vertex is locked", Y="vertex has yielded inarcs", B="inarcs are busy"
  // Available = /L + L * Y * /B
  return __vertex_is_unlocked(V)
          ||
          
          (
              __vertex_is_inarcs_yielded(V)
              &&
              !__vertex_is_borrowed_inarcs_busy(V)
          );
}

// WRITABLE transitions

/**************************************************************************//**
 * __vertex_is_lockable_as_writable
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_lockable_as_writable( const vgx_Vertex_t *V ) {
 // Vertex is lockable for writable access if:  
  return  /* not locked and no other thread has requested write */
          ( 
                /* not locked */
                __vertex_is_unlocked( V )
                &&
                /* no write request pending by another thread (current thread may or may not have pending request) */
                !__vertex_is_write_requested_by_other_thread( V )
          )

          || 

          /* OR already write-locked by this thread */
          (
                /* already write-locked */
                __vertex_is_locked_writable(V)
                &&
                /* by this thread */
                __vertex_is_writer_reentrant(V)
          )
          ;
}


/**************************************************************************//**
 * __vertex_lock_writable_CS
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_lock_writable_CS( vgx_Vertex_t *V ) {
  // TODO: WE CANNOT lock writable if WRQ is set and we are not the same thread as the WRQ requestor!
  // ( WRQ will never be cleared in this case. )
  //
  if( __vertex_inc_writer_recursion_CS( V ) == 1 ) {
    __vertex_set_locked( V );
    __vertex_set_writable( V );
    __vertex_set_writer_thread_or_redeem_write_request( V );
  }
  return V;
}


/**************************************************************************//**
 * __vertex_unlock_writable_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_unlock_writable_CS( vgx_Vertex_t *V ) {
  int count = __vertex_dec_writer_recursion_CS( V );
  if( count == 0 ) {
    __vertex_clear_writer_thread( V );
    __vertex_clear_writable( V );
    __vertex_set_unlocked( V );
  }
  return count;
}

// READONLY transitions

/**************************************************************************//**
 * __vertex_is_lockable_as_readonly
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_lockable_as_readonly( const vgx_Vertex_t *V ) { 
  // Vertex is lockable for readonly access if:
  return __vertex_is_unlocked(V)                // it's not locked
         || (                                   //      OR
              __vertex_is_locked_readonly(V)    //    LOCKED as READONLY 
              &&                                //        AND 
              __vertex_has_reader_capacity(V)   //    maximum readcount not yet reached
              /*
              // NOTE: We have to remove the WRQ check because it leads to deadlock in
              //       a scenario where thread-1 becomes reader, then thread-2 wants to write
              //       and sets the WRQ, then thread-1 tries to re-enter the readonly lock.
              //       Since we have no way of keeping track of reader's threadid with
              //       individual re-entrancy count for each we are not able to safely use
              //       the WRQ. The original intent was to disallow NEW readers when WRQ
              //       is set but we don't know the difference between a new reader and a
              //       re-entrant reader due to lack of book-keeping for individual readers.
              &&                                //        AND
              !__vertex_is_write_requested(V)   //    no writer waiting to write
              */
         );
}


/**************************************************************************//**
 * __vertex_lock_readonly_CS
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __vertex_lock_readonly_CS( vgx_Vertex_t *V ) {
  __vertex_set_locked_readonly( V );
  __vertex_inc_readers_CS( V );
  return V;
}

/**************************************************************************//**
 * __vertex_unlock_readonly_CS
 *
 ******************************************************************************
 */
__inline static int __vertex_unlock_readonly_CS( vgx_Vertex_t *V ) {
  int count = __vertex_dec_readers_CS( V );
  if( count == 0 ) {
    __vertex_clear_readonly( V );
    __vertex_set_unlocked( V );
  }
  return count;
}

// ESCALATION transitions
//
// NOTE: This logic is flawed at the moment because 1) we are making the assumption that
//       the caller already knows it is a valid reader, 2) we rely on the semcount being
//       equal to one which is not true if the reader has acquired the readonly lock more than
//       once, and 3) we are relying on the WRQ which was found to be unusable in general
//       (see note above for __vertex_is_lockable_as_readonly)
//
// DO NOT USE THIS FUNCTION!
//
//

/**************************************************************************//**
 * __vertex_is_readonly_lockable_as_writable
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_readonly_lockable_as_writable( const vgx_Vertex_t *V ) {
  // Vertex is locked readonly by single reader (presumably us) and no write requested
  return __vertex_is_locked_readonly(V)           // it's readonly
         &&                                       //    AND
         __vertex_get_readers(V) == 1             // there's a single reader (us)
         &&                                       //    AND
         ( !__vertex_is_write_requested(V)        //   no write has been requested
            ||                                    //      OR
            __vertex_is_writer_current_thread(V)  //   write currently requested by us
         );
}

/* state.context */

// state.context.sus
__inline static bool __vertex_is_active_context( const vgx_Vertex_t *V ) {
  return V->descriptor.state.context.sus == VERTEX_STATE_CONTEXT_SUS_ACTIVE; 
}


/**************************************************************************//**
 * __vertex_is_suspended_context
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_suspended_context( const vgx_Vertex_t *V ) {
  return V->descriptor.state.context.sus == VERTEX_STATE_CONTEXT_SUS_SUSPENDED; 
}


/**************************************************************************//**
 * __vertex_set_active_context
 *
 ******************************************************************************
 */
__inline static void __vertex_set_active_context( vgx_Vertex_t *V ) {
  V->descriptor.state.context.sus = VERTEX_STATE_CONTEXT_SUS_ACTIVE;
}


/**************************************************************************//**
 * __vertex_set_suspended_context
 *
 ******************************************************************************
 */
__inline static void __vertex_set_suspended_context( vgx_Vertex_t *V ) {
  V->descriptor.state.context.sus = VERTEX_STATE_CONTEXT_SUS_SUSPENDED;
}

// state.context.man

/**************************************************************************//**
 * __vertex_is_manifestation_null
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_manifestation_null( const vgx_Vertex_t *V ) {
  return V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_NULL;
}


/**************************************************************************//**
 * __vertex_is_manifestation_real
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_manifestation_real( const vgx_Vertex_t *V ) {
  return V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_REAL;
}


/**************************************************************************//**
 * __vertex_is_manifestation_virtual
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_manifestation_virtual( const vgx_Vertex_t *V ) {
  return V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_VIRTUAL;
}


/**************************************************************************//**
 * __vertex_set_manifestation_null
 *
 ******************************************************************************
 */
__inline static void __vertex_set_manifestation_null( vgx_Vertex_t *V ) {
  V->descriptor.state.context.man = VERTEX_STATE_CONTEXT_MAN_NULL;
}


/**************************************************************************//**
 * __vertex_set_manifestation_real
 *
 ******************************************************************************
 */
__inline static void __vertex_set_manifestation_real( vgx_Vertex_t *V ) {
  V->descriptor.state.context.man = VERTEX_STATE_CONTEXT_MAN_REAL;
}


/**************************************************************************//**
 * __vertex_set_manifestation_virtual
 *
 ******************************************************************************
 */
__inline static void __vertex_set_manifestation_virtual( vgx_Vertex_t *V ) {
  V->descriptor.state.context.man = VERTEX_STATE_CONTEXT_MAN_VIRTUAL;
}


/**************************************************************************//**
 * __vertex_get_manifestation
 *
 ******************************************************************************
 */
__inline static vgx_VertexStateContext_man_t __vertex_get_manifestation( const vgx_Vertex_t *V ) {
  return (vgx_VertexStateContext_man_t)(V->descriptor.state.context.man);
}


/**************************************************************************//**
 * __vertex_set_manifestation
 *
 ******************************************************************************
 */
__inline static void __vertex_set_manifestation( vgx_Vertex_t *V, vgx_VertexStateContext_man_t man ) {
  V->descriptor.state.context.man = man;
}

/* property.index */

// property.index.main
__inline static bool __vertex_is_indexed_main( const vgx_Vertex_t *V ) {
  return V->descriptor.property.index.main == VERTEX_PROPERTY_INDEX_MAIN_INDEXED;
}


/**************************************************************************//**
 * __vertex_set_indexed_main
 *
 ******************************************************************************
 */
__inline static void __vertex_set_indexed_main( vgx_Vertex_t *V ) {
  V->descriptor.property.index.main = VERTEX_PROPERTY_INDEX_MAIN_INDEXED;
}


/**************************************************************************//**
 * __vertex_clear_indexed_main
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_indexed_main( vgx_Vertex_t *V ) {
  V->descriptor.property.index.main = VERTEX_PROPERTY_INDEX_MAIN_UNINDEXED;
}

// property.index.type

/**************************************************************************//**
 * __vertex_is_indexed_type
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_indexed_type( const vgx_Vertex_t *V ) {
  return V->descriptor.property.index.type == VERTEX_PROPERTY_INDEX_TYPE_INDEXED;
}


/**************************************************************************//**
 * __vertex_set_indexed_type
 *
 ******************************************************************************
 */
__inline static void __vertex_set_indexed_type( vgx_Vertex_t *V ) {
  V->descriptor.property.index.type = VERTEX_PROPERTY_INDEX_TYPE_INDEXED;
}


/**************************************************************************//**
 * __vertex_clear_indexed_type
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_indexed_type( vgx_Vertex_t *V ) {
  V->descriptor.property.index.type = VERTEX_PROPERTY_INDEX_TYPE_UNINDEXED;
}

// property.index

/**************************************************************************//**
 * __vertex_is_indexed
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_indexed( const vgx_Vertex_t *V ) {
  return V->descriptor.property.index.bits == VERTEX_PROPERTY_INDEX_INDEXED;
}


/**************************************************************************//**
 * __vertex_is_unindexed
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_unindexed( const vgx_Vertex_t *V ) {
  return V->descriptor.property.index.bits == VERTEX_PROPERTY_INDEX_UNINDEXED;
}


/**************************************************************************//**
 * __vertex_clear_indexed
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_indexed( vgx_Vertex_t *V ) {
  V->descriptor.property.index.bits = VERTEX_PROPERTY_INDEX_TYPE_UNINDEXED;
}


/* property.event */

// property.event.sch
__inline static bool __vertex_has_event_scheduled( const vgx_Vertex_t *V ) {
  return V->descriptor.property.event.sch == VERTEX_PROPERTY_EVENT_SCH_SCHEDULED;
}


/**************************************************************************//**
 * __vertex_set_event_scheduled
 *
 ******************************************************************************
 */
__inline static void __vertex_set_event_scheduled( vgx_Vertex_t *V ) {
  V->descriptor.property.event.sch = VERTEX_PROPERTY_EVENT_SCH_SCHEDULED;
}


/**************************************************************************//**
 * __vertex_clear_event_scheduled
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_event_scheduled( vgx_Vertex_t *V ) {
  V->descriptor.property.event.sch = VERTEX_PROPERTY_EVENT_SCH_NONE;
}


/* property.scope */

// property.scope.def
__inline static bool __vertex_scope_is_local( const vgx_Vertex_t *V ) {
  return V->descriptor.property.scope.def == VERTEX_PROPERTY_SCOPE_LOCAL;
}


/**************************************************************************//**
 * __vertex_set_scope_local
 *
 ******************************************************************************
 */
__inline static void __vertex_set_scope_local( vgx_Vertex_t *V ) {
  V->descriptor.property.scope.def = VERTEX_PROPERTY_SCOPE_LOCAL;
}


// property.scope.global

/**************************************************************************//**
 * __vertex_scope_is_global
 *
 ******************************************************************************
 */
__inline static bool __vertex_scope_is_global( const vgx_Vertex_t *V ) {
  return V->descriptor.property.scope.def == VERTEX_PROPERTY_SCOPE_GLOBAL;
}


/**************************************************************************//**
 * __vertex_set_scope_global
 *
 ******************************************************************************
 */
__inline static void __vertex_set_scope_global( vgx_Vertex_t *V ) {
  V->descriptor.property.scope.def = VERTEX_PROPERTY_SCOPE_GLOBAL;
}



/* property.vector */

// property.vector.vec
__inline static bool __vertex_has_vector( const vgx_Vertex_t *V ) {
  return V->descriptor.property.vector.vec == VERTEX_PROPERTY_VECTOR_VEC_EXISTS;
}


/**************************************************************************//**
 * __vertex_set_has_vector
 *
 ******************************************************************************
 */
__inline static void __vertex_set_has_vector( vgx_Vertex_t *V ) {
  V->descriptor.property.vector.vec = VERTEX_PROPERTY_VECTOR_VEC_EXISTS;
}


/**************************************************************************//**
 * __vertex_clear_has_vector
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_has_vector( vgx_Vertex_t *V ) {
  V->descriptor.property.vector.vec = VERTEX_PROPERTY_VECTOR_VEC_NONE;
}

// property.vector.ctr

/**************************************************************************//**
 * __vertex_has_centroid
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_centroid( const vgx_Vertex_t *V ) {
  return V->descriptor.property.vector.ctr == VERTEX_PROPERTY_VECTOR_CTR_CENTROID;
}


/**************************************************************************//**
 * __vertex_set_has_centroid
 *
 ******************************************************************************
 */
__inline static void __vertex_set_has_centroid( vgx_Vertex_t *V ) {
  V->descriptor.property.vector.ctr = VERTEX_PROPERTY_VECTOR_CTR_CENTROID;
}


/**************************************************************************//**
 * __vertex_clear_has_centroid
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_has_centroid( vgx_Vertex_t *V ) {
  V->descriptor.property.vector.ctr = VERTEX_PROPERTY_VECTOR_CTR_STANDARD;
}

/* property.degree */

// property.degree.in
__inline static bool __vertex_has_inarcs( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.in == VERTEX_PROPERTY_DEGREE_IN_NONZERO;
}


/**************************************************************************//**
 * __vertex_set_has_inarcs
 *
 ******************************************************************************
 */
__inline static void __vertex_set_has_inarcs( vgx_Vertex_t *V ) {
  V->descriptor.property.degree.in = VERTEX_PROPERTY_DEGREE_IN_NONZERO;
}


/**************************************************************************//**
 * __vertex_clear_has_inarcs
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_has_inarcs( vgx_Vertex_t *V ) {
  V->descriptor.property.degree.in = VERTEX_PROPERTY_DEGREE_IN_ZERO;
}

// property.degree.out

/**************************************************************************//**
 * __vertex_has_outarcs
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_outarcs( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.out == VERTEX_PROPERTY_DEGREE_OUT_NONZERO;
}


/**************************************************************************//**
 * __vertex_set_has_outarcs
 *
 ******************************************************************************
 */
__inline static void __vertex_set_has_outarcs( vgx_Vertex_t *V ) {
  V->descriptor.property.degree.out = VERTEX_PROPERTY_DEGREE_OUT_NONZERO;
}


/**************************************************************************//**
 * __vertex_clear_has_outarcs
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_has_outarcs( vgx_Vertex_t *V ) {
  V->descriptor.property.degree.out = VERTEX_PROPERTY_DEGREE_OUT_ZERO;
}

// property.degree interpretations

/**************************************************************************//**
 * __vertex_set_isolated
 *
 ******************************************************************************
 */
__inline static void __vertex_set_isolated( vgx_Vertex_t *V ) {
  V->descriptor.property.degree.bits = VERTEX_PROPERTY_DEGREE_ISOLATED;
}


/**************************************************************************//**
 * __vertex_is_isolated
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_isolated( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.bits == VERTEX_PROPERTY_DEGREE_ISOLATED;
}


/**************************************************************************//**
 * __vertex_is_source
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_source( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.bits == VERTEX_PROPERTY_DEGREE_SOURCE;
}


/**************************************************************************//**
 * __vertex_is_sink
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_sink( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.bits == VERTEX_PROPERTY_DEGREE_SINK;
}


/**************************************************************************//**
 * __vertex_is_internal
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_internal( const vgx_Vertex_t *V ) {
  return V->descriptor.property.degree.bits == VERTEX_PROPERTY_DEGREE_INTERNAL;
}


/* type.enumeration */
__inline static vgx_vertex_type_t __vertex_get_type( const vgx_Vertex_t *V ) {
  return V->descriptor.type.enumeration;
}


/**************************************************************************//**
 * __vertex_set_type
 *
 ******************************************************************************
 */
__inline static vgx_vertex_type_t __vertex_set_type( vgx_Vertex_t *V, vgx_vertex_type_t vt ) {
  return (V->descriptor.type.enumeration = vt);
}


/**************************************************************************//**
 * __vertex_clear_type
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_type( vgx_Vertex_t *V ) {
  V->descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_NONE;
}


/**************************************************************************//**
 * __vertex_set_defunct
 *
 ******************************************************************************
 */
__inline static void __vertex_set_defunct( vgx_Vertex_t *V ) {
  V->descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_DEFUNCT;
}


/**************************************************************************//**
 * __vertex_is_defunct
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_defunct( const vgx_Vertex_t *V ) {
  return V->descriptor.type.enumeration == VERTEX_TYPE_ENUMERATION_DEFUNCT;
}


// TMX (vertex)

/**************************************************************************//**
 * __vertex_set_expiration_ts
 *
 ******************************************************************************
 */
__inline static uint32_t __vertex_set_expiration_ts( vgx_Vertex_t *V, uint32_t tmx_s ) {
  return V->TMX.vertex_ts = tmx_s;
}


/**************************************************************************//**
 * __vertex_set_expiration_tms
 *
 ******************************************************************************
 */
__inline static int64_t __vertex_set_expiration_tms( vgx_Vertex_t *V, int64_t tmx_ms ) {
  return __vertex_set_expiration_ts( V, (uint32_t)(tmx_ms/1000) );
}


/**************************************************************************//**
 * __vertex_get_expiration_ts
 *
 ******************************************************************************
 */
__inline static uint32_t __vertex_get_expiration_ts( const vgx_Vertex_t *V ) {
  return V->TMX.vertex_ts;
}


/**************************************************************************//**
 * __vertex_get_expiration_tms
 *
 ******************************************************************************
 */
__inline static int64_t __vertex_get_expiration_tms( const vgx_Vertex_t *V ) {
  return 1000LL * __vertex_get_expiration_ts( V );
}


/**************************************************************************//**
 * __vertex_clear_expiration
 *
 ******************************************************************************
 */
__inline static void __vertex_clear_expiration( vgx_Vertex_t *V ) {
  V->TMX.vertex_ts = TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_has_expiration
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_expiration( const vgx_Vertex_t *V ) {
  return V->TMX.vertex_ts < TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_is_expired
 *
 ******************************************************************************
 */
__inline static bool __vertex_is_expired( const vgx_Vertex_t *V, uint32_t now_ts ) {
  return V->TMX.vertex_ts < now_ts;
}


// TMX (arc)

/**************************************************************************//**
 * __vertex_arcvector_set_expiration_ts
 *
 ******************************************************************************
 */
__inline static uint32_t __vertex_arcvector_set_expiration_ts( vgx_Vertex_t *V, uint32_t tmx_s ) {
  return V->TMX.arc_ts = tmx_s;
}


/**************************************************************************//**
 * __vertex_arcvector_set_expiration_tms
 *
 ******************************************************************************
 */
__inline static int64_t __vertex_arcvector_set_expiration_tms( vgx_Vertex_t *V, int64_t tmx_ms ) {
  return __vertex_arcvector_set_expiration_ts( V, (uint32_t)(tmx_ms/1000) );
}


/**************************************************************************//**
 * __vertex_arcvector_get_expiration_ts
 *
 ******************************************************************************
 */
__inline static uint32_t __vertex_arcvector_get_expiration_ts( const vgx_Vertex_t *V ) {
  return V->TMX.arc_ts;
}


/**************************************************************************//**
 * __vertex_arcvector_get_expiration_tms
 *
 ******************************************************************************
 */
__inline static int64_t __vertex_arcvector_get_expiration_tms( const vgx_Vertex_t *V ) {
  return 1000LL * __vertex_arcvector_get_expiration_ts( V );
}


/**************************************************************************//**
 * __vertex_arcvector_clear_expiration
 *
 ******************************************************************************
 */
__inline static void __vertex_arcvector_clear_expiration( vgx_Vertex_t *V ) {
  V->TMX.arc_ts = TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_arcvector_has_expiration
 *
 ******************************************************************************
 */
__inline static bool __vertex_arcvector_has_expiration( const vgx_Vertex_t *V ) {
  return V->TMX.arc_ts < TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_arcvector_is_expired
 *
 ******************************************************************************
 */
__inline static bool __vertex_arcvector_is_expired( const vgx_Vertex_t *V, uint32_t now_ts ) {
  return V->TMX.arc_ts <= now_ts;
}


/**************************************************************************//**
 * __vertex_has_any_expiration
 *
 ******************************************************************************
 */
__inline static bool __vertex_has_any_expiration( const vgx_Vertex_t *V ) {
  return V->TMX.vertex_ts < TIME_EXPIRES_NEVER || V->TMX.arc_ts < TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_all_clear_expiration
 *
 ******************************************************************************
 */
__inline static void __vertex_all_clear_expiration( vgx_Vertex_t *V ) {
  V->TMX.vertex_ts = TIME_EXPIRES_NEVER;
  V->TMX.arc_ts = TIME_EXPIRES_NEVER;
}


/**************************************************************************//**
 * __vertex_get_min_expiration_ts
 *
 ******************************************************************************
 */
__inline static uint32_t __vertex_get_min_expiration_ts( const vgx_Vertex_t *V ) {
  return minimum_value( V->TMX.vertex_ts, V->TMX.arc_ts );
}


/**************************************************************************//**
 * __vertex_get_min_expiration_tms
 *
 ******************************************************************************
 */
__inline static int64_t __vertex_get_min_expiration_tms( const vgx_Vertex_t *V ) {
  return 1000LL * __vertex_get_min_expiration_ts( V );
}



DLL_VISIBLE extern const vgx_Similarity_config_t DEFAULT_FEATURE_SIMCONFIG;
DLL_VISIBLE extern const vgx_Similarity_config_t DEFAULT_EUCLIDEAN_SIMCONFIG;
DLL_VISIBLE extern const vgx_Similarity_config_t UNSET_SIMCONFIG;





/**************************************************************************//**
 * __expect_vertex_WL_CS
 *
 ******************************************************************************
 */
static int __expect_vertex_WL_CS( const vgx_Vertex_t *vertex_WL ) {
  int status = 0;
  if( vertex_WL ) {
    if( __vertex_is_locked_writable_by_current_thread( vertex_WL ) == false ) {
      status = -1;
    }
  }
  else {
    status = -2;
  }
  return status;
}



/**************************************************************************//**
 * __expect_vertex_iWL_CS
 *
 ******************************************************************************
 */
static int __expect_vertex_iWL_CS( const vgx_Vertex_t *vertex_iWL ) {
  // vertex must be at least iWL (may be WL)
  int status = 0;
  if( vertex_iWL ) {
    // if WL by current thread it's ok, but if not we have to check for iWL
    if( __vertex_is_locked_writable_by_current_thread( vertex_iWL ) == false ) {
      // problem 1: if not write locked at all
      if( __vertex_is_locked_writable( vertex_iWL ) == false ) {
        status = -1;
      }
      // problem 2: if WL by other thread and inarcs not yielded
      if( __vertex_is_inarcs_yielded( vertex_iWL ) == false ) {
        status = -2;
      }
      // problem 3: if inarcs yielded but not borrowed
      if( __vertex_is_borrowed_inarcs_busy( vertex_iWL ) == false ) {
        status = -3;
      }
      // OK so far. All we know at this point is that the vertex is
      // write locked by another thread and that the inarcs have been yielded
      // and borrowed. But we don't know WHO borrowed them, we can only
      // assume that they were borrowed by the current thread. The
      // vertex descriptor does not have room to store these details.
    }
  }
  else {
    status = -4;
  }
  return status;
}





#endif
