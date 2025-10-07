/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    framehash.h
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

#ifndef FRAMEHASH_H
#define FRAMEHASH_H

#include "comlib.h"
#include "cxmalloc.h"

#if defined _FRAMEHASH_EXPORT
#define DLL_FRAMEHASH_PUBLIC DLL_EXPORT
#else
#define DLL_FRAMEHASH_PUBLIC DLL_IMPORT
#endif

struct s_framehash_t;
struct s_framehash_processing_context_t;


#define FRAMEHASH_256CHUNKS_PER_SLOT        ((int)(sizeof(cacheline_t)/sizeof(__m256i)))
#define FRAMEHASH_CELLS_PER_SLOT            ((int)(sizeof(cacheline_t)/sizeof(framehash_cell_t)))
#define FRAMEHASH_256CHUNKS_PER_HALFSLOT    (FRAMEHASH_256CHUNKS_PER_SLOT/2)
#define FRAMEHASH_CELLS_PER_HALFSLOT        (FRAMEHASH_CELLS_PER_SLOT/2)

#define FRAMEHASH_DEFAULT_ORDER             6
#define FRAMEHASH_DEFAULT_MAX_CACHE_DOMAIN  3
#define FRAMEHASH_ARG_UNDEFINED             -1

#define FRAMEHASH_KEY_NONE                  0x8000000000000000ULL
#define FRAMEHASH_REF_NULL                  0ULL

typedef aptr_t framehash_cell_t;



typedef union u_framehash_slot_t {
  framehash_cell_t cells[ FRAMEHASH_CELLS_PER_SLOT ];
  __m256i _chunks[FRAMEHASH_256CHUNKS_PER_SLOT];
} framehash_slot_t;


typedef union u_framehash_halfslot_t {
  framehash_cell_t cells[ FRAMEHASH_CELLS_PER_HALFSLOT ];
  __m256i _chunks[FRAMEHASH_256CHUNKS_PER_HALFSLOT];
} framehash_halfslot_t;




// !!!!
// NOTE: The enumeration values MUST BE EXACTLY THESE NUMBERS
// !!!!
typedef enum e_framehash_ftype_t {
  FRAME_TYPE_CACHE    = 0,  // header-less frame where each slot is a cache slot
  FRAME_TYPE_LEAF     = 1,  // frame where all slots are leaf slots
  FRAME_TYPE_INTERNAL = 2,  // frame where one or more slots are chain slots
  FRAME_TYPE_BASEMENT = 3,  // special outlier frame
  FRAME_TYPE_NONE     = 7
} framehash_ftype_t;


typedef enum _e_framehash_keytype_t {
  CELL_KEY_TYPE_NONE    = TAGGED_DKEY_NONE,
  CELL_KEY_TYPE_PLAIN64 = TAGGED_DKEY_PLAIN64,
  CELL_KEY_TYPE_HASH64  = TAGGED_DKEY_HASH64,
  CELL_KEY_TYPE_HASH128 = TAGGED_DKEY_HASH128,
  CELL_KEY_TYPE_ERROR
} framehash_keytype_t;


typedef enum _e_framehash_valuetype_t {
  CELL_VALUE_TYPE_NULL,       //
  CELL_VALUE_TYPE_MEMBER,     //
  CELL_VALUE_TYPE_BOOLEAN,    //  true/false
  CELL_VALUE_TYPE_UNSIGNED,   //  56-bit unsigned integer
  CELL_VALUE_TYPE_INTEGER,    //  56-bit signed integer
  CELL_VALUE_TYPE_REAL,       //  56-bit floating point
  CELL_VALUE_TYPE_POINTER,    //  generic pointer to anything
  CELL_VALUE_TYPE_OBJECT64,   //  pointer to COMLIB object, i.e. expects ptr->vtable to be valid, keyed by PLAIN64 or HASH64
  CELL_VALUE_TYPE_OBJECT128,  //  pointer to COMLIB object, i.e. expects ptr->vtable to be valid, keyed by HASH128
  CELL_VALUE_TYPE_NOACCESS,   //
  CELL_VALUE_TYPE_ERROR       //
} framehash_valuetype_t;


typedef enum _e_framehash_fcache_type_t {
  FCACHE_TYPE_NONE   = 0, // reserved
  FCACHE_TYPE_CMORPH = 1, // frame is allowed to morph between a true cache frame and internal frame
  FCACHE_TYPE_CZONEP = 2, // frame is allowed to cache in largest zone k=p when chainzone is fully converted
  FCACHE_TYPE_STATIC = 3  // frame is always a true cache and cannot be converted to another frame type 
} _framehash_fcache_type_t;


typedef struct s_framehash_metas_t {
  union {
    uint64_t QWORD;       // enforce struct to be a qword
    struct {  
      union {
        uint32_t dword;       // 32-bit integer for custom use when domain and fileno are implied
        struct {
          struct {
            uint8_t cancache  : 1;  // flag=1 if frame is allowed to perform caching when chainzone is fully converted
            uint8_t cachetyp  : 2;  // FCACHE TYPE to use for performing caching, if allowed
            uint8_t readonly  : 1;  // flag=1 if write operations are prohibited in this frame
            uint8_t dobalance : 1;
            uint8_t __rsv     : 3;  //
          } flags;
          uint8_t domain;     // domain=i is the frame's number of levels from the top. i=0 is the top frame.
          uint16_t fileno;    // File descriptor: 0,1,2 are reserved. Assume < 65536 open files. (although open() returns int, we'll manage it.)
        };
      };
      struct {
        struct {
          uint8_t order : 4;  // order=p is the log2 of number of cache lines in frame. Cache frames only has one zone.
          uint8_t ftype : 4;  // Frame type (downcast from enum/int)
        };
        union {
          uint8_t nactive;      // number of active items in frame (excluding chains, items in subframes, and cached items)
          union {
            uint8_t _acc;
            struct {
              uint8_t low : 4;
              uint8_t val : 4;
            };
          } hitrate;
        };
        union {
          uint16_t chainbits; // Bitmap indicating which slots in zone k=K_MAX-1 are chainslots. APPLIES TO INTERNAL FRAMES ONLY.
          uint16_t nchains;   // Number of non-NULL chains in a cache frame. APPLIES TO CACHE FRAMES ONLY.
          uint16_t hasnext;   // True/false indicating whether a subframe exists. APPLIES TO BASEMENT FRAMES ONLY.
        };
      };
    };
  };
} framehash_metas_t;



CALIGNED_TYPE(union) u_framehash_access_guard_t {
  cacheline_t __cl;
  struct {
    CS_COND ready;    // 6 Q
    int32_t busy;     // .5Q (enum)
    int32_t _rsv1_;   // .5Q
    int32_t _rsv2_;   // .5Q
    int32_t _rsv3_;   // .5Q
  };
} framehash_access_guard_t;



/*******************************************************************//**
 * TEST OBJECT
 *
 *
 ***********************************************************************
 */
struct s_FramehashTestObject_t;

typedef struct s_FramehashTestObject_constructor_args_t {
  int _rsv_;
} FramehashTestObject_constructor_args_t;


typedef struct s_FramehashTestObject_t_vtable_t {
  // base type head
  COMLIB_VTABLE_HEAD
  // extensions
  int (*DemoMethod)( const struct s_FramehashTestObject_t *self );
} FramehashTestObject_t_vtable_t;



typedef struct s_FramehashTestObject_t {
  COMLIB_OBJECT_HEAD( FramehashTestObject_t_vtable_t )
  objectid_t obid;
} FramehashTestObject_t;



/* Interpret the cell annotation as a pointer to frame metas */
#define CELL_GET_FRAME_METAS( CellPtr )         ((framehash_metas_t*)APTR_GET_ANNOTATION_POINTER( CellPtr ))

/* Interpret the cell annotation as the frame type of the frame metas */
#define CELL_GET_FRAME_TYPE( CellPtr )          (CELL_GET_FRAME_METAS( CellPtr )->ftype)

/* Interpret the cell reference as a pointer to a slot array */
#define CELL_GET_FRAME_SLOTS( CellPtr )         ((framehash_slot_t*)APTR_GET_POINTER( CellPtr ))

/* Interpret the cell reference as a pointer to a cell array */
#define CELL_GET_FRAME_CELLS( CellPtr )         ((framehash_cell_t*)APTR_GET_POINTER( CellPtr ))

/* Set the cell reference to NULL, not touching the tag */
#define DELETE_CELL_REFERENCE( CellPtr )        APTR_SET_POINTER( CellPtr, NULL )





/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef int64_t (*f_framehash_cell_processor_t)( struct s_framehash_processing_context_t * const context, framehash_cell_t * const cell );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_constructor_args_t {
  struct {                                            /* 1     */
    int8_t order;
    int8_t synchronized;
    int8_t shortkeys;
    int8_t cache_depth;
    int8_t obclass_changelog;
    int8_t _rsv[2];
  } param;
  const char *dirpath;                                /* 1     */
  const char *name;                                   /* 1     */
  CQwordQueue_t *input_queue;                         /* 1     */
  char *fpath;                                        /* 1     */
  cxmalloc_family_t **frame_allocator;                /* 1     */
  cxmalloc_family_t **basement_allocator;             /* 1     */
  shortid_t (*shortid_hashfunction)( QWORD key );     /* 1     */

} framehash_constructor_args_t;


DLL_FRAMEHASH_PUBLIC extern const framehash_constructor_args_t FRAMEHASH_DEFAULT_ARGS;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct _s_framehash_op_counters_t {
  // -----------------------------------------------
  // [Q1]
  CS_LOCK lock;
  // -----------------------------------------------
  // [Q2.1-6]
  struct {
    size_t depth;
    size_t nCL;
    size_t ncachecells;
    size_t nleafcells;
    size_t nleafzones;
    size_t nbasementcells;
  } probe;
  // [Q2.7-8]
  struct {
    size_t hits;
    size_t misses;
  } cache;
  // -----------------------------------------------
  // [Q3.1]
  size_t opcount;
  // [Q3.2]
  size_t resize_up;
  // [Q3.3]
  size_t resize_down;
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
} _framehash_counters_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_percounters_t {
  _framehash_counters_t read;
  _framehash_counters_t write;
} framehash_perfcounters_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_instrument_locator_t {
  int probe_i;
  int domain_h;
  framehash_ftype_t ftype;
  int order_p;
  int zone_k;
  unsigned hashbits;
  framehash_cell_t *cell;
} framehash_instrument_locator_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_instrument_t {
  struct {
    int depth;            // The highest domain probed
    int nCL;              // The number of processor cache lines fetched from memory to complete the operation
    int ncachecells;      // Number of cache cells visited during probing
    int nleafcells;       // Number of leaf cells visited during probing (includes leaf, internal)
    int nleafzones;       // Number of leaf zones visited during probing (includes leaf, internal)
    int nbasementcells;   // Number of basement cells visited during probing
    bool hit;             // 1=found what we were looking for, 0=did not find what we were looking for
    framehash_ftype_t hit_ftype;  // frame type where we found what we were looking for if we found it
  } probe;
  framehash_instrument_locator_t *trail;
  struct {
    int nup;
    int ndown;
  } resize;
} framehash_instrument_t;


#define _FRAMEHASH_INIT_INSTRUMENT {0}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_dynamic_t {
  // -----------------------------------------------
  // [Q.1]
  CS_LOCK lock;

  // -----------------------------------------------
  // [Q2.1]
  CS_LOCK *pflock;
  // [Q2.2.1]
  int cache_depth;
  // [Q2.2.2]
  int __rsv_2_2_2;
  // [Q2.3]
  shortid_t (*hashf)( QWORD key );
  // [Q2.4]
  cxmalloc_family_t *falloc;
  // [Q2.5]
  cxmalloc_family_t *balloc;
  // [Q2.6]
  CaptrList_t *cell_list;
  // [Q2.7]
  CaptrHeap_t *cell_heap;
  // [Q2.8]
  QWORD __rsv_2_8;

  // -----------------------------------------------
  // [Q3.1]
  CtptrList_t *ref_list;
  // [Q3.2]
  CtptrHeap_t *ref_heap;
  // [Q3.3]
  CQwordList_t *annotation_list;
  // [Q3.4]
  CQwordHeap_t *annotation_heap;
  // [Q3.5]
  QWORD __rsv_3_5;
  // [Q3.6]
  QWORD __rsv_3_6;
  // [Q3.7]
  QWORD __rsv_3_7;
  // [Q3.8]
  QWORD __rsv_3_8;

} framehash_dynamic_t;



#define DYNAMIC_LOCK( DynPtr )                  \
  do {                                          \
    CXLOCK_TYPE *__pdynlock__ = &(((DynPtr)->lock).lock);  \
    ENTER_CRITICAL_SECTION( __pdynlock__ );     \
    do


#define DYNAMIC_RELEASE                         \
    WHILE_ZERO;                                 \
    LEAVE_CRITICAL_SECTION( __pdynlock__ );     \
  } WHILE_ZERO



#define FORCE_DYNAMIC_RELEASE               \
  LEAVE_CRITICAL_SECTION( __pdynlock__ );   \
  __pdynlock__ = NULL                       \
  /*               ^ expect ; in user code for consistency  */
  /* better know what you're doing at this point            */




#define FRAMEHASH_DYNAMIC_PUSH_CELL_LIST( DynamicPtr, CellListPtr ) \
do {                                                                \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                    \
  CaptrList_t *__cell_list__ = __dynamic__->cell_list;              \
  __dynamic__->cell_list = CellListPtr;                             \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_CELL_LIST                             \
  WHILE_ZERO;                                                       \
  __dynamic__->cell_list = __cell_list__;                           \
} WHILE_ZERO


#define FRAMEHASH_DYNAMIC_PUSH_CELL_HEAP( DynamicPtr, CellHeapPtr ) \
do {                                                                \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                    \
  CaptrHeap_t *__cell_heap__ = __dynamic__->cell_heap;              \
  __dynamic__->cell_heap = CellHeapPtr;                             \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_CELL_HEAP                             \
  WHILE_ZERO;                                                       \
  __dynamic__->cell_heap = __cell_heap__;                           \
} WHILE_ZERO


#define FRAMEHASH_DYNAMIC_PUSH_REF_LIST( DynamicPtr, RefListPtr )   \
do {                                                                \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                    \
  CtptrList_t *__ref_list__ = __dynamic__->ref_list;                \
  __dynamic__->ref_list = RefListPtr;                               \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_REF_LIST                              \
  WHILE_ZERO;                                                       \
  __dynamic__->ref_list = __ref_list__;                             \
} WHILE_ZERO


#define FRAMEHASH_DYNAMIC_PUSH_REF_HEAP( DynamicPtr, RefHeapPtr )   \
do {                                                                \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                    \
  CtptrHeap_t *__ref_heap__ = __dynamic__->ref_heap;                \
  __dynamic__->ref_heap = RefHeapPtr;                               \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_REF_HEAP                              \
  WHILE_ZERO;                                                       \
  __dynamic__->ref_heap = __ref_heap__;                             \
} WHILE_ZERO


#define FRAMEHASH_DYNAMIC_PUSH_ANNOTATION_LIST( DynamicPtr, AnnotationListPtr ) \
do {                                                                            \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                                \
  CtptrList_t *__annotation_list__ = __dynamic__->annotation_list;              \
  __dynamic__->annotation_list = AnnotationListPtr;                             \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_ANNOTATION_LIST                                   \
  WHILE_ZERO;                                                                   \
  __dynamic__->annotation_list = __annotation_list__;                           \
} WHILE_ZERO


#define FRAMEHASH_DYNAMIC_PUSH_ANNOTATION_HEAP( DynamicPtr, AnnotationHeapPtr ) \
do {                                                                            \
  framehash_dynamic_t *__dynamic__ = DynamicPtr;                                \
  CtptrList_t *__annotation_heap__ = __dynamic__->annotation_heap;              \
  __dynamic__->annotation_heap = AnnotationHeapPtr;                             \
  do /* recursion call here */

#define FRAMEHASH_DYNAMIC_POP_ANNOTATION_HEAP                                   \
  WHILE_ZERO;                                                                   \
  __dynamic__->annotation_heap = __annotation_heap__;                           \
} WHILE_ZERO



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef union u_framehash_control_flags_t {
  QWORD bits;
  struct {
    uint8_t __expect_nonexist;  /* Set to non-zero ONLY WHEN FILLING FRAMES FOR THE FIRST TIME */
    struct {
      uint8_t enable_read   : 1;
      uint8_t enable_write  : 1;
      uint8_t allow_cmorph  : 1;
      uint8_t __rsv         : 5;
    } cache;
    struct {
      uint8_t high;   // Max loadfactor allowed in frame, used when >0
      uint8_t low;    // Min loadfactor allowed in frame, used when >0
    } loadfactor;
    struct {
      uint8_t minimal       : 1;  // When 1 all frame expansion will minimize memory consumption
      uint8_t __rsv         : 7;
    } growth;
    struct {
      uint8_t __rsv[3];
    } __rsv;
  };
} framehash_control_flags_t;




DLL_FRAMEHASH_PUBLIC extern const framehash_control_flags_t FRAMEHASH_CONTROL_FLAGS_INIT;
DLL_FRAMEHASH_PUBLIC extern const framehash_control_flags_t FRAMEHASH_CONTROL_FLAGS_REHASH;




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
CALIGNED_TYPE(struct) s_framehash_context_t {
  /* CL 1 */
  framehash_cell_t *frame;            /* 1    frame to be visited (next in recursion) */
  struct {
    QWORD plain;                      /* 1    */
    shortid_t shortid;                /* 1    */
  } key;
  const objectid_t *obid;             /* 1    */
  framehash_dynamic_t *dynamic;       /* 1    */
  union {                             /* 1    */
    QWORD     qword;                  /*      raw QWORD */
    uint64_t  idH56;                  /*      56 bits of idH (8 MSB are discarded) */
    QWORD     raw56;                  /*      56 bit data value (8 MSB are discarded) */
    int64_t   int56;                  /*      56 bit signed integer value */
    double    real56;                 /*      56 bit precision floating point */
    char      *ptr56;                 /*      generic 56-bit pointer */
    comlib_object_t *obj56;           /*      COMLIB object pointer, keyed by PLAIN64 or HASH64*/
    comlib_object_t *pobject;         /*      COMLIB object pointer, keyed by HASH128 */
  } value;
  framehash_keytype_t ktype;          /* 0.5  */
  framehash_valuetype_t vtype;        /* 0.5  */
  framehash_control_flags_t control;  /* 1    */
  /* CL 2 */
#ifdef FRAMEHASH_INSTRUMENTATION
  framehash_instrument_t *instrument; /* 1    */
  QWORD __rsv[7];                     /* 7    */
                                      /* 16   */
#else
                                      /* 8    */
#endif
} framehash_context_t;


#ifdef FRAMEHASH_INSTRUMENTATION
DLL_FRAMEHASH_PUBLIC extern framehash_instrument_t null_instrument;
#define _FRAMEHASH_CONTEXT_NULL_INSTRUMENT , .instrument = &null_instrument
#define _FRAMEHASH_CONTEXT_SET_NULL_INSTRUMENT( Context )  ((Context).instrument = &null_instrument)
#else
#define _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
#define _FRAMEHASH_CONTEXT_SET_NULL_INSTRUMENT( Context )
#endif



#define CONTEXT_INIT_TOP_FRAME( TopPtr, DynPtr ) {    \
  .frame=(TopPtr),                            /* */   \
  .dynamic=(DynPtr),                          /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define CONTEXT_INIT_NEW_FRAME( ParentContextPtr, NewTargetPtr ) { \
  .frame=(NewTargetPtr),                      /* */   \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_REHASH( ParentContextPtr, RehashTargetPtr ) { \
  .frame=(RehashTargetPtr),                   /* */   \
  .obid=NULL,                                 /* */   \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_REHASH     /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_MIGRANT( ParentContextPtr ) {   \
  .frame=NULL,                                /* */   \
  .obid=NULL,                                 /* */   \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_REHASH     /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_CACHE_CONVERT( ParentContextPtr, CacheTargetPtr ) { \
  .frame=(CacheTargetPtr),                    /* */   \
  .key={0},                                   /* */   \
  .obid=NULL,                                 /* */   \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .value=0,                                   /* */   \
  .ktype=CELL_KEY_TYPE_ERROR,                 /* */   \
  .vtype=CELL_VALUE_TYPE_NULL,                /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_WRITEBACK( ParentContextPtr, ChainTargetPtr ) { \
  .frame=(ChainTargetPtr),                    /* */   \
  .obid=NULL,                                 /* */   \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_ROLLBACK( ParentContextPtr ) {  \
  .dynamic=(ParentContextPtr)->dynamic,       /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}

#define _CONTEXT_INIT_CELL_DESERIALIZE {              \
  .frame=NULL,                                /* */   \
  .obid=NULL,                                 /* */   \
  .dynamic=NULL,                              /* */   \
  .control=FRAMEHASH_CONTROL_FLAGS_INIT       /* */   \
  _FRAMEHASH_CONTEXT_NULL_INSTRUMENT          /* */   \
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
CALIGNED_TYPE(struct) s_framehash_processing_context_t {
  struct {
    framehash_cell_t *frame;
    framehash_dynamic_t *dynamic;
  } instance;
  struct {
    f_framehash_cell_processor_t function;
    int64_t limit;
    void *input;
    void *output;
  } processor;
  union {
    QWORD bits;
    struct {
      uint8_t allow_cached;
      uint8_t readonly;
      uint8_t failed;
      uint8_t completed;
      uint8_t __rsv[4];
    };
  } flags;
  struct {
    int64_t __delta_items;
  } __internal;
} framehash_processing_context_t;



DLL_FRAMEHASH_PUBLIC extern framehash_processing_context_t FRAMEHASH_PROCESSOR_NEW_CONTEXT( framehash_cell_t *startframe, framehash_dynamic_t *dynamic, f_framehash_cell_processor_t function );
DLL_FRAMEHASH_PUBLIC extern framehash_processing_context_t FRAMEHASH_PROCESSOR_NEW_CONTEXT_LIMIT( framehash_cell_t *startframe, framehash_dynamic_t *dynamic, f_framehash_cell_processor_t function, int64_t limit );
DLL_FRAMEHASH_PUBLIC extern void FRAMEHASH_PROCESSOR_SET_IO( framehash_processing_context_t *context, void *input, void *output );
DLL_FRAMEHASH_PUBLIC extern void FRAMEHASH_PROCESSOR_MAY_MODIFY( framehash_processing_context_t *context );
DLL_FRAMEHASH_PUBLIC extern void FRAMEHASH_PROCESSOR_PRESERVE_CACHE( framehash_processing_context_t *context );
DLL_FRAMEHASH_PUBLIC extern void FRAMEHASH_PROCESSOR_DELETE_CELL( framehash_processing_context_t *context, framehash_cell_t *cell );


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_CURRENT_FRAME
 *
 ******************************************************************************
 */
__inline static framehash_cell_t * FRAMEHASH_PROCESSOR_CURRENT_FRAME( framehash_processing_context_t *context ) {
  return context->instance.frame;
}


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_SET_FAILED
 *
 ******************************************************************************
 */
__inline static void FRAMEHASH_PROCESSOR_SET_FAILED( framehash_processing_context_t *context ) {
  context->flags.failed = 1;
}


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_IS_FAILED
 *
 ******************************************************************************
 */
__inline static int FRAMEHASH_PROCESSOR_IS_FAILED( const framehash_processing_context_t *context ) {
  return context->flags.failed;
}


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_SET_COMPLETED
 *
 ******************************************************************************
 */
__inline static void FRAMEHASH_PROCESSOR_SET_COMPLETED( framehash_processing_context_t *context ) {
  context->flags.completed = 1;
}


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_IS_COMPLETED
 *
 ******************************************************************************
 */
__inline static int FRAMEHASH_PROCESSOR_IS_COMPLETED( const framehash_processing_context_t *context ) {
  return context->flags.completed;
}


/**************************************************************************//**
 * FRAMEHASH_PROCESSOR_INHERIT_COMPLETION
 *
 ******************************************************************************
 */
__inline static void FRAMEHASH_PROCESSOR_INHERIT_COMPLETION( framehash_processing_context_t *dest, const framehash_processing_context_t *src ) {
  dest->flags.completed = src->flags.completed;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef int (*f_framehash_cell_collector_t)( const framehash_cell_t * const cell );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_cell_simple_filter_t {

  int64_t n;
  // TODO:
  // Add useful filter criteria as long as they're simple

} framehash_cell_simple_filter_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef int (*f_framehash_cell_filter_t)( const framehash_cell_t * const cell );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef union u_framehash_retcode_t {
  uint64_t code;
  struct {
    uint8_t completed;
    uint8_t cacheable;
    uint8_t modified;
    uint8_t empty;
    uint8_t unresizable;
    uint8_t error;
    uint8_t delta;
    uint8_t depth;
  };
} framehash_retcode_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_cache_hitrate_t {
  double rate[ 5 ];
  int64_t accval[ 5 ];
  int64_t count[ 5 ];
} framehash_cache_hitrate_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_IFramehashMathProtocol {
  int64_t (*Mul)( struct s_framehash_t * const self, double factor );
  int64_t (*Add)( struct s_framehash_t * const self, double add );
  int64_t (*Sqrt)( struct s_framehash_t * const self );
  int64_t (*Pow)( struct s_framehash_t * const self, double exponent );
  int64_t (*Log)( struct s_framehash_t * const self, double base );
  int64_t (*Exp)( struct s_framehash_t * const self, double base );
  int64_t (*Decay)( struct s_framehash_t * const self, double exponent );
  int64_t (*Set)( struct s_framehash_t * const self, double value );
  int64_t (*Randomize)( struct s_framehash_t * const self );
  int64_t (*Int)( struct s_framehash_t * const self );
  int64_t (*Float)( struct s_framehash_t * const self );
  int64_t (*Abs)( struct s_framehash_t * const self );
  int64_t (*Sum)( struct s_framehash_t * const self, double *sum );
  int64_t (*Avg)( struct s_framehash_t * const self, double *avg );
  int64_t (*Stdev)( struct s_framehash_t * const self, double *stdev );
} IFramehashMathProtocol;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_vtable_t {
  COMLIB_VTABLE_HEAD
  /* api generic */
  framehash_valuetype_t (*Set)(         struct s_framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const void *pkey, const void * const data_bytes );
  framehash_valuetype_t (*Delete)(      struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
  framehash_valuetype_t (*Has)(         struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
  framehash_valuetype_t (*Get)(         struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, void *return_bytes );
  framehash_valuetype_t (*Inc)(         struct s_framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const QWORD key, void * const upsert_bytes, void * return_bytes );

  /* api object */
  framehash_valuetype_t (*SetObj)(      struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, const comlib_object_t * const obj );
  framehash_valuetype_t (*DelObj)(      struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
  framehash_valuetype_t (*GetObj)(      struct s_framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, comlib_object_t ** const retobj );
  framehash_valuetype_t (*SetObj128Nolock)( struct s_framehash_t * const self, const objectid_t *pobid, const comlib_object_t * const obj );
  framehash_valuetype_t (*DelObj128Nolock)( struct s_framehash_t * const self, const objectid_t *pobid );
  framehash_valuetype_t (*GetObj128Nolock)( struct s_framehash_t * const self, const objectid_t *pobid, comlib_object_t ** const retobj );
  framehash_valuetype_t (*HasObj128Nolock)( struct s_framehash_t * const self, const objectid_t *pobid );

  /* api int56 */
  framehash_valuetype_t (*SetInt56)(    struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t value );
  framehash_valuetype_t (*DelKey64)(    struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key );
  framehash_valuetype_t (*HasKey64)(    struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key );
  framehash_valuetype_t (*GetInt56)(    struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, int64_t *ret_val );
  framehash_valuetype_t (*IncInt56)(    struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t inc_value, int64_t * const retval );

  /* api real56 */
  framehash_valuetype_t (*SetReal56)(   struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double value );
  framehash_valuetype_t (*GetReal56)(   struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, double *ret_val );
  framehash_valuetype_t (*IncReal56)(   struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double inc_value, double * const retval );

  /* api pointer */
  framehash_valuetype_t (*SetPointer)(  struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const void * const ptr );
  framehash_valuetype_t (*GetPointer)(  struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, void ** const retptr );
  framehash_valuetype_t (*IncPointer)(  struct s_framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const uintptr_t inc_bytes, void ** const retptr );

  /* api iterator */ 
  int64_t (*GetKeys)( struct s_framehash_t * const self );
  int64_t (*GetValues)( struct s_framehash_t * const self );
  int64_t (*GetItems)( struct s_framehash_t * const self );

  /* api manage */
  int (*Flush)( struct s_framehash_t *self, bool invalidate );
  int (*Discard)( struct s_framehash_t *self );
  int (*Compactify)( struct s_framehash_t *self );
  int (*CompactifyPartial)( struct s_framehash_t *self, uint64_t selector );
  int (*SetReadonly)( struct s_framehash_t *self );
  int (*IsReadonly)( struct s_framehash_t *self );
  int (*ClearReadonly)( struct s_framehash_t *self );
  int (*EnableReadCaches)( struct s_framehash_t *self );
  int (*DisableReadCaches)( struct s_framehash_t *self );
  int (*EnableWriteCaches)( struct s_framehash_t *self );
  int (*DisableWriteCaches)( struct s_framehash_t *self );

  /* api info */
  int64_t (*Items)( const struct s_framehash_t *self );
  const CString_t * (*Masterpath)( const struct s_framehash_t *self );
  framehash_cache_hitrate_t (*Hitrate)( struct s_framehash_t * const self );
  
  /* processing */
  int64_t (*CountActive)( struct s_framehash_t *self );
  int64_t (*Process)( struct s_framehash_t *self, framehash_processing_context_t *processor );
  int64_t (*ProcessPartial)( struct s_framehash_t *self, framehash_processing_context_t *processor, uint64_t selector );

  /* serialization */
  int64_t (*BulkSerialize)( struct s_framehash_t *self, bool force );
  int64_t (*Erase)( struct s_framehash_t *self );

  /* framemath */
  IFramehashMathProtocol *Math;

  /* instrumentation */
  framehash_perfcounters_t * (*GetPerfCounters)( struct s_framehash_t * const self, framehash_perfcounters_t *target );
  void (*ResetPerfCounters)( struct s_framehash_t * const self );

  /* debug */
  void (*PrintAllocators)( struct s_framehash_t * const self );
  int (*CheckAllocators)( struct s_framehash_t * const self );

} framehash_vtable_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_framehash_t {
  // -----------------------------------------------
  // [Q1.1/2]
  COMLIB_OBJECT_HEAD( framehash_vtable_t )  // 2
  // [Q1.3/4]
  framehash_cell_t _topframe;               // 2    annotated pointer to the top cache frame
  // [Q1.5]
  int64_t _nobj;                            // 1    number of active objects
  // [Q1.6]
  uint64_t _opcnt;                          // 1    number of modifying operations executed
  // [Q1.7.1]
  struct {
    int8_t synchronized;                    //  1/8   indicates synchronized behavior is desired
    int8_t readonly;                        //  1/8   >0 means writes are disallowed, but read caching is still allowed
    struct {                                //  1/8
      uint8_t shortkeys   : 1;              //        1 indicates we allow HASH64 keys for objects
      uint8_t clean       : 1;              //        1 means all leaf cells have up-to-date values (i.e. no dirty caches)
      uint8_t persisted   : 1;              //        1 means all data is persisted to disk, 0 means unpersisted data exists
      uint8_t ready       : 1;              //        1 means instance can be used
      uint8_t dontenter   : 1;              //        recursion detector for serialization
      uint8_t __rsv_3_3   : 3;
    };
    struct {                                //  1/8
      uint8_t r_ena       : 1;              //        1 means read caches are enabled
      uint8_t w_ena       : 1;              //        1 means write caches are enabled
      uint8_t __rsv_4_6   : 6;              //
    } cache;
  } _flag;                                  // 4/8 = 0.5
  // [Q1.7.2]
  int32_t _order;                           // 0.5  order determines number of slots in topframe as 2**order
  // [Q1.8]
  framehash_access_guard_t *_guard;         // 1    pointer to top frame's slot guards
 
  // -----------------------------------------------
  // [Q2-4]
  framehash_dynamic_t _dynamic;             // 16

  // -----------------------------------------------
  // [Q5.1/2]
  objectid_t obid;                          // 2    unique identifier
  // [Q5.3]
  CS_LOCK *_plock;                          // 1    pointer to lock inside _dynamic, or NULL if non-sync
  // [Q5.4]
  framehash_perfcounters_t *_counters;      // 1    performance counters used in debug builds
  // [Q5.5-8]
  QWORD __rsv4[4];                          // 4    *future*

  // -----------------------------------------------
  // [Q6.1]
  CString_t *_CSTR__dirname;                // 1    directory name for persisted data
  // [Q6.2]
  CString_t *_CSTR__basename;               // 1    framehash filename base
  // [Q6.3]
  CString_t *_CSTR__masterpath;             // 1    full file path
  // [Q6.4-8]
  struct {                                  // 4
    union {                                 //    0.5
      int32_t meta;
      struct {
        uint8_t enable;                     //      1/8 changelog is enabled for framehash instance when 1
        uint8_t obclass;                    //      1/8 the (single) comlib object class to register changelog for
        struct {                            //      1/8
          uint8_t __req_start : 1;          //          toggle to 1 to request startup of changelog processor thread
          uint8_t __req_stop  : 1;          //          toggle to 1 to request shutdown of changelog processor thread
          uint8_t __running   : 1;          //          changelog processor thread is running when 1
          uint8_t __rsv       : 5;          //
        } state;
        uint8_t __rsv;
      };
    };
    int32_t seq_commit;                     //    0.5   changelog commit point (future merge to be processed up to this point)
    int32_t seq_start;                      //    0.5   changelog start point (first log that has not been merged)
    int32_t seq_end;                        //    0.5   changelog end point (next log to be written)
    cxlib_thread_t monitor;                 //    1     changelog processor
    Cm256iQueue_t *Qapi;                    //    1     outer delta queue for recent api operations
    Cm256iQueue_t *Qmon;                    //    1     inner delta queue for the changelog processor
  } changelog;

} framehash_t;


DLL_FRAMEHASH_PUBLIC extern shortid_t framehash_hashing__short_hashkey( uint64_t n );
DLL_FRAMEHASH_PUBLIC extern uint32_t framehash_hashing__tiny_hashkey( uint32_t n );

#ifdef __cplusplus
extern "C" {
#endif


#ifdef __cplusplus
}
#endif



typedef const void * framehash_key_t;

/* This is a general purpose QWORD that may or may not be interpreted as a pointer, depending on the actual value type */
typedef void * framehash_value_t;

typedef union u_Key64Value56_t {
  __m128i m128;
  struct {
    QWORD key;
    int64_t value56;  // 56-bit usable
  };
} Key64Value56_t;


typedef Cm128iList_t Key64Value56List_t;

typedef Cm128iQueue_t Key64Value56Queue_t;




/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IStandardCellProcessors_t {
  int64_t (*count_nactive)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*destroy_objects)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_cell)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_cell_filter)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_cell_into_list)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_cell_into_heap)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_ref_into_list)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_ref_into_heap)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_annotation_into_list)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*collect_annotation_into_heap)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
} IStandardCellProcessors_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IStandardSubtreeProcessors_t {
  int64_t (*count_nactive)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*count_nactive_limit)( framehash_cell_t *frame, framehash_dynamic_t *dynamic, int64_t limit );
  int64_t (*destroy_objects)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_cells)( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector );
  int64_t (*collect_cells_filter)( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector, f_framehash_cell_filter_t *filter );
  int64_t (*collect_cells_into_list)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_cells_into_heap)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_refs_into_list)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_refs_into_heap)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_annotations_into_list)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
  int64_t (*collect_annotations_into_heap)( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
} IStandardSubtreeProcessors_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IMathCellProcessors_t {
  // MULTIPLY
  int64_t (*math_fmul)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_imul)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // ADD
  int64_t (*math_fadd)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_iadd)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_sqrt)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // POWER
  int64_t (*math_pow)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_square)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_cube)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // LOG
  int64_t (*math_log)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_log2)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // EXP
  int64_t (*math_exp_base)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_exp)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // DECAY
  int64_t (*math_decay)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // SET
  int64_t (*math_set)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // RANDOMIZE
  int64_t (*math_randomize)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // CAST
  int64_t (*math_int)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_float)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // MISC
  int64_t (*math_abs)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  // SUM
  int64_t (*math_sum)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_avg)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
  int64_t (*math_stdev)( framehash_processing_context_t * const context, framehash_cell_t * const cell );
} IMathCellProcessors_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashSimple_t {
  framehash_cell_t *    (*New)(           framehash_dynamic_t *dynamic );
  int64_t               (*Destroy)(       framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic );
  int                   (*Set)(           framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t fvalue );
  int                   (*SetInt)(        framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, QWORD key, int64_t value );
  framehash_valuetype_t (*Inc)(           framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t finc, framehash_value_t *pfvalue, bool *created );
  framehash_valuetype_t (*Dec)(           framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t fdec, framehash_value_t *pfvalue, bool *deleted );
  int                   (*IncInt)(        framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, QWORD key, int64_t inc, int64_t *value );
  int                   (*DecInt)(        framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, QWORD key, int64_t dec, int64_t *value, bool autodelete, bool *deleted );
  framehash_valuetype_t (*Get)(           framehash_cell_t  *entrypoint,  const framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_value_t *pfvalue );
  int                   (*GetInt)(        framehash_cell_t  *entrypoint,  const framehash_dynamic_t *dynamic, QWORD key, int64_t *value );
  int                   (*GetIntHash64)(  framehash_cell_t  *entrypoint,  shortid_t key, int64_t *rvalue );
  framehash_valuetype_t (*GetHash64)(     framehash_cell_t  *entrypoint,  shortid_t key, framehash_value_t *pfvalue );
  int                   (*Has)(           framehash_cell_t  *entrypoint,  const framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey );
  int                   (*HasInt)(        framehash_cell_t  *entrypoint,  const framehash_dynamic_t *dynamic, QWORD key );
  int                   (*HasHash64)(     framehash_cell_t  *entrypoint,  shortid_t key );
  int                   (*Del)(           framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey );
  int                   (*DelInt)(        framehash_cell_t **entrypoint,  framehash_dynamic_t *dynamic, QWORD key );
  int64_t               (*Length)(        framehash_cell_t  *entrypoint );
  bool                  (*Empty)(         framehash_cell_t  *entrypoint );
  int                   (*SetReadonly)(   framehash_cell_t  *entrypoint );
  int                   (*IsReadonly)(    framehash_cell_t  *entrypoint );
  int                   (*ClearReadonly)( framehash_cell_t  *entrypoint );
  Key64Value56List_t *  (*IntItems)(      framehash_cell_t  *entrypoint,  framehash_dynamic_t *dynamic, Key64Value56List_t *output );
  int64_t               (*Process)(       framehash_cell_t  *entrypoint,  f_framehash_cell_processor_t cellfunc, void *input, void *output );
} IFramehashSimple_t;

#define SIMPLE_FRAMEHASH_LENGTH( TopCellPtr )     APTR_AS_INT64( TopCellPtr )
#define SIMPLE_FRAMEHASH_IS_EMPTY( TopCellPtr )   (SIMPLE_FRAMEHASH_LENGTH( TopCellPtr ) == 0)



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashMemory_t {
  framehash_slot_t * (*NewFrame)( framehash_context_t *context, const int order, const int domain, const framehash_ftype_t ftype );
  int64_t (*DiscardFrame)( framehash_context_t * const context );
  cxmalloc_family_t * (*DefaultFrameAllocator)( void );
  cxmalloc_family_t * (*DefaultBasementAllocator)( void );
  cxmalloc_family_t * (*NewFrameAllocator)( const char *description, size_t block_size );
  cxmalloc_family_t * (*NewBasementAllocator)( const char *description, size_t block_size );
  bool (*IsFrameValid)( const framehash_cell_t * const frameref );
} IFramehashMemory_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashProcessing_t {
  IStandardCellProcessors_t * StandardCellProcessors;
  IStandardSubtreeProcessors_t *StandardSubtreeProcessors;
  IMathCellProcessors_t * MathCellProcessors;
  int64_t (*ProcessNolockNocache)( framehash_processing_context_t *processor );
  int64_t (*ProcessNolock)( framehash_processing_context_t *processor );
} IFramehashProcessing_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashTest_t {
  char ** (*GetTestNames)( void );
  int (*RunUnitTests)( const char *runonly[], const char *testdir );
} IFramehashTest_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashDynamic_t {
  framehash_dynamic_t * (*InitDynamic)( framehash_dynamic_t *dynamic, const framehash_constructor_args_t *args );
  framehash_dynamic_t * (*InitDynamicSimple)( framehash_dynamic_t *dynamic, const char *name, int block_size_order );
  int                   (*ResetDynamicSimple)( framehash_dynamic_t *dynamic );
  void                  (*ClearDynamic)( framehash_dynamic_t *dynamic );
  void                  (*PrintAllocators)( framehash_dynamic_t *dynamic );
  int                   (*CheckAllocators)( framehash_dynamic_t *dynamic );
} IFramehashDynamic_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehashAccess_t {
  framehash_retcode_t (*SetContext)( framehash_context_t * const context );
  framehash_retcode_t (*DelContext)( framehash_context_t * const context );
  framehash_retcode_t (*GetContext)( framehash_context_t * const context );
  framehash_cell_t * (*TopCell)( const framehash_slot_t *slots );
  framehash_cell_t * (*UpdateTopCell)( framehash_cell_t *frameref );
  int (*Compactify)( framehash_context_t * const context );
  uint8_t (*DeleteItem)( framehash_cell_t *frame, framehash_cell_t *target );
  int64_t (*SubtreeLength)( framehash_cell_t *frame, framehash_dynamic_t *dynamic, int64_t limit );
} IFramehashAccess_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct s_IFramehash_t {
  IFramehashMemory_t memory;
  IFramehashProcessing_t processing;
  IFramehashDynamic_t dynamic;
  IFramehashAccess_t access;
  IFramehashSimple_t simple;
  const char * (*Version)( bool ext );
  CString_t * (*GetError)( void );
  IFramehashTest_t test;
} IFramehash_t;


DLL_HIDDEN extern IFramehashSimple_t iFramehashSimple;

DLL_FRAMEHASH_PUBLIC extern CString_t * FRAMEHASH_ROOT_ERROR_MESSAGE;


#ifndef _FORCE_FRAMEHASH_IMPORT
DLL_FRAMEHASH_PUBLIC extern IFramehash_t iFramehash;
DLL_FRAMEHASH_PUBLIC extern int framehash_INIT( void );
DLL_FRAMEHASH_PUBLIC extern int framehash_DESTROY( void );
#else
DLL_IMPORT extern IFramehash_t iFramehash;
DLL_IMPORT extern int framehash_INIT( void );
DLL_IMPORT extern int framehash_DESTROY( void );
#endif


#endif
