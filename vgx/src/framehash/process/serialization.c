/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    serialization.c
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

#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


// TODO: define invalid annotation properly
static framehash_cell_t g_dummy_chain = {.annotation=0xFFFFFFFFFFFFFFFFULL, .tptr={0}};




typedef struct __s_cellbuf3_t {
  // QWORD 1: key type and cell value type
  struct {
    _serialized_cellkey_t cellkey;
    _serialized_celltype_t celltype;
  } type;
  // QWORD 2: the key
  union {
    QWORD qword;
    QWORD plain;
    QWORD shortid;
  } key;
  // QWORD 3: the cell value or typeinfo for objects
  union {
    QWORD qw3;
    double real;
  };
} __cellbuf3_t;



typedef struct __s_serialized_prehead_t {
  QWORD begin_file[4];
  QWORD typeinfo;
  QWORD smode;
} __serialized_prehead_t;



typedef struct __s_serialized_header_t {
  /* CL 1 */
  QWORD begin_file[4];        // 4  File start marker
  QWORD typeinfo;             // 1  Registered COMLIB typeinfo for framehash objects
  QWORD smode;                // 1  Framehash serialization mode
  QWORD start_delim[2];       // 2  Delimiter
  /* CL 2 */
  objectid_t obid;            // 2  Internal object id
  QWORD format_version;       // 1  Framehash serialization format
  QWORD persist_t0;           // 1  Timestamp when serialization started (ms since 1970)
  QWORD persist_t1;           // 1  Timestamp when serialization completed (ms since 1970)
  QWORD persist_incomplete;   // 1  Non-zero when serialized data is not verified to be coherent and may require delta operations to be applied.
  QWORD __rsv2[2];            // 2  *future*
  /* CL 3 */
  QWORD nobj;                 // 1  Number of mapped objects in file
  QWORD opcnt;                // 1  Current operation count in file
  QWORD top_order;            // 1  Order of framehash top frame
  QWORD qwsz_path;            // 1  Number of whole qwords in filepath, including additional 0-qword if actual string size is a multiple of 8
  QWORD nqwords;              // 1  Total number of qwords in file (should match end struct)
  QWORD __rsv3[3];            // 3  *future*
  /* CL 4 */
  QWORD synch;                // 1  Non-zero if framehash instance uses internal synchronization for access
  QWORD shortkeys;            // 1  Non-zero if framehash instance allows 64-bit keys for objects
  QWORD cache_depth;          // 1  Last domain that is allowed to contain cache frames
  QWORD changelog_obclass;    // 1  Changelog enabled for this COMLIB object class (when other than CLASS_NONE)
  QWORD changelog_seq_start;  // 1  First changelog delta file to be applied in order to bring map up to date
  QWORD changelog_seq_end;    // 1  Next changelog delta file that will include new changes (one beyond last to apply to bring map up to date)
  QWORD changelog_nobj;       // 1  Number of mapped objects after changelog
  QWORD changelog_opcnt;      // 1  Operation count after changelog
} __serialized_header_t;


static const __serialized_header_t INIT_HEADER = {
  // 1
  .begin_file           = _SERIALIZATION_FILE_CAP,
  .typeinfo             = 0,
  .smode                = 0,
  .start_delim          = _SERIALIZATION_START,
  // 2
  .obid                 = {0},
  .format_version       = _FRAMEHASH_FORMAT_VERSION_1_1,
  .persist_t0           = 0,
  .persist_t1           = ULLONG_MAX,
  .persist_incomplete   = 1,
  .__rsv2               = {0},
  // 3
  .nobj                 = 0,
  .opcnt                = 0,
  .top_order            = 0,
  .qwsz_path            = 0,
  .nqwords              = 0,
  .__rsv3               = {0},
  // 4
  .synch                = 0,
  .shortkeys            = 0,
  .cache_depth          = 0,
  .changelog_obclass    = CLASS_NONE,
  .changelog_seq_start  = 0,
  .changelog_seq_end    = 0,
  .changelog_nobj       = 0,
  .changelog_opcnt      = 0
};


typedef struct __s_serialized_header_and_file_t {
  __serialized_header_t header;
  union {
    char filepath[ MAX_PATH ];
    QWORD qwfilepath[ MAX_PATH / sizeof(QWORD) ];
  };
} __serialized_header_and_file_t;


typedef struct __s_frame_orders_t {
  /* 16 qwords */
  QWORD __rsv[16-_FRAMEHASH_P_MAX-6];
  QWORD p_max;
  QWORD n_top;
  QWORD n_cache;
  QWORD n_internal;
  QWORD n_leaf[_FRAMEHASH_P_MAX+1];
  QWORD n_basement;
} __frame_orders_t;


static const __frame_orders_t INIT_FRAME_ORDERS = {
  .__rsv          = {0},
  .p_max          = _FRAMEHASH_P_MAX,
  .n_top          = 0,
  .n_cache        = 0,
  .n_internal     = 0,
  .n_leaf         = {0},
  .n_basement     = 0
};



typedef struct __s_framehash_end_t {
  /* CL 1 */
  QWORD end_delim[2];
  QWORD complete_ts;
  QWORD nqwords;
  QWORD end_file[4];
} __serialized_end_t;


static const __serialized_end_t INIT_END = {
  .end_delim      = _SERIALIZATION_END,
  .complete_ts    = 0,
  .nqwords        = 0,
  .end_file       = _SERIALIZATION_FILE_CAP
};


typedef struct __s_detached_child_t {
  QWORD typeinfo;
  QWORD smode;
  QWORD nobj;
  QWORD opcnt;
  QWORD qwsz_path;  // number of whole qwords in filepath, including additional 0-qword if actual string size is a multiple of 8
} __detached_child_t;


typedef int64_t (*__f_enqueue_t)( CQwordQueue_t *Q, const QWORD *data, int64_t nqwords );
typedef int64_t (*__f_dequeue_t)( CQwordQueue_t *Q, void **data, int64_t nqwords );
typedef void (*__f_unread_t)( CQwordQueue_t *Q, int64_t nqwords );

typedef struct __s_output_t {
  CQwordQueue_t *Q;
  __f_enqueue_t write;
  int64_t nqwords;
  QWORD prefix;
  int fileno;
} __output_t;

typedef int64_t (*__f_serializer_t)( const framehash_cell_t *self, __output_t *output );

typedef struct __s_input_t {
  CQwordQueue_t *Q;
  __f_dequeue_t read;
  __f_unread_t unread;
  int64_t nqwords;
  QWORD fmtver;
} __input_t;




#define __OUTPUT( OutputPtr, SrcPtr, QwordCount )                                           \
do {                                                                                        \
  (OutputPtr)->nqwords += QwordCount;                                                       \
  if( (OutputPtr)->write( (OutputPtr)->Q, (const QWORD*)SrcPtr, QwordCount ) != (int64_t)(QwordCount) ) { \
    return -1;                                                                              \
  }                                                                                         \
} WHILE_ZERO

#define __OUTPUT_THROWS( OutputPtr, SrcPtr, QwordCount, ErrCode )                           \
do {                                                                                        \
  (OutputPtr)->nqwords += QwordCount;                                                       \
  if( (OutputPtr)->write( (OutputPtr)->Q, (const QWORD*)SrcPtr, QwordCount ) != (int64_t)(QwordCount) ) { \
    THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                              \
  }                                                                                         \
} WHILE_ZERO

#define __INPUT( InputPtr, DestPtrPtr, QwordCount )                                           \
do {                                                                                          \
  if( (InputPtr)->read( (InputPtr)->Q, (void**)(DestPtrPtr), QwordCount ) != (int64_t)(QwordCount) ) {  \
    return NULL;                                                                              \
  }                                                                                           \
  else {                                                                                      \
    (InputPtr)->nqwords += QwordCount;                                                        \
  }                                                                                           \
} WHILE_ZERO

#define __INPUT_THROWS( InputPtr, DestPtrPtr, QwordCount, ErrCode )                           \
do {                                                                                          \
  if( (InputPtr)->read( (InputPtr)->Q, (void**)(DestPtrPtr), QwordCount ) != (int64_t)(QwordCount) ) {  \
    THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                                \
  }                                                                                           \
  else {                                                                                      \
    (InputPtr)->nqwords += QwordCount;                                                        \
  }                                                                                           \
} WHILE_ZERO

#define __INPUT_PEEK_THROWS( InputPtr, DestPtrPtr, QwordCount, ErrCode )                      \
do {                                                                                          \
  if( (InputPtr)->read( (InputPtr)->Q, (void**)(DestPtrPtr), QwordCount ) != (int64_t)(QwordCount) ) {  \
    THROW_ERROR( CXLIB_ERR_GENERAL, ErrCode );                                                \
  }                                                                                           \
  else {                                                                                      \
    (InputPtr)->unread( (InputPtr)->Q, QwordCount );                                          \
  }                                                                                           \
} WHILE_ZERO





typedef enum __e_serialization_mode_t {
  NO_OUTPUT       = 0x0000,
  FILE_START      = 0x1111,
  ROOT_PARENT     = 0x2222,
  DETACHED_CHILD  = 0x3333,
  EMBEDDED_CHILD  = 0x4444
} __serialization_mode_t;





static void __subframe_orders( const framehash_cell_t * const frame, __frame_orders_t * const counter );

static int64_t     __cell_serialize( const framehash_cell_t * const cell,     __output_t * const output );
static int64_t    __cache_serialize( const framehash_cell_t * const cache,    __output_t * const output );
static int64_t     __leaf_serialize( const framehash_cell_t * const leaf,     __output_t * const output );
static int64_t __internal_serialize( const framehash_cell_t * const internal, __output_t * const output );
static int64_t __basement_serialize( const framehash_cell_t * const basement, __output_t * const output );
static int64_t __subframe_serialize( const framehash_cell_t * const frame,    __output_t * const output );


static framehash_cell_t *     __cell_deserialize( framehash_cell_t * const cell, __input_t * const input );

typedef framehash_cell_t * (*__f_deserializer_t)( framehash_context_t * const context,          const framehash_metas_t * const metas,          __input_t * const input );
static framehash_cell_t *    __cache_deserialize( framehash_context_t * const cache_context,    const framehash_metas_t * const cache_metas,    __input_t * const input );
static framehash_cell_t *     __leaf_deserialize( framehash_context_t * const leaf_context,     const framehash_metas_t * const leaf_metas,     __input_t * const input );
static framehash_cell_t * __internal_deserialize( framehash_context_t * const internal_context, const framehash_metas_t * const internal_metas, __input_t * const input );
static framehash_cell_t * __basement_deserialize( framehash_context_t * const basement_context, const framehash_metas_t * const basement_metas, __input_t * const input );

static framehash_cell_t * __subframe_deserialize( framehash_context_t * const context, __input_t * const input );

static framehash_t * __inner_deserializing_constructor( __input_t * const input );



/*******************************************************************//**
 * __subframe_orders
 *
 ***********************************************************************
 */
static void __subframe_orders( const framehash_cell_t * const frame, __frame_orders_t * const counter ) {
  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS(frame);
  int p = frame_metas->order;
  framehash_slot_t *slot = CELL_GET_FRAME_SLOTS(frame);
  framehash_cell_t *chain;
  int nslots;

  if( _CELL_REFERENCE_IS_NULL(frame) ) return;

  switch( frame_metas->ftype ) {
  case FRAME_TYPE_CACHE:
    // increment cache count
    if( frame_metas->domain == 0 ) {
      counter->n_top++; // should be only one
    }
    else {
      counter->n_cache++;
    }
    // number of cache slots
    nslots = _CACHE_FRAME_NSLOTS( p );
    for( int fx=0; fx<nslots; fx++, slot++ ) {
      // last cell in cache slot is chain cell
      chain = &slot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
      __subframe_orders( chain, counter );
    }
    return;
  case FRAME_TYPE_LEAF:
    // increment leaf count for this order
    counter->n_leaf[ p ]++;
    return;
  case FRAME_TYPE_INTERNAL:
    // increment internal count
    counter->n_internal++;
    // number of chainslots
    nslots = _FRAME_NCHAINSLOTS( p );
    // first slot in chainzone - advance to slot by adding frame index
    slot += _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), 0 );
    for( int q=0; q<nslots; q++, slot++ ) {
      // scan all chainslots
      if( _framehash_radix__is_chain( frame_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
        // slot is a chain, follow it
        chain = slot->cells;
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, chain++ ) {
          __subframe_orders( chain, counter );
        }
      }
    }
    return;
  case FRAME_TYPE_BASEMENT:
    // increment basement count
    counter->n_basement++;
    if( frame_metas->hasnext == true ) {
      chain = _framehash_basementallocator__get_chain_cell( frame );
      __subframe_orders( chain, counter );
    }
    return;
  }
}



/*******************************************************************//**
 * __object_serialize
 *
 *    OBJECT CELL:
 *     
 *    | cell header info         | object data                                                                                             |
 *    |                          |                |                |                                             |                |        |
 *    |__________________________|________________|________________|_____________________________________________|________________|________|
 *    |   8    |   8    |   8    |       16       |       16       |                                             |       16       |   8    |
 *    | otype  | shortid| typeinf|  OBJECT START  |    OBJECTID    |  ... object specific serialized bytes ...   |   OBJECT END   | nqwords|
 *    |________|________|________|________________|________________|_____________________________________________|________________|________|
 *
 *                                                                 \---------------------------------------------/
 *                                                                                         ^
 *                                                                                         |
 *                                                                               this part is handled
 *                                                                          in the implementation specific            
 *                                                                                 object serializer
 *
 *
 *  NOTE ABOUT SERIALIZED INNER FRAMEHASH OBJECTS WITH SEPARATE FILE STORAGE
 *  ------------------------------------------------------------------------
 *  So-called DETACHED CHILD framehash objects have their own file storage
 *  and are represented in the parent in a special serialization format
 *  referencing the child by filepath and a few other parameters to ensure
 *  consistency.
 *
 *    DETACHED CHILD CELL:
 *
 *    | cell header info         | object data                                                                                                                   |
 *    |                          |                |                | detached child serialization (reference to file)                  |                         |
 *    |__________________________|________________|________________|___________________________________________________________________|_________________________|
 *    |   8    |   8    |   8    |       16       |       16       |   8    |   8    |   8    |   8    |   8    |                      |       16       |        |
 *    | otype  | shortid| typeinf|  OBJECT START  |    OBJECTID    | typeinf| detchld|  nobj  | opcnt  | qwszfn |   ... filepath ...   |   OBJECT END   | nqwords|
 *    |________|________|________|________________|________________|________|________|________|________|________|______________________|________________|________|
 *
 *
 *
 *
 *
 *
 *
 *
 ***********************************************************************
 */
__inline static int __object_serialize( const framehash_cell_t * const cell, __output_t * const output, _serialized_cellkey_t ktype, _serialized_celltype_t vtype ) {
  static const QWORD begin_object[] = _SERIALIZATION_BEGIN_OBJECT;
  static const QWORD end_object[] = _SERIALIZATION_END_OBJECT;
  int64_t obj_nqwords;
  comlib_object_t *obj = vtype == SERIALIZED_CELL_TYPE_OBJECT128 ? _CELL_GET_OBJECT128(cell) : _CELL_GET_OBJECT64(cell);
  objectid_t *pobid = COMLIB_OBJECT_GETID( obj );
  
  __cellbuf3_t cellbuf = {
    .type = {
      .cellkey = ktype,
      .celltype = vtype
    },
    .key = {
      .qword=_CELL_KEY(cell)
    },
    .qw3 = COMLIB_OBJECT_TYPEINFO_QWORD( obj )
  };

  // | TYPE | KEY | TYPEINF |
  __OUTPUT( output, &cellbuf, qwsizeof(__cellbuf3_t) );

  // BEGIN
  __OUTPUT( output, begin_object, qwsizeof(begin_object) );

  // OBID
  __OUTPUT( output, pobid, qwsizeof(objectid_t) );

  // |...object data...|
  if( (obj_nqwords = COMLIB_OBJECT_SERIALIZE( obj, output->Q )) < 0 ) {
    return -1;
  }
  else {
    output->nqwords += obj_nqwords;
  }

  // END
  __OUTPUT( output, end_object, qwsizeof(end_object) );

  // qwords
  __OUTPUT( output, &output->nqwords, 1 );

  return 0; // ok
}



/*******************************************************************//**
 * __cell_serialize
 *    ___________________________
 *    |   8    |   8    |   8    |
 *    | type   |  key   |  val   |
 *    |________|________|________|
 * 
 * 
 *    OBJECT CELL:  see __object_serialize()
 *
 ***********************************************************************
 */
static int64_t __cell_serialize( const framehash_cell_t * const cell, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;

  if( _ITEM_IS_DIRTY( cell ) ) {
    return -1; // TODO: For now we disallow dirty items - flush must have occurred before serialization.
  }

  if( _ITEM_IS_VALID( cell ) ) {
    // valid item
    if( _CELL_IS_REFERENCE( cell ) ) {
      // OBJECT 128
      if( __object_serialize( cell, output, SERIALIZED_CELL_KEY_HASH128, SERIALIZED_CELL_TYPE_OBJECT128 ) < 0 ) {
        return -1;
      }
    }
    else {
      __cellbuf3_t cellbuf = { .key={ .qword=_CELL_KEY(cell) } };

      // KEY
      switch( _CELL_VALUE_DKEY( cell ) ) {
      case TAGGED_DKEY_PLAIN64:
        cellbuf.type.cellkey = SERIALIZED_CELL_KEY_PLAIN64;
        break;
      case TAGGED_DKEY_HASH64:
        cellbuf.type.cellkey = SERIALIZED_CELL_KEY_HASH64;
        break;
      case TAGGED_DKEY_HASH128:
        cellbuf.type.cellkey = SERIALIZED_CELL_KEY_HASH128;
        break;
      default:
        cellbuf.type.cellkey = SERIALIZED_CELL_KEY_ERROR;
        break;
      }

      // VALUE
      tagged_dtype_t dtype = _CELL_VALUE_DTYPE( cell );
      if( dtype == TAGGED_DTYPE_OBJ56 ) {
        // OBJECT 64
        if( __object_serialize( cell, output, cellbuf.type.cellkey, SERIALIZED_CELL_TYPE_OBJECT64 ) < 0 ) {
          return -1;
        }
      }
      else {
        switch( dtype ) {
        case TAGGED_DTYPE_ID56:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_MEMBER;
          cellbuf.qw3 = _CELL_GET_IDHIGH( cell );
          break;
        case TAGGED_DTYPE_BOOL:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_BOOLEAN;
          cellbuf.qw3 = _CELL_AS_UNSIGNED( cell );
          break;
        case TAGGED_DTYPE_UINT56:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_UNSIGNED;
          cellbuf.qw3 = _CELL_AS_UNSIGNED( cell );
          break;
        case TAGGED_DTYPE_INT56:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_INTEGER;
          cellbuf.qw3 = _CELL_AS_INTEGER( cell );
          break;
        case TAGGED_DTYPE_REAL56:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_REAL;
          cellbuf.real = _CELL_GET_REAL( cell );
          break;
        case TAGGED_DTYPE_PTR56:
          REASON( 0x600, "Framehash unserializable: raw pointers cannot be serialized!" );
          return -1; // It is illegal to serialize framehash containing 
        default:
          cellbuf.type.celltype = SERIALIZED_CELL_TYPE_ERROR;
          cellbuf.qw3 = 0;
        }
        // |TYPE|SHORTID|VALUE|
        __OUTPUT( output, &cellbuf, qwsizeof(cellbuf) );
      }
    }
  }
  else {
    __cellbuf3_t cellbuf = {
      .type = {
        .cellkey  = SERIALIZED_CELL_KEY_NONE,
        .celltype = _CELL_IS_END( cell ) ? SERIALIZED_CELL_TYPE_END : _CELL_IS_EMPTY( cell ) ? SERIALIZED_CELL_TYPE_EMPTY : SERIALIZED_CELL_TYPE_ERROR
      },
      .key = {
        .qword = _CELL_KEY(cell)
      },
      .qw3 = 0
    };
    if( cellbuf.type.celltype == SERIALIZED_CELL_TYPE_ERROR ) {
      return -1;
    }
    // |TYPE|KEY|0|
    __OUTPUT( output, &cellbuf, qwsizeof(cellbuf) );
  }

  return output->nqwords;
}



/*******************************************************************//**
 * __object_deserialize
 *
 ***********************************************************************
 */
__inline static comlib_object_t * __object_deserialize( comlib_object_typeinfo_t typeinfo,  __input_t * const input, const objectid_t **ppobid ) {
  static const QWORD begin_object[] = _SERIALIZATION_BEGIN_OBJECT;
  static const QWORD end_object[] = _SERIALIZATION_END_OBJECT;
  QWORD beginbuf[qwsizeof(begin_object)], *pbeginbuf = beginbuf;
  QWORD endbuf[qwsizeof(end_object)], *pendbuf = endbuf;
  QWORD nqwords, *p_nqwords=&nqwords;

  comlib_object_t *obj = NULL;

  XTRY {
    // BEGIN
    __INPUT_THROWS( input, &pbeginbuf, qwsizeof(begin_object), 0x601 );
    SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY
    if( memcmp( beginbuf, begin_object, sizeof(begin_object) ) ) {
      // bad object start marker
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x602 );
    }

    // OBID 
    if( input->fmtver >= _FRAMEHASH_FORMAT_VERSION_1_1 ) {
      __INPUT_THROWS( input, ppobid, qwsizeof(objectid_t), 0x603 );
    }
    else {
      *ppobid = NULL;
    }

    // OBJECT DATA (null pointer ok, caller will handle)
    obj = COMLIB_DeserializeObject( NULL, &typeinfo, input->Q );

    // END
    __INPUT_THROWS( input, &pendbuf, qwsizeof(end_object), 0x604 );
    SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY
    if( memcmp( endbuf, end_object, sizeof(end_object) ) ) {
      // bad checkpoint
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x605 );
    }

    // qwords, ok!
    __INPUT_THROWS( input, &p_nqwords, 1, 0x606 );
  }
  XCATCH( errcode ) {
    if( obj ) {
      COMLIB_OBJECT_DESTROY( obj );
    }
    // error
    *ppobid = NULL;
  }
  XFINALLY {
  }

  return obj;
}



/*******************************************************************//**
 * __cell_deserialize
 *
 ***********************************************************************
 */
static framehash_cell_t * __cell_deserialize( framehash_cell_t * const cell, __input_t * const input ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  framehash_cell_t *retcell = NULL;
  __cellbuf3_t cellbuf, *pcellbuf = &cellbuf;
  comlib_object_typeinfo_t typeinfo;
  framehash_context_t context = _CONTEXT_INIT_CELL_DESERIALIZE;
  objectid_t obid;

  XTRY {
    // INPUT => |TYPE|KEY|QW3|
    __INPUT_THROWS( input, &pcellbuf, qwsizeof(__cellbuf3_t), 0x611 );

    // Populate context key
    switch( cellbuf.type.cellkey ) {
    case SERIALIZED_CELL_KEY_NONE:
      context.ktype = CELL_KEY_TYPE_NONE;
      context.key.plain = FRAMEHASH_KEY_NONE;
      context.key.shortid = 0;
      break;
    case SERIALIZED_CELL_KEY_PLAIN64:
      context.ktype = CELL_KEY_TYPE_PLAIN64;
      context.key.plain = cellbuf.key.qword;
      context.key.shortid = 0;
      break;
    case SERIALIZED_CELL_KEY_HASH64:
      context.ktype = CELL_KEY_TYPE_HASH64;
      context.key.plain = FRAMEHASH_KEY_NONE;
      context.key.shortid = cellbuf.key.qword;
      break;
    case SERIALIZED_CELL_KEY_HASH128:
      context.ktype = CELL_KEY_TYPE_HASH128;
      context.key.plain = FRAMEHASH_KEY_NONE;
      context.key.shortid = cellbuf.key.qword;
      break;
    default:
      context.ktype = CELL_KEY_TYPE_ERROR;
      context.key.plain = FRAMEHASH_KEY_NONE;
      context.key.shortid = 0;
      break;
    }

    // Populate cell
    switch( cellbuf.type.celltype ) {
    case SERIALIZED_CELL_TYPE_END:
      // QW3 is ignored - should be 0
      _SET_CELL_FROM_ELEMENTS( cell, 0, CELL_TYPE_END );
      break;

    case SERIALIZED_CELL_TYPE_OBJECT128:
      if( context.ktype == CELL_KEY_TYPE_HASH128 ) {
        // QW3 is TYPEINFO
        typeinfo.qword = cellbuf.qw3;
        context.obid = &obid;
        if( (context.value.pobject = __object_deserialize( typeinfo, input, &context.obid )) != NULL ) {
          _STORE_OBJECT128_POINTER( &context, cell );
        }
#ifdef FRAMEHASH_CHANGELOG
        // Special case: A referenced object could not be deserialized because it does not exist.
        //               Replace the object with "key exists".
        else if( context.obid ) {
          context.value.idH56 = context.obid->H;
          _STORE_MEMBERSHIP( &context, cell );
        }
#endif
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x612 );
        }
      }
      // invalid key type for this object
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x613 );
      }
      break;

    case SERIALIZED_CELL_TYPE_OBJECT64:
      if( context.ktype != CELL_KEY_TYPE_HASH128 ) {
        // QW3 is TYPEINFO
        typeinfo.qword = cellbuf.qw3;
        context.obid = &obid;
        if( (context.value.pobject = __object_deserialize( typeinfo, input, &context.obid )) != NULL ) {
          _STORE_OBJECT64_POINTER( &context, cell );
        }
        // Special case: A referenced object could not be deserialized because it does not exist
        //               Replace the object with "key exists"
        else if( context.obid ) {
          context.value.idH56 = context.obid->H;
          _STORE_MEMBERSHIP( &context, cell );
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x614 );
        }
      }
      // invalid key type for this object
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x615 );
      }
      break;

    case SERIALIZED_CELL_TYPE_EMPTY:
      // QW3 is ignored
      _DELETE_ITEM( cell );
      break;

    case SERIALIZED_CELL_TYPE_BOOLEAN:
      // QW3 is 1 or 0
      context.value.qword = cellbuf.qw3;
      _STORE_BOOLEAN( &context, cell );
      break;

    case SERIALIZED_CELL_TYPE_MEMBER:
      // QW3 is MEMBERSHIP
      context.value.qword = cellbuf.qw3;
      _STORE_MEMBERSHIP( &context, cell );
      break;

    case SERIALIZED_CELL_TYPE_UNSIGNED:
      // QW3 is UNSIGNED
      context.value.qword = cellbuf.qw3;
      _STORE_UNSIGNED( &context, cell );
      break;

    case SERIALIZED_CELL_TYPE_INTEGER:
      // QW3 is INTEGER
      context.value.qword = cellbuf.qw3;
      _STORE_INTEGER( &context, cell );
      break;

    case SERIALIZED_CELL_TYPE_REAL:
      // QW3 is REAL
      context.value.qword = cellbuf.qw3;
      _STORE_REAL( &context, cell );
      break;

    case SERIALIZED_CELL_TYPE_CHAIN:
      // QW3 is ignored
      // WARNING: Temporary reference, do not use as is!
      _SET_CELL_REFERENCE( cell, &g_dummy_chain ); // <= NOTE: this is a temporary placeholder
      break;

    default:
      CALLABLE( input->Q )->UnreadNolock( input->Q, -1 );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x616 );
    }

    retcell = cell;
  }
  XCATCH( errcode ) {
    retcell = NULL;
  }
  XFINALLY {
  }
  return retcell;
}



/*******************************************************************//**
 * __cache_serialize
 *
 *   _____________________________________________
 *  |        ...        |                         |
 *  |    dummy cells    |    ... subframes ...    |
 *  |___________________|_________________________|
 *   \-----------------/ \-----------------------/ 
 *            ^             handled by recursion 
 *            |
 *        cells are:
 *            |
 *  __________________________
 * |   8    |   8    |   8    |
 * |  TYPE* |   0    |   0    |
 * |________|________|________|
 *   *TYPE is END, CHAIN or EMPTY
 *
 ***********************************************************************
 */
static int64_t __cache_serialize( const framehash_cell_t * const cache, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  const int p = CELL_GET_FRAME_METAS( cache )->order;
  framehash_slot_t *chainslot = NULL;
  framehash_cell_t *chain = NULL;
  const int nchainslots = _CACHE_FRAME_NSLOTS( p );
  const __cellbuf3_t cellbuf_end =   { .type={.cellkey=SERIALIZED_CELL_KEY_NONE, .celltype=SERIALIZED_CELL_TYPE_END},   .key={0}, .qw3=0 };
  const __cellbuf3_t cellbuf_chain = { .type={.cellkey=SERIALIZED_CELL_KEY_NONE, .celltype=SERIALIZED_CELL_TYPE_CHAIN}, .key={0}, .qw3=0 };
  const __cellbuf3_t cellbuf_empty = { .type={.cellkey=SERIALIZED_CELL_KEY_NONE, .celltype=SERIALIZED_CELL_TYPE_EMPTY}, .key={0}, .qw3=0 };
  QWORD this_prefix = output->prefix;

  // =========================================
  // STEP 1: Write the cache frame placeholder
  // =========================================

  chainslot = CELL_GET_FRAME_SLOTS( cache );
  for( int fx=0; fx<nchainslots; fx++, chainslot++ ) {
    // cache cells are written as EMPTY
    for( int j=0; j<_FRAMEHASH_CACHE_CHAIN_CELL_J; j++ ) {
      __OUTPUT( output, &cellbuf_empty, qwsizeof(cellbuf_empty) );
    }
    // chain cell is written as CHAIN or END
    if( _CELL_REFERENCE_EXISTS( &chainslot->cells[_FRAMEHASH_CACHE_CHAIN_CELL_J] ) ) {
      __OUTPUT( output, &cellbuf_chain, qwsizeof(cellbuf_chain) );
    }
    else {
      __OUTPUT( output, &cellbuf_end, qwsizeof(cellbuf_end) );
    }
  }

  // =======================
  // STEP 2: Traverse chains
  // =======================

  chainslot = CELL_GET_FRAME_SLOTS( cache );
  for( int fx=0; fx<nchainslots; fx++, chainslot++ ) {
    chain = &chainslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
    if( _CELL_REFERENCE_EXISTS( chain ) ) {
      output->prefix = (this_prefix << _CACHE_FRAME_INDEX_BITS( p )) | fx; // compute the prefix for the lower level
      if( __subframe_serialize( chain, output ) < 0 ) {
        return -1;
      }
    }
  }
  output->prefix = this_prefix;

  return output->nqwords;
}



/*******************************************************************//**
 * __cache_deserialize
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * __cache_deserialize( framehash_context_t * const cache_context, const framehash_metas_t * const cache_metas, __input_t * const input ) {
  framehash_cell_t *cache = NULL;

  XTRY {
    framehash_cell_t *cell = NULL;
    framehash_slot_t *chainslot = NULL;
    framehash_cell_t *chain = NULL;
    const int p = cache_metas->order;
    framehash_cell_t *end = NULL;
    const int nchainslots = _CACHE_FRAME_NSLOTS( p );

    // Allocate new cache frame
    if( _framehash_memory__new_frame( cache_context, p, cache_metas->domain, cache_metas->ftype ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x621 );
    }
    cache = cache_context->frame;
    CELL_GET_FRAME_METAS( cache )->nactive = cache_metas->nactive;
    CELL_GET_FRAME_METAS( cache )->nchains = cache_metas->nchains;

    // INPUT => populate cache frame cells with placeholders
    cell = CELL_GET_FRAME_CELLS( cache );
    end = cell + _CACHE_FRAME_NSLOTS( p ) * (int)FRAMEHASH_CELLS_PER_SLOT;
    while( cell < end ) {
      if( __cell_deserialize( cell++, input ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x622 );
      }
    }

    // Traverse to populate frame pointers
    chainslot = CELL_GET_FRAME_SLOTS( cache );
    for( int fx=0; fx<nchainslots; fx++, chainslot++ ) {
      chain = &chainslot->cells[ _FRAMEHASH_CACHE_CHAIN_CELL_J ];
      if( CELL_GET_FRAME_CELLS( chain ) == &g_dummy_chain ) { // <= as loaded by __cell_deserialize() above
        // a chain exists, now deserialize it and hook up
        framehash_cell_t *sub;
        _PUSH_CONTEXT_FRAME( cache_context, chain ) {
          sub = __subframe_deserialize( cache_context, input );
        } _POP_CONTEXT_FRAME;
        if( sub == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x623 );
        }
      }
      else {
        // no chain exists
        _SET_CELL_FROM_ELEMENTS( chain, 0, CELL_TYPE_END );
      }
    }

  }
  XCATCH( errcode ) {
    if( cache ) {
      _framehash_memory__discard_frame( cache_context );
      cache = NULL;
    }
  }
  XFINALLY {
  }

  return cache;
}




/*******************************************************************//**
 * __leaf_serialize
 *
 *   _________________________________________
 *  |       48        |          ...          |
 *  | halfslot cells  |     zone  cells       |
 *  |_________________|_______________________|
 *
 *
 ***********************************************************************
 */
static int64_t __leaf_serialize( const framehash_cell_t * const leaf, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  const int p = CELL_GET_FRAME_METAS( leaf )->order;
  const framehash_halfslot_t *halfslot = _framehash_frameallocator__header_half_slot( (framehash_cell_t*)leaf );
  framehash_cell_t *cell = CELL_GET_FRAME_CELLS( leaf );
  const framehash_cell_t *end = cell + _FRAME_NSLOTS( p ) * (int)FRAMEHASH_CELLS_PER_SLOT;


  
  // header cells
  for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
    if( __cell_serialize( &halfslot->cells[j], output ) < 0 ) {
      return -1;
    }
  }

  // zone cells
  while( cell < end ) {
    if( __cell_serialize( cell++, output ) < 0 ) {
      return -1;
    }
  }

  return output->nqwords;
}



/*******************************************************************//**
 * __leaf_deserialize
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * __leaf_deserialize( framehash_context_t * const leaf_context, const framehash_metas_t * const leaf_metas, __input_t * const input ) {
  const int p = leaf_metas->order;
  framehash_cell_t *leaf = NULL;

  XTRY {
    framehash_cell_t *cell = NULL;
    framehash_cell_t *end = NULL;
    framehash_halfslot_t *halfslot;

    // New leaf
    if( _framehash_memory__new_frame( leaf_context, p, leaf_metas->domain, leaf_metas->ftype ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x631 );
    }
    leaf = leaf_context->frame;
    CELL_GET_FRAME_METAS( leaf )->nactive = leaf_metas->nactive;

    // INPUT => populate leaf frame cells
    // header's halfslot cells
    halfslot = _framehash_frameallocator__header_half_slot( leaf );
    for( int j=0; j<FRAMEHASH_CELLS_PER_HALFSLOT; j++ ) {
      if( __cell_deserialize( &halfslot->cells[j], input ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x632 );
      }
    }
    // zone cells
    cell = CELL_GET_FRAME_CELLS( leaf );
    end = cell + _FRAME_NSLOTS( p ) * (int)FRAMEHASH_CELLS_PER_SLOT;
    while( cell < end ) {
      if( __cell_deserialize( cell++, input ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x633 );
      }
    }

  }
  XCATCH( errcode ) {
    if( leaf ) {
      _framehash_memory__discard_frame( leaf_context );
      leaf = NULL;
    }
  }
  XFINALLY {}

  return leaf;
}



/*******************************************************************//**
 * __internal_serialize
 *
 *
 * | internal frame cell data                                                            | all subframe data    |
 * |_____________________________________________________________________________________|______________________|
 * |                ...                |           ...         |           ...           |          ...         |
 * |          large zone cells         |    chain zone cells   |  remaining zone cells   |       subframes      |
 * |___________________________________|_______________________|_________________________|______________________|
 *                                      \---------------------/                           \--------------------/ 
 *                                                  ^                                      handled by recursion 
 *                                                  |
 *                                       cells in chain slots are:
 *                                                  |
 *                                       __________________________
 *                                      |   8    |   8    |   8    |
 *                                      |  CHAIN |   0    |   0    |
 *                                      |________|________|________|
 *
 *
 *                                       *This zone may also contain
 *                                        non-chain slots and cells
 *                                        in those slots are normal
 *                                        LEAF cells.
 *                     
 ***********************************************************************
 */
static int64_t __internal_serialize( const framehash_cell_t * const internal, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  framehash_metas_t *internal_metas = CELL_GET_FRAME_METAS( internal );
  const int p = internal_metas->order;
  framehash_slot_t *slot = NULL;
  framehash_cell_t *cell = CELL_GET_FRAME_CELLS( internal );
  framehash_cell_t *chain = NULL;
  const int nchainslots = _FRAME_NCHAINSLOTS( p );
  const __cellbuf3_t cellbuf_chain = { .type={.cellkey=SERIALIZED_CELL_KEY_NONE, .celltype=SERIALIZED_CELL_TYPE_CHAIN}, .key={0}, .qw3=0 };
  QWORD this_prefix = output->prefix;


  // ==================================================
  // STEP 1: Write the internal frame with placeholders
  // ==================================================

  for( int k=p-1; k >= 0; k-- ) {
    if( k == _FRAME_CHAINZONE(p) ) {
      // chainzone - special treatment for chain slots
      for( int q=0; q<nchainslots; q++ ) {
        if( _framehash_radix__is_chain( internal_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
          // chainslot - special
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
            __OUTPUT( output, &cellbuf_chain, qwsizeof(cellbuf_chain) );
          }
        }
        else {
          // leafslot - normal 
          for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
            if( __cell_serialize( cell, output ) < 0 ) {
              return -1;
            }
          }
        }
      }
    }
    else {
      // leafzone - normal
      for( int q=0; q < (1<<k); q++ ) {
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, cell++ ) {
          if( __cell_serialize( cell, output ) < 0 ) {
            return -1;
          }
        }
      }
    }
    // NOTE: header cells not used for internal frames
  }

  // =======================
  // STEP 2: Traverse chains
  // =======================

  slot = CELL_GET_FRAME_SLOTS( internal ) + _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), 0 );
  for( int q=0; q<nchainslots; q++, slot++ ) {
    if( _framehash_radix__is_chain( internal_metas, _SLOT_Q_GET_CHAININDEX(q) ) ) {
      chain = slot->cells;
      for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, chain++ ) {
        output->prefix = (this_prefix << _FRAME_INDEX_BITS( p )) | (q*FRAMEHASH_CELLS_PER_SLOT + j) ; // compute the prefix for the lower level
        if( __subframe_serialize( chain, output ) < 0 ) {
          return -1;
        }
      }
    }
  }
  output->prefix = this_prefix;

  return output->nqwords;
}



/*******************************************************************//**
 * __internal_deserialize
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * __internal_deserialize( framehash_context_t * const internal_context, const framehash_metas_t * const internal_metas, __input_t * const input ) {
  framehash_cell_t *internal = NULL;

  XTRY {
    framehash_cell_t *cell = NULL;
    framehash_cell_t *chain = NULL;
    const int p = internal_metas->order;
    framehash_cell_t *end = NULL;
    framehash_slot_t *slot = NULL;
    const int nchainslots = _FRAME_NCHAINSLOTS( p );

    // New frame
    if( _framehash_memory__new_frame( internal_context, p, internal_metas->domain, internal_metas->ftype ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x641 );
    }
    internal = internal_context->frame;
    CELL_GET_FRAME_METAS( internal )->nactive = internal_metas->nactive;
    CELL_GET_FRAME_METAS( internal )->chainbits = internal_metas->chainbits;

    // INPUT => populate internal frame cells
    cell = CELL_GET_FRAME_CELLS( internal );
    end = cell + _FRAME_NSLOTS( p ) * (int)FRAMEHASH_CELLS_PER_SLOT;
    while( cell < end ) {
      if( __cell_deserialize( cell++, input ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x642 );
      }
    }

    // Traverse to populate frame pointers
    slot = CELL_GET_FRAME_SLOTS( internal ) + _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), 0 );
    for( int q=0; q<nchainslots; q++, slot++ ) {
      if( _framehash_radix__is_chain( CELL_GET_FRAME_METAS(internal), _SLOT_Q_GET_CHAININDEX(q) ) ) {
        chain = slot->cells;
        for( int j=0; j<FRAMEHASH_CELLS_PER_SLOT; j++, chain++ ) {
          // chain reference should be initialized to dummy in __cell_deserialize() above
          if( CELL_GET_FRAME_CELLS( chain ) != &g_dummy_chain ) {
            THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x643 );
          }
          // replace chain reference with a proper subtree
          framehash_cell_t *sub;
          _PUSH_CONTEXT_FRAME( internal_context, chain ) {
            sub = __subframe_deserialize( internal_context, input );
          } _POP_CONTEXT_FRAME;
          if( sub == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x644 );
          }
        }
      }
    }

  }
  XCATCH( errcode ) {
    if( internal ) {
      _framehash_memory__discard_frame( internal_context );
      internal = NULL;
    }
  }
  XFINALLY {
  }

  return internal;
}



/*******************************************************************//**
 * __basement_serialize
 *
 *
 * | basement cell data                | all sub-basement data |
 * |___________________________________|_______________________|
 * |                144                |                       |
 * |              6 cells              |       subframes       |
 * |___________________________________|_______________________|
 *                                      \---------------------/ 
 *                                       handled by recursion 
 *
 ***********************************************************************
 */
static int64_t __basement_serialize( const framehash_cell_t * const basement, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  framehash_cell_t *cell = CELL_GET_FRAME_CELLS( basement );
  framehash_metas_t *basement_metas = CELL_GET_FRAME_METAS( basement );
  framehash_cell_t *next_basement = _framehash_basementallocator__get_chain_cell( basement ); // may be NULL

  // ==========================
  // STEP 1: Write the basement
  // ==========================
  // cells => FRAME
  for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++, cell++ ) {
    if( __cell_serialize( cell, output ) < 0 ) {
      return -1;
    }
  }

  // ==================================
  // STEP 2: Descend into next basement
  // ==================================

  if( basement_metas->hasnext == true ) {
    if( __subframe_serialize( next_basement, output ) < 0 ) {
      return -1;
    }
  }

  return output->nqwords;
}



/*******************************************************************//**
 * __basement_deserialize
 *
 *
 ***********************************************************************
 */
static framehash_cell_t * __basement_deserialize( framehash_context_t * const basement_context, const framehash_metas_t * const basement_metas, __input_t * const input ) {
  framehash_cell_t *basement = NULL;

  XTRY {
    framehash_cell_t *cell = NULL;

    // New frame
    if( _framehash_memory__new_basement( basement_context, basement_metas->domain ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x651 );
    }
    basement = basement_context->frame;
    CELL_GET_FRAME_METAS( basement )->nactive = basement_metas->nactive;
    CELL_GET_FRAME_METAS( basement )->hasnext = basement_metas->hasnext;

    // INPUT => populate basement cells
    cell = CELL_GET_FRAME_CELLS( basement );
    for( int j=0; j<_FRAMEHASH_BASEMENT_SIZE; j++, cell++ ) {
      if( __cell_deserialize( cell, input ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x652 );
      }
    }

    // Continue down to next basement if we have one
    if( CELL_GET_FRAME_METAS( basement )->hasnext == true ) {
      framehash_cell_t *sub;
      _PUSH_CONTEXT_FRAME( basement_context, _framehash_basementallocator__get_chain_cell( basement ) ) {
        sub = __subframe_deserialize( basement_context, input );
      } _POP_CONTEXT_FRAME;
      if( sub == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x653 );
      }
    }

  }
  XCATCH( errcode ) {
    if( basement ) {
      _framehash_memory__discard_basement( basement_context );
      basement = NULL;
    }
  }
  XFINALLY {
  }

  return basement;
}



/*******************************************************************//**
 * __subframe_serialize
 *
 * | common frame start                        | frame specific data  | frame end check |
 * |___________________________________________|______________________|_________________|
 * |       16       |   8    |   8    |   8    |                      |   8    |   8    |
 * |  FRAME DELIM   | typmark| metas  | prefix |   ...frame data...   | chkpnt | nqwords|
 * |________________|________|________|________|______________________|________|________|
 *                                              \--------------------/ \---------------/
 *                                               handled by recursion      checkpoint
 *
 *
 ***********************************************************************
 */
static int64_t __subframe_serialize( const framehash_cell_t * const frame, __output_t * const output ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  static const QWORD delim[] = _SERIALIZATION_FRAMEDELIM;

  framehash_metas_t *frame_metas = CELL_GET_FRAME_METAS(frame);
  __f_serializer_t serialize_frame = NULL;
  QWORD marker = 0;
  CQwordQueue_t *Q = output->Q;

  XTRY {
    // FRAME DELIMITER
    __OUTPUT_THROWS( output, delim, qwsizeof(delim), 0x661 );

    // frame type specific actions
    switch( frame_metas->ftype ) {
    case FRAME_TYPE_CACHE:
      serialize_frame = __cache_serialize;
      marker = _SERIALIZATION_CACHE_FRAME;
      break;
    case FRAME_TYPE_LEAF:
      serialize_frame = __leaf_serialize;
      marker = _SERIALIZATION_LEAF_FRAME;
      break;
    case FRAME_TYPE_INTERNAL:
      serialize_frame = __internal_serialize;
      marker = _SERIALIZATION_INTERNAL_FRAME;
      break;
    case FRAME_TYPE_BASEMENT:
      serialize_frame = __basement_serialize;
      marker = _SERIALIZATION_BASEMENT_FRAME;
      break;
    default:
      THROW_ERROR( CXLIB_ERR_BUG, 0x662 );
      break;
    }

    // FRAME TYPE MARKER
    __OUTPUT_THROWS( output, &marker, 1, 0x663 );

    // FRAME METAS
    __OUTPUT_THROWS( output, frame_metas, qwsizeof(framehash_metas_t), 0x664 );

    // PREFIX
    __OUTPUT( output, &output->prefix, 1 );

    // SERIALIZE FRAME
    if( serialize_frame( frame, output ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x665 );
    }

    // CHECKPOINT
    __OUTPUT_THROWS( output, &checkpoint, 1, 0x666 );
    __OUTPUT_THROWS( output, &output->nqwords, 1, 0x667 );

    // OUTPUT => FILE - this flushes the entire queue if we have an output file attached

    if( CALLABLE( Q )->HasOutputDescriptor( Q ) && CALLABLE( Q )->Length( Q ) > _SERIALIZATION_FLUSH_THRESHOLD ) {
      if( CALLABLE( Q )->FlushNolock( Q ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x668 );
      }
    }
  }
  XCATCH( errcode ) {
    output->nqwords = -1;
  }
  XFINALLY {}

  return output->nqwords;
}



/*******************************************************************//**
 * __subframe_deserialize
 *
 ***********************************************************************
 */
static framehash_cell_t * __subframe_deserialize( framehash_context_t * const context, __input_t * const input ) {
  static const QWORD checkpoint = _SERIALIZATION_CHECKPOINT;
  QWORD checkbuf, *pcheckbuf=&checkbuf;
  static const QWORD delim[] = _SERIALIZATION_FRAMEDELIM;

  framehash_cell_t *frame = NULL;


  XTRY {
    QWORD delim_buffer[qwsizeof(delim)];
    QWORD *p_delim_buffer = delim_buffer;
    QWORD nqwords, *p_nqwords=&nqwords;
    __f_deserializer_t deserialize_frame;
    QWORD marker = 0, *pmarker = &marker;
    QWORD prefix, *p_prefix=&prefix;

    // Expect: FRAME DELIMITER
    SUPPRESS_WARNING_USING_UNINITIALIZED_MEMORY
    __INPUT_THROWS( input, &p_delim_buffer, qwsizeof(delim), 0x671 );
    if( memcmp( delim_buffer, delim, sizeof(delim) ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x672 );
    }

    // Expect: FRAME TYPE MARKER
    __INPUT_THROWS( input, &pmarker, 1, 0x673 );

    switch( marker ) {
    case _SERIALIZATION_CACHE_FRAME:
      deserialize_frame = __cache_deserialize;
      break;
    case _SERIALIZATION_LEAF_FRAME:
      deserialize_frame = __leaf_deserialize;
      break;
    case _SERIALIZATION_INTERNAL_FRAME:
      deserialize_frame = __internal_deserialize;
      break;
    case _SERIALIZATION_BASEMENT_FRAME:
      deserialize_frame = __basement_deserialize;
      break;
    default:
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x674 );
    }

    // Expect: FRAME METAS
    framehash_metas_t target_metas;
    QWORD *p_metas = &target_metas.QWORD;
    __INPUT_THROWS( input, &p_metas, qwsizeof(framehash_metas_t), 0x675 );

    // Expect: PREFIX
    __INPUT_THROWS( input, &p_prefix, 1, 0x676 );

    // DESERIALIZE FRAME
    if( (frame = deserialize_frame( context, &target_metas, input )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x677 );
    }

    // CHECKPOINT
    __INPUT_THROWS( input, &pcheckbuf, 1, 0x678 );
    if( checkbuf != checkpoint ) {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x679 );
    }
    __INPUT_THROWS( input, &p_nqwords, 1, 0x67A );

  }
  XCATCH( errcode ) {
    if( frame ) {
      _framehash_memory__discard_frame( context );
      frame = NULL;
    }
  }
  XFINALLY {}

  return frame;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static framehash_t * __inner_deserializing_constructor( __input_t * const input ) {
  framehash_t *self = NULL;
  framehash_constructor_args_t fargs = FRAMEHASH_DEFAULT_ARGS;

  fargs.param.order               = FRAMEHASH_ARG_UNDEFINED;
  fargs.param.synchronized        = FRAMEHASH_ARG_UNDEFINED;
  fargs.param.shortkeys           = FRAMEHASH_ARG_UNDEFINED;
  fargs.param.cache_depth         = FRAMEHASH_ARG_UNDEFINED;
  // TODO: Fix the following LIMITATION:
  //    LIMITATION: Deserializing inner framehash instances from input queue
  //    will use the DEFAULT allocators, not the containing parent's allocators
  fargs.frame_allocator     = NULL; 
  fargs.basement_allocator  = NULL; 

  if( input == NULL ) {
    return NULL;
  }

  XTRY {
    const QWORD file_cap[] = _SERIALIZATION_FILE_CAP;

    struct { 
      __detached_child_t info;
      union {
        char filepath[ MAX_PATH ];
        QWORD qw_filepath[ MAX_PATH / 8 ];
      };
    } dchild, *p_dchild;
    p_dchild = &dchild;

    __serialized_prehead_t s_prehead = {0};
    __serialized_prehead_t *p_prehead = &s_prehead;
    __INPUT_PEEK_THROWS( input, &p_prehead, qwsizeof( __serialized_prehead_t ), 0x681 );

    // Verify start of data
    if( memcmp( s_prehead.begin_file, file_cap, sizeof(file_cap) ) != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x682, "bad file cap" );
    }
    // Expect framehash
    if( !COMLIB_TYPEINFO_QWORD_MATCH( COMLIB_CLASS_TYPEINFO( framehash_t ).qword, s_prehead.typeinfo ) ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x683, "expected framehash typecode" );
    }

    // Select mode
    switch( s_prehead.smode ) {
    case DETACHED_CHILD:
      // framehash reference needed
      {
        QWORD buf[4], *pbuf=buf;
        // begin file
        __INPUT_THROWS( input, &pbuf, qwsizeof( file_cap ), 0x684 );
        __INPUT_PEEK_THROWS( input, &p_dchild, qwsizeof( __detached_child_t ), 0x685 );
        __INPUT_THROWS( input, &p_dchild, qwsizeof( __detached_child_t ) + dchild.info.qwsz_path, 0x686 );
        // end file
        __INPUT_THROWS( input, &pbuf, qwsizeof( file_cap ), 0x687 );
        fargs.fpath = dchild.filepath;
      }
      break;
    case EMBEDDED_CHILD:
      // inner framehash comes next in input queue
      fargs.input_queue = input->Q;
      break;
    default:
      // only children possible, anything else is illegal
      THROW_ERROR( CXLIB_ERR_BUG, 0x688 );
    }

    // Construct new framehash from input (file or queue)
    if( (self = COMLIB_OBJECT_NEW( framehash_t, NULL, &fargs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x689 );
    }

    // 6. Validate detached child since it came from another file that could be out of synch with the parent
    if( s_prehead.smode == DETACHED_CHILD ) {
      if( self->_opcnt != dchild.info.opcnt || self->_nobj != (int64_t)dchild.info.nobj ) {
        if( self->_opcnt == CXLIB_OPERATION_START && self->_nobj == 0 ) {
          WARN( 0x68A, "Nested Framehash seems to have been lost: %s", CStringValue( self->_CSTR__masterpath ) );
        }
        else {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x68B, "Referenced nested Framehash is out of sync: %s [opcnt=%llu/%llu  nobj=%llu/%llu]", 
                                                             CStringValue( self->_CSTR__masterpath ), self->_opcnt, dchild.info.opcnt, self->_nobj, dchild.info.nobj );
        }
      }
    }

  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }
  return self;
}



/*******************************************************************//**
 * _framehash_serialization__bulk_serialize
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_serialization__bulk_serialize( framehash_t *self, bool force ) {
  return _framehash_serialization__serialize( self, NULL, force );
}



/*******************************************************************//**
 * _framehash_serialization__erase
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_serialization__erase( framehash_t *self ) {
  int64_t ret = 0;

  // Clear
  CALLABLE( self )->Discard( self );

  // Clean up files
  const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;
  if( masterpath ) {
#ifdef FRAMEHASH_CHANGELOG
    // Suspend the changelog permanently
    if( self->changelog.enable ) {
      _framehash_changelog__end( self );
      _framehash_changelog__destroy( self );
    }

    // Remove all changelogs
    for( int seq = self->changelog.seq_start; seq<self->changelog.seq_end; seq++ ) {
      if( _framehash_changelog__remove( self, seq ) < 0 ) {
        const char *dirname = CStringValue( self->_CSTR__dirname );
        const char *basename = CStringValue( self->_CSTR__basename );
        WARN( 0x001, "Failed to remove changelog: '%08x' for %s in %s", seq, basename, dirname );
        ret = -1;
      }
    }
#endif

    // Remove main file
    if( file_exists( masterpath ) ) {
      if( remove( masterpath ) != 0 ) {
        WARN( 0x002, "Failed to remove: %s", masterpath );
        ret = -1;
      }
    }
  }

  return ret;
}



#ifdef FRAMEHASH_CHANGELOG
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_serialization__validate_applied_changelog( framehash_t *self ) {

  int64_t opcnt_delta = 0;

  const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;
  if( !masterpath ) {
    return -1;
  }

  errno_t err;
  int fd = 0;

  XTRY {
    __serialized_header_t s_header;

    // Verify master file exists
    if( !file_exists( masterpath ) ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x001, "Changelog error: missing file '%s'", masterpath );
    }

    // Open master file
    if( (err = OPEN_R_SEQ( &fd, masterpath )) != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x002, "Changelog file error: %d %s", err, strerror( err ) );
    }

    // Read existing header
    // Assume header at offset=0 for changelogged instances
    if( CX_READ( &s_header, sizeof( __serialized_header_t ), 1, fd ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x003, "Changelog error: bad header" );
    }

    // Verify object count
    if( self->_nobj != (int64_t)s_header.changelog_nobj ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x004, "Object count mismatch after applied changelog: %lld (expected %llu)", self->_nobj, s_header.changelog_nobj );
    }

    // Verify operation number
    if( self->_opcnt != s_header.changelog_opcnt ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x005, "Operation mismatch after applied changelog: %llu (expected %llu)", self->_opcnt, s_header.changelog_opcnt );
    }


    // Up to date!
    opcnt_delta = self->_opcnt - s_header.opcnt;

    // TODO: Full persist of instance when significant amount of changelog was applied at construction

  }
  XCATCH( errcode ) {
    opcnt_delta = -1;
  }
  XFINALLY {
    if( __FD_VALID( fd ) ) {
      CX_CLOSE( fd );
    }
  }

  return opcnt_delta;

}



/*******************************************************************//**
 *
 * Assume framehash is readonly
 ***********************************************************************
 */
static int64_t __update_incremental( framehash_t *self ) {
  int64_t nqwords = 0;
  int fd = 0;

  XTRY {
    errno_t err;
    const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;

    // Update header only
    if( masterpath && file_exists( masterpath ) ) {
      __serialized_header_t s_header = {0};
      // Open the file in r/w mode
      if( (err = OPEN_RW_SEQ( &fd, masterpath )) != 0 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6D1, "%d %s", err, strerror( err ) );
      }

      // Assume header is at offset=0 when updating incrementally
      if( CX_READ( &s_header, sizeof( __serialized_header_t ), 1, fd ) != 1 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6D2, "Bad header in '%s'", masterpath );
      }
      CX_SEEK( fd, 0, SEEK_SET );

      // Update header fields with changelog information
      s_header.changelog_nobj = self->_nobj;
      s_header.changelog_opcnt = self->_opcnt;
      s_header.changelog_seq_start = self->changelog.seq_start;
      s_header.changelog_seq_end = self->changelog.seq_end;
      s_header.persist_incomplete = 1;
      s_header.persist_t0 = __MILLISECONDS_SINCE_1970();

      if( CX_WRITE( &s_header, sizeof( __serialized_header_t ), 1, fd ) != 1 ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6D3, "Incremental serialization to '%s' failed (header)", masterpath );
      }

      // Update instance commit point
      self->changelog.seq_commit = self->changelog.seq_end - 1;

      // Return size of header
      nqwords = sizeof( __serialized_header_t );
    }   
  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
    if( __FD_VALID( fd ) ) {
      CX_CLOSE( fd );
    }
  }

  return nqwords;
}
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __write_header( framehash_t *self, __serialized_header_t *header, __output_t *output, bool complete, int fd ) {
  int64_t offset = 0;

  header->obid = self->obid;
  if( !complete ) {
    header->persist_t0 = __MILLISECONDS_SINCE_1970();
    header->persist_incomplete = 1;
  }
  else {
    header->persist_t1 = __MILLISECONDS_SINCE_1970();
    header->persist_incomplete = 0;
  }
  header->nobj = self->_nobj;
  header->opcnt = self->_opcnt;
  header->top_order = CELL_GET_FRAME_METAS( &self->_topframe )->order;
  if( self->_CSTR__masterpath ) {
    header->qwsz_path = qwcount( CStringLength( self->_CSTR__masterpath ) + 1 );
  }
  else {
    header->qwsz_path = 0;
  }
  header->nqwords = output->nqwords;
  header->synch = self->_flag.synchronized;
  header->shortkeys = self->_flag.shortkeys;
  header->cache_depth = self->_dynamic.cache_depth;
  header->changelog_obclass = self->changelog.obclass;
  header->changelog_seq_start = self->changelog.seq_start;
  header->changelog_seq_end = self->changelog.seq_end;
  
  if( __FD_VALID( output->fileno ) ) {
    offset = CX_TELL( output->fileno );
    if( fd == output->fileno ) {
      size_t sz = sizeof( __serialized_header_t );
      if( CX_WRITE( header, sz, 1, fd ) != 1 ) {
        return -1;
      }
      offset += sz;
      return offset;
    }
  }

  __OUTPUT( output, header, qwsizeof(__serialized_header_t) );
  return offset;
}



/*******************************************************************//**
 * _framehash_serialization__serialize
 *
 *
 * FRAMEHASH SERIALIZATION FORMAT
 *
 * *** HIGH LEVEL LAYOUT ***
 *  ____________________________________________________________
 * |                |        |        |        |         |      |
 * |      HEADER    | frame  | frame  | frame  |   ...   |  END |
 * |________________|________|________|________|_________|______|
 *  
 *  
 * All data is serializes in QWORD increments.
 *  
 *
 * **************
 * *** HEADER ***
 * Contains parameters and summary information
 * 
 * | start                                                             | parameters and summary
 * |___________________________________________________________________|______________________________________________________________________________________
 * |               64               |   8    |   8    |       16       |       16       |   ... 48 ...   |   8    |   8    |   8    |   8    |   ... 32 ...   |
 * |   FILE START: all 0xFFFFFF...  | typinf | smode  |  START DELIM   |      obid      |   ... rsv ...  |  nobj  | opcnt  | top_p  | qwszfn |   ... rsv ...  | ...
 * |________________________________|________|________|________________|________________|________________|________|________|________|________|________________|
 *  
 *  
 *                                                                      | frame orders                                                                                                               |
 *       _______________________________________________________________|____________________________________________________________________________________________________________________________|
 *      |   8    |   8    |   8    |   ... 104 ...  |      8*qwszfn     |   ... 32 ...   |   8    |   8    |   8    |   8    |   8    |   8    |   8    |   8    |   8    |   8    |   8    |   8    |
 *  ''' |  synch | shrtky | cdpth  |   ... rsv ...  |      filepath     |   ... rsv ...  | p_max=6| n_top  | n_cache| n_intrn| n_leaf0| n_leaf1| n_leaf2| n_leaf3| n_leaf4| n_leaf5| n_leaf6| n_bsmnt|
 *      |________|________|________|________________|___________________|________________|________|________|________|________|________|________|________|________|________|________|________|________|
 *  
 *  
 * ***********
 * *** END ***
 * Terminates the serialized framehash
 * 
 * | end                                                      |
 * |__________________________________________________________|
 * |       16       |   8    |               64               |
 * |   END DELIM    | nqwords|   FILE END: all 0xFFFFFFF...   |
 * |________________|________|________________________________|
 *  
 *  
 * ************** 
 * *** FRAMES ***
 * Frames are serialized using a common layout shared by all cache, internal, leaf and basement frames
 *
 * | common start of frame            | cell data             | all subframes if any       | common end      |
 * |__________________________________|_______________________|____________________________|_________________|
 * |       16       |   8    |   8    |                       |        (optional)          |   8    |   8    |
 * |  FRAME DELIM   | ftypmrk| prefix |  ... cell data ....   | ... all subframe data ...  | chkpnt | nqwords|
 * |________________|________|________|_______________________|____________________________|________|________|
 *
 * NOTE 1: All non-leaf frames may have chain cells that link to subframes.
 *         These cells are serialized as dummy placeholder cells indicating "CHAIN" 
 *         and hold no further information about the subframes. When restored from 
 *         serialized data, the presence of a Nth "CHAIN" placeholder cell uniquely
 *         identifies the Nth subframe that follows in the serialized data.
 *         The subframe's metas and its new memory location (after restore) will then
 *         be used to populate the chain cell in the restored frame.
 *
 * NOTE 2: Cache frame cells are all serialized as empty cells.
 *
 *
 * *************
 * *** CELLS ***
 * 
 * SIMPLE VALUE CELLS:
 * |                          |
 * |__________________________|
 * |   8    |   8    |   8    |
 * |  type  |  key   | qw3/val|
 * |________|________|________|
 *
 *
 * COMPLEX OBJECT CELLS:
 * |                          |                                                                                        |
 * |__________________________|________________________________________________________________________________________|
 * |   8    |   8    |   8    |       16       |                                             |       16       |   8    |
 * | otype  | shortid| typeinf|  OBJECT START  |  ... object specific serialized bytes ...   |   OBJECT END   | nqwords|
 * |________|________|________|________________|_____________________________________________|________________|________|
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_serialization__serialize( framehash_t *self, CQwordQueue_t *output_queue, bool force ) {
  int64_t nqwords = 0;
  const char *dirname = self->_CSTR__dirname ? CStringValue( self->_CSTR__dirname ) : NULL;
  const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;
  errno_t err;

  // Data modified since last persist
  if( self->_flag.persisted == 0 || force == true ) {
    // The framehash instance is either made readonly or locked during serialization.
    // By default lock on the framehash master lock (if one exists, i.e. if framehash is responsible for locking)
    CS_LOCK *sync_lock = self->_plock;
    // However, if we are in readonly mode we do not require locking to perform serialization, i.e. readonly mode makes
    // it save to serialize while other threads perform lookups in parallel
    if( _framehash_api_manage__is_readonly( self ) ) {
      sync_lock = NULL;
    }

#ifdef FRAMEHASH_CHANGELOG
    // CHANGELOG SUSPEND
    // Flush current changelog to disk and suspend
    if( self->changelog.enable ) {
      if( _framehash_changelog__suspend( self ) < 0 ) {
        CRITICAL( 0x691, "Failed to suspend changelog, serialization cannot continue" );
        return -1;
      }
    }
#endif

    // Lock as needed
    SYNCHRONIZE_ON_PTR( sync_lock ) {
      __output_t this_output = {0};
      __output_t parent_output = {0};
      char *filepath = NULL;

      XTRY {
#ifdef FRAMEHASH_CHANGELOG
        // -------------------------------------------------------------------------------
        // Incremental mode if changelog is enabled and we are not requesting full persist
        // -------------------------------------------------------------------------------
        if( self->changelog.enable && force == false ) {
          if( (nqwords = __update_incremental( self )) < 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x692, "Incremental persist failed" );
          }
        }

        // -----------------------------------------------------------------
        // Either we don't use changelog or we requested full persist anyway
        // -----------------------------------------------------------------
        else
#endif 
        {
          CQwordQueue_constructor_args_t qargs = {.element_capacity = ARCH_PAGE_SIZE/sizeof(QWORD), .comparator=NULL };
          __serialization_mode_t smode;
          __serialized_header_t s_header = INIT_HEADER;
          __frame_orders_t s_frame_orders = INIT_FRAME_ORDERS;
          __serialized_end_t s_end = INIT_END;
          framehash_context_t serialization_context = CONTEXT_INIT_TOP_FRAME( &self->_topframe, &self->_dynamic );
          framehash_retcode_t flushing;

          if( output_queue ) {
            parent_output.Q = output_queue;
            parent_output.write = CALLABLE(parent_output.Q)->WriteNolock;
          }

          // Determine serialization mode
          if( _FRAMEHASH_HAS_FILE(self) ) {
            if( parent_output.Q ) {
              smode = DETACHED_CHILD; // file-backed map nested inside another map
            }
            else {
              smode = ROOT_PARENT;    // file-backed map
            }
          }
          else {
            if( parent_output.Q ) {
              smode = EMBEDDED_CHILD; // anonymous map nested inside another map
            }
            else {
              smode = NO_OUTPUT;
            }
          }

          if( smode != NO_OUTPUT ) {

            if( self->_flag.dontenter ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x693, "Cycle detected, serialization aborted" );
            }

            // Prevent recursion when child map refers to ancestor map (illegal for now)
            self->_flag.dontenter = 1;

            // Flush all caches before we begin serialization
            flushing = _framehash_cache__flush( &serialization_context, false );
            if( flushing.error || flushing.completed == false ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x694, "Failed to flush caches" );
            }

            // Set framehash typeinfo in the header
            s_header.typeinfo = COMLIB_OBJECT_TYPEINFO_QWORD( self );

            // Create/attach the main queue
            switch( smode ) {
            case ROOT_PARENT:
              /* FALLTHRU */
            case DETACHED_CHILD:
              // Allocate a QWORD aligned buffer to hold filepath data to be serialized
              if( masterpath && dirname ) {
                size_t _sz_path = strlen( masterpath );
                QWORD qwsz_path = qwcount( _sz_path+1 );
                QWORD *pqw;
                TALIGNED_INITIALIZED_ARRAY_THROWS( pqw, QWORD, qwsz_path, 0, 0x695 );
                filepath = (char*)pqw;
                memcpy( filepath, masterpath, _sz_path );

                // Make sure directory exists
                if( !dir_exists( dirname ) && create_dirs( dirname ) != 0 ) {
                  THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x696, "Failed to create directory: %s", dirname );
                }

                // Open the data file for writing, replacing old file if any
                if( (err = OPEN_W_SEQ( &this_output.fileno, masterpath )) != 0 ) {
                  THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x697, "%d %s", err, strerror( err ) );
                }

                // Validate usable file descriptor
                if( !__FD_VALID( this_output.fileno ) ) {
                  THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x698, "Unsupported file descriptor: %d", this_output.fileno );
                }
                
                // Create the main output queue
                if( (this_output.Q = COMLIB_OBJECT_NEW( CQwordQueue_t, NULL, &qargs )) == NULL ) {
                  THROW_ERROR_MESSAGE( CXLIB_ERR_MEMORY, 0x699, "Failed to create main output queue" );
                }
                CALLABLE( this_output.Q )->AttachOutputDescriptor( this_output.Q, (short)this_output.fileno );
                this_output.write = CALLABLE( this_output.Q )->WriteNolock;
                
                // Serialization mode is FILE
                s_header.smode = FILE_START;

                // Stream out REFERENCE INFO for detached child into the parent's output queue
                if( smode == DETACHED_CHILD ) {
                  const QWORD file_cap[] = _SERIALIZATION_FILE_CAP;
                  __detached_child_t detached_child = {
                    .typeinfo = COMLIB_OBJECT_TYPEINFO_QWORD( self ),
                    .smode = DETACHED_CHILD,
                    .nobj = self->_nobj,
                    .opcnt = self->_opcnt,
                    .qwsz_path = qwsz_path
                  };
                  int64_t qw_pre = parent_output.nqwords;
                  // BEGIN
                  __OUTPUT_THROWS( &parent_output, file_cap, qwsizeof(file_cap), 0x69A );
                  // INFO
                  __OUTPUT_THROWS( &parent_output, &detached_child, qwsizeof(detached_child), 0x69B );
                  // FILE REFERENCE
                  __OUTPUT_THROWS( &parent_output, filepath, qwsz_path, 0x69C );  // 0-padded file path, qword quantized
                  // END
                  __OUTPUT_THROWS( &parent_output, file_cap, qwsizeof(file_cap), 0x69D );

                  nqwords = parent_output.nqwords - qw_pre;
                }
              }
              else {
                THROW_ERROR( CXLIB_ERR_BUG, 0x69E );
              }
              break;

            case EMBEDDED_CHILD:
              // embed output in parent if we don't have our own output file
              this_output.Q = parent_output.Q;
              this_output.write = parent_output.write;
              this_output.fileno = parent_output.fileno;
              // Serialization mode is EMBEDDED
              s_header.smode = EMBEDDED_CHILD;
              break;

            default:
              THROW_ERROR_MESSAGE( CXLIB_ERR_BUG, 0x69F, "Invalid serialization mode: %d", smode );
            }

            // -------------------
            // START SERIALIZATION 
            // -------------------

            CQwordQueue_t *Q = this_output.Q;
            int64_t s_header_offset = 0;
            int64_t s_end_offset = 0;

            // FLUSH main output to file if file-backed
            if( CALLABLE( Q )->HasOutputDescriptor( Q ) && CALLABLE( Q )->FlushNolock( Q ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6A0, "Failed to flush serialization buffer to '%s'", masterpath ? masterpath : "embedded" );
            }

            // CX_WRITE Header attributes (pre-completion)
            if( (s_header_offset = __write_header( self, &s_header, &this_output, false, 0 )) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6A1, "Serialization to '%s' failed (header)", masterpath ? masterpath : "embedded" );
            }

            // CX_WRITE File name
            if( filepath ) {
              __OUTPUT_THROWS( &this_output, filepath, s_header.qwsz_path, 0x6A2 );
            }

            // CX_WRITE Frame orders
            __subframe_orders( serialization_context.frame, &s_frame_orders );
            __OUTPUT_THROWS( &this_output, &s_frame_orders, qwsizeof(s_frame_orders), 0x6A3 );

            // ----------------------
            // RUN MAIN SERIALIZATION 
            // ----------------------
            // Run serialization routine, writing data to file (single thread guaranteed)
            if( __subframe_serialize( serialization_context.frame, &this_output ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6A4, "Serialization to '%s' failed", masterpath ? masterpath : "embedded" );
            }

            // CX_WRITE End attributes
            s_end.complete_ts = __MILLISECONDS_SINCE_1970();
            s_end.nqwords = this_output.nqwords + qwsizeof(s_end);
            __OUTPUT_THROWS( &this_output, &s_end, qwsizeof(s_end), 0x6A5 );

            // FLUSH main output to file if file-backed
            if( CALLABLE( Q )->HasOutputDescriptor( Q ) && CALLABLE( Q )->FlushNolock( Q ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6A6, "Failed to flush serialization buffer to '%s'", masterpath ? masterpath : "embedded" );
            }

#ifdef FRAMEHASH_CHANGELOG
            // Advance changelog seq start to end since changelogs are no longer needed
            const char *basename = self->_CSTR__basename ? CStringValue( self->_CSTR__basename ) : NULL;
            for( int seq = self->changelog.seq_start; seq<self->changelog.seq_end; seq++ ) {
              if( _framehash_changelog__remove( self, seq ) < 0 ) {
                WARN( 0x6A7, "Failed to clean up changelog: '%08x' for %s in %s", seq, basename, dirname );
              }
              self->changelog.seq_start++;
            }
#endif

            // UPDATE header (post-completion)
            s_end_offset = CX_TELL( this_output.fileno );
            CX_SEEK( this_output.fileno, s_header_offset, SEEK_SET );
            if( __write_header( self, &s_header, &this_output, true, this_output.fileno ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6A8, "Serialization to '%s' failed (header)", masterpath ? masterpath : "embedded" );
            }
            CX_SEEK( this_output.fileno, s_end_offset, SEEK_SET );

            // ----------------------
            // SERIALIZATION COMPLETE
            // ----------------------

            if( Q != parent_output.Q ) {
              if( CALLABLE( Q )->DetachOutput( Q ) < 0 ) {
                THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6A9, "Failed to detach output file from buffer" );
              }
            }

            if( nqwords == 0 ) {
              nqwords = this_output.nqwords;
            }
          }
        }

        // Persisted
        if( nqwords > 0 ) {
          self->_flag.persisted = 1;
        }

      }
      XCATCH( errcode ) {
        this_output.nqwords = -1;
      }
      XFINALLY {
        // Destroy main queue if we own it
        if( this_output.Q && this_output.Q != parent_output.Q ) {
          COMLIB_OBJECT_DESTROY( this_output.Q );
          this_output.Q = NULL;
        }

        // Close data file before exit
        if( __FD_VALID( this_output.fileno ) && this_output.fileno != parent_output.fileno ) {
          CX_CLOSE( this_output.fileno );
          this_output.fileno = 0;
        }

        // Free temporary file path
        if( filepath ) {
          ALIGNED_FREE( filepath );
        }

        // Re-allow function entry for this instance
        self->_flag.dontenter = 0;
      }
    } RELEASE;
      
#ifdef FRAMEHASH_CHANGELOG
    // CHANGELOG RESUME
    //
    if( self->changelog.enable ) {
      if( _framehash_changelog__resume( self ) < 0 ) {
        CRITICAL( 0x691, "Failed to resume changelog - incremental persist will be disabled" );
        self->changelog.enable = 0;
      }
    }
#endif
  }

  SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE
  return nqwords;
}



/*******************************************************************//**
 * _framehash_serialization__deserialize
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_t *_framehash_serialization__deserialize( framehash_t *framehash, CQwordQueue_t *input_queue ) {
  framehash_t *self = NULL;

  __input_t input = {0};


  input.Q = input_queue;
  if( input.Q ) {
    input.read = CALLABLE(input.Q)->ReadNolock;
    input.unread = CALLABLE(input.Q)->UnreadNolock;
  }

  /* No framehash container - delegate deserialization and return new, populated framehash */
  if( framehash == NULL ) {
    self = __inner_deserializing_constructor( &input );
  }
  /* Catch the no-input condition */
  else if( !_FRAMEHASH_HAS_FILE(framehash) && input.Q == NULL ) {
    self = NULL;
  }
  /* Populate empty framehash instance from input data */
  else {
    int fileno = -1;

    SYNCHRONIZE_ON_PTR( framehash->_dynamic.pflock ) {

      framehash_context_t deserialization_context = CONTEXT_INIT_TOP_FRAME( &framehash->_topframe, &framehash->_dynamic );

      char *filepath = NULL;
      const char *masterpath = NULL;

      XTRY {
        const QWORD file_cap[] = _SERIALIZATION_FILE_CAP;
        const QWORD start_delim[] = _SERIALIZATION_START;
        const QWORD end_delim[] = _SERIALIZATION_END;
        __serialized_header_t s_header = {0};
        __frame_orders_t s_frame_orders = {0};
        __frame_orders_t *p_frame_orders = &s_frame_orders;
        __serialized_end_t s_end = {0};
        __serialized_end_t *p_end = &s_end;
        bool apply_changelog = false;

        // Open and validate input file if we have one, and create an input queue backed by this file
        if( _FRAMEHASH_HAS_FILE(framehash) ) {
          masterpath = CStringValue( framehash->_CSTR__masterpath );
          CQwordQueue_constructor_args_t qargs = {.element_capacity = ARCH_PAGE_SIZE/sizeof(QWORD), .comparator=NULL };
          errno_t err;
          // Open the data file for reading
          if( (err = OPEN_R_SEQ( &fileno, masterpath )) != 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6B1, "%d %s", err, strerror( err ) );
          }

          // Validate usable file descriptor
          if( !__FD_VALID(fileno) ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x6B2, "Unsupported file descriptor: %d", fileno );
          }

          // Check end of file for correct terminator and expected qword count
          CX_SEEK( fileno, 0, SEEK_END );
          int64_t actual_size = CX_TELL( fileno );
          CX_SEEK( fileno, -(int)sizeof( __serialized_end_t ), SEEK_END );
          if( CX_READ( p_end, sizeof( __serialized_end_t ), 1, fileno ) != 1 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6B3, "expected end of framehash" );
          }

          // Validate end delimiter
          if( memcmp( s_end.end_delim, end_delim, sizeof(end_delim) ) != 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6B4, "bad end delimiter" );
          }
          // Validate file size
          int64_t expect_size = s_end.nqwords * sizeof( QWORD );
          if( expect_size != actual_size ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6B5, "bad file size %lld, expected %lld", actual_size, expect_size );
          }
          // Validate end file
          if( memcmp( s_end.end_file, file_cap, sizeof(file_cap) ) != 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6B6, "bad file end" );
          }

          // Rewind to start
          CX_SEEK( fileno, 0, SEEK_SET );

          // Create input queue
          if( input.Q ) {
            WARN( 0x6B6, "Framehash will be deserialized from: %s (input queue ignored)", masterpath );
          }
          if( (input.Q = COMLIB_OBJECT_NEW( CQwordQueue_t, NULL, &qargs )) == NULL ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_MEMORY, 0x6B7, "Failed to create input queue" );
          }
          input.read = CALLABLE(input.Q)->ReadNolock;
          input.unread = CALLABLE(input.Q)->UnreadNolock;
          CALLABLE( input.Q )->AttachInputDescriptor( input.Q, (short)fileno );
        }

        // ---------------------
        // START DESERIALIZATION
        // ---------------------

        // CX_READ the header
        __serialized_header_t *p_header = &s_header;
        __INPUT_THROWS( &input, &p_header, qwsizeof( s_header ), 0x69A );

        // ---------------
        // Validate header
        // ---------------

        // Start of data
        if( memcmp( s_header.begin_file, file_cap, sizeof(file_cap) ) != 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6B8, "bad file start" );
        }
        // Expect a framehash instance
        if( !COMLIB_TYPEINFO_QWORD_MATCH( COMLIB_CLASS_TYPEINFO( framehash_t ).qword, s_header.typeinfo ) ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6B9, "unexpected class identifier 0x%08llx during framehash deserialization (%s)", s_header.typeinfo, masterpath ? masterpath : "embedded" );
        }

        // Serialization mode
        if( _FRAMEHASH_HAS_FILE(framehash) ) {
          // We are deserializing from the beginning of a new file, i.e. FILE_START
          if( s_header.smode != FILE_START ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x6BA );
          }
        }
        else {
          // We are deserializing in the current input queue - i.e. EMBEDDED_CHILD
          if( s_header.smode != EMBEDDED_CHILD ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x6BB );
          }
        }

        // Start delimiter
        if( memcmp( s_header.start_delim, start_delim, sizeof(start_delim) ) != 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6BC, "bad start delimiter" );
        }

        // Format Version
        input.fmtver = s_header.format_version;
        if( input.fmtver != _FRAMEHASH_FORMAT_VERSION_1_0 && input.fmtver != _FRAMEHASH_FORMAT_VERSION_1_1 ) {
          WARN( 0x6BD, "Framehash format version mismatch, found %016X, expected %016X.", s_header.format_version, _FRAMEHASH_FORMAT_VERSION_1_1 );
        }

        // Framehash order already defined in instance, verify match
        if( framehash->_order != FRAMEHASH_ARG_UNDEFINED && framehash->_order != (int)s_header.top_order ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6BE, "incorrect framehash order, expected %d, got %llu", framehash->_order, s_header.top_order );
        }

        // Max filepath size
        if( s_header.qwsz_path*sizeof(QWORD) > MAX_PATH ) { // (max 256, not 260, but whatever)
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x6BF ); 
        }

        // ---------------------------------------------------
        // Populate instance attributes from header attributes
        // ---------------------------------------------------

        // Object ID
        framehash->obid = s_header.obid;

        // Mapped items
        framehash->_nobj = s_header.nobj;
        
        // Data synchronization counter
        framehash->_opcnt = s_header.opcnt;

        // Top Cache Order
        framehash->_order = (int)s_header.top_order;

        // Synchronization mode
        framehash->_flag.synchronized = s_header.synch ? 1 : 0;

        // Use shortkeys
        framehash->_flag.shortkeys = s_header.shortkeys ? 1 : 0;

        // Cache depth (unless already defined in instance)
        if( framehash->_dynamic.cache_depth == FRAMEHASH_ARG_UNDEFINED ) {
          framehash->_dynamic.cache_depth = (int)s_header.cache_depth;
        }

        // Changelog
        if( framehash->changelog.obclass != CLASS_NONE ) {
          if( framehash->changelog.obclass == (uint8_t)s_header.changelog_obclass || s_header.changelog_obclass == CLASS_NONE ) {
            // Range
            framehash->changelog.seq_start = (int)s_header.changelog_seq_start;
            framehash->changelog.seq_end = (int)s_header.changelog_seq_end;
            framehash->changelog.seq_commit = framehash->changelog.seq_end - 1;
            // Apply changelog after deserialization if instance invalid flag was set (i.e. incremental persist(s) occurred)
            if( s_header.persist_incomplete ) {
              apply_changelog = true;
            }
          }
          else {
            THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6C0, "inconsistent changelog object class %02x (expected %02x)", (int)s_header.changelog_obclass, (int)framehash->changelog.obclass );
          }
        }

        // CX_READ File path into qword buffer
        if( s_header.qwsz_path > 0 ) {
          QWORD *pqw = NULL;
          TALIGNED_ARRAY( pqw, QWORD, s_header.qwsz_path );
          if( (filepath = (char*)pqw) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x6C1 );
          }
          __INPUT_THROWS( &input, (QWORD**)(&filepath), s_header.qwsz_path, 0x6C2 );
        }

        // CX_READ Frame orders summary
        __INPUT_THROWS( &input, &p_frame_orders, qwsizeof(__frame_orders_t), 0x6C3 );

        // Validate max order compatibility
        if( s_frame_orders.p_max != _FRAMEHASH_P_MAX ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6C4, "incompatible p_max" );
        }

        // ------------------------
        // RUN MAIN DESERIALIZATION
        // ------------------------
        // Run the deserialization routine to produce a fully populated framehash instance
        if( __subframe_deserialize( &deserialization_context, &input ) == NULL ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6C5, "Frame deserialization failed" );
        }

        // CX_READ end attributes
        __INPUT_THROWS( &input, &p_end, qwsizeof(__serialized_end_t), 0x6C6 );

        // Validate end
        if( memcmp( s_end.end_delim, end_delim, sizeof(end_delim) ) != 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6C7, "bad end delimiter" );
        }
        if( memcmp( s_end.end_file, file_cap, sizeof(file_cap) ) != 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6C8, "bad file end" );
        }

        // ------------------------
        // DESERIALIZATION COMPLETE
        // ------------------------
        
        // Verify file completely read and detach from input queue
        if( _FRAMEHASH_HAS_FILE(framehash) ) {
          // Detach file from input queue
          if( CALLABLE( input.Q )->DetachInput( input.Q ) < 0 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x6C9, "failed to properly detach input file from input queue" );
          }
          // Apply changelog
          if( apply_changelog ) {
            if( _framehash_changelog__apply( framehash ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0x6CA, "failed to apply changelog" );
            }
          }
        }

        // Success
        self = framehash;
      }
      XCATCH( errcode ) {
        // Failed at the tail end of deserialization
        if( _CELL_REFERENCE_EXISTS( deserialization_context.frame ) ) {
          _framehash_memory__discard_frame( &deserialization_context );
          _INITIALIZE_REFERENCE_CELL( deserialization_context.frame, 0xF, 0xF, FRAME_TYPE_NONE );
        }
        if( _FRAMEHASH_HAS_FILE(framehash) ) {
          cxlib_msg_set( NULL, 0, "framehash\t%s", masterpath );
        }
        self = NULL;
      }
      XFINALLY {
        // Destroy input queue if we own it
        if( _FRAMEHASH_HAS_FILE(framehash) && input.Q ) {
          COMLIB_OBJECT_DESTROY( input.Q );
        }

        // Close data file before exit
        if( __FD_VALID(fileno) ) {
          CX_CLOSE( fileno );
          fileno = 0;
        }

        // Free temporary filepath
        if( filepath ) {
          ALIGNED_FREE( filepath );
        }
      }
    } RELEASE;
  }

  return self;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_serialization.h"

DLL_HIDDEN test_descriptor_t _framehash_serialization_tests[] = {
  { "serialization",   __utest_framehash_serialization },
  {NULL}
};
#endif
