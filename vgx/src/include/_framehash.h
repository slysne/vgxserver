/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    _framehash.h
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

#ifndef _FRAMEHASH_H
#define _FRAMEHASH_H

// This will cause DLL_PUBLIC symbols to be exported. The default is to import.
// Code that includes _framehash.h will export
// Code that includes framehash.h will import
#define _FRAMEHASH_EXPORT

#include "_comlib.h"
#include "_fhbase.h"
#include "_fmacro.h"
#include "frameallocator.h"
#include "basementallocator.h"



/* framehash member attributes */
typedef struct _s_framehash_members_t {
  int framehash_init_ok;       // framehash initialization flag
} _framehash_members_t;


extern _framehash_members_t g_FRAMEHASH;




/*
A few words about order p and zone k in the updated version:

Order p now denotes the log2 of the number of cache lines in a frame. This means
p=0 is a frame with a single cache line, the first half being used for metas and
reserved allocator fields as before, and the second half now holding two cells
whereas before the second half was unused. Order p=1 adds another cache line, for
a total of six cells. Both p=0 and p=1 are linear search only, i.e. the key hash
is not used. Starting at p=2 the key hash is used for zone probing.

Zone k is numbered as before, where the maximum k for a frame is p-1. For example,
in the largest frame of order p=6 there are six zones k=0 through k=5. The zone
k=5 has 2**5=32 slots, the zone k=0 has 2**0=1 slot, just as before.

Remember:
> max_k = p - 1
> p = max_k + 1
> Order p is the number of zones
> p=0 : no zones, only the first cacheline with 2 header cells in second half, no hashing, linear scan
> p=1 : single zone k=0, no hashing, linear scan of zone k=0 first, then two header cells
> p=2 : two zones k=1 and k=0, use key hash to zone probe k=1 and k=0, then two header cells
> p=3 : three zones k=2,1,0 use key hash to zone probe k=2 through k=0, then two header cells
> p=4 : four zones k=3,2,1,0, use key hash to zone probe k=3 through k=0, then two header cells
> p=5 : five zones k=4,3,2,1,0, use key hash to zone probe k=4 through k=0, then two header cells
> p=6 : six zones k=5,4,3,2,1,0, use key hash to zone probe k=5 through k=0, then two header cells
for( int k=p-1; k >=0 ; k-- ); // zone progression


There are several advantages to this update:
- Increased frame capacity (p=0: inf; p=1: 50%; p=2: 17%; p=3: 7%; p=4: 3%; p=5: 2%; p=6: 1%)
- Cut the memory overhead in half when minimal leaves of 1 or 2 items are used
- Order is consistent between cache frames and leaf/internal frames (e.g. p=6 means 64 cachelines regardless of frame type)
- Order is consistent with the frame allocator's aidx (e.g. aidx=p=0 means only the header cells, aidx=p=6 means 63 slots)


*/



#define FRAMEHASH_MAX_P_MAX 6 /* This is always 6, never change it! */


#define _FRAMEHASH_MAX_CACHE_FRAME_ORDER 15  // <= 15 is set in stone, do not modify!
#if _FRAMEHASH_MAX_CACHE_FRAME_ORDER != 15
#error  "max cache order must be 15"
#endif


// Sanity check parameters
#if _FRAMEHASH_P_INCREMENT < 1
#error "_FRAMEHASH_P_INCREMENT must be at least 1"
#endif

#if _FRAMEHASH_P_MAX > FRAMEHASH_MAX_P_MAX
#error "_FRAMEHASH_P_MAX too large"
#endif

#if _FRAMEHASH_P_MIN > _FRAMEHASH_P_MAX
#error "_FRAMEHASH_P_MIN cannot be larger than _FRAMEHASH_P_MAX"
#endif



#define _FRAMEHASH_CACHE_CELLS_PER_SLOT  (FRAMEHASH_CELLS_PER_SLOT-1)  /* in cache frames, all but the last cell in a slot are cache cells */
#define _FRAMEHASH_CACHE_CHAIN_CELL_J    (FRAMEHASH_CELLS_PER_SLOT-1)  /* the index of the chain cell in cache frames */
#define _FRAMEHASH_CACHE_EVICTION_J      (FRAMEHASH_CELLS_PER_SLOT-2)  /* last last cell before the chain cell */




typedef enum e_framehash_domain_t {
  FRAME_DOMAIN_TOP = 0,
  FRAME_DOMAIN_FIRST_FRAME = 1,
  FRAME_DOMAIN_LAST_FRAME = 4,
  FRAME_DOMAIN_FIRST_BASEMENT = 5,
  FRAME_DOMAIN_LAST_BASEMENT = 255
} _framehash_domain_t;


typedef int _chainindex_t;

typedef uint16_t _hashbits_t;

#define _FRAMEHASH_HAS_FILE( FramehashPtr )    ((FramehashPtr)->_CSTR__masterpath != NULL)
#define __FD_VALID( FD )  (FD >= 3 && FD <= _FRAMEHASH_MAX_FILENO)

#define _SERIALIZATION_FILE_CAP           { 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL }
#define _SERIALIZATION_START              { 0x5555555555555555ULL, 0x5555555555555555ULL }
#define _SERIALIZATION_END                { 0xEEEEEEEEEEEEEEEEULL, 0xEEEEEEEEEEEEEEEEULL }
#define _SERIALIZATION_FRAMEDELIM         { 0xAAAAAAAAAAAAAAAAULL, 0xFFFFFFFFFFFFFFFFULL }
#define _SERIALIZATION_CACHE_FRAME        0xAAFF00000000FFAAULL
#define _SERIALIZATION_LEAF_FRAME         0xAAFF11111111FFAAULL
#define _SERIALIZATION_INTERNAL_FRAME     0xAAFF22222222FFAAULL
#define _SERIALIZATION_BASEMENT_FRAME     0xAAFF33333333FFAAULL
#define _SERIALIZATION_CHECKPOINT         0xCCCCCCCCCCCCCCCCULL
#define _SERIALIZATION_BEGIN_OBJECT       { 0x0B0B0B0B0B0B0B0BULL, 0x0D0D0D0D0D0D0D0DULL }
#define _SERIALIZATION_END_OBJECT         { 0xBBBBBBBBBBBBBBBBULL, 0xDDDDDDDDDDDDDDDDULL }
#define _SERIALIZATION_FLUSH_THRESHOLD    (1UL << 18)  // 2**18=262144 QWORDS = 2 MB

typedef enum _e_serialized_cellkey_t {
  SERIALIZED_CELL_KEY_NONE      = 0x0,
  SERIALIZED_CELL_KEY_PLAIN64   = 0x1,
  SERIALIZED_CELL_KEY_HASH64    = 0x2,
  SERIALIZED_CELL_KEY_HASH128   = 0x3,
  SERIALIZED_CELL_KEY_ERROR     = 0x4
} _serialized_cellkey_t;


typedef enum _e_serialized_celltype_t {
  SERIALIZED_CELL_TYPE_END        = 0x1,
  SERIALIZED_CELL_TYPE_MEMBER     = 0x2,
  SERIALIZED_CELL_TYPE_BOOLEAN    = 0x3,
  SERIALIZED_CELL_TYPE_UNSIGNED   = 0x4,
  SERIALIZED_CELL_TYPE_INTEGER    = 0x5,
  SERIALIZED_CELL_TYPE_REAL       = 0x6,
  SERIALIZED_CELL_TYPE_OBJECT64   = 0x7,
  SERIALIZED_CELL_TYPE_OBJECT128  = 0x8,
  SERIALIZED_CELL_TYPE_CHAIN      = 0xC,
  SERIALIZED_CELL_TYPE_EMPTY      = 0xE,
  SERIALIZED_CELL_TYPE_ERROR      = 0xF
} _serialized_celltype_t;



/*******************************************************************//**
 * Compute the slot index for a topframe under which IdHigh belongs
 * Order  : the topframe's order
 * IdHigh : the upper 64-bits of an object ID used to determine the subtree
 ***********************************************************************
 */
#define _FRAMEHASH_TOPINDEX( Order, IdHigh )  ( IdHigh & _ZONE_INDEXMASK(Order) )



/*******************************************************************//**
 * Adjust various counters after item insertion or modification
 *
 * TODO: WARNING, This macro is called from within sections synchronized
 * on SUB-RADIXES ONLY. This is not safe since several threads may then
 * be performing updates to _nobj and opcount flags. We need to protect
 * the counters within a master lock section. All framehash access must 
 * be synchronized externally until this is fixed!
 *
 ***********************************************************************
 */
#define __MOD_INCREMENT( InsertionCode, FramehashPtr )      \
do {                                                        \
  /* increment item count */                                \
  if( (InsertionCode).delta ) {                             \
    (FramehashPtr)->_nobj++;                                \
  }                                                         \
  /* increment operations since last persist */             \
  if( (InsertionCode).completed ) {                         \
    (FramehashPtr)->_opcnt++;                               \
    (FramehashPtr)->_flag.clean = 0;                        \
    (FramehashPtr)->_flag.persisted = 0;                    \
  }                                                         \
}                                                           \
WHILE_ZERO



/*******************************************************************//**
 * Adjust various counters after item removal
 *
 * TODO: WARNING, This macro is called from within sections synchronized
 * on SUB-RADIXES ONLY. This is not safe since several threads may then
 * be performing updates to _nobj and opcount flags. We need to protect
 * the counters within a master lock section. All framehash access must 
 * be synchronized externally until this is fixed!
 *
 ***********************************************************************
 */
#define __MOD_DECREMENT( DeletionCode, FramehashPtr )       \
do {                                                        \
  /* decrement item count */                                \
  if( (DeletionCode).delta ) {                              \
    (FramehashPtr)->_nobj--;                                \
  }                                                         \
  /* increment operations since last persist */             \
  if( (DeletionCode).completed ) {                          \
    (FramehashPtr)->_opcnt++;                               \
    (FramehashPtr)->_flag.clean = 0;                        \
    (FramehashPtr)->_flag.persisted = 0;                    \
  }                                                         \
}                                                           \
WHILE_ZERO



/*******************************************************************//**
 * Determine the appropriate return code based on the operation's flags
 *
 ***********************************************************************
 */
#define __RETCODE_BY_OPERATION( OperationCode ) \
  ((OperationCode).completed ? 0 : (OperationCode).error ? -1 : 1)



/*******************************************************************//**
 * Determine the appropriate action code based on the operation's flags
 *
 ***********************************************************************
 */
#define __ACTIONCODE_BY_OPERATION( OperationCode, ValueType ) \
  ((OperationCode).completed ? ValueType : (OperationCode).error ? CELL_VALUE_TYPE_ERROR : CELL_VALUE_TYPE_NULL)


 
/*******************************************************************//**
 * Select and acquire the lock on a radix structure based on the high
 * portion of the obid.
 * If map is not synchronized on the inside (i.e. locks are guaranteed by
 * the surrounding application instead) no action is performed here.
 ***********************************************************************
 */
#define __SYNCHRONIZE_RADIX_ON_OBID( FramehashPtr, ObidPtr ) \
do {                                                        \
  framehash_t *__framehash = FramehashPtr;                  \
  uint64_t __id_high = 0;                                   \
  if( __framehash->_flag.synchronized ) {                   \
    __id_high = (ObidPtr)->H;                               \
    __acquire_subtree( __framehash, __id_high );            \
  }                                                         \
  do
/*
    {
      CODE GOES HERE
    }
*/



/*******************************************************************//**
 * Select and acquire the lock on a radix structure based on the shortid,
 * i.e. the same 64-bit key as used by the radix itself. This basically
 * re-maps the deepest  domain's key-bits up to the radix selector which
 * will lead to aliasing when  the map gets very full, making the deepest
 * domain less efficient and  resulting in early formation of basements.
 * We use this selector only when shortids are used for the map.
 * If map is not synchronized on the inside (i.e. locks are guaranteed by
 * the surrounding application instead) no action is performed here.
 ***********************************************************************
 */
#define __SYNCHRONIZE_RADIX_ON_SHORTID( FramehashPtr, ShortId )  \
do {                                                            \
  framehash_t *__framehash = FramehashPtr;                      \
  uint64_t __id_high = 0;                                       \
  if( __framehash->_flag.synchronized ) {                       \
    objectid_t __surrogate = _framehash_radix__get_surrogate_id( ShortId );  \
    __id_high = __surrogate.H;                                  \
    __acquire_subtree( __framehash, __id_high );                \
  }                                                             \
  do
/*
    {
      CODE GOES HERE
    }
*/



/*******************************************************************//**
 * Release the lock on the selected radix.
 *
 ***********************************************************************
 */
#define __RELEASE_RADIX                            \
  WHILE_ZERO;                                     \
  if( __framehash->_flag.synchronized ) {         \
    __release_subtree( __framehash, __id_high );  \
  }                                               \
}                                                 \
WHILE_ZERO                                        \



/*******************************************************************//**
 * __acquire_subtree
 *
 ***********************************************************************
 */
__inline static void __acquire_subtree( framehash_t * const self, const uint64_t idHigh ) {
  if( self->_plock ) {
    int fx = (int)_FRAMEHASH_TOPINDEX( self->_order, idHigh );
    SYNCHRONIZE_ON_PTR( self->_plock ) {
      while( self->_guard[fx].busy ) {
        // already busy - release and wait for ready condition, then re-acquire framelock
        // Initialize the slot-ready condition variable
        WAIT_CONDITION( &(self->_guard[fx].ready.cond), &(self->_dynamic.lock.lock) );
      }

      // We now hold the framelock, let's own the busy flag
      self->_guard[fx].busy = 1;
    } RELEASE;
  }
}



/*******************************************************************//**
 * __release_subtree
 *
 ***********************************************************************
 */
__inline static void __release_subtree( framehash_t * const self, const uint64_t idHigh ) {
  if( self->_plock ) {
    int fx = (int)_FRAMEHASH_TOPINDEX( self->_order, idHigh );
    SYNCHRONIZE_ON_PTR( self->_plock ) {
      self->_guard[fx].busy = 0;
      // Wake all threads waiting for the slot ready condition variable
      SIGNAL_ALL_CONDITION( &(self->_guard[fx].ready.cond) );
    } RELEASE;
  }
}



/*******************************************************************//**
 * __acquire_all_subtrees
 *
 ***********************************************************************
 */
__inline static void __acquire_all_subtrees( framehash_t * const self ) {
  if( self->_plock ) {
    for( int fx=0; fx < (1 << self->_order); fx++ ) {
      __acquire_subtree( self, fx );
    }
  }
}



/*******************************************************************//**
 * __release_all_subtrees
 *
 ***********************************************************************
 */
__inline static void __release_all_subtrees( framehash_t * const self ) {
  if( self->_plock ) {
    for( int fx=0; fx < (1 << self->_order); fx++ ) {
      __release_subtree( self, fx );
    }
  }
}



/*******************************************************************//**
 * ALL SUBTREES SYNCHRONIZATION
 *
 ***********************************************************************
 */
#define __SYNCHRONIZE_ALL_SUBTREES( FramehashPtr )  \
  do {                                              \
    framehash_t *__framehash__ = FramehashPtr;      \
    __acquire_all_subtrees( __framehash__ );        \
    do

#define __RELEASE_ALL_SUBTREES                      \
    WHILE_ZERO;                                     \
    __release_all_subtrees( __framehash__ );        \
  } WHILE_ZERO
    


/*******************************************************************//**
 * SUBTREE SYNCHRONIZATION
 *
 ***********************************************************************
 */
#define __SYNCHRONIZE_SUBTREE( FramehashPtr, SubtreeN ) \
  do {                                                  \
    framehash_t *__framehash__ = FramehashPtr;          \
    uint64_t __subtree_n__ = SubtreeN;                  \
    __acquire_subtree( __framehash__, __subtree_n__ );  \
    do

#define __RELEASE_SUBTREE                               \
    WHILE_ZERO;                                         \
    __release_subtree( __framehash__, __subtree_n__ );  \
  } WHILE_ZERO



/*******************************************************************//**
 * __framehash_update_counters
 *
 ***********************************************************************
 */
#ifdef FRAMEHASH_INSTRUMENTATION

/**************************************************************************//**
 * __framehash_update_counters
 *
 ******************************************************************************
 */
__inline static void __framehash_update_counters( _framehash_counters_t * const counters, const framehash_instrument_t * const instrument ) {
  SYNCHRONIZE_ON( counters->lock ) {
    counters->opcount++;
    counters->probe.depth += instrument->probe.depth;
    counters->probe.nCL += instrument->probe.nCL;
    counters->probe.ncachecells += instrument->probe.ncachecells;
    counters->probe.nleafcells += instrument->probe.nleafcells;
    counters->probe.nleafzones += instrument->probe.nleafzones;
    counters->probe.nbasementcells += instrument->probe.nbasementcells;
    counters->resize_up += instrument->resize.nup;
    counters->resize_down += instrument->resize.ndown;
    if( instrument->probe.hit ) {
      // we count cache hit/miss only if the searched item actually exists
      if( instrument->probe.hit_ftype == FRAME_TYPE_CACHE ) {
        counters->cache.hits++;
      }
      else {
        counters->cache.misses++;
      }
    }
  } RELEASE;
}

#define INSTRUMENT_WRITE_CONTEXT( WriteContextPtr )                         \
  framehash_instrument_t __writeop_instrument = _FRAMEHASH_INIT_INSTRUMENT; \
  (WriteContextPtr)->instrument = &__writeop_instrument

#define INSTRUMENT_READ_CONTEXT( ReadContextPtr )                           \
  framehash_instrument_t __readop_instrument = _FRAMEHASH_INIT_INSTRUMENT;  \
  (ReadContextPtr)->instrument = &__readop_instrument

#define UPDATE_WRITE_COUNTERS( FramehashPtr ) __framehash_update_counters( &(FramehashPtr)->_counters->write, &__writeop_instrument )
#define UPDATE_READ_COUNTERS( FramehashPtr ) __framehash_update_counters( &(FramehashPtr)->_counters->read, &__readop_instrument )

#else

#define INSTRUMENT_WRITE_CONTEXT( WriteContext )  ((void)0)
#define INSTRUMENT_READ_CONTEXT( ReadContext )    ((void)0)
#define UPDATE_WRITE_COUNTERS( FramehashPtr )     ((void)0)
#define UPDATE_READ_COUNTERS( FramehashPtr )      ((void)0)

#endif


#define BEGIN_FRAMEHASH_WRITE( FramehashPtr, WriteContextPtr )  \
  do {                                                          \
    framehash_t *__self__ = FramehashPtr;                       \
    framehash_context_t *__write__ = WriteContextPtr;           \
    INSTRUMENT_WRITE_CONTEXT( __write__ );                      \
    if( __self__->_flag.readonly == 0 )

#define END_FRAMEHASH_WRITE( RetcodePtr )                       \
    else {                                                      \
      (RetcodePtr)->completed = 0;                              \
      (RetcodePtr)->error = 1;                                  \
      __write__->vtype = CELL_VALUE_TYPE_NOACCESS;              \
    }                                                           \
    UPDATE_WRITE_COUNTERS( __self__ );                          \
  } WHILE_ZERO

#ifdef FRAMEHASH_INSTRUMENTATION
#define BEGIN_FRAMEHASH_READ( FramehashPtr, ReadContextPtr )    \
  do {                                                          \
    framehash_t *__self__ = FramehashPtr;                       \
    framehash_context_t *__read__ = ReadContextPtr;             \
    INSTRUMENT_READ_CONTEXT( __read__ );                        \
    do

#define END_FRAMEHASH_READ( RetcodePtr )                        \
    WHILE_ZERO;                                                 \
    UPDATE_READ_COUNTERS( __self__ );                           \
  } WHILE_ZERO
#else
#define BEGIN_FRAMEHASH_READ( FramehashPtr, ReadContextPtr )    \
  do

#define END_FRAMEHASH_READ( RetcodePtr )                        \
  WHILE_ZERO
#endif


/* OPERATION SIGNATURE */
typedef framehash_retcode_t (*f_framehash_operation_t)( framehash_context_t * const context );




/*******************************************************************//**
 * __framehash__set_context_key
 *
 ***********************************************************************
 */
__inline static framehash_keytype_t __framehash__set_context_key( framehash_t * const self, framehash_context_t *context, const framehash_keytype_t ktype, const void *pkey ) {
  context->frame = &self->_topframe;
  context->dynamic = &self->_dynamic;
  context->ktype = ktype;

  switch( ktype ) {
  case CELL_KEY_TYPE_PLAIN64:
    context->key.plain = *((QWORD*)pkey);
    context->key.shortid = self->_dynamic.hashf( context->key.plain );
    context->obid = NULL;
    return ktype;
  case CELL_KEY_TYPE_HASH64:
    context->key.plain = FRAMEHASH_KEY_NONE;
    context->key.shortid = *((QWORD*)pkey);
    context->obid = NULL;
    return ktype;
  case CELL_KEY_TYPE_HASH128:
    context->key.plain = FRAMEHASH_KEY_NONE;
    context->key.shortid = ((objectid_t*)pkey)->L;
    context->obid = (objectid_t*)pkey;
    return ktype;
  default:
    // only 64-bit keys allowed
    return CELL_KEY_TYPE_ERROR;
  }
}



/*******************************************************************//**
 * api_class
 *
 ***********************************************************************
 */
DLL_HIDDEN extern void _framehash_api_class__register_class( void );
DLL_HIDDEN extern void _framehash_api_class__unregister_class( void );
DLL_HIDDEN extern void _framehash_api_class__test_object_register_class( void );
DLL_HIDDEN extern void _framehash_api_class__test_object_unregister_class( void );



/*******************************************************************//**
 * api_generic
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_generic__set( framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const void *pkey, const void * const data_bytes );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_generic__del( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_generic__has( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_generic__get( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, void * const return_bytes );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_generic__inc( framehash_t * const self, const framehash_keytype_t ktype, const framehash_valuetype_t vtype, const QWORD key, void * const upsert_bytes, void * const return_bytes );



/*******************************************************************//**
 * api_object
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__set( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, const comlib_object_t * const obj );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__del( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__get( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, comlib_object_t ** const retobj );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__set_object128_nolock( framehash_t * const self, const objectid_t *pobid, const comlib_object_t * const obj );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__del_object128_nolock( framehash_t * const self, const objectid_t *pobid );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__get_object128_nolock( framehash_t * const self, const objectid_t *pobid, comlib_object_t ** const retobj );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_object__has_object128_nolock( framehash_t * const self, const objectid_t *pobid );



/*******************************************************************//**
 * api_int56
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_int56__set(       framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t value );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_int56__del_key64( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_int56__has_key64( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_int56__get(       framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, int64_t * const retval );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_int56__inc(       framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const int64_t inc_value, int64_t * const retval );



/*******************************************************************//**
 * api_real56
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_real56__set( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double value );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_real56__get( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, double * const retval );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_real56__inc( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const double inc_value, double * const retval );



/*******************************************************************//**
 * api_pointer
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_pointer__set( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const void * const ptr );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_pointer__get( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, void ** const retobj );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_pointer__inc( framehash_t * const self, const framehash_keytype_t ktype, const QWORD key, const uintptr_t inc_bytes, void ** const retptr );



/*******************************************************************//**
 * api_iterator
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t _framehash_api_iterator__get_items( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_api_iterator__get_keys( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_api_iterator__get_values( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_api_iterator__count_nactive( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_api_iterator__process( framehash_t * const self, framehash_processing_context_t * const processor );
DLL_HIDDEN extern int64_t _framehash_api_iterator__process_partial( framehash_t * const self, framehash_processing_context_t * const processor, uint64_t selector );



/*******************************************************************//**
 * api_manage
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int _framehash_api_manage__flush( framehash_t * const self, bool invalidate );
DLL_HIDDEN extern int _framehash_api_manage__invalidate_caches( framehash_t * const self );
DLL_HIDDEN extern int _framehash_api_manage__discard( framehash_t * const self );
DLL_HIDDEN extern int _framehash_api_manage__compactify( framehash_t * const self );
DLL_HIDDEN extern int _framehash_api_manage__compactify_partial( framehash_t * const self, uint64_t selector );
DLL_HIDDEN extern int _framehash_api_manage__set_readonly( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__is_readonly( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__clear_readonly( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__enable_read_caches( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__disable_read_caches( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__enable_write_caches( framehash_t *self );
DLL_HIDDEN extern int _framehash_api_manage__disable_write_caches( framehash_t *self );



/*******************************************************************//**
 * api_info
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t _framehash_api_info__items( const framehash_t * const self );
DLL_HIDDEN extern const CString_t * _framehash_api_info__masterpath( const framehash_t * const self );
DLL_HIDDEN extern framehash_cache_hitrate_t _framehash_api_info__hitrate( framehash_t * const self );



/*******************************************************************//**
 * api_simple
 *
 ***********************************************************************
 */
// Memory
DLL_HIDDEN extern    framehash_cell_t * _framehash_api_simple__new(      framehash_dynamic_t *dynamic );
DLL_HIDDEN extern               int64_t _framehash_api_simple__destroy(  framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic );

// Set
DLL_HIDDEN extern                   int _framehash_api_simple__set(      framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                      framehash_key_t fkey,
                                                                                                      framehash_valuetype_t vtype, 
                                                                                                      const framehash_value_t fvalue );
DLL_HIDDEN extern                   int _framehash_api_simple__set_int(  framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key,
                                                                                                      int64_t value );

// Inc
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_simple__inc(      framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                      framehash_key_t fkey,
                                                                                                      framehash_valuetype_t vtype,
                                                                                                      const framehash_value_t finc,
                                                                                                      framehash_value_t *pfvalue,
                                                                                                      bool *created );
DLL_HIDDEN extern                   int _framehash_api_simple__inc_int(  framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key,
                                                                                                      int64_t inc,
                                                                                                      int64_t *value );

// Dec
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_simple__dec(      framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                      framehash_key_t fkey,
                                                                                                      framehash_valuetype_t vtype,
                                                                                                      const framehash_value_t fdec,
                                                                                                      framehash_value_t *pfvalue,
                                                                                                      bool *created );
DLL_HIDDEN extern                   int _framehash_api_simple__dec_int(  framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key,
                                                                                                      int64_t dec,
                                                                                                      int64_t *value,
                                                                                                      bool autodelete,
                                                                                                      bool *deleted );

// Get
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_simple__get(      framehash_cell_t *entrypoint,  const framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                      framehash_key_t fkey,
                                                                                                      framehash_value_t *pfvalue );
DLL_HIDDEN extern                   int _framehash_api_simple__get_int(  framehash_cell_t *entrypoint,  const framehash_dynamic_t *dynamic, QWORD key,
                                                                                                      int64_t *value );
DLL_HIDDEN extern int                   _framehash_api_simple__get_int_hash64( framehash_cell_t *entrypoint, shortid_t key, int64_t *rvalue );
DLL_HIDDEN extern framehash_valuetype_t _framehash_api_simple__get_hash64( framehash_cell_t *entrypoint, shortid_t key, framehash_value_t *pfvalue );


// Has
DLL_HIDDEN extern                   int _framehash_api_simple__has(      framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                     framehash_key_t fkey );
DLL_HIDDEN extern                   int _framehash_api_simple__has_int(  framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, QWORD key );
DLL_HIDDEN extern                   int _framehash_api_simple__has_hash64( framehash_cell_t *entrypoint, shortid_t key );

// Del
DLL_HIDDEN extern                   int _framehash_api_simple__del(      framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype,
                                                                                                      framehash_key_t fkey );
DLL_HIDDEN extern                   int _framehash_api_simple__del_int(  framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key );

// Info
DLL_HIDDEN extern               int64_t _framehash_api_simple__length(   framehash_cell_t *entrypoint );
DLL_HIDDEN extern                  bool _framehash_api_simple__empty(    framehash_cell_t *entrypoint );

// Readonly
DLL_HIDDEN extern                   int _framehash_api_simple__set_readonly( framehash_cell_t *entrypoint );
DLL_HIDDEN extern                   int _framehash_api_simple__is_readonly( framehash_cell_t *entrypoint );
DLL_HIDDEN extern                   int _framehash_api_simple__clear_readonly( framehash_cell_t *entrypoint );

// Processing
DLL_HIDDEN extern               int64_t _framehash_api_simple__process(  framehash_cell_t *entrypoint, f_framehash_cell_processor_t cellfunc, void *input, void *output );
DLL_HIDDEN extern  Key64Value56List_t * _framehash_api_simple__int_items( framehash_cell_t *entrypoint, framehash_dynamic_t *dynamic, Key64Value56List_t *output );



/*******************************************************************//**
 * dynamic
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_dynamic_t * _framehash_dynamic__initialize( framehash_dynamic_t *dynamic, const framehash_constructor_args_t *args );
DLL_HIDDEN extern framehash_dynamic_t * _framehash_dynamic__initialize_simple( framehash_dynamic_t *dynamic, const char *name, int block_size_order );
DLL_HIDDEN extern int _framehash_dynamic__reset_simple( framehash_dynamic_t *dynamic );
DLL_HIDDEN extern void _framehash_dynamic__clear( framehash_dynamic_t *dynamic );
DLL_HIDDEN extern void _framehash_dynamic__print_allocators( framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int _framehash_dynamic__check_allocators( framehash_dynamic_t *dynamic );



/*******************************************************************//**
 * memory
 *
 ***********************************************************************
 */
DLL_HIDDEN extern cxmalloc_family_t * _framehash_memory__new_frame_allocator( const char *description, size_t block_size );
DLL_HIDDEN extern cxmalloc_family_t * _framehash_memory__new_basement_allocator( const char *description, size_t block_size );
DLL_HIDDEN extern framehash_slot_t * _framehash_memory__new_frame( framehash_context_t *context, const int order, const int domain, const framehash_ftype_t ftype );
DLL_HIDDEN extern int64_t _framehash_memory__discard_frame( framehash_context_t * const context );
DLL_HIDDEN extern framehash_cell_t * _framehash_memory__new_basement( framehash_context_t *context, const int domain );
DLL_HIDDEN extern int64_t _framehash_memory__discard_basement( framehash_context_t *context );



/*******************************************************************//**
 * leaf
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_retcode_t _framehash_leaf__set( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_leaf__get( framehash_context_t * const context );
DLL_HIDDEN extern uint8_t _framehash_leaf__delete_item( framehash_cell_t *frame, framehash_cell_t *target );
DLL_HIDDEN extern framehash_retcode_t _framehash_leaf__del( framehash_context_t * const context );



/*******************************************************************//**
 * basement
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_retcode_t _framehash_basement__set( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_basement__del( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_basement__get( framehash_context_t * const context );



/*******************************************************************//**
 * cache
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_retcode_t _framehash_cache__set( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_cache__del( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_cache__get( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_cache__flush_slot( framehash_context_t * const context, framehash_slot_t * const chainslot, bool invalidate );
DLL_HIDDEN extern framehash_retcode_t _framehash_cache__flush( framehash_context_t * const context, bool invalidate );
DLL_HIDDEN extern int _framehash_cache__hitrate( framehash_context_t * const context, framehash_cache_hitrate_t * const hitrate );


/*******************************************************************//**
 * hashing
 *
 ***********************************************************************
 */
/*******************************************************************//**
 * _framehash_hashing__get_hashbits
 * Return the 15-bit region in id64 relevant to the specified domain
 * domain : 1,2,3,4
 * An invalid region is represented by MSB set to 1
 ***********************************************************************
 */

typedef enum __e_hashbits_mask_t {
  HASHBITS_VALID   = 0x7FFF,
  HASHBITS_INVALID = 0x8000
} __hashbits_mask_t;


/**************************************************************************//**
 * _framehash_hashing__get_hashbits
 *
 ******************************************************************************
 */
__inline static _hashbits_t _framehash_hashing__get_hashbits( const int domain, const shortid_t id64 ) {
  return (_hashbits_t)(domain > 0 ? ( id64 >> ((domain-1) << 4) ) & HASHBITS_VALID : HASHBITS_INVALID);
}



/*******************************************************************//**
 * radix
 *
 ***********************************************************************
 */
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__try_compaction( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__try_destroy_cache_chain( framehash_context_t * const context );
DLL_HIDDEN extern          objectid_t _framehash_radix__get_surrogate_id( const shortid_t shortid );
DLL_HIDDEN extern  framehash_slot_t * _framehash_radix__get_cacheslot( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__create_internal_chain( framehash_context_t *context );
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__internal_set( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__internal_get( framehash_context_t * const context );
DLL_HIDDEN extern framehash_retcode_t _framehash_radix__internal_del( framehash_context_t * const context );
DLL_HIDDEN extern int                 _framehash_radix__compactify_subtree( framehash_context_t * const context );
DLL_HIDDEN extern int64_t             _framehash_radix__subtree_length( framehash_cell_t *self, framehash_dynamic_t *dynamic, int64_t limit );


/*******************************************************************//**
 * _framehash_radix__is_chain
 * Return 1 if a chain exists at chainindex, given the frame metas, 0 otherwise
 *
 ***********************************************************************
 */
__inline static int _framehash_radix__is_chain( const framehash_metas_t * const frame_metas, const _chainindex_t cidx ) {
  return (frame_metas->chainbits >> _CHAININDEX_SLOT_Q(cidx) ) & 1;
}



/*******************************************************************//**
 * _framehash_radix__get_chainindex
 * Return the bit region in hashbits to be used for indexing a chain slot
 * and chain cell for internal frames, and for indexing a cache slot for
 * cache frames.
 *
 ***********************************************************************
 */
__inline static _chainindex_t _framehash_radix__get_chainindex( const _hashbits_t h16 ) {
#if _FRAMEHASH_P_MAX == 6
  // -122333444455555
  //        qqqqjj  <- internal frame slot=q, cell=j
  //        cccccc  <- cache frame slot=c
  return (h16 >> 3) & 0x3F; // x4 plus 2 bits of x5 (shift through 3 ignored bits in x5, then mask in 6 bits)
#elif _FRAMEHASH_P_MAX == 5
  // -122333444455555
  //     qqqjj
  //     ccccc
  return (h16 >> 7) & 0x1F; // x3 plus 2 bits of x4 (shift through 2 ignored bits in x4 plus entire x5, then mask in 5 bits)
#elif _FRAMEHASH_P_MAX == 4
  // -122333444455555
  //   qqjj
  //   cccc
  return (h16 >> 10) & 0xF; // x2 plus 2 bits of x3 (shift through 1 ignored bit in x3 plus entire x4 and x5, then mask in 4 bits)
#elif _FRAMEHASH_P_MAX == 3
  // -122333444455555
  //  qjj
  //  ccc
  return (h16 >> 12) & 0x7; // x1 plus x2 (shift through entire x3, x4 and x5, then mask in 3 bits)
#else
#error "invalid _FRAMEHASH_P_MAX"
#endif
}



/*******************************************************************//**
 * __INVALID_CALL
 ***********************************************************************
 */
static const framehash_retcode_t __INVALID_CALL = {
  .completed    = false,
  .cacheable    = false,
  .modified     = false,
  .empty        = false,
  .unresizable  = false,
  .error        = true,
  .delta        = false,
  .depth        = 0
};



/*******************************************************************//**
 * __NULL_OBJ_COMPLETION
 ***********************************************************************
 */
static const framehash_retcode_t __NULL_OBJ_COMPLETION = {
  .completed    = true,
  .cacheable    = false,
  .modified     = false,
  .empty        = false,
  .unresizable  = false,
  .error        = false,
  .delta        = false,
  .depth        = 0
};



/*******************************************************************//**
 * _framehash_radix__set
 ***********************************************************************
 */
__inline static framehash_retcode_t _framehash_radix__set( framehash_context_t * const context ) {
  switch( CELL_GET_FRAME_TYPE( context->frame ) ) {
  case FRAME_TYPE_CACHE:
    return _framehash_cache__set( context );
  case FRAME_TYPE_LEAF:
    return _framehash_leaf__set( context );
  case FRAME_TYPE_INTERNAL:
    return _framehash_radix__internal_set( context );
  case FRAME_TYPE_BASEMENT:
    return _framehash_basement__set( context );
  default:
    return __INVALID_CALL;
  }
}



/*******************************************************************//**
 * _framehash_radix__get
 ***********************************************************************
 */
__inline static framehash_retcode_t _framehash_radix__get( framehash_context_t * const context ) {
  switch( CELL_GET_FRAME_TYPE( context->frame ) ) {
  case FRAME_TYPE_CACHE:
    return _framehash_cache__get( context );
  case FRAME_TYPE_LEAF:
    return _framehash_leaf__get( context );
  case FRAME_TYPE_INTERNAL:
    return _framehash_radix__internal_get( context );
  case FRAME_TYPE_BASEMENT:
    return _framehash_basement__get( context );
  default:
    return __INVALID_CALL;
  }
}



/*******************************************************************//**
 * _framehash_radix__del
 ***********************************************************************
 */
__inline static framehash_retcode_t _framehash_radix__del( framehash_context_t * const context ) {
  switch( CELL_GET_FRAME_TYPE( context->frame ) ) {
  case FRAME_TYPE_CACHE:
    return _framehash_cache__del( context );
  case FRAME_TYPE_LEAF:
    return _framehash_leaf__del( context );
  case FRAME_TYPE_INTERNAL:
    return _framehash_radix__internal_del( context );
  case FRAME_TYPE_BASEMENT:
    return _framehash_basement__del( context );
  default:
    return __INVALID_CALL;
  }
}



/*******************************************************************//**
 * _framehash_radix__get_frameindex
 * Return the frame index for slot=q in zone=k given the frame order=p
 *
 ***********************************************************************
 */
__inline static int _framehash_radix__get_frameindex( const int order_p, const int zone_k, const int slot_q ) {
  //
  // k                                k                k        k    k  k
  // 55555555555555555555555555555555 4444444444444444 33333333 2222 11 0
  // ex:                       p=5 k=2   p=6 k=5   p=6 k=0
  //      0b111111 >> (6-p) = 0b011111  0b111111  0b111111
  //      0b111111 >> (5-k) = 0b000111  0b111111  0b000001
  //           XOR          = 0b011000  0b000000  0b111110
  //  offset:                       24         0        62
  //
  return ((_FRAMEHASH_OFFSET_MASK >> (_FRAMEHASH_P_MAX - order_p)) ^ (_FRAMEHASH_OFFSET_MASK >> (_FRAMEHASH_P_MAX - 1 - zone_k))) + slot_q;
}



/*******************************************************************//**
 * __get_internal_chainslot
 * Return a pointer to the chainslot selected by the shortid,
 * or NULL if no chaincell found for this shortid.
 *
 ***********************************************************************
 */
static framehash_slot_t * __get_internal_chainslot( const framehash_context_t * const context ) {

  framehash_cell_t *frame = context->frame;

  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( frame );
  if( self_metas->chainbits != 0 ) {
    _hashbits_t h16 = _framehash_hashing__get_hashbits( self_metas->domain, context->key.shortid );
    _chainindex_t chainindex = _framehash_radix__get_chainindex( h16 );
    if( _framehash_radix__is_chain( self_metas, chainindex ) ) {
      int p = self_metas->order;
      int fx = _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), _CHAININDEX_SLOT_Q(chainindex) );
      return CELL_GET_FRAME_SLOTS( frame ) + fx;
    }
  }
  return NULL;
}



/*******************************************************************//**
 * __get_internal_chaincell
 * Return a pointer to the chaincell selected by the shortid,
 * or NULL if no chaincell found for this shortid.
 *
 ***********************************************************************
 */
static framehash_cell_t * __get_internal_chaincell( const framehash_context_t * const context ) {

  framehash_cell_t *frame = context->frame;

  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS( frame );
  if( self_metas->chainbits != 0 ) {
    _hashbits_t h16 = _framehash_hashing__get_hashbits( self_metas->domain, context->key.shortid );
    _chainindex_t chainindex = _framehash_radix__get_chainindex( h16 );
    if( _framehash_radix__is_chain( self_metas, chainindex ) ) {
      int p = self_metas->order;
      int fx = _framehash_radix__get_frameindex( p, _FRAME_CHAINZONE(p), _CHAININDEX_SLOT_Q(chainindex) );
      framehash_slot_t *chainslot = CELL_GET_FRAME_SLOTS( frame ) + fx;
      return chainslot->cells + _CHAININDEX_CELL_J(chainindex);
    }
  }
  return NULL;
}



/*******************************************************************//**
 * __get_loadfactor
 * 0 - 100
 * NOTE: this does not account for cells in header's halfslot
 ***********************************************************************
 */
__inline static int __get_loadfactor( const framehash_cell_t * const self ) {
  static int factor = 100 / FRAMEHASH_CELLS_PER_SLOT;
  framehash_metas_t *self_metas = CELL_GET_FRAME_METAS(self);
  int p = self_metas->order;
  if( self_metas->nactive == 0 || p == 0 ) {
    return 0;;
  }
  else {
    int nactive_x_factor = factor * self_metas->nactive;
    switch( self_metas->ftype ) {
    case FRAME_TYPE_INTERNAL:
      return nactive_x_factor / (_FRAME_NSLOTS( p ) - POPCNT16( self_metas->chainbits ));  // <- slots capable of holding items
    case FRAME_TYPE_LEAF:
      return nactive_x_factor / _FRAME_NSLOTS( p );
    case FRAME_TYPE_BASEMENT:
      return nactive_x_factor / _FRAMEHASH_BASEMENT_SIZE;
    default:
      return 0;
    }
  }
}



/*******************************************************************//**
 * serialization
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t       _framehash_serialization__serialize( framehash_t *self, CQwordQueue_t *output_queue, bool force );
DLL_HIDDEN extern framehash_t * _framehash_serialization__deserialize( framehash_t *self, CQwordQueue_t *input_queue );
DLL_HIDDEN extern int64_t       _framehash_serialization__bulk_serialize( framehash_t *self, bool force );
DLL_HIDDEN extern int64_t       _framehash_serialization__erase( framehash_t *self );
DLL_HIDDEN extern int64_t              _framehash_serialization__validate_applied_changelog( framehash_t *self );



#ifdef FRAMEHASH_CHANGELOG
/*******************************************************************//**
 * changelog
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t _framehash_changelog__emit_operation( framehash_t *self, framehash_context_t *context );
DLL_HIDDEN extern int     _framehash_changelog__init( framehash_t *self, object_class_t obclass );
DLL_HIDDEN extern int     _framehash_changelog__destroy( framehash_t *self );
DLL_HIDDEN extern int     _framehash_changelog__start( framehash_t *self );
DLL_HIDDEN extern int     _framehash_changelog__suspend( framehash_t *self );
DLL_HIDDEN extern int     _framehash_changelog__resume( framehash_t *self );
DLL_HIDDEN extern int     _framehash_changelog__remove( framehash_t *self, int seq );
DLL_HIDDEN extern int     _framehash_changelog__apply( framehash_t *self );
DLL_HIDDEN extern int     _framehash_changelog__end( framehash_t *self );


/**************************************************************************//**
 * CHANGELOG_EMIT_OPERATION
 *
 ******************************************************************************
 */
__inline static int64_t CHANGELOG_EMIT_OPERATION( framehash_t *self, framehash_context_t *context ) {
  if( self->changelog.enable ) {
    return _framehash_changelog__emit_operation( self, context );
  }
  else {
    return -1;
  }
}
#else
IGNORE_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int64_t CHANGELOG_EMIT_OPERATION( framehash_t *self, framehash_context_t *context ) { return 0; }
__inline static int64_t _framehash_changelog__emit_operation( framehash_t *self, framehash_context_t *context ) { return 0; }
__inline static int     _framehash_changelog__init( framehash_t *self, object_class_t obclass ) { return 0; }
__inline static int     _framehash_changelog__destroy( framehash_t *self ) { return 0; }
__inline static int     _framehash_changelog__start( framehash_t *self ) { return 0; }
__inline static int     _framehash_changelog__suspend( framehash_t *self ) { return 0; }
__inline static int     _framehash_changelog__resume( framehash_t *self ) { return 0; }
__inline static int     _framehash_changelog__remove( framehash_t *self, int seq ) { return 0; }
__inline static int     _framehash_changelog__apply( framehash_t *self ) { return 0; }
__inline static int     _framehash_changelog__end( framehash_t *self ) { return 0; }
RESUME_WARNINGS
#endif



/*******************************************************************//**
 * processor
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t _framehash_processor__subtree_count_nactive_limit( framehash_cell_t *frame, framehash_dynamic_t *dynamic, int64_t limit );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_count_nactive( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_destroy_objects( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_cells( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_cells_filter( framehash_cell_t *frame, framehash_dynamic_t *dynamic, f_framehash_cell_collector_t collector, f_framehash_cell_filter_t *filter );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_cells_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_cells_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_refs_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_refs_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_annotations_into_list( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__subtree_collect_annotations_into_heap( framehash_cell_t *frame, framehash_dynamic_t *dynamic );
DLL_HIDDEN extern int64_t _framehash_processor__count_nactive_limit( framehash_t * const self, int64_t limit );
DLL_HIDDEN extern int64_t _framehash_processor__count_nactive( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__destroy_objects( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_cells( framehash_t * const self, f_framehash_cell_collector_t collector );
DLL_HIDDEN extern int64_t _framehash_processor__collect_cells_filter( framehash_t * const self, f_framehash_cell_collector_t collector, f_framehash_cell_filter_t *filter );
DLL_HIDDEN extern int64_t _framehash_processor__collect_cells_into_list( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_cells_into_heap( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_refs_into_list( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_refs_into_heap( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_annotations_into_list( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_processor__collect_annotations_into_heap( framehash_t * const self );
DLL_HIDDEN extern IStandardCellProcessors_t _framehash_processor__iStandardCellProcessors;
DLL_HIDDEN extern IStandardSubtreeProcessors_t _framehash_processor__iStandardSubtreeProcessors;
DLL_HIDDEN extern int64_t _framehash_processor__process_nolock_nocache( framehash_processing_context_t * const processor );
DLL_HIDDEN extern int64_t _framehash_processor__process_cache_partial_nolock_nocache( framehash_processing_context_t * const processor, uint64_t selector );
DLL_HIDDEN extern int64_t _framehash_processor__process_nolock( framehash_processing_context_t * const processor );
DLL_HIDDEN extern int64_t _framehash_processor__process( framehash_processing_context_t * const processor );



/*******************************************************************//**
 * framemath
 *
 ***********************************************************************
 */
DLL_HIDDEN extern IMathCellProcessors_t _framehash_framemath__iMathCellProcessors;
DLL_HIDDEN extern int64_t _framehash_framemath__mul( framehash_t * const self, double factor );
DLL_HIDDEN extern int64_t _framehash_framemath__add( framehash_t * const self, double value );
DLL_HIDDEN extern int64_t _framehash_framemath__sqrt( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_framemath__pow( framehash_t * const self, double exponent );
DLL_HIDDEN extern int64_t _framehash_framemath__log( framehash_t * const self, double base );
DLL_HIDDEN extern int64_t _framehash_framemath__exp( framehash_t * const self, double base );
DLL_HIDDEN extern int64_t _framehash_framemath__decay( framehash_t * const self, double exponent );
DLL_HIDDEN extern int64_t _framehash_framemath__set( framehash_t * const self, double value );
DLL_HIDDEN extern int64_t _framehash_framemath__randomize( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_framemath__int( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_framemath__float( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_framemath__abs( framehash_t * const self );
DLL_HIDDEN extern int64_t _framehash_framemath__sum( framehash_t * const self, double *sum );
DLL_HIDDEN extern int64_t _framehash_framemath__avg( framehash_t * const self, double *avg );
DLL_HIDDEN extern int64_t _framehash_framemath__stdev( framehash_t * const self, double *stdev );



/*******************************************************************//**
 * fhtest
 *
 ***********************************************************************
 */
DLL_EXPORT extern char ** _framehash_fhtest__get_unit_test_names( void );
DLL_HIDDEN extern int _framehash_fhtest__run_unit_tests( const char *runonly[], const char *testdir );



/* TEST DESCRIPTORS */
// Allocator
DLL_HIDDEN extern test_descriptor_t _framehash_memory_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_frameallocator_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_basementallocator_tests[];

// Process
DLL_HIDDEN extern test_descriptor_t _framehash_processor_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_framemath_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_delete_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_serialization_tests[];
#ifdef FRAMEHASH_CHANGELOG
DLL_HIDDEN extern test_descriptor_t _framehash_changelog_tests[];
#endif

// Radix
DLL_HIDDEN extern test_descriptor_t _framehash_hashing_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_leaf_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_basement_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_cache_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_radix_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_fmacro_tests[];

// API
DLL_HIDDEN extern test_descriptor_t _framehash_framehash_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_class_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_generic_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_object_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_int56_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_real56_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_pointer_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_info_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_iterator_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_manage_tests[];
DLL_HIDDEN extern test_descriptor_t _framehash_api_simple_tests[];

#endif
