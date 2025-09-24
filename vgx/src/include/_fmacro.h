/*
###################################################
#
# File:   fmacro.h
#
###################################################
*/

#ifndef FMACRO_H
#define FMACRO_H

#include "_fhbase.h"



#define USE_FUNCTION_FOR_CELL_ACCESS (!_FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS)


#define _FRAMEHASH_FORMAT_VERSION_1_0          0xFFFFFFFF20180001ULL
#define _FRAMEHASH_FORMAT_VERSION_1_1          0xFFFFFFFF20190011ULL

#define _FRAMEHASH_MAX_FILENO              ((1U << 16) - 1)      /* since fileno in framehash_metas_t has type uint16_t */

#define _FRAMEHASH_OFFSET_MASK             ( (1U << (_FRAMEHASH_P_MAX)) - 1 )

// TODO: These things seem to be tied to FRAMEHASH_CELLS_PER_SLOT = 4.
// Lots of things will break in strange places if we change it...

#define _CHAININDEX_SLOT_Q( ChainIndex )    ( (ChainIndex) >> 2 )             /*  qqqq--  =>  00qqqq    */
#define _CHAININDEX_CELL_J( ChainIndex )    ( (ChainIndex) & 0x3 )            /*  ----jj  =>  0000jj    */

#define _SLOT_Q_GET_CHAININDEX( Q )         ( (Q) << 2 )                      /*    qqqq  =>  qqqq00    */

#define _CACHE_FRAME_NSLOTS( Order_p )      ((uint16_t)( 1UL << (Order_p) ))  /* */
#define _CACHE_FRAME_INDEX_BITS( Order_p )  ( Order_p )                       /* */
#define _ZONE_INDEXMASK( Zone_k )           ( ( 1ULL << (Zone_k) ) - 1 )      /* */
#define _FRAME_NSLOTS( Order_p )            ( (1UL << (Order_p)) - 1 )        /* only counts slots of cells outside the header, i.e. the 2 header cells are NOT counted here */
#define _FRAME_CHAINZONE( Order_p )         ( (Order_p)-2 )                   /* zone k for chaining is the second largest, so k=(p-1)-1 */
#define _FRAME_NCHAINSLOTS( Order_p )       ( (1UL << ((Order_p)-2)) )        /* the number of chain slots in an internal frame */
#define _FRAME_INDEX_BITS( Order_p )        ( _FRAME_CHAINZONE(Order_p) + 2 ) /* Index bits for the chain zone (p-2) plus 2 bits for the 4 cells in slot ( log2(4)=2 ) */
#define _FRAME_ZONESLOTS( Zone_k )          ( 1UL << (Zone_k) )               /* */
#define _SHORTID( ShortIdPtr, ObidPtr )     ((ObidPtr) ? (ObidPtr)->L : *(ShortIdPtr))


typedef enum _e_framehash_celltype_t {
  //                                      // N D T  <= SEE ANNOTATED POINTER DEFINITION!
  CELL_TYPE_END                   = 0x0,  // 0 0 0  overloaded tag, shared with FRAME - distinguish by context
  CELL_TYPE_FRAME                 = 0x0,  // 0 0 0  overloaded tag, shared with END   - distinguish by context
  CELL_TYPE_BASEMENT              = 0x0,  // 0 0 0  overloaded tag, shared with END   - distinguish by context
  CELL_TYPE_OBJECT128             = 0x1,  // 0 0 1
  CELL_TYPE_DIRTY_INVALID_OBJECT  = 0x2,  // 0 1 0
  CELL_TYPE_DIRTY_OBJECT          = 0x3,  // 0 1 1
  CELL_TYPE_EMPTY                 = 0x4,  // 1 0 0
  CELL_TYPE_RAW56                 = 0x5,  // 1 0 1
  CELL_TYPE_DIRTY_INVALID_RAW56   = 0x6,  // 1 1 0
  CELL_TYPE_DIRTY_RAW56           = 0x7   // 1 1 1
} _framehash_celltype_t;






#define _PUSH_CONTEXT_FRAME( ContextPtr, FramePtr )   \
do {                                                  \
  framehash_context_t *__context__ = ContextPtr;     \
  framehash_cell_t *__frame__ = __context__->frame;   \
  __context__->frame = (FramePtr);                    \
  do /* recursion call here */

#define _POP_CONTEXT_FRAME        \
  WHILE_ZERO;                     \
  __context__->frame = __frame__; \
} WHILE_ZERO




#define _DEFINE_FUNCTION_POINTER_FROM_DATA( FunctionPointerType, VarName, SourcePtr )  \
  FunctionPointerType VarName;                                                  \
  SUPPRESS_WARNING_TYPE_CAST_DATA_TO_FROM_FUNCTION_POINTER                      \
  VarName = (FunctionPointerType)(SourcePtr)



extern void __CELL_ITEM_MATCH( const framehash_context_t * const context, const framehash_cell_t * const cell, bool * const ismatch );
extern bool __CELL_REFERENCE_MATCH( const framehash_cell_t * const cell, const void * const pointer );
extern bool __CELL_REFERENCE_IS_DIFFERENT( const framehash_cell_t * const cell, const void * const pointer );
extern bool __CELL_IS_DESTRUCTIBLE( const framehash_cell_t * const cell );
extern bool __CELL_OBJECT128_ID_MATCH( const framehash_cell_t * const cell, const objectid_t * const pobid );
extern void __SET_CELL_KEY( const framehash_context_t * const context, framehash_cell_t * const cell );
extern void __MARK_CACHED_NONEXIST( const framehash_context_t * const context, framehash_cell_t * const cell );
extern void __STORE_OBJECT128_POINTER( const framehash_context_t * const context, framehash_cell_t * const cell );
extern void __STORE_MEMBERSHIP( const framehash_context_t * const context, framehash_cell_t * const cell );
extern void __STORE_BOOLEAN( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __STORE_UNSIGNED( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __STORE_INTEGER( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __STORE_REAL( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __STORE_POINTER( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __STORE_OBJECT64_POINTER( const framehash_context_t * const context, framehash_cell_t *cell );
extern void __INITIALIZE_DATA_CELL( framehash_cell_t * const cell, const _framehash_celltype_t tag );
extern void __INITIALIZE_REFERENCE_CELL( framehash_cell_t * const cell, int order, int domain, framehash_ftype_t ftype );
extern void __SET_ITEM_TAG( framehash_cell_t * const cell, const _framehash_celltype_t tag );
extern void __SET_CELL_FROM_ELEMENTS( framehash_cell_t * const cell, const QWORD annotation, const QWORD data );
extern void __DESTROY_PREVIOUS_OBJECT128( const framehash_context_t * const context, framehash_cell_t * const cell );
DLL_EXPORT extern void __DELETE_ITEM( framehash_cell_t * const cell );
extern void __CELL_STEAL( framehash_cell_t * const dest, framehash_cell_t * const src );
extern objectid_t * __GET_CELL_OBJECT128_ID( framehash_cell_t * const cell );
extern void __LOAD_OBJECT128_POINTER_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_OBJECT128_POINTER_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_RAW56_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_RAW56_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_NULL_CONTEXT_VALUE( framehash_context_t * const context );
extern void __LOAD_CONTEXT_VALUE( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_CONTEXT_KEY( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_CONTEXT_VALUE_TYPE( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_CONTEXT_KEY_TYPE( framehash_context_t * const context, const framehash_cell_t * const cell );
extern void __LOAD_CONTEXT_FROM_CELL( framehash_context_t * const context, const framehash_cell_t * const cell );
extern uint16_t __CACHE_INCREMENT_NCHAINS( framehash_metas_t * const cache_metas );
extern uint16_t __CACHE_DECREMENT_NCHAINS( framehash_metas_t * const cache_metas );
extern bool __CACHE_HAS_NO_CHAINS( const framehash_metas_t * const cache_metas );
extern uint16_t __CACHE_SET_NCHAINS( framehash_metas_t * const cache_metas, const uint16_t nchains );
extern uint16_t __CACHE_GET_NCHAINS( const framehash_metas_t * const cache_metas );


framehash_metas_t * __CELL_GET_FRAME_METAS( const framehash_cell_t * const cell );
uint8_t __CELL_GET_FRAME_TYPE( const framehash_cell_t * const cell );
framehash_slot_t * __CELL_GET_FRAME_SLOTS( const framehash_cell_t * const cell );
framehash_cell_t * __CELL_GET_FRAME_CELLS( const framehash_cell_t * const cell );
void __DELETE_CELL_REFERENCE( framehash_cell_t * const cell );


/*******************************************************************//**
 * Let's make macros for the tricky annotated pointer stuff so we
 * don't get sneaky bugs in our code.
 *
 ***********************************************************************
 */



/************************/
/* CELL INTERPRETATIONS */
/************************/

/* Interpret the cell pointer as its data tag */
#define _CELL_TYPE( CellPtr )                       APTR_AS_TAG( CellPtr )

/* */
#define _CELL_VALUE_DTYPE( CellPtr )                APTR_AS_DTYPE( CellPtr )

/* */
#define _CELL_VALUE_DKEY( CellPtr )                 APTR_AS_DKEY( CellPtr )

/* Interpret the cell pointer as its annotation integer value */
#define _CELL_KEY( CellPtr )                        APTR_AS_ANNOTATION( CellPtr )

/* Interpret the cell pointer as its (hashed) annotation integer value */
#define _CELL_SHORTID( CellPtr )                    APTR_AS_ANNOTATION( CellPtr )

/* */
#define _CELL_GET_IDHIGH( CellPtr )                 APTR_GET_IDHIGH( CellPtr )

/* */
#define _CELL_GET_BOOLEAN( CellPtr )                APTR_GET_BOOLEAN( CellPtr )

/* */
#define _CELL_AS_UNSIGNED( CellPtr )                APTR_AS_UNSIGNED( CellPtr )

/* Interpret the cell pointer as its data element cast to real integer value (not a pointer) */
#define _CELL_AS_INTEGER( CellPtr )                 APTR_AS_INTEGER( CellPtr )

/* */
#define _CELL_GET_REAL( CellPtr )                   APTR_GET_REAL( CellPtr )

/* */
#define _CELL_GET_OBJECT64( CellPtr )               APTR_GET_OBJ56( CellPtr )

/* Interpret the cell reference as a pointer to an object */
#define _CELL_GET_OBJECT128( CellPtr )              ((comlib_object_t*)APTR_GET_POINTER( CellPtr ))

/* */
#define _CELL_SET_IDHIGH( CellPtr, IdHigh )         APTR_SET_IDHIGH( CellPtr, IdHigh )

/* */
#define _CELL_SET_BOOLEAN( CellPtr, Boolean )       APTR_SET_BOOLEAN( CellPtr, Boolean )

/* */
#define _CELL_SET_UNSIGNED( CellPtr, Unsigned )     APTR_SET_UNSIGNED( CellPtr, Unsigned )

/* */
#define _CELL_SET_INTEGER( CellPtr, Integer )       APTR_SET_INTEGER( CellPtr, Integer )

/* */
#define _CELL_SET_REAL( CellPtr, Real )             APTR_SET_REAL( CellPtr, Real )



/**************/
/* CELL TESTS */
/**************/

/* Evaluates to true if the cell holds no value but is not the end of probing either */
#define _CELL_IS_EMPTY( CellPtr )                   (_CELL_TYPE( CellPtr ) == CELL_TYPE_EMPTY)

/* Evaluates to true if the cell marks the end of probing */
#define _CELL_IS_END( CellPtr )                     (_CELL_TYPE( CellPtr ) == CELL_TYPE_END)

/* Evaluates to true is the cell holds an integer value */
#define _CELL_IS_VALUE( CellPtr )                   APTR_IS_NONPTR( CellPtr )

/* Evaluates to true is the cell is a pointer to something */
#define _CELL_IS_REFERENCE( CellPtr )               APTR_IS_POINTER( CellPtr )

/* True if the cell is a NULL pointer (disregarding any tag) */
#define _CELL_REFERENCE_IS_NULL( CellPtr )          (APTR_GET_POINTER( CellPtr ) == NULL)

/* True if the cell is not a NULL reference */
#define _CELL_REFERENCE_EXISTS( CellPtr )           (APTR_IS_POINTER( CellPtr ) && APTR_GET_POINTER( CellPtr ) != NULL)

/* Evaluates to true if the cell has been modified in a cache and not persisted do a subframe */
#define _ITEM_IS_DIRTY( CellPtr )                   APTR_IS_DIRTY( CellPtr )

/* Evaluates to true if the cell contains a valid object reference or integer, but not a reference to a subframe */
#define _ITEM_IS_VALID( CellPtr )                   APTR_IS_VALID( CellPtr )

/* Evaluates to true if the cell contains an invalid object reference or integer */
#define _ITEM_IS_INVALID( CellPtr )                 APTR_IS_INVALID( CellPtr )



#if USE_FUNCTION_FOR_CELL_ACCESS
/* Evaluates to true if the cell matches the probe given various probe criteria */
#define _CELL_ITEM_MATCH __CELL_ITEM_MATCH
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define __1HIT( MatchPtr )   { *(MatchPtr) = true; break; }
#define __0MISS( MatchPtr )  { *(MatchPtr) = false; break; }
#define _CELL_ITEM_MATCH( ContextPtr, CellPtr, MatchPtr )                                                   \
  do {                                                                                                      \
    switch( (ContextPtr)->ktype ) {                                                                         \
    case CELL_KEY_TYPE_PLAIN64:                                                                             \
      if( _CELL_KEY( CellPtr ) == (ContextPtr)->key.plain ) __1HIT(MatchPtr)              /* integer key match  */ \
      else __0MISS(MatchPtr)                                                              /* no match */    \
    case CELL_KEY_TYPE_HASH64:                                                                              \
      if( _CELL_SHORTID( CellPtr ) == (ContextPtr)->key.shortid ) __1HIT(MatchPtr)        /* 64-bit hash match */ \
      else __0MISS(MatchPtr)                                                              /* not match */   \
    case CELL_KEY_TYPE_HASH128:                                                                             \
      if( _CELL_SHORTID( CellPtr ) != (ContextPtr)->key.shortid ) __0MISS(MatchPtr)       /* idL miss, declare no match */ \
      /* Shortid match */                                                                                    \
      else {                                                                                                \
        /* Cell holds a plain value */                                                                      \
        if( _CELL_IS_VALUE( CellPtr ) ) {                                                                   \
          if( (ContextPtr)->obid == NULL ) __1HIT(MatchPtr)                               /* by definition, we allow match on shorter key when storing integer if probe's obid is NULL */ \
          else if( APTR_MATCH_IDHIGH( CellPtr, (ContextPtr)->obid->H ) ) __1HIT(MatchPtr) /* special case for negative cache hits (cached nonexist) and set memberships */ \
          else __0MISS(MatchPtr)                                                          /* probe's idH does not match the cell value */ \
        }                                                                                                   \
        /* Cell holds object */                                                                             \
        else {                                                                                              \
          if( (ContextPtr)->control.__expect_nonexist ) __0MISS(MatchPtr)                       /* collision protection during rehashing (shortid match but different object)  */  \
          else if ( (ContextPtr)->obid == NULL ) __1HIT(MatchPtr)                               /* by definition, we allow match on idL only if probe doesn't specify obid */ \
          else if( _CELL_OBJECT128_ID_MATCH( CellPtr, (ContextPtr)->obid ) )  __1HIT(MatchPtr)  /* idH verified in referenced object, match */ \
          else __0MISS(MatchPtr)                                                                /* object's idH does not match probe's idH, no match */ \
        }                                                                                                   \
      }                                                                                                     \
    default:                                                                                                \
      __0MISS(MatchPtr)                                                                   /* unknown key type, no match */ \
    }                                                                                                       \
  } WHILE_ZERO
#else
#error
#endif


/* True if the cell's pointer matches the given pointer */
#if USE_FUNCTION_FOR_CELL_ACCESS
#define _CELL_REFERENCE_MATCH __CELL_REFERENCE_MATCH
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CELL_REFERENCE_MATCH( CellPtr, Pointer ) ( \
  APTR_IS_POINTER( CellPtr )                        \
  &&                                                \
  APTR_MATCH_POINTER( CellPtr, Pointer )            \
)
#else
#error
#endif


/* True if the cell's pointer matches the given pointer */
#if USE_FUNCTION_FOR_CELL_ACCESS
#define _CELL_REFERENCE_IS_DIFFERENT __CELL_REFERENCE_IS_DIFFERENT
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CELL_REFERENCE_IS_DIFFERENT( CellPtr, Pointer ) (  \
  APTR_IS_POINTER( CellPtr )                                \
  &&                                                        \
  !APTR_MATCH_POINTER( CellPtr, Pointer )                   \
)
#else
#error
#endif


/* Evaluates to true if the cell contains an object that can be destroyed in the given context */
#if USE_FUNCTION_FOR_CELL_ACCESS
#define _CELL_IS_DESTRUCTIBLE __CELL_IS_DESTRUCTIBLE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CELL_IS_DESTRUCTIBLE( CellPtr ) (                              \
  _CELL_REFERENCE_EXISTS( CellPtr )                                     \
  &&                                                                    \
  COMLIB_OBJECT_DESTRUCTIBLE( _CELL_GET_OBJECT128( CellPtr ) )          \
)
#else
#error
#endif


/* */
#if USE_FUNCTION_FOR_CELL_ACCESS
#define _CELL_OBJECT128_ID_MATCH __CELL_OBJECT128_ID_MATCH
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CELL_OBJECT128_ID_MATCH( CellPtr, ObidPtr )  ( APTR_IS_NULL( CellPtr ) || SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER COMLIB_OBJECT_CMPID( _CELL_GET_OBJECT128( CellPtr ), ObidPtr ) == 0 )
#else
#error
#endif


/**************/
/* CELL MARKS */
/**************/

/* Set the cell's key */
#if USE_FUNCTION_FOR_CELL_ACCESS
#define _SET_CELL_KEY __SET_CELL_KEY
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _SET_CELL_KEY( ContextPtr, CellPtr )                      \
  do {                                                            \
    if( (ContextPtr)->ktype == CELL_KEY_TYPE_PLAIN64 )            \
      APTR_AS_ANNOTATION( CellPtr ) = (ContextPtr)->key.plain;    \
    else                                                          \
      APTR_AS_ANNOTATION( CellPtr ) = (ContextPtr)->key.shortid;  \
    APTR_AS_DKEY( CellPtr ) = (ContextPtr)->ktype;                \
  } WHILE_ZERO
#else
#error
#endif


/* Indicate that the cell has been modified in a cache */
#define _MARK_ITEM_DIRTY( CellPtr )                APTR_MAKE_DIRTY( CellPtr )

/* Indicate that the cell is deleted */
#define _MARK_ITEM_INVALID( CellPtr )              APTR_MAKE_INVALID( CellPtr )

/* Indicate that the cell is unmodified in a cache with respect to the cache's subframe */
#define _MARK_ITEM_CLEAN( CellPtr )                APTR_MAKE_CLEAN( CellPtr )

/* Indicate that the cell contains valid data */
#define _MARK_ITEM_VALID( CellPtr )                APTR_MAKE_VALID( CellPtr )

#if USE_FUNCTION_FOR_CELL_ACCESS
/* 
In caches we overload the EMPTY marker to indicate a non-existing item. A cache "hit" on a non-existing item will terminate
the search if the ID matches as follows: For shortid lookups a match on the annotation is enough to terminate search. For obid
lookups an additional match on idH is required. Since the obid is stored outside of framehash as part of the object we normally
dereference the cell's pointer, but for non-existing (and even deleted) objects we don't have (or can't trust) a pointer
to dereference. Instead, when marking objects as non-existing (or deleted) we repurpose the pointer part of the cell as plain
integer to store the 56 LSBs of the obid->H which we have (possibly last-time) access to at the point of deletion. The EMPTY tag
together with a non-zero annotation and non-zero integer value indicate that the 56 value bits should be interpreted as (partial) idH data.
*/
#define _MARK_CACHED_NONEXIST __MARK_CACHED_NONEXIST
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _MARK_CACHED_NONEXIST( ContextPtr, CellPtr )                            \
  do {                                                                          \
    _SET_CELL_KEY( ContextPtr, CellPtr );                                       \
    APTR_SET_IDHIGH( CellPtr, (ContextPtr)->obid ? (ContextPtr)->obid->H : 0 ); \
    APTR_AS_TAG( CellPtr ) = CELL_TYPE_EMPTY;                                   \
  } WHILE_ZERO
#else
#error
#endif


/***************/
/* CELL ACCESS */
/***************/

/* Set the cell reference to point to the given array, not touching the tag */
#define _SET_CELL_REFERENCE( CellPtr, Array )      APTR_SET_POINTER( CellPtr, Array )

/* Set all annotation and reference data in the destination cell to the same as the source cell */
#define _CELL_COPY( DestCellPtr, SrcCellPtr )      APTR_COPY( DestCellPtr, SrcCellPtr )


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store an object reference into a cell identified by a 64-bit ID */
#define _STORE_OBJECT128_POINTER __STORE_OBJECT128_POINTER
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_OBJECT128_POINTER( ContextPtr, CellPtr )           \
  do {                                                            \
    /* NOTE: objects can only use hashed id */                    \
    APTR_AS_ANNOTATION( CellPtr ) = (ContextPtr)->key.shortid;    \
    /* store the object reference */                              \
    APTR_SET_POINTER_AND_TAG( CellPtr, (ContextPtr)->value.pobject, CELL_TYPE_OBJECT128 );     \
  }  WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store the existence of something in cell */
#define _STORE_MEMBERSHIP __STORE_MEMBERSHIP
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_MEMBERSHIP( ContextPtr, CellPtr )              \
  do {                                                        \
    _SET_CELL_KEY( ContextPtr, CellPtr );                     \
    /* store membership high ID if any */                     \
    APTR_SET_IDHIGH_AND_TAG( CellPtr, (ContextPtr)->value.idH56, CELL_TYPE_RAW56 );    \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store an unsigned integer in a cell identified by a 64-bit ID */
#define _STORE_BOOLEAN __STORE_BOOLEAN
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_BOOLEAN( ContextPtr, CellPtr )                               \
  do {                                                                      \
    _SET_CELL_KEY( ContextPtr, CellPtr );                                   \
    /* store the boolean value */                                           \
    APTR_SET_BOOLEAN_AND_TAG( CellPtr, (ContextPtr)->value.raw56 ? true : false, CELL_TYPE_RAW56 );  \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store an unsigned integer in a cell identified by a 64-bit ID */
#define _STORE_UNSIGNED __STORE_UNSIGNED
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_UNSIGNED( ContextPtr, CellPtr )                \
  do {                                                        \
    _SET_CELL_KEY( ContextPtr, CellPtr );                     \
    /* store the unsigned integer value */                    \
    APTR_SET_UNSIGNED_AND_TAG( CellPtr, (ContextPtr)->value.raw56, CELL_TYPE_RAW56 );  \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store an integer in a cell identified by a 64-bit ID */
#define _STORE_INTEGER __STORE_INTEGER
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_INTEGER( ContextPtr, CellPtr )               \
  do {                                                      \
    _SET_CELL_KEY( ContextPtr, CellPtr );                   \
    /* store the integer value */                           \
    APTR_SET_INTEGER_AND_TAG( CellPtr, (ContextPtr)->value.raw56, CELL_TYPE_RAW56 ); \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store floating point value in a cell identified by a 64-bit ID */
#define _STORE_REAL __STORE_REAL
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_REAL( ContextPtr, CellPtr )                \
  do {                                                    \
    _SET_CELL_KEY( ContextPtr, CellPtr );                 \
    /* store the real value */                            \
    APTR_SET_REAL_AND_TAG( CellPtr, (ContextPtr)->value.real56, CELL_TYPE_RAW56 ); \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store data-encoded plain pointer value in a cell identified by a 64-bit ID */
#define _STORE_POINTER __STORE_POINTER
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_POINTER( ContextPtr, CellPtr )             \
  do {                                                    \
    _SET_CELL_KEY( ContextPtr, CellPtr );                 \
    /* store the plain pointer value */                   \
    APTR_SET_PTR56_AND_TAG( CellPtr, (ContextPtr)->value.ptr56, CELL_TYPE_RAW56 ); \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Store data-encoded object pointer value in a cell identified by a 64-bit ID */
#define _STORE_OBJECT64_POINTER __STORE_OBJECT64_POINTER
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _STORE_OBJECT64_POINTER( ContextPtr, CellPtr )            \
  do {                                                    \
    _SET_CELL_KEY( ContextPtr, CellPtr );                 \
    /* store the object pointer value */                  \
    APTR_SET_OBJ56_AND_TAG( CellPtr, (ContextPtr)->value.obj56, CELL_TYPE_RAW56 ); \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Set the data cell to an initial value */
#define _INITIALIZE_DATA_CELL __INITIALIZE_DATA_CELL
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _INITIALIZE_DATA_CELL( CellPtr, CellType )                \
  do {                                                            \
    APTR_AS_ANNOTATION( CellPtr ) = FRAMEHASH_KEY_NONE;           \
    APTR_AS_QWORD( CellPtr ) = (QWORD)(CellType) & TPTR_TAG_MASK; \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Set the reference cell to an initial value */
#define _INITIALIZE_REFERENCE_CELL __INITIALIZE_REFERENCE_CELL
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _INITIALIZE_REFERENCE_CELL( CellPtr, Order, Domain, FrameType )                     \
  do {                                                                                      \
    framehash_metas_t *metas = (framehash_metas_t*)APTR_GET_ANNOTATION_POINTER( CellPtr );  \
    metas->QWORD = 0;                                                                       \
    metas->order = (uint8_t)Order;                                                          \
    metas->domain = (uint8_t)Domain;                                                        \
    metas->ftype = (uint8_t)FrameType;                                                      \
    APTR_AS_QWORD( CellPtr ) = 0;                                                           \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
#define _SET_ITEM_TAG __SET_ITEM_TAG
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _SET_ITEM_TAG( CellPtr, Tag )     \
  do {                                    \
    /* set the tag back to something */   \
    APTR_AS_TAG( CellPtr ) = Tag;         \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _SET_CELL_FROM_ELEMENTS __SET_CELL_FROM_ELEMENTS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _SET_CELL_FROM_ELEMENTS( CellPtr, Annotation, Data )  \
  do {                                                        \
    APTR_AS_ANNOTATION( CellPtr ) = (QWORD)(Annotation);      \
    APTR_AS_QWORD( CellPtr ) = (uintptr_t)(Data);             \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Destroy the cell's object if it is in conflict with the supplied context, and evaluate to 0 when the cell no longer references a valid object */ \
#define _DESTROY_PREVIOUS_OBJECT128 __DESTROY_PREVIOUS_OBJECT128
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _DESTROY_PREVIOUS_OBJECT128( ContextPtr, CellPtr ) do {           \
  if (                                                                    \
    _CELL_IS_DESTRUCTIBLE( CellPtr )                                      \
    &&                                                                    \
    _CELL_REFERENCE_IS_DIFFERENT( CellPtr, (ContextPtr)->value.pobject )  \
  ) {                                                                     \
    /* object exists and we have a destructor: Destroy now */             \
    COMLIB_OBJECT_DESTROY( _CELL_GET_OBJECT128( CellPtr ) );              \
  }                                                                       \
} WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Remove a previously stored item */
// NOTE: Find a way to IMPORT this in other applications that need to use _framehash.h for inner access to stuff.
#define _DELETE_ITEM __DELETE_ITEM
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _DELETE_ITEM( CellPtr )                    \
  _INITIALIZE_DATA_CELL( CellPtr, CELL_TYPE_EMPTY )
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CELL_STEAL __CELL_STEAL
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CELL_STEAL( DestCellPtr, SrcCellPtr )  \
  do {                                          \
    APTR_COPY( DestCellPtr, SrcCellPtr );       \
    DELETE_CELL_REFERENCE( SrcCellPtr );       \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Produce a pointer to the object ID for the object pointed to by the cell reference */
#define _GET_CELL_OBJECT128_ID __GET_CELL_OBJECT128_ID
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _GET_CELL_OBJECT128_ID( CellPtr )  ( COMLIB_OBJECT_GETID( _CELL_GET_OBJECT128( CellPtr ) ) )
#else
#error
#endif



/*******************/
/* CONTEXT LOADING */
/*******************/

#if USE_FUNCTION_FOR_CELL_ACCESS
/* Interpret the cell data as a pointer to an object and set the context value to the object address */
#define _LOAD_OBJECT128_POINTER_CONTEXT_VALUE __LOAD_OBJECT128_POINTER_CONTEXT_VALUE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_OBJECT128_POINTER_CONTEXT_VALUE( ContextPtr, CellPtr )     \
  do {  /* NOTE: no streaming! (we assign address not data) */  \
    (ContextPtr)->value.pobject = APTR_GET_POINTER(CellPtr);    \
    (ContextPtr)->vtype = CELL_VALUE_TYPE_OBJECT128;              \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Interpret the cell data as a pointer to an object and set the context value to the object address */
#define _LOAD_OBJECT128_POINTER_CONTEXT_KEY __LOAD_OBJECT128_POINTER_CONTEXT_KEY
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_OBJECT128_POINTER_CONTEXT_KEY( ContextPtr, CellPtr )   \
  do {  /* NOTE: no streaming! (we assign address not data) */    \
    /* COMLIB pointers have only 3-bit tag and nothing to indicate DTYPE or DKEY like the RAW56 context */ \
    /* WARNING: This does not load the COMLIB OBJECT OBID into the context! */  \
    (ContextPtr)->ktype = CELL_KEY_TYPE_HASH128;     /* <- implied */   \
    (ContextPtr)->key.plain = FRAMEHASH_KEY_NONE;                 \
    (ContextPtr)->key.shortid = _CELL_SHORTID( CellPtr );         \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Interpret the cell data as an integer and set the context value to the integer value */
#define _LOAD_RAW56_CONTEXT_VALUE __LOAD_RAW56_CONTEXT_VALUE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_RAW56_CONTEXT_VALUE( ContextPtr, CellPtr )        \
  do {                                                          \
    switch( APTR_AS_DTYPE( CellPtr ) ) {                        \
    case TAGGED_DTYPE_ID56:                                     \
      (ContextPtr)->value.idH56 = APTR_GET_IDHIGH( CellPtr );   \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_MEMBER;             \
      break;                                                    \
    case TAGGED_DTYPE_BOOL:                                     \
      (ContextPtr)->value.raw56 = APTR_GET_BOOLEAN( CellPtr );  \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_BOOLEAN;            \
      break;                                                    \
    case TAGGED_DTYPE_UINT56:                                   \
      (ContextPtr)->value.raw56 = APTR_AS_UNSIGNED( CellPtr );  \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_UNSIGNED;           \
      break;                                                    \
    case TAGGED_DTYPE_INT56:                                    \
      (ContextPtr)->value.raw56 = APTR_AS_INTEGER( CellPtr );   \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_INTEGER;            \
      break;                                                    \
    case TAGGED_DTYPE_REAL56:                                   \
      (ContextPtr)->value.real56 = APTR_GET_REAL( CellPtr );    \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_REAL;               \
      break;                                                    \
    case TAGGED_DTYPE_PTR56:                                    \
      (ContextPtr)->value.ptr56 = APTR_GET_PTR56( CellPtr );    \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_POINTER;            \
      break;                                                    \
    case TAGGED_DTYPE_OBJ56:                                    \
      (ContextPtr)->value.obj56 = APTR_GET_OBJ56( CellPtr );    \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_OBJECT64;           \
      break;                                                    \
    default:                                                    \
      (ContextPtr)->value.qword = APTR_AS_QWORD( CellPtr );     \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_ERROR;              \
      break;                                                    \
    }                                                           \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* Interpret the cell data as a pointer to an object and set the context value to the object address */
#define _LOAD_RAW56_CONTEXT_KEY __LOAD_RAW56_CONTEXT_KEY
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_RAW56_CONTEXT_KEY( ContextPtr, CellPtr )                                      \
  do {                                                                                      \
    switch( APTR_AS_DKEY( CellPtr ) ) {                                                     \
    case TAGGED_DKEY_PLAIN64:                                                               \
      (ContextPtr)->key.plain = _CELL_KEY( CellPtr );                                       \
      (ContextPtr)->key.shortid = (ContextPtr)->dynamic->hashf( (ContextPtr)->key.plain );  \
      (ContextPtr)->ktype = CELL_KEY_TYPE_PLAIN64;                                          \
      break;                                                                                \
    case TAGGED_DKEY_HASH64:                                                                \
      (ContextPtr)->key.plain = FRAMEHASH_KEY_NONE;                                         \
      (ContextPtr)->key.shortid = _CELL_SHORTID( CellPtr );                                 \
      (ContextPtr)->ktype = CELL_KEY_TYPE_HASH64;                                           \
      break;                                                                                \
    default:                                                                                \
      (ContextPtr)->key.plain = FRAMEHASH_KEY_NONE;                                         \
      if( (ContextPtr)->vtype == CELL_VALUE_TYPE_MEMBER ) {                                 \
        (ContextPtr)->key.shortid = _CELL_SHORTID( CellPtr );                               \
        (ContextPtr)->ktype = CELL_KEY_TYPE_HASH128;                                        \
      }                                                                                     \
      /* invalid data */                                                                    \
      else {                                                                                \
        (ContextPtr)->key.shortid = 0;                                                      \
        (ContextPtr)->ktype = CELL_KEY_TYPE_ERROR;                                          \
      }                                                                                     \
      break;                                                                                \
    }                                                                                       \
  } WHILE_ZERO
#else
#error
#endif


/*******************************************************************//**
 * _LOAD_RAW56_CONTEXT_KEY
 *
 ***********************************************************************
 */


#if USE_FUNCTION_FOR_CELL_ACCESS
#define _LOAD_NULL_CONTEXT_VALUE __LOAD_NULL_CONTEXT_VALUE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_NULL_CONTEXT_VALUE( ContextPtr )        \
  do {                                          \
    (ContextPtr)->value.pobject = NULL;         \
    (ContextPtr)->vtype = CELL_VALUE_TYPE_NULL; \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _LOAD_CONTEXT_VALUE __LOAD_CONTEXT_VALUE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_CONTEXT_VALUE( ContextPtr, CellPtr )                \
  do {                                                            \
    if( _CELL_IS_REFERENCE( CellPtr ) ) {                         \
      _LOAD_OBJECT128_POINTER_CONTEXT_VALUE( ContextPtr, CellPtr );  \
    }                                                             \
    else {                                                        \
      _LOAD_RAW56_CONTEXT_VALUE( ContextPtr, CellPtr );           \
    }                                                             \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _LOAD_CONTEXT_KEY __LOAD_CONTEXT_KEY
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_CONTEXT_KEY( ContextPtr, CellPtr )              \
  if( _CELL_IS_REFERENCE( CellPtr ) ) {                       \
    _LOAD_OBJECT128_POINTER_CONTEXT_KEY( ContextPtr, CellPtr );  \
  }                                                           \
  else {                                                      \
    _LOAD_RAW56_CONTEXT_KEY( ContextPtr, CellPtr );           \
  }                                                                                     
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _LOAD_CONTEXT_VALUE_TYPE __LOAD_CONTEXT_VALUE_TYPE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_CONTEXT_VALUE_TYPE( ContextPtr, CellPtr )     \
  do {                                                      \
    if( _CELL_IS_REFERENCE( CellPtr ) ) {                   \
      (ContextPtr)->vtype = CELL_VALUE_TYPE_OBJECT128;      \
    }                                                       \
    else {                                                  \
      switch( APTR_AS_DTYPE( CellPtr ) ) {                  \
      case TAGGED_DTYPE_ID56:                               \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_MEMBER;       \
        break;                                              \
      case TAGGED_DTYPE_BOOL:                               \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_BOOLEAN;      \
        break;                                              \
      case TAGGED_DTYPE_UINT56:                             \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_UNSIGNED;     \
        break;                                              \
      case TAGGED_DTYPE_INT56:                              \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_INTEGER;      \
        break;                                              \
      case TAGGED_DTYPE_REAL56:                             \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_REAL;         \
        break;                                              \
      case TAGGED_DTYPE_PTR56:                              \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_POINTER;      \
        break;                                              \
      case TAGGED_DTYPE_OBJ56:                              \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_OBJECT64;     \
        break;                                              \
      default:                                              \
        (ContextPtr)->vtype = CELL_VALUE_TYPE_ERROR;        \
        break;                                              \
      }                                                     \
    }                                                       \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _LOAD_CONTEXT_KEY_TYPE __LOAD_CONTEXT_KEY_TYPE
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_CONTEXT_KEY_TYPE( ContextPtr, CellPtr )     \
  do {                                                    \
    if( _CELL_IS_REFERENCE( CellPtr ) ) {                 \
      (ContextPtr)->ktype = CELL_KEY_TYPE_HASH128;        \
    }                                                     \
    else {                                                \
      (ContextPtr)->ktype = _CELL_VALUE_DKEY( CellPtr );  \
    }                                                     \
  } WHILE_ZERO
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _LOAD_CONTEXT_FROM_CELL __LOAD_CONTEXT_FROM_CELL
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _LOAD_CONTEXT_FROM_CELL( ContextPtr, CellPtr )  \
  do {                                                  \
    _LOAD_CONTEXT_VALUE( ContextPtr, CellPtr );         \
    _LOAD_CONTEXT_KEY( ContextPtr, CellPtr );           \
  } WHILE_ZERO
#else
#error
#endif



/********************/
/* COUNT MANAGEMENT */
/********************/


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CACHE_INCREMENT_NCHAINS __CACHE_INCREMENT_NCHAINS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CACHE_INCREMENT_NCHAINS( CacheMetasPtr )    ((CacheMetasPtr)->nchains++)
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CACHE_DECREMENT_NCHAINS __CACHE_DECREMENT_NCHAINS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CACHE_DECREMENT_NCHAINS( CacheMetasPtr )    ((CacheMetasPtr)->nchains--)
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CACHE_HAS_NO_CHAINS __CACHE_HAS_NO_CHAINS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CACHE_HAS_NO_CHAINS( CacheMetasPtr )    ( (CacheMetasPtr)->nchains == 0 ? true : false )
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CACHE_SET_NCHAINS __CACHE_SET_NCHAINS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CACHE_SET_NCHAINS( CacheMetasPtr, Nchains )    ((CacheMetasPtr)->nchains = (Nchains))
#else
#error
#endif


#if USE_FUNCTION_FOR_CELL_ACCESS
/* */
#define _CACHE_GET_NCHAINS __CACHE_GET_NCHAINS
#elif _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS
#define _CACHE_GET_NCHAINS( CacheMetasPtr )    ((CacheMetasPtr)->nchains)
#else
#error
#endif




#endif




