/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    comlibsequence_defs.h
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

#ifndef COMLIB_COMLIBSEQUENCE_DEFS_H
#define COMLIB_COMLIBSEQUENCE_DEFS_H

#include "cxlib.h"

/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

#define COMLIB_SEQUENCE_METHOD_SETCAPACITY( MethodName, TypeName )            int (* MethodName)( struct s_##TypeName##_t *self, int64_t element_capacity )
#define COMLIB_SEQUENCE_METHOD_ATTACHINPUTDESCRIPTOR( MethodName, TypeName )  int (* MethodName)( struct s_##TypeName##_t *self, short fd )
#define COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTDESCRIPTOR( MethodName, TypeName ) int (* MethodName)( struct s_##TypeName##_t *self, short fd )
#define COMLIB_SEQUENCE_METHOD_ATTACHINPUTSTREAM( MethodName, TypeName )      int (* MethodName)( struct s_##TypeName##_t *self, const char *fpath )
#define COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTSTREAM( MethodName, TypeName )     int (* MethodName)( struct s_##TypeName##_t *self, const char *fpath )
#define COMLIB_SEQUENCE_METHOD_DETACHINPUT( MethodName, TypeName )            int (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_DETACHOUTPUT( MethodName, TypeName )           int (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_DETACHOUTPUTNOFLUSH( MethodName, TypeName )    int (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_HASINPUTDESCRIPTOR( MethodName, TypeName )     int (* MethodName)( const struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_HASOUTPUTDESCRIPTOR( MethodName, TypeName )    int (* MethodName)( const struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_CAPACITY( MethodName, TypeName )               int64_t (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_LENGTH( MethodName, TypeName )                 int64_t (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_REMAIN( MethodName, TypeName )                 int64_t (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_INDEX( MethodName, TypeName, ElemType )        int64_t (* MethodName)( struct s_##TypeName##_t *self, const ElemType *probe, int64_t plen )
#define COMLIB_SEQUENCE_METHOD_OCC( MethodName, TypeName, ElemType )          int64_t (* MethodName)( struct s_##TypeName##_t *self, const ElemType *probe, int64_t plen )
#define COMLIB_SEQUENCE_METHOD_PEEK( MethodName, TypeName )                   int64_t (* MethodName)( struct s_##TypeName##_t *self, void **dest, int64_t element_index, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_EXPECT( MethodName, TypeName, ElemType )       bool (* MethodName)( struct s_##TypeName##_t *self, const ElemType *probe, int64_t plen, int64_t element_index )
#define COMLIB_SEQUENCE_METHOD_INITIALIZE( MethodName, TypeName, ElemType )   int64_t (* MethodName)( struct s_##TypeName##_t *self, const ElemType *value, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_DEADSPACE( MethodName, TypeName )              int64_t (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_WRITE( MethodName, TypeName, ElemType )        int64_t (* MethodName)( struct s_##TypeName##_t *self, const ElemType *elements, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_SET( MethodName, TypeName, ElemType )          int (* MethodName)( struct s_##TypeName##_t *self, int64_t index, const ElemType *item )
#define COMLIB_SEQUENCE_METHOD_APPEND( MethodName, TypeName, ElemType )       int (* MethodName)( struct s_##TypeName##_t *self, const ElemType *item )
#define COMLIB_SEQUENCE_METHOD_FORMAT( MethodName, TypeName )                 int64_t (* MethodName)( struct s_##TypeName##_t *self, const char *format, ... )
#define COMLIB_SEQUENCE_METHOD_NULTERM( MethodName, TypeName )                int64_t (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_NEXT( MethodName, TypeName, ElemType )         int (* MethodName)( struct s_##TypeName##_t *self, ElemType *dest )
#define COMLIB_SEQUENCE_METHOD_READ( MethodName, TypeName )                   int64_t (* MethodName)( struct s_##TypeName##_t *self, void **dest, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_CURSOR( MethodName, TypeName, ElemType )       ElemType * (* MethodName)( struct s_##TypeName##_t *self, int64_t index )
#define COMLIB_SEQUENCE_METHOD_GET( MethodName, TypeName, ElemType )          int (* MethodName)( struct s_##TypeName##_t *self, int64_t index, ElemType *dest )
#define COMLIB_SEQUENCE_METHOD_READUNTIL( MethodName, TypeName, ElemType )    int64_t (* MethodName)( struct s_##TypeName##_t *self, void **dest, const ElemType *probe, int64_t plen, int exclude_probe )
#define COMLIB_SEQUENCE_METHOD_READLINE( MethodName, TypeName, ElemType )     int64_t (* MethodName)( struct s_##TypeName##_t *self, void **dest )
#define COMLIB_SEQUENCE_METHOD_UNREAD( MethodName, TypeName )                 void (* MethodName)( struct s_##TypeName##_t *self, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_TRUNCATE( MethodName, TypeName )               int64_t (* MethodName)( struct s_##TypeName##_t *self, int64_t tail_index )
#define COMLIB_SEQUENCE_METHOD_CLEAR( MethodName, TypeName )                  void (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_RESET( MethodName, TypeName )                  void (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_GETVALUE( MethodName, TypeName )               int64_t (* MethodName)( struct s_##TypeName##_t *self, void **dest )
#define COMLIB_SEQUENCE_METHOD_ABSORB( MethodName, TypeName )                 int64_t (* MethodName)( struct s_##TypeName##_t *self, struct s_##TypeName##_t *other, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( MethodName, TypeName, ElemType )  int64_t (* MethodName)( struct s_##TypeName##_t *self, struct s_##TypeName##_t *other, const ElemType *probe, int64_t plen_or_elemcount, int exclude_probe )
#define COMLIB_SEQUENCE_METHOD_CLONEINTO( MethodName, TypeName, ElemType )    int64_t (* MethodName)( struct s_##TypeName##_t *self, struct s_##TypeName##_t *other )
#define COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( MethodName, TypeName )         int64_t (* MethodName)( struct s_##TypeName##_t *self, struct s_##TypeName##_t *other )
#define COMLIB_SEQUENCE_METHOD_YANKBUFFER( MethodName, TypeName, ElemType )   ElemType * (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_HEAPIFY( MethodName, TypeName )                void (* MethodName)( struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_HEAPTOP( MethodName, TypeName, ElemType )      int (* MethodName)( struct s_##TypeName##_t *self, ElemType *dest )
#define COMLIB_SEQUENCE_METHOD_HEAPPUSH( MethodName, TypeName, ElemType )     int (* MethodName)( struct s_##TypeName##_t *self, const ElemType *elem )
#define COMLIB_SEQUENCE_METHOD_HEAPPOP( MethodName, TypeName, ElemType )      int (* MethodName)( struct s_##TypeName##_t *self, ElemType *dest ) 
#define COMLIB_SEQUENCE_METHOD_HEAPREPLACE( MethodName, TypeName, ElemType )  int (* MethodName)( struct s_##TypeName##_t *self, ElemType *popped, ElemType *pushed ) 
#define COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( MethodName, TypeName, ElemType ) ElemType * (* MethodName)( struct s_##TypeName##_t *self, const ElemType *candidate, ElemType *discarded ) 
#define COMLIB_SEQUENCE_METHOD_DISCARD( MethodName, TypeName )                void (* MethodName)( struct s_##TypeName##_t *self, int64_t element_count )
#define COMLIB_SEQUENCE_METHOD_UNWRITE( MethodName, TypeName )                int64_t (* MethodName)( struct s_##TypeName##_t *self, int64_t element_count ) 
#define COMLIB_SEQUENCE_METHOD_POP( MethodName, TypeName, ElemType )          int (* MethodName)( struct s_##TypeName##_t *self, ElemType *dest ) 
#define COMLIB_SEQUENCE_METHOD_DUMP( MethodName, TypeName )                   int64_t (* MethodName)( const struct s_##TypeName##_t *self )
#define COMLIB_SEQUENCE_METHOD_FLUSH( MethodName, TypeName )                  int64_t (* MethodName)( struct s_##TypeName##_t *self ) 
#define COMLIB_SEQUENCE_METHOD_SORT( MethodName, TypeName )                   bool (* MethodName)( struct s_##TypeName##_t *self ) 
#define COMLIB_SEQUENCE_METHOD_REVERSE( MethodName, TypeName )                bool (* MethodName)( struct s_##TypeName##_t *self ) 
#define COMLIB_SEQUENCE_METHOD_OPTIMIZE( MethodName, TypeName )               int64_t (* MethodName)( struct s_##TypeName##_t *self ) 
#define COMLIB_SEQUENCE_METHOD_GETERROR( MethodName, TypeName )               const char *(* MethodName)( struct s_##TypeName##_t *self )


#define COMLIB_SEQUENCE_ELEMENT_COMPARATOR( TypeName, ElemType )              int (* f_##TypeName##_comparator_t)( const ElemType *e1, const ElemType *e2 )





#define COMLIB_SEQUENCE_GENERIC_QUEUE_CLASS( ClassName, UnitType )                                              \
                                                                                                                \
/* COMLIB SEQUENCE CIRCULAR BUFFER OBJECT FOR GENERIC QUEUE */                                                  \
typedef struct s_##ClassName##_t {                                                                              \
  /* cacheline #1 */                                                                                            \
  COMLIB_OBJECT_HEAD( ClassName##_vtable_t )                                                                    \
  int64_t _size;                        /* current number of elements in circular buffer                    */  \
  UnitType *_wp;                        /* element write pointer                                            */  \
  UnitType *_rp;                        /* element read pointer                                             */  \
  UnitType *_buffer;                    /* the circular element buffer                                      */  \
  uintptr_t _mask;                      /* address mask for aligned data range                              */  \
  short _fd_in;                         /* input file descriptor when streams are in use                    */  \
  short _fd_out;                        /* output file descriptor when streams are in use                   */  \
  struct {                                                                                                      \
    unsigned char _capacity_exp;        /* current buffer capacity exponent (capacity = 2 ** _capacity_exp) */  \
    unsigned char _initial_exp;         /* the initial buffer capacity exponent                             */  \
    unsigned char _load;                /* for capacity maintenance                                         */  \
    struct {                                                                                                    \
      unsigned char owns_fd_in    : 1;  /* true if sequence owns file                                       */  \
      unsigned char owns_fd_out   : 1;  /* true if sequence owns file                                       */  \
      unsigned char __rsv__       : 6;  /*                                                                  */  \
    } _flags;                                                                                                   \
  };                                                                                                            \
                                                                                                                \
  /* cacheline #2 */                                                                                            \
  CS_LOCK _lock;                        /* (forced to 64 bytes on all platforms)                            */  \
  /* cacheline #3 */                                                                                            \
  UnitType *_prev_rp;              /* previous read pointer (where it was prior to last read operation)     */  \
  int64_t _prev_fpos;                   /* previous input file position                                     */  \
  char *_error;                         /* last error string, if any                                        */  \
  int (*_cmp)( const UnitType *a, const UnitType *b);   /* comparator function for sort order */                \
} ClassName##_t;                                                                                                \


#define X_COMLIB_SEQUENCE_GENERIC_QUEUE_CLASS( ClassName, UnitType )    COMLIB_SEQUENCE_GENERIC_QUEUE_CLASS( ClassName, UnitType )



#define COMLIB_SEQUENCE_GENERIC_BUFFER_CLASS( ClassName, UnitType )                                             \
                                                                                                                \
/* COMLIB SEQUENCE CIRCULAR BUFFER OBJECT FOR GENERIC BUFFER */                                                 \
typedef struct s_##ClassName##_t {                                                                              \
  /* cacheline #1 */                                                                                            \
  COMLIB_OBJECT_HEAD( ClassName##_vtable_t )                                                                    \
  int64_t _size;                        /* current number of elements in circular buffer                    */  \
  UnitType *_wp;                        /* element write pointer                                            */  \
  UnitType *_rp;                        /* element read pointer                                             */  \
  UnitType *_buffer;                    /* the circular element buffer                                      */  \
  uintptr_t _mask;                      /* address mask for aligned data range                              */  \
  struct {                                                                                                      \
    unsigned char _capacity_exp;        /* current buffer capacity exponent (capacity = 2 ** _capacity_exp) */  \
    unsigned char _initial_exp;         /* the initial buffer capacity exponent                             */  \
    unsigned char _load;                /* for capacity maintenance                                         */  \
    unsigned char _rsv[5];                                                                                      \
  };                                                                                                            \
} ClassName##_t;                                                                                                \


#define X_COMLIB_SEQUENCE_GENERIC_BUFFER_CLASS( ClassName, UnitType )    COMLIB_SEQUENCE_GENERIC_BUFFER_CLASS( ClassName, UnitType )



#define COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( ClassName, UnitType )                                              \
/* COMLIB SEQUENCE LINEAR BUFFER OBJECT */                                                                      \
typedef struct s_##ClassName##_t {                                                                              \
  /* cacheline #1 */                                                                                            \
  COMLIB_OBJECT_HEAD( ClassName##_vtable_t )                                                                    \
  int64_t _size;                        /* current number of elements in buffer                             */  \
  UnitType *_wp;                        /* element write pointer                                            */  \
  UnitType *_rp;                        /* element read pointer                                             */  \
  UnitType *_buffer;                    /* the linear element buffer                                        */  \
  struct {                                                                                                      \
    unsigned char _capacity_exp;        /* current buffer capacity exponent (capacity = 2 ** _capacity_exp) */  \
    unsigned char _initial_exp;         /* the initial buffer capacity exponent                             */  \
    unsigned char _load;                /* for capacity maintenance                                         */  \
    unsigned char _rsv[5];                                                                                      \
  };                                                                                                            \
  int (*_cmp)( const UnitType *a, const UnitType *b);   /* comparator function for sort order */                \
} ClassName##_t;                                                                                                \


#define X_COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( ClassName, UnitType )    COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( ClassName, UnitType )



#define ComlibSequenceLength( Obj ) (Obj)->_size



/*******************************************************************//**
 * ComlibQueue TEMPLATE
 * 
 ***********************************************************************
 */
#define COMLIB_GENERIC_QUEUE_TYPEDEFS( QueueTypeName, QueueUnitType )                                           \
                                                                                                                \
struct s_##QueueTypeName##_t;                                                                                   \
                                                                                                                \
/* COMLIB QUEUE CONSTRUCTOR */                                                                                  \
typedef struct s_##QueueTypeName##_constructor_args_t {                                                         \
  int64_t element_capacity;                                                                                     \
  int (*comparator)( const QueueUnitType *a, const QueueUnitType *b);                                           \
} QueueTypeName##_constructor_args_t;                                                                           \
                                                                                                                \
                                                                                                                \
/* COMLIB GENERIC QUEUE VTABLE*/                                                                                \
typedef struct s_##QueueTypeName##_vtable_t {                                                                   \
  /* vtable head */                                                                                             \
  COMLIB_VTABLE_HEAD                                                                                            \
                                                                                                                \
  /* interface for those who like a pseudo object-oriented approach */                                          \
  COMLIB_SEQUENCE_METHOD_SETCAPACITY( SetCapacity, QueueTypeName );                                             \
                                                                                                                \
  /* IO Stream Methods */                                                                                       \
  COMLIB_SEQUENCE_METHOD_ATTACHINPUTDESCRIPTOR( AttachInputDescriptor, QueueTypeName );                         \
  COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTDESCRIPTOR( AttachOutputDescriptor, QueueTypeName );                       \
  COMLIB_SEQUENCE_METHOD_ATTACHINPUTSTREAM( AttachInputStream, QueueTypeName );                                 \
  COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTSTREAM( AttachOutputStream, QueueTypeName );                               \
  COMLIB_SEQUENCE_METHOD_DETACHINPUT( DetachInput, QueueTypeName );                                             \
  COMLIB_SEQUENCE_METHOD_DETACHOUTPUT( DetachOutput, QueueTypeName );                                           \
  COMLIB_SEQUENCE_METHOD_DETACHOUTPUTNOFLUSH( DetachOutputNoFlush, QueueTypeName );                             \
  COMLIB_SEQUENCE_METHOD_HASINPUTDESCRIPTOR( HasInputDescriptor, QueueTypeName );                               \
  COMLIB_SEQUENCE_METHOD_HASOUTPUTDESCRIPTOR( HasOutputDescriptor, QueueTypeName );                             \
                                                                                                                \
  COMLIB_SEQUENCE_METHOD_CAPACITY( Capacity, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_LENGTH( Length, QueueTypeName );                                                       \
  COMLIB_SEQUENCE_METHOD_REMAIN( Remain, QueueTypeName );                                                       \
                                                                                                                \
  /* Nolock versions */                                                                                         \
  COMLIB_SEQUENCE_METHOD_INDEX( IndexNolock, QueueTypeName, QueueUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_OCC( OccNolock, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_PEEK( PeekNolock, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_EXPECT( ExpectNolock, QueueTypeName, QueueUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_INITIALIZE( InitializeNolock, QueueTypeName, QueueUnitType );                          \
  COMLIB_SEQUENCE_METHOD_WRITE( WriteNolock, QueueTypeName, QueueUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_SET( SetNolock, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_APPEND( AppendNolock, QueueTypeName, QueueUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_FORMAT( FormatNolock, QueueTypeName );                                                 \
  COMLIB_SEQUENCE_METHOD_NULTERM( NulTermNolock, QueueTypeName );                                               \
  COMLIB_SEQUENCE_METHOD_NEXT( NextNolock, QueueTypeName, QueueUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_READ( ReadNolock, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_GET( GetNolock, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_READUNTIL( ReadUntilNolock, QueueTypeName, QueueUnitType );                            \
  COMLIB_SEQUENCE_METHOD_READLINE( ReadlineNolock, QueueTypeName, QueueUnitType );                              \
  COMLIB_SEQUENCE_METHOD_UNREAD( UnreadNolock, QueueTypeName );                                                 \
  COMLIB_SEQUENCE_METHOD_TRUNCATE( TruncateNolock, QueueTypeName );                                             \
  COMLIB_SEQUENCE_METHOD_CLEAR( ClearNolock, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_RESET( ResetNolock, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_GETVALUE( GetValueNolock, QueueTypeName );                                             \
  COMLIB_SEQUENCE_METHOD_ABSORB( AbsorbNolock, QueueTypeName );                                                 \
  COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( AbsorbUntilNolock, QueueTypeName, QueueUnitType );                        \
  COMLIB_SEQUENCE_METHOD_CLONEINTO( CloneIntoNolock, QueueTypeName, QueueUnitType );                            \
  COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( TransplantFromNolock, QueueTypeName );                                 \
  COMLIB_SEQUENCE_METHOD_HEAPIFY( HeapifyNolock, QueueTypeName );                                               \
  COMLIB_SEQUENCE_METHOD_HEAPTOP( HeapTopNolock, QueueTypeName, QueueUnitType );                                \
  COMLIB_SEQUENCE_METHOD_HEAPPUSH( HeapPushNolock, QueueTypeName, QueueUnitType );                              \
  COMLIB_SEQUENCE_METHOD_HEAPPOP( HeapPopNolock, QueueTypeName, QueueUnitType );                                \
  COMLIB_SEQUENCE_METHOD_HEAPREPLACE( HeapReplaceNolock, QueueTypeName, QueueUnitType );                        \
  COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( HeapPushTopKNolock, QueueTypeName, QueueUnitType );                      \
  COMLIB_SEQUENCE_METHOD_DISCARD( DiscardNolock, QueueTypeName );                                               \
  COMLIB_SEQUENCE_METHOD_UNWRITE( UnwriteNolock, QueueTypeName );                                               \
  COMLIB_SEQUENCE_METHOD_POP( PopNolock, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_DUMP( DumpNolock, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_FLUSH( FlushNolock, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_SORT( SortNolock, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_REVERSE( ReverseNolock, QueueTypeName );                                               \
  COMLIB_SEQUENCE_METHOD_OPTIMIZE( OptimizeNolock, QueueTypeName );                                             \
                                                                                                                \
  /* Standard versions */                                                                                       \
  COMLIB_SEQUENCE_METHOD_INDEX( Index, QueueTypeName, QueueUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_OCC( Occ, QueueTypeName, QueueUnitType );                                              \
  COMLIB_SEQUENCE_METHOD_PEEK( Peek, QueueTypeName );                                                           \
  COMLIB_SEQUENCE_METHOD_EXPECT( Expect, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_INITIALIZE( Initialize, QueueTypeName, QueueUnitType );                                \
  COMLIB_SEQUENCE_METHOD_WRITE( Write, QueueTypeName, QueueUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_SET( Set, QueueTypeName, QueueUnitType );                                              \
  COMLIB_SEQUENCE_METHOD_APPEND( Append, QueueTypeName, QueueUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_FORMAT( Format, QueueTypeName );                                                       \
  COMLIB_SEQUENCE_METHOD_NULTERM( NulTerm, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_NEXT( Next, QueueTypeName, QueueUnitType );                                            \
  COMLIB_SEQUENCE_METHOD_READ( Read, QueueTypeName );                                                           \
  COMLIB_SEQUENCE_METHOD_GET( Get, QueueTypeName, QueueUnitType );                                              \
  COMLIB_SEQUENCE_METHOD_READUNTIL( ReadUntil, QueueTypeName, QueueUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_READLINE( Readline, QueueTypeName, QueueUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_UNREAD( Unread, QueueTypeName );                                                       \
  COMLIB_SEQUENCE_METHOD_TRUNCATE( Truncate, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_CLEAR( Clear, QueueTypeName );                                                         \
  COMLIB_SEQUENCE_METHOD_RESET( Reset, QueueTypeName );                                                         \
  COMLIB_SEQUENCE_METHOD_GETVALUE( GetValue, QueueTypeName );                                                   \
  COMLIB_SEQUENCE_METHOD_ABSORB( Absorb, QueueTypeName );                                                       \
  COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( AbsorbUntil, QueueTypeName, QueueUnitType );                              \
  COMLIB_SEQUENCE_METHOD_CLONEINTO( CloneInto, QueueTypeName, QueueUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( TransplantFrom, QueueTypeName );                                       \
  COMLIB_SEQUENCE_METHOD_HEAPIFY( Heapify, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_HEAPTOP( HeapTop, QueueTypeName, QueueUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_HEAPPUSH( HeapPush, QueueTypeName, QueueUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_HEAPPOP( HeapPop, QueueTypeName, QueueUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_HEAPREPLACE( HeapReplace, QueueTypeName, QueueUnitType );                              \
  COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( HeapPushTopK, QueueTypeName, QueueUnitType );                            \
  COMLIB_SEQUENCE_METHOD_DISCARD( Discard, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_UNWRITE( Unwrite, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_POP( Pop, QueueTypeName, QueueUnitType );                                              \
  COMLIB_SEQUENCE_METHOD_DUMP( Dump, QueueTypeName );                                                           \
  COMLIB_SEQUENCE_METHOD_FLUSH( Flush, QueueTypeName );                                                         \
  COMLIB_SEQUENCE_METHOD_SORT( Sort, QueueTypeName );                                                           \
  COMLIB_SEQUENCE_METHOD_REVERSE( Reverse, QueueTypeName );                                                     \
  COMLIB_SEQUENCE_METHOD_OPTIMIZE( Optimize, QueueTypeName );                                                   \
                                                                                                                \
  /* misc */                                                                                                    \
  COMLIB_SEQUENCE_METHOD_GETERROR( GetError, QueueTypeName );                                                   \
} QueueTypeName##_vtable_t;                                                                                     \
                                                                                                                \
                                                                                                                \
                                                                                                                \
/* API function pointer definitions */                                                                          \
typedef COMLIB_SEQUENCE_METHOD_SETCAPACITY( f_##QueueTypeName##_setcapacity, QueueTypeName );                   \
typedef COMLIB_SEQUENCE_METHOD_ATTACHINPUTDESCRIPTOR( f_##QueueTypeName##_attachinputdescriptor, QueueTypeName );   \
typedef COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTDESCRIPTOR( f_##QueueTypeName##_attachoutputdescriptor, QueueTypeName ); \
typedef COMLIB_SEQUENCE_METHOD_ATTACHINPUTSTREAM( f_##QueueTypeName##_attachinputstream, QueueTypeName );       \
typedef COMLIB_SEQUENCE_METHOD_ATTACHOUTPUTSTREAM( f_##QueueTypeName##_attachoutputstream, QueueTypeName );     \
typedef COMLIB_SEQUENCE_METHOD_DETACHINPUT( f_##QueueTypeName##_detachinput, QueueTypeName );                   \
typedef COMLIB_SEQUENCE_METHOD_DETACHOUTPUT( f_##QueueTypeName##_detachoutput, QueueTypeName );                 \
typedef COMLIB_SEQUENCE_METHOD_DETACHOUTPUTNOFLUSH( f_##QueueTypeName##_detachoutputnoflush, QueueTypeName );   \
typedef COMLIB_SEQUENCE_METHOD_HASINPUTDESCRIPTOR( f_##QueueTypeName##_hasinputdescriptor, QueueTypeName );     \
typedef COMLIB_SEQUENCE_METHOD_HASOUTPUTDESCRIPTOR( f_##QueueTypeName##_hasoutputdescriptor, QueueTypeName );   \
typedef COMLIB_SEQUENCE_METHOD_CAPACITY( f_##QueueTypeName##_capacity, QueueTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_LENGTH( f_##QueueTypeName##_length, QueueTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_REMAIN( f_##QueueTypeName##_remain, QueueTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_INDEX( f_##QueueTypeName##_index, QueueTypeName, QueueUnitType );                \
typedef COMLIB_SEQUENCE_METHOD_OCC( f_##QueueTypeName##_occ, QueueTypeName, QueueUnitType );                    \
typedef COMLIB_SEQUENCE_METHOD_PEEK( f_##QueueTypeName##_peek, QueueTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_EXPECT( f_##QueueTypeName##_expect, QueueTypeName, QueueUnitType );              \
typedef COMLIB_SEQUENCE_METHOD_INITIALIZE( f_##QueueTypeName##_initialize, QueueTypeName, QueueUnitType );      \
typedef COMLIB_SEQUENCE_METHOD_WRITE( f_##QueueTypeName##_write, QueueTypeName, QueueUnitType );                \
typedef COMLIB_SEQUENCE_METHOD_SET( f_##QueueTypeName##_set, QueueTypeName, QueueUnitType );                    \
typedef COMLIB_SEQUENCE_METHOD_APPEND( f_##QueueTypeName##_append, QueueTypeName, QueueUnitType );              \
typedef COMLIB_SEQUENCE_METHOD_FORMAT( f_##QueueTypeName##_format, QueueTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_NULTERM( f_##QueueTypeName##_nulterm, QueueTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_NEXT( f_##QueueTypeName##_next, QueueTypeName, QueueUnitType );                  \
typedef COMLIB_SEQUENCE_METHOD_READ( f_##QueueTypeName##_read, QueueTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_GET( f_##QueueTypeName##_get, QueueTypeName, QueueUnitType );                    \
typedef COMLIB_SEQUENCE_METHOD_READUNTIL( f_##QueueTypeName##_readuntil, QueueTypeName, QueueUnitType );        \
typedef COMLIB_SEQUENCE_METHOD_READLINE( f_##QueueTypeName##_readline, QueueTypeName, QueueUnitType );          \
typedef COMLIB_SEQUENCE_METHOD_UNREAD( f_##QueueTypeName##_unread, QueueTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_TRUNCATE( f_##QueueTypeName##_truncate, QueueTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_CLEAR( f_##QueueTypeName##_clear, QueueTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_RESET( f_##QueueTypeName##_reset, QueueTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_GETVALUE( f_##QueueTypeName##_getvalue, QueueTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_ABSORB( f_##QueueTypeName##_absorb, QueueTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( f_##QueueTypeName##_absorbuntil, QueueTypeName, QueueUnitType );    \
typedef COMLIB_SEQUENCE_METHOD_CLONEINTO( f_##QueueTypeName##_cloneinto, QueueTypeName, QueueUnitType );        \
typedef COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( f_##QueueTypeName##_transplantfrom, QueueTypeName );             \
typedef COMLIB_SEQUENCE_METHOD_HEAPIFY( f_##QueueTypeName##_heapify, QueueTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_HEAPTOP( f_##QueueTypeName##_heaptop, QueueTypeName, QueueUnitType );            \
typedef COMLIB_SEQUENCE_METHOD_HEAPPUSH( f_##QueueTypeName##_heappush, QueueTypeName, QueueUnitType );          \
typedef COMLIB_SEQUENCE_METHOD_HEAPPOP( f_##QueueTypeName##_heappop, QueueTypeName, QueueUnitType );            \
typedef COMLIB_SEQUENCE_METHOD_HEAPREPLACE( f_##QueueTypeName##_heapreplace, QueueTypeName, QueueUnitType );    \
typedef COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( f_##QueueTypeName##_heappushtopk, QueueTypeName, QueueUnitType );  \
typedef COMLIB_SEQUENCE_METHOD_DISCARD( f_##QueueTypeName##_discard, QueueTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_UNWRITE( f_##QueueTypeName##_unwrite, QueueTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_POP( f_##QueueTypeName##_pop, QueueTypeName, QueueUnitType );                    \
typedef COMLIB_SEQUENCE_METHOD_DUMP( f_##QueueTypeName##_dump, QueueTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_FLUSH( f_##QueueTypeName##_flush, QueueTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_SORT( f_##QueueTypeName##_sort, QueueTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_REVERSE( f_##QueueTypeName##_reverse, QueueTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_OPTIMIZE( f_##QueueTypeName##_optimize, QueueTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_GETERROR( f_##QueueTypeName##_geterror, QueueTypeName );                         \
                                                                                                                \
typedef COMLIB_SEQUENCE_ELEMENT_COMPARATOR( QueueTypeName, QueueUnitType );                                     \
                                                                                                                \
X_COMLIB_SEQUENCE_GENERIC_QUEUE_CLASS( QueueTypeName, QueueUnitType );                                          \
                                                                                                                \
void QueueTypeName##_RegisterClass( void );                                                                     \
void QueueTypeName##_UnregisterClass( void );                                                                   \
                                                                                                                \
DLL_COMLIB_PUBLIC extern QueueTypeName##_vtable_t * g_##QueueTypeName##_vtable;                                 \


#define X_COMLIB_GENERIC_QUEUE_TYPEDEFS( QueueTypeName, QueueUnitType ) COMLIB_GENERIC_QUEUE_TYPEDEFS( QueueTypeName, QueueUnitType )




/*******************************************************************//**
 * CStringQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CStringQueue_TYPENAME          CStringQueue
#define COMLIB_CStringQueue_ELEMENT_TYPE      char
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CStringQueue_TYPENAME, COMLIB_CStringQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CStringQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * CByteQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CByteQueue_TYPENAME            CByteQueue
#define COMLIB_CByteQueue_ELEMENT_TYPE        BYTE
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CByteQueue_TYPENAME, COMLIB_CByteQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CByteQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * CWordQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CWordQueue_TYPENAME            CWordQueue
#define COMLIB_CWordQueue_ELEMENT_TYPE        WORD
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CWordQueue_TYPENAME, COMLIB_CWordQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CWordQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * CDwordQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CDwordQueue_TYPENAME           CDwordQueue
#define COMLIB_CDwordQueue_ELEMENT_TYPE       DWORD
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CDwordQueue_TYPENAME, COMLIB_CDwordQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CDwordQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * CQwordQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CQwordQueue_TYPENAME           CQwordQueue
#define COMLIB_CQwordQueue_ELEMENT_TYPE       QWORD
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CQwordQueue_TYPENAME, COMLIB_CQwordQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CQwordQueue_t_ElementCapacity( void );


// This class is used often for serialization so let's make some convenience methods
DLL_COMLIB_PUBLIC extern  CQwordQueue_t * CQwordQueueNew( int64_t initial_capacity );
DLL_COMLIB_PUBLIC extern  CQwordQueue_t * CQwordQueueNewOutput( int64_t initial_capacity, const char *path );
DLL_COMLIB_PUBLIC extern  CQwordQueue_t * CQwordQueueNewInput( int64_t initial_capacity, const char *path );



/*******************************************************************//**
 * CtptrQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CtptrQueue_TYPENAME           CtptrQueue
#define COMLIB_CtptrQueue_ELEMENT_TYPE       tptr_t
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CtptrQueue_TYPENAME, COMLIB_CtptrQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CtptrQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * Cx2tptrQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cx2tptrQueue_TYPENAME           Cx2tptrQueue
#define COMLIB_Cx2tptrQueue_ELEMENT_TYPE       x2tptr_t
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_Cx2tptrQueue_TYPENAME, COMLIB_Cx2tptrQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cx2tptrQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * CaptrQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_CaptrQueue_TYPENAME           CaptrQueue
#define COMLIB_CaptrQueue_ELEMENT_TYPE       aptr_t
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_CaptrQueue_TYPENAME, COMLIB_CaptrQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CaptrQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm128iQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm128iQueue_TYPENAME           Cm128iQueue
#define COMLIB_Cm128iQueue_ELEMENT_TYPE       __m128i
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_Cm128iQueue_TYPENAME, COMLIB_Cm128iQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm128iQueue_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm256iQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm256iQueue_TYPENAME           Cm256iQueue
#define COMLIB_Cm256iQueue_ELEMENT_TYPE       __m256i
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_Cm256iQueue_TYPENAME, COMLIB_Cm256iQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm256iQueue_t_ElementCapacity( void );

DLL_COMLIB_PUBLIC extern  Cm256iQueue_t * Cm256iQueueNew( int64_t initial_capacity );
DLL_COMLIB_PUBLIC extern  Cm256iQueue_t * Cm256iQueueNewOutput( int64_t initial_capacity, const char *path );
DLL_COMLIB_PUBLIC extern  Cm256iQueue_t * Cm256iQueueNewInput( int64_t initial_capacity, const char *path );




/*******************************************************************//**
 * Cm512iQueue_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm512iQueue_TYPENAME           Cm512iQueue
#define COMLIB_Cm512iQueue_ELEMENT_TYPE       cacheline_t
X_COMLIB_GENERIC_QUEUE_TYPEDEFS( COMLIB_Cm512iQueue_TYPENAME, COMLIB_Cm512iQueue_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm512iQueue_t_ElementCapacity( void );




/*******************************************************************//**
 * ComlibBuffer TEMPLATE
 * 
 ***********************************************************************
 */
#define COMLIB_GENERIC_BUFFER_TYPEDEFS( BufferTypeName, BufferUnitType )                                        \
                                                                                                                \
struct s_##BufferTypeName##_t;                                                                                  \
                                                                                                                \
/* COMLIB BUFFER CONSTRUCTOR */                                                                                 \
typedef struct s_##BufferTypeName##_constructor_args_t {                                                        \
  int64_t element_capacity;                                                                                     \
} BufferTypeName##_constructor_args_t;                                                                          \
                                                                                                                \
                                                                                                                \
/* COMLIB GENERIC BUFFER VTABLE*/                                                                               \
typedef struct s_##BufferTypeName##_vtable_t {                                                                  \
  /* vtable head */                                                                                             \
  COMLIB_VTABLE_HEAD                                                                                            \
                                                                                                                \
  /* interface for those who like a pseudo object-oriented approach */                                          \
  COMLIB_SEQUENCE_METHOD_SETCAPACITY( SetCapacity, BufferTypeName );                                            \
                                                                                                                \
  COMLIB_SEQUENCE_METHOD_CAPACITY( Capacity, BufferTypeName );                                                  \
  COMLIB_SEQUENCE_METHOD_LENGTH( Length, BufferTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_REMAIN( Remain, BufferTypeName );                                                      \
                                                                                                                \
  COMLIB_SEQUENCE_METHOD_INDEX( Index, BufferTypeName, BufferUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_OCC( Occ, BufferTypeName, BufferUnitType );                                            \
  COMLIB_SEQUENCE_METHOD_PEEK( Peek, BufferTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_EXPECT( Expect, BufferTypeName, BufferUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_INITIALIZE( Initialize, BufferTypeName, BufferUnitType );                              \
  COMLIB_SEQUENCE_METHOD_WRITE( Write, BufferTypeName, BufferUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_SET( Set, BufferTypeName, BufferUnitType );                                            \
  COMLIB_SEQUENCE_METHOD_APPEND( Append, BufferTypeName, BufferUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_FORMAT( Format, BufferTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_NULTERM( NulTerm, BufferTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_NEXT( Next, BufferTypeName, BufferUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_READ( Read, BufferTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_GET( Get, BufferTypeName, BufferUnitType );                                            \
  COMLIB_SEQUENCE_METHOD_READUNTIL( ReadUntil, BufferTypeName, BufferUnitType );                                \
  COMLIB_SEQUENCE_METHOD_READLINE( Readline, BufferTypeName, BufferUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_TRUNCATE( Truncate, BufferTypeName );                                                  \
  COMLIB_SEQUENCE_METHOD_CLEAR( Clear, BufferTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_RESET( Reset, BufferTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_GETVALUE( GetValue, BufferTypeName );                                                  \
  COMLIB_SEQUENCE_METHOD_ABSORB( Absorb, BufferTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( AbsorbUntil, BufferTypeName, BufferUnitType );                            \
  COMLIB_SEQUENCE_METHOD_CLONEINTO( CloneInto, BufferTypeName, BufferUnitType );                                \
  COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( TransplantFrom, BufferTypeName );                                      \
  COMLIB_SEQUENCE_METHOD_DISCARD( Discard, BufferTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_UNWRITE( Unwrite, BufferTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_POP( Pop, BufferTypeName, BufferUnitType );                                            \
  COMLIB_SEQUENCE_METHOD_REVERSE( Reverse, BufferTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_OPTIMIZE( Optimize, BufferTypeName );                                                  \
} BufferTypeName##_vtable_t;                                                                                    \
                                                                                                                \
                                                                                                                \
                                                                                                                \
/* API function pointer definitions */                                                                          \
typedef COMLIB_SEQUENCE_METHOD_SETCAPACITY( f_##BufferTypeName##_setcapacity, BufferTypeName );                 \
typedef COMLIB_SEQUENCE_METHOD_CAPACITY( f_##BufferTypeName##_capacity, BufferTypeName );                       \
typedef COMLIB_SEQUENCE_METHOD_LENGTH( f_##BufferTypeName##_length, BufferTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_REMAIN( f_##BufferTypeName##_remain, BufferTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_INDEX( f_##BufferTypeName##_index, BufferTypeName, BufferUnitType );             \
typedef COMLIB_SEQUENCE_METHOD_OCC( f_##BufferTypeName##_occ, BufferTypeName, BufferUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_PEEK( f_##BufferTypeName##_peek, BufferTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_EXPECT( f_##BufferTypeName##_expect, BufferTypeName, BufferUnitType );           \
typedef COMLIB_SEQUENCE_METHOD_INITIALIZE( f_##BufferTypeName##_initialize, BufferTypeName, BufferUnitType );   \
typedef COMLIB_SEQUENCE_METHOD_WRITE( f_##BufferTypeName##_write, BufferTypeName, BufferUnitType );             \
typedef COMLIB_SEQUENCE_METHOD_SET( f_##BufferTypeName##_set, BufferTypeName, BufferUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_APPEND( f_##BufferTypeName##_append, BufferTypeName, BufferUnitType );           \
typedef COMLIB_SEQUENCE_METHOD_FORMAT( f_##BufferTypeName##_format, BufferTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_NULTERM( f_##BufferTypeName##_nulterm, BufferTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_NEXT( f_##BufferTypeName##_next, BufferTypeName, BufferUnitType );               \
typedef COMLIB_SEQUENCE_METHOD_READ( f_##BufferTypeName##_read, BufferTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_GET( f_##BufferTypeName##_get, BufferTypeName, BufferUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_READUNTIL( f_##BufferTypeName##_readuntil, BufferTypeName, BufferUnitType );     \
typedef COMLIB_SEQUENCE_METHOD_READLINE( f_##BufferTypeName##_readline, BufferTypeName, BufferUnitType );       \
typedef COMLIB_SEQUENCE_METHOD_TRUNCATE( f_##BufferTypeName##_truncate, BufferTypeName );                       \
typedef COMLIB_SEQUENCE_METHOD_CLEAR( f_##BufferTypeName##_clear, BufferTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_RESET( f_##BufferTypeName##_reset, BufferTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_GETVALUE( f_##BufferTypeName##_getvalue, BufferTypeName );                       \
typedef COMLIB_SEQUENCE_METHOD_ABSORB( f_##BufferTypeName##_absorb, BufferTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( f_##BufferTypeName##_absorbuntil, BufferTypeName, BufferUnitType ); \
typedef COMLIB_SEQUENCE_METHOD_CLONEINTO( f_##BufferTypeName##_cloneinto, BufferTypeName, BufferUnitType );     \
typedef COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( f_##BufferTypeName##_transplantfrom, BufferTypeName );           \
typedef COMLIB_SEQUENCE_METHOD_DISCARD( f_##BufferTypeName##_discard, BufferTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_UNWRITE( f_##BufferTypeName##_unwrite, BufferTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_POP( f_##BufferTypeName##_pop, BufferTypeName, BufferUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_REVERSE( f_##BufferTypeName##_reverse, BufferTypeName );                         \
typedef COMLIB_SEQUENCE_METHOD_OPTIMIZE( f_##BufferTypeName##_optimize, BufferTypeName );                       \
                                                                                                                \
X_COMLIB_SEQUENCE_GENERIC_BUFFER_CLASS( BufferTypeName, BufferUnitType );                                       \
                                                                                                                \
void BufferTypeName##_RegisterClass( void );                                                                    \
void BufferTypeName##_UnregisterClass( void );                                                                  \
                                                                                                                \
DLL_COMLIB_PUBLIC extern BufferTypeName##_vtable_t * g_##BufferTypeName##_vtable;                               \


#define X_COMLIB_GENERIC_BUFFER_TYPEDEFS( BufferTypeName, BufferUnitType ) COMLIB_GENERIC_BUFFER_TYPEDEFS( BufferTypeName, BufferUnitType )




/*******************************************************************//**
 * CStringBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CStringBuffer_TYPENAME         CStringBuffer
#define COMLIB_CStringBuffer_ELEMENT_TYPE     char
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CStringBuffer_TYPENAME, COMLIB_CStringBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CStringBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CByteBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CByteBuffer_TYPENAME           CByteBuffer
#define COMLIB_CByteBuffer_ELEMENT_TYPE       BYTE
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CByteBuffer_TYPENAME, COMLIB_CByteBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CByteBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CWordBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CWordBuffer_TYPENAME            CWordBuffer
#define COMLIB_CWordBuffer_ELEMENT_TYPE        WORD
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CWordBuffer_TYPENAME, COMLIB_CWordBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CWordBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CDwordBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CDwordBuffer_TYPENAME           CDwordBuffer
#define COMLIB_CDwordBuffer_ELEMENT_TYPE       DWORD
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CDwordBuffer_TYPENAME, COMLIB_CDwordBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CDwordBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CQwordBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CQwordBuffer_TYPENAME           CQwordBuffer
#define COMLIB_CQwordBuffer_ELEMENT_TYPE       QWORD
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CQwordBuffer_TYPENAME, COMLIB_CQwordBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CQwordBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CtptrBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CtptrBuffer_TYPENAME           CtptrBuffer
#define COMLIB_CtptrBuffer_ELEMENT_TYPE       tptr_t
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CtptrBuffer_TYPENAME, COMLIB_CtptrBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CtptrBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * Cx2tptrBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cx2tptrBuffer_TYPENAME           Cx2tptrBuffer
#define COMLIB_Cx2tptrBuffer_ELEMENT_TYPE       x2tptr_t
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_Cx2tptrBuffer_TYPENAME, COMLIB_Cx2tptrBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cx2tptrBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * CaptrBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_CaptrBuffer_TYPENAME           CaptrBuffer
#define COMLIB_CaptrBuffer_ELEMENT_TYPE       aptr_t
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_CaptrBuffer_TYPENAME, COMLIB_CaptrBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CaptrBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm128iBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm128iBuffer_TYPENAME           Cm128iBuffer
#define COMLIB_Cm128iBuffer_ELEMENT_TYPE       __m128i
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_Cm128iBuffer_TYPENAME, COMLIB_Cm128iBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm128iBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm256iBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm256iBuffer_TYPENAME           Cm256iBuffer
#define COMLIB_Cm256iBuffer_ELEMENT_TYPE       __m256i
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_Cm256iBuffer_TYPENAME, COMLIB_Cm256iBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm256iBuffer_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm512iBuffer_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm512iBuffer_TYPENAME           Cm512iBuffer
#define COMLIB_Cm512iBuffer_ELEMENT_TYPE       cacheline_t
X_COMLIB_GENERIC_BUFFER_TYPEDEFS( COMLIB_Cm512iBuffer_TYPENAME, COMLIB_Cm512iBuffer_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm512iBuffer_t_ElementCapacity( void );




/*******************************************************************//**
 * ComlibHeap TEMPLATE
 * 
 ***********************************************************************
 */
#define COMLIB_HEAP_TYPEDEFS( HeapTypeName, HeapUnitType )                                                      \
                                                                                                                \
struct s_##HeapTypeName##_t;                                                                                    \
                                                                                                                \
/* COMLIB HEAP CONSTRUCTOR */                                                                                   \
typedef struct s_##HeapTypeName##_constructor_args_t {                                                          \
  int64_t element_capacity;                                                                                     \
  int (*comparator)( const HeapUnitType *a, const HeapUnitType *b );                                            \
} HeapTypeName##_constructor_args_t;                                                                            \
                                                                                                                \
                                                                                                                \
/* COMLIB HEAP VTABLE */                                                                                        \
typedef struct s_##HeapTypeName##_vtable_t {                                                                    \
  /* vtable head */                                                                                             \
  COMLIB_VTABLE_HEAD                                                                                            \
  COMLIB_SEQUENCE_METHOD_SETCAPACITY( SetCapacity, HeapTypeName );                                              \
  COMLIB_SEQUENCE_METHOD_CAPACITY( Capacity, HeapTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_LENGTH( Length, HeapTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_REMAIN( Remain, HeapTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_INITIALIZE( Initialize, HeapTypeName, HeapUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_WRITE( Extend, HeapTypeName, HeapUnitType );                                           \
  COMLIB_SEQUENCE_METHOD_APPEND( Append, HeapTypeName, HeapUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_CLEAR( Clear, HeapTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_RESET( Reset, HeapTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_CLONEINTO( CloneInto, HeapTypeName, HeapUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( TransplantFrom, HeapTypeName );                                        \
  COMLIB_SEQUENCE_METHOD_HEAPIFY( Heapify, HeapTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_HEAPTOP( HeapTop, HeapTypeName, HeapUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_HEAPPUSH( HeapPush, HeapTypeName, HeapUnitType );                                      \
  COMLIB_SEQUENCE_METHOD_HEAPPOP( HeapPop, HeapTypeName, HeapUnitType );                                        \
  COMLIB_SEQUENCE_METHOD_HEAPREPLACE( HeapReplace, HeapTypeName, HeapUnitType );                                \
  COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( HeapPushTopK, HeapTypeName, HeapUnitType );                              \
                                                                                                                \
} HeapTypeName##_vtable_t;                                                                                      \
                                                                                                                \
typedef COMLIB_SEQUENCE_METHOD_SETCAPACITY( f_##HeapTypeName##_setcapacity, HeapTypeName );                     \
typedef COMLIB_SEQUENCE_METHOD_CAPACITY( f_##HeapTypeName##_capacity, HeapTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_LENGTH( f_##HeapTypeName##_length, HeapTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_REMAIN( f_##HeapTypeName##_remain, HeapTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_INITIALIZE( f_##HeapTypeName##_initialize, HeapTypeName, HeapUnitType );         \
typedef COMLIB_SEQUENCE_METHOD_WRITE( f_##HeapTypeName##_extend, HeapTypeName, HeapUnitType );                  \
typedef COMLIB_SEQUENCE_METHOD_APPEND( f_##HeapTypeName##_append, HeapTypeName, HeapUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_CLEAR( f_##HeapTypeName##_clear, HeapTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_RESET( f_##HeapTypeName##_reset, HeapTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_CLONEINTO( f_##HeapTypeName##_cloneinto, HeapTypeName, HeapUnitType );           \
typedef COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( f_##HeapTypeName##_transplantfrom, HeapTypeName );               \
typedef COMLIB_SEQUENCE_METHOD_HEAPIFY( f_##HeapTypeName##_heapify, HeapTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_HEAPTOP( f_##HeapTypeName##_heaptop, HeapTypeName, HeapUnitType );               \
typedef COMLIB_SEQUENCE_METHOD_HEAPPUSH( f_##HeapTypeName##_heappush, HeapTypeName, HeapUnitType );             \
typedef COMLIB_SEQUENCE_METHOD_HEAPPOP( f_##HeapTypeName##_heappop, HeapTypeName, HeapUnitType );               \
typedef COMLIB_SEQUENCE_METHOD_HEAPREPLACE( f_##HeapTypeName##_heapreplace, HeapTypeName, HeapUnitType );       \
typedef COMLIB_SEQUENCE_METHOD_HEAPPUSHTOPK( f_##HeapTypeName##_heappushtopk, HeapTypeName, HeapUnitType );     \
                                                                                                                \
                                                                                                                \
typedef COMLIB_SEQUENCE_ELEMENT_COMPARATOR( HeapTypeName, HeapUnitType );                                       \
                                                                                                                \
X_COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( HeapTypeName, HeapUnitType );                                            \
                                                                                                                \
void HeapTypeName##_RegisterClass( void );                                                                      \
void HeapTypeName##_UnregisterClass( void );                                                                    \
                                                                                                                \
DLL_COMLIB_PUBLIC extern HeapTypeName##_vtable_t * g_##HeapTypeName##_vtable;                                   \


#define X_COMLIB_HEAP_TYPEDEFS( HeapTypeName, HeapUnitType ) COMLIB_HEAP_TYPEDEFS( HeapTypeName, HeapUnitType )


/*******************************************************************//**
 * CByteHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CByteHeap_TYPENAME            CByteHeap
#define COMLIB_CByteHeap_ELEMENT_TYPE        BYTE

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CByteHeap_TYPENAME, COMLIB_CByteHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CByteHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * CWordHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CWordHeap_TYPENAME            CWordHeap
#define COMLIB_CWordHeap_ELEMENT_TYPE        WORD

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CWordHeap_TYPENAME, COMLIB_CWordHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CWordHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * CDwordHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CDwordHeap_TYPENAME           CDwordHeap
#define COMLIB_CDwordHeap_ELEMENT_TYPE       DWORD

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CDwordHeap_TYPENAME, COMLIB_CDwordHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CDwordHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * CQwordHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CQwordHeap_TYPENAME           CQwordHeap
#define COMLIB_CQwordHeap_ELEMENT_TYPE       QWORD

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CQwordHeap_TYPENAME, COMLIB_CQwordHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CQwordHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm128iHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm128iHeap_TYPENAME           Cm128iHeap
#define COMLIB_Cm128iHeap_ELEMENT_TYPE       __m128i

X_COMLIB_HEAP_TYPEDEFS( COMLIB_Cm128iHeap_TYPENAME, COMLIB_Cm128iHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm128iHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm256iHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm256iHeap_TYPENAME           Cm256iHeap
#define COMLIB_Cm256iHeap_ELEMENT_TYPE       __m256i

X_COMLIB_HEAP_TYPEDEFS( COMLIB_Cm256iHeap_TYPENAME, COMLIB_Cm256iHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm256iHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm512iHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm512iHeap_TYPENAME           Cm512iHeap
#define COMLIB_Cm512iHeap_ELEMENT_TYPE       cacheline_t

X_COMLIB_HEAP_TYPEDEFS( COMLIB_Cm512iHeap_TYPENAME, COMLIB_Cm512iHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm512iHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * CtptrHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CtptrHeap_TYPENAME           CtptrHeap
#define COMLIB_CtptrHeap_ELEMENT_TYPE       tptr_t

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CtptrHeap_TYPENAME, COMLIB_CtptrHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CtptrHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * Cx2tptr2Heap_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cx2tptrHeap_TYPENAME           Cx2tptrHeap
#define COMLIB_Cx2tptrHeap_ELEMENT_TYPE       x2tptr_t

X_COMLIB_HEAP_TYPEDEFS( COMLIB_Cx2tptrHeap_TYPENAME, COMLIB_Cx2tptrHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cx2tptrHeap_t_ElementCapacity( void );


/*******************************************************************//**
 * CaptrHeap_t
 * 
 ***********************************************************************
 */
#define COMLIB_CaptrHeap_TYPENAME           CaptrHeap
#define COMLIB_CaptrHeap_ELEMENT_TYPE       aptr_t

X_COMLIB_HEAP_TYPEDEFS( COMLIB_CaptrHeap_TYPENAME, COMLIB_CaptrHeap_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CaptrHeap_t_ElementCapacity( void );





/*******************************************************************//**
 * ComlibList TEMPLATE
 * 
 ***********************************************************************
 */
#define COMLIB_LIST_TYPEDEFS( ListTypeName, ListUnitType )                                                      \
                                                                                                                \
struct s_##ListTypeName##_t;                                                                                    \
struct s_##ListUnitType##_t;                                                                                    \
                                                                                                                \
/* COMLIB LIST CONSTRUCTOR */                                                                                   \
typedef struct s_##ListTypeName##_constructor_args_t {                                                          \
  int64_t element_capacity;                                                                                     \
  int (*comparator)( const ListUnitType *a, const ListUnitType *b );                                            \
} ListTypeName##_constructor_args_t;                                                                            \
                                                                                                                \
                                                                                                                \
/* COMLIB LIST VTABLE */                                                                                        \
typedef struct s_##ListTypeName##_vtable_t {                                                                    \
  /* vtable head */                                                                                             \
  COMLIB_VTABLE_HEAD                                                                                            \
  COMLIB_SEQUENCE_METHOD_SETCAPACITY( SetCapacity, ListTypeName );                                              \
  COMLIB_SEQUENCE_METHOD_CAPACITY( Capacity, ListTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_LENGTH( Length, ListTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_REMAIN( Remain, ListTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_INITIALIZE( Initialize, ListTypeName, ListUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_DEADSPACE( DeadSpace, ListTypeName );                                                  \
  COMLIB_SEQUENCE_METHOD_WRITE( Extend, ListTypeName, ListUnitType );                                           \
  COMLIB_SEQUENCE_METHOD_SET( Set, ListTypeName, ListUnitType );                                                \
  COMLIB_SEQUENCE_METHOD_APPEND( Append, ListTypeName, ListUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_CURSOR( Cursor, ListTypeName, ListUnitType );                                          \
  COMLIB_SEQUENCE_METHOD_GET( Get, ListTypeName, ListUnitType );                                                \
  COMLIB_SEQUENCE_METHOD_TRUNCATE( Truncate, ListTypeName );                                                    \
  COMLIB_SEQUENCE_METHOD_CLEAR( Clear, ListTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_RESET( Reset, ListTypeName );                                                          \
  COMLIB_SEQUENCE_METHOD_ABSORB( Absorb, ListTypeName );                                                        \
  COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( AbsorbUntil, ListTypeName, ListUnitType );                                \
  COMLIB_SEQUENCE_METHOD_CLONEINTO( CloneInto, ListTypeName, ListUnitType );                                    \
  COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( TransplantFrom, ListTypeName );                                        \
  COMLIB_SEQUENCE_METHOD_YANKBUFFER( YankBuffer, ListTypeName, ListUnitType );                                  \
  COMLIB_SEQUENCE_METHOD_DISCARD( Discard, ListTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_SORT( Sort, ListTypeName );                                                            \
  COMLIB_SEQUENCE_METHOD_REVERSE( Reverse, ListTypeName );                                                      \
  COMLIB_SEQUENCE_METHOD_OPTIMIZE( Optimize, ListTypeName );                                                    \
                                                                                                                \
} ListTypeName##_vtable_t;                                                                                      \
                                                                                                                \
typedef COMLIB_SEQUENCE_METHOD_SETCAPACITY( f_##ListTypeName##_setcapacity, ListTypeName );                     \
typedef COMLIB_SEQUENCE_METHOD_CAPACITY( f_##ListTypeName##_capacity, ListTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_LENGTH( f_##ListTypeName##_length, ListTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_REMAIN( f_##ListTypeName##_remain, ListTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_INITIALIZE( f_##ListTypeName##_initialize, ListUnitType, ListUnitType );         \
typedef COMLIB_SEQUENCE_METHOD_DEADSPACE( f_##ListTypeName##_deadspace, ListUnitType );                         \
typedef COMLIB_SEQUENCE_METHOD_WRITE( f_##ListTypeName##_extend, ListUnitType, ListUnitType );                  \
typedef COMLIB_SEQUENCE_METHOD_SET( f_##ListTypeName##_set, ListTypeName, ListUnitType );                       \
typedef COMLIB_SEQUENCE_METHOD_APPEND( f_##ListTypeName##_append, ListUnitType, ListUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_CURSOR( f_##ListTypeName##_cursor, ListTypeName, ListUnitType );                 \
typedef COMLIB_SEQUENCE_METHOD_GET( f_##ListTypeName##_get, ListTypeName, ListUnitType );                       \
typedef COMLIB_SEQUENCE_METHOD_TRUNCATE( f_##ListTypeName##_truncate, ListTypeName );                           \
typedef COMLIB_SEQUENCE_METHOD_CLEAR( f_##ListTypeName##_clear, ListTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_RESET( f_##ListTypeName##_reset, ListTypeName );                                 \
typedef COMLIB_SEQUENCE_METHOD_ABSORB( f_##ListTypeName##_absorb, ListTypeName );                               \
typedef COMLIB_SEQUENCE_METHOD_ABSORBUNTIL( f_##ListTypeName##_absorbuntil, ListTypeName, ListUnitType );       \
typedef COMLIB_SEQUENCE_METHOD_CLONEINTO( f_##ListTypeName##_cloneinto, ListTypeName, ListUnitType );           \
typedef COMLIB_SEQUENCE_METHOD_TRANSPLANTFROM( f_##ListTypeName##_transplantfrom, ListTypeName );               \
typedef COMLIB_SEQUENCE_METHOD_YANKBUFFER( f_##ListTypeName##_yankbuffer, ListTypeName, ListUnitType );         \
typedef COMLIB_SEQUENCE_METHOD_DISCARD( f_##ListTypeName##_discard, ListTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_SORT( f_##ListTypeName##_sort, ListTypeName );                                   \
typedef COMLIB_SEQUENCE_METHOD_REVERSE( f_##ListTypeName##_reverse, ListTypeName );                             \
typedef COMLIB_SEQUENCE_METHOD_OPTIMIZE( f_##ListTypeName##_optimize, ListTypeName );                           \
                                                                                                                \
                                                                                                                \
typedef COMLIB_SEQUENCE_ELEMENT_COMPARATOR( ListTypeName, ListUnitType );                                       \
                                                                                                                \
X_COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( ListTypeName, ListUnitType );                                            \
                                                                                                                \
void ListTypeName##_RegisterClass( void );                                                                      \
void ListTypeName##_UnregisterClass( void );                                                                    \
                                                                                                                \
DLL_COMLIB_PUBLIC extern ListTypeName##_vtable_t * g_##ListTypeName##_vtable;                                   \


#define X_COMLIB_LIST_TYPEDEFS( ListTypeName, ListUnitType ) COMLIB_LIST_TYPEDEFS( ListTypeName, ListUnitType )



/*******************************************************************//**
 * CByteList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CByteList_TYPENAME           CByteList
#define COMLIB_CByteList_ELEMENT_TYPE       BYTE

X_COMLIB_LIST_TYPEDEFS( COMLIB_CByteList_TYPENAME, COMLIB_CByteList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CByteList_t_ElementCapacity( void );


/*******************************************************************//**
 * CWordList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CWordList_TYPENAME           CWordList
#define COMLIB_CWordList_ELEMENT_TYPE       WORD

X_COMLIB_LIST_TYPEDEFS( COMLIB_CWordList_TYPENAME, COMLIB_CWordList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CWordList_t_ElementCapacity( void );


/*******************************************************************//**
 * CDwordList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CDwordList_TYPENAME           CDwordList
#define COMLIB_CDwordList_ELEMENT_TYPE       DWORD

X_COMLIB_LIST_TYPEDEFS( COMLIB_CDwordList_TYPENAME, COMLIB_CDwordList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CDwordList_t_ElementCapacity( void );


/*******************************************************************//**
 * CQwordList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CQwordList_TYPENAME           CQwordList
#define COMLIB_CQwordList_ELEMENT_TYPE       QWORD

X_COMLIB_LIST_TYPEDEFS( COMLIB_CQwordList_TYPENAME, COMLIB_CQwordList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CQwordList_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm128iList_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm128iList_TYPENAME           Cm128iList
#define COMLIB_Cm128iList_ELEMENT_TYPE       __m128i

X_COMLIB_LIST_TYPEDEFS( COMLIB_Cm128iList_TYPENAME, COMLIB_Cm128iList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm128iList_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm256iList_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm256iList_TYPENAME           Cm256iList
#define COMLIB_Cm256iList_ELEMENT_TYPE       __m256i

X_COMLIB_LIST_TYPEDEFS( COMLIB_Cm256iList_TYPENAME, COMLIB_Cm256iList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm256iList_t_ElementCapacity( void );


/*******************************************************************//**
 * Cm512iList_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cm512iList_TYPENAME           Cm512iList
#define COMLIB_Cm512iList_ELEMENT_TYPE       cacheline_t

X_COMLIB_LIST_TYPEDEFS( COMLIB_Cm512iList_TYPENAME, COMLIB_Cm512iList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cm512iList_t_ElementCapacity( void );


/*******************************************************************//**
 * CtptrList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CtptrList_TYPENAME           CtptrList
#define COMLIB_CtptrList_ELEMENT_TYPE       tptr_t

X_COMLIB_LIST_TYPEDEFS( COMLIB_CtptrList_TYPENAME, COMLIB_CtptrList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CtptrList_t_ElementCapacity( void );


/*******************************************************************//**
 * Cx2tptrList_t
 * 
 ***********************************************************************
 */
#define COMLIB_Cx2tptrList_TYPENAME           Cx2tptrList
#define COMLIB_Cx2tptrList_ELEMENT_TYPE       x2tptr_t

X_COMLIB_LIST_TYPEDEFS( COMLIB_Cx2tptrList_TYPENAME, COMLIB_Cx2tptrList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_Cx2tptrList_t_ElementCapacity( void );


/*******************************************************************//**
 * CaptrList_t
 * 
 ***********************************************************************
 */
#define COMLIB_CaptrList_TYPENAME           CaptrList
#define COMLIB_CaptrList_ELEMENT_TYPE       aptr_t

X_COMLIB_LIST_TYPEDEFS( COMLIB_CaptrList_TYPENAME, COMLIB_CaptrList_ELEMENT_TYPE )

DLL_COMLIB_PUBLIC extern int64_t Comlib_CaptrList_t_ElementCapacity( void );




/*******************************************************************//**
 * ComlibLinearSequence TEMPLATE
 * 
 ***********************************************************************
 */
#define COMLIB_LINEAR_SEQUENCE_TYPEDEFS( Name, UnitType )                                                       \
                                                                                                                \
struct s_##Name##_t;                                                                                            \
                                                                                                                \
/* COMLIB LINEAR SEQUENCE CONSTRUCTOR */                                                                        \
typedef struct s_##Name##_constructor_args_t {                                                                  \
  int64_t element_capacity;                                                                                     \
  int (*comparator)( const UnitType *a, const UnitType *b );                                                    \
} Name##_constructor_args_t;                                                                                    \
                                                                                                                \
                                                                                                                \
/* COMLIB LINEAR SEQUENCE VTABLE */                                                                             \
typedef struct s_##Name##_vtable_t {                                                                            \
  /* vtable head */                                                                                             \
  COMLIB_VTABLE_HEAD                                                                                            \
  COMLIB_SEQUENCE_METHOD_SETCAPACITY( SetCapacity, Name );                                                      \
  COMLIB_SEQUENCE_METHOD_CAPACITY( Capacity, Name );                                                            \
  COMLIB_SEQUENCE_METHOD_LENGTH( Length, Name );                                                                \
  COMLIB_SEQUENCE_METHOD_REMAIN( Remain, Name );                                                                \
                                                                                                                \
} Name##_vtable_t;                                                                                              \
                                                                                                                \
typedef COMLIB_SEQUENCE_METHOD_SETCAPACITY( f_##Name##_setcapacity, Name );                                     \
typedef COMLIB_SEQUENCE_METHOD_CAPACITY( f_##Name##_capacity, Name );                                           \
typedef COMLIB_SEQUENCE_METHOD_LENGTH( f_##Name##_length, Name );                                               \
typedef COMLIB_SEQUENCE_METHOD_REMAIN( f_##Name##_remain, Name );                                               \
                                                                                                                \
                                                                                                                \
typedef COMLIB_SEQUENCE_ELEMENT_COMPARATOR( Name, UnitType );                                                   \
                                                                                                                \
X_COMLIB_SEQUENCE_LINEAR_BUFFER_CLASS( Name, UnitType );                                                        \
                                                                                                                \
                                                                                                                \
void Name##_RegisterClass( void );                                                                              \
void Name##_UnregisterClass( void );                                                                            \
                                                                                                                \
DLL_COMLIB_PUBLIC extern Name##_vtable_t * g_##Name##_vtable;                                                   \


#define X_COMLIB_LINEAR_SEQUENCE_TYPEDEFS( Name, UnitType ) COMLIB_LINEAR_SEQUENCE_TYPEDEFS( Name, UnitType )



#define COMLIB_SEQUENCE_LOCK_PTR( LockableComlibSequence ) &(LockableComlibSequence)->_lock



#define SYNCHRONIZE_TWO_COMLIB_SEQUENCES( LockableComlibSequence1, LockableComlibSequence2 ) \
  do {                                                                    \
    CS_LOCK *__L1__;                                                      \
    CS_LOCK *__L2__;                                                      \
    if( (LockableComlibSequence1) - (LockableComlibSequence2) > 0 ) {     \
      __L1__ = COMLIB_SEQUENCE_LOCK_PTR( LockableComlibSequence1 );       \
      __L2__ = COMLIB_SEQUENCE_LOCK_PTR( LockableComlibSequence2 );       \
    }                                                                     \
    else {                                                                \
      __L1__ = COMLIB_SEQUENCE_LOCK_PTR( LockableComlibSequence2 );       \
      __L2__ = COMLIB_SEQUENCE_LOCK_PTR( LockableComlibSequence1 );       \
    }                                                                     \
    SYNCHRONIZE_ON_PTR( __L1__ ) {                                        \
      SYNCHRONIZE_ON_PTR( __L2__ )


#define RELEASE_TWO_COMLIB_SEQUENCES                                      \
        RELEASE;                                                          \
    } RELEASE;                                                            \
  } WHILE_ZERO





/*******************************************************************//**
 * ComlibLinearSequence_t
 * 
 ***********************************************************************
 */
#define COMLIB_ComlibLinearSequence_TYPENAME           ComlibLinearSequence
#define COMLIB_ComlibLinearSequence_ELEMENT_TYPE       BYTE

X_COMLIB_LINEAR_SEQUENCE_TYPEDEFS( COMLIB_ComlibLinearSequence_TYPENAME, COMLIB_ComlibLinearSequence_ELEMENT_TYPE )





/* generic signature for putting one item into a sequence somehow */
typedef int (*f_ComlibSequence_additem)( void *sequence, void *item );

typedef int (*f_CStringSequence_additem)( void *sequence, char *item );
typedef int (*f_CQwordSequence_additem)( void *sequence, QWORD *item );
typedef int (*f_Cm128iSequence_additem)( void *sequence, __m128i *item );
typedef int (*f_Cm256iSequence_additem)( void *sequence, __m256i *item );
typedef int (*f_Cm512iSequence_additem)( void *sequence, cacheline_t *item );

typedef int (*f_CtptrSequence_additem)( void *sequence, tptr_t *item );
typedef int (*f_Cx2tptrSequence_additem)( void *sequence, x2tptr_t *item );
typedef int (*f_CaptrSequence_additem)( void *sequence, aptr_t *item );



#endif
