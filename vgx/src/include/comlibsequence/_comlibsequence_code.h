/*
 * _comlibsequence_code.h
 *
 *
*/
#ifndef _COMLIBSEQUENCE_CODE_H_INCLUDED
#define _COMLIBSEQUENCE_CODE_H_INCLUDED

#include "_comlibsequence_header.h"


typedef enum e_resize_direction_t {
  RESIZE_DOWN = 0,
  RESIZE_UP   = 1
} resize_direction_t;


// BEGIN FAKE INTELLISENSE HELPERS
// Define these here so that intellisense has something to work with (these will already be defined elsewhere at compile time!)

#ifndef _CSEQ_TYPENAME
#define _CSEQ_TYPENAME CQwordQueue_t
#endif
#ifndef _CSEQ_ELEMENT_TYPE
#define _CSEQ_ELEMENT_TYPE QWORD
#endif
#ifndef _CSEQ_VTABLETYPE
#define _CSEQ_VTABLETYPE CQwordQueue_vtable_t
#endif
#ifndef _CSEQ_CONSTRUCTOR_ARGSTYPE
#define _CSEQ_CONSTRUCTOR_ARGSTYPE CQwordQueue_constructor_args_t
#endif
#ifndef _CSEQ_MIN_CAPACITY_EXP
#define _CSEQ_MIN_CAPACITY_EXP 3
#endif
#ifndef _CSEQ_MAX_CAPACITY_EXP
#define _CSEQ_MAX_CAPACITY_EXP 29
#endif
#ifndef _CSEQ_DEFAULT_CAPACITY_EXP
#define _CSEQ_DEFAULT_CAPACITY_EXP 9
#endif


#define CAPACITY_FUNC_NAME( Name ) Comlib_##Name##_ElementCapacity
#define X_CAPACITY_FUNC_NAME( Name ) CAPACITY_FUNC_NAME( Name )


DLL_EXPORT extern int64_t X_CAPACITY_FUNC_NAME(_CSEQ_TYPENAME)( void ) {
  return (1ULL << _CSEQ_MAX_CAPACITY_EXP)-1;
}


static int64_t __sizeof_unread( _CSEQ_TYPENAME *self );
static int64_t __expand_to_accomodate( _CSEQ_TYPENAME *self, int64_t element_count );


// END FAKE INTELLISENSE HELPERS

#if defined( _CSEQ_FEATURE_ERRORS )

static char g_default_errstr[] = "unknown error";

static int __ComlibSequence_has_errstr( _CSEQ_TYPENAME *self );
static int __ComlibSequence_set_error( _CSEQ_TYPENAME *self, const char *errstr, ... );
static int __ComlibSequence_resize_nolock( _CSEQ_TYPENAME *self, int exp_delta, resize_direction_t direction );
static int64_t __ComlibSequence_read_file_nolock( _CSEQ_TYPENAME *self, int64_t element_count );

// last error
static const char * ComlibSequence_get_error( _CSEQ_TYPENAME *self );
#endif

#if defined( _CSEQ_FEATURE_STREAM_IO )
// stream attachment
static int ComlibSequence_attach_input_descriptor( _CSEQ_TYPENAME *self, short fd );
static int ComlibSequence_attach_output_descriptor( _CSEQ_TYPENAME *self, short fd );
static int ComlibSequence_attach_input_stream( _CSEQ_TYPENAME *self, const char *fpath );
static int ComlibSequence_attach_output_stream( _CSEQ_TYPENAME *self, const char *fpath );
static int ComlibSequence_detach_input( _CSEQ_TYPENAME *self );
static int ComlibSequence_detach_output( _CSEQ_TYPENAME *self );
static int ComlibSequence_detach_output_noflush( _CSEQ_TYPENAME *self );
static int ComlibSequence_has_input_descriptor( const _CSEQ_TYPENAME *self );
static int ComlibSequence_has_output_descriptor( const _CSEQ_TYPENAME *self );
#endif

// construct
static int ComlibSequence_set_capacity( _CSEQ_TYPENAME *self, int64_t element_capacity );

// capacity
static int64_t ComlibSequence_capacity( _CSEQ_TYPENAME *self );

// length
#if defined( _CSEQ_FEATURE_LENGTH_METHOD )
static int64_t ComlibSequence_length( _CSEQ_TYPENAME *self );
#endif

// remain
#if defined( _CSEQ_FEATURE_REMAIN_METHOD )
static int64_t ComlibSequence_remain( _CSEQ_TYPENAME *self );
#endif

// index
#if defined( _CSEQ_FEATURE_INDEX_METHOD )
static int64_t ComlibSequence_index_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_index( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen );
#endif
#endif

// occurrences
#if defined( _CSEQ_FEATURE_OCC_METHOD )
static int64_t ComlibSequence_occ_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_occ( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen );
#endif
#endif

// peek
#if defined( _CSEQ_FEATURE_PEEK_METHOD )
static int64_t ComlibSequence_peek_nolock( _CSEQ_TYPENAME *self, void **dest, int64_t element_index, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_peek( _CSEQ_TYPENAME *self, void **dest, int64_t element_index, int64_t element_count );
#endif
#endif

// expect
#if defined( _CSEQ_FEATURE_EXPECT_METHOD )
static bool ComlibSequence_expect_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int64_t element_index );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static bool ComlibSequence_expect( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int64_t element_index );
#endif
#endif

// initialize
#if defined( _CSEQ_FEATURE_INITIALIZE_METHOD )
static int64_t ComlibSequence_initialize_nolock( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *value, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_initialize( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *value, int64_t element_count );
#endif
#endif

// deadspace
#if defined( _CSEQ_FEATURE_DEADSPACE_METHOD )
static int64_t ComlibSequence_deadspace_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_deadspace( _CSEQ_TYPENAME *self );
#endif
#endif

// write
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
static int64_t ComlibSequence_write_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elements, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_write( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elements, int64_t element_count );
#endif
#endif

// set
#if defined( _CSEQ_FEATURE_SET_METHOD )
static int ComlibSequence_set_nolock( _CSEQ_TYPENAME *self, int64_t index, const _CSEQ_ELEMENT_TYPE *item );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_set( _CSEQ_TYPENAME *self, int64_t index, const _CSEQ_ELEMENT_TYPE *item );
#endif
#endif

// append
#if defined( _CSEQ_FEATURE_APPEND_METHOD )
static int ComlibSequence_append_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *item );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_append( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *item );
#endif
#endif

// format
#if defined( _CSEQ_FEATURE_FORMAT_METHOD )
static int64_t ComlibSequence_format_nolock( _CSEQ_TYPENAME *self, const char *format, ... );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_format( _CSEQ_TYPENAME *self, const char *format, ... );
#endif
#endif

// NulTerm
#if defined( _CSEQ_FEATURE_NULTERM_METHOD )
static int64_t ComlibSequence_nul_term_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_nul_term( _CSEQ_TYPENAME *self );
#endif
#endif

// next
#if defined( _CSEQ_FEATURE_NEXT_METHOD )
static int ComlibSequence_next_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_next( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#endif
#endif

// read
#if defined( _CSEQ_FEATURE_READ_METHOD )
static int64_t ComlibSequence_read_nolock( _CSEQ_TYPENAME *self, void **dest, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_read( _CSEQ_TYPENAME *self, void **dest, int64_t element_count );
#endif
#endif

// cursor
#if defined( _CSEQ_FEATURE_CURSOR_METHOD )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_cursor_nolock( _CSEQ_TYPENAME *self, int64_t index );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_cursor( _CSEQ_TYPENAME *self, int64_t index );
#endif
#endif

// get
#if defined( _CSEQ_FEATURE_GET_METHOD )
static int ComlibSequence_get_nolock( _CSEQ_TYPENAME *self, int64_t index, _CSEQ_ELEMENT_TYPE *dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_get( _CSEQ_TYPENAME *self, int64_t index, _CSEQ_ELEMENT_TYPE *dest );
#endif
#endif

// readuntil
#if defined( _CSEQ_FEATURE_READUNTIL_METHOD )
static int64_t ComlibSequence_readuntil_nolock( _CSEQ_TYPENAME *self, void **dest, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int exclude_probe );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_readuntil( _CSEQ_TYPENAME *self, void **dest, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int exclude_probe );
#endif
#endif

// readline
#if defined( _CSEQ_FEATURE_READLINE_METHOD )
static int64_t ComlibSequence_readline_nolock( _CSEQ_TYPENAME *self, void **dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_readline( _CSEQ_TYPENAME *self, void **dest );
#endif
#endif

// unread
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
static void ComlibSequence_unread_nolock( _CSEQ_TYPENAME *self, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static void ComlibSequence_unread( _CSEQ_TYPENAME *self, int64_t element_count );
#endif
#endif

// clear
#if defined( _CSEQ_FEATURE_TRUNCATE_METHOD )
static int64_t ComlibSequence_truncate_nolock( _CSEQ_TYPENAME *self, int64_t tail_index );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_truncate( _CSEQ_TYPENAME *self, int64_t tail_index );
#endif
#endif

// clear
#if defined( _CSEQ_FEATURE_CLEAR_METHOD )
static void ComlibSequence_clear_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static void ComlibSequence_clear( _CSEQ_TYPENAME *self );
#endif
#endif

// reset
#if defined( _CSEQ_FEATURE_RESET_METHOD )
static void ComlibSequence_reset_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static void ComlibSequence_reset( _CSEQ_TYPENAME *self );
#endif
#endif

// getvalue
#if defined( _CSEQ_FEATURE_GETVALUE_METHOD )
static int64_t ComlibSequence_getvalue_nolock( _CSEQ_TYPENAME *self, void **dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_getvalue( _CSEQ_TYPENAME *self, void **dest );
#endif
#endif

// absorb
#if defined( _CSEQ_FEATURE_ABSORB_METHOD )
static int64_t ComlibSequence_absorb_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_absorb( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, int64_t element_count );
#endif
#endif

// absorb until
#if defined( _CSEQ_FEATURE_ABSORBUNTIL_METHOD )
static int64_t ComlibSequence_absorb_until_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen_or_elemcount, int exclude_probe );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_absorb_until( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen_or_elemcount, int exclude_probe );
#endif
#endif

// clone into
#if defined( _CSEQ_FEATURE_CLONEINTO_METHOD )
static int64_t ComlibSequence_clone_into_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_clone_into( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other );
#endif
#endif

// transplant from
#if defined( _CSEQ_FEATURE_TRANSPLANTFROM_METHOD )
static int64_t ComlibSequence_transplant_from_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_transplant_from( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other );
#endif
#endif

// yank buffer
#if defined( _CSEQ_FEATURE_YANKBUFFER_METHOD )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_yank_buffer_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_yank_buffer( _CSEQ_TYPENAME *self );
#endif
#endif

// heapify
#if defined( _CSEQ_FEATURE_HEAPIFY_METHOD )
static void ComlibSequence_heapify_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static void ComlibSequence_heapify( _CSEQ_TYPENAME *self );
#endif
#endif

// heap top
#if defined( _CSEQ_FEATURE_HEAPTOP_METHOD )
static int ComlibSequence_heap_top_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_heap_top( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#endif
#endif

// heap push
#if defined( _CSEQ_FEATURE_HEAPPUSH_METHOD )
static int ComlibSequence_heap_push_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elem );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_heap_push( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elem );
#endif
#endif

// heap pop
#if defined( _CSEQ_FEATURE_HEAPPOP_METHOD )
static int ComlibSequence_heap_pop_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_heap_pop( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#endif
#endif

// heap replace
#if defined( _CSEQ_FEATURE_HEAPREPLACE_METHOD )
static int ComlibSequence_heap_replace_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *popped, _CSEQ_ELEMENT_TYPE *pushed );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_heap_replace( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *popped, _CSEQ_ELEMENT_TYPE *pushed );
#endif
#endif

// heap push top-k
#if defined( _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_heap_pushtopk_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *candidate, _CSEQ_ELEMENT_TYPE *discarded );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static _CSEQ_ELEMENT_TYPE * ComlibSequence_heap_pushtopk( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *candidate, _CSEQ_ELEMENT_TYPE *discarded );
#endif
#endif

// discard
#if defined( _CSEQ_FEATURE_DISCARD_METHOD )
static void ComlibSequence_discard_nolock( _CSEQ_TYPENAME *self, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static void ComlibSequence_discard( _CSEQ_TYPENAME *self, int64_t element_count );
#endif
#endif

// unwrite
#if defined( _CSEQ_FEATURE_UNWRITE_METHOD )
static int64_t ComlibSequence_unwrite_nolock( _CSEQ_TYPENAME *self, int64_t element_count );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_unwrite( _CSEQ_TYPENAME *self, int64_t element_count );
#endif
#endif

// pop
#if defined( _CSEQ_FEATURE_POP_METHOD )
static int ComlibSequence_pop_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int ComlibSequence_pop( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest );
#endif
#endif

// dump to stream
#if defined( _CSEQ_FEATURE_DUMP_METHOD )
static int64_t ComlibSequence_dump_nolock( const _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_dump( const _CSEQ_TYPENAME *self );
#endif
#endif

// flush to stream (empty buffer after flush)
#if defined( _CSEQ_FEATURE_FLUSH_METHOD )
static int64_t ComlibSequence_flush_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_flush( _CSEQ_TYPENAME *self );
#endif
#endif

// sort
#if defined( _CSEQ_FEATURE_SORT_METHOD )
static bool ComlibSequence_sort_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static bool ComlibSequence_sort( _CSEQ_TYPENAME *self );
#endif
#endif

// reverse
#if defined( _CSEQ_FEATURE_REVERSE_METHOD )
static bool ComlibSequence_reverse_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static bool ComlibSequence_reverse( _CSEQ_TYPENAME *self );
#endif
#endif

// optimize
#if defined( _CSEQ_FEATURE_OPTIMIZE_METHOD )
static int64_t ComlibSequence_optimize_nolock( _CSEQ_TYPENAME *self );
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
static int64_t ComlibSequence_optimize( _CSEQ_TYPENAME *self );
#endif
#endif





#if defined( _CSEQ_CIRCULAR_BUFFER )

#define __begin_general_buffer_section( CircularQueue ) \
  do { \
    register uintptr_t __base__ = (uintptr_t)((CircularQueue)->_buffer); \
    register uintptr_t __mask__ = (CircularQueue)->_mask; \
    do

#define __end_general_buffer_section \
    WHILE_ZERO; \
  } WHILE_ZERO


#define __begin_buffer_section( CircularQueue, Cursor ) \
  do { \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = (Cursor);  \
    register uintptr_t __base__ = (uintptr_t)((CircularQueue)->_buffer); \
    register uintptr_t __mask__ = (CircularQueue)->_mask; \
    do

#define __end_buffer_section \
    WHILE_ZERO; \
  } WHILE_ZERO


#define __begin_read_buffer_section( CircularQueue ) \
  do { \
    _CSEQ_TYPENAME *__Q__ = (CircularQueue); \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = __Q__->_rp;  \
    register uintptr_t __base__ = (uintptr_t)(__Q__->_buffer); \
    register uintptr_t __mask__ = __Q__->_mask; \
    do

#define __end_read_buffer_section \
    WHILE_ZERO; \
    __Q__->_rp = __cursor__;  \
  } WHILE_ZERO


#define __begin_write_buffer_section( CircularQueue ) \
  do { \
    _CSEQ_TYPENAME *__Q__ = (CircularQueue); \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = __Q__->_wp;  \
    register uintptr_t __base__ = (uintptr_t)(__Q__->_buffer); \
    register uintptr_t __mask__ = __Q__->_mask; \
    do

#define __end_write_buffer_section \
    WHILE_ZERO; \
    __Q__->_wp = __cursor__;  \
  } WHILE_ZERO

#define __cursor __cursor__


#define __general_cursor_guard( Cursor ) \
  ((Cursor) = (_CSEQ_ELEMENT_TYPE*)(((uintptr_t)(Cursor) & __mask__ ) | __base__))


#define __cursor_inc \
  (__cursor__ = (_CSEQ_ELEMENT_TYPE*)(((uintptr_t)(__cursor__+1) & __mask__ ) | __base__))


#define __cursor_dec \
  (__cursor__ = (_CSEQ_ELEMENT_TYPE*)(((uintptr_t)(__cursor__-1) & __mask__ ) | __base__))




#define __guard( CircularQueue, Cursor ) \
  ((Cursor) = (_CSEQ_ELEMENT_TYPE*)(( (uintptr_t)(Cursor) & (CircularQueue)->_mask ) | (uintptr_t)((CircularQueue)->_buffer) ))


#define __guarded_add( CircularQueue, Cursor, Amount )  \
  do {                                                  \
    (Cursor) += (Amount);                               \
    __guard( CircularQueue, Cursor );                   \
  } WHILE_ZERO


#define __guarded_inc( CircularQueue, Cursor )          \
  do {                                                  \
    (Cursor)++;                                         \
    __guard( CircularQueue, Cursor );                   \
  } WHILE_ZERO


#define __guarded_dec( CircularQueue, Cursor )          \
  do {                                                  \
    (Cursor)--;                                         \
    __guard( CircularQueue, Cursor );                   \
  } WHILE_ZERO




#elif defined( _CSEQ_LINEAR_BUFFER )

#define __begin_general_buffer_section( LinearQueue ) \
  do

#define __end_general_buffer_section \
  WHILE_ZERO


#define __begin_buffer_section( LinearQueue, Cursor ) \
  do { \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = (Cursor);  \
    do

#define __end_buffer_section \
    WHILE_ZERO; \
  } WHILE_ZERO


#define __begin_read_buffer_section( LinearQueue ) \
  do { \
    _CSEQ_TYPENAME *__Q__ = (LinearQueue); \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = __Q__->_rp;  \
    do

#define __end_read_buffer_section \
    WHILE_ZERO; \
    __Q__->_rp = __cursor__;  \
  } WHILE_ZERO


#define __begin_write_buffer_section( LinearQueue ) \
  do { \
    _CSEQ_TYPENAME *__Q__ = (LinearQueue); \
    register _CSEQ_ELEMENT_TYPE * __cursor__ = __Q__->_wp;  \
    do

#define __end_write_buffer_section \
    WHILE_ZERO; \
    __Q__->_wp = __cursor__;  \
  } WHILE_ZERO

#define __cursor __cursor__

#define __general_cursor_guard( Cursor )

#define __cursor_inc (__cursor__++)

#define __cursor_dec (__cursor__--)

#define __guard( LinearQueue, Cursor )

#define __guarded_add( LinearQueue, Cursor, Amount )  \
  do {                                                \
    (Cursor) += (Amount);                             \
  } WHILE_ZERO

#define __guarded_inc( LinearQueue, Cursor )    ((Cursor)++)

#define __guarded_dec( LinearQueue, Cursor )    ((Cursor)--)


#else
#error Buffer implementation undefined
#endif





#if _CSEQ_IS_PRIMITIVE == 1

__inline static void __copy_element( _CSEQ_ELEMENT_TYPE *dest, _CSEQ_ELEMENT_TYPE *src ) {
  memcpy( dest, src, sizeof( _CSEQ_ELEMENT_TYPE ) );
}
__inline static void __set_element_int( _CSEQ_ELEMENT_TYPE *e, _CSEQ_ELEMENT_TYPE v ) { *e = v; }
__inline static _CSEQ_ELEMENT_TYPE __get_element_int( _CSEQ_ELEMENT_TYPE *e ) { return *e; }
__inline static void __print_element( _CSEQ_ELEMENT_TYPE *e ) {
  switch( sizeof( _CSEQ_ELEMENT_TYPE ) ) {
  case 1:
    printf( "uint8_t @ %p = %02X\n", e, *((uint8_t*)e) );
    break;
  case 2:
    printf( "uint16_t @ %p = %04X\n", e, *((uint16_t*)e) );
    break;
  case 4:
    printf( "uint32_t @ %p = %08X\n", e, *((uint32_t*)e) );
    break;
  case 8:
    printf( "uint64_t @ %p = %016llX\n", e, *((uint64_t*)e) );
    break;
  default:
    break;
  }
}

__inline static void __random_element( _CSEQ_ELEMENT_TYPE *dest ) {
  switch( sizeof( _CSEQ_ELEMENT_TYPE ) ) {
  case 1:
    *((uint8_t*)dest) = rand8();
    break;
  case 2:
    *((uint16_t*)dest) = rand16();
    break;
  case 4:
    *((uint32_t*)dest) = rand32();
    break;
  case 8:
    *((uint64_t*)dest) = rand64();
    break;
  default:
    break;
  }
}
__inline static int __eq_element( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) { return *e1 == *e2; }
__inline static void __set_zero_element( _CSEQ_ELEMENT_TYPE *e ) { *e = 0; }
__inline static int __is_zero_element( const _CSEQ_ELEMENT_TYPE *e ) { return *e == 0; }
__inline static _CSEQ_ELEMENT_TYPE * __swap_elements( _CSEQ_ELEMENT_TYPE *e1, _CSEQ_ELEMENT_TYPE *e2 ) {
  _CSEQ_ELEMENT_TYPE tmp = *e1;
  *e1 = *e2;
  *e2 = tmp;
  return e1;
}
static int __compare_elements_default( const _CSEQ_ELEMENT_TYPE *e1, const _CSEQ_ELEMENT_TYPE *e2 ) {
  return (*e1 > *e2) - (*e1 < *e2);
}
#endif



/*******************************************************************//**
 *
 ***********************************************************************
 */
static const _CSEQ_ELEMENT_TYPE * __ComlibSequence_shift_cursor( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE **cursor, int64_t element_amount ) {
  int64_t bsize = 1ULL << self->_capacity_exp;

  // Allow only rp/wp of self to be modified
  if( cursor != &self->_rp && cursor != &self->_wp ) {
    return NULL; // illegal
  }

  if( element_amount != 0 ) {
    if( element_amount >= bsize || element_amount <= -bsize ) {
      // silly edge case
      element_amount = element_amount % bsize;
    }

    // shift cursor (positive or negative)
    __guarded_add( self, *cursor, element_amount );

    // correct size
    if( self->_rp <= self->_wp ) {
      self->_size = (int64_t)(self->_wp - self->_rp); // NOTE: when rp==wp size=0
    }
#if defined( _CSEQ_CIRCULAR_BUFFER )
    else {
      self->_size = (int64_t)(bsize - (self->_rp - self->_wp));
    }
#endif
  }

  return *cursor;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int ComlibSequence_cmpid( const _CSEQ_TYPENAME *self, const void *idptr ) {
  const _CSEQ_TYPENAME *other = (const _CSEQ_TYPENAME*)idptr;
  return (self > other) - (other > self);
}

/*******************************************************************//**
 *
 ***********************************************************************
 */
static _CSEQ_TYPENAME * ComlibSequence_getid( _CSEQ_TYPENAME *self ) {
  return self;
}

/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t ComlibSequence_serialize( const _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *out_queue ) {
  return 0; // not supported
}

/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static _CSEQ_TYPENAME * ComlibSequence_deserialize( comlib_object_t *container, _CSEQ_TYPENAME *in_queue ) {
  return NULL; // not supported
}

/*******************************************************************//**
 *
 ***********************************************************************
 */
static _CSEQ_TYPENAME * ComlibSequence_constructor( const void *identifier, _CSEQ_CONSTRUCTOR_ARGSTYPE *args );

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void ComlibSequence_destructor( _CSEQ_TYPENAME *self );


struct s_CStringQueue_t;

/*******************************************************************//**
 *
 ***********************************************************************
 */
static struct s_CStringQueue_t * ComlibSequence_representer( const _CSEQ_TYPENAME* self, struct s_CStringQueue_t *output );

/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static struct s_comlib_object_t * ComlibSequence_allocator( const _CSEQ_TYPENAME* self );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_VTABLETYPE ComlibSequenceMethods = {
  /* common cxlib_object_vtable interface */
  .vm_cmpid                 = (f_object_comparator_t)ComlibSequence_cmpid,          /* vm_cmpid       */
  .vm_getid                 = (f_object_identifier_t)ComlibSequence_getid,          /* vm_getid       */
  .vm_serialize             = (f_object_serializer_t)ComlibSequence_serialize,      /* vm_serialize   */
  .vm_deserialize           = (f_object_deserializer_t)ComlibSequence_deserialize,  /* vm_deserialize */
  .vm_construct             = (f_object_constructor_t)ComlibSequence_constructor,   /* vm_construct   */
  .vm_destroy               = (f_object_destructor_t)ComlibSequence_destructor,     /* vm_destroy     */
  .vm_represent             = (f_object_representer_t)ComlibSequence_representer,   /* vm_represent   */
  .vm_allocator             = (f_object_allocator_t)ComlibSequence_allocator,       /* vm_allocator   */
  /* ComlibSequence methods */
  .SetCapacity              = ComlibSequence_set_capacity,              /* SetCapacity            */
#if defined( _CSEQ_FEATURE_STREAM_IO )
#if defined( _CSEQ_FEATURE_ATTACHINPUTDESCRIPTOR_METHOD )
  .AttachInputDescriptor    = ComlibSequence_attach_input_descriptor,   /* AttachInputDescriptor  */
#endif
#if defined( _CSEQ_FEATURE_ATTACHOUTPUTDESCRIPTOR_METHOD )
  .AttachOutputDescriptor   = ComlibSequence_attach_output_descriptor,  /* AttachOutputDescriptor */
#endif
#if defined( _CSEQ_FEATURE_ATTACHINPUTSTREAM_METHOD )
  .AttachInputStream        = ComlibSequence_attach_input_stream,       /* AttachInputStream      */
#endif
#if defined( _CSEQ_FEATURE_ATTACHOUTPUTSTREAM_METHOD )
  .AttachOutputStream       = ComlibSequence_attach_output_stream,      /* AttachOutputStream     */
#endif
#if defined( _CSEQ_FEATURE_DETACHINPUT_METHOD )
  .DetachInput              = ComlibSequence_detach_input,              /* DetachInput            */
#endif
#if defined( _CSEQ_FEATURE_DETACHOUTPUT_METHOD )
  .DetachOutput             = ComlibSequence_detach_output,             /* DetachOutput           */
#endif
#if defined( _CSEQ_FEATURE_DETACHOUTPUTNOFLUSH_METHOD )
  .DetachOutputNoFlush      = ComlibSequence_detach_output_noflush,     /* DetachOutputNoFlush    */
#endif
#if defined( _CSEQ_FEATURE_HASINPUTDESCRIPTOR_METHOD )
  .HasInputDescriptor       = ComlibSequence_has_input_descriptor,      /* HasInputDescriptor     */
#endif
#if defined( _CSEQ_FEATURE_HASOUTPUTDESCRIPTOR_METHOD )
  .HasOutputDescriptor      = ComlibSequence_has_output_descriptor,     /* HasOutputDescriptor    */
#endif
#endif
#if defined( _CSEQ_FEATURE_CAPACITY_METHOD )
  .Capacity                 = ComlibSequence_capacity,                  /* Capacity               */
#endif
#if defined( _CSEQ_FEATURE_LENGTH_METHOD )
  .Length                   = ComlibSequence_length,                    /* Length                 */
#endif
#if defined( _CSEQ_FEATURE_REMAIN_METHOD )
  .Remain                   = ComlibSequence_remain,                    /* Remain                 */
#endif

/* ONLY NOLOCK VERSIONS */
#if !defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
#if defined( _CSEQ_FEATURE_INDEX_METHOD )
  .Index                    = ComlibSequence_index_nolock,              /* IndexNolock            */
#endif
#if defined( _CSEQ_FEATURE_OCC_METHOD )
  .Occ                      = ComlibSequence_occ_nolock,                /* OccNolock              */
#endif
#if defined( _CSEQ_FEATURE_PEEK_METHOD )
  .Peek                     = ComlibSequence_peek_nolock,               /* PeekNolock             */
#endif
#if defined( _CSEQ_FEATURE_EXPECT_METHOD )
  .Expect                   = ComlibSequence_expect_nolock,             /* ExpectNolock           */
#endif
#if defined( _CSEQ_FEATURE_INITIALIZE_METHOD )
  .Initialize               = ComlibSequence_initialize_nolock,         /* InitializeNolock       */
#endif
#if defined( _CSEQ_FEATURE_DEADSPACE_METHOD )
  .DeadSpace                = ComlibSequence_deadspace_nolock,          /* DeadSpaceNolock        */
#endif
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
#if defined( _CSEQ_CIRCULAR_BUFFER )
  .Write                    = ComlibSequence_write_nolock,              /* WriteNolock            */
#elif defined( _CSEQ_LINEAR_BUFFER )
  .Extend                   = ComlibSequence_write_nolock,              /* Extend                 */
#endif
#endif
#if defined( _CSEQ_FEATURE_SET_METHOD )
  .Set                      = ComlibSequence_set_nolock,                /* SetNolock              */
#endif
#if defined( _CSEQ_FEATURE_APPEND_METHOD )
  .Append                   = ComlibSequence_append_nolock,             /* AppendNolock           */
#endif
#if defined( _CSEQ_FEATURE_FORMAT_METHOD )
  .Format                   = ComlibSequence_format_nolock,             /* FormatNolock           */
#endif
#if defined( _CSEQ_FEATURE_NULTERM_METHOD )
  .NulTerm                  = ComlibSequence_nul_term_nolock,           /* NulTermNolock          */
#endif
#if defined( _CSEQ_FEATURE_NEXT_METHOD )
  .Next                     = ComlibSequence_next_nolock,               /* NextNolock             */
#endif
#if defined( _CSEQ_FEATURE_READ_METHOD )
  .Read                     = ComlibSequence_read_nolock,               /* ReadNolock             */
#endif
#if defined( _CSEQ_FEATURE_CURSOR_METHOD )
  .Cursor                   = ComlibSequence_cursor_nolock,             /* CursorNolock           */
#endif
#if defined( _CSEQ_FEATURE_GET_METHOD )
  .Get                      = ComlibSequence_get_nolock,                /* GetNolock              */
#endif
#if defined( _CSEQ_FEATURE_READUNTIL_METHOD )
  .ReadUntil                = ComlibSequence_readuntil_nolock,          /* ReadUntilNolock        */
#endif
#if defined( _CSEQ_FEATURE_READLINE_METHOD )
  .Readline                 = ComlibSequence_readline_nolock,           /* ReadlineNolock         */
#endif
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  .Unread                   = ComlibSequence_unread_nolock,             /* UnreadNolock           */
#endif
#if defined( _CSEQ_FEATURE_TRUNCATE_METHOD )
  .Truncate                 = ComlibSequence_truncate_nolock,           /* TruncateNolock         */
#endif
#if defined( _CSEQ_FEATURE_CLEAR_METHOD )
  .Clear                    = ComlibSequence_clear_nolock,              /* ClearNolock            */
#endif
#if defined( _CSEQ_FEATURE_RESET_METHOD )
  .Reset                    = ComlibSequence_reset_nolock,              /* ResetNolock            */
#endif
#if defined( _CSEQ_FEATURE_GETVALUE_METHOD )
  .GetValue                 = ComlibSequence_getvalue_nolock,           /* GetValueNolock         */
#endif
#if defined( _CSEQ_FEATURE_ABSORB_METHOD )
  .Absorb                   = ComlibSequence_absorb_nolock,             /* AbsorbNolock           */
#endif
#if defined( _CSEQ_FEATURE_ABSORBUNTIL_METHOD )
  .AbsorbUntil              = ComlibSequence_absorb_until_nolock,       /* AbsorbUntilNolock      */
#endif
#if defined( _CSEQ_FEATURE_CLONEINTO_METHOD )
  .CloneInto                = ComlibSequence_clone_into_nolock,         /* CloneIntoNolock        */
#endif
#if defined( _CSEQ_FEATURE_TRANSPLANTFROM_METHOD )
  .TransplantFrom           = ComlibSequence_transplant_from_nolock,    /* TransplantFromNolock   */
#endif
#if defined( _CSEQ_FEATURE_YANKBUFFER_METHOD )
  .YankBuffer               = ComlibSequence_yank_buffer_nolock,        /* YankBufferNolock       */
#endif
#if defined( _CSEQ_FEATURE_HEAPIFY_METHOD )
  .Heapify                  = ComlibSequence_heapify_nolock,            /* HeapifyNolock          */
#endif
#if defined( _CSEQ_FEATURE_HEAPTOP_METHOD )
  .HeapTop                  = ComlibSequence_heap_top_nolock,           /* HeapTopNolock          */
#endif
#if defined( _CSEQ_FEATURE_HEAPPUSH_METHOD )
  .HeapPush                 = ComlibSequence_heap_push_nolock,          /* HeapPushNolock         */
#endif
#if defined( _CSEQ_FEATURE_HEAPPOP_METHOD )
  .HeapPop                  = ComlibSequence_heap_pop_nolock,           /* HeapPopNolock          */
#endif
#if defined( _CSEQ_FEATURE_HEAPREPLACE_METHOD )
  .HeapReplace              = ComlibSequence_heap_replace_nolock,       /* HeapReplaceNolock      */
#endif
#if defined( _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD )
  .HeapPushTopK             = ComlibSequence_heap_pushtopk_nolock,      /* HeapPushTopKNolock      */
#endif
#if defined( _CSEQ_FEATURE_DISCARD_METHOD )
  .Discard                  = ComlibSequence_discard_nolock,            /* DiscardNolock          */
#endif
#if defined( _CSEQ_FEATURE_UNWRITE_METHOD )
  .Unwrite                  = ComlibSequence_unwrite_nolock,            /* UnwriteNolock          */
#endif
#if defined( _CSEQ_FEATURE_POP_METHOD )
  .Pop                      = ComlibSequence_pop_nolock,                /* PopNolock              */
#endif
#if defined( _CSEQ_FEATURE_DUMP_METHOD )
  .Dump                     = ComlibSequence_dump_nolock,               /* DumpNolock             */
#endif
#if defined( _CSEQ_FEATURE_FLUSH_METHOD )
  .Flush                    = ComlibSequence_flush_nolock,              /* FlushNolock            */
#endif
#if defined( _CSEQ_FEATURE_SORT_METHOD )
  .Sort                     = ComlibSequence_sort_nolock,               /* SortNolock             */
#endif
#if defined( _CSEQ_FEATURE_REVERSE_METHOD )
  .Reverse                  = ComlibSequence_reverse_nolock,            /* ReverseNolock          */
#endif
#if defined( _CSEQ_FEATURE_OPTIMIZE_METHOD )
  .Optimize                 = ComlibSequence_optimize_nolock,           /* OptimizeNolock         */
#endif

#else

/* BOTH SYNCHRONIZED AND NOLOCK VERSIONS */
#if defined( _CSEQ_FEATURE_INDEX_METHOD )
  .IndexNolock              = ComlibSequence_index_nolock,              /* IndexNolock            */
  .Index                    = ComlibSequence_index,                     /* Index                  */
#endif
#if defined( _CSEQ_FEATURE_OCC_METHOD )
  .OccNolock                = ComlibSequence_occ_nolock,                /* OccNolock              */
  .Occ                      = ComlibSequence_occ,                       /* Occ                    */
#endif
#if defined( _CSEQ_FEATURE_PEEK_METHOD )
  .PeekNolock               = ComlibSequence_peek_nolock,               /* PeekNolock             */
  .Peek                     = ComlibSequence_peek,                      /* Peek                   */
#endif
#if defined( _CSEQ_FEATURE_EXPECT_METHOD )
  .ExpectNolock             = ComlibSequence_expect_nolock,             /* ExpectNolock           */
  .Expect                   = ComlibSequence_expect,                    /* Expect                 */
#endif
#if defined( _CSEQ_FEATURE_INITIALIZE_METHOD )
  .InitializeNolock         = ComlibSequence_initialize_nolock,         /* InitializeNolock       */
  .Initialize               = ComlibSequence_initialize,                /* Initialize             */
#endif
#if defined( _CSEQ_FEATURE_DEADSPACE_METHOD )
  .DeadSpaceNolock          = ComlibSequence_deadspace_nolock,          /* DeadSpaceNolock         */
  .DeadSpace                = ComlibSequence_deadspace,                 /* DeadSpace               */
#endif
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
#if defined( _CSEQ_CIRCULAR_BUFFER )
  .WriteNolock              = ComlibSequence_write_nolock,              /* WriteNolock            */
  .Write                    = ComlibSequence_write,                     /* Write                  */
#elif defined( _CSEQ_LINEAR_BUFFER )
  .ExtendNolock             = ComlibSequence_write_nolock,              /* ExtendNolock           */
  .Extend                   = ComlibSequence_write,                     /* Extend                 */
#endif
#endif
#if defined( _CSEQ_FEATURE_SET_METHOD )
  .SetNolock                = ComlibSequence_set_nolock,                /* SetNolock             */
  .Set                      = ComlibSequence_set,                       /* Set                   */
#endif
#if defined( _CSEQ_FEATURE_APPEND_METHOD )
  .AppendNolock             = ComlibSequence_append_nolock,             /* AppendNolock           */
  .Append                   = ComlibSequence_append,                    /* Append                 */
#endif
#if defined( _CSEQ_FEATURE_FORMAT_METHOD )
  .FormatNolock             = ComlibSequence_format_nolock,             /* FormatNolock           */
  .Format                   = ComlibSequence_format,                    /* Format                 */
#endif
#if defined( _CSEQ_FEATURE_NULTERM_METHOD )
  .NulTermNolock            = ComlibSequence_nul_term_nolock,           /* NulTermNolock          */
  .NulTerm                  = ComlibSequence_nul_term,                  /* NulTerm                */
#endif
#if defined( _CSEQ_FEATURE_NEXT_METHOD )
  .NextNolock               = ComlibSequence_next_nolock,               /* NextNolock             */
  .Next                     = ComlibSequence_next,                      /* Next                   */
#endif
#if defined( _CSEQ_FEATURE_READ_METHOD )
  .ReadNolock               = ComlibSequence_read_nolock,               /* ReadNolock             */
  .Read                     = ComlibSequence_read,                      /* Read                   */
#endif
#if defined( _CSEQ_FEATURE_CURSOR_METHOD )
  .CursorNolock             = ComlibSequence_cursor_nolock,             /* CursorNolock           */
  .Cursor                   = ComlibSequence_cursor,                    /* Cursor                 */
#endif
#if defined( _CSEQ_FEATURE_GET_METHOD )
  .GetNolock                = ComlibSequence_get_nolock,                /* GetNolock              */
  .Get                      = ComlibSequence_get,                       /* Get                    */
#endif
#if defined( _CSEQ_FEATURE_READUNTIL_METHOD )
  .ReadUntilNolock          = ComlibSequence_readuntil_nolock,          /* ReadUntilNolock        */
  .ReadUntil                = ComlibSequence_readuntil,                 /* ReadUntil              */
#endif
#if defined( _CSEQ_FEATURE_READLINE_METHOD )
  .ReadlineNolock           = ComlibSequence_readline_nolock,           /* ReadlineNolock         */
  .Readline                 = ComlibSequence_readline,                  /* Readline               */
#endif
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  .UnreadNolock             = ComlibSequence_unread_nolock,             /* UnreadNolock           */
  .Unread                   = ComlibSequence_unread,                    /* Unread                 */
#endif
#if defined( _CSEQ_FEATURE_TRUNCATE_METHOD )
  .TruncateNolock           = ComlibSequence_truncate_nolock,           /* TruncateNolock         */
  .Truncate                 = ComlibSequence_truncate,                  /* Truncate               */
#endif
#if defined( _CSEQ_FEATURE_CLEAR_METHOD )
  .ClearNolock              = ComlibSequence_clear_nolock,              /* ClearNolock            */
  .Clear                    = ComlibSequence_clear,                     /* Clear                  */
#endif
#if defined( _CSEQ_FEATURE_RESET_METHOD )
  .ResetNolock              = ComlibSequence_reset_nolock,              /* ResetNolock            */
  .Reset                    = ComlibSequence_reset,                     /* Reset                  */
#endif
#if defined( _CSEQ_FEATURE_GETVALUE_METHOD )
  .GetValueNolock           = ComlibSequence_getvalue_nolock,           /* GetValueNolock         */
  .GetValue                 = ComlibSequence_getvalue,                  /* GetValue               */
#endif
#if defined( _CSEQ_FEATURE_ABSORB_METHOD )
  .AbsorbNolock             = ComlibSequence_absorb_nolock,             /* AbsorbNolock           */
  .Absorb                   = ComlibSequence_absorb,                    /* Absorb                 */
#endif
#if defined( _CSEQ_FEATURE_ABSORBUNTIL_METHOD )
  .AbsorbUntilNolock        = ComlibSequence_absorb_until_nolock,       /* AbsorbUntilNolock      */
  .AbsorbUntil              = ComlibSequence_absorb_until,              /* AbsorbUntil            */
#endif
#if defined( _CSEQ_FEATURE_CLONEINTO_METHOD )
  .CloneIntoNolock          = ComlibSequence_clone_into_nolock,         /* CloneIntoNolock        */
  .CloneInto                = ComlibSequence_clone_into,                /* CloneInto              */
#endif
#if defined( _CSEQ_FEATURE_TRANSPLANTFROM_METHOD )
  .TransplantFromNolock     = ComlibSequence_transplant_from_nolock,    /* TransplantFromNolock   */
  .TransplantFrom           = ComlibSequence_transplant_from,           /* TransplantFrom         */
#endif
#if defined( _CSEQ_FEATURE_YANKBUFFER_METHOD )
  .YankBufferNolock         = ComlibSequence_yank_buffer_nolock,        /* YankBufferNolock       */
  .YankBuffer               = ComlibSequence_yank_buffer,               /* YankBuffer             */
#endif
#if defined( _CSEQ_FEATURE_HEAPIFY_METHOD )
  .HeapifyNolock            = ComlibSequence_heapify_nolock,            /* HeapifyNolock          */
  .Heapify                  = ComlibSequence_heapify,                   /* Heapify                */
#endif
#if defined( _CSEQ_FEATURE_HEAPTOP_METHOD )
  .HeapTopNolock            = ComlibSequence_heap_top_nolock,           /* HeapTopNolock          */
  .HeapTop                  = ComlibSequence_heap_top,                  /* HeapTop                */
#endif
#if defined( _CSEQ_FEATURE_HEAPPUSH_METHOD )
  .HeapPushNolock           = ComlibSequence_heap_push_nolock,          /* HeapPushNolock         */
  .HeapPush                 = ComlibSequence_heap_push,                 /* HeapPush               */
#endif
#if defined( _CSEQ_FEATURE_HEAPPOP_METHOD )
  .HeapPopNolock            = ComlibSequence_heap_pop_nolock,           /* HeapPopNolock          */
  .HeapPop                  = ComlibSequence_heap_pop,                  /* HeapPop                */
#endif
#if defined( _CSEQ_FEATURE_HEAPREPLACE_METHOD )
  .HeapReplaceNolock        = ComlibSequence_heap_replace_nolock,       /* HeapReplaceNolock      */
  .HeapReplace              = ComlibSequence_heap_replace,              /* HeapReplace            */
#endif
#if defined( _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD )
  .HeapPushTopKNolock       = ComlibSequence_heap_pushtopk_nolock,      /* HeapPushTopKNolock      */
  .HeapPushTopK             = ComlibSequence_heap_pushtopk,             /* HeapPushTopK            */
#endif
#if defined( _CSEQ_FEATURE_DISCARD_METHOD )
  .DiscardNolock            = ComlibSequence_discard_nolock,            /* DiscardNolock          */
  .Discard                  = ComlibSequence_discard,                   /* Discard                */
#endif
#if defined( _CSEQ_FEATURE_UNWRITE_METHOD )
  .UnwriteNolock            = ComlibSequence_unwrite_nolock,            /* UnwriteNolock          */
  .Unwrite                  = ComlibSequence_unwrite,                   /* Unwrite                */
#endif
#if defined( _CSEQ_FEATURE_POP_METHOD )
  .PopNolock                = ComlibSequence_pop_nolock,                /* PopNolock              */
  .Pop                      = ComlibSequence_pop,                       /* Pop                    */
#endif
#if defined( _CSEQ_FEATURE_DUMP_METHOD )
  .DumpNolock               = ComlibSequence_dump_nolock,               /* DumpNolock             */
  .Dump                     = ComlibSequence_dump,                      /* Dump                   */
#endif
#if defined( _CSEQ_FEATURE_FLUSH_METHOD )
  .FlushNolock              = ComlibSequence_flush_nolock,              /* FlushNolock            */
  .Flush                    = ComlibSequence_flush,                     /* Flush                  */
#endif
#if defined( _CSEQ_FEATURE_SORT_METHOD )
  .SortNolock               = ComlibSequence_sort_nolock,               /* SortNolock             */
  .Sort                     = ComlibSequence_sort,                      /* Sort                   */
#endif
#if defined( _CSEQ_FEATURE_REVERSE_METHOD )
  .ReverseNolock            = ComlibSequence_reverse_nolock,            /* ReverseNolock          */
  .Reverse                  = ComlibSequence_reverse,                   /* Reverse                */
#endif
#if defined( _CSEQ_FEATURE_OPTIMIZE_METHOD )
  .OptimizeNolock           = ComlibSequence_optimize_nolock,           /* OptimizeNolock         */
  .Optimize                 = ComlibSequence_optimize,                  /* Optimize               */
#endif
#endif

#if defined( _CSEQ_FEATURE_ERRORS )
#if defined( _CSEQ_FEATURE_GETERROR_METHOD )
  ComlibSequence_get_error                  /* GetError               */
#endif
#endif
};



/*******************************************************************//**
 * beautiful!
 *
 ***********************************************************************
 */
DLL_EXPORT _CSEQ_VTABLETYPE * _CSEQ_GLOBAL_VTABLE = &ComlibSequenceMethods;



#if defined( _CSEQ_FEATURE_ERRORS )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __ComlibSequence_has_errstr( _CSEQ_TYPENAME *self ) {
  return self->_error != NULL && self->_error != g_default_errstr;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __ComlibSequence_set_error( _CSEQ_TYPENAME *self, const char *errstr, ... ) {
  va_list args;
  if( errstr != NULL ) {
    if( !__ComlibSequence_has_errstr( self ) ) {
      if( (self->_error = (char*)malloc( COMLIBSEQUENCE_MAX_ERRSTRING_SIZE+1 )) == NULL ) {
        return -1;
      }
    }
    va_start( args, errstr );
    vsnprintf( self->_error, COMLIBSEQUENCE_MAX_ERRSTRING_SIZE, errstr, args );
    va_end( args );
  }
  return -1;
}
#endif



#if defined( _CSEQ_FEATURE_ERRORS )
#if defined( _CSEQ_FEATURE_GETERROR_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * ComlibSequence_get_error( _CSEQ_TYPENAME *self ) {
  return self->_error;
}
#endif
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __ComlibSequence_resize_nolock( _CSEQ_TYPENAME* self, int exp_delta, resize_direction_t direction ) {
  int retcode = 0;
  int new_exp = 0;
  int64_t new_element_capacity = 0;
  size_t alignment = 0;
  _CSEQ_ELEMENT_TYPE *buf = NULL;
  _CSEQ_ELEMENT_TYPE *wp = NULL;

  XTRY {

#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
    int64_t unread_size = __sizeof_unread( self );
    // Perform any undo-read we may have before we expand, so we can carry the undo-read over to the new buffer
    if( unread_size > 0 ) {
      ComlibSequence_unread_nolock( self, -1 );
    }
#endif

    if( exp_delta < 0 ) {
      THROW_ERROR( CXLIB_ERR_BUG, 0x101 );
    }

    // Compute new capacity
    switch( direction ) {
    case RESIZE_UP:
      if( (new_exp = self->_capacity_exp + exp_delta) > _CSEQ_MAX_CAPACITY_EXP ) {
        new_exp = _CSEQ_MAX_CAPACITY_EXP;
      }
      break;
    case RESIZE_DOWN:
      if( (new_exp = self->_capacity_exp - exp_delta) < _CSEQ_MIN_CAPACITY_EXP ) {
        new_exp = _CSEQ_MIN_CAPACITY_EXP;
      }
      break;
    default:
      THROW_ERROR( CXLIB_ERR_BUG, 0x102 );
      break;
    }

    // Compute new element capacity as power of two
    new_element_capacity = 1LL << new_exp;

    // Sanity check
    if( new_element_capacity < self->_size ) {
      THROW_ERROR( CXLIB_ERR_BUG, 0x103 );
    }

    // Aligned on own size
    alignment = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE );

    // Allocate new buffer, power of two size, aligned on its own size
    ALIGNED_ARRAY_THROWS( buf, _CSEQ_ELEMENT_TYPE, new_element_capacity, alignment, 0x104 );

    // Copy data from old buffer to new buffer
    wp = buf;
    __begin_read_buffer_section( self ) {
      for( int64_t i=0; i < self->_size; i++ ) {
        *wp++ = *__cursor;
        __cursor_inc;
      }
    } __end_read_buffer_section;

    // Replace old buffer with new buffer
    ALIGNED_FREE( self->_buffer );
    self->_buffer = buf;
    self->_rp = self->_buffer;
    self->_wp = wp;
    self->_capacity_exp = (uint8_t)new_exp;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
    if( unread_size > 0 ) {
      self->_rp += unread_size;
      self->_size -= unread_size;
      self->_prev_rp = self->_buffer;
    }
    else {
      self->_prev_rp = NULL;
    }
#endif
#if defined( _CSEQ_CIRCULAR_BUFFER )
    self->_mask = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE ) - 1;
#endif
    self->_load = self->_capacity_exp;
  }
  XCATCH( errcode ) {
    retcode = -1;

    COMLIB_OBJECT_REPR( self, NULL );

    int64_t physical_bytes = 0;
    int64_t use_pct = 0;
    int64_t use_bytes = 0;
    get_system_physical_memory(
              &physical_bytes,
              &use_pct,
              &use_bytes
            );

    INFO( 0x105, "Physical memory : %lld bytes", physical_bytes );
    INFO( 0x106, "Use percent     : %lld", use_pct );
    INFO( 0x107, "Use memory      : %lld bytes", use_bytes );
    INFO( 0x108, "New Exp         : %d", new_exp );
    INFO( 0x109, "New Capacity    : %lld", new_element_capacity );
    INFO( 0x10A, "Alignment       : %llu", (uint64_t)alignment );

    do {
      buf = NULL;
      ALIGNED_ARRAY( buf, _CSEQ_ELEMENT_TYPE, new_element_capacity, alignment );
      INFO( 0x10B, "Trying again with new_exp=%d, new_element_capacity=%lld, alignment=%llu, result=%p", new_exp, new_element_capacity, (uint64_t)alignment, buf );
      new_exp -= 1;
      new_element_capacity = 1LL << new_exp;
      alignment = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE );
      if( buf ) {
        ALIGNED_FREE( buf );
      }
    } while( new_exp >= 0 );

    FATAL( 0x10C, "resize failed!" );
  }
  XFINALLY {}



  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void _CSEQ_REGISTER_CLASS( void ) {

  X_COMLIB_REGISTER_CLASS( _CSEQ_TYPENAME, CXLIB_OBTYPE_SEQUENCE, _CSEQ_GLOBAL_VTABLE, OBJECT_IDENTIFIED_BY_ADDRESS, 0 );
  const size_t n_methods = sizeof( _CSEQ_VTABLETYPE ) / sizeof( void* );
  _CSEQ_VTABLETYPE *vtable = _CSEQ_GLOBAL_VTABLE;

  uintptr_t *data = (uintptr_t*)vtable;
  uintptr_t *end = data + n_methods;
  uintptr_t *cursor = data;
  while( cursor < end ) {
    if( *cursor++ == 0 ) {
      FATAL( 0x001, "invalid vtable for class '%s': one or more entries are NULL", __MACRO_TO_STRING( _CSEQ_TYPENAME ) );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void _CSEQ_UNREGISTER_CLASS( void ) {
  X_COMLIB_UNREGISTER_CLASS( _CSEQ_TYPENAME );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_TYPENAME * ComlibSequence_constructor( const void *identifier, _CSEQ_CONSTRUCTOR_ARGSTYPE *args ) {
  _CSEQ_TYPENAME *self;

  // allocate object memory
  if( CALIGNED_MALLOC( self, _CSEQ_TYPENAME ) == NULL ) {
    return NULL;
  }

  // Initialize object header
  if( X_COMLIB_OBJECT_INIT( _CSEQ_TYPENAME, self, self ) == NULL ) {
    ALIGNED_FREE( self );
    return NULL;
  }

  if( identifier != NULL ) {
    WARN( 0x111, "identifier not supported" );
  }

  // Initialize members
  self->_size = 0;
  self->_wp = NULL;
  self->_rp = NULL;
#if defined( _CSEQ_CIRCULAR_BUFFER )
  self->_mask = 0;
#endif
  self->_buffer = NULL;
  self->_capacity_exp = 0;
  self->_initial_exp = 0;
  self->_load = 0;
#if defined( _CSEQ_FEATURE_STREAM_IO )
  self->_fd_in = COMLIBSEQUENCE_FD_NONE;
  self->_fd_out = COMLIBSEQUENCE_FD_NONE;
  self->_flags.owns_fd_in = false;
  self->_flags.owns_fd_out = false;
  self->_prev_fpos = -1;
#endif
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  INIT_SPINNING_CRITICAL_SECTION( &self->_lock.lock, 4000 ); 
#endif
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  self->_prev_rp = NULL;
#endif
#if defined( _CSEQ_FEATURE_ERRORS )
  self->_error = NULL;
#endif
#if defined( _CSEQ_FEATURE_SORTABLE )
  self->_cmp = (args && args->comparator) ? args->comparator : __compare_elements_default;
#endif

  if( args ) {
    if( ComlibSequence_set_capacity( self, args->element_capacity ) != 0 ) {
      ComlibSequence_destructor( self );
      self = NULL;
    }
  }

  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_set_capacity( _CSEQ_TYPENAME *self, int64_t element_capacity ) {
  int retcode = 0;
  XTRY {
    int exp;
    int64_t initial_element_capacity;

    if( element_capacity == 0 ) {
      exp = _CSEQ_DEFAULT_CAPACITY_EXP;
    }
    else {
      exp = imag2( element_capacity );
      if( exp < _CSEQ_MIN_CAPACITY_EXP ) {
        exp = _CSEQ_MIN_CAPACITY_EXP;
      }
      else if( exp > _CSEQ_MAX_CAPACITY_EXP ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "requested capacity too large" );
#endif
        THROW_SILENT( CXLIB_ERR_CAPACITY, 0x121 );
      }
    }

    self->_capacity_exp = self->_initial_exp = (uint8_t)exp;

    initial_element_capacity = 1LL << self->_initial_exp;

    if( ALIGNED_ARRAY( self->_buffer, _CSEQ_ELEMENT_TYPE, initial_element_capacity, initial_element_capacity * sizeof(_CSEQ_ELEMENT_TYPE) ) == NULL ) {
#if defined( _CSEQ_FEATURE_ERRORS )
      __ComlibSequence_set_error( self, "out of memory" );
#endif
      THROW_SILENT( CXLIB_ERR_MEMORY, 0x122 );
    }
#if defined( _CSEQ_CIRCULAR_BUFFER )
    self->_mask = initial_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE ) - 1;
#endif

    self->_wp = self->_rp = self->_buffer;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
    self->_prev_rp = NULL;
#endif

  }
  XCATCH( errcode ) {
    if( self->_buffer ) {
      ALIGNED_FREE( self->_buffer );
      self->_buffer = NULL;
    }
    retcode = -1;
  }
  XFINALLY {}
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_destructor( _CSEQ_TYPENAME* self ) {
  if( self ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
    if( ComlibSequence_has_output_descriptor(self) ) {
      ComlibSequence_detach_output(self);
    }
    if( ComlibSequence_has_input_descriptor(self) ) {
      ComlibSequence_detach_input(self);
    }
#endif
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
    DEL_CRITICAL_SECTION( &self->_lock.lock );
#endif
#if defined( _CSEQ_FEATURE_STREAM_IO )
    if( __ComlibSequence_has_errstr(self) ) {
      free( self->_error );
      self->_error = NULL;
    }
#endif
    if( self->_buffer ) {
      ALIGNED_FREE( self->_buffer );
      self->_buffer = NULL;
    }
    ALIGNED_FREE( self );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static struct s_CStringQueue_t * ComlibSequence_representer( const _CSEQ_TYPENAME* self, struct s_CStringQueue_t *output ) {

#define write( Format, ... )  \
  do {                        \
    if( output ) {            \
      CALLABLE( output )->FormatNolock( output, Format, ##__VA_ARGS__ );  \
    }                         \
    else {                    \
      CXLIB_OSTREAM( Format, ##__VA_ARGS__ ); \
    }                         \
  } WHILE_ZERO


  // No output queue, write to stdout, we must lock it
  if( output == NULL ) {
    cxlib_ostream_lock();
  }

#define __get_string(x) x
#define __Xget_string(x) #x
#define MACRO_TO_STRING(x) __get_string( __Xget_string( x ) )

  const char TYPENAME[] = MACRO_TO_STRING( _CSEQ_TYPENAME );
  const char ELEMENT_TYPE[] = MACRO_TO_STRING( _CSEQ_ELEMENT_TYPE );
  uint64_t sz_elem = sizeof( _CSEQ_ELEMENT_TYPE );
  bool primitive = _CSEQ_IS_PRIMITIVE;
  _CSEQ_ELEMENT_TYPE *_end = self->_buffer + (1LL << self->_capacity_exp);

  // COMMON
  write( "===========================================================\n" );
  write( "ComlibSequence %s <%s> @ %p\n", TYPENAME, ELEMENT_TYPE, self );
  write( "===========================================================\n" );
  write( "  address       = %p\n", self );
  write( "  type          = %s\n", TYPENAME );
  write( "  element_type  = %s\n", ELEMENT_TYPE );
  write( "  element_size  = %llu bytes\n", sz_elem );
  write( "  primitive     = %s\n", primitive ? "yes" : "no" );
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // CIRCULAR
  write( "  layout        = circular\n" );
#elif defined( _CSEQ_LINEAR_BUFFER )
  // LINEAR
  write( "  layout        = linear\n" );
#endif
  write( "-----------------------------------------------------------\n" );
  write( "  _size         = %lld\n", self->_size );
  write( "  _wp           = %p\n", self->_wp );
  write( "  _rp           = %p\n", self->_rp );
  write( "  _buffer       = %p\n", self->_buffer );
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // CIRCULAR
  write( "  _mask         = %p\n", self->_mask );
#if defined( _CSEQ_FEATURE_STREAM_IO )
  write( "  _fd_in        = %d\n", self->_fd_in );
  write( "  _fd_out       = %d\n", self->_fd_out );
#endif
#elif defined( _CSEQ_LINEAR_BUFFER )
  // LINEAR
#endif
  // COMMON
  write( "  _capacity_exp = %u\n", self->_capacity_exp );
  write( "  _initial_exp  = %u\n", self->_initial_exp );
  write( "  _load         = %u\n", self->_load );
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // CIRCULAR
#if defined( _CSEQ_FEATURE_STREAM_IO )
  write( "  _flags.owns_fd_in  = %u\n", self->_flags.owns_fd_in );
  write( "  _flags.owns_fd_out = %u\n", self->_flags.owns_fd_out );
#endif
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  QWORD *qw = (QWORD*)&self->_lock;
  write( "  _lock         = %016llX %016llX %016llX %016llX %016llX\n", qw[0], qw[1], qw[2], qw[3], qw[4] );
#endif
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  write( "  _prev_rp      = %p\n", self->_prev_rp );
  write( "  _prev_fpos    = %lld\n", self->_prev_fpos );
#endif
#if defined( _CSEQ_FEATURE_ERRORS )
  write( "  _error        = %s\n", self->_error ? self->_error : "" );
#endif
#elif defined( _CSEQ_LINEAR_BUFFER )
  // LINEAR
#endif
  // COMMON
#if defined( _CSEQ_FEATURE_SORTABLE )
  cxlib_symbol_name symname;
  const char *cmp = "";
  if( self->_cmp ) {
    symname = cxlib_get_symbol_name( (uintptr_t)self->_cmp );
    cmp = (char*)symname.value;
  }

  write( "  _cmp          = %016llx (%s)\n", (uintptr_t)self->_cmp, cmp );
#endif
  write( "-----------------------------------------------------------\n" );

#if defined( _CSEQ_CIRCULAR_BUFFER )
  // CIRCULAR
  if( self->_wp < self->_rp ) {
    write( "[  %llu  <wp>  %llu  <rp>  %llu  ]\n", self->_wp - self->_buffer, self->_rp - self->_wp, _end - self->_rp );
  }
  else {
    write( "[  %llu  <rp>  %llu  <wp>  %llu  ]\n", self->_rp - self->_buffer, self->_wp - self->_rp, _end - self->_wp );
  }
#elif defined( _CSEQ_LINEAR_BUFFER )
  // LINEAR
    write( "[  %llu  <rp>  %llu  <wp>  %llu  ]\n", self->_rp - self->_buffer, self->_wp - self->_rp, _end - self->_wp );
#endif
  write( "===========================================================\n" );
  write( "\n" );
  
  // No output queue, write to stdout, we must release it
  if( output == NULL ) {
    cxlib_ostream_release();
  }

  return output;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static struct s_comlib_object_t * ComlibSequence_allocator( const _CSEQ_TYPENAME* self ) {
  return NULL;
}



#if defined( _CSEQ_FEATURE_STREAM_IO )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __ComlibSequence_attach_input( _CSEQ_TYPENAME *self, short fd ) {
  if( !ComlibSequence_has_input_descriptor(self) ) {
    self->_fd_in = fd;
    self->_prev_fpos = CX_TELL( fd ); // start at wherever the file is (typically 0)
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_attach_input_descriptor( _CSEQ_TYPENAME *self, short fd ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    if( (retcode = __ComlibSequence_attach_input( self, fd )) == 0 ) {
      // ok
      self->_flags.owns_fd_in = false;
    }
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_attach_input_stream( _CSEQ_TYPENAME *self, const char *fpath ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    int fd = -1;
    XTRY {
      errno_t err;

      if( ComlibSequence_has_input_descriptor(self) ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "input stream already exists, cannot attach new until existing is detached" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x131 );
      }
#endif

      if( file_exists( fpath ) == false ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "input file not found: '%s'", fpath );
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x132 );
      }
#endif

      if( (err = OPEN_R_SEQ( &fd, fpath )) != 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "cannot open file '%s': %s", fpath, strerror(err) );
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x133 );
      }
#endif

      if( __ComlibSequence_attach_input( self, (short)fd ) != 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "failed to attach input" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x134 );
      }
#endif

      self->_flags.owns_fd_in = true;
    }
    XCATCH( errcode ) {
      if( fd >= 0 ) {
        CX_CLOSE( fd );
      }
      if( self->_error ) {
        REASON( 0x135, "Failed to attach input stream: %s", self->_error );
      }
      retcode = -1;
    }
    XFINALLY {}
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __ComlibSequence_attach_output( _CSEQ_TYPENAME *self, short fd ) {
  if( !ComlibSequence_has_output_descriptor(self) ) {
    self->_fd_out = fd;
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_attach_output_descriptor( _CSEQ_TYPENAME *self, short fd ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    if( (retcode = __ComlibSequence_attach_output( self, fd )) == 0 ) {
      self->_flags.owns_fd_out = false;
    }
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_attach_output_stream( _CSEQ_TYPENAME *self, const char *fpath ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    int fd = -1;
    XTRY {
      errno_t err;

      if( ComlibSequence_has_output_descriptor(self) ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "output stream already exists, cannot attach new until existing is detached" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x141 );
      }
#endif

      if( (err = OPEN_W_SEQ( &fd, fpath )) != 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "cannot open file '%s': %s", fpath, strerror(err) );
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x142 );
      }
#endif

      if( __ComlibSequence_attach_output( self, (short)fd ) != 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "failed to attach output" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x143 );
      }
#endif

      self->_flags.owns_fd_out = true;
    }
    XCATCH( errcode ) {
      if( fd >= 0 ) {
        CX_CLOSE( fd );
      }
      if( self->_error ) {
        REASON( 0x144, "Failed to attach output stream: %s", self->_error );
      }
      retcode = -1;
    }
    XFINALLY {}
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_detach_input( _CSEQ_TYPENAME *self ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    if( ComlibSequence_has_input_descriptor(self) ) {
      // close input file if we own it
      if( self->_flags.owns_fd_in ) {
        CX_CLOSE( self->_fd_in );
      }

      // reset file descriptor
      self->_fd_in = COMLIBSEQUENCE_FD_NONE;
      self->_flags.owns_fd_in = false;
    }
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_detach_output( _CSEQ_TYPENAME *self ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    XTRY {

      if( ComlibSequence_has_output_descriptor(self) ) {
        // flush buffer to output before closing
#if defined( _CSEQ_FEATURE_FLUSH_METHOD )
        if( ComlibSequence_flush_nolock( self ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x151 );
        }
#else
#error This implementation requires _CSEQ_FEATURE_FLUSH_METHOD
#endif

        // close output file if we own it
        if( self->_flags.owns_fd_out ) {
          CX_CLOSE( self->_fd_out );
        }

        // reset file descriptor
        self->_fd_out = COMLIBSEQUENCE_FD_NONE;
        self->_flags.owns_fd_out = false;
      }
    }
    XCATCH( errcode ) {
      retcode = -1;
    }
    XFINALLY {}
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_detach_output_noflush( _CSEQ_TYPENAME *self ) {
  int retcode = 0;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  SYNCHRONIZE_ON( self->_lock ) {
#endif
    // close output file if we own it
    if( self->_flags.owns_fd_out ) {
      CX_CLOSE( self->_fd_out );
    }

    // reset file descriptor
    self->_fd_out = COMLIBSEQUENCE_FD_NONE;
    self->_flags.owns_fd_out = false;
#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
  } RELEASE;
#endif
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int ComlibSequence_has_input_descriptor( const _CSEQ_TYPENAME *self ) {
  return self->_fd_in > COMLIBSEQUENCE_FD_NONE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int ComlibSequence_has_output_descriptor( const _CSEQ_TYPENAME *self ) {
  return self->_fd_out > COMLIBSEQUENCE_FD_NONE;
}
#endif


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t ComlibSequence_capacity( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // Circular buffer's capacity is always the full allocated buffer
  return self->_buffer ? (1LL << self->_capacity_exp) : 0;
#elif defined( _CSEQ_LINEAR_BUFFER )
  // Linear buffer's capacity is the allocated buffer minus any gap created by a moved read pointer
  return self->_buffer ? (1LL << self->_capacity_exp) - (self->_rp - self->_buffer) : 0;
#endif
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_length( _CSEQ_TYPENAME *self ) {
  return self->_size;
}



#if defined( _CSEQ_FEATURE_REMAIN_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_remain( _CSEQ_TYPENAME *self ) {
  return ComlibSequence_capacity( self ) - self->_size;
}
#endif



#if defined( _CSEQ_FEATURE_INDEX_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_index_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen ) {
  int64_t i = 0;
  const _CSEQ_ELEMENT_TYPE *pp = probe;
  int64_t index = -1;
  
  if( probe == NULL || plen < 1 ) {
    return 0;
  }

  __begin_buffer_section( self, self->_rp ) {
    while( i++ < self->_size ) {
      if( __eq_element(pp, __cursor) || (pp > probe && __eq_element(pp=probe, __cursor) ) ) {
        if( (++pp - probe) == plen ) {
          index = i - plen; 
          break;
        }
      }
      __cursor_inc;
    }
  } __end_buffer_section;
  return index;
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_index( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen ) {
  int64_t index = -1;
  SYNCHRONIZE_ON( self->_lock ) {
    index = ComlibSequence_index_nolock( self, probe, plen );
  } RELEASE;
  return index;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_OCC_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_occ_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen ) {
  int64_t i = 0;
  const _CSEQ_ELEMENT_TYPE *pp = probe;
  int64_t occ = 0;
  
  if( probe == NULL || plen < 1 ) {
    return 0;
  }
    
  __begin_buffer_section( self, self->_rp ) {
    while( i++ < self->_size ) {
      if( __eq_element(pp, __cursor) || (pp > probe && __eq_element(pp=probe, __cursor)) ) {
        if( (++pp - probe) == plen ) {
          ++occ;
          pp = probe;
        }
      }
      __cursor_inc;
    }
  } __end_buffer_section;
    
  return occ;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_occ( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen ) {
  int64_t index = -1;
  SYNCHRONIZE_ON( self->_lock ) {
    index = ComlibSequence_occ_nolock( self, probe, plen );
  } RELEASE;
  return index;
}
#endif
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int64_t __sizeof_unread( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  int64_t capacity = ComlibSequence_capacity( self );
  if( self->_prev_rp ) {
    if( self->_prev_rp < self->_rp ) {
      return (int64_t)(self->_rp - self->_prev_rp);
    }
    else if( self->_prev_rp > self->_rp ) {
      return capacity - (int64_t)(self->_prev_rp - self->_rp);
    }
    else {
      return capacity;
    }
  }
#endif
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __expand_to_accomodate( _CSEQ_TYPENAME *self, int64_t element_count ) {
#if defined( _CSEQ_LINEAR_BUFFER )
  // Anything before a moved read pointer is unavailable for writing
  int64_t total_occupied = self->_wp - self->_buffer;
#else
  // Circular buffer has available everything that is not accounted for by size
  int64_t total_occupied = self->_size;
#endif
  // calculate the necessary expansion exponent
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  int exp_delta = imag2( total_occupied + __sizeof_unread( self ) + element_count) - self->_capacity_exp;
#else
  int exp_delta = imag2( total_occupied + element_count) - self->_capacity_exp;
#endif


  // sanity check
  if( self->_capacity_exp + exp_delta > _CSEQ_MAX_CAPACITY_EXP ) {
#if defined( _CSEQ_FEATURE_ERRORS )
    __ComlibSequence_set_error( self, "max capacity exceeded" );
#endif
    return -1;
  }

  // resize
  if( (__ComlibSequence_resize_nolock( self, exp_delta, RESIZE_UP )) < 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
    __ComlibSequence_set_error( self, "out of memory during buffer expansion" );
#endif
    return -1;
  }

  return element_count;
}



#if defined( _CSEQ_FEATURE_INITIALIZE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_initialize_nolock( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *value, int64_t element_count ) {
  int64_t element_capacity = ComlibSequence_capacity(self);
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    return -1; // can't initialize a sequence with input file descriptor
  }
#endif

  // Make sure sequence has enough space
  if( self->_size + element_count > element_capacity ) {
    if( __expand_to_accomodate( self, element_count ) != element_count ) {
      return -1;
    }
  }

  // Initialize specified number of elements to the given value
  __begin_write_buffer_section( self ) {
    for( int64_t i=0; i<element_count; i++ ) {
      *__cursor = *value;
      __cursor_inc;
    }
  } __end_write_buffer_section;
  self->_size += element_count;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  self->_prev_rp = NULL; // invalidate any read-undo
#endif
  return element_count;    

}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_initialize( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *value, int64_t element_count ) {
  int64_t elements_initialized;
  SYNCHRONIZE_ON( self->_lock ) {
    // Initialize data
    if( (elements_initialized = ComlibSequence_initialize_nolock( self, value, element_count )) != element_count ) {
#if defined( _CSEQ_FEATURE_ERRORS )
      __ComlibSequence_set_error( self, "incorrect data length, %lld initialized, %lld expected", elements_initialized, element_count );
#endif
    }
  } RELEASE;
  return elements_initialized;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_DEADSPACE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_deadspace_nolock( _CSEQ_TYPENAME* self ) {
  return self->_rp - self->_buffer;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_deadspace( _CSEQ_TYPENAME* self ) {
  int64_t dead;
  SYNCHRONIZE_ON( self->_lock ) {
    dead = ComlibSequence_deadspace_nolock( self );
  } RELEASE;
  return dead;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_WRITE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_write_nolock( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *elements, int64_t element_count ) {
  int64_t element_capacity = ComlibSequence_capacity(self);
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    return -1; // can't write to a queue with input file descriptor
  }
#endif
  // negative element count will compute number of elements up until first element == 0 (e.g. for type=char, interpreted as a C string)
  if( element_count < 0 ) {
    int64_t limit = 1LL << _CSEQ_MAX_CAPACITY_EXP;
    SUPPRESS_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT
    if( sizeof(_CSEQ_ELEMENT_TYPE) == 1 ) {
      element_count = (int64_t)strnlen((const char*)elements, limit);
    }
    else {
      element_count = 0;
      const _CSEQ_ELEMENT_TYPE *elem = elements;
      while( !__is_zero_element( elem++ ) && ++element_count < limit );
    }
  }

  // Ensure we have capacity to write
  if( self->_size + element_count 
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
      + __sizeof_unread( self )
#endif
    > element_capacity ) 
  {
#if defined( _CSEQ_FEATURE_STREAM_IO )
    // Buffer already large, flush to output file if we have one
    int flush_exp = _CSEQ_MAX_CAPACITY_EXP - 2;
    if( self->_capacity_exp > flush_exp ) {
      if( ComlibSequence_has_output_descriptor( self ) ) {
        if( ComlibSequence_flush_nolock( self ) < 0 ) {
          return -1;
        }
      }
    }
    // If still not enough room, expand to accommodate data
    if( self->_size + element_count
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
      + __sizeof_unread( self )
#endif
        > element_capacity )
    {
      if( __expand_to_accomodate( self, element_count ) != element_count ) {
        return -1;
      }
    }
#else
    if( __expand_to_accomodate( self, element_count ) != element_count ) {
      return -1;
    }
#endif
  }

  // Write data
  __begin_write_buffer_section( self ) {
    const _CSEQ_ELEMENT_TYPE *dp = elements;
    for( int64_t i=0; i<element_count; i++ ) {
      *__cursor = *dp++;
      __cursor_inc;
    }
  } __end_write_buffer_section;
  self->_size += element_count;

  return element_count;    
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_write( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *elements, int64_t element_count ) {
  int64_t elements_written;
  SYNCHRONIZE_ON( self->_lock ) {
    XTRY {
      // Write data
      if( (elements_written = ComlibSequence_write_nolock( self, elements, element_count )) != element_count && element_count > 0) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "incorrect data length, %lld written, %lld expected", elements_written, element_count );
#endif
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x161 );
      }

      // Reduce capacity if we've been consistently much smaller than our capacity for several writes
      // and reduction will not result in smaller than initial size
      short new_exp = self->_capacity_exp - COMLIBSEQUENCE_REDUCE_EXP;
      if( new_exp >= self->_initial_exp ) {
        if( new_exp > imag2( self->_size + __sizeof_unread(self) ) ) {
          if( new_exp > --(self->_load) ) {
            __ComlibSequence_resize_nolock( self, COMLIBSEQUENCE_REDUCE_EXP, RESIZE_DOWN );
          }
        }
      }
      else {
        self->_load = self->_capacity_exp;
      }

    }
    XCATCH( errcode ) {
      if( errcode != 0 ) {
        elements_written = -1;
      }
    }
    XFINALLY {}
  } RELEASE;
  return elements_written;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_SET_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_set_nolock( _CSEQ_TYPENAME *self, int64_t index, const _CSEQ_ELEMENT_TYPE *item ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    return -1; // can't set to a queue with input file descriptor
  }
#endif
  // Negative index wraps from end
  if( index < 0 ) {
    index = index + self->_size;
    if( index < 0 ) {
      return -1; // out of range
    }
  }
  else if( index >= self->_size ) {
    return -1; // out of range
  }

  _CSEQ_ELEMENT_TYPE *wp = self->_buffer;
  __guarded_add( self, wp, index );
  *wp = *item;

  return 1;
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_set( _CSEQ_TYPENAME *self, int64_t index, const _CSEQ_ELEMENT_TYPE *item ) {
  int set;
  SYNCHRONIZE_ON( self->_lock ) {
    // Set item
    set = ComlibSequence_set_nolock( self, index, item );
  } RELEASE;
  return set;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_APPEND_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_append_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *item ) {
  int64_t element_capacity = ComlibSequence_capacity(self);
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    return -1; // can't append to a queue with input file descriptor
  }
#endif
  // We have room to append
  if( self->_size < element_capacity 
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
                    - __sizeof_unread(self)
#endif
  ) 
  {
    // Append item
    __begin_write_buffer_section( self ) {
      *__cursor = *item;
      __cursor_inc;
    } __end_write_buffer_section;
    self->_size++;
    return 1;
  }
  // No room, expand then try again
  else {
    int appended = 0;
    XTRY {
      int exp_delta = 1;

      // sanity check
      if( self->_capacity_exp + exp_delta > _CSEQ_MAX_CAPACITY_EXP ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "max capacity exceeded" );
#endif
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0x171 );
      }

      // resize
      if( (__ComlibSequence_resize_nolock( self, exp_delta, RESIZE_UP )) < 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "out of memory during buffer expansion" );
#endif
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x172 );
      }

      // append data 
      appended = ComlibSequence_append_nolock( self, item ); // this will work
    }
    XCATCH( errcode ) {
      appended = -1;
    }
    XFINALLY {
    }
    return appended;
  }
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_append( _CSEQ_TYPENAME* self, const _CSEQ_ELEMENT_TYPE *item ) {
  int appended;
  SYNCHRONIZE_ON( self->_lock ) {
    // Append data
    appended = ComlibSequence_append_nolock( self, item );
  } RELEASE;
  return appended;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_FORMAT_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_format_nolock( _CSEQ_TYPENAME* self, const char *format, ... ) {
  char char_buffer[1024];
  _CSEQ_ELEMENT_TYPE *elem_buffer = (_CSEQ_ELEMENT_TYPE*)char_buffer;

  va_list args;
  int64_t n;
  va_start( args, format );
  if( (n = vsnprintf( char_buffer, 1023, format, args )) < 0 ) {
    // TODO: handle too large format
  }
  else {
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
    n = ComlibSequence_write_nolock( self, elem_buffer, x_elemcount( _CSEQ_ELEMENT_TYPE, n ) );
#else
#error This implementation requires _CSEQ_FEATURE_WRITE_METHOD
#endif
  }
  va_end( args );
  return n;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_format( _CSEQ_TYPENAME *self, const char *format, ... ) {
  char char_buffer[1024];
  _CSEQ_ELEMENT_TYPE *elem_buffer = (_CSEQ_ELEMENT_TYPE*)char_buffer;

  va_list args;
  int64_t n;
  va_start( args, format );
  if( (n = vsnprintf( char_buffer, 1023, format, args )) < 0 ) {
    // TODO: handle too large format
  }
  else {
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
    n = ComlibSequence_write( self, elem_buffer, x_elemcount( _CSEQ_ELEMENT_TYPE, n ) );
#else
#error This implementation requires _CSEQ_FEATURE_WRITE_METHOD
#endif
  }
  va_end( args );
  return n;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_NULTERM_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_nul_term_nolock( _CSEQ_TYPENAME *self ) {
  _CSEQ_ELEMENT_TYPE term;
  __set_zero_element( &term );
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
  return ComlibSequence_write_nolock( self, &term, 1 );
#else
#error This implementation requires _CSEQ_FEATURE_WRITE_METHOD
#endif
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_nul_term( _CSEQ_TYPENAME *self ) {
  _CSEQ_ELEMENT_TYPE term;
  __set_zero_element( &term );
#if defined( _CSEQ_FEATURE_WRITE_METHOD )
  return ComlibSequence_write( self, &term, 1 );
#else
#error This implementation requires _CSEQ_FEATURE_WRITE_METHOD
#endif
}
#endif
#endif



#if defined( _CSEQ_FEATURE_STREAM_IO )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __ComlibSequence_read_file_nolock( _CSEQ_TYPENAME *self, int64_t element_count ) {
  int64_t read_elements = 0;
  int64_t refill_elements = 0;
  int64_t required_elements = 0;
  int64_t element_capacity = ComlibSequence_capacity(self);

  XTRY {
    self->_prev_fpos = CX_TELL( self->_fd_in );

    // Compute the number of bytes we need to read from file to satisfy the number of elements requested
    if( element_count < 0 ) { // the entire buffer + file requested
      int64_t cur_byte = self->_prev_fpos;
      int64_t end_byte = CX_SEEK( self->_fd_in, 0, SEEK_END);
      if( CX_SEEK( self->_fd_in, cur_byte, SEEK_SET ) != cur_byte ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x181 );
      }
      if( CX_TELL( self->_fd_in ) != cur_byte ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x182 );
      }
      if( cur_byte < 0 || end_byte < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x183 );
      }
      required_elements = (end_byte - cur_byte) / sizeof(_CSEQ_ELEMENT_TYPE); // <= rest of file (as told by tell), aligned on element type (i.e. some bytes may remain!)
      element_count = self->_size + required_elements; // whatever was in buffer plus everything from file
      refill_elements = -1; // trigger buffer resize and set refill amount below
    }
    else {
      required_elements = element_count - self->_size;   // required elements to fulfil specific elements requested
      refill_elements = element_capacity - self->_size;  // actual elements we'll read
    }

    // Expand buffer if filling existing buffer is not enough to serve request
    if( refill_elements < required_elements ) {
      int exp_delta = imag2( element_count + __sizeof_unread( self ) ) - self->_capacity_exp;
      if( __ComlibSequence_resize_nolock( self, exp_delta, RESIZE_UP ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x184 );
      }
      element_capacity = (1ULL << self->_capacity_exp);
      refill_elements = element_capacity - self->_size;
    }

    _CSEQ_ELEMENT_TYPE *endwall = self->_buffer + (1ULL << self->_capacity_exp);

    if( refill_elements <= (int64_t)(endwall - self->_wp) ) {
      // fill buffer with one read from file 
      if( (read_elements = CX_READ( self->_wp, sizeof(_CSEQ_ELEMENT_TYPE), refill_elements, self->_fd_in )) < required_elements ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x185 );
      }
      if( read_elements < (int64_t)(endwall - self->_wp) ) {
        self->_wp += read_elements;
      }
      else {
        self->_wp = self->_buffer;
      }
    }
    else {
      // fill buffer with two reads from file
      int64_t partial_elements = (int64_t)(endwall - self->_wp);
      if( (read_elements = CX_READ( self->_wp, sizeof(_CSEQ_ELEMENT_TYPE), partial_elements, self->_fd_in )) < partial_elements ) {
        if( read_elements >= required_elements ) { // ok... that works
          if( read_elements < (int64_t)(endwall - self->_wp) ) {
            self->_wp += read_elements;
          }
          else {
            self->_wp = self->_buffer;
          }
        }
        else {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x186 );
        }
      }
      else {
        // continue 
        refill_elements -= partial_elements;
        partial_elements = CX_READ( self->_buffer, sizeof(_CSEQ_ELEMENT_TYPE), refill_elements, self->_fd_in );
        read_elements += partial_elements;
        if( read_elements >= required_elements ) {
          self->_wp = self->_buffer + partial_elements;
        }
        else {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x187 );
        }
      }
    }

    self->_size += read_elements;

  }
  XCATCH( errcode ) {
    read_elements = -1;
  }
  XFINALLY {}

  return read_elements;
}
#endif



#if defined( _CSEQ_FEATURE_NEXT_METHOD )
/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int ComlibSequence_next_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1; // can't read if output file attached
  }
 
  if( self->_size == 0 && ComlibSequence_has_input_descriptor(self) ) {
    int64_t read_elements = __ComlibSequence_read_file_nolock( self, 1 );
    if( read_elements < 0 ) {
      return -1;
    }
  }
#endif

  if( self->_size > 0 ) {
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
    self->_prev_rp = self->_rp; // allow one level of read-undo
#endif
    *dest = *self->_rp;
    __guarded_inc( self, self->_rp );
    self->_size--;
    return 1;
  }
  else {
    return 0;
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int ComlibSequence_next( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_next_nolock( self, dest );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_READ_METHOD )
/*******************************************************************//**
 *
 * self   : Sequence instance
 * dest   : Destination buffer where read data will be placed (auto allocate if *dest==NULL)
 *          This memory is untouched if zero elements are read.
 * count  : Number of elements to read, or negative to read everything
 * 
 * Return : Number of elements read, or -1 on error
 *          If zero elements were read and *dest==NULL, no return buffer is allocated,
 *          i.e. *dest will still point to NULL when call returns.
 ***********************************************************************
 */
static int64_t ComlibSequence_read_nolock( _CSEQ_TYPENAME *self, void **dest, int64_t element_count ) {
  _CSEQ_ELEMENT_TYPE *mem = NULL;

#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1; // can't read if output file attached
  }
#endif
  XTRY {
    _CSEQ_ELEMENT_TYPE *sp;
    int count_ok = element_count >= 0 && element_count <= self->_size; // buffer can serve request

    if( !count_ok ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
      if( ComlibSequence_has_input_descriptor(self) ) {
        int64_t read_elements;
        if( (read_elements = __ComlibSequence_read_file_nolock( self, element_count )) < 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
          __ComlibSequence_set_error( self, "" );
#endif
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x191 );
        }
        if( element_count < 0 ) {
          element_count = read_elements;
        }
      }
      else {
        element_count = self->_size;
      }
#else
      element_count = self->_size;
#endif
    }

    if( element_count > 0 ) {
      if( *dest == NULL ) {
        // Allocate one extra NULL element at end
        int64_t bytes = (element_count + 1) * sizeof(_CSEQ_ELEMENT_TYPE);

        CALIGNED_ARRAY( mem, _CSEQ_ELEMENT_TYPE, bytes );
        if( mem == NULL ) {
#if defined( _CSEQ_FEATURE_ERRORS )
          __ComlibSequence_set_error( self, "out of memory while allocating read buffer" );
#endif
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x192 );
        }
        // Terminate
        __set_zero_element( mem + element_count );
        // Assign internally allocated memory to output array
        *dest = mem;

        // WARNING!  Caller is responsible for freeing this memory!
      }
      sp = (_CSEQ_ELEMENT_TYPE*)*dest;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
      self->_prev_rp = self->_rp; // allow one level of read-undo
#endif
      __begin_read_buffer_section( self ) {
        for( int64_t i = 0; i < element_count; i++ ) {
          *sp++ = *__cursor;
          __cursor_inc;
        }
      } __end_read_buffer_section;
      self->_size -= element_count;
    }
  }
  XCATCH( errcode ) {
    if( mem ) {
      ALIGNED_FREE( mem );
    }
    if( errcode != 0 ) {
      element_count = -1;
    }
  }
  XFINALLY {}

  return element_count; // don't create a leak now...  caller must free dest if NULL was passed!
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_read( _CSEQ_TYPENAME *self, void **dest, int64_t element_count ) {
  SYNCHRONIZE_ON( self->_lock ) {
    element_count = ComlibSequence_read_nolock( self, dest, element_count );
  } RELEASE;
  return element_count; // don't create a leak now...  caller must free dest if NULL was passed!
}
#endif
#endif



#if defined( _CSEQ_FEATURE_CURSOR_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_cursor_nolock( _CSEQ_TYPENAME *self, int64_t index ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return NULL; // can't get cursor from a sequence with output file descriptor
  }
#endif
  // Negative index wraps from end
  if( index < 0 ) {
    index = index + self->_size;
    if( index < 0 ) {
      return NULL; // out of range
    }
  }
  else if( index >= self->_size ) {
    return NULL; // out of range
  }

  _CSEQ_ELEMENT_TYPE *rp = self->_rp;
  __guarded_add( self, rp, index );
  return rp;
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_cursor( _CSEQ_TYPENAME *self, int64_t index ) {
  _CSEQ_ELEMENT_TYPE *cursor;
  SYNCHRONIZE_ON( self->_lock ) {
    // Cursor
    cursor = ComlibSequence_cursor_nolock( self, index );
  } RELEASE;
  return cursor;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_GET_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_get_nolock( _CSEQ_TYPENAME *self, int64_t index, _CSEQ_ELEMENT_TYPE *dest ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1; // can't get from a queue with output file descriptor
  }
#endif
  // Negative index wraps from end
  if( index < 0 ) {
    index = index + self->_size;
    if( index < 0 ) {
      return -1; // out of range
    }
  }
  else if( index >= self->_size ) {
    return -1; // out of range
  }

  _CSEQ_ELEMENT_TYPE *rp = self->_rp;
  __guarded_add( self, rp, index );
  *dest = *rp;
  
  return 1;
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_get( _CSEQ_TYPENAME *self, int64_t index, _CSEQ_ELEMENT_TYPE *dest ) {
  int get;
  SYNCHRONIZE_ON( self->_lock ) {
    // Set item
    get = ComlibSequence_get_nolock( self, index, dest );
  } RELEASE;
  return get;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_READUNTIL_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_readuntil_nolock( _CSEQ_TYPENAME *self, void **dest, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int exclude_probe ) {
  // don't create a leak now...  caller must free dest!
  int64_t element_count = 0;
  _CSEQ_ELEMENT_TYPE *output = NULL;

#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1; // can't read when output file attached
  }
#endif

  XTRY {
    const _CSEQ_ELEMENT_TYPE *pp = probe;
    _CSEQ_ELEMENT_TYPE *wp = NULL;  // TODO: clean up logic. This is currently always set below since size == capacity on first iteration.
    int64_t element_capacity = 0;

    if( probe == NULL || plen < 1 ) {
      XBREAK;
    }
    if( *dest != NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x1A1 ); // we will allocate here, enforce protocol
    }


    // !!!!!!!!!
    // !! TODO : Support file input.
    // !!!!!!!!!
    // TO be safe, throw exception if we're using this function and a file is registered

#if defined( _CSEQ_FEATURE_STREAM_IO )
    if( ComlibSequence_has_input_descriptor(self) ) {
      THROW_FATAL( CXLIB_ERR_GENERAL, -1 ); // so we don't fool ourselves
    }
#endif

    __begin_buffer_section( self, self->_rp ) {
      while( element_count < self->_size ) {
        // expand output buffer if we need to
        if( element_count == element_capacity ) {
          if( element_capacity == 0 ) {
            element_capacity = 1024 / sizeof(_CSEQ_ELEMENT_TYPE);  // start at 1kB
          }
          else {
            element_capacity *= 2;    // and double until we're done
          }
          // Allocate output memory with one additional element at the end to ensure we can always fit a 0-terminator
          //
          //
          // !!! WARNING !!!
          // TODO: REPLACE realloc() with aligned memory allocation (have to write our own since there is no default aligned realloc)
          // 
          //
          //
          _CSEQ_ELEMENT_TYPE *new_output = (_CSEQ_ELEMENT_TYPE*)realloc( output, (element_capacity+1) * sizeof(_CSEQ_ELEMENT_TYPE) );
          if( new_output == NULL ) {
          //
          //
          //
#if defined( _CSEQ_FEATURE_ERRORS )
            __ComlibSequence_set_error( self, "out of memory while allocating output buffer" );
#endif
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x1A2 );
          }
          output = new_output;
          wp = output + element_count; // set the wp to continue writing
        }

        // copy next _CSEQ_ELEMENT_TYPE to output buffer
        *wp++ = *__cursor;      // <===== THIS REQUIRES destination to be aligned on data type (), see TODO above
        ++element_count;

        // check for end condition
        if( __eq_element(pp, __cursor) || (pp > probe && __eq_element(pp=probe, __cursor)) ) {
          if( (++pp - probe) == plen ) {
            if( exclude_probe ) {
              element_count -= plen;
            }
            break;
          }
        }
        __cursor_inc;
      }

      // No output because sequence has no data or some other reason
      if( output == NULL ) {
        output = malloc( (element_capacity+1) * sizeof(_CSEQ_ELEMENT_TYPE) );
      }

      // Add 0-terminator
      __set_zero_element( output + element_count );
    } __end_buffer_section;



    // shift the rp ahead (with undo)
#if defined( _CSEQ_FEATURE_DISCARD_METHOD )
    ComlibSequence_discard_nolock( self, element_count );
#else
#error This implementation requires _CSEQ_FEATURE_DISCARD_METHOD
#endif

  }
  XCATCH( errcode ) {
    if( output ) {
      free( output ); // <====== TODO: find a way to create ALIGNED allocations to keep the entire comlibsequence API consistent wrt. freeing of auto-allocated output arrays!
      output = NULL;
    }
    element_count = -1;
  }
  XFINALLY {
    // WARNING!  Caller is responsible for freeing this memory!
    *dest = output;
  }

  return element_count; // don't create a leak now...  caller must free dest!

}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_readuntil( _CSEQ_TYPENAME *self, void **dest, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int exclude_probe ) {
  int64_t element_count;
  SYNCHRONIZE_ON( self->_lock ) {
    element_count = ComlibSequence_readuntil_nolock( self, dest, probe, plen, exclude_probe );
  } RELEASE;
  return element_count; // don't create a leak now...  caller must free dest!
}
#endif
#endif



#if defined( _CSEQ_FEATURE_READLINE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_readline_nolock( _CSEQ_TYPENAME *self, void **dest ) {
  // don't create a leak now...  caller must free dest!
#if defined( _CSEQ_FEATURE_READUNTIL_METHOD )
  static const _CSEQ_ELEMENT_TYPE newline = {0xa}; // "\n"
  return ComlibSequence_readuntil_nolock( self, dest, &newline, 1, 0 );
#else
#error This implementation requires _CSEQ_FEATURE_READUNTIL_METHOD
#endif
} 



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_readline( _CSEQ_TYPENAME *self, void **dest ) {
  int64_t len;
  SYNCHRONIZE_ON( self->_lock ) {
    len = ComlibSequence_readline_nolock( self, dest );
  } RELEASE;
  return len;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_PEEK_METHOD )
/*******************************************************************//**
 *
 * self   : queue instance
 * dest   : where read data will be placed (auto allocate if NULL)
 * index  : the offset to peek at
 * count  : number of elements to read starting at index, or negative to read until end
 * 
 * Return : number of elements read, or -1 on error
 ***********************************************************************
 */
static int64_t ComlibSequence_peek_nolock( _CSEQ_TYPENAME *self, void **dest, int64_t element_index, int64_t element_count ) {
  int64_t elements_peeked = 0;
  _CSEQ_ELEMENT_TYPE *mem = NULL;

  if( element_index < 0 ) {
    element_index += self->_size; // negative indexing allowed, it means from end, like Python L[-1] is last element
  }

  XTRY {
    int64_t sztail = self->_size - element_index;
    int64_t szout = (element_count > sztail || element_count < 0) ? sztail : element_count;
    _CSEQ_ELEMENT_TYPE *rp = self->_rp;
    _CSEQ_ELEMENT_TYPE *wp;
    if( element_index >= 0 && sztail > 0 && szout > 0 ) {
      elements_peeked = szout;
      if( *dest == NULL ) {
        // allocate output buffer (caller will own it!)
        CALIGNED_ARRAY( mem, _CSEQ_ELEMENT_TYPE, elements_peeked+1 );
        if( mem == NULL ) {
#if defined _CSEQ_FEATURE_ERRORS
          __ComlibSequence_set_error( self, "out of memory while allocating read buffer" );
#endif
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x1B1 );
        }
        *dest = mem;
        // WARNING!  Caller is responsible for freeing this memory!
      }
      wp = (_CSEQ_ELEMENT_TYPE*)*dest;

      // advance the read pointer given by index
      __guarded_add( self, rp, element_index );

      // copy data to output
      __begin_buffer_section( self, rp ) {
        for( int64_t i=0; i<elements_peeked; i++ ) {
          *wp++ = *__cursor;
          __cursor_inc;
        }
      } __end_buffer_section;
    }
    // Terminate
    __set_zero_element( (_CSEQ_ELEMENT_TYPE*)mem + elements_peeked );
  }
  XCATCH( errcode ) {
    if( mem ) {
      ALIGNED_FREE( mem );
    }
    elements_peeked = -1;
  }
  XFINALLY {
  }

  return elements_peeked;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_peek( _CSEQ_TYPENAME *self, void **dest, int64_t element_index, int64_t element_count ) {
  int64_t elements_peeked;
  SYNCHRONIZE_ON( self->_lock ) {
    elements_peeked = ComlibSequence_peek_nolock( self, dest, element_index, element_count );
  } RELEASE;
  return elements_peeked;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_EXPECT_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_expect_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int64_t element_index ) {
  // Index from end, compute actual index
  if( element_index < 0 && (element_index = self->_size + element_index) < 0 ) {
    return false;
  }
  // No chance of match
  if( element_index + plen > self->_size ) {
    return false;
  }

  // Let's match the buffer's contents against probe
  const _CSEQ_ELEMENT_TYPE *p = probe;
  const _CSEQ_ELEMENT_TYPE *pend = probe + plen;  // one element beyond end

  // Scan and expect all probe elements to match
  _CSEQ_ELEMENT_TYPE *rp = self->_rp;
  __guarded_add( self, rp, element_index );
  __begin_buffer_section( self, rp ) {
    while( p < pend ) {
      if( !__eq_element( __cursor, p++ ) ) {
        return false; // no match
      }
      __cursor_inc;
    }
  } __end_buffer_section;

  // Match if we made it here - including match on empty probe
  return true;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_expect( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen, int64_t element_index ) {
  bool match;
  SYNCHRONIZE_ON( self->_lock ) {
    match = ComlibSequence_expect_nolock( self, probe, plen, element_index );
  } RELEASE;
  return match;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_unread_nolock( _CSEQ_TYPENAME *self, int64_t element_count ) {

#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return;  // can't unread when output file attached
  }
#endif
  if( self->_prev_rp != NULL ) {
    int64_t element_delta_in_buffer = 0;

#if !defined( _CSEQ_CIRCULAR_BUFFER )
#error Not implemented for non-circular buffers
#endif

    // rp is "after" prev_rp in the buffer
    if( self->_prev_rp < self->_rp ) { 
      if( element_count >= 0 && element_count <= (self->_rp - self->_prev_rp) ) {
        // partial unread can be handled by adjusting buffer pointers when the count is small enough
        self->_size += element_count;
        self->_rp -= element_count;
      }
      else {
        // a large (possible full) unread will reset buffer to previous state, and possibly adjust file position later
        element_delta_in_buffer = (int64_t)(self->_rp - self->_prev_rp);
        self->_size += element_delta_in_buffer;
        self->_rp = self->_prev_rp;
      }
    }
    // rp is "before" prev_rp in the buffer (circular buf, remember?)
    else if( self->_prev_rp > self->_rp ) {
      int64_t element_capacity = ComlibSequence_capacity( self );
      if( element_count < 0 || element_count > element_capacity - (int64_t)(self->_prev_rp - self->_rp) ) {
        // a large (possible full) unread will reset buffer to previous state, and possibly adjust file position later
        element_delta_in_buffer = element_capacity - (int64_t)(self->_prev_rp - self->_rp);
        self->_size += element_delta_in_buffer;
        self->_rp = self->_prev_rp;
      }
      else {
        // partial unread can be handled by adjusting buffer pointers when the count is small enough
        self->_size += element_count;
        __guarded_add( self, self->_rp, -element_count );
      }
    }
    // rp == prev_rp which means we should unread the entire capacity of the buffer
    else {
      self->_size = ComlibSequence_capacity( self );
    }

    // Do we need to consider input file?
#if defined( _CSEQ_FEATURE_STREAM_IO )
    if( ComlibSequence_has_input_descriptor(self) && self->_prev_fpos >= 0 && element_delta_in_buffer ) {
      // go backwards in file
      if( element_count < 0 ) {
        // undo entire read in file
        CX_SEEK( self->_fd_in, self->_prev_fpos, SEEK_SET );
      }
      else {
        // undo partial file read
        int64_t rewind_elements = element_count - element_delta_in_buffer;
        int64_t rewind_bytes = rewind_elements * sizeof(_CSEQ_ELEMENT_TYPE);
        if( rewind_bytes > (CX_TELL( self->_fd_in ) - self->_prev_fpos) ) { // too much undoing requested, undo the entire last read
          CX_SEEK( self->_fd_in, self->_prev_fpos, SEEK_SET );
        }
        else {
          CX_SEEK( self->_fd_in, -rewind_bytes, SEEK_CUR );
        }
      }
      self->_prev_fpos = -1; // used up the file unread
    }
#endif

    self->_prev_rp = NULL; // used up the buffer unread
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_unread( _CSEQ_TYPENAME *self, int64_t element_count ) {
  SYNCHRONIZE_ON( self->_lock ) {
    ComlibSequence_unread_nolock( self, element_count );
  } RELEASE;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_TRUNCATE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_truncate_nolock( _CSEQ_TYPENAME *self, int64_t tail_index ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1;  // no action if output file attached
  }
#endif
  int64_t truncate_amount;

  // Negative index counts from end
  if( tail_index < 0 ) {
    if( self->_size + tail_index < 0 ) {
      return -1;
    }
    truncate_amount = -tail_index;
  }
  else if( tail_index >= self->_size ) {
    return -1;
  }
  else {
    truncate_amount = self->_size - tail_index;
  }

  // Back up the wp to truncate everything starting at tail_index 
  __guarded_add( self, self->_wp, -truncate_amount );
  self->_size -= truncate_amount;

#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  self->_prev_rp = NULL;
#endif
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    self->_prev_fpos = -1;
  }
#endif
  return truncate_amount;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_truncate( _CSEQ_TYPENAME *self, int64_t tail_index ) {
  int64_t truncate_amount;
  SYNCHRONIZE_ON( self->_lock ) {
    truncate_amount = ComlibSequence_truncate_nolock( self, tail_index );
  } RELEASE;
  return truncate_amount;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_CLEAR_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_clear_nolock( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return;  // no action if output file attached
  }
#endif
  self->_wp = self->_rp = self->_buffer;
  // Force the initial element to 0 to help debugging
  if( self->_buffer != NULL ) {
    __set_zero_element( self->_buffer );
  }
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  self->_prev_rp = NULL;
#endif
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    self->_prev_fpos = -1;
  }
#endif
  self->_size = 0;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_clear( _CSEQ_TYPENAME *self ) {
  SYNCHRONIZE_ON( self->_lock ) {
    ComlibSequence_clear_nolock( self );
  } RELEASE;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_RESET_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_reset_nolock( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return;  // no action if output file attached
  }
#endif
  // Resize the buffer to initial capacity
  if( self->_capacity_exp != self->_initial_exp ) {
    int new_exp = self->_initial_exp;
    int64_t new_element_capacity = 1LL << new_exp;
    size_t alignment = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE );
    _CSEQ_ELEMENT_TYPE *buf = NULL;
    ALIGNED_ARRAY( buf, _CSEQ_ELEMENT_TYPE, new_element_capacity, alignment );
    // If allocation ok, perform reset (otherwise no action)
    if( buf ) {
      self->_size = 0;
      ALIGNED_FREE( self->_buffer );
      self->_buffer = buf;
      self->_wp = self->_buffer;
      self->_rp = self->_buffer;
#if defined( _CSEQ_CIRCULAR_BUFFER )
      self->_mask = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE ) - 1;
#endif
      self->_capacity_exp = (uint8_t)new_exp;
      self->_load = 0;
#if defined _CSEQ_FEATURE_UNREAD_METHOD
      self->_prev_rp = NULL;
#endif
#if defined( _CSEQ_FEATURE_ERRORS )
      if( __ComlibSequence_has_errstr( self ) ) {
        free( self->_error );
        self->_error = NULL;
      }
#endif
    }
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_reset( _CSEQ_TYPENAME *self ) {
  SYNCHRONIZE_ON( self->_lock ) {
    ComlibSequence_reset_nolock( self );
  } RELEASE;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_GETVALUE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_getvalue_nolock( _CSEQ_TYPENAME *self, void **dest ) {
  int64_t element_count = 0;
  
  XTRY {
    _CSEQ_ELEMENT_TYPE *rstr, *sp;
    if( *dest != NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x1C1 ); // we will allocate here, enforce protocol
    }

    CALIGNED_ARRAY( rstr, _CSEQ_ELEMENT_TYPE, self->_size );
    if( rstr == NULL ) {
#if defined( _CSEQ_FEATURE_ERRORS )
      __ComlibSequence_set_error( self, "out of memory while allocating read buffer" );
#endif
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x1C2 );
    }
    sp = rstr;
    _CSEQ_ELEMENT_TYPE *rp = self->_rp;
    __begin_buffer_section( self, rp ) {
      for( int64_t i=0; i<self->_size; i++ ) {
        *sp++ = *__cursor;
        __cursor_inc;
      }
    } __end_buffer_section;
    
    // WARNING!  Caller is responsible for freeing this memory!
    *dest = rstr;
    element_count = self->_size;
  }
  XCATCH( errcode ) {
    if( errcode != 0 ) {
      element_count = -1;
    }
  }
  XFINALLY {}
  return element_count; // don't create a leak now...
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_getvalue( _CSEQ_TYPENAME *self, void **dest ) {
  int64_t element_count;
  SYNCHRONIZE_ON( self->_lock ) {
    element_count = ComlibSequence_getvalue_nolock( self, dest );
  } RELEASE;
  return element_count;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_ABSORBUNTIL_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_absorb_until_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen_or_elemcount, int exclude_probe ) {
  int64_t absorbed_elements = 0;
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) ) {
    return -1; // can't absorb other queue if this queue has input file descriptor
  }
#endif

  // NOTE: absorbing the other buffer does NOT read from file if the other buffer is file-backed!

  XTRY {
    // Absorb until probe match
    if( probe ) {
      // Ensure we have enough room for worst-case (entire other)
      if( self->_size + other->_size > ComlibSequence_capacity( self ) ) {
        if( __expand_to_accomodate( self, other->_size ) != other->_size ) {
          return -1;
        }
      }

      // append data from other to self until the probe condition is met in the other, or entire other has been absorbed
      int64_t plen = plen_or_elemcount;
      const _CSEQ_ELEMENT_TYPE *pp = probe;       // start of probe array
      bool probe_match = false;
      __begin_write_buffer_section( self ) {
        while( probe_match == false && absorbed_elements < other->_size ) {
          // copy next element from other to self
          *__cursor = *other->_rp; 
          absorbed_elements++;
          // check for end condition
          if( plen == 0 || __eq_element(pp, other->_rp) || (pp > probe && __eq_element(pp=probe, other->_rp)) ) {
            if( (++pp - probe) >= plen ) {
              probe_match = true; // match found, stop absorbing
            }
          }
          __cursor_inc;
          __guarded_inc( other, other->_rp );
        }
      } __end_write_buffer_section;

      // adjust self's and other's sizes by the absorbed amount
      self->_size += absorbed_elements;
#if defined( _CSEQ_CIRCULAR_BUFFER )
      other->_size -= absorbed_elements;
#elif defined( _CSEQ_LINEAR_BUFFER )
      // TODO: how do we deal with size reduction in a linear buffer?????
      // We need a capacity description other than 2**cap_exp... it needs to be
      // 2**cap_exp - (rp-buffer) basically. So the space between buffer and rp is dead in linear buffers.
#endif
#if defined _CSEQ_FEATURE_UNREAD_METHOD
      other->_prev_rp = NULL;
#endif
      if( probe_match && exclude_probe ) {
        // we have copied too much from other to self, undo this
        __ComlibSequence_shift_cursor( self, &self->_wp, -plen );     // we wrote too much to self (the probe string) - undo the write
        __ComlibSequence_shift_cursor( other, &other->_rp, -plen );   // we read too much from other (the probe string) - undo the read
      }
    }
    // Absorb specified number of elements
    else {
      if( plen_or_elemcount < 0 || plen_or_elemcount > other->_size ) {
        // absorb all data
        absorbed_elements = other->_size;
      }
      else {
        // absorb partial data
        absorbed_elements = plen_or_elemcount;
      }

      // Ensure we have enough room
      if( self->_size + absorbed_elements > ComlibSequence_capacity( self ) ) {
        if( __expand_to_accomodate( self, absorbed_elements ) != absorbed_elements ) {
          return -1;
        }
      }

      // append data in other to self
      __begin_write_buffer_section( self ) {
        for( int64_t i=0; i<absorbed_elements; i++ ) {
          *__cursor = *other->_rp;
          __cursor_inc;
          __guarded_inc( other, other->_rp );
        }
      } __end_write_buffer_section;

      // adjust self's and other's sizes by the absorbed amount
      self->_size += absorbed_elements;
      other->_size -= absorbed_elements;
#if defined _CSEQ_FEATURE_UNREAD_METHOD
      other->_prev_rp = NULL;
#endif
      if( other->_size == 0 ) {
        // the other should now be empty - sanity check
        if( other->_wp != other->_rp ) {
          THROW_FATAL( CXLIB_ERR_BUG, 0x1D2 );
        }
#if defined( _CSEQ_LINEAR_BUFFER )
        // Empty linear buffer needs to be reset otherwise it
        // will continue to grow forever if used repeatedly
        ComlibSequence_clear_nolock( other );
#endif
      }
    }
  }
  XCATCH( errcode ) {
    absorbed_elements = -1;
  }
  XFINALLY {}

  return absorbed_elements;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_absorb_until( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, const _CSEQ_ELEMENT_TYPE *probe, int64_t plen_or_elemcount, int exclude_probe ) {
  int64_t absorbed_elements;

  if( self == other ) { 
    return 0;
  }
  
  SYNCHRONIZE_TWO_COMLIB_SEQUENCES( self, other ) {
    absorbed_elements = ComlibSequence_absorb_until_nolock( self, other, probe, plen_or_elemcount, exclude_probe );
  } RELEASE_TWO_COMLIB_SEQUENCES;

  return absorbed_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_ABSORB_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_absorb_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, int64_t element_count ) {
#if defined( _CSEQ_FEATURE_ABSORBUNTIL_METHOD )
  return ComlibSequence_absorb_until_nolock( self, other, NULL, element_count, 0 );
#else
#error This implementation requires _CSEQ_FEATURE_ABSORBUNTIL_METHOD
#endif
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_absorb( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other, int64_t element_count ) {
  return ComlibSequence_absorb_until( self, other, NULL, element_count, 0 );
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPIFY_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void ComlibSequence_heapify_nolock( _CSEQ_TYPENAME *self ) {
  // TODO: heapify!
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_heapify( _CSEQ_TYPENAME *self ) {
  SYNCHRONIZE_ON( self->_lock ) {
    ComlibSequence_heapify_nolock( self );
  } RELEASE;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPTOP_METHOD )
/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int ComlibSequence_heap_top_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
  if( self->_size > 0 ) {
    // Copy top item to destination
#if defined( _CSEQ_CIRCULAR_BUFFER )
    *dest = *self->_rp;
#elif defined( _CSEQ_LINEAR_BUFFER )
    *dest = *self->_buffer;
#else
#error
#endif
    return 1;
  }
  else {
    return 0;
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int ComlibSequence_heap_top( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_heap_top_nolock( self, dest );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPPUSH_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_push_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elem ) {
  
  // Check size
  if( self->_size < ComlibSequence_capacity( self ) ) {
    // Add element to end
    __begin_write_buffer_section( self ) {
      int64_t idx = self->_size;  // <= element will be added here

      // Write pushed element to end
      *__cursor = *elem;
      __cursor_inc;
      self->_size++;

      _CSEQ_ELEMENT_TYPE *root = self->_rp;
      _CSEQ_ELEMENT_TYPE *item, *parent;
      int (*cmp)( const _CSEQ_ELEMENT_TYPE *a, const _CSEQ_ELEMENT_TYPE *b) = self->_cmp;

      // Perform up-heap until heap property restored
      __begin_general_buffer_section( self ) {
        while( idx > 0 ) {
          item = root + idx;
          __general_cursor_guard( item );
          idx = (idx-1) >> 1;
          parent = root + idx;
          __general_cursor_guard( parent );
          if( cmp( parent, item ) < 0 ) {
            __swap_elements( parent, item );
          }
          else {
            break;
          }
        }
      } __end_general_buffer_section;
    } __end_write_buffer_section;
    return 1;
  }
  // Expand and retry
  else {
    int ret = 0;
    XTRY {
      if( self->_capacity_exp == _CSEQ_MAX_CAPACITY_EXP ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "max capacity exceeded for heap push" );
#endif
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0x1E1 );
      }
      if( __ComlibSequence_resize_nolock( self, 1, RESIZE_UP ) < 0 ) {
#if defined( _CSEQ_FEATURE_ERRORS )
        __ComlibSequence_set_error( self, "out of memory for heap push" );
#endif
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x1E2 );
      }
      // retry
      ret = ComlibSequence_heap_push_nolock( self, elem );
    }
    XCATCH( errcode ) {
      ret = -1;
    }
    XFINALLY {
    }
    return ret;
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_push( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *elem ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_heap_push_nolock( self, elem );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPPOP_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_pop_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {

  if( self->_size > 0 ) {
    __begin_write_buffer_section( self ) {

      _CSEQ_ELEMENT_TYPE *root = self->_rp;
      _CSEQ_ELEMENT_TYPE *parent, *high, *left, *right;
      int (*cmp)( const _CSEQ_ELEMENT_TYPE *a, const _CSEQ_ELEMENT_TYPE *b) = self->_cmp;
      
      // Extract root into destination
      *dest = *root;

      // Replace root with the last element and remove last element
      __cursor_dec;
      self->_size--;
      *root = *__cursor;

      // Perform down-heap
      parent = root;
      int64_t size = self->_size;
      int64_t idx = 0;
      int64_t high_idx = idx;
      int64_t left_idx = 1;
      int64_t right_idx = 2;
      __begin_general_buffer_section( self ) {
        while( idx < size ) {
          left_idx = 2*idx + 1;
          right_idx = left_idx + 1;
          high = parent;
          left = root + left_idx;
          right = root + right_idx;
          __general_cursor_guard( left );
          __general_cursor_guard( right );
          if( left_idx < size && cmp( left, parent ) > 0 ) {
            high_idx = left_idx;
            high = left;
          }
          if( right_idx < size && cmp( right, high ) > 0 ) {
            high_idx = right_idx;
            high = right;
          }
          if( high != parent ) {
            __swap_elements( parent, high );
            idx = high_idx;
            parent = high;
          }
          else {
            break;
          }
        }
      } __end_general_buffer_section;
    } __end_write_buffer_section;

    // TODO: reduce size if below threshold

    return 1;
  }
  else {
    return 0;
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_pop( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_heap_pop_nolock( self, dest );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPREPLACE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_replace_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *popped, _CSEQ_ELEMENT_TYPE *pushed ) {

  if( self->_size > 0 ) {

    __begin_general_buffer_section( self ) {

      _CSEQ_ELEMENT_TYPE *root = self->_rp;
      _CSEQ_ELEMENT_TYPE *parent, *high, *left, *right, *item;
      int (*cmp)( const _CSEQ_ELEMENT_TYPE *a, const _CSEQ_ELEMENT_TYPE *b) = self->_cmp;
      
      // Extract root into destination (if we care)
      if( popped ) {
        *popped = *root;
      }

      // Locate last element (don't modify wp)
      _CSEQ_ELEMENT_TYPE *last = self->_wp - 1;
      __general_cursor_guard( last );  

      // Replace root with the last element
      *root = *last;

      // Perform down-heap on a smaller-by-one heap
      parent = root;
      int64_t size = self->_size - 1; // ignore the last element (the one we're pushing to replace the popped one)
      int64_t idx = 0;
      int64_t high_idx = idx;
      int64_t left_idx = 1;
      int64_t right_idx = 2;
      while( idx < size ) {
        left_idx = 2*idx + 1;
        right_idx = left_idx + 1;
        high = parent;
        left = root + left_idx;
        right = root + right_idx;
        __general_cursor_guard( left );
        __general_cursor_guard( right );
        if( left_idx < size && cmp( left, parent ) > 0 ) {
          high_idx = left_idx;
          high = left;
        }
        if( right_idx < size && cmp( right, high ) > 0 ) {
          high_idx = right_idx;
          high = right;
        }
        if( high != parent ) {
          __swap_elements( parent, high );
          idx = high_idx;
          parent = high;
        }
        else {
          break;
        }
      }

      // Set last element to replacement element
      *last = *pushed;

      // Perform up-heap
      idx = size;
      while( idx > 0 ) {
        item = root + idx;
        __general_cursor_guard( item );
        idx = (idx-1) >> 1;
        parent = root + idx;
        __general_cursor_guard( parent );
        if( cmp( parent, item ) < 0 ) {
          __swap_elements( parent, item );
        }
        else {
          break;
        }
      }

    } __end_general_buffer_section;

  }
  return 0;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_heap_replace( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *popped, _CSEQ_ELEMENT_TYPE *pushed ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_heap_replace_nolock( self, popped, pushed );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD )


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_heap_pushtopk_nolock( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *candidate, _CSEQ_ELEMENT_TYPE *discarded ) {
  // NOTE: The buffer will not be resized, the current size of the buffer defines the "k"
  // NOTE2: "top k" is implemented using min-heap semantics so that the smallest item will always be
  // at the root. For min-heaps our cmp(A,B) returns >0 when A<B. So for a candidate to be able to
  // enter the heap (and therefore remove the root) the candidate has to be larger than the root,
  // i.e. cmp( root, candidate ) > 0.

  // This will point to the pushed item after heap insertion, or remain NULL if not inserted
  _CSEQ_ELEMENT_TYPE *location = NULL;

  if( self->_size > 0 ) {
    __begin_general_buffer_section( self ) {

      _CSEQ_ELEMENT_TYPE *root = self->_rp;
      int (*cmp)( const _CSEQ_ELEMENT_TYPE *a, const _CSEQ_ELEMENT_TYPE *b) = self->_cmp;
      
      // When the root compares "greater than" the candidate it will be yanked and the candidate will be
      // inserted. (For min-heaps the cmp() function is reversed so really if root is smaller than the candidate
      // the root is yanked and the candidate inserted, maintaining a heap with a growing root value.)
      if( cmp( root, candidate ) > 0 ) {
        // The discarded root will be returned
        *discarded = *root;

        // Replace root with the new element in preparation for down-heap operation
        *root = *candidate;

        // Perform down-heap
        _CSEQ_ELEMENT_TYPE *parent = root;
        _CSEQ_ELEMENT_TYPE *end = root + self->_size;
        for( int64_t idx=1; idx < self->_size; idx = 2*idx+1 ) {
          _CSEQ_ELEMENT_TYPE *left = root + idx;
          _CSEQ_ELEMENT_TYPE *right = left + 1;
          __general_cursor_guard( left );
          __general_cursor_guard( right );
          _CSEQ_ELEMENT_TYPE *next = right;
          if( cmp( left, parent ) > 0 ) { // maybe traverse left branch
            if( right < end && cmp( right, left ) > 0 ) {
              ++idx; // traverse right branch
            }
            else {
              --next; // back to left left
            }
          }
          else if( right < end && cmp( right, parent ) > 0 ) {
            ++idx; // traverse right branch
          }
          else {
            break;
          }
          parent = __swap_elements( next, parent );
        }

        location = parent;
      }
    } __end_general_buffer_section;
  }
  
  return location;
}




#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_heap_pushtopk( _CSEQ_TYPENAME *self, const _CSEQ_ELEMENT_TYPE *candidate, _CSEQ_ELEMENT_TYPE *discarded ) {
  _CSEQ_ELEMENT_TYPE *location;
  SYNCHRONIZE_ON( self->_lock ) {
    location = ComlibSequence_heap_pushtopk_nolock( self, candidate, discarded );
  } RELEASE;
  return location;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_CLONEINTO_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_clone_into_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other ) {

  int64_t self_size = self->_size;

  int64_t other_remain = (1LL << other->_capacity_exp) - other->_size;

  // We must resize other queue to fit self's data into it
  if( self_size > other_remain ) {
    int exp_delta = imag2( self_size + __sizeof_unread(self) + other->_size ) - other->_capacity_exp;
    if( __ComlibSequence_resize_nolock( other, exp_delta, RESIZE_UP ) < 0 ) {
      return -1;
    }
  }

  _CSEQ_ELEMENT_TYPE *rp = self->_rp;
  __begin_buffer_section( self, rp ) {
    for( int64_t i=0; i<self_size; i++ ) {
      *other->_wp = *__cursor;
      __cursor_inc;
      __guarded_inc( other, other->_wp );
    }
    other->_size += self_size;
  } __end_buffer_section;

  return self_size;

}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_clone_into( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other ) {
  int64_t cloned_elements;

  SYNCHRONIZE_TWO_COMLIB_SEQUENCES( self, other ) {
    cloned_elements = ComlibSequence_clone_into_nolock( self, other );
  } RELEASE_TWO_COMLIB_SEQUENCES;

  return cloned_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_TRANSPLANTFROM_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_transplant_from_nolock( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other ) {
  // NOTE: we may get "other" from a typecast argument and can't rely on checking file descriptors
  // since access to these may be invalid if cast from a type without these attributes.
  // DO NOT TRANSPLANT FROM A SEQUENCE WITH OPEN FILES: UNDEFINED BEHAVIOR.

#if defined _CSEQ_FEATURE_STREAM_IO
  if( ComlibSequence_has_input_descriptor( self ) || ComlibSequence_has_output_descriptor( self ) ) {
    return -1; // can't when we have active IO
  }
#endif
  // Delete own buffer
  if( self->_buffer ) {
    ALIGNED_FREE( self->_buffer );
  }

  // Steal other's buffer
  self->_buffer = other->_buffer;
  other->_buffer = NULL;

  // Copy state from other and reset other's state
  self->_size = other->_size;
  other->_size = 0;

  self->_wp = other->_wp;
  other->_wp = NULL;

  self->_rp = other->_rp;
  other->_rp = NULL;

  self->_capacity_exp = other->_capacity_exp;
  other->_capacity_exp = 0;

  self->_initial_exp = other->_initial_exp;
  other->_initial_exp = 0;

  self->_load = other->_load;
  other->_load = 0;
#if defined _CSEQ_CIRCULAR_BUFFER
  self->_mask = other->_mask;
#endif
#if defined _CSEQ_FEATURE_UNREAD_METHOD
  self->_prev_rp = NULL;
#endif
#if defined _CSEQ_FEATURE_STREAM_IO
  self->_fd_in = COMLIBSEQUENCE_FD_NONE;
  self->_fd_out = COMLIBSEQUENCE_FD_NONE;
  self->_flags.owns_fd_in = false;
  self->_flags.owns_fd_out = false;
  self->_prev_fpos = -1;
#endif

  return self->_size;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_transplant_from( _CSEQ_TYPENAME *self, _CSEQ_TYPENAME *other ) {
  // NOTE: Calling this with typecast "other" causes undefined behavior, don't do it.

  int64_t transplanted_elements;
  
  SYNCHRONIZE_TWO_COMLIB_SEQUENCES( self, other ) {
    transplanted_elements = ComlibSequence_transplant_from_nolock( self, other );
  } RELEASE_TWO_COMLIB_SEQUENCES;

  return transplanted_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_YANKBUFFER_METHOD ) 
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_yank_buffer_nolock( _CSEQ_TYPENAME *self ) {
  _CSEQ_ELEMENT_TYPE *buffer = self->_buffer;
  self->_buffer = NULL;
  self->_rp = NULL;
  self->_wp = NULL;
#if defined _CSEQ_CIRCULAR_BUFFER
  self->_mask = 0; // (probably never enabled since we can't yank circular buffer, but just in case)
#endif
  self->_size = 0;
  self->_capacity_exp = 0;
  self->_load = 0;
  return buffer;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static _CSEQ_ELEMENT_TYPE * ComlibSequence_yank_buffer( _CSEQ_TYPENAME *self ) {
  _CSEQ_ELEMENT_TYPE *buffer;
  SYNCHRONIZE_ON( self->_lock ) {
    buffer = ComlibSequence_yank_buffer_nolock( self );
  } RELEASE;
  return buffer;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_DISCARD_METHOD ) 
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_discard_nolock( _CSEQ_TYPENAME *self, int64_t element_count ) {
  int count_ok = element_count >= 0 && element_count <= self->_size;


#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
  self->_prev_rp = self->_rp; // save read pointer before discard
#endif

#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_input_descriptor(self) && !count_ok ) {
    self->_prev_fpos = CX_TELL( self->_fd_in ); // allow on level of read-undo (file)
    // have to advance the file
    if( element_count > self->_size ) { // skip delta in file
      int64_t skip_bytes = (element_count - self->_size) * sizeof(_CSEQ_ELEMENT_TYPE);
      CX_SEEK( self->_fd_in, skip_bytes, SEEK_CUR ); // any error ignored. (TODO: handle?)
    }
    else { // skip entire file
      CX_SEEK( self->_fd_in, 0, SEEK_END ); // any error ignored.  (TODO: handle?)
    }
    // empty entire buffer
    self->_rp = self->_wp;
    self->_size = 0;
  }
  else {
#endif
    // fast forward
    if( !count_ok ) {
      element_count = self->_size; // entire buffer if count was outside bounds
    }
    // Proceed with in-buffer discard
    if( self->_size > 0 ) {
      __guarded_add( self, self->_rp, element_count );
      self->_size -= element_count;
    }
    // Full reset when we discard an already empty buffer (secret optimization trick)
    else {
      self->_rp = self->_wp = self->_buffer;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
      self->_prev_rp = NULL; // no undo for this
#endif
    }
#if defined( _CSEQ_FEATURE_STREAM_IO )
  }
#endif
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void ComlibSequence_discard( _CSEQ_TYPENAME *self, int64_t element_count ) {
  SYNCHRONIZE_ON( self->_lock ) {
    ComlibSequence_discard_nolock( self, element_count );
  } RELEASE;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_UNWRITE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_unwrite_nolock( _CSEQ_TYPENAME *self, int64_t element_count ) {
  int count_ok = element_count >= 0 && element_count <= self->_size;
  int64_t unwritten_elements = 0;

#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    // Not supported when we have output file (it's too late to unwrite)
    unwritten_elements = -1;
  }
  else {
#endif
    // rewind buffer
    if( !count_ok ) {
      element_count = self->_size; // entire buffer if count was outside bounds
    }

    // Proceed with in-buffer rewind
    __ComlibSequence_shift_cursor( self, &self->_wp, -element_count );
    unwritten_elements = element_count;
#if defined( _CSEQ_FEATURE_STREAM_IO )
  }
#endif

  return unwritten_elements;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_unwrite( _CSEQ_TYPENAME *self, int64_t element_count ) {
  int64_t unwritten_elements;
  SYNCHRONIZE_ON( self->_lock ) {
    unwritten_elements = ComlibSequence_unwrite_nolock( self, element_count );
  } RELEASE;
  return unwritten_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_POP_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_pop_nolock( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) {
    return -1; // can't pop from a file
  }
#endif
  if( self->_size > 0 ) {
    __guarded_dec( self, self->_wp );
    *dest = *self->_wp;
    self->_size--;
    return 1;
  }
  else {
    return 0;
  }
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int ComlibSequence_pop( _CSEQ_TYPENAME *self, _CSEQ_ELEMENT_TYPE *dest ) {
  int ret;
  SYNCHRONIZE_ON( self->_lock ) {
    ret = ComlibSequence_pop_nolock( self, dest );
  } RELEASE;
  return ret;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_DUMP_METHOD )
#if !defined( _CSEQ_FEATURE_STREAM_IO )
#error This implementation requires _CSEQ_FEATURE_STREAM_IO
#endif
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_dump_nolock( const _CSEQ_TYPENAME *self ) {
  int64_t dumped_elements = 0;
  XTRY {
    if( !ComlibSequence_has_output_descriptor(self) ) {
      COMLIB_OBJECT_REPR( self, NULL );
    }
    else {
      if( self->_wp <= self->_rp && self->_size > 0 ) {
        // discontinuity - must write twice
        _CSEQ_ELEMENT_TYPE *endwall = self->_buffer + (1ULL << self->_capacity_exp);
        int64_t sztail = endwall - self->_rp;
        int64_t szhead = self->_wp - self->_buffer;
        int64_t written_elements = 0;
        if( (written_elements = CX_WRITE( self->_rp, sizeof(_CSEQ_ELEMENT_TYPE), sztail, self->_fd_out )) != sztail ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x1F2 );
        }
        dumped_elements += written_elements;
        if( (written_elements = CX_WRITE( self->_buffer, sizeof(_CSEQ_ELEMENT_TYPE), szhead, self->_fd_out )) != szhead ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x1F3 );
        }
        dumped_elements += written_elements;
      }
      else {
        // data is contiguous, single call to write is ok
        if( (dumped_elements = CX_WRITE( self->_rp, sizeof(_CSEQ_ELEMENT_TYPE), self->_size, self->_fd_out )) != self->_size ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x1F4 );
        }
      }
    }
  }
  XCATCH( errcode ) {
    __ComlibSequence_set_error( (_CSEQ_TYPENAME*)self, "dump failed" );
    dumped_elements = -1;
  }
  XFINALLY {}
  return dumped_elements;
}


#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_dump( const _CSEQ_TYPENAME *self ) {
  int64_t dumped_elements;
  SYNCHRONIZE_ON( ((_CSEQ_TYPENAME*)self)->_lock ) {
    dumped_elements = ComlibSequence_dump_nolock( self );
  } RELEASE;
  return dumped_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_FLUSH_METHOD )
#if !defined( _CSEQ_FEATURE_STREAM_IO )
#error This implementation requires _CSEQ_FEATURE_STREAM_IO
#endif
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_flush_nolock( _CSEQ_TYPENAME *self ) {
  int64_t flushed_elements = ComlibSequence_dump_nolock( self );

  // fast forward, non-undoable discard of entire queue
  self->_rp = self->_wp;
  self->_size = 0;

  return flushed_elements;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_flush( _CSEQ_TYPENAME *self ) {
  int64_t flushed_elements;
  SYNCHRONIZE_ON( self->_lock ) {
    flushed_elements = ComlibSequence_flush_nolock( self );
  } RELEASE;
  return flushed_elements;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_SORT_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_sort_nolock( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) { // <= can't reorder when streaming to output!
    return false;
  }
#endif
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // We need to ensure contiguous buffer first
  int64_t sz = ComlibSequence_optimize_nolock( self );
  qsort( self->_rp, sz, sizeof(_CSEQ_ELEMENT_TYPE), (f_compare_t)self->_cmp );
#elif defined( _CSEQ_LINEAR_BUFFER )
  qsort( self->_buffer, self->_size, sizeof(_CSEQ_ELEMENT_TYPE), (f_compare_t)self->_cmp );
#else
#error
#endif
  return true;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_sort( _CSEQ_TYPENAME *self ) {
  bool success;
  SYNCHRONIZE_ON( self->_lock ) {
    success = ComlibSequence_sort_nolock( self );
  } RELEASE;
  return success;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_REVERSE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __reverse( _CSEQ_ELEMENT_TYPE *base, int64_t sz, int64_t stop_after ) {
  _CSEQ_ELEMENT_TYPE *p1, *p2;
  p1 = base;
  p2 = base + sz - 1;
  if( stop_after > 0 ) {
    while( stop_after-- > 0 && p1 < p2 ) {
      __swap_elements( p1++, p2-- );
    }
  }
  else {
    while( p1 < p2 ) {
      __swap_elements( p1++, p2-- );
    }
  }
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_reverse_nolock( _CSEQ_TYPENAME *self ) {
#if defined( _CSEQ_FEATURE_STREAM_IO )
  if( ComlibSequence_has_output_descriptor(self) ) { // <= can't reorder when streaming to output!
    return false;
  }
#endif
#if defined( _CSEQ_CIRCULAR_BUFFER )
  // Disjoint
  if( self->_size > 0 && self->_wp <= self->_rp ) {
    // From:  D E - - - A B C
    //          wp^   rp^
    // To:    E D C B A - - -
    //        ^rp     wp^
    _CSEQ_ELEMENT_TYPE *endwall = self->_buffer + (1ULL << self->_capacity_exp);
    int64_t sz_tail = (int64_t)(endwall - self->_rp);
    int64_t sz_head = (int64_t)(self->_wp - self->_buffer);
    int64_t sz_gap = (int64_t)(self->_rp - self->_wp);
    // E D - - - A B C
    __reverse( self->_buffer, sz_head, -1 );
    // E D C B A - - -
    __reverse( self->_buffer + sz_head, sz_gap + sz_tail, sz_tail );
    // reset cursors
    self->_rp = self->_buffer;
    self->_wp = self->_buffer + self->_size;
  }
  // Contiguous
  else {
    // From:  - - A B C D E -
    //          rp^       wp^
    // To  :  - - E D C B A -
    //          rp^       wp^
    __reverse( self->_rp, self->_size, -1 );
  }
#elif defined( _CSEQ_LINEAR_BUFFER )
  __reverse( self->_buffer, self->_size, -1 );
#else
#error
#endif

  return true;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool ComlibSequence_reverse( _CSEQ_TYPENAME *self ) {
  bool success;
  SYNCHRONIZE_ON( self->_lock ) {
    success = ComlibSequence_reverse_nolock( self );
  } RELEASE;
  return success;
}
#endif
#endif



#if defined( _CSEQ_FEATURE_OPTIMIZE_METHOD )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_optimize_nolock( _CSEQ_TYPENAME *self ) {
  int64_t element_count = self->_size;
  _CSEQ_ELEMENT_TYPE *newbuf = NULL;
  int64_t new_element_capacity = 0;

  XTRY {
    _CSEQ_ELEMENT_TYPE *wp;
    int new_exp = imag2( self->_size );
    new_exp = new_exp < _CSEQ_MIN_CAPACITY_EXP ? _CSEQ_MIN_CAPACITY_EXP : new_exp;
    new_element_capacity = 1ULL << new_exp;

#if defined( _CSEQ_LINEAR_BUFFER )
    if( true ) {
#else
    // CASE 1: we shrink the (way oversized) queue into a new, smaller contiguous buffer
    if( new_exp < self->_capacity_exp - 2 ) { // <= only shrink if the existing buffer is many times too large for the data
#endif

      size_t alignment = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE );

      // allocate new buffer as small as possible to hold current data
      ALIGNED_ARRAY_THROWS( newbuf, _CSEQ_ELEMENT_TYPE, new_element_capacity, alignment, 0x201 );
      wp = newbuf;

      // copy data to new buffer
      _CSEQ_ELEMENT_TYPE *rp = self->_rp;
      __begin_buffer_section( self, rp ) {
        for( int64_t i=0; i<element_count; i++ ) {
          *wp++ = *__cursor;
          __cursor_inc;
        }
      } __end_buffer_section;

      // replace the old buffer with new optimized buffer
      ALIGNED_FREE( self->_buffer );
      self->_buffer = newbuf;
      self->_rp = self->_buffer;
      self->_wp = self->_buffer + self->_size;
#if defined( _CSEQ_FEATURE_UNREAD_METHOD )
      self->_prev_rp = NULL;
#endif
      newbuf = NULL;
      self->_capacity_exp = (uint8_t)new_exp;
#if defined( _CSEQ_CIRCULAR_BUFFER )
      self->_mask = new_element_capacity * sizeof( _CSEQ_ELEMENT_TYPE ) - 1;
#endif
    }
#if defined( _CSEQ_CIRCULAR_BUFFER )
    // CASE 2: we can't shrink but we can make contiguous in-place
    else if( self->_wp <= self->_rp && self->_size > 0 ) {
#if defined( _CSEQ_FEATURE_REVERSE_METHOD )
      // first reverse converts disjoint buffer to contiguous buffer and leaves reverse data at start of buffer
      ComlibSequence_reverse_nolock( self );
      // second reverse restores original order at start of buffer
      ComlibSequence_reverse_nolock( self );
#else
#error This implementation requires _CSEQ_FEATURE_REVERSE_METHOD
#endif
    }
    // CASE 3: no optimization needed
    else {
    }
#endif
  }
  XCATCH( errcode ) {
    if( newbuf ) {
      ALIGNED_FREE( newbuf );
    }
    element_count = -1;
  }
  XFINALLY {}

  return element_count;
}



#if defined( _CSEQ_FEATURE_SYNCHRONIZED_API )
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t ComlibSequence_optimize( _CSEQ_TYPENAME *self ) {
  int64_t element_count;
  SYNCHRONIZE_ON( self->_lock ) {
    element_count = ComlibSequence_optimize_nolock( self );
  } RELEASE;
  return element_count;
}
#endif



#endif






#endif

