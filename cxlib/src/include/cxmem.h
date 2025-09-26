/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxmem.h
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

#ifndef CXLIB_CXMEM_H
#define CXLIB_CXMEM_H

#include "cxexcept.h"

/* Aligned memory allocation */
#if defined CXPLAT_ARCH_X64
#define CACHE_LINE_SIZE 64  // x86_64 alignment fixed for now, more platforms in future?
#define ARCH_PAGE_SIZE 4096ULL // TODO: find a better way
#elif defined CXPLAT_ARCH_ARM64
#define CACHE_LINE_SIZE 64  // L1 is often 64 bytes but L1D may be 128?, L2+ may be 128? Keep at 64 for now
#define ARCH_PAGE_SIZE 4096ULL // TODO: find a better way
#else
#error "Unsupported platform"
#endif



#if defined CXPLAT_WINDOWS_X64
#  define _ALIGNED_BYTES(p, cnt, A)         ( p = (void*)(_aligned_malloc( cnt, A ) ) )
#  define _ALIGNED_ELEMENTS(p, T, cnt, A)   ( p = (T*)(_aligned_malloc( sizeof(T)*(cnt), A ) ) )
#  define _ALIGNED_FREE(p)                  _aligned_free(p)
#  define _ALIGNED_(A)                      __declspec(align(A))
#else
__inline static size_t __min_memalignment( size_t A ) { return A > sizeof(void*) ? A : sizeof(void*); }
#  define _ALIGNED_BYTES(p, cnt, A)         (posix_memalign( (void*)(&p), __min_memalignment(A), cnt) == 0 ? (char*)(p) : NULL)
#  define _ALIGNED_ELEMENTS(p, T, cnt, A)   (posix_memalign( (void*)(&p), __min_memalignment(A), sizeof(T)*(cnt)) == 0 ? (T*)(p) : NULL)
#  define _ALIGNED_FREE(p)                  free(p)
#  define _ALIGNED_(A)                      __attribute__ ((aligned(A)))
#endif

/* convenience pattern for NULL-checking allocation within a XTRY-XCATCH-XFINALLY macro */

#define _ALIGNED_BYTES_THROWS(p, cnt, A, err)                             \
  do {                                                                    \
    if( _ALIGNED_BYTES(p, cnt, A) == NULL ) {                             \
      THROW_CRITICAL( CXLIB_ERR_MEMORY, err & CXLIB_EXC_CODE_MASK );      \
    }                                                                     \
  } WHILE_ZERO

#define _ALIGNED_ELEMENTS_THROWS(p, T, cnt, A, err)                       \
  do {                                                                    \
    if( _ALIGNED_ELEMENTS(p, T, cnt, A) == NULL ) {                       \
      THROW_CRITICAL( CXLIB_ERR_MEMORY, err & CXLIB_EXC_CODE_MASK );      \
    }                                                                     \
  } WHILE_ZERO

#define _ALIGNED_ZELEMENTS_THROWS(p, T, cnt, A, err)                      \
  do {                                                                    \
    if( _ALIGNED_ELEMENTS(p, T, cnt, A) == NULL ) {                       \
      THROW_CRITICAL( CXLIB_ERR_MEMORY, err & CXLIB_EXC_CODE_MASK );      \
    }                                                                     \
    memset( p, 0, sizeof(T) * cnt );                                      \
  } WHILE_ZERO


#define _ALIGNED_INITIALIZED_BYTES_THROWS(p, cnt, initval, A, err)        \
  do {                                                                    \
    _ALIGNED_BYTES_THROWS(p,cnt,A,err);                                   \
    for( size_t i=0; i<cnt; i++) {                                        \
      (p)[i]=(initval);                                                   \
    }                                                                     \
  } WHILE_ZERO

#define _ALIGNED_INITIALIZED_ELEMENTS_THROWS(p, T, cnt, initval, A, err)  \
  do {                                                                    \
    _ALIGNED_ELEMENTS_THROWS(p,T,cnt,A,err);                              \
    for( size_t i=0; i<cnt; i++) {                                        \
      (p)[i]=(initval);                                                   \
    }                                                                     \
  } WHILE_ZERO

/* allocate bytes on specified alignment boundary */
#define ALIGNED_BYTES(memptr, sz, alignment)      _ALIGNED_BYTES(memptr,sz,alignment)
/* allocate bytes on cache line boundary */
#define CALIGNED_BYTES(memptr, sz)                _ALIGNED_BYTES(memptr,sz,CACHE_LINE_SIZE)
/* allocate bytes on page boundary */
#define PALIGNED_BYTES(memptr, sz)                _ALIGNED_BYTES(memptr,sz,ARCH_PAGE_SIZE)

/* allocate memory for one element of type T on specified alignment boundary */
#define ALIGNED_MALLOC(memptr, T, alignment)      _ALIGNED_ELEMENTS(memptr,T,1,alignment)
/* allocate memory for one element of type T with type alignment */
#define TALIGNED_MALLOC(memptr, T)                _ALIGNED_ELEMENTS(memptr,T,1,__alignof(T))
/* allocate memory for one element of type T on cache line boundary */
#define CALIGNED_MALLOC(memptr, T)                _ALIGNED_ELEMENTS(memptr,T,1,CACHE_LINE_SIZE)
/* allocate memory for one element of type T on page boundary */
#define PALIGNED_MALLOC(memptr, T)                _ALIGNED_ELEMENTS(memptr,T,1,ARCH_PAGE_SIZE)

/* allocate memory for N elements of type T on specified alignment boundary */
#define ALIGNED_ARRAY(arrayptr, T, N, alignment)  _ALIGNED_ELEMENTS(arrayptr,T,N,alignment)
/* allocate memory for N elements of type T with type alignment */
#define TALIGNED_ARRAY(arrayptr, T, N)            _ALIGNED_ELEMENTS(arrayptr,T,N,__alignof(T))
/* allocate memory for N elements of type T on cache line boundary */
#define CALIGNED_ARRAY(arrayptr, T, N)            _ALIGNED_ELEMENTS(arrayptr,T,N,CACHE_LINE_SIZE)
/* allocate memory for N elements of type T on page boundary */
#define PALIGNED_ARRAY(arrayptr, T, N)            _ALIGNED_ELEMENTS(arrayptr,T,N,ARCH_PAGE_SIZE)

/* allocate bytes on specified alignment boundary */
#define ALIGNED_BYTES_THROWS(memptr, sz, alignment, err)      _ALIGNED_BYTES_THROWS(memptr,sz,alignment,err)
/* allocate bytes on cache line boundary */
#define CALIGNED_BYTES_THROWS(memptr, sz, err)                _ALIGNED_BYTES_THROWS(memptr,sz,CACHE_LINE_SIZE,err)
/* allocate bytes on page boundary */
#define PALIGNED_BYTES_THROWS(memptr, sz, err)                _ALIGNED_BYTES_THROWS(memptr,sz,ARCH_PAGE_SIZE,err

/* allocate memory for one element of type T on specified alignment boundary */
#define ALIGNED_MALLOC_THROWS(memptr, T, alignment, err)      _ALIGNED_ELEMENTS_THROWS(memptr,T,1,alignment,err)
/* allocate memory for one element of type T with type alignment */
#define TALIGNED_MALLOC_THROWS(memptr, T, err)                _ALIGNED_ELEMENTS_THROWS(memptr,T,1,__alignof(T),err)
/* allocate memory for one element of type T on cache line boundary */
#define CALIGNED_MALLOC_THROWS(memptr, T, err)                _ALIGNED_ELEMENTS_THROWS(memptr,T,1,CACHE_LINE_SIZE,err)
/* allocate memory for one element of type T on page boundary */
#define PALIGNED_MALLOC_THROWS(memptr, T, err)                _ALIGNED_ELEMENTS_THROWS(memptr,T,1,ARCH_PAGE_SIZE,err)

/* allocate memory for N elements of type T on specified alignment boundary */
#define ALIGNED_ARRAY_THROWS(arrayptr, T, N, alignment, err)  _ALIGNED_ELEMENTS_THROWS(arrayptr,T,N,alignment,err)
/* allocate memory for N elements of type T with type alignment */
#define TALIGNED_ARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ELEMENTS_THROWS(arrayptr,T,N,__alignof(T),err)
/* allocate memory for N elements of type T on cache line boundary */
#define CALIGNED_ARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ELEMENTS_THROWS(arrayptr,T,N,CACHE_LINE_SIZE,err)
/* allocate memory for N elements of type T on page boundary */
#define PALIGNED_ARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ELEMENTS_THROWS(arrayptr,T,N,ARCH_PAGE_SIZE,err)

/* allocate memory for N elements of type T on specified alignment boundary and set all to zero */
#define ALIGNED_ZARRAY_THROWS(arrayptr, T, N, alignment, err)  _ALIGNED_ZELEMENTS_THROWS(arrayptr,T,N,alignment,err)
/* allocate memory for N elements of type T with type alignment and set all to zero */
#define TALIGNED_ZARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ZELEMENTS_THROWS(arrayptr,T,N,__alignof(T),err)
/* allocate memory for N elements of type T on cache line boundary and set all to zero  */
#define CALIGNED_ZARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ZELEMENTS_THROWS(arrayptr,T,N,CACHE_LINE_SIZE,err)
/* allocate memory for N elements of type T on page boundary and set all to zero */
#define PALIGNED_ZARRAY_THROWS(arrayptr, T, N, err)            _ALIGNED_ZELEMENTS_THROWS(arrayptr,T,N,ARCH_PAGE_SIZE,err)


/* allocate memory for N elements of type T on specified alignment boundary and set initial values */
#define ALIGNED_INITIALIZED_ARRAY_THROWS(arrayptr, T, N, V, alignment, err)  _ALIGNED_INITIALIZED_ELEMENTS_THROWS(arrayptr,T,N,V,alignment,err)
/* allocate memory for N elements of type T with type alignment and set initial values */
#define TALIGNED_INITIALIZED_ARRAY_THROWS(arrayptr, T, N, V, err)            _ALIGNED_INITIALIZED_ELEMENTS_THROWS(arrayptr,T,N,V,__alignof(T),err)
/* allocate memory for N elements of type T on cache line boundary and set initial values */
#define CALIGNED_INITIALIZED_ARRAY_THROWS(arrayptr, T, N, V, err)            _ALIGNED_INITIALIZED_ELEMENTS_THROWS(arrayptr,T,N,V,CACHE_LINE_SIZE,err)
/* allocate memory for N elements of type T on page boundary and set initial values */
#define PALIGNED_INITIALIZED_ARRAY_THROWS(arrayptr, T, N, V, err)            _ALIGNED_INITIALIZED_ELEMENTS_THROWS(arrayptr,T,N,V,ARCH_PAGE_SIZE,err)


/* free aligned memory */
#define ALIGNED_FREE(memptr)                      _ALIGNED_FREE(memptr)
/* variable aligned on specified boundary */
#define ALIGNED_(alignment)                       _ALIGNED_(alignment)
/* variable aligned on cache line boundary */
#define CALIGNED_                                 _ALIGNED_(CACHE_LINE_SIZE)
/* variable aligned on page boundary */
#define PALIGNED_                                 _ALIGNED_(ARCH_PAGE_SIZE)

/* declare variable aligned on specified boundary */
#define ALIGNED_VAR( type, name, alignment )      type _ALIGNED_(alignment) name
/* declare variable aligned on cache line boundary */
#define CALIGNED_VAR( type, name )                type CALIGNED_ name
/* declare variable aligned on page size boundary */
#define PALIGNED_VAR( type, name )                type PALIGNED_ name

/* define new type with specified alignment requirement */
#define ALIGNED_TYPE(struct_or_union, alignment)  typedef struct_or_union ALIGNED_(alignment)
/* define new type with cache line alignment requirement */
#define CALIGNED_TYPE(struct_or_union)            typedef struct_or_union CALIGNED_
/* define new type with page size alignment requirement */
#define PALIGNED_TYPE(struct_or_union)            typedef struct_or_union PALIGNED_


__inline static void free_const( const void *Memory ) {
  free( (void*)Memory );
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
CALIGNED_TYPE(union) u_cacheline_t {
  uint8_t   bytes[CACHE_LINE_SIZE];
  uint16_t  words[CACHE_LINE_SIZE/2];
  uint32_t  dwords[CACHE_LINE_SIZE/4];
  uint64_t  qwords[CACHE_LINE_SIZE/8];
  __m128i   m128i[CACHE_LINE_SIZE/16];
  __m256i   m256i[CACHE_LINE_SIZE/32];
} cacheline_t;



#if defined CXPLAT_ARCH_X64
__inline static void __prefetch_L1( void *p ) {
  _mm_prefetch( (char*)p, _MM_HINT_T0 );
}

__inline static void __prefetch_L2( void *p ) {
  _mm_prefetch( (char*)p, _MM_HINT_T1 );
}

__inline static void __prefetch_L3( void *p ) {
  _mm_prefetch( (char*)p, _MM_HINT_T2 );
}

__inline static void __prefetch_nta( void *p ) {
  _mm_prefetch( (char*)p, _MM_HINT_NTA );
}
#elif defined CXPLAT_ARCH_ARM64
__inline static void __prefetch_L1( void *p ) {
  __builtin_prefetch(p, 0, 3 );
}

__inline static void __prefetch_L2( void *p ) {
  __builtin_prefetch(p, 0, 2 );
}

__inline static void __prefetch_L3( void *p ) {
  __builtin_prefetch(p, 0, 1 );
}

__inline static void __prefetch_nta( void *p ) {
  __builtin_prefetch(p, 0, 0 );
}
#else
#error "Unsupported architecture"
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define ASSERT_TYPE_SIZE( TypeName, ExpectedBytes ) \
  do {                                              \
    size_t type_bytes = sizeof( TypeName );         \
    size_t expected_bytes = ExpectedBytes;          \
    if( type_bytes != expected_bytes ) {            \
      FATAL( 0xFFF, "%s must be %llu bytes, got %llu", #TypeName, expected_bytes, type_bytes ); \
    }                                               \
  } WHILE_ZERO



#if defined CXPLAT_ARCH_X64
void stream_memset( cacheline_t *dst, const cacheline_t *src, const size_t cnt );
#endif






/*******************************************************************//**
 *
 * TODO: generalize and move elsewhere
 ***********************************************************************
 */
typedef union __u_float_bits_t {
  DWORD d;
  float f;
  struct {
    DWORD fraction  : 23;
    DWORD exponent  : 8;
    DWORD sign      : 1;
  } format;
  struct {
    DWORD __discard : 20;
    DWORD code      : 6;
    DWORD high      : 6;
  } vmag;
  struct {
    DWORD discard   : 16;
    DWORD keep      : 16; 
  } trunc;
} __float_bits_t;



#endif
