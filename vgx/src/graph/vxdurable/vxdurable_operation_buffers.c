/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_buffers.c
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

#include "_vxoperation.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static const char START_GUARD[]     = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";
#define sz_START_GUARD  (sizeof( START_GUARD ) - 1)
static const char END_GUARD[]       = "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<";
#define sz_END_GUARD    (sizeof( END_GUARD ) - 1)



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static void __set_buffer_guard( char *data, int64_t capacity ) {
  char *s_guard = data - sz_START_GUARD;
  memcpy( s_guard, START_GUARD, sz_START_GUARD );
  char *e_guard = data + capacity;
  memcpy( e_guard, END_GUARD, sz_END_GUARD );
}


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __trap_buffer_guard_corrupted( const vgx_OperationBuffer_t *buffer ) {
  char sbuf[ sz_START_GUARD + 1 ] = {0};
  char ebuf[ sz_END_GUARD + 1 ] = {0};
  memcpy( sbuf, buffer->data - sz_START_GUARD, sz_START_GUARD );
  memcpy( ebuf, buffer->data - buffer->capacity, sz_END_GUARD );
  FATAL( 0xFFF, "BUFFER CORRUPTED!  start='%s' end='%s'", sbuf, ebuf );
}


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static void __check_buffer_guard( const vgx_OperationBuffer_t *buffer ) {
  const char *s = buffer->data - sz_START_GUARD;
  const char *e = buffer->data + buffer->capacity;
  if( memcmp( s, START_GUARD, sz_START_GUARD ) || memcmp( e, END_GUARD, sz_END_GUARD ) ) {
    __trap_buffer_guard_corrupted( buffer );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static const char * __end_buffer( const vgx_OperationBuffer_t *buffer ) {
  return buffer->data + buffer->capacity;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __sz_end_segment( const vgx_OperationBuffer_t *buffer, const char *p ) {
  return __end_buffer( buffer ) - p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __sz_start_segment( const vgx_OperationBuffer_t *buffer, const char *p ) {
  return p - buffer->data;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __distance( const vgx_OperationBuffer_t *buffer, const char *a, const char *b ) {
  if( a <= b ) {
    return b - a;
  }
  else {
    return __sz_end_segment( buffer, a ) + __sz_start_segment( buffer, b );
  }
}



/*******************************************************************//**
 * Writable is the empty space between write pointer and commit pointer
 * minus one. (We can't allow wp to catch up with cp because that would
 * make the buffer size indistinguishable from zero.)
 *           
 *         wwwwwwwwwwww
 * [.......            .....]
 *         ^           ^
 *         wp          cp
 ***********************************************************************
 */
__inline static int64_t __sz_writable( const vgx_OperationBuffer_t *buffer ) {
  int64_t d = __distance( buffer, buffer->wp, buffer->cp );
  if( d == 0 ) {
    return buffer->capacity - 1;
  }
  else {
    return d - 1;
  }
}



/*******************************************************************//**
 * Readable is the written space between read pointer and write pointer.
 * 
 * 
 *         rrrrrrrrrrrr
 * [       ............      ]
 *         ^           ^
 *         rp          wp
 ***********************************************************************
 */
__inline static int64_t __sz_readable( const vgx_OperationBuffer_t *buffer ) {
  return __distance( buffer, buffer->rp, buffer->wp );
}



/*******************************************************************//**
 * Unconfirmed is the space between the commit pointer and read pointer.
 * 
 * 
 *         dddddddddddd
 * [       ............      ]
 *         ^           ^
 *         cp          rp
 ***********************************************************************
 */
__inline static int64_t __sz_unconfirmed( const vgx_OperationBuffer_t *buffer ) {
  return __distance( buffer, buffer->cp, buffer->rp );
}



/*******************************************************************//**
 * Written is the space between the commit pointer and write pointer.
 * 
 * 
 *         dddddddddddd
 * [       ............      ]
 *         ^           ^
 *         cp   (rp)   wp
 ***********************************************************************
 */
__inline static int64_t __sz_written( const vgx_OperationBuffer_t *buffer ) {
  return __distance( buffer, buffer->cp, buffer->wp );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_non_empty( vgx_OperationBuffer_t *buffer ) {
  OPERATION_BUFFER_LOCK( buffer ) {
    buffer->flags_CS.has_readable = 1;
    buffer->flags_CS.has_unconfirmed = 1;
  } OPERATION_BUFFER_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_empty( vgx_OperationBuffer_t *buffer ) {
  OPERATION_BUFFER_LOCK( buffer ) {
    buffer->flags_CS.has_readable = 0;
    buffer->flags_CS.has_unconfirmed = 0;
  } OPERATION_BUFFER_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __empty( vgx_OperationBuffer_t *buffer ) {
  bool e;
  OPERATION_BUFFER_LOCK( buffer ) {
    e = buffer->flags_CS.has_readable == 0 && buffer->flags_CS.has_unconfirmed == 0;
  } OPERATION_BUFFER_RELEASE;
  return e;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_all_read( vgx_OperationBuffer_t *buffer ) {
  OPERATION_BUFFER_LOCK( buffer ) {
    buffer->flags_CS.has_readable = 0;
  } OPERATION_BUFFER_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_all_confirmed( vgx_OperationBuffer_t *buffer ) {
  OPERATION_BUFFER_LOCK( buffer ) {
    buffer->flags_CS.has_unconfirmed = 0;
  } OPERATION_BUFFER_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __readable( vgx_OperationBuffer_t *buffer ) {
  bool r;
  OPERATION_BUFFER_LOCK( buffer ) {
    r = buffer->flags_CS.has_readable;
  } OPERATION_BUFFER_RELEASE;
  return r;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __unconfirmed( vgx_OperationBuffer_t *buffer ) {
  bool u;
  OPERATION_BUFFER_LOCK( buffer ) {
    u = buffer->flags_CS.has_unconfirmed;
  } OPERATION_BUFFER_RELEASE;
  return u;
}




static vgx_OperationBuffer_t *  _operation_buffer__new( int order, const char *Name );
static void                     _operation_buffer__delete( vgx_OperationBuffer_t **buffer );
static const char *             _operation_buffer__name( vgx_OperationBuffer_t *buffer, const char *name );

static int64_t                  _operation_buffer__capacity( const vgx_OperationBuffer_t *buffer );
static int64_t                  _operation_buffer__size( const vgx_OperationBuffer_t *buffer );
static bool                     _operation_buffer__empty( vgx_OperationBuffer_t *buffer );
static int64_t                  _operation_buffer__readable( const vgx_OperationBuffer_t *buffer );
static int64_t                  _operation_buffer__writable( const vgx_OperationBuffer_t *buffer );
static int64_t                  _operation_buffer__unconfirmed( const vgx_OperationBuffer_t *buffer );

static int64_t                  _operation_buffer__get_readable( const vgx_OperationBuffer_t *buffer, char **data );
static int64_t                  _operation_buffer__get_tail( const vgx_OperationBuffer_t *buffer, char **data, int64_t n );
static int64_t                  _operation_buffer__read_until( vgx_OperationBuffer_t *buffer, int64_t max, char **data, const char probe );
static int64_t                  _operation_buffer__get_unconfirmed( const vgx_OperationBuffer_t *buffer, char **data );

static unsigned int             _operation_buffer__crc_readable( const vgx_OperationBuffer_t *buffer, unsigned int crc );
static unsigned int             _operation_buffer__crc_unconfirmed( const vgx_OperationBuffer_t *buffer, unsigned int crc );
static unsigned int             _operation_buffer__crc_tail( const vgx_OperationBuffer_t *buffer, int64_t n, unsigned int crc );

static int                      _operation_buffer__expand( vgx_OperationBuffer_t *buffer );
static int                      _operation_buffer__trim( vgx_OperationBuffer_t *buffer, int64_t max_sz );

static int64_t                  _operation_buffer__write( vgx_OperationBuffer_t *buffer, const char *data, int64_t n );
static int64_t                  _operation_buffer__write_crc( vgx_OperationBuffer_t *buffer, const char *data, int64_t n, unsigned int *crc );
static int64_t                  _operation_buffer__copy( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other );
static int64_t                  _operation_buffer__copy_crc( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc );
static int64_t                  _operation_buffer__absorb( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other );
static int64_t                  _operation_buffer__absorb_crc( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc );
static int64_t                  _operation_buffer__unwrite( vgx_OperationBuffer_t *buffer, int64_t n );
static int64_t                  _operation_buffer__writable_segment( vgx_OperationBuffer_t *buffer, int64_t max, char **segment, char **end );
static int64_t                  _operation_buffer__advance_write( vgx_OperationBuffer_t *buffer, int64_t n );
static int64_t                  _operation_buffer__readable_segment( vgx_OperationBuffer_t *buffer, int64_t max, const char **segment, const char **end );
static int64_t                  _operation_buffer__advance_read( vgx_OperationBuffer_t *buffer, int64_t n );
static int64_t                  _operation_buffer__confirm( vgx_OperationBuffer_t *buffer, int64_t n );
static int64_t                  _operation_buffer__rollback( vgx_OperationBuffer_t *buffer );
static int64_t                  _operation_buffer__clear( vgx_OperationBuffer_t *buffer );

static int                      _operation_buffer__acquire( vgx_OperationBuffer_t *buffer );
static int                      _operation_buffer__release( vgx_OperationBuffer_t *buffer );

static void                     _operation_buffer__dump_state( vgx_OperationBuffer_t *buffer );



DLL_EXPORT vgx_IOperationBuffer_t iOpBuffer = {
  .New              = _operation_buffer__new,
  .Delete           = _operation_buffer__delete,
  .Name             = _operation_buffer__name,
  .Capacity         = _operation_buffer__capacity,
  .Size             = _operation_buffer__size,
  .Empty            = _operation_buffer__empty,
  .Readable         = _operation_buffer__readable,
  .Writable         = _operation_buffer__writable,
  .Unconfirmed      = _operation_buffer__unconfirmed,
  .GetReadable      = _operation_buffer__get_readable,
  .GetTail          = _operation_buffer__get_tail,
  .ReadUntil        = _operation_buffer__read_until,
  .GetUnconfirmed   = _operation_buffer__get_unconfirmed,
  .CrcReadable      = _operation_buffer__crc_readable,
  .CrcUnconfirmed   = _operation_buffer__crc_unconfirmed,
  .CrcTail          = _operation_buffer__crc_tail,
  .Expand           = _operation_buffer__expand,
  .Trim             = _operation_buffer__trim,
  .Write            = _operation_buffer__write,
  .WriteCrc         = _operation_buffer__write_crc,
  .Copy             = _operation_buffer__copy,
  .CopyCrc          = _operation_buffer__copy_crc,
  .Absorb           = _operation_buffer__absorb,
  .AbsorbCrc        = _operation_buffer__absorb_crc,
  .Unwrite          = _operation_buffer__unwrite,
  .WritableSegment  = _operation_buffer__writable_segment,
  .AdvanceWrite     = _operation_buffer__advance_write,
  .ReadableSegment  = _operation_buffer__readable_segment,
  .AdvanceRead      = _operation_buffer__advance_read,
  .Confirm          = _operation_buffer__confirm,
  .Rollback         = _operation_buffer__rollback,
  .Clear            = _operation_buffer__clear,
  .Acquire          = _operation_buffer__acquire,
  .Release          = _operation_buffer__release,
  .DumpState        = _operation_buffer__dump_state
};




static vgx_OperationBufferPool_t *  _operation_buffer_pool__new( int64_t capacity, int order, const char *name );
static void                         _operation_buffer_pool__delete( vgx_OperationBufferPool_t **pool );
static int64_t                      _operation_buffer_pool__capacity( const vgx_OperationBufferPool_t *pool );
static int64_t                      _operation_buffer_pool__remain( const vgx_OperationBufferPool_t *pool );
static vgx_OperationBuffer_t *      _operation_buffer_pool__get_buffer( vgx_OperationBufferPool_t *pool );
static int                          _operation_buffer_pool__return_buffer( vgx_OperationBufferPool_t *pool, vgx_OperationBuffer_t **buffer );




DLL_HIDDEN vgx_IOperationBufferPool_t iOpBufferPool = {
  .New              = _operation_buffer_pool__new,
  .Delete           = _operation_buffer_pool__delete,
  .Capacity         = _operation_buffer_pool__capacity,
  .Remain           = _operation_buffer_pool__remain,
  .GetBuffer        = _operation_buffer_pool__get_buffer,
  .ReturnBuffer     = _operation_buffer_pool__return_buffer
};




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __resize( vgx_OperationBuffer_t *buffer, int order ) {

  // Set new capacity
  int64_t capacity = 1LL << order;

  // The new capacity is too small to hold current data
  if( capacity < __sz_written( buffer ) ) {
    return -1;
  }

  VERBOSE( 0x001, "Resize operation buffer '%s' capacity %lld -> %lld", _operation_buffer__name( buffer, NULL ), buffer->capacity, capacity );
  char *prev_alloc = buffer->__allocated;
  char *prev_data = buffer->data;
  const char *prev_cp = buffer->cp;
  const char *prev_rp = buffer->rp;
  char *prev_wp = buffer->wp;

  unsigned int crcA_r = 0;
  unsigned int crcA_u = 0;
  crcA_r = iOpBuffer.CrcReadable( buffer, crcA_r );
  crcA_u = iOpBuffer.CrcUnconfirmed( buffer, crcA_u );

  // Allocate new data
  char *alloc;
  if( CALIGNED_ARRAY( alloc, char, 64 + capacity + 64 ) == NULL ) {
    return -1;
  }
  char *data = alloc + 64;
  __set_buffer_guard( data, capacity );

  // Copy
  char *dst = data;
  const char *src = buffer->cp;
  const char *end;
  
  // Two segments
  if( buffer->cp > buffer->wp ) {
    // Copy until end
    end = __end_buffer( buffer );
    while( src < end ) {
      *dst++ = *src++;
    }
    // Continue at start
    src = buffer->data;
  }

  // Copy until we hit write pointer
  end = buffer->wp;
  while( src < end ) {
    *dst++ = *src++;
  }

  // Swap in new data buffer
  int64_t uA = __sz_unconfirmed( buffer );
  int64_t rA = __sz_readable( buffer );

  buffer->__allocated = alloc;
  buffer->data = data;
  buffer->capacity = capacity;
  buffer->cp = buffer->data;
  buffer->rp = buffer->cp + uA;
  buffer->wp = (char*)buffer->rp + rA;

  int64_t uB = __sz_unconfirmed( buffer );
  int64_t rB = __sz_readable( buffer );

  unsigned int crcB_r = 0;
  unsigned int crcB_u = 0;
  crcB_r = iOpBuffer.CrcReadable( buffer, crcB_r );
  crcB_u = iOpBuffer.CrcUnconfirmed( buffer, crcB_u );
  if( crcA_r != crcB_r || crcA_u != crcB_u ) {
    INFO( 0x000, "CRC mismatch after buffer resize!" );
    INFO( 0x000, "Old CRC:    r=%08x u=%08x", crcA_r, crcA_u );
    INFO( 0x000, "New CRC:    r=%08x u=%08x", crcB_r, crcB_u );
    INFO( 0x000, "Old size:   r=%lld u=%lld", rA, uA );
    INFO( 0x000, "New size:   r=%lld u=%lld", rB, uB );
    INFO( 0x000, "Old offset: c=%lld r=%lld w=%lld", prev_cp - prev_data, prev_rp - prev_data, prev_wp - prev_data ); 
    INFO( 0x000, "New offset: c=%lld r=%lld w=%lld", buffer->cp - buffer->data, buffer->rp - buffer->data , buffer->wp - buffer->data ); 
    FATAL( 0xEEE, "Operation buffer resize corruption" );
  }

  ALIGNED_FREE( prev_alloc );

  return 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_OperationBuffer_t * _operation_buffer__new( int order, const char *name ) {

  vgx_OperationBuffer_t *buffer = NULL;
  int64_t capacity = 1LL << order;

  // Arbitrary warning limit: > 4GB
  if( order > 32 ) {
    WARN( 0x001, "Large buffer allocation (order=%d size=%lld)", order, capacity );
  }

  XTRY {
    // Allocate object
    if( (buffer = calloc( 1, sizeof( vgx_OperationBuffer_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    buffer->lock_count = -1;

    // Initialize
    buffer->capacity = capacity;

    // Allocate data
    CALIGNED_ARRAY_THROWS( buffer->__allocated, char, 64 + buffer->capacity + 64, 0x003 );
    buffer->data = buffer->__allocated + 64;
    __set_buffer_guard( buffer->data, buffer->capacity );


    // Initialize cursors
    buffer->wp = buffer->data;
    buffer->rp = buffer->data;
    buffer->cp = buffer->data;

    // Initialize lock
    INIT_CRITICAL_SECTION( &buffer->lock.lock );
    buffer->lock_count = 0;

    // Reset flags
    buffer->flags_CS.data = 0;
    
    // Set name if provided
    if( name ) {
      if( (buffer->CSTR__name = CStringNew( name )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
    }

  }
  XCATCH( errcode ) {
    _operation_buffer__delete( &buffer );
  }
  XFINALLY {
  }

  return buffer;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _operation_buffer__delete( vgx_OperationBuffer_t **buffer ) {
  if( buffer && *buffer ) {

    if( (*buffer)->__allocated ) {
      ALIGNED_FREE( (*buffer)->__allocated );
    }

    if( (*buffer)->lock_count == 0 ) {
      DEL_CRITICAL_SECTION( &((*buffer)->lock.lock) );
    }

    iString.Discard( &(*buffer)->CSTR__name );

    free( *buffer );
    *buffer = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static const char * _operation_buffer__name( vgx_OperationBuffer_t *buffer, const char *name ) {
  if( name ) {
    CString_t *CSTR__tmp = CStringNew( name );
    if( CSTR__tmp == NULL ) {
      return NULL;
    }
    iString.Discard( &buffer->CSTR__name );
    buffer->CSTR__name = CSTR__tmp;
  }

  if( buffer->CSTR__name ) {
    return CStringValue( buffer->CSTR__name );
  }
  else {
    return "";
  }
}



/*******************************************************************//**
 * Return the buffer capacity
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__capacity( const vgx_OperationBuffer_t *buffer ) {
  // One less that total space since wp cannot be allowed to catch up with rp (would alias with empty state)
  return buffer->capacity - 1;
}



/*******************************************************************//**
 * Return the number of bytes written
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__size( const vgx_OperationBuffer_t *buffer ) {
  return __sz_written( buffer );
}



/*******************************************************************//**
 * Return true if buffer has no readable or unconfirmed data
 *
 * THREAD SAFE
 ***********************************************************************
 */
static bool _operation_buffer__empty( vgx_OperationBuffer_t *buffer ) {
  return __empty( buffer );
}



/*******************************************************************//**
 * Return the number of bytes that can be read
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__readable( const vgx_OperationBuffer_t *buffer ) {
  return __sz_readable( buffer );
}



/*******************************************************************//**
 * Return the number of bytes that can be written
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__writable( const vgx_OperationBuffer_t *buffer ) {
  return __sz_writable( buffer );
}



/*******************************************************************//**
 * Return the number of bytes that can be discarded
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__unconfirmed( const vgx_OperationBuffer_t *buffer ) {
  return __sz_unconfirmed( buffer );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__get_readable( const vgx_OperationBuffer_t *buffer, char **data ) {
  return _operation_buffer__get_tail( buffer, data, -1 );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__get_tail( const vgx_OperationBuffer_t *buffer, char **data, int64_t n ) {
  // Tail size
  if( n < 0 || n > __sz_readable( buffer ) ) {
    n = __sz_readable( buffer );
  }

  char *dst;
  // Allocate one extra byte for nul-terminator
  if( (dst = *data = malloc( n + 1 )) == NULL ) {
    return -1;
  }

  // Amount to skip to get to tail data
  int64_t skip = __sz_readable( buffer ) - n;

  // Start reading at rp plus the skip amount
  const char *src = buffer->rp + skip;
  const char *end;

  // Readable data is split
  if( buffer->rp > buffer->wp ) {
    // Tail data is split
    if( src < (end = __end_buffer( buffer)) ) {
      // Copy from src pointer until end of buffer
      while( src < end ) {
        *dst++ = *src++;
      }
      // Wrap src to start of buffer
      src = buffer->data;
    }
    // Tail data is contained within start segment
    else {
      src = buffer->wp - n;
    }
  }

  // Copy from src until write pointer
  end = buffer->wp;
  while( src < end ) {
    *dst++ = *src++;
  }

  // Add safety terminator
  *dst = '\0';

  return n;
}



/*******************************************************************//**
 *
 * Returns  >0 : length of data read into *data (including probe char)
 *           0 : probe not found
 *          -1 : destination buffer *data too small
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__read_until( vgx_OperationBuffer_t *buffer, int64_t max, char **data, const char probe ) {
  char *dst = *data;
  const char *lim = dst + max - 1; // leave room for nul at end

  // Start reading at rp
  const char *rp = buffer->rp;
  const char *end;

  // Two segments
  if( buffer->rp > buffer->wp ) {
    // Copy from rp until end of buffer
    int64_t sz_end = __sz_end_segment( buffer, buffer->rp );
    end = rp + sz_end;
    while( rp < end && dst < lim ) {
      if( (*dst++ = *rp++) == probe ) {
        // SUCCESS
        goto found;
      }
    }

    // Wrap rp to start of buffer
    rp = buffer->data;
  }

  // Copy from rp until write pointer
  end = buffer->wp;
  while( rp < end && dst < lim ) {
    if( (*dst++ = *rp++) == probe ) {
      // SUCCESS
      goto found;
    }
  }

  // Probe not found
  if( dst < lim ) {
    return 0;
  }
  // Destination buffer too small, search terminated
  else {
    return -1;
  }

found:
  // Add safety terminator to the returned buffer
  *dst = '\0';

  // Consume the read data
  buffer->rp = rp;

  // All read
  if( buffer->rp == buffer->wp ) {
    __set_all_read( buffer );
  }

  // Return length of read data string
  return dst - *data;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__get_unconfirmed( const vgx_OperationBuffer_t *buffer, char **data ) {
  int64_t n = __sz_unconfirmed( buffer );
  char *dst;
  // Allocate one extra byte for nul-terminator
  if( (dst = *data = malloc( n + 1 )) == NULL ) {
    return -1;
  }

  // Start reading at cp
  const char *cp = buffer->cp;
  const char *end;

  // Two segments
  if( buffer->cp > buffer->rp ) {
    // Copy from cp until end of buffer
    end = __end_buffer( buffer );
    while( cp < end ) {
      *dst++ = *cp++;
    }
    // Wrap cp to start of buffer
    cp = buffer->data;
  }

  // Copy from cp until read pointer
  end = buffer->rp;
  while( cp < end ) {
    *dst++ = *cp++;
  }

  // Add safety terminator
  *dst = '\0';

  return n;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static unsigned int _operation_buffer__crc_readable( const vgx_OperationBuffer_t *buffer, unsigned int crc ) {
  return _operation_buffer__crc_tail( buffer, -1, crc );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static unsigned int _operation_buffer__crc_tail( const vgx_OperationBuffer_t *buffer, int64_t n, unsigned int crc ) {
  // Tail size
  if( n < 0 || n > __sz_readable( buffer ) ) {
    n = __sz_readable( buffer );
  }

  // Amount to skip to get to tail data
  int64_t skip = __sz_readable( buffer ) - n;

  // Start reading at rp plus the skip amount
  const char *src = buffer->rp + skip;

  // Readable data is split
  if( buffer->rp > buffer->wp ) {
    const char *end = __end_buffer( buffer );
    // Tail data is split
    if( src < end ) {
      // Scan from src pointer until end of buffer
      crc = crc32c( crc, src, end - src );
      // Wrap src to start of buffer
      src = buffer->data;
    }
    // Tail data is contained within start segment
    else {
      src = buffer->wp - n;
    }
  }

  // Scan from src until write pointer
  crc = crc32c( crc, src, buffer->wp - src );

  return crc;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static unsigned int _operation_buffer__crc_unconfirmed( const vgx_OperationBuffer_t *buffer, unsigned int crc ) {

  // Start reading at cp
  const char *src = buffer->cp;

  // Confirmable data is split
  if( buffer->cp > buffer->rp ) {
    const char *end = __end_buffer( buffer );
    // Scan from src pointer until end of buffer
    crc = crc32c( crc, src, end - src );
    // Wrap src to start of buffer
    src = buffer->data;
  }

  // Scan from src until read pointer
  crc = crc32c( crc, src, buffer->rp - src );

  return crc;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _operation_buffer__expand( vgx_OperationBuffer_t *buffer ) {
  int next_order = imag2( buffer->capacity ) + 1;
  return __resize( buffer, next_order );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _operation_buffer__trim( vgx_OperationBuffer_t *buffer, int64_t max_sz ) {
  // Buffer allocation larger than desired, and data in buffer will fit in smaller allocation
  if( buffer->capacity > max_sz && __sz_written( buffer ) < max_sz ) {
    int order = imag2( max_sz ) + 1;
    return __resize( buffer, order );
  }
  // Buffer already within limits, or data in buffer prevents trim
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__write( vgx_OperationBuffer_t *buffer, const char *data, int64_t n ) {

  int64_t w = __sz_written( buffer );
  // Check capacity
  if( n > __sz_writable( buffer ) ) {
    // Total capacity required
    int64_t n_tot = w + n + 1; // +1 for the unusable last byte since wp can't fully catch up with rp
    // Compute order needed
    int order = imag2( n_tot );
    // Sanity check
    if( order <= imag2( buffer->capacity ) ) {
      FATAL( 0x001, "buffer capacity error" );
    }
    // Resize
    if( __resize( buffer, order ) < 0 ) {
      return -1;
    }
    // Try again
    return _operation_buffer__write( buffer, data, n );
  }
  // We have capacity to write n bytes
  else if( n > 0 ) {
    // Set protected flags only when transitioning from empty
    if( w == 0 ) {
      __set_non_empty( buffer );
    }

    const char *src = data;
    const char *end_src = data + n;

    char *dest = buffer->wp;

    // Writable segment bounded by end of buffer, or buffer is empty
    if( buffer->wp >= buffer->cp ) {
      // Two passes required.
      // (Include edge case when src will fit exactly, to handle wrapping.)
      if( n >= __sz_end_segment( buffer, buffer->wp ) ) {
        // First pass: write until end of buffer
        const char *end_buffer = __end_buffer( buffer );
        while( dest < end_buffer ) {
          *dest++ = *src++;
        }
        
        // Second pass: write remaining data at start of buffer
        // (Include edge case where zero remaining data.)
        dest = buffer->data;
      }
    }

    // Consume remaining source into buffer
    while( src < end_src ) {
      *dest++ = *src++;
    }

    // Update write pointer
    buffer->wp = dest;
  }
  // We are unwriting
  else {
    int64_t max_unwritable = __sz_readable( buffer );
    int64_t u = -n;
    if( u > max_unwritable ) {
      u = max_unwritable;
    }

    // Unwritable data in two segments.
    if( buffer->wp < buffer->rp ) {
      // Move wp and adjust unwrite amount if unwrite spans
      // both segments.
      int64_t sz_start = __sz_start_segment( buffer, buffer->wp );
      if( u > sz_start ) {
        buffer->wp = (char*)__end_buffer( buffer );
        u -= sz_start;
      }
    }

    // Unwrite
    buffer->wp -= u;

    // Clear protected flags when transitioning to empty
    if( __sz_written( buffer ) == 0 ) {
      __set_empty( buffer );
    }
  }

  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__write_crc( vgx_OperationBuffer_t *buffer, const char *data, int64_t n, unsigned int *crc ) {
  if( _operation_buffer__write( buffer, data, n ) == n ) {
    *crc = _operation_buffer__crc_tail( buffer, n, *crc );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__copy( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other ) {
  // Destination cannot be the same as source!
  if( buffer == other ) {
    return -1;
  }

  // Only copy readable data, i.e. unconfirmed data in the source is considered
  // to be confirmed by the copy operation.
  int64_t n_src = __sz_readable( other );

  // Check capacity
  if( n_src > __sz_writable( buffer ) ) {
    // Total capacity required
    int64_t n_tot = n_src + __sz_written( buffer ) + 1; // +1 for the unusable last byte since wp can't fully catch up with rp
    // Compute order needed
    int order = imag2( n_tot );
    // Sanity check
    if( order <= imag2( buffer->capacity ) ) {
      FATAL( 0x001, "buffer capacity error" );
    }
    // Resize
    if( __resize( buffer, order ) < 0 ) {
      return -1;
    }
    // Try again
    return _operation_buffer__copy( buffer, other );
  }

  // Capture read pointer before copy
  const char *restore_rp = other->rp;

  // Copy source data into destination
  const char *segment;
  int64_t n;
  // Read linear segment from source
  while( (n = _operation_buffer__readable_segment( other, LLONG_MAX, &segment, NULL )) > 0 ) {
    // Write linear segment to destination
    if( _operation_buffer__write( buffer, segment, n ) == n ) {
      _operation_buffer__advance_read( other, n );
    }
    // Error
    else {
      n_src = -1;
      break;
    }
  }

  // Restore source read pointer to original location
  other->rp = restore_rp;

  // Return amount copied
  return n_src;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__copy_crc( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc ) {
  int64_t n_src = _operation_buffer__copy( buffer, other );
  if( n_src > 0 ) {
    *crc = _operation_buffer__crc_tail( buffer, n_src, *crc );
  }
  return n_src;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__absorb( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other ) {

  // Copy source buffer into destination buffer
  int64_t n_src = _operation_buffer__copy( buffer, other );

  // Reset source if anything was copied from it into destination buffer
  if( n_src > 0 ) {
    _operation_buffer__clear( other );
  }

  // Return amount absorbed
  return n_src;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__absorb_crc( vgx_OperationBuffer_t *buffer, vgx_OperationBuffer_t *other, unsigned int *crc ) {
  int64_t n_src = _operation_buffer__absorb( buffer, other );
  if( n_src > 0 ) {
    *crc = _operation_buffer__crc_tail( buffer, n_src, *crc );
  }
  return n_src;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__unwrite( vgx_OperationBuffer_t *buffer, int64_t n ) {
  return _operation_buffer__write( buffer, NULL, -n );
}



/*******************************************************************//**
 * Return the number of bytes that can be written to buffer in a single
 * linear segment. A pointer to the start of writable data is returned
 * in pointer pointed to by 'segment'.
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__writable_segment( vgx_OperationBuffer_t *buffer, int64_t max, char **segment, char **end ) {
  int64_t n;
  // Write from here
  *segment = buffer->wp;
  // All writable data in single linear segment, return
  // size of linear segment bounded by confirmation pointer minus one.
  // Will return 0 when no writable data.
  if( buffer->wp < buffer->cp ) {
    n = buffer->cp - buffer->wp - 1; // -1 since wp can't catch up with cp (would alias with zero buffer length)
  }
  // Writable data split (or wp==cp is empty buffer), return size of linear segment
  // bounded by end of buffer, less one if cp is at start of buffer.
  else {
    n = __sz_end_segment( buffer, buffer->wp );
    // One less writable if confirmation pointer happens to be at start
    if( buffer->cp == buffer->data ) {
      --n;
    }
  }

  // Limit segment size
  if( n > max ) {
    n = max;
  }

  // Set end of segment
  if( end ) {
    *end = *segment + n;
  }

  // Return size of segment
  return n;
}



/*******************************************************************//**
 * Advance cursor for writable data
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__advance_write( vgx_OperationBuffer_t *buffer, int64_t n ) {
  if( n < 0 || n > __sz_writable( buffer ) ) {
    return -1;
  }
  
  // Writable region is split or no data written
  if( buffer->wp >= buffer->cp ) {
    // Set protected flags only when transitioning from empty
    if( buffer->wp == buffer->cp && n > 0 ) {
      __set_non_empty( buffer );
    }
    // Check if data can fit in end segment
    int64_t sz_end = __sz_end_segment( buffer, buffer->wp );
    if( n >= sz_end ) {
      // Data can't fit, wrap around
      buffer->wp = buffer->data + (n - sz_end);
      return n;
    }
  }

  // Advance
  buffer->wp += n;

  return n;
}



/*******************************************************************//**
 * Return the number of bytes that can be read from buffer in a single
 * linear segment. A pointer to the start of readable data is returned
 * in pointer pointed to by 'segment'.
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__readable_segment( vgx_OperationBuffer_t *buffer, int64_t max, const char **segment, const char **end ) {
  int64_t n;
  // Read from here
  *segment = buffer->rp;
  // All readable data in single linear segment, return
  // size of linear segment bounded by write pointer.
  // Will return 0 when no readable data.
  if( buffer->rp <= buffer->wp ) {
    n = buffer->wp - buffer->rp;
  }
  // Readable data split, return size of linear segment
  // bounded by end of buffer.
  else {
    n = __sz_end_segment( buffer, buffer->rp );
  }

  // Limit segment size
  if( n > max ) {
    n = max;
  }

  // Set end of segment
  if( end ) {
    *end = *segment + n;
  }

  // Return size of segment
  return n;
}



/*******************************************************************//**
 * Advance cursor for readable content that has been transferred to a
 * different location but not yet confirmed.
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__advance_read( vgx_OperationBuffer_t *buffer, int64_t n ) {
  if( n < 0 || n > __sz_readable( buffer ) ) {
    return -1;
  }
  
  // Readable region is split
  if( buffer->rp > buffer->wp ) {
    int64_t sz_end = __sz_end_segment( buffer, buffer->rp );
    if( n >= sz_end ) {
      buffer->rp = buffer->data + (n - sz_end);
      return n;
    }
  }

  // Advance
  buffer->rp += n;

  // All read
  if( buffer->rp == buffer->wp ) {
    __set_all_read( buffer );
  }

  return n;
}



/*******************************************************************//**
 * Confirm number of bytes that have been secured elsewhere and can be
 * safely discarded from buffer at this time.
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__confirm( vgx_OperationBuffer_t *buffer, int64_t n ) {
  // Discard amount can't be negative and can't be larger than the discardable buffer region
  if( n < 0 || n > __sz_unconfirmed( buffer ) ) {
    return -1;
  }

  // Discard region is split
  if( buffer->cp > buffer->rp ) {
    int64_t sz_end = __sz_end_segment( buffer, buffer->cp );
    if( n >= sz_end ) {
      buffer->cp = buffer->data + (n - sz_end);
      return n;
    }
  }

  // Advance
  buffer->cp += n;

  // All confirmed
  if( buffer->cp == buffer->rp ) {
    __set_all_confirmed( buffer );
  }

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer__rollback( vgx_OperationBuffer_t *buffer ) {
  int64_t pre = __sz_readable( buffer );
  if( buffer->cp != buffer->rp ) {
    buffer->rp = buffer->cp; 
    __set_non_empty( buffer );
  }
  int64_t post = __sz_readable( buffer );
  return pre - post; // negative number
}



/*******************************************************************//**
 * Delete all data and reset all internal pointers to their initial
 * positions.
 * Returns the number of bytes erased.
 ***********************************************************************
 */
static int64_t _operation_buffer__clear( vgx_OperationBuffer_t *buffer ) {
  int64_t n = _operation_buffer__size( buffer );
  char *b = buffer->data;
  buffer->wp = b;
  buffer->rp = b;
  buffer->cp = b;
  __set_empty( buffer );
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _operation_buffer__acquire( vgx_OperationBuffer_t *buffer ) {
  return __enter_buffer_CS( buffer );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _operation_buffer__release( vgx_OperationBuffer_t *buffer ) {
  return __leave_buffer_CS( buffer );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _operation_buffer__dump_state( vgx_OperationBuffer_t *buffer ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_OperationBuffer_t, buffer ) {
    CXLIB_OSTREAM( "buffer object    : %p", buffer );
    CXLIB_OSTREAM( "" );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( "Members" );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( " .data           : %p", buffer->data );
    CXLIB_OSTREAM( " .capacity       : %lld", buffer->capacity );
    CXLIB_OSTREAM( " .wp             : %p (%lld)", buffer->wp, buffer->wp - buffer->data );
    CXLIB_OSTREAM( " .rp             : %p (%lld)", buffer->rp, buffer->rp - buffer->data );
    CXLIB_OSTREAM( " .cp             : %p (%lld)", buffer->cp, buffer->cp - buffer->data );
    CXLIB_OSTREAM( " .lock_count     : %d", buffer->lock_count );
    CXLIB_OSTREAM( " .flags_CS" );
    CXLIB_OSTREAM( "   .has_readable    : %u", buffer->flags_CS.has_readable );
    CXLIB_OSTREAM( "   .has_unconfirmed : %u", buffer->flags_CS.has_unconfirmed );
    CXLIB_OSTREAM( " .CSTR__name     : %s (%p)", (buffer->CSTR__name ? CStringValue( buffer->CSTR__name ) : ""), buffer->CSTR__name );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( "" );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( "Derived" );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( "[cp, rp]         : %lld", __distance( buffer, buffer->cp, buffer->rp ) );
    CXLIB_OSTREAM( "[rp, wp]         : %lld", __distance( buffer, buffer->cp, buffer->rp ) );
    CXLIB_OSTREAM( "[wp, cp]         : %lld", __distance( buffer, buffer->cp, buffer->rp ) );
    CXLIB_OSTREAM( "writable         : %lld", __sz_writable( buffer ) );
    CXLIB_OSTREAM( "readable         : %lld", __sz_readable( buffer ) );
    CXLIB_OSTREAM( "unconfirmed      : %lld", __sz_unconfirmed( buffer ) );
    CXLIB_OSTREAM( "written          : %lld", __sz_written( buffer ) );
    CXLIB_OSTREAM( "----------------------" );
    CXLIB_OSTREAM( "" );
  } END_CXLIB_OBJ_DUMP;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationBufferPool_t *  _operation_buffer_pool__new( int64_t capacity, int order, const char *name ) {

  vgx_OperationBufferPool_t *pool = NULL;
  
  XTRY {
  
    if( (pool = calloc( 1, sizeof( vgx_OperationBufferPool_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    
    // [Q1.1]
    pool->capacity = capacity;

    // [Q1.2]
    pool->remain = capacity;

    // [Q1.6]
    if( (pool->_buffers = calloc( pool->capacity, sizeof( vgx_OperationBuffer_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    char namebuf[64] = {0};
    for( int64_t i=0; i<pool->capacity; i++ ) {
      snprintf( namebuf, 63, "%s.pool_buffer_%lld", name, i );
      if( (pool->_buffers[ i ] = iOpBuffer.New( order, namebuf )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
    }

    // [Q1.3]
    pool->next = pool->_buffers;

    // [Q1.4]
    pool->end = pool->_buffers + pool->capacity;

    // [Q1.5.1]
    pool->init_order = order;

    // [Q1.5.2]
    pool->__rsv_1_2_2 = 0;

    // [Q1.7]
    size_t sz_name = strlen( name );
    if( (pool->name = calloc( sz_name, sizeof(char)+1 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }
    strncpy( pool->name, name, sz_name );

    // Q[1.8]
    pool->__rsv_1_8 = 0;

  }
  XCATCH( errcode ) {
    iOpBufferPool.Delete( &pool );
  }
  XFINALLY {
  }

  return pool;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _operation_buffer_pool__delete( vgx_OperationBufferPool_t **pool ) {
  if( pool && *pool ) {
    vgx_OperationBufferPool_t *p = (*pool);
    

    // [Q1.7]
    if( p->name ) {
      free( p->name );
    }

    // [Q1.6]
    for( int64_t i=0; i<p->capacity; i++ ) {
      iOpBuffer.Delete( &p->_buffers[i] );
    }
    if( p->_buffers ) {
      free( p->_buffers );
    }

    free( *pool );
    *pool = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer_pool__capacity( const vgx_OperationBufferPool_t *pool ) {
  return pool->capacity;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _operation_buffer_pool__remain( const vgx_OperationBufferPool_t *pool ) {
  return pool->remain;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_OperationBuffer_t * _operation_buffer_pool__get_buffer( vgx_OperationBufferPool_t *pool ) {
  vgx_OperationBuffer_t *buffer = NULL;

  // Next buffer is available
  if( pool->next < pool->end ) {
    buffer = *pool->next;
    // Buffer checked out
    *pool->next = NULL;
    // Advance
    pool->next++;
    pool->remain--;
  }

  return buffer;
}



/*******************************************************************//**
 *
 * Returns: 1 buffer returned to pool
 *          0 buffer was null, pool unaffected
 *         -1 (error) no room in pool to return buffer into
 * 
 ***********************************************************************
 */
static int _operation_buffer_pool__return_buffer( vgx_OperationBufferPool_t *pool, vgx_OperationBuffer_t **buffer ) {
  vgx_OperationBuffer_t *b = *buffer;
  if( b ) {
    iOpBuffer.Clear( b );
    // Pool has room to return buffer
    if( pool->next > pool->_buffers ) {
      // Pool takes over ownership
      *(--pool->next) = b;
      *buffer = NULL;
      return 1;
    }
    // Cannot return buffer to pool
    return -1;
  }
  return 0;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_buffers.h"
  
test_descriptor_t _vgx_vxdurable_operation_buffers_tests[] = {
  { "VGX Graph Durable Operation Buffers Tests", __utest_vxdurable_operation_buffers },
  {NULL}
};

#endif
