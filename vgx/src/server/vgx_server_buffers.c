/*######################################################################
 *#
 *# vgx_server_buffers.c
 *#
 *#
 *######################################################################
 */

#include "_vgx.h"
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


#define ALLOC_PAD 0



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static const char * __end_buffer( const vgx_StreamBuffer_t *buffer ) {
  return buffer->data + buffer->capacity;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __sz_end_segment( const vgx_StreamBuffer_t *buffer, const char *p ) {
  return __end_buffer( buffer ) - p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __sz_start_segment( const vgx_StreamBuffer_t *buffer, const char *p ) {
  return p - buffer->data;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int64_t __distance( const vgx_StreamBuffer_t *buffer, const char *a, const char *b ) {
  if( a <= b ) {
    return b - a;
  }
  else {
    return __sz_end_segment( buffer, a ) + __sz_start_segment( buffer, b );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static bool __is_single_segment( const vgx_StreamBuffer_t *buffer ) {
  return buffer->rp <= buffer->wp;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static const char * __end_single_segment( const vgx_StreamBuffer_t *buffer ) {
  return buffer->wp;
}



/*******************************************************************//**
 * Writable is the empty space between write pointer and read pointer
 * minus one. (We can't allow wp to catch up with rp because that would
 * make the buffer size indistinguishable from zero.)
 *           
 *         wwwwwwwwwwww
 * [.......            .....]
 *         ^           ^
 *         wp          rp
 ***********************************************************************
 */
__inline static int64_t __sz_writable( const vgx_StreamBuffer_t *buffer ) {
  int64_t d = __distance( buffer, buffer->wp, buffer->rp );
  if( d == 0 ) {
    return buffer->capacity - 1;
  }
  else {
    return d - 1;
  }
}



/*******************************************************************//**
 * Readable size is the written space between read pointer and write pointer.
 * 
 * 
 *         rrrrrrrrrrrr
 * [       ............      ]
 *         ^           ^
 *         rp          wp
 ***********************************************************************
 */
__inline static int64_t __size( const vgx_StreamBuffer_t *buffer ) {
  return __distance( buffer, buffer->rp, buffer->wp );
}




static vgx_StreamBuffer_t *     _stream_buffer__new( int order );
static void                     _stream_buffer__delete( vgx_StreamBuffer_t **buffer );
static void                     _stream_buffer__set_name( vgx_StreamBuffer_t *buffer, const char *name );
static const char *             _stream_buffer__get_name( const vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__capacity( const vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__size( const vgx_StreamBuffer_t *buffer );
static bool                     _stream_buffer__empty( vgx_StreamBuffer_t *buffer );
static bool                     _stream_buffer__is_readable( const vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__writable( const vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__read_until( vgx_StreamBuffer_t *buffer, int64_t max, char **data, const char probe );
static const char *             _stream_buffer__get_linear_line( const vgx_StreamBuffer_t *buffer, int offset, int max_sz, int *rsz );
static int                      _stream_buffer__expand( vgx_StreamBuffer_t *buffer );
static int                      _stream_buffer__trim( vgx_StreamBuffer_t *buffer, int64_t max_sz );
static int64_t                  _stream_buffer__write( vgx_StreamBuffer_t *buffer, const char *data, int64_t n );
static int64_t                  _stream_buffer__write_string( vgx_StreamBuffer_t *buffer, const char *data );
static int                      _stream_buffer__term_string( vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__copy( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other );
static int64_t                  _stream_buffer__absorb( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other, int64_t n );
static void                     _stream_buffer__swap( vgx_StreamBuffer_t *A, vgx_StreamBuffer_t *B );
static int64_t                  _stream_buffer__writable_segment( vgx_StreamBuffer_t *buffer, int64_t max, char **segment, char **end );
static char *                   _stream_buffer__writable_segment_ex( vgx_StreamBuffer_t *buffer, int64_t sz, int64_t *wsz );
static int64_t                  _stream_buffer__advance_write( vgx_StreamBuffer_t *buffer, int64_t n );
static bool                     _stream_buffer__is_single_segment( const vgx_StreamBuffer_t *buffer );
static const char *             _stream_buffer__end_single_segment( const vgx_StreamBuffer_t *buffer );
static int64_t                  _stream_buffer__readable_segment( vgx_StreamBuffer_t *buffer, int64_t max, const char **segment, const char **end );
static int64_t                  _stream_buffer__advance_read( vgx_StreamBuffer_t *buffer, int64_t n );
static int64_t                  _stream_buffer__clear( vgx_StreamBuffer_t *buffer );
static void                     _stream_buffer__dump( const vgx_StreamBuffer_t *buffer );



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IStreamBuffer_t iStreamBuffer = {
  .New                = _stream_buffer__new,
  .Delete             = _stream_buffer__delete,
  .SetName            = _stream_buffer__set_name,
  .GetName            = _stream_buffer__get_name,
  .Capacity           = _stream_buffer__capacity,
  .Size               = _stream_buffer__size,
  .Empty              = _stream_buffer__empty,
  .IsReadable         = _stream_buffer__is_readable,
  .Writable           = _stream_buffer__writable,
  .ReadUntil          = _stream_buffer__read_until,
  .GetLinearLine      = _stream_buffer__get_linear_line,
  .Expand             = _stream_buffer__expand,
  .Trim               = _stream_buffer__trim,
  .Write              = _stream_buffer__write,
  .WriteString        = _stream_buffer__write_string,
  .TermString         = _stream_buffer__term_string,
  .Copy               = _stream_buffer__copy,
  .Absorb             = _stream_buffer__absorb,
  .Swap               = _stream_buffer__swap,
  .WritableSegment    = _stream_buffer__writable_segment,
  .WritableSegmentEx  = _stream_buffer__writable_segment_ex,
  .AdvanceWrite       = _stream_buffer__advance_write,
  .IsSingleSegment    = _stream_buffer__is_single_segment,
  .EndSingleSegment   = _stream_buffer__end_single_segment,
  .ReadableSegment    = _stream_buffer__readable_segment,
  .AdvanceRead        = _stream_buffer__advance_read,
  .Clear              = _stream_buffer__clear,
  .Dump               = _stream_buffer__dump
};



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __resize( vgx_StreamBuffer_t *buffer, int order ) {

  // Set new capacity
  int64_t capacity = 1LL << order;

  // The new capacity is too small to hold current data
  if( capacity < __size( buffer ) ) {
    return -1;
  }

  VERBOSE( 0x001, "Resize stream buffer '%s' @ %llp capacity %lld -> %lld", CStringValueDefault( buffer->CSTR__name, "?" ), buffer, buffer->capacity, capacity );
  char *prev_alloc = buffer->__allocated;

  // Allocate new data
  char *alloc;
  if( CALIGNED_ARRAY( alloc, char, ALLOC_PAD + capacity + ALLOC_PAD ) == NULL ) {
    return -1;
  }
  char *data = alloc + ALLOC_PAD;

  // Copy
  char *dst = data;
  const char *src = buffer->rp;
  const char *end;
  
  // Two segments
  if( buffer->rp > buffer->wp ) {
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
  int64_t r = __size( buffer );

  buffer->__allocated = alloc;
  buffer->data = data;
  buffer->capacity = capacity;
  buffer->rp = buffer->data;
  buffer->wp = (char*)buffer->rp + r;

  ALIGNED_FREE( prev_alloc );

  return 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __resize_to_fit( vgx_StreamBuffer_t *buffer, int64_t n_inc ) {
  // Don't resize if we don't need to
  if( n_inc <= __sz_writable( buffer ) ) {
    return 0;
  }
  // +1 for the unusable last byte since wp can't fully catch up with rp
  int64_t target = buffer->capacity + n_inc + 1;
  // Compute order needed
  int order = imag2( target );
  // Sanity check
  if( order <= imag2( buffer->capacity ) ) {
    FATAL( 0x001, "buffer capacity error" );
  }
  // Resize
  return __resize( buffer, order );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_StreamBuffer_t * _stream_buffer__new( int order ) {

  vgx_StreamBuffer_t *buffer = NULL;
  int64_t capacity = 1LL << order;

  // Arbitrary warning limit: > 4GB
  if( order > 32 ) {
    WARN( 0x001, "Large buffer allocation (order=%d size=%lld)", order, capacity );
  }

  XTRY {
    // Allocate object
    if( (buffer = calloc( 1, sizeof( vgx_StreamBuffer_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Initialize
    // [Q1.3]
    buffer->capacity = capacity;

    // Allocate data
    // [Q1.2]
    CALIGNED_ARRAY_THROWS( buffer->__allocated, char, ALLOC_PAD + buffer->capacity + ALLOC_PAD, 0x003 );

    // [Q1.1]
    buffer->data = buffer->__allocated + ALLOC_PAD;

    // Initialize cursors
    // [Q1.4]
    buffer->wp = buffer->data;

    // [Q1.5]
    buffer->rp = buffer->data;

    // [Q1.6]
    buffer->CSTR__name = NULL;

    // [Q1.7]
    buffer->__rsv_1_7 = 0;

    // [Q1.8]
    buffer->__rsv_1_8 = 0;

  }
  XCATCH( errcode ) {
    _stream_buffer__delete( &buffer );
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
static void _stream_buffer__delete( vgx_StreamBuffer_t **buffer ) {
  if( buffer && *buffer ) {

    if( (*buffer)->__allocated ) {
      ALIGNED_FREE( (*buffer)->__allocated );
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
static void _stream_buffer__set_name( vgx_StreamBuffer_t *buffer, const char *name ) {
  iString.Discard( &buffer->CSTR__name );
  if( name ) {
    buffer->CSTR__name = CStringNew( name );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static const char * _stream_buffer__get_name( const vgx_StreamBuffer_t *buffer ) {
  static char *noname = "";
  if( buffer->CSTR__name == NULL ) {
    return noname;
  }
  return CStringValue( buffer->CSTR__name );
}



/*******************************************************************//**
 * Return the buffer capacity
 *
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__capacity( const vgx_StreamBuffer_t *buffer ) {
  // One less than total space since wp cannot be allowed to catch up with rp (would alias with empty state)
  return buffer->capacity - 1;
}



/*******************************************************************//**
 * Return the number of bytes written
 *
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__size( const vgx_StreamBuffer_t *buffer ) {
  return __size( buffer );
}



/*******************************************************************//**
 * Return true if buffer has no readable data
 *
 *
 ***********************************************************************
 */
static bool _stream_buffer__empty( vgx_StreamBuffer_t *buffer ) {
  return buffer->rp == buffer->wp;
}



/*******************************************************************//**
 * Return true if buffer has readable data
 *
 *
 ***********************************************************************
 */
static bool _stream_buffer__is_readable( const vgx_StreamBuffer_t *buffer ) {
  return buffer->rp != buffer->wp;
}



/*******************************************************************//**
 * Return the number of bytes that can be written
 *
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__writable( const vgx_StreamBuffer_t *buffer ) {
  return __sz_writable( buffer );
}



/*******************************************************************//**
 *
 * Returns  >0 : length of data read into *data (including probe char)
 *           0 : probe not found
 *          -1 : destination buffer *data too small
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__read_until( vgx_StreamBuffer_t *buffer, int64_t max, char **data, const char probe ) {
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

  // Return length of read data string
  return dst - *data;
}



/*******************************************************************//**
 * 
 * Return a pointer to the start of a complete linear segment terminated
 * by the '\n' character found in buffer starting at 'offset' characters
 * from the start of the buffer, and return the number of characters in
 * '\n' terminated segment in 'rsz'.
 *
 * The buffer is never modified.
 * 
 * If NULL is returned 'rsz' indicates the reason:
 *   *rsz < 0 : Buffer is either non-linear or offset is too large
 *   *rsz = 0 : No '\n' terminated line found starting at offset
 *
 ***********************************************************************
 */
static const char * _stream_buffer__get_linear_line( const vgx_StreamBuffer_t *buffer, int offset, int max_sz, int *rsz ) {
  const char *line = buffer->rp + offset;

  // This function requires single-segment buffer and offset cannot be beyond end of buffer
  if( line > buffer->wp ) {
    *rsz = -1;
    return NULL;
  }

  // Set end boundary to the lesser of buffer size and max_sz
  const char *c = line;
  const char *end = buffer->wp;
  if( end - c > max_sz ) {
    end = c + max_sz;
  }

  // Look for first occurrence of '\n'
  while( c < end ) {
    // Found line
    if( *c++ == '\n' ) {
      *rsz = (int)(c - line);
      return line;
    }
  }

  // Scanned to end without finding '\n'
  *rsz = 0;
  return NULL;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _stream_buffer__expand( vgx_StreamBuffer_t *buffer ) {
  int next_order = imag2( buffer->capacity ) + 1;
  return __resize( buffer, next_order );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _stream_buffer__trim( vgx_StreamBuffer_t *buffer, int64_t max_sz ) {
  // Buffer allocation larger than desired, and data in buffer will fit in smaller allocation
  if( buffer->capacity > max_sz && __size( buffer ) < max_sz ) {
    int order = imag2( max_sz ) + 1;
    int ret = __resize( buffer, order );
    return ret;
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
static int64_t _stream_buffer__write( vgx_StreamBuffer_t *buffer, const char *data, int64_t n ) {
  // Check capacity
  if( n > __sz_writable( buffer ) ) {
    // Total capacity required
    if( __resize_to_fit( buffer, n ) < 0 ) {
      return -1;
    }
    // Try again
    return _stream_buffer__write( buffer, data, n );
  }
  // We have capacity to write n bytes
  else if( n > 0 ) {
    const char *src = data;
    const char *end_src = data + n;

    char *dest = buffer->wp;

    // Writable segment bounded by end of buffer, or buffer is empty
    if( buffer->wp >= buffer->rp ) {
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

  return n;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__write_string( vgx_StreamBuffer_t *buffer, const char *data ) {
  return _stream_buffer__write( buffer, data, strlen( data ) );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _stream_buffer__term_string( vgx_StreamBuffer_t *buffer ) {
  char TERM[] = {0};
  return (int)_stream_buffer__write( buffer, TERM, 1 );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__copy( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other ) {
  // Destination cannot be the same as source!
  if( buffer == other ) {
    return -1;
  }

  int64_t n_src = __size( other );

  // Check capacity
  if( n_src > __sz_writable( buffer ) ) {
    if( __resize_to_fit( buffer, n_src ) < 0 ) {
      return -1;
    }
    // Try again
    return _stream_buffer__copy( buffer, other );
  }

  // Capture read pointer before copy
  const char *restore_rp = other->rp;

  // Copy source data into destination
  const char *segment;
  int64_t n;
  // Read linear segment from source
  while( (n = _stream_buffer__readable_segment( other, LLONG_MAX, &segment, NULL )) > 0 ) {
    // Write linear segment to destination
    if( _stream_buffer__write( buffer, segment, n ) == n ) {
      _stream_buffer__advance_read( other, n );
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
static int64_t _stream_buffer__absorb( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other, int64_t n ) {
  // Destination cannot be the same as source!
  if( buffer == other ) {
    return -1;
  }

  // Copy up to n bytes
  int64_t n_src = __size( other );
  if( n < n_src ) {
    n_src = n;
  }

  // Check capacity
  if( n_src > __sz_writable( buffer ) ) {
    if( __resize_to_fit( buffer, n_src ) ) {
      return -1;
    }
    // Try again
    return _stream_buffer__absorb( buffer, other, n );
  }

  // Capture read pointer before we attempt to absorb
  const char *restore_rp = other->rp;

  // Copy source data into destination
  const char *segment;
  int64_t sz_segm;
  int64_t n_remain = n_src;
  // Read linear segment from source
  while( n_remain > 0 && (sz_segm = _stream_buffer__readable_segment( other, n_remain, &segment, NULL )) > 0 ) {
    // Write linear segment to destination
    if( _stream_buffer__write( buffer, segment, sz_segm ) == sz_segm ) {
      _stream_buffer__advance_read( other, sz_segm );
      n_remain -= sz_segm;
    }
    // Error
    else {
      // Restore source read pointer to original location
      other->rp = restore_rp;
      return -1;
    }
  }

  // Optimize source to a reset state if empty after absorb
  if( _stream_buffer__empty( other ) ) {
    _stream_buffer__clear( other );
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
static void _stream_buffer__swap( vgx_StreamBuffer_t *A, vgx_StreamBuffer_t *B ) {
  vgx_StreamBuffer_t tmp = *A;
  *A = *B;
  *B = tmp;
}



/*******************************************************************//**
 * Return the number of bytes that can be written to buffer in a single
 * linear segment. A pointer to the start of writable data is returned
 * in pointer pointed to by 'segment'.
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__writable_segment( vgx_StreamBuffer_t *buffer, int64_t max, char **segment, char **end ) {
  int64_t n;
  // Write from here
  *segment = buffer->wp;
  // All writable data in single linear segment, return
  // size of linear segment bounded by read pointer minus one.
  // Will return 0 when no writable data.
  if( buffer->wp < buffer->rp ) {
    n = buffer->rp - buffer->wp - 1; // -1 since wp can't catch up with rp (would alias with zero buffer length)
  }
  // Writable data split (or wp==rp is empty buffer), return size of linear segment
  // bounded by end of buffer, less one if cp is at start of buffer.
  else {
    n = __sz_end_segment( buffer, buffer->wp );
    // One less writable if read pointer happens to be at start
    if( buffer->rp == buffer->data ) {
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
 * Return a pointer to a linear segment of buffer, while ensuring that
 * the entire buffer can accommodate no less than 'sz' bytes. Return the
 * actual length of the returned writable segment in 'wsz'.
 *
 * Return NULL on error.
 ***********************************************************************
 */
static char * _stream_buffer__writable_segment_ex( vgx_StreamBuffer_t *buffer, int64_t sz, int64_t *wsz ) {

  // Ensure sufficient space
  int64_t deficit = sz - __sz_writable( buffer );
  if( deficit > 0 && __resize_to_fit( buffer, deficit ) < 0 ) {
    return NULL;
  }

  // Get a linear segment. This may be shorter than sz if buffer is split,
  // but it will never be shorter than 1.
  char *segment;
  if( (*wsz = _stream_buffer__writable_segment( buffer, sz, &segment, NULL )) < 1 ) {
    return NULL; // should never happen
  }

  return segment;
}



/*******************************************************************//**
 * Advance cursor for writable data
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__advance_write( vgx_StreamBuffer_t *buffer, int64_t n ) {
  if( n < 0 || n > __sz_writable( buffer ) ) {
    return -1;
  }
  
  // Writable region is split or no data written
  if( buffer->wp >= buffer->rp ) {
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
 * Return true if entire buffer data is contained in a single linear
 * segment, i.e. the read pointer is before the write pointer.
 *
 ***********************************************************************
 */
static bool _stream_buffer__is_single_segment( const vgx_StreamBuffer_t *buffer ) {
  return __is_single_segment( buffer );
}



/*******************************************************************//**
 * Return pointer to the character just beynod the end of readable linear segment.
 * This pointer is valid ONLY if buffer contains a single segment.
 * To ensure the validity of this pointer, make sure IsSingleSegment() returns true.
 *
 ***********************************************************************
 */
static const char * _stream_buffer__end_single_segment( const vgx_StreamBuffer_t *buffer ) {
  return __end_single_segment( buffer );
}



/*******************************************************************//**
 * Return the number of bytes that can be read from buffer in a single
 * linear segment. A pointer to the start of readable data is returned
 * in pointer pointed to by 'segment'.
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__readable_segment( vgx_StreamBuffer_t *buffer, int64_t max, const char **segment, const char **end ) {
  int64_t n;
  // Read from here
  *segment = buffer->rp;
  // All readable data in single linear segment, return
  // size of linear segment bounded by write pointer.
  // Will return 0 when no readable data.
  if( __is_single_segment( buffer ) ) {
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
 * Advance cursor for readable content
 *
 ***********************************************************************
 */
static int64_t _stream_buffer__advance_read( vgx_StreamBuffer_t *buffer, int64_t n ) {
  if( n < 0 || n > __size( buffer ) ) {
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

  return n;
}



/*******************************************************************//**
 * Delete all data and reset all internal pointers to their initial
 * positions.
 * Returns the number of bytes erased.
 ***********************************************************************
 */
static int64_t _stream_buffer__clear( vgx_StreamBuffer_t *buffer ) {
  int64_t n = _stream_buffer__size( buffer );
  char *b = buffer->data;
  buffer->wp = b;
  buffer->rp = b;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _stream_buffer__dump( const vgx_StreamBuffer_t *buffer ) {
  BEGIN_CXLIB_OBJ_DUMP( vgx_StreamBuffer_t, buffer ) {
    char *writable = NULL;
    char *end_writable = NULL;
    const char *readable = NULL;
    const char *end_readable = NULL;
    CXLIB_OSTREAM( "Public interface:" );
    CXLIB_OSTREAM( "    Capacity()        = %lld", iStreamBuffer.Capacity( buffer ) );
    CXLIB_OSTREAM( "    IsReadable()      = %s",   iStreamBuffer.IsReadable( buffer ) ? "true" : "false" );
    CXLIB_OSTREAM( "    IsSingleSegment() = %s",   iStreamBuffer.IsSingleSegment( buffer ) ? "true" : "false" );
    CXLIB_OSTREAM( "    Size()            = %lld", iStreamBuffer.Size( buffer ) );
    CXLIB_OSTREAM( "    ReadableSegment() = %lld", iStreamBuffer.ReadableSegment( (vgx_StreamBuffer_t*)buffer, LLONG_MAX, &readable, &end_readable ) );
    CXLIB_OSTREAM( "    Writable()        = %lld", iStreamBuffer.Writable( buffer ) );
    CXLIB_OSTREAM( "    WritableSegment() = %lld", iStreamBuffer.WritableSegment( (vgx_StreamBuffer_t*)buffer, LLONG_MAX, &writable, &end_writable ) );
    CXLIB_OSTREAM( "Internal data:" );
    CXLIB_OSTREAM( "    data        = @ %llp", buffer->data );
    CXLIB_OSTREAM( "    __allocated = @ %llp", buffer->__allocated );
    CXLIB_OSTREAM( "    capacity    = %lld", buffer->capacity );
    CXLIB_OSTREAM( "    wp          = @ %llp", buffer->wp );
    CXLIB_OSTREAM( "    rp          = @ %llp", buffer->rp );
  } END_CXLIB_OBJ_DUMP;
}



