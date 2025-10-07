/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    partial.c
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

#include "_vxserver_dispatcher.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_dispatcher_partial__dump_header( const x_vgx_partial__header *header, const char *fatal_message ) {
  BEGIN_CXLIB_OBJ_DUMP( x_vgx_partial__header, header ) {
    CXLIB_OSTREAM( "status        = %08X", header->status );
    CXLIB_OSTREAM( "maxhits       = %d",   header->maxhits );
    CXLIB_OSTREAM( "ktype         = %02x", header->ktype );
    CXLIB_OSTREAM( "sortspec      = %04X", header->sortspec );
    CXLIB_OSTREAM( "n_entries     = %llp", header->n_entries );
    CXLIB_OSTREAM( "segment" );
    CXLIB_OSTREAM( "    message   = %lld", header->segment.message );
    CXLIB_OSTREAM( "    keys      = %lld", header->segment.keys );
    CXLIB_OSTREAM( "    strings   = %lld", header->segment.strings );
    CXLIB_OSTREAM( "    items     = %lld", header->segment.items );
    CXLIB_OSTREAM( "    end       = %lld", header->segment.end );
    CXLIB_OSTREAM( "hitcount      = %lld", header->hitcount );
    CXLIB_OSTREAM( "message_type  = %02X", header->message_type );
    CXLIB_OSTREAM( "__rsv_2_2_2   = %u",   header->__rsv_2_2_2 );
    CXLIB_OSTREAM( "level" );
    CXLIB_OSTREAM( "    number    = %d",   header->level.number );
    CXLIB_OSTREAM( "    parts     = %d",   header->level.parts );
    CXLIB_OSTREAM( "    deep_parts= %d",   header->level.deep_parts );
    CXLIB_OSTREAM( "    incompl_p = %d",   header->level.incomplete_parts );
    CXLIB_OSTREAM( "aggregator" );
    CXLIB_OSTREAM( "    i0        = %lld", header->aggregator.int_aggr[0] );
    CXLIB_OSTREAM( "    i1        = %lld", header->aggregator.int_aggr[1] );
    CXLIB_OSTREAM( "    f2        = %g",   header->aggregator.dbl_aggr[0] );
    CXLIB_OSTREAM( "    f3        = %g",   header->aggregator.dbl_aggr[1] );
  } END_CXLIB_OBJ_DUMP;
  if( fatal_message ) {
    FATAL( 0xEEE, "%s", fatal_message );
  }
}



/*******************************************************************//**
 * __cmp_a_gt_b( a, b )
 * Evaluates to:
 *                1 : if a > b
 *                0 : if a == b
 *               -1 : if a < b
 ***********************************************************************
 */
#define __cmp_a_gt_b( a, b ) (int)( (a > b) - (a < b) )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_max_double( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  double a = k1->sortkey.dval;
  double b = k2->sortkey.dval;
  return __cmp_a_gt_b( a, b );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_min_double( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  double a = k1->sortkey.dval;
  double b = k2->sortkey.dval;
  return __cmp_a_gt_b( b, a );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_max_i64( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  int64_t a = k1->sortkey.ival;
  int64_t b = k2->sortkey.ival;
  return __cmp_a_gt_b( a, b );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_min_i64( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  int64_t a = k1->sortkey.ival;
  int64_t b = k2->sortkey.ival;
  return __cmp_a_gt_b( b, a );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_max_bytes( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  int sz_a = *(int*)k1->sortkey.ptr;
  int sz_b = *(int*)k2->sortkey.ptr;
  const char *a = (char*)k1->sortkey.ptr + sizeof(int);
  const char *b = (char*)k2->sortkey.ptr + sizeof(int);
  return memcmp( a, b, minimum_value( sz_a, sz_b ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_min_bytes( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  int sz_a = *(int*)k1->sortkey.ptr;
  int sz_b = *(int*)k2->sortkey.ptr;
  const char *a = (char*)k1->sortkey.ptr + sizeof(int);
  const char *b = (char*)k2->sortkey.ptr + sizeof(int);
  return memcmp( b, a, minimum_value( sz_a, sz_b ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __cmp_true( const x_vgx_partial__entry_key *k1, const x_vgx_partial__entry_key *k2 ) {
  return 1;
}




/**************************************************************************//**
 * __invalid_header_data
 *
 ******************************************************************************
 */
static int __invalid_header_data( const char *data, int64_t sz, x_vgx_partial__header *header ) {
  
  char buffer[512] = {0}; 
  char *p = buffer;
  const char *src = data;
  const char *end = data + (sz < 255 ? sz : 255);
  while( src < end ) {
    p = write_hex_byte( p, *src++ );
  }

  BEGIN_CXLIB_OSTREAM {
    CXLIB_OSTREAM( "raw header data:" );
    CXLIB_OSTREAM( "sz = %lld", sz );
    CXLIB_OSTREAM( "------------------------" );
    CXLIB_OSTREAM( "%s", buffer );
    CXLIB_OSTREAM( "------------------------" );
  } END_CXLIB_OSTREAM;
  
  vgx_server_dispatcher_partial__dump_header( header, NULL );

  return -1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_EXPORT int vgx_server_dispatcher_partial__deserialize_header( const char *data, int64_t sz_data, x_vgx_partial__header *dest ) {
  // Ensure enough data for header
  if( sz_data < sizeof( x_vgx_partial__header ) ) {
    return __invalid_header_data( data, sz_data, NULL );
  }
  // Populate destination from start of data
  memcpy( dest->bytes, data, sizeof( x_vgx_partial__header ) );
  // Verify valid offsets
  if( dest->segment.keys > dest->segment.strings || dest->segment.strings > dest->segment.items || dest->segment.items > dest->segment.end ) {
    return __invalid_header_data( data, sz_data, dest );
  }
  // Verify data size within bounds
  if( sz_data < dest->segment.end ) {
    return __invalid_header_data( data, sz_data, dest );
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_EXPORT void vgx_server_dispatcher_partial__reset_header( x_vgx_partial__header *header ) {
  memset( header->bytes, 0, sizeof(x_vgx_partial__header) );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_EXPORT int64_t vgx_server_dispatcher_partial__write_output_binary( const x_vgx_partial__header *header, const char *msg, int sz_msg, const x_vgx_partial__entry *entries, vgx_StreamBuffer_t *output ) {

  const x_vgx_partial__entry *entry;
  const x_vgx_partial__entry *end = entries + header->n_entries;

  // OUTPUT
  int64_t szout_pre = iStreamBuffer.Size( output );
  int64_t szout_delta = 0;

  // 1. Header
  szout_delta += iStreamBuffer.Write( output, header->bytes, sizeof( header->bytes ) );

  // 2. Message
  x_vgx_partial__binentry message;
  message.sz.val = sz_msg;
  message.data = msg;
  szout_delta += iStreamBuffer.Write( output, message.sz.bytes, sizeof( message.sz.bytes ) );
  szout_delta += iStreamBuffer.Write( output, message.data, message.sz.val );

  // 3. Entry keys
  for( entry=entries; entry < end; ++entry ) {
    szout_delta += iStreamBuffer.Write( output, (const char*)&entry->key.m128i, sizeof( entry->key ) );
  }

  // 4. Strings
  if( x_vgx_partial__is_sortkeytype_string( header->ktype ) ) {
    for( entry=entries; entry < end; ++entry ) {
      szout_delta += iStreamBuffer.Write( output, entry->sortkey.sz.bytes, sizeof( entry->sortkey.sz.bytes ) );
      szout_delta += iStreamBuffer.Write( output, entry->sortkey.data, entry->sortkey.sz.val );
    }
  }

  // 5. Items
  for( entry=entries; entry < end; ++entry ) {
    szout_delta += iStreamBuffer.Write( output, entry->item.sz.bytes, sizeof( entry->item.sz.bytes ) );
    szout_delta += iStreamBuffer.Write( output, entry->item.data, entry->item.sz.val );
  }

  if( szout_delta != iStreamBuffer.Size( output ) - szout_pre ) {
    return -1;
  }

  return szout_delta;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_EXPORT int64_t vgx_server_dispatcher_partial__serialize_partial_error( const char *msg, int64_t sz_msg, vgx_StreamBuffer_t *output ) {
  x_vgx_partial__header header = {0};
  header.message_type = X_VGX_PARTIAL_MESSAGE__UTF8;
  header.status = X_VGX_PARTIAL_STATUS__ERROR;
  header.segment.message = sizeof( x_vgx_partial__header );
  header.segment.keys = header.segment.message + sizeof(int) + sz_msg;
  header.segment.strings = header.segment.keys;
  header.segment.items = header.segment.strings;
  header.segment.end = header.segment.items;
  return vgx_server_dispatcher_partial__write_output_binary( &header, msg, (int)sz_msg, NULL, output );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __serialize( x_vgx_partial__header *header, Cm128iList_t *list, vgx_StreamBuffer_t *output, CString_t **CSTR__error ) {
  int ret = 0;

  // Count non-null list entries to determine actual size of result 
  const x_vgx_partial__entry_key * const start_list = (x_vgx_partial__entry_key*)CALLABLE( list )->Cursor( list, 0 );
  const x_vgx_partial__entry_key * const end_list = start_list + CALLABLE( list )->Length( list );
  for( const x_vgx_partial__entry_key *src = start_list; src < end_list && src->item.ptr != NULL; ++src ) {
    header->n_entries++;
  }
  const x_vgx_partial__entry_key * const end_src = start_list + header->n_entries;


#define UpdateRunningOffset( OffsetCounter, BinEntry ) (OffsetCounter) += sizeof( (BinEntry).sz.val ) + (BinEntry).sz.val

  x_vgx_partial__entry *entries = NULL;

  XTRY {

    if( (entries = calloc( header->n_entries, sizeof(x_vgx_partial__entry) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Header: Set offsets for keys and strings
    header->status = X_VGX_PARTIAL_STATUS__OK;
    header->segment.message = sizeof( x_vgx_partial__header );
    header->segment.keys = sizeof( x_vgx_partial__header ) + sizeof(int) + 0; // zero message length
    header->segment.strings = header->segment.keys + header->n_entries * sizeof( x_vgx_partial__entry_key );
    int64_t running_offset = header->segment.strings;

    bool stringsort = x_vgx_partial__is_sortkeytype_string( header->ktype );

    // Prepare sortkeys and items, and compute string offsets as needed
    const x_vgx_partial__entry_key *src = start_list;
    x_vgx_partial__entry *entry = entries;

    if( stringsort ) {
      for( ; src < end_src; ++entry, ++src ) {
        // Prepare sortkey
        SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
        entry->sortkey.sz.val = *(int*)src->sortkey.ptr;
        entry->sortkey.data = (char*)src->sortkey.ptr + sizeof(int);
        entry->key.sortkey.offset = running_offset;
        UpdateRunningOffset( running_offset, entry->sortkey );
        // Prepare item
        entry->item.sz.val = *(int*)src->item.ptr;
        entry->item.data = (char*)src->item.ptr + sizeof(int);
      }
    }
    else {
      for( ; src < end_src; ++entry, ++src ) {
        // Prepare sortkey
        entry->key.sortkey.bits = src->sortkey.bits;
        // Prepare item
        SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
        entry->item.sz.val = *(int*)src->item.ptr;
        entry->item.data = (char*)src->item.ptr + sizeof(int);
      }
    }

    // Header: we now know the end of strings, set the items offset
    header->segment.items = running_offset;

    // Compute item offsets
    const x_vgx_partial__entry *end_entries = entries + header->n_entries;
    entry = entries;
    for( ; entry < end_entries; ++entry ) {
      // Populate item offset
      entry->key.item.offset = running_offset;
      UpdateRunningOffset( running_offset, entry->item );
    }

    // Header: we now know the end of data, set the end offset
    header->segment.end = running_offset;

    // OUTPUT
    if( vgx_server_dispatcher_partial__write_output_binary( header, NULL, 0, entries, output ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
    }

  }
  XCATCH( errcode ) {
    __set_error_string_from_errcode( CSTR__error, errcode, NULL );
    ret = -1;
  }
  XFINALLY {
    if( entries ) {
      free( entries );
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_aggregation_structs( Cm128iHeap_t **heap, Cm128iList_t **list ) {
  if( *heap ) {
    COMLIB_OBJECT_DESTROY( *heap );
    *heap = NULL;
  }
  if( *list ) {
    COMLIB_OBJECT_DESTROY( *list );
    *list = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __new_aggregation_structs( const x_vgx_partial__header *header, int64_t sz, Cm128iHeap_t **heap, Cm128iList_t **list ) {
#define REP1X(X)   X
#define REP2X(X)   REP1X(X),   REP1X(X)
#define REP4X(X)   REP2X(X),   REP2X(X)
#define REP8X(X)   REP4X(X),   REP4X(X)
#define REP16X(X)  REP8X(X),   REP8X(X)
#define REP32X(X)  REP16X(X),  REP16X(X)
#define REP64X(X)  REP32X(X),  REP32X(X)
#define REP128X(X) REP64X(X),  REP64X(X)
#define REP256X(X) REP128X(X), REP128X(X)


  static char BYTES_LOW[] =  { 1, 0, 0, 0, 0 };             // [0]\0
  static char BYTES_HIGH[] = { 0, 1, 0, 0, REP256X(255) };  // [256]yyy...y   (where y=255)

  Cm128iHeap_constructor_args_t heap_args = {
    .element_capacity = sz
  };
  Cm128iList_constructor_args_t list_args = {
    .element_capacity = 0,
  };
  x_vgx_partial__entry_key nokey = {
    .item.ptr = NULL
  };

  vgx_sortspec_t sdir = _vgx_sort_direction( header->sortspec );

  switch( header->ktype ) {
  case X_VGX_PARTIAL_SORTKEYTYPE__double:
    if( sdir == VGX_SORT_DIRECTION_ASCENDING ) {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_max_double;
      nokey.sortkey.dval = DBL_MAX;
    }
    else {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_min_double;
      nokey.sortkey.dval = -DBL_MAX;
    }
    break;
  case X_VGX_PARTIAL_SORTKEYTYPE__int64:
    if( sdir == VGX_SORT_DIRECTION_ASCENDING ) {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_max_i64;
      nokey.sortkey.ival = LLONG_MAX;
    }
    else {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_min_i64;
      nokey.sortkey.ival = LLONG_MIN;
    }
    break;
  case X_VGX_PARTIAL_SORTKEYTYPE__bytes:
  case X_VGX_PARTIAL_SORTKEYTYPE__unicode:
    if( sdir == VGX_SORT_DIRECTION_ASCENDING ) {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_max_bytes;
      nokey.sortkey.ptr = BYTES_HIGH;
    }
    else {
      list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_min_bytes;
      nokey.sortkey.ptr = BYTES_LOW;
    }
    break;
  default:
    list_args.comparator = heap_args.comparator = (f_Cm128iHeap_comparator_t)__cmp_true;
  }

  int ret = 0;

  XTRY {
    if( (*heap = COMLIB_OBJECT_NEW( Cm128iHeap_t, NULL, &heap_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    CALLABLE( *heap )->Initialize( *heap, &nokey.m128i, heap_args.element_capacity );

    if( (*list = COMLIB_OBJECT_NEW( Cm128iList_t, NULL, &list_args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
  }
  XCATCH( errcode ) {
    __delete_aggregation_structs( heap, list );
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_dispatcher_partial__aggregate_partials( vgx_VGXServerClient_t *client, CString_t **CSTR__error ) {
  static int64_t HSZ = sizeof( x_vgx_partial__header );
  static int64_t KSZ = sizeof( x_vgx_partial__entry_key );

  int ret = 0;

  vgx_VGXServerResponse_t *response = &client->response;

 
  CString_t *CSTR__detail = NULL;

  x_vgx_partial__header merged_header = {0};
  merged_header.maxhits = -1;

  Cm128iHeap_t *heap = NULL;
  Cm128iList_t *list = NULL;

  XTRY {
    vgx_VGXServerDispatcherStreamSet_t *stream_set = client->dispatcher.streams;
    vgx_VGXServerRequest_t *matrix_request = client->dispatcher.streams->prequest;
    merged_header.level.parts = stream_set->responses.len;

    int64_t sum_n_entries = 0;

    int partial_start = 0;
    int partial_end = merged_header.level.parts;
    if( matrix_request->target_partial >= 0 ) {
      partial_start = matrix_request->target_partial;
      partial_end = partial_start + 1;
      if( partial_end > merged_header.level.parts ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }
    }

    int partial_level = 0;

    // Validate and determine merge parameters from partials
    for( int i=partial_start; i<partial_end; i++ ) {
      // Current partial header and response
      x_vgx_partial__header *partial_header = &stream_set->responses.headers[i];
      vgx_VGXServerResponse_t *partial_response = stream_set->responses.list[i];
      const char *data=NULL, *end=NULL;

      // Verify clean partial before merge
      switch( partial_header->status ) {
      // Expect reset state before we process this partial
      case X_VGX_PARTIAL_STATUS__RESET:
        break;
      // Partial is to be ignored
      case X_VGX_PARTIAL_STATUS__EMPTY:
        continue;
      // Invalid status
      default:
        goto not_reset;
      }

      // Verify mediatype
      if( partial_response->mediatype != MEDIA_TYPE__application_x_vgx_partial ) {
        goto invalid_mediatype;
      }

      // Skip until start of content
      if( iStreamBuffer.AdvanceRead( partial_response->buffers.content, partial_response->content_offset ) < 0 ) {
        goto truncated_data;
      }

      // Get entire partial data
      int64_t sz = iStreamBuffer.ReadableSegment( partial_response->buffers.content, LLONG_MAX, &data, &end );

      // Load header and check
      if( vgx_server_dispatcher_partial__deserialize_header( data, sz, partial_header ) < 0 ) {
        goto invalid_header;
      }

      // Verify successful partial response
      if( partial_response->status.code != HTTP_STATUS__OK ) {
        goto status_error;
      }

      // Check status
      if( partial_header->status != X_VGX_PARTIAL_STATUS__OK ) {
        goto status_error;
      }

      // Sum up hitcounts from all partitions
      merged_header.hitcount += partial_header->hitcount;

      // Determine maxhits to return from partition parameter
      if( partial_header->maxhits > merged_header.maxhits ) {
        merged_header.maxhits = partial_header->maxhits;
      }

      // Sum up actual hits produced by all partitions
      sum_n_entries += partial_header->n_entries;

      // Number of leaf node partials
      merged_header.level.deep_parts += partial_header->level.deep_parts;

      // (max) partial level
      if( partial_header->level.number > partial_level ) {
        partial_level = partial_header->level.number;
      }

      // General purpose aggregators
      x_vgx_partial__aggregator *dst = &merged_header.aggregator;
      x_vgx_partial__aggregator *src = &partial_header->aggregator;
      dst->int_aggr[0] += src->int_aggr[0];
      dst->int_aggr[1] += src->int_aggr[1];
      dst->dbl_aggr[0] += src->dbl_aggr[0];
      dst->dbl_aggr[1] += src->dbl_aggr[1];
      continue;

    not_reset:
      __format_error_string( &CSTR__detail, "partial %d was not reset", i );
      goto bad_data;

    invalid_mediatype:
      __format_error_string( &CSTR__detail, "partial %d invalid mediatype: %08X", i, partial_response->mediatype );
      goto bad_data;

    invalid_header:
      __format_error_string( &CSTR__detail, "partial %d invalid header", i );
      goto bad_data;

    truncated_data:
      __format_error_string( &CSTR__detail, "partial %d truncated data", i );
      goto bad_data;

    status_error:
      {
        CString_t *CSTR__partial_message = NULL;
        if( data ) {
          x_vgx_partial__binentry message;
          const char *p = data + partial_header->segment.message;
          memcpy( message.sz.bytes, p, sizeof(message.sz.bytes) );
          message.data = p + sizeof(message.sz.bytes);
          if( (CSTR__partial_message = iString.NewLen( NULL, message.data, message.sz.val )) != NULL ) {
            // Modify string in-place (remove " to avoid creating invalid json at a later point)
            char *c = (char*)CStringValue( CSTR__partial_message );
            end = c + CStringLength( CSTR__partial_message );
            while( c < end ) {
              if( *c == '"' ) {
                *c = ' ';
              }
              ++c;
            }
          }
        }
        vgx_HTTPStatus_t *s = &partial_response->status;
        const char *http_reason = __get_http_response_reason( s->code );
        __format_error_string( &CSTR__detail, "<partial: %d width: %d> | %d %s | %s", i, merged_header.level.parts, s->code, http_reason, CSTR__partial_message ? CStringValue( CSTR__partial_message ) : "?" );
        iString.Discard( &CSTR__partial_message );
      }
      goto bad_data;

    }

    // Set dispatcher level
    merged_header.level.number = partial_level + 1;

    // Inherit sort parameters from first partial with non-zero hits
    for( int i=partial_start; i<partial_end; i++ ) {
      x_vgx_partial__header *h = &stream_set->responses.headers[i];
      if( h->status == X_VGX_PARTIAL_STATUS__OK && h->n_entries > 0 ) {
        merged_header.ktype = h->ktype;
        merged_header.sortspec = h->sortspec;
        break;
      }
    }

    // Determine upper limit for number of aggregated hits
    int64_t limit = merged_header.maxhits < 0 ? LLONG_MAX : merged_header.maxhits;

    // Limit number of aggregated hits
    int64_t sz_merged = sum_n_entries > limit ? limit : sum_n_entries;

    // Build aggregation structs from header parameters
    if( __new_aggregation_structs( &merged_header, sz_merged, &heap, &list ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
  
    bool numericsort = x_vgx_partial__is_sortkeytype_numeric( merged_header.ktype );
    bool stringsort = x_vgx_partial__is_sortkeytype_string( merged_header.ktype );

    // Merge
    for( int i=0; i<merged_header.level.parts; i++ ) {

      // Current partial header
      x_vgx_partial__header *partial_header = &stream_set->responses.headers[i];

      // Ignore partial
      if( partial_header->status != X_VGX_PARTIAL_STATUS__OK ) {
        continue;
      }

      // Current partial response
      vgx_VGXServerResponse_t *partial_response = stream_set->responses.list[i];

      // Get partial data
      const char *data, *end;
      iStreamBuffer.ReadableSegment( partial_response->buffers.content, LLONG_MAX, &data, &end );
      // Final valid location that can hold segment size
      const char *finloc = end - 4;

      // Process partial entries
      x_vgx_partial__entry_key discarded;
      x_vgx_partial__entry_key key;
      const char *cursor = data + partial_header->segment.keys;
      const char *end_keys = data + partial_header->segment.strings;


      if( numericsort ) {
        const char *item;
        for( ; cursor < end_keys; cursor += KSZ ) {
          // Load key
          memcpy( &key.m128i, cursor, KSZ );
          // Finalize item pointer by adding item offset to start of partial data
          if( (item = data + key.item.offset) > finloc || item + *(int*)item > end ) {
            goto data_corruption;
          }
          key.item.ptr = (void*)item;
          if( CALLABLE( heap )->HeapPushTopK( heap, &key.m128i, &discarded.m128i ) == NULL ) {
            // This item and all subsequent items not sorted high enough to be incluced, done with this partition
            break;
          }
        }
      }
      else if( stringsort ) {
        const char *sortkey, *item;
        for( ; cursor < end_keys; cursor += KSZ ) {
          // Load key
          memcpy( &key.m128i, cursor, KSZ );
          // Finalize sortkey pointer
          if( (sortkey = data + key.sortkey.offset) > finloc || sortkey + *(int*)sortkey > end ) {
            goto data_corruption;
          }
          key.sortkey.ptr = (void*)sortkey;
          // Finalize item pointer by adding item offset to start of partial data
          if( (item = data + key.item.offset) > finloc || item + *(int*)item > end ) {
            goto data_corruption;
          }
          key.item.ptr = (void*)item;
          if( CALLABLE( heap )->HeapPushTopK( heap, &key.m128i, &discarded.m128i ) == NULL ) {
            // This item and all subsequent items not sorted high enough to be incluced, done with this partition
            break;
          }
        }
      }
      else {
        const char *item;
        for( ; cursor < end_keys && ComlibSequenceLength( list ) < sz_merged; cursor += KSZ ) {
          // Load key
          memcpy( &key.m128i, cursor, KSZ );
          // Finalize item pointer by adding item offset to start of partial data
          if( (item = data + key.item.offset) > finloc || item + *(int*)item > end ) {
            goto data_corruption;
          }
          key.item.ptr = (void*)item;
          CALLABLE( list )->Append( list, &key.m128i );
        }
        if( ComlibSequenceLength( list ) == sz_merged ) {
          break;
        }
      }

      continue;

    data_corruption:
      __format_error_string( &CSTR__detail, "partial %d data corruption", i );
      goto bad_data;

    }

    // Extract heap data and sort
    if( numericsort || stringsort ) {
      CALLABLE( list )->TransplantFrom( list, (Cm128iList_t*)heap );
      CALLABLE( list )->Sort( list );
    }

    // Produce new partial from merged partials
    if( __serialize( &merged_header, list, response->buffers.content, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    XBREAK;

  bad_data:
    if( CSTR__detail == NULL ) {
      THROW_ERROR( CXLIB_ERR_BUG, 0x003 );
    }
    vgx_server_dispatcher_partial__serialize_partial_error( CStringValue( CSTR__detail ), CStringLength( CSTR__detail ), response->buffers.content );
    if( CSTR__error && *CSTR__error == NULL ) {
      *CSTR__error = CSTR__detail;
      CSTR__detail = NULL;
    }
    ret = -1;

  }
  XCATCH( errcode ) {
    __set_error_string_from_errcode( CSTR__error, errcode, CSTR__detail ? CStringValue( CSTR__detail ) : NULL );
    ret = -1;
  }
  XFINALLY {
    __delete_aggregation_structs( &heap, &list );
    iString.Discard( &CSTR__detail );
  }

  return ret;
}
