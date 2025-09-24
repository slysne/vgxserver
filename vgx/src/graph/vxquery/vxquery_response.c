/*######################################################################
 *#
 *# vxquery_response.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxcollector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct __s_output_buffer {
  int64_t remain;
  char *wp;
  int64_t sz;
  char *buffer;
  bool quote;
} __output_buffer;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef char * (*__f_value_to_char_buffer)( vgx_ResponseFieldValue_t, __output_buffer *buffer );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __output_buffer * __resize_output_buffer( __output_buffer *buffer );
static char * __named_field_to_buffer( const vgx_ResponseFieldMap_t *fieldmap_cursor , vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __string_to_buffer( const char *str, int64_t max_sz, __output_buffer *buffer );
static char * __format_to_buffer( int max_sz, __output_buffer *buffer, const char *format, ... );
static char * __predicator_fields_to_buffer( const vgx_ResponseFieldData_t *entry, const vgx_ResponseFieldMap_t **cursor, __output_buffer *buffer, const CString_t *CSTR__root_anchor, int distance, bool label );
static char * __single_predicator_field_to_buffer( const vgx_ResponseFieldData_t *entry, const vgx_ResponseFieldMap_t **cursor, __output_buffer *buffer, bool label );

static char * __render_identifier( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_internalid( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_type( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_int64( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_real( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_qword_hex( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_vector( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_properties( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_descriptor( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_handle( vgx_ResponseFieldValue_t value, __output_buffer *buffer );
static char * __render_raw_vertex( vgx_ResponseFieldValue_t value, __output_buffer *buffer );

static int64_t __build_response_list( vgx_SearchResult_t *search_result );

static CTokenizer_t * __new_generic_tokenizer( void );

static framehash_cell_t * __new_internal_attribute_keymap( framehash_dynamic_t *dyn );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxquery_response__initialize( void );
static void _vxquery_response__destroy( void );
static vgx_ResponseFieldMap_t * _vxquery_response__new_fieldmap( const vgx_ResponseFieldData_t *entry, int entry_width, const vgx_ResponseFieldMap_t fieldmap_definition[] );
static int64_t _vxquery_response__build_search_result( vgx_Graph_t *self, const vgx_ResponseFields_t *fields, const vgx_ResponseFieldMap_t *fieldmap, vgx_BaseQuery_t *query );
static int64_t _vxquery_response__build_default_search_result( vgx_Graph_t *self, vgx_BaseQuery_t *query );
static void _vxquery_response__delete_search_result( vgx_SearchResult_t **search_result );
static void _vxquery_response__delete_properties( vgx_Graph_t *self, vgx_SelectProperties_t **selected_properties );
static void _vxquery_response__format_results_to_stream( vgx_Graph_t *self, vgx_BaseQuery_t *query, FILE *output );
static vgx_VertexProperty_t * _vxquery_response__select_property( vgx_Graph_t *graph, const char *name, vgx_VertexProperty_t *prop );
static vgx_Evaluator_t * _vxquery_response__parse_select_properties( vgx_Graph_t *graph, const char *select_statement, vgx_Vector_t *vector, CString_t **CSTR__error );
static char * __prepare_select_statement( const char *select_statement, CString_t **CSTR__error );

/*******************************************************************//**
 * IGrapResponse_t
 ***********************************************************************
 */

DLL_EXPORT vgx_IGraphResponse_t iGraphResponse = {
  .Initialize               = _vxquery_response__initialize,
  .Destroy                  = _vxquery_response__destroy,
  .NewFieldMap              = _vxquery_response__new_fieldmap,
  .BuildSearchResult        = _vxquery_response__build_search_result,
  .BuildDefaultSearchResult = _vxquery_response__build_default_search_result,
  .DeleteSearchResult       = _vxquery_response__delete_search_result,
  .DeleteProperties         = _vxquery_response__delete_properties,
  .FormatResultsToStream    = _vxquery_response__format_results_to_stream,
  .SelectProperty           = _vxquery_response__select_property,
  .ParseSelectProperties    = _vxquery_response__parse_select_properties
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t *g_keymap = NULL;
static framehash_dynamic_t g_keymap_dyn = {0};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t * __new_internal_attribute_keymap( framehash_dynamic_t *dyn ) {
  framehash_cell_t *keymap = NULL;
  if( (keymap = iMapping.NewIntegerMap( dyn, "internal_attribute_keymap.dyn" )) != NULL ) {
    iMapping.IntegerMapAdd( &keymap, dyn, "_id",           VGX_RESPONSE_ATTR_ID );
    iMapping.IntegerMapAdd( &keymap, dyn, "_internalid",   VGX_RESPONSE_ATTR_OBID );
    iMapping.IntegerMapAdd( &keymap, dyn, "_type",         VGX_RESPONSE_ATTR_TYPENAME );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#type",        VGX_RESPONSE_ATTR_TYPENAME | VGX_RESPONSE_ATTR_AS_ENUM );
    iMapping.IntegerMapAdd( &keymap, dyn, "_degree",       VGX_RESPONSE_ATTR_DEGREE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_indegree",     VGX_RESPONSE_ATTR_INDEGREE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_outdegree",    VGX_RESPONSE_ATTR_OUTDEGREE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_vector",       VGX_RESPONSE_ATTR_VECTOR );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#vector",      VGX_RESPONSE_ATTR_VECTOR | VGX_RESPONSE_ATTR_AS_ENUM );

    iMapping.IntegerMapAdd( &keymap, dyn, "_arcdir",       VGX_RESPONSE_ATTR_ARCDIR );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#arcdir",      VGX_RESPONSE_ATTR_ARCDIR | VGX_RESPONSE_ATTR_AS_ENUM );
    iMapping.IntegerMapAdd( &keymap, dyn, "_arcrel",       VGX_RESPONSE_ATTR_RELTYPE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#arcrel",      VGX_RESPONSE_ATTR_RELTYPE | VGX_RESPONSE_ATTR_AS_ENUM );
    iMapping.IntegerMapAdd( &keymap, dyn, "_arcmod",       VGX_RESPONSE_ATTR_MODIFIER );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#arcmod",      VGX_RESPONSE_ATTR_MODIFIER | VGX_RESPONSE_ATTR_AS_ENUM );
    iMapping.IntegerMapAdd( &keymap, dyn, "_arcval",       VGX_RESPONSE_ATTR_VALUE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_#arcval",      VGX_RESPONSE_ATTR_VALUE | VGX_RESPONSE_ATTR_AS_ENUM );

    iMapping.IntegerMapAdd( &keymap, dyn, "_rank",         VGX_RESPONSE_ATTR_RANKSCORE );

    iMapping.IntegerMapAdd( &keymap, dyn, "_tmc",          VGX_RESPONSE_ATTR_TMC );
    iMapping.IntegerMapAdd( &keymap, dyn, "_tmm",          VGX_RESPONSE_ATTR_TMM );
    iMapping.IntegerMapAdd( &keymap, dyn, "_tmx",          VGX_RESPONSE_ATTR_TMX );
    iMapping.IntegerMapAdd( &keymap, dyn, "_descriptor",   VGX_RESPONSE_ATTR_DESCRIPTOR );
    iMapping.IntegerMapAdd( &keymap, dyn, "_address",      VGX_RESPONSE_ATTR_ADDRESS );
    iMapping.IntegerMapAdd( &keymap, dyn, "_handle",       VGX_RESPONSE_ATTR_HANDLE );
    iMapping.IntegerMapAdd( &keymap, dyn, "_raw",          VGX_RESPONSE_ATTR_RAW_VERTEX );
    
    if( !(iMapping.IntegerMapSize( keymap ) > 0 ) ) {
      iMapping.DeleteIntegerMap( &keymap, dyn );
    }
  }
  return keymap;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_ResponseFieldMap_t default_fieldmap_definition[] = {
  // Anchor
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ANCHOR,     .render=(f_ResponseValueRender)__render_identifier,       .fieldname="anchor" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ANCHOR_OBID,.render=(f_ResponseValueRender)__render_internalid,       .fieldname="anchor-internalid" },
  // Predicator
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ARCDIR,     .render=(f_ResponseValueRender)NULL,                      .fieldname="direction" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RELTYPE,    .render=(f_ResponseValueRender)NULL,                      .fieldname="relationship" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_MODIFIER,   .render=(f_ResponseValueRender)NULL,                      .fieldname="modifier" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_VALUE,      .render=(f_ResponseValueRender)NULL,                      .fieldname="value" },
  // Vertex
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ID,         .render=(f_ResponseValueRender)__render_identifier,       .fieldname="id" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_OBID,       .render=(f_ResponseValueRender)__render_internalid,       .fieldname="internalid" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TYPENAME,   .render=(f_ResponseValueRender)__render_type,             .fieldname="type" },
  // Degree
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_DEGREE,     .render=(f_ResponseValueRender)__render_int64,            .fieldname="degree" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_INDEGREE,   .render=(f_ResponseValueRender)__render_int64,            .fieldname="indegree" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_OUTDEGREE,  .render=(f_ResponseValueRender)__render_int64,            .fieldname="outdegree" },
  // Properties
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_VECTOR,     .render=(f_ResponseValueRender)__render_vector,           .fieldname="vector" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_PROPERTY,   .render=(f_ResponseValueRender)__render_properties,       .fieldname="properties" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__P_RSV,     .render=(f_ResponseValueRender)__render_qword_hex,        .fieldname="RESERVED" },
  // Relevance
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RANKSCORE,  .render=(f_ResponseValueRender)__render_real,             .fieldname="rankscore" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_SIMILARITY, .render=(f_ResponseValueRender)__render_real,             .fieldname="similarity" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_HAMDIST,    .render=(f_ResponseValueRender)__render_int64,            .fieldname="hamming-distance" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__R_RSV,     .render=(f_ResponseValueRender)__render_qword_hex,        .fieldname="RESERVED" },
  // Timestamps
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMC,        .render=(f_ResponseValueRender)__render_int64,            .fieldname="created" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMM,        .render=(f_ResponseValueRender)__render_int64,            .fieldname="modified" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_TMX,        .render=(f_ResponseValueRender)__render_int64,            .fieldname="expires" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR__T_RSV,     .render=(f_ResponseValueRender)__render_qword_hex,        .fieldname="RESERVED" },
  // Details
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_DESCRIPTOR, .render=(f_ResponseValueRender)__render_descriptor,       .fieldname="descriptor" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_ADDRESS,    .render=(f_ResponseValueRender)__render_int64,            .fieldname="address" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_HANDLE,     .render=(f_ResponseValueRender)__render_handle,           .fieldname="handle" },
  { .srcpos=-1,  .attr=VGX_RESPONSE_ATTR_RAW_VERTEX, .render=(f_ResponseValueRender)__render_raw_vertex,       .fieldname="raw-vertex" },
  // END
  { .srcpos=-1,  .attr=0,                            .render=(f_ResponseValueRender)NULL,                         .fieldname=NULL }
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int _vxquery_response__initialize( void ) {
  if( !g_keymap ) {
    if( (g_keymap = __new_internal_attribute_keymap( &g_keymap_dyn )) == NULL ) { 
      return -1; // error
    }
    return 1; // initialized ok
  }
  return 0; // already initialized
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _vxquery_response__destroy( void ) {
  if( g_keymap ) {
    iMapping.DeleteIntegerMap( &g_keymap, &g_keymap_dyn );
    iFramehash.dynamic.ClearDynamic( &g_keymap_dyn );
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_ResponseFieldMap_t * _vxquery_response__new_fieldmap( const vgx_ResponseFieldData_t *entry, int entry_width, const vgx_ResponseFieldMap_t fieldmap_definition[] ) {

  vgx_ResponseFieldMap_t *fieldmap = NULL;

  XTRY {
    // Create the map of function pointers that will create Python objects from the result entry values
    if( (fieldmap = calloc( (entry_width+1), sizeof( vgx_ResponseFieldMap_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x901 );
    }

    // Add selected fields to the map with position indicator
    const vgx_ResponseFieldMap_t *definition_cursor = fieldmap_definition ? fieldmap_definition : default_fieldmap_definition;

    int dstpos = 0;
    while( definition_cursor->attr != 0 ) {
      for( int srcpos=0; srcpos<entry_width; srcpos++ ) {
        if( entry[srcpos].attr == definition_cursor->attr ) {
          fieldmap[ dstpos ] = *definition_cursor;
          fieldmap[ dstpos ].srcpos = srcpos;
          ++dstpos;
          break;
        }
      }
      ++definition_cursor;
    }

    // Terminate
    fieldmap[ entry_width ].srcpos = -1;
  }
  XCATCH( errcode ) {
    if( fieldmap ) {
      free( fieldmap );
      fieldmap = NULL;
    }
  }
  XFINALLY {
  }

  return fieldmap;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static __output_buffer * __resize_output_buffer( __output_buffer *buffer ) {
  int64_t old_sz = buffer->sz;
  int64_t increment = old_sz > 0 ? old_sz : 64;
  int64_t new_sz = old_sz + increment;
  char *old_wp = buffer->wp;
  char *old_buf = buffer->buffer;
  char *new_buf = realloc( buffer->buffer, new_sz );
  if( new_buf ) {
    buffer->buffer = new_buf;
    if( old_wp ) {
      int64_t offset = old_wp - old_buf;
      buffer->wp = buffer->buffer + offset;
    }
    else {
      buffer->wp = buffer->buffer;
    }
    buffer->remain += increment;
    buffer->sz = new_sz;
    return buffer;
  }
  else {
    return NULL;
  }
}


#define ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( BufferPtr, MinCapacity ) \
  while( (BufferPtr)->remain < (int64_t)(MinCapacity) ) {               \
    if( __resize_output_buffer( BufferPtr ) == NULL ) {                 \
      return NULL;                                                      \
    }                                                                   \
  }



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_named_field( const vgx_ResponseFieldMap_t *fieldmap_cursor , vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 )
  const char *name = fieldmap_cursor->fieldname;
  char *pre = buffer->wp;
  *buffer->wp++ = '"';
  while( (*buffer->wp = *name++) != '\0' ) {
    buffer->wp++;
  }
  *buffer->wp++ = '"';
  *buffer->wp++ = ':';
  *buffer->wp++ = ' ';
  size_t used = buffer->wp - pre;
  buffer->remain -= used;
  return fieldmap_cursor->render( value, buffer );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static char * __string_to_buffer( const char *str, int64_t max_sz, __output_buffer *buffer ) {
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, max_sz+8 ) // plus some safety padding
  char *pre = buffer->wp;
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  while( (*buffer->wp = *str++) != '\0' ) {
    buffer->wp++;
  }
  size_t used = buffer->wp - pre;
  buffer->remain -= used;
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __format_to_buffer( int max_sz, __output_buffer *buffer, const char *format, ... ) {
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, max_sz + 8LL ) // plus some safety padding
  va_list args;
  va_start( args, format );
  char *pre = buffer->wp;
  int limit = max_sz - 1;
  int fmt_sz = vsnprintf( pre, limit, format, args );
  va_end( args );
  if( fmt_sz < 0 ) {
    while( pre < buffer->wp + limit ) {
      *pre++ = '?';
    }
  }
  else {
    int used = minimum_value( fmt_sz, limit );
    buffer->wp += used;
    buffer->remain -= used;
  }
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static char * __render_identifier( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  if( value.ident != NULL ) {
    CString_t *CSTR__id = value.ident->identifier.CSTR__idstr;
    if( CSTR__id ) {
      return __string_to_buffer( CStringValue( CSTR__id ), CStringLength( CSTR__id ), buffer );
    }
    else {
      size_t sz = strlen( value.ident->identifier.idprefix.data );
      return __string_to_buffer( value.ident->identifier.idprefix.data, sz, buffer );
    }
  }
  else {
    return __string_to_buffer( "*", 1, buffer );
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_internalid( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 40 ) // with some padding
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  idtostr( buffer->wp, &value.ident->internalid );
  buffer->wp += 32;
  buffer->remain -= 32;
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_type( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  return __string_to_buffer( CStringValue( value.CSTR__str ), CStringLength( value.CSTR__str ), buffer );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_int64( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  return __format_to_buffer( 64, buffer, "%lld", value.i64 );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_real( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  return __format_to_buffer( 64, buffer, "%#g", value.real );
}


/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_qword_hex( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  if( buffer->quote ) {
    return __format_to_buffer( 64, buffer, "\"0x%016llX\"", value.bits );
  }
  else {
    return __format_to_buffer( 64, buffer, "0x%016llX", value.bits );
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_vector( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  vgx_Vector_t *vector = (vgx_Vector_t*)value.vector;
  if( vector ) {
    int required = vector->metas.vlen * 64; // (over)estimate characters needed to render the vector
    ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, required + 8LL ) // with some safety padding
    char *pre = buffer->wp;
    CALLABLE( value.vector )->ToBuffer( value.vector, (int)buffer->remain, &buffer->wp );
    buffer->wp--; // erase the nul terminator so we can continue filling the buffer
    int used = (int)(buffer->wp - pre);
    buffer->remain -= used;
    return buffer->buffer;
  }
  else {
    bool quote = buffer->quote;
    buffer->quote = false;
    char *p = __string_to_buffer( "[]", 2, buffer );
    buffer->quote = quote;
    return p;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_properties( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  static const char str_true[] = "true";
  static const char str_nan[] = "nan";
  static const char str_null[] = "null";

  char PALIGNED_ decomp_buffer[ARCH_PAGE_SIZE];

  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 )

  *buffer->wp++ = '{';
  buffer->remain--;

  vgx_ExpressEvalStack_t *stack = value.eval_properties;
  if( stack ) {
    int used;
    vgx_EvalStackItem_t *cursor = stack->data + 1; // Skip first dummy item
    int n_items = 0;
    while( !EvalStackItemIsTerminator( cursor ) ) {

      // Ignore empty slot
      if( cursor->type != STACK_ITEM_TYPE_CSTRING ) {
        ++cursor;
        ++cursor;
        continue;
      }

      // Write separator
      if( n_items++ > 0 ) {
        *buffer->wp++ = ',';
        buffer->remain--;
      }

      // Write 'key':
      const char *key = CStringValue( cursor->CSTR__str );
      int32_t maxkeylen = CStringLength( cursor->CSTR__str );
      ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, maxkeylen + 8LL ) // with safety padding
      *buffer->wp++ = '"';
      while( (*buffer->wp = *key++) != '\0' ) {
        buffer->wp++;
        buffer->remain--;
      }
      *buffer->wp++ = '"';
      *buffer->wp++ = ':';
      buffer->remain -= 3; // "": 

      // Value is next
      ++cursor;

      // Write value
      char *decompressed = NULL;
      const char *strval = NULL;
      int32_t maxlen = 0;
      CString_attr attr;
      CString_attr aattr;
      switch( cursor->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 64 ) // with safety padding
        used = sprintf( buffer->wp, "%lld", cursor->integer );
        buffer->wp += used;
        buffer->remain -= used;
        break;
      case STACK_ITEM_TYPE_REAL:
        if( cursor->real < INFINITY ) {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 64 ) // with safety padding
          used = sprintf( buffer->wp, "%#g", cursor->real );
          buffer->wp += used;
          buffer->remain -= used;
          break;
        }
      case STACK_ITEM_TYPE_NAN:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 ) // with safety padding
        used = sprintf( buffer->wp, "%s", str_nan );
        buffer->wp += used;
        buffer->remain -= used;
        break;
      case STACK_ITEM_TYPE_NONE:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 ) // with safety padding
        used = sprintf( buffer->wp, "%s", str_null );
        buffer->wp += used;
        buffer->remain -= used;
        break;
      case STACK_ITEM_TYPE_VERTEX:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 48 ) // with safety padding
        idtostr( buffer->wp, __vertex_internalid( cursor->vertex ) );
        buffer->wp += 32;
        buffer->remain -= 32;
        break;
      case STACK_ITEM_TYPE_VERTEXID:
        if( cursor->vertexid->CSTR__idstr == NULL ) {
          strval = cursor->vertexid->idprefix.data;
          maxlen = sizeof( vgx_VertexIdentifierPrefix_t ); // worst case
        }
        else {
          strval = CStringValue( cursor->vertexid->CSTR__idstr );
          maxlen = CStringLength( cursor->vertexid->CSTR__idstr );
        }
        /* FALLTHRU */
      case STACK_ITEM_TYPE_CSTRING:
        if( strval == NULL ) {
          attr = CStringAttributes( cursor->CSTR__str );
          if( (attr & CSTRING_ATTR_COMPRESSED) ) {
            int32_t _ign;
            if( CALLABLE( cursor->CSTR__str )->DecompressToBytes( cursor->CSTR__str, decomp_buffer, ARCH_PAGE_SIZE, &decompressed, &maxlen, &_ign, &attr ) < 0 ) {
#define decompress_error "<decompress_error>"
              strval = decompress_error;
              maxlen = sizeof( decompress_error );
            }
            else {
              strval = decompressed;
            }
          }
          else {
            strval = CStringValue( cursor->CSTR__str );
            maxlen = CStringLength( cursor->CSTR__str );
          }
        }
        else {
          attr = CSTRING_ATTR_NONE;
        }
        if( attr == CSTRING_ATTR_NONE ) {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, maxlen + 16LL ) // with safety padding
          *buffer->wp++ = '"';
          while( (*buffer->wp = *strval++) != '\0' ) {
            buffer->wp++;
            buffer->remain--;
          }
          *buffer->wp++ = '"';
          buffer->remain -= 2; // "" 
        }
        else if( (attr & CSTRING_ATTR_BYTES) && maxlen > 0 ) {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 4LL*maxlen + 16LL ) // with safety padding
          *buffer->wp++ = '"';
          int64_t errpos;
          if( (used = (int)COMLIB_copy_utf8( (const BYTE*)strval, buffer->wp, &errpos )) < 0 ) {
            used =  bytes_to_escaped_hex( &buffer->wp, (unsigned char*)strval, maxlen ); // never -1 since buffer is already allocated
          }
          buffer->wp += used;
          *buffer->wp++ = '"';
          buffer->remain -= used + 2LL;
        }
        else if( (attr & CSTRING_ATTR_BYTEARRAY) && maxlen > 0 ) {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 4LL*maxlen + 16LL ) // with safety padding
          *buffer->wp++ = '"';
          used =  bytes_to_escaped_hex( &buffer->wp, (unsigned char*)strval, maxlen ); // never -1 since buffer is already allocated
          buffer->wp += used;
          *buffer->wp++ = '"';
          buffer->remain -= used + 2LL;
        }
        else if( (aattr = attr & __CSTRING_ATTR_ARRAY_MASK) != 0 ) {
          int64_t nqw = VGX_CSTRING_ARRAY_LENGTH( cursor->CSTR__str );
          if( aattr == CSTRING_ATTR_ARRAY_INT || aattr == CSTRING_ATTR_ARRAY_FLOAT ) { 
            if( nqw > 0 ) {
              ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, nqw*32 ) // with safety padding
              QWORD *qw = (QWORD*)strval;
              QWORD *qw_end = qw + nqw;
              *buffer->wp++ = '[';
              if( aattr == CSTRING_ATTR_ARRAY_INT ) {
                while( qw < qw_end ) {
                  used = sprintf( buffer->wp, "%lld, ", *(int64_t*)qw++ );
                  buffer->wp += used;
                  buffer->remain -= used;
                }
              }
              else {
                while( qw < qw_end ) {
                  used = sprintf( buffer->wp, "%g, ", *(double*)qw++ );
                  buffer->wp += used;
                  buffer->remain -= used;
                }
              }
              buffer->wp -= 2; // erase last ", "
              *buffer->wp++ = ']';
              // note: remain +2 for the erased ", " and -2 for the leading [ and trailing ] ==> remain -= 0
            }
            else {
              ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 8 )
              *buffer->wp++ = '[';
              *buffer->wp++ = ']';
              buffer->remain -= 2;
            }
          }
          else if( aattr == CSTRING_ATTR_ARRAY_MAP ) {
            if( nqw > 0 ) {
              ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, nqw*40 ) // with safety padding
              *buffer->wp++ = '{';
              vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)strval;
              if( header->items > 0 ) {
                vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)strval + 1;
                vgx_cstring_array_map_cell_t *last = cell + header->mask;
                while( cell <= last ) {
                  if( cell->key != VGX_CSTRING_ARRAY_MAP_KEY_NONE ) {
                    used = sprintf( buffer->wp, "\"%d\": %g, ", cell->key, cell->value );
                    buffer->wp += used;
                    buffer->remain -= used;
                  }
                  ++cell;
                }
                buffer->wp -= 2; // erase last ", "
                *buffer->wp++ = '}';
                // note: remain +2 for the erased ", " and -2 for the leading { and trailing } ==> remain -= 0
              }
              else {
                *buffer->wp++ = '}';
                buffer->remain -= 2;
              }
            }
            else {
              ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 8 )
              *buffer->wp++ = '{';
              *buffer->wp++ = '}';
              buffer->remain -= 2;
            }
          }
        }
        else if( attr & CSTRING_ATTR_CALLABLE ) {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, maxlen + 16LL ) // with safety padding
          static const char func[] = "\"<function>\"";
          static const size_t z = sizeof( func )-1;
          strcpy( buffer->wp, func );
          buffer->wp += z;
          buffer->remain -= z;
        }
        else {
          ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, maxlen + 16LL ) // with safety padding
          static const char s[] = "\"<unknown>\"";
          static const size_t z = sizeof( s )-1;
          strcpy( buffer->wp, s );
          buffer->wp += z;
          buffer->remain -= z;
        }
        break;
      case STACK_ITEM_TYPE_VECTOR:
        {
          vgx_ResponseFieldValue_t rv = { .vector = cursor->vector };
          __render_vector( rv, buffer );
        }
        break;
      case STACK_ITEM_TYPE_BITVECTOR:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 64 ) // with safety padding
        used = sprintf( buffer->wp, "0x%016llx", cursor->bits );
        buffer->wp += used;
        buffer->remain -= used;
        break;
      case STACK_ITEM_TYPE_KEYVAL:
        ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 40 ) // with safety padding
        used = sprintf( buffer->wp, "[%d, %g]", vgx_cstring_array_map_key( &cursor->bits ), vgx_cstring_array_map_val( &cursor->bits ) );
        buffer->wp += used;
        buffer->remain -= used;
        break;
      case STACK_ITEM_TYPE_SET:
      case STACK_ITEM_TYPE_RANGE:
      case STACK_ITEM_TYPE_INIT:
      default:
        *buffer->wp++ = '"';
        *buffer->wp++ = '?';
        *buffer->wp++ = '"';
        buffer->remain -= 3; // "?"
      }

      if( decompressed && decompressed != decomp_buffer ) {
        ALIGNED_FREE( decompressed );
        decompressed = NULL;
      }

      // Next key/val pair
      cursor++;
    }
  }

  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 16 ) // with safety padding

  *buffer->wp++ = '}';
  buffer->remain--;
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_descriptor( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  // worst case 36 chars
  // desc=[127 0xff 0xff 0xff 0xffffffff]
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 64 )
  vgx_VertexDescriptor_t desc = { .bits = value.bits };

  int8_t count = desc.semaphore.count;
  uint8_t type = desc.type.enumeration;
  uint8_t state = desc.state.bits;
  uint8_t prop = desc.property.bits;
  unsigned long threadid = desc.writer.threadid;
  int used;
  if( buffer->quote ) {
    used = sprintf( buffer->wp, "\"%3d 0x%02x 0x%02x 0x%02x 0x%08lx\"", count, type, state, prop, threadid ); // used does not include nul-term
  }
  else {
    used = sprintf( buffer->wp, "%3d 0x%02x 0x%02x 0x%02x 0x%08lx", count, type, state, prop, threadid ); // used does not include nul-term
  }
  buffer->wp += used;
  buffer->remain -= used;
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_handle( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {
  cxmalloc_handle_t handle = {0};
  handle.qword = value.bits;
  unsigned aidx = handle.aidx;
  unsigned bidx = handle.bidx;
  unsigned offset = handle.offset;
  unsigned objclass = handle.objclass;

  // worst case: 27 chars
  // handle=65535:65535:16777216
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 64 )
  int used;
  if( buffer->quote ) {
    used = sprintf( buffer->wp, "\"%02X:%u:%u:%u\"", objclass, aidx, bidx, offset ); // used does not include nul-term
  }
  else {
    used = sprintf( buffer->wp, "%02X:%u:%u:%u", objclass, aidx, bidx, offset ); // used does not include nul-term
  }
  buffer->wp += used;
  buffer->remain -= used;
  return buffer->buffer;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __render_raw_vertex( vgx_ResponseFieldValue_t value, __output_buffer *buffer ) {

  QWORD *vtxqw = (QWORD*)_cxmalloc_linehead_from_object( value.vertex );
  int nqwords = (int)qwsizeof( vgx_AllocatedVertex_t );
  int size = 20*nqwords + 16; // add some extra padding
  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, size )
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  for( int i=0; i<nqwords; i++ ) {
    int used = sprintf( buffer->wp, "<0x%016llX>", *vtxqw++ );
    buffer->wp += used;
    buffer->remain -= used;
  }
  if( buffer->quote ) {
    *buffer->wp++ = '"';
    buffer->remain--;
  }
  return buffer->buffer; 
}


#define PUT_CHAR_TO_BUFFER( Char )  \
  {                                 \
    *buffer->wp++ = Char;           \
    buffer->remain--;               \
  }

#define ENSURE_ARC_START                                        \
  if( arc_start ) {                                             \
    if( __string_to_buffer( arc_start, 4, buffer ) == NULL ) {  \
      return NULL;                                              \
    }                                                           \
    PUT_CHAR_TO_BUFFER( ' ' )                                   \
    arc_start = NULL;                                           \
  }

#define PUT_ARC_END                                             \
  {                                                             \
    if( __string_to_buffer( arc_end, 4, buffer ) == NULL ) {    \
      return NULL;                                              \
    }                                                           \
  }

#define PUT_FORMAT_TO_BUFFER( Format, Value )                                   \
  if( (used = sprintf( buffer->wp, Format, Value )) > 0 ) {                     \
    buffer->wp += used;                                                         \
    buffer->remain -= used;                                                     \
  }



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static char * __predicator_fields_to_buffer( const vgx_ResponseFieldData_t *entry, const vgx_ResponseFieldMap_t **cursor, __output_buffer *buffer, const CString_t *CSTR__root_anchor, int distance, bool label ) {
  // -[ score <M_ACC> 12.34 ]->
  static const char dirout_start[]  = "-[";
  static const char dirout_end[]    = "]->";
  static const char dirin_start[]   = "<-[";
  static const char dirin_end[]     = "]-";

  const char *arc_start = dirout_start;
  const char *arc_end = dirin_end;
  int len_arc_start = 2;
  int len_arc_end = 2;

  const vgx_ResponseFieldData_t *field;

  if( label ) {
    ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 8 )
    static const char *name = "\"arc\": \"";
    static const int n = sizeof(name);
    memcpy( buffer->wp, name, n );
    buffer->wp += sizeof(name);
    buffer->remain -= sizeof(name);
  }

  // Temporarily turn off string quoting
  bool savedquote = buffer->quote;
  buffer->quote = false;

  if( distance > 1 ) {
    __format_to_buffer( CStringLength( CSTR__root_anchor ) + 16, buffer, "( %s )", CStringValue( CSTR__root_anchor ) );
    for( int d=1; d<distance-1; d++ ) {
      __format_to_buffer( 16, buffer, "-[...]-(%d)", d );
    }
    __string_to_buffer( "-[...]-", 8, buffer );
  }

  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 8 )
  PUT_CHAR_TO_BUFFER( '(' )
  PUT_CHAR_TO_BUFFER( ' ' )
  if( (*cursor)->attr == VGX_RESPONSE_ATTR_ANCHOR ) {
    field = entry + (*cursor)->srcpos;
    __render_identifier( field->value, buffer );
    (*cursor)++;
  }
  PUT_CHAR_TO_BUFFER( ' ' )
  PUT_CHAR_TO_BUFFER( ')' )


  int used;
  while( (*cursor)->srcpos != -1 && ((*cursor)->attr & VGX_RESPONSE_ATTRS_PREDICATOR) ) {
    field = entry + (*cursor)->srcpos;
    switch( field->attr ) {
    // DIRECTION
    case VGX_RESPONSE_ATTR_ARCDIR:
      switch( field->value.pred.rel.dir ) {
      case VGX_ARCDIR_OUT:
        arc_end = dirout_end;
        len_arc_end = 3;
        break;
      case VGX_ARCDIR_IN:
        arc_start = dirin_start;
        len_arc_start = 3;
        break;
      default:
        break;
      }
      break;

    // RELATIONSHIP
    case VGX_RESPONSE_ATTR_RELTYPE:
      ENSURE_ARC_START
      if( __string_to_buffer( CStringValue( field->value.CSTR__str ), CStringLength( field->value.CSTR__str ) + 1LL, buffer ) == NULL ) {
        return NULL;
      }
      PUT_CHAR_TO_BUFFER( ' ' )
      break;

    // MODIFIER
    case VGX_RESPONSE_ATTR_MODIFIER:
      ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 )
      ENSURE_ARC_START
      PUT_CHAR_TO_BUFFER( '<' )
      if( __string_to_buffer( _vgx_modifier_as_string( field->value.pred.mod ), 16, buffer ) == NULL ) {
        return NULL;
      }
      PUT_CHAR_TO_BUFFER( '>' )
      PUT_CHAR_TO_BUFFER( ' ' )
      break;
    
    // VALUE
    case VGX_RESPONSE_ATTR_VALUE:
      ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 )
      ENSURE_ARC_START
      switch( _vgx_predicator_value_range( NULL, NULL, field->value.pred.mod.bits ) ) {
      case VGX_PREDICATOR_VAL_TYPE_UNITY:
        PUT_CHAR_TO_BUFFER( '1' )
        PUT_CHAR_TO_BUFFER( ' ' )
        break;
      case VGX_PREDICATOR_VAL_TYPE_INTEGER:
        PUT_FORMAT_TO_BUFFER( "%d ", field->value.pred.val.integer )
        break;
      case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
        PUT_FORMAT_TO_BUFFER( "%u ", field->value.pred.val.uinteger )
        break;
      case VGX_PREDICATOR_VAL_TYPE_REAL:
        PUT_FORMAT_TO_BUFFER( "%#g ", field->value.pred.val.real )
        break;
      default:
        PUT_CHAR_TO_BUFFER( ' ' )
      }
      break;

    default:
      break;
    }

    (*cursor)++;

  }

  ENSURE_ARC_START
  PUT_ARC_END

  PUT_CHAR_TO_BUFFER( '(' )
  PUT_CHAR_TO_BUFFER( ' ' );
  if( (*cursor)->srcpos != -1 && (*cursor)->attr == VGX_RESPONSE_ATTR_ID ) {
    field = entry + (*cursor)->srcpos;
    __render_identifier( field->value, buffer );
    (*cursor)++;
  }
  PUT_CHAR_TO_BUFFER( ' ' );
  PUT_CHAR_TO_BUFFER( ')' )
  if( label ) {
    PUT_CHAR_TO_BUFFER( '"' )
  }
  PUT_CHAR_TO_BUFFER( ',' );
  PUT_CHAR_TO_BUFFER( ' ' );

  buffer->quote = savedquote;

  return buffer->buffer;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __single_predicator_field_to_buffer( const vgx_ResponseFieldData_t *entry, const vgx_ResponseFieldMap_t **cursor, __output_buffer *buffer, bool label ) {
  static const char *dir = "\"dir\": \"";
  static const char *rel = "\"rel\": \"";
  static const char *mod = "\"mod\": \"";
  static const char *val = "\"val\": \"";
  const int llen = 8;

  ENSURE_BUFFER_CAPACITY_OR_RETURN_NULL( buffer, 32 )

  bool savedquote = buffer->quote;
  buffer->quote = false;

#define PUT_LABEL( L )              \
  if( label ) {                     \
    memcpy( buffer->wp, L, llen );  \
    buffer->wp += llen;             \
    buffer->remain -= llen;         \
  }

  int used;
  const vgx_ResponseFieldData_t *field = entry + (*cursor)++->srcpos;
  switch( field->attr ) {
  // DIRECTION
  case VGX_RESPONSE_ATTR_ARCDIR:
    PUT_LABEL( dir )
    switch( field->value.pred.rel.dir ) {
    case VGX_ARCDIR_OUT:
      PUT_CHAR_TO_BUFFER( '-' )
      PUT_CHAR_TO_BUFFER( '>' )
      break;
    case VGX_ARCDIR_IN:
      PUT_CHAR_TO_BUFFER( '<' )
      PUT_CHAR_TO_BUFFER( '-' )
      break;
    default:
      break;
    }
    break;

  // RELATIONSHIP
  case VGX_RESPONSE_ATTR_RELTYPE:
    PUT_LABEL( rel )
    if( __string_to_buffer( CStringValue( field->value.CSTR__str ), CStringLength( field->value.CSTR__str ) + 1LL, buffer ) == NULL ) {
      return NULL;
    }
    break;

  // MODIFIER
  case VGX_RESPONSE_ATTR_MODIFIER:
    PUT_LABEL( mod )
    if( __string_to_buffer( _vgx_modifier_as_string( field->value.pred.mod ), 16, buffer ) == NULL ) {
      return NULL;
    }
    break;
  
  // VALUE
  case VGX_RESPONSE_ATTR_VALUE:
    PUT_LABEL( val )
    switch( _vgx_predicator_value_range( NULL, NULL, field->value.pred.mod.bits ) ) {
    case VGX_PREDICATOR_VAL_TYPE_UNITY:
      PUT_CHAR_TO_BUFFER( '1' )
      break;
    case VGX_PREDICATOR_VAL_TYPE_INTEGER:
      PUT_FORMAT_TO_BUFFER( "%d", field->value.pred.val.integer )
      break;
    case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
      PUT_FORMAT_TO_BUFFER( "%u", field->value.pred.val.uinteger )
      break;
    case VGX_PREDICATOR_VAL_TYPE_REAL:
      PUT_FORMAT_TO_BUFFER( "%#g", field->value.pred.val.real )
      break;
    default:
      break;
    }
    break;

  default:
    break;
  }

  if( label ) {
    PUT_CHAR_TO_BUFFER( '"' )
    PUT_CHAR_TO_BUFFER( ',' )
    PUT_CHAR_TO_BUFFER( ' ' )
  }

  buffer->quote = savedquote;

  return buffer->buffer;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef VGX_CONSISTENCY_CHECK

__inline static void __assert_safe_tail( vgx_Graph_t *graph, vgx_CollectorItem_t *item ) {
  GRAPH_LOCK( graph ) {
    if( !_vgx_is_readonly_CS( &graph->readonly ) ) {
      uint32_t tid = GET_CURRENT_THREAD_ID();
      if( item->tailref->slot.locked < 1 || !__vertex_is_locked_safe_for_thread_CS( item->tailref->vertex, tid ) ) {
        FATAL( 0x001, "Deref tail vertex not locked" );
      }
    }
  } GRAPH_RELEASE;
}

__inline static void __assert_safe_head( vgx_Graph_t *graph, vgx_CollectorItem_t *item ) {
  GRAPH_LOCK( graph ) {
    if( !_vgx_is_readonly_CS( &graph->readonly ) ) {
      uint32_t tid = GET_CURRENT_THREAD_ID();
      if( item->headref->slot.locked < 1 || !__vertex_is_locked_safe_for_thread_CS( item->headref->vertex, tid ) ) {
        FATAL( 0x002, "Deref head vertex not locked" );
      }
    }
  } GRAPH_RELEASE;
}

#else
#define __assert_safe_tail( graph, item ) ((void)0)
#define __assert_safe_head( graph, item ) ((void)0)
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __set_identifier( vgx_Graph_t *graph, vgx_VertexCompleteIdentifier_t *vid, vgx_Vertex_t *vertex ) {
  if( (vid->identifier = vertex->identifier).CSTR__idstr ) {
    // Replace long id with an ephemeral clone
    vid->identifier.CSTR__idstr = CStringCloneAlloc( vertex->identifier.CSTR__idstr, graph->ephemeral_string_allocator_context );
  }
  // Internalid
  idcpy( &vid->internalid, COMLIB_OBJECT_GETID( vertex ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __build_response_list( vgx_SearchResult_t *search_result ) {

  if( search_result == NULL || search_result->query == NULL ) {
    return -1;
  }

  vgx_Graph_t *graph = search_result->graph;

  const CString_t *CSTR__root_anchor = NULL;

  // Raw search result
  vgx_BaseCollector_context_t *collector = NULL;
  
  switch( search_result->query->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    collector = ((vgx_NeighborhoodQuery_t*)search_result->query)->collector;
    CSTR__root_anchor = ((vgx_NeighborhoodQuery_t*)search_result->query)->CSTR__anchor_id;
    search_result->total_neighbors = ((vgx_NeighborhoodQuery_t*)search_result->query)->n_neighbors;
    search_result->total_arcs = ((vgx_NeighborhoodQuery_t*)search_result->query)->n_arcs;
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    collector = ((vgx_GlobalQuery_t*)search_result->query)->collector;
    CSTR__root_anchor = ((vgx_GlobalQuery_t*)search_result->query)->CSTR__vertex_id;
    switch( ((vgx_GlobalQuery_t*)search_result->query)->collector_mode ) {
    case VGX_COLLECTOR_MODE_COLLECT_ARCS:
      search_result->total_arcs = ((vgx_GlobalQuery_t*)search_result->query)->n_items;
      break;
    case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
      search_result->total_vertices = ((vgx_GlobalQuery_t*)search_result->query)->n_items;
      // Turn off any fields that are not supported in global search mode
      search_result->list_fields.fastmask &= ~VGX_RESPONSE_ATTRS_ANCHOR;
      search_result->list_fields.fastmask &= ~VGX_RESPONSE_ATTRS_PREDICATOR;
      break;
    default:
      break;
    }
    break;
  default:
    break;
  }

  f_vgx_RankScoreFromItem getrank;
  if( search_result->query->ranking_condition ) {
    getrank = __get_rank_score_from_item_function( search_result->query->ranking_condition->sortspec );
  }
  else {
    getrank = __get_rank_score_from_item_function( VGX_SORTBY_NONE ); 
  }
 
  if( collector == NULL ) {
    return 0; // zero hits
  }
  Cm256iList_vtable_t *iList = CALLABLE( collector->container.sequence.list );
  if( (search_result->list_length = iList->Length( collector->container.sequence.list )) == 0 ) {
    return 0; // zero hits
  }
  vgx_CollectorItem_t *cursor = (vgx_CollectorItem_t*)iList->Cursor( collector->container.sequence.list, 0 );
  vgx_CollectorItem_t *end = cursor + search_result->list_length;
  int64_t n_items = 0; // actual number of items should match list length, unless invalid vertices encountered

  const vgx_ResponseAttrFastMask response_fastmask = vgx_response_attrs( search_result->list_fields.fastmask );

  // Support for single string entry output
  int is_string_entry = vgx_response_show_as_string( search_result->list_fields.fastmask );
  int single_pred_element = POPCNT32( response_fastmask ) == 1 && ( VGX_RESPONSE_ATTRS_PREDICATOR & response_fastmask );
  CString_constructor_args_t string_entry_args = {0};
  string_entry_args.alloc = graph->ephemeral_string_allocator_context;
  CString_constructor_args_t ovsz_string_entry_args = {0};

  // JSON string output if multiple fields and at least one of those outside the arc fields
  int emulate_json = POPCNT32( response_fastmask ) > 1 && (POPCNT32( response_fastmask & ~VGX_RESPONSE_ATTRS_ANCHORED_ARC ) > 0 );

  __output_buffer output_buffer = {0};
  output_buffer.quote = emulate_json ? true : false;
  
#define MAX_FIELDBUF_WIDTH 28 // all 28 (potential) bit slots of vgx_ResponseAttrFastMask enum
  vgx_ResponseFieldData_t field_buffer[ MAX_FIELDBUF_WIDTH ];
  vgx_ResponseFieldMap_t *render_as_string_fieldmap = NULL;

  // Compute the width of the data array
  int data_width = (int)POPCNT32( response_fastmask & VGX_RESPONSE_ATTRS_MASK ); // fastmask bits

  if( data_width == 0 ) {
    return 0;
  }

  if( is_string_entry ) {
    search_result->list_width = 1;
    int attr_pattern = 1;
    int fx = 0;
    for( int i=0; i < MAX_FIELDBUF_WIDTH; i++ ) {
      if( attr_pattern & response_fastmask ) {
        field_buffer[ fx++ ].attr = (vgx_ResponseAttrFastMask)attr_pattern;
      }
      attr_pattern <<= 1;
    }
    // Initialize buffer
    if( __resize_output_buffer( &output_buffer ) == NULL ) {
      return -1;
    }
    // Make fieldmap
    render_as_string_fieldmap = iGraphResponse.NewFieldMap( field_buffer, fx, default_fieldmap_definition );
  }
  else {
    search_result->list_width = data_width;
  }

  // Allocate the return data array
  // This is a low-level array of QWORDs that will represent object pointers or concrete values depending
  // on relative position according to the result attribute spec
  size_t n_field_elements = search_result->list_width * search_result->list_length;
  if( PALIGNED_ARRAY( search_result->list, vgx_ResponseFieldData_t, n_field_elements ) == NULL ) {
    // ERROR: Out of memory
    if( render_as_string_fieldmap ) {
      free( render_as_string_fieldmap );
    }
    return -1;
  }

#define IF_FIELDS_REMAIN( SingleFieldBitmask, Remain, WritePointer )  \
if( SingleFieldBitmask & Remain )  {                                  \
  vgx_ResponseAttrFastMask remain = Remain ^= SingleFieldBitmask;     \
  (WritePointer)->attr = SingleFieldBitmask;

#define END_IF_FIELDS_REMAIN                              \
  if( remain == VGX_RESPONSE_ATTRS_NONE ) {               \
    break;                                                \
  }                                                       \
}

  vgx_Vector_t *probe_vector = NULL;

  if( response_fastmask & VGX_RESPONSE_ATTRS_RELEVANCE ) {
    vgx_Vector_t *vector;
    // Try first to use the ranking context's vector for sorting
    if( probe_vector == NULL ) {
      if( search_result->query->ranking_condition && (vector=search_result->query->ranking_condition->vector) != NULL ) {
        if( !CALLABLE( vector )->IsNull( vector ) ) {
          probe_vector = vector;
        }
      }
    }
    // Next try to use the similarity condition vector of the first neighborhood for sorting
    if( probe_vector == NULL ) {
      if( search_result->query->vertex_condition && search_result->query->vertex_condition->advanced.similarity_condition ) {
        if( (vector = search_result->query->vertex_condition->advanced.similarity_condition->probevector) != NULL ) {
          if( !CALLABLE( vector )->IsNull( vector ) ) {
            probe_vector = vector;
          }
        }
      }
    }
    if( probe_vector == NULL ) {
      // warning?
    }
  }
    
  vgx_Similarity_t *similarity = graph->similarity;
  vgx_Vector_t *vector;


  // Acquire the graph lock only when necessary.
  // Then keep it for a while to avoid constant in/out, then yield.
  // Re-acquire again only when necessary.
  vgx_Graph_t *locked_graph = NULL;

  // Iterate over entries raw search result  // Fieldspec bitmaps
  vgx_ResponseFieldData_t *output_entry = search_result->list;
  vgx_CollectorItem_t *collected;
#define __YIELD_INTERVAL 100
  int64_t yield_after = __YIELD_INTERVAL; // For large result sets, we yield the state lock once in a while to avoid long blocking 

  vgx_ResponseAttrFastMask attr_fastmask = response_fastmask;
  vgx_ResponseFields_t fields = search_result->list_fields;

  // We are including head identifier
  int head_identify = (attr_fastmask & VGX_RESPONSE_ATTRS_ID) != 0;
 
  // Head vertex will be dereferenced if any fields other than predicator fields are needed
  int head_deref = vgx_query_response_head_deref( search_result->query );

  // Anchor vertex will be dereferenced
  int tail_identify = (attr_fastmask & VGX_RESPONSE_ATTRS_ANCHOR) != 0;

  // Override deref and identify flags if select evaluator is in use
  if( fields.selecteval ) {
    if( CALLABLE( fields.selecteval )->Identifiers( fields.selecteval ) > 0 ) {
      head_deref = head_identify = 1;
      tail_identify = 1;
    }
    if( CALLABLE( fields.selecteval )->HeadDeref( fields.selecteval ) > 0 ) {
      head_deref = 1;
    }
  }

  vgx_VertexCompleteIdentifier_t tail_ident = {0}, *ptail_ident = &tail_ident;
  vgx_VertexCompleteIdentifier_t head_ident = {0}, *phead_ident = &head_ident;

  if( !is_string_entry ) {
    // Allocate identifier buffers as needed
    if( tail_identify ) {
      if( CALIGNED_ARRAY( search_result->tail_identifiers, vgx_VertexCompleteIdentifier_t, search_result->list_length ) == NULL ) {
        return -1;
      }
      ptail_ident = search_result->tail_identifiers;
    }
    if( head_identify ) {
      if( CALIGNED_ARRAY( search_result->head_identifiers, vgx_VertexCompleteIdentifier_t, search_result->list_length ) == NULL ) {
        return -1;
      }
      phead_ident = search_result->head_identifiers;
    }
  }

  static const vgx_VertexCompleteIdentifier_t zero_identifier = {0};

  static const vgx_ResponseAttrFastMask CS_REQUIRED_MASK = VGX_RESPONSE_ATTR_RELTYPE | VGX_RESPONSE_ATTR_TYPENAME | VGX_RESPONSE_ATTR_VECTOR | VGX_RESPONSE_ATTR_PROPERTY;

  int64_t n_dummy = 0;

  while( (collected=cursor++) < end ) {
    // Omit results with time-class modifiers unless time modifiers enabled
    vgx_predicator_t predicator = collected->predicator;
    if( !fields.include_mod_tm && _vgx_predicator_mod_is_time( predicator ) ) {
      continue;
    }

    vgx_VertexRef_t *tailref = collected->tailref;
    vgx_VertexRef_t *headref = collected->headref;

    // Sanity-check head vertex
    if( headref == NULL || headref->refcnt < 1 ) {
      ++n_dummy;
      continue;
    }

    vgx_Vertex_t *head = headref->vertex;

    vgx_ResponseFieldData_t *wp = is_string_entry ? field_buffer : output_entry;

    // Tail ID required
    if( tail_identify ) {
      if( tailref != NULL ) {
        __assert_safe_tail( graph, collected );
        // Tail identifier(s) requested
        __set_identifier( graph, ptail_ident, tailref->vertex );
      }
      else {
        *ptail_ident = zero_identifier;
      }
    }

    // Head vertex will be dereferenced
    if( head_deref ) {
      __assert_safe_head( graph, collected );
      // Head identifier(s) requested
      if( head_identify ) {
        __set_identifier( graph, phead_ident, head );
      }
      else {
        *phead_ident = zero_identifier;
      }
    }

    // Rendered attribute(s) require CS
    if( (attr_fastmask & CS_REQUIRED_MASK) != 0 ) {
      if( locked_graph == NULL ) {
        locked_graph = GRAPH_ENTER_CRITICAL_SECTION( graph );
      }
    }

    vgx_ResponseAttrFastMask remaining_fields = attr_fastmask;

    // COUNT
    ++n_items;
    

    if( single_pred_element ) {
      switch( remaining_fields ) {
      case VGX_RESPONSE_ATTR_ARCDIR:
        IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ARCDIR, remaining_fields, wp )      { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
        break;
      case VGX_RESPONSE_ATTR_RELTYPE:
        IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_RELTYPE, remaining_fields, wp )     { wp++->value.CSTR__str = _vxenum_rel__decode_CS( graph, predicator.rel.enc ); } END_IF_FIELDS_REMAIN

        break;
      case VGX_RESPONSE_ATTR_MODIFIER:
        IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_MODIFIER, remaining_fields, wp )    { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
        break;
      case VGX_RESPONSE_ATTR_VALUE:
        IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_VALUE, remaining_fields, wp )       { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
        break;
      default:
        break;
      }
    }
    else {
      do {
        // ANCHOR
        if( VGX_RESPONSE_ATTRS_ANCHOR & remaining_fields ) {
          vgx_VertexCompleteIdentifier_t *ident = ptail_ident;
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ANCHOR, remaining_fields, wp )        { wp++->value.ident = ident; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ANCHOR_OBID, remaining_fields, wp )   { wp++->value.ident = ident; } END_IF_FIELDS_REMAIN
        }

        // ID
        if( VGX_RESPONSE_ATTRS_ID & remaining_fields ) {
          vgx_VertexCompleteIdentifier_t *ident = phead_ident;
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ID, remaining_fields, wp )            { wp++->value.ident = ident; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_OBID, remaining_fields, wp )          { wp++->value.ident = ident; } END_IF_FIELDS_REMAIN
        } 
        
        // TYPE
        IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_TYPENAME, remaining_fields, wp )        { wp++->value.CSTR__str = _vxenum_vtx__decode_CS( graph, head->descriptor.type.enumeration ); } END_IF_FIELDS_REMAIN

        // DEGREES
        if( VGX_RESPONSE_ATTRS_DEGREES & remaining_fields ) {
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_DEGREE, remaining_fields, wp )      { wp++->value.i64 = iarcvector.Degree( &head->inarcs ) + iarcvector.Degree( &head->outarcs ); } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_INDEGREE, remaining_fields, wp )    { wp++->value.i64 = iarcvector.Degree( &head->inarcs ); } END_IF_FIELDS_REMAIN           
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_OUTDEGREE, remaining_fields, wp )   { wp++->value.i64 = iarcvector.Degree( &head->outarcs ); } END_IF_FIELDS_REMAIN
        }

        // PREDICATOR
        if( VGX_RESPONSE_ATTRS_PREDICATOR & remaining_fields ) {
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ARCDIR, remaining_fields, wp )        { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_RELTYPE, remaining_fields, wp )       { wp++->value.CSTR__str = _vxenum_rel__decode_CS( graph, predicator.rel.enc ); } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_MODIFIER, remaining_fields, wp )      { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_VALUE, remaining_fields, wp )         { wp++->value.pred.data = predicator.data; } END_IF_FIELDS_REMAIN
        }

        // PROPERTIES
        if( VGX_RESPONSE_ATTRS_PROPERTIES & remaining_fields ) {
          // LEAK WARNING: If vector is included in output, callers owns memory! (since the call to externalize vector creates a new object)
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_VECTOR, remaining_fields, wp ) {
            if( head->vector ) {
              wp++->value.vector = CALLABLE( similarity )->ExternalizeVector( similarity, head->vector, true );
            }
            else {
              wp++->value.vector = ivectorobject.Null( similarity );
            }
          } END_IF_FIELDS_REMAIN
          // LEAK WARNING: If properties are included in output, caller owns memory! (since the call to get properties creates a new object)
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_PROPERTY, remaining_fields, wp ) {
            // Get selected properties
            vgx_VertexIdentifier_t *ptail_id = ptail_ident ? &ptail_ident->identifier : NULL;
            wp++->value.eval_properties = _vxvertex_property__eval_properties_RO_CS( collected, fields.selecteval, ptail_id, &phead_ident->identifier );
          } END_IF_FIELDS_REMAIN
        }

        // RELEVANCE
        if( VGX_RESPONSE_ATTRS_RELEVANCE & remaining_fields ) {
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_RANKSCORE, remaining_fields, wp )     { wp++->value.real = getrank( collected ); } END_IF_FIELDS_REMAIN
          vector = head->vector;
          if( vector && probe_vector ) {
            IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_SIMILARITY, remaining_fields, wp )  { wp++->value.real = CALLABLE(similarity)->Similarity( similarity, probe_vector, vector ); } END_IF_FIELDS_REMAIN
            IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_HAMDIST, remaining_fields, wp )     { wp++->value.i64 = hamdist64( probe_vector->fp, vector->fp ); } END_IF_FIELDS_REMAIN
          }
          else {
            IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_SIMILARITY, remaining_fields, wp )  { wp++->value.real = -1.0; } END_IF_FIELDS_REMAIN
            IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_HAMDIST, remaining_fields, wp )     { wp++->value.i64 = 64; } END_IF_FIELDS_REMAIN
          }
        }

        // TIMESTAMP
        if( VGX_RESPONSE_ATTRS_TIMESTAMP & remaining_fields ) {
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_TMC, remaining_fields, wp )           { wp++->value.i64 = head->TMC; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_TMM, remaining_fields, wp )           { wp++->value.i64 = head->TMM; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_TMX, remaining_fields, wp )           { wp++->value.i64 = __vertex_get_expiration_ts( head ); } END_IF_FIELDS_REMAIN
        }

        // DETAILS
        if( VGX_RESPONSE_ATTRS_DETAILS & remaining_fields ) {
          cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( head );
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_DESCRIPTOR, remaining_fields, wp )  { wp++->value.bits = head->descriptor.bits; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_ADDRESS, remaining_fields, wp )     { wp++->value.vertex = head; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_HANDLE, remaining_fields, wp )      { wp++->value.bits = handle.qword; } END_IF_FIELDS_REMAIN
          IF_FIELDS_REMAIN( VGX_RESPONSE_ATTR_RAW_VERTEX, remaining_fields, wp )  { wp++->value.vertex = head; } END_IF_FIELDS_REMAIN
        }
      } WHILE_ZERO;
      if( ptail_ident != &tail_ident ) {
        ++ptail_ident;
      }
      if( phead_ident != &head_ident ) {
        ++phead_ident;
      }
    }

    // Writing single string entries to the output list
    if( is_string_entry ) {
      const vgx_ResponseFieldMap_t *fieldmap_cursor = render_as_string_fieldmap;
      if( emulate_json ) {
        *output_buffer.wp++ = '{';
        output_buffer.remain--;
      }
      if( single_pred_element ) {
        __single_predicator_field_to_buffer( field_buffer, &fieldmap_cursor, &output_buffer, emulate_json );
      }
      else {
        if( POPCNT32( attr_fastmask & VGX_RESPONSE_ATTRS_ANCHORED_ARC ) > 1 ) {
          int distance = _vgx_predicator_eph_get_value( predicator );
          __predicator_fields_to_buffer( field_buffer, &fieldmap_cursor, &output_buffer, CSTR__root_anchor, distance, emulate_json );
        }
        while( fieldmap_cursor->srcpos != -1 ) {
          vgx_ResponseFieldData_t *field = field_buffer + fieldmap_cursor->srcpos;
          if( field->attr & VGX_RESPONSE_ATTRS_PREDICATOR ) {
            __single_predicator_field_to_buffer( field_buffer, &fieldmap_cursor, &output_buffer, emulate_json );
            continue;
          }
          else if( emulate_json ) {
            __render_named_field( fieldmap_cursor, field->value, &output_buffer );
          }
          else {
            fieldmap_cursor->render( field->value, &output_buffer );
          }

          switch( field->attr ) {
          // Clean up vector if used
          case VGX_RESPONSE_ATTR_VECTOR:
            if( (vector = (vgx_Vector_t*)field->value.vector) != NULL ) {
              CALLABLE( vector )->Decref( vector );
            }
            break;
          // Clean up properties if used
          case VGX_RESPONSE_ATTR_PROPERTY:
            iEvaluator.DiscardStack_CS( &field->value.eval_properties );
            break;

          default:
            break;
          }
          *output_buffer.wp++ = ',';
          *output_buffer.wp++ = ' ';
          output_buffer.remain -= 2;
          ++fieldmap_cursor;
        }

        if( output_buffer.wp > output_buffer.buffer ) {
          output_buffer.wp -= 2; // remove trailing ", "
        }
      }


      if( emulate_json ) {
        *output_buffer.wp++ = '}';
        output_buffer.remain--;
      }

      *output_buffer.wp = '\0';

      // Construct string instance from buffer
      CString_constructor_args_t *cargs = &string_entry_args;
      size_t strsz = output_buffer.wp - output_buffer.buffer;
      if( strsz > ARCH_PAGE_SIZE ) {
        // Leave CS if large string entries are being rendered -- will have to be re-acquired again when needed
        GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );
        // Oversized string
        if( strsz > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
          cargs = &ovsz_string_entry_args;
        }
      }
      cargs->string = output_buffer.buffer;
      cargs->len = (int)strsz;
      if( (output_entry->value.CSTR__str = COMLIB_OBJECT_NEW( CString_t, NULL, cargs )) == NULL ) {
        CRITICAL( 0x922, "Result entry omitted (out of memory)" );
      }
      output_entry->attr = VGX_RESPONSE_SHOW_AS_STRING; // SPECIAL USE OF THIS ENUM
      // Next entry
      output_entry++;
      // Reset output buffer
      output_buffer.wp = output_buffer.buffer;
      output_buffer.remain = output_buffer.sz;
      // Reset identifiers
      if( tail_ident.identifier.CSTR__idstr ) {
        icstringobject.DecrefNolock( tail_ident.identifier.CSTR__idstr );
      }
      if( head_ident.identifier.CSTR__idstr ) {
        icstringobject.DecrefNolock( head_ident.identifier.CSTR__idstr );
      }
    }
    // Writing individual fields directly to the output list
    else {
      // Next entry
      output_entry = wp;
    }

    // Free the head and/or tail reference(s) and release any locks
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( collector, collected->tailref, &locked_graph );
    _vxquery_collector__del_vertex_reference_ACQUIRE_CS( collector, headref, &locked_graph );

    if( locked_graph ) {
      if( --yield_after == 0 ) {
        GRAPH_YIELD_AND_SIGNAL( graph );
        yield_after = __YIELD_INTERVAL;
      }
    }
  }

  if( n_dummy > 0 ) {
    WARN( 0x921, "Skipping render of %lld dummy collector item(s)", n_dummy );
  }

  GRAPH_LEAVE_CRITICAL_SECTION( &locked_graph );

  if( output_buffer.buffer ) {
    free( output_buffer.buffer ); // QUESTION: What is the alignment story here? TODO: is buffer ever being populated by a Read() on a comlibsequence??
  }
  if( render_as_string_fieldmap ) {
    free( render_as_string_fieldmap );
  }

  // Some results were omitted
  if( search_result->list_length != n_items ) {
    // Account for omitted items
    search_result->n_excluded = search_result->list_length - n_items;

    // Actual number of items returned (allocated list is not completely filled)
    search_result->list_length = n_items;
  }


  return search_result->list_length;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t _vxquery_response__build_search_result( vgx_Graph_t *self, const vgx_ResponseFields_t *fields, const vgx_ResponseFieldMap_t *fieldmap, vgx_BaseQuery_t *query ) {

  int64_t n = 0;

  // Delete any previous result we may have
  iGraphResponse.DeleteSearchResult( &query->search_result );
  
  XTRY {
    // Allocate the return object
    if( (query->search_result = (vgx_SearchResult_t*)calloc( 1, sizeof( vgx_SearchResult_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x921 );
    }

    // Keep a reference to the query
    query->search_result->query = query;

    // Set the response field spec
    if( !vgx_is_response_attrs_valid( fields->fastmask ) ) {
      THROW_ERROR( CXLIB_ERR_API, 0x922 );
    }
    query->search_result->list_fields = *fields;

    // Set the response field map
    query->search_result->fieldmap = fieldmap;

    query->search_result->tail_identifiers = NULL;
    query->search_result->head_identifiers = NULL;

    query->search_result->graph = self;

    if( (n = __build_response_list( query->search_result )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x923 );
    }

  }
  XCATCH( errcode ) {
    iGraphResponse.DeleteSearchResult( &query->search_result );
    n = -1;
  }
  XFINALLY {
  }

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxquery_response__build_default_search_result( vgx_Graph_t *self, vgx_BaseQuery_t *query ) {
  static const vgx_ResponseFields_t response_fields = { 
    .fastmask       = VGX_RESPONSE_DEFAULT,
    .selecteval     = NULL,
    .include_mod_tm = true
  };
  return _vxquery_response__build_search_result( self, &response_fields, NULL, query );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_response__delete_properties( vgx_Graph_t *self, vgx_SelectProperties_t **selected_properties ) {
  GRAPH_LOCK( self ) {
    _vxvertex_property__free_select_properties_CS( self, selected_properties );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __discard_critical_objects( vgx_Graph_t *self, vgx_ResponseFieldData_t *entry, int width, int64_t length, int64_t vecx, int64_t propx ) {
  GRAPH_LOCK( self ) {
    vgx_Vector_t *vector;
    for( int64_t n=0; n<length; n++ ) {
      if( vecx >= 0 ) {
        if( (vector = (vgx_Vector_t*)entry[ vecx ].value.vector) != NULL ) {
          // Necessary lock managed on the inside
          CALLABLE(vector)->Decref(vector);
        }
      }
      if( propx >= 0 ) {
        iEvaluator.DiscardStack_CS( &entry[ propx ].value.eval_properties );
      }
      entry += width;
    }
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_response__delete_search_result( vgx_SearchResult_t **search_result ) {
  
  if( search_result && *search_result ) {
    int64_t length = 0;

    vgx_SearchResult_t *sr = *search_result;
    if( sr->list ) {
      vgx_ResponseFieldData_t *list = sr->list;
      length = sr->list_length;
      if( length ) {
        vgx_ResponseAttrFastMask fastmask = sr->list_fields.fastmask;

        // Clean up allocated string entries for the simple string response
        if( vgx_response_show_as_string( fastmask ) ) {
          // Start cleanup at first entry
          vgx_ResponseFieldData_t *entry = list;
          // Go through all entries in list
          for( int64_t n=0; n<length; n++ ) {
            if( entry->value.CSTR__str ) {
              CStringDelete( entry->value.CSTR__str );
            }
            ++entry;
          }
        }
        // Clean up properties and/or vectors
        else if( (fastmask & VGX_RESPONSE_ATTRS_PROPERTIES) ) {
          int width = sr->list_width;

          // Find the vector index, if any
          int64_t vecx = -1;
          for( int64_t i=0; i < width && vecx < 0; i++ ) {
            if( list[i].attr == VGX_RESPONSE_ATTR_VECTOR ) {
              vecx = i;
            }
          }

          // Find the property index, if any
          int64_t propx = -1;
          for( int64_t i=0; i < width && propx < 0; i++ ) {
            if( list[i].attr == VGX_RESPONSE_ATTR_PROPERTY ) {
              propx = i;
            }
          }

          // Hold CS while discarding objects to avoid large number of in/out of CS inside calls during loop
          if( vecx >= 0 || propx >= 0 ) {
            // Go through all entries in list
            __discard_critical_objects( sr->graph, list, width, length, vecx, propx );
          }
        }
      }

      // Delete the result list
      ALIGNED_FREE( sr->list );
    }

    if( sr->tail_identifiers ) {
      vgx_VertexCompleteIdentifier_t *ptail_ident = sr->tail_identifiers;
      vgx_VertexCompleteIdentifier_t *ptail_end = ptail_ident + length;
      CString_t *CSTR__id;
      while( ptail_ident < ptail_end ) {
        if( (CSTR__id = ptail_ident++->identifier.CSTR__idstr) != NULL ) {
          // Assumption: this string object is never accessed by other threads => ok to not lock allocator family
          icstringobject.DecrefNolock( CSTR__id );
        }
      }
      ALIGNED_FREE( sr->tail_identifiers );
    }

    if( sr->head_identifiers ) {
      vgx_VertexCompleteIdentifier_t *phead_ident = sr->head_identifiers;
      vgx_VertexCompleteIdentifier_t *phead_end = phead_ident + length;
      CString_t *CSTR__id;
      while( phead_ident < phead_end ) {
        if( (CSTR__id = phead_ident++->identifier.CSTR__idstr) != NULL ) {
          // Assumption: this string object is never accessed by other threads => ok to not lock allocator family
          icstringobject.DecrefNolock( CSTR__id );
        }
      }
      ALIGNED_FREE( sr->head_identifiers );
    }

    // Delete the search_result structure
    free( *search_result );

    *search_result = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_response__format_results_to_stream( vgx_Graph_t *self, vgx_BaseQuery_t *query, FILE *output ) {
  _vxquery_response__build_default_search_result( self, query );
  vgx_SearchResult_t *sr = query->search_result;
  const vgx_ResponseFieldData_t *entry = sr->list;
  for( int64_t n=0; n < sr->list_length; n++ ) {
    const CString_t *CSTR__string = entry++->value.CSTR__str;
    fprintf( output, "%lld: %s\n", n+1, CStringValue( CSTR__string ) );
  }
  _vxquery_response__delete_search_result( &query->search_result );
}



/******************************************************************************
 *
 *
 *
 ******************************************************************************
 */
static CTokenizer_t * __new_generic_tokenizer( void ) {

  unicode_codepoint_t ucKEEP[] = {
    '!',  '#',  '$',  '%',
    '&',  '*',  '/',  '<',
    '=',  '>',  '.',  '@',
    '_',  '|',
    0
  };

  unicode_codepoint_t ucSKIP[] = {
    0
  };

  unicode_codepoint_t ucSPLIT[] = { 
    ',',  ';',  '\'',  '"',
    '~',
    0
  };

  unicode_codepoint_t ucIGNORE[] = {
    0
  };

  unicode_codepoint_t ucCOMBINE[] = {
    0
  };


  CTokenizer_constructor_args_t tokargs = {
    .keep_digits        = 1,
    .normalize_accents  = 0,
    .lowercase          = 0,
    .keepsplit          = 1,
    .strict_utf8        = 1,
    .overrides  = {
      .keep             = ucKEEP,
      .skip             = ucSKIP,
      .split            = ucSPLIT,
      .ignore           = ucIGNORE,
      .combine          = ucCOMBINE
    }
  };

  CTokenizer_t *tokenizer = COMLIB_OBJECT_NEW(  CTokenizer_t, "Generic Tokenizer", &tokargs );
  return tokenizer;
}



/******************************************************************************
 *
 * _vxquery_response__select_property
 *
 ******************************************************************************
 */
static vgx_VertexProperty_t * _vxquery_response__select_property( vgx_Graph_t *graph, const char *name, vgx_VertexProperty_t *prop ) {

  // Internal attribute
  if( *name == '_' ) {
    // Abuse the keyhash in this case to store internal attribute code
    if( !iMapping.IntegerMapGet( g_keymap, &g_keymap_dyn, name, (int64_t*)&prop->keyhash ) ) {
      prop->keyhash = 0; // Not found
    }
    return prop;
  }

  // Get key from enumerator
  if( *name == '#' ) {
    ++name;
  }

  if( (prop->key = iEnumerator_OPEN.Property.Key.NewSelect( graph, name, &prop->keyhash )) == NULL ) {
    prop->keyhash = 0;
  }

  return prop;

}



/******************************************************************************
 *
 * _vxquery_response__parse_select_properties
 *
 ******************************************************************************
 */
static vgx_Evaluator_t * _vxquery_response__parse_select_properties( vgx_Graph_t *graph, const char *select_statement, vgx_Vector_t *vector, CString_t **CSTR__error ) {
  // Return value
  vgx_Evaluator_t *propeval = NULL;

  char *expression = NULL;

  XTRY {

    if( (expression = __prepare_select_statement( select_statement, CSTR__error )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( (propeval = iEvaluator.NewEvaluator( graph, expression, vector, CSTR__error )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    if( CALLABLE( propeval )->HasCull( propeval ) ) {
      __set_error_string( CSTR__error, "mcull() not allowed in select statement" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Vertex properties that don't exist will be set to STACK_ITEM_TYPE_NONE
    CALLABLE( propeval )->SetDefaultProp( propeval, NULL );

  }
  XCATCH( errcode ) {
    iEvaluator.DiscardEvaluator( &propeval );
  }
  XFINALLY {
    if( expression ) {
      ALIGNED_FREE( expression );
    }
  }

  return propeval;
}



/******************************************************************************
 *
 * Transform an external select statement to internal form which can be used
 * to construct an evaluator.
 * 
 * Example:
 *
 * prop1, .type, prop2(17), .arc.value
 *   
 *      becomes
 *
 * { "prop1", .property( "prop1" ), ".type", .type, "prop2", .property( "prop2", 17 ), ".arc.value", .arc.value; 0 }
 *
 ******************************************************************************
 */
static char * __prepare_select_statement( const char *select_statement, CString_t **CSTR__error ) {
  // Return value
  char *expression = NULL;

  enum parser_state {
    EXPECT_NONE,
    EXPECT_NAME,
    EXPECT_NAME_OR_END,
    EXPECT_SEPARATOR,
    EXPECT_SEPARATOR_OR_DEFAULT,
    EXPECT_DEFAULT,
    EXPECT_FREE_SYNTAX
  } state = EXPECT_NAME;
  

  // Create the tokenizer if needed
  static CTokenizer_t *tokenizer = NULL;
  if( tokenizer == NULL ) {
    tokenizer = __new_generic_tokenizer();
  }
  tokenmap_t *tokenmap = NULL;
  const char *current_token = NULL;

  CStringQueue_t *output = NULL;


  XTRY {
    // Tokenize input string
    if( (tokenmap = CALLABLE( tokenizer )->Tokenize( tokenizer, (BYTE*)select_statement, CSTR__error )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Empty string
    if( tokenmap->ntok == 0 ) {
      __set_error_string( CSTR__error, "one or more property fields required" );
      THROW_SILENT( CXLIB_ERR_API, 0x004 );
    }

    // Create output buffer
    if( (output = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    CStringQueue_vtable_t *iQ = CALLABLE( output );
    int64_t (*Write)( CStringQueue_t *output, const char *value, int64_t sz ) = iQ->WriteNolock;

#define STRLEN( String )       ((sizeof( String ) / sizeof( String[0] )) - 1)

#define WRITE_CONST( String )   Write( output, String, STRLEN( String ) )
#define CX_WRITE( String, Length ) Write( output, String, Length )



    WRITE_CONST( "{ " );

    const BYTE *token = NULL;
    tokinfo_t tokinfo;
    int token_len;
    char propname[512] = {0};
    int proplen = 0;
    int nesting = 0;
    char quoted[1] = {0};
    uint32_t soffset = 0;
    while( ( token = CALLABLE( tokenizer )->GetTokenAndInfo( tokenizer, tokenmap, &tokinfo ) ) != NULL ) {
      current_token = (char*)token;
      token_len = tokinfo.len;

      if( *quoted ) {
        while( ++soffset < tokinfo.soffset ) {
          WRITE_CONST( " " );
        }
        soffset += token_len-1;
      }

      if( *current_token == '"' || *current_token == '\'' ) {
        if( !*quoted ) {
          *quoted = *current_token;
          soffset = tokinfo.soffset;
        }
        else if( *current_token == *quoted ) {
          *quoted = 0;
        }
      }

      // Expect fieldname
      switch( state ) {
      case EXPECT_NAME:
      case EXPECT_NAME_OR_END:
        // Current field name: <field>]
        // "'<field>', "
        WRITE_CONST( "'" );
        CX_WRITE( current_token, token_len );
        WRITE_CONST( "', " );
        // Attributes start with period .
        if( *current_token == '.' ) {
          // ".arc.<x>"
          if( token_len > 5 && CharsStartsWithConst( current_token, ".arc." ) ) {
            // "reldec( .arc.type )"
            if( CharsEqualsConst( current_token, ".arc.type" ) ) {
              WRITE_CONST( "reldec( .arc.type ) " );
            }
            // "modtostr( .arc.mod )"
            else if( CharsEqualsConst( current_token, ".arc.mod" ) ) {
              WRITE_CONST( "modtostr( .arc.mod ) " );
            }
            // "dirtostr( .arc.dir )"
            else if( CharsEqualsConst( current_token, ".arc.dir" ) ) {
              WRITE_CONST( "dirtostr( .arc.dir ) " );
            }
            // ".arc.<x>"
            else {
              CX_WRITE( current_token, token_len );
              WRITE_CONST( " " );
            }
          }
          // "typedec( .type )"
          else if( CharsEqualsConst( current_token, ".type" ) ) {
            WRITE_CONST( "typedec( .type ) " );
          }
          // ".<field>"
          else {
            CX_WRITE( current_token, token_len );
            WRITE_CONST( " " );
          }
          // ", "
          WRITE_CONST( ", " );
          state = EXPECT_SEPARATOR;
        }
        // Everything else is treated as a vertex property name or free syntax expression
        else {
          proplen = token_len < (int)sizeof( propname ) ? token_len : sizeof( propname );
          strncpy( propname, current_token, proplen );
          state = EXPECT_SEPARATOR_OR_DEFAULT;
        }
        continue;
      case EXPECT_SEPARATOR:
        if( !(*current_token == ',' || *current_token == ';') ) {
          __format_error_string( CSTR__error, "property select syntax error: '%s', expected ',' or ';'", current_token );
          THROW_SILENT( CXLIB_ERR_API, 0x003 );
        } 
        state = EXPECT_NAME_OR_END;
        continue;
      case EXPECT_SEPARATOR_OR_DEFAULT:
        switch( *current_token ) {
        // No default specified, continue
        case ';':
        case ',':
          WRITE_CONST( ".property('" );
          CX_WRITE( propname, proplen );
          WRITE_CONST( "'), " );
          proplen = 0;
          state = EXPECT_NAME;
          continue;
        // Default value next
        case '?':
          state = EXPECT_DEFAULT;
          continue;
        // Free syntax is next
        case ':':
          ++nesting;
          WRITE_CONST( "(" );
          state = EXPECT_FREE_SYNTAX;
          continue;
        default:
          __format_error_string( CSTR__error, "property select syntax error: '%s', expected '('", current_token );
          THROW_SILENT( CXLIB_ERR_API, 0x004 );
        }
      case EXPECT_DEFAULT:
        WRITE_CONST( ".property('" );
        CX_WRITE( propname, proplen );
        WRITE_CONST( "', " );
        CX_WRITE( current_token, token_len );
        WRITE_CONST( " ), " );
        proplen = 0;
        state = EXPECT_SEPARATOR;
        continue;
      case EXPECT_FREE_SYNTAX:
        switch( *current_token ) {
        case '(':
        case '{':
          ++nesting;
          CX_WRITE( current_token, token_len );
          break;
        case ')':
        case '}':
          --nesting;
          CX_WRITE( current_token, token_len );
          break;
        case ';':
          // end of free syntax
          if( nesting == 1 ) {
            WRITE_CONST( "), " );
            nesting = 0;
            state = EXPECT_NAME_OR_END;
            continue;
          }
          break;
        default:
          CX_WRITE( current_token, token_len );
          if( !*quoted ) {
            WRITE_CONST( " " );
          }
        }
        continue;

      default:
        break;
      }
    }

    // Finish
    switch( state ) {
    case EXPECT_SEPARATOR_OR_DEFAULT:
      WRITE_CONST( ".property('" );
      CX_WRITE( propname, proplen );
      WRITE_CONST( "'), " );
      break;
    case EXPECT_FREE_SYNTAX:
      if( nesting == 1 ) {
        WRITE_CONST( "), " );
      }
      else {
        __format_error_string( CSTR__error, "property select syntax error: '%s'", current_token );
        THROW_SILENT( CXLIB_ERR_API, 0x005 );
      }
      break;
    case EXPECT_SEPARATOR:
    case EXPECT_NAME_OR_END:
      break;
    default:
      __format_error_string( CSTR__error, "property select syntax error: '%s'", current_token );
      THROW_SILENT( CXLIB_ERR_API, 0x007 );
    }

    // Terminate and produce output expression
    WRITE_CONST( " 0 }" );
    CALLABLE( output )->NulTermNolock( output );
    CALLABLE( output )->GetValueNolock( output, (void**)&expression );
  }
  XCATCH( errcode ) {
    if( CSTR__error && *CSTR__error == NULL ) {
      if( current_token ) {
        __format_error_string( CSTR__error, "invalid property key: '%s'", current_token );
      }
      else {
        __set_error_string( CSTR__error, "invalid select statement" );
      }
    }
  }
  XFINALLY {
    if( tokenmap ) {
      CALLABLE( tokenizer )->DeleteTokenmap( tokenizer, &tokenmap );
    }
    if( output ) {
      COMLIB_OBJECT_DESTROY( output );
    }
  }

  return expression;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_response.h"

test_descriptor_t _vgx_vxquery_response_tests[] = {
  { "VGX Graph Response Tests", __utest_vxquery_response },
  {NULL}
};
#endif

