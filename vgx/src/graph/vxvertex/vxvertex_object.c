/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxvertex_object.c
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

#include "_vgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


DLL_VISIBLE comlib_object_typeinfo_t _vgx_Vertex_t_typeinfo = {0}; // Will be set when class is registered



/* common comlib_object_vtable_t interface */
static int Vertex_cmpid( const vgx_Vertex_t *self, const void *idptr );
static objectid_t * Vertex_getid( vgx_Vertex_t *self );
static int64_t Vertex_serialize( vgx_Vertex_t *self, CQwordQueue_t *output );
static vgx_Vertex_t * Vertex_deserialize( comlib_object_t *container, CQwordQueue_t *input );
static vgx_Vertex_t * Vertex_constructor( const void *identifier, vgx_Vertex_constructor_args_t *args );
static void Vertex_destructor( vgx_Vertex_t *self );
static CStringQueue_t * Vertex_representer( vgx_Vertex_t *self, CStringQueue_t *output );
static comlib_object_t * Vertex_allocator( vgx_Vertex_t *self );



DLL_HIDDEN CString_t *g_CSTR__ondelete_key = NULL;
DLL_HIDDEN shortid_t g_ondelete_keyhash = 0;



/* -------------- */
/* Vertex Methods */
/* -------------- */
static vgx_Graph_t * Vertex_parent( vgx_Vertex_t *self_RO );
static bool Vertex_is_stable_CS( const vgx_Vertex_t *self_OPEN );
static bool Vertex_readonly( const vgx_Vertex_t *self_OPEN );
static bool Vertex_readable( const vgx_Vertex_t *self_OPEN );
static bool Vertex_writable( const vgx_Vertex_t *self_OPEN );
// Rank
static vgx_Rank_t Vertex_set_rank( vgx_Vertex_t *self_WL, float c1, float c0 );
static vgx_Rank_t Vertex_get_rank( const vgx_Vertex_t *self_RO );
// Degree
static int64_t Vertex_degree( const vgx_Vertex_t *self_RO );
static int64_t Vertex_indegree( const vgx_Vertex_t *self_RO );
static int64_t Vertex_outdegree( const vgx_Vertex_t *self_RO );
// Arcs
static int64_t Vertex_remove_outarcs( vgx_Vertex_t *self_WL );
static int64_t Vertex_remove_inarcs( vgx_Vertex_t *self_WL );
static int64_t Vertex_remove_arcs( vgx_Vertex_t *self_WL );
// Neighbors
static vgx_BaseCollector_context_t * Vertex_neighbors( vgx_Vertex_t *self_RO );
static vgx_BaseCollector_context_t * Vertex_initials( vgx_Vertex_t *self_RO );
static vgx_BaseCollector_context_t * Vertex_terminals( vgx_Vertex_t *self_RO );
// Names
static size_t Vertex_id_length( const vgx_Vertex_t *self_RO );
static const vgx_VertexIdentifier_t * Vertex_identifier( const vgx_Vertex_t *self_RO );
static const char * Vertex_id_string( const vgx_Vertex_t *self_RO );
static const char * Vertex_id_prefix( const vgx_Vertex_t *self_RO );
static CString_t * Vertex_id_cstring( const vgx_Vertex_t *self_RO );
static objectid_t Vertex_internal_id( const vgx_Vertex_t *self_RO );
static const vgx_vertex_type_t Vertex_type( const vgx_Vertex_t *self_RO );
static const CString_t * Vertex_type_name( const vgx_Vertex_t *self_RO );
static const CString_t * Vertex_type_name_CS( const vgx_Vertex_t *self_RO );
// Properties
static int Vertex_set_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
static int Vertex_set_property_key_val( vgx_Vertex_t *self_WL, const char *key, vgx_value_t *value );
static int Vertex_set_int_property( vgx_Vertex_t *self_WL, const char *key, int64_t intval );
static int Vertex_set_real_property( vgx_Vertex_t *self_WL, const char *key, double realval );
static int Vertex_set_string_property( vgx_Vertex_t *self_WL, const char *key, const char *strval );
static int Vertex_format_string_property( vgx_Vertex_t *self_WL, const char *key, const char *valfmt, ... );

static int Vertex_set_on_delete( vgx_Vertex_t *self_WL, vgx_VertexOnDeleteAction action, const char *data );

static vgx_VertexProperty_t * Vertex_inc_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
static int32_t Vertex_enum( vgx_Vertex_t *self_LCK );
static vgx_VertexProperty_t * Vertex_get_internal_attribute( vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop );

static vgx_VertexProperty_t * Vertex_get_property( vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop );
static int Vertex_get_property_value( vgx_Vertex_t *self_RO, const char *key, vgx_value_t *rvalue );
static int Vertex_get_int_property( vgx_Vertex_t *self_RO, const char *key, int64_t *intval );
static int Vertex_get_real_property( vgx_Vertex_t *self_RO, const char *key, double *realval );
static int Vertex_get_string_property( vgx_Vertex_t *self_RO, const char *key, CString_t **CSTR__strval );

static vgx_SelectProperties_t * Vertex_get_properties( vgx_Vertex_t *self_RO );
static bool Vertex_has_property( const vgx_Vertex_t *self_RO, const vgx_VertexProperty_t *prop );
static bool Vertex_has_property_key( const vgx_Vertex_t *self_RO, const char *key );
static bool Vertex_has_properties( vgx_Vertex_t *self_RO );
static int64_t Vertex_num_properties( vgx_Vertex_t *self_RO );
static int Vertex_remove_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
static int Vertex_remove_property_key( vgx_Vertex_t *self_WL, const char *key );
static int64_t Vertex_remove_properties( vgx_Vertex_t *self_WL );
// Timestamps
static bool Vertex_is_expired( const vgx_Vertex_t *self_RO );
static uint32_t Vertex_TMC( const vgx_Vertex_t *self_RO );
static uint32_t Vertex_TMM( const vgx_Vertex_t *self_RO );
static uint32_t Vertex_get_TMX( const vgx_Vertex_t *self_RO );
static int Vertex_set_TMX( vgx_Vertex_t *self_WL, uint32_t new_tmx );
// Vector
static int Vertex_set_vector( vgx_Vertex_t *self_WL, vgx_Vector_t **pvector );
static vgx_Vector_t * Vertex_get_vector( vgx_Vertex_t *self_RO );
static int Vertex_remove_vector( vgx_Vertex_t *self_WL );

static CStringQueue_t * Vertex_descriptor( vgx_Vertex_t *self_RO, CStringQueue_t *output );
static void Vertex_print_vertex_allocator( vgx_Vertex_t *self_RO );
static CString_t * Vertex_short_repr( const vgx_Vertex_t *self_RO );



static void __execute_ondelete_event_CS( vgx_Vertex_t *self_WL );
static void __deconstruct_CS( vgx_Vertex_t *self_WL );
static void __virtualize_CS( vgx_Vertex_t *self_WL );



static vgx_Vertex_vtable_t Vertex_Methods = {
  /* common comlib_object_vtable_t interface */
  .vm_cmpid       = (f_object_comparator_t)Vertex_cmpid,
  .vm_getid       = (f_object_identifier_t)Vertex_getid,
  .vm_serialize   = (f_object_serializer_t)Vertex_serialize,
  .vm_deserialize = (f_object_deserializer_t)Vertex_deserialize,
  .vm_construct   = (f_object_constructor_t)Vertex_constructor,
  .vm_destroy     = (f_object_destructor_t)Vertex_destructor,
  .vm_represent   = (f_object_representer_t)Vertex_representer,
  .vm_allocator   = (f_object_allocator_t)Vertex_allocator,
  /* Vertex interface */
  //
  .Initialize_CS        = __deconstruct_CS,
  .Virtualize_CS        = __virtualize_CS,
  //
  .Parent               = Vertex_parent,
  .IsStable_CS          = Vertex_is_stable_CS,
  .Readonly             = Vertex_readonly,
  .Readable             = Vertex_readable,
  .Writable             = Vertex_writable,
  // Rank
  .SetRank              = Vertex_set_rank,
  .GetRank              = Vertex_get_rank,
  // Degree
  .Degree               = Vertex_degree,
  .InDegree             = Vertex_indegree,
  .OutDegree            = Vertex_outdegree,
  // Arcs
  .RemoveOutarcs        = Vertex_remove_outarcs,
  .RemoveInarcs         = Vertex_remove_inarcs,
  .RemoveArcs           = Vertex_remove_arcs,
  // Neighborhood
  .Neighbors            = Vertex_neighbors,
  .Initials             = Vertex_initials,
  .Terminals            = Vertex_terminals,
  // Names
  .IDLength             = Vertex_id_length,
  .Identifier           = Vertex_identifier,
  .IDString             = Vertex_id_string,
  .IDPrefix             = Vertex_id_prefix,
  .IDCString            = Vertex_id_cstring,
  .InternalID           = Vertex_internal_id,
  .Type                 = Vertex_type,
  .TypeName             = Vertex_type_name,
  // Properties
  .SetProperty          = Vertex_set_property,
  .SetPropertyKeyVal    = Vertex_set_property_key_val,
  .SetIntProperty       = Vertex_set_int_property,
  .SetRealProperty      = Vertex_set_real_property,
  .SetStringProperty    = Vertex_set_string_property,
  .FormatStringProperty = Vertex_format_string_property,
  .SetOnDelete          = Vertex_set_on_delete,
  .IncProperty          = Vertex_inc_property,
  .VertexEnum           = Vertex_enum,
  .GetInternalAttribute = Vertex_get_internal_attribute,
  .GetProperty          = Vertex_get_property,
  .GetPropertyValue     = Vertex_get_property_value,
  .GetIntProperty       = Vertex_get_int_property,
  .GetRealProperty      = Vertex_get_real_property,
  .GetStringProperty    = Vertex_get_string_property,
  .HasProperty          = Vertex_has_property,
  .HasPropertyKey       = Vertex_has_property_key,
  .GetProperties        = Vertex_get_properties,
  .HasProperties        = Vertex_has_properties,
  .NumProperties        = Vertex_num_properties,
  .RemoveProperty       = Vertex_remove_property,
  .RemovePropertyKey    = Vertex_remove_property_key,
  .RemoveProperties     = Vertex_remove_properties,
  // Timestamps
  .IsExpired            = Vertex_is_expired,
  .CreationTime         = Vertex_TMC,
  .ModificationTime     = Vertex_TMM,
  .GetExpirationTime    = Vertex_get_TMX,
  .SetExpirationTime    = Vertex_set_TMX,
  // Vector
  .SetVector            = Vertex_set_vector,
  .GetVector            = Vertex_get_vector,
  .RemoveVector         = Vertex_remove_vector,

  // Misc
  .Descriptor           = Vertex_descriptor,
  .PrintVertexAllocator = Vertex_print_vertex_allocator,
  .ShortRepr            = Vertex_short_repr


  // TODO
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __ondelete_remove_file( vgx_Vertex_t *self, const char *filepath ) {
  const char *idstr = CALLABLE( self )->IDString( self );
  VERBOSE( 0x000, "OnDelete:%s RemoveFile:%s", idstr, filepath );
  if( file_exists( filepath ) ) {
#ifdef VGX_CONSISTENCY_CHECK
    char rpath[ MAX_PATH + 1];
    int ret = snprintf( rpath, MAX_PATH, "%s.DELETED", filepath );
    if( ret < 0 || ret >= MAX_PATH ) {
      rpath[ MAX_PATH ] = '\0';
    }
    if( rename( filepath, rpath ) < 0 )
#else
    if( remove( filepath ) < 0 )
#endif
    {
      REASON( 0x000, "OnDelete:%s FAILED RemoveFile:%s", idstr, filepath );
    }
  }
  else {
    WARN( 0x000, "OnDelete:%s (no such file) RemoveFile:%s", idstr, filepath );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __ondelete_log_message( vgx_Vertex_t *self, const char *message ) {
  const char *idstr = CALLABLE( self )->IDString( self );
  INFO( 0x000, "OnDelete:%s Message:%s", idstr, message );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __ondelete_noop( vgx_Vertex_t *self, const char *data ) {
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Vertex_RegisterClass( void ) {
  // Do some basic sanity checking for critical types
  ASSERT_TYPE_SIZE( vgx_predicator_t,       sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_VertexSortValue_t,  sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( vgx_ArcHead_t,          sizeof( vgx_Vertex_t* ) + sizeof( vgx_predicator_t ) );
  ASSERT_TYPE_SIZE( vgx_Arc_t,              sizeof( vgx_ArcHead_t ) + sizeof( vgx_Vertex_t* ) );
  ASSERT_TYPE_SIZE( vgx_CollectorItem_t,    sizeof( vgx_Arc_t ) + sizeof( vgx_predicator_t ) );  
  ASSERT_TYPE_SIZE( vgx_Operation_t,          8 );
  ASSERT_TYPE_SIZE( vgx_VertexDescriptor_t,   8 );
  ASSERT_TYPE_SIZE( vgx_Rank_t,               8 );
  ASSERT_TYPE_SIZE( vgx_VertexHead_t,       sizeof( __m256i )  );
  ASSERT_TYPE_SIZE( vgx_VertexData_t,       sizeof( cacheline_t ) * 2 );
  ASSERT_TYPE_SIZE( vgx_Vertex_t,           sizeof( cacheline_t ) * 2 + sizeof( __m256i ) );
  ASSERT_TYPE_SIZE( vgx_AllocatedVertex_t,  sizeof( cacheline_t ) * 3 );
  
  COMLIB_REGISTER_CLASS( vgx_Vertex_t, CXLIB_OBTYPE_GRAPH_VERTEX, &Vertex_Methods, OBJECT_IDENTIFIED_BY_OBJECTID, -1 );
  _vgx_Vertex_t_typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Vertex_t );


  if( (g_CSTR__ondelete_key = CStringNew( VGX_VERTEX_ONDELETE_PROPERTY )) != NULL ) {
    g_ondelete_keyhash = CStringHash64( g_CSTR__ondelete_key );
  }
  else {
    FATAL( 0x000, "out of memory" );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Vertex_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Vertex_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcVector_cell_t * _vxvertex__get_vertex_outarcs( vgx_Vertex_t *vertex ) {
  return &vertex->outarcs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcVector_cell_t * _vxvertex__get_vertex_inarcs( vgx_Vertex_t *vertex ) {
  return &vertex->inarcs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * NewVertex_CS( vgx_Graph_t *self, const objectid_t *obid, const CString_t *CSTR__identifier, vgx_VertexTypeEnumeration_t vxtype, vgx_Rank_t rank, vgx_VertexStateContext_man_t manifestation, CString_t **CSTR__error ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex;
  uint32_t ts = _vgx_graph_seconds( self );
  vgx_Vertex_constructor_args_t args;

  GRAPH_SUSPEND_LOCK( self ) {
    args.CSTR__error    = NULL;
    args.CSTR__idstring = CSTR__identifier;
    args.vxtype         = vxtype;
    args.graph          = self;
    args.manifestation  = manifestation;
    args.rank           = rank;
    args.ts             = ts;
    
    // ----------------------------------------
    vertex = Vertex_constructor( obid, &args );
    // ----------------------------------------
    
    if( args.CSTR__error ) {
      __set_error_string( CSTR__error, CStringValue( args.CSTR__error ) );
    }
  } GRAPH_RESUME_LOCK;
  
  if( vertex ) {  
    // Graph's current vertex lock count
    _vgx_graph_inc_vertex_WL_count_CS( self );

    // Open vertex operation for capture
    if( iOperation.Open_CS( self, &vertex->operation, COMLIB_OBJECT( vertex ), false ) == 1 ) {


      // Capture
      // NOTE: Captured vertex will always be created REAL by operation consumer.
      //       Explicit conversion to VIRTUAL is required after this for the
      //       operation consumer to modify manifestation to VIRTUAL.
      // NOTE: If vertex identifier is a cstring we get a reference and use
      //       it to supply the operation capture, since this cstring is CS
      //       protected. (Remember the capture process involves async jobs.)
      //       If vertex identifier is not a cstring we have to clone the
      //       original args cstring because we can't make any assumptions
      //       regarding locking by the API that constructed it.

      // Vertex id is safe to use in threads, own another reference
      CString_t *CSTR__idstring;
      if( (CSTR__idstring = vertex->identifier.CSTR__idstr) != NULL ) {
        icstringobject.IncrefNolock( CSTR__idstring );
      }
      // We have no cstring in the vertex object and the API string is unsafe in threads, clone it
      else {
        CSTR__idstring = icstringobject.Clone( self->ephemeral_string_allocator_context, CSTR__identifier );
      }
      // Capture and commit
      args.CSTR__idstring = CSTR__idstring;
      iOperation.Vertex_CS.New( vertex, &args );
      COMMIT_GRAPH_OPERATION_CS( self );
      // Discard id string (either decref vertex object's id or discard the clone)
      icstringobject.DecrefNolock( CSTR__idstring );
    }
    else {
      Vertex_destructor( vertex );
      vertex = NULL;
      __set_error_string( CSTR__error, "Failed to open vertex operation" );
    }
  }


  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexList_t * vxvertex_object__vertex_list_new( int64_t sz );
static void               vxvertex_object__vertex_list_delete( vgx_VertexList_t **vertices );
static int64_t            vxvertex_object__vertex_list_truncate( vgx_VertexList_t *vertices, int64_t n );
static vgx_Vertex_t *     vxvertex_object__vertex_list_get( vgx_VertexList_t *vertices, int64_t i );
static const objectid_t * vxvertex_object__vertex_list_get_obid( const vgx_VertexList_t *vertices, int64_t i );
static vgx_Operation_t *  vxvertex_object__vertex_list_get_operation( vgx_VertexList_t *vertices, int64_t i );
static vgx_Vertex_t *     vxvertex_object__vertex_list_set( vgx_VertexList_t *vertices, int64_t i, vgx_Vertex_t *vertex );
static int64_t            vxvertex_object__vertex_list_size( const vgx_VertexList_t *vertices );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexIdentifiers_t *    vxvertex_object__vertex_identifiers_new( vgx_Graph_t *graph, int64_t sz );
static vgx_VertexIdentifiers_t *    vxvertex_object__vertex_identifiers_new_pair( vgx_Graph_t *graph, const char *A, const char *B );
static vgx_VertexIdentifiers_t *    vxvertex_object__vertex_identifiers_new_obid_pair( vgx_Graph_t *graph, const objectid_t *obidA, const objectid_t *obidB );
static vgx_VertexIdentifiers_t *    vxvertex_object__vertex_identifiers_new_obids( vgx_Graph_t *graph, const objectid_t *obid_list, int64_t sz );
static void                         vxvertex_object__vertex_identifiers_delete( vgx_VertexIdentifiers_t **identifiers );
static int64_t                      vxvertex_object__vertex_identifiers_truncate( vgx_VertexIdentifiers_t *identifiers, int64_t n );
static vgx_Vertex_t *               vxvertex_object__vertex_identifiers_get_vertex( vgx_VertexIdentifiers_t *identifiers, int64_t i );
static objectid_t                   vxvertex_object__vertex_identifiers_get_obid( vgx_VertexIdentifiers_t *identifiers, int64_t i );
static const char *                 vxvertex_object__vertex_identifiers_get_id( vgx_VertexIdentifiers_t *identifiers, int64_t i );
static CString_t *                  vxvertex_object__vertex_identifiers_set_cstring( vgx_VertexIdentifiers_t *identifiers, int64_t i, CString_t *CSTR__id );
static const char *                 vxvertex_object__vertex_identifiers_set_id( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id );
static const char *                 vxvertex_object__vertex_identifiers_set_id_len( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id, int len );
static objectid_t                   vxvertex_object__vertex_identifiers_set_obid( vgx_VertexIdentifiers_t *identifiers, int64_t i, const objectid_t *obid );
static const vgx_Vertex_t *         vxvertex_object__vertex_identifiers_set_vertex( vgx_VertexIdentifiers_t *identifiers, int64_t i, const vgx_Vertex_t *vertex );
static int64_t                      vxvertex_object__vertex_identifiers_size( const vgx_VertexIdentifiers_t *identifiers );
static bool                         vxvertex_object__vertex_identifiers_check_unique( vgx_VertexIdentifiers_t *identifiers );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IVertex_t iVertex = {
  .List = {
    .New          = vxvertex_object__vertex_list_new,
    .Delete       = vxvertex_object__vertex_list_delete,
    .Truncate     = vxvertex_object__vertex_list_truncate,
    .Get          = vxvertex_object__vertex_list_get,
    .GetObid      = vxvertex_object__vertex_list_get_obid,
    .GetOperation = vxvertex_object__vertex_list_get_operation,
    .Set          = vxvertex_object__vertex_list_set,
    .Size         = vxvertex_object__vertex_list_size
  },
  .Identifiers = {
    .New          = vxvertex_object__vertex_identifiers_new,
    .NewPair      = vxvertex_object__vertex_identifiers_new_pair,
    .NewObidPair  = vxvertex_object__vertex_identifiers_new_obid_pair,
    .NewObids     = vxvertex_object__vertex_identifiers_new_obids,
    .Delete       = vxvertex_object__vertex_identifiers_delete,
    .Truncate     = vxvertex_object__vertex_identifiers_truncate,
    .GetVertex    = vxvertex_object__vertex_identifiers_get_vertex,
    .GetObid      = vxvertex_object__vertex_identifiers_get_obid,
    .GetId        = vxvertex_object__vertex_identifiers_get_id,
    .SetCString   = vxvertex_object__vertex_identifiers_set_cstring,
    .SetId        = vxvertex_object__vertex_identifiers_set_id,
    .SetIdLen     = vxvertex_object__vertex_identifiers_set_id_len,
    .SetObid      = vxvertex_object__vertex_identifiers_set_obid,
    .SetVertex    = vxvertex_object__vertex_identifiers_set_vertex,
    .Size         = vxvertex_object__vertex_identifiers_size,
    .CheckUnique  = vxvertex_object__vertex_identifiers_check_unique
  }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexList_t * vxvertex_object__vertex_list_new( int64_t sz ) {
  vgx_VertexList_t *vertices = calloc( 1, sizeof( vgx_VertexList_t ) + sz * sizeof( vgx_Vertex_t* ) );
  if( vertices ) {
    vertices->sz = sz;
    vertices->vertex = (vgx_Vertex_t**)((char*)vertices + sizeof( vgx_VertexList_t ));
  }
  return vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void vxvertex_object__vertex_list_delete( vgx_VertexList_t **vertices ) {
  if( vertices && *vertices ) {
    free( *vertices );
    *vertices = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t vxvertex_object__vertex_list_truncate( vgx_VertexList_t *vertices, int64_t n ) {
  if( n >= 0 && n < vertices->sz ) {
    vertices->sz = n;
  }
  return vertices->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * vxvertex_object__vertex_list_get( vgx_VertexList_t *vertices, int64_t i ) {
  return vertices->vertex[ i ];
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const objectid_t * vxvertex_object__vertex_list_get_obid( const vgx_VertexList_t *vertices, int64_t i ) {
  return __vertex_internalid( vertices->vertex[ i ] );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Operation_t * vxvertex_object__vertex_list_get_operation( vgx_VertexList_t *vertices, int64_t i ) {
  return &vertices->vertex[ i ]->operation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * vxvertex_object__vertex_list_set( vgx_VertexList_t *vertices, int64_t i, vgx_Vertex_t *vertex ) {
  return vertices->vertex[ i ] = vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t vxvertex_object__vertex_list_size( const vgx_VertexList_t *vertices ) {
  return vertices->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexIdentifiers_t * vxvertex_object__vertex_identifiers_new( vgx_Graph_t *graph, int64_t sz ) {
  vgx_VertexIdentifiers_t *identifiers = calloc( 1, sizeof( vgx_VertexIdentifiers_t ) + sz * sizeof( vgx_VertexCompleteIdentifier_t ) );
  if( identifiers ) {
    identifiers->ids = (vgx_VertexCompleteIdentifier_t*)((char*)identifiers + sizeof( vgx_VertexIdentifiers_t ) );
    identifiers->sz = sz;
    identifiers->graph = graph;
  }
  return identifiers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexIdentifiers_t * vxvertex_object__vertex_identifiers_new_pair( vgx_Graph_t *graph, const char *A, const char *B ) {
  vgx_VertexIdentifiers_t *identifiers = vxvertex_object__vertex_identifiers_new( graph, 2 );
  if( identifiers ) {
    if( vxvertex_object__vertex_identifiers_set_id( identifiers, 0, A ) == NULL ||
        vxvertex_object__vertex_identifiers_set_id( identifiers, 0, B ) == NULL )
    {
      vxvertex_object__vertex_identifiers_delete( &identifiers );
    }
  }
  return identifiers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexIdentifiers_t * vxvertex_object__vertex_identifiers_new_obid_pair( vgx_Graph_t *graph, const objectid_t *obidA, const objectid_t *obidB ) {
  vgx_VertexIdentifiers_t *identifiers = vxvertex_object__vertex_identifiers_new( graph, 2 );
  if( identifiers ) {
    vxvertex_object__vertex_identifiers_set_obid( identifiers, 0, obidA );
    vxvertex_object__vertex_identifiers_set_obid( identifiers, 1, obidB );
  }
  return identifiers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexIdentifiers_t * vxvertex_object__vertex_identifiers_new_obids( vgx_Graph_t *graph, const objectid_t *obid_list, int64_t sz ) {
  vgx_VertexIdentifiers_t *identifiers = vxvertex_object__vertex_identifiers_new( graph, sz );
  if( identifiers ) {
    vgx_VertexCompleteIdentifier_t *cid = identifiers->ids;
    const vgx_VertexCompleteIdentifier_t *end = identifiers->ids + sz;
    const objectid_t *src = obid_list;
    objectid_t *dest; 
    while( cid < end ) {
      dest = &(cid++)->internalid;
      idcpy( dest, src++ );
    }
  }
  return identifiers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void vxvertex_object__vertex_identifiers_delete( vgx_VertexIdentifiers_t **identifiers ) {
  if( identifiers && *identifiers ) {
    vgx_VertexIdentifiers_t *L = *identifiers;
    vgx_VertexCompleteIdentifier_t *ids = L->ids;
    for( int64_t i=0; i < L->sz; i++ ) {
      vgx_VertexIdentifier_t *id = &ids[i].identifier;
      iString.Discard( &id->CSTR__idstr );
    }
    free( *identifiers );
    *identifiers = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t vxvertex_object__vertex_identifiers_truncate( vgx_VertexIdentifiers_t *identifiers, int64_t n ) {
  if( n >= 0 && n < identifiers->sz ) {
    identifiers->sz = n;
  }
  return identifiers->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * vxvertex_object__vertex_identifiers_get_vertex( vgx_VertexIdentifiers_t *identifiers, int64_t i ) {
  vgx_Vertex_t *vertex;
  objectid_t obid = vxvertex_object__vertex_identifiers_get_obid( identifiers, i );
  vertex = _vxgraph_vxtable__query_OPEN( identifiers->graph, NULL, &obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t vxvertex_object__vertex_identifiers_get_obid( vgx_VertexIdentifiers_t *identifiers, int64_t i ) {
  objectid_t obid;
  vgx_VertexCompleteIdentifier_t *cid = &identifiers->ids[ i ];
  // We don't have the internalid directly
  if( idnone( &cid->internalid ) ) {
    vgx_VertexIdentifier_t *id = &cid->identifier;
    // Get the internalid from the CString
    if( id->CSTR__idstr ) {
      idcpy( &obid, CALLABLE( id->CSTR__idstr )->Obid( id->CSTR__idstr ) );
    }
    // Compute the internalid from the raw string
    else {
      obid = smartstrtoid( id->idprefix.data, -1 );
    }
    // Store the internalid so we have it for next time
    idcpy( &cid->internalid, &obid );
  }
  // We have the internalid
  else {
    obid = cid->internalid;
  }
  return obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * vxvertex_object__vertex_identifiers_get_id( vgx_VertexIdentifiers_t *identifiers, int64_t i ) {
  const char *idstring;
  vgx_VertexCompleteIdentifier_t *cid = &identifiers->ids[ i ];
  vgx_VertexIdentifier_t *id = &cid->identifier;
  // We have a CString, get its value
  if( id->CSTR__idstr ) {
    idstring = CStringValue( id->CSTR__idstr );
  }
  // We have a non-empty prefix, return it
  else if( id->idprefix.data[0] != '\0' ) {
    idstring = id->idprefix.data;
  }
  // We have no string data, convert internalid to string
  else {
    // Use the prefix buffer to store the string
    idstring = idtostr( id->idprefix.data, &cid->internalid );
  }
  return idstring;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * vxvertex_object__vertex_identifiers_set_cstring( vgx_VertexIdentifiers_t *identifiers, int64_t i, CString_t *CSTR__id ) {
  static const size_t sz_prefix = sizeof( vgx_VertexIdentifierPrefix_t ) - 1;
  vgx_VertexCompleteIdentifier_t *cid = &identifiers->ids[ i ];
  // Discard existing
  if( cid->identifier.CSTR__idstr != NULL ) {
    CStringDelete( cid->identifier.CSTR__idstr );
    memset( cid, 0, sizeof( vgx_VertexCompleteIdentifier_t ) );
  }
  // Populate CString field
  if( (cid->identifier.CSTR__idstr = OwnOrCloneCString( CSTR__id, identifiers->graph->ephemeral_string_allocator_context )) != NULL ) {
    // Populate internalid field
    const objectid_t *pobid = CALLABLE( cid->identifier.CSTR__idstr )->Obid( cid->identifier.CSTR__idstr );
    idcpy( &cid->internalid, pobid );
    // Populate prefix field
    const char *data = CStringValue( cid->identifier.CSTR__idstr );
    size_t sz = CStringLength( cid->identifier.CSTR__idstr );
    if( sz > sz_prefix ) {
      sz = sz_prefix;
    }
    memcpy( cid->identifier.idprefix.data, data, sz );
    cid->identifier.idprefix.data[ sz ] = '\0';
  }
  return cid->identifier.CSTR__idstr;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * vxvertex_object__vertex_identifiers_set_id( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id ) {
  return vxvertex_object__vertex_identifiers_set_id_len( identifiers, i, id, (int)strlen( id ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * vxvertex_object__vertex_identifiers_set_id_len( vgx_VertexIdentifiers_t *identifiers, int64_t i, const char *id, int len ) {
  const char *idstring = NULL;
  if( len > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
    return NULL;
  }
  CString_t *CSTR__id = NewEphemeralCStringLen( identifiers->graph, id, len, 0 ); 
  if( CSTR__id ) {
    CString_t *CSTR__instance = vxvertex_object__vertex_identifiers_set_cstring( identifiers, i, CSTR__id );
    if( CSTR__instance ) {
      idstring = CStringValue( CSTR__instance );
    }
    icstringobject.DecrefNolock( CSTR__id );
  }
  return idstring;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t vxvertex_object__vertex_identifiers_set_obid( vgx_VertexIdentifiers_t *identifiers, int64_t i, const objectid_t *obid ) {
  vgx_VertexCompleteIdentifier_t *cid = &identifiers->ids[ i ];
  iString.Discard( &cid->identifier.CSTR__idstr );
  *cid->identifier.idprefix.qwords = 0;
  objectid_t *pobid = idcpy( &cid->internalid, obid );
  return *pobid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const vgx_Vertex_t * vxvertex_object__vertex_identifiers_set_vertex( vgx_VertexIdentifiers_t *identifiers, int64_t i, const vgx_Vertex_t *vertex ) {
  vgx_VertexCompleteIdentifier_t *cid = &identifiers->ids[ i ];
  if( vertex ) {
    // Clone CString if vertex has one
    if( vertex->identifier.CSTR__idstr ) {
      if( (cid->identifier.CSTR__idstr = CStringCloneAlloc( vertex->identifier.CSTR__idstr, identifiers->graph->ephemeral_string_allocator_context )) == NULL ) {
        return NULL;
      }
    }
    else {
      cid->identifier.CSTR__idstr = NULL;
    }
    // Copy prefix
    cid->identifier.idprefix = vertex->identifier.idprefix;
    // Copy internalid
    idcpy( &cid->internalid, __vertex_internalid( vertex ) );
  }
  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t vxvertex_object__vertex_identifiers_size( const vgx_VertexIdentifiers_t *identifiers ) {
  return identifiers->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __has_duplicate( vgx_VertexIdentifiers_t *identifiers, int64_t n, const objectid_t *probe ) {
  for( int64_t i=0; i<n; i++ ) {
    objectid_t obid = vxvertex_object__vertex_identifiers_get_obid( identifiers, i );
    if( idmatch( &obid, probe ) ) {
      return true; // duplicate found
    }
  }
  return false; // no duplicate
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool vxvertex_object__vertex_identifiers_check_unique( vgx_VertexIdentifiers_t *identifiers ) {
#define BruteForceCutoff 4
#define OrderOffset 4
#define MallocQwordCutoff 8

  int64_t sz = identifiers->sz;

  // In trivial cases O(N^2) brute force is simple and fast
  if( sz <= BruteForceCutoff ) {
    for( int64_t i=0; i<sz; i++ ) {
      objectid_t obid = vxvertex_object__vertex_identifiers_get_obid( identifiers, i );
      if( __has_duplicate( identifiers, i, &obid ) ) {
        return false;
      }
    }
    return true;
  }

  // Use bitvectors / bloom filter 

  bool unique = true;

  int order = imag2( sz ) + OrderOffset;
  size_t bitselect = (1ULL << order) - 1;
  size_t nqwords = 1ULL << (order-6);


  QWORD local[ MallocQwordCutoff ];
  QWORD *bitvector = local;
  if( nqwords > MallocQwordCutoff ) {
    if( (bitvector = calloc( nqwords, sizeof(QWORD) )) == NULL ) {
      return false;
    }
  }
  else {
    memset( local, 0, sizeof(local) );
  }

  for( int64_t i=0; i<sz; i++ ) {
    objectid_t obid = vxvertex_object__vertex_identifiers_get_obid( identifiers, i );

    // four bit locations determined by obid
    size_t b0 =  obid.L      & bitselect;
    size_t b1 = (obid.L>>32) & bitselect;
    size_t b2 =  obid.H      & bitselect;
    size_t b3 = (obid.H>>32) & bitselect;

    // select four target qwords in the bitvector
    QWORD *y0 = bitvector + (b0 >> 6);
    QWORD *y1 = bitvector + (b1 >> 6);
    QWORD *y2 = bitvector + (b2 >> 6);
    QWORD *y3 = bitvector + (b3 >> 6);

    // single bitmask within each selected qword
    QWORD m0 = 1ULL << (b0 & 0x3f);
    QWORD m1 = 1ULL << (b1 & 0x3f);
    QWORD m2 = 1ULL << (b2 & 0x3f);
    QWORD m3 = 1ULL << (b3 & 0x3f);

    // check if all three bit positions are already set
    if( (*y0 & m0) && (*y1 & m1) && (*y2 & m2) && (*y3 & m3) ) {
      // all three bit positions already set, possible duplicate
      if( __has_duplicate( identifiers, i, &obid ) ) {
        unique = false;
        break;
      }
    }

    // set four bit positions
    *y0 |= m0;
    *y1 |= m1;
    *y2 |= m2;
    *y3 |= m3;
  }

  if( bitvector != local ) {
    free( bitvector );
  }

  return unique;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Vertex_cmpid( const vgx_Vertex_t *self, const void *idptr ) {
  return idcmp( Vertex_getid( (vgx_Vertex_t*)self), (const objectid_t*)idptr );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static objectid_t * Vertex_getid( vgx_Vertex_t *self ) {
  return __vertex_internalid( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_serialize( vgx_Vertex_t *self, CQwordQueue_t *output ) {
  cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( self );
  if( CALLABLE( output )->WriteNolock( output, &handle.qword, 1 ) != 1 ) {
    return -1;
  }
  return 1; // 1 QWORD written: the handle
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Vertex_t * Vertex_deserialize( comlib_object_t *container, CQwordQueue_t *input ) {
  cxmalloc_handle_t vertex_handle;
  if( CALLABLE( input )->NextNolock( input, &vertex_handle.qword ) == 1 ) {
    return ivertexobject.FromHandleNolock( vertex_handle ); // will incref
  }
  return NULL;
}



/*******************************************************************//**
 *
 * CS
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Vertex_constructor( const void *internalid, vgx_Vertex_constructor_args_t *args ) {
  if( args == NULL ) {
    return NULL;
  }
  
  vgx_Graph_t *graph = args->graph;
  vgx_Vertex_t *self = NULL;
  
  XTRY {
    //  FIRST CACHELINE
    // +----------------+----------------+--------+--------+--------+--------+
    // |  128-bit ID    |  (allocator)   | vtable | obtype |   op   | descr. |
    // +----------------+----------------+--------+--------+--------+--------+
    //                                   ^
    //                                   |  (vgx_VertexHead_t)
    //                            (vgx_Vertex_t *self )

    object_allocator_context_t *prop_alloc_ctx = graph->property_allocator_context;
    size_t sz_id = 0;
    objectid_t obid;

    // Validate vertex ID
    if( args->CSTR__idstring ) {
      sz_id = CStringLength( args->CSTR__idstring );
      if( sz_id > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
        args->CSTR__error = CStringNewFormat( "Vertex identifier too long (%llu), max length is %d", sz_id, _VXOBALLOC_CSTRING_MAX_LENGTH );
        THROW_SILENT( CXLIB_ERR_CAPACITY, 0x401 );
      }
      
      if( args->CSTR__idstring->allocator_context == NULL ) {
        args->CSTR__error = CStringNewFormat( "Vertex identifier has no allocator context: %s", CStringValue( args->CSTR__idstring ) );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x402 );
      }
    }

    // Determine the vertex objectid
    if( internalid ) {
      obid = *((objectid_t*)internalid);    // 128-bit internalid supplied from the outside
    }
    else if( args->CSTR__idstring ) {
      obid = *CStringObid( args->CSTR__idstring );   // no 128-bit internalid, use id string's obid
    }
    else {
      THROW_ERROR( CXLIB_ERR_API, 0x403 );  // can't construct Vertex if we don't supply any id info
    }


    // Allocate new vertex - ID is set and data is initialized to zero
    if( (self = ivertexobject.New( graph, &obid )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x404 );
    }

    // Init Operation
    iOperation.InitId( &self->operation, 0 );     // No operation yet, will be assigned when vertex is committed

    // Init Descriptor
    self->descriptor = __vgx_default_vertex_descriptor( args->manifestation, args->vxtype );

    //  SECOND CACHELINE                                                     THIRD CACHELINE
    // +--------+--------+--------+----------------+----------------+--------+----------------------------------------------------------------+
    // | *graph | *vector|  rank  |     inarcs     |    outarcs     | prop.  |                            data512                             |
    // +--------+--------+--------+----------------+----------------+--------+----------------------------------------------------------------+
    // ^
    // |
    // (vgx_VertexData_t *vertex_data)

    // Rank
    self->rank = args->rank;

    // Isolated
    iarcvector.SetNoArc( &self->inarcs );
    iarcvector.SetNoArc( &self->outarcs );
    __vertex_set_isolated( self );

    // Identifier
    if( args->CSTR__idstring ) {
      // Large string, we need extra storage
      const size_t sz_prefix = sizeof( vgx_VertexIdentifierPrefix_t ) - 1;
      const char *id_string = CStringValue( args->CSTR__idstring );
      char *prefix = self->identifier.idprefix.data;
      size_t sz_embedded;
      if( sz_id > sz_prefix ) {
        // Own additional reference to string object (if already using property allocator) or clone the string object
        if( (self->identifier.CSTR__idstr = OwnOrCloneCString( args->CSTR__idstring, prop_alloc_ctx )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x405 );
        }
        // Embed the maximum number of prefix characters
        sz_embedded = sz_prefix;
      }
      // Small string, store entire in embedded
      else {
        // No extra string needed to store the ID
        self->identifier.CSTR__idstr = NULL;
        // Embed the entire ID
        sz_embedded = sz_id;
      }
      // Set the ID prefix in the embedded vertex space
      memcpy( prefix, id_string, sz_embedded );
      prefix[ sz_embedded ] = '\0';
    }
    else {
      idtostr( self->identifier.idprefix.data, Vertex_getid(self) );
      self->identifier.CSTR__idstr = NULL;
    }

    // Vertex time attributes (resolution is 1 second)
    self->TMC = args->ts;
    self->TMM = self->TMC;
    self->TMX.arc_ts = TIME_EXPIRES_NEVER;
    __vertex_clear_expiration( self );
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
 *
 *
 ***********************************************************************
 */
static void Vertex_destructor( vgx_Vertex_t *self ) {
  Vertex_DECREF_WL( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Graph_t * Vertex_parent( vgx_Vertex_t *self_RO ) {
  return self_RO->graph;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vertex_readonly( const vgx_Vertex_t *self_OPEN ) {
  return __vertex_is_locked_readonly( self_OPEN );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vertex_readable( const vgx_Vertex_t *self_OPEN ) {
  return __vertex_is_locked_readonly( self_OPEN ) || __vertex_is_locked_writable_by_current_thread( self_OPEN );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vertex_writable( const vgx_Vertex_t *self_OPEN ) {
  return __vertex_is_locked_writable_by_current_thread( self_OPEN );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Rank_t Vertex_set_rank( vgx_Vertex_t *self_WL, float c1, float c0 ) {
  vgx_RankSetC1( &self_WL->rank, c1 );
  vgx_RankSetC0( &self_WL->rank, c0 );
  // Capture
  iOperation.Vertex_WL.SetRank( self_WL, &self_WL->rank );
  return self_WL->rank;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Rank_t Vertex_get_rank( const vgx_Vertex_t *self_RO ) {
  return self_RO->rank;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_degree( const vgx_Vertex_t *self_RO ) {
  int64_t i=0, o=0;
  i = iarcvector.Degree( &self_RO->inarcs );
  o = iarcvector.Degree( &self_RO->outarcs );
  if( i >= 0 && o >= 0 ) {
    return i + o;
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
static int64_t Vertex_indegree( const vgx_Vertex_t *self_RO ) {
  return iarcvector.Degree( &self_RO->inarcs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_outdegree( const vgx_Vertex_t *self_RO ) {
  return iarcvector.Degree( &self_RO->outarcs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_remove_outarcs( vgx_Vertex_t *self_WL ) {
  int64_t n_outarcs_removed = 0;
  if( __vertex_has_outarcs( self_WL ) ) {
    vgx_Graph_t *graph = self_WL->graph;
    vgx_Arc_t any_outarcs = {
      .tail = self_WL,
      .head = {
        .vertex       = NULL,
        .predicator   = VGX_PREDICATOR_ANY_OUT
      }
    };
    vgx_ExecutionTimingBudget_t infinite = _vgx_get_infinite_execution_timing_budget();
    n_outarcs_removed = iarcvector.Remove( &graph->arcvector_fhdyn, &any_outarcs, &infinite, _vxgraph_arc__disconnect_WL_reverse_GENERAL ); // <= Assume: self is WL
    __vertex_clear_has_outarcs( self_WL );

    // Capture
    iOperation.Vertex_WL.RemoveOutarcs( self_WL, n_outarcs_removed );
  }
  return n_outarcs_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_remove_inarcs( vgx_Vertex_t *self_WL ) {
  int64_t n_inarcs_removed = 0;
  if( __vertex_has_inarcs( self_WL ) ) {
    vgx_Graph_t *graph = self_WL->graph;
    vgx_Arc_t any_inarcs = {
      .tail = self_WL,
      .head = {
        .vertex       = NULL,
        .predicator = VGX_PREDICATOR_ANY_IN
      }
    };
    vgx_ExecutionTimingBudget_t infinite = _vgx_get_infinite_execution_timing_budget();
    n_inarcs_removed = iarcvector.Remove( &graph->arcvector_fhdyn, &any_inarcs, &infinite, _vxgraph_arc__disconnect_iWL_forward_GENERAL );  // <= Assume: self is WL
    __vertex_clear_has_inarcs( self_WL );
    
    // Capture
    iOperation.Vertex_WL.RemoveInarcs( self_WL, n_inarcs_removed );
  }
  return n_inarcs_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vertex_remove_arcs( vgx_Vertex_t *self_WL ) {
  int64_t n_outarcs_removed = Vertex_remove_outarcs( self_WL );
  int64_t n_inarcs_removed = Vertex_remove_inarcs( self_WL );
  return n_outarcs_removed + n_inarcs_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * __Vertex_collect_neighborhood( vgx_Vertex_t *self_RO, const vgx_ArcVector_cell_t *inarcs, const vgx_ArcVector_cell_t *outarcs ) {
  vgx_BaseCollector_context_t *result = NULL;

  vgx_Graph_t *graph = self_RO->graph;
  vgx_NeighborhoodQuery_t *query = iGraphQuery.NewNeighborhoodQuery( graph, CALLABLE( self_RO )->IDString( self_RO ), NULL, VGX_COLLECTOR_MODE_COLLECT_VERTICES, NULL );
  if( query == NULL ) {
    return NULL;
  }
  CALLABLE( query )->SetTimeout( query, 0, false );
  CALLABLE( query )->SetResponseFormat( query, VGX_RESPONSE_ATTR_ID );

  // Pass Filter
  vgx_GenericArcFilter_context_t pass_filter = {
    .type                           = VGX_ARC_FILTER_TYPE_PASS,
    .positive_match                 = true,
    .arcfilter_locked_head_access   = true,
    .eval_synarc                    = false,
    .traversing_evaluator           = NULL,
    .filter                         = arcfilterfunc.Pass,
    .timing_budget                  = vgx_query_timing_budget( (vgx_BaseQuery_t*)query ),
    .previous_context               = NULL,
    .current_tail                   = NULL,
    .current_head                   = NULL,
    .superfilter                    = NULL,
    .terminal                       = {
        .current                        = NULL,
        .list                           = NULL,
        .logic                          = VGX_LOGICAL_NO_LOGIC
    },
    .arcfilter_callback             = NULL,
    .logic                          = VGX_LOGICAL_NO_LOGIC,
    .pred_condition1                = VGX_PREDICATOR_ANY,
    .pred_condition2                = VGX_PREDICATOR_ANY,
    .vertex_probe                   = NULL,
    .culleval                       = NULL,
    .locked_cull                    = 0
  };

  // Ranking Context
  vgx_ranking_context_t ranking_context = {
    .sortspec           = VGX_SORTBY_NONE,
    .modifier           = VGX_PREDICATOR_MOD_WILDCARD,
    .graph              = graph,
    .readonly_graph     = false,
    .simcontext         = NULL,
    .vector             = NULL,
    .fingerprint        = 0,
    .evaluator          = NULL,
    .timing_budget      = vgx_query_timing_budget( (vgx_BaseQuery_t*)query ),
    .postfilter_context = NULL
  };

  // Degree
  int64_t d = 0;
  if( inarcs ) {
    d += iarcvector.Degree( inarcs );
  }
  if( outarcs ) {
    d += iarcvector.Degree( outarcs );
  }
  
  vgx_collect_counts_t counts = {
    .data_size  = d,
    .n_collect  = d,
    .hits       = d,
    .offset     = 0
  };

  // Vertex Collector
  vgx_VertexCollector_context_t *vertex_collector;
  if( (vertex_collector = iGraphCollector.NewVertexCollector( graph, &ranking_context, (vgx_BaseQuery_t*)query, &counts )) != NULL ) {

    // Neighborhood Probe
    vgx_neighborhood_probe_t probe = {
      .graph                  = graph,
      .conditional = {
        .arcfilter              = NULL,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        },
      },
      .traversing = {
        .arcfilter              = (vgx_virtual_ArcFilter_context_t*)&pass_filter,
        .vertex_probe           = NULL,
        .arcdir                 = VGX_ARCDIR_ANY,
        .evaluator              = NULL,
        .override               = {
            .enable                 = false,
            .match                  = VGX_ARC_FILTER_MATCH_MISS
        },
      },
      .distance               = 1,
      .readonly_graph         = false,  // ????
      .collector_mode         = VGX_COLLECTOR_MODE_COLLECT_VERTICES,
      .current_tail_RO        = self_RO,
      .common_collector       = (vgx_BaseCollector_context_t*)vertex_collector,
      .pre_evaluator          = NULL,
      .post_evaluator         = NULL,
      .collect_filter_context = (vgx_virtual_ArcFilter_context_t*)&pass_filter,
    };
    
    vgx_ArcFilter_match match_in = VGX_ARC_FILTER_MATCH_HIT;
    vgx_ArcFilter_match match_out = VGX_ARC_FILTER_MATCH_HIT;

    // Execute
    if( inarcs ) {
      match_in = iarcvector.GetVertices( inarcs, &probe );
    }
    if( outarcs ) {
      match_out = iarcvector.GetVertices( outarcs, &probe );
    }

    if( __is_arcfilter_error( match_in )
        ||
        __is_arcfilter_error( match_out )
        ||
        (result = iGraphCollector.ConvertToBaseListCollector( (vgx_BaseCollector_context_t*)vertex_collector )) == NULL )
    {
      iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&vertex_collector );
    }

    iGraphQuery.DeleteNeighborhoodQuery( &query );
  }

  return result;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * Vertex_neighbors( vgx_Vertex_t *self_RO ) {
  return __Vertex_collect_neighborhood( self_RO, &self_RO->inarcs, &self_RO->outarcs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * Vertex_initials(  vgx_Vertex_t *self_RO ) {
  return __Vertex_collect_neighborhood( self_RO, &self_RO->inarcs, NULL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_BaseCollector_context_t * Vertex_terminals( vgx_Vertex_t *self_RO ) {
  return __Vertex_collect_neighborhood( self_RO, NULL, &self_RO->outarcs );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static size_t Vertex_id_length( const vgx_Vertex_t *self_RO ) {
  if( self_RO->identifier.CSTR__idstr ) {
    return CStringLength( self_RO->identifier.CSTR__idstr );
  }
  else {
    return strnlen( self_RO->identifier.idprefix.data, sizeof( vgx_VertexIdentifierPrefix_t ) );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const vgx_VertexIdentifier_t * Vertex_identifier( const vgx_Vertex_t *self_RO ) {
  return &self_RO->identifier;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * Vertex_id_string( const vgx_Vertex_t *self_RO ) {
  if( self_RO->identifier.CSTR__idstr ) {
    return CStringValue( self_RO->identifier.CSTR__idstr );
  }
  else {
    return self_RO->identifier.idprefix.data;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * Vertex_id_prefix( const vgx_Vertex_t *self_RO ) {
  return self_RO->identifier.idprefix.data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * Vertex_id_cstring( const vgx_Vertex_t *self_RO ) {
  object_allocator_context_t *context = self_RO->graph->ephemeral_string_allocator_context;
  if( self_RO->identifier.CSTR__idstr ) {
    return CStringCloneAlloc( self_RO->identifier.CSTR__idstr, context );
  }
  else {
    return CStringNewAlloc( self_RO->identifier.idprefix.data, context );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t Vertex_internal_id( const vgx_Vertex_t *self_RO ) {
  objectid_t obid;
  idcpy( &obid, Vertex_getid( (vgx_Vertex_t*)self_RO ) );
  return obid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const vgx_vertex_type_t Vertex_type( const vgx_Vertex_t *self_RO ) {
  return self_RO->descriptor.type.enumeration;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * Vertex_type_name( const vgx_Vertex_t *self_RO ) {
  // NOTE: We must ensure synchronized access. We may or may not hold the graph state lock, so lock it again to be sure.
  return _vxenum_vtx__decode_OPEN( self_RO->graph, self_RO->descriptor.type.enumeration );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const CString_t * Vertex_type_name_CS( const vgx_Vertex_t *self_RO ) {
  return _vxenum_vtx__decode_CS( self_RO->graph, self_RO->descriptor.type.enumeration );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vertex_is_stable_CS( const vgx_Vertex_t *self_OPEN ) {
  return _cxmalloc_is_object_active( self_OPEN );
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_set_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__set_property_WL( self_WL, prop );
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_set_property_key_val( vgx_Vertex_t *self_WL, const char *key, vgx_value_t *value ) {
  int ret = 0;
  vgx_VertexProperty_t prop = {0};
  vgx_Graph_t *graph = self_WL->graph;
  
  if( (prop.key = iEnumerator_OPEN.Property.Key.New( graph, key )) == NULL ) {
    return -1;
  }

  prop.val.type = value->type;
  prop.val.data.bits = value->data.bits;
  
  ret = _vxvertex_property__set_property_WL( self_WL, &prop );

  iString.Discard( &prop.key );

  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_set_int_property( vgx_Vertex_t *self_WL, const char *key, int64_t intval ) {
  int ret = -1;
  vgx_value_t value = {
    .type = VGX_VALUE_TYPE_INTEGER,
    .data = { .simple.integer = intval }
  };
  ret = Vertex_set_property_key_val( self_WL, key, &value );
  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_set_real_property( vgx_Vertex_t *self_WL, const char *key, double realval ) {
  int ret = -1;
  vgx_value_t value = {
    .type = VGX_VALUE_TYPE_REAL,
    .data = { .simple.real = realval }
  };
  ret = Vertex_set_property_key_val( self_WL, key, &value );
  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_set_string_property( vgx_Vertex_t *self_WL, const char *key, const char *strval ) {

  CString_t *CSTR__value = NewEphemeralCString( self_WL->graph, strval );
  if( CSTR__value == NULL ) {
    return -1;
  }

  vgx_value_t value = {
    .type = VGX_VALUE_TYPE_ENUMERATED_CSTRING,
    .data = { .simple.CSTR__string = CSTR__value }
  };
  
  int ret = Vertex_set_property_key_val( self_WL, key, &value );

  iString.Discard( &CSTR__value );

  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 if property added or changed, 0 if no change, -1 on failure
 ***********************************************************************
 */
static int Vertex_format_string_property( vgx_Vertex_t *self_WL, const char *key, const char *valfmt, ... ) {
  char buffer[ 4096 ];

  va_list args;
  va_start( args, valfmt );
  int nw = vsnprintf( buffer, 4095, valfmt, args );
  va_end( args );

  if( nw > 4094 || nw < 0 ) {
    return -1;
  }

  return Vertex_set_string_property( self_WL, key, buffer );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Vertex_set_on_delete( vgx_Vertex_t *self_WL, vgx_VertexOnDeleteAction action, const char *data ) {
  static const char key[] = VGX_VERTEX_ONDELETE_PROPERTY;

  CString_t *CSTR__callback = CStringNewFormatAlloc( self_WL->graph->ephemeral_string_allocator_context, "%08llx%s", (unsigned int)action, data ? data : "" );
  if( CSTR__callback == NULL ) {
    return -1;
  }

  vgx_value_t value = {
    .type = VGX_VALUE_TYPE_ENUMERATED_CSTRING,
    .data = { .simple.CSTR__string = CSTR__callback }
  };

  int ret = Vertex_set_property_key_val( self_WL, key, &value );

  iString.Discard( &CSTR__callback );

  return ret;
}



/*******************************************************************//**
 *
 * Returns: Pointer to the passed property container on successful
 * increment or creation of numeric property value, or NULL on failure.
 ***********************************************************************
 */
static vgx_VertexProperty_t * Vertex_inc_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__inc_property_WL( self_WL, prop );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int32_t Vertex_enum( vgx_Vertex_t *self_LCK ) {
  return _vxvertex_property__vertex_enum_LCK( self_LCK );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * Vertex_get_internal_attribute( vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__get_internal_attribute_RO( self_RO, prop );
}



/*******************************************************************//**
 *
 * Returns: Pointer to the passed property container on success, NULL on failure.
 * WARNING: Caller already owns the property instance, and also owns a new reference
 * to prop->value if prop->type is set to the enumerated string (CString)
 * Client must decref prop->value before freeing the property container.
 ***********************************************************************
 */
static vgx_VertexProperty_t * Vertex_get_property( vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__get_property_RO( self_RO, prop );
}



/*******************************************************************//**
 *
 * WARNING: If the retrieved property value is enumerated CString the 
 *          caller OWNS A REFERENCE to the string and must decref it.
 *
 ***********************************************************************
 */
static int Vertex_get_property_value( vgx_Vertex_t *self_RO, const char *key, vgx_value_t *rvalue ) {
  int ret = 0;

  vgx_VertexProperty_t prop = {0};
  vgx_Graph_t *graph = self_RO->graph;
  
  if( (prop.key = iEnumerator_OPEN.Property.Key.New( graph, key )) == NULL ) {
    return -1;
  }

  if( _vxvertex_property__get_property_RO( self_RO, &prop ) != NULL ) {
    *rvalue = prop.val;
    if( prop.val.type != VGX_VALUE_TYPE_NULL ) {
      ret = 1; // found
    }
  }
  else {
    ret = -1; // internal error
  }

  iString.Discard( &prop.key );

  return ret;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int Vertex_get_int_property( vgx_Vertex_t *self_RO, const char *key, int64_t *intval ) {
  vgx_value_t val = {0};
  if( Vertex_get_property_value( self_RO, key, &val ) > 0 && val.type == VGX_VALUE_TYPE_INTEGER ) {
    *intval = val.data.simple.integer;
    return 0;
  }
  else {
    if( val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
      _vxenum_propval__discard_value_OPEN( self_RO->graph, val.data.simple.CSTR__string );
    }
    return -1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int Vertex_get_real_property( vgx_Vertex_t *self_RO, const char *key, double *realval ) {
  vgx_value_t val = {0};
  if( Vertex_get_property_value( self_RO, key, &val ) > 0 && val.type == VGX_VALUE_TYPE_REAL ) {
    *realval = val.data.simple.real;
    return 0;
  }
  else {
    if( val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
      _vxenum_propval__discard_value_OPEN( self_RO->graph, val.data.simple.CSTR__string );
    }
    return -1;
  }
}



/*******************************************************************//**
 *
 * WARNING: Caller owns a reference to returned cstring
 *
 ***********************************************************************
 */
static int Vertex_get_string_property( vgx_Vertex_t *self_RO, const char *key, CString_t **CSTR__strval ) {
  vgx_value_t val = {0};
  if( Vertex_get_property_value( self_RO, key, &val ) > 0 && val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
    *CSTR__strval = val.data.simple.CSTR__string;
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * Returns: true if property exists, false if not.
 ***********************************************************************
 */
static bool Vertex_has_property( const vgx_Vertex_t *self_RO, const vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__has_property_RO( self_RO, prop );
}



/*******************************************************************//**
 *
 * Returns: true if property exists, false if not.
 ***********************************************************************
 */
static bool Vertex_has_property_key( const vgx_Vertex_t *self_RO, const char *key ) {
  vgx_VertexProperty_t prop = {0};
  prop.keyhash = CharsHash64( key );
  return _vxvertex_property__has_property_RO( self_RO, &prop );
}



/*******************************************************************//**
 *
 * Returns: true if vertex has properties, false if not
 ***********************************************************************
 */
static bool Vertex_has_properties( vgx_Vertex_t *self_RO ) {
  return _vxvertex_property__num_properties_RO( self_RO ) > 0;
}



/*******************************************************************//**
 *
 * Returns: number of properties
 ***********************************************************************
 */
static int64_t Vertex_num_properties( vgx_Vertex_t *self_RO ) {
  return _vxvertex_property__num_properties_RO( self_RO );
}



/*******************************************************************//**
 *
 * Returns: New allocation of all vertex properties, or NULL on failure.
 *          The returned properties have the same length as the total
 *          number of properties for this vertex. Key are populated
 *          positionally as they appear internally in the vertex (i.e.
 *          arbitrary order.) 
 *
 * NOTE:    Caller owns the return value and must use
 *          iVertexProperty.FreeSelectProperties to discard it.
 *         
 ***********************************************************************
 */
static vgx_SelectProperties_t * Vertex_get_properties( vgx_Vertex_t *self_RO ) {
  return _vxvertex_property__get_properties_RO( self_RO );
}



/*******************************************************************//**
 *         
 * Returns: 1 if named property was removed, 0 if it didn't exist, -1 on failure
 ***********************************************************************
 */
static int Vertex_remove_property( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  return _vxvertex_property__del_property_WL( self_WL, prop );
}



/*******************************************************************//**
 *         
 * Returns: 1 if named property was removed, 0 if it didn't exist, -1 on failure
 ***********************************************************************
 */
static int Vertex_remove_property_key( vgx_Vertex_t *self_WL, const char *key ) {
  vgx_VertexProperty_t prop = {0};
  prop.keyhash = CharsHash64( key );
  return _vxvertex_property__del_property_WL( self_WL, &prop );
}



/*******************************************************************//**
 *
 * Returns: The number of removed properties
 ***********************************************************************
 */
static int64_t Vertex_remove_properties( vgx_Vertex_t *self_WL ) {
  return _vxvertex_property__del_properties_WL( self_WL );
}



/*******************************************************************//**
 *
 * Returns: true if vertex has expired, false otherwise
 ***********************************************************************
 */
static bool Vertex_is_expired( const vgx_Vertex_t *self_RO ) {
  return _vgx_graph_seconds( self_RO->graph ) > __vertex_get_expiration_ts( self_RO );
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint32_t Vertex_TMC( const vgx_Vertex_t *self_RO ) {
  return self_RO->TMC;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint32_t Vertex_TMM( const vgx_Vertex_t *self_RO ) {
  return self_RO->TMM;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint32_t Vertex_get_TMX( const vgx_Vertex_t *self_RO ) {
  return __vertex_get_expiration_ts( self_RO );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Vertex_set_TMX( vgx_Vertex_t *self_WL, uint32_t new_tmx ) {
  int ret = 0;
  
  if( new_tmx == 0 ) {
    new_tmx = TIME_EXPIRES_NEVER;
  }

  // Update tmx if different from existing
  uint32_t prev_tmx = __vertex_get_expiration_ts( self_WL );
  if( new_tmx != prev_tmx ) {
    // Set expiration
    if( new_tmx < TIME_EXPIRES_NEVER ) {
      __vertex_set_expiration_ts( self_WL, new_tmx );
      // Schedule event if it occurs earlier than any arc expiration
      if( new_tmx < __vertex_arcvector_get_expiration_ts( self_WL ) ) {
        ret = iGraphEvent.ScheduleVertexEvent.Expiration_WL( self_WL->graph, self_WL, new_tmx );
      }
    }
    // Clear vertex expiration
    else {
      __vertex_clear_expiration( self_WL );
      uint32_t arc_tmx = __vertex_arcvector_get_expiration_ts( self_WL );
      // Re-schedule next event to occur at arc expiration
      if( arc_tmx < TIME_EXPIRES_NEVER ) {
        ret = iGraphEvent.ScheduleVertexEvent.Expiration_WL( self_WL->graph, self_WL, arc_tmx );
      }
      // Cancel event if no expiring arcs
      else {
        iGraphEvent.CancelVertexEvent.RemoveSchedule_WL( self_WL->graph, self_WL );
      }
    }
    // Capture
    iOperation.Vertex_WL.SetTMX( self_WL, new_tmx );
  }

  __check_vertex_consistency_WL( self_WL );

  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 if vector was set or changed, 0 if no change, -1 on failure
 *
 * NOTE: The vector instance may be stolen if doing so is more efficient
 *       than owning another reference. The supplied pointer to pointer
 *       will be set to NULL if stealing occurs. The caller must check
 *       the supplied pointer pointer after the call returns to see
 *       whether stealing occurred.
 ***********************************************************************
 */
static int Vertex_set_vector( vgx_Vertex_t *self_WL, vgx_Vector_t **pvector ) {

  // Same vector being set, no change
  if( self_WL->vector == *pvector ) {
    return 0;
  }

  vgx_Graph_t *graph = self_WL->graph;

  // Set and steal new vector
  if( *pvector ) {
    vgx_Vector_t *vector;

    // The vector is ephemeral, we need to clone it with the persistent allocator
    if( (*pvector)->metas.flags.eph ) {
      if( (vector = CALLABLE( *pvector )->Clone( *pvector, false )) == NULL ) {
        return -1;
      }
    }
    // Otherwise STEAL the supplied instance
    else {
      vector = *pvector;
      *pvector = NULL;
    }

    // Discard any existing vector
    if( self_WL->vector ) {
      CALLABLE( self_WL->vector )->Decref( self_WL->vector );
    }
    // No previous vector
    else {
      // Increment global vector counter
      IncGraphVectorCount( graph );
    }

    // Assign
    self_WL->vector = vector;

    // Mark vertex as having a vector
    __vertex_set_has_vector( self_WL );

    // Capture
    iOperation.Vertex_WL.SetVector( self_WL, self_WL->vector );
  }
  // Null vector, mark vertex as not having a vector
  else {
    // Discard any existing vector
    if( self_WL->vector ) {
      CALLABLE( self_WL->vector )->Decref( self_WL->vector );
      // Decrement global vector counter
      DecGraphVectorCount( graph );
    }

    // Clear flag
    __vertex_clear_has_vector( self_WL );

    // Capture
    iOperation.Vertex_WL.DelVector( self_WL );
  }

  return 1;
}



/*******************************************************************//**
 *
 * Returns: Pointer to vector if it exists, NULL if it does not
 ***********************************************************************
 */
static vgx_Vector_t * Vertex_get_vector( vgx_Vertex_t *self_RO ) {
  return self_RO->vector;
}



/*******************************************************************//**
 *
 * Returns: 1 if vector removed, 0 if it didn't exist, -1 on failure
 ***********************************************************************
 */
static int Vertex_remove_vector( vgx_Vertex_t *self_WL ) {
  if( self_WL->vector ) {
    // Capture
    iOperation.Vertex_WL.DelVector( self_WL );

    // Decrement global counter
    DecGraphVectorCount( self_WL->graph );

    CALLABLE( self_WL->vector )->Decref( self_WL->vector );
    self_WL->vector = NULL;
    __vertex_clear_has_vector( self_WL );
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __execute_ondelete_event_CS( vgx_Vertex_t *self_WL ) {
  vgx_VertexProperty_t ondelete_prop = {
    .key        = g_CSTR__ondelete_key,
    .keyhash    = g_ondelete_keyhash,
    .val        = {0},
    .condition  = {0}
  };

  if( _vxvertex_property__get_property_RO( self_WL, &ondelete_prop ) ) {
    if( ondelete_prop.val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
      const char *p = CStringValue( ondelete_prop.val.data.simple.CSTR__string );
      // Parse first 8 chars as index into ondelete callback function lookup table.
      // Action command is any remaining string past the 8 hex digits.
      vgx_VertexOnDeleteAction action = VertexOnDeleteAction_None;
      const char *data = hex_to_DWORD( p, (DWORD*)&action );
      iString.Discard( &ondelete_prop.val.data.simple.CSTR__string );
      // Execute
      switch( action ) {
      case VertexOnDeleteAction_RemoveFile:
        __ondelete_remove_file( self_WL, data );
        break;
      case VertexOnDeleteAction_LogMessage:
        __ondelete_log_message( self_WL, data );
        break;
      default:
        __ondelete_noop( self_WL, data );
      }
    }
    _vxvertex_property__del_property_WL( self_WL, &ondelete_prop );
  }
}



/*******************************************************************//**
 * This function will break down a vertex that will leave it in an
 * initialized state. All arcs will be removed, which means that the
 * vertex will become an isolated vertex in the parent graph. If the
 * graph has indexed the vertex the graph is responsible for removing
 * the vertex from its index. (This function does not perform un-indexing.)
 *
 * The vertex refcount will be affected if any arc removal is performed.
 * However, the vertex will not be un-indexed here, so if the vertex is
 * still indexed then the graph is responsible for unindexing.
 *
 * As long as the refcount does not go to zero, the vertex memory will remain
 * valid. If the refcount goes to zero after removing arcs, the memory will be
 * invalid. This should not normally happen since removal from the index
 * will not occur until all arcs have been removed, and if they are removed
 * here the graph would not have had time to un-index the vertex yet.
 *
 * Upon completion of this function the vertex will have no data and
 * be in a NULL state.
 *
 ***********************************************************************
 */
static void __deconstruct_CS( vgx_Vertex_t *self_WL ) {
  if( self_WL ) {
    vgx_Graph_t *graph = self_WL->graph;

    if( self_WL->properties ) {
      __execute_ondelete_event_CS( self_WL );
    }

    // [14] CSTR__idstr
    if( self_WL->identifier.CSTR__idstr ) {
      icstringobject.DecrefNolock( self_WL->identifier.CSTR__idstr );
      self_WL->identifier.CSTR__idstr = NULL;
    }
    *self_WL->identifier.idprefix.qwords = 0;

    // [6] descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_DEFUNCT
    __vertex_set_defunct( self_WL );


    GRAPH_SUSPEND_LOCK( graph ) {
      vgx_Vertex_vtable_t *iV = CALLABLE(self_WL);
      int64_t n_outarcs_removed = 0;
      int64_t n_inarcs_removed = 0;

#ifdef VGX_CONSISTENCY_CHECK
      int64_t expected_outarc_drop = iarcvector.Degree( &self_WL->outarcs );
      int64_t expected_inarc_drop = iarcvector.Degree( &self_WL->inarcs );
      int64_t refcnt_pre = Vertex_REFCNT_WL( self_WL );
      if( refcnt_pre == 0 ) {
        FATAL( 0x411, "%s: Refcount unexpectedly zero before deconstruct!", __FUNCTION__ );
      }
      if( refcnt_pre < (expected_outarc_drop + expected_inarc_drop) ) {
        FATAL( 0x412, "%s: Refcount unexpectedly low before deconstruct. Refcnt:%lld, Outdegree:%lld, Indegree:%lld", __FUNCTION__, refcnt_pre, expected_outarc_drop, expected_inarc_drop );
      }

      // [5] operation
      if( iOperation.IsOpen( &self_WL->operation ) ) {
        iOperation.Dump_CS( &self_WL->operation );
        FATAL( 0x413, "%s: Operation still open during deconstruct.", __FUNCTION__ );
      }
#endif

      // ===================
      // DESTROY VERTEX DATA
      // ===================
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x414, "%s: Refcount unexpectedly low during deconstruct. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif

      // --- Remove Properties ---
      // [12]  properties = NULL
      iV->RemoveProperties( self_WL );
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x415, "%s: Refcount unexpectedly low during deconstruct. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif


      // --- Remove Vector ---
      // [8]   vector = NULL
      // [6]   descriptor.property.vector = NONE
      iV->RemoveVector( self_WL );
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x416, "%s: Refcount unexpectedly low during deconstruct. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif


      // NOTE: We don't know what is expected in terms of graph state lock (we may or may not be inside a CS)
      // but we will assume the self vertex is write locked (WL)
      
      // --- Remove Outarcs ---
      // [11]  outarcs = NO ARCS
      // [6]   descriptor.property.degree.out = 0
      n_outarcs_removed = iV->RemoveOutarcs( self_WL );

      // --- Remove Inarcs ---
      // [10]  inarcs = NO ARCS
      // [6]   descriptor.property.degree.in = 0
      n_inarcs_removed = iV->RemoveInarcs( self_WL );
    } GRAPH_RESUME_LOCK;

    // ---------
    // INITIALIZE STATE
    // ---------
    // [6] descriptor.state.context.man = NULL
    __vertex_set_manifestation_null( self_WL );


    // Descriptor fields affected by initialization:
    // ----------------------------------------------------
    // DESCRIPTOR FIELD     |  Initialized  |  Preserved  |
    // ----------------------------------------------------
    // semaphore.count      |               |      Y      |
    // type.enumeration     | defunct 0x1D  |             |
    // state.context.man    |     null      |             |
    // state.context.sus    |               |      Y      |
    // state.lock.yib       |               |      Y      |
    // state.lock.iny       |               |      Y      |
    // state.lock.wrq       |               |      Y      |
    // state.lock.rwl       |               |      Y      |
    // state.lock.lck       |               |      Y      |
    // property.degree.in   |      0        |             |
    // property.degree.out  |      0        |             |
    // property.vector.ctr  |      0        |             |
    // property.vector.vec  |      0        |             |
    // property.scope.def   |               |      Y      |
    // property.event.sch   |               |      Y      |
    // property.index.type  |               |      Y      |
    // property.index.main  |               |      Y      |
    // writer.threadid      |               |      Y      |
    // ----------------------------------------------------


    // Things we PRESERVE when making vertex defunct:
    // [1] ID
    // [2] Allocator (refcnt etc)
    // [3] vtable
    // [4] typeinfo
    // [5] operation
    // [6] descriptor.semaphore.count 
    // [6] descriptor.state.lock.*
    // [6] descriptor.property.scope.def
    // [6] descriptor.property.event.sch
    // [6] descriptor.property.index.type
    // [6] descriptor.property.index.main
    // [6] descriptor.writer.threadid
    // [7] graph
    // [9] rank ???
    // [15] TMX
    // [16] TMC
    // [17] TMM
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __virtualize_CS( vgx_Vertex_t *self_WL ) {
  if( self_WL && __vertex_is_manifestation_real( self_WL ) ) {
    vgx_Graph_t *graph = self_WL->graph;

    if( self_WL->properties ) {
      __execute_ondelete_event_CS( self_WL );
    }

    GRAPH_SUSPEND_LOCK( graph ) {
      vgx_Vertex_vtable_t *iV = CALLABLE(self_WL);
      int64_t n_outarcs_removed = 0;

#ifdef VGX_CONSISTENCY_CHECK
      int64_t expected_outarc_drop = iarcvector.Degree( &self_WL->outarcs );
      int64_t refcnt_pre = Vertex_REFCNT_WL( self_WL );
      if( refcnt_pre == 0 ) {
        FATAL( 0x511, "%s: Refcount unexpectedly zero before virtualization!", __FUNCTION__ );
      }
      if( refcnt_pre < expected_outarc_drop ) {
        FATAL( 0x512, "%s: Refcount unexpectedly low before virtualization. Refcnt:%lld, Outdegree:%lld", __FUNCTION__, refcnt_pre, expected_outarc_drop );
      }

      // [5] operation
      if( iOperation.IsOpen( &self_WL->operation ) ) {
        iOperation.Dump_CS( &self_WL->operation );
        FATAL( 0x513, "%s: Operation still open during virtualization.", __FUNCTION__ );
      }
#endif

      // ===================
      // DESTROY VERTEX DATA
      // ===================
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x514, "%s: Refcount unexpectedly low during virtualization. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif

      // --- Remove Properties ---
      // [12]  properties = NULL
      iV->RemoveProperties( self_WL );
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x515, "%s: Refcount unexpectedly low during virtualization. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif


      // --- Remove Vector ---
      // [8]   vector = NULL
      // [6]   descriptor.property.vector = NONE
      iV->RemoveVector( self_WL );
#ifdef VGX_CONSISTENCY_CHECK
      if( Vertex_REFCNT_WL( self_WL ) != refcnt_pre ) {
        FATAL( 0x516, "%s: Refcount unexpectedly low during virtualization. refcnt:%lld, pre=%lld", __FUNCTION__, Vertex_REFCNT_WL( self_WL ), refcnt_pre  );
      }
#endif


      // NOTE: We don't know what is expected in terms of graph state lock (we may or may not be inside a CS)
      // but we will assume the self vertex is write locked (WL)
      
      // --- Remove Outarcs ---
      // [11]  outarcs = NO ARCS
      // [6]   descriptor.property.degree.out = 0
      n_outarcs_removed = iV->RemoveOutarcs( self_WL );

    } GRAPH_RESUME_LOCK;

    // ---------
    // SET TO VIRTUAL
    // ---------
    // [6] descriptor.state.context.man = VIRTUAL
    __vertex_set_manifestation_virtual( self_WL );

  }
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Vertex_representer( vgx_Vertex_t *self, CStringQueue_t *output ) {
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )
  char buf[512];
  COMLIB_DefaultRepresenter( (const comlib_object_t*)self, output );
  PUT( "\n" );

  vgx_Graph_t *graph = self->graph;
  const CString_t * CSTR__type = NULL;

  PUT( "<vgx_Vertex_t at %p (mem=%p) ", self, ivertexobject.AsAllocatedVertex( self ) );
  PUT( "identifier=%s ", Vertex_id_string( self ) );
  int64_t opid = 0;
  GRAPH_LOCK( graph ) {
    if( __vertex_is_locked_writable_by_current_thread( self ) || __vertex_is_locked_readonly( self ) ) {
      opid = iOperation.GetId_LCK( &self->operation );
    }
    CSTR__type = _vxenum_vtx__decode_CS( graph, self->descriptor.type.enumeration );
  } GRAPH_RELEASE;
  PUT( "operation={.id=%llu .dirty=%d} ", opid, iOperation.IsDirty( &self->operation ) );
  PUT( "descriptor.bits=0x%016llx >", self->descriptor.bits );
  PUT( "\n\n" );

  switch( self->descriptor.property.degree.bits ) {
  case VERTEX_PROPERTY_DEGREE_ISOLATED: PUT( "Isolated, " ); break;
  case VERTEX_PROPERTY_DEGREE_SOURCE:   PUT( "Source, " ); break;
  case VERTEX_PROPERTY_DEGREE_SINK:     PUT( "Sink, " ); break;
  case VERTEX_PROPERTY_DEGREE_INTERNAL: PUT( "Internal, " ); break;
  }
  switch( self->descriptor.state.context.man ) {
  case VERTEX_STATE_CONTEXT_MAN_NULL:     PUT( "NULL " ); break;
  case VERTEX_STATE_CONTEXT_MAN_REAL:     PUT( "REAL " ); break;
  case VERTEX_STATE_CONTEXT_MAN_VIRTUAL:  PUT( "VIRTUAL "); break;
  }
  time_t ts;
  struct tm *_tm;
  const CString_t * CSTR__name = CALLABLE(graph)->Name(graph);
  PUT( "vertex of type '%s' in graph '%s'\n", CStringValue( CSTR__type ), CStringValue( CSTR__name ) );
  PUT( "\n" );
  PUT( "ID=\"%s\"\n", Vertex_id_string( self ) );
  PUT( "Internalid=\"%s\"\n", idtostr( buf, Vertex_getid( self ) ) );
  PUT( "Indegree=%lld\n", iarcvector.Degree( &self->inarcs ) );
  PUT( "Outdegree=%lld\n", iarcvector.Degree( &self->outarcs ) );
  PUT( "\n" );
  ts = self->TMC;
  if( (_tm = localtime( &ts )) != NULL ) {
    strftime( buf, 20, "%Y-%m-%d %H:%M:%S", _tm ); 
    PUT( "TMC=%lu (%s)\n", self->TMC, buf );
  }
  ts = self->TMM;
  if( (_tm = localtime( &ts )) != NULL ) {
    strftime( buf, 20, "%Y-%m-%d %H:%M:%S", _tm ); 
    PUT( "TMM=%lu (%s)\n", self->TMM, buf );
  }
  ts = __vertex_get_expiration_ts( self );
  if( (_tm = localtime( &ts )) != NULL ) {
    strftime( buf, 20, "%Y-%m-%d %H:%M:%S", _tm ); 
    PUT( "TMX=%lu (%s)\n", ts, buf );
  }
  PUT( "\n" );
  PUT( "Descriptor details:\n" );
  PUT( "writer.threadid       = %lu\n",   self->descriptor.writer.threadid );
  PUT( "property.bits         = %s\n",    uint8_to_bin( buf, self->descriptor.property.bits ) );
  PUT( "        .index.bits   = 0x%x\n",  self->descriptor.property.index.bits );
  PUT( "              .type   = 0x%x\n",  self->descriptor.property.index.type );
  PUT( "              .main   = 0x%x\n",  self->descriptor.property.index.main );
  PUT( "        .event.sch    = 0x%x\n",  self->descriptor.property.event.sch );
  PUT( "        .scope.def    = 0x%x\n",  self->descriptor.property.scope.def );
  PUT( "        .vector.bits  = 0x%x\n",  self->descriptor.property.vector.bits );
  PUT( "               .ctr   = 0x%x\n",  self->descriptor.property.vector.ctr );
  PUT( "               .vec   = 0x%x\n",  self->descriptor.property.vector.vec );
  PUT( "        .degree.bits  = 0x%x\n",  self->descriptor.property.degree.bits );
  PUT( "               .in    = 0x%x\n",  self->descriptor.property.degree.in );
  PUT( "               .out   = 0x%x\n",  self->descriptor.property.degree.out );
  PUT( "state.bits            = %s\n",    uint8_to_bin( buf, self->descriptor.state.bits ) );
  PUT( "     .lock.bits       = 0x%x\n",  self->descriptor.state.lock.bits );
  PUT( "          .yib        = 0x%x\n",  self->descriptor.state.lock.yib );
  PUT( "          .iny        = 0x%x\n",  self->descriptor.state.lock.iny );
  PUT( "          .wrq        = 0x%x\n",  self->descriptor.state.lock.wrq );
  PUT( "          .rwl        = 0x%x\n",  self->descriptor.state.lock.rwl );
  PUT( "          .lck        = 0x%x\n",  self->descriptor.state.lock.lck );
  PUT( "     .context.bits    = 0x%x\n",  self->descriptor.state.context.bits );
  PUT( "             .man     = 0x%x\n",  self->descriptor.state.context.man );
  PUT( "             .sus     = 0x%x\n",  self->descriptor.state.context.sus );
  PUT( "type.enumeration      = 0x%x\n",  self->descriptor.type.enumeration );
  PUT( "semaphore.count       = %d\n",    self->descriptor.semaphore.count );
  PUT( "\n" );
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( self );
  cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( self );
  PUT( "Allocator details:\n" );
  PUT( "    handle            = %016llX\n",   handle.qword );
  PUT( "    class             = %02X (%s)\n", handle.objclass, OBJECT_CLASSNAME[ handle.objclass ].classname );
  PUT( "    aidx              = %u\n",        handle.aidx );
  PUT( "    bidx              = %u\n",        handle.bidx );
  PUT( "    offset            = %lu\n",       handle.offset );
  PUT( "    flags.invl        = %u\n",        linehead->data.flags.invl );
  PUT( "    flags.ovsz        = %u\n",        linehead->data.flags.ovsz );
  PUT( "    flags._act        = %u\n",        linehead->data.flags._act );
  PUT( "    flags._chk        = %u\n",        linehead->data.flags._chk );
  PUT( "    flags._mod        = %u\n",        linehead->data.flags._mod );
  PUT( "    refc              = %lu\n",       linehead->data.refc );
  PUT( "    size              = %lu\n",       linehead->data.size );
  PUT( "\n" );


  return output;
#undef PUT

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * Vertex_allocator( vgx_Vertex_t *self ) {
  if( self && self->graph ) {
    return (comlib_object_t*)self->graph->vertex_allocator;
  }
  else {
    return (comlib_object_t*)ivertexalloc.GetCurrent();
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Vertex_descriptor( vgx_Vertex_t *self_RO, CStringQueue_t *output ) {
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )
  static const char *man[] = {"NULL","REAL","VIRT","?"};
  static const char *sus[] = {"ACTIVE", "SUSPENDED"};
  static const char *yib[] = {"IDLE", "BUSY"};
  static const char *iny[] = {"NORMAL", "YIELDED"};
  static const char *wrq[] = {"-", "WREQ"};
  static const char *rwl[] = {"-", "WRITABLE", "READONLY"};
  static const char *lck[] = {"OPEN", "LOCKED"};
  static const char *in[]  = {"-", "INARCS"};
  static const char *out[] = {"-", "OUTARCS"};
  static const char *ctr[] = {"-", "STANDARD", "CENTROID"};
  static const char *vec[] = {"-", "VECTOR"};
  static const char *scp[] = {"GLOBAL", "LOCAL"};

  vgx_VertexDescriptor_t D = self_RO->descriptor;
  const CString_t * CSTR__type = iEnumerator_OPEN.VertexType.Decode( self_RO->graph, D.type.enumeration );
  PUT( "[%d] [%s] [[%s %s] [%s %s %s %s %s]] [[%s %s] [%s %s] [%s] [0]] [%ld]",
         D.semaphore.count,
              CStringValue( CSTR__type ),
                    man[ D.state.context.man ],
                       sus[ D.state.context.sus ],
                            yib[ D.state.lock.yib ],
                               iny[ D.state.lock.iny ],
                                  wrq[ D.state.lock.wrq ],
                                     rwl[ D.state.lock.lck ? D.state.lock.rwl+1 : 0 ],
                                        lck[ D.state.lock.lck ],
                                                in[ D.property.degree.in ],
                                                   out[ D.property.degree.out ],
                                                        ctr[ D.property.vector.vec ? D.property.vector.ctr+1 : 0 ],
                                                            vec[ D.property.vector.vec ],
                                                                scp[ D.property.scope.def ],
                                                                          D.writer.threadid
  );
  return output;
#undef PUT

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void Vertex_print_vertex_allocator( vgx_Vertex_t *self_RO ) {
  vgx_Graph_t *graph = self_RO->graph;
  GRAPH_LOCK( graph ) {
    cxmalloc_family_t *vertex_allocator_family = self_RO->graph->vertex_allocator;
    int64_t vertex_refcnt = Vertex_REFCNT_CS_RO( self_RO );
    cxmalloc_linehead_t *vertex_linehead = _cxmalloc_linehead_from_object( self_RO );
    uint16_t aidx = vertex_linehead->data.aidx;
    uint16_t bidx = vertex_linehead->data.bidx;
    uint32_t offset = vertex_linehead->data.offset;

    printf( "VERTEX ALLOCATOR FAMILY\n" );
    PRINT( vertex_allocator_family );
    printf( "\n" );
  
    printf( "VERTEX\n" );
    PRINT( self_RO );
    printf( "\n" );

    printf( "DETAILS\n" );
    printf( "aidx=%u bidx=%u offset=%u\n", aidx, bidx, offset );
    printf( "refcnt=%lld\n", vertex_refcnt );
    printf( "\n" );
  } GRAPH_RELEASE;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static CString_t * Vertex_short_repr( const vgx_Vertex_t *self_RO ) {
  static const char *no_yes[2] = {"no","yes"};

  if( self_RO ) {
    vgx_Vertex_vtable_t *iV = CALLABLE( self_RO );

    int64_t indegree = iV->InDegree( self_RO );
    int64_t outdegree = iV->OutDegree( self_RO );
    const char *id = iV->IDString( self_RO );
    const CString_t *CSTR__type = iV->TypeName( self_RO );
    int64_t refcnt = Vertex_REFCNT_CS_RO( self_RO  );

    const char *readable = no_yes[ iV->Readable( self_RO  ) ];
    const char *writable = no_yes[ iV->Writable( self_RO  ) ];
    int semcnt = self_RO ->descriptor.semaphore.count;
    const char *man = __vertex_is_manifestation_real( self_RO  ) ? "REAL" : "VIRTUAL";
    DWORD owner = self_RO->descriptor.writer.threadid;

    CString_t *CSTR__repr = CStringNewFormat( "-%lld->(%s)-%lld-> type=%s man=%s refcnt=%lld read=%s write=%s semcnt=%d owner=%d",
                                    /*         |        \   \         |      |         |          |        |         |        |     */
                                               indegree, id, outdegree, /*   |         |          |        |         |        |     */
                                                                      CStringValue(CSTR__type), /*|        |         |        |     */
                                                                             man, /*   |          |        |         |        |     */
                                                                                       refcnt, /* |        |         |        |     */ 
                                                                                                  readable, writable, semcnt,
                                                                                                                              owner
                                            );


    return CSTR__repr;
  }
  else {
    return NULL;
  }
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxvertex_object.h"

test_descriptor_t _vgx_vxvertex_object_tests[] = {
  { "VGX Graph Vertex Object Tests", __utest_vxvertex_object },

  {NULL}
};
#endif
