/*
###################################################
#
# File:   _vgx.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VGX_H
#define _VGX_VGX_H


#include "_vxprivate.h"
#include "_vxpathdef.h"
#include "_vxgraph.h"
#include "_vxalloc.h"
#include "vgx.h"


#define Vertex_INCREF_WL( Vertex_WL )                     _vxoballoc_vertex_incref_WL( Vertex_WL )
#define Vertex_INCREF_DELTA_WL( Vertex_WL, Delta )        _vxoballoc_vertex_incref_delta_WL( Vertex_WL, Delta )
#define Vertex_DECREF_WL( Vertex_WL )                     _vxoballoc_vertex_decref_WL( Vertex_WL )
#define Vertex_REFCNT_WL( Vertex_WL )                     _vxoballoc_vertex_refcnt_WL( Vertex_WL )


#define Vertex_INCREF_CS_RO( Vertex_CS_RO )               _vxoballoc_vertex_incref_CS_RO( Vertex_CS_RO )
#define Vertex_INCREF_DELTA_CS_RO( Vertex_CS_RO, Delta )  _vxoballoc_vertex_incref_delta_CS_RO( Vertex_CS_RO, Delta )
#define Vertex_DECREF_CS_RO( Vertex_CS_RO )               _vxoballoc_vertex_decref_CS_RO( Vertex_CS_RO )
#define Vertex_REFCNT_CS_RO( Vertex_CS_RO )               _vxoballoc_vertex_refcnt_CS_RO( Vertex_CS_RO )


#define GraphRevSize( Graph )  (ATOMIC_READ_i64( &(Graph)->rev_size_atomic ))
#define GraphSize( Graph )  (ATOMIC_READ_i64( &(Graph)->_size_atomic ))
#define GraphOrder( Graph ) (ATOMIC_READ_i64( &(Graph)->_order_atomic ))

#define AddGraphRevSize( Graph, Delta )  (ATOMIC_ADD_i64( &(Graph)->rev_size_atomic, (Delta) ))
#define SubGraphRevSize( Graph, Delta )  (ATOMIC_SUB_i64( &(Graph)->rev_size_atomic, (Delta) ))
#define IncGraphRevSize( Graph )  (ATOMIC_INCREMENT_i64( &(Graph)->rev_size_atomic ))
#define DecGraphRevSize( Graph )  (ATOMIC_DECREMENT_i64( &(Graph)->rev_size_atomic ))

#define AddGraphSize( Graph, Delta )  (ATOMIC_ADD_i64( &(Graph)->_size_atomic, (Delta) ))
#define SubGraphSize( Graph, Delta )  (ATOMIC_SUB_i64( &(Graph)->_size_atomic, (Delta) ))
#define IncGraphSize( Graph )  (ATOMIC_INCREMENT_i64( &(Graph)->_size_atomic ))
#define DecGraphSize( Graph )  (ATOMIC_DECREMENT_i64( &(Graph)->_size_atomic ))

#define AddGraphOrder( Graph, Delta ) (ATOMIC_ADD_i64( &(Graph)->_order_atomic, (Delta) ))
#define SubGraphOrder( Graph, Delta ) (ATOMIC_SUB_i64( &(Graph)->_order_atomic, (Delta) ))
#define IncGraphOrder( Graph ) (ATOMIC_INCREMENT_i64( &(Graph)->_order_atomic ))
#define DecGraphOrder( Graph ) (ATOMIC_DECREMENT_i64( &(Graph)->_order_atomic ))


#define GraphPropCount( Graph )           (ATOMIC_READ_i64( &(Graph)->_nproperties_atomic ))
#define AddGraphPropCount( Graph, Delta ) (ATOMIC_ADD_i64( &(Graph)->_nproperties_atomic, (Delta) ))
#define SubGraphPropCount( Graph, Delta ) (ATOMIC_SUB_i64( &(Graph)->_nproperties_atomic, (Delta) ))
#define IncGraphPropCount( Graph )        (ATOMIC_INCREMENT_i64( &(Graph)->_nproperties_atomic ))
#define DecGraphPropCount( Graph )        (ATOMIC_DECREMENT_i64( &(Graph)->_nproperties_atomic ))

#define GraphVectorCount( Graph )     (ATOMIC_READ_i64( &(Graph)->_nvectors_atomic ))
#define IncGraphVectorCount( Graph )  (ATOMIC_INCREMENT_i64( &(Graph)->_nvectors_atomic ))
#define DecGraphVectorCount( Graph )  (ATOMIC_DECREMENT_i64( &(Graph)->_nvectors_atomic ))


DLL_HIDDEN extern CString_t *g_CSTR__ondelete_key;
DLL_HIDDEN extern shortid_t g_ondelete_keyhash;

DLL_HIDDEN extern vgx_ArcComparator_t _iArcMaxComparator;
DLL_HIDDEN extern vgx_ArcComparator_t _iArcMinComparator;

DLL_HIDDEN extern vgx_VertexComparator_t _iVertexMaxComparator;
DLL_HIDDEN extern vgx_VertexComparator_t _iVertexMinComparator;

DLL_HIDDEN extern vgx_RankScoreFromItem_t _iRankScoreFromItem;

DLL_HIDDEN extern vgx_VertexCollector_t _iCollectVertex;
DLL_HIDDEN extern vgx_ArcCollector_t _iCollectArc;
DLL_HIDDEN extern vgx_VertexStager_t _iStageVertex;
DLL_HIDDEN extern vgx_ArcStager_t _iStageArc;

DLL_HIDDEN extern int _vxarcvector_comparator__unstage( vgx_BaseCollector_context_t *collector, int index );
DLL_HIDDEN extern int _vxarcvector_comparator__commit( vgx_BaseCollector_context_t *collector, int index );

DLL_HIDDEN extern vgx_ArcRankScorer_t _iComputeArcRankScore;
DLL_HIDDEN extern vgx_VertexRankScorer_t _iComputeVertexRankScore;

/* TEST DESCRIPTORS */
#ifdef INCLUDE_UNIT_TESTS

// vxoballoc
extern test_descriptor_t _vgx_vxoballoc_graph_tests[];
extern test_descriptor_t _vgx_vxoballoc_vertex_tests[];
extern test_descriptor_t _vgx_vxoballoc_vector_tests[];
extern test_descriptor_t _vgx_vxoballoc_cstring_tests[];

// vxvertex
extern test_descriptor_t _vgx_vxvertex_object_tests[];
extern test_descriptor_t _vgx_vxvertex_property_tests[];

// vxarvector
extern test_descriptor_t _vgx_vxarcvector_comparator_tests[];
extern test_descriptor_t _vgx_vxarcvector_filter_tests[];
extern test_descriptor_t _vgx_vxarcvector_fhash_tests[];

extern test_descriptor_t _vgx_vxarcvector_cellproc_tests[];
extern test_descriptor_t _vgx_vxarcvector_traverse_tests[];
extern test_descriptor_t _vgx_vxarcvector_exists_tests[];
extern test_descriptor_t _vgx_vxarcvector_delete_tests[];
extern test_descriptor_t _vgx_vxarcvector_expire_tests[];

extern test_descriptor_t _vgx_vxarcvector_dispatch_tests[];
extern test_descriptor_t _vgx_vxarcvector_serialization_tests[];
extern test_descriptor_t _vgx_vxarcvector_api_tests[];

// vxsim
extern test_descriptor_t _vgx_vxsim_centroid_tests[];
extern test_descriptor_t _vgx_vxsim_lsh_tests[];
extern test_descriptor_t _vgx_vxsim_vector_tests[];
extern test_descriptor_t _vgx_vxsim_tests[];

// io
//
extern test_descriptor_t _vgx_vxio_uri_tests[];

// vxgraph
//
extern test_descriptor_t _vgx_vxgraph_mapping_tests[];
extern test_descriptor_t _vgx_vxgraph_caching_tests[];

// vxenum
extern test_descriptor_t _vgx_vxenum_tests[];
extern test_descriptor_t _vgx_vxenum_rel_tests[];
extern test_descriptor_t _vgx_vxenum_vtx_tests[];
extern test_descriptor_t _vgx_vxenum_dim_tests[];
extern test_descriptor_t _vgx_vxenum_propkey_tests[];
extern test_descriptor_t _vgx_vxenum_propval_tests[];
extern test_descriptor_t _vgx_vxgraph_vxtable_tests[];

// vxevent
extern test_descriptor_t _vgx_vxevent_eventapi_tests[];
extern test_descriptor_t _vgx_vxevent_eventmon_tests[];
extern test_descriptor_t _vgx_vxevent_eventexec_tests[];

// vxdurable
extern test_descriptor_t _vgx_vxdurable_registry_tests[];
extern test_descriptor_t _vgx_vxdurable_system_tests[];
extern test_descriptor_t _vgx_vxdurable_commit_tests[];
extern test_descriptor_t _vgx_vxdurable_serialization_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_buffers_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_transaction_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_capture_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_emitter_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_produce_op_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_parser_tests[];
extern test_descriptor_t _vgx_vxdurable_operation_consumer_service_tests[];

//
extern test_descriptor_t _vgx_vxgraph_object_tests[];
extern test_descriptor_t _vgx_vxgraph_state_tests[];
extern test_descriptor_t _vgx_vxgraph_tracker_tests[];
extern test_descriptor_t _vgx_vxgraph_relation_tests[];
extern test_descriptor_t _vgx_vxgraph_arc_tests[];

//
extern test_descriptor_t _vgx_vxgraph_tick_tests[];

//
extern test_descriptor_t _vgx_vxeval_tests[];

// vxquery
extern test_descriptor_t _vgx_vxquery_query_tests[];
extern test_descriptor_t _vgx_vxquery_probe_tests[];
extern test_descriptor_t _vgx_vxquery_inspect_tests[];
extern test_descriptor_t _vgx_vxquery_collector_tests[];
extern test_descriptor_t _vgx_vxquery_traverse_tests[];
extern test_descriptor_t _vgx_vxquery_aggregator_tests[];
extern test_descriptor_t _vgx_vxquery_response_tests[];
extern test_descriptor_t _vgx_vxquery_rank_tests[];
//

// vxapi
extern test_descriptor_t _vgx_vxapi_simple_tests[];
extern test_descriptor_t _vgx_vxapi_advanced_tests[];

// vgx_server
extern test_descriptor_t _vgx_server_tests[];

#endif

// TODO: FIND A BETTER PLACE FOR THIS
__inline static objectid_t * __vertex_internalid( const vgx_Vertex_t *vertex ) {
  return (objectid_t*)(&CXMALLOC_META_FROM_OBJECT( vertex )->M);
}



static CString_t * __format_error_string( CString_t **CSTR__error, const char *format, ... ) {
  if( CSTR__error == NULL ) {
    return NULL;
  }

  CString_t *CSTR__ret;
  va_list args;
  va_start( args, format );

  // Error already set, no action
  if( *CSTR__error ) {
    CSTR__ret = *CSTR__error;
  }
  // Format error
  else if( iString.SetNewVFormat( NULL, CSTR__error, format, &args ) == 0 ) {
    CSTR__ret = *CSTR__error;
  }
  // Failed
  else {
    CSTR__ret = NULL;
  }

  va_end( args );
  return CSTR__ret;
}




static CString_t * __set_error_string( CString_t **CSTR__error, const char *string ) {
  if( CSTR__error == NULL ) {
    return NULL;
  }
  // Error already set, no action
  if( *CSTR__error ) {
    return *CSTR__error;
  }
  // Set error
  else if( iString.SetNew( NULL, CSTR__error, string ? string : "?" ) == 0 ) {
    return *CSTR__error;
  }
  // Failed
  else {
    return NULL;
  }
}



static const char * __get_error_string( CString_t **CSTR__error, const char *dflt ) {
  if( CSTR__error && *CSTR__error ) {
    return CStringValue( *CSTR__error );
  }
  else {
    return dflt;
  }
}



static CString_t * __set_error_string_with_object_name( CString_t **CSTR__error, const char *string, const CString_t *CSTR__name ) {
  int err = 0;
  int64_t len = CSTR__name ? CStringLength( CSTR__name ) : -1;
  if( len > 508 ) {
    const CString_t *CSTR__prefix = CALLABLE( CSTR__name )->Prefix( CSTR__name, 508 );
    err = iString.SetNewFormat( NULL, CSTR__error, "%s: \"%s...\" (%lu more)", string, CStringValue( CSTR__prefix ), len - CStringLength( CSTR__prefix ) );
    CStringDelete( CSTR__prefix );
  }
  else if( len >= 0 ) {
    err = iString.SetNewFormat( NULL, CSTR__error, "%s: \"%s\"", string, CStringValue( CSTR__name ) );
  }
  else {
    err = iString.SetNew( NULL, CSTR__error, "Unspecified object" );
  }
  if( err != 0 ) {
    return NULL;
  }
  else {
    return *CSTR__error;
  }
}



static CString_t * __set_error_string_from_reason( CString_t **CSTR__error, const CString_t *CSTR__name, vgx_AccessReason_t reason ) {
  switch( reason ) {
  case VGX_ACCESS_REASON_NOEXIST:
  case VGX_ACCESS_REASON_NOEXIST_MSG:
    return __set_error_string_with_object_name( CSTR__error, "Object does not exist", CSTR__name );
  case VGX_ACCESS_REASON_NOCREATE:
    return __set_error_string_with_object_name( CSTR__error, "Object cannot be created", CSTR__name );
  case VGX_ACCESS_REASON_LOCKED:
    return __set_error_string_with_object_name( CSTR__error, "Object is locked", CSTR__name );
  case VGX_ACCESS_REASON_TIMEOUT:
    return __set_error_string_with_object_name( CSTR__error, "Object acquisition timeout", CSTR__name );
  case VGX_ACCESS_REASON_OPFAIL:
    return __set_error_string_with_object_name( CSTR__error, "Object operation capture failed", CSTR__name );
  case VGX_ACCESS_REASON_EXECUTION_TIMEOUT:
    return __set_error_string_with_object_name( CSTR__error, "Execution timeout", CSTR__name );
  case VGX_ACCESS_REASON_NO_VERTEX_VECTOR:
    return __set_error_string_with_object_name( CSTR__error, "Vertex has no vector", CSTR__name );
  case VGX_ACCESS_REASON_VERTEX_ARC_ERROR:
    return __set_error_string_with_object_name( CSTR__error, "Vertex arc error", CSTR__name );
  case VGX_ACCESS_REASON_READONLY_GRAPH:
    return __set_error_string_with_object_name( CSTR__error, "Graph is readonly", CSTR__name );
  case VGX_ACCESS_REASON_READONLY_PENDING:
    return __set_error_string_with_object_name( CSTR__error, "Graph readonly transition pending", CSTR__name );
  case VGX_ACCESS_REASON_INVALID:
    return __set_error_string_with_object_name( CSTR__error, "Invalid object access", CSTR__name );
  case VGX_ACCESS_REASON_ENUM_NOTYPESPACE:
    return __set_error_string_with_object_name( CSTR__error, "Enumerator typespace exhausted", CSTR__name );
  case VGX_ACCESS_REASON_TYPEMISMATCH:
    return __set_error_string_with_object_name( CSTR__error, "Enumerator type mismatch", CSTR__name );
  case VGX_ACCESS_REASON_SEMAPHORE:
    return __set_error_string_with_object_name( CSTR__error, "Semaphore error", CSTR__name );
  case VGX_ACCESS_REASON_BAD_CONTEXT:
    return __set_error_string_with_object_name( CSTR__error, "Bad context", CSTR__name );
  case VGX_ACCESS_REASON_RO_DISALLOWED:
    return __set_error_string_with_object_name( CSTR__error, "Readonly disallowed", CSTR__name );
  case VGX_ACCESS_REASON_ERROR:
    return __set_error_string_with_object_name( CSTR__error, "General object access error", CSTR__name );
  default:
    return *CSTR__error;
  }
}



static CString_t * __set_error_string_from_errcode( CString_t **CSTR__error, int errcode, const char *message ) {
  static char codebuf[512] = {0};
#define FORMAT_ERROR_CODE( Message, Errcode ) \

  int msg_sub = cxlib_exc_subtype( errcode );
  cxlib_format_code( errcode, codebuf, 511 );
  const char *err = "Unknown";
  switch( msg_sub ) {
  case CXLIB_ERR_INITIALIZATION:
    err = "Initialization";
    break;
  case CXLIB_ERR_CONFIG:
    err = "Configuration";
    break;
  case CXLIB_ERR_FORMAT:
    err = "Format";
    break;
  case CXLIB_ERR_LOOKUP:
    err = "Lookup";
    break;
  case CXLIB_ERR_BUG:
    err = "Code";
    break;
  case CXLIB_ERR_FILESYSTEM:
    err = "Filesystem";
    break;
  case CXLIB_ERR_CAPACITY:
    err = "System Capacity";
    break;
  case CXLIB_ERR_MEMORY:
    err = "Out Of Memory";
    break;
  case CXLIB_ERR_CORRUPTION:
    err = "Data Corruption";
    break;
  case CXLIB_ERR_SEMAPHORE:
    err = "Semaphore";
    break;
  case CXLIB_ERR_MUTEX:
    err = "Mutex";
    break;
  case CXLIB_ERR_GENERAL:
    err = "General";
    break;
  case CXLIB_ERR_API:
    err = "API";
    break;
  case CXLIB_ERR_ASSERTION:
    err = "Assertion";
    break;
  case CXLIB_ERR_IGNORE:
    return NULL;
    break;
  }
  return __format_error_string( CSTR__error, "%s Error %s: %s", err, codebuf, message ? message : "(no details)" );
}




static CString_t * __transfer_error_string( CString_t **CSTR__dest, CString_t **CSTR__src ) {
  CString_t * CSTR__err = NULL;
  if( CSTR__dest && CSTR__src && *CSTR__src ) {
    // Discard any previous destination string
    iString.Discard( CSTR__dest );
    // Steal the src pointer into dest (this is the transfer)
    *CSTR__dest = *CSTR__src;
    // Forget the string in the src
    *CSTR__src = NULL;
    CSTR__err = *CSTR__dest;
  }
  return CSTR__err;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN extern void __check_arc_balance( vgx_Graph_t *self, const char *funcname, size_t linenum );

#ifndef NDEBUG
#define __CHECK_ARC_BALANCE( Graph ) \
  __check_arc_balance( Graph, __FUNCTION__, __LINE__ );
#else
#define __CHECK_ARC_BALANCE( Graph )
#endif



static void PRINT_VERTEX( const vgx_Vertex_t *vertex ) {
  if( vertex != NULL ) {
    COMLIB_OBJECT_PRINT( vertex );
  }
  else {
    printf( "vertex=NULL\n" );
  }
}




/*******************************************************************//**
 *
 * vxvertex
 *
 ***********************************************************************
 */

DLL_HIDDEN extern vgx_ArcVector_cell_t * _vxvertex__get_vertex_outarcs( vgx_Vertex_t *vertex );
DLL_HIDDEN extern vgx_ArcVector_cell_t * _vxvertex__get_vertex_inarcs( vgx_Vertex_t *vertex );
DLL_HIDDEN extern vgx_Vertex_t * NewVertex_CS( vgx_Graph_t *self, const objectid_t *obid, const CString_t *CSTR__identifier, vgx_VertexTypeEnumeration_t vxtype, vgx_Rank_t rank, vgx_VertexStateContext_man_t manifestation, CString_t **CSTR__error );



/*******************************************************************//**
 *
 * vxgraph_mapping
 *
 ***********************************************************************
 */

#define MAPPING_SIZE_UNLIMITED -1
#define MAPPING_DEFAULT_ORDER -1





/*******************************************************************//**
 *
 * vxgraph_vxtable
 *
 ***********************************************************************
 */
#define VXTABLE_VERTEX_REFCOUNT 2 /* generic plus type index */

// Return 1 if the refcount suggests index is only owner and if it is indexed
__inline static int __vertex_owned_by_index_only_LCK( vgx_Vertex_t *vertex_LCK, int64_t refcnt ) {
  return refcnt == VXTABLE_VERTEX_REFCOUNT && __vertex_is_indexed( vertex_LCK );
}


#ifdef VXTABLE_PERSIST
DLL_HIDDEN extern             int _vxgraph_vxtable__create_index_OPEN( vgx_Graph_t *self );
#endif
DLL_HIDDEN extern            void _vxgraph_vxtable__destroy_index_CS( vgx_Graph_t *self );
DLL_HIDDEN extern             int _vxgraph_vxtable__index_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern             int _vxgraph_vxtable__index_vertex_OPEN_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern             int _vxgraph_vxtable__index_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern             int _vxgraph_vxtable__unindex_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern             int _vxgraph_vxtable__unindex_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern             int _vxgraph_vxtable__reindex_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_vertex_type_t new_type );
DLL_HIDDEN extern  vgx_Vertex_t * _vxgraph_vxtable__query_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern     CString_t * _vxgraph_vxtable__get_idstring_CS( vgx_Graph_t *self, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern            bool _vxgraph_vxtable__exists_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__len_CS( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern             int _vxgraph_vxtable__set_readonly_CS( vgx_Graph_t *self );
DLL_HIDDEN extern             int _vxgraph_vxtable__clear_readonly_CS( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__collect_items_ROG_or_CSNOWL( vgx_Graph_t *self, vgx_global_search_context_t *search, vgx_VertexFilter_context_t *filter );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__count_vertex_properties_ROG( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__count_vertex_tmx_ROG( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__prepare_vertices_CS_NT( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__truncate_noeventproc_CS( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype, CString_t **CSTR__error );
DLL_HIDDEN extern  vgx_Vertex_t * _vxgraph_vxtable__query_OPEN( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern            bool _vxgraph_vxtable__exists_OPEN( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern     CString_t * _vxgraph_vxtable__get_idstring_OPEN( vgx_Graph_t *self, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__len_OPEN( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__collect_items_OPEN( vgx_Graph_t *self, vgx_global_search_context_t *search, bool readonly_graph, vgx_VertexFilter_context_t *filter );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__truncate_OPEN( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype, CString_t **CSTR__error );
DLL_EXPORT extern             int _vxgraph_vxtable__get_index_counters_OPEN( vgx_Graph_t *self, framehash_perfcounters_t *counters, const CString_t *CSTR__vertex_type, CString_t **CSTR__error );
DLL_EXPORT extern             int _vxgraph_vxtable__reset_index_counters_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__operation_sync_vertices_CS_NT_NOROG( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__operation_sync_arcs_CS_NT_NOROG( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_vxtable__operation_sync_virtual_vertices_CS_NT_NOROG( vgx_Graph_t *self );



/*******************************************************************//**
 *
 * vxdurable_operation
 *
 ***********************************************************************
 */
DLL_HIDDEN extern void    _vxdurable_operation_consumer_service__perform_disk_cleanup_OPEN( vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int64_t _vxdurable_operation__emitter_checkpoint_CS( vgx_Graph_t *graph, cxmalloc_object_processing_context_t *sync_context, int64_t obj_cnt );




/*******************************************************************//**
 *
 * vxenum
 *
 ***********************************************************************
 */

// vxenum_rel
DLL_HIDDEN extern int                     _vxenum_rel__initialize( void );
DLL_HIDDEN extern int                     _vxenum_rel__destroy( void );
DLL_HIDDEN extern int                     _vxenum_rel__create_enumerator( vgx_Graph_t *self );
DLL_HIDDEN extern void                    _vxenum_rel__destroy_enumerator( vgx_Graph_t *self );
DLL_HIDDEN extern vgx_predicator_rel_enum _vxenum_rel__set_CS( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
DLL_HIDDEN extern vgx_predicator_rel_enum _vxenum_rel__set_OPEN( vgx_Graph_t *self, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode );
DLL_HIDDEN extern vgx_predicator_rel_enum _vxenum_rel__encode_CS( vgx_Graph_t *self, const CString_t *CSTR__relationship, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_rel__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__relationshp );
DLL_HIDDEN extern vgx_predicator_rel_enum _vxenum_rel__encode_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationshp, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_rel__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationshp );
DLL_HIDDEN extern const CString_t *       _vxenum_rel__decode_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel );
DLL_HIDDEN extern const CString_t *       _vxenum_rel__decode_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum rel );
DLL_HIDDEN extern void                    _vxenum_rel__clear_thread_cache( void );
DLL_HIDDEN extern bool                    _vxenum_rel__has_mapping_CS( vgx_Graph_t *self, const CString_t *CSTR__relationship );
DLL_HIDDEN extern bool                    _vxenum_rel__has_mapping_OPEN( vgx_Graph_t *self, const CString_t *CSTR__relationship );
DLL_HIDDEN extern bool                    _vxenum_rel__has_mapping_rel_CS( vgx_Graph_t *self, vgx_predicator_rel_enum rel );
DLL_HIDDEN extern bool                    _vxenum_rel__has_mapping_rel_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum rel );
DLL_HIDDEN extern int                     _vxenum_rel__entries_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_rel__entries_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_rel__codes_CS( vgx_Graph_t *self, CtptrList_t *output );
DLL_HIDDEN extern int                     _vxenum_rel__codes_OPEN( vgx_Graph_t *self, CtptrList_t *output );
DLL_HIDDEN extern int64_t                 _vxenum_rel__count_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_rel__count_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_rel__operation_sync_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_rel__operation_sync_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int                     _vxenum_rel__remove_mapping_CS( vgx_Graph_t *self, vgx_predicator_rel_enum enc );
DLL_HIDDEN extern int                     _vxenum_rel__remove_mapping_OPEN( vgx_Graph_t *self, vgx_predicator_rel_enum enc );



// vxenum_vtx
DLL_HIDDEN extern int                     _vxenum_vtx__initialize( void );
DLL_HIDDEN extern int                     _vxenum_vtx__destroy( void );
DLL_HIDDEN extern int                     _vxenum_vtx__create_enumerator( vgx_Graph_t *self );
DLL_HIDDEN extern void                    _vxenum_vtx__destroy_enumerator( vgx_Graph_t *self );

DLL_HIDDEN extern vgx_vertex_type_t       _vxenum_vtx__set_CS( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
DLL_HIDDEN extern vgx_vertex_type_t       _vxenum_vtx__set_OPEN( vgx_Graph_t *self, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode );
DLL_HIDDEN extern vgx_vertex_type_t       _vxenum_vtx__encode_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_vtx__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
DLL_HIDDEN extern vgx_vertex_type_t       _vxenum_vtx__encode_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_vtx__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__decode_CS( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__decode_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
DLL_HIDDEN extern void                    _vxenum_vtx__clear_cache( void );
DLL_HIDDEN extern bool                    _vxenum_vtx__has_mapping_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
DLL_HIDDEN extern bool                    _vxenum_vtx__has_mapping_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_type_name );
DLL_HIDDEN extern bool                    _vxenum_vtx__has_mapping_vtx_CS( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
DLL_HIDDEN extern bool                    _vxenum_vtx__has_mapping_vtx_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vertex_type );
DLL_HIDDEN extern int                     _vxenum_vtx__entries_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_vtx__entries_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_vtx__codes_CS( vgx_Graph_t *self, CtptrList_t *output );
DLL_HIDDEN extern int                     _vxenum_vtx__codes_OPEN( vgx_Graph_t *self, CtptrList_t *output );
DLL_HIDDEN extern int64_t                 _vxenum_vtx__count_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_vtx__count_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int                     _vxenum_vtx__remove_mapping_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype );
DLL_HIDDEN extern int                     _vxenum_vtx__remove_mapping_OPEN( vgx_Graph_t *self, vgx_vertex_type_t vxtype );
DLL_HIDDEN extern int64_t                 _vxenum_vtx__operation_sync_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_vtx__operation_sync_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__reserved_None( vgx_Graph_t *self );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__reserved_Vertex( vgx_Graph_t *self );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__reserved_System( vgx_Graph_t *self );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__reserved_Unknown( vgx_Graph_t *self );
DLL_HIDDEN extern const CString_t *       _vxenum_vtx__reserved_LockObject( vgx_Graph_t *self );


// vxenum_dim
DLL_HIDDEN extern int                     _vxenum_dim__initialize( void );
DLL_HIDDEN extern int                     _vxenum_dim__destroy( void );
DLL_HIDDEN extern int                     _vxenum_dim__create_enumerator( vgx_Similarity_t *self );
DLL_HIDDEN extern void                    _vxenum_dim__destroy_enumerator( vgx_Similarity_t *self );

DLL_HIDDEN extern feature_vector_dimension_t      _vxenum_dim__set_CS( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode );
DLL_HIDDEN extern feature_vector_dimension_t      _vxenum_dim__set_OPEN( vgx_Similarity_t *self, QWORD dimhash, CString_t *CSTR__dimension, feature_vector_dimension_t dimcode );
DLL_HIDDEN extern feature_vector_dimension_t      _vxenum_dim__encode_chars_CS( vgx_Similarity_t *self, const char *dimension, QWORD *ret_dimhash, bool create_nonexist );
DLL_HIDDEN extern feature_vector_dimension_t      _vxenum_dim__encode_chars_OPEN( vgx_Similarity_t *self, const char *dimension, QWORD *ret_dimhash, bool create_nonexist );
DLL_HIDDEN extern const CString_t *       _vxenum_dim__decode_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern const CString_t *       _vxenum_dim__decode_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern int64_t                 _vxenum_dim__own_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern int64_t                 _vxenum_dim__own_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern int64_t                 _vxenum_dim__discard_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern int64_t                 _vxenum_dim__discard_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern bool                    _vxenum_dim__has_mapping_chars_CS( vgx_Similarity_t *self, const char *dimension );
DLL_HIDDEN extern bool                    _vxenum_dim__has_mapping_chars_OPEN( vgx_Similarity_t *self, const char *dimension );
DLL_HIDDEN extern bool                    _vxenum_dim__has_mapping_dim_CS( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern bool                    _vxenum_dim__has_mapping_dim_OPEN( vgx_Similarity_t *self, feature_vector_dimension_t dim );
DLL_HIDDEN extern int                     _vxenum_dim__entries_CS( vgx_Similarity_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_dim__entries_OPEN( vgx_Similarity_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int64_t                 _vxenum_dim__count_CS( vgx_Similarity_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_dim__count_OPEN( vgx_Similarity_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_dim__operation_sync_CS( vgx_Similarity_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_dim__operation_sync_OPEN( vgx_Similarity_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_dim__remain_CS( vgx_Similarity_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_dim__remain_OPEN( vgx_Similarity_t *self );


// vxenum_prop
DLL_HIDDEN extern int                     _vxenum_prop__initialize( void );
DLL_HIDDEN extern int                     _vxenum_prop__destroy( void );
DLL_HIDDEN extern int                     _vxenum_prop__create_enumerator( vgx_Graph_t *self );
DLL_HIDDEN extern void                    _vxenum_prop__destroy_enumerator( vgx_Graph_t *self );
// KEY
DLL_HIDDEN extern CString_t *             _vxenum_propkey__new_key( vgx_Graph_t *self, const char *key );
DLL_HIDDEN extern CString_t *             _vxenum_propkey__new_select_key( vgx_Graph_t *self, const char *key, shortid_t *keyhash );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__set_key_CS( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__set_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__set_key_chars_CS( vgx_Graph_t *self, const char *key );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__set_key_chars_OPEN( vgx_Graph_t *self, const char *key );
DLL_HIDDEN extern CString_t *             _vxenum_propkey__get_key_CS( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern CString_t *             _vxenum_propkey__get_key_OPEN( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__encode_key_CS( vgx_Graph_t *self, const CString_t *CSTR__key, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_propkey__get_enum_CS( vgx_Graph_t *self, const CString_t *CSTR__key );
DLL_HIDDEN extern shortid_t               _vxenum_propkey__encode_key_OPEN( vgx_Graph_t *self, const CString_t *CSTR__key, CString_t **CSTR__mapped_instance, bool create_nonexist );
DLL_HIDDEN extern uint64_t                _vxenum_propkey__get_enum_OPEN( vgx_Graph_t *self, const CString_t *CSTR__key );
DLL_HIDDEN extern const CString_t *       _vxenum_propkey__decode_key_CS( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern const CString_t *       _vxenum_propkey__decode_key_OPEN( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__own_key_CS( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__own_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__own_key_by_hash_CS( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__own_key_by_hash_OPEN( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__discard_key_CS( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__discard_key_OPEN( vgx_Graph_t *self, CString_t *CSTR__key );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__discard_key_by_hash_CS( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__discard_key_by_hash_OPEN( vgx_Graph_t *self, shortid_t keyhash );
DLL_HIDDEN extern int                     _vxenum_propkey__keys_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_propkey__keys_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__count_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__count_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__operation_sync_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propkey__operation_sync_OPEN( vgx_Graph_t *self );

DLL_HIDDEN extern int64_t                 _vxenum_propkey__rebuild_cstring_key_index( vgx_Graph_t *self );
// VALUE
DLL_HIDDEN extern objectid_t              _vxenum_propval__obid( const CString_t *CSTR__value );
DLL_HIDDEN extern CString_t *             _vxenum_propval__store_value_chars_CS( vgx_Graph_t *self, const char *value, int32_t len );
DLL_HIDDEN extern CString_t *             _vxenum_propval__store_value_chars_OPEN( vgx_Graph_t *self, const char *value, int32_t len );
DLL_HIDDEN extern CString_t *             _vxenum_propval__store_value_CS( vgx_Graph_t *self, const CString_t *CSTR__value );
DLL_HIDDEN extern CString_t *             _vxenum_propval__store_value_OPEN( vgx_Graph_t *self, const CString_t *CSTR__value );
DLL_HIDDEN extern CString_t *             _vxenum_propval__get_value_CS( vgx_Graph_t *self, const objectid_t * value_obid );
DLL_HIDDEN extern CString_t *             _vxenum_propval__get_value_OPEN( vgx_Graph_t *self, const objectid_t * value_obid );
DLL_HIDDEN extern int64_t                 _vxenum_propval__own_value_CS( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
DLL_HIDDEN extern int64_t                 _vxenum_propval__own_value_OPEN( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
DLL_HIDDEN extern int64_t                 _vxenum_propval__discard_value_CS( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
DLL_HIDDEN extern int64_t                 _vxenum_propval__discard_value_OPEN( vgx_Graph_t *self, CString_t *CSTR__shared_value_instance );
DLL_HIDDEN extern int                     _vxenum_propval__values_CS( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int                     _vxenum_propval__values_OPEN( vgx_Graph_t *self, CtptrList_t *CSTR__output );
DLL_HIDDEN extern int64_t                 _vxenum_propval__count_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propval__count_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propval__operation_sync_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int64_t                 _vxenum_propval__operation_sync_OPEN( vgx_Graph_t *self );

DLL_HIDDEN extern int64_t                 _vxenum_propval__rebuild_cstring_value_index( vgx_Graph_t *self );

DLL_HIDDEN extern int                     _vxenum__is_valid_storable_key( const char *key );
DLL_HIDDEN extern int                     _vxenum__is_valid_select_key( const char *key );
DLL_HIDDEN extern int                     _vxenum__is_valid_uri_path_segment( const char *key );

DLL_HIDDEN extern CString_t *             _vxenum__parse_allow_prefix_wildcard_and_escape( vgx_Graph_t *self, const char *probe, vgx_value_comparison *vcomp, CString_t **CSTR__error );







/*******************************************************************//**
 *
 * vxapi_simple
 *
 ***********************************************************************
 */
DLL_HIDDEN extern vgx_IGraphSimple_t * _vxapi_simple;




/*******************************************************************//**
 *
 * vxapi_advanced
 *
 ***********************************************************************
 */
DLL_HIDDEN extern vgx_IGraphAdvanced_t * _vxapi_advanced;






/*******************************************************************//**
 *
 * vxgraph_state
 *
 ***********************************************************************
 */
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__acquire_writable_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexStateContext_man_t default_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__acquire_writable_vertex_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexStateContext_man_t default_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error );

DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__escalate_to_writable_vertex_CS_RO( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record, CString_t **CSTR__error );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__escalate_to_writable_vertex_OPEN_RO( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record, CString_t **CSTR__error );

DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__relax_to_readonly_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t *vertex_LCK, vgx_vertex_record record );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__relax_to_readonly_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t *vertex_LCK, vgx_vertex_record record );

DLL_HIDDEN extern void            _vxgraph_state__convert_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_VertexStateContext_man_t man, vgx_vertex_record record );
DLL_HIDDEN extern int             _vxgraph_state__create_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_typename, vgx_Vertex_t **ret_vertex_WL, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__acquire_readonly_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__acquire_readonly_vertex_OPEN( vgx_Graph_t *self, const objectid_t *vertex_obid, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern bool            _vxgraph_state__release_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );
DLL_HIDDEN extern bool            _vxgraph_state__release_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );
DLL_HIDDEN extern bool            _vxgraph_state__unlock_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK, vgx_vertex_record record );
DLL_HIDDEN extern bool            _vxgraph_state__unlock_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK, vgx_vertex_record record );

// PAIRS
DLL_HIDDEN extern int             _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( vgx_Graph_t *self, vgx_Vertex_t **ret_initial, const CString_t *CSTR__initial_idstr, const objectid_t *initial_obid, vgx_Vertex_t **ret_terminal, const CString_t *CSTR__terminal_idstr, const objectid_t *terminal_obid, vgx_VertexStateContext_man_t default_terminal_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error );
DLL_HIDDEN extern int             _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( vgx_Graph_t *self, vgx_Vertex_t **ret_initial, const objectid_t *initial_obid, vgx_Vertex_t **ret_terminal, const objectid_t *terminal_obid, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern bool            _vxgraph_state__release_initial_and_terminal_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **initial_LCK, vgx_Vertex_t **terminal_LCK );

DLL_HIDDEN extern int64_t         _vxgraph_state__wait_max_writable_CS( vgx_Graph_t *self, int64_t max_writable, vgx_ExecutionTimingBudget_t *timing_budget );


DLL_HIDDEN extern int             _vxgraph_state__create_arc_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, vgx_AccessReason_t *reason, CString_t **CSTR__error );
DLL_HIDDEN extern int64_t         _vxgraph_state__remove_arc_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_vertex_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_terminal_inarcs_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *terminal, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern bool            _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( vgx_Graph_t *self, vgx_Vertex_t **terminal_iWL );
DLL_HIDDEN extern int64_t         _vxgraph_state__remove_outarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe );
DLL_HIDDEN extern int64_t         _vxgraph_state__remove_outarcs_OPEN_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe );
DLL_HIDDEN extern int64_t         _vxgraph_state__remove_inarcs_CS_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe );
DLL_HIDDEN extern int64_t         _vxgraph_state__remove_inarcs_OPEN_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe );
DLL_HIDDEN extern int             _vxgraph_state__yield_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern int             _vxgraph_state__reclaim_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern void            _vxgraph_state__yield_vertex_inarcs_and_wait_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, int sleep_ms );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_arc_head_inarcs_writable_yield_tail_inarcs_CS_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL_to_ANY, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_vertex_readonly_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_vertex_readonly_OPEN( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record );
DLL_HIDDEN extern vgx_Vertex_t *  _vxgraph_state__lock_vertex_readonly_multi_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, int8_t n_locks );

// GRAPH READONLY
DLL_HIDDEN extern int             _vxgraph_state__acquire_graph_readonly_CS( vgx_Graph_t *self, bool force, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern int             _vxgraph_state__acquire_graph_readonly_OPEN( vgx_Graph_t *self, bool force, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern int             _vxgraph_state__release_graph_readonly_CS( vgx_Graph_t *self );
DLL_HIDDEN extern int             _vxgraph_state__release_graph_readonly_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern int             _vxgraph_state__get_num_readonly_graphs_OPEN( void );



/*******************************************************************//**
 *
 * vxgraph_tick
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int _vxgraph_tick__synchronize( vgx_Graph_t *self, int64_t external_tms_ref );
DLL_HIDDEN extern int _vxgraph_tick__initialize_CS( vgx_Graph_t *self, uint32_t inception_t0 );
DLL_HIDDEN extern int _vxgraph_tick__destroy_CS( vgx_Graph_t *self );



/*******************************************************************//**
 *
 * vxgraph_tracker
 *
 ***********************************************************************
 */
DLL_HIDDEN extern         int64_t _vxgraph_tracker__register_vertex_WL_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__register_vertex_WL_for_thread_OPEN( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__unregister_vertex_WL_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__register_vertex_RO_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__unregister_vertex_RO_for_thread_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO );
DLL_HIDDEN extern             int _vxgraph_tracker__has_readonly_locks_CS( vgx_Graph_t *self );
DLL_HIDDEN extern             int _vxgraph_tracker__has_readonly_locks_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__num_readonly_locks_CS( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__num_readonly_locks_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern             int _vxgraph_tracker__has_writable_locks_CS( vgx_Graph_t *self );
DLL_HIDDEN extern             int _vxgraph_tracker__has_writable_locks_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__num_writable_locks_CS( vgx_Graph_t *self );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__num_writable_locks_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern vgx_Vertex_t ** _vxgraph_tracker__get_readonly_vertices_CS( vgx_Graph_t *self );
DLL_HIDDEN extern vgx_Vertex_t ** _vxgraph_tracker__get_readonly_vertices_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern vgx_Vertex_t ** _vxgraph_tracker__get_writable_vertices_CS( vgx_Graph_t *self );
DLL_HIDDEN extern vgx_Vertex_t ** _vxgraph_tracker__get_writable_vertices_OPEN( vgx_Graph_t *self );
DLL_HIDDEN extern     CString_t * _vxgraph_tracker__readonly_vertices_as_cstring_CS( vgx_Graph_t *self, int64_t *n );
DLL_HIDDEN extern     CString_t * _vxgraph_tracker__readonly_vertices_as_cstring_OPEN( vgx_Graph_t *self, int64_t *n );
DLL_HIDDEN extern     CString_t * _vxgraph_tracker__writable_vertices_as_cstring_CS( vgx_Graph_t *self, int64_t *n );
DLL_HIDDEN extern     CString_t * _vxgraph_tracker__writable_vertices_as_cstring_OPEN( vgx_Graph_t *self, int64_t *n );
DLL_HIDDEN extern             int _vxgraph_tracker__enter_safe_multilock_CS( vgx_Graph_t *self, vgx_Vertex_t *exempt_if_WL, vgx_AccessReason_t *reason );
DLL_HIDDEN extern             int _vxgraph_tracker__leave_safe_multilock_CS( vgx_Graph_t *self, vgx_AccessReason_t *reason );
DLL_HIDDEN extern             int _vxgraph_tracker__get_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter, VertexAndInt64List_t **readonly, VertexAndInt64List_t **writable );
DLL_HIDDEN extern             int _vxgraph_tracker__get_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter, VertexAndInt64List_t **readonly, VertexAndInt64List_t **writable );
DLL_HIDDEN extern             int _vxgraph_tracker__num_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter, int64_t *readonly, int64_t *writable );
DLL_HIDDEN extern             int _vxgraph_tracker__num_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter, int64_t *readonly, int64_t *writable );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_readonly_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_readonly_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_writable_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_writable_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_open_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__close_open_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__commit_writable_vertices_CS( vgx_Graph_t *self, int64_t thread_id_filter );
DLL_HIDDEN extern         int64_t _vxgraph_tracker__commit_writable_vertices_OPEN( vgx_Graph_t *self, int64_t thread_id_filter );



#define ENTER_SAFE_MULTILOCK_CS( GraphPtr, FocusVertex, ReasonPtr )                  \
  do {                                                                                \
    vgx_Graph_t *__graph__ = GraphPtr;                                                \
    vgx_AccessReason_t **__reason__ = &(ReasonPtr);                             \
    bool __safe_multilock__ = true;                                                   \
    if( _vxgraph_tracker__enter_safe_multilock_CS( __graph__, FocusVertex, *__reason__ ) < 0 ) {   \
      __safe_multilock__ = false;                                                     \
    }                                                                                 \
    else /* {
      your code here
    } */


#define LEAVE_SAFE_MULTILOCK_CS( OnError_Statement )                                  \
    if( __safe_multilock__ == true ) {                                                \
      if( _vxgraph_tracker__leave_safe_multilock_CS( __graph__, *__reason__ ) < 0 ) { \
        CRITICAL( 0xEEE, "Acquisition state not restored!" );                         \
        OnError_Statement;                                                            \
      }                                                                               \
    }                                                                                 \
    else {                                                                            \
      OnError_Statement;                                                              \
    }                                                                                 \
  } WHILE_ZERO





/*******************************************************************//**
 *
 * vxeval
 *
 ***********************************************************************
 */
DLL_HIDDEN extern void   (*f_ecld_pi8)( vgx_Evaluator_t *self );
DLL_HIDDEN extern void   (*f_ssq_pi8)( vgx_Evaluator_t *self );
DLL_HIDDEN extern void   (*f_rsqrtssq_pi8)( vgx_Evaluator_t *self );
DLL_HIDDEN extern void   (*f_dp_pi8)( vgx_Evaluator_t *self );
DLL_HIDDEN extern void   (*f_cos_pi8)( vgx_Evaluator_t *self );
DLL_HIDDEN extern double (*vxeval_bytearray_distance)( const BYTE *A, const BYTE *B, float fA, float fB, int len );
DLL_HIDDEN extern double (*vxeval_bytearray_sum_squares)( const BYTE *A, int len );
DLL_HIDDEN extern double (*vxeval_bytearray_rsqrt_ssq)( const BYTE *A, int len );
DLL_HIDDEN extern double (*vxeval_bytearray_dot_product)( const BYTE *A, const BYTE *B, int len );
DLL_HIDDEN extern double (*vxeval_bytearray_cosine)( const BYTE *A, const BYTE *B, int len );





/*******************************************************************//**
 *
 * vxquery_dump
 *
 ***********************************************************************
 */
DLL_HIDDEN extern CStringQueue_t * _vxquery_dump__query( const vgx_BaseQuery_t *base, CStringQueue_t *output );
DLL_HIDDEN extern void _vxquery_dump__search_context( vgx_Graph_t *graph, const vgx_base_search_context_t *base );


/*******************************************************************//**
 *
 * vxquery_probe
 *
 ***********************************************************************
 */
typedef struct s_IGraphProbe_t {
  vgx_adjacency_search_context_t    * (*NewAdjacencySearch)(      vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AdjacencyQuery_t    *query );
  vgx_neighborhood_search_context_t * (*NewNeighborhoodSearch)(   vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_NeighborhoodQuery_t *query );
  vgx_global_search_context_t       * (*NewGlobalSearch)(         vgx_Graph_t *self, bool readonly_graph,                          vgx_GlobalQuery_t       *query );
  vgx_aggregator_search_context_t   * (*NewAggregatorSearch)(     vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AggregatorQuery_t   *query );
  void                                (*DeleteSearch)(            vgx_base_search_context_t **search );
  vgx_base_search_context_t         * (*CloneSearch)(             vgx_Graph_t *self, const vgx_base_search_context_t *other );
} IGraphProbe_t;

DLL_HIDDEN extern IGraphProbe_t iGraphProbe;



/*******************************************************************//**
 *
 * vxquery_inspect
 *
 ***********************************************************************
 */
typedef struct s_IGraphInspect_t {
  vgx_ArcFilter_match (*HasAdjacency)( vgx_Vertex_t *vertex_RO, vgx_adjacency_search_context_t *search, vgx_Arc_t *first_match );
} IGraphInspect_t;

DLL_HIDDEN extern IGraphInspect_t iGraphInspect;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_collect_counts_t {
  int64_t data_size;
  int64_t n_collect;
  int64_t hits;
  int offset;
} vgx_collect_counts_t;



/*******************************************************************//**
 *
 * vxquery_collector
 *
 ***********************************************************************
 */
typedef struct s_IGraphCollector_t {
  void (*DeleteCollector)( vgx_BaseCollector_context_t **collector );
  vgx_ArcCollector_context_t * (*NewArcCollector)( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts );
  vgx_VertexCollector_context_t * (*NewVertexCollector)( vgx_Graph_t *graph, vgx_ranking_context_t *ranking_context, vgx_BaseQuery_t *query, vgx_collect_counts_t *counts );
  vgx_BaseCollector_context_t * (*ConvertToBaseListCollector)( vgx_BaseCollector_context_t *collector );
  vgx_BaseCollector_context_t * (*TrimBaseListCollector)( vgx_BaseCollector_context_t *collector, int64_t n_collected, int offset, int64_t hits );
  int64_t (*TransferBaseList)( vgx_ranking_context_t *ranking_context, vgx_BaseCollector_context_t **src, vgx_BaseCollector_context_t **dest );
} IGraphCollector_t;

DLL_HIDDEN extern IGraphCollector_t iGraphCollector;
DLL_HIDDEN extern vgx_VertexRef_t * _vxquery_collector__add_vertex_reference( vgx_BaseCollector_context_t *collector, vgx_Vertex_t *vertex, vgx_VertexRefLock_t *ext_lock );
DLL_HIDDEN extern int64_t _vxquery_collector__del_vertex_reference_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_VertexRef_t *vertexref, vgx_Graph_t **locked_graph );
DLL_HIDDEN extern vgx_Vertex_t * _vxquery_collector__safe_tail_access_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_Graph_t **locked_graph );
DLL_HIDDEN extern vgx_Vertex_t * _vxquery_collector__safe_head_access_OPEN( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc );
DLL_HIDDEN extern vgx_Vertex_t * _vxquery_collector__safe_head_access_ACQUIRE_CS( vgx_BaseCollector_context_t *collector, vgx_LockableArc_t *larc, vgx_Graph_t **locked_graph );




/*******************************************************************//**
 *
 * vxquery_traverse
 *
 ***********************************************************************
 */
typedef struct s_IGraphTraverse_t {
  vgx_collect_counts_t (*GetNeighborhoodCollectableCounts)( vgx_Graph_t *self, bool readonly_graph, const vgx_Vertex_t *vertex_RO, vgx_arc_direction arcdir, int offset, int64_t hits );
  vgx_collect_counts_t (*GetGlobalCollectableCounts)( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
  int64_t (*ValidateNeighborhoodCollectableCounts)( vgx_Graph_t *self, bool readonly_graph, vgx_NeighborhoodQuery_t *query, const vgx_Vertex_t *vertex_RO );
  int64_t (*ValidateGlobalCollectableCounts)( vgx_Graph_t *self, vgx_GlobalQuery_t *query );
  int (*TraverseNeighborArcs)( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *context );
  int (*TraverseNeighborVertices)( const vgx_Vertex_t *vertex_RO, vgx_neighborhood_search_context_t *search );
  int (*TraverseGlobalItems)( vgx_Graph_t *self, vgx_global_search_context_t *search, bool readonly_graph );
  int (*AggregateNeighborhood)( const vgx_Vertex_t *vertex_RO, vgx_aggregator_search_context_t *search );
} IGraphTraverse_t;

DLL_HIDDEN extern IGraphTraverse_t iGraphTraverse;



/*******************************************************************//**
 *
 * vxquery_aggregator
 *
 ***********************************************************************
 */
typedef struct s_IGraphAggregator_t {
  void (*DeleteAggregator)( vgx_BaseCollector_context_t **collector );
  vgx_ArcCollector_context_t * (*NewNeighborhoodAggregator)( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget );
  vgx_VertexCollector_context_t * (*NewVertexAggregator)( vgx_Graph_t *graph, vgx_aggregator_fields_t *fields, vgx_ExecutionTimingBudget_t *timing_budget );
} IGraphAggregator_t;

DLL_HIDDEN extern IGraphAggregator_t iGraphAggregator;




/*******************************************************************//**
 *
 * vxdurable_commit
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int64_t         _vxdurable_commit__close_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
DLL_HIDDEN extern int64_t         _vxdurable_commit__commit_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, bool hold_CS );
DLL_HIDDEN extern int64_t         _vxdurable_commit__commit_inarcs_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL );
DLL_HIDDEN extern vgx_Vertex_t *  _vxdurable_commit__new_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype, vgx_Rank_t rank, vgx_VertexStateContext_man_t manifestation, CString_t **CSTR__error );
DLL_HIDDEN extern int             _vxdurable_commit__delete_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t **vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record );
DLL_HIDDEN extern int             _vxdurable_commit__delete_vertex_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_ExecutionTimingBudget_t *timing_budget, vgx_AccessReason_t *reason, CString_t **CSTR__error );




/*******************************************************************//**
 *
 * vxgraph_arc
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int     _vxgraph_arc__connect_WL_reverse_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int refdelta );
DLL_HIDDEN extern int     _vxgraph_arc__connect_WL_to_WL_no_reverse( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_WL, int refdelta );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_WL_reverse_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int64_t refdelta, vgx_ExecutionTimingBudget_t *__na_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_WL_reverse_iWL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_iWL, int64_t head_refdelta, vgx_ExecutionTimingBudget_t *__na_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_WL_reverse_CS( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_WL_reverse_GENERAL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_WL_forward_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_WL_to_WL, int64_t refdelta, vgx_ExecutionTimingBudget_t *__na_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_iWL_forward_CS( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_iWL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget );
DLL_HIDDEN extern int64_t _vxgraph_arc__disconnect_iWL_forward_GENERAL( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_iWL_to_ANY, int64_t rev_head_refdelta, vgx_ExecutionTimingBudget_t *timing_budget );




/*******************************************************************//**
 *
 * vxvertex_property
 *
 ***********************************************************************
 */
DLL_HIDDEN extern int                       _vxvertex_property__set_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
DLL_HIDDEN extern vgx_VertexProperty_t *    _vxvertex_property__inc_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );
DLL_HIDDEN extern int32_t                   _vxvertex_property__set_vertex_enum_WL( vgx_Vertex_t *self_WL, int32_t e32 );
DLL_HIDDEN extern int32_t                   _vxvertex_property__vertex_enum_LCK( vgx_Vertex_t *self_LCK );
DLL_HIDDEN extern double                    _vxvertex_property__get_double_by_keyhash_RO( const vgx_Vertex_t *self_RO, shortid_t keyhash );
DLL_HIDDEN extern vgx_VertexProperty_t *    _vxvertex_property__get_internal_attribute_RO( const vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop );
DLL_HIDDEN extern vgx_VertexProperty_t *    _vxvertex_property__get_property_RO( const vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop );
DLL_HIDDEN extern bool                      _vxvertex_property__has_property_RO( const vgx_Vertex_t *self_RO, const vgx_VertexProperty_t *prop );
DLL_HIDDEN extern int64_t                   _vxvertex_property__num_properties_RO( vgx_Vertex_t *self_RO );
DLL_HIDDEN extern int                       _vxvertex_property__del_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop );

DLL_HIDDEN extern vgx_SelectProperties_t *  _vxvertex_property__get_properties_RO_CS( vgx_Vertex_t *self_RO_CS );
DLL_HIDDEN extern vgx_SelectProperties_t *  _vxvertex_property__get_properties_RO( vgx_Vertex_t *self_RO );
DLL_HIDDEN extern vgx_ExpressEvalStack_t *  _vxvertex_property__eval_properties_RO_CS( const vgx_CollectorItem_t *item_RO_CS, vgx_Evaluator_t *selecteval, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id );
DLL_HIDDEN extern int64_t                   _vxvertex_property__sync_properties_WL_CS_NT( vgx_Vertex_t *self_WL_CS );
DLL_HIDDEN extern int64_t                   _vxvertex_property__del_properties_WL( vgx_Vertex_t *self_WL );
DLL_HIDDEN extern int64_t                   _vxvertex_property__serialize_RO_CS( vgx_Vertex_t *self_WL, CQwordQueue_t *__OUTPUT );
DLL_HIDDEN extern int64_t                   _vxvertex_property__deserialize_WL_CS( vgx_Vertex_t *self_WL, CQwordQueue_t *__INPUT );
DLL_HIDDEN extern void                      _vxvertex_property__free_property_CS( vgx_Graph_t *self, vgx_VertexProperty_t **prop );
DLL_HIDDEN extern void                      _vxvertex_property__free_select_properties_CS( vgx_Graph_t *self, vgx_SelectProperties_t **selected );
DLL_HIDDEN extern void                      _vxvertex_property__clear_select_property_CS( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop );

DLL_HIDDEN extern void                      _vxvertex_property__virtual_properties_close( vgx_Graph_t *graph );
DLL_HIDDEN extern int                       _vxvertex_property__virtual_properties_open( vgx_Graph_t *graph, const char *tmpbase );
DLL_HIDDEN extern int                       _vxvertex_property__virtual_properties_move( vgx_Graph_t *graph, const char *tmpbase );
DLL_HIDDEN extern int64_t                   _vxvertex_property__virtual_properties_commit( vgx_Graph_t *graph );
DLL_HIDDEN extern void                      _vxvertex_property__virtual_properties_destroy( vgx_Graph_t *graph );
DLL_HIDDEN extern int                       _vxvertex_property__virtual_properties_init( vgx_Graph_t *graph );
DLL_HIDDEN extern CString_t *               _vxvertex_property__read_virtual_property( vgx_Graph_t *graph, int64_t offset, CString_t *CSTR__buffer, int64_t *rsz );



// Use a random key the we hope has no collision (no more likely than normal property collisions)
#define _vgx__vertex_enum_key 0x5ebcb35176dee1bd

#define IsPropertyKeyHashVertexEnum( KeyHash )  ((KeyHash) == _vgx__vertex_enum_key)


__inline static int32_t _vxvertex_property__get_vertex_enum_RO( const vgx_Vertex_t *vertex_RO ) {
  // Try to retrieve the internal enum property
  int64_t rvalue;
  if( vertex_RO->properties && iFramehash.simple.GetIntHash64( vertex_RO->properties, _vgx__vertex_enum_key, &rvalue ) > 0 ) {
    return (int32_t)rvalue;
  }
  
  // Not found
  return -1;
}

#define Vertex_GetEnum( Vertex_RO )         _vxvertex_property__get_vertex_enum_RO( Vertex_RO )
#define Vertex_HasEnum( Vertex_RO )         (Vertex_GetEnum( Vertex_RO ) > 0)
#define Vertex_SetEnum( Vertex_WL, Enum32 )  _vxvertex_property__set_vertex_enum_WL( Vertex_WL, Enum32 )

#endif

